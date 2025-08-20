#!/usr/bin/env python3
"""
Crawl Selver food categories, collect product pages, and emit a CSV compatible
with staging_selver_products:

  ext_id,name,ean_raw,size_text,price,currency,category_path,category_leaf

Notes
- Be polite: configurable concurrency + delay.
- Excludes non-food by keyword list (same file used by EAN fetcher).
- Robust-ish selectors with graceful fallbacks; continues on errors.
- If a category list file is missing, we fall back to a small built-in seed
  you can replace at data/selver_categories.txt (one URL per line).
"""

import os
import re
import csv
import json
import time
import asyncio
import unicodedata
from urllib.parse import urljoin, urlparse, urlencode, urlunparse, parse_qsl

import aiohttp
from bs4 import BeautifulSoup

SELVER_BASE = os.getenv("SELVER_BASE", "https://www.selver.ee")
OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")  # keep same as loader expects
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")
MAX_CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no explicit limit

# ---------- helpers ----------

def norm(s: str | None) -> str:
    if not s:
        return ""
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s

def load_lines(path: str) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    except FileNotFoundError:
        return []

def ensure_abs(url: str) -> str:
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urljoin(SELVER_BASE, url)

def canon_url_add_page(u: str, page: int) -> str:
    """
    Magento-ish listing often uses ?p=2. Preserve existing query and add/replace p.
    """
    parsed = urlparse(u)
    q = dict(parse_qsl(parsed.query))
    q["p"] = str(page)
    new_q = urlencode(q)
    return urlunparse(parsed._replace(query=new_q))

def load_banned(path="data/selver_excluded_keywords.txt") -> list[str]:
    lines = load_lines(path)
    if lines:
        return [ln.lower() for ln in lines]
    # fallback: same defaults you used on Prisma side
    fallback = [
        "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid","vaikesed-sisustuskaubad","kuunlad",
        "kook-ja-lauakatmine","uhekordsed-noud","kirja-ja-kontoritarbed","remondi-ja-turvatooted",
        "kulmutus-ja-kokkamisvahendid","omblus-ja-kasitootarbed","meisterdamine","ajakirjad","autojuhtimine",
        "kotid","aed-ja-lilled","lemmikloom","sport","pallimangud","jalgrattasoit","ujumine","matkamine",
        "tervisesport","manguasjad","lutid","lapsehooldus","ideed-ja-hooajad","kodumasinad","elektroonika",
        "meelelahutuselektroonika","vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
        "ilu-ja-tervis","kosmeetika","meigitooted","hugieen","loodustooted-ja-toidulisandid"
    ]
    return fallback

BANNED = load_banned()

def looks_banned(name: str, cat: str, url: str) -> bool:
    hay = " ".join([name.lower(), cat.lower(), url.lower()])
    return any(kw in hay for kw in BANNED)

SIZE_RE = re.compile(r"(\b\d+(?:[\.,]\d+)?\s?(?:g|kg|ml|l)\b)", re.I)

def guess_size(s: str) -> str | None:
    m = SIZE_RE.search(s or "")
    return m.group(1).replace(",", ".") if m else None

def parse_jsonld_scripts(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                out.extend(data)
            else:
                out.append(data)
        except Exception:
            # Sometimes invalid JSON; ignore
            continue
    return out

def pick_product(ld: dict) -> bool:
    t = ld.get("@type")
    if isinstance(t, list):
        types = [str(x).lower() for x in t]
    else:
        types = [str(t).lower()] if t else []
    return any(x in ("product", "schema:product") for x in types)

def extract_breadcrumbs(ld_blocks: list[dict]) -> str:
    for ld in ld_blocks:
        t = str(ld.get("@type", "")).lower()
        if t in ("breadcrumblist", "schema:breadcrumblist"):
            try:
                items = ld.get("itemListElement") or []
                parts = []
                for it in items:
                    item = it.get("item") or {}
                    name = item.get("name") or it.get("name") or ""
                    if name:
                        parts.append(norm(name))
                return " / ".join(parts)
            except Exception:
                continue
    return ""

def extract_from_product_jsonld(ld: dict) -> tuple[str, str, float, str]:
    name = norm(ld.get("name") or "")
    # EAN variations
    ean = ld.get("gtin13") or ld.get("gtin") or ld.get("sku") or ""
    ean = re.sub(r"\D", "", str(ean))
    offers = ld.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price_raw = str(offers.get("price", "")).replace(",", ".")
    try:
        price = float(price_raw) if price_raw else 0.0
    except ValueError:
        price = 0.0
    currency = (offers.get("priceCurrency") or "EUR").upper()
    return name, ean, price, currency

# ---------- network ----------

async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        return await r.text()

async def parse_product_page(session: aiohttp.ClientSession, url: str) -> dict | None:
    html = await fetch(session, url)
    soup = BeautifulSoup(html, "lxml")

    ld_blocks = parse_jsonld_scripts(soup)
    prod_ld = next((ld for ld in ld_blocks if pick_product(ld)), None)

    # basics
    name, ean, price, currency = "", "", 0.0, "EUR"
    if prod_ld:
        name, ean, price, currency = extract_from_product_jsonld(prod_ld)

    # fallback name
    if not name:
        h = soup.select_one("h1.page-title span.base") or soup.select_one("h1.page-title")
        if h:
            name = norm(h.get_text(" ", strip=True))

    # fallback price
    if not price:
        # Try a more generic price hint
        price_el = soup.select_one("[data-price-amount]") or soup.select_one("span.price")
        if price_el:
            amt = price_el.get("data-price-amount") or price_el.get_text("", strip=True)
            amt = amt.replace("\u00a0", "").replace("€", "").replace(",", ".")
            try:
                price = float(re.findall(r"[\d\.]+", amt)[0])
            except Exception:
                pass

    # category path via breadcrumbs
    cat_path = extract_breadcrumbs(ld_blocks)
    if not cat_path:
        bc = soup.select("ul.items > li[itemprop='itemListElement'] a span, .breadcrumbs a span")
        if bc:
            cat_path = " / ".join(norm(x.get_text(" ", strip=True)) for x in bc)

    if not name:
        return None

    if looks_banned(name, cat_path, url):
        return None

    return {
        "ext_id": url,
        "name": name,
        "ean_raw": ean or "",
        "size_text": guess_size(name),
        "price": price,
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": (cat_path.split(" / ")[-1] if cat_path else "")
    }

async def extract_product_links_from_category(session: aiohttp.ClientSession, cat_url: str) -> set[str]:
    """
    Returns product detail page URLs from a category listing, following pagination.
    """
    links: set[str] = set()
    page = 1

    while True:
        url = canon_url_add_page(cat_url, page)
        try:
            html = await fetch(session, url)
        except Exception:
            break

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a.product-item-link"):
            href = a.get("href")
            if href:
                links.add(ensure_abs(href))

        # pagination: look for "next" (Magento often uses li.pages-item-next a)
        next_link = soup.select_one("li.pages-item-next a, a.next")
        if next_link and next_link.get("href"):
            page += 1
            if PAGE_LIMIT and page > PAGE_LIMIT:
                break
            await asyncio.sleep(REQ_DELAY)
            continue

        break

    return links

# ---------- main runner ----------

def load_categories() -> list[str]:
    cat_urls = load_lines(CATEGORIES_FILE)
    if cat_urls:
        return [ensure_abs(u) for u in cat_urls]

    # Fallback tiny seed (you can replace with your curated list in data/selver_categories.txt)
    seeds = [
        "/piim-tooted-ja-munad.html",
        "/leib-sai-ja-maiustused.html",
        "/puu-ja-koogiviljad.html",
        "/liha-kala-ja-valmistoit.html",
        "/kuivained-ja-maitseained.html",
        "/joogid.html",
        "/konservid-ja-olikaste.html",
    ]
    return [ensure_abs(s) for s in seeds]

async def runner():
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SelverCrawlBot/0.1; +https://example.invalid)",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
    }
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY)

    categories = load_categories()
    if not categories:
        print("No category URLs provided; create data/selver_categories.txt (one URL per line).")
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    seen_products: set[str] = set()

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=[
                "ext_id", "name", "ean_raw", "size_text", "price", "currency",
                "category_path", "category_leaf",
            ],
        )
        writer.writeheader()

        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:

            async def crawl_category(cat_url: str):
                async with sem:
                    try:
                        product_links = await extract_product_links_from_category(session, cat_url)
                        await asyncio.sleep(REQ_DELAY)
                        for link in sorted(product_links):
                            if link in seen_products:
                                continue
                            seen_products.add(link)
                            try:
                                item = await parse_product_page(session, link)
                                await asyncio.sleep(REQ_DELAY)
                                if not item:
                                    continue
                                writer.writerow(item)
                            except Exception:
                                # continue with next link
                                continue
                    except Exception:
                        # continue with next category
                        return

            await asyncio.gather(*(crawl_category(u) for u in categories))

if __name__ == "__main__":
    asyncio.run(runner())

#!/usr/bin/env python3
import os, re, csv, asyncio, json
from urllib.parse import urljoin, urlparse, parse_qs
import aiohttp
from bs4 import BeautifulSoup

BASE = "https://www.selver.ee"
OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")
CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = unlimited

# very broad “non-food” filter
BANNED = [
    "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid",
    "vaikesed-sisustuskaubad","kuunlad","kook-ja-lauakatmine",
    "uhekordsed-noud","kirja-ja-kontoritarbed","remondi-ja-turvatooted",
    "kulmutus-ja-kokkamisvahendid","omblus-ja-kasitootarbed","meisterdamine",
    "ajakirjad","autojuhtimine","kotid","aed-ja-lilled","lemmikloom",
    "sport","pallimangud","jalgrattasoit","ujumine","matkamine",
    "tervisesport","manguasjad","lutid","lapsehooldus","ideed-ja-hooajad",
    "kodumasinad","elektroonika","meelelahutuselektroonika",
    "vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
    "ilu-ja-tervis","kosmeetika","meigitooted","hugieen",
    "loodustooted-ja-toidulisandid",
]

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
    "Referer": BASE,
    "Cache-Control": "no-cache",
}

def looks_banned(url_or_text: str) -> bool:
    t = (url_or_text or "").lower()
    return any(kw in t for kw in BANNED)

async def fetch_html(session, url: str) -> tuple[str, str]:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30), allow_redirects=True) as r:
        r.raise_for_status()
        return await r.text(), str(r.url)

def extract_jsonld_list(soup: BeautifulSoup) -> list[dict]:
    out = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, list): out.extend(data)
            else: out.append(data)
        except Exception:
            pass
    return out

def product_urls_from_soup(soup: BeautifulSoup) -> set[str]:
    # 1) obvious product cards
    sels = [
        "a.product-item-link",
        "li.product-item a.product-item-link",
        "a[href*='/toode/']",
        "a[href*='/product/']",
        "a[href*='/e-selver/']:has(img)",  # sometimes product tiles live under e-selver/
    ]
    urls = set()
    for sel in sels:
        for a in soup.select(sel):
            href = a.get("href")
            if href:
                urls.add(href)

    # 2) fallback: ItemList JSON-LD
    for block in extract_jsonld_list(soup):
        if str(block.get("@type")).lower() in ("itemlist", "schema:itemlist"):
            items = block.get("itemListElement") or []
            for it in items:
                # Many schemas use {"item": {"@id": "...", "url": "..."}} or direct "url"
                item = it.get("item") if isinstance(it, dict) else None
                url = None
                if isinstance(item, dict):
                    url = item.get("url") or item.get("@id")
                if not url and isinstance(it, dict):
                    url = it.get("url")
                if url:
                    urls.add(url)
    return {url for url in urls if "/toode/" in url or "/product/" in url}

def next_page_url(soup: BeautifulSoup, cur_url: str) -> str | None:
    # multiple patterns for pagination
    cand = (soup.select_one("a[rel='next']") or
            soup.select_one("a.page-next") or
            soup.select_one("a.pagination-next") or
            soup.select_one("a[href*='?p=']"))
    href = cand.get("href") if cand else None
    if not href:
        return None
    return urljoin(cur_url, href)

def normalize_price(txt: str) -> float:
    txt = (txt or "").strip().replace("\xa0", " ").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", txt)
    return float(m.group(1)) if m else 0.0

def extract_price_from_card(card: BeautifulSoup) -> float:
    # several possible price spans
    for sel in [".price", ".price-final_price .price", ".amount", "[data-price-amount]"]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            return normalize_price(el.get_text(strip=True))
        if el and el.has_attr("data-price-amount"):
            try: return float(el["data-price-amount"])
            except Exception: pass
    return 0.0

async def scrape_category(session, url: str, writer, seen_products: set[str], page_limit: int = 0):
    if looks_banned(url):
        return 0
    pages = 0
    new_rows = 0
    cur = url
    while cur:
        html, cur_final = await fetch_html(session, cur)
        soup = BeautifulSoup(html, "lxml")

        # product tiles
        grid = soup.select("li.product-item, div.product-item, div.product-item-info")
        links = product_urls_from_soup(soup)
        # tie prices if possible (best-effort)
        if grid and links:
            # try pairing by index count
            cards = []
            for li in soup.select("li.product-item, div.product-item-info, div.product-item"):
                a = li.select_one("a.product-item-link") or li.select_one("a[href*='/toode/']")
                href = a.get("href") if a else None
                if href:
                    cards.append((href, extract_price_from_card(li)))

            price_map = {href: price for (href, price) in cards}
        else:
            price_map = {}

        wrote_this_page = 0
        for href in links:
            if looks_banned(href): continue
            if href in seen_products: continue
            seen_products.add(href)

            price = price_map.get(href, 0.0)
            writer.writerow({
                "ext_id": href,
                "name": "",                # filled by loader match, not needed here
                "ean_raw": "",             # we only need ext_id + price for candidates too
                "size_text": "",
                "price": f"{price:.2f}" if price else "",
                "currency": "EUR",
                "category_path": "",
                "category_leaf": "",
            })
            wrote_this_page += 1
            new_rows += 1

        pages += 1
        if page_limit and pages >= page_limit:
            break

        nxt = next_page_url(soup, cur_final)
        if nxt and nxt != cur_final:
            cur = nxt
        else:
            break

        await asyncio.sleep(REQ_DELAY)

    return new_rows

async def discover_categories(session) -> list[str]:
    # Try to get main nav categories from homepage
    html, final = await fetch_html(session, BASE)
    soup = BeautifulSoup(html, "lxml")
    cats = set()

    # broad nav anchors; prefer /e-selver/ or /catalog/ type paths
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href: continue
        url = urljoin(final, href)
        p = urlparse(url).path.lower()
        if any(seg in p for seg in ("/e-selver", "/catalog", "/tooted", "/food")):
            if not looks_banned(url):
                cats.add(url)

    # If nothing found, fallback to a few common top-levels
    if not cats:
        fallback = [
            "/e-selver/liha-ja-kalatooted",
            "/e-selver/piimatooted-ja-munad",
            "/e-selver/joogid",
            "/e-selver/kuivtooted",
            "/e-selver/kulmutatud-tooted",
            "/e-selver/leib-sai-ja-kondiitritooted",
        ]
        cats = {urljoin(BASE, x) for x in fallback}

    # De-dup
    out = sorted({c for c in cats if c.startswith("http")})
    return out

def load_categories_from_file() -> list[str]:
    if not os.path.exists(CATEGORIES_FILE):
        return []
    rows = []
    with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                rows.append(ln)
    return rows

async def main():
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    sem = asyncio.Semaphore(CONCURRENCY)
    seen_products: set[str] = set()

    # CSV header
    f = open(OUTPUT, "w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=[
        "ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"
    ])
    w.writeheader()

    # gather category list
    cats = load_categories_from_file()
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        if not cats:
            cats = await discover_categories(session)

        # filter only “food-ish” and not banned
        cats = [c for c in cats if not looks_banned(c)]
        print(f"[selver] Categories to crawl: {len(cats)}")

        async def work(url):
            async with sem:
                try:
                    got = await scrape_category(session, url, w, seen_products, PAGE_LIMIT)
                    print(f"[selver] {url} → +{got} products (total so far: {len(seen_products)})")
                except Exception as e:
                    print(f"[selver] ERR {url}: {e}")

        await asyncio.gather(*(work(u) for u in cats))

    f.close()
    print(f"[selver] Finished. Unique product URLs written: {len(seen_products)}")

if __name__ == "__main__":
    asyncio.run(main())

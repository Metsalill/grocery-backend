#!/usr/bin/env python3
import os, re, csv, asyncio, json, unicodedata
from urllib.parse import urljoin, urlparse, parse_qs
import aiohttp
from bs4 import BeautifulSoup

BASE = "https://www.selver.ee"
OUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
CONC = int(os.getenv("CONCURRENCY", "4"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.7"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0=no limit
CATS_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

DEFAULT_SEEDS = [
    "https://www.selver.ee/e-selver/puu-ja-koogiviljad",
    "https://www.selver.ee/e-selver/piimatooted-ja-munad",
    "https://www.selver.ee/e-selver/leivad-saia-ja-saialised",
    "https://www.selver.ee/e-selver/liha-ja-kalatooted",
    "https://www.selver.ee/e-selver/valmistoit",
    "https://www.selver.ee/e-selver/kuivained",
    "https://www.selver.ee/e-selver/suupisted-ja-maiustused",
    "https://www.selver.ee/e-selver/jook",
    "https://www.selver.ee/e-selver/kulmutatud-toit",
    "https://www.selver.ee/e-selver/laste-toit",
]

EXCLUDE = set("""
sisustus kodutekstiil valgustus kardin jouluvalgustid vaikesed-sisustuskaubad kuunlad
kook-ja-lauakatmine uhekordsed-noud kirja-ja-kontoritarbed remondi-ja-turvatooted
kulmutus-ja-kokkamisvahendid omblus-ja-kasitootarbed meisterdamine ajakirjad autojuhtimine
kotid aed-ja-lilled lemmikloom sport pallimangud jalgrattasoit ujumine matkamine
tervisesport manguasjad lutid lapsehooldus ideed-ja-hooajad kodumasinad elektroonika
meelelahutuselektroonika vaikesed-kodumasinad lambid-patareid-ja-taskulambid
ilu-ja-tervis kosmeetika meigitooted hugieen loodustooted-ja-toidulisandid
""".split())

def n(s: str | None) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return " ".join(s.split())

def load_seeds() -> list[str]:
    try:
        with open(CATS_FILE, "r", encoding="utf-8") as f:
            seeds = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
            if seeds:
                return seeds
    except FileNotFoundError:
        pass
    return DEFAULT_SEEDS

def is_food_category(url: str) -> bool:
    low = url.lower()
    return "/e-selver/" in low and not any(k in low for k in EXCLUDE)

async def fetch(session, url: str) -> tuple[str, str]:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=35), allow_redirects=True) as r:
        r.raise_for_status()
        return await r.text(), str(r.url)

def parse_jsonld(soup: BeautifulSoup) -> list[dict]:
    out = []
    for t in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(t.string or "{}")
            if isinstance(data, list): out.extend(data)
            else: out.append(data)
        except Exception: pass
    return out

def pick_product(ld: dict) -> bool:
    t = ld.get("@type")
    if isinstance(t, str): t = [t.lower()]
    if isinstance(t, list): t = [str(x).lower() for x in t]
    return bool(t) and ("product" in t or "schema:product" in t)

def get_price_currency(ld: dict) -> tuple[float, str]:
    offers = ld.get("offers") or {}
    if isinstance(offers, list): offers = offers[0] if offers else {}
    cur = (offers.get("priceCurrency") or "EUR").upper()
    raw = str(offers.get("price", "0")).replace(",", ".")
    try: price = float(raw)
    except Exception: price = 0.0
    return price, cur

def extract_breadcrumbs(ld_blocks: list[dict]) -> str:
    for ld in ld_blocks:
        t = str(ld.get("@type","")).lower()
        if t in ("breadcrumblist","schema:breadcrumblist"):
            try:
                items = ld.get("itemListElement") or []
                return " / ".join(n(x["item"]["name"]) for x in items if x.get("item"))
            except Exception: pass
    return ""

def ean_from_details(soup: BeautifulSoup) -> str:
    # Look for a “Ribakood/EAN/SKU” label followed by numbers
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(Ribakood|EAN(?:-kood)?|SKU)\s*[:\-]?\s*(\d{8,14})", text, re.I)
    if m: return m.group(2)
    # Table-style dt/dd
    for dt in soup.select("dt"):
        lab = n(dt.get_text(" ", strip=True)).lower()
        if any(k in lab for k in ("ribakood","ean","sku")):
            dd = dt.find_next("dd")
            if dd:
                m2 = re.search(r"\d{8,14}", dd.get_text(" ", strip=True))
                if m2: return m2.group(0)
    return ""

async def parse_product(session, url: str) -> dict | None:
    html, final = await fetch(session, url)
    soup = BeautifulSoup(html, "lxml")
    ld_blocks = parse_jsonld(soup)
    prod = next((ld for ld in ld_blocks if pick_product(ld)), None)
    if not prod:
        return None
    name = n(prod.get("name") or "")
    price, currency = get_price_currency(prod)
    ean = re.sub(r"\D", "", str(prod.get("gtin13") or prod.get("gtin") or prod.get("sku") or ""))
    if not ean:
        ean = ean_from_details(soup)
    cat_path = extract_breadcrumbs(ld_blocks)
    size = None
    m = re.search(r"(\b\d+(?:[.,]\d+)?\s?(?:g|kg|ml|l)\b)", name, re.I)
    if m: size = m.group(1).replace(",", ".")
    return {
        "ext_id": final,
        "name": name,
        "ean_raw": ean,
        "size_text": size or "",
        "price": price,
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": cat_path.split(" / ")[-1] if cat_path else "",
    }

def extract_links_from_category(html: str, base_url: str) -> tuple[set[str], str | None]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("a.product-item-link") or soup.select("li.product-item a[href*='/toode/']")
    prods = {urljoin(base_url, a["href"]) for a in cards if a.get("href")}
    # next page
    nxt = soup.select_one("a[rel='next']") or soup.select_one("a.pages-item-next a") or soup.select_one("a[href*='?p=']")
    next_url = urljoin(base_url, nxt["href"]) if nxt and nxt.get("href") else None
    return prods, next_url

async def crawl_category(session, start_url: str, writer, sem: asyncio.Semaphore):
    if not is_food_category(start_url):
        return
    seen_pages = set()
    pages_done = 0
    url = start_url
    while url and (PAGE_LIMIT == 0 or pages_done < PAGE_LIMIT):
        if url in seen_pages: break
        seen_pages.add(url)
        try:
            html, final = await fetch(session, url)
        except Exception:
            break
        prods, next_url = extract_links_from_category(html, final)
        for p in prods:
            async with sem:
                try:
                    item = await parse_product(session, p)
                    await asyncio.sleep(REQ_DELAY)
                    if item:
                        writer.writerow(item)
                except Exception:
                    pass
        url = next_url
        pages_done += 1

async def main():
    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    seeds = load_seeds()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
        "Referer": BASE,
    }
    sem = asyncio.Semaphore(CONC)
    async with aiohttp.ClientSession(headers=headers) as session:
        with open(OUT, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"
            ])
            w.writeheader()
            tasks = [crawl_category(session, s, w, sem) for s in seeds]
            await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

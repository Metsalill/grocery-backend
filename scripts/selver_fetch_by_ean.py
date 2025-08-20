#!/usr/bin/env python3
import os, re, csv, asyncio, ssl, json, unicodedata
from urllib.parse import urljoin, urlencode
import aiohttp
from bs4 import BeautifulSoup
import asyncpg

SELVER_BASE = "https://www.selver.ee"
SEARCH_PATH = "/catalogsearch/result/?"
OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
MAX_CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))

def norm(s: str | None) -> str:
    if not s: return ""
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s

def load_banned(path="data/selver_excluded_keywords.txt"):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip().lower() for ln in f if ln.strip() and not ln.startswith("#")]
    except FileNotFoundError:
        return []
BANNED = load_banned()

def looks_banned(name: str, cat: str, url: str) -> bool:
    hay = " ".join([name.lower(), cat.lower(), url.lower()])
    return any(kw in hay for kw in BANNED)

SIZE_RE = re.compile(r'(\b\d+(\.\d+)?\s?(g|kg|ml|l)\b)', re.I)

def guess_size(s: str) -> str | None:
    m = SIZE_RE.search(s or "")
    return m.group(1) if m else None

def parse_jsonld_scripts(soup: BeautifulSoup) -> list[dict]:
    out = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, list): out.extend(data)
            else: out.append(data)
        except Exception:
            continue
    return out

def pick_product(ld: dict) -> bool:
    t = ld.get("@type")
    if isinstance(t, list): t = [x.lower() for x in t]
    if isinstance(t, str): t = [t.lower()]
    return t and ("product" in t or "schema:product" in t)

def extract_from_jsonld(ld: dict) -> tuple[str,str,float,str]:
    name = norm(ld.get("name") or "")
    # EAN/GTIN options commonly used
    ean = ld.get("gtin13") or ld.get("gtin") or ld.get("sku") or ""
    ean = re.sub(r"\D", "", str(ean))
    offers = ld.get("offers") or {}
    if isinstance(offers, list): offers = offers[0] if offers else {}
    price = float(str(offers.get("price","0")).replace(",", ".") or 0)
    currency = (offers.get("priceCurrency") or "EUR").upper()
    return name, ean, price, currency

def extract_breadcrumbs(ld_blocks: list[dict]) -> str:
    for ld in ld_blocks:
        if str(ld.get("@type")).lower() in ("breadcrumblist","schema:breadcrumblist"):
            try:
                items = ld.get("itemListElement") or []
                return " / ".join(norm(x["item"]["name"]) for x in items if x.get("item"))
            except Exception:
                continue
    return ""

async def fetch_text(session, url):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        return await r.text()

async def parse_search_result(session, ean: str) -> str | None:
    # Selver search by q=<EAN>, return first product link (if any)
    url = urljoin(SELVER_BASE, SEARCH_PATH) + urlencode({"q": ean})
    html = await fetch_text(session, url)
    soup = BeautifulSoup(html, "lxml")
    first = soup.select_one("a.product-item-link")
    if first and first.get("href"):
        return first["href"]
    # fallback: any product link
    alt = soup.select_one("a[href*='/toode']") or soup.select_one("a[href*='/product']")
    return alt["href"] if alt and alt.get("href") else None

async def parse_product_page(session, url: str) -> dict | None:
    html = await fetch_text(session, url)
    soup = BeautifulSoup(html, "lxml")
    ld_blocks = parse_jsonld_scripts(soup)
    prod = next((ld for ld in ld_blocks if pick_product(ld)), None)
    if not prod:  # some pages embed data elsewhere — extend here if needed
        return None
    name, ean_ld, price, currency = extract_from_jsonld(prod)
    cat_path = extract_breadcrumbs(ld_blocks)
    if looks_banned(name, cat_path, url):
        return None
    return {
        "ext_id": url,             # stable enough; DB will key on ext_id for candidates
        "name": name,
        "ean_raw": ean_ld or "",
        "size_text": guess_size(name),
        "price": price,
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": cat_path.split(" / ")[-1] if cat_path else "",
    }

async def fetch_eans_from_db(db_url: str) -> list[tuple[str,int]]:
    conn = await asyncpg.connect(dsn=db_url, ssl=ssl.create_default_context())
    rows = await conn.fetch("""
        SELECT DISTINCT pe.ean_norm, pe.product_id
        FROM public.product_eans pe
        JOIN public.products p ON p.id = pe.product_id
        """)
    await conn.close()
    return [(r["ean_norm"], r["product_id"]) for r in rows]

async def runner():
    db_url = os.environ["DATABASE_URL"]
    out_path = OUTPUT
    eans = await fetch_eans_from_db(db_url)
    if not eans:
        print("No EANs found; exiting.")
        return

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SelverBot/0.1)"}

    async with aiohttp.ClientSession(headers=headers) as session, \
        open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"
        ])
        w.writeheader()

        async def process(ean: str):
            async with sem:
                try:
                    url = await parse_search_result(session, ean)
                    await asyncio.sleep(REQ_DELAY)
                    if not url: return
                    item = await parse_product_page(session, url)
                    await asyncio.sleep(REQ_DELAY)
                    if not item: return
                    w.writerow(item)
                except Exception as e:
                    # keep going
                    return

        await asyncio.gather(*(process(e) for e,_ in eans))

if __name__ == "__main__":
    asyncio.run(runner())

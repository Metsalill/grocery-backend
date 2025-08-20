#!/usr/bin/env python3
import os, re, csv, asyncio, ssl, json, unicodedata
from urllib.parse import urljoin, urlencode, urlparse, parse_qs
import aiohttp
from bs4 import BeautifulSoup
import asyncpg

SELVER_BASE = "https://www.selver.ee"
SEARCH_PATH = "/catalogsearch/result/?"
OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
MAX_CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
EAN_LIMIT = int(os.getenv("EAN_LIMIT", "0"))  # 0 = no limit

# ------------------------- helpers -------------------------

def norm(s: str | None) -> str:
    if not s:
        return ""
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
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

SIZE_RE = re.compile(r'(\b\d+(?:[.,]\d+)?\s?(?:g|kg|ml|l)\b)', re.I)

def guess_size(s: str) -> str | None:
    m = SIZE_RE.search(s or "")
    return m.group(1) if m else None

def parse_jsonld_scripts(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            raw = tag.string or ""
            if not raw.strip():
                continue
            data = json.loads(raw)
            if isinstance(data, list):
                out.extend([d for d in data if isinstance(d, dict)])
            elif isinstance(data, dict):
                out.append(data)
        except Exception:
            continue
    return out

def pick_product(ld: dict) -> bool:
    t = ld.get("@type")
    if isinstance(t, list):
        t = [str(x).lower() for x in t]
    elif isinstance(t, str):
        t = [t.lower()]
    else:
        t = []
    return ("product" in t) or ("schema:product" in t)

def extract_from_jsonld(ld: dict) -> tuple[str, str, float, str]:
    name = norm(ld.get("name") or "")
    # Common EAN/GTIN fields
    ean = ld.get("gtin13") or ld.get("gtin") or ld.get("sku") or ""
    ean = re.sub(r"\D", "", str(ean))
    offers = ld.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price_str = str(offers.get("price", "0")).replace(",", ".")
    try:
        price = float(price_str) if price_str else 0.0
    except Exception:
        price = 0.0
    currency = (offers.get("priceCurrency") or "EUR").upper()
    return name, ean, price, currency

def extract_breadcrumbs(ld_blocks: list[dict]) -> str:
    for ld in ld_blocks:
        t = str(ld.get("@type", "")).lower()
        if t in ("breadcrumblist", "schema:breadcrumblist"):
            try:
                items = ld.get("itemListElement") or []
                parts = []
                for x in items:
                    item = x.get("item")
                    if isinstance(item, dict):
                        parts.append(norm(item.get("name") or ""))
                    elif isinstance(item, str):
                        parts.append(norm(item))
                return " / ".join(p for p in parts if p)
            except Exception:
                continue
    return ""

def ssl_context_for(url: str) -> ssl.SSLContext | None:
    """
    Emulate libpq-ish sslmode for asyncpg.
    - require/prefer/allow -> encrypt but don't verify (CERT_NONE)
    - verify-ca/verify-full -> default context (verify)
    - disable -> None
    """
    q = parse_qs(urlparse(url).query)
    mode = (q.get("sslmode", ["require"])[0] or "require").lower()
    if mode == "disable":
        return None
    ctx = ssl.create_default_context()
    if mode in ("require", "prefer", "allow"):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx

def get_db_url_env() -> str:
    db_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_PUBLIC")
    if not db_url:
        raise RuntimeError("DATABASE_URL (or DATABASE_URL_PUBLIC) is not set")
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    return db_url

# ------------------------- HTTP -------------------------

async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        return await r.text()

async def parse_search_result(session: aiohttp.ClientSession, ean: str) -> str | None:
    """Search by EAN and return absolute URL of the first product hit."""
    url = urljoin(SELVER_BASE, SEARCH_PATH) + urlencode({"q": ean})
    try:
        html = await fetch_text(session, url)
    except Exception:
        return None
    soup = BeautifulSoup(html, "lxml")
    first = soup.select_one("a.product-item-link")
    if first and first.get("href"):
        return urljoin(SELVER_BASE, first["href"])
    # fallback to any plausible product link
    alt = soup.select_one("a[href*='/toode'], a[href*='/product']")
    if alt and alt.get("href"):
        return urljoin(SELVER_BASE, alt["href"])
    return None

async def parse_product_page(session: aiohttp.ClientSession, url: str) -> dict | None:
    try:
        html = await fetch_text(session, url)
    except Exception:
        return None
    soup = BeautifulSoup(html, "lxml")
    ld_blocks = parse_jsonld_scripts(soup)
    prod = next((ld for ld in ld_blocks if pick_product(ld)), None)
    if not prod:
        return None

    name, ean_ld, price, currency = extract_from_jsonld(prod)
    cat_path = extract_breadcrumbs(ld_blocks)
    if looks_banned(name, cat_path, url):
        return None

    return {
        "ext_id": url,  # stable enough; used as key in candidates
        "name": name,
        "ean_raw": ean_ld or "",
        "size_text": guess_size(name),
        "price": price,
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": cat_path.split(" / ")[-1] if cat_path else "",
    }

# ------------------------- DB -------------------------

async def fetch_eans_from_db(db_url: str) -> list[tuple[str, int]]:
    ctx = ssl_context_for(db_url)
    conn = await asyncpg.connect(dsn=db_url, ssl=ctx, timeout=30)
    rows = await conn.fetch("""
        SELECT DISTINCT pe.ean_norm, pe.product_id
        FROM public.product_eans pe
        JOIN public.products p ON p.id = pe.product_id
        WHERE pe.ean_norm IS NOT NULL AND pe.ean_norm <> ''
        ORDER BY pe.product_id
    """)
    await conn.close()
    items = [(r["ean_norm"], r["product_id"]) for r in rows]
    if EAN_LIMIT > 0:
        items = items[:EAN_LIMIT]
    return items

# ------------------------- main runner -------------------------

async def runner():
    db_url = get_db_url_env()
    out_path = OUTPUT

    eans = await fetch_eans_from_db(db_url)
    if not eans:
        print("No EANs found; exiting.")
        return

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SelverBot/0.1)",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
    }

    async with aiohttp.ClientSession(headers=headers) as session, \
            open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ext_id", "name", "ean_raw", "size_text", "price", "currency", "category_path", "category_leaf"],
        )
        writer.writeheader()

        async def process(ean: str):
            async with sem:
                try:
                    url = await parse_search_result(session, ean)
                    await asyncio.sleep(REQ_DELAY)
                    if not url:
                        return
                    item = await parse_product_page(session, url)
                    await asyncio.sleep(REQ_DELAY)
                    if not item:
                        return
                    writer.writerow(item)
                except Exception:
                    # best-effort scraping: skip on any error
                    return

        await asyncio.gather(*(process(e) for e, _ in eans))

if __name__ == "__main__":
    asyncio.run(runner())

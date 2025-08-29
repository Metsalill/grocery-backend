#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee category crawler → PDP extractor → CSV

- Robust category → product discovery (next / load-more / scroll).
- PDP parser uses DOM, JSON-LD (offers), microdata and window globals.
- Reliable price extraction, including split integer/decimal spans.
- Optional DB preload: skip already-seen ext_id (rimi_candidates + staging).
  * SKIP_KNOWN env (default 0) controls whether to skip.
  * PRELOAD_DB (default 1) enables DB preloading when DATABASE_URL is set.
- CLI is resilient in CI: non-fatal on minor issues, clean logging.

CLI:
  --cats-file FILE        File with category URLs (one per line)
  --page-limit N          Max pages per category (0=all)
  --max-products N        Hard cap on total PDPs (0=unlimited)
  --headless 1/0          Headless browser (default 1)
  --req-delay SEC         Delay between page ops (default 0.5)
  --output-csv FILE       Output CSV path (default data/rimi_products.csv)
"""

from __future__ import annotations
import argparse, os, re, csv, json, sys, traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ------------------------------- store meta ----------------------------------

STORE_CHAIN   = "Rimi"
STORE_NAME    = "Rimi ePood"
STORE_CHANNEL = "online"
BASE = "https://www.rimi.ee"

# ------------------------------- regexes -------------------------------------

EAN_RE = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE = re.compile(r"(\d+[.,]\d+)\s*€")
PRICE_FRAGMENT_RE = re.compile(r"(\d+)[^\d]+(\d{2})")  # e.g. "3 99" → 3.99

SKU_KEYS = {"sku","mpn","itemNumber","productCode","code","id","itemid"}
EAN_KEYS = {"ean","ean13","gtin","gtin13","barcode"}

# --------------------------- small utils / helpers ---------------------------

def deep_find_kv(obj: Any, keys: set) -> Dict[str,str]:
    out: Dict[str,str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k,v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str,int,float)):
                    out[lk] = str(v)
                walk(v)
        elif isinstance(x, list):
            for i in x: walk(i)
    walk(obj)
    return out

def normalize_href(href: Optional[str]) -> Optional[str]:
    if not href: return None
    href = href.split("?")[0].split("#")[0]
    return href if href.startswith("http") else urljoin(BASE, href)

def _ext_from_href(u: str) -> str:
    try:
        parts = urlparse(u).path.rstrip("/").split("/")
        if "p" in parts:
            i = parts.index("p")
            return parts[i+1]
    except Exception:
        pass
    return ""

def auto_accept_overlays(page) -> None:
    labels = [
        r"Nõustun", r"Nõustu", r"Accept", r"Allow all", r"OK", r"Selge",
        r"Jätka", r"Vali hiljem", r"Continue", r"Close", r"Sulge",
        r"Vali pood", r"Vali teenus", r"Telli koju", r"Vali kauplus",
        r"Näita kõiki tooteid", r"Kuva tooted", r"Kuva kõik tooted",
    ]
    for lab in labels:
        try:
            page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=800)
            page.wait_for_timeout(120)
        except Exception:
            pass

def wait_for_hydration(page, timeout_ms: int = 9000) -> None:
    """
    Rimi often hides <main> until hydration; or renders cards lazily.
    """
    try:
        page.wait_for_function(
            """() => {
                const main = document.querySelector('main');
                const hidden = main && getComputedStyle(main).visibility === 'hidden';
                const cards = document.querySelector('.js-product-container a.card__url, a[href*="/p/"]');
                return (main && !hidden) || !!cards;
            }""",
            timeout=timeout_ms
        )
    except Exception:
        pass

# ----------------------------- price extraction ------------------------------

def _norm_price_text(txt: str) -> Optional[str]:
    if not txt:
        return None
    txt = txt.replace("\xa0", " ").replace(",", ".")
    # "3 99"  or any non-digit separator between int and decimals
    m = PRICE_FRAGMENT_RE.search(txt)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    # plain decimal near euro
    m2 = re.search(r"(\d+(?:\.\d{1,2}))\s*€?", txt)
    return m2.group(1) if m2 else None

def extract_price_pw(page) -> Tuple[Optional[str], Optional[str]]:
    """Strong PDP price finder: UI spans -> JSON-LD offers -> meta -> brute text."""
    # 1) UI blocks
    try:
        got = page.evaluate("""
        () => {
          const pickText = (sel) => {
            const el = document.querySelector(sel);
            return el ? el.textContent : '';
          };
          const blocks = [
            '[data-test="product-price"]',
            '[class*="price"]',
            '.price', '.product-price', '.price__main'
          ];
          for (const b of blocks) {
            const el = document.querySelector(b);
            if (el) return el.textContent || '';
          }
          // integer / decimal split
          const i = pickText('[class*="price"] [class*="int"], .price [class*="int"]');
          const f = pickText('[class*="price"] [class*="dec"], .price [class*="dec"]');
          if (i || f) return (i || '') + ' ' + (f || '');
          return '';
        }
        """)
        val = _norm_price_text(got or "")
        if val:
            return val, "EUR"
    except Exception:
        pass

    # 2) JSON-LD offers
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(10, scripts.count())):
            raw = scripts.nth(i).inner_text()
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            seq = obj if isinstance(obj, list) else [obj]
            for d in seq:
                if isinstance(d, dict) and "offers" in d:
                    off = d["offers"]
                    if isinstance(off, list):
                        off = off[0]
                    price = str(off.get("price") or "").replace(",", ".").strip()
                    cur = (off.get("priceCurrency") or "EUR").strip() or "EUR"
                    if price:
                        return price, cur
    except Exception:
        pass

    # 3) meta fallbacks
    try:
        val = page.locator('meta[itemprop="price"]').first.get_attribute("content") or ""
        val = val or page.locator('meta[property="product:price:amount"]').first.get_attribute("content") or ""
        val = (val or "").strip()
        if val:
            return val.replace(",", "."), "EUR"
    except Exception:
        pass

    # 4) last resort: visible text
    try:
        txt = page.text_content() or ""
        m = re.search(r"(\d+[ \u00A0]\d{2})\s*€", txt)
        if m:
            val = _norm_price_text(m.group(1))
            if val:
                return val, "EUR"
        m2 = re.search(r"(\d+[.,]\d{1,2})\s*€", txt)
        if m2:
            return m2.group(1).replace(",", "."), "EUR"
    except Exception:
        pass

    return None, None

# ----------------------------- PDP parsing utils -----------------------------

def parse_price_from_dom_or_meta_soup(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for tag in soup.select('meta[itemprop="price"], [itemprop="price"]'):
        val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
        if val:
            return val.replace(",", "."), "EUR"
    m = MONEY_RE.search(soup.get_text(" ", strip=True))
    if m:
        return m.group(1).replace(",", "."), "EUR"
    return None, None

def parse_brand_and_size(soup: BeautifulSoup, name: str) -> Tuple[Optional[str], Optional[str]]:
    brand = size_text = None
    for row in soup.select("table tr"):
        th, td = row.find("th"), row.find("td")
        if not th or not td:
            continue
        key = th.get_text(" ", strip=True).lower()
        val = td.get_text(" ", strip=True)
        if (not brand) and ("tootja" in key or "brand" in key):
            brand = val
        if (not size_text) and any(k in key for k in ("kogus","maht","netokogus","pakend","neto","suurus")):
            size_text = val
    if not size_text:
        m = re.search(r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk))\b', name or "")
        if m: size_text = m.group(1).replace("L", "l")
    return brand, size_text

def extract_ext_id(url: str) -> str:
    try:
        parts = urlparse(url).path.rstrip("/").split("/")
        if "p" in parts:
            i = parts.index("p")
            return parts[i+1]
    except Exception:
        pass
    return ""

def parse_jsonld_and_microdata(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    ean = sku = None
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        try:
            data = json.loads(tag.text)
        except Exception:
            continue
        seq = data if isinstance(data, list) else [data]
        for d in seq:
            got = deep_find_kv(d, { *EAN_KEYS, *SKU_KEYS })
            ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
            sku = sku or got.get("sku") or got.get("mpn") or got.get("code")
    if not ean:
        for it in ("gtin13","gtin","ean","ean13","barcode"):
            meta = soup.find(attrs={"itemprop": it})
            if meta:
                ean = ean or (meta.get("content") or meta.get_text(strip=True))
    if not sku:
        for it in ("sku","mpn"):
            meta = soup.find(attrs={"itemprop": it})
            if meta:
                sku = sku or (meta.get("content") or meta.get_text(strip=True))
    return ean, sku

def parse_visible_for_ean(soup: BeautifulSoup) -> Optional[str]:
    for el in soup.find_all(string=EAN_LABEL_RE):
        seg = el.parent.get_text(" ", strip=True) if el and el.parent else str(el)
        m = EAN_RE.search(seg)
        if m: return m.group(0)
    m = EAN_RE.search(soup.get_text(" ", strip=True))
    return m.group(0) if m else None

# ----------------------------- link collectors -------------------------------

def collect_pdp_links(page) -> List[str]:
    sels = [
        ".js-product-container a.card__url",
        "a[href*='/p/']",
        "a[href^='/epood/ee/p/']",
        "a[href^='/epood/ee/tooted/'][href*='/p/']",
        "[data-test*='product'] a[href*='/p/']",
        ".product-card a[href*='/p/']",
    ]
    hrefs: set[str] = set()
    for sel in sels:
        try:
            for el in page.locator(sel).all():
                h = normalize_href(el.get_attribute("href"))
                if h and "/p/" in h:
                    hrefs.add(h)
        except Exception:
            pass
    return sorted(hrefs)

def collect_subcategory_links(page, base_cat_url: str) -> List[str]:
    sels = [
        "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
        ".category-card a[href^='/epood/ee/tooted/']",
        ".category, .subcategory a[href^='/epood/ee/tooted/']",
        "nav a[href^='/epood/ee/tooted/']",
        "a[href^='/epood/ee/tooted/']:not([href*='/p/'])",
    ]
    hrefs: set[str] = set()
    for sel in sels:
        try:
            for el in page.locator(sel).all():
                h = normalize_href(el.get_attribute("href"))
                if h and "/epood/ee/tooted/" in h and "/p/" not in h:
                    hrefs.add(h)
        except Exception:
            pass
    hrefs.discard(base_cat_url.split("?")[0].split("#")[0])
    return sorted(hrefs)

# -------------------------- DB preload for skip list -------------------------

def _db_connect():
    """Return (driver_name, connection) using pg8000 (preferred) or psycopg2."""
    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        return None, None
    from urllib.parse import urlparse
    u = urlparse(dburl)
    user, password, host = u.username, u.password, u.hostname or "localhost"
    port, database = int(u.port or 5432), (u.path or "/postgres").lstrip("/")
    try:
        import pg8000.dbapi as pg8000  # type: ignore
        conn = pg8000.connect(user=user, password=password, host=host, port=port, database=database)
        return "pg8000", conn
    except Exception as e_pg:
        try:
            import psycopg2  # type: ignore
            conn = psycopg2.connect(user=user, password=password, host=host, port=port, dbname=database, connect_timeout=10)
            return "psycopg2", conn
        except Exception as e_psy:
            print(f"[rimi] DB drivers failed: pg8000={e_pg}; psycopg2={e_psy}")
            return None, None

def preload_ext_ids_from_db() -> set[str]:
    seen: set[str] = set()
    if not (os.getenv("PRELOAD_DB", "1") == "1" and os.getenv("DATABASE_URL")):
        return seen
    q = os.getenv("PRELOAD_DB_QUERY") or """
        SELECT DISTINCT ext_id
        FROM (
          SELECT ext_id FROM public.rimi_candidates
          UNION
          SELECT ext_id FROM public.staging_rimi_products
        ) u
        WHERE ext_id IS NOT NULL AND ext_id <> ''
    """
    drv, conn = _db_connect()
    if not conn:
        return seen
    try:
        cur = conn.cursor()
        cur.execute(q)
        rows = cur.fetchall()
        for r in rows:
            if not r: continue
            raw = str(r[0]).strip()
            if raw: seen.add(raw)
        try: cur.close(); conn.close()
        except Exception: pass
        print(f"[rimi] preloaded {len(seen)} ext_id(s) from DB ({drv})")
    except Exception as e:
        print(f"[rimi] DB preload query failed: {type(e).__name__}: {e}")
        try: conn.close()
        except Exception: pass
    return seen

# ------------------------------- crawler -------------------------------------

def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float,
                   skip_known: bool, known_ext: set[str]) -> List[str]:
    browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
    ctx = browser.new_context(
        locale="et-EE",
        viewport={"width":1440, "height":900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124 Safari/537.36"),
    )
    page = ctx.new_page()
    visited: set[str] = set()
    q: List[str] = [normalize_href(cat_url) or cat_url]
    all_pdps: List[str] = []

    try:
        while q:
            cat = q.pop(0)
            if not cat or cat in visited:
                continue
            visited.add(cat)

            try:
                page.goto(cat, timeout=45000, wait_until="domcontentloaded")
            except Exception:
                continue
            auto_accept_overlays(page)
            wait_for_hydration(page)

            # enqueue subcategories
            for sc in collect_subcategory_links(page, cat):
                if sc not in visited:
                    q.append(sc)

            pages_seen = 0
            last_total = -1
            while True:
                page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                raw_links = collect_pdp_links(page)

                # skip known if requested
                if skip_known and known_ext:
                    links = [u for u in raw_links if _ext_from_href(u) not in known_ext]
                else:
                    links = raw_links

                all_pdps.extend(links)

                # pagination: next button / load more / scroll
                clicked = False
                for sel in [
                    "a[rel='next']",
                    "button[aria-label*='Järgmine']",
                    "button:has-text('Järgmine')",
                    "button:has-text('Kuva rohkem')",
                    "button:has-text('Laadi rohkem')",
                    "a:has-text('Järgmine')",
                ]:
                    if page.locator(sel).count() > 0:
                        try:
                            page.locator(sel).first.click(timeout=3000)
                            clicked = True
                            page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                            break
                        except Exception:
                            pass

                if not clicked:
                    before = len(collect_pdp_links(page))
                    for _ in range(3):
                        page.mouse.wheel(0, 2400)
                        page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                    after = len(collect_pdp_links(page))
                    if after <= before:
                        break

                pages_seen += 1
                if page_limit and pages_seen >= page_limit:
                    break

                if len(all_pdps) == last_total:
                    page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                last_total = len(all_pdps)

    finally:
        ctx.close(); browser.close()

    # dedupe preserving order
    seen, out = set(), []
    for u in all_pdps:
        if u and u not in seen:
            seen.add(u); out.append(u)
    return out

# ------------------------------- PDP parser ----------------------------------

def parse_pdp(pw, url: str, headless: bool, req_delay: float) -> Dict[str,str]:
    browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
    ctx = browser.new_context(
        locale="et-EE",
        viewport={"width":1440,"height":900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
    )
    page = ctx.new_page()

    name = brand = size_text = image_url = category_path = ""
    ean = sku = price = currency = None
    ext_id_from_attr = ""

    try:
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        auto_accept_overlays(page)
        wait_for_hydration(page)
        page.wait_for_timeout(int(req_delay*1000))

        # Try to read quick data from any product card container
        try:
            card = page.locator(".js-product-container").first
            if card.count() > 0:
                raw = card.get_attribute("data-gtm-eec-product")
                if raw:
                    try:
                        eec = json.loads(raw)
                        if isinstance(eec, dict):
                            price = str(eec.get("price")) if eec.get("price") is not None else price
                            currency = eec.get("currency") or currency
                            brand = eec.get("brand") or brand
                            sku = sku or str(eec.get("id") or "")
                    except Exception:
                        pass
                dp = card.get_attribute("data-product-code")
                if dp:
                    ext_id_from_attr = dp.strip()
        except Exception:
            pass

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        # name, image
        h1 = soup.find("h1")
        if h1: name = h1.get_text(strip=True)
        ogimg = soup.find("meta", {"property":"og:image"})
        if ogimg and ogimg.get("content"):
            image_url = normalize_href(ogimg.get("content"))
        else:
            img = soup.find("img")
            if img:
                image_url = normalize_href(img.get("src") or img.get("data-src") or "")

        # breadcrumb
        crumbs = [a.get_text(strip=True) for a in soup.select("nav a, .breadcrumb a") if a.get_text(strip=True)]
        if crumbs:
            crumbs = [c for c in crumbs if c]
            category_path = " > ".join(crumbs[-5:])

        # brand & size
        b2, s2 = parse_brand_and_size(soup, name or "")
        brand = brand or b2
        size_text = size_text or s2

        # ean/sku via ld+json/microdata and globals
        e1, s1 = parse_jsonld_and_microdata(soup)
        ean = ean or e1; sku = sku or s1

        for glb in ["__NUXT__","__NEXT_DATA__","APP_STATE","dataLayer","Storefront","CART_CONFIG"]:
            try:
                data = page.evaluate(f"window['{glb}']")
                if data:
                    got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS, "price", "currency" })
                    ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
                    sku = sku or got.get("sku") or got.get("mpn") or got.get("code") or got.get("id")
                    if not price and ("price" in got): price = got.get("price")
                    if not currency and ("currency" in got): currency = got.get("currency")
            except Exception:
                pass

        if not ean:
            e2 = parse_visible_for_ean(soup)
            if e2: ean = e2

        # price robust fallback
        if not price:
            p2, c2 = extract_price_pw(page)
            if p2:
                price, currency = p2, c2 or "EUR"

        # if still nothing, try soup-based meta/text
        if not price:
            p3, c3 = parse_price_from_dom_or_meta_soup(soup)
            if p3:
                price, currency = p3, c3

    except PWTimeout:
        name = name or ""
    finally:
        ctx.close(); browser.close()

    # prefer URL /p/<id>, else data-product-code
    ext_id = extract_ext_id(url) or ext_id_from_attr

    # normalize price string
    if isinstance(price, (int, float)):
        price = f"{float(price):.2f}"
    elif isinstance(price, str):
        price = _norm_price_text(price) or price

    return {
        "store_chain": STORE_CHAIN,
        "store_name": STORE_NAME,
        "store_channel": STORE_CHANNEL,
        "ext_id": (ext_id or "").strip(),
        "ean_raw": (ean or "").strip(),
        "sku_raw": (sku or "").strip(),
        "name": (name or "").strip(),
        "size_text": (size_text or "").strip(),
        "brand": (brand or "").strip(),
        "manufacturer": "",
        "price": (str(price) if price is not None else "").strip(),
        "currency": (currency or "").strip(),
        "image_url": (image_url or "").strip(),
        "category_path": (category_path or "").strip(),
        "category_leaf": category_path.split(" > ")[-1] if category_path else "",
        "source_url": url.split("?")[0],
    }

# ---------------------------------- main -------------------------------------

def _safe_int(s: Optional[str], default: int) -> int:
    try: return int(str(s))
    except Exception: return default

def main():
    # --- CLI ---
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--cats-file")
    p.add_argument("--page-limit")
    p.add_argument("--max-products")
    p.add_argument("--headless")
    p.add_argument("--req-delay")
    p.add_argument("--output-csv")
    try:
        args, _ = p.parse_known_args()
    except SystemExit:
        # never exit 2 in CI
        class A: pass
        args = A()
        args.cats_file = None
        args.page_limit = None
        args.max_products = None
        args.headless = None
        args.req_delay = None
        args.output_csv = None

    cats_file   = args.cats_file or os.getenv("CATS_FILE") or "data/rimi_categories.txt"
    page_limit  = _safe_int(args.page_limit or os.getenv("PAGE_LIMIT"), 0)
    max_products= _safe_int(args.max_products or os.getenv("MAX_PRODUCTS"), 0)
    headless    = (str(args.headless or os.getenv("HEADLESS", "1")) == "1")
    req_delay   = float(args.req_delay or os.getenv("REQ_DELAY") or "0.5")
    out_csv     = args.output_csv or os.getenv("OUTPUT_CSV") or "data/rimi_products.csv"

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)

    # categories
    cats: List[str] = []
    if os.path.exists(cats_file):
        with open(cats_file, "r", encoding="utf-8") as f:
            for ln in f:
                ln = (ln or "").strip()
                if ln and not ln.startswith("#"):
                    cats.append(ln)
    if not cats:
        print("[rimi] No categories found in", cats_file)
        return 0

    # preload known ext_ids only if skipping is requested
    skip_known = (os.getenv("SKIP_KNOWN", "0") == "1")
    known_ext = preload_ext_ids_from_db() if skip_known else set()

    total_written = 0
    fields = [
        "store_chain","store_name","store_channel",
        "ext_id","ean_raw","sku_raw","name","size_text","brand","manufacturer",
        "price","currency","image_url","category_path","category_leaf","source_url",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as fcsv:
        w = csv.DictWriter(fcsv, fieldnames=fields)
        w.writeheader()
        try:
            with sync_playwright() as pw:
                all_pdps: List[str] = []
                for ci, cat in enumerate(cats, 1):
                    links = crawl_category(pw, cat, page_limit, headless, req_delay,
                                           skip_known, known_ext)
                    before = len(all_pdps)
                    for u in links:
                        if u not in all_pdps:
                            all_pdps.append(u)
                    print(f"[rimi] {cat} → +{len(all_pdps)-before} products (total so far: {len(all_pdps)})")
                    if max_products and len(all_pdps) >= max_products:
                        all_pdps = all_pdps[:max_products]
                        break

                # Visit PDPs
                for i, url in enumerate(all_pdps, 1):
                    row = parse_pdp(pw, url, headless, req_delay)
                    if not row or not row.get("ext_id") or not row.get("name"):
                        continue
                    # write even if price missing; DB step can handle it
                    w.writerow(row); total_written += 1
                    if (i % 25) == 0:
                        fcsv.flush()
                fcsv.flush()
        except KeyboardInterrupt:
            pass
        except Exception:
            traceback.print_exc()

    print(f"[rimi] wrote {total_written} product rows.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

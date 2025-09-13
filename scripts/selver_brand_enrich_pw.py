#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (PDP, structured-only; navigates search → PDP).

- Accepts rows directly from products (+ ext_product_map for Selver).
- For each item, opens the PDP (direct URL if mapped; otherwise searches),
  then extracts brand from:
    • Käitleja / Tootja / Valmistaja / Kaubamärk / Brand rows (th/dt → td/dd)
    • JSON-LD (brand/manufacturer) and <meta property="product:brand">
- Never guesses from product title.
- Normalizes "Määramata", "Määrmata" → "Määramata".

ENV
  DATABASE_URL         required
  MAX_ITEMS            default 500
  HEADLESS             1/0, default 1
  REQ_DELAY            seconds between items (default 0.25)
  TIMEBOX_SECONDS      soft wall time for whole run (default 1800)
  OVERWRITE_PRODUCTS   1=overwrite existing product.brand (else only fill empties/garbage)
"""

from __future__ import annotations
import os, re, json, time, signal, sys, random
import psycopg2
import psycopg2.extras
from contextlib import closing
from typing import Optional, Tuple, List
from urllib.parse import quote_plus, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "https://www.selver.ee"
SEARCH_URL = BASE + "/search?q={q}"

HEADLESS = os.getenv("HEADLESS", "1") == "1"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.25"))
TIMEBOX = int(os.getenv("TIMEBOX_SECONDS", "1800"))
OVERWRITE = os.getenv("OVERWRITE_PRODUCTS", "0") in ("1", "true", "True", "YES", "yes")

# --- brand labels / helpers ---------------------------------------------------
LABEL_RX = re.compile(r'(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)', re.I)

def _clean(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)          # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _normalize_brand(b: str) -> str:
    if not b:
        return ""
    t = b.strip()
    # common misspelling seen on site occasionally
    if re.fullmatch(r'mää?ramata', t, re.I):
        return "Määramata"
    return t

def _is_bad_brand(b: Optional[str]) -> bool:
    if not b: return True
    b = b.strip()
    if not b: return True
    if len(b) > 100: return True
    if re.search(r'(http|www\.)', b, re.I): return True
    if '@' in b: return True
    if b.lower().startswith("e-selveri info"): return True
    return False

# --- navigation helpers (ported from EAN probe) --------------------------------
BLOCK_SUBSTR = (
    "adobedtm", "typekit", "use.typekit.net", "googletagmanager", "google-analytics",
    "doubleclick", "facebook.net", "newrelic", "pingdom", "cookiebot", "hotjar",
)

def _router(route, request):
    try:
        url = request.url.lower()
        if any(s in url for s in BLOCK_SUBSTR):
            return route.abort()
    except Exception:
        pass
    return route.continue_()

def kill_overlays(page):
    for sel in [
        "button:has-text('Nõustun')", "button:has-text('Luba kõik')",
        "button:has-text('Accept all')", "button:has-text('Accept')",
        "[data-testid='uc-accept-all-button']",
    ]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=500)
                time.sleep(0.1)
        except Exception:
            pass
    try: page.keyboard.press("Escape")
    except Exception: pass

def looks_like_pdp_href(href: str) -> bool:
    if not href: return False
    href = href.split("?", 1)[0].split("#", 1)[0]
    if href.startswith("http"):
        try: href = (urlparse(href).path) or "/"
        except Exception: return False
    if not href.startswith("/"): return False

    bad = ("/search", "/konto", "/login", "/registreeru", "/logout", "/kliendimangud",
           "/kauplused", "/selveekspress", "/tule-toole", "/uudised", "/kinkekaardid",
           "/selveri-kook", "/kampaania", "/retseptid", "/app")
    if any(href.startswith(p) for p in bad): return False

    if href.startswith("/toode/") or href.startswith("/e-selver/toode/"):
        return True

    segs = [s for s in href.split("/") if s]
    if not segs: return False
    last = segs[-1]
    if "-" in last and (any(ch.isdigit() for ch in last) or re.search(r"(?:^|[-_])(kg|g|l|ml|tk)$", last)):
        return True
    return False

def _candidate_anchors(page):
    sels = [
        "[data-testid='product-grid'] a[href]:visible",
        "[data-testid='product-list'] a[href]:visible",
        ".product-list a[href]:visible",
        ".product-grid a[href]:visible",
        ".product-card a[href]:visible",
        "article a[href]:visible",
        "[data-href]:visible",
        "a[href^='/']:visible",
    ]
    out = []
    for s in sels:
        try: out.extend(page.locator(s).all())
        except Exception: pass
    return out[:200]

def open_first_search_tile(page) -> bool:
    links = _candidate_anchors(page)
    for a in links:
        try:
            href = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if not looks_like_pdp_href(href): continue
            a.click(timeout=1200)
            try: page.wait_for_selector("h1", timeout=2500)
            except Exception: pass
            try: page.wait_for_load_state("networkidle", timeout=2500)
            except Exception: pass
            return True
        except Exception:
            continue
    # fallback: navigate by href
    for a in links:
        try:
            href = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if not looks_like_pdp_href(href): continue
            page.goto(href if href.startswith("http") else BASE + href, timeout=15000, wait_until="domcontentloaded")
            return True
        except Exception:
            continue
    return False

def search_and_open(page, query: str) -> bool:
    try:
        page.goto(SEARCH_URL.format(q=quote_plus(query)), timeout=15000, wait_until="domcontentloaded")
        kill_overlays(page)
        # wait for product grid/list to mount a little
        try: page.wait_for_selector("[data-testid='product-grid'], .product-list, a[href^='/']", timeout=4000)
        except Exception: pass
        if open_first_search_tile(page):
            return True
    except Exception:
        pass
    return False

def ensure_specs_open(page):
    for sel in ["button:has-text('Tooteinfo')", "button:has-text('Lisainfo')", "button:has-text('Tootekirjeldus')"]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=500)
                time.sleep(0.12)
        except Exception:
            pass

# --- brand extraction ----------------------------------------------------------
def extract_brand(page) -> str:
    # direct th/dt → td/dd pairs
    sel_pairs = [
        'th:has(:text-matches("Käitleja","i")) + td',
        'th:has(:text-matches("Tootja","i")) + td',
        'th:has(:text-matches("Valmistaja","i")) + td',
        'th:has(:text-matches("Kaubamärk","i")) + td',
        'th:has(:text-matches("Brand","i")) + td',
        'dt:has(:text-matches("Käitleja","i")) + dd',
        'dt:has(:text-matches("Tootja","i")) + dd',
        'dt:has(:text-matches("Valmistaja","i")) + dd',
        'dt:has(:text-matches("Kaubamärk","i")) + dd',
        'dt:has(:text-matches("Brand","i")) + dd',
    ]
    try:
        for sel in sel_pairs:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                txt = _clean(loc.inner_text(timeout=800))
                if txt:
                    return _normalize_brand(txt)
    except Exception:
        pass

    # JSON-LD
    try:
        for el in page.locator('script[type="application/ld+json"]').all():
            txt = el.inner_text(timeout=400) or ''
            if not txt.strip(): continue
            try: data = json.loads(txt)
            except Exception: continue
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                b = n.get('brand')
                if isinstance(b, dict): b = b.get('name')
                b = _clean(b)
                if b: return _normalize_brand(b)
                m = n.get('manufacturer')
                if isinstance(m, dict): m = m.get('name')
                m = _clean(m)
                if m: return _normalize_brand(m)
    except Exception:
        pass

    # meta product:brand
    try:
        val = page.locator('meta[property="product:brand"]').first.get_attribute("content", timeout=300) or ""
        val = _clean(val)
        if val:
            return _normalize_brand(val)
    except Exception:
        pass

    # brute force HTML pairs
    try:
        html = page.content()
        for k, v in re.findall(r'(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html):
            if LABEL_RX.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b: return _normalize_brand(b)
        for k, v in re.findall(r'(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html):
            if LABEL_RX.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b: return _normalize_brand(b)
    except Exception:
        pass

    return ""

# --- DB helpers ----------------------------------------------------------------
def pick_rows(conn, limit: int):
    """Prefer products that have a Selver URL mapping; fallback to Selver-priced ones."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        where = "TRUE" if OVERWRITE else "(p.brand IS NULL OR p.brand = '' OR length(p.brand)>100 OR p.brand ~ '(http|www\\.)' OR p.brand ~ '@' OR p.brand ILIKE 'e-selveri info%')"

        cur.execute(
            f"""
            SELECT DISTINCT ON (p.id)
                   p.id, p.name, p.ean,
                   COALESCE(NULLIF(p.brand,''),'') AS brand,
                   m.ext_id AS selver_url
            FROM products p
            JOIN ext_product_map m ON m.product_id = p.id AND m.source='selver'
            WHERE {where}
            ORDER BY p.id
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        if rows:
            return rows

        # fallback if mapping missing
        cur.execute(
            f"""
            SELECT DISTINCT p.id, p.name, p.ean,
                   COALESCE(NULLIF(p.brand,''),'') AS brand,
                   NULL::text AS selver_url
            FROM products p
            JOIN prices pr ON pr.product_id=p.id
            JOIN stores s  ON s.id = pr.store_id AND s.chain='Selver'
            WHERE {where}
            ORDER BY p.id
            LIMIT %s;
            """,
            (limit,),
        )
        return cur.fetchall()

def save_brand(conn, product_id: int, brand: str):
    brand = _normalize_brand(brand)
    with conn.cursor() as cur:
        if OVERWRITE:
            cur.execute("UPDATE products SET brand = %s WHERE id = %s;", (brand, product_id))
        else:
            cur.execute(
                """
                UPDATE products
                   SET brand = %s
                 WHERE id = %s
                   AND (brand IS NULL OR brand = '' OR length(brand)>100 OR brand ~ '(http|www\\.)' OR brand ~ '@' OR brand ILIKE 'e-selveri info%');
                """,
                (brand, product_id),
            )
    conn.commit()

# --- main ----------------------------------------------------------------------
def process_one(page, row) -> Tuple[bool, str]:
    """Returns (ok, url_tried)"""
    pid = row["id"]
    url = (row.get("selver_url") or "").strip()
    tried = url

    try:
        if url:
            page.goto(url, timeout=15000, wait_until="domcontentloaded")
            kill_overlays(page)
            ensure_specs_open(page)
            b = extract_brand(page)
            if b:
                return True, page.url
        # fallback by search (EAN is perfect, otherwise name)
        q = (row.get("ean") or "").strip() or (row.get("name") or "").strip()
        if not q:
            return False, tried or "<no url>"
        if search_and_open(page, q):
            kill_overlays(page)
            ensure_specs_open(page)
            b = extract_brand(page)
            if b:
                return True, page.url
            return False, page.url
    except PWTimeout:
        pass
    except Exception:
        pass
    return False, tried or (SEARCH_URL.format(q=quote_plus((row.get("ean") or row.get("name") or ""))))

def main():
    dsn = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_PUBLIC")
    if not dsn:
        print("Missing DATABASE_URL", file=sys.stderr); sys.exit(2)

    deadline = time.time() + TIMEBOX
    with closing(psycopg2.connect(dsn)) as conn:
        rows = pick_rows(conn, MAX_ITEMS)
        if not rows:
            print("Nothing to do."); return

    found = 0; processed = 0
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(
            locale="et-EE",
            extra_http_headers={"Accept-Language":"et-EE,et;q=0.9,en;q=0.8,ru;q=0.7"},
            viewport={"width":1280,"height":900},
        )
        ctx.route("**/*", _router)
        page = ctx.new_page()
        page.set_default_timeout(8000)
        page.set_default_navigation_timeout(20000)

        with closing(psycopg2.connect(dsn)) as conn2:
            for r in rows:
                if time.time() > deadline:
                    print("Timebox reached, stopping."); break
                ok, tried_url = process_one(page, r)
                processed += 1
                if ok:
                    # re-extract on current PDP after process_one
                    b = extract_brand(page)
                    if b and not _is_bad_brand(b):
                        try:
                            save_brand(conn2, r["id"], b)
                            found += 1
                            print(f'[BRAND] product_id={r["id"]} brand="{b}" url={page.url}')
                        except Exception as e:
                            conn2.rollback()
                            print(f'[DB_ERR] product_id={r["id"]} err={e}')
                    else:
                        print(f'[MISS_BRAND_EMPTY] product_id={r["id"]} url={page.url}')
                else:
                    print(f'[MISS_BRAND_EMPTY] product_id={r["id"]} url={tried_url}')
                time.sleep(max(0.05, REQ_DELAY + random.uniform(-0.08, 0.08)))

        try: browser.close()
        except Exception: pass
    print(f"Done. processed={processed} brand_found={found}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

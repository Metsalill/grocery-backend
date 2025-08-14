#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee FOOD & DRINKS scraper → direct Postgres upsert

- Crawls grocery categories under /tooted/ and /en/tooted/ (auto-discovers subcats)
- Filters to FOOD/DRINKS only (skips appliances, pets, cosmetics, etc.)
- Extracts Prisma product metadata (incl. EAN) and UPSERTs into Postgres
- Keyed by EAN so re-runs will keep the latest metadata

Run:
  pip install playwright psycopg2-binary
  python -m playwright install chromium
  python scripts/prisma_food_scrape_to_db.py --max-products 500 --headless 1
"""
from __future__ import annotations
import argparse, os, random, re, sys, time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import psycopg2, psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# -----------------------------------------------------------------------------
# Config
BASE = "https://prismamarket.ee"

# Start from both EN and ET roots + food-market so we don't miss locale switches
SEEDS = ["/en/tooted/", "/tooted/", "/en/food-market/", "/food-market/"]

# Paths
CATEGORY_PREFIXES = ("/en/tooted/", "/tooted/", "/en/food-market/", "/food-market/")
PRODUCT_PREFIXES  = ("/en/toode/", "/toode/")

# FOOD-ONLY whitelist (keeps grocery sections, excludes non-food)
_ALLOW_KEYS = (
    # high-level groceries
    "toit", "joog", "food-market",
    # common food areas
    "leivad", "küpsised", "kook", "saia", "bakery",
    "piim", "munad", "rasvad", "dairy", "eggs", "juust", "jogurt", "või",
    "puu", "köögivil", "fruit", "vegetable", "salat", "herb",
    "liha", "kana", "sealiha", "veis", "lamb", "kalkun", "hakkliha",
    "kala", "mereann", "tuna", "lõhe", "räim", "heering",
    "külmutatud", "frozen",
    "kuivtooted", "pasta", "riis", "teravili", "jahu", "suhkur",
    "õli", "maitseaine", "kastmed", "konserv", "konservid",
    "snack", "suupisted", "pähkl", "müsl",
    "valmistoit", "ready", "supp", "püree",
    "joogid", "vein", "õlu", "siider", "limonaad", "vesi", "mahl",
    "magus", "maiustused", "šokolaad",
)
_DENY_KEYS = (
    "kodumasinad", "elektroonika", "kodukaubad", "kodu", "sisustus",
    "kosmeetika", "hügieen", "ilutooted",
    "riided", "jalatsid", "lapsed", "mänguasjad",
    "lemmikloom", "kass", "koer",
    "aia", "auto", "tööriist", "spord", "jõulud", "kingitused"
)

# -------- Amount / EAN patterns (enhanced) -----------------------------------
PACK_RE   = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIMPLE_RE = re.compile(r"\b(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
BONUS_RE  = re.compile(r"\+\s*\d+%")  # e.g. "500 g + 20%"
EAN_RE    = re.compile(r"(\d{8,14})$")

# -----------------------------------------------------------------------------
# Small utils
def jitter(a=0.6, b=1.4): time.sleep(random.uniform(a, b))
def clean(s: str | None) -> str: return re.sub(r"\s+", " ", s or "").strip()

def is_category_path(path: str) -> bool:
    return any(path.startswith(p) for p in CATEGORY_PREFIXES)

def is_product_path(path: str) -> bool:
    return any(path.startswith(p) for p in PRODUCT_PREFIXES)

def _has_any(s: str, keys: tuple[str, ...]) -> bool:
    s = s.lower()
    return any(k in s for k in keys)

def is_in_whitelist(url: str) -> bool:
    """
    Allow only FOOD/DRINKS categories.
    - Path must be a category path
    - Must contain an allow key (or be under food-market)
    - Must NOT contain a deny key
    """
    path = urlparse(url).path
    if not is_category_path(path):
        return False
    if _has_any(path, _DENY_KEYS):
        return False
    return "food-market" in path or _has_any(path, _ALLOW_KEYS)

# -----------------------------------------------------------------------------
# Food group mapper (normalize per-store categories)
def map_food_group(c1: str, c2: str, c3: str, title: str) -> str:
    t = " ".join([c1, c2, c3, title]).lower()
    def has(*keys): return any(k in t for k in keys)
    if has("joogid", "drink", "water", "juice", "soda", "beer", "wine", "kõvad joogid"): return "drinks"
    if has("leivad", "küpsised", "kook", "saia", "bakery", "pastry", "biscuit", "bread", "cake"): return "bakery"
    if has("piim", "juust", "kohuke", "kohupiim", "või", "jogurt", "dairy", "eggs", "munad", "cream"): return "dairy_eggs"
    if has("puu", "köögivil", "vegetable", "fruit", "salat", "herb"): return "produce"
    if has("liha", "meat", "kana", "chicken", "beef", "pork", "lamb", "veal", "ham", "saus"): return "meat"
    if has("kala", "fish", "lõhe", "räim", "heering", "tuna", "shrimp", "kammkarp", "mereann", "seafood"): return "fish"
    if has("külmutatud", "frozen"): return "frozen"
    if has("kuivtooted", "pasta", "riis", "rice", "jahu", "flour", "sugar", "suhkur", "oil", "õli",
           "konserv", "canned", "maitseaine", "spice", "kastme", "sauce", "teravili", "cereal", "snack", "pähkl", "müsl"): return "pantry"
    if has("valmistoit", "prepared", "ready"): return "prepared"
    return "other"

# -----------------------------------------------------------------------------
# DB
def get_database_url() -> str:
    try:
        import settings  # type: ignore
        db = getattr(settings, "DATABASE_URL", None)
        if db: return db
    except Exception:
        pass
    db = os.getenv("DATABASE_URL")
    if not db: raise RuntimeError("DATABASE_URL not set (env or settings.py)")
    return db

PRODUCTS_TABLE = os.getenv("PRODUCTS_TABLE", "products")

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PRODUCTS_TABLE} (
    id SERIAL PRIMARY KEY,
    ean TEXT UNIQUE,
    product_name TEXT,
    name TEXT,
    amount TEXT,
    brand TEXT,
    manufacturer TEXT,
    country_of_manufacture TEXT,
    category_1 TEXT,
    category_2 TEXT,
    category_3 TEXT,
    food_group TEXT,
    image_url TEXT,
    source_url TEXT,
    last_seen_utc TIMESTAMPTZ
);
"""
ADD_FOOD_GROUP_SQL = f"ALTER TABLE {PRODUCTS_TABLE} ADD COLUMN IF NOT EXISTS food_group TEXT;"
CREATE_FOOD_GROUP_INDEX_SQL = f"CREATE INDEX IF NOT EXISTS idx_{PRODUCTS_TABLE}_food_group ON {PRODUCTS_TABLE}(food_group);"

UPSERT_SQL = f"""
INSERT INTO {PRODUCTS_TABLE} (
    ean, product_name, name, amount, brand, manufacturer,
    country_of_manufacture, category_1, category_2, category_3,
    food_group, image_url, source_url, last_seen_utc
)
VALUES (
    %(ean)s, %(product_name)s, %(name)s, %(amount)s, %(brand)s, %(manufacturer)s,
    %(country_of_manufacture)s, %(category_1)s, %(category_2)s, %(category_3)s,
    %(food_group)s, %(image_url)s, %(source_url)s, %(last_seen_utc)s
)
ON CONFLICT (ean) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    name         = EXCLUDED.name,
    amount       = EXCLUDED.amount,
    brand        = EXCLUDED.brand,
    manufacturer = EXCLUDED.manufacturer,
    country_of_manufacture = EXCLUDED.country_of_manufacture,
    category_1   = EXCLUDED.category_1,
    category_2   = EXCLUDED.category_2,
    category_3   = EXCLUDED.category_3,
    food_group   = EXCLUDED.food_group,
    image_url    = EXCLUDED.image_url,
    source_url   = EXCLUDED.source_url,
    last_seen_utc= EXCLUDED.last_seen_utc
;
"""

def db_connect() -> PGConn:
    dsn = get_database_url()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(ADD_FOOD_GROUP_SQL)
        cur.execute(CREATE_FOOD_GROUP_INDEX_SQL)
    return conn

# -----------------------------------------------------------------------------
# Extraction helpers
def extract_title(page) -> str:
    try: return clean(page.locator("h1").first.inner_text())
    except Exception: return ""

def extract_image_url(page) -> str:
    sels = ["main img[alt][src]", "img[alt][src]", "img[src]"]
    for sel in sels:
        try:
            img = page.locator(sel).first
            if img.count() > 0:
                src = img.get_attribute("src")
                if src:
                    return urljoin(BASE, src)
        except Exception:
            continue
    return ""

def extract_label_value(page, labels: list[str]) -> str:
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[normalize-space(.)='{label}']").first
            if lab.count() > 0:
                sib = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception: pass
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
            if lab.count() > 0:
                sib = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception: pass
    try:
        for label in labels:
            dt = page.locator(f"xpath=//dt[normalize-space()='{label}'] | //dt[contains(normalize-space(),'{label}')]").first
            if dt.count() > 0:
                dd = dt.locator("xpath=following-sibling::dd[1]")
                if dd.count() > 0:
                    return clean(dd.inner_text())
    except Exception: pass
    try:
        html = page.content()
        for label in labels:
            m = re.search(fr"{re.escape(label)}\s*:?\s*</?[^>]*>?(.*?)<", html, re.I | re.S)
            if m:
                txt = re.sub(r"<[^>]+>", " ", m.group(1))
                txt = clean(txt)
                if txt: return txt
    except Exception: pass
    return ""

def extract_ean(page, url: str) -> str:
    val = extract_label_value(page, ["EAN", "EAN-kood", "Ribakood"])
    if val and re.fullmatch(r"\d{8,14}", val): return val
    m = EAN_RE.search(url)
    return m.group(1) if m else ""

def extract_country(page) -> str:
    return extract_label_value(page, [
        "Country of manufacture", "Country of origin", "Valmistajariik", "Päritoluriik"
    ]) or ""

def extract_manufacturer(page) -> str:
    return extract_label_value(page, [
        "Manufacturer", "Tootja", "Producer"
    ]) or ""

def extract_breadcrumbs(page) -> list[str]:
    sels = [
        "nav[aria-label='breadcrumb'] a",
        "nav.breadcrumb a",
        "ol.breadcrumb a",
        "a.breadcrumb__link",
        "nav a[href*='/tooted/']",
    ]
    texts = []
    for sel in sels:
        try:
            els = page.locator(sel)
            if els.count() > 0:
                for i in range(min(10, els.count())):
                    t = clean(els.nth(i).inner_text())
                    if t: texts.append(t)
                if texts: break
        except Exception: continue
    texts = [t for t in texts if t.lower() not in {"home", "avaleht"}]
    if not texts:
        try:
            scripts = page.locator("script[type='application/ld+json']")
            for i in range(scripts.count()):
                raw = scripts.nth(i).inner_text()
                if "BreadcrumbList" in raw:
                    import json
                    data = json.loads(raw)
                    if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
                        items = data.get("itemListElement", [])
                        texts = [clean(it.get("name", "")) for it in items if isinstance(it, dict)]
                        texts = [t for t in texts if t]
                        if texts: break
        except Exception: pass
    return texts[-3:]

def parse_amount_from_title(title: str) -> str:
    t = BONUS_RE.sub("", title)
    m = PACK_RE.search(t)
    if m:
        qty, num, unit = m.groups()
        num = num.replace(",", ".")
        return f"{qty}x{num} {unit}".replace(" .", " ")
    m = SIMPLE_RE.search(t)
    if m:
        num, unit = m.groups()
        num = num.replace(",", ".")
        return f"{num} {unit}"
    return ""

def infer_brand_from_title(title: str) -> str:
    parts = title.split()
    if not parts: return ""
    if len(parts) >= 2 and parts[0][:1].isupper() and parts[1][:1].isupper():
        return f"{parts[0]} {parts[1]}"
    return parts[0]

# -----------------------------------------------------------------------------
# Listing helpers
def paginate_listing(page, max_pages: int = 80):
    """
    Reveal more products via:
    - 'Load more' buttons
    - Infinite scroll
    - Classic next link
    Stops when no progress or max_pages reached.
    """
    def page_height():
        try:
            return page.evaluate("document.body.scrollHeight")
        except Exception:
            return 0

    load_more_selectors = [
        "button:has-text('Load more')",
        "button:has-text('Show more')",
        "button:has-text('Load More')",
        "[data-testid*='load'][data-testid*='more']",
        "button[aria-label*='more']",
    ]
    next_selectors = [
        "a[rel='next']",
        "a.pagination__next",
        "button:has-text('Next')",
        "a:has-text('Next')",
    ]

    pages_clicked = 0
    prev_h = -1

    while pages_clicked < max_pages:
        progressed = False

        # 1) 'Load more'
        for sel in load_more_selectors:
            try:
                btn = page.locator(sel)
                if btn.count() > 0 and btn.first.is_enabled():
                    btn.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter(0.6, 1.2)
                    new_h = page_height()
                    if new_h > prev_h:
                        prev_h = new_h
                        progressed = True
                        pages_clicked += 1
                        break
            except Exception:
                continue
        if progressed:
            continue

        # 2) Infinite scroll
        try:
            cur_h = page_height()
            page.mouse.wheel(0, 20000)
            jitter(0.5, 1.0)
            new_h = page_height()
            if new_h > cur_h:
                prev_h = new_h
                progressed = True
        except Exception:
            pass
        if progressed:
            continue

        # 3) Classic "next"
        for sel in next_selectors:
            try:
                nxt = page.locator(sel)
                if nxt.count() > 0 and nxt.first.is_enabled():
                    nxt.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter(0.6, 1.2)
                    new_h = page_height()
                    if new_h >= prev_h:
                        prev_h = new_h
                        progressed = True
                        pages_clicked += 1
                        break
            except Exception:
                continue
        if progressed:
            continue

        break  # no way to progress

def collect_links_from_listing(page, current_url: str) -> tuple[set[str], set[str]]:
    # reveal as many items as possible
    try:
        paginate_listing(page, max_pages=100)
    except Exception:
        pass

    # final deep scroll
    try:
        last_h = 0
        for _ in range(6):
            page.mouse.wheel(0, 20000); jitter(0.4, 0.9)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h: break
            last_h = h
    except Exception: pass

    anchors = page.locator("a[href]")
    prod, cats = set(), set()
    try: count = anchors.count()
    except PlaywrightTimeout: count = 0

    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href")
            if not href: continue
            url = urljoin(BASE, href)
            path = urlparse(url).path
            if is_product_path(path):
                prod.add(url)
            elif is_category_path(path) and is_in_whitelist(url):
                cats.add(url)
        except Exception: continue
    return prod, cats

# -----------------------------------------------------------------------------
# Main crawl → DB
def crawl_to_db(max_products: int = 500, headless: bool = True, discover_cap: int = 120):
    conn = db_connect()
    rows_written = 0
    skipped_no_ean = 0
    product_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = context.new_page()

        seen_categories = set()
        to_visit = [urljoin(BASE, s) for s in SEEDS]

        # Phase A: discover product links (food-only)
        while to_visit and len(product_urls) < max_products and len(seen_categories) < discover_cap:
            cat_url = to_visit.pop(0)
            if cat_url in seen_categories: continue
            if not is_in_whitelist(cat_url):  # safety (seed duplicates)
                continue
            seen_categories.add(cat_url)
            try:
                page.goto(cat_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded"); jitter()
            except PlaywrightTimeout:
                continue

            prod, cats = collect_links_from_listing(page, cat_url)
            product_urls.update(prod)
            for c in cats:
                if c not in seen_categories and c not in to_visit:
                    to_visit.append(c)

            print(f"[DISCOVER] {cat_url} → +{len(prod)} products, +{len(cats)} cats "
                  f"(totals: products={len(product_urls)}, seen={len(seen_categories)}, queue={len(to_visit)})")

            if len(product_urls) >= max_products:
                break

        # Phase B: visit products → UPSERT
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for url in list(product_urls)[:max_products]:
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded"); jitter()
                except PlaywrightTimeout:
                    continue

                title = extract_title(page)
                ean = extract_ean(page, url)
                if not ean:
                    skipped_no_ean += 1
                    continue
                amount = parse_amount_from_title(title)
                brand = infer_brand_from_title(title)
                manufacturer = extract_manufacturer(page)
                country = extract_country(page)
                image_url = extract_image_url(page)
                crumbs = extract_breadcrumbs(page)
                c1 = crumbs[0] if len(crumbs) > 0 else ""
                c2 = crumbs[1] if len(crumbs) > 1 else ""
                c3 = crumbs[2] if len(crumbs) > 2 else ""
                food_group = map_food_group(c1, c2, c3, title)

                rec = {
                    "ean": ean,
                    "product_name": title,
                    "name": title,  # legacy NOT NULL column
                    "amount": amount,
                    "brand": brand,
                    "manufacturer": manufacturer,
                    "country_of_manufacture": country,
                    "category_1": c1,
                    "category_2": c2,
                    "category_3": c3,
                    "food_group": food_group,
                    "image_url": image_url,
                    "source_url": url,
                    "last_seen_utc": datetime.now(timezone.utc),
                }
                try:
                    cur.execute(UPSERT_SQL, rec)
                    rows_written += 1
                except Exception as e:
                    print(f"UPSERT failed for EAN {ean}: {e}")
                    conn.rollback()
                else:
                    conn.commit()

        browser.close()

    # Run summary
    print(f"Discovered {len(product_urls)} product URLs (food-only).")
    print(f"Upserted {rows_written} rows into '{PRODUCTS_TABLE}'. Skipped (no EAN): {skipped_no_ean}.")

# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Prisma.ee FOOD & DRINKS → Postgres")
    ap.add_argument("--max-products", type=int, default=500)
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--discover-cap", type=int, default=120, help="Max categories to visit during discovery before visiting products")
    args = ap.parse_args()
    crawl_to_db(max_products=args.max_products, headless=bool(args.headless), discover_cap=int(args.discover_cap))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

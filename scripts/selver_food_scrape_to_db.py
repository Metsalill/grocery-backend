#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver.ee FOOD & DRINKS scraper → direct Postgres upsert (canonical schema)

Canonical columns written:
  <PRODUCTS_TABLE>(ean UNIQUE, name, size_text, brand, manufacturer,
                   country_of_manufacture, category_1..3, food_group,
                   image_url, source_url, last_seen_utc)

ENV / Args:
  DATABASE_URL           (required) Postgres DSN
  PRODUCTS_TABLE         (default: selver_products)
  SELVER_SEEDS_FILE      path to a text file with category URLs (one per line)
  --max-products         discovery cap (default 500)
  --headless 0|1         (default 1)
"""
from __future__ import annotations
import argparse
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ---------------------------------------------------------------------------
# Config (Selver)
BASE = "https://www.selver.ee"

# If you don’t provide SELVER_SEEDS_FILE, we’ll start from these high-level food roots.
DEFAULT_SEEDS = [
    "/liha-ja-kalatooted",
    "/piimatooted-ja-munad",
    "/puu-ja-koogiviljad",
    "/kuivained-ja-sai",
    "/valmistoit-ja-salatid",
    "/joogid",
    "/suupisted-ja-maiustused",
    "/kylmutatud-tooted",
]
PRODUCT_LINK_SEL = "a.product-item-link"  # card links in category listings

# -------- Amount / EAN patterns ---------------------------------------------
PACK_RE   = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIMPLE_RE = re.compile(r"\b(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
PIECES_RE = re.compile(r"\b(\d+)\s*(?:tk|pcs?|pk|pack)\b", re.I)
BONUS_RE  = re.compile(r"\+\s*\d+%")
EAN_RE    = re.compile(r"(\d{8,14})$")

# ---------------------------------------------------------------------------
# Small utils
def jitter(a=0.6, b=1.4):
    time.sleep(random.uniform(a, b))

def clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# ---------------------------------------------------------------------------
# Whitelist / blacklist filtering (reuse your Prisma list)
EXCLUDED_CATEGORY_KEYWORDS = [
    "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid","vaikesed-sisustuskaubad","kuunlad",
    "kook-ja-lauakatmine","uhekordsed-noud","kirja-ja-kontoritarbed","remondi-ja-turvatooted",
    "kulmutus-ja-kokkamisvahendid","omblus-ja-kasitootarbed","meisterdamine","ajakirjad","autojuhtimine",
    "kotid","aed-ja-lilled","lemmikloom","sport","pallimangud","jalgrattasoit","ujumine","matkamine",
    "tervisesport","manguasjad","lutid","lapsehooldus","ideed-ja-hooajad","kodumasinad","elektroonika",
    "meelelahutuselektroonika","vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
    "ilu-ja-tervis","kosmeetika","meigitooted","hugieen","loodustooted-ja-toidulisandid",
]
def is_in_whitelist(url: str) -> bool:
    """Accept Selver category paths that are food-related, reject obvious non-food branches."""
    path = urlparse(url).path.lower()
    if any(ex in path for ex in EXCLUDED_CATEGORY_KEYWORDS):
        return False
    # A loose allow rule: it’s a site-internal path without extension, not a search,
    # and not the cart/account/etc — and we’ll only enqueue if we also find product cards on the page.
    bad = ("/catalogsearch" in path or "/konto" in path or "/ostukorv" in path or "." in path)
    return not bad and path.count("/") >= 1

# ---------------------------------------------------------------------------
# Food group mapper (normalize per-store categories)
def map_food_group(c1: str, c2: str, c3: str, title: str) -> str:
    t = " ".join([c1, c2, c3, title]).lower()
    def has(*keys): return any(k in t for k in keys)
    if has("joogid","drink","water","juice","soda","beer","wine","kõvad joogid"): return "drinks"
    if has("leib","küpsis","kook","sai","bakery","pastry","biscuit","bread","cake"): return "bakery"
    if has("piim","juust","kohuke","kohupiim","või","jogurt","dairy","eggs","munad","cream"): return "dairy_eggs"
    if has("puu","köögivil","vegetable","fruit","salat","herb"): return "produce"
    if has("liha","meat","kana","chicken","beef","pork","lamb","veal","ham","saus"): return "meat"
    if has("kala","fish","lõhe","räim","heering","tuna","shrimp","mereann","seafood"): return "fish"
    if has("külmutatud","frozen"): return "frozen"
    if has("kuivtooted","pasta","riis","rice","jahu","flour","sugar","suhkur","oil","õli","konserv","canned",
            "maitseaine","spice","kastme","sauce","cereal","snack","pähkl","müsl"): return "pantry"
    if has("valmistoit","prepared","ready"): return "prepared"
    return "other"

# ---------------------------------------------------------------------------
# DB
def get_database_url() -> str:
    db = os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("DATABASE_URL not set")
    return db

PRODUCTS_TABLE = os.getenv("PRODUCTS_TABLE", "selver_products")

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PRODUCTS_TABLE} (
    id SERIAL PRIMARY KEY,
    ean TEXT,
    name TEXT,
    size_text TEXT,
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
CREATE_EAN_UNIQUE_SQL = f"""
CREATE UNIQUE INDEX IF NOT EXISTS uq_{PRODUCTS_TABLE}_ean
ON {PRODUCTS_TABLE} (ean)
WHERE ean IS NOT NULL AND ean <> '';
"""
CREATE_FOOD_GROUP_INDEX_SQL  = f"CREATE INDEX IF NOT EXISTS idx_{PRODUCTS_TABLE}_food_group  ON {PRODUCTS_TABLE}(food_group);"
CREATE_SOURCE_URL_INDEX_SQL  = f"CREATE INDEX IF NOT EXISTS idx_{PRODUCTS_TABLE}_source_url ON {PRODUCTS_TABLE}(source_url);"
CREATE_NAME_LOWER_IDX_SQL    = f"CREATE INDEX IF NOT EXISTS idx_{PRODUCTS_TABLE}_name_lower ON {PRODUCTS_TABLE}(LOWER(name));"

UPSERT_SQL = f"""
INSERT INTO {PRODUCTS_TABLE} (
    ean, name, size_text, brand, manufacturer,
    country_of_manufacture, category_1, category_2, category_3,
    food_group, image_url, source_url, last_seen_utc
)
VALUES (
    %(ean)s, %(name)s, %(size_text)s, %(brand)s, %(manufacturer)s,
    %(country_of_manufacture)s, %(category_1)s, %(category_2)s, %(category_3)s,
    %(food_group)s, %(image_url)s, %(source_url)s, %(last_seen_utc)s
)
ON CONFLICT (ean) DO UPDATE SET
    name         = COALESCE(NULLIF(EXCLUDED.name,''),         {PRODUCTS_TABLE}.name),
    size_text    = COALESCE(NULLIF(EXCLUDED.size_text,''),    {PRODUCTS_TABLE}.size_text),
    brand        = COALESCE(NULLIF(EXCLUDED.brand,''),        {PRODUCTS_TABLE}.brand),
    manufacturer = COALESCE(NULLIF(EXCLUDED.manufacturer,''), {PRODUCTS_TABLE}.manufacturer),
    country_of_manufacture = COALESCE(NULLIF(EXCLUDED.country_of_manufacture,''), {PRODUCTS_TABLE}.country_of_manufacture),
    category_1   = COALESCE(NULLIF(EXCLUDED.category_1,''),   {PRODUCTS_TABLE}.category_1),
    category_2   = COALESCE(NULLIF(EXCLUDED.category_2,''),   {PRODUCTS_TABLE}.category_2),
    category_3   = COALESCE(NULLIF(EXCLUDED.category_3,''),   {PRODUCTS_TABLE}.category_3),
    food_group   = COALESCE(NULLIF(EXCLUDED.food_group,''),   {PRODUCTS_TABLE}.food_group),
    image_url    = COALESCE(NULLIF(EXCLUDED.image_url,''),    {PRODUCTS_TABLE}.image_url),
    source_url   = COALESCE(NULLIF(EXCLUDED.source_url,''),   {PRODUCTS_TABLE}.source_url),
    last_seen_utc= EXCLUDED.last_seen_utc;
"""

def db_connect() -> PGConn:
    dsn = get_database_url()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_EAN_UNIQUE_SQL)
        cur.execute(CREATE_FOOD_GROUP_INDEX_SQL)
        cur.execute(CREATE_SOURCE_URL_INDEX_SQL)
        cur.execute(CREATE_NAME_LOWER_IDX_SQL)
    return conn

# ---------------------------------------------------------------------------
# Extraction helpers (Selver)

def extract_title(page) -> str:
    try:
        return clean(page.locator("h1").first.inner_text())
    except Exception:
        return ""

def extract_image_url(page) -> str:
    for sel in ["main img[alt][src]", ".product.media img[src]", "img[alt][src]", "img[src]"]:
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
    # exact
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[normalize-space(.)='{label}']").first
            if lab.count() > 0:
                sib = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception:
            pass
    # contains
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
            if lab.count() > 0:
                sib = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception:
            pass
    # dl/dt/dd
    try:
        for label in labels:
            dt = page.locator(f"xpath=//dt[normalize-space()='{label}'] | //dt[contains(normalize-space(),'{label}')]").first
            if dt.count() > 0:
                dd = dt.locator("xpath=following-sibling::dd[1]")
                if dd.count() > 0:
                    return clean(dd.inner_text())
    except Exception:
        pass
    # raw HTML fallback
    try:
        html = page.content()
        for label in labels:
            m = re.search(fr"{re.escape(label)}\s*:?\s*</?[^>]*>?(.*?)<", html, re.I | re.S)
            if m:
                txt = re.sub(r"<[^>]+>", " ", m.group(1))
                txt = clean(txt)
                if txt:
                    return txt
    except Exception:
        pass
    return ""

def extract_ean(page, url: str) -> str:
    # Selver uses "Ribakood" (ET) on the product page; JSON-LD often has gtin13.
    # Try JSON-LD first (quick), then labels, finally URL tail.
    try:
        import json
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(6, scripts.count())):
            raw = scripts.nth(i).inner_text()
            data = json.loads(raw)
            def find_gtin(obj):
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        if str(k).lower() in {"gtin13","gtin","sku","mpn"}:
                            s = clean(str(v))
                            if re.fullmatch(r"\d{8,14}", s): return s
                        r = find_gtin(v)
                        if r: return r
                elif isinstance(obj, list):
                    for it in obj:
                        r = find_gtin(it)
                        if r: return r
                return ""
            gt = find_gtin(data)
            if gt: return gt
    except Exception:
        pass

    val = extract_label_value(page, ["Ribakood", "EAN", "EAN-kood"])
    if val and re.fullmatch(r"\d{8,14}", val): return val

    m = EAN_RE.search(url)
    return m.group(1) if m else ""

def extract_country(page) -> str:
    return extract_label_value(page, [
        "Päritoluriik", "Valmistajariik",
        "Country of manufacture", "Country of origin"
    ]) or ""

def extract_manufacturer(page) -> str:
    return extract_label_value(page, ["Tootja", "Manufacturer", "Producer"]) or ""

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
    m = PIECES_RE.search(t)
    if m:
        return f"{m.group(1)} pcs"
    return ""

def normalize_size_text(s: str) -> str:
    if not s: return ""
    s = clean(s).lower().replace(",", ".")
    m = PACK_RE.search(s)
    if m:
        qty, num, unit = m.groups()
        return f"{int(qty)}x{num} {unit}"
    m = SIMPLE_RE.search(s)
    if m:
        num, unit = m.groups()
        return f"{num} {unit}"
    m = PIECES_RE.search(s)
    if m:
        return f"{m.group(1)} pcs"
    if re.search(r"\bkg\b", s): return "kg"
    if re.search(r"\bl\b", s):  return "l"
    return ""

def extract_size_text(page, title: str) -> str:
    lbl_val = extract_label_value(page, [
        "Netokogus","Neto kogus","Kogus","Maht","Pakendi suurus","Pakendi maht","Suurus","Kaal",
        "Net weight", "Net quantity", "Net content", "Volume", "Weight", "Size",
    ])
    size = normalize_size_text(lbl_val)
    if size:
        return size

    # JSON-LD scan for size-like fields
    try:
        import json
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(8, scripts.count())):
            raw = scripts.nth(i).inner_text()
            data = json.loads(raw)
            def walk(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(v, (str, int, float)) and str(k).lower() in {
                            "weight","netweight","size","contentsize","packagesize","volume","netcontent"
                        }:
                            cand = normalize_size_text(str(v))
                            if cand: return cand
                        r = walk(v)
                        if r: return r
                elif isinstance(obj, list):
                    for it in obj:
                        r = walk(it)
                        if r: return r
                return ""
            cand = walk(data)
            if cand:
                return cand
    except Exception:
        pass

    return normalize_size_text(parse_amount_from_title(title))

def infer_brand_from_title(title: str) -> str:
    parts = title.split()
    if not parts: return ""
    if len(parts) >= 2 and parts[0][:1].isupper() and parts[1][:1].isupper():
        return f"{parts[0]} {parts[1]}"
    return parts[0]

def extract_breadcrumbs(page) -> list[str]:
    """Best-effort breadcrumb capture on Selver product pages."""
    crumbs = []
    for sel in [
        "nav.breadcrumbs a",
        "ul.items li.item a",
        "nav[aria-label*='breadcrumb'] a",
    ]:
        try:
            items = page.locator(sel)
            n = min(items.count(), 10)
            for i in range(n):
                txt = clean(items.nth(i).inner_text())
                if txt and txt not in crumbs:
                    crumbs.append(txt)
            if crumbs:
                break
        except Exception:
            continue
    return crumbs[:3]

# ---------------------------------------------------------------------------
# Listing helpers (Selver)
def paginate_listing(page, max_pages: int = 80):
    # Try “Load more” / infinite scroll — then fall back to paging.
    def page_height():
        try:
            return page.evaluate("document.body.scrollHeight")
        except Exception:
            return 0

    load_more_selectors = [
        "button:has-text('Näita rohkem')", "button:has-text('Load more')", "button:has-text('Show more')",
        "[data-testid*='load'][data-testid*='more']", "button[aria-label*='more']",
    ]
    next_selectors = ["a[rel='next']", "a.pagination__next", "button[aria-label='Next page']"]

    pages_clicked = 0
    prev_h = -1

    while pages_clicked < max_pages:
        progressed = False
        for sel in load_more_selectors:
            try:
                btn = page.locator(sel)
                if btn.count() > 0 and btn.first.is_enabled():
                    btn.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter(0.5, 1.0)
                    new_h = page_height()
                    if new_h > prev_h:
                        prev_h = new_h
                        progressed = True
                        pages_clicked += 1
                        break
            except Exception:
                continue
        if progressed: continue

        try:
            cur_h = page_height()
            page.mouse.wheel(0, 20000)
            jitter(0.4, 0.9)
            new_h = page_height()
            if new_h > cur_h:
                prev_h = new_h
                progressed = True
        except Exception:
            pass
        if progressed: continue

        for sel in next_selectors:
            try:
                nxt = page.locator(sel)
                if nxt.count() > 0 and nxt.first.is_enabled():
                    nxt.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter(0.5, 1.0)
                    progressed = True
                    pages_clicked += 1
                    break
            except Exception:
                continue
        if progressed: continue

        break

def collect_links_from_listing(page, current_url: str) -> tuple[set[str], set[str]]:
    try:
        page.wait_for_selector(PRODUCT_LINK_SEL, timeout=6000)
    except Exception:
        pass

    try:
        paginate_listing(page, max_pages=100)
    except Exception:
        pass

    # Ensure loaded content
    try:
        last_h = 0
        for _ in range(6):
            page.mouse.wheel(0, 20000); jitter(0.3, 0.7)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h: break
            last_h = h
    except Exception:
        pass

    prod, cats = set(), set()

    # Product cards
    try:
        cards = page.locator(PRODUCT_LINK_SEL)
        for i in range(min(cards.count(), 2000)):
            href = cards.nth(i).get_attribute("href")
            if href:
                prod.add(urljoin(BASE, href))
    except Exception:
        pass

    # Side/nav category links (constrained by whitelist)
    for sel in ["aside a[href^='/']", "nav a[href^='/']", ".sidebar a[href^='/']"]:
        try:
            anchors = page.locator(sel)
            n = min(anchors.count(), 800)
            for i in range(n):
                href = anchors.nth(i).get_attribute("href")
                if not href: continue
                url = urljoin(BASE, href)
                if is_in_whitelist(url):
                    cats.add(url)
        except Exception:
            continue

    return prod, cats

# ---------------------------------------------------------------------------
# Main crawl → DB
def read_seeds() -> list[str]:
    p = os.getenv("SELVER_SEEDS_FILE")
    if p and os.path.isfile(p):
        with open(p, "r", encoding="utf-8") as f:
            seeds = [urljoin(BASE, clean(l)) for l in f if clean(l) and not l.startswith("#")]
            return seeds
    return [urljoin(BASE, s) for s in DEFAULT_SEEDS]

def crawl_to_db(max_products: int = 500, headless: bool = True):
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

        # Try accept cookies (best-effort)
        def accept_cookies(page):
            for sel in [
                "button:has-text('Nõustun')",
                "button:has-text('Nõustu')",
                "button:has-text('Accept')",
                "#CybotCookiebotDialogBodyLevelButtonAccept",
                "button[aria-label*='accept']",
            ]:
                try:
                    btn = page.locator(sel)
                    if btn.count() > 0 and btn.first.is_enabled():
                        btn.first.click()
                        page.wait_for_load_state("domcontentloaded")
                        jitter(0.2, 0.6)
                        return
                except Exception:
                    pass

        # Phase A: discover product links from category seeds
        seen_categories = set()
        to_visit = read_seeds()

        while to_visit and len(product_urls) < max_products:
            cat_url = to_visit.pop(0)
            if cat_url in seen_categories: 
                continue
            seen_categories.add(cat_url)

            try:
                page.goto(cat_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded"); jitter()
                accept_cookies(page)
            except PlaywrightTimeout:
                continue

            prod, cats = collect_links_from_listing(page, cat_url)
            product_urls.update(prod)
            for c in cats:
                if c not in seen_categories and c not in to_visit:
                    to_visit.append(c)

            print(f"[DISCOVER] {cat_url} → +{len(prod)} products, +{len(cats)} cats "
                  f"(totals: products={len(product_urls)}, queue={len(to_visit)})")

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

                size_text   = extract_size_text(page, title)
                brand       = infer_brand_from_title(title)
                manufacturer= extract_manufacturer(page)
                country     = extract_country(page)
                image_url   = extract_image_url(page)
                crumbs      = extract_breadcrumbs(page)
                c1 = crumbs[0] if len(crumbs) > 0 else ""
                c2 = crumbs[1] if len(crumbs) > 1 else ""
                c3 = crumbs[2] if len(crumbs) > 2 else ""
                food_group  = map_food_group(c1, c2, c3, title)

                rec = {
                    "ean": ean,
                    "name": title,
                    "size_text": size_text,
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

    print(f"Discovered {len(product_urls)} product URLs.")
    print(f"Upserted {rows_written} rows into '{PRODUCTS_TABLE}'. Skipped (no EAN): {skipped_no_ean}.")

# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Selver.ee FOOD & DRINKS → Postgres")
    ap.add_argument("--max-products", type=int, default=500)
    ap.add_argument("--headless", type=int, default=1)
    args = ap.parse_args()
    crawl_to_db(max_products=args.max_products, headless=bool(args.headless))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee scraper -> Postgres via upsert_product_and_price()

This script crawls Prisma's online store (food / household FMCG),
discovers product URLs, visits each PDP, extracts:
  - ext_id (Prisma SKU / URL slug tail)
  - product name
  - brand guess
  - size_text
  - EAN/barcode
  - price (EUR)
  - source_url (canonical PDP)
and then calls the DB function upsert_product_and_price(
    in_source      text,      -- 'prisma'
    in_ext_id      text,      -- Prisma product code / slug
    in_name        text,
    in_brand       text,
    in_size_text   text,
    in_ean_raw     text,
    in_price       numeric,
    in_currency    text,      -- 'EUR'
    in_store_id    integer,   -- stores.id for Prisma Online
    in_seen_at     timestamptz,
    in_source_url  text
) RETURNS integer;

That function is already created in Railway and is responsible for:
 - creating / reusing canonical product in products
 - inserting/extending ext_product_map
 - inserting/merging price row in prices for the given store_id

IMPORTANT:
    Prisma Online (Tallinn) has store_id = 14 in your `stores` table.
    We also allow overriding via env STORE_ID so we don't hardcode 14 forever.
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
import json
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ---------------------------------------------------------------------------
# Global config / constants

BASE = "https://prismamarket.ee"
SEEDS = ["/en/tooted/", "/tooted/"]  # root category listings to start crawling
CATEGORY_PREFIXES = ("/en/tooted/", "/tooted/", "/en/food-market/", "/food-market/")
PRODUCT_PREFIXES = ("/en/toode/", "/toode/")

# Regex helpers
PACK_RE   = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIMPLE_RE = re.compile(r"\b(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
PIECES_RE = re.compile(r"\b(\d+)\s*(?:tk|pcs?|pk|pack)\b", re.I)
BONUS_RE  = re.compile(r"\+\s*\d+%")      # "500 g +20%"
EAN_RE    = re.compile(r"(\d{8,14})$")
PRICE_NUM_RE = re.compile(r"(\d+[\.,]\d+)")

# ---------------------------------------------------------------------------
# Small utils

def jitter(a: float = 0.6, b: float = 1.4) -> None:
    """Polite random sleep between requests / actions."""
    time.sleep(random.uniform(a, b))

def clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def is_category_path(path: str) -> bool:
    return any(path.startswith(p) for p in CATEGORY_PREFIXES)

def is_product_path(path: str) -> bool:
    p = path.lower()
    return any(p.startswith(pref) for pref in PRODUCT_PREFIXES)

# ---------------------------------------------------------------------------
# Whitelist / blacklist filtering for categories

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
    """
    We only want food / FMCG, not furniture, electronics etc.
    """
    path = urlparse(url).path.lower()
    if not is_category_path(path):
        return False
    if any(ex in path for ex in EXCLUDED_CATEGORY_KEYWORDS):
        return False
    return True

# ---------------------------------------------------------------------------
# Optional high-level food group classifier (not strictly needed for ingest)

def map_food_group(c1: str, c2: str, c3: str, title: str) -> str:
    """
    Best-effort mapping of category/keywords to a broad food group.
    We keep this so we can re-use it later, but we don't push it
    into upsert_product_and_price() because the DB function doesn't need it.
    """
    t = " ".join([c1, c2, c3, title]).lower()

    def has(*keys): 
        return any(k in t for k in keys)

    if has("joogid","drink","water","juice","soda","beer","wine","kõvad joogid"):
        return "drinks"
    if has("leivad","küpsised","kook","saia","bakery","pastry","biscuit","bread","cake"):
        return "bakery"
    if has("piim","juust","kohuke","kohupiim","või","jogurt","dairy","eggs","munad","cream"):
        return "dairy_eggs"
    if has("puu","köögivil","vegetable","fruit","salat","herb"):
        return "produce"
    if has("liha","meat","kana","chicken","beef","pork","lamb","veal","ham","saus"):
        return "meat"
    if has("kala","fish","lõhe","räim","heering","tuna","shrimp","kammkarp","mereann","seafood"):
        return "fish"
    if has("külmutatud","frozen"):
        return "frozen"
    if has("kuivtooted","pasta","riis","rice","jahu","flour","sugar","suhkur","oil","õli","konserv",
           "canned","maitseaine","spice","kastme","sauce","teravili","cereal","snack","pähkl",
           "müsl"):
        return "pantry"
    if has("valmistoit","prepared","ready"):
        return "prepared"
    return "other"

# ---------------------------------------------------------------------------
# DB helpers

def get_database_url() -> str:
    """
    Pull DATABASE_URL from `settings.py` if present, else from env.
    """
    try:
        import settings  # type: ignore
        db = getattr(settings, "DATABASE_URL", None)
        if db:
            return db
    except Exception:
        pass

    db = os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("DATABASE_URL not set (env or settings.py)")
    return db

def db_connect() -> PGConn:
    """
    Connects to Postgres (autocommit).
    We are NOT creating tables here anymore. The DB already has:
      - products
      - prices
      - ext_product_map
      - upsert_product_and_price(...)
    """
    dsn = get_database_url()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn

def get_store_id_for_prisma() -> int:
    """
    Prisma Online (Tallinn) is store_id 14 in `stores`.
    Allow override via env STORE_ID so we don't hardcode forever.
    """
    try:
        return int(os.environ.get("STORE_ID", "14") or "14")
    except Exception:
        return 14

# ---------------------------------------------------------------------------
# PDP extraction helpers

def extract_title(page) -> str:
    try:
        return clean(page.locator("h1").first.inner_text())
    except Exception:
        return ""

def extract_image_url(page) -> str:
    sels = [
        "main img[alt][src]",
        "img[alt][src]",
        "img[src]"
    ]
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
    """
    Try multiple strategies to find something like:
    'EAN', 'Manufacturer', 'Country of origin', etc.
    """
    # exact text match -> nearest following element
    for label in labels:
        try:
            lab = page.locator(
                f"xpath=//*[normalize-space(.)='{label}']"
            ).first
            if lab.count() > 0:
                sib = lab.locator(
                    "xpath=following::*[self::div or self::span or self::p][1]"
                )
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception:
            pass

    # contains(...) fuzzy match
    for label in labels:
        try:
            lab = page.locator(
                f"xpath=//*[contains(normalize-space(.), '{label}')]"
            ).first
            if lab.count() > 0:
                sib = lab.locator(
                    "xpath=following::*[self::div or self::span or self::p][1]"
                )
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception:
            pass

    # dl/dt/dd patterns
    try:
        for label in labels:
            dt = page.locator(
                "xpath=//dt[normalize-space()='{0}'] | //dt[contains(normalize-space(),'{0}')]"
                .format(label)
            ).first
            if dt.count() > 0:
                dd = dt.locator("xpath=following-sibling::dd[1]")
                if dd.count() > 0:
                    return clean(dd.inner_text())
    except Exception:
        pass

    # fallback: regex in raw HTML
    try:
        html = page.content()
        for label in labels:
            m = re.search(
                fr"{re.escape(label)}\s*:?\s*</?[^>]*>?(.*?)<",
                html,
                re.I | re.S
            )
            if m:
                txt = re.sub(r"<[^>]+>", " ", m.group(1))
                txt = clean(txt)
                if txt:
                    return txt
    except Exception:
        pass

    return ""

def extract_ean(page, url: str) -> str:
    """
    Prefer explicit EAN/Ribakood if Prisma shows it.
    Otherwise try to grab digits from URL tail.
    """
    val = extract_label_value(page, ["EAN", "EAN-kood", "Ribakood", "EAN code"])
    if val and re.fullmatch(r"\d{8,14}", val):
        return val

    m = EAN_RE.search(url)
    if m:
        return m.group(1)
    return ""

def extract_country(page) -> str:
    return extract_label_value(
        page,
        ["Country of manufacture", "Country of origin", "Valmistajariik", "Päritoluriik"]
    )

def extract_manufacturer(page) -> str:
    return extract_label_value(
        page,
        ["Manufacturer", "Tootja", "Producer", "Valmistaja"]
    )

def parse_amount_from_title(title: str) -> str:
    """
    Best effort parse of size text like:
      '6x200 ml'
      '390 g'
      '1 l'
      '12 pcs'
    """
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
    """
    Lowercase, collapse spaces, dot-decimals, unify units.
    Return '' if not parseable.
    """
    if not s:
        return ""
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

    if re.search(r"\bkg\b", s):
        return "kg"
    if re.search(r"\bl\b", s):
        return "l"

    return ""

def extract_size_text(page, title: str) -> str:
    """
    Try to read net quantity / volume from PDP labels.
    Fallback to parsing the title.
    """
    lbl_val = extract_label_value(
        page,
        [
            "Net weight", "Net quantity", "Net content", "Net mass",
            "Kogus", "Maht", "Neto kogus", "Netokogus", "Pakendi suurus",
            "Pakendi maht", "Suurus", "Kaal", "Weight", "Volume", "Size",
            "Net weight / Net volume",
        ],
    )
    size = normalize_size_text(lbl_val)
    if size:
        return size

    # also attempt to mine JSON-LD blobs for something like "weight" / "size"
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(8, scripts.count())):
            raw = scripts.nth(i).inner_text()
            data = json.loads(raw)

            def walk(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(v, (str, int, float)):
                            if str(k).lower() in {
                                "weight","netweight","size","contentsize",
                                "packagesize","volume","netcontent",
                                "netContent","packageSize","contentSize",
                            }:
                                cand = normalize_size_text(str(v))
                                if cand:
                                    return cand
                        r = walk(v)
                        if r:
                            return r
                elif isinstance(obj, list):
                    for it in obj:
                        r = walk(it)
                        if r:
                            return r
                return ""
            cand = walk(data)
            if cand:
                return cand
    except Exception:
        pass

    # final fallback: parse the title itself
    return normalize_size_text(parse_amount_from_title(title))

def infer_brand_from_title(title: str) -> str:
    """
    Dumb brand guess: first word, or first two words if both Look TitleCase.
    """
    parts = title.split()
    if not parts:
        return ""
    if len(parts) >= 2 and parts[0][:1].isupper() and parts[1][:1].isupper():
        return f"{parts[0]} {parts[1]}"
    return parts[0]

def extract_price_eur(page) -> float:
    """
    Try to read the main product price (EUR) from the PDP.
    We'll grab the first text node that looks like '1,59' or '2.35'
    near common price selectors.
    """
    candidates = [
        "[data-test='product-price']",
        "[data-testid='product-price']",
        "[class*='price']",
        "span:has-text('€')",
        "span:has-text('EUR')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            txt = clean(loc.first.inner_text())
            m = PRICE_NUM_RE.search(txt.replace("\u00a0", " "))
            if m:
                raw = m.group(1).replace(",", ".")
                try:
                    return float(raw)
                except Exception:
                    pass
        except Exception:
            continue
    # fallback: try scanning entire page text
    try:
        txt = clean(page.inner_text("body"))
        m = PRICE_NUM_RE.search(txt)
        if m:
            raw = m.group(1).replace(",", ".")
            return float(raw)
    except Exception:
        pass
    return 0.0

def extract_ext_id_from_url(url: str) -> str:
    """
    Prisma PDP URLs usually look like
      /en/toode/some-product-name-ABC123/123456
    We'll just grab the last non-empty path segment.
    """
    path_parts = [p for p in urlparse(url).path.split("/") if p]
    if not path_parts:
        return url
    return path_parts[-1]

# ---------------------------------------------------------------------------
# Crawling helpers (category pagination / URL discovery)

def paginate_listing(page, max_pages: int = 80) -> None:
    """
    Scroll / click 'Load more' / 'Next' buttons to reveal products
    in long category listings.
    """
    def page_height() -> int:
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
        "[data-testid='load-more']",
        "button:has-text('Load more products')",
        "button:has-text('Show more products')",
        "button:has-text('Näita rohkem')",
    ]
    next_selectors = [
        "a[rel='next']",
        "a.pagination__next",
        "button:has-text('Next')",
        "a:has-text('Next')",
        "a.pagination-next",
        "button[aria-label='Next page']",
        "[data-testid='pagination-next']",
    ]

    pages_clicked = 0
    prev_h = -1

    while pages_clicked < max_pages:
        progressed = False

        # Try clicking any "load more" buttons
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
                        break
            except Exception:
                continue
        if progressed:
            continue

        # Try just scrolling
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

        # Finally, try a "Next" pagination button / link
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

        # no more progress
        break

def collect_links_from_listing(page, current_url: str) -> tuple[set[str], set[str]]:
    """
    Reads the current listing page (category page), scrolls/loads more,
    and returns:
        - product URLs discovered
        - category URLs discovered
    """
    try:
        page.wait_for_selector(
            "a[href*='/toode/'], a[href*='/en/toode/']",
            timeout=6000
        )
    except Exception:
        pass

    # attempt infinite scroll / pagination
    try:
        paginate_listing(page, max_pages=100)
    except Exception:
        pass

    # a little more scrolling at the end, just in case
    try:
        last_h = 0
        for _ in range(6):
            page.mouse.wheel(0, 20000)
            jitter(0.4, 0.9)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
    except Exception:
        pass

    prod, cats = set(), set()
    try:
        anchors = page.locator("a[href]")
        count = anchors.count()
    except PlaywrightTimeout:
        count = 0

    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href")
            if not href:
                continue
            url = urljoin(BASE, href)
            path = urlparse(url).path
            if is_product_path(path):
                prod.add(url)
            elif is_category_path(path) and is_in_whitelist(url):
                cats.add(url)
        except Exception:
            continue

    return prod, cats

# ---------------------------------------------------------------------------
# Main crawl -> DB ingest

def crawl_to_db(max_products: int = 500, headless: bool = True) -> None:
    conn = db_connect()
    store_id = get_store_id_for_prisma()

    rows_written = 0
    skipped_no_ean = 0
    skipped_no_price = 0
    product_urls: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ))
        page = context.new_page()

        # best-effort cookie acceptor
        def accept_cookies(pg):
            for sel in [
                "button:has-text('Accept all')",
                "button:has-text('Accept cookies')",
                "button:has-text('Nõustu')",
                "button[aria-label*='accept']",
                "button[aria-label*='Accept']",
            ]:
                try:
                    btn = pg.locator(sel)
                    if btn.count() > 0 and btn.first.is_enabled():
                        btn.first.click()
                        pg.wait_for_load_state("domcontentloaded")
                        jitter(0.2, 0.6)
                        return
                except Exception:
                    pass

        accept_cookies(page)

        # ---- Phase A: discover product links by crawling categories ----
        seen_categories: set[str] = set()
        to_visit = [urljoin(BASE, s) for s in SEEDS]

        while to_visit and len(product_urls) < max_products:
            cat_url = to_visit.pop(0)
            if cat_url in seen_categories:
                continue
            seen_categories.add(cat_url)

            try:
                page.goto(cat_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                jitter()
            except PlaywrightTimeout:
                continue

            prod, cats = collect_links_from_listing(page, cat_url)
            product_urls.update(prod)

            # queue newly discovered subcategories
            for c in cats:
                if c not in seen_categories and c not in to_visit:
                    to_visit.append(c)

            print(
                f"[DISCOVER] {cat_url} -> +{len(prod)} products, "
                f"+{len(cats)} cats (totals: products={len(product_urls)}, "
                f"queue={len(to_visit)})"
            )

            if len(product_urls) >= max_products:
                break

        # ---- Phase B: visit each product URL -> DB upsert via function ----
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for url in list(product_urls)[:max_products]:
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    jitter()
                except PlaywrightTimeout:
                    continue

                title = extract_title(page)
                ean = extract_ean(page, url)

                # We need EAN for reliable canonical matching downstream.
                if not ean:
                    skipped_no_ean += 1
                    continue

                size_text = extract_size_text(page, title)
                brand = infer_brand_from_title(title)
                manufacturer = extract_manufacturer(page)
                country = extract_country(page)
                image_url = extract_image_url(page)

                # get price
                price_val = extract_price_eur(page)
                if not price_val or price_val <= 0:
                    skipped_no_price += 1
                    continue

                # ext_id for Prisma (SKU / last segment in URL)
                ext_id = extract_ext_id_from_url(url)

                # call DB function upsert_product_and_price(...)
                try:
                    cur.execute(
                        """
                        SELECT upsert_product_and_price(
                            %s,  -- in_source ('prisma')
                            %s,  -- in_ext_id
                            %s,  -- in_name
                            %s,  -- in_brand
                            %s,  -- in_size_text
                            %s,  -- in_ean_raw
                            %s,  -- in_price
                            %s,  -- in_currency
                            %s,  -- in_store_id
                            %s,  -- in_seen_at
                            %s   -- in_source_url
                        );
                        """,
                        (
                            "prisma",
                            ext_id,
                            title,
                            brand,
                            size_text,
                            ean,
                            price_val,
                            "EUR",
                            store_id,
                            datetime.now(timezone.utc),
                            url,
                        ),
                    )
                    rows_written += 1
                except Exception as e:
                    print(
                        f"[prisma] upsert_product_and_price() failed for "
                        f"{ext_id} / EAN {ean}: {e}"
                    )
                    # because autocommit=True we don't have to rollback manually
                    # but we can continue to next product anyway
                    continue

        browser.close()

    print(
        f"[DONE] discovered {len(product_urls)} product URLs. "
        f"Inserted/updated {rows_written} rows via upsert_product_and_price(). "
        f"Skipped no-EAN: {skipped_no_ean}, skipped no-price: {skipped_no_price}."
    )

# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Prisma.ee food scrape -> Railway DB using upsert_product_and_price()"
    )
    ap.add_argument("--max-products", type=int, default=500,
                    help="Max distinct product PDPs to visit")
    ap.add_argument("--headless", type=int, default=1,
                    help="1=headless browser, 0=show browser (debug)")
    args = ap.parse_args()

    crawl_to_db(
        max_products=args.max_products,
        headless=bool(args.headless),
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

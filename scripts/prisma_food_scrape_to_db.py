#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee FOOD & DRINKS scraper → direct Postgres upsert

- Scrapes only grocery categories (whitelist) and auto-discovers subcats
- Extracts Prisma product metadata (incl. EAN) and UPSERTs into Postgres
- Keyed by EAN so re-runs will keep the latest metadata

Run:
  pip install playwright psycopg2-binary
  python -m playwright install chromium
  python scripts/prisma_food_scrape_to_db.py --max-products 500 --headless 1

DB connection:
  - Reads DATABASE_URL from settings.py (if available) or env var DATABASE_URL
  - Expected format: postgres://user:pass@host:port/dbname

Table:
  - Uses table name PRODUCTS_TABLE (env var, default: products)
  - Requires a UNIQUE constraint on ean (script will create table if missing)

Notes:
  - Be polite: throttle + jitter
  - Language: uses /en pages to stabilize labels like "EAN"
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

# -----------------------------------------------------------------------------
# Config
BASE = "https://prismamarket.ee"
SEEDS = [
    "/en/tooted/joogid",                      # beverages
    "/en/tooted/piim-munad-ja-rasvad",        # milk, eggs & fats
    "/en/tooted/puu-ja-koogiviljad",          # fruit & veg
    "/en/tooted/leivad-kupsised-ja-kupsetised",# bakery
    "/en/tooted/kuivtooted-ja-kupsetamine",   # dry & baking
    "/en/tooted/kulmutatud-toidud",           # frozen foods
    "/en/tooted/kala-ja-mereannid",           # fish & seafood
    "/en/tooted/food-market/liha",            # meat
    "/en/tooted/food-market/valmistoit",      # prepared
]
PRODUCT_PATH = "/toode/"
CATEGORY_PATH = "/tooted/"
AMOUNT_RE = re.compile(r"(\d+[\.,]?\d*\s?(?:kg|g|l|ml|cl|dl|tk|pcs|pk|pack|x\s*\d+\s*(?:g|ml|l)))", re.I)
EAN_RE = re.compile(r"(\d{8,14})$")

# -----------------------------------------------------------------------------
# Small utils
def jitter(a=0.6, b=1.4):
    time.sleep(random.uniform(a, b))

def clean(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def is_in_whitelist(url: str) -> bool:
    path = urlparse(url).path
    return any(path.startswith(seed) for seed in SEEDS)

# -----------------------------------------------------------------------------
# DB
def get_database_url() -> str:
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
    image_url TEXT,
    source_url TEXT,
    last_seen_utc TIMESTAMPTZ
);
"""

UPSERT_SQL = f"""
INSERT INTO {PRODUCTS_TABLE} (
    ean, product_name, name, amount, brand, manufacturer,
    country_of_manufacture, category_1, category_2, category_3,
    image_url, source_url, last_seen_utc
)
VALUES (
    %(ean)s, %(product_name)s, %(name)s, %(amount)s, %(brand)s, %(manufacturer)s,
    %(country_of_manufacture)s, %(category_1)s, %(category_2)s, %(category_3)s,
    %(image_url)s, %(source_url)s, %(last_seen_utc)s
)
ON CONFLICT (ean) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    name = EXCLUDED.name,
    amount = EXCLUDED.amount,
    brand = EXCLUDED.brand,
    manufacturer = EXCLUDED.manufacturer,
    country_of_manufacture = EXCLUDED.country_of_manufacture,
    category_1 = EXCLUDED.category_1,
    category_2 = EXCLUDED.category_2,
    category_3 = EXCLUDED.category_3,
    image_url = EXCLUDED.image_url,
    source_url = EXCLUDED.source_url,
    last_seen_utc = EXCLUDED.last_seen_utc
;
"""

def db_connect() -> PGConn:
    dsn = get_database_url()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    return conn

# -----------------------------------------------------------------------------
# Extraction helpers
def extract_title(page) -> str:
    try:
        return clean(page.locator("h1").first.inner_text())
    except Exception:
        return ""

def extract_image_url(page) -> str:
    for sel in ["main img[alt]", "img[alt][src]"]:
        try:
            img = page.locator(sel).first
            if img.count() > 0:
                src = img.get_attribute("src")
                if src:
                    from urllib.parse import urljoin as _join
                    return _join(BASE, src)
        except Exception:
            pass
    return ""

def extract_label_value(page, labels: list[str]) -> str:
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')] ")
            if lab.count() > 0:
                sib = lab.first.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.first.inner_text())
        except Exception:
            continue
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
    val = extract_label_value(page, ["EAN", "EAN-kood", "Ribakood"])
    if val and re.fullmatch(r"\d{8,14}", val):
        return val
    m = EAN_RE.search(url)
    return m.group(1) if m else ""

def extract_country(page) -> str:
    return extract_label_value(page, ["Country of manufacture", "Valmistajariik", "Päritoluriik"]) or ""

def extract_manufacturer(page) -> str:
    return extract_label_value(page, ["Manufacturer", "Tootja"]) or ""

def extract_breadcrumbs(page) -> list[str]:
    crumbs = []
    try:
        els = page.locator("nav a[href*='/tooted/']")
        for i in range(els.count()):
            txt = clean(els.nth(i).inner_text())
            if txt:
                crumbs.append(txt)
    except Exception:
        pass
    return crumbs[-3:]

def parse_amount_from_title(title: str) -> str:
    m = AMOUNT_RE.search(title)
    return m.group(0) if m else ""

def infer_brand_from_title(title: str) -> str:
    parts = title.split()
    if not parts:
        return ""
    if len(parts) >= 2 and parts[0][:1].isupper() and parts[1][:1].isupper():
        return f"{parts[0]} {parts[1]}"
    return parts[0]

# -----------------------------------------------------------------------------
# Listing crawling
def collect_links_from_listing(page, current_url: str) -> tuple[set[str], set[str]]:
    try:
        last_h = 0
        for _ in range(8):
            page.mouse.wheel(0, 20000)
            jitter(0.4, 0.9)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
    except Exception:
        pass

    anchors = page.locator("a[href]")
    prod, cats = set(), set()
    try:
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
            if PRODUCT_PATH in path:
                prod.add(url)
            elif path.startswith(CATEGORY_PATH) and is_in_whitelist(url):
                cats.add(url)
        except Exception:
            continue
    return prod, cats

# -----------------------------------------------------------------------------
# Main crawl → DB
def crawl_to_db(max_products: int = 500, headless: bool = True):
    conn = db_connect()
    rows_written = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = context.new_page()

        seen_categories = set()
        to_visit = [urljoin(BASE, s) for s in SEEDS]
        product_urls = set()

        # Phase A: discover product links
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
            for c in cats:
                if c not in seen_categories and c not in to_visit:
                    to_visit.append(c)

            try:
                for _ in range(10):
                    next_btn = page.locator("a[rel='next'], button:has-text('Next'), a:has-text('Next')")
                    if next_btn.count() == 0:
                        break
                    next_btn.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter()
                    prod2, cats2 = collect_links_from_listing(page, cat_url)
                    product_urls.update(prod2)
                    for c in cats2:
                        if c not in seen_categories and c not in to_visit:
                            to_visit.append(c)
                    if len(product_urls) >= max_products:
                        break
            except Exception:
                pass

            if len(product_urls) >= max_products:
                break

        # Phase B: visit products → UPSERT
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
                if not ean:
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

                rec = {
                    "ean": ean,
                    "product_name": title,
                    "name": title,  # <-- populate legacy NOT NULL column
                    "amount": amount,
                    "brand": brand,
                    "manufacturer": manufacturer,
                    "country_of_manufacture": country,
                    "category_1": c1,
                    "category_2": c2,
                    "category_3": c3,
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
    print(f"Upserted {rows_written} rows into '{PRODUCTS_TABLE}'.")

# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Prisma.ee FOOD & DRINKS → Postgres")
    ap.add_argument("--max-products", type=int, default=500)
    ap.add_argument("--headless", type=int, default=1)
    args = ap.parse_args()
    crawl_to_db(max_products=args.max_products, headless=bool(args.headless))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

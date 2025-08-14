#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch current prices for Prisma products and insert into prices(product_id, store_id, price, seen_at).
"""

import os, re, sys, time, random, argparse
from datetime import datetime, timezone
import psycopg2, psycopg2.extras
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

STORE_ID = int(os.getenv("PRISMA_STORE_ID", "14"))  # Prisma Online (Tallinn)
PRICE_RE = re.compile(r"(\d+[.,]?\d*)")  # extract number from "€3.29", "3,29 €", etc.

def jitter(a=0.4, b=1.1): time.sleep(random.uniform(a, b))

def get_db() -> psycopg2.extensions.connection:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL missing")
        sys.exit(2)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn

def pick_price_text(page) -> str:
    # Try several likely selectors; adjust as the site evolves
    sels = [
        "[data-testid*='price']",              # generic testid
        "[class*='price']", "[class*='Price']",# css class fragments
        "span:has-text('€')", "div:has-text('€')",
        "meta[itemprop='price'][content]"      # structured price
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            if loc.count() == 0: continue
            # meta content case
            if "meta" in sel:
                val = loc.first.get_attribute("content")
                if val: return val
            txt = loc.first.inner_text().strip()
            if txt: return txt
        except Exception:
            continue
    return ""

def parse_price(val: str) -> float | None:
    if not val: return None
    m = PRICE_RE.search(val.replace("\u00A0"," ").replace(",", "."))
    if not m: return None
    try:
        return round(float(m.group(1)), 2)
    except Exception:
        return None

def load_prisma_products(conn, limit: int | None):
    q = """
    SELECT id, source_url, product_name
    FROM products
    WHERE source_url ILIKE '%prismamarket.ee%'
    ORDER BY last_seen_utc DESC NULLS LAST
    """
    if limit:
        q += " LIMIT %s"
        args = (limit,)
    else:
        args = None
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(q, args)
        return cur.fetchall()

def insert_price(conn, product_id: int, price: float):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO prices (product_id, store_id, price, seen_at) VALUES (%s, %s, %s, %s)",
            (product_id, STORE_ID, price, datetime.now(timezone.utc))
        )

def main():
    ap = argparse.ArgumentParser(description="Prisma price updater")
    ap.add_argument("--max-products", type=int, default=400)
    ap.add_argument("--headless", type=int, default=1)
    args = ap.parse_args()

    conn = get_db()
    rows = load_prisma_products(conn, args.max_products)
    if not rows:
        print("No Prisma products found.")
        return

    wrote, skipped = 0, 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=bool(args.headless))
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = ctx.new_page()

        for r in rows:
            pid = r["id"]
            url = r["source_url"]
            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                jitter()
            except PlaywrightTimeout:
                skipped += 1
                continue

            price_text = pick_price_text(page)
            price = parse_price(price_text)
            if price is None:
                skipped += 1
                continue

            try:
                insert_price(conn, pid, price)
                wrote += 1
            except Exception as e:
                print(f"price insert failed for product_id={pid}: {e}")
                conn.rollback()

        browser.close()

    print(f"Prices written: {wrote}, skipped: {skipped}, scanned: {len(rows)}")

if __name__ == "__main__":
    main()

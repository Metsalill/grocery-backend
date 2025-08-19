#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch current prices for Prisma products and write:
  - price_history(product_id, amount, currency, captured_at, store_id, price_type, source_url)
  - prices(product_id UNIQUE, store_id, price, currency, collected_at, source_url)
"""

import os, re, sys, time, random, argparse
from datetime import datetime, timezone
import psycopg2, psycopg2.extras
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

STORE_ID   = int(os.getenv("PRISMA_STORE_ID", "14"))   # Prisma Online (Tallinn)
CURRENCY   = os.getenv("PRICE_CURRENCY", "EUR")
PRICE_TYPE = os.getenv("PRICE_TYPE", "regular")        # free text; keep if you ever add promos
PRICE_RE   = re.compile(r"(\d+(?:[.,]\d*)?)")          # extract number from "€3.29", "3,29 €", etc.

def jitter(a=0.4, b=1.1):
    time.sleep(random.uniform(a, b))

def get_db() -> psycopg2.extensions.connection:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL missing")
        sys.exit(2)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False  # commit per product so history + canonical stay consistent
    return conn

# --- Make sure tables/columns/indexes exist (idempotent) ----------------------
def ensure_schema(conn):
    with conn.cursor() as cur:
        # price_history (append-only)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
              id          SERIAL PRIMARY KEY,
              product_id  INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
              amount      NUMERIC(10,2) NOT NULL,
              currency    TEXT DEFAULT 'EUR',
              captured_at TIMESTAMPTZ NOT NULL,
              store_id    INT NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
              price_type  TEXT,
              source_url  TEXT
            );
        """)
        # prices (canonical: one row per product)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prices (
              id           SERIAL PRIMARY KEY,
              product_id   INT NOT NULL UNIQUE REFERENCES products(id) ON DELETE CASCADE,
              store_id     INT NOT NULL REFERENCES stores(id),
              price        NUMERIC(10,2) NOT NULL,
              currency     TEXT DEFAULT 'EUR',
              collected_at TIMESTAMPTZ NOT NULL,
              source_url   TEXT
            );
        """)
        # Columns that might be missing in older DBs
        cur.execute("ALTER TABLE prices ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE prices ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'EUR';")
        cur.execute("ALTER TABLE prices ADD COLUMN IF NOT EXISTS source_url TEXT;")
        # Indexes
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_product ON prices(product_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_store ON prices(store_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_time ON prices(collected_at DESC);")
    conn.commit()

def pick_price_text(page) -> str:
    sels = [
        "[data-testid*='price']",
        "[class*='price']",
        "[class*='Price']",
        "[itemprop='price'][content]",
        "meta[itemprop='price'][content]",
        "span:has-text('€')",
        "div:has-text('€')",
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            # meta/content case
            if ("meta" in sel) or ("[itemprop='price'][content]" in sel):
                val = loc.first.get_attribute("content")
                if val:
                    return val
            txt = loc.first.inner_text().strip()
            if txt:
                return txt
        except Exception:
            continue
    return ""

def parse_price(val: str):
    if not val:
        return None
    # normalize NBSP and comma as decimal separator
    val = val.replace("\u00A0", " ").replace(",", ".")
    m = PRICE_RE.search(val)
    if not m:
        return None
    try:
        return round(float(m.group(1)), 2)
    except Exception:
        return None

def load_prisma_products(conn, limit: int | None):
    """
    Return rows (id, source_url) for Prisma products.
    """
    sql = """
        SELECT id, source_url
        FROM products
        WHERE source_url ILIKE %s
        ORDER BY last_seen_utc DESC NULLS LAST
    """
    params = ['%prismamarket.ee%']
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def write_price(conn, *, product_id: int, price: float, source_url: str):
    """
    In ONE transaction:
      1) Append to price_history
      2) Upsert into prices (unique on product_id)
         Rule: update only if the incoming row is newer OR
               same timestamp but cheaper.
    """
    ts = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        # 1) append history
        cur.execute(
            """
            INSERT INTO price_history
              (product_id, amount, currency, captured_at, store_id, price_type, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (product_id, price, CURRENCY, ts, STORE_ID, PRICE_TYPE, source_url)
        )

        # 2) canonical upsert (note: collected_at, not seen_at)
        cur.execute(
            """
            INSERT INTO prices (product_id, store_id, price, currency, collected_at, source_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE
            SET store_id     = EXCLUDED.store_id,
                price        = EXCLUDED.price,
                currency     = EXCLUDED.currency,
                collected_at = EXCLUDED.collected_at,
                source_url   = EXCLUDED.source_url
            WHERE
              EXCLUDED.collected_at > prices.collected_at
              OR (EXCLUDED.collected_at = prices.collected_at AND EXCLUDED.price < prices.price)
            """,
            (product_id, STORE_ID, price, CURRENCY, ts, source_url)
        )
    conn.commit()

def main():
    ap = argparse.ArgumentParser(description="Prisma price updater")
    ap.add_argument("--max-products", type=int, default=400)
    ap.add_argument("--headless", type=int, default=1)
    args = ap.parse_args()

    conn = get_db()
    ensure_schema(conn)

    rows = load_prisma_products(conn, args.max_products)
    if not rows:
        print("No Prisma products found.")
        return
    print(f"Loaded {len(rows)} Prisma products to price.")

    wrote, skipped = 0, 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=bool(args.headless))
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = ctx.new_page()

        # One-time cookie accept (best-effort)
        def accept_cookies():
            for sel in [
                "button:has-text('Accept all')",
                "button:has-text('Accept cookies')",
                "button:has-text('Nõustu')",
                "button[aria-label*='accept']",
            ]:
                try:
                    btn = page.locator(sel)
                    if btn.count() > 0 and btn.first.is_enabled():
                        btn.first.click()
                        jitter(0.2, 0.6)
                        return
                except Exception:
                    pass

        for r in rows:
            pid = int(r["id"])
            url = r["source_url"] or ""
            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                accept_cookies()
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
                write_price(conn, product_id=pid, price=price, source_url=url)
                wrote += 1
            except Exception as e:
                conn.rollback()
                print(f"price write failed for product_id={pid}: {e}")

        browser.close()

    try:
        conn.close()
    except Exception:
        pass

    print(f"Prices written: {wrote}, skipped: {skipped}, scanned: {len(rows)}")

if __name__ == "__main__":
    main()

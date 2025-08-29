#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma price updater

Writes ONLY into:
  - public.price_history  (append-only)
  - public.prices         (canonical one row per (product_id, store_id))

Everything is schema-qualified to avoid accidental writes elsewhere.
"""

import os, re, sys, time, random, argparse
from datetime import datetime, timezone
from typing import Optional, Iterable

import psycopg2, psycopg2.extras
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

SCHEMA     = "public"
DEFAULT_STORE_ID = os.getenv("PRISMA_STORE_ID", "").strip()
CURRENCY   = os.getenv("PRICE_CURRENCY", "EUR")
PRICE_TYPE = os.getenv("PRICE_TYPE", "regular")
PRICE_RE   = re.compile(r"(\d+(?:[.,]\d*)?)")  # extracts number from "€3.29", "3,29 €", etc.

def jitter(a=0.3, b=0.9):  # slightly faster default
    time.sleep(random.uniform(a, b))

def get_db() -> psycopg2.extensions.connection:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL missing", file=sys.stderr)
        sys.exit(2)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    with conn, conn.cursor() as cur:
        # keep scope tight to public
        cur.execute("SET search_path TO public;")
    return conn

def resolve_store_id(conn) -> int:
    """
    Uses env PRISMA_STORE_ID if valid positive int; otherwise resolves by chain='Prisma' AND is_online=TRUE.
    """
    if DEFAULT_STORE_ID.isdigit() and int(DEFAULT_STORE_ID) > 0:
        return int(DEFAULT_STORE_ID)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {SCHEMA}.stores WHERE chain='Prisma' AND is_online=TRUE LIMIT 1;"
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Could not resolve Prisma store_id from stores table.")
        return int(row[0])

# --- Make sure tables/columns/indexes exist (idempotent) ----------------------
def ensure_schema(conn):
    with conn, conn.cursor() as cur:
        # price_history (append-only)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.price_history (
              id          SERIAL PRIMARY KEY,
              product_id  INT NOT NULL REFERENCES {SCHEMA}.products(id) ON DELETE CASCADE,
              amount      NUMERIC(10,2) NOT NULL,
              currency    TEXT DEFAULT 'EUR',
              captured_at TIMESTAMPTZ NOT NULL,
              store_id    INT NOT NULL REFERENCES {SCHEMA}.stores(id) ON DELETE CASCADE,
              price_type  TEXT,
              source_url  TEXT
            );
        """)

        # prices (canonical): one row per (product_id, store_id)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.prices (
              id           SERIAL PRIMARY KEY,
              product_id   INT NOT NULL REFERENCES {SCHEMA}.products(id) ON DELETE CASCADE,
              store_id     INT NOT NULL REFERENCES {SCHEMA}.stores(id),
              price        NUMERIC(10,2) NOT NULL,
              currency     TEXT DEFAULT 'EUR',
              collected_at TIMESTAMPTZ NOT NULL,
              source_url   TEXT
            );
        """)

        # Columns that might be missing in older DBs
        cur.execute(f"ALTER TABLE {SCHEMA}.prices ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ;")
        cur.execute(f"ALTER TABLE {SCHEMA}.prices ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'EUR';")
        cur.execute(f"ALTER TABLE {SCHEMA}.prices ADD COLUMN IF NOT EXISTS source_url TEXT;")

        # Drop any legacy unique just on (product_id)
        cur.execute(f"""
        DO $$
        DECLARE
          con_name text;
          idx_name text;
        BEGIN
          -- drop UNIQUE constraints only on (product_id)
          FOR con_name IN
            SELECT c.conname
            FROM pg_constraint c
            WHERE c.conrelid = '{SCHEMA}.prices'::regclass
              AND c.contype  = 'u'
              AND c.conkey   = ARRAY[
                (SELECT attnum FROM pg_attribute
                 WHERE attrelid='{SCHEMA}.prices'::regclass AND attname='product_id')
              ]
          LOOP
            EXECUTE format('ALTER TABLE {SCHEMA}.prices DROP CONSTRAINT %I', con_name);
          END LOOP;

          -- drop UNIQUE indexes that cover only (product_id)
          FOR idx_name IN
            SELECT ic.relname
            FROM pg_index i
            JOIN pg_class t  ON t.oid = i.indrelid AND t.relname = 'prices'
            JOIN pg_namespace n ON n.oid = t.relnamespace AND n.nspname='{SCHEMA}'
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indisunique
              AND i.indnatts = 1
              AND i.indkey[1] = (
                SELECT attnum FROM pg_attribute
                WHERE attrelid=t.oid AND attname='product_id'
              )
          LOOP
            EXECUTE format('DROP INDEX IF EXISTS %I', idx_name);
          END LOOP;
        END$$;
        """)

        # De-dup by (product_id, store_id) keeping newest before adding unique
        cur.execute(f"""
            WITH r AS (
              SELECT id,
                     ROW_NUMBER() OVER (
                       PARTITION BY product_id, store_id
                       ORDER BY collected_at DESC, id DESC
                     ) rn
              FROM {SCHEMA}.prices
            )
            DELETE FROM {SCHEMA}.prices p
            USING r
            WHERE p.id = r.id AND r.rn > 1;
        """)

        # Ensure composite unique exists regardless of name/order
        cur.execute(f"""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                WHERE c.conrelid = '{SCHEMA}.prices'::regclass
                  AND c.contype  = 'u'
                  AND (
                    c.conkey = ARRAY[
                      (SELECT attnum FROM pg_attribute WHERE attrelid='{SCHEMA}.prices'::regclass AND attname='product_id'),
                      (SELECT attnum FROM pg_attribute WHERE attrelid='{SCHEMA}.prices'::regclass AND attname='store_id')
                    ]
                    OR
                    c.conkey = ARRAY[
                      (SELECT attnum FROM pg_attribute WHERE attrelid='{SCHEMA}.prices'::regclass AND attname='store_id'),
                      (SELECT attnum FROM pg_attribute WHERE attrelid='{SCHEMA}.prices'::regclass AND attname='product_id')
                    ]
                  )
              ) THEN
                EXECUTE 'ALTER TABLE {SCHEMA}.prices ADD CONSTRAINT uq_prices_per_store UNIQUE (product_id, store_id)';
              END IF;
            END$$;
        """)

        # Helpful indexes
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_prices_store ON {SCHEMA}.prices(store_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_prices_time ON {SCHEMA}.prices(collected_at DESC);")

def pick_price_text(page) -> str:
    """
    Try a bunch of common price locations on prismamarket.ee PDP.
    """
    sels = [
        # most-reliable first
        "meta[itemprop='price'][content]",
        "meta[property='product:price:amount'][content]",
        "[itemprop='price'][content]",
        "[data-testid*='price']",
        "[data-test*='price']",
        "[class*='product-price']",
        "[class*='Price']",
        "[class*='price']",
        "span:has-text('€')",
        "div:has-text('€')",
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            if "meta" in sel and "content" in sel:
                val = loc.first.get_attribute("content")
                if val:
                    return val.strip()
            txt = loc.first.inner_text().strip()
            if txt:
                return txt
        except Exception:
            continue
    return ""

def parse_price(val: str) -> Optional[float]:
    if not val:
        return None
    val = val.replace("\u00A0", " ").replace(",", ".")
    m = PRICE_RE.search(val)
    if not m:
        return None
    try:
        return round(float(m.group(1)), 2)
    except Exception:
        return None

def load_prisma_products(conn, limit: Optional[int]) -> Iterable[psycopg2.extras.DictRow]:
    sql = f"""
        SELECT id, source_url
        FROM {SCHEMA}.products
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

def write_price(conn, *, store_id: int, product_id: int, price: float, source_url: str):
    ts = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        # 1) append to history
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.price_history
              (product_id, amount, currency, captured_at, store_id, price_type, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (product_id, price, CURRENCY, ts, store_id, PRICE_TYPE, source_url)
        )

        # 2) upsert canonical (product_id, store_id)
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.prices (product_id, store_id, price, currency, collected_at, source_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id, store_id) DO UPDATE
            SET price        = EXCLUDED.price,
                currency     = EXCLUDED.currency,
                collected_at = EXCLUDED.collected_at,
                source_url   = EXCLUDED.source_url
            WHERE
              EXCLUDED.collected_at > {SCHEMA}.prices.collected_at
              OR (EXCLUDED.collected_at = {SCHEMA}.prices.collected_at AND EXCLUDED.price < {SCHEMA}.prices.price)
            """,
            (product_id, store_id, price, CURRENCY, ts, source_url)
        )

def main():
    ap = argparse.ArgumentParser(description="Prisma price updater")
    ap.add_argument("--max-products", type=int, default=400)
    ap.add_argument("--headless", type=int, default=1)
    args = ap.parse_args()

    conn = get_db()
    ensure_schema(conn)

    try:
        store_id = resolve_store_id(conn)
    except Exception as e:
        print(f"Error resolving Prisma store_id: {e}", file=sys.stderr)
        sys.exit(2)

    rows = load_prisma_products(conn, args.max_products)
    if not rows:
        print("No Prisma products found (source_url like prismamarket.ee).")
        return
    print(f"Loaded {len(rows)} Prisma products to price. Using store_id={store_id}.")

    wrote, skipped = 0, 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=bool(args.headless))
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = ctx.new_page()

        def accept_cookies():
            for sel in [
                "button:has-text('Accept all')",
                "button:has-text('Accept cookies')",
                "button:has-text('Nõustu')",
                "button:has-text('Nõustun')",
                "button[aria-label*='accept']",
                "[data-testid*='cookie'] button",
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
            url = (r["source_url"] or "").strip()
            if not url:
                skipped += 1
                continue

            ok = False
            for attempt in range(2):  # small retry
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    accept_cookies()
                    jitter()
                    price_text = pick_price_text(page)
                    price = parse_price(price_text)
                    if price is None or price <= 0:
                        raise ValueError("price not found")
                    write_price(conn, store_id=store_id, product_id=pid, price=price, source_url=url)
                    conn.commit()
                    wrote += 1
                    ok = True
                    break
                except (PlaywrightTimeout, ValueError):
                    if attempt == 0:
                        jitter(0.6, 1.2)
                        continue
                    skipped += 1
                except Exception as e:
                    conn.rollback()
                    print(f"price write failed for product_id={pid}: {e}")
                    skipped += 1
                    break

        browser.close()

    try:
        conn.close()
    except Exception:
        pass

    print(f"Prices written: {wrote}, skipped: {skipped}, scanned: {len(rows)}")

if __name__ == "__main__":
    main()

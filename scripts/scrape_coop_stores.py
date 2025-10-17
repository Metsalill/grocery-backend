#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape Coop physical stores from https://www.coop.ee/kauplused and upsert into stores.
- Finds rows in #shop-search-page .c-shops-list .c-shops-list__item
- Extracts: name, address, lat, lon (from the 'Google Maps' href)
- Upserts into stores on UNIQUE(name) via stores_name_key
- Writes CSV to data/coop_stores.csv for audit
"""

import os, csv, time, re
from urllib.parse import urlparse, parse_qs
from contextlib import closing

import psycopg
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

COOP_URL = "https://www.coop.ee/kauplused"
CHAIN = "Coop"
CSV_PATH = "data/coop_stores.csv"

def ensure_data_dir():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

def parse_lat_lon_from_maps(href: str):
    # formats seen: https://maps.google.com/?q=58.3776,26.7290
    if not href:
        return None, None
    try:
        q = parse_qs(urlparse(href).query).get("q", [""])[0]
        m = re.match(r"\s*([+-]?\d+(?:\.\d+)?)[ ,]+([+-]?\d+(?:\.\d+)?)\s*$", q)
        if not m:
            return None, None
        return float(m.group(1)), float(m.group(2))
    except Exception:
        return None, None

def auto_scroll(page, max_idle_loops=4, sleep_ms=700, hard_cap=1500):
    """
    Scrolls the page until the number of visible store cards stops growing for a few loops.
    """
    last = -1
    idle = 0
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(sleep_ms)
        try:
            count = page.locator(".c-shops-list__item").count()
        except Exception:
            count = 0
        if count == last:
            idle += 1
        else:
            idle = 0
            last = count
        if idle >= max_idle_loops or count >= hard_cap:
            break

def collect_rows():
    rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(locale="et-EE")
        page = context.new_page()
        page.goto(COOP_URL, wait_until="domcontentloaded")

        # Wait for the list container, then for the first item to render
        page.wait_for_selector("#shop-search-page .c-shops-list", timeout=30000)
        try:
            page.wait_for_selector(".c-shops-list__item", timeout=30000)
        except PWTimeout:
            # Give the page a nudge and more time (first paint can lag)
            page.wait_for_timeout(1500)

        # Load everything (lazy/infinite scroll)
        auto_scroll(page)

        items = page.locator(".c-shops-list__item")
        total = items.count()

        for i in range(total):
            card = items.nth(i)

            # Name (make resilient)
            name = card.locator("p.u-fw-600").first.inner_text().strip()

            # Address
            address = card.locator("p.mb-12").first.inner_text().strip()

            # Google Maps href → lat/lon
            href = card.locator("a:has-text('Google Maps')").first.get_attribute("href")
            lat, lon = parse_lat_lon_from_maps(href)

            rows.append(
                {
                    "chain": CHAIN,
                    "name": name,
                    "address": address,
                    "lat": lat,
                    "lon": lon,
                    "is_online": False,
                }
            )

        context.close()
        browser.close()

    return rows

def write_csv(rows):
    ensure_data_dir()
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["chain", "name", "address", "lat", "lon", "is_online"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

def upsert_rows(rows):
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PUBLIC")
    if not db_url:
        print("[coop-stores] no DATABASE_URL provided; CSV only")
        return 0

    sql = """
        INSERT INTO stores (name, chain, is_online, lat, lon, address)
        VALUES (%(name)s, %(chain)s, %(is_online)s, %(lat)s, %(lon)s, %(address)s)
        ON CONFLICT ON CONSTRAINT stores_name_key
        DO UPDATE SET
            chain     = EXCLUDED.chain,
            is_online = EXCLUDED.is_online,
            lat       = EXCLUDED.lat,
            lon       = EXCLUDED.lon,
            address   = EXCLUDED.address;
    """

    with closing(psycopg.connect(db_url, autocommit=True)) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)

    return len(rows)

def main():
    print("[coop-stores] navigate…")
    rows = collect_rows()
    print(f"[coop-stores] parsed {len(rows)} candidate cards")

    # Filter out cards with no coordinates (just in case)
    rows = [r for r in rows if r["lat"] is not None and r["lon"] is not None]
    print(f"[coop-stores] keeping {len(rows)} rows with coords")

    write_csv(rows)
    print(f"[coop-stores] wrote CSV → {CSV_PATH}")

    n = upsert_rows(rows)
    print(f"[coop-stores] upserted {n} rows into stores")

if __name__ == "__main__":
    main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Maxima physical stores from https://www.maxima.ee/kauplused and upsert into `stores`.
- Extracts: name, address, lat, lon from accordion button data-attributes
- Upserts into `stores` on UNIQUE(name) via stores_name_key
- Writes CSV to data/maxima_stores.csv for audit
Notes:
  * Some store cards may not include a nice `data-name`. We build a stable name as
    "Maxima — {address}" in those cases to satisfy the unique(name) constraint.
"""
import os, csv
from contextlib import closing

import psycopg
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.maxima.ee/kauplused"
CHAIN = "Maxima"
CSV_PATH = "data/maxima_stores.csv"


def ensure_data_dir():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)


def collect_rows():
    rows = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(locale="et-EE")
        page = context.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        # The store list renders as accordion buttons with data-* attributes.
        # Examples (observed in DevTools):
        #   <div class="accordion-button ..." data-name="Maxima X - Sütište"
        #        data-address="Sütiste tee 28, Tallinn" data-lat="59.39831" data-lng="24.69061" ...>
        #
        # Be tolerant: wait for at least one, then grab them all.
        try:
            page.wait_for_selector("div.accordion-button[data-address][data-lat][data-lng]", timeout=30000)
        except PWTimeout:
            # Fallback: give the page a little more time
            page.wait_for_timeout(2000)

        items = page.locator("div.accordion-button[data-address][data-lat][data-lng]")
        total = items.count()

        for i in range(total):
            el = items.nth(i)
            address = (el.get_attribute("data-address") or "").strip()
            lat = el.get_attribute("data-lat")
            lng = el.get_attribute("data-lng") or el.get_attribute("data-lon")
            name_attr = (el.get_attribute("data-name") or "").strip()
            size = (el.get_attribute("data-size") or "").strip()

            # Parse coords safely
            try:
                lat = float(lat) if lat is not None else None
            except Exception:
                lat = None
            try:
                lon = float(lng) if lng is not None else None
            except Exception:
                lon = None

            # Build a stable store name (unique on `stores.name`)
            name = name_attr if name_attr else f"Maxima — {address}"
            # Optionally add size marker if present but missing from name
            if size and size not in name:
                name = f"{name} ({size})"

            if lat is None or lon is None or not address:
                # Skip incomplete cards
                continue

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
    import csv
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["chain", "name", "address", "lat", "lon", "is_online"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def upsert_rows(rows):
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PUBLIC")
    if not db_url:
        print("[maxima-stores] no DATABASE_URL provided; CSV only")
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
    print("[maxima-stores] navigating…")
    rows = collect_rows()
    print(f"[maxima-stores] parsed {len(rows)} rows with coords")
    write_csv(rows)
    print(f"[maxima-stores] wrote CSV → {CSV_PATH}")
    n = upsert_rows(rows)
    print(f"[maxima-stores] upserted {n} rows into stores")


if __name__ == "__main__":
    main()

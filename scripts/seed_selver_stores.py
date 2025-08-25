#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape https://www.selver.ee/kauplused → store list, geocode (Nominatim), upsert into Postgres stores().
- Creates e-Selver if missing (is_online=TRUE)
- Upserts physical stores (is_online=FALSE)
- Sets chain='Selver'
"""
import os, re, time, json
import psycopg2, psycopg2.extras
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import requests

DB_URL = os.environ.get("DATABASE_URL") or os.environ.get("RW_DATABASE_URL")

def geocode(addr):
    if not addr: return None
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": addr, "format": "json", "limit": 1}
    headers = {"User-Agent": "grocery-backend/seed-selver-stores"}
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    arr = r.json()
    if not arr: return None
    return float(arr[0]["lat"]), float(arr[0]["lon"])

def main():
    assert DB_URL, "Set DATABASE_URL or RW_DATABASE_URL"
    with sync_playwright() as pw:
        b = pw.chromium.launch(headless=True)
        page = b.new_page()
        page.goto("https://www.selver.ee/kauplused", wait_until="load", timeout=60000)

        # Heuristic: store cards usually contain name + address + link
        cards = page.locator("a:visible", has_text=re.compile("Selver|Delice", re.I))
        hrefs = list(set(cards.evaluate_all("els => els.map(e => e.href)")))
        stores = []
        for href in sorted(hrefs):
            try:
                page.goto(href, wait_until="load", timeout=45000)
                name = page.locator("h1, h2").first.inner_text().strip()
                addr = page.locator("text=/\\d{1,3}.+|pst|tee|mnt|maantee|tänav|tee/i").first.inner_text().strip()
            except Exception:
                name = page.title().split("|")[0].strip() or href.split("/")[-1].replace("-", " ").title()
                addr = None
            stores.append({"name": name, "address": addr, "href": href})
        b.close()

    # Upsert into DB
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    # Ensure e-Selver online
    cur.execute("""
        INSERT INTO stores (name, chain, is_online) VALUES ('e-Selver','Selver',TRUE)
        ON CONFLICT DO NOTHING;
    """)
    # Insert physical stores
    up = """
    INSERT INTO stores (name, chain, is_online, lat, lon)
    VALUES (%s, 'Selver', FALSE, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    for s in stores:
        latlon = geocode(s["address"]) if s["address"] else None
        lat, lon = (latlon or (None, None))
        cur.execute(up, (s["name"], lat, lon))
        time.sleep(1)  # polite geocoding
    cur.close(); conn.close()

if __name__ == "__main__":
    main()

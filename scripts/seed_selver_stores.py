#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape https://www.selver.ee/kauplused → store detail pages and either:
  - FULL SEED: insert physical stores and set address/coords non-destructively.
  - BACKFILL: only update rows in DB that are missing/zero lat/lon.

Also ensures a single online row (e-Selver) exists with is_online=TRUE.

Env:
  DATABASE_URL / RW_DATABASE_URL   (required)
  BACKFILL_ONLY   1|0  default 1   (1 = only update rows missing coords)
  GEOCODE         1|0  default 1   (use Nominatim; still parses coords from gmaps links)
  DRY_RUN         1|0  default 0
  CHAIN                 default "Selver"
  ONLINE_NAME           default "e-Selver"
"""

from __future__ import annotations
import os, re, time, sys
import psycopg2, psycopg2.extras
import requests
from urllib.parse import urlparse, parse_qs, unquote
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ------------ config & helpers ------------

DB_URL = os.environ.get("DATABASE_URL") or os.environ.get("RW_DATABASE_URL")
if not DB_URL:
    print("Set DATABASE_URL or RW_DATABASE_URL", file=sys.stderr)
    sys.exit(1)

BACKFILL_ONLY = os.environ.get("BACKFILL_ONLY", "1") == "1"
GEOCODE       = os.environ.get("GEOCODE", "1") == "1"
DRY_RUN       = os.environ.get("DRY_RUN", "0") == "1"
CHAIN         = os.environ.get("CHAIN", "Selver")
ONLINE_NAME   = os.environ.get("ONLINE_NAME", "e-Selver")

UA = "grocery-backend/seed-selver-stores (+gha)"

# schedules like "E-P 08:00–23:00"
SCHEDULE_SPLIT = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)

CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

ADDR_TOKEN = re.compile(r'\b(mnt|tee|tn|pst|puiestee|maantee|tänav|keskus|turg|väljak)\b', re.I)

DETAIL_HREF_RE = re.compile(r"^https?://[^/]*selver\.ee/kauplused/[^/?#]+$")

def clean_name(s: str) -> str:
    n = SCHEDULE_SPLIT.split(s or "")[0].strip()
    m = CITY_PREFIX_RE.match(n)
    if m:
        rest = m.group(2).strip()
        if 'selver' in rest.lower() and rest.lower() != 'selver':
            n = rest
    n = re.sub(r'\s{2,}', ' ', n)
    m = CAP_NAME.search(n)
    if m:
        n = m.group(1)
    if BAD_NAME.match(n):
        return ''
    if re.search(r'\be-?selver\b', n, re.I):
        return ''
    return n.strip()

def parse_coords_or_query_from_maps(href: str):
    if not href:
        return (None, None, None)
    try:
        m = re.search(r'/@([0-9\.\-]+),([0-9\.\-]+)', href)
        if m:
            return (float(m.group(1)), float(m.group(2)), None)
        u = urlparse(href)
        qs = parse_qs(u.query)
        if 'q' in qs and qs['q']:
            return (None, None, unquote(qs['q'][0]))
    except Exception:
        pass
    return (None, None, None)

def geocode(addr: str):
    if not addr or not GEOCODE:
        return (None, None)
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": addr, "format": "json", "limit": 1}
    headers = {"User-Agent": UA}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=25)
        r.raise_for_status()
        arr = r.json()
        if not arr:
            return (None, None)
        return float(arr[0]["lat"]), float(arr[0]["lon"])
    except Exception:
        return (None, None)

# ------------ scraping (detail pages) ------------

def _accept_cookies(page):
    for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
        try:
            page.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1000)
            return
        except Exception:
            pass

def _address_from_textblob(text: str) -> str | None:
    parts = [x.strip() for x in re.split(r' ?[•\u2022\u00B7\u25CF\|;/] ?|\n', text) if x.strip()]
    candidates = []
    for ln in parts:
        if ADDR_TOKEN.search(ln) and re.search(r'\d', ln) and 6 <= len(ln) <= 160 and 'selver' not in ln.lower():
            candidates.append(ln)
    if candidates:
        return sorted(candidates, key=len)[0]
    return None

def scrape_all_detail_pages() -> dict[str, dict]:
    """
    Collect detail links from /kauplused and then visit each detail page to fetch:
      name, address text, and Google Maps link ('Leia kaardilt').
    Returns {name: {"address": str|None, "href": str|None}}
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(user_agent=UA, locale="et-EE", viewport={"width": 1280, "height": 900})
        page = ctx.new_page()

        page.goto("https://www.selver.ee/kauplused", wait_until="domcontentloaded", timeout=60000)
        _accept_cookies(page)
        try:
            page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass

        hrefs = set(page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)"))
        detail_urls = sorted(h for h in hrefs if DETAIL_HREF_RE.match(h))

        entries: dict[str, dict] = {}

        for url in detail_urls:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
                _accept_cookies(page)
                try:
                    page.wait_for_load_state("networkidle", timeout=4000)
                except Exception:
                    pass
            except Exception:
                continue

            # name from h1/crumb
            name = ""
            for sel in ["h1", "main h1", "article h1", "h2"]:
                try:
                    if page.locator(sel).count() > 0:
                        name = page.locator(sel).first.inner_text(timeout=1500).strip()
                        break
                except Exception:
                    pass
            name = clean_name(name) or clean_name(url.rstrip("/").split("/")[-1].replace("-", " ").title())
            if not name:
                continue

            # maps link (“Leia kaardilt”) or any Google Maps href
            href = None
            for sel in [
                "a:has-text('Leia kaardilt')",
                "a[href*='google.com/maps']",
                "a[href*='goo.gl/maps']",
                "a[href*='maps.app.goo.gl']",
            ]:
                try:
                    if page.locator(sel).count() > 0:
                        href = page.locator(sel).first.get_attribute("href")
                        if href:
                            break
                except Exception:
                    pass

            # address: first try the container around the maps link
            address = None
            try:
                if page.locator("a:has-text('Leia kaardilt')").count() > 0:
                    link = page.locator("a:has-text('Leia kaardilt')").first
                    # step up a few parents and read the block text
                    container = link
                    for _ in range(5):
                        container = container.locator("xpath=..")
                    text_blob = container.inner_text(timeout=1500)
                    address = _address_from_textblob(text_blob)
            except Exception:
                pass

            # fallback: search entire main/body text for address-like lines
            if not address:
                try:
                    scope = "main" if page.locator("main").count() > 0 else "body"
                    blob = page.locator(scope).inner_text(timeout=2000)
                    address = _address_from_textblob(blob)
                except Exception:
                    pass

            prev = entries.get(name, {})
            entries[name] = {
                "address": prev.get("address") or address,
                "href": prev.get("href") or href,
            }

        try:
            ctx.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

    return {k: v for k, v in entries.items() if k and not BAD_NAME.match(k)}

# ------------ DB helpers ------------

def db():
    return psycopg2.connect(DB_URL)

def ensure_online_store():
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
            WITH existing AS (
              SELECT 1 FROM stores
              WHERE lower(chain)=lower(%s) AND COALESCE(is_online,false)=true
              LIMIT 1
            )
            INSERT INTO stores (name, chain, is_online)
            SELECT %s, %s, TRUE
            WHERE NOT EXISTS (SELECT 1 FROM existing);
        """, (CHAIN, ONLINE_NAME, CHAIN))
        conn.commit()

def load_missing_rows():
    with db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
          SELECT id, name, address
          FROM stores
          WHERE chain=%s
            AND COALESCE(is_online,false)=false
            AND (
                  lat IS NULL OR lon IS NULL
               OR  COALESCE(lat,0)=0 OR COALESCE(lon,0)=0
            )
          ORDER BY name;
        """, (CHAIN,))
        return cur.fetchall()

def upsert_physical(name, address, lat, lon):
    with db() as conn:
        cur = conn.cursor()
        # insert if not exists
        cur.execute("""
          INSERT INTO stores (name, chain, is_online, address, lat, lon)
          SELECT %s, %s, FALSE, %s, %s, %s
          WHERE NOT EXISTS (
            SELECT 1 FROM stores WHERE chain=%s AND name=%s AND COALESCE(is_online,false)=false
          );
        """, (name, CHAIN, address, lat, lon, CHAIN, name))
        # non-destructive update
        cur.execute("""
          UPDATE stores
             SET address = COALESCE(address, %s),
                 lat     = COALESCE(lat, %s),
                 lon     = COALESCE(lon, %s)
           WHERE chain=%s AND name=%s AND COALESCE(is_online,false)=false;
        """, (address, lat, lon, CHAIN, name))
        conn.commit()

# ------------ flows ------------

def backfill_only_flow(web_entries):
    rows = load_missing_rows()
    if not rows:
        print("Nothing to backfill — all coords present.")
        return
    print(f"Backfilling {len(rows)} store(s) with missing/zero coords…")

    updated = 0
    for r in rows:
        name = r["name"]
        if name.lower() == ONLINE_NAME.lower():
            continue
        meta = web_entries.get(name, {})
        href = meta.get("href")
        addr_web = meta.get("address")
        addr_db  = r.get("address")

        lat = lon = None
        lt, ln, q = parse_coords_or_query_from_maps(href)
        if lt and ln:
            lat, lon = lt, ln
        else:
            query = addr_db or addr_web or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr_db='{addr_db}' | addr_web='{addr_web}' | latlon={lat,lon}")
        else:
            if lat is not None and lon is not None:
                upsert_physical(name, addr_web, lat, lon)
                updated += 1
                print(f"[OK] {name} ← ({lat:.6f},{lon:.6f})")
            else:
                print(f"[MISS] {name} — still no coordinates")

        time.sleep(0.8)  # polite geocoding
    print(f"Backfill done. Updated {updated}/{len(rows)}.")

def full_seed_flow(web_entries):
    ensure_online_store()
    print(f"Seeding {len(web_entries)} Selver/Delice physical names…")

    for name, meta in web_entries.items():
        if name.lower() == ONLINE_NAME.lower():
            continue
        href = meta.get("href")
        addr = meta.get("address")

        lat = lon = None
        lt, ln, q = parse_coords_or_query_from_maps(href)
        if lt and ln:
            lat, lon = lt, ln
        else:
            query = addr or q or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr='{addr}' | latlon={lat,lon}")
        else:
            upsert_physical(name, addr, lat, lon)

        time.sleep(0.8)  # polite geocoding

    print("Seed/refresh complete.")

# ------------ main ------------

def main():
    web_entries = scrape_all_detail_pages()
    print(f"Scraped {len(web_entries)} physical store detail pages from Selver.")
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

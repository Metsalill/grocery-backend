#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape https://www.selver.ee/kauplused → store detail pages, and either:
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

import os, re, time, sys
import psycopg2, psycopg2.extras
import requests
from urllib.parse import urlparse, parse_qs, unquote, urljoin
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

UA = "grocery-backend/seed-selver-stores (+github-actions)"

BASE = "https://www.selver.ee"

# schedules like "E-P 08:00–23:00"
SCHEDULE_SPLIT = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)

CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

# address heuristics
ADDR_TOKEN = re.compile(r'\b(mnt|maantee|tee|tn|pst|puiestee|tänav|väljak|keskus)\b', re.I)

def keyize(name: str) -> str:
    """Case-fold + collapse spaces for robust dict keys."""
    return re.sub(r'\s+', ' ', (name or '').strip()).casefold()

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
    """Try to get exact coords from a Google Maps link; else return the 'q=' query."""
    if not href:
        return (None, None, None)
    try:
        m = re.search(r'/@([0-9.\-]+),([0-9.\-]+)', href)
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

# ------------ scraping (DETAIL PAGES) ------------

def _extract_address_near(anchor):
    """Given a BeautifulSoup anchor, try to find an address-like line in nearby text."""
    def scan_text(txt: str):
        parts = [x.strip() for x in re.split(r' ?[•\u2022\u00B7\u25CF\|;/] ?|\n', txt) if x.strip()]
        for ln in parts:
            if ADDR_TOKEN.search(ln) and re.search(r'\d', ln) and 6 <= len(ln) <= 140 and 'selver' not in ln.lower():
                return ln
        return None

    node = anchor
    for _ in range(4):  # climb a few ancestors
        if not node: break
        if node.parent:
            node = node.parent
            txt = node.get_text(" ", strip=True)
            found = scan_text(txt)
            if found:
                return found
    return None

def scrape_detail_pages():
    """
    Visit the list once, collect detail-page links, then visit each detail page and
    pull {name -> {address, href}}.
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, locale="et-EE", viewport={"width": 1280, "height": 900})
        p = ctx.new_page()

        p.goto(urljoin(BASE, "/kauplused"), wait_until="domcontentloaded", timeout=60000)
        # Accept cookies if present
        for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
            try:
                p.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
                break
            except Exception:
                pass
        p.wait_for_timeout(1200)

        # Collect unique detail links under /kauplused/slug
        links = set()
        for a in p.locator("a[href*='/kauplused/']").all():
            try:
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                href = urljoin(BASE, href)
                # keep only actual detail pages (exclude the index itself)
                if re.search(r'/kauplused/[^/?#]+$', href):
                    links.add(href)
            except Exception:
                pass

        entries = {}  # keyed by keyize(name)
        for href in sorted(links):
            try:
                p.goto(href, wait_until="domcontentloaded", timeout=45000)
                html = p.content()
            except Exception:
                continue

            soup = BeautifulSoup(html, "html.parser")
            # H1 is the store name on detail pages
            h1 = soup.find(["h1", "h2"])
            raw_name = (h1.get_text(strip=True) if h1 else "") or (soup.title.get_text(strip=True).split("|")[0] if soup.title else "")
            name = clean_name(raw_name) or raw_name.strip()
            if not name or BAD_NAME.match(name):
                continue

            # Google Maps link: "Leia kaardilt" anchor, or any maps-looking link
            a = soup.find("a", string=re.compile(r"Leia kaardilt", re.I))
            if not a:
                a = soup.find("a", href=re.compile(r"(google\.[^/]+/maps|goo\.gl/maps|maps\.app\.goo\.gl)", re.I))
            gmaps_href = a["href"] if a and a.has_attr("href") else None

            # Address – try close to the anchor; otherwise scan page text for an address-like line
            address = None
            if a:
                address = _extract_address_near(a)
            if not address:
                txt = soup.get_text(" ", strip=True)
                m = re.search(r'([A-ZÄÖÜÕa-zäöüõ0-9\.\-/ ]+(?:mnt|maantee|tee|tn|pst|puiestee|tänav|väljak|keskus)[^,]{0,40},\s*[A-ZÄÖÜÕa-zäöüõ \-]+(?:,\s*\d{4,6})?)', txt)
                if m:
                    address = m.group(1).strip()

            entries[keyize(name)] = {
                "name": name,
                "address": address,
                "href": gmaps_href,
                "detail_url": href,
            }

        ctx.close()
        browser.close()

    print(f"Scraped {len(entries)} physical store detail pages from Selver.")
    return entries

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

        meta = web_entries.get(keyize(name), {})
        href = meta.get("href")
        addr_web = meta.get("address")
        addr_db  = r.get("address")

        # Prefer exact coords from google maps link
        lat = lon = None
        lt, ln, q = parse_coords_or_query_from_maps(href)
        if lt and ln:
            lat, lon = lt, ln

        if lat is None or lon is None:
            # Try DB address, then scraped, then a generic query
            query = addr_db or addr_web or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr_db='{addr_db}' | addr_web='{addr_web}' | latlon={(lat,lon)}")
        else:
            if lat is not None and lon is not None:
                # keep scraped address if we have it; DB address otherwise unchanged
                upsert_physical(name, addr_web or addr_db, lat, lon)
                updated += 1
                print(f"[OK] {name} ← ({lat:.6f},{lon:.6f})")
            else:
                print(f"[MISS] {name} — still no coordinates")

        time.sleep(1)  # polite geocoding
    print(f"Backfill done. Updated {updated}/{len(rows)}.")

def full_seed_flow(web_entries):
    ensure_online_store()
    print(f"Seeding {len(web_entries)} Selver/Delice physical names…")

    for _, meta in web_entries.items():
        name = meta["name"]
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
            print(f"[DRY] {name}: addr='{addr}' | latlon={(lat,lon)}")
        else:
            upsert_physical(name, addr, lat, lon)

        time.sleep(1)  # polite geocoding

    print("Seed/refresh complete.")

# ------------ main ------------

def main():
    web_entries = scrape_detail_pages()  # keyized-name -> {name,address,href}
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

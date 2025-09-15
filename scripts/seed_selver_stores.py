#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape https://www.selver.ee/kauplused → store list, and either:
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
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

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

# schedules like "E-P 08:00–23:00"
SCHEDULE_SPLIT = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)

CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

ADDR_TOKEN = re.compile(r'\b(mnt|maantee|tee|tn|pst|puiestee|väljak|keskus|turg|tee\.|tn\.)\b', re.I)


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


# ------------ scraping ------------

BASE = "https://www.selver.ee"

def _accept_cookies(page):
    for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
        try:
            page.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
            break
        except Exception:
            pass

def discover_store_detail_links(page) -> list[str]:
    """Return a list of absolute hrefs to /kauplused/<slug> pages."""
    hrefs = set()
    # gather all anchors with /kauplused/ in href
    anchors = page.locator("a[href*='/kauplused/']").all()
    for a in anchors:
        try:
            h = a.get_attribute("href") or ""
            if not h:
                continue
            if h.startswith("/"):
                h = BASE + h
            if "/kauplused/" in h and not h.endswith("/kauplused"):
                # normalize hash/query
                h = h.split("#", 1)[0].split("?", 1)[0]
                hrefs.add(h)
        except Exception:
            continue
    return sorted(hrefs)

def extract_address_and_maps_from_detail(page) -> tuple[str|None, str|None, str|None]:
    """From a store detail page, return (name, address, gmaps_href)."""
    # name
    name = None
    try:
        name = (page.locator("h1").first.inner_text(timeout=1500) or "").strip()
        name = clean_name(name)
    except Exception:
        pass

    # maps link by explicit text
    gmaps = None
    for sel in [
        "a:has-text('Leia kaardilt')",
        "a:has-text('Leidke kaardilt')",
        "a[href*='google.'][href*='/maps']",
        "a[href*='goo.gl/maps']",
        "a[href*='maps.app.goo.gl']",
    ]:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                gmaps = el.get_attribute("href")
                if gmaps:
                    break
        except Exception:
            pass

    # address – several heuristics
    address = None
    try:
        # This class has worked on recent pages
        el = page.locator(".Store__details").first
        if el.count() > 0:
            txt = el.inner_text(timeout=1200).strip()
            # Often the block contains only the address
            if ADDR_TOKEN.search(txt) and re.search(r'\d', txt):
                address = txt
    except Exception:
        pass

    if not address:
        # Try finding a text node that looks like an address anywhere in the main article
        try:
            main_html = page.locator("main").first.inner_text(timeout=1500)
            # split to lines and pick the shortest plausible addr
            lines = [ln.strip() for ln in re.split(r"[\n\r]+", main_html) if ln.strip()]
            cands = [ln for ln in lines if ADDR_TOKEN.search(ln) and re.search(r"\d", ln) and 6 <= len(ln) <= 140]
            if cands:
                address = sorted(cands, key=len)[0]
        except Exception:
            pass

    return name, address, gmaps


def scrape_kauplused() -> dict[str, dict]:
    """
    Render /kauplused, collect all detail pages, visit each, and extract:
      {name: {"address": str|None, "href": google_maps_link|None}}
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            user_agent=UA,
            locale="et-EE",
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()

        # Open list page
        page.goto(f"{BASE}/kauplused", wait_until="domcontentloaded", timeout=60000)
        _accept_cookies(page)
        # small wait for lazy content
        try:
            page.wait_for_load_state("networkidle", timeout=2000)
        except Exception:
            pass

        detail_links = discover_store_detail_links(page)
        # De-duplicate by slug
        seen_slugs = set()
        unique_links = []
        for h in detail_links:
            slug = h.rstrip("/").rsplit("/", 1)[-1]
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                unique_links.append(h)

        results: dict[str, dict] = {}
        ok = 0
        for href in unique_links:
            try:
                page.goto(href, wait_until="domcontentloaded", timeout=45000)
                _accept_cookies(page)
                try:
                    page.wait_for_load_state("networkidle", timeout=1000)
                except Exception:
                    pass

                name, addr, gmaps = extract_address_and_maps_from_detail(page)
                if name and not BAD_NAME.match(name):
                    prev = results.get(name, {})
                    results[name] = {
                        "address": prev.get("address") or addr,
                        "href": prev.get("href") or gmaps,
                    }
                    ok += 1
            except PWTimeout:
                continue
            except Exception:
                continue
            finally:
                time.sleep(0.15)  # polite

        try:
            ctx.close()
            browser.close()
        except Exception:
            pass

    print(f"Scraped {ok} physical store detail pages from Selver.")
    # filter empties
    return {k: v for k, v in results.items() if k}


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
            # Prefer the address we scraped from detail page; if not, DB; else fallback query
            query = addr_web or addr_db or q or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr_db='{addr_db}' | addr_web='{addr_web}' | latlon={lat,lon}")
        else:
            if lat is not None and lon is not None:
                upsert_physical(name, addr_web or addr_db, lat, lon)
                updated += 1
                print(f"[OK] {name} ← ({lat:.6f},{lon:.6f})")
            else:
                print(f"[MISS] {name} — still no coordinates")

        time.sleep(0.6)  # polite geocoding
    print(f"Backfill done. Updated {updated}/{len(rows)}.")


def full_seed_flow(web_entries):
    ensure_online_store()
    print(f"Seeding/refreshing {len(web_entries)} Selver/Delice physical names…")

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

        time.sleep(0.6)  # polite geocoding

    print("Seed/refresh complete.")


# ------------ main ------------

def main():
    web_entries = scrape_kauplused()  # {name: {address, href}}
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

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
from typing import Dict, Tuple, Optional, List

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
SPLIT_SCHEDULE = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)

CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

# address tokens (Estonian)
ADDR_TOKEN = re.compile(r'\b(mnt|tee|tn|pst|puiestee|maantee|tänav|keskus|turg|väljak)\b', re.I)

MAPS_HREF = re.compile(r'(google\.[^/]+/maps|goo\.gl/maps|maps\.app\.goo\.gl)', re.I)


def clean_name(s: str) -> str:
    n = SPLIT_SCHEDULE.split(s or "")[0].strip()
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


def parse_coords_or_query_from_maps(href: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
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


def geocode(addr: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
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

# ------------ rendering ------------

def render_html(url: str) -> str:
    """Render a page with Playwright and return HTML."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, locale="et-EE",
                                  viewport={"width": 1280, "height": 900})
        p = ctx.new_page()
        p.goto(url, wait_until="domcontentloaded", timeout=60000)
        # cookie banner (best-effort)
        for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
            try:
                p.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
                break
            except Exception:
                pass
        p.wait_for_timeout(800)
        html = p.content()
        ctx.close()
        browser.close()
    return html

# ------------ scraping ------------

def scrape_list_detail_urls() -> List[str]:
    """
    From the list page, collect all /kauplused/* store detail URLs.
    We'll dedup and visit each to read name/address/maps exactly.
    """
    html = render_html(urljoin(BASE, "/kauplused"))
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/kauplused/" in href:
            full = urljoin(BASE, href)
            urls.append(full)
    # dedupe while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def scrape_detail(detail_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Open a store detail page and return (clean_name, address, maps_href).
    - address: single clean line like "Street 12, City, 12345"
    - maps_href: the 'Leia kaardilt' Google Maps link
    """
    html = render_html(detail_url)
    soup = BeautifulSoup(html, "html.parser")

    # 1) store name from H1/H2
    name = None
    for tag in soup.select("h1, h2"):
        nm = clean_name(tag.get_text(" ", strip=True))
        if nm:
            name = nm
            break

    # 2) maps link "Leia kaardilt"
    maps_href = None
    a = soup.find("a", string=re.compile(r"Leia kaardilt", re.I))
    if a and a.has_attr("href"):
        maps_href = urljoin(BASE, a["href"])
    if not maps_href:
        a2 = soup.find("a", href=MAPS_HREF)
        if a2 and a2.has_attr("href"):
            maps_href = a2["href"]

    # 3) address line: search within detail/info-ish blocks first
    address = None
    candidates = soup.find_all(True, class_=re.compile(r"(Store__details|details|info|column|Store__info)", re.I))
    # if nothing with those classes, allow whole page search fallback
    blocks = candidates if candidates else [soup]
    for blk in blocks:
        # iterate *text nodes* only and keep short plausible address lines
        txts = [t.strip() for t in blk.find_all(string=True) if t and t.strip()]
        for ln in txts:
            if ADDr_LINE_OK(ln):
                address = ln
                break
        if address:
            break

    return (name, address, maps_href)


def ADDr_LINE_OK(ln: str) -> bool:
    """Heuristic: a single line looks like an address, not phone/email/CTA."""
    if not ln: 
        return False
    if "@" in ln:     # email
        return False
    if re.fullmatch(r'[0-9\s]+', ln):  # just phone
        return False
    if "Leia kaardilt" in ln:
        return False
    if len(ln) < 8 or len(ln) > 120:
        return False
    if not re.search(r'\d', ln):
        return False
    if not ADDR_TOKEN.search(ln):
        return False
    return True


def build_web_entries() -> Dict[str, Dict[str, Optional[str]]]:
    """
    Visit each detail page → {name: {"address": str|None, "href": maps_href|None}}
    """
    urls = scrape_list_detail_urls()
    out: Dict[str, Dict[str, Optional[str]]] = {}
    count = 0
    for u in urls:
        try:
            nm, addr, href = scrape_detail(u)
            if nm and not BAD_NAME.match(nm):
                out[nm] = {"address": addr, "href": href}
        except Exception:
            # keep going if any single page fails
            pass
        count += 1
        # be polite
        time.sleep(0.5)
    return out

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

def backfill_only_flow(web_entries: Dict[str, Dict[str, Optional[str]]]):
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
        addr_web = meta.get("address")
        href = meta.get("href")
        addr_db  = r.get("address")

        lat = lon = None

        lt, ln, q = parse_coords_or_query_from_maps(href)
        if lt and ln:
            lat, lon = lt, ln
        else:
            # prefer DB address, then web address, then a generic query
            query = addr_db or addr_web or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr_db='{addr_db}' | addr_web='{addr_web}' | latlon={lat, lon}")
        else:
            if lat is not None and lon is not None:
                upsert_physical(name, addr_web or addr_db, lat, lon)
                updated += 1
                print(f"[OK] {name} ← ({lat:.6f},{lon:.6f})")
            else:
                print(f"[MISS] {name} — still no coordinates")

        time.sleep(0.6)  # polite

    print(f"Backfill done. Updated {updated}/{len(rows)}.")


def full_seed_flow(web_entries: Dict[str, Dict[str, Optional[str]]]):
    ensure_online_store()
    print(f"Seeding {len(web_entries)} Selver/Delice physical names…")

    for name, meta in web_entries.items():
        if name.lower() == ONLINE_NAME.lower():
            continue

        addr = meta.get("address")
        href = meta.get("href")

        lat = lon = None
        lt, ln, q = parse_coords_or_query_from_maps(href)
        if lt and ln:
            lat, lon = lt, ln
        else:
            query = addr or q or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr='{addr}' | latlon={lat, lon}")
        else:
            upsert_physical(name, addr, lat, lon)

        time.sleep(0.6)  # polite

    print("Seed/refresh complete.")

# ------------ main ------------

def main():
    # Build a fresh snapshot by visiting detail pages (reliable address + maps href)
    web_entries = {}
    print("Scraping Selver store detail pages…")
    try:
        web_entries = build_web_entries()
    except Exception as e:
        print(f"Warning: failed to build web entries snapshot: {e}")

    print(f"Scraped {len(web_entries)} physical store detail pages from Selver.")
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

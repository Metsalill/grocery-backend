#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver stores → seed/backfill store rows with address + lat/lon.
"""

from __future__ import annotations
import os, re, time, sys
from typing import Dict, Tuple, Optional, List

import psycopg2, psycopg2.extras
import requests
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------------- config ----------------

DB_URL = os.environ.get("DATABASE_URL") or os.environ.get("RW_DATABASE_URL")
if not DB_URL:
    print("Set DATABASE_URL or RW_DATABASE_URL", file=sys.stderr)
    sys.exit(1)

BACKFILL_ONLY = os.environ.get("BACKFILL_ONLY", "1") == "1"
GEOCODE       = os.environ.get("GEOCODE", "1") == "1"
DRY_RUN       = os.environ.get("DRY_RUN", "0") == "1"
CHAIN         = os.environ.get("CHAIN", "Selver")
ONLINE_NAME   = os.environ.get("ONLINE_NAME", "e-Selver")

UA   = "grocery-backend/seed-selver-stores (+github-actions)"
BASE = "https://www.selver.ee"

SPLIT_SCHEDULE = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)
CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

ADDR_TOKEN = re.compile(r'\b(mnt|tee|tn|pst|puiestee|maantee|tänav|keskus|turg|väljak)\b', re.I)
MAPS_HREF  = re.compile(r'(google\.[^/]+/maps|goo\.gl/maps|maps\.app\.goo\.gl)', re.I)

# ---------------- helpers ----------------

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
    headers = {"User-Agent": UA}
    url = "https://nominatim.openstreetmap.org/search"

    def _req(q: str):
        try:
            r = requests.get(url, params={"q": q, "format":"json", "limit":1},
                             headers=headers, timeout=25)
            r.raise_for_status()
            arr = r.json()
            if arr:
                return float(arr[0]["lat"]), float(arr[0]["lon"])
        except Exception:
            return (None, None)
        return (None, None)

    # try with explicit country hint first (improves hit-rate)
    q1 = addr if re.search(r'\b(Eesti|Estonia)\b', addr, re.I) else f"{addr}, Estonia"
    lat, lon = _req(q1)
    if lat is not None and lon is not None:
        return lat, lon
    # second attempt: raw address (sometimes already full)
    return _req(addr)

# ---------------- rendering ----------------

def render_html(url: str) -> str:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, locale="et-EE",
                                  viewport={"width":1280, "height":900})
        p = ctx.new_page()
        p.goto(url, wait_until="domcontentloaded", timeout=60000)
        for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
            try:
                p.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
                break
            except Exception:
                pass
        p.wait_for_timeout(800)
        html = p.content()
        ctx.close(); browser.close()
    return html

# ---------------- scraping ----------------

def scrape_list_detail_urls() -> List[str]:
    html = render_html(urljoin(BASE, "/kauplused"))
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/kauplused/" in href:
            urls.append(urljoin(BASE, href))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def _extract_maps_href(soup: BeautifulSoup) -> Optional[str]:
    # Prefer explicit text “Leia kaardilt”, even if split across spans
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True)
        href = a["href"]
        if re.search(r'leia\s+kaardilt', txt, re.I) or MAPS_HREF.search(href):
            # keep absolute URLs as-is; otherwise join with site
            return href if href.startswith("http") else urljoin(BASE, href)
    return None

def _looks_like_address(ln: str) -> bool:
    if not ln or "@" in ln:
        return False
    if re.fullmatch(r'[0-9\s]+', ln):
        return False
    if "Leia kaardilt" in ln:
        return False
    if len(ln) < 8 or len(ln) > 140:
        return False
    if not re.search(r'\d', ln):
        return False
    if not ADDR_TOKEN.search(ln):
        return False
    return True

def scrape_detail(detail_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    html = render_html(detail_url)
    soup = BeautifulSoup(html, "html.parser")

    name = None
    for tag in soup.select("h1, h2"):
        cand = clean_name(tag.get_text(" ", strip=True))
        if cand:
            name = cand
            break

    maps_href = _extract_maps_href(soup)

    address = None
    # prefer the info/column blocks; fall back to whole page if needed
    blocks = soup.find_all(True, class_=re.compile(r"(Store__details|details|info|column|Store__info)", re.I)) or [soup]
    for blk in blocks:
        for ln in [t.strip() for t in blk.find_all(string=True) if t and t.strip()]:
            if _looks_like_address(ln):
                address = ln
                break
        if address:
            break

    return (name, address, maps_href)

def build_web_entries() -> Dict[str, Dict[str, Optional[str]]]:
    entries: Dict[str, Dict[str, Optional[str]]] = {}
    for u in scrape_list_detail_urls():
        try:
            nm, addr, href = scrape_detail(u)
            if nm and not BAD_NAME.match(nm):
                entries[nm] = {"address": addr, "href": href}
        except Exception:
            pass
        time.sleep(0.5)
    return entries

# ---------------- DB ----------------

def db():
    return psycopg2.connect(DB_URL)

def ensure_online_store():
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
            WITH existing AS (
              SELECT 1 FROM stores
               WHERE lower(chain)=lower(%s)
                 AND COALESCE(is_online,false)=true
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
             AND (lat IS NULL OR lon IS NULL
                  OR COALESCE(lat,0)=0 OR COALESCE(lon,0)=0)
           ORDER BY name;
        """, (CHAIN,))
        return cur.fetchall()

def upsert_physical(name, address, lat, lon):
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO stores (name, chain, is_online, address, lat, lon)
          SELECT %s, %s, FALSE, %s, %s, %s
           WHERE NOT EXISTS (
             SELECT 1 FROM stores
              WHERE chain=%s AND name=%s AND COALESCE(is_online,false)=false
           );
        """, (name, CHAIN, address, lat, lon, CHAIN, name))
        cur.execute("""
          UPDATE stores
             SET address = COALESCE(address, %s),
                 lat     = COALESCE(lat, %s),
                 lon     = COALESCE(lon, %s)
           WHERE chain=%s AND name=%s AND COALESCE(is_online,false)=false;
        """, (address, lat, lon, CHAIN, name))
        conn.commit()

# ---------------- flows ----------------

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
        meta     = web_entries.get(name, {})
        addr_web = meta.get("address")
        href     = meta.get("href")
        addr_db  = r.get("address")

        lat = lon = None

        lt, ln, q = parse_coords_or_query_from_maps(href)
        if lt and ln:
            lat, lon = lt, ln
        else:
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
        time.sleep(0.6)

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
        time.sleep(0.6)
    print("Seed/refresh complete.")

# ---------------- main ----------------

def main():
    print("Scraping Selver store detail pages…")
    web_entries = build_web_entries()
    print(f"Scraped {len(web_entries)} physical store detail pages from Selver.")
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

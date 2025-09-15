#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape https://www.selver.ee/kauplused → store list, and either:
  - FULL SEED: insert physical stores and set address/coords non-destructively.
  - BACKFILL: only update rows in DB that are missing/zero lat/lon.

Also ensures a single online row (e-Selver) exists with is_online=TRUE.

Env:
  DATABASE_URL / RW_DATABASE_URL   (required)
  BACKFILL_ONLY   1|0  default 1
  GEOCODE         1|0  default 1
  DRY_RUN         1|0  default 0
  CHAIN                 default "Selver"
  ONLINE_NAME           default "e-Selver"
"""

import os, re, time, sys, json
import psycopg2, psycopg2.extras
import requests
from urllib.parse import urlparse, parse_qs, unquote, urljoin
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

BASE = "https://www.selver.ee"
UA = "grocery-backend/seed-selver-stores (+github-actions)"

# schedules like "E-P 08:00–23:00"
SCHEDULE_SPLIT = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)

CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

ADDR_TOKEN = re.compile(r'\b(mnt|tee|tn|pst|puiestee|maantee|tänav|keskus|turg|väljak)\b', re.I)

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
            q = unquote(qs['q'][0])
            mm = re.match(r'\s*([0-9\.\-]+)\s*,\s*([0-9\.\-]+)\s*$', q)
            if mm:
                return (float(mm.group(1)), float(mm.group(2)), None)
            return (None, None, q)
    except Exception:
        pass
    return (None, None, None)

def nominatim(query: str):
    """Geocode with retries, EE country restriction, and 'Estonia' suffix."""
    if not query or not GEOCODE:
        return (None, None)
    url = "https://nominatim.openstreetmap.org/search"
    q = f"{query}, Estonia" if "estonia" not in query.lower() else query
    params = {
        "q": q, "format": "json", "limit": 1,
        "countrycodes": "ee", "addressdetails": 0, "accept-language": "et"
    }
    headers = {"User-Agent": UA}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=25)
            if r.status_code == 429:
                time.sleep(2 + attempt); continue
            r.raise_for_status()
            arr = r.json()
            if arr:
                return float(arr[0]["lat"]), float(arr[0]["lon"])
            if attempt == 0:
                params["q"] = re.sub(r'\b\d{4,6}\b', '', params["q"]).strip(', ')
        except Exception:
            time.sleep(1 + attempt)
    return (None, None)

# ------------ scraping (detail pages) ------------

def collect_store_detail_urls(page) -> list[str]:
    """From /kauplused collect unique links to store detail pages (collapsed items included)."""
    page.goto(f"{BASE}/kauplused", wait_until="domcontentloaded", timeout=60000)
    # cookie
    for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
        try:
            page.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
            break
        except Exception:
            pass
    # light scroll to trigger any lazy DOM
    try:
        for y in range(0, 5000, 800):
            page.evaluate("window.scrollTo(0, arguments[0])", y)
            page.wait_for_timeout(150)
        page.wait_for_load_state("networkidle", timeout=2000)
    except Exception:
        pass

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    hrefs = set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/kauplused/" in h and not h.rstrip("/").endswith("/kauplused"):
            hrefs.add(urljoin(BASE, h.split("#", 1)[0]))
    return sorted(hrefs)

def best_addr_from(container):
    if not container:
        return None
    txt = container.get_text(" ", strip=True)
    parts = [x.strip() for x in re.split(r' ?[•\u2022\u00B7\u25CF\|;/] ?|\n', txt) if x.strip()]
    cands = []
    for ln in parts:
        if ADDR_TOKEN.search(ln) and re.search(r'\d', ln) and 6 <= len(ln) <= 140 and 'selver' not in ln.lower():
            cands.append(ln)
    return (sorted(cands, key=len)[0] if cands else None)

def extract_from_detail_html(html: str):
    """Return (name, address, maps_href) from a detail page's HTML."""
    soup = BeautifulSoup(html, "html.parser")
    name = ""
    h1 = soup.find(["h1", "h2"])
    if h1:
        name = clean_name(h1.get_text(" ", strip=True))

    # JSON-LD address fallback
    address_jsonld = None
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(s.string or "{}")
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for n in nodes:
            if not isinstance(n, dict): continue
            addr = n.get("address")
            if isinstance(addr, dict):
                line = " ".join(str(addr.get(k) or "") for k in ["streetAddress","postalCode","addressLocality"]).strip()
                if line:
                    address_jsonld = line
                    break
        if address_jsonld:
            break

    a_maps = soup.find("a", href=re.compile(r'(google\.[^/]+/maps|goo\.gl/maps|maps\.app\.goo\.gl)', re.I))
    maps_href = a_maps["href"] if a_maps and a_maps.has_attr("href") else None

    address = None
    if a_maps:
        address = best_addr_from(a_maps.parent) or best_addr_from(a_maps.parent.parent)

    if not address:
        main = soup.find("main") or soup
        address = best_addr_from(main) or address_jsonld

    return name, address, maps_href

def scrape_detail_pages() -> dict[str, dict]:
    """
    Visit each store detail page and produce:
      {name: {"address": str|None, "href": google_maps_link|None}}
    """
    out = {}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, locale="et-EE", viewport={"width": 1280, "height": 900})
        p = ctx.new_page()

        links = collect_store_detail_urls(p)
        # de-duplicate by last slug
        seen_slugs = set()
        detail_links = []
        for h in links:
            slug = h.rstrip("/").split("/")[-1]
            if slug not in seen_slugs:
                seen_slugs.add(slug)
                detail_links.append(h)

        for url in detail_links:
            try:
                p.goto(url, wait_until="domcontentloaded", timeout=60000)
                p.wait_for_timeout(350)
                html = p.content()
            except PWTimeout:
                continue
            except Exception:
                continue

            name, addr, href = extract_from_detail_html(html)
            if not name or BAD_NAME.match(name):
                continue
            prev = out.get(name, {})
            out[name] = {
                "address": prev.get("address") or addr,
                "href": prev.get("href") or href
            }

        try:
            ctx.close(); browser.close()
        except Exception:
            pass

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
            query = addr_db or addr_web or q or f"{name}, Estonia"
            lat, lon = nominatim(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr_db='{addr_db}' | addr_web='{addr_web}' | latlon={lat,lon}")
        else:
            if lat is not None and lon is not None:
                upsert_physical(name, addr_web or addr_db, lat, lon)
                updated += 1
                print(f"[OK] {name} ← ({lat:.6f},{lon:.6f})")
            else:
                print(f"[MISS] {name} — still no coordinates (addr_db='{addr_db}' | addr_web='{addr_web}')")

        time.sleep(1)  # polite geocoding
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
            lat, lon = nominatim(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr='{addr}' | latlon={lat,lon}")
        else:
            upsert_physical(name, addr, lat, lon)

        time.sleep(1)  # polite geocoding

    print("Seed/refresh complete.")

# ------------ main ------------

def main():
    web_entries = scrape_detail_pages()
    print(f"Scraped {len(web_entries)} physical store detail pages from Selver.")
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

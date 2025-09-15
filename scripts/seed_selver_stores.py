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

import os, re, time, sys, json
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

# schedules like "E-P 08:00–23:00"
SCHEDULE_SPLIT = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)

CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)

# tokens that make a line look like an Estonian street address
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

def fetch_detail_meta(detail_url: str):
    """Fetch a store detail page and try to extract an address and maps link."""
    if not detail_url:
        return (None, None)
    try:
        hdrs = {"User-Agent": UA}
        r = requests.get(detail_url, headers=hdrs, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # any google maps link on detail page?
        a = soup.find("a", href=re.compile(r"(google\.[^/]+/maps|goo\.gl/maps|maps\.app\.goo\.gl)", re.I))
        href = a["href"] if a and a.has_attr("href") else None

        # pick a plausible address line from visible text
        text = soup.get_text(" ", strip=True)
        parts = [x.strip() for x in re.split(r" ?[•\u2022\u00B7\u25CF\|;/] ?|\n", text) if x.strip()]
        candidates = [ln for ln in parts
                      if ADDR_TOKEN.search(ln) and re.search(r"\d", ln)
                      and 8 <= len(ln) <= 120 and "selver" not in ln.lower()]
        address = sorted(candidates, key=len)[0] if candidates else None

        return (address, href)
    except Exception:
        return (None, None)


def scrape_kauplused():
    """
    Render the list page once with Playwright and extract:
      {name: {"address": str|None, "href": google_maps_link|None}}
    If address/href missing on the list, fetch the detail page and try again.
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA, locale="et-EE", viewport={"width": 1280, "height": 900})
        p = ctx.new_page()
        p.goto("https://www.selver.ee/kauplused", wait_until="domcontentloaded", timeout=60000)
        # handle cookies if present
        for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
            try:
                p.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
                break
            except Exception:
                pass
        p.wait_for_timeout(1500)
        html = p.content()
        ctx.close()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    out = {}  # name -> {"address": ..., "href": ..., "detail": ...}

    # Look for anything that looks like a store name, then search locally for address / gmaps link / detail link
    for node in soup.find_all(string=re.compile(r'(Selver|Delice)', re.I)):
        raw = str(node)
        candidates = re.findall(
            r'(?:[A-ZÄÖÜÕ][\wÄÖÜÕäöüõ\'’\- ]{1,60}\sSelver(?:\sABC)?)|Delice(?:\s+Toidupood)?',
            raw
        ) or [raw]
        for cand in candidates:
            name = clean_name(cand)
            if not name:
                continue

            # Climb up to find a "card"-ish container
            container = node.parent
            for _ in range(6):
                if not container or container.name in ("html", "body"):
                    break
                txt_here = container.get_text(" ", strip=True)
                if len(re.findall(r'(Selver|Delice)', txt_here, re.I)) > 3:
                    container = container.parent
                else:
                    break

            address = None
            href = None
            detail = None
            if container:
                # any google maps-ish link
                a = container.find("a", href=re.compile(r'(google\.[^/]+/maps|goo\.gl/maps|maps\.app\.goo\.gl)', re.I))
                if a and a.has_attr("href"):
                    href = a["href"]

                # “Rohkem infot” link → detail page (or fall back to a sluggy selver link)
                a_more = container.find("a", string=re.compile(r"Rohkem infot", re.I))
                if not a_more:
                    a_more = container.find("a", href=re.compile(r"/[a-z0-9\-]+-selver(?:-abc)?/?$", re.I))
                if a_more and a_more.has_attr("href"):
                    detail = a_more["href"]
                    if not detail.startswith("http"):
                        detail = urljoin("https://www.selver.ee/", detail.lstrip("/"))

                # scan text fragments for an address-looking piece
                text = container.get_text(" ", strip=True)
                parts = [x.strip() for x in re.split(r' ?[•\u2022\u00B7\u25CF\|;/] ?|\n', text) if x.strip()]
                addr_like = []
                for ln in parts:
                    if ADDR_TOKEN.search(ln) and re.search(r'\d', ln) and 8 <= len(ln) <= 120 and 'selver' not in ln.lower():
                        addr_like.append(ln)
                if addr_like:
                    address = sorted(addr_like, key=len)[0]

            prev = out.get(name, {})
            out[name] = {
                "address": prev.get("address") or address,
                "href":    prev.get("href")    or href,
                "detail":  prev.get("detail")  or detail,
            }

    # Second pass: augment from detail pages when list card didn't have address/href
    for name, meta in list(out.items()):
        if (not meta.get("address")) or (not meta.get("href")):
            detail = meta.get("detail")
            addr2, href2 = fetch_detail_meta(detail) if detail else (None, None)
            if addr2 and not meta.get("address"):
                meta["address"] = addr2
            if href2 and not meta.get("href"):
                meta["href"] = href2

    # Return without the internal "detail" key
    cleaned = {k: {"address": v.get("address"), "href": v.get("href")}
               for k, v in out.items()
               if k and not BAD_NAME.match(k)}
    return cleaned


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
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr='{addr}' | latlon={lat,lon}")
        else:
            upsert_physical(name, addr, lat, lon)

        time.sleep(1)  # polite geocoding

    print("Seed/refresh complete.")


# ------------ main ------------

def main():
    web_entries = scrape_kauplused()
    print(f"Scraped {len(web_entries)} physical store names from Selver Kauplused.")
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

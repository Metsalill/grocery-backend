#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver stores: seed OR backfill missing lat/lon.
- FULL SEED: insert/refresh physical stores non-destructively.
- BACKFILL: update only rows with missing/zero coords.

Env:
  DATABASE_URL / RW_DATABASE_URL  (required)
  BACKFILL_ONLY  1|0  default 1
  GEOCODE        1|0  default 1
  DRY_RUN        1|0  default 0
  CHAIN               default "Selver"
  ONLINE_NAME         default "e-Selver"
"""
import os, re, sys, time
from urllib.parse import urlparse, parse_qs, unquote

import psycopg2, psycopg2.extras
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

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

SCHEDULE_SPLIT = re.compile(r'\s+(?:E[-–]?P|E[-–]?[A-Z](?:\s*\d)?)\b')
CITY_PREFIX_RE = re.compile(r'^(Tallinn|Tartu)\s+(.*)$', re.I)
CAP_NAME = re.compile(r'(Delice(?:\s+Toidupood)?|[A-ZÄÖÜÕ][A-Za-zÄÖÜÕäöüõ0-9\'’\- ]{1,60}\sSelver(?:\sABC)?)')
BAD_NAME = re.compile(r'^(?:e-?Selver|Selver)$', re.I)
ADDR_TOKEN = re.compile(r'\b(mnt|tee|tn|pst|puiestee|maantee|tänav|keskus|turg|väljak)\b', re.I)

def clean_text(s: str | None) -> str:
    if not s: return ""
    return re.sub(r'\s+', ' ', s).strip()

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

def parse_coords_or_query_from_maps(href: str | None):
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

def geocode(q: str | None):
    if not q or not GEOCODE:
        return (None, None)
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "json", "limit": 1},
            headers={"User-Agent": UA},
            timeout=25
        )
        r.raise_for_status()
        data = r.json()
        if not data: return (None, None)
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return (None, None)

# ---------- Playwright scraping ----------

def collect_detail_urls():
    with sync_playwright() as pw:
        br = pw.chromium.launch(headless=True)
        ctx = br.new_context(user_agent=UA, locale="et-EE", viewport={"width": 1280, "height": 1000})
        p = ctx.new_page()
        p.goto("https://www.selver.ee/kauplused", wait_until="domcontentloaded", timeout=60000)
        for txt in ["Nõustun", "Nõustu", "Accept", "Allow all", "OK"]:
            try:
                p.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=1200)
                break
            except Exception:
                pass
        # prefer explicit “Rohkem infot” links
        hrefs = set()
        try:
            more = p.locator("a:has-text('Rohkem infot'), a:has-text('ROHKEM INFOT')")
            if more.count() > 0:
                hrefs.update([clean_text(h) for h in more.evaluate_all("els => els.map(e => e.href)")])
        except Exception:
            pass
        # fallback: any /kauplused/<slug> hrefs
        try:
            extra = p.eval_on_selector_all(
                "a[href*='/kauplused/']",
                "els => Array.from(new Set(els.map(e => e.href)))"
            )
            for h in extra or []:
                if re.search(r"/kauplused/[^/]+$", h):
                    hrefs.add(h)
        except Exception:
            pass
        ctx.close(); br.close()
    # filter list vs root
    urls = sorted(u for u in hrefs if re.search(r"/kauplused/[^/]+$", u))
    return urls

def scrape_detail(urls):
    out = {}
    if not urls:
        return out

    with sync_playwright() as pw:
        br = pw.chromium.launch(headless=True)
        ctx = br.new_context(user_agent=UA, locale="et-EE", viewport={"width": 1280, "height": 1000})
        page = ctx.new_page()

        for url in urls:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
                # let client-side render settle
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                continue

            # name
            name = ""
            try:
                name = clean_name(page.locator("h1").first.inner_text(timeout=3000))
            except Exception:
                pass
            if not name or BAD_NAME.match(name):
                continue

            # address
            address = None
            for sel in [".Store__details", ".store__details", "main .Store__details", "main .store__details"]:
                try:
                    if page.locator(sel).count() > 0:
                        address = clean_text(page.locator(sel).first.inner_text(timeout=3000))
                        if address:
                            break
                except Exception:
                    pass
            if not address:
                try:
                    txt = page.locator("main").first.inner_text(timeout=4000)
                    parts = [x.strip() for x in re.split(r' ?[•\u2022\u00B7\u25CF\|;/] ?|\n', txt) if x.strip()]
                    cand = []
                    for ln in parts:
                        if "Leia kaardilt" in ln:  # not an address
                            continue
                        if ADDR_TOKEN.search(ln) and re.search(r'\d', ln) and 8 <= len(ln) <= 140 and 'selver' not in ln.lower():
                            cand.append(ln)
                    if cand:
                        address = sorted(cand, key=len)[0]
                except Exception:
                    pass

            # maps link
            href = None
            try:
                a = page.locator("a:has-text('Leia kaardilt')")
                if a.count() > 0:
                    href = a.first.get_attribute("href") or None
            except Exception:
                pass
            if not href:
                try:
                    a = page.locator("a[href*='google.'][href*='maps'], a[href*='goo.gl/maps'], a[href*='maps.app.goo.gl']")
                    if a.count() > 0:
                        href = a.first.get_attribute("href") or None
                except Exception:
                    pass

            prev = out.get(name, {})
            out[name] = {
                "address": prev.get("address") or address,
                "href": prev.get("href") or href
            }

            time.sleep(0.2)  # be gentle

        ctx.close(); br.close()
    return out

def scrape_all():
    urls = collect_detail_urls()
    details = scrape_detail(urls)
    print(f"Collected {len(urls)} store URLs; scraped {len(details)} detail pages.")
    return details

# ---------- DB helpers ----------

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
        cur.execute("""
          INSERT INTO stores (name, chain, is_online, address, lat, lon)
          SELECT %s, %s, FALSE, %s, %s, %s
          WHERE NOT EXISTS (
            SELECT 1 FROM stores WHERE chain=%s AND name=%s AND COALESCE(is_online,false)=false
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

# ---------- flows ----------

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
        if lt is not None and ln is not None:
            lat, lon = lt, ln
        if lat is None or lon is None:
            query = addr_db or addr_web or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr_db='{addr_db}' | addr_web='{addr_web}' | latlon={(lat,lon)}")
        else:
            if lat is not None and lon is not None:
                upsert_physical(name, addr_web or addr_db, lat, lon)
                updated += 1
                print(f"[OK] {name} ← ({lat:.6f},{lon:.6f})")
            else:
                print(f"[MISS] {name} — still no coordinates")
        time.sleep(0.8)  # Nominatim fair use
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
        if lt is not None and ln is not None:
            lat, lon = lt, ln
        else:
            query = addr or q or f"{name}, Estonia"
            lat, lon = geocode(query)

        if DRY_RUN:
            print(f"[DRY] {name}: addr='{addr}' | latlon={(lat,lon)}")
        else:
            upsert_physical(name, addr, lat, lon)
        time.sleep(0.8)
    print("Seed/refresh complete.")

def main():
    web_entries = scrape_all()  # name -> {address, href}
    if BACKFILL_ONLY:
        backfill_only_flow(web_entries)
    else:
        full_seed_flow(web_entries)

if __name__ == "__main__":
    main()

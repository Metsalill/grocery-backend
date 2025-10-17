#!/usr/bin/env python3
import os, re, csv, json, time, sys
from urllib.parse import unquote, urlparse, parse_qs
from contextlib import closing

import psycopg2
from psycopg2.extras import execute_values
from playwright.sync_api import sync_playwright

COOP_URL = "https://www.coop.ee/kauplused"
OUT_CSV = "data/coop_stores.csv"

def slug(s: str) -> str:
    s = re.sub(r"\s+", "-", s.strip().lower())
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def parse_latlon_from_gmaps(href: str):
    if not href:
        return None, None
    href = unquote(href)
    # patterns: .../@59.12345,24.54321..., or ...?q=59.12345,24.54321
    m = re.search(r"/@(-?\d+\.\d+),\s*(-?\d+\.\d+)", href)
    if m:
        return float(m.group(1)), float(m.group(2))
    q = parse_qs(urlparse(href).query)
    if "q" in q:
        parts = unquote(q["q"][0]).split(",")
        if len(parts) >= 2:
            try:
                return float(parts[0]), float(parts[1])
            except Exception:
                pass
    return None, None

def collect_all_cards(page):
    # Wait for the list container that holds the store cards
    list_sel_variants = [
        'section[data-testid="store-list"]',
        'section[aria-label="store-list"]',
        'section:has(article)',
        'main section:has(article)'
    ]
    container = None
    for sel in list_sel_variants:
        try:
            container = page.wait_for_selector(sel, timeout=8000)
            if container:
                break
        except Exception:
            continue
    if not container:
        print("[coop-stores] ERROR: store list container not found", file=sys.stderr)
        return []

    # Keep scrolling the **container** (the right-hand list), not the window
    last_count = -1
    stable_rounds = 0
    for _ in range(60):
        count = page.locator(f"{sel} article").count()
        # Scroll container down a bit each loop
        page.evaluate(
            """(el) => { el.scrollBy(0, el.clientHeight * 0.9); }""",
            container
        )
        time.sleep(0.25)
        if count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = count
        if stable_rounds >= 6:
            break

    # Collect candidate cards with multiple fallbacks
    candidates = []
    for cards_sel in [
        f"{sel} article",
        "article:has(a:has-text('Google Maps'))",
        "article"
    ]:
        loc = page.locator(cards_sel)
        n = loc.count()
        if n > 0:
            for i in range(n):
                candidates.append(loc.nth(i))
            break

    print(f"[coop-stores] found {len(candidates)} cards")
    return candidates

def extract_card(card):
    # name
    name = None
    for name_sel in ["h3", "header h3", "h2", "[data-testid='store-name']"]:
        try:
            text = card.locator(name_sel).first.inner_text(timeout=500)
            if text and text.strip():
                name = text.strip()
                break
        except Exception:
            pass
    if not name:
        return None

    # address (first paragraph-ish text block under the title)
    address = None
    for addr_sel in [
        "a:has-text('Google Maps') >> xpath=preceding::p[1]",
        "p",
        "div:has-text(', Eesti')",
    ]:
        try:
            txt = card.locator(addr_sel).first.inner_text(timeout=400)
            if txt and len(txt.strip()) > 5:
                address = " ".join(txt.split())
                break
        except Exception:
            pass

    # Google Maps link for lat/lon
    gmaps_href = None
    try:
        gmaps_href = card.locator("a:has-text('Google Maps')").first.get_attribute("href", timeout=400)
    except Exception:
        pass
    lat, lon = parse_latlon_from_gmaps(gmaps_href)

    return {
        "name": name,
        "address": address,
        "lat": lat,
        "lon": lon,
        "gmaps": gmaps_href,
    }

def write_csv(rows):
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["chain", "name", "address", "lat", "lon", "external_key"])
        for r in rows:
            w.writerow(["Coop", r["name"], r["address"], r["lat"], r["lon"], r["external_key"]])
    print(f"[coop-stores] wrote {len(rows)} rows -> {OUT_CSV} (with_coords={sum(1 for r in rows if r['lat'] and r['lon'])})")

def upsert_to_db(rows):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[coop-stores] DATABASE_URL not set; skipping DB upsert", file=sys.stderr)
        return

    payload = []
    for r in rows:
        payload.append((
            r["external_key"],            # 1
            "Coop",                       # 2 chain
            r["name"],                    # 3 name
            r["address"],                 # 4 address
            r["lat"],                     # 5 lat
            r["lon"],                     # 6 lon
            False                         # 7 is_online
        ))

    sql = """
    INSERT INTO stores (external_key, chain, name, address, lat, lon, is_online)
    VALUES %s
    ON CONFLICT (external_key) DO UPDATE
       SET name = EXCLUDED.name,
           address = EXCLUDED.address,
           lat = EXCLUDED.lat,
           lon = EXCLUDED.lon,
           chain = EXCLUDED.chain,
           is_online = EXCLUDED.is_online
    ;
    """
    with closing(psycopg2.connect(dsn)) as conn, conn, conn.cursor() as cur:
        execute_values(cur, sql, payload)
    print(f"[coop-stores] upserted {len(rows)} rows into stores")

def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(COOP_URL, wait_until="networkidle", timeout=60000)

        cards = collect_all_cards(page)
        parsed = []
        for card in cards:
            data = extract_card(card)
            if not data:
                continue
            data["external_key"] = f"coop:physical:{slug(data['name'])}"
            parsed.append(data)

        browser.close()

    # de-dup by name
    by_name = {}
    for r in parsed:
        by_name.setdefault(r["name"], r)

    rows = list(by_name.values())
    write_csv(rows)
    upsert_to_db(rows)

if __name__ == "__main__":
    sys.exit(main())

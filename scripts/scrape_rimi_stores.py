#!/usr/bin/env python3
import csv, json, re, time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

import requests
from playwright.sync_api import sync_playwright

OUT = Path("data/rimi_stores.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

def latlon_from_gmaps(href: str):
    if not href:
        return None, None
    try:
        u = urlparse(href)
        q = parse_qs(u.query)
        if "q" in q:
            m = re.search(r"(-?\d+\.\d+),\s*(-?\d+\.\d+)", q["q"][0])
            if m:
                return float(m.group(1)), float(m.group(2))
        m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", href)
        if m:
            return float(m.group(1)), float(m.group(2))
    except Exception:
        pass
    return None, None

def nominatim_geocode(addr: str):
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format":"jsonv2","q":addr,"countrycodes":"ee"},
            headers={"User-Agent":"basket-compare/1.0"},
            timeout=20
        )
        r.raise_for_status()
        j = r.json()
        if j:
            return float(j[0]["lat"]), float(j[0]["lon"])
    except Exception:
        return None, None
    return None, None

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.rimi.ee/kauplused", timeout=90_000)

        # Infinite-scroll until no new height appears
        last_h = 0
        stall = 0
        while stall < 5:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.2)
            h = page.evaluate("document.body.scrollHeight")
            if h <= last_h:
                stall += 1
            else:
                stall = 0
            last_h = h

        # Each card: name, address, Google Maps "Juhised" link
        cards = page.locator("section >> css=div:has(h3), article:has(h3)")
        n = cards.count()
        for i in range(n):
            card = cards.nth(i)
            name = card.locator("h3, h2").first.text_content() or ""
            name = name.strip()

            # address line is typically the first small text block
            address = ""
            for sel in ["p", "div", "span"]:
                t = card.locator(sel).first.text_content()
                if t:
                    t = t.strip()
                    if "," in t or re.search(r"\d", t):
                        address = t
                        break

            href = ""
            # try explicit “Juhised” (Directions) button
            for s in ['a:has-text("Juhised")', 'a:has-text("Directions")', 'a[href*="google.com/maps"]']:
                if card.locator(s).count():
                    href = card.locator(s).first.get_attribute("href") or ""
                    break

            lat, lon = latlon_from_gmaps(href)
            # fallback geocode if needed
            if (lat is None or lon is None) and address:
                lat, lon = nominatim_geocode(address)
                time.sleep(1.1)  # be polite to Nominatim

            if not name:
                continue

            rows.append({
                "name": name,
                "address": address,
                "lat": f"{lat:.6f}" if lat is not None else "",
                "lon": f"{lon:.6f}" if lon is not None else "",
                "external_key": re.sub(r"[^a-z0-9\-]+", "-", name.lower()).strip("-")
            })

        browser.close()

    # de-dup by name (keep first)
    seen = set()
    dedup = []
    for r in rows:
        k = (r["name"], r["address"])
        if k in seen: 
            continue
        seen.add(k)
        dedup.append(r)

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name","address","lat","lon","external_key"])
        w.writeheader()
        for r in dedup:
            w.writerow(r)

    print(f"wrote {len(dedup)} rows -> {OUT}")

if __name__ == "__main__":
    scrape()

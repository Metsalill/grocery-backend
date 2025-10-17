#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Coop Estonia physical stores from https://www.coop.ee/kauplused → CSV.

Output CSV columns:
  name,address,lat,lon,external_key

Notes
- Treats EVERY result on the page as a *physical* store (is_online = FALSE in SQL step).
- Robust coordinate gathering: JSON/XHR sniff → DOM data-attrs → Google Maps link → (optional) OSM.
"""

from __future__ import annotations
import argparse, csv, re, sys, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

URL = "https://www.coop.ee/kauplused"
DEFAULT_OUT = Path("data/coop_stores.csv")

# ------------------------------ Model ------------------------------ #
@dataclass(frozen=True)
class StoreRow:
    name: str
    address: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    external_key: Optional[str] = None

    def as_csv_row(self) -> List[str]:
        return [
            self.name,
            self.address,
            "" if self.lat is None else f"{self.lat:.8f}",
            "" if self.lon is None else f"{self.lon:.8f}",
            self.external_key or "",
        ]

def n(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s)).strip() if s is not None else ""

def log(msg: str) -> None:
    print(f"[coop-stores] {msg}", file=sys.stderr)

# --------------------------- Playwright ---------------------------- #
def _dismiss_cookies(page: Page) -> None:
    for sel in ["button:has-text('Nõustu')", "button:has-text('Nõustun')", "button:has-text('Accept')"]:
        try:
            btn = page.locator(sel)
            if btn.count() and btn.first.is_visible():
                btn.first.click(timeout=1500)
                time.sleep(0.2)
                break
        except Exception:
            pass

def collect_html(timeout_ms: int = 45000, max_scrolls: int = 30) -> Tuple[str, Page]:
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(locale="et-EE")
    page = ctx.new_page()
    log("navigate…")
    page.goto(URL, wait_until="networkidle", timeout=timeout_ms)
    _dismiss_cookies(page)

    # The list is lazy-loaded; scroll until stable
    last_h = 0
    for _ in range(max_scrolls):
        page.mouse.wheel(0, 2600)
        page.wait_for_timeout(250)
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

    html = page.content()
    return html, page  # caller closes browser

# ---------------------------- Helpers ------------------------------ #
def extract_latlon_from_href(href: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not href:
        return None, None
    if not ("google.com/maps" in href or "goo.gl/maps" in href or "maps.app.goo.gl" in href):
        return None, None
    m = re.search(r"@(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)(?:[,/]|$)", href)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            return None, None
    m = re.search(r"[?&]query=(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)", href)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    return None, None

def _flatten(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _flatten(v)
    elif isinstance(obj, list):
        for i in obj:
            yield from _flatten(i)

def sniff_coords_from_json(obj: Any, store_keys: set[str]) -> Dict[str, Tuple[float, float]]:
    out: Dict[str, Tuple[float, float]] = {}
    for node in _flatten(obj):
        if not isinstance(node, dict):
            continue
        # Try common id/slug keys we’ve seen on Coop
        for k in ("id", "storeId", "shopId", "slug", "uuid"):
            if k in node:
                sid = str(node[k])
                if store_keys and sid not in store_keys:
                    break
                lat = node.get("lat") or node.get("latitude")
                lon = node.get("lon") or node.get("lng") or node.get("longitude")
                try:
                    if lat is not None and lon is not None:
                        out[sid] = (float(str(lat).replace(",", ".")), float(str(lon).replace(",", ".")))
                except Exception:
                    pass
                break
    return out

def geocode(address: str, email: str = "", cc: str = "ee", timeout: float = 10.0) -> Tuple[Optional[float], Optional[float]]:
    try:
        params = {"format": "jsonv2", "q": address, "limit": 1, "countrycodes": cc}
        if email:
            params["email"] = email
        headers = {"User-Agent": f"coop-stores-scraper/1.0 (+{email or 'no-email'})"}
        r = requests.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None, None
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None, None

# ----------------------------- Parser -------------------------------- #
def parse_cards(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")

    # Cards list (grid on the right of the map)
    cards = soup.select("div[class*='list'] article, li article, .store-card, .MuiCard-root")
    if not cards:
        # fallback: generic card-looking blocks
        cards = soup.select("article, li")

    out: List[Dict[str, Any]] = []
    for c in cards:
        # Name
        name_el = c.select_one("a[href*='/kauplused/'], h3, .store-card__title, .MuiCardHeader-title")
        name = n(name_el.get_text(" ", strip=True)) if name_el else ""

        # Address
        addr_el = (
            c.select_one("a[href^='https://maps.google.']") or
            c.select_one(".store-card__address, .MuiCardContent-root p, address") or
            c.find(string=re.compile(r"\d{2} ?\d{3}"))  # zip-ish
        )
        address = n(addr_el.get_text(" ", strip=True) if hasattr(addr_el, "get_text") else addr_el)

        if not name or not address or "Coop" not in name:
            continue

        # External key (slug/id) if present in the link
        link = c.select_one("a[href*='/kauplused/']")
        external_key = None
        if link and link.has_attr("href"):
            m = re.search(r"/kauplused/([^/?#]+)/?", link["href"])
            if m:
                external_key = m.group(1)

        # “Directions”/map link for quick lat/lon parse
        gmaps = c.select_one("a[href*='google.com/maps'], a[href*='maps.app.goo.gl'], a[href*='goo.gl/maps']")
        map_href = gmaps["href"] if (gmaps and gmaps.has_attr("href")) else None

        out.append({"name": name, "address": address, "external_key": external_key, "map_href": map_href})

    log(f"parsed {len(out)} candidate cards")
    return out

def enrich_coords(page: Page, rows: List[Dict[str, Any]], wait_ms: int = 800) -> Dict[str, Tuple[float, float]]:
    coords: Dict[str, Tuple[float, float]] = {}
    keys = {r["external_key"] for r in rows if r.get("external_key")}
    def on_response(resp):
        try:
            if "json" not in (resp.headers.get("content-type","")).lower():
                return
            data = resp.json()
            found = sniff_coords_from_json(data, keys)
            for k, pair in found.items():
                if k not in coords:
                    coords[k] = pair
        except Exception:
            pass
    page.on("response", on_response)

    # Click “Vaata kaardilt / Kaart” link per card to trigger any map XHR
    for r in rows:
        ek = r.get("external_key")
        if not ek or ek in coords:
            continue
        try:
            # try a few likely selectors
            for sel in [
                f"a[href*='{ek}']",
                f"a[href*='/kauplused/{ek}']",
            ]:
                loc = page.locator(sel)
                if loc.count():
                    loc.first.scroll_into_view_if_needed()
                    loc.first.click()
                    page.wait_for_timeout(wait_ms)
                    break
        except Exception:
            pass

    return coords

# ------------------------------ Main -------------------------------- #
def write_csv(path: Path, rows: List[StoreRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "address", "lat", "lon", "external_key"])
        for r in rows:
            w.writerow(r.as_csv_row())

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Scrape Coop physical stores → CSV")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--timeout-ms", type=int, default=50000)
    ap.add_argument("--max-scrolls", type=int, default=30)
    ap.add_argument("--click-wait-ms", type=int, default=800)
    ap.add_argument("--geocode-missing", action="store_true")
    ap.add_argument("--nominatim-email", default="")
    ap.add_argument("--geocode-sleep-ms", type=int, default=1100)
    args = ap.parse_args(argv)

    html, page = collect_html(timeout_ms=args.timeout_ms, max_scrolls=args.max_scrolls)
    raw = parse_cards(html)

    coords_by_key: Dict[str, Tuple[float, float]] = {}
    try:
        coords_by_key = enrich_coords(page, raw, wait_ms=args.click_wait_ms)
    finally:
        try: page.context.browser.close()
        except Exception: pass

    rows: List[StoreRow] = []
    for r in raw:
        lat = lon = None
        if r.get("external_key") and r["external_key"] in coords_by_key:
            lat, lon = coords_by_key[r["external_key"]]
        if (lat is None or lon is None) and r.get("map_href"):
            _lat, _lon = extract_latlon_from_href(r["map_href"])
            if _lat is not None and _lon is not None:
                lat, lon = _lat, _lon
        rows.append(StoreRow(r["name"], r["address"], lat, lon, r.get("external_key")))

    if args.geocode_missing:
        sleep_s = max(0.0, args.geocode_sleep_ms / 1000.0)
        missing = [i for i, x in enumerate(rows) if x.lat is None or x.lon is None]
        log(f"Nominatim fallback for {len(missing)} rows…")
        for i in missing:
            lat, lon = geocode(rows[i].address, email=args.nominatim_email or "")
            if lat is not None and lon is not None:
                rows[i] = StoreRow(rows[i].name, rows[i].address, lat, lon, rows[i].external_key)
            time.sleep(sleep_s)

    out = Path(args.out)
    write_csv(out, rows)
    with_coords = sum(1 for r in rows if r.lat is not None and r.lon is not None)
    print(f"Wrote {len(rows)} rows → {out} (with_coords={with_coords})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Rimi Estonia physical stores from https://www.rimi.ee/kauplused → CSV.

Output CSV columns:
  name,address,lat,lon,external_key

How it gets coordinates (in order):
1) Clicks each store's "Vaata kaardilt" link to trigger the page's map/XHR and
   sniffs JSON for latitude/longitude.
2) Tries to parse lat/lon from any Google Maps "Juhised" URL on the card.
3) (Optional) Reverse geocodes any remaining missing coordinates via
   OpenStreetMap Nominatim using the store address (rate-limited).
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict, Any

import requests
from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

URL = "https://www.rimi.ee/kauplused"
DEFAULT_OUT = Path("data/rimi_stores.csv")


# ------------------------------ Data model -------------------------------- #

@dataclass(frozen=True)
class StoreRow:
    name: str
    address: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    external_key: Optional[str] = None  # site "shop id" when available

    def as_csv_row(self) -> List[str]:
        return [
            self.name,
            self.address,
            "" if self.lat is None else f"{self.lat:.8f}",
            "" if self.lon is None else f"{self.lon:.8f}",
            self.external_key or "",
        ]


# ------------------------------ Utils ------------------------------------- #

def log(msg: str) -> None:
    print(f"[rimi-stores] {msg}", file=sys.stderr)


def normalize_ws(s: Optional[Any]) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


ASSET_LIKE = re.compile(r"\.(gif|png|jpg|jpeg|svg|webp|js|css)$", re.I)


def looks_like_store_name(s: str) -> bool:
    s = normalize_ws(s)
    if not s or ASSET_LIKE.search(s):
        return False
    return "rimi" in s.lower()


def looks_like_address(s: str) -> bool:
    s = normalize_ws(s)
    if not s or "@" in s:
        return False
    if not any(ch.isdigit() for ch in s):
        return False
    if s.lower().startswith(("tel", "telefon", "e-post", "email", "ava", "avatud")):
        return False
    return True


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
            pass
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


# ------------------------------ Playwright -------------------------------- #

def _dismiss_cookies(page: Page) -> None:
    try:
        for sel in [
            "button:has-text('Nõustu')",
            "button:has-text('Nõustun')",
            "button:has-text('Accept')",
            "[data-testid='cookie-accept']",
        ]:
            btn = page.locator(sel)
            if btn.count() and btn.first.is_visible():
                btn.first.click(timeout=1500)
                time.sleep(0.25)
                break
    except Exception:
        pass


def collect_html(timeout_ms: int = 45000, max_scrolls: int = 24) -> Tuple[str, Page]:
    """Navigate and return full HTML + a live page handle (caller must close context/browser)."""
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        locale="et-EE",
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    )
    page = ctx.new_page()

    log("navigate to kauplused…")
    page.goto(URL, wait_until="networkidle", timeout=timeout_ms)
    _dismiss_cookies(page)

    try:
        page.wait_for_selector("li.shop.js-shop-item", timeout=timeout_ms)
    except Exception:
        pass

    last_h = 0
    for _ in range(max_scrolls):
        page.mouse.wheel(0, 2600)
        page.wait_for_timeout(250)
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

    html = page.content()
    return html, page  # caller must close page.context.browser


# ------------------------------ Parse store cards ------------------------- #

def parse_cards(html: str) -> List[Dict[str, Any]]:
    """Return raw card info: dicts with id, name, address, directions href."""
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("li.shop.js-shop-item")
    log(f"found {len(cards)} store cards")

    raw: List[Dict[str, Any]] = []
    for card in cards:
        name_el = card.select_one("a.shop__name, .shop__top .shop__name")
        name = normalize_ws(name_el.get_text(" ", strip=True)) if name_el else ""

        addr_el = (
            card.select_one(".shop__address.shop__address--desktop")
            or card.select_one(".shop__address.shop__address--mobile")
            or card.select_one(".shop__address")
        )
        address = normalize_ws(addr_el.get_text(" ", strip=True)) if addr_el else ""

        map_link = card.select_one("a.js-shop-map-link") or card.select_one("a.shop__view.js-shop-map-link")
        store_id = normalize_ws(map_link["data-shop-id"]) if (map_link and map_link.has_attr("data-shop-id")) else None

        directions = card.select_one(".shop__info--directions a[href]")
        map_href = directions["href"] if (directions and directions.has_attr("href")) else None

        if not looks_like_store_name(name) or not looks_like_address(address):
            continue

        raw.append({"id": store_id, "name": name, "address": address, "map_href": map_href})
    return raw


# ------------------------------ Enrich with coords ------------------------ #

def _match_and_extract_coords(obj: Any, target_ids: set[str]) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    out: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    for node in _flatten(obj):
        if not isinstance(node, dict):
            continue
        node_id = None
        for key in ("id", "shopId", "storeId", "store_id"):
            if key in node:
                node_id = str(node[key])
                break
        if node_id is None or (target_ids and node_id not in target_ids):
            continue

        lat = node.get("lat") or node.get("latitude")
        lon = node.get("lon") or node.get("lng") or node.get("longitude")
        try:
            latf = float(str(lat).replace(",", ".")) if lat is not None else None
            lonf = float(str(lon).replace(",", ".")) if lon is not None else None
        except ValueError:
            latf = lonf = None

        if latf is not None and lonf is not None:
            out[str(node_id)] = (latf, lonf)
    return out


def enrich_with_coords(page: Page, rows: List[Dict[str, Any]], per_click_wait_ms: int = 900) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    """
    Click each store's map link to trigger any JSON/XHR containing coordinates.
    Returns mapping by store id (string). Tries DOM data-lat / data-lng as fallback.
    """
    target_ids = {r["id"] for r in rows if r.get("id")}
    coords: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    def on_response(resp):
        try:
            ctype = resp.headers.get("content-type", "")
            if "json" not in ctype.lower():
                return
            data = resp.json()
            found = _match_and_extract_coords(data, target_ids)
            for sid, pair in found.items():
                if sid not in coords:
                    coords[sid] = pair
        except Exception:
            pass

    page.on("response", on_response)

    for r in rows:
        sid = r.get("id")
        if not sid or sid in coords:
            continue
        try:
            selector = f"a.js-shop-map-link[data-shop-id='{sid}']"
            link = page.locator(selector)
            if link.count() == 0:
                continue
            link.first.scroll_into_view_if_needed()
            link.first.click()
            page.wait_for_timeout(per_click_wait_ms)

            # DOM fallback after clicking
            dom_lat = page.evaluate("""() => {
              const el = document.querySelector('[data-lat][data-lng], [data-latitude][data-longitude]');
              if (!el) return null;
              const lat = el.getAttribute('data-lat') || el.getAttribute('data-latitude');
              const lng = el.getAttribute('data-lng') || el.getAttribute('data-longitude');
              return [lat, lng];
            }""")
            if dom_lat and sid not in coords:
                try:
                    latf = float(str(dom_lat[0]).replace(",", "."))
                    lonf = float(str(dom_lat[1]).replace(",", "."))
                    coords[sid] = (latf, lonf)
                except Exception:
                    pass
        except Exception:
            continue

    return coords


# ------------------------------ Nominatim fallback ------------------------ #

def geocode_nominatim(address: str, email: Optional[str], countrycodes: str = "ee", timeout: float = 10.0) -> Tuple[Optional[float], Optional[float]]:
    """
    Geocode an address to (lat, lon) using OpenStreetMap Nominatim.
    Returns (None, None) on failure.
    """
    try:
        params = {
            "format": "jsonv2",
            "q": address,
            "limit": 1,
            "countrycodes": countrycodes,
            "addressdetails": 0,
        }
        if email:
            params["email"] = email  # per Nominatim usage policy
        headers = {"User-Agent": f"rimi-stores-scraper/1.0 (+{email or 'no-email'})"}
        resp = requests.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None, None
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        return lat, lon
    except Exception:
        return None, None


# ------------------------------ Write & main ------------------------------ #

def dedup_rows(rows: List[StoreRow]) -> List[StoreRow]:
    by_key: Dict[Tuple[str, str], StoreRow] = {}
    for r in rows:
        key = (normalize_ws(r.name), normalize_ws(r.address))
        if key not in by_key:
            by_key[key] = r
        else:
            ex = by_key[key]
            if (ex.lat is None or ex.lon is None) and (r.lat is not None and r.lon is not None):
                by_key[key] = StoreRow(r.name, r.address, r.lat, r.lon, ex.external_key or r.external_key)
    return sorted(by_key.values(), key=lambda x: (x.name.lower(), x.address.lower()))


def write_csv(path: Path, rows: List[StoreRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "address", "lat", "lon", "external_key"])
        for r in rows:
            w.writerow(r.as_csv_row())


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Scrape Rimi Estonia stores → CSV")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="Output CSV path (default: data/rimi_stores.csv)")
    ap.add_argument("--timeout-ms", type=int, default=45000, help="Playwright navigation timeout (ms)")
    ap.add_argument("--max-scrolls", type=int, default=24, help="How many scroll steps to trigger lazy-load")
    ap.add_argument("--per-click-wait-ms", type=int, default=900, help="Wait after clicking each map link (ms)")
    ap.add_argument("--geocode-missing", action="store_true", help="Use OSM Nominatim to fill missing coords")
    ap.add_argument("--nominatim-email", default="", help="Contact email for Nominatim (recommended)")
    ap.add_argument("--geocode-sleep-ms", type=int, default=1100, help="Sleep between geocode requests (ms)")
    ap.add_argument("--country-codes", default="ee", help="Nominatim countrycodes filter (default: ee)")
    args = ap.parse_args(argv)

    # 1) Load page and parse cards
    try:
        html, page = collect_html(timeout_ms=args.timeout_ms, max_scrolls=args.max_scrolls)
    except PWTimeout:
        log("navigation timed out; retry once with longer timeout…")
        html, page = collect_html(timeout_ms=max(args.timeout_ms, 70000), max_scrolls=args.max_scrolls)

    raw = parse_cards(html)
    log(f"parsed cards: {len(raw)}")

    # 2) Enrich with lat/lon via page XHR/DOM
    coords_by_id: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    try:
        coords_by_id = enrich_with_coords(page, raw, per_click_wait_ms=args.per_click_wait_ms)
    finally:
        # Close Playwright resources
        try:
            page.context.browser.close()
        except Exception:
            pass

    # 3) Build rows; attempt Google URL parse if page coords missing
    rows: List[StoreRow] = []
    for r in raw:
        lat = lon = None
        sid = r.get("id")
        if sid and sid in coords_by_id:
            lat, lon = coords_by_id[sid]
        if (lat is None or lon is None) and r.get("map_href"):
            _lat, _lon = extract_latlon_from_href(r["map_href"])
            if _lat is not None and _lon is not None:
                lat, lon = _lat, _lon

        rows.append(StoreRow(
            name=r["name"],
            address=r["address"],
            lat=lat,
            lon=lon,
            external_key=sid or None
        ))

    # 4) Optional: Nominatim for remaining missing coordinates
    if args.geocode_missing:
        missing = [i for i, row in enumerate(rows) if row.lat is None or row.lon is None]
        log(f"geocoding fallback for {len(missing)} rows via Nominatim…")
        sleep_sec = max(0.0, args.geocode_sleep_ms / 1000.0)
        for idx in missing:
            addr = rows[idx].address
            q = addr
            lat, lon = geocode_nominatim(q, email=(args.nominatim_email or None), countrycodes=args.country_codes)
            if lat is not None and lon is not None:
                rows[idx] = StoreRow(
                    name=rows[idx].name,
                    address=rows[idx].address,
                    lat=lat,
                    lon=lon,
                    external_key=rows[idx].external_key,
                )
            time.sleep(sleep_sec)

    # 5) Write CSV
    rows = dedup_rows(rows)
    out_path = Path(args.out)
    write_csv(out_path, rows)

    with_coords = sum(1 for r in rows if r.lat is not None and r.lon is not None)
    print(f"Wrote {len(rows)} rows → {out_path} (with_coords={with_coords})")
    try:
        preview = out_path.read_text(encoding="utf-8").splitlines()[:10]
        for ln in preview:
            print(ln)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

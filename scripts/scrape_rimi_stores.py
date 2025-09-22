#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Rimi Estonia physical stores from https://www.rimi.ee/kauplused → CSV.

Output CSV columns:
  name,address,lat,lon,external_key

Notes
- Attempts to capture any JSON/XHR payloads with store data first.
- Falls back to parsing visible DOM cards.
- Extracts lat/lon from Google Maps href if present.
- Deduplicates by (name,address).
- Safe to run in CI (no repo changes; writes under ./data).

Requires:
  pip install playwright bs4 lxml
  python -m playwright install --with-deps chromium
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

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

URL = "https://www.rimi.ee/kauplused"
DEFAULT_OUT = Path("data/rimi_stores.csv")


# ------------------------------ Helpers ---------------------------------- #

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


def log(msg: str) -> None:
    print(f"[rimi-stores] {msg}", file=sys.stderr)


def normalize_ws(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def extract_latlon_from_href(href: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not href:
        return None, None
    # @59.437123,24.740987 (optionally followed by ,15z etc)
    m = re.search(r"@(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)(?:[,/]|$)", href)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    # query=59.437123,24.740987
    m = re.search(r"[?&]query=(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)", href)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    return None, None


def flatten(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from flatten(v)
    elif isinstance(obj, list):
        for i in obj:
            yield from flatten(i)


# ------------------------------ Scraping --------------------------------- #

def _dismiss_cookies(page: Page) -> None:
    """Best-effort cookie popup dismissal (selectors may change; safe no-op if absent)."""
    try:
        candidates = [
            "button:has-text('Nõustu')",
            "button:has-text('Nõustun')",
            "button:has-text('Accept')",
            "[data-testid='cookie-accept']",
        ]
        for sel in candidates:
            btn = page.locator(sel)
            if btn.count() and btn.first.is_visible():
                btn.first.click(timeout=1500)
                time.sleep(0.25)
                break
    except Exception:
        pass


def collect_with_playwright(timeout_ms: int = 30000, max_scrolls: int = 18) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Returns (page_html, captured_json_payloads)
    """
    json_payloads: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="et-EE",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
        )

        # Lightweight request blocking for speed
        ctx.route(
            "**/*",
            lambda route: route.abort()
            if any(host in route.request.url for host in
                   ["googletagmanager.com", "google-analytics.com", "facebook.net", "hotjar.com"])
            else route.continue_(),
        )

        page = ctx.new_page()

        def on_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                if "json" in ctype.lower():
                    u = resp.url.lower()
                    if any(k in u for k in ("kaupl", "store", "shop", "location", "map")):
                        data = resp.json()
                        json_payloads.append(data)
            except Exception:
                pass

        page.on("response", on_response)

        log("navigate to kauplused…")
        page.goto(URL, wait_until="networkidle", timeout=timeout_ms)
        _dismiss_cookies(page)

        # Lazy-load scrolling; stop when height stops changing
        last_h = 0
        for _ in range(max_scrolls):
            page.mouse.wheel(0, 2600)
            time.sleep(0.25)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        html = page.content()
        browser.close()
        return html, json_payloads


def parse_from_json_payloads(payloads: List[Dict[str, Any]]) -> List[StoreRow]:
    rows: List[StoreRow] = []
    for payload in payloads:
        for node in flatten(payload):
            if not isinstance(node, dict):
                continue
            name = node.get("name") or node.get("title") or node.get("label")
            address = node.get("address") or node.get("location") or node.get("addressLine")
            lat = node.get("lat") or node.get("latitude")
            lon = node.get("lon") or node.get("lng") or node.get("longitude")
            ext = node.get("id") or node.get("slug") or node.get("key")
            name = normalize_ws(name)
            address = normalize_ws(address)
            if name and address:
                latf = None
                lonf = None
                try:
                    if lat is not None:
                        latf = float(str(lat).replace(",", "."))
                    if lon is not None:
                        lonf = float(str(lon).replace(",", "."))
                except ValueError:
                    latf = lonf = None
                rows.append(StoreRow(name=name, address=address, lat=latf, lon=lonf, external_key=normalize_ws(ext)))
    return rows


def parse_from_dom(html: str) -> List[StoreRow]:
    soup = BeautifulSoup(html, "lxml")

    candidates = soup.select("article, li, div, section, a")
    rows: List[StoreRow] = []
    seen: set[Tuple[str, str]] = set()

    for el in candidates:
        # 1) Store name
        name = None
        for tag in el.select("strong, h1, h2, h3, .title, .store__title, .shop__title"):
            t = normalize_ws(tag.get_text(" ", strip=True))
            if t and "rimi" in t.lower():
                name = t
                break
        if not name:
            continue

        # 2) Address line
        addr = None
        text = el.get_text("\n", strip=True)
        for line in (normalize_ws(x) for x in text.splitlines()):
            if any(ch.isdigit() for ch in line) and 3 <= len(line) <= 120 and "@" not in line:
                addr = line
                break
        if not addr:
            continue

        # 3) Optional map link → lat/lon
        href = None
        a = el.select_one("a[href*='google.com/maps'], a[href*='goo.gl/maps'], a[href*='maps.app.goo.gl']")
        if a and a.has_attr("href"):
            href = a["href"]
        lat, lon = extract_latlon_from_href(href)

        key = (name, addr)
        if key in seen:
            continue
        seen.add(key)
        rows.append(StoreRow(name=name, address=addr, lat=lat, lon=lon, external_key=None))
    return rows


def dedup_rows(rows: List[StoreRow]) -> List[StoreRow]:
    by_key: Dict[Tuple[str, str], StoreRow] = {}
    for r in rows:
        key = (normalize_ws(r.name), normalize_ws(r.address))
        if key not in by_key:
            by_key[key] = StoreRow(
                name=key[0],
                address=key[1],
                lat=r.lat,
                lon=r.lon,
                external_key=r.external_key,
            )
        else:
            existing = by_key[key]
            if (existing.lat is None or existing.lon is None) and (r.lat is not None and r.lon is not None):
                by_key[key] = StoreRow(
                    name=key[0],
                    address=key[1],
                    lat=r.lat,
                    lon=r.lon,
                    external_key=existing.external_key or r.external_key,
                )
    return sorted(by_key.values(), key=lambda x: (x.name.lower(), x.address.lower()))


def write_csv(path: Path, rows: List[StoreRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "address", "lat", "lon", "external_key"])
        for r in rows:
            w.writerow(r.as_csv_row())


# ------------------------------ CLI / Main -------------------------------- #

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Scrape Rimi Estonia stores → CSV")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="Output CSV path (default: data/rimi_stores.csv)")
    ap.add_argument("--timeout-ms", type=int, default=30000, help="Playwright navigation timeout (ms)")
    ap.add_argument("--max-scrolls", type=int, default=18, help="How many scroll steps to trigger lazy-load")
    args = ap.parse_args(argv)

    try:
        html, payloads = collect_with_playwright(timeout_ms=args.timeout_ms, max_scrolls=args.max_scrolls)
    except PWTimeout:
        log("navigation timed out; retry once with longer timeout…")
        html, payloads = collect_with_playwright(timeout_ms=max(args.timeout_ms, 60000), max_scrolls=args.max_scrolls)

    rows_json = parse_from_json_payloads(payloads) if payloads else []
    if rows_json:
        log(f"json payload rows: {len(rows_json)}")
    rows_dom = parse_from_dom(html)
    log(f"dom rows: {len(rows_dom)}")

    rows_all = rows_json + rows_dom if rows_json else rows_dom
    rows = dedup_rows(rows_all)
    out_path = Path(args.out)
    write_csv(out_path, rows)

    print(f"Wrote {len(rows)} rows → {out_path}")
    try:
        preview = out_path.read_text(encoding="utf-8").splitlines()[:10]
        for ln in preview:
            print(ln)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

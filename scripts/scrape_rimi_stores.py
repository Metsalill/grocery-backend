#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Rimi Estonia physical stores from https://www.rimi.ee/kauplused → CSV.

Output CSV columns:
  name,address,lat,lon,external_key
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

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

URL = "https://www.rimi.ee/kauplused"
DEFAULT_OUT = Path("data/rimi_stores.csv")


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


def normalize_ws(s: Optional[Any]) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


ASSET_LIKE = re.compile(r"\.(gif|png|jpg|jpeg|svg|webp|js|css)$", re.I)


def looks_like_store_name(s: str) -> bool:
    s = normalize_ws(s)
    if not s:
        return False
    if ASSET_LIKE.search(s):
        return False
    return "rimi" in s.lower()  # every card uses "Rimi" in name


def looks_like_address(s: str) -> bool:
    """Require a digit (house no.) and usually a comma (street, city)."""
    s = normalize_ws(s)
    if not s or "@" in s:
        return False
    if not any(ch.isdigit() for ch in s):
        return False
    # many addresses look like "Haabersti 1, Tallinn"
    if "," not in s and len(s) < 8:
        return False
    if s.lower().startswith(("tel", "telefon", "e-post", "email", "ava", "avatud")):
        return False
    return True


def extract_latlon_from_href(href: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not href:
        return None, None
    if not ("google.com/maps" in href or "goo.gl/maps" in href or "maps.app.goo.gl" in href):
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


# ------------------------------ Scraping --------------------------------- #

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


def collect_with_playwright(timeout_ms: int = 45000, max_scrolls: int = 24) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="et-EE",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        )
        page = ctx.new_page()

        log("navigate to kauplused…")
        page.goto(URL, wait_until="networkidle", timeout=timeout_ms)
        _dismiss_cookies(page)

        # Wait until at least one card is present
        try:
            page.wait_for_selector("li.shop.js-shop-item", timeout=timeout_ms)
        except Exception:
            pass

        # Scroll until height stabilizes
        last_h = 0
        for _ in range(max_scrolls):
            page.mouse.wheel(0, 2600)
            page.wait_for_timeout(250)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        html = page.content()
        browser.close()
        return html


# ------------------------------ DOM parsing ------------------------------ #

def parse_cards(html: str) -> List[StoreRow]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("li.shop.js-shop-item")
    log(f"found {len(cards)} store cards")

    rows: List[StoreRow] = []
    for card in cards:
        # name
        name_el = card.select_one("a.shop__name, .shop__top .shop__name")
        name = normalize_ws(name_el.get_text(" ", strip=True)) if name_el else ""

        # address (prefer desktop)
        addr_el = card.select_one(".shop__address.shop__address--desktop") or card.select_one(".shop__address.shop__address--mobile") or card.select_one(".shop__address")
        address = normalize_ws(addr_el.get_text(" ", strip=True)) if addr_el else ""

        # Juhised → Google maps link (may not contain coords)
        maps_a = card.select_one(".shop__info--directions a[href*='google.com/maps'], .shop__info--directions a[href*='goo.gl/maps'], .shop__info--directions a[href*='maps.app.goo.gl']")
        href = maps_a.get("href") if maps_a and maps_a.has_attr("href") else None
        lat, lon = extract_latlon_from_href(href)

        # Validate
        if not looks_like_store_name(name):
            continue
        if not looks_like_address(address):
            continue

        rows.append(StoreRow(name=name, address=address, lat=lat, lon=lon, external_key=None))

    return rows


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


# ------------------------------ CLI / Main -------------------------------- #

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Scrape Rimi Estonia stores → CSV")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="Output CSV path (default: data/rimi_stores.csv)")
    ap.add_argument("--timeout-ms", type=int, default=45000, help="Playwright navigation timeout (ms)")
    ap.add_argument("--max-scrolls", type=int, default=24, help="How many scroll steps to trigger lazy-load")
    args = ap.parse_args(argv)

    try:
        html = collect_with_playwright(timeout_ms=args.timeout_ms, max_scrolls=args.max_scrolls)
    except PWTimeout:
        log("navigation timed out; retry once with longer timeout…")
        html = collect_with_playwright(timeout_ms=max(args.timeout_ms, 70000), max_scrolls=args.max_scrolls)

    rows_dom = parse_cards(html)
    log(f"valid store rows: {len(rows_dom)}")

    rows = dedup_rows(rows_dom)
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

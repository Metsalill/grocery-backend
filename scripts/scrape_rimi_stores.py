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

from bs4 import BeautifulSoup, Tag
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


def normalize_ws(s: Optional[Any]) -> str:
    """Coerce to string and collapse whitespace; None -> ''."""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def extract_latlon_from_href(href: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not href:
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


def collect_with_playwright(timeout_ms: int = 45000, max_scrolls: int = 24) -> Tuple[str, List[Dict[str, Any]]]:
    json_payloads: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="et-EE",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        )

        page = ctx.new_page()

        def on_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                if "json" in ctype.lower():
                    u = resp.url.lower()
                    if any(k in u for k in ("kaupl", "store", "shop", "location", "map")):
                        json_payloads.append(resp.json())
            except Exception:
                pass

        page.on("response", on_response)

        log("navigate to kauplused…")
        page.goto(URL, wait_until="networkidle", timeout=timeout_ms)
        _dismiss_cookies(page)

        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(1000)

        last_h = 0
        for _ in range(max_scrolls):
            page.mouse.wheel(0, 2600)
            page.wait_for_timeout(250)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        # Try any visible "load more" buttons
        for text in ["Näita", "Laadi", "Load", "Show"]:
            try:
                btn = page.locator(f"button:has-text('{text}')")
                if btn.count() and btn.first.is_visible():
                    btn.first.click(timeout=1500)
                    page.wait_for_timeout(800)
            except Exception:
                pass

        html = page.content()
        browser.close()
        return html, json_payloads


# ---------- DOM parsing (improved heuristics around 'Juhised' link) ---------- #

def _nearest_card(node: Tag) -> Tag:
    for parent in node.parents:
        if isinstance(parent, Tag) and parent.name in ("article", "li", "section", "div"):
            if len(parent.find_all(["a", "h1", "h2", "h3", "strong"])) >= 2:
                return parent
    return node


def _extract_name(card: Tag) -> Optional[str]:
    for sel in ["h1", "h2", "h3", "strong", ".title", ".store__title", ".shop__title"]:
        el = card.select_one(sel)
        if el:
            t = normalize_ws(el.get_text(" ", strip=True))
            if t:
                return t
    text = card.get_text("\n", strip=True)
    for line in text.splitlines():
        if "@" in line.lower():
            continue
        l = normalize_ws(line)
        if l and "rimi" in l.lower():
            return l
    return None


def _extract_address(card: Tag) -> Optional[str]:
    text = card.get_text("\n", strip=True)
    for line in text.splitlines():
        l = normalize_ws(line)
        if not l:
            continue
        if "@" in l or l.lower().startswith(("tel", "telefon", "e-post", "email", "ava", "avatud")):
            continue
        if any(ch.isdigit() for ch in l) and 3 <= len(l) <= 120:
            return l
    return None


def parse_from_dom(html: str) -> List[StoreRow]:
    soup = BeautifulSoup(html, "lxml")

    hint_nodes = soup.find_all(string=re.compile(r"^\s*Juhised\s*$", re.I))
    log(f"found {len(hint_nodes)} 'Juhised' hints in DOM")

    rows: List[StoreRow] = []
    seen: set[Tuple[str, str]] = set()

    def add_from_card(card: Tag):
        href = None
        a = card.select_one("a[href*='google.com/maps'], a[href*='goo.gl/maps'], a[href*='maps.app.goo.gl'], a:contains('Juhised')")
        if a and a.has_attr("href"):
            href = a["href"]
        name = _extract_name(card)
        addr = _extract_address(card)
        if not name or not addr:
            return
        name, addr = normalize_ws(name), normalize_ws(addr)
        lat, lon = extract_latlon_from_href(href)
        key = (name, addr)
        if key in seen:
            return
        seen.add(key)
        rows.append(StoreRow(name=name, address=addr, lat=lat, lon=lon, external_key=None))

    for n in hint_nodes:
        card = _nearest_card(n.parent if isinstance(n, Tag) else n)
        add_from_card(card)

    if not rows:
        for a in soup.select("a[href*='google.com/maps'], a[href*='goo.gl/maps'], a[href*='maps.app.goo.gl']"):
            card = _nearest_card(a)
            add_from_card(card)

    if not rows:
        for card in soup.select("article, li, div, section"):
            add_from_card(card)

    return rows


# ------------------------------ JSON parsing ------------------------------ #

def parse_from_json_payloads(payloads: List[Dict[str, Any]]) -> List[StoreRow]:
    rows: List[StoreRow] = []
    for payload in payloads:
        for node in flatten(payload):
            if not isinstance(node, dict):
                continue
            name = normalize_ws(node.get("name") or node.get("title") or node.get("label"))
            address = normalize_ws(node.get("address") or node.get("location") or node.get("addressLine"))
            lat = node.get("lat") or node.get("latitude")
            lon = node.get("lon") or node.get("lng") or node.get("longitude")
            ext = normalize_ws(node.get("id") or node.get("slug") or node.get("key"))
            if name and address:
                try:
                    latf = float(str(lat).replace(",", ".")) if lat is not None else None
                    lonf = float(str(lon).replace(",", ".")) if lon is not None else None
                except ValueError:
                    latf = lonf = None
                rows.append(StoreRow(name=name, address=address, lat=latf, lon=lonf, external_key=ext or None))
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
        html, payloads = collect_with_playwright(timeout_ms=args.timeout_ms, max_scrolls=args.max_scrolls)
    except PWTimeout:
        log("navigation timed out; retry once with longer timeout…")
        html, payloads = collect_with_playwright(timeout_ms=max(args.timeout_ms, 70000), max_scrolls=args.max_scrolls)

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

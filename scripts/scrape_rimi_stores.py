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
BAD_IN_NAME = re.compile(r"(?:©|token|ga_|__secure|cookie|policy|hotjar|google|analytics)", re.I)
BAD_CONTAINER = re.compile(r"(footer|cookie|consent|policy|gdpr|header|nav)", re.I)


def looks_like_store_name(s: str) -> bool:
    s = normalize_ws(s)
    if not s:
        return False
    if ASSET_LIKE.search(s):
        return False
    if BAD_IN_NAME.search(s):
        return False
    return "rimi" in s.lower()


def looks_like_address(s: str) -> bool:
    """Require a digit (house no.) and a comma (street, city)."""
    s = normalize_ws(s)
    if not s or "@" in s:
        return False
    if not any(ch.isdigit() for ch in s):
        return False
    if "," not in s:
        return False
    # avoid obvious non-addresses
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
        return html, json_payloads


# ------------------------------ DOM parsing ------------------------------ #

def _nearest_card(node: Tag) -> Optional[Tag]:
    for parent in node.parents:
        if not isinstance(parent, Tag):
            continue
        # avoid non-content containers
        if parent.has_attr("class") and BAD_CONTAINER.search(" ".join(parent.get("class", []))):
            return None
        if parent.has_attr("id") and BAD_CONTAINER.search(parent["id"]):
            return None
        if parent.name in ("article", "li", "section", "div"):
            if len(parent.find_all(["a", "h1", "h2", "h3", "strong"])) >= 2:
                return parent
    return None


def _extract_name(card: Tag) -> Optional[str]:
    for sel in ["h1", "h2", "h3", "strong", ".title", ".store__title", ".shop__title"]:
        el = card.select_one(sel)
        if el:
            t = normalize_ws(el.get_text(" ", strip=True))
            if looks_like_store_name(t):
                return t
    # Fallback: any line with 'rimi'
    text = card.get_text("\n", strip=True)
    for line in text.splitlines():
        l = normalize_ws(line)
        if looks_like_store_name(l):
            return l
    return None


def _extract_address(card: Tag) -> Optional[str]:
    text = card.get_text("\n", strip=True)
    for line in text.splitlines():
        l = normalize_ws(line)
        if looks_like_address(l):
            return l
    return None


def parse_from_dom(html: str) -> List[StoreRow]:
    soup = BeautifulSoup(html, "lxml")

    # Find visible “Juhised” anchors that link to Google Maps
    hint_links = soup.select(
        "a:-soup-contains('Juhised')[href*='google.com/maps'], "
        "a:-soup-contains('Juhised')[href*='goo.gl/maps'], "
        "a:-soup-contains('Juhised')[href*='maps.app.goo.gl']"
    )

    rows: List[StoreRow] = []
    seen: set[Tuple[str, str]] = set()

    def add_card(card: Optional[Tag], href: Optional[str]):
        if not card:
            return
        # skip if container is clearly non-content
        if (card.has_attr("class") and BAD_CONTAINER.search(" ".join(card.get("class", [])))) or \
           (card.has_attr("id") and BAD_CONTAINER.search(card["id"])):
            return
        lat, lon = extract_latlon_from_href(href)
        if lat is None or lon is None:
            return
        name = _extract_name(card)
        addr = _extract_address(card)
        if not name or not addr:
            return
        key = (normalize_ws(name), normalize_ws(addr))
        if key in seen:
            return
        seen.add(key)
        rows.append(StoreRow(name=key[0], address=key[1], lat=lat, lon=lon, external_key=None))

    # 1) Around “Juhised” links
    for a in hint_links:
        card = _nearest_card(a)
        add_card(card, a.get("href"))

    # 2) If nothing, fall back to any Google Maps anchor
    if not rows:
        for a in soup.select("a[href*='google.com/maps'], a[href*='goo.gl/maps'], a[href*='maps.app.goo.gl']"):
            card = _nearest_card(a)
            add_card(card, a.get("href"))

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
            if not (name and address):
                continue
            if not looks_like_store_name(name) or not looks_like_address(address):
                continue
            try:
                latf = float(str(lat).replace(",", ".")) if lat is not None else None
                lonf = float(str(lon).replace(",", ".")) if lon is not None else None
            except ValueError:
                latf = lonf = None
            # Prefer only rows that include lat/lon from JSON; otherwise DOM step will supply
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
        log(f"json payload rows (pre-filter): {len(rows_json)}")
    rows_dom = parse_from_dom(html)
    log(f"dom rows (pre-dedup): {len(rows_dom)}")

    # Merge (DOM has stricter validity checks incl. lat/lon)
    rows_all = (rows_json + rows_dom) if rows_json else rows_dom
    rows = dedup_rows([r for r in rows_all if looks_like_store_name(r.name) and looks_like_address(r.address)])
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

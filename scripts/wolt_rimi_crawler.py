#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt → Rimi EAN crawler (stand-alone)
- Targets a single Wolt venue (default: rimi-haabersti)
- Reads category URLs/slugs from a file (one per line)
- Harvests product data, including barcode_gtin (EAN)
- Writes to CSV (no DB side-effects)

Usage:
  python wolt_rimi_crawler.py \
    --venue-slug rimi-haabersti \
    --categories-file rimi-haabersti.txt \
    --language et \
    --out out/rimi_wolt_haabersti.csv \
    --delay 0.2
"""

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Any
import requests


API_BASE = "https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1"
DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://wolt.com",
    "referer": "https://wolt.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    ),
}

CSV_FIELDS = [
    "chain",                # "Rimi"
    "channel",              # "wolt"
    "venue_slug",           # e.g. rimi-haabersti
    "category_slug",        # e.g. pitsa-58
    "category_name",        # as returned by API, if available
    "ext_id",               # Wolt product id if present
    "name",
    "brand",
    "price",
    "currency",
    "unit_price_value",
    "unit_price_unit",
    "barcode_gtin",         # EAN we need
    "description",
    "image_url",
]

CHAIN = "Rimi"
CHANNEL = "wolt"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Wolt → Rimi EAN crawler")
    p.add_argument("--venue-slug", default="rimi-haabersti", help="Wolt venue slug")
    p.add_argument("--categories-file", required=True, help="Path to file with category URLs or slugs (one per line)")
    p.add_argument("--language", default="et", help="Language code (et/en/fi/...)")
    p.add_argument("--out", default="out/rimi_wolt.csv", help="Output CSV path")
    p.add_argument("--delay", type=float, default=0.2, help="Delay between requests (seconds)")
    p.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout (seconds)")
    return p.parse_args()


_slug_re = re.compile(r"/categories/slug/([^/?#]+)")

def extract_category_slug(line: str) -> str:
    """Accept either a full Wolt category URL or just '<slug>'. Returns slug."""
    line = line.strip()
    if not line:
        return ""
    m = _slug_re.search(line)
    if m:
        return m.group(1)
    # If it's already a slug (no scheme), return as-is.
    return line.split("?")[0].strip()


def read_category_slugs(path: str) -> List[str]:
    slugs = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        s = extract_category_slug(raw)
        if s:
            slugs.append(s)
    # de-dup while preserving order
    seen = set()
    ordered = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    return ordered


def build_category_url(venue_slug: str, category_slug: str, language: str) -> str:
    # Example:
    # /venues/slug/rimi-haabersti/assortment/categories/slug/pitsa-58?language=et
    return f"{API_BASE}/venues/slug/{venue_slug}/assortment/categories/slug/{category_slug}?language={language}"


def request_json(session: requests.Session, url: str, timeout: float) -> Dict[str, Any]:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def find_products_in_payload(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Attempt to extract:
      - a list of product-like items (each either the product itself or with 'product' key),
      - a 'next' URL if present,
      - a 'category name' if present.

    Wolt payloads vary a bit by endpoint. We probe a few shapes safely:
    """
    items: List[Dict[str, Any]] = []
    next_url: Optional[str] = None
    category_name: Optional[str] = None

    # common places to look:
    # - payload.get('items')
    # - payload.get('products')
    # - payload.get('category', {}).get('items')
    # pagination hints:
    # - payload.get('paging', {}).get('next')
    # - payload.get('links', {}).get('next')
    # - payload.get('next') or payload.get('cursor', {}).get('next')
    # category name:
    # - payload.get('category', {}).get('name')

    if isinstance(payload.get("category"), dict):
        category_name = payload["category"].get("name") or category_name
        cat_items = payload["category"].get("items")
        if isinstance(cat_items, list):
            items.extend(cat_items)

    # top-level 'items'
    if isinstance(payload.get("items"), list):
        items.extend(payload["items"])

    # sometimes 'products'
    if isinstance(payload.get("products"), list):
        items.extend(payload["products"])

    # pagination
    paging = payload.get("paging") or {}
    if isinstance(paging, dict):
        next_url = paging.get("next") or next_url
        # sometimes 'next_cursor' exists but needs embedding; we only follow 'next' when it's a URL.

    links = payload.get("links") or {}
    if isinstance(links, dict):
        next_url = links.get("next") or next_url

    # Some endpoints embed 'next' directly
    if not next_url and isinstance(payload.get("next"), str):
        next_url = payload.get("next")

    # Normalize items: flatten if wrapper objects contain {'product': {...}}
    flat: List[Dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict) and "product" in it and isinstance(it["product"], dict):
            flat.append(it["product"])
        elif isinstance(it, dict):
            flat.append(it)

    return flat, next_url, category_name


def pick_first(d: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def extract_image_url(prod: Dict[str, Any]) -> Optional[str]:
    # Try a few common shapes
    # prod.get('image', {}).get('url') or images[0]['url'], or media[0]['url']
    img = prod.get("image")
    if isinstance(img, dict) and isinstance(img.get("url"), str):
        return img["url"]
    images = prod.get("images") or prod.get("media") or []
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict) and isinstance(first.get("url"), str):
            return first["url"]
    return None


def extract_numeric(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace(",", "."))
        except Exception:
            return None
    return None


def product_to_row(venue_slug: str, category_slug: str, category_name: Optional[str], prod: Dict[str, Any]) -> Dict[str, Any]:
    # Best-effort extraction; Wolt models differ by market/vertical
    ext_id = pick_first(prod, "id", "ext_id", "sku", default=None)
    name = pick_first(prod, "name", "title", default="")
    brand = pick_first(prod, "brand", "brand_name", default=None)
    currency = pick_first(prod, "currency", default=None)

    # Price can be in several keys; prefer 'price' or 'unit_price'
    price = pick_first(prod, "price", "base_price", default=None)
    price = extract_numeric(price)

    unit_price_value = None
    unit_price_unit = None
    unit_price = prod.get("unit_price")
    if isinstance(unit_price, dict):
        unit_price_value = extract_numeric(unit_price.get("value"))
        unit_price_unit = unit_price.get("unit")

    barcode_gtin = pick_first(prod, "barcode_gtin", "gtin", "ean", default=None)
    description = pick_first(prod, "description", "short_description", default=None)
    image_url = extract_image_url(prod)

    return {
        "chain": CHAIN,
        "channel": CHANNEL,
        "venue_slug": venue_slug,
        "category_slug": category_slug,
        "category_name": category_name or "",
        "ext_id": ext_id or "",
        "name": name or "",
        "brand": brand or "",
        "price": f"{price:.2f}" if isinstance(price, float) else "",
        "currency": currency or "",
        "unit_price_value": f"{unit_price_value:.4f}" if isinstance(unit_price_value, float) else "",
        "unit_price_unit": unit_price_unit or "",
        "barcode_gtin": (str(barcode_gtin) if barcode_gtin is not None else ""),
        "description": description or "",
        "image_url": image_url or "",
    }


def crawl_category(session: requests.Session, venue_slug: str, category_slug: str, language: str, timeout: float, delay: float) -> List[Dict[str, Any]]:
    url = build_category_url(venue_slug, category_slug, language)
    rows: List[Dict[str, Any]] = []
    seen_ids: set = set()

    while url:
        payload = request_json(session, url, timeout=timeout)
        products, next_url, category_name = find_products_in_payload(payload)

        for p in products:
            row = product_to_row(venue_slug, category_slug, category_name, p)
            # de-dup on (ext_id or name+barcode) to be safe
            key = row["ext_id"] or f"{row['name']}|{row['barcode_gtin']}"
            if key not in seen_ids:
                seen_ids.add(key)
                rows.append(row)

        if not next_url:
            break

        url = next_url if next_url.startswith("http") else (API_BASE + next_url if next_url.startswith("/") else None)
        if delay:
            time.sleep(delay)

    return rows


def write_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    args = parse_args()
    cat_slugs = read_category_slugs(args.categories_file)
    if not cat_slugs:
        print("No category slugs/URLs found in file.", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    all_rows: List[Dict[str, Any]] = []
    for idx, slug in enumerate(cat_slugs, 1):
        try:
            rows = crawl_category(
                session=session,
                venue_slug=args.venue_slug,
                category_slug=slug,
                language=args.language,
                timeout=args.timeout,
                delay=args.delay,
            )
            all_rows.extend(rows)
            print(f"[{idx}/{len(cat_slugs)}] {slug}: {len(rows)} items")
        except requests.HTTPError as e:
            print(f"HTTP error on {slug}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Error on {slug}: {e}", file=sys.stderr)

        if args.delay:
            time.sleep(args.delay)

    # Optional: stable sort by category_slug then name
    all_rows.sort(key=lambda r: (r["category_slug"], r["name"]))
    write_csv(args.out, all_rows)
    print(f"Done. Wrote {len(all_rows)} rows → {args.out}")


if __name__ == "__main__":
    main()

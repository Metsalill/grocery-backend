#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt → Rimi EAN crawler (stand-alone)

- Targets a single Wolt venue (default: rimi-haabersti)
- Reads category URLs OR slugs from a file (one per line)
- Harvests products, incl. barcode_gtin (EAN)
- Writes clean CSV (no DB writes)
- Normalizes prices: treats integer-like values as cents (579 -> 5.79)
- Adds `barcode_gtin_text` to display EANs nicely in Excel

Usage example:
  python scripts/wolt_rimi_crawler.py \
    --venue-slug rimi-haabersti \
    --categories-file data/rimi-haabersti.txt \
    --language et \
    --out out/rimi_wolt_haabersti.csv \
    --delay 0.2
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
    "chain",                  # "Rimi"
    "channel",                # "wolt"
    "venue_slug",             # rimi-haabersti
    "category_slug",
    "category_name",
    "ext_id",
    "name",
    "brand",
    "price",                  # in euros (5.79)
    "currency",
    "unit_price_value",
    "unit_price_unit",
    "barcode_gtin",           # raw GTIN for DB merge
    "barcode_gtin_text",      # Excel-friendly view (e.g., '4750123456789)
    "description",
    "image_url",
]

CHAIN = "Rimi"
CHANNEL = "wolt"

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Wolt → Rimi EAN crawler")
    p.add_argument("--venue-slug", default="rimi-haabersti", help="Wolt venue slug")
    p.add_argument("--categories-file", required=True, help="Path to file with category URLs/slugs (one per line)")
    p.add_argument("--language", default="et", help="Language code")
    p.add_argument("--out", default="out/rimi_wolt_haabersti.csv", help="Output CSV path")
    p.add_argument("--delay", type=float, default=0.2, help="Delay between HTTP requests (seconds)")
    p.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout (seconds)")
    return p.parse_args()

_slug_re = re.compile(r"/categories/slug/([^/?#]+)")

def extract_category_slug(line: str) -> str:
    line = line.strip()
    if not line:
        return ""
    m = _slug_re.search(line)
    if m:
        return m.group(1)
    return line.split("?")[0].strip()

def read_category_slugs(path: str) -> List[str]:
    slugs, seen = [], set()
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        s = extract_category_slug(raw)
        if s and s not in seen:
            seen.add(s)
            slugs.append(s)
    return slugs

def build_category_url(venue_slug: str, category_slug: str, language: str) -> str:
    return f"{API_BASE}/venues/slug/{venue_slug}/assortment/categories/slug/{category_slug}?language={language}"

def request_json(session: requests.Session, url: str, timeout: float) -> Dict[str, Any]:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def find_products_in_payload(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    items: List[Dict[str, Any]] = []
    next_url: Optional[str] = None
    category_name: Optional[str] = None

    if isinstance(payload.get("category"), dict):
        category_name = payload["category"].get("name") or category_name
        cat_items = payload["category"].get("items")
        if isinstance(cat_items, list):
            items.extend(cat_items)

    if isinstance(payload.get("items"), list):
        items.extend(payload["items"])

    if isinstance(payload.get("products"), list):
        items.extend(payload["products"])

    paging = payload.get("paging") or {}
    if isinstance(paging, dict):
        next_url = paging.get("next") or next_url

    links = payload.get("links") or {}
    if isinstance(links, dict):
        next_url = links.get("next") or next_url

    if not next_url and isinstance(payload.get("next"), str):
        next_url = payload.get("next")

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
    ext_id = pick_first(prod, "id", "ext_id", "sku", default="")
    name = pick_first(prod, "name", "title", default="") or ""
    brand = pick_first(prod, "brand", "brand_name", default="") or ""
    currency = pick_first(prod, "currency", default="") or ""

    # price normalization
    price_raw = pick_first(prod, "price", "base_price", default=None)
    price = extract_numeric(price_raw)
    if isinstance(price, float):
        if price >= 100 and str(price_raw).isdigit():  # likely cents
            price = price / 100.0
        price_str = f"{price:.2f}"
    else:
        price_str = ""

    unit_price_value = ""
    unit_price_unit = ""
    unit_price = prod.get("unit_price")
    if isinstance(unit_price, dict):
        v = extract_numeric(unit_price.get("value"))
        unit_price_value = f"{v:.4f}" if isinstance(v, float) else ""
        unit_price_unit = unit_price.get("unit") or ""

    barcode_gtin = pick_first(prod, "barcode_gtin", "gtin", "ean", default="")
    ean_text = f"'{barcode_gtin}" if barcode_gtin not in (None, "") else ""

    description = pick_first(prod, "description", "short_description", default="") or ""
    image_url = extract_image_url(prod) or ""

    return {
        "chain": CHAIN,
        "channel": CHANNEL,
        "venue_slug": venue_slug,
        "category_slug": category_slug,
        "category_name": category_name or "",
        "ext_id": str(ext_id or ""),
        "name": name,
        "brand": brand,
        "price": price_str,
        "currency": currency,
        "unit_price_value": unit_price_value,
        "unit_price_unit": unit_price_unit,
        "barcode_gtin": str(barcode_gtin or ""),
        "barcode_gtin_text": ean_text,
        "description": description,
        "image_url": image_url,
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

    all_rows.sort(key=lambda r: (r["category_slug"], r["name"]))
    write_csv(args.out, all_rows)
    print(f"Done. Wrote {len(all_rows)} rows → {args.out}")

if __name__ == "__main__":
    main()

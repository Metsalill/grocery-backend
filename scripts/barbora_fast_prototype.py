#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee FAST prototype crawler — requests only, NO Playwright.

Why this exists
----------------
The current production scraper (barbora_crawl_categories_pw.py) visits every
single PDP with Playwright to scrape name/price/brand/category/image. But the
category LISTING page already embeds a full JSON array of every product on
that page, in a plain <script> tag:

    <script>
        window.b_productList = [{"id":"...","title":"...","price":0.89,
                                  "brand_name":"7DAYS","category_name_full_path":"...",
                                  "image":"https://cdn.barbora.ee/...","Url":"...", ...}, ...]
    </script>

This is server-side rendered — no JS execution needed. This script fetches
category pages with plain `requests`, extracts that JSON blob, and paginates
via ?page=N. No browser, no PDP visits.

IMPORTANT — this is a STANDALONE PROTOTYPE for comparison testing only.
It does NOT write to the database and does NOT touch the production scraper
or its GitHub Actions workflow.

Known limitation (confirmed acceptable): Barbora does not expose EAN/GTIN
anywhere — not on the listing, not on the PDP. The production scraper has
always written ean_raw="" for every row.

Usage
-----
    python barbora_fast_prototype.py --cats-file data/barbora_categories.txt --limit-cats 3
    python barbora_fast_prototype.py --url https://barbora.ee/leivad-saiad-kondiitritooted
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import requests

BASE = "https://barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME = "Barbora ePood"
STORE_CHANNEL = "online"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
}

SIZE_RE = re.compile(r"(?ix)(\d+\s?(?:x\s?\d+)?\s?(?:ml|l|cl|g|kg|mg|tk|pcs))|(\d+\s?x\s?\d+)")

CSV_HEADER = [
    "store_chain", "store_name", "store_channel",
    "ext_id", "ean_raw", "name", "size_text",
    "brand", "price", "currency", "image_url",
    "category_path", "category_leaf", "source_url",
]


def extract_size_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    m = SIZE_RE.search(name)
    return m.group(0) if m else None


def set_page_param(url: str, page_num: int) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q["page"] = str(page_num)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def find_max_page(html: str) -> int:
    nums = [int(n) for n in re.findall(r"\?page=(\d+)", html)]
    return max(nums) if nums else 1


def extract_product_list(html: str) -> List[Dict]:
    marker = "window.b_productList = "
    idx = html.find(marker)
    if idx == -1:
        return []
    start = idx + len(marker)
    decoder = json.JSONDecoder()
    try:
        data, _end = decoder.raw_decode(html, start)
    except json.JSONDecodeError as e:
        print(f"[warn] JSON decode failed: {e}", file=sys.stderr)
        return []
    if isinstance(data, list):
        return data
    return []


def fetch(session: requests.Session, url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                return resp.text
            print(f"[warn] {url} -> HTTP {resp.status_code} (attempt {attempt+1}/{retries})")
        except requests.RequestException as e:
            print(f"[warn] {url} -> {e} (attempt {attempt+1}/{retries})")
        time.sleep(1.5 * (attempt + 1))
    return None


def crawl_category(session: requests.Session, cat_url: str, req_delay: float) -> List[Dict]:
    all_products: Dict[str, Dict] = {}

    html = fetch(session, cat_url)
    if html is None:
        print(f"[cat] {cat_url} -> FAILED (no response)")
        return []

    max_page = find_max_page(html)

    for page_num in range(1, max_page + 1):
        page_url = cat_url if page_num == 1 else set_page_param(cat_url, page_num)
        if page_num > 1:
            html = fetch(session, page_url)
            if html is None:
                print(f"[cat] {cat_url} page {page_num} -> FAILED, stopping category")
                break
            time.sleep(req_delay)

        products = extract_product_list(html)
        if not products and page_num == 1:
            print(f"[cat] {cat_url} -> 0 products found on page 1 (blocked? layout change?)")

        for p in products:
            ext_id = str(p.get("id") or "").strip()
            if not ext_id:
                continue
            name = p.get("title") or ""
            price = p.get("price")
            brand = p.get("brand_name") or ""
            cat_path = p.get("category_name_full_path") or ""
            cat_leaf = cat_path.split("/")[-1] if cat_path else ""
            image_url = p.get("big_image") or p.get("image") or ""
            slug = p.get("Url") or ""
            source_url = f"{BASE}/toode/{slug}" if slug else cat_url

            all_products[ext_id] = {
                "store_chain": STORE_CHAIN,
                "store_name": STORE_NAME,
                "store_channel": STORE_CHANNEL,
                "ext_id": ext_id,
                "ean_raw": "",
                "name": name,
                "size_text": extract_size_from_name(name) or "",
                "brand": brand,
                "price": price if price is not None else "",
                "currency": "EUR",
                "image_url": image_url,
                "category_path": cat_path,
                "category_leaf": cat_leaf,
                "source_url": source_url,
            }

    print(f"[cat] {cat_url} -> {len(all_products)} unique products across {max_page} page(s)")
    return list(all_products.values())


def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Barbora fast prototype crawler (requests only)")
    ap.add_argument("--cats-file", default="", help="Text file with category URLs, one per line")
    ap.add_argument("--url", action="append", default=[], help="Single category URL (repeatable)")
    ap.add_argument("--limit-cats", type=int, default=0, help="Only process first N categories (0=all)")
    ap.add_argument("--req-delay", type=float, default=0.4, help="Delay between page requests (seconds)")
    ap.add_argument("--output-csv", default="out/barbora_fast_prototype.csv")
    args = ap.parse_args()

    cats: List[str] = list(args.url)
    if args.cats_file:
        cats.extend(read_lines(args.cats_file))

    if not cats:
        print("No categories given. Use --cats-file or --url.", file=sys.stderr)
        sys.exit(1)

    if args.limit_cats > 0:
        cats = cats[: args.limit_cats]

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)

    session = requests.Session()
    t0 = time.time()
    total_rows = 0

    with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()

        for i, cat_url in enumerate(cats, start=1):
            print(f"[{i}/{len(cats)}] {cat_url}")
            rows = crawl_category(session, cat_url, args.req_delay)
            for r in rows:
                writer.writerow(r)
            total_rows += len(rows)
            time.sleep(args.req_delay)

    elapsed = time.time() - t0
    print(f"\n[done] {total_rows} rows from {len(cats)} categories in {elapsed:.1f}s")
    print(f"[done] wrote {args.output_csv}")


if __name__ == "__main__":
    main()

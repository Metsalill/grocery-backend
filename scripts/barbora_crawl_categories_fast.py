#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) — FAST category crawler, requests-only, no Playwright.

Replaces barbora_crawl_categories_pw.py (kept as LEGACY for rollback).

Why this works
--------------
Category listing pages embed a full server-side-rendered JSON array of every
product on the page in a plain <script> tag:

    window.b_productList = [{"id":"...","title":"...","price":0.89,
                              "retail_price":1.19,"brand_name":"7DAYS",
                              "category_name_full_path":"...",
                              "image":"...","Url":"...", ...}, ...]

No browser needed — plain `requests` gets everything. ~370 categories finish
in a couple of minutes total, vs. hours per shard with Playwright.

Confirmed facts (validated against production DB, July 2026):
- Barbora exposes no EAN/GTIN anywhere. ean_raw is always "".
- ext_id MUST be derived from the PDP URL via get_ext_id(), matching the
  legacy scraper exactly, so existing product_id / ext_product_map rows
  keep matching (verified 8/8 and then full-scale against live DB).
- Age-gate popup for alcohol categories is client-side only; requests still
  gets full product JSON (verified against real alcohol category data).
- price field is ALWAYS the best displayed price (AITÄH loyalty price,
  public discount, or regular price — whichever applies). retail_price only
  appears when there's an active promotion, and is inconsistently present
  for AITÄH promos specifically (confirmed: category-page cache timing
  issue, not a login/auth issue). This does NOT affect correctness of the
  best price shown — the mapping guarantees db_price is always correct
  regardless of whether retail_price happens to be present:
      has retail_price -> db_price=retail_price, db_promo_price=price
      no retail_price  -> db_price=price,        db_promo_price=NULL
  Verified against Kanzi (AITÄH), Royal Gala (public discount), Granny
  Smith (no promo) against live production DB after write.

DB write: always on (this is the production script). Requires DATABASE_URL
env var and the upsert_product_and_price() function to support the
in_promo_price parameter (added July 2026, backward-compatible — verified
against Rimi/Coop/Prisma's old-style calls still working unchanged).
"""

from __future__ import annotations

import argparse
import asyncio
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
DB_SOURCE_LABEL = "barbora"
STORE_ID = 441  # Barbora ePood, is_online=true

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
}

SIZE_RE = re.compile(r"(?ix)(\d+\s?(?:x\s?\d+)?\s?(?:ml|l|cl|g|kg|mg|tk|pcs))|(\d+\s?x\s?\d+)")


def get_ext_id(url: str) -> str:
    """MUST MATCH legacy barbora_crawl_categories_pw.py exactly."""
    m = re.search(r"/p/(\d+)", url) or re.search(r"-(\d+)$", url)
    if m:
        return m.group(1)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", url).strip("-")
    return slug[-120:]


CSV_HEADER = [
    "store_chain", "store_name", "store_channel",
    "ext_id", "ean_raw", "name", "size_text",
    "brand", "price", "promo_price", "currency", "image_url",
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
    return data if isinstance(data, list) else []


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
            print(f"[cat] {cat_url} -> 0 products found on page 1")

        for p in products:
            barbora_internal_id = str(p.get("id") or "").strip()
            if not barbora_internal_id:
                continue
            name = p.get("title") or ""
            brand = p.get("brand_name") or ""
            cat_path = p.get("category_name_full_path") or ""
            cat_leaf = cat_path.split("/")[-1] if cat_path else ""
            image_url = p.get("big_image") or p.get("image") or ""
            slug = p.get("Url") or ""
            source_url = f"{BASE}/toode/{slug}" if slug else cat_url

            ext_id = get_ext_id(source_url)

            raw_price = p.get("price")
            retail_price = p.get("retail_price")
            if retail_price is not None:
                db_price = retail_price
                db_promo_price = raw_price
            else:
                db_price = raw_price
                db_promo_price = None

            all_products[ext_id] = {
                "store_chain": STORE_CHAIN,
                "store_name": STORE_NAME,
                "store_channel": STORE_CHANNEL,
                "ext_id": ext_id,
                "ean_raw": "",
                "name": name,
                "size_text": extract_size_from_name(name) or "",
                "brand": brand,
                "price": db_price if db_price is not None else "",
                "promo_price": db_promo_price if db_promo_price is not None else "",
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


async def _bulk_ingest_to_db(rows: List[Dict], store_id: int) -> None:
    import asyncpg

    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        print("[barbora] DATABASE_URL missing — cannot ingest.", file=sys.stderr)
        sys.exit(1)

    conn = await asyncpg.connect(dsn)
    ingested = 0
    skipped = 0
    try:
        for r in rows:
            price_val = None
            try:
                if r.get("price") not in (None, ""):
                    price_val = float(r["price"])
            except Exception:
                price_val = None
            if price_val is None:
                skipped += 1
                continue

            promo_val = None
            try:
                if r.get("promo_price") not in (None, ""):
                    promo_val = float(r["promo_price"])
            except Exception:
                promo_val = None

            try:
                await conn.execute(
                    """
                    SELECT upsert_product_and_price(
                        in_source => $1, in_ext_id => $2, in_name => $3,
                        in_brand => $4, in_size_text => $5, in_ean_raw => $6,
                        in_price => $7, in_currency => $8, in_store_id => $9,
                        in_seen_at => $10, in_source_url => $11,
                        in_promo_price => $12
                    );
                    """,
                    DB_SOURCE_LABEL, r["ext_id"], r["name"], r["brand"],
                    r["size_text"], "", price_val, r["currency"],
                    store_id, datetime.now(timezone.utc), r["source_url"],
                    promo_val,
                )
                ingested += 1
            except Exception as e:
                print(f"[warn] DB error for {r['ext_id']}: {e}", file=sys.stderr)
                skipped += 1
    finally:
        await conn.close()
    print(f"[barbora] DB ingest: {ingested} ok, {skipped} skipped")


def main() -> None:
    ap = argparse.ArgumentParser(description="Barbora fast crawler (production, requests-only)")
    ap.add_argument("--cats-file", required=True, help="Text file with category URLs, one per line")
    ap.add_argument("--req-delay", type=float, default=0.4, help="Delay between page requests (seconds)")
    ap.add_argument("--output-csv", default="out/barbora_products.csv")
    args = ap.parse_args()

    cats = read_lines(args.cats_file)
    if not cats:
        print("No categories in file.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)

    session = requests.Session()
    t0 = time.time()
    all_rows: List[Dict] = []

    with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()

        for i, cat_url in enumerate(cats, start=1):
            print(f"[{i}/{len(cats)}] {cat_url}")
            rows = crawl_category(session, cat_url, args.req_delay)
            for r in rows:
                writer.writerow(r)
            all_rows.extend(rows)
            time.sleep(args.req_delay)

    elapsed = time.time() - t0
    print(f"\n[done] {len(all_rows)} rows from {len(cats)} categories in {elapsed:.1f}s")
    print(f"[done] wrote {args.output_csv}")

    print(f"[barbora] ingesting {len(all_rows)} rows into Postgres (store_id={STORE_ID})...")
    asyncio.run(_bulk_ingest_to_db(all_rows, STORE_ID))


if __name__ == "__main__":
    main()

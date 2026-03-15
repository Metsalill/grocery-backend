#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee scraper — __NEXT_DATA__ / Apollo cache version.

No Playwright. No PDP visits. Fetches HTML category pages, extracts
the embedded Apollo cache from <script id="__NEXT_DATA__">, and pulls
all product data directly. Fast (~2-3 min for full catalog).

Flow:
  1. GET /tooted/ → discover all top-level category slugs
  2. For each category, paginate through ?sivu=1..N
  3. Extract products from Apollo cache (all data inline)
  4. Upsert via upsert_product_and_price()
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import asyncpg
except ImportError:
    asyncpg = None

# ── constants ─────────────────────────────────────────────────────────────────

BASE_URL   = "https://www.prismamarket.ee"
STORE_ID   = os.environ.get("STORE_ID", "14")

# Root category page slugs to crawl (Estonian paths)
# We discover sub-slugs dynamically from the navigation
ROOT_CATEGORIES = [
    "tooted/puu-ja-koogiviljad",
    "tooted/liha-kala-ja-mereannid",
    "tooted/piimatooted-munad-ja-rasvaained",
    "tooted/pagaritooted",
    "tooted/kuivtooted-ja-konservid",
    "tooted/joogid",
    "tooted/maiustused-ja-snackid",
    "tooted/valmistoidud-ja-pooltooted",
    "tooted/beebid-ja-lapsed",
    "tooted/kodumajapidamine",
    "tooted/isikuhooldus",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ── HTTP session ───────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return s


# ── __NEXT_DATA__ extraction ───────────────────────────────────────────────────

NEXT_DATA_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.DOTALL,
)

def fetch_next_data(session: requests.Session, url: str) -> Optional[dict]:
    """Fetch a page and extract __NEXT_DATA__ JSON."""
    try:
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            print(f"  [warn] {r.status_code} for {url}")
            return None
        m = NEXT_DATA_RE.search(r.text)
        if not m:
            print(f"  [warn] no __NEXT_DATA__ in {url}")
            return None
        return json.loads(m.group(1))
    except Exception as e:
        print(f"  [warn] fetch failed for {url}: {e}")
        return None


# ── product extraction from Apollo cache ──────────────────────────────────────

def extract_products_from_apollo(apollo: dict, slug: str) -> List[Dict]:
    """
    Pull all Product:* entries from the Apollo cache.
    Returns flat list of product dicts.
    """
    products = []
    seen_eans = set()

    for key, obj in apollo.items():
        if not key.startswith("Product:"):
            continue
        if not isinstance(obj, dict):
            continue

        # Basic fields
        name = (obj.get("name") or "").strip()
        if not name:
            continue

        ean = str(obj.get("ean") or "").strip()
        price = obj.get("price")

        # Prefer pricing.currentPrice
        pricing = obj.get("pricing") or {}
        if isinstance(pricing, dict):
            price = pricing.get("currentPrice") or pricing.get("regularPrice") or price

        if not price or float(price) <= 0:
            continue

        # Deduplicate by EAN
        if ean and ean in seen_eans:
            continue
        if ean:
            seen_eans.add(ean)

        # Brand
        brand = (obj.get("brandName") or "").strip()

        # Size text — from comparisonUnit / priceUnit
        size_text = ""
        comparison_unit = obj.get("comparisonUnit") or ""
        price_unit = obj.get("priceUnit") or ""
        # e.g. comparisonPrice=1.19, comparisonUnit=KG → size hint
        # We don't have explicit size, use priceUnit as fallback
        if price_unit and price_unit not in ("KPL", "PCE"):
            size_text = price_unit.lower()

        # Image URL
        image = ""
        try:
            details = obj.get("productDetails") or {}
            imgs = details.get("productImages") or {}
            main = imgs.get("mainImage") or {}
            template = main.get("urlTemplate") or ""
            if template:
                image = template.replace("{MODIFIERS}", "q_auto,f_auto,w_400").replace("{EXTENSION}", "webp")
        except Exception:
            pass

        # Category from hierarchyPath
        category = ""
        try:
            hp = obj.get("hierarchyPath") or []
            if hp:
                # last item is the most specific category
                last_ref = hp[0].get("__ref", "") if hp else ""
                m = re.search(r'"name":"([^"]+)"', last_ref)
                if m:
                    category = m.group(1)
        except Exception:
            pass

        # Source URL
        product_slug = obj.get("slug") or ""
        product_id = obj.get("id") or ean
        source_url = f"{BASE_URL}/toode/{product_slug}/{product_id}" if product_slug else ""

        products.append({
            "ext_id": str(product_id),
            "name": name,
            "brand": brand,
            "size_text": size_text,
            "ean": ean,
            "price": float(price),
            "image": image,
            "category": category,
            "source_url": source_url,
        })

    return products


# ── category pagination ────────────────────────────────────────────────────────

def scrape_category(session: requests.Session, slug: str) -> List[Dict]:
    """
    Scrape all pages of a category. Returns all products found.
    Prisma uses ?sivu=N for pagination (Finnish for 'page').
    """
    all_products = []
    seen_ext_ids = set()
    page = 1

    while True:
        if page == 1:
            url = f"{BASE_URL}/{slug}"
        else:
            url = f"{BASE_URL}/{slug}?sivu={page}"

        data = fetch_next_data(session, url)
        if not data:
            break

        apollo = (data.get("props") or {}).get("pageProps", {}).get("apolloState") or {}
        if not apollo:
            break

        products = extract_products_from_apollo(apollo, slug)
        if not products:
            break

        # Detect if we're seeing the same products (end of pagination)
        new_count = 0
        for p in products:
            if p["ext_id"] not in seen_ext_ids:
                seen_ext_ids.add(p["ext_id"])
                all_products.append(p)
                new_count += 1

        print(f"  page {page}: {len(products)} products ({new_count} new)")

        if new_count == 0:
            break

        # Check if there are more pages
        # Look for pagination info in apollo cache
        total_products = None
        for key, obj in apollo.items():
            if isinstance(obj, dict) and obj.get("__typename") == "SectionProducts":
                total_products = obj.get("totalCount") or obj.get("count")
                break

        if total_products and len(seen_ext_ids) >= total_products:
            break

        page += 1
        time.sleep(0.3)

        # Safety limit
        if page > 50:
            break

    return all_products


# ── subcategory discovery ──────────────────────────────────────────────────────

def discover_subcategories(session: requests.Session, slug: str) -> List[str]:
    """
    Fetch a top-level category page and find subcategory slugs
    from the navigation section in Apollo cache.
    """
    url = f"{BASE_URL}/{slug}"
    data = fetch_next_data(session, url)
    if not data:
        return []

    apollo = (data.get("props") or {}).get("pageProps", {}).get("apolloState") or {}
    subcats = []

    for key, obj in apollo.items():
        if not isinstance(obj, dict):
            continue
        if obj.get("__typename") != "HierarchyItem":
            continue
        item_slug = obj.get("slug") or ""
        if item_slug and item_slug != slug.replace("tooted/", ""):
            full_slug = f"tooted/{item_slug}"
            if full_slug not in subcats and full_slug != slug:
                subcats.append(full_slug)

    return subcats


# ── DB ingest ──────────────────────────────────────────────────────────────────

async def ingest_to_db(products: List[Dict]) -> None:
    if not asyncpg:
        print("[db] asyncpg not available")
        return

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[db] DATABASE_URL not set")
        return

    store_id = int(os.environ.get("STORE_ID", "14"))
    conn = await asyncpg.connect(db_url)
    total = errors = 0

    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        for p in products:
            try:
                await conn.fetchval(
                    """
                    SELECT upsert_product_and_price(
                        $1::text, $2::text, $3::text, $4::text, $5::text,
                        $6::text, $7::numeric, $8::text, $9::integer,
                        $10::timestamptz, $11::text
                    );
                    """,
                    "prisma",
                    p["ext_id"],
                    p["name"],
                    p["brand"],
                    p["size_text"],
                    p["ean"],
                    p["price"],
                    "EUR",
                    store_id,
                    now,
                    p["source_url"],
                )
                total += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"[db] upsert failed for {p['ext_id']}: {e}")
    finally:
        await conn.close()

    print(f"[db] upserted {total} rows (errors: {errors})")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    session = make_session()
    all_products: List[Dict] = []
    seen_ext_ids: set = set()

    print(f"[prisma] starting scrape of {len(ROOT_CATEGORIES)} root categories")

    for root_slug in ROOT_CATEGORIES:
        print(f"\n[cat] {root_slug}")

        # Try direct category first
        products = scrape_category(session, root_slug)

        if not products:
            # Try discovering subcategories
            print(f"  no products directly, checking subcategories...")
            subcats = discover_subcategories(session, root_slug)
            print(f"  found {len(subcats)} subcategories")
            for subcat in subcats:
                print(f"  [subcat] {subcat}")
                sub_products = scrape_category(session, subcat)
                products.extend(sub_products)
                time.sleep(0.3)

        # Deduplicate globally
        new = 0
        for p in products:
            if p["ext_id"] not in seen_ext_ids:
                seen_ext_ids.add(p["ext_id"])
                all_products.append(p)
                new += 1

        print(f"  → {new} new products (total so far: {len(all_products)})")

    print(f"\n[prisma] total unique products: {len(all_products)}")

    if all_products:
        asyncio.run(ingest_to_db(all_products))


if __name__ == "__main__":
    main()

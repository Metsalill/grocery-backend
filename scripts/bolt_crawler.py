#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bolt Food crawler (Coop venues) — direct API version.

Fetches categories dynamically via getMenuCategories so SMC IDs are always
current. No static category files needed (they go stale as Bolt rotates IDs).
"""

import argparse
import asyncio
import csv
import datetime
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import asyncpg
except ImportError:
    asyncpg = None

# ---------------------- constants ---------------------- #
API_BASE       = "https://deliveryuser.live.boltsvc.net/deliveryClient/public/getMenuDishes"
CATEGORIES_API = "https://deliveryuser.live.boltsvc.net/deliveryClient/public/getMenuCategories"
API_VERSION    = "FW.1.106"
DELIVERY_LAT   = "58.377983"
DELIVERY_LNG   = "26.729038"

SMC_RE      = re.compile(r"/smc/(\d+)")
CITY_RE     = re.compile(r"/et-[Ee][Ee]/([^/]+)/p/(\d+)")
SPACE_RE    = re.compile(r"\s+")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------- helpers ---------------------- #
def norm_space(s: str) -> str:
    return SPACE_RE.sub(" ", s or "").strip()


ESTONIAN_MAP = str.maketrans({
    "ä": "a", "ö": "o", "ü": "u", "õ": "o",
    "š": "s", "ž": "z",
    "Ä": "a", "Ö": "o", "Ü": "u", "Õ": "o",
    "Š": "s", "Ž": "z",
})


def slugify_for_ext(s: str) -> str:
    s2 = (s or "").translate(ESTONIAN_MAP).lower()
    s2 = re.sub(r"[^a-z0-9]+", "-", s2).strip("-")
    return s2


def _cents_to_eur(val) -> Optional[float]:
    if val is None:
        return None
    f = float(val)
    if isinstance(val, int) and f > 100:
        return f / 100.0
    if isinstance(val, float) and f < 100:
        return f
    if isinstance(val, int) and f <= 100:
        return f
    return f / 100.0


# ---------------------- session ---------------------- #
def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "et-EE,et;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Origin": "https://food.bolt.eu",
        "Referer": "https://food.bolt.eu/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s


# ---------------------- dynamic category fetch ---------------------- #
def fetch_categories_from_api(
    session: requests.Session,
    venue_id: str,
    session_id: str,
    device_id: str,
    delivery_lat: str = DELIVERY_LAT,
    delivery_lng: str = DELIVERY_LNG,
) -> List[Tuple[str, str]]:
    """
    Call getMenuCategories to get current SMC IDs for this venue.
    Returns list of (category_name, smc_id) pairs.
    """
    params = {
        "provider_id": venue_id,
        "delivery_lat": delivery_lat,
        "delivery_lng": delivery_lng,
        "version": API_VERSION,
        "language": "et-EE",
        "session_id": session_id,
        "distinct_id": f"%24device%3A{device_id}",
        "country": "ee",
        "device_name": "web",
        "device_os_version": "web",
        "deviceId": device_id,
        "deviceType": "web",
    }

    try:
        r = session.get(CATEGORIES_API, params=params, timeout=30)
        if r.status_code != 200:
            print(f"[categories] API returned {r.status_code}: {r.text[:200]}")
            return []

        data = r.json()
    except Exception as e:
        print(f"[categories] API call failed: {e}")
        return []

    categories: List[Tuple[str, str]] = []

    def _extract_cats(obj):
        if isinstance(obj, list):
            for item in obj:
                _extract_cats(item)
        elif isinstance(obj, dict):
            # A category object typically has id + name
            cat_id = str(obj.get("id") or obj.get("category_id") or "")
            cat_name = norm_space(obj.get("name") or obj.get("title") or "")
            if cat_id and cat_name and cat_id.isdigit():
                categories.append((cat_name, cat_id))
            # Recurse into sub-keys
            for key, val in obj.items():
                if isinstance(val, (dict, list)):
                    _extract_cats(val)

    _extract_cats(data)

    # Deduplicate preserving order
    seen = set()
    unique = []
    for name, smc in categories:
        if smc not in seen:
            seen.add(smc)
            unique.append((name, smc))

    print(f"[categories] found {len(unique)} categories via API for venue {venue_id}")
    return unique


# ---------------------- file parsing (kept as fallback) ---------------------- #
def parse_categories_file(path: str) -> List[Tuple[str, str]]:
    """
    Returns list of (category_name, smc_id) pairs from a .txt file.
    Accepts lines like:
      https://food.bolt.eu/.../smc/1234567/?categoryName=Foo
    and extracts the smc number.
    """
    out: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Support "name -> url" format
            if "->" in line:
                name, href = [x.strip() for x in line.split("->", 1)]
            else:
                href = line
                qs = parse_qs(urlparse(href).query)
                name = qs.get("categoryName", [""])[0] or "Unknown"

            m = SMC_RE.search(href)
            if m:
                out.append((name, m.group(1)))
            else:
                print(f"[warn] no smc ID in line: {line[:80]}")
    return out


# ---------------------- product fetching ---------------------- #
def fetch_category_products(
    session: requests.Session,
    venue_id: str,
    category_id: str,
    session_id: str,
    device_id: str,
    delivery_lat: str = DELIVERY_LAT,
    delivery_lng: str = DELIVERY_LNG,
) -> dict:
    params = {
        "provider_id": venue_id,
        "category_id": category_id,
        "delivery_lat": delivery_lat,
        "delivery_lng": delivery_lng,
        "version": API_VERSION,
        "language": "et-EE",
        "session_id": session_id,
        "distinct_id": f"%24device%3A{device_id}",
        "country": "ee",
        "device_name": "web",
        "device_os_version": "web",
        "deviceId": device_id,
        "deviceType": "web",
    }

    try:
        r = session.get(API_BASE, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"  [warn] API returned {r.status_code} for category_id={category_id}: {r.text[:100]}")
            return {}
    except Exception as e:
        print(f"  [warn] API call failed for category_id={category_id}: {e}")
        return {}


def parse_menu_dishes_response(data: dict, cat_name: str, venue_id: str) -> List[Dict]:
    if not data or not isinstance(data, dict):
        return []

    products = []

    def _extract_items(obj):
        if isinstance(obj, list):
            for item in obj:
                _extract_items(item)
        elif isinstance(obj, dict):
            for key in ("items", "dishes", "products", "data"):
                val = obj.get(key)
                if isinstance(val, list) and val:
                    for item in val:
                        if isinstance(item, dict) and ("name" in item or "id" in item):
                            _process_item(item)
                elif isinstance(val, dict):
                    _extract_items(val)
            for key, val in obj.items():
                if key not in ("items", "dishes", "products", "data") and isinstance(val, (dict, list)):
                    _extract_items(val)

    def _process_item(item: dict):
        name = norm_space(item.get("name") or item.get("title") or "")
        if not name:
            return

        price_raw = item.get("price")
        price_eur = None
        if isinstance(price_raw, dict):
            amount = price_raw.get("amount") or price_raw.get("value") or price_raw.get("price")
            price_eur = _cents_to_eur(amount)
        elif price_raw is not None:
            price_eur = _cents_to_eur(price_raw)

        if price_eur is None or price_eur <= 0:
            return

        image = ""
        img = item.get("image") or item.get("imageUrl") or item.get("image_url") or ""
        if isinstance(img, str):
            image = img
        elif isinstance(img, dict):
            image = img.get("url") or img.get("src") or ""

        unit_text = norm_space(
            item.get("unitText") or item.get("unit_text") or
            item.get("quantity") or item.get("size") or ""
        )
        ean = item.get("barcode_gtin") or item.get("ean") or item.get("gtin") or ""
        item_id = str(item.get("id") or item.get("_id") or "")

        products.append({
            "item_id": item_id,
            "name": name,
            "price_eur": price_eur,
            "unit_text": unit_text,
            "image": image,
            "ean": ean,
            "category": cat_name,
            "venue_id": venue_id,
        })

    _extract_items(data)

    seen_ids, seen_names, unique = set(), set(), []
    for p in products:
        if p["item_id"] and p["item_id"] in seen_ids:
            continue
        name_key = p["name"].lower()
        if name_key in seen_names:
            continue
        if p["item_id"]:
            seen_ids.add(p["item_id"])
        seen_names.add(name_key)
        unique.append(p)

    return unique


# ---------------------- DB ingest ---------------------- #
async def _ingest_to_db(products: List[Dict]) -> None:
    if not asyncpg:
        print("[db] asyncpg not available, skipping DB ingest.")
        return

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[db] DATABASE_URL not set → skipping DB ingest.")
        return

    env_store_id = int(os.environ.get("STORE_ID", "0") or "0")

    conn = await asyncpg.connect(db_url)
    try:
        venue_ids = sorted({p["venue_id"] for p in products if p["venue_id"]})
        store_map: Dict[str, int] = {}

        for v_id in venue_ids:
            if env_store_id > 0:
                store_map[v_id] = env_store_id
            else:
                row = await conn.fetchrow(
                    "SELECT id FROM stores WHERE chain = 'Coop' AND external_key = $1 LIMIT 1;",
                    v_id,
                )
                if row:
                    store_map[v_id] = row["id"]
                else:
                    print(f"[db] WARNING: no matching store for venue_id={v_id}")

        total = 0
        errors = 0
        for p in products:
            store_id = store_map.get(p["venue_id"])
            if not store_id:
                continue

            base_slug = slugify_for_ext(p["name"])[:40]
            size_slug = slugify_for_ext(p.get("unit_text") or "")[:20]
            if p["item_id"]:
                ext_id = f"bolt:{p['venue_id']}:{p['item_id']}"
            elif size_slug:
                ext_id = f"bolt:{p['venue_id']}:{base_slug}:{size_slug}"
            else:
                ext_id = f"bolt:{p['venue_id']}:{base_slug}"

            try:
                await conn.fetchval(
                    """
                    SELECT upsert_product_and_price(
                        $1::text, $2::text, $3::text, $4::text, $5::text,
                        $6::text, $7::numeric, $8::text, $9::integer,
                        $10::timestamptz, $11::text
                    );
                    """,
                    "coop",
                    ext_id,
                    p["name"],
                    "",
                    p.get("unit_text") or "",
                    p.get("ean") or "",
                    float(p["price_eur"]),
                    "EUR",
                    store_id,
                    datetime.datetime.now(datetime.timezone.utc),
                    "",
                )
                total += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"[db] upsert failed for {ext_id}: {e}")

        print(f"[db] upserted {total} rows via upsert_product_and_price() (errors: {errors})")
    finally:
        await conn.close()


# ---------------------- main crawl ---------------------- #
def crawl(
    venue_id: str,
    out_path: str,
    categories: Optional[List[Tuple[str, str]]] = None,
    delivery_lat: str = DELIVERY_LAT,
    delivery_lng: str = DELIVERY_LNG,
    req_delay: float = 0.5,
) -> List[Dict]:

    session_id = str(uuid.uuid4())
    device_id = str(uuid.uuid4()).replace("-", "")[:32]
    session = _make_session()

    # Fetch categories dynamically if not provided via file
    if not categories:
        print(f"[info] fetching categories dynamically for venue_id={venue_id}")
        categories = fetch_categories_from_api(
            session, venue_id, session_id, device_id, delivery_lat, delivery_lng
        )

    if not categories:
        print("[error] no categories found, aborting.")
        return []

    print(f"[info] venue_id={venue_id}  categories={len(categories)}")

    all_products: List[Dict] = []

    for idx, (cat_name, category_id) in enumerate(categories, 1):
        print(f"[cat] {idx}/{len(categories)} '{cat_name}' (smc={category_id})")

        data = fetch_category_products(
            session, venue_id, category_id,
            session_id, device_id,
            delivery_lat, delivery_lng,
        )

        products = parse_menu_dishes_response(data, cat_name, venue_id)
        print(f"  -> {len(products)} products")
        all_products.extend(products)

        time.sleep(req_delay)

    # Deduplicate across categories
    seen = set()
    unique_all: List[Dict] = []
    for p in all_products:
        key = p["item_id"] if p["item_id"] else p["name"].lower()
        if key not in seen:
            seen.add(key)
            unique_all.append(p)

    print(f"[done] {len(unique_all)} unique products across all categories")

    if unique_all:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        fieldnames = [
            "venue_id", "category", "item_id",
            "name", "price_eur", "unit_text", "ean", "image",
        ]
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
            w.writeheader()
            for p in unique_all:
                row = dict(p)
                row["price_eur"] = f"{p['price_eur']:.2f}"
                w.writerow(row)
        print(f"[csv] wrote {len(unique_all)} rows → {out_path}")
    else:
        print("[csv] no products to write")

    return unique_all


# ---------------------- CLI ---------------------- #
def main():
    ap = argparse.ArgumentParser("bolt food store crawler (direct API, dynamic categories)")
    ap.add_argument("--venue-id", required=True, help="Bolt venue/provider ID (e.g. 2281)")
    ap.add_argument("--delivery-lat", default=DELIVERY_LAT)
    ap.add_argument("--delivery-lng", default=DELIVERY_LNG)
    ap.add_argument("--out", required=True)
    ap.add_argument("--req-delay", type=float, default=0.5)
    ap.add_argument("--upsert-db", default="1")
    # Legacy / compat flags (ignored but kept so old YML doesn't break)
    ap.add_argument("--categories-file", default="")
    ap.add_argument("--categories-dir", default="")
    ap.add_argument("--city", default="")
    ap.add_argument("--store", default="")
    ap.add_argument("--headless", default="1")
    ap.add_argument("--deep", default="0")
    ap.add_argument("--ingest-mode", default="main")
    args = ap.parse_args()

    # Optional: still support categories file as override for testing
    categories = None
    if args.categories_file and os.path.isfile(args.categories_file):
        print(f"[info] using categories file override: {args.categories_file}")
        categories = parse_categories_file(args.categories_file)

    products = crawl(
        venue_id=args.venue_id,
        out_path=args.out,
        categories=categories,
        delivery_lat=args.delivery_lat,
        delivery_lng=args.delivery_lng,
        req_delay=args.req_delay,
    )

    if products and str(args.upsert_db) == "1":
        try:
            asyncio.run(_ingest_to_db(products))
        except Exception as e:
            print(f"[db] ingest error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

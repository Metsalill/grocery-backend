#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orphan image backfill -> Cloudflare R2

Fixes products with chain='coop' (or any chain) that have NO image_url and NO
source_url on the `products` row itself, but DO have a usable source_url on
one of their `prices` rows (added via cross-chain EAN matching in
upsert_product_and_price). We reuse that price-row source_url (preferring
prisma > rimi > selver) to fetch the product page and pull an image, exactly
like coop_image_backfill_r2.py does for coophaapsalu.ee, then upload to R2 and
update products.image_url. We do NOT touch products.chain or products.source_url.

Run:
  python scripts/orphan_image_backfill_r2.py [--limit 5000] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import random
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2
import requests

from settings import DATABASE_URL, R2_PREFIX, r2_public_url
from services.r2_client import upload_image_to_r2, image_exists_in_r2

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
})

MIN_IMAGE_BYTES = 2000
BAD_URL_PATTERNS = ["placeholder", "no-image", "missing", "default", "noimage"]

# Preference order: whichever source's page is most likely to have a clean,
# stable image URL first.
SOURCE_PRIORITY = ["prisma", "rimi", "selver"]


def jitter(a=0.3, b=0.8):
    time.sleep(random.uniform(a, b))


def is_valid_image_url(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    u = url.lower()
    return not any(p in u for p in BAD_URL_PATTERNS)


def extract_image_url(html: str) -> Optional[str]:
    """Same extraction strategy as coop_image_backfill_r2.py: og:image,
    woocommerce gallery, then JSON-LD. Works fine for Prisma/Rimi/Selver PDPs
    too since they all set og:image and/or JSON-LD Product.image."""

    m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html)
    if m and is_valid_image_url(m.group(1)):
        return m.group(1)

    m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html)
    if m and is_valid_image_url(m.group(1)):
        return m.group(1)

    for jm in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(jm.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                img = item.get("image")
                if isinstance(img, list):
                    img = img[0]
                if isinstance(img, str) and is_valid_image_url(img):
                    return img
        except Exception:
            continue

    for pattern in [
        r'<img[^>]+class=["\'][^"\']*wp-post-image[^"\']*["\'][^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]+class=["\'][^"\']*wp-post-image[^"\']*["\']',
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m and is_valid_image_url(m.group(1)):
            return m.group(1)

    return None


def download_image(url: str) -> tuple[Optional[bytes], Optional[str]]:
    try:
        r = SESSION.get(url, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return None, None
        ct = r.headers.get("content-type", "image/jpeg")
        if len(r.content) < MIN_IMAGE_BYTES:
            return None, None
        return r.content, ct
    except Exception as e:
        print(f"  [warn] download failed: {e}", file=sys.stderr)
        return None, None


def r2_key_from_url(url: str, pid: int, chain: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"[^a-zA-Z0-9-]", "", slug)[:60]
    return f"{R2_PREFIX}{chain}/{slug or pid}.webp"


def fetch_page(url: str) -> Optional[str]:
    try:
        r = SESSION.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.text
    except Exception as e:
        print(f"  [warn] page fetch failed: {e}", file=sys.stderr)
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    # Pull, per orphan product, the best available price-row source_url,
    # preferring prisma > rimi > selver.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (p.id)
                p.id, p.chain, pr.source, pr.source_url
            FROM products p
            JOIN prices pr ON pr.product_id = p.id
            WHERE (p.image_url IS NULL OR p.image_url = '')
              AND (p.source_url IS NULL OR p.source_url = '')
              AND pr.source IN ('prisma', 'rimi', 'selver')
              AND pr.source_url IS NOT NULL
              AND pr.source_url != ''
            ORDER BY p.id,
                CASE pr.source
                    WHEN 'prisma' THEN 1
                    WHEN 'rimi' THEN 2
                    WHEN 'selver' THEN 3
                END
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} orphan products with a usable price-row source_url")

    uploaded = 0
    skipped = 0
    failed = 0

    with conn.cursor() as cur:
        for i, (pid, chain, source, source_url) in enumerate(rows):
            html = fetch_page(source_url)
            if not html:
                print(f"[{pid}] page fetch failed: {source_url}")
                failed += 1
                jitter()
                continue

            image_url = extract_image_url(html)
            if not image_url:
                print(f"[{pid}] no image found on {source} page")
                skipped += 1
                jitter(0.2, 0.5)
                continue

            if args.dry_run:
                print(f"[DRY] [{pid}] ({source}) {image_url}")
                uploaded += 1
                jitter(0.2, 0.4)
                continue

            r2_key = r2_key_from_url(source_url, pid, chain)

            try:
                if image_exists_in_r2(r2_key):
                    public_url = r2_public_url(r2_key)
                    cur.execute("UPDATE products SET image_url = %s WHERE id = %s", (public_url, pid))
                    uploaded += 1
                    if uploaded % 50 == 0:
                        conn.commit()
                        print(f"  ... {uploaded} processed")
                    jitter(0.1, 0.3)
                    continue
            except Exception:
                pass

            data, content_type = download_image(image_url)
            if not data:
                print(f"[{pid}] download failed: {image_url}")
                failed += 1
                jitter(0.3, 0.6)
                continue

            try:
                public_url = upload_image_to_r2(data, r2_key, content_type or "image/jpeg")
                cur.execute("UPDATE products SET image_url = %s WHERE id = %s", (public_url, pid))
                uploaded += 1
                print(f"[{pid}] ✅ ({source}) {r2_key}")
                if uploaded % 50 == 0:
                    conn.commit()
                    print(f"  ... {uploaded} uploaded")
            except Exception as e:
                print(f"[{pid}] R2 upload failed: {e}", file=sys.stderr)
                failed += 1

            jitter(0.4, 0.9)

    if not args.dry_run:
        conn.commit()

    conn.close()
    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

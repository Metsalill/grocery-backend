#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi image backfill → Cloudflare R2

- Selects Rimi products with image_url pointing to Cloudinary (rimibaltic-res.cloudinary.com)
  OR with image_url missing but having a source_url with product ID
- Downloads image from Rimi Cloudinary CDN
- Uploads to R2 as products/rimi/{product_id}.webp
- Updates products.image_url with the R2 public URL

Run:
  python scripts/rimi_image_backfill_r2.py [--limit 1000] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
import random
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras
import requests

from settings import DATABASE_URL, R2_PREFIX, r2_public_url
from services.r2_client import upload_image_to_r2, image_exists_in_r2

PRODUCT_ID_RE = re.compile(r"/p/(\d+)")
CLOUDINARY_URLS = [
    "https://rimibaltic-res.cloudinary.com/image/upload/"
    "b_white,c_limit,f_auto,q_auto,w_350/"
    "d_ecommerce:backend-fallback.png/MAT_{product_id}_PCE_EE",
    "https://rimibaltic-res.cloudinary.com/image/upload/"
    "b_white,c_limit,f_auto,q_auto,w_350/"
    "d_ecommerce:backend-fallback.png/MAT_{product_id}_KGH_EE",
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.rimi.ee/",
})


def jitter(a=0.3, b=0.8):
    time.sleep(random.uniform(a, b))


def get_product_id(source_url: str) -> Optional[str]:
    m = PRODUCT_ID_RE.search(source_url or "")
    return m.group(1) if m else None


def build_cloudinary_urls(product_id: str) -> list[str]:
    return [url.format(product_id=product_id) for url in CLOUDINARY_URLS]


def download_image(url: str, timeout: int = 15) -> tuple[Optional[bytes], Optional[str]]:
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None, None
        ct = r.headers.get("content-type", "image/webp")
        # Skip Cloudinary fallback placeholder (very small file ~1KB)
        if len(r.content) < 2000:
            return None, None
        return r.content, ct
    except Exception as e:
        print(f"  [warn] download failed: {e}", file=sys.stderr)
        return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    # Select Rimi products that either:
    # 1. Have Cloudinary URL (old format we built) - need to re-upload to R2
    # 2. Have no image_url but have source_url with product ID
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, source_url, image_url
            FROM products
            WHERE (chain = 'rimi' OR source_url ILIKE '%%rimi.ee%%')
              AND source_url ~ '/p/\d+'
              AND (
                image_url IS NULL
                OR image_url = ''
                OR image_url ILIKE '%%cloudinary%%'
              )
            ORDER BY id ASC
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} Rimi products to process")

    uploaded = 0
    skipped = 0
    failed = 0

    with conn.cursor() as cur:
        for row in rows:
            pid = row["id"]
            source_url = row["source_url"] or ""
            product_id = get_product_id(source_url)

            if not product_id:
                skipped += 1
                continue

            r2_key = f"{R2_PREFIX}rimi/{product_id}.webp"

            # Check if already in R2
            if not args.dry_run:
                try:
                    if image_exists_in_r2(r2_key):
                        public_url = r2_public_url(r2_key)
                        cur.execute(
                            "UPDATE products SET image_url = %s WHERE id = %s",
                            (public_url, pid)
                        )
                        uploaded += 1
                        if uploaded % 100 == 0:
                            conn.commit()
                            print(f"  ... {uploaded} processed")
                        continue
                except Exception:
                    pass

            # Download from Cloudinary - try both suffixes
            if args.dry_run:
                print(f"[DRY] [{pid}] product_id={product_id} → {r2_key}")
                uploaded += 1
                continue

            data = None
            content_type = None
            for cloudinary_url in build_cloudinary_urls(product_id):
                data, content_type = download_image(cloudinary_url)
                if data:
                    break
            if not data:
                print(f"[{pid}] no image at {cloudinary_url}")
                failed += 1
                jitter(0.1, 0.3)
                continue

            # Upload to R2
            try:
                public_url = upload_image_to_r2(
                    data, r2_key, content_type or "image/webp"
                )
                cur.execute(
                    "UPDATE products SET image_url = %s WHERE id = %s",
                    (public_url, pid)
                )
                uploaded += 1
                print(f"[{pid}] ✅ {r2_key}")
                if uploaded % 100 == 0:
                    conn.commit()
                    print(f"  ... {uploaded} uploaded")
            except Exception as e:
                print(f"[{pid}] R2 upload failed: {e}", file=sys.stderr)
                failed += 1

            jitter(0.2, 0.6)

    if not args.dry_run:
        conn.commit()

    conn.close()
    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver image backfill → Cloudflare R2

- Selects Selver products with image_url pointing to selver.ee CDN
  OR with image_url missing but EAN known
- Downloads image from Selver CDN
- Uploads to R2 as products/selver/{ean}.webp
- Updates products.image_url with the R2 public URL

Run:
  python scripts/selver_image_backfill_r2.py [--limit 5000] [--dry-run]
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

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras
import requests

from settings import DATABASE_URL, R2_PREFIX, r2_public_url
from services.r2_client import upload_image_to_r2, image_exists_in_r2

SELVER_CDN = "https://www.selver.ee/img/310/300/resize"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.selver.ee/",
})


def jitter(a=0.2, b=0.6):
    time.sleep(random.uniform(a, b))


def build_selver_url(ean: str) -> Optional[str]:
    ean = ean.strip()
    if len(ean) < 2:
        return None
    return f"{SELVER_CDN}/{ean[0]}/{ean[1]}/{ean}.jpg"


def download_image(url: str, timeout: int = 15) -> tuple[Optional[bytes], Optional[str]]:
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None, None
        ct = r.headers.get("content-type", "image/jpeg")
        # Skip placeholder images (very small)
        if len(r.content) < 2000:
            return None, None
        return r.content, ct
    except Exception as e:
        print(f"  [warn] download failed: {e}", file=sys.stderr)
        return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, ean, image_url
            FROM products
            WHERE (chain = 'selver' OR source_url ILIKE '%%selver%%')
              AND ean IS NOT NULL
              AND ean != ''
              AND char_length(ean) >= 8
              AND (
                image_url IS NULL
                OR image_url = ''
                OR image_url ILIKE '%%selver.ee%%'
              )
            ORDER BY id ASC
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} Selver products to process")

    uploaded = 0
    skipped = 0
    failed = 0

    with conn.cursor() as cur:
        for row in rows:
            pid = row["id"]
            ean = (row["ean"] or "").strip()

            if len(ean) < 2:
                skipped += 1
                continue

            r2_key = f"{R2_PREFIX}selver/{ean}.jpg"

            # Already in R2?
            if not args.dry_run:
                try:
                    if image_exists_in_r2(r2_key):
                        public_url = r2_public_url(r2_key)
                        cur.execute(
                            "UPDATE products SET image_url = %s WHERE id = %s",
                            (public_url, pid)
                        )
                        uploaded += 1
                        if uploaded % 200 == 0:
                            conn.commit()
                            print(f"  ... {uploaded} processed")
                        continue
                except Exception:
                    pass

            selver_url = build_selver_url(ean)
            if not selver_url:
                skipped += 1
                continue

            if args.dry_run:
                print(f"[DRY] [{pid}] EAN={ean} → {r2_key}")
                uploaded += 1
                continue

            data, content_type = download_image(selver_url)
            if not data:
                print(f"[{pid}] no image for EAN={ean}")
                failed += 1
                jitter(0.05, 0.15)
                continue

            try:
                public_url = upload_image_to_r2(
                    data, r2_key, content_type or "image/jpeg"
                )
                cur.execute(
                    "UPDATE products SET image_url = %s WHERE id = %s",
                    (public_url, pid)
                )
                uploaded += 1
                print(f"[{pid}] ✅ EAN={ean} → {r2_key}")
                if uploaded % 200 == 0:
                    conn.commit()
                    print(f"  ... {uploaded} uploaded")
            except Exception as e:
                print(f"[{pid}] R2 upload failed: {e}", file=sys.stderr)
                failed += 1

            jitter(0.1, 0.3)

    if not args.dry_run:
        conn.commit()

    conn.close()
    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

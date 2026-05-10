#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi PDP image backfill — kraabib pildi URL-i otse Rimi toote lehelt.

Run: python scripts/rimi_image_backfill_pdp.py [--limit 200] [--dry-run]
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
import requests
from bs4 import BeautifulSoup

from settings import DATABASE_URL, R2_PREFIX, r2_public_url
from services.r2_client import upload_image_to_r2, image_exists_in_r2

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "et-EE,et;q=0.9",
    "Referer": "https://www.rimi.ee/",
})

CLOUDINARY_BASE = (
    "https://rimibaltic-res.cloudinary.com/image/upload/"
    "b_white,c_limit,f_auto,q_auto,w_350/"
    "d_ecommerce:backend-fallback.png/"
)


def jitter(a=0.5, b=1.5):
    time.sleep(random.uniform(a, b))


def extract_image_url_from_rimi_page(source_url: str) -> Optional[str]:
    try:
        r = SESSION.get(source_url, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if "rimibaltic-res.cloudinary.com" in src and "MAT_" in src:
                m = re.search(r"MAT_(\d+)_(\w+)", src)
                if m:
                    return f"{CLOUDINARY_BASE}MAT_{m.group(1)}_{m.group(2)}"
        for tag in soup.find_all(attrs={"data-src": True}):
            src = tag.get("data-src", "")
            if "MAT_" in src:
                m = re.search(r"MAT_(\d+)_(\w+)", src)
                if m:
                    return f"{CLOUDINARY_BASE}MAT_{m.group(1)}_{m.group(2)}"
    except Exception as e:
        print(f"  [warn] page fetch failed: {e}", file=sys.stderr)
    return None


def download_image(url: str) -> tuple[Optional[bytes], Optional[str]]:
    try:
        r = SESSION.get(url, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return None, None
        ct = r.headers.get("content-type", "image/webp")
        if len(r.content) < 2000:
            return None, None
        return r.content, ct
    except Exception as e:
        print(f"  [warn] download failed: {e}", file=sys.stderr)
        return None, None


def get_product_id_from_url(source_url: str) -> Optional[str]:
    m = re.search(r"/p/(\d+)", source_url or "")
    return m.group(1) if m else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, name, source_url
            FROM products
            WHERE chain = 'rimi'
              AND source_url IS NOT NULL
              AND source_url ILIKE '%%rimi.ee%%'
              AND (image_url IS NULL OR image_url = '')
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
            pid, name, source_url = row[0], row[1], row[2]

            print(f"[{pid}] {(name or '')[:50]}")

            cloudinary_url = extract_image_url_from_rimi_page(source_url)
            if not cloudinary_url:
                print(f"  no image found on page")
                skipped += 1
                jitter(0.3, 0.8)
                continue

            if args.dry_run:
                print(f"  [DRY] {cloudinary_url}")
                uploaded += 1
                continue

            product_id = get_product_id_from_url(source_url)
            if not product_id:
                skipped += 1
                continue

            r2_key = f"{R2_PREFIX}rimi/{product_id}.webp"

            try:
                if image_exists_in_r2(r2_key):
                    public_url = r2_public_url(r2_key)
                    cur.execute("UPDATE products SET image_url = %s WHERE id = %s", (public_url, pid))
                    uploaded += 1
                    if uploaded % 20 == 0:
                        conn.commit()
                    jitter(0.1, 0.3)
                    continue
            except Exception:
                pass

            data, content_type = download_image(cloudinary_url)
            if not data:
                print(f"  download failed")
                failed += 1
                jitter(0.3, 0.8)
                continue

            try:
                public_url = upload_image_to_r2(data, r2_key, content_type or "image/webp")
                cur.execute("UPDATE products SET image_url = %s WHERE id = %s", (public_url, pid))
                uploaded += 1
                print(f"  ✅ {r2_key}")
                if uploaded % 20 == 0:
                    conn.commit()
            except Exception as e:
                print(f"  R2 upload failed: {e}", file=sys.stderr)
                failed += 1

            jitter(0.5, 1.2)

    if not args.dry_run:
        conn.commit()

    conn.close()
    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

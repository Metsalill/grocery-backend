#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi image URL backfiller — no scraping needed!

Rimi Cloudinary URL pattern:
  https://rimibaltic-res.cloudinary.com/image/upload/b_white,c_limit,f_auto,q_auto,w_350/d_ecommerce:backend-fallback.png/MAT_{product_id}_KGH_EE

Product ID comes from source_url: https://www.rimi.ee/epood/ee/tooted/p/274323
                                                                              ^^^^^^

Run:
  python scripts/rimi_image_backfill.py [--dry-run] [--limit 10000]
"""
from __future__ import annotations

import argparse
import os
import re
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]

CDN_TEMPLATE = (
    "https://rimibaltic-res.cloudinary.com/image/upload/"
    "b_white,c_limit,f_auto,q_auto,w_350/"
    "d_ecommerce:backend-fallback.png/MAT_{product_id}_KGH_EE"
)

PRODUCT_ID_RE = re.compile(r"/p/(\d+)")


def build_rimi_image_url(source_url: str) -> str | None:
    m = PRODUCT_ID_RE.search(source_url)
    if not m:
        return None
    return CDN_TEMPLATE.format(product_id=m.group(1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=10000)
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, source_url, name
            FROM products
            WHERE (image_url IS NULL OR image_url = '')
              AND source_url IS NOT NULL
              AND source_url ILIKE '%%rimi.ee%%'
              AND source_url ~ '/p/\d+'
            ORDER BY id ASC
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} Rimi products missing image_url")

    updated = 0
    skipped = 0

    with conn.cursor() as cur:
        for row in rows:
            pid = row["id"]
            source_url = row["source_url"]

            url = build_rimi_image_url(source_url)
            if not url:
                skipped += 1
                continue

            if args.dry_run:
                print(f"[DRY] [{pid}] {row['name'][:40]} → {url}")
                updated += 1
                continue

            cur.execute(
                "UPDATE products SET image_url = %s WHERE id = %s",
                (url, pid)
            )
            updated += 1
            if updated % 500 == 0:
                conn.commit()
                print(f"  ... {updated} updated so far")

    if not args.dry_run:
        conn.commit()

    conn.close()
    print(f"\nDone. Updated: {updated}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

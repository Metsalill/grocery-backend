#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver image URL backfiller — no scraping needed!

Selver CDN URL pattern:
  https://www.selver.ee/img/310/300/resize/{ean[0]}/{ean[1]}/{ean}.jpg

We just need the EAN — no Playwright, no R2, just a DB UPDATE.

Run:
  python scripts/selver_image_backfill.py [--dry-run] [--limit 5000]
"""
from __future__ import annotations

import argparse
import os
import sys
import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]

CDN_BASE = "https://www.selver.ee/img/310/300/resize"


def build_selver_image_url(ean: str) -> str:
    ean = ean.strip()
    return f"{CDN_BASE}/{ean[0]}/{ean[1]}/{ean}.jpg"


def url_exists(url: str, timeout: int = 5) -> bool:
    """HEAD request to check if image actually exists on Selver CDN."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Don't update DB")
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--check-url", action="store_true",
                    help="Verify image exists on CDN before updating (slower)")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, ean, name
            FROM products
            WHERE (image_url IS NULL OR image_url = '')
              AND ean IS NOT NULL
              AND ean != ''
              AND char_length(ean) >= 8
              AND (chain = 'selver'
                   OR source_url ILIKE '%%selver%%')
            ORDER BY id ASC
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} Selver products missing image_url")

    updated = 0
    skipped = 0

    with conn.cursor() as cur:
        for row in rows:
            pid = row["id"]
            ean = row["ean"].strip()
            name = row["name"]

            if len(ean) < 2:
                skipped += 1
                continue

            url = build_selver_image_url(ean)

            if args.check_url:
                if not url_exists(url):
                    print(f"[{pid}] no image on CDN: {url}")
                    skipped += 1
                    continue

            if args.dry_run:
                print(f"[DRY] [{pid}] {name[:40]} → {url}")
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

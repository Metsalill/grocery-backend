#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop (coophaapsalu.ee) image backfill → Cloudflare R2

Run:
  python scripts/coop_image_backfill_r2.py [--limit 5000] [--dry-run]
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
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from settings import DATABASE_URL, R2_PREFIX, r2_public_url
from services.r2_client import upload_image_to_r2, image_exists_in_r2

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://coophaapsalu.ee/",
})

MIN_IMAGE_BYTES = 2000
BAD_URL_PATTERNS = ["placeholder", "no-image", "missing", "default", "noimage"]


def jitter(a=0.5, b=1.2):
    time.sleep(random.uniform(a, b))


def is_valid_image_url(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    url_lower = url.lower()
    return not any(p in url_lower for p in BAD_URL_PATTERNS)


def extract_image_url(html: str) -> Optional[str]:
    """Parsi pildi URL coophaapsalu lehelt."""

    # 1) og:image
    m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html)
    if m and is_valid_image_url(m.group(1)):
        return m.group(1)

    m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html)
    if m and is_valid_image_url(m.group(1)):
        return m.group(1)

    # 2) woocommerce product image
    for pattern in [
        r'<img[^>]+class=["\'][^"\']*wp-post-image[^"\']*["\'][^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]+class=["\'][^"\']*wp-post-image[^"\']*["\']',
        r'class=["\']woocommerce-product-gallery[^"\']*["\'][^>]*>.*?<img[^>]+src=["\']([^"\']+)["\']',
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m and is_valid_image_url(m.group(1)):
            return m.group(1)

    # 3) JSON-LD
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


def r2_key_from_source_url(source_url: str, pid: int) -> str:
    slug = source_url.rstrip("/").split("/")[-1]
    slug = re.sub(r"[^a-zA-Z0-9-]", "", slug)[:60]
    return f"{R2_PREFIX}coop/{slug or pid}.webp"


def new_browser(pw):
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(locale="et-EE", timezone_id="Europe/Tallinn")
    page = ctx.new_page()
    return browser, ctx, page


def new_db_connection():
    """Opens a fresh DB connection with autocommit enabled.

    autocommit=True is deliberate and important here: each product's
    UPDATE is an independent, single-row write with a slow Playwright/
    requests/R2 network call in between (page load, image download,
    R2 upload — each can take many seconds, and a "no image found" skip
    does no DB write at all before moving to the next product). With
    autocommit=False and only a periodic commit() every 50 uploads, the
    connection could sit "idle in transaction" across dozens of slow
    network calls in a row. Since Railway's Postgres now has
    idle_in_transaction_session_timeout='60s' (added to prevent zombie
    scraper transactions from locking tables for hours), that idle
    transaction gets killed mid-run, the cursor becomes unusable, and
    every subsequent write fails with "cursor already closed" — which
    is exactly what happened here. With autocommit=True, every write
    commits immediately, so there is never an open transaction sitting
    idle during the slow parts, regardless of how long a single
    product's Playwright/R2 work takes.
    """
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = new_db_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, source_url
            FROM products
            WHERE source_url ILIKE '%%coophaapsalu.ee%%'
              AND (image_url IS NULL OR image_url = '')
            ORDER BY id ASC
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} Coop products to process")

    uploaded = 0
    skipped = 0
    failed = 0

    def run_update(pid: int, public_url: str) -> bool:
        """Runs the per-product UPDATE, transparently reopening the DB
        connection once if it was dropped for any reason (idle timeout,
        network blip, server restart) instead of aborting the whole
        multi-hour run over a single lost connection. Returns True on
        success, False if the retry also failed."""
        nonlocal conn
        for attempt in (1, 2):
            try:
                with conn.cursor() as cur2:
                    cur2.execute("UPDATE products SET image_url = %s WHERE id = %s", (public_url, pid))
                return True
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                print(f"[{pid}] DB connection issue ({e}); reconnecting (attempt {attempt})", file=sys.stderr)
                try:
                    conn.close()
                except Exception:
                    pass
                conn = new_db_connection()
        return False

    with sync_playwright() as pw:
        browser, ctx, page = new_browser(pw)

        for i, row in enumerate(rows):
            pid, source_url = row[0], row[1]

            try:
                page.goto(source_url, timeout=25000, wait_until="domcontentloaded")
                try:
                    page.wait_for_selector("img.wp-post-image", timeout=4000)
                except PWTimeout:
                    pass
                html = page.content()
                image_url = extract_image_url(html)
            except Exception as e:
                print(f"[{pid}] page load failed: {e}", file=sys.stderr)
                failed += 1
                try:
                    page.close(); ctx.close(); browser.close()
                except Exception:
                    pass
                browser, ctx, page = new_browser(pw)
                continue

            if not image_url:
                print(f"[{pid}] no image found")
                skipped += 1
                jitter(0.2, 0.5)
                continue

            if args.dry_run:
                print(f"[DRY] [{pid}] {image_url}")
                uploaded += 1
                continue

            r2_key = r2_key_from_source_url(source_url, pid)

            try:
                if image_exists_in_r2(r2_key):
                    public_url = r2_public_url(r2_key)
                    if run_update(pid, public_url):
                        uploaded += 1
                        if uploaded % 50 == 0:
                            print(f"  ... {uploaded} processed")
                    else:
                        print(f"[{pid}] DB update failed after retry (image already in R2)", file=sys.stderr)
                        failed += 1
                    jitter(0.1, 0.3)
                    continue
            except Exception:
                pass

            data, content_type = download_image(image_url)
            if not data:
                print(f"[{pid}] download failed: {image_url}")
                failed += 1
                jitter(0.3, 0.8)
                continue

            try:
                public_url = upload_image_to_r2(data, r2_key, content_type or "image/jpeg")
            except Exception as e:
                print(f"[{pid}] R2 upload failed: {e}", file=sys.stderr)
                failed += 1
                jitter(0.5, 1.0)
                continue

            if run_update(pid, public_url):
                uploaded += 1
                print(f"[{pid}] ✅ {r2_key}")
                if uploaded % 50 == 0:
                    print(f"  ... {uploaded} uploaded")
            else:
                print(f"[{pid}] DB update failed after retry (image uploaded to R2 as {r2_key})", file=sys.stderr)
                failed += 1

            jitter(0.5, 1.0)

            if (i + 1) % 200 == 0:
                try:
                    page.close(); ctx.close(); browser.close()
                except Exception:
                    pass
                browser, ctx, page = new_browser(pw)
                print(f"[info] browser restarted after {i+1} products")

        try:
            page.close(); ctx.close(); browser.close()
        except Exception:
            pass

    conn.close()
    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

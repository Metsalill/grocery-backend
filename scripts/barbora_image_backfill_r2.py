#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora image backfill → Cloudflare R2

- Selects Barbora products with missing image_url but having source_url
- Fetches the product page (Playwright for JS rendering)
- Extracts image URL from JSON-LD or img[itemprop="image"]
- Downloads image from cdn.barbora.ee
- Uploads to R2 as products/barbora/{uuid}.webp
- Updates products.image_url with the R2 public URL

Run:
  python scripts/barbora_image_backfill_r2.py [--limit 5000] [--dry-run]
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
import psycopg2.extras
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
    "Referer": "https://barbora.ee/",
})


def jitter(a=0.5, b=1.5):
    time.sleep(random.uniform(a, b))


def extract_image_url_from_html(html: str) -> Optional[str]:
    """Parsi pildi URL JSON-LD-st või img[itemprop=image]-st."""

    # 1) JSON-LD
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(m.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                img = item.get("image")
                if isinstance(img, list):
                    img = img[0]
                if isinstance(img, str) and img.startswith("http"):
                    return img
        except Exception:
            continue

    # 2) img[itemprop="image"]
    m = re.search(r'<img[^>]+itemprop=["\']image["\'][^>]+src=["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)

    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\'][^>]+itemprop=["\']image["\']', html)
    if m:
        return m.group(1)

    # 3) cdn.barbora.ee/products/ URL
    m = re.search(r'(https://cdn\.barbora\.ee/products/[^"\'>\s]+\.(?:png|jpg|jpeg|webp))', html)
    if m:
        return m.group(1)

    # 4) data-b-item-id JSON blob
    m = re.search(r'"image"\s*:\s*"(https://cdn\.barbora\.ee/[^"]+)"', html)
    if m:
        return m.group(1)

    return None


def download_image(url: str, timeout: int = 15) -> tuple[Optional[bytes], Optional[str]]:
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None, None
        ct = r.headers.get("content-type", "image/png")
        if len(r.content) < 1000:
            return None, None
        return r.content, ct
    except Exception as e:
        print(f"  [warn] download failed: {e}", file=sys.stderr)
        return None, None


def r2_key_from_image_url(image_url: str) -> str:
    """Võta UUID osa cdn.barbora.ee URL-ist."""
    # https://cdn.barbora.ee/products/2653480e-27b4-4ed0-b46b-12dce558c8e4_m.png
    m = re.search(r'/products/([^/]+?)(?:_[sml])?\.(?:png|jpg|jpeg|webp)', image_url)
    if m:
        return f"{R2_PREFIX}barbora/{m.group(1)}.webp"
    # fallback: hash URL
    import hashlib
    h = hashlib.md5(image_url.encode()).hexdigest()
    return f"{R2_PREFIX}barbora/{h}.webp"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, source_url
            FROM products
            WHERE chain = 'barbora'
              AND source_url IS NOT NULL
              AND source_url != ''
              AND (image_url IS NULL OR image_url = '')
            ORDER BY id ASC
            LIMIT %s
        """, (args.limit,))
        rows = cur.fetchall()

    print(f"Found {len(rows)} Barbora products to process")

    uploaded = 0
    skipped = 0
    failed = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="et-EE",
            timezone_id="Europe/Tallinn",
        )
        page = ctx.new_page()

        # Age gate + cookies
        page.add_init_script("""
            try {
                localStorage.setItem('ageConfirmed', 'true');
                localStorage.setItem('adult', 'true');
                document.cookie = 'ageConfirmed=true; path=/; max-age=31536000';
            } catch(e) {}
        """)

        with conn.cursor() as cur:
            for i, row in enumerate(rows):
                pid = row["id"]
                source_url = row["source_url"]

                try:
                    page.goto(source_url, timeout=30000, wait_until="domcontentloaded")

                    # Tühjenda age gate
                    for sel in [
                        "button:has-text('Olen 18-aastane')",
                        "button:has-text('Olen 18')",
                    ]:
                        try:
                            loc = page.locator(sel)
                            if loc.count() and loc.first.is_visible():
                                loc.first.click(timeout=1000)
                        except Exception:
                            pass

                    try:
                        page.wait_for_selector("img[itemprop='image']", timeout=5000)
                    except PWTimeout:
                        pass

                    html = page.content()
                    image_url = extract_image_url_from_html(html)

                except Exception as e:
                    print(f"[{pid}] page load failed: {e}", file=sys.stderr)
                    failed += 1
                    continue

                if not image_url:
                    print(f"[{pid}] no image found at {source_url}")
                    skipped += 1
                    continue

                if args.dry_run:
                    print(f"[DRY] [{pid}] image_url={image_url}")
                    uploaded += 1
                    continue

                r2_key = r2_key_from_image_url(image_url)

                # Kontrolli kas juba R2-s
                try:
                    if image_exists_in_r2(r2_key):
                        public_url = r2_public_url(r2_key)
                        cur.execute(
                            "UPDATE products SET image_url = %s WHERE id = %s",
                            (public_url, pid)
                        )
                        uploaded += 1
                        if uploaded % 50 == 0:
                            conn.commit()
                            print(f"  ... {uploaded} processed")
                        jitter(0.1, 0.3)
                        continue
                except Exception:
                    pass

                # Lae pilt alla
                data, content_type = download_image(image_url)
                if not data:
                    print(f"[{pid}] download failed: {image_url}")
                    failed += 1
                    jitter(0.3, 0.8)
                    continue

                # Lae R2-sse
                try:
                    public_url = upload_image_to_r2(data, r2_key, content_type or "image/png")
                    cur.execute(
                        "UPDATE products SET image_url = %s WHERE id = %s",
                        (public_url, pid)
                    )
                    uploaded += 1
                    print(f"[{pid}] ✅ {r2_key}")
                    if uploaded % 50 == 0:
                        conn.commit()
                        print(f"  ... {uploaded} uploaded")
                except Exception as e:
                    print(f"[{pid}] R2 upload failed: {e}", file=sys.stderr)
                    failed += 1

                jitter(0.5, 1.2)

                # Restardi brauser iga 200 toote järel
                if (i + 1) % 200 == 0:
                    try:
                        page.close()
                        ctx.close()
                        browser.close()
                    except Exception:
                        pass
                    browser = pw.chromium.launch(headless=True)
                    ctx = browser.new_context(locale="et-EE", timezone_id="Europe/Tallinn")
                    page = ctx.new_page()
                    page.add_init_script("""
                        try {
                            localStorage.setItem('ageConfirmed', 'true');
                            document.cookie = 'ageConfirmed=true; path=/; max-age=31536000';
                        } catch(e) {}
                    """)
                    print(f"[info] browser restarted after {i+1} products")

        try:
            page.close()
            ctx.close()
            browser.close()
        except Exception:
            pass

    if not args.dry_run:
        conn.commit()

    conn.close()
    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()

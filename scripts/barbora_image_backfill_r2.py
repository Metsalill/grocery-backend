#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora image backfill → Cloudflare R2

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


def is_valid_barbora_image(url: str) -> bool:
    """Ainult cdn.barbora.ee pildid on päris pildid."""
    return (
        url
        and url.startswith("http")
        and "cdn.barbora.ee" in url
        and "placeholder" not in url.lower()
        and "no-image" not in url.lower()
    )


def extract_image_url_from_html(html: str) -> Optional[str]:
    """Parsi pildi URL — ainult cdn.barbora.ee URL-id."""

    # 1) JSON-LD — kõige usaldusväärsem
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(m.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                img = item.get("image")
                if isinstance(img, list):
                    img = img[0]
                if isinstance(img, str) and is_valid_barbora_image(img):
                    return img
        except Exception:
            continue

    # 2) data-b-item JSON blob (Barbora lisab tooteinfot data atribuutidesse)
    m = re.search(r'"image"\s*:\s*"(https://cdn\.barbora\.ee/[^"]+)"', html)
    if m and is_valid_barbora_image(m.group(1)):
        return m.group(1)

    # 3) cdn.barbora.ee/products/ URL otse HTML-ist
    for m in re.finditer(
        r'(https://cdn\.barbora\.ee/products/[^"\'>\s]+\.(?:png|jpg|jpeg|webp))',
        html
    ):
        url = m.group(1)
        if is_valid_barbora_image(url):
            return url

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
    m = re.search(r'/products/([^/]+?)(?:_[sml])?\.(?:png|jpg|jpeg|webp)', image_url)
    if m:
        return f"{R2_PREFIX}barbora/{m.group(1)}.webp"
    import hashlib
    h = hashlib.md5(image_url.encode()).hexdigest()
    return f"{R2_PREFIX}barbora/{h}.webp"


def new_browser(pw):
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(locale="et-EE", timezone_id="Europe/Tallinn")
    page = ctx.new_page()
    page.add_init_script("""
        try {
            localStorage.setItem('ageConfirmed', 'true');
            localStorage.setItem('adult', 'true');
            document.cookie = 'ageConfirmed=true; path=/; max-age=31536000';
        } catch(e) {}
    """)
    return browser, ctx, page


def new_db_connection():
    """Opens a fresh DB connection with autocommit enabled.

    autocommit=True is deliberate: each product's UPDATE is an
    independent, single-row write with a slow Playwright/requests/R2
    network call in between (page load, age-gate click, image
    download, R2 upload — each can take several seconds, and a "no
    image found" skip does no DB write at all before moving on). With
    autocommit=False and only a periodic commit() every 50 uploads, the
    connection could sit "idle in transaction" across many slow
    network calls in a row. Railway's Postgres has
    idle_in_transaction_session_timeout='60s' (added to prevent zombie
    scraper transactions from locking tables for hours), so that idle
    transaction gets killed mid-run, the cursor becomes unusable, and
    every subsequent write fails with "cursor already closed" — which
    is exactly the failure pattern in this run's log (scattered
    failures building up, then total collapse at the final commit()).
    With autocommit=True, every write commits immediately, so there is
    never an open transaction sitting idle regardless of how long a
    single product's Playwright/R2 work takes.
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
            pid = row["id"]
            source_url = row["source_url"]

            try:
                page.goto(source_url, timeout=30000, wait_until="domcontentloaded")

                # Age gate
                for sel in ["button:has-text('Olen 18-aastane')", "button:has-text('Olen 18')"]:
                    try:
                        loc = page.locator(sel)
                        if loc.count() and loc.first.is_visible():
                            loc.first.click(timeout=1000)
                    except Exception:
                        pass

                # Oota kuni pilt laetud
                try:
                    page.wait_for_selector("script[type='application/ld+json']", timeout=6000)
                except PWTimeout:
                    pass

                html = page.content()
                image_url = extract_image_url_from_html(html)

            except Exception as e:
                print(f"[{pid}] page load failed: {e}", file=sys.stderr)
                failed += 1
                # Restardi brauser
                try:
                    page.close(); ctx.close(); browser.close()
                except Exception:
                    pass
                browser, ctx, page = new_browser(pw)
                continue

            if not image_url:
                print(f"[{pid}] no cdn.barbora.ee image found")
                skipped += 1
                jitter(0.2, 0.5)
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
                public_url = upload_image_to_r2(data, r2_key, content_type or "image/png")
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

            # Restardi brauser iga 200 toote järel
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

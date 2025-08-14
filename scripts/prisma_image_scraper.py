#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee product image backfiller → Cloudflare R2

- Selects products with empty image_url AND source_url on prismamarket.ee
- Extracts the main product image from the product page
- Uploads to R2 as <R2_PREFIX>prisma/<ean or id>.<ext>
- Updates products.image_url with the public R2 URL
- If a 'note' column exists and no image is found, sets: 'Kontrolli visuaali!'

Run:
  pip install playwright psycopg2-binary
  python -m playwright install chromium
  python scripts/prisma_image_scraper.py --limit 500 --headless 1
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
import random
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure repo root is importable so `settings.py` resolves even when CWD differs
# (e.g. in GitHub Actions). This assumes the script lives in `<root>/scripts/`.
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from settings import DATABASE_URL, DB_CONNECT_TIMEOUT, R2_PREFIX, r2_public_url
from services.r2_client import upload_image_to_r2

BASE = "https://prismamarket.ee"

# -----------------------------------------------------------------------------
# Small utils
def jitter(a: float = 0.6, b: float = 1.4) -> None:
    time.sleep(random.uniform(a, b))

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def guess_ext_from_mime(m: str) -> str:
    m = (m or "").lower()
    if "jpeg" in m or m == "image/jpg":
        return "jpg"
    if "png" in m:
        return "png"
    if "webp" in m:
        return "webp"
    if "gif" in m:
        return "gif"
    return "jpg"  # safe default

def is_prisma_url(u: str) -> bool:
    try:
        return "prismamarket.ee" in urlparse(u).netloc
    except Exception:
        return False

# -----------------------------------------------------------------------------
# DB
def db_connect() -> PGConn:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=int(DB_CONNECT_TIMEOUT))
    conn.autocommit = True
    return conn

def table_has_note_column(conn: PGConn) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'products' AND column_name = 'note'
            LIMIT 1
        """)
        return cur.fetchone() is not None

def select_missing_images(conn: PGConn, limit: int) -> list[dict]:
    """
    Pick Prisma rows where image_url is null/empty and we have a source_url.
    """
    sql = """
        SELECT id, ean, product_name, source_url
        FROM products
        WHERE (image_url IS NULL OR image_url = '')
          AND source_url IS NOT NULL
          AND source_url <> ''
          AND POSITION('prismamarket.ee' IN source_url) > 0
        ORDER BY id ASC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (limit,))
        return [dict(r) for r in cur.fetchall()]

def update_image_url(conn: PGConn, pid: int, url: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE products SET image_url = %s WHERE id = %s", (url, pid))

def set_note(conn: PGConn, pid: int, msg: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE products SET note = %s WHERE id = %s", (msg, pid))

# -----------------------------------------------------------------------------
# Scraping helpers
IMG_SELECTORS = [
    "main img[alt][src]",          # primary product image in main area
    "img[alt][src]",               # any alt+src image
    "img.product-image[src]",      # common class patterns
    "img[data-src]",               # lazy images
]

def accept_cookies(page) -> None:
    for sel in [
        "button:has-text('Accept all')",
        "button:has-text('Accept cookies')",
        "button:has-text('Nõustu')",
        "button[aria-label*='accept' i]",
    ]:
        try:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_enabled():
                btn.first.click()
                page.wait_for_load_state("domcontentloaded")
                jitter(0.2, 0.6)
                return
        except Exception:
            pass

def extract_image_src(page) -> Optional[str]:
    # Try explicit img selectors
    for sel in IMG_SELECTORS:
        try:
            img = page.locator(sel).first
            if img.count() > 0:
                src = img.get_attribute("src") or img.get_attribute("data-src")
                if src and not src.startswith("data:"):
                    return src
        except Exception:
            continue

    # Fallback to OpenGraph / link rel
    try:
        og = page.locator("meta[property='og:image']").first
        if og.count() > 0:
            content = og.get_attribute("content")
            if content:
                return content
    except Exception:
        pass
    try:
        link = page.locator("link[rel='image_src']").first
        if link.count() > 0:
            href = link.get_attribute("href")
            if href:
                return href
    except Exception:
        pass

    return None

def fetch_image_bytes(context, url: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Download image bytes using Playwright's request client.
    Returns (bytes, content_type)
    """
    try:
        resp = context.request.get(url, timeout=20000)
        if not resp.ok:
            return None, None
        content_type = resp.headers.get("content-type") or ""
        return resp.body(), content_type
    except Exception:
        return None, None

# -----------------------------------------------------------------------------
# Main
def backfill_images(limit: int = 500, headless: bool = True):
    conn = db_connect()
    rows = select_missing_images(conn, limit)
    has_note = table_has_note_column(conn)

    if not rows:
        print("No Prisma rows with missing image_url found.")
        return

    print(f"Found {len(rows)} Prisma rows missing images. Starting…")

    uploaded = 0
    failed = 0
    skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = context.new_page()

        accept_cookies(page)

        for r in rows:
            pid = r["id"]
            ean = (r.get("ean") or "").strip()
            src_url = r["source_url"]

            # Safety filters
            if not is_prisma_url(src_url):
                skipped += 1
                continue

            try:
                page.goto(src_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
            except PlaywrightTimeout:
                print(f"[{pid}] timeout loading page")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            jitter(0.5, 1.2)

            img_src = extract_image_src(page)
            if not img_src:
                print(f"[{pid}] no image src found")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            img_url_abs = urljoin(BASE, img_src)

            # Download bytes
            data, mime = fetch_image_bytes(context, img_url_abs)
            if not data:
                print(f"[{pid}] failed to download image: {img_url_abs}")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            ext = guess_ext_from_mime(mime)
            fname = f"{ean}.{ext}" if ean else f"id-{pid}.{ext}"
            key = f"{R2_PREFIX}prisma/{fname}"

            # Upload → R2
            try:
                public_url = upload_image_to_r2(data, key, mime or "image/jpeg")
            except Exception as e:
                print(f"[{pid}] R2 upload failed: {e}")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            # Update DB
            try:
                update_image_url(conn, pid, public_url or r2_public_url(key))
                uploaded += 1
                print(f"[{pid}] ✅ uploaded → {public_url or r2_public_url(key)}")
            except Exception as e:
                print(f"[{pid}] DB update failed: {e}")
                failed += 1

            jitter(0.6, 1.5)

        browser.close()

    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}.")

# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Prisma image backfill → R2")
    ap.add_argument("--limit", type=int, default=500, help="Max rows to process")
    ap.add_argument("--headless", type=int, default=1, help="Run browser headless (1/0)")
    args = ap.parse_args()

    backfill_images(limit=args.limit, headless=bool(args.headless))

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee product image backfiller → Cloudflare R2

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

# Minimaalne pildi suurus — alla selle on placeholder
MIN_IMAGE_BYTES = 5000

# URL-id mida ei tohi kasutada
BAD_URL_PATTERNS = [
    "backend-fallback",
    "placeholder",
    "no-image",
    "missing",
    "default-product",
]


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
    return "jpg"


def is_prisma_url(u: str) -> bool:
    try:
        return "prismamarket.ee" in urlparse(u).netloc
    except Exception:
        return False


def is_bad_image_url(url: str) -> bool:
    """Kontrolli kas URL on placeholder/fallback pilt."""
    url_lower = url.lower()
    return any(p in url_lower for p in BAD_URL_PATTERNS)


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
    sql = """
        SELECT id, ean, name, source_url
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


IMG_SELECTORS = [
    "main img[alt][src]",
    "img[alt][src]",
    "img.product-image[src]",
    "img[data-src]",
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
    """Leia pildi URL — filtreeri välja placeholder URL-id."""
    candidates = []

    for sel in IMG_SELECTORS:
        try:
            locs = page.locator(sel)
            count = locs.count()
            for i in range(min(count, 10)):
                loc = locs.nth(i)
                src = loc.get_attribute("src") or loc.get_attribute("data-src")
                if src and not src.startswith("data:") and not is_bad_image_url(src):
                    candidates.append(src)
        except Exception:
            continue

    # Eelistame prismamarket CDN URL-e
    for src in candidates:
        if "cdn.s-cloud.fi" in src or "prisma" in src.lower():
            return src

    # Fallback: esimene mis pole halb
    if candidates:
        return candidates[0]

    # OpenGraph
    try:
        og = page.locator("meta[property='og:image']").first
        if og.count() > 0:
            content = og.get_attribute("content")
            if content and not is_bad_image_url(content):
                return content
    except Exception:
        pass

    return None


def fetch_image_bytes(context, url: str) -> Tuple[Optional[bytes], Optional[str]]:
    """Lae pilt — kontrolli suurus ja et pole placeholder."""
    if is_bad_image_url(url):
        return None, None
    try:
        resp = context.request.get(url, timeout=20000)
        if not resp.ok:
            return None, None
        content_type = resp.headers.get("content-type") or ""
        data = resp.body()
        # Kontrolli minimaalne suurus
        if len(data) < MIN_IMAGE_BYTES:
            print(f"  [warn] image too small ({len(data)} bytes) — likely placeholder")
            return None, None
        return data, content_type
    except Exception:
        return None, None


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
            name = (r.get("name") or "")[:50]
            src_url = r["source_url"]

            print(f"[{pid}] {name}")

            if not is_prisma_url(src_url):
                skipped += 1
                continue

            try:
                page.goto(src_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
            except PlaywrightTimeout:
                print(f"  timeout loading page")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            jitter(0.5, 1.2)

            img_src = extract_image_src(page)
            if not img_src:
                print(f"  no valid image src found")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            img_url_abs = urljoin(BASE, img_src)
            print(f"  → {img_url_abs[:80]}")

            data, mime = fetch_image_bytes(context, img_url_abs)
            if not data:
                print(f"  failed to download valid image")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            ext = guess_ext_from_mime(mime)
            fname = f"{ean}.{ext}" if ean else f"id-{pid}.{ext}"
            key = f"{R2_PREFIX}prisma/{fname}"

            try:
                public_url = upload_image_to_r2(data, key, mime or "image/jpeg")
            except Exception as e:
                print(f"  R2 upload failed: {e}")
                failed += 1
                if has_note:
                    set_note(conn, pid, "Kontrolli visuaali!")
                continue

            try:
                update_image_url(conn, pid, public_url or r2_public_url(key))
                uploaded += 1
                print(f"  ✅ {public_url or r2_public_url(key)}")
            except Exception as e:
                print(f"  DB update failed: {e}")
                failed += 1

            jitter(0.6, 1.5)

        browser.close()

    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}.")


def main():
    ap = argparse.ArgumentParser(description="Prisma image backfill → R2")
    ap.add_argument("--limit", type=int, default=500, help="Max rows to process")
    ap.add_argument("--headless", type=int, default=1, help="Run browser headless (1/0)")
    args = ap.parse_args()
    backfill_images(limit=args.limit, headless=bool(args.headless))


if __name__ == "__main__":
    main()

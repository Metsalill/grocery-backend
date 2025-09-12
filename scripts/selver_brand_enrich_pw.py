#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (no guessing).
- Visits PDP: https://www.selver.ee/p/<ext_id>
- Extracts brand from:
  1) JSON-LD: product.brand / manufacturer.name
  2) Spec rows: "Kaubamärk" / "Tootja" (dt/dd or th/td)
  3) Meta: product:brand
- Writes selver_candidates.brand; also fills products.brand if empty (by EAN).

Env:
  DATABASE_URL     (required)
  MAX_ITEMS        (default 500)   -- how many missing-brand rows to process
  HEADLESS         (1|0, default 1)
  REQ_DELAY        (seconds, default 0.25)
  TIMEBOX_SECONDS  (default 1200)

Usage:
  python scripts/selver_brand_enrich_pw.py
"""

from __future__ import annotations
import os, re, json, time, signal, sys
import psycopg2
from contextlib import closing
from playwright.sync_api import sync_playwright

BASE = "https://www.selver.ee/p/"

BRAND_LABELS = re.compile(r'(kaubam[aä]rk|tootja|valmistaja|brand)', re.I)

def _clean(s: str|None) -> str:
    if not s: return ''
    s = re.sub(r'[\u2122\u00AE]', '', s)     # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    # ultra-conservative: reject obvious non-brands (units etc.)
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, flags=re.I):
        return ''
    return s

def extract_brand(page) -> str:
    # 1) JSON-LD blocks
    try:
        for el in page.locator('script[type="application/ld+json"]').all():
            txt = el.text_content() or ''
            if not txt.strip(): continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                # brand
                b = n.get('brand')
                if isinstance(b, dict): b = b.get('name')
                b = _clean(b)
                if b: return b
                # manufacturer
                m = n.get('manufacturer')
                if isinstance(m, dict): m = m.get('name')
                m = _clean(m)
                if m: return m
    except Exception:
        pass

    # 2) Spec rows (dt/dd or th/td)
    try:
        html = page.content()
        # dt/dd pairs
        for k, v in re.findall(r'(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b: return b
        # th/td pairs
        for k, v in re.findall(r'(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b: return b
    except Exception:
        pass

    # 3) Meta: product:brand
    try:
        val = page.eval_on_selector('meta[property="product:brand"]',
                                    'el => el ? el.content || el.getAttribute("content") : null')
        b = _clean(val or '')
        if b: return b
    except Exception:
        pass

    return ''  # no brand found

def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("Missing DATABASE_URL", file=sys.stderr)
        sys.exit(2)

    max_items = int(os.environ.get("MAX_ITEMS", "500"))
    headless = os.environ.get("HEADLESS", "1") == "1"
    req_delay = float(os.environ.get("REQ_DELAY", "0.25"))
    timebox = int(os.environ.get("TIMEBOX_SECONDS", "1200"))
    deadline = time.time() + timebox

    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT ext_id::text, COALESCE(ean_norm, ean_raw) AS ean
            FROM selver_candidates
            WHERE (brand IS NULL OR brand = '')
              AND COALESCE(ean_norm, ean_raw) IS NOT NULL
            ORDER BY ext_id
            LIMIT %s
        """, (max_items,))
        rows = cur.fetchall()

    if not rows:
        print("Nothing to do.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        processed = 0
        found = 0
        for ext_id, ean in rows:
            if time.time() > deadline:
                print("Timebox reached, stopping.")
                break
            url = BASE + str(ext_id)
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
            except Exception as e:
                print(f"[MISS_NAV] ext_id={ext_id} url={url} err={e}", flush=True)
                continue

            b = extract_brand(page)
            processed += 1
            if not b:
                print(f"[MISS_BRAND] ext_id={ext_id} url={url}")
                time.sleep(req_delay)
                continue

            # Write to DB
            with closing(psycopg2.connect(dsn)) as conn2, conn2.cursor() as cur2:
                try:
                    cur2.execute(
                        "UPDATE selver_candidates SET brand = %s WHERE ext_id = %s AND (brand IS NULL OR brand = '')",
                        (b, ext_id)
                    )
                    if ean:
                        cur2.execute("""
                            UPDATE products p
                               SET brand = %s
                             WHERE p.ean = %s
                               AND (p.brand IS NULL OR p.brand = '')
                        """, (b, ean))
                    conn2.commit()
                    found += 1
                    print(f"[BRAND] ext_id={ext_id} brand=\"{b}\"")
                except Exception as e:
                    conn2.rollback()
                    print(f"[DB_ERR] ext_id={ext_id} err={e}")

            time.sleep(req_delay)

        browser.close()
        print(f"Done. processed={processed} brand_found={found}")

if __name__ == "__main__":
    # graceful ctrl-c in CI timebox
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

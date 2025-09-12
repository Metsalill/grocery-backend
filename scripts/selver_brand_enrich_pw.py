#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (Käitleja-based) — drives from products/ext_product_map.

What it does
------------
- Builds a worklist of Selver-sourced products whose current brand is empty or junk.
- For each PDP, extracts *Käitleja* (preferred; also accepts Tootja/Valmistaja/Manufacturer).
  Falls back to JSON-LD `manufacturer` (then `brand`) if needed.
- Writes the result to `products.brand` (only if brand is empty/junk).
- If a matching row exists in `selver_candidates`, it also fills that brand (best-effort).

Env:
  DATABASE_URL     (required)
  MAX_ITEMS        (default 500)
  HEADLESS         (1|0, default 1)
  REQ_DELAY        (seconds, default 0.25)
  TIMEBOX_SECONDS  (default 1200)
"""

from __future__ import annotations
import os, re, json, time, signal, sys
import psycopg2
from contextlib import closing
from playwright.sync_api import sync_playwright

BASE_HOST = "https://www.selver.ee"
PDP_NUMERIC_PREFIX = BASE_HOST + "/p/"

# Prefer the company/owner fields as the brand
FAVOR_LABELS = re.compile(r'(käitleja|tootja|valmistaja|manufacturer)', re.I)

def build_url(ext: str) -> str:
    s = (ext or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("/"):
        return BASE_HOST + s
    if re.fullmatch(r"\d+", s):
        return PDP_NUMERIC_PREFIX + s
    return f"{BASE_HOST}/{s}"

def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)  # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    # reject obvious size/unit strings
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, re.I):
        return ""
    # reject long blobs / URLs / emails
    if len(s) > 100 or re.search(r'(http|www\.)', s, re.I) or '@' in s:
        return ""
    return s

def accept_overlays(page):
    for sel in [
        'button#onetrust-accept-btn-handler',
        'button:has-text("Nõustun")',
        'button:has-text("Accept")',
    ]:
        try:
            loc = page.locator(sel).first
            if loc.is_visible():
                loc.click(timeout=2000)
                break
        except Exception:
            pass

def wait_for_pdp_ready(page):
    try:
        page.wait_for_selector(
            'table.ProductAttributes__table, script[type="application/ld+json"]',
            timeout=15000
        )
    except Exception:
        pass

def _first_text(page, selectors: list[str]) -> str:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                txt = _clean(loc.text_content() or '')
                if txt:
                    return txt
        except Exception:
            pass
    return ""

def extract_brand(page) -> str:
    # 0) Direct label/value pairs – favor Käitleja (& synonyms)
    direct = _first_text(page, [
        'th:has-text("Käitleja") + td',
        'th:has-text("Tootja") + td',
        'th:has-text("Valmistaja") + td',
        'th:has-text("Manufacturer") + td',
        'dt:has-text("Käitleja") + dd',
        'dt:has-text("Tootja") + dd',
        'dt:has-text("Valmistaja") + dd',
        'dt:has-text("Manufacturer") + dd',
    ])
    if direct:
        return direct

    # 1) JSON-LD: prefer "manufacturer", then "brand"
    try:
        for el in page.locator('script[type="application/ld+json"]').all():
            txt = el.text_content() or ''
            if not txt.strip():
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                m = n.get('manufacturer')
                if isinstance(m, dict):
                    m = m.get('name')
                m = _clean(m)
                if m:
                    return m
                b = n.get('brand')
                if isinstance(b, dict):
                    b = b.get('name')
                b = _clean(b)
                if b:
                    return b
    except Exception:
        pass

    # 2) Generic sibling scan (limit to favorite labels)
    try:
        js = """
        () => {
          const keys = /(käitleja|tootja|valmistaja|manufacturer)/i;
          const clean = (t) => (t||'').replace(/[\\u2122\\u00AE]/g,'').replace(/\\s+/g,' ').trim();
          const els = Array.from(document.querySelectorAll('dt,th,td,li,span,div,p,strong,b'));
          for (const el of els) {
            const k = clean(el.textContent || '');
            if (!keys.test(k)) continue;
            let v = '';
            if (el.nextElementSibling) v = el.nextElementSibling.textContent || '';
            if (!v && el.parentElement) {
              const sibs = Array.from(el.parentElement.children);
              const i = sibs.indexOf(el);
              if (i >= 0 && i+1 < sibs.length) v = sibs[i+1].textContent || '';
            }
            v = clean(v);
            if (v) return v;
          }
          return '';
        }
        """
        v = page.evaluate(js)
        v = _clean(v)
        if v:
            return v
    except Exception:
        pass

    return ''

BAD_BRAND_CLAUSE = """
(p.brand IS NULL OR p.brand = '' OR
 p.brand ILIKE 'e-selveri info%%' OR
 length(p.brand) > 100 OR
 p.brand ~* '(http|www\\.)' OR
 p.brand ~* '@')
"""

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

    # Worklist: Selver products with empty/garbage brand
    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT ON (m.ext_id, p.ean)
                   m.ext_id::text, p.ean
              FROM products p
              JOIN ext_product_map m
                ON m.product_id = p.id
               AND m.source = 'selver'
             WHERE p.ean IS NOT NULL
               AND {BAD_BRAND_CLAUSE}
             ORDER BY m.ext_id, p.ean, p.id
             LIMIT %s
        """, (max_items,))
        rows = cur.fetchall()

    if not rows:
        print("Nothing to do (no Selver products with bad/empty brand).")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()

        # Block heavy third-party noise
        block_domains = [
            "googletagmanager", "google-analytics", "doubleclick",
            "facebook", "fonts.googleapis.com", "use.typekit.net",
        ]
        def _route_blocker(route, request):
            url = request.url
            if any(d in url for d in block_domains):
                route.abort()
            else:
                route.continue_()
        context.route("**/*", _route_blocker)

        page = context.new_page()

        processed = 0
        updated = 0
        for ext_id, ean in rows:
            if time.time() > deadline:
                print("Timebox reached, stopping.")
                break

            url = build_url(str(ext_id))
            if not url:
                print(f"[MISS_BRAND] ext_id={ext_id} url=<empty>")
                continue

            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                accept_overlays(page)
                wait_for_pdp_ready(page)
            except Exception as e:
                print(f"[MISS_NAV] ext_id={ext_id} url={url} err={e}")
                continue

            b = extract_brand(page)
            processed += 1
            if not b:
                print(f"[MISS_BRAND] ext_id={ext_id} url={url}")
                time.sleep(req_delay)
                continue

            with closing(psycopg2.connect(dsn)) as conn2, conn2.cursor() as cur2:
                try:
                    # Update products (guarded by BAD_BRAND_CLAUSE again)
                    cur2.execute(f"""
                        UPDATE products p
                           SET brand = %s
                          FROM ext_product_map m
                         WHERE m.product_id = p.id
                           AND m.source = 'selver'
                           AND p.ean = %s
                           AND {BAD_BRAND_CLAUSE}
                    """, (b, ean))
                    # Best-effort: cache into selver_candidates if row exists
                    cur2.execute("""
                        UPDATE selver_candidates
                           SET brand = %s
                         WHERE ext_id::text = %s
                           AND (brand IS NULL OR brand = '')
                    """, (b, ext_id))
                    conn2.commit()
                    updated += 1
                    print(f"[BRAND] ext_id={ext_id} brand=\"{b}\"")
                except Exception as e:
                    conn2.rollback()
                    print(f"[DB_ERR] ext_id={ext_id} err={e}")

            time.sleep(req_delay)

        browser.close()
        print(f"Done. processed={processed} products_updated={updated}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

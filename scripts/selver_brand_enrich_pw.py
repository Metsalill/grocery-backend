#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (structured fields only).
- Works with ext_id being: numeric id, slug path, absolute URL, or "/slug".
- Extracts brand from JSON-LD, spec rows (Käitleja/Tootja/Valmistaja/Kaubamärk/Brand),
  and meta product:brand. No title/name guessing.

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

# include Käitleja and common variants
BRAND_LABELS = re.compile(r'(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)', re.I)

def build_url(ext: str) -> str:
    """Accepts numeric id, slug, '/slug', or absolute URL."""
    s = (ext or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("/"):
        return BASE_HOST + s
    if re.fullmatch(r"\d+", s):                    # pure numeric id
        return PDP_NUMERIC_PREFIX + s
    # plain slug
    return f"{BASE_HOST}/{s}"

def _clean(s: str | None) -> str:
    if not s: return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)           # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    # reject obvious size/unit strings
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, re.I):
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
    """Wait until the product attributes table or JSON-LD is present."""
    try:
        page.wait_for_selector(
            'table.ProductAttributes__table, script[type="application/ld+json"]',
            timeout=15000
        )
    except Exception:
        # best-effort; some pages might still be fine
        pass

def extract_brand(page) -> str:
    # 0) Direct selectors: table with label/value pairs (fast, robust)
    sel_pairs = [
        'th:has-text("Käitleja") + td',
        'th:has-text("Tootja") + td',
        'th:has-text("Valmistaja") + td',
        'th:has-text("Kaubamärk") + td',
        'th:has-text("Brand") + td',
        'dt:has-text("Käitleja") + dd',
        'dt:has-text("Tootja") + dd',
        'dt:has-text("Valmistaja") + dd',
        'dt:has-text("Kaubamärk") + dd',
        'dt:has-text("Brand") + dd',
    ]
    try:
        for sel in sel_pairs:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                txt = loc.text_content() or ''
                b = _clean(txt)
                if b: return b
    except Exception:
        pass

    # 1) JSON-LD
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
                b = n.get('brand')
                if isinstance(b, dict): b = b.get('name')
                b = _clean(b)
                if b: return b
                m = n.get('manufacturer')
                if isinstance(m, dict): m = m.get('name')
                m = _clean(m)
                if m: return m
    except Exception:
        pass

    # 2) Spec rows (regex over HTML) and generic sibling scan
    try:
        html = page.content()
        for k, v in re.findall(r'(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b: return b
        for k, v in re.findall(r'(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b: return b

        js = """
        () => {
          const keys = /(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)/i;
          const clean = (t) => (t||'').replace(/[\\u2122\\u00AE]/g,'').replace(/\\s+/g,' ').trim();
          const els = Array.from(document.querySelectorAll('dt,th,td,li,span,div,p,strong,b'));
          for (const el of els) {
            const k = clean(el.textContent || '');
            if (!keys.test(k)) continue;
            let v = '';
            if (el.nextElementSibling) v = el.nextElementSibling.textContent;
            if (!v && el.parentElement) {
              const sibs = Array.from(el.parentElement.children);
              const i = sibs.indexOf(el);
              if (i >= 0 && i+1 < sibs.length) v = sibs[i+1].textContent;
            }
            v = clean(v);
            if (v) return v;
          }
          return '';
        }
        """
        v = page.evaluate(js)
        v = _clean(v)
        if v: return v
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

    return ''

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

        # Create context and (optionally) block heavy third-party requests
        context = browser.new_context()

        block_domains = [
            "googletagmanager", "google-analytics", "doubleclick",
            "facebook", "fonts.googleapis.com", "use.typekit.net",
        ]

        def _route_blocker(route, request):
            url = request.url  # Request is an object, not a function
            if any(d in url for d in block_domains):
                route.abort()
            else:
                route.continue_()

        context.route("**/*", _route_blocker)

        page = context.new_page()

        processed = 0
        found = 0
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
                wait_for_pdp_ready(page)  # <-- wait for attributes/JSON-LD to be present
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
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

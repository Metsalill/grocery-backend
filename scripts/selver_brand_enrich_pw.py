#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (structured fields only).

- Accepts ext_id as: slug path, absolute URL, "/slug", "/p/<numeric>", raw numeric,
  or an EAN (8/13 digits).  EANs are handled via on-site search.
- Extracts brand from Käitleja/Tootja/Valmistaja/Kaubamärk/Brand rows,
  JSON-LD (brand/manufacturer) and meta product:brand. No title/name guessing.

Env:
  DATABASE_URL        (required)
  MAX_ITEMS           (default 500)
  HEADLESS            (1|0, default 1)
  REQ_DELAY           (seconds, default 0.25)
  TIMEBOX_SECONDS     (default 1200)
  OVERWRITE_PRODUCTS  (1|0, default 0)  # if 1, overwrite brands in products
"""

from __future__ import annotations
import os, re, json, time, signal, sys
import psycopg2
from contextlib import closing
from playwright.sync_api import sync_playwright

BASE_HOST = "https://www.selver.ee"
PDP_NUMERIC_PREFIX = BASE_HOST + "/p/"

BRAND_LABELS = re.compile(r'(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)', re.I)
IS_EAN = re.compile(r'^\d{8}(\d{5})?$')   # 8 or 13 digits

def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)     # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, re.I):
        return ""
    return s

def build_target(ext: str) -> tuple[str, str]:
    """Return ("url", absolute_url) or ("search", query)."""
    s = (ext or "").strip()
    if not s:
        return ("url", "")

    if s.startswith(("http://", "https://")):
        return ("url", s)
    if s.startswith("/p/") and re.fullmatch(r"/p/\d+", s):
        return ("url", BASE_HOST + s)
    if s.startswith("/"):
        return ("url", BASE_HOST + s)
    if re.search(r"[A-Za-z]", s) and not s.startswith("http"):
        return ("url", f"{BASE_HOST}/{s}")
    if IS_EAN.fullmatch(s):
        return ("search", s)
    if re.fullmatch(r"\d+", s):
        return ("url", PDP_NUMERIC_PREFIX + s)
    return ("url", f"{BASE_HOST}/{s}")

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

def try_open_first_search_result(page) -> bool:
    try:
        hrefs = page.eval_on_selector_all(
            "main a[href]", "els => els.map(e => e.getAttribute('href'))"
        ) or []
        for href in hrefs:
            if not href or href.startswith("#") or "javascript:" in href:
                continue
            if re.search(r"/[-a-z0-9]+(?:-[a-z0-9]+)+/?$", href, re.I):
                page.goto(href if href.startswith("http") else BASE_HOST + href,
                          timeout=30000, wait_until="domcontentloaded")
                return True
    except Exception:
        pass
    return False

def navigate_to_candidate(page, ext_id: str, ean_for_search: str | None) -> bool:
    """Try direct PDP; on 404/failure, search by EAN (preferred) or ext_id."""
    mode, value = build_target(ext_id)

    # direct URL first
    if mode == "url":
        try:
            page.goto(value, timeout=30000, wait_until="domcontentloaded")
            accept_overlays(page)
            if page.locator('text=Lehekülge ei leitud').first.is_visible():
                mode, value = ("search", ean_for_search or ext_id)
            else:
                return True
        except Exception:
            mode, value = ("search", ean_for_search or ext_id)

    if mode == "search":
        try:
            page.goto(BASE_HOST, timeout=30000, wait_until="domcontentloaded")
            accept_overlays(page)
            search_box = page.locator('input[placeholder*="Otsi toodet"]').first
            if not search_box.is_visible():
                search_box = page.locator('header input[type="text"]').first
            search_box.fill(value)
            search_box.press("Enter")
            page.wait_for_load_state("domcontentloaded")
            if try_open_first_search_result(page):
                return True
        except Exception:
            return False

    return False

def extract_brand(page) -> str:
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
                if b:
                    return b
    except Exception:
        pass

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
                b = n.get('brand')
                if isinstance(b, dict):
                    b = b.get('name')
                b = _clean(b)
                if b:
                    return b
                m = n.get('manufacturer')
                if isinstance(m, dict):
                    m = m.get('name')
                m = _clean(m)
                if m:
                    return m
    except Exception:
        pass

    try:
        html = page.content()
        for k, v in re.findall(r'(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b:
                    return b
        for k, v in re.findall(r'(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _clean(re.sub(r'<.*?>', ' ', v))
                if b:
                    return b

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
        if v:
            return v
    except Exception:
        pass

    try:
        val = page.eval_on_selector(
            'meta[property="product:brand"]',
            'el => el ? el.content || el.getAttribute("content") : null'
        )
        b = _clean(val or '')
        if b:
            return b
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
    overwrite = os.environ.get("OVERWRITE_PRODUCTS", "0") == "1"
    deadline = time.time() + timebox

    # Pull Selver work from products↔ext_product_map
    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        brand_cond = "" if overwrite else "AND COALESCE(p.brand,'') = ''"
        cur.execute(
            f"""
            SELECT
                p.id,
                COALESCE(NULLIF(m.ext_id::text, ''), p.ean::text) AS ext_id,
                p.ean::text AS ean
            FROM products p
            JOIN ext_product_map m
              ON m.product_id = p.id
             AND m.source = 'selver'
            WHERE p.ean IS NOT NULL
              {brand_cond}
            ORDER BY p.id
            LIMIT %s
            """,
            (max_items,),
        )
        rows = cur.fetchall()

    if not rows:
        print("Nothing to do.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()

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
        found = 0
        for product_id, ext_id, ean in rows:
            if time.time() > deadline:
                print("Timebox reached, stopping.")
                break

            if not ext_id and not ean:
                print(f"[MISS_BRAND] product_id={product_id} no ext_id/ean")
                continue

            ok = navigate_to_candidate(page, str(ext_id or ean), ean_for_search=ean)
            if not ok:
                print(f"[MISS_BRAND] product_id={product_id} nav failed (ext_id={ext_id}, ean={ean})")
                time.sleep(req_delay)
                continue

            wait_for_pdp_ready(page)
            b = extract_brand(page)
            processed += 1
            if not b:
                curr_url = ""
                try:
                    curr_url = page.url
                except Exception:
                    pass
                print(f"[MISS_BRAND] product_id={product_id} url={curr_url}")
                time.sleep(req_delay)
                continue

            with closing(psycopg2.connect(dsn)) as conn2, conn2.cursor() as cur2:
                try:
                    if overwrite:
                        cur2.execute(
                            "UPDATE products SET brand = %s WHERE ean = %s",
                            (b, ean),
                        )
                    else:
                        cur2.execute(
                            "UPDATE products SET brand = %s "
                            "WHERE ean = %s AND (brand IS NULL OR brand = '')",
                            (b, ean),
                        )
                    conn2.commit()
                    found += 1
                    print(f"[BRAND] product_id={product_id} ean={ean} brand=\"{b}\"")
                except Exception as e:
                    conn2.rollback()
                    print(f"[DB_ERR] product_id={product_id} err={e}")

            time.sleep(req_delay)

        browser.close()
        print(f"Done. processed={processed} brand_found={found}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

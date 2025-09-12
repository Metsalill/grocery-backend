#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (structured sources only).

- Worklist comes from products joined with ext_product_map (source='selver').
- ext_id can be a slug, absolute URL, "/slug", "/p/<numeric>", raw numeric id,
  or an EAN (8/13 digits). EANs are handled via on-site search.
- Extracts brand from Käitleja/Tootja/Valmistaja/Kaubamärk/Brand rows,
  JSON-LD (brand/manufacturer) and meta product:brand. No title/name guessing.

Env:
  DATABASE_URL        (required)
  MAX_ITEMS           (default 500)
  HEADLESS            (1|0, default 1)
  REQ_DELAY           (seconds, default 0.25)
  TIMEBOX_SECONDS     (default 1200)
  OVERWRITE_PRODUCTS  (1|0, default 0) — when 1, overwrite even non-empty brands
"""

from __future__ import annotations

import os, re, json, time, signal, sys
from contextlib import closing
from urllib.parse import quote

import psycopg2
from playwright.sync_api import sync_playwright

BASE_HOST = "https://www.selver.ee"
PDP_NUMERIC_PREFIX = BASE_HOST + "/p/"

# labels we accept in spec tables
BRAND_LABELS = re.compile(r'(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)', re.I)

# EAN: 8 or 13 digits
IS_EAN = re.compile(r'^\d{8}(\d{5})?$')

# Things that are definitely NOT a brand
URL_OR_EMAIL = re.compile(r'(https?://|www\.)|@', re.I)

# Keep generic placeholders, but DO NOT include "määramata" (we accept it).
PLACEHOLDER_BRANDS = {
    "täpsustamata",
    "puudub",
    "unknown",
    "n/a",
    "na",
    "none",
    "-", "—", "–",
}

def _clean(s: str | None) -> str:
    if not s:
        return ""
    # strip ™ and ® then collapse whitespace
    s = re.sub(r'[\u2122\u00AE]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    # reject obvious size/unit strings like "500 ml"
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, re.I):
        return ""
    return s

def _normalize_maaramata(b: str) -> str:
    """
    Normalize common variants/typos of 'Määramata' to exactly 'Määramata'.
    (We accept this as a valid brand per business rules.)
    """
    t = (b or "").strip()
    if not t:
        return ""
    low = t.lower()
    low_no_diac = low.replace('ä', 'a')
    # direct matches
    if low in {"määramata", "määrmata"}:
        return "Määramata"
    # crude but effective typo coverage without external libs
    if low_no_diac in {"maaramata", "maarmata", "maarama"}:
        return "Määramata"
    # also catch cases missing one character around 'mata'
    if re.fullmatch(r"m[äa]ära?m?ata", low):
        return "Määramata"
    return t

def _canonicalize(b: str) -> str:
    return _normalize_maaramata(_clean(b))

def _is_bad_brand(b: str) -> bool:
    """
    Reject only real junk. 'Määramata' must PASS and be stored.
    """
    if not b:
        return True
    if len(b) > 120:
        return True
    if URL_OR_EMAIL.search(b):
        return True
    bl = b.strip().lower()
    if bl in PLACEHOLDER_BRANDS:
        return True
    # also treat just punctuation/dashes as empty
    if re.fullmatch(r'[-–—\s]+', b):
        return True
    return False

def build_target(ext: str) -> tuple[str, str]:
    """
    Returns (mode, value):
      ("url", absolute_url)  or  ("search", query) for EAN or general fallback.
    """
    s = (ext or "").strip()
    if not s:
        return ("url", "")

    if s.startswith(("http://","https://")):
        return ("url", s)
    if s.startswith("/p/") and re.fullmatch(r"/p/\d+", s):
        return ("url", BASE_HOST + s)
    if s.startswith("/"):
        return ("url", BASE_HOST + s)
    if re.search(r"[A-Za-z]", s) and not s.startswith(("http://","https://")):
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
            if loc and loc.is_visible():
                loc.click(timeout=2000)
                break
        except Exception:
            pass

def wait_for_pdp_ready(page):
    """Wait until product attributes or JSON-LD appears."""
    try:
        page.wait_for_selector(
            'table.ProductAttributes__table, script[type="application/ld+json"]',
            timeout=15000
        )
    except Exception:
        pass

def _first_product_link_on_results(page) -> str:
    """Return the first plausible product href on a search results page (or "")."""
    try:
        hrefs = page.eval_on_selector_all(
            "main a[href]",
            "els => els.map(e => e.getAttribute('href'))"
        ) or []
        for href in hrefs:
            if not href:
                continue
            if href.startswith("#") or "javascript:" in href or "?" in href:
                continue
            # prefer sluggy product URL
            if re.search(r"/[-a-z0-9]+(?:-[a-z0-9]+)+/?$", href, re.I):
                return href
    except Exception:
        pass
    return ""

def _search_url_from_form(page, query: str) -> str | None:
    """
    Inspect the search form (action + input name) and construct the actual search URL.
    """
    try:
        data = page.evaluate("""
        () => {
          const form = document.querySelector('form[role="search"], form[action*="otsing"], header form, form');
          if (!form) return null;
          const input = form.querySelector('input[name]') || form.querySelector('input[type="search"]') || form.querySelector('input[type="text"]');
          let action = (form.getAttribute('action') || '/otsing').trim();
          let param  = input ? (input.getAttribute('name') || 'q') : 'q';
          return { action, param };
        }
        """)
        if not data:
            return None
        action = data.get("action") or "/otsing"
        param  = data.get("param") or "q"
        if action.startswith("http"):
            base = action
        elif action.startswith("/"):
            base = BASE_HOST + action
        else:
            base = BASE_HOST + "/" + action
        return f"{base}?{param}={quote(query)}"
    except Exception:
        return None

def navigate_via_search(page, query: str) -> bool:
    """Go to home, construct real search URL, open it, and click the first product."""
    try:
        page.goto(BASE_HOST, timeout=30000, wait_until="domcontentloaded")
        accept_overlays(page)

        url = _search_url_from_form(page, query)
        if url:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
        else:
            box = page.locator('input[placeholder*="Otsi toodet"]').first
            if not box or not box.is_visible():
                box = page.locator('header input[type="text"]').first
            box.fill(query)
            box.press("Enter")
            page.wait_for_load_state("domcontentloaded")

        href = _first_product_link_on_results(page)
        if href:
            if href.startswith("/"):
                page.goto(BASE_HOST + href, timeout=30000, wait_until="domcontentloaded")
            else:
                page.goto(href, timeout=30000, wait_until="domcontentloaded")
            return True
    except Exception:
        return False
    return False

def navigate_to_candidate(page, ext_id: str | None, ean: str | None) -> bool:
    """
    Navigate to a PDP using ext_id; if that 404s or fails, search by EAN.
    """
    if ext_id:
        mode, value = build_target(str(ext_id))
        if mode == "url":
            try:
                page.goto(value, timeout=30000, wait_until="domcontentloaded")
                accept_overlays(page)
                if page.locator('text=Lehekülge ei leitud').first.is_visible():
                    pass  # will try EAN
                else:
                    return True
            except Exception:
                pass
        else:
            if navigate_via_search(page, value):
                return True

    if ean:
        if navigate_via_search(page, ean):
            return True

    return False

def _accept_or_none(raw: str) -> str:
    """Clean, normalize 'Määramata' variants, and reject actual junk."""
    b = _canonicalize(raw)
    # Allow 'Määramata' explicitly (it will pass _is_bad_brand anyway, but be explicit).
    if b.lower() in {"määramata"}:
        return "Määramata"
    if _is_bad_brand(b):
        return ""
    return b

def extract_brand(page) -> str:
    # 0) Direct spec table/definition list selectors
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
                b = _accept_or_none(txt)
                if b:
                    return b
    except Exception:
        pass

    # 1) JSON-LD
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
                b = _accept_or_none(b or "")
                if b:
                    return b
                m = n.get('manufacturer')
                if isinstance(m, dict):
                    m = m.get('name')
                m = _accept_or_none(m or "")
                if m:
                    return m
    except Exception:
        pass

    # 2) Regex over HTML + generic sibling probing
    try:
        html = page.content()
        for k, v in re.findall(r'(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _accept_or_none(re.sub(r'<.*?>', ' ', v))
                if b:
                    return b
        for k, v in re.findall(r'(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html):
            if BRAND_LABELS.search(re.sub(r'<.*?>', ' ', k)):
                b = _accept_or_none(re.sub(r'<.*?>', ' ', v))
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
        v = _accept_or_none(v)
        if v:
            return v
    except Exception:
        pass

    # 3) Meta tag: product:brand
    try:
        val = page.eval_on_selector(
            'meta[property="product:brand"]',
            'el => el ? el.content || el.getAttribute("content") : null'
        )
        b = _accept_or_none(val or '')
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

    # Build worklist from products + ext_product_map(source='selver')
    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT p.id,
                   p.ean::text AS ean,
                   COALESCE(m.ext_id::text, '') AS ext_id
              FROM products p
              JOIN ext_product_map m
                ON m.product_id = p.id
               AND m.source = 'selver'
             WHERE COALESCE(p.ean::text,'') <> ''
               AND (
                     %s = TRUE
                     OR  -- overwrite disabled -> only rows that look missing/bad
                     p.brand IS NULL
                     OR p.brand = ''
                     OR p.brand ILIKE 'e-selveri info%%'
                     OR length(p.brand) > 100
                     OR p.brand ~* '(http|www\\.)'
                     OR p.brand ~* '@'
                   )
             ORDER BY p.id
             LIMIT %s
        """, (overwrite, max_items))
        rows = cur.fetchall()

    if not rows:
        print("Nothing to do.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()

        # Block heavy 3rd-party requests
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

        with closing(psycopg2.connect(dsn)) as uconn:
            uconn.autocommit = False
            with uconn.cursor() as ucur:
                for product_id, ean, ext_id in rows:
                    if time.time() > deadline:
                        print("Timebox reached, stopping.")
                        break

                    ok = navigate_to_candidate(page, ext_id, ean)
                    if not ok:
                        print(f"[MISS_BRAND] product_id={product_id} nav failed (ext_id={ext_id}, ean={ean})")
                        time.sleep(req_delay)
                        continue

                    wait_for_pdp_ready(page)
                    brand = extract_brand(page)
                    processed += 1

                    if not brand:
                        cur_url = ""
                        try:
                            cur_url = page.url
                        except Exception:
                            pass
                        print(f"[MISS_BRAND_EMPTY] product_id={product_id} url={cur_url}")
                        time.sleep(req_delay)
                        continue

                    try:
                        if overwrite:
                            ucur.execute("UPDATE products SET brand = %s WHERE id = %s", (brand, product_id))
                        else:
                            ucur.execute(
                                "UPDATE products SET brand = %s "
                                "WHERE id = %s AND (brand IS NULL OR brand = '' "
                                "OR brand ILIKE 'e-selveri info%%' OR length(brand) > 100 "
                                "OR brand ~* '(http|www\\.)' OR brand ~* '@')",
                                (brand, product_id)
                            )
                        if ucur.rowcount > 0:
                            updated += 1
                            print(f"[OK] id={product_id} ← brand “{brand}”")
                        else:
                            print(f"[SKIP] id={product_id} brand already OK")
                        uconn.commit()
                    except Exception as e:
                        uconn.rollback()
                        print(f"[DB_ERR] id={product_id} err={e}")

                    time.sleep(req_delay)

        browser.close()
        print(f"Done. processed={processed} updated={updated}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

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
from urllib.parse import quote, urlparse

import psycopg2
from playwright.sync_api import sync_playwright

BASE_HOST = "https://www.selver.ee"
PDP_NUMERIC_PREFIX = BASE_HOST + "/p/"

BRAND_LABELS = re.compile(r'(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)', re.I)
IS_EAN = re.compile(r'^\d{8}(\d{5})?$')
URL_OR_EMAIL = re.compile(r'(https?://|www\.)|@', re.I)

PLACEHOLDER_BRANDS = {
    "täpsustamata", "puudub", "unknown", "n/a", "na", "none", "-", "—", "–",
}

def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)     # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, re.I):
        return ""
    return s

def _normalize_maaramata(b: str) -> str:
    t = (b or "").strip()
    if not t:
        return ""
    low = t.lower()
    low_no_diac = low.replace('ä', 'a')
    if low in {"määramata", "määrmata"}:
        return "Määramata"
    if low_no_diac in {"maaramata", "maarmata", "maarama"}:
        return "Määramata"
    if re.fullmatch(r"m[äa]ära?m?ata", low):
        return "Määramata"
    return t

def _canonicalize(b: str) -> str:
    return _normalize_maaramata(_clean(b))

def _is_bad_brand(b: str) -> bool:
    if not b:
        return True
    if len(b) > 120:
        return True
    if URL_OR_EMAIL.search(b):
        return True
    bl = b.strip().lower()
    if bl in PLACEHOLDER_BRANDS:
        return True
    if re.fullmatch(r'[-–—\s]+', b):
        return True
    return False

def _accept_or_none(raw: str) -> str:
    b = _canonicalize(raw)
    if b.lower() == "määramata":
        return "Määramata"
    if _is_bad_brand(b):
        return ""
    return b

def build_target(ext: str) -> tuple[str, str]:
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

def _is_selver_url(u: str) -> bool:
    if not u:
        return False
    if u.startswith("/"):
        return True
    if u.startswith("http"):
        try:
            host = urlparse(u).hostname or ""
        except Exception:
            return False
        return host.endswith("selver.ee")
    return False

def accept_overlays(page):
    # OneTrust + Cookiebot common accept buttons
    for sel in [
        'button#onetrust-accept-btn-handler',
        '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
        '#CybotCookiebotDialogBodyButtonAccept',
        'button:has-text("Nõustun")',
        'button:has-text("Allow all")',
        'button:has-text("Accept all")',
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
    try:
        page.wait_for_selector(
            'table.ProductAttributes__table, script[type="application/ld+json"]',
            timeout=15000
        )
    except Exception:
        pass

def _first_product_link_on_results(page) -> str:
    try:
        try:
            page.wait_for_selector(
                'a[href*="/p/"], .ProductCard a[href], a.ProductCard__link[href], main a[href*="-"]',
                timeout=8000
            )
        except Exception:
            pass
        hrefs = page.eval_on_selector_all(
            "main a[href], .content a[href], a[href]",
            "els => els.map(e => e.getAttribute('href'))"
        ) or []

        def looks_like_product(h: str) -> bool:
            if not h or not _is_selver_url(h):
                return False
            if h.startswith("#") or "javascript:" in h:
                return False
            h_path = h.split('#', 1)[0].split('?', 1)[0]
            if "/p/" in h_path:
                return True
            leaf = h_path.rstrip("/").split("/")[-1]
            return ("-" in leaf and len(leaf) > 3)

        for href in hrefs:
            if looks_like_product(href):
                base = href.split('#', 1)[0].split('?', 1)[0]
                if base.startswith("/"):
                    return BASE_HOST + base
                if base.startswith("http"):
                    return base

        # explicit fallbacks within selver domain
        for sel in ['a[href*="/p/"]', '.ProductCard a[href]', 'a.ProductCard__link[href]']:
            try:
                el = page.locator(sel).first
                if el and el.count() > 0 and el.is_visible():
                    href = el.get_attribute("href") or ""
                    if _is_selver_url(href):
                        base = href.split('#', 1)[0].split('?', 1)[0]
                        if base.startswith("/"):
                            return BASE_HOST + base
                        if base.startswith("http"):
                            return base
            except Exception:
                pass
    except Exception:
        pass
    return ""

def _search_url_from_form(page, query: str) -> str | None:
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

def _visit_search_and_open_first(page, query: str) -> bool:
    tried = []
    url = _search_url_from_form(page, query)
    if url:
        tried.append(url)
    tried.append(f"{BASE_HOST}/otsing?query={quote(query)}")
    tried.append(f"{BASE_HOST}/otsing?q={quote(query)}")

    for u in tried:
        try:
            page.goto(u, timeout=30000, wait_until="domcontentloaded")
            accept_overlays(page)
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            href = _first_product_link_on_results(page)
            if href:
                page.goto(href, timeout=30000, wait_until="domcontentloaded")
                return True
        except Exception:
            continue
    return False

def navigate_via_search(page, query: str) -> bool:
    try:
        page.goto(BASE_HOST, timeout=30000, wait_until="domcontentloaded")
        accept_overlays(page)
        return _visit_search_and_open_first(page, query)
    except Exception:
        return False

def navigate_to_candidate(page, ext_id: str | None, ean: str | None) -> bool:
    if ext_id:
        mode, value = build_target(str(ext_id))
        if mode == "url":
            try:
                page.goto(value, timeout=30000, wait_until="domcontentloaded")
                accept_overlays(page)
                if not page.locator('text=Lehekülge ei leitud').first.is_visible():
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

def extract_brand(page) -> str:
    # 0) direct table/dl selectors
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

    # 2) HTML regex + sibling probing
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

    # 3) meta tag
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
        print("Missing DATABASE_URL", file=sys.stderr); sys.exit(2)

    max_items = int(os.environ.get("MAX_ITEMS", "500"))
    headless   = os.environ.get("HEADLESS", "1") == "1"
    req_delay  = float(os.environ.get("REQ_DELAY", "0.25"))
    timebox    = int(os.environ.get("TIMEBOX_SECONDS", "1200"))
    overwrite  = os.environ.get("OVERWRITE_PRODUCTS", "0") == "1"
    deadline   = time.time() + timebox

    # Worklist: only Selver-mapped products; restrict when overwrite=0
    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.ean::text AS ean, COALESCE(m.ext_id::text, '') AS ext_id
              FROM products p
              JOIN ext_product_map m ON m.product_id = p.id AND m.source = 'selver'
             WHERE COALESCE(p.ean::text,'') <> ''
               AND (
                     %s = TRUE
                     OR p.brand IS NULL OR p.brand = ''
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
        print("Nothing to do."); return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()

        block_domains = [
            "googletagmanager","google-analytics","doubleclick",
            "facebook","fonts.googleapis.com","use.typekit.net",
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
        updated   = 0

        with closing(psycopg2.connect(dsn)) as uconn:
            uconn.autocommit = False
            with uconn.cursor() as ucur:
                for product_id, ean, ext_id in rows:
                    if time.time() > deadline:
                        print("Timebox reached, stopping."); break

                    ok = navigate_to_candidate(page, ext_id, ean)
                    if not ok:
                        print(f"[MISS_BRAND] product_id={product_id} nav failed (ext_id={ext_id}, ean={ean})")
                        time.sleep(req_delay); continue

                    wait_for_pdp_ready(page)
                    brand = extract_brand(page)
                    processed += 1

                    if not brand:
                        cur_url = ""
                        try: cur_url = page.url
                        except Exception: pass
                        print(f"[MISS_BRAND_EMPTY] product_id={product_id} url={cur_url}")
                        time.sleep(req_delay); continue

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

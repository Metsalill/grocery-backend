#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (PDP, structured fields only).

Scope:
- Only products mapped to Selver (ext_product_map.source = 'selver').
- For each product: open PDP by ext_id; if that fails, search by EAN and
  open the first result; verify PDP by matching Ribakood == EAN.
- Extract consumer brand in this priority:
    1) JSON-LD: Product.brand.name (or brand string)
    2) Spec table label "Kaubamärk" / "Brand"
    3) <meta property="product:brand" content="...">
- No title/name guessing.

Env:
  DATABASE_URL       (required)
  MAX_ITEMS          (default 800)
  HEADLESS           (1|0, default 1)
  REQ_DELAY          (seconds, default 0.25)
  TIMEBOX_SECONDS    (default 1800)
  OVERWRITE_ALL      (1|0, default 0)  -> if 1, overwrite any existing brand
"""

from __future__ import annotations
import os, re, json, time, signal, sys
from contextlib import closing
import psycopg2
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "https://www.selver.ee"

BAD_BRAND_RE = re.compile(r'(http|www\.)|@', re.I)

def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)  # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _is_junk(s: str | None) -> bool:
    if not s: return True
    if s.lower().startswith('e-selveri info'): return True
    if len(s) > 100: return True
    if BAD_BRAND_RE.search(s): return True
    return False

def accept_overlays(page):
    for sel in [
        'button#onetrust-accept-btn-handler',
        'button:has-text("Nõustun")',
        'button:has-text("Accept")',
    ]:
        try:
            b = page.locator(sel).first
            if b.is_visible():
                b.click(timeout=1500)
                break
        except Exception:
            pass

def goto_by_ext_id(page, ext_id: str) -> bool:
    """Return True if navigation succeeded."""
    if not ext_id:
        return False
    s = ext_id.strip()
    if s.startswith("http"):
        url = s
    elif s.startswith("/"):
        url = BASE + s
    elif re.fullmatch(r"\d+", s):
        url = f"{BASE}/p/{s}"
    else:
        url = f"{BASE}/{s}"
    try:
        page.goto(url, timeout=30000, wait_until="domcontentloaded")
        accept_overlays(page)
        return True
    except Exception:
        return False

def goto_by_ean_search(page, ean: str) -> bool:
    """Search by EAN, click first result. Returns True if ended on some PDP."""
    try:
        page.goto(BASE, timeout=30000, wait_until="domcontentloaded")
        accept_overlays(page)
        inp = page.locator('input[placeholder*="Otsi"], input[type="search"]').first
        inp.fill(ean)
        inp.press("Enter")
        # click first product card link
        page.wait_for_selector("a[href^='/' i]", timeout=15000)
        first = page.locator("a[href*='/' i]").first
        href = first.get_attribute("href") or ""
        if href:
            if href.startswith("/"):
                page.goto(BASE + href, timeout=30000, wait_until="domcontentloaded")
            else:
                page.goto(href, timeout=30000, wait_until="domcontentloaded")
            accept_overlays(page)
            return True
    except Exception:
        pass
    return False

def wait_for_pdp_bits(page):
    try:
        page.wait_for_selector(
            'script[type="application/ld+json"], table.ProductAttributes__table',
            timeout=12000
        )
    except PWTimeout:
        pass

def extract_spec_value(page, est_label_variants: list[str]) -> str:
    """Look up a labeled value from th/td or dt/dd."""
    def from_pairs(pairs):
        for k, v in pairs:
            kk = _clean(re.sub(r'<.*?>', ' ', k)).lower()
            if any(lbl in kk for lbl in est_label_variants):
                return _clean(re.sub(r'<.*?>', ' ', v))
        return ""

    try:
        html = page.content()
        # dt/dd
        m = re.findall(r'(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html)
        val = from_pairs(m)
        if val: return val
        # th/td
        m = re.findall(r'(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>', html)
        val = from_pairs(m)
        if val: return val
    except Exception:
        pass
    return ""

def extract_ean_from_pdp(page) -> str:
    return extract_spec_value(page, ["ribakood"])

def extract_brand_from_pdp(page) -> str:
    # 1) JSON-LD
    try:
        for el in page.locator('script[type="application/ld+json"]').all():
            raw = el.text_content() or ""
            if not raw.strip():
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for n in items:
                b = n.get("brand")
                if isinstance(b, dict): b = b.get("name")
                b = _clean(b)
                if b: return b
    except Exception:
        pass

    # 2) Spec labels: Kaubamärk / Brand
    v = extract_spec_value(page, ["kaubamärk", "brand"])
    if v: return v

    # 3) Meta: product:brand
    try:
        val = page.eval_on_selector(
            'meta[property="product:brand"]',
            'el => el ? (el.content || el.getAttribute("content")) : null'
        )
        val = _clean(val or "")
        if val:
            return val
    except Exception:
        pass

    return ""

def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("Missing DATABASE_URL", file=sys.stderr)
        sys.exit(2)

    max_items = int(os.environ.get("MAX_ITEMS", "800"))
    headless = os.environ.get("HEADLESS", "1") == "1"
    req_delay = float(os.environ.get("REQ_DELAY", "0.25"))
    timebox = int(os.environ.get("TIMEBOX_SECONDS", "1800"))
    overwrite_all = os.environ.get("OVERWRITE_ALL", "0") == "1"
    deadline = time.time() + timebox

    # Pick Selver-mapped products that need a brand refresh.
    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id, p.ean, m.ext_id
            FROM products p
            JOIN ext_product_map m ON m.product_id = p.id AND m.source = 'selver'
            WHERE
              -- only items that need work unless OVERWRITE_ALL=1
              (%s = 1) OR
              p.brand IS NULL OR p.brand = '' OR
              p.brand ILIKE 'e-selveri info%%' OR
              length(p.brand) > 100 OR p.brand ~ '(http|www\\.)' OR p.brand ~ '@'
            ORDER BY p.id
            LIMIT %s;
            """,
            (1 if overwrite_all else 0, max_items),
        )
        rows = cur.fetchall()

    if not rows:
        print("Nothing to do.")
        return

    processed = 0
    updated = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        # block heavy 3rd-party requests
        block = [
            "googletagmanager", "google-analytics", "doubleclick",
            "facebook", "fonts.googleapis.com", "use.typekit.net",
        ]
        def route_blocker(route, request):
            u = request.url
            if any(d in u for d in block):
                return route.abort()
            return route.continue_()
        context.route("**/*", route_blocker)
        page = context.new_page()

        for pid, ean, ext_id in rows:
            if time.time() > deadline:
                print("Timebox reached, stopping.")
                break

            ok = goto_by_ext_id(page, (ext_id or "").strip())
            if not ok and ean:
                ok = goto_by_ean_search(page, ean)
            if not ok:
                print(f"[MISS_NAV] id={pid} ext_id={ext_id}")
                time.sleep(req_delay); continue

            wait_for_pdp_bits(page)

            # verify PDP: Ribakood must match (when we know EAN)
            if ean:
                try:
                    ribakood = extract_ean_from_pdp(page)
                    if ribakood and re.sub(r'\D', '', ribakood) != re.sub(r'\D', '', ean):
                        print(f"[SKIP_MISMATCH] id={pid} ean_db={ean} ean_pdp={ribakood}")
                        time.sleep(req_delay); continue
                except Exception:
                    pass

            brand = extract_brand_from_pdp(page)
            brand = _clean(brand)
            processed += 1

            if not brand or _is_junk(brand):
                print(f"[MISS_BRAND] id={pid} ext_id={ext_id}")
                time.sleep(req_delay); continue

            # Write to DB (products + selver_candidates, when present)
            try:
                with closing(psycopg2.connect(dsn)) as conn2, conn2.cursor() as cur2:
                    if overwrite_all:
                        cur2.execute(
                            "UPDATE products SET brand = %s WHERE id = %s",
                            (brand, pid),
                        )
                    else:
                        cur2.execute(
                            """
                            UPDATE products
                               SET brand = %s
                             WHERE id = %s
                               AND (brand IS NULL OR brand = '' OR
                                    brand ILIKE 'e-selveri info%%' OR
                                    length(brand) > 100 OR brand ~ '(http|www\\.)' OR brand ~ '@')
                            """,
                            (brand, pid),
                        )
                    # Helpful backfill for local cache, when we have that row:
                    cur2.execute(
                        """
                        UPDATE selver_candidates
                           SET brand = %s
                         WHERE ext_id = %s
                           AND (brand IS NULL OR brand = '')
                        """,
                        (brand, ext_id),
                    )
                    conn2.commit()
                    updated += 1
                    print(f'[BRAND] id={pid} brand="{brand}"')
            except Exception as e:
                print(f"[DB_ERR] id={pid} err={e}")

            time.sleep(req_delay)

        browser.close()

    print(f"Done. processed={processed} brand_updated={updated}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

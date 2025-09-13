#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (structured fields only).

- Uses ext_product_map (source='selver'):
  • If ext_id is an absolute Selver URL → go direct.
  • If ext_id is 8/13 digits (EAN) → open search and click the first/best tile.
- Extracts brand from Käitleja/Tootja/Valmistaja/Kaubamärk/Brand rows,
  JSON-LD (brand/manufacturer) and meta product:brand. No title guessing.
- “Määrmata” is normalized to “Määramata”.

Env:
  DATABASE_URL / DATABASE_URL_PUBLIC  (required)
  MAX_ITEMS          default 500
  HEADLESS           1|0 (default 1)
  REQ_DELAY          seconds (default 0.25)
  TIMEBOX_SECONDS    default 1200
  OVERWRITE_PRODUCTS 1|0 (default 0)  # if 1, overwrite any existing brand
"""

from __future__ import annotations
import json
import os
import re
import sys
import time
import signal
from contextlib import closing

import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_HOST = "https://www.selver.ee"
SEARCH_URL = BASE_HOST + "/search?q={q}"

IS_EAN = re.compile(r"^\d{8}(\d{5})?$")  # 8 or 13 digits
BRAND_LABELS = re.compile(r"(kaubam[aä]rk|tootja|valmistaja|käitleja|brand)", re.I)

# ---- graceful stop flag (set by SIGINT) ----
_STOP = False
def _on_sigint(signum, frame):
    # Don’t raise SystemExit here; just ask the main loop to stop.
    global _STOP
    _STOP = True
    # Print once; avoid flooding logs during repeated signals.
    try:
        print("SIGINT received → finishing current item and shutting down gracefully…")
    except Exception:
        pass


def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r"[\u2122\u00AE]", "", s)  # ™ ®
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_maarmata(b: str) -> str:
    return "Määramata" if b.lower() == "määrmata" else b


def _looks_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def _looks_searchable_digits(s: str) -> bool:
    return bool(IS_EAN.fullmatch(s))


def _accept_overlays(page):
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button:has-text('Nõustun')",
        "button:has-text('Accept')",
        "[data-testid='uc-accept-all-button']",
        "[aria-label='Accept all']",
    ]:
        try:
            loc = page.locator(sel).first
            if loc and loc.count() > 0 and loc.is_visible():
                loc.click(timeout=800)
                time.sleep(0.12)
        except Exception:
            pass


# Trimmed list; don’t block cookiebot (it sometimes influences layout timing)
BLOCK_SUBSTR = (
    "adobedtm",
    "use.typekit.net",
    "typekit",
    "googletagmanager",
    "google-analytics",
    "doubleclick",
    "facebook.net",
    "newrelic",
    "pingdom",
    "hotjar",
)


def _router(route, request):
    url = (request.url or "").lower()
    if any(s in url for s in BLOCK_SUBSTR):
        return route.abort()
    return route.continue_()


def _looks_like_pdp(page) -> bool:
    try:
        if page.locator("meta[property='og:type'][content='product']").count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.locator(":text-matches('Ribakood|Barcode|Штрихкод','i')").count() > 0:
            return True
    except Exception:
        pass
    return False


# ---------- robust PDP-href recognition ----------
def _looks_like_pdp_href(href: str) -> bool:
    if not href:
        return False
    href = href.split("?", 1)[0].split("#", 1)[0]
    if href.startswith("http"):
        # Only allow selver.ee links
        if "selver.ee" not in href:
            return False
    path = href
    if href.startswith("http"):
        # crude path strip
        try:
            path = "/" + href.split("selver.ee", 1)[1].split("/", 1)[1]
        except Exception:
            path = "/"
    if not path.startswith("/"):
        return False

    # obvious non-PDP paths
    bad_prefixes = (
        "/search",
        "/konto",
        "/login",
        "/registreeru",
        "/logout",
        "/kliendimangud",
        "/kauplused",
        "/selveekspress",
        "/tule-toole",
        "/uudised",
        "/kinkekaardid",
        "/selveri-kook",
        "/kampaania",
        "/retseptid",
        "/app",
        "/vabad-",  # jobs
    )
    if any(path.startswith(p) for p in bad_prefixes):
        return False

    if path.startswith("/toode/") or path.startswith("/e-selver/toode/"):
        return True

    segs = [s for s in path.split("/") if s]
    if not segs:
        return False
    last = segs[-1]
    # slug with at least one hyphen (most PDPs)
    if "-" in last:
        return True
    return False


def _candidate_anchors(page):
    sels = [
        "[data-testid='product-grid'] a[href]:visible",
        "[data-testid='product-list'] a[href]:visible",
        ".product-card a[href]:visible",
        "article a[href]:visible",
        "main a[href]:visible",
    ]
    nodes = []
    for s in sels:
        try:
            nodes.extend(page.locator(s).all())
        except Exception:
            pass
    return nodes[:300]


def _open_first_pdp_from_search(page) -> bool:
    # Wait briefly for grid/list to render
    try:
        page.wait_for_selector("[data-testid='product-grid'], .product-list, article a[href]", timeout=5000)
    except Exception:
        pass
    _accept_overlays(page)

    # Prefer anchors that look like PDP links
    hrefs = []
    for a in _candidate_anchors(page):
        try:
            h = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if _looks_like_pdp_href(h):
                hrefs.append(h)
        except Exception:
            continue

    if not hrefs:
        return False

    href = hrefs[0]
    if href.startswith("/"):
        href = BASE_HOST + href
    try:
        page.goto(href, timeout=15000, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=2000)
        except Exception:
            pass
        return _looks_like_pdp(page) or page.locator("h1").count() > 0
    except Exception:
        return False


def _navigate(page, ext_id: str) -> tuple[bool, str]:
    """(ok, current_url). ext_id is absolute URL or searchable digits (EAN)."""
    try:
        if _looks_url(ext_id):
            page.goto(ext_id, timeout=20000, wait_until="domcontentloaded")
            _accept_overlays(page)
            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass
            return True, page.url

        if _looks_searchable_digits(ext_id):
            page.goto(SEARCH_URL.format(q=ext_id), timeout=20000, wait_until="domcontentloaded")
            _accept_overlays(page)
            try:
                page.wait_for_load_state("networkidle", timeout=1500)
            except Exception:
                pass
            if _open_first_pdp_from_search(page):
                return True, page.url
            return False, page.url

        # fallback (site-relative or slug)
        if ext_id.startswith("/"):
            page.goto(BASE_HOST + ext_id, timeout=20000, wait_until="domcontentloaded")
        else:
            page.goto(f"{BASE_HOST}/{ext_id}", timeout=20000, wait_until="domcontentloaded")
        _accept_overlays(page)
        return True, page.url
    except PWTimeout:
        return False, ""
    except Exception:
        return False, ""


def _extract_brand(page) -> str:
    # 0) tables / definition lists
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
                txt = _clean(loc.inner_text(timeout=700) or "")
                if txt:
                    return _norm_maarmata(txt)
    except Exception:
        pass

    # 1) JSON-LD
    try:
        scripts = page.locator('script[type="application/ld+json"]')
        for i in range(scripts.count()):
            try:
                raw = scripts.nth(i).inner_text(timeout=600) or ""
                data = json.loads(raw)
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                if not isinstance(n, dict):
                    continue
                b = n.get("brand")
                if isinstance(b, dict):
                    b = b.get("name")
                b = _clean(b or "")
                if b:
                    return _norm_maarmata(b)
                m = n.get("manufacturer")
                if isinstance(m, dict):
                    m = m.get("name")
                m = _clean(m or "")
                if m:
                    return _norm_maarmata(m)
    except Exception:
        pass

    # 2) regex over HTML pairs
    try:
        html = page.content()
        for k, v in re.findall(r"(?is)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", html):
            if BRAND_LABELS.search(re.sub(r"<.*?>", " ", k)):
                b = _clean(re.sub(r"<.*?>", " ", v))
                if b:
                    return _norm_maarmata(b)
        for k, v in re.findall(r"(?is)<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>", html):
            if BRAND_LABELS.search(re.sub(r"<.*?>", " ", k)):
                b = _clean(re.sub(r"<.*?>", " ", v))
                if b:
                    return _norm_maarmata(b)
    except Exception:
        pass

    # 3) meta
    try:
        el = page.locator('meta[property="product:brand"]').first
        if el and el.count() > 0:
            val = el.get_attribute("content", timeout=400) or ""
            b = _clean(val)
            if b:
                return _norm_maarmata(b)
    except Exception:
        pass

    return ""


# ----------------- DB helpers -----------------
def _conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_PUBLIC")
    if not dsn:
        print("Missing DATABASE_URL / DATABASE_URL_PUBLIC", file=sys.stderr)
        sys.exit(2)
    return psycopg2.connect(dsn)


GARBAGE_BRAND_SQL = """
(p.brand IS NULL OR p.brand = ''
 OR p.brand ILIKE 'e-selveri info%%'
 OR length(p.brand) > 100
 OR p.brand ~* '(http|www\\.)'
 OR p.brand ~* '@')
"""


def pick_rows(conn, limit: int):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (p.id)
                   p.id AS product_id,
                   p.name,
                   COALESCE(NULLIF(p.brand,''), '')  AS old_brand,
                   COALESCE(NULLIF(p.amount,''), '') AS amount,
                   m.ext_id
              FROM products p
              JOIN ext_product_map m ON m.product_id = p.id
             WHERE m.source = 'selver'
               AND {GARBAGE_BRAND_SQL}
             ORDER BY p.id
             LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        if rows:
            return rows

        cur.execute(
            """
            SELECT DISTINCT ON (p.id)
                   p.id AS product_id,
                   p.name,
                   COALESCE(NULLIF(p.brand,''), '')  AS old_brand,
                   COALESCE(NULLIF(p.amount,''), '') AS amount,
                   m.ext_id
              FROM products p
              JOIN ext_product_map m ON m.product_id = p.id
             WHERE m.source = 'selver'
             ORDER BY p.id
             LIMIT %s;
            """,
            (limit,),
        )
        return cur.fetchall()


def persist_brand(conn, product_id: int, brand: str, overwrite: bool) -> bool:
    brand = _norm_maarmata(_clean(brand))
    if not brand:
        return False
    with conn.cursor() as cur:
        if overwrite:
            cur.execute("UPDATE products SET brand = %s WHERE id = %s;", (brand, product_id))
        else:
            cur.execute(
                f"UPDATE products SET brand = %s WHERE id = %s AND {GARBAGE_BRAND_SQL};",
                (brand, product_id),
            )
    conn.commit()
    return True


# ----------------- main -----------------
def main():
    MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
    HEADLESS = os.getenv("HEADLESS", "1") == "1"
    REQ_DELAY = float(os.getenv("REQ_DELAY", "0.25"))
    TIMEBOX = int(os.getenv("TIMEBOX_SECONDS", "1200"))
    OVERWRITE = os.getenv("OVERWRITE_PRODUCTS", "0") in ("1", "true", "yes")

    deadline = time.time() + TIMEBOX

    with closing(_conn()) as conn:
        rows = pick_rows(conn, MAX_ITEMS)

    if not rows:
        print("Nothing to do.")
        return

    processed = 0
    found = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            locale="et-EE",
            extra_http_headers={"Accept-Language": "et-EE,et;q=0.9,en;q=0.8,ru;q=0.7"},
            viewport={"width": 1280, "height": 900},
        )
        ctx.route("**/*", _router)
        page = ctx.new_page()
        page.set_default_timeout(8000)
        page.set_default_navigation_timeout(20000)

        for r in rows:
            # honor timebox and external stop
            if time.time() > deadline or _STOP:
                print("Timebox/stop reached, stopping.")
                break

            pid = r["product_id"]
            ext_id = str(r.get("ext_id") or "")
            if not ext_id:
                print(f"[MISS_BRAND] product_id={pid} ext_id=<empty>")
                continue

            ok, at = _navigate(page, ext_id)
            if not ok:
                print(f"[MISS_BRAND] product_id={pid} nav failed (ext_id={ext_id})")
                time.sleep(REQ_DELAY)
                continue

            b = _extract_brand(page)
            processed += 1
            if not b:
                print(f"[MISS_BRAND_EMPTY] product_id={pid} url={at or ext_id}")
                time.sleep(REQ_DELAY)
                continue

            # persist
            with closing(_conn()) as conn2:
                try:
                    if persist_brand(conn2, pid, b, OVERWRITE):
                        found += 1
                        print(f"[BRAND] product_id={pid} brand=\"{b}\"")
                except Exception as e:
                    print(f"[DB_ERR] product_id={pid} err={e}")

            time.sleep(REQ_DELAY)

        try:
            browser.close()
        except Exception:
            pass

    print(f"Done. processed={processed} brand_found={found}")


if __name__ == "__main__":
    # graceful SIGINT (from the workflow timeout) → no tracebacks / pipe errors
    signal.signal(signal.SIGINT, _on_sigint)
    try:
        main()
    except KeyboardInterrupt:
        # Just in case (rare), also exit quietly on Ctrl-C.
        pass

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver brand enrichment (structured sources only).

Worklist: products joined with ext_product_map (source='selver').
Navigation: direct PDP when ext_id looks like a Selver product URL, else
EAN search first (and name/brand/amount fallback if needed).

Extraction sources (in order):
  1) Facts table row: Käitleja / Tootja / Valmistaja / Kaubamärk / Brand
  2) JSON-LD: brand/manufacturer
  3) <meta property="product:brand">

Env:
  DATABASE_URL        (required)
  MAX_ITEMS           (default 500)
  HEADLESS            (1|0, default 1)
  REQ_DELAY           (seconds, default 0.25)
  TIMEBOX_SECONDS     (default 1200)
  OVERWRITE_PRODUCTS  (1|0, default 0)  # overwrite even non-empty brands
"""

from __future__ import annotations
import os, re, json, time, signal, sys
from contextlib import closing
from urllib.parse import urlparse, quote_plus

import psycopg2
from playwright.sync_api import sync_playwright

BASE = "https://www.selver.ee"
SEARCH = BASE + "/search?q={q}"

# ---------------- brand normalization ----------------
LABEL_RX = r"(Käitleja|Tootja|Valmistaja|Kaubamärk|Brand)"
LABELS_RE = re.compile(LABEL_RX, re.I)
URL_OR_EMAIL = re.compile(r'(https?://|www\.)|@', re.I)

PLACEHOLDER = {"täpsustamata", "puudub", "unknown", "n/a", "na", "none", "-", "—", "–"}

def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r'[\u2122\u00AE]', '', s)     # ™ ®
    s = re.sub(r'\s+', ' ', s).strip()
    if re.search(r'\b(\d+(\s)?(ml|l|g|kg|tk|pcs))\b', s, re.I):
        return ""
    return s

def _norm_maaramata(b: str) -> str:
    t = (b or "").strip()
    if not t: return ""
    low = t.lower()
    if low in {"määramata", "määrmata"}:  # site shows both
        return "Määramata"
    if low.replace("ä","a") in {"maaramata", "maarmata"}:
        return "Määramata"
    if re.fullmatch(r"m[äa]ära?m?ata", low):
        return "Määramata"
    return t

def _is_bad(b: str) -> bool:
    if not b or len(b) > 120: return True
    if URL_OR_EMAIL.search(b): return True
    bl = b.strip().lower()
    if bl in PLACEHOLDER: return True
    if re.fullmatch(r'[-–—\s]+', b): return True
    return False

def _accept_or_empty(raw: str) -> str:
    b = _norm_maaramata(_clean(raw))
    if b.lower() == "määramata":  # keep explicit “Määramata”
        return "Määramata"
    return "" if _is_bad(b) else b

# ---------------- navigation helpers (from EAN probe) ----------------
def looks_like_pdp_href(href: str) -> bool:
    if not href:
        return False
    href = href.split("?", 1)[0].split("#", 1)[0]
    if href.startswith("http"):
        try:
            href = (urlparse(href).path) or "/"
        except Exception:
            return False
    if not href.startswith("/"):
        return False

    # obvious non-product sections
    bad = (
        "/search", "/konto", "/login", "/registreeru", "/logout",
        "/kliendimangud", "/kauplused", "/selveekspress", "/tule-toole",
        "/uudised", "/kinkekaardid", "/selveri-kook", "/kampaania",
        "/retseptid", "/app",
    )
    if any(href.startswith(p) for p in bad):
        return False

    if href.startswith("/toode/") or href.startswith("/e-selver/toode/"):
        return True

    segs = [s for s in href.split("/") if s]
    if not segs:
        return False
    last = segs[-1]
    return "-" in last and (any(ch.isdigit() for ch in last) or re.search(r"(?:^|[-_])(kg|g|l|ml|cl|dl|tk)$", last))

def kill_consents(page):
    for sel in [
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "#CybotCookiebotDialogBodyButtonAccept",
        "button#onetrust-accept-btn-handler",
        "button:has-text('Nõustun')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "[data-testid='uc-accept-all-button']",
        "[aria-label='Accept all']",
    ]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=600)
                page.wait_for_timeout(120)
        except Exception:
            pass
    try: page.keyboard.press("Escape")
    except Exception: pass

def is_search_page(page) -> bool:
    try:
        url = page.url
        if "/search?" in url: return True
        if page.locator("text=Otsingu:").count() > 0: return True
    except Exception:
        pass
    return False

def _product_tile_links(page):
    sels = [
        "[data-testid='product-grid'] a[href]:visible",
        "[data-testid='product-list'] a[href]:visible",
        ".product-list a[href]:visible",
        ".product-grid a[href]:visible",
        ".product-card a[href]:visible",
        "article a[href]:visible",
        "a[href^='/']:visible",
    ]
    nodes = []
    for s in sels:
        try:
            nodes.extend(page.locator(s).all())
        except Exception:
            pass
    return nodes[:200]

def _open_first_product(page) -> bool:
    for a in _product_tile_links(page):
        try:
            href = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if not looks_like_pdp_href(href): continue
            a.click(timeout=1200)
            try: page.wait_for_selector("h1", timeout=2500)
            except Exception: pass
            return True
        except Exception:
            continue
    return False

def _open_best_hit(page, qname: str, brand: str, amount: str) -> bool:
    # rough scoring like in EAN probe
    def score(t: str) -> float:
        s = 0.0; tt = (t or "").lower()
        for tok in set(re.findall(r"\w+", (qname or "").lower())):
            if len(tok) >= 3 and tok in tt: s += 1.0
        if brand and brand.lower() in tt: s += 2.0
        if amount and amount.lower() in tt: s += 1.0
        return s
    best, best_s = None, -1.0
    for a in _product_tile_links(page):
        try:
            href = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if not looks_like_pdp_href(href): continue
            sc = score(a.inner_text() or "")
            if sc > best_s: best, best_s = a, sc
        except Exception: continue
    if not best: return False
    try:
        best.click(timeout=1200)
        try: page.wait_for_selector("h1", timeout=2500)
        except Exception: pass
        return True
    except Exception:
        return False

def goto_search_and_open(page, query: str, qname: str, brand: str, amount: str) -> bool:
    try:
        page.goto(SEARCH.format(q=quote_plus(query)), timeout=15000, wait_until="domcontentloaded")
        try: page.wait_for_load_state("networkidle", timeout=2500)
        except Exception: pass
        kill_consents(page)
        # try best, then first
        if _open_best_hit(page, qname, brand, amount) or _open_first_product(page):
            return True
    except Exception:
        pass
    return False

def direct_is_ok(url: str) -> bool:
    if not url: return False
    try:
        u = urlparse(url)
        if not (u.scheme and u.netloc and u.hostname and u.hostname.endswith("selver.ee")):
            return False
        return looks_like_pdp_href(u.path or "/")
    except Exception:
        return False

# --------------- extraction ---------------
def wait_for_pdp(page):
    try:
        page.wait_for_selector(
            'table.ProductAttributes__table, script[type="application/ld+json"], h1',
            timeout=7000
        )
    except Exception:
        pass

def brand_from_table(page) -> str:
    # (dl/th/td) variants
    pairs = [
        (f"dt:has(:text-matches('{LABEL_RX}','i'))", "dd"),
        (f"tr:has(td:has(:text-matches('{LABEL_RX}','i')))", "td"),
        (f"tr:has(th:has(:text-matches('{LABEL_RX}','i')))", "td"),
        (f"th:has(:text-matches('{LABEL_RX}','i'))", "xpath=following-sibling::*[1]"),
        (f"dt:has-text('Käitleja')", "xpath=following-sibling::*[1]"),
    ]
    for k_sel, v_sel in pairs:
        try:
            k = page.locator(k_sel).first
            if not k or k.count() == 0:
                continue
            # try value cell, sibling, and container text
            zones = [k.locator(v_sel), k, k.locator("xpath=..")]
            for z in zones:
                try:
                    t = (z.inner_text(timeout=800) or "").strip()
                    if not t: continue
                    # value might include the label too; strip left label if present
                    t = re.sub(LABELS_RE, "", t, flags=re.I).strip(": ").strip()
                    b = _accept_or_empty(t)
                    if b: return b
                except Exception:
                    pass
        except Exception:
            pass
    return ""

def brand_from_jsonld(page) -> str:
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
                if isinstance(b, dict): b = b.get('name')
                b = _accept_or_empty(b or "")
                if b: return b
                m = n.get('manufacturer')
                if isinstance(m, dict): m = m.get('name')
                m = _accept_or_empty(m or "")
                if m: return m
    except Exception:
        pass
    return ""

def brand_from_meta(page) -> str:
    try:
        val = page.eval_on_selector(
            'meta[property="product:brand"]',
            'el => el ? (el.content || el.getAttribute("content")) : null'
        )
        b = _accept_or_empty(val or "")
        if b: return b
    except Exception:
        pass
    return ""

def extract_brand(page) -> str:
    b = brand_from_table(page)
    if b: return b
    b = brand_from_jsonld(page)
    if b: return b
    return brand_from_meta(page)

# --------------- DB filter ---------------
BAD_BRAND_SQL = """
  (p.brand ILIKE 'e-selveri info%%'
   OR p.brand ~* '(http|www\\.)'
   OR p.brand ~* '@'
   OR length(p.brand) > 100)
"""

# --------------- main ---------------
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

    with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id,
                   p.name,
                   COALESCE(NULLIF(p.brand,''), '')  AS cur_brand,
                   COALESCE(NULLIF(p.amount,''), '') AS amount,
                   p.ean::text                       AS ean,
                   COALESCE(m.ext_id::text,'')       AS ext_id
              FROM products p
              JOIN ext_product_map m ON m.product_id = p.id AND m.source = 'selver'
             WHERE COALESCE(p.ean::text,'') <> ''
               AND (
                     %s = TRUE
                     OR p.brand IS NULL OR p.brand = '' OR {BAD_BRAND_SQL}
                   )
             ORDER BY p.id
             LIMIT %s
        """, (overwrite, max_items))
        rows = cur.fetchall()

    if not rows:
        print("Nothing to do.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(
            locale="et-EE",
            extra_http_headers={"Accept-Language":"et-EE,et;q=0.9,en;q=0.8"},
            viewport={"width":1280,"height":880},
        )
        # stub noisy analytics + swallow SPA errors
        ctx.add_init_script("""
            (function(){
              try { window.fbq = window.fbq || function(){}; } catch(e){}
              try { window.dataLayer = window.dataLayer || []; } catch(e){}
              window.addEventListener('error', function(e){try{e.stopImmediatePropagation&&e.stopImmediatePropagation();}catch(_){}} , true);
              window.addEventListener('unhandledrejection', function(e){try{e.preventDefault&&e.preventDefault();}catch(_){}} , true);
            })();
        """)
        # block heavy 3rd parties (keeps selver.ee only)
        ctx.route("**/*", lambda route, req:
            route.abort() if any(s in req.url for s in (
                "googletagmanager","google-analytics","doubleclick",
                "facebook","cookiebot","hotjar","newrelic","pingdom",
                "use.typekit.net","fonts.googleapis.com"
            )) else route.continue_()
        )
        page = ctx.new_page()
        page.set_default_timeout(8000)
        page.set_default_navigation_timeout(20000)

        updated = 0
        processed = 0

        with closing(psycopg2.connect(dsn)) as wconn, wconn.cursor() as wcur:
            for (pid, name, cur_brand, amount, ean, ext_id) in rows:
                if time.time() > deadline:
                    print("Timebox reached, stopping."); break

                # --- navigate
                ok = False
                if ext_id and ext_id.startswith("http") and direct_is_ok(ext_id):
                    try:
                        page.goto(ext_id, timeout=15000, wait_until="domcontentloaded")
                        kill_consents(page)
                        ok = True
                    except Exception:
                        ok = False
                if not ok and ean:
                    ok = goto_search_and_open(page, ean, name or "", cur_brand or "", amount or "")
                if not ok and name:
                    q = " ".join(x for x in [name, cur_brand, amount] if x)
                    ok = goto_search_and_open(page, q, name or "", cur_brand or "", amount or "")

                if not ok:
                    print(f"[MISS_BRAND] product_id={pid} nav failed (ext_id={ext_id}, ean={ean})")
                    time.sleep(req_delay); continue

                wait_for_pdp(page)
                b = extract_brand(page)
                processed += 1

                if not b:
                    print(f"[MISS_BRAND_EMPTY] product_id={pid} url={page.url}")
                    time.sleep(req_delay); continue

                try:
                    if overwrite:
                        wcur.execute("UPDATE products SET brand = %s WHERE id = %s", (b, pid))
                    else:
                        wcur.execute(
                            f"UPDATE products SET brand = %s WHERE id = %s AND (brand IS NULL OR brand = '' OR {BAD_BRAND_SQL})",
                            (b, pid)
                        )
                    if wcur.rowcount > 0:
                        updated += 1
                        print(f"[OK] id={pid} ← brand “{b}”")
                    else:
                        print(f"[SKIP] id={pid} brand already OK")
                    wconn.commit()
                except Exception as e:
                    wconn.rollback()
                    print(f"[DB_ERR] id={pid} err={e}")

                time.sleep(req_delay)

        try: browser.close()
        except Exception: pass
        print(f"Done. processed={processed} updated={updated}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
selver_probe_ean_pw.py
Purpose: Backfill missing EANs (and SKU if present) for Selver products, FAST.

Key speedups in this version:
- Uses direct Selver PDP URLs from ext_product_map (no search needed).
- Falls back to search flow only when no mapping URL exists.
- Simple multi-process concurrency (each worker runs its own Chromium).

ENV VARS (sane defaults):
  DATABASE_URL          Postgres URL (required)
  HEADLESS=1            Run Chromium headless
  BATCH=1000            Total products to attempt this run
  CONCURRENCY=8         Number of parallel browser workers (6–10 is good)
  REQ_DELAY=0.30        Small think-time between items per worker
  OVERWRITE_BAD_EANS=0  If 1/true: allow replacing bad-looking existing EANs

Run:
  CONCURRENCY=8 BATCH=1200 REQ_DELAY=0.30 python scripts/selver_probe_ean_pw.py
"""

from __future__ import annotations
import os
import re
import sys
import time
import json
import math
import random
from typing import Optional, List, Tuple
from multiprocessing import Process, current_process
from urllib.parse import urlparse, quote_plus

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SELVER_BASE = "https://www.selver.ee"
SEARCH_URL = SELVER_BASE + "/search?q={q}"

HEADLESS = os.getenv("HEADLESS", "1") == "1"
BATCH = int(os.getenv("BATCH", "1000"))
CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "1")))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.30"))
OVERWRITE_BAD = os.getenv("OVERWRITE_BAD_EANS", "0").lower() in ("1", "true", "yes")
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_PUBLIC")

JSON_EAN = re.compile(r'"(?:gtin14|gtin13|gtin|ean|barcode|sku)"\s*:\s*"(?P<d>\d{8,14})"', re.I)

# SKU patterns & labels
SKU_RX = re.compile(r"\b(?:T\d{8,12}|[A-Z]{1,3}\d{6,12}|[A-Z0-9]{3,}-[A-Z0-9]{3,})\b")
SKU_LABEL_RX = r"(?:SKU|Tootekood|Toote\s*kood|Artikkel|Артикул|Код\s*товара|Код)"


# ----------------- small utils -----------------
def norm_ean(s: Optional[str]) -> Optional[str]:
    """Normalize varied GTINs to EAN-13/EAN-8.
    - Accepts GTIN-14 (drops leading 0/1 commonly used as logistic indicator)
    - Accepts UPC-A (12) and pads to EAN-13 with leading zero
    """
    if not s:
        return None
    d = re.sub(r"\D", "", s)
    if not d:
        return None
    if len(d) == 14 and d[0] in "01":
        d = d[1:]
    if len(d) == 12:
        d = "0" + d
    return d if len(d) in (8, 13) else None


def looks_bogus_ean(e: Optional[str]) -> bool:
    if not e:
        return False
    return bool(re.fullmatch(r'(\d)\1{7}', e)) or e in {"00000000", "0000000000000"}


def is_bad_ean_python(e: Optional[str]) -> bool:
    if e is None or e == "":
        return True
    if not e.isdigit() or len(e) not in (8, 13):
        return True
    if looks_bogus_ean(e):
        return True
    return False


def connect() -> PGConn:
    if not DB_URL:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(2)
    return psycopg2.connect(DB_URL)


def table_exists(conn: PGConn, tbl: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_name = %s
            );
        """,
            (tbl,),
        )
        return cur.fetchone()[0]


def column_exists(conn: PGConn, tbl: str, col: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
          SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
          );
        """,
            (tbl, col),
        )
        return cur.fetchone()[0]


# ----------------- batch pick -----------------
def pick_batch(conn: PGConn, limit: int):
    """
    Prefer Selver PDP URLs from ext_product_map → fastest path to EAN.
    Fallback to 'search by name' picks if no mapping exists.
    """
    bad_sql = """
        (p.ean !~ '^[0-9]+$' OR length(p.ean) NOT IN (8,13)
         OR p.ean ~ '^([0-9])\\1{7}$'
         OR p.ean IN ('00000000','0000000000000'))
    """
    where_target = (
        "(p.ean IS NULL OR p.ean = '')"
        if not OVERWRITE_BAD
        else f"(p.ean IS NULL OR p.ean = '' OR {bad_sql})"
    )

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 1) Fast path: Selver mapping URL exists
        cur.execute(
            f"""
          SELECT DISTINCT ON (p.id)
                 p.id AS product_id,
                 p.name,
                 COALESCE(NULLIF(p.brand,''), '')  AS brand,
                 COALESCE(NULLIF(p.amount,''), '') AS amount,
                 m.ext_id                          AS selver_url
          FROM products p
          JOIN ext_product_map m ON m.product_id = p.id
          WHERE m.source='selver'
            AND m.ext_id LIKE 'http%%'
            AND {where_target}
          ORDER BY p.id
          LIMIT %s;
        """,
            (limit,),
        )
        rows = cur.fetchall()
        if rows:
            return rows

        # 2) Fallback: old flow (search by name/brand/amount)
        cur.execute(
            f"""
          SELECT DISTINCT p.id AS product_id, p.name,
                 COALESCE(NULLIF(p.brand,''), '') AS brand,
                 COALESCE(NULLIF(p.amount,''), '') AS amount,
                 NULL::text AS selver_url
          FROM products p
          JOIN prices pr ON pr.product_id = p.id
          JOIN stores s  ON s.id = pr.store_id
          WHERE s.chain = 'Selver'
            AND {where_target}
          ORDER BY p.id
          LIMIT %s;
        """,
            (limit,),
        )
        return cur.fetchall()


# ----------------- DB writes -----------------
def update_success(conn: PGConn, product_id: int, ean: str, sku: Optional[str] = None) -> str:
    try:
        with conn.cursor() as cur:
            if looks_bogus_ean(ean):
                if table_exists(conn, "selver_ean_backfill_queue"):
                    cur.execute(
                        """
                      UPDATE selver_ean_backfill_queue
                         SET attempts = attempts + 1,
                             last_error = %s,
                             updated_at = now()
                       WHERE product_id = %s;
                    """,
                        (f"bogus EAN {ean}", product_id),
                    )
                conn.commit()
                return "BOGUS_CANDIDATE"

            cur.execute("SELECT ean FROM products WHERE id = %s;", (product_id,))
            prev = (cur.fetchone() or [None])[0]

            if prev:
                if prev == ean:
                    if table_exists(conn, "selver_ean_backfill_queue"):
                        cur.execute(
                            """
                          UPDATE selver_ean_backfill_queue
                             SET attempts = attempts + 1,
                                 last_error = NULL,
                                 updated_at = now()
                           WHERE product_id = %s;
                        """,
                            (product_id,),
                        )
                    conn.commit()
                    return "EXISTS"
                if not OVERWRITE_BAD:
                    if table_exists(conn, "selver_ean_backfill_queue"):
                        cur.execute(
                            """
                          UPDATE selver_ean_backfill_queue
                             SET attempts = attempts + 1,
                                 last_error = %s,
                                 updated_at = now()
                           WHERE product_id = %s;
                        """,
                            (f"has existing EAN {prev}, overwrite disabled", product_id),
                        )
                    conn.commit()
                    return "SKIP_PRESENT"
                if not is_bad_ean_python(prev):
                    if table_exists(conn, "selver_ean_backfill_queue"):
                        cur.execute(
                            """
                          UPDATE selver_ean_backfill_queue
                             SET attempts = attempts + 1,
                                 last_error = %s,
                                 updated_at = now()
                           WHERE product_id = %s;
                        """,
                            (f"existing EAN {prev} not considered bad", product_id),
                        )
                    conn.commit()
                    return "SKIP_NOT_BAD"

            cur.execute(
                "SELECT id FROM products WHERE ean = %s AND id <> %s LIMIT 1;",
                (ean, product_id),
            )
            dup = cur.fetchone()
            if dup:
                if table_exists(conn, "selver_ean_backfill_queue"):
                    cur.execute(
                        """
                      UPDATE selver_ean_backfill_queue
                         SET attempts = attempts + 1,
                             last_error = %s,
                             updated_at = now()
                       WHERE product_id = %s;
                    """,
                        (f"duplicate EAN {ean} already on product {dup[0]}", product_id),
                    )
                conn.commit()
                return "DUP_FOUND"

            if sku and column_exists(conn, "products", "sku"):
                cur.execute(
                    "UPDATE products SET ean = %s, sku = COALESCE(%s, sku) WHERE id = %s;",
                    (ean, sku, product_id),
                )
            else:
                cur.execute("UPDATE products SET ean = %s WHERE id = %s;", (ean, product_id))

            if table_exists(conn, "selver_ean_backfill_queue"):
                cur.execute(
                    """
                  UPDATE selver_ean_backfill_queue
                     SET attempts = attempts + 1,
                         last_error = NULL,
                         updated_at = now()
                   WHERE product_id = %s;
                """,
                    (product_id,),
                )
        conn.commit()
        return "OVERWRITE" if prev else "OK"
    except Exception:
        conn.rollback()
        raise


def update_failure(conn: PGConn, product_id: int, err: str):
    try:
        if table_exists(conn, "selver_ean_backfill_queue"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                  UPDATE selver_ean_backfill_queue
                     SET attempts = attempts + 1,
                         last_error = LEFT(%s, 500),
                         updated_at = now()
                   WHERE product_id = %s;
                """,
                    (err, product_id),
                )
        conn.commit()
    except Exception:
        conn.rollback()


# ----------------- page helpers -----------------
def _first_text(page, selectors: List[str], timeout_ms: int = 2000) -> Optional[str]:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                t = loc.inner_text(timeout=timeout_ms)
                t = (t or "").strip()
                if t:
                    return t
        except Exception:
            pass
    return None


def _meta_content(page, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el and el.count() > 0:
                v = el.get_attribute("content", timeout=800)
                v = (v or "").strip()
                if v:
                    return v
        except Exception:
            pass
    return None


def looks_like_pdp(page) -> bool:
    try:
        og_type = _meta_content(page, ["meta[property='og:type']"]) or ""
        if og_type.lower() == "product":
            return True
    except Exception:
        pass
    try:
        if page.locator("meta[itemprop='gtin13'], meta[itemprop='gtin'], meta[itemprop='sku']").count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.locator(":text-matches('Ribakood|Штрихкод|Barcode', 'i')").count() > 0:
            return True
    except Exception:
        pass
    return False


def score_hit(qname: str, brand: str, amount: str, text: str) -> float:
    s = 0.0
    t = (text or "").lower()
    for tok in set(re.findall(r"\w+", (qname or "").lower())):
        if len(tok) >= 3 and tok in t:
            s += 1.0
    if brand and brand.lower() in t:
        s += 2.0
    if amount and amount.lower() in t:
        s += 1.0
    return s


def handle_age_gate(page):
    try:
        if page.locator(":text-matches('vähemalt\\s*18|at\\s*least\\s*18|18\\+|18\\s*years|18\\s*лет', 'i')").count() == 0:
            return
        for sel in [
            "button:has-text('Olen vähemalt 18')",
            "button:has-text('Jah')",
            "a:has-text('Jah')",
            "button:has-text('ENTER')",
            "button:has-text('Yes')",
            "button:has-text('Да')",
        ]:
            try:
                btn = page.locator(sel).first
                if btn and btn.count() > 0 and btn.is_visible():
                    btn.click(timeout=1500)
                    time.sleep(0.2)
                    break
            except Exception:
                pass
    except Exception:
        pass


def kill_consents_and_overlays(page):
    for sel in [
        "button:has-text('Nõustun')",
        "button:has-text('Luba kõik')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "[data-testid='uc-accept-all-button']",
        "[aria-label='Accept all']",
    ]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=800)
                time.sleep(0.2)
        except Exception:
            pass
    try:
        handle_age_gate(page)
    except Exception:
        pass
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


# --- Request router to reduce 3rd-party noise ---
BLOCK_SUBSTR = (
    "adobedtm",
    "googletagmanager",
    "google-analytics",
    "doubleclick",
    "facebook.net",
    "newrelic",
    "pingdom",
    "cookiebot",
    "hotjar",
)


def _router(route, request):
    try:
        url = request.url.lower()
        if any(s in url for s in BLOCK_SUBSTR):
            return route.abort()
        if "service_worker" in url or "sw_iframe" in url:
            return route.abort()
    except Exception:
        pass
    return route.continue_()


# --- PDP-href recognizer ---
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
    )
    if any(href.startswith(p) for p in bad_prefixes):
        return False

    if href.startswith("/toode/") or href.startswith("/e-selver/toode/"):
        return True

    segs = [s for s in href.split("/") if s]
    if not segs:
        return False
    last = segs[-1]
    if "-" in last and (any(ch.isdigit() for ch in last) or re.search(r"(?:^|[-_])(kg|g|l|ml|cl|dl|tk)$", last)):
        return True

    return False


def is_search_page(page) -> bool:
    try:
        url = page.url
        if "/search?" in url:
            return True
        if any(
            seg in url
            for seg in (
                "/eritooted/",
                "/puu-ja-koogiviljad/",
                "/liha-ja-kalatooted/",
                "/piimatooted",
                "/juustud",
                "/leivad",
                "/valmistoidud",
                "/joogid",
                "/magusad-ja-snackid",
                "/maitseained",
                "/kodukeemia",
            )
        ):
            return True
        if page.locator("text=Otsingu:").count() > 0:
            return True
    except Exception:
        pass
    return False


def _candidate_anchors(page):
    selectors = [
        "[data-testid='product-grid'] a[href]:visible",
        "[data-testid='product-list'] a[href]:visible",
        ".product-list a[href]:visible",
        ".product-grid a[href]:visible",
        ".product-card a[href]:visible",
        "article a[href]:visible",
        "[data-href]:visible",
        "a[href^='/']:visible",
    ]
    nodes = []
    for sel in selectors:
        try:
            nodes.extend(page.locator(sel).all())
        except Exception:
            pass
    return nodes[:200]


def best_search_hit(page, qname: str, brand: str, amount: str) -> Optional[str]:
    links = _candidate_anchors(page)
    scored: List[Tuple[str, float]] = []
    for a in links:
        try:
            href = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if not looks_like_pdp_href(href):
                continue
            txt = a.inner_text() or ""
            scored.append((href, score_hit(qname, brand, amount, txt)))
        except Exception:
            continue
    if not scored:
        return None
    scored.sort(key=lambda x: x[1], reverse=True)
    href = scored[0][0]
    return SELVER_BASE + href if href.startswith("/") else href


def _click_best_tile(page, name: str, brand: str, amount: str) -> bool:
    links = _candidate_anchors(page)
    best = None
    best_score = -1.0
    for a in links:
        try:
            href = a.get_attribute("href") or a.get_attribute("data-href") or ""
            if not looks_like_pdp_href(href):
                continue
            txt = a.inner_text() or ""
            sc = score_hit(name, brand, amount, txt)
            if sc > best_score:
                best, best_score = a, sc
        except Exception:
            continue
    if not best:
        return False
    try:
        best.click(timeout=4000)
        try:
            page.wait_for_selector("h1", timeout=8000)
        except Exception:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=6000)
        except Exception:
            pass
        if looks_like_pdp(page) or page.locator("h1").count() > 0:
            return True
    except Exception:
        pass
    try:
        href = best.get_attribute("href") or best.get_attribute("data-href") or ""
        if href:
            if href.startswith("/"):
                href = SELVER_BASE + href
            page.evaluate("url => window.location.assign(url)", href)
            try:
                page.wait_for_selector("h1", timeout=8000)
            except Exception:
                pass
            try:
                page.wait_for_load_state("networkidle", timeout=6000)
            except Exception:
                pass
            return looks_like_pdp(page) or page.locator("h1").count() > 0
    except Exception:
        pass
    return False


def _click_first_search_tile(page) -> bool:
    for sel in [
        "[data-testid='product-grid'] a[href]:visible",
        "[data-testid='product-list'] a[href]:visible",
        ".product-list a[href]:visible",
        ".product-grid a[href]:visible",
        ".product-card a[href]:visible",
        "article a[href]:visible",
        "[data-href]:visible",
        "a[href^='/']:visible",
    ]:
        try:
            a = page.locator(sel).first
            if a and a.count() > 0:
                href = a.get_attribute("href") or a.get_attribute("data-href") or ""
                if not looks_like_pdp_href(href):
                    continue
                a.click(timeout=4000)
                try:
                    page.wait_for_selector("h1", timeout=8000)
                except Exception:
                    pass
                try:
                    page.wait_for_load_state("networkidle", timeout=6000)
                except Exception:
                    pass
                return looks_like_pdp(page) or page.locator("h1").count() > 0
        except Exception:
            continue
    return False


# ----- PDP/title verification gate -----
def _pdp_title(page) -> str:
    return (_first_text(page, ["h1", "h1.product-title", "h1[itemprop='name']"]) or "").strip()


_STOP = {"kg", "tk", "g", "ml", "l", "dl", "cl", "ja", "või", "with", "ilma", "bio", "mahe"}


def _tokens(s: str) -> set:
    raw = re.findall(r"[\w]+", (s or "").casefold(), flags=re.UNICODE)
    toks = set()
    for t in raw:
        if t.isdigit() or len(t) < 3 or t in _STOP:
            continue
        toks.add(t)
    return toks


def _pdp_matches_target(page, name: str, brand: str, amount: str) -> bool:
    title = _pdp_title(page)
    tset = _tokens(title)
    want = _tokens(name) | _tokens(brand) | _tokens(amount)
    name_overlap = len(_tokens(name) & tset)
    total_overlap = len(want & tset)
    if name_overlap >= 1 and total_overlap >= 2:
        return True
    if brand and (_tokens(brand) & tset):
        return True
    if looks_like_pdp(page):
        return True
    return False


# ----- EAN/SKU extraction -----
def parse_ld_product(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        data = json.loads(text.strip())
    except Exception:
        return None, None, None

    def pick(d):
        if isinstance(d, list) and d:
            return d[0]
        return d

    data = pick(data)
    if not isinstance(data, dict):
        return None, None, None

    def is_product(d: dict) -> bool:
        t = d.get("@type")
        return isinstance(t, str) and t.lower() == "product"

    if not is_product(data):
        for v in data.values():
            p = pick(v)
            if isinstance(p, dict) and is_product(p):
                data = p
                break

    if not is_product(data):
        return None, None, None

    name = (data.get("name") or "").strip() or None
    ean = norm_ean(data.get("gtin14") or data.get("gtin13") or data.get("gtin") or data.get("ean"))
    sku = (data.get("sku") or "").strip() or None
    return name, ean, sku


def _wait_pdp_facts(page):
    try:
        page.wait_for_selector(":text-matches('Ribakood|Штрихкод|Barcode', 'i')", timeout=3000)
        return
    except Exception:
        pass
    try:
        page.evaluate("window.scrollBy(0, 800)")
        page.wait_for_selector(":text-matches('Ribakood|Штрихкод|Barcode', 'i')", timeout=3000)
    except Exception:
        pass


def _ean_sku_via_label_xpath(page) -> Tuple[Optional[str], Optional[str]]:
    def pick_ean(s: str) -> Optional[str]:
        if not s:
            return None
        m = re.search(r"(\b\d{13}\b|\b\d{8}\b)", s)
        return m.group(1) if m else None

    def pick_sku(s: str) -> Optional[str]:
        if not s:
            return None
        m = SKU_RX.search(s.upper())
        return m.group(0) if m else None

    ean_label_rx = r"Ribakood|Штрихкод|Barcode|EAN|Triipkood"
    ean_css_pairs = [
        ("dt:has(:text-matches('" + ean_label_rx + "','i'))", "dd"),
        ("tr:has(td:has(:text-matches('" + ean_label_rx + "','i')))", "td"),
        ("tr:has(th:has(:text-matches('" + ean_label_rx + "','i')))", "td"),
    ]

    ean_found: Optional[str] = None
    for k_sel, v_sel in ean_css_pairs:
        try:
            k = page.locator(k_sel).first
            if k and k.count() > 0:
                zones = [k, k.locator(v_sel)]
                for z in zones:
                    try:
                        t = (z.inner_text(timeout=900) or "").strip()
                        e = pick_ean(t)
                        if e:
                            ean_found = e
                            break
                    except Exception:
                        pass
            if ean_found:
                break
        except Exception:
            pass

    sku_css_pairs = [
        ("dt:has(:text-matches('" + SKU_LABEL_RX + "','i'))", "dd"),
        ("tr:has(td:has(:text-matches('" + SKU_LABEL_RX + "','i')))", "td"),
        ("tr:has(th:has(:text-matches('" + SKU_LABEL_RX + "','i')))", "td"),
    ]
    sku_found: Optional[str] = None
    for k_sel, v_sel in sku_css_pairs:
        try:
            k = page.locator(k_sel).first
            if not k or k.count() == 0:
                continue
            zones = [k, k.locator(v_sel), k.locator("xpath=following-sibling::*[1]")]
            for z in zones:
                try:
                    t = (z.inner_text(timeout=900) or "").strip()
                    s = pick_sku(t)
                    if s:
                        sku_found = s
                        break
                except Exception:
                    pass
            if sku_found:
                break
        except Exception:
            pass

    return ean_found, sku_found


def _extract_ids_dom_bruteforce(page) -> Tuple[Optional[str], Optional[str]]:
    try:
        got = page.evaluate(
            """
        () => {
          const txt = n => (n && n.textContent || '').replace(/\\s+/g,' ').trim();
          const pickDigits = s => { const m = s && s.match(/(\\b\\d{13}\\b|\\b\\d{8}\\b)/); return m ? m[1] : null; };
          let ean = null;
          const nodes = Array.from(document.querySelectorAll('div,section,span,p,li,td,th,dd,dt,strong,em'));
          for (const el of nodes) {
            const t = txt(el);
            if (!t) continue;
            if (/(^|\\b)(ribakood|ean|triipkood|штрихкод|barcode)(\\b|:)/i.test(t)) {
              const zone = [el, el.parentElement, el.nextElementSibling, el.previousElementSibling, el.parentElement && el.parentElement.nextElementSibling];
              for (const z of zone) {
                const tt = txt(z);
                if (!ean) ean = pickDigits(tt);
                if (ean) break;
              }
            }
            if (ean) break;
          }
          return { ean };
        }
        """
        )
        if got:
            return got.get("ean") or None, None
    except Exception:
        pass
    return None, None


def extract_ids_on_pdp(page) -> Tuple[Optional[str], Optional[str]]:
    sku_found: Optional[str] = None
    try:
        page.wait_for_timeout(250)
    except Exception:
        pass
    try:
        handle_age_gate(page)
    except Exception:
        pass

    # JSON-LD
    try:
        scripts = page.locator("script[type='application/ld+json']")
        n = scripts.count()
        for i in range(n):
            try:
                _, ean, sku = parse_ld_product(scripts.nth(i).inner_text())
                if sku and not sku_found:
                    m = SKU_RX.search((sku or "").upper())
                    if m:
                        sku_found = m.group(0)
                if ean:
                    return ean, sku_found
            except Exception:
                pass
    except Exception:
        pass

    meta_sku = _meta_content(page, ["meta[itemprop='sku']"]) or ""
    m = SKU_RX.search(meta_sku.upper())
    if m and not sku_found:
        sku_found = m.group(0)

    meta_ean = _meta_content(page, ["meta[itemprop='gtin13']", "meta[itemprop='gtin']"])
    if meta_ean:
        e = norm_ean(meta_ean)
        if e:
            return e, sku_found

    _wait_pdp_facts(page)
    e_spec, s_spec = _ean_sku_via_label_xpath(page)
    if s_spec and not sku_found:
        sku_found = s_spec
    if e_spec:
        return norm_ean(e_spec), sku_found

    e_dom, _ = _extract_ids_dom_bruteforce(page)
    if e_dom:
        return norm_ean(e_dom), sku_found

    try:
        html = page.content() or ""
        m = JSON_EAN.search(html)
        if m:
            e = norm_ean(m.group("d"))
            if e:
                return e, sku_found
    except Exception:
        pass
    try:
        html = page.content() or ""
        m = re.search(r"(ribakood|ean|triipkood|штрихкод|barcode)[\s\S]{0,800}?(\d{8,14})", html, re.I)
        if m:
            return norm_ean(m.group(2)), sku_found
    except Exception:
        pass

    return None, sku_found


def ensure_specs_open(page):
    for sel in ["button:has-text('Tooteinfo')", "button:has-text('Lisainfo')", "button:has-text('Tootekirjeldus')"]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=1000)
                time.sleep(0.15)
        except Exception:
            pass


# --------- unified search→open helper ---------
def open_best_or_first(page, name: str, brand: str, amount: str) -> bool:
    try:
        page.wait_for_selector("[data-testid='product-grid'], .product-list, a[href^='/']", timeout=7000)
    except Exception:
        pass
    kill_consents_and_overlays(page)
    try:
        handle_age_gate(page)
    except Exception:
        pass
    try:
        page.evaluate("window.scrollBy(0, 300)")
    except Exception:
        pass

    if _click_best_tile(page, name, brand, amount):
        return True

    hit = best_search_hit(page, name, brand, amount)
    if hit:
        try:
            page.goto(hit, timeout=20000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=6000)
            except Exception:
                pass
            try:
                handle_age_gate(page)
            except Exception:
                pass
            if looks_like_pdp(page) or page.locator("h1").count() > 0:
                return True
        except Exception:
            pass

    try:
        for _ in range(2):
            page.keyboard.press("End")
            time.sleep(0.25)
            page.keyboard.press("Home")
            time.sleep(0.15)
            if _click_best_tile(page, name, brand, amount):
                return True
    except Exception:
        pass

    return _click_first_search_tile(page)


# ----------------- main probe helpers -----------------
def process_one(
    page, name: str, brand: str, amount: str, direct_url: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """Try direct Selver PDP URL first; if that fails, fall back to search."""
    # Fast path: direct PDP from mapping
    if direct_url:
        try:
            page.goto(direct_url, timeout=15000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            kill_consents_and_overlays(page)
            handle_age_gate(page)
            ensure_specs_open(page)
            ean, sku = extract_ids_on_pdp(page)
            if ean:
                return norm_ean(ean), sku
        except Exception:
            # Fall through to search flow
            pass

    # Search flow
    q_variants = []
    q_full = " ".join(x for x in [name or "", brand or "", amount or ""] if x).strip()
    if q_full:
        q_variants.append(q_full)
    if name:
        q_variants.append(name.strip())
    if brand and name:
        q_variants.append(f"{name} {brand}")

    for q in q_variants:
        try:
            url = SEARCH_URL.format(q=quote_plus(q))
            page.goto(url, timeout=20000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=6000)
            except Exception:
                pass
            kill_consents_and_overlays(page)
            try:
                handle_age_gate(page)
            except Exception:
                pass
        except PWTimeout:
            continue

        if is_search_page(page) or not looks_like_pdp(page):
            opened = open_best_or_first(page, name, brand, amount)
            if not opened:
                continue

        if not (
            _pdp_matches_target(page, name, brand, amount)
            or looks_like_pdp(page)
            or page.locator(":text-matches('Ribakood|Штрихкод|Barcode','i')").count() > 0
        ):
            want = (name or "")[:60]
            got = (_pdp_title(page) or "")[:120]
            print(f"[WRONG_PDP] want='{want}' got='{got}' url={page.url}")
            continue

        ensure_specs_open(page)
        try:
            handle_age_gate(page)
        except Exception:
            pass
        ean, sku = extract_ids_on_pdp(page)
        if ean:
            ean = norm_ean(ean)
            if not looks_bogus_ean(ean):
                return ean, (sku or None)

    return None, None


def _worker(rows: List[dict], req_delay: float):
    conn = connect()
    random.shuffle(rows)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(
                locale="et-EE",
                extra_http_headers={"Accept-Language": "et-EE,et;q=0.9,en;q=0.8,ru;q=0.7"},
                viewport={"width": 1280, "height": 880},
            )
            ctx.route("**/*", _router)
            page = ctx.new_page()

            for row in rows:
                pid = row["product_id"]
                name = row.get("name") or ""
                brand = row.get("brand") or ""
                amount = row.get("amount") or ""
                url = row.get("selver_url") or None
                try:
                    ean, sku = process_one(page, name, brand, amount, url)
                    if ean:
                        status = update_success(conn, pid, ean, sku)
                        tag = "OK" if status == "OK" else status
                        print(
                            f"[{current_process().name}:{tag}] id={pid} ← EAN {ean}{(' | SKU ' + sku) if sku else ''}"
                        )
                    else:
                        update_failure(conn, pid, "ean not found or bogus")
                        print(f"[{current_process().name}:MISS] id={pid} name='{name[:60]}'")
                except Exception as e:
                    conn.rollback()
                    update_failure(conn, pid, str(e))
                    print(f"[{current_process().name}:FAIL] id={pid} err={e}", file=sys.stderr)
                finally:
                    time.sleep(max(0.05, req_delay + random.uniform(-0.08, 0.08)))
            try:
                browser.close()
            except Exception:
                pass
    finally:
        conn.close()


def main():
    conn = connect()
    try:
        total = int(os.getenv("BATCH", str(BATCH)))
        conc = max(1, int(os.getenv("CONCURRENCY", str(CONCURRENCY))))
        rows = pick_batch(conn, total)
        if not rows:
            print("No Selver products to backfill. Done.")
            return

        n = min(conc, len(rows))
        if n <= 1:
            _worker(rows, REQ_DELAY)
            return

        chunk = math.ceil(len(rows) / n)
        procs: List[Process] = []
        for i in range(n):
            part = rows[i * chunk : (i + 1) * chunk]
            if not part:
                break
            p = Process(name=f"W{i+1}", target=_worker, args=(part, REQ_DELAY))
            p.start()
            procs.append(p)
        for p in procs:
            p.join()
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

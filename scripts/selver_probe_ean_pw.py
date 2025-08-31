# scripts/selver_probe_ean_pw.py
# Purpose: Backfill missing EANs (and SKU if present) for Selver products (no CSV required)
#
# Usage:
#   pip install playwright psycopg2-binary
#   python -m playwright install chromium
#   export DATABASE_URL=postgres://...
#   [optional] export BATCH=150 HEADLESS=1 REQ_DELAY=0.6 OVERWRITE_BAD_EANS=1
#   python scripts/selver_probe_ean_pw.py
#
# Env:
#   DATABASE_URL / DATABASE_URL_PUBLIC  Postgres connection string
#   BATCH         Rows per run (default 100)
#   HEADLESS      1|0 (default 1)
#   REQ_DELAY     Seconds between actions (default 0.6)
#   OVERWRITE_BAD_EANS  1|0 (default 0). If 1, also fix obviously-bad EANs.

from __future__ import annotations
import os, re, sys, time, json
from typing import Optional, List, Tuple
import psycopg2, psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SELVER_BASE = "https://www.selver.ee"
SEARCH_URL = SELVER_BASE + "/search?q={q}"

HEADLESS = os.getenv("HEADLESS", "1") == "1"
BATCH = int(os.getenv("BATCH", "100"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.6"))
OVERWRITE_BAD = os.getenv("OVERWRITE_BAD_EANS", "0").lower() in ("1", "true", "yes")
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_PUBLIC")

EAN_RE = re.compile(r"\b(\d{13}|\d{8})\b")
JSON_EAN = re.compile(r'"(?:gtin14|gtin13|gtin|ean|barcode|sku)"\s*:\s*"(?P<d>\d{8,14})"', re.I)

def norm_ean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = re.sub(r"\D", "", s)
    return d if d and len(d) in (8, 13) else None

def looks_bogus_ean(e: Optional[str]) -> bool:
    if not e:
        return False
    # 8 identical digits (e.g., 33333333) or all zeros for 8/13
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
        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_name = %s
            );
        """, (tbl,))
        return cur.fetchone()[0]

def column_exists(conn: PGConn, tbl: str, col: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
          SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name=%s AND column_name=%s
          );
        """, (tbl, col))
        return cur.fetchone()[0]

def pick_batch(conn: PGConn, limit: int):
    """Prefer queue if present; otherwise query products directly."""
    bad_sql = """
        (p.ean !~ '^[0-9]+$' OR length(p.ean) NOT IN (8,13)
         OR p.ean ~ '^([0-9])\\1{7}$'
         OR p.ean IN ('00000000','0000000000000'))
    """
    where_target = "(p.ean IS NULL OR p.ean = '')" if not OVERWRITE_BAD else f"(p.ean IS NULL OR p.ean = '' OR {bad_sql})"

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if table_exists(conn, "selver_ean_backfill_queue"):
            cur.execute(f"""
              SELECT q.product_id, p.name,
                     COALESCE(NULLIF(p.brand,''), '') AS brand,
                     COALESCE(NULLIF(p.amount,''), '') AS amount
              FROM selver_ean_backfill_queue q
              JOIN products p ON p.id = q.product_id
              JOIN prices pr ON pr.product_id = p.id
              JOIN stores s  ON s.id = pr.store_id
              WHERE {where_target}
                AND s.chain = 'Selver'
              GROUP BY q.product_id, p.name, p.brand, p.amount
              ORDER BY q.attempts ASC, q.updated_at ASC
              LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
            if rows:
                return rows

        cur.execute(f"""
          SELECT DISTINCT p.id AS product_id, p.name,
                 COALESCE(NULLIF(p.brand,''), '') AS brand,
                 COALESCE(NULLIF(p.amount,''), '') AS amount
          FROM products p
          JOIN prices pr ON pr.product_id = p.id
          JOIN stores s  ON s.id = pr.store_id
          WHERE s.chain = 'Selver'
            AND {where_target}
          ORDER BY p.id
          LIMIT %s;
        """, (limit,))
        return cur.fetchall()

# -------- duplicate-aware, rollback-safe updates ----------

def update_success(conn: PGConn, product_id: int, ean: str, sku: Optional[str] = None) -> str:
    """
    Update EAN (and SKU if present).
    Returns a status: 'OK', 'OVERWRITE', 'EXISTS', 'SKIP_PRESENT', 'SKIP_NOT_BAD', 'DUP_FOUND', 'BOGUS_CANDIDATE'
    """
    try:
        with conn.cursor() as cur:
            # candidate sanity
            if looks_bogus_ean(ean):
                if table_exists(conn, "selver_ean_backfill_queue"):
                    cur.execute("""
                      UPDATE selver_ean_backfill_queue
                         SET attempts = attempts + 1,
                             last_error = %s,
                             updated_at = now()
                       WHERE product_id = %s;
                    """, (f"bogus EAN {ean}", product_id))
                conn.commit()
                return "BOGUS_CANDIDATE"

            # what is currently on the product?
            cur.execute("SELECT ean FROM products WHERE id = %s;", (product_id,))
            prev = (cur.fetchone() or [None])[0]

            if prev:
                if prev == ean:
                    if table_exists(conn, "selver_ean_backfill_queue"):
                        cur.execute("""
                          UPDATE selver_ean_backfill_queue
                             SET attempts = attempts + 1,
                                 last_error = NULL,
                                 updated_at = now()
                           WHERE product_id = %s;
                        """, (product_id,))
                    conn.commit()
                    return "EXISTS"

                if not OVERWRITE_BAD:
                    if table_exists(conn, "selver_ean_backfill_queue"):
                        cur.execute("""
                          UPDATE selver_ean_backfill_queue
                             SET attempts = attempts + 1,
                                 last_error = %s,
                                 updated_at = now()
                           WHERE product_id = %s;
                        """, (f"has existing EAN {prev}, overwrite disabled", product_id))
                    conn.commit()
                    return "SKIP_PRESENT"

                # overwrite mode: only if current EAN is actually bad
                if not is_bad_ean_python(prev):
                    if table_exists(conn, "selver_ean_backfill_queue"):
                        cur.execute("""
                          UPDATE selver_ean_backfill_queue
                             SET attempts = attempts + 1,
                                 last_error = %s,
                                 updated_at = now()
                           WHERE product_id = %s;
                        """, (f"existing EAN {prev} not considered bad", product_id))
                    conn.commit()
                    return "SKIP_NOT_BAD"

            # Avoid creating a duplicate EAN on another product
            cur.execute("SELECT id FROM products WHERE ean = %s AND id <> %s LIMIT 1;", (ean, product_id))
            row = cur.fetchone()
            if row:
                if table_exists(conn, "selver_ean_backfill_queue"):
                    cur.execute("""
                      UPDATE selver_ean_backfill_queue
                         SET attempts = attempts + 1,
                             last_error = %s,
                             updated_at = now()
                       WHERE product_id = %s;
                    """, (f"duplicate EAN {ean} already on product {row[0]}", product_id))
                conn.commit()
                return "DUP_FOUND"

            # perform update
            if sku and column_exists(conn, "products", "sku"):
                cur.execute(
                    "UPDATE products SET ean = %s, sku = COALESCE(%s, sku) WHERE id = %s;",
                    (ean, sku, product_id)
                )
            else:
                cur.execute("UPDATE products SET ean = %s WHERE id = %s;", (ean, product_id))

            if table_exists(conn, "selver_ean_backfill_queue"):
                cur.execute("""
                  UPDATE selver_ean_backfill_queue
                     SET attempts = attempts + 1,
                         last_error = NULL,
                         updated_at = now()
                   WHERE product_id = %s;
                """, (product_id,))
        conn.commit()
        return "OVERWRITE" if prev else "OK"
    except Exception:
        conn.rollback()
        raise

def update_failure(conn: PGConn, product_id: int, err: str):
    """Record a failure and keep the connection usable."""
    try:
        if table_exists(conn, "selver_ean_backfill_queue"):
            with conn.cursor() as cur:
                cur.execute("""
                  UPDATE selver_ean_backfill_queue
                     SET attempts = attempts + 1,
                         last_error = LEFT(%s, 500),
                         updated_at = now()
                   WHERE product_id = %s;
                """, (err, product_id))
        conn.commit()
    except Exception:
        conn.rollback()

# ------------- page helpers -------------

def _first_text(page, selectors: List[str], timeout_ms: int = 2000) -> Optional[str]:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                t = loc.inner_text(timeout=timeout_ms)
                if t:
                    t = t.strip()
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
                if v:
                    v = v.strip()
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
        if page.locator("text=Ribakood").count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.locator("meta[itemprop='gtin13'], meta[itemprop='gtin'], meta[itemprop='sku']").count() > 0:
            return True
    except Exception:
        pass
    return False

def score_hit(qname: str, brand: str, amount: str, text: str) -> float:
    s = 0.0
    t = (text or "").lower()
    for tok in set(re.findall(r'\w+', (qname or "").lower())):
        if len(tok) >= 3 and tok in t:
            s += 1.0
    if brand and brand.lower() in t:
        s += 2.0
    if amount and amount.lower() in t:
        s += 1.0
    return s

def kill_consents(page):
    buttons = [
        "button:has-text('Nõustun')",
        "button:has-text('Luba kõik')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "[data-testid='uc-accept-all-button']",
        "[aria-label='Accept all']",
    ]
    for sel in buttons:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=800)
                time.sleep(0.2)
        except Exception:
            pass

def best_search_hit(page, qname: str, brand: str, amount: str) -> Optional[str]:
    # Robust: tolerate different result grids; treat current page as PDP if so
    t0 = time.time()
    while time.time() - t0 < 12:
        kill_consents(page)
        if looks_like_pdp(page):
            return page.url
        candidate_sets = [
            "[data-testid='product-grid'] a[href]",
            "[data-testid='product-list'] a[href]",
            ".product-list a[href]",
            "article a[href]",
            "a[href^='/toode/']",
            "a[href^='/']",
        ]
        links = []
        for css in candidate_sets:
            try:
                links.extend(page.locator(css).all()[:24])
            except Exception:
                pass
        scored: List[Tuple[str, float]] = []
        for a in links:
            try:
                href = a.get_attribute("href") or ""
                if not href or "/search" in href:
                    continue
                txt = a.inner_text() or ""
                scored.append((href, score_hit(qname, brand, amount, txt)))
            except Exception:
                continue
        if scored:
            scored.sort(key=lambda x: x[1], reverse=True)
            href = scored[0][0]
            if href.startswith("/"):
                href = SELVER_BASE + href
            return href
        time.sleep(0.25)
    return None

def parse_ld_product(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (name, ean, sku) from JSON-LD 'Product' if present."""
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
    sku  = (data.get("sku") or "").strip() or None
    return name, ean, sku

def extract_ids_on_pdp(page) -> Tuple[Optional[str], Optional[str]]:
    """Return (ean, sku) from PDP using several strategies."""
    sku_found: Optional[str] = None

    try:
        page.wait_for_timeout(300)
    except Exception:
        pass

    # A) JSON-LD
    try:
        scripts = page.locator("script[type='application/ld+json']")
        n = scripts.count()
        for i in range(n):
            try:
                _, ean, sku = parse_ld_product(scripts.nth(i).inner_text())
                if sku and not sku_found:
                    sku_found = sku
                if ean:
                    return ean, sku_found
            except Exception:
                pass
    except Exception:
        pass

    # B) meta itemprops
    meta_sku = _meta_content(page, ["meta[itemprop='sku']"])
    if meta_sku and not sku_found:
        sku_found = (meta_sku or "").strip() or None
    meta_ean = _meta_content(page, ["meta[itemprop='gtin13']", "meta[itemprop='gtin']"])
    if meta_ean:
        e = norm_ean(meta_ean)
        if e:
            return e, sku_found

    # C) labels: Ribakood/EAN/Triipkood + SKU/Tootekood
    label_xpaths = [
        "//*[contains(translate(normalize-space(text()), 'EANRIBAKOODTRIIPKOOD', 'eanribakoodtriipkood'), 'ribakood')]",
        "//*[contains(translate(normalize-space(text()), 'EANRIBAKOODTRIIPKOOD', 'eanribakoodtriipkood'), 'ean')]",
        "//*[contains(translate(normalize-space(text()), 'EANRIBAKOODTRIIPKOOD', 'eanribakoodtriipkood'), 'triipkood')]",
    ]
    sku_xpaths = [
        "//*[contains(translate(normalize-space(text()), 'SKUTOOTEKOOD', 'skutootekood'), 'sku')]",
        "//*[contains(translate(normalize-space(text()), 'SKUTOOTEKOOD', 'skutootekood'), 'tootekood')]",
    ]

    for xp in sku_xpaths:
        try:
            el = page.locator(f"xpath={xp}").first
            if el.count() > 0 and not sku_found:
                for c in [el, el.locator("xpath=.."), el.locator("xpath=following-sibling::*[1]")]:
                    try:
                        txt = (c.inner_text(timeout=800) or "").strip()
                        if txt:
                            m = re.search(r'([A-Z0-9\-]{6,})', txt, re.I)
                            if m:
                                sku_found = m.group(1)
                                break
                    except Exception:
                        pass
        except Exception:
            pass

    for xp in label_xpaths:
        try:
            el = page.locator(f"xpath={xp}").first
            if el.count() > 0:
                for c in [
                    el, el.locator("xpath=.."), el.locator("xpath=../.."),
                    el.locator("xpath=following-sibling::*[1]"),
                    el.locator("xpath=following-sibling::*[2]"),
                    el.locator("xpath=preceding-sibling::*[1]")
                ]:
                    try:
                        txt = c.inner_text(timeout=800) or ""
                        m = EAN_RE.search(txt)
                        if m:
                            return m.group(1), sku_found
                    except Exception:
                        pass
        except Exception:
            pass

    # D) structured JSON anywhere
    try:
        html = page.content() or ""
        m = JSON_EAN.search(html)
        if m:
            e = norm_ean(m.group("d"))
            if e:
                return e, sku_found
    except Exception:
        pass

    # E) full-page fallback
    try:
        full = page.text_content("body") or ""
        nums = EAN_RE.findall(full)
        if nums:
            e13 = [x for x in nums if len(x) == 13]
            return (e13[0] if e13 else nums[0]), sku_found
    except Exception:
        pass

    return None, sku_found

def ensure_specs_open(page):
    for sel in ["button:has-text('Tooteinfo')", "button:has-text('Lisainfo')"]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=800)
                time.sleep(0.2)
        except Exception:
            pass

def process_one(page, name: str, brand: str, amount: str) -> Tuple[Optional[str], Optional[str]]:
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
            url = SEARCH_URL.format(q=q.replace(" ", "+"))
            page.goto(url, timeout=20000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            kill_consents(page)
        except PWTimeout:
            continue

        if not looks_like_pdp(page):
            hit = best_search_hit(page, name, brand, amount)
            if not hit:
                continue
            page.goto(hit, timeout=20000, wait_until="load")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

        ensure_specs_open(page)
        ean, sku = extract_ids_on_pdp(page)
        if ean:
            ean = norm_ean(ean)
            if not looks_bogus_ean(ean):
                return ean, (sku or None)

    return None, None

def main():
    conn = connect()
    batch = pick_batch(conn, BATCH)
    if not batch:
        print("No Selver products without EAN. Done.")
        conn.close()
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()

        for row in batch:
            pid = row["product_id"]
            name = row["name"] or ""
            brand = row["brand"] or ""
            amount = row["amount"] or ""
            try:
                ean, sku = process_one(page, name, brand, amount)
                if ean:
                    status = update_success(conn, pid, ean, sku)
                    tag = "OK" if status == "OK" else status
                    print(f"[{tag}] id={pid} ← EAN {ean}{(' | SKU ' + sku) if sku else ''}")
                else:
                    update_failure(conn, pid, "ean not found or bogus")
                    print(f"[MISS] id={pid} name='{name}'")
            except Exception as e:
                conn.rollback()
                update_failure(conn, pid, str(e))
                print(f"[FAIL] id={pid} err={e}", file=sys.stderr)
            finally:
                time.sleep(REQ_DELAY)

        browser.close()
    conn.close()

if __name__ == "__main__":
    main()

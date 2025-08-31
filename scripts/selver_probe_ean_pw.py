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
import os, re, sys, time, json, unicodedata
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

# How strict should the PDP name check be (0..1). 0.35 works well in practice.
NAME_MATCH_THRESHOLD = float(os.getenv("NAME_MATCH_THRESHOLD", "0.35"))

EAN_RE = re.compile(r"\b(\d{13}|\d{8})\b")
JSON_EAN = re.compile(r'"(?:gtin14|gtin13|gtin|ean|barcode|sku)"\s*:\s*"(?P<d>\d{8,14})"', re.I)

def _digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def ean13_valid(code: str) -> bool:
    if not re.fullmatch(r"\d{13}", code):
        return False
    s_odd  = sum(int(code[i]) for i in range(0, 12, 2))
    s_even = sum(int(code[i]) * 3 for i in range(1, 12, 2))
    chk = (10 - ((s_odd + s_even) % 10)) % 10
    return chk == int(code[-1])

def ean8_valid(code: str) -> bool:
    if not re.fullmatch(r"\d{8}", code):
        return False
    s = sum(int(code[i]) * (3 if i % 2 == 0 else 1) for i in range(0, 7))
    chk = (10 - (s % 10)) % 10
    return chk == int(code[-1])

def valid_ean(code: Optional[str]) -> bool:
    if not code: return False
    if len(code) == 13: return ean13_valid(code)
    if len(code) == 8:  return ean8_valid(code)
    return False

def norm_ean(s: Optional[str]) -> Optional[str]:
    d = _digits(s)
    return d if d and len(d) in (8, 13) else None

def looks_bogus_ean(e: Optional[str]) -> bool:
    if not e:
        return False
    # 8 identical digits (e.g., 33333333) or all zeros for 8/13
    return bool(re.fullmatch(r'(\d)\1{7}', e)) or e in {"00000000", "0000000000000"}

def is_bad_ean_python(e: Optional[str]) -> bool:
    if not e or not e.isdigit() or len(e) not in (8, 13):
        return True
    if looks_bogus_ean(e):
        return True
    # require checksum to pass
    return not valid_ean(e)

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
            if is_bad_ean_python(ean):
                if table_exists(conn, "selver_ean_backfill_queue"):
                    cur.execute("""
                      UPDATE selver_ean_backfill_queue
                         SET attempts = attempts + 1,
                             last_error = %s,
                             updated_at = now()
                       WHERE product_id = %s;
                    """, (f"bogus/invalid EAN {ean}", product_id))
                conn.commit()
                return "BOGUS_CANDIDATE"

            # current value?
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

            # Avoid duplicates on another product
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

def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()

def _name_similarity(qname: str, brand: str, amount: str, pdp_name: str) -> float:
    q = _normalize_text(" ".join(x for x in [qname or "", brand or "", amount or ""] if x))
    p = _normalize_text(pdp_name or "")
    if not q or not p:
        return 0.0
    qset = set(q.split())
    pset = set(p.split())
    inter = len(qset & pset)
    uni = max(1, len(qset | pset))
    return inter / uni

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
        can = _meta_content(page, ["link[rel='canonical']"])
        if can and "/toode/" in can:
            return True
    except Exception:
        pass
    try:
        url_path = page.url.split("://",1)[-1]
        if "/toode/" in url_path:
            return True
    except Exception:
        pass
    try:
        og_type = _meta_content(page, ["meta[property='og:type']"]) or ""
        if og_type.lower() == "product" and page.locator("h1").count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.locator("text=Ribakood").count() > 0 and page.locator("h1").count() > 0:
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
    # slight bonus if number-unit appears (e.g., "250 g", "1 kg", "500 ml")
    if re.search(r"\b\d+\s?(?:g|kg|ml|l|cl|dl)\b", t):
        s += 0.5
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

        # Collect both anchors and data-href tiles
        links = []
        for css in [
            "[data-testid='product-grid'] a[href]",
            "[data-testid='product-list'] a[href]",
            ".product-list a[href]",
            "article a[href]",
            "a[href^='/toode/']",
            "a[href^='/']",
        ]:
            try:
                links.extend(page.locator(css).all()[:24])
            except Exception:
                pass
        try:
            links.extend(page.locator("[data-href]").all()[:24])
        except Exception:
            pass

        scored: List[Tuple[str, float]] = []
        for a in links:
            try:
                href = a.get_attribute("href") or a.get_attribute("data-href") or ""
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

def _extract_ids_dom_bruteforce(page) -> Tuple[Optional[str], Optional[str]]:
    """
    Very robust DOM pass:
    - find any node mentioning ribakood/ean/triipkood and grab closest 8/13 digits
    - try to pick SKU/Tootekood nearby as well
    """
    try:
        got = page.evaluate("""
        () => {
          const txt = n => (n && n.textContent || '').replace(/\s+/g,' ').trim();
          const pickDigits = s => {
            if (!s) return null;
            const m = s.match(/(\d{13}|\d{8})/);
            return m ? m[1] : null;
          };
          const pickSKU = s => {
            if (!s) return null;
            const m = s.match(/([A-Z0-9_-]{6,})/i);
            return m ? m[1] : null;
          };

          let ean = null, sku = null;

          const nodes = Array.from(document.querySelectorAll('div,span,p,li,td,th,dd,dt'));
          for (const el of nodes) {
            const t = txt(el);
            if (!t) continue;

            if (/\b(ribakood|ean|triipkood)\b/i.test(t)) {
              const zone = [el, el.parentElement, el.nextElementSibling, el.previousElementSibling];
              for (const z of zone) {
                const tt = txt(z);
                if (!ean) ean = pickDigits(tt);
                if (!sku) sku = pickSKU(tt);
                if (ean && sku) break;
              }
            }
            if (ean && sku) break;
          }

          // No label seen? don't guess a random 13 digits from the whole page.
          return { ean, sku };
        }
        """)
        if not got:
            return None, None
        e = got.get("ean") or None
        s = got.get("sku") or None
        return e, s
    except Exception:
        return None, None

def _pdp_name(page) -> str:
    # prefer JSON-LD name, else H1
    try:
        scripts = page.locator("script[type='application/ld+json']")
        n = scripts.count()
        for i in range(n):
            try:
                name, _, _ = parse_ld_product(scripts.nth(i).inner_text())
                if name:
                    return name
            except Exception:
                pass
    except Exception:
        pass
    try:
        h = page.locator("h1").first
        if h and h.count() > 0:
            t = (h.inner_text() or "").strip()
            if t:
                return t
    except Exception:
        pass
    return ""

def extract_ids_on_pdp(page) -> Tuple[Optional[str], Optional[str]]:
    """Return (ean, sku) from PDP using several strategies."""
    sku_found: Optional[str] = None

    try:
        page.wait_for_timeout(250)
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
                    e = norm_ean(ean)
                    if e and valid_ean(e) and not looks_bogus_ean(e):
                        return e, sku_found
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
        if e and valid_ean(e) and not looks_bogus_ean(e):
            return e, sku_found

    # C) DOM brute-force near labels (Ribakood/EAN/Triipkood + SKU/Tootekood)
    e_dom, s_dom = _extract_ids_dom_bruteforce(page)
    if e_dom:
        e = norm_ean(e_dom)
        if e and valid_ean(e) and not looks_bogus_ean(e):
            return e, s_dom or sku_found

    # D) JSON blob anywhere (still require validation)
    try:
        html = page.content() or ""
        m = JSON_EAN.search(html)
        if m:
            e = norm_ean(m.group("d"))
            if e and valid_ean(e) and not looks_bogus_ean(e):
                return e, sku_found or s_dom
    except Exception:
        pass

    # E) HTML regex tolerant of arbitrary markup between label and digits (still requires label)
    try:
        html = page.content() or ""
        m = re.search(r"(ribakood|ean|triipkood)[\s\S]{0,600}?(\d{8,14})", html, re.I)
        if m:
            e = norm_ean(m.group(2))
            if e and valid_ean(e) and not looks_bogus_ean(e):
                return e, sku_found or s_dom
    except Exception:
        pass

    return None, sku_found or s_dom

def ensure_specs_open(page):
    for sel in ["button:has-text('Tooteinfo')", "button:has-text('Lisainfo')"]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=800)
                time.sleep(0.2)
        except Exception:
            pass

def process_one(page, name: str, brand: str, amount: str) -> Tuple[Optional[str], Optional[str]]:
    # build several query variants
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

        if not looks_like_pdp(page):
            # not a product page, try next query variant
            continue

        ensure_specs_open(page)

        # sanity: PDP name should roughly match our query, otherwise skip
        pdp_name = _pdp_name(page)
        if pdp_name:
            sim = _name_similarity(name or "", brand or "", amount or "", pdp_name)
            if sim < NAME_MATCH_THRESHOLD:
                # low confidence match → skip this hit
                continue

        ean, sku = extract_ids_on_pdp(page)
        if ean:
            ean = norm_ean(ean)
            if ean and valid_ean(ean) and not looks_bogus_ean(ean):
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
        ctx = browser.new_context(
            locale="et-EE",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language": "et-EE,et;q=0.9,en-US;q=0.8,en;q=0.7"},
            ignore_https_errors=True,
        )
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
                    update_failure(conn, pid, "ean not found or low-confidence match")
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

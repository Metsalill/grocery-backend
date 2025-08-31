# scripts/selver_probe_ean_pw.py
# Purpose: Backfill missing EANs (and SKU if present) for Selver products (no CSV required)

from __future__ import annotations
import os, re, sys, time, json
from typing import Optional, List, Tuple
import psycopg2, psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SELVER_BASE = "https://www.selver.ee"
SEARCH_URL  = SELVER_BASE + "/search?q={q}"

HEADLESS = os.getenv("HEADLESS", "1") == "1"
BATCH = int(os.getenv("BATCH", "100"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.6"))
OVERWRITE_BAD = os.getenv("OVERWRITE_BAD_EANS", "0").lower() in ("1", "true", "yes")
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_PUBLIC")

EAN_RE   = re.compile(r"\b(\d{8}|\d{13})\b")
JSON_EAN = re.compile(r'"(?:gtin14|gtin13|gtin|ean|barcode|sku)"\s*:\s*"(?P<d>\d{8,14})"', re.I)

# ----------------- small utils -----------------

def norm_ean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = re.sub(r"\D", "", s)
    return d if d and len(d) in (8, 13) else None

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

# ----------------- batch pick -----------------

def pick_batch(conn: PGConn, limit: int):
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

# ----------------- DB writes -----------------

def update_success(conn: PGConn, product_id: int, ean: str, sku: Optional[str] = None) -> str:
    try:
        with conn.cursor() as cur:
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

            cur.execute("SELECT id FROM products WHERE ean = %s AND id <> %s LIMIT 1;", (ean, product_id))
            dup = cur.fetchone()
            if dup:
                if table_exists(conn, "selver_ean_backfill_queue"):
                    cur.execute("""
                      UPDATE selver_ean_backfill_queue
                         SET attempts = attempts + 1,
                             last_error = %s,
                             updated_at = now()
                       WHERE product_id = %s;
                    """, (f"duplicate EAN {ean} already on product {dup[0]}", product_id))
                conn.commit()
                return "DUP_FOUND"

            if sku and column_exists(conn, "products", "sku"):
                cur.execute("UPDATE products SET ean = %s, sku = COALESCE(%s, sku) WHERE id = %s;", (ean, sku, product_id))
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

def kill_consents_and_overlays(page):
    # Cookie/consent
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
    # Close search suggestion overlay if open
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

# --- PDP-href recognizer to avoid category/landing pages ---
def looks_like_pdp_href(href: str) -> bool:
    if not href or not href.startswith("/") or "?" in href or "#" in href:
        return False
    if href.count("/") != 1:  # PDP slugs are single-segment
        return False
    bad_prefixes = (
        "/search", "/eritooted", "/puu-ja-koogiviljad", "/liha-ja-kalatooted",
        "/piimatooted", "/juustud", "/leivad", "/valmistoidud",
        "/kauplused", "/kliendimangud", "/selveekspress", "/tule-toole",
        "/uudised", "/kinkekaardid", "/selveri-kook", "/kampaania", "/retseptid",
    )
    return not any(href.startswith(p) for p in bad_prefixes)

def is_search_page(page) -> bool:
    try:
        url = page.url
        if "/search?" in url:
            return True
        if any(seg in url for seg in (
            "/eritooted/", "/puu-ja-koogiviljad/", "/liha-ja-kalatooted/",
            "/piimatooted", "/juustud", "/leivad", "/valmistoidud"
        )):
            return True
        if page.locator("text=Otsingu:").count() > 0:
            return True
    except Exception:
        pass
    return False

def best_search_hit(page, qname: str, brand: str, amount: str) -> Optional[str]:
    roots = [
        "[data-testid='product-grid']",
        "[data-testid='product-list']",
        ".product-list",
        ".product-grid",
        ".product-card",
    ]
    links = []
    for root in roots:
        try:
            links.extend(page.locator(f"{root} a[href]").all()[:60])
        except Exception:
            pass
    for css in ["article a[href]", "[data-href]"]:
        try:
            links.extend(page.locator(css).all()[:40])
        except Exception:
            pass

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

def _click_first_search_tile(page) -> bool:
    selectors = [
        "[data-testid='product-grid'] a[href]:visible",
        "[data-testid='product-list'] a[href]:visible",
        ".product-list a[href]:visible",
        ".product-grid a[href]:visible",
        ".product-card a[href]:visible",
        "article a[href]:visible",
        "[data-href]:visible",
    ]
    for sel in selectors:
        try:
            links = page.locator(sel)
            n = min(links.count(), 60)
            for i in range(n):
                a = links.nth(i)
                try:
                    href = a.get_attribute("href") or a.get_attribute("data-href") or ""
                    if not looks_like_pdp_href(href):
                        continue
                    if not a.is_visible():
                        continue
                    a.click(timeout=4000)
                    try: page.wait_for_selector("h1", timeout=8000)
                    except Exception: pass
                    try: page.wait_for_load_state("networkidle", timeout=6000)
                    except Exception: pass
                    # PDP ONLY (no plain h1 fallback)
                    return looks_like_pdp(page)
                except Exception:
                    continue
        except Exception:
            continue
    return False

# ----- PDP/title verification gate -----

def _pdp_title(page) -> str:
    return (_first_text(page, ["h1", "h1.product-title", "h1[itemprop='name']"]) or "").strip()

_STOP = {"kg","tk","g","ml","l","dl","cl","kõik","ja","või","with","ilma","bio","mahe"}

def _tokens(s: str) -> set:
    # Unicode-aware tokenization, includes š/ž etc.
    raw = re.findall(r"[\w]+", (s or "").casefold(), flags=re.UNICODE)
    toks = set()
    for t in raw:
        if t.isdigit():
            continue
        if len(t) < 3:
            continue
        if t in _STOP:
            continue
        toks.add(t)
    return toks

def _pdp_matches_target(page, name: str, brand: str, amount: str) -> bool:
    # Hard gate: refuse search/listing pages entirely
    if is_search_page(page) or not looks_like_pdp(page):
        return False

    title = _pdp_title(page)
    tset = _tokens(title)
    want = _tokens(name) | _tokens(brand) | _tokens(amount)
    name_overlap  = len(_tokens(name) & tset)
    total_overlap = len(want & tset)
    # Looser rule: at least 1 token from name must match; overall >=2 preferred
    if name_overlap >= 1 and total_overlap >= 2:
        return True
    # If brand is distinctive and present, allow with any overlap
    if brand and (_tokens(brand) & tset):
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
    ean  = norm_ean(data.get("gtin14") or data.get("gtin13") or data.get("gtin") or data.get("ean"))
    sku  = (data.get("sku") or "").strip() or None
    return name, ean, sku

def _wait_pdp_facts(page):
    try:
        page.wait_for_selector("text=Ribakood", timeout=3000); return
    except Exception:
        pass
    try:
        page.evaluate("window.scrollBy(0, 800)")
        page.wait_for_selector("text=Ribakood", timeout=3000)
    except Exception:
        pass

def _ean_sku_via_label_xpath(page) -> Tuple[Optional[str], Optional[str]]:
    try:
        labels = ["ribakood", "ean", "triipkood"]
        xpaths = [
            "//*[contains(translate(normalize-space(.),'RIBAKOODEANTRIIPKOODÄÖÜÕŠŽ','ribakoodeantriipkoodäöüõšž') , '{lbl}')]"
            for lbl in labels
        ]
        css_pairs = [
            ("dt:has-text('Ribakood')", "dd"),
            ("tr:has(td:has-text('Ribakood'))", "td"),
            ("tr:has(th:has-text('Ribakood'))", "td"),
        ]

        def pick_digits(s: str) -> Optional[str]:
            if not s:
                return None
            m = re.search(r"(\d{13}|\d{8})", s)
            return m.group(1) if m else None

        for k_sel, v_sel in css_pairs:
            try:
                k = page.locator(k_sel).first
                if k and k.count() > 0:
                    zones = [k, k.locator(v_sel)]
                    for z in zones:
                        try:
                            t = (z.inner_text(timeout=800) or "").strip()
                            e = pick_digits(t)
                            if e:
                                sku = None
                                for near in [k, k.locator("xpath=.."), k.locator("xpath=following-sibling::*[1]")]:
                                    try:
                                        tt = (near.inner_text(timeout=800) or "").strip()
                                        m2 = re.search(r"([A-Z0-9_-]{6,})", tt, re.I)
                                        if m2:
                                            sku = sku or m2.group(1)
                                    except Exception:
                                        pass
                                return e, sku
                        except Exception:
                            pass
            except Exception:
                pass

        for xp in xpaths:
            try:
                lab = page.locator(f"xpath={xp}").first
                if not lab or lab.count() == 0:
                    continue
                zones = [
                    lab,
                    lab.locator("xpath=.."),
                    lab.locator("xpath=following-sibling::*[1]"),
                    lab.locator("xpath=following-sibling::*[2]"),
                    lab.locator("xpath=../following-sibling::*[1]"),
                ]
                sku_found = None
                for z in zones:
                    try:
                        t = (z.inner_text(timeout=800) or "").strip()
                        if not t:
                            continue
                        e = pick_digits(t)
                        if not sku_found:
                            msku = re.search(r"([A-Z0-9_-]{6,})", t, re.I)
                            if msku:
                                sku_found = msku.group(1)
                        if e:
                            return e, sku_found
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass
    return None, None

def _extract_ids_dom_bruteforce(page) -> Tuple[Optional[str], Optional[str]]:
    try:
        got = page.evaluate("""
        () => {
          const txt = n => (n && n.textContent || '').replace(/\\s+/g,' ').trim();
          const pickDigits = s => { const m = s && s.match(/(\\d{13}|\\d{8})/); return m ? m[1] : null; };
          const pickSKU = s => { const m = s && s.match(/([A-Z0-9_-]{6,})/i); return m ? m[1] : null; };

          let ean = null, sku = null;
          const nodes = Array.from(document.querySelectorAll('div,section,span,p,li,td,th,dd,dt,strong,em'));
          for (const el of nodes) {
            const t = txt(el);
            if (!t) continue;
            if (/(^|\\b)(ribakood|ean|triipkood)(\\b|:)/i.test(t)) {
              const zone = [el, el.parentElement, el.nextElementSibling, el.previousElementSibling, el.parentElement && el.parentElement.nextElementSibling];
              for (const z of zone) {
                const tt = txt(z);
                if (!ean) ean = pickDigits(tt);
                if (!sku) sku = pickSKU(tt);
                if (ean) break;
              }
            }
            if (ean) break;
          }
          return { ean, sku };
        }
        """)
        if got:
            return got.get("ean") or None, got.get("sku") or None
    except Exception:
        pass
    return None, None

def extract_ids_on_pdp(page) -> Tuple[Optional[str], Optional[str]]:
    sku_found: Optional[str] = None
    try: page.wait_for_timeout(350)
    except Exception: pass

    # JSON-LD
    try:
        scripts = page.locator("script[type='application/ld+json']")
        n = scripts.count()
        for i in range(n):
            try:
                _, ean, sku = parse_ld_product(scripts.nth(i).inner_text())
                if sku and not sku_found: sku_found = sku
                if ean: return ean, sku_found
            except Exception: pass
    except Exception: pass

    # meta itemprops
    meta_sku = _meta_content(page, ["meta[itemprop='sku']"])
    if meta_sku and not sku_found: sku_found = (meta_sku or "").strip() or None
    meta_ean = _meta_content(page, ["meta[itemprop='gtin13']", "meta[itemprop='gtin']"])
    if meta_ean:
        e = norm_ean(meta_ean)
        if e: return e, sku_found

    # facts/labels
    _wait_pdp_facts(page)
    e_spec, s_spec = _ean_sku_via_label_xpath(page)
    if e_spec: return norm_ean(e_spec), s_spec or sku_found

    # DOM brute
    e_dom, s_dom = _extract_ids_dom_bruteforce(page)
    if e_dom: return norm_ean(e_dom), s_dom or sku_found

    # JSON blobs / regex (label … digits)
    try:
        html = page.content() or ""
        m = JSON_EAN.search(html)
        if m:
            e = norm_ean(m.group("d"))
            if e: return e, sku_found or s_dom
    except Exception:
        pass
    try:
        html = page.content() or ""
        m = re.search(r"(ribakood|ean|triipkood)[\s\S]{0,800}?(\d{8,14})", html, re.I)
        if m: return norm_ean(m.group(2)), sku_found or s_dom
    except Exception:
        pass

    return None, sku_found or s_dom

def ensure_specs_open(page):
    for sel in ["button:has-text('Tooteinfo')", "button:has-text('Lisainfo')", "button:has-text('Tootekirjeldus')"]:
        try:
            if page.locator(sel).count():
                page.click(sel, timeout=1000)
                time.sleep(0.2)
        except Exception:
            pass

# --------- unified search→open helper ---------

def open_best_or_first(page, name: str, brand: str, amount: str) -> bool:
    """On a search page, **click** a PDP tile first; if that fails, try URL; then click again."""
    try:
        page.wait_for_selector("[data-testid='product-grid'], .product-list, article a[href]", timeout=6000)
    except Exception:
        pass

    # make sure results are visible and overlays closed
    kill_consents_and_overlays(page)
    try:
        page.evaluate("window.scrollBy(0, 300)")
    except Exception:
        pass

    # 1) Click a tile (preferred – reliably lands on PDP)
    if _click_first_search_tile(page):
        return True

    # 2) Navigate via best href
    hit = best_search_hit(page, name, brand, amount)
    if hit:
        try:
            page.goto(hit, timeout=25000, wait_until="domcontentloaded")
            try: page.wait_for_load_state("networkidle", timeout=9000)
            except Exception: pass
            # PDP ONLY (no plain h1 fallback)
            if looks_like_pdp(page):
                return True
        except Exception:
            pass

    # 3) Force lazy load + click again
    try:
        page.keyboard.press("End"); time.sleep(0.4)
        page.keyboard.press("Home"); time.sleep(0.2)
    except Exception:
        pass
    return _click_first_search_tile(page)

# ----------------- main probe flow -----------------

def process_one(page, name: str, brand: str, amount: str) -> Tuple[Optional[str], Optional[str]]:
    q_variants = []
    q_full = " ".join(x for x in [name or "", brand or "", amount or ""] if x).strip()
    if q_full: q_variants.append(q_full)
    if name:   q_variants.append(name.strip())
    if brand and name: q_variants.append(f"{name} {brand}")

    for q in q_variants:
        try:
            url = SEARCH_URL.format(q=q.replace(" ", "+"))
            page.goto(url, timeout=25000, wait_until="domcontentloaded")
            try: page.wait_for_load_state("networkidle", timeout=9000)
            except Exception: pass
            kill_consents_and_overlays(page)
        except PWTimeout:
            continue

        if is_search_page(page) or not looks_like_pdp(page):
            opened = open_best_or_first(page, name, brand, amount)
            if not opened:
                continue

        # PDP/title verification gate (Unicode-aware + refuses search pages)
        if not _pdp_matches_target(page, name, brand, amount):
            want = (name or "")[:60]
            got  = (_pdp_title(page) or "")[:120]
            print(f"[WRONG_PDP] want='{want}' got='{got}' url={page.url}")
            continue

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
        # locale helps the site render Estonian labels ("Ribakood" etc.)
        ctx = browser.new_context(locale="et-EE", viewport={"width": 1360, "height": 900})
        page = ctx.new_page()

        for row in batch:
            pid    = row["product_id"]
            name   = row["name"]   or ""
            brand  = row["brand"]  or ""
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

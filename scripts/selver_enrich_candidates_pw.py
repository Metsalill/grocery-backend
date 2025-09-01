#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrich selver_candidates rows that are missing EAN/SKU by visiting PDPs.

Usage:
  python scripts/selver_enrich_candidates_pw.py --limit 5000 --headless 1 --req-delay 0.5
Env:
  DATABASE_URL (required)
"""

import os, re, time, argparse, psycopg2, psycopg2.extras
from contextlib import closing
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

EAN_RE = re.compile(r"\b(\d{8,14})\b")
NBSP = "\u00A0"

def norm_ean(txt: str | None) -> str | None:
    if not txt: return None
    digits = re.sub(r"\D", "", txt)
    return digits if 8 <= len(digits) <= 14 else None

def get_conn():
    dsn = os.getenv("DATABASE_URL")
    assert dsn, "DATABASE_URL is required"
    return psycopg2.connect(dsn)

def fetch_targets(conn, limit: int):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT ext_id
            FROM selver_candidates
            WHERE ean_raw IS NULL
            ORDER BY last_seen DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [r["ext_id"] for r in cur.fetchall()]

def parse_label_table(page):
    """Return dict of {label_lower: value_text} from PDP spec table/blocks."""
    result = {}
    # Try common containers
    candidates = [
        "table", "dl", "[class*='product'] [class*='spec']",
        "[class*='product'] [class*='detail']",
        "section", "div"
    ]
    for sel in candidates:
        try:
            els = page.locator(sel)
            if els.count() == 0: continue
            html = els.first.inner_text().replace(NBSP, " ").strip()
            # Heuristic split by lines and colon
            for line in html.splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    k = k.strip().lower()
                    v = v.strip()
                    if k and v:
                        result[k] = v
        except Exception:
            pass
    return result

def pick_price(page):
    locs = [
        "[data-testid*='price']",
        "meta[itemprop='price'][content]",
        "[itemprop='price'][content]",
        "[class*='price']",
        "span:has-text('€')",
    ]
    for sel in locs:
        try:
            loc = page.locator(sel)
            if loc.count() == 0: continue
            if "meta" in sel or "content" in sel:
                v = loc.first.get_attribute("content")
            else:
                v = loc.first.inner_text().strip()
            if not v: continue
            v = v.replace(NBSP, " ").replace(",", ".")
            m = re.search(r"(\d+(?:\.\d{1,2})?)", v)
            if m: return float(m.group(1))
        except Exception:
            pass
    return None

def enrich_one(page, url: str):
    """Return dict with ean_raw, sku_raw, price (optional), name (optional), size_text (optional)."""
    out = {"ean_raw": None, "sku_raw": None, "price": None, "name": None, "size_text": None}
    page.goto(url, timeout=30000)
    page.wait_for_load_state("domcontentloaded")

    # Accept cookies best-effort
    for sel in ["button:has-text('Accept')","button:has-text('Nõustu')","button[aria-label*='accept']"]:
        try:
            btn = page.locator(sel)
            if btn.count() and btn.first.is_enabled():
                btn.first.click()
                break
        except Exception:
            pass

    # Name (hero heading)
    try:
        h = page.locator("h1")
        if h.count():
            out["name"] = h.first.inner_text().strip()
    except Exception:
        pass

    # Rough size from heading tail, e.g., "..., 400 g"
    if out["name"]:
        m = re.search(r"(\d{1,4}\s*(?:g|kg|ml|l|cl))\b", out["name"].lower())
        if m:
            out["size_text"] = m.group(1).replace(" ", NBSP)

    # Price
    out["price"] = pick_price(page)

    # Spec table parsing
    kv = parse_label_table(page)
    # Common keys in Estonian PDPs
    for k in ["ribakood", "ean", "ribakood (ean)", "barcode"]:
        if k in kv:
            out["ean_raw"] = kv[k]
            break
    for k in ["sku", "tootekood", "tunnus"]:
        if k in kv:
            out["sku_raw"] = kv[k]
            break

    # Fallback: search whole page text for 8–14 digit EAN
    if not out["ean_raw"]:
        try:
            txt = page.locator("body").inner_text()
            m = EAN_RE.search(txt)
            if m: out["ean_raw"] = m.group(1)
        except Exception:
            pass

    return out

def upsert_candidate(conn, url: str, data: dict):
    ean_norm = norm_ean(data.get("ean_raw"))
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE selver_candidates
               SET ean_raw   = COALESCE(%s, ean_raw),
                   ean_norm  = COALESCE(%s, ean_norm),
                   sku_raw   = COALESCE(%s, sku_raw),
                   name      = COALESCE(%s, name),
                   size_text = COALESCE(%s, size_text),
                   price     = COALESCE(%s, price),
                   currency  = COALESCE(%s, currency),
                   last_seen = now()
             WHERE ext_id = %s
            """,
            (
                data.get("ean_raw"),
                ean_norm,
                data.get("sku_raw"),
                data.get("name"),
                data.get("size_text"),
                data.get("price"),
                "EUR",
                url,
            ),
        )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--req-delay", type=float, default=0.5)
    args = ap.parse_args()

    with closing(get_conn()) as conn:
        conn.autocommit = False
        # Ensure columns / indexes exist
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE selver_candidates ADD COLUMN IF NOT EXISTS ean_norm text;")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_selver_candidates_ean ON selver_candidates (ean_norm);")
        conn.commit()

        targets = fetch_targets(conn, args.limit)
        if not targets:
            print("No selver_candidates rows missing EAN.")
            return

        enriched, failures = 0, 0
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=bool(args.headless))
            ctx = browser.new_context()
            page = ctx.new_page()

            for url in targets:
                try:
                    data = enrich_one(page, url)
                    upsert_candidate(conn, url, data)
                    conn.commit()
                    enriched += 1
                except PWTimeout:
                    conn.rollback()
                    failures += 1
                except Exception as e:
                    conn.rollback()
                    print(f"Failed {url}: {e}")
                    failures += 1
                time.sleep(args.req_delay)

            browser.close()

        print(f"Enriched {enriched}, failed {failures}, scanned {len(targets)}")

if __name__ == "__main__":
    main()

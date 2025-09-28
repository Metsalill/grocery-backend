#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop eCoop (region-specific) category → PDP crawler → CSV/optional DB upsert.

• Discovers product PDP links from eCoop category pages (pagination + "Load more")
• Extracts PDP data: name, brand, manufacturer (Tootja), image, price, EAN/GTIN, size
• Streams results to CSV and (optionally) upserts to Postgres

Env:
- COOP_UPSERT_DB=1 to enable DB upsert (requires asyncpg + DATABASE_URL).
- COOP_DEDUP_DB=1 to enable de-duplication against DB by (store_host, ean_norm).

Domains:
- Vändra → https://vandra.ecoop.ee
- Haapsalu → https://coophaapsalu.ee  (note: not *.ecoop.ee)
"""

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
import re
import signal
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set, Literal
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

# ---------- regexes & helpers ----------

SIZE_RE = re.compile(r"(\b\d+[\,\.]?\d*\s?(?:kg|g|l|ml|tk|pcs|x|×)\s?\d*\b)", re.IGNORECASE)
DIGITS_ONLY = re.compile(r"[^0-9]")
BRAND_KEYS_ET = ["Kaubamärk", "Bränd", "Brand"]
EAN_KEYS_ET = ["Ribakood", "EAN", "Tootekood", "GTIN"]
CTX_TA_CODE = re.compile(r"(?:Tootekood)\s*[:\-]?\s*(\d{8,14})", re.IGNORECASE)
CTX_MANUF   = re.compile(r"(?:Tootja|Valmistaja)\s*[:\-]?\s*([^\n<]{2,120})", re.IGNORECASE)

def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def clean_digits(s: str) -> str:
    return DIGITS_ONLY.sub("", s or "")

def normalize_ean(e: Optional[str]) -> Optional[str]:
    if not e:
        return None
    if e.strip() == "-":
        return None
    d = clean_digits(e)
    if len(d) in (8, 12, 13, 14):
        if len(d) == 14 and d.startswith("0"):
            d = d[1:]
        if len(d) == 12:  # UPC-A to EAN-13
            d = "0" + d
        return d
    return None

def likely_brand_from_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    token = (name or "").strip().split()[0]
    token = re.sub(r"[^\w\-’'`]+", "", token)
    if 2 <= len(token) <= 24:
        return token
    return None

# --- URL cleaning: keep pagination, drop junk ---
_ALLOWED_QUERY_KEYS = {"page"}

def clean_url_keep_allowed_query(u: str) -> str:
    if not u:
        return u
    s = urlsplit(u)
    keep = [(k, v) for (k, v) in parse_qsl(s.query or "", keep_blank_values=False)
            if k.lower() in _ALLOWED_QUERY_KEYS]
    q = urlencode(keep)
    return urlunsplit((s.scheme, s.netloc, s.path, q, ""))

def same_host(u: str, host: str) -> bool:
    try:
        return urlparse(u).netloc.lower() in ("", host.lower())
    except Exception:
        return False

def _normalize_region(region: str, category_candidates: List[str]) -> str:
    """
    Ensure region is a fully-qualified origin like 'https://vandra.ecoop.ee/'.
    If region lacks a host, try inferring from any absolute category URL.
    """
    r = (region or "").strip() or "https://vandra.ecoop.ee"
    if not re.match(r"^https?://", r, flags=re.I):
        r = "https://" + r
    u = urlparse(r)
    if not u.netloc:
        for c in category_candidates:
            cu = urlparse(c)
            if cu.scheme in ("http", "https") and cu.netloc:
                u = u._replace(scheme=cu.scheme or "https", netloc=cu.netloc, path="/", query="", fragment="")
                break
    if not u.netloc:
        u = urlparse("https://vandra.ecoop.ee/")
    return urlunsplit((u.scheme, u.netloc, "/", "", ""))

# ---------- store_host mapping ----------
def map_store_host(region_url: str) -> str:
    """
    Canonical store_host expected in DB:
      - vandra.ecoop.ee  → vandra.ecoop.ee  (use as-is)
      - haapsalu.ecoop.ee (legacy) → coophaapsalu.ee (live site)
      - coophaapsalu.ee → coophaapsalu.ee (as-is)
      - otherwise: use the parsed netloc lowercased
    """
    host = urlparse(region_url).netloc.lower()
    if host == "haapsalu.ecoop.ee":
        return "coophaapsalu.ee"
    return host  # vandra.ecoop.ee stays as vandra.ecoop.ee

# ---------- Playwright (required) ----------
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout  # noqa: F401
except Exception as e:  # pragma: no cover
    async_playwright = None
    _IMPORT_ERROR = e

async def wait_cookie_banner(page: Any):
    try:
        for sel in [
            'button:has-text("Nõustun")',
            'button:has-text("Olen nõus")',
            'button:has-text("Accept")',
            '[data-testid="accept-cookies"] button',
        ]:
            b = page.locator(sel)
            if await b.count() > 0:
                await b.first.click(timeout=1000)
                break
    except Exception:
        pass

async def collect_category_product_links(page: Any, category_url: str, page_limit: int, req_delay: float, max_depth: int = 2) -> List[str]:
    base = urlparse(category_url)
    base_host = base.netloc

    def norm_abs(u: str) -> str:
        u = urljoin(f"{base.scheme}://{base.netloc}{base.path}", u or "")
        return clean_url_keep_allowed_query(u)

    def is_product(u: str) -> bool:
        return "/toode/" in u

    def is_category(u: str) -> bool:
        return "/tootekategooria/" in u

    seen_products: set[str] = set()
    seen_pages: set[str] = set()
    queue: List[Tuple[str, int]] = [(clean_url_keep_allowed_query(category_url), 0)]

    while queue:
        url, depth = queue.pop(0)
        if not url or url in seen_pages or not same_host(url, base_host):
            continue
        seen_pages.add(url)

        try:
            await page.goto(url, wait_until="domcontentloaded")
        except Exception as e:
            print(f"[warn] category goto fail: {url} -> {e}")
            continue

        await wait_cookie_banner(page)

        stable_rounds = 0
        for _ in range(1000):
            try:
                hrefs = await page.eval_on_selector_all('a[href*="/toode/"]', "els => els.map(e => e.href)")
            except Exception:
                hrefs = []
            for h in hrefs:
                h = norm_abs(h)
                if h and is_product(h):
                    seen_products.add(h)

            clicked = False
            for sel in [
                'button:has-text("Lae veel")',
                'button:has-text("Näita rohkem")',
                '[data-testid="load-more"]',
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click()
                        await page.wait_for_timeout(int(req_delay * 1000))
                        clicked = True
                        break
                except Exception:
                    pass

            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
            await page.wait_for_timeout(int(req_delay * 1000))

            before = len(seen_products)
            try:
                hrefs2 = await page.eval_on_selector_all('a[href*="/toode/"]', "els => els.map(e => e.href)")
            except Exception:
                hrefs2 = []
            for h in hrefs2:
                h = norm_abs(h)
                if h and is_product(h):
                    seen_products.add(h)

            if len(seen_products) == before and not clicked:
                stable_rounds += 1
            else:
                stable_rounds = 0

            if stable_rounds >= 3:
                break
            if page_limit > 0 and len(seen_products) >= page_limit:
                break

        # pagination links
        try:
            next_links = await page.eval_on_selector_all(
                'a[rel="next"], a[href*="?page="]', 'els => els.map(e => e.getAttribute("href"))'
            )
        except Exception:
            next_links = []
        for nl in next_links:
            nl = norm_abs(nl or "")
            if nl and same_host(nl, base_host) and nl not in seen_pages:
                queue.append((nl, depth))

        # subcategories
        if depth < 2:
            try:
                subcats = await page.eval_on_selector_all(
                    'a[href*="/tootekategooria/"]', "els => els.map(e => e.getAttribute('href'))"
                )
            except Exception:
                subcats = []
            for sc in subcats:
                sc = norm_abs(sc or "")
                if sc and is_category(sc) and same_host(sc, base_host) and sc not in seen_pages:
                    queue.append((sc, depth + 1))

        if page_limit > 0 and len(seen_products) >= page_limit:
            break

    return list(seen_products)

async def parse_json_ld(page: Any) -> Dict:
    data: Dict = {}
    try:
        scripts = await page.eval_on_selector_all('script[type="application/ld+json"]', "els => els.map(e => e.textContent)")
        for s in scripts:
            try:
                obj = json.loads(s)
            except Exception:
                continue
            items = obj if isinstance(obj, list) else [obj]
            for it in items:
                if isinstance(it, dict) and (it.get("@type") in ("Product", "Schema:Product", "schema:Product") or "offers" in it):
                    for k, v in it.items():
                        if k not in data and v:
                            data[k] = v
    except Exception:
        pass
    return data

# --- price extractor (handles split ints/cents like "1 99 €") ---
async def extract_visible_price(page: Any) -> Optional[float]:
    candidates: List[float] = []

    js = """
    () => {
      const sel = [
        '[data-testid="product-price"]',
        '[data-test="product-price"]',
        '.product-price',
        '.price',
        '.current-price',
        '[class*="price"]'
      ];
      const out = [];
      const reSpace = /[\\s\\u00A0\\u2009\\u202F]+/g;
      for (const s of sel) {
        for (const el of document.querySelectorAll(s)) {
          const t = (el.textContent || '').trim().replace(reSpace, ' ');
          if (t) out.push(t);
          const intNode = el.querySelector('[data-testid*="int"], [data-test*="int"], .price__int, .int, .integer, .whole');
          const centNode = el.querySelector('[data-testid*="cent"], [data-test*="cent"], .price__cent, .cents, .cent, sup');
          const curNode = el.querySelector('[data-testid*="cur"], [data-test*="cur"], .price__cur, .currency');
          const whole = intNode && (intNode.textContent || '').replace(/\\D+/g,'');
          const cents = centNode && (centNode.textContent || '').replace(/\\D+/g,'');
          const curTxt = (curNode && curNode.textContent) || (el.textContent || '');
          if (whole && cents && /€/.test(curTxt)) {
            out.push(`${whole},${cents} €`);
          }
        }
      }
      return out;
    }
    """
    try:
        texts = await page.evaluate(js)
        for s in texts or []:
            s = (s or "").replace("\xa0", " ")
            m = re.search(r"(\d+[.,]\d{2})\s*€", s)
            if m:
                candidates.append(float(m.group(1).replace(",", ".")))
                continue
            m2 = re.search(r"\b(\d+)\s+(\d{2})\s*€", s)
            if m2:
                candidates.append(float(f"{m2.group(1)}.{m2.group(2)}"))
    except Exception:
        pass

    if not candidates:
        try:
            all_txt = await page.locator("xpath=//*[contains(., '€')]").all_inner_texts()
            for s in (all_txt or [])[:200]:
                s = (s or "").replace("\xa0", " ")
                m = re.search(r"(\d+[.,]\d{2})\s*€", s)
                if m:
                    candidates.append(float(m.group(1).replace(",", ".")))
                else:
                    m2 = re.search(r"\b(\d+)\s+(\d{2})\s*€", s)
                    if m2:
                        candidates.append(float(f"{m2.group(1)}.{m2.group(2)}"))
        except Exception:
            pass

    if not candidates:
        return None
    return round(max(candidates), 2)

async def extract_text_after_label(page: Any, label: str) -> Optional[str]:
    try:
        nodes = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]")
        n = await nodes.count()
        for i in range(min(n, 8)):
            html = await nodes.nth(i).inner_html()
            txt  = await nodes.nth(i).inner_text()
            m = re.search(rf"{re.escape(label)}\s*[:\-]?\s*([^\n<]{{2,120}})", txt, flags=re.I)
            if m:
                return m.group(1).strip()
            sib = await nodes.nth(i).evaluate_handle("el => el.nextElementSibling && el.nextElementSibling.textContent")
            try:
                sval = await sib.json_value()
                if isinstance(sval, str) and sval.strip():
                    return sval.strip()
            except Exception:
                pass
            m2 = re.search(rf"{re.escape(label)}\s*[:\-]?\s*</[^>]+>\s*([^<]{{2,120}})", html or "", flags=re.I)
            if m2:
                return m2.group(1).strip()
    except Exception:
        pass
    return None

async def extract_pdp(
    page: Any,
    url: str,
    req_delay: float,
    store_host: str,
    goto_strategy: Literal["auto","domcontentloaded","networkidle","load"]="auto",
    nav_timeout_ms: int = 45000
) -> Dict:
    # resilient navigation (networkidle often never fires)
    async def safe_goto(target_url: str) -> str:
        order: List[str]
        if goto_strategy in ("domcontentloaded", "networkidle", "load"):
            order = [goto_strategy]
        else:
            order = ["domcontentloaded", "load", "networkidle"]  # auto preference
        last_err = None
        for ws in order:
            try:
                await page.goto(target_url, wait_until=ws, timeout=nav_timeout_ms)
                return ws
            except Exception as e:
                last_err = e
        try:
            await page.goto(target_url, timeout=nav_timeout_ms)
            return "none"
        except Exception:
            if last_err:
                raise last_err
            raise

    _ = await safe_goto(url)
    await wait_cookie_banner(page)
    try:
        await page.wait_for_selector("h1, [data-testid='product-title']", timeout=5000)
    except Exception:
        pass
    await page.wait_for_timeout(int(max(req_delay, 0.8) * 1000))

    name = None
    for sel in ["h1", '[data-testid="product-title"]', "article h1"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                txt = await loc.first.text_content()
                if txt:
                    name = txt.strip()
                    break
        except Exception:
            pass

    ld = await parse_json_ld(page)

    brand = None
    manufacturer = None
    if isinstance(ld.get("brand"), dict):
        brand = ld["brand"].get("name")
    elif isinstance(ld.get("brand"), (str, int)):
        brand = str(ld["brand"]).strip() or None
    if isinstance(ld.get("manufacturer"), dict):
        manufacturer = ld["manufacturer"].get("name")

    if not manufacturer:
        manufacturer = await extract_text_after_label(page, "Tootja") or await extract_text_after_label(page, "Valmistaja")
        if not manufacturer:
            try:
                full = await page.inner_text("body")
                m = CTX_MANUF.search(full or "")
                if m:
                    manufacturer = m.group(1).strip()
            except Exception:
                pass

    if not brand:
        try:
            spec_xpath = ("xpath=//dt[normalize-space()[contains(., $key)]]/following-sibling::dd[1]"
                          " | //tr[th[normalize-space()[contains(., $key)]]]/td[1]")
            for key in BRAND_KEYS_ET:
                loc = page.locator(spec_xpath.replace("$key", key))
                if await loc.count() > 0:
                    txt = await loc.first.text_content()
                    if txt:
                        brand = txt.strip()
                        break
        except Exception:
            pass

    if not brand:
        brand = likely_brand_from_name(name) or manufacturer

    price = None
    currency = None
    offers = ld.get("offers")
    if isinstance(offers, dict):
        price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
        currency = offers.get("priceCurrency") or offers.get("priceSpecification", {}).get("priceCurrency")
    if price is None:
        price = await extract_visible_price(page)
    if not currency:
        currency = "EUR"

    image_url = None
    try:
        if ld.get("image"):
            image_url = ld["image"] if isinstance(ld["image"], str) else (ld["image"][0] if isinstance(ld["image"], list) and ld["image"] else None)
        if not image_url:
            image_url = await page.get_attribute('meta[property="og:image"]', "content")
    except Exception:
        pass

    ean_raw = None
    val = await extract_text_after_label(page, "Tootekood")
    if not val:
        try:
            body_txt = await page.inner_text("body")
            m = CTX_TA_CODE.search(body_txt or "")
            if m:
                val = m.group(1)
        except Exception:
            pass
    if val:
        ean_raw = val.strip()

    if not ean_raw:
        for key in ["gtin13", "gtin", "gtin8", "gtin12"]:
            if ld.get(key):
                ean_raw = str(ld[key])
                break

    if not ean_raw:
        try:
            spec_xpath = ("xpath=//dt[normalize-space()[contains(., $key)]]/following-sibling::dd[1]"
                          " | //tr[th[normalize-space()[contains(., $key)]]]/td[1]")
            for key in EAN_KEYS_ET:
                loc = page.locator(spec_xpath.replace("$key", key))
                if await loc.count() > 0:
                    txt = await loc.first.text_content()
                    if txt:
                        ean_raw = txt.strip()
                        break
        except Exception:
            pass

    size_text = None
    if name:
        m = SIZE_RE.search(name)
        if m:
            size_text = m.group(1)

    ext_id = None
    if ean_raw and normalize_ean(ean_raw):
        ext_id = normalize_ean(ean_raw)
    if not ext_id:
        m = re.search(r"/toode/(\d+)", url)
        if m:
            ext_id = m.group(1)
    if not ext_id:
        m2 = re.search(r"/toode/([^/?#]+)/?", url)
        if m2:
            ext_id = m2.group(1)
    if not ext_id:
        ext_id = url.rstrip("/").split("/")[-1]

    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None

    return {
        "chain": "Coop",
        "store_host": store_host,
        "channel": "online",
        "ext_id": ext_id,
        "ean_raw": ean_raw,
        "ean_norm": normalize_ean(ean_raw),
        "name": name,
        "size_text": size_text,
        "brand": brand,
        "manufacturer": manufacturer,
        "price": price,
        "currency": currency,
        "image_url": image_url,
        "url": url,
    }

# ---------- outputs ----------

CSV_COLS = [
    "chain","store_host","channel","ext_id","ean_raw","ean_norm","name",
    "size_text","brand","manufacturer","price","currency","image_url","url",
]

def _ensure_csv_with_header(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not out_path.exists() or out_path.stat().st_size == 0:
        with out_path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_COLS).writeheader()

def append_csv(rows: List[Dict], out_path: Path) -> None:
    if not rows:
        return
    write_header = (not out_path.exists()) or out_path.stat().st_size == 0
    with out_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        if write_header:
            w.writeheader()
        for r in rows:
            row = {k: r.get(k) for k in CSV_COLS}
            if row.get("price") is None:
                row["price"] = ""
            else:
                try:
                    row["price"] = f"{float(row['price']):.2f}"
                except Exception:
                    pass
            w.writerow(row)

# ---------- DB helpers ----------

async def _fetch_existing_gtins(store_host: str) -> Set[str]:
    if os.environ.get("COOP_DEDUP_DB", "0").lower() not in ("1", "true"):
        return set()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return set()
    try:
        import asyncpg  # type: ignore
    except Exception:
        return set()
    try:
        conn = await asyncpg.connect(dsn)
    except Exception:
        return set()
    try:
        rows = await conn.fetch(
            "SELECT DISTINCT ean_norm FROM public.staging_coop_products "
            "WHERE store_host=$1 AND ean_norm IS NOT NULL",
            store_host,
        )
        return {r["ean_norm"] for r in rows if r["ean_norm"]}
    finally:
        await conn.close()

async def maybe_upsert_db(rows: List[Dict]) -> None:
    """Best-effort upsert. Never crash the crawl on DB errors."""
    if not rows:
        print("[info] No rows to upsert.")
        return
    if os.environ.get("COOP_UPSERT_DB", "0").lower() not in ("1", "true"):
        print("[info] DB upsert disabled (COOP_UPSERT_DB != 1)")
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("[info] DATABASE_URL not set; skipping DB upsert")
        return

    try:
        import asyncpg  # type: ignore
    except Exception:
        print("[warn] asyncpg not installed; skipping DB upsert")
        return

    table = "staging_coop_products"

    try:
        conn = await asyncpg.connect(dsn)
    except Exception as e:
        print(f"[warn] Could not connect to DB for upsert ({e!r}). Skipping DB upsert.")
        return

    try:
        try:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=$1)",
                table,
            )
        except Exception as e:
            print(f"[warn] Failed to check table existence: {e!r}. Skipping DB upsert.")
            return

        if not exists:
            print(f"[info] Table {table} does not exist → skipping DB upsert.")
            return

        stmt = f"""
            INSERT INTO {table}
              (store_host, ext_id, name, brand, manufacturer, ean_raw, ean_norm, size_text, price, currency, image_url, url)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (store_host, ext_id) DO UPDATE SET
              name         = COALESCE(EXCLUDED.name,         {table}.name),
              brand        = COALESCE(NULLIF(EXCLUDED.brand,''),        {table}.brand),
              manufacturer = COALESCE(NULLIF(EXCLUDED.manufacturer,''), {table}.manufacturer),
              ean_raw      = COALESCE(EXCLUDED.ean_raw,      {table}.ean_raw),
              ean_norm     = COALESCE(EXCLUDED.ean_norm,     {table}.ean_norm),
              size_text    = COALESCE(EXCLUDED.size_text,    {table}.size_text),
              price        = COALESCE(EXCLUDED.price,        {table}.price),
              currency     = COALESCE(EXCLUDED.currency,     {table}.currency),
              image_url    = COALESCE(EXCLUDED.image_url,    {table}.image_url),
              url          = COALESCE(EXCLUDED.url,          {table}.url),
              scraped_at   = now();
        """

        payload = []
        for r in rows:
            if not r.get("ext_id"):
                continue
            pr = r.get("price")
            try:
                pr = round(float(pr), 2) if pr is not None else None
            except Exception:
                pr = None
            payload.append((
                r.get("store_host"), r.get("ext_id"), r.get("name"),
                r.get("brand"), r.get("manufacturer"),
                r.get("ean_raw"), r.get("ean_norm"), r.get("size_text"),
                pr, r.get("currency") or "EUR", r.get("image_url"), r.get("url")
            ))

        if not payload:
            print("[warn] No rows with ext_id — skipped DB upsert")
            return

        try:
            await conn.executemany(stmt, payload)
            print(f"[ok] Upserted {len(payload)} rows into {table}")
        except Exception as e:
            print(f"[warn] Upsert failed ({e!r}). Skipping DB upsert.")
    finally:
        try:
            await conn.close()
        except Exception:
            pass

# ---------- router (block trackers only) ----------
async def _route_handler(route):
    try:
        req = route.request
        if req.resource_type in ("image", "media", "font"):
            return await route.abort()
        url = req.url
        if any(h in url for h in [
            "googletagmanager.com","google-analytics.com","doubleclick.net",
            "facebook.net","connect.facebook.net","hotjar","fullstory",
            "cdn.segment.com","intercom",
        ]):
            return await route.abort()
        return await route.continue_()
    except Exception:
        try:
            return await route.continue_()
        except Exception:
            return

# ---------- runner ----------
async def run_ecoop(args, categories: List[str], base_region: str, on_rows) -> None:
    if async_playwright is None:
        raise RuntimeError(f"Playwright is not installed but eCoop crawling was requested: {_IMPORT_ERROR}")
    # map ecoop region to canonical store_host
    store_host = map_store_host(base_region)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=bool(int(args.headless)))
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
        )
        try:
            context.set_default_navigation_timeout(max(15000, int(args.nav_timeout)))
            context.set_default_timeout(max(15000, int(args.nav_timeout)))
        except Exception:
            pass

        await context.route("**/*", _route_handler)

        try:
            for cat in categories:
                print(f"[cat] {cat}")
                page = await context.new_page()
                try:
                    links = await collect_category_product_links(page, cat, args.page_limit, args.req_delay, max_depth=2)
                finally:
                    await page.close()

                if args.max_products > 0:
                    links = links[:args.max_products]

                sem = asyncio.Semaphore(args.pdp_workers)

                async def worker(url: str) -> Optional[Dict]:
                    async with sem:
                        p = await context.new_page()
                        try:
                            return await extract_pdp(
                                p, url, args.req_delay, store_host,
                                goto_strategy=args.goto_strategy,
                                nav_timeout_ms=int(args.nav_timeout)
                            )
                        except Exception as e:
                            print(f"[warn] PDP fail {url}: {e}")
                            return None
                        finally:
                            await p.close()

                pending_batch: List[Dict] = []
                tasks = [asyncio.create_task(worker(u)) for u in links]
                for coro in asyncio.as_completed(tasks):
                    r = await coro
                    if r:
                        pending_batch.append(r)
                        if len(pending_batch) >= 25:
                            on_rows(pending_batch); pending_batch = []
                if pending_batch:
                    on_rows(pending_batch)
        finally:
            await context.close(); await browser.close()

# ---------- main ----------
async def main(args):
    categories: List[str] = []
    if args.categories_multiline:
        categories.extend([ln.strip() for ln in args.categories_multiline.splitlines() if ln.strip()])
    if args.categories_file and Path(args.categories_file).exists():
        categories.extend([ln.strip() for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines() if ln.strip()])
    if not categories:
        print("[error] No category URLs provided. Pass --categories-multiline or --categories-file.")
        sys.exit(2)

    base_region = _normalize_region(args.region, categories)

    def norm_url(u: str) -> str:
        absu = urljoin(base_region, u)
        return clean_url_keep_allowed_query(absu)

    categories = [norm_url(u) for u in categories]

    if args.cat_shards > 1:
        if args.cat_index < 0 or args.cat_index >= args.cat_shards:
            print(f"[error] --cat-index must be in [0, {args.cat_shards-1}]")
            sys.exit(2)
        categories = [u for i, u in enumerate(categories) if i % args.cat_shards == args.cat_index]
        print(f"[shard] Using {len(categories)} categories for shard {args.cat_index}/{args.cat_shards}")

    out_path = Path(args.out)
    if out_path.is_dir() or str(out_path).endswith("/"):
        out_path = out_path / f"coop_ecoop_{now_stamp()}.csv"
    print(f"[out] streaming CSV → {out_path}")

    if args.write_empty_csv:
        _ensure_csv_with_header(out_path)

    all_rows: List[Dict] = []

    def on_rows(batch: List[Dict]):
        nonlocal all_rows
        if not batch:
            return
        append_csv(batch, out_path)
        all_rows.extend(batch)
        print(f"[stream] +{len(batch)} rows (total {len(all_rows)})")

    # graceful shutdown so Playwright can close cleanly (avoids Node EPIPE)
    def _sig_handler(signum, frame):
        print(f"[warn] received signal {signum}; CSV already streamed. Exiting 130 gracefully.")
        sys.exit(130)  # raise SystemExit -> executes finally blocks

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT,  _sig_handler)

    await run_ecoop(args, categories, base_region, on_rows)

    gtin_ok = sum(1 for r in all_rows if (r.get("ean_norm") or (r.get("ean_raw") and r.get("ean_raw") != "-")))
    brand_ok = sum(1 for r in all_rows if (r.get("brand") or r.get("manufacturer")))
    print(f"[stats] rows={len(all_rows)}  gtin_present={gtin_ok}  brand_or_manufacturer_present={brand_ok}")
    print(f"[ok] CSV ready: {out_path}")
    await maybe_upsert_db(all_rows)

def parse_args():
    p = argparse.ArgumentParser(description="Coop eCoop category crawler → PDP extractor")
    p.add_argument("--region", default="https://vandra.ecoop.ee",
                   help="Base region origin, e.g. https://vandra.ecoop.ee or https://coophaapsalu.ee")
    p.add_argument("--categories-multiline", dest="categories_multiline", default="",
                   help="Newline-separated category URLs or paths")
    p.add_argument("--categories-file", dest="categories_file", default="", help="Path to txt file with category URLs")
    p.add_argument("--page-limit", type=int, default=0, help="Hard cap of product links per category (0=all)")
    p.add_argument("--max-products", type=int, default=0, help="Global cap per category after discovery (0=all)")
    p.add_argument("--headless", default="1", help="1/0 headless")
    p.add_argument("--req-delay", type=float, default=0.5, help="Seconds between ops")
    p.add_argument("--pdp-workers", type=int, default=4, help="Concurrent PDP tabs per category")
    p.add_argument("--cat-shards", type=int, default=1, help="Total number of category shards")
    p.add_argument("--cat-index", type=int, default=0, help="This shard index (0-based)")
    p.add_argument("--out", default="out/coop_ecoop.csv", help="CSV file or output directory")
    p.add_argument("--write-empty-csv", action="store_true", default=True, help="Always write CSV header even if no rows.")
    # robustness flags
    p.add_argument("--goto-strategy", choices=["auto","domcontentloaded","networkidle","load"],
                   default="auto", help="Playwright wait_until strategy for PDP navigation.")
    p.add_argument("--nav-timeout", default="45000",
                   help="Navigation timeout in milliseconds for PDP pages.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop eCoop (multi-region) & Wolt venue crawler → CSV/DB-friendly

Modes
- ecoop: Crawls eCoop category pages with Playwright (handles JS/lazy-load + pagination).
         PDP extraction: title, brand, manufacturer (Tootja), image, price, EAN/GTIN,
         Tootekood, etc. Writes CSV; optional Postgres upsert.
- wolt : Tries server data first, else Playwright fallback. In the PW fallback we:
         • capture JSON used by Wolt SPA,
         • open each product modal, click “Toote info”, switch into the iframe
           and scrape GTIN + Tarnija info.

DB alignment
- Target table: public.staging_coop_products
- PRIMARY KEY (store_host, ext_id)

Flag note
- Boolean flag: `--wolt-force-playwright` (presence = True). Also respects env
  WOLT_FORCE_PLAYWRIGHT=1/true (for CI convenience).
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
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

# Optional Playwright (needed for ecoop and Wolt PW fallback)
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except Exception:  # pragma: no cover
    async_playwright = None  # loaded only when needed

# ---------- regexes & helpers ----------

SIZE_RE = re.compile(r"(\b\d+[\,\.]?\d*\s?(?:kg|g|l|ml|tk|pcs|x|×)\s?\d*\b)", re.IGNORECASE)
DIGITS_ONLY = re.compile(r"[^0-9]")
PRICE_TOKEN = re.compile(r"(\d+[.,]\d{2})\s*€", re.U)

BRAND_KEYS_ET = ["Kaubamärk", "Bränd", "Brand"]
EAN_KEYS_ET = ["Ribakood", "EAN", "Tootekood", "GTIN"]

CTX_TA_CODE = re.compile(r"(?:Tootekood)\s*[:\-]?\s*(\d{8,14})", re.IGNORECASE)
CTX_MANUF   = re.compile(r"(?:Tootja|Valmistaja)\s*[:\-]?\s*([^\n<]{2,120})", re.IGNORECASE)

GTIN_IN_ANY      = re.compile(r"\b(?:GTIN|EAN|Ribakood)\b[^\d]{0,20}(\d{8,14})", re.I)
TARNIJA_BLOCK_RE = re.compile(r"(?:Tarnija info|Supplier)\s*[\n\r]+([^\n\r]{2,200})", re.I)

def str2bool(v: Optional[str]) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}

def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def clean_digits(s: str) -> str:
    return DIGITS_ONLY.sub("", s or "")

def normalize_ean(e: Optional[str]) -> Optional[str]:
    if not e:
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

def looks_like_unit_price(text: str) -> bool:
    t = (text or "").strip().lower()
    return ("/" in t) or any(u in t for u in ("€/kg", "€ / kg", "€/l", "€ / l", "€/tk", "€ / tk"))

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

# ---------- ecoop (Playwright) utilities ----------

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

# ---------- price helpers ----------

def _parse_wolt_price(value: Any) -> Optional[float]:
    try:
        if isinstance(value, (int, float)):
            return round(float(value) / (100.0 if float(value) >= 50 else 1.0), 2)
        if isinstance(value, str):
            return round(float(value.replace(",", ".").strip()), 2)
        if isinstance(value, dict):
            for k in ("value", "amount", "price", "current", "total", "unit"):
                if k in value:
                    return _parse_wolt_price(value[k])
    except Exception:
        return None
    return None

async def extract_visible_price(page: Any) -> Optional[float]:
    candidates: List[Tuple[float, str]] = []
    selectors = [
        '[data-testid="product-price"]', ".product-price", ".price", ".current-price", '[class*="price"]'
    ]
    for sel in selectors:
        try:
            locs = page.locator(sel)
            n = await locs.count()
            for i in range(min(n, 10)):
                txt = await locs.nth(i).inner_text()
                if not txt or looks_like_unit_price(txt):
                    continue
                m = PRICE_TOKEN.search(txt.replace("\xa0", ""))
                if m:
                    candidates.append((float(m.group(1).replace(",", ".")), txt))
        except Exception:
            pass

    if not candidates:
        try:
            all_txt = await page.locator("xpath=//*[contains(., '€')]").all_inner_texts()
            for txt in all_txt[:100]:
                if not txt or looks_like_unit_price(txt):
                    continue
                m = PRICE_TOKEN.search(txt.replace("\xa0", ""))
                if m:
                    candidates.append((float(m.group(1).replace(",", ".")), txt))
        except Exception:
            pass

    if not candidates:
        return None
    return max(candidates, key=lambda x: x[0])[0]

# ---------- PDP extraction (ecoop) ----------

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

async def extract_pdp(page: Any, url: str, req_delay: float, store_host: str) -> Dict:
    await page.goto(url, wait_until="domcontentloaded")
    await wait_cookie_banner(page)
    await page.wait_for_timeout(int(req_delay * 1000))

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
        for k in ("sku", "productID", "mpn"):
            v = ld.get(k)
            if v:
                ext_id = str(v)
                break
    if not ext_id:
        ext_id = url.rstrip("/").split("/")[-1]

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
        "price": float(price) if price is not None else None,
        "currency": currency,
        "image_url": image_url,
        "url": url,
    }

# ---------- WOLT parsing ----------

def _html_get_next_data(html: str) -> Optional[Dict]:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S | re.I)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    m2 = re.search(r'__NEXT_DATA__\s*=\s*({.*?})\s*</script>', html, re.S | re.I)
    if m2:
        try:
            return json.loads(m2.group(1))
        except Exception:
            pass
    return None

def _html_get_build_id(html: str) -> Optional[str]:
    m = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
    return m.group(1) if m else None

def _html_get_apollo_state(html: str) -> Optional[Dict]:
    m = re.search(r'__APOLLO_STATE__\s*=\s*({.*?})\s*;', html, re.S | re.I)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def _walk_collect_items(obj: Any, found: Dict[str, Dict]) -> None:
    if isinstance(obj, dict):
        has_name = isinstance(obj.get("name"), str) and obj.get("name").strip()
        price_keys = ("price", "baseprice", "base_price", "unit_price", "total_price", "current_price")
        has_priceish = any(k in obj for k in price_keys)
        if has_name and has_priceish:
            key = str(obj.get("id") or obj.get("slug") or obj.get("name"))
            found.setdefault(key, obj)
        for v in obj.values():
            _walk_collect_items(v, found)
    elif isinstance(obj, list):
        for v in obj:
            _walk_collect_items(v, found)

def _search_info_label(value: Any, *labels: str) -> Optional[str]:
    lbls = {l.lower() for l in labels}
    try:
        if isinstance(value, list):
            for it in value:
                if isinstance(it, dict):
                    lab = str(it.get("label") or it.get("title") or "").strip().lower()
                    val = it.get("value") or it.get("text") or it.get("content")
                    if lab in lbls and isinstance(val, str) and val.strip():
                        return val.strip()
        if isinstance(value, dict):
            for v in value.values():
                out = _search_info_label(v, *labels)
                if out:
                    return out
    except Exception:
        return None
    return None

def _first_str(obj: Dict, *keys: str) -> Optional[str]:
    for k in keys:
        if k in obj and isinstance(obj[k], str) and obj[k].strip():
            return obj[k].strip()
    return None

def _first_urlish(obj: Dict, *keys: str) -> Optional[str]:
    for k in keys:
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            for kk in ("url", "src"):
                if isinstance(v.get(kk), str) and v.get(kk).strip():
                    return v.get(kk).strip()
    return None

def _extract_wolt_row(item: Dict, category_url: str, store_host: str) -> Dict:
    name = str(item.get("name") or "").strip() or None
    price = None
    for k in ("price", "baseprice", "base_price", "current_price", "total_price", "unit_price"):
        if k in item:
            price = _parse_wolt_price(item[k])
            if price is not None:
                break
    image_url = _first_urlish(item, "image", "image_url", "imageUrl", "media")
    manufacturer = (item.get("supplier") or
                    _search_info_label(item, "Tarnija info", "Tarnija", "Tootja", "Valmistaja", "Supplier", "Manufacturer") or
                    _first_str(item, "supplier", "manufacturer", "producer"))
    ean_raw  = item.get("gtin") or (_search_info_label(item, "GTIN", "EAN", "Ribakood") or _first_str(item, "gtin", "ean", "barcode"))
    ean_norm = normalize_ean(ean_raw)
    brand = _first_str(item, "brand") or likely_brand_from_name(name) or manufacturer
    size_text = _search_info_label(item, "Size", "Kogus", "Maht", "Kaal")
    if not size_text and name:
        m = SIZE_RE.search(name)
        if m:
            size_text = m.group(1)
    ext_id = ean_norm or str(item.get("id") or item.get("slug") or name or "")
    url = category_url
    if item.get("id"):
        url = f"{category_url}#item-{item.get('id')}"
    return {
        "chain": "Coop",
        "store_host": store_host,
        "channel": "wolt",
        "ext_id": ext_id,
        "ean_raw": ean_raw,
        "ean_norm": ean_norm,
        "name": name,
        "size_text": size_text,
        "brand": brand,
        "manufacturer": manufacturer,
        "price": price if price is not None else None,
        "currency": "EUR",
        "image_url": image_url,
        "url": url,
    }

def _format_price_for_csv(v):
    if isinstance(v, (int, float)):
        return f"{float(v):.2f}"
    try:
        return f"{float(str(v).replace(',', '.')):.2f}"
    except Exception:
        return v

# ---------- outputs (streaming-friendly) ----------

CSV_COLS = [
    "chain","store_host","channel","ext_id","ean_raw","ean_norm","name",
    "size_text","brand","manufacturer","price","currency","image_url","url",
]

def append_csv(rows: List[Dict], out_path: Path) -> None:
    if not rows:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = (not out_path.exists()) or out_path.stat().st_size == 0
    with out_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        if write_header:
            w.writeheader()
        for r in rows:
            row = {k: r.get(k) for k in CSV_COLS}
            row["price"] = _format_price_for_csv(row.get("price"))
            w.writerow(row)

async def maybe_upsert_db(rows: List[Dict]) -> None:
    if not rows:
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
    conn = await asyncpg.connect(dsn)
    try:
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
            table,
        )
        if not exists:
            print(f"[info] Table {table} does not exist → skipping DB upsert.")
            return

        stmt = f"""
            INSERT INTO {table}
              (store_host, ext_id, name, brand, manufacturer, ean_raw, ean_norm, size_text, price, currency, image_url, url)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (store_host, ext_id) DO UPDATE SET
              name         = COALESCE(EXCLUDED.name,         {table}.name),
              brand        = COALESCE(EXCLUDED.brand,        {table}.brand),
              manufacturer = COALESCE(EXCLUDED.manufacturer, {table}.manufacturer),
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

        await conn.executemany(stmt, payload)
        print(f"[ok] Upserted {len(payload)} rows into {table}")
    finally:
        await conn.close()

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

# ---------- Wolt PW-network fallback ----------

def _browser_headers(referer: Optional[str] = None) -> Dict[str, str]:
    h = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept-Encoding": "gzip",
    }
    if referer:
        h["Referer"] = referer
    return h

async def _fetch_html(url: str) -> str:
    import urllib.request, gzip, io
    req = urllib.request.Request(url, headers=_browser_headers())
    with urllib.request.urlopen(req) as resp:  # nosec
        data = resp.read()
        if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
        return data.decode("utf-8", errors="replace")

async def _fetch_json(url: str) -> Optional[Dict]:
    import urllib.request, gzip, io
    req = urllib.request.Request(url, headers=_browser_headers())
    with urllib.request.urlopen(req) as resp:  # nosec
        data = resp.read()
        if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
        try:
            return json.loads(data.decode("utf-8", errors="replace"))
        except Exception:
            return None

def _wolt_store_host(sample_url: str) -> str:
    m = re.search(r"/venue/([^/]+)", sample_url)
    if m:
        return f"wolt:{m.group(1)}"
    return urlparse(sample_url).netloc.lower()

async def _load_wolt_payload(url: str) -> Optional[Dict]:
    html = await _fetch_html(url)
    nd = _html_get_next_data(html)
    if nd:
        return nd
    build_id = _html_get_build_id(html)
    if build_id:
        u = urlparse(url)
        path = u.path.rstrip("/")
        jd = await _fetch_json(f"{u.scheme}://{u.netloc}/_next/data/{build_id}{path}.json")
        if jd:
            return jd
    apollo = _html_get_apollo_state(html)
    if apollo:
        return {"apollo": apollo}
    return None

async def _maybe_collect_json(resp, out_list: List[Any]):
    try:
        ct = (resp.headers.get("content-type") or "").lower()
        if "application/json" not in ct:
            return
        url = resp.url or ""
        if any(x in url for x in ["wolt.com", "wolt-static-assets", "restaurant-api.wolt", "graphql"]):
            txt = await resp.text()
            if not txt:
                return
            try:
                obj = json.loads(txt)
                out_list.append(obj)
            except Exception:
                pass
    except Exception:
        pass

# ====== iframe scraping helpers (Wolt “Toote info”) ======

async def _get_info_iframe(page) -> Any:
    """Wait for the product-info iframe and return the Frame object."""
    await page.wait_for_timeout(200)
    for _ in range(25):  # ~5s
        for f in page.frames:
            try:
                u = (f.url or "").lower()
                n = (f.name or "").lower()
                if "prodinfo.wolt" in u or "product-info-iframe" in u or "product-info-iframe" in n:
                    return f
            except Exception:
                pass
        if await page.locator("iframe#product-info-iframe, iframe[data-test-id='product-info-iframe'], iframe[src*='prodinfo.wolt.com']").count() > 0:
            await page.wait_for_timeout(120)
        await page.wait_for_timeout(120)
    raise RuntimeError('Product info iframe did not appear')

async def _read_iframe_text_strict(page) -> str:
    frame = await _get_info_iframe(page)
    try:
        txt = await frame.locator("body").inner_text(timeout=2000)
        if not txt or not txt.strip():
            raise RuntimeError("Empty iframe body")
        return txt
    except PWTimeout:
        raise RuntimeError("Timed out reading iframe body")

# --- robust modal opener ---

async def _open_product_modal_for_name(page, target_name: str) -> bool:
    """
    Try to open product modal by multiple strategies.
    Returns True if opened, False otherwise.
    """
    name_part = target_name.split(",")[0].strip()

    async def _try_click(locator):
        try:
            if await locator.count() > 0:
                el = locator.first
                await el.scroll_into_view_if_needed(timeout=1500)
                await el.click(timeout=1500)
                await page.get_by_role("dialog").wait_for(timeout=2500)
                return True
        except Exception:
            return False
        return False

    for _ in range(12):
        if await _try_click(page.get_by_role("link", name=target_name, exact=True)): return True
        if await _try_click(page.get_by_role("button", name=target_name, exact=True)): return True
        if await _try_click(page.locator(f'img[alt="{target_name}"]')): return True
        if await _try_click(page.locator(f'img[alt*="{name_part}"]')): return True
        if await _try_click(page.locator("a").filter(has_text=target_name)): return True
        if await _try_click(page.locator("a").filter(has_text=name_part)): return True
        if await _try_click(page.locator("article,div,button").filter(has_text=name_part)): return True
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(200)

    print(f'[warn] Could not open modal for product: "{target_name}"')
    return False

async def _open_product_modal(page, item: Dict) -> bool:
    """Try open modal by ID first, then fallback to name. Returns True/False."""
    iid = item.get("id")
    if iid:
        sel = f'a[href*="itemid-{iid}"]'
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.scroll_into_view_if_needed(timeout=1500)
                await loc.first.click(timeout=1500)
                await page.get_by_role("dialog").wait_for(timeout=2500)
                return True
        except Exception:
            pass
    return await _open_product_modal_for_name(page, str(item.get("name") or "").strip())

# --- modal enrichment (skip-on-fail) ---

async def _wolt_enrich_with_modal(page, items: List[Dict], category_url: str,
                                  max_to_probe: int = 120, strict_toote_info: bool = True) -> None:
    """Try to enrich items with Toote info. Skip products that fail instead of raising."""
    probed = 0
    for item in items:
        if probed >= max_to_probe:
            break
        name = str(item.get("name") or "").strip()
        if not name:
            continue

        opened = await _open_product_modal(page, item)
        if not opened:
            print(f'[warn] Skipping "{name}" — could not open modal ({category_url})')
            continue

        info_clicked = False
        try:
            info = page.locator('a:has-text("Toote info"), button:has-text("Toote info")')
            if await info.count() > 0:
                await info.first.click(timeout=1500)
                info_clicked = True
            else:
                print(f'[warn] "Toote info" not present for "{name}" ({category_url})')
        except Exception:
            print(f'[warn] Could not click "Toote info" for "{name}"')
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            continue

        if info_clicked:
            try:
                info_text = await _read_iframe_text_strict(page)
                if info_text:
                    m1 = GTIN_IN_ANY.search(info_text or "")
                    if m1:
                        item["gtin"] = m1.group(1).strip()
                    m2 = TARNIJA_BLOCK_RE.search(info_text or "")
                    if m2:
                        supplier = (m2.group(1) or "").strip()
                        if supplier:
                            item["supplier"] = supplier
                            if not item.get("brand"):
                                item["brand"] = supplier
            except Exception as e:
                print(f'[warn] Failed to read iframe for "{name}": {e}')

        try:
            close_btn = page.locator('button[aria-label*="Close"], button:has-text("×")')
            if await close_btn.count() > 0:
                await close_btn.first.click()
            else:
                await page.keyboard.press("Escape")
        except Exception:
            pass

        probed += 1
        await page.wait_for_timeout(160)

async def _wolt_capture_category_with_playwright(cat_url: str, strict_toote_info: bool = True) -> List[Dict]:
    """Open a Wolt category with Playwright, capture JSON responses, then force Toote info scraping."""
    if async_playwright is None:
        raise RuntimeError("Playwright is required for Wolt fallback but is not installed.")

    found: Dict[str, Dict] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
        )

        async def route_filter(route):
            try:
                url = route.request.url
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

        await context.route("**/*", route_filter)

        page = await context.new_page()
        json_blobs: List[Any] = []
        page.on("response", lambda resp: asyncio.create_task(_maybe_collect_json(resp, json_blobs)))

        try:
            await page.goto(cat_url, wait_until="networkidle")
            await wait_cookie_banner(page)
            for _ in range(5):
                await page.mouse.wheel(0, 1500)
                await page.wait_for_timeout(300)

            # Try window states too
            for varname in ["__APOLLO_STATE__", "__NEXT_DATA__", "__NUXT__", "__INITIAL_STATE__", "__REACT_QUERY_STATE__", "__REDUX_STATE__"]:
                try:
                    data = await page.evaluate(f"window.{varname} || null")
                    if data:
                        json_blobs.append(data)
                except Exception:
                    pass

            for blob in json_blobs:
                try:
                    _walk_collect_items(blob, found)
                except Exception:
                    pass

            items = list(found.values())

            # Toote info enrichment (skip-on-fail)
            await _wolt_enrich_with_modal(page, items, cat_url, max_to_probe=120, strict_toote_info=strict_toote_info)
            return items

        finally:
            await context.close()
            await browser.close()

# ---------- runners (streaming) ----------

async def run_ecoop(args, categories: List[str], base_region: str, on_rows) -> None:
    if async_playwright is None:
        raise RuntimeError("Playwright is not installed but mode=ecoop was requested.")
    store_host = urlparse(base_region).netloc.lower()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=bool(int(args.headless)))
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
        )
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
                            return await extract_pdp(p, url, args.req_delay, store_host)
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

async def run_wolt(args, categories: List[str], on_rows) -> None:
    store_host = _wolt_store_host(categories[0] if categories else args.region)
    force_pw = bool(args.wolt_force_playwright or str2bool(os.getenv("WOLT_FORCE_PLAYWRIGHT")))
    # We allow skipping on failures; no hard fail.
    strict_toote_info = True

    for cat in categories:
        print(f"[cat-wolt] {cat}")
        try:
            payload = None if force_pw else await _load_wolt_payload(cat)
            if not payload:
                print(f"[info] forcing Playwright fallback for {cat}")
                items = await _wolt_capture_category_with_playwright(cat, strict_toote_info=strict_toote_info)
                rows = [_extract_wolt_row(item, cat, store_host) for item in items]
                if args.max_products and args.max_products > 0:
                    rows = rows[: args.max_products]
                print(f"[info] category rows: {len(rows)} (pw-fallback)")
                on_rows(rows)
                continue

            found: Dict[str, Dict] = {}
            _walk_collect_items(payload, found)
            rows: List[Dict] = [_extract_wolt_row(item, cat, store_host) for item in found.values()]
            if args.max_products and args.max_products > 0:
                rows = rows[: args.max_products]
            print(f"[info] category rows: {len(rows)}")
            on_rows(rows)
        except Exception as e:
            # Log category-level error and continue to next category
            print(f"[warn] Wolt category failed {cat}: {e}")

# ---------- main ----------

async def run(args):
    categories: List[str] = []
    if args.categories_multiline:
        categories.extend([ln.strip() for ln in args.categories_multiline.splitlines() if ln.strip()])
    if args.categories_file and Path(args.categories_file).exists():
        categories.extend([ln.strip() for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines() if ln.strip()])
    if not categories:
        print("[error] No category URLs provided. Pass --categories-multiline or --categories-file.")
        sys.exit(2)

    base_region = args.region.strip()
    if not re.match(r"^https?://", base_region, flags=re.I):
        base_region = "https://" + base_region
    if not base_region.endswith("/"):
        base_region += "/"

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
        out_path = out_path / f"coop_products_{now_stamp()}.csv"
    print(f"[out] streaming CSV → {out_path}")

    all_rows: List[Dict] = []

    def on_rows(batch: List[Dict]):
        nonlocal all_rows
        if not batch:
            return
        append_csv(batch, out_path)
        all_rows.extend(batch)
        print(f"[stream] +{len(batch)} rows (total {len(all_rows)})")

    def _sig_handler(signum, frame):
        print(f"[warn] received signal {signum}; CSV already streamed. Exiting 130.")
        os._exit(130)

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT,  _sig_handler)

    if args.mode.lower() == "wolt" or ("wolt.com" in base_region.lower()):
        await run_wolt(args, categories, on_rows)
    else:
        await run_ecoop(args, categories, base_region, on_rows)

    print(f"[ok] CSV ready: {out_path}")
    await maybe_upsert_db(all_rows)

def parse_args():
    p = argparse.ArgumentParser(description="Coop eCoop/Wolt category crawler → PDP extractor")
    p.add_argument("--mode", default="ecoop", help="Crawler mode: ecoop or wolt")
    p.add_argument("--region", default="https://vandra.ecoop.ee", help="Base region (ecoop) or venue root (wolt).")
    p.add_argument("--categories-multiline", dest="categories_multiline", default="",
                   help="Newline-separated category URLs or paths")
    p.add_argument("--categories-file", dest="categories_file", default="", help="Path to txt file with category URLs")
    p.add_argument("--page-limit", type=int, default=0, help="(ecoop) Hard cap of product links per category (0=all)")
    p.add_argument("--max-products", type=int, default=0, help="Global cap per category after discovery (0=all)")
    p.add_argument("--headless", default="1", help="(ecoop) 1/0 headless")
    p.add_argument("--req-delay", type=float, default=0.5, help="(ecoop) Seconds between ops")
    p.add_argument("--pdp-workers", type=int, default=4, help="(ecoop) Concurrent PDP tabs per category")
    p.add_argument("--cat-shards", type=int, default=1, help="Total number of category shards")
    p.add_argument("--cat-index", type=int, default=0, help="This shard index (0-based)")
    p.add_argument("--out", default="out/coop_products.csv", help="CSV file or output directory")
    p.add_argument("--wolt-force-playwright", action="store_true",
                   help="Force Playwright network fallback for Wolt categories.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("[info] aborted by user")

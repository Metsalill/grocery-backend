#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop on Wolt category crawler → CSV/optional DB upsert.

Fast path:
- Load Wolt category; try __NEXT_DATA__/buildId JSON, Apollo state, etc.
- Collect item objects from server payloads.

Fallback:
- Playwright page load, scroll, capture JSON blobs, tile prices, and venueId via modal.
- Enrich items via https://prodinfo.wolt.com/<lang>/<venueId>/<itemId> to fetch GTIN & Supplier.

Noise guard:
- Filters out cookie/consent/etc. junk.
- Accepts items with valid GTIN immediately; accepts GTIN '-' only if name looks productish and price present.

Env:
- WOLT_FORCE_PLAYWRIGHT=1 to force PW fallback
- WOLT_PROBE_LIMIT (default 2000) to cap prodinfo probes
- COOP_UPSERT_DB=1 / COOP_DEDUP_DB=1 (same semantics as ecoop)
"""

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
import random
import re
import signal
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

DIGITS_ONLY = re.compile(r"[^0-9]")
SIZE_RE = re.compile(r"(\b\d+[\,\.]?\d*\s?(?:kg|g|l|ml|tk|pcs|x|×)\s?\d*\b)", re.IGNORECASE)
HEX24_RE = re.compile(r"\b[a-f0-9]{24}\b", re.I)

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

# ---------- HTTP utils with backoff ----------
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]
_GLOBAL_UA = random.choice(_UA_POOL)

def _browser_headers(referer: Optional[str] = None) -> Dict[str, str]:
    h = {
        "User-Agent": _GLOBAL_UA,
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

async def _sleep_backoff(attempt: int, retry_after: Optional[str], base: float = 1.2) -> None:
    if retry_after and retry_after.isdigit():
        wait_s = max(0.0, float(retry_after))
    else:
        wait_s = base * (2 ** attempt) + random.uniform(0.3, 0.9)
    await asyncio.sleep(wait_s)

async def _fetch_html(url: str, max_tries: int = 7) -> str:
    import urllib.request, gzip, io
    for attempt in range(max_tries):
        try:
            req = urllib.request.Request(url, headers=_browser_headers())
            with urllib.request.urlopen(req) as resp:  # nosec
                data = resp.read()
                if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
                    data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
                return data.decode("utf-8", errors="replace")
        except Exception as e:
            retry_after = None
            try:
                if hasattr(e, "code") and e.code == 429 and hasattr(e, "headers"):
                    retry_after = e.headers.get("Retry-After")
            except Exception:
                pass
            if attempt < max_tries - 1:
                await _sleep_backoff(attempt, retry_after)
                continue
            raise

async def _fetch_json(url: str, max_tries: int = 7) -> Optional[Dict]:
    import urllib.request, gzip, io
    for attempt in range(max_tries):
        try:
            req = urllib.request.Request(url, headers=_browser_headers())
            with urllib.request.urlopen(req) as resp:  # nosec
                data = resp.read()
                if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
                    data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
                try:
                    return json.loads(data.decode("utf-8", errors="replace"))
                except Exception:
                    return None
        except Exception as e:
            retry_after = None
            try:
                if hasattr(e, "code") and e.code == 429 and hasattr(e, "headers"):
                    retry_after = e.headers.get("Retry-After")
            except Exception:
                pass
            if attempt < max_tries - 1:
                await _sleep_backoff(attempt, retry_after, base=1.0)
                continue
            return None

# ---------- Wolt parsers ----------
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
        priceish_keys = ("price", "baseprice", "base_price", "unit_price", "total_price", "current_price")
        has_priceish = any(k in obj for k in priceish_keys)
        has_idish = any(k in obj for k in ("id", "itemId", "itemID", "_id", "slug"))
        if has_name and (has_priceish or has_idish):
            key = str(obj.get("id") or obj.get("slug") or obj.get("name"))
            found.setdefault(key, obj)
        for v in obj.values():
            _walk_collect_items(v, found)
    elif isinstance(obj, list):
        for v in obj:
            _walk_collect_items(v, found)

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

# ---------- enrichment via prodinfo.wolt.com ----------
PRODINFO_GTIN_RE = re.compile(r"<h3[^>]*>\s*GTIN\s*</h3>\s*<p[^>]*>(\d{8,14})</p>", re.I)
PRODINFO_SUPPLIER_RE = re.compile(
    r"<h3[^>]*>\s*(?:Tarnija info|Tootja info|Supplier)\s*</h3>\s*<p[^>]*>([^<]{2,200})</p>",
    re.I
)
PRODINFO_JSONLD_GTIN_RE = re.compile(r'"gtin(?:8|12|13|14)"\s*:\s*"(\d{8,14})"', re.I)
PRODINFO_TITLE_RE = re.compile(r"<h2[^>]*>([^<]{2,200})</h2>", re.I)

async def _fetch_prodinfo_fields(lang: str, venue_id: str, item_id: str) -> Dict[str, Optional[str]]:
    url = f"https://prodinfo.wolt.com/{lang}/{venue_id}/{item_id}"
    try:
        html = await _fetch_html(url)
    except Exception:
        return {"gtin": None, "supplier": None, "name": None}

    gtin = None
    m = PRODINFO_GTIN_RE.search(html or "")
    if m:
        gtin = normalize_ean(m.group(1))
    if not gtin:
        m2 = PRODINFO_JSONLD_GTIN_RE.search(html or "")
        if m2:
            gtin = normalize_ean(m2.group(1))

    supplier = None
    m3 = PRODINFO_SUPPLIER_RE.search(html or "")
    if m3:
        supplier = (m3.group(1) or "").strip()

    name = None
    m4 = PRODINFO_TITLE_RE.search(html or "")
    if m4:
        name = (m4.group(1) or "").strip()

    return {"gtin": gtin, "supplier": supplier, "name": name}

async def _enrich_items_via_prodinfo(items: List[Dict], lang: str, venue_id: str,
                                     max_to_probe: Optional[int] = None) -> None:
    if max_to_probe is None:
        try:
            max_to_probe = int(os.getenv("WOLT_PROBE_LIMIT", "2000"))
        except Exception:
            max_to_probe = 2000

    def _needs_gtin(it: Dict) -> bool:
        return normalize_ean(it.get("gtin") or it.get("ean") or it.get("ean_norm")) is None

    queue = sorted(items, key=lambda it: (not _needs_gtin(it), str(it.get("id") or "")))

    probed = 0
    for it in queue:
        if max_to_probe and probed >= max_to_probe:
            break
        iid = str(it.get("id") or "")
        if not iid or not HEX24_RE.fullmatch(iid):
            continue
        try:
            info = await _fetch_prodinfo_fields(lang, venue_id, iid)
            gt = normalize_ean(info.get("gtin"))
            if gt:
                it["gtin"] = gt
            if info.get("supplier"):
                it["supplier"] = info["supplier"]
                if not it.get("brand"):
                    it["brand"] = info["supplier"]
            if info.get("name") and not it.get("name"):
                it["name"] = info["name"]
        except Exception:
            pass
        probed += 1
        await asyncio.sleep(0.08)  # probe throttle

# ---------- noise guards ----------
_DENY_EXACT = {"web tracking bundle", "functional", "required", "marketing", "analytics",
               "privacy", "cookie", "consent", "pant", "deposit"}
_DENY_SUBSTR = {"cookie", "consent", "tracking", "privacy"}
_DENY_PREFIX = {"otsi", "avasta", "tulemused"}  # search/UX headings
_DENY_LOCATIONS = {"aabenraa","aabybro","aachen","aalborg","õnekoski"}

def _looks_like_noise(name: Optional[str]) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return True
    if n in _DENY_EXACT or n in _DENY_LOCATIONS:
        return True
    if any(n.startswith(p) for p in _DENY_PREFIX):
        return True
    if any(s in n for s in _DENY_SUBSTR):
        return True
    if len(n.split()) == 1 and len(n) <= 6:
        return True
    return False

def _valid_productish(name: Optional[str], price: Optional[float], gtin_norm: Optional[str],
                      url: Optional[str], brand: Optional[str], manufacturer: Optional[str]) -> bool:
    if gtin_norm:
        return not _looks_like_noise(name)
    if price is None or price <= 0:
        return False
    if _looks_like_noise(name):
        return False
    has_size = bool(SIZE_RE.search(name or ""))
    if has_size or brand or manufacturer:
        return True
    n = (name or "").strip()
    return bool(n and len(n) >= 6 and len(n.split()) >= 2)

def _parse_wolt_price(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, dict):
            for k in ("value", "amount", "current", "total", "price", "baseprice", "base_price", "unit_price"):
                if k in value:
                    out = _parse_wolt_price(value[k])
                    if out is not None:
                        return round(float(out), 2)
            return None
        if isinstance(value, int):
            return round(value / 100.0, 2)
        if isinstance(value, float):
            return round((value / 100.0 if value > 100 else value), 2)
        if isinstance(value, str):
            s = value.strip().replace("\xa0", "").replace("€", "")
            if "," in s or "." in s:
                try:
                    return round(float(s.replace(",", ".")), 2)
                except Exception:
                    pass
            d = clean_digits(s)
            if d:
                try:
                    return round(int(d) / 100.0, 2)
                except Exception:
                    pass
    except Exception:
        return None
    return None

def _extract_row_from_item(item: Dict, category_url: str, store_host: str) -> Optional[Dict]:
    name = str(item.get("name") or "").strip() or None
    price = None
    for k in ("price", "baseprice", "base_price", "current_price", "total_price", "unit_price"):
        if k in item:
            price = _parse_wolt_price(item[k])
            if price is not None:
                break

    image_url = _first_urlish(item, "image", "image_url", "imageUrl", "media")
    manufacturer = (item.get("supplier")
                    or _search_info_label(item, "Tarnija info", "Tarnija", "Tootja", "Valmistaja", "Supplier", "Manufacturer")
                    or _first_str(item, "supplier", "manufacturer", "producer"))

    ean_raw  = item.get("gtin") or (_search_info_label(item, "GTIN", "EAN", "Ribakood") or _first_str(item, "gtin", "ean", "barcode"))
    ean_norm = normalize_ean(ean_raw)
    brand = _first_str(item, "brand") or likely_brand_from_name(name) or manufacturer

    size_text = _search_info_label(item, "Size", "Kogus", "Maht", "Kaal")
    if not size_text and name:
        m = SIZE_RE.search(name)
        if m:
            size_text = m.group(1)

    if not _valid_productish(name, price, ean_norm, category_url, brand, manufacturer):
        return None

    if ean_norm:
        ext_id = ean_norm
    else:
        iid = str(item.get("id") or "").lower()
        if not iid or not HEX24_RE.fullmatch(iid):
            return None
        ext_id = f"iid:{iid}"

    url = category_url
    if item.get("id"):
        url = f"{category_url}#item-{item.get('id')}"

    return {
        "chain": "Coop",
        "store_host": store_host,
        "channel": "wolt",
        "ext_id": ext_id,
        "ean_raw": ean_raw if ean_raw not in (None, "") else "-",
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

# ---------- fast path loader ----------
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

# ---------- Playwright fallback ----------
try:
    from playwright.async_api import async_playwright  # type: ignore
except Exception:
    async_playwright = None

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

async def _scrape_tile_prices(page) -> Dict[str, float]:
    try:
        data = await page.evaluate(
            """(() => {
                const out = [];
                const anchors = Array.from(document.querySelectorAll('a[href*="item"]'));
                for (const a of anchors) {
                    const href = a.getAttribute('href') || a.href || '';
                    const m = href && href.match(/(?:itemid-|item-)([a-f0-9]{24})/i);
                    if (!m) continue;
                    const card = a.closest('article,

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
            # Handle HTTP 429 specifically (urllib.error.HTTPError)
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

# --- NEW: robust tile extractor that works for DIV/button tiles as well as anchors
async def _scrape_tiles_full(page):
    """
    Returns dict keyed by 24-hex iid:
      { id, name, price, image_url }
    """
    data = await page.evaluate(
        """(() => {
            const seen = {};
            const out = [];
            const rx = /(?:itemid-|item-)([a-f0-9]{24})/i;

            // Scan any element whose outerHTML mentions itemid-
            const all = Array.from(document.querySelectorAll('body *'));
            for (const el of all) {
              const html = el.outerHTML || '';
              const m = html.match(rx);
              if (!m) continue;
              const id = m[1].toLowerCase();
              if (seen[id]) continue; seen[id] = true;

              // build a tile context
              const tile = el.closest('article, li, div, a, button') || el;

              // name guess
              let name = '';
              const nameEl = tile.querySelector('h3, h4, strong, [aria-label], img[alt]');
              if (nameEl) {
                name = (nameEl.getAttribute('aria-label') || nameEl.textContent || nameEl.getAttribute('alt') || '').trim();
              }

              // price guess
              let price = null;
              const t = (tile.textContent || '').replace(/\\s+/g,' ');
              const pm = t.match(/(\\d+[.,]\\d{2})\\s*€/);
              if (pm) price = parseFloat(pm[1].replace(',', '.'));

              // image
              let image_url = '';
              const im = tile.querySelector('img');
              if (im) image_url = im.src || im.getAttribute('src') || '';

              out.push({ id, name, price, image_url });
            }
            return out;
        })()"""
    )
    # pack
    by_id: Dict[str, Dict[str, Optional[str]]] = {}
    for row in (data or []):
        try:
            iid = str(row.get("id") or "").lower()
            if not iid or not re.match(r"^[a-f0-9]{24}$", iid):
                continue
            price = row.get("price")
            by_id[iid] = {
                "id": iid,
                "name": (row.get("name") or "").strip() or None,
                "price": float(price) if price is not None else None,
                "image_url": row.get("image_url") or None,
            }
        except Exception:
            pass
    return by_id

# legacy simple price scraper (kept for compatibility)
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
                    const card = a.closest('article, a, div') || a;
                    const txt = (card && card.textContent) ? card.textContent : '';
                    const mt = txt.match(/(\\d+[.,]\\d{2})\\s*€/);
                    if (mt) {
                        const raw = mt[1].replace(',', '.');
                        const val = parseFloat(raw);
                        if (!isNaN(val)) out.push([m[1], val]);
                    }
                }
                return out;
            })()"""
        )
        prices: Dict[str, float] = {}
        for iid, val in data or []:
            prices[str(iid).lower()] = float(val)
        return prices
    except Exception:
        return {}

async def _extract_venue_id_via_modal(page) -> Optional[str]:
    try:
        a = page.locator('a[href*="itemid-"], a[href*="item-"]').first
        if await a.count() == 0:
            return None
        await a.scroll_into_view_if_needed(timeout=1500)
        await a.click(timeout=2000)
        for _ in range(20):
            frames = page.frames
            for fr in frames:
                try:
                    src = (fr.url or "").lower()
                    m = re.search(r"/([a-f0-9]{24})/", src)
                    if "prodinfo.wolt.com" in src and m:
                        return m.group(1)
                except Exception:
                    pass
            await page.wait_for_timeout(150)
    except Exception:
        pass
    return None

async def _goto_with_backoff(page, url: str, max_tries: int, nav_timeout_ms: int, strategies: List[str]):
    last_err = None
    for attempt in range(max_tries):
        for ws in strategies:
            try:
                resp = await page.goto(url, wait_until=ws, timeout=nav_timeout_ms)
                return resp
            except Exception as e:
                last_err = e
        await _sleep_backoff(attempt, retry_after=None, base=1.2)
    if last_err:
        raise last_err

async def _capture_with_playwright(cat_url: str, headless: bool, req_delay: float,
                                   goto_strategy: str, nav_timeout_ms: int):
    if async_playwright is None:
        raise RuntimeError("Playwright is required for Wolt fallback but is not installed.")

    found: Dict[str, Dict] = {}
    blobs: List[Any] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=bool(int(headless)))
        context = await browser.new_context(
            user_agent=_GLOBAL_UA,
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
        )
        try:
            context.set_default_navigation_timeout(max(15000, int(nav_timeout_ms)))
            context.set_default_timeout(max(15000, int(nav_timeout_ms)))
        except Exception:
            pass

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
        collected_blobs: List[Any] = []
        page.on("response", lambda resp: asyncio.create_task(_maybe_collect_json(resp, collected_blobs)))

        html = ""
        tile_prices: Dict[str, float] = {}
        venue_id: Optional[str] = None
        try:
            strategies = [goto_strategy] if goto_strategy in ("domcontentloaded","networkidle","load") else ["domcontentloaded","load","networkidle"]
            await _goto_with_backoff(page, cat_url, max_tries=6, nav_timeout_ms=nav_timeout_ms, strategies=strategies)

            await wait_cookie_banner(page)
            # gentle scroll with throttle to let lazy content render
            for _ in range(10):
                await page.mouse.wheel(0, 1500)
                await page.wait_for_timeout(int(max(req_delay, 0.4)*1000 + random.uniform(250, 900)))

            # robust tile scan
            tiles = await _scrape_tiles_full(page)
            # legacy price-only fallback for anchors
            legacy_prices = await _scrape_tile_prices(page)
            for iid, p in legacy_prices.items():
                if iid in tiles and tiles[iid].get("price") is None:
                    tiles[iid]["price"] = p

            # leak global state blobs
            for varname in ["__APOLLO_STATE__", "__NEXT_DATA__", "__NUXT__", "__INITIAL_STATE__", "__REACT_QUERY_STATE__", "__REDUX_STATE__"]:
                try:
                    data = await page.evaluate(f"window.{varname} || null")
                    if data:
                        collected_blobs.append(data)
                except Exception:
                    pass

            html = await page.content()

            def _walk(o):
                if isinstance(o, dict):
                    has_name = isinstance(o.get("name"), str) and o.get("name").strip()
                    priceish_keys = ("price", "baseprice", "base_price", "unit_price", "total_price", "current_price")
                    has_priceish = any(k in o for k in priceish_keys)
                    has_idish = any(k in o for k in ("id", "itemId", "itemID", "_id", "slug"))
                    if has_name and (has_priceish or has_idish):
                        key = str(o.get("id") or o.get("slug") or o.get("name"))
                        found.setdefault(key, o)
                    for v in o.values():
                        _walk(v)
                elif isinstance(o, list):
                    for v in o:
                        _walk(v)

            for blob in collected_blobs:
                try:
                    _walk(blob)
                except Exception:
                    pass

            # If no items were discovered from blobs, synthesize from tiles
            if not found and tiles:
                for iid, t in tiles.items():
                    found[iid] = {"id": iid, "name": t.get("name"), "price": t.get("price"), "image": t.get("image_url")}
            else:
                # Enrich existing with tile info where missing
                for key, it in list(found.items()):
                    iid = str(it.get("id") or "").lower()
                    if iid and iid in tiles:
                        t = tiles[iid]
                        it.setdefault("name", t.get("name"))
                        if (it.get("price") in (None, 0)) and t.get("price") is not None:
                            it["price"] = t.get("price")
                        it.setdefault("image", t.get("image_url"))

            if not found:
                ids = set(re.findall(r"(?:itemid-|item-)([a-f0-9]{24})", html or "", re.I))
                for iid in ids:
                    found[iid] = {"id": iid}

            # venue id
            m = re.search(r"/menu-images/([a-f0-9]{24})/", html, re.I)
            if m:
                venue_id = m.group(1)
            if not venue_id:
                venue_id = await _extract_venue_id_via_modal(page)

            # expose tile prices map for outer join (iid->price)
            tile_prices = {iid: (v.get("price") if v else None) for iid, v in tiles.items() if v and v.get("price") is not None}

            return list(found.values()), collected_blobs, html, tile_prices, venue_id

        finally:
            await context.close()
            await browser.close()

# ---------- runner ----------
def _lang_from_url(u: str) -> str:
    m = re.search(r"https?://[^/]+/([a-z]{2})(?:/|$)", u, re.I)
    return (m.group(1).lower() if m else "et")

async def run_wolt(args, categories: List[str], on_rows) -> None:
    force_pw = bool(args.force_playwright or str(os.getenv("WOLT_FORCE_PLAYWRIGHT", "")).lower() in ("1","true","t","yes","y","on"))

    for idx, cat in enumerate(categories):
        # prefer explicit --store-host; otherwise infer from category URL
        store_host_cat = args.store_host.strip() if args.store_host else _wolt_store_host(cat)
        print(f"[cat-wolt] {cat}")

        # small stagger between categories to reduce rate-limit bursts
        if idx > 0:
            await asyncio.sleep(float(args.req_delay) + random.uniform(0.5, 1.2))

        try:
            payload = None if force_pw else await _load_wolt_payload(cat)

            if payload:
                found: Dict[str, Dict] = {}
                _walk_collect_items(payload, found)
                items = list(found.values())
                blobs = [payload]
                html = ""
                tile_prices = {}
                venue_id = None
            else:
                print(f"[info] forcing Playwright fallback for {cat}")
                items, blobs, html, tile_prices, venue_id = await _capture_with_playwright(
                    cat,
                    headless=bool(int(args.headless)),
                    req_delay=float(args.req_delay),
                    goto_strategy=args.goto_strategy,
                    nav_timeout_ms=int(args.nav_timeout),
                )

            if venue_id:
                lang = _lang_from_url(cat)
                await _enrich_items_via_prodinfo(items, lang, venue_id, max_to_probe=240)
            else:
                print("[warn] venueId not found — skipping direct prodinfo enrichment")

            # Fill missing prices from tile text when PW path used
            if not payload and items:
                for it in items:
                    if it.get("id"):
                        iid = str(it["id"]).lower()
                        if it.get("price") in (None, 0) and iid in tile_prices:
                            it["price"] = tile_prices[iid]

            existing_gtins = await _fetch_existing_gtins(store_host_cat)

            rows_raw = []
            for item in items:
                row = _extract_row_from_item(item, cat, store_host_cat)
                if not row:
                    continue
                if row.get("ean_norm") and row["ean_norm"] in existing_gtins:
                    continue
                rows_raw.append(row)

            rows = rows_raw
            if args.max_products and args.max_products > 0:
                rows = rows[: args.max_products]

            print(f"[info] category rows: {len(rows)}" + (" (pw-fallback)" if not payload else ""))
            on_rows(rows)

        except Exception as e:
            print(f"[warn] Wolt category failed {cat}: {e}")

# ---------- main ----------
async def main(args):
    # categories: from --categories-multiline OR --categories-file (file wins if provided)
    categories: List[str] = []
    if args.categories_multiline:
        categories.extend([ln.strip() for ln in args.categories_multiline.splitlines() if ln.strip()])
    if args.categories_file and Path(args.categories_file).exists():
        categories = [ln.strip() for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines() if ln.strip()] or categories
    if not categories:
        print("[error] No category URLs provided. Pass --categories-multiline or --categories-file.")
        sys.exit(2)

    # echo a bit of the runtime configuration
    print(f"[args] headless={args.headless} req_delay={args.req_delay} pdp_workers(not used)={args.pdp_workers} goto={args.goto_strategy} nav_timeout={args.nav_timeout}ms store_host={args.store_host or '(auto)'}")

    out_path = Path(args.out)
    if out_path.is_dir() or str(out_path).endswith("/"):
        out_path = out_path / f"coop_wolt_{now_stamp()}.csv"
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
        sys.exit(130)  # triggers finally blocks

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT,  _sig_handler)

    await run_wolt(args, categories, on_rows)

    gtin_ok = sum(1 for r in all_rows if (r.get("ean_norm") or (r.get("ean_raw") and r.get("ean_raw") != "-")))
    brand_ok = sum(1 for r in all_rows if (r.get("brand") or r.get("manufacturer")))
    print(f"[stats] rows={len(all_rows)}  gtin_present={gtin_ok}  brand_or_manufacturer_present={brand_ok}")
    print(f"[ok] CSV ready: {out_path}")
    await maybe_upsert_db(all_rows)

def parse_args():
    p = argparse.ArgumentParser(description="Coop on Wolt category crawler")
    # Venue and store-host mostly for clarity/consistency with workflows; store-host overrides inference
    p.add_argument("--venue", default="", help="Wolt venue URL (informational).")
    p.add_argument("--store-host", default="", help="Store host label to use in output/DB (e.g., wolt:coop-parnu).")

    # Categories (required via one of these)
    p.add_argument("--categories-multiline", dest="categories_multiline", default="",
                   help="Newline-separated category URLs")
    p.add_argument("--categories-file", dest="categories_file", default="", help="Path to txt file with category URLs")

    # General limits / perf
    p.add_argument("--max-products", type=int, default=0, help="Global cap per category (0=all)")
    p.add_argument("--pdp-workers", type=int, default=4, help="(Reserved) Concurrency hint; not used in Wolt path")
    p.add_argument("--req-delay", type=float, default=0.4, help="Seconds between ops in PW fallback")
    p.add_argument("--headless", default="1", help="1/0 headless for PW fallback")

    # Navigation robustness
    p.add_argument("--goto-strategy", choices=["auto","domcontentloaded","networkidle","load"],
                   default="auto", help="Playwright wait_until strategy for category navigation.")
    p.add_argument("--nav-timeout", default="45000", help="Navigation timeout in milliseconds.")

    # Output & behavior
    p.add_argument("--out", default="out/coop_wolt.csv", help="CSV file or output directory")
    p.add_argument("--force-playwright", action="store_true", help="Force Playwright network fallback.")
    p.add_argument("--write-empty-csv", action="store_true", default=True, help="Always write CSV header even if no rows.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))

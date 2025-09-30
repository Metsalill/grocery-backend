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

GTIN/Tootja enrichment is tolerant:
- Handles JSON-LD, plain text, <dt>/<dd>, headings.
- If still missing, opens product modal and reads “GTIN / Tootja / Tarnija / Supplier”.
  Limited by --modal-probe-limit.

Important:
- ext_id is kept STABLE as iid:<24hex> (Wolt item id) so later runs can update rows.

Guard rails:
- Filters noisy junk.
- Accepts GTIN '-' only when the site explicitly shows '-' for GTIN.
- Per-category watchdog timeout so a stuck page can’t stall the job.

Env:
- WOLT_FORCE_PLAYWRIGHT=1 to force PW fallback
- WOLT_PROBE_LIMIT (default 60) for prodinfo probes
- WOLT_MODAL_PROBE_LIMIT (default 15) for modal clicks per category
- COOP_UPSERT_DB=1 / COOP_DEDUP_DB=1 for DB semantics
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

# ---------- shared utils ----------
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
        if len(d) == 12:  # UPC-A → EAN-13
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
            for k, v in list(row.items()):
                if v is None:
                    row[k] = ""
            if row.get("price") is None or row.get("price") == "":
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
    if retry_after and str(retry_after).isdigit():
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
PRODINFO_JSONLD_GTIN_RE   = re.compile(r'"gtin(?:8|12|13|14)"\s*:\s*"(\d{8,14})"', re.I)
PRODINFO_ANY_GTIN_RE      = re.compile(r"gtin[^0-9]{0,40}(\d{8,14})", re.I)
PRODINFO_SUPPLIER_TXT_RE  = re.compile(r"(?:Tootja|Tarnija|Supplier|Manufacturer)[^A-Za-z0-9]{0,20}([^<>\n]{2,200})", re.I)
TAG_STRIP_RE              = re.compile(r"<[^>]+>")

async def _fetch_prodinfo_fields(lang: str, venue_id: str, item_id: str) -> Dict[str, Optional[str]]:
    url = f"https://prodinfo.wolt.com/{lang}/{venue_id}/{item_id}"
    try:
        html = await _fetch_html(url)
    except Exception:
        return {"gtin": None, "supplier": None, "name": None}

    m = PRODINFO_JSONLD_GTIN_RE.search(html or "")
    gtin = normalize_ean(m.group(1)) if m else None

    if not gtin:
        txt = TAG_STRIP_RE.sub(" ", html or " ")
        m2 = PRODINFO_ANY_GTIN_RE.search(txt)
        if m2:
            gtin = normalize_ean(m2.group(1))
        else:
            if re.search(r"\bGTIN\b", txt, re.I) and not re.search(r"\b\d{8,14}\b", txt):
                if re.search(r"\bGTIN\b[^0-9]{0,40}[-–—]", txt, re.I):
                    gtin = "-"

    supplier = None
    txt = TAG_STRIP_RE.sub(" ", html or " ")
    m3 = PRODINFO_SUPPLIER_TXT_RE.search(txt)
    if m3:
        supplier = m3.group(1).strip()

    name = None
    mt = re.search(r"<h2[^>]*>([^<]{2,200})</h2>", html or "", re.I)
    if mt:
        name = (mt.group(1) or "").strip()

    return {"gtin": gtin, "supplier": supplier, "name": name}

async def _enrich_items_via_prodinfo(items: List[Dict], lang: str, venue_id: str,
                                     max_to_probe: Optional[int] = None) -> None:
    if max_to_probe is None:
        try:
            max_to_probe = int(os.getenv("WOLT_PROBE_LIMIT", "60"))
        except Exception:
            max_to_probe = 60

    def _needs_gtin(it: Dict) -> bool:
        if (it.get("gtin") or "").strip() == "-":
            return False
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
            graw = info.get("gtin")
            gt = normalize_ean(graw)
            if gt:
                it["gtin"] = gt
            elif (graw or "").strip() == "-":
                it["gtin"] = "-"
            if info.get("supplier"):
                it["supplier"] = info["supplier"]
                if not it.get("brand"):
                    it["brand"] = info["supplier"]
            if info.get("name") and not it.get("name"):
                it["name"] = info["name"]
        except Exception:
            pass
        probed += 1
        await asyncio.sleep(0.08)

# ---------- noise guards ----------
_DENY_EXACT = {"web tracking bundle", "functional", "required", "marketing", "analytics",
               "privacy", "cookie", "consent", "pant", "deposit"}
_DENY_SUBSTR = {"cookie", "consent", "tracking", "privacy"}
_DENY_PREFIX = {"otsi", "avasta", "tulemused"}
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
        "ean_raw": ean_raw or "",
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
    """Best-effort server payload loader. Never raise; return None on failure so caller can PW-fallback."""
    try:
        html = await _fetch_html(url)
    except Exception as e:
        print(f"[warn] fast-path: HTML fetch failed for {url} ({e}); will use Playwright.")
        return None
    try:
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
    except Exception as e:
        print(f"[warn] fast-path: parse failed for {url} ({e}); will use Playwright.")
        return None
    return None

# ---------- Playwright ----------
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

async def dismiss_location_prompt(page: Any):
    """Close the address/geo popover and any close(X) buttons on dialogs."""
    try:
        # explicit 'X' in the address banner
        close_btns = [
            'button[aria-label="Close"]',
            'button:has-text("✕")',
            # popup sheet close icons
            'button[class*="close"]',
        ]
        for sel in close_btns:
            b = page.locator(sel)
            if await b.count() > 0:
                try:
                    await b.first.click(timeout=1200)
                    await page.wait_for_timeout(200)
                    break
                except Exception:
                    pass
        # try ESC a couple of times just in case
        for _ in range(2):
            try:
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(120)
            except Exception:
                pass
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
        js = r'''
(() => {
  const out = [];
  // widen the search: menu cards also carry data-test="menu-item"
  const cards = Array.from(document.querySelectorAll('a[href*="item"], article,[data-test*="menu-item"]'));
  const reId = /(?:itemid-|item-)([a-f0-9]{24})/i;
  for (const el of cards) {
    const href = (el.getAttribute && el.getAttribute('href')) || el.href || '';
    let m = href && href.match(reId);
    if (!m) {
      // try attributes and text
      const attrs = ['href','id','data-id','data-item-id','data-product-id','data-testid','data-test'];
      for (const a of attrs) {
        const v = el.getAttribute && el.getAttribute(a);
        if (v) { const mm = v.match(/[a-f0-9]{24}/i); if (mm) { m = [null, mm[0]]; break; } }
      }
      if (!m) {
        const t = (el.textContent||'');
        const mm = t.match(/(?:itemid-|item-)?([a-f0-9]{24})/i);
        if (mm) m = [null, mm[1]];
      }
    }
    if (!m) continue;
    const txt = (el.textContent||'');
    const mt = txt.match(/(\d+[.,]\d{2})\s*€/);
    if (mt) {
      const raw = mt[1].replace(',', '.');
      const val = parseFloat(raw);
      if (!isNaN(val)) out.push([m[1].toLowerCase(), val]);
    }
  }
  return out;
})()
'''
        data = await page.evaluate(js)
        prices: Dict[str, float] = {}
        for iid, val in data or []:
            prices[str(iid).lower()] = float(val)
        return prices
    except Exception:
        return {}

async def _extract_venue_id_via_modal(page) -> Optional[str]:
    try:
        # broaden the click target; menu tiles sometimes are role=button
        for sel in [
            'a[href*="itemid-"]',
            'a[href*="item-"]',
            '[data-test*="menu-item"]',
            'article:has-text("€")',
            '[role="button"]:has-text("€")',
        ]:
            a = page.locator(sel).first
            if await a.count() == 0:
                continue
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
            break
    except Exception:
        pass
    return None

async def _extract_venue_id_any(page, html: str) -> Optional[str]:
    # 1) HTML regex
    m = re.search(r"/menu-images/([a-f0-9]{24})/", html or "", re.I)
    if m:
        return m.group(1)
    # 2) resource timing + images + link/script URLs
    try:
        js = r'''
(() => {
  const re = /\/menu-images\/([a-f0-9]{24})\//i;
  const hit = (s) => { const m = re.exec(s||''); return m ? m[1] : null; };
  const perf = (window.performance && performance.getEntriesByType) ? performance.getEntriesByType('resource') : [];
  for (const e of (perf || [])) { const id = hit(e.name || ''); if (id) return id; }
  for (const img of Array.from(document.images || [])) { const id = hit(img.currentSrc || img.src || ''); if (id) return id; }
  for (const el of Array.from(document.querySelectorAll('link,script'))) { const id = hit(el.src || el.href || ''); if (id) return id; }
  return null;
})()
'''
        found = await page.evaluate(js)
        if isinstance(found, str) and HEX24_RE.fullmatch(found.lower()):
            return found.lower()
    except Exception:
        pass
    # 3) modal probe
    return await _extract_venue_id_via_modal(page)

async def _ensure_category_route(page, cat_url: str):
    """Make sure SPA ended up on the intended category route (prevents 'Lilled' snap)."""
    await page.wait_for_timeout(300)
    def _norm(u: str) -> str:
        return (u or "").split("?")[0].rstrip("/")
    ok = False
    try:
        ok = _norm(page.url) == _norm(cat_url)
    except Exception:
        ok = False
    if not ok:
        # retry navigation with stricter wait, then re-check
        try:
            await page.wait_for_timeout(200)
            await page.goto(cat_url, wait_until="networkidle", timeout=45000)
            await page.wait_for_timeout(300)
        except Exception:
            pass

async def _goto_with_backoff(page, url: str, max_tries: int, nav_timeout_ms: int, strategies: List[str]):
    last_err = None
    for attempt in range(max_tries):
        for ws in strategies:
            try:
                resp = await page.goto(url, wait_until=ws, timeout=nav_timeout_ms)
                return resp
            except Exception as e:
                last_err = e
        await _sleep_backoff(attempt, retry_after=None, base=1.1)
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
        # IMPORTANT: create a Browser context from the *browser*, not BrowserType
        context = await browser.new_context(
            user_agent=_GLOBAL_UA,
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
            geolocation={"latitude": 58.384, "longitude": 24.497},  # Pärnu-ish
        )
        try:
            await context.grant_permissions(["geolocation"])
        except Exception:
            pass
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
            strategies = [goto_strategy] if goto_strategy in ("domcontentloaded","networkidle","load") else ["domcontentloaded"]
            await _goto_with_backoff(page, cat_url, max_tries=3, nav_timeout_ms=nav_timeout_ms, strategies=strategies)

            await wait_cookie_banner(page)
            await dismiss_location_prompt(page)
            await _ensure_category_route(page, cat_url)

            for _ in range(10):
                await page.mouse.wheel(0, 1500)
                await page.wait_for_timeout(int(max(req_delay, 0.4)*1000 + random.uniform(250, 900)))

            tile_prices = await _scrape_tile_prices(page)

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

            if not found:
                ids = set(re.findall(r"(?:itemid-|item-)([a-f0-9]{24})", html or "", re.I))
                for iid in ids:
                    found[iid] = {"id": iid}

            # venue id detection (multi-strategy)
            venue_id = await _extract_venue_id_any(page, html)
            if venue_id:
                print(f"[info] venueId detected: {venue_id}")
            else:
                print("[warn] venueId not found in PW capture")

            return list(found.values()), collected_blobs, html, tile_prices, venue_id

        finally:
            await context.close()
            await browser.close()

# ---------- modal enrichment ----------
async def _enrich_items_via_modal(cat_url: str, items: List[Dict], headless: bool, req_delay: float,
                                  goto_strategy: str, nav_timeout_ms: int, max_modal: int) -> None:
    if async_playwright is None or max_modal <= 0:
        return

    missing = [it for it in items
               if (it.get("gtin") or "").strip() != "-"
               and not normalize_ean(it.get("gtin") or it.get("ean") or it.get("ean_norm"))
               and HEX24_RE.fullmatch(str(it.get("id") or ""))]
    if not missing:
        return
    missing = missing[:max_modal]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=bool(int(headless)))
        context = await browser.new_context(
            user_agent=_GLOBAL_UA,
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
            geolocation={"latitude": 58.384, "longitude": 24.497},
        )
        try:
            await context.grant_permissions(["geolocation"])
        except Exception:
            pass
        page = await context.new_page()
        try:
            strategies = [goto_strategy] if goto_strategy in ("domcontentloaded","networkidle","load") else ["domcontentloaded"]
            await _goto_with_backoff(page, cat_url, max_tries=3, nav_timeout_ms=nav_timeout_ms, strategies=strategies)
            await wait_cookie_banner(page)
            await dismiss_location_prompt(page)
            await _ensure_category_route(page, cat_url)

            for it in missing:
                iid = str(it.get("id"))
                # open product modal
                sel_candidates = [
                    f'a[href*="itemid-{iid}"]',
                    f'a[href*="item-{iid}"]',
                    '[data-test*="menu-item"]',
                    'article:has-text("€")',
                    '[role="button"]:has-text("€")',
                ]
                a = None
                for s in sel_candidates:
                    loc = page.locator(s)
                    if await loc.count() > 0:
                        a = loc.first
                        break
                if not a:
                    continue
                try:
                    await a.scroll_into_view_if_needed(timeout=1500)
                    await asyncio.sleep(max(req_delay, 0.3))
                    await a.click(timeout=2000)
                except Exception:
                    continue

                # click Toote info / Product info
                for btnsel in [
                    'button:has-text("Toote info")',
                    'a:has-text("Toote info")',
                    'button:has-text("Product info")',
                    'a:has-text("Product info")',
                    'button:has-text("Tooteinfo")',
                ]:
                    btn = page.locator(btnsel)
                    try:
                        if await btn.count() > 0:
                            await btn.first.click(timeout=1200)
                            break
                    except Exception:
                        pass

                await page.wait_for_timeout(int(max(req_delay, 0.3) * 1000))

                try:
                    modal_text = await page.inner_text("body", timeout=1500)
                except Exception:
                    modal_text = ""
                mgt = re.search(r"GTIN[^0-9]{0,40}(\d{8,14})", modal_text, re.I)
                if mgt and not it.get("gtin"):
                    it["gtin"] = normalize_ean(mgt.group(1))
                elif re.search(r"\bGTIN\b", modal_text, re.I) and not re.search(r"\b\d{8,14}\b", modal_text):
                    it["gtin"] = "-"
                msup = re.search(r"(?:Tootja|Tarnija|Supplier|Manufacturer)[^A-Za-z0-9]{0,20}([^\n]{2,200})", modal_text, re.I)
                if msup and not it.get("supplier"):
                    it["supplier"] = msup.group(1).strip()
                    if not it.get("brand"):
                        it["brand"] = it["supplier"]

                # close modal (Esc)
                try:
                    await page.keyboard.press("Escape")
                except Exception:
                    pass
                await page.wait_for_timeout(int(max(req_delay, 0.25) * 1000))
        finally:
            await context.close()
            await browser.close()

# ---------- runner ----------
def _lang_from_url(u: str) -> str:
    m = re.search(r"https?://[^/]+/([a-z]{2})(?:/|$)", u, re.I)
    return (m.group(1).lower() if m else "et")

async def run_wolt(args, categories: List[str], on_rows_async) -> None:
    force_pw = bool(args.force_playwright or str(os.getenv("WOLT_FORCE_PLAYWRIGHT", "")).lower() in ("1","true","t","yes","y","on"))

    async def _process_one(cat: str):
        store_host_cat = args.store_host.strip() if args.store_host else _wolt_store_host(cat)
        print(f"[cat-wolt] {cat}")

        payload = None
        if not force_pw:
            try:
                payload = await _load_wolt_payload(cat)
            except Exception as e:
                print(f"[warn] fast-path failed for {cat} ({e}); using Playwright fallback.")
                payload = None

        if payload:
            found: Dict[str, Dict] = {}
            _walk_collect_items(payload, found)
            items = list(found.values())
            blobs, html, tile_prices, venue_id = [payload], "", {}, None
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
            await _enrich_items_via_prodinfo(items, lang, venue_id, max_to_probe=(args.probe_limit or None))
        else:
            print("[warn] venueId not found — skipping direct prodinfo enrichment")

        if not payload and items:
            for it in items:
                if it.get("id"):
                    iid = str(it["id"]).lower()
                    if it.get("price") in (None, 0) and iid in tile_prices:
                        it["price"] = tile_prices[iid]

        if args.modal_probe_limit > 0:
            await _enrich_items_via_modal(
                cat, items,
                headless=bool(int(args.headless)),
                req_delay=float(args.req_delay),
                goto_strategy=args.goto_strategy,
                nav_timeout_ms=int(args.nav_timeout),
                max_modal=int(args.modal_probe_limit),
            )

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
        await on_rows_async(rows)

        if args.upsert_per_category and rows:
            try:
                await maybe_upsert_db(rows)
            except Exception:
                pass

    for idx, cat in enumerate(categories):
        if idx > 0:
            await asyncio.sleep(float(args.req_delay) + random.uniform(0.5, 1.2))
        try:
            # Per-category watchdog to prevent long stalls
            await asyncio.wait_for(_process_one(cat), timeout=float(args.category_timeout))
        except asyncio.TimeoutError:
            print(f"[warn] category watchdog timeout after {args.category_timeout}s → skipping remainder of {cat}")
        except Exception as e:
            print(f"[warn] Wolt category failed {cat}: {e}")

# ---------- main ----------
async def main(args):
    categories: List[str] = []
    if args.categories_multiline:
        categories.extend([ln.strip() for ln in args.categories_multiline.splitlines() if ln.strip()])
    if args.categories_file and Path(args.categories_file).exists():
        categories = [ln.strip() for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines() if ln.strip()] or categories
    if not categories:
        print("[error] No category URLs provided. Pass --categories-multiline or --categories-file.")
        sys.exit(2)

    print(f"[args] headless={args.headless} req_delay={args.req_delay} pdp_workers(not used)={args.pdp_workers} "
          f"goto={args.goto_strategy} nav_timeout={args.nav_timeout}ms store_host={args.store_host or '(auto)'} "
          f"probe_limit={args.probe_limit or os.getenv('WOLT_PROBE_LIMIT','(env default 60)')} "
          f"modal_probe_limit={args.modal_probe_limit or os.getenv('WOLT_MODAL_PROBE_LIMIT','(env default 15)')} "
          f"category_timeout={args.category_timeout}s "
          f"upsert_per_category={args.upsert_per_category} flush_every={args.flush_every}")

    out_path = Path(args.out)
    if out_path.is_dir() or str(out_path).endswith("/"):
        out_path = out_path / f"coop_wolt_{now_stamp()}.csv"
    print(f"[out] streaming CSV → {out_path}")
    if args.write_empty_csv:
        _ensure_csv_with_header(out_path)

    all_rows: List[Dict] = []
    accum_rows: List[Dict] = []

    async def flush_now():
        nonlocal accum_rows
        if accum_rows:
            try:
                await maybe_upsert_db(accum_rows)
            finally:
                accum_rows = []

    async def on_rows_async(batch: List[Dict]):
        nonlocal all_rows, accum_rows
        if not batch:
            return
        append_csv(batch, out_path)
        all_rows.extend(batch)
        accum_rows.extend(batch)
        print(f"[stream] +{len(batch)} rows (total {len(all_rows)})")
        if args.flush_every and len(accum_rows) >= args.flush_every:
            print(f"[info] flush-every threshold reached ({len(accum_rows)} rows) → upserting now…")
            await flush_now()

    def _sig_handler(signum, frame):
        print(f"[warn] received signal {signum}; exiting 130 gracefully.")
        sys.exit(130)

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT,  _sig_handler)

    await run_wolt(args, categories, on_rows_async)

    await flush_now()

    gtin_ok = sum(1 for r in all_rows if (r.get("ean_norm") or (r.get("ean_raw") and r.get("ean_raw") != "-")))
    brand_ok = sum(1 for r in all_rows if (r.get("brand") or r.get("manufacturer")))
    print(f"[stats] rows={len(all_rows)}  gtin_present={gtin_ok}  brand_or_manufacturer_present={brand_ok}")
    print(f"[ok] CSV ready: {out_path}")

    await maybe_upsert_db(all_rows)

def parse_args():
    p = argparse.ArgumentParser(description="Coop on Wolt category crawler")
    p.add_argument("--venue", default="", help="Wolt venue URL (informational).")
    p.add_argument("--store-host", default="", help="Store host label to use in output/DB (e.g., wolt:coop-parnu).")
    p.add_argument("--categories-multiline", dest="categories_multiline", default="",
                   help="Newline-separated category URLs")
    p.add_argument("--categories-file", dest="categories_file", default="", help="Path to txt file with category URLs")
    p.add_argument("--max-products", type=int, default=0, help="Global cap per category (0=all)")
    p.add_argument("--pdp-workers", type=int, default=4, help="(Reserved) Concurrency hint; not used in Wolt path")
    p.add_argument("--req-delay", type=float, default=0.6, help="Seconds between ops in PW/modal paths")
    p.add_argument("--headless", default="1", help="1/0 headless for PW")
    p.add_argument("--goto-strategy", choices=["auto","domcontentloaded","networkidle","load"],
                   default="domcontentloaded", help="Playwright wait_until strategy.")
    p.add_argument("--nav-timeout", default="45000", help="Navigation timeout in milliseconds.")
    p.add_argument("--out", default="out/coop_wolt.csv", help="CSV file or output directory")
    p.add_argument("--force-playwright", action="store_true", help="Force Playwright network fallback.")
    p.add_argument("--write-empty-csv", action="store_true", default=True, help="Always write CSV header even if no rows.")
    # incremental upserts
    p.add_argument("--upsert-per-category", action="store_true", help="Upsert to DB after each category finishes.")
    p.add_argument("--flush-every", type=int, default=0, help="If >0, also upsert every N streamed rows.")
    # probe limits
    p.add_argument("--probe-limit", type=int, default=0, help="Override max prodinfo probes per run (env default 60).")
    p.add_argument("--modal-probe-limit", type=int, default=0, help="Override max modal clicks per category (env default 15).")
    # per-category watchdog timeout
    p.add_argument("--category-timeout", type=float, default=75.0, help="Max seconds to spend on a single category before skipping.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if not args.modal_probe_limit:
        try:
            args.modal_probe_limit = int(os.getenv("WOLT_MODAL_PROBE_LIMIT", "15"))
        except Exception:
            args.modal_probe_limit = 15
    asyncio.run(main(args))

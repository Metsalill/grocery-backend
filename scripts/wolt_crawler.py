#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt store crawler (JSON endpoints; Playwright + __NEXT_DATA__ fallback)

Priority:
  1) requests -> /items/<slug>?language=...
  2) Playwright context.request.get(...)
  3) Playwright page scrape of __NEXT_DATA__ from the items page

Inputs:
  --store-url | --store-host
  --categories-file (slugs or full URLs; if it includes URLs, its base venue URL overrides host)
  --city parnu|tallinn (inferred if omitted)
  --out (exact) | --out-dir

Still supports all the compatibility flags used by CI.
"""

import re
import csv
import time
import json
import argparse
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple, Optional, Union
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selectolax.parser import HTMLParser

# Optional Playwright (only used when needed)
try:
    from playwright.sync_api import sync_playwright
    PW_AVAILABLE = True
except Exception:
    PW_AVAILABLE = False

WOLT_HOST = "https://wolt.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

CATEGORY_HREF_RE = re.compile(r"/venue/[^/]+/items/([^/?#]+)")

GEO = {
    "parnu":   {"latitude": 58.3859, "longitude": 24.4971, "accuracy": 1500},
    "tallinn": {"latitude": 59.4370, "longitude": 24.7536, "accuracy": 1500},
}

# --------------------- shared helpers ---------------------

def _base_headers() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

def _normalize_city(c: Optional[str]) -> str:
    c = (c or "").lower()
    if c.startswith("pär"):
        return "parnu"
    if c in ("lasname", "lasnamae", "lasnamäe", "lasna"):
        return "tallinn"
    if c not in ("parnu", "tallinn"):
        return "parnu"
    return c

def _city_headers(city: str) -> Dict[str, str]:
    city = _normalize_city(city)
    return {"x-city-id": city, "Cookie": f"wolt-session-city={city}"}

def make_session(city: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, read=5, connect=5, backoff_factor=0.4,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=frozenset(["GET", "HEAD"]), raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(_base_headers())
    s.headers.update(_city_headers(city))
    return s

def normalize_store_url(store_url: str) -> str:
    store_url = store_url.strip()
    if not store_url.startswith("http"):
        store_url = urljoin(WOLT_HOST, store_url)
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    if "venue" in segs:
        idx = segs.index("venue")
        segs = segs[: idx + 2]
    clean_path = "/" + "/".join(segs)
    return f"{parts.scheme}://{parts.netloc}{clean_path}"

def infer_city_from_string(s: str) -> str:
    s = s.lower()
    if "parnu" in s or "pärnu" in s:
        return "parnu"
    if any(x in s for x in ("tallinn", "lasna", "lasname", "lasnamae", "lasnamäe")):
        return "tallinn"
    return "parnu"

def build_url_from_host(store_host: str, city_hint: Optional[str]) -> str:
    slug = store_host.split(":", 1)[-1]
    city = _normalize_city(city_hint or infer_city_from_string(slug))
    return f"{WOLT_HOST}/et/est/{city}/venue/{slug}"

def infer_store_host_from_url(store_url: str) -> str:
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    store_slug = segs[-1] if segs else "unknown-store"
    return f"wolt:{store_slug}"

def discover_category_slugs(session: requests.Session, store_url: str) -> List[str]:
    r = session.get(store_url, timeout=30)
    r.raise_for_status()
    slugs: set[str] = set()
    tree = HTMLParser(r.text)
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        m = CATEGORY_HREF_RE.search(href)
        if m:
            slugs.add(m.group(1))
    if not slugs:
        for m in re.findall(r"/venue/[^/]+/items/([a-z0-9\-]+)", r.text):
            slugs.add(m)
    return sorted(slugs)

def parse_categories_file(path: Path) -> Tuple[List[str], Optional[str]]:
    slugs: List[str] = []; base_url: Optional[str] = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("http"):
            m = CATEGORY_HREF_RE.search(line)
            if not m:
                continue
            slugs.append(m.group(1))
            if base_url is None:
                u = urlparse(line)
                parts = u.path.split("/items/")[0]
                base_url = f"{u.scheme}://{u.netloc}{parts}".rstrip("/")
        else:
            slugs.append(line)
    seen, ordered = set(), []
    for s in slugs:
        if s not in seen:
            seen.add(s); ordered.append(s)
    return ordered, base_url

# --------------------- fetch JSON (requests/Playwright) ---------------------

def requests_fetch_category_json(session, base_store_url, slug, language) -> Optional[Dict[str, Any]]:
    url = urljoin(base_store_url + "/", f"items/{slug}")
    params = {"language": language}
    r = session.get(url, params=params, timeout=30)
    if r.status_code in (403, 404):
        return None
    try:
        r.raise_for_status()
    except Exception:
        return None
    t = r.text.strip()
    if t.startswith("{"):
        try:
            return r.json()
        except Exception:
            return None
    time.sleep(0.5)
    r2 = session.get(url, params=params, timeout=30)
    if r2.ok and r2.text.strip().startswith("{"):
        try:
            return r2.json()
        except Exception:
            return None
    return None

def _playwright_context(city: str):
    city = _normalize_city(city)
    geo = GEO.get(city, GEO["parnu"])
    return dict(
        locale="et-EE",
        geolocation=geo,
        permissions=["geolocation"],
        user_agent=UA,
        extra_http_headers={**_base_headers(), **_city_headers(city)},
    )

def playwright_fetch_category_json_via_request(store_url, slug, language, city) -> Optional[Dict[str, Any]]:
    if not PW_AVAILABLE:
        return None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(**_playwright_context(city))
        try:
            page = context.new_page()
            page.goto(store_url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(500)
            base = normalize_store_url(store_url)
            url = urljoin(base + "/", f"items/{slug}")
            resp = context.request.get(url, params={"language": language}, timeout=30000)
            if resp.ok:
                try:
                    return resp.json()
                except Exception:
                    pass
            # Tallinn fallback
            if _normalize_city(city) != "tallinn":
                resp2 = context.request.get(
                    url, params={"language": language},
                    headers={**_base_headers(), **_city_headers("tallinn")}, timeout=30000
                )
                if resp2.ok:
                    try:
                        return resp2.json()
                    except Exception:
                        pass
            return None
        finally:
            context.close(); browser.close()

# --------------------- Playwright __NEXT_DATA__ fallback ---------------------

def _recursive_find_items(node: Any) -> List[Dict[str, Any]]:
    """
    Walk arbitrary JSON looking for product-like 'items' arrays where
    elements look like dicts with 'id' and 'name' or 'price'.
    """
    found: List[Dict[str, Any]] = []

    def looks_like_item(x: Any) -> bool:
        return isinstance(x, dict) and ("name" in x or "price" in x or "id" in x)

    def walk(obj: Any):
        nonlocal found
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "items" and isinstance(v, list) and v and all(isinstance(e, dict) for e in v):
                    # Heuristic: at least some elements look like product entries
                    if any(looks_like_item(e) for e in v):
                        # extend but keep as dicts
                        found.extend(e for e in v if isinstance(e, dict))
                walk(v)
        elif isinstance(obj, list):
            for e in obj:
                walk(e)

    walk(node)
    return found

def playwright_fetch_category_json_via_page(store_url, slug, language, city) -> Optional[Dict[str, Any]]:
    """
    Navigate to the items page, read __NEXT_DATA__, extract product 'items'.
    Returns a synthetic payload: {"category":{...}, "items":[...]} or None.
    """
    if not PW_AVAILABLE:
        return None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(**_playwright_context(city))
        page = context.new_page()
        try:
            base = normalize_store_url(store_url)
            url = urljoin(base + "/", f"items/{slug}?language={language}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            # Wait briefly for client-side hydration
            page.wait_for_timeout(800)

            # Pull Next.js data
            el = page.locator("script#__NEXT_DATA__")
            if not el.count():
                return None
            raw = el.first.inner_text()
            data = json.loads(raw)

            # Heuristically find items anywhere in the object graph
            items = _recursive_find_items(data)
            if not items:
                return None

            # Try to find a category name (left nav often present in Next data)
            category_name = ""
            # simple heuristic search for slug->name mapping
            def find_category_name(obj: Any) -> Optional[str]:
                if isinstance(obj, dict):
                    # look for objects that contain both 'slug' and 'name'
                    if obj.get("slug") == slug and isinstance(obj.get("name"), str):
                        return obj["name"]
                    for v in obj.values():
                        r = find_category_name(v)
                        if r: return r
                elif isinstance(obj, list):
                    for v in obj:
                        r = find_category_name(v)
                        if r: return r
                return None
            category_name = find_category_name(data) or ""

            # Normalize fields to what the CSV pipeline expects
            norm_items: List[Dict[str, Any]] = []
            for it in items:
                norm_items.append({
                    "id": it.get("id") or it.get("_id") or it.get("item_id") or "",
                    "name": it.get("name") or it.get("title") or "",
                    "price": it.get("price") if isinstance(it.get("price"), (int, float)) else it.get("unit_price") or None,
                    "unit_info": it.get("unit_info") or "",
                    "unit_price": {
                        "price": (it.get("unit_price", {}) or {}).get("price") if isinstance(it.get("unit_price"), dict) else None,
                        "unit":  (it.get("unit_price", {}) or {}).get("unit")  if isinstance(it.get("unit_price"), dict) else "",
                    },
                    "barcode_gtin": it.get("barcode_gtin") or "",
                    "description": it.get("description") or it.get("desc") or "",
                    "checksum": it.get("checksum") or "",
                    "vat_category_code": it.get("vat_category_code") or "",
                    "vat_percentage": it.get("vat_percentage") if isinstance(it.get("vat_percentage"), (int, float)) else None,
                    "images": [{"url": (it.get("image") or it.get("img") or it.get("image_url") or "")}],
                    # venue_id generally missing in Next payload; leave blank
                })

            payload = {
                "category": {"name": category_name or slug, "id": slug},
                "items": norm_items,
            }
            return payload
        except Exception:
            return None
        finally:
            context.close(); browser.close()

def fetch_category_json(session, store_url, slug, language, city) -> Dict[str, Any]:
    base_store_url = normalize_store_url(store_url)

    # 1) requests direct JSON
    data = requests_fetch_category_json(session, base_store_url, slug, language)
    if data is not None:
        return data

    # 2) playwright network request
    data = playwright_fetch_category_json_via_request(base_store_url, slug, language, city)
    if data is not None:
        return data

    # 3) playwright page scrape of __NEXT_DATA__
    data = playwright_fetch_category_json_via_page(base_store_url, slug, language, city)
    if data is not None:
        return data

    raise ValueError(f"Failed to fetch JSON for slug={slug} (city={city}) via requests/Playwright/NextData")

# --------------------- rows & CSV ---------------------

def extract_rows(payload: Dict[str, Any], store_host: str, category_slug: str) -> Tuple[List[Dict[str, Any]], str]:
    rows: List[Dict[str, Any]] = []
    venue_id: str = ""

    category = payload.get("category", {}) or {}
    items = payload.get("items", []) or []

    for it in items:
        if not venue_id:
            venue_id = it.get("venue_id", "") or ""
        row = {
            "store_host": store_host,
            "venue_id": venue_id,
            "category_slug": category_slug,
            "category_name": category.get("name", ""),
            "category_id": category.get("id", ""),
            "item_id": it.get("id", ""),
            "name": it.get("name", ""),
            "price": it.get("price", None),
            "unit_info": it.get("unit_info", ""),
            "unit_price_value": (it.get("unit_price", {}) or {}).get("price", None) if isinstance(it.get("unit_price"), dict) else None,
            "unit_price_unit": (it.get("unit_price", {}) or {}).get("unit", "") if isinstance(it.get("unit_price"), dict) else "",
            "barcode_gtin": it.get("barcode_gtin", ""),
            "description": it.get("description", ""),
            "checksum": it.get("checksum", ""),
            "vat_category_code": it.get("vat_category_code", ""),
            "vat_percentage": it.get("vat_percentage", None),
            "image_url": ((it.get("images") or [{}])[0] or {}).get("url", "") if isinstance(it.get("images"), list) else (it.get("image_url") or ""),
        }
        rows.append(row)

    return rows, venue_id

def maybe_fill_venue(session, store_url, language, city) -> str:
    slugs = discover_category_slugs(session, store_url)
    if not slugs:
        return ""
    data = fetch_category_json(session, store_url, slugs[0], language, city)
    if isinstance(data.get("venue_id"), str) and data["venue_id"]:
        return data["venue_id"]
    for it in (data.get("items") or []):
        if isinstance(it, dict) and it.get("venue_id"):
            return it["venue_id"]
    return ""

def write_csv(rows: Iterable[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    fieldnames = [
        "store_host","venue_id","category_slug","category_name","category_id",
        "item_id","name","price","unit_info","unit_price_value","unit_price_unit",
        "barcode_gtin","description","checksum","vat_category_code","vat_percentage","image_url"
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            for k in fieldnames:
                r.setdefault(k, "")
            w.writerow(r)

# --------------------- main ---------------------

def main():
    ap = argparse.ArgumentParser(description="Wolt JSON crawler with Playwright & NextData fallback")
    ap.add_argument("--store-url")
    ap.add_argument("--store-host")
    ap.add_argument("--city")
    ap.add_argument("--out")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--language", default="et")
    ap.add_argument("--categories-file")
    # compatibility (ignored)
    ap.add_argument("--max-products"); ap.add_argument("--headless")
    ap.add_argument("--req-delay"); ap.add_argument("--goto-strategy")
    ap.add_argument("--nav-timeout"); ap.add_argument("--category-timeout")
    ap.add_argument("--upsert-per-category", action="store_true")
    ap.add_argument("--flush-every"); ap.add_argument("--probe-limit")
    ap.add_argument("--modal-probe-limit")
    args = ap.parse_args()

    # Load categories file (slugs + potential base URL)
    file_slugs: List[str] = []; file_base_url: Optional[str] = None
    if args.categories_file:
        p = Path(args.categories_file)
        if not p.exists():
            raise SystemExit(f"[error] categories file not found: {p}")
        file_slugs, file_base_url = parse_categories_file(p)

    # Venue precedence: store-url > categories-file base_url > store-host
    if args.store_url:
        store_url = normalize_store_url(args.store_url)
        store_host = infer_store_host_from_url(store_url)
        city = _normalize_city(args.city or infer_city_from_string(store_url))
        if file_base_url and normalize_store_url(file_base_url) != store_url:
            print(f"::warning:: categories file base URL ({file_base_url}) differs from --store-url ({store_url}); using --store-url.")
    elif file_base_url:
        store_url = normalize_store_url(file_base_url)
        store_host = infer_store_host_from_url(store_url)
        city = _normalize_city(args.city or infer_city_from_string(store_url))
        if args.store_host and args.store_host != store_host:
            print(f"::notice:: Overriding --store-host ({args.store_host}) with host derived from categories file ({store_host}) to avoid mismatch.")
    elif args.store_host:
        store_url = build_url_from_host(args.store_host, args.city)
        store_host = args.store_host
        city = _normalize_city(args.city or infer_city_from_string(args.store_host))
    else:
        ap.error("Provide --store-url or --store-host (or include full URLs in --categories-file).")
        return

    session = make_session(city)

    print(f"[info] Store URL:   {store_url}")
    print(f"[info] Store host:  {store_host}")
    print(f"[info] Language:    {args.language}")
    print(f"[info] City tag:    {city}")

    # Category slugs
    if file_slugs:
        slugs = file_slugs
        print(f"[info] Using {len(slugs)} category slugs from file: {args.categories_file}")
    else:
        slugs = discover_category_slugs(session, store_url)
        if not slugs:
            raise SystemExit("[error] Could not find any category slugs on the venue page.")
        print(f"[info] Found {len(slugs)} category slugs via HTML discovery")

    all_rows: List[Dict[str, Any]] = []
    global_venue_id = ""

    for i, slug in enumerate(slugs, 1):
        try:
            data = fetch_category_json(session, store_url, slug, args.language, city)
        except Exception as e:
            print(f"[warn] Failed category '{slug}': {e}")
            continue

        rows, venue_id = extract_rows(data, store_host, slug)
        if venue_id and not global_venue_id:
            global_venue_id = venue_id

        all_rows.extend(rows)
        print(f"[ok]  {i:>2}/{len(slugs)}  '{slug}' → {len(rows)} items")
        time.sleep(0.2)

    if not global_venue_id:
        try:
            global_venue_id = maybe_fill_venue(session, store_url, args.language, city) or "unknown"
        except Exception:
            global_venue_id = "unknown"

    out_path = Path(args.out) if args.out else Path(args.out_dir) / f"coop_wolt_{global_venue_id}_{city}.csv"
    write_csv(all_rows, out_path)
    print(f"[done] Wrote {len(all_rows)} rows → {out_path}")

if __name__ == "__main__":
    main()

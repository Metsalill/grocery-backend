#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt store crawler (consumer API with browser-like headers; Playwright/Next.js fallbacks)
+ CSV export
+ Optional UPSERT to staging_coop_products (same contract as bolt_crawler)

Primary endpoint per category (we try both forms, because the site uses both):
  1) https://consumer-api.wolt.com/consumer-assortment/v1/venues/slug/<venue>/assortment/categories/slug/<slug>?language=<lang>
  2) https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1/venues/slug/<venue>/assortment/categories/slug/<slug>?language=<lang>

If those fail, we fall back to Playwright network using the same headers, then to scraping
__NEXT_DATA__ from the items page.
"""

import re
import os
import csv
import time
import json
import uuid
import hashlib
import argparse
import datetime as dt
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selectolax.parser import HTMLParser

# Optional DB (same as bolt_crawler)
try:
    import psycopg
except Exception:
    psycopg = None

# Playwright is optional (only used for fallbacks)
try:
    from playwright.sync_api import sync_playwright
    PW_AVAILABLE = True
except Exception:
    PW_AVAILABLE = False

WOLT_HOST = "https://wolt.com"
CONSUMER_API_BASES = [
    # order matters; try the short one first, then the prefixed one we saw in cURL
    "https://consumer-api.wolt.com/consumer-assortment/v1",
    "https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)

CATEGORY_HREF_RE = re.compile(r"/venue/[^/]+/items/([^/?#]+)")

GEO = {
    "parnu":   {"latitude": 58.3859, "longitude": 24.4971, "accuracy": 1500},
    "tallinn": {"latitude": 59.4370, "longitude": 24.7536, "accuracy": 1500},
}

CHAIN = "Coop"
CHANNEL = "wolt"

# Known Tallinn venue slugs to improve city inference when --city is omitted
KNOWN_TALLINN_VENUES = {
    "coop-lasname",
    "coop-mustakivi",
    "coop-akadeemia",
    "coop-miiduranna",
    "coop-laagri",
    "konsum-juri",
    "konsum-saku",
}

# --------------------- helpers ---------------------

def _normalize_city(c: Optional[str]) -> str:
    c = (c or "").lower()
    if c.startswith("pär"):
        return "parnu"
    if c in ("lasname", "lasnamae", "lasnamäe", "lasna"):
        return "tallinn"
    if c not in ("parnu", "tallinn"):
        return "parnu"
    return c

def _browserish_headers(language: str, city: str, client_id: str, session_id: str) -> Dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": f"{language}-EE,{language};q=0.9,en;q=0.8",
        "app-language": language,
        "client-version": "1.16.39",
        "clientversionnumber": "1.16.39",
        "platform": "Web",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "origin": WOLT_HOST,
        "referer": f"{WOLT_HOST}/",
        "user-agent": UA,
        "x-city-id": _normalize_city(city),
        "x-wolt-web-clientid": client_id,
        "x-wolt-web-client-id": client_id,
        "w-wolt-session-id": session_id,
    }

def _cookie_string(city: str, client_id: str, analytics_id: str) -> str:
    consents = (
        '{"analytics":true,"functional":true,'
        '"interaction":{"bundle":"allow"},"marketing":true}'
    )
    return "; ".join([
        f"wolt-session-city={_normalize_city(city)}",
        f"__woltUid={client_id}",
        f"__woltAnalyticsId={analytics_id}",
        f"cwc-consents={consents}",
    ])

def _base_requests_session(headers: Dict[str, str], cookies: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, read=5, connect=5, backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(headers)
    s.headers.update({
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
    })
    s.headers.update({"Cookie": cookies})
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
    """Heuristics to pick city from a slug or URL path."""
    s = (s or "").lower()
    # Pärnu: handles "parnu" and "prnu" (slug form)
    if "parnu" in s or "pärnu" in s or "prnu" in s:
        return "parnu"
    # Tallinn: explicit mentions OR known venue slugs
    if ("tallinn" in s or "lasna" in s or "lasname" in s or "lasnamae" in s or "lasnamäe" in s
        or any(v in s for v in KNOWN_TALLINN_VENUES)):
        return "tallinn"
    # Default to Pärnu if unknown
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

def venue_slug_from_url(store_url: str) -> str:
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    return segs[-1] if segs else ""

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

# --------------------- consumer API fetch ---------------------

def consumer_api_fetch_category_json(
    session: requests.Session, venue_slug: str, category_slug: str, language: str
) -> Optional[Dict[str, Any]]:
    for base in CONSUMER_API_BASES:
        url = (
            f"{base}/venues/slug/{venue_slug}/assortment/"
            f"categories/slug/{category_slug}"
        )
        r = session.get(url, params={"language": language}, timeout=30)
        if r.status_code in (403, 404):
            continue
        try:
            r.raise_for_status()
        except Exception:
            continue
        try:
            return r.json()
        except Exception:
            continue
    return None

# --------------------- Playwright fallbacks ---------------------

def _playwright_context(headers: Dict[str, str]):
    return dict(
        locale="et-EE",
        geolocation={"latitude": 58.3859, "longitude": 24.4971, "accuracy": 1500},
        permissions=["geolocation"],
        user_agent=headers.get("user-agent", UA),
        extra_http_headers=headers,
    )

def playwright_fetch_consumer_api(
    store_url: str, venue_slug: str, category_slug: str, language: str, headers: Dict[str, str], cookies: str
) -> Optional[Dict[str, Any]]:
    if not PW_AVAILABLE:
        return None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(**_playwright_context(headers))
        page = context.new_page()
        try:
            page.goto(store_url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(300)
            for base in CONSUMER_API_BASES:
                url = (
                    f"{base}/venues/slug/{venue_slug}/assortment/"
                    f"categories/slug/{category_slug}"
                )
                resp = context.request.get(url, params={"language": language}, timeout=30000)
                if resp.ok:
                    try:
                        return resp.json()
                    except Exception:
                        pass
            return None
        finally:
            context.close(); browser.close()

def _recursive_find_items(node: Any) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    def looks_like_item(x: Any) -> bool:
        return isinstance(x, dict) and ("name" in x or "price" in x or "id" in x)
    def walk(obj: Any):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "items" and isinstance(v, list) and v and all(isinstance(e, dict) for e in v):
                    if any(looks_like_item(e) for e in v):
                        found.extend(e for e in v if isinstance(e, dict))
                walk(v)
        elif isinstance(obj, list):
            for e in obj:
                walk(e)
    walk(node)
    return found

def _cents_to_eur(x):
    return (float(x) / 100.0) if isinstance(x, (int, float)) else None

def playwright_nextdata_items(
    store_url: str, category_slug: str, language: str, headers: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    if not PW_AVAILABLE:
        return None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(**_playwright_context(headers))
        page = context.new_page()
        try:
            base = normalize_store_url(store_url)
            url = urljoin(base + "/", f"items/{category_slug}?language={language}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(800)
            el = page.locator("script#__NEXT_DATA__")
            if not el.count():
                return None
            data = json.loads(el.first.inner_text())
            items = _recursive_find_items(data)
            if not items:
                return None

            def find_cat(obj: Any) -> Optional[str]:
                if isinstance(obj, dict):
                    if obj.get("slug") == category_slug and isinstance(obj.get("name"), str):
                        return obj["name"]
                    for v in obj.values():
                        r = find_cat(v)
                        if r: return r
                elif isinstance(obj, list):
                    for v in obj:
                        r = find_cat(v)
                        if r: return r
                return None
            cat_name = find_cat(data) or category_slug

            norm_items: List[Dict[str, Any]] = []
            for it in items:
                up = it.get("unit_price", {}) or {}
                norm_items.append({
                    "id": it.get("id") or it.get("_id") or "",
                    "name": it.get("name") or it.get("title") or "",
                    # SCALE: cents -> euros
                    "price": _cents_to_eur(it.get("price")),
                    "unit_info": it.get("unit_info") or "",
                    "unit_price": {
                        "price": _cents_to_eur(up.get("price")) if isinstance(up, dict) else None,
                        "unit":  up.get("unit") if isinstance(up, dict) else "",
                    },
                    "barcode_gtin": it.get("barcode_gtin") or "",
                    "description": it.get("description") or "",
                    "checksum": it.get("checksum") or "",
                    "vat_category_code": it.get("vat_category_code") or "",
                    "vat_percentage": it.get("vat_percentage") if isinstance(it.get("vat_percentage"), (int, float)) else None,
                    "images": [{"url": (it.get("image") or it.get("image_url") or "")}],
                })
            return {"category": {"name": cat_name, "id": category_slug}, "items": norm_items, "source_url": url}
        except Exception:
            return None
        finally:
            context.close(); browser.close()

# --------------------- orchestration ---------------------

def fetch_category_json(
    session: requests.Session, store_url: str, category_slug: str, language: str, headers: Dict[str, str], cookies: str
) -> Dict[str, Any]:
    venue_slug = venue_slug_from_url(store_url)

    data = consumer_api_fetch_category_json(session, venue_slug, category_slug, language)
    if data is not None:
        data["source_url"] = f"{CONSUMER_API_BASES[0]}/venues/slug/{venue_slug}/assortment/categories/slug/{category_slug}?language={language}"
        return data

    data = playwright_fetch_consumer_api(store_url, venue_slug, category_slug, language, headers, cookies)
    if data is not None:
        data["source_url"] = f"{CONSUMER_API_BASES[0]}/venues/slug/{venue_slug}/assortment/categories/slug/{category_slug}?language={language}"
        return data

    data = playwright_nextdata_items(store_url, category_slug, language, headers)
    if data is not None:
        return data

    return {"category": {"id": category_slug, "name": category_slug}, "items": [], "source_url": ""}

# --------------------- rows & CSV ---------------------

def extract_rows(payload: Dict[str, Any], store_host: str, category_slug: str) -> Tuple[List[Dict[str, Any]], str, str]:
    rows: List[Dict[str, Any]] = []
    venue_id: str = ""
    category = payload.get("category", {}) or {}
    items = payload.get("items", []) or []
    category_name = category.get("name", "") or category_slug

    for it in items:
        if not venue_id:
            venue_id = it.get("venue_id", "") or ""
        # ext_id from Wolt id where possible; otherwise stable hash
        raw_id = str(it.get("id") or "").strip()
        if raw_id:
            ext_id = f"wolt:{raw_id}"
        else:
            ext_id = "wolt:" + hashlib.md5(f"{store_host}|{it.get('name','')}".encode("utf-8")).hexdigest()[:16]

        # SCALE: Wolt API returns price and unit_price.price in cents (integers)
        up = it.get("unit_price", {}) or {}
        price_eur = (
            it.get("price") if isinstance(it.get("price"), float) and it.get("price") < 1000
            else _cents_to_eur(it.get("price"))
        )
        unit_price_value_eur = (
            up.get("price") if isinstance(up.get("price"), float) and up.get("price") < 1000
            else _cents_to_eur(up.get("price")) if isinstance(up, dict) else None
        )

        row = {
            "store_host": store_host,
            "venue_id": venue_id,
            "category_slug": category_slug,
            "category_name": category_name,
            "category_id": category.get("id", ""),
            "item_id": it.get("id", ""),
            "name": it.get("name", ""),
            "price": price_eur,
            "unit_info": it.get("unit_info", ""),
            "unit_price_value": unit_price_value_eur,
            "unit_price_unit": up.get("unit", "") if isinstance(up, dict) else "",
            "barcode_gtin": it.get("barcode_gtin", ""),
            "description": it.get("description", ""),
            "checksum": it.get("checksum", ""),
            "vat_category_code": it.get("vat_category_code", ""),
            "vat_percentage": it.get("vat_percentage", None),
            "image_url": ((it.get("images") or [{}])[0] or {}).get("url", "") if isinstance(it.get("images"), list) else (it.get("image_url") or ""),
            "_ext_id": ext_id,
        }
        rows.append(row)

    return rows, venue_id, payload.get("source_url") or ""

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
            clean = {k: r.get(k, "") for k in fieldnames}
            w.writerow(clean)

# --------------------- DB upsert (new) ---------------------

def ensure_staging_schema(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS staging_coop_products(
      chain           text,
      channel         text,
      store_name      text,
      store_host      text,
      city_path       text,
      category_name   text,
      ext_id          text,
      name            text,
      brand           text,
      manufacturer    text,
      size_text       text,
      price           numeric(12,2),
      currency        text,
      image_url       text,
      url             text,
      description     text,
      ean_raw         text,
      scraped_at      timestamptz DEFAULT now()
    );
    """
    idx = """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'i'
          AND c.relname = 'ux_staging_coop_storehost_extid'
      )
      THEN
        CREATE UNIQUE INDEX ux_staging_coop_storehost_extid
          ON staging_coop_products (store_host, ext_id);
      END IF;
    END $$;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(idx)

def upsert_rows_to_staging_coop(rows: List[Dict], db_url: str):
    if not psycopg or not db_url or not rows:
        return
    with psycopg.connect(db_url) as conn:
        ensure_staging_schema(conn)
        ins = """
        INSERT INTO staging_coop_products(
          chain,channel,store_name,store_host,city_path,category_name,
          ext_id,name,brand,manufacturer,size_text,price,currency,image_url,url,
          description,ean_raw,scraped_at
        )
        VALUES (
          %(chain)s,%(channel)s,%(store_name)s,%(store_host)s,%(city_path)s,%(category_name)s,
          %(ext_id)s,%(name)s,%(brand)s,%(manufacturer)s,%(size_text)s,%(price)s,%(currency)s,%(image_url)s,%(url)s,
          %(description)s,%(ean_raw)s,%(scraped_at)s
        )
        ON CONFLICT (store_host, ext_id) DO UPDATE SET
          chain = EXCLUDED.chain,
          channel = EXCLUDED.channel,
          store_name = EXCLUDED.store_name,
          city_path = EXCLUDED.city_path,
          category_name = EXCLUDED.category_name,
          name = EXCLUDED.name,
          brand = COALESCE(EXCLUDED.brand, staging_coop_products.brand),
          manufacturer = COALESCE(EXCLUDED.manufacturer, staging_coop_products.manufacturer),
          size_text = COALESCE(EXCLUDED.size_text, staging_coop_products.size_text),
          price = EXCLUDED.price,
          currency = EXCLUDED.currency,
          image_url = COALESCE(EXCLUDED.image_url, staging_coop_products.image_url),
          url = EXCLUDED.url,
          description = COALESCE(EXCLUDED.description, staging_coop_products.description),
          ean_raw = COALESCE(EXCLUDED.ean_raw, staging_coop_products.ean_raw),
          scraped_at = EXCLUDED.scraped_at
        ;
        """
        with conn.cursor() as cur:
            cur.executemany(ins, rows)
        conn.commit()
    print(f"[db] upserted {len(rows)} rows into staging_coop_products")

# --------------------- main ---------------------

def main():
    ap = argparse.ArgumentParser(description="Wolt crawler (consumer API + robust headers + DB upsert)")
    ap.add_argument("--store-url")
    ap.add_argument("--store-host")
    ap.add_argument("--city")
    ap.add_argument("--out")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--language", default="et")
    ap.add_argument("--categories-file")
    ap.add_argument("--upsert-db", default="1", help="Upsert into staging_coop_products if DATABASE_URL is set (default: 1)")
    # legacy flags ignored (kept for CI)
    ap.add_argument("--max-products"); ap.add_argument("--headless")
    ap.add_argument("--req-delay"); ap.add_argument("--goto-strategy")
    ap.add_argument("--nav-timeout"); ap.add_argument("--category-timeout")
    ap.add_argument("--upsert-per-category", action="store_true")
    ap.add_argument("--flush-every"); ap.add_argument("--probe-limit")
    ap.add_argument("--modal-probe-limit")
    args = ap.parse_args()

    # UUIDs that stand in for the browser IDs we saw in cURL
    client_id = str(uuid.uuid4())
    analytics_id = str(uuid.uuid4())
    session_id = analytics_id

    # Venue & city resolution (categories file can override base URL)
    file_slugs: List[str] = []; file_base_url: Optional[str] = None
    if args.categories_file:
        p = Path(args.categories_file)
        if not p.exists():
            raise SystemExit(f"[error] categories file not found: {p}")
        file_slugs, file_base_url = parse_categories_file(p)

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
        # Improved inference: recognizes Tallinn venues and 'coop-prnu'
        city = _normalize_city(args.city or infer_city_from_string(args.store_host))
    else:
        ap.error("Provide --store-url or --store-host (or include full URLs in --categories-file).")
        return

    # Headers + cookies exactly like the browser
    headers = _browserish_headers(args.language, city, client_id, session_id)
    cookies = _cookie_string(city, client_id, analytics_id)
    session = _base_requests_session(headers, cookies)

    print(f"[info] Store URL:   {store_url}")
    print(f"[info] Store host:  {store_host}")
    print(f"[info] Language:    {args.language}")
    print(f"[info] City tag:    {city}")

    # Prime session context like a browser
    try:
        session.get(store_url, timeout=20)
    except Exception:
        pass

    # Category slugs
    if file_slugs:
        slugs = file_slugs
        print(f"[info] Using {len(slugs)} category slugs from file: {args.categories_file}")
    else:
        slugs = discover_category_slugs(session, store_url)
        if not slugs:
            raise SystemExit("[error] Could not find any category slugs on the venue page.")
        print(f"[info] Found {len(slugs)} category slugs via HTML discovery")

    # Crawl
    all_rows_csv: List[Dict[str, Any]] = []
    all_rows_db: List[Dict[str, Any]] = []
    global_venue_id = ""
    scraped_at = dt.datetime.utcnow().isoformat()

    for i, slug in enumerate(slugs, 1):
        try:
            data = fetch_category_json(session, store_url, slug, args.language, headers, cookies)
        except Exception as e:
            print(f"[warn] Failed category '{slug}': {e}")
            continue

        rows, venue_id, src_url = extract_rows(data, store_host, slug)
        if venue_id and not global_venue_id:
            global_venue_id = venue_id

        # CSV rows
        all_rows_csv.extend(rows)

        # DB rows projection (uses already-scaled euros from extract_rows)
        for r in rows:
            all_rows_db.append(
                dict(
                    chain=CHAIN,
                    channel=CHANNEL,
                    store_name=store_host.split(":",1)[-1].replace("-", " "),  # best-effort
                    store_host=store_host,
                    city_path=city,
                    category_name=r.get("category_name") or slug,
                    ext_id=r.get("_ext_id"),
                    name=r.get("name"),
                    brand=None,
                    manufacturer=None,
                    size_text=None,
                    price=(r.get("price") if isinstance(r.get("price"), (int,float)) else None),
                    currency="EUR",
                    image_url=r.get("image_url"),
                    url=src_url or store_url,
                    description=r.get("description"),
                    ean_raw=r.get("barcode_gtin"),
                    scraped_at=scraped_at,
                )
            )

        print(f"[ok]  {i:>2}/{len(slugs)}  '{slug}' → {len(rows)} items")
        time.sleep(0.12)

    if not global_venue_id:
        try:
            probe = discover_category_slugs(session, store_url)
            if probe:
                d = fetch_category_json(session, store_url, probe[0], args.language, headers, cookies)
                if isinstance(d.get("venue_id"), str) and d["venue_id"]:
                    global_venue_id = d["venue_id"]
        except Exception:
            pass
    if not global_venue_id:
        global_venue_id = "unknown"

    # CSV out
    out_path = Path(args.out) if args.out else Path(args.out_dir) / f"coop_wolt_{global_venue_id or 'unknown'}_{_normalize_city(city)}.csv"
    write_csv(all_rows_csv, out_path)
    print(f"[done] Wrote {len(all_rows_csv)} rows → {out_path}")

    # Optional DB upsert
    if str(args.upsert_db) == "1" and os.getenv("DATABASE_URL"):
        upsert_rows_to_staging_coop(all_rows_db, os.getenv("DATABASE_URL"))
    else:
        if str(args.upsert_db) == "1":
            print("[db] DATABASE_URL not set; skipping upsert")

if __name__ == "__main__":
    main()

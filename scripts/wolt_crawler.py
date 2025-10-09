#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt store crawler (consumer API with browser-like headers; Playwright/Next.js fallbacks)

Primary endpoint per category (we try both forms, because the site uses both):
  1) https://consumer-api.wolt.com/consumer-assortment/v1/venues/slug/<venue>/assortment/categories/slug/<slug>?language=<lang>
  2) https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1/venues/slug/<venue>/assortment/categories/slug/<slug>?language=<lang>

If those fail, we fall back to Playwright network using the same headers, then to scraping
__NEXT_DATA__ from the items page.

NEW: If a category returns 0 items, we will auto-expand it by scraping its
/items/<parent-slug> page and trying likely child slugs from the left menu.

CSV columns are stable with the rest of the pipeline.
"""

import re
import csv
import time
import json
import uuid
import argparse
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selectolax.parser import HTMLParser

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

# generic "find slug from venue items URL" (works both on venue landing and items pages)
CATEGORY_HREF_RE = re.compile(r"/venue/[^/]+/items/([^/?#]+)")

GEO = {
    "parnu":   {"latitude": 58.3859, "longitude": 24.4971, "accuracy": 1500},
    "tallinn": {"latitude": 59.4370, "longitude": 24.7536, "accuracy": 1500},
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
    """
    Headers mirrored from your cURL (safe to send without auth).
    """
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": f"{language}-EE,{language};q=0.9,en;q=0.8",
        "app-language": language,
        # harmless placeholders that some endpoints expect
        "client-version": "1.16.39",
        "clientversionnumber": "1.16.39",
        "platform": "Web",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "origin": WOLT_HOST,
        "referer": f"{WOLT_HOST}/",
        "user-agent": UA,
        # city selection still helps some edge cases
        "x-city-id": _normalize_city(city),
        # ids seen in network requests (any UUID works)
        "x-wolt-web-clientid": client_id,          # seen variant without dash, include this
        "x-wolt-web-client-id": client_id,         # include dashed variant just in case
        "w-wolt-session-id": session_id,
    }

def _cookie_string(city: str, client_id: str, analytics_id: str) -> str:
    """
    Minimal cookie set that has worked reliably.
    We include city selection + two stable IDs (client/analytics).
    """
    # A permissive consent blob so the API doesn't hide anything
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
    # Cookie header
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
    session: requests.Session,
    venue_slug: str,
    category_slug: str,
    language: str,
    header_variants: List[Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    """
    Try both API base paths *and* a few header variants (with/without x-city-id, alt city).
    Return the first payload that contains non-empty 'items'.
    """
    for hdrs in header_variants:
        # Temporarily override session headers for this attempt
        old_vals = {k: session.headers.get(k) for k in hdrs}
        session.headers.update(hdrs)
        try:
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
                    data = r.json()
                except Exception:
                    continue
                # Accept only if the API actually returned items
                if isinstance(data, dict) and isinstance(data.get("items"), list) and data["items"]:
                    return data
        finally:
            # Restore original header values
            for k, v in old_vals.items():
                if v is None:
                    session.headers.pop(k, None)
                else:
                    session.headers[k] = v
    return None

# --------------------- Playwright fallbacks ---------------------

def _playwright_context(headers: Dict[str, str]):
    # Map our browser-like headers into a Playwright context
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
                        data = resp.json()
                    except Exception:
                        data = None
                    if isinstance(data, dict) and isinstance(data.get("items"), list) and data["items"]:
                        return data
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
                norm_items.append({
                    "id": it.get("id") or it.get("_id") or "",
                    "name": it.get("name") or it.get("title") or "",
                    "price": it.get("price") if isinstance(it.get("price"), (int, float)) else None,
                    "unit_info": it.get("unit_info") or "",
                    "unit_price": {
                        "price": (it.get("unit_price", {}) or {}).get("price") if isinstance(it.get("unit_price"), dict) else None,
                        "unit":  (it.get("unit_price", {}) or {}).get("unit")  if isinstance(it.get("unit_price"), dict) else "",
                    },
                    "barcode_gtin": it.get("barcode_gtin") or "",
                    "description": it.get("description") or "",
                    "checksum": it.get("checksum") or "",
                    "vat_category_code": it.get("vat_category_code") or "",
                    "vat_percentage": it.get("vat_percentage") if isinstance(it.get("vat_percentage"), (int, float)) else None,
                    "images": [{"url": (it.get("image") or it.get("image_url") or "")}],
                })
            return {"category": {"name": cat_name, "id": category_slug}, "items": norm_items}
        except Exception:
            return None
        finally:
            context.close(); browser.close()

# --------------------- parent → child auto-expansion ---------------------

def discover_child_slugs_from_items_page(session: requests.Session, store_url: str, parent_slug: str, language: str) -> List[str]:
    """
    Load the /items/<parent> page and collect candidate child slugs from the left sidebar.
    We simply collect all /items/<slug> links on that page (in order), then de-duplicate
    and drop the parent itself. We will later verify each candidate by trying the API.
    """
    base = normalize_store_url(store_url)
    url = urljoin(base + "/", f"items/{parent_slug}?language={language}")
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception:
        return []

    candidates: List[str] = []
    for m in CATEGORY_HREF_RE.finditer(r.text):
        slug = m.group(1)
        if slug and slug != parent_slug:
            candidates.append(slug)

    # keep order, drop dups
    seen = set()
    ordered: List[str] = []
    for s in candidates:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    return ordered

def expand_parent_category(
    session: requests.Session,
    store_url: str,
    parent_slug: str,
    language: str,
    headers: Dict[str, str],
    cookies: str,
    max_children: int = 12,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Try to auto-discover and fetch child categories for a 'parent' slug that returned 0 items.
    We scrape candidates from the page and then keep only those that return non-empty items.
    """
    child_slugs = discover_child_slugs_from_items_page(session, store_url, parent_slug, language)
    if not child_slugs:
        return []

    found: List[Tuple[str, Dict[str, Any]]] = []
    consecutive_empty = 0
    for s in child_slugs:
        # stop if we have enough, or too many empties
        if len(found) >= max_children:
            break
        data = fetch_category_json(session, store_url, s, language, headers, cookies)
        if isinstance(data, dict) and isinstance(data.get("items"), list) and data["items"]:
            found.append((s, data))
            consecutive_empty = 0
        else:
            consecutive_empty += 1
            # a small guard to avoid testing the entire menu if it's not subcats
            if len(found) >= 2 and consecutive_empty >= 6:
                break
    return found

# --------------------- orchestration ---------------------

def fetch_category_json(
    session: requests.Session, store_url: str, category_slug: str, language: str, headers: Dict[str, str], cookies: str
) -> Dict[str, Any]:
    venue_slug = venue_slug_from_url(store_url)

    # Header variants to dodge empty arrays from API when context doesn't match exactly
    base_city = headers.get("x-city-id", "")
    header_variants: List[Dict[str, str]] = [
        {},                         # as-is
        {"x-city-id": ""},          # drop city header
        {"x-city-id": "tallinn"},   # alternate city sometimes coerces data
        {"x-city-id": base_city},   # restore explicit (noop for first pass, helps on later calls)
    ]

    # 1) Consumer API via requests with browser-like headers/cookies
    data = consumer_api_fetch_category_json(session, venue_slug, category_slug, language, header_variants)
    if data and isinstance(data.get("items"), list) and data["items"]:
        return data

    # 2) Same via Playwright network
    data = playwright_fetch_consumer_api(store_url, venue_slug, category_slug, language, headers, cookies)
    if data and isinstance(data.get("items"), list) and data["items"]:
        return data

    # 3) As a last resort, scrape Next.js data
    data = playwright_nextdata_items(store_url, category_slug, language, headers)
    if data and isinstance(data.get("items"), list) and data["items"]:
        return data

    # If everything returned empty or failed, produce an explicit empty payload so caller can log 0
    return {"category": {"id": category_slug, "name": category_slug}, "items": []}

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

def maybe_fill_venue(session: requests.Session, store_url: str, language: str, headers: Dict[str, str], cookies: str) -> str:
    slugs = discover_category_slugs(session, store_url)
    if not slugs:
        return ""
    data = fetch_category_json(session, store_url, slugs[0], language, headers, cookies)
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
    ap = argparse.ArgumentParser(description="Wolt crawler (consumer API + robust headers + parent auto-expansion)")
    ap.add_argument("--store-url")
    ap.add_argument("--store-host")
    ap.add_argument("--city")
    ap.add_argument("--out")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--language", default="et")
    ap.add_argument("--categories-file")
    # Accept legacy flags used by CI (ignored)
    ap.add_argument("--max-products"); ap.add_argument("--headless")
    ap.add_argument("--req-delay"); ap.add_argument("--goto-strategy")
    ap.add_argument("--nav-timeout"); ap.add_argument("--category-timeout")
    ap.add_argument("--upsert-per-category", action="store_true")
    ap.add_argument("--flush-every"); ap.add_argument("--probe-limit")
    ap.add_argument("--modal-probe-limit")
    # tuning for parent expansion
    ap.add_argument("--max-children-per-parent", type=int, default=12)
    args = ap.parse_args()

    # UUIDs that stand in for the browser IDs we saw in your cURL
    client_id = str(uuid.uuid4())
    analytics_id = str(uuid.uuid4())
    session_id = analytics_id  # good enough

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

    # Prime session context like a browser (sets cookies, geo, etc.)
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

    all_rows: List[Dict[str, Any]] = []
    global_venue_id = ""

    for i, slug in enumerate(slugs, 1):
        try:
            data = fetch_category_json(session, store_url, slug, args.language, headers, cookies)
        except Exception as e:
            print(f"[warn] Failed category '{slug}': {e}")
            continue

        rows, venue_id = extract_rows(data, store_host, slug)

        # If this looks like a parent (0 items), try to auto-expand
        if not rows:
            children = expand_parent_category(
                session=session,
                store_url=store_url,
                parent_slug=slug,
                language=args.language,
                headers=headers,
                cookies=cookies,
                max_children=args.max_children_per_parent,
            )
            if children:
                total_child_rows = 0
                for child_slug, child_data in children:
                    child_rows, child_vid = extract_rows(child_data, store_host, child_slug)
                    if child_vid and not global_venue_id:
                        global_venue_id = child_vid
                    all_rows.extend(child_rows)
                    total_child_rows += len(child_rows)
                    print(f"[ok]  .. expanded '{slug}' → child '{child_slug}' = {len(child_rows)} items")
                print(f"[ok]  {i:>2}/{len(slugs)}  '{slug}' (parent) expanded → {total_child_rows} items across {len(children)} children")
            else:
                print(f"[ok]  {i:>2}/{len(slugs)}  '{slug}' → 0 items (no children found)")
        else:
            if venue_id and not global_venue_id:
                global_venue_id = venue_id
            all_rows.extend(rows)
            print(f"[ok]  {i:>2}/{len(slugs)}  '{slug}' → {len(rows)} items")

        time.sleep(0.12)

    if not global_venue_id:
        try:
            global_venue_id = maybe_fill_venue(session, store_url, args.language, headers, cookies) or "unknown"
        except Exception:
            global_venue_id = "unknown"

    out_path = Path(args.out) if args.out else Path(args.out_dir) / f"coop_wolt_{global_venue_id or 'unknown'}_{_normalize_city(city)}.csv"
    write_csv(all_rows, out_path)
    print(f"[done] Wrote {len(all_rows)} rows → {out_path}")

if __name__ == "__main__":
    main()

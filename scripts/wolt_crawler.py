#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt store crawler (JSON endpoints, no browser automation)

- Discovers category slugs from a Wolt venue page HTML OR reads them from --categories-file.
- Fetches JSON from /items/<slug>?language=<lang> for each category.
- Emits a CSV with all items.

Works with either:
  A) --store-url https://wolt.com/et/est/parnu/venue/coop-prnu
  B) --store-host wolt:coop-parnu  [and optional --city parnu]
Plus an optional:
  --categories-file data/wolt_coop_parnu_categories.txt
    * lines can be either slugs (e.g. "soe-toit-3") or full URLs
      (e.g. "https://wolt.com/et/est/parnu/venue/coop-prnu/items/soe-toit-3")

Output path:
  --out <exact filepath>  (preferred in CI)  OR
  --out-dir <dir> + auto filename "coop_wolt_<venueId>_<city>.csv"

Crawler #9 changes:
- Removed lxml; use selectolax for HTML parsing.
- Added dual-CLI compatibility (store-url / store-host, out / out-dir).
- categories-file now first-class (supports full URLs or slugs).
- **City context headers/cookies** added to mimic being in Pärnu (or inferred city).
- 404 fallback retry once with Tallinn city context.
"""

import re
import csv
import time
import argparse
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selectolax.parser import HTMLParser

# ----------------------------- Constants -------------------------------- #

WOLT_HOST = "https://wolt.com"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# e.g. /et/est/parnu/venue/coop-prnu/items/kohv-117
CATEGORY_HREF_RE = re.compile(r"/venue/[^/]+/items/([^/?#]+)")

# ----------------------------- Helpers ---------------------------------- #

def _base_headers() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

def _city_headers(city: str) -> Dict[str, str]:
    """
    Nudge Wolt to treat us as located in a specific city.
    """
    c = (city or "").lower()
    # normalize
    if c.startswith("pär"):  # "pärnu"
        c = "parnu"
    if c in ("lasname", "lasnamae", "lasnamäe", "lasna"):
        # Lasnamäe is a district; city context must still be Tallinn
        c = "tallinn"
    if c not in ("parnu", "tallinn"):
        c = "parnu"
    return {
        "x-city-id": c,
        "Cookie": f"wolt-session-city={c}",
    }

def make_session(city: str) -> requests.Session:
    """
    Build a session with retries and city/location context so the site returns data.
    """
    s = requests.Session()
    retries = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(_base_headers())
    s.headers.update(_city_headers(city))
    return s

def normalize_store_url(store_url: str) -> str:
    """Force base venue URL like https://wolt.com/et/est/parnu/venue/coop-prnu"""
    store_url = store_url.strip()
    if not store_url.startswith("http"):
        store_url = urljoin(WOLT_HOST, store_url)
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    if "venue" in segs:
        idx = segs.index("venue")
        segs = segs[: idx + 2]  # keep .../venue/<slug>
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
    # store_host like "wolt:coop-parnu"
    slug = store_host.split(":", 1)[-1]
    city = city_hint or infer_city_from_string(slug)
    return f"{WOLT_HOST}/et/est/{city}/venue/{slug}"

def infer_store_host_from_url(store_url: str) -> str:
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    store_slug = segs[-1] if segs else "unknown-store"
    return f"wolt:{store_slug}"

def discover_category_slugs(session: requests.Session, store_url: str) -> List[str]:
    """Parse the venue page HTML to discover category slugs under /items/<slug>."""
    r = session.get(store_url, timeout=30)
    r.raise_for_status()

    slugs: set[str] = set()
    tree = HTMLParser(r.text)

    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        m = CATEGORY_HREF_RE.search(href)
        if m:
            slugs.add(m.group(1))

    # Fallback on raw HTML scan
    if not slugs:
        for m in re.findall(r"/venue/[^/]+/items/([a-z0-9\-]+)", r.text):
            slugs.add(m)

    return sorted(slugs)

def parse_categories_file(path: Path) -> Tuple[List[str], Optional[str]]:
    """
    Read categories file.
    Lines can be:
      - full URLs: https://wolt.com/.../venue/<slug>/items/<cat>
      - or just slugs: soe-toit-3
    Returns (unique_slugs_in_order, base_store_url_if_derived_from_urls)
    """
    slugs: List[str] = []
    base_url: Optional[str] = None

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("http"):
            # Extract slug
            m = CATEGORY_HREF_RE.search(line)
            if not m:
                continue
            slugs.append(m.group(1))

            # Derive base venue URL from the URL path (only once)
            if base_url is None:
                u = urlparse(line)
                parts = u.path.split("/items/")[0]
                base_url = f"{u.scheme}://{u.netloc}{parts}".rstrip("/")
        else:
            # plain slug
            slugs.append(line)

    # de-dupe while preserving order
    seen = set()
    ordered = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            ordered.append(s)

    return ordered, base_url

def fetch_category_json(
    session: requests.Session, base_store_url: str, slug: str, language: str
) -> Dict[str, Any]:
    """GET .../venue/<store>/items/<slug>?language=<lang> (with city context)."""
    url = urljoin(base_store_url + "/", f"items/{slug}")
    params = {"language": language}

    r = session.get(url, params=params, timeout=30)
    # If Wolt shows the “journey ends” page, it often comes as 404 with HTML.
    if r.status_code == 404:
        # Retry once with Tallinn city context
        alt_headers = dict(session.headers)
        alt_headers.update(_city_headers("tallinn"))
        r2 = session.get(url, params=params, headers=alt_headers, timeout=30)
        if not r2.ok:
            r.raise_for_status()  # raise the original 404
        r = r2

    r.raise_for_status()

    text = r.text.strip()
    if text.startswith("{"):
        return r.json()

    # Sometimes first body isn't JSON; retry once.
    time.sleep(0.5)
    r2 = session.get(url, params=params, timeout=30)
    r2.raise_for_status()
    if r2.text.strip().startswith("{"):
        return r2.json()

    raise ValueError(f"Category endpoint did not return JSON for slug={slug}. Got: {r.text[:120]}...")

def extract_rows(
    payload: Dict[str, Any],
    store_host: str,
    category_slug: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """Flatten a category JSON payload into rows; return rows and venue_id if seen."""
    rows: List[Dict[str, Any]] = []
    venue_id: str = ""

    category = payload.get("category", {}) or {}
    items = payload.get("items", []) or []

    for it in items:
        if not venue_id:
            venue_id = it.get("venue_id", "") or ""
        row = {
            "store_host": store_host,
            "venue_id": venue_id,  # may be blank; will fill later if we find it
            "category_slug": category_slug,
            "category_name": category.get("name", ""),
            "category_id": category.get("id", ""),
            "item_id": it.get("id", ""),
            "name": it.get("name", ""),
            "price": it.get("price", None),  # integer cents
            "unit_info": it.get("unit_info", ""),
            "unit_price_value": (it.get("unit_price", {}) or {}).get("price", None),
            "unit_price_unit": (it.get("unit_price", {}) or {}).get("unit", ""),
            "barcode_gtin": it.get("barcode_gtin", ""),
            "description": it.get("description", ""),
            "checksum": it.get("checksum", ""),
            "vat_category_code": it.get("vat_category_code", ""),
            "vat_percentage": it.get("vat_percentage", None),
            "image_url": ((it.get("images") or [{}])[0] or {}).get("url", ""),
        }
        rows.append(row)

    return rows, venue_id

def maybe_fill_venue(session: requests.Session, store_url: str, language: str) -> str:
    """Try to obtain venue_id from a category payload or its items."""
    slugs = discover_category_slugs(session, store_url)
    if not slugs:
        return ""
    data = fetch_category_json(session, store_url, slugs[0], language)
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

# ------------------------------ Main ----------------------------------- #

def main():
    ap = argparse.ArgumentParser(description="Wolt JSON crawler")

    # Primary inputs (either store-url or store-host)
    ap.add_argument("--store-url", help="e.g. https://wolt.com/et/est/parnu/venue/coop-prnu")
    ap.add_argument("--store-host", help='e.g. "wolt:coop-parnu"')

    # Optional hints
    ap.add_argument("--city", help="city tag for filename and headers (e.g., parnu)")

    # Output options
    ap.add_argument("--out", help="Exact output filepath (CSV). If not given, uses --out-dir + auto filename.")
    ap.add_argument("--out-dir", default="out", help="Output directory when --out is not provided")

    # Other options
    ap.add_argument("--language", default="et", help="language code used by Wolt endpoint (default: et)")

    # Categories file (now actually used)
    ap.add_argument("--categories-file", help="File with category slugs or full URLs (one per line)")

    # Compatibility flags (accepted but unused in JSON mode)
    ap.add_argument("--max-products")
    ap.add_argument("--headless")
    ap.add_argument("--req-delay")
    ap.add_argument("--goto-strategy")
    ap.add_argument("--nav-timeout")
    ap.add_argument("--category-timeout")
    ap.add_argument("--upsert-per-category", action="store_true")
    ap.add_argument("--flush-every")
    ap.add_argument("--probe-limit")
    ap.add_argument("--modal-probe-limit")

    args = ap.parse_args()

    # Resolve slugs if a categories file is provided
    file_slugs: List[str] = []
    file_base_url: Optional[str] = None
    if args.categories_file:
        p = Path(args.categories_file)
        if not p.exists():
            raise SystemExit(f"[error] categories file not found: {p}")
        file_slugs, file_base_url = parse_categories_file(p)

    # Resolve store URL / host
    if args.store_host and not args.store_url:
        store_url = build_url_from_host(args.store_host, args.city)
        store_host = args.store_host
        city = args.city or infer_city_from_string(args.store_host)
    elif args.store_url:
        store_url = normalize_store_url(args.store_url)
        store_host = infer_store_host_from_url(store_url)
        city = args.city or infer_city_from_string(store_url)
    else:
        # Neither provided → try deriving from categories file URLs
        if file_base_url:
            store_url = normalize_store_url(file_base_url)
            store_host = infer_store_host_from_url(store_url)
            city = args.city or infer_city_from_string(store_url)
        else:
            ap.error("Provide --store-url or --store-host (or include full URLs in --categories-file).")
            return  # unreachable

    session = make_session(city)

    print(f"[info] Store URL:   {store_url}")
    print(f"[info] Store host:  {store_host}")
    print(f"[info] Language:    {args.language}")
    print(f"[info] City tag:    {city}")

    # Determine category slugs: prefer file if given and non-empty; else discover
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
            data = fetch_category_json(session, store_url, slug, args.language)
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
        global_venue_id = maybe_fill_venue(session, store_url, args.language) or "unknown"

    # Decide output path
    if args.out:
        out_path = Path(args.out)
    else:
        out_filename = f"coop_wolt_{global_venue_id}_{city}.csv"
        out_path = Path(args.out_dir) / out_filename

    write_csv(all_rows, out_path)
    print(f"[done] Wrote {len(all_rows)} rows → {out_path}")

if __name__ == "__main__":
    main()

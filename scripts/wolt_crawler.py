#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wolt store crawler (JSON endpoints, no browser automation)

- Discovers category slugs from a Wolt venue page HTML.
- Fetches JSON from /items/<slug>?language=<lang> for each category.
- Emits a single CSV with all items.

Usage:
  python wolt_crawler.py \
    --store-url "https://wolt.com/et/est/parnu/venue/coop-prnu" \
    --city parnu \
    --language et \
    --out-dir out

Notes:
- Relies on the same JSON you saw in DevTools under requests like:
    https://wolt.com/et/est/parnu/venue/coop-prnu/items/kohv-117?language=et
- Keeps fields stable; feel free to add/remove columns as needed.

Change log (crawler #9):
- Removed lxml dependency; switched to selectolax for HTML parsing.
- Fixed Retry import (use urllib3 Retry).
"""

import re
import csv
import time
import json
import argparse
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selectolax.parser import HTMLParser

# ----------------------------- Helpers --------------------------------- #

WOLT_HOST = "https://wolt.com"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# e.g. /et/est/parnu/venue/coop-prnu/items/kohv-117
CATEGORY_HREF_RE = re.compile(r"/venue/[^/]+/items/([^/?#]+)")

def make_session() -> requests.Session:
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
    s.headers.update(HEADERS)
    return s

def normalize_store_url(store_url: str) -> str:
    """
    Force base venue URL like https://wolt.com/et/est/parnu/venue/coop-prnu
    """
    store_url = store_url.strip()
    if not store_url.startswith("http"):
        store_url = urljoin(WOLT_HOST, store_url)
    # strip trailing paths like /items/.. if user pasted a category page
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    # expected .../<lang>/<country>/<city>/venue/<slug>
    if "venue" in segs:
        idx = segs.index("venue")
        segs = segs[: idx + 2]  # keep .../venue/<slug>
    clean_path = "/" + "/".join(segs)
    return f"{parts.scheme}://{parts.netloc}{clean_path}"

def discover_category_slugs(session: requests.Session, store_url: str) -> List[str]:
    """
    Parse the venue page HTML to discover category slugs under /items/<slug>.
    We scan all anchors and extract those matching the pattern.
    """
    r = session.get(store_url, timeout=30)
    r.raise_for_status()

    slugs: set[str] = set()

    # Parse with selectolax
    tree = HTMLParser(r.text)

    # Look at all anchors
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        m = CATEGORY_HREF_RE.search(href)
        if m:
            slugs.add(m.group(1))

    # Fallback: scan raw HTML for /items/<slug>
    if not slugs:
        for m in re.findall(r"/venue/[^/]+/items/([a-z0-9\-]+)", r.text):
            slugs.add(m)

    return sorted(slugs)

def fetch_category_json(
    session: requests.Session, base_store_url: str, slug: str, language: str
) -> Dict[str, Any]:
    """
    GET https://wolt.com/.../venue/<store>/items/<slug>?language=<lang>
    Returns JSON exactly like you saw in DevTools.
    """
    url = urljoin(base_store_url + "/", f"items/{slug}")
    params = {"language": language}
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()

    text = r.text.strip()
    if text and text[0] == "{":
        return r.json()

    # Sometimes the first response is a short/non-JSON body; retry once.
    time.sleep(0.5)
    r2 = session.get(url, params=params, timeout=30)
    r2.raise_for_status()
    if r2.text.strip().startswith("{"):
        return r2.json()

    raise ValueError(f"Category endpoint did not return JSON for slug={slug}. Got: {r.text[:120]}...")

def infer_store_host(store_url: str) -> str:
    """
    e.g., 'wolt:coop-prnu' from https://wolt.com/et/est/parnu/venue/coop-prnu
    """
    parts = urlparse(store_url)
    segs = [p for p in parts.path.split("/") if p]
    store_slug = segs[-1] if segs else "unknown-store"
    return f"wolt:{store_slug}"

def extract_rows(
    payload: Dict[str, Any],
    store_host: str,
    category_slug: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Turn a single category JSON payload into flat rows.
    Also returns venue_id if present (we use it to build output filename).
    """
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
    """
    Try to obtain a reliable venue_id from a category payload or items.
    """
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
        if rows:
            for r in rows:
                # ensure all columns exist
                for k in fieldnames:
                    r.setdefault(k, "")
                w.writerow(r)

# ------------------------------ Main ----------------------------------- #

def main():
    ap = argparse.ArgumentParser(description="Wolt JSON crawler")
    ap.add_argument("--store-url", required=True, help="e.g. https://wolt.com/et/est/parnu/venue/coop-prnu")
    ap.add_argument("--city", required=True, help="city tag for filename (e.g., parnu)")
    ap.add_argument("--language", default="et", help="language code used by Wolt endpoint (default: et)")
    ap.add_argument("--out-dir", default="out", help="output directory")
    args = ap.parse_args()

    store_url = normalize_store_url(args.store_url)
    store_host = infer_store_host(store_url)

    session = make_session()

    print(f"[info] Store URL:   {store_url}")
    print(f"[info] Store host:  {store_host}")
    print(f"[info] Language:    {args.language}")

    slugs = discover_category_slugs(session, store_url)
    if not slugs:
        raise SystemExit("[error] Could not find any category slugs on the venue page.")

    print(f"[info] Found {len(slugs)} category slugs")

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

        # be polite
        time.sleep(0.2)

    if not global_venue_id:
        global_venue_id = maybe_fill_venue(session, store_url, args.language) or "unknown"

    # Output path: coop_wolt_<venueId>_<city>.csv
    out_filename = f"coop_wolt_{global_venue_id}_{args.city}.csv"
    out_path = Path(args.out_dir) / out_filename
    write_csv(all_rows, out_path)

    print(f"[done] Wrote {len(all_rows)} rows → {out_path}")

if __name__ == "__main__":
    main()

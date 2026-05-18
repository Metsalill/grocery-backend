#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee fast scraper — requests + BeautifulSoup (no Playwright needed)

HTML page source contains all product data in data-gtm-eec-product attributes:
  {"id":"274969","name":"Viigimari, tk","category":"SH-12-5-28","brand":null,"price":1.19,"currency":"EUR"}

Also reads source_url from card href and image from img data-src.

Speed: ~125 HTTP requests for 10,000 products vs ~10,000 Playwright PDP loads.
"""

from __future__ import annotations
import argparse, os, re, csv, json, sys, time, asyncio
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import asyncpg

BASE = "https://www.rimi.ee"
PAGE_SIZE = 80

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124 Safari/537.36"
    ),
    "Accept-Language": "et-EE,et;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ------------------------------- utils ---------------------------------------

def to_float_price(s: Any) -> Optional[float]:
    if s is None:
        return None
    try:
        v = float(str(s).replace(",", ".").strip())
        return v if v > 0 else None
    except Exception:
        return None

def extract_ext_id(url: str) -> str:
    m = re.search(r"/p/(\d+)", url or "")
    return m.group(1) if m else ""

def normalize_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.split("?")[0].split("#")[0]
    return href if href.startswith("http") else urljoin(BASE, href)

# ------------------------------- category page parser ------------------------

def parse_category_page(html: str, cat_url: str) -> Tuple[List[Dict], int, int]:
    """
    Parse one category page HTML.
    Returns (products, current_page, total_pages).
    """
    soup = BeautifulSoup(html, "lxml")
    products = []

    for card in soup.select("[data-gtm-eec-product]"):
        raw = card.get("data-gtm-eec-product", "")
        try:
            data = json.loads(raw)
        except Exception:
            continue

        ext_id = str(data.get("id") or "")
        name = (data.get("name") or "").strip()
        price = data.get("price")
        brand = (data.get("brand") or "").strip() if data.get("brand") else ""
        category = (data.get("category") or "").strip()

        if not ext_id or not name:
            continue

        # source URL from card link
        link = card.select_one("a.card__url")
        source_url = normalize_href(link.get("href")) if link else f"{BASE}/epood/ee/tooted/p/{ext_id}"

        # image
        img = card.select_one("img[data-src]") or card.select_one("img")
        image_url = ""
        if img:
            image_url = normalize_href(img.get("data-src") or img.get("src") or "") or ""

        # size_text from name (e.g. "200g", "1kg", "500ml")
        size_match = re.search(
            r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|tk))\b',
            name, re.I
        )
        size_text = size_match.group(1) if size_match else ""

        products.append({
            "ext_id": ext_id,
            "name": name,
            "brand": brand,
            "size_text": size_text,
            "price": str(price) if price is not None else "",
            "currency": (data.get("currency") or "EUR"),
            "image_url": image_url,
            "category_path": category,
            "category_leaf": category.split("-")[-1] if category else "",
            "source_url": source_url or "",
            "ean_raw": "",
            "sku_raw": ext_id,
        })

    # pagination
    current_page = 1
    total_pages = 1

    # current page from hidden input
    page_input = soup.select_one("input#currentPage")
    if page_input and page_input.get("value"):
        try:
            current_page = int(page_input["value"])
        except Exception:
            pass

    # total pages from pagination
    pag = soup.select("ul.pagination li a, .pagination__item")
    page_nums = []
    for p in pag:
        txt = p.get_text(strip=True)
        try:
            page_nums.append(int(txt))
        except Exception:
            pass
    if page_nums:
        total_pages = max(page_nums)

    # fallback: check if "next" button exists
    if total_pages == 1:
        has_next = soup.select_one("a[rel='next'], .pagination__next:not(.disabled)")
        if has_next:
            total_pages = current_page + 1

    return products, current_page, total_pages


# ------------------------------- crawler -------------------------------------

def crawl_category(
    cat_url: str,
    req_delay: float = 0.3,
    session: Optional[requests.Session] = None,
) -> List[Dict]:
    """Crawl all pages of one category URL."""
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)

    all_products: List[Dict] = []
    page = 1

    while True:
        url = f"{cat_url}?pageSize={PAGE_SIZE}&currentPage={page}"
        print(f"  [rimi-req] GET {url}")

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [rimi-req] ERROR {url}: {e}", file=sys.stderr)
            break

        products, cur_page, total_pages = parse_category_page(resp.text, cat_url)
        all_products.extend(products)
        print(f"  [rimi-req] page {cur_page}/{total_pages} — {len(products)} products")

        if cur_page >= total_pages or not products:
            break

        page += 1
        if req_delay > 0:
            time.sleep(req_delay)

    return all_products


def read_categories(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]


def write_csv(rows: List[Dict], out_path: str) -> None:
    fields = [
        "store_chain", "store_name", "store_channel",
        "ext_id", "ean_raw", "sku_raw", "name", "size_text", "brand", "manufacturer",
        "price", "currency", "image_url", "category_path", "category_leaf", "source_url",
    ]
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    new_file = not os.path.exists(out_path)
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


# ------------------------------- DB ingest -----------------------------------

async def _bulk_ingest_to_db(rows: List[Dict], store_id: int) -> None:
    if store_id <= 0:
        print("[rimi-req] STORE_ID not set, skipping DB ingest.")
        return

    dsn = os.environ.get("DATABASE_URL") or os.environ.get("DB_DSN") or os.environ.get("PG_DSN")
    if not dsn:
        print("[rimi-req] DATABASE_URL not set, skipping DB ingest.")
        return

    pool = await asyncpg.create_pool(dsn)
    try:
        upserted = 0
        for r in rows:
            price_val = to_float_price(r.get("price", ""))
            if price_val is None:
                continue
            try:
                await pool.fetchval(
                    """
                    SELECT upsert_product_and_price(
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10
                    );
                    """,
                    "rimi",
                    r.get("ext_id") or "",
                    r.get("name") or "",
                    r.get("brand") or "",
                    r.get("size_text") or "",
                    r.get("ean_raw") or "",
                    price_val,
                    (r.get("currency") or "EUR"),
                    store_id,
                    r.get("source_url") or "",
                )
                upserted += 1
            except Exception as ex:
                print(f"[rimi-req] upsert FAILED ext_id={r.get('ext_id')}: {ex}")

        print(f"[rimi-req] DB ingest complete ({upserted} rows upserted).")
    finally:
        await pool.close()


# ------------------------------- main ----------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cats-file", required=True)
    ap.add_argument("--output-csv", default=os.environ.get("OUTPUT_CSV", "data/rimi_products.csv"))
    ap.add_argument("--req-delay", default="0.3")
    ap.add_argument("--max-products", default="0")
    args = ap.parse_args()

    req_delay = float(args.req_delay or "0.3")
    max_products = int(args.max_products or "0")
    cats = read_categories(args.cats_file)

    session = requests.Session()
    session.headers.update(HEADERS)

    all_rows: List[Dict] = []
    total = 0

    for cat in cats:
        print(f"[rimi-req] category: {cat}")
        products = crawl_category(cat, req_delay=req_delay, session=session)

        for p in products:
            p["store_chain"] = "Rimi"
            p["store_name"] = "Rimi ePood"
            p["store_channel"] = "online"
            p["manufacturer"] = ""

        all_rows.extend(products)
        total += len(products)
        print(f"[rimi-req] category done: {len(products)} products (total so far: {total})")

        if max_products and total >= max_products:
            print(f"[rimi-req] max_products={max_products} reached, stopping.")
            break

    # deduplicate by ext_id (same product may appear in multiple categories)
    seen = set()
    deduped = []
    for r in all_rows:
        if r["ext_id"] not in seen:
            seen.add(r["ext_id"])
            deduped.append(r)

    print(f"[rimi-req] total unique products: {len(deduped)} (from {total} with dupes)")

    write_csv(deduped, args.output_csv)
    print(f"[rimi-req] wrote CSV: {args.output_csv}")

    try:
        store_id_env = int(os.environ.get("STORE_ID", "440") or "440")
    except Exception:
        store_id_env = 440

    asyncio.run(_bulk_ingest_to_db(deduped, store_id_env))
    print(f"[rimi-req] done. {len(deduped)} products.")


if __name__ == "__main__":
    main()

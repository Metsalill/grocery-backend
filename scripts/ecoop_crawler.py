#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop eCoop crawler — fast requests+BeautifulSoup version.

Scrapes category listing pages only (no PDP visits).
All data needed (name, price, EAN/SKU, image) is present on the listing page.
No Playwright needed — coophaapsalu.ee is server-side rendered WordPress/WooCommerce.

Usage:
  python3 ecoop_crawler.py \
    --store-url https://coophaapsalu.ee \
    --store-host coophaapsalu.ee \
    --store-id 445 \
    --categories-file data/coop_haapsalu_categories.txt \
    --cat-shards 8 \
    --cat-index 0 \
    --out out/ecoop_haapsalu_0.csv \
    --upsert-db main
"""

import argparse
import asyncio
import csv
import datetime as dt
import hashlib
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

try:
    import asyncpg
except ImportError:
    asyncpg = None

# ---------------------------------------------------------------------------
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DIGITS_ONLY = re.compile(r"[^0-9]")
SIZE_RE = re.compile(r"(\b\d+[\,\.]?\d*\s?(?:kg|g|l|ml|tk|pcs|x|×)\s?\d*\b)", re.IGNORECASE)
PRICE_RE = re.compile(r"(\d+[.,]\d{2})")

CSV_COLS = [
    "chain", "store_host", "channel", "ext_id", "ean_raw", "ean_norm",
    "name", "size_text", "brand", "manufacturer", "price", "currency",
    "image_url", "url",
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

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
        if len(d) == 12:
            d = "0" + d
        return d
    return None


def map_store_id(store_host: str) -> int:
    host = (store_host or "").lower()
    if "haapsalu" in host:
        return 445
    if "vandra" in host:
        return 446
    return 0


def _stable_bucket(s: str, buckets: int, salt: str = "") -> int:
    h = hashlib.sha1((salt + "|" + s).encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % max(1, buckets)


def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
    })
    return s


# ---------------------------------------------------------------------------
# scraping
# ---------------------------------------------------------------------------

def fetch_page(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
        else:
            print(f"  [warn] HTTP {r.status_code} for {url}")
            return None
    except Exception as e:
        print(f"  [warn] fetch failed for {url}: {e}")
        return None


def parse_price_text(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.replace("\xa0", " ").replace(",", ".")
    m = re.search(r"(\d+\.\d{2})", text)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    # integer price like "3 €"
    m2 = re.search(r"(\d+)\s*€", text)
    if m2:
        try:
            return float(m2.group(1))
        except Exception:
            pass
    return None


def scrape_product_cards(soup: BeautifulSoup, base_url: str, store_host: str) -> List[Dict]:
    """Extract all product cards from a category listing page."""
    products = []

    # WooCommerce product cards — various selectors
    cards = (
        soup.select("li.product")
        or soup.select("div.product")
        or soup.select("[data-testid='product-card']")
        or soup.select(".wc-block-grid__product")
        or soup.select("article.product")
    )

    for card in cards:
        # Name
        name = ""
        name_el = (
            card.select_one(".woocommerce-loop-product__title")
            or card.select_one("h2.product-name")
            or card.select_one("h2")
            or card.select_one(".product-title")
            or card.select_one("[data-testid='product-name']")
        )
        if name_el:
            name = name_el.get_text(strip=True)
        if not name:
            continue

        # URL
        url = ""
        link_el = card.select_one("a.woocommerce-loop-product__link") or card.select_one("a[href*='/toode/']") or card.select_one("a")
        if link_el:
            url = urljoin(base_url, link_el.get("href", ""))

        # Price — WooCommerce uses .price .amount bdi
        price = None
        price_el = (
            card.select_one(".price ins .amount bdi")  # sale price
            or card.select_one(".price .amount bdi")
            or card.select_one(".price .amount")
            or card.select_one(".price")
            or card.select_one("[data-testid='product-price']")
        )
        if price_el:
            price = parse_price_text(price_el.get_text())

        # Image
        image_url = ""
        img_el = card.select_one("img")
        if img_el:
            image_url = img_el.get("src") or img_el.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url

        # EAN/SKU from data attributes or product link
        ean_raw = None
        sku = None

        # WooCommerce often puts product id in class like "post-XXXX"
        for cls in (card.get("class") or []):
            m = re.match(r"post-(\d+)", cls)
            if m:
                sku = m.group(1)
                break

        # Try data-product_id or data-product-id
        data_id = card.get("data-product_id") or card.get("data-product-id") or card.get("data-id")
        if data_id:
            sku = str(data_id)

        # Try to extract EAN-like number from product URL slug
        if url:
            slug = url.rstrip("/").split("/")[-1]
            # Look for a sequence of 8-13 digits in the slug
            digits_in_slug = re.findall(r"\d{8,14}", slug)
            if digits_in_slug:
                ean_raw = digits_in_slug[0]

        # ext_id: prefer EAN norm, then SKU, then slug
        ean_norm = normalize_ean(ean_raw)
        ext_id = ean_norm or sku or (url.rstrip("/").split("/")[-1] if url else "")

        # size_text from name
        size_text = None
        if name:
            m = SIZE_RE.search(name)
            if m:
                size_text = m.group(1)

        if not ext_id:
            continue

        products.append({
            "chain": "Coop",
            "store_host": store_host,
            "channel": "online",
            "ext_id": ext_id,
            "ean_raw": ean_raw or "",
            "ean_norm": ean_norm or "",
            "name": name,
            "size_text": size_text or "",
            "brand": "",
            "manufacturer": "",
            "price": f"{price:.2f}" if price is not None else "",
            "currency": "EUR",
            "image_url": image_url,
            "url": url,
        })

    return products


def get_next_page_url(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """Find the next page URL from pagination."""
    # WooCommerce standard pagination
    next_el = (
        soup.select_one("a.next.page-numbers")
        or soup.select_one('a[rel="next"]')
        or soup.select_one(".woocommerce-pagination a.next")
        or soup.select_one("nav.woocommerce-pagination a.next")
    )
    if next_el:
        href = next_el.get("href", "")
        if href:
            return urljoin(current_url, href)

    # ?page= style pagination
    current_page = 1
    m = re.search(r"[?&]page=(\d+)", current_url)
    if m:
        current_page = int(m.group(1))

    # Check if there's a "next" indicator in page numbers
    page_nums = soup.select(".page-numbers")
    for el in page_nums:
        if "current" in (el.get("class") or []):
            try:
                curr = int(el.get_text(strip=True))
                # Look for curr+1
                for el2 in page_nums:
                    try:
                        if int(el2.get_text(strip=True)) == curr + 1:
                            href = el2.get("href", "")
                            if href:
                                return urljoin(current_url, href)
                    except Exception:
                        pass
            except Exception:
                pass

    return None


def scrape_category(
    session: requests.Session,
    category_url: str,
    store_host: str,
    req_delay: float = 0.3,
) -> List[Dict]:
    """Scrape all products from a category, following pagination."""
    all_products = []
    url = category_url
    page_num = 0

    while url:
        page_num += 1
        print(f"  [page {page_num}] {url}")

        soup = fetch_page(session, url)
        if not soup:
            break

        products = scrape_product_cards(soup, category_url, store_host)
        all_products.extend(products)
        print(f"  -> {len(products)} products (total so far: {len(all_products)})")

        next_url = get_next_page_url(soup, url)
        if next_url and next_url != url:
            url = next_url
            time.sleep(req_delay)
        else:
            break

    return all_products


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def _ensure_csv_header(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not out_path.exists() or out_path.stat().st_size == 0:
        with out_path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_COLS, lineterminator="\n").writeheader()


def append_csv(rows: List[Dict], out_path: Path) -> None:
    if not rows:
        return
    write_header = not out_path.exists() or out_path.stat().st_size == 0
    with out_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, lineterminator="\n")
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CSV_COLS})


# ---------------------------------------------------------------------------
# DB ingest
# ---------------------------------------------------------------------------

async def _bulk_ingest_to_db(rows: List[Tuple], store_id: int) -> None:
    if store_id <= 0:
        print("[ecoop] STORE_ID not set, skipping DB ingest.")
        return
    if not rows:
        print("[ecoop] No rows to ingest.")
        return
    if not asyncpg:
        print("[ecoop] asyncpg not installed, skipping DB ingest.")
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("[ecoop] DATABASE_URL not set, skipping DB ingest.")
        return

    sql = """
        SELECT upsert_product_and_price(
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
        );
    """

    try:
        conn = await asyncpg.connect(dsn)
    except Exception as e:
        print(f"[ecoop] DB connect failed: {e}")
        return

    sent = 0
    errors = 0
    try:
        for row in rows:
            try:
                await conn.fetchval(sql, *row)
                sent += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"[ecoop] upsert failed: {e}")
        print(f"[ecoop] upserted {sent} rows (errors: {errors})")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main(args):
    # Load categories
    categories: List[str] = []
    if args.categories_file and Path(args.categories_file).exists():
        categories = [
            ln.strip()
            for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
    if not categories:
        print("[error] No categories found.")
        sys.exit(2)

    # Store config
    store_host = args.store_host or urlparse(args.store_url or "https://coophaapsalu.ee").netloc
    try:
        store_id = int(args.store_id) if args.store_id else 0
    except Exception:
        store_id = 0
    if not store_id:
        store_id = int(os.environ.get("STORE_ID", "0") or "0")
    if not store_id:
        store_id = map_store_id(store_host)
    print(f"[ecoop] store_host={store_host} store_id={store_id}")

    # Sharding
    cat_shards = max(1, args.cat_shards)
    cat_index = args.cat_index
    if cat_shards > 1:
        categories = [u for i, u in enumerate(categories) if i % cat_shards == cat_index]
        print(f"[shard] {len(categories)} categories for shard {cat_index}/{cat_shards}")

    # Output
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_csv_header(out_path)
    print(f"[out] {out_path}")

    session = _make_session()
    all_rows: List[Dict] = []
    ts_now = dt.datetime.now(dt.timezone.utc)

    for i, cat_url in enumerate(categories, 1):
        print(f"[cat] {i}/{len(categories)} {cat_url}")
        products = scrape_category(session, cat_url, store_host, req_delay=args.req_delay)
        if products:
            append_csv(products, out_path)
            all_rows.extend(products)
        print(f"[cat] done: {len(products)} products")
        time.sleep(args.req_delay)

    print(f"[done] total {len(all_rows)} rows → {out_path}")

    # DB ingest
    rows_for_db: List[Tuple] = []
    for r in all_rows:
        price_val = r.get("price")
        try:
            price_float = float(price_val) if price_val else None
        except Exception:
            price_float = None
        if price_float is None:
            continue

        rows_for_db.append((
            "coop",
            r.get("ext_id") or "",
            r.get("name") or "",
            r.get("brand") or "",
            r.get("size_text") or "",
            r.get("ean_raw") or "",
            price_float,
            "EUR",
            store_id,
            ts_now,
            r.get("url") or "",
        ))

    await _bulk_ingest_to_db(rows_for_db, store_id)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--store-url", default="https://coophaapsalu.ee")
    p.add_argument("--store-host", default="coophaapsalu.ee")
    p.add_argument("--store-id", default=0, type=int)
    p.add_argument("--categories-file", default="")
    p.add_argument("--cat-shards", type=int, default=1)
    p.add_argument("--cat-index", type=int, default=0)
    p.add_argument("--out", default="out/coop_ecoop.csv")
    p.add_argument("--req-delay", type=float, default=0.3)
    p.add_argument("--upsert-db", default="main")
    # legacy flags kept for YML compatibility — ignored
    p.add_argument("--page-limit", type=int, default=0)
    p.add_argument("--max-products", type=int, default=0)
    p.add_argument("--headless", default="1")
    p.add_argument("--pdp-workers", type=int, default=2)
    p.add_argument("--goto-strategy", default="auto")
    p.add_argument("--nav-timeout", type=int, default=45000)
    p.add_argument("--write-empty-csv", action="store_true", default=True)
    p.add_argument("--rotate-buckets", type=int, default=1)
    p.add_argument("--rotate-index", type=int, default=-1)
    p.add_argument("--rotate-salt", default="")
    p.add_argument("--categories-multiline", default="")
    p.add_argument("--region", default="")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop eCoop crawler — fast requests+BeautifulSoup version.

Scrapes category listing pages only (no PDP visits).
Handles subcategory containers recursively (up to depth 3).
No Playwright needed — coophaapsalu.ee is server-side rendered WooCommerce.
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
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

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
    m2 = re.search(r"(\d+)\s*€", text)
    if m2:
        try:
            return float(m2.group(1))
        except Exception:
            pass
    return None


def get_subcategory_links(soup: BeautifulSoup, current_url: str, base_host: str) -> List[str]:
    """Find subcategory links on a page that shows category tiles instead of products."""
    links = []
    seen = set()

    # WooCommerce subcategory tiles
    for el in soup.select("a[href*='/tootekategooria/']"):
        href = el.get("href", "")
        if not href:
            continue
        abs_url = urljoin(current_url, href)
        # Must be same host and a deeper category path
        parsed = urlparse(abs_url)
        if parsed.netloc.lower() != base_host.lower():
            continue
        # Must be different from current URL
        abs_clean = abs_url.rstrip("/")
        curr_clean = current_url.rstrip("/")
        if abs_clean == curr_clean:
            continue
        # Must be a subcategory (longer path than current)
        curr_path = urlparse(current_url).path.rstrip("/")
        if not parsed.path.rstrip("/").startswith(curr_path + "/"):
            continue
        if abs_clean not in seen:
            seen.add(abs_clean)
            links.append(abs_url)

    return links


def scrape_product_cards(soup: BeautifulSoup, base_url: str, store_host: str) -> List[Dict]:
    """Extract all product cards from a category listing page."""
    products = []

    cards = (
        soup.select("li.product")
        or soup.select("div.product")
        or soup.select("[data-testid='product-card']")
        or soup.select(".wc-block-grid__product")
        or soup.select("article.product")
    )

    for card in cards:
        # Skip subcategory tiles (they have class 'product-category')
        card_classes = card.get("class") or []
        if "product-category" in card_classes:
            continue

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
        link_el = (
            card.select_one("a.woocommerce-loop-product__link")
            or card.select_one("a[href*='/toode/']")
            or card.select_one("a")
        )
        if link_el:
            url = urljoin(base_url, link_el.get("href", ""))

        # Price — target WooCommerce structure specifically to avoid unit prices
        price = None
        # First try sale price (ins .amount bdi)
        sale_el = card.select_one(".price ins .amount bdi")
        if sale_el:
            price = parse_price_text(sale_el.get_text())
        # Then regular price
        if price is None:
            regular_el = card.select_one(".price .woocommerce-Price-amount.amount bdi")
            if regular_el:
                price = parse_price_text(regular_el.get_text())
        # Fallback to first .amount
        if price is None:
            amount_el = card.select_one(".price .amount")
            if amount_el:
                price = parse_price_text(amount_el.get_text())

        # Image
        image_url = ""
        img_el = card.select_one("img")
        if img_el:
            image_url = img_el.get("src") or img_el.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url

        # EAN/SKU from data attributes
        ean_raw = None
        sku = None

        # WooCommerce post ID from class
        for cls in card_classes:
            m = re.match(r"post-(\d+)", cls)
            if m:
                sku = m.group(1)
                break

        data_id = card.get("data-product_id") or card.get("data-product-id") or card.get("data-id")
        if data_id:
            sku = str(data_id)

        # Try EAN from image filename — coophaapsalu.ee uses EAN as image filename
        # e.g. https://coophaapsalu.ee/wp-content/uploads/2025/03/8711327667020.png
        if image_url:
            img_filename = image_url.rstrip("/").split("/")[-1]
            img_stem = re.sub(r"\.[^.]+$", "", img_filename)  # remove extension
            digits = re.sub(r"[^0-9]", "", img_stem)
            if len(digits) in (8, 12, 13, 14):
                ean_raw = digits

        # Fallback: try EAN from URL slug
        if not ean_raw and url:
            slug = url.rstrip("/").split("/")[-1]
            digits_in_slug = re.findall(r"\d{8,14}", slug)
            if digits_in_slug:
                ean_raw = digits_in_slug[0]

        ean_norm = normalize_ean(ean_raw)
        ext_id = ean_norm or sku or (url.rstrip("/").split("/")[-1] if url else "")

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
    return None


def scrape_category(
    session: requests.Session,
    category_url: str,
    store_host: str,
    req_delay: float = 0.3,
    visited: Optional[Set[str]] = None,
    depth: int = 0,
    max_depth: int = 3,
) -> List[Dict]:
    """
    Scrape all products from a category, following pagination.
    If the category page shows subcategory tiles instead of products,
    recurse into each subcategory (up to max_depth).
    """
    if visited is None:
        visited = set()

    clean_url = category_url.rstrip("/")
    if clean_url in visited:
        return []
    visited.add(clean_url)

    if depth > max_depth:
        print(f"  [warn] max depth {max_depth} reached at {category_url}")
        return []

    all_products = []
    url = category_url
    page_num = 0
    base_host = urlparse(category_url).netloc

    while url:
        page_num += 1
        print(f"  [page {page_num}] {url}")

        soup = fetch_page(session, url)
        if not soup:
            break

        products = scrape_product_cards(soup, category_url, store_host)

        if products:
            all_products.extend(products)
            print(f"  -> {len(products)} products (total so far: {len(all_products)})")

            # Follow pagination
            next_url = get_next_page_url(soup, url)
            if next_url and next_url.rstrip("/") != url.rstrip("/"):
                url = next_url
                time.sleep(req_delay)
            else:
                break

        else:
            # No products on this page — check for subcategory tiles
            subcats = get_subcategory_links(soup, url, base_host)

            if subcats:
                print(f"  -> no products, found {len(subcats)} subcategories (depth={depth})")
                for subcat_url in subcats:
                    time.sleep(req_delay)
                    sub_products = scrape_category(
                        session, subcat_url, store_host,
                        req_delay=req_delay,
                        visited=visited,
                        depth=depth + 1,
                        max_depth=max_depth,
                    )
                    all_products.extend(sub_products)
            else:
                print(f"  -> no products and no subcategories found")
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
    visited: Set[str] = set()
    ts_now = dt.datetime.now(dt.timezone.utc)

    for i, cat_url in enumerate(categories, 1):
        print(f"[cat] {i}/{len(categories)} {cat_url}")
        products = scrape_category(
            session, cat_url, store_host,
            req_delay=args.req_delay,
            visited=visited,
        )
        if products:
            append_csv(products, out_path)
            all_rows.extend(products)
        print(f"[cat] done: {len(products)} products")
        time.sleep(args.req_delay)

    print(f"[done] total {len(all_rows)} rows → {out_path}")

    # DB ingest — only rows with a price
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

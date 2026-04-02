#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma scraper v2 — requests + BeautifulSoup, no Playwright.

Key insight: EAN is in the product URL slug, price is in the category
listing page HTML (SSR). We never need to visit individual PDPs.

URL pattern: /toode/{slug}/{EAN}
Category listing: data-test-id="product-card" cards contain name + price.

Strategy:
  1. For each category page, fetch HTML with requests
  2. Parse all product cards → name, price, EAN (from href), size
  3. Upsert via upsert_product_and_price()
  4. Paginate via ?page=N until no more products

Run: python prisma_scraper_v2.py [--store-id 14] [--delay 0.5] [--shard N] [--shards M]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse, urlencode, urlunparse, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
BASE = "https://prismamarket.ee"

CATEGORIES = [
    "/tooted/food-market",
    "/tooted/puu-ja-koogiviljad",
    "/tooted/leivad-kupsised-ja-kupsetised",
    "/tooted/liha-ja-taimsed-valgud",
    "/tooted/kala-ja-mereannid",
    "/tooted/piim-munad-ja-rasvad",
    "/tooted/juustud",
    "/tooted/valmistoit",
    "/tooted/olid-vurtsid-maitseained",
    "/tooted/kuivtooted-ja-kupsetamine",
    "/tooted/joogid",
    "/tooted/kulmutatud-toidud",
    "/tooted/maiustused-ja-suupisted",
    "/tooted/kosmeetika-ja-hugieen/juuksed-ja-juuksehooldus",
    "/tooted/kosmeetika-ja-hugieen/naohooldus",
    "/tooted/kosmeetika-ja-hugieen/nahahooldus",
    "/tooted/kosmeetika-ja-hugieen/intiimhugieen-ja-intiimtooted",
    "/tooted/kosmeetika-ja-hugieen/suuhooldus",
    "/tooted/kosmeetika-ja-hugieen/seebid-ja-pesuvahendid",
    "/tooted/loodustooted-ja-toidulisandid",
    "/tooted/kodu-ja-majapidamistarbed",
    "/tooted/lapsed/emapiimaasendajad",
    "/tooted/lapsed/pudrud-ja-pureesupid",
    "/tooted/lapsed/lastetoidud",
    "/tooted/lapsed/laste-pureed-ja-muud-vahepalad",
    "/tooted/lapsed/mahkmed-ja-lapsehooldus",
    "/tooted/lapsed/puhastamine-ja-hugieen",
    "/tooted/lapsed/laste-vahepalad",
    "/tooted/lapsed/beebi-ja-lapsehooldusvahendid",
    "/tooted/lemmikloomad/koeratoit",
    "/tooted/lemmikloomad/kassitoit",
    "/tooted/lemmikloomad/muud-lemmikloomade-tarvikud",
    "/tooted/lemmikloomad/kassiliiv",
    "/tooted/kodu-ja-vaba-aeg/pesupesemine",
    "/tooted/kodu-ja-vaba-aeg/tualettpaber",
    "/tooted/kodu-ja-vaba-aeg/kodupuhastusvahendid",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PRICE_RE = re.compile(r"(\d+[.,]\d+)")
PACK_RE  = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIZE_RE  = re.compile(r"\b(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
EAN_RE   = re.compile(r"/(\d{8,14})(?:[/?#]|$)")

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ---------------------------------------------------------------------------
def get_db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return url


def fetch_html(url: str, retries: int = 3, delay: float = 1.0) -> Optional[str]:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
            print(f"[warn] HTTP {r.status_code} for {url}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] fetch error ({attempt+1}/{retries}): {e}", file=sys.stderr)
        if attempt < retries - 1:
            time.sleep(delay * (attempt + 1))
    return None


def parse_ean_from_url(href: str) -> Optional[str]:
    m = EAN_RE.search(href)
    if m:
        return m.group(1)
    return None


def parse_size_from_name(name: str) -> str:
    m = PACK_RE.search(name)
    if m:
        qty, num, unit = m.groups()
        return f"{qty}x{num.replace(',', '.')} {unit.lower()}"
    m = SIZE_RE.search(name)
    if m:
        num, unit = m.groups()
        return f"{num.replace(',', '.')} {unit.lower()}"
    return ""


def parse_price(txt: str) -> Optional[float]:
    # Remove "umbes" and other text, find first price number
    txt = txt.replace("\xa0", " ").replace("umbes", "").strip()
    m = PRICE_RE.search(txt)
    if m:
        val = float(m.group(1).replace(",", "."))
        if val > 0:
            return val
    return None


def parse_category_page(html: str, cat_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    products = []

    cards = soup.find_all(attrs={"data-test-id": "product-card"})
    if not cards:
        # Try alternative selector
        cards = soup.find_all("article", attrs={"data-test-id": True})

    for card in cards:
        try:
            # Product link — contains slug/EAN
            link = card.find("a", class_=lambda c: c and "product-link" in c)
            if not link:
                link = card.find("a", href=re.compile(r"/toode/"))
            if not link:
                continue

            href = link.get("href", "")
            full_url = urljoin(BASE, href)
            ean = parse_ean_from_url(full_url)
            if not ean:
                continue

            # ext_id = last slug segment before EAN or EAN itself
            path_parts = [p for p in urlparse(full_url).path.split("/") if p]
            ext_id = ean  # EAN as ext_id — unique and stable

            # Product name
            name_el = card.find(attrs={"data-test-id": "product-card_productName"})
            if not name_el:
                name_el = card.find(class_=lambda c: c and "product-card_productname" in c.lower() if c else False)
            if not name_el:
                name_el = card.find("span", attrs={"data-test-id": lambda x: x and "productname" in x.lower() if x else False})
            if not name_el:
                # Try link text
                name_el = link
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                continue

            # Price — look for display-price element
            price_el = card.find(attrs={"data-test-id": "display-price"})
            if not price_el:
                price_el = card.find(attrs={"data-test-id": lambda x: x and "price" in x.lower() if x else False})
            price = None
            if price_el:
                price = parse_price(price_el.get_text())
            if not price:
                # Try any text with € in card
                for el in card.find_all(string=re.compile(r"\d+[.,]\d+\s*€")):
                    price = parse_price(el)
                    if price:
                        break

            if not price:
                continue

            size_text = parse_size_from_name(name)

            products.append({
                "ext_id": ext_id,
                "ean": ean,
                "name": name,
                "size_text": size_text,
                "price": price,
                "source_url": full_url,
            })

        except Exception as e:
            print(f"[warn] card parse error: {e}", file=sys.stderr)
            continue

    return products


def get_total_pages(html: str) -> int:
    """Extract total page count from pagination."""
    soup = BeautifulSoup(html, "lxml")
    # Look for "Leht 1 / 45" pattern
    m = re.search(r"Leht\s*\d+\s*/\s*(\d+)", soup.get_text())
    if m:
        return int(m.group(1))
    # Try pagination links
    pages = soup.find_all("a", attrs={"aria-label": re.compile(r"page|leht", re.I)})
    nums = []
    for p in pages:
        try:
            nums.append(int(p.get_text(strip=True)))
        except Exception:
            pass
    return max(nums) if nums else 1


def scrape_category(cat_path: str, delay: float = 0.5) -> list[dict]:
    all_products = []
    seen_eans: set[str] = set()

    base_url = BASE + cat_path
    page_num = 1

    # Get first page to find total pages
    first_url = f"{base_url}?page=1"
    html = fetch_html(first_url)
    if not html:
        print(f"[skip] {cat_path} — failed to fetch", file=sys.stderr)
        return []

    total_pages = get_total_pages(html)
    print(f"[cat] {cat_path} — {total_pages} pages", file=sys.stderr)

    while page_num <= total_pages:
        url = f"{base_url}?page={page_num}"
        if page_num > 1:
            html = fetch_html(url)
            if not html:
                break

        products = parse_category_page(html, url)
        new_products = [p for p in products if p["ean"] not in seen_eans]
        for p in new_products:
            seen_eans.add(p["ean"])
        all_products.extend(new_products)

        print(
            f"[page] {cat_path} p{page_num}/{total_pages} "
            f"→ {len(new_products)} new (total: {len(all_products)})",
            file=sys.stderr
        )

        if not products or len(new_products) == 0:
            break

        page_num += 1
        time.sleep(delay)

    return all_products


def upsert_batch(cur, rows: list[dict], store_id: int) -> tuple[int, int]:
    ok = 0
    errors = 0
    ts_now = datetime.datetime.now(datetime.timezone.utc)

    sql = """
        SELECT upsert_product_and_price(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    for row in rows:
        try:
            cur.execute(sql, (
                "prisma",
                row["ext_id"],
                row["name"],
                "",           # brand — inferred by DB or left empty
                row["size_text"],
                row["ean"],
                row["price"],
                "EUR",
                store_id,
                ts_now,
                row["source_url"],
            ))
            ok += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"[warn] upsert failed {row['ext_id']}: {e}", file=sys.stderr)

    return ok, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store-id", type=int, default=int(os.getenv("STORE_ID", "14")))
    ap.add_argument("--delay", type=float, default=float(os.getenv("REQ_DELAY", "0.5")))
    ap.add_argument("--shard", type=int, default=int(os.getenv("SHARD", "0")))
    ap.add_argument("--shards", type=int, default=int(os.getenv("SHARDS", "1")))
    args = ap.parse_args()

    # Shard categories
    my_cats = [c for i, c in enumerate(CATEGORIES) if i % args.shards == args.shard]
    print(
        f"[info] shard {args.shard}/{args.shards} — "
        f"{len(my_cats)}/{len(CATEGORIES)} categories, "
        f"store_id={args.store_id}, delay={args.delay}s",
        file=sys.stderr
    )

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    total_ok = 0
    total_errors = 0

    for cat in my_cats:
        products = scrape_category(cat, delay=args.delay)
        if not products:
            continue
        ok, errors = upsert_batch(cur, products, args.store_id)
        total_ok += ok
        total_errors += errors
        print(
            f"[done] {cat} → upserted {ok}, errors {errors}",
            file=sys.stderr
        )

    cur.close()
    conn.close()
    print(
        f"[TOTAL] upserted {total_ok} rows, errors {total_errors}",
        file=sys.stderr
    )


if __name__ == "__main__":
    main()

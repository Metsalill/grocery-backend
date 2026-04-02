#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver scraper v2 — requests + BeautifulSoup, no Playwright.

Key insight: EAN is embedded in the product image URL on category listing pages.
Image src pattern: .../resize/2/7/2710831000008.jpg -> EAN = 2710831000008
Price and name are also in the SSR HTML of category pages.
Pagination: ?page=N

Strategy:
  1. For each category page fetch HTML with requests
  2. Parse product cards -> name, price, EAN (from img src), slug (ext_id)
  3. Paginate via ?page=N
  4. Upsert via upsert_product_and_price()

Run: python selver_scraper_v2.py [--store-id 31] [--delay 0.5] [--shard N] [--shards M]
"""

from __future__ import annotations

import argparse
import datetime
import os
import re
import sys
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import psycopg2

# ---------------------------------------------------------------------------
BASE = "https://www.selver.ee"

CATEGORIES = [
    "/puu-ja-koogiviljad/ounad-pirnid",
    "/puu-ja-koogiviljad/troopilised-eksootilised-viljad",
    "/puu-ja-koogiviljad/koogiviljad-juurviljad",
    "/puu-ja-koogiviljad/seened",
    "/puu-ja-koogiviljad/maitsetaimed-varsked-saltid-piprad",
    "/puu-ja-koogiviljad/puuviljasalatid",
    "/puu-ja-koogiviljad/marjad",
    "/puu-ja-koogiviljad/smuutid-varsked-mahlad",
    "/liha-ja-kalatooted/sealiha",
    "/liha-ja-kalatooted/linnuliha",
    "/liha-ja-kalatooted/veise-lamba-ja-ulukiliha",
    "/liha-ja-kalatooted/hakkliha",
    "/liha-ja-kalatooted/keedu-ja-suitsuvorstid-viinerid",
    "/liha-ja-kalatooted/singid-rulaadid",
    "/liha-ja-kalatooted/muud-lihatooted",
    "/liha-ja-kalatooted/grillvorstid-verivorstid",
    "/liha-ja-kalatooted/gurmee-lihatooted",
    "/liha-ja-kalatooted/varske-kala-mereannid",
    "/liha-ja-kalatooted/soolatud-ja-suitsutatud-kalatooted",
    "/liha-ja-kalatooted/toodeldud-mereannid",
    "/liha-ja-kalatooted/muud-kalatooted",
    "/piimatooted-munad-void/piimad-koored",
    "/piimatooted-munad-void/kohupiimad-kodujuustud",
    "/piimatooted-munad-void/jogurtid-jogurtijoogid",
    "/piimatooted-munad-void/kohukesed",
    "/piimatooted-munad-void/muud-magustoidud",
    "/piimatooted-munad-void/munad",
    "/piimatooted-munad-void/void-margariinid",
    "/juustud/juustud",
    "/juustud/maardejuustud",
    "/juustud/delikatessjuustud",
    "/leivad-saiad-kondiitritooted/leivad",
    "/leivad-saiad-kondiitritooted/saiad",
    "/leivad-saiad-kondiitritooted/sepikud-kuklid-lavassid",
    "/leivad-saiad-kondiitritooted/nakileivad",
    "/leivad-saiad-kondiitritooted/selveri-pagarid",
    "/leivad-saiad-kondiitritooted/tordid",
    "/leivad-saiad-kondiitritooted/koogid-rullbiskviidid",
    "/leivad-saiad-kondiitritooted/saiakesed-stritslid-kringlid",
    "/valmistoidud/salatid",
    "/valmistoidud/jahutatud-valmistoidud",
    "/valmistoidud/magustoidud",
    "/valmistoidud/sushi",
    "/suurpakendid/puu-ja-koogiviljad",
    "/suurpakendid/piimatooted",
    "/suurpakendid/lihatooted",
    "/suurpakendid/jahutatud-valmistoit",
    "/suurpakendid/salatid",
    "/suurpakendid/kuivained",
    "/suurpakendid/maitseained",
    "/suurpakendid/hoidised",
    "/suurpakendid/kastmed-ja-olid",
    "/suurpakendid/joogid",
    "/kuivained-hoidised/kuivained-hommikusoogid",
    "/kuivained-hoidised/hoidised",
    "/kuivained-hoidised/kohv-tee-kakao",
    "/maitseained-ja-puljongid/maitseained",
    "/maitseained-ja-puljongid/maailma-kook",
    "/maitseained-ja-puljongid/puljongid",
    "/kastmed-olid/olid-aadikad",
    "/kastmed-olid/majoneesid-sinepid",
    "/kastmed-olid/ketsupid-tomatipastad-kastmed",
    "/kastmed-olid/gurmee-kastmed",
    "/maiustused-kupsised-naksid/kommipakid",
    "/maiustused-kupsised-naksid/kommikarbid",
    "/maiustused-kupsised-naksid/sokolaadid",
    "/maiustused-kupsised-naksid/natsud-pastillid",
    "/maiustused-kupsised-naksid/muud-maiustused",
    "/maiustused-kupsised-naksid/kupsised",
    "/maiustused-kupsised-naksid/nakileivad",
    "/maiustused-kupsised-naksid/pahklid-ja-kuivatatud-puuviljad",
    "/maiustused-kupsised-naksid/sipsid",
    "/kulmutatud-toidukaubad/kulmutatud-liha-ja-kalatooted",
    "/kulmutatud-toidukaubad/kulmutatud-valmistooted",
    "/kulmutatud-toidukaubad/kulmutatud-koogiviljad-marjad-puuviljad",
    "/kulmutatud-toidukaubad/kulmutatud-taignad-ja-kondiitritooted",
    "/kulmutatud-toidukaubad/jaatised",
    "/joogid/veed-mahlad-siirupid-smuutid",
    "/joogid/karastus-ja-energiajoogid-toonikud",
    "/joogid/spordijoogid-pulbrid-batoonid",
    "/joogid/kohv-tee-kakao",
    "/joogid/lahja-alkohol",
    "/joogid/kange-alkohol",
    "/lastekaubad/lastetoidud",
    "/lastekaubad/mahkmed",
    "/lastekaubad/beebi-hooldusvahendid",
    "/lemmiklooma-kaubad/kassitoidud",
    "/lemmiklooma-kaubad/koeratoidud",
    "/lemmiklooma-kaubad/lemmikloomatarbed",
    "/enesehooldustarbed/suuhooldus",
    "/enesehooldustarbed/naohooldus",
    "/enesehooldustarbed/juuksehooldus",
    "/enesehooldustarbed/kehahooldus",
    "/majapidamis-ja-kodukaubad/paberitooted",
    "/majapidamis-ja-kodukaubad/puhastus-ja-koristusvahendid",
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

# EAN from image URL: .../resize/X/Y/1234567890123.jpg
EAN_FROM_IMG_RE = re.compile(r"/resize/\d+/\d+/(\d{8,14})\.(?:jpg|png|webp|avif)", re.I)
# Also try SKU-style T-codes from image filename (Selveri own products)
TCODE_FROM_IMG_RE = re.compile(r"/(\d{8,14})\.", re.I)

PRICE_RE = re.compile(r"(\d+[.,]\d+)")
PACK_RE  = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIZE_RE  = re.compile(r"\b(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

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


def extract_ean_from_img(src: str) -> Optional[str]:
    """Extract EAN from Selver CDN image URL."""
    m = EAN_FROM_IMG_RE.search(src)
    if m:
        candidate = m.group(1)
        # Filter out T-codes (Selver internal, start with many zeros)
        if len(candidate) >= 8:
            return candidate
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
    txt = txt.replace("\xa0", " ").strip()
    m = PRICE_RE.search(txt)
    if m:
        val = float(m.group(1).replace(",", "."))
        if 0 < val < 10000:
            return val
    return None


def get_total_pages(soup: BeautifulSoup) -> int:
    """Extract last page number from Selver pagination."""
    # Selver shows numbered page links
    page_links = soup.find_all("a", attrs={"data-testid": "productlink"})

    # Try pagination — look for numbered buttons
    pagination = soup.find(class_=lambda c: c and "pagination" in c.lower() if c else False)
    if pagination:
        nums = []
        for a in pagination.find_all("a"):
            try:
                nums.append(int(a.get_text(strip=True)))
            except Exception:
                pass
        if nums:
            return max(nums)

    # Try finding page numbers in any nav
    for el in soup.find_all(["a", "button"]):
        txt = el.get_text(strip=True)
        try:
            n = int(txt)
            if 1 < n < 200:
                # could be a page number
                pass
        except Exception:
            pass

    # Fallback: look for pagination numbers in the HTML text
    text = soup.get_text()
    # Pattern like "1 2 3 ... 7" or just numbered links
    m = re.search(r'(?:leht|page)[^\d]*(\d+)[^\d]*(?:leht|page)', text, re.I)
    if m:
        return int(m.group(1))

    return 1


def parse_category_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    products = []

    # Find all product cards
    cards = soup.find_all(attrs={"data-testid": "productlink"})
    if not cards:
        # Try by class pattern
        cards = soup.find_all("a", class_=lambda c: c and "ProductCard__link" in c if c else False)
    if not cards:
        cards = soup.find_all("a", class_=lambda c: c and "product-link" in c.lower() if c else False)

    for card_link in cards:
        try:
            # Get the parent card container
            card = card_link.find_parent(class_=lambda c: c and "ProductCard" in c if c else False)
            if not card:
                card = card_link.parent

            href = card_link.get("href", "")
            if not href:
                continue
            full_url = urljoin(BASE, href)

            # ext_id = slug (last path segment)
            ext_id = urlparse(full_url).path.rstrip("/").split("/")[-1]
            if not ext_id:
                continue

            # EAN from product image
            ean = None
            img = card.find("img") if card else card_link.find("img")
            if img:
                for attr in ["src", "data-src", "srcset"]:
                    src = img.get(attr, "")
                    if src:
                        ean = extract_ean_from_img(src)
                        if ean:
                            break

            if not ean:
                continue

            # Product name
            name_el = card.find(attrs={"data-testid": "productName"}) if card else None
            if not name_el:
                name_el = card.find(class_=lambda c: c and "ProductName" in c if c else False) if card else None
            if not name_el:
                name_el = card_link
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                continue

            # Price
            price_el = card.find(class_=lambda c: c and "ProductPrice" in c if c else False) if card else None
            price = None
            if price_el:
                price = parse_price(price_el.get_text())
            if not price:
                # Search for € in card text
                card_text = card.get_text() if card else ""
                for m in PRICE_RE.finditer(card_text.replace("\xa0", " ")):
                    val = float(m.group(1).replace(",", "."))
                    if 0 < val < 10000:
                        price = val
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


def find_last_page(html: str) -> int:
    """Find the last page number from pagination."""
    soup = BeautifulSoup(html, "lxml")
    max_page = 1

    # Look for all links/buttons that are just numbers
    for el in soup.find_all(["a", "button", "li"]):
        txt = el.get_text(strip=True)
        try:
            n = int(txt)
            if 1 < n <= 500:
                max_page = max(max_page, n)
        except Exception:
            pass

    return max_page


def scrape_category(cat_path: str, delay: float = 0.5) -> list[dict]:
    all_products = []
    seen_eans: set[str] = set()

    base_url = BASE + cat_path
    page_num = 1

    # Fetch first page
    first_url = f"{base_url}?page=1"
    html = fetch_html(first_url)
    if not html:
        print(f"[skip] {cat_path} — failed to fetch", file=sys.stderr)
        return []

    total_pages = find_last_page(html)
    print(f"[cat] {cat_path} — {total_pages} pages", file=sys.stderr)

    while page_num <= total_pages:
        if page_num > 1:
            url = f"{base_url}?page={page_num}"
            html = fetch_html(url)
            if not html:
                break

        products = parse_category_page(html)
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
                "selver",
                row["ext_id"],
                row["name"],
                "",
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
    ap.add_argument("--store-id", type=int, default=int(os.getenv("STORE_ID", "31")))
    ap.add_argument("--delay", type=float, default=float(os.getenv("REQ_DELAY", "0.5")))
    ap.add_argument("--shard", type=int, default=int(os.getenv("SHARD", "0")))
    ap.add_argument("--shards", type=int, default=int(os.getenv("SHARDS", "1")))
    args = ap.parse_args()

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
            print(f"[warn] {cat} → 0 products parsed", file=sys.stderr)
            continue
        ok, errors = upsert_batch(cur, products, args.store_id)
        total_ok += ok
        total_errors += errors
        print(f"[done] {cat} → upserted {ok}, errors {errors}", file=sys.stderr)

    cur.close()
    conn.close()
    print(f"[TOTAL] upserted {total_ok} rows, errors {total_errors}", file=sys.stderr)


if __name__ == "__main__":
    main()

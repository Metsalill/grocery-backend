#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma scraper v3 — requests + __NEXT_DATA__ Apollo cache, no Playwright.

Prisma uses Next.js. The page HTML contains a <script id="__NEXT_DATA__">
JSON blob with an Apollo GraphQL cache. Products are stored as:
  apolloState['Product:{"id":"EAN","storeId":"..."}'] = {ean, name, price, ...}

Price comes from ProductStoreEdge entries linked to each product.

Run: python prisma_food_scrape_to_db.py [--store-id 14] [--delay 0.5] [--shard N] [--shards M]
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
import time
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import psycopg2

# ---------------------------------------------------------------------------
BASE = "https://prismamarket.ee"

CATEGORIES = [
    # Food market
    "/tooted/food-market/liha",
    "/tooted/food-market/valmistoit",
    "/tooted/food-market/kalalett",
    # Puu- ja koogiviljad
    "/tooted/puu-ja-koogiviljad/puuviljad",
    "/tooted/puu-ja-koogiviljad/juurviljad",
    "/tooted/puu-ja-koogiviljad/koogiviljad",
    "/tooted/puu-ja-koogiviljad/marjad",
    "/tooted/puu-ja-koogiviljad/seened",
    # Leivad
    "/tooted/leivad-kupsised-ja-kupsetised/gluteenivaba",
    "/tooted/leivad-kupsised-ja-kupsetised/leivad",
    "/tooted/leivad-kupsised-ja-kupsetised/kupsetusleti-tooted",
    "/tooted/leivad-kupsised-ja-kupsetised/kupsetised",
    "/tooted/leivad-kupsised-ja-kupsetised/kuivikud-ja-kringlid",
    "/tooted/leivad-kupsised-ja-kupsetised/kuivikud-ja-nakileivad",
    "/tooted/leivad-kupsised-ja-kupsetised/kupsised",
    # Liha
    "/tooted/liha-ja-taimsed-valgud/hakkliha",
    "/tooted/liha-ja-taimsed-valgud/veiseliha",
    "/tooted/liha-ja-taimsed-valgud/sealiha",
    "/tooted/liha-ja-taimsed-valgud/muu-liha",
    "/tooted/liha-ja-taimsed-valgud/kana-broiler-ja-kalkun",
    "/tooted/liha-ja-taimsed-valgud/singi-ja-vorstiloigud",
    "/tooted/liha-ja-taimsed-valgud/vorstid-viinerid-ja-peekon",
    "/tooted/liha-ja-taimsed-valgud/taimsed-valgud-ja-juustuvalgud",
    "/tooted/liha-ja-taimsed-valgud/broiler-ja-kalkun",
    "/tooted/liha-ja-taimsed-valgud/lambaliha-ja-ulukid",
    # Kala
    "/tooted/kala-ja-mereannid/mereannid",
    "/tooted/kala-ja-mereannid/muud-kalatooted",
    "/tooted/kala-ja-mereannid/kala",
    # Piim
    "/tooted/piim-munad-ja-rasvad/piim-ja-hapupiim",
    "/tooted/piim-munad-ja-rasvad/toiduvalmistustooted",
    "/tooted/piim-munad-ja-rasvad/jogurtid",
    "/tooted/piim-munad-ja-rasvad/taimsed-piimajoogid",
    "/tooted/piim-munad-ja-rasvad/hapupiim",
    "/tooted/piim-munad-ja-rasvad/koored",
    "/tooted/piim-munad-ja-rasvad/kohupiim-puding-ja-magustoit",
    "/tooted/piim-munad-ja-rasvad/rasvad",
    "/tooted/piim-munad-ja-rasvad/munad",
    "/tooted/piim-munad-ja-rasvad/kohupiim",
    # Juustud
    "/tooted/juustud/taimsed-juustud",
    "/tooted/juustud/toidu-ja-gurmeejuustud",
    "/tooted/juustud/tuki-ja-viilujuustud",
    # Valmistoit
    "/tooted/valmistoit/salatid-supid-ja-leivad",
    "/tooted/valmistoit/einesalatid-ja-varske-pasta",
    "/tooted/valmistoit/pallid-pihvid-ja-pannkoogid",
    "/tooted/valmistoit/vormiroad-pasta-ja-lasanje",
    "/tooted/valmistoit/valmistoidud-ja-supid",
    "/tooted/valmistoit/puder-ja-kissellid",
    # Olid, vürtsid
    "/tooted/olid-vurtsid-maitseained/maitsekastmed-ja-pastad",
    "/tooted/olid-vurtsid-maitseained/texmex",
    "/tooted/olid-vurtsid-maitseained/ketsupid-ja-sinepid",
    "/tooted/olid-vurtsid-maitseained/aadikad-ja-palsamiaadikad",
    "/tooted/olid-vurtsid-maitseained/puljongid-ja-kastmepohjad",
    "/tooted/olid-vurtsid-maitseained/soolad",
    "/tooted/olid-vurtsid-maitseained/maitseained",
    "/tooted/olid-vurtsid-maitseained/olid",
    "/tooted/olid-vurtsid-maitseained/salatikastmed",
    "/tooted/olid-vurtsid-maitseained/majonees",
    # Kuivtooted
    "/tooted/kuivtooted-ja-kupsetamine/kliid-idud-tangud",
    "/tooted/kuivtooted-ja-kupsetamine/konservid",
    "/tooted/kuivtooted-ja-kupsetamine/jahud-ja-kupsetussegud",
    "/tooted/kuivtooted-ja-kupsetamine/riis-pasta-ja-nuudlid",
    "/tooted/kuivtooted-ja-kupsetamine/helbed-krobinad-ja-muslid",
    "/tooted/kuivtooted-ja-kupsetamine/suhkur-magusained-ja-mesi",
    "/tooted/kuivtooted-ja-kupsetamine/seemned-pahklid-ja-kuivatatud-puuviljad",
    "/tooted/kuivtooted-ja-kupsetamine/toiduained",
    "/tooted/kuivtooted-ja-kupsetamine/magustoidud",
    "/tooted/kuivtooted-ja-kupsetamine/kupsetusvahendid",
    "/tooted/kuivtooted-ja-kupsetamine/moosid-ja-marmelaadid",
    # Joogid
    "/tooted/joogid/energia-ja-spordijoogid",
    "/tooted/joogid/long-dringid",
    "/tooted/joogid/alkoholisegud",
    "/tooted/joogid/veinid",
    "/tooted/joogid/karastusjoogid",
    "/tooted/joogid/vesi",
    "/tooted/joogid/kakao",
    "/tooted/joogid/muud-joogid",
    "/tooted/joogid/olled",
    "/tooted/joogid/tee",
    "/tooted/joogid/kange-alkohol",
    "/tooted/joogid/siidrid",
    "/tooted/joogid/joogikontsentraadid",
    "/tooted/joogid/mahlad",
    "/tooted/joogid/kohv-ja-kohvifiltrid",
    # Külmutatud
    "/tooted/kulmutatud-toidud/kulmutatud-liha-ja-kala",
    "/tooted/kulmutatud-toidud/kulmutatud-eined",
    "/tooted/kulmutatud-toidud/kulmutatud-kupsetised-ja-leivad",
    "/tooted/kulmutatud-toidud/kulmutatud-koogiviljad",
    "/tooted/kulmutatud-toidud/kulmutatud-pitsad",
    "/tooted/kulmutatud-toidud/kulmutatud-kartulitooted",
    "/tooted/kulmutatud-toidud/kulmutatud-puuviljad-ja-marjad",
    "/tooted/kulmutatud-toidud/muud-kulmutatud-tooted",
    "/tooted/kulmutatud-toidud/jaatised",
    # Maiustused
    "/tooted/maiustused-ja-suupisted/kropsud-ja-muud-naksid",
    "/tooted/maiustused-ja-suupisted/hooajalised-ja-kinkemaiustused",
    "/tooted/maiustused-ja-suupisted/narimiskummid",
    "/tooted/maiustused-ja-suupisted/pastillid",
    "/tooted/maiustused-ja-suupisted/sokolaadid",
    "/tooted/maiustused-ja-suupisted/muud-maiustused",
    "/tooted/maiustused-ja-suupisted/kommikotid",
    # Kosmeetika
    "/tooted/kosmeetika-ja-hugieen/juuksed-ja-juuksehooldus",
    "/tooted/kosmeetika-ja-hugieen/naohooldus",
    "/tooted/kosmeetika-ja-hugieen/nahahooldus",
    "/tooted/kosmeetika-ja-hugieen/intiimhugieen-ja-intiimtooted",
    "/tooted/kosmeetika-ja-hugieen/suuhooldus",
    "/tooted/kosmeetika-ja-hugieen/seebid-ja-pesuvahendid",
    "/tooted/loodustooted-ja-toidulisandid",
    # Lapsed
    "/tooted/lapsed/emapiimaasendajad",
    "/tooted/lapsed/pudrud-ja-pureesupid",
    "/tooted/lapsed/lastetoidud",
    "/tooted/lapsed/laste-pureed-ja-muud-vahepalad",
    "/tooted/lapsed/mahkmed-ja-lapsehooldus",
    "/tooted/lapsed/puhastamine-ja-hugieen",
    "/tooted/lapsed/laste-vahepalad",
    "/tooted/lapsed/beebi-ja-lapsehooldusvahendid",
    # Lemmikloomad
    "/tooted/lemmikloomad/koeratoit",
    "/tooted/lemmikloomad/kassitoit",
    "/tooted/lemmikloomad/muud-lemmikloomade-tarvikud",
    "/tooted/lemmikloomad/kassiliiv",
    # Kodu
    "/tooted/kodu-ja-majapidamistarbed",
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

PACK_RE = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
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


def extract_next_data(html: str) -> Optional[dict]:
    """Extract __NEXT_DATA__ JSON from page HTML."""
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html, re.S
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        print(f"[warn] __NEXT_DATA__ JSON parse error: {e}", file=sys.stderr)
        return None


def get_build_id(next_data: dict) -> Optional[str]:
    """Extract Next.js build ID from __NEXT_DATA__."""
    return next_data.get("buildId")


def parse_apollo_products(apollo_state: dict) -> list[dict]:
    """
    Extract products from Apollo GraphQL cache.

    Structure:
      'Product:{"id":"4740113093549","storeId":"542860184"}': {
          ean: '4740113093549',
          name: 'Farmi Kodujuust...',
          slug: 'farmi-kodujuust-...',
          price: 2.12,   (may be here or in ProductStoreEdge)
          ...
      }
      'ProductStoreEdge:{"sorId":"941944000"}': {
          price: 2.12,
          product: {'__ref': 'Product:{"id":"..."}'},
          ...
      }
    """
    # First collect prices from ProductStoreEdge
    prices_by_product_ref: dict[str, float] = {}
    for key, val in apollo_state.items():
        if not key.startswith("ProductStoreEdge:"):
            continue
        if not isinstance(val, dict):
            continue
        price = val.get("price") or val.get("regularPrice") or val.get("campaignPrice")
        if price is None:
            continue
        try:
            price_f = float(price)
        except Exception:
            continue
        if price_f <= 0:
            continue
        product_ref = val.get("product", {})
        if isinstance(product_ref, dict):
            ref_key = product_ref.get("__ref", "")
            if ref_key:
                # Keep lowest price if multiple edges per product
                if ref_key not in prices_by_product_ref or price_f < prices_by_product_ref[ref_key]:
                    prices_by_product_ref[ref_key] = price_f

    products = []
    for key, val in apollo_state.items():
        if not key.startswith("Product:"):
            continue
        if not isinstance(val, dict):
            continue
        if val.get("__typename") != "Product":
            continue

        ean = str(val.get("ean") or val.get("id") or "").strip()
        if not ean or len(ean) < 8:
            continue

        name = str(val.get("name") or "").strip()
        if not name:
            continue

        # Price: try direct, then from ProductStoreEdge
        price = None
        for price_key in ["price", "regularPrice", "campaignPrice", "lowestPrice"]:
            p = val.get(price_key)
            if p is not None:
                try:
                    price = float(p)
                    if price > 0:
                        break
                except Exception:
                    pass
        if not price:
            price = prices_by_product_ref.get(key)
        if not price or price <= 0:
            continue

        slug = str(val.get("slug") or val.get("urlSlug") or "").strip()
        source_url = f"{BASE}/toode/{slug}/{ean}" if slug else f"{BASE}/toode/{ean}"
        ext_id = ean

        products.append({
            "ext_id": ext_id,
            "ean": ean,
            "name": name,
            "size_text": parse_size_from_name(name),
            "price": price,
            "source_url": source_url,
        })

    return products


def find_total_pages(html: str) -> int:
    """Find total pages from Prisma pagination links in SSR HTML."""
    soup = BeautifulSoup(html, "lxml")
    max_page = 1
    # Prisma uses data-test-id="pagination-link" with href="/tooted/X?page=N"
    for a in soup.find_all("a", attrs={"data-test-id": "pagination-link"}):
        href = a.get("href", "")
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    if max_page > 1:
        return max_page
    # Fallback: look for any page number links
    for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
        m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def scrape_category(cat_path: str, delay: float = 0.5) -> list[dict]:
    all_products: list[dict] = []
    seen_eans: set[str] = set()

    base_url = BASE + cat_path

    # Fetch first page HTML to get build ID and total pages
    html = fetch_html(base_url)
    if not html:
        print(f"[skip] {cat_path} — failed to fetch", file=sys.stderr)
        return []

    next_data = extract_next_data(html)
    if not next_data:
        print(f"[skip] {cat_path} — no __NEXT_DATA__", file=sys.stderr)
        return []

    build_id = get_build_id(next_data)
    total_pages = find_total_pages(html)
    print(f"[cat] {cat_path} — {total_pages} pages (build: {build_id})", file=sys.stderr)

    # Cat path for Next.js JSON API: /tooted/juustud -> et/tooted/juustud
    # Next.js data URL: /_next/data/{buildId}/et{cat_path}.json
    def get_page_data(page_num: int) -> Optional[dict]:
        if page_num == 1:
            # Use already fetched HTML
            return next_data
        if build_id:
            # Use Next.js JSON API for subsequent pages
            json_url = f"{BASE}/_next/data/{build_id}/et{cat_path}.json?page={page_num}"
            resp = fetch_html(json_url)
            if resp:
                try:
                    return json.loads(resp)
                except Exception:
                    pass
        # Fallback: fetch HTML page
        page_html = fetch_html(f"{base_url}?page={page_num}")
        if page_html:
            return extract_next_data(page_html)
        return None

    page_num = 1
    while page_num <= total_pages:
        page_data = get_page_data(page_num)
        if not page_data:
            print(f"[warn] no data on page {page_num}", file=sys.stderr)
            break

        apollo_state = (
            page_data
            .get("props", {})
            .get("pageProps", {})
            .get("apolloState", {})
        )
        # Next.js JSON API wraps in pageProps differently
        if not apollo_state:
            apollo_state = page_data.get("pageProps", {}).get("apolloState", {})

        if not apollo_state:
            print(f"[warn] no apolloState on page {page_num}", file=sys.stderr)
            break

        products = parse_apollo_products(apollo_state)
        new_products = [p for p in products if p["ean"] not in seen_eans]
        for p in new_products:
            seen_eans.add(p["ean"])
        all_products.extend(new_products)

        print(
            f"[page] {cat_path} p{page_num}/{total_pages} "
            f"→ {len(new_products)} new (total: {len(all_products)})",
            file=sys.stderr
        )

        if not new_products:
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
    ap.add_argument("--store-id", type=int, default=int(os.getenv("STORE_ID", "14")))
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
            print(f"[warn] {cat} → 0 products", file=sys.stderr)
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

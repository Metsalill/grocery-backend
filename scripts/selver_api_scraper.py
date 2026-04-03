#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver scraper v2 - Vue Storefront Elasticsearch API, no Playwright.
Category IDs verified from live API 2026-04-03.
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

import requests
import psycopg2

BASE = "https://www.selver.ee"
API_BASE = f"{BASE}/api/catalog/vue_storefront_catalog_et"
PAGE_SIZE = 24

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
    "Referer": BASE + "/",
}

# Verified category IDs from /api/catalog/vue_storefront_catalog_et/category/_search
# Using leaf-level categories (product_count > 0, no children).
CATEGORIES = {
    # Puu- ja koogiviljad (parent 209)
    "ounad-pirnid":                          [210],
    "troopilised-eksootilised-viljad":        [212],
    "koogiviljad-juurviljad":                 [213],
    "seened":                                 [214],
    "maitsetaimed-varsked-saltid-piprad":     [215],
    "puuviljasalatid":                        [216],
    "marjad":                                 [217],
    "smuutid-varsked-mahlad-produce":         [369],

    # Liha- ja kalatooted (parent 218)
    "sealiha":                                [219],
    "linnuliha":                              [220],
    "veise-lamba-ulukiliha":                  [221],
    "hakkliha":                               [222],
    "keedu-suitsuvorstid-viinerid":           [223],
    "singid-rulaadid":                        [224],
    "muud-lihatooted":                        [225],
    "grillvorstid-verivorstid":               [226],
    "gurmee-lihatooted":                      [227],
    "varske-kala-mereannid":                  [228],
    "soolatud-suitsutatud-kalatooted":        [229],
    "toodeldud-mereannid":                    [230],
    "muud-kalatooted":                        [231],

    # Piimatooted, munad, void (parent 233)
    "piimad-koored":                          [234],
    "kohupiimad-kodujuustud":                 [235],
    "jogurtid-jogurtijoogid":                 [236],
    "kohukesed":                              [237],
    "muud-magustoidud-dairy":                 [238],
    "munad":                                  [239],
    "void-margariinid":                       [240],

    # Juustud (parent 242)
    "juustud":                                [243],
    "maardejuustud":                          [244],
    "delikatessjuustud":                      [245],

    # Leivad, saiad, kondiitritooted (parent 247)
    "leivad":                                 [248],
    "saiad":                                  [249],
    "sepikud-kuklid-lavassid":                [250],
    "nakileivad-leib":                        [251],
    "selveri-pagarid":                        [252],
    "tordid":                                 [253],
    "koogid-rullbiskviidid":                  [254],
    "saiakesed-stritslid-kringlid":           [255],

    # Valmistoidud (parent 256)
    "salatid":                                [257],
    "jahutatud-valmistoidud":                 [258],
    "magustoidud-valmis":                     [260],
    "sushi":                                  [261],

    # Maitseained ja puljongid (parent 262)
    "maitseained":                            [263],
    "maailma-kook":                           [264],
    "puljongid":                              [265],

    # Kastmed, olid (parent 266)
    "olid-aadikad":                           [267],
    "majoneesid-sinepid":                     [268],
    "ketsupid-tomatipastad-kastmed":          [269],
    "gurmee-kastmed":                         [270],

    # Maiustused, kupsised, naksid (parent 271)
    "kommipakid":                             [272],
    "natsud-pastillid":                       [273],
    "muud-maiustused":                        [274],
    "kupsised":                               [275],
    "nakileivad-maiustused":                  [276],
    "pahklid-kuivatatud-puuviljad":           [277],
    "sipsid":                                 [278],
    "kommikarbid":                            [282],
    "sokolaadid":                             [283],

    # Kulmutatud toidukaubad (parent 284)
    "kulmutatud-liha-kalatooted":             [285],
    "kulmutatud-valmistooted":                [286],
    "kulmutatud-koogiviljad-marjad":          [287],
    "kulmutatud-taignad-kondiitritooted":     [288],
    "jaatised":                               [289],

    # Joogid - lahja alkohol (parent 29)
    "olled-siidrid-segud-kokteilid":          [30],
    "punased-veinid":                         [31],
    "valged-veinid":                          [32],
    "roosad-veinid":                          [33],
    "likoorveinid":                           [34],
    "shampanjad-vahuveinid":                  [35],

    # Kange alkohol (parent 37)
    "viinad":                                 [38],
    "dzinnid":                                [39],
    "viskid":                                 [40],
    "konjakid-brandid":                       [41],
    "rummid":                                 [42],
    "aperitiiviid":                           [43],
    "likoorid":                               [44],
    "muud-kanged-alkohoolsed-joogid":         [45],

    # Kohv, tee, kakao (under joogid, parent 46)
    "kohvid-joogid":                          [373],
    "teed":                                   [374],
    "kakaod-kakaojoogid":                     [375],

    # Veed, mahlad (parent 48)
    "veed":                                   [50],
    "mahlad-kontsentraadid-siirupid":         [51],
    "smuutid-varsked-mahlad-joogid":          [49],

    # Karastus- ja energiajoogid (parent 52)
    "karastusjoogid-toonikud":                [53],
    "energiajoogid":                          [54],
    "alkoholivabad-joogid":                   [55],

    # Spordijoogid (parent 56)
    "spordijoogid":                           [57],

    # Kuivained (parent 9)
    "jahud":                                  [10],
    "makaronid":                              [11],
    "tangained":                              [12],
    "riisid":                                 [13],
    "hommikuhelbed-muslid-kiirpudrud":        [15],
    "kuivsupid-kastmed":                      [16],
    "paja-nuudliroad":                        [17],

    # Hoidised (parent 18)
    "magusad-hoidised":                       [19],
    "hoidised":                               [20],
    "valmistoidud-purgis":                    [21],

    # Lastekaubad (parent 306)
    "lastetoidud":                            [307],
    "mahkmed":                                [308],
    "beebi-hooldusvahendid":                  [309],

    # Lemmikloomakaubad (parent 314)
    "kassitoidud":                            [315],
    "koeratoidud":                            [316],
    "vaikeloomatoidud":                       [317],
    "lemmikloomatarbed":                      [319],

    # Enesehooldustarbed (parent 63)
    "tervisekaubad":                          [65],
    "apteegikaubad":                          [66],
    "hambapastad-suuveed":                    [69],
    "hambaharjad-hambaniidid":                [70],
    "naokreemid":                             [72],
    "naopuhastus":                            [73],
    "naomaskid":                              [425],
    "meeste-naohooldus":                      [76],
    "shampoonid-palsamid-maskid":             [78],
    "soengutugevdajad":                       [79],
    "juuksevarvid":                           [80],
    "juukseharjad-kammid-kummid":             [81],
    "dushigeelid-seebid":                     [83],
    "kehakreemid-ihupiimad":                  [84],
    "raseerimis-epileerimistarbed":           [86],
    "higistamisvastased-tarbed":              [88],
    "intiimhugieen":                          [91],
    "lohnad-tualettveed":                     [92],
    "meeste-kehahooldus":                     [93],

    # Majapidamis- ja kodukaubad (parent 100)
    "majapidamispaberid":                     [102],
    "tualettpaberid":                         [103],
    "salvratikud-laudlinad":                  [104],
    "noudepesuvahendid":                      [108],
    "svammid-harjad":                         [109],
    "universaalsed-puhastusvahendid":         [110],
    "eriotstarbelised-puhastusvahendid":      [111],
    "prugikotid-tolmukotid":                  [112],
    "pesupesemisvahendid":                    [114],
}

PACK_RE = re.compile(r"(\d+)\s*[x*]\s*(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIZE_RE  = re.compile(r"\b(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def parse_size(name: str) -> str:
    m = PACK_RE.search(name)
    if m:
        qty, num, unit = m.groups()
        return f"{qty}x{num.replace(',', '.')} {unit.lower()}"
    m = SIZE_RE.search(name)
    if m:
        num, unit = m.groups()
        return f"{num.replace(',', '.')} {unit.lower()}"
    return ""


def fetch_products_for_category(category_ids: list[int], from_: int = 0) -> Optional[dict]:
    query = {
        "query": {
            "bool": {
                "filter": {
                    "bool": {
                        "must": [
                            {"terms": {"category_ids": category_ids}},
                            {"terms": {"visibility": [2, 3, 4]}},
                            {"terms": {"status": [1]}},
                        ]
                    }
                }
            }
        },
        "_source": [
            "sku", "name", "barcode", "price", "final_price",
            "prices", "brand", "manufacturer", "url_key",
        ],
        "sort": [{"entity_id": {"order": "asc"}}],
    }

    url = f"{API_BASE}/product/_search"
    params = {
        "from": from_,
        "size": PAGE_SIZE,
        "request": json.dumps(query, separators=(",", ":")),
        "sort": "",
    }

    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            print(f"  [warn] HTTP {r.status_code} from={from_}", file=sys.stderr)
        except Exception as e:
            print(f"  [warn] fetch error attempt {attempt+1}: {e}", file=sys.stderr)
        time.sleep(1.0 * (attempt + 1))
    return None


def extract_price(src: dict) -> Optional[float]:
    prices = src.get("prices") or []
    for p in prices:
        if isinstance(p, dict) and p.get("customer_group_id") == 0:
            fp = p.get("final_price") or p.get("price")
            if fp and float(fp) > 0:
                return float(fp)
    for field in ["final_price", "special_price", "price"]:
        val = src.get(field)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    return f
            except Exception:
                pass
    return None


def scrape_category(slug: str, category_ids: list[int], delay: float = 0.3) -> list[dict]:
    all_products = []
    seen_skus: set[str] = set()
    from_ = 0
    total = None

    while True:
        data = fetch_products_for_category(category_ids, from_=from_)
        if not data:
            break

        hits_obj = data.get("hits", {})
        if total is None:
            total = hits_obj.get("total", {})
            if isinstance(total, dict):
                total = total.get("value", 0)
            else:
                total = int(total or 0)
            print(f"  [cat] {slug} total={total}", file=sys.stderr)

        hits = hits_obj.get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})
            sku = str(src.get("sku") or "").strip()
            if not sku or sku in seen_skus:
                continue
            seen_skus.add(sku)

            name = str(src.get("name") or "").strip()
            if not name:
                continue

            barcode = str(src.get("barcode") or "").strip()
            ean_raw = barcode if len(re.sub(r"\D", "", barcode)) >= 8 else ""

            price = extract_price(src)
            if not price or price <= 0:
                continue

            brand = str(src.get("brand") or src.get("manufacturer") or "").strip()
            url_key = str(src.get("url_key") or "").strip()
            source_url = f"{BASE}/{url_key}" if url_key else f"{BASE}/toode/{sku}"

            all_products.append({
                "ext_id": sku,
                "name": name,
                "brand": brand,
                "ean_raw": ean_raw,
                "size_text": parse_size(name),
                "price": price,
                "source_url": source_url,
            })

        fetched = from_ + len(hits)
        print(f"  [page] from={from_} fetched={len(hits)} running={len(all_products)}", file=sys.stderr)

        if fetched >= total:
            break
        from_ = fetched
        time.sleep(delay)

    return all_products


def upsert_batch(conn, rows: list[dict], store_id: int) -> tuple[int, int]:
    if not rows:
        return 0, 0

    ts_now = datetime.datetime.now(datetime.timezone.utc)
    sql = """
        SELECT upsert_product_and_price(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
    payload = [
        (
            "selver",
            row["ext_id"],
            row["name"],
            row["brand"],
            row["size_text"],
            row["ean_raw"],
            row["price"],
            "EUR",
            store_id,
            ts_now,
            row["source_url"],
        )
        for row in rows
    ]

    try:
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()
        return len(payload), 0
    except Exception as e:
        conn.rollback()
        print(f"[warn] batch upsert failed: {e}", file=sys.stderr)
        return 0, len(payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store-id", type=int, default=int(os.getenv("STORE_ID", "31")))
    ap.add_argument("--delay", type=float, default=float(os.getenv("REQ_DELAY", "0.3")))
    ap.add_argument("--shard", type=int, default=int(os.getenv("SHARD", "0")))
    ap.add_argument("--shards", type=int, default=int(os.getenv("SHARDS", "1")))
    args = ap.parse_args()

    all_cats = list(CATEGORIES.items())
    my_cats = [item for i, item in enumerate(all_cats) if i % args.shards == args.shard]

    print(
        f"[info] shard {args.shard}/{args.shards} "
        f"{len(my_cats)}/{len(all_cats)} cats "
        f"store_id={args.store_id} delay={args.delay}s",
        file=sys.stderr
    )

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False

    total_ok = 0
    total_errors = 0

    for slug, category_ids in my_cats:
        print(f"[cat] {slug} ids={category_ids}", file=sys.stderr)
        products = scrape_category(slug, category_ids, delay=args.delay)
        if not products:
            print(f"[warn] {slug} -> 0 products", file=sys.stderr)
            continue
        ok, errors = upsert_batch(conn, products, args.store_id)
        total_ok += ok
        total_errors += errors
        print(f"[done] {slug} -> {ok} ok, {errors} errors", file=sys.stderr)

    conn.close()
    print(f"[TOTAL] {total_ok} ok, {total_errors} errors", file=sys.stderr)


if __name__ == "__main__":
    main()

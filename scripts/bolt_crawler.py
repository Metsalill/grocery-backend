#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coop on Bolt Food → categories → products → CSV / upsert to staging_coop_products

- Opens https://food.bolt.eu/en-US/{city_path}
- Finds the store by its visible display name (exact match).
- Auto-discovers category tabs (links containing ?categoryName=).
- Scrapes tiles; Bolt does not expose EAN/GTIN → keep blank.
- Writes CSV and upserts into your existing `staging_coop_products`
  with channel='bolt' and store_host='bolt:<slug-of-store-name>'.

Expected columns present in staging_coop_products (script adds missing ones):
  chain, channel, store_name, store_host, city_path, category_name,
  ext_id, name, brand, manufacturer, size_text, price, currency,
  image_url, url, description, ean_raw, scraped_at
"""

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

from tenacity import retry, stop_after_attempt, wait_fixed
from playwright.sync_api import sync_playwright
from selectolax.parser import HTMLParser

# Optional DB
try:
    import psycopg
except Exception:
    psycopg = None

EUR = "€"
CHAIN = "Coop"
CHANNEL = "bolt"


def slugify_host(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return f"bolt:{s}"


def parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    t = text.replace("\xa0", " ").strip()
    cur = "EUR" if EUR in t or "€" in t else None
    num = re.sub(r"[^0-9,.\-]", "", t).replace(",", ".")
    try:
        return round(float(num), 2), cur
    except Exception:
        return None, cur


def guess_size(name: str) -> Optional[str]:
    m = re.search(r"(\b\d+\s?(?:g|kg|l|ml|cl|pcs|tk)\b)", name, flags=re.I)
    return m.group(1) if m else None


def guess_brand(name: str) -> Optional[str]:
    parts = re.split(r"[,-]", name)
    head = parts[0].strip()
    tok = re.findall(r"\b[A-ZÄÖÜÕ][\wÄÖÜÕäöüõ&'.-]+\b", head)
    return tok[0] if tok else None


def extract_category_links(page_html: str) -> List[Tuple[str, str]]:
    tree = HTMLParser(page_html)
    seen = set()
    out = []
    for a in tree.css("a"):
        href = a.attributes.get("href", "")
        if "categoryName=" in href:
            cat = a.text().strip() or re.search(r"categoryName=([^&]+)", href)
            if not isinstance(cat, str) and cat:
                cat = cat.group(1)
                cat = re.sub(r"%20", " ", cat)
            cat = (cat or "").strip()
            key = (cat, href)
            if key not in seen:
                seen.add(key)
                out.append((cat, href))
    return out


def extract_tiles_from_dom(page_html: str) -> List[Dict]:
    tree = HTMLParser(page_html)
    tiles = []
    for btn in tree.css("button"):
        btxt = (btn.text() or "").strip().lower()
        if btxt in {"+", "add", "lisa", "add to cart", "add "}:
            tile = btn
            for _ in range(6):
                tile = tile.parent
                if tile is None:
                    break
                if tile.tag == "article" or ("card" in tile.attributes.get("class", "")):
                    break
            if not tile:
                continue

            name = None
            price_txt = None
            img = None

            for cand in tile.css("h1,h2,h3,h4,strong,p,span"):
                t = (cand.text() or "").strip()
                if not t:
                    continue
                if EUR in t or re.search(r"\d[\d\.,]\s?€", t):
                    price_txt = price_txt or t
                if not name and len(t) > 6 and "€" not in t:
                    name = t

            for im in tile.css("img"):
                src = im.attributes.get("src") or im.attributes.get("data-src")
                if src and "http" in src:
                    img = src
                    break

            price, currency = parse_price(price_txt or "")
            tiles.append(dict(
                name=name or "",
                price=price,
                currency=currency or "EUR",
                image_url=img or "",
            ))
    return tiles


def ensure_staging_schema(conn):
    """Add any missing columns to staging_coop_products (idempotent)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS staging_coop_products(
      chain           text,
      channel         text,
      store_name      text,
      store_host      text,
      city_path       text,
      category_name   text,
      ext_id          text,
      name            text,
      brand           text,
      manufacturer    text,
      size_text       text,
      price           numeric(12,2),
      currency        text,
      image_url       text,
      url             text,
      description     text,
      ean_raw         text,
      scraped_at      timestamptz DEFAULT now()
    );
    ALTER TABLE staging_coop_products
      ADD COLUMN IF NOT EXISTS chain           text,
      ADD COLUMN IF NOT EXISTS channel         text,
      ADD COLUMN IF NOT EXISTS store_name      text,
      ADD COLUMN IF NOT EXISTS store_host      text,
      ADD COLUMN IF NOT EXISTS city_path       text,
      ADD COLUMN IF NOT EXISTS category_name   text,
      ADD COLUMN IF NOT EXISTS ext_id          text,
      ADD COLUMN IF NOT EXISTS name            text,
      ADD COLUMN IF NOT EXISTS brand           text,
      ADD COLUMN IF NOT EXISTS manufacturer    text,
      ADD COLUMN IF NOT EXISTS size_text       text,
      ADD COLUMN IF NOT EXISTS price           numeric(12,2),
      ADD COLUMN IF NOT EXISTS currency        text,
      ADD COLUMN IF NOT EXISTS image_url       text,
      ADD COLUMN IF NOT EXISTS url             text,
      ADD COLUMN IF NOT EXISTS description     text,
      ADD COLUMN IF NOT EXISTS ean_raw         text,
      ADD COLUMN IF NOT EXISTS scraped_at      timestamptz DEFAULT now();
    CREATE INDEX IF NOT EXISTS idx_stg_coop_host ON staging_coop_products(store_host);
    CREATE INDEX IF NOT EXISTS idx_stg_coop_name ON staging_coop_products (lower(name));
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def upsert_rows_to_staging_coop(rows: List[Dict], db_url: str):
    if not psycopg:
        print("psycopg not installed; skipping DB.", file=sys.stderr)
        return
    if not db_url:
        print("DATABASE_URL empty; skipping DB.", file=sys.stderr)
        return

    with psycopg.connect(db_url) as conn:
        ensure_staging_schema(conn)
        ins = """
        INSERT INTO staging_coop_products(
          chain,channel,store_name,store_host,city_path,category_name,
          ext_id,name,brand,manufacturer,size_text,price,currency,image_url,url,
          description,ean_raw,scraped_at
        )
        VALUES (
          %(chain)s,%(channel)s,%(store_name)s,%(store_host)s,%(city_path)s,%(category_name)s,
          %(ext_id)s,%(name)s,%(brand)s,%(manufacturer)s,%(size_text)s,%(price)s,%(currency)s,%(image_url)s,%(url)s,
          %(description)s,%(ean_raw)s,%(scraped_at)s
        );
        """
        with conn.cursor() as cur:
            cur.executemany(ins, rows)
        conn.commit()
    print(f"[db] upserted {len(rows)} rows into staging_coop_products")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def safe_get_text(el):
    return (el.inner_text() or "").strip()


def run(city: str, store_name: str, headless: bool, req_delay: float,
        out_csv: str, upsert_db: bool):

    start_url = f"https://food.bolt.eu/en-US/{city}"
    scraped_at = dt.datetime.utcnow().isoformat()
    rows_out: List[Dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=(headless is True or str(headless) == "1"))
        context = browser.new_context()
        page = context.new_page()

        page.goto(start_url, timeout=60_000)
        page.wait_for_load_state("domcontentloaded")

        # open search and find store
        try:
            page.click('input[placeholder*="Restaurants"][placeholder*="stores"], input[type="search"]', timeout=10_000)
        except Exception:
            try:
                page.click("button:has(svg)", timeout=5_000)
            except Exception:
                pass

        page.keyboard.type(store_name)
        time.sleep(0.6)
        page.keyboard.press("Enter")
        time.sleep(1.0)

        page.wait_for_selector(f"text={store_name}", timeout=20_000)
        page.click(f"text={store_name}")

        page.wait_for_load_state("domcontentloaded")
        time.sleep(req_delay)

        store_html = page.content()
        store_host = slugify_host(store_name)

        # discover categories
        categories = []
        seen_cat = set()
        for cat_name, href in extract_category_links(store_html):
            if cat_name and cat_name.lower() not in seen_cat:
                seen_cat.add(cat_name.lower())
                if href.startswith("/"):
                    href = "https://food.bolt.eu" + href
                elif href.startswith("?"):
                    href = page.url.split("?")[0] + href
                categories.append((cat_name, href))
        if not categories:
            categories = [("All", page.url)]

        print(f"[info] discovered {len(categories)} categories")

        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")
            page.goto(href, timeout=60_000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(req_delay)

            # lazy-load
            try:
                for _ in range(8):
                    page.mouse.wheel(0, 2000)
                    time.sleep(0.25)
            except Exception:
                pass

            html = page.content()
            tiles = extract_tiles_from_dom(html)

            for t in tiles:
                name = (t.get("name") or "").strip()
                if not name:
                    continue
                price = t.get("price")
                currency = t.get("currency") or "EUR"
                image_url = t.get("image_url") or ""
                size_text = guess_size(name)
                brand = guess_brand(name)
                manufacturer = None
                ext_id = None

                rows_out.append(dict(
                    chain=CHAIN,
                    channel=CHANNEL,
                    store_name=store_name,
                    store_host=store_host,
                    city_path=city,
                    category_name=cat_name,
                    ext_id=ext_id,
                    name=name,
                    brand=brand,
                    manufacturer=manufacturer,
                    size_text=size_text,
                    price=price,
                    currency=currency,
                    image_url=image_url,
                    url=page.url,
                    description=None,
                    ean_raw=None,
                    scraped_at=scraped_at
                ))

        # CSV
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "chain","channel","store_name","store_host","city_path","category_name",
                "ext_id","name","brand","manufacturer","size_text","price","currency",
                "image_url","url","description","ean_raw","scraped_at"
            ])
            w.writeheader()
            for r in rows_out:
                w.writerow(r)
        print(f"[out] wrote {len(rows_out)} rows → {out_csv}")

        if upsert_db and os.getenv("DATABASE_URL"):
            upsert_rows_to_staging_coop(rows_out, os.getenv("DATABASE_URL"))

        context.close()
        browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True, help="Bolt city path (e.g. 2-tartu)")
    ap.add_argument("--store", required=True, help="Store display name (exact as shown in Bolt)")
    ap.add_argument("--headless", default="1")
    ap.add_argument("--req-delay", default="0.25", type=float)
    ap.add_argument("--out", required=True)
    ap.add_argument("--upsert-db", default="1")
    args = ap.parse_args()

    run(
        city=args.city,
        store_name=args.store,
        headless=(str(args.headless) == "1"),
        req_delay=float(args.req_delay),
        out_csv=args.out,
        upsert_db=(str(args.upsert_db) == "1"),
    )

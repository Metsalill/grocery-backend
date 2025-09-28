#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coop on Bolt Food → categories → products → CSV / upsert to staging_coop_products

- Opens https://food.bolt.eu/en-US/{city_path}
- Finds the store by its visible display name (exact match).
- Discovers category tabs (links containing ?categoryName=) OR
  uses an optional categories file:
    • --categories-file <path>  (one URL or query per line), or
    • --categories-dir <base>; will auto-pick {base}/{city}/{slug}.txt
      where slug = slugified store name (e.g., eedeni-coop-maksimarket)
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


def store_slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.lower())
    return re.sub(r"-+", "-", s).strip("-")


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


def normalize_cat_url(base_url: str, href: str) -> str:
    """Resolve category href against the store's base URL."""
    if not href:
        return base_url
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://food.bolt.eu" + href
    if href.startswith("?"):
        return base_url.split("?")[0] + href
    # relative path segment
    if base_url.endswith("/") and href.startswith("/"):
        return base_url[:-1] + href
    return base_url.rsplit("/", 1)[0] + "/" + href


def read_categories_override(path: str, base_url: str) -> List[Tuple[str, str]]:
    """Read one category per line; lines can be absolute URLs or ?categoryName=..."""
    out: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            href = line.strip()
            if not href or href.startswith("#"):
                continue
            url = normalize_cat_url(base_url, href)
            m = re.search(r"[?&]categoryName=([^&]+)", url)
            cat = (m.group(1) if m else href).replace("%20", " ")
            out.append((cat, url))
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
            tiles.append(
                dict(
                    name=name or "",
                    price=price,
                    currency=currency or "EUR",
                    image_url=img or "",
                )
            )
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
    try:
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
    except Exception as e:
        print(f"[db] WARN: upsert skipped due to connection error: {e}", file=sys.stderr)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def safe_get_text(el):
    return (el.inner_text() or "").strip()


def run(
    city: str,
    store_name: str,
    headless: bool,
    req_delay: float,
    out_csv: str,
    upsert_db: bool,
    categories_file: Optional[str] = None,
    categories_dir: Optional[str] = None,
):
    # Start in et-EE; tends to be more consistent for COOP placeholders/names
    start_url = f"https://food.bolt.eu/et-EE/{city}"
    scraped_at = dt.datetime.utcnow().isoformat()
    rows_out: List[Dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=(headless is True or str(headless) == "1"))
        context = browser.new_context()
        page = context.new_page()

        page.goto(start_url, timeout=60_000)
        page.wait_for_load_state("domcontentloaded")

        # --- robust store search & select ---
        def open_search() -> bool:
            # Try various entry points (search input or icon)
            selectors = [
                'input[type="search"]',
                'input[placeholder*="poed"], input[placeholder*="Pood"]',
                'input[placeholder*="Restaurants"][placeholder*="stores"]',
                'button:has(svg)',
                'button[aria-label*="Search"], button[aria-label*="Otsi"]',
            ]
            for sel in selectors:
                try:
                    page.click(sel, timeout=2000)
                    return True
                except Exception:
                    continue
            return False

        if not open_search():
            try:
                page.click('a:has-text("Pood"), a:has-text("Stores")', timeout=3000)
                time.sleep(0.5)
                open_search()
            except Exception:
                pass

        # Type name and pick the suggestion that best matches
        page.keyboard.type(store_name)
        time.sleep(0.8)

        candidates = [
            f'role=link[name="{store_name}"]',
            f'text="{store_name}"',
            f'li:has-text("{store_name}")',
            f'button:has-text("{store_name}")',
        ]
        clicked = False
        for loc in candidates:
            try:
                page.locator(loc).first.click(timeout=5000)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            try:
                page.keyboard.press("Enter")
                clicked = True
            except Exception:
                pass
        if not clicked:
            raise RuntimeError(f"Could not open store: {store_name}")

        page.wait_for_load_state("domcontentloaded")
        time.sleep(req_delay)

        store_host = slugify_host(store_name)
        base_url = page.url
        slug = store_slug(store_name)

        # Decide category source: explicit file > auto file in dir > autodiscovery
        cats: List[Tuple[str, str]] = []
        if categories_file and os.path.isfile(categories_file):
            cats = read_categories_override(categories_file, base_url)
            print(f"[info] using explicit categories file: {categories_file} ({len(cats)} cats)")
        elif categories_dir:
            auto_path = os.path.join(categories_dir, city, f"{slug}.txt")
            if os.path.isfile(auto_path):
                cats = read_categories_override(auto_path, base_url)
                print(f"[info] using categories from: {auto_path} ({len(cats)} cats)")

        if not cats:
            store_html = page.content()
            discovered = extract_category_links(store_html)
            seen_cat = set()
            for cat_name, href in discovered:
                if cat_name and cat_name.lower() not in seen_cat:
                    seen_cat.add(cat_name.lower())
                    cats.append((cat_name, normalize_cat_url(base_url, href)))
            if not cats:
                cats = [("All", base_url)]
        print(f"[info] categories selected: {len(cats)}")

        for cat_name, href in cats:
            print(f"[cat] {cat_name} -> {href}")
            page.goto(href, timeout=60_000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(max(0.5, req_delay))

            # Wait for any product card / add button to appear
            try:
                page.wait_for_selector('button:has-text("+")', timeout=15000)
            except Exception:
                pass

            # Progressive scroll until tile count stabilizes
            last_count = -1
            tiles_now: List[Dict] = []
            tries = 0
            while tries < 10:
                html = page.content()
                tiles_now = extract_tiles_from_dom(html)
                if len(tiles_now) == last_count:
                    tries += 1
                else:
                    tries = 0
                    last_count = len(tiles_now)
                try:
                    page.mouse.wheel(0, 2500)
                except Exception:
                    pass
                time.sleep(0.4)

            tiles = tiles_now

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

                rows_out.append(
                    dict(
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
                        scraped_at=scraped_at,
                    )
                )

        # CSV
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "chain",
                    "channel",
                    "store_name",
                    "store_host",
                    "city_path",
                    "category_name",
                    "ext_id",
                    "name",
                    "brand",
                    "manufacturer",
                    "size_text",
                    "price",
                    "currency",
                    "image_url",
                    "url",
                    "description",
                    "ean_raw",
                    "scraped_at",
                ],
            )
            w.writeheader()
            for r in rows_out:
                w.writerow(r)
        print(f"[out] wrote {len(rows_out)} rows → {out_csv}")

        # Safe upsert (only if we have rows and a DB URL)
        db_url = os.getenv("DATABASE_URL")
        if upsert_db and db_url and rows_out:
            upsert_rows_to_staging_coop(rows_out, db_url)
        else:
            reason = []
            if not upsert_db:
                reason.append("upsert disabled")
            if not db_url:
                reason.append("no DATABASE_URL")
            if not rows_out:
                reason.append("0 rows")
            print(f"[db] skipped upsert ({', '.join(reason)})")

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
    ap.add_argument("--categories-file", default="", help="Optional: file with category URLs (one per line)")
    ap.add_argument("--categories-dir", default="", help="Optional: base dir with {dir}/{city}/{slug}.txt")

    args = ap.parse_args()

    run(
        city=args.city,
        store_name=args.store,
        headless=(str(args.headless) == "1"),
        req_delay=float(args.req_delay),
        out_csv=args.out,
        upsert_db=(str(args.upsert_db) == "1"),
        categories_file=(args.categories_file or None),
        categories_dir=(args.categories_dir or None),
    )

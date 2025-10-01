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
- NEW: --deep 1 → opens each product modal to extract brand/manufacturer/size reliably
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
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

from tenacity import retry, stop_after_attempt, wait_fixed
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from selectolax.parser import HTMLParser

# Optional DB
try:
    import psycopg
except Exception:
    psycopg = None

EUR = "€"
CHAIN = "Coop"
CHANNEL = "bolt"

# -----------------------------
# Helpers
# -----------------------------

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

_STOPWORDS = {
    # Estonian category nouns that often prefix names
    "piim", "piimatooted", "keefir", "jogurt", "energiajoogid", "energiajoogi", "energiajoog",
    "vesi", "vett", "mahl", "jook", "joogid", "jogid", "õlu", "olu", "leib", "sai", "pagaritooted",
    "kommid", "šokolaad", "sokolaad", "krõpsud", "krõps", "krõbinad", "vorst", "sink",
}

def guess_brand(name: str) -> Optional[str]:
    """
    Better brand guess:
    - take capitalized tokens from the head of the name
    - drop known category nouns (stopwords)
    - prefer last 1–2 tokens that aren’t stopwords
    """
    tokens = re.findall(r"[A-ZÄÖÜÕ][\wÄÖÜÕäöüõ&'.-]+", name)
    tokens = [t for t in tokens if t.lower() not in _STOPWORDS]
    if not tokens:
        return None
    # Often the brand is one or two tokens near the end of the capitalized sequence
    if len(tokens) >= 2:
        return " ".join(tokens[-2:])
    return tokens[-1]

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
    if not rows:
        print("[db] no rows to upsert.")
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
def safe_inner_text(locator) -> str:
    try:
        return (locator.inner_text() or "").strip()
    except Exception:
        return ""

# -----------------------------
# Product scraping (deep)
# -----------------------------

def product_links_on_page(page) -> List[str]:
    """
    Return absolute URLs for product anchors on the current category page.
    Bolt usually uses anchors containing '/p/' and a trailing '/smc/<id>'.
    """
    urls = []
    for a in page.locator('a[href*="/p/"]').all():
        try:
            href = a.get_attribute("href") or ""
            if not href:
                continue
            if href.startswith("http"):
                urls.append(href)
            else:
                urls.append("https://food.bolt.eu" + href)
        except Exception:
            continue
    # unique preserve order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def parse_modal_text(modal_text: str) -> Dict[str, Optional[str]]:
    """
    Parse Brand, Manufacturer, Size/volume etc. from the modal's inner text.
    We keep it regex-based because Bolt's internal markup changes.
    """
    def grab(label: str) -> Optional[str]:
        m = re.search(label + r"\s*:\s*(.+)", modal_text, flags=re.I)
        return (m.group(1).strip() if m else None)

    brand = grab(r"Brand")
    manufacturer = grab(r"Manufacturer")
    size_text = grab(r"(Size|Size, volume)")
    description = None

    # Try to capture a “Ingredients:” block as description if present
    dm = re.search(r"(Ingredients|Koostis|Koostisosad)\s*:\s*(.+)", modal_text, flags=re.I)
    if dm:
        description = dm.group(2).strip()

    return dict(brand=brand, manufacturer=manufacturer, size_text=size_text, description=description)

def extract_ext_id(url: str) -> Optional[str]:
    m = re.search(r"/smc/(\d+)", url)
    return m.group(1) if m else None

def deep_scrape_one(page, product_url: str, req_delay: float) -> Dict[str, Optional[str]]:
    """
    Click the product link, wait for modal (or product view) and parse details.
    Return struct with: name, brand, manufacturer, size_text, image_url, ext_id, description.
    """
    result: Dict[str, Optional[str]] = {
        "name": None, "brand": None, "manufacturer": None, "size_text": None,
        "image_url": None, "ext_id": extract_ext_id(product_url), "description": None,
    }

    # Click by URL
    try:
        # target link on page
        link = page.locator(f'a[href="{product_url.replace("https://food.bolt.eu","")}"]')
        if link.count() == 0:
            # try absolute
            link = page.locator(f'a[href="{product_url}"]')
        link.first.click(timeout=10000)
    except Exception:
        # As a fallback, go to the URL in the same tab (Bolt usually keeps modal overlay).
        try:
            page.goto(product_url, timeout=60000)
        except Exception:
            return result

    # Wait a bit for modal
    time.sleep(req_delay)

    # Modal is a dialog overlay; grab full text and the heading
    modal = None
    try:
        modal = page.locator('[role="dialog"]').first
        modal_text = safe_inner_text(modal)
        # name from modal heading if present
        heading = safe_inner_text(modal.locator("h1, h2").first) or None
    except Exception:
        modal_text = ""
        heading = None

    # Extract first visible image inside modal for image_url
    image_url = None
    if modal:
        try:
            for im in modal.locator("img").all():
                src = im.get_attribute("src") or im.get_attribute("data-src")
                if src and src.startswith("http"):
                    image_url = src
                    break
        except Exception:
            pass

    details = parse_modal_text(modal_text or "")

    result.update({
        "name": heading,
        "brand": details.get("brand"),
        "manufacturer": details.get("manufacturer"),
        "size_text": details.get("size_text"),
        "image_url": image_url,
        "description": details.get("description"),
    })

    # Close modal if it exists
    try:
        if modal and modal.count() > 0:
            # Try close button, else ESC
            close_btn = modal.locator('button[aria-label*="Close"], button:has-text("×")')
            if close_btn.count():
                close_btn.first.click()
            else:
                page.keyboard.press("Escape")
    except Exception:
        pass

    return result

# -----------------------------
# Main run
# -----------------------------

def run(
    city: str,
    store_name: str,
    headless: bool,
    req_delay: float,
    out_csv: str,
    upsert_db: bool,
    categories_file: Optional[str] = None,
    categories_dir: Optional[str] = None,
    deep: bool = False,
):

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

        store_host = slugify_host(store_name)
        base_url = page.url
        slug = store_slug(store_name)

        # Decide category source
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
            # fallback: autodiscover per store
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
            time.sleep(req_delay)

            # lazy-load to fetch most items
            try:
                for _ in range(10):
                    page.mouse.wheel(0, 2200)
                    time.sleep(0.25)
            except Exception:
                pass

            # Collect product links
            links = product_links_on_page(page)
            print(f"[cat] found {len(links)} product links")

            if deep and links:
                # Deep mode: open modal per product
                for u in links:
                    details = deep_scrape_one(page, u, req_delay)
                    name = (details.get("name") or "").strip()
                    if not name:
                        # fallback: get some name from anchor element text
                        try:
                            anchor = page.locator(f'a[href="{u.replace("https://food.bolt.eu","")}"]')
                            if anchor.count() == 0:
                                anchor = page.locator(f'a[href="{u}"]')
                            name = safe_inner_text(anchor.first)
                        except Exception:
                            name = ""
                    if not name:
                        continue

                    row = dict(
                        chain=CHAIN,
                        channel=CHANNEL,
                        store_name=store_name,
                        store_host=store_host,
                        city_path=city,
                        category_name=cat_name,
                        ext_id=details.get("ext_id"),
                        name=name,
                        brand=details.get("brand") or guess_brand(name),
                        manufacturer=details.get("manufacturer"),
                        size_text=details.get("size_text") or guess_size(name),
                        price=None,                 # price not shown in modal text reliably → keep from tile?
                        currency="EUR",
                        image_url=details.get("image_url") or "",
                        url=u,
                        description=details.get("description"),
                        ean_raw=None,
                        scraped_at=scraped_at,
                    )
                    # Try to find price from the card near the anchor (best-effort)
                    try:
                        anc = page.locator(f'a[href="{u.replace("https://food.bolt.eu","")}"]').first
                        card = anc.locator("xpath=ancestor::article | xpath=ancestor::*[contains(@class,'card')]").first
                        price_txt = safe_inner_text(card.locator("text=€").first) or safe_inner_text(card)
                        price, _cur = parse_price(price_txt)
                        row["price"] = price
                    except Exception:
                        pass

                    rows_out.append(row)
            else:
                # Basic scan (fallback if deep disabled)
                html = page.content()
                # very rough tile extract
                tiles = []
                tree = HTMLParser(html)
                for a in tree.css('a'):
                    href = a.attributes.get("href") or ""
                    if "/p/" in href:
                        # name: try nearest heading text
                        name = a.text().strip()
                        if not name:
                            # fallback: img alt or next sibling text
                            for im in a.css("img"):
                                alt = im.attributes.get("alt")
                                if alt:
                                    name = alt.strip(); break
                        tiles.append({"name": name, "href": normalize_cat_url(href, href)})

                print(f"[cat] basic tiles parsed: {len(tiles)}")
                for t in tiles:
                    name = (t.get("name") or "").strip()
                    if not name:
                        continue
                    rows_out.append(dict(
                        chain=CHAIN,
                        channel=CHANNEL,
                        store_name=store_name,
                        store_host=store_host,
                        city_path=city,
                        category_name=cat_name,
                        ext_id=extract_ext_id(t["href"]),
                        name=name,
                        brand=guess_brand(name),
                        manufacturer=None,
                        size_text=guess_size(name),
                        price=None,
                        currency="EUR",
                        image_url="",
                        url=t["href"],
                        description=None,
                        ean_raw=None,
                        scraped_at=scraped_at,
                    ))

        # CSV
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "chain","channel","store_name","store_host","city_path","category_name",
                    "ext_id","name","brand","manufacturer","size_text","price","currency",
                    "image_url","url","description","ean_raw","scraped_at"
                ],
            )
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
    ap.add_argument("--categories-file", default="", help="Optional: file with category URLs (one per line)")
    ap.add_argument("--categories-dir", default="", help="Optional: base dir with {dir}/{city}/{slug}.txt")
    ap.add_argument("--deep", default="1", help="Open product modal to parse brand/manufacturer (1/0)")
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
        deep=(str(args.deep) == "1"),
    )

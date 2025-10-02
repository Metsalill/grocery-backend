#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coop on Bolt Food → categories → products → CSV / upsert to staging_coop_products

- Opens https://food.bolt.eu/{locale}/{city_path} (locale is taken from the first
  workable URL we touch so we don't fight et-EE/en-US swaps).
- If a categories override is present (file or dir), uses those links. Otherwise,
  discovers categories from the store page.
- For each category:
    * tries the URL directly;
    * if Bolt bounces to the store home, it clicks the visible category chip by text;
    * if Bolt shows “Menu updated / Menüüd uuendati”, it acknowledges and retries;
    * waits until real product tiles exist before parsing.
- Scrapes tiles without opening modals (fast path), but constructs stable ext_id and
  fills brand/size heuristically from name (Bolt doesn’t expose EAN).
"""

import argparse
import csv
import datetime as dt
import hashlib
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
            cat = a.text().strip()
            if not cat:
                m = re.search(r"[?&]categoryName=([^&]+)", href)
                if m:
                    cat = m.group(1).replace("%20", " ")
            key = (cat, href)
            if key not in seen:
                seen.add(key)
                out.append((cat, href))
    return out


def normalize_cat_url(base_url: str, href: str) -> str:
    """Build a fully-qualified category URL that preserves the locale."""
    if not href:
        return base_url
    if href.startswith("http"):
        return href
    # pull the protocol + host + locale prefix from base_url
    m = re.match(r"^(https://food\.bolt\.eu/(?:[a-z]{2}-[A-Z]{2}|en-US))", base_url)
    prefix = m.group(1) if m else "https://food.bolt.eu/en-US"
    if href.startswith("/"):
        return prefix + href
    if href.startswith("?"):
        return base_url.split("?")[0] + href
    if base_url.endswith("/") and href.startswith("/"):
        return base_url[:-1] + href
    return base_url.rsplit("/", 1)[0] + "/" + href


def base_url_from_category(url: str) -> str:
    m = re.search(r"^(https://food\.bolt\.eu/(?:[a-z]{2}-[A-Z]{2}|en-US)/[^/]+/p/\d+)", url)
    if m:
        return m.group(1)
    u = url.split("?", 1)[0]
    return u.split("/smc/")[0]


def read_categories_override(path: str, base_url: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            href = line.strip()
            if not href or href.startswith("#"):
                continue
            url = normalize_cat_url(base_url, href) if not href.startswith("http") else href
            m = re.search(r"[?&]categoryName=([^&]+)", url)
            cat = (m.group(1) if m else href).replace("%20", " ")
            out.append((cat, url))
    return out


def _extract_from_card(card) -> Optional[Dict]:
    price_txt = None
    name = None
    img = None

    for cand in card.css("h1,h2,h3,strong,span,p,div"):
        t = (cand.text() or "").strip()
        if not t:
            continue
        if (EUR in t) or re.search(r"\d[\d\.,]\s?€", t):
            if not price_txt:
                price_txt = t
        if not name and len(t) > 3 and "€" not in t:
            name = t

    for im in card.css("img"):
        src = im.attributes.get("src") or im.attributes.get("data-src")
        if src and src.startswith("http"):
            img = src
            break

    if not (name and price_txt):
        return None

    price, currency = parse_price(price_txt)
    return dict(name=name, price=price, currency=currency or "EUR", image_url=img or "")


def extract_tiles_from_dom(page_html: str) -> List[Dict]:
    tree = HTMLParser(page_html)
    tiles: List[Dict] = []
    for card in tree.css("article"):
        data = _extract_from_card(card)
        if data:
            tiles.append(data)
    if not tiles:
        for card in tree.css("div"):
            cls = card.attributes.get("class", "")
            if cls and "card" in cls:
                data = _extract_from_card(card)
                if data:
                    tiles.append(data)
    return tiles


def ensure_staging_schema(conn):
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
        ) VALUES (
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
def safe_text(page, sel, timeout=5000) -> str:
    page.wait_for_selector(sel, timeout=timeout)
    return page.inner_text(sel).strip()


def dismiss_update_modal(page):
    """Dismiss 'Menu updated / Menüüd uuendati' modal if present."""
    try:
        # Estonian
        if page.is_visible("text=Menüüd uuendati"):
            page.click("text=OK", timeout=2000)
            time.sleep(0.2)
        # English
        if page.is_visible("text=Menu updated"):
            page.click("text=OK", timeout=2000)
            time.sleep(0.2)
    except Exception:
        pass


def wait_for_tiles(page, req_delay: float) -> bool:
    """Scroll and check if tiles appear."""
    try:
        for _ in range(10):
            time.sleep(req_delay)
            html = page.content()
            if extract_tiles_from_dom(html):
                return True
            page.mouse.wheel(0, 1400)
        return False
    except Exception:
        return False


def run(
    city: str,
    store_name: str,
    headless: bool,
    req_delay: float,
    out_csv: str,
    upsert_db: bool,
    categories_file: Optional[str] = None,
    categories_dir: Optional[str] = None,
    deep: bool = True,
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

        store_host = slugify_host(store_name)
        slug = store_slug(store_name)

        # Prepare categories source
        cats_from_file: List[Tuple[str, str]] = []
        override_path = None
        if categories_file and os.path.isfile(categories_file):
            override_path = categories_file
        elif categories_dir:
            auto_path = os.path.join(categories_dir, city, f"{slug}.txt")
            if os.path.isfile(auto_path):
                override_path = auto_path

        base_url = None

        if override_path:
            # derive base from the first url keeping locale
            tmp = read_categories_override(override_path, page.url)
            if tmp:
                first_url = tmp[0][1]
                base_url = base_url_from_category(first_url)
                cats_from_file = tmp
                print(f"[info] using categories from: {override_path} ({len(cats_from_file)} cats)")
                print(f"[info] derived base store URL: {base_url}")
                page.goto(base_url, timeout=60_000)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(req_delay)
        else:
            # Find store by name
            try:
                # Try a couple of searchbox variants
                for sel in [
                    'input[placeholder*="Stores"]',
                    'input[placeholder*="Poed"]',
                    'input[type="search"]',
                    'input[role="searchbox"]',
                ]:
                    try:
                        page.wait_for_selector(sel, timeout=4000)
                        page.click(sel)
                        break
                    except PWTimeout:
                        continue
                page.keyboard.type(store_name)
                time.sleep(0.6)
                page.keyboard.press("Enter")
                time.sleep(1.0)

                # Click store card / title
                try:
                    page.click(f"text={store_name}", timeout=10_000)
                except PWTimeout:
                    page.click(f"xpath=//h1|//h2|//a[contains(., '{store_name}')]", timeout=10_000)

                page.wait_for_load_state("domcontentloaded")
                time.sleep(req_delay)
                base_url = page.url
            except PWTimeout:
                print(f"[warn] could not find store by name; stopping. name={store_name}", file=sys.stderr)
                context.close()
                browser.close()
                return

        # Build category list
        if cats_from_file:
            categories = cats_from_file
        else:
            store_html = page.content()
            discovered = extract_category_links(store_html)
            seen_cat = set()
            categories: List[Tuple[str, str]] = []
            for cat_name, href in discovered:
                if cat_name:
                    key = cat_name.lower()
                    if key not in seen_cat:
                        seen_cat.add(key)
                        categories.append((cat_name, normalize_cat_url(base_url, href)))
            if not categories:
                categories = [("All", base_url)]

        print(f"[info] categories selected: {len(categories)}")

        # Crawl categories
        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")
            success = False

            for attempt in range(1, 4):
                try:
                    # 1) go directly
                    page.goto(href, timeout=60_000)
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(req_delay)
                    dismiss_update_modal(page)

                    # If URL doesn't include /smc/ (i.e., we're at store home), try clicking the chip by text
                    if "/smc/" not in page.url:
                        try:
                            # category tabs across top
                            sel = f"button:has-text('{cat_name}')"
                            if page.is_visible(sel):
                                page.click(sel, timeout=4000)
                                time.sleep(req_delay)
                                dismiss_update_modal(page)
                        except Exception:
                            pass

                    if not wait_for_tiles(page, req_delay):
                        raise RuntimeError("no tiles yet")

                    html = page.content()
                    tiles = extract_tiles_from_dom(html)
                    print(f"[cat] parsed {len(tiles)} tiles")
                    for t in tiles:
                        name = (t.get("name") or "").strip()
                        if not name:
                            continue
                        price = t.get("price")
                        currency = t.get("currency") or "EUR"
                        image_url = t.get("image_url") or ""
                        size_text = guess_size(name)
                        brand = guess_brand(name)
                        ext_id = "bolt:" + hashlib.md5(
                            f"{store_host}|{name}".encode("utf-8")
                        ).hexdigest()[:16]

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
                                manufacturer=None,
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
                    success = True
                    break
                except Exception as e:
                    print(f"[cat] attempt {attempt} failed: {e}")
                    # small backoff then retry (may hit new smc id)
                    time.sleep(0.8)

            if not success:
                print(f"[cat] gave up: {cat_name}")

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
    ap.add_argument("--deep", default="1", help="(reserved) deep parse of modals for brand/manufacturer")
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

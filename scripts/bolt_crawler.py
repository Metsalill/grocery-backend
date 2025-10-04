#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coop on Bolt Food → categories → products → CSV / upsert to staging_coop_products
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
    cur = "EUR" if (EUR in t or "EUR" in t) else None
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
    out: List[Tuple[str, str]] = []
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


def base_url_from_category(url: str) -> str:
    m = re.search(r"^(https://food\.bolt\.eu/(?:[a-z]{2}-[A-Z]{2}|en-US|et-EE)/[^/]+/p/\d+)", url)
    if m:
        return m.group(1)
    u = url.split("?", 1)[0]
    return u.split("/smc/")[0]


# ----------------------- Playwright helpers -----------------------

def dismiss_popups(page):
    for sel in [
        "button:has-text('OK')",
        "button:has-text('Proovi uuesti')",
        "button[aria-label='Close']",
        "button:has-text('Got it')",
    ]:
        try:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible():
                loc.first.click()
                time.sleep(0.2)
        except Exception:
            pass


def _first_text_without_euro(raw: str) -> str:
    for line in [l.strip() for l in (raw or "").splitlines()]:
        if line and EUR not in line:
            return line
    return ""


def _style_bg_url(style: str) -> str:
    m = re.search(r'background-image:\s*url\(["\']?(.*?)["\']?\)', style or "")
    return m.group(1) if m else ""


def wait_for_grid(page, timeout=20000) -> None:
    sel = ",".join(
        [
            '[data-testid^="screens.Provider.GridMenu"]',
            '[data-testid^="components.GridMenu"]',
            '[class*="GridMenu"]',
            '[data-testid*="CategoryGridView"]',
        ]
    )
    page.wait_for_selector(sel, timeout=timeout)


def auto_scroll(page, max_steps: int = 60, pause: float = 0.25) -> None:
    """True infinite scroll to force lazy tiles to mount."""
    last_h = 0
    for _ in range(max_steps):
        page.mouse.wheel(0, 2200)
        try:
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        time.sleep(pause)
        try:
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
        except Exception:
            pass


def extract_tiles_runtime(page) -> List[Dict]:
    """
    Robust tile extractor that tolerates Bolt UI changes.
    Supports GridMenu dishes, CategoryGridView tiles and generic fallbacks.
    """
    tiles: List[Dict] = []

    candidates = page.locator(
        ",".join(
            [
                # GridMenu & ProviderDish
                '[data-testid="components.GridMenu.dishItem"] button',
                '[data-testid*="GridMenu.dishItem"] button',
                '[data-testid="components.ProviderDish.tile"]',
                '[data-testid*="ProviderDish.tile"]',

                # Category grid
                '[data-testid="components.CategoryGridView.tile"]',
                '[data-testid*="CategoryGridView.tile"]',

                # Fallbacks
                'button[aria-label][data-testid]',
                'div[role="button"][aria-label]',
                'article:has(:text("€"))',
            ]
        )
    )

    cnt = candidates.count()
    for i in range(cnt):
        try:
            el = candidates.nth(i)

            # Name
            name = (el.get_attribute("aria-label") or "").strip()
            if not name:
                title = el.locator('[data-testid="components.ProviderDish.title"], [data-testid*="ProviderDish.title"]')
                if title.count():
                    name = (title.first.inner_text() or "").strip()
            if not name:
                name = _first_text_without_euro(el.inner_text())

            # Price
            price_text = ""
            price_node = el.locator('[data-testid="components.Price"], [data-testid*="Price"]')
            if price_node.count():
                price_text = (price_node.first.inner_text() or "").strip()
            if not price_text:
                euro_nodes = el.locator(f":text-matches('.*{EUR}.*')")
                c = euro_nodes.count()
                if c:
                    texts = [(euro_nodes.nth(j).inner_text() or "").strip() for j in range(min(8, c))]
                    texts = [t for t in texts if EUR in t]
                    if texts:
                        price_text = sorted(texts, key=len)[0]
            price, currency = parse_price(price_text)

            # Image
            image_url = ""
            dish_img = el.locator('[data-testid="components.ProviderDish.dishImage"], [data-testid*="dishImage"]')
            if dish_img.count():
                style = dish_img.first.get_attribute("style") or ""
                image_url = _style_bg_url(style)

            if name and price is not None:
                tiles.append(
                    dict(
                        name=name,
                        price=price,
                        currency=currency or "EUR",
                        image_url=image_url or "",
                    )
                )
        except Exception:
            continue

    return tiles


def click_category_chip(page, cat_name: str) -> bool:
    """If we’re on the store root, click the tab/chip with the category name."""
    try:
        chip = page.locator(
            f"button:has-text('{cat_name}'), a:has-text('{cat_name}'), div[role='tab']:has-text('{cat_name}')"
        ).first
        if chip and chip.is_visible():
            chip.click()
            time.sleep(0.7)
            dismiss_popups(page)
            return True
    except Exception:
        pass
    return False


# -------------------------- DB utilities --------------------------

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
        # -------- UP S E R T  (conflict on existing PK store_host + ext_id) --------
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
        )
        ON CONFLICT (store_host, ext_id) DO UPDATE SET
          chain = EXCLUDED.chain,
          channel = EXCLUDED.channel,
          store_name = EXCLUDED.store_name,
          city_path = EXCLUDED.city_path,
          category_name = EXCLUDED.category_name,
          name = EXCLUDED.name,
          brand = COALESCE(EXCLUDED.brand, staging_coop_products.brand),
          manufacturer = COALESCE(EXCLUDED.manufacturer, staging_coop_products.manufacturer),
          size_text = COALESCE(EXCLUDED.size_text, staging_coop_products.size_text),
          price = EXCLUDED.price,
          currency = EXCLUDED.currency,
          image_url = COALESCE(EXCLUDED.image_url, staging_coop_products.image_url),
          url = EXCLUDED.url,
          description = COALESCE(EXCLUDED.description, staging_coop_products.description),
          ean_raw = COALESCE(EXCLUDED.ean_raw, staging_coop_products.ean_raw),
          scraped_at = EXCLUDED.scraped_at
        ;
        """
        with conn.cursor() as cur:
            cur.executemany(ins, rows)
        conn.commit()
    print(f"[db] upserted {len(rows)} rows into staging_coop_products")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def safe_get_text(el):
    return (el.inner_text() or "").strip()


# ------------------------------ Run -------------------------------

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
    start_url = f"https://food.bolt.eu/et-EE/{city}"
    scraped_at = dt.datetime.utcnow().isoformat()
    rows_out: List[Dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=(headless is True or str(headless) == "1"))
        context = browser.new_context(
            viewport={"width": 1366, "height": 2200},
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
        )
        page = context.new_page()

        page.goto(start_url, timeout=60_000)
        page.wait_for_load_state("domcontentloaded")
        dismiss_popups(page)

        store_host = slugify_host(store_name)
        slug = store_slug(store_name)

        # Determine categories
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
            tmp: List[str] = []
            with open(override_path, "r", encoding="utf-8") as f:
                for line in f:
                    href = line.strip()
                    if href and not href.startswith("#"):
                        tmp.append(href)
            if tmp:
                base_url = base_url_from_category(
                    tmp[0] if tmp[0].startswith("http") else normalize_cat_url(start_url, tmp[0])
                )
                cats_from_file = []
                for href in tmp:
                    url = href if href.startswith("http") else normalize_cat_url(base_url, href)
                    m = re.search(r"[?&]categoryName=([^&]+)", url)
                    cat = (m.group(1) if m else href).replace("%20", " ")
                    cats_from_file.append((cat, url))
                print(f"[info] using categories from: {override_path} ({len(cats_from_file)} cats)")
                print(f"[info] derived base store URL: {base_url}")
                page.goto(base_url, timeout=60_000)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(req_delay)
                dismiss_popups(page)
        else:
            # Search by store name
            try:
                for sel in [
                    'input[placeholder*="Otsi"]',
                    'input[placeholder*="Stores"]',
                    'input[placeholder*="Poed"]',
                    'input[type="search"]',
                    'input[role="searchbox"]',
                ]:
                    try:
                        page.wait_for_selector(sel, timeout=5000)
                        page.click(sel)
                        break
                    except PWTimeout:
                        pass

                page.keyboard.type(store_name)
                time.sleep(0.6)
                page.keyboard.press("Enter")
                time.sleep(1.0)

                try:
                    page.wait_for_selector(f"text={store_name}", timeout=30000)
                except PWTimeout:
                    page.wait_for_selector(
                        f"xpath=//h1|//h2|//a[contains(., '{store_name}')]", timeout=15000
                    )

                try:
                    page.click(f"text={store_name}", timeout=5000)
                except PWTimeout:
                    page.click(f"xpath=//h1|//h2|//a[contains(., '{store_name}')]", timeout=5000)

                page.wait_for_load_state("domcontentloaded")
                time.sleep(req_delay)
                dismiss_popups(page)
                base_url = page.url
            except PWTimeout:
                print(f"[warn] could not find store by name; stopping. name={store_name}", file=sys.stderr)
                context.close()
                browser.close()
                return

        # Collect categories
        categories: List[Tuple[str, str]] = []
        if cats_from_file:
            categories = cats_from_file
        else:
            store_html = page.content()
            discovered = extract_category_links(store_html)
            seen = set()
            for cat_name, href in discovered:
                if cat_name and cat_name.lower() not in seen:
                    seen.add(cat_name.lower())
                    categories.append((cat_name, normalize_cat_url(base_url, href)))
            if not categories:
                categories = [("All", base_url)]

        print(f"[info] categories selected: {len(categories)}")
        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")
            page.goto(href, timeout=60_000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(req_delay)
            dismiss_popups(page)

            tiles: List[Dict] = []
            for attempt in range(1, 4):
                try:
                    wait_for_grid(page, timeout=15000)
                except PWTimeout:
                    pass

                auto_scroll(page, max_steps=50, pause=0.22)

                tiles = extract_tiles_runtime(page)
                if tiles:
                    print(f"[cat] parsed {len(tiles)} tiles")
                    break

                # If 0 tiles, try clicking the category chip manually (store homepage case)
                if click_category_chip(page, cat_name):
                    time.sleep(0.7)
                    auto_scroll(page, max_steps=40, pause=0.22)
                    tiles = extract_tiles_runtime(page)
                    if tiles:
                        print(f"[cat] parsed {len(tiles)} tiles (after chip click)")
                        break

                print(f"[cat] attempt {attempt} failed: no tiles yet")
                time.sleep(0.7)
                dismiss_popups(page)

            if not tiles:
                print(f"[cat] gave up: {cat_name}")
                continue

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
                ext_id = "bolt:" + hashlib.md5(f"{store_host}|{name}".encode("utf-8")).hexdigest()[:16]

                rows_out.append(
                    dict(
                        chain=CHAIN,
                        channel=CHANNEL,
                        store_name=store_name,
                        store_host=slugify_host(store_name),
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
                    "chain","channel","store_name","store_host","city_path","category_name",
                    "ext_id","name","brand","manufacturer","size_text","price","currency",
                    "image_url","url","description","ean_raw","scraped_at",
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

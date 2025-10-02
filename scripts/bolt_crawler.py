#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coop on Bolt Food → categories → products → CSV / upsert to staging_coop_products

Key points:
- Category discovery prefers a per-store override file if present, otherwise
  discovers from the store page.
- Product cards are <button role="button" aria-label="NAME … PRICE €">.
  We read aria-labels directly and also try to pull an <img> src or a
  background-image URL for the tile.
- Auto-dismisses occasional "Menüüd uuendati / OK" dialog.
- Writes CSV and (optionally) upserts to staging_coop_products.
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
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page
from selectolax.parser import HTMLParser

# Optional DB
try:
    import psycopg
except Exception:
    psycopg = None

EUR_SYMBOL = "€"
CHAIN = "Coop"
CHANNEL = "bolt"


def slugify_host(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return f"bolt:{s}"


def store_slug(name: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", name.lower())).strip("-")


def parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    t = text.replace("\xa0", " ").strip()
    cur = "EUR" if (EUR_SYMBOL in t or "€" in t or "EUR" in t.upper()) else None
    # pick the last number block in case the label contains other digits
    m = re.findall(r"(\d[\d\.\,\s]*)(?:\s*€|$)", t)
    num = m[-1] if m else None
    if not num:
        # broad fallback
        m2 = re.findall(r"(\d+[.,]\d{1,2})", t)
        num = m2[-1] if m2 else None
    if not num:
        return None, cur
    num = num.replace(" ", "").replace(",", ".")
    try:
        return round(float(num), 2), cur
    except Exception:
        return None, cur


def guess_size(name: str) -> Optional[str]:
    m = re.search(r"(\b\d+\s?(?:g|kg|l|ml|cl|pcs|tk)\b)", name, flags=re.I)
    return m.group(1) if m else None


def guess_brand(name: str) -> Optional[str]:
    # take a plausible first capitalized token
    head = re.split(r"[–—,:]| {2,}", name)[0]
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
                m = re.search(r"categoryName=([^&]+)", href)
                if m:
                    cat = m.group(1).replace("%20", " ")
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
    m = re.search(r"^(https://food\.bolt\.eu/(?:[a-z]{2}-[A-Z]{2}|en-US)/[^/]+/p/\d+)", url)
    if m:
        return m.group(1)
    return url.split("?", 1)[0].split("/smc/", 1)[0]


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
    CREATE INDEX IF NOT EXISTS idx_stg_coop_host ON staging_coop_products(store_host);
    CREATE INDEX IF NOT EXISTS idx_stg_coop_name ON staging_coop_products (lower(name));
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def upsert_rows_to_staging_coop(rows: List[Dict], db_url: str):
    if not psycopg or not db_url:
        print("DB skip: psycopg missing or DATABASE_URL empty.", file=sys.stderr)
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


# ---------- Bolt specific helpers ----------

def click_ok_if_menu_updated(page: Page):
    """Dismiss 'Menüüd uuendati' dialog, if it appears."""
    try:
        page.locator("text=Menüüd uuendati").wait_for(timeout=2000)
        page.get_by_text("OK", exact=True).click(timeout=2000)
        time.sleep(0.2)
        print("[ui] dismissed 'Menüüd uuendati'")
    except PWTimeout:
        pass
    except Exception:
        pass


def wait_for_grid(page: Page, timeout_ms: int = 15000):
    # Something with aria-label product tiles must exist
    page.wait_for_load_state("domcontentloaded")
    try:
        page.locator('button[aria-label]').first.wait_for(timeout=timeout_ms)
    except PWTimeout:
        # sometimes the list is virtualized; try scrolling a bit to force render
        for _ in range(5):
            page.mouse.wheel(0, 2000)
            time.sleep(0.15)
        page.locator('button[aria-label]').first.wait_for(timeout=timeout_ms)


def scroll_to_load_all(page: Page, max_passes: int = 12):
    """Scroll until no new height is observed (virtualized lists)."""
    last_h = 0
    for _ in range(max_passes):
        try:
            h = page.evaluate("() => document.scrollingElement.scrollHeight")
        except Exception:
            h = 0
        if h <= last_h:
            break
        last_h = h
        page.mouse.wheel(0, 2500)
        time.sleep(0.25)


def extract_tiles_via_dom(page: Page) -> List[Dict]:
    """
    Read product tiles from aria-labels; pull img src or background-image if present.
    Returns list of dicts with: name, price, currency, image_url
    """
    js = r"""
    () => {
      const cards = Array.from(document.querySelectorAll('button[aria-label]'));
      return cards.map(btn => {
        const label = btn.getAttribute('aria-label') || '';
        // Prefer real <img> if exists, else try background-image on a nested div
        let img = '';
        const tagImg = btn.querySelector('img');
        if (tagImg && tagImg.src) img = tagImg.src;
        if (!img) {
          const bg = btn.querySelector('[style*="background-image"]');
          if (bg) {
            const s = getComputedStyle(bg).backgroundImage || '';
            const m = s.match(/^url\("?(.*?)"?\)$/);
            if (m) img = m[1];
          }
        }
        return { label, image: img };
      });
    }
    """
    raw = page.evaluate(js)
    tiles: List[Dict] = []
    for item in raw:
        label = (item.get("label") or "").strip()
        if not label:
            continue
        price, currency = parse_price(label)
        # name is label with the trailing price trimmed
        name = re.sub(r"\s*\d[\d\.\,\s]*\s*€\s*$", "", label).strip(" ,–-")
        if not name:
            # fallback: take everything before last comma/dash
            parts = re.split(r"[–—,-]", label)
            name = parts[0].strip()
        tiles.append({
            "name": name,
            "price": price,
            "currency": currency or "EUR",
            "image_url": item.get("image") or ""
        })
    return tiles


def extract_some_from_modals(page: Page, limit: int = 4) -> List[Dict]:
    """
    Very light modal scrape (first N products): click first tiles and read the title + price + image.
    Used only if aria-label parsing returned zero (or looks suspicious).
    """
    results: List[Dict] = []
    cards = page.locator('button[aria-label]')
    count = min(limit, cards.count())
    for i in range(count):
        try:
            cards.nth(i).click(timeout=3000)
            page.locator('[role="dialog"]').wait_for(timeout=4000)
            # title is usually a text element in dialog
            title = page.locator('[role="dialog"] h1, [role="dialog"] [data-testid*="headerContainer"] div').first.inner_text(timeout=3000).strip()
            # price is near the "Lisa" button area – take the last number with €
            dlg_text = page.locator('[role="dialog"]').inner_text(timeout=3000)
            price, currency = parse_price(dlg_text)
            # image
            img = ""
            try:
                img = page.locator('[role="dialog"] img').first.get_attribute("src", timeout=1500) or ""
            except Exception:
                pass
            results.append({
                "name": title,
                "price": price,
                "currency": currency or "EUR",
                "image_url": img
            })
        except Exception:
            pass
        finally:
            try:
                page.keyboard.press("Escape")
                time.sleep(0.1)
            except Exception:
                pass
    return results


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def safe_text(el):
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

        click_ok_if_menu_updated(page)

        store_host = slugify_host(store_name)
        slug = store_slug(store_name)

        # Pick category source
        override_path = None
        if categories_file and os.path.isfile(categories_file):
            override_path = categories_file
        elif categories_dir:
            auto_path = os.path.join(categories_dir, city, f"{slug}.txt")
            if os.path.isfile(auto_path):
                override_path = auto_path

        base_url = None
        cats_from_file: List[Tuple[str, str]] = []

        if override_path:
            tmp = read_categories_override(override_path, start_url)
            if tmp:
                base_url = base_url_from_category(tmp[0][1])
                cats_from_file = tmp
                print(f"[info] using categories from: {override_path} ({len(cats_from_file)} cats)")
                print(f"[info] derived base store URL: {base_url}")
                page.goto(base_url, timeout=60_000)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(req_delay)
        else:
            # Basic search open + click store by name (fallback)
            try:
                opened = False
                for sel in [
                    'input[placeholder*="Stores"]',
                    'input[placeholder*="Poed"]',
                    'input[type="search"]',
                    'input[role="searchbox"]',
                ]:
                    try:
                        page.wait_for_selector(sel, timeout=3_000)
                        page.click(sel)
                        opened = True
                        break
                    except PWTimeout:
                        pass
                if not opened:
                    try:
                        page.click("button:has(svg)", timeout=2_000)
                    except PWTimeout:
                        pass

                page.keyboard.type(store_name)
                time.sleep(0.8)
                page.keyboard.press("Enter")
                time.sleep(1.0)
                try:
                    page.wait_for_selector(f"text={store_name}", timeout=25_000)
                    page.click(f"text={store_name}", timeout=2_000)
                except PWTimeout:
                    page.click(f"xpath=//h1|//h2|//a[contains(., '{store_name}')]", timeout=4_000)

                page.wait_for_load_state("domcontentloaded")
                time.sleep(req_delay)
                base_url = page.url
            except PWTimeout:
                print(f"[warn] could not find store by name; stopping. name={store_name}", file=sys.stderr)
                context.close()
                browser.close()
                return

        # Categories
        categories: List[Tuple[str, str]] = []
        if cats_from_file:
            categories = cats_from_file
        else:
            store_html = page.content()
            discovered = extract_category_links(store_html)
            seen_cat = set()
            for cat_name, href in discovered:
                if cat_name and cat_name.lower() not in seen_cat:
                    seen_cat.add(cat_name.lower())
                    categories.append((cat_name, normalize_cat_url(base_url, href)))
            if not categories:
                categories = [("All", base_url)]

        print(f"[info] categories selected: {len(categories)}")

        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")
            page.goto(href, timeout=60_000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(req_delay)
            click_ok_if_menu_updated(page)

            # Ensure grid rendered and pre-scroll
            try:
                wait_for_grid(page, 15_000)
            except Exception:
                pass

            scroll_to_load_all(page)

            tiles = extract_tiles_via_dom(page)
            print(f"[cat] parsed {len(tiles)} tiles")

            if not tiles and deep:
                # Try modals for a few items in case aria-labels aren't present yet
                modal_tiles = extract_some_from_modals(page, limit=6)
                print(f"[cat] parsed {len(modal_tiles)} tiles via modal")
                tiles = modal_tiles

            # materialize rows
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
    ap.add_argument("--store", required=True, help="Store display name (Bolt)")
    ap.add_argument("--headless", default="1")
    ap.add_argument("--req-delay", default="0.25", type=float)
    ap.add_argument("--out", required=True)
    ap.add_argument("--upsert-db", default="1")
    ap.add_argument("--categories-file", default="", help="Optional file with category URLs (one per line)")
    ap.add_argument("--categories-dir", default="", help="Optional base dir with {dir}/{city}/{slug}.txt")
    ap.add_argument("--deep", default="1", help="Click a few modals if the grid extractor returns 0")
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

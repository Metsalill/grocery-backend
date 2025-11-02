#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bolt Food crawler (Coop venues) — now with direct DB upsert.

What this script does now:
1. Crawl Bolt Food categories for a single Coop venue (like "Wolt: Coop ..."/"Bolt: Coop ...").
2. Build a list of Product objects.
3. Write a CSV (unchanged, good for debugging / diffing).
4. 💾 If DATABASE_URL is set:
   - Look up the correct store_id in Postgres using the Bolt venue_id
     (we store that venue_id in stores.external_key for those "Wolt: Coop ..." rows).
   - Call SELECT upsert_product_and_price(...) for every scraped product.
     That auto-updates:
       products
       ext_product_map
       prices (with the right store_id)

Environment it expects in GitHub Actions:
    DATABASE_URL = postgres://...  (Railway RW URL)
Python deps:
    playwright, asyncpg
"""

import argparse
import asyncio
import csv
import datetime
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import asyncpg
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------------- regexes ---------------------- #
PRICE_RE = re.compile(r"(\d+([.,]\d{1,2})?)\s*€")
SPACE_RE = re.compile(r"\s+")
SMC_ID_RE = re.compile(r"/smc/(\d+)")
CITY_RE = re.compile(r"/et-EE/([^/]+)/p/(\d+)")
CATEGORY_NAME_Q = "categoryName"


@dataclass
class Product:
    category: str
    name: str
    price_eur: float
    unit_text: str
    image: str
    url: str
    store_url: str
    city_slug: str
    venue_id: str          # Bolt venue numeric id from /p/<venue_id>
    raw: Dict


# ---------------------- helpers ---------------------- #
def norm_space(s: str) -> str:
    return SPACE_RE.sub(" ", s or "").strip()


def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = PRICE_RE.search(text.replace("\xa0", " "))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def extract_city_and_venue(url: str) -> Tuple[str, str]:
    """
    Given something like:
      https://boltfood.com/et-EE/2-tartu/p/551?something...
    we grab:
      city_slug = "2-tartu"
      venue_id  = "551"
    """
    m = CITY_RE.search(url)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


# slug helpers for ext_id
ESTONIAN_MAP = str.maketrans({
    "ä": "a", "ö": "o", "ü": "u", "õ": "o",
    "š": "s", "ž": "z",
    "Ä": "a", "Ö": "o", "Ü": "u", "Õ": "o",
    "Š": "s", "Ž": "z",
})


def slugify_for_ext(s: str) -> str:
    """
    Turn product name or unit text into a safe-ish slug to help build a stable ext_id.
    """
    s2 = (s or "").translate(ESTONIAN_MAP).lower()
    s2 = re.sub(r"[^a-z0-9]+", "-", s2).strip("-")
    return s2


def _norm_for_match(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", slugify_for_ext(s))


def parse_categories_file(path: str) -> List[Tuple[str, str]]:
    """
    Accepts lines like:
        Piimatooted -> https://boltfood.com/et-EE/2-tartu/p/551/smc/12345?categoryName=Piimatooted&backPath=...
    or just the URL alone.
    """
    out: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "->" in line:
                name, href = [x.strip() for x in line.split("->", 1)]
                out.append((name, href))
            else:
                href = line
                name = parse_qs(urlparse(href).query).get(CATEGORY_NAME_Q, [""])[0] or "Unknown"
                out.append((name, href))
    return out


def find_categories_file(categories_dir: str, store_name: str, city: str = "") -> Optional[str]:
    """
    Resolve <categories-file> the same forgiving way we've been doing:
      1) <dir>/<city>/<slugified_store>.txt
      2) <dir>/<slugified_store>.txt
      3) recursive walk of <dir> looking for any .txt whose normalized filename matches
    """
    if not categories_dir or not store_name:
        return None

    want_slug = slugify_for_ext(store_name)
    want_norm = _norm_for_match(store_name)

    # 1) city subfolder
    if city:
        candidate = os.path.join(categories_dir, city, f"{want_slug}.txt")
        if os.path.isfile(candidate):
            return candidate

    # 2) top-level
    candidate = os.path.join(categories_dir, f"{want_slug}.txt")
    if os.path.isfile(candidate):
        return candidate

    # 3) recursive search
    best = None
    if os.path.isdir(categories_dir):
        for root, _, files in os.walk(categories_dir):
            for fn in files:
                if not fn.lower().endswith(".txt"):
                    continue
                if _norm_for_match(fn) == want_norm:
                    best = os.path.join(root, fn)
                    return best
    return best


# ---------------------- Playwright helpers ---------------------- #
def dismiss_popups(page) -> None:
    """
    Bolt sometimes drops cookie banners / app-install nags.
    We'll just try-click a bunch of obvious things.
    """
    selectors = [
        'button:has-text("Nõustun")',
        'button:has-text("Luba kõik")',
        'button:has-text("Luban kõik")',
        'button:has-text("OK")',
        'button:has-text("Accept")',
        '[data-testid="cookie-accept-all"]',
        '[aria-label="Close"]',
        'button[aria-label="close"]',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=500)
                time.sleep(0.2)
        except Exception:
            pass


def click_category_chip(page, category_name: str) -> bool:
    """
    Try to click a visible "chip" or menu link that matches `category_name`.
    """
    if not category_name:
        return False
    try:
        chip = page.get_by_role("link", name=re.compile(rf"^{re.escape(category_name)}\b", re.I))
        if chip.count():
            chip.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass
    try:
        chip = page.locator(f'//a[contains(normalize-space(.), "{category_name}")]')
        if chip.count():
            chip.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass
    return False


def open_first_category_from_hc(page) -> bool:
    """
    If Bolt lands us on something like /hc/..., open the first real smc/<id> link.
    """
    try:
        anchors = page.locator('a[href*="/smc/"]')
        if anchors.count():
            anchors.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass
    return False


def wait_for_grid(page, timeout: int = 20000) -> None:
    """
    Wait until we see product tiles. We consider multiple possible layouts,
    because Bolt keeps A/B testing.
    """
    candidates = [
        '[data-testid="components.GridMenuDishBase.button"]',
        '[data-testid="components.GridMenuDishBase.view"]',
        '[data-testid="product-card"]',
        '[data-test="product-card"]',
        '[data-testid="productTile"]',
        '[data-test="productTile"]',
        'article:has-text("€")',
        'div:has(> div >> text=/€/)',
    ]
    start = time.time()
    while (time.time() - start) * 1000 < timeout:
        for sel in candidates:
            try:
                if page.locator(sel).count():
                    return
            except Exception:
                pass
        time.sleep(0.2)
    raise PWTimeout("product grid not found")


def auto_scroll(page, max_steps: int = 60, pause: float = 0.25) -> None:
    """
    Infinite-scroll style loader.
    """
    page.evaluate(
        """
        ({ steps, pause }) => new Promise(async (res) => {
          const sleep = (ms) => new Promise(r => setTimeout(r, ms));
          for (let i = 0; i < steps; i++) {
            window.scrollBy(0, Math.round(window.innerHeight * 0.9));
            await sleep(pause * 1000);
          }
          res();
        })
        """,
        {"steps": int(max_steps), "pause": float(pause)},
    )


def extract_tiles_bolt(page) -> List[Dict]:
    """
    Bolt prefers 'GridMenuDishBase' components with testids.
    We'll grab name / price / image from that.
    """
    return page.evaluate(
        """
        () => {
          const btns = Array.from(document.querySelectorAll(
            'button[data-testid="components.GridMenuDishBase.button"],button[data-testid="components.GridMenuDishBase.view"]'
          ));
          const out = [];
          const seen = new Set();
          for (const btn of btns) {
            const nameEl  = btn.querySelector('[data-testid="components.GridMenuDishBase.title"]');
            const priceEl = btn.querySelector('[data-testid="components.GridMenuDishBase.price"]');
            const imgEl   = btn.querySelector('[data-testid="components.GridMenuDishBase.image"] img, img');
            const aria    = btn.getAttribute('aria-label') || '';

            let name = (nameEl?.textContent || aria || '').replace(/\\s+/g, ' ').trim();
            if (!nameEl && /€/.test(name)) {
              name = name.split('€')[0].trim().replace(/,\\s*$/, '');
            }

            const rawPriceText = (priceEl?.textContent || aria || '').replace(/\\s+/g, ' ').trim();
            const m = rawPriceText.match(/(\\d+(?:[.,]\\d{1,2}))\\s*€/);
            const price_text = m ? (m[1] + ' €') : '';

            const hrefEl = btn.closest('a') || btn.querySelector('a[href]');
            const href = hrefEl?.getAttribute('href') || '';
            const image = imgEl?.getAttribute('src')
                        || imgEl?.getAttribute('data-src')
                        || '';

            if (!name || !price_text) continue;

            const key = name + '|' + price_text + '|' + image;
            if (seen.has(key)) continue;
            seen.add(key);

            out.push({
              name,
              price_text,
              unit_text: '',
              image,
              href,
              text: btn.textContent || ''
            });
          }
          return out;
        }
        """
    ) or []


def extract_tiles_runtime(page) -> List[Dict]:
    """
    Generic fallback extractor if Bolt changes markup.
    """
    tiles = page.evaluate(
        """
        () => {
          const result = [];
          const cards = Array.from(document.querySelectorAll(
            '[data-testid="product-card"],[data-test="product-card"],[data-testid="productTile"],[data-test="productTile"], article, div'
          )).filter(el => /€/.test(el.textContent || ''));
          const seen = new Set();
          for (const el of cards) {
            try {
              const txt = (el.textContent || "").replace(/\\s+/g, " ").trim();
              if (!/€/.test(txt)) continue;

              let nameEl =
                el.querySelector('[data-testid="product-name"],[data-test="product-name"]') ||
                el.querySelector('h3,h4,strong') ||
                el.querySelector('div[title]');
              let name = nameEl ? (nameEl.getAttribute('title') || nameEl.textContent || '') : '';
              name = name.replace(/\\s+/g, " ").trim();

              if (!name) {
                const parts = txt.split('€')[0].trim();
                name = parts.split(' + ')[0].trim();
              }
              if (!name) continue;

              let priceEl =
                el.querySelector('[data-testid="product-price"],[data-test="product-price"]') ||
                el.querySelector('span,div');
              let priceText = priceEl ? priceEl.textContent || '' : txt;
              priceText = priceText.replace(/\\s+/g, " ").trim();
              const m = priceText.match(/(\\d+(?:[.,]\\d{1,2})?)\\s*€/);
              if (!m) continue;
              const price = m[1];

              let imgEl = el.querySelector('img');
              let img = imgEl ? (imgEl.getAttribute('src') || imgEl.getAttribute('data-src') || '') : '';

              let linkEl = el.closest('a')
                        || el.querySelector('a[href*="/p/"]')
                        || el.querySelector('a[href*="/smc/"]');
              let href = linkEl ? linkEl.getAttribute('href') : '';

              const key = name + '|' + price + '|' + img;
              if (seen.has(key)) continue;
              seen.add(key);

              result.push({
                name,
                price_text: price + ' €',
                unit_text: '',
                image: img || '',
                href: href || '',
                text: txt
              });
            } catch {}
          }
          return result;
        }
        """
    )
    return tiles or []


def ensure_on_store_page(page, base_url: str, req_delay: float = 0.3) -> None:
    """
    Make sure we're sitting on the store root (/p/<venue_id>),
    not some leftover /hc/... page.
    """
    if not base_url:
        return
    try:
        if "/p/" not in (page.url or "") or page.url.split("?")[0] != base_url:
            page.goto(base_url, timeout=60_000, wait_until="domcontentloaded")
            time.sleep(req_delay)
            dismiss_popups(page)
    except Exception:
        page.goto(base_url, timeout=60_000)
        time.sleep(req_delay)
        dismiss_popups(page)


def open_category_via_page(page, base_url: str, href: str, cat_name: str, req_delay: float = 0.3) -> bool:
    """
    Try to click category links instead of direct navigation,
    because Bolt sometimes uses client-side routing.
    """
    ensure_on_store_page(page, base_url, req_delay)

    # direct href match
    try:
        locator = page.locator(f'a[href="{href}"]')
        if locator.count():
            locator.first.click()
            page.wait_for_load_state("networkidle")
            time.sleep(req_delay)
            dismiss_popups(page)
            return True
    except Exception:
        pass

    # same smc/<id>
    try:
        m = SMC_ID_RE.search(href or "")
        if m:
            smc_id = m.group(1)
            locator = page.locator(f'a[href*="/smc/{smc_id}"]')
            if locator.count():
                locator.first.click()
                page.wait_for_load_state("networkidle")
                time.sleep(req_delay)
                dismiss_popups(page)
                return True
    except Exception:
        pass

    # chip fallback
    if click_category_chip(page, cat_name):
        time.sleep(req_delay)
        dismiss_popups(page)
        return True

    return False


# ---------------------- DB ingest ---------------------- #
async def _ingest_to_db(products: List[Product]) -> None:
    """
    Push scraped rows straight into:
      products / ext_product_map / prices
    using the DB's upsert_product_and_price() function.
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[db] DATABASE_URL not set → skipping DB ingest.")
        return

    conn = await asyncpg.connect(db_url)
    try:
        # 1. resolve Bolt venue_id -> store_id from the stores table
        #    We assume we stored each Bolt/Wolt venue's numeric ID in `stores.external_key`
        #    and chain='Coop'. (That's how those online rows like "Wolt: Coop ..." are stored.)
        venue_ids = sorted({p.venue_id for p in products if p.venue_id})
        store_map: Dict[str, int] = {}
        for v_id in venue_ids:
            row = await conn.fetchrow(
                """
                SELECT id
                FROM stores
                WHERE chain = 'Coop'
                  AND external_key = $1
                LIMIT 1;
                """,
                v_id,
            )
            if row:
                store_map[v_id] = row["id"]
            else:
                print(f"[db] WARNING: no matching store for venue_id={v_id} in stores.external_key")

        # 2. loop products and call upsert_product_and_price()
        total_inserted = 0
        for p in products:
            store_id = store_map.get(p.venue_id)
            if not store_id:
                continue  # we warned above

            # Build deterministic ext_id for this SKU inside Bolt.
            # We don't get a real EAN from Bolt, so ext_id is
            #   bolt:<venue_id>:<slug(product_name)>[:<slug(unit_text)>]
            base_slug = slugify_for_ext(p.name)[:40]
            size_slug = slugify_for_ext(p.unit_text or "")[:20]
            if size_slug:
                ext_id = f"bolt:{p.venue_id}:{base_slug}:{size_slug}"
            else:
                ext_id = f"bolt:{p.venue_id}:{base_slug}"

            # timestamp with tz for seen_at / collected_at
            seen_at_ts = datetime.datetime.now(datetime.timezone.utc)

            # upsert
            await conn.fetchval(
                """
                SELECT upsert_product_and_price(
                    $1::text,          -- in_source (we keep 'coop' so Bolt + eCoop land in same chain)
                    $2::text,          -- in_ext_id
                    $3::text,          -- in_name
                    $4::text,          -- in_brand
                    $5::text,          -- in_size_text
                    $6::text,          -- in_ean_raw (NULL ok)
                    $7::numeric,       -- in_price
                    $8::text,          -- in_currency
                    $9::integer,       -- in_store_id
                    $10::timestamptz,  -- in_seen_at
                    $11::text          -- in_source_url
                );
                """,
                "coop",                   # in_source (chain label in ext_product_map.source)
                ext_id,                   # in_ext_id
                p.name,                   # in_name
                "",                       # in_brand (Bolt doesn't expose real Coop brand cleanly)
                p.unit_text or "",        # in_size_text
                None,                     # in_ean_raw (Bolt doesn't usually expose barcodes)
                p.price_eur,              # in_price
                "EUR",                    # in_currency
                store_id,                 # in_store_id
                seen_at_ts,               # in_seen_at
                p.url or p.store_url,     # in_source_url
            )
            total_inserted += 1

        print(f"[db] upserted {total_inserted} rows via upsert_product_and_price()")
    finally:
        await conn.close()


# ---------------------- main crawl logic ---------------------- #
def crawl(categories: List[Tuple[str, str]],
          out_path: str,
          headless: bool = True,
          req_delay: float = 0.35) -> List[Product]:
    """
    Crawl all given categories from a single Bolt venue.
    Return the list of Product objects.
    """
    if not categories:
        print("No categories to crawl.")
        return []

    # Derive a stable "base_url" like https://boltfood.com/et-EE/<city_slug>/p/<venue_id>
    first_href = categories[0][1]
    if "/smc/" in first_href:
        base_url = first_href.split("/smc/")[0]
    else:
        parsed = urlparse(first_href)
        parts = parsed.path.split("/")
        try:
            p_idx = parts.index("p")
            base_url = (
                f"{parsed.scheme}://{parsed.netloc}/" +
                "/".join([x for x in parts[:p_idx + 2] if x])
            )
        except Exception:
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    city_slug, venue_id = extract_city_and_venue(base_url)
    print(f"[info] derived base store URL: {base_url}")
    print(f"[info] city={city_slug} venue={venue_id}")
    print(f"[info] categories selected: {len(categories)}")

    products: List[Product] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 2200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            timezone_id="Europe/Tallinn",
            locale="et-EE",
            geolocation={"latitude": 58.3776, "longitude": 26.7290},  # Tartu-ish
            permissions=["geolocation"],
            extra_http_headers={"Accept-Language": "et-EE,et;q=0.9,en;q=0.8"},
        )
        # light stealth
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        context.add_init_script("window.chrome = { runtime: {} };")
        context.add_init_script("Object.defineProperty(navigator, 'languages', {get: () => ['et-EE','et','en']});")
        context.add_init_script("Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});")

        page = context.new_page()
        page.set_default_timeout(30_000)

        # land on store root
        page.goto(base_url, wait_until="domcontentloaded")
        time.sleep(req_delay)
        dismiss_popups(page)

        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")

            ok = open_category_via_page(page, base_url, href, cat_name, req_delay=req_delay)
            if not ok:
                # fallback hard navigation
                try:
                    page.goto(href, timeout=60_000, wait_until="domcontentloaded")
                    time.sleep(req_delay)
                    dismiss_popups(page)
                except Exception:
                    pass

            tiles: List[Dict] = []
            for attempt in range(1, 4):
                # if Bolt dumped us back to /hc/... etc, get back to root and try again
                if "/p/" not in (page.url or ""):
                    ensure_on_store_page(page, base_url, req_delay)
                    click_category_chip(page, cat_name)

                try:
                    wait_for_grid(page, timeout=18_000)
                except PWTimeout:
                    pass

                auto_scroll(page, max_steps=50, pause=0.22)
                tiles = extract_tiles_bolt(page) or extract_tiles_runtime(page)
                if tiles:
                    print(f"[cat] parsed {len(tiles)} tiles")
                    break

                if "hc/" in (page.url or ""):
                    if open_first_category_from_hc(page):
                        auto_scroll(page, max_steps=40, pause=0.22)
                        tiles = extract_tiles_bolt(page) or extract_tiles_runtime(page)
                        if tiles:
                            print(f"[cat] parsed {len(tiles)} tiles (from hc → first category)")
                            break

                print(f"[cat] attempt {attempt} failed: no tiles yet")
                time.sleep(0.7)
                dismiss_popups(page)

            if not tiles:
                print(f"[cat] gave up: {cat_name}")
                ensure_on_store_page(page, base_url, req_delay)
                continue

            # build Product list
            for t in tiles:
                name = norm_space(t.get("name", ""))
                price_val = parse_price(t.get("price_text") or t.get("text") or "")
                unit_text = ""
                img = t.get("image", "")
                href_rel = t.get("href", "")
                if href_rel and href_rel.startswith("/"):
                    href_abs = (
                        f"{urlparse(base_url).scheme}://"
                        f"{urlparse(base_url).netloc}{href_rel}"
                    )
                else:
                    href_abs = href_rel or page.url

                if not name or price_val is None:
                    continue

                products.append(
                    Product(
                        category=cat_name,
                        name=name,
                        price_eur=price_val,
                        unit_text=unit_text,
                        image=img,
                        url=href_abs,
                        store_url=base_url,
                        city_slug=city_slug,
                        venue_id=venue_id,
                        raw=t,
                    )
                )

            # go "home" between categories so the next click works
            ensure_on_store_page(page, base_url, req_delay)

        browser.close()

    # Write CSV for debugging / archives
    if products:
        fieldnames = [
            "city_slug",
            "venue_id",
            "store_url",
            "category",
            "name",
            "price_eur",
            "unit_text",
            "image",
            "url",
            "raw_json",
        ]
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for p in products:
                w.writerow(
                    {
                        "city_slug": p.city_slug,
                        "venue_id": p.venue_id,
                        "store_url": p.store_url,
                        "category": p.category,
                        "name": p.name,
                        "price_eur": f"{p.price_eur:.2f}",
                        "unit_text": p.unit_text,
                        "image": p.image,
                        "url": p.url,
                        "raw_json": json.dumps(p.raw, ensure_ascii=False),
                    }
                )
        print(f"[done] wrote {len(products)} rows → {out_path}")
    else:
        print("[done] no products extracted")

    return products


# ---------------------- CLI ---------------------- #
def main():
    ap = argparse.ArgumentParser("bolt food store crawler")

    # new style
    ap.add_argument("--categories-file", help="File with 'Name -> URL' lines (or raw URLs)")

    # legacy style (kept for GH Actions backwards compat)
    ap.add_argument("--categories-dir", help="Root dir with per-store .txt category files")
    ap.add_argument("--city", help="City slug (like '2-tartu')", default="")
    ap.add_argument("--store", help="Store name (used to pick <categories-dir>/<city>/<slug>.txt)")
    ap.add_argument("--deep", help="ignored legacy flag", default="0")
    ap.add_argument("--upsert-db", help="ignored legacy flag (DB ingest is automatic now)", default="1")

    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--headless", type=int, default=1, help="1=headless (default), 0=show browser")
    ap.add_argument("--req-delay", type=float, default=0.35, help="Delay after navigations (seconds)")
    args = ap.parse_args()

    # figure out which categories file we should use
    categories_file = args.categories_file
    if not categories_file:
        categories_file = find_categories_file(
            args.categories_dir or "",
            args.store or "",
            args.city or "",
        )

    if not categories_file or not os.path.isfile(categories_file):
        ap.error(
            "the following arguments are required: --categories-file "
            "(or provide --categories-dir AND --store, optionally --city)"
        )

    print(f"[info] using categories file: {categories_file}")
    categories = parse_categories_file(categories_file)

    # crawl → get Product list
    products = crawl(
        categories=categories,
        out_path=args.out,
        headless=bool(args.headless),
        req_delay=args.req_delay,
    )

    # DB ingest (async) if DATABASE_URL is set
    try:
        if products:
            asyncio.run(_ingest_to_db(products))
    except Exception as e:
        print(f"[db] ingest error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bolt Food crawler (Coop venues) — now with direct DB upsert.

What this script does now:
1. Crawl Bolt Food categories for a single Coop venue (like "Bolt: Coop ...").
2. Intercepts the getDishesById API response during Playwright navigation
   to get the FULL product list per category (not just the 5 visible tiles).
3. Build a list of Product objects.
4. Write a CSV (unchanged, good for debugging / diffing).
5. If DATABASE_URL is set:
   - Look up the correct store_id in Postgres using the Bolt venue_id.
   - Call SELECT upsert_product_and_price(...) for every scraped product.

Environment it expects in GitHub Actions:
    DATABASE_URL = postgres://...  (Railway RW URL)
    STORE_ID = <stores.id>         (overrides venue_id lookup)
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
CITY_RE = re.compile(r"/et-[Ee][Ee]/([^/]+)/p/(\d+)")
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
    venue_id: str
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
    m = CITY_RE.search(url)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


ESTONIAN_MAP = str.maketrans({
    "ä": "a", "ö": "o", "ü": "u", "õ": "o",
    "š": "s", "ž": "z",
    "Ä": "a", "Ö": "o", "Ü": "u", "Õ": "o",
    "Š": "s", "Ž": "z",
})


def slugify_for_ext(s: str) -> str:
    s2 = (s or "").translate(ESTONIAN_MAP).lower()
    s2 = re.sub(r"[^a-z0-9]+", "-", s2).strip("-")
    return s2


def _norm_for_match(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", slugify_for_ext(s))


def parse_categories_file(path: str) -> List[Tuple[str, str]]:
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
    if not categories_dir or not store_name:
        return None
    want_slug = slugify_for_ext(store_name)
    want_norm = _norm_for_match(store_name)
    if city:
        candidate = os.path.join(categories_dir, city, f"{want_slug}.txt")
        if os.path.isfile(candidate):
            return candidate
    candidate = os.path.join(categories_dir, f"{want_slug}.txt")
    if os.path.isfile(candidate):
        return candidate
    if os.path.isdir(categories_dir):
        for root, _, files in os.walk(categories_dir):
            for fn in files:
                if not fn.lower().endswith(".txt"):
                    continue
                if _norm_for_match(fn) == want_norm:
                    return os.path.join(root, fn)
    return None


# ---------------------- API response parsing ---------------------- #
def _cents_to_eur(val) -> Optional[float]:
    """Convert cents integer to EUR float, or pass through if already EUR-range float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        # Bolt API returns cents for integers, but sometimes floats already in EUR range
        if isinstance(val, int) and f > 100:
            return f / 100.0
        if isinstance(val, float) and f < 100:
            return f  # already EUR
        if isinstance(val, int) and f <= 100:
            return f  # small int, assume EUR (e.g. price = 3)
        return f / 100.0
    return None


def parse_dishes_response(data: dict, cat_name: str) -> List[Dict]:
    """
    Parse a getDishesById or getMenuDishes API response into a flat list of product dicts.
    Bolt returns a nested structure: data -> sections -> items -> dishes
    """
    results = []

    def _walk(obj):
        if isinstance(obj, list):
            for item in obj:
                _walk(item)
        elif isinstance(obj, dict):
            # Check if this looks like a dish/product
            if "name" in obj and ("price" in obj or "unitPrice" in obj or "unit_price" in obj):
                name = obj.get("name") or ""
                if not isinstance(name, str):
                    name = str(name)
                name = name.strip()
                if not name:
                    return

                # Price: try various field names
                price_raw = (
                    obj.get("price")
                    or obj.get("unitPrice")
                    or obj.get("unit_price")
                    or obj.get("displayPrice")
                )
                price_eur = None
                if isinstance(price_raw, dict):
                    # {amount: 119, currency: "EUR"} style
                    amount = price_raw.get("amount") or price_raw.get("price")
                    price_eur = _cents_to_eur(amount)
                else:
                    price_eur = _cents_to_eur(price_raw)

                if price_eur is None or price_eur <= 0:
                    return

                # Image
                image = ""
                img_obj = obj.get("image") or obj.get("imageUrl") or obj.get("image_url")
                if isinstance(img_obj, str):
                    image = img_obj
                elif isinstance(img_obj, dict):
                    image = img_obj.get("url") or img_obj.get("src") or ""

                # Unit text
                unit_text = obj.get("unitText") or obj.get("unit_text") or obj.get("description") or ""
                if not isinstance(unit_text, str):
                    unit_text = ""

                results.append({
                    "name": name,
                    "price_eur": price_eur,
                    "unit_text": unit_text.strip(),
                    "image": image,
                    "category": cat_name,
                    "raw_id": obj.get("id") or obj.get("_id") or "",
                })
                return  # don't recurse into product children

            # Recurse into all values
            for v in obj.values():
                _walk(v)

    _walk(data)
    return results


# ---------------------- Playwright helpers ---------------------- #
def dismiss_popups(page) -> None:
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


def wait_for_grid(page, timeout: int = 20000) -> None:
    candidates = [
        '[data-testid="components.GridMenuDishBase.button"]',
        '[data-testid="components.GridMenuDishBase.view"]',
        '[data-testid="product-card"]',
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


def auto_scroll(page, max_steps: int = 30, pause: float = 0.2) -> None:
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
    """DOM tile fallback — used when API interception yields nothing."""
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
            if (!nameEl && /€/.test(name)) name = name.split('€')[0].trim().replace(/,\\s*$/, '');
            const rawPriceText = (priceEl?.textContent || aria || '').replace(/\\s+/g, ' ').trim();
            const m = rawPriceText.match(/(\\d+(?:[.,]\\d{1,2}))\\s*€/);
            const price_text = m ? (m[1] + ' €') : '';
            const image = imgEl?.getAttribute('src') || imgEl?.getAttribute('data-src') || '';
            if (!name || !price_text) continue;
            const key = name + '|' + price_text;
            if (seen.has(key)) continue;
            seen.add(key);
            out.push({ name, price_text, unit_text: '', image, href: '', text: btn.textContent || '' });
          }
          return out;
        }
        """
    ) or []


def ensure_on_store_page(page, base_url: str, req_delay: float = 0.3) -> None:
    if not base_url:
        return
    try:
        current = page.url or ""
        if "/p/" not in current or current.split("?")[0].rstrip("/") != base_url.rstrip("/"):
            page.goto(base_url, timeout=60_000, wait_until="domcontentloaded")
            time.sleep(req_delay)
            dismiss_popups(page)
    except Exception:
        page.goto(base_url, timeout=60_000)
        time.sleep(req_delay)
        dismiss_popups(page)


# ---------------------- DB ingest ---------------------- #
async def _ingest_to_db(products: List[Product]) -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[db] DATABASE_URL not set → skipping DB ingest.")
        return

    conn = await asyncpg.connect(db_url)
    try:
        venue_ids = sorted({p.venue_id for p in products if p.venue_id})
        store_map: Dict[str, int] = {}

        # Check for STORE_ID env override first
        env_store_id = int(os.environ.get("STORE_ID", "0") or "0")

        for v_id in venue_ids:
            if env_store_id > 0:
                store_map[v_id] = env_store_id
            else:
                row = await conn.fetchrow(
                    "SELECT id FROM stores WHERE chain = 'Coop' AND external_key = $1 LIMIT 1;",
                    v_id,
                )
                if row:
                    store_map[v_id] = row["id"]
                else:
                    print(f"[db] WARNING: no matching store for venue_id={v_id} in stores.external_key")

        total_inserted = 0
        for p in products:
            store_id = store_map.get(p.venue_id)
            if not store_id:
                continue

            base_slug = slugify_for_ext(p.name)[:40]
            size_slug = slugify_for_ext(p.unit_text or "")[:20]
            if size_slug:
                ext_id = f"bolt:{p.venue_id}:{base_slug}:{size_slug}"
            else:
                ext_id = f"bolt:{p.venue_id}:{base_slug}"

            seen_at_ts = datetime.datetime.now(datetime.timezone.utc)

            await conn.fetchval(
                """
                SELECT upsert_product_and_price(
                    $1::text, $2::text, $3::text, $4::text, $5::text,
                    $6::text, $7::numeric, $8::text, $9::integer,
                    $10::timestamptz, $11::text
                );
                """,
                "coop", ext_id, p.name, "", p.unit_text or "",
                None, p.price_eur, "EUR", store_id,
                seen_at_ts, p.url or p.store_url,
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

    if not categories:
        print("No categories to crawl.")
        return []

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
            geolocation={"latitude": 58.3776, "longitude": 26.7290},
            permissions=["geolocation"],
            extra_http_headers={"Accept-Language": "et-EE,et;q=0.9,en;q=0.8"},
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        context.add_init_script("window.chrome = { runtime: {} };")
        context.add_init_script("Object.defineProperty(navigator, 'languages', {get: () => ['et-EE','et','en']});")
        context.add_init_script("Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});")

        page = context.new_page()
        page.set_default_timeout(30_000)

        # Land on store root
        page.goto(base_url, wait_until="domcontentloaded")
        time.sleep(req_delay)
        dismiss_popups(page)

        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")

            # ----------------------------------------------------------------
            # API interception: capture getDishesById / getMenuDishes response
            # This gives us ALL products, not just the 5 visible tiles.
            # ----------------------------------------------------------------
            captured_api_data: List[dict] = []

            def _on_response(response):
                try:
                    url = response.url
                    if (
                        "getDishesById" in url
                        or "getMenuDishes" in url
                        or "getDishesByCategory" in url
                        or ("boltsvc.net" in url and "dishes" in url.lower())
                        or ("boltsvc.net" in url and "menu" in url.lower())
                    ):
                        try:
                            data = response.json()
                            captured_api_data.append(data)
                        except Exception:
                            pass
                except Exception:
                    pass

            page.on("response", _on_response)

            # Navigate to category
            try:
                page.goto(href, timeout=60_000, wait_until="domcontentloaded")
                time.sleep(req_delay)
                dismiss_popups(page)
                # Wait a bit more for XHR to fire
                page.wait_for_timeout(2000)
                auto_scroll(page, max_steps=15, pause=0.2)
                page.wait_for_timeout(1000)
            except Exception as e:
                print(f"[warn] navigation failed for {cat_name}: {e}")

            page.remove_listener("response", _on_response)

            # ----------------------------------------------------------------
            # Parse API responses first (preferred — full product list)
            # ----------------------------------------------------------------
            api_products = []
            for api_data in captured_api_data:
                parsed = parse_dishes_response(api_data, cat_name)
                api_products.extend(parsed)

            # Deduplicate by name
            seen_names = set()
            unique_api = []
            for item in api_products:
                key = item["name"].lower()
                if key not in seen_names:
                    seen_names.add(key)
                    unique_api.append(item)

            if unique_api:
                print(f"[cat] parsed {len(unique_api)} products from API response")
                for item in unique_api:
                    products.append(Product(
                        category=cat_name,
                        name=item["name"],
                        price_eur=item["price_eur"],
                        unit_text=item.get("unit_text") or "",
                        image=item.get("image") or "",
                        url=href,
                        store_url=base_url,
                        city_slug=city_slug,
                        venue_id=venue_id,
                        raw=item,
                    ))
            else:
                # ----------------------------------------------------------------
                # Fallback: DOM tile scraping (gets only visible tiles, ~5 per cat)
                # ----------------------------------------------------------------
                print(f"[cat] no API data captured, falling back to DOM tile scraping")
                wait_for_grid(page, timeout=10000)
                tiles = extract_tiles_bolt(page)
                print(f"[cat] parsed {len(tiles)} tiles from DOM")
                for t in tiles:
                    name = norm_space(t.get("name", ""))
                    price_val = parse_price(t.get("price_text") or "")
                    if not name or price_val is None:
                        continue
                    img = t.get("image", "")
                    products.append(Product(
                        category=cat_name,
                        name=name,
                        price_eur=price_val,
                        unit_text="",
                        image=img,
                        url=href,
                        store_url=base_url,
                        city_slug=city_slug,
                        venue_id=venue_id,
                        raw=t,
                    ))

            # Return to store root between categories
            ensure_on_store_page(page, base_url, req_delay)

        browser.close()

    # Write CSV
    if products:
        fieldnames = [
            "city_slug", "venue_id", "store_url", "category",
            "name", "price_eur", "unit_text", "image", "url", "raw_json",
        ]
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
            w.writeheader()
            for p in products:
                w.writerow({
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
                })
        print(f"[done] wrote {len(products)} rows → {out_path}")
    else:
        print("[done] no products extracted")

    return products


# ---------------------- CLI ---------------------- #
def main():
    ap = argparse.ArgumentParser("bolt food store crawler")
    ap.add_argument("--categories-file", help="File with 'Name -> URL' lines (or raw URLs)")
    ap.add_argument("--categories-dir")
    ap.add_argument("--city", default="")
    ap.add_argument("--store")
    ap.add_argument("--deep", default="0")
    ap.add_argument("--upsert-db", default="1")
    ap.add_argument("--out", required=True)
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--req-delay", type=float, default=0.35)
    args = ap.parse_args()

    categories_file = args.categories_file
    if not categories_file:
        categories_file = find_categories_file(
            args.categories_dir or "", args.store or "", args.city or ""
        )

    if not categories_file or not os.path.isfile(categories_file):
        ap.error("--categories-file required (or --categories-dir + --store)")

    print(f"[info] using categories file: {categories_file}")
    categories = parse_categories_file(categories_file)

    products = crawl(
        categories=categories,
        out_path=args.out,
        headless=bool(args.headless),
        req_delay=args.req_delay,
    )

    try:
        if products:
            asyncio.run(_ingest_to_db(products))
    except Exception as e:
        print(f"[db] ingest error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

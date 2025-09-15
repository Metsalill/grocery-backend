#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop eCoop (multi-region) category crawler → PDP extractor → CSV/DB-friendly

What this does
- Crawls category pages with Playwright (handles JS/lazy-load).
- Extracts title, brand, manufacturer, image, price, EAN/GTIN (from JSON-LD,
  spec tables, legacy “Tootekood”, or by clicking “Toote info” modal on new UI).
- Writes CSV (always) and optionally upserts to Postgres if COOP_UPSERT_DB=1.

DB alignment (Railway)
- Target table: public.staging_coop_products
- PRIMARY KEY (store_host, ext_id)
- Columns: store_host, ext_id, name, brand, manufacturer, ean_raw, ean_norm,
           size_text, price, currency, image_url, url, scraped_at (default now()).

Notes
- store_host is derived from --region (e.g. https://coophaapsalu.ee → coophaapsalu.ee).
- Supports **category sharding** via --cat-shards / --cat-index for parallel runs.
- Blocks heavy trackers and images/fonts to speed up.
"""

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Browser, Page

# ---------- regexes & helpers ----------

SIZE_RE = re.compile(r"(\b\d+[\,\.]?\d*\s?(?:kg|g|l|ml|tk|pcs|x|×)\s?\d*\b)", re.IGNORECASE)
DIGITS_ONLY = re.compile(r"[^0-9]")

BRAND_KEYS_ET = ["Kaubamärk", "Bränd", "Brand", "Tootja", "Valmistaja"]
EAN_KEYS_ET = ["Ribakood", "EAN", "Tootekood", "GTIN"]

CTX_EAN = re.compile(r"(?:EAN|Ribakood|Tootekood|GTIN)[^0-9]{0,12}(\d{8,14})", re.IGNORECASE)
ANY_EAN = re.compile(r"(?<!\d)(\d{8}|\d{12,14})(?!\d)")

def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def clean_digits(s: str) -> str:
    return DIGITS_ONLY.sub("", s or "")

def normalize_ean(e: Optional[str]) -> Optional[str]:
    if not e:
        return None
    d = clean_digits(e)
    if len(d) in (8, 12, 13, 14):
        if len(d) == 14 and d.startswith("0"):
            d = d[1:]               # strip logistics 0
        if len(d) == 12:
            d = "0" + d            # UPC-A → EAN-13
        return d
    return None

# ---------- page utilities ----------

async def wait_cookie_banner(page: Page):
    try:
        for sel in [
            'button:has-text("Nõustun")',
            'button:has-text("Olen nõus")',
            'button:has-text("Accept")',
            '[data-testid="accept-cookies"] button',
        ]:
            b = page.locator(sel)
            if await b.count() > 0:
                await b.first.click(timeout=1000)
                break
    except Exception:
        pass

async def collect_category_product_links(page: Page, category_url: str, page_limit: int, req_delay: float) -> List[str]:
    await page.goto(category_url, wait_until="domcontentloaded")
    await wait_cookie_banner(page)

    seen = set()
    stable_rounds = 0
    max_stable = 3

    for _ in range(1000):  # safety
        links = await page.eval_on_selector_all('a[href*="/toode/"]', "els => els.map(e => e.href)")
        for u in links:
            seen.add(u.split('#')[0])

        clicked = False
        for sel in ['button:has-text("Lae veel")', 'button:has-text("Näita rohkem")', '[data-testid="load-more"]']:
            try:
                btn = page.locator(sel)
                if await btn.count() > 0:
                    await btn.first.click()
                    await page.wait_for_timeout(int(req_delay * 1000))
                    clicked = True
                    break
            except Exception:
                pass

        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(int(req_delay * 1000))

        more = await page.eval_on_selector_all('a[href*="/toode/"]', "els => els.map(e => e.href)")
        before = len(seen)
        for u in more:
            seen.add(u.split('#')[0])
        after = len(seen)

        stable_rounds = stable_rounds + 1 if (after == before and not clicked) else 0
        if stable_rounds >= max_stable:
            break
        if page_limit > 0 and after >= page_limit:
            break

    return list(seen)

async def parse_json_ld(page: Page) -> Dict:
    data: Dict = {}
    try:
        scripts = await page.eval_on_selector_all('script[type="application/ld+json"]', "els => els.map(e => e.textContent)")
        for s in scripts:
            try:
                obj = json.loads(s)
            except Exception:
                continue
            items = obj if isinstance(obj, list) else [obj]
            for it in items:
                if isinstance(it, dict) and (it.get("@type") in ("Product", "Schema:Product", "schema:Product") or "offers" in it):
                    data.update(it)
    except Exception:
        pass
    return data

async def detect_variant(page: Page) -> str:
    """
    Best-effort PDP variant detector:
    - 'ecoop-legacy' (e.g., Haapsalu) shows 'Tootekood' inline.
    - 'ecoop-new' often has 'Toote info' which reveals GTIN in a modal.
    - 'generic' fallback.
    """
    host = (await page.evaluate("location.host")) or ""
    html = (await page.content()) or ""
    if "coophaapsalu.ee" in host or "Tootekood" in html:
        return "ecoop-legacy"
    if "Toote info" in html or "GTIN" in html:
        return "ecoop-new"
    return "generic"

async def extract_from_modal_gtin(page: Page) -> Optional[str]:
    """Click 'Toote info' and try to read GTIN from the opened sheet/modal."""
    try:
        btn = page.locator("text=Toote info")
        if await btn.count() == 0:
            return None
        await btn.first.click()
        await page.wait_for_timeout(400)

        # Try structured siblings (td/dd/div)
        val = await page.locator(
            "xpath=(//*[self::td or self::dd or self::div][contains(normalize-space(.), 'GTIN')]/following-sibling::*[1])[1]"
        ).first.text_content()
        if val:
            return val.strip()

        # Fallback: regex scan after opening
        txt = await page.content()
        m = CTX_EAN.search(txt)
        return m.group(1) if m else None
    except Exception:
        return None
    finally:
        # best-effort close
        try:
            close_btn = page.locator("button:has-text('✕'), button[aria-label='Close'], [data-testid='close']")
            if await close_btn.count() > 0:
                await close_btn.first.click()
        except Exception:
            pass

# ---------- PDP extraction ----------

async def extract_pdp(page: Page, url: str, req_delay: float, store_host: str) -> Dict:
    await page.goto(url, wait_until="domcontentloaded")
    await wait_cookie_banner(page)
    await page.wait_for_timeout(int(req_delay * 1000))

    # Name
    name = None
    for sel in ["h1", '[data-testid="product-title"]', "article h1"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                txt = await loc.first.text_content()
                if txt:
                    name = txt.strip()
                    break
        except Exception:
            pass

    ld = await parse_json_ld(page)

    # Brand / Manufacturer from JSON-LD if present
    brand = None
    manufacturer = None
    if isinstance(ld.get("brand"), dict):
        brand = ld["brand"].get("name")
    elif isinstance(ld.get("brand"), (str, int)):
        brand = str(ld["brand"])
    if isinstance(ld.get("manufacturer"), dict):
        manufacturer = ld["manufacturer"].get("name")

    # Price & currency
    price = None
    currency = None
    offers = ld.get("offers")
    if isinstance(offers, dict):
        price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
        currency = offers.get("priceCurrency") or offers.get("priceSpecification", {}).get("priceCurrency")

    # Fallback price (visible)
    if price is None:
        try:
            ptxt = await page.locator("xpath=(//*[contains(., '€') or contains(., ' EUR')])[1]").first.text_content()
            if ptxt:
                pnum = re.findall(r"[\d\.,]+", ptxt.replace("\xa0", ""))
                if pnum:
                    price = float(pnum[0].replace(",", "."))
        except Exception:
            pass
    if not currency:
        currency = "EUR"

    # Image
    image_url = None
    try:
        if ld.get("image"):
            image_url = ld["image"] if isinstance(ld["image"], str) else (ld["image"][0] if isinstance(ld["image"], list) and ld["image"] else None)
        if not image_url:
            image_url = await page.get_attribute('meta[property="og:image"]', "content")
    except Exception:
        pass

    # EAN/GTIN
    ean_raw = None
    # 1) JSON-LD
    for key in ["gtin13", "gtin", "gtin8", "gtin12"]:
        if ld.get(key):
            ean_raw = str(ld[key])
            break

    # Detect variant and apply UI-specific strategies
    variant = await detect_variant(page)

    # 2) New ecoop: click "Toote info" → GTIN
    if not ean_raw and variant == "ecoop-new":
        ean_raw = await extract_from_modal_gtin(page)

    # 3) Spec tables (dt/dd or tr/th/td)
    if not ean_raw:
        try:
            spec_xpath = (
                "xpath=//dt[normalize-space()[contains(., $key)]]/following-sibling::dd[1]"
                " | //tr[th[normalize-space()[contains(., $key)]]]/td[1]"
            )
            for key in EAN_KEYS_ET:
                loc = page.locator(spec_xpath.replace("$key", key))
                if await loc.count() > 0:
                    txt = await loc.first.text_content()
                    if txt:
                        ean_raw = txt.strip()
                        break
        except Exception:
            pass

    # 4) Legacy: 'Tootekood' label blob → next sibling
    if not ean_raw and variant == "ecoop-legacy":
        try:
            val = await page.locator(
                "xpath=(//*[contains(normalize-space(.), 'Tootekood')]/following-sibling::*[1])[1]"
            ).first.text_content()
            if val:
                ean_raw = val.strip()
        except Exception:
            pass

    # 5) Fallback: regex scan
    if not ean_raw:
        try:
            txt = await page.content()
            m = CTX_EAN.search(txt) or ANY_EAN.search(txt)
            if m:
                ean_raw = m.group(1)
        except Exception:
            pass

    # Size from name
    size_text = None
    if name:
        m = SIZE_RE.search(name)
        if m:
            size_text = m.group(1)

    # ext_id from URL
    ext_id = None
    m = re.search(r"/toode/(\d+)", url)
    if m:
        ext_id = m.group(1)

    return {
        "chain": "Coop",
        "store_host": store_host,
        "channel": "online",
        "ext_id": ext_id,
        "ean_raw": ean_raw,
        "ean_norm": normalize_ean(ean_raw),
        "name": name,
        "size_text": size_text,
        "brand": brand,
        "manufacturer": manufacturer,
        "price": float(price) if price is not None else None,
        "currency": currency,
        "image_url": image_url,
        "url": url,
    }

# ---------- category runner ----------

async def process_category(browser: Browser, category_url: str, page_limit: int, req_delay: float, pdp_workers: int, max_products: int, store_host: str) -> List[Dict]:
    page = await browser.new_page()
    items: List[Dict] = []
    try:
        links = await collect_category_product_links(page, category_url, page_limit, req_delay)
        if max_products > 0:
            links = links[:max_products]
    finally:
        await page.close()

    sem = asyncio.Semaphore(pdp_workers)

    async def worker(url: str) -> Optional[Dict]:
        async with sem:
            p = await browser.new_page()
            try:
                return await extract_pdp(p, url, req_delay, store_host)
            except Exception as e:
                print(f"[warn] PDP fail {url}: {e}")
                return None
            finally:
                await p.close()

    results = await asyncio.gather(*(worker(u) for u in links))
    for r in results:
        if r:
            items.append(r)
    return items

# ---------- outputs ----------

def write_csv(rows: List[Dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "chain", "store_host", "channel", "ext_id", "ean_raw", "ean_norm", "name",
        "size_text", "brand", "manufacturer", "price", "currency", "image_url", "url",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})

async def maybe_upsert_db(rows: List[Dict]) -> None:
    if not rows:
        return
    if os.environ.get("COOP_UPSERT_DB", "0") not in ("1", "true", "True"):
        print("[info] DB upsert disabled (COOP_UPSERT_DB != 1)")
        return
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("[info] DATABASE_URL not set; skipping DB upsert")
        return
    try:
        import asyncpg  # type: ignore
    except Exception:
        print("[warn] asyncpg not installed; skipping DB upsert")
        return

    table = "staging_coop_products"
    conn = await asyncpg.connect(dsn)
    try:
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
            table,
        )
        if not exists:
            print(f"[info] Table {table} does not exist → skipping DB upsert. (Create it manually.)")
            return

        # composite PK (store_host, ext_id)
        stmt = f"""
            INSERT INTO {table}
              (store_host, ext_id, name, brand, manufacturer, ean_raw, ean_norm, size_text, price, currency, image_url, url)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (store_host, ext_id) DO UPDATE SET
              name = EXCLUDED.name,
              brand = EXCLUDED.brand,
              manufacturer = EXCLUDED.manufacturer,
              ean_raw = EXCLUDED.ean_raw,
              ean_norm = EXCLUDED.ean_norm,
              size_text = EXCLUDED.size_text,
              price = EXCLUDED.price,
              currency = EXCLUDED.currency,
              image_url = EXCLUDED.image_url,
              url = EXCLUDED.url,
              scraped_at = now();
        """
        await conn.executemany(
            stmt,
            [
                (
                    r.get("store_host"),
                    r.get("ext_id"),
                    r.get("name"),
                    r.get("brand"),
                    r.get("manufacturer"),
                    r.get("ean_raw"),
                    r.get("ean_norm"),
                    r.get("size_text"),
                    r.get("price"),
                    r.get("currency"),
                    r.get("image_url"),
                    r.get("url"),
                )
                for r in rows
            ],
        )
        print(f"[ok] Upserted {len(rows)} rows into {table}")
    finally:
        await conn.close()

# ---------- router (blocking heavy 3rd parties) ----------

async def _route_filter(route):
    try:
        req = route.request
        # Trim heavy resource types
        if req.resource_type in ("image", "media", "font"):
            return await route.abort()
        url = req.url
        if any(h in url for h in [
            "googletagmanager.com", "google-analytics.com", "doubleclick.net",
            "facebook.net", "connect.facebook.net", "hotjar", "fullstory",
            "cdn.segment.com", "intercom",
        ]):
            return await route.abort()
        return await route.continue_()
    except Exception:
        try:
            return await route.continue_()
        except Exception:
            return

# wrapper handler for Playwright (so we can await registration)
async def _route_handler(route):
    await _route_filter(route)

# ---------- main ----------

async def run(args):
    # categories
    categories: List[str] = []
    if args.categories_multiline:
        categories.extend([ln.strip() for ln in args.categories_multiline.splitlines() if ln.strip()])
    if args.categories_file and Path(args.categories_file).exists():
        categories.extend([ln.strip() for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines() if ln.strip()])
    if not categories:
        print("[error] No category URLs provided. Pass --categories-multiline or --categories-file.")
        sys.exit(2)

    # normalize to region
    def norm_url(u: str) -> str:
        if u.startswith("http"):
            return u
        base = args.region.rstrip("/")
        if not u.startswith("/"):
            u = "/" + u
        return base + u

    categories = [norm_url(u) for u in categories]

    # optional sharding
    if args.cat_shards > 1:
        if args.cat_index < 0 or args.cat_index >= args.cat_shards:
            print(f"[error] --cat-index must be in [0, {args.cat_shards-1}]")
            sys.exit(2)
        categories = [u for i, u in enumerate(categories) if i % args.cat_shards == args.cat_index]
        print(f"[shard] Using {len(categories)} categories for shard {args.cat_index}/{args.cat_shards}")

    store_host = urlparse(args.region).netloc.lower()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=bool(int(args.headless)))
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
        )
        # IMPORTANT: await the route registration
        await context.route("**/*", _route_handler)

        all_rows: List[Dict] = []
        try:
            for cat in categories:
                print(f"[cat] {cat}")
                rows = await process_category(context, cat, args.page_limit, args.req_delay, args.pdp_workers, args.max_products, store_host)
                print(f"[info] category rows: {len(rows)}")
                all_rows.extend(rows)
        finally:
            await context.close()
            await browser.close()

    out_path = Path(args.out)
    if out_path.is_dir():
        out_path = out_path / f"coop_products_{now_stamp()}.csv"
    write_csv(all_rows, out_path)
    print(f"[ok] CSV written: {out_path}")

    await maybe_upsert_db(all_rows)

def parse_args():
    p = argparse.ArgumentParser(description="Coop eCoop category crawler → PDP extractor")
    p.add_argument("--region", default="https://vandra.ecoop.ee", help="Base region, e.g., https://vandra.ecoop.ee or https://coophaapsalu.ee")
    p.add_argument("--categories-multiline", dest="categories_multiline", default="", help="Newline-separated category URLs or paths")
    p.add_argument("--categories-file", dest="categories_file", default="", help="Path to txt file with category URLs")
    p.add_argument("--page-limit", type=int, default=0, help="Hard cap of product links per category (0=all)")
    p.add_argument("--max-products", type=int, default=0, help="Global cap per category after discovery (0=all)")
    p.add_argument("--headless", default="1", help="1/0 headless")
    p.add_argument("--req-delay", type=float, default=0.5, help="Seconds between ops")
    p.add_argument("--pdp-workers", type=int, default=4, help="Concurrent PDP tabs per category")
    # sharding
    p.add_argument("--cat-shards", type=int, default=1, help="Total number of category shards")
    p.add_argument("--cat-index", type=int, default=0, help="This shard index (0-based)")
    p.add_argument("--out", default="out/coop_products.csv", help="CSV file or output directory")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("[info] aborted by user")

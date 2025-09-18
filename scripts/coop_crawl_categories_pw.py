#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coop eCoop (multi-region) category crawler → PDP extractor → CSV/DB-friendly

- Crawls category pages with Playwright (handles JS/lazy-load).
- PDP extraction: title, brand, manufacturer (Tootja), image, price, EAN/GTIN,
  Tootekood, etc. Writes CSV; optional Postgres upsert.

DB alignment
- Target table: public.staging_coop_products
- PRIMARY KEY (store_host, ext_id)
- Columns: store_host, ext_id, name, brand, manufacturer, ean_raw, ean_norm,
           size_text, price, currency, image_url, url, scraped_at (default now()).

Notes
- store_host is derived from --region (e.g. https://coophaapsalu.ee → coophaapsalu.ee).
- Supports category sharding via --cat-shards / --cat-index for parallel runs.
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
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ---------- regexes & helpers ----------

SIZE_RE = re.compile(r"(\b\d+[\,\.]?\d*\s?(?:kg|g|l|ml|tk|pcs|x|×)\s?\d*\b)", re.IGNORECASE)
DIGITS_ONLY = re.compile(r"[^0-9]")

BRAND_KEYS_ET = ["Kaubamärk", "Bränd", "Brand"]
MANUF_KEYS_ET = ["Tootja", "Valmistaja"]
EAN_KEYS_ET = ["Ribakood", "EAN", "Tootekood", "GTIN"]

CTX_TA_CODE = re.compile(r"(?:Tootekood)\s*[:\-]?\s*(\d{8,14})", re.IGNORECASE)
CTX_MANUF   = re.compile(r"(?:Tootja|Valmistaja)\s*[:\-]?\s*([^\n<]{2,120})", re.IGNORECASE)

PRICE_TOKEN = re.compile(r"(\d+[.,]\d{2})\s*€", re.U)

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
            d = d[1:]
        if len(d) == 12:
            d = "0" + d  # UPC-A → EAN-13
        return d
    return None

def likely_brand_from_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    token = (name or "").strip().split()[0]
    token = re.sub(r"[^\w\-’'`]+", "", token)
    if 2 <= len(token) <= 24:
        return token
    return None

def looks_like_unit_price(text: str) -> bool:
    t = (text or "").strip().lower()
    return (
        "/" in t
        or "€/kg" in t or "€ / kg" in t
        or "€/l"  in t or "€ / l"  in t
        or "€/tk" in t or "€ / tk" in t
    )

def strip_query_and_fragment(u: str) -> str:
    """Drop ?query and #fragment from URL path to avoid add-to-cart etc."""
    if not u:
        return u
    u = u.split("#", 1)[0]
    u = u.split("?", 1)[0]
    return u

def same_host(u: str, host: str) -> bool:
    try:
        return urlparse(u).netloc.lower() in ("", host.lower())
    except Exception:
        return False

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

async def collect_category_product_links(page: Page, category_url: str, page_limit: int, req_delay: float, max_depth: int = 2) -> List[str]:
    """
    Collect PDP links from a category. Handles "hub" pages by descending into
    subcategories (up to max_depth). Critical fix: strip query/fragment so we never
    navigate to links like '?add-to-cart=...'.
    """
    base = urlparse(category_url)
    base_host = base.netloc

    def norm_abs(u: str) -> str:
        if not u:
            return u
        # Join relative to the current category root
        if not u.startswith("http"):
            u = urljoin(f"{base.scheme}://{base.netloc}/", u)
        # Always drop query/fragment junk (add-to-cart, sorting, etc.)
        u = strip_query_and_fragment(u)
        return u

    def is_product(u: str) -> bool:
        return "/toode/" in u

    def is_category(u: str) -> bool:
        return "/tootekategooria/" in u

    seen_products: set[str] = set()
    seen_cats: set[str] = set()
    queue: List[Tuple[str, int]] = [(category_url, 0)]

    while queue:
        url, depth = queue.pop(0)
        url = norm_abs(url)
        if not url or url in seen_cats or not same_host(url, base_host):
            continue
        seen_cats.add(url)

        try:
            await page.goto(url, wait_until="domcontentloaded")
        except Exception as e:
            print(f"[warn] category goto fail: {url} -> {e}")
            continue

        await wait_cookie_banner(page)

        # Continuous scroll + "Load more" taps to populate products on listing pages
        stable_rounds = 0
        max_stable = 3
        for _ in range(1000):
            # Gather product anchors
            try:
                hrefs = await page.eval_on_selector_all(
                    'a[href*="/toode/"]',
                    "els => els.map(e => e.href)"
                )
            except Exception:
                hrefs = []

            for h in hrefs:
                h = norm_abs(h)
                if h and is_product(h):
                    seen_products.add(h)

            # Try generic load-more controls
            clicked = False
            for sel in [
                'button:has-text("Lae veel")',
                'button:has-text("Näita rohkem")',
                '[data-testid="load-more"]',
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click()
                        await page.wait_for_timeout(int(req_delay * 1000))
                        clicked = True
                        break
                except Exception:
                    pass

            # Scroll to trigger lazy load
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
            await page.wait_for_timeout(int(req_delay * 1000))

            # Check if new products appeared
            before = len(seen_products)
            try:
                hrefs2 = await page.eval_on_selector_all(
                    'a[href*="/toode/"]',
                    "els => els.map(e => e.href)"
                )
            except Exception:
                hrefs2 = []
            for h in hrefs2:
                h = norm_abs(h)
                if h and is_product(h):
                    seen_products.add(h)
            if len(seen_products) == before and not clicked:
                stable_rounds += 1
            else:
                stable_rounds = 0

            if stable_rounds >= max_stable:
                break
            if page_limit > 0 and len(seen_products) >= page_limit:
                break

        # If this page was a hub of subcategories, traverse deeper
        if depth < max_depth:
            try:
                subcats = await page.eval_on_selector_all(
                    'a[href*="/tootekategooria/"]',
                    "els => els.map(e => e.getAttribute('href'))"
                )
            except Exception:
                subcats = []
            for sc in subcats:
                sc = norm_abs(sc or "")
                if not sc:
                    continue
                if is_category(sc) and same_host(sc, base_host):
                    queue.append((sc, depth + 1))

        if page_limit > 0 and len(seen_products) >= page_limit:
            break

    return list(seen_products)

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
                    for k, v in it.items():
                        if k not in data and v:
                            data[k] = v
    except Exception:
        pass
    return data

# ---------- price helpers ----------

async def extract_visible_price(page: Page) -> Optional[float]:
    """Pick the main price: ignore unit-price snippets like '0,02 €/tk'."""
    candidates: List[Tuple[float, str]] = []
    selectors = [
        '[data-testid="product-price"]',
        '.product-price', '.price', '.current-price', '[class*="price"]'
    ]
    for sel in selectors:
        try:
            locs = page.locator(sel)
            n = await locs.count()
            for i in range(min(n, 10)):
                txt = await locs.nth(i).inner_text()
                if not txt or looks_like_unit_price(txt):
                    continue
                m = PRICE_TOKEN.search(txt.replace("\xa0", ""))
                if m:
                    try:
                        candidates.append((float(m.group(1).replace(",", ".")), txt))
                    except Exception:
                        pass
        except Exception:
            pass

    if not candidates:
        try:
            all_txt = await page.locator("xpath=//*[contains(., '€')]").all_inner_texts()
            for txt in all_txt[:100]:
                if not txt or looks_like_unit_price(txt):
                    continue
                m = PRICE_TOKEN.search(txt.replace("\xa0", ""))
                if m:
                    try:
                        candidates.append((float(m.group(1).replace(",", ".")), txt))
                    except Exception:
                        pass
        except Exception:
            pass

    if not candidates:
        return None
    return max(candidates, key=lambda x: x[0])[0]

# ---------- PDP extraction ----------

async def extract_text_after_label(page: Page, label: str) -> Optional[str]:
    """Find text following a visible label like 'Tootekood:' or 'Tootja:'."""
    try:
        nodes = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]")
        n = await nodes.count()
        for i in range(min(n, 8)):
            html = await nodes.nth(i).inner_html()
            txt  = await nodes.nth(i).inner_text()
            m = re.search(rf"{re.escape(label)}\s*[:\-]?\s*([^\n<]{{2,120}})", txt, flags=re.I)
            if m:
                return m.group(1).strip()
            sib = await nodes.nth(i).evaluate_handle("el => el.nextElementSibling && el.nextElementSibling.textContent")
            try:
                sval = await sib.json_value()
                if sval and isinstance(sval, str) and sval.strip():
                    return sval.strip()
            except Exception:
                pass
            m2 = re.search(rf"{re.escape(label)}\s*[:\-]?\s*</[^>]+>\s*([^<]{{2,120}})", html or "", flags=re.I)
            if m2:
                return m2.group(1).strip()
    except Exception:
        pass
    return None

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

    # Brand / Manufacturer
    brand = None
    manufacturer = None
    if isinstance(ld.get("brand"), dict):
        brand = ld["brand"].get("name")
    elif isinstance(ld.get("brand"), (str, int)):
        brand = str(ld["brand"]).strip() or None
    if isinstance(ld.get("manufacturer"), dict):
        manufacturer = ld["manufacturer"].get("name")

    # Manufacturer from page (Tootja / Valmistaja)
    if not manufacturer:
        manufacturer = await extract_text_after_label(page, "Tootja") or await extract_text_after_label(page, "Valmistaja")
        if not manufacturer:
            try:
                full = await page.inner_text("body")
                m = CTX_MANUF.search(full or "")
                if m:
                    manufacturer = m.group(1).strip()
            except Exception:
                pass

    # Brand heuristic fallback
    if not brand:
        brand = likely_brand_from_name(name) or manufacturer

    # Price & currency
    price = None
    currency = None
    offers = ld.get("offers")
    if isinstance(offers, dict):
        price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
        currency = offers.get("priceCurrency") or offers.get("priceSpecification", {}).get("priceCurrency")
    if price is None:
        price = await extract_visible_price(page)
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

    # EAN/GTIN / Tootekood
    ean_raw = None
    val = await extract_text_after_label(page, "Tootekood")
    if not val:
        try:
            body_txt = await page.inner_text("body")
            m = CTX_TA_CODE.search(body_txt or "")
            if m:
                val = m.group(1)
        except Exception:
            pass
    if val:
        ean_raw = val.strip()

    if not ean_raw:
        for key in ["gtin13", "gtin", "gtin8", "gtin12"]:
            if ld.get(key):
                ean_raw = str(ld[key])
                break

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

    # Size from name
    size_text = None
    if name:
        m = SIZE_RE.search(name)
        if m:
            size_text = m.group(1)

    # ext_id selection
    ext_id = None
    if ean_raw and normalize_ean(ean_raw):
        ext_id = normalize_ean(ean_raw)
    if not ext_id:
        m = re.search(r"/toode/(\d+)", url)
        if m:
            ext_id = m.group(1)
    if not ext_id:
        m2 = re.search(r"/toode/([^/?#]+)/?", url)
        if m2:
            ext_id = m2.group(1)
    if not ext_id:
        for k in ("sku", "productID", "mpn"):
            v = ld.get(k)
            if v:
                ext_id = str(v)
                break
    if not ext_id:
        ext_id = url.rstrip("/").split("/")[-1]

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

async def process_category(browser_like: BrowserContext | Browser, category_url: str, page_limit: int, req_delay: float, pdp_workers: int, max_products: int, store_host: str) -> List[Dict]:
    page = await browser_like.new_page()
    items: List[Dict] = []
    try:
        links = await collect_category_product_links(page, category_url, page_limit, req_delay, max_depth=2)
        if max_products > 0:
            links = links[:max_products]
    finally:
        await page.close()

    sem = asyncio.Semaphore(pdp_workers)

    async def worker(url: str) -> Optional[Dict]:
        async with sem:
            p = await browser_like.new_page()
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

        stmt = f"""
            INSERT INTO {table}
              (store_host, ext_id, name, brand, manufacturer, ean_raw, ean_norm, size_text, price, currency, image_url, url)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (store_host, ext_id) DO UPDATE SET
              name         = COALESCE(EXCLUDED.name,         {table}.name),
              brand        = COALESCE(EXCLUDED.brand,        {table}.brand),
              manufacturer = COALESCE(EXCLUDED.manufacturer, {table}.manufacturer),
              ean_raw      = COALESCE(EXCLUDED.ean_raw,      {table}.ean_raw),
              ean_norm     = COALESCE(EXCLUDED.ean_norm,     {table}.ean_norm),
              size_text    = COALESCE(EXCLUDED.size_text,    {table}.size_text),
              price        = COALESCE(EXCLUDED.price,        {table}.price),
              currency     = COALESCE(EXCLUDED.currency,     {table}.currency),
              image_url    = COALESCE(EXCLUDED.image_url,    {table}.image_url),
              url          = COALESCE(EXCLUDED.url,          {table}.url),
              scraped_at   = now();
        """

        payload = []
        skipped = 0
        for r in rows:
            if not r.get("ext_id"):
                skipped += 1
                continue
            # Round price to 2 decimals before upsert (display-friendly)
            pr = r.get("price")
            pr = round(float(pr), 2) if isinstance(pr, (float, int)) else None
            payload.append((
                r.get("store_host"),
                r.get("ext_id"),
                r.get("name"),
                r.get("brand"),
                r.get("manufacturer"),
                r.get("ean_raw"),
                r.get("ean_norm"),
                r.get("size_text"),
                pr,
                r.get("currency"),
                r.get("image_url"),
                r.get("url"),
            ))
        if not payload:
            print("[warn] No rows with ext_id — skipped DB upsert")
            return

        await conn.executemany(stmt, payload)
        print(f"[ok] Upserted {len(payload)} rows into {table} (skipped {skipped} without ext_id)")
    finally:
        await conn.close()

# ---------- router (blocking heavy 3rd parties) ----------

async def _route_filter(route):
    try:
        req = route.request
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

async def _route_handler(route):
    await _route_filter(route)

# ---------- main ----------

async def run(args):
    # Read categories
    categories: List[str] = []
    if args.categories_multiline:
        categories.extend([ln.strip() for ln in args.categories_multiline.splitlines() if ln.strip()])
    if args.categories_file and Path(args.categories_file).exists():
        categories.extend([ln.strip() for ln in Path(args.categories_file).read_text(encoding="utf-8").splitlines() if ln.strip()])
    if not categories:
        print("[error] No category URLs provided. Pass --categories-multiline or --categories-file.")
        sys.exit(2)

    # Normalise base region and categories to ABSOLUTE URLs with urljoin.
    base_region = args.region.strip()
    if not re.match(r"^https?://", base_region, flags=re.I):
        base_region = "https://" + base_region
    if not base_region.endswith("/"):
        base_region += "/"

    def norm_url(u: str) -> str:
        absu = urljoin(base_region, u)  # handles both absolute and relative inputs
        return strip_query_and_fragment(absu)

    categories = [norm_url(u) for u in categories]

    # optional sharding
    if args.cat_shards > 1:
        if args.cat_index < 0 or args.cat_index >= args.cat_shards:
            print(f"[error] --cat-index must be in [0, {args.cat_shards-1}]")
            sys.exit(2)
        categories = [u for i, u in enumerate(categories) if i % args.cat_shards == args.cat_index]
        print(f"[shard] Using {len(categories)} categories for shard {args.cat_index}/{args.cat_shards}")

    store_host = urlparse(base_region).netloc.lower()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=bool(int(args.headless)))
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
            java_script_enabled=True,
        )
        await context.route("**/*", _route_handler)

        all_rows: List[Dict] = []
        try:
            for cat in categories:
                # Ensure absolute once more (belt & suspenders)
                full_cat = cat if cat.startswith("http") else norm_url(cat)
                print(f"[cat] {full_cat}")
                rows = await process_category(context, full_cat, args.page_limit, args.req_delay, args.pdp_workers, args.max_products, store_host)
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) – Category crawler → CSV (for canonical pipeline)

- Accepts a list of category URLs (file or via --cats-file).
- Iterates category pages (multiple pagination strategies).
- Collects PDP links, then opens each PDP to extract structured data.
- Writes a single CSV with the schema you already use in Rimi/Selver/Prisma flows.

CSV columns (exact order):
  store_chain,store_name,store_channel,ext_id,ean_raw,sku_raw,
  name,size_text,brand,manufacturer,price,currency,
  image_url,category_path,category_leaf,source_url

Notes:
- EAN/GTIN is extracted from JSON-LD and other script JSON when possible.
- ext_id is taken from PDP URL (digits/slug) or embedded script data.
- Pagination: tries common patterns (query param ?page=, rel=next, numbered anchors).
- Category path/leaf: taken from breadcrumbs if present; else fallback to cat URL.

Usage:
  python scripts/barbora_crawl_categories_pw.py \
    --cats-file data/barbora_categories.txt \
    --page-limit 0 --max-products 0 --headless 1 --req-delay 0.25 \
    --output-csv data/barbora_products.csv

"""

from __future__ import annotations
import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Iterable, List, Optional, Set, Tuple, Dict, Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

BASE = "https://www.barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME = "Barbora ePood"
STORE_CHANNEL = "online"

DIGITS_RE = re.compile(r"\d+")
GTIN_REGEX = re.compile(r"\b(\d{12,14})\b")
# Common PDP URL shapes we’ll accept as “product link”
PDP_PATTERNS = [
    re.compile(r"/toode/"),       # ee “product” in Estonian
    re.compile(r"/p/"),           # generic /p/12345
    re.compile(r"/product/"),     # generic
]
# extract ext_id from PDP URL if present
EXT_ID_PATTERNS = [
    re.compile(r"/p/(\d+)"),
    re.compile(r"/(\d+)(?:-[a-z0-9\-]+)?/?$"),  # trailing numeric id before optional slug
]

GTIN_KEYS = {"gtin13", "gtin", "gtin12", "gtin14", "productID", "productId", "product_id"}
SKU_KEYS  = {"sku", "SKU", "Sku"}

@dataclass
class Row:
    store_chain: str
    store_name: str
    store_channel: str
    ext_id: str
    ean_raw: str
    sku_raw: str
    name: str
    size_text: str
    brand: str
    manufacturer: str
    price: str
    currency: str
    image_url: str
    category_path: str
    category_leaf: str
    source_url: str

    def as_list(self) -> List[str]:
        return [
            self.store_chain, self.store_name, self.store_channel,
            self.ext_id, self.ean_raw, self.sku_raw,
            self.name, self.size_text, self.brand, self.manufacturer,
            self.price, self.currency,
            self.image_url, self.category_path, self.category_leaf, self.source_url
        ]

CSV_FIELDS = [
    "store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw",
    "name","size_text","brand","manufacturer","price","currency",
    "image_url","category_path","category_leaf","source_url"
]

# ----------------------- Helpers -----------------------

def norm_digits(s: Optional[str]) -> str:
    if not s:
        return ""
    return "".join(DIGITS_RE.findall(str(s)))

def safe_text(s: Optional[str]) -> str:
    return (s or "").strip()

def is_pdp_url(u: str) -> bool:
    try:
        p = urlparse(u)
        if not p.netloc:
            return False
        for rx in PDP_PATTERNS:
            if rx.search(p.path):
                return True
        # also consider /product-name-SKU123 etc. — keep conservative
        return False
    except Exception:
        return False

def url_abs(href: str, base: str = BASE) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def get_ext_id_from_url(u: str) -> str:
    path = urlparse(u).path
    for rx in EXT_ID_PATTERNS:
        m = rx.search(path)
        if m:
            return m.group(1)
    # if nothing numeric, last segment slug as fallback
    seg = path.rstrip("/").split("/")[-1]
    return seg or ""

def ldjson_blocks(page: Page) -> List[Any]:
    blocks = []
    for el in page.locator('script[type="application/ld+json"]').all():
        try:
            txt = el.inner_text().strip()
            if not txt:
                continue
            blocks.append(txt)
        except Exception:
            continue
    return blocks

def parse_json(txt: str) -> Optional[Any]:
    import json
    try:
        return json.loads(txt)
    except Exception:
        return None

def walk_find(o: Any) -> Tuple[str, str, str, str, str]:
    """
    Walk a JSON-like structure and try to pull:
      name, brand, size_text (weight/size), gtin, sku
    """
    found_name = ""
    found_brand = ""
    found_size = ""
    found_gtin = ""
    found_sku = ""

    def walk(x: Any):
        nonlocal found_name, found_brand, found_size, found_gtin, found_sku
        if isinstance(x, dict):
            # try name/title
            if not found_name:
                for k in ("name","productName","title"):
                    if k in x and isinstance(x[k], (str,int,float)):
                        found_name = str(x[k]).strip()
                        break
            # try brand
            if not found_brand:
                b = x.get("brand")
                if isinstance(b, dict):
                    nm = b.get("name")
                    if isinstance(nm, (str,int,float)):
                        found_brand = str(nm).strip()
                elif isinstance(b, (str,int,float)):
                    found_brand = str(b).strip()
            # try size/weight
            for k in ("size","sizeText","weight","netWeight","packageSize","size_text"):
                if not found_size and k in x and isinstance(x[k], (str,int,float)):
                    found_size = str(x[k]).strip()
                    break
            # gtin
            if not found_gtin:
                for k in GTIN_KEYS:
                    if k in x and isinstance(x[k], (str,int,float)):
                        found_gtin = str(x[k]).strip()
                        break
            # sku
            if not found_sku:
                for k in SKU_KEYS:
                    if k in x and isinstance(x[k], (str,int,float)):
                        found_sku = str(x[k]).strip()
                        break
            # offers nested
            if "offers" in x and isinstance(x["offers"], (dict,list)):
                walk(x["offers"])
            # image sometimes nested
            for v in x.values():
                if isinstance(v, (dict,list)):
                    walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(o)
    return found_name, found_brand, found_size, found_gtin, found_sku

def extract_from_jsonld(page: Page) -> Tuple[str,str,str,str]:
    """
    Return (name, brand, size_text, ean_raw_or_empty, sku_raw)
    """
    for raw in ldjson_blocks(page):
        data = parse_json(raw)
        if data is None:
            continue
        name, brand, size_text, gtin, sku = walk_find(data)
        if any([name, brand, size_text, gtin, sku]):
            return safe_text(name), safe_text(brand), safe_text(size_text), safe_text(gtin), safe_text(sku)
    return "", "", "", "", ""

def extract_from_other_scripts(page: Page) -> Tuple[str,str,str,str]:
    """
    Scan other <script> JSON blobs for product data as a fallback
    """
    scripts = page.locator('script:not([type="application/ld+json"])').all()
    for s in scripts:
        try:
            txt = s.inner_text().strip()
        except Exception:
            continue
        if not txt or ("{" not in txt and "[" not in txt):
            continue
        # try to find a JSON object quickly (heuristic)
        for m in re.finditer(r"(\{.*\}|\[.*\])", txt, re.DOTALL):
            blob = m.group(1)
            data = parse_json(blob)
            if data is None:
                continue
            name, brand, size_text, gtin, sku = walk_find(data)
            if any([name, brand, size_text, gtin, sku]):
                return safe_text(name), safe_text(brand), safe_text(size_text), safe_text(gtin), safe_text(sku)
    return "", "", "", ""

def extract_price_currency(page: Page) -> Tuple[str, str]:
    """
    Try JSON-LD first (offers.price, offers.priceCurrency); then meta; then visible price spans.
    """
    # JSON-LD offers
    for raw in ldjson_blocks(page):
        data = parse_json(raw)
        if not data:
            continue
        def get_offer(o: Any) -> Tuple[str,str]:
            price = ""
            curr = ""
            if isinstance(o, dict):
                p = o.get("price") or o.get("priceSpecification", {}).get("price")
                if isinstance(p, (str,int,float)):
                    price = str(p)
                c = o.get("priceCurrency") or o.get("priceSpecification", {}).get("priceCurrency")
                if isinstance(c, (str,int,float)):
                    curr = str(c)
            return price, curr

        if isinstance(data, dict) and "offers" in data:
            offers = data["offers"]
            if isinstance(offers, list):
                for it in offers:
                    price, curr = get_offer(it)
                    if price:
                        return price, curr or "EUR"
            else:
                price, curr = get_offer(offers)
                if price:
                    return price, curr or "EUR"

    # Meta / visible fallback (site-specific tuning may be needed)
    try:
        # Look for common price containers
        price_el = page.locator('[data-testid*="price"], .price, .product-price, [itemprop="price"]').first
        if price_el and price_el.count() > 0:
            txt = price_el.inner_text().strip()
            # normalize "1,29 €" → "1.29"
            val = re.sub(r"[^0-9,\.]", "", txt).replace(",", ".")
            val = re.findall(r"\d+(?:\.\d+)?", val)
            if val:
                return val[0], "EUR"
    except Exception:
        pass
    return "", ""

def extract_image_url(page: Page) -> str:
    # JSON-LD
    for raw in ldjson_blocks(page):
        data = parse_json(raw)
        if not data:
            continue
        def pull(o: Any) -> Optional[str]:
            if isinstance(o, dict):
                im = o.get("image")
                if isinstance(im, str):
                    return im
                if isinstance(im, list) and im and isinstance(im[0], str):
                    return im[0]
                # nested
                for v in o.values():
                    if isinstance(v, (dict,list)):
                        r = pull(v)
                        if r: return r
            elif isinstance(o, list):
                for it in o:
                    r = pull(it)
                    if r: return r
            return None
        r = pull(data)
        if r:
            return url_abs(r, BASE)

    # Visible image fallback
    try:
        # primary product image candidates
        img = page.locator('img[alt*="toode"], img[alt*="produkt"], img[alt*="product"], img').first
        if img and img.count() > 0:
            src = img.get_attribute("src") or ""
            if src:
                return url_abs(src, BASE)
    except Exception:
        pass
    return ""

def extract_breadcrumbs(page: Page) -> Tuple[str, str]:
    """
    Return (category_path, category_leaf). Best effort via breadcrumb nav.
    """
    try:
        crumbs = []
        # Try common breadcrumb navs
        for sel in [
            'nav[aria-label*="crumb"], .breadcrumb, [data-testid*="breadcrumb"]'
        ]:
            loc = page.locator(f"{sel} a, {sel} span, {sel} li")
            if loc.count() > 0:
                for i in range(loc.count()):
                    t = safe_text(loc.nth(i).inner_text())
                    if t and t not in crumbs:
                        crumbs.append(t)
                break
        # Clean very short crumbs
        crumbs = [c for c in crumbs if len(c) > 1]
        if crumbs:
            return " / ".join(crumbs), crumbs[-1]
    except Exception:
        pass
    return "", ""

def discover_pdp_links_on_category(page: Page) -> List[str]:
    """
    Collect product card links from a category page.
    Adjust the selectors if Barbora uses different classes.
    """
    links: Set[str] = set()
    candidates = page.locator("a").all()
    for a in candidates:
        href = ""
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            continue
        if not href:
            continue
        absu = url_abs(href, BASE)
        if is_pdp_url(absu):
            links.add(absu)
    return list(links)

def next_category_page_url(current_url: str, page: Page, page_index: int) -> Optional[str]:
    """
    Try to compute next page URL.
    Strategies:
      1) ?page=N param increment
      2) <a rel="next"> href
      3) specific numbered links with 'active' class → pick next
    """
    # 1) explicit ?page=
    try:
        u = urlparse(current_url)
        qs = parse_qs(u.query)
        cur = 1
        if "page" in qs and qs["page"]:
            try:
                cur = int(qs["page"][0])
            except Exception:
                cur = page_index
        else:
            cur = page_index
        nxt = cur + 1
        nq = dict(qs)
        nq["page"] = [str(nxt)]
        new_q = urlencode({k: v[0] if isinstance(v, list) and len(v)==1 else v for k, v in nq.items()}, doseq=True)
        candidate = urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
        # Only use if page actually changes (we'll let caller test by loading it)
        return candidate
    except Exception:
        pass

    # 2) rel="next"
    try:
        next_el = page.locator('a[rel="next"], link[rel="next"]').first
        if next_el and next_el.count() > 0:
            href = next_el.get_attribute("href") or ""
            if href:
                return url_abs(href, current_url)
    except Exception:
        pass

    # 3) numbered paging (very heuristic)
    try:
        pagers = page.locator('a[href*="page="], .pagination a').all()
        active_index = None
        items = []
        for i, el in enumerate(pagers):
            t = safe_text(el.inner_text())
            h = el.get_attribute("href") or ""
            items.append((t, h))
            if "active" in (el.get_attribute("class") or ""):
                active_index = i
        if active_index is not None and active_index + 1 < len(items):
            t, h = items[active_index + 1]
            if h:
                return url_abs(h, current_url)
        else:
            # fallback: pick highest page + 1? risky — skip
            pass
    except Exception:
        pass

    return None

def read_categories(args) -> List[str]:
    cats: List[str] = []
    if args.cats_file and os.path.isfile(args.cats_file):
        with open(args.cats_file, "r", encoding="utf-8") as f:
            for line in f:
                u = safe_text(line)
                if u:
                    cats.append(url_abs(u, BASE))
    return cats

# ----------------------- PDP extraction -----------------------

def extract_pdp(page: Page, source_url: str, category_hint: str) -> Row:
    name, brand, size_text, ean_raw, sku_raw = extract_from_jsonld(page)
    if not any([name, brand, size_text, ean_raw, sku_raw]):
        # fallback to other scripts
        _n, _b, _s, _g = extract_from_other_scripts(page)
        name = name or _n
        brand = brand or _b
        size_text = size_text or _s
        ean_raw = ean_raw or _g

    # price & currency
    price, currency = extract_price_currency(page)

    # image
    image_url = extract_image_url(page)

    # breadcrumbs → category path/leaf
    cat_path, cat_leaf = extract_breadcrumbs(page)
    if not cat_path and category_hint:
        cat_path = category_hint
        # leaf = last segment of path or URL hint
        cat_leaf = category_hint.split("/")[-1] if "/" in category_hint else category_hint

    ext_id = get_ext_id_from_url(source_url)

    return Row(
        store_chain=STORE_CHAIN,
        store_name=STORE_NAME,
        store_channel=STORE_CHANNEL,
        ext_id=ext_id,
        ean_raw=ean_raw,
        sku_raw=sku_raw,
        name=name,
        size_text=size_text,
        brand=brand,
        manufacturer="",     # rarely present; keep empty
        price=price,
        currency=currency or "EUR",
        image_url=image_url,
        category_path=cat_path,
        category_leaf=cat_leaf,
        source_url=source_url
    )

# ----------------------- Main crawl -----------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cats-file", help="File with category URLs (one per line)")
    ap.add_argument("--page-limit", default="0", help="Max pages per category (0 = all)")
    ap.add_argument("--max-products", default="0", help="Hard cap on total PDPs (0 = unlimited)")
    ap.add_argument("--headless", default="1", help="Headless (1/0)")
    ap.add_argument("--req-delay", default="0.25", help="Delay between steps (sec)")
    ap.add_argument("--output-csv", default="data/barbora_products.csv", help="CSV output path")
    ap.add_argument("--skip-ext-file", default="", help="Optional file with ext_id list to skip")
    args = ap.parse_args()

    cats = read_categories(args)
    if not cats:
        print("[barbora] No categories provided. Provide --cats-file.", file=sys.stderr)
        sys.exit(2)

    headless = args.headless.strip() != "0"
    req_delay = float(args.req_delay)
    page_limit = int(args.page_limit or "0")
    max_products = int(args.max_products or "0")
    skip_ids: Set[str] = set()

    if args.skip_ext_file and os.path.isfile(args.skip_ext_file):
        with open(args.skip_ext_file, "r", encoding="utf-8") as f:
            for line in f:
                s = safe_text(line)
                if s:
                    skip_ids.add(s)

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)
    out = open(args.output_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(out)
    writer.writerow(CSV_FIELDS)

    total_written = 0
    seen_pdp: Set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(base_url=BASE)
        page = ctx.new_page()

        for cat in cats:
            try:
                print(f"[barbora] Category: {cat}", file=sys.stderr)
                current_url = cat
                cat_pages = 0
                while True:
                    cat_pages += 1
                    if page_limit and cat_pages > page_limit:
                        break

                    try:
                        page.goto(current_url, timeout=45000, wait_until="domcontentloaded")
                    except PWTimeout:
                        print(f"[barbora] timeout on {current_url}", file=sys.stderr)
                        break
                    except Exception as e:
                        print(f"[barbora] nav error on {current_url}: {e}", file=sys.stderr)
                        break

                    # discover PDP links
                    pdp_links = discover_pdp_links_on_category(page)
                    if not pdp_links:
                        # no links found → stop paging this category
                        print(f"[barbora] no PDP links on page: {current_url}", file=sys.stderr)

                    # visit PDPs
                    for u in pdp_links:
                        if max_products and total_written >= max_products:
                            break
                        if u in seen_pdp:
                            continue
                        seen_pdp.add(u)

                        ext_id = get_ext_id_from_url(u)
                        if ext_id and ext_id in skip_ids:
                            continue

                        # open PDP in a temp tab to reduce memory
                        p = ctx.new_page()
                        try:
                            p.goto(u, timeout=45000, wait_until="domcontentloaded")
                            row = extract_pdp(p, u, category_hint=cat)
                            writer.writerow(row.as_list())
                            total_written += 1
                        except PWTimeout:
                            print(f"[barbora] PDP timeout: {u}", file=sys.stderr)
                        except Exception as e:
                            print(f"[barbora] PDP error on {u}: {e}", file=sys.stderr)
                        finally:
                            try:
                                p.close()
                            except Exception:
                                pass

                        if max_products and total_written >= max_products:
                            break

                        time.sleep(req_delay)

                    if max_products and total_written >= max_products:
                        break

                    # try to compute next page
                    nxt = next_category_page_url(current_url, page, cat_pages)
                    if not nxt or nxt == current_url:
                        break

                    # naive guard: if loading nxt yields same content url, we bail next loop
                    current_url = nxt
                    time.sleep(req_delay)

            except Exception as e:
                print(f"[barbora] category error: {cat} -> {e}", file=sys.stderr)
                continue

        page.close()
        ctx.close()
        browser.close()

    out.close()
    print(f"[barbora] done. rows={total_written} -> {args.output_csv}", file=sys.stderr)


if __name__ == "__main__":
    main()

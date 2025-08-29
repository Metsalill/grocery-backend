#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) category crawler → PDP extractor → CSV/DB friendly
- Robust price extraction (JSON-LD, meta, globals, visible text; handles "3 99 €").
- Strict breadcrumb parsing (real PDP breadcrumbs only).
- Category → subcategory discovery and paging/scroll fallbacks.
- Skips already-priced PDPs when --skip-ext-file is provided.
- Single Chromium instance for all PDPs (big speedup).
"""

from __future__ import annotations
import argparse, os, re, csv, json, sys, traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

STORE_CHAIN   = "Rimi"
STORE_NAME    = "Rimi ePood"
STORE_CHANNEL = "online"
BASE = "https://www.rimi.ee"

# ------------------------------- regexes -------------------------------------

EAN_RE = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE = re.compile(r"(\d{1,5}(?:[.,]\d{1,2}|\s?\d{2})?)\s*€")

SKU_KEYS = {"sku","mpn","itemNumber","productCode","code","id","itemid"}
EAN_KEYS = {"ean","ean13","gtin","gtin13","barcode"}
PRICE_KEYS = {"price","currentprice","priceamount","unitprice","value"}
CURR_KEYS  = {"currency","pricecurrency","currencycode","curr"}

# ------------------------------- utils ---------------------------------------

def norm_price_str(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    if " " in s and s.replace(" ", "").isdigit() and len(s.replace(" ", "")) >= 3:
        digits = s.replace(" ", "")
        s = f"{digits[:-2]}.{digits[-2:]}"
    return s.replace(",", ".")

def deep_find_kv(obj: Any, keys: set) -> Dict[str, str]:
    out: Dict[str, str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str, int, float)):
                    out[lk] = str(v)
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)
    walk(obj)
    return out

def normalize_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.split("?")[0].split("#")[0]
    return href if href.startswith("http") else urljoin(BASE, href)

def auto_accept_overlays(page) -> None:
    labels = [
        r"Nõustun", r"Nõustu", r"Accept", r"Allow all", r"OK", r"Selge",
        r"Jätka", r"Vali hiljem", r"Continue", r"Close", r"Sulge",
        r"Vali pood", r"Vali teenus", r"Telli koju", r"Vali kauplus",
        r"Näita kõiki tooteid", r"Kuva tooted", r"Kuva kõik tooted",
    ]
    for lab in labels:
        try:
            page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=800)
            page.wait_for_timeout(120)
        except Exception:
            pass

def wait_for_hydration(page, timeout_ms: int = 8000) -> None:
    try:
        page.wait_for_function(
            """() => {
                const main = document.querySelector('main');
                const hidden = main && getComputedStyle(main).visibility === 'hidden';
                const cards = document.querySelector('.js-product-container a.card__url, a[href*="/p/"]');
                return (main && !hidden) || !!cards;
            }""",
            timeout=timeout_ms
        )
    except Exception:
        pass

# ----------------------------- parsing helpers --------------------------------

def parse_price_from_dom_or_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for sel in [
        'meta[itemprop="price"]',
        'meta[property="product:price:amount"]',
        'meta[property="og:price:amount"]',
    ]:
        for tag in soup.select(sel):
            val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
            if val:
                return norm_price_str(val), "EUR"

    tag = soup.find(attrs={"itemprop": "price"})
    if tag:
        val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
        if val:
            return norm_price_str(val), "EUR"

    m = MONEY_RE.search(soup.get_text(" ", strip=True))
    if m:
        return norm_price_str(m.group(1)), "EUR"
    return None, None

def parse_brand_and_size(soup: BeautifulSoup, name: str) -> Tuple[Optional[str], Optional[str]]:
    brand = size_text = None
    for row in soup.select("table tr"):
        th, td = row.find("th"), row.find("td")
        if not th or not td:
            continue
        key = th.get_text(" ", strip=True).lower()
        val = td.get_text(" ", strip=True)
        if (not brand) and ("tootja" in key or "brand" in key):
            brand = val
        if (not size_text) and any(k in key for k in ("kogus","maht","netokogus","pakend","neto","suurus")):
            size_text = val
    if not size_text:
        m = re.search(r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk))\b', name or "")
        if m: size_text = m.group(1).replace("L", "l")
    return brand, size_text

def extract_ext_id(url: str) -> str:
    try:
        parts = urlparse(url).path.rstrip("/").split("/")
        if "p" in parts:
            i = parts.index("p"); return parts[i+1]
    except Exception:
        pass
    return ""

def parse_jsonld_for_product_and_breadcrumbs(soup: BeautifulSoup) -> Tuple[Dict[str,Any], List[str]]:
    flat: Dict[str, Any] = {}
    crumbs: List[str] = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.text)
        except Exception:
            continue
        seq = data if isinstance(data, list) else [data]
        for d in seq:
            if isinstance(d, dict) and d.get("@type") in ("Product", ["Product"]):
                offers = d.get("offers")
                if isinstance(offers, dict):
                    if "price" in offers: flat["price"] = offers.get("price")
                    if "priceCurrency" in offers: flat["currency"] = offers.get("priceCurrency")
                elif isinstance(offers, list) and offers:
                    of0 = offers[0]
                    if isinstance(of0, dict):
                        if "price" in of0: flat["price"] = of0.get("price")
                        if "priceCurrency" in of0: flat["currency"] = of0.get("priceCurrency")
                for k in ("gtin13","gtin","ean","ean13","barcode","sku","mpn"):
                    if k in d and d.get(k):
                        flat[k] = d.get(k)
            if isinstance(d, dict) and d.get("@type") in ("BreadcrumbList", ["BreadcrumbList"]):
                try:
                    items = d.get("itemListElement") or []
                    names = []
                    for it in items:
                        if isinstance(it, dict):
                            t = it.get("name") or (it.get("item") or {}).get("name")
                            if not t and isinstance(it.get("item"), str):
                                t = it.get("item").split("/")[-1]
                            if t:
                                names.append(str(t).strip())
                    if names:
                        crumbs = names
                except Exception:
                    pass
    return flat, crumbs

def parse_visible_for_ean(soup: BeautifulSoup) -> Optional[str]:
    for el in soup.find_all(string=EAN_LABEL_RE):
        seg = el.parent.get_text(" ", strip=True) if el and el.parent else str(el)
        m = EAN_RE.search(seg)
        if m: return m.group(0)
    m = EAN_RE.search(soup.get_text(" ", strip=True))
    return m.group(0) if m else None

# ---------------------------- collectors --------------------------------------

def collect_pdp_links(page) -> List[str]:
    sels = [
        ".js-product-container a.card__url",
        "a[href*='/p/']",
        "a[href^='/epood/ee/p/']",
        "a[href^='/epood/ee/tooted/'][href*='/p/']",
        "[data-test*='product'] a[href*='/p/']",
        ".product-card a[href*='/p/']",
    ]
    hrefs: set[str] = set()
    for sel in sels:
        try:
            for el in page.locator(sel).all():
                h = normalize_href(el.get_attribute("href"))
                if h and "/p/" in h:
                    hrefs.add(h)
        except Exception:
            pass
    return sorted(hrefs)

def collect_subcategory_links(page, base_cat_url: str) -> List[str]:
    sels = [
        "nav[aria-label='categories'] a[href^='/epood/ee/tooted/']",
        "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
        ".category-card a[href^='/epood/ee/tooted/']",
        ".category, .subcategory a[href^='/epood/ee/tooted/']",
        "a[href^='/epood/ee/tooted/']:not([href*='/p/'])",
    ]
    hrefs: set[str] = set()
    for sel in sels:
        try:
            for el in page.locator(sel).all():
                h = normalize_href(el.get_attribute("href"))
                if h and "/epood/ee/tooted/" in h and "/p/" not in h:
                    hrefs.add(h)
        except Exception:
            pass
    hrefs.discard(base_cat_url.split("?")[0].split("#")[0])
    return sorted(hrefs)

# ---------------------------- crawler -----------------------------------------

def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float) -> List[str]:
    browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
    ctx = browser.new_context(
        locale="et-EE",
        viewport={"width":1440, "height":900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124 Safari/537.36"),
    )
    page = ctx.new_page()
    visited: set[str] = set()
    q: List[str] = [normalize_href(cat_url) or cat_url]
    all_pdps: List[str] = []

    try:
        while q:
            cat = q.pop(0)
            if not cat or cat in visited:
                continue
            visited.add(cat)

            try:
                page.goto(cat, timeout=45000, wait_until="domcontentloaded")
            except Exception:
                continue
            auto_accept_overlays(page)
            wait_for_hydration(page)

            for sc in collect_subcategory_links(page, cat):
                if sc not in visited:
                    q.append(sc)

            pages_seen = 0
            last_total = -1
            while True:
                all_pdps.extend(collect_pdp_links(page))

                clicked = False
                for sel in [
                    "a[rel='next']",
                    "button[aria-label*='Järgmine']",
                    "button:has-text('Järgmine')",
                    "button:has-text('Kuva rohkem')",
                    "button:has-text('Laadi rohkem')",
                    "a:has-text('Järgmine')",
                ]:
                    if page.locator(sel).count() > 0:
                        try:
                            page.locator(sel).first.click(timeout=3000)
                            clicked = True
                            page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                            break
                        except Exception:
                            pass

                if not clicked:
                    before = len(collect_pdp_links(page))
                    for _ in range(3):
                        page.mouse.wheel(0, 2400)
                        page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                    after = len(collect_pdp_links(page))
                    if after <= before:
                        break

                pages_seen += 1
                if page_limit and pages_seen >= page_limit:
                    break
                if len(all_pdps) == last_total:
                    page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                last_total = len(all_pdps)

    finally:
        ctx.close(); browser.close()

    # dedupe preserving order
    seen, out = set(), []
    for u in all_pdps:
        if u and u not in seen:
            seen.add(u); out.append(u)
    return out

# --------------------------- PDP parser (reused page) --------------------------

def parse_pdp_with_page(page, url: str, req_delay: float) -> Dict[str,str]:
    name = brand = size_text = image_url = ""
    ean = sku = price = currency = None
    category_path = ""
    ext_id_from_attr = ""

    try:
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        auto_accept_overlays(page)
        wait_for_hydration(page)
        page.wait_for_timeout(int(max(req_delay, 0.1)*1000))

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        ogimg = soup.find("meta", {"property":"og:image"})
        if ogimg and ogimg.get("content"):
            image_url = ogimg.get("content") or ""
        else:
            img = soup.find("img")
            if img:
                image_url = img.get("src") or img.get("data-src") or ""
        if image_url:
            image_url = normalize_href(image_url) or ""

        flat_ld, crumbs_ld = parse_jsonld_for_product_and_breadcrumbs(soup)
        if flat_ld.get("price") and not price:
            price = norm_price_str(str(flat_ld.get("price")))
            currency = currency or (flat_ld.get("currency") or "EUR")
        for k in ("gtin13","ean","ean13","barcode","gtin"):
            if not ean and flat_ld.get(k):
                ean = str(flat_ld.get(k))
        for k in ("sku","mpn"):
            if not sku and flat_ld.get(k):
                sku = str(flat_ld.get(k))

        crumbs_dom = [a.get_text(strip=True) for a in soup.select("nav[aria-label='breadcrumb'] a, .breadcrumbs a, .breadcrumb a, ol.breadcrumb a") if a.get_text(strip=True)]
        crumbs = crumbs_dom or crumbs_ld
        if crumbs:
            crumbs = [c for c in crumbs if c]
            category_path = " > ".join(crumbs[-5:])

        b2, s2 = parse_brand_and_size(soup, name or "")
        brand = brand or b2
        size_text = size_text or s2

        if not ean or not sku:
            for it in ("gtin13","gtin","ean","ean13","barcode","sku","mpn"):
                meta = soup.find(attrs={"itemprop": it})
                if meta:
                    val = (meta.get("content") or meta.get_text(strip=True))
                    if not val: continue
                    if it in ("gtin13","gtin","ean","ean13","barcode") and not ean:
                        ean = val
                    if it in ("sku","mpn") and not sku:
                        sku = val

        if not price:
            p, c = parse_price_from_dom_or_meta(soup)
            price, currency = p or price, c or currency

        for glb in [
            "__NUXT__", "__NEXT_DATA__", "APP_STATE", "dataLayer",
            "Storefront", "CART_CONFIG", "__APOLLO_STATE__", "APOLLO_STATE",
            "apolloState", "__INITIAL_STATE__", "__PRELOADED_STATE__", "__STATE__"
        ]:
            try:
                data = page.evaluate(f"window['{glb}']")
            except Exception:
                data = None
            if not data:
                continue
            got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS, *PRICE_KEYS, *CURR_KEYS })
            if not ean:
                for k in ("gtin13","ean","ean13","barcode","gtin"):
                    if got.get(k): ean = got.get(k); break
            if not sku:
                for k in ("sku","mpn","code","id"):
                    if got.get(k): sku = got.get(k); break
            if not price:
                for k in ("price","currentprice","priceamount","value","unitprice"):
                    if got.get(k):
                        price = norm_price_str(got.get(k)); break
            if not currency:
                for k in ("currency","pricecurrency","currencycode","curr"):
                    if got.get(k):
                        currency = got.get(k); break

        if not ean:
            e2 = parse_visible_for_ean(soup)
            if e2: ean = e2

        if not currency and price:
            currency = "EUR"

    except PWTimeout:
        name = name or ""

    ext_id = extract_ext_id(url) or ext_id_from_attr

    return {
        "store_chain": STORE_CHAIN,
        "store_name": STORE_NAME,
        "store_channel": STORE_CHANNEL,
        "ext_id": ext_id,
        "ean_raw": (ean or "").strip(),
        "sku_raw": (sku or "").strip(),
        "name": (name or "").strip(),
        "size_text": (size_text or "").strip(),
        "brand": (brand or "").strip(),
        "manufacturer": "",
        "price": (str(price) if price is not None else "").strip(),
        "currency": (currency or "").strip(),
        "image_url": (image_url or "").strip(),
        "category_path": (category_path or "").strip(),
        "category_leaf": category_path.split(" > ")[-1] if category_path else "",
        "source_url": url.split("?")[0],
    }

# ------------------------------- IO -------------------------------------------

def read_categories(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

def read_skip_file(path: Optional[str]) -> tuple[set[str], set[str]]:
    """
    Returns (skip_urls, skip_ext_ids)
    File may contain full PDP URLs and/or bare ext_ids (one per line).
    """
    skip_urls: set[str] = set()
    skip_ext: set[str] = set()
    if not path or not os.path.exists(path):
        return skip_urls, skip_ext
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            if s.startswith("http"):
                skip_urls.add(s.split("?")[0].split("#")[0])
                xid = extract_ext_id(s)
                if xid:
                    skip_ext.add(xid)
            else:
                skip_ext.add(s)
    return skip_urls, skip_ext

def write_csv(rows: List[Dict[str,str]], out_path: str) -> None:
    fields = [
        "store_chain","store_name","store_channel",
        "ext_id","ean_raw","sku_raw","name","size_text","brand","manufacturer",
        "price","currency","image_url","category_path","category_leaf","source_url",
    ]
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    new_file = not os.path.exists(out_path)
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,"") for k in fields})

# -------------------------------- main ----------------------------------------

def main():
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--cats-file", required=True, help="File with category URLs (one per line)")
    ap.add_argument("--page-limit", default="0")
    ap.add_argument("--max-products", default="0")
    ap.add_argument("--headless", default="1")
    ap.add_argument("--req-delay", default="0.5")
    ap.add_argument("--output-csv", default=os.environ.get("OUTPUT_CSV","data/rimi_products.csv"))
    ap.add_argument("--skip-ext-file", default=os.environ.get("SKIP_EXT_FILE",""))
    args = ap.parse_args()

    page_limit   = int(args.page_limit or "0")
    max_products = int(args.max_products or "0")
    headless     = (str(args.headless or "1") != "0")
    req_delay    = float(args.req_delay or "0.5")
    cats         = read_categories(args.cats_file)
    skip_urls, skip_ext = read_skip_file(args.skip_ext_file)

    all_pdps: List[str] = []
    with sync_playwright() as pw:
        # 1) collect PDP URLs from categories
        for cat in cats:
            try:
                print(f"[rimi] {cat}")
                pdps = crawl_category(pw, cat, page_limit, headless, req_delay)
                all_pdps.extend(pdps)
                if max_products and len(all_pdps) >= max_products:
                    break
            except Exception as e:
                print(f"[rimi] category error: {cat} → {e}", file=sys.stderr)

        # dedupe keep order
        seen, q = set(), []
        for u in all_pdps:
            if u not in seen:
                seen.add(u); q.append(u)

        # 2) filter with skip list
        if skip_urls or skip_ext:
            q2 = []
            skipped = 0
            for url in q:
                if (url in skip_urls) or (extract_ext_id(url) in skip_ext):
                    skipped += 1
                    continue
                q2.append(url)
            print(f"[rimi] skip filter: {skipped} URLs skipped (already priced).")
            q = q2

        # 3) single browser/context/page for all PDPs
        browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
        ctx = browser.new_context(
            locale="et-EE",
            viewport={"width":1440,"height":900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
        )
        page = ctx.new_page()

        rows, total = [], 0
        for i, url in enumerate(q, 1):
            try:
                row = parse_pdp_with_page(page, url, req_delay)
                rows.append(row); total += 1
                if len(rows) >= 120:   # slightly bigger batch, fewer fsyncs
                    write_csv(rows, args.output_csv); rows = []
            except Exception:
                traceback.print_exc()
            if max_products and total >= max_products:
                break

        if rows:
            write_csv(rows, args.output_csv)

        ctx.close(); browser.close()

    print(f"[rimi] wrote {total} product rows.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) → category crawler (Playwright) → PDP extractor → CSV

Highlights
- Robust product-link discovery (.js-product-container .card__url, .card__info a, any a[href*="/p/"])
- Hydration wait (main hidden → visible OR product cards present)
- Infinite scroll & "Load more / Next" buttons until no growth
- PDP parser: name, brand, size, EAN/SKU (JSON-LD, microdata, window globals, visible text), price & currency
- Safe CLI (no argparse exit 2), headless flag, page caps, product caps, delay, custom cats file & output path
"""

from __future__ import annotations
import os, re, csv, json, sys, time, traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

STORE_CHAIN   = "Rimi"
STORE_NAME    = "Rimi ePood"
STORE_CHANNEL = "online"
BASE = "https://www.rimi.ee"

EAN_RE = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE = re.compile(r"(\d+[.,]\d+)\s*€")
SKU_KEYS = {"sku","mpn","itemNumber","productCode","code","id","itemid"}
EAN_KEYS = {"ean","ean13","gtin","gtin13","barcode"}

# ---------- utils ------------------------------------------------------------

def deep_find_kv(obj: Any, keys: set) -> Dict[str,str]:
    out: Dict[str,str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k,v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str,int,float)):
                    out[lk] = str(v)
                walk(v)
        elif isinstance(x, list):
            for i in x: walk(i)
    walk(obj)
    return out

def normalize_href(href: Optional[str]) -> Optional[str]:
    if not href: return None
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

def wait_for_hydration(page, timeout_ms: int = 10000) -> None:
    try:
        page.wait_for_function(
            """() => {
                const main = document.querySelector('main');
                const hidden = main && getComputedStyle(main).visibility === 'hidden';
                const haveCards =
                  !!document.querySelector('.js-product-container a.card__url') ||
                  !!document.querySelector('.card__info a') ||
                  !!document.querySelector('a[href*="/p/"]');
                return (main && !hidden) || haveCards;
            }""",
            timeout=timeout_ms
        )
    except Exception:
        pass

def scroll_until_stable(page, min_cycles: int = 2, max_cycles: int = 20, sleep_s: float = 0.6) -> None:
    """Scroll to bottom until item count stops growing."""
    def count_cards():
        try:
            return page.evaluate(
                "document.querySelectorAll('.js-product-container, .card, [data-gtm-eec-product]').length"
            )
        except Exception:
            return 0
    seen = -1
    same = 0
    for _ in range(max_cycles):
        page.mouse.wheel(0, 2400)
        page.wait_for_timeout(int(sleep_s * 1000))
        cur = count_cards()
        if cur == seen:
            same += 1
            if same >= min_cycles:
                break
        else:
            same = 0
            seen = cur

# ---------- parsing ----------------------------------------------------------

def parse_price_from_dom_or_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for tag in soup.select('meta[itemprop="price"], [itemprop="price"]'):
        val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
        if val:
            return val.replace(",", "."), "EUR"
    m = MONEY_RE.search(soup.get_text(" ", strip=True))
    if m:
        return m.group(1).replace(",", "."), "EUR"
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

def parse_jsonld_and_microdata(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    ean = sku = None
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        try:
            data = json.loads(tag.text)
        except Exception:
            continue
        seq = data if isinstance(data, list) else [data]
        for d in seq:
            got = deep_find_kv(d, { *EAN_KEYS, *SKU_KEYS })
            ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
            sku = sku or got.get("sku") or got.get("mpn") or got.get("code")
    if not ean:
        for it in ("gtin13","gtin","ean","ean13","barcode"):
            meta = soup.find(attrs={"itemprop": it})
            if meta:
                ean = ean or (meta.get("content") or meta.get_text(strip=True))
    if not sku:
        for it in ("sku","mpn"):
            meta = soup.find(attrs={"itemprop": it})
            if meta:
                sku = sku or (meta.get("content") or meta.get_text(strip=True))
    return ean, sku

def parse_visible_for_ean(soup: BeautifulSoup) -> Optional[str]:
    for el in soup.find_all(string=EAN_LABEL_RE):
        seg = el.parent.get_text(" ", strip=True) if el and el.parent else str(el)
        m = EAN_RE.search(seg)
        if m: return m.group(0)
    m = EAN_RE.search(soup.get_text(" ", strip=True))
    return m.group(0) if m else None

# ---------- collectors -------------------------------------------------------

def collect_pdp_links(page) -> List[str]:
    sels = [
        # most reliable first:
        ".js-product-container a.card__url",
        ".card__info a[href*='/p/']",
        "a.card__url[href*='/p/']",
        # any anchor to PDP:
        "a[href*='/epood/ee/p/']",
        "a[href^='/epood/ee/tooted/'][href*='/p/']",
        "a[href*='/p/']",
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
        "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
        ".category-card a[href^='/epood/ee/tooted/']",
        ".category, .subcategory a[href^='/epood/ee/tooted/']",
        "nav a[href^='/epood/ee/tooted/']",
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

# ---------- crawler ----------------------------------------------------------

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
            page.wait_for_timeout(int(req_delay * 1000))

            # enqueue subcategories (if any)
            for sc in collect_subcategory_links(page, cat):
                if sc not in visited:
                    q.append(sc)

            # collect pdps with pager/scroll fallback
            pages_seen = 0
            last_count = -1
            while True:
                # infinite scroll until stable
                scroll_until_stable(page, sleep_s=max(0.3, req_delay))
                all_pdps.extend(collect_pdp_links(page))

                # try next / load-more controls
                clicked = False
                for sel in [
                    "a[rel='next']",
                    "button[aria-label*='Järgmine']",
                    "button:has-text('Järgmine')",
                    "button:has-text('Kuva rohkem')",
                    "button:has-text('Laadi rohkem')",
                    "a:has-text('Järgmine')",
                ]:
                    try:
                        btn = page.locator(sel).first
                        if btn and btn.count() > 0 and btn.is_enabled():
                            btn.click(timeout=2500)
                            clicked = True
                            page.wait_for_timeout(int(max(0.3, req_delay) * 1000))
                            break
                    except Exception:
                        pass

                pages_seen += 1
                cur = len(all_pdps)
                if (not clicked) or (cur == last_count):
                    break
                last_count = cur
                if page_limit and pages_seen >= page_limit:
                    break

    finally:
        ctx.close(); browser.close()

    # dedupe preserving order
    seen, out = set(), []
    for u in all_pdps:
        if u and u not in seen:
            seen.add(u); out.append(u)
    return out

# ---------- PDP parser -------------------------------------------------------

def parse_pdp(pw, url: str, headless: bool, req_delay: float) -> Dict[str,str]:
    browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
    ctx = browser.new_context(
        locale="et-EE",
        viewport={"width":1440,"height":900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
    )
    page = ctx.new_page()
    name = brand = size_text = image_url = category_path = ""
    ean = sku = price = currency = None
    ext_id_from_attr = ""

    try:
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        auto_accept_overlays(page)
        wait_for_hydration(page)
        page.wait_for_timeout(int(req_delay*1000))

        # quick data from container if present
        try:
            card = page.locator(".js-product-container").first
            if card.count() > 0:
                raw = card.get_attribute("data-gtm-eec-product")
                if raw:
                    try:
                        eec = json.loads(raw)
                        if isinstance(eec, dict):
                            if eec.get("price") is not None: price = str(eec.get("price"))
                            currency = eec.get("currency") or currency
                            brand = eec.get("brand") or brand
                            sku = sku or str(eec.get("id") or "")
                    except Exception:
                        pass
                dp = card.get_attribute("data-product-code")
                if dp:
                    ext_id_from_attr = dp.strip()
        except Exception:
            pass

        soup = BeautifulSoup(page.content(), "lxml")

        # name & image
        h1 = soup.find("h1")
        if h1: name = h1.get_text(strip=True)
        ogimg = soup.find("meta", {"property":"og:image"})
        if ogimg and ogimg.get("content"):
            image_url = normalize_href(ogimg.get("content") or "")
        else:
            img = soup.find("img")
            if img:
                image_url = normalize_href(img.get("src") or img.get("data-src") or "")

        # breadcrumb
        crumbs = [a.get_text(strip=True) for a in soup.select("nav a, .breadcrumb a") if a.get_text(strip=True)]
        if crumbs:
            crumbs = [c for c in crumbs if c]
            category_path = " > ".join(crumbs[-5:])

        # brand & size
        b2, s2 = parse_brand_and_size(soup, name or "")
        brand = brand or b2
        size_text = size_text or s2

        # ean/sku via ld+json/microdata and globals
        e1, s1 = parse_jsonld_and_microdata(soup)
        ean = ean or e1; sku = sku or s1

        for glb in ["__NUXT__","__NEXT_DATA__","APP_STATE","dataLayer","Storefront","CART_CONFIG"]:
            try:
                data = page.evaluate(f"window['{glb}']")
                if data:
                    got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS })
                    ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
                    sku = sku or got.get("sku") or got.get("mpn") or got.get("code") or got.get("id")
                    if not price and ("price" in got): price = got.get("price")
                    if not currency and ("currency" in got): currency = got.get("currency")
            except Exception:
                pass

        if not ean:
            e2 = parse_visible_for_ean(soup)
            if e2: ean = e2

        if not price:
            price, currency = parse_price_from_dom_or_meta(soup)

    except PWTimeout:
        name = name or ""
    finally:
        ctx.close(); browser.close()

    # prefer URL /p/<id>, else data-product-code
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

# ---------- CLI / main -------------------------------------------------------

def parse_args_safe():
    # argparse that never exit(2) in Actions logs
    import argparse
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--cats-file")
    p.add_argument("--page-limit", default="0")
    p.add_argument("--max-products", default="0")
    p.add_argument("--headless", default="1")
    p.add_argument("--req-delay", default="0.5")
    p.add_argument("--output-csv", default="rimi_products.csv")
    try:
        a, _ = p.parse_known_args()
    except Exception:
        class A: pass
        a = A(); a.cats_file=None; a.page_limit="0"; a.max_products="0"; a.headless="1"; a.req_delay="0.5"; a.output_csv="rimi_products.csv"
    return a

def main():
    args = parse_args_safe()
    cats_file = args.cats_file or "data/rimi_categories.txt"
    page_limit = int(str(args.page_limit or "0") or "0")
    max_products = int(str(args.max_products or "0") or "0")
    headless = (str(args.headless or "1") == "1")
    req_delay = float(str(args.req_delay or "0.5") or "0.5")
    out_csv = args.output_csv or "rimi_products.csv"

    # load categories
    seeds: List[str] = []
    if os.path.exists(cats_file):
        with open(cats_file, "r", encoding="utf-8") as f:
            for ln in f:
                ln = (ln or "").strip()
                if ln and not ln.startswith("#"):
                    seeds.append(normalize_href(ln) or ln)
    if not seeds:
        seeds = ["https://www.rimi.ee/epood/ee/tooted/leivad-saiad-kondiitritooted"]

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    fieldnames = [
        "store_chain","store_name","store_channel",
        "ext_id","ean_raw","sku_raw","name","size_text","brand","manufacturer",
        "price","currency","image_url","category_path","category_leaf","source_url",
    ]

    total_rows = 0
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        with sync_playwright() as pw:
            all_pdps: List[str] = []
            for cu in seeds:
                pdps = crawl_category(pw, cu, page_limit, headless, req_delay)
                print(f"[rimi] {cu} → +{len(pdps)} products (total so far: {len(all_pdps)+len(pdps)})")
                all_pdps.extend(pdps)
                if max_products and len(all_pdps) >= max_products:
                    all_pdps = all_pdps[:max_products]
                    break

            for i, pu in enumerate(all_pdps, 1):
                try:
                    row = parse_pdp(pw, pu, headless, req_delay)
                    if row.get("ext_id") and row.get("name") and row.get("price"):
                        w.writerow(row); total_rows += 1
                except Exception:
                    traceback.print_exc(limit=1)

    print(f"[rimi] wrote {total_rows} product rows.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

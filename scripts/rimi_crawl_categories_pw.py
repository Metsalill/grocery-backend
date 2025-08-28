#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) category crawler → PDP extractor → CSV

- Resilient CLI for GitHub Actions (no argparse exit 2).
- Category → subcategory discovery + product links (pagination & infinite scroll).
- PDP parser uses DOM, JSON-LD, microdata, and window globals.
- Safe defaults; always produces a CSV (at least a header).
"""

from __future__ import annotations
import os, re, csv, json, sys, traceback, argparse
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

# ------------------------------- utils --------------------------------------

def deep_find_kv(obj: Any, keys: set) -> Dict[str,str]:
    out: Dict[str,str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k,v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str,int,float,str)):
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

def wait_for_hydration(page, timeout_ms: int = 9000) -> None:
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

# ----------------------------- parsing --------------------------------------

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

# ---------------------------- collectors ------------------------------------

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

# ---------------------------- crawler ---------------------------------------

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

            # discover subcategories
            for sc in collect_subcategory_links(page, cat):
                if sc not in visited:
                    q.append(sc)

            # collect PDP links with pager/scroll fallbacks
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
                    try:
                        if page.locator(sel).count() > 0:
                            page.locator(sel).first.click(timeout=2500)
                            clicked = True
                            page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
                            break
                    except Exception:
                        pass

                if not clicked:
                    before = len(collect_pdp_links(page))
                    for _ in range(3):
                        page.mouse.wheel(0, 2200)
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

# --------------------------- PDP parser -------------------------------------

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

        # Quick data from product container (if present)
        try:
            card = page.locator(".js-product-container").first
            if card.count() > 0:
                raw = card.get_attribute("data-gtm-eec-product")
                if raw:
                    try:
                        eec = json.loads(raw)
                        if isinstance(eec, dict):
                            price = str(eec.get("price")) if eec.get("price") is not None else price
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

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        # name, image
        h1 = soup.find("h1")
        if h1: name = h1.get_text(strip=True)
        ogimg = soup.find("meta", {"property":"og:image"})
        if ogimg and ogimg.get("content"):
            image_url = ogimg.get("content")
        else:
            img = soup.find("img")
            if img:
                image_url = img.get("src") or img.get("data-src") or ""
        if image_url:
            image_url = normalize_href(image_url)

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
                    got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS, "price","currency" })
                    ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
                    sku = sku or got.get("sku") or got.get("mpn") or got.get("code") or got.get("id")
                    if not price and ("price" in got):
                        price = got.get("price")
                    if not currency and ("currency" in got):
                        currency = got.get("currency")
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

# ------------------------------ main ----------------------------------------

FIELDS = [
    "store_chain","store_name","store_channel",
    "ext_id","ean_raw","sku_raw","name","size_text","brand","manufacturer",
    "price","currency","image_url","category_path","category_leaf","source_url",
]

def safe_parse_cli(argv: List[str]) -> dict:
    """Parse CLI but never let argparse kill the process with exit 2."""
    defaults = {
        "cats_file": "data/rimi_categories.txt",
        "page_limit": 0,
        "max_products": 0,
        "headless": 1,
        "req_delay": 0.5,
        "output_csv": "data/rimi_products.csv",
    }
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--cats-file", dest="cats_file", default=defaults["cats_file"])
    parser.add_argument("--page-limit", dest="page_limit", default=str(defaults["page_limit"]))
    parser.add_argument("--max-products", dest="max_products", default=str(defaults["max_products"]))
    parser.add_argument("--headless", dest="headless", default=str(defaults["headless"]))
    parser.add_argument("--req-delay", dest="req_delay", default=str(defaults["req_delay"]))
    parser.add_argument("--output-csv", dest="output_csv", default=defaults["output_csv"])
    try:
        ns = parser.parse_args(argv)
        out = {
            "cats_file": str(ns.cats_file),
            "page_limit": int(str(ns.page_limit) or "0"),
            "max_products": int(str(ns.max_products) or "0"),
            "headless": bool(int(str(ns.headless) or "1")),
            "req_delay": float(str(ns.req_delay) or "0.5"),
            "output_csv": str(ns.output_csv or defaults["output_csv"]),
        }
        return out
    except SystemExit:
        return defaults
    except Exception:
        print("[rimi] CLI parse failed; using defaults.", file=sys.stderr)
        return defaults

def read_categories(path: str) -> List[str]:
    out: List[str] = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = (ln or "").strip()
                if ln and not ln.startswith("#"):
                    out.append(ln)
    if not out:
        out = [f"{BASE}/epood/ee/tooted/leivad-saiad-kondiitritooted"]
    # unique, preserve order
    seen, uniq = set(), []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

def ensure_csv(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()

def main(argv: List[str]) -> int:
    cfg = safe_parse_cli(argv)
    cats = read_categories(cfg["cats_file"])
    os.makedirs("data", exist_ok=True)
    dbg_dir = "data/rimi_debug"
    os.makedirs(dbg_dir, exist_ok=True)
    ensure_csv(cfg["output_csv"])

    total_rows = 0
    all_pdps: List[str] = []

    try:
        with sync_playwright() as pw:
            # Discover product URLs from all categories
            for ci, cat in enumerate(cats, 1):
                try:
                    pdps = crawl_category(
                        pw,
                        cat_url=cat,
                        page_limit=cfg["page_limit"],
                        headless=cfg["headless"],
                        req_delay=cfg["req_delay"],
                    )
                    all_pdps.extend(pdps)
                    print(f"[rimi] {cat} → +{len(pdps)} products (total so far: {len(all_pdps)})")
                except Exception as e:
                    print(f"[rimi] category fail {cat}: {type(e).__name__}: {e}")
                    try:
                        # best-effort page screenshot already closed in crawl; skip
                        pass
                    except Exception:
                        pass

            # Dedup pdps while preserving order
            seen, pdp_list = set(), []
            for u in all_pdps:
                if u not in seen:
                    seen.add(u); pdp_list.append(u)

            # Hard cap
            if cfg["max_products"] and len(pdp_list) > cfg["max_products"]:
                pdp_list = pdp_list[: cfg["max_products"]]

            # Open CSV for appending rows
            with open(cfg["output_csv"], "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS)

                # Parse each PDP
                for i, url in enumerate(pdp_list, 1):
                    try:
                        row = parse_pdp(pw, url, cfg["headless"], cfg["req_delay"])
                        if row.get("ext_id") and row.get("name") and row.get("price"):
                            w.writerow(row)
                            total_rows += 1
                        else:
                            # keep a tiny breadcrumb for debug
                            print(f"[rimi] skip (incomplete): {url}")
                    except Exception as e:
                        print(f"[rimi] PDP fail {i}/{len(pdp_list)}: {type(e).__name__}: {e}")
                        # do not crash the whole run
                        continue

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("[rimi] FATAL:", type(e).__name__, str(e))
        traceback.print_exc()

    print(f"[rimi] wrote {total_rows} product rows.")
    # Always 0 to let the workflow carry on; debugging is done via logs/artifacts
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

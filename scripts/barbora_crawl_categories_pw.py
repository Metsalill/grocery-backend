#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) – Category crawler → CSV (for canonical pipeline)

CSV columns (exact order):
  store_chain,store_name,store_channel,ext_id,ean_raw,sku_raw,
  name,size_text,brand,manufacturer,price,currency,
  image_url,category_path,category_leaf,source_url
"""
from __future__ import annotations
import argparse, csv, os, re, sys, json
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple, Dict, Any
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

BASE = "https://barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME  = "Barbora ePood"
STORE_CHANNEL = "online"

DIGITS_RE = re.compile(r"\d+")
PDP_PATTERNS = [re.compile(r"/toode/"), re.compile(r"/p/"), re.compile(r"/product/")]

# ext_id patterns (prefer numeric if present; else last path seg/slug)
EXT_ID_PATTERNS = [
    re.compile(r"/p/(\d+)"),
    re.compile(r"/(\d+)(?:-[a-z0-9\-]+)?/?$"),
]

GTIN_KEYS = {"gtin13","gtin","gtin12","gtin14","productid","product_id","barcode","ean","ean13"}
SKU_KEYS  = {"sku","mpn","code","id","productcode","itemnumber"}
PRICE_KEYS= {"price","currentprice","priceamount","unitprice","value","amount"}
CURR_KEYS = {"currency","pricecurrency","currencycode","curr"}
BRAND_KEYS= {"brand","manufacturer","producer","tootja"}

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
        return [self.store_chain,self.store_name,self.store_channel,
                self.ext_id,self.ean_raw,self.sku_raw,self.name,self.size_text,
                self.brand,self.manufacturer,self.price,self.currency,
                self.image_url,self.category_path,self.category_leaf,self.source_url]

CSV_FIELDS = ["store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw",
              "name","size_text","brand","manufacturer","price","currency",
              "image_url","category_path","category_leaf","source_url"]

# ----------------------- tiny helpers -----------------------
def safe_text(s: Optional[str]) -> str: return (s or "").strip()
def norm_digits(s: Optional[str]) -> str: return "".join(DIGITS_RE.findall(str(s or "")))
def norm_price_str(s: Optional[str]) -> str:
    s = safe_text(s)
    if not s: return s
    s = s.replace("\xa0", " ").replace(",", ".")
    m = re.search(r"\d+(?:\.\d{1,2})?", s)
    return m.group(0) if m else s
def is_pdp_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return bool(p.netloc) and any(rx.search(p.path) for rx in PDP_PATTERNS)
    except Exception:
        return False
def url_abs(href: str, base: str = BASE) -> str:
    try: return urljoin(base, href)
    except Exception: return href

def ext_id_from_url(u: str) -> str:
    path = urlparse(u).path
    for rx in EXT_ID_PATTERNS:
        m = rx.search(path)
        if m: return m.group(1)
    seg = path.rstrip("/").split("/")[-1]
    return seg or ""

def keyset_for_url(u: str) -> Set[str]:
    """
    Build multiple comparable keys for matching against skip/only files:
    - full absolute URL (canonicalized)
    - just the path
    - numeric id (if present)
    - last path slug
    """
    try:
        uu = urlparse(url_abs(u, BASE))
        full = urlunparse((uu.scheme, uu.netloc, uu.path.rstrip("/"), "", "", ""))
        path = uu.path.rstrip("/")
    except Exception:
        full = u.strip().rstrip("/")
        path = full

    keys = {full, path}
    # numeric id
    m = re.search(r"/(\d+)(?:-[a-z0-9\-]+)?/?$", path)
    if m: keys.add(m.group(1))
    # slug
    slug = path.rsplit("/", 1)[-1]
    if slug: keys.add(slug)
    return keys

# ----------------------- name validation + DOM title fallback -----------------------
_BAD_NAME_TOKENS = {"inst-badge", "badge", "widget", "component", "modal", "breadcrumb", "product"}
def looks_like_product_name(s: str) -> bool:
    s = safe_text(s)
    if not s: return False
    low = s.lower()
    if low in _BAD_NAME_TOKENS: return False
    if len(s) < 6: return False
    if not re.search(r"[A-Za-zÄÖÜÕäöüõ]", s): return False
    # avoid pure techy tokens
    if re.fullmatch(r"[a-z0-9\-_]+", low): return False
    return True

def dom_title_fallback(page: Page) -> str:
    # 1) H1-ish
    try:
        h1 = page.locator("h1, .b-product h1, [data-testid='product-name']").first
        if h1 and h1.count() > 0:
            t = safe_text(h1.inner_text())
            if looks_like_product_name(t): return t
    except Exception: pass
    # 2) og:title
    try:
        og = page.eval_on_selector("meta[property='og:title']", "el => el?.getAttribute('content') || null")
        og = safe_text(og)
        if looks_like_product_name(og): return og
    except Exception: pass
    # 3) common classes
    for sel in [".product-title", ".b-product__info h1", ".pdp-title", "[itemprop='name']"]:
        try:
            el = page.locator(sel).first
            if el and el.count() > 0:
                t = safe_text(el.inner_text())
                if looks_like_product_name(t): return t
        except Exception: pass
    return ""

# ----------------------- JSON/DOM extraction -----------------------
def ldjson_blocks(page: Page) -> List[Any]:
    blocks = []
    for el in page.locator('script[type="application/ld+json"]').all():
        try:
            t = el.inner_text().strip()
            if t: blocks.append(t)
        except Exception:
            continue
    return blocks

def parse_json(txt: str) -> Optional[Any]:
    try: return json.loads(txt)
    except Exception: return None

def deep_find_kv(obj: Any, keys: Set[str]) -> Dict[str, str]:
    out: Dict[str,str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str, int, float)):
                    out[lk] = str(v)
                # nested brand/manufacturer objects
                if lk == "brand" and isinstance(v, dict) and v.get("name"):
                    out["brand"] = str(v["name"])
                if lk == "manufacturer" and isinstance(v, dict) and v.get("name"):
                    out["manufacturer"] = str(v["name"])
                walk(v)
        elif isinstance(x, list):
            for i in x: walk(i)
    walk(obj)
    return out

def walk_find(o: Any) -> Tuple[str,str,str,str,str]:
    """
    Extract name/brand/size/gtin/sku from arbitrary JSON.
    We ignore obvious UI junk names (e.g., 'inst-badge').
    """
    name = brand = size_text = gtin = sku = ""
    def maybe_set_name(v: Any):
        nonlocal name
        if isinstance(v, (str, int, float)):
            cand = str(v).strip()
            if looks_like_product_name(cand) and not name:
                name = cand
    def walk(x: Any):
        nonlocal brand, size_text, gtin, sku
        if isinstance(x, dict):
            if not name:
                for k in ("name","productName","title"):
                    if k in x: maybe_set_name(x.get(k))
                    if name: break
            if not brand:
                b = x.get("brand")
                if isinstance(b, dict):
                    v = b.get("name")
                    if isinstance(v, (str,int,float)): brand = str(v).strip()
                elif isinstance(b, (str,int,float)):
                    brand = str(b).strip()
            if not size_text:
                for k in ("size","sizeText","weight","netWeight","packageSize","size_text"):
                    v = x.get(k)
                    if isinstance(v, (str,int,float)): size_text = str(v).strip(); break
            if not gtin:
                for k in GTIN_KEYS:
                    v = x.get(k) or x.get(k.upper())
                    if isinstance(v, (str,int,float)): gtin = str(v).strip(); break
            if not sku:
                for k in SKU_KEYS:
                    v = x.get(k) or x.get(k.upper())
                    if isinstance(v, (str,int,float)): sku = str(v).strip(); break
            for v in x.values():
                if isinstance(v, (dict,list)): walk(v)
        elif isinstance(x, list):
            for it in x: walk(it)
    walk(o); return name, brand, size_text, gtin, sku

def extract_from_jsonld(page: Page) -> Tuple[str,str,str,str,str]:
    for raw in ldjson_blocks(page):
        data = parse_json(raw)
        if not data: continue
        n,b,s,g,sku = walk_find(data)
        if any([n,b,s,g,sku]):
            return safe_text(n), safe_text(b), safe_text(s), safe_text(g), safe_text(sku)
    return "","","","",""

def extract_from_other_scripts(page: Page) -> Tuple[str,str,str,str,str]:
    scripts = page.locator('script:not([type="application/ld+json"])').all()
    for s in scripts:
        try: txt = s.inner_text().strip()
        except Exception: continue
        if not txt or ("{" not in txt and "[" not in txt): continue
        # fast and tolerant JSON block finder
        for m in re.finditer(r"(\{(?:.|\n)*?\}|\[(?:.|\n)*?\])", txt):
            obj = parse_json(m.group(1))
            if obj is None: continue
            n,b,sz,g,sku = walk_find(obj)
            if any([n,b,sz,g,sku]):
                return safe_text(n), safe_text(b), safe_text(sz), safe_text(g), safe_text(sku)
    return "","","","",""

def extract_from_js_globals(page: Page) -> Dict[str,str]:
    out: Dict[str,str] = {}
    for glb in ["__NUXT__","__NEXT_DATA__","APP_STATE","__INITIAL_STATE__","dataLayer","apolloState","APOLLO_STATE"]:
        try:
            data = page.evaluate(f"window['{glb}']")
        except Exception:
            data = None
        if not data: continue
        got = deep_find_kv(data, GTIN_KEYS | SKU_KEYS | PRICE_KEYS | CURR_KEYS | BRAND_KEYS)
        out.update({k: v for k, v in got.items() if v})
    return out

def extract_price_currency(page: Page) -> Tuple[str,str]:
    # JSON-LD first
    for raw in ldjson_blocks(page):
        data = parse_json(raw)
        if not data: continue
        def get_offer(o):
            if not isinstance(o, dict): return "",""
            p = o.get("price") or o.get("priceSpecification",{}).get("price")
            c = o.get("priceCurrency") or o.get("priceSpecification",{}).get("priceCurrency")
            return (norm_price_str(p), safe_text(c))
        if isinstance(data, dict) and "offers" in data:
            off = data["offers"]
            if isinstance(off, list):
                for it in off:
                    pr,cur = get_offer(it)
                    if pr: return pr, (cur or "EUR")
            else:
                pr,cur = get_offer(off)
                if pr: return pr, (cur or "EUR")
    # visible DOM fallback
    try:
        el = page.locator('[data-testid*="price"], [itemprop="price"], .price, .product-price').first
        if el and el.count() > 0:
            txt = el.inner_text().strip()
            pr = norm_price_str(txt)
            if pr: return pr, "EUR"
    except Exception:
        pass
    return "",""

def extract_image_url(page: Page) -> str:
    # JSON-LD
    for raw in ldjson_blocks(page):
        data = parse_json(raw)
        if not data: continue
        def pull(o: Any) -> Optional[str]:
            if isinstance(o, dict):
                im = o.get("image")
                if isinstance(im, str): return url_abs(im, BASE)
                if isinstance(im, list) and im and isinstance(im[0], str): return url_abs(im[0], BASE)
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
        if r: return r
    # DOM
    try:
        img = page.locator('img').first
        if img and img.count() > 0:
            src = img.get_attribute("src") or ""
            if src: return url_abs(src, BASE)
    except Exception:
        pass
    return ""

def extract_breadcrumbs(page: Page) -> Tuple[str,str]:
    try:
        crumbs = []
        for sel in ['nav[aria-label*="crumb"]', '.breadcrumb', '[data-testid*="breadcrumb"]']:
            loc = page.locator(f"{sel} a, {sel} span, {sel} li")
            if loc.count() > 0:
                for i in range(loc.count()):
                    t = safe_text(loc.nth(i).inner_text())
                    if t: crumbs.append(t)
                break
        crumbs = [c for c in crumbs if len(c) > 1]
        if crumbs: return " / ".join(crumbs), crumbs[-1]
    except Exception:
        pass
    return "",""

# --------- DOM spec fallback (brand/manufacturer/size) ---------
_SPEC_BRAND_KEYS = {
    "kaubamärk", "kaubamark", "bränd", "brand", "bränd/brand"
}
_SPEC_MFR_KEYS = {
    "tarnija kontaktid", "tarnija kontakt", "tarnija",
    "tootja", "manufacturer", "valmistaja", "supplier"
}
_SPEC_SIZE_KEYS = {
    "kogus", "netokogus", "maht", "neto", "pakendi suurus", "pakend", "suurus", "kaal"
}

def _norm_key_et(s: str) -> str:
    s = (s or "").strip().lower()
    return (s.replace("ä","a").replace("ö","o").replace("õ","o")
             .replace("ü","u").replace("š","s").replace("ž","z"))

def extract_specs_from_dom(page: Page) -> Tuple[str, str, str, str]:
    """
    Returns (brand, manufacturer, size_text, ean_raw) from visible spec blocks.
    Scans tables (th/td), dl/dt/dd, and generic 'Key: Value' rows.
    """
    brand = ""
    mfr = ""
    size_text = ""
    ean_raw = ""

    def set_brand(v: str):
        nonlocal brand
        v = (v or "").strip()
        if v and not brand:
            brand = v

    def set_mfr(v: str):
        nonlocal mfr
        v = (v or "").strip()
        if v and not mfr:
            mfr = v

    def set_size(v: str):
        nonlocal size_text
        v = (v or "").strip()
        if v and not size_text:
            size_text = v

    def set_ean(v: str):
        nonlocal ean_raw
        v = norm_digits(v)
        if v and not ean_raw:
            ean_raw = v

    soup = BeautifulSoup(page.content(), "lxml")

    # 1) Table rows
    for row in soup.select("table tr"):
        th, td = row.find("th"), row.find("td")
        if not th or not td: continue
        k = _norm_key_et(th.get_text(" ", strip=True))
        v = td.get_text(" ", strip=True)
        if k in _SPEC_BRAND_KEYS:
            set_brand(v)
        elif k in _SPEC_MFR_KEYS:
            set_mfr(v)
        elif any(t in k for t in _SPEC_SIZE_KEYS):
            set_size(v)
        elif any(t in k for t in ("ribakood","triipkood","ean","gtin")):
            set_ean(v)

    # 2) <dl> lists
    for dl in soup.select("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            k = _norm_key_et(dt.get_text(" ", strip=True))
            v = dd.get_text(" ", strip=True)
            if k in _SPEC_BRAND_KEYS:
                set_brand(v)
            elif k in _SPEC_MFR_KEYS:
                set_mfr(v)
            elif any(t in k for t in _SPEC_SIZE_KEYS):
                set_size(v)
            elif any(t in k for t in ("ribakood","triipkood","ean","gtin")):
                set_ean(v)

    # 3) Generic rows “Key: Value”
    for el in soup.select("li, .row, .key-value, .product-attributes__row, .product-details__row, div, span, p"):
        t = (el.get_text(" ", strip=True) or "")
        if ":" not in t or len(t) > 240: continue
        k, v = t.split(":", 1)
        k = _norm_key_et(k); v = v.strip()
        if k in _SPEC_BRAND_KEYS:
            set_brand(v)
        elif k in _SPEC_MFR_KEYS:
            set_mfr(v)
        elif any(tk in k for tk in _SPEC_SIZE_KEYS):
            set_size(v)
        elif any(tk in k for tk in ("ribakood","triipkood","ean","gtin")):
            set_ean(v)

    return brand, mfr, size_text, ean_raw

# ---------- dynamic-page helpers ----------
def accept_cookies_if_present(page: Page) -> None:
    for sel in ('button:has-text("Nõustu")','button:has-text("Nõustu kõigiga")',
                'button:has-text("Accept all")','[data-testid="uc-accept-all-button"]'):
        try:
            b = page.locator(sel).first
            if b and b.is_visible(): b.click(timeout=1500); page.wait_for_timeout(150); return
        except Exception: pass
    try:
        for fr in page.frames:
            for sel in ('[data-testid="uc-accept-all-button"]','button:has-text("Accept all")',
                        'button:has-text("Nõustu kõigiga")','button:has-text("OK")'):
                loc = fr.locator(sel).first
                if loc and loc.is_visible(timeout=1000): loc.click(); page.wait_for_timeout(150); return
    except Exception:
        return

def wait_for_hydration(page: Page, timeout_ms: int = 9000) -> None:
    try:
        page.wait_for_function(
            """() => !!(document.querySelector('h1') || document.querySelector('[itemprop="price"], .price, .product-price'))""",
            timeout=timeout_ms
        )
    except Exception:
        pass

def auto_scroll(page: Page, total_px: int = 2500, step: int = 600, pause_ms: int = 250) -> None:
    climbed = 0
    while climbed < total_px:
        page.mouse.wheel(0, step); climbed += step; page.wait_for_timeout(pause_ms)

def discover_pdp_links_on_category(page: Page) -> List[str]:
    try:
        page.wait_for_selector('a[href*="/toode/"], [data-testid*="product-card"], .b-product', timeout=12000)
    except Exception:
        auto_scroll(page, 1200, 600, 200)
    auto_scroll(page, 2200, 700, 200)
    links: Set[str] = set()
    try:
        hrefs = page.eval_on_selector_all(
            'a[href*="/toode/"], a[href^="/toode/"], a[href*="/product/"], a[href*="/p/"]',
            "els => els.map(e => e.href).filter(Boolean)"
        )
        for u in hrefs or []: links.add(url_abs(u, BASE))
    except Exception: pass
    for sel, attr in (('[data-link*="/toode/"]',"data-link"),
                      ('[data-product-url*="/toode/"]',"data-product-url"),
                      ('[data-href*="/toode/"]',"data-href")):
        try:
            vals = page.eval_on_selector_all(sel, f"els => els.map(e => e.getAttribute('{attr}'))")
            for v in vals or []:
                if v: links.add(url_abs(v, BASE))
        except Exception: pass
    try:
        blobs = page.eval_on_selector_all('[data-gtm-product]', "els => els.map(e => e.getAttribute('data-gtm-product'))")
        for b in blobs or []:
            if not b: continue
            try:
                obj = json.loads(b); u = obj.get("url") or obj.get("link") or ""
                if u: links.add(url_abs(u, BASE))
            except Exception: continue
    except Exception: pass
    return sorted({u for u in links if is_pdp_url(u)})

# ---------- paging helpers ----------
def _cat_base(url: str) -> str:
    u = urlparse(url); return urlunparse((u.scheme,u.netloc,u.path,"","",""))
def _build_page_url(seed: str, n: int) -> str:
    return _cat_base(seed) if n <= 1 else f"{_cat_base(seed)}?page={n}"
def _max_pages_from_dom(page: Page) -> int:
    try:
        nums = page.evaluate("""
        (() => {
          const getN = (a) => { try { const u = new URL(a.href, location.href); const v = parseInt(u.searchParams.get('page')||''); return Number.isNaN(v)?null:v; } catch { return null; } };
          const anchors = [...document.querySelectorAll('a[href*="page="]')];
          const ns = anchors.map(getN).filter(n => n && n > 0);
          [...document.querySelectorAll('a,button')].forEach(el => { const t=(el.textContent||'').trim(); const m=t.match(/^\\d{1,3}$/); if(m) ns.push(parseInt(m[0],10)); });
          return ns.length ? Math.max(...ns) : 1;
        })()
        """)
        return int(nums) if nums and nums > 0 else 1
    except Exception:
        return 1

def read_categories(args) -> List[str]:
    cats: List[str] = []
    if args.cats_file and os.path.isfile(args.cats_file):
        with open(args.cats_file, "r", encoding="utf-8") as f:
            for line in f:
                u = safe_text(line)
                if u: cats.append(url_abs(u, BASE))
    return cats

# ----------------------- PDP extraction -----------------------
def extract_pdp(page: Page, source_url: str, category_hint: str) -> Row:
    # make sure the page is ready
    accept_cookies_if_present(page); wait_for_hydration(page)

    name, brand, size_text, ean_raw, sku_raw = extract_from_jsonld(page)
    if not any([name, brand, size_text, ean_raw, sku_raw]):
        _n,_b,_s,_g,_sku = extract_from_other_scripts(page)
        name = name or _n
        brand = brand or _b
        size_text = size_text or _s
        ean_raw = ean_raw or _g
        sku_raw = sku_raw or _sku

    # JS globals (Nuxt/Redux, etc.)
    js = extract_from_js_globals(page)
    if not ean_raw:
        for k in ("gtin13","ean","ean13","gtin","barcode"):
            if k in js: ean_raw = js[k]; break
    if not sku_raw:
        for k in ("sku","mpn","code","id"):
            if k in js: sku_raw = js[k]; break
    if not brand:
        brand = js.get("brand","") or js.get("manufacturer","")
    manufacturer = js.get("manufacturer","")

    # If JSON name is junk, prefer DOM title
    if not looks_like_product_name(name):
        dom_name = dom_title_fallback(page)
        if dom_name: name = dom_name

    # DOM spec fallback to fill brand/manufacturer/size/EAN when missing
    try:
        b_dom, mfr_dom, size_dom, ean_dom = extract_specs_from_dom(page)
        if b_dom and not brand: brand = b_dom
        if mfr_dom and not manufacturer: manufacturer = mfr_dom
        if size_dom and not size_text: size_text = size_dom
        if ean_dom and not ean_raw: ean_raw = ean_dom
    except Exception:
        pass

    price, currency = extract_price_currency(page)
    if not price and "price" in js:
        price = norm_price_str(js["price"])
    if not currency:
        currency = js.get("currency") or js.get("pricecurrency") or "EUR"

    image_url = extract_image_url(page)
    cat_path, cat_leaf = extract_breadcrumbs(page)
    if not cat_path and category_hint:
        cat_path = category_hint
        cat_leaf = category_hint.split("/")[-1] if "/" in category_hint else category_hint

    # normalize digits
    ean_raw = norm_digits(ean_raw)
    sku_raw = safe_text(sku_raw)
    price = norm_price_str(price)
    ext_id = ext_id_from_url(source_url)

    return Row(
        STORE_CHAIN, STORE_NAME, STORE_CHANNEL,
        ext_id, ean_raw, sku_raw, (name or ""), (size_text or ""),
        (brand or ""), (manufacturer or ""), (price or ""), (currency or "EUR"),
        (image_url or ""), (cat_path or ""), (cat_leaf or ""), source_url
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
    ap.add_argument("--skip-ext-file", default="", help="File with ext ids/URLs to SKIP (optional)")
    ap.add_argument("--only-ext-file", default="", help="File with ext ids/URLs to process EXCLUSIVELY (optional)")
    args = ap.parse_args()

    cats = read_categories(args)
    if not cats:
        print("[barbora] No categories provided. Provide --cats-file.", file=sys.stderr); sys.exit(2)

    headless = args.headless.strip() != "0"
    req_delay = float(args.req_delay); page_limit = int(args.page_limit or "0"); max_products = int(args.max_products or "0")

    # Load skip/only files
    skip_keys: Set[str] = set()
    only_keys: Set[str] = set()
    if args.skip_ext_file and os.path.isfile(args.skip_ext_file):
        with open(args.skip_ext_file, "r", encoding="utf-8") as f:
            for line in f:
                s = safe_text(line)
                if not s: continue
                skip_keys |= keyset_for_url(s)
    if args.only_ext_file and os.path.isfile(args.only_ext_file):
        with open(args.only_ext_file, "r", encoding="utf-8") as f:
            for line in f:
                s = safe_text(line)
                if not s: continue
                only_keys |= keyset_for_url(s)

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)
    out = open(args.output_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(out); writer.writerow(CSV_FIELDS)

    total_written = 0; seen_pdp: Set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
        ctx = browser.new_context(
            base_url=BASE, locale="et-EE", is_mobile=False,
            viewport={"width": 1360, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()

        for cat in cats:
            try:
                print(f"[barbora] Category: {cat}", file=sys.stderr)
                base = _cat_base(cat)
                try:
                    page.goto(base, timeout=45000, wait_until="domcontentloaded")
                except (PWTimeout, Exception) as e:
                    print(f"[barbora] nav error on {base}: {e}", file=sys.stderr); continue
                accept_cookies_if_present(page); wait_for_hydration(page); auto_scroll(page, 1200, 600, 200)

                detected_max = _max_pages_from_dom(page)
                last_page = min(detected_max, page_limit) if page_limit > 0 else detected_max
                last_page = max(1, last_page)

                prev_links: Set[str] = set()
                for pnum in range(1, last_page + 1):
                    cur = _build_page_url(base, pnum)
                    try:
                        page.goto(cur, timeout=45000, wait_until="domcontentloaded")
                    except (PWTimeout, Exception) as e:
                        print(f"[barbora] nav error on {cur}: {e}", file=sys.stderr); break
                    accept_cookies_if_present(page); wait_for_hydration(page); auto_scroll(page, 2200, 700, 200)

                    links = discover_pdp_links_on_category(page)
                    cur_set = set(links)
                    if cur_set and cur_set == prev_links: break
                    prev_links = cur_set

                    for u in links:
                        if max_products and total_written >= max_products: break
                        if u in seen_pdp: continue
                        seen_pdp.add(u)

                        # build keys and apply ONLY/SKIP
                        keys = keyset_for_url(u) | keyset_for_url(ext_id_from_url(u))
                        if only_keys and keys.isdisjoint(only_keys):  # ONLY wins
                            continue
                        if skip_keys and not keys.isdisjoint(skip_keys):
                            continue

                        p = ctx.new_page()
                        try:
                            p.goto(u, timeout=45000, wait_until="domcontentloaded")
                            accept_cookies_if_present(p); wait_for_hydration(p)
                            row = extract_pdp(p, u, category_hint=cat)
                            writer.writerow(row.as_list()); total_written += 1
                        except PWTimeout:
                            print(f"[barbora] PDP timeout: {u}", file=sys.stderr)
                        except Exception as e:
                            print(f"[barbora] PDP error on {u}: {e}", file=sys.stderr)
                        finally:
                            try: p.close()
                            except Exception: pass

                        if max_products and total_written >= max_products: break
                        page.wait_for_timeout(int(req_delay*1000))

                    if max_products and total_written >= max_products: break
                    page.wait_for_timeout(int(req_delay*1000))

            except Exception as e:
                print(f"[barbora] category error: {cat} -> {e}", file=sys.stderr)
                continue

        try: page.close(); ctx.close(); browser.close()
        except Exception: pass

    out.close()
    print(f"[barbora] done. rows={total_written} -> {args.output_csv}", file=sys.stderr)

if __name__ == "__main__":
    main()

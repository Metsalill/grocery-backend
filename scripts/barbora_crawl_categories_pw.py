#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) – Category crawler → PDP extractor → CSV/DB-friendly

CSV columns (exact order):
  store_chain,store_name,store_channel,ext_id,ean_raw,sku_raw,
  name,size_text,brand,manufacturer,price,currency,
  image_url,category_path,category_leaf,source_url
"""
from __future__ import annotations
import os, re, csv, json, time, html
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

BASE = "https://barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME = "Barbora ePood"
STORE_CHANNEL = "online"

OUTPUT_CSV = os.getenv("OUTPUT_CSV", "data/barbora.csv")
MODE = os.getenv("MODE", "metadata").strip().lower()     # metadata | prices
CLICK_PRODUCTS = int(os.getenv("CLICK_PRODUCTS", "1"))    # 1 = open PDPs
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "0"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.4"))
LOG_CONSOLE = os.getenv("LOG_CONSOLE", "warn")            # 0 | warn | all

DIGITS_RE = re.compile(r"\d+")
SIZE_RE = re.compile(r"(?i)\b(\d+\s?(?:x\s?\d+)?\s?(?:ml|l|g|kg|cl|pcs|tk))\b|(\d+\s?x\s?\d+)")

SPEC_KEYS_BRAND = [ "kaubamärk", "bränd", "brand" ]
SPEC_KEYS_MFR   = [ "tootja", "valmistaja", "manufacturer" ]
SPEC_KEYS_SIZE  = [ "kogus", "netokogus", "maht", "neto" ]

def norm(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip().lower()

def text_of(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""

def from_json_ld(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {"name": None, "brand": None, "manufacturer": None, "ean": None, "image": None, "price": None, "currency": None}
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        # sometimes it's a list
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict): continue
            if it.get("@type") and "Product" in (it["@type"] if isinstance(it["@type"], list) else [it["@type"]]):
                if not out["name"]: out["name"] = it.get("name")
                brand = it.get("brand")
                if isinstance(brand, dict):
                    brand = brand.get("name")
                if brand and not out["brand"]:
                    out["brand"] = brand
                mfr = it.get("manufacturer")
                if isinstance(mfr, dict):
                    mfr = mfr.get("name")
                if mfr and not out["manufacturer"]:
                    out["manufacturer"] = mfr
                gtin = it.get("gtin13") or it.get("gtin") or it.get("sku")
                if gtin and not out["ean"]:
                    out["ean"] = re.sub(r"[^0-9]", "", str(gtin))
                img = it.get("image")
                if isinstance(img, list): img = img[0]
                if img and not out["image"]:
                    out["image"] = img
                offers = it.get("offers") or {}
                if isinstance(offers, dict):
                    if not out["price"] and offers.get("price") is not None:
                        out["price"] = str(offers.get("price"))
                    if not out["currency"] and offers.get("priceCurrency"):
                        out["currency"] = offers.get("priceCurrency")
    return out

def parse_spec_table(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {"brand": None, "manufacturer": None, "size": None, "ean": None}
    # generic dl/dt/dd pairs
    for row in soup.select("dl, table"):
        text = text_of(row)
        # iterate dt/dd when possible
    for dt in soup.select("dt, th"):
        key = norm(text_of(dt))
        dd = dt.find_next_sibling(["dd","td"])
        val = norm(text_of(dd)) if dd else ""
        if not val: continue
        if key in SPEC_KEYS_BRAND and not out["brand"]:
            out["brand"] = val
        elif key in SPEC_KEYS_MFR and not out["manufacturer"]:
            out["manufacturer"] = val
        elif key in SPEC_KEYS_SIZE and not out["size"]:
            out["size"] = val
        elif "ribakood" in key or "ean" in key:
            out["ean"] = re.sub(r"[^0-9]", "", val)
    return out

def parse_app_state_for_brand(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    # scan inline scripts for "brand" / "manufacturer" keys
    for s in soup.find_all("script"):
        txt = s.string or ""
        if not txt or ("brand" not in txt and "manufacturer" not in txt): 
            continue
        # very forgiving extraction
        m = re.search(r'"brand"\s*:\s*"([^"]+)"', txt)
        b = m.group(1).strip() if m else None
        m2 = re.search(r'"manufacturer"\s*:\s*"([^"]+)"', txt)
        mf = m2.group(1).strip() if m2 else None
        if b or mf:
            return b, mf
    return None, None

def safe_product_name(soup: BeautifulSoup, category_leaf: str, listing_title: Optional[str]) -> str:
    # Primary: PDP h1 or og:title
    h1 = soup.select_one("h1,[itemprop=name]")
    title = text_of(h1)
    if not title:
        og = soup.find("meta", {"property": "og:title"})
        title = og["content"] if og and og.has_attr("content") else ""
    # Fallback to listing card title
    if not title and listing_title:
        title = listing_title
    # Guard: avoid category name
    if norm(title) == norm(category_leaf):
        # try next best: breadcrumb last-1 (often product name is not in breadcrumb, but try)
        bc = [text_of(x) for x in soup.select("nav.breadcrumb a, .breadcrumb a, .breadcrumbs a")]
        if bc:
            title2 = bc[-1]
            if norm(title2) != norm(category_leaf):
                title = title2
    return title

def extract_size_from_name(name: str) -> Optional[str]:
    if not name: return None
    m = SIZE_RE.search(name)
    if not m: return None
    return m.group(0)

def parse_price_from_dom(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    # Barbora shows price in PDP in a few spots; be tolerant
    p = soup.select_one("[data-testid=product-price], .price, .product-price")
    price = None
    if p:
        price = re.sub(r"[^\d,\.]", "", text_of(p)).replace(",", ".")
    cur = "EUR"  # Barbora EE
    return price, cur

def get_ext_id(url: str) -> str:
    # usually last numeric segment
    m = re.search(r"/p/(\d+)", url) or re.search(r"-(\d+)$", url)
    return m.group(1) if m else re.sub(r"\W+", "", url)

def discover_category_urls() -> List[str]:
    # If data/barbora_categories.txt exists, use it; else use curated FOOD roots
    path = "data/barbora_categories.txt"
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            urls = [ln.strip() for ln in f if ln.strip()]
            return urls
    return [
        f"{BASE}/koogiviljad-puuviljad",
        f"{BASE}/piimatooted-munad-void",
        f"{BASE}/juustud",
        f"{BASE}/leib-sai-kondiitritooted",
        f"{BASE}/valmistoidud",
        f"{BASE}/kuivained-ja-hoidised",
        f"{BASE}/maitseained-ja-kastmed",
        f"{BASE}/suupisted-maiustused",
        f"{BASE}/joogid",
        f"{BASE}/kulmutatud-tooted",
    ]

def list_products_from_category(page: Page, cat_url: str) -> List[Tuple[str,str]]:
    """Return list of (pdp_url, listing_title)."""
    page.goto(cat_url, timeout=60000)
    time.sleep(REQ_DELAY)
    # pagination: scroll/load more
    for _ in range(60):
        before = page.content()
        page.mouse.wheel(0, 30000)
        time.sleep(0.4)
        after = page.content()
        if len(after) == len(before):
            break
    soup = BeautifulSoup(page.content(), "html.parser")
    out: List[Tuple[str,str]] = []
    for a in soup.select("a[href*='/toode/'], a[href*='/p/']"):
        href = a.get("href")
        if not href: continue
        title = text_of(a)
        url = urljoin(BASE, href)
        out.append((url, title))
    # de-dup while preserving order
    seen = set()
    uniq = []
    for u,t in out:
        if u in seen: continue
        seen.add(u)
        uniq.append((u,t))
    return uniq

def write_csv(rows: List[List[str]]):
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    header = [
        "store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw",
        "name","size_text","brand","manufacturer","price","currency",
        "image_url","category_path","category_leaf","source_url"
    ]
    newfile = not os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if newfile:
            w.writerow(header)
        for r in rows:
            w.writerow(r)

def crawl():
    cats = discover_category_urls()
    total_rows: List[List[str]] = []
    count = 0

    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=True)
        ctx = browser.new_context(locale="et-EE")
        page = ctx.new_page()
        if LOG_CONSOLE in ("warn","all"):
            page.on("console", lambda msg: print(f"[console:{msg.type()}] {msg.text()}") if (LOG_CONSOLE=="all" or msg.type()!="log") else None)

        for idx, cat in enumerate(cats, start=1):
            if PAGE_LIMIT and idx > PAGE_LIMIT: break
            # category leaf from breadcrumb last segment
            category_leaf = cat.strip("/").split("/")[-1].replace("-", " ").title()
            category_path = ""  # optional: could build from breadcrumb later

            products = list_products_from_category(page, cat)
            if not products:
                continue

            for url, listing_title in products:
                if MAX_PRODUCTS and count >= MAX_PRODUCTS: break

                ext_id = get_ext_id(url)
                name = listing_title
                size_text = None
                brand = None
                manufacturer = None
                ean_raw = None
                price = None
                currency = "EUR"
                image_url = None

                if MODE == "metadata" and CLICK_PRODUCTS:
                    try:
                        page.goto(url, timeout=60000)
                        time.sleep(REQ_DELAY)
                    except PWTimeout:
                        continue
                    soup = BeautifulSoup(page.content(), "html.parser")

                    # JSON-LD first
                    jl = from_json_ld(soup)
                    image_url = jl["image"] or image_url
                    price = jl["price"] or price
                    currency = jl["currency"] or currency
                    brand = jl["brand"] or brand
                    manufacturer = jl["manufacturer"] or manufacturer
                    ean_raw = jl["ean"] or ean_raw
                    # Name with guards
                    name = safe_product_name(soup, category_leaf, listing_title) or name

                    # spec table
                    spec = parse_spec_table(soup)
                    brand = brand or spec["brand"]
                    manufacturer = manufacturer or spec["manufacturer"]
                    ean_raw = ean_raw or spec["ean"]
                    size_text = spec["size"] or extract_size_from_name(name)

                    # DOM price fallback
                    if not price:
                        price, currency = parse_price_from_dom(soup)

                    # As a last resort, avoid category-as-name
                    if norm(name) == norm(category_leaf):
                        # don’t write this row; it’s very likely a category leak
                        continue
                else:
                    # listing-only (prices mode)
                    size_text = extract_size_from_name(name)

                row = [
                    STORE_CHAIN, STORE_NAME, STORE_CHANNEL, ext_id, ean_raw or "", "",
                    name or "", size_text or "", brand or "", manufacturer or "", price or "", currency or "EUR",
                    image_url or "", category_path, category_leaf, url
                ]
                total_rows.append(row)
                count += 1
                if REQ_DELAY: time.sleep(REQ_DELAY)

        write_csv(total_rows)

if __name__ == "__main__":
    crawl()

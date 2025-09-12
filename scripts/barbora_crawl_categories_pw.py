#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) – Category → PDP crawler → CSV (EAN intentionally blank)

CSV columns:
  store_chain,store_name,store_channel,ext_id,ean_raw,sku_raw,
  name,size_text,brand,manufacturer,price,currency,
  image_url,category_path,category_leaf,source_url
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import Page, sync_playwright

BASE = "https://barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME = "Barbora ePood"
STORE_CHANNEL = "online"

DEFAULT_REQ_DELAY = 0.25
DEFAULT_HEADLESS = 1

SIZE_RE = re.compile(
    r"(?ix)"
    r"(\d+\s?(?:x\s?\d+)?\s?(?:ml|l|cl|g|kg|mg|tk|pcs))"
    r"|"
    r"(\d+\s?x\s?\d+)"
)

SPEC_KEYS_BRAND = {"kaubamärk", "bränd", "brand"}
SPEC_KEYS_MFR = {"tootja", "valmistaja", "manufacturer"}
SPEC_KEYS_SIZE = {"kogus", "netokogus", "maht", "neto"}
BAD_NAMES = {"pealeht"}  # "Home" in Estonian

def norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip().lower()

def text_of(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""

def get_ext_id(url: str) -> str:
    # Stable ID derived from URL (no numeric id available on Barbora).
    m = re.search(r"/p/(\d+)", url) or re.search(r"-(\d+)$", url)
    if m:
        return m.group(1)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", url).strip("-")
    return slug[-120:]

# -------------------- Cookie banner / helpers --------------------

def accept_cookies(page: Page) -> None:
    """Try hard to accept cookie banner so products render & clicks work."""
    selectors = [
        "[data-testid='cookie-banner-accept-all']",
        "button#onetrust-accept-btn-handler",
        "button:has-text('Nõustun')",
        "button:has-text('Sain aru')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count() and btn.first.is_visible():
                btn.first.click(timeout=1500)
                page.wait_for_timeout(500)
                return
        except Exception:
            pass
    # Some banners use links
    try:
        page.get_by_role("button", name=re.compile("Nõus|Accept|OK", re.I)).click(timeout=1000)
    except Exception:
        pass

def ensure_ready(page: Page) -> None:
    accept_cookies(page)

# -------------------- PDP parsing --------------------

def from_json_ld(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {"name": None, "brand": None, "manufacturer": None, "image": None, "price": None, "currency": None}
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            t = it.get("@type")
            types = t if isinstance(t, list) else [t]
            if not types or "Product" not in types:
                continue
            if it.get("name"):
                out["name"] = it["name"]
            brand = it.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")
            if brand:
                out["brand"] = brand
            manufacturer = it.get("manufacturer")
            if isinstance(manufacturer, dict):
                manufacturer = manufacturer.get("name")
            if manufacturer:
                out["manufacturer"] = manufacturer
            img = it.get("image")
            if isinstance(img, list):
                img = img[0]
            if img:
                out["image"] = img
            offers = it.get("offers") or {}
            if isinstance(offers, dict):
                if offers.get("price") is not None:
                    out["price"] = str(offers.get("price"))
                if offers.get("priceCurrency"):
                    out["currency"] = offers.get("priceCurrency")
    return out

def parse_spec_table(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {"brand": None, "manufacturer": None, "size": None, "sku": None}
    # Generic dl/table patterns
    for head in soup.select("dt, th"):
        k = norm(text_of(head))
        val_el = head.find_next_sibling(["dd", "td"])
        v = text_of(val_el).strip() if val_el else ""
        if not v:
            continue
        if k in SPEC_KEYS_BRAND and not out["brand"]:
            out["brand"] = v
        elif k in SPEC_KEYS_MFR and not out["manufacturer"]:
            out["manufacturer"] = v
        elif k in SPEC_KEYS_SIZE and not out["size"]:
            out["size"] = v
        elif "sku" in k and not out["sku"]:
            out["sku"] = v

    # Barbora "Muu info" blocks
    labels = soup.select(".e-attribute__label, .product-attribute__label")
    for lab in labels:
        k = norm(text_of(lab))
        val_el = lab.find_next_sibling(class_="e-attribute__value") or lab.find_next_sibling(class_="product-attribute__value")
        v = text_of(val_el)
        if not v:
            continue
        if k in SPEC_KEYS_BRAND and not out["brand"]:
            out["brand"] = v
        elif k in SPEC_KEYS_MFR and not out["manufacturer"]:
            out["manufacturer"] = v
        elif k in SPEC_KEYS_SIZE and not out["size"]:
            out["size"] = v
        elif "sku" in k and not out["sku"]:
            out["sku"] = v

    return out

def parse_app_state_for_brand(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for s in soup.find_all("script"):
        txt = s.string or ""
        if not txt or ("brand" not in txt and "manufacturer" not in txt):
            continue
        mb = re.search(r'"brand"\s*:\s*"([^"]+)"', txt)
        mm = re.search(r'"manufacturer"\s*:\s*"([^"]+)"', txt)
        b = mb.group(1).strip() if mb else None
        m = mm.group(1).strip() if mm else None
        if b or m:
            return b, m
    return None, None

def extract_product_title_from_dom(soup: BeautifulSoup) -> str:
    sel = (
        ".e-product__name, [data-testid=product-title], [data-testid=product-name], "
        ".product__title, .product-title, .pdp__title, .product-view__title, "
        "h1[itemprop=name]"
    )
    el = soup.select_one(sel)
    return text_of(el)

def parse_price_from_dom(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    cur = "EUR"
    price_el = soup.select_one(
        "[data-testid=product-price], .e-price__main, .product-price, .price"
    )
    if not price_el:
        return None, cur
    val = re.sub(r"[^\d,\.]", "", text_of(price_el)).replace(",", ".")
    return (val if val else None), cur

def prefer_valid_name(candidates: List[str], category_leaf: str) -> str:
    for cand in candidates:
        c = (cand or "").strip()
        if not c:
            continue
        if norm(c) in BAD_NAMES:
            continue
        if norm(c) == norm(category_leaf):
            continue
        return c
    return candidates[0] if candidates else ""

def extract_breadcrumbs(soup: BeautifulSoup) -> Tuple[str, str]:
    """Return (category_path, category_leaf) from breadcrumbs if found."""
    path = []
    # Try a few common breadcrumb containers
    for bc in soup.select("nav[aria-label*=breadcrumb] a, .breadcrumb a, .breadcrumbs a"):
        t = text_of(bc)
        if not t:
            continue
        path.append(t)
    if not path:
        return "", ""
    # De-dup consecutive repeats like 'Pealeht'
    cleaned = []
    for p in path:
        if cleaned and norm(cleaned[-1]) == norm(p):
            continue
        cleaned.append(p)
    leaf = cleaned[-1] if cleaned else ""
    return " / ".join(cleaned), leaf

def extract_size_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    m = SIZE_RE.search(name)
    return m.group(0) if m else None

def extract_from_pdp(page: Page, url: str, listing_title: Optional[str], category_leaf_hint: str, req_delay: float) -> Dict[str, Optional[str]]:
    page.goto(url, timeout=60000, wait_until="domcontentloaded")
    ensure_ready(page)
    # Wait briefly for either JSON-LD or a product title to render
    try:
        page.wait_for_selector("script[type='application/ld+json']", timeout=6000)
    except PWTimeout:
        pass
    try:
        page.wait_for_selector(".e-product__name, [data-testid=product-title], [data-testid=product-name]", timeout=5000)
    except PWTimeout:
        pass
    page.wait_for_timeout(int(req_delay * 1000))

    soup = BeautifulSoup(page.content(), "html.parser")

    jl = from_json_ld(soup)
    spec = parse_spec_table(soup)
    b2, m2 = parse_app_state_for_brand(soup)

    # Name preference: JSON-LD > PDP title > listing title > any h1
    h1_any = text_of(soup.select_one("h1"))
    dom_title = extract_product_title_from_dom(soup)
    candidates = [
        jl.get("name") or "",
        dom_title,
        listing_title or "",
        h1_any,
    ]
    # If breadcrumb present, use it to filter "category-as-title" cases
    cat_path, cat_leaf_bc = extract_breadcrumbs(soup)
    category_leaf = cat_leaf_bc or category_leaf_hint
    name = prefer_valid_name(candidates, category_leaf)

    price = jl.get("price")
    currency = jl.get("currency") or "EUR"
    if not price:
        price, currency = parse_price_from_dom(soup)

    size_text = spec["size"] or extract_size_from_name(name)
    image_url = jl.get("image")
    brand = jl.get("brand") or spec["brand"] or b2
    manufacturer = jl.get("manufacturer") or spec["manufacturer"] or m2
    sku_raw = spec["sku"]

    # Final category info
    if not cat_path:
        # Best-effort from URL
        parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
        cat_path = " / ".join(p.replace("-", " ").title() for p in parts[:-1]) if parts else ""
        if not category_leaf:
            category_leaf = (parts[-2] if len(parts) >= 2 else (parts[-1] if parts else "")).replace("-", " ").title()

    return {
        "name": name,
        "size_text": size_text,
        "brand": brand,
        "manufacturer": manufacturer,
        "price": price,
        "currency": currency or "EUR",
        "image_url": image_url,
        "sku_raw": sku_raw,
        "category_path": cat_path,
        "category_leaf": category_leaf,
    }

# -------------------- Category listing --------------------

def wait_for_products(page: Page) -> None:
    try:
        page.wait_for_selector("a[href*='/toode/'], a[href*='/p/'], [data-testid*='product']", timeout=15000)
    except PWTimeout:
        # Try accepting cookies once more and wait briefly
        accept_cookies(page)
        page.wait_for_timeout(1000)

def click_load_more(page: Page) -> int:
    """Click 'load more' buttons until exhausted. Returns how many clicks made."""
    clicks = 0
    candidates = [
        "button:has-text('Laadi veel')",
        "button:has-text('Lae rohkem')",
        "button:has-text('Load more')",
        "[data-testid='load-more']",
    ]
    while True:
        btn = None
        for sel in candidates:
            loc = page.locator(sel)
            try:
                if loc.count() and loc.first.is_visible() and loc.first.is_enabled():
                    btn = loc.first
                    break
            except Exception:
                continue
        if not btn:
            break
        try:
            btn.click(timeout=2000)
            page.wait_for_timeout(900)
            clicks += 1
        except Exception:
            break
    return clicks

def infinite_scroll(page: Page, rounds: int = 50) -> None:
    stagnant = 0
    last_count = 0
    for _ in range(rounds):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
        count = page.locator("a[href*='/toode/'], a[href*='/p/']").count()
        if count <= last_count:
            stagnant += 1
        else:
            stagnant = 0
        last_count = count
        if stagnant >= 3:
            break

def list_products_from_category(page: Page, cat_url: str, req_delay: float) -> List[Tuple[str, str]]:
    """Return list of (pdp_url, listing_title)."""
    page.goto(cat_url, timeout=60000, wait_until="domcontentloaded")
    ensure_ready(page)
    page.wait_for_timeout(int(req_delay * 1000))

    wait_for_products(page)
    clicked = click_load_more(page)
    if clicked == 0:
        infinite_scroll(page)

    # After loading, parse the current DOM
    soup = BeautifulSoup(page.content(), "html.parser")
    out: List[Tuple[str, str]] = []
    for a in soup.select("a[href*='/toode/'], a[href*='/p/']"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        url = urljoin(BASE, href)
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc.endswith("barbora.ee"):
            continue
        title = text_of(a)
        out.append((url, title))

    # Deduplicate while preserving order
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((u, t))

    print(f"[cat] {cat_url} → products found: {len(uniq)}")
    return uniq

# -------------------- CSV / Runner --------------------

def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def write_csv(rows: List[List[str]], out_path: str) -> None:
    ensure_dir(out_path)
    header = [
        "store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw",
        "name","size_text","brand","manufacturer","price","currency",
        "image_url","category_path","category_leaf","source_url"
    ]
    newfile = not os.path.exists(out_path)
    with open(out_path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if newfile:
            w.writerow(header)
        w.writerows(rows)

def crawl(args) -> None:
    cats = read_lines(args.cats_file)
    skip_ext: set[str] = set(read_lines(args.skip_ext_file)) if args.skip_ext_file and os.path.exists(args.skip_ext_file) else set()
    only_ext: set[str] = set(read_lines(args.only_ext_file)) if args.only_ext_file and os.path.exists(args.only_ext_file) else set()
    only_urls: List[str] = read_lines(args.only_url_file) if args.only_url_file and os.path.exists(args.only_url_file) else []

    out_rows: List[List[str]] = []
    total = 0
    headless = bool(int(args.headless))
    req_delay = float(args.req_delay)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(locale="et-EE")
        page = ctx.new_page()

        if only_urls:
            for url in only_urls:
                if int(args.max_products) and total >= int(args.max_products):
                    break
                ext_id = get_ext_id(url)
                if skip_ext and ext_id in skip_ext:
                    continue
                # Heuristic category leaf from URL path
                parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
                cat_leaf = (parts[-2] if len(parts) >= 2 else (parts[-1] if parts else "")).replace("-", " ").title()
                try:
                    data = extract_from_pdp(page, url, listing_title=None, category_leaf_hint=cat_leaf, req_delay=req_delay)
                except Exception as e:
                    print(f"[warn] PDP parse failed for {ext_id}: {e}", file=sys.stderr)
                    continue

                row = [
                    STORE_CHAIN, STORE_NAME, STORE_CHANNEL, ext_id,
                    "",  # ean_raw intentionally blank
                    data.get("sku_raw") or "",
                    data.get("name") or "",
                    data.get("size_text") or "",
                    data.get("brand") or "",
                    data.get("manufacturer") or "",
                    data.get("price") or "",
                    data.get("currency") or "EUR",
                    data.get("image_url") or "",
                    data.get("category_path") or "",
                    data.get("category_leaf") or cat_leaf,
                    url,
                ]
                out_rows.append(row)
                total += 1
                if req_delay:
                    time.sleep(req_delay)
        else:
            for idx, cat in enumerate(cats, start=1):
                if int(args.page_limit) and idx > int(args.page_limit):
                    break
                leaf_seg = cat.strip("/").split("/")[-1]
                category_leaf = leaf_seg.replace("-", " ").title()
                category_path = ""  # filled on PDP

                prods = list_products_from_category(page, cat, req_delay)
                if not prods:
                    print(f"[cat] {cat} → 0 items (check if category requires login or geo).")
                    continue

                for url, listing_title in prods:
                    if int(args.max_products) and total >= int(args.max_products):
                        break
                    ext_id = get_ext_id(url)
                    if skip_ext and ext_id in skip_ext:
                        continue
                    if only_ext and ext_id not in only_ext:
                        continue
                    try:
                        data = extract_from_pdp(page, url, listing_title, category_leaf, req_delay)
                    except Exception as e:
                        print(f"[warn] PDP parse failed for {ext_id}: {e}", file=sys.stderr)
                        continue

                    # Guard against garbage titles
                    if norm(data["name"]) in BAD_NAMES or norm(data["name"]) == norm(data.get("category_leaf") or category_leaf):
                        continue

                    row = [
                        STORE_CHAIN, STORE_NAME, STORE_CHANNEL, ext_id,
                        "",  # ean_raw intentionally blank
                        data.get("sku_raw") or "",
                        data.get("name") or "",
                        data.get("size_text") or "",
                        data.get("brand") or "",
                        data.get("manufacturer") or "",
                        data.get("price") or "",
                        data.get("currency") or "EUR",
                        data.get("image_url") or "",
                        data.get("category_path") or category_path,
                        data.get("category_leaf") or category_leaf,
                        url,
                    ]
                    out_rows.append(row)
                    total += 1
                    if req_delay:
                        time.sleep(req_delay)

        write_csv(out_rows, args.output_csv)
        print(f"[done] wrote {len(out_rows)} rows to {args.output_csv}")

def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Barbora.ee category→PDP crawler (no EAN).")
    p.add_argument("--cats-file", required=True, help="Text file with category URLs (one per line)")
    p.add_argument("--page-limit", default="0", help="Max categories to process (0=all)")
    p.add_argument("--max-products", default="0", help="Cap total PDPs visited (0=unlimited)")
    p.add_argument("--headless", default=str(DEFAULT_HEADLESS), help="1/0")
    p.add_argument("--req-delay", default=str(DEFAULT_REQ_DELAY), help="Delay between steps in seconds")
    p.add_argument("--output-csv", default="data/barbora_products.csv", help="Output CSV path")
    p.add_argument("--skip-ext-file", default="", help="Optional file with ext_ids to skip (one per line)")
    p.add_argument("--only-ext-file", default="", help="Optional file with ext_ids to include exclusively")
    p.add_argument("--only-url-file", default="", help="Optional file with PDP URLs to visit exclusively")
    return p

if __name__ == "__main__":
    parser = build_argparser()
    crawl(parser.parse_args())

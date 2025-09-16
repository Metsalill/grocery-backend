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
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import Page, sync_playwright

BASE = "https://barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME = "Barbora ePood"
STORE_CHANNEL = "online"

DEFAULT_REQ_DELAY = 0.25
DEFAULT_HEADLESS = 1

SIZE_RE = re.compile(r"(?ix)(\d+\s?(?:x\s?\d+)?\s?(?:ml|l|cl|g|kg|mg|tk|pcs))|(\d+\s?x\s?\d+)")
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
        "button:has-text('Nõustun')", "button:has-text('Sain aru')",
        "button:has-text('Accept')", "button:has-text('OK')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count() and btn.first.is_visible():
                btn.first.click(timeout=1500)
                page.wait_for_timeout(400)
                return
        except Exception:
            pass
    try:
        page.get_by_role("button", name=re.compile("Nõus|Accept|OK", re.I)).click(timeout=800)
    except Exception:
        pass


def ensure_ready(page: Page) -> None:
    accept_cookies(page)


# -------------------- PDP parsing --------------------

def _wait_for_price_and_specs(page: Page) -> None:
    """Wait for client hydration and expand product-info sections."""
    # Let SPA settle a bit
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except PWTimeout:
        pass

    # Try to ensure price exists in DOM
    price_sels = [
        "[data-testid='product-price']",
        ".e-price__main",
        ".product-price",
        ".price .e-price__main",
        ".price",
    ]
    for sel in price_sels:
        try:
            page.wait_for_selector(sel, state="attached", timeout=4000)
            break
        except PWTimeout:
            continue

    # Expand specs / product info accordions so brand/manufacturer become visible
    expanders = [
        "button:has-text('Tooteinfo')",
        "button:has-text('Tooteandmed')",
        "button:has-text('Lisainfo')",
        "button[aria-controls*='spec']",
        "button[aria-expanded='false']",
        "[data-testid*='accordion'] button",
    ]
    for sel in expanders:
        try:
            el = page.locator(sel)
            if el.count() and el.first.is_visible():
                # click if collapsed
                if el.first.get_attribute("aria-expanded") in (None, "false"):
                    el.first.click(timeout=1000)
                    page.wait_for_timeout(300)
        except Exception:
            pass


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
    """Try several shapes of brand/manufacturer inside inline scripts."""
    brand, manu = None, None
    brand_pat = [
        r'"brand"\s*:\s*"([^"]+)"',
        r'"brand"\s*:\s*{\s*"name"\s*:\s*"([^"]+)"',
    ]
    manu_pat = [
        r'"manufacturer"\s*:\s*"([^"]+)"',
        r'"manufacturer"\s*:\s*{\s*"name"\s*:\s*"([^"]+)"',
    ]
    for s in soup.find_all("script"):
        txt = (s.string or "")[:200000]  # guard
        if not txt:
            continue
        if brand is None:
            for p in brand_pat:
                m = re.search(p, txt)
                if m:
                    brand = m.group(1).strip()
                    break
        if manu is None:
            for p in manu_pat:
                m = re.search(p, txt)
                if m:
                    manu = m.group(1).strip()
                    break
        if brand and manu:
            break
    return brand, manu


def extract_product_title_from_dom(soup: BeautifulSoup) -> str:
    sel = (
        ".e-product__name, [data-testid=product-title], [data-testid=product-name], "
        ".product__title, .product-title, .pdp__title, .product-view__title, h1[itemprop=name]"
    )
    el = soup.select_one(sel)
    return text_of(el)


def parse_price_from_dom(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    cur = "EUR"

    # 1) Visible nodes
    node_selectors = [
        "[data-testid=product-price]",
        ".e-price__main",
        ".product-price",
        ".price",
        "[class*='price'] span",
        "[class*='price'] div",
    ]
    for sel in node_selectors:
        el = soup.select_one(sel)
        if el:
            val = re.sub(r"[^\d,\.]", "", text_of(el)).replace(",", ".")
            if val:
                return val, cur

    # 2) Script fallbacks
    for tag in soup.find_all("script"):
        txt = tag.string or ""
        m = re.search(r'"price"\s*:\s*"?(?P<p>\d+(?:[.,]\d{1,2})?)"?', txt)
        if not m:
            m = re.search(r'"priceValue"\s*:\s*"?(?P<p>\d+(?:[.,]\d{1,2})?)"?', txt)
        if not m:
            m = re.search(r'"currentPrice"\s*:\s*"?(?P<p>\d+(?:[.,]\d{1,2})?)"?', txt)
        if m:
            return m.group("p").replace(",", "."), cur

    return None, cur


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
    path = []
    for bc in soup.select("nav[aria-label*=breadcrumb] a, .breadcrumb a, .breadcrumbs a"):
        t = text_of(bc)
        if t:
            path.append(t)
    if not path:
        return "", ""
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

    # NEW: wait for SPA hydration + expand specs so brand/manufacturer appear
    _wait_for_price_and_specs(page)

    try:
        page.wait_for_selector("script[type='application/ld+json']", timeout=6000)
    except PWTimeout:
        pass
    try:
        page.wait_for_selector(".e-product__name, [data-testid=product-title], [data-testid=product-name]", timeout=5000)
    except PWTimeout:
        pass

    # honor requested delay after hydration/expansion
    page.wait_for_timeout(int(req_delay * 1000))

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    jl = from_json_ld(soup)
    spec = parse_spec_table(soup)
    b2, m2 = parse_app_state_for_brand(soup)

    h1_any = text_of(soup.select_one("h1"))
    dom_title = extract_product_title_from_dom(soup)
    candidates = [jl.get("name") or "", dom_title, listing_title or "", h1_any]

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

    if not cat_path:
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


# -------------------- Category listing (robust link harvest + robust pagination) --------------------

def harvest_product_links(page: Page) -> List[Tuple[str, str]]:
    """Pull PDP links from all anchors on the page and filter by pathname."""
    hrefs = page.eval_on_selector_all(
        "a",
        "els => els.map(e => ({href: e.href || e.getAttribute('href') || '', text: (e.textContent||'').trim()}))",
    )
    out: List[Tuple[str, str]] = []
    for item in hrefs:
        href = (item.get("href") or "").strip()
        if not href:
            continue
        if "/toode/" in href or "/p/" in href:
            if href.startswith("/"):
                href = urljoin(BASE, href)
            out.append((href, item.get("text") or ""))
    # De-dup while preserving order
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((u, t))
    return uniq


def go_to_category(page: Page, url: str, req_delay: float) -> None:
    page.goto(url, timeout=60000, wait_until="domcontentloaded")
    ensure_ready(page)
    try:
        page.wait_for_selector("a, [role='link']", timeout=8000)
    except PWTimeout:
        pass
    page.wait_for_timeout(int(req_delay * 1000))


def _set_query_param(u: str, key: str, value: str) -> str:
    parts = urlsplit(u)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def _current_page_from_url(u: str) -> int:
    try:
        q = dict(parse_qsl(urlsplit(u).query, keep_blank_values=True))
        return int(q.get("page", "1"))
    except Exception:
        return 1


def next_page_if_any(page: Page) -> bool:
    """
    Click 'next' if pagination exists. Returns True if navigation happened.
    Handles both arrow '›' and '»', and falls back to constructing ?page=N URL.
    """
    # Scroll a bit to make pagination visible
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(200)
    except Exception:
        pass

    selectors = [
        "a[rel='next']",
        "a:has-text('Järgmine')",
        "a:has-text('Edasi')",
        "a:has-text('Next')",
        "a.pagination__link[aria-label*='Next']",
        "li.pagination-next a",
        "a[aria-label='›'], a:has-text('›')",
        "a[aria-label='»'], a:has-text('»')",
        "button[aria-label='»'], button:has-text('»')",
    ]

    for sel in selectors:
        loc = page.locator(sel)
        try:
            if loc.count() and loc.first.is_visible():
                before = page.url
                loc.first.click(timeout=2000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                page.wait_for_timeout(700)
                if page.url != before:
                    return True
        except Exception:
            continue

    # Fallback: derive next page from URL (?page=N) and navigate programmatically
    cur = _current_page_from_url(page.url)
    try:
        nums = page.eval_on_selector_all(
            "a, button",
            "els => els.map(e => (e.textContent||'').trim()).filter(t => /^\\d+$/.test(t)).map(t => parseInt(t,10))",
        )
        max_num = max(nums) if nums else None
    except Exception:
        max_num = None

    next_num = cur + 1
    if max_num is not None and next_num > max_num:
        return False

    next_url = _set_query_param(page.url, "page", str(next_num))
    if next_url == page.url:
        return False
    try:
        page.goto(next_url, timeout=15000, wait_until="domcontentloaded")
        page.wait_for_timeout(500)
        return True
    except Exception:
        return False


def collect_category_products(page: Page, cat_url: str, req_delay: float, max_pages: int = 60) -> List[Tuple[str, str]]:
    """
    Iterate through paginated listing. Returns [(pdp_url, listing_title), ...]
    The site only shows a window of page numbers at once; we keep clicking the
    'next' arrow (»/›) and fall back to building ?page=N when needed.
    """
    go_to_category(page, cat_url, req_delay)

    all_links: List[Tuple[str, str]] = []
    seen_pages = set()
    pages_done = 0

    # interpret 0 as "unlimited"
    limit = max_pages if max_pages and max_pages > 0 else 10_000

    while True:
        if page.url in seen_pages:
            break
        seen_pages.add(page.url)

        links = harvest_product_links(page)
        all_links.extend(links)

        pages_done += 1
        if pages_done >= limit:
            break

        moved = next_page_if_any(page)
        if not moved:
            break

        if req_delay:
            time.sleep(min(req_delay, 1.0))

    # unique at the very end
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in all_links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((u, t))

    print(f"[cat] {cat_url} → products found: {len(uniq)} across {pages_done} page(s)")
    return uniq


# -------------------- CSV helpers --------------------

CSV_HEADER = [
    "store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw",
    "name","size_text","brand","manufacturer","price","currency",
    "image_url","category_path","category_leaf","source_url"
]

def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def ensure_csv_header(path: str) -> None:
    ensure_dir(path)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)

def append_rows(path: str, rows: List[List[str]]) -> None:
    if not rows:
        return
    ensure_csv_header(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerows(rows)


# -------------------- Runner --------------------

def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def crawl(args) -> None:
    cats = read_lines(args.cats_file)
    skip_ext: set[str] = set(read_lines(args.skip_ext_file)) if args.skip_ext_file and os.path.exists(args.skip_ext_file) else set()
    only_ext: set[str] = set(read_lines(args.only_ext_file)) if args.only_ext_file and os.path.exists(args.only_ext_file) else set()
    only_urls: List[str] = read_lines(args.only_url_file) if args.only_url_file and os.path.exists(args.only_url_file) else []

    total = 0
    headless = bool(int(args.headless))
    req_delay = float(args.req_delay)
    per_cat_page_limit = int(args.max_pages_per_category or "0")

    ensure_csv_header(args.output_csv)

    with sync_playwright() as pw:
        def new_browser():
            b = pw.chromium.launch(headless=headless)
            ctx = b.new_context(locale="et-EE")
            return b, ctx, ctx.new_page()

        browser, ctx, page = new_browser()

        try:
            if only_urls:
                batch: List[List[str]] = []
                for url in only_urls:
                    if int(args.max_products) and total >= int(args.max_products):
                        break
                    ext_id = get_ext_id(url)
                    if skip_ext and ext_id in skip_ext:
                        continue
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
                    batch.append(row)
                    total += 1

                    # periodic flush
                    if len(batch) >= 50:
                        append_rows(args.output_csv, batch)
                        batch.clear()

                    if req_delay:
                        time.sleep(req_delay)

                append_rows(args.output_csv, batch)
            else:
                for idx, cat in enumerate(cats, start=1):
                    if int(args.page_limit) and idx > int(args.page_limit):
                        break
                    leaf_seg = cat.strip("/").split("/")[-1]
                    category_leaf = leaf_seg.replace("-", " ").title()
                    category_path = ""  # filled on PDP

                    prods = collect_category_products(page, cat, req_delay,
                                                     max_pages=per_cat_page_limit if per_cat_page_limit > 0 else 120)
                    if not prods:
                        print(f"[cat] {cat} → 0 items (check if category requires login or geo).")
                        # restart browser even on empty, to be safe
                        try:
                            page.close(); ctx.close(); browser.close()
                        except Exception:
                            pass
                        time.sleep(0.5)
                        browser, ctx, page = new_browser()
                        print("[info] restarted browser (post-category)")
                        continue

                    batch: List[List[str]] = []
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

                        # filter bogus names like "Pealeht"
                        if norm(data.get("name") or "") in BAD_NAMES or norm(data.get("name") or "") == norm(data.get("category_leaf") or category_leaf):
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
                        batch.append(row)
                        total += 1

                        # periodic flush
                        if len(batch) >= 50:
                            append_rows(args.output_csv, batch)
                            batch.clear()

                        if req_delay:
                            time.sleep(req_delay)

                    # flush per-category remainder
                    append_rows(args.output_csv, batch)

                    # harden: restart browser after each category to avoid EPIPE
                    try:
                        page.close(); ctx.close(); browser.close()
                    except Exception:
                        pass
                    time.sleep(0.5)
                    browser, ctx, page = new_browser()
                    print("[info] restarted browser (post-category)")
        finally:
            try:
                page.close(); ctx.close(); browser.close()
            except Exception:
                pass

    # print a summary based on file growth (best effort)
    try:
        lines = sum(1 for _ in open(args.output_csv, "r", encoding="utf-8"))
        print(f"[done] wrote ~{max(0, lines-1)} rows to {args.output_csv}")
    except Exception:
        print(f"[done] wrote rows to {args.output_csv}")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Barbora.ee category→PDP crawler (no EAN).")
    p.add_argument("--cats-file", required=True, help="Text file with category URLs (one per line)")
    p.add_argument("--page-limit", default="0", help="Max categories to process (0=all)")
    p.add_argument("--max-products", default="0", help="Cap total PDPs visited (0=unlimited)")
    p.add_argument("--max-pages-per-category", default="0", help="Cap pages per category (0=unlimited)")
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

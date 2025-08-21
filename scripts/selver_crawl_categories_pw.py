#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selver category crawler (Playwright) -> CSV for staging loader.

Env:
  OUTPUT_CSV        (default: data/selver.csv)
  CATEGORIES_FILE   (default: data/selver_categories.txt)
  PAGE_LIMIT        (default: 0 = unlimited)
  REQ_DELAY         (default: 0.8 seconds polite sleep)
"""

import csv
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "https://www.selver.ee"
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "data/selver.csv")
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))

# --- helpers -----------------------------------------------------------------

def dbg(msg: str):
    print(f"[selver] {msg}", flush=True)

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

SIZE_RE = re.compile(r"\b(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

def guess_size_from_name(name: str) -> str:
    m = SIZE_RE.search(name or "")
    if not m:
        return ""
    num, unit = m.groups()
    return f"{num.replace(',', '.')} {unit.lower()}"

def accept_cookies(page):
    # Try several common phrasings (ET + EN)
    candidates = [
        "Nõustu", "Nõustun", "Nõustun kõik", "Luban kõik", "Aksepteeri",
        "Accept all", "Accept", "Allow all", "I agree",
    ]
    for txt in candidates:
        try:
            btn = page.get_by_role("button", name=re.compile(txt, re.I))
            if btn and btn.count() > 0:
                btn.first.click(timeout=2000)
                time.sleep(0.4)
                return
        except Exception:
            pass
    # fallbacks: any “cookie” consent button
    try:
        btn = page.locator("button:has-text('cookie'), button:has-text('küps')")
        if btn and btn.count() > 0:
            btn.first.click(timeout=2000)
            time.sleep(0.4)
    except Exception:
        pass

def read_categories() -> list[str]:
    paths: list[str] = []
    p = Path(CATEGORIES_FILE)
    if p.exists():
        for ln in p.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            url = ln if ln.startswith("http") else urljoin(BASE, ln)
            paths.append(url)
    if paths:
        return paths
    # small safe default seed (food)
    return [
        urljoin(BASE, "/e-selver/liha-ja-kalatooted/sealiha"),
        urljoin(BASE, "/e-selver/puu-ja-koogiviljad"),
    ]

def with_page_param(url: str, page_no: int) -> str:
    u = urlparse(url)
    qs = dict(parse_qs(u.query))
    qs["p"] = [str(page_no)]
    new_q = urlencode([(k, v[0]) for k, v in qs.items()])
    return u._replace(query=new_q).geturl()

def gather_product_links(page) -> set[str]:
    """Collect product links from a category page."""
    urls = set()
    # Magento product cards usually expose this anchor:
    anchors = page.locator("a.product-item-link[href]")
    try:
        n = anchors.count()
    except PWTimeout:
        n = 0
    for i in range(n):
        try:
            href = anchors.nth(i).get_attribute("href")
            if not href:
                continue
            url = href if href.startswith("http") else urljoin(BASE, href)
            urls.add(url)
        except Exception:
            continue
    return urls

def extract_ean_from_html(html: str) -> str:
    # Look for "Ribakood" followed by digits, or GTIN fields
    m = re.search(r"Ribakood[^0-9]{0,40}(\d{8,14})", html, re.I | re.S)
    if m:
        return m.group(1)
    m = re.search(r"(?:gtin13|gtin|sku)[^0-9]{0,20}(\d{8,14})", html, re.I)
    return m.group(1) if m else ""

def extract_price_currency_from_html(html: str) -> tuple[float, str]:
    # Prefer JSON-ld price, otherwise price-wrapper
    m = re.search(r'"price"\s*:\s*"?(?P<p>[\d.,]+)"?\s*,\s*"priceCurrency"\s*:\s*"(?P<c>[A-Z]{3})"', html, re.I)
    if m:
        p = float(m.group("p").replace(",", "."))
        return p, m.group("c").upper()
    m = re.search(r'data-price-amount="([\d.,]+)"', html, re.I)
    if m:
        return float(m.group(1).replace(",", ".")), "EUR"
    return 0.0, "EUR"

def extract_breadcrumbs_text(page) -> list[str]:
    # Try several breadcrumb containers; return list of crumb names
    sels = [
        "nav.breadcrumbs li a",
        ".breadcrumbs a",
        "nav.breadcrumbs li, .breadcrumbs li",
    ]
    for sel in sels:
        try:
            items = page.locator(sel)
            if items.count() > 0:
                texts = [norm_space(items.nth(i).inner_text()) for i in range(min(items.count(), 12))]
                return [t for t in texts if t]
        except Exception:
            continue
    return []

# --- main crawl ---------------------------------------------------------------

def crawl():
    cats = read_categories()
    dbg(f"Categories to crawl: {len(cats)}")

    # We’ll visit all product pages we see under those categories.
    seen_product_urls: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = ctx.new_page()

        # Phase A: discover product URLs
        for cat in cats:
            total_here = 0
            page_no = 1
            while True:
                if PAGE_LIMIT and page_no > PAGE_LIMIT:
                    break
                url = with_page_param(cat, page_no)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    accept_cookies(page)
                    # Wait for grid; don’t fail the whole crawl if it times out
                    try:
                        page.wait_for_selector("a.product-item-link", timeout=8000)
                    except Exception:
                        pass
                    time.sleep(REQ_DELAY)
                except PWTimeout:
                    break

                found = gather_product_links(page)
                if not found:
                    dbg(f"{url} → +0 products (page {page_no})")
                    # Stop if page has no items
                    break

                new_urls = found - seen_product_urls
                seen_product_urls.update(new_urls)
                total_here += len(new_urls)
                dbg(f"{url} → +{len(new_urls)} products (page {page_no})")

                # Detect presence of “next” link. Magento uses li.pages-item-next > a
                has_next = False
                try:
                    nxt = page.locator("li.pages-item-next a[href]")
                    has_next = nxt.count() > 0
                except Exception:
                    pass

                page_no += 1
                if not has_next:
                    break

        dbg(f"Discovered product URLs: {len(seen_product_urls)}")

        # Phase B: visit each product page and extract fields
        out_path = Path(OUTPUT_CSV)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "ext_id", "name", "ean_raw", "size_text",
                    "price", "currency", "category_path", "category_leaf",
                ],
            )
            w.writeheader()

            for i, url in enumerate(sorted(seen_product_urls)):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    accept_cookies(page)
                    time.sleep(REQ_DELAY)

                    # Name (H1)
                    try:
                        name = norm_space(page.locator("h1").first.inner_text())
                    except Exception:
                        name = ""

                    html = page.content()

                    ean = extract_ean_from_html(html)
                    price, currency = extract_price_currency_from_html(html)

                    crumbs = extract_breadcrumbs_text(page)
                    cat_path = " / ".join(crumbs)
                    leaf = crumbs[-1] if crumbs else ""

                    size_text = guess_size_from_name(name)

                    w.writerow({
                        "ext_id": url,
                        "name": name,
                        "ean_raw": ean,
                        "size_text": size_text,
                        "price": f"{price:.2f}",
                        "currency": currency or "EUR",
                        "category_path": cat_path,
                        "category_leaf": leaf,
                    })
                except Exception:
                    # Keep crawling even if single product fails
                    continue

        browser.close()

    dbg(f"Finished. CSV written: {OUTPUT_CSV}")

if __name__ == "__main__":
    crawl()

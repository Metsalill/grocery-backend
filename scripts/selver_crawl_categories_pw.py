#!/usr/bin/env python3
# Selver category → products (Playwright)
import os
import csv
import json
import re
import time
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no explicit cap
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

HEADERS = [
    "ext_id", "name", "ean_raw", "size_text",
    "price", "currency", "category_path", "category_leaf",
]

EAN_RE = re.compile(r"\b(\d{8,14})\b")

DEFAULT_CATS = [
    "/e-selver/puu-ja-koogiviljad",
    "/e-selver/piimatooted-ja-munad",
    "/e-selver/leiavad-saia-ja-saiakesed",
    "/e-selver/liha-ja-kalatooted",
    "/e-selver/liha-ja-kalatooted/sealiha",
    "/e-selver/liha-ja-kalatooted/veiseliha",
    "/e-selver/liha-ja-kalatooted/kanaliha",
    "/e-selver/liha-ja-kalatooted/kala-ja-mereannid",
    "/e-selver/valmistoit",
    "/e-selver/kuivained",
    "/e-selver/kuivained/pasta-riis-ja-teraviljad",
    "/e-selver/kuivained/jahu-suhkur-ja-kupsetamine",
    "/e-selver/kuivained/konservid-ja-purgitooted",
    "/e-selver/maitseained-kastmed-ja-oliivid",
    "/e-selver/suupisted-ja-maiustused",
    "/e-selver/jook",
    "/e-selver/jook/karastusjoogid-ja-vesi",
    "/e-selver/mahlad-ja-joogid",
    "/e-selver/kulmutatud-toit",
    "/e-selver/laste-toit",
]

def load_categories() -> list[str]:
    if os.path.exists(CATEGORIES_FILE):
        with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
            cats = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    else:
        cats = DEFAULT_CATS
    # normalize to absolute + start from first page explicitly (?p=1)
    out = []
    for c in cats:
        url = c if c.startswith("http") else urljoin(BASE, c)
        if "?" in url:
            out.append(url)
        else:
            out.append(url + "?p=1")
    return out

def accept_cookies(page):
    for sel in [
        "button:has-text('Nõustu')",
        "button:has-text('Nõustu kõigiga')",
        "button:has-text('Accept')",
        "[data-testid*='accept'][role='button']",
        "button[aria-label*='Nõustu']",
    ]:
        try:
            b = page.locator(sel).first
            if b.count() > 0 and b.is_enabled():
                b.click()
                page.wait_for_load_state("domcontentloaded")
                break
        except Exception:
            pass

def wait_idle(page, secs=0.4):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    except PWTimeout:
        pass
    time.sleep(secs)

def page_has_products(page) -> bool:
    try:
        page.wait_for_selector("li.product-item, .product-items .product-item", timeout=6000)
        return True
    except Exception:
        # some categories still lazy-load; scroll a bit
        try:
            page.mouse.wheel(0, 20000)
            page.wait_for_selector("li.product-item, .product-items .product-item", timeout=4000)
            return True
        except Exception:
            return False

def collect_product_links(page) -> set[str]:
    # robust selectors for Selver product cards
    js = """
    () => Array.from(
      document.querySelectorAll(
        'li.product-item a.product-item-link, ' +
        '.product-item-info a.product-item-link, ' +
        '.product-item a[href]:not([href^="#"])'
      )
    ).map(a => a.href)
    """
    hrefs = []
    try:
        hrefs = page.evaluate(js)
    except Exception:
        pass
    urls = set()
    for h in hrefs or []:
        try:
            u = urljoin(BASE, h)
            p = urlparse(u).path
            # exclude category & internal anchors; product links on Selver do NOT start with /e-selver
            if not p.startswith("/e-selver") and not p.startswith("/cart") and not p.startswith("/customer"):
                urls.add(u.split("?")[0])
        except Exception:
            continue
    return urls

def next_page_url(cur_url: str, page_idx: int) -> str:
    # Selver supports ?p=2,3… for pagination
    if "p=" in cur_url:
        base = re.sub(r"[?&]p=\d+", "", cur_url)
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}p={page_idx}"
    else:
        sep = "&" if "?" in cur_url else "?"
        return f"{cur_url}{sep}p={page_idx}"

def parse_jsonld(texts: list[str]) -> dict | None:
    for raw in texts:
        try:
            data = json.loads(raw)
            block = None
            if isinstance(data, list):
                for it in data:
                    t = it.get("@type")
                    if (isinstance(t, str) and t.lower() == "product") or (isinstance(t, list) and "Product" in t):
                        block = it; break
            elif isinstance(data, dict):
                t = data.get("@type")
                if (isinstance(t, str) and t.lower() == "product") or (isinstance(t, list) and "Product" in t):
                    block = data
            if block:
                return block
        except Exception:
            continue
    return None

def extract_from_product(page, url: str) -> dict | None:
    # name & breadcrumbs
    try:
        name = page.locator("h1").first.inner_text().strip()
    except Exception:
        name = ""

    # JSON-LD for price/currency (+ optional gtin)
    price = 0.0
    currency = "EUR"
    ean = ""
    try:
        scripts = [page.locator("script[type='application/ld+json']").nth(i).inner_text()
                   for i in range(min(6, page.locator("script[type='application/ld+json']").count()))]
        ld = parse_jsonld(scripts)
        if ld:
            offers = ld.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            pr = str(offers.get("price", "0")).replace(",", ".") or "0"
            try:
                price = float(pr)
            except Exception:
                price = 0.0
            currency = (offers.get("priceCurrency") or "EUR").upper()
            ean = str(ld.get("gtin13") or ld.get("gtin") or ld.get("sku") or "").strip()
    except Exception:
        pass

    # EAN fallback from “Ribakood” field
    if not ean:
        val = ""
        try:
            # exact text “Ribakood” → closest following value
            lab = page.locator("xpath=//*[normalize-space()='Ribakood']").first
            if lab.count() > 0:
                dd = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if dd.count() > 0:
                    val = dd.inner_text().strip()
        except Exception:
            pass
        if not val:
            try:
                html = page.content()
                m = re.search(r"Ribakood\s*</[^>]*>\s*([^<>{}]+)<", html, re.I)
                if m:
                    val = re.sub(r"<[^>]+>", " ", m.group(1)).strip()
            except Exception:
                pass
        m = EAN_RE.search(val)
        if m:
            ean = m.group(1)

    # breadcrumbs -> category path/leaf
    crumbs = []
    try:
        for a in page.locator("nav a, .breadcrumbs a").all():
            try:
                t = a.inner_text().strip()
                if t and t.lower() not in {"e-selver"}:
                    crumbs.append(t)
            except Exception:
                continue
    except Exception:
        pass
    category_path = " / ".join(crumbs)
    category_leaf = crumbs[-1] if crumbs else ""

    # quick size from name
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml)\b", name, re.I)
    size_text = m.group(0).replace(",", ".") if m else ""

    if not name:
        return None
    return {
        "ext_id": url,
        "name": name,
        "ean_raw": ean,
        "size_text": size_text,
        "price": price,
        "currency": currency,
        "category_path": category_path,
        "category_leaf": category_leaf,
    }

def main():
    cats = load_categories()
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f, sync_playwright() as pw:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = context.new_page()

        all_products = set()

        for cat in cats:
            cur_page = 1
            seen_zero = 0
            while True:
                url = next_page_url(cat, cur_page)
                try:
                    page.goto(url, timeout=30000)
                except PWTimeout:
                    break
                accept_cookies(page)
                wait_idle(page, REQ_DELAY)

                if not page_has_products(page):
                    seen_zero += 1
                    if seen_zero >= 1:
                        break
                links = collect_product_links(page)
                print(f"[selver] {url} → +{len(links)} products (page {cur_page})")
                if not links:
                    break

                all_products |= links

                cur_page += 1
                if PAGE_LIMIT and cur_page > PAGE_LIMIT:
                    break

        # Visit products
        for i, url in enumerate(sorted(all_products)):
            try:
                page.goto(url, timeout=30000)
                accept_cookies(page)
                wait_idle(page, REQ_DELAY)
                rec = extract_from_product(page, url)
                if rec:
                    writer.writerow(rec)
            except Exception:
                continue

        browser.close()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os, re, csv, time, json
from datetime import datetime, timezone
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))   # 0 = no limit
SLEEP = float(os.getenv("REQ_DELAY", "0.8"))

# optional curated list (one URL per line); else use a reasonable default set
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")
DEFAULT_CATS = [
    "/e-selver/puu-ja-koogiviljad",
    "/e-selver/kuivained/pasta-riis-ja-teraviljad",
    "/e-selver/kuivained/jahu-suhkur-ja-kupsetamine",
    "/e-selver/kuivained/konservid-ja-purgitooted",
    "/e-selver/liha-ja-kalatooted/sealiha",
    "/e-selver/liha-ja-kalatooted/veiseliha",
    "/e-selver/liha-ja-kalatooted/kanaliha",
    "/e-selver/liha-ja-kalatooted/kala-ja-mereannid",
    "/e-selver/piimatooted-ja-munad",
    "/e-selver/jook/karastusjoogid-ja-vesi",
    "/e-selver/kulmutatud-toit",
    "/e-selver/maitseained-kastmed-ja-oliivid",
    "/e-selver/supistised-ja-maiustused",
    "/e-selver/laste-toit",
]

SIZE_RE = re.compile(r"\b(\d+(?:[.,]\d+)?)\s?(kg|g|l|ml|cl|dl)\b", re.I)

def guess_size(title: str) -> str:
    m = SIZE_RE.search(title or "")
    if not m: return ""
    n,u = m.groups()
    return f"{n.replace(',','.') } {u.lower()}"

def read_category_list() -> list[str]:
    if os.path.exists(CATEGORIES_FILE):
        with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
            rows = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
        return [r if r.startswith("http") else urljoin(BASE, r) for r in rows]
    return [urljoin(BASE, p) for p in DEFAULT_CATS]

def accept_cookies(page):
    for sel in [
        "button:has-text('Nõustu')",
        "button:has-text('Nõustun')",
        "button:has-text('Accept')",
        "[data-testid*='accept']",
        "button[aria-label*='accept']",
    ]:
        try:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_enabled():
                btn.first.click()
                page.wait_for_load_state("domcontentloaded")
                break
        except Exception:
            pass

def collect_product_links_in_page(page) -> set[str]:
    """Return product links found in the current category page."""
    # Selver (Magento 2) uses <a class="product-item-link" href="...">
    links = set()
    try:
        page.wait_for_selector("a.product-item-link[href]", timeout=7000)
    except Exception:
        pass

    # Scroll to bottom a few times to ensure content is loaded
    last_h = 0
    for _ in range(6):
        try:
            page.mouse.wheel(0, 18000)
            time.sleep(SLEEP)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
        except Exception:
            break

    anchors = page.locator("a.product-item-link[href]")
    try:
        count = anchors.count()
    except PWTimeout:
        count = 0

    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href")
            if href:
                links.add(href)
        except Exception:
            continue
    return links

def next_page(page) -> bool:
    """Try to move to next pagination page; return True if navigated."""
    for sel in [
        "li.pages-item-next a",
        "a[rel='next']",
        "a.pagination__next",
        "a:has-text('Järgmine')",
        "a:has-text('Next')",
    ]:
        try:
            nxt = page.locator(sel)
            if nxt.count() > 0 and nxt.first.is_enabled():
                nxt.first.click()
                page.wait_for_load_state("domcontentloaded")
                time.sleep(SLEEP)
                return True
        except Exception:
            continue
    return False

def parse_jsonld(text: str) -> list[dict]:
    out = []
    try:
        data = json.loads(text)
        if isinstance(data, list): out.extend(data)
        else: out.append(data)
    except Exception:
        pass
    return out

def extract_from_product(page, url: str) -> dict | None:
    title = page.locator("h1").first.inner_text(timeout=0) or ""
    title = re.sub(r"\s+", " ", title).strip()

    # JSON-LD
    ean, price, currency = "", 0.0, "EUR"
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(8, scripts.count())):
            for block in parse_jsonld(scripts.nth(i).inner_text()):
                ty = block.get("@type")
                if (isinstance(ty, list) and any("Product" in str(t) for t in ty)) or str(ty).lower().endswith("product"):
                    ean = re.sub(r"\D", "", str(block.get("gtin13") or block.get("gtin") or block.get("sku") or ""))
                    offers = block.get("offers") or {}
                    if isinstance(offers, list): offers = offers[0] if offers else {}
                    p = str(offers.get("price", "0")).replace(",", ".")
                    try: price = float(p)
                    except Exception: price = 0.0
                    currency = (offers.get("priceCurrency") or "EUR").upper()
    except Exception:
        pass

    # Fallback: read “Ribakood” from details block
    if not ean:
        try:
            ribakood = page.locator("xpath=//*[normalize-space()='Ribakood']/following::*[self::div or self::span or self::p][1]").first
            if ribakood.count() > 0:
                ean = re.sub(r"\D", "", ribakood.inner_text())
        except Exception:
            pass

    # Breadcrumbs → category path
    cat_path = ""
    try:
        crumbs = page.locator("nav.breadcrumbs li a, nav.breadcrumbs li span")
        parts = []
        for i in range(crumbs.count()):
            t = re.sub(r"\s+", " ", crumbs.nth(i).inner_text()).strip()
            if t and t.lower() not in {"e-selver"}:
                parts.append(t)
        cat_path = " / ".join(parts)
    except Exception:
        pass

    if not title: return None
    return {
        "ext_id": url,
        "name": title,
        "ean_raw": ean,
        "size_text": guess_size(title),
        "price": price,
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": cat_path.split(" / ")[-1] if cat_path else "",
    }

def main():
    cats = read_category_list()
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    written = 0

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"
        ])
        w.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ))
            page = ctx.new_page()

            for cat in cats:
                try:
                    page.goto(cat, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    accept_cookies(page)
                except Exception:
                    continue

                total_pages = 0
                while True:
                    links = collect_product_links_in_page(page)
                    print(f"[selver] {cat} → +{len(links)} products (page {total_pages+1})")

                    for href in sorted(links):
                        try:
                            page.goto(href, timeout=30000)
                            page.wait_for_load_state("domcontentloaded")
                            time.sleep(SLEEP)
                            rec = extract_from_product(page, href)
                            if rec:
                                w.writerow(rec)
                                written += 1
                        except Exception:
                            continue

                    total_pages += 1
                    if PAGE_LIMIT and total_pages >= PAGE_LIMIT:
                        break
                    if not next_page(page):
                        break

            browser.close()

    print(f"[selver] Finished. Rows written: {written}")

if __name__ == "__main__":
    main()

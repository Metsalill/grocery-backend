#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver (e-selver) category crawler -> CSV (for staging_selver_products)
Auto-discovers food categories, paginates/scrolls, then visits product pages and extracts:
  ext_id (url), name, ean_raw, size_text, price, currency, category_path, category_leaf
"""

from __future__ import annotations
import os
import re
import csv
import time
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, TimeoutError

BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no limit
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

# ---- block noisy third-party domains that sometimes hijack navigation
BLOCK_HOSTS = {
    "adobe.com", "assets.adobedtm.com", "adobedtm.com", "demdex.net", "omtrdc.net",
    "googletagmanager.com", "google-analytics.com", "doubleclick.net", "facebook.net",
}

def _should_block(url: str) -> bool:
    h = urlparse(url).netloc.lower()
    return any(h == d or h.endswith("." + d) for d in BLOCK_HOSTS)

# ---- obvious non-food category keywords
BANNED_KEYWORDS = {
    "sisustus", "kodutekstiil", "valgustus", "kardin", "jouluvalgustid",
    "vaikesed-sisustuskaubad", "kuunlad", "kirja-ja-kontoritarbed",
    "remondi-ja-turvatooted", "omblus-ja-kasitootarbed", "meisterdamine",
    "ajakirjad", "autojuhtimine", "kotid", "aed-ja-lilled", "lemmikloom",
    "sport", "pallimangud", "jalgrattasoit", "ujumine", "matkamine",
    "tervisesport", "manguasjad", "lutid", "lapsehooldus", "ideed-ja-hooajad",
    "kodumasinad", "elektroonika", "meelelahutuselektroonika",
    "vaikesed-kodumasinad", "lambid-patareid-ja-taskulambid",
    "ilu-ja-tervis", "kosmeetika", "meigitooted", "hugieen",
    "loodustooted-ja-toidulisandid",
}

# ---- product-ish link filter for selver.ee
NON_PRODUCT_PATH_SNIPPETS = {
    "/e-selver/", "/ostukorv", "/cart", "/checkout", "/search", "/otsi",
    "/konto", "/customer", "/login", "/logout", "/registreeru", "/uudised",
    "/tootajad", "/kontakt", "/tingimused", "/privaatsus", "/privacy",
    "/kampaania", "/kampaaniad", "/blogi", "/app", "/store-locator",
}

def _is_selver_product_like(url: str) -> bool:
    u = urlparse(url)
    host_ok = u.netloc.lower().endswith("selver.ee") or (u.netloc == "" and url.startswith("/"))
    if not host_ok:
        return False
    path = (u.path or "/").lower()
    if any(sn in path for sn in NON_PRODUCT_PATH_SNIPPETS):
        return False
    # product slugs have no “file.ext” at the end
    if "." in path.rsplit("/", 1)[-1]:
        return False
    # at least one segment (e.g. /sea-kaelakarbonaad-rakvere-lk-kg)
    return path.count("/") >= 1

# ---- helpers
SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def guess_size_from_title(title: str) -> str:
    m = SIZE_RE.search(title or "")
    if not m:
        return ""
    num, unit = m.groups()
    return f"{num.replace(',', '.')} {unit.lower()}"

def is_food_category(path: str) -> bool:
    p = path.lower()
    if not p.startswith("/e-selver/"):
        return False
    return not any(bad in p for bad in BANNED_KEYWORDS)

def safe_goto(page, url: str, timeout: int = 30000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        return True
    except TimeoutError:
        return False

def accept_cookies(page):
    for sel in [
        "button:has-text('Nõustu')",
        "button:has-text('Nõustun')",
        "button:has-text('Accept')",
        "button[aria-label*='accept']",
    ]:
        try:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_enabled():
                btn.first.click(timeout=3000)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(0.3)
                return
        except Exception:
            pass

def discover_categories(page, start_urls: list[str]) -> list[str]:
    """BFS over category pages; read sidebar & content category tiles."""
    seen, queue, out = set(), list(start_urls), []

    def push(url: str):
        path = urlparse(url).path
        if path in seen:
            return
        if is_food_category(path):
            seen.add(path)
            queue.append(url)

    while queue:
        url = queue.pop(0)
        if not safe_goto(page, url):
            continue
        time.sleep(REQ_DELAY)
        out.append(url)

        for sel in [
            "nav a[href*='/e-selver/']",
            "aside a[href*='/e-selver/']",
            "a.category-card[href*='/e-selver/']",
            "a[href*='/e-selver/']",
        ]:
            try:
                links = page.locator(sel)
                for i in range(min(500, links.count())):
                    href = links.nth(i).get_attribute("href")
                    if not href:
                        continue
                    u = urljoin(BASE, href)
                    if is_food_category(urlparse(u).path):
                        push(u)
            except Exception:
                pass

    uniq, _seen = [], set()
    for u in out:
        p = urlparse(u).path
        if p not in _seen:
            uniq.append(u)
            _seen.add(p)
    return uniq

# ------------ product grid handling (scroll + paginate) ------------
def _scroll_until_stable(page, rounds: int = 8) -> None:
    """Scrolls to bottom a few times to trigger lazy-loading grids."""
    last_h = -1
    for _ in range(rounds):
        try:
            cur = page.evaluate("document.body.scrollHeight")
            page.mouse.wheel(0, 20000)
            time.sleep(0.6)
            new = page.evaluate("document.body.scrollHeight")
            if new == last_h or new == cur:
                break
            last_h = new
        except Exception:
            break

def collect_product_links(page, page_limit: int = 0) -> set[str]:
    """
    On a category page, scroll & collect product card links.
    Selver (Magento) typically uses `.product-item-link`.
    """
    links: set[str] = set()
    pages_seen = 0

    # selectors that most reliably point to the product page
    PRODUCT_LINK_SELECTORS = [
        "a.product-item-link",
        "li.product-item a.product-item-link",
        "div.product-item-info a.product-item-link",
        # fallbacks:
        "a[href^='/']:not([href*='/e-selver/'])",
        "a[href^='https://www.selver.ee/']:not([href*='/e-selver/'])",
    ]

    def grab_cards() -> int:
        found = 0
        try:
            # give the grid a moment, then scroll to hydrate lazy tiles
            page.wait_for_selector("li.product-item, .products-grid, a.product-item-link", timeout=4000)
        except Exception:
            pass
        _scroll_until_stable(page, rounds=6)

        for sel in PRODUCT_LINK_SELECTORS:
            try:
                as_ = page.locator(sel)
                cnt = as_.count()
                for i in range(cnt):
                    href = as_.nth(i).get_attribute("href")
                    if not href:
                        continue
                    u = urljoin(BASE, href)
                    if _is_selver_product_like(u):
                        if u not in links:
                            links.add(u)
                            found += 1
            except Exception:
                continue
        return found

    def next_selector():
        for sel in [
            "a[rel='next']",
            "a[aria-label*='Next']",
            "a.page:has-text('>')",
            "li.pages-item-next a",
            "button:has-text('Näita rohkem')",
            "button:has-text('Load more')",
        ]:
            if page.locator(sel).count() > 0:
                return sel
        return None

    while True:
        pages_seen += 1
        grab_cards()
        if page_limit and pages_seen >= page_limit:
            break

        nxt = next_selector()
        if not nxt:
            break
        try:
            page.locator(nxt).first.click(timeout=5000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(REQ_DELAY)
        except Exception:
            break

    return links

# ------------ detail-page extractors ------------
def breadcrumbs(page) -> list[str]:
    for sel in [
        "nav ol li a",
        "nav.breadcrumbs a",
        "ol.breadcrumbs a",
        "nav[aria-label*='crumb'] a",
    ]:
        try:
            items = page.locator(sel)
            if items.count() > 0:
                vals = []
                for i in range(items.count()):
                    vals.append(normspace(items.nth(i).inner_text()))
                vals = [v for v in vals if v and v.lower() != "e-selver"]
                if vals:
                    return vals
        except Exception:
            pass
    return []

def extract_price(page) -> tuple[float, str]:
    # Prefer explicit price elements, then fallback to any € text.
    try:
        p = page.locator("span.price, .price").first
        if p and p.count() > 0:
            txt = p.inner_text()
            m = re.search(r"(\d+(?:[.,]\d+)?)", txt)
            if m:
                return float(m.group(1).replace(",", ".")), "EUR"
    except Exception:
        pass
    for sel in ["text=/€/"]:
        try:
            node = page.locator(sel).first
            if node and node.count() > 0:
                txt = node.inner_text()
                m = re.search(r"(\d+(?:[.,]\d+)?)\s*€", txt)
                if m:
                    return float(m.group(1).replace(",", ".")), "EUR"
        except Exception:
            pass
    return 0.0, "EUR"

def extract_ean(page, url: str) -> str:
    labels = ["Ribakood", "EAN", "EAN-kood", "EAN kood", "GTIN"]
    for lab in labels:
        try:
            el = page.locator(f"xpath=//*[normalize-space()='{lab}']").first
            if el.count() > 0:
                nxt = el.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if nxt.count() > 0:
                    txt = normspace(nxt.inner_text())
                    if re.fullmatch(r"\d{8,14}", txt):
                        return txt
        except Exception:
            pass
    try:
        html = page.content()
        m = re.search(r"(?:Ribakood|EAN(?:-kood)?)\D*?(\d{8,14})", html, re.I)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""

# ------------ main ------------
def crawl():
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "ext_id", "name", "ean_raw", "size_text", "price", "currency",
                "category_path", "category_leaf",
            ],
        )
        w.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
            )
            context.route("**/*", lambda route, req:
                          route.abort() if _should_block(req.url) else route.continue_())
            page = context.new_page()

            # Seeds: explicit file or autodiscover from /e-selver
            seeds: list[str] = []
            if os.path.exists(CATEGORIES_FILE):
                with open(CATEGORIES_FILE, "r", encoding="utf-8") as cf:
                    for ln in cf:
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            seeds.append(urljoin(BASE, ln))
            if not seeds and safe_goto(page, urljoin(BASE, "/e-selver")):
                accept_cookies(page)
                time.sleep(REQ_DELAY)
                top = set()
                for sel in ["a[href*='/e-selver/']", "nav a[href*='/e-selver/']", "aside a[href*='/e-selver/']"]:
                    try:
                        aa = page.locator(sel)
                        for i in range(aa.count()):
                            href = aa.nth(i).get_attribute("href")
                            if not href:
                                continue
                            u = urljoin(BASE, href)
                            if is_food_category(urlparse(u).path):
                                top.add(u)
                    except Exception:
                        pass
                seeds = sorted(top)

            cats = discover_categories(page, seeds)
            print(f"[selver] writing CSV -> {OUTPUT}")
            print(f"[selver] Categories to crawl: {len(cats)}")
            for cu in cats:
                print(f"[selver] {cu}")

            # Collect product URLs
            product_urls: set[str] = set()
            for cu in cats:
                if not safe_goto(page, cu):
                    continue
                time.sleep(REQ_DELAY)
                links = collect_product_links(page, page_limit=PAGE_LIMIT)
                product_urls.update(links)
                print(f"[selver] {cu} -> +{len(links)} products (total so far: {len(product_urls)})")

            # Visit product pages -> write rows
            wrote = 0
            for i, pu in enumerate(sorted(product_urls)):
                if not _is_selver_product_like(pu):
                    continue
                if not safe_goto(page, pu):
                    continue
                time.sleep(REQ_DELAY)

                try:
                    name = normspace(page.locator("h1").first.inner_text())
                except Exception:
                    name = ""

                price, currency = extract_price(page)
                ean = extract_ean(page, pu)
                size_text = guess_size_from_title(name)
                crumbs = breadcrumbs(page)
                cat_path = " / ".join(crumbs)
                cat_leaf = crumbs[-1] if crumbs else ""

                if not name:
                    continue

                w.writerow({
                    "ext_id": pu,
                    "name": name,
                    "ean_raw": ean,
                    "size_text": size_text,
                    "price": f"{price:.2f}",
                    "currency": currency,
                    "category_path": cat_path,
                    "category_leaf": cat_leaf,
                })
                wrote += 1
                if (i + 1) % 25 == 0:
                    f.flush()

            browser.close()
            print(f"[selver] wrote {wrote} product rows.")

if __name__ == "__main__":
    try:
        crawl()
    except KeyboardInterrupt:
        pass

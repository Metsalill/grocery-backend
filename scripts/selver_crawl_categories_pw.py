#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver (e-selver) category crawler → CSV (for staging_selver_products)

Auto-discovers food categories from /e-selver, paginates category listings,
then visits product pages and extracts:
  ext_id (url), name, ean_raw, size_text, price, currency, category_path, category_leaf
"""

from __future__ import annotations
import os
import re
import csv
import time
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, TimeoutError

# ---------------------------------------------------------------------------
# Config
BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no page cap
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

# Ban obvious non-food areas (same spirit as Prisma)
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

# Third-party hosts that sometimes hijack navigation (privacy banner / analytics)
BLOCK_HOSTS = {
    "adobe.com", "assets.adobedtm.com", "adobedtm.com", "demdex.net", "omtrdc.net",
    "googletagmanager.com", "google-analytics.com", "doubleclick.net", "facebook.net",
}

# Obvious non-product path snippets we should filter out
NON_PRODUCT_PATH_SNIPPETS = {
    "/e-selver/", "/ostukorv", "/cart", "/checkout", "/search", "/otsi",
    "/konto", "/customer", "/login", "/logout", "/registreeru", "/uudised",
    "/tootajad", "/kontakt", "/tingimused", "/privaatsus", "/privacy",
    "/kampaania", "/kampaaniad", "/blogi", "/app", "/store-locator",
}

SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

# ---------------------------------------------------------------------------
# Helpers

def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def guess_size_from_title(title: str) -> str:
    m = SIZE_RE.search(title or "")
    if not m:
        return ""
    num, unit = m.groups()
    return f"{num.replace(',', '.')} {unit.lower()}"

def is_food_category(path: str) -> bool:
    """We only want /e-selver/... categories and skip banned keywords."""
    p = path.lower()
    if not p.startswith("/e-selver/"):
        return False
    return not any(bad in p for bad in BANNED_KEYWORDS)

def _should_block(url: str) -> bool:
    h = urlparse(url).netloc.lower()
    return any(h == d or h.endswith("." + d) for d in BLOCK_HOSTS)

def _is_selver_product_like(url: str) -> bool:
    u = urlparse(url)
    host_ok = u.netloc.lower().endswith("selver.ee") or (u.netloc == "" and url.startswith("/"))
    if not host_ok:
        return False
    path = u.path or "/"
    # exclude category & obvious non-product paths
    if any(sn in path.lower() for sn in NON_PRODUCT_PATH_SNIPPETS):
        return False
    # products are sluggy paths (no file extension in the last segment)
    if "." in path.rsplit("/", 1)[-1]:
        return False
    # at least 1 slash (i.e., not just '/')
    return path.count("/") >= 1

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

        # left-nav / tiles / chips / “sub categories”
        for sel in [
            "nav a[href*='/e-selver/']",
            "aside a[href*='/e-selver/']",
            "a.category-card, a[href*='/e-selver/']",
        ]:
            try:
                links = page.locator(sel)
                cnt = min(800, links.count())
                for i in range(cnt):
                    href = links.nth(i).get_attribute("href")
                    if not href:
                        continue
                    u = urljoin(BASE, href)
                    if is_food_category(urlparse(u).path):
                        push(u)
            except Exception:
                pass

    # unique by path, keep first
    uniq, seenp = [], set()
    for u in out:
        p = urlparse(u).path
        if p not in seenp:
            uniq.append(u)
            seenp.add(p)
    return uniq

def collect_product_links(page, page_limit: int = 0) -> set[str]:
    """On a category page, paginate & collect product card links."""
    links: set[str] = set()
    pages_seen = 0

    def grab_cards():
        found = 0
        # Be generous with selectors, then filter with _is_selver_product_like().
        for sel in [
            "a.product-item-link",          # common Magento-ish selector
            "a.product-card__link",
            "a[href][data-product-id]",
            "article a[href]",
            "li a[href]",
            "div a[href]",
            "a[href]:has(img)",
        ]:
            try:
                as_ = page.locator(sel)
                cnt = min(1200, as_.count())
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
                pass
        return found

    def next_selector():
        for sel in [
            "a[rel='next']",
            "a[aria-label*='Next']",
            "button:has-text('Näita rohkem')",
            "button:has-text('Load more')",
            "a.page-link:has-text('>')",
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

def breadcrumbs(page) -> list[str]:
    for sel in [
        "nav ol li a",
        "nav.breadcrumbs a",
        "ol.breadcrumbs a",
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
    # find something like "5,99 €" on product page
    for sel in [
        "text=/€/",
        "span:has-text('€')",
        "div:has-text('€')",
        "p:has-text('€')",
    ]:
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
    # read nearby labels (Ribakood / EAN / EAN-kood)
    labels = ["Ribakood", "EAN", "EAN-kood", "EAN kood", "GTIN"]
    # 1) label → following value
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
    # 2) regex over HTML
    try:
        html = page.content()
        m = re.search(r"(?:Ribakood|EAN(?:-kood)?)\D*?(\d{8,14})", html, re.I)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""

# ---------------------------------------------------------------------------
# Crawl

def crawl():
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    print(f"[selver] writing CSV -> {OUTPUT}")

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
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                )
            )
            # Block third-party/analytics that can hijack navigation (Adobe privacy, etc.)
            context.route(
                "**/*",
                lambda route, req: route.abort()
                if _should_block(req.url) else route.continue_()
            )

            page = context.new_page()

            # Seeds: from file or autodiscover from /e-selver
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
                for sel in [
                    "a[href*='/e-selver/']",
                    "nav a[href*='/e-selver/']",
                    "aside a[href*='/e-selver/']",
                ]:
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

            # Discover nested categories
            cats = discover_categories(page, seeds)
            print(f"[selver] Categories to crawl: {len(cats)}")
            for cu in cats:
                print(f"[selver] {cu}")

            # Gather product URLs
            product_urls: set[str] = set()
            for cu in cats:
                if not safe_goto(page, cu):
                    continue
                time.sleep(REQ_DELAY)
                links = collect_product_links(page, page_limit=PAGE_LIMIT)
                product_urls.update(links)
                print(f"[selver] {cu} → +{len(links)} products (total so far: {len(product_urls)})")

            # Visit product pages → rows
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
                    continue  # skip junk rows

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

                if (i + 1) % 25 == 0:
                    f.flush()

            browser.close()

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        crawl()
    except KeyboardInterrupt:
        pass

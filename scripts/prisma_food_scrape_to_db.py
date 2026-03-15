#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prisma.ee scraper -> Postgres via upsert_product_and_price()

KEY CHANGE: Loads known product URLs from DB first (Phase A bypass).
Phase A (category crawling) only runs if DB has fewer URLs than max_products.
This means after the first successful run, subsequent runs go straight to
visiting PDPs and updating prices — much faster, fits in 25min chunks.
"""

from __future__ import annotations

import argparse
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
import json
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ---------------------------------------------------------------------------
BASE = "https://prismamarket.ee"
SEEDS = ["/en/tooted/", "/tooted/"]
CATEGORY_PREFIXES = ("/en/tooted/", "/tooted/", "/en/food-market/", "/food-market/")
PRODUCT_PREFIXES = ("/en/toode/", "/toode/")

PACK_RE      = re.compile(r"(\d+)\s*[x×]\s*(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
SIMPLE_RE    = re.compile(r"\b(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
PIECES_RE    = re.compile(r"\b(\d+)\s*(?:tk|pcs?|pk|pack)\b", re.I)
BONUS_RE     = re.compile(r"\+\s*\d+%")
EAN_RE       = re.compile(r"(\d{8,14})$")
PRICE_NUM_RE = re.compile(r"(\d+[\.,]\d+)")

# ---------------------------------------------------------------------------
def jitter(a: float = 0.6, b: float = 1.4) -> None:
    time.sleep(random.uniform(a, b))

def clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def is_category_path(path: str) -> bool:
    return any(path.startswith(p) for p in CATEGORY_PREFIXES)

def is_product_path(path: str) -> bool:
    p = path.lower()
    return any(p.startswith(pref) for pref in PRODUCT_PREFIXES)

EXCLUDED_CATEGORY_KEYWORDS = [
    "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid","vaikesed-sisustuskaubad","kuunlad",
    "kook-ja-lauakatmine","uhekordsed-noud","kirja-ja-kontoritarbed","remondi-ja-turvatooted",
    "kulmutus-ja-kokkamisvahendid","omblus-ja-kasitootarbed","meisterdamine","ajakirjad","autojuhtimine",
    "kotid","aed-ja-lilled","lemmikloom","sport","pallimangud","jalgrattasoit","ujumine","matkamine",
    "tervisesport","manguasjad","lutid","lapsehooldus","ideed-ja-hooajad","kodumasinad","elektroonika",
    "meelelahutuselektroonika","vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
    "ilu-ja-tervis","kosmeetika","meigitooted","hugieen","loodustooted-ja-toidulisandid",
]

def is_in_whitelist(url: str) -> bool:
    path = urlparse(url).path.lower()
    if not is_category_path(path):
        return False
    if any(ex in path for ex in EXCLUDED_CATEGORY_KEYWORDS):
        return False
    return True

# ---------------------------------------------------------------------------
def get_database_url() -> str:
    try:
        import settings  # type: ignore
        db = getattr(settings, "DATABASE_URL", None)
        if db:
            return db
    except Exception:
        pass
    db = os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("DATABASE_URL not set")
    return db

def db_connect() -> PGConn:
    conn = psycopg2.connect(get_database_url())
    conn.autocommit = True
    return conn

def get_store_id_for_prisma() -> int:
    try:
        return int(os.environ.get("STORE_ID", "14") or "14")
    except Exception:
        return 14

# ---------------------------------------------------------------------------
def load_urls_from_db(conn: PGConn) -> list[str]:
    """
    Load known Prisma product URLs from DB.
    Stored as source_url in prices during previous scrape runs.
    """
    query = """
        SELECT DISTINCT p.source_url
        FROM public.prices p
        JOIN public.stores s ON s.id = p.store_id
        WHERE s.chain = 'Prisma'
          AND s.is_online = TRUE
          AND p.source_url IS NOT NULL
          AND p.source_url <> ''
          AND p.source_url LIKE '%/toode/%'
        ORDER BY p.source_url;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            urls = [r[0] for r in rows if r[0]]
            print(f"[db] loaded {len(urls)} known product URLs from DB")
            return urls
    except Exception as e:
        print(f"[db] failed to load URLs: {e}")
        return []

# ---------------------------------------------------------------------------
def extract_title(page) -> str:
    try:
        return clean(page.locator("h1").first.inner_text())
    except Exception:
        return ""

def extract_image_url(page) -> str:
    for sel in ["main img[alt][src]", "img[alt][src]", "img[src]"]:
        try:
            img = page.locator(sel).first
            if img.count() > 0:
                src = img.get_attribute("src")
                if src:
                    return urljoin(BASE, src)
        except Exception:
            continue
    return ""

def extract_label_value(page, labels: list[str]) -> str:
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[normalize-space(.)='{label}']").first
            if lab.count() > 0:
                sib = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception:
            pass
    for label in labels:
        try:
            lab = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
            if lab.count() > 0:
                sib = lab.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if sib.count() > 0:
                    return clean(sib.inner_text())
        except Exception:
            pass
    try:
        for label in labels:
            dt = page.locator(
                "xpath=//dt[normalize-space()='{0}'] | //dt[contains(normalize-space(),'{0}')]"
                .format(label)
            ).first
            if dt.count() > 0:
                dd = dt.locator("xpath=following-sibling::dd[1]")
                if dd.count() > 0:
                    return clean(dd.inner_text())
    except Exception:
        pass
    try:
        html = page.content()
        for label in labels:
            m = re.search(fr"{re.escape(label)}\s*:?\s*</?[^>]*>?(.*?)<", html, re.I | re.S)
            if m:
                txt = re.sub(r"<[^>]+>", " ", m.group(1))
                txt = clean(txt)
                if txt:
                    return txt
    except Exception:
        pass
    return ""

def extract_ean(page, url: str) -> str:
    val = extract_label_value(page, ["EAN", "EAN-kood", "Ribakood", "EAN code"])
    if val and re.fullmatch(r"\d{8,14}", val):
        return val
    m = EAN_RE.search(url)
    if m:
        return m.group(1)
    return ""

def extract_country(page) -> str:
    return extract_label_value(page, ["Country of manufacture", "Country of origin", "Valmistajariik", "Päritoluriik"])

def extract_manufacturer(page) -> str:
    return extract_label_value(page, ["Manufacturer", "Tootja", "Producer", "Valmistaja"])

def parse_amount_from_title(title: str) -> str:
    t = BONUS_RE.sub("", title)
    m = PACK_RE.search(t)
    if m:
        qty, num, unit = m.groups()
        return f"{qty}x{num.replace(',','.')} {unit}"
    m = SIMPLE_RE.search(t)
    if m:
        num, unit = m.groups()
        return f"{num.replace(',','.')} {unit}"
    m = PIECES_RE.search(t)
    if m:
        return f"{m.group(1)} pcs"
    return ""

def normalize_size_text(s: str) -> str:
    if not s:
        return ""
    s = clean(s).lower().replace(",", ".")
    m = PACK_RE.search(s)
    if m:
        qty, num, unit = m.groups()
        return f"{int(qty)}x{num} {unit}"
    m = SIMPLE_RE.search(s)
    if m:
        num, unit = m.groups()
        return f"{num} {unit}"
    m = PIECES_RE.search(s)
    if m:
        return f"{m.group(1)} pcs"
    if re.search(r"\bkg\b", s):
        return "kg"
    if re.search(r"\bl\b", s):
        return "l"
    return ""

def extract_size_text(page, title: str) -> str:
    lbl_val = extract_label_value(page, [
        "Net weight", "Net quantity", "Net content", "Net mass",
        "Kogus", "Maht", "Neto kogus", "Netokogus", "Pakendi suurus",
        "Pakendi maht", "Suurus", "Kaal", "Weight", "Volume", "Size",
        "Net weight / Net volume",
    ])
    size = normalize_size_text(lbl_val)
    if size:
        return size
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(8, scripts.count())):
            raw = scripts.nth(i).inner_text()
            data = json.loads(raw)
            def walk(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(v, (str, int, float)):
                            if str(k).lower() in {"weight","netweight","size","contentsize","packagesize","volume","netcontent"}:
                                cand = normalize_size_text(str(v))
                                if cand:
                                    return cand
                        r = walk(v)
                        if r:
                            return r
                elif isinstance(obj, list):
                    for it in obj:
                        r = walk(it)
                        if r:
                            return r
                return ""
            cand = walk(data)
            if cand:
                return cand
    except Exception:
        pass
    return normalize_size_text(parse_amount_from_title(title))

def infer_brand_from_title(title: str) -> str:
    parts = title.split()
    if not parts:
        return ""
    if len(parts) >= 2 and parts[0][:1].isupper() and parts[1][:1].isupper():
        return f"{parts[0]} {parts[1]}"
    return parts[0]

def extract_price_eur(page) -> float:
    for sel in ["[data-test='product-price']", "[data-testid='product-price']",
                "[class*='price']", "span:has-text('€')", "span:has-text('EUR')"]:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            txt = clean(loc.first.inner_text())
            m = PRICE_NUM_RE.search(txt.replace("\u00a0", " "))
            if m:
                return float(m.group(1).replace(",", "."))
        except Exception:
            continue
    try:
        txt = clean(page.inner_text("body"))
        m = PRICE_NUM_RE.search(txt)
        if m:
            return float(m.group(1).replace(",", "."))
    except Exception:
        pass
    return 0.0

def extract_ext_id_from_url(url: str) -> str:
    path_parts = [p for p in urlparse(url).path.split("/") if p]
    return path_parts[-1] if path_parts else url

# ---------------------------------------------------------------------------
def paginate_listing(page, max_pages: int = 80) -> None:
    def page_height() -> int:
        try:
            return page.evaluate("document.body.scrollHeight")
        except Exception:
            return 0

    load_more_selectors = [
        "button:has-text('Load more')", "button:has-text('Show more')",
        "button:has-text('Load More')", "[data-testid*='load'][data-testid*='more']",
        "button[aria-label*='more']", "[data-testid='load-more']",
        "button:has-text('Load more products')", "button:has-text('Show more products')",
        "button:has-text('Näita rohkem')",
    ]
    next_selectors = [
        "a[rel='next']", "a.pagination__next", "button:has-text('Next')",
        "a:has-text('Next')", "a.pagination-next", "button[aria-label='Next page']",
        "[data-testid='pagination-next']",
    ]

    pages_clicked = 0
    prev_h = -1

    while pages_clicked < max_pages:
        progressed = False
        for sel in load_more_selectors:
            try:
                btn = page.locator(sel)
                if btn.count() > 0 and btn.first.is_enabled():
                    btn.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter(0.6, 1.2)
                    new_h = page_height()
                    if new_h > prev_h:
                        prev_h = new_h
                        progressed = True
                        break
            except Exception:
                continue
        if progressed:
            continue
        try:
            cur_h = page_height()
            page.mouse.wheel(0, 20000)
            jitter(0.5, 1.0)
            new_h = page_height()
            if new_h > cur_h:
                prev_h = new_h
                progressed = True
        except Exception:
            pass
        if progressed:
            continue
        for sel in next_selectors:
            try:
                nxt = page.locator(sel)
                if nxt.count() > 0 and nxt.first.is_enabled():
                    nxt.first.click()
                    page.wait_for_load_state("domcontentloaded")
                    jitter(0.6, 1.2)
                    new_h = page_height()
                    if new_h >= prev_h:
                        prev_h = new_h
                        progressed = True
                        pages_clicked += 1
                        break
            except Exception:
                continue
        if progressed:
            continue
        break

def collect_links_from_listing(page, current_url: str) -> tuple[set[str], set[str]]:
    try:
        page.wait_for_selector("a[href*='/toode/'], a[href*='/en/toode/']", timeout=6000)
    except Exception:
        pass
    try:
        paginate_listing(page, max_pages=100)
    except Exception:
        pass
    try:
        last_h = 0
        for _ in range(6):
            page.mouse.wheel(0, 20000)
            jitter(0.4, 0.9)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
    except Exception:
        pass

    prod, cats = set(), set()
    try:
        anchors = page.locator("a[href]")
        count = anchors.count()
    except PlaywrightTimeout:
        count = 0

    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href")
            if not href:
                continue
            url = urljoin(BASE, href)
            path = urlparse(url).path
            if is_product_path(path):
                prod.add(url)
            elif is_category_path(path) and is_in_whitelist(url):
                cats.add(url)
        except Exception:
            continue
    return prod, cats

# ---------------------------------------------------------------------------
def crawl_to_db(max_products: int = 500, headless: bool = True) -> None:
    conn = db_connect()
    store_id = get_store_id_for_prisma()

    rows_written = 0
    skipped_no_ean = 0
    skipped_no_price = 0

    # ---- Load known URLs from DB first ----
    db_urls = load_urls_from_db(conn)
    product_urls: set[str] = set(db_urls)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ))
        page = context.new_page()

        def accept_cookies(pg):
            for sel in [
                "button:has-text('Accept all')", "button:has-text('Accept cookies')",
                "button:has-text('Nõustu')", "button[aria-label*='accept']",
                "button[aria-label*='Accept']",
            ]:
                try:
                    btn = pg.locator(sel)
                    if btn.count() > 0 and btn.first.is_enabled():
                        btn.first.click()
                        pg.wait_for_load_state("domcontentloaded")
                        jitter(0.2, 0.6)
                        return
                except Exception:
                    pass

        accept_cookies(page)

        # ---- Phase A: only if DB didn't give us enough URLs ----
        if len(product_urls) < max_products:
            print(f"[phase-a] DB has {len(product_urls)} URLs, need {max_products} — crawling categories")
            seen_categories: set[str] = set()
            to_visit = [urljoin(BASE, s) for s in SEEDS]

            while to_visit and len(product_urls) < max_products:
                cat_url = to_visit.pop(0)
                if cat_url in seen_categories:
                    continue
                seen_categories.add(cat_url)

                try:
                    page.goto(cat_url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    jitter()
                except PlaywrightTimeout:
                    continue

                prod, cats = collect_links_from_listing(page, cat_url)
                product_urls.update(prod)

                for c in cats:
                    if c not in seen_categories and c not in to_visit:
                        to_visit.append(c)

                print(
                    f"[DISCOVER] {cat_url} -> +{len(prod)} products, "
                    f"+{len(cats)} cats (totals: products={len(product_urls)}, queue={len(to_visit)})"
                )

                if len(product_urls) >= max_products:
                    break
        else:
            print(f"[phase-a] skipped — {len(product_urls)} URLs from DB, going straight to PDPs")

        # ---- Phase B: visit PDPs and upsert ----
        urls_to_visit = list(product_urls)[:max_products]
        print(f"[phase-b] visiting {len(urls_to_visit)} PDPs")

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for i, url in enumerate(urls_to_visit):
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_load_state("domcontentloaded")
                    jitter()
                except PlaywrightTimeout:
                    continue

                title = extract_title(page)
                ean = extract_ean(page, url)

                if not ean:
                    skipped_no_ean += 1
                    continue

                size_text = extract_size_text(page, title)
                brand = infer_brand_from_title(title)
                price_val = extract_price_eur(page)

                if not price_val or price_val <= 0:
                    skipped_no_price += 1
                    continue

                ext_id = extract_ext_id_from_url(url)

                try:
                    cur.execute(
                        """
                        SELECT upsert_product_and_price(
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        );
                        """,
                        (
                            "prisma", ext_id, title, brand, size_text, ean,
                            price_val, "EUR", store_id,
                            datetime.now(timezone.utc), url,
                        ),
                    )
                    rows_written += 1
                    if rows_written % 100 == 0:
                        print(f"[phase-b] {rows_written} upserted ({i+1}/{len(urls_to_visit)} visited)")
                except Exception as e:
                    print(f"[prisma] upsert failed for {ext_id} / EAN {ean}: {e}")
                    continue

        browser.close()

    print(
        f"[DONE] visited {len(urls_to_visit)} URLs. "
        f"Upserted {rows_written} rows. "
        f"Skipped no-EAN: {skipped_no_ean}, no-price: {skipped_no_price}."
    )

# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=500)
    ap.add_argument("--headless", type=int, default=1)
    args = ap.parse_args()
    crawl_to_db(max_products=args.max_products, headless=bool(args.headless))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

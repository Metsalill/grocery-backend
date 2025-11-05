#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) – Category → PDP crawler → CSV + direct DB ingest

Highlights
- Writes CSV *and* ingests directly into Railway via
  SELECT upsert_product_and_price(...).
- Default STORE_ID=441 (Barbora ePood). Can be overridden via env STORE_ID.
- Time-budget friendly: SOFT_TIMEOUT_MIN env (e.g. "118") to stop early,
  flush CSV, and still ingest before GH Actions’ cap.
- Horizontal sharding:
    * Env: SHARD=<0..N-1>, SHARDS=<N>
    * CLI: --cat-index <i> and --cat-shards <N>  (CLI overrides env)
- Tough pagination, price parsing, PDP extraction.
- Backward-compatible CLI flags for your workflows:
    --out-csv (alias of --output-csv), --cat-shards, --cat-index, --upsert-db

YAML deps (example):
  pip install playwright asyncpg bs4 lxml selectolax
  playwright install --with-deps chromium
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import (
    urljoin,
    urlparse,
    urlsplit,
    urlunsplit,
    parse_qsl,
    urlencode,
)

import asyncpg
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import Page, sync_playwright

# ---------------------------------------------------------------------
# Chain / store identity
# ---------------------------------------------------------------------

BASE = "https://barbora.ee"
STORE_CHAIN = "Maxima"
STORE_NAME = "Barbora ePood"
STORE_CHANNEL = "online"

# short, lowercase label for ext_product_map.source
DB_SOURCE_LABEL = "barbora"

# Defaults
DEFAULT_REQ_DELAY = 0.25
DEFAULT_HEADLESS = 1

# Common size tokens
SIZE_RE = re.compile(r"(?ix)(\d+\s?(?:x\s?\d+)?\s?(?:ml|l|cl|g|kg|mg|tk|pcs))|(\d+\s?x\s?\d+)")

# Labels for brand/manufacturer in Barbora PDPs
SPEC_KEYS_BRAND = {"kaubamärk", "bränd", "brand"}
SPEC_KEYS_MFR = {"tootja", "valmistaja", "manufacturer", "tarnija"}
SPEC_KEYS_SIZE = {"kogus", "netokogus", "maht", "neto"}
BAD_NAMES = {"pealeht"}  # "Home" in Estonian etc.


# ---------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------

def norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip().lower()


def text_of(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""


def get_ext_id(url: str) -> str:
    """
    Stable per-product external ID.
    Prefer numeric ID (/p/12345 or slug-12345), fallback to sanitized tail.
    """
    m = re.search(r"/p/(\d+)", url) or re.search(r"-(\d+)$", url)
    if m:
        return m.group(1)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", url).strip("-")
    return slug[-120:]


# ---------------------------------------------------------------------
# cookie banner / page priming
# ---------------------------------------------------------------------

def accept_cookies(page: Page) -> None:
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
                page.wait_for_timeout(400)
                return
        except Exception:
            pass
    try:
        page.get_by_role("button", name=re.compile("Nõus|Accept|OK", re.I)).click(timeout=800)
        page.wait_for_timeout(200)
    except Exception:
        pass


def ensure_ready(page: Page) -> None:
    accept_cookies(page)
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(150)
        page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass


# ---------------------------------------------------------------------
# price parsing helpers
# ---------------------------------------------------------------------

def _first_str(*vals) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _clean_decimal(s: str) -> Optional[str]:
    """
    Normalize to a decimal string "X.YY".
    Handles "3,49", "3.49", "3 49", "3€49", and strips % etc.
    """
    if not s:
        return None
    raw = s.replace("\xa0", " ").strip()

    if re.fullmatch(r"\d+\s*%+", raw):
        return None

    m = re.search(r"(\d[\d\s]*)\s*€\s*(\d{1,2})", raw)
    if m:
        whole = re.sub(r"\D", "", m.group(1))
        cents = m.group(2)
        if whole:
            return f"{int(whole)}.{cents[:2]:0<2}"

    m = re.search(r"(\d+)[,\.](\d{1,2})", raw)
    if m:
        return f"{m.group(1)}.{m.group(2):0<2}"

    m = re.search(r"(\d+)\s+(\d{2})", raw)
    if m:
        return f"{m.group(1)}.{m.group(2)}"

    digits = re.sub(r"[^\d]", "", raw)
    if digits and len(digits) > 2:
        return f"{digits[:-2]}.{digits[-2:]}"
    return None


def parse_price_from_dom(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    # 1) itemprop meta
    meta = soup.select_one("[itemprop=price][content]")
    if meta and meta.get("content"):
        val = _clean_decimal(meta.get("content"))
        if val:
            cur = (soup.select_one("[itemprop=priceCurrency][content]") or {}).get("content") or "EUR"
            return val, cur

    # 2) whole+cents blocks
    for box in soup.select(
        "[data-testid*=price], .e-price, .e-price__main, .product-price, "
        ".price, .pdp-price"
    ):
        whole = box.select_one(".e-price__whole, .price__whole, .whole, .int")
        cents = box.select_one(".e-price__cents, .price__cents, .cents, .fract, .fraction, .decimal")
        if whole:
            w = re.sub(r"\D", "", text_of(whole))
            c = re.sub(r"\D", "", text_of(cents)) if cents else ""
            if w:
                return (f"{int(w)}.{c[:2]:0<2}" if c else str(int(w))), "EUR"

    # 3) data-* attributes
    data_attrs = [
        "[data-testid=buy-button-price]",
        "[data-price]",
        "[data-product-price]",
        "[data-price-value]",
    ]
    for sel in data_attrs:
        for el in soup.select(sel):
            for attr in ("data-price", "data-product-price", "data-price-value"):
                v = el.get(attr)
                val = _clean_decimal(v or "")
                if val:
                    return val, "EUR"

    # 4) scan text for "€"
    for node in soup.find_all(string=re.compile("€")):
        val = _clean_decimal(str(node))
        if val:
            return val, "EUR"

    return None, "EUR"


# ---------------------------------------------------------------------
# PDP parsing
# ---------------------------------------------------------------------

def from_json_ld(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {
        "name": None,
        "brand": None,
        "manufacturer": None,
        "image": None,
        "price": None,
        "currency": None,
    }
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            types = it.get("@type")
            types = types if isinstance(types, list) else [types]
            if not types or "Product" not in types:
                continue

            out["name"] = _first_str(it.get("name"), out["name"])

            brand_val = it.get("brand")
            if isinstance(brand_val, dict):
                brand_val = brand_val.get("name")
            out["brand"] = _first_str(brand_val, out["brand"])

            manuf_val = it.get("manufacturer")
            if isinstance(manuf_val, dict):
                manuf_val = manuf_val.get("name")
            out["manufacturer"] = _first_str(manuf_val, out["manufacturer"])

            img = it.get("image")
            if isinstance(img, list):
                img = img[0]
            out["image"] = _first_str(img, out["image"])

            offers = it.get("offers")
            offer_list = []
            if isinstance(offers, dict):
                offer_list = [offers]
            elif isinstance(offers, list):
                offer_list = [o for o in offers if isinstance(o, dict)]

            for off in offer_list:
                price = off.get("price")
                if not price and isinstance(off.get("priceSpecification"), dict):
                    price = off["priceSpecification"].get("price")
                currency = _first_str(
                    off.get("priceCurrency"),
                    (off.get("priceSpecification") or {}).get("priceCurrency"),
                )
                price = _clean_decimal(str(price) if price is not None else "")
                if price:
                    out["price"] = price
                if currency and not out["currency"]:
                    out["currency"] = currency
    return out


def _scan_label_value_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    capture: Dict[str, str] = {}

    containers = []
    for head in soup.find_all(["h2", "h3", "h4"]):
        ht = norm(text_of(head))
        if any(k in ht for k in ("muu info", "tooteinfo", "lisainfo", "info")):
            sib = head.find_next_sibling()
            if sib:
                containers.append(sib)

    containers.extend(soup.select("li, p, div"))

    label_re = re.compile(r"^\s*([^:]+):\s*(.+)\s*$")
    for el in containers:
        t = text_of(el)
        m = label_re.match(t)
        if not m:
            continue
        label = norm(m.group(1)).rstrip(":")
        value = m.group(2).strip()
        if not value:
            continue
        if any(k == label for k in SPEC_KEYS_BRAND) and "brand" not in capture:
            capture["brand"] = value
        elif any(k == label for k in SPEC_KEYS_MFR) and "manufacturer" not in capture:
            capture["manufacturer"] = value
    return capture


def parse_spec_table(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {"brand": None, "manufacturer": None, "size": None, "sku": None}

    for head in soup.select("dt, th"):
        k = norm(text_of(head)).rstrip(":")
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
        elif "ean" in k and not out["sku"]:
            out["sku"] = v

    labels = soup.select(".e-attribute__label, .product-attribute__label")
    for lab in labels:
        k = norm(text_of(lab)).rstrip(":")
        val_el = (
            lab.find_next_sibling(class_="e-attribute__value")
            or lab.find_next_sibling(class_="product-attribute__value")
        )
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
        elif "ean" in k and not out["sku"]:
            out["sku"] = v

    pairs = _scan_label_value_pairs(soup)
    out["brand"] = out["brand"] or pairs.get("brand")
    out["manufacturer"] = out["manufacturer"] or pairs.get("manufacturer")

    if out["brand"] and norm(out["brand"]) in {"-", "puudub"}:
        out["brand"] = None

    return out


def parse_app_state_for_brand_or_price(
    soup: BeautifulSoup,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    brand = manufacturer = price = currency = None
    for s in soup.find_all("script"):
        txt = (s.string or "").strip()
        if not txt:
            continue

        if brand is None:
            mb = re.search(r'"brand"\s*:\s*"([^"]+)"', txt)
            if mb:
                brand = mb.group(1).strip()

        if manufacturer is None:
            mm = re.search(r'"manufacturer"\s*:\s*"([^"]+)"', txt)
            if mm:
                manufacturer = mm.group(1).strip()

        if price is None:
            mp = re.search(r'"price"\s*:\s*"?(?!\s*0\s*%)(\d+[.,]?\d*)"?', txt)
            if mp:
                price = _clean_decimal(mp.group(1))
        if currency is None:
            mc = re.search(r'"priceCurrency"\s*:\s*"([A-Z]{3})"', txt)
            if mc:
                currency = mc.group(1)
    return brand, manufacturer, price, currency


def extract_product_title_from_dom(soup: BeautifulSoup) -> str:
    sel = (
        ".e-product__name, [data-testid=product-title], [data-testid=product-name], "
        ".product__title, .product-title, .pdp__title, .product-view__title, "
        "h1[itemprop=name]"
    )
    el = soup.select_one(sel)
    return text_of(el)


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


def extract_from_pdp(
    page: Page,
    url: str,
    listing_title: Optional[str],
    category_leaf_hint: str,
    req_delay: float,
) -> Dict[str, Optional[str]]:
    page.goto(url, timeout=60000, wait_until="domcontentloaded")
    ensure_ready(page)

    try:
        page.wait_for_load_state("networkidle", timeout=4000)
    except PWTimeout:
        pass

    try:
        page.wait_for_selector("script[type='application/ld+json']", timeout=6000)
    except PWTimeout:
        pass
    try:
        page.wait_for_selector(
            ".e-product__name, [data-testid=product-title], [data-testid=product-name]",
            timeout=5000,
        )
    except PWTimeout:
        pass

    try:
        page.wait_for_selector(
            "css=[data-testid*='price'], .e-price, .e-price__main, "
            ".e-price--current, .product-price, .price, .pdp-price, "
            ".price__current",
            timeout=15000,
        )
    except PWTimeout:
        pass
    try:
        page.wait_for_selector("text=Pole saadaval", timeout=1500)
    except PWTimeout:
        pass

    page.wait_for_timeout(int(req_delay * 1000))

    # open accordions / spec
    try:
        for sel in [
            "button[aria-expanded='false']",
            ".accordion__toggle",
            ".expand",
            "button:has-text('Rohkem')",
        ]:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=800)
                page.wait_for_timeout(150)
    except Exception:
        pass

    soup = BeautifulSoup(page.content(), "html.parser")

    jl = from_json_ld(soup)
    spec = parse_spec_table(soup)
    b3, m3, p3, c3 = parse_app_state_for_brand_or_price(soup)

    h1_any = text_of(soup.select_one("h1"))
    dom_title = extract_product_title_from_dom(soup)
    candidates = [jl.get("name") or "", dom_title, listing_title or "", h1_any]

    cat_path, cat_leaf_bc = extract_breadcrumbs(soup)
    category_leaf = cat_leaf_bc or category_leaf_hint

    name = prefer_valid_name(candidates, category_leaf)

    price = jl.get("price")
    currency = jl.get("currency") or "EUR"
    if not price:
        price, cur2 = parse_price_from_dom(soup)
        currency = currency or cur2 or "EUR"
    if not price and p3:
        price = p3
    if not currency and c3:
        currency = c3

    size_text = spec["size"] or extract_size_from_name(name)
    image_url = jl.get("image")
    brand = jl.get("brand") or spec["brand"] or b3
    manufacturer = jl.get("manufacturer") or spec["manufacturer"] or m3
    sku_raw = spec["sku"]

    # fallback breadcrumb guess
    if not cat_path:
        parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
        cat_path = " / ".join(p.replace("-", " ").title() for p in parts[:-1]) if parts else ""
        if not category_leaf:
            category_leaf = (
                parts[-2] if len(parts) >= 2 else (parts[-1] if parts else "")
            ).replace("-", " ").title()

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


# ---------------------------------------------------------------------
# category listing / pagination
# ---------------------------------------------------------------------

def harvest_product_links(page: Page) -> List[Tuple[str, str]]:
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
    # dedupe
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((u, t))
    return uniq


def ensure_category_loaded(page: Page, req_delay: float) -> None:
    try:
        page.wait_for_selector("a, [role='link']", timeout=8000)
    except PWTimeout:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=3000)
    except PWTimeout:
        pass
    page.wait_for_timeout(int(req_delay * 1000))


def go_to_category(page: Page, url: str, req_delay: float) -> None:
    page.goto(url, timeout=60000, wait_until="domcontentloaded")
    ensure_ready(page)
    ensure_category_loaded(page, req_delay)


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

    cur = _current_page_from_url(page.url)

    # estimate max page from numeric buttons
    try:
        nums = page.eval_on_selector_all(
            "a, button",
            "els => els.map(e => (e.textContent||'').trim())"
            ".filter(t => /^\\d+$/.test(t)).map(t => parseInt(t,10))",
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


def collect_category_products(
    page: Page,
    cat_url: str,
    req_delay: float,
    max_pages: int = 60,
) -> List[Tuple[str, str]]:
    go_to_category(page, cat_url, req_delay)

    all_links: List[Tuple[str, str]] = []
    seen_pages = set()
    pages_done = 0

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

    # final unique pass
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in all_links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((u, t))

    print(f"[cat] {cat_url} → {len(uniq)} products across {pages_done} page(s)")
    return uniq


# ---------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------

CSV_HEADER = [
    "store_chain",
    "store_name",
    "store_channel",
    "ext_id",
    "ean_raw",
    "sku_raw",
    "name",
    "size_text",
    "brand",
    "manufacturer",
    "price",
    "currency",
    "image_url",
    "category_path",
    "category_leaf",
    "source_url",
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


# ---------------------------------------------------------------------
# DB ingest helper
# ---------------------------------------------------------------------

async def _bulk_ingest_to_db(rows: List[Dict[str, object]], store_id: int) -> None:
    """
    Call upsert_product_and_price(...) for each row.
    """
    if store_id <= 0:
        print("[barbora] STORE_ID not set or invalid, skipping DB ingest.")
        return

    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        print("[barbora] DATABASE_URL missing, skipping DB ingest.")
        return

    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            for r in rows:
                price_val = None
                try:
                    ptxt = r.get("price")
                    if ptxt not in (None, ""):
                        price_val = float(ptxt)
                except Exception:
                    price_val = None

                await conn.fetchval(
                    """
                    SELECT upsert_product_and_price(
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
                    );
                    """,
                    DB_SOURCE_LABEL,                 # $1 in_source
                    r.get("ext_id") or "",           # $2 in_ext_id
                    r.get("name") or "",             # $3 in_name
                    r.get("brand") or "",            # $4 in_brand
                    r.get("size_text") or "",        # $5 in_size_text
                    r.get("ean_raw") or "",          # $6 in_ean_raw
                    price_val,                       # $7 in_price
                    r.get("currency") or "EUR",      # $8 in_currency
                    store_id,                        # $9 in_store_id
                    r.get("seen_at"),                # $10 in_seen_at (aware datetime)
                    r.get("source_url") or "",       # $11 in_source_url
                )
    finally:
        await conn.close()


# ---------------------------------------------------------------------
# sharding utils
# ---------------------------------------------------------------------

def apply_shard(full_list: List[str], shard: Optional[int], shards: Optional[int]) -> List[str]:
    """Return the subset of categories for this shard, using CLI overrides first, then env."""
    try:
        env_shard = int(os.environ.get("SHARD", "0"))
        env_shards = int(os.environ.get("SHARDS", "1"))
    except Exception:
        env_shard, env_shards = 0, 1

    s = env_shard if shard is None else shard
    n = env_shards if shards is None else shards

    if n <= 1:
        return full_list

    s = max(0, min(s, n - 1))
    out = [c for i, c in enumerate(full_list) if i % n == s]
    print(f"[shard] shard {s+1}/{n}: {len(out)}/{len(full_list)} categories")
    return out


# ---------------------------------------------------------------------
# main crawl runner
# ---------------------------------------------------------------------

def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def crawl(args) -> None:
    cats_all = read_lines(args.cats_file)
    cats = apply_shard(cats_all, args.cat_index, args.cat_shards)

    skip_ext: set[str] = (
        set(read_lines(args.skip_ext_file))
        if args.skip_ext_file and os.path.exists(args.skip_ext_file)
        else set()
    )
    only_ext: set[str] = (
        set(read_lines(args.only_ext_file))
        if args.only_ext_file and os.path.exists(args.only_ext_file)
        else set()
    )
    only_urls: List[str] = (
        read_lines(args.only_url_file)
        if args.only_url_file and os.path.exists(args.only_url_file)
        else []
    )

    total = 0
    headless = bool(int(args.headless))
    req_delay = float(args.req_delay)
    per_cat_page_limit = int(args.max_pages_per_category or "0")

    # budget kill switch
    stop_flag = {"v": False}

    def _sig_handler(signum, frame):
        stop_flag["v"] = True

    for _sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(_sig, _sig_handler)
        except Exception:
            pass

    soft_minutes = float(os.environ.get("SOFT_TIMEOUT_MIN", "0") or "0")
    deadline_ts = time.time() + soft_minutes * 60 if soft_minutes > 0 else None

    def time_left() -> float:
        return (deadline_ts - time.time()) if deadline_ts else 9e9

    def budget_low() -> bool:
        return stop_flag["v"] or (deadline_ts is not None and time_left() <= 90)

    ensure_csv_header(args.output_csv)

    rows_for_ingest: List[Dict[str, object]] = []

    with sync_playwright() as pw:
        def new_browser():
            b = pw.chromium.launch(headless=headless)
            ctx = b.new_context(locale="et-EE")
            return b, ctx, ctx.new_page()

        browser, ctx, page = new_browser()

        def restart_browser(reason: str = ""):
            nonlocal browser, ctx, page
            try:
                page.close()
                ctx.close()
                browser.close()
            except Exception:
                pass
            time.sleep(0.5)
            browser, ctx, page = new_browser()
            if reason:
                print(f"[info] restarted browser ({reason})")

        try:
            # MODE A: ONLY-URLs
            if only_urls:
                batch_csv: List[List[str]] = []
                processed_since_restart = 0
                RESTART_EVERY = 250

                for url in only_urls:
                    if budget_low():
                        print("[info] budget reached (ONLY URLs); flushing & exit.")
                        break
                    if int(args.max_products) and total >= int(args.max_products):
                        break

                    ext_id = get_ext_id(url)
                    if skip_ext and ext_id in skip_ext:
                        continue

                    parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
                    cat_leaf_guess = (
                        parts[-2] if len(parts) >= 2 else (parts[-1] if parts else "")
                    ).replace("-", " ").title()

                    data: Optional[Dict[str, Optional[str]]] = None
                    for attempt in (1, 2):
                        try:
                            data = extract_from_pdp(
                                page,
                                url,
                                listing_title=None,
                                category_leaf_hint=cat_leaf_guess,
                                req_delay=req_delay,
                            )
                            break
                        except Exception as e:
                            print(f"[warn] PDP parse failed for {ext_id} (attempt {attempt}): {e}", file=sys.stderr)
                            restart_browser("only-urls retry")
                    if not data:
                        continue

                    if norm(data["name"]) in BAD_NAMES or norm(
                        data["name"]
                    ) == norm(data.get("category_leaf") or cat_leaf_guess):
                        continue

                    seen_at_ts = datetime.now(timezone.utc)

                    row_csv = [
                        STORE_CHAIN,
                        STORE_NAME,
                        STORE_CHANNEL,
                        ext_id,
                        "",  # ean_raw unknown/unreliable
                        data.get("sku_raw") or "",
                        data.get("name") or "",
                        data.get("size_text") or "",
                        data.get("brand") or "",
                        data.get("manufacturer") or "",
                        data.get("price") or "",
                        data.get("currency") or "EUR",
                        data.get("image_url") or "",
                        data.get("category_path") or "",
                        data.get("category_leaf") or cat_leaf_guess,
                        url,
                    ]
                    batch_csv.append(row_csv)

                    rows_for_ingest.append(
                        {
                            "ext_id": ext_id,
                            "name": data.get("name") or "",
                            "brand": data.get("brand") or "",
                            "size_text": data.get("size_text") or "",
                            "ean_raw": "",
                            "price": data.get("price") or "",
                            "currency": data.get("currency") or "EUR",
                            "seen_at": seen_at_ts,
                            "source_url": url,
                        }
                    )

                    total += 1
                    processed_since_restart += 1

                    if len(batch_csv) >= 50:
                        append_rows(args.output_csv, batch_csv)
                        batch_csv.clear()

                    if processed_since_restart >= RESTART_EVERY:
                        append_rows(args.output_csv, batch_csv)
                        batch_csv.clear()
                        processed_since_restart = 0
                        restart_browser("periodic")

                    if req_delay:
                        time.sleep(req_delay)

                append_rows(args.output_csv, batch_csv)

            # MODE B: crawl categories
            else:
                for idx, cat in enumerate(cats, start=1):
                    if budget_low():
                        print("[info] budget reached before next category; exit early.")
                        break
                    if int(args.page_limit) and idx > int(args.page_limit):
                        break

                    leaf_seg = cat.strip("/").split("/")[-1]
                    category_leaf_hint = leaf_seg.replace("-", " ").title()
                    category_path_hint = ""

                    prods = collect_category_products(
                        page,
                        cat,
                        req_delay,
                        max_pages=per_cat_page_limit if per_cat_page_limit > 0 else 120,
                    )
                    if not prods:
                        print(f"[cat] {cat} → 0 items (maybe geo/login block).")
                        restart_browser("post-category")
                        continue

                    batch_csv: List[List[str]] = []

                    for url, listing_title in prods:
                        if budget_low():
                            print("[info] budget reached mid-category; flushing & exit.")
                            break
                        if int(args.max_products) and total >= int(args.max_products):
                            break

                        ext_id = get_ext_id(url)
                        if skip_ext and ext_id in skip_ext:
                            continue
                        if only_ext and ext_id not in only_ext:
                            continue

                        data: Optional[Dict[str, Optional[str]]] = None
                        for attempt in (1, 2):
                            try:
                                data = extract_from_pdp(
                                    page,
                                    url,
                                    listing_title,
                                    category_leaf_hint,
                                    req_delay,
                                )
                                break
                            except Exception as e:
                                print(f"[warn] PDP parse failed for {ext_id} (attempt {attempt}): {e}", file=sys.stderr)
                                restart_browser("pdp retry")
                        if not data:
                            continue

                        if norm(data["name"]) in BAD_NAMES or norm(
                            data["name"]
                        ) == norm(data.get("category_leaf") or category_leaf_hint):
                            continue

                        seen_at_ts = datetime.now(timezone.utc)

                        row_csv = [
                            STORE_CHAIN,
                            STORE_NAME,
                            STORE_CHANNEL,
                            ext_id,
                            "",  # ean_raw blank
                            data.get("sku_raw") or "",
                            data.get("name") or "",
                            data.get("size_text") or "",
                            data.get("brand") or "",
                            data.get("manufacturer") or "",
                            data.get("price") or "",
                            data.get("currency") or "EUR",
                            data.get("image_url") or "",
                            data.get("category_path") or category_path_hint,
                            data.get("category_leaf") or category_leaf_hint,
                            url,
                        ]
                        batch_csv.append(row_csv)

                        rows_for_ingest.append(
                            {
                                "ext_id": ext_id,
                                "name": data.get("name") or "",
                                "brand": data.get("brand") or "",
                                "size_text": data.get("size_text") or "",
                                "ean_raw": "",
                                "price": data.get("price") or "",
                                "currency": data.get("currency") or "EUR",
                                "seen_at": seen_at_ts,
                                "source_url": url,
                            }
                        )

                        total += 1

                        if len(batch_csv) >= 50:
                            append_rows(args.output_csv, batch_csv)
                            batch_csv.clear()

                        if req_delay:
                            time.sleep(req_delay)

                    append_rows(args.output_csv, batch_csv)
                    restart_browser("post-category")

        finally:
            try:
                page.close()
                ctx.close()
                browser.close()
            except Exception:
                pass

    # -----------------------------------------------------------------
    # after crawling: DB ingest
    # -----------------------------------------------------------------
    try:
        store_id_env = int(os.environ.get("STORE_ID", "441") or "441")
    except Exception:
        store_id_env = 441

    asyncio.run(_bulk_ingest_to_db(rows_for_ingest, store_id_env))

    try:
        lines = sum(1 for _ in open(args.output_csv, "r", encoding="utf-8"))
        print(f"[done] barbora: wrote ~{max(0, lines-1)} CSV rows, ingested {len(rows_for_ingest)} rows to DB")
    except Exception:
        print(f"[done] barbora: wrote CSV + ingested {len(rows_for_ingest)} rows to DB")


# ---------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------

def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Barbora.ee category→PDP crawler (CSV + direct DB ingest)."
    )
    p.add_argument(
        "--cats-file",
        required=True,
        help="Text file with category URLs (one per line)",
    )
    p.add_argument(
        "--page-limit",
        default="0",
        help="Max categories to process (0=all)",
    )
    p.add_argument(
        "--max-products",
        default="0",
        help="Cap total PDPs visited (0=unlimited)",
    )
    p.add_argument(
        "--max-pages-per-category",
        default="0",
        help="Cap pages per category (0=unlimited)",
    )
    p.add_argument("--headless", default=str(DEFAULT_HEADLESS), help="1/0")
    p.add_argument(
        "--req-delay",
        default=str(DEFAULT_REQ_DELAY),
        help="Delay between steps in seconds",
    )
    # Accept both --output-csv and legacy --out-csv
    p.add_argument(
        "--output-csv", "--out-csv",
        dest="output_csv",
        default="data/barbora_products.csv",
        help="Output CSV path (will append)",
    )
    p.add_argument("--skip-ext-file", default="", help="File with ext_ids to SKIP (one per line)")
    p.add_argument("--only-ext-file", default="", help="File with ext_ids to INCLUDE exclusively")
    p.add_argument("--only-url-file", default="", help="File with PDP URLs to visit exclusively")

    # Compatibility flags used by older YAMLs (ignored except for sharding)
    p.add_argument("--cat-shards", type=int, default=None, help="Total shards (CLI overrides env SHARDS)")
    p.add_argument("--cat-index", type=int, default=None, help="This shard index [0..N-1] (overrides env SHARD)")
    p.add_argument("--upsert-db", default="", help="Compat: ignored; ingest uses DATABASE_URL")
    return p


if __name__ == "__main__":
    parser = build_argparser()
    crawl(parser.parse_args())

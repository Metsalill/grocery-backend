#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Barbora.ee (Maxima EE) – Category → PDP crawler → CSV (no EAN parsing)

CSV columns (exact order; ean_raw left blank intentionally to keep loaders stable):
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

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
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

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip().lower()


def text_of(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""


def get_ext_id(url: str) -> str:
    # Prefer trailing numeric id (…/p/123456 or …-123456)
    m = re.search(r"/p/(\d+)", url) or re.search(r"-(\d+)$", url)
    if m:
        return m.group(1)
    # Fallback to slug tail
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", url).strip("-")
    return slug[-80:]


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
            t = it.get("@type")
            types = t if isinstance(t, list) else [t]
            if not types or "Product" not in types:
                continue
            out["name"] = out["name"] or it.get("name")

            brand = it.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")
            if brand and not out["brand"]:
                out["brand"] = brand

            manufacturer = it.get("manufacturer")
            if isinstance(manufacturer, dict):
                manufacturer = manufacturer.get("name")
            if manufacturer and not out["manufacturer"]:
                out["manufacturer"] = manufacturer

            img = it.get("image")
            if isinstance(img, list):
                img = img[0]
            if img and not out["image"]:
                out["image"] = img

            offers = it.get("offers") or {}
            if isinstance(offers, dict):
                if out["price"] is None and offers.get("price") is not None:
                    out["price"] = str(offers.get("price"))
                if out["currency"] is None and offers.get("priceCurrency"):
                    out["currency"] = offers.get("priceCurrency")
    return out


def parse_spec_table(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = {"brand": None, "manufacturer": None, "size": None, "sku": None}
    # Inspect definition lists/tables: dt/th headers; dd/td values
    for head in soup.select("dt, th"):
        k = norm(text_of(head))
        val_el = head.find_next_sibling(["dd", "td"])
        v = norm(text_of(val_el)) if val_el else ""
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
    # Scan inline scripts for "brand" / "manufacturer"
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


def safe_product_name(soup: BeautifulSoup, category_leaf: str, listing_title: Optional[str]) -> str:
    # Primary: PDP H1 / itemprop=name
    h1 = soup.select_one("h1,[itemprop=name]")
    title = text_of(h1)
    if not title:
        og = soup.find("meta", {"property": "og:title"})
        title = (og.get("content") or "").strip() if og else ""
    if not title and listing_title:
        title = listing_title
    # Guard: avoid names equal to category leaf
    if norm(title) == norm(category_leaf):
        bc = [text_of(x) for x in soup.select("nav.breadcrumb a, .breadcrumbs a, .breadcrumb a")]
        if bc:
            alt = bc[-1]
            if norm(alt) != norm(category_leaf):
                title = alt
    return title


def extract_size_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    m = SIZE_RE.search(name)
    return m.group(0) if m else None


def parse_price_from_dom(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    cur = "EUR"
    price_el = soup.select_one("[data-testid=product-price], .price, .product-price, .e-price__main")
    if not price_el:
        return None, cur
    val = re.sub(r"[^\d,\.]", "", text_of(price_el)).replace(",", ".")
    return (val if val else None), cur


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# --------------------------------------------------------------------------- #
# Category listing
# --------------------------------------------------------------------------- #
def list_products_from_category(page: Page, cat_url: str, req_delay: float) -> List[Tuple[str, str]]:
    """Return list of (pdp_url, listing_title)."""
    page.goto(cat_url, timeout=60000, wait_until="domcontentloaded")
    time.sleep(req_delay)

    # Try to load full grid via scrolling
    stagnant_rounds = 0
    last_len = 0
    for _ in range(80):
        page.mouse.wheel(0, 24000)
        time.sleep(0.35)
        html0 = page.content()
        cur_len = len(html0)
        if cur_len == last_len:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        last_len = cur_len
        if stagnant_rounds >= 3:
            break

    soup = BeautifulSoup(page.content(), "html.parser")
    out: List[Tuple[str, str]] = []
    for a in soup.select("a[href*='/toode/'], a[href*='/p/']"):
        href = a.get("href")
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
    return uniq

# --------------------------------------------------------------------------- #
# PDP extraction
# --------------------------------------------------------------------------- #
def extract_from_pdp(page: Page, url: str, listing_title: Optional[str], category_leaf: str, req_delay: float) -> Dict[str, Optional[str]]:
    page.goto(url, timeout=60000, wait_until="domcontentloaded")
    time.sleep(req_delay)
    soup = BeautifulSoup(page.content(), "html.parser")

    jl = from_json_ld(soup)
    spec = parse_spec_table(soup)
    b2, m2 = parse_app_state_for_brand(soup)

    name = safe_product_name(soup, category_leaf, listing_title) or jl["name"] or listing_title or ""
    price = jl["price"]
    currency = jl["currency"] or "EUR"
    if not price:
        price, currency = parse_price_from_dom(soup)

    size_text = spec["size"] or extract_size_from_name(name)
    image_url = jl["image"]
    brand = jl["brand"] or spec["brand"] or b2
    manufacturer = jl["manufacturer"] or spec["manufacturer"] or m2
    sku_raw = spec["sku"]

    return {
        "name": name,
        "size_text": size_text,
        "brand": brand,
        "manufacturer": manufacturer,
        "price": price,
        "currency": currency or "EUR",
        "image_url": image_url,
        "sku_raw": sku_raw,
    }

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


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

    out_rows: List[List[str]] = []
    total = 0

    headless = bool(int(args.headless))
    req_delay = float(args.req_delay)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(locale="et-EE")
        page = ctx.new_page()

        for idx, cat in enumerate(cats, start=1):
            if int(args.page_limit) and idx > int(args.page_limit):
                break

            leaf_seg = cat.strip("/").split("/")[-1]
            category_leaf = leaf_seg.replace("-", " ").title()
            category_path = ""  # optional

            prods = list_products_from_category(page, cat, req_delay)
            if not prods:
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
                except PWTimeout:
                    continue
                except Exception as e:
                    print(f"[warn] PDP parse failed for {ext_id}: {e}", file=sys.stderr)
                    continue

                if norm(data["name"]) == norm(category_leaf):
                    # avoid category-as-name leakage
                    continue

                row = [
                    STORE_CHAIN,
                    STORE_NAME,
                    STORE_CHANNEL,
                    ext_id,
                    "",  # ean_raw intentionally blank (Barbora does not expose)
                    data.get("sku_raw") or "",
                    data.get("name") or "",
                    data.get("size_text") or "",
                    data.get("brand") or "",
                    data.get("manufacturer") or "",
                    data.get("price") or "",
                    data.get("currency") or "EUR",
                    data.get("image_url") or "",
                    category_path,
                    category_leaf,
                    url,
                ]
                out_rows.append(row)
                total += 1
                if req_delay:
                    time.sleep(req_delay)

        write_csv(out_rows, args.output_csv)


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
    return p


if __name__ == "__main__":
    parser = build_argparser()
    crawl(parser.parse_args())

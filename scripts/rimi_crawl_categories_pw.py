#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) category crawler → PDP extractor → CSV
- Resilient CLI for GitHub Actions (no argparse exit 2).
- Category → subcategory discovery + product links.
- PDP parser uses DOM, JSON-LD, microdata, and window globals.
"""

from __future__ import annotations
import argparse, os, re, csv, json, sys, traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

STORE_CHAIN   = "Rimi"
STORE_NAME    = "Rimi ePood"
STORE_CHANNEL = "online"
BASE = "https://www.rimi.ee"

EAN_RE = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE = re.compile(r"(\d+[.,]\d+)\s*€")
SKU_KEYS = {"sku","mpn","itemNumber","productCode","code","id","itemid"}
EAN_KEYS = {"ean","ean13","gtin","gtin13","barcode"}

# ------------------------------- utils --------------------------------------

def deep_find_kv(obj: Any, keys: set) -> Dict[str,str]:
    out: Dict[str,str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k,v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str,int,float)):
                    out[lk] = str(v)
                walk(v)
        elif isinstance(x, list):
            for i in x: walk(i)
    walk(obj)
    return out

def normalize_href(href: Optional[str]) -> Optional[str]:
    if not href: return None
    href = href.split("?")[0].split("#")[0]
    return href if href.startswith("http") else urljoin(BASE, href)

def auto_accept_overlays(page) -> None:
    labels = [
        r"Nõustun", r"Nõustu", r"Accept", r"Allow all", r"OK", r"Selge",
        r"Jätka", r"Vali hiljem", r"Continue", r"Close", r"Sulge",
        r"Vali pood", r"Vali teenus", r"Telli koju", r"Vali kauplus",
        r"Näita kõiki tooteid", r"Kuva tooted", r"Kuva kõik tooted",
    ]
    for lab in labels:
        try:
            page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=800)
            page.wait_for_timeout(120)
        except Exception:
            pass

def wait_for_hydration(page, timeout_ms: int = 8000) -> None:
    try:
        page.wait_for_function(
            """() => {
                const main = document.querySelector('main');
                const hidden = main && getComputedStyle(main).visibility === 'hidden';
                const cards = document.querySelector('.js-product-container a.card__url, a[href*="/p/"]');
                return (main && !hidden) || !!cards;
            }""",
            timeout=timeout_ms
        )
    except Exception:
        pass

# ----------------------------- parsing --------------------------------------

def parse_price_from_dom_or_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for tag in soup.select('meta[itemprop="price"], [itemprop="price"]'):
        val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
        if val:
            return val.replace(",", "."), "EUR"
    m = MONEY_RE.search(soup.get_text(" ", strip=True))
    if m:
        return m.group(1).replace(",", "."), "EUR"
    return None, None

def parse_brand_and_size(soup: BeautifulSoup, name: str) -> Tuple[Optional[str], Optional[str]]:
    brand = size_text = None
    for row in soup.select("table tr"):
        th, td = row.find("th"), row.find("td")
        if not th or not td: 
            continue
        key = th.get_text(" ", strip=True).lower()
        val = td.get_text(" ", strip=True)
        if (not brand) and ("tootja" in key or "brand" in key):
            brand = val
        if (not size_text) and any(k in key for k in ("kogus","maht","netokogus","pakend","neto","suurus")):
            size_text = val
    if not size_text:
        m = re.search(r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk))\b', name or "")
        if m: size_text = m.group(1).replace("L", "l")
    return brand, size_text

def extract_ext_id(url: str) -> str:
    try:
        parts = urlparse(url).path.rstrip("/").split("/")
        if "p" in parts:
            i = parts.index("p"); return parts[i+1]
    except Exception:
        pass
    return ""

def parse_jsonld_and_microdata(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    ean = sku = None
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        try:
            data = json.loads(tag.text)
        except Exception:
            continue
        seq = data if isinstance(data, list) else [data]
        for d in seq:
            got = deep_find_kv(d, { *EAN_KEYS, *SKU_KEYS })
            ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
            sku = sku or got.get("sku") or got.get("mpn") or got.get("code")
    if not ean:
        for it in ("gtin13","gtin","ean","ean13","barcode"):
            meta = soup.find(attrs={"itemprop": it})
            if meta:
                ean = ean or (meta.get("content") or meta.get_text(strip=True))
    if not sku:
        for it in ("sku","mpn"):
            meta = soup.find(attrs={"itemprop": it})
            if meta:
                sku = sku or (meta.get("content") or meta.get_text(strip=True))
    return ean, sku

def parse_visible_for_ean(soup: BeautifulSoup) -> Optional[str]:
    for el in soup.find_all(string=EAN_LABEL_RE):
        seg = el.parent.get_text(" ", strip=True) if el and el.parent else str(el)
        m = EAN_RE.search(seg)
        if m: return m.group(0)
    m = EAN_RE.search(soup.get_text(" ", strip=True))
    return m.group(0) if m else None

# ---------------------------- collectors ------------------------------------

def collect_pdp_links(page) -> List[str]:
    sels = [
        ".js-product-container a.card__url",
        "a[href*='/p/']",
        "a[href^='/epood/ee/p/']",
        "a[href^='/epood/ee/tooted/'][href*='/p/']",
        "[data-test*='product'] a[href*='/p/']",
        ".product-card a[href*='/p/']",
    ]
    hrefs: set[str] = set()
    for sel in sels:
        try:
            for el in page.locator(sel).all():
                h = normalize_href(el.get_attribute("href"))
                if h and "/p/" in h:
                    hrefs.add(h)
        except Exception:
            pass
    return sorted(hrefs)

def collect_subcategory_links(page, base_cat_url: str) -> List[str]:
    sels = [
        "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
        ".category-card a[href^='/epood/ee/tooted/']",
        ".category, .subcategory a[href^='/epood/ee/tooted/']",
        "nav a[href^='/epood/ee/tooted/']",
        "a[href^='/epood/ee/tooted/']:not([href*='/p/'])",
    ]
    hrefs: set[str] = set()
    for sel in sels:
        try:
            for el in page.locator(sel).all():
                h = normalize_href(el.get_attribute("href"))
                if h and "/epood/ee/tooted/" in h and "/p/" not in h:
                    hrefs.add(h)
        except Exception:
            pass
    hrefs.discard(base_cat_url.split("?")[0].split("#")[0])
    return sorted(hrefs)

# ---------------------------- crawler ---------------------------------------

def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float) -> List[str]:
    browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
    ctx = browser.new_context(
        locale="et-EE",
        viewport={"width":1440, "height":900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124 Safari/537.36"),
    )
    page = ctx.new_page()
    visited: set[str] = set()
    q: List[str] = [normalize_href(cat_url) or cat_url]
    all_pdps: List[str] = []

    try:
        while q:
            cat = q.pop(0)
            if not cat or cat in visited:
                continue
            visited.add(cat)

            try:
                page.goto(cat, timeout=45000, wait_until="domcontentloaded")
            except Exception:
                continue
            auto_accept_overlays(page)
            wait_for_hydration(page)

            for sc in collect_subcategory_links(page, cat):
                if sc not in visited:
                    q.append(sc)

            pages_seen = 0
            last_total = -1
            while True:
                all_pdps.extend(collect_pdp_links(page))

                clicked = False
                for sel in [
                    "a[rel='next']",
                    "button[aria-label*='Järgmine']",
                    "button:has-text('Järgmine')",
                    "button:has-text('Kuva rohkem')",
                    "button:has-text('Laadi rohkem')",
                    "a:has-text('Järgmine')",
                ]:
                    if page.locator(sel).count() > 0:
                        try

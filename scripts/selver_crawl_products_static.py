#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver.ee static category crawler (no Playwright) -> CSV for staging_selver_products.

Outputs columns:
  ext_id,name,ean_raw,size_text,price,currency,category_path,category_leaf
"""

from __future__ import annotations
import asyncio
import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlencode, urlsplit, urlunsplit, parse_qsl
import re
import csv
import os
import json
import unicodedata
import time
from typing import Iterable, Tuple

BASE = "https://www.selver.ee"

OUTPUT_CSV   = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY    = float(os.getenv("REQ_DELAY", "0.6"))
CONCURRENCY  = int(os.getenv("CONCURRENCY", "8"))
PAGE_LIMIT   = int(os.getenv("PAGE_LIMIT", "0"))    # 0 = unlimited (sensible caps in code)
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

# ---- filters (skip non-food) ----
BANNED = {
    "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid","vaikesed-sisustuskaubad","kuunlad",
    "kook-ja-lauakatmine","uhekordsed-noud","kirja-ja-kontoritarbed","remondi-ja-turvatooted",
    "kulmutus-ja-kokkamisvahendid","omblus-ja-kasitootarbed","meisterdamine","ajakirjad","autojuhtimine",
    "kotid","aed-ja-lilled","lemmikloom","sport","pallimangud","jalgrattasoit","ujumine","matkamine",
    "tervisesport","manguasjad","lutid","lapsehooldus","ideed-ja-hooajad","kodumasinad","elektroonika",
    "meelelahutuselektroonika","vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
    "ilu-ja-tervis","kosmeetika","meigitooted","hugieen","loodustooted-ja-toidulisandid",
}

NON_PRODUCT_SNIPPETS = {
    "/e-selver/","/ostukorv","/cart","/checkout","/search","/otsi","/konto","/customer","/login",
    "/logout","/uudised","/tingimused","/privaatsus","/privacy","/kampaan","/blogi","/app","/pood/",
}

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

def norm(s: str | None) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def is_food_category(path: str) -> bool:
    p = path.lower()
    if not p.startswith("/e-selver/"):
        return False
    return not any(b in p for b in BANNED)

def is_product_like(url: str) -> bool:
    u = urlparse(url)
    if not (u.netloc.endswith("selver.ee") or u.netloc == ""): return False
    p = (u.path or "/").lower()
    if any(s in p for s in NON_PRODUCT_SNIPPETS): return False
    # crude: product slugs are pathy and without file extension
    last = p.rsplit("/", 1)[-1]
    if "." in last: return False
    return p.count("/") >= 1

def add_or_replace_query(url: str, key: str, val: str) -> str:
    u = urlsplit(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[key] = val
    new_q = urlencode(q, doseq=True)
    return urlunsplit((u.scheme, u.netloc, u.path, new_q, u.fragment))

SIZE_RE = re.compile(r"\b(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)
def guess_size(title: str) -> str:
    m = SIZE_RE.search(title or "")
    if not m: return ""
    num, unit = m.groups()
    return f"{num.replace(',', '.')} {unit.lower()}"

def parse_jsonld(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []
    for s in soup.find_all("script", {"type":"application/ld+json"}):
        try:
            data = json.loads(s.string or "{}")
            if isinstance(data, list): out.extend(data)
            elif isinstance(data, dict): out.append(data)
        except Exception:
            continue
    return out

def take_product_block(blocks: Iterable[dict]) -> dict | None:
    for b in blocks:
        t = b.get("@type")
        if isinstance(t, list): t = [str(x).lower() for x in t]
        elif isinstance(t, str): t = [t.lower()]
        if t and ("product" in t or "schema:product" in t):
            return b
    return None

def extract_breadcrumbs(blocks: list[dict]) -> str:
    for b in blocks:
        t = str(b.get("@type","")).lower()
        if t in ("breadcrumblist","schema:breadcrumblist"):
            try:
                items = b.get("itemListElement") or []
                names = []
                for it in items:
                    item = it.get("item") or {}
                    nm = norm(item.get("name"))
                    if nm and nm.lower() != "e-selver":
                        names.append(nm)
                return " / ".join(names)
            except Exception:
                continue
    return ""

def extract_price_currency(block: dict, soup: BeautifulSoup) -> Tuple[float, str]:
    offers = block.get("offers") if block else None
    if isinstance(offers, list): offers = offers[0] if offers else None
    if isinstance(offers, dict):
        pr_raw = str(offers.get("price","0")).replace(",", ".")
        try: price = float(pr_raw)
        except: price = 0.0
        cur = (offers.get("priceCurrency") or "EUR").upper()
        return price, cur
    # fallback: scrape a “xx,yy €”
    try:
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*€", txt)
        if m: return float(m.group(1).replace(",", ".")), "EUR"
    except Exception:
        pass
    return 0.0, "EUR"

def extract_ean(block: dict, soup: BeautifulSoup, html: str) -> str:
    # 1) JSON-LD
    if block:
        ean = block.get("gtin13") or block.get("gtin") or block.get("sku")
        if ean and re.fullmatch(r"\d{8,14}", str(ean)): return str(ean)
    # 2) Definition lists or labels: Ribakood / EAN / GTIN/EAN
    LABELS = ["Ribakood","EAN","EAN-kood","EAN kood","GTIN","GTIN/EAN"]
    # dt/dd
    for dt in soup.select("dt, strong, b, span, div"):
        t = norm(dt.get_text())
        if t in LABELS or any(lbl.lower() in t.lower() for lbl in LABELS):
            nx = dt.find_next_sibling()
            if nx:
                val = re.sub(r"\D", "", nx.get_text())
                if re.fullmatch(r"\d{8,14}", val): return val
    # regex across HTML
    m = re.search(r"(?:Ribakood|GTIN(?:/EAN)?|EAN(?:-kood)?)\D*?(\d{8,14})", html, re.I|re.S)
    return m.group(1) if m else ""

async def fetch(session: aiohttp.ClientSession, url: str) -> tuple[str, str]:
    async with session.get(url, allow_redirects=True, timeout=ClientTimeout(total=35)) as r:
        r.raise_for_status()
        txt = await r.text()
        return txt, str(r.url)

async def discover_top_categories(session) -> list[str]:
    start = urljoin(BASE, "/e-selver")
    try:
        html, _ = await fetch(session, start)
    except Exception:
        return []
    soup = BeautifulSoup(html, "lxml")
    out, seen = [], set()
    for a in soup.select("a[href*='/e-selver/']"):
        href = a.get("href")
        if not href: continue
        u = urljoin(BASE, href)
        p = urlparse(u).path
        if is_food_category(p) and p not in seen:
            out.append(u); seen.add(p)
    return out

async def bfs_categories(session, seeds: list[str]) -> list[str]:
    # simple BFS: follow links that still look like /e-selver/... and pass the food filter
    seen_paths, q, out = set(), list(seeds), []
    while q:
        url = q.pop(0)
        pth = urlparse(url).path
        if pth in seen_paths: continue
        seen_paths.add(pth)
        out.append(url)
        try:
            html, _ = await fetch(session, url)
        except Exception:
            continue
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href*='/e-selver/']"):
            u = urljoin(BASE, a.get("href") or "")
            pp = urlparse(u).path
            if is_food_category(pp) and pp not in seen_paths:
                q.append(u)
        await asyncio.sleep(REQ_DELAY)
    # dedupe by path to keep it stable
    uniq, seen = [], set()
    for u in out:
        p = urlparse(u).path
        if p not in seen:
            uniq.append(u); seen.add(p)
    return uniq

def collect_product_links_from_html(html: str, base_url: str) -> set[str]:
    soup = BeautifulSoup(html, "lxml")
    links: set[str] = set()
    for sel in ["a[href]"]:
        for a in soup.select(sel):
            href = a.get("href")
            if not href: continue
            u = urljoin(BASE, href)
            if is_product_like(u):
                links.add(u)
    return links

async def crawl_category(session, cat_url: str, page_limit: int) -> set[str]:
    """
    Try pagination with ?page=N, then ?p=N. Stop on no new links or after cap.
    """
    product_urls: set[str] = set()
    max_pages = page_limit or 50

    for param in ("page", "p"):
        no_progress = 0
        for n in range(1, max_pages + 1):
            url_n = add_or_replace_query(cat_url, param, str(n))
            try:
                html, _ = await fetch(session, url_n)
            except Exception:
                break
            new_links = collect_product_links_from_html(html, url_n) - product_urls
            product_urls |= new_links
            await asyncio.sleep(REQ_DELAY)
            if not new_links:
                no_progress += 1
            else:
                no_progress = 0
            if no_progress >= 2:   # two empties in a row → give up this mode
                break
        if product_urls:
            break
    return product_urls

async def parse_product(session, url: str) -> dict | None:
    try:
        html, final = await fetch(session, url)
    except Exception:
        return None
    soup = BeautifulSoup(html, "lxml")
    blocks = parse_jsonld(soup)
    prod = take_product_block(blocks)

    name = norm(soup.select_one("h1").get_text() if soup.select_one("h1") else (prod.get("name") if prod else ""))
    if not name:
        return None

    ean = extract_ean(prod or {}, soup, html)
    price, currency = extract_price_currency(prod or {}, soup)
    crumbs = extract_breadcrumbs(blocks) or ""
    size = guess_size(name)

    return {
        "ext_id": final,
        "name": name,
        "ean_raw": ean,
        "size_text": size,
        "price": f"{price:.2f}",
        "currency": currency,
        "category_path": crumbs,
        "category_leaf": crumbs.split(" / ")[-1] if crumbs else "",
    }

async def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV) or ".", exist_ok=True)

    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "et-EE,et;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": BASE,
    }

    timeout = ClientTimeout(total=40)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=conn) as session:
        # seeds: from file or autodiscover
        seeds: list[str] = []
        if os.path.exists(CATEGORIES_FILE):
            with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if ln and not ln.startswith("#"):
                        seeds.append(urljoin(BASE, ln))
        if not seeds:
            top = await discover_top_categories(session)
            seeds = await bfs_categories(session, top)

        print(f"[selver] Food categories to crawl: {len(seeds)}")
        for s in seeds: print("[selver] ", s)

        # 1) collect product URLs
        all_products: set[str] = set()
        for cu in seeds:
            urls = await crawl_category(session, cu, PAGE_LIMIT)
            all_products |= urls
            print(f"[selver] {cu} -> +{len(urls)} products (total: {len(all_products)})")

        if not all_products:
            print("[selver] No product URLs found.")
            # still write header so downstream doesn't explode
            with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=[
                    "ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"
                ]).writeheader()
            return

        # 2) fetch product pages and write CSV
        fieldnames = ["ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"]
        written = 0
        async def bounded_parse(u: str, sem: asyncio.Semaphore, w: csv.DictWriter):
            nonlocal written
            async with sem:
                item = await parse_product(session, u)
                await asyncio.sleep(REQ_DELAY)
                if item:
                    w.writerow(item); written += 1

        sem = asyncio.Semaphore(CONCURRENCY)
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            tasks = [bounded_parse(u, sem, w) for u in sorted(all_products)]
            # process in chunks to keep memory small
            CHUNK = 300
            for i in range(0, len(tasks), CHUNK):
                await asyncio.gather(*tasks[i:i+CHUNK])
                f.flush()
                print(f"[selver] wrote {written} rows so far…")

        print(f"[selver] Done. Total rows: {written}")

if __name__ == "__main__":
    asyncio.run(main())

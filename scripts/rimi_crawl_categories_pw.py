#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee category crawler → PDP extractor → CSV (and optional DB upsert)

Output CSV columns (canonical-ish):
  ext_id, ean_raw, sku_raw, name, size_text, brand, manufacturer,
  price, currency, image_url, category_path, category_leaf, source_url

Notes:
  - EAN is often *not* exposed; we sniff JSON-LD, microdata, globals, XHR.
  - size_text/brand parsed heuristically from name & spec blocks.
  - ext_id is the trailing /p/<id> on PDP URLs.
"""
from __future__ import annotations
import argparse, os, re, sys, time, csv, json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

EAN_RE = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE = re.compile(r"(\d+[.,]\d+)\s*€")
SKU_KEYS = {"sku","mpn","itemNumber","productCode","code","id","itemid"}
EAN_KEYS = {"ean","ean13","gtin","gtin13","barcode"}

def deep_find_kv(obj: Any, keys: set) -> Dict[str,str]:
  out = {}
  def walk(x):
    if isinstance(x, dict):
      for k,v in x.items():
        lk = str(k).lower()
        if lk in keys and isinstance(v,(str,int)):
          out[lk] = str(v)
        walk(v)
    elif isinstance(x, list):
      for i in x: walk(i)
  walk(obj); return out

def parse_price(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
  """
  Try to read main price; Rimi often formats as "1,29 €"
  """
  # Try meta/ld first
  for tag in soup.select('meta[itemprop="price"], [itemprop="price"]'):
    val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
    if val: return val.replace(",", "."), "EUR"
  # Visible text fallback
  m = MONEY_RE.search(soup.get_text(" ", strip=True))
  if m: return m.group(1).replace(",", "."), "EUR"
  return None, None

def parse_brand_and_size(soup: BeautifulSoup, name: str) -> Tuple[Optional[str], Optional[str]]:
  brand = None
  size_text = None
  # Brand often elsewhere; try spec table
  for row in soup.select("table tr"):
    th = row.find("th")
    td = row.find("td")
    if not th or not td: continue
    key = th.get_text(" ", strip=True).lower()
    val = td.get_text(" ", strip=True)
    if not brand and "tootja" in key or "brand" in key:
      brand = val
    if not size_text and ("kogus" in key or "maht" in key or "netokogus" in key or "pakend" in key or "neto" in key):
      size_text = val
  # Heuristic from name tail “... 500 g”, “1,5 L” etc
  if not size_text:
    m = re.search(r'(\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk))\b', name)
    if m: size_text = m.group(1).replace("L","l")
  return brand, size_text

def extract_ext_id(url: str) -> str:
  # /p/<id>
  try:
    parts = urlparse(url).path.rstrip("/").split("/")
    if "p" in parts:
      i = parts.index("p")
      return parts[i+1]
  except Exception:
    pass
  return ""

def parse_jsonld_and_microdata(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
  ean = sku = None
  # JSON-LD
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
  # Microdata itemprop
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

def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float) -> List[str]:
  urls: List[str] = []
  browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
  ctx = browser.new_context(locale="et-EE", viewport={"width":1280, "height":900})
  page = ctx.new_page()
  try:
    page.goto(cat_url, timeout=30000, wait_until="domcontentloaded")
    # cookie accept if present
    for label in ("Nõustun","Nõustu","Accept","Allow all","OK","Selge"):
      try:
        page.get_by_role("button", name=re.compile(label, re.I)).click(timeout=1200); break
      except Exception: pass

    cur = 1
    while True:
      page.wait_for_timeout(int(req_delay*1000))
      anchors = page.locator("a[href*='/p/']").all()
      hrefs = sorted(set(a.get_attribute("href") for a in anchors if a))
      hrefs = [h.split("?")[0] for h in hrefs if h and "/p/" in h]
      urls.extend(hrefs)
      if page_limit and cur >= page_limit:
        break
      next_btn = page.locator("a[rel='next'], button[aria-label*='Järgmine'], a:has-text('Järgmine')")
      if next_btn.count() == 0:
        break
      try:
        next_btn.first.click(timeout=5000)
        cur += 1
      except Exception:
        break
  finally:
    ctx.close(); browser.close()
  seen, out = set(), []
  for u in urls:
    if u and u not in seen:
      seen.add(u); out.append(u)
  return out

def parse_pdp(pw, url: str, headless: bool, req_delay: float) -> Dict[str,str]:
  browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
  ctx = browser.new_context(locale="et-EE", viewport={"width":1280,"height":900})
  page = ctx.new_page()
  name = brand = size_text = image_url = category_path = ""
  ean = sku = price = currency = None
  try:
    page.goto(url, timeout=30000, wait_until="domcontentloaded")
    for label in ("Nõustun","Nõustu","Accept","Allow all","OK","Selge"):
      try:
        page.get_by_role("button", name=re.compile(label, re.I)).click(timeout=1200); break
      except Exception: pass
    page.wait_for_timeout(int(req_delay*1000))
    html = page.content()
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    if h1: name = h1.get_text(strip=True)
    img = soup.find("img", {"src": re.compile(r"/images/")}) or soup.find("img")
    if img:
      image_url = img.get("src") or img.get("data-src") or ""
    crumbs = [a.get_text(strip=True) for a in soup.select("nav a, .breadcrumb a") if a.get_text(strip=True)]
    if crumbs:
      category_path = " > ".join(crumbs[-5:])

    brand, size_text = parse_brand_and_size(soup, name)

    e1, s1 = parse_jsonld_and_microdata(soup)
    ean = ean or e1; sku = sku or s1

    for glb in ["__NUXT__","__NEXT_DATA__","APP_STATE","dataLayer"]:
      try:
        data = page.evaluate(f"window['{glb}']")
        if data:
          got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS })
          ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
          sku = sku or got.get("sku") or got.get("mpn") or got.get("code") or got.get("id")
      except Exception:
        pass

    if not ean:
      e2 = parse_visible_for_ean(soup)
      if e2: ean = e2

    price, currency = parse_price(soup)

  except PWTimeout:
    name = name or ""
  finally:
    ctx.close(); browser.close()

  ext_id = extract_ext_id(url)
  row = {
    "ext_id": ext_id,
    "ean_raw": (ean or "").strip(),
    "sku_raw": (sku or "").strip(),
    "name": name.strip(),
    "size_text": (size_text or "").strip(),
    "brand": (brand or "").strip(),
    "manufacturer": "",
    "price": (price or "").strip(),
    "currency": (currency or "").strip(),
    "image_url": (image_url or "").strip(),
    "category_path": (category_path or "").strip(),
    "category_leaf": category_path.split(" > ")[-1] if category_path else "",
    "source_url": url.split("?")[0],
  }
  return row

def maybe_upsert_db(csv_path: str):
  dsn = os.getenv("DATABASE_URL", "")
  if not dsn:
    print("DATABASE_URL not set → skipping DB upsert.")
    return
  import psycopg2, psycopg2.extras
  conn = psycopg2.connect(dsn)
  cur = conn.cursor()
  with open(csv_path, encoding="utf-8") as f:
    r = csv.DictReader(f)
    rows = list(r)
  for x in rows:
    ean = x["ean_raw"] or None
    name = x["name"]
    cur.execute(
      """
      INSERT INTO products (ean, name, size_text, brand, image_url, source_url, last_seen_utc)
      VALUES (%s,%s,%s,%s,%s,%s, NOW())
      ON CONFLICT (ean) DO UPDATE
      SET name=EXCLUDED.name, size_text=EXCLUDED.size_text, brand=EXCLUDED.brand,
          image_url=EXCLUDED.image_url, source_url=EXCLUDED.source_url, last_seen_utc=NOW()
      """,
      (ean, name, x["size_text"] or None, x["brand"] or None, x["image_url"] or None, x["source_url"]) 
    )
    if x["price"]:
      cur.execute(
        """
        INSERT INTO prices (product_ean, store_id, price, currency, collected_at)
        VALUES (%s, NULL, %s, %s, NOW())
        """,
        (ean, x["price"], x["currency"] or "EUR")
      )
  conn.commit()
  cur.close(); conn.close()
  print(f"Upserted {len(rows)} rows into DB.")

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--cats-file", default="data/rimi_categories.txt")
  ap.add_argument("--page-limit", type=int, default=0)
  ap.add_argument("--max-products", type=int, default=0)
  ap.add_argument("--headless", type=int, default=1)
  ap.add_argument("--req-delay", type=float, default=0.4)
  args = ap.parse_args()

  with open(args.cats_file, encoding="utf-8") as f:
    categories = [l.strip() for l in f if l.strip()]
  print(f"[rimi] categories: {len(categories)}")

  out_csv = "rimi_products.csv"
  seen_pdp = set()
  count = 0

  with sync_playwright() as pw, open(out_csv, "w", newline="", encoding="utf-8") as f:
    wr = csv.DictWriter(f, fieldnames=[
      "ext_id","ean_raw","sku_raw","name","size_text","brand","manufacturer",
      "price","currency","image_url","category_path","category_leaf","source_url"
    ])
    wr.writeheader()

    for ci, cat in enumerate(categories, 1):
      print(f"[cat {ci}/{len(categories)}] {cat}")
      try:
        pdps = crawl_category(pw, cat, args.page_limit, bool(args.headless), args.req_delay)
      except Exception as e:
        print(f"  ! category error: {e}")
        continue
      print(f"  -> found {len(pdps)} PDPs (pre-dedupe)")

      for u in pdps:
        if u in seen_pdp: continue
        seen_pdp.add(u)
        row = parse_pdp(pw, u, bool(args.headless), args.req_delay)
        wr.writerow(row)
        count += 1
        if args.max_products and count >= args.max_products:
          print(f"[rimi] reached max_products={args.max_products}")
          break
      if args.max_products and count >= args.max_products:
        break

  print(f"[rimi] wrote {out_csv} with {count} rows")
  try:
    maybe_upsert_db(out_csv)
  except Exception as e:
    print(f"[db] upsert skipped/failed: {e}")

if __name__ == "__main__":
  main()

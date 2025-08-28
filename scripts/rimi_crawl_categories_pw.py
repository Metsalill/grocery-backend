#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) category crawler → PDP extractor → CSV (and optional DB upsert)

CSV columns:
  store_chain, store_name, store_channel,
  ext_id, ean_raw, sku_raw, name, size_text, brand, manufacturer,
  price, currency, image_url, category_path, category_leaf, source_url

Notes:
  - EAN is often *not* exposed; we sniff JSON-LD, microdata, global JS objects, and JSON XHRs.
  - size_text/brand parsed heuristically from name & spec blocks.
  - ext_id is the trailing /p/<id> on PDP URLs.
"""
from __future__ import annotations
import argparse, os, re, sys, time, csv, json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# --- Store metadata ----------------------------------------------------------
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
  out = {}
  def walk(x):
    if isinstance(x, dict):
      for k,v in x.items():
        lk = str(k).lower()
        if lk in keys and isinstance(v,(str,int,str)):
          out[lk] = str(v)
        walk(v)
    elif isinstance(x, list):
      for i in x: walk(i)
  walk(obj); return out

def normalize_href(href: Optional[str]) -> Optional[str]:
  if not href:
    return None
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
      page.wait_for_timeout(150)
    except Exception:
      pass

def collect_pdp_links(page) -> List[str]:
  sels = [
    "a[href*='/p/']",
    "a[href^='/epood/ee/p/']",
    "a[href^='/epood/ee/tooted/'][href*='/p/']",
    "[data-test*='product'] a[href*='/p/']",
    ".product-card a[href*='/p/']",
  ]
  hrefs: set[str] = set()
  for sel in sels:
    for el in page.locator(sel).all():
      h = el.get_attribute("href")
      h = normalize_href(h)
      if h and "/p/" in h:
        hrefs.add(h)
  return sorted(hrefs)

def collect_subcategory_links(page, base_cat_url: str) -> List[str]:
  # Find obvious category cards/tiles/menus that stay inside the tooted subtree
  sels = [
    "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
    ".category-card a[href^='/epood/ee/tooted/']",
    ".category, .subcategory a[href^='/epood/ee/tooted/']",
    "nav a[href^='/epood/ee/tooted/']",
    "a[href^='/epood/ee/tooted/']:not([href*='/p/'])",
  ]
  hrefs: set[str] = set()
  for sel in sels:
    for el in page.locator(sel).all():
      h = normalize_href(el.get_attribute("href"))
      if not h or "/p/" in h: 
        continue
      if "/epood/ee/tooted/" in h:
        hrefs.add(h)
  hrefs.discard(base_cat_url.split("?")[0].split("#")[0])
  return sorted(hrefs)

# ----------------------------- parsing --------------------------------------

def parse_price(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
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
    m = re.search(r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk))\b', name)
    if m: size_text = m.group(1).replace("L","l")
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

# ---------------------------- crawler ---------------------------------------

def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float) -> List[str]:
  """
  BFS through category → subcategories until leaf pages; collect PDP links.
  Handles both explicit pagers and infinite scroll.
  """
  browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
  ctx = browser.new_context(
    locale="et-EE",
    viewport={"width":1440, "height":900},
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
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

      try:
        page.wait_for_selector(
          "a[href*='/p/'], .product-card, [data-test*='product'], a[href^='/epood/ee/tooted/']:has(h3)",
          timeout=6000
        )
      except Exception:
        pass

      # enqueue subcategories (if any)
      for sc in collect_subcategory_links(page, cat):
        if sc not in visited:
          q.append(sc)

      # now collect pdps with pagination/scroll
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
            try:
              page.locator(sel).first.click(timeout=3000)
              clicked = True
              page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
              break
            except Exception:
              pass

        if not clicked:
          before = len(collect_pdp_links(page))
          for _ in range(3):
            page.mouse.wheel(0, 2400)
            page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
          after = len(collect_pdp_links(page))
          if after <= before:
            break

        pages_seen += 1
        if page_limit and pages_seen >= page_limit:
          break
        if len(all_pdps) == last_total:
          page.wait_for_timeout(int(max(req_delay, 0.2) * 1000))
        last_total = len(all_pdps)

  finally:
    ctx.close(); browser.close()

  # dedupe preserving order
  seen, out = set(), []
  for u in all_pdps:
    if u and u not in seen:
      seen.add(u); out.append(u)
  return out

def parse_pdp(pw, url: str, headless: bool, req_delay: float) -> Dict[str,str]:
  browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
  ctx = browser.new_context(
    locale="et-EE",
    viewport={"width":1440,"height":900},
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
  )
  page = ctx.new_page()
  name = brand = size_text = image_url = category_path = ""
  ean = sku = price = currency = None
  try:
    page.goto(url, timeout=45000, wait_until="domcontentloaded")
    auto_accept_overlays(page)
    page.wait_for_timeout(int(req_delay*1000))
    soup = BeautifulSoup(page.content(), "lxml")

    h1 = soup.find("h1")
    if h1: name = h1.get_text(strip=True)
    img = soup.find("img", {"src": re.compile(r"/images/")}) or soup.find("img")
    if img:
      image_url = normalize_href(img.get("src") or img.get("data-src") or "")

    crumbs = [a.get_text(strip=True) for a in soup.select("nav a, .breadcrumb a") if a.get_text(strip=True)]
    if crumbs:
      crumbs = [c for c in crumbs if c]
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
  return {
    "store_chain": STORE_CHAIN,
    "store_name": STORE_NAME,
    "store_channel": STORE_CHANNEL,
    "ext_id": ext_id,
    "ean_raw": (ean or "").strip(),
    "sku_raw": (sku or "").strip(),
    "name": (name or "").strip(),
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

# ------------------------------ DB ------------------------------------------

def maybe_upsert_db(csv_path: str):
  dsn = os.getenv("DATABASE_URL", "")
  if not dsn:
    print("DATABASE_URL not set → skipping DB upsert.")
    return
  import psycopg2, psycopg2.extras
  conn = psycopg2.connect(dsn)
  cur = conn.cursor()

  cur.execute("""
    INSERT INTO stores (name, chain)
    VALUES (%s, %s)
    ON CONFLICT (name, chain) DO NOTHING
    RETURNING id
  """, (STORE_NAME, STORE_CHAIN))
  row = cur.fetchone()
  if row is None:
    cur.execute("SELECT id FROM stores WHERE name=%s AND chain=%s", (STORE_NAME, STORE_CHAIN))
    row = cur.fetchone()
  store_id = row[0] if row else None

  with open(csv_path, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

  for x in rows:
    ean = x["ean_raw"] or None
    name = x["name"] or None
    cur.execute(
      """
      INSERT INTO products (ean, name, size_text, brand, image_url, source_url, last_seen_utc)
      VALUES (%s,%s,%s,%s,%s,%s, NOW())
      ON CONFLICT (ean) DO UPDATE
      SET name = COALESCE(EXCLUDED.name, products.name),
          size_text = COALESCE(EXCLUDED.size_text, products.size_text),
          brand = COALESCE(EXCLUDED.brand, products.brand),
          image_url = COALESCE(EXCLUDED.image_url, products.image_url),
          source_url = EXCLUDED.source_url,
          last_seen_utc = NOW()
      """,
      (ean, name, x["size_text"] or None, x["brand"] or None, x["image_url"] or None, x["source_url"])
    )
    if x["price"]:
      cur.execute(
        """
        INSERT INTO prices (product_ean, store_id, price, currency, collected_at)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (ean, store_id, x["price"], x["currency"] or "EUR")
      )

  conn.commit()
  cur.close(); conn.close()
  print(f"Upserted {len(rows)} rows into DB (store_id={store_id}).")

# ------------------------------ main ----------------------------------------

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
  seen = set()
  count = 0

  with sync_playwright() as pw, open(out_csv, "w", newline="", encoding="utf-8") as f:
    wr = csv.DictWriter(f, fieldnames=[
      "store_chain","store_name","store_channel",
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
        if u in seen: 
          continue
        seen.add(u)
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

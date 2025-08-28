#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) category crawler → PDP extractor → CSV (and optional DB upsert)

Highlights borrowed from your Selver crawler:
- Request router blocks heavy third-parties/images/fonts but keeps rimi.ee JS/XHR.
- Robust PDP/category detection for /epood/.../p/<id> and /c/<code>.
- Hydration wait (Rimi hides main until hydrated).
- Strict subcategory discovery (only /c/ links; never /p/).
- JSON-LD prioritized for name/brand/price/currency; DOM/microdata/globals as fallbacks.
- Pager and infinite scroll fallback.
- Conservative EAN/SKU detection (labels + 13-digit scan fallback).

CLI:
  python rimi_crawl_categories_pw.py \
      --category https://www.rimi.ee/epood/ee/tooted/leivad-saiad-kondiitritooted \
      --page-limit 0 --headless 1 --req-delay 0.4 --output rimi_products.csv

Or seed from file (one category URL per line):
  python rimi_crawl_categories_pw.py --from-file data/rimi_categories.txt --output rimi.csv

Optional DB upsert (Postgres, simple ON CONFLICT on ext_id):
  env DATABASE_URL=postgres://user:pass@host:5432/db \
  python rimi_crawl_categories_pw.py ... --upsert 1 --table staging_rimi_products
"""
from __future__ import annotations
import argparse, os, re, csv, json, sys
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ------------------------------ constants -----------------------------------
STORE_CHAIN   = "Rimi"
STORE_NAME    = "Rimi ePood"
STORE_CHANNEL = "online"
BASE = "https://www.rimi.ee"

EAN_RE        = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE  = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE      = re.compile(r"(\d+[.,]\d+)\s*€")
SKU_KEYS      = {"sku","mpn","itemNumber","productCode","code","id","itemid","retailer_item_id"}
EAN_KEYS      = {"ean","ean13","gtin","gtin13","barcode"}

ALLOWED_HOSTS = {"www.rimi.ee","rimi.ee"}
BLOCK_HOSTS = {
  # analytics/ads/monitoring
  "googletagmanager.com","google-analytics.com","doubleclick.net","facebook.net",
  "demdex.net","adobedtm.com","assets.adobedtm.com","omtrdc.net",
  "cookiebot.com","consent.cookiebot.com","consentcdn.cookiebot.com",
  "hotjar.com","static.hotjar.com","nr-data.net","newrelic.com","pingdom.net",
  # heavy image CDNs (PDP has OG image anyway; we read src if present)
  "rimibaltic-res.cloudinary.com","rimibaltic-web-res.cloudinary.com",
}
BLOCK_TYPES = {"image","font","media","websocket","manifest"}

# ------------------------------- utilities ----------------------------------
def deep_find_kv(obj: Any, keys: set) -> Dict[str,str]:
  out: Dict[str,str] = {}
  def walk(x):
    if isinstance(x, dict):
      for k,v in x.items():
        lk = str(k).lower()
        if lk in keys and isinstance(v, (str,int,float,bool)):
          out[lk] = str(v)
        walk(v)
    elif isinstance(x, list):
      for i in x: walk(i)
  walk(obj); return out

def normalize_href(href: Optional[str]) -> Optional[str]:
  if not href: return None
  href = href.split("?")[0].split("#")[0]
  return href if href.startswith("http") else urljoin(BASE, href)

def auto_accept_overlays(page) -> None:
  labels = [
    r"Nõustun", r"Nõustu", r"Luba kõik", r"Accept", r"Allow all", r"OK", r"Selge",
    r"Jätka", r"Vali hiljem", r"Continue", r"Close", r"Sulge",
    r"Vali pood", r"Vali teenus", r"Telli koju", r"Vali kauplus",
    r"Näita kõiki tooteid", r"Kuva tooted", r"Kuva kõik tooted",
  ]
  for lab in labels:
    try:
      page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=700)
      page.wait_for_timeout(150)
    except Exception:
      pass

def wait_for_hydration(page, timeout_ms: int = 10000) -> None:
  try:
    page.wait_for_function(
      """() => {
           const main = document.querySelector('main');
           const vis = main && getComputedStyle(main).visibility !== 'hidden';
           const pdps = document.querySelector('a[href*="/p/"]');
           const cards = document.querySelector('.js-product-container a.card__url');
           return (main && vis) || pdps || cards;
         }""",
      timeout=timeout_ms
    )
  except Exception:
    pass

def _router(route, request):
  try:
    url  = request.url
    typ  = request.resource_type
    host = urlparse(url).netloc.lower()
    if typ in BLOCK_TYPES: return route.abort()
    if any(host == d or host.endswith("."+d) for d in BLOCK_HOSTS): return route.abort()
    # keep first-party
    if host.endswith("rimi.ee"): return route.continue_()
    return route.continue_()
  except Exception:
    return route.continue_()

def _is_rimi_product_like(url: str) -> bool:
  try:
    u = urlparse(url)
    if u.netloc and u.netloc.lower() not in ALLOWED_HOSTS: return False
    p = (u.path or "").lower()
    return p.startswith("/epood/") and ("/p/" in p) and re.search(r"/p/\d{3,}", p) is not None
  except Exception:
    return False

def _is_rimi_category_like(url: str) -> bool:
  try:
    u = urlparse(url)
    if u.netloc and u.netloc.lower() not in ALLOWED_HOSTS: return False
    p = (u.path or "").lower()
    return p.startswith("/epood/") and ("/c/" in p) and ("/p/" not in p)
  except Exception:
    return False

def extract_ext_id(url: str) -> str:
  try:
    parts = urlparse(url).path.rstrip("/").split("/")
    if "p" in parts:
      i = parts.index("p"); return parts[i+1]
  except Exception:
    pass
  return ""

# ------------------------------- parsing ------------------------------------
def parse_price_from_ldjson(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
  for tag in soup.find_all("script", {"type":"application/ld+json"}):
    try:
      data = json.loads(tag.text)
    except Exception:
      continue
    seq = data if isinstance(data, list) else [data]
    for d in seq:
      if isinstance(d, dict) and ("Product" in str(d.get("@type","")) or "product" in json.dumps(d).lower()):
        offers = d.get("offers")
        if isinstance(offers, list):
          offers = offers[0] if offers else {}
        if isinstance(offers, dict):
          price = offers.get("price")
          curr  = offers.get("priceCurrency") or "EUR"
          if price:
            return str(price).replace(",", "."), curr
  return None, None

def parse_price_from_dom_or_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
  p,c = parse_price_from_ldjson(soup)
  if p: return p,c
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
    if (not brand) and ("kaubamärk" in key or "tootja" in key or "brand" in key):
      brand = val
    if (not size_text) and any(k in key for k in ("kogus","maht","netokogus","pakend","neto","suurus")):
      size_text = val
  if not size_text:
    m = re.search(r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|L|tk))\b', name or "")
    if m: size_text = m.group(1).replace("L","l")
  return brand, size_text

def parse_jsonld_and_microdata(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
  ean = sku = brand = pname = None
  for tag in soup.find_all("script", {"type":"application/ld+json"}):
    try:
      data = json.loads(tag.text)
    except Exception:
      continue
    seq = data if isinstance(data, list) else [data]
    for d in seq:
      if not isinstance(d, dict): continue
      pname = pname or d.get("name")
      if isinstance(d.get("brand"), dict):
        brand = brand or d["brand"].get("name")
      elif isinstance(d.get("brand"), str):
        brand = brand or d.get("brand")
      got = deep_find_kv(d, { *EAN_KEYS, *SKU_KEYS })
      ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
      sku = sku or got.get("sku") or got.get("mpn") or got.get("code") or got.get("retailer_item_id")
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
  return (ean or None), (sku or None), (brand or None), (pname or None)

def parse_visible_for_ean(soup: BeautifulSoup) -> Optional[str]:
  for el in soup.find_all(string=EAN_LABEL_RE):
    seg = el.parent.get_text(" ", strip=True) if el and hasattr(el, "parent") and el.parent else str(el)
    m = EAN_RE.search(seg)
    if m: return m.group(0)
  m = EAN_RE.search(soup.get_text(" ", strip=True))
  return m.group(0) if m else None

# ---------------------------- collectors ------------------------------------
def collect_pdp_links(page) -> List[str]:
  sels = [
    ".js-product-container a.card__url",
    "a[href^='/epood/ee/tooted/'][href*='/p/']",
    "a[href^='/epood/ee/p/']",
    "a[href*='/p/']",
    "[data-test*='product'] a[href*='/p/']",
    ".product-card a[href*='/p/']",
  ]
  hrefs: set[str] = set()
  for sel in sels:
    try:
      for el in page.locator(sel).all():
        h = normalize_href(el.get_attribute("href"))
        if h and _is_rimi_product_like(h):
          hrefs.add(h)
    except Exception:
      pass
  return sorted(hrefs)

def collect_subcategory_links(page, base_cat_url: str) -> List[str]:
  sels = [
    "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
    ".category-card a[href^='/epood/ee/tooted/']",
    "nav a[href^='/epood/ee/tooted/']",
    "a[href^='/epood/ee/tooted/']:not([href*='/p/'])",
  ]
  hrefs: set[str] = set()
  base_clean = (base_cat_url.split("?")[0].split("#")[0] if base_cat_url else "")
  for sel in sels:
    try:
      for el in page.locator(sel).all():
        h = normalize_href(el.get_attribute("href"))
        if not h: continue
        if h.split("?")[0].split("#")[0] == base_clean:
          continue
        if _is_rimi_category_like(h):
          hrefs.add(h)
    except Exception:
      pass
  return sorted(hrefs)

# ---------------------------- crawler ---------------------------------------
def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float) -> List[str]:
  browser = pw.chromium.launch(headless=headless, args=["--no-sandbox","--disable-dev-shm-usage"])
  ctx = browser.new_context(
    locale="et-EE",
    viewport={"width":1440, "height":900},
    user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
    ignore_https_errors=True,
    service_workers="block",
  )
  ctx.route("**/*", _router)
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

      # enqueue subcategories (if any)
      for sc in collect_subcategory_links(page, cat):
        if sc not in visited:
          q.append(sc)

      # collect pdps with pager/scroll fallback
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

# --------------------------- PDP parser -------------------------------------
def parse_pdp(pw, url: str, headless: bool, req_delay: float) -> Dict[str,str]:
  browser = pw.chromium.launch(headless=headless, args=["--no-sandbox","--disable-dev-shm-usage"])
  ctx = browser.new_context(
    locale="et-EE",
    viewport={"width":1440,"height":900},
    user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
    ignore_https_errors=True,
    service_workers="block",
  )
  ctx.route("**/*", _router)
  page = ctx.new_page()

  name = brand = size_text = image_url = category_path = ""
  ean = sku = price = currency = None
  ext_id_from_attr = ""

  try:
    page.goto(url, timeout=45000, wait_until="domcontentloaded")
    auto_accept_overlays(page)
    wait_for_hydration(page)
    page.wait_for_timeout(int(req_delay*1000))

    # product card container often carries EEC JSON and product code
    try:
      card = page.locator(".js-product-container").first
      if card.count() > 0:
        raw = card.get_attribute("data-gtm-eec-product")
        if raw:
          try:
            eec = json.loads(raw)
            if isinstance(eec, dict):
              if eec.get("price") is not None and not price:
                price = str(eec.get("price"))
              currency = currency or eec.get("currency")
              brand = brand or eec.get("brand")
              sku = sku or str(eec.get("id") or "")
          except Exception:
            pass
        dp = card.get_attribute("data-product-code")
        if dp:
          ext_id_from_attr = dp.strip()
    except Exception:
      pass

    html = page.content()
    soup = BeautifulSoup(html, "lxml")

    # JSON-LD: name/brand/ids
    e_ld, s_ld, b_ld, n_ld = parse_jsonld_and_microdata(soup)
    ean = ean or e_ld
    sku = sku or s_ld
    brand = brand or b_ld
    if n_ld: name = name or n_ld

    # name (DOM fallback)
    if not name:
      h1 = soup.find("h1")
      if h1: name = h1.get_text(strip=True)

    # image
    ogimg = soup.find("meta", {"property":"og:image"})
    if ogimg and ogimg.get("content"):
      image_url = ogimg.get("content")
    else:
      img = soup.find("img")
      if img:
        image_url = img.get("src") or img.get("data-src") or ""
    if image_url:
      image_url = normalize_href(image_url)

    # breadcrumbs (stable container)
    crumbs = [a.get_text(strip=True) for a in soup.select(".section-header a")]
    if not crumbs:
      crumbs = [a.get_text(strip=True) for a in soup.select("nav a, .breadcrumb a")]
    if crumbs:
      crumbs = [c for c in crumbs if c]
      category_path = " > ".join(crumbs[-5:])

    # brand & size (DOM tables)
    b2, s2 = parse_brand_and_size(soup, name or "")
    brand = brand or b2
    size_text = size_text or s2

    # window globals sometimes carry ids/prices
    for glb in ["Storefront","CART_CONFIG","__NUXT__","__NEXT_DATA__","APP_STATE","dataLayer"]:
      try:
        data = page.evaluate(f"window['{glb}']")
        if data:
          got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS, "price","currency","brand","name" })
          ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
          sku = sku or got.get("sku") or got.get("mpn") or got.get("code") or got.get("id")
          if not price and ("price" in got):        price = got.get("price")
          if not currency and ("currency" in got):  currency = got.get("currency")
          if not brand and ("brand" in got):        brand = got.get("brand")
          if not name and ("name" in got):          name = got.get("name")
      except Exception:
        pass

    if not ean:
      e2 = parse_visible_for_ean(soup)
      if e2: ean = e2

    if not price:
      price, currency = parse_price_from_dom_or_meta(soup)

  except PWTimeout:
    name = name or ""
  finally:
    ctx.close(); browser.close()

  ext_id = extract_ext_id(url) or ext_id_from_attr

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
    "price": (str(price) if price is not None else "").strip(),
    "currency": (currency or "").strip(),
    "image_url": (image_url or "").strip(),
    "category_path": (category_path or "").strip(),
    "category_leaf": category_path.split(" > ")[-1] if category_path else "",
    "source_url": url.split("?")[0],
  }

# ------------------------------ DB helpers ----------------------------------
def _db_connect():
  dburl = os.getenv("DATABASE_URL")
  if not dburl:
    return None, None
  # parse
  u = urlparse(dburl)
  user = u.username
  password = u.password
  host = u.hostname or "localhost"
  port = int(u.port or 5432)
  database = (u.path or "/postgres").lstrip("/")
  # prefer pg8000; fallback psycopg2
  try:
    import pg8000.dbapi as pg8000
    conn = pg8000.connect(user=user, password=password, host=host, port=port, database=database)
    return "pg8000", conn
  except Exception as e_pg:
    try:
      import psycopg2
      conn = psycopg2.connect(user=user, password=password, host=host, port=port, dbname=database, connect_timeout=10)
      return "psycopg2", conn
    except Exception as e_psy:
      print(f"[db] connect failed: pg8000={e_pg}; psycopg2={e_psy}", file=sys.stderr)
      return None, None

def maybe_upsert_db(rows: List[Dict[str,str]], table: str) -> int:
  if not rows:
    return 0
  driver, conn = _db_connect()
  if not conn:
    return 0
  cols = [
    "store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw",
    "name","size_text","brand","manufacturer","price","currency",
    "image_url","category_path","category_leaf","source_url"
  ]
  placeholders = "(" + ",".join(["%s"]*len(cols)) + ")"
  if driver == "pg8000":
    placeholders = "(" + ",".join(["%s"]*len(cols)) + ")"
    on_conflict = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in ("ext_id","store_chain","store_name","store_channel")])
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES {placeholders} " \
          f"ON CONFLICT (ext_id) DO UPDATE SET {on_conflict}"
  else:
    # psycopg2 uses %s as well
    on_conflict = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in ("ext_id","store_chain","store_name","store_channel")])
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES {placeholders} " \
          f"ON CONFLICT (ext_id) DO UPDATE SET {on_conflict}"

  done = 0
  try:
    cur = conn.cursor()
    for r in rows:
      vals = [r.get(c,"") for c in cols]
      cur.execute(sql, vals)
      done += 1
    conn.commit()
    try: cur.close(); conn.close()
    except Exception: pass
  except Exception as e:
    print(f"[db] upsert failed: {type(e).__name__}: {e}", file=sys.stderr)
    try: conn.rollback()
    except Exception: pass
    try: conn.close()
    except Exception: pass
  return done

# ---------------------------------- main ------------------------------------
def main():
  ap = argparse.ArgumentParser(description="Rimi.ee category crawler → CSV (+optional DB upsert)")
  gsrc = ap.add_mutually_exclusive_group(required=True)
  gsrc.add_argument("--category", help="Start category URL (e.g., https://www.rimi.ee/epood/ee/tooted/leivad-saiad-kondiitritooted)")
  gsrc.add_argument("--from-file", help="Text file with category URLs (one per line)")
  ap.add_argument("--page-limit", type=int, default=0, help="Max paginated pages per category (0 = no limit)")
  ap.add_argument("--headless", type=int, default=1, help="Run browser headless (1) or headed (0)")
  ap.add_argument("--req-delay", type=float, default=0.4, help="Delay between UI actions (seconds)")
  ap.add_argument("--output", default="rimi_products.csv", help="CSV output file")
  ap.add_argument("--upsert", type=int, default=0, help="If 1, upsert into Postgres DATABASE_URL")
  ap.add_argument("--table", default="staging_rimi_products", help="Destination table for upsert")
  args = ap.parse_args()

  # prepare seeds
  seeds: List[str] = []
  if args.category:
    seeds = [args.category.strip()]
  else:
    if not os.path.exists(args.from_file):
      print(f"[err] seed file not found: {args.from_file}", file=sys.stderr); sys.exit(2)
    with open(args.from_file, "r", encoding="utf-8") as f:
      for ln in f:
        ln = (ln or "").strip()
        if ln and not ln.startswith("#"):
          seeds.append(ln)
  if not seeds:
    print("[err] no seeds", file=sys.stderr); sys.exit(2)

  os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
  rows_out: List[Dict[str,str]] = []

  with sync_playwright() as pw:
    # 1) crawl all PDP links
    all_pdps: List[str] = []
    for si, seed in enumerate(seeds, 1):
      print(f"[rimi] discovering: {seed}")
      try:
        pdps = crawl_category(
          pw, seed, page_limit=args.page_limit,
          headless=bool(args.headless), req_delay=args.req_delay
        )
        print(f"[rimi]   → found {len(pdps)} PDP links")
        all_pdps.extend(pdps)
      except Exception as e:
        print(f"[rimi]   crawl failed: {type(e).__name__}: {e}", file=sys.stderr)

    # dedupe PDPs preserving order
    seen, pdp_urls = set(), []
    for u in all_pdps:
      if u and u not in seen:
        seen.add(u); pdp_urls.append(u)

    # 2) parse PDPs
    print(f"[rimi] parsing {len(pdp_urls)} PDPs…")
    for i, url in enumerate(pdp_urls, 1):
      try:
        row = parse_pdp(pw, url, headless=bool(args.headless), req_delay=args.req_delay)
        if row.get("ext_id") and row.get("name") and row.get("price"):
          rows_out.append(row)
      except Exception as e:
        print(f"[rimi]   PDP fail {url}: {type(e).__name__}: {e}", file=sys.stderr)
      if (i % 25) == 0:
        print(f"[rimi]   parsed {i}/{len(pdp_urls)}")

  # 3) write CSV
  fieldnames = [
    "store_chain","store_name","store_channel","ext_id","ean_raw","sku_raw","name",
    "size_text","brand","manufacturer","price","currency","image_url",
    "category_path","category_leaf","source_url"
  ]
  with open(args.output, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in rows_out:
      w.writerow({k: r.get(k,"") for k in fieldnames})
  print(f"[rimi] wrote {len(rows_out)} rows → {args.output}")

  # 4) optional DB upsert
  if args.upsert:
    n = maybe_upsert_db(rows_out, args.table)
    print(f"[rimi] upserted {n} rows into {args.table}")

if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    pass

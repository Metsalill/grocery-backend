#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rimi.ee (Rimi ePood) category crawler → PDP extractor → CSV/DB friendly

Key bits:
- Strong brand/manufacturer/size extraction (JSON-LD, meta, spec tables, dl/dt/dd, generic "Key: Value")
- DOM-side brand/manufacturer extractor (post-hydration; catches "Kaubamärk" / "Brand" and "Tootja" / "Producer")
- EAN normalization (accepts 8/12/13/14 → normalizes to 13 when possible)
- Robust price parsing (JSON-LD, meta, visible text)
- Stable/fast (blocks heavy 3rd-party, auto-accepts overlays)
- Reuses a single Chromium page for all PDPs
- Supports --only-ext-file to crawl *only* the ext_ids you feed in

CSV columns written:
  store_chain, store_name, store_channel,
  ext_id, ean_raw, sku_raw, name, size_text, brand, manufacturer,
  price, currency, image_url, category_path, category_leaf, source_url
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

# ------------------------------- regexes -------------------------------------

EAN13_RE = re.compile(r"\b\d{13}\b")
EAN_LABEL_RE = re.compile(r"\b(ean|gtin|gtin13|barcode|triipkood|ribakood)\b", re.I)
MONEY_RE = re.compile(r"(\d{1,5}(?:[.,]\d{1,2}|\s?\d{2})?)\s*€")

SKU_KEYS  = {"sku","mpn","itemNumber","productCode","code","id","itemid"}
EAN_KEYS  = {"ean","ean13","gtin","gtin13","barcode"}
BRAND_KEYS = {"brand","manufacturer","producer","tootja"}
PRICE_KEYS = {"price","currentprice","priceamount","unitprice","value"}
CURR_KEYS  = {"currency","pricecurrency","currencycode","curr"}

# ------------------------------- utils ---------------------------------------

def norm_price_str(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    # "3 99" → "3.99"
    if " " in s and s.replace(" ", "").isdigit() and len(s.replace(" ", "")) >= 3:
        digits = s.replace(" ", "")
        s = f"{digits[:-2]}.{digits[-2:]}"
    return s.replace(",", ".")

def deep_find_kv(obj: Any, keys: set) -> Dict[str, str]:
    out: Dict[str, str] = {}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str, int, float)):
                    out[lk] = str(v)
                # nested brand/manufacturer with {name}
                if lk == "brand":
                    if isinstance(v, dict) and "name" in v:
                        out["brand"] = str(v.get("name") or "")
                if lk == "manufacturer":
                    if isinstance(v, dict) and "name" in v:
                        out["manufacturer"] = str(v.get("name") or "")
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)
    walk(obj)
    return out

def normalize_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.split("?")[0].split("#")[0]
    return href if href.startswith("http") else urljoin(BASE, href)

def canonical_url(page) -> Optional[str]:
    try:
        href = page.evaluate("() => document.querySelector('link[rel=canonical]')?.href || null")
        if href:
            return normalize_href(href)
    except Exception:
        pass
    try:
        return normalize_href(page.url)
    except Exception:
        return None

def auto_accept_overlays(page) -> None:
    labels = [
        r"Nõustun", r"Nõustu", r"Accept", r"Allow all", r"OK", r"Selge",
        r"Jätka", r"Vali hiljem", r"Continue", r"Close", r"Sulge",
        r"Vali pood", r"Vali teenus", r"Telli koju", r"Vali kauplus",
        r"Vali aeg", r"Näita kõiki tooteid", r"Kuva tooted", r"Kuva kõik tooted",
    ]
    for lab in labels:
        try:
            page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=800)
            page.wait_for_timeout(120)
        except Exception:
            pass

def wait_for_hydration(page, timeout_ms: int = 15000) -> None:
    # Wait for price OR spec rows to appear; Rimi fills spec after a few ticks.
    try:
        page.wait_for_function(
            """() => {
                const hasH1 = !!document.querySelector('h1');
                const hasPrice = !!document.querySelector('[itemprop="price"], [data-test*="price"]');
                const hasSpec = !!document.querySelector('table, dl, .product-attributes__row, .product-details__row');
                const hasBrandCell = [...document.querySelectorAll('th,dt')].some(e => /kaubam[aä]rk|br[äa]nd/i.test(e.textContent||''));
                const hasMfrCell = [...document.querySelectorAll('th,dt')].some(e => /tootja|manufacturer|producer/i.test(e.textContent||''));
                return hasH1 && (hasPrice || hasSpec || hasBrandCell || hasMfrCell);
            }""",
            timeout=timeout_ms
        )
    except Exception:
        pass

# ---------- Brand/manufacturer sanitization ----------

_BAD_BRAND_TOKENS = [
    "vali", "tarne", "tarneviis", "ostukorv", "add to cart", "lisa ostukorvi",
    "book delivery", "delivery time", "accept", "cookie", "kampaania", "campaign",
    "logi", "login", "registreeru", "close", "sulge", "continue"
]

def _has_letter(s: str) -> bool:
    return bool(re.search(r"[A-Za-zÄÖÜÕäöüõŠšŽž]", s or ""))

def clean_brand(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    low = s.lower()
    if ":" in s or "\n" in s or len(s) > 50 or len(s) < 2:
        return ""
    if not _has_letter(s):
        return ""
    if any(tok in low for tok in _BAD_BRAND_TOKENS):
        return ""
    return s

def clean_manufacturer(s: str) -> str:
    # Same gate as brand, but allow longer legal names
    s = (s or "").strip()
    if not s:
        return ""
    low = s.lower()
    if ":" in s or "\n" in s or len(s) > 80 or len(s) < 2:
        return ""
    if not _has_letter(s):
        return ""
    if any(tok in low for tok in _BAD_BRAND_TOKENS):
        return ""
    return s

# ---------- EAN helpers ----------

DIGITS_ONLY = re.compile(r"\D+")

def _digits(s: str) -> str:
    return DIGITS_ONLY.sub("", s or "")

def _valid_ean13(code: str) -> bool:
    if not re.fullmatch(r"\d{13}", code or ""):
        return False
    s_odd  = sum(int(code[i]) for i in range(0, 12, 2))
    s_even = sum(int(code[i]) * 3 for i in range(1, 12, 2))
    chk = (10 - ((s_odd + s_even) % 10)) % 10
    return chk == int(code[-1])

def normalize_ean_digits(e: str) -> str:
    d = _digits(e)
    if len(d) == 13 and _valid_ean13(d):
        return d
    if len(d) == 14 and d[0] in ("0","1") and _valid_ean13(d[1:]):
        return d[1:]
    if len(d) == 12 and _valid_ean13("0" + d):
        return "0" + d
    if len(d) == 8:
        return d  # keep as-is; downstream may map EAN-8
    return d  # keep raw digits for visibility

# ----------------------------- parsing helpers --------------------------------

SIZE_IN_NAME_RE = re.compile(
    r'(\d+\s*[×x]\s*\d+[.,]?\d*\s?(?:g|kg|ml|l|tk)|\d+[.,]?\d*\s?(?:g|kg|ml|l|tk))\b',
    re.I
)

def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    return (s
            .replace("ä", "a").replace("ö", "o").replace("õ", "o").replace("ü", "u")
            .replace("š", "s").replace("ž", "z"))

def parse_brand_mfr_size(soup: BeautifulSoup, name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    brand = mfr = size_text = None

    def set_brand(v: str):
        nonlocal brand; v = (v or "").strip()
        if v and not brand: brand = v

    def set_mfr(v: str):
        nonlocal mfr; v = (v or "").strip()
        if v and not mfr: mfr = v

    def set_size(v: str):
        nonlocal size_text; v = (v or "").strip()
        if v and not size_text: size_text = v

    # 1) table rows
    for row in soup.select("table tr"):
        th, td = row.find("th"), row.find("td")
        if not th or not td:
            continue
        key = _norm_key(th.get_text(" ", strip=True))
        val = td.get_text(" ", strip=True)
        if key in ("kaubamark","brand","brand:","brand name","brandname","brand/kaubamark","bränd","brand/kaubamärk"):
            set_brand(val)
        elif key in ("tootja","manufacturer","valmistaja","producer"):
            set_mfr(val)
        elif any(k in key for k in ("kogus","netokogus","maht","pakend","neto","suurus","mahtuvus")):
            set_size(val)

    # 2) dl/dt/dd pairs
    for dl in soup.select("dl"):
        dts, dds = dl.find_all("dt"), dl.find_all("dd")
        for i in range(min(len(dts), len(dds))):
            key = _norm_key(dts[i].get_text(" ", strip=True))
            val = dds[i].get_text(" ", strip=True)
            if key in ("kaubamark","brand","bränd"): set_brand(val)
            elif key in ("tootja","manufacturer","valmistaja","producer"): set_mfr(val)
            elif any(k in key for k in ("kogus","netokogus","maht","pakend","neto","suurus","mahtuvus")): set_size(val)

    # 3) generic “Key: Value” rows
    for el in soup.select(".product-attributes__row, .product-details__row, .key-value, .MuiGrid-root, li, div"):
        t = (el.get_text(" ", strip=True) or "")
        if ":" not in t or len(t) > 200:
            continue
        k, v = t.split(":", 1)
        key = _norm_key(k); val = v.strip()
        if key in ("kaubamark","brand","bränd"): set_brand(val)
        elif key in ("tootja","manufacturer","valmistaja","producer"): set_mfr(val)
        elif any(k in key for k in ("kogus","netokogus","maht","pakend","neto","suurus","mahtuvus")): set_size(val)

    # 4) size from name
    if not size_text and name:
        m = SIZE_IN_NAME_RE.search(name)
        if m: size_text = m.group(1).replace("L", "l")

    return brand, mfr, size_text

def parse_price_from_dom_or_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for sel in [
        'meta[itemprop="price"]',
        'meta[property="product:price:amount"]',
        'meta[property="og:price:amount"]',
    ]:
        for tag in soup.select(sel):
            val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
            if val:
                return norm_price_str(val), "EUR"

    tag = soup.find(attrs={"itemprop": "price"})
    if tag:
        val = (tag.get("content") or tag.get_text(strip=True) or "").strip()
        if val:
            return norm_price_str(val), "EUR"

    m = MONEY_RE.search(soup.get_text(" ", strip=True))
    if m:
        return norm_price_str(m.group(1)), "EUR"
    return None, None

def extract_ext_id(url: str) -> str:
    try:
        parts = urlparse(url).path.rstrip("/").split("/")
        if "p" in parts:
            i = parts.index("p"); return parts[i+1]
    except Exception:
        pass
    return ""

def parse_jsonld_for_product_and_breadcrumbs_and_brand(soup: BeautifulSoup) -> Tuple[Dict[str,Any], List[str], Optional[str], Optional[str]]:
    """returns (flat_product, breadcrumbs, brand, manufacturer)"""
    flat: Dict[str, Any] = {}
    crumbs: List[str] = []
    brand = None
    manufacturer = None

    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.text)
        except Exception:
            continue
        seq = data if isinstance(data, list) else [data]
        for d in seq:
            at = d.get("@type")
            at_list = at if isinstance(at, list) else [at]
            if isinstance(d, dict) and ("Product" in at_list):
                offers = d.get("offers")
                if isinstance(offers, dict):
                    if "price" in offers: flat["price"] = offers.get("price")
                    if "priceCurrency" in offers: flat["currency"] = offers.get("priceCurrency")
                    if not brand:
                        bobj = offers.get("seller") or {}
                        if isinstance(bobj, dict) and bobj.get("name"):
                            brand = str(bobj["name"]).strip()
                elif isinstance(offers, list) and offers:
                    of0 = offers[0]
                    if isinstance(of0, dict):
                        if "price" in of0: flat["price"] = of0.get("price")
                        if "priceCurrency" in of0: flat["currency"] = of0.get("priceCurrency")
                for k in ("gtin13","gtin","ean","ean13","barcode","sku","mpn"):
                    if k in d and d.get(k):
                        flat[k] = d.get(k)
                # brand / manufacturer in jsonld
                if "brand" in d and not brand:
                    v = d.get("brand")
                    if isinstance(v, dict) and v.get("name"):
                        brand = str(v["name"])
                    elif isinstance(v, str):
                        brand = v
                if "manufacturer" in d and not manufacturer:
                    v = d.get("manufacturer")
                    if isinstance(v, dict) and v.get("name"):
                        manufacturer = str(v["name"])
                    elif isinstance(v, str):
                        manufacturer = v
            if isinstance(d, dict) and ("BreadcrumbList" in at_list):
                try:
                    items = d.get("itemListElement") or []
                    names = []
                    for it in items:
                        if isinstance(it, dict):
                            t = it.get("name") or (it.get("item") or {}).get("name")
                            if not t and isinstance(it.get("item"), str):
                                t = it.get("item").split("/")[-1]
                            if t:
                                names.append(str(t).strip())
                    if names:
                        crumbs = names
                except Exception:
                    pass
    return flat, crumbs, (brand.strip() if brand else None), (manufacturer.strip() if manufacturer else None)

def parse_visible_for_ean(soup: BeautifulSoup) -> Optional[str]:
    for el in soup.find_all(string=EAN_LABEL_RE):
        seg = el.parent.get_text(" ", strip=True) if el and el.parent else str(el)
        m = EAN13_RE.search(seg)
        if m: return m.group(0)
    m = EAN13_RE.search(soup.get_text(" ", strip=True))
    return m.group(0) if m else None

# --------- DOM (hydrated) brand/manufacturer extractor (Rimi-specific) --------

def extract_brand_mfr_dom(page) -> Tuple[str, str]:
    """
    Runs inside the live DOM (after hydration). Looks for:
      • table/dl spec rows: Kaubamärk/Brand, Tootja/Manufacturer/Producer
      • generic "Key: Value"
      • helper line '… kaubamärgilt <a>Rimi</a>'
    Returns (brand, manufacturer) — either may be "".
    """
    try:
        # Ensure the spec tab is open/visible.
        for label in ("Toote andmed", "Tooteinfo", "Tooteinfo", "Toote andmed"):
            try:
                page.get_by_role("tab", name=re.compile(label, re.I)).click(timeout=400)
            except Exception:
                try:
                    page.get_by_role("button", name=re.compile(label, re.I)).click(timeout=400)
                except Exception:
                    pass
        page.mouse.wheel(0, 1200)
        page.wait_for_timeout(200)

        got = page.evaluate("""
        () => {
          const pick = (s) => (s || '').replace(/\\s+/g,' ').trim();
          const norm = (s) => pick(s)
            .toLowerCase()
            .normalize('NFD').replace(/[\\u0300-\\u036f]/g,'')
            .replaceAll('ä','a').replaceAll('ö','o').replaceAll('õ','o').replaceAll('ü','u')
            .replaceAll('š','s').replaceAll('ž','z');

          let brand = '', manufacturer = '';

          // Helper to read the cell right of a TH/DT
          const readSibling = (n) => {
            if (!n) return '';
            let sib = n.nextElementSibling;
            if (sib) return pick(sib.textContent);
            // table layout variant
            if (n.parentElement) {
              const tds = n.parentElement.querySelectorAll('td');
              if (tds && tds.length) return pick(tds[0].textContent);
            }
            return '';
          };

          // 1) exact 'Kaubamärk' / 'Tootja' in th/dt
          document.querySelectorAll('th, dt').forEach(n => {
            const k = norm(n.textContent);
            if (!brand && /(kaubamark|brand|br[aä]nd)/.test(k))  brand = readSibling(n);
            if (!manufacturer && /(tootja|manufacturer|producer|valmistaja)/.test(k)) manufacturer = readSibling(n);
          });

          // 2) generic 'Key: Value'
          if (!brand || !manufacturer) {
            const nodes = Array.from(document.querySelectorAll('.product-attributes__row, .product-details__row, .key-value, .MuiGrid-root, li, div, p, span'))
              .slice(0, 1500);
            for (const n of nodes){
              const t = pick(n.textContent);
              if (!t || t.length > 250 || !t.includes(':')) continue;
              const idx = t.indexOf(':');
              const k = norm(t.slice(0, idx));
              const v = pick(t.slice(idx+1));
              if (!brand && /(kaubamark|brand|br[aä]nd)/.test(k)) brand = v;
              if (!manufacturer && /(tootja|manufacturer|producer|valmistaja)/.test(k)) manufacturer = v;
              if (brand && manufacturer) break;
            }
          }

          // 3) header helper: "... kaubamärgilt <a>Rimi</a>"
          if (!brand) {
            const host = Array.from(document.querySelectorAll('div, p, section')).find(
              el => /kaubam[aä]rgilt/i.test(el.textContent || '')
            );
            if (host) {
              const a = host.querySelector('a');
              if (a && pick(a.textContent).length > 1) brand = pick(a.textContent);
            }
          }

          return { brand: pick(brand), manufacturer: pick(manufacturer) };
        }
        """)
        return (got.get("brand") or "").strip(), (got.get("manufacturer") or "").strip()
    except Exception:
        return "", ""

# ---------------------------- collectors --------------------------------------

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
        "nav[aria-label='categories'] a[href^='/epood/ee/tooted/']",
        "a[href^='/epood/ee/tooted/']:has(h2), a[href^='/epood/ee/tooted/']:has(h3)",
        ".category-card a[href^='/epood/ee/tooted/']",
        ".category, .subcategory a[href^='/epood/ee/tooted/']",
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

# ---------------------------- crawler -----------------------------------------

def crawl_category(pw, cat_url: str, page_limit: int, headless: bool, req_delay: float) -> List[str]:
    browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
    ctx = browser.new_context(
        locale="et-EE",
        viewport={"width":1440, "height":900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124 Safari/537.36"),
    )

    # Block heavy 3rd-party stuff to speed up
    BLOCK = [
        "googletagmanager.com","google-analytics.com","doubleclick.net",
        "facebook.net","hotjar.com","newrelic.com","cookiebot.com","demdex.net","adobedtm.com",
        "nr-data.net","js-agent.newrelic.com","typekit.net","use.typekit.net"
    ]
    def router(route, request):
        host = urlparse(request.url).netloc.lower()
        if any(host.endswith(d) for d in BLOCK):
            return route.abort()
        if request.resource_type in {"image","font","media","stylesheet","websocket","manifest"}:
            return route.abort()
        return route.continue_()
    ctx.route("**/*", router)

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

# --------------------------- PDP parser (reused page) -------------------------

def parse_pdp_with_page(page, url: str, req_delay: float) -> Dict[str,str]:
    name = brand = manufacturer = size_text = image_url = ""
    ean = sku = price = currency = None
    category_path = ""
    try:
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        auto_accept_overlays(page)
        wait_for_hydration(page)
        # Open the spec tab explicitly and give SPA time to render rows
        try:
            page.get_by_role("tab", name=re.compile(r"Toote (andmed|info)", re.I)).click(timeout=700)
        except Exception:
            try:
                page.get_by_role("button", name=re.compile(r"Toote (andmed|info)", re.I)).click(timeout=700)
            except Exception:
                pass
        page.wait_for_timeout(int(max(req_delay, 0.4)*1000))

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        # title
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        # image
        ogimg = soup.find("meta", {"property":"og:image"})
        if ogimg and ogimg.get("content"):
            image_url = normalize_href(ogimg.get("content")) or ""
        else:
            img = soup.find("img")
            if img:
                image_url = normalize_href(img.get("src") or img.get("data-src") or "") or ""

        # JSON-LD
        flat_ld, crumbs_ld, brand_ld, manufacturer_ld = parse_jsonld_for_product_and_breadcrumbs_and_brand(soup)
        if flat_ld.get("price") and not price:
            price = norm_price_str(str(flat_ld.get("price")))
            currency = currency or (flat_ld.get("currency") or "EUR")
        for k in ("gtin13","ean","ean13","barcode","gtin"):
            if not ean and flat_ld.get(k):
                ean = str(flat_ld.get(k))
        for k in ("sku","mpn"):
            if not sku and flat_ld.get(k):
                sku = str(flat_ld.get(k))
        brand = brand or brand_ld or ""
        manufacturer = manufacturer or manufacturer_ld or ""

        # breadcrumbs
        crumbs_dom = [a.get_text(strip=True) for a in soup.select(
            "nav[aria-label='breadcrumb'] a, .breadcrumbs a, .breadcrumb a, ol.breadcrumb a, nav.breadcrumbs a"
        ) if a.get_text(strip=True)]
        crumbs = crumbs_dom or crumbs_ld
        if crumbs:
            crumbs = [c for c in crumbs if c]
            category_path = " > ".join(crumbs[-5:])

        # spec: brand + manufacturer + size (HTML snapshot)
        b2, m2, s2 = parse_brand_mfr_size(soup, name or "")
        brand = brand or (b2 or "")
        manufacturer = manufacturer or (m2 or "")
        size_text = size_text or (s2 or "")

        # DOM-hydrated brand/manufacturer (Rimi-specific; runs after SPA loads)
        if not brand or not manufacturer:
            b_dom, m_dom = extract_brand_mfr_dom(page)
            if not brand and b_dom: brand = b_dom
            if not manufacturer and m_dom: manufacturer = m_dom

        # microdata hints
        if not ean or not sku:
            for it in ("gtin13","gtin","ean","ean13","barcode","sku","mpn"):
                meta = soup.find(attrs={"itemprop": it})
                if meta:
                    val = (meta.get("content") or meta.get_text(strip=True))
                    if not val: continue
                    if it in ("gtin13","gtin","ean","ean13","barcode") and not ean:
                        ean = val
                    if it in ("sku","mpn") and not sku:
                        sku = val

        # meta brand
        if not brand:
            mbrand = soup.find("meta", {"property":"product:brand"})
            if mbrand and mbrand.get("content"):
                brand = mbrand["content"].strip()

        # price fallback
        if not price:
            p, c = parse_price_from_dom_or_meta(soup)
            price, currency = p or price, c or currency

        # JS globals (last resort)
        if not (brand and manufacturer):
            for glb in [
                "__NUXT__", "__NEXT_DATA__", "APP_STATE", "dataLayer",
                "Storefront", "__APOLLO_STATE__", "APOLLO_STATE",
                "apolloState", "__INITIAL_STATE__", "__PRELOADED_STATE__", "__STATE__"
            ]:
                try:
                    data = page.evaluate(f"window['{glb}']")
                except Exception:
                    data = None
                if not data:
                    continue
                got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS, *PRICE_KEYS, *CURR_KEYS, *BRAND_KEYS })
                if not ean:
                    for k in ("gtin13","ean","ean13","barcode","gtin"):
                        if got.get(k):
                            ean = got.get(k); break
                if not sku:
                    for k in ("sku","mpn","code","id"):
                        if got.get(k):
                            sku = got.get(k); break
                if not price:
                    for k in ("price","currentprice","priceamount","value","unitprice"):
                        if got.get(k):
                            price = norm_price_str(got.get(k)); break
                if not currency:
                    for k in ("currency","pricecurrency","currencycode","curr"):
                        if got.get(k):
                            currency = got.get(k); break
                if not brand and got.get("brand"):
                    cb = clean_brand(got.get("brand"))
                    if cb:
                        brand = cb
                if not manufacturer and got.get("manufacturer"):
                    cm = clean_manufacturer(got.get("manufacturer"))
                    if cm:
                        manufacturer = cm

        # visible EAN last resort
        if not ean:
            e2 = parse_visible_for_ean(soup)
            if e2: ean = e2

        if not currency and price:
            currency = "EUR"

    except PWTimeout:
        name = name or ""

    # Final cleanup / normalization
    if ean:
        ean = normalize_ean_digits(ean)
    brand = clean_brand(brand)
    manufacturer = clean_manufacturer(manufacturer)

    ext_id = extract_ext_id(url)
    src_url = canonical_url(page) or url.split("?")[0]

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
        "manufacturer": (manufacturer or "").strip(),
        "price": (str(price) if price is not None else "").strip(),
        "currency": (currency or "").strip(),
        "image_url": (image_url or "").strip(),
        "category_path": (category_path or "").strip(),
        "category_leaf": category_path.split(" > ")[-1] if category_path else "",
        "source_url": src_url,
    }

# ------------------------------- IO -------------------------------------------

def read_categories(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

def _read_id_file(path: Optional[str]) -> tuple[set[str], set[str]]:
    urls: set[str] = set()
    ids: set[str] = set()
    if not path or not os.path.exists(path):
        return urls, ids
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            if s.startswith("http"):
                u = s.split("?")[0].split("#")[0]
                urls.add(u)
                xid = extract_ext_id(u)
                if xid:
                    ids.add(xid)
            else:
                ids.add(s)
    return urls, ids

def read_skip_file(path: Optional[str]) -> tuple[set[str], set[str]]:
    return _read_id_file(path)

def read_only_file(path: Optional[str]) -> tuple[set[str], set[str]]:
    return _read_id_file(path)

def write_csv(rows: List[Dict[str,str]], out_path: str) -> None:
    fields = [
        "store_chain","store_name","store_channel",
        "ext_id","ean_raw","sku_raw","name","size_text","brand","manufacturer",
        "price","currency","image_url","category_path","category_leaf","source_url",
    ]
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    new_file = not os.path.exists(out_path)
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,"") for k in fields})

# -------------------------------- main ----------------------------------------

def main():
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--cats-file", required=True, help="File with category URLs (one per line)")
    ap.add_argument("--page-limit", default="0")
    ap.add_argument("--max-products", default="0")
    ap.add_argument("--headless", default="1")
    ap.add_argument("--req-delay", default="0.5")
    ap.add_argument("--output-csv", default=os.environ.get("OUTPUT_CSV","data/rimi_products.csv"))
    ap.add_argument("--skip-ext-file", default=os.environ.get("SKIP_EXT_FILE",""))
    ap.add_argument("--only-ext-file", default=os.environ.get("ONLY_EXT_FILE",""))  # NEW
    args = ap.parse_args()

    page_limit   = int(args.page_limit or "0")
    max_products = int(args.max_products or "0")
    headless     = (str(args.headless or "1") != "0")
    req_delay    = float(args.req_delay or "0.5")
    cats         = read_categories(args.cats_file)

    skip_urls, skip_ext = read_skip_file(args.skip_ext_file)
    only_urls, only_ext = read_only_file(args.only_ext_file)

    all_pdps: List[str] = []
    with sync_playwright() as pw:
        # 1) collect PDP URLs from categories
        for cat in cats:
            try:
                print(f"[rimi] {cat}")
                pdps = crawl_category(pw, cat, page_limit, headless, req_delay)
                all_pdps.extend(pdps)
                if max_products and len(all_pdps) >= max_products:
                    break
            except Exception as e:
                print(f"[rimi] category error: {cat} → {e}", file=sys.stderr)

        # dedupe keep order
        seen, q = set(), []
        for u in all_pdps:
            if u not in seen:
                seen.add(u); q.append(u)

        # 2a) ONLY filter (if provided)
        if only_urls or only_ext:
            q_only = []
            for u in q:
                xid = extract_ext_id(u)
                if (u in only_urls) or (xid and xid in only_ext):
                    q_only.append(u)
            print(f"[rimi] ONLY filter active: {len(q_only)} URLs retained (of {len(q)})")
            q = q_only

        # 2b) SKIP filter
        if skip_urls or skip_ext:
            q2 = []
            skipped = 0
            for u in q:
                if (u in skip_urls) or (extract_ext_id(u) in skip_ext):
                    skipped += 1
                    continue
                q2.append(u)
            print(f"[rimi] skip filter: {skipped} URLs skipped (already priced/complete).")
            q = q2

        # 3) single browser/context/page for all PDPs
        browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
        ctx = browser.new_context(
            locale="et-EE",
            viewport={"width":1440,"height":900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
        )
        page = ctx.new_page()

        rows, total = [], 0
        for i, url in enumerate(q, 1):
            try:
                row = parse_pdp_with_page(page, url, req_delay)
                rows.append(row); total += 1
                if len(rows) >= 120:   # batch flush
                    write_csv(rows, args.output_csv); rows = []
            except Exception:
                traceback.print_exc()
            if max_products and total >= max_products:
                break

        if rows:
            write_csv(rows, args.output_csv)

        ctx.close(); browser.close()

    print(f"[rimi] wrote {total} product rows.")

if __name__ == "__main__":
    main()

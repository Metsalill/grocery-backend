#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver category crawler → CSV (staging_selver_products)

Features:
- Collect PDP links from listings and remember which listing page each link came from.
- Two modes:
  1) Direct mode (default): open PDPs directly, with click-through fallback if needed.
  2) CLICK mode (CLICK_PRODUCTS=1): literally click each product card on listings,
     open PDP, extract, then go back (no URL harvesting required).
- CSV columns (downstream-safe): ext_id, name, ean_raw, sku_raw, size_text,
  price, currency, category_path, category_leaf.

Noise & speed fixes:
- Canonical URLs (no /e-selver/) and pagination via ?page=N.
- Tight router (default ON): aborts non-Selver scripts/xhr/fetch/fonts/stylesheet/websocket/manifest/eventsource.
- No 'Upgrade-Insecure-Requests' header.
- Block service workers (context + router kill known SW iframes).
- Wider 3P blocklist.
- Console is quiet by default (opt-in with LOG_CONSOLE).

Run:
  OUTPUT_CSV=data/selver.csv python scripts/selver_crawl_categories_pw.py

Env toggles:
  CLICK_PRODUCTS=0 (default) | 1
  ALLOWLIST_ONLY=1 (default) | 0
  PAGE_LIMIT=0 (no cap)
  USE_ROUTER=1 (default) | 0
  LOG_CONSOLE=0 (default) | warn | all
  REQ_DELAY=0.6 (seconds between steps)
"""

from __future__ import annotations
import os, re, csv, time, json
from typing import Dict, Set, Tuple, List, Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qs, urlencode
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Config
BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.6"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no limit
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

USE_ROUTER = int(os.getenv("USE_ROUTER", "1")) == 1
CLICK_PRODUCTS = int(os.getenv("CLICK_PRODUCTS", "0")) == 1
LOG_CONSOLE = (os.getenv("LOG_CONSOLE", "0") or "0").lower()  # 0|off, warn, all

# Strict allowlist of FOOD roots/leaves (canonical, no /e-selver/)
STRICT_ALLOWLIST = [
    "/puu-ja-koogiviljad",
    "/liha-ja-kalatooted",
    "/piimatooted-munad-void",
    "/juustud",
    "/leivad-saiad-kondiitritooted",
    "/valmistoidud",
    "/kuivained-hommikusoogid-hoidised",
    "/maitseained-ja-puljongid",
    "/maitseained-ja-puljongid/kastmed",
    "/maitseained-ja-puljongid/olid-ja-aadikad",
    "/suupisted-ja-maiustused",
    "/joogid",
    "/sugavkylm",
    "/kulmutatud-toidukaubad",
    "/suurpakendid",
]
ALLOWLIST_ONLY = int(os.getenv("ALLOWLIST_ONLY", "1")) == 1

BANNED_KEYWORDS = {
    "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid",
    "vaikesed-sisustuskaubad","kuunlad","kirja-ja-kontoritarbed",
    "remondi-ja-turvatooted","omblus-ja-kasitootarbed","meisterdamine",
    "ajakirjad","autojuhtimine","kotid","aed-ja-lilled","lemmikloom",
    "sport","pallimangud","jalgrattasoit","ujumine","matkamine",
    "tervisesport","manguasjad","lutid","lapsehooldus","ideed-ja-hooajad",
    "kodumasinad","elektroonika","meelelahutuselektroonika",
    "vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
    "ilu-ja-tervis","kosmeetika","meigitooted","hugieen",
    "loodustooted-ja-toidulisandid",
}

SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

# ---------- Third-party noise to block ----------
BLOCK_HOSTS = {
    # common 3P seen on Selver
    "adobe.com","assets.adobedtm.com","adobedtm.com","demdex.net","omtrdc.net",
    "googletagmanager.com","google-analytics.com","doubleclick.net","facebook.net",
    "cookiebot.com","consent.cookiebot.com","imgct.cookiebot.com","consentcdn.cookiebot.com",
    "use.typekit.net","typekit.net","p.typekit.net",
    "nr-data.net","newrelic.com","js-agent.newrelic.com",
    "pingdom.net","rum-collector.pingdom.net","rum-collector-2.pingdom.net",
    "gstatic.com","cdn.jsdelivr.net","googleadservices.com",
    "hotjar.com","static.hotjar.com",
}
ALLOWED_HOSTS = {"www.selver.ee", "selver.ee"}

NON_PRODUCT_PATH_SNIPPETS = {
    "/e-selver/","/ostukorv","/cart","/checkout","/search","/otsi",
    "/konto","/customer","/login","/logout","/registreeru","/uudised",
    "/tootajad","/kontakt","/tingimused","/privaatsus","/privacy",
    "/kampaania","/kampaaniad","/blogi","/app","/store-locator",
}
NON_PRODUCT_KEYWORDS = {
    "login", "registreeru", "tingimused", "garantii", "pretens", "hinnasilt",
    "jatkusuutlik", "b2b", "privaatsus", "privacy", "kontakt", "uudis",
    "blog", "pood", "poed", "kaart", "arikliend", "karjaar", "karjäär",
}

# ---------------------------------------------------------------------------
# Small utils

def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def guess_size_from_title(title: str) -> str:
    m = SIZE_RE.search(title or "")
    if not m:
        return ""
    num, unit = m.groups()
    return f"{num.replace(',', '.')} {unit.lower()}"

def _strip_eselver_prefix(path: str) -> str:
    return path.replace("/e-selver", "", 1) if path.startswith("/e-selver/") else path

def _clean_abs(href: str) -> Optional[str]:
    if not href:
        return None
    url = urljoin(BASE, href)
    parts = urlsplit(url)
    host = (parts.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS:
        return None
    path = _strip_eselver_prefix(parts.path)
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", ""))

def canonical_from_page(page) -> Optional[str]:
    try:
        href = page.evaluate("""(d=>d.querySelector('link[rel="canonical"]')?.href||null)(document)""")
        if href:
            return _clean_abs(href)
    except Exception:
        pass
    try:
        return _clean_abs(page.url)
    except Exception:
        return None

def _in_allowlist(path: str) -> bool:
    if not STRICT_ALLOWLIST:
        return True
    p = (path or "/").rstrip("/")
    return any(p == root or p.startswith(root + "/") for root in STRICT_ALLOWLIST)

def _is_selver_product_like(url: str) -> bool:
    u = urlparse(url)
    host = (u.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS:
        return False
    path = _strip_eselver_prefix((u.path or "/").lower())
    if path.startswith("/ru/"): return False
    if any(sn in path for sn in NON_PRODUCT_PATH_SNIPPETS): return False
    if any(kw in path for kw in NON_PRODUCT_KEYWORDS): return False
    if re.fullmatch(r"/p/[a-z0-9-]+/?", path): return True
    last = path.rstrip("/").rsplit("/", 1)[-1]
    return re.fullmatch(r"[a-z0-9-]+", last) and ("-" in last) and (
        any(ch.isdigit() for ch in last) or last.count("-") >= 2
    )

def _is_category_like_path(path: str) -> bool:
    p = _strip_eselver_prefix((path or "/").lower())
    if ALLOWLIST_ONLY and STRICT_ALLOWLIST and not _in_allowlist(p): return False
    if "/e-selver/" in p or p.startswith("/ru/"): return False
    if any(bad in p for bad in BANNED_KEYWORDS): return False
    if any(sn in p for sn in NON_PRODUCT_PATH_SNIPPETS): return False
    if _is_selver_product_like(urljoin(BASE, p)): return False
    segs = [s for s in p.strip("/").split("/") if s]
    if len(segs) < 1: return False
    last = segs[-1]
    if any(ch.isdigit() for ch in last): return False
    return any("-" in s for s in segs)

# Extra helpers for EAN/SKU parsing
DIGITS_RE = re.compile(r"\D+")

def _digits(s: str) -> str:
    return DIGITS_RE.sub("", s or "")

def _valid_ean13(code: str) -> bool:
    if not re.fullmatch(r"\d{13}", code): return False
    s_odd  = sum(int(code[i]) for i in range(0, 12, 2))
    s_even = sum(int(code[i]) * 3 for i in range(1, 12, 2))
    chk = (10 - ((s_odd + s_even) % 10)) % 10
    return chk == int(code[-1])

def _pick_ean_from_html(html: str) -> str:
    if not html: return ""
    # Prefer numbers next to clear labels
    label_pat = re.compile(
        r"(?:\b(?:ean|gtin|ribakood|triipkood|barcode)\b)[^0-9]{0,40}(\d{8,14})",
        re.I | re.S,
    )
    cand = [m.group(1) for m in label_pat.finditer(html)]
    cand = list(dict.fromkeys(cand))
    if not cand:
        cand = list(dict.fromkeys(re.findall(r"\b(\d{13})\b", html)))
    for c in cand:
        d = _digits(c)
        if _valid_ean13(d): return d
    for c in cand:
        d = _digits(c)
        if re.fullmatch(r"\d{13}", d): return d
    m8 = re.search(r"\b(\d{8})\b", html)
    return m8.group(1) if m8 else ""

# ---------------------------------------------------------------------------
# Cookies / navigation

def accept_cookies(page):
    for sel in [
        "button:has-text('Nõustu')","button:has-text('Nõustun')",
        "button:has-text('Accept')","button[aria-label*='accept']",
        "button:has-text('Luba kõik')",
    ]:
        try:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_enabled():
                btn.first.click(timeout=2500)
                time.sleep(0.2)
                return
        except Exception:
            pass

def safe_goto(page, url: str, timeout: int = 30000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        accept_cookies(page)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout)
        except Exception:
            pass
        time.sleep(0.6)
        return True
    except Exception as e:
        print(f"[selver] NAV FAIL {url} -> {type(e).__name__}: {e}")
        return False

def _wait_listing_ready(page):
    try:
        for _ in range(12):
            if (page.locator("button:has-text('OSTA')").count() > 0 or
                page.locator("a[href^='/'][href*='-'] img").count() > 0 or
                page.locator(".product, .product-list, .productgrid").count() > 0):
                return
            time.sleep(0.35)
    except Exception:
        pass

def _wait_pdp_ready(page):
    for _ in range(24):
        if page.locator("h1").count() > 0:
            if (page.locator("script[type='application/ld+json']").count() > 0 or
                page.locator("text=Ribakood").count() > 0):
                return
        time.sleep(0.25)

# Expand hidden PDP sections (tabs/accordions) — often holds EAN
def _expand_pdp_details(page):
    selectors = [
        "button:has-text('Lisainfo')",
        "button:has-text('Toote info')",
        "[role='tab']:has-text('Lisainfo')",
        "[role='tab']:has-text('Toote info')",
        "[data-toggle='collapse']",
        "[aria-controls*='detail']",
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el and el.count() > 0 and el.is_enabled():
                el.click(timeout=1200)
                time.sleep(0.15)
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Discovery

def _extract_category_links(page) -> List[str]:
    _wait_listing_ready(page)
    try:
        hrefs = page.evaluate("""
          [...document.querySelectorAll('a[href^="/"]')]
            .map(a => a.getAttribute('href')).filter(Boolean)
        """)
    except Exception:
        hrefs = []
    out, seen = [], set()
    for h in hrefs:
        u = _clean_abs(h)
        if not u: continue
        path = urlparse(u).path
        if ALLOWLIST_ONLY and STRICT_ALLOWLIST and not _in_allowlist(path):
            continue
        if _is_category_like_path(path) and u not in seen:
            seen.add(u); out.append(u)
    return out

def discover_categories(page, start_urls: List[str]) -> List[str]:
    queue = list(dict.fromkeys(start_urls))
    seen_pages = set(queue)
    cats: List[str] = []
    while queue:
        url = queue.pop(0)
        if not safe_goto(page, url): continue
        time.sleep(REQ_DELAY)
        p = urlparse(url).path
        if _is_category_like_path(p) and url not in cats: cats.append(url)
        for u in _extract_category_links(page):
            if u not in seen_pages:
                seen_pages.add(u); queue.append(u)
        if len(cats) > 2000: break
    out, seen = [], set()
    for u in cats:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

# ---------------------------------------------------------------------------
# Listing → product links (pagination with ?page=N)

def _with_page(url: str, n: int) -> str:
    parts = urlsplit(url)
    qs = parse_qs(parts.query)
    qs["page"] = [str(n)]
    query = urlencode({k: v[0] for k, v in qs.items()}, doseq=False)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))

def _max_page_number(page) -> int:
    try:
        maxn = page.evaluate("""
          (() => {
            const ns = [...document.querySelectorAll('a[href*="?page="]')].map(a => {
              try { return parseInt(new URL(a.href).searchParams.get('page')||''); }
              catch { return NaN; }
            }).filter(n => !Number.isNaN(n));
            return ns.length ? Math.max(...ns) : 1;
          })()
        """)
        return int(maxn) if maxn and maxn > 0 else 1
    except Exception:
        return 1

def _extract_product_hrefs(page) -> List[str]:
    try:
        hrefs = page.evaluate("""
          [...document.querySelectorAll('a[href^="/"]')]
            .map(a => a.getAttribute('href'))
            .filter(Boolean)
        """)
    except Exception:
        hrefs = []
    links = []
    for h in hrefs:
        u = _clean_abs(h)
        if u and _is_selver_product_like(u):
            links.append(u)
    return list(dict.fromkeys(links))

def collect_product_links_from_listing(page, seed_url: str) -> Tuple[Set[str], Dict[str, str]]:
    """Returns (unique PDP links, mapping link->this listing page URL)."""
    links: Set[str] = set()
    link2listing: Dict[str, str] = {}

    _wait_listing_ready(page)
    max_pages = _max_page_number(page)
    if PAGE_LIMIT > 0:
        max_pages = min(max_pages, PAGE_LIMIT)

    for n in range(1, max_pages + 1):
        url = seed_url if n == 1 else _with_page(seed_url, n)
        if not safe_goto(page, url): continue
        _wait_listing_ready(page)
        time.sleep(REQ_DELAY)

        page_links = _extract_product_hrefs(page)
        for u in page_links:
            if u not in links:
                links.add(u); link2listing[u] = url

    if links:
        sample = list(sorted(links))[:5]
        print(f"[selver]   harvested {len(links)} PDP links; sample: {sample}")
    else:
        print("[selver]   no PDP links found on listing.")
    return links, link2listing

# ---------------------------------------------------------------------------
# JSON-LD helpers

def jsonld_all(page) -> List[dict]:
    out = []
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(min(12, scripts.count())):
            raw = scripts.nth(i).inner_text()
            try:
                obj = json.loads(raw)
                if isinstance(obj, list):
                    out.extend([x for x in obj if isinstance(x, dict)])
                elif isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
    except Exception:
        pass
    return out

def jsonld_pick_product(blocks: List[dict]) -> dict:
    for b in blocks:
        t = (b.get("@type") or "")
        t_low = t.lower() if isinstance(t, str) else ""
        if "product" in t_low or any(k in b for k in ("gtin13","gtin","sku")):
            return b
    return {}

def jsonld_pick_breadcrumbs(blocks: List[dict]) -> List[str]:
    for b in blocks:
        t = (b.get("@type") or "").lower()
        if t == "breadcrumblist" and "itemListElement" in b:
            try:
                return [el["item"]["name"].strip()
                        for el in b["itemListElement"]
                        if isinstance(el, dict) and "item" in el and "name" in el["item"]]
            except Exception:
                continue
    return []

# ---------------------------------------------------------------------------
# PDP extraction (single source of truth — returns a row dict)

def extract_price(page) -> tuple[float, str]:
    for sel in ["text=/€/","span:has-text('€')","div:has-text('€')"]:
        try:
            node = page.locator(sel).first
            if node and node.count() > 0:
                txt = node.inner_text()
                m = re.search(r"(\d+(?:[.,]\d+)?)\s*€", txt)
                if m: return float(m.group(1).replace(",", ".")), "EUR"
        except Exception:
            pass
    return 0.0, "EUR"

def extract_ean_and_sku(page) -> tuple[str, str]:
    ean, sku = "", ""

    # A) JSON-LD
    try:
        blocks = jsonld_all(page)
        prod = jsonld_pick_product(blocks)
        if prod:
            ean = _digits(str(prod.get("gtin13") or prod.get("gtin") or ""))
            sku = normspace(str(prod.get("sku") or ""))
            if _valid_ean13(ean):
                return ean, sku
    except Exception:
        pass

    # B) Microdata/meta
    try:
        got = page.evaluate("""
        () => {
          const pick = (sel) => {
            const el = document.querySelector(sel);
            if (!el) return null;
            return el.getAttribute('content') || el.textContent || null;
          };
          return {
            gtin13: pick('[itemprop="gtin13"], meta[itemprop="gtin13"]'),
            gtin:   pick('[itemprop="gtin"], meta[itemprop="gtin"]'),
            sku:    pick('[itemprop="sku"], meta[itemprop="sku"], meta[property="product:retailer_item_id"]')
          };
        }
        """)
        if got:
            ean = ean or _digits(got.get("gtin13") or got.get("gtin") or "")
            sku = sku or normspace(got.get("sku") or "")
            if _valid_ean13(ean):
                return ean, sku
    except Exception:
        pass

    # C) Expand possible hidden details and scan HTML
    _expand_pdp_details(page)
    time.sleep(0.05)
    try:
        html = page.content()
        e_dom = _pick_ean_from_html(html)
        if e_dom: ean = ean or e_dom
        if not sku:
            m2 = re.search(r"\bSKU\b\D*([A-Z0-9_-]{3,})", html, re.I)
            if m2: sku = m2.group(1).strip()
    except Exception:
        pass

    # Clean/validate final EAN
    if not _valid_ean13(ean):
        e13 = _digits(ean or "")
        ean = e13 if _valid_ean13(e13) else ""

    return ean, sku

def breadcrumbs_dom(page) -> List[str]:
    for sel in ["nav ol li a","nav.breadcrumbs a","ol.breadcrumbs a"]:
        try:
            items = page.locator(sel)
            if items.count() > 0:
                vals = [normspace(items.nth(i).inner_text()) for i in range(items.count())]
                vals = [v for v in vals if v and v.lower() != "e-selver"]
                if vals: return vals
        except Exception:
            pass
    return []

def _extract_row_from_pdp(page, product_url_hint: Optional[str] = None) -> Optional[dict]:
    """Build a single CSV row from the currently open PDP."""
    _wait_pdp_ready(page)
    _expand_pdp_details(page)  # open accordions/tabs that may hold EAN

    # Canonical/ext_id
    ext_id = canonical_from_page(page) or product_url_hint
    if not ext_id:
        return None

    blocks = jsonld_all(page)
    prod_ld = jsonld_pick_product(blocks)
    crumbs_ld = jsonld_pick_breadcrumbs(blocks)

    # Name
    name = normspace(prod_ld.get("name") or "") if prod_ld else ""
    if not name:
        try:
            name = normspace(page.locator("h1").first.inner_text())
        except Exception:
            name = ""
    if not name:
        return None

    # EAN & SKU
    ean, sku = "", ""
    if prod_ld:
        ean = _digits(str(prod_ld.get("gtin13") or prod_ld.get("gtin") or "")) or ""
        sku = normspace(str(prod_ld.get("sku") or ""))
    if not (ean and sku):
        e2, s2 = extract_ean_and_sku(page)
        ean = ean or e2
        sku = sku or s2

    # Price & currency
    price, currency = 0.0, "EUR"
    if prod_ld and "offers" in prod_ld:
        offers = prod_ld["offers"]
        if isinstance(offers, list) and offers:
            offers = offers[0]
        try:
            price = float(str(offers.get("price")).replace(",", "."))
            currency = offers.get("priceCurrency") or currency
        except Exception:
            pass
    if price == 0.0:
        price, currency = extract_price(page)

    # Breadcrumbs
    crumbs = crumbs_ld or breadcrumbs_dom(page)
    cat_path = " / ".join(crumbs); cat_leaf = crumbs[-1] if crumbs else ""

    size_text = guess_size_from_title(name)

    return {
        "ext_id": ext_id,
        "name": name,
        "ean_raw": ean,
        "sku_raw": sku,
        "size_text": size_text,
        "price": f"{price:.2f}",
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": cat_leaf,
    }

# ---------------------------------------------------------------------------
# Request router (ON by default) — stronger filters

BLOCK_TYPES = {"image", "font", "media", "stylesheet", "websocket", "manifest"}
BLOCK_ACTIVE_TYPES = {"script", "xhr", "fetch", "eventsource"}

def _router(route, request):
    try:
        url = request.url
        host = urlparse(url).netloc.lower()
        rtype = request.resource_type
        method = (getattr(request, "method", None) or "GET").upper()

        # Kill known GTM SW iframes anywhere
        if "service_worker" in url or "sw_iframe" in url:
            return route.abort()

        # Always allow Selver origin navigations / same-origin assets
        if host.endswith("selver.ee"):
            return route.continue_()

        # Kill OPTIONS preflights etc. from foreign origins
        if method == "OPTIONS":
            return route.abort()

        # Block noisy resource types for non-Selver origins
        if rtype in BLOCK_TYPES or rtype in BLOCK_ACTIVE_TYPES:
            return route.abort()

        # Explicit domain blocklist
        if any(host == d or host.endswith("." + d) for d in BLOCK_HOSTS):
            return route.abort()

        return route.continue_()
    except Exception:
        return route.continue_()

# ---------------------------------------------------------------------------
# Click-through helpers

def open_product_via_click(page, listing_url: str, product_url: str) -> bool:
    if not listing_url:
        return False
    if not safe_goto(page, listing_url):
        return False
    _wait_listing_ready(page)
    time.sleep(0.2)

    path = urlparse(product_url).path
    eselver_path = "/e-selver" + path if not path.startswith("/e-selver/") else path
    candidates = [
        f"a[href$='{path}']",
        f"a[href$='{path}/']",
        f"a[href$='{eselver_path}']",
        f"a[href$='{eselver_path}/']",
    ]

    for sel in candidates:
        a = page.locator(sel).first
        try:
            if a.count() > 0 and a.is_visible():
                a.click(timeout=5000)
                for _ in range(24):
                    up = urlparse(page.url).path.lower()
                    if "/p/" in up or up.rstrip("/").endswith(path.rstrip("/").rsplit("/",1)[-1]):
                        break
                    if page.locator("h1").count() > 0:
                        break
                    time.sleep(0.25)
                _wait_pdp_ready(page)
                return True
        except Exception:
            continue
    return False

def collect_write_by_clicking(page, seed_url: str, writer: csv.DictWriter, seen_ext_ids: Set[str]) -> int:
    """Click every product card on every page of a category, write rows, return count."""
    wrote = 0
    if not safe_goto(page, seed_url):
        return 0
    _wait_listing_ready(page)
    max_pages = _max_page_number(page)
    if PAGE_LIMIT > 0:
        max_pages = min(max_pages, PAGE_LIMIT)

    for n in range(1, max_pages + 1):
        url = seed_url if n == 1 else _with_page(seed_url, n)
        if not safe_goto(page, url): continue
        _wait_listing_ready(page)
        time.sleep(REQ_DELAY)

        # Collect unique HREFs but still click (stability)
        try:
            hrefs = page.evaluate("""
              [...document.querySelectorAll('a[href^="/"]:not([href*="#"])')]
                .filter(a => a.closest('article, .product, .product-card, .product-item, li'))
                .map(a => a.getAttribute('href'))
            """)
        except Exception:
            hrefs = []

        cleaned = []
        seen = set()
        for h in hrefs:
            u = _clean_abs(h)
            if not u or not _is_selver_product_like(u): continue
            if u not in seen:
                seen.add(u); cleaned.append(u)

        for href in cleaned:
            path = urlparse(href).path
            eselver_path = "/e-selver" + path if not path.startswith("/e-selver/") else path
            loc = page.locator(
                f"a[href$='{path}'], a[href$='{path}/'], a[href$='{eselver_path}'], a[href$='{eselver_path}/']"
            ).first
            try:
                if loc.count() == 0:
                    continue
                loc.scroll_into_view_if_needed(timeout=3000)
                loc.click(timeout=5000)
                _wait_pdp_ready(page)
                row = _extract_row_from_pdp(page, href)
                if row:
                    ext_id = row["ext_id"]
                    if ext_id not in seen_ext_ids:
                        writer.writerow(row)
                        seen_ext_ids.add(ext_id)
                        wrote += 1
                page.go_back(wait_until="domcontentloaded", timeout=30000)
                _wait_listing_ready(page)
                time.sleep(0.2)
            except Exception:
                # Try to recover to listing
                try:
                    page.go_back(wait_until="domcontentloaded", timeout=15000)
                    _wait_listing_ready(page)
                except Exception:
                    pass
                continue

    return wrote

# ---------------------------------------------------------------------------
# Main crawl

def crawl():
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    dbg_dir = "data/selver_debug"; os.makedirs(dbg_dir, exist_ok=True)

    print(f"[selver] writing CSV -> {OUTPUT}")
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["ext_id","name","ean_raw","sku_raw","size_text","price","currency","category_path","category_leaf"],
        )
        w.writeheader()

        with sync_playwright() as p:
            print("[selver] launching chromium (headless)")
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = browser.new_context(
                locale="et-EE",
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"),
                extra_http_headers={
                    "Accept-Language": "et-EE,et;q=0.9,en-US;q=0.8,en;q=0.7",
                    # DO NOT send 'Upgrade-Insecure-Requests'
                },
                ignore_https_errors=True,
                service_workers="block",  # avoid GTM SW iframes / registrations
            )
            if USE_ROUTER:
                context.route("**/*", _router)

            page = context.new_page()
            page.set_default_navigation_timeout(30000)
            page.set_default_timeout(10000)

            # Console is quiet by default; opt in via LOG_CONSOLE
            if LOG_CONSOLE == "all":
                page.on("console", lambda m: print(f"[pw] {m.type}: {m.text}"))
            elif LOG_CONSOLE == "warn":
                def _warn_err_only(m):
                    if m.type in ("warning","error"):
                        t = (m.text or "").replace("\n"," ")[:800]
                        print(f"[pw] {m.type}: {t}")
                page.on("console", _warn_err_only)

            # ---- seeds
            print("[selver] collecting seeds…")
            seeds: List[str] = [urljoin(BASE, pth) for pth in STRICT_ALLOWLIST]
            if os.path.exists(CATEGORIES_FILE):
                with open(CATEGORIES_FILE, "r", encoding="utf-8") as cf:
                    for ln in cf:
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            u = _clean_abs(ln)
                            if u: seeds.append(u)
            seeds = list(dict.fromkeys(seeds))
            cats = seeds if ALLOWLIST_ONLY else discover_categories(page, seeds)

            print(f"[selver] Categories to crawl: {len(cats)}")
            for cu in cats[:40]:
                print(f"[selver]   {cu}")
            if len(cats) > 40:
                print(f"[selver]   … (+{len(cats)-40} more)")

            rows_written = 0
            seen_ext_ids: Set[str] = set()

            if CLICK_PRODUCTS:
                # --- Click mode: extract rows while clicking product cards
                for ci, cu in enumerate(cats, 1):
                    try:
                        wrote = collect_write_by_clicking(page, cu, w, seen_ext_ids)
                        rows_written += wrote
                        print(f"[selver] {cu} → +{wrote} rows (click mode, total: {rows_written})")
                        if (ci % 1) == 0:
                            f.flush()
                    except Exception:
                        try: page.screenshot(path=f"{dbg_dir}/click_mode_fail_{ci}.png", full_page=True)
                        except Exception: pass
                        continue

            else:
                # --- Direct mode: collect PDP links first, then open PDPs (with click fallback)
                product_urls: Set[str] = set()
                prod2listing: Dict[str, str] = {}
                for ci, cu in enumerate(cats, 1):
                    if not safe_goto(page, cu):
                        try: page.screenshot(path=f"{dbg_dir}/cat_nav_fail_{ci}.png", full_page=True)
                        except Exception: pass
                        continue
                    time.sleep(REQ_DELAY)

                    links, mapping = collect_product_links_from_listing(page, cu)
                    if not links:
                        try: page.screenshot(path=f"{dbg_dir}/cat_empty_{ci}.png", full_page=True)
                        except Exception: pass

                    for u in links:
                        if u not in product_urls:
                            product_urls.add(u)
                            prod2listing[u] = mapping.get(u, cu)

                    print(f"[selver] {cu} → +{len(links)} products (total so far: {len(product_urls)})")

                for i, pu in enumerate(sorted(product_urls), 1):
                    if not _is_selver_product_like(pu):
                        continue

                    got = safe_goto(page, pu)
                    if not got:
                        got = open_product_via_click(page, prod2listing.get(pu, ""), pu)
                        if not got:
                            try: page.screenshot(path=f"{dbg_dir}/prod_nav_fail_{i}.png", full_page=True)
                            except Exception: pass
                            continue

                    time.sleep(REQ_DELAY)

                    row = _extract_row_from_pdp(page, pu)
                    if not row:
                        # try click-through from its listing (SPA hydration)
                        if open_product_via_click(page, prod2listing.get(pu, ""), pu):
                            time.sleep(0.3)
                            row = _extract_row_from_pdp(page, pu)

                    if row:
                        ext_id = row["ext_id"]
                        if ext_id not in seen_ext_ids:
                            w.writerow(row)
                            seen_ext_ids.add(ext_id)
                            rows_written += 1

                    if (i % 25) == 0:
                        f.flush()

            browser.close()

    print(f"[selver] wrote {rows_written} product rows.")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        crawl()
    except KeyboardInterrupt:
        pass

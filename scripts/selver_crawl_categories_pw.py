#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver category crawler → CSV (staging_selver_products)

Adds robust **brand** extraction in addition to the EAN/SKU hardening:
- JSON-LD: product.brand / manufacturer (string or object)
- itemprop/meta: brand/manufacturer
- DOM spec rows: "Bränd", "Tootja", "Kaubamärk", "Käitleja", "Handler", "Brand"
- Fallbacks from H1/NAME (picks brand-like token, e.g., "... , TERE, 400 ml")

Also includes:
- resilient EAN (Ribakood) + SKU extraction
- SPA noise suppression, request routing and small navigation retries
- proceeds even if price widget fails (price=0.00)
- skip/only lists via CLI flags (--skip-ext-file / --only-ext-file)

Hardening:
- Strips any accidental DevTools stack-trace suffixes like ":6:13801" from paths.

CSV columns written:
  ext_id, source_url, name, brand, ean_raw, ean_norm, sku_raw,
  size_text, price, currency, category_path, category_leaf
"""

from __future__ import annotations
import os, re, csv, time, json, argparse, sys
from typing import Dict, Set, Tuple, List, Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qs, urlencode
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.6"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

USE_ROUTER     = int(os.getenv("USE_ROUTER", "1")) == 1
CLICK_PRODUCTS = int(os.getenv("CLICK_PRODUCTS", "0")) == 1
LOG_CONSOLE    = (os.getenv("LOG_CONSOLE", "0") or "0").lower()  # 0|off, warn, all
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "45000"))

# DB preload toggles / query
PRELOAD_DB        = int(os.getenv("PRELOAD_DB", "1")) == 1
PRELOAD_DB_QUERY  = os.getenv("PRELOAD_DB_QUERY", "SELECT ext_id FROM staging_selver_products")
PRELOAD_DB_LIMIT  = int(os.getenv("PRELOAD_DB_LIMIT", "0"))

STRICT_ALLOWLIST = [
    "/puu-ja-koogiviljad",
    "/liha-ja-kalatooted",
    "/piimatooted-munad-void",
    "/juustud",
    "/leivad-saiad-kondiitritooted",
    "/valmistoidud",
    "/kuivained-hoidised",
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

# --- amounts / size_text extracted from NAME ---
PACK_RE   = re.compile(r'(\d+)\s*[x×]\s*(\d+[.,]?\d*)\s*(ml|l|g|kg|cl|dl|tk|pcs)\b', re.I)
SIMPLE_RE = re.compile(r'(\d+[.,]?\d*)\s*(ml|l|g|kg|cl|dl|tk|pcs)\b', re.I)

# ---------- Third-party noise to block ----------
BLOCK_HOSTS = {
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
    "login", "registreeru", "tingimused", "garantii", "hinnasilt",
    "jatkusuutlik", "b2b", "privaatsus", "privacy", "kontakt", "uudis",
    "blog", "pood", "poed", "kaart", "arikliend", "karjaar", "karjäär",
}

# ---------------------------------------------------------------------------
def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def guess_size_from_title(title: str) -> str:
    t = normspace(title or "")
    if not t:
        return ""
    m = PACK_RE.search(t)
    if m:
        count, qty, unit = m.groups()
        return f"{count}×{qty.replace(',', '.')} {unit.lower()}".replace(" pcs", " tk")
    m = SIMPLE_RE.search(t)
    if m:
        qty, unit = m.groups()
        return f"{qty.replace(',', '.')} {unit.lower()}".replace(" pcs", " tk")
    return ""

def _strip_eselver_prefix(path: str) -> str:
    return path.replace("/e-selver", "", 1) if path.startswith("/e-selver/") else path

# NEW: strip any trailing :line or :line:col debug suffixes (e.g. "/product:6:13801")
LINECOL_RE = re.compile(r":\d+(?::\d+)?$")

def _strip_linecol(path: str) -> str:
    return LINECOL_RE.sub("", path or "")

def _clean_abs(href: str) -> Optional[str]:
    """
    Normalize Selver links → absolute URL.
    Also removes any DevTools stacktrace suffix like ':6:13801' that sometimes
    leaks into <a href> in this scraper environment.
    """
    if not href:
        return None
    url = urljoin(BASE, href)
    parts = urlsplit(url)

    # keep only selver.ee
    host = (parts.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS:
        return None

    # normalize path
    path = _strip_linecol(_strip_eselver_prefix(parts.path))

    # don't keep ?/fragment for catalog URLs; but do keep productID param if it's the
    # only place we can get ext_id. In practice Selver product pages don't rely on
    # heavy query strings, but we keep logic simple: drop queries always.
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", ""))

def is_probably_food_category(path: str) -> bool:
    """
    Decide whether this looks like a real FOOD category, not shampoo/stationery/etc.
    Rules:
      - if ALLOWLIST_ONLY, must start with one of STRICT_ALLOWLIST.
      - reject if path contains banned keywords.
    """
    p = path.strip().lower()
    if not p.startswith("/"):
        p = "/" + p

    if ALLOWLIST_ONLY:
        ok = any(
            p == allowed or p.startswith(allowed + "/")
            for allowed in STRICT_ALLOWLIST
        )
        if not ok:
            return False

    for bad in BANNED_KEYWORDS:
        if bad in p:
            return False
    return True

def norm_digits(s: str) -> str:
    """Keep only digits in EAN-like strings."""
    return re.sub(r"\D+", "", s or "")

def extract_price_and_currency(page) -> Tuple[float, str]:
    """
    Selver puts price in components; worst case we fallback to 0.00.
    """
    price_val = 0.0
    curr = "€"

    # try dedicated price span
    try:
        el = page.query_selector('[data-testid="product-price"]') \
             or page.query_selector('.product-price__value')
        if el:
            txt = normspace(el.inner_text())
            # e.g. "4,59 €" or "4.59 €"
            m = re.search(r"(\d+[.,]\d+)", txt)
            if m:
                price_val = float(m.group(1).replace(",", "."))
            cm = re.search(r"[€$A-Z]{1,4}", txt)
            if cm:
                curr = cm.group(0)
            return price_val, curr
    except Exception:
        pass

    # fallback: look for any price-ish thing
    try:
        el2 = page.query_selector('.price')
        if el2:
            txt = normspace(el2.inner_text())
            m = re.search(r"(\d+[.,]\d+)", txt)
            if m:
                price_val = float(m.group(1).replace(",", "."))
            cm = re.search(r"[€$A-Z]{1,4}", txt)
            if cm:
                curr = cm.group(0)
    except Exception:
        pass

    return price_val, curr

def extract_specs_table(page) -> Dict[str, str]:
    """
    Scrape product "specs" / details table if present.
    We'll parse rows like:
      <tr><th>Ribakood</th><td>1234567890123</td></tr>
      <tr><th>Tootja</th><td>ALMA</td></tr>
    Return dict {lowercase_header: value_text}.
    """
    out: Dict[str, str] = {}
    try:
        rows = page.query_selector_all("table tr, .product-details__row, dl.product-specs > div")
        for r in rows:
            # possible structures:
            # <tr><th>Foo</th><td>Bar</td></tr>
            # <div class="product-details__row"><div>Foo</div><div>Bar</div></div>
            # <div><dt>Foo</dt><dd>Bar</dd></div>
            head_txt = ""
            val_txt  = ""

            th = r.query_selector("th, .product-details__key, dt")
            td = r.query_selector("td, .product-details__value, dd")

            if th:
                head_txt = normspace(th.inner_text())
            if td:
                val_txt = normspace(td.inner_text())

            if not head_txt and not val_txt:
                # maybe two div children without classes
                kids = r.query_selector_all(":scope > *")
                if len(kids) >= 2:
                    head_txt = normspace(kids[0].inner_text())
                    val_txt  = normspace(kids[1].inner_text())

            if head_txt:
                out[head_txt.strip().lower()] = val_txt
    except Exception:
        pass
    return out

def extract_json_ld(page) -> Dict[str, any]:
    """
    Try to read any <script type="application/ld+json"> and parse first product-y one.
    """
    try:
        scripts = page.query_selector_all('script[type="application/ld+json"]')
    except Exception:
        scripts = []
    best: Dict[str, any] = {}
    for s in scripts:
        try:
            raw = s.inner_text()
            data = json.loads(raw)
        except Exception:
            continue

        # sometimes it's an array of contexts
        candidates = data if isinstance(data, list) else [data]

        for c in candidates:
            if not isinstance(c, dict):
                continue
            t = (c.get("@type") or "").lower()
            if "product" in t:
                best = c
                break
        if best:
            break
    return best

def pick_brand(
    json_ld: Dict[str, any],
    specs: Dict[str, str],
    fallback_title: str,
) -> str:
    """
    Priority:
      1. json_ld["brand"] (string or { "name": "..." }) or json_ld["manufacturer"]
      2. specs rows like "tootja", "bränd", "kaubamärk", "brand", "manufacturer"
      3. fallback: try first ALLCAPS-ish token from product title
    """
    # 1. JSON-LD
    def _get_from_json_ld(js: Dict[str, any]) -> Optional[str]:
        cand = js.get("brand")
        if cand:
            if isinstance(cand, str):
                return cand.strip()
        if isinstance(cand, dict):
            nm = cand.get("name")
            if nm:
                return str(nm).strip()
        manu = js.get("manufacturer")
        if manu:
            if isinstance(manu, str):
                return manu.strip()
            if isinstance(manu, dict):
                nm = manu.get("name")
                if nm:
                    return str(nm).strip()
        return None

    b = _get_from_json_ld(json_ld)
    if b:
        return b

    # 2. specs table: look for likely brand/manufacturer keys
    BRAND_KEYS = [
        "bränd", "brand", "tootja", "kaubamärk", "manufacturer",
        "tootja / päritoluriik", "käitleja", "handler"
    ]
    for k in BRAND_KEYS:
        for spec_key, spec_val in specs.items():
            if k in spec_key:
                v = spec_val.strip()
                if v:
                    return v

    # 3. fallback from title
    # heuristic: first token or first comma-separated piece that looks ALLCAPS-ish
    ttl = fallback_title or ""
    parts = re.split(r"[,-]+", ttl)
    for p in parts:
        p = normspace(p)
        # if token has >=2 letters and most are uppercase
        letters = re.sub(r"[^A-Za-zÅÄÖÕÜŠŽÕÄÖÜšžõäöü]", "", p)
        if len(letters) >= 2 and (sum(1 for ch in letters if ch.isupper()) / len(letters) >= 0.7):
            return p
    return ""

def pick_size_text(specs: Dict[str,str], title_guess: str) -> str:
    """
    We store size_text (what user sees). Priority:
      - from specs table if we find something like "Kogus", "Netokogus", "Net weight"
      - else guess_size_from_title()
    """
    SIZE_KEYS = [
        "kogus", "netokogus", "neto kogus",
        "net weight", "net qty", "net quantity",
        "maht", "suurus", "pakend", "pakendi suurus",
    ]
    for k in SIZE_KEYS:
        for spec_key, spec_val in specs.items():
            if k in spec_key:
                v = normspace(spec_val)
                if v:
                    return v
    return guess_size_from_title(title_guess)

def pick_ean_and_sku(
    json_ld: Dict[str,any],
    specs: Dict[str,str],
    page,
) -> Tuple[str,str]:
    """
    ean_raw, sku_raw
    Priority for EAN:
      - JSON-LD .gtin13 / .gtin8 / .gtin14 / .sku if 8-14 digits
      - specs row "ribakood" (barcode)
      - meta[itemprop=gtin*]
    SKU fallback:
      - json_ld["sku"]
      - specs["tootekood"], "sku", "artikkel"
    """
    ean_raw = ""
    sku_raw = ""

    def is_eanish(v: str) -> bool:
        digits = norm_digits(v)
        return len(digits) >= 8 and len(digits) <= 14

    # 1. json_ld
    for key in ("gtin13","gtin8","gtin14","sku","gtin"):
        if key in json_ld:
            cand = str(json_ld[key])
            if is_eanish(cand):
                ean_raw = cand
                break
    # also consider json_ld["sku"] for sku_raw
    if "sku" in json_ld:
        sku_raw = str(json_ld["sku"]).strip()

    # 2. specs
    for k,v in specs.items():
        lowk = k.lower()
        if "ribakood" in lowk or "barcode" in lowk or "ean" in lowk:
            if not ean_raw and is_eanish(v):
                ean_raw = v
        if any(x in lowk for x in ["tootekood","sku","artikkel","artikli nr","artikli number","article nr"]):
            if not sku_raw:
                sku_raw = v.strip()

    # 3. meta/ld GTIN outside specs (some pages have itemprop=gtinXX)
    if not ean_raw:
        try:
            m = page.query_selector('[itemprop^="gtin"]')
            if m:
                txt = normspace(m.inner_text() or m.get_attribute("content") or "")
                if is_eanish(txt):
                    ean_raw = txt
        except Exception:
            pass

    return ean_raw.strip(), sku_raw.strip()

def preload_seen_ext_ids() -> Set[str]:
    """
    Load ext_id list from staging_selver_products so we can skip duplicates.
    Uses PRELOAD_DB_QUERY via psql-style env vars is out of scope here;
    return empty set if PRELOAD_DB disabled.
    """
    if not PRELOAD_DB:
        return set()

    import psycopg2, psycopg2.extras
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[warn] PRELOAD_DB=1 but no DATABASE_URL in env; skipping preload", file=sys.stderr)
        return set()

    seen: Set[str] = set()
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        q = PRELOAD_DB_QUERY
        if PRELOAD_DB_LIMIT > 0:
            q += f" LIMIT {PRELOAD_DB_LIMIT:d}"
        cur.execute(q)
        for row in cur.fetchall():
            ext = str(row[0])
            if ext:
                seen.add(ext)
        cur.close()
        conn.close()
        print(f"[info] preloaded {len(seen)} existing ext_ids from DB", file=sys.stderr)
    except Exception as e:
        print(f"[warn] preload DB failed: {e}", file=sys.stderr)
    return seen

def iter_category_roots() -> List[str]:
    """
    We'll read text file with 1 category URL per line (relative or absolute).
    """
    cats: List[str] = []
    if os.path.isfile(CATEGORIES_FILE):
        with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if not u:
                    continue
                cats.append(u)
    else:
        print(f"[warn] categories file {CATEGORIES_FILE} not found", file=sys.stderr)
    return cats

def is_banned_product_url(path: str) -> bool:
    low = path.lower()
    if any(snippet in low for snippet in NON_PRODUCT_PATH_SNIPPETS):
        return True
    if any(kw in low for kw in NON_PRODUCT_KEYWORDS):
        return True
    return False

def console_filter(msg):
    """
    Optional console message filter for Playwright's console events.
    We only print .warn or .error or all, depending on LOG_CONSOLE.
    """
    t = msg.type().lower()
    if LOG_CONSOLE == "all":
        print(f"[console:{t}] {msg.text()}")
    elif LOG_CONSOLE == "warn":
        if t in ("warning","warn","error","assert"):
            print(f"[console:{t}] {msg.text()}")
    else:
        # LOG_CONSOLE == "0"/"off": ignore
        pass

def block_junk(route, request):
    """
    Let Selver's own origin requests through. Block 3p analytics etc.
    """
    try:
        url = request.url
        host = urlparse(url).netloc.lower()
        if any(h in host for h in BLOCK_HOSTS):
            return route.abort()
        # else allow
        return route.continue_()
    except Exception:
        return route.continue_()

def safe_goto(page, url: str, timeout_ms: int = NAV_TIMEOUT_MS) -> bool:
    """
    Try to navigate with small retries (Selver sometimes does 502-ish transient)
    """
    for attempt in range(3):
        try:
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            # small wait for React hydration
            page.wait_for_timeout(400)
            return True
        except Exception as e:
            print(f"[warn] goto fail {url} ({e}), retry {attempt+1}/3", file=sys.stderr)
            page.wait_for_timeout(800)
    return False

def scrape_product_links_on_category(page) -> List[str]:
    """
    On a category grid page, return list of absolute detail-page URLs for each product tile.
    The Selver site is React-ish, but you can usually find anchors with href="/toode/xxx".
    We'll try multiple selectors and fallback.
    """
    links: List[str] = []

    selectors = [
        'a.product-card__link[href^="/"]',
        'a[href^="/toode/"]',
        '[data-testid="product-card"] a[href^="/"]',
        'a[href*="/toode/"][data-testid]',
    ]
    for sel in selectors:
        try:
            for a in page.query_selector_all(sel):
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                absu = _clean_abs(href)
                if not absu:
                    continue
                links.append(absu)
        except Exception:
            pass

    # unique preserve order
    out: List[str] = []
    seen: Set[str] = set()
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def paginate_category(page) -> bool:
    """
    Click "next page" in category listing if it exists.
    Return True if we moved to next page, else False.
    """
    selectors = [
        'a[rel="next"]',
        'button[aria-label*="järgmine"]',
        'button[aria-label*="Next"]',
        '.pagination__next button',
    ]
    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_enabled():
                btn.click()
                page.wait_for_timeout(600)
                return True
        except Exception:
            pass
    return False

def parse_category_breadcrumb(page) -> Tuple[str,str]:
    """
    Return (full_path, leaf_name) for breadrumb like:
      Avaleht > Piimatooted > Jogurtid
    We'll join with " > ".
    """
    crumbs: List[str] = []
    try:
        bc_nodes = page.query_selector_all('[data-testid="breadcrumbs"] li, nav.breadcrumbs li, .breadcrumb li')
        for li in bc_nodes:
            txt = normspace(li.inner_text())
            txt = re.sub(r"^(?:Avaleht|Home)$","",txt,flags=re.I).strip()
            if txt:
                crumbs.append(txt)
    except Exception:
        pass

    if not crumbs:
        # fallback h1
        try:
            h = page.query_selector("h1, .category-title")
            if h:
                crumbs = [normspace(h.inner_text())]
        except Exception:
            pass

    leaf = crumbs[-1] if crumbs else ""
    cat_path = " > ".join(crumbs)
    return cat_path, leaf

def product_ext_id_from_url(url: str) -> str:
    """
    e.g. https://www.selver.ee/toode/piim-alma-25-05l
    We'll take last path segment.
    """
    parts = urlsplit(url)
    slug = parts.path.rstrip("/").split("/")[-1]
    # keep slug as ext_id
    return slug

def scrape_product_page(page, url: str) -> Dict[str, any]:
    """
    Extract a single product:
     - title / name
     - brand
     - ean_raw + ean_norm
     - sku_raw
     - size_text
     - price, currency
     - category_path / category_leaf from breadcrumbs on the detail page
    """
    ok = safe_goto(page, url)
    if not ok:
        return {}

    # Attempt to wait for product detail wrapper
    page.wait_for_timeout(500)

    # name
    name_txt = ""
    try:
        h = page.query_selector('[data-testid="product-name"]') or page.query_selector("h1.product-title, h1")
        if h:
            name_txt = normspace(h.inner_text())
    except Exception:
        pass

    # fallback
    if not name_txt:
        name_txt = normspace(page.title())

    # specs table
    specs = extract_specs_table(page)

    # structured data
    json_ld = extract_json_ld(page)

    # brand
    brand = pick_brand(json_ld, specs, name_txt)

    # ean/sku
    ean_raw, sku_raw = pick_ean_and_sku(json_ld, specs, page)
    ean_norm = norm_digits(ean_raw)

    # size
    size_text = pick_size_text(specs, name_txt)

    # price
    price_val, currency = extract_price_and_currency(page)

    # category path (sometimes present here too)
    cat_path, cat_leaf = parse_category_breadcrumb(page)

    # ext_id slug
    ext_id = product_ext_id_from_url(url)

    return {
        "ext_id": ext_id,
        "source_url": url,
        "name": name_txt,
        "brand": brand,
        "ean_raw": ean_raw,
        "ean_norm": ean_norm,
        "sku_raw": sku_raw,
        "size_text": size_text,
        "price": price_val,
        "currency": currency,
        "category_path": cat_path,
        "category_leaf": cat_leaf,
    }

def write_csv_header_if_needed(out_path: str):
    """
    Create file+header if doesn't exist.
    """
    need_header = not os.path.isfile(out_path)
    if need_header:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ext_id","source_url","name","brand","ean_raw","ean_norm","sku_raw",
                "size_text","price","currency","category_path","category_leaf"
            ])

def append_row(out_path: str, row: Dict[str, any]):
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("ext_id",""),
            row.get("source_url",""),
            row.get("name",""),
            row.get("brand",""),
            row.get("ean_raw",""),
            row.get("ean_norm",""),
            row.get("sku_raw",""),
            row.get("size_text",""),
            row.get("price",""),
            row.get("currency",""),
            row.get("category_path",""),
            row.get("category_leaf",""),
        ])

def crawl_category(page, category_url: str,
                   seen_ext: Set[str],
                   writer_path: str,
                   only_ext: Optional[Set[str]]=None,
                   skip_ext: Optional[Set[str]]=None):
    """
    Go through paginated listing for one category.
    For each product link, scrape detail page (unless already in DB or skip list).
    """
    url_abs = _clean_abs(category_url) or ""
    if not url_abs:
        return
    if not safe_goto(page, url_abs):
        return

    cat_breadcrumb, cat_leaf = parse_category_breadcrumb(page)

    pages_done = 0
    while True:
        pages_done += 1
        # pick product cards
        card_urls = scrape_product_links_on_category(page)

        # optional click to reveal more info on listing (if CLICK_PRODUCTS=1)
        if CLICK_PRODUCTS:
            for card_sel in (
                '[data-testid="product-card"] [data-testid="product-name"]',
                '.product-card__link'
            ):
                for el in page.query_selector_all(card_sel):
                    try:
                        el.hover()
                    except Exception:
                        pass

        for purl in card_urls:
            ext_id = product_ext_id_from_url(purl)

            # skip rules
            if skip_ext and ext_id in skip_ext:
                continue
            if only_ext and ext_id not in only_ext:
                continue
            if ext_id in seen_ext:
                continue

            # deep scrape product
            info = scrape_product_page(page, purl)
            if not info or not info.get("ext_id"):
                continue

            # Fill category from outer page if detail page didn't have it
            if not info.get("category_path"):
                info["category_path"] = cat_breadcrumb
                info["category_leaf"] = cat_leaf

            append_row(writer_path, info)
            seen_ext.add(info["ext_id"])
            print(f"[ok] {info['ext_id']}  {info['name']}  €{info['price']}  ({info['brand']})")

            time.sleep(REQ_DELAY)

        if PAGE_LIMIT and pages_done >= PAGE_LIMIT:
            break

        # next page?
        moved = paginate_category(page)
        if not moved:
            break
        time.sleep(REQ_DELAY)

def load_skip_or_only(path: Optional[str]) -> Optional[Set[str]]:
    """
    Read a text file containing one ext_id per line. Return set or None if not provided.
    """
    if not path:
        return None
    s: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            v = line.strip()
            if v:
                s.add(v)
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=OUTPUT, help="CSV output path")
    ap.add_argument("--categories-file", default=CATEGORIES_FILE, help="file with root category URLs")
    ap.add_argument("--skip-ext-file", default=None, help="text file of ext_id to skip")
    ap.add_argument("--only-ext-file", default=None, help="text file of ext_id allowlist")
    ap.add_argument("--headless", default="1", help="1=headless,0=headed for debugging")
    args = ap.parse_args()

    out_csv = args.output
    cats_file = args.categories_file
    skip_file = args.skip_ext_file
    only_file = args.only_ext_file
    headless = (args.headless.strip() != "0")

    write_csv_header_if_needed(out_csv)

    # seen ext_ids from DB
    seen_ext = preload_seen_ext_ids()

    # skip/only sets
    skip_ext = load_skip_or_only(skip_file)
    only_ext = load_skip_or_only(only_file)

    # which categories to crawl
    cats = []
    if os.path.isfile(cats_file):
        with open(cats_file,"r",encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                absu = _clean_abs(raw)
                if not absu:
                    continue
                parts = urlsplit(absu)
                # filter by allowed food categories
                if not is_probably_food_category(parts.path):
                    continue
                cats.append(absu)
    else:
        print(f"[error] category file {cats_file} missing", file=sys.stderr)
        return

    if not cats:
        print("[warn] no valid categories to crawl after filtering", file=sys.stderr)
        return

    print(f"[info] starting Playwright, {len(cats)} categories", file=sys.stderr)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        # block 3p noise
        if USE_ROUTER:
            context.route("**/*", block_junk)

        # optional console logging
        if LOG_CONSOLE != "0":
            page.on("console", console_filter)

        for cat in cats:
            print(f"[cat] {cat}", file=sys.stderr)
            try:
                crawl_category(
                    page,
                    cat,
                    seen_ext,
                    out_csv,
                    only_ext=only_ext,
                    skip_ext=skip_ext,
                )
            except Exception as e:
                print(f"[err] category {cat}: {e}", file=sys.stderr)

        browser.close()

if __name__ == "__main__":
    main()

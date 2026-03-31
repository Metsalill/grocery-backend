#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver category crawler → CSV + direct DB ingest (upsert_product_and_price)
"""

from __future__ import annotations
import os, re, csv, time, json, argparse, sys, datetime
from typing import Dict, Set, Tuple, List, Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit
from playwright.sync_api import sync_playwright

import psycopg2, psycopg2.extras

# ---------------------------------------------------------------------------
BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.6"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

USE_ROUTER     = int(os.getenv("USE_ROUTER", "1")) == 1
CLICK_PRODUCTS = int(os.getenv("CLICK_PRODUCTS", "0")) == 1
LOG_CONSOLE    = (os.getenv("LOG_CONSOLE", "0") or "0").lower()
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "45000"))

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
    "/maitseained-ja-puljongid",
    "/kastmed-olid",
    "/maiustused-kupsised-naksid",
    "/joogid",
    "/kulmutatud-toidukaubad",
    "/suurpakendid",
    "/lastekaubad",
    "/lemmiklooma-kaubad",
    "/enesehooldustarbed",
    "/majapidamis-ja-kodukaubad",
]
ALLOWLIST_ONLY = int(os.getenv("ALLOWLIST_ONLY", "1")) == 1

BANNED_KEYWORDS = {
    "sisustus","kodutekstiil","valgustus","kardin","jouluvalgustid",
    "vaikesed-sisustuskaubad","kuunlad","kirja-ja-kontoritarbed",
    "remondi-ja-turvatooted","omblus-ja-kasitootarbed","meisterdamine",
    "ajakirjad","autojuhtimine","kotid","aed-ja-lilled",
    "sport","pallimangud","jalgrattasoit","ujumine","matkamine",
    "tervisesport","manguasjad","lutid","ideed-ja-hooajad",
    "kodumasinad","elektroonika","meelelahutuselektroonika",
    "vaikesed-kodumasinad","lambid-patareid-ja-taskulambid",
    "loodustooted-ja-toidulisandid","roivaste-ja-jalatsite-hooldus",
    "muud-majapidamise-kaubad","koogitarbed","kodutehnika",
    "vannitoa-ja-saunatarvikud","laste-sokid-sukad-pesu",
}

PACK_RE   = re.compile(r'(\d+)\s*[x×]\s*(\d+[.,]?\d*)\s*(ml|l|g|kg|cl|dl|tk|pcs)\b', re.I)
SIMPLE_RE = re.compile(r'(\d+[.,]?\d*)\s*(ml|l|g|kg|cl|dl|tk|pcs)\b', re.I)

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

def clean_field(s: str) -> str:
    """Strip carriage returns and newlines from any scraped string value."""
    return re.sub(r"[\r\n]+", " ", s or "").strip()

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

LINECOL_RE = re.compile(r":\d+(?::\d+)?$")

def _strip_linecol(path: str) -> str:
    return LINECOL_RE.sub("", path or "")

def _clean_abs(href: str) -> Optional[str]:
    if not href:
        return None
    url = urljoin(BASE, href)
    parts = urlsplit(url)
    host = (parts.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS:
        return None
    path = _strip_linecol(_strip_eselver_prefix(parts.path))
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", ""))

def is_probably_food_category(path: str) -> bool:
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
    return re.sub(r"\D+", "", s or "")

def extract_price_and_currency(page) -> Tuple[float, str]:
    """
    Extract price from Selver product page.

    Selver uses class="ProductPrice" with the price as a direct text node:
        <div class="ProductPrice">
            " 2,99 € "
            <span class="ProductPrice__unit-price">2,99 €/kg</span>
        </div>

    We try selectors in priority order, then fall back to JSON-LD offers.
    """
    price_val = 0.0
    curr = "€"

    # Ordered by specificity — Selver's real selector first
    SELECTORS = [
        '.ProductPrice',                    # Selver (confirmed correct)
        '[data-testid="product-price"]',    # generic / other stores
        '.product-price__value',            # generic / other stores
        '.price',                           # last-resort generic
    ]

    for sel in SELECTORS:
        try:
            el = page.query_selector(sel)
            if not el:
                continue
            txt = normspace(el.inner_text())
            if not txt:
                continue
            m = re.search(r"(\d+[.,]\d+)", txt)
            if m:
                price_val = float(m.group(1).replace(",", "."))
                cm = re.search(r"[€$A-Z]{1,4}", txt)
                if cm:
                    curr = cm.group(0)
                if price_val > 0:
                    return price_val, curr
        except Exception:
            pass

    # Fallback: JSON-LD offers block (most reliable, store-agnostic)
    try:
        jld = extract_json_ld(page)
        offers = jld.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        p = offers.get("price")
        if p:
            price_val = float(str(p).replace(",", "."))
            curr = offers.get("priceCurrency", "EUR")
            if price_val > 0:
                return price_val, curr
    except Exception:
        pass

    return price_val, curr

def extract_specs_table(page) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        rows = page.query_selector_all("table tr, .product-details__row, dl.product-specs > div")
        for r in rows:
            head_txt = ""
            val_txt  = ""
            th = r.query_selector("th, .product-details__key, dt")
            td = r.query_selector("td, .product-details__value, dd")
            if th:
                head_txt = normspace(th.inner_text())
            if td:
                val_txt = normspace(td.inner_text())
            if not head_txt and not val_txt:
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

def pick_brand(json_ld, specs, fallback_title):
    def _get_from_json_ld(js):
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

    ttl = fallback_title or ""
    parts = re.split(r"[,-]+", ttl)
    for p in parts:
        p = normspace(p)
        letters = re.sub(r"[^A-Za-zÅÄÖÕÜŠŽÕÄÖÜšžõäöü]", "", p)
        if len(letters) >= 2:
            upper_count = sum(1 for ch in letters if ch.isupper())
            if upper_count / len(letters) >= 0.7:
                return p
    return ""

def pick_size_text(specs, title_guess):
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

def pick_ean_and_sku(json_ld, specs, page):
    ean_raw = ""
    sku_raw = ""

    def is_eanish(v):
        digits = norm_digits(v)
        return len(digits) >= 8 and len(digits) <= 14

    for key in ("gtin13","gtin8","gtin14","sku","gtin"):
        if key in json_ld:
            cand = str(json_ld[key])
            if is_eanish(cand):
                ean_raw = cand
                break
    if "sku" in json_ld:
        sku_raw = str(json_ld["sku"]).strip()

    for k,v in specs.items():
        lowk = k.lower()
        if "ribakood" in lowk or "barcode" in lowk or "ean" in lowk:
            if not ean_raw and is_eanish(v):
                ean_raw = v
        if any(x in lowk for x in ["tootekood","sku","artikkel","artikli nr","artikli number","article nr"]):
            if not sku_raw:
                sku_raw = v.strip()

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
    if not PRELOAD_DB:
        return set()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[warn] PRELOAD_DB=1 but no DATABASE_URL; skipping preload", file=sys.stderr)
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

def is_banned_product_url(path: str) -> bool:
    low = path.lower()
    if any(snippet in low for snippet in NON_PRODUCT_PATH_SNIPPETS):
        return True
    if any(kw in low for kw in NON_PRODUCT_KEYWORDS):
        return True
    return False

def console_filter(msg):
    t = msg.type.lower()
    if LOG_CONSOLE == "all":
        print(f"[console:{t}] {msg.text}")
    elif LOG_CONSOLE == "warn":
        if t in ("warning","warn","error","assert"):
            print(f"[console:{t}] {msg.text}")

def block_junk(route, request):
    try:
        url = request.url
        host = urlparse(url).netloc.lower()
        if any(h in host for h in BLOCK_HOSTS):
            return route.abort()
        return route.continue_()
    except Exception:
        return route.continue_()

def safe_goto(page, url: str, timeout_ms: int = NAV_TIMEOUT_MS) -> bool:
    for attempt in range(3):
        try:
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            page.wait_for_timeout(400)
            return True
        except Exception as e:
            print(f"[warn] goto fail {url} ({e}), retry {attempt+1}/3", file=sys.stderr)
            page.wait_for_timeout(800)
    return False

def _is_product_url(url: str) -> bool:
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    if len(segments) != 1:
        return False
    slug = segments[0]
    if "-" not in slug:
        return False
    NON_PRODUCT_SLUGS = {
        "ostukorv", "cart", "checkout", "otsi", "search", "konto",
        "login", "logout", "registreeru", "kontakt", "blogi", "uudised",
        "tootajad", "tingimused", "privaatsus", "kampaania", "kampaaniad",
        "retseptid", "kinkekaardid", "kauplused", "app", "e-selver",
        "selveri-kook", "kliendimangud", "selveekspress", "tule-toolle",
    }
    if slug in NON_PRODUCT_SLUGS:
        return False
    if any(slug == allowed.strip("/") for allowed in STRICT_ALLOWLIST):
        return False
    if any(kw in slug for kw in NON_PRODUCT_KEYWORDS):
        return False
    return True

def scrape_product_links_on_category(page) -> List[str]:
    links: List[str] = []
    selectors = [
        '[data-testid="product-card"] a[href^="/"]',
        'a.product-card__link[href^="/"]',
        '.product-list a[href^="/"]',
        '.products-grid a[href^="/"]',
        'article a[href^="/"]',
        'main a[href^="/"]',
    ]
    seen_local: Set[str] = set()
    for sel in selectors:
        try:
            for a in page.query_selector_all(sel):
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                absu = _clean_abs(href)
                if not absu:
                    continue
                if not _is_product_url(absu):
                    continue
                if absu not in seen_local:
                    seen_local.add(absu)
                    links.append(absu)
        except Exception:
            pass
    return links

def paginate_category(page) -> bool:
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
    parts = urlsplit(url)
    slug = parts.path.rstrip("/").split("/")[-1]
    return slug

def scrape_product_page(page, url: str) -> Dict[str, any]:
    ok = safe_goto(page, url)
    if not ok:
        return {}

    page.wait_for_timeout(500)

    name_txt = ""
    try:
        h = page.query_selector('[data-testid="product-name"]') or page.query_selector("h1.product-title, h1")
        if h:
            name_txt = normspace(h.inner_text())
    except Exception:
        pass
    if not name_txt:
        name_txt = normspace(page.title())

    specs = extract_specs_table(page)
    json_ld = extract_json_ld(page)
    brand = pick_brand(json_ld, specs, name_txt)
    ean_raw, sku_raw = pick_ean_and_sku(json_ld, specs, page)
    ean_norm = norm_digits(ean_raw)
    size_text = pick_size_text(specs, name_txt)
    price_val, currency = extract_price_and_currency(page)
    cat_path, cat_leaf = parse_category_breadcrumb(page)
    ext_id = product_ext_id_from_url(url)

    return {
        "ext_id": ext_id,
        "source_url": url,
        "name": clean_field(name_txt),
        "brand": clean_field(brand),
        "ean_raw": clean_field(ean_raw),
        "ean_norm": clean_field(ean_norm),
        "sku_raw": clean_field(sku_raw),
        "size_text": clean_field(size_text),
        "price": price_val,
        "currency": clean_field(currency),
        "category_path": clean_field(cat_path),
        "category_leaf": clean_field(cat_leaf),
    }

def write_csv_header_if_needed(out_path: str):
    need_header = not os.path.isfile(out_path)
    if need_header:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([
                "ext_id","source_url","name","brand","ean_raw","ean_norm","sku_raw",
                "size_text","price","currency","category_path","category_leaf"
            ])

def append_row(out_path: str, row: Dict[str, any]):
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow([
            row.get("ext_id",""), row.get("source_url",""), row.get("name",""),
            row.get("brand",""), row.get("ean_raw",""), row.get("ean_norm",""),
            row.get("sku_raw",""), row.get("size_text",""), row.get("price",""),
            row.get("currency",""), row.get("category_path",""), row.get("category_leaf",""),
        ])

def normalize_currency(cur: str) -> str:
    c = (cur or "").strip()
    if c == "€":
        return "EUR"
    if not c:
        return "EUR"
    return c


def bulk_ingest_to_db(rows: List[Dict[str, any]], store_id: int) -> None:
    if store_id <= 0:
        print("[selver] STORE_ID not set or invalid, skipping DB ingest.", file=sys.stderr)
        return

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[selver] no DATABASE_URL env var, skipping DB ingest.", file=sys.stderr)
        return

    if not rows:
        print("[selver] nothing to ingest.", file=sys.stderr)
        return

    # Filter out zero-price rows — don't overwrite good data with 0.00
    valid_rows = [r for r in rows if float(r.get("price") or 0.0) > 0]
    skipped = len(rows) - len(valid_rows)
    if skipped > 0:
        print(f"[selver] skipping {skipped} rows with price=0.00", file=sys.stderr)

    if not valid_rows:
        print("[selver] no valid rows to ingest after filtering zero prices.", file=sys.stderr)
        return

    ts_now = datetime.datetime.now(datetime.timezone.utc)

    sql = """
        SELECT upsert_product_and_price(
            %s::text,      -- in_source
            %s::text,      -- in_ext_id
            %s::text,      -- in_name
            %s::text,      -- in_brand
            %s::text,      -- in_size_text
            %s::text,      -- in_ean_raw
            %s::numeric,   -- in_price
            %s::text,      -- in_currency
            %s::integer,   -- in_store_id
            %s,            -- in_seen_at
            %s::text       -- in_source_url
        );
    """

    payload: List[tuple] = []
    for r in valid_rows:
        payload.append((
            "selver",
            r.get("ext_id") or "",
            r.get("name") or "",
            r.get("brand") or "",
            r.get("size_text") or "",
            r.get("ean_raw") or "",
            float(r.get("price") or 0.0),
            normalize_currency(r.get("currency") or ""),
            int(store_id),
            ts_now,
            r.get("source_url") or "",
        ))

    sent = 0
    errors = 0
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        for row_tuple in payload:
            try:
                cur.execute(sql, row_tuple)
                sent += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"[warn] upsert failed for {row_tuple[1]}: {e}", file=sys.stderr)
                conn.rollback()
        conn.commit()
        cur.close()
        conn.close()
        print(f"[selver] ingested {sent} rows into DB (errors: {errors}).", file=sys.stderr)
    except Exception as e:
        print(f"[selver] DB ingest FAILED: {e}", file=sys.stderr)


def crawl_category(page, category_url, seen_ext, writer_path, rows_for_ingest,
                   only_ext=None, skip_ext=None):
    url_abs = _clean_abs(category_url) or ""
    if not url_abs:
        return
    if not safe_goto(page, url_abs):
        return

    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    cat_breadcrumb, cat_leaf = parse_category_breadcrumb(page)

    pages_done = 0
    while True:
        pages_done += 1
        card_urls = scrape_product_links_on_category(page)

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
            if skip_ext and ext_id in skip_ext:
                continue
            if only_ext and ext_id not in only_ext:
                continue
            if ext_id in seen_ext:
                continue

            info = scrape_product_page(page, purl)
            if not info or not info.get("ext_id"):
                continue

            if not info.get("category_path"):
                info["category_path"] = clean_field(cat_breadcrumb)
                info["category_leaf"] = clean_field(cat_leaf)

            append_row(writer_path, info)
            rows_for_ingest.append(info)
            seen_ext.add(info["ext_id"])

            print(
                f"[ok] {info['ext_id']}  {info['name']}  €{info['price']}  ({info['brand']})",
                file=sys.stderr
            )
            time.sleep(REQ_DELAY)

        if PAGE_LIMIT and pages_done >= PAGE_LIMIT:
            break

        moved = paginate_category(page)
        if not moved:
            break
        time.sleep(REQ_DELAY)

def load_skip_or_only(path):
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
    ap.add_argument("--output", default=OUTPUT)
    ap.add_argument("--categories-file", default=CATEGORIES_FILE)
    ap.add_argument("--skip-ext-file", default=None)
    ap.add_argument("--only-ext-file", default=None)
    ap.add_argument("--headless", default="1")
    args = ap.parse_args()

    out_csv = args.output
    cats_file = args.categories_file
    headless = (args.headless.strip() != "0")

    write_csv_header_if_needed(out_csv)
    seen_ext = preload_seen_ext_ids()
    skip_ext = load_skip_or_only(args.skip_ext_file)
    only_ext = load_skip_or_only(args.only_ext_file)

    cats: List[str] = []
    if os.path.isfile(cats_file):
        with open(cats_file,"r",encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                absu = _clean_abs(raw)
                if not absu:
                    continue
                path_only = urlsplit(absu).path
                if not is_probably_food_category(path_only):
                    continue
                cats.append(absu)
    else:
        print(f"[error] category file {cats_file} missing", file=sys.stderr)
        return

    if not cats:
        print("[warn] no valid categories to crawl after filtering", file=sys.stderr)
        return

    print(f"[info] starting Playwright, {len(cats)} categories", file=sys.stderr)

    rows_for_ingest: List[Dict[str, any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        if USE_ROUTER:
            context.route("**/*", block_junk)

        if LOG_CONSOLE != "0":
            page.on("console", console_filter)

        for cat in cats:
            print(f"[cat] {cat}", file=sys.stderr)
            try:
                crawl_category(
                    page, cat, seen_ext, out_csv, rows_for_ingest,
                    only_ext=only_ext, skip_ext=skip_ext,
                )
            except Exception as e:
                print(f"[err] category {cat}: {e}", file=sys.stderr)

        browser.close()

    try:
        store_id_env = int(os.environ.get("STORE_ID", "31") or "31")
    except Exception:
        store_id_env = 31

    bulk_ingest_to_db(rows_for_ingest, store_id_env)
    print(f"[selver] wrote {len(rows_for_ingest)} product rows.", file=sys.stderr)

if __name__ == "__main__":
    main()

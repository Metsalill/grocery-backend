#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver (e-selver) category crawler -> CSV (for staging_selver_products)
Auto-discovers food categories, paginates / infinite-scrolls, then visits product pages and extracts:
  ext_id (url), name, ean_raw, size_text, price, currency, category_path, category_leaf
"""

from __future__ import annotations
import os, re, csv, time, json
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------------------------------------------------------------------
# Config
BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.8"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no limit
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

# Ban obvious non-food areas (for category discovery)
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

# Size finder (best-effort from title)
SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

# Third-party noise to block (extended)
BLOCK_HOSTS = {
    "adobe.com","assets.adobedtm.com","adobedtm.com","demdex.net","omtrdc.net",
    "googletagmanager.com","google-analytics.com","doubleclick.net","facebook.net",
    "cookiebot.com","consent.cookiebot.com","imgct.cookiebot.com",
    "klevu.com","js.klevu.com","klimg.klevu.com",
    "cobrowsing.promon.net",
}

# Hosts we consider valid for product pages
ALLOWED_HOSTS = {"www.selver.ee", "selver.ee"}

# Definite non-PDP paths (substrings that must include a slash)
NON_PRODUCT_PATH_SNIPPETS = {
    "/e-selver/","/ostukorv","/cart","/checkout","/search","/otsi",
    "/konto","/customer","/login","/logout","/registreeru","/uudised",
    "/tootajad","/kontakt","/tingimused","/privaatsus","/privacy",
    "/kampaania","/kampaaniad","/blogi","/app","/store-locator",
}

# Extra keywords that, if present anywhere in the path, mean "not a PDP"
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

def is_food_category(path: str) -> bool:
    p = path.lower()
    if not p.startswith("/e-selver/"):
        return False
    return not any(bad in p for bad in BANNED_KEYWORDS)

def _should_block(url: str) -> bool:
    h = urlparse(url).netloc.lower()
    return any(h == d or h.endswith("." + d) for d in BLOCK_HOSTS)

def _is_selver_product_like(url: str) -> bool:
    """
    Accept only real PDPs:
      • https://www.selver.ee/<hyphenated-slug>           (root PDP)
      • https://www.selver.ee/p/<slug>                    (explicit PDP prefix)
    Reject:
      • categories (/e-selver/...), RU locale (/ru/...), any other subdomains
      • generic site pages (login, terms, guarantee, sustainability, etc.)
    """
    u = urlparse(url)
    host = (u.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS:
        return False

    path = (u.path or "/").lower()

    # obvious non-PDPs
    if path.startswith("/ru/"):
        return False
    if any(sn in path for sn in NON_PRODUCT_PATH_SNIPPETS):
        return False
    if any(kw in path for kw in NON_PRODUCT_KEYWORDS):
        return False

    last = path.rstrip("/").rsplit("/", 1)[-1]

    # explicit PDP prefix
    if re.fullmatch(r"/p/[a-z0-9-]+/?", path):
        return True

    # root PDP: single segment with hyphens (avoid generic one-word pages)
    if re.fullmatch(r"/[a-z0-9-]+/?", path):
        # require at least one hyphen AND (either a digit OR >= 2 hyphens)
        # This prunes pages like /b2b-login (digit but also keyword filtered),
        # and generic info pages like /karjaar.
        return ("-" in last) and (any(ch.isdigit() for ch in last) or last.count("-") >= 2)

    return False

# ---------------------------------------------------------------------------
# Cookies / overlays / navigation

def accept_cookies(page):
    for sel in [
        "button:has-text('Nõustu')","button:has-text('Nõustun')",
        "button:has-text('Accept')","button[aria-label*='accept']",
        "button:has-text('Luba kõik')",
    ]:
        try:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_enabled():
                btn.first.click(timeout=3000)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(0.2)
                return
        except Exception:
            pass

def quiesce_overlays(page):
    try:
        page.evaluate("""
            for (const sel of [
              '#klevu_min_ltr','.klevu-mlntr','.klevu-fluid',
              '.Notification.fixed','#onetrust-consent-sdk',
              '#CookiebotDialog','iframe[name="_uspapiLocator"]'
            ]) { document.querySelectorAll(sel).forEach(n => n.remove()); }
        """)
    except Exception:
        pass

def safe_goto(page, url: str, timeout: int = 15000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        accept_cookies(page)
        quiesce_overlays(page)
        return True
    except Exception as e:
        print(f"[selver] NAV FAIL {url} -> {type(e).__name__}: {e}")
        return False

# ---------------------------------------------------------------------------
# Discovery

def discover_categories(page, start_urls: list[str]) -> list[str]:
    seen, queue, out = set(), list(start_urls), []

    def push(url: str):
        path = urlparse(url).path
        if path in seen:
            return
        if is_food_category(path):
            seen.add(path); queue.append(url)

    while queue:
        url = queue.pop(0)
        try:
            page.goto(url, timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            accept_cookies(page); quiesce_overlays(page)
        except TimeoutError:
            continue
        time.sleep(REQ_DELAY)

        out.append(url)

        for sel in ["nav a[href*='/e-selver/']","aside a[href*='/e-selver/']","a.category-card, a[href*='/e-selver/']"]:
            try:
                links = page.locator(sel)
                for i in range(min(500, links.count())):
                    href = links.nth(i).get_attribute("href")
                    if not href: continue
                    u = urljoin(BASE, href)
                    if is_food_category(urlparse(u).path):
                        push(u)
            except Exception:
                pass

    uniq, seen_paths = [], set()
    for u in out:
        p = urlparse(u).path
        if p not in seen_paths:
            uniq.append(u); seen_paths.add(p)
    return uniq

# ---------------------------------------------------------------------------
# Listing helpers (infinite-scroll + pagination)

def _candidate_hrefs_js(page) -> list[str]:
    """
    Prefer anchors inside main/product containers and outside header/footer/nav/aside.
    """
    try:
        return page.evaluate("""
          (function(){
            const inContent = Array.from(document.querySelectorAll(
              'main a[href], .Category a[href], .Products a[href], .product-list a[href], ' +
              '.productgrid a[href], [class*="Product"] a[href]'
            ));
            const good = inContent.length ? inContent : Array.from(document.querySelectorAll('a[href]'));
            return Array.from(new Set(
              good
                .filter(a => !a.closest('header,footer,nav,aside,[role="navigation"]'))
                .map(a => a.getAttribute('href') || '')
                .filter(Boolean)
            ));
          })()
        """)
    except Exception:
        return []

def _harvest_via_js(page) -> set[str]:
    """Fast DOM-wide href sweep; then filter to product-like."""
    hrefs = _candidate_hrefs_js(page)
    out: set[str] = set()
    for href in hrefs:
        if href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        u = urljoin(BASE, href)
        pu = urlparse(u)
        if (pu.netloc or urlparse(BASE).netloc).lower() not in ALLOWED_HOSTS:
            continue
        if _is_selver_product_like(u):
            out.add(u)
    return out

def collect_product_links(page, page_limit: int = 0) -> set[str]:
    """On a category page, infinite-scrolls and/or paginates to collect product links."""
    links: set[str] = set()
    pages_seen = 0

    def infinite_scroll(max_rounds=40, settle_rounds=3):
        nonlocal links
        last_count = -1
        same_count = 0
        for _ in range(max_rounds):
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
            try:
                page.wait_for_load_state("networkidle", timeout=4500)
            except Exception:
                pass
            time.sleep(0.35)
            accept_cookies(page); quiesce_overlays(page)
            links |= _harvest_via_js(page)

            if len(links) == last_count:
                same_count += 1
                if same_count >= settle_rounds:
                    break
            else:
                same_count = 0
                last_count = len(links)

    def next_selector():
        for sel in ["a[rel='next']","a[aria-label*='Next']",
                    "button:has-text('Näita rohkem')","button:has-text('Load more')",
                    "a.page-link:has-text('>')"]:
            if page.locator(sel).count() > 0:
                return sel
        return None

    while True:
        pages_seen += 1
        infinite_scroll()
        if page_limit and pages_seen >= page_limit:
            break
        nxt = next_selector()
        if not nxt:
            break
        try:
            page.locator(nxt).first.click(timeout=5000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(REQ_DELAY)
            accept_cookies(page); quiesce_overlays(page)
        except Exception:
            break

    # Debug peek
    if links:
        sample = list(sorted(links))[:5]
        print(f"[selver]   harvested {len(links)} product-like links; sample: {sample}")
    else:
        try:
            any_hrefs = _candidate_hrefs_js(page)[:12]
            print(f"[selver]   no product-like links; first anchors on page: {any_hrefs}")
        except Exception:
            print("[selver]   no product-like links; (JS href probe failed)")
    return links

# ---------------------------------------------------------------------------
# Page extraction helpers

def breadcrumbs(page) -> list[str]:
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

def extract_ean(page, url: str) -> str:
    labels = ["Ribakood","EAN","EAN-kood","EAN kood","GTIN"]
    for lab in labels:
        try:
            el = page.locator(f"xpath=//*[normalize-space()='{lab}']").first
            if el.count() > 0:
                nxt = el.locator("xpath=following::*[self::div or self::span or self::p][1]")
                if nxt.count() > 0:
                    txt = normspace(nxt.inner_text())
                    if re.fullmatch(r"\d{8,14}", txt): return txt
        except Exception:
            pass
    try:
        html = page.content()
        m = re.search(r"(?:Ribakood|EAN(?:-kood)?)\D*?(\d{8,14})", html, re.I)
        if m: return m.group(1)
    except Exception:
        pass
    return ""

# -------------------- JSON-LD helpers --------------------

def jsonld_all(page) -> list[dict]:
    out = []
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(scripts.count()):
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

def jsonld_pick_product(blocks: list[dict]) -> dict:
    for b in blocks:
        t = (b.get("@type") or "")
        t_low = t.lower() if isinstance(t, str) else ""
        if "product" in t_low or any(k in b for k in ("gtin13","gtin","sku")):
            return b
    return {}

def jsonld_pick_breadcrumbs(blocks: list[dict]) -> list[str]:
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
# Request router (block noisy 3P but never Selver)

def _router(route, request):
    try:
        url = request.url
        host = urlparse(url).netloc.lower()
        if _should_block(url) and not host.endswith("selver.ee"):
            return route.abort()
        return route.continue_()
    except Exception:
        return route.continue_()

# ---------------------------------------------------------------------------
# Main crawl

def crawl():
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    dbg_dir = "data/selver_debug"
    os.makedirs(dbg_dir, exist_ok=True)

    print(f"[selver] writing CSV -> {OUTPUT}")
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf"],
        )
        w.writeheader()

        with sync_playwright() as p:
            print("[selver] launching chromium (headless)")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
            )
            context.route("**/*", _router)

            page = context.new_page()
            page.set_default_navigation_timeout(15000)
            page.set_default_timeout(8000)

            # NOTE: .type / .text are properties in Playwright Python
            page.on("console", lambda m: print(f"[pw] {m.type}: {m.text}"))

            # ---- seeds
            print("[selver] collecting seeds…")
            seeds: list[str] = []
            if os.path.exists(CATEGORIES_FILE):
                with open(CATEGORIES_FILE, "r", encoding="utf-8") as cf:
                    for ln in cf:
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            seeds.append(urljoin(BASE, ln))
            if not seeds and safe_goto(page, urljoin(BASE, "/e-selver")):
                time.sleep(REQ_DELAY)
                top = set()
                for sel in ["a[href*='/e-selver/']","nav a[href*='/e-selver/']","aside a[href*='/e-selver/']"]:
                    try:
                        aa = page.locator(sel)
                        for i in range(aa.count()):
                            href = aa.nth(i).get_attribute("href")
                            if not href: continue
                            u = urljoin(BASE, href)
                            if is_food_category(urlparse(u).path):
                                top.add(u)
                    except Exception:
                        pass
                seeds = sorted(top)

            cats = discover_categories(page, seeds)
            print(f"[selver] Categories to crawl: {len(cats)}")
            for cu in cats: print(f"[selver]   {cu}")

            # ---- crawl categories -> collect product URLs
            product_urls: set[str] = set()
            for ci, cu in enumerate(cats, 1):
                if not safe_goto(page, cu):
                    try: page.screenshot(path=f"{dbg_dir}/cat_nav_fail_{ci}.png", full_page=True)
                    except Exception: pass
                    continue

                # Rare SPA bounce guard
                if page.url.startswith("about:blank"):
                    safe_goto(page, cu)

                time.sleep(REQ_DELAY)
                links = collect_product_links(page, page_limit=PAGE_LIMIT)
                if not links:
                    try: page.screenshot(path=f"{dbg_dir}/cat_empty_{ci}.png", full_page=True)
                    except Exception: pass

                product_urls.update(links)
                print(f"[selver] {cu} → +{len(links)} products (total so far: {len(product_urls)})")

            # ---- visit product pages -> write rows
            rows_written = 0
            for i, pu in enumerate(sorted(product_urls), 1):
                if not _is_selver_product_like(pu):
                    continue
                if not safe_goto(page, pu):
                    try: page.screenshot(path=f"{dbg_dir}/prod_nav_fail_{i}.png", full_page=True)
                    except Exception: pass
                    continue

                time.sleep(REQ_DELAY)

                # Prefer JSON-LD for robust data
                blocks = jsonld_all(page)
                prod_ld = jsonld_pick_product(blocks)
                crumbs_ld = jsonld_pick_breadcrumbs(blocks)

                name = normspace(prod_ld.get("name") or "") if prod_ld else ""
                if not name:
                    try:
                        name = normspace(page.locator("h1").first.inner_text())
                    except Exception:
                        name = ""

                # EAN/GTIN
                ean = ""
                if prod_ld:
                    ean = (prod_ld.get("gtin13") or prod_ld.get("gtin") or prod_ld.get("sku") or "")
                ean = re.sub(r"\D+", "", ean or "")
                if not ean:
                    ean = extract_ean(page, pu)

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

                crumbs = crumbs_ld or breadcrumbs(page)
                cat_path = " / ".join(crumbs); cat_leaf = crumbs[-1] if crumbs else ""
                size_text = guess_size_from_title(name)

                if not name:
                    try: page.screenshot(path=f"{dbg_dir}/prod_empty_{i}.png", full_page=True)
                    except Exception: pass
                    continue

                w.writerow({
                    "ext_id": pu, "name": name, "ean_raw": ean, "size_text": size_text,
                    "price": f"{price:.2f}", "currency": currency,
                    "category_path": cat_path, "category_leaf": cat_leaf,
                })
                rows_written += 1
                if (i % 25) == 0: f.flush()

            browser.close()

    print(f"[selver] wrote {rows_written} product rows.")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        crawl()
    except KeyboardInterrupt:
        pass

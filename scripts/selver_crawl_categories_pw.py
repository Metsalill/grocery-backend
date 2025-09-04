#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver category crawler → CSV (staging_selver_products)

Adds robust **brand** extraction in addition to the EAN/SKU hardening:
- JSON-LD: product.brand / manufacturer (string or object)
- itemprop/meta: brand/manufacturer
- DOM spec rows: "Bränd", "Tootja", "Kaubamärk", "Brand"
- Fallback (guarded): first brand-like token from H1 if all else fails

Also includes:
- resilient EAN (Ribakood) + SKU extraction
- SPA noise suppression, request routing and small navigation retries
- proceeds even if price widget fails (price=0.00)

CSV columns written:
  ext_id, source_url, name, brand, ean_raw, ean_norm, sku_raw,
  size_text, price, currency, category_path, category_leaf
"""

from __future__ import annotations
import os, re, csv, time, json
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

SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|l|ml|cl|dl)\b", re.I)

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
    m = SIZE_RE.search(title or "")
    if not m: return ""
    n,u = m.groups()
    return f"{n.replace(',', '.')} {u.lower()}"

def _strip_eselver_prefix(path: str) -> str:
    return path.replace("/e-selver", "", 1) if path.startswith("/e-selver/") else path

def _clean_abs(href: str) -> Optional[str]:
    if not href: return None
    url = urljoin(BASE, href)
    parts = urlsplit(url)
    host = (parts.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS: return None
    path = _strip_eselver_prefix(parts.path)
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", ""))

def canonical_from_page(page) -> Optional[str]:
    try:
        href = page.evaluate("""(d=>d.querySelector('link[rel="canonical"]')?.href||null)(document)""")
        if href: return _clean_abs(href)
    except Exception: pass
    try:
        return _clean_abs(page.url)
    except Exception:
        return None

def _in_allowlist(path: str) -> bool:
    if not STRICT_ALLOWLIST: return True
    p = (path or "/").rstrip("/")
    return any(p == root or p.startswith(root + "/") for root in STRICT_ALLOWLIST)

# ---- PDP detection ---------------------------------------------------------
def _is_selver_product_like(url: str) -> bool:
    u = urlparse(url)
    host = (u.netloc or urlparse(BASE).netloc).lower()
    if host not in ALLOWED_HOSTS: return False
    path = _strip_eselver_prefix((u.path or "/").lower())
    if path.startswith("/ru/"): return False
    if any(sn in path for sn in NON_PRODUCT_PATH_SNIPPETS): return False
    if any(kw in path for kw in NON_PRODUCT_KEYWORDS): return False
    if path.startswith("/toode/"): return True
    segs = [s for s in path.strip("/").split("/") if s]
    if len(segs) == 1:
        last = segs[0]
        if not re.fullmatch(r"[a-z0-9-]{3,}", last): return False
        if any(ch.isdigit() for ch in last): return True
        if re.search(r"(?:-|^)(?:kg|g|l|ml|cl|dl|tk|pk|pcs)$", last): return True
    return False

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

# ---------------------------------------------------------------------------
DIGITS_ONLY = re.compile(r"\D+")
def _digits(s: str) -> str: return DIGITS_ONLY.sub("", s or "")

def _valid_ean13(code: str) -> bool:
    if not re.fullmatch(r"\d{13}", code): return False
    s_odd  = sum(int(code[i]) for i in range(0, 12, 2))
    s_even = sum(int(code[i]) * 3 for i in range(1, 12, 2))
    chk = (10 - ((s_odd + s_even) % 10)) % 10
    return chk == int(code[-1])

# normalize to 8/13 when possible
def normalize_ean_digits(e: str) -> str:
    d = _digits(e)
    if len(d) in (8, 13):
        return d
    if len(d) == 14 and d[0] in ("0", "1"):
        return d[1:]
    if len(d) == 12 and _valid_ean13("0" + d):
        return "0" + d
    return ""

# --- Very robust DOM search for "Ribakood" / EAN and SKU ----------------
def _ean_sku_from_dom(page) -> tuple[str, str]:
    ean = ""
    sku = ""
    try:
        got = page.evaluate(
            """
            () => {
              const pickDigits = (txt) => {
                if (!txt) return null;
                const m = txt.replace(/\\s+/g,' ').match(/(\\d{8,14})/);
                return m ? m[1] : null;
              };

              const nodes = Array.from(document.querySelectorAll(
                'tr, .row, .product-attributes__row, .product-details__row, li, .attribute, .key-value, dl, dt, dd, .MuiGrid-root, div, span, p, th, td'
              ));

              let ean=null, sku=null;

              for (const row of nodes) {
                const txt = (row.textContent||'').replace(/\\s+/g,' ').trim();
                if (!txt) continue;

                if (!ean && /\\bribakood\\b/i.test(txt)) {
                  const d = pickDigits(txt);
                  if (d) ean = d;
                }

                if (!sku && /(\\bSKU\\b|\\bTootekood\\b|\\bArtikkel\\b)/i.test(txt)) {
                  const m = txt.match(/([A-Z0-9_-]{6,})/i);
                  if (m) sku = m[1];
                }

                if (ean && sku) break;
              }

              if (!ean) {
                const any = Array.from(document.querySelectorAll('div,span,p,li,td,dd,th'));
                for (const el of any) {
                  const t = (el.textContent||'').replace(/\\s+/g,' ');
                  if (/\\bribakood\\b/i.test(t)) {
                    const d = pickDigits(t);
                    if (d) { ean = d; break; }
                  }
                }
              }

              return { ean, sku };
            }
            """
        )
        if got:
            ean = got.get("ean") or ""
            sku = got.get("sku") or ""
    except Exception:
        pass

    if not ean:
        try:
            html = page.content()
            m = re.search(r"ribakood[\s\S]{0,400}?(\d{8,14})", html, re.I)
            if m:
                ean = m.group(1)
        except Exception:
            pass

    e13 = _digits(ean)
    if _valid_ean13(e13):
        ean = e13
    else:
        if not re.fullmatch(r"\d{8,14}", e13 or ""):
            ean = ""

    return ean, (sku or "")

def _pick_ean_from_html(html: str) -> str:
    if not html: return ""
    label_pat = re.compile(r"(?:\b(?:ean|gtin|ribakood|triipkood|barcode)\b)[^0-9]{0,200}([0-9]{8,14})", re.I | re.S)
    m = label_pat.search(html)
    return m.group(1) if m else ""

# ---------------------------------------------------------------------------
# Brand helpers

# Descriptor words that often attach to brands; we cut them if at the end.
_BRAND_TAILS = {
    "viilutatud","viilud","must","valge","originaal","täispiim","taispiim",
    "mitmevilja","rukkipala","pooltiivad","gluteenivaba","laktoosivaba",
    "klassikaline","klassik","kerg","light","tume","hele","ruks"
}
# Non-brand first tokens to avoid when falling back to H1.
_NONBRAND_FIRST = {
    "kookose","kookos","must","valge","originaal","viilutatud","täispiim","taispiim"
}
_SIZE_TOK = re.compile(r"^\d+(?:[.,]\d+)?\s*(?:kg|g|l|ml|cl|dl|tk|pk)\b", re.I)

def _canon_brand(b: str) -> str:
    b = normspace(b)
    if not b:
        return ""
    # Drop size-like fragments inside brand just in case.
    if _SIZE_TOK.search(b):
        return ""
    # If brand contains separators like "Brand / Line", keep left-most.
    b = re.split(r"\s+[\/|•·]\s+", b)[0].strip(" ,;–—-")
    toks = [t for t in b.split() if t]
    # Remove tail descriptors
    while toks and toks[-1].lower() in _BRAND_TAILS:
        toks.pop()
    # Max 2 tokens to avoid "Fazer Must", "Leibur Rukkipala" → keep base.
    if len(toks) > 2:
        toks = toks[:2]
    b = " ".join(toks).strip(" -–—·,;")
    return b if 1 <= len(b) <= 60 else ""

_BRAND_KEY_RE = re.compile(r"\b(bränd|brand|tootja|kaubamärk)\b", re.I)

def _extract_brand_from_dom_texts(texts: List[str]) -> str:
    for t in texts:
        if not t or len(t) < 3:
            continue
        if _BRAND_KEY_RE.search(t):
            parts = t.split(":", 1)
            tail = parts[1] if len(parts) == 2 else re.sub(_BRAND_KEY_RE, "", t, count=1, flags=re.I)
            cand = normspace(tail)
            cand = re.split(r"\b(Ribakood|SKU|Tootekood|Artikkel)\b", cand, maxsplit=1, flags=re.I)[0].strip()
            cand = _canon_brand(cand)
            if cand:
                return cand
    return ""

def extract_brand(page, prod_ld: dict) -> str:
    # 1) JSON-LD brand/manufacturer
    if prod_ld:
        b = prod_ld.get("brand") or prod_ld.get("manufacturer")
        if isinstance(b, dict):
            name = _canon_brand(str(b.get("name") or ""))
            if name:
                return name
        elif isinstance(b, str):
            name = _canon_brand(b)
            if name:
                return name

    # 2) itemprop/meta brand/manufacturer
    try:
        got = page.evaluate("""
        () => {
          const sel = [
            '[itemprop="brand"]','meta[itemprop="brand"]',
            '[itemprop="manufacturer"]','meta[itemprop="manufacturer"]'
          ];
          for (const s of sel) {
            const el = document.querySelector(s);
            if (!el) continue;
            const v = el.getAttribute('content') || el.textContent || '';
           

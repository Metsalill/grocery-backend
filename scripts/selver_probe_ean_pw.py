# scripts/selver_probe_ean_pw.py
# Usage:
#   python scripts/selver_probe_ean_pw.py data/prisma_eans_to_probe.csv data/selver_probe.csv 0.4
# Input CSV must have a header with a column named "ean".
# Output CSV columns: ext_id,url,ean,name,price_raw,size_text

from __future__ import annotations
import csv, json, os, re, sys, time
from pathlib import Path
from typing import Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

EAN_DIGITS = re.compile(r"\d{8,14}")
JSON_EAN = re.compile(r'"(?:gtin14|gtin13|gtin|ean|barcode|sku)"\s*:\s*"(?P<d>\d{8,14})"', re.I)
LABEL_EAN = re.compile(r"\b(ribakood|ean|barcode)\b", re.I)

def norm_ean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = re.sub(r"\D", "", s)
    return d or None

def _first_text(page, selectors: list[str], timeout_ms: int = 2000) -> Optional[str]:
    for sel in selectors:
        try:
            t = page.locator(sel).first.inner_text(timeout=timeout_ms)
            if t:
                t = t.strip()
                if t:
                    return t
        except Exception:
            pass
    return None

def _meta_content(page, selectors: list[str]) -> Optional[str]:
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                v = el.get_attribute("content", timeout=1000)
                if v:
                    v = v.strip()
                    if v:
                        return v
        except Exception:
            pass
    return None

def parse_ld_product(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (name, price_raw, ean_found) from JSON-LD if present."""
    try:
        data = json.loads(text.strip())
    except Exception:
        return None, None, None

    def pick(d):
        if isinstance(d, list) and d:
            return d[0]
        return d

    data = pick(data)
    if not isinstance(data, dict):
        return None, None, None

    # Try to locate a Product object anywhere in the structure
    def is_product(d: dict) -> bool:
        t = d.get("@type")
        return isinstance(t, str) and t.lower() == "product"

    if not is_product(data):
        for v in data.values():
            p = pick(v)
            if isinstance(p, dict) and is_product(p):
                data = p
                break

    if not is_product(data):
        return None, None, None

    name = (data.get("name") or "").strip() or None
    ean = norm_ean(
        data.get("gtin14") or data.get("gtin13") or data.get("gtin") or data.get("ean") or data.get("sku")
    )
    price = None
    offers = data.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    if isinstance(offers, dict):
        price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")

    return name, (str(price).strip() if price is not None else None), ean

def looks_like_pdp(page) -> bool:
    """Heuristic: true if we're on a product page."""
    try:
        og_type = _meta_content(page, ["meta[property='og:type']"])
        if (og_type or "").lower() == "product":
            return True
    except Exception:
        pass
    try:
        if page.locator("text=Ribakood").count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.locator("meta[itemprop='sku'], meta[itemprop='gtin'], meta[itemprop='gtin13']").count() > 0:
            return True
    except Exception:
        pass
    return False

def goto_first_result(page) -> None:
    """
    On a search results page, open the first product result.
    Selver uses plain slugs (no /toode/), so we just find the first
    product tile link that isn't another search link.
    """
    candidates = [
        "article a[href^='/']:not([href*='/search'])",
        ".product-list a[href^='/']:not([href*='/search'])",
        "a[href^='/']:not([href*='/search'])",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc and loc.count() > 0:
                href = loc.get_attribute("href")
                if href:
                    if not href.startswith("http"):
                        href = "https://www.selver.ee" + href
                    page.goto(href, wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass
                    return
        except Exception:
            pass

def extract_ean_on_pdp(page) -> Optional[str]:
    """Find an EAN on a Selver PDP using multiple strategies."""
    # 1) JSON-LD
    try:
        scripts = page.locator("script[type='application/ld+json']")
        cnt = scripts.count()
        for i in range(cnt):
            try:
                n, p, g = parse_ld_product(scripts.nth(i).inner_text())
                if g:
                    return norm_ean(g)
            except Exception:
                pass
    except Exception:
        pass

    # 2) meta itemprop
    meta_val = _meta_content(page, [
        "meta[itemprop='gtin13']",
        "meta[itemprop='gtin']",
        "meta[itemprop='sku']",
    ])
    if meta_val:
        d = norm_ean(meta_val)
        if d:
            return d

    # 3) any script JSON blob containing gtin/ean/barcode
    try:
        html = page.content()
        m = JSON_EAN.search(html or "")
        if m:
            return m.group("d")
    except Exception:
        pass

    # 4) visible text near label “Ribakood / EAN / Barcode”
    try:
        body_text = page.locator("body").inner_text(timeout=2000)
        if LABEL_EAN.search(body_text or ""):
            nums = EAN_DIGITS.findall(body_text or "")
            nums = sorted(nums, key=len, reverse=True)  # prefer 13/14
            if nums:
                return nums[0]
    except Exception:
        pass

    return None

def probe_one(page, ean: str, delay: float, dbg_rows: list) -> Optional[dict]:
    wanted = norm_ean(ean)
    if not wanted:
        return None

    search_url = f"https://www.selver.ee/search?q={wanted}"
    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
    except PWTimeout:
        dbg_rows.append({"ean": wanted, "stage": "goto_search_timeout", "url": search_url, "note": ""})
        return None

    # If not already on PDP, open the first result
    if not looks_like_pdp(page):
        goto_first_result(page)

    if not looks_like_pdp(page):
        dbg_rows.append({"ean": wanted, "stage": "no_pdp", "url": page.url, "note": ""})
        return None

    # Extract name/price with robust fallbacks
    name = _first_text(page, ["h1", "[data-testid='product-title']"])
    if not name:
        name = _meta_content(page, ["meta[property='og:title']"])
    if name:
        name = name.strip()

    price_raw = _first_text(page, [
        "[data-testid='product-price']",
        "[itemprop='price']",
        ".price, .product-price, .product__price"
    ]) or _meta_content(page, ["meta[itemprop='price']"])

    found_ean = extract_ean_on_pdp(page)

    # Guards: real name (not 'Selver'), EAN must match search
    if not name or name.lower() in {"selver", "e-selver"} or len(name) < 3:
        dbg_rows.append({"ean": wanted, "stage": "bad_name", "url": page.url, "note": name or ""})
        return None

    if norm_ean(found_ean) != wanted:
        dbg_rows.append({"ean": wanted, "stage": "ean_mismatch", "url": page.url, "note": f"found={found_ean}"})
        return None

    if delay > 0:
        time.sleep(delay)

    return {
        "ext_id": page.url,
        "url": page.url,
        "ean": wanted,
        "name": name,
        "price_raw": price_raw or "",
        "size_text": "",
    }

def main(in_csv: str, out_csv: str, delay: float):
    in_path, out_path = Path(in_csv), Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    DEBUG = os.getenv("DEBUG_PROBE", "").lower() in ("1", "true", "yes")
    dbg_rows: list[dict] = []

    with in_path.open("r", newline="", encoding="utf-8") as fin, \
         out_path.open("w", newline="", encoding="utf-8") as fout, \
         sync_playwright() as p:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=["ext_id","url","ean","name","price_raw","size_text"])
        writer.writeheader()

        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for row in reader:
            ean = (row.get("ean") or "").strip()
            if not ean:
                continue
            try:
                info = probe_one(page, ean, delay, dbg_rows)
                if info:
                    writer.writerow(info)
            except Exception as ex:
                dbg_rows.append({"ean": ean, "stage": "exception", "url": page.url if page else "", "note": str(ex)})

        context.close()
        browser.close()

    if DEBUG:
        # Save reasoned skips for inspection
        dbg_path = out_path.parent / "selver_probe_debug.csv"
        with dbg_path.open("w", newline="", encoding="utf-8") as fdbg:
            fields = ["ean", "stage", "url", "note"]
            w = csv.DictWriter(fdbg, fieldnames=fields)
            w.writeheader()
            for r in dbg_rows:
                w.writerow(r)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/selver_probe_ean_pw.py <in.csv> <out.csv> [delay_seconds]")
        sys.exit(2)
    delay = float(sys.argv[3]) if len(sys.argv) >= 4 else 0.4
    main(sys.argv[1], sys.argv[2], delay)

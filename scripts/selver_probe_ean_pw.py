# scripts/selver_probe_ean_pw.py
# Usage:
#   python scripts/selver_probe_ean_pw.py data/prisma_eans_to_probe.csv data/selver_probe.csv 0.4
# Input CSV must have a header with a column named "ean".
# Output CSV columns: ext_id,url,ean,name,price_raw,size_text

from __future__ import annotations
import csv, json, re, sys, time
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
            v = page.locator(sel).first.get_attribute("content", timeout=1000)
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
    if (data.get("@type") or "").lower() != "product":
        for v in data.values():
            p = pick(v)
            if isinstance(p, dict) and (p.get("@type") or "").lower() == "product":
                data = p
                break

    if (data.get("@type") or "").lower() != "product":
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

def extract_ean_on_pdp(page) -> Optional[str]:
    """Find an EAN on a Selver PDP using multiple strategies."""
    # 1) JSON-LD
    try:
        scripts = page.locator("script[type='application/ld+json']")
        count = scripts.count()
        for i in range(count):
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
        # grab a chunk of body text; cheap heuristic
        body_text = page.locator("body").inner_text(timeout=2000)
        if LABEL_EAN.search(body_text or ""):
            nums = EAN_DIGITS.findall(body_text or "")
            # prefer 13/14 digit hits
            nums = sorted(nums, key=len, reverse=True)
            if nums:
                return nums[0]
    except Exception:
        pass

    return None

def probe_one(page, ean: str, delay: float) -> Optional[dict]:
    wanted = norm_ean(ean)
    if not wanted:
        return None

    search_url = f"https://www.selver.ee/search?q={wanted}"
    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=10000)
    except PWTimeout:
        return None

    # If search didn’t land on PDP, click first product
    if "/toode/" not in page.url:
        try:
            link = page.locator("a[href*='/toode/']").first
            if link and link.is_visible():
                href = link.get_attribute("href")
                if href:
                    if not href.startswith("http"):
                        href = "https://www.selver.ee" + href
                    page.goto(href, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass

    if "/toode/" not in page.url:
        return None

    # Extract name/price with robust fallbacks
    name = _first_text(page, ["h1", "[data-testid='product-title']"]) \
        or _first_text(page, ["meta[property='og:title']"], 1000)
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
        return None
    if norm_ean(found_ean) != wanted:
        return None

    # small pacing so we don’t hammer
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
                info = probe_one(page, ean, delay)
                if info:
                    writer.writerow(info)
            except Exception:
                # be resilient – skip any transient errors
                pass

        context.close()
        browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/selver_probe_ean_pw.py <in.csv> <out.csv> [delay_seconds]")
        sys.exit(2)
    delay = float(sys.argv[3]) if len(sys.argv) >= 4 else 0.4
    main(sys.argv[1], sys.argv[2], delay)

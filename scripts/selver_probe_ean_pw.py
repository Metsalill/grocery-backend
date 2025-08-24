# scripts/selver_probe_ean_pw.py
#
# Usage:
#   python scripts/selver_probe_ean_pw.py data/prisma_eans_to_probe.csv data/selver_probe.csv 0.4
#
# Input CSV must have a header with a column named "ean".
# Output CSV columns: ext_id,url,ean,name,price_raw,size_text
#
from __future__ import annotations
import csv, json, re, sys, time
from pathlib import Path
from typing import Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

EAN_RE = re.compile(r"\b(\d{8,14})\b")  # allow 8–14, we’ll normalize to digits

def norm_ean(s: str | None) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits or None

def parse_ld_product(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return (name, price_raw, ean_found) from JSON-LD if present
    """
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

    # Many shops nest the Product object
    if data.get("@type") != "Product":
        for k, v in data.items():
            if isinstance(v, (list, dict)):
                pp = pick(v)
                if isinstance(pp, dict) and pp.get("@type") == "Product":
                    data = pp
                    break

    if data.get("@type") != "Product":
        return None, None, None

    name = (data.get("name") or "").strip() or None
    ean = norm_ean(data.get("gtin14") or data.get("gtin13") or data.get("gtin") or data.get("sku"))
    price = None
    offers = data.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    if isinstance(offers, dict):
        price = offers.get("price") or offers.get("priceSpecification", {}).get("price")

    return name, (str(price).strip() if price is not None else None), ean

def probe_one(page, ean: str, delay: float) -> Optional[dict]:
    """
    Go to Selver search by EAN -> if a PDP found and its EAN matches the requested one,
    return a dict with product info. Otherwise return None (skip).
    """
    wanted = norm_ean(ean)
    if not wanted:
        return None

    search_url = f"https://www.selver.ee/search?q={wanted}"
    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
    except PWTimeout:
        return None

    # If search redirects to a PDP or there’s a PDP link, open it
    url = page.url
    if "/toode/" not in url:
        # Try to click the first product card link
        link = None
        for sel in [
            "a[href*='/toode/']",
            "a.product-card, a[href*='/en/toode/']",
        ]:
            try:
                link = page.locator(sel).first
                if link and link.is_visible():
                    url = link.get_attribute("href")
                    if url:
                        if not url.startswith("http"):
                            url = "https://www.selver.ee" + url
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        break
            except Exception:
                pass

    # If still not on a product page, give up
    if "/toode/" not in page.url:
        return None

    # Try JSON-LD first
    name = price_raw = found_ean = None
    try:
        scripts = page.locator("script[type='application/ld+json']")
        for i in range(scripts.count()):
            text = scripts.nth(i).inner_text()
            n, p, g = parse_ld_product(text)
            name = name or n
            price_raw = price_raw or p
            found_ean = found_ean or g
    except Exception:
        pass

    # Fallbacks
    if not name:
        try:
            name = (page.locator("h1").first.inner_text(timeout=2000) or "").strip() or None
        except Exception:
            pass
    if not price_raw:
        for sel in [
            "[data-testid='product-price']",
            ".price, .product-price, .product__price"
        ]:
            try:
                txt = page.locator(sel).first.inner_text(timeout=1500)
                if txt:
                    price_raw = txt.strip()
                    break
            except Exception:
                pass
    if not found_ean:
        # Scan visible text for something that looks like an EAN
        try:
            full = page.locator("body").inner_text(timeout=2000)
            m = EAN_RE.search(full or "")
            if m:
                found_ean = norm_ean(m.group(1))
        except Exception:
            pass

    # Hard guards:
    #  - need a real name (not just 'Selver', > 3 chars)
    #  - EAN from PDP must match exactly the requested digits
    if not name or name.strip().lower() in {"selver", "e-selver"} or len(name.strip()) < 3:
        return None
    if norm_ean(found_ean) != wanted:
        return None

    time.sleep(delay)

    return {
        "ext_id": page.url,
        "url": page.url,
        "ean": wanted,
        "name": name.strip(),
        "price_raw": price_raw or "",
        "size_text": "",  # optional; can be parsed later
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
                # be resilient – skip any bad EANs / transient errors
                pass

        context.close()
        browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/selver_probe_ean_pw.py <in.csv> <out.csv> [delay_seconds]")
        sys.exit(2)
    delay = float(sys.argv[3]) if len(sys.argv) >= 4 else 0.4
    main(sys.argv[1], sys.argv[2], delay)

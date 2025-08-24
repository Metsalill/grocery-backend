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
from typing import Optional, Tuple, Iterable
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

EAN_RE = re.compile(r"\b(\d{8,14})\b")  # allow 8–14, normalize to digits
SIZE_RE = re.compile(r"\b(\d+(?:[.,]\d+)?)\s?(?:ml|l|cl|g|kg)\b", re.I)

def norm_ean(s: str | None) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits or None

def ean_variants(ean13: str) -> Iterable[str]:
    """Try common forms: original, drop-first-0 (UPC-A), strip all leading zeros."""
    cand = []
    if ean13:
        cand.append(ean13)
        if ean13.startswith("0") and len(ean13) >= 2:
            cand.append(ean13[1:])
        stripped = ean13.lstrip("0")
        if stripped and stripped not in cand:
            cand.append(stripped)
    # de-dup while keeping order
    out, seen = [], set()
    for c in cand:
        if c and c not in seen:
            seen.add(c); out.append(c)
    return out

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
        for v in data.values():
            pv = pick(v)
            if isinstance(pv, dict) and pv.get("@type") == "Product":
                data = pv
                break

    if data.get("@type") != "Product":
        return None, None, None

    name = (data.get("name") or "").strip() or None
    ean = norm_ean(
        data.get("gtin14")
        or data.get("gtin13")
        or data.get("gtin")
        or data.get("ean")
        or data.get("sku")
    )
    price = None
    offers = data.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    if isinstance(offers, dict):
        price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")

    return name, (str(price).strip() if price is not None else None), ean

def safe_inner_text(page, selector: str, timeout_ms: int = 2000) -> Optional[str]:
    try:
        txt = page.locator(selector).first.inner_text(timeout=timeout_ms)
        if txt:
            t = txt.strip()
            return t if t else None
    except Exception:
        pass
    return None

def extract_size_text(name: str | None, page) -> str:
    # Prefer a size from the name; fallback to page text
    if name:
        m = SIZE_RE.search(name)
        if m:
            return m.group(0)
    try:
        body = page.locator("body").inner_text(timeout=1500) or ""
        m = SIZE_RE.search(body)
        if m:
            return m.group(0)
    except Exception:
        pass
    return ""

def is_pdp_url(url: str) -> bool:
    u = (url or "").lower()
    return "/toode/" in u or "/en/toode/" in u

def probe_one(page, ean: str, delay: float) -> Optional[dict]:
    """
    Try EAN variants on Selver search.
    Success = we land on a PDP and the PDP's EAN equals the original EAN digits.
    """
    wanted = norm_ean(ean)
    if not wanted:
        return None

    for q in ean_variants(wanted):
        search_url = f"https://www.selver.ee/search?q={q}"
        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
        except PWTimeout:
            continue

        # If search did not redirect to PDP, click first product card link if present.
        if not is_pdp_url(page.url):
            clicked = False
            for sel in [
                "a[href*='/toode/']",
                "[data-testid='product-card'] a[href]",
                "a.product-card",
            ]:
                try:
                    loc = page.locator(sel).first
                    if loc and loc.count() and loc.is_visible():
                        href = loc.get_attribute("href")
                        if href:
                            if not href.startswith("http"):
                                href = "https://www.selver.ee" + href
                            page.goto(href, wait_until="domcontentloaded", timeout=30000)
                            clicked = True
                            break
                except Exception:
                    pass
            if not clicked and not is_pdp_url(page.url):
                # no product cards → try next variant
                continue

        # Must be on a product page now
        if not is_pdp_url(page.url):
            continue

        # JSON-LD first
        name = price_raw = found_ean = None
        try:
            scripts = page.locator("script[type='application/ld+json']")
            cnt = scripts.count()
            for i in range(cnt):
                text = scripts.nth(i).inner_text()
                n, p, g = parse_ld_product(text)
                name = name or n
                price_raw = price_raw or p
                found_ean = found_ean or g
        except Exception:
            pass

        # Fallbacks
        if not name:
            name = safe_inner_text(page, "h1", 2000)

        if not price_raw:
            for sel in [
                "[data-testid='product-price']",
                ".product-price",
                ".product__price",
                ".price",
            ]:
                price_raw = safe_inner_text(page, sel, 1500)
                if price_raw:
                    break

        if not found_ean:
            # Scan visible text for something that looks like an EAN
            try:
                full = page.locator("body").inner_text(timeout=2000) or ""
                m = EAN_RE.search(full)
                if m:
                    found_ean = norm_ean(m.group(1))
            except Exception:
                pass

        # Guards
        if not name or name.strip().lower() in {"selver", "e-selver"} or len(name.strip()) < 3:
            continue
        if norm_ean(found_ean) != wanted:
            # EAN from PDP must match the original wanted digits
            continue

        time.sleep(delay)

        return {
            "ext_id": page.url,
            "url": page.url,
            "ean": wanted,              # keep the canonical wanted EAN
            "name": name.strip(),
            "price_raw": price_raw or "",
            "size_text": extract_size_text(name, page),
        }

    # No variant succeeded
    return None

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

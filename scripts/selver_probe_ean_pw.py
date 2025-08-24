# scripts/selver_probe_ean_pw.py
# Usage:
#   python scripts/selver_probe_ean_pw.py data/prisma_eans_to_probe.csv data/selver_probe.csv 0.4
from __future__ import annotations
import csv, json, re, sys, time
from pathlib import Path
from typing import Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

EAN_RE = re.compile(r"\b(\d{8,14})\b")

def norm_ean(s: str | None) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits or None

def _pick_one(x):
    if isinstance(x, list) and x:
        return x[0]
    return x

def _find_gtin_anywhere(d: dict) -> Optional[str]:
    # Try common places and then walk additionalProperty
    for key in ("gtin14", "gtin13", "gtin", "sku", "mpn", "identifier"):
        v = d.get(key)
        v = (v if not isinstance(v, list) else (v[0] if v else None))
        e = norm_ean(v if isinstance(v, str) else None)
        if e:
            return e
    ap = d.get("additionalProperty") or d.get("additionalProperties")
    if isinstance(ap, list):
        for item in ap:
            if not isinstance(item, dict): continue
            name = (item.get("name") or "").lower()
            val  = item.get("value") or item.get("propertyID")
            if "ean" in name or "gtin" in name:
                e = norm_ean(str(val))
                if e:
                    return e
    return None

def parse_ld_product(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        data = json.loads(text.strip())
    except Exception:
        return None, None, None
    data = _pick_one(data)
    if not isinstance(data, dict):
        return None, None, None
    # Find a Product object anywhere
    node = None
    stack = [data]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if (cur.get("@type") == "Product") or (isinstance(cur.get("@type"), list) and "Product" in cur.get("@type")):
                node = cur; break
            for v in cur.values():
                if isinstance(v, (dict, list)): stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    if not node:
        return None, None, None
    name = (node.get("name") or "").strip() or None
    price = None
    offers = node.get("offers")
    offers = _pick_one(offers)
    if isinstance(offers, dict):
        price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
    gtin = _find_gtin_anywhere(node)
    return name, (str(price).strip() if price is not None else None), gtin

def _page_contains_ean_html(page, wanted: str) -> bool:
    try:
        html = page.content()
        # look for exact digits anywhere in HTML (covers JSON-LD, dataLayer, hidden attrs)
        return bool(re.search(rf"\b{re.escape(wanted)}\b", html))
    except Exception:
        return False

def probe_one(page, ean: str, delay: float) -> Optional[dict]:
    wanted = norm_ean(ean)
    if not wanted:
        return None

    search_url = f"https://www.selver.ee/search?q={wanted}"
    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
    except PWTimeout:
        return None

    # Click first product card if still on search
    if "/toode/" not in page.url:
        try:
            link = page.locator("a[href*='/toode/']").first
            if link and link.count() > 0:
                href = link.get_attribute("href")
                if href:
                    if not href.startswith("http"):
                        href = "https://www.selver.ee" + href
                    page.goto(href, wait_until="domcontentloaded", timeout=30000)
        except Exception:
            pass

    if "/toode/" not in page.url:
        return None

    # Try JSON-LD
    name = price_raw = found_ean = None
    try:
        scripts = page.locator("script[type='application/ld+json']")
        n_scripts = scripts.count()
        for i in range(n_scripts):
            text = scripts.nth(i).inner_text()
            n, p, g = parse_ld_product(text)
            name = name or n
            price_raw = price_raw or p
            found_ean = found_ean or g
    except Exception:
        pass

    # Meta/itemprop/data-* fallbacks for EAN
    if not found_ean:
        for sel in [
            "meta[itemprop='gtin13']",
            "[itemprop='gtin13']",
            "[data-gtin]",
            "[data-product-gtin]"
        ]:
            try:
                el = page.locator(sel).first
                if el and el.count() > 0:
                    raw = el.get_attribute("content") or el.get_attribute("value") or el.inner_text()
                    e = norm_ean(raw or "")
                    if e:
                        found_ean = e
                        break
            except Exception:
                pass

    # Visible fallbacks
    if not name:
        try:
            name = (page.locator("h1").first.inner_text(timeout=2000) or "").strip() or None
        except Exception:
            pass
    if not price_raw:
        for sel in ["[data-testid='product-price']", ".price, .product-price, .product__price"]:
            try:
                txt = page.locator(sel).first.inner_text(timeout=1500)
                if txt:
                    price_raw = txt.strip()
                    break
            except Exception:
                pass

    # Last resort: if HTML contains the requested digits, accept as matched EAN
    if not found_ean and _page_contains_ean_html(page, wanted):
        found_ean = wanted

    # Guards
    if not name or name.strip().lower() in {"selver", "e-selver"} or len(name.strip()) < 3:
        return None
    if norm_ean(found_ean) != wanted:
        return None

    time.sleep(delay)  # be polite

    return {
        "ext_id": page.url,
        "url": page.url,
        "ean": wanted,
        "name": name.strip(),
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
                # skip transient issues
                pass
        context.close()
        browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/selver_probe_ean_pw.py <in.csv> <out.csv> [delay_seconds]")
        sys.exit(2)
    delay = float(sys.argv[3]) if len(sys.argv) >= 4 else 0.4
    main(sys.argv[1], sys.argv[2], delay)

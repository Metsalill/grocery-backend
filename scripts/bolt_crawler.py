#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bolt Food — store category crawler (Coop / Wolt-style venues)
Fixes: deep-link redirects to /et-EE/<city> by navigating via the store page UI.
Tested headless and non-headless.

Usage example:
  python bolt_crawler.py \
    --categories-file kvartali-coop-maksimarket.txt \
    --out out/coop_kvartali_bolt.csv \
    --headless 0 \
    --req-delay 0.45
"""
import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PRICE_RE = re.compile(r"(\d+([.,]\d{1,2})?)\s*€")
SPACE_RE = re.compile(r"\s+")
SMC_ID_RE = re.compile(r"/smc/(\d+)")
CITY_RE = re.compile(r"/et-EE/([^/]+)/p/(\d+)")
CATEGORY_NAME_Q = "categoryName"


@dataclass
class Product:
    category: str
    name: str
    price_eur: float
    unit_text: str
    image: str
    url: str
    store_url: str
    city_slug: str
    venue_id: str
    raw: Dict


def norm_space(s: str) -> str:
    return SPACE_RE.sub(" ", s or "").strip()


def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = PRICE_RE.search(text.replace("\xa0", " "))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def extract_city_and_venue(url: str) -> Tuple[str, str]:
    """
    Returns (city_slug, venue_id) from a store URL like:
    https://food.bolt.eu/et-EE/2-tartu/p/1969
    """
    m = CITY_RE.search(url)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def parse_categories_file(path: str) -> List[Tuple[str, str]]:
    """
    Accepts lines either as:
      Category Name -> https://food.bolt.eu/et-EE/2-tartu/p/1969/smc/<id>?categoryName=Piim&backPath=%2Fp%2F1969
    or:
      https://food.bolt.eu/et-EE/2-tartu/p/1969/smc/<id>?categoryName=Piim&backPath=%2Fp%2F1969
    """
    out: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "->" in line:
                name, href = [x.strip() for x in line.split("->", 1)]
                out.append((name, href))
            else:
                href = line
                # derive category name from query param if present
                name = parse_qs(urlparse(href).query).get(CATEGORY_NAME_Q, [""])[0]
                if not name:
                    # fallback: last path segment or empty
                    name = " ".join(urlparse(href).path.split("/")[-1].split("-")).strip() or "Unknown"
                out.append((name, href))
    return out


# ---------------------- Playwright helpers ---------------------- #

def dismiss_popups(page) -> None:
    """
    Close cookie banners, sign-in nags, or app-download overlays when they appear.
    """
    selectors = [
        'button:has-text("Nõustun")',
        'button:has-text("Luban kõik")',
        'button:has-text("Luba kõik")',
        'button:has-text("OK")',
        'button:has-text("Accept")',
        'button:has-text("Got it")',
        '[data-testid="cookie-accept-all"]',
        '[aria-label="Close"]',
        'button[aria-label="close"]',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=500)
                time.sleep(0.2)
        except Exception:
            pass


def click_category_chip(page, category_name: str) -> bool:
    """
    Clicks the category chip/tab by visible text.
    """
    if not category_name:
        return False

    # Try a11y role first
    try:
        chip = page.get_by_role("link", name=re.compile(rf"^{re.escape(category_name)}\b", re.I))
        if chip.count():
            chip.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass

    # Try text contains
    try:
        chip = page.locator(f'//a[contains(normalize-space(.), "{category_name}")]')
        if chip.count():
            chip.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass

    return False


def open_first_category_from_hc(page) -> bool:
    """
    When landing to a holding/collection page (hc) that shows a list of categories,
    open the first visible category.
    """
    try:
        anchors = page.locator('a[href*="/smc/"]')
        if anchors.count():
            anchors.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass
    return False


def wait_for_grid(page, timeout: int = 20000) -> None:
    """
    Wait until product grid/tiles render.
    """
    candidates = [
        '[data-testid="product-card"]',
        '[data-test="product-card"]',
        '[data-testid="productTile"]',
        '[data-test="productTile"]',
        # generic card items
        'div:has(> div >> text=/€/)',
        'article:has-text("€")',
    ]
    start = time.time()
    last_err = None
    for _ in range(50):
        for sel in candidates:
            try:
                loc = page.locator(sel)
                if loc.count():
                    return
            except Exception as e:
                last_err = e
        if (time.time() - start) * 1000 > timeout:
            break
        time.sleep(0.2)
    if last_err:
        raise PWTimeout(str(last_err))
    raise PWTimeout("product grid not found")


def auto_scroll(page, max_steps: int = 60, pause: float = 0.25) -> None:
    """
    Simple incremental scroll to load lazy content.
    """
    page.evaluate(
        """
        (steps, pause) => new Promise(async (res) => {
          const sleep = (ms) => new Promise(r => setTimeout(r, ms));
          for (let i=0; i<steps; i++) {
            window.scrollBy(0, Math.round(window.innerHeight * 0.9));
            await sleep(pause * 1000);
          }
          res();
        })
        """,
        max_steps,
        pause,
    )


def extract_tiles_runtime(page) -> List[Dict]:
    """
    Extract tiles via DOM traversal. Returns a list of dicts with basic fields.
    We attempt to find each card's name, price, unit text, image, and link.
    """
    tiles = page.evaluate(
        """
        () => {
          const result = [];
          const cards = Array.from(document.querySelectorAll(
            '[data-testid="product-card"],[data-test="product-card"],[data-testid="productTile"],[data-test="productTile"], article, div'
          )).filter(el => /€/.test(el.textContent || ''));
          const seen = new Set();
          for (const el of cards) {
            try {
              const txt = (el.textContent || "").replace(/\\s+/g, " ").trim();
              if (!/€/.test(txt)) continue;
              // name: try specific selectors, else generic strong/span/div lines
              let nameEl =
                el.querySelector('[data-testid="product-name"],[data-test="product-name"]') ||
                el.querySelector('h3,h4,strong') ||
                el.querySelector('div[title]');
              let name = nameEl ? (nameEl.getAttribute('title') || nameEl.textContent || '') : '';
              name = name.replace(/\\s+/g, ' ').trim();
              if (!name) {
                // heuristics: take first line before price
                const parts = txt.split('€')[0].trim();
                name = parts.split(' + ')[0].trim();
              }
              if (!name) continue;

              // price: grab closest price-looking token
              const priceEl =
                el.querySelector('[data-testid="product-price"],[data-test="product-price"]') ||
                el.querySelector('span,div');
              let priceText = priceEl ? priceEl.textContent || '' : txt;
              priceText = priceText.replace(/\\s+/g, ' ').trim();
              const m = priceText.match(/(\\d+(?:[.,]\\d{1,2})?)\\s*€/);
              if (!m) continue;
              const price = m[1];

              // image
              let imgEl = el.querySelector('img');
              let img = imgEl ? (imgEl.getAttribute('src') || imgEl.getAttribute('data-src') || '') : '';

              // link
              let linkEl = el.closest('a') || el.querySelector('a[href*="/p/"]') || el.querySelector('a[href*="/smc/"]');
              let href = linkEl ? linkEl.getAttribute('href') : '';

              const key = name + '|' + price + '|' + img;
              if (seen.has(key)) continue;
              seen.add(key);

              result.push({
                name,
                price_text: price + ' €',
                unit_text: '',
                image: img || '',
                href: href || '',
                text: txt,
              });
            } catch {}
          }
          return result;
        }
        """
    )
    return tiles or []


def ensure_on_store_page(page, base_url: str, req_delay: float = 0.3) -> None:
    """Guarantee we're on the store root (/p/<venue>) before opening a category."""
    if not base_url:
        return
    try:
        if "/p/" not in (page.url or "") or page.url.split("?")[0] != base_url:
            page.goto(base_url, timeout=60_000, wait_until="domcontentloaded")
            time.sleep(req_delay)
            dismiss_popups(page)
    except Exception:
        # try once more with a normal goto
        page.goto(base_url, timeout=60_000)
        time.sleep(req_delay)
        dismiss_popups(page)


def open_category_via_page(page, base_url: str, href: str, cat_name: str, req_delay: float = 0.3) -> bool:
    """
    Prefer clicking the exact <a href=".../smc/<id>?categoryName=..."> that exists on the store page.
    Falls back to clicking the chip by visible text.
    """
    ensure_on_store_page(page, base_url, req_delay)

    # Try exact href match first
    try:
        locator = page.locator(f'a[href="{href}"]')
        if locator.count():
            locator.first.click()
            page.wait_for_load_state("networkidle")
            time.sleep(req_delay)
            dismiss_popups(page)
            return True
    except Exception:
        pass

    # Try matching by smc/<id>
    try:
        m = SMC_ID_RE.search(href or "")
        if m:
            smc_id = m.group(1)
            locator = page.locator(f'a[href*="/smc/{smc_id}"]')
            if locator.count():
                locator.first.click()
                page.wait_for_load_state("networkidle")
                time.sleep(req_delay)
                dismiss_popups(page)
                return True
    except Exception:
        pass

    # Fall back to chip click by category name
    if click_category_chip(page, cat_name):
        time.sleep(req_delay)
        dismiss_popups(page)
        return True

    return False


# ---------------------- Main crawler ---------------------- #

def crawl(categories: List[Tuple[str, str]],
          out_path: str,
          headless: bool = True,
          req_delay: float = 0.35) -> None:

    if not categories:
        print("No categories to crawl.")
        return

    # Derive base store URL from first category
    first_href = categories[0][1]
    if "/smc/" in first_href:
        base_url = first_href.split("/smc/")[0]  # .../et-EE/<city>/p/<venue>
    else:
        # fallback to removing query part and keeping /p/<venue>
        parsed = urlparse(first_href)
        parts = parsed.path.split("/")
        try:
            p_idx = parts.index("p")
            base_url = f"{parsed.scheme}://{parsed.netloc}/" + "/".join([x for x in parts[:p_idx+2] if x])
        except Exception:
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    city_slug, venue_id = extract_city_and_venue(base_url)
    print(f"[info] derived base store URL: {base_url}")
    print(f"[info] city={city_slug} venue={venue_id}")
    print(f"[info] categories selected: {len(categories)}")

    products: List[Product] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 2200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            timezone_id="Europe/Tallinn",
            locale="et-EE",
            geolocation={"latitude": 58.3776, "longitude": 26.7290},  # Tartu
            permissions=["geolocation"],
            extra_http_headers={"Accept-Language": "et-EE,et;q=0.9,en;q=0.8"},
        )
        # Light stealth
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        context.add_init_script("window.chrome = { runtime: {} };")
        context.add_init_script("Object.defineProperty(navigator, 'languages', {get: () => ['et-EE','et','en']});")
        context.add_init_script("Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});")

        page = context.new_page()
        page.set_default_timeout(30_000)

        # Enter the store root once
        page.goto(base_url, wait_until="domcontentloaded")
        time.sleep(req_delay)
        dismiss_popups(page)

        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")

            # Navigate via anchors/chips from the store page (prevents deep-link redirect to city root)
            ok = open_category_via_page(page, base_url, href, cat_name, req_delay=req_delay)

            # If that still failed, try a direct goto once as a last resort
            if not ok:
                try:
                    page.goto(href, timeout=60_000, wait_until="domcontentloaded")
                    time.sleep(req_delay)
                    dismiss_popups(page)
                except Exception:
                    pass

            tiles: List[Dict] = []
            for attempt in range(1, 4):
                # If we got bounced away from the store or not on a /p/<venue> path, jump back and try chip
                if "/p/" not in (page.url or ""):
                    ensure_on_store_page(page, base_url, req_delay)
                    click_category_chip(page, cat_name)

                try:
                    wait_for_grid(page, timeout=18_000)
                except PWTimeout:
                    pass

                auto_scroll(page, max_steps=50, pause=0.22)
                tiles = extract_tiles_runtime(page)
                if tiles:
                    print(f"[cat] parsed {len(tiles)} tiles")
                    break

                # Explicit fallback if landing on holding/collection
                if "hc/" in (page.url or ""):
                    if open_first_category_from_hc(page):
                        auto_scroll(page, max_steps=40, pause=0.22)
                        tiles = extract_tiles_runtime(page)
                        if tiles:
                            print(f"[cat] parsed {len(tiles)} tiles (from hc → first category)")
                            break

                print(f"[cat] attempt {attempt} failed: no tiles yet")
                time.sleep(0.7)
                dismiss_popups(page)

            if not tiles:
                print(f"[cat] gave up: {cat_name}")
                # go back to store root for next category regardless
                ensure_on_store_page(page, base_url, req_delay)
                continue

            # Build product rows
            for t in tiles:
                name = norm_space(t.get("name", ""))
                price_val = parse_price(t.get("price_text") or t.get("text") or "")
                unit_text = ""
                img = t.get("image", "")
                href_rel = t.get("href", "")
                # absolute URL if href is relative
                if href_rel and href_rel.startswith("/"):
                    href_abs = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}{href_rel}"
                else:
                    href_abs = href_rel or page.url

                if not name or price_val is None:
                    continue

                products.append(
                    Product(
                        category=cat_name,
                        name=name,
                        price_eur=price_val,
                        unit_text=unit_text,
                        image=img,
                        url=href_abs,
                        store_url=base_url,
                        city_slug=city_slug,
                        venue_id=venue_id,
                        raw=t,
                    )
                )

            # Return to store root for the next category
            ensure_on_store_page(page, base_url, req_delay)

        browser.close()

    # Write CSV
    if products:
        fieldnames = [
            "city_slug",
            "venue_id",
            "store_url",
            "category",
            "name",
            "price_eur",
            "unit_text",
            "image",
            "url",
            "raw_json",
        ]
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for p in products:
                w.writerow(
                    {
                        "city_slug": p.city_slug,
                        "venue_id": p.venue_id,
                        "store_url": p.store_url,
                        "category": p.category,
                        "name": p.name,
                        "price_eur": f"{p.price_eur:.2f}",
                        "unit_text": p.unit_text,
                        "image": p.image,
                        "url": p.url,
                        "raw_json": json.dumps(p.raw, ensure_ascii=False),
                    }
                )
        print(f"[done] wrote {len(products)} rows → {out_path}")
    else:
        print("[done] no products extracted")


def main():
    ap = argparse.ArgumentParser("bolt food store crawler")
    ap.add_argument("--categories-file", required=True, help="File with category lines: 'Name -> URL' or just URL")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--headless", type=int, default=1, help="1=headless (default), 0=show browser")
    ap.add_argument("--req-delay", type=float, default=0.35, help="Delay after navigations")
    args = ap.parse_args()

    categories = parse_categories_file(args.categories_file)
    try:
        crawl(
            categories=categories,
            out_path=args.out,
            headless=bool(args.headless),
            req_delay=args.req_delay,
        )
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()

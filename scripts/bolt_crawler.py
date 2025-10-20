#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bolt Food crawler (Coop venues) — backward-compatible CLI.

Fixes:
- Deep-link redirect by navigating via store root + clicking anchors/chips.
- Auto-resolve categories file in city subfolders (e.g., data/bolt/2-tartu/*.txt).
- Recursive search fallback inside --categories-dir.

CLI:
  Old (kept for GitHub Actions):
    --city --store --categories-dir --out [--headless 0/1] [--req-delay 0.45] [--deep 0/1] [--upsert-db 0/1]
    -> picks categories file: <categories-dir>/<city>/<slugified store>.txt
       (falls back to <categories-dir>/<slugified store>.txt and then recursive search)

  New (simple):
    --categories-file <file> --out <csv>
"""
import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------------- regexes ---------------------- #
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


# ---------------------- utils ---------------------- #
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
    m = CITY_RE.search(url)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


ESTONIAN_MAP = str.maketrans({
    "ä": "a", "ö": "o", "ü": "u", "õ": "o",
    "š": "s", "ž": "z", "Ä": "a", "Ö": "o", "Ü": "u", "Õ": "o", "Š": "s", "Ž": "z",
})


def slugify_store(name: str) -> str:
    s = (name or "").translate(ESTONIAN_MAP).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def parse_categories_file(path: str) -> List[Tuple[str, str]]:
    """
    Accepts either:
      Category Name -> https://.../smc/<id>?categoryName=Piim&backPath=%2Fp%2F1969
    or just the URL.
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
                name = parse_qs(urlparse(href).query).get(CATEGORY_NAME_Q, [""])[0] or "Unknown"
                out.append((name, href))
    return out


def _norm_for_match(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", slugify_store(s))


def find_categories_file(categories_dir: str, store_name: str, city: str = "") -> Optional[str]:
    """
    Resolve categories file in a forgiving way:
      1) <dir>/<city>/<slug>.txt
      2) <dir>/<slug>.txt
      3) recursive search under <dir> for any file whose normalized name matches.
    """
    if not categories_dir or not store_name:
        return None
    want_slug = slugify_store(store_name)
    want_norm = _norm_for_match(store_name)

    # 1) city subfolder
    if city:
        candidate = os.path.join(categories_dir, city, f"{want_slug}.txt")
        if os.path.isfile(candidate):
            return candidate

    # 2) top-level
    candidate = os.path.join(categories_dir, f"{want_slug}.txt")
    if os.path.isfile(candidate):
        return candidate

    # 3) recursive scan
    best = None
    if os.path.isdir(categories_dir):
        for root, _, files in os.walk(categories_dir):
            for fn in files:
                if not fn.lower().endswith(".txt"):
                    continue
                if _norm_for_match(fn) == want_norm:
                    best = os.path.join(root, fn)
                    return best
    return best


# ---------------------- Playwright helpers ---------------------- #
def dismiss_popups(page) -> None:
    selectors = [
        'button:has-text("Nõustun")',
        'button:has-text("Luba kõik")',
        'button:has-text("Luban kõik")',
        'button:has-text("OK")',
        'button:has-text("Accept")',
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
    if not category_name:
        return False
    try:
        chip = page.get_by_role("link", name=re.compile(rf"^{re.escape(category_name)}\b", re.I))
        if chip.count():
            chip.first.click()
            page.wait_for_load_state("networkidle")
            return True
    except Exception:
        pass
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
    candidates = [
        '[data-testid="product-card"]',
        '[data-test="product-card"]',
        '[data-testid="productTile"]',
        '[data-test="productTile"]',
        'article:has-text("€")',
        'div:has(> div >> text=/€/)',
    ]
    start = time.time()
    while (time.time() - start) * 1000 < timeout:
        for sel in candidates:
            try:
                if page.locator(sel).count():
                    return
            except Exception:
                pass
        time.sleep(0.2)
    raise PWTimeout("product grid not found")


def auto_scroll(page, max_steps: int = 60, pause: float = 0.25) -> None:
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

              let nameEl =
                el.querySelector('[data-testid="product-name"],[data-test="product-name"]') ||
                el.querySelector('h3,h4,strong') ||
                el.querySelector('div[title]');
              let name = nameEl ? (nameEl.getAttribute('title') || nameEl.textContent || '') : '';
              name = name.replace(/\\s+/g, ' ').trim();
              if (!name) {
                const parts = txt.split('€')[0].trim();
                name = parts.split(' + ')[0].trim();
              }
              if (!name) continue;

              const priceEl =
                el.querySelector('[data-testid="product-price"],[data-test="product-price"]') ||
                el.querySelector('span,div');
              let priceText = priceEl ? priceEl.textContent || '' : txt;
              priceText = priceText.replace(/\\s+/g, ' ').trim();
              const m = priceText.match(/(\\d+(?:[.,]\\d{1,2})?)\\s*€/);
              if (!m) continue;
              const price = m[1];

              let imgEl = el.querySelector('img');
              let img = imgEl ? (imgEl.getAttribute('src') || imgEl.getAttribute('data-src') || '') : '';

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
    if not base_url:
        return
    try:
        if "/p/" not in (page.url or "") or page.url.split("?")[0] != base_url:
            page.goto(base_url, timeout=60_000, wait_until="domcontentloaded")
            time.sleep(req_delay)
            dismiss_popups(page)
    except Exception:
        page.goto(base_url, timeout=60_000)
        time.sleep(req_delay)
        dismiss_popups(page)


def open_category_via_page(page, base_url: str, href: str, cat_name: str, req_delay: float = 0.3) -> bool:
    ensure_on_store_page(page, base_url, req_delay)

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

    if click_category_chip(page, cat_name):
        time.sleep(req_delay)
        dismiss_popups(page)
        return True

    return False


# ---------------------- main crawl ---------------------- #
def crawl(categories: List[Tuple[str, str]], out_path: str, headless: bool = True, req_delay: float = 0.35) -> None:
    if not categories:
        print("No categories to crawl.")
        return

    first_href = categories[0][1]
    if "/smc/" in first_href:
        base_url = first_href.split("/smc/")[0]
    else:
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
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        context.add_init_script("window.chrome = { runtime: {} };")
        context.add_init_script("Object.defineProperty(navigator, 'languages', {get: () => ['et-EE','et','en']});")
        context.add_init_script("Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});")

        page = context.new_page()
        page.set_default_timeout(30_000)

        page.goto(base_url, wait_until="domcontentloaded")
        time.sleep(req_delay)
        dismiss_popups(page)

        for cat_name, href in categories:
            print(f"[cat] {cat_name} -> {href}")

            ok = open_category_via_page(page, base_url, href, cat_name, req_delay=req_delay)
            if not ok:
                try:
                    page.goto(href, timeout=60_000, wait_until="domcontentloaded")
                    time.sleep(req_delay)
                    dismiss_popups(page)
                except Exception:
                    pass

            tiles: List[Dict] = []
            for attempt in range(1, 4):
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
                ensure_on_store_page(page, base_url, req_delay)
                continue

            for t in tiles:
                name = norm_space(t.get("name", ""))
                price_val = parse_price(t.get("price_text") or t.get("text") or "")
                unit_text = ""
                img = t.get("image", "")
                href_rel = t.get("href", "")
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
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
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


# ---------------------- CLI ---------------------- #
def main():
    ap = argparse.ArgumentParser("bolt food store crawler")
    # New style
    ap.add_argument("--categories-file", help="File with category lines: 'Name -> URL' or just URL")
    # Old style (back-compat)
    ap.add_argument("--categories-dir", help="Directory containing per-store .txt category files")
    ap.add_argument("--city", help="City slug (e.g. '2-tartu')", default="")
    ap.add_argument("--store", help="Store name (used to find <categories-dir>/<city>/<slugified>.txt)")
    ap.add_argument("--deep", help="Back-compat flag, ignored", default="0")
    ap.add_argument("--upsert-db", help="Back-compat flag, ignored", default="0")

    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--headless", type=int, default=1, help="1=headless (default), 0=show browser")
    ap.add_argument("--req-delay", type=float, default=0.35, help="Delay after navigations")
    args = ap.parse_args()

    # Resolve categories file
    categories_file = args.categories_file
    if not categories_file:
        categories_file = find_categories_file(args.categories_dir or "", args.store or "", args.city or "")

    if not categories_file or not os.path.isfile(categories_file):
        ap.error(
            "the following arguments are required: --categories-file "
            "(or provide --categories-dir AND --store, optionally --city, so I can infer it)"
        )

    print(f"[info] using categories file: {categories_file}")
    categories = parse_categories_file(categories_file)

    # Go crawl
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

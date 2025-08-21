# at the top (already present)
import os, re, csv, time
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError

# ... keep your existing code ...

def safe_goto(page, url: str, timeout: int = 15000) -> bool:
    """Navigate and return True/False instead of throwing."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        return True
    except Exception as e:
        print(f"[selver] NAV FAIL {url} -> {type(e).__name__}: {e}")
        return False

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
            context.route("**/*", lambda route, req:
                route.abort() if _should_block(req.url) else route.continue_())

            page = context.new_page()
            page.set_default_navigation_timeout(15000)
            page.set_default_timeout(8000)

            # (optional) pipe console to logs for visibility
            page.on("console", lambda msg: print(f"[pw] {msg.type()} {msg.text()}"))

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
                accept_cookies(page); time.sleep(REQ_DELAY)
                top = set()
                for sel in ["a[href*='/e-selver/']", "nav a[href*='/e-selver/']", "aside a[href*='/e-selver/']"]:
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
            for cu in cats:
                print(f"[selver]   {cu}")

            # ---- crawl
            product_urls: set[str] = set()
            for ci, cu in enumerate(cats, 1):
                if not safe_goto(page, cu):
                    # keep a screenshot for debugging
                    try: page.screenshot(path=f"{dbg_dir}/cat_nav_fail_{ci}.png", full_page=True)
                    except Exception: pass
                    continue

                time.sleep(REQ_DELAY)
                links = collect_product_links(page, page_limit=PAGE_LIMIT)
                if not links:
                    # screenshot the category page so we can inspect layout later
                    try: page.screenshot(path=f"{dbg_dir}/cat_empty_{ci}.png", full_page=True)
                    except Exception: pass

                product_urls.update(links)
                print(f"[selver] {cu} → +{len(links)} products (total so far: {len(product_urls)})")

            # ---- visit product pages
            rows_written = 0
            for i, pu in enumerate(sorted(product_urls), 1):
                if not _is_selver_product_like(pu):  # safety
                    continue
                if not safe_goto(page, pu):
                    try: page.screenshot(path=f"{dbg_dir}/prod_nav_fail_{i}.png", full_page=True)
                    except Exception: pass
                    continue

                time.sleep(REQ_DELAY)
                try:
                    name = normspace(page.locator("h1").first.inner_text())
                except Exception:
                    name = ""

                price, currency = extract_price(page)
                ean = extract_ean(page, pu)
                size_text = guess_size_from_title(name)
                crumbs = breadcrumbs(page)
                cat_path = " / ".join(crumbs); cat_leaf = crumbs[-1] if crumbs else ""

                if not name:
                    # keep a failing product page too
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

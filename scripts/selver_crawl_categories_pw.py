# … (header & imports unchanged)

BLOCK_HOSTS = {
    "adobe.com", "assets.adobedtm.com", "adobedtm.com", "demdex.net", "omtrdc.net",
    "googletagmanager.com", "google-analytics.com", "doubleclick.net", "facebook.net",
}

NON_PRODUCT_PATH_SNIPPETS = {
    "/e-selver/", "/ostukorv", "/cart", "/checkout", "/search", "/otsi",
    "/konto", "/customer", "/login", "/logout", "/registreeru", "/uudised",
    "/tootajad", "/kontakt", "/tingimused", "/privaatsus", "/privacy",
    "/kampaania", "/kampaaniad", "/blogi", "/app", "/store-locator",
}

def _should_block(url: str) -> bool:
    h = urlparse(url).netloc.lower()
    return any(h == d or h.endswith("." + d) for d in BLOCK_HOSTS)

def _is_selver_product_like(url: str) -> bool:
    u = urlparse(url)
    host_ok = u.netloc.lower().endswith("selver.ee") or (u.netloc == "" and url.startswith("/"))
    if not host_ok:
        return False
    path = u.path or "/"
    # exclude category & obvious non-product paths
    if any(sn in path.lower() for sn in NON_PRODUCT_PATH_SNIPPETS):
        return False
    # products are sluggy paths (no file extension, at least 1 segment)
    if "." in path.rsplit("/", 1)[-1]:
        return False
    return path.count("/") >= 1

def safe_goto(page, url: str, timeout: int = 30000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        return True
    except TimeoutError:
        return False

# … discover_categories & accept_cookies unchanged …

def collect_product_links(page, page_limit: int = 0) -> set[str]:
    """On a category page, paginate & collect product card links."""
    links: set[str] = set()
    pages_seen = 0

    def grab_cards():
        found = 0
        # Be generous with selectors, but filter aggressively afterwards.
        for sel in [
            "a.product-card__link",
            "a[href][data-product-id]",
            "article a[href]",
            "li a[href]",
            "div a[href]",
        ]:
            try:
                as_ = page.locator(sel)
                cnt = as_.count()
                for i in range(cnt):
                    href = as_.nth(i).get_attribute("href")
                    if not href:
                        continue
                    u = urljoin(BASE, href)
                    if _is_selver_product_like(u):
                        if u not in links:
                            links.add(u); found += 1
            except Exception:
                pass
        return found

    def next_selector():
        for sel in [
            "a[rel='next']",
            "a[aria-label*='Next']",
            "button:has-text('Näita rohkem')",
            "button:has-text('Load more')",
            "a.page-link:has-text('>')",
        ]:
            if page.locator(sel).count() > 0:
                return sel
        return None

    while True:
        pages_seen += 1
        grab_cards()
        if page_limit and pages_seen >= page_limit:
            break
        nxt = next_selector()
        if not nxt:
            break
        try:
            page.locator(nxt).first.click(timeout=5000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(REQ_DELAY)
        except Exception:
            break

    return links

# … breadcrumbs, extract_price, extract_ean unchanged …

def crawl():
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "ext_id","name","ean_raw","size_text","price","currency","category_path","category_leaf",
        ])
        w.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
            )
            context.route("**/*", lambda route, req:
                route.abort() if _should_block(req.url) else route.continue_())

            page = context.new_page()

            # Seeds: file or autodiscover
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
            for cu in cats: print(f"[selver] {cu}")

            product_urls: set[str] = set()
            for cu in cats:
                if not safe_goto(page, cu): continue
                time.sleep(REQ_DELAY)
                links = collect_product_links(page, page_limit=PAGE_LIMIT)
                product_urls.update(links)
                print(f"[selver] {cu} → +{len(links)} products (total so far: {len(product_urls)})")

            for i, pu in enumerate(sorted(product_urls)):
                # final safety: only same-host selver links
                if not _is_selver_product_like(pu):
                    continue
                if not safe_goto(page, pu): continue
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

                if not name:  # skip junk
                    continue

                w.writerow({
                    "ext_id": pu, "name": name, "ean_raw": ean, "size_text": size_text,
                    "price": f"{price:.2f}", "currency": currency,
                    "category_path": cat_path, "category_leaf": cat_leaf,
                })
                if (i + 1) % 25 == 0: f.flush()

            browser.close()

if __name__ == "__main__":
    try:
        crawl()
    except KeyboardInterrupt:
        pass

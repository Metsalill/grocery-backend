# scripts/selver_probe_ean_pw.py
import asyncio, csv, sys, re
from pathlib import Path
from playwright.async_api import async_playwright

SEARCH = "https://www.selver.ee/search?q={ean}"

async def fetch_one(page, ean: str):
    url = SEARCH.format(ean=ean)
    await page.goto(url, wait_until="domcontentloaded")
    # if search shows one product, many sites auto-redirect; otherwise click first result
    # fallback: try meta og:url/title
    try:
        # try click first product tile if present
        first = page.locator("a").filter(has_text=re.compile(r".+")).first
        href = await first.get_attribute("href")
        if href and "/toode/" in href:
            await page.goto(href, wait_until="domcontentloaded")
    except:
        pass

    final_url = page.url
    title = (await page.title()) or ""
    # Try price selectors (heuristics; safe to leave empty if not found)
    price_txt = ""
    for sel in ["[data-testid=price]", ".price", ".product-price", "[itemprop=price]"]:
        el = page.locator(sel).first
        if await el.count():
            price_txt = (await el.inner_text()).strip()
            break

    return {
        "ext_id": final_url,
        "url": final_url,
        "ean": ean,
        "name": title.strip(),
        "price": price_txt,
        "size_text": "",
    }

async def main(in_csv: str, out_csv: str, delay: float = 0.4):
    ins = [r["ean"] for r in csv.DictReader(open(in_csv, newline="", encoding="utf-8"))]
    outp = csv.DictWriter(open(out_csv, "w", newline="", encoding="utf-8"),
                          fieldnames=["ext_id","url","ean","name","price","size_text"])
    outp.writeheader()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        for e in ins:
            row = await fetch_one(page, e)
            outp.writerow(row)
            await asyncio.sleep(delay)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2]))

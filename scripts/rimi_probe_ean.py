import asyncio, json, re, sys, csv, time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

EAN_RE = re.compile(r'\b\d{13}\b')            # GTIN-13
EAN_LABEL_RE = re.compile(r'\b(ean|gtin|gtin13|barcode|triipkood)\b', re.I)
SKU_KEYS = {"sku","mpn","itemNumber","productCode","code"}
EAN_KEYS = {"ean","ean13","gtin","gtin13","barcode"}

def deep_find_kv(obj: Any, keys: set) -> Dict[str, str]:
    """Recursively search dict/list for first matching keys (case-insensitive)."""
    found = {}
    def walk(x):
        nonlocal found
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if lk in keys and isinstance(v, (str,int)):
                    found[lk] = str(v)
                walk(v)
        elif isinstance(x, list):
            for i in x: walk(i)
    walk(obj)
    return found

def parse_jsonld(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], List[str]]:
    flags = []
    ean = sku = None
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.text)
        except Exception:
            continue
        if isinstance(data, list):
            for d in data:
                got = deep_find_kv(d, { *EAN_KEYS, *SKU_KEYS })
                ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
                sku = sku or got.get("sku") or got.get("mpn")
        else:
            got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS })
            ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
            sku = sku or got.get("sku") or got.get("mpn")
    if ean or sku: flags.append("jsonld")
    return ean, sku, flags

def parse_microdata_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], List[str]]:
    flags = []
    ean = sku = None
    # <meta itemprop="gtin13" content="..."> or <span itemprop="gtin13">...</span>
    for ip in ("gtin13","gtin","ean","ean13","barcode"):
        meta = soup.find(attrs={"itemprop": ip})
        if meta:
            ean = ean or (meta.get("content") or meta.get_text(strip=True))
    for ip in ("sku","mpn"):
        meta = soup.find(attrs={"itemprop": ip})
        if meta:
            sku = sku or (meta.get("content") or meta.get_text(strip=True))
    if ean or sku: flags.append("microdata")
    return ean, sku, flags

def parse_visible_text_for_ean(soup: BeautifulSoup) -> Tuple[Optional[str], List[str]]:
    flags = []
    # Find labels like "EAN: 4740..." or "Triipkood 4740..."
    for el in soup.find_all(text=EAN_LABEL_RE):
        seg = el.parent.get_text(" ", strip=True) if el and el.parent else str(el)
        m = EAN_RE.search(seg)
        if m:
            flags.append("visible")
            return m.group(0), flags
    # fallback: any 13-digit that looks like a GTIN
    m = EAN_RE.search(soup.get_text(" ", strip=True))
    if m:
        flags.append("visible_guess")
        return m.group(0), flags
    return None, flags

def parse_imgs_for_gtin(soup: BeautifulSoup) -> Tuple[Optional[str], List[str]]:
    flags = []
    for img in soup.find_all("img"):
        cand = " ".join(filter(None, [img.get("src",""), img.get("alt","")]))
        m = EAN_RE.search(cand)
        if m:
            flags.append("img_url")
            return m.group(0), flags
    return None, flags

async def probe_url(pw, url: str, timeout_ms=25000) -> Dict[str, Any]:
    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
    ctx = await browser.new_context(
        locale="et-EE",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        viewport={"width": 1366, "height": 900}
    )
    page = await ctx.new_page()

    # Collect JSON responses for sniffing
    sniffed: List[Dict[str,Any]] = []
    async def on_response(resp):
        try:
            ct = resp.headers.get("content-type","")
            if "application/json" in ct:
                data = await resp.json()
                sniffed.append({"url": resp.url, "data": data})
        except Exception:
            pass
    page.on("response", on_response)

    # Navigate
    name = None
    ean = None
    sku = None
    sources = []

    try:
        await page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
        # Cookie banner
        for label in ("Nõustun","Nõustu","Accept","Allow all","OK","Selge"):
            try:
                await page.get_by_role("button", name=re.compile(label, re.I)).click(timeout=1500)
                break
            except Exception:
                pass

        # Small wait for hydration/network calls
        await page.wait_for_timeout(1200)

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Try to grab product name early
        h1 = soup.find("h1")
        if h1: name = h1.get_text(strip=True)

        # 1) JSON-LD
        e1, s1, f1 = parse_jsonld(soup); sources += f1
        ean = ean or e1; sku = sku or s1

        # 2) Microdata / meta
        e2, s2, f2 = parse_microdata_meta(soup); sources += f2
        ean = ean or e2; sku = sku or s2

        # 3) Globals
        for glb in ["__NUXT__","__NEXT_DATA__","APP_STATE","dataLayer"]:
            try:
                data = await page.evaluate(f"window['{glb}']")
                if data:
                    got = deep_find_kv(data, { *EAN_KEYS, *SKU_KEYS })
                    ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
                    sku = sku or got.get("sku") or got.get("mpn") or got.get("code")
                    if got: sources.append(f"global:{glb}")
            except Exception:
                pass

        # 4) Visible text
        if not ean:
            e3, f3 = parse_visible_text_for_ean(soup); sources += f3
            ean = ean or e3

        # 5) Image URLs/alt
        if not ean:
            e4, f4 = parse_imgs_for_gtin(soup); sources += f4
            ean = ean or e4

        # 6) Network sniff
        if not ean or not sku:
            for rec in sniffed:
                got = deep_find_kv(rec["data"], { *EAN_KEYS, *SKU_KEYS })
                if got:
                    ean = ean or got.get("gtin13") or got.get("ean") or got.get("ean13") or got.get("barcode") or got.get("gtin")
                    sku = sku or got.get("sku") or got.get("mpn") or got.get("code")
                    sources.append("net")

    except PWTimeout:
        sources.append("timeout")
    except Exception as e:
        sources.append(f"err:{type(e).__name__}")
    finally:
        await ctx.close()
        await browser.close()

    return {
        "ext_id": url,
        "url": url,
        "name": name,
        "ean_raw": ean or "",
        "sku_raw": sku or "",
        "source_flags": ",".join(dict.fromkeys(sources))  # de-dup while preserving order
    }

async def main():
    # Read URLs from stdin or file args
    urls: List[str] = []
    if len(sys.argv) > 1 and Path(sys.argv[1]).exists():
        urls = [u.strip() for u in Path(sys.argv[1]).read_text().splitlines() if u.strip()]
    else:
        print("Paste Rimi PDP URLs (one per line), end with Ctrl+D:", file=sys.stderr)
        urls = [line.strip() for line in sys.stdin if line.strip()]

    if not urls:
        print("No URLs given.", file=sys.stderr)
        sys.exit(1)

    out_path = Path("rimi_probe_eans.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["ext_id","url","name","ean_raw","sku_raw","source_flags"])
        wr.writeheader()
        async with async_playwright() as pw:
            for i, u in enumerate(urls, 1):
                row = await probe_url(pw, u)
                wr.writerow(row)
                print(f"[{i}/{len(urls)}] {row['ean_raw'] or '—'}  {row['name'] or ''}")
                time.sleep(0.6)  # be gentle

    print(f"Wrote {out_path}")

if __name__ == "__main__":
    asyncio.run(main())

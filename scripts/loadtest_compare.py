#!/usr/bin/env python3
import argparse, asyncio, json, random, time
import httpx

DEFAULT_PRODUCTS = [
    "Milk", "Eggs", "Bread", "Butter", "Cheese", "Sugar", "Salt", "Flour",
    "Tomato", "Cucumber", "Potato", "Banana", "Apple", "Chicken", "Pasta",
]

def build_basket(all_products, n_items):
    picks = random.sample(all_products, k=min(n_items, len(all_products)))
    return [{"product": p, "quantity": random.randint(1, 2)} for p in picks]

async def worker(client, url, lat, lon, radius, all_products, items_per_req, results):
    payload = {
        "grocery_list": {"items": build_basket(all_products, items_per_req)},
        "lat": lat, "lon": lon, "radius_km": radius
    }
    t0 = time.perf_counter()
    try:
        r = await client.post(url, json=payload, timeout=30.0)
        dt = (time.perf_counter() - t0) * 1000
        results.append((r.status_code, dt))
    except Exception as e:
        dt = (time.perf_counter() - t0) * 1000
        results.append(("ERR", dt))

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--concurrency", type=int, default=10)
    ap.add_argument("--requests", type=int, default=100)
    ap.add_argument("--items", type=int, default=10)
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--radius", type=float, default=10.0)
    ap.add_argument("--products-file", default=None, help="one product name per line")
    args = ap.parse_args()

    all_products = DEFAULT_PRODUCTS
    if args.products_file:
        with open(args.products_file, "r", encoding="utf-8") as f:
            all_products = [ln.strip() for ln in f if ln.strip()]

    url = args.base_url.rstrip("/") + "/compare"
    results = []
    limits = httpx.Limits(max_keepalive_connections=args.concurrency, max_connections=args.concurrency)
    timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=None)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(args.concurrency)
        async def run_one():
            async with sem:
                await worker(client, url, args.lat, args.lon, args.radius,
                             all_products, args.items, results)

        tasks = [asyncio.create_task(run_one()) for _ in range(args.requests)]
        await asyncio.gather(*tasks)

    # Summary
    oks = [dt for code, dt in results if code == 200]
    errs = len([1 for code, _ in results if code != 200])
    print(f"Total: {len(results)}; OK: {len(oks)}; Errors: {errs}")
    if oks:
        print(f"P50: {sorted(oks)[len(oks)//2]:.1f} ms; "
              f"Avg: {sum(oks)/len(oks):.1f} ms; "
              f"Max: {max(oks):.1f} ms")

if __name__ == "__main__":
    asyncio.run(main())

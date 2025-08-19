#!/usr/bin/env python3
import argparse, asyncio, json, random, time
import httpx
from collections import Counter
from statistics import median

DEFAULT_PRODUCTS = [
    "Milk","Eggs","Bread","Butter","Cheese","Sugar","Salt","Flour",
    "Tomato","Cucumber","Potato","Banana","Apple","Chicken","Pasta",
]

def build_basket(all_products, n_items):
    picks = random.sample(all_products, k=min(n_items, len(all_products)))
    return [{"product": p, "quantity": random.randint(1, 2)} for p in picks]

async def one_request(client, url, lat, lon, radius, items_per_req, all_products,
                      headers, results, status_counts, err_types, err_samples, timeout_s):
    payload = {
        "grocery_list": {"items": build_basket(all_products, items_per_req)},
        "lat": lat, "lon": lon, "radius_km": radius
    }
    t0 = time.perf_counter()
    try:
        r = await client.post(url, json=payload, headers=headers, timeout=timeout_s)
        dt = (time.perf_counter() - t0) * 1000
        code = r.status_code
        results.append((code, dt))
        status_counts[code] = status_counts.get(code, 0) + 1
    except Exception as e:
        dt = (time.perf_counter() - t0) * 1000
        results.append(("ERR", dt))
        status_counts["ERR"] = status_counts.get("ERR", 0) + 1
        et = type(e).__name__
        err_types[et] += 1
        # keep first example for this error type
        if et not in err_samples:
            err_samples[et] = repr(e)

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--path", default="/compare")
    ap.add_argument("--concurrency", type=int, default=10)
    ap.add_argument("--requests", type=int, default=100)
    ap.add_argument("--items", type=int, default=10)
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--radius", type=float, default=10.0)
    ap.add_argument("--products-file", default=None)
    ap.add_argument("--auth-bearer", default=None, help="Bearer token (optional)")
    ap.add_argument("--target-rps", type=float, default=0.0, help="Requests/sec pacing; 0=as fast as possible")
    ap.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout (seconds)")
    ap.add_argument("--out", default=None, help="Write JSON summary to this path")
    args = ap.parse_args()

    all_products = DEFAULT_PRODUCTS
    if args.products_file:
        with open(args.products_file, "r", encoding="utf-8") as f:
            all_products = [ln.strip() for ln in f if ln.strip()]

    url = args.base_url.rstrip("/") + (args.path if args.path.startswith("/") else "/" + args.path)
    print(f"Target: {url}")

    results: list[tuple[object, float]] = []
    status_counts: dict[object, int] = {}
    error_types: Counter = Counter()
    error_samples: dict[str, str] = {}

    headers = {}
    if args.auth_bearer:
        headers["Authorization"] = f"Bearer {args.auth_bearer}"

    limits = httpx.Limits(max_keepalive_connections=args.concurrency, max_connections=args.concurrency)
    timeout = httpx.Timeout(connect=10.0, read=args.timeout, write=10.0, pool=None)

    start = time.perf_counter()
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(args.concurrency)
        interval = (1.0 / args.target_rps) if args.target_rps and args.target_rps > 0 else 0.0

        async def scheduled(i: int):
            # simple pacing: schedule each request at start + i*interval
            if interval > 0:
                target = start + i * interval
                now = time.perf_counter()
                delay = target - now
                if delay > 0:
                    await asyncio.sleep(delay)
            async with sem:
                await one_request(
                    client, url, args.lat, args.lon, args.radius, args.items,
                    all_products, headers, results, status_counts,
                    error_types, error_samples, args.timeout
                )

        await asyncio.gather(*[asyncio.create_task(scheduled(i)) for i in range(args.requests)])

    elapsed_s = time.perf_counter() - start

    # Stats
    oks = [dt for code, dt in results if code == 200]
    errs = len([1 for code, _ in results if code != 200])
    total = len(results)

    def percentile(data, p):
        if not data: return None
        data = sorted(data)
        k = int(round((p / 100.0) * (len(data) - 1)))
        return data[k]

    p50 = percentile(oks, 50)
    p90 = percentile(oks, 90)
    p95 = percentile(oks, 95)
    p99 = percentile(oks, 99)

    print(f"Total: {total}; OK: {len(oks)}; Errors: {errs}; Elapsed: {elapsed_s:.2f}s; "
          f"Throughput: {total/elapsed_s:.2f} req/s")
    print("Status counts:", dict(sorted(status_counts.items(), key=lambda kv: str(kv[0]))))
    if error_types:
        print("Errors by type:", dict(error_types))
        # print one example if available
        et, cnt = next(iter(error_types.items()))
        if et in error_samples:
            print("Sample error:", f"{et}: {error_samples[et]}")
    if oks:
        print(f"P50: {p50:.1f} ms; P90: {p90:.1f} ms; P95: {p95:.1f} ms; P99: {p99:.1f} ms; "
              f"Avg: {sum(oks)/len(oks):.1f} ms; Max: {max(oks):.1f} ms")

    if args.out:
        summary = {
            "total": total,
            "ok": len(oks),
            "errors": errs,
            "elapsed_seconds": elapsed_s,
            "throughput_rps": total/elapsed_s if elapsed_s > 0 else None,
            "status_counts": status_counts,
            "error_types": dict(error_types),
            "error_samples": error_samples,
            "latency_ms": {
                "p50": p50, "p90": p90, "p95": p95, "p99": p99,
                "avg": (sum(oks)/len(oks) if oks else None),
                "max": (max(oks) if oks else None),
            },
            "config": vars(args),
        }
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"Summary written to {args.out}")

if __name__ == "__main__":
    asyncio.run(main())

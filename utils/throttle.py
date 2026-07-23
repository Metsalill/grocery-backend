# utils/throttle.py
import time
import asyncio
import hashlib
import json
from functools import wraps
from fastapi import Request, HTTPException

from utils.client_ip import get_client_ip

_CLEANUP_THRESHOLD = 2000  # only sweep the bucket dict when it grows large, not every request


def _hash_ip(ip: str) -> str:
    # Store a truncated hash, not the raw IP -- keeps enough entropy to spot
    # repeat offenders without writing a directly identifying address into
    # analytics_events (GDPR consideration flagged in review).
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


async def _log_rate_limit_breach(request: Request, endpoint: str, ip_hash: str, limit: int):
    """
    Best-effort logging of the FIRST rejection per (ip, endpoint, window)
    bucket only -- not every subsequent one -- so a sustained flood after
    the limit is hit can't turn the logger itself into a DB-load amplifier.
    Fire-and-forget: never raises, never blocks the 429 response.
    """
    try:
        pool = getattr(request.app.state, "db", None)
        if pool is None:
            return
        payload = {"ip_hash": ip_hash, "endpoint": endpoint, "limit": limit}
        await pool.execute(
            """
            INSERT INTO analytics_events (event_type, chain, payload)
            VALUES ($1, $2, $3::jsonb)
            """,
            "rate_limit_exceeded",
            "",
            json.dumps(payload),
        )
    except Exception:
        pass


def throttle(limit: int, window: int = 60):
    buckets = {}    # (ip, name, window) -> count
    logged = set()  # (ip, name, window) already logged once, to avoid log spam
    lock = asyncio.Lock()

    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            request: Request = kwargs.get("request")
            if not request:
                for a in args:
                    if isinstance(a, Request):
                        request = a
                        break

            # Uses the same get_client_ip() helper as RateLimitMiddleware so
            # the two protection layers can never disagree about who a
            # request came from.
            ip = get_client_ip(request) if request else "unknown"
            name = fn.__name__
            now_window = int(time.time() // window)
            bucket = (ip, name, now_window)

            async with lock:
                if len(buckets) > _CLEANUP_THRESHOLD:
                    stale_keys = [k for k in buckets if k[2] < now_window]
                    for k in stale_keys:
                        del buckets[k]
                    logged.difference_update(stale_keys)

                buckets[bucket] = buckets.get(bucket, 0) + 1
                current_count = buckets[bucket]

                if current_count > limit:
                    if bucket not in logged:
                        logged.add(bucket)
                        asyncio.create_task(
                            _log_rate_limit_breach(request, name, _hash_ip(ip), limit)
                        )
                    raise HTTPException(status_code=429, detail="Too many requests")

            return await fn(*args, **kwargs)
        return wrapper
    return decorator

# utils/throttle.py
import time
import asyncio
from functools import wraps
from fastapi import Request, HTTPException

def throttle(limit: int, window: int = 60):
    buckets = {}
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
            ip = request.client.host if request and request.client else "unknown"
            name = fn.__name__
            bucket = (ip, name, int(time.time() // window))
            async with lock:
                buckets[bucket] = buckets.get(bucket, 0) + 1
                if buckets[bucket] > limit:
                    raise HTTPException(status_code=429, detail="Too many requests")
            return await fn(*args, **kwargs)
        return wrapper
    return decorator

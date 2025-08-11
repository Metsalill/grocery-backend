# middlewares/rate_limit.py
import time
from typing import Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

try:
    import aioredis  # type: ignore
except Exception:
    aioredis = None  # graceful fallback


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, rate_per_min: int, window: int, redis_url: Optional[str]):
        super().__init__(app)
        self.rate_per_min = rate_per_min
        self.window = window
        self.redis_url = redis_url
        self.redis = None
        self.local_counts = {}

    async def _hit_local(self, key: str) -> int:
        now_bucket = int(time.time() // self.window)
        k = (key, now_bucket)
        self.local_counts[k] = self.local_counts.get(k, 0) + 1
        if len(self.local_counts) > 5000:
            old = [kk for kk in self.local_counts if kk[1] < now_bucket]
            for kk in old:
                self.local_counts.pop(kk, None)
        return self.local_counts[k]

    async def _hit_redis(self, key: str) -> int:
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
        bucket = f"{key}:{int(time.time()//self.window)}"
        n = await self.redis.incr(bucket)
        if n == 1:
            await self.redis.expire(bucket, self.window)
        return n

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if (
            path.startswith("/static/")
            or path in ("/robots.txt", "/healthz", "/favicon.ico")
            or path.startswith("/docs")
            or path.startswith("/redoc")
            or path.startswith("/openapi.json")
        ):
            return await call_next(request)

        authz = request.headers.get("authorization") or ""
        parts = authz.split()
        token = parts[1] if (len(parts) == 2 and parts[0].lower() == "bearer") else "anon"

        ip = request.client.host if request.client else "unknown"
        key_user = f"rl:u:{token}"
        key_ip = f"rl:ip:{ip}"

        try:
            if aioredis and self.redis_url:
                n_user = await self._hit_redis(key_user)
                n_ip = await self._hit_redis(key_ip)
            else:
                n_user = await self._hit_local(key_user)
                n_ip = await self._hit_local(key_ip)
        except Exception:
            return await call_next(request)

        if n_user > self.rate_per_min or n_ip > self.rate_per_min:
            return JSONResponse({"detail": "rate limit"}, status_code=429)

        return await call_next(request)

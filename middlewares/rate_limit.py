import time
import hashlib
import inspect
import asyncio
from typing import Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from utils.client_ip import get_client_ip

try:
    import aioredis  # type: ignore
except Exception:
    aioredis = None  # graceful fallback


def _hash_identifier(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, rate_per_min: int, window: int, redis_url: Optional[str]):
        super().__init__(app)
        self.rate_per_min = rate_per_min
        self.window = window
        self.redis_url = redis_url
        self.redis = None
        self.local_counts = {}
        self.local_lock = asyncio.Lock()

    async def _hit_local(self, key: str) -> int:
        async with self.local_lock:
            now_bucket = int(time.time() // self.window)
            k = (key, now_bucket)
            self.local_counts[k] = self.local_counts.get(k, 0) + 1
            if len(self.local_counts) > 5000:
                old = [kk for kk in self.local_counts if kk[1] < now_bucket]
                for kk in old:
                    self.local_counts.pop(kk, None)
            return self.local_counts[k]

    async def _get_redis(self):
        if self.redis is None:
            client = aioredis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
            if inspect.isawaitable(client):
                client = await client
            self.redis = client
        return self.redis

    async def _hit_redis(self, key: str) -> int:
        redis_client = await self._get_redis()
        bucket = f"{key}:{int(time.time()//self.window)}"
        n = await redis_client.incr(bucket)
        if n == 1:
            await redis_client.expire(bucket, self.window)
        return n

    async def _count(self, hit_fn, key_user: Optional[str], key_ip: str):
        if key_user:
            u = await hit_fn(key_user)
            i = await hit_fn(key_ip)
            return u, i
        else:
            i = await hit_fn(key_ip)
            return i, i

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

        ip = get_client_ip(request)

        authz = request.headers.get("authorization") or ""
        parts = authz.split()
        token = parts[1] if (len(parts) == 2 and parts[0].lower() == "bearer") else None

        key_user = f"rl:u:{_hash_identifier(token)}" if token else None
        key_ip = f"rl:ip:{ip}"

        try:
            if aioredis and self.redis_url:
                n_user, n_ip = await self._count(self._hit_redis, key_user, key_ip)
            else:
                n_user, n_ip = await self._count(self._hit_local, key_user, key_ip)
        except Exception:
            try:
                n_user, n_ip = await self._count(self._hit_local, key_user, key_ip)
            except Exception:
                return await call_next(request)

        if n_user > self.rate_per_min or n_ip > self.rate_per_min:
            return JSONResponse({"detail": "rate limit"}, status_code=429)

        return await call_next(request)

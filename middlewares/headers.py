# middlewares/headers.py
from fastapi import Request, Response

async def security_and_cache_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    path = request.url.path

    if path.startswith("/static/"):
        resp.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")
        resp.headers.setdefault("Content-Security-Policy",
                                "default-src 'self'; img-src * data: blob:; media-src *;")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return resp

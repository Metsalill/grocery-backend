from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse   # ⬅️ add JSONResponse
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBearer
from fastapi import UploadFile, Form, HTTPException, Depends, status

import base64
import os
import shutil
import uvicorn
import asyncpg
import time
import asyncio
from functools import wraps
from typing import Optional

# --- Optional Redis (auto if REDIS_URL is set) ---
try:
    import aioredis  # type: ignore
except Exception:
    aioredis = None  # graceful fallback

from auth import router as auth_router
from compare import router as compare_router
from products import router as products_router
from upload_prices import router as upload_router

# Load .env
load_dotenv()

# ---------- Static paths (env-first for Railway Volume) ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# If STATIC_DIR is set (e.g., /data/static on Railway), use it; otherwise use repo's ./static
STATIC_DIR = os.getenv("STATIC_DIR", os.path.join(BASE_DIR, "static"))
IMAGES_DIR = os.path.join(STATIC_DIR, "images")
os.makedirs(IMAGES_DIR, exist_ok=True)
# -----------------------------------------------------------------

app = FastAPI()
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Optional: cache static images aggressively (good for CDN)
@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    if request.url.path.startswith("/static/"):
        resp.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")
    # small security set
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    # CSP is conservative; adjust if you host web frontend here
    resp.headers.setdefault("Content-Security-Policy", "default-src 'none'; img-src * data: blob:; media-src *;")
    return resp

# --------- CORS (tighten via env) ----------
# Set APP_WEB_ORIGIN="https://your.site" or comma-separate multiple
origins_env = (os.getenv("APP_WEB_ORIGIN") or "").strip()
allow_origins = [o.strip() for o in origins_env.split(",") if o.strip()] or ["*"]  # keep * until you set env
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET","POST","PUT","DELETE","OPTIONS"],
    allow_headers=["Authorization","Content-Type"],
)

# Swagger Basic Auth (for /docs, /redoc, /openapi.json)
class SwaggerAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.username = os.getenv("SWAGGER_USERNAME")
        self.password = os.getenv("SWAGGER_PASSWORD")

    async def dispatch(self, request: Request, call_next):
        protected_paths = ["/docs", "/redoc", "/openapi.json"]
        if any(request.url.path.startswith(p) for p in protected_paths):
            auth = request.headers.get("Authorization")
            expected = f"{self.username}:{self.password}"
            expected_encoded = "Basic " + base64.b64encode(expected.encode()).decode()
            if auth != expected_encoded:
                return Response(status_code=401, headers={"WWW-Authenticate": "Basic"}, content="Unauthorized")
        return await call_next(request)

app.add_middleware(SwaggerAuthMiddleware)

# --- Reuse the same Basic Auth for admin pages (/ and /upload) + optional IP allowlist ---
def _admin_ip_allowed(req: Request) -> bool:
    allow_env = os.getenv("ADMIN_IP_ALLOWLIST", "").strip()
    if not allow_env:
        return True
    allowed = {ip.strip() for ip in allow_env.split(",") if ip.strip()}
    return req.client and req.client.host in allowed

def basic_guard(req: Request):
    if not _admin_ip_allowed(req):
        raise HTTPException(status_code=403, detail="Admin IP not allowed")
    username = os.getenv("SWAGGER_USERNAME")
    password = os.getenv("SWAGGER_PASSWORD")
    if not username or not password:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Admin auth not configured")
    expected = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
    auth = req.headers.get("Authorization")
    if auth != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

# ------------- Rate limiting middleware (per IP + per token) -------------
RATE_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MINUTE", "300"))   # global soft cap
WINDOW = 60

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.redis_url: Optional[str] = os.getenv("REDIS_URL")
        self.redis = None
        self.local_counts = {}  # {(key, bucket): count}

    async def _hit_local(self, key: str) -> int:
        now_bucket = int(time.time() // WINDOW)
        k = (key, now_bucket)
        self.local_counts[k] = self.local_counts.get(k, 0) + 1
        # prune old buckets occasionally
        if len(self.local_counts) > 5000:
            old = [kk for kk in self.local_counts if kk[1] < now_bucket]
            for kk in old:
                self.local_counts.pop(kk, None)
        return self.local_counts[k]

    async def _hit_redis(self, key: str) -> int:
        if self.redis is None:
            # lazily connect
            self.redis = await aioredis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
        bucket = f"{key}:{int(time.time()//WINDOW)}"
        n = await self.redis.incr(bucket)
        if n == 1:
            await self.redis.expire(bucket, WINDOW)
        return n

    async def dispatch(self, request: Request, call_next):
        # Skip static and docs
        path = request.url.path
        if path.startswith("/static/") or path in ("/robots.txt", "/healthz"):
            return await call_next(request)

        token = (request.headers.get("authorization") or "").split()[-1] or "anon"
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
            # fail-open on limiter errors
            return await call_next(request)

        if n_user > RATE_PER_MIN or n_ip > RATE_PER_MIN:
            return JSONResponse({"detail": "rate limit"}, status_code=429)

        return await call_next(request)

app.add_middleware(RateLimitMiddleware)

# ----------- Tiny per-route throttle decorator (reuse on search endpoints) -----------
def throttle(limit:int, window:int=60):
    buckets = {}
    lock = asyncio.Lock()
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            request: Request = kwargs.get("request")
            if not request:
                # try positional
                for a in args:
                    if isinstance(a, Request):
                        request = a
                        break
            ip = request.client.host if request and request.client else "unknown"
            name = fn.__name__
            bucket = (ip, name, int(time.time()//window))
            async with lock:
                buckets[bucket] = buckets.get(bucket, 0) + 1
                if buckets[bucket] > limit:
                    raise HTTPException(status_code=429, detail="Too many requests")
            return await fn(*args, **kwargs)
        return wrapper
    return decorator

# DB Pool
DATABASE_URL = os.getenv("DATABASE_URL")

@app.on_event("startup")
async def startup():
    app.state.db = await asyncpg.create_pool(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

# Include routes
app.include_router(auth_router)
app.include_router(compare_router)
app.include_router(products_router)
app.include_router(upload_router)

# robots.txt (policy helps your legal footing)
@app.get("/robots.txt", response_class=PlainTextResponse := type(
    "PlainTextResponse",
    (Response,),
    {"media_type": "text/plain"}
))
async def robots():
    # Disallow API/data endpoints
    return "User-agent: *\nDisallow: /products\nDisallow: /search-products\nDisallow: /compare\n"

# Simple health check
@app.get("/healthz")
async def healthz():
    return {"ok": True}

# Dashboard for missing product images (Basic Auth)
@app.get("/", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def dashboard(request: Request):
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT
                   product,
                   COALESCE(manufacturer,'') AS manufacturer,
                   COALESCE(amount,'')       AS amount
            FROM prices
            WHERE (image_url IS NULL OR image_url = '' OR image_url = 'missing.jpg')
               OR note = 'Kontrolli visuaali!'
            ORDER BY product
        """)
    html = "<h2>Missing Product Images</h2><ul>"
    for row in rows:
        product = row["product"]
        manufacturer = row["manufacturer"]
        amount = row["amount"]
        html += f"""
        <li>
            <b>{product}</b>
            {' · ' + manufacturer if manufacturer else ''}
            {' · ' + amount if amount else ''}
            <form action="/upload" method="post" enctype="multipart/form-data" style="margin-top:6px">
                <input type="hidden" name="product" value="{product}"/>
                <input type="hidden" name="manufacturer" value="{manufacturer}"/>
                <input type="hidden" name="amount" value="{amount}"/>
                <input type="file" name="image" accept="image/*" required/>
                <button type="submit">Upload</button>
            </form>
        </li>
        """
    html += "</ul>"
    return html

# Upload image once → apply to all matching products across stores (Basic Auth)
MAX_UPLOAD_MB = int(os.getenv("MAX_IMAGE_MB", "6"))

@app.post("/upload", dependencies=[Depends(basic_guard)])
async def upload_image(
    request: Request,                                     # ⬅️ need Request to decide HTML vs JSON
    product: str = Form(...),
    image: UploadFile = Form(...),
    manufacturer: str = Form(""),
    amount: str = Form("")
):
    def wants_html(req: Request) -> bool:
        accept = (req.headers.get("accept") or "").lower()
        return "text/html" in accept and "application/json" not in accept

    try:
        # reject large files early if content-length is provided
        cl = request.headers.get("content-length")
        if cl and int(cl) > MAX_UPLOAD_MB * 1024 * 1024:
            raise HTTPException(413, f"Image too large (>{MAX_UPLOAD_MB}MB)")

        # safe filename
        safe_base = (
            product.replace("/", "_")
                   .replace("\\", "_")
                   .replace(" ", "_")
                   .strip()
        )
        # keep original extension if present, default to .jpg
        ext = os.path.splitext(image.filename or "")[1].lower() or ".jpg"
        filename = f"{safe_base}{ext}"

        # save under images dir (volume-backed if STATIC_DIR=/data/static)
        file_path = os.path.join(IMAGES_DIR, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)

        # size check after save (fallback)
        try:
            if os.path.getsize(file_path) > MAX_UPLOAD_MB * 1024 * 1024:
                os.remove(file_path)
                raise HTTPException(413, f"Image too large (>{MAX_UPLOAD_MB}MB)")
        except Exception:
            pass

        # build absolute (CDN) URL if configured; else relative
        cdn_base = os.getenv("CDN_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or ""
        image_path = f"/static/images/{filename}"
        image_url = f"{cdn_base.rstrip('/')}{image_path}" if cdn_base else image_path

        # update ALL rows across stores for same product (+ optional manufacturer/amount)
        async with app.state.db.acquire() as conn:
            if manufacturer or amount:
                status_txt = await conn.execute("""
                    UPDATE prices
                       SET image_url = $4,
                           note = CASE WHEN note = 'Kontrolli visuaali!' THEN '' ELSE note END
                     WHERE LOWER(product) = LOWER($1)
                       AND LOWER(COALESCE(manufacturer,'')) = LOWER($2)
                       AND LOWER(COALESCE(amount,'')) = LOWER($3)
                """, product.strip(), manufacturer.strip(), amount.strip(), image_url)
            else:
                status_txt = await conn.execute("""
                    UPDATE prices
                       SET image_url = $2,
                           note = CASE WHEN note = 'Kontrolli visuaali!' THEN '' ELSE note END
                     WHERE LOWER(product) = LOWER($1)
                """, product.strip(), image_url)

        updated_rows = 0
        try:
            updated_rows = int((status_txt or "0").split()[-1])
        except Exception:
            pass

        if wants_html(request):
            # friendly confirmation page
            html = f"""
            <h2>✅ Image uploaded</h2>
            <p><b>Product:</b> {product}</p>
            <p><b>Rows updated:</b> {updated_rows}</p>
            <p><img src="{image_url}" alt="{product}" style="max-width:520px;height:auto;border:1px solid #eee"/></p>
            <p><a href="/">← Back to Missing Product Images</a></p>
            """
            return HTMLResponse(html)

        # default JSON for API clients
        saved = os.path.exists(file_path)
        size_bytes = os.path.getsize(file_path) if saved else 0
        return JSONResponse({
            "status": "success",
            "product": product,
            "image_url": image_url,
            "rows_updated": updated_rows,
            "saved": saved,
            "bytes": size_bytes
        })

    except HTTPException:
        raise
    except Exception as e:
        if wants_html(request):
            return HTMLResponse(
                f"<h2>❌ Upload failed</h2><pre>{str(e)}</pre><p><a href='/'>← Back</a></p>",
                status_code=500
            )
        raise

# Swagger bearer token support (for API docs)
bearer_scheme = HTTPBearer()

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Grocery App",
        version="1.0.0",
        description="Compare prices, upload product data, and manage users",
        routes=app.routes,
    )

    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT"
        }
    }

    for path in openapi_schema["paths"].values():
        for operation in path.values():
            operation.setdefault("security", [{"BearerAuth": []}])

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT","8000")), reload=True, log_level="debug")

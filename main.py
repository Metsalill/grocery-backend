from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
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
import logging
from typing import Optional

# import throttle from a helper to avoid circular imports
from utils.throttle import throttle  # <-- uses utils/throttle.py

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

logger = logging.getLogger("uvicorn.error")

# ---------- Static paths ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.getenv("STATIC_DIR", os.path.join(BASE_DIR, "static"))
IMAGES_DIR = os.path.join(STATIC_DIR, "images")
os.makedirs(IMAGES_DIR, exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Optional: cache static images aggressively (good for CDN)
@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    path = request.url.path

    # Cache + CSP only for static assets
    if path.startswith("/static/"):
        resp.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")
        # Keep a CSP for static assets only; do NOT apply globally or Swagger will break.
        resp.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src * data: blob:; media-src *;"
        )

    # Safe security headers applied globally
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return resp

# --------- CORS ----------
origins_env = (os.getenv("APP_WEB_ORIGIN") or "").strip()
allow_origins = [o.strip() for o in origins_env.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Swagger Basic Auth
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

# --- Optional simple request logger (enable with LOG_REQUESTS=true) ---
if (os.getenv("LOG_REQUESTS") or "").lower() in {"1", "true", "yes"}:
    @app.middleware("http")
    async def _req_logger(request: Request, call_next):
        logger.info(f"➡ {request.method} {request.url.path} from {request.client.host if request.client else 'unknown'}")
        resp = await call_next(request)
        logger.info(f"⬅ {request.method} {request.url.path} -> {resp.status_code}")
        return resp

# --- Admin guard ---
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

# --- Rate limiting middleware ---
RATE_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MINUTE", "300"))
WINDOW = 60

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.redis_url: Optional[str] = os.getenv("REDIS_URL")
        self.redis = None
        self.local_counts = {}

    async def _hit_local(self, key: str) -> int:
        now_bucket = int(time.time() // WINDOW)
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
        bucket = f"{key}:{int(time.time()//WINDOW)}"
        n = await self.redis.incr(bucket)
        if n == 1:
            await self.redis.expire(bucket, WINDOW)
        return n

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip static, health, robots, and docs-related endpoints
        if (
            path.startswith("/static/")
            or path in ("/robots.txt", "/healthz", "/favicon.ico")
            or path.startswith("/docs")
            or path.startswith("/redoc")
            or path.startswith("/openapi.json")
        ):
            return await call_next(request)

        # Defensive token parsing
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
            # fail-open on limiter errors
            return await call_next(request)

        if n_user > RATE_PER_MIN or n_ip > RATE_PER_MIN:
            return JSONResponse({"detail": "rate limit"}, status_code=429)

        return await call_next(request)

app.add_middleware(RateLimitMiddleware)

# --- DB Pool (resilient startup) ---
DATABASE_URL = os.getenv("DATABASE_URL")
DB_CONNECT_TIMEOUT = float(os.getenv("DB_CONNECT_TIMEOUT", "8"))

@app.on_event("startup")
async def startup():
    try:
        app.state.db = await asyncpg.create_pool(DATABASE_URL, timeout=DB_CONNECT_TIMEOUT)
        logger.info("✅ DB pool created")
    except Exception as e:
        app.state.db = None
        logger.error(f"⚠️ Failed to connect to DB at startup: {e}")

@app.on_event("shutdown")
async def shutdown():
    try:
        if getattr(app.state, "db", None):
            await app.state.db.close()
            logger.info("🔌 DB pool closed")
    except Exception as e:
        logger.error(f"Shutdown error: {e}")

# Include routes
app.include_router(auth_router)
app.include_router(compare_router)
app.include_router(products_router)
app.include_router(upload_router)

# robots.txt
@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots():
    return "User-agent: *\nDisallow: /products\nDisallow: /search-products\nDisallow: /compare\n"

# Simple health check (plain text, no DB)
@app.get("/healthz", response_class=PlainTextResponse)
async def healthz():
    return "ok"

# Dashboard
@app.get("/", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def dashboard(request: Request):
    if getattr(app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet. Try again in a few seconds.</h2>", status_code=503)

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

# Upload
MAX_UPLOAD_MB = int(os.getenv("MAX_IMAGE_MB", "6"))

@app.post("/upload", dependencies=[Depends(basic_guard)])
async def upload_image(
    request: Request,
    product: str = Form(...),
    image: UploadFile = Form(...),
    manufacturer: str = Form(""),
    amount: str = Form("")
):
    def wants_html(req: Request) -> bool:
        accept = (req.headers.get("accept") or "").lower()
        return "text/html" in accept and "application/json" not in accept

    try:
        cl = request.headers.get("content-length")
        if cl and int(cl) > MAX_UPLOAD_MB * 1024 * 1024:
            raise HTTPException(413, f"Image too large (>{MAX_UPLOAD_MB}MB)")

        safe_base = (
            product.replace("/", "_")
                   .replace("\\", "_")
                   .replace(" ", "_")
                   .strip()
        )
        ext = os.path.splitext(image.filename or "")[1].lower() or ".jpg"
        filename = f"{safe_base}{ext}"

        file_path = os.path.join(IMAGES_DIR, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)

        try:
            if os.path.getsize(file_path) > MAX_UPLOAD_MB * 1024 * 1024:
                os.remove(file_path)
                raise HTTPException(413, f"Image too large (>{MAX_UPLOAD_MB}MB)")
        except Exception:
            pass

        cdn_base = os.getenv("CDN_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or ""
        image_path = f"/static/images/{filename}"
        image_url = f"{cdn_base.rstrip('/')}{image_path}" if cdn_base else image_path

        if getattr(app.state, "db", None) is None:
            raise HTTPException(status_code=503, detail="Database not ready")

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
            html = f"""
            <h2>✅ Image uploaded</h2>
            <p><b>Product:</b> {product}</p>
            <p><b>Rows updated:</b> {updated_rows}</p>
            <p><img src="{image_url}" alt="{product}" style="max-width:520px;height:auto;border:1px solid #eee"/></p>
            <p><a href="/">← Back to Missing Product Images</a></p>
            """
            return HTMLResponse(html)

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

# Swagger bearer token
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

app.openapi = custom_openapi()

if __name__ == "__main__":
    # For local dev only; on Railway you're using the start command.
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True, log_level="debug")

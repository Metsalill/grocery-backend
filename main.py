# main.py
import os
import logging
import asyncpg
import traceback  # <-- DEV helper
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware  # <-- DEV helper

from settings import (
    ENABLE_DOCS, STATIC_DIR, IMAGES_DIR,
    ALLOW_ORIGINS, DATABASE_URL, DB_CONNECT_TIMEOUT,
    LOG_REQUESTS, RATE_PER_MIN, REDIS_URL, WINDOW,
)
from middlewares.headers import security_and_cache_headers
from middlewares.rate_limit import RateLimitMiddleware
from middlewares.docs_guard import SwaggerAuthMiddleware

from auth import router as auth_router
from compare import router as compare_router
from products import router as products_router
from upload_prices import router as upload_router
from admin.routes import router as admin_router
from basket_history import router as basket_history_router
from api.upload_image import router as upload_image_router            # manual R2 upload
from admin.image_gallery import router as image_admin_router          # <-- NEW: image gallery

logger = logging.getLogger("uvicorn.error")
os.makedirs(IMAGES_DIR, exist_ok=True)

app = FastAPI(
    title="Grocery App",
    version="1.0.0",
    docs_url="/docs" if ENABLE_DOCS else None,
    redoc_url="/redoc" if ENABLE_DOCS else None,
    openapi_url="/openapi.json" if ENABLE_DOCS else None,
)

# --- Traceback middleware (DEV helper) ---
class TraceLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            return await call_next(request)
        except Exception:
            logger.error("\n===== UNCAUGHT EXCEPTION =====")
            logger.error("Path: %s %s", request.method, request.url.path)
            logger.error(traceback.format_exc())
            logger.error("===== END TRACE =====\n")
            raise

app.add_middleware(TraceLogMiddleware)
# -----------------------------------------

# Static + headers
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.middleware("http")(security_and_cache_headers)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Docs basic auth (only if docs are enabled)
if ENABLE_DOCS:
    from settings import SWAGGER_USERNAME, SWAGGER_PASSWORD
    app.add_middleware(SwaggerAuthMiddleware, username=SWAGGER_USERNAME, password=SWAGGER_PASSWORD)

# Rate limit
app.add_middleware(RateLimitMiddleware, rate_per_min=RATE_PER_MIN, window=WINDOW, redis_url=REDIS_URL)

# DB pool
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

# Routers
app.include_router(auth_router)
app.include_router(compare_router)
app.include_router(products_router)
app.include_router(upload_router)
app.include_router(admin_router)
app.include_router(basket_history_router)
app.include_router(upload_image_router)   # manual R2 upload endpoint
app.include_router(image_admin_router)    # <-- NEW: /admin/images gallery

# robots.txt + health
@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots():
    return (
        "User-agent: *\n"
        "Disallow: /products\n"
        "Disallow: /search-products\n"
        "Disallow: /compare\n"
        "Disallow: /basket-history\n"
        "Disallow: /api/upload-image\n"
        "Disallow: /admin/images\n"  # <-- NEW: hide gallery from bots
    )

@app.get("/healthz", response_class=PlainTextResponse)
async def healthz():
    return "ok"

# Optional request logging
if LOG_REQUESTS:
    @app.middleware("http")
    async def _req_logger(request, call_next):
        logger.info(f"➡ {request.method} {request.url.path} from {request.client.host if request.client else 'unknown'}")
        resp = await call_next(request)
        logger.info(f"⬅ {request.method} {request.url.path} -> {resp.status_code}")
        return resp

# OpenAPI security default
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBearer

bearer_scheme = HTTPBearer()

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title="Grocery App",
        version="1.0.0",
        description="Compare prices, upload product data, and manage users",
        routes=app.routes,
    )
    schema["components"]["securitySchemes"] = {
        "BearerAuth": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
    }
    for path in schema["paths"].values():
        for operation in path.values():
            operation.setdefault("security", [{"BearerAuth": []}])
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True, log_level="debug")

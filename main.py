from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
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
    return resp

# CORS for frontend (tighten later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

# --- Reuse the same Basic Auth for admin pages (/ and /upload) ---
def basic_guard(req: Request):
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

# Dashboard for missing product images (Basic Auth)
@app.get("/", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def dashboard():
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
@app.post("/upload", dependencies=[Depends(basic_guard)])
async def upload_image(
    product: str = Form(...),
    image: UploadFile = Form(...),
    manufacturer: str = Form(""),
    amount: str = Form("")
):
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

    # build absolute (CDN) URL if configured; else relative
    cdn_base = os.getenv("CDN_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or ""
    image_path = f"/static/images/{filename}"
    image_url = f"{cdn_base.rstrip('/')}{image_path}" if cdn_base else image_path

    # update ALL rows across stores for same product (+ optional manufacturer/amount)
    async with app.state.db.acquire() as conn:
        if manufacturer or amount:
            await conn.execute("""
                UPDATE prices
                   SET image_url = $4,
                       note = CASE WHEN note = 'Kontrolli visuaali!' THEN '' ELSE note END
                 WHERE LOWER(product) = LOWER($1)
                   AND LOWER(COALESCE(manufacturer,'')) = LOWER($2)
                   AND LOWER(COALESCE(amount,'')) = LOWER($3)
            """, product.strip(), manufacturer.strip(), amount.strip(), image_url)
        else:
            await conn.execute("""
                UPDATE prices
                   SET image_url = $2,
                       note = CASE WHEN note = 'Kontrolli visuaali!' THEN '' ELSE note END
                 WHERE LOWER(product) = LOWER($1)
            """, product.strip(), image_url)

    saved = os.path.exists(file_path)
    size_bytes = os.path.getsize(file_path) if saved else 0

    return {
        "status": "success",
        "product": product,
        "image_url": image_url,
        "saved": saved,
        "bytes": size_bytes
    }

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
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="debug")

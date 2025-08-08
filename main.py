from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBearer

import base64
import os
import shutil
import uvicorn
import asyncpg
from fastapi import UploadFile, Form, HTTPException

from auth import router as auth_router
from compare import router as compare_router
from products import router as products_router
from upload_prices import router as upload_router

# Load .env
load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

# Dashboard for missing product images
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT product, image_url FROM prices WHERE note = 'Kontrolli visuaali!'")
    html = "<h2>Missing Product Images</h2><ul>"
    for row in rows:
        html += f"""
        <li>
            <b>{row['product']}</b><br>
            <form action=\"/upload\" method=\"post\" enctype=\"multipart/form-data\">
                <input type=\"hidden\" name=\"product\" value=\"{row['product']}\"/>
                <input type=\"file\" name=\"image\"/>
                <button type=\"submit\">Upload</button>
            </form>
        </li>
        """
    html += "</ul>"
    return html

@app.post("/upload")
async def upload_image(product: str = Form(...), image: UploadFile = Form(...)):
    filename = f"{product.replace(' ', '_')}.jpg"
    path = f"static/images/{filename}"

    os.makedirs("static/images", exist_ok=True)
    with open(path, "wb") as buffer:
        shutil.copyfileobj(image.file, buffer)

    image_url = f"/static/images/{filename}"

    async with app.state.db.acquire() as conn:
        await conn.execute("UPDATE prices SET image_url = $1, note = '' WHERE product = $2", image_url, product)

    return {"status": "success", "product": product}

# Swagger bearer token support
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

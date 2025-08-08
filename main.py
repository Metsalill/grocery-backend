from auth import router as auth_router
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import os
load_dotenv()

import shutil
import pandas as pd
import io
import asyncpg
from typing import List
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
import base64
import uvicorn
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBearer
from geopy.distance import geodesic

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

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/grocerydb")

@app.on_event("startup")
async def startup():
    app.state.db = await asyncpg.create_pool(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

# Models
class GroceryItem(BaseModel):
    product: str
    quantity: int = 1

class GroceryList(BaseModel):
    items: List[GroceryItem]

# Upload prices
@app.post("/upload-prices")
async def upload_prices(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith(".xlsx"):
            raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        df.rename(columns={
            "Toode": "product",
            "Tootja": "manufacturer",
            "Kogus": "amount",
            "Hind (€)": "price"
        }, inplace=True)

        required_columns = {"product", "manufacturer", "amount", "price"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail="Missing required columns in Excel")

        # Extract store name from filename
        store_name = file.filename.replace(".xlsx", "").replace("_tooted", "").replace("_", " ").title()

        async with app.state.db.acquire() as conn:
            # 1. Find or insert store
            store_row = await conn.fetchrow("SELECT id FROM stores WHERE name = $1", store_name)
            if not store_row:
                await conn.execute("""
                    INSERT INTO stores (name, chain, lat, lon)
                    VALUES ($1, $2, $3, $4)
                """, store_name, store_name.split()[0], 0.0, 0.0)
                store_row = await conn.fetchrow("SELECT id FROM stores WHERE name = $1", store_name)

            store_id = store_row["id"]

            # 2. Insert products with store_id
            for _, row in df.iterrows():
                await conn.execute("""
                    INSERT INTO prices (store_id, product, manufacturer, amount, price)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (store_id, product, manufacturer, amount) DO UPDATE
                    SET price = EXCLUDED.price
                """, store_id, row["product"], row["manufacturer"], row["amount"], float(row["price"]))

                await conn.execute("""
                    UPDATE prices
                    SET note = 'Kontrolli visuaali!'
                    WHERE store_id = $1 AND product = $2 AND manufacturer = $3 AND amount = $4
                    AND (image_url IS NULL OR image_url = '')
                """, store_id, row["product"], row["manufacturer"], row["amount"])

        return {"status": "success", "store": store_name, "items_uploaded": len(df)}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}

# Compare basket with location filtering
@app.post("/compare")
async def compare_basket(grocery_list: GroceryList, lat: float = Form(...), lon: float = Form(...), radius_km: float = 5.0):
    try:
        async with app.state.db.acquire() as conn:
            # 1. Find nearby stores
            store_rows = await conn.fetch("SELECT id, name, lat, lon FROM stores")
            nearby_store_ids = []

            for row in store_rows:
                store_location = (row["lat"], row["lon"])
                user_location = (lat, lon)
                if geodesic(store_location, user_location).km <= radius_km:
                    nearby_store_ids.append(row["id"])

            if not nearby_store_ids:
                raise HTTPException(status_code=404, detail="No stores found within given radius")

            # 2. Fetch prices for only nearby stores
            prices = {}

            for item in grocery_list.items:
                rows = await conn.fetch("""
                    SELECT s.name AS store_name, p.price 
                    FROM prices p
                    JOIN stores s ON p.store_id = s.id
                    WHERE LOWER(p.product) = LOWER($1)
                    AND s.id = ANY($2::int[])
                """, item.product, nearby_store_ids)

                for row in rows:
                    store = row["store_name"]
                    unit_price = float(row["price"])
                    total_price = unit_price * item.quantity

                    prices.setdefault(store, 0.0)
                    prices[store] += total_price

            if not prices:
                raise HTTPException(status_code=404, detail="No matching products found in nearby stores")

            return dict(sorted({store: round(total, 2) for store, total in prices.items()}.items(), key=lambda x: x[1]))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Get all products
@app.get("/products")
async def list_products():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.name as store, p.product, p.price, p.manufacturer, p.amount, p.image_url, p.note 
            FROM prices p
            JOIN stores s ON p.store_id = s.id
            ORDER BY s.name
        """)
    return [
        {
            "store": row["store"],
            "product": row["product"],
            "price": round(float(row["price"]), 2),
            "manufacturer": row["manufacturer"],
            "amount": row["amount"],
            "image_url": row["image_url"],
            "note": row["note"]
        }
        for row in rows
    ]

@app.get("/search-products")
async def search_products(query: str):
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT product, image_url 
            FROM prices 
            WHERE LOWER(product) ILIKE '%' || LOWER($1) || '%' 
            ORDER BY product 
            LIMIT 10
        """, query)
    return [{"name": row["product"], "image": row["image_url"]} for row in rows]

@app.get("/stores/nearby")
async def stores_nearby(lat: float, lon: float, radius_km: float = 5.0):
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, chain, lat, lon FROM stores")

    result = []
    for row in rows:
        store_location = (row["lat"], row["lon"])
        user_location = (lat, lon)
        distance_km = geodesic(user_location, store_location).km

        if distance_km <= radius_km:
            result.append({
                "id": row["id"],
                "name": row["name"],
                "chain": row["chain"],
                "lat": row["lat"],
                "lon": row["lon"],
                "distance_km": round(distance_km, 2)
            })

    return sorted(result, key=lambda x: x["distance_km"])

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

# Auth router
app.include_router(auth_router)

# Swagger security
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

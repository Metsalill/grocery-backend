from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import shutil
import pandas as pd
import io
import asyncpg
import os
from typing import List

app = FastAPI()

from fastapi.staticfiles import StaticFiles
app.mount("/static", StaticFiles(directory="static"), name="static")

# Allow CORS for frontend testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DATABASE URL (replace with your actual Supabase/Postgres URL)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/grocerydb")

# Database connection pool
@app.on_event("startup")
async def startup():
    app.state.db = await asyncpg.create_pool(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

# Pydantic model for grocery list input
class GroceryItem(BaseModel):
    name: str

class GroceryList(BaseModel):
    items: List[GroceryItem]

# Endpoint to upload Excel and store in DB
@app.post("/upload-prices")
async def upload_prices(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith(".xlsx"):
            raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        required_columns = {"Toode", "Tootja", "Kogus", "Hind (€)"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail="Missing required columns in Excel")

        store_name = file.filename.replace("_extended_prices.xlsx", "").replace("_", " ").title()

        async with app.state.db.acquire() as conn:
            for _, row in df.iterrows():
                await conn.execute("""
                    INSERT INTO prices (store, product, manufacturer, amount, price)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (store, product, manufacturer, amount) DO UPDATE
                    SET price = EXCLUDED.price
                """, store_name, row["Toode"], row["Tootja"], row["Kogus"], float(row["Hind (€)"]))

        return {"status": "success", "store": store_name, "items_uploaded": len(df)}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}

# Endpoint to compare basket prices
@app.post("/compare")
async def compare_basket(grocery_list: GroceryList):
    try:
        async with app.state.db.acquire() as conn:
            prices = {}
            for item in grocery_list.items:
                rows = await conn.fetch(
                    "SELECT store, price FROM prices WHERE LOWER(product) = LOWER($1)", item.name
                )
                for row in rows:
                    prices.setdefault(row["store"], 0.0)
                    prices[row["store"]] += float(row["price"])

        if not prices:
            raise HTTPException(status_code=404, detail="No matching products found")

        return dict(sorted({store: round(total, 2) for store, total in prices.items()}.items(), key=lambda x: x[1]))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to list all stored products
@app.get("/products")
async def list_products():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT store, product, manufacturer, amount, price FROM prices ORDER BY store")
    return [dict(row) for row in rows]

# 🔍 New endpoint: search products by name (for image-grid frontend)
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

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT product, image_url FROM prices WHERE note = 'Kontrolli visuaali!'")
    html = "<h2>Missing Product Images</h2><ul>"
    for row in rows:
        html += f"""
        <li>
            <b>{row['product']}</b><br>
            <form action="/upload" method="post" enctype="multipart/form-data">
                <input type="hidden" name="product" value="{row['product']}"/>
                <input type="file" name="image"/>
                <button type="submit">Upload</button>
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
        await conn.execute(
            "UPDATE prices SET image_url = $1, note = '' WHERE product = $2",
            image_url, product
        )

    return {"status": "success", "product": product}

import uvicorn

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import io
import asyncpg
import os
from typing import List

app = FastAPI()

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
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))

    required_columns = {"Toode", "Hind (€)"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=400, detail="Missing required columns in Excel")

    # Infer store name from filename
    store_name = file.filename.replace("_extended_prices.xlsx", "").replace("_", " ").title()

    # Store in DB
    async with app.state.db.acquire() as conn:
        for _, row in df.iterrows():
            await conn.execute("""
                INSERT INTO prices (store, product, price) 
                VALUES ($1, $2, $3)
                ON CONFLICT (store, product) DO UPDATE SET price = EXCLUDED.price
            """, store_name, row["Toode"], float(row["Hind (€)"]))

    return {"status": "success", "store": store_name, "items_uploaded": len(df)}

# Endpoint to compare basket prices
@app.post("/compare")
async def compare_basket(grocery_list: GroceryList):
    async with app.state.db.acquire() as conn:
        prices = {}
        for item in grocery_list.items:
            rows = await conn.fetch("SELECT store, price FROM prices WHERE LOWER(product) = LOWER($1)", item.name)
            for row in rows:
                prices.setdefault(row["store"], 0.0)
                prices[row["store"]] += row["price"]

    if not prices:
        raise HTTPException(status_code=404, detail="No matching products found")

    return dict(sorted(prices.items(), key=lambda x: x[1]))

# Endpoint to list all stored products
@app.get("/products")
async def list_products():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT store, product, price FROM prices ORDER BY store")
    return [dict(row) for row in rows]
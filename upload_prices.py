# upload_prices.py
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
import pandas as pd
import io

router = APIRouter()

@router.post("/upload-prices")
async def upload_prices(
    file: UploadFile = File(...),
    lat: float = Form(0.0),
    lon: float = Form(0.0),
):
    from main import app  # Access DB pool from main

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

        store_name = file.filename.replace(".xlsx", "").replace("_tooted", "").replace("_", " ").title()

        async with app.state.db.acquire() as conn:
            store_row = await conn.fetchrow("SELECT id FROM stores WHERE name = $1", store_name)
            if not store_row:
                await conn.execute("""
                    INSERT INTO stores (name, chain, lat, lon)
                    VALUES ($1, $2, $3, $4)
                """, store_name, store_name.split()[0], lat, lon)
                store_row = await conn.fetchrow("SELECT id FROM stores WHERE name = $1", store_name)

            store_id = store_row["id"]

            # Insert/update prices
            for _, row in df.iterrows():
                await conn.execute("""
                    INSERT INTO prices (store_id, product, manufacturer, amount, price)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (store_id, product, manufacturer, amount) DO UPDATE
                    SET price = EXCLUDED.price
                """, store_id, row["product"], row["manufacturer"], row["amount"], float(row["price"]))

            # 🔹 Bulk auto-flag missing images for THIS store
            await conn.execute("""
                UPDATE prices
                SET image_url = 'missing.jpg',
                    note      = COALESCE(NULLIF(note, ''), 'Kontrolli visuaali!')
                WHERE store_id = $1
                  AND (image_url IS NULL OR image_url = '')
            """, store_id)

        return {
            "status": "success",
            "store": store_name,
            "items_uploaded": len(df)
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}

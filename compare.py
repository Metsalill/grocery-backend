from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
from geopy.distance import geodesic

router = APIRouter()

class GroceryItem(BaseModel):
    product: str
    quantity: int = 1

class GroceryList(BaseModel):
    items: List[GroceryItem]

class CompareRequest(BaseModel):
    grocery_list: GroceryList
    lat: float
    lon: float
    radius_km: float = 10.0

@router.post("/compare")
async def compare_basket(body: CompareRequest, request):
    try:
        grocery_list = body.grocery_list
        lat = body.lat
        lon = body.lon
        radius_km = body.radius_km

        async with request.app.state.db.acquire() as conn:
            store_rows = await conn.fetch("SELECT id, name, lat, lon FROM stores")
            nearby_store_ids = []

            for row in store_rows:
                store_location = (row["lat"], row["lon"])
                user_location = (lat, lon)
                if geodesic(store_location, user_location).km <= radius_km:
                    nearby_store_ids.append(row["id"])

            if not nearby_store_ids:
                raise HTTPException(status_code=404, detail="No stores found within given radius")

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

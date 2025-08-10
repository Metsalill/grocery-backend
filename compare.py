from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Dict
from geopy.distance import geodesic

# import throttle decorator from utils instead of main
from utils.throttle import throttle

router = APIRouter()

# ---- server-side caps ----
MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 15.0

class GroceryItem(BaseModel):
    product: str
    quantity: conint(ge=1) = 1

class GroceryList(BaseModel):
    items: List[GroceryItem]

class CompareRequest(BaseModel):
    grocery_list: GroceryList
    lat: float
    lon: float
    radius_km: confloat(ge=MIN_RADIUS, le=MAX_RADIUS) = 2.0

@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(body: CompareRequest, request: Request):
    try:
        grocery_list = body.grocery_list
        user_lat = body.lat
        user_lon = body.lon
        radius_km = float(max(MIN_RADIUS, min(MAX_RADIUS, float(body.radius_km))))

        if not grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(grocery_list.items) > MAX_ITEMS:
            raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")
        for it in grocery_list.items:
            if not it.product or not it.product.strip():
                raise HTTPException(status_code=400, detail="Product name cannot be empty")

        user_location = (user_lat, user_lon)

        async with request.app.state.db.acquire() as conn:
            store_rows = await conn.fetch("SELECT id, name, lat, lon FROM stores")

            nearby_store_ids: List[int] = []
            store_dist_km: Dict[int, float] = {}
            store_name_by_id: Dict[int, str] = {}

            for row in store_rows:
                store_id = row["id"]
                store_name = row["name"]
                store_lat = row["lat"]
                store_lon = row["lon"]

                store_name_by_id[store_id] = store_name

                if store_lat is None or store_lon is None:
                    continue

                distance = geodesic((store_lat, store_lon), user_location).km
                if distance <= radius_km:
                    nearby_store_ids.append(store_id)
                    store_dist_km[store_id] = round(float(distance), 2)

            if not nearby_store_ids:
                raise HTTPException(status_code=404, detail="No stores found within given radius")

            totals_by_id: Dict[int, float] = {}

            for item in grocery_list.items:
                rows = await conn.fetch(
                    """
                    SELECT s.id AS store_id, s.name AS store_name, p.price
                    FROM prices p
                    JOIN stores s ON p.store_id = s.id
                    WHERE LOWER(p.product) = LOWER($1)
                      AND s.id = ANY($2::int[])
                    """,
                    item.product.strip(),
                    nearby_store_ids
                )

                for r in rows:
                    sid = r["store_id"]
                    unit_price = float(r["price"])
                    totals_by_id[sid] = totals_by_id.get(sid, 0.0) + unit_price * item.quantity

            if not totals_by_id:
                raise HTTPException(status_code=404, detail="No matching products found in nearby stores")

            results = []
            totals_by_name: Dict[str, float] = {}

            for sid, total in totals_by_id.items():
                name = store_name_by_id.get(sid, f"Store {sid}")
                dist = store_dist_km.get(sid)
                rounded_total = round(float(total), 2)

                results.append({
                    "store": name,
                    "total": rounded_total,
                    "distance_km": dist
                })
                totals_by_name[name] = rounded_total

            results.sort(key=lambda x: x["total"])

            return {
                "results": results,
                "totals": totals_by_name
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

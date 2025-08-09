from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel, confloat, conint
from typing import List, Dict
from geopy.distance import geodesic

router = APIRouter()

# Quantity must be at least 1
class GroceryItem(BaseModel):
    product: str
    quantity: conint(ge=1) = 1

class GroceryList(BaseModel):
    items: List[GroceryItem]

# radius_km: default 2.0, min 0.1, max 15.0
class CompareRequest(BaseModel):
    grocery_list: GroceryList
    lat: float
    lon: float
    radius_km: confloat(ge=0.1, le=15.0) = 2.0

@router.post("/compare")
async def compare_basket(body: CompareRequest, request: Request):
    """
    Compare a user's grocery_list across nearby stores (within radius_km)
    and return totals per store + distance_km to each store.

    Response:
      {
        "results": [
          {"store": "...", "total": 12.34, "distance_km": 2.1},
          ...
        ],
        "totals": {"Store A": 12.34, "Store B": 13.21}
      }
    """
    try:
        grocery_list = body.grocery_list
        user_lat = body.lat
        user_lon = body.lon
        radius_km = float(body.radius_km)

        user_location = (user_lat, user_lon)

        async with request.app.state.db.acquire() as conn:
            # Fetch all stores with coordinates
            store_rows = await conn.fetch("SELECT id, name, lat, lon FROM stores")

            # Pre-compute distances and keep only stores within radius
            nearby_store_ids: List[int] = []
            store_dist_km: Dict[int, float] = {}
            store_name_by_id: Dict[int, str] = {}

            for row in store_rows:
                store_id = row["id"]
                store_name = row["name"]
                store_lat = row["lat"]
                store_lon = row["lon"]

                store_name_by_id[store_id] = store_name

                # Skip stores missing coordinates
                if store_lat is None or store_lon is None:
                    continue

                distance = geodesic((store_lat, store_lon), user_location).km
                if distance <= radius_km:
                    nearby_store_ids.append(store_id)
                    store_dist_km[store_id] = round(float(distance), 2)

            if not nearby_store_ids:
                raise HTTPException(status_code=404, detail="No stores found within given radius")

            # Accumulate totals per store (id)
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
                    item.product,
                    nearby_store_ids
                )

                for r in rows:
                    sid = r["store_id"]
                    unit_price = float(r["price"])
                    totals_by_id[sid] = totals_by_id.get(sid, 0.0) + unit_price * item.quantity

            if not totals_by_id:
                raise HTTPException(status_code=404, detail="No matching products found in nearby stores")

            # Build results list with distances
            results = []
            totals_by_name: Dict[str, float] = {}

            for sid, total in totals_by_id.items():
                name = store_name_by_id.get(sid, f"Store {sid}")
                dist = store_dist_km.get(sid)  # exists because filtered by radius
                rounded_total = round(float(total), 2)

                results.append({
                    "store": name,
                    "total": rounded_total,
                    "distance_km": dist
                })
                totals_by_name[name] = rounded_total

            # Sort by total ascending
            results.sort(key=lambda x: x["total"])

            return {
                "results": results,
                "totals": totals_by_name  # backwards-compat for older clients
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

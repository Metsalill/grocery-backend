from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Dict, Optional, Tuple
from geopy.distance import geodesic

# import throttle decorator from utils instead of main
from utils.throttle import throttle

router = APIRouter()

# ---- server-side caps ----
MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 15.0


# --------- Pydantic models (public API) ----------
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


# --------- Internal reusable function ----------
async def compute_compare(
    pool,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
) -> Dict:
    """
    Core comparison logic that can be reused by other routes (e.g., basket history).
    Returns a rich structure with store IDs, names, totals, distances, and per-item pricing.

    items: list of (product_name, quantity)
    """
    if not items:
        raise HTTPException(status_code=400, detail="Basket is empty")
    if len(items) > MAX_ITEMS:
        raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

    # Basic validation
    norm_items: List[Tuple[str, int]] = []
    for name, qty in items:
        if not name or not str(name).strip():
            raise HTTPException(status_code=400, detail="Product name cannot be empty")
        norm_items.append((str(name).strip(), int(qty)))

    user_location = (user_lat, user_lon)
    radius_km = float(max(MIN_RADIUS, min(MAX_RADIUS, float(radius_km))))

    async with pool.acquire() as conn:
        # 1) Load stores
        store_rows = await conn.fetch("SELECT id, name, lat, lon FROM stores")

        nearby_store_ids: List[int] = []
        store_dist_km: Dict[int, float] = {}
        store_name_by_id: Dict[int, str] = {}

        for row in store_rows:
            sid = row["id"]
            sname = row["name"]
            slat = row["lat"]
            slon = row["lon"]

            store_name_by_id[sid] = sname

            if slat is None or slon is None:
                continue
            dist = geodesic((slat, slon), user_location).km
            if dist <= radius_km:
                nearby_store_ids.append(sid)
                store_dist_km[sid] = round(float(dist), 2)

        if not nearby_store_ids:
            raise HTTPException(status_code=404, detail="No stores found within given radius")

        # 2) Build totals and per-item breakdown per store
        totals_by_id: Dict[int, float] = {}
        # per-store items: sid -> { product -> {price, quantity, line_total} }
        items_by_store: Dict[int, Dict[str, Dict[str, Optional[float]]]] = {sid: {} for sid in nearby_store_ids}

        for product_name, quantity in norm_items:
            rows = await conn.fetch(
                """
                SELECT s.id AS store_id, s.name AS store_name, p.price
                FROM prices p
                JOIN stores s ON p.store_id = s.id
                WHERE LOWER(p.product) = LOWER($1)
                  AND s.id = ANY($2::int[])
                """,
                product_name,
                nearby_store_ids,
            )

            # note: a product may be missing in some stores
            for r in rows:
                sid = r["store_id"]
                unit_price = float(r["price"])
                line_total = unit_price * quantity
                totals_by_id[sid] = totals_by_id.get(sid, 0.0) + line_total
                items_by_store[sid][product_name] = {
                    "price": unit_price,
                    "quantity": quantity,
                    "line_total": round(line_total, 2),
                }

        if not totals_by_id:
            raise HTTPException(status_code=404, detail="No matching products found in nearby stores")

        # 3) Assemble results
        stores_detailed = []
        totals_by_name: Dict[str, float] = {}
        for sid, total in totals_by_id.items():
            name = store_name_by_id.get(sid, f"Store {sid}")
            dist = store_dist_km.get(sid)
            rounded_total = round(float(total), 2)

            # convert item map to array with product name included
            items_array = [
                {
                    "product": prod,
                    "price": info.get("price"),
                    "quantity": info.get("quantity"),
                    "line_total": info.get("line_total"),
                }
                for prod, info in items_by_store.get(sid, {}).items()
            ]

            stores_detailed.append({
                "store_id": sid,
                "store_name": name,
                "total": rounded_total,
                "distance_km": dist,
                "items": items_array,
            })
            totals_by_name[name] = rounded_total

        # sort by cheapest first
        stores_detailed.sort(key=lambda x: x["total"])

        # Backwards-compatible fields + richer payload
        legacy_results = [
            {"store": s["store_name"], "total": s["total"], "distance_km": s["distance_km"]}
            for s in stores_detailed
        ]

        return {
            # New, rich structure used by basket history and future UI
            "stores": stores_detailed,
            # Back-compat fields used by existing clients
            "results": legacy_results,
            "totals": totals_by_name,
            "radius_km": radius_km,
        }


# --------- Public API endpoint (uses the reusable core) ----------
@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(body: CompareRequest, request: Request):
    try:
        pool = request.app.state.db
        items_tuples: List[Tuple[str, int]] = [(it.product, it.quantity) for it in body.grocery_list.items]
        payload = await compute_compare(
            pool=pool,
            items=items_tuples,
            user_lat=body.lat,
            user_lon=body.lon,
            radius_km=body.radius_km,
        )
        # keep returning the same top-level keys the client already expects
        return {
            "results": payload["results"],
            "totals": payload["totals"],
            # provide richer data too (non-breaking addition)
            "stores": payload["stores"],
            "radius_km": payload["radius_km"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

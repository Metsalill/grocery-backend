# compare.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Tuple

from utils.throttle import throttle
from services.compare_service import compare_basket_service

router = APIRouter(prefix="")  # route stays /compare

# ---- server-side caps ----
MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 15.0
MAX_STORES = 200


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
    # optional tuning knobs (forwarded)
    limit_stores: conint(ge=1, le=MAX_STORES) = 50
    offset_stores: conint(ge=0) = 0
    include_lines: bool = True
    require_all_items: bool = True


# --------- Backward-compat shim ----------
async def compute_compare(
    pool,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    """
    Compatibility wrapper for older code (e.g., basket_history.py).
    Delegates to the SQL-first compare service.
    """
    # Call POSITIONALLY to avoid keyword-name mismatches.
    return await compare_basket_service(
        pool,
        items,
        float(user_lat),
        float(user_lon),
        float(radius_km),
        50,          # limit_stores
        0,           # offset_stores
        True,        # include_lines
        True,        # require_all_items
    )


# --------- Public API endpoint ----------
@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(body: CompareRequest, request: Request):
    """
    Compare a basket across nearby stores.

    Request:
      - grocery_list.items: [{ product: string, quantity: int >= 1 }]
      - lat, lon: user location (required)
      - radius_km: search radius (km)
      - limit_stores, offset_stores: paging
      - include_lines: include per-product line details
      - require_all_items: hint for ranking behavior

    Response:
      - results: ranked per-store totals (and optional lines)
      - totals: summary across results
      - stores: store metadata (id, name, chain, distance_km)
      - radius_km: effective radius used
      - missing_products: unresolved product names
    """
    try:
        if not body.grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(body.grocery_list.items) > MAX_ITEMS:
            raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

        # Normalize items as (name, qty)
        items_tuples: List[Tuple[str, int]] = [
            (it.product.strip(), int(it.quantity))
            for it in body.grocery_list.items
            if isinstance(it.product, str) and it.product.strip()
        ]
        if not items_tuples:
            raise HTTPException(status_code=400, detail="All product names are empty")

        pool = request.app.state.db
        if pool is None:
            raise HTTPException(status_code=500, detail="DB not ready")

        # Call POSITIONALLY (no keywords) to match the service signature.
        payload = await compare_basket_service(
            pool,
            items_tuples,
            float(body.lat),
            float(body.lon),
            float(body.radius_km),
            int(body.limit_stores),
            int(body.offset_stores),
            bool(body.include_lines),
            bool(body.require_all_items),
        )

        return {
            "results": payload.get("results", []),
            "totals": payload.get("totals", {}),
            "stores": payload.get("stores", []),
            "radius_km": payload.get("radius_km"),
            "missing_products": payload.get("missing_products", []),
        }

    except HTTPException:
        raise
    except Exception as e:
        # concise to client; full trace is logged by middleware
        raise HTTPException(status_code=500, detail=str(e))

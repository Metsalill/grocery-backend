from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Tuple

# import throttle decorator from utils instead of main
from utils.throttle import throttle

# SQL-first service
from services.compare_service import compare_basket_service

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


# --------- Backward-compat shim (for basket_history.py etc.) ----------
async def compute_compare(
    pool,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    """
    Backward-compatibility wrapper.
    Delegates to compare_basket_service so older code (like basket_history.py) still works.
    """
    return await compare_basket_service(
        pool=pool,
        items=items,
        lat=float(user_lat),
        lon=float(user_lon),
        radius_km=float(radius_km),
        require_all_items=True,
    )


# --------- Public API endpoint ----------
@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(body: CompareRequest, request: Request):
    try:
        if not body.grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(body.grocery_list.items) > MAX_ITEMS:
            raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

        items_tuples: List[Tuple[str, int]] = [
            (it.product.strip(), int(it.quantity))
            for it in body.grocery_list.items
            if it.product and it.product.strip()
        ]
        if not items_tuples:
            raise HTTPException(status_code=400, detail="All product names are empty")

        pool = request.app.state.db

        payload = await compare_basket_service(
            pool=pool,
            items=items_tuples,
            lat=body.lat,
            lon=body.lon,
            radius_km=float(body.radius_km),
            require_all_items=True,
        )

        return {
            "results": payload.get("results", []),
            "totals": payload.get("totals", {}),
            "stores": payload.get("stores", []),  # richer data
            "radius_km": payload.get("radius_km"),
            "missing_products": payload.get("missing_products", []),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

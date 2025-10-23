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
MAX_RADIUS = 50.0          # <— bumped from 15.0 to 50.0
MAX_STORES = 50


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
    # optional tuning knobs passed through to the service
    limit_stores: conint(ge=1, le=MAX_STORES) = 50
    offset_stores: conint(ge=0) = 0
    include_lines: bool = True
    require_all_items: bool = True


# --------- helpers ----------
def _clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return lo if v < lo else hi if v > hi else v


def _normalize_items(gl: GroceryList) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for it in gl.items:
        name = (it.product or "").strip()
        if name:
            out.append((name, int(it.quantity)))
    return out


# --------- Backward-compat shim (for basket_history.py etc.) ----------
async def compute_compare(
    pool,  # this one IS used by the current service
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    """
    Backward-compatible wrapper that delegates to compare_basket_service
    with the signature compare_basket_service(pool, payload_dict).
    """
    payload = {
        "items": items,
        "lat": float(user_lat),
        "lon": float(user_lon),
        "radius_km": float(_clamp(float(radius_km), MIN_RADIUS, MAX_RADIUS)),
        "limit_stores": 50,
        "offset_stores": 0,
        "include_lines": True,
        "require_all_items": True,
    }
    return await compare_basket_service(pool, payload)


# --------- Public API endpoint ----------
@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(body: CompareRequest, request: Request):
    """
    Compare a basket across nearby stores.
    """
    try:
        if not body.grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(body.grocery_list.items) > MAX_ITEMS:
            raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

        items_tuples = _normalize_items(body.grocery_list)
        if not items_tuples:
            raise HTTPException(status_code=400, detail="All product names are empty")

        pool = getattr(request.app.state, "db", None)
        if pool is None:
            raise HTTPException(status_code=500, detail="DB not ready")

        radius_km = float(_clamp(float(body.radius_km), MIN_RADIUS, MAX_RADIUS))
        limit_stores = int(_clamp_int(int(body.limit_stores), 1, MAX_STORES))
        offset_stores = max(0, int(body.offset_stores))

        payload_in = {
            "items": items_tuples,
            "lat": float(body.lat),
            "lon": float(body.lon),
            "radius_km": radius_km,
            "limit_stores": limit_stores,
            "offset_stores": offset_stores,
            "include_lines": bool(body.include_lines),
            "require_all_items": bool(body.require_all_items),
        }

        # Service signature: compare_basket_service(pool, payload_dict)
        payload_out = await compare_basket_service(pool, payload_in)

        return {
            "results": payload_out.get("results", []),
            "totals": payload_out.get("totals", {}),
            "stores": payload_out.get("stores", []),
            "radius_km": payload_out.get("radius_km", radius_km),
            "missing_products": payload_out.get("missing_products", []),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

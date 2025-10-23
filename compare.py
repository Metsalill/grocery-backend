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
    pool,  # kept for call-site compatibility; not used by current service
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    """
    Backward-compatible wrapper that delegates to compare_basket_service.
    IMPORTANT: call the service *positionally* (no keywords) and do NOT pass pool.
    """
    r = float(_clamp(float(radius_km), MIN_RADIUS, MAX_RADIUS))
    return await compare_basket_service(
        items,
        float(user_lat),
        float(user_lon),
        r,
        50,      # limit_stores
        0,       # offset_stores
        True,    # include_lines
        True,    # require_all_items
    )


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

        # Ensure DB pool exists (service may use app state internally)
        if getattr(request.app.state, "db", None) is None:
            raise HTTPException(status_code=500, detail="DB not ready")

        r = float(_clamp(float(body.radius_km), MIN_RADIUS, MAX_RADIUS))
        limit_stores = int(_clamp_int(int(body.limit_stores), 1, MAX_STORES))
        offset_stores = max(0, int(body.offset_stores))

        # IMPORTANT: positional call — avoids 'unexpected keyword argument' errors
        payload = await compare_basket_service(
            items_tuples,
            float(body.lat),
            float(body.lon),
            r,
            limit_stores,
            offset_stores,
            bool(body.include_lines),
            bool(body.require_all_items),
        )

        return {
            "results": payload.get("results", []),
            "totals": payload.get("totals", {}),
            "stores": payload.get("stores", []),
            "radius_km": payload.get("radius_km", r),
            "missing_products": payload.get("missing_products", []),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

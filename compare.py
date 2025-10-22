# compare.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Tuple, Optional, Dict, Any

from utils.throttle import throttle
from services.compare_service import compare_basket_service

# single router object; we’ll register two paths on it (/compare and /api/compare)
router = APIRouter()

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
    # optional: app may omit and let backend do non-geo candidate list
    lat: Optional[float] = None
    lon: Optional[float] = None
    radius_km: confloat(ge=MIN_RADIUS, le=MAX_RADIUS) = 2.0
    limit_stores: conint(ge=1, le=MAX_STORES) = 50
    offset_stores: conint(ge=0) = 0
    include_lines: bool = True
    require_all_items: bool = True


# --------- Backward-compat shim (for basket_history.py etc.) ----------
async def compute_compare(
    pool,  # asyncpg.Pool
    items: List[Tuple[str, int]],
    user_lat: Optional[float],
    user_lon: Optional[float],
    radius_km: float,
):
    """
    Delegates to compare_basket_service so older code still works.
    """
    body: Dict[str, Any] = {
        "grocery_list": {"items": [{"product": n, "quantity": q} for n, q in items]},
        "lat": user_lat,
        "lon": user_lon,
        "radius_km": float(max(MIN_RADIUS, min(MAX_RADIUS, radius_km))),
        "limit_stores": 50,
        "offset_stores": 0,
        "include_lines": True,
        "require_all_items": True,
    }
    return await compare_basket_service(pool, body)


# --------- Public API endpoint (registered on /compare and /api/compare) ----------
@throttle(limit=30, window=60)
async def _compare_handler(request: Request, body: CompareRequest):
    """
    Compare a basket across nearby stores.
    """
    # basic basket validation
    if not body.grocery_list.items:
        raise HTTPException(status_code=400, detail="Basket is empty")
    if len(body.grocery_list.items) > MAX_ITEMS:
        raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

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

    # clamp server-controlled knobs
    radius = float(max(MIN_RADIUS, min(MAX_RADIUS, float(body.radius_km))))
    limit_stores = int(max(1, min(MAX_STORES, int(body.limit_stores))))
    offset_stores = int(max(0, int(body.offset_stores)))

    svc_body: Dict[str, Any] = {
        "grocery_list": {"items": [{"product": n, "quantity": q} for n, q in items_tuples]},
        "lat": body.lat,   # may be None -> service handles
        "lon": body.lon,   # may be None -> service handles
        "radius_km": radius,
        "limit_stores": limit_stores,
        "offset_stores": offset_stores,
        "include_lines": bool(body.include_lines),
        "require_all_items": bool(body.require_all_items),
    }

    try:
        payload = await compare_basket_service(pool, svc_body)
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
        # concise error; detailed stack is logged by middleware
        raise HTTPException(status_code=500, detail=str(e))


# Register the same handler on BOTH paths so app calls to /api/compare also work.
router.add_api_route("/compare", _compare_handler, methods=["POST"])
router.add_api_route("/api/compare", _compare_handler, methods=["POST"])

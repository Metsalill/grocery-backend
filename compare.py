# compare.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Tuple, Dict, Any

from utils.throttle import throttle
from services.compare_service import compare_basket_service

router = APIRouter(prefix="")  # route stays /compare

# ---- server-side caps ----
MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 50.0          # bumped from 15 → 50 so the app can slide up to 50km
MAX_STORES = 50


# --------- Pydantic models (public API) ----------
class GroceryItem(BaseModel):
    # Human-readable fallback / debug string from the app
    product: str

    # How many the user wants
    quantity: conint(ge=1) = 1

    # NEW: canonical product id from our products table (nullable for legacy clients)
    product_id: int | None = None


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


def _normalize_items(gl: GroceryList) -> List[Dict[str, Any]]:
    """
    Convert the incoming GroceryList into a list of dicts the service layer
    can understand.

    Each dict looks like:
    {
        "product": "Piim ALMA 2,5%, 0,5L",
        "quantity": 1,
        "product_id": 421264   # may be None if we don't have it
    }

    We keep `product` + `quantity` for backward-compat and debugging, but
    `product_id` lets the service skip fuzzy name matching and directly price
    by canonical ID.
    """
    out: List[Dict[str, Any]] = []
    for it in gl.items:
        name = (it.product or "").strip()
        if not name:
            continue
        out.append(
            {
                "product": name,
                "quantity": int(it.quantity),
                "product_id": it.product_id,  # may be None
            }
        )
    return out


# --------- Backward-compat shim (for basket_history.py etc.) ----------
async def compute_compare(
    pool,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    """
    Backward-compatible wrapper that delegates to compare_basket_service
    with the signature compare_basket_service(pool, payload_dict).

    NOTE: this older path still passes `items` as List[Tuple[str,int]].
    The service must continue to accept that "old" shape too.
    """
    payload = {
        "items": items,  # old shape: [("Piim ...", 1), ...]
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

    Flutter now sends each item like:
      {
        "product": "Piim ALMA 2,5%, 0,5L",
        "quantity": 1,
        "product_id": 421264    # canonical ID from /api/products
      }

    We forward that straight to the service layer, instead of discarding product_id.
    """
    try:
        # basic sanity
        if not body.grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(body.grocery_list.items) > MAX_ITEMS:
            raise HTTPException(
                status_code=400,
                detail=f"Basket too large (>{MAX_ITEMS} items)",
            )

        # normalize into dicts with product / quantity / product_id
        items_payload = _normalize_items(body.grocery_list)
        if not items_payload:
            raise HTTPException(
                status_code=400,
                detail="All product names are empty",
            )

        # grab db pool off app state
        pool = getattr(request.app.state, "db", None)
        if pool is None:
            raise HTTPException(
                status_code=500,
                detail="DB not ready",
            )

        # clamp/sanitize tunables
        radius_km = float(_clamp(float(body.radius_km), MIN_RADIUS, MAX_RADIUS))
        limit_stores = int(_clamp_int(int(body.limit_stores), 1, MAX_STORES))
        offset_stores = max(0, int(body.offset_stores))

        # build payload for service layer
        payload_in = {
            "items": items_payload,  # NEW: list[dict] w/ product_id
            "lat": float(body.lat),
            "lon": float(body.lon),
            "radius_km": radius_km,
            "limit_stores": limit_stores,
            "offset_stores": offset_stores,
            "include_lines": bool(body.include_lines),
            "require_all_items": bool(body.require_all_items),
        }

        # hand off to the DB/service logic
        payload_out = await compare_basket_service(pool, payload_in)

        # make sure we always return the same envelope the app expects
        return {
            "results": payload_out.get("results", []),
            "totals": payload_out.get("totals", {}),
            "stores": payload_out.get("stores", []),
            "radius_km": payload_out.get("radius_km", radius_km),
            "missing_products": payload_out.get("missing_products", []),
        }

    except HTTPException:
        # pass through normal HTTP errors
        raise
    except Exception as e:
        # anything else -> 500
        raise HTTPException(status_code=500, detail=str(e))

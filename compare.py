# compare.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Body, HTTPException, Request

from utils.throttle import throttle
from services.compare_service import compare_basket_service

# Expose /api/compare (mobile calls this path)
router = APIRouter(prefix="/api", tags=["compare"])

# ---- server-side caps / sane defaults ----
MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 15.0
MAX_STORES = 200


# --------- Backward-compat shim (for basket_history.py etc.) ----------
async def compute_compare(
    pool: Any,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    """
    Legacy helper that builds a request body and forwards to the new service.
    """
    body: Dict[str, Any] = {
        "grocery_list": {
            "items": [
                {"product": p.strip(), "quantity": int(q)}
                for (p, q) in items
                if isinstance(p, str) and p.strip()
            ]
        },
        "lat": float(user_lat),
        "lon": float(user_lon),
        "radius_km": float(max(MIN_RADIUS, min(MAX_RADIUS, radius_km))),
        "limit_stores": 50,
        "offset_stores": 0,
        "include_lines": True,
        "require_all_items": True,
    }
    return await compare_basket_service(pool, body)


# --------- Public API endpoint (/api/compare) ----------
@router.post("/compare")
@throttle(limit=60, window=60)
async def compare_basket(
    request: Request,
    body: Dict[str, Any] = Body(..., description="Basket compare payload"),
):
    """
    Proxy endpoint that validates input lightly and forwards to compare_basket_service.

    Expected body (keys optional except grocery_list.items):
    {
      "grocery_list": { "items": [ { "product": "Nutella 400g", "quantity": 1 }, ... ] },
      "lat": 59.43,                // optional; service can work without
      "lon": 24.75,                // optional; service can work without
      "radius_km": 2.0,            // [0.1 .. 15.0]
      "limit_stores": 50,          // <= 200
      "offset_stores": 0,
      "include_lines": false,
      "require_all_items": false
    }
    """
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="DB not ready")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Basic basket validation & caps
    gl = (body.get("grocery_list") or {}).get("items") or []
    if not isinstance(gl, list) or not gl:
        raise HTTPException(status_code=400, detail="Basket is empty")

    if len(gl) > MAX_ITEMS:
        raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

    # Clamp optional knobs if present
    if "radius_km" in body:
        try:
            r = float(body["radius_km"])
            body["radius_km"] = max(MIN_RADIUS, min(MAX_RADIUS, r))
        except Exception:
            body["radius_km"] = 2.0

    if "limit_stores" in body:
        try:
            body["limit_stores"] = max(1, min(MAX_STORES, int(body["limit_stores"])))
        except Exception:
            body["limit_stores"] = 50

    if "offset_stores" in body:
        try:
            body["offset_stores"] = max(0, int(body["offset_stores"]))
        except Exception:
            body["offset_stores"] = 0

    try:
        # IMPORTANT: pass the DB pool/conn as the first arg; body as-is
        result = await compare_basket_service(db, body)
        return result
    except HTTPException:
        raise
    except Exception as e:
        # Tracebacks are logged by TraceLogMiddleware; surface concise message to client
        raise HTTPException(status_code=500, detail=f"Compare failed: {e}")

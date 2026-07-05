# compare.py
import json
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Tuple, Dict, Any, Optional
from utils.throttle import throttle
from services.compare_service import compare_basket_service

router = APIRouter(prefix="")

MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 50.0
MAX_STORES = 50


class GroceryItem(BaseModel):
    product: str
    quantity: confloat(ge=0.1) = 1.0
    product_id: int | None = None
    ingredient_name_en: str | None = None


class GroceryList(BaseModel):
    items: List[GroceryItem]


class CompareRequest(BaseModel):
    grocery_list: GroceryList
    lat: Optional[float] = None  # None = ei filtreeri kauguse järgi
    lon: Optional[float] = None
    radius_km: confloat(ge=MIN_RADIUS, le=MAX_RADIUS) = 2.0
    limit_stores: conint(ge=1, le=MAX_STORES) = 50
    offset_stores: conint(ge=0) = 0
    include_lines: bool = True
    require_all_items: bool = True


def _clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return lo if v < lo else hi if v > hi else v


def _normalize_items(gl: GroceryList) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in gl.items:
        name = (it.product or "").strip()
        if not name:
            continue
        item: Dict[str, Any] = {
            "product": name,
            "quantity": float(it.quantity),
            "product_id": it.product_id,
        }
        if it.ingredient_name_en:
            item["ingredient_name_en"] = it.ingredient_name_en
        out.append(item)
    return out


def _build_chain_totals(results: List[Dict[str, Any]]) -> Tuple[Dict[str, float], Dict[str, int]]:
    """Reduces the per-store comparison results to one entry per chain:
    the cheapest complete-basket total found in that chain, and which
    store achieved it. Stores with no total_price (incomplete basket —
    e.g. require_all_items filtered them out, or nothing was found)
    are skipped, since a missing total can't be compared against a
    winner in euros.
    """
    chain_totals: Dict[str, float] = {}
    chain_store_ids: Dict[str, int] = {}
    for r in results:
        total = r.get("total_price")
        if total is None:
            continue
        chain = (r.get("chain") or "").lower().strip()
        if not chain:
            continue
        if chain not in chain_totals or total < chain_totals[chain]:
            chain_totals[chain] = total
            chain_store_ids[chain] = r.get("store_id")
    return chain_totals, chain_store_ids


async def _log_basket_compare(
    request: Request,
    payload_out: Dict[str, Any],
    basket_size: int,
    radius_km: float,
) -> None:
    """Logs a basket_compare analytics event carrying the per-chain
    totals for this comparison, so the partner dashboard can later
    compute "lost to the winner by less than X€" insights (V2.5).

    This is deliberately chain-level aggregate data, not tied to a
    person or device — unlike product_view/basket_add/basket_win it
    does not need user_id/device_key, so it doesn't touch that hashing
    logic at all. Only logged when at least two chains produced a
    complete total, since a single-chain result has nothing to be
    compared against. Never raises: an analytics failure must not
    break the actual /compare response.
    """
    try:
        results = payload_out.get("results", [])
        chain_totals, chain_store_ids = _build_chain_totals(results)
        if len(chain_totals) < 2:
            return

        totals = payload_out.get("totals", {}) or {}
        cheapest_chain = totals.get("cheapest_chain")
        cheapest_total = totals.get("cheapest_total")
        stores_compared = sum(1 for r in results if r.get("total_price") is not None)
        required_lines = next(
            (r.get("required_lines") for r in results if r.get("required_lines") is not None),
            basket_size,
        )

        pool = getattr(request.app.state, "db", None)
        if pool is None:
            return

        event_payload = {
            "radius_km": radius_km,
            "required_lines": required_lines,
            "stores_compared": stores_compared,
            "cheapest_chain": cheapest_chain,
            "cheapest_total": cheapest_total,
            "chain_totals": chain_totals,
            "chain_store_ids": chain_store_ids,
            "basket_size": basket_size,
        }

        await pool.execute(
            """
            INSERT INTO analytics_events (event_type, chain, payload)
            VALUES ($1, $2, $3::jsonb)
            """,
            "basket_compare",
            cheapest_chain,
            json.dumps(event_payload),
        )
    except Exception:
        # Analytics is best-effort — never let a logging problem affect
        # the person's actual price comparison.
        pass


async def compute_compare(
    pool,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    # compare_basket_service expects a list of dicts (product/quantity/
    # product_id/ingredient_name_en) — it silently skips anything that
    # isn't a dict. This legacy helper used to pass raw (name, qty)
    # tuples straight through, which meant every row got dropped and
    # the comparison always came back empty. Build the dicts here so
    # this still works if anything still calls compute_compare().
    payload_items = [
        {
            "product": str(name).strip(),
            "quantity": float(quantity),
            "product_id": None,
        }
        for name, quantity in items
        if name and str(name).strip()
    ]

    payload = {
        "items": payload_items,
        "lat": float(user_lat),
        "lon": float(user_lon),
        "radius_km": float(_clamp(float(radius_km), MIN_RADIUS, MAX_RADIUS)),
        "limit_stores": 50,
        "offset_stores": 0,
        "include_lines": True,
        "require_all_items": True,
    }
    return await compare_basket_service(pool, payload)


@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(body: CompareRequest, request: Request):
    try:
        if not body.grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(body.grocery_list.items) > MAX_ITEMS:
            raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

        items_payload = _normalize_items(body.grocery_list)
        if not items_payload:
            raise HTTPException(status_code=400, detail="All product names are empty")

        pool = getattr(request.app.state, "db", None)
        if pool is None:
            raise HTTPException(status_code=500, detail="DB not ready")

        radius_km = float(_clamp(float(body.radius_km), MIN_RADIUS, MAX_RADIUS))
        limit_stores = int(_clamp_int(int(body.limit_stores), 1, MAX_STORES))
        offset_stores = max(0, int(body.offset_stores))

        payload_in = {
            "items": items_payload,
            "lat": body.lat,  # None lubatud
            "lon": body.lon,  # None lubatud
            "radius_km": radius_km,
            "limit_stores": limit_stores,
            "offset_stores": offset_stores,
            "include_lines": bool(body.include_lines),
            "require_all_items": bool(body.require_all_items),
        }

        payload_out = await compare_basket_service(pool, payload_in)

        await _log_basket_compare(
            request,
            payload_out,
            basket_size=len(items_payload),
            radius_km=payload_out.get("radius_km", radius_km),
        )

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

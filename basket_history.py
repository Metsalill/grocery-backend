# basket_history.py
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import asyncpg
import json
import traceback
import uuid

from auth import get_current_user
from settings import get_db_pool
from services.compare_service import compare_basket_service

router = APIRouter(prefix="/basket-history", tags=["basket-history"])


# ---------- helpers ----------

def _extract_user_id(obj):
    if not obj:
        return None
    if isinstance(obj, dict):
        for k in ("id", "user_id", "uid", "sub", "userId", "uuid"):
            v = obj.get(k)
            if v:
                return v
        for k in ("user", "account", "data", "profile"):
            inner = obj.get(k)
            if inner:
                got = _extract_user_id(inner)
                if got:
                    return got
        return None
    for k in ("id", "user_id", "uid", "sub", "userId", "uuid"):
        v = getattr(obj, k, None)
        if v:
            return v
    for k in ("user", "account", "data", "profile"):
        inner = getattr(obj, k, None)
        if inner:
            got = _extract_user_id(inner)
            if got:
                return got
    return None


def _coerce_to_uuid_str(value: object) -> str:
    s = str(value)
    try:
        return str(uuid.UUID(s))
    except Exception:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"grocery-user:{s}"))


async def resolve_user_id(user, pool: asyncpg.pool.Pool) -> Optional[str]:
    direct = _extract_user_id(user)
    if direct:
        return _coerce_to_uuid_str(direct)
    email = user.get("email") if isinstance(user, dict) else getattr(user, "email", None)
    if email:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT id FROM users WHERE email=$1 LIMIT 1", email)
            if row:
                return _coerce_to_uuid_str(row["id"])
        except Exception as e:
            print("RESOLVE_UID_ERROR:", type(e).__name__, str(e))
    return None


def _call_compare(pool, items_dicts, lat, lon, radius_km, require_all=False):
    """
    Build the body dict that compare_basket_service(db, body) expects
    and return the coroutine.
    """
    body = {
        "items": items_dicts,
        "lat": float(lat),
        "lon": float(lon),
        "radius_km": float(radius_km),
        "limit_stores": 50,
        "offset_stores": 0,
        "include_lines": False,
        "require_all_items": require_all,
    }
    return compare_basket_service(pool, body)


def _winner_total(store_dict: dict) -> float:
    """
    Extract total from a store result — handles both
    new shape (total_price) and legacy shape (total).
    """
    v = store_dict.get("total_price") or store_dict.get("total")
    return float(v) if v is not None else float("inf")


# ---------- Schemas ----------

class BasketItemIn(BaseModel):
    product: str = Field(..., description="Product name")
    quantity: float = 1
    unit: Optional[str] = None
    brand: Optional[str] = None
    size_text: Optional[str] = None
    image_url: Optional[str] = None


class SaveBasketIn(BaseModel):
    items: List[BasketItemIn]
    lat: float
    lon: float
    radius_km: float = 10.0
    selected_store_id: Optional[int] = None
    note: Optional[str] = None


class BasketSummaryOut(BaseModel):
    id: int
    created_at: datetime
    winner_store_name: Optional[str]
    winner_total: Optional[float]
    radius_km: Optional[float]


class BasketDetailOut(BaseModel):
    id: int
    created_at: datetime
    radius_km: Optional[float]
    winner_store_id: Optional[int]
    winner_store_name: Optional[str]
    winner_total: Optional[float]
    stores: Optional[List[dict]]
    note: Optional[str]
    items: List[dict]


# ---------- Routes ----------

@router.post("", response_model=BasketSummaryOut)
async def save_basket(
    payload: SaveBasketIn,
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = await resolve_user_id(user, pool)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 1) Build items list for compare — include product_id if we had it
    #    (BasketItemIn doesn't carry product_id so we use name-only path)
    items_dicts = [
        {"product": it.product, "quantity": int(it.quantity), "product_id": None}
        for it in payload.items
        if (it.product or "").strip()
    ]
    if not items_dicts:
        raise HTTPException(status_code=400, detail="Basket is empty")

    # 2) Run compare — require_all=False so partial matches still save
    try:
        cmp = await _call_compare(
            pool, items_dicts,
            payload.lat, payload.lon, payload.radius_km,
            require_all=False,
        )
    except Exception as e:
        print("COMPARE_ERROR_IN_SAVE:", type(e).__name__, str(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Compare failed: {e}")

    # 3) Get store results — new service returns results[], not stores[]
    #    results has store_id, store_name, total_price, distance_km
    results = cmp.get("results") or []
    if not results:
        raise HTTPException(status_code=400, detail="No stores found within given radius")

    # 4) Pick winner
    results_sorted = sorted(results, key=_winner_total)
    winner = None
    if payload.selected_store_id is not None:
        winner = next(
            (r for r in results_sorted if r.get("store_id") == payload.selected_store_id),
            None,
        )
    if winner is None:
        winner = results_sorted[0]

    winner_store_id = winner.get("store_id")
    winner_store_name = (winner.get("store_name") or winner.get("chain") or "Unknown store").strip()
    raw_total = _winner_total(winner)
    winner_total = max(0.0, min(round(raw_total, 2), 9999.99)) if raw_total != float("inf") else 0.0

    # 5) Build stores snapshot for DB (use results list)
    stores_snapshot = [
        {
            "store_id": r.get("store_id"),
            "store_name": r.get("store_name"),
            "chain": r.get("chain"),
            "total": _winner_total(r) if _winner_total(r) != float("inf") else None,
            "distance_km": r.get("distance_km"),
            "lines_found": r.get("lines_found"),
            "required_lines": r.get("required_lines"),
        }
        for r in results
    ]
    stores_json = json.dumps(stores_snapshot, ensure_ascii=False)

    # 6) Persist
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                head = await conn.fetchrow(
                    """
                    INSERT INTO basket_history (
                        user_id, radius_km, origin_lat, origin_lon,
                        winner_store_id, winner_store_name,
                        winner_total, stores, note
                    ) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
                    RETURNING id, created_at, winner_store_name, winner_total, radius_km
                    """,
                    uid,
                    payload.radius_km,
                    payload.lat,
                    payload.lon,
                    winner_store_id,
                    winner_store_name,
                    winner_total,
                    stores_json,
                    payload.note,
                )
                basket_id = head["id"]

                rows = []
                for it in payload.items:
                    rows.append((
                        basket_id, it.product, float(it.quantity), it.unit,
                        None, None,  # price/line_total not available without include_lines
                        winner_store_id, winner_store_name,
                        it.image_url, it.brand, it.size_text,
                    ))

                if rows:
                    await conn.executemany(
                        """
                        INSERT INTO basket_items (
                            basket_id, product, quantity, unit, price, line_total,
                            store_id, store_name, image_url, brand, size_text
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                        """,
                        rows,
                    )
    except Exception as e:
        print("SAVE_BASKET_DB_ERROR:", type(e).__name__, str(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to save basket: {e}")

    return BasketSummaryOut(
        id=head["id"],
        created_at=head["created_at"],
        winner_store_name=head["winner_store_name"],
        winner_total=float(head["winner_total"]) if head["winner_total"] is not None else None,
        radius_km=float(head["radius_km"]) if head["radius_km"] is not None else None,
    )


# ---------- GET saved basket ----------

@router.get("/{basket_id}")
async def get_basket(
    basket_id: int,
    include_current: bool = Query(False),
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    radius_km: Optional[float] = Query(None),
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = await resolve_user_id(user, pool)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with pool.acquire() as conn:
            head = await conn.fetchrow(
                """
                SELECT id, created_at,
                  radius_km::float8 AS radius_km,
                  origin_lat::float8 AS origin_lat,
                  origin_lon::float8 AS origin_lon,
                  winner_store_id, winner_store_name,
                  winner_total::float8 AS winner_total,
                  stores, note
                FROM basket_history
                WHERE id=$1 AND user_id=$2::uuid AND deleted_at IS NULL
                """,
                basket_id, uid,
            )
            if not head:
                raise HTTPException(status_code=404, detail="Basket not found")

            raw_stores = head["stores"]
            stores_payload: List[dict] = []
            if isinstance(raw_stores, list):
                stores_payload = raw_stores
            elif isinstance(raw_stores, dict):
                stores_payload = [raw_stores]
            elif isinstance(raw_stores, str):
                try:
                    parsed = json.loads(raw_stores)
                    stores_payload = parsed if isinstance(parsed, list) else [parsed]
                except Exception:
                    pass

            items = await conn.fetch(
                """
                SELECT product, quantity::float8 AS quantity, unit,
                  price::float8 AS price, line_total::float8 AS line_total,
                  store_id, store_name, image_url, brand, size_text
                FROM basket_items
                WHERE basket_id=$1 ORDER BY id
                """,
                basket_id,
            )

        snapshot = {
            "id": head["id"],
            "created_at": head["created_at"],
            "radius_km": head["radius_km"],
            "origin_lat": head["origin_lat"],
            "origin_lon": head["origin_lon"],
            "winner_store_id": head["winner_store_id"],
            "winner_store_name": head["winner_store_name"],
            "winner_total": head["winner_total"],
            "stores": stores_payload,
            "note": head["note"],
            "items": [dict(r) for r in items],
        }

        if not include_current:
            return snapshot

        use_lat = lat if lat is not None else head["origin_lat"]
        use_lon = lon if lon is not None else head["origin_lon"]
        use_radius = radius_km if radius_km is not None else head["radius_km"]

        if use_lat is None or use_lon is None:
            current = {"error": "origin coordinates unavailable"}
        else:
            item_dicts = [
                {"product": r["product"], "quantity": int(r["quantity"]), "product_id": None}
                for r in items
            ]
            current = await _call_compare(pool, item_dicts, use_lat, use_lon, use_radius or 10.0)

        return {"snapshot": snapshot, "current": current}

    except HTTPException:
        raise
    except Exception as e:
        print("GET_BASKET_ERROR:", type(e).__name__, str(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")


# ---------- Recompare and save as new ----------

class RecompareIn(BaseModel):
    lat: Optional[float] = None
    lon: Optional[float] = None
    radius_km: Optional[float] = None
    selected_store_id: Optional[int] = None
    note: Optional[str] = None


@router.post("/{basket_id}/recompare", response_model=BasketSummaryOut)
async def recompare_and_save_new(
    basket_id: int,
    payload: RecompareIn = Body(default=RecompareIn()),
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = await resolve_user_id(user, pool)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with pool.acquire() as conn:
        head = await conn.fetchrow(
            """
            SELECT id, radius_km::float8 AS radius_km,
              origin_lat::float8 AS origin_lat, origin_lon::float8 AS origin_lon
            FROM basket_history
            WHERE id=$1 AND user_id=$2::uuid AND deleted_at IS NULL
            """,
            basket_id, uid,
        )
        if not head:
            raise HTTPException(status_code=404, detail="Basket not found")

        items = await conn.fetch(
            "SELECT product, quantity::float8 AS quantity FROM basket_items WHERE basket_id=$1 ORDER BY id",
            basket_id,
        )

    if not items:
        raise HTTPException(status_code=400, detail="Basket has no items")

    use_lat = payload.lat if payload.lat is not None else head["origin_lat"]
    use_lon = payload.lon if payload.lon is not None else head["origin_lon"]
    use_radius = payload.radius_km if payload.radius_km is not None else head["radius_km"] or 10.0

    if use_lat is None or use_lon is None:
        raise HTTPException(status_code=400, detail="No coordinates available")

    item_dicts = [
        {"product": r["product"], "quantity": int(r["quantity"]), "product_id": None}
        for r in items
    ]
    cmp = await _call_compare(pool, item_dicts, use_lat, use_lon, use_radius, require_all=False)

    results = cmp.get("results") or []
    if not results:
        raise HTTPException(status_code=400, detail="No stores found")

    results_sorted = sorted(results, key=_winner_total)
    winner = None
    if payload.selected_store_id is not None:
        winner = next((r for r in results_sorted if r.get("store_id") == payload.selected_store_id), None)
    if winner is None:
        winner = results_sorted[0]

    winner_store_id = winner.get("store_id")
    winner_store_name = (winner.get("store_name") or winner.get("chain") or "Unknown store").strip()
    raw_total = _winner_total(winner)
    winner_total = max(0.0, min(round(raw_total, 2), 999999.99)) if raw_total != float("inf") else 0.0

    stores_snapshot = [
        {
            "store_id": r.get("store_id"),
            "store_name": r.get("store_name"),
            "chain": r.get("chain"),
            "total": _winner_total(r) if _winner_total(r) != float("inf") else None,
            "distance_km": r.get("distance_km"),
        }
        for r in results
    ]
    stores_json = json.dumps(stores_snapshot, ensure_ascii=False)

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                head_new = await conn.fetchrow(
                    """
                    INSERT INTO basket_history (
                        user_id, radius_km, origin_lat, origin_lon,
                        winner_store_id, winner_store_name,
                        winner_total, stores, note
                    ) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
                    RETURNING id, created_at, winner_store_name, winner_total, radius_km
                    """,
                    uid, float(use_radius), float(use_lat), float(use_lon),
                    winner_store_id, winner_store_name, winner_total,
                    stores_json, payload.note or f"Recomputed from basket #{basket_id}",
                )
                new_id = head_new["id"]

                rows = [
                    (new_id, r["product"], float(r["quantity"]), None,
                     None, None, winner_store_id, winner_store_name, None, None, None)
                    for r in items
                ]
                if rows:
                    await conn.executemany(
                        """
                        INSERT INTO basket_items (
                            basket_id, product, quantity, unit, price, line_total,
                            store_id, store_name, image_url, brand, size_text
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                        """,
                        rows,
                    )
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to save recomputed basket")

    return BasketSummaryOut(
        id=head_new["id"],
        created_at=head_new["created_at"],
        winner_store_name=head_new["winner_store_name"],
        winner_total=float(head_new["winner_total"]) if head_new["winner_total"] is not None else None,
        radius_km=float(head_new["radius_km"]) if head_new["radius_km"] is not None else None,
    )

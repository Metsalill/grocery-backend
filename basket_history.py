# basket_history.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import asyncio
import asyncpg
import json
import traceback
import uuid  # <-- keep

from auth import get_current_user
from settings import get_db_pool
from services.compare_service import compare_basket_service  # updated import

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
            traceback.print_exc()

    keys = list(user.keys()) if isinstance(user, dict) else [k for k in dir(user) if not k.startswith("_")]
    print("AUTH_USER_SHAPE_DEBUG(resolve_user_id):", type(user).__name__, "keys:", keys)
    return None

# ---------- Schemas ----------
class BasketItemIn(BaseModel):
    product: str = Field(..., description="Product name (same as used in /compare)")
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
    stores: Optional[List[dict]]  # normalized list
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

    # 1) Compute pricing snapshot (using new service)
    items_tuples = [(it.product, int(it.quantity)) for it in payload.items]
    cmp = await compare_basket_service(
        pool=pool,
        items=items_tuples,
        lat=payload.lat,
        lon=payload.lon,
        radius_km=payload.radius_km,
        require_all_items=True,
    )

    # 2) Normalize compare payload to a 'stores' list
    stores = cmp.get("stores")
    if not stores:
        legacy_results = cmp.get("results") or []
        stores = [
            {
                "store_id": None,
                "store_name": r.get("store"),
                "total": float(r.get("total", 0) or 0),
                "distance_km": r.get("distance_km"),
                "items": [],
            }
            for r in legacy_results
            if r.get("store") is not None
        ]
    if not stores:
        raise HTTPException(status_code=400, detail="No stores found within given radius")

    # 3) Decide winner
    stores_sorted = sorted(stores, key=lambda s: s["total"])
    winner = next((s for s in stores_sorted if s.get("store_id") == payload.selected_store_id), None) \
             if payload.selected_store_id is not None else None
    if winner is None:
        winner = stores_sorted[0]
    winner_store_id = winner.get("store_id")
    winner_store_name = (winner.get("store_name") or "Unknown store").strip()
    winner_total = max(0.0, min(round(float(winner.get("total") or 0.0), 2), 9999.99))

    stores_json = json.dumps(stores, ensure_ascii=False)

    # 4) Persist
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                head = await conn.fetchrow(
                    """
                    INSERT INTO basket_history (
                        user_id, radius_km, winner_store_id, winner_store_name,
                        winner_total, stores, note
                    ) VALUES ($1::uuid,$2,$3,$4,$5,$6::jsonb,$7)
                    RETURNING id, created_at, winner_store_name, winner_total, radius_km
                    """,
                    uid,
                    payload.radius_km,
                    winner_store_id,
                    winner_store_name,
                    winner_total,
                    stores_json,
                    payload.note,
                )
                basket_id = head["id"]

                price_map = {
                    (i.get("product") or "").strip().lower(): i
                    for i in (winner.get("items") or [])
                }

                rows = []
                for it in payload.items:
                    key = (it.product or "").strip().lower()
                    pinfo = price_map.get(key)
                    price = float(pinfo["price"]) if (pinfo and pinfo.get("price") is not None) else None
                    line_total = (price * float(it.quantity)) if price is not None else None

                    rows.append((
                        basket_id, it.product, float(it.quantity), it.unit,
                        price, line_total, winner_store_id, winner_store_name,
                        it.image_url, it.brand, it.size_text
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
        print(
            "SAVE_BASKET_ERROR:",
            type(e).__name__,
            str(e),
            {
                "uid": uid,
                "radius_km": payload.radius_km,
                "winner_store_id": winner_store_id,
                "winner_store_name": winner_store_name,
                "winner_total": winner_total,
                "stores_len": len(stores_json),
                "items_count": len(payload.items),
            },
        )
        traceback.print_exc()
        raise

    return BasketSummaryOut(
        id=head["id"],
        created_at=head["created_at"],
        winner_store_name=head["winner_store_name"],
        winner_total=float(head["winner_total"]) if head["winner_total"] is not None else None,
        radius_km=float(head["radius_km"]) if head["radius_km"] is not None else None,
    )

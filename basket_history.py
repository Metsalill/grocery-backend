# basket_history.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import asyncio
import asyncpg

from auth import get_current_user
from settings import get_db_pool
from compare import compute_compare

router = APIRouter(prefix="/basket-history", tags=["basket-history"])


def get_user_id(user):
    return user["id"] if isinstance(user, dict) else getattr(user, "id", None)


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
    stores: Optional[dict]
    note: Optional[str]
    items: List[dict]


# ---------- Routes ----------
@router.post("", response_model=BasketSummaryOut)
async def save_basket(
    payload: SaveBasketIn,
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = get_user_id(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 1) Compute pricing snapshot
    items_tuples = [(it.product, int(it.quantity)) for it in payload.items]
    cmp = await compute_compare(
        pool=pool,
        items=items_tuples,
        user_lat=payload.lat,
        user_lon=payload.lon,
        radius_km=payload.radius_km,
    )

    stores = cmp.get("stores") or []
    if not stores:
        raise HTTPException(status_code=400, detail="No stores found within given radius")

    # 2) Decide winner
    stores_sorted = sorted(stores, key=lambda s: s["total"])
    winner = None
    if payload.selected_store_id is not None:
        winner = next((s for s in stores_sorted if s["store_id"] == payload.selected_store_id), None)
    if winner is None:
        winner = stores_sorted[0]

    # 3) Persist header + items
    async with pool.acquire() as conn:
        async with conn.transaction():
            head = await conn.fetchrow(
                """
                INSERT INTO basket_history (
                    user_id, radius_km, winner_store_id, winner_store_name,
                    winner_total, stores, note
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                RETURNING id, created_at, winner_store_name, winner_total, radius_km
                """,
                uid,
                payload.radius_km,
                winner["store_id"],
                winner["store_name"],
                winner["total"],
                stores,
                payload.note,
            )
            basket_id = head["id"]

            # map per-product price info from winner
            price_map = { (i.get("product") or "").strip().lower(): i for i in (winner.get("items") or []) }

            tasks = []
            for it in payload.items:
                key = (it.product or "").strip().lower()
                pinfo = price_map.get(key)
                price = float(pinfo["price"]) if (pinfo and pinfo.get("price") is not None) else None
                line_total = (price * float(it.quantity)) if price is not None else None

                tasks.append(conn.execute(
                    """
                    INSERT INTO basket_items (
                        basket_id, product, quantity, unit, price, line_total,
                        store_id, store_name, image_url, brand, size_text
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    """,
                    basket_id,
                    it.product,
                    float(it.quantity),
                    it.unit,
                    price,
                    line_total,
                    winner["store_id"],
                    winner["store_name"],
                    it.image_url,
                    it.brand,
                    it.size_text,
                ))
            await asyncio.gather(*tasks)

    return BasketSummaryOut(
        id=head["id"],
        created_at=head["created_at"],
        winner_store_name=head["winner_store_name"],
        winner_total=float(head["winner_total"]) if head["winner_total"] is not None else None,
        radius_km=float(head["radius_km"]) if head["radius_km"] is not None else None,
    )


@router.get("", response_model=List[BasketSummaryOut])
async def list_baskets(
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = get_user_id(user)
    rows = await pool.fetch(
        """
        SELECT id, created_at, winner_store_name, winner_total, radius_km
        FROM basket_history
        WHERE user_id=$1 AND deleted_at IS NULL
        ORDER BY created_at DESC
        """,
        uid,
    )
    return [
        BasketSummaryOut(
            id=r["id"],
            created_at=r["created_at"],
            winner_store_name=r["winner_store_name"],
            winner_total=float(r["winner_total"]) if r["winner_total"] is not None else None,
            radius_km=float(r["radius_km"]) if r["radius_km"] is not None else None,
        )
        for r in rows
    ]


@router.get("/{basket_id}", response_model=BasketDetailOut)
async def get_basket(
    basket_id: int,
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = get_user_id(user)
    head = await pool.fetchrow(
        """
        SELECT id, created_at, radius_km, winner_store_id, winner_store_name, winner_total, stores, note
        FROM basket_history
        WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL
        """,
        basket_id,
        uid,
    )
    if not head:
        raise HTTPException(status_code=404, detail="Basket not found")

    items = await pool.fetch(
        """
        SELECT product, quantity, unit, price, line_total, store_id, store_name, image_url, brand, size_text
        FROM basket_items
        WHERE basket_id=$1
        ORDER BY id
        """,
        basket_id,
    )

    return BasketDetailOut(
        id=head["id"],
        created_at=head["created_at"],
        radius_km=float(head["radius_km"]) if head["radius_km"] is not None else None,
        winner_store_id=head["winner_store_id"],
        winner_store_name=head["winner_store_name"],
        winner_total=float(head["winner_total"]) if head["winner_total"] is not None else None,
        stores=head["stores"],
        note=head["note"],
        items=[dict(r) for r in items],
    )


@router.delete("/{basket_id}")
async def delete_basket(
    basket_id: int,
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = get_user_id(user)
    res = await pool.execute(
        """
        UPDATE basket_history
        SET deleted_at = NOW()
        WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL
        """,
        basket_id,
        uid,
    )
    if res.split()[-1] == "0":
        raise HTTPException(status_code=404, detail="Basket not found")
    return {"ok": True}

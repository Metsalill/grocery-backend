# basket_history.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import asyncpg
from auth import get_current_user, User  # adjust import if your User differs
from settings import get_db_pool         # same helper you use elsewhere
from compare import compare_basket       # reuse your compare logic if it’s a callable

router = APIRouter(prefix="/basket-history", tags=["basket-history"])

class BasketItemIn(BaseModel):
    product: str = Field(..., description="Product name (same as used in /compare)")
    quantity: float = 1
    unit: Optional[str] = None
    brand: Optional[str] = None
    size_text: Optional[str] = None
    image_url: Optional[str] = None

class SaveBasketIn(BaseModel):
    items: List[BasketItemIn]
    radius_km: float = 10.0
    selected_store_id: Optional[int] = None  # if client wants to override winner
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

@router.post("", response_model=BasketSummaryOut)
async def save_basket(payload: SaveBasketIn,
                      user: User = Depends(get_current_user),
                      pool: asyncpg.pool.Pool = Depends(get_db_pool)):
    """
    Saves a snapshot of the user's basket, including the winner store and per-item pricing.
    Reuses /compare logic to compute totals. If selected_store_id is provided, use that; else pick the winner.
    """
    # 1) Call existing compare logic to get pricing snapshot
    #    Build request compatible with your compare function: [{product: name, quantity}]
    compare_req = [{"product": it.product, "quantity": it.quantity} for it in payload.items]
    cmp = await compare_basket(compare_req, radius_km=payload.radius_km, user=user)  # adapt signature if needed

    if not cmp["stores"]:
        raise HTTPException(status_code=400, detail="No stores found within given radius")

    # Decide winner store
    stores_sorted = sorted(cmp["stores"], key=lambda s: s["total"])
    winner = next((s for s in stores_sorted if (payload.selected_store_id is None or s["store_id"] == payload.selected_store_id)), stores_sorted[0])

    # 2) Persist basket header + items in a transaction
    async with pool.acquire() as conn:
        async with conn.transaction():
            rec = await conn.fetchrow(
                """
                INSERT INTO basket_history (user_id, radius_km, winner_store_id, winner_store_name, winner_total, stores, note)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                RETURNING id, created_at, winner_store_name, winner_total, radius_km
                """,
                user.id, payload.radius_km, winner["store_id"], winner["store_name"], winner["total"], cmp["stores"], payload.note
            )
            basket_id = rec["id"]

            # Map items with winner store prices (fallback None if not found)
            price_map = {i["product"]: i for i in winner.get("items", [])}
            rows = []
            for it in payload.items:
                p = price_map.get(it.product)
                price = p["price"] if p else None
                line_total = (price * it.quantity) if (price is not None) else None
                rows.append(
                    conn.execute(
                        """
                        INSERT INTO basket_items (basket_id, product, quantity, unit, price, line_total,
                                                  store_id, store_name, image_url, brand, size_text)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                        """,
                        basket_id, it.product, it.quantity, it.unit, price, line_total,
                        winner["store_id"], winner["store_name"], it.image_url, it.brand, it.size_text
                    )
                )
            await asyncio.gather(*rows)

    return BasketSummaryOut(
        id=rec["id"],
        created_at=rec["created_at"],
        winner_store_name=rec["winner_store_name"],
        winner_total=float(rec["winner_total"]) if rec["winner_total"] is not None else None,
        radius_km=float(rec["radius_km"]) if rec["radius_km"] is not None else None,
    )

@router.get("", response_model=List[BasketSummaryOut])
async def list_baskets(user: User = Depends(get_current_user),
                       pool: asyncpg.pool.Pool = Depends(get_db_pool)):
    rows = await pool.fetch(
        """
        SELECT id, created_at, winner_store_name, winner_total, radius_km
        FROM basket_history
        WHERE user_id=$1 AND deleted_at IS NULL
        ORDER BY created_at DESC
        """, user.id
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
async def get_basket(basket_id: int,
                     user: User = Depends(get_current_user),
                     pool: asyncpg.pool.Pool = Depends(get_db_pool)):
    head = await pool.fetchrow(
        """
        SELECT id, created_at, radius_km, winner_store_id, winner_store_name, winner_total, stores, note
        FROM basket_history
        WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL
        """, basket_id, user.id
    )
    if not head:
        raise HTTPException(status_code=404, detail="Basket not found")

    items = await pool.fetch(
        """
        SELECT product, quantity, unit, price, line_total, store_id, store_name, image_url, brand, size_text
        FROM basket_items WHERE basket_id=$1
        ORDER BY id
        """, basket_id
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
        items=[dict(r) for r in items]
    )

@router.delete("/{basket_id}")
async def delete_basket(basket_id: int,
                        user: User = Depends(get_current_user),
                        pool: asyncpg.pool.Pool = Depends(get_db_pool)):
    res = await pool.execute(
        """
        UPDATE basket_history
        SET deleted_at = NOW()
        WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL
        """, basket_id, user.id
    )
    if res.split()[-1] == "0":
        raise HTTPException(status_code=404, detail="Basket not found")
    return {"ok": True}

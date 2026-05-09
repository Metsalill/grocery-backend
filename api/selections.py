# api/selections.py

from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Optional

from auth import get_current_user

router = APIRouter()


async def _get_pool(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return pool


async def _get_user_id(conn, email: str) -> Optional[int]:
    row = await conn.fetchrow(
        "SELECT id FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
        email,
    )
    return row["id"] if row else None


@router.post("/products/{product_id}/select")
async def record_selection(
    product_id: int,
    request: Request,
    current_user=Depends(get_current_user),
):
    """Märgi toode valituks — suurendab count-i personaliseeritud järjestuse jaoks."""
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _get_user_id(conn, current_user["email"])
        if user_id is None:
            raise HTTPException(status_code=404, detail="User not found")

        await conn.execute(
            """
            INSERT INTO user_product_selections (user_id, product_id, count, last_used)
            VALUES ($1, $2, 1, now())
            ON CONFLICT (user_id, product_id) DO UPDATE
              SET count     = user_product_selections.count + 1,
                  last_used = now()
            """,
            user_id,
            product_id,
        )
    return {"ok": True}


@router.get("/products/my-selections")
async def my_selections(
    request: Request,
    current_user=Depends(get_current_user),
):
    """Tagasta kasutaja top valitud tooted (järjestatud count DESC)."""
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _get_user_id(conn, current_user["email"])
        if user_id is None:
            raise HTTPException(status_code=404, detail="User not found")

        rows = await conn.fetch(
            """
            SELECT product_id, count, last_used
            FROM user_product_selections
            WHERE user_id = $1
            ORDER BY count DESC, last_used DESC
            LIMIT 100
            """,
            user_id,
        )
    return [dict(r) for r in rows]

# api/favourites.py

from fastapi import APIRouter, Request, Header, HTTPException
from typing import Optional, Dict, Any

router = APIRouter()


async def _get_pool(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return pool


async def _require_user_id(conn, authorization: Optional[str]) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        import os
        from jose import jwt
        token = authorization.split(" ")[1]
        SECRET_KEY = os.getenv("JWT_SECRET", "super-secret-key")
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        email = (payload.get("sub") or "").lower()
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        row = await conn.fetchrow(
            "SELECT id FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
            email,
        )
        if not row:
            raise HTTPException(status_code=401, detail="User not found")
        return row["id"]
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.get("/favourites")
async def get_favourites(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)
        rows = await conn.fetch("""
            SELECT
                p.id,
                COALESCE(pg.canonical_name, p.name) AS name,
                p.image_url,
                COALESCE(pg.brand, p.brand) AS brand,
                p.size_text,
                p.sub_code,
                gc.chains AS available_chains,
                gc.min_price,
                CASE WHEN p.size_text = 'kg' THEN true ELSE false END AS is_per_kg,
                f.created_at AS favourited_at
            FROM favourite_products f
            JOIN products p ON p.id = f.product_id
            LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
            LEFT JOIN product_groups pg ON pg.id = pgm.group_id
            LEFT JOIN mv_group_chains gc
                ON gc.dedup_key = COALESCE(pgm.group_id::text, 'u_' || p.id::text)
            WHERE f.user_id = $1
            ORDER BY f.created_at DESC
        """, user_id)

    items = []
    for r in rows:
        d = dict(r)
        chains = d.get("available_chains") or []
        items.append({
            "id": d["id"],
            "name": d["name"] or "",
            "image_url": d["image_url"],
            "brand": d["brand"] or "",
            "size_text": d["size_text"] or "",
            "sub_code": d["sub_code"],
            "available_chains": sorted(list(set(chains))) if chains else [],
            "min_price": float(d["min_price"]) if d["min_price"] is not None else None,
            "is_per_kg": d["is_per_kg"],
            "favourited_at": d["favourited_at"].isoformat() if d["favourited_at"] else None,
        })

    return {"items": items, "count": len(items)}


@router.post("/favourites/{product_id}")
async def add_favourite(
    request: Request,
    product_id: int,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)
        exists = await conn.fetchval("SELECT 1 FROM products WHERE id = $1", product_id)
        if not exists:
            raise HTTPException(status_code=404, detail="Product not found")
        await conn.execute("""
            INSERT INTO favourite_products (user_id, product_id)
            VALUES ($1, $2)
            ON CONFLICT (user_id, product_id) DO NOTHING
        """, user_id, product_id)

    return {"success": True, "product_id": product_id, "is_favourite": True}


@router.delete("/favourites/{product_id}")
async def remove_favourite(
    request: Request,
    product_id: int,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)
        await conn.execute("""
            DELETE FROM favourite_products
            WHERE user_id = $1 AND product_id = $2
        """, user_id, product_id)

    return {"success": True, "product_id": product_id, "is_favourite": False}


@router.get("/favourites/check/{product_id}")
async def check_favourite(
    request: Request,
    product_id: int,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)
        is_fav = await conn.fetchval("""
            SELECT 1 FROM favourite_products
            WHERE user_id = $1 AND product_id = $2
        """, user_id, product_id)

    return {"product_id": product_id, "is_favourite": bool(is_fav)}

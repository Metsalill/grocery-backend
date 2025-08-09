from fastapi import APIRouter, Request, Query
from typing import Optional

router = APIRouter()

def format_price(price) -> float:
    return round(float(price), 2)

@router.get("/products")
async def list_products(
    request: Request,
    q: Optional[str] = Query("", description="Search by product name"),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),  # 🔹 default first 20
):
    like = f"%{q.strip()}%" if q else "%"

    async with request.app.state.db.acquire() as conn:
        # total distinct products (for pagination UI)
        total = await conn.fetchval("""
            SELECT COUNT(*) FROM (
              SELECT 1
              FROM prices p
              WHERE LOWER(p.product) LIKE LOWER($1)
              GROUP BY p.product, COALESCE(p.manufacturer,''), COALESCE(p.amount,'')
            ) t
        """, like)

        # page of grouped products
        rows = await conn.fetch("""
            WITH grouped AS (
              SELECT
                p.product,
                COALESCE(p.manufacturer,'') AS manufacturer,
                COALESCE(p.amount,'')       AS amount,
                MIN(p.price)                AS min_price,
                MAX(p.price)                AS max_price,
                COUNT(*)                    AS store_count,
                (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
              FROM prices p
              WHERE LOWER(p.product) LIKE LOWER($1)
              GROUP BY p.product, COALESCE(p.manufacturer,''), COALESCE(p.amount,'')
            )
            SELECT *
            FROM grouped
            ORDER BY product
            OFFSET $2
            LIMIT  $3
        """, like, offset, limit)

    items = [{
        "product": r["product"],
        "manufacturer": r["manufacturer"],
        "amount": r["amount"],
        "min_price": format_price(r["min_price"]),
        "max_price": format_price(r["max_price"]),
        "store_count": r["store_count"],
        "image_url": r["image_url"],
    } for r in rows]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }

@router.get("/search-products")
async def search_products(
    request: Request,
    query: str = Query(..., min_length=1)
):
    like = f"%{query.strip()}%"

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            WITH grouped AS (
              SELECT
                p.product,
                (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
              FROM prices p
              WHERE LOWER(p.product) LIKE LOWER($1)
              GROUP BY p.product
            )
            SELECT product, image_url
            FROM grouped
            ORDER BY product
            LIMIT 10
        """, like)

    return [{"name": r["product"], "image": r["image_url"]} for r in rows]

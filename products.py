from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from asyncpg import exceptions as pgerr

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side cap

def _fmt(price) -> Optional[float]:
    return None if price is None else round(float(price), 2)

# ----------------------------- LIST (paged) -----------------------------
@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query("", description="Search by product name"),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
):
    limit = min(int(limit), MAX_LIMIT)
    like = f"%{q.strip()}%" if q else "%"

    async with request.app.state.db.acquire() as conn:
        # Count distinct product groups visible in prices (so only items that actually have a price)
        total = await conn.fetchval(
            """
            SELECT COUNT(*) FROM (
              SELECT 1
              FROM prices p
              JOIN products pr ON pr.id = p.product_id
              WHERE LOWER(pr.name) LIKE LOWER($1)
              GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
            ) t
            """,
            like,
        ) or 0

        rows = await conn.fetch(
            """
            WITH grouped AS (
              SELECT
                pr.name                                 AS product,
                COALESCE(pr.brand,'')                   AS brand,
                COALESCE(pr.size_text,'')               AS size_text,
                MIN(p.price)                            AS min_price,
                MAX(p.price)                            AS max_price,
                COUNT(DISTINCT p.store_id)              AS store_count,
                (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
              FROM prices p
              JOIN products pr ON pr.id = p.product_id
              WHERE LOWER(pr.name) LIKE LOWER($1)
              GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
            )
            SELECT *
            FROM grouped
            ORDER BY product
            OFFSET $2
            LIMIT  $3
            """,
            like, offset, limit,
        )

    items = [{
        "product": r["product"],
        "brand": r["brand"],
        "size_text": r["size_text"],
        "min_price": _fmt(r["min_price"]),
        "max_price": _fmt(r["max_price"]),
        "store_count": r["store_count"],
        "image_url": r["image_url"],
    } for r in rows]

    return {"total": total, "offset": offset, "limit": limit, "items": items}

# ----------------------------- LEGACY SUGGESTIONS (with image) -----------------------------
@router.get("/search-products")
@throttle(limit=30, window=60)
async def search_products_legacy(
    request: Request,
    query: str = Query(..., min_length=2),
):
    q = query.strip()
    if not q or set(q) <= {"%", "*"}:
        raise HTTPException(status_code=400, detail="Query too broad")
    like = f"%{q}%"

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH base AS (
              SELECT
                pr.name AS name,
                (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
              FROM prices p
              JOIN products pr ON pr.id = p.product_id
              WHERE LOWER(pr.name) LIKE LOWER($1)
              GROUP BY pr.name
            )
            SELECT name, image_url
            FROM base
            ORDER BY name
            LIMIT 10
            """,
            like,
        )

    return [{"name": r["name"], "image": r["image_url"]} for r in rows]

# ----------------------------- NEW: TRIGRAM AUTOCOMPLETE -----------------------------
@router.get("/products/search")
@throttle(limit=60, window=60)
async def products_search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64, description="Search text"),
    limit: int = Query(10, ge=1, le=50),
):
    term = q.strip()
    if not term:
        return []

    sql_trgm = """
    WITH input AS (SELECT $1::text AS q)
    SELECT p.id, p.name
    FROM products p, input
    WHERE      p.name ILIKE q || '%'                -- prefix
           OR  p.name % q                           -- trigram (pg_trgm)
           OR  p.name ILIKE '%' || q || '%'         -- contains
    ORDER BY
      CASE WHEN p.name ILIKE q || '%' THEN 0 ELSE 1 END,
      similarity(p.name, q) DESC,
      p.name ASC
    LIMIT $2
    """

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(sql_trgm, term, limit)
            if rows:
                return [{"id": r["id"], "name": r["name"]} for r in rows]
            # Fallback: simple LIKE from products if trigram returns nothing
            fb = await conn.fetch(
                "SELECT id, name FROM products WHERE LOWER(name) LIKE LOWER($1) ORDER BY name LIMIT $2",
                f"%{term}%", limit,
            )
            return [{"id": r["id"], "name": r["name"]} for r in fb]
        except (pgerr.UndefinedFunctionError, pgerr.UndefinedTableError):
            fb = await conn.fetch(
                "SELECT id, name FROM products WHERE LOWER(name) LIKE LOWER($1) ORDER BY name LIMIT $2",
                f"%{term}%", limit,
            )
            return [{"id": r["id"], "name": r["name"]} for r in fb]

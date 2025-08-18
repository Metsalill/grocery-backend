from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional

# import from utils.throttle instead of main.py to avoid circular import
from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # hard cap to avoid huge pages

def format_price(price) -> float:
    return round(float(price), 2)

@router.get("/products")
@throttle(limit=120, window=60)  # up to 120 req/min per IP for listing
async def list_products(
    request: Request,
    q: Optional[str] = Query("", description="Search by product name"),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),  # UI hint; hard-capped below
):
    # enforce server-side cap
    limit = min(int(limit), MAX_LIMIT)

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
@throttle(limit=30, window=60)  # tighter: search is a hot target
async def search_products_legacy(
    request: Request,
    query: str = Query(..., min_length=2)  # enforce min length
):
    """
    Legacy LIKE-based search for quick suggestions. Kept for back-compat.
    """
    q = query.strip()
    # deny wildcard-only or junk queries that tend to be used by scrapers
    if not q or set(q) <= {"%", "*"}:
        raise HTTPException(status_code=400, detail="Query too broad")

    like = f"%{q}%"

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

# ------------------ NEW: Trigram + prefix autocomplete ------------------

@router.get("/products/search")
@throttle(limit=60, window=60)  # balanced; autocomplete gets frequent hits
async def products_search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64, description="Search text"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Autocomplete using pg_trgm similarity + prefix boost.
    Prefers the `products` table; falls back to a LIKE search on `prices`
    if `products` or pg_trgm isn't available.
    Returns [{id, name}] (id may be null on fallback).
    """
    term = q.strip()
    if not term:
        return []

    # Primary: products table with trigram
    sql_products = """
    WITH input AS (SELECT $1::text AS q)
    SELECT p.id, p.product AS name
    FROM products p, input
    WHERE p.product ILIKE q || '%'         -- prefix boost
       OR p.product % q                    -- trigram similarity (pg_trgm)
    ORDER BY
      CASE WHEN p.product ILIKE q || '%' THEN 0 ELSE 1 END,
      similarity(p.product, q) DESC,
      p.product ASC
    LIMIT $2
    """

    # Fallback: DISTINCT names from prices with LIKE only
    sql_fallback = """
    WITH input AS (SELECT $1::text AS q)
    SELECT name FROM (
      SELECT DISTINCT p.product AS name
      FROM prices p, input
      WHERE p.product ILIKE q || '%'
         OR p.product ILIKE '%' || q || '%'
    ) t
    ORDER BY
      CASE WHEN name ILIKE $1 || '%' THEN 0 ELSE 1 END,
      name ASC
    LIMIT $2
    """

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(sql_products, term, limit)
            if rows:
                return [{"id": r["id"], "name": r["name"]} for r in rows]
            # no hits? fall back to LIKE to be generous
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]
        except Exception:
            # products table or pg_trgm might be missing in some envs
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]

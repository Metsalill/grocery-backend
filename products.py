from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from asyncpg import exceptions as pgerr
from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # hard cap

def format_price(price) -> Optional[float]:
    if price is None:
        return None
    try:
        return round(float(price), 2)
    except Exception:
        return None

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
        # ---------- Primary: group from prices ----------
        total_prices = await conn.fetchval("""
            SELECT COUNT(*) FROM (
              SELECT 1
              FROM prices p
              WHERE LOWER(p.product) LIKE LOWER($1)
              GROUP BY p.product, COALESCE(p.manufacturer,''), COALESCE(p.amount,'')
            ) t
        """, like)

        rows = []
        if total_prices and total_prices > 0:
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

            total = total_prices
        else:
            # ---------- Fallback: list from products ----------
            total_products = await conn.fetchval("""
                SELECT COUNT(*)
                FROM products p
                WHERE LOWER(COALESCE(p.product, p.name, p.product_name)) LIKE LOWER($1)
            """, like)

            rows = await conn.fetch("""
                SELECT
                  COALESCE(p.product, p.name, p.product_name) AS product,
                  ''::text       AS manufacturer,
                  ''::text       AS amount,
                  NULL::numeric  AS min_price,
                  NULL::numeric  AS max_price,
                  0::int         AS store_count,
                  NULL::text     AS image_url
                FROM products p
                WHERE LOWER(COALESCE(p.product, p.name, p.product_name)) LIKE LOWER($1)
                ORDER BY 1
                OFFSET $2
                LIMIT  $3
            """, like, offset, limit)

            total = int(total_products or 0)

    items = [{
        "product": r["product"],
        "manufacturer": r["manufacturer"],
        "amount": r["amount"],
        "min_price": format_price(r.get("min_price") if isinstance(r, dict) else r["min_price"]),
        "max_price": format_price(r.get("max_price") if isinstance(r, dict) else r["max_price"]),
        "store_count": r["store_count"],
        "image_url": r["image_url"],
    } for r in rows]

    return {"total": total, "offset": offset, "limit": limit, "items": items}

# ------------------ Trigram + prefix autocomplete ------------------

@router.get("/products/search")
@throttle(limit=60, window=60)
async def products_search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64),
    limit: int = Query(10, ge=1, le=50),
):
    term = q.strip()
    if not term:
        return []

    sql_products = """
    WITH input AS (SELECT $1::text AS q)
    SELECT
      p.id,
      COALESCE(p.product, p.name, p.product_name) AS name
    FROM products p, input
    WHERE
          COALESCE(p.product, p.name, p.product_name) ILIKE q || '%'
       OR COALESCE(p.product, p.name, p.product_name) % q
       OR COALESCE(p.product, p.name, p.product_name) ILIKE '%' || q || '%'
    ORDER BY
      CASE WHEN COALESCE(p.product, p.name, p.product_name) ILIKE q || '%' THEN 0 ELSE 1 END,
      similarity(COALESCE(p.product, p.name, p.product_name), q) DESC,
      COALESCE(p.product, p.name, p.product_name) ASC
    LIMIT $2
    """

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
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]
        except (pgerr.UndefinedTableError, pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]
        except Exception:
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]

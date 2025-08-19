from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from asyncpg import exceptions as pgerr  # for graceful fallbacks

# import from utils.throttle instead of main.py to avoid circular import
from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # hard cap to avoid huge pages


def format_price(price) -> Optional[float]:
    if price is None:
        return None
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
        # ----- primary: group from PRICES -----
        total = await conn.fetchval(
            """
            SELECT COUNT(*) FROM (
              SELECT 1
              FROM prices p
              WHERE LOWER(p.product) LIKE LOWER($1)
              GROUP BY p.product, COALESCE(p.manufacturer,''), COALESCE(p.amount,'')
            ) t
            """,
            like,
        )

        rows = []
        if total and total > 0:
            rows = await conn.fetch(
                """
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
                """,
                like,
                offset,
                limit,
            )
        else:
            # ----- fallback: surface names from PRODUCTS (no price data) -----
            try:
                # count from products
                total = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM products p
                    WHERE LOWER(COALESCE(p.product, p.name, p.product_name)) LIKE LOWER($1)
                    """,
                    like,
                )

                rows = await conn.fetch(
                    """
                    SELECT
                      COALESCE(p.product, p.name, p.product_name) AS product,
                      ''::text                    AS manufacturer,
                      ''::text                    AS amount,
                      NULL::numeric               AS min_price,
                      NULL::numeric               AS max_price,
                      0::int                      AS store_count,
                      NULL::text                  AS image_url
                    FROM products p
                    WHERE LOWER(COALESCE(p.product, p.name, p.product_name)) LIKE LOWER($1)
                    ORDER BY COALESCE(p.product, p.name, p.product_name)
                    OFFSET $2
                    LIMIT  $3
                    """,
                    like,
                    offset,
                    limit,
                )
            except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError):
                total = 0
                rows = []

    items = [
        {
            "product": r["product"],
            "manufacturer": r["manufacturer"],
            "amount": r["amount"],
            "min_price": format_price(r["min_price"]),
            "max_price": format_price(r["max_price"]),
            "store_count": r["store_count"],
            "image_url": r["image_url"],
        }
        for r in rows
    ]

    return {"total": int(total or 0), "offset": offset, "limit": limit, "items": items}


# ------------------ Legacy: LIKE suggestions from PRICES ------------------
@router.get("/search-products")
@throttle(limit=30, window=60)  # tighter: search is a hot target
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
            """,
            like,
        )

    return [{"name": r["product"], "image": r["image_url"]} for r in rows]


# ------------------ NEW: Trigram + prefix autocomplete ------------------
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
        except (
            pgerr.UndefinedTableError,
            pgerr.UndefinedFunctionError,
            pgerr.UndefinedObjectError,
        ):
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]
        except Exception:
            fb = await conn.fetch(sql_fallback, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]

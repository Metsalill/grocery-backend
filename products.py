from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from asyncpg import exceptions as pgerr
from utils.throttle import throttle

router = APIRouter()
MAX_LIMIT = 50

def fmt_price(v):
    return round(float(v), 2) if v is not None else None

# -------- /products --------
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

    # Old schema: prices.product (text)
    SQL_TOTAL_PLAIN = """
        SELECT COUNT(*) FROM (
          SELECT 1
          FROM prices p
          WHERE LOWER(p.product) LIKE LOWER($1)
          GROUP BY p.product, COALESCE(p.manufacturer,''), COALESCE(p.amount,'')
        ) t
    """
    SQL_ROWS_PLAIN = """
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
    """

    # New schema: prices.product_id -> products (name or product_name)
    SQL_TOTAL_JOIN = """
        SELECT COUNT(*) FROM (
          SELECT 1
          FROM prices p
          LEFT JOIN products pr ON pr.id = p.product_id
          WHERE LOWER(COALESCE(pr.product_name, pr.name, '')) LIKE LOWER($1)
          GROUP BY
            COALESCE(pr.product_name, pr.name, ''),
            COALESCE(p.manufacturer, ''),
            COALESCE(p.amount, '')
        ) t
    """
    SQL_ROWS_JOIN = """
        WITH base AS (
          SELECT
            COALESCE(pr.product_name, pr.name, '') AS product,
            COALESCE(p.manufacturer, '')           AS manufacturer,
            COALESCE(p.amount, '')                 AS amount,
            p.price,
            p.image_url
          FROM prices p
          LEFT JOIN products pr ON pr.id = p.product_id
          WHERE LOWER(COALESCE(pr.product_name, pr.name, '')) LIKE LOWER($1)
        ),
        grouped AS (
          SELECT
            product,
            manufacturer,
            amount,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            COUNT(*)   AS store_count,
            (ARRAY_AGG(image_url ORDER BY (image_url IS NULL) ASC))[1] AS image_url
          FROM base
          GROUP BY product, manufacturer, amount
        )
        SELECT *
        FROM grouped
        ORDER BY product
        OFFSET $2
        LIMIT  $3
    """

    # Products-only fallback (no prices)
    SQL_TOTAL_PRODUCTS_ONLY = """
        SELECT COUNT(*)
        FROM products pr
        WHERE LOWER(COALESCE(pr.product_name, pr.name, '')) LIKE LOWER($1)
    """
    SQL_ROWS_PRODUCTS_ONLY = """
        SELECT
          COALESCE(pr.product_name, pr.name, '') AS product,
          ''::text AS manufacturer,
          ''::text AS amount,
          NULL::float8 AS min_price,
          NULL::float8 AS max_price,
          0::int AS store_count,
          NULL::text AS image_url
        FROM products pr
        WHERE LOWER(COALESCE(pr.product_name, pr.name, '')) LIKE LOWER($1)
        ORDER BY COALESCE(pr.product_name, pr.name, '')
        OFFSET $2
        LIMIT  $3
    """

    async with request.app.state.db.acquire() as conn:
        try:
            total = await conn.fetchval(SQL_TOTAL_PLAIN, like)
            rows = []
            if total and total > 0:
                rows = await conn.fetch(SQL_ROWS_PLAIN, like, offset, limit)
            else:
                try:
                    total = await conn.fetchval(SQL_TOTAL_JOIN, like)
                    if total and total > 0:
                        rows = await conn.fetch(SQL_ROWS_JOIN, like, offset, limit)
                    else:
                        total = await conn.fetchval(SQL_TOTAL_PRODUCTS_ONLY, like) or 0
                        rows = await conn.fetch(SQL_ROWS_PRODUCTS_ONLY, like, offset, limit)
                except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError):
                    total = await conn.fetchval(SQL_TOTAL_PRODUCTS_ONLY, like) or 0
                    rows = await conn.fetch(SQL_ROWS_PRODUCTS_ONLY, like, offset, limit)
        except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError):
            try:
                total = await conn.fetchval(SQL_TOTAL_JOIN, like)
                if total and total > 0:
                    rows = await conn.fetch(SQL_ROWS_JOIN, like, offset, limit)
                else:
                    total = await conn.fetchval(SQL_TOTAL_PRODUCTS_ONLY, like) or 0
                    rows = await conn.fetch(SQL_ROWS_PRODUCTS_ONLY, like, offset, limit)
            except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError):
                total = await conn.fetchval(SQL_TOTAL_PRODUCTS_ONLY, like) or 0
                rows = await conn.fetch(SQL_ROWS_PRODUCTS_ONLY, like, offset, limit)

    items = [{
        "product": r["product"],
        "manufacturer": r["manufacturer"],
        "amount": r["amount"],
        "min_price": fmt_price(r.get("min_price")),
        "max_price": fmt_price(r.get("max_price")),
        "store_count": r["store_count"],
        "image_url": r.get("image_url"),
    } for r in rows]

    return {"total": total or 0, "offset": offset, "limit": limit, "items": items}


# ---- Legacy suggestions (safe for product_id schema too) ----
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

    SQL_SUGGEST_PLAIN = """
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
    """
    SQL_SUGGEST_JOIN = """
        WITH base AS (
          SELECT
            COALESCE(pr.product_name, pr.name, '') AS product,
            p.image_url
          FROM prices p
          LEFT JOIN products pr ON pr.id = p.product_id
          WHERE LOWER(COALESCE(pr.product_name, pr.name, '')) LIKE LOWER($1)
        ),
        grouped AS (
          SELECT
            product,
            (ARRAY_AGG(image_url ORDER BY (image_url IS NULL) ASC))[1] AS image_url
          FROM base
          GROUP BY product
        )
        SELECT product, image_url
        FROM grouped
        ORDER BY product
        LIMIT 10
    """

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(SQL_SUGGEST_PLAIN, like)
        except pgerr.UndefinedColumnError:
            rows = await conn.fetch(SQL_SUGGEST_JOIN, like)
        except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError):
            rows = []

    return [{"name": r["product"], "image": r["image_url"]} for r in rows]


# ---- Trigram autocomplete (fallback updated too) ----
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
      COALESCE(p.product_name, p.name) AS name
    FROM products p, input
    WHERE
          COALESCE(p.product_name, p.name) ILIKE q || '%'
       OR COALESCE(p.product_name, p.name) % q
       OR COALESCE(p.product_name, p.name) ILIKE '%' || q || '%'
    ORDER BY
      CASE WHEN COALESCE(p.product_name, p.name) ILIKE q || '%' THEN 0 ELSE 1 END,
      similarity(COALESCE(p.product_name, p.name), q) DESC,
      COALESCE(p.product_name, p.name) ASC
    LIMIT $2
    """

    sql_fallback = """
    WITH input AS (SELECT $1::text AS q)
    SELECT name FROM (
      SELECT DISTINCT COALESCE(pr.product_name, pr.name) AS name
      FROM prices p
      LEFT JOIN products pr ON pr.id = p.product_id
      , input
      WHERE COALESCE(pr.product_name, pr.name) ILIKE q || '%'
         OR COALESCE(pr.product_name, pr.name) ILIKE '%' || q || '%'
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

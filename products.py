from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from asyncpg import exceptions as pgerr  # graceful fallbacks
from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server cap


def _fmt(v) -> Optional[float]:
    return None if v is None else round(float(v), 2)


# ========================= /products (list + search) =========================
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
        try:
            # ---- Canonical schema: prices(product_id) -> products(id, name) ----
            total = await conn.fetchval(
                """
                SELECT COUNT(*) FROM (
                  SELECT 1
                  FROM prices p
                  JOIN products pr ON pr.id = p.product_id
                  WHERE LOWER(pr.name) LIKE LOWER($1)
                  GROUP BY pr.name
                ) t
                """,
                like,
            )

            rows = await conn.fetch(
                """
                WITH grouped AS (
                  SELECT
                    pr.name                                AS product,
                    MIN(p.price)                           AS min_price,
                    MAX(p.price)                           AS max_price,
                    COUNT(DISTINCT p.store_id)             AS store_count,
                    (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
                  FROM prices p
                  JOIN products pr ON pr.id = p.product_id
                  WHERE LOWER(pr.name) LIKE LOWER($1)
                  GROUP BY pr.name
                )
                SELECT
                  product,
                  ''::text   AS manufacturer,  -- brand/amount not guaranteed in schema
                  ''::text   AS amount,
                  min_price, max_price, store_count, image_url
                FROM grouped
                ORDER BY product
                OFFSET $2
                LIMIT  $3
                """,
                like,
                offset,
                limit,
            )

        except (pgerr.UndefinedColumnError, pgerr.UndefinedTableError):
            # ---- Legacy schema fallback: prices.product (text) ----
            total = await conn.fetchval(
                """
                SELECT COUNT(*) FROM (
                  SELECT 1
                  FROM prices p
                  WHERE LOWER(p.product) LIKE LOWER($1)
                  GROUP BY p.product
                ) t
                """,
                like,
            ) or 0

            rows = await conn.fetch(
                """
                WITH grouped AS (
                  SELECT
                    p.product                              AS product,
                    MIN(p.price)                           AS min_price,
                    MAX(p.price)                           AS max_price,
                    COUNT(DISTINCT p.store_id)             AS store_count,
                    (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
                  FROM prices p
                  WHERE LOWER(p.product) LIKE LOWER($1)
                  GROUP BY p.product
                )
                SELECT
                  product,
                  ''::text AS manufacturer,
                  ''::text AS amount,
                  min_price, max_price, store_count, image_url
                FROM grouped
                ORDER BY product
                OFFSET $2
                LIMIT  $3
                """,
                like,
                offset,
                limit,
            )

    items = [
        {
            "product": r["product"],
            "manufacturer": r["manufacturer"],
            "amount": r["amount"],
            "min_price": _fmt(r["min_price"]),
            "max_price": _fmt(r["max_price"]),
            "store_count": r["store_count"],
            "image_url": r["image_url"],
        }
        for r in rows
    ]
    return {"total": total or 0, "offset": offset, "limit": limit, "items": items}


# ========================= Legacy suggestions =========================
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
        try:
            # Canonical join
            rows = await conn.fetch(
                """
                WITH grouped AS (
                  SELECT
                    pr.name                                AS product,
                    (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
                  FROM prices p
                  JOIN products pr ON pr.id = p.product_id
                  WHERE LOWER(pr.name) LIKE LOWER($1)
                  GROUP BY pr.name
                )
                SELECT product, image_url
                FROM grouped
                ORDER BY product
                LIMIT 10
                """,
                like,
            )
        except (pgerr.UndefinedColumnError, pgerr.UndefinedTableError):
            # Legacy prices.product
            rows = await conn.fetch(
                """
                WITH grouped AS (
                  SELECT
                    p.product                              AS product,
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


# ========================= New: trigram autocomplete =========================
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
    SELECT id, name
    FROM products, input
    WHERE name ILIKE q || '%'
       OR name % q
       OR name ILIKE '%' || q || '%'
    ORDER BY
      CASE WHEN name ILIKE q || '%' THEN 0 ELSE 1 END,
      similarity(name, q) DESC,
      name ASC
    LIMIT $2
    """

    # Fallback: get distinct names from catalogued prices
    sql_fallback = """
    WITH input AS (SELECT $1::text AS q)
    SELECT name FROM (
      SELECT DISTINCT pr.name AS name
      FROM prices p
      JOIN products pr ON pr.id = p.product_id, input
      WHERE pr.name ILIKE q || '%' OR pr.name ILIKE '%' || q || '%'
    ) t
    ORDER BY
      CASE WHEN name ILIKE $1 || '%' THEN 0 ELSE 1 END,
      name ASC
    LIMIT $2
    """

    sql_fallback_legacy = """
    WITH input AS (SELECT $1::text AS q)
    SELECT name FROM (
      SELECT DISTINCT p.product AS name
      FROM prices p, input
      WHERE p.product ILIKE q || '%' OR p.product ILIKE '%' || q || '%'
    ) t
    ORDER BY
      CASE WHEN name ILIKE $1 || '%' THEN 0 ELSE 1 END,
      name ASC
    LIMIT $2
    """

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(sql_trgm, term, limit)
            if rows:
                return [{"id": r["id"], "name": r["name"]} for r in rows]
            # no trigram hits → fallback to names seen in prices
            try:
                fb = await conn.fetch(sql_fallback, term, limit)
            except (pgerr.UndefinedColumnError, pgerr.UndefinedTableError):
                fb = await conn.fetch(sql_fallback_legacy, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]
        except (pgerr.UndefinedFunctionError, pgerr.UndefinedTableError):
            # pg_trgm or products missing → fallback
            try:
                fb = await conn.fetch(sql_fallback, term, limit)
            except (pgerr.UndefinedColumnError, pgerr.UndefinedTableError):
                fb = await conn.fetch(sql_fallback_legacy, term, limit)
            return [{"id": None, "name": r["name"]} for r in fb]

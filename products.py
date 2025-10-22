# products.py
from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional, List, Dict, Any
from asyncpg import exceptions as pgerr

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side cap


def _fmt(price) -> Optional[float]:
    return None if price is None else round(float(price), 2)


# -------------------------------------------------------------------
# LIST (paged) — one row per logical product (name+brand+size_text)
# NOTE: no image fields are referenced; image_url is returned as null.
# -------------------------------------------------------------------
@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query(
        "",
        description="Search by product name (products.name + optional aliases)",
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
):
    """
    Returns one row per product (name, brand, size_text) aggregated over prices.
    Only products that have at least one price row are included.
    """
    like = f"%{(q or '').strip()}%" if q else "%"
    limit = min(int(limit), MAX_LIMIT)

    # Prefer matching products + aliases if aliases table exists;
    # otherwise fall back to products-only queries.
    SQL_COUNT_WITH_ALIASES = """
      SELECT COUNT(*) FROM (
        SELECT 1
        FROM prices p
        JOIN products pr ON pr.id = p.product_id
        WHERE
          LOWER(pr.name) LIKE LOWER($1)
          OR EXISTS (
            SELECT 1 FROM product_aliases a
            WHERE a.product_id = pr.id AND LOWER(a.alias) LIKE LOWER($1)
          )
        GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
      ) t
    """

    SQL_PAGE_WITH_ALIASES = """
      WITH grouped AS (
        SELECT
          pr.name                         AS product,
          COALESCE(pr.brand,'')           AS brand,
          COALESCE(pr.size_text,'')       AS size_text,
          MIN(p.price)                    AS min_price,
          MAX(p.price)                    AS max_price,
          COUNT(DISTINCT p.store_id)      AS store_count
        FROM prices p
        JOIN products pr ON pr.id = p.product_id
        WHERE
          LOWER(pr.name) LIKE LOWER($1)
          OR EXISTS (
            SELECT 1 FROM product_aliases a
            WHERE a.product_id = pr.id AND LOWER(a.alias) LIKE LOWER($1)
          )
        GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
      )
      SELECT product, brand, size_text, min_price, max_price, store_count
      FROM grouped
      ORDER BY lower(product), lower(brand), lower(size_text)
      OFFSET $2
      LIMIT  $3
    """

    SQL_COUNT_NO_ALIASES = """
      SELECT COUNT(*) FROM (
        SELECT 1
        FROM prices p
        JOIN products pr ON pr.id = p.product_id
        WHERE LOWER(pr.name) LIKE LOWER($1)
        GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
      ) t
    """

    SQL_PAGE_NO_ALIASES = """
      WITH grouped AS (
        SELECT
          pr.name                         AS product,
          COALESCE(pr.brand,'')           AS brand,
          COALESCE(pr.size_text,'')       AS size_text,
          MIN(p.price)                    AS min_price,
          MAX(p.price)                    AS max_price,
          COUNT(DISTINCT p.store_id)      AS store_count
        FROM prices p
        JOIN products pr ON pr.id = p.product_id
        WHERE LOWER(pr.name) LIKE LOWER($1)
        GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
      )
      SELECT product, brand, size_text, min_price, max_price, store_count
      FROM grouped
      ORDER BY lower(product), lower(brand), lower(size_text)
      OFFSET $2
      LIMIT  $3
    """

    async with request.app.state.db.acquire() as conn:
        try:
            total = await conn.fetchval(SQL_COUNT_WITH_ALIASES, like) or 0
            rows = await conn.fetch(SQL_PAGE_WITH_ALIASES, like, offset, limit)
        except pgerr.UndefinedTableError:
            total = await conn.fetchval(SQL_COUNT_NO_ALIASES, like) or 0
            rows = await conn.fetch(SQL_PAGE_NO_ALIASES, like, offset, limit)

    items = [
        {
            "product": r["product"],
            "brand": r["brand"],
            "size_text": r["size_text"],
            "min_price": _fmt(r["min_price"]),
            "max_price": _fmt(r["max_price"]),
            "store_count": int(r["store_count"]),
            "image_url": None,  # image support will be added later
        }
        for r in rows
    ]

    return {"total": int(total), "offset": int(offset), "limit": int(limit), "items": items}


# -------------------------------------------------------------------
# Legacy suggestions endpoint — no images; returns minimal payload.
# -------------------------------------------------------------------
@router.get("/search-products")
@throttle(limit=30, window=60)
async def search_products_legacy(
    request: Request,
    query: str = Query(..., min_length=2),
):
    """
    Simple LIKE suggestions (products with at least one price).
    Returns list of { name, image } where image is null (for now).
    """
    q = (query or "").strip()
    if not q or set(q) <= {"%", "*"}:
        raise HTTPException(status_code=400, detail="Query too broad")
    like = f"%{q}%"

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pr.name AS name
            FROM prices p
            JOIN products pr ON pr.id = p.product_id
            WHERE LOWER(pr.name) LIKE LOWER($1)
            GROUP BY pr.name
            ORDER BY lower(pr.name)
            LIMIT 10
            """,
            like,
        )

    return [{"name": r["name"], "image": None} for r in rows]


# -------------------------------------------------------------------
# Trigram (or LIKE) autocomplete for the typeahead.
# -------------------------------------------------------------------
@router.get("/products/search")
@throttle(limit=60, window=60)
async def products_search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64, description="Search text"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Autocomplete based on products.name, optionally product_aliases.alias,
    using pg_trgm when available; falls back to LIKE if needed.
    """
    term = (q or "").strip()
    if not term:
        return []

    SQL_TRGM_WITH_ALIASES = """
      WITH input AS (SELECT $1::text AS q)
      SELECT p.id, p.name
      FROM products p
      LEFT JOIN product_aliases a ON a.product_id = p.id
      , input
      WHERE
             p.name ILIKE q || '%'
          OR p.name % q
          OR p.name ILIKE '%' || q || '%'
          OR a.alias ILIKE q || '%'
          OR a.alias % q
          OR a.alias ILIKE '%' || q || '%'
      GROUP BY p.id, p.name
      ORDER BY
        CASE WHEN p.name ILIKE q || '%' THEN 0 ELSE 1 END,
        similarity(p.name, q) DESC,
        p.name ASC
      LIMIT $2
    """

    SQL_TRGM_NO_ALIASES = """
      WITH input AS (SELECT $1::text AS q)
      SELECT p.id, p.name
      FROM products p, input
      WHERE
             p.name ILIKE q || '%'
          OR p.name % q
          OR p.name ILIKE '%' || q || '%'
      ORDER BY
        CASE WHEN p.name ILIKE q || '%' THEN 0 ELSE 1 END,
        similarity(p.name, q) DESC,
        p.name ASC
      LIMIT $2
    """

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(SQL_TRGM_WITH_ALIASES, term, limit)
        except (pgerr.UndefinedTableError, pgerr.UndefinedFunctionError):
            try:
                rows = await conn.fetch(SQL_TRGM_NO_ALIASES, term, limit)
            except pgerr.UndefinedFunctionError:
                rows = await conn.fetch(
                    """
                    SELECT id, name
                    FROM products
                    WHERE LOWER(name) LIKE LOWER($1)
                    ORDER BY name
                    LIMIT $2
                    """,
                    f"%{term}%", limit,
                )

    return [{"id": r["id"], "name": r["name"]} for r in rows]

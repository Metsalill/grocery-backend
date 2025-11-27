from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from asyncpg import exceptions as pgerr

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side cap


# ----------------------------- LIST (paged, NO PRICES) -----------------------------
@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query(
        "",
        description="Search by product name (uses products + optional aliases); empty lists everything (paged).",
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    main_code: Optional[str] = Query(
        None,
        description="Optional main category code (e.g. 'produce', 'meat_fish'). Matches products.food_group.",
    ),
    sub_code: Optional[str] = Query(
        None,
        description="Optional subcategory code (e.g. 'produce_apples_pears'). Uses product_categories when present.",
    ),
):
    """
    Lightweight catalogue:
      - One row per product (from `products`).
      - NO min/max/store price aggregation.
      - Only include products that have at least one row in `prices`.
      - Hide garbage names that are only digits (e.g. '19765').

    Filters:
      - q: name / alias search (LIKE).
      - main_code: matches products.food_group (produce, meat_fish, ...).
      - sub_code: requires at least one row in product_categories for that subcategory.

    Returned fields: id, product, brand, size_text, image_url (null for now).
    """
    limit = min(int(limit), MAX_LIMIT)
    like = f"%{(q or '').strip()}%" if q is not None else "%"

    # With product_aliases (preferred)
    SQL_COUNT_WITH_ALIASES = """
      SELECT COUNT(*)
      FROM products pr
      WHERE pr.name !~ '^[0-9]+$'                    -- hide purely numeric "names"
        AND EXISTS (SELECT 1 FROM prices p WHERE p.product_id = pr.id)
        -- main category via products.food_group; ignored when $2 is NULL
        AND ($2::text IS NULL OR pr.food_group = $2)
        -- subcategory via product_categories; ignored when $3 is NULL
        AND (
          $3::text IS NULL OR EXISTS (
            SELECT 1
            FROM product_categories pc
            JOIN categories_sub cs ON cs.id = pc.sub_id
            WHERE pc.product_id = pr.id
              AND cs.code = $3
          )
        )
        AND (
              LOWER(pr.name) LIKE LOWER($1)
           OR EXISTS (
                SELECT 1
                FROM product_aliases a
                WHERE a.product_id = pr.id AND LOWER(a.alias) LIKE LOWER($1)
           )
        )
    """

    SQL_PAGE_WITH_ALIASES = """
      SELECT
        pr.id,
        pr.name                               AS product,
        COALESCE(pr.brand, '')                AS brand,
        COALESCE(pr.size_text, '')            AS size_text,
        NULL::text                            AS image_url
      FROM products pr
      WHERE pr.name !~ '^[0-9]+$'
        AND EXISTS (SELECT 1 FROM prices p WHERE p.product_id = pr.id)
        AND ($2::text IS NULL OR pr.food_group = $2)
        AND (
          $3::text IS NULL OR EXISTS (
            SELECT 1
            FROM product_categories pc
            JOIN categories_sub cs ON cs.id = pc.sub_id
            WHERE pc.product_id = pr.id
              AND cs.code = $3
          )
        )
        AND (
              LOWER(pr.name) LIKE LOWER($1)
           OR EXISTS (
                SELECT 1
                FROM product_aliases a
                WHERE a.product_id = pr.id AND LOWER(a.alias) LIKE LOWER($1)
           )
        )
      ORDER BY lower(pr.name), lower(brand), lower(size_text)
      OFFSET $4
      LIMIT  $5
    """

    # Fallback if product_aliases table is missing
    SQL_COUNT_NO_ALIASES = """
      SELECT COUNT(*)
      FROM products pr
      WHERE pr.name !~ '^[0-9]+$'
        AND EXISTS (SELECT 1 FROM prices p WHERE p.product_id = pr.id)
        AND ($2::text IS NULL OR pr.food_group = $2)
        AND (
          $3::text IS NULL OR EXISTS (
            SELECT 1
            FROM product_categories pc
            JOIN categories_sub cs ON cs.id = pc.sub_id
            WHERE pc.product_id = pr.id
              AND cs.code = $3
          )
        )
        AND LOWER(pr.name) LIKE LOWER($1)
    """

    SQL_PAGE_NO_ALIASES = """
      SELECT
        pr.id,
        pr.name                               AS product,
        COALESCE(pr.brand, '')                AS brand,
        COALESCE(pr.size_text, '')            AS size_text,
        NULL::text                            AS image_url
      FROM products pr
      WHERE pr.name !~ '^[0-9]+$'
        AND EXISTS (SELECT 1 FROM prices p WHERE p.product_id = pr.id)
        AND ($2::text IS NULL OR pr.food_group = $2)
        AND (
          $3::text IS NULL OR EXISTS (
            SELECT 1
            FROM product_categories pc
            JOIN categories_sub cs ON cs.id = pc.sub_id
            WHERE pc.product_id = pr.id
              AND cs.code = $3
          )
        )
        AND LOWER(pr.name) LIKE LOWER($1)
      ORDER BY lower(pr.name), lower(brand), lower(size_text)
      OFFSET $4
      LIMIT  $5
    """

    async with request.app.state.db.acquire() as conn:
        try:
            total = await conn.fetchval(
                SQL_COUNT_WITH_ALIASES, like, main_code, sub_code
            ) or 0
            rows = await conn.fetch(
                SQL_PAGE_WITH_ALIASES, like, main_code, sub_code, offset, limit
            )
        except pgerr.UndefinedTableError:
            # product_aliases or product_categories doesn’t exist – proceed without it
            total = await conn.fetchval(
                SQL_COUNT_NO_ALIASES, like, main_code, sub_code
            ) or 0
            rows = await conn.fetch(
                SQL_PAGE_NO_ALIASES, like, main_code, sub_code, offset, limit
            )

    items = [
        {
            "id": r["id"],
            "product": r["product"],
            "brand": r["brand"],
            "size_text": r["size_text"],
            "image_url": r["image_url"],  # null for now (app shows placeholder)
        }
        for r in rows
    ]

    return {"total": total, "offset": offset, "limit": limit, "items": items}


# ----------------------------- LEGACY SUGGESTIONS (still light) -----------------------------
@router.get("/search-products")
@throttle(limit=30, window=60)
async def search_products_legacy(
    request: Request,
    query: str = Query(..., min_length=2),
):
    """
    Legacy suggestions for typeahead (name only).
    Filters to products that have at least one price.
    """
    q = query.strip()
    if not q or set(q) <= {"%", "*"}:
        raise HTTPException(status_code=400, detail="Query too broad")
    like = f"%{q}%"

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pr.name AS name
            FROM products pr
            WHERE pr.name !~ '^[0-9]+$'
              AND EXISTS (SELECT 1 FROM prices p WHERE p.product_id = pr.id)
              AND LOWER(pr.name) LIKE LOWER($1)
            GROUP BY pr.name
            ORDER BY lower(pr.name)
            LIMIT 10
            """,
            like,
        )

    return [{"name": r["name"], "image": None} for r in rows]


# ----------------------------- AUTOCOMPLETE (pg_trgm when available) -----------------------------
@router.get("/products/search")
@throttle(limit=60, window=60)
async def products_search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64, description="Search text"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Autocomplete based on products.name (and product_aliases.alias when present).
    Uses pg_trgm if available; otherwise falls back to LIKE.
    """
    term = q.strip()
    if not term:
        return []

    # pg_trgm + aliases
    SQL_TRGM_WITH_ALIASES = """
      SELECT p.id, p.name
      FROM products p
      LEFT JOIN product_aliases a ON a.product_id = p.id
      WHERE p.name !~ '^[0-9]+$'
        AND (
              p.name ILIKE $1 || '%'
           OR p.name % $1
           OR p.name ILIKE '%' || $1 || '%'
           OR a.alias ILIKE $1 || '%'
           OR a.alias % $1
           OR a.alias ILIKE '%' || $1 || '%'
        )
      GROUP BY p.id, p.name
      ORDER BY
        CASE WHEN p.name ILIKE $1 || '%' THEN 0 ELSE 1 END,
        similarity(p.name, $1) DESC,
        p.name ASC
      LIMIT $2
    """

    # pg_trgm without aliases
    SQL_TRGM_NO_ALIASES = """
      SELECT p.id, p.name
      FROM products p
      WHERE p.name !~ '^[0-9]+$'
        AND (
              p.name ILIKE $1 || '%'
           OR p.name % $1
           OR p.name ILIKE '%' || $1 || '%'
        )
      ORDER BY
        CASE WHEN p.name ILIKE $1 || '%' THEN 0 ELSE 1 END,
        similarity(p.name, $1) DESC,
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
                    WHERE name !~ '^[0-9]+$'
                      AND LOWER(name) LIKE LOWER($1)
                    ORDER BY name
                    LIMIT $2
                    """,
                    f"%{term}%",
                    limit,
                )

    return [{"id": r["id"], "name": r["name"]} for r in rows]

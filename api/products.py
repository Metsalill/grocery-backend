# api/products.py

from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional, List, Dict, Any

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side hard cap


def _row_to_safe_product(row: Dict[str, Any]) -> Dict[str, Any]:
    chains = row.get("available_chains") or []
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "image_url": row.get("image_url"),
        "brand": row.get("brand"),
        "manufacturer": row.get("manufacturer"),
        "size_text": row.get("size_text"),
        "amount": row.get("amount"),
        "food_group": row.get("food_group"),
        "sub_code": row.get("sub_code"),
        "available_chains": sorted(list(set(chains))) if chains else [],
    }


async def _get_pool(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return pool


# Source priority: lower = preferred display representative
SOURCE_PRIORITY_SQL = """
    CASE
        WHEN p.source_url ILIKE '%prisma%'                             THEN 1
        WHEN p.source_url ILIKE '%selver%'                             THEN 2
        WHEN p.source_url ILIKE '%rimi%'                               THEN 3
        WHEN p.source_url ILIKE '%barbora%'
          OR p.source_url ILIKE '%maxima%'                             THEN 4
        WHEN p.source_url ILIKE '%ecoop%'
          OR (p.source_url ILIKE '%coop%'
              AND p.source_url NOT ILIKE '%wolt%')                     THEN 5
        WHEN p.source_url ILIKE '%wolt%'                               THEN 6
        WHEN p.source_url IS NULL OR p.source_url = ''                 THEN 7
        ELSE 8
    END
"""


def _build_dedup_sql(where_sql: str) -> str:
    """
    Returns one representative product per group.
    Grouped products (same real-world item across chains) collapse to one card.
    Ungrouped products each get their own card.
    Within a group, picks the best representative by chain priority, image, EAN, id.
    Also returns available_chains: all chains where any group member has a price.
    """
    return f"""
        SELECT DISTINCT ON (COALESCE(pgm.group_id::text, 'u_' || p.id::text))
            p.*,
            pgm.group_id,
            (
                SELECT ARRAY_AGG(DISTINCT s.chain ORDER BY s.chain)
                FROM product_group_members pgm2
                JOIN prices pr ON pr.product_id = pgm2.product_id
                JOIN stores s ON s.id = pr.store_id
                WHERE pgm2.group_id = pgm.group_id
                  AND s.chain IS NOT NULL
                  AND s.chain != ''
            ) AS available_chains
        FROM products p
        LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
        {where_sql}
        ORDER BY
            COALESCE(pgm.group_id::text, 'u_' || p.id::text),
            {SOURCE_PRIORITY_SQL},
            CASE WHEN p.image_url IS NOT NULL AND p.image_url != '' THEN 0 ELSE 1 END,
            CASE WHEN p.ean      IS NOT NULL AND p.ean      != '' THEN 0 ELSE 1 END,
            p.id
    """


# ----------------------------- LIST (paged) -----------------------------
@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query("", description="Search by product name (ILIKE)."),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    main_code: Optional[str] = Query(None, description="Main category code (e.g. 'produce')."),
    sub_code: Optional[str] = Query(None, description="Subcategory code (e.g. 'produce_apples_pears')."),
) -> Dict[str, Any]:
    limit = min(limit, MAX_LIMIT)
    q = (q or "").strip()
    main_code = (main_code or "").strip() or None
    sub_code = (sub_code or "").strip() or None

    pool = await _get_pool(request)

    params: List[Any] = []
    where: List[str] = []

    if sub_code:
        params.append(sub_code)
        where.append(f"p.sub_code = ${len(params)}")
    elif main_code:
        params.append(main_code)
        where.append(f"p.food_group = ${len(params)}")

    if q:
        params.append(f"%{q}%")
        where.append(f"p.name ILIKE ${len(params)}")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    dedup_sql = _build_dedup_sql(where_sql)
    count_sql = f"SELECT COUNT(*) FROM ({dedup_sql}) AS deduped"
    params_with_paging = params + [limit, offset]
    data_sql = (
        f"SELECT * FROM ({dedup_sql}) AS deduped\n"
        f"ORDER BY name\n"
        f"LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
    )

    try:
        async with pool.acquire() as conn:
            total_row = await conn.fetchrow(count_sql, *params)
            total = total_row[0] if total_row else 0
            rows = await conn.fetch(data_sql, *params_with_paging)

        items = [_row_to_safe_product(dict(r)) for r in rows]

        return {
            "items": items,
            "total": total,
            "offset": offset,
            "limit": limit,
            "count": len(items),
            "filters": {
                "q": q or None,
                "main_code": main_code,
                "sub_code": sub_code,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List products error: {e}")


# ----------------------------- SEARCH (lightweight) -----------------------------
@router.get("/products/search")
@throttle(limit=180, window=60)
async def search_products(
    request: Request,
    q: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(10, ge=1, le=50),
) -> Dict[str, Any]:
    limit = min(limit, 50)
    q = q.strip()

    pool = await _get_pool(request)

    where_sql = "WHERE p.name ILIKE $1"
    dedup_sql = _build_dedup_sql(where_sql)
    sql = f"""
        SELECT * FROM ({dedup_sql}) AS deduped
        ORDER BY name
        LIMIT $2
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, f"%{q}%", limit)

        items = [_row_to_safe_product(dict(r)) for r in rows]

        return {
            "items": items,
            "count": len(items),
            "q": q,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search products error: {e}")


# ----------------------------- PRODUCT DETAIL -----------------------------
@router.get("/products/{product_id}")
@throttle(limit=120, window=60)
async def get_product(
    request: Request,
    product_id: int,
) -> Dict[str, Any]:
    pool = await _get_pool(request)

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT p.* FROM products p WHERE p.id = $1 LIMIT 1",
                product_id,
            )

        if not row:
            raise HTTPException(status_code=404, detail="Product not found")

        return _row_to_safe_product(dict(row))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get product error: {e}")

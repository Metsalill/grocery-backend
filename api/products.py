# api/products.py

from fastapi import APIRouter, Request, Query, HTTPException, Header
from typing import Optional, List, Dict, Any

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side hard cap

PRICE_FRESHNESS_FILTER = "EXISTS (SELECT 1 FROM prices pr WHERE pr.product_id = p.id AND pr.collected_at > NOW() - INTERVAL '14 days')"


def _row_to_safe_product(row: Dict[str, Any]) -> Dict[str, Any]:
    chains = row.get("available_chains") or []
    size_text = (row.get("size_text") or "").strip()
    is_per_kg = size_text.lower() == "kg"
    min_price = row.get("min_price")
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "image_url": row.get("image_url"),
        "brand": row.get("brand"),
        "manufacturer": row.get("manufacturer"),
        "size_text": size_text,
        "amount": row.get("amount"),
        "food_group": row.get("food_group"),
        "sub_code": row.get("sub_code"),
        "available_chains": sorted(list(set(chains))) if chains else [],
        "is_per_kg": is_per_kg,
        "min_price": float(min_price) if min_price is not None else None,
    }


async def _get_pool(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return pool


async def _get_user_id_from_token(conn, authorization: Optional[str]) -> Optional[int]:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    try:
        import os
        from jose import jwt, JWTError
        token = authorization.split(" ")[1]
        SECRET_KEY = os.getenv("JWT_SECRET", "super-secret-key")
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        email = (payload.get("sub") or "").lower()
        if not email:
            return None
        row = await conn.fetchrow(
            "SELECT id FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
            email,
        )
        return row["id"] if row else None
    except Exception:
        return None


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
    if where_sql.strip().upper().startswith("WHERE"):
        combined_where = f"{where_sql} AND {PRICE_FRESHNESS_FILTER}"
    else:
        combined_where = f"WHERE {PRICE_FRESHNESS_FILTER}"

    return f"""
        WITH base AS (
            SELECT DISTINCT ON (COALESCE(pgm.group_id::text, 'u_' || p.id::text))
                p.*,
                pgm.group_id,
                COALESCE(pgm.group_id::text, 'u_' || p.id::text) AS dedup_key
            FROM products p
            LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
            {combined_where}
            ORDER BY
                COALESCE(pgm.group_id::text, 'u_' || p.id::text),
                {SOURCE_PRIORITY_SQL},
                CASE WHEN p.image_url IS NOT NULL AND p.image_url != '' THEN 0 ELSE 1 END,
                CASE WHEN p.ean      IS NOT NULL AND p.ean      != '' THEN 0 ELSE 1 END,
                p.id
        )
        SELECT b.*, gc.chains AS available_chains, gc.min_price
        FROM base b
        LEFT JOIN mv_group_chains gc ON gc.dedup_key = b.dedup_key
    """


def _build_personalized_sql(where_sql: str, user_id: int) -> str:
    if where_sql.strip().upper().startswith("WHERE"):
        combined_where = f"{where_sql} AND {PRICE_FRESHNESS_FILTER}"
    else:
        combined_where = f"WHERE {PRICE_FRESHNESS_FILTER}"

    return f"""
        WITH base AS (
            SELECT DISTINCT ON (COALESCE(pgm.group_id::text, 'u_' || p.id::text))
                p.*,
                pgm.group_id,
                COALESCE(pgm.group_id::text, 'u_' || p.id::text) AS dedup_key,
                COALESCE(ups.count, 0) AS selection_count
            FROM products p
            LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
            LEFT JOIN user_product_selections ups
                ON ups.product_id = p.id AND ups.user_id = {user_id}
            {combined_where}
            ORDER BY
                COALESCE(pgm.group_id::text, 'u_' || p.id::text),
                {SOURCE_PRIORITY_SQL},
                CASE WHEN p.image_url IS NOT NULL AND p.image_url != '' THEN 0 ELSE 1 END,
                CASE WHEN p.ean      IS NOT NULL AND p.ean      != '' THEN 0 ELSE 1 END,
                p.id
        )
        SELECT b.*, gc.chains AS available_chains, gc.min_price
        FROM base b
        LEFT JOIN mv_group_chains gc ON gc.dedup_key = b.dedup_key
    """


@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query("", description="Search by product name (ILIKE)."),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    main_code: Optional[str] = Query(None),
    sub_code: Optional[str] = Query(None),
    authorization: Optional[str] = Header(None),
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

    try:
        async with pool.acquire() as conn:
            user_id = await _get_user_id_from_token(conn, authorization)

            if user_id:
                dedup_sql = _build_personalized_sql(where_sql, user_id)
                order_clause = "ORDER BY selection_count DESC, name"
            else:
                dedup_sql = _build_dedup_sql(where_sql)
                order_clause = "ORDER BY name"

            params_with_paging = params + [limit, offset]
            data_sql = (
                f"SELECT * FROM ({dedup_sql}) AS deduped\n"
                f"{order_clause}\n"
                f"LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
            )

            rows = await conn.fetch(data_sql, *params_with_paging)

        items = [_row_to_safe_product(dict(r)) for r in rows]

        has_more = len(items) == limit
        estimated_total = offset + len(items) + (1 if has_more else 0)

        return {
            "items": items,
            "total": estimated_total,
            "offset": offset,
            "limit": limit,
            "count": len(items),
            "has_more": has_more,
            "filters": {
                "q": q or None,
                "main_code": main_code,
                "sub_code": sub_code,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List products error: {e}")


@router.get("/products/search")
@throttle(limit=180, window=60)
async def search_products(
    request: Request,
    q: str = Query(..., min_length=1),
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

        return {"items": items, "count": len(items), "q": q}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search products error: {e}")


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

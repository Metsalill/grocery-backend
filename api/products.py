# api/products.py

import re
from fastapi import APIRouter, Request, Query, HTTPException, Header
from typing import Optional, List, Dict, Any

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side hard cap

PRICE_FRESHNESS_FILTER = "EXISTS (SELECT 1 FROM prices pr WHERE pr.product_id = p.id AND pr.collected_at > NOW() - INTERVAL '14 days')"

# Tuvastab mahu nimes -- nt "500ml", "0.5L", "75cl", "1.5 l", "6x568ml", "24x330ml"
_SIZE_IN_NAME_RE = re.compile(
    r'\b\d+(?:[.,]\d+)?\s*(?:ml|cl|dl|l|g|kg)\b'
    r'|\b\d+\s*[x*]\s*\d+(?:[.,]\d+)?\s*(?:ml|cl|dl|l|g|kg)\b',
    re.I
)


def _row_to_safe_product(row: Dict[str, Any]) -> Dict[str, Any]:
    chains = row.get("available_chains") or []
    size_text = (row.get("size_text") or "").strip()
    is_per_kg = size_text.lower() == "kg"
    min_price = row.get("min_price")

    canonical = (row.get("canonical_name") or "").strip()
    name = canonical if canonical else (row.get("name") or "")

    if not is_per_kg and size_text and _SIZE_IN_NAME_RE.search(name):
        size_text = ""

    group_brand = (row.get("group_brand") or "").strip()
    product_brand = (row.get("brand") or "").strip()
    display_brand = group_brand if group_brand else product_brand

    return {
        "id": row.get("id"),
        "group_id": row.get("group_id"),
        "name": name,
        "image_url": row.get("image_url"),
        "brand": display_brand,
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
        from jose import jwt
        token = authorization.split(" ")[1]
        SECRET_KEY = os.getenv("JWT_SECRET")
        if not SECRET_KEY:
            return None
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
            LEFT JOIN product_groups pg ON pg.id = pgm.group_id
            {combined_where}
            ORDER BY
                COALESCE(pgm.group_id::text, 'u_' || p.id::text),
                {SOURCE_PRIORITY_SQL},
                CASE WHEN p.image_url IS NOT NULL AND p.image_url != '' THEN 0 ELSE 1 END,
                CASE WHEN p.ean      IS NOT NULL AND p.ean      != '' THEN 0 ELSE 1 END,
                p.id
        )
        SELECT b.*, gc.chains AS available_chains, gc.min_price,
               pg.canonical_name, pg.brand AS group_brand
        FROM base b
        LEFT JOIN mv_group_chains gc ON gc.dedup_key = b.dedup_key
        LEFT JOIN product_groups pg ON pg.id = b.group_id
    """


def _build_personalized_sql(where_sql: str, user_param_index: int) -> str:
    if where_sql.strip().upper().startswith("WHERE"):
        combined_where = f"{where_sql} AND {PRICE_FRESHNESS_FILTER}"
    else:
        combined_where = f"WHERE {PRICE_FRESHNESS_FILTER}"

    return f"""
        WITH selection_totals AS (
            SELECT
                COALESCE(pgm.group_id::text, 'u_' || ups.product_id::text) AS dedup_key,
                SUM(ups.count) AS selection_count
            FROM user_product_selections ups
            LEFT JOIN product_group_members pgm ON pgm.product_id = ups.product_id
            WHERE ups.user_id = ${user_param_index}
            GROUP BY COALESCE(pgm.group_id::text, 'u_' || ups.product_id::text)
        ),
        base AS (
            SELECT DISTINCT ON (COALESCE(pgm.group_id::text, 'u_' || p.id::text))
                p.*,
                pgm.group_id,
                COALESCE(pgm.group_id::text, 'u_' || p.id::text) AS dedup_key
            FROM products p
            LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
            LEFT JOIN product_groups pg ON pg.id = pgm.group_id
            {combined_where}
            ORDER BY
                COALESCE(pgm.group_id::text, 'u_' || p.id::text),
                {SOURCE_PRIORITY_SQL},
                CASE WHEN p.image_url IS NOT NULL AND p.image_url != '' THEN 0 ELSE 1 END,
                CASE WHEN p.ean      IS NOT NULL AND p.ean      != '' THEN 0 ELSE 1 END,
                p.id
        )
        SELECT b.*, COALESCE(st.selection_count, 0) AS selection_count,
               gc.chains AS available_chains, gc.min_price,
               pg.canonical_name, pg.brand AS group_brand
        FROM base b
        LEFT JOIN selection_totals st ON st.dedup_key = b.dedup_key
        LEFT JOIN mv_group_chains gc ON gc.dedup_key = b.dedup_key
        LEFT JOIN product_groups pg ON pg.id = b.group_id
    """


@router.get("/products/alternatives")
@throttle(limit=300, window=60)
async def get_alternatives(
    request: Request,
    product_name: str = Query(..., min_length=1),
    store_id: int = Query(...),
    limit: int = Query(6, ge=1, le=20),
) -> Dict[str, Any]:
    """
    Leiab sama poe sarnased tooted puuduva toote asendamiseks.
    Otsib sub_code jargi samast poest, sorteerib hinna jargi.
    """
    pool = await _get_pool(request)
    product_name = product_name.strip()

    try:
        async with pool.acquire() as conn:
            # 1. Leia puuduva toote sub_code nime jargi
            sub_code_row = await conn.fetchrow("""
                SELECT p.sub_code
                FROM products p
                WHERE p.name ILIKE $1
                  AND p.sub_code IS NOT NULL
                  AND p.sub_code != ''
                ORDER BY p.id
                LIMIT 1
            """, f"%{product_name}%")

            if not sub_code_row:
                return {"items": [], "sub_code": None, "store_id": store_id}

            sub_code = sub_code_row["sub_code"]

            # 2. Leia selle poe tooted samast sub_code-st
            rows = await conn.fetch("""
                SELECT DISTINCT ON (COALESCE(pgm.group_id::text, 'u_' || p.id::text))
                    p.id,
                    p.name,
                    p.brand,
                    p.size_text,
                    p.image_url,
                    p.sub_code,
                    pgm.group_id,
                    pg.canonical_name,
                    pg.brand AS group_brand,
                    pr.price
                FROM products p
                JOIN prices pr ON pr.product_id = p.id
                LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
                LEFT JOIN product_groups pg ON pg.id = pgm.group_id
                WHERE p.sub_code = $1
                  AND pr.store_id = $2
                  AND pr.price > 0
                  AND pr.collected_at > NOW() - INTERVAL '14 days'
                ORDER BY
                    COALESCE(pgm.group_id::text, 'u_' || p.id::text),
                    pr.price ASC
                LIMIT $3
            """, sub_code, store_id, limit)

        items = []
        for r in rows:
            size_text = (r["size_text"] or "").strip()
            is_per_kg = size_text.lower() == "kg"
            canonical = (r["canonical_name"] or "").strip()
            name = canonical if canonical else (r["name"] or "")
            group_brand = (r["group_brand"] or "").strip()
            product_brand = (r["brand"] or "").strip()
            display_brand = group_brand if group_brand else product_brand

            items.append({
                "id": r["id"],
                "group_id": r["group_id"],
                "name": name,
                "brand": display_brand,
                "size_text": size_text,
                "image_url": r["image_url"],
                "price": float(r["price"]),
                "is_per_kg": is_per_kg,
                "sub_code": r["sub_code"],
            })

        return {
            "items": items,
            "sub_code": sub_code,
            "store_id": store_id,
            "product_name": product_name,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Alternatives error: {e}")


@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query("", description="Search by product name (ILIKE)."),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    main_code: Optional[str] = Query(None),
    sub_code: Optional[str] = Query(None),
    sort: Optional[str] = Query(None, description="Sort order: price_asc | price_desc"),
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    limit = min(limit, MAX_LIMIT)
    q = (q or "").strip()
    main_code = (main_code or "").strip() or None
    sub_code = (sub_code or "").strip() or None
    sort = (sort or "").strip().lower() or None

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
        where.append(
            f"(p.name ILIKE ${len(params)} OR pg.canonical_name ILIKE ${len(params)}"
            f" OR p.brand ILIKE ${len(params)} OR pg.brand ILIKE ${len(params)})"
        )

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Sorteerimise ORDER BY klausel
    if sort == "price_asc":
        price_order_clause = "ORDER BY min_price ASC NULLS LAST, COALESCE(NULLIF(canonical_name, ''), name), id"
    elif sort == "price_desc":
        price_order_clause = "ORDER BY min_price DESC NULLS LAST, COALESCE(NULLIF(canonical_name, ''), name), id"
    else:
        price_order_clause = None  # kasutame allpool vaikimisi järjestust

    try:
        async with pool.acquire() as conn:
            user_id = await _get_user_id_from_token(conn, authorization)

            if user_id and not sort:
                # Personaliseeritud järjestus ainult siis kui sort pole määratud
                user_param_index = len(params) + 1
                params_for_query = params + [user_id]
                dedup_sql = _build_personalized_sql(where_sql, user_param_index)
                order_clause = "ORDER BY selection_count DESC, COALESCE(NULLIF(canonical_name, ''), name), id"
            elif price_order_clause:
                params_for_query = params
                dedup_sql = _build_dedup_sql(where_sql)
                order_clause = price_order_clause
            else:
                params_for_query = params
                dedup_sql = _build_dedup_sql(where_sql)
                order_clause = "ORDER BY COALESCE(NULLIF(canonical_name, ''), name), id"

            fetch_limit = limit + 1
            params_with_paging = params_for_query + [fetch_limit, offset]
            limit_param = len(params_for_query) + 1
            offset_param = len(params_for_query) + 2

            data_sql = (
                f"SELECT * FROM ({dedup_sql}) AS deduped\n"
                f"{order_clause}\n"
                f"LIMIT ${limit_param} OFFSET ${offset_param}"
            )

            rows = await conn.fetch(data_sql, *params_with_paging)

        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [_row_to_safe_product(dict(r)) for r in rows]

        return {
            "items": items,
            "offset": offset,
            "limit": limit,
            "count": len(items),
            "has_more": has_more,
            "next_offset": offset + len(items) if has_more else None,
            "filters": {
                "q": q or None,
                "main_code": main_code,
                "sub_code": sub_code,
                "sort": sort,
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
    sub_code: Optional[str] = Query(None),
) -> Dict[str, Any]:
    limit = min(limit, 50)
    q = q.strip()

    if not q:
        raise HTTPException(status_code=422, detail="Search query cannot be empty")

    pool = await _get_pool(request)

    params: List[Any] = [f"%{q}%"]
    where_parts = ["""(
        p.name ILIKE $1
        OR pg.canonical_name ILIKE $1
        OR p.brand ILIKE $1
        OR pg.brand ILIKE $1
    )"""]

    if sub_code:
        params.append(sub_code.strip())
        where_parts.append(f"p.sub_code = ${len(params)}")

    where_sql = "WHERE " + " AND ".join(where_parts)
    dedup_sql = _build_dedup_sql(where_sql)

    params.append(limit)
    limit_param = len(params)

    sql = f"""
        SELECT * FROM ({dedup_sql}) AS deduped
        ORDER BY COALESCE(NULLIF(canonical_name, ''), name), id
        LIMIT ${limit_param}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

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
            row = await conn.fetchrow("""
                SELECT
                    p.*,
                    pgm.group_id,
                    gc.chains AS available_chains,
                    gc.min_price,
                    pg.canonical_name,
                    pg.brand AS group_brand
                FROM products p
                LEFT JOIN product_group_members pgm ON pgm.product_id = p.id
                LEFT JOIN mv_group_chains gc
                    ON gc.dedup_key = COALESCE(pgm.group_id::text, 'u_' || p.id::text)
                LEFT JOIN product_groups pg ON pg.id = pgm.group_id
                WHERE p.id = $1
                LIMIT 1
            """, product_id)

        if not row:
            raise HTTPException(status_code=404, detail="Product not found")

        return _row_to_safe_product(dict(row))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get product error: {e}")

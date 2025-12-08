# api/products.py

from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional, List, Dict, Any
from asyncpg import exceptions as pgerr

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side hard cap


def _row_to_safe_product(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a DB row into a stable API shape.
    We intentionally keep this minimal for the product list UI.
    """
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        # Optional fields if present in your table/view
        "image_url": row.get("image_url"),
        "brand": row.get("brand"),
        "manufacturer": row.get("manufacturer"),
        "size_text": row.get("size_text"),
        "amount": row.get("amount"),
        # Keep legacy fields if your frontend still references them
        "food_group": row.get("food_group"),
        "sub_code": row.get("sub_code"),
    }


async def _get_pool(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return pool


# ----------------------------- LIST (paged) -----------------------------
@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query(
        "",
        description=(
            "Search by product name (ILIKE). "
            "Empty lists everything (paged)."
        ),
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    main_code: Optional[str] = Query(
        None,
        description=(
            "Optional main category code (e.g. 'produce', 'meat_fish'). "
            "When provided, uses product_categories mapping."
        ),
    ),
    sub_code: Optional[str] = Query(
        None,
        description=(
            "Optional subcategory code (e.g. 'produce_apples_pears'). "
            "When provided, uses product_categories mapping."
        ),
    ),
) -> Dict[str, Any]:
    limit = min(limit, MAX_LIMIT)

    q = (q or "").strip()
    main_code = (main_code or "").strip() or None
    sub_code = (sub_code or "").strip() or None

    pool = await _get_pool(request)

    # Build SQL dynamically but safely via positional params
    params: List[Any] = []
    where: List[str] = []

    # Category-filtered path (uses mapping tables)
    if main_code or sub_code:
        sql = """
            SELECT p.*
            FROM products p
            JOIN product_categories pc ON pc.product_id = p.id
            JOIN categories_main m ON m.id = pc.main_id
            JOIN categories_sub  s ON s.id = pc.sub_id
        """

        if main_code:
            params.append(main_code)
            where.append(f"m.code = ${len(params)}")

        if sub_code:
            params.append(sub_code)
            where.append(f"s.code = ${len(params)}")

    # Non-category path
    else:
        sql = """
            SELECT p.*
            FROM products p
        """

    # Search filter (applies in both paths)
    if q:
        params.append(f"%{q}%")
        where.append(f"p.name ILIKE ${len(params)}")

    if where:
        sql += "\nWHERE " + " AND ".join(where)

    # Stable ordering
    sql += "\nORDER BY p.name"

    # Pagination
    params.append(limit)
    params.append(offset)
    sql += f"\nLIMIT ${len(params)-1} OFFSET ${len(params)}"

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        items: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            items.append(_row_to_safe_product(d))

        return {
            "items": items,
            "offset": offset,
            "limit": limit,
            "count": len(items),
            "filters": {
                "q": q or None,
                "main_code": main_code,
                "sub_code": sub_code,
            },
        }

    except pgerr.UndefinedTableError:
        raise HTTPException(
            status_code=500,
            detail="Missing required tables for products/categories mapping",
        )
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

    sql = """
        SELECT p.*
        FROM products p
        WHERE p.name ILIKE $1
        ORDER BY p.name
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


# ----------------------------- PRODUCT DETAIL (optional but useful) -----------------------------
@router.get("/products/{product_id}")
@throttle(limit=120, window=60)
async def get_product(
    request: Request,
    product_id: int,
) -> Dict[str, Any]:
    pool = await _get_pool(request)

    sql = """
        SELECT p.*
        FROM products p
        WHERE p.id = $1
        LIMIT 1
    """

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(sql, product_id)

        if not row:
            raise HTTPException(status_code=404, detail="Product not found")

        d = dict(row)

        # Return more fields for detail view
        return {
            **_row_to_safe_product(d),
            "raw": d,  # keeps debugging easy; remove later if you want a strict schema
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Get product error: {e}")

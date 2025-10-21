from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional, List, Dict, Any
from asyncpg import exceptions as pgerr

from utils.throttle import throttle

router = APIRouter()

MAX_LIMIT = 50  # server-side cap


def _fmt(price) -> Optional[float]:
    return None if price is None else round(float(price), 2)


# ----------------------------- LIST (paged) -----------------------------
@router.get("/products")
@throttle(limit=120, window=60)
async def list_products(
    request: Request,
    q: Optional[str] = Query(
        "",
        description="Search by product name; also matches aliases when available. "
                    "Digits-only text is treated as EAN prefix."
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
):
    """
    Returns one row per product (name + brand + size_text), aggregated over *existing* prices.
    The query path automatically degrades if some tables/extensions/columns are missing.
    """
    limit = min(int(limit), MAX_LIMIT)
    term = (q or "").strip()
    like = f"%{term}%" if term else "%"
    is_ean = term.isdigit() and 8 <= len(term) <= 14
    # normalize EAN in SQL using regexp_replace(p.ean, '\D','','g')
    # We will pass ean_prefix = term, but only use it when is_ean is True
    ean_prefix = term if is_ean else None

    async def _run(conn) -> Dict[str, Any]:
        """
        Try a sequence of queries from most feature-rich -> most basic.
        We return {"total": int, "rows": list[Record]} when one succeeds.
        """

        # 1) With product_aliases + prices.image_url present
        SQL_COUNT_WITH_ALIASES = """
          WITH base AS (
            SELECT
              pr.name,
              COALESCE(pr.brand,'')       AS brand,
              COALESCE(pr.size_text,'')   AS size_text
            FROM prices p
            JOIN products pr ON pr.id = p.product_id
            WHERE
              (LOWER(pr.name) LIKE LOWER($1)
               OR EXISTS (
                    SELECT 1 FROM product_aliases a
                    WHERE a.product_id = pr.id AND LOWER(a.alias) LIKE LOWER($1)
               )
              )
              OR (
                    $4::boolean
                AND regexp_replace(COALESCE(pr.ean,''), '\\D', '', 'g') LIKE $2
              )
            GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
          )
          SELECT COUNT(*) FROM base;
        """
        SQL_PAGE_WITH_ALIASES = """
          WITH grouped AS (
            SELECT
              pr.name                                 AS product,
              COALESCE(pr.brand,'')                   AS brand,
              COALESCE(pr.size_text,'')               AS size_text,
              MIN(p.price)                            AS min_price,
              MAX(p.price)                            AS max_price,
              COUNT(DISTINCT p.store_id)              AS store_count,
              (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
            FROM prices p
            JOIN products pr ON pr.id = p.product_id
            WHERE
              (LOWER(pr.name) LIKE LOWER($1)
               OR EXISTS (
                    SELECT 1 FROM product_aliases a
                    WHERE a.product_id = pr.id AND LOWER(a.alias) LIKE LOWER($1)
               )
              )
              OR (
                    $4::boolean
                AND regexp_replace(COALESCE(pr.ean,''), '\\D', '', 'g') LIKE $2
              )
            GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
          )
          SELECT *
          FROM grouped
          ORDER BY lower(product), lower(brand), lower(size_text)
          OFFSET $3
          LIMIT  $5
        """

        # 2) With product_aliases but without prices.image_url (UndefinedColumn)
        SQL_PAGE_WITH_ALIASES_NO_IMG = SQL_PAGE_WITH_ALIASES.replace(
            "(ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url",
            "NULL::text AS image_url"
        )

        # 3) No product_aliases, still try image
        SQL_COUNT_NO_ALIASES = """
          WITH base AS (
            SELECT
              pr.name,
              COALESCE(pr.brand,'')       AS brand,
              COALESCE(pr.size_text,'')   AS size_text
            FROM prices p
            JOIN products pr ON pr.id = p.product_id
            WHERE
              LOWER(pr.name) LIKE LOWER($1)
              OR (
                    $4::boolean
                AND regexp_replace(COALESCE(pr.ean,''), '\\D', '', 'g') LIKE $2
              )
            GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
          )
          SELECT COUNT(*) FROM base;
        """
        SQL_PAGE_NO_ALIASES = """
          WITH grouped AS (
            SELECT
              pr.name                                 AS product,
              COALESCE(pr.brand,'')                   AS brand,
              COALESCE(pr.size_text,'')               AS size_text,
              MIN(p.price)                            AS min_price,
              MAX(p.price)                            AS max_price,
              COUNT(DISTINCT p.store_id)              AS store_count,
              (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
            FROM prices p
            JOIN products pr ON pr.id = p.product_id
            WHERE
              LOWER(pr.name) LIKE LOWER($1)
              OR (
                    $4::boolean
                AND regexp_replace(COALESCE(pr.ean,''), '\\D', '', 'g') LIKE $2
              )
            GROUP BY pr.name, COALESCE(pr.brand,''), COALESCE(pr.size_text,'')
          )
          SELECT *
          FROM grouped
          ORDER BY lower(product), lower(brand), lower(size_text)
          OFFSET $3
          LIMIT  $5
        """

        # 4) No aliases and no image column
        SQL_PAGE_NO_ALIASES_NO_IMG = SQL_PAGE_NO_ALIASES.replace(
            "(ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url",
            "NULL::text AS image_url"
        )

        params = (like, f"{ean_prefix or ''}%", offset, bool(is_ean), limit)

        # Try chain
        try:
            total = await conn.fetchval(SQL_COUNT_WITH_ALIASES, *params) or 0
            rows = await conn.fetch(SQL_PAGE_WITH_ALIASES, *params)
            return {"total": total, "rows": rows}
        except pgerr.UndefinedColumnError:
            # prices.image_url is missing – re-run with NULL image
            total = await conn.fetchval(SQL_COUNT_WITH_ALIASES, *params) or 0
            rows = await conn.fetch(SQL_PAGE_WITH_ALIASES_NO_IMG, *params)
            return {"total": total, "rows": rows}
        except pgerr.UndefinedTableError:
            # product_aliases missing – drop aliases
            try:
                total = await conn.fetchval(SQL_COUNT_NO_ALIASES, *params) or 0
                rows = await conn.fetch(SQL_PAGE_NO_ALIASES, *params)
                return {"total": total, "rows": rows}
            except pgerr.UndefinedColumnError:
                total = await conn.fetchval(SQL_COUNT_NO_ALIASES, *params) or 0
                rows = await conn.fetch(SQL_PAGE_NO_ALIASES_NO_IMG, *params)
                return {"total": total, "rows": rows}

        # If we get here, retry the no-alias path as a last resort
        try:
            total = await conn.fetchval(SQL_COUNT_NO_ALIASES, *params) or 0
            rows = await conn.fetch(SQL_PAGE_NO_ALIASES, *params)
            return {"total": total, "rows": rows}
        except pgerr.UndefinedColumnError:
            total = await conn.fetchval(SQL_COUNT_NO_ALIASES, *params) or 0
            rows = await conn.fetch(SQL_PAGE_NO_ALIASES_NO_IMG, *params)
            return {"total": total, "rows": rows}

    async with request.app.state.db.acquire() as conn:
        res = await _run(conn)

    items = [
        {
            "product": r["product"],
            "brand": r["brand"],
            "size_text": r["size_text"],
            "min_price": _fmt(r["min_price"]),
            "max_price": _fmt(r["max_price"]),
            "store_count": r["store_count"],
            "image_url": r["image_url"],
        }
        for r in res["rows"]
    ]
    return {"total": res["total"], "offset": offset, "limit": limit, "items": items}


# ----------------------------- LEGACY SUGGESTIONS (with image) -----------------------------
@router.get("/search-products")
@throttle(limit=30, window=60)
async def search_products_legacy(
    request: Request,
    query: str = Query(..., min_length=2),
):
    """
    Legacy suggestions for typeahead that also surface an example image (if any).
    Only hits products that have prices (so results are actionable).
    Falls back if prices.image_url is missing.
    """
    q = query.strip()
    if not q or set(q) <= {"%", "*"}:
        raise HTTPException(status_code=400, detail="Query too broad")
    like = f"%{q}%"

    SQL = """
        WITH base AS (
          SELECT
            pr.name AS name,
            (ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url
          FROM prices p
          JOIN products pr ON pr.id = p.product_id
          WHERE LOWER(pr.name) LIKE LOWER($1)
          GROUP BY pr.name
        )
        SELECT name, image_url
        FROM base
        ORDER BY lower(name)
        LIMIT 10
    """
    SQL_NO_IMG = SQL.replace(
        "(ARRAY_AGG(p.image_url ORDER BY (p.image_url IS NULL) ASC))[1] AS image_url",
        "NULL::text AS image_url"
    )

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(SQL, like)
        except pgerr.UndefinedColumnError:
            rows = await conn.fetch(SQL_NO_IMG, like)

    return [{"name": r["name"], "image": r["image_url"]} for r in rows]


# ----------------------------- NEW: TRIGRAM AUTOCOMPLETE -----------------------------
@router.get("/products/search")
@throttle(limit=60, window=60)
async def products_search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64, description="Search text"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Autocomplete based on products.name, with pg_trgm + prefix boost when available.
    Also matches product_aliases.alias when available.
    Falls back to simple LIKE if pg_trgm is missing.
    """
    term = q.strip()
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

    SQL_LIKE_ONLY = """
      SELECT id, name
      FROM products
      WHERE LOWER(name) LIKE LOWER($1)
      ORDER BY name
      LIMIT $2
    """

    async with request.app.state.db.acquire() as conn:
        try:
            rows = await conn.fetch(SQL_TRGM_WITH_ALIASES, term, limit)
        except (pgerr.UndefinedTableError, pgerr.UndefinedFunctionError):
            try:
                rows = await conn.fetch(SQL_TRGM_NO_ALIASES, term, limit)
            except pgerr.UndefinedFunctionError:
                rows = await conn.fetch(SQL_LIKE_ONLY, f"%{term}%", limit)

    return [{"id": r["id"], "name": r["name"]} for r in rows]

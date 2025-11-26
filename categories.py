# categories.py
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

router = APIRouter(prefix="/categories", tags=["categories"])
bearer_scheme = HTTPBearer(auto_error=False)


async def get_db(request: Request):
    conn = getattr(request.app.state, "db", None)
    if conn is None:
        raise HTTPException(status_code=500, detail="DB pool not available")
    return conn


# ─────────────────────────────────────────────────────────
# 1) Main categories
# ─────────────────────────────────────────────────────────
@router.get("/main")
async def list_main_categories(
    request: Request,
    db=Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    """
    Return all main categories with an approximate product_count.

    product_count is currently based on products.food_group so that
    you see real numbers even before product_categories is filled.
    """
    sql = """
        SELECT
            m.code,
            m.label_et,
            COALESCE(m.label_en, m.label_et) AS label_en,
            COUNT(DISTINCT p.id) AS product_count
        FROM categories_main AS m
        LEFT JOIN products AS p
          ON p.food_group = m.code
        GROUP BY
            m.id, m.code, m.label_et, m.label_en, m.sort_order
        ORDER BY m.sort_order, m.id;
    """

    rows = await db.fetch(sql)
    return [
        {
            "code": r["code"],
            "label": r["label_et"],
            "label_en": r["label_en"],
            "product_count": r["product_count"],
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────
# 2) Subcategories under a main category
# ─────────────────────────────────────────────────────────
@router.get("/{main_code}/sub")
async def list_subcategories(
    main_code: str,
    request: Request,
    db=Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    """
    Return subcategories for a given main category.

    Uses categories_sub + product_categories mapping.
    No more reference to products.food_subgroup.
    """
    # Make sure main category exists
    main_row = await db.fetchrow(
        "SELECT id, code, label_et FROM categories_main WHERE code = $1",
        main_code,
    )
    if not main_row:
        raise HTTPException(status_code=404, detail="Main category not found")

    sql = """
        SELECT
            s.code,
            s.label_et,
            COALESCE(s.label_en, s.label_et) AS label_en,
            COUNT(DISTINCT pc.product_id) AS product_count
        FROM categories_sub AS s
        JOIN categories_main AS m
          ON s.main_id = m.id
        LEFT JOIN product_categories AS pc
          ON pc.main_id = m.id
         AND pc.sub_id  = s.id
        WHERE m.code = $1
        GROUP BY
            s.id, s.code, s.label_et, s.label_en, s.sort_order
        ORDER BY s.sort_order, s.id;
    """

    rows = await db.fetch(sql, main_code)

    return [
        {
            "code": r["code"],
            "label": r["label_et"],
            "label_en": r["label_en"],
            "product_count": r["product_count"],
        }
        for r in rows
    ]

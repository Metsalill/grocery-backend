# categories.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
import asyncpg

from settings import get_db_pool

router = APIRouter(prefix="/categories", tags=["categories"])


class CategoryOut(BaseModel):
    code: str
    label: str
    product_count: int


class SubcategoryOut(BaseModel):
    code: str
    label: str
    product_count: int


@router.get("/main", response_model=List[CategoryOut])
async def list_main_categories(
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    """
    Main categories from products.food_group.
    Replace `food_group` with your actual column name if different.
    """
    sql = """
        SELECT
            food_group AS label,
            lower(regexp_replace(food_group, '\W+', '_', 'g')) AS code,
            COUNT(*)::int AS product_count
        FROM products
        WHERE food_group IS NOT NULL
          AND food_group <> ''
        GROUP BY food_group
        ORDER BY food_group;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)

    return [
        CategoryOut(
            code=row["code"],
            label=row["label"],
            product_count=row["product_count"],
        )
        for row in rows
    ]


@router.get("/{main_code}/sub", response_model=List[SubcategoryOut])
async def list_subcategories(
    main_code: str,
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    """
    Sub-categories from products.food_subgroup under a given food_group.

    ⚠️ If your schema uses another column name for the sub-level
       (e.g. category_l2), replace `food_subgroup` accordingly.
    """
    sql = """
        WITH main_groups AS (
            SELECT
                food_group,
                lower(regexp_replace(food_group, '\W+', '_', 'g')) AS main_code
            FROM products
            WHERE food_group IS NOT NULL
              AND food_group <> ''
            GROUP BY food_group
        )
        SELECT
            food_subgroup AS label,
            lower(regexp_replace(food_subgroup, '\W+', '_', 'g')) AS code,
            COUNT(*)::int AS product_count
        FROM products p
        JOIN main_groups g ON p.food_group = g.food_group
        WHERE g.main_code = $1
          AND food_subgroup IS NOT NULL
          AND food_subgroup <> ''
        GROUP BY food_subgroup
        ORDER BY food_subgroup;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, main_code)

    return [
        SubcategoryOut(
            code=row["code"],
            label=row["label"],
            product_count=row["product_count"],
        )
        for row in rows
    ]

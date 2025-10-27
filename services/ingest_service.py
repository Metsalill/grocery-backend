import re
import asyncpg
from typing import Optional, Any


def normalize_ean(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D+", "", raw)  # keep only 0-9
    return digits or None


def normalize_string(s: Optional[str]) -> str:
    return (s or "").strip()


async def upsert_product_and_price(
    pool: Any,
    *,
    raw_ean: Optional[str],
    raw_name: str,
    raw_size_text: Optional[str],
    raw_brand: Optional[str],
    price: float,
    store_id: int,
) -> int:
    """
    1. Find or create a canonical product row in `products`.
    2. Insert a price snapshot row in `prices`.

    Returns the canonical product_id we used.
    """

    ean = normalize_ean(raw_ean)
    name = normalize_string(raw_name)
    size_text = normalize_string(raw_size_text)
    brand = normalize_string(raw_brand)

    async with pool.acquire() as conn:
        product_id = None

        # STEP A: try to reuse an existing product via EAN
        if ean:
            row = await conn.fetchrow(
                """
                SELECT id
                FROM products
                WHERE ean = $1
                LIMIT 1
                """,
                ean,
            )
            if row:
                product_id = row["id"]

        # STEP B: no match found -> insert a new product row
        if product_id is None:
            # NOTE: this INSERT will fail with unique_violation if we race
            # and someone else just inserted the same EAN in parallel.
            # We catch and re-select in that case.
            try:
                product_id = await conn.fetchval(
                    """
                    INSERT INTO products (ean, name, size_text, brand)
                    VALUES ($1, $2, $3, $4)
                    RETURNING id
                    """,
                    ean,
                    name,
                    size_text,
                    brand,
                )
            except asyncpg.UniqueViolationError:
                # EAN got inserted by another task after our first SELECT.
                # Just re-select.
                if ean:
                    row2 = await conn.fetchrow(
                        """
                        SELECT id
                        FROM products
                        WHERE ean = $1
                        LIMIT 1
                        """,
                        ean,
                    )
                    if row2:
                        product_id = row2["id"]
                if product_id is None:
                    raise  # weird edge case: no ean or still not found

        # STEP C: insert today's price snapshot for this store+product
        await conn.execute(
            """
            INSERT INTO prices (store_id, product_id, price, collected_at)
            VALUES ($1, $2, $3, NOW())
            """,
            store_id,
            product_id,
            price,
        )

        return int(product_id)

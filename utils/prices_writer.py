# utils/prices_writer.py
from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras

def write_price(
    conn,
    *,
    product_id: int,
    store_id: Optional[int],
    amount: float,
    currency: str = "EUR",
    seen_at: Optional[datetime] = None,
    price_type: Optional[str] = None,
    source_url: Optional[str] = None,
) -> None:
    """
    - Appends to price_history (append-only)
    - Upserts canonical prices row (one row per product_id)
      Rule: prefer newer seen_at; on same timestamp, prefer cheaper price.
    """
    if seen_at is None:
        seen_at = datetime.now(timezone.utc)

    with conn:  # transaction
        with conn.cursor() as cur:
            # 1) append-only history
            cur.execute(
                """
                INSERT INTO price_history
                    (product_id, amount, currency, captured_at, store_id, price_type, source_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (product_id, amount, currency, seen_at, store_id, price_type, source_url),
            )

            # 2) canonical row in prices (unique on product_id)
            #    keep newest; on tie, keep cheaper
            cur.execute(
                """
                INSERT INTO prices (product_id, store_id, price, seen_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET
                  store_id = EXCLUDED.store_id,
                  price    = EXCLUDED.price,
                  seen_at  = EXCLUDED.seen_at
                WHERE
                  -- accept if the incoming row is newer …
                  EXCLUDED.seen_at > prices.seen_at
                  -- … or same timestamp but cheaper
                  OR (EXCLUDED.seen_at = prices.seen_at AND EXCLUDED.price < prices.price)
                """,
                (product_id, store_id, amount, seen_at),
            )

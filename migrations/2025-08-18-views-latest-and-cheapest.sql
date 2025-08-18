-- 2025-08-18-views-latest-and-cheapest.sql
-- Creates/updates both views used by the API

BEGIN;

-- Latest row per (product, store)
CREATE OR REPLACE VIEW public.v_latest_store_prices AS
SELECT
  pr.product_id,
  pr.store_id,
  pr.price,
  pr.seen_at,
  s.name      AS store_name,
  s.chain     AS store_chain,
  s.is_online AS is_online
FROM public.prices pr
JOIN (
  SELECT product_id, store_id, MAX(seen_at) AS max_seen
  FROM public.prices
  GROUP BY product_id, store_id
) last
  ON last.product_id = pr.product_id
 AND last.store_id   = pr.store_id
 AND last.max_seen   = pr.seen_at
LEFT JOIN public.stores s ON s.id = pr.store_id;

-- Cheapest current offer per product
-- tie-breakers: newest seen_at, then lower store_id (stable)
CREATE OR REPLACE VIEW public.v_cheapest_offer AS
WITH latest AS (
  SELECT * FROM public.v_latest_store_prices
),
ranked AS (
  SELECT
    latest.*,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY price ASC, seen_at DESC, store_id ASC
    ) AS rn
  FROM latest
)
SELECT *
FROM ranked
WHERE rn = 1;

COMMIT;

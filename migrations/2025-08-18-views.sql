-- 2025-08-18-views.sql
-- Keep this file non-destructive. The canonical definition of
-- v_latest_store_prices lives in 2025-08-18-views-latest-and-cheapest.sql.
-- Do NOT redefine it here (doing so can cause "cannot drop columns from view").

BEGIN;

-- Recreate only the dependent view using the canonical base view.
CREATE OR REPLACE VIEW public.v_cheapest_offer AS
WITH latest AS (
  SELECT
    product_id, store_id, price, collected_at, seen_at,
    store_name, store_chain, is_online
  FROM public.v_latest_store_prices
),
ranked AS (
  SELECT
    latest.*,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY price ASC,
               collected_at DESC NULLS LAST,
               store_id ASC
    ) AS rn
  FROM latest
)
SELECT
  product_id, store_id, price, collected_at, seen_at,
  store_name, store_chain, is_online
FROM ranked
WHERE rn = 1;

COMMIT;

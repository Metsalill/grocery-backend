-- 2025-08-18-views-latest-and-cheapest.sql
-- Creates/updates both views used by the API

BEGIN;

-- Latest row per (product, store)
-- Works whether you have collected_at or only seen_at.
CREATE OR REPLACE VIEW public.v_latest_store_prices AS
SELECT DISTINCT ON (pr.product_id, pr.store_id)
  pr.product_id,
  pr.store_id,
  pr.price,
  COALESCE(pr.collected_at, pr.seen_at) AS collected_at, -- canonical "latest" timestamp
  pr.seen_at,                                            -- keep for backward-compat
  s.name      AS store_name,
  s.chain     AS store_chain,
  s.is_online AS is_online
FROM public.prices pr
LEFT JOIN public.stores s ON s.id = pr.store_id
ORDER BY
  pr.product_id,
  pr.store_id,
  COALESCE(pr.collected_at, pr.seen_at) DESC NULLS LAST;

-- Cheapest current offer per product
-- Tie-breakers: newest collected_at, then lower store_id (stable)
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

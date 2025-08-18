-- Latest row per (product, store)
CREATE OR REPLACE VIEW public.v_latest_store_prices AS
SELECT
  pr.product_id,
  pr.store_id,
  pr.price,
  pr.seen_at,
  s.code  AS store_code,
  s.name  AS store_name
FROM prices pr
JOIN (
  SELECT product_id, store_id, MAX(seen_at) AS max_seen
  FROM prices
  GROUP BY product_id, store_id
) last
  ON last.product_id = pr.product_id
 AND last.store_id   = pr.store_id
 AND last.max_seen   = pr.seen_at
LEFT JOIN stores s ON s.id = pr.store_id;

-- Cheapest current offer per product (tie-break: newest seen_at, then store_id)
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

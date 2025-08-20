-- migrations/2025-08-21-store-price-fallback.sql
BEGIN;

CREATE TABLE IF NOT EXISTS public.store_price_source (
  store_id        INT PRIMARY KEY REFERENCES public.stores(id) ON DELETE CASCADE,
  source_store_id INT NOT NULL     REFERENCES public.stores(id) ON DELETE RESTRICT,
  CONSTRAINT sps_distinct CHECK (store_id <> source_store_id)
);

CREATE INDEX IF NOT EXISTS ix_sps_source ON public.store_price_source(source_store_id);

-- Rebuild views to prefer a store's own prices, else fall back to the mapped source
DROP VIEW IF EXISTS public.v_cheapest_offer;
DROP VIEW IF EXISTS public.v_latest_store_prices;

WITH latest_self AS (
  SELECT DISTINCT ON (pr.product_id, pr.store_id)
    pr.product_id,
    pr.store_id,
    pr.price::numeric AS price,
    COALESCE(pr.collected_at, pr.seen_at) AS collected_at,
    pr.seen_at
  FROM public.prices pr
  ORDER BY pr.product_id, pr.store_id, COALESCE(pr.collected_at, pr.seen_at) DESC NULLS LAST
),
latest_src AS (
  SELECT DISTINCT ON (pr.product_id, pr.store_id)
    pr.product_id,
    pr.store_id,
    pr.price::numeric AS price,
    COALESCE(pr.collected_at, pr.seen_at) AS collected_at,
    pr.seen_at
  FROM public.prices pr
  ORDER BY pr.product_id, pr.store_id, COALESCE(pr.collected_at, pr.seen_at) DESC NULLS LAST
),
store_map AS (
  -- Map each destination (real) store to the price source (or itself if not mapped)
  SELECT s.id AS dest_store_id, COALESCE(m.source_store_id, s.id) AS src_store_id
  FROM public.stores s
  LEFT JOIN public.store_price_source m ON m.store_id = s.id
),
resolved AS (
  -- Priority 1: store's own latest price
  SELECT
    ls.product_id,
    ls.store_id                AS store_id,
    ls.price,
    ls.collected_at,
    ls.seen_at,
    s.name      AS store_name,
    s.chain     AS store_chain,
    s.is_online AS is_online,
    1           AS priority
  FROM latest_self ls
  JOIN public.stores s ON s.id = ls.store_id

  UNION ALL

  -- Priority 2: fallback to mapped source store's latest price
  SELECT
    lsrc.product_id,
    sm.dest_store_id           AS store_id,          -- expose the real store
    lsrc.price,
    lsrc.collected_at,
    lsrc.seen_at,
    ds.name     AS store_name,
    ds.chain    AS store_chain,
    ds.is_online AS is_online,
    2           AS priority
  FROM store_map sm
  JOIN latest_src lsrc ON lsrc.store_id = sm.src_store_id
  JOIN public.stores ds ON ds.id = sm.dest_store_id
)
CREATE VIEW public.v_latest_store_prices AS
SELECT DISTINCT ON (product_id, store_id)
  product_id,
  store_id,
  price,
  collected_at,
  seen_at,
  store_name,
  store_chain,
  is_online
FROM resolved
ORDER BY product_id, store_id, priority ASC, collected_at DESC NULLS LAST;

-- Dependent view (unchanged semantics)
CREATE VIEW public.v_cheapest_offer AS
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
      ORDER BY price ASC, collected_at DESC NULLS LAST, store_id ASC
    ) AS rn
  FROM latest
)
SELECT
  product_id, store_id, price, collected_at, seen_at,
  store_name, store_chain, is_online
FROM ranked
WHERE rn = 1;

COMMIT;

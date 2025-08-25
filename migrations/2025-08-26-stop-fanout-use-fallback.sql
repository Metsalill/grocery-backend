BEGIN;

-- Disable/remove any fan-out trigger/function (idempotent).
DROP TRIGGER IF EXISTS trg_prices_mirror_chain ON public.prices;
DROP FUNCTION IF EXISTS public.replicate_chain_prices_from_online();

-- Fallback mapping speed-ups (safe if they exist)
CREATE INDEX IF NOT EXISTS ix_prices_product_store_seen
  ON public.prices (product_id, store_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS ix_sps_store
  ON public.store_price_source (store_id);

CREATE INDEX IF NOT EXISTS ix_sps_source
  ON public.store_price_source (source_store_id);

-- Effective "latest price per store with fallback".
-- Prefer a store's own physical price; otherwise fall back to its mapped online source.
CREATE OR REPLACE VIEW public.v_latest_store_prices AS
WITH latest AS (
  SELECT
    p.product_id, p.store_id, p.price, p.currency, p.collected_at, p.source,
    ROW_NUMBER() OVER (PARTITION BY p.product_id, p.store_id ORDER BY p.collected_at DESC) AS rn
  FROM public.prices p
),
phys AS (
  SELECT * FROM latest WHERE rn = 1
),
map AS (
  -- Every store points to a "price source".
  -- If a mapping exists, use it; otherwise the store points to itself.
  SELECT s.id AS store_id, COALESCE(m.source_store_id, s.id) AS src_id
  FROM public.stores s
  LEFT JOIN public.store_price_source m ON m.store_id = s.id
),
eff AS (
  SELECT
    m.store_id,
    COALESCE(ph.product_id, onl.product_id) AS product_id,
    COALESCE(ph.price,       onl.price)       AS price,
    COALESCE(ph.currency,    onl.currency)    AS currency,
    COALESCE(ph.collected_at,onl.collected_at)AS collected_at,
    CASE WHEN ph.product_id IS NOT NULL THEN 'physical'
         WHEN onl.product_id IS NOT NULL THEN 'mirror:online'
         ELSE NULL END AS source
  FROM map m
  LEFT JOIN phys ph
    ON ph.store_id = m.store_id
  LEFT JOIN phys onl
    ON onl.store_id = m.src_id
)
SELECT product_id, store_id, price, currency, collected_at, source
FROM eff
WHERE product_id IS NOT NULL;

-- Cheapest offer view built on top of the effective prices.
CREATE OR REPLACE VIEW public.v_cheapest_offer AS
SELECT
  ep.product_id,
  (ARRAY_AGG(ep.store_id ORDER BY ep.price ASC, ep.collected_at DESC))[1] AS store_id,
  MIN(ep.price) AS price,
  (ARRAY_AGG(ep.currency ORDER BY ep.price ASC, ep.collected_at DESC))[1] AS currency,
  (ARRAY_AGG(ep.collected_at ORDER BY ep.price ASC, ep.collected_at DESC))[1] AS collected_at
FROM public.v_latest_store_prices ep
GROUP BY ep.product_id;

COMMIT;

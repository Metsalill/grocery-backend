-- 2025-08-26-stop-fanout-use-fallback.sql
-- Stop any price fan-out and rely on fallback views.

BEGIN;

-- 1) Drop ALL triggers on public.prices that call replicate_chain_prices_from_online()
DO $$
DECLARE trname text;
BEGIN
  FOR trname IN
    SELECT t.tgname
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid AND c.relname = 'prices' AND c.relnamespace = 'public'::regnamespace
    JOIN pg_proc  p ON p.oid = t.tgfoid
    WHERE p.proname = 'replicate_chain_prices_from_online'
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON public.prices;', trname);
  END LOOP;
END$$;

-- Also drop known legacy names (harmless if absent)
DROP TRIGGER IF EXISTS trg_prices_mirror_chain       ON public.prices;
DROP TRIGGER IF EXISTS trg_prices_mirror_from_online ON public.prices;

-- 2) Drop the mirror function (now unreferenced)
DROP FUNCTION IF EXISTS public.replicate_chain_prices_from_online();

-- 3) Helper indexes for fallback mapping
CREATE INDEX IF NOT EXISTS ix_prices_product_store_seen
  ON public.prices (product_id, store_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS ix_sps_store  ON public.store_price_source (store_id);
CREATE INDEX IF NOT EXISTS ix_sps_source ON public.store_price_source (source_store_id);

-- 4) Replace views cleanly (avoid column-mismatch error)
-- Drop dependent view first, then base view
DROP VIEW IF EXISTS public.v_cheapest_offer;
DROP VIEW IF EXISTS public.v_latest_store_prices;

-- Effective latest price per store with fallback to mapped source (e.g., e-Selver)
CREATE VIEW public.v_latest_store_prices AS
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
  SELECT s.id AS store_id, COALESCE(m.source_store_id, s.id) AS src_id
  FROM public.stores s
  LEFT JOIN public.store_price_source m ON m.store_id = s.id
),
eff AS (
  SELECT
    m.store_id,
    COALESCE(ph.product_id, onl.product_id)     AS product_id,
    COALESCE(ph.price,       onl.price)         AS price,
    COALESCE(ph.currency,    onl.currency)      AS currency,
    COALESCE(ph.collected_at,onl.collected_at)  AS collected_at,
    CASE WHEN ph.product_id IS NOT NULL THEN 'physical'
         WHEN onl.product_id IS NOT NULL THEN 'mirror:online'
         ELSE NULL END AS source
  FROM map m
  LEFT JOIN phys ph ON ph.store_id = m.store_id
  LEFT JOIN phys onl ON onl.store_id = m.src_id
)
SELECT product_id, store_id, price, currency, collected_at, source
FROM eff
WHERE product_id IS NOT NULL;

-- Cheapest offer built on top of the effective prices
CREATE VIEW public.v_cheapest_offer AS
SELECT
  ep.product_id,
  (ARRAY_AGG(ep.store_id     ORDER BY ep.price ASC, ep.collected_at DESC))[1] AS store_id,
  MIN(ep.price)  AS price,
  (ARRAY_AGG(ep.currency     ORDER BY ep.price ASC, ep.collected_at DESC))[1] AS currency,
  (ARRAY_AGG(ep.collected_at ORDER BY ep.price ASC, ep.collected_at DESC))[1] AS collected_at
FROM public.v_latest_store_prices ep
GROUP BY ep.product_id;

COMMIT;

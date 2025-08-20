-- 20250818_backend_logic_01.sql
-- Safe, idempotent helper objects for backend logic.
-- NOTE: We now standardize "latest" as COALESCE(collected_at, seen_at) and
-- rely on the canonical views created in 2025-08-18-views-latest-and-cheapest.sql.
-- This file no longer redefines those views.

BEGIN;

-- 1) Safety extensions
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- 2) Geo index (noop if already present)
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON public.stores USING gist (ll_to_earth(lat, lon));

-- 3) Helper: cheapest offer per product within a radius (meters) from a point
--    Uses canonical public.v_latest_store_prices (contains: product_id, store_id, price, collected_at, seen_at, …)
DROP FUNCTION IF EXISTS public.f_cheapest_offer_within_radius(double precision, double precision, integer);

CREATE FUNCTION public.f_cheapest_offer_within_radius(
  in_lat DOUBLE PRECISION,
  in_lon DOUBLE PRECISION,
  in_radius_m INTEGER
)
RETURNS TABLE (
  product_id BIGINT,
  store_id   BIGINT,
  price      NUMERIC,
  collected_at TIMESTAMPTZ
) AS $$
  WITH inradius AS (
    SELECT
      lsp.product_id,
      lsp.store_id,
      lsp.price::numeric AS price,
      lsp.collected_at
    FROM public.v_latest_store_prices lsp
    JOIN public.stores s ON s.id = lsp.store_id
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      AND earth_distance(
            ll_to_earth(in_lat, in_lon),
            ll_to_earth(s.lat, s.lon)
          ) <= in_radius_m
  ),
  ranked AS (
    SELECT
      i.*,
      ROW_NUMBER() OVER (
        PARTITION BY i.product_id
        ORDER BY i.price ASC,
                 i.collected_at DESC NULLS LAST,
                 i.store_id ASC
      ) AS rn
    FROM inradius i
  )
  SELECT product_id, store_id, price, collected_at
  FROM ranked
  WHERE rn = 1;
$$ LANGUAGE sql STABLE;

-- 4) (Optional) latest-price lookup helper index (noop if already created elsewhere)
CREATE INDEX IF NOT EXISTS ix_prices_latest
  ON public.prices (product_id, store_id, collected_at DESC);

COMMIT;

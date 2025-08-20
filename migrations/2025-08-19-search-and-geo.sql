-- 2025-08-19-search-and-geo.sql
-- Search + geo helpers (idempotent and fixed to use products.name)

BEGIN;

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- Clean up legacy indexes that referenced a non-existent "product" column
DROP INDEX IF EXISTS idx_products_trgm_product;
DROP INDEX IF EXISTS idx_products_lower_product;

-- Correct product-name search indexes
CREATE INDEX IF NOT EXISTS idx_products_name_trgm
  ON public.products USING gin (lower(name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_products_name_lower
  ON public.products (lower(name));

-- Geo: index for ll_to_earth lookups
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON public.stores USING gist (ll_to_earth(lat, lon));

COMMIT;

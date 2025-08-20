-- 2025-08-18-extensions-and-indexes.sql
-- Geo + fuzzy matching
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Geo index for earthdistance
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON stores USING gist (ll_to_earth(lat, lon));

-- Clean up any old/wrong indexes (safe: only drops if they exist)
DROP INDEX IF EXISTS idx_products_lower_name;      -- old file used LOWER(product)
DROP INDEX IF EXISTS idx_products_trgm_product;    -- old file used product gin_trgm_ops

-- Correct name-based indexes
CREATE INDEX IF NOT EXISTS idx_products_name_lower
  ON products (lower(name));

CREATE INDEX IF NOT EXISTS idx_products_name_trgm
  ON products USING gin (lower(name) gin_trgm_ops);

-- Helper for latest-price lookups (optional but useful)
CREATE INDEX IF NOT EXISTS ix_prices_latest
  ON prices (product_id, store_id, collected_at DESC);

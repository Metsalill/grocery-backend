-- Geo + fuzzy matching
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON stores USING gist (ll_to_earth(lat, lon));

-- Correct name-based indexes (use products.name, not "product")
CREATE INDEX IF NOT EXISTS idx_products_name_lower
  ON products (lower(name));

CREATE INDEX IF NOT EXISTS idx_products_name_trgm
  ON products USING gin (lower(name) gin_trgm_ops);

-- Optional: speed up latest-price lookups
CREATE INDEX IF NOT EXISTS ix_prices_latest
  ON prices (product_id, store_id, collected_at DESC);

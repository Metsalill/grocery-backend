-- Enable extensions (idempotent)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- Product search indexes
CREATE INDEX IF NOT EXISTS idx_products_trgm_product
  ON products USING gin (product gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_products_lower_product
  ON products ((lower(product)));

-- Geo: index for ll_to_earth lookups
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON stores USING gist (ll_to_earth(lat, lon));

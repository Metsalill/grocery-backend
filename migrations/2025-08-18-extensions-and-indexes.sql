-- Geo + fuzzy matching
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON stores USING gist (ll_to_earth(lat, lon));

CREATE INDEX IF NOT EXISTS idx_products_lower_name
  ON products (LOWER(product));

CREATE INDEX IF NOT EXISTS idx_products_trgm_product
  ON products USING gin (product gin_trgm_ops);

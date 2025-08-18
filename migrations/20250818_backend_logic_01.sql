-- 1. Safety extensions
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- 2. Helpful computed point on stores
-- If not already present: nothing to store, we compute on the fly using ll_to_earth(lat, lon)

-- 3. Latest price per product per store (dedupe uploads)
CREATE OR REPLACE VIEW v_latest_store_prices AS
SELECT DISTINCT ON (p.product_id, p.store_id)
  p.product_id,
  p.store_id,
  p.price::numeric(12,2) AS price,
  p.currency,
  p.updated_at
FROM prices p
WHERE p.deleted_at IS NULL
ORDER BY p.product_id, p.store_id, p.updated_at DESC;

-- 4. Cheapest offer per product (all stores)
CREATE OR REPLACE VIEW v_cheapest_offer AS
SELECT
  lsp.product_id,
  lsp.store_id,
  lsp.price,
  lsp.currency,
  lsp.updated_at
FROM v_latest_store_prices lsp
JOIN (
  SELECT product_id, MIN(price) AS min_price
  FROM v_latest_store_prices
  GROUP BY product_id
) m ON m.product_id = lsp.product_id AND m.min_price = lsp.price;

-- 5. Geo-filtered cheapest offer (within a radius from a point)
--   We implement as a SQL function to pass user’s coordinates & radius (meters).
CREATE OR REPLACE FUNCTION f_cheapest_offer_within_radius(
  in_lat DOUBLE PRECISION,
  in_lon DOUBLE PRECISION,
  in_radius_m INTEGER
)
RETURNS TABLE (
  product_id BIGINT,
  store_id BIGINT,
  price NUMERIC,
  currency TEXT,
  updated_at TIMESTAMPTZ
) AS $$
  SELECT
    lsp.product_id,
    lsp.store_id,
    lsp.price,
    lsp.currency,
    lsp.updated_at
  FROM v_latest_store_prices lsp
  JOIN stores s ON s.id = lsp.store_id
  WHERE earth_distance(
          ll_to_earth(in_lat, in_lon),
          ll_to_earth(s.lat, s.lon)
        ) <= in_radius_m
  QUALIFY lsp.price = MIN(lsp.price) OVER (PARTITION BY lsp.product_id);
$$ LANGUAGE sql STABLE;

-- 6. Availability count per product
CREATE OR REPLACE VIEW v_product_availability AS
SELECT
  lsp.product_id,
  COUNT(*) AS store_count
FROM v_latest_store_prices lsp
GROUP BY lsp.product_id;

-- 7. Useful indexes (tune names/columns to your exact schema)
-- prices table likely has (product_id, store_id, updated_at) already; ensure:
CREATE INDEX IF NOT EXISTS idx_prices_product_store_updated
  ON prices (product_id, store_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_prices_not_deleted
  ON prices (product_id) WHERE deleted_at IS NULL;

-- stores geospatial acceleration (earthdistance uses btree on ll_to_earth)
CREATE INDEX IF NOT EXISTS idx_stores_earth
  ON stores USING gist (ll_to_earth(lat, lon));

-- 8. Optional: materialize cheapest for speed (refresh on price uploads)
-- CREATE MATERIALIZED VIEW mv_cheapest_offer AS
--   SELECT * FROM v_cheapest_offer;
-- CREATE UNIQUE INDEX ON mv_cheapest_offer(product_id);

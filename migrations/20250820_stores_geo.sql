-- migrations/20250820_stores_geo.sql
-- Haversine distance in KM (no PostGIS needed)
CREATE OR REPLACE FUNCTION haversine_km(
  lat1 double precision, lon1 double precision,
  lat2 double precision, lon2 double precision
) RETURNS double precision AS $$
  SELECT 2 * 6371 * asin(
    sqrt(
      sin(radians((lat2 - lat1)/2))^2 +
      cos(radians(lat1)) * cos(radians(lat2)) *
      sin(radians((lon2 - lon1)/2))^2
    )
  );
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

-- Light index to help with rough prefilter
CREATE INDEX IF NOT EXISTS idx_stores_lat_lon ON stores(lat, lon);

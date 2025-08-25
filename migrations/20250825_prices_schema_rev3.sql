-- 20250825_prices_schema_rev3.sql
-- Canonical price schema + trigger to keep `prices` in sync from `price_history`
-- and views for latest-per-store and cheapest-per-product.

BEGIN;

-- 1) Safety: required tables (minimal columns shown; extend if you already have more)
CREATE TABLE IF NOT EXISTS products (
  id BIGSERIAL PRIMARY KEY,
  ean TEXT UNIQUE,
  name TEXT NOT NULL,
  size_text TEXT,
  brand TEXT
);

CREATE TABLE IF NOT EXISTS stores (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  chain TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION
);

-- 2) Full timeline of prices
CREATE TABLE IF NOT EXISTS price_history (
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  store_id   BIGINT NOT NULL REFERENCES stores(id)   ON DELETE CASCADE,
  price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
  currency TEXT NOT NULL DEFAULT 'EUR',
  collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT
);
CREATE INDEX IF NOT EXISTS idx_price_history_prod_store_time
  ON price_history (product_id, store_id, collected_at DESC);

-- Optional: dedupe guard (same second)
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_history_exact
  ON price_history (product_id, store_id, collected_at, price, currency);

-- 3) Latest snapshot table (one row per product+store)
CREATE TABLE IF NOT EXISTS prices (
  product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  store_id   BIGINT NOT NULL REFERENCES stores(id)   ON DELETE CASCADE,
  price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
  currency TEXT NOT NULL DEFAULT 'EUR',
  collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT,
  PRIMARY KEY (product_id, store_id)
);
CREATE INDEX IF NOT EXISTS idx_prices_prod_store_time
  ON prices (product_id, store_id, collected_at DESC);

-- 4) Upsert function: keep `prices` in sync from inserted price_history
CREATE OR REPLACE FUNCTION upsert_latest_price_from_history()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO prices (product_id, store_id, price, currency, collected_at, source)
  VALUES (NEW.product_id, NEW.store_id, NEW.price, COALESCE(NEW.currency, 'EUR'), NEW.collected_at, NEW.source)
  ON CONFLICT (product_id, store_id)
  DO UPDATE SET
    price        = EXCLUDED.price,
    currency     = EXCLUDED.currency,
    collected_at = EXCLUDED.collected_at,
    source       = EXCLUDED.source
  WHERE EXCLUDED.collected_at >= prices.collected_at;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_price_history_to_prices ON price_history;
CREATE TRIGGER trg_price_history_to_prices
AFTER INSERT ON price_history
FOR EACH ROW
EXECUTE FUNCTION upsert_latest_price_from_history();

-- 5) (Re)build prices snapshot from history if needed
--    Safe if you already have data; it will upsert only when newer.
WITH latest AS (
  SELECT DISTINCT ON (ph.product_id, ph.store_id)
         ph.product_id, ph.store_id, ph.price, ph.currency, ph.collected_at, ph.source
  FROM price_history ph
  ORDER BY ph.product_id, ph.store_id, ph.collected_at DESC
)
INSERT INTO prices (product_id, store_id, price, currency, collected_at, source)
SELECT product_id, store_id, price, COALESCE(currency,'EUR'), collected_at, source
FROM latest
ON CONFLICT (product_id, store_id) DO UPDATE
SET price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    collected_at = EXCLUDED.collected_at,
    source = EXCLUDED.source
WHERE EXCLUDED.collected_at >= prices.collected_at;

-- 6) Views
CREATE OR REPLACE VIEW v_latest_store_prices AS
SELECT p.product_id, p.store_id, p.price, p.currency, p.collected_at, p.source
FROM prices p;

CREATE OR REPLACE VIEW v_cheapest_offer AS
WITH ranked AS (
  SELECT
    p.product_id,
    p.store_id,
    p.price,
    p.currency,
    p.collected_at,
    RANK() OVER (
      PARTITION BY p.product_id
      ORDER BY p.price ASC, p.collected_at DESC, p.store_id ASC
    ) AS r
  FROM prices p
)
SELECT product_id, store_id, price, currency, collected_at
FROM ranked
WHERE r = 1;

COMMIT;

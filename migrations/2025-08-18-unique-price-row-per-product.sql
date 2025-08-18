-- canonical “one row per product” in prices
CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_product ON prices (product_id);

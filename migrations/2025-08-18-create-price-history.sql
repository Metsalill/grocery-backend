CREATE TABLE IF NOT EXISTS price_history (
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL REFERENCES products(id),
  amount NUMERIC(10,2) NOT NULL,
  currency TEXT NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  store_id BIGINT,
  price_type TEXT,
  source_url TEXT
);

CREATE INDEX IF NOT EXISTS ix_price_history_prod_seen
  ON price_history (product_id, captured_at DESC);

CREATE TABLE IF NOT EXISTS product_aliases (
  id SERIAL PRIMARY KEY,
  product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  alias TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_alias_lower ON product_aliases (lower(alias));
CREATE INDEX IF NOT EXISTS idx_product_aliases_product ON product_aliases (product_id);

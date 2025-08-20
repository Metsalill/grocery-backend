-- migrations/20250820_products_qty.sql
ALTER TABLE products
  ADD COLUMN IF NOT EXISTS pack_pattern TEXT,
  ADD COLUMN IF NOT EXISTS net_qty NUMERIC,                  -- per unit (e.g., 330 or 500 or 1500)
  ADD COLUMN IF NOT EXISTS net_unit TEXT
    CHECK (net_unit IN ('g','ml')) ,
  ADD COLUMN IF NOT EXISTS pack_count INTEGER;

CREATE INDEX IF NOT EXISTS idx_products_net ON products (net_unit, net_qty);

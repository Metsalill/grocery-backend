-- 2025-08-22-fix-prices-unique-key.sql
BEGIN;

-- Drop the old “one row per product” uniqueness (exists as either a constraint or index)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'prices' AND c.conname = 'uq_prices_product'
  ) THEN
    ALTER TABLE public.prices DROP CONSTRAINT uq_prices_product;
  END IF;
END$$;

DROP INDEX IF EXISTS public.uq_prices_product;

-- Ensure collected_at exists (safe if already present)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS collected_at timestamptz;

-- Correct uniqueness: allow multiple stores, allow time-series per store
ALTER TABLE public.prices
  ADD CONSTRAINT uq_prices_store_product_at
  UNIQUE (store_id, product_id, collected_at);

-- Helpful index for “latest per store/product”
CREATE INDEX IF NOT EXISTS idx_prices_store_product_at_desc
  ON public.prices (store_id, product_id, collected_at DESC);

COMMIT;

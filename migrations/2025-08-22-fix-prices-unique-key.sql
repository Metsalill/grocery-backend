-- 2025-08-22-fix-prices-unique-key.sql
-- Make 'prices' consistent (PK on (product_id, store_id)) and avoid duplicate index errors.
-- Also ensure a sane uniqueness on price_history by (product_id, store_id, collected_at).

BEGIN;

-- 1) Cleanup: old/bad unique index on prices(product_id) if it ever existed
DROP INDEX IF EXISTS uq_prices_product;

-- 2) Ensure collected_at exists on prices (harmless if already present)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ;

-- 3) Ensure composite PK on prices(product_id, store_id)
DO $$
BEGIN
  -- If no primary key yet, create one via a unique index and promote
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.prices'::regclass AND contype='p'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname='public' AND indexname='uq_prices_product_store'
    ) THEN
      EXECUTE 'CREATE UNIQUE INDEX uq_prices_product_store ON public.prices(product_id, store_id)';
    END IF;
    EXECUTE 'ALTER TABLE public.prices ADD CONSTRAINT prices_pkey PRIMARY KEY USING INDEX uq_prices_product_store';
  END IF;
END $$;

-- 4) History table uniqueness: at most one entry per product+store+timestamp
-- (If you already have a stricter unique index on history, this is still safe.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_history_store_product_at
  ON public.price_history (product_id, store_id, collected_at);

-- 5) If someone previously created a redundant unique index on prices with collected_at,
--    don't recreate it; if it already exists, keep or drop as you prefer. We choose to KEEP.
--    (But do NOT try to CREATE it again, which caused the error earlier.)
--    If you want it gone, uncomment the next line:
-- DROP INDEX IF EXISTS uq_prices_store_product_at;

COMMIT;

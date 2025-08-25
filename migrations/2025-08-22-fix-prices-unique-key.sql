-- 2025-08-22-fix-prices-unique-key.sql
-- Make 'prices' PK solid; add history uniqueness using whichever timestamp column exists.

BEGIN;

-- Clean old single-column uniqueness if it ever existed
DROP INDEX IF EXISTS uq_prices_product;
-- Also clean up a redundant unique index on prices including collected_at, if someone created it
DROP INDEX IF EXISTS uq_prices_store_product_at;

-- Ensure collected_at exists on prices (harmless if already present)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ;

-- Ensure composite PK on prices(product_id, store_id)
DO $$
BEGIN
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

-- History uniqueness: prefer collected_at, else seen_at, else last_seen_utc
DO $$
DECLARE tscol text;
BEGIN
  SELECT column_name
  INTO tscol
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='price_history'
    AND column_name IN ('collected_at','seen_at','last_seen_utc')
  ORDER BY CASE column_name
             WHEN 'collected_at'   THEN 1
             WHEN 'seen_at'        THEN 2
             WHEN 'last_seen_utc'  THEN 3
             ELSE 99
           END
  LIMIT 1;

  IF tscol IS NULL THEN
    RAISE NOTICE 'price_history has no known timestamp column; skipping unique index';
  ELSE
    -- Create a single canonical unique index on (product_id, store_id, tscol)
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname='public' AND indexname='uq_price_history_store_product_ts'
    ) THEN
      EXECUTE format(
        'CREATE UNIQUE INDEX uq_price_history_store_product_ts ON public.price_history (product_id, store_id, %I)',
        tscol
      );
    END IF;
  END IF;
END $$;

COMMIT;

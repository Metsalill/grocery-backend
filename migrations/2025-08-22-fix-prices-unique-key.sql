-- 2025-08-22-fix-prices-unique-key.sql
-- Keep prices PK = (product_id, store_id); remove any UNIQUE that involves collected_at.
-- Make history unique by timestamp (collected_at/seen_at/last_seen_utc). Idempotent.

BEGIN;

-- 0) Clean legacy per-product unique index if it ever existed
DROP INDEX IF EXISTS uq_prices_product;

-- 1) If there is a UNIQUE constraint on prices that uses collected_at, drop it first
ALTER TABLE public.prices
  DROP CONSTRAINT IF EXISTS uq_prices_store_product_at;

-- Also drop ANY other UNIQUE constraint on prices that includes collected_at
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT pc.conname
    FROM pg_constraint pc
    WHERE pc.conrelid = 'public.prices'::regclass
      AND pc.contype  = 'u'
      AND EXISTS (
        SELECT 1
        FROM unnest(pc.conkey) AS k
        JOIN pg_attribute att
          ON att.attrelid = pc.conrelid
         AND att.attnum   = k
        WHERE att.attname = 'collected_at'
      )
  LOOP
    EXECUTE format('ALTER TABLE public.prices DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

-- If an index with that old name exists independently, drop it
DROP INDEX IF EXISTS uq_prices_store_product_at;

-- 2) Ensure collected_at column exists on prices (metadata only)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ;

-- 3) Ensure composite PK on prices(product_id, store_id)
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

-- 4) History uniqueness: choose an existing timestamp column
DO $$
DECLARE tscol text;
BEGIN
  SELECT column_name INTO tscol
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='price_history'
    AND column_name IN ('collected_at','seen_at','last_seen_utc')
  ORDER BY CASE column_name
             WHEN 'collected_at'  THEN 1
             WHEN 'seen_at'       THEN 2
             WHEN 'last_seen_utc' THEN 3
             ELSE 99
           END
  LIMIT 1;

  IF tscol IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname='public' AND indexname='uq_price_history_store_product_ts'
    ) THEN
      EXECUTE format(
        'CREATE UNIQUE INDEX uq_price_history_store_product_ts ON public.price_history (product_id, store_id, %I)',
        tscol
      );
    END IF;
  ELSE
    RAISE NOTICE 'price_history has no known timestamp column; skipping unique index';
  END IF;
END $$;

COMMIT;

-- 2025-08-22-fix-prices-unique-key.sql
-- Keep prices PK = (product_id, store_id); remove any unique constraint that includes collected_at.
-- Make history unique by timestamp (collected_at/seen_at/last_seen_utc).

BEGIN;

-- 0) Clean legacy per-product unique index if it ever existed
DROP INDEX IF EXISTS uq_prices_product;

-- 1) If there is a UNIQUE constraint on prices that uses collected_at, drop it first.
--    (Your earlier runs named it uq_prices_store_product_at.)
ALTER TABLE public.prices
  DROP CONSTRAINT IF EXISTS uq_prices_store_product_at;

-- Also be robust if a differently-named UNIQUE(collected_at, ...) exists:
DO $$
DECLARE c RECORD;
BEGIN
  FOR c IN
    SELECT conname
    FROM pg_constraint c
    WHERE c.conrelid = 'public.prices'::regclass
      AND c.contype  = 'u'
      AND (
        SELECT bool_or(att.attname = 'collected_at')
        FROM unnest(c.conkey) k
        JOIN pg_attribute att ON att.attrelid=c.conrelid AND att.attnum=k
      )
  LOOP
    EXECUTE format('ALTER TABLE public.prices DROP CONSTRAINT %I', c.conname);
  END LOOP;
END $$;

-- 2) Now it is safe to drop any leftover index with that name (if it wasn't owned by a constraint)
DROP INDEX IF EXISTS uq_prices_store_product_at;

-- 3) Ensure collected_at column exists on prices (metadata only)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ;

-- 4) Ensure composite PK on prices(product_id, store_id)
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

-- 5) History uniqueness: choose an existing timestamp column
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

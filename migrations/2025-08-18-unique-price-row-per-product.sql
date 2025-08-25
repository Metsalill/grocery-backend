-- 2025-08-18-unique-price-row-per-product.sql
-- Enforce one row per (product_id, store_id) in `prices` (NOT per product).

BEGIN;

-- Drop a bad PK (if any) so we can redefine properly.
DO $$
DECLARE pkname text;
BEGIN
  SELECT conname INTO pkname
  FROM pg_constraint
  WHERE conrelid = 'public.prices'::regclass AND contype = 'p';
  IF pkname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE public.prices DROP CONSTRAINT %I', pkname);
  END IF;
END $$;

-- Drop the wrong single-column unique index if it exists.
DROP INDEX IF EXISTS uq_prices_product;

-- Clean rows that would violate a composite PK.
DELETE FROM prices WHERE product_id IS NULL OR store_id IS NULL;

-- Deduplicate: keep newest by collected_at per (product_id, store_id)
WITH ranked AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY product_id, store_id
      ORDER BY collected_at DESC NULLS LAST, ctid DESC
    ) AS rn
  FROM prices
)
DELETE FROM prices p
USING ranked r
WHERE p.ctid = r.ctid AND r.rn > 1;

-- Create composite unique index if missing, then promote to PK if missing.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='uq_prices_product_store'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_prices_product_store ON public.prices(product_id, store_id)';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.prices'::regclass AND contype='p'
  ) THEN
    EXECUTE 'ALTER TABLE public.prices ADD CONSTRAINT prices_pkey PRIMARY KEY USING INDEX uq_prices_product_store';
  END IF;
END $$;

COMMIT;

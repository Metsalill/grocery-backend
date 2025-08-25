-- 2025-08-18-unique-price-row-per-product.sql
-- FIX: enforce one row per (product_id, store_id), not per product.

BEGIN;

-- 1) Drop the wrong uniqueness (if previously created/attempted)
DROP INDEX IF EXISTS uq_prices_product;

-- 2) Dedupe exact duplicates per (product_id, store_id), keep newest by collected_at
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

-- 3) Enforce uniqueness per (product_id, store_id), and promote to PK if none exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uq_prices_product_store'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_prices_product_store ON prices(product_id, store_id)';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.prices'::regclass AND contype = 'p'
  ) THEN
    EXECUTE 'ALTER TABLE prices ADD CONSTRAINT prices_pkey PRIMARY KEY USING INDEX uq_prices_product_store';
  END IF;
END $$;

COMMIT;

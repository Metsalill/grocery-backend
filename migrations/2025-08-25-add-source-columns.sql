-- 2025-08-25-add-source-columns.sql
BEGIN;

-- provenance for snapshot table (used by mirror trigger)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS source TEXT;

-- provenance for timeline too (so history → prices carries it through)
ALTER TABLE public.price_history
  ADD COLUMN IF NOT EXISTS source TEXT;

-- backfill any existing rows to a neutral value
UPDATE public.prices
SET source = COALESCE(source, 'unknown');

COMMIT;

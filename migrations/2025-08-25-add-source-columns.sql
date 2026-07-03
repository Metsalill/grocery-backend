-- 2025-08-25-add-source-columns.sql
BEGIN;
-- provenance for snapshot table (used by mirror trigger)
ALTER TABLE public.prices
  ADD COLUMN IF NOT EXISTS source TEXT;
-- provenance for timeline too (so history → prices carries it through)
ALTER TABLE public.price_history
  ADD COLUMN IF NOT EXISTS source TEXT;
-- backfill any existing rows to a neutral value
-- WHERE clause is critical: without it, this UPDATE physically rewrites
-- every row in the table on every run (Postgres MVCC does not skip a row
-- just because COALESCE would keep its value unchanged), which locks the
-- table for the full duration of the write. On a multi-million-row prices
-- table under concurrent scraper upserts, this caused a >1h lock chain
-- and canceled workflow run (see grocery-backend DB Views Migration #66).
-- With the WHERE clause, this becomes a fast no-op on every run after the
-- first successful backfill.
UPDATE public.prices
SET source = 'unknown'
WHERE source IS NULL;
COMMIT;

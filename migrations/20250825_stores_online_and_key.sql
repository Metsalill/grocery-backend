BEGIN;

ALTER TABLE stores
  ADD COLUMN IF NOT EXISTS is_online BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS external_key TEXT;

-- Use either (chain, external_key) if you set external ids, else (chain, name).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_stores_chain_external'
  ) THEN
    BEGIN
      EXECUTE 'CREATE UNIQUE INDEX uq_stores_chain_external ON stores (COALESCE(chain, ''''), COALESCE(external_key, ''''))';
    EXCEPTION WHEN OTHERS THEN
      -- fallback if you don't use external_key
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_stores_chain_name'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX uq_stores_chain_name ON stores (COALESCE(chain, ''''), COALESCE(name, ''''))';
      END IF;
    END;
  END IF;
END $$;

COMMIT;

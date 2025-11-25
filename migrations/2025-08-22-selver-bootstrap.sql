-- 2025-08-22-selver-bootstrap.sql
-- Selver bootstrap: helper EAN table + Selver staging table + indexes.
-- Designed to be idempotent so it can be re-run safely.

BEGIN;

-- 1) Ensure pg_trgm is available for trigram search on Selver product names
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2) Helper table for mapping canonical products <-> EAN
CREATE TABLE IF NOT EXISTS product_eans (
    product_id INT  NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    ean_raw    TEXT NOT NULL,
    ean_norm   TEXT NOT NULL
);

-- Unique EAN (normalized) across the helper table
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_eans_norm
    ON product_eans (ean_norm);

-- Backfill product_eans from products.ean, but only if that column exists.
-- Uses ON CONFLICT DO NOTHING so it is safe to run multiple times.
DO $$
BEGIN
    -- If products.ean doesn’t exist yet, just skip the bootstrap.
    PERFORM 1
    FROM information_schema.columns
    WHERE table_name = 'products' AND column_name = 'ean';

    IF NOT FOUND THEN
        RETURN;
    END IF;

    INSERT INTO product_eans (product_id, ean_raw, ean_norm)
    SELECT
        p.id,
        p.ean,
        regexp_replace(p.ean, '\D', '', 'g')  -- keep only digits
    FROM products p
    WHERE p.ean IS NOT NULL
      AND p.ean <> ''
    ON CONFLICT DO NOTHING;
END
$$;

-- 3) Staging table for raw Selver crawl results
CREATE TABLE IF NOT EXISTS staging_selver_products (
    id           BIGSERIAL PRIMARY KEY,
    ext_id       TEXT NOT NULL,               -- Selver external id / SKU
    name         TEXT NOT NULL,               -- product name as shown on site
    brand        TEXT,
    size_text    TEXT,
    ean          TEXT,
    price        NUMERIC(10,2),
    image_url    TEXT,
    category_raw TEXT,                        -- raw category path / breadcrumb
    payload      JSONB,                       -- full raw JSON from crawler
    seen_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful indexes for Selver staging lookups
CREATE INDEX IF NOT EXISTS ix_selver_ean
    ON staging_selver_products (ean);

CREATE INDEX IF NOT EXISTS ix_selver_name_trgm
    ON staging_selver_products
    USING GIN (name gin_trgm_ops);

-- 4) Stores: drop any old unique index on online chains and replace
--    it with a non-unique index so multiple online stores per chain
--    (e.g. Coop eCoop, Wolt Coop, Bolt Coop) are allowed.

-- Old version (problematic) was:
--   CREATE UNIQUE INDEX uniq_chain_online ON stores (lower(chain)) WHERE is_online;
-- which fails as soon as there are multiple is_online rows for the same chain.
DROP INDEX IF EXISTS uniq_chain_online;

CREATE INDEX IF NOT EXISTS idx_chain_online
    ON stores (lower(chain))
    WHERE is_online;

COMMIT;

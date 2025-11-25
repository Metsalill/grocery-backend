-- 2025-08-22-selver-bootstrap.sql
-- Selver helper structures: EANs, staging table, host → store mapping

BEGIN;

-- 1) Ensure pg_trgm, used elsewhere as well
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 2) Canonical product EANs
--    ean_norm is a GENERATED column, so we never insert into it manually.
CREATE TABLE IF NOT EXISTS product_eans (
    product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    ean_raw    TEXT NOT NULL,
    -- keep only digits from ean_raw
    ean_norm   TEXT GENERATED ALWAYS AS (regexp_replace(ean_raw, '\D', '', 'g')) STORED,
    PRIMARY KEY (product_id, ean_norm)
);

-- unique across all products, so we can look up by normalized EAN
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_eans_norm
    ON product_eans (ean_norm);

-- Backfill from existing products.
-- NOTE: we only insert product_id + ean_raw; ean_norm is generated.
DO $$
BEGIN
    INSERT INTO product_eans (product_id, ean_raw)
    SELECT
        p.id,
        p.ean
    FROM products p
    WHERE p.ean IS NOT NULL
      AND p.ean <> ''
    ON CONFLICT DO NOTHING;
END $$;

-- 3) Staging table for Selver web scrape
CREATE TABLE IF NOT EXISTS staging_selver_products (
    id          BIGSERIAL PRIMARY KEY,
    ext_id      TEXT,             -- external id from Selver
    name        TEXT,
    size_text   TEXT,
    brand       TEXT,
    ean         TEXT,
    category    TEXT,
    subcategory TEXT,
    price       NUMERIC(10, 2),
    url         TEXT,
    image_url   TEXT,
    raw         JSONB             -- full raw payload if we want it
);

CREATE INDEX IF NOT EXISTS ix_selver_ean
    ON staging_selver_products (ean);

CREATE INDEX IF NOT EXISTS ix_selver_name_trgm
    ON staging_selver_products
    USING GIN (name gin_trgm_ops);

-- 4) Mapping from HTTP host → store_id
CREATE TABLE IF NOT EXISTS store_host_map (
    host     TEXT PRIMARY KEY,
    store_id INT NOT NULL REFERENCES stores(id)
);

COMMIT;

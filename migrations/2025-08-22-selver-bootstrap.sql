-- 2025-08-22-selver-bootstrap.sql
-- Bootstrap tables for Selver scraping + shared EAN storage.
-- Pure DDL, idempotent, no data backfill.

BEGIN;

-- Make sure trigram extension exists (for name search etc)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1) Table for normalised EANs per canonical product
CREATE TABLE IF NOT EXISTS product_eans (
    product_id  INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    ean_raw     TEXT NOT NULL,
    -- digits-only version, stored as generated column
    ean_norm    TEXT GENERATED ALWAYS AS (regexp_replace(ean_raw, '\D', '', 'g')) STORED,
    PRIMARY KEY (product_id, ean_norm)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_product_eans_norm
    ON product_eans (ean_norm, product_id);

-- 2) Staging table for raw Selver products (scraper will insert here)
CREATE TABLE IF NOT EXISTS staging_selver_products (
    id           BIGSERIAL PRIMARY KEY,
    ext_id       TEXT,
    name         TEXT NOT NULL,
    size_text    TEXT,
    brand        TEXT,
    ean_raw      TEXT,
    price        NUMERIC(10, 2),
    url          TEXT,
    image_url    TEXT,
    collected_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_selver_products_name_trgm
    ON staging_selver_products
    USING gin (name gin_trgm_ops);

COMMIT;

-- migrations/2025-08-22-selver-bootstrap.sql
-- Selver bootstrap / helper tables
-- Simplified to be FAST and idempotent for GitHub Actions:
--   - Only creates tables / indexes if missing
--   - Does NOT run any large backfill INSERT/UPDATE

-- 1) Ensure pg_trgm is available (used elsewhere too)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2) Helper table for EANs per canonical product
CREATE TABLE IF NOT EXISTS product_eans (
    product_id   INT PRIMARY KEY,
    ean_raw      TEXT NOT NULL,
    -- Normalised digits-only EAN, generated from ean_raw
    ean_norm     TEXT GENERATED ALWAYS AS (
        regexp_replace(ean_raw, '\D', '', 'g')
    ) STORED
);

-- Unique index on the normalised EAN (safe no-op if already exists)
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_eans_norm
    ON product_eans (ean_norm);

-- 3) Staging table for Selver crawler.
-- We only declare a minimal, backwards-compatible structure.
CREATE TABLE IF NOT EXISTS staging_selver_products (
    id          BIGSERIAL PRIMARY KEY,
    ext_id      TEXT,
    chain       TEXT,
    name        TEXT,
    brand       TEXT,
    size_text   TEXT,
    price       NUMERIC,
    url         TEXT,
    raw_json    JSONB,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- NOTE:
-- Any heavy backfill of product_eans or staging_selver_products
-- has been intentionally removed from this migration to keep CI
-- under the 10 minute limit.
--
-- If you ever need to regenerate product_eans manually, you can run
-- something like (outside of GitHub Actions):
--
--   INSERT INTO product_eans (product_id, ean_raw)
--   SELECT id, ean
--   FROM products
--   WHERE ean IS NOT NULL AND ean <> ''
--   ON CONFLICT (product_id) DO UPDATE
--     SET ean_raw = EXCLUDED.ean_raw;

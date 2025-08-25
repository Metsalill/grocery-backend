-- 2025-08-22-selver-bootstrap.sql
-- Bootstrap for Selver integration: canonical EANs, Selver staging table, and one Selver online store

BEGIN;

-- Extensions we rely on elsewhere
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1) Canonical EANs for your products (many barcodes can map to one product)
CREATE TABLE IF NOT EXISTS public.product_eans (
  product_id INT  NOT NULL REFERENCES public.products(id) ON DELETE CASCADE,
  ean_raw    TEXT,
  ean_norm   TEXT GENERATED ALWAYS AS (regexp_replace(coalesce(ean_raw,''), '\D', '', 'g')) STORED,
  PRIMARY KEY (product_id, ean_norm)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_eans_norm ON public.product_eans(ean_norm);

-- Optional backfill: if your products table already has an EAN/Barcode column, copy it in once.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='products' AND column_name='ean') THEN
    INSERT INTO public.product_eans(product_id, ean_raw)
    SELECT id, ean
    FROM public.products
    WHERE ean IS NOT NULL AND btrim(ean) <> ''
    ON CONFLICT DO NOTHING;
  ELSIF EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='products' AND column_name='barcode') THEN
    INSERT INTO public.product_eans(product_id, ean_raw)
    SELECT id, barcode
    FROM public.products
    WHERE barcode IS NOT NULL AND btrim(barcode) <> ''
    ON CONFLICT DO NOTHING;
  END IF;
END$$;

-- 2) Selver staging (what the importer writes to before matching)
CREATE TABLE IF NOT EXISTS public.staging_selver_products (
  ext_id       TEXT PRIMARY KEY,        -- Selver SKU/ID
  name         TEXT NOT NULL,
  ean_raw      TEXT,
  ean_norm     TEXT GENERATED ALWAYS AS (regexp_replace(coalesce(ean_raw,''), '\D', '', 'g')) STORED,
  size_text    TEXT,
  price        NUMERIC(12,2) NOT NULL,
  currency     TEXT DEFAULT 'EUR',
  collected_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_selver_ean       ON public.staging_selver_products(ean_norm);
CREATE INDEX IF NOT EXISTS ix_selver_name_trgm ON public.staging_selver_products USING gin (name gin_trgm_ops);

-- 3) Exactly one ONLINE Selver per chain (partial unique index)
CREATE UNIQUE INDEX IF NOT EXISTS uniq_chain_online
  ON public.stores (lower(chain))
  WHERE COALESCE(is_online,false) = TRUE;

-- Insert the canonical online Selver store iff none exists (conflict-safe)
INSERT INTO public.stores (name, chain, is_online)
VALUES ('Selver e-Selver', 'Selver', TRUE)
ON CONFLICT DO NOTHING;

COMMIT;

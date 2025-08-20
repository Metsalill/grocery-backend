-- 2025-08-22-selver-verify-views.sql
BEGIN;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Manual/external mapping table (used for non-EAN matches or overrides)
CREATE TABLE IF NOT EXISTS public.ext_product_map (
  source     TEXT NOT NULL,                               -- e.g. 'selver'
  ext_id     TEXT NOT NULL,                               -- staging key
  product_id INT  NOT NULL REFERENCES public.products(id) ON DELETE CASCADE,
  reason     TEXT NOT NULL DEFAULT 'manual',              -- 'ean','name+qty','manual'
  confidence NUMERIC(4,3) DEFAULT 1,
  PRIMARY KEY (source, ext_id)
);

-- Side-by-side preview of resolved Selver items (EAN or manual map)
CREATE OR REPLACE VIEW public.v_selver_match_preview AS
WITH ean_match AS (
  SELECT
    s.ext_id,
    s.name        AS selver_name,
    s.ean_norm,
    s.size_text   AS selver_size,
    p.id          AS product_id,
    p.name        AS prisma_name,
    p.size_text   AS prisma_size,
    'ean'::text   AS method
  FROM public.staging_selver_products s
  JOIN public.product_eans pe ON pe.ean_norm = s.ean_norm
  JOIN public.products    p  ON p.id = pe.product_id
),
manual_match AS (
  SELECT
    s.ext_id,
    s.name        AS selver_name,
    s.ean_norm,
    s.size_text   AS selver_size,
    p.id          AS product_id,
    p.name        AS prisma_name,
    p.size_text   AS prisma_size,
    COALESCE(m.reason,'manual')::text AS method
  FROM public.staging_selver_products s
  JOIN public.ext_product_map m ON m.source='selver' AND m.ext_id = s.ext_id
  JOIN public.products p        ON p.id = m.product_id
  LEFT JOIN public.product_eans pe ON pe.ean_norm = s.ean_norm
  WHERE pe.product_id IS NULL        -- avoid duplicates already covered by EAN
),
all_matches AS (
  SELECT * FROM ean_match
  UNION ALL
  SELECT * FROM manual_match
)
SELECT
  a.ext_id,
  a.method,
  a.ean_norm,
  a.selver_name,
  a.selver_size,
  a.product_id,
  a.prisma_name,
  a.prisma_size,
  similarity(lower(a.selver_name), lower(a.prisma_name)) AS name_sim,
  (similarity(lower(a.selver_name), lower(a.prisma_name)) >= 0.55) AS name_ok
FROM all_matches a
ORDER BY a.method, a.ext_id;

-- Suspicious pairs (low similarity or differing size text)
CREATE OR REPLACE VIEW public.v_selver_match_anomalies AS
SELECT *
FROM public.v_selver_match_preview v
WHERE v.name_ok IS FALSE
   OR NULLIF(trim(coalesce(v.selver_size,'')), '') IS DISTINCT FROM NULLIF(trim(coalesce(v.prisma_size,'')), '');

-- Helpful list of Selver rows with EAN that didn’t match any of your product_eans (data quality queue)
CREATE OR REPLACE VIEW public.v_selver_unmatched_with_ean AS
SELECT s.*
FROM public.staging_selver_products s
LEFT JOIN public.product_eans pe ON pe.ean_norm = s.ean_norm
WHERE s.ean_norm IS NOT NULL
  AND pe.product_id IS NULL;

-- Selver rows without any EAN (need manual mapping or skip)
CREATE OR REPLACE VIEW public.v_selver_unmatched_no_ean AS
SELECT *
FROM public.staging_selver_products
WHERE ean_norm IS NULL;

COMMIT;

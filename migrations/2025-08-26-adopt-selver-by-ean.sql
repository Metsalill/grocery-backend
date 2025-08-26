-- 2025-08-26-adopt-selver-by-ean.sql
BEGIN;

-- Adopt a single Selver candidate (when it has an EAN)
CREATE OR REPLACE FUNCTION public.adopt_candidate_with_ean(_ext_id text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  c        public.selver_candidates%ROWTYPE;
  v_prod   int;
  v_store  int;
  have_map boolean;
BEGIN
  SELECT * INTO c FROM public.selver_candidates WHERE ext_id = _ext_id;
  IF NOT FOUND OR COALESCE(c.ean_norm,'') = '' THEN
    RETURN;
  END IF;

  -- single online Selver store
  SELECT id INTO v_store
  FROM public.stores
  WHERE chain='Selver' AND COALESCE(is_online,false)=true
  ORDER BY id LIMIT 1;
  IF v_store IS NULL THEN
    RETURN;
  END IF;

  -- product by EAN?
  SELECT product_id INTO v_prod
  FROM public.product_eans
  WHERE ean_norm = c.ean_norm;

  -- if not, create a minimal product and attach EAN
  IF v_prod IS NULL THEN
    INSERT INTO public.products (name)
    VALUES (NULLIF(c.name,'')) RETURNING id INTO v_prod;

    INSERT INTO public.product_eans (product_id, ean_raw)
    VALUES (v_prod, c.ean_raw)
    ON CONFLICT DO NOTHING;
  END IF;

  -- if ext_product_map exists, remember ext_id -> product
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name='ext_product_map'
  ) INTO have_map;

  IF have_map THEN
    INSERT INTO public.ext_product_map (ext_id, product_id)
    VALUES (c.ext_id, v_prod)
    ON CONFLICT (ext_id) DO UPDATE SET product_id = EXCLUDED.product_id;
  END IF;

  -- write/update the e-Selver price
  INSERT INTO public.prices (store_id, product_id, price, currency, collected_at, source_url, source)
  VALUES (v_store, v_prod, c.price, c.currency, now(), c.ext_id, 'selver:online')
  ON CONFLICT (product_id, store_id) DO UPDATE
    SET price        = EXCLUDED.price,
        currency     = EXCLUDED.currency,
        collected_at = EXCLUDED.collected_at,
        source_url   = EXCLUDED.source_url,
        source       = EXCLUDED.source;

  -- remove from candidates
  DELETE FROM public.selver_candidates WHERE ext_id = c.ext_id;
END $$;

-- Optional wrapper to adopt all candidates that already have an EAN
CREATE OR REPLACE FUNCTION public.adopt_all_selver_candidates_with_ean()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT ext_id
    FROM public.selver_candidates
    WHERE COALESCE(ean_norm,'') <> ''
  LOOP
    PERFORM public.adopt_candidate_with_ean(r.ext_id);
  END LOOP;
END $$;

-- One-time backfill now
DO $$
BEGIN
  PERFORM public.adopt_all_selver_candidates_with_ean();
END $$;

COMMIT;

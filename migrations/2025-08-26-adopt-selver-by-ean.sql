-- 2025-08-26-adopt-selver-by-ean.sql (patched)
BEGIN;

-- 1) Ensure mapping table exists; don't force a new PK if one already exists
CREATE TABLE IF NOT EXISTS public.ext_product_map (
  ext_id     text PRIMARY KEY,
  product_id int  REFERENCES public.products(id) ON DELETE CASCADE,
  source     text,
  last_seen  timestamptz DEFAULT now()
);

-- Ensure columns exist if table was older
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='ext_product_map' AND column_name='product_id'
  ) THEN
    ALTER TABLE public.ext_product_map ADD COLUMN product_id int;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='ext_product_map' AND column_name='source'
  ) THEN
    ALTER TABLE public.ext_product_map ADD COLUMN source text;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='ext_product_map' AND column_name='last_seen'
  ) THEN
    ALTER TABLE public.ext_product_map ADD COLUMN last_seen timestamptz DEFAULT now();
  END IF;
END $$;

-- Try to ensure ON CONFLICT can work: add UNIQUE on ext_id unless a PK/UNIQUE on ext_id already exists.
DO $$
DECLARE has_pkey bool; has_unique_on_ext bool;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.ext_product_map'::regclass AND contype='p'
  ) INTO has_pkey;

  SELECT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN unnest(c.conkey) k ON true
    JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k
    WHERE c.conrelid='public.ext_product_map'::regclass
      AND c.contype IN ('p','u')
      AND a.attname='ext_id'
  ) INTO has_unique_on_ext;

  IF NOT has_unique_on_ext THEN
    BEGIN
      CREATE UNIQUE INDEX IF NOT EXISTS uq_ext_product_map_ext_id
      ON public.ext_product_map(ext_id);
    EXCEPTION WHEN unique_violation THEN
      RAISE NOTICE 'Duplicates exist in ext_product_map.ext_id; continuing without unique index (functions use UPDATE→INSERT fallback).';
    END;
  END IF;
END $$;

-- 2) Functions (robust: UPDATE then INSERT)
CREATE OR REPLACE FUNCTION public.adopt_candidate_with_ean(_ext_id text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  c       public.selver_candidates%ROWTYPE;
  v_prod  int;
  v_store int;
  have_map boolean;
BEGIN
  SELECT * INTO c FROM public.selver_candidates WHERE ext_id = _ext_id;
  IF NOT FOUND OR COALESCE(c.ean_norm,'') = '' THEN
    RETURN;
  END IF;

  SELECT id INTO v_store
  FROM public.stores
  WHERE chain='Selver' AND COALESCE(is_online,false)=true
  ORDER BY id LIMIT 1;
  IF v_store IS NULL THEN
    RETURN;
  END IF;

  -- Try to find product by EAN
  SELECT product_id INTO v_prod
  FROM public.product_eans
  WHERE ean_norm = c.ean_norm;

  -- If missing, create minimal product + attach EAN
  IF v_prod IS NULL THEN
    INSERT INTO public.products (name)
    VALUES (NULLIF(c.name,'')) RETURNING id INTO v_prod;

    INSERT INTO public.product_eans (product_id, ean_raw)
    VALUES (v_prod, c.ean_raw)
    ON CONFLICT DO NOTHING;
  END IF;

  -- ext_id → product map: UPDATE, then INSERT on miss
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='ext_product_map'
  ) INTO have_map;

  IF have_map THEN
    UPDATE public.ext_product_map
       SET product_id = v_prod, source='selver', last_seen=now()
     WHERE ext_id = c.ext_id;

    IF NOT FOUND THEN
      BEGIN
        INSERT INTO public.ext_product_map (ext_id, product_id, source, last_seen)
        VALUES (c.ext_id, v_prod, 'selver', now());
      EXCEPTION WHEN unique_violation THEN
        UPDATE public.ext_product_map
           SET product_id = v_prod, source='selver', last_seen=now()
         WHERE ext_id = c.ext_id;
      END;
    END IF;
  END IF;

  -- Upsert price to the single Selver online store
  INSERT INTO public.prices (store_id, product_id, price, currency, collected_at, source_url, source)
  VALUES (v_store, v_prod, c.price, c.currency, now(), c.ext_id, 'selver:online')
  ON CONFLICT (product_id, store_id) DO UPDATE
    SET price        = EXCLUDED.price,
        currency     = EXCLUDED.currency,
        collected_at = EXCLUDED.collected_at,
        source_url   = EXCLUDED.source_url,
        source       = EXCLUDED.source;

  -- Candidate adopted -> remove it
  DELETE FROM public.selver_candidates WHERE ext_id = c.ext_id;
END $$;

CREATE OR REPLACE FUNCTION public.adopt_all_selver_candidates_with_ean()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT ext_id FROM public.selver_candidates WHERE COALESCE(ean_norm,'') <> ''
  LOOP
    PERFORM public.adopt_candidate_with_ean(r.ext_id);
  END LOOP;
END $$;

-- 3) One-time backfill
DO $$
BEGIN
  PERFORM public.adopt_all_selver_candidates_with_ean();
END $$;

COMMIT;

-- 2025-08-26-adopt-selver-by-ean.sql (fixed)
BEGIN;

-- Ensure ext_product_map exists and has a unique key on ext_id
CREATE TABLE IF NOT EXISTS public.ext_product_map (
  ext_id     text PRIMARY KEY,
  product_id int  REFERENCES public.products(id) ON DELETE CASCADE,
  source     text,
  last_seen  timestamptz DEFAULT now()
);

-- If table exists but ext_id isn’t unique/PK or product_id missing, patch it
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='ext_product_map' AND column_name='product_id'
  ) IS FALSE THEN
    ALTER TABLE public.ext_product_map ADD COLUMN product_id int;
  END IF;

  -- Add a UNIQUE index on ext_id if there is no PK/UNIQUE yet
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='public.ext_product_map'::regclass
      AND contype IN ('p','u')
      AND conkey = ARRAY[
        (SELECT attnum FROM pg_attribute WHERE attrelid='public.ext_product_map'::regclass AND attname='ext_id')
      ]
  ) THEN
    -- try to promote ext_id as primary key if possible, otherwise add a unique index
    BEGIN
      ALTER TABLE public.ext_product_map ADD CONSTRAINT ext_product_map_pkey PRIMARY KEY (ext_id);
    EXCEPTION WHEN duplicate_object THEN
      CREATE UNIQUE INDEX IF NOT EXISTS uq_ext_product_map_ext_id ON public.ext_product_map(ext_id);
    END;
  END IF;
END $$;

-- Adopt a single Selver candidate when it has an EAN
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

  SELECT id INTO v_store
  FROM public.stores
  WHERE chain='Selver' AND COALESCE(is_online,false)=true
  ORDER BY id LIMIT 1;
  IF v_store IS NULL THEN
    RETURN;
  END IF;

  -- Try to find an existing product by EAN
  SELECT product_id INTO v_prod
  FROM public.product_eans
  WHERE ean_norm = c.ean_norm;

  -- If not found, create a minimal product and attach the EAN
  IF v_prod IS NULL THEN
    INSERT INTO public.products (name)
    VALUES (NULLIF(c.name,'')) RETURNING id INTO v_prod;

    INSERT INTO public.product_eans (product_id, ean_raw)
    VALUES (v_prod, c.ean_raw)
    ON CONFLICT DO NOTHING;
  END IF;

  -- ext_id -> product map (make it robust even without UNIQUE)
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='ext_product_map'
  ) INTO have_map;

  IF have_map THEN
    UPDATE public.ext_product_map SET product_id = v_prod, last_seen = now()
    WHERE ext_id = c.ext_id;

    IF NOT FOUND THEN
      BEGIN
        INSERT INTO public.ext_product_map (ext_id, product_id, source, last_seen)
        VALUES (c.ext_id, v_prod, 'selver', now());
      EXCEPTION WHEN unique_violation THEN
        UPDATE public.ext_product_map SET product_id = v_prod, last_seen = now()
        WHERE ext_id = c.ext_id;
      END;
    END IF;
  END IF;

  -- Upsert e-Selver price
  INSERT INTO public.prices (store_id, product_id, price, currency, collected_at, source_url, source)
  VALUES (v_store, v_prod, c.price, c.currency, now(), c.ext_id, 'selver:online')
  ON CONFLICT (product_id, store_id) DO UPDATE
    SET price        = EXCLUDED.price,
        currency     = EXCLUDED.currency,
        collected_at = EXCLUDED.collected_at,
        source_url   = EXCLUDED.source_url,
        source       = EXCLUDED.source;

  -- Done with this candidate
  DELETE FROM public.selver_candidates WHERE ext_id = c.ext_id;
END $$;

-- Batch adopt all candidates that already have an EAN
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

-- One-time backfill
DO $$
BEGIN
  PERFORM public.adopt_all_selver_candidates_with_ean();
END $$;

COMMIT;

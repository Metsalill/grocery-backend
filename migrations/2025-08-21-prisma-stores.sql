-- 2025-08-21-prisma-stores.sql
-- Insert Prisma physical stores (safe to re-run) and optionally map them
-- to the Prisma Online price source if available.

BEGIN;

-- === Insert physical Prisma stores (no lat/lon yet) ===
-- Tallinna piirkond
INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Kristiine Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Kristiine Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Lasnamäe Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Lasnamäe Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Mustamäe Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Mustamäe Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Rocca al Mare Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Rocca al Mare Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Roo Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Roo Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Sikupilli Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Sikupilli Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Tiskre Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Tiskre Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Vanalinna Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Vanalinna Prisma');

-- Muud linnad
INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Maardu Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Maardu Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Narva Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Narva Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Rapla Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Rapla Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Annelinna Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Annelinna Prisma');

INSERT INTO public.stores (name, chain, is_online, lat, lon)
SELECT 'Sõbra Prisma', 'Prisma', FALSE, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM public.stores WHERE name='Sõbra Prisma');

-- === Optional: map physical Prisma stores to Prisma Online prices ===
DO $$
DECLARE online_id INT;
BEGIN
  -- only do this if the mapping table exists
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name='store_price_source'
  ) THEN
    -- try to find a Prisma online store (chain='Prisma' AND is_online)
    SELECT id INTO online_id
    FROM public.stores
    WHERE chain='Prisma' AND COALESCE(is_online, FALSE) = TRUE
    ORDER BY id
    LIMIT 1;

    IF online_id IS NOT NULL THEN
      -- map every Prisma store (non-online) to the online store as price source
      INSERT INTO public.store_price_source (store_id, source_store_id)
      SELECT s.id, online_id
      FROM public.stores s
      WHERE s.chain='Prisma'
        AND COALESCE(s.is_online, FALSE) = FALSE
        AND s.id <> online_id
        AND NOT EXISTS (
          SELECT 1 FROM public.store_price_source m WHERE m.store_id = s.id
        );
    END IF;
  END IF;
END$$;

COMMIT;

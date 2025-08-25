BEGIN;

-- Find the canonical e-Selver id
WITH online AS (
  SELECT id
  FROM public.stores
  WHERE chain='Selver' AND COALESCE(is_online, FALSE) = TRUE
  ORDER BY id
  LIMIT 1
)
INSERT INTO public.store_price_source (store_id, source_store_id)
SELECT s.id, (SELECT id FROM online)
FROM public.stores s
WHERE s.chain='Selver' AND COALESCE(s.is_online, FALSE) = FALSE
  AND NOT EXISTS (
    SELECT 1 FROM public.store_price_source m WHERE m.store_id = s.id
  );

COMMIT;

BEGIN;

CREATE OR REPLACE VIEW public.v_latest_store_prices AS
SELECT DISTINCT ON (ph.product_id, ph.store_id)
  ph.product_id,
  ph.store_id,
  ph.amount,
  ph.currency,
  ph.captured_at
FROM public.price_history ph
ORDER BY ph.product_id, ph.store_id, ph.captured_at DESC;

CREATE OR REPLACE VIEW public.v_cheapest_offer AS
SELECT lsp.product_id,
       lsp.store_id,
       lsp.amount,
       lsp.currency,
       lsp.captured_at
FROM public.v_latest_store_prices lsp
JOIN (
  SELECT product_id, MIN(amount) AS min_amount
  FROM public.v_latest_store_prices
  GROUP BY product_id
) m
  ON m.product_id = lsp.product_id
 AND m.min_amount = lsp.amount;

COMMIT;

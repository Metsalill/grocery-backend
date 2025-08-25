BEGIN;

-- Mirror from online store row in `prices` to all other stores in the same chain.
CREATE OR REPLACE FUNCTION replicate_chain_prices_from_online()
RETURNS TRIGGER AS $$
DECLARE
  ch TEXT;
BEGIN
  -- Act only for online stores
  SELECT chain INTO ch FROM stores WHERE id = NEW.store_id AND is_online IS TRUE;
  IF ch IS NULL THEN
    RETURN NEW;
  END IF;

  -- Mirror to all non-online stores in the same chain.
  INSERT INTO prices (product_id, store_id, price, currency, collected_at, source)
  SELECT NEW.product_id, s.id, NEW.price, NEW.currency, NEW.collected_at, 'mirror:' || ch || ':online'
  FROM stores s
  WHERE s.chain = ch AND COALESCE(s.is_online, FALSE) = FALSE
  ON CONFLICT (product_id, store_id)
  DO UPDATE SET
      price        = EXCLUDED.price,
      currency     = EXCLUDED.currency,
      collected_at = EXCLUDED.collected_at,
      source       = EXCLUDED.source
  -- Do NOT overwrite a store-specific physical price:
  WHERE prices.source IS DISTINCT FROM 'physical';

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prices_mirror_from_online ON prices;
CREATE TRIGGER trg_prices_mirror_from_online
AFTER INSERT OR UPDATE OF price, collected_at ON prices
FOR EACH ROW
EXECUTE FUNCTION replicate_chain_prices_from_online();

COMMIT;

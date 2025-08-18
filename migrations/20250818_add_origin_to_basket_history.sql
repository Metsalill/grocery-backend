-- Add origin coordinates to basket_history (if not present)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'basket_history' AND column_name = 'origin_lat'
  ) THEN
    ALTER TABLE basket_history ADD COLUMN origin_lat double precision;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'basket_history' AND column_name = 'origin_lon'
  ) THEN
    ALTER TABLE basket_history ADD COLUMN origin_lon double precision;
  END IF;
END $$;

-- Optional: handy index for queries filtering by user + created_at
CREATE INDEX IF NOT EXISTS idx_basket_history_user_created
  ON basket_history (user_id, created_at DESC);

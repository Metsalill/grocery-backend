-- 2025_08_12_add_basket_history_tables.sql

-- Table for storing a saved basket header
CREATE TABLE IF NOT EXISTS basket_history (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    radius_km NUMERIC(6,2),
    winner_store_id INTEGER,
    winner_store_name TEXT,
    winner_total NUMERIC(12,2),
    stores JSONB,            -- JSON array of all store results from compare
    note TEXT
);

CREATE INDEX IF NOT EXISTS idx_basket_history_user_created
    ON basket_history(user_id, created_at DESC);

-- Table for storing the individual items in a saved basket
CREATE TABLE IF NOT EXISTS basket_items (
    id BIGSERIAL PRIMARY KEY,
    basket_id BIGINT NOT NULL REFERENCES basket_history(id) ON DELETE CASCADE,
    product TEXT NOT NULL,
    quantity NUMERIC(10,3) DEFAULT 1,
    unit TEXT,
    price NUMERIC(12,2),     -- unit price at the winner store
    line_total NUMERIC(12,2),
    store_id INTEGER,        -- store that this product's price came from (winner)
    store_name TEXT,
    image_url TEXT,
    brand TEXT,
    size_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_basket_items_basket
    ON basket_items(basket_id);

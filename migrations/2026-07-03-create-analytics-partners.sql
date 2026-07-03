-- 2026-07-03-create-analytics-partners.sql
-- Tootja/kett-partnerite haldus analüütika-dashboard'i jaoks. Kettide
-- praegused tokenid jäävad muutmata env-muutujatesse (ANALYTICS_TOKEN_*)
-- — see tabel on uus, paralleelne süsteem, mõeldud eelkõige tootjatele
-- (brand_filter), kuna neid võib tulevikus koguneda palju rohkem kui
-- praegust 5 ketti.

CREATE TABLE IF NOT EXISTS analytics_partners (
    id SERIAL PRIMARY KEY,
    partner_type TEXT NOT NULL CHECK (partner_type IN ('retailer', 'brand')),
    name TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    brand_filter TEXT[],
    chain_filter TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_partners_token
ON analytics_partners (token);

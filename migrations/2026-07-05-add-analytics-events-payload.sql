SET client_encoding = 'UTF8';

ALTER TABLE analytics_events
ADD COLUMN IF NOT EXISTS payload JSONB;

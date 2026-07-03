SET client_encoding = 'UTF8';

ALTER TABLE analytics_events
ADD COLUMN IF NOT EXISTS device_key TEXT;

CREATE INDEX IF NOT EXISTS idx_analytics_events_device_key
ON analytics_events (device_key)
WHERE device_key IS NOT NULL;

-- Kontroll: veerg peaks olema olemas ja NULL kõikidel varasematel ridadel
-- (device_key hakkab täituma alles pärast Flutteri ja backend'i deploy'i)
SELECT COUNT(*) AS total_rows,
       COUNT(device_key) AS rows_with_device_key
FROM analytics_events;

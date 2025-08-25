BEGIN;

-- Insert the online store (price source)
INSERT INTO stores (name, chain, is_online, lat, lon)
VALUES ('e-Selver', 'Selver', TRUE, NULL, NULL)
ON CONFLICT DO NOTHING;

COMMIT;

-- =============================================================
-- build_product_groups.sql
-- SQL equivalent of scripts/build_product_groups.py
--
-- Run this in your psql terminal.
-- Safe to re-run: clears and rebuilds groups each time.
-- =============================================================

-- -------------------------------------------------------------
-- STEP 1: Create the normalize() function in Postgres
-- (mirrors the Python normalize() function exactly)
-- -------------------------------------------------------------

CREATE OR REPLACE FUNCTION normalize_product_name(name TEXT)
RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    s TEXT;
    prev TEXT;
    i INT;
BEGIN
    s := lower(trim(name));

    -- Strip chain prefixes
    s := regexp_replace(s, '^https\s+barbora\s+ee\s+toode\s+', '', 'i');
    s := regexp_replace(s, '^coop\s+',   '', 'i');
    s := regexp_replace(s, '^rimi\s+',   '', 'i');
    s := regexp_replace(s, '^selver\s+', '', 'i');
    s := regexp_replace(s, '^prisma\s+', '', 'i');
    s := regexp_replace(s, '^maxima\s+', '', 'i');
    s := regexp_replace(s, '^lidl\s+',   '', 'i');

    -- Strip size/class suffixes — repeat up to 3 times until stable
    FOR i IN 1..3 LOOP
        prev := s;
        s := trim(regexp_replace(s, '\s+\d+\s*kl\.?,?\s*kg$',  '', 'i'));
        s := trim(regexp_replace(s, '\s+i+\s+klass?\y',         '', 'i'));
        s := trim(regexp_replace(s, '\s+\d+\s*kg$',             '', 'i'));
        s := trim(regexp_replace(s, ',\s*kg$',                  '', 'i'));
        s := trim(regexp_replace(s, '\s+kg$',                   '', 'i'));
        s := trim(regexp_replace(s, '\s+kl\s+kg$',              '', 'i'));
        s := trim(regexp_replace(s, '\s+pakitud.*$',             '', 'i'));
        EXIT WHEN s = prev;
    END LOOP;

    -- Collapse internal whitespace
    s := trim(regexp_replace(s, '\s+', ' ', 'g'));

    RETURN s;
END;
$$;


-- -------------------------------------------------------------
-- STEP 2: Preview what will be grouped (run this first!)
-- Check the output looks sane before inserting anything.
-- -------------------------------------------------------------

SELECT
    normalize_product_name(name) AS canon,
    sub_code,
    COUNT(*)                      AS member_count,
    array_agg(id)                 AS product_ids,
    array_agg(name)               AS product_names
FROM products
WHERE sub_code LIKE 'produce_%'
  AND (
      name ILIKE '% kg%'
   OR name ILIKE '%, kg'
   OR name ILIKE '%kl kg%'
   OR name ILIKE '%kl., kg%'
   OR name ILIKE '% Kg'
  )
GROUP BY normalize_product_name(name), sub_code
HAVING COUNT(*) >= 2
ORDER BY member_count DESC, canon
LIMIT 30;


-- -------------------------------------------------------------
-- STEP 3: When preview looks good — run the INSERT block below
-- (Clear existing data and rebuild)
-- -------------------------------------------------------------

BEGIN;

DELETE FROM product_group_members;
DELETE FROM product_groups;

-- Insert groups and members in one go using a CTE
WITH candidates AS (
    SELECT
        id AS product_id,
        normalize_product_name(name) AS canon,
        sub_code
    FROM products
    WHERE sub_code LIKE 'produce_%'
      AND (
          name ILIKE '% kg%'
       OR name ILIKE '%, kg'
       OR name ILIKE '%kl kg%'
       OR name ILIKE '%kl., kg%'
       OR name ILIKE '% Kg'
      )
),
groups AS (
    SELECT canon, sub_code, array_agg(product_id) AS pids
    FROM candidates
    GROUP BY canon, sub_code
    HAVING COUNT(*) >= 2
),
inserted_groups AS (
    INSERT INTO product_groups (canonical_name, sub_code, unit)
    SELECT canon, sub_code, 'kg'
    FROM groups
    RETURNING id, canonical_name, sub_code
)
INSERT INTO product_group_members (group_id, product_id)
SELECT
    ig.id,
    unnest(g.pids)
FROM inserted_groups ig
JOIN groups g
  ON g.canon    = ig.canonical_name
 AND g.sub_code = ig.sub_code
ON CONFLICT DO NOTHING;

COMMIT;


-- -------------------------------------------------------------
-- STEP 4: Verification
-- -------------------------------------------------------------

SELECT
    'product_groups'        AS tbl,
    COUNT(*)                AS rows
FROM product_groups
UNION ALL
SELECT
    'product_group_members' AS tbl,
    COUNT(*)                AS rows
FROM product_group_members;

-- Sample: show 10 groups with their members
SELECT
    pg.id,
    pg.canonical_name,
    pg.sub_code,
    COUNT(pgm.product_id) AS member_count,
    array_agg(p.name)     AS member_names
FROM product_groups pg
JOIN product_group_members pgm ON pgm.group_id = pg.id
JOIN products p                ON p.id = pgm.product_id
GROUP BY pg.id, pg.canonical_name, pg.sub_code
ORDER BY member_count DESC
LIMIT 10;

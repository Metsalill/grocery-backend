-- 2025-11-25-categories-main.sql
-- Schema + main categories (Selver-style)

BEGIN;

-- 1) Main categories
CREATE TABLE IF NOT EXISTS categories_main (
    id        SERIAL PRIMARY KEY,
    code      TEXT NOT NULL UNIQUE,   -- stable EN-ish code
    label_et  TEXT NOT NULL,          -- Estonian label
    label_en  TEXT,                   -- Optional English label
    sort_order INT NOT NULL DEFAULT 0
);

-- 2) Subcategories (empty for now; we’ll fill later)
CREATE TABLE IF NOT EXISTS categories_sub (
    id        SERIAL PRIMARY KEY,
    main_id   INT NOT NULL REFERENCES categories_main(id) ON DELETE CASCADE,
    code      TEXT NOT NULL,
    label_et  TEXT NOT NULL,
    label_en  TEXT,
    sort_order INT NOT NULL DEFAULT 0,
    UNIQUE (main_id, code)
);

-- 3) Mapping canonical products → one (main, sub) category
CREATE TABLE IF NOT EXISTS product_categories (
    product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    main_id    INT NOT NULL REFERENCES categories_main(id),
    sub_id     INT REFERENCES categories_sub(id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_product_categories_main
    ON product_categories(main_id);

CREATE INDEX IF NOT EXISTS idx_product_categories_sub
    ON product_categories(sub_id);

-- 4) Insert / upsert MAIN CATEGORIES (Selver-style)
INSERT INTO categories_main (code, label_et, label_en, sort_order) VALUES
    ('produce',        'Puu- ja köögiviljad',                 'Fruit & vegetables',          10),
    ('meat_fish',      'Liha- ja kalatooted',                 'Meat & fish',                 20),
    ('dairy_eggs_fats','Piimatooted, munad, võid',            'Dairy, eggs & fats',          30),
    ('cheese',         'Juustud',                             'Cheese',                      40),
    ('bakery',         'Leivad, saiad, kondiitritooted',      'Bread, bakery & pastries',    50),
    ('ready_meals',    'Valmistoidud',                        'Ready meals',                 60),
    ('dry_preserves',  'Kuivained, hommikusöögid, hoidised',  'Dry goods & preserves',       80),
    ('world_spices',   'Maailma köök, maitseained, puljongid','World foods & spices',        90),
    ('sauces_oils',    'Kastmed, õlid',                       'Sauces & oils',               100),
    ('sweets_snacks',  'Maiustused, küpsised, näksid',        'Sweets, biscuits & snacks',   110),
    ('frozen_food',    'Külmutatud toidukaubad',              'Frozen foods',                120),
    ('drinks',         'Joogid',                              'Drinks',                      130),
    ('baby',           'Lastekaubad',                         'Baby & kids',                 140),
    ('pet',            'Lemmikloomakaubad',                   'Pet supplies',                150),
    ('personal_care',  'Enesehooldustarbed',                  'Personal care & beauty',      160),
    ('household',      'Majapidamis- ja kodukaubad',          'Household & home',            170),
    ('other',          'Muu',                                 'Other',                       999)
ON CONFLICT (code) DO UPDATE
SET
    label_et  = EXCLUDED.label_et,
    label_en  = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

COMMIT;

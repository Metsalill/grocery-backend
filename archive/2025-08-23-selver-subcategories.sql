-- 2025-08-23-selver-subcategories.sql
-- Seed Selver-style subcategories under our 17 main categories.
-- Idempotent: uses INSERT .. SELECT .. ON CONFLICT (main_id, code) DO UPDATE.

BEGIN;

-- Helper: small function to fetch main_id by code.
-- (We don't actually need a function; just SELECT in each INSERT.)

----------------------------
-- 1) Puu- ja köögiviljad --
----------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_apples_pears', 'Õunad, pirnid', 'Apples & pears', 10
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_tropical_exotic', 'Troopilised, eksootilised viljad', 'Tropical & exotic fruit', 20
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_vegetables', 'Köögiviljad, juurviljad', 'Vegetables & roots', 30
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_mushrooms', 'Seened', 'Mushrooms', 40
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_herbs_salads', 'Maitsetaimed, värsked salatid, piprad, idud', 'Herbs, fresh salads, peppers, sprouts', 50
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_fruit_salads', 'Puuviljasalatid', 'Fruit salads', 60
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_berries', 'Marjad', 'Berries', 70
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'produce_smoothies_juices', 'Smuutid, värsked mahlad', 'Smoothies & fresh juices', 80
FROM categories_main WHERE code = 'produce'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

--------------------------
-- 2) Liha- ja kalatooted
--------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_pork', 'Sealiha', 'Pork', 10
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_poultry', 'Linnuliha', 'Poultry', 20
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_beef_lamb_game', 'Veise-, lamba- ja ulukiliha', 'Beef, lamb & game', 30
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_minced', 'Hakkliha', 'Minced meat', 40
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_sausages', 'Keedu- ja suitsuvorstid, viinerid', 'Boiled & smoked sausages', 50
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_hams', 'Singid, rulaadid', 'Hams & roulades', 60
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_other', 'Muud lihatooted', 'Other meat products', 70
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_grill_blood_sausages', 'Grillvorstid, verivorstid', 'Grill sausages & blood sausages', 80
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'meat_gourmet', 'Gurmee lihatooted', 'Gourmet meat', 90
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'fish_fresh', 'Värske kala, mereannid', 'Fresh fish & seafood', 100
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'fish_salted_smoked', 'Soolatud ja suitsutatud kalatooted', 'Salted & smoked fish', 110
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'fish_processed', 'Töödeldud mereannid', 'Processed seafood', 120
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'fish_other', 'Muud kalatooted', 'Other fish products', 130
FROM categories_main WHERE code = 'meat_fish'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

--------------------------------
-- 3) Piimatooted, munad, võid --
--------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_milks_creams', 'Piimad, koored', 'Milks & creams', 10
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_curd_cottage', 'Kohupiimad, kodujuustud', 'Curds & cottage cheese', 20
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_yogurts', 'Jogurtid, jogurtijoogid', 'Yoghurts & yogurt drinks', 30
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_curd_snacks', 'Kohukesed', 'Curd snacks', 40
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_other_desserts', 'Muud magustoidud', 'Other desserts', 50
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_eggs', 'Munad', 'Eggs', 60
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dairy_butter_margarine', 'Võid, margariinid', 'Butter & margarines', 70
FROM categories_main WHERE code = 'dairy_eggs_fats'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

------------ 
-- 4) Juustud
------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'cheese_regular', 'Juustud', 'Cheeses', 10
FROM categories_main WHERE code = 'cheese'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'cheese_spreadable', 'Määrdejuustud', 'Spreadable cheeses', 20
FROM categories_main WHERE code = 'cheese'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'cheese_delicacy', 'Delikatessjuustud', 'Delicacy cheeses', 30
FROM categories_main WHERE code = 'cheese'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

--------------------------------------
-- 5) Leivad, saiad, kondiitritooted --
--------------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_breads', 'Leivad', 'Breads', 10
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_white_breads', 'Saiad', 'White breads', 20
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_rolls_flatbreads', 'Sepikud, kuklid, lavašid', 'Dark breads, rolls & flatbreads', 30
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_crispbreads', 'Näkileivad', 'Crispbreads', 40
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_selver_bakery', 'Selveri Pagarid', 'Selver bakery', 50
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_cakes', 'Tordid', 'Cakes', 60
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_pastries_doughs', 'Koogid, rullbiskviidid, tainad', 'Pastries, swiss rolls & doughs', 70
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'bakery_sweet_pastries', 'Saiakesed, stritslid, kringlid', 'Sweet pastries & kringels', 80
FROM categories_main WHERE code = 'bakery'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

--------------------
-- 6) Valmistoidud
--------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'ready_salads', 'Salatid', 'Salads', 10
FROM categories_main WHERE code = 'ready_meals'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'ready_chilled_meals', 'Jahutatud valmistoidud', 'Chilled ready meals', 20
FROM categories_main WHERE code = 'ready_meals'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'ready_desserts', 'Magustoidud', 'Desserts', 30
FROM categories_main WHERE code = 'ready_meals'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'ready_sushi', 'Sushi', 'Sushi', 40
FROM categories_main WHERE code = 'ready_meals'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

-----------------------------------------------
-- 7) Kuivained, hommikusöögid, hoidised (dry)
-----------------------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dry_breakfast', 'Kuivained, hommikusöögid', 'Dry goods & breakfast', 10
FROM categories_main WHERE code = 'dry_preserves'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'dry_preserves_canned', 'Hoidised', 'Preserves & canned goods', 20
FROM categories_main WHERE code = 'dry_preserves'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

------------------------------------------
-- 8) Maailma köök, maitseained, puljongid
------------------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'spices_seasonings', 'Maitseained', 'Spices & seasonings', 10
FROM categories_main WHERE code = 'world_spices'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'world_cuisine', 'Maailma köök', 'World cuisine', 20
FROM categories_main WHERE code = 'world_spices'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'stocks_broths', 'Puljongid', 'Stocks & broths', 30
FROM categories_main WHERE code = 'world_spices'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

-------------------
-- 9) Kastmed, õlid
-------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'oils_vinegars', 'Õlid, äädikad', 'Oils & vinegars', 10
FROM categories_main WHERE code = 'sauces_oils'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'mayonnaise_mustard', 'Majoneesid, sinepid', 'Mayonnaises & mustards', 20
FROM categories_main WHERE code = 'sauces_oils'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'ketchups_sauces', 'Ketšupid, tomatipastad, kastmed', 'Ketchups, tomato pastes & sauces', 30
FROM categories_main WHERE code = 'sauces_oils'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'gourmet_sauces', 'Gurmee kastmed', 'Gourmet sauces', 40
FROM categories_main WHERE code = 'sauces_oils'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

-------------------------------------------
-- 10) Maiustused, küpsised, näksid (sweets)
-------------------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_candy_bags', 'Kommipakid', 'Candy bags', 10
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_candy_boxes', 'Kommikarbid', 'Candy boxes', 20
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_chocolate', 'Šokolaadid', 'Chocolates', 30
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_gum_pastilles', 'Nätsud, pastillid', 'Chewing gum & pastilles', 40
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_other', 'Muud maiustused', 'Other sweets', 50
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_cookies', 'Küpsised', 'Cookies & biscuits', 60
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_crispbreads_snacks', 'Näkileivad', 'Crispbreads & savoury snacks', 70
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_nuts_dried_fruit', 'Pähklid ja kuivatatud puuviljad', 'Nuts & dried fruit', 80
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_crisps', 'Sipsid', 'Crisps', 90
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'sweets_seasonal', 'Tähtpäeva maiustused', 'Seasonal sweets', 100
FROM categories_main WHERE code = 'sweets_snacks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

-----------------------------
-- 11) Külmutatud toidukaubad
-----------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'frozen_meat_fish', 'Külmutatud liha- ja kalatooted', 'Frozen meat & fish', 10
FROM categories_main WHERE code = 'frozen_food'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'frozen_ready_meals', 'Külmutatud valmistooted', 'Frozen ready meals', 20
FROM categories_main WHERE code = 'frozen_food'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'frozen_veg_berries', 'Külmutatud köögiviljad, marjad, puuviljad', 'Frozen vegetables, berries & fruit', 30
FROM categories_main WHERE code = 'frozen_food'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'frozen_dough_bakery', 'Külmutatud tainad ja kondiitritooted', 'Frozen dough & bakery', 40
FROM categories_main WHERE code = 'frozen_food'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'ice_cream', 'Jäätised', 'Ice creams', 50
FROM categories_main WHERE code = 'frozen_food'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

-------------
-- 12) Joogid
-------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_water_juices', 'Veed, mahlad, siirupid, smuutid', 'Water, juices, syrups, smoothies', 10
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_soft_energy', 'Karastus- ja energiajoogid, toonikud', 'Soft & energy drinks, tonics', 20
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_sport', 'Spordijoogid, pulbrid, batoonid', 'Sports drinks, powders & bars', 30
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_coffee_tea_cocoa', 'Kohv, tee, kakao', 'Coffee, tea & cocoa', 40
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_low_alcohol', 'Lahja alkohol', 'Low-alcohol drinks', 50
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_strong_alcohol', 'Kange alkohol', 'Strong alcohol', 60
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'drinks_lighters_matches', 'Välgumihklid ja tikud', 'Lighters & matches', 70
FROM categories_main WHERE code = 'drinks'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

----------------
-- 13) Lapsed --
----------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'baby_food', 'Lastetooted', 'Baby food', 10
FROM categories_main WHERE code = 'baby'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'baby_diapers', 'Mähkmed', 'Diapers', 20
FROM categories_main WHERE code = 'baby'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'baby_care', 'Beebi hooldusvahendid', 'Baby care products', 30
FROM categories_main WHERE code = 'baby'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'baby_accessories', 'Tarvikud', 'Accessories', 40
FROM categories_main WHERE code = 'baby'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'baby_toys', 'Mänguasjad', 'Toys', 50
FROM categories_main WHERE code = 'baby'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'baby_socks_underwear', 'Laste sokid, sukad, pesu', 'Kids socks & underwear', 60
FROM categories_main WHERE code = 'baby'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

------------------------
-- 14) Lemmikloomad --
------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'pet_cat_food', 'Kassitoidud', 'Cat foods', 10
FROM categories_main WHERE code = 'pet'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'pet_dog_food', 'Koeratoidud', 'Dog foods', 20
FROM categories_main WHERE code = 'pet'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'pet_small_animal_food', 'Väikeloomatoidud', 'Small animal foods', 30
FROM categories_main WHERE code = 'pet'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'pet_fish_bird_food', 'Kala- ja linnutoidud', 'Fish & bird foods', 40
FROM categories_main WHERE code = 'pet'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'pet_accessories', 'Lemmikloomatarbed', 'Pet accessories', 50
FROM categories_main WHERE code = 'pet'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

------------------------------
-- 15) Enesehooldustarbed --
------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'personal_oral_care', 'Suuhooldus', 'Oral care', 10
FROM categories_main WHERE code = 'personal_care'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'personal_face_care', 'Näohooldus', 'Face care', 20
FROM categories_main WHERE code = 'personal_care'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'personal_hair_care', 'Juuksehooldus', 'Hair care', 30
FROM categories_main WHERE code = 'personal_care'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'personal_body_care', 'Kehahooldus', 'Body care', 40
FROM categories_main WHERE code = 'personal_care'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'personal_decorative_cosmetics', 'Dekoratiivkosmeetika tooted', 'Decorative cosmetics', 50
FROM categories_main WHERE code = 'personal_care'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'personal_health_goods', 'Tervisekaubad', 'Health goods', 60
FROM categories_main WHERE code = 'personal_care'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

-----------------------------------
-- 16) Majapidamis- ja kodukaubad
-----------------------------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'household_paper', 'Paberitooted', 'Paper products', 10
FROM categories_main WHERE code = 'household'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'household_cleaning', 'Puhastus-ja koristusvahendid', 'Cleaning products', 20
FROM categories_main WHERE code = 'household'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'household_other', 'Muud majapidamistarbed', 'Other household goods', 30
FROM categories_main WHERE code = 'household'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

----------------
-- 17) Muu/Other
----------------

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'other_bulk_packs', 'Suurpakendid', 'Bulk packs', 10
FROM categories_main WHERE code = 'other'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

INSERT INTO categories_sub (main_id, code, label_et, label_en, sort_order)
SELECT id, 'other_misc', 'Muu', 'Other', 20
FROM categories_main WHERE code = 'other'
ON CONFLICT (main_id, code) DO UPDATE
SET label_et = EXCLUDED.label_et,
    label_en = EXCLUDED.label_en,
    sort_order = EXCLUDED.sort_order;

COMMIT;

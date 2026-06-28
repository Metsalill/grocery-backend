# categories.py
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

router = APIRouter(prefix="/categories", tags=["categories"])
bearer_scheme = HTTPBearer(auto_error=False)

async def get_db(request: Request):
    conn = getattr(request.app.state, "db", None)
    if conn is None:
        raise HTTPException(status_code=500, detail="DB pool not available")
    return conn

# ─── Translations (no DB column needed) ───────────────────────────────────

_MAIN_RU = {
    'produce': 'Фрукты и овощи',
    'meat_fish': 'Мясо и рыба',
    'dairy_eggs_fats': 'Молочные продукты, яйца, масло',
    'cheese': 'Сыры',
    'bakery': 'Хлеб и выпечка',
    'frozen_food': 'Замороженные продукты',
    'dry_preserves': 'Сухие продукты и консервы',
    'drinks': 'Безалкогольные напитки',
    'alcohol': 'Алкоголь',
    'coffee': 'Кофе и чай',
    'sweets_snacks': 'Сладости и снеки',
    'world_spices': 'Специи и приправы',
    'sauces_oils': 'Соусы и масла',
    'baby': 'Детские товары',
    'personal_care': 'Гигиена и уход',
    'household': 'Бытовые товары',
    'pet': 'Корм для животных',
}

_MAIN_EN = {
    'produce': 'Fruit & vegetables',
    'meat_fish': 'Meat & fish',
    'dairy_eggs_fats': 'Dairy, eggs & fats',
    'cheese': 'Cheese',
    'bakery': 'Bread & bakery',
    'frozen_food': 'Frozen food',
    'dry_preserves': 'Dry goods & preserves',
    'drinks': 'Non-alcoholic drinks',
    'alcohol': 'Alcohol',
    'coffee': 'Coffee & tea',
    'sweets_snacks': 'Sweets & snacks',
    'world_spices': 'Spices & seasonings',
    'sauces_oils': 'Sauces & oils',
    'baby': 'Baby products',
    'personal_care': 'Personal care',
    'household': 'Household',
    'pet': 'Pet food',
}

_SUB_RU = {
    # produce
    'produce_root_veg': 'Корнеплоды',
    'produce_apples_pears': 'Яблоки и груши',
    'produce_tropical': 'Тропические фрукты',
    'produce_berries': 'Ягоды',
    'produce_herbs_salads_sprouts': 'Зелень и салаты',
    'produce_mushrooms': 'Грибы',
    'produce_fruit_salads': 'Фруктовые салаты',
    'produce_smoothies_fresh_juices': 'Смузи и свежие соки',
    # meat
    'meat_pork': 'Свинина',
    'meat_poultry': 'Птица',
    'meat_beef_lamb_game': 'Говядина, баранина, дичь',
    'meat_minced': 'Фарш',
    'meat_sausages': 'Колбасы',
    'meat_hams': 'Ветчина',
    'meat_grill_blood_sausages': 'Гриль и кровяная колбаса',
    'meat_gourmet': 'Деликатесы',
    'meat_other': 'Мясо прочее',
    # fish
    'fish_fresh': 'Свежая рыба',
    'fish_salted_smoked': 'Соленая и копченая рыба',
    'fish_processed': 'Рыбные продукты',
    'fish_other': 'Рыба прочее',
    # dairy
    'dairy_milk': 'Молоко',
    'dairy_plant_drinks': 'Растительные напитки',
    'dairy_yogurt_kefir': 'Йогурт и кефир',
    'dairy_cottage_quark': 'Творог',
    'dairy_cream_sourcream': 'Сливки и сметана',
    'dairy_butter_margarine': 'Масло и маргарин',
    'dairy_eggs': 'Яйца',
    'dairy_desserts': 'Молочные десерты',
    'dairy_other': 'Молочные продукты прочее',
    # cheese
    'cheese_regular': 'Сыр',
    'cheese_delicatessen': 'Деликатесный сыр',
    'dairy_cheese_slices': 'Нарезанный сыр',
    # bakery
    'bakery_bread_loaves': 'Хлеб',
    'bakery_buns_croissants': 'Булочки и круассаны',
    'bakery_cakes_pastries': 'Торты и пирожные',
    'bakery_flatbreads_wraps': 'Лепешки и обертки',
    'bakery_sweet_buns_donuts': 'Сладкие булочки и пончики',
    'bakery_gluten_free': 'Безглютеновая выпечка',
    'bakery_other': 'Выпечка прочее',
    # alcohol/wine
    'wine_red': 'Красное вино',
    'wine_white': 'Белое вино',
    'wine_rose': 'Розовое вино',
    'wine_sparkling': 'Игристое вино',
    'wine_sweet': 'Десертное вино',
    'drinks_beer_cider': 'Пиво и сидр',
    'drinks_spirits': 'Крепкие напитки',
    'spirits_vodka': 'Водка',
    'spirits_whisky': 'Виски и бурбон',
    'spirits_gin': 'Джин',
    'spirits_rum': 'Ром',
    'spirits_cognac': 'Коньяк и бренди',
    'spirits_liqueur': 'Ликёры',
    'spirits_other': 'Другие крепкие напитки',
    # drinks
    'drinks_water': 'Вода',
    'drinks_soft_soda': 'Газировка',
    'drinks_juices': 'Соки',
    'drinks_energy': 'Энергетики',
    'drinks_sport': 'Спортивные напитки',
    'drinks_syrups_powders': 'Сиропы и порошки',
    'drinks_other': 'Напитки прочее',
    'drinks_non_alcoholic': 'Безалкогольные напитки',
    # coffee/tea
    'coffee_beans_ground': 'Зерновой и молотый кофе',
    'coffee_capsules': 'Кофе в капсулах',
    'coffee_instant': 'Растворимый кофе',
    'tea': 'Чай',
    # sweets
    'sweets_chocolate_bars': 'Шоколад',
    'sweets_candies': 'Конфеты',
    'sweets_biscuits_cookies': 'Печенье',
    'sweets_waffles_cakes': 'Вафли и торты',
    'sweets_snacks_salty': 'Соленые снеки',
    'sweets_nuts_driedfruit': 'Орехи и сухофрукты',
    'sweets_gums_mints': 'Жвачка и мятные конфеты',
    'sweets_other': 'Сладости прочее',
    # household
    'hh_cleaners': 'Чистящие средства',
    'hh_laundry': 'Стиральные средства',
    'hh_dishwashing': 'Средства для мытья посуды',
    'hh_paper': 'Бумажные изделия',
    'hh_bins_bags': 'Мешки и контейнеры',
    'hh_candles_airfresh': 'Свечи и освежители',
    'hh_kitchenware': 'Кухонные принадлежности',
    'hh_other': 'Бытовые товары прочее',
    # personal care
    'pcare_shampoo_cond': 'Шампунь и кондиционер',
    'pcare_body_wash': 'Гель для душа',
    'pcare_soap': 'Мыло',
    'pcare_deo': 'Дезодорант',
    'pcare_oral_care': 'Уход за полостью рта',
    'pcare_shaving': 'Бритьё',
    'pcare_feminine_hygiene': 'Женская гигиена',
    'pcare_cosmetics': 'Косметика',
    'pcare_other': 'Уход прочее',
    # pet
    'pet_cat_dry': 'Сухой корм для кошек',
    'pet_cat_wet': 'Влажный корм для кошек',
    'pet_cat_treats': 'Лакомства для кошек',
    'pet_dog_dry': 'Сухой корм для собак',
    'pet_dog_wet': 'Влажный корм для собак',
    'pet_dog_treats': 'Лакомства для собак',
    'pet_litter_sand': 'Наполнитель для лотка',
    'pet_birds_fish': 'Корм для птиц и рыб',
    'pet_small_animals': 'Корм для грызунов',
    'pet_other': 'Зоотовары прочее',
    # baby
    'baby_food_jars': 'Детское питание (банки)',
    'baby_food_pouches': 'Детское питание (пюре)',
    'baby_formula': 'Молочная смесь',
    'baby_porridge_cereal': 'Каши и злаки',
    'baby_snacks': 'Детские снеки',
    'baby_diapers': 'Подгузники',
    'baby_wipes': 'Влажные салфетки',
    'baby_care': 'Уход за малышом',
    'baby_other': 'Детские товары прочее',
    # dry
    'dry_pasta_rice': 'Макароны и рис',
    'dry_groats_beans': 'Крупы и бобовые',
    'dry_flour_sugar_baking': 'Мука, сахар, выпечка',
    'dry_cereals_muesli': 'Хлопья и мюсли',
    'dry_soups_noodles': 'Супы и лапша',
    'dry_canned_meat_fish': 'Консервы мясные и рыбные',
    'dry_canned_veg': 'Консервы овощные',
    'dry_canned_fruit': 'Консервы фруктовые',
    'dry_ready_meals_jars': 'Готовые блюда',
    'dry_other': 'Сухие продукты прочее',
    # frozen
    'frozen_meat': 'Замороженное мясо',
    'frozen_fish': 'Замороженная рыба',
    'frozen_veg': 'Замороженные овощи',
    'frozen_berries_fruit': 'Замороженные ягоды и фрукты',
    'frozen_pizza': 'Пицца',
    'frozen_ready_meals': 'Готовые замороженные блюда',
    'frozen_potato_products': 'Картофельные продукты',
    'frozen_bakery': 'Замороженная выпечка',
    'frozen_desserts_icecream': 'Мороженое и десерты',
    'frozen_other': 'Замороженное прочее',
    # spices/sauces/oils
    'spices_basic_salt_pepper': 'Соль и перец',
    'spices_herbs_spice_mix': 'Специи и травы',
    'spices_broth_stock': 'Бульон и приправы',
    'spices_asian': 'Азиатские специи',
    'spices_mexican': 'Мексиканские специи',
    'spices_other': 'Специи прочее',
    'oils_olive': 'Оливковое масло',
    'oils_other': 'Масло прочее',
    'oils_vinegar': 'Уксус',
    'sauces_ketchup_mayo': 'Кетчуп и майонез',
    'sauces_pasta_cooking': 'Соусы для пасты',
    'sauces_salad_dressings': 'Заправки для салатов',
    'sauces_marinades': 'Маринады',
    'sauces_soy_worcester': 'Соевый и вустерский соус',
    'sauces_other': 'Соусы прочее',
}

_SUB_EN = {
    # produce
    'produce_root_veg': 'Root vegetables',
    'produce_apples_pears': 'Apples & pears',
    'produce_tropical': 'Tropical & exotic fruit',
    'produce_berries': 'Berries',
    'produce_herbs_salads_sprouts': 'Herbs, salads & sprouts',
    'produce_mushrooms': 'Mushrooms',
    'produce_fruit_salads': 'Fruit salads',
    'produce_smoothies_fresh_juices': 'Smoothies & fresh juices',
    # meat
    'meat_pork': 'Pork',
    'meat_poultry': 'Poultry',
    'meat_beef_lamb_game': 'Beef, lamb & game',
    'meat_minced': 'Minced meat',
    'meat_sausages': 'Sausages',
    'meat_hams': 'Ham & rolls',
    'meat_grill_blood_sausages': 'Grill & blood sausages',
    'meat_gourmet': 'Gourmet meat products',
    'meat_other': 'Other meat products',
    # fish
    'fish_fresh': 'Fresh fish & seafood',
    'fish_salted_smoked': 'Salted & smoked fish',
    'fish_processed': 'Processed fish products',
    'fish_other': 'Other fish',
    # dairy
    'dairy_milk': 'Milk',
    'dairy_plant_drinks': 'Plant-based drinks',
    'dairy_yogurt_kefir': 'Yoghurt & kefir',
    'dairy_cottage_quark': 'Cottage cheese & quark',
    'dairy_cream_sourcream': 'Cream & sour cream',
    'dairy_butter_margarine': 'Butter & margarine',
    'dairy_eggs': 'Eggs',
    'dairy_desserts': 'Dairy desserts',
    'dairy_other': 'Other dairy',
    # cheese
    'cheese_regular': 'Cheese',
    'cheese_delicatessen': 'Delicatessen cheese',
    'dairy_cheese_slices': 'Sliced cheese',
    # bakery
    'bakery_bread_loaves': 'Bread loaves',
    'bakery_buns_croissants': 'Buns & croissants',
    'bakery_cakes_pastries': 'Cakes & pastries',
    'bakery_flatbreads_wraps': 'Flatbreads & wraps',
    'bakery_sweet_buns_donuts': 'Sweet buns & donuts',
    'bakery_gluten_free': 'Gluten-free bakery',
    'bakery_other': 'Other bakery',
    # alcohol/wine
    'wine_red': 'Red wine',
    'wine_white': 'White wine',
    'wine_rose': 'Rosé wine',
    'wine_sparkling': 'Sparkling wine',
    'wine_sweet': 'Dessert wine',
    'drinks_beer_cider': 'Beer & cider',
    'drinks_spirits': 'Spirits',
    'spirits_vodka': 'Vodka',
    'spirits_whisky': 'Whisky & bourbon',
    'spirits_gin': 'Gin',
    'spirits_rum': 'Rum',
    'spirits_cognac': 'Cognac & brandy',
    'spirits_liqueur': 'Liqueurs',
    'spirits_other': 'Other spirits',
    # drinks
    'drinks_water': 'Water',
    'drinks_soft_soda': 'Soft drinks & soda',
    'drinks_juices': 'Juices',
    'drinks_energy': 'Energy drinks',
    'drinks_sport': 'Sports drinks',
    'drinks_syrups_powders': 'Syrups & powders',
    'drinks_other': 'Other drinks',
    'drinks_non_alcoholic': 'Non-alcoholic drinks',
    # coffee/tea
    'coffee_beans_ground': 'Coffee beans & ground coffee',
    'coffee_capsules': 'Coffee capsules',
    'coffee_instant': 'Instant coffee',
    'tea': 'Tea',
    # sweets
    'sweets_chocolate_bars': 'Chocolate',
    'sweets_candies': 'Candies',
    'sweets_biscuits_cookies': 'Biscuits & cookies',
    'sweets_waffles_cakes': 'Waffles & cakes',
    'sweets_snacks_salty': 'Salty snacks',
    'sweets_nuts_driedfruit': 'Nuts & dried fruit',
    'sweets_gums_mints': 'Gum & mints',
    'sweets_other': 'Other sweets',
    # household
    'hh_cleaners': 'Cleaning products',
    'hh_laundry': 'Laundry products',
    'hh_dishwashing': 'Dishwashing products',
    'hh_paper': 'Paper products',
    'hh_bins_bags': 'Bins & bags',
    'hh_candles_airfresh': 'Candles & air fresheners',
    'hh_kitchenware': 'Kitchenware',
    'hh_other': 'Other household',
    # personal care
    'pcare_shampoo_cond': 'Shampoo & conditioner',
    'pcare_body_wash': 'Body wash',
    'pcare_soap': 'Soap',
    'pcare_deo': 'Deodorant',
    'pcare_oral_care': 'Oral care',
    'pcare_shaving': 'Shaving',
    'pcare_feminine_hygiene': 'Feminine hygiene',
    'pcare_cosmetics': 'Cosmetics',
    'pcare_other': 'Other personal care',
    # pet
    'pet_cat_dry': 'Dry cat food',
    'pet_cat_wet': 'Wet cat food',
    'pet_cat_treats': 'Cat treats',
    'pet_dog_dry': 'Dry dog food',
    'pet_dog_wet': 'Wet dog food',
    'pet_dog_treats': 'Dog treats',
    'pet_litter_sand': 'Cat litter',
    'pet_birds_fish': 'Bird & fish food',
    'pet_small_animals': 'Small animal food',
    'pet_other': 'Other pet products',
    # baby
    'baby_food_jars': 'Baby food (jars)',
    'baby_food_pouches': 'Baby food (pouches)',
    'baby_formula': 'Baby formula',
    'baby_porridge_cereal': 'Baby porridge & cereal',
    'baby_snacks': 'Baby snacks',
    'baby_diapers': 'Diapers',
    'baby_wipes': 'Baby wipes',
    'baby_care': 'Baby care',
    'baby_other': 'Other baby products',
    # dry
    'dry_pasta_rice': 'Pasta & rice',
    'dry_groats_beans': 'Groats & beans',
    'dry_flour_sugar_baking': 'Flour, sugar & baking',
    'dry_cereals_muesli': 'Cereals & muesli',
    'dry_soups_noodles': 'Soups & noodles',
    'dry_canned_meat_fish': 'Canned meat & fish',
    'dry_canned_veg': 'Canned vegetables',
    'dry_canned_fruit': 'Canned fruit',
    'dry_ready_meals_jars': 'Ready meals',
    'dry_other': 'Other dry goods',
    # frozen
    'frozen_meat': 'Frozen meat',
    'frozen_fish': 'Frozen fish',
    'frozen_veg': 'Frozen vegetables',
    'frozen_berries_fruit': 'Frozen berries & fruit',
    'frozen_pizza': 'Pizza',
    'frozen_ready_meals': 'Frozen ready meals',
    'frozen_potato_products': 'Potato products',
    'frozen_bakery': 'Frozen bakery',
    'frozen_desserts_icecream': 'Ice cream & desserts',
    'frozen_other': 'Other frozen',
    # spices/sauces/oils
    'spices_basic_salt_pepper': 'Salt & pepper',
    'spices_herbs_spice_mix': 'Herbs & spice mixes',
    'spices_broth_stock': 'Broth & stock',
    'spices_asian': 'Asian spices',
    'spices_mexican': 'Mexican spices',
    'spices_other': 'Other spices',
    'oils_olive': 'Olive oil',
    'oils_other': 'Other oils',
    'oils_vinegar': 'Vinegar',
    'sauces_ketchup_mayo': 'Ketchup & mayonnaise',
    'sauces_pasta_cooking': 'Pasta & cooking sauces',
    'sauces_salad_dressings': 'Salad dressings',
    'sauces_marinades': 'Marinades',
    'sauces_soy_worcester': 'Soy & worcestershire sauce',
    'sauces_other': 'Other sauces',
}

# ─────────────────────────────────────────────────────────
# 1) Main categories
# ─────────────────────────────────────────────────────────
@router.get("/main")
async def list_main_categories(
    request: Request,
    db=Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    sql = """
        SELECT
            m.code,
            m.label_et,
            COALESCE(m.label_en, m.label_et) AS label_en,
            COUNT(DISTINCT p.id) AS product_count
        FROM categories_main m
        LEFT JOIN categories_sub s ON s.main_id = m.id
        LEFT JOIN products p ON p.sub_code = s.code
        GROUP BY m.id, m.code, m.label_et, m.label_en, m.sort_order
        ORDER BY m.sort_order, m.id;
    """
    rows = await db.fetch(sql)
    return [
        {
            "code": r["code"],
            "label": r["label_et"],
            "label_et": r["label_et"],
            "label_ru": _MAIN_RU.get(r["code"], r["label_et"]),
            "label_en": _MAIN_EN.get(r["code"], r["label_en"] or r["label_et"]),
            "product_count": r["product_count"],
        }
        for r in rows
    ]

# ─────────────────────────────────────────────────────────
# 2) Subcategories under a main category (only top-level, no parent)
# ─────────────────────────────────────────────────────────
@router.get("/{main_code}/sub")
async def list_subcategories(
    main_code: str,
    request: Request,
    db=Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    main_row = await db.fetchrow(
        "SELECT id, code, label_et FROM categories_main WHERE code = $1",
        main_code,
    )
    if not main_row:
        raise HTTPException(status_code=404, detail="Main category not found")
    sql = """
        SELECT
            s.code,
            s.label_et,
            COALESCE(s.label_en, s.label_et) AS label_en,
            s.parent_id,
            COUNT(DISTINCT p.id) AS product_count
        FROM categories_sub s
        JOIN categories_main m ON s.main_id = m.id
        LEFT JOIN products p ON p.sub_code = s.code
        WHERE m.code = $1
          AND s.parent_id IS NULL
        GROUP BY s.id, s.code, s.label_et, s.label_en, s.sort_order, s.parent_id
        ORDER BY s.sort_order, s.id;
    """
    rows = await db.fetch(sql, main_code)
    result = []
    for r in rows:
        count = r["product_count"]
        if r["code"] == "drinks_spirits":
            child_count = await db.fetchval("""
                SELECT COUNT(DISTINCT p.id)
                FROM categories_sub s
                LEFT JOIN products p ON p.sub_code = s.code
                WHERE s.parent_id = (SELECT id FROM categories_sub WHERE code = 'drinks_spirits')
            """)
            count = (count or 0) + (child_count or 0)
        result.append({
            "code": r["code"],
            "label": r["label_et"],
            "label_et": r["label_et"],
            "label_ru": _SUB_RU.get(r["code"], r["label_et"]),
            "label_en": _SUB_EN.get(r["code"], r["label_en"] or r["label_et"]),
            "product_count": count,
            "has_children": r["code"] == "drinks_spirits",
        })
    return result

# ─────────────────────────────────────────────────────────
# 3) Sub-subcategories under a subcategory (by sub_code)
# ─────────────────────────────────────────────────────────
@router.get("/{main_code}/sub/{sub_code}/sub")
async def list_sub_subcategories(
    main_code: str,
    sub_code: str,
    request: Request,
    db=Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    parent_row = await db.fetchrow(
        "SELECT id, code, label_et FROM categories_sub WHERE code = $1",
        sub_code,
    )
    if not parent_row:
        raise HTTPException(status_code=404, detail="Subcategory not found")
    sql = """
        SELECT
            s.code,
            s.label_et,
            COALESCE(s.label_en, s.label_et) AS label_en,
            COUNT(DISTINCT p.id) AS product_count
        FROM categories_sub s
        LEFT JOIN products p ON p.sub_code = s.code
        WHERE s.parent_id = $1
        GROUP BY s.id, s.code, s.label_et, s.label_en, s.sort_order
        ORDER BY s.sort_order, s.id;
    """
    rows = await db.fetch(sql, parent_row["id"])
    return [
        {
            "code": r["code"],
            "label": r["label_et"],
            "label_et": r["label_et"],
            "label_ru": _SUB_RU.get(r["code"], r["label_et"]),
            "label_en": _SUB_EN.get(r["code"], r["label_en"] or r["label_et"]),
            "product_count": r["product_count"],
            "has_children": False,
        }
        for r in rows
    ]

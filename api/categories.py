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

# ─── Russian translations (no DB column needed) ────────────────────────────

_MAIN_RU = {
    'produce': 'Фрукты и овощи',
    'meat_fish': 'Мясо и рыба',
    'dairy_eggs_fats': 'Молочные продукты, яйца, масло',
    'cheese': 'Сыры',
    'bakery': 'Хлеб и выпечка',
    'frozen_food': 'Замороженные продукты',
    'dry_preserves': 'Сухие продукты и консервы',
    'drinks': 'Напитки',
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
    # drinks
    'drinks_water': 'Вода',
    'drinks_soft_soda': 'Газировка',
    'drinks_juices': 'Соки',
    'drinks_energy': 'Энергетики',
    'drinks_sport': 'Спортивные напитки',
    'drinks_syrups_powders': 'Сиропы и порошки',
    'drinks_other': 'Напитки прочее',
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
            "label_en": r["label_en"],
            "product_count": r["product_count"],
        }
        for r in rows
    ]

# ─────────────────────────────────────────────────────────
# 2) Subcategories under a main category
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
            COUNT(DISTINCT p.id) AS product_count
        FROM categories_sub s
        JOIN categories_main m ON s.main_id = m.id
        LEFT JOIN products p ON p.sub_code = s.code
        WHERE m.code = $1
        GROUP BY s.id, s.code, s.label_et, s.label_en, s.sort_order
        ORDER BY s.sort_order, s.id;
    """
    rows = await db.fetch(sql, main_code)
    return [
        {
            "code": r["code"],
            "label": r["label_et"],
            "label_et": r["label_et"],
            "label_ru": _SUB_RU.get(r["code"], r["label_et"]),
            "label_en": r["label_en"],
            "product_count": r["product_count"],
        }
        for r in rows
    ]

import json
import os
import httpx
from fastapi import APIRouter, HTTPException, Request, Query
from typing import Optional

router = APIRouter()

THEMEALDB_BASE = "https://www.themealdb.com/api/json/v1/1"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

FEATURED_MEALS = [
    ("52982", "Spaghetti Carbonara"),
    ("52917", "Creme Brulee"),
    ("52844", "Lasanje"),
    ("53261", "Kanatiivad sidruniga"),
    ("53065", "Sushi"),
    ("52840", "Koorene karpsupp"),
    ("52874", "Veiseliha hautis sinepiga"),
    ("53305", "Kapsarullid"),
    ("53064", "Alfredo pasta"),
    ("52959", "Ahjulõhe apteegitilliga"),
]

SKIP_INGREDIENTS = {
    "water", "salt", "black pepper", "pepper", "white pepper",
    "mixed herbs", "seasoning", "oil spray", "to taste",
}

CHAIN_DISPLAY_NAMES = {
    "rimi": "Rimi",
    "selver": "Selver",
    "prisma": "Prisma",
    "coop": "Coop",
    "maxima": "Maxima",
    "barbora": "Maxima",
}

VALID_SUB_CODES = [
    "dry_pasta_rice", "dairy_eggs", "dairy_milk", "dairy_butter_margarine",
    "dairy_cream_sourcream", "dairy_yogurt_kefir", "cheese_regular",
    "cheese_delicatessen", "dairy_cheese_slices", "meat_poultry", "meat_beef_lamb_game",
    "meat_minced", "meat_pork", "meat_hams", "meat_sausages",
    "fish_fresh", "fish_salted_smoked", "fish_other", "fish_processed",
    "produce_root_veg", "produce_mushrooms", "produce_tropical",
    "produce_herbs_salads_sprouts", "produce_smoothies_fresh_juices",
    "dry_flour_sugar_baking", "dry_canned_veg", "dry_other", "dry_ready_meals_jars",
    "frozen_bakery", "frozen_veg", "frozen_berries_fruit", "frozen_ready_meals",
    "frozen_meat", "frozen_other", "frozen_desserts_icecream",
    "oils_olive", "oils_other", "oils_vinegar",
    "sauces_ketchup_mayo", "sauces_pasta_cooking", "sauces_soy_worcester",
    "sauces_other", "sauces_marinades",
    "spices_herbs_spice_mix", "spices_broth_stock",
    "drinks_wine", "drinks_beer_cider", "drinks_soft_soda",
    "sweets_chocolate_bars", "sweets_nuts_driedfruit",
    "bakery_other", "bakery_bread_loaves",
    "dry_canned_fruit",
]

INGREDIENT_TRANSLATIONS = {
    "chicken": "kana", "chicken breast": "kanafileed", "chicken breasts": "kanafileed",
    "chicken thighs": "kanareis", "chicken wings": "kanatiivad", "whole chicken": "terve kana",
    "beef": "veiseliha", "ground beef": "veisehakkliha", "minced beef": "veisehakkliha",
    "pork": "sealiha", "bacon": "peekon", "pancetta": "peekon", "salmon": "lõhe",
    "pasta": "pasta", "spaghetti": "spaghetti", "fettuccine": "fettuccine",
    "lasagne sheets": "lasanjeplaadid", "rice": "riis", "sushi rice": "sushi riis",
    "potato": "kartul", "potatoes": "kartulid", "onion": "sibul", "onions": "sibulad",
    "garlic": "küüslauk", "garlic cloves": "küüslauguküüned", "tomato": "tomat",
    "tomatoes": "tomatid", "cherry tomatoes": "kirsstomatid", "tomato puree": "tomatipasta",
    "chopped tomatoes": "konservtomatid", "tinned tomatoes": "konservtomatid",
    "olive oil": "oliiviõli", "oil": "taimeõli", "rapeseed oil": "rapsiõli",
    "butter": "või", "milk": "piim", "cream": "koor", "double cream": "vahukoor",
    "heavy cream": "vahukoor", "creme fraiche": "hapukoor", "egg": "muna", "eggs": "munad",
    "egg yolks": "munakollased", "cheese": "juust", "parmesan": "parmesan",
    "parmesan cheese": "parmesan", "pecorino": "parmesan", "mozzarella": "mozzarella",
    "mozzarella balls": "mozzarella", "cheddar": "cheddar", "salt": "sool",
    "pepper": "pipar", "black pepper": "must pipar", "sugar": "suhkur",
    "caster sugar": "tuhksuhkur", "vanilla": "vanill", "flour": "jahu", "plain flour": "nisujahu",
    "puff pastry": "lehttainas", "chicken stock": "kanapuljong", "beef stock": "veisepuljong",
    "vegetable stock": "köögiviljapuljong", "carrot": "porgand", "carrots": "porgandid",
    "celery": "seller", "fennel": "apteegitill", "mushrooms": "seened", "mushroom": "seened",
    "red pepper": "punane paprika", "capsicum": "paprika", "green beans": "rohelised oad",
    "cabbage leaves": "kapsas", "cabbage": "kapsas", "lemon": "sidrun", "cucumber": "kurk",
    "soy sauce": "sojakaste", "honey": "mesi", "mustard": "sinep", "mayonnaise": "majonees",
    "breadcrumbs": "riivsai", "basil leaves": "basiilik", "basil": "basiilik",
    "parsley": "petersell", "thyme": "tüümian", "cumin seeds": "köömned",
    "bay leaf": "loorberileht", "clams": "merekarbid", "mussels": "merekarbid",
    "white wine": "valge vein", "red wine": "punane vein",
    "white chocolate chips": "valge šokolaad", "ginger": "ingver",
    "black olives": "oliivid", "clear honey": "mesi", "balsamic vinegar": "äädikas",
    "rosemary": "rosmariin", "basmati rice": "riis",
    "rice wine": "riisivein", "cooked chestnut": "keedetud kastanid",
    "cranberry": "jõhvikad", "vanilla extract": "vanilliekstrakt",
    "vanilla pod": "vanillikaun",
}

MEASURE_TRANSLATIONS = {
    "as required": "maitse järgi",
    "as needed": "maitse järgi",
    "to taste": "maitse järgi",
    "to serve": "serveerimiseks",
    "tbs": "spl", "tbsp": "spl", "tablespoon": "spl", "tablespoons": "spl",
    "tblsp": "spl", "tbls": "spl",
    "tsp": "tl", "teaspoon": "tl", "teaspoons": "tl",
    "cup": "tass", "cups": "tassi",
    "oz": "g", "lb": "450g",
    "g": "g", "kg": "kg", "ml": "ml", "l": "l",
    "medium": "tk", "large": "tk", "small": "tk",
    "clove": "küüs", "cloves": "küünt",
    "cloves minced": "küünt hakitud", "cloves chopped": "küünt hakitud",
    "slice": "viil", "slices": "viilu",
    "bunch": "kamp", "handful": "peotäis",
    "pinch": "näputäis",
    "pod of": "kaun", "pod": "kaun",
    "top": "peale", "topping": "peale",
    "sprigs of fresh": "oksa värsket", "sprigs": "oksa", "sprig": "oks",
    "stick": "vars",
    "zest and juice of 1": "koor ja mahl 1-st",
    "juice of 1": "mahl 1-st",
    "juice of": "mahl",
    "chopped": "hakitud", "diced": "kuubikuteks", "sliced": "viilutatud",
    "grated": "riivitud", "minced": "hakitud", "crushed": "purustatud",
    "finely chopped": "peenelt hakitud", "roughly chopped": "jämedalt hakitud",
    "finely sliced": "peenelt viilutatud",
    "halved": "pooleks", "quartered": "neljaks",
    "free-range": "vabalt peetud",
}

CATEGORY_TRANSLATIONS = {
    "Seafood": "Mereannid", "Chicken": "Kana", "Beef": "Veiseliha",
    "Pasta": "Pasta", "Dessert": "Magustoit", "Vegetarian": "Taimetoit",
    "Pork": "Sealiha", "Lamb": "Lambaliha", "Breakfast": "Hommikusöök",
    "Side": "Lisand", "Starter": "Eelroog", "Vegan": "Vegantoit",
    "Miscellaneous": "Muud", "Goat": "Kitseliha", "Fish": "Kala",
}

AREA_TRANSLATIONS = {
    "British": "Briti", "Italian": "Itaalia", "Japanese": "Jaapani",
    "French": "Prantsuse", "American": "Ameerika", "Chinese": "Hiina",
    "Mexican": "Mehhiko", "Indian": "India", "Thai": "Tai",
    "Greek": "Kreeka", "Spanish": "Hispaania", "Russian": "Vene",
    "Turkish": "Türgi", "Moroccan": "Maroko", "Malaysian": "Malaisia",
    "Vietnamese": "Vietnami", "Canadian": "Kanada", "Croatian": "Horvaatia",
    "Dutch": "Hollandi", "Egyptian": "Egiptuse", "Filipino": "Filipiini",
    "Irish": "Iiri", "Jamaican": "Jamaika", "Kenyan": "Keenia",
    "Polish": "Poola", "Portuguese": "Portugali", "Tunisian": "Tuneesia",
    "Unknown": "Tundmatu",
}


def translate_measure(measure: str) -> str:
    if not measure:
        return ""
    result = measure.strip()
    for en, et in MEASURE_TRANSLATIONS.items():
        import re
        result = re.sub(r'\b' + re.escape(en) + r'\b', et, result, flags=re.IGNORECASE)
    return result


def translate_ingredient(name: str) -> str:
    name_lower = name.lower().strip()
    if name_lower in INGREDIENT_TRANSLATIONS:
        return INGREDIENT_TRANSLATIONS[name_lower]
    for en, et in INGREDIENT_TRANSLATIONS.items():
        if en in name_lower:
            return et
    return name


def parse_ingredients(meal: dict) -> list[dict]:
    ingredients = []
    for i in range(1, 21):
        ingredient = meal.get(f"strIngredient{i}", "")
        measure = meal.get(f"strMeasure{i}", "")
        if ingredient and ingredient.strip():
            ingredients.append({
                "name_en": ingredient.strip(),
                "name_et": translate_ingredient(ingredient.strip()),
                "measure": measure.strip() if measure else "",
                "measure_et": translate_measure(measure.strip() if measure else ""),
            })
    return ingredients


async def get_cached_translation(db, meal_id: str) -> Optional[dict]:
    row = await db.fetchrow(
        "SELECT instructions_et, category_et, area_et FROM recipe_translations WHERE meal_id = $1",
        meal_id
    )
    if row:
        return {
            "instructions_et": row["instructions_et"],
            "category_et": row["category_et"],
            "area_et": row["area_et"],
        }
    return None


async def save_translation_cache(db, meal_id: str, instructions_et: str, category_et: str, area_et: str):
    await db.execute(
        """INSERT INTO recipe_translations (meal_id, instructions_et, category_et, area_et)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (meal_id) DO UPDATE
           SET instructions_et = EXCLUDED.instructions_et,
               category_et = EXCLUDED.category_et,
               area_et = EXCLUDED.area_et,
               created_at = NOW()""",
        meal_id, instructions_et, category_et, area_et
    )


async def translate_instructions_claude(instructions: str, recipe_name: str) -> str:
    if not ANTHROPIC_API_KEY:
        return instructions

    prompt = f"""Tõlgi järgmine retsepti valmistamisjuhend eesti keelde. Retsept: "{recipe_name}".

Juhised tõlkimiseks:
- Tõlgi loomulikus eesti keeles
- Säilita lõigud ja struktuur
- Temperatuurid jäta samaks (180C jne)
- Mõõdud jäta samaks (g, ml, tbs→spl, tsp→tl)
- Ära lisa selgitusi, tagasta ainult tõlge

Tekst tõlkimiseks:
{instructions}"""

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            }
        )
        data = resp.json()
        if "content" in data:
            return data["content"][0]["text"].strip()
        return instructions


async def get_or_create_translation(db, meal_id: str, meal: dict, recipe_name: str) -> dict:
    cached = await get_cached_translation(db, meal_id)
    if cached:
        return cached

    instructions = meal.get("strInstructions", "")
    category = meal.get("strCategory", "")
    area = meal.get("strArea", "")

    try:
        instructions_et = await translate_instructions_claude(instructions, recipe_name)
    except Exception as e:
        print(f"[recipes] Translation error for {meal_id}: {e}")
        instructions_et = instructions

    category_et = CATEGORY_TRANSLATIONS.get(category, category)
    area_et = AREA_TRANSLATIONS.get(area, area)

    await save_translation_cache(db, meal_id, instructions_et, category_et, area_et)

    return {
        "instructions_et": instructions_et,
        "category_et": category_et,
        "area_et": area_et,
    }


async def get_cached_ingredient(db, ingredient_en: str):
    row = await db.fetchrow(
        "SELECT search_terms, sub_codes FROM recipe_ingredient_cache WHERE ingredient_en = $1",
        ingredient_en.lower().strip()
    )
    if row:
        return {"search_terms": list(row["search_terms"]), "sub_codes": list(row["sub_codes"])}
    return None


async def save_ingredient_cache(db, ingredient_en: str, search_terms: list, sub_codes: list):
    await db.execute(
        """INSERT INTO recipe_ingredient_cache (ingredient_en, search_terms, sub_codes)
           VALUES ($1, $2, $3)
           ON CONFLICT (ingredient_en) DO UPDATE
           SET search_terms = EXCLUDED.search_terms,
               sub_codes = EXCLUDED.sub_codes,
               created_at = NOW()""",
        ingredient_en.lower().strip(), search_terms, sub_codes
    )


async def ask_claude_for_ingredient(ingredient_en: str) -> dict:
    if not ANTHROPIC_API_KEY:
        return {"search_terms": [ingredient_en.lower()], "sub_codes": []}

    prompt = f"""You help match recipe ingredients to Estonian grocery store product names.

Ingredient: "{ingredient_en}"

CRITICAL: search_terms MUST be Estonian words used in store databases. sub_codes MUST come from the list.

Estonian translations:
- bacon, pancetta → "peekon"
- egg, eggs, egg yolks → "muna"
- spaghetti → "spaghetti"
- parmesan, pecorino, hard cheese → "parmesan", "parmigiano", "grana padano", "dziugas"
- butter → "või"
- milk → "piim"
- cream, double cream, heavy cream → "koor", "vahukoor"
- chicken breast → "kanafileed"
- chicken wings → "kanatiivad"
- ground beef, minced beef → "veisehakkliha"
- beef → "veiseliha"
- pork → "sealiha"
- salmon → "lõhe"
- onion → "sibul"
- garlic → "küüslauk"
- tomato, tomatoes → "tomat"
- cherry tomatoes → "kirsstomat"
- carrot → "porgand"
- potato → "kartul"
- mushrooms → "seened"
- olive oil → "oliiviõli"
- flour, plain flour → "jahu"
- sugar → "suhkur"
- rice → "riis"
- pasta → "pasta"
- lemon → "sidrun"
- cheese → "juust"
- mozzarella → "mozzarella"
- honey → "mesi"
- mustard → "sinep"
- soy sauce → "sojakaste"
- white wine → "valge vein"
- red wine → "punane vein", "merlot", "cabernet", "shiraz", "malbec", "pinot noir", "tempranillo", "syrah"

Valid sub_codes: {json.dumps(VALID_SUB_CODES)}

Examples:
- "bacon" → {{"search_terms": ["peekon"], "sub_codes": ["meat_hams"]}}
- "egg yolks" → {{"search_terms": ["muna"], "sub_codes": ["dairy_eggs"]}}
- "parmesan" → {{"search_terms": ["parmesan", "parmigiano", "grana padano", "dziugas"], "sub_codes": ["cheese_regular", "cheese_delicatessen"]}}
- "spaghetti" → {{"search_terms": ["spaghetti"], "sub_codes": ["dry_pasta_rice"]}}
- "red wine" → {{"search_terms": ["punane vein", "merlot", "cabernet", "shiraz", "malbec", "pinot noir", "tempranillo", "syrah"], "sub_codes": ["drinks_wine"]}}
- "white wine" → {{"search_terms": ["valge vein", "chardonnay", "sauvignon", "riesling", "pinot grigio"], "sub_codes": ["drinks_wine"]}}
- "water" → {{"search_terms": [], "sub_codes": []}}
- "salt" → {{"search_terms": [], "sub_codes": []}}

Return ONLY valid JSON for "{ingredient_en}":"""

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            }
        )
        data = resp.json()
        if "error" in data:
            raise ValueError(f"API error: {data['error']}")
        if "content" not in data:
            raise ValueError(f"Unexpected response: {data}")
        text = data["content"][0]["text"].strip()
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)


async def resolve_ingredient(db, ingredient_en: str) -> dict:
    name_lower = ingredient_en.lower().strip()
    if name_lower in SKIP_INGREDIENTS:
        return {"search_terms": [], "sub_codes": []}
    cached = await get_cached_ingredient(db, name_lower)
    if cached:
        return cached
    try:
        result = await ask_claude_for_ingredient(ingredient_en)
    except Exception as e:
        print(f"[recipes] Claude API error for '{ingredient_en}': {e}")
        import traceback; traceback.print_exc()
        result = {"search_terms": [name_lower], "sub_codes": []}
    await save_ingredient_cache(db, name_lower, result["search_terms"], result["sub_codes"])
    return result


async def find_products_per_store_for_ingredient(db, ingredient_en: str) -> dict:
    resolved = await resolve_ingredient(db, ingredient_en)
    search_terms = resolved.get("search_terms", [])
    sub_codes = resolved.get("sub_codes", [])

    if not search_terms:
        return {}

    results_by_chain = {}

    for term in search_terms:
        if sub_codes:
            rows = await db.fetch("""
                SELECT p.id, p.name, p.chain, p.image_url, p.brand, p.size_text,
                    MIN(pr.price) as min_price
                FROM products p
                JOIN prices pr ON pr.product_id = p.id
                WHERE p.name ILIKE $1
                  AND p.sub_code = ANY($2::text[])
                  AND pr.price > 0
                  AND pr.collected_at > NOW() - INTERVAL '14 days'
                  AND p.name NOT ILIKE '%kaitstud%'
                  AND p.name NOT ILIKE '%geograafilise%'
                  AND p.name NOT ILIKE '%strooganov%'
                  AND p.name NOT ILIKE '%valmistoit%'
                  AND p.name NOT ILIKE '%praad%'
                  AND p.name NOT ILIKE '%kotlet%'
                GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
                ORDER BY p.chain, min_price ASC
            """, f"%{term}%", sub_codes)
        else:
            rows = await db.fetch("""
                SELECT p.id, p.name, p.chain, p.image_url, p.brand, p.size_text,
                    MIN(pr.price) as min_price
                FROM products p
                JOIN prices pr ON pr.product_id = p.id
                WHERE p.name ILIKE $1
                  AND p.sub_code NOT IN (
                    'hh_other','hh_cleaners','hh_laundry','hh_dishwashing',
                    'pcare_oral_care','pcare_other','pcare_feminine_hygiene',
                    'baby_diapers','pet_cat_wet','pet_dog_wet','pet_cat_dry','pet_dog_dry'
                  )
                  AND pr.price > 0
                  AND pr.collected_at > NOW() - INTERVAL '14 days'
                  AND p.name NOT ILIKE '%kaitstud%'
                  AND p.name NOT ILIKE '%geograafilise%'
                  AND p.name NOT ILIKE '%strooganov%'
                  AND p.name NOT ILIKE '%valmistoit%'
                  AND p.name NOT ILIKE '%praad%'
                  AND p.name NOT ILIKE '%kotlet%'
                GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
                ORDER BY p.chain, min_price ASC
            """, f"%{term}%")

        for r in rows:
            chain = (r["chain"] or "").lower()
            price = float(r["min_price"])
            if chain not in results_by_chain or price < results_by_chain[chain]["price"]:
                results_by_chain[chain] = {
                    "product_id": r["id"],
                    "name": r["name"],
                    "chain": chain,
                    "image_url": r["image_url"] or "",
                    "brand": r["brand"] or "",
                    "size_text": r["size_text"] or "",
                    "is_per_kg": (r["size_text"] or "").lower() == "kg",
                    "price": price,
                    "quantity": 1,
                }

    return results_by_chain


async def find_product_for_ingredient(db, ingredient_en: str):
    per_store = await find_products_per_store_for_ingredient(db, ingredient_en)
    if not per_store:
        return None
    return min(per_store.values(), key=lambda x: x["price"])


async def _get_nearby_chains(db, lat: float, lon: float, radius_km: float) -> dict:
    rows = await db.fetch("""
        WITH with_dist AS (
            SELECT
                CASE
                    WHEN LOWER(s.chain) = 'maxima' THEN 'barbora'
                    ELSE LOWER(s.chain)
                END AS chain,
                2*6371*asin(sqrt(
                    pow(sin(radians((s.lat - $1) / 2)), 2) +
                    cos(radians($1)) * cos(radians(s.lat)) *
                    pow(sin(radians((s.lon - $2) / 2)), 2)
                )) AS distance_km
            FROM stores s
            WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
              AND COALESCE(s.is_online, false) = false
              AND s.chain IS NOT NULL
        )
        SELECT chain, MIN(distance_km) AS min_distance_km
        FROM with_dist
        WHERE distance_km <= $3
        GROUP BY chain
        ORDER BY min_distance_km ASC
    """, float(lat), float(lon), float(radius_km))

    return {
        r["chain"]: round(float(r["min_distance_km"]), 2)
        for r in rows if r["chain"]
    }


@router.get("/recipes")
async def get_recipes():
    recipes = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        for meal_id, estonian_name in FEATURED_MEALS:
            try:
                resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
                data = resp.json()
                meal = data["meals"][0]
                recipes.append({
                    "id": meal["idMeal"],
                    "name": estonian_name,
                    "name_en": meal["strMeal"],
                    "category": CATEGORY_TRANSLATIONS.get(meal["strCategory"], meal["strCategory"]),
                    "area": AREA_TRANSLATIONS.get(meal["strArea"], meal["strArea"]),
                    "image": meal["strMealThumb"],
                    "instructions_preview": meal["strInstructions"][:300] + "...",
                    "youtube": meal.get("strYoutube", ""),
                    "ingredients": parse_ingredients(meal),
                })
            except Exception:
                continue
    return {"recipes": recipes}


@router.get("/recipes/{meal_id}/compare")
async def get_recipe_compare(
    meal_id: str,
    request: Request,
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    radius_km: float = Query(10.0, ge=0.5, le=50.0),
):
    db = request.app.state.db
    if not db:
        raise HTTPException(status_code=503, detail="DB unavailable")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
            data = resp.json()
            meal = data["meals"][0]
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Recipe not found: {e}")

    ingredients = parse_ingredients(meal)
    estonian_name = next(
        (name for mid, name in FEATURED_MEALS if mid == meal_id),
        meal["strMeal"]
    )

    nearby_chains: Optional[dict] = None
    if lat is not None and lon is not None:
        nearby_chains = await _get_nearby_chains(db, lat, lon, radius_km)

    ingredient_results = []
    for ing in ingredients:
        name_lower = ing["name_en"].lower().strip()
        if name_lower in SKIP_INGREDIENTS:
            continue
        per_store = await find_products_per_store_for_ingredient(db, ing["name_en"])
        ingredient_results.append({
            "ingredient_name": ing["name_et"],
            "ingredient_name_en": ing["name_en"],
            "measure": ing["measure_et"],
            "by_chain": per_store,
        })

    all_chains = set()
    for ir in ingredient_results:
        all_chains.update(ir["by_chain"].keys())

    if nearby_chains is not None:
        nearby_keys = set(nearby_chains.keys())
        if "barbora" in nearby_keys:
            nearby_keys.add("maxima")
        if "maxima" in nearby_keys:
            nearby_keys.add("barbora")
        all_chains = all_chains & nearby_keys

    store_summaries = []
    for chain in all_chains:
        products = []
        not_found = []
        total_price = 0.0

        for ir in ingredient_results:
            if chain in ir["by_chain"]:
                product = dict(ir["by_chain"][chain])
                product["ingredient_name"] = ir["ingredient_name"]
                product["measure"] = ir["measure"]
                products.append(product)
                total_price += product["price"]
            else:
                not_found.append(ir["ingredient_name"])

        distance_km = None
        if nearby_chains is not None:
            distance_km = nearby_chains.get(chain)

        store_summaries.append({
            "chain": chain,
            "display_name": CHAIN_DISPLAY_NAMES.get(chain, chain.title()),
            "covered": len(products),
            "total": len(ingredient_results),
            "total_price": round(total_price, 2),
            "distance_km": distance_km,
            "products": products,
            "not_found": not_found,
        })

    if nearby_chains is not None:
        store_summaries.sort(key=lambda x: (
            -(x["covered"]),
            x["distance_km"] if x["distance_km"] is not None else 999,
            x["total_price"],
        ))
    else:
        store_summaries.sort(key=lambda x: (-x["covered"], x["total_price"]))

    return {
        "meal_id": meal_id,
        "recipe_name": estonian_name,
        "total_ingredients": len(ingredient_results),
        "location_used": lat is not None and lon is not None,
        "stores": store_summaries,
    }


@router.get("/recipes/{meal_id}/basket")
async def get_recipe_basket(meal_id: str, request: Request):
    db = request.app.state.db
    if not db:
        raise HTTPException(status_code=503, detail="DB unavailable")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
            data = resp.json()
            meal = data["meals"][0]
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Recipe not found: {e}")

    ingredients = parse_ingredients(meal)
    estonian_name = next(
        (name for mid, name in FEATURED_MEALS if mid == meal_id),
        meal["strMeal"]
    )

    matched = []
    not_found = []

    for ing in ingredients:
        product = await find_product_for_ingredient(db, ing["name_en"])
        if product:
            product["ingredient_name"] = ing["name_et"]
            product["measure"] = ing["measure_et"]
            matched.append(product)
        else:
            not_found.append({
                "name_en": ing["name_en"],
                "name_et": ing["name_et"],
                "measure": ing["measure_et"],
            })

    return {
        "meal_id": meal_id,
        "recipe_name": estonian_name,
        "matched_products": matched,
        "not_found": not_found,
        "matched_count": len(matched),
        "total_ingredients": len(ingredients),
    }


@router.get("/recipes/{meal_id}")
async def get_recipe(meal_id: str, request: Request):
    db = getattr(request.app.state, "db", None)

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
            data = resp.json()
            meal = data["meals"][0]
            estonian_name = next(
                (name for mid, name in FEATURED_MEALS if mid == meal_id),
                meal["strMeal"]
            )

            translation = None
            if db:
                try:
                    translation = await get_or_create_translation(db, meal_id, meal, estonian_name)
                except Exception as e:
                    print(f"[recipes] Translation error: {e}")

            instructions_et = translation["instructions_et"] if translation else meal["strInstructions"]
            category_et = translation["category_et"] if translation else CATEGORY_TRANSLATIONS.get(meal["strCategory"], meal["strCategory"])
            area_et = translation["area_et"] if translation else AREA_TRANSLATIONS.get(meal["strArea"], meal["strArea"])

            return {
                "id": meal["idMeal"],
                "name": estonian_name,
                "name_en": meal["strMeal"],
                "category": category_et,
                "area": area_et,
                "image": meal["strMealThumb"],
                "instructions": instructions_et,
                "instructions_en": meal["strInstructions"],
                "youtube": meal.get("strYoutube", ""),
                "ingredients": parse_ingredients(meal),
            }
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

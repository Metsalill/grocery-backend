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
    "caster sugar": "suhkur", "vanilla": "vanill", "flour": "jahu", "plain flour": "nisujahu",
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
}


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
            })
    return ingredients


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
- red wine → "punane vein"

Valid sub_codes: {json.dumps(VALID_SUB_CODES)}

Examples:
- "bacon" → {{"search_terms": ["peekon"], "sub_codes": ["meat_hams"]}}
- "egg yolks" → {{"search_terms": ["muna"], "sub_codes": ["dairy_eggs"]}}
- "parmesan" → {{"search_terms": ["parmesan", "parmigiano", "grana padano", "dziugas"], "sub_codes": ["cheese_regular", "cheese_delicatessen"]}}
- "spaghetti" → {{"search_terms": ["spaghetti"], "sub_codes": ["dry_pasta_rice"]}}
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
    """Cache → Claude API → salvesta cache."""
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
                SELECT
                    p.id, p.name, p.chain, p.image_url, p.brand, p.size_text,
                    MIN(pr.price) as min_price
                FROM products p
                JOIN prices pr ON pr.product_id = p.id
                WHERE p.name ILIKE $1
                  AND p.sub_code = ANY($2::text[])
                  AND pr.price > 0
                GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
                ORDER BY p.chain, min_price ASC
            """, f"%{term}%", sub_codes)
        else:
            rows = await db.fetch("""
                SELECT
                    p.id, p.name, p.chain, p.image_url, p.brand, p.size_text,
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
                GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
                ORDER BY p.chain, min_price ASC
            """, f"%{term}%")

        for r in rows:
            chain = (r["chain"] or "").lower()
            if chain not in results_by_chain:
                results_by_chain[chain] = {
                    "product_id": r["id"],
                    "name": r["name"],
                    "chain": chain,
                    "image_url": r["image_url"] or "",
                    "brand": r["brand"] or "",
                    "size_text": r["size_text"] or "",
                    "is_per_kg": (r["size_text"] or "").lower() == "kg",
                    "price": float(r["min_price"]),
                    "quantity": 1,
                }

    return results_by_chain


async def find_product_for_ingredient(db, ingredient_en: str):
    per_store = await find_products_per_store_for_ingredient(db, ingredient_en)
    if not per_store:
        return None
    return min(per_store.values(), key=lambda x: x["price"])


async def _get_nearby_chains(db, lat: float, lon: float, radius_km: float) -> dict:
    """
    Tagastab lähimate füüsiliste poodide chain -> min_distance_km mapping.
    Kasutab sama haversine valemit mis compare_service.
    """
    rows = await db.fetch("""
        WITH with_dist AS (
            SELECT
                s.chain,
                2*6371*asin(sqrt(
                    pow(sin(radians((s.lat - $1) / 2)), 2) +
                    cos(radians($1)) * cos(radians(s.lat)) *
                    pow(sin(radians((s.lon - $2) / 2)), 2)
                )) AS distance_km
            FROM stores s
            WHERE s.lat IS NOT NULL
              AND s.lon IS NOT NULL
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
        r["chain"].lower(): round(float(r["min_distance_km"]), 2)
        for r in rows
        if r["chain"]
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
                    "category": meal["strCategory"],
                    "area": meal["strArea"],
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
    lat: Optional[float] = Query(None, description="Kasutaja laiuskraad"),
    lon: Optional[float] = Query(None, description="Kasutaja pikkuskraad"),
    radius_km: float = Query(10.0, ge=0.5, le=50.0, description="Raadius km"),
):
    """
    Tagastab retsepti hinna võrdluse poodide kaupa.
    Kui lat/lon on antud, filtreeritakse ainult lähimad poed radius_km raadiuses
    ja tulemused sorteeritakse distantsi järgi.
    """
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

    # Leia lähimad poed kui koordinaadid on antud
    nearby_chains: Optional[dict] = None
    if lat is not None and lon is not None:
        nearby_chains = await _get_nearby_chains(db, lat, lon, radius_km)

    # Leia iga koostisosa jaoks tooted poodide kaupa
    ingredient_results = []
    for ing in ingredients:
        name_lower = ing["name_en"].lower().strip()
        if name_lower in SKIP_INGREDIENTS:
            continue
        per_store = await find_products_per_store_for_ingredient(db, ing["name_en"])
        ingredient_results.append({
            "ingredient_name": ing["name_et"],
            "ingredient_name_en": ing["name_en"],
            "measure": ing["measure"],
            "by_chain": per_store,
        })

    # Kogu kõik unikaalsed poed — filtreeri asukoha järgi kui antud
    all_chains = set()
    for ir in ingredient_results:
        all_chains.update(ir["by_chain"].keys())

    if nearby_chains is not None:
        # Normaliseeri nearby_chains võtmed
        nearby_keys = set(nearby_chains.keys())
        # Barbora on Maxima — lisa mõlemad
        if "barbora" in nearby_keys:
            nearby_keys.add("maxima")
        if "maxima" in nearby_keys:
            nearby_keys.add("barbora")
        all_chains = all_chains & nearby_keys

    # Ehita iga poe jaoks kokkuvõte
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

    # Sorteeri: asukoha järgi kui lat/lon antud, muidu covered+hind
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
    """Legacy endpoint."""
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
            product["measure"] = ing["measure"]
            matched.append(product)
        else:
            not_found.append({
                "name_en": ing["name_en"],
                "name_et": ing["name_et"],
                "measure": ing["measure"],
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
async def get_recipe(meal_id: str):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
            data = resp.json()
            meal = data["meals"][0]
            estonian_name = next(
                (name for mid, name in FEATURED_MEALS if mid == meal_id),
                meal["strMeal"]
            )
            return {
                "id": meal["idMeal"],
                "name": estonian_name,
                "name_en": meal["strMeal"],
                "category": meal["strCategory"],
                "area": meal["strArea"],
                "image": meal["strMealThumb"],
                "instructions": meal["strInstructions"],
                "youtube": meal.get("strYoutube", ""),
                "ingredients": parse_ingredients(meal),
            }
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

import httpx
from fastapi import APIRouter, HTTPException, Request

router = APIRouter()

THEMEALDB_BASE = "https://www.themealdb.com/api/json/v1/1"

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
    "white wine": "valge vein", "red wine": "punane vein", "white chocolate chips": "valge šokolaad",
    "ginger": "ingver", "black olives": "oliivid", "clear honey": "mesi",
    "balsamic vinegar": "äädikas", "rosemary": "rosmariin", "basmati rice": "riis",
}

SKIP_INGREDIENTS = {
    "water", "salt", "black pepper", "pepper", "white pepper",
    "mixed herbs", "seasoning", "oil spray",
}

SEARCH_TERMS = {
    "spaghetti": ["spaghetti"],
    "fettuccine": ["fettuccine", "pasta"],
    "lasagne sheets": ["lasanje", "lasagna"],
    "pasta": ["pasta"],
    "sushi rice": ["sushi riis"],
    "rice": ["riis"],
    "basmati rice": ["basmati"],
    "egg yolks": ["muna"],
    "eggs": ["muna"],
    "egg": ["muna"],
    "bacon": ["peekon"],
    "pancetta": ["peekon"],
    "chicken wings": ["kanatiivad", "kana tiib"],
    "chicken breast": ["kanafileed"],
    "chicken breasts": ["kanafileed"],
    "chicken thighs": ["kanareis"],
    "whole chicken": ["terve kana"],
    "minced beef": ["veisehakkliha"],
    "ground beef": ["veisehakkliha"],
    "beef": ["veiseliha"],
    "pork": ["sealiha"],
    "salmon": ["lõhe"],
    "clams": ["merekarbid"],
    "mussels": ["merekarbid"],
    "butter": ["või"],
    "milk": ["piim"],
    "double cream": ["vahukoor"],
    "heavy cream": ["vahukoor"],
    "cream": ["koor"],
    "creme fraiche": ["hapukoor"],
    "parmesan": ["parmesan"],
    "parmesan cheese": ["parmesan"],
    "pecorino": ["parmesan"],
    "mozzarella": ["mozzarella"],
    "mozzarella balls": ["mozzarella"],
    "cheddar": ["cheddar"],
    "cheese": ["juust"],
    "plain flour": ["nisujahu"],
    "flour": ["jahu"],
    "puff pastry": ["lehttainas"],
    "sugar": ["suhkur"],
    "caster sugar": ["suhkur"],
    "honey": ["mesi"],
    "clear honey": ["mesi"],
    "olive oil": ["oliiviõli"],
    "rapeseed oil": ["rapsiõli"],
    "oil": ["taimeõli"],
    "soy sauce": ["sojakaste"],
    "mustard": ["sinep"],
    "mayonnaise": ["majonees"],
    "tomato puree": ["tomatipasta"],
    "chopped tomatoes": ["konservtomatid"],
    "tinned tomatoes": ["konservtomatid"],
    "cherry tomatoes": ["kirsstomat"],
    "tomatoes": ["tomat"],
    "tomato": ["tomat"],
    "onion": ["sibul"],
    "onions": ["sibul"],
    "garlic": ["küüslauk"],
    "garlic cloves": ["küüslauk"],
    "carrot": ["porgand"],
    "carrots": ["porgand"],
    "celery": ["seller"],
    "fennel": ["apteegitill"],
    "mushrooms": ["seened"],
    "mushroom": ["seened"],
    "potatoes": ["kartul"],
    "potato": ["kartul"],
    "capsicum": ["paprika"],
    "red pepper": ["punane paprika"],
    "green beans": ["rohelised oad"],
    "cabbage leaves": ["kapsas"],
    "cabbage": ["kapsas"],
    "lemon": ["sidrun"],
    "cucumber": ["kurk"],
    "basil leaves": ["basiilik"],
    "basil": ["basiilik"],
    "parsley": ["petersell"],
    "thyme": ["tüümian"],
    "cumin seeds": ["köömned"],
    "ginger": ["ingver"],
    "white wine": ["valge vein"],
    "red wine": ["punane vein"],
    "beef stock": ["veisepuljong"],
    "chicken stock": ["kanapuljong"],
    "vanilla": ["vanill"],
    "white chocolate chips": ["valge šokolaad"],
    "breadcrumbs": ["riivsai"],
    "balsamic vinegar": ["balsamiäädikas"],
    "black olives": ["oliivid"],
    "rosemary": ["rosmariin"],
}

INGREDIENT_SUB_CODES = {
    "spaghetti":        ["dry_pasta_rice"],
    "fettuccine":       ["dry_pasta_rice"],
    "lasagne sheets":   ["dry_pasta_rice"],
    "pasta":            ["dry_pasta_rice"],
    "rice":             ["dry_pasta_rice"],
    "sushi rice":       ["dry_pasta_rice"],
    "basmati rice":     ["dry_pasta_rice"],
    "egg":              ["dairy_eggs"],
    "eggs":             ["dairy_eggs"],
    "egg yolks":        ["dairy_eggs"],
    "bacon":            ["meat_hams"],
    "pancetta":         ["meat_hams"],
    "chicken wings":    ["meat_poultry"],
    "chicken breast":   ["meat_poultry"],
    "chicken breasts":  ["meat_poultry"],
    "chicken thighs":   ["meat_poultry"],
    "whole chicken":    ["meat_poultry"],
    "beef":             ["meat_beef_lamb_game"],
    "minced beef":      ["meat_minced"],
    "ground beef":      ["meat_minced"],
    "pork":             ["meat_pork"],
    "salmon":           ["fish_fresh", "fish_salted_smoked"],
    "clams":            ["fish_fresh", "fish_other", "fish_processed"],
    "mussels":          ["fish_fresh", "fish_other", "fish_processed"],
    "butter":           ["dairy_butter_margarine"],
    "milk":             ["dairy_milk"],
    "cream":            ["dairy_cream_sourcream"],
    "double cream":     ["dairy_cream_sourcream"],
    "heavy cream":      ["dairy_cream_sourcream"],
    "creme fraiche":    ["dairy_cream_sourcream"],
    "parmesan":         ["cheese_regular", "cheese_delicatessen"],
    "parmesan cheese":  ["cheese_regular", "cheese_delicatessen"],
    "pecorino":         ["cheese_regular", "cheese_delicatessen"],
    "mozzarella":       ["cheese_regular"],
    "mozzarella balls": ["cheese_regular"],
    "cheddar":          ["cheese_regular"],
    "cheese":           ["cheese_regular", "cheese_delicatessen"],
    "plain flour":      ["dry_flour_sugar_baking"],
    "flour":            ["dry_flour_sugar_baking"],
    "sugar":            ["dry_flour_sugar_baking"],
    "caster sugar":     ["dry_flour_sugar_baking"],
    "vanilla":          ["dry_flour_sugar_baking", "dry_other"],
    "breadcrumbs":      ["dry_other", "bakery_other"],
    "puff pastry":      ["frozen_bakery"],
    "honey":            ["dry_other"],
    "clear honey":      ["dry_other"],
    "chicken stock":    ["spices_broth_stock"],
    "beef stock":       ["spices_broth_stock"],
    "olive oil":        ["oils_olive"],
    "rapeseed oil":     ["oils_other"],
    "oil":              ["oils_other", "oils_olive"],
    "soy sauce":        ["sauces_soy_worcester"],
    "mustard":          ["sauces_other", "sauces_marinades"],
    "mayonnaise":       ["sauces_ketchup_mayo"],
    "tomato puree":     ["sauces_pasta_cooking", "sauces_other"],
    "chopped tomatoes": ["dry_canned_veg"],
    "tinned tomatoes":  ["dry_canned_veg"],
    "black olives":     ["dry_canned_veg"],
    "cherry tomatoes":  ["produce_root_veg"],
    "tomatoes":         ["produce_root_veg"],
    "tomato":           ["produce_root_veg"],
    "onion":            ["produce_root_veg"],
    "onions":           ["produce_root_veg"],
    "garlic":           ["produce_root_veg"],
    "garlic cloves":    ["produce_root_veg"],
    "carrot":           ["produce_root_veg"],
    "carrots":          ["produce_root_veg"],
    "potato":           ["produce_root_veg"],
    "potatoes":         ["produce_root_veg"],
    "celery":           ["produce_root_veg"],
    "fennel":           ["produce_root_veg"],
    "capsicum":         ["produce_root_veg"],
    "red pepper":       ["produce_root_veg"],
    "green beans":      ["produce_root_veg"],
    "cabbage leaves":   ["produce_root_veg"],
    "cabbage":          ["produce_root_veg"],
    "cucumber":         ["produce_root_veg"],
    "mushrooms":        ["produce_mushrooms"],
    "mushroom":         ["produce_mushrooms"],
    "lemon":            ["produce_tropical"],
    "white wine":       ["drinks_wine"],
    "red wine":         ["drinks_wine"],
    "rosemary":         ["spices_herbs_spice_mix"],
    "basil":            ["spices_herbs_spice_mix", "produce_herbs_salads_sprouts"],
    "basil leaves":     ["spices_herbs_spice_mix", "produce_herbs_salads_sprouts"],
    "parsley":          ["spices_herbs_spice_mix", "produce_herbs_salads_sprouts"],
    "thyme":            ["spices_herbs_spice_mix"],
    "cumin seeds":      ["spices_herbs_spice_mix"],
    "ginger":           ["spices_herbs_spice_mix"],
    "balsamic vinegar": ["oils_vinegar"],
    "white chocolate chips": ["sweets_chocolate_bars"],
}

# Kuvamiseks kasutatavad poenimed
CHAIN_DISPLAY_NAMES = {
    "rimi": "Rimi",
    "selver": "Selver",
    "prisma": "Prisma",
    "coop": "Coop",
    "maxima": "Maxima",
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


async def find_products_per_store_for_ingredient(db, ingredient_en: str) -> dict:
    """
    Tagastab iga poe parima (odavaima) toote antud koostisosa jaoks.
    Võti = chain, väärtus = tooteinfo dict.
    """
    name_lower = ingredient_en.lower().strip()
    if name_lower in SKIP_INGREDIENTS:
        return {}

    search_terms = SEARCH_TERMS.get(name_lower, [])
    if not search_terms:
        translated = translate_ingredient(ingredient_en)
        if translated != ingredient_en:
            search_terms = [translated]
        else:
            return {}

    sub_codes = INGREDIENT_SUB_CODES.get(name_lower)

    results_by_chain = {}

    for term in search_terms:
        if sub_codes:
            rows = await db.fetch("""
                SELECT
                    p.id,
                    p.name,
                    p.chain,
                    p.image_url,
                    p.brand,
                    p.size_text,
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
                    p.id,
                    p.name,
                    p.chain,
                    p.image_url,
                    p.brand,
                    p.size_text,
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
                # Võta ainult odavaim toode igast poest
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

        if results_by_chain:
            break  # Esimene otsingutermin andis tulemusi, piisab

    return results_by_chain


async def find_product_for_ingredient(db, ingredient_en: str):
    """Tagastab odavaima toote (legacy endpoint jaoks)."""
    per_store = await find_products_per_store_for_ingredient(db, ingredient_en)
    if not per_store:
        return None
    # Tagasta globaalselt odavaim
    return min(per_store.values(), key=lambda x: x["price"])


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
async def get_recipe_compare(meal_id: str, request: Request):
    """
    Tagastab retsepti hinna võrdluse poodide kaupa.

    Vastus:
    {
      "meal_id": "...",
      "recipe_name": "...",
      "total_ingredients": 6,
      "stores": [
        {
          "chain": "rimi",
          "display_name": "Rimi",
          "covered": 5,          // mitu koostisosa leiti
          "total": 6,            // kokku koostisosasid
          "total_price": 8.40,
          "products": [
            {
              "ingredient_name": "spaghetti",
              "product_id": 123,
              "name": "Barilla Spaghetti 500g",
              "price": 1.29,
              ...
            }
          ],
          "not_found": ["parmesan"]   // mis puudub sellest poest
        },
        ...
      ]
    }
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

    # Leia iga koostisosa jaoks tooted poodide kaupa
    # ingredient_results[i] = {chain: product_dict}
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
            "by_chain": per_store,  # {chain: product}
        })

    # Kogu kõik unikaalsed poed
    all_chains = set()
    for ir in ingredient_results:
        all_chains.update(ir["by_chain"].keys())

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

        store_summaries.append({
            "chain": chain,
            "display_name": CHAIN_DISPLAY_NAMES.get(chain, chain.title()),
            "covered": len(products),
            "total": len(ingredient_results),
            "total_price": round(total_price, 2),
            "products": products,
            "not_found": not_found,
        })

    # Sorteeri: enim kaetud ees, seejärel odavaim
    store_summaries.sort(key=lambda x: (-x["covered"], x["total_price"]))

    return {
        "meal_id": meal_id,
        "recipe_name": estonian_name,
        "total_ingredients": len(ingredient_results),
        "stores": store_summaries,
    }


@router.get("/recipes/{meal_id}/basket")
async def get_recipe_basket(meal_id: str, request: Request):
    """Legacy endpoint — tagastab globaalselt odavaima toote iga koostisosa jaoks."""
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

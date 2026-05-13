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


async def find_product_for_ingredient(db, ingredient_en: str):
    name_lower = ingredient_en.lower().strip()
    if name_lower in SKIP_INGREDIENTS:
        return None

    search_terms = SEARCH_TERMS.get(name_lower, [])
    if not search_terms:
        translated = translate_ingredient(ingredient_en)
        if translated != ingredient_en:
            search_terms = [translated]
        else:
            return None

    for term in search_terms:
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
            GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
            ORDER BY min_price ASC
            LIMIT 1
        """, f"%{term}%")

        if rows:
            r = rows[0]
            return {
                "product_id": r["id"],
                "name": r["name"],
                "chain": r["chain"],
                "image_url": r["image_url"] or "",
                "brand": r["brand"] or "",
                "size_text": r["size_text"] or "",
                "is_per_kg": (r["size_text"] or "").lower() == "kg",
                "price": float(r["min_price"]) if r["min_price"] else 0.0,
                "quantity": 1,
            }

    return None


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


@router.get("/recipes/{meal_id}/basket")
async def get_recipe_basket(meal_id: str, request: Request):
    """Tagastab retsepti koostisosade põhjal päris tooted andmebaasist."""
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

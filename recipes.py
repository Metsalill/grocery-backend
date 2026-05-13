import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter()

THEMEALDB_BASE = "https://www.themealdb.com/api/json/v1/1"

# 10 retsepti otsitakse nime järgi TheMealDB-st
FEATURED_MEAL_NAMES = [
    "Spaghetti Carbonara",
    "Creme Brulee",
    "Lasagne",
    "Pizza Margherita",
    "Buffalo Wings",
    "Sushi",
    "Mussels with Fennel",
    "Beef and Mustard Pie",
    "Stuffed Capsicum",
    "Fettuccine Alfredo",
]

# Eestikeelsed nimed
ESTONIAN_NAMES = {
    "Spaghetti Carbonara": "Spaghetti Carbonara",
    "Creme Brulee": "Creme Brûlée",
    "Lasagne": "Lasanje",
    "Pizza Margherita": "Margherita Pizza",
    "Buffalo Wings": "Kanatiivad",
    "Sushi": "Sushi",
    "Mussels with Fennel": "Sinimerekarbid",
    "Beef and Mustard Pie": "Ühepajatoit veiselihaga",
    "Stuffed Capsicum": "Täidisega paprika (kapsarullid)",
    "Fettuccine Alfredo": "Alfredo pasta",
}

INGREDIENT_TRANSLATIONS = {
    "chicken": "kana",
    "chicken breast": "kanafileed",
    "chicken breasts": "kanafileed",
    "chicken thighs": "kanareis",
    "chicken wings": "kanatiivad",
    "whole chicken": "terve kana",
    "beef": "veiseliha",
    "ground beef": "veisehakkliha",
    "minced beef": "veisehakkliha",
    "pork": "sealiha",
    "bacon": "peekon",
    "pancetta": "peekon",
    "salmon": "lõhe",
    "pasta": "pasta",
    "spaghetti": "spaghetti",
    "fettuccine": "fettuccine",
    "lasagne sheets": "lasanjeplaadid",
    "rice": "riis",
    "sushi rice": "sushi riis",
    "nori": "nori-lehed",
    "potato": "kartul",
    "potatoes": "kartulid",
    "onion": "sibul",
    "onions": "sibulad",
    "red onion": "punane sibul",
    "spring onion": "roheline sibul",
    "garlic": "küüslauk",
    "garlic clove": "küüslauguküüs",
    "garlic cloves": "küüslauguküüned",
    "tomato": "tomat",
    "tomatoes": "tomatid",
    "cherry tomatoes": "kirsstomatid",
    "tomato puree": "tomatipasta",
    "tomato sauce": "tomatikaste",
    "tinned tomatoes": "konservtomatid",
    "chopped tomatoes": "hakitud tomatid",
    "olive oil": "oliiviõli",
    "oil": "õli",
    "vegetable oil": "taimeõli",
    "butter": "või",
    "milk": "piim",
    "cream": "koor",
    "double cream": "vahukoor",
    "heavy cream": "vahukoor",
    "whipping cream": "vahukoor",
    "egg": "muna",
    "eggs": "munad",
    "egg yolks": "munakollased",
    "egg yolk": "munakollane",
    "cheese": "juust",
    "parmesan": "parmesan",
    "pecorino": "pecorino",
    "mozzarella": "mozzarella",
    "cheddar": "cheddar",
    "ricotta": "ricotta",
    "mascarpone": "mascarpone",
    "salt": "sool",
    "pepper": "pipar",
    "black pepper": "must pipar",
    "white pepper": "valge pipar",
    "sugar": "suhkur",
    "caster sugar": "peensuhkur",
    "vanilla extract": "vaniljeekstrakt",
    "vanilla sugar": "vanillsuhkur",
    "flour": "jahu",
    "plain flour": "nisujahu",
    "water": "vesi",
    "stock": "puljong",
    "chicken stock": "kanapuljong",
    "beef stock": "veisepuljong",
    "vegetable stock": "köögiviljapuljong",
    "carrot": "porgand",
    "carrots": "porgandid",
    "celery": "seller",
    "fennel": "apteegitill",
    "spinach": "spinat",
    "mushroom": "seen",
    "mushrooms": "seened",
    "red pepper": "punane paprika",
    "green pepper": "roheline paprika",
    "capsicum": "paprika",
    "bell pepper": "paprika",
    "lemon": "sidrun",
    "lemon juice": "sidrunimahl",
    "lime": "laim",
    "avocado": "avokaado",
    "cucumber": "kurk",
    "ginger": "ingver",
    "soy sauce": "sojakaste",
    "rice vinegar": "riisiäädikas",
    "sesame oil": "seesamiõli",
    "sesame seeds": "seesamiseemned",
    "wasabi": "wasabi",
    "honey": "mesi",
    "vinegar": "äädikas",
    "mustard": "sinep",
    "dijon mustard": "dijoni sinep",
    "mayonnaise": "majonees",
    "hot sauce": "kuum kaste",
    "worcestershire sauce": "Worcestershire kaste",
    "breadcrumbs": "riivsai",
    "bread": "leib",
    "basil": "basiilik",
    "parsley": "petersell",
    "oregano": "pune",
    "thyme": "tüümian",
    "bay leaves": "loorberilehed",
    "bay leaf": "loorberileht",
    "mussels": "merekarbid",
    "white wine": "valge vein",
    "red wine": "punane vein",
    "tomato paste": "tomatipasta",
    "cinnamon": "kaneel",
    "nutmeg": "muskaatpähkel",
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


@router.get("/recipes")
async def get_recipes():
    """Tagastab 10 retsepti TheMealDB-st"""
    recipes = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        for meal_name in FEATURED_MEAL_NAMES:
            try:
                resp = await client.get(
                    f"{THEMEALDB_BASE}/search.php",
                    params={"s": meal_name}
                )
                data = resp.json()
                if not data.get("meals"):
                    continue
                meal = data["meals"][0]
                name_et = ESTONIAN_NAMES.get(meal_name, meal["strMeal"])
                recipes.append({
                    "id": meal["idMeal"],
                    "name": name_et,
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


@router.get("/recipes/{meal_id}")
async def get_recipe(meal_id: str):
    """Tagastab ühe retsepti täisdetailidega"""
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
            data = resp.json()
            meal = data["meals"][0]
            name_et = next(
                (et for en, et in ESTONIAN_NAMES.items()
                 if en.lower() in meal["strMeal"].lower()),
                meal["strMeal"]
            )
            return {
                "id": meal["idMeal"],
                "name": name_et,
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

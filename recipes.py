import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter()

THEMEALDB_BASE = "https://www.themealdb.com/api/json/v1/1"

# (otsingusõna TheMealDB-s, eestikeelne nimi)
FEATURED_MEALS = [
    ("Spaghetti Carbonara", "Spaghetti Carbonara"),
    ("White chocolate creme brulee", "Creme Brûlée"),
    ("Lasagne", "Lasanje"),
    ("Pizza Express Margherita", "Margherita Pizza"),
    ("Buffalo Wings", "Kanatiivad"),
    ("Sushi", "Sushi"),
    ("Mussels with Fennel", "Sinimerekarbid"),
    ("Beef and Mustard Pie", "Ühepajatoit veiselihaga"),
    ("Stuffed Capsicum", "Täidisega paprika"),
    ("Fettuccine Alfredo", "Alfredo pasta"),
]

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
    "tomato paste": "tomatipasta",
    "tinned tomatoes": "konservtomatid",
    "chopped tomatoes": "hakitud tomatid",
    "olive oil": "oliiviõli",
    "oil": "õli",
    "vegetable oil": "taimeõli",
    "rapeseed oil": "rapsiõli",
    "butter": "või",
    "milk": "piim",
    "cream": "koor",
    "double cream": "vahukoor",
    "heavy cream": "vahukoor",
    "whipping cream": "vahukoor",
    "creme fraiche": "hapukoor",
    "egg": "muna",
    "eggs": "munad",
    "egg yolks": "munakollased",
    "egg yolk": "munakollane",
    "cheese": "juust",
    "parmesan": "parmesan",
    "parmesan cheese": "parmesan",
    "pecorino": "pecorino",
    "mozzarella": "mozzarella",
    "mozzarella balls": "mozzarella",
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
    "vanilla": "vaniljekaun",
    "flour": "jahu",
    "plain flour": "nisujahu",
    "puff pastry": "lehttainas",
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
    "green beans": "rohelised oad",
    "lemon": "sidrun",
    "lemon juice": "sidrunimahl",
    "lime": "laim",
    "avocado": "avokaado",
    "cucumber": "kurk",
    "ginger": "ingver",
    "soy sauce": "sojakaste",
    "rice wine": "riisivein",
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
    "basil leaves": "basiilik",
    "parsley": "petersell",
    "oregano": "pune",
    "thyme": "tüümian",
    "bay leaves": "loorberilehed",
    "bay leaf": "loorberileht",
    "mussels": "merekarbid",
    "white wine": "valge vein",
    "red wine": "punane vein",
    "white chocolate chips": "valge šokolaad",
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
    recipes = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        for search_name, estonian_name in FEATURED_MEALS:
            try:
                resp = await client.get(
                    f"{THEMEALDB_BASE}/search.php",
                    params={"s": search_name}
                )
                data = resp.json()
                if not data.get("meals"):
                    continue
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


@router.get("/recipes/{meal_id}")
async def get_recipe(meal_id: str):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{THEMEALDB_BASE}/lookup.php?i={meal_id}")
            data = resp.json()
            meal = data["meals"][0]
            return {
                "id": meal["idMeal"],
                "name": meal["strMeal"],
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

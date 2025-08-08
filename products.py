from fastapi import APIRouter
from geopy.distance import geodesic

router = APIRouter()


def format_price(price):
    return round(float(price), 2)


@router.get("/products")
async def list_products(request):
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.name as store, p.product, p.price, p.manufacturer, p.amount, p.image_url, p.note 
            FROM prices p
            JOIN stores s ON p.store_id = s.id
            ORDER BY s.name
        """)
    return [
        {
            "store": row["store"],
            "product": row["product"],
            "price": format_price(row["price"]),
            "manufacturer": row["manufacturer"],
            "amount": row["amount"],
            "image_url": row["image_url"],
            "note": row["note"]
        }
        for row in rows
    ]


@router.get("/search-products")
async def search_products(query: str, request):
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT product, image_url 
            FROM prices 
            WHERE LOWER(product) ILIKE '%' || LOWER($1) || '%' 
            ORDER BY product 
            LIMIT 10
        """, query)
    return [{"name": row["product"], "image": row["image_url"]} for row in rows]

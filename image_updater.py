
import os
import asyncpg
import aiohttp
import asyncio
import pandas as pd
from urllib.parse import quote
from dotenv import load_dotenv
load_dotenv()

# Load DB URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/grocerydb")

# Search engines and backup logic
async def fetch_image(session, query):
    search_urls = [
        f"https://www.selver.ee/search/?q={quote(query)}",
        f"https://www.coop.ee/search?q={quote(query)}",
        f"https://www.prisma.ee/search?q={quote(query)}",
        f"https://www.rimi.ee/search?q={quote(query)}",
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    for url in search_urls:
        try:
            async with session.get(url, timeout=10, headers=headers) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Extremely basic match for images (needs improving in real deployment)
                    if "https://images." in text:
                        start = text.find("https://images.")
                        end = text.find(".jpg", start)
                        if start != -1 and end != -1:
                            return text[start:end+4]
        except Exception:
            continue
    return "missing.jpg"

async def update_images():
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT product FROM prices WHERE image_url IS NULL OR image_url = 'missing.jpg'")
        print(f"Found {len(rows)} products to update.")
        if not rows:
            return

        async with aiohttp.ClientSession() as session:
            for row in rows:
                product = row["product"]
                image_url = await fetch_image(session, product)
                note = "Kontrolli visuaali!" if image_url == "missing.jpg" else ""
                await conn.execute(
                    "UPDATE prices SET image_url = $1, note = $2 WHERE product = $3",
                    image_url, note, product
                )
                print(f"Updated: {product} -> {image_url}")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(update_images())

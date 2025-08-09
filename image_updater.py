import os
import asyncpg
import asyncio
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/grocerydb")

async def update_images():
    pool = await asyncpg.create_pool(DATABASE_URL)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT product
                FROM prices
                WHERE image_url IS NULL OR image_url = ''
            """)
            print(f"Found {len(rows)} products without images.")
            if not rows:
                return

            for row in rows:
                product = row["product"]

                # Set placeholder + flag for manual check
                image_url = "missing.jpg"
                note = "Kontrolli visuaali!"

                await conn.execute(
                    """
                    UPDATE prices
                    SET image_url = $1, note = $2
                    WHERE product = $3
                    """,
                    image_url, note, product
                )
                print(f"Flagged: {product} -> missing.jpg")

    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(update_images())

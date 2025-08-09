import os
import asyncio
import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/grocerydb")

PLACEHOLDER_URL = "missing.jpg"          # keep consistent with app usage
REVIEW_NOTE     = "Kontrolli visuaali!"  # only set if note is empty/null

async def update_images():
    pool = await asyncpg.create_pool(DATABASE_URL)
    try:
        async with pool.acquire() as conn:
            # How many rows currently missing images?
            missing = await conn.fetchval("""
                SELECT COUNT(*) FROM prices
                WHERE image_url IS NULL OR image_url = ''
            """)
            print(f"Found {missing} rows without images.")

            if missing == 0:
                print("Nothing to do.")
                return

            # Bulk update in one shot:
            status = await conn.execute("""
                UPDATE prices
                   SET image_url = $1,
                       note = COALESCE(NULLIF(note, ''), $2)
                 WHERE image_url IS NULL OR image_url = ''
            """, PLACEHOLDER_URL, REVIEW_NOTE)

            # asyncpg returns status like "UPDATE 123"
            updated = int(status.split()[-1]) if status else 0
            print(f"✅ Flagged {updated} rows with placeholder image and review note.")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(update_images())

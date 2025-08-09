import os
import re
import asyncpg
import aiohttp
import asyncio
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection string (Railway provides DATABASE_URL)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/grocerydb")

# Compile a simple image URL regex (jpg/jpeg/png/webp)
IMG_RE = re.compile(r'https?://[^\s"\'<>]+?\.(?:jpg|jpeg|png|webp)', re.IGNORECASE)

# Prefer CDN/Barbora image hosts if multiple images are present
def pick_best_image(urls):
    if not urls:
        return None
    # Prefer barbora/cdn-looking URLs
    preferred = [u for u in urls if "barbora" in u or "cdn" in u]
    return (preferred[0] if preferred else urls[0])

async def fetch_barbora_image(session: aiohttp.ClientSession, query: str) -> str | None:
    """
    Try Barbora.ee search once and return a best-guess image URL, or None if not found.
    Barbora has used different paths; try a few common ones.
    """
    search_urls = [
        f"https://barbora.ee/otsing?searchTerm={quote(query)}",
        f"https://barbora.ee/search?searchTerm={quote(query)}",
        f"https://barbora.ee/search?query={quote(query)}",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ImageUpdater/1.0; +https://example.com)"
    }

    for url in search_urls:
        try:
            async with session.get(url, timeout=12, headers=headers) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text()

                # Find all candidate image URLs
                candidates = IMG_RE.findall(html)

                # Deduplicate while preserving order
                seen = set()
                deduped = []
                for u in candidates:
                    if u not in seen:
                        seen.add(u)
                        deduped.append(u)

                best = pick_best_image(deduped)
                if best:
                    return best
        except Exception:
            # Ignore and try next variant
            continue

    return None

async def update_images():
    pool = await asyncpg.create_pool(DATABASE_URL)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT product
                FROM prices
                WHERE image_url IS NULL OR image_url = 'missing.jpg'
            """)
            print(f"Found {len(rows)} products to update.")
            if not rows:
                return

            async with aiohttp.ClientSession() as session:
                for row in rows:
                    product = row["product"]
                    found_url = await fetch_barbora_image(session, product)

                    if found_url:
                        image_url = found_url
                        note = ""  # looks good
                    else:
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
                    print(f"Updated: {product} -> {image_url} ({'OK' if found_url else 'FLAGGED'})")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(update_images())

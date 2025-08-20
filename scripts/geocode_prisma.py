# scripts/geocode_prisma.py
import os, time, asyncpg, asyncio, aiohttp

PRISMA_ADDR = {
  "Maardu Prisma": "Keemikute tee 43, Maardu, Estonia",
  "Narva Prisma": "Kangelaste prospekt 29, Narva, Estonia",
  "Rapla Prisma": "Risti 1, Rapla, Estonia",
  "Kristiine Prisma": "Endla 45, Tallinn, Estonia",
  "Lasnamäe Prisma": "Mustakivi tee 17, Tallinn, Estonia",
  "Mustamäe Prisma": "Karjavälja 4, Tallinn, Estonia",
  "Rocca al Mare Prisma": "Paldiski mnt 102, Tallinn, Estonia",
  "Roo Prisma": "Roo tee 1, 76912, Estonia",
  "Sikupilli Prisma": "Tartu mnt 87, Tallinn, Estonia",
  "Tiskre Prisma": "Liiva tee 61, 76916, Estonia",
  "Vanalinna Prisma": "Aia 3, Tallinn, Estonia",
  "Annelinna Prisma": "Nõlvaku 2, Tartu, Estonia",
  "Sõbra Prisma": "Sõbra 58, Tartu, Estonia",
}

UA = "yourapp/1.0 (marko@minetech.ee)"  # set your contact email per Nominatim policy
NOMINATIM = "https://nominatim.openstreetmap.org/search"

async def geocode(session, q):
    params = {"q": q, "format": "json", "limit": 1, "addressdetails": 0}
    async with session.get(NOMINATIM, params=params, headers={"User-Agent": UA}) as r:
        r.raise_for_status()
        data = await r.json()
        return (float(data[0]["lat"]), float(data[0]["lon"])) if data else (None, None)

async def main():
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(dsn, timeout=30)
    async with pool.acquire() as conn, aiohttp.ClientSession() as session:
        for name, addr in PRISMA_ADDR.items():
            lat, lon = await geocode(session, addr)
            if lat and lon:
                await conn.execute(
                    "UPDATE public.stores SET lat=$1, lon=$2 WHERE name=$3",
                    lat, lon, name
                )
                print(f"OK {name}: {lat:.6f}, {lon:.6f}")
            else:
                print(f"MISS {name}: {addr}")
            time.sleep(1)  # gentle rate limit
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

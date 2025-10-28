import csv
import asyncio
import asyncpg
import sys
from datetime import datetime

# EDIT THESE to match however you connect in main.py right now
PG_DSN = "postgresql://USER:PASSWORD@HOST:PORT/DATABASE"

INSERT_SQL = """
INSERT INTO staging_products_raw (
    store_chain,
    store_name,
    store_channel,
    ext_id,
    ean_raw,
    sku_raw,
    name,
    size_text,
    brand,
    manufacturer,
    price,
    currency,
    image_url,
    category_path,
    category_leaf,
    source_url,
    scraped_at
) VALUES (
    $1,$2,$3,
    $4,$5,$6,
    $7,$8,$9,$10,
    $11,$12,
    $13,$14,$15,
    $16, NOW()
)
"""

async def load_csv_into_staging(pool, path):
    print(f"loading {path} ...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            for r in rows:
                # pull columns defensively so missing keys don't explode
                store_chain   = r.get("store_chain") or r.get("chain") or ""
                store_name    = r.get("store_name") or ""
                store_channel = r.get("store_channel") or r.get("channel") or ""
                ext_id        = r.get("ext_id") or r.get("id") or r.get("external_id") or ""
                ean_raw       = r.get("ean_raw") or r.get("ean") or ""
                sku_raw       = r.get("sku_raw") or r.get("sku") or ""
                name          = r.get("name") or r.get("product_name") or ""
                size_text     = r.get("size_text") or r.get("size") or ""
                brand         = r.get("brand") or r.get("manufacturer") or ""
                manufacturer  = r.get("manufacturer") or ""
                price_raw     = r.get("price") or r.get("unit_price") or None
                currency      = r.get("currency") or "EUR"
                image_url     = r.get("image_url") or r.get("image") or ""
                category_path = r.get("category_path") or r.get("category_full") or ""
                category_leaf = r.get("category_leaf") or r.get("category") or ""
                source_url    = r.get("source_url") or r.get("url") or ""

                # cast price to decimal-ish numeric
                if price_raw in (None, ""):
                    price_val = None
                else:
                    try:
                        price_val = float(str(price_raw).replace(",", "."))
                    except:
                        price_val = None

                await conn.execute(
                    INSERT_SQL,
                    store_chain,
                    store_name,
                    store_channel,
                    ext_id,
                    ean_raw,
                    sku_raw,
                    name,
                    size_text,
                    brand,
                    manufacturer,
                    price_val,
                    currency,
                    image_url,
                    category_path,
                    category_leaf,
                    source_url,
                )

    print(f"done. inserted {len(rows)} rows from {path}")

async def main():
    if len(sys.argv) < 2:
        print("usage: python load_staging.py <file1.csv> [file2.csv ...]")
        sys.exit(1)

    pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=4)
    try:
        for csv_path in sys.argv[1:]:
            await load_csv_into_staging(pool, csv_path)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

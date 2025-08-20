# scripts/ingest_selver.py
import os, csv, asyncio, asyncpg
from decimal import Decimal

CSV_PATH = os.getenv("SELVER_CSV", "data/selver-sample.csv")
DATABASE_URL = os.getenv("DATABASE_URL")
RESET_STAGING = os.getenv("RESET_STAGING", "false").lower() in ("1", "true", "yes")

UPSERT_STAGE_SQL = """
INSERT INTO public.staging_selver_products (ext_id, name, ean_raw, size_text, price, currency, collected_at)
VALUES ($1, $2, $3, $4, $5, COALESCE($6,'EUR'), now())
ON CONFLICT (ext_id) DO UPDATE
SET name = EXCLUDED.name,
    ean_raw = EXCLUDED.ean_raw,
    size_text = EXCLUDED.size_text,
    price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    collected_at = EXCLUDED.collected_at
"""

INSERT_PRICES_SQL = """
INSERT INTO public.prices (store_id, product_id, price, currency, collected_at)
SELECT
  s.id AS store_id,
  pe.product_id,
  st.price,
  st.currency,
  st.collected_at
FROM public.staging_selver_products st
JOIN public.product_eans pe ON pe.ean_norm = st.ean_norm
JOIN public.stores s       ON s.name = 'Selver e-pood'
"""

async def main():
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is not set")
    if not os.path.exists(CSV_PATH):
        raise SystemExit(f"CSV file not found: {CSV_PATH}")

    conn = await asyncpg.connect(DATABASE_URL)

    if RESET_STAGING:
        await conn.execute("TRUNCATE public.staging_selver_products")

    # upsert CSV rows into staging
    batch = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"ext_id", "name", "price"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise SystemExit(f"CSV must have columns at least: {', '.join(sorted(required))}")
        for row in reader:
            ext_id    = row.get("ext_id", "").strip()
            name      = row.get("name", "").strip()
            ean_raw   = (row.get("ean") or row.get("ean_raw") or "").strip()
            size_text = (row.get("size_text") or "").strip()
            price_txt = (row.get("price") or "").replace(",", ".").strip()
            if not ext_id or not name or not price_txt:
                continue
            price = Decimal(price_txt)
            currency = (row.get("currency") or "EUR").strip().upper()
            batch.append((ext_id, name, ean_raw, size_text, price, currency))

    if batch:
        await conn.executemany(UPSERT_STAGE_SQL, batch)

    # insert prices for EAN-matched rows (append-only; your views pick latest)
    await conn.execute(INSERT_PRICES_SQL)

    # quick tallies
    total_stage = await conn.fetchval("SELECT COUNT(*) FROM public.staging_selver_products")
    matched     = await conn.fetchval("""
        SELECT COUNT(*) FROM public.staging_selver_products st
        JOIN public.product_eans pe ON pe.ean_norm = st.ean_norm
    """)
    print(f"Upserted to staging: {len(batch)}; staging total now: {total_stage}; EAN matched: {matched}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())

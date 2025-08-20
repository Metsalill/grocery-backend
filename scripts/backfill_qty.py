# scripts/backfill_qty.py
import asyncpg, re, os, asyncio

PACK_RE = re.compile(r'^\s*(\d+)\s*[x×]\s*([\d.,]+)\s*(ml|l|g|kg)\s*$', re.I)
SIMPLE_RE = re.compile(r'^\s*([\d.,]+)\s*(ml|l|g|kg)\s*$', re.I)

def to_base(q, unit):
    q = float(str(q).replace(',', '.'))
    u = unit.lower()
    if u == 'kg': return q * 1000, 'g'
    if u == 'l':  return q * 1000, 'ml'
    if u == 'g':  return q, 'g'
    if u == 'ml': return q, 'ml'
    raise ValueError(f'Unsupported unit: {unit}')

def parse(size_text: str):
    if not size_text:
        return None
    m = PACK_RE.match(size_text)
    if m:
        pack = int(m.group(1))
        q, base = to_base(m.group(2), m.group(3))
        return {'pack_count': pack, 'net_qty': q, 'net_unit': base, 'pack_pattern': m.group(0).strip()}
    m = SIMPLE_RE.match(size_text)
    if m:
        q, base = to_base(m.group(1), m.group(2))
        return {'pack_count': 1, 'net_qty': q, 'net_unit': base, 'pack_pattern': None}
    return None

async def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    # normalize scheme for asyncpg
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]

    # Connect with SSL (required for public Railway URLs from GitHub Actions)
    conn = await asyncpg.connect(dsn=url, ssl=True, timeout=30)

    # Ensure we hit the expected schema; also qualify tables below for safety
    await conn.execute("SET search_path TO public")

    # Fetch products
    rows = await conn.fetch("SELECT id, size_text FROM public.products")
    print(f"Found {len(rows)} products")

    updated = 0
    for r in rows:
        p = parse(r['size_text'] or '')
        if not p:
            continue
        await conn.execute(
            """
            UPDATE public.products
            SET pack_count=$1, net_qty=$2, net_unit=$3, pack_pattern=$4
            WHERE id=$5
            """,
            p['pack_count'], p['net_qty'], p['net_unit'], p['pack_pattern'], r['id']
        )
        updated += 1

    await conn.close()
    print(f"Updated {updated} products")

if __name__ == "__main__":
    asyncio.run(main())

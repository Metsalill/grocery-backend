# scripts/backfill_qty.py
import asyncpg, re, math, os, asyncio

PACK_RE = re.compile(r'^\s*(\d+)\s*[x×]\s*([\d.,]+)\s*(ml|l|g|kg)\s*$', re.I)
SIMPLE_RE = re.compile(r'^\s*([\d.,]+)\s*(ml|l|g|kg)\s*$', re.I)

def to_base(q, unit):
    q = float(str(q).replace(',', '.'))
    u = unit.lower()
    if u == 'kg': return q * 1000, 'g'
    if u == 'l':  return q * 1000, 'ml'
    return q, {'g':'g','ml':'ml'}[u]

def parse(size_text: str):
    if not size_text: return None
    m = PACK_RE.match(size_text)
    if m:
        pack = int(m.group(1)); q, base = to_base(m.group(2), m.group(3))
        return {'pack_count': pack, 'net_qty': q, 'net_unit': base, 'pack_pattern': m.group(0).strip()}
    m = SIMPLE_RE.match(size_text)
    if m:
        q, base = to_base(m.group(1), m.group(2))
        return {'pack_count': 1, 'net_qty': q, 'net_unit': base, 'pack_pattern': None}
    return None

async def main():
    db = await asyncpg.connect(os.getenv("DATABASE_URL"))
    rows = await db.fetch("SELECT id, size_text FROM products")
    for r in rows:
        p = parse(r['size_text'] or '')
        if p:
            await db.execute("""
              UPDATE products SET pack_count=$1, net_qty=$2, net_unit=$3, pack_pattern=$4
              WHERE id=$5
            """, p['pack_count'], p['net_qty'], p['net_unit'], p['pack_pattern'], r['id'])
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())

# scripts/backfill_qty.py
import asyncpg, re, os, asyncio, ssl
from urllib.parse import urlparse, parse_qs

PACK_RE = re.compile(r'^\s*(\d+)\s*[x×]\s*([\d.,]+)\s*(ml|l|g|kg)\s*$', re.I)
SIMPLE_RE = re.compile(r'^\s*([\d.,]+)\s*(ml|l|g|kg)\s*$', re.I)

def to_base(q, unit):
    q = float(str(q).replace(',', '.'))
    u = unit.lower()
    if u == 'kg': return q * 1000, 'g'
    if u == 'l':  return q * 1000, 'ml'
    if u in ('g', 'ml'): return q, u
    raise ValueError(f'Unsupported unit: {unit}')

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

def ssl_context_for(url: str) -> ssl.SSLContext | None:
    """
    Emulate libpq sslmode semantics for asyncpg.
    - require/prefer/allow -> encrypt without verifying CA/hostname
    - verify-ca/verify-full -> verify (needs a trusted CA)
    - disable/allow (no TLS) -> return None (not recommended over internet)
    """
    q = parse_qs(urlparse(url).query)
    mode = (q.get('sslmode', ['require'])[0] or 'require').lower()
    if mode in ('disable',):
        return None
    ctx = ssl.create_default_context()
    if mode in ('require', 'prefer', 'allow'):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    # else verify-ca / verify-full keep defaults (verify)
    return ctx

async def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    ctx = ssl_context_for(url)

    conn = await asyncpg.connect(dsn=url, ssl=ctx, timeout=30)
    await conn.execute("SET search_path TO public")

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

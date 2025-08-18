# stores.py
from fastapi import APIRouter, Depends, Query
import asyncpg
from typing import Optional, List
from settings import get_db_pool

router = APIRouter(prefix="/stores", tags=["stores"])

@router.get("/nearby")
async def nearby_stores(
    lat: float = Query(..., description="Origin latitude"),
    lon: float = Query(..., description="Origin longitude"),
    radius_km: float = Query(10.0, ge=0.1, le=100.0),
    limit: int = Query(50, ge=1, le=200),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    """
    Returns stores within radius_km, ordered by distance.
    Requires cube + earthdistance extensions and the GiST index from the migration.
    """
    sql = """
    SELECT
      s.id,
      s.name,
      s.chain,
      s.lat,
      s.lon,
      earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) / 1000.0 AS distance_km
    FROM stores s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      AND earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) <= ($3::float8 * 1000.0)
    ORDER BY distance_km ASC, s.id
    LIMIT $4
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, float(lat), float(lon), float(radius_km), int(limit))
    return [dict(r) for r in rows]

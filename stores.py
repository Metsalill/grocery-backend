# stores.py
from fastapi import APIRouter, Depends, Query, HTTPException
import asyncpg
from asyncpg import exceptions as pgerr
from typing import List
import math

from settings import get_db_pool
from utils.throttle import throttle

router = APIRouter(prefix="/stores", tags=["stores"])

# Simple Python fallback if earthdistance isn't available
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = p2 - p1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

@router.get("/nearby")
@throttle(limit=60, window=60)  # keep-friendly default
async def nearby_stores(
    lat: float = Query(..., description="Origin latitude"),
    lon: float = Query(..., description="Origin longitude"),
    radius_km: float = Query(10.0, ge=0.1, le=100.0),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    """
    Returns stores within radius_km, ordered by distance ASC.
    Uses Postgres earthdistance if present; falls back to Python haversine.
    """
    if pool is None:
        raise HTTPException(status_code=500, detail="DB not ready")

    sql_earth = """
    SELECT
      s.id,
      s.name,
      s.lat,
      s.lon,
      earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) / 1000.0 AS distance_km
    FROM stores s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      AND earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) <= ($3::float8 * 1000.0)
    ORDER BY distance_km ASC, s.id
    OFFSET $4
    LIMIT  $5
    """

    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                sql_earth, float(lat), float(lon), float(radius_km), int(offset), int(limit)
            )
            items = [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "lat": float(r["lat"]) if r["lat"] is not None else None,
                    "lon": float(r["lon"]) if r["lon"] is not None else None,
                    "distance_km": round(float(r["distance_km"]), 2) if r["distance_km"] is not None else None,
                }
                for r in rows
            ]
            return {"items": items, "offset": offset, "limit": limit}
        except (pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
            # Fallback: compute in Python if earthdistance isn't available
            all_rows = await conn.fetch(
                "SELECT id, name, lat, lon FROM stores WHERE lat IS NOT NULL AND lon IS NOT NULL"
            )

    # Python fallback distance calculation + filter + sort + paginate
    computed = []
    for r in all_rows:
        d = _haversine_km(lat, lon, float(r["lat"]), float(r["lon"]))
        if d <= radius_km:
            computed.append({
                "id": r["id"],
                "name": r["name"],
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "distance_km": round(d, 2),
            })
    computed.sort(key=lambda x: (x["distance_km"], x["id"]))
    return {
        "items": computed[offset: offset + limit],
        "offset": offset,
        "limit": limit,
    }

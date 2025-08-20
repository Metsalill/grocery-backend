# stores.py
from fastapi import APIRouter, Depends, Query, HTTPException
import asyncpg
from asyncpg import exceptions as pgerr
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
    Prefers Postgres cube+earthdistance with earth_box prefilter; falls back to earthdistance-only;
    finally falls back to Python haversine with SQL bbox prefilter.
    """
    if pool is None:
        raise HTTPException(status_code=500, detail="DB not ready")

    # Bounding box (used in Python fallback and optional SQL prefilter)
    lat_deg = radius_km / 111.0
    # avoid division by zero near the poles
    cos_lat = max(math.cos(math.radians(lat)), 1e-6)
    lon_deg = radius_km / (111.0 * cos_lat)
    min_lat, max_lat = lat - lat_deg, lat + lat_deg
    min_lon, max_lon = lon - lon_deg, lon + lon_deg

    sql_earth_box = """
    -- Requires: CREATE EXTENSION cube; CREATE EXTENSION earthdistance;
    SELECT
      s.id,
      s.name,
      s.chain,
      s.lat,
      s.lon,
      earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) / 1000.0 AS distance_km
    FROM stores s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      -- fast prefilter using earth_box to limit to a circle's bounding region
      AND earth_box(ll_to_earth($1::float8, $2::float8), $3::float8 * 1000.0) @> ll_to_earth(s.lat, s.lon)
      -- exact circle filter
      AND earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) <= ($3::float8 * 1000.0)
    ORDER BY distance_km ASC, s.id
    OFFSET $4
    LIMIT  $5
    """

    sql_earth_simple = """
    -- Works with earthdistance (cube optional but not required here)
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
    OFFSET $4
    LIMIT  $5
    """

    async with pool.acquire() as conn:
        # Try fast path with earth_box prefilter
        try:
            rows = await conn.fetch(
                sql_earth_box, float(lat), float(lon), float(radius_km), int(offset), int(limit)
            )
            items = [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "chain": r["chain"],
                    "lat": float(r["lat"]) if r["lat"] is not None else None,
                    "lon": float(r["lon"]) if r["lon"] is not None else None,
                    "distance_km": round(float(r["distance_km"]), 2) if r["distance_km"] is not None else None,
                }
                for r in rows
            ]
            return {"items": items, "offset": offset, "limit": limit}
        except (pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
            # Fall back to earthdistance-only (no cube/earth_box)
            try:
                rows = await conn.fetch(
                    sql_earth_simple, float(lat), float(lon), float(radius_km), int(offset), int(limit)
                )
                items = [
                    {
                        "id": r["id"],
                        "name": r["name"],
                        "chain": r["chain"],
                        "lat": float(r["lat"]) if r["lat"] is not None else None,
                        "lon": float(r["lon"]) if r["lon"] is not None else None,
                        "distance_km": round(float(r["distance_km"]), 2) if r["distance_km"] is not None else None,
                    }
                    for r in rows
                ]
                return {"items": items, "offset": offset, "limit": limit}
            except (pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
                # Final fallback: Python haversine with SQL bbox prefilter
                all_rows = await conn.fetch(
                    """
                    SELECT id, name, chain, lat, lon
                    FROM stores
                    WHERE lat IS NOT NULL AND lon IS NOT NULL
                      AND lat BETWEEN $1 AND $2
                      AND lon BETWEEN $3 AND $4
                    """,
                    float(min_lat), float(max_lat), float(min_lon), float(max_lon),
                )

    # Python fallback distance calculation + filter + sort + paginate
    computed = []
    for r in all_rows:
        d = _haversine_km(lat, lon, float(r["lat"]), float(r["lon"]))
        if d <= radius_km:
            computed.append({
                "id": r["id"],
                "name": r["name"],
                "chain": r["chain"],
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

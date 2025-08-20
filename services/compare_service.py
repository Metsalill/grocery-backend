# services/compare_service.py
from __future__ import annotations
import asyncpg
from asyncpg import exceptions as pgerr
from typing import List, Tuple, Dict, Any, Optional
from decimal import Decimal, ROUND_HALF_UP

# ---------------- helpers ----------------

def _round2(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def _norm(s: str) -> str:
    return s.strip().lower()

def _rv(rec: asyncpg.Record, key: str, default=None):
    try:
        return rec[key]
    except KeyError:
        return default

def _unit_price_line(prod: asyncpg.Record, price_one: float) -> Optional[float]:
    """
    price per g/ml for ONE sell unit.
    total_net_qty = COALESCE(pack_count,1) * net_qty
    Returns None if qty data not available.
    """
    net_qty = _rv(prod, "net_qty")
    if net_qty is None:
        return None
    pack_count = _rv(prod, "pack_count") or 1
    try:
        total_qty = float(net_qty) * float(pack_count)
    except Exception:
        return None
    if total_qty <= 0:
        return None
    return price_one / total_qty

# ---------------- data access ----------------

async def _candidate_stores(
    conn: asyncpg.Connection,
    lat: Optional[float],
    lon: Optional[float],
    radius_km: float,
    limit: int,
    offset: int,
) -> List[asyncpg.Record]:
    """
    Returns rows: id, name, chain, lat, lon, distance_km (NULL if no geo).
    Tries earth_box fast-path → earth_distance-only → fallback (no-geo).
    """
    if lat is None or lon is None:
        return await conn.fetch(
            """
            SELECT id, name, chain, lat, lon, NULL::double precision AS distance_km
            FROM stores
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY id
            OFFSET $1 LIMIT $2
            """,
            int(offset), int(limit),
        )

    sql_earth_box = """
    SELECT
      s.id, s.name, s.chain, s.lat, s.lon,
      earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) / 1000.0 AS distance_km
    FROM stores s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      AND earth_box(ll_to_earth($1::float8, $2::float8), $3::float8 * 1000.0) @> ll_to_earth(s.lat, s.lon)
      AND earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) <= ($3::float8 * 1000.0)
    ORDER BY distance_km ASC, s.id
    OFFSET $4 LIMIT $5
    """
    sql_earth_simple = """
    SELECT
      s.id, s.name, s.chain, s.lat, s.lon,
      earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) / 1000.0 AS distance_km
    FROM stores s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      AND earth_distance(ll_to_earth($1::float8, $2::float8), ll_to_earth(s.lat, s.lon)) <= ($3::float8 * 1000.0)
    ORDER BY distance_km ASC, s.id
    OFFSET $4 LIMIT $5
    """
    try:
        return await conn.fetch(sql_earth_box, float(lat), float(lon), float(radius_km), int(offset), int(limit))
    except (pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
        try:
            return await conn.fetch(sql_earth_simple, float(lat), float(lon), float(radius_km), int(offset), int(limit))
        except (pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
            # sane fallback: no geo filtering, paged
            return await conn.fetch(
                """
                SELECT id, name, chain, lat, lon, NULL::double precision AS distance_km
                FROM stores
                WHERE lat IS NOT NULL AND lon IS NOT NULL
                ORDER BY id
                OFFSET $1 LIMIT $2
                """,
                int(offset), int(limit),
            )

async def _resolve_products(conn: asyncpg.Connection, names: List[str]) -> Dict[str, asyncpg.Record]:
    """
    Exact (case-insensitive) name resolution.
    Returns mapping normalized_name -> product row with needed fields.
    Tries to fetch qty fields; falls back to minimal set if columns not present.
    """
    if not names:
        return {}
    uniq = sorted({ _norm(n) for n in names if n and n.strip() })

    try:
        rows = await conn.fetch(
            """
            SELECT id, name, size_text, net_qty, net_unit, pack_count
            FROM products
            WHERE lower(name) = ANY($1::text[])
            """,
            uniq,
        )
    except (pgerr.UndefinedColumnError, pgerr.UndefinedTableError):
        rows = await conn.fetch(
            """
            SELECT id, name, size_text
            FROM products
            WHERE lower(name) = ANY($1::text[])
            """,
            uniq,
        )

    by_norm: Dict[str, asyncpg.Record] = {}
    for r in rows:
        by_norm[_norm(r["name"])] = r
    return by_norm

async def _latest_prices(
    conn: asyncpg.Connection,
    product_ids: List[int],
    store_ids: List[int],
) -> List[asyncpg.Record]:
    """
    Latest price per (product_id, store_id) via view v_latest_store_prices.
    """
    if not product_ids or not store_ids:
        return []
    return await conn.fetch(
        """
        SELECT product_id, store_id, price, collected_at
        FROM v_latest_store_prices
        WHERE product_id = ANY($1::int[])
          AND store_id  = ANY($2::int[])
        """,
        product_ids, store_ids,
    )

# --------------- public service ----------------

async def compare_basket_service(
    pool,
    items: List[Tuple[str, int]],
    lat: float,
    lon: float,
    radius_km: float,
    *,
    limit_stores: int = 50,
    offset_stores: int = 0,
    include_lines: bool = True,
    require_all_items: bool = True,
) -> Dict[str, Any]:
    """
    Core compare logic:
      1) pick candidate stores by geo
      2) resolve products
      3) fetch latest prices for (product, store)
      4) compute totals per store
    Returns dict consumed by compare.py endpoint.
    """
    async with pool.acquire() as conn:
        # 1) candidate stores
        stores = await _candidate_stores(
            conn=conn,
            lat=float(lat) if lat is not None else None,
            lon=float(lon) if lon is not None else None,
            radius_km=float(radius_km),
            limit=int(limit_stores),
            offset=int(offset_stores),
        )
        if not stores:
            return {
                "results": [],
                "totals": {"candidate_stores": 0, "stores_ok": 0},
                "stores": [],
                "radius_km": float(radius_km),
                "missing_products": [],
            }

        # 2) resolve products (keep original order, aggregate quantities by normalized name)
        requested_names: List[str] = []
        qty_by_norm: Dict[str, int] = {}
        for (name, qty) in items:
            if not name or not str(name).strip():
                continue
            n = _norm(name)
            requested_names.append(name)
            qty_by_norm[n] = qty_by_norm.get(n, 0) + int(qty or 1)

        by_norm = await _resolve_products(conn, requested_names)
        missing_products = [n for n in { _norm(x) for x in requested_names } if n not in by_norm]

        if not by_norm:
            return {
                "results": [],
                "totals": {"candidate_stores": len(stores), "stores_ok": 0},
                "stores": [
                    {
                        "id": s["id"],
                        "name": s["name"],
                        "chain": s["chain"],
                        "distance_km": _round2(float(s["distance_km"])) if s["distance_km"] is not None else None,
                        "lat": s["lat"],
                        "lon": s["lon"],
                    } for s in stores
                ],
                "radius_km": float(radius_km),
                "missing_products": missing_products,
            }

        # preserve order for lines; unique ids for price query
        prod_rows_ordered = [by_norm[_norm(n)] for n in requested_names if _norm(n) in by_norm]
        product_ids_unique = sorted({ int(r["id"]) for r in prod_rows_ordered })
        store_ids = [int(s["id"]) for s in stores]

        # 3) latest prices
        latest = await _latest_prices(conn, product_ids_unique, store_ids)
        prices_by_store: Dict[int, Dict[int, float]] = {}
        for r in latest:
            sid = int(r["store_id"])
            pid = int(r["product_id"])
            prices_by_store.setdefault(sid, {})[pid] = float(r["price"])

    # 4) compute totals per store
    results: List[Dict[str, Any]] = []
    for s in stores:
        sid = int(s["id"])
        store_prices = prices_by_store.get(sid, {})
        total = 0.0
        missing_for_store: List[str] = []
        lines: List[Dict[str, Any]] = []

        for name in requested_names:
            n = _norm(name)
            prod = by_norm.get(n)
            if not prod:
                continue
            pid = int(prod["id"])
            qty = int(qty_by_norm.get(n, 1))

            price_one = store_prices.get(pid)
            if price_one is None:
                missing_for_store.append(prod["name"])
                continue

            line_total = float(price_one) * qty
            total += line_total

            if include_lines:
                unit_price = _unit_price_line(prod, float(price_one))
                lines.append({
                    "product": prod["name"],
                    "qty": qty,
                    "price_each": _round2(price_one),
                    "line_total": _round2(line_total),
                    "unit_price": _round2(unit_price) if unit_price is not None else None,
                    "size_text": _rv(prod, "size_text"),
                })

        total_price = _round2(total) if (total > 0 or not require_all_items) else None
        if require_all_items and missing_for_store:
            total_price = None  # push incomplete baskets to bottom

        results.append({
            "store_id": sid,
            "store_name": s["name"],
            "chain": s["chain"],
            "distance_km": _round2(float(s["distance_km"])) if s["distance_km"] is not None else None,
            "total_price": total_price,
            "missing_items": missing_for_store,
            **({"lines": lines} if include_lines else {}),
        })

    # sort: complete baskets first (total_price not None), then by price asc
    results.sort(key=lambda r: (r["total_price"] is None, r["total_price"] if r["total_price"] is not None else 0))

    # summary totals
    cheapest = next((r for r in results if r["total_price"] is not None), None)
    totals = {
        "candidate_stores": len(stores),
        "stores_ok": sum(1 for r in results if r["total_price"] is not None),
        "cheapest_total": cheapest["total_price"] if cheapest else None,
        "cheapest_store_id": cheapest["store_id"] if cheapest else None,
    }

    return {
        "results": results,
        "totals": totals,
        "stores": [
            {
                "id": s["id"],
                "name": s["name"],
                "chain": s["chain"],
                "distance_km": _round2(float(s["distance_km"])) if s["distance_km"] is not None else None,
                "lat": s["lat"],
                "lon": s["lon"],
            } for s in stores
        ],
        "radius_km": float(radius_km),
        "missing_products": missing_products,
    }

# services/compare_service.py
from __future__ import annotations

import asyncio
import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from asyncpg import exceptions as pgerr


# ---------------- helpers ----------------


def _round2(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _rv(r: asyncpg.Record, key: str) -> Any:
    # record value getter with key existence check
    try:
        return r[key]
    except Exception:
        # asyncpg.Record supports attributes too sometimes
        return getattr(r, key)


async def _acquire(conn_or_pool: Any) -> Tuple[asyncpg.Connection, bool]:
    """
    Accept either an asyncpg.Connection or asyncpg.Pool, and return
    a connection plus a flag telling whether we should release it.
    """
    if isinstance(conn_or_pool, asyncpg.Connection):
        return conn_or_pool, False
    # asyncpg pool duck-typing
    if hasattr(conn_or_pool, "acquire"):
        conn = await conn_or_pool.acquire()
        return conn, True
    raise TypeError("Expected asyncpg Connection or Pool")


# ---------------- product resolution ----------------


async def _resolve_products(conn: asyncpg.Connection, names: List[str]) -> Dict[str, asyncpg.Record]:
    """
    Resolve user-provided product names (or EAN strings) to a product row.
    Prefers product_aliases -> products by normalized name; falls back to
    matching products.name; additionally accepts EAN-only strings.
    Returns dict keyed by normalized original input.
    """
    if not names:
        return {}

    keys_raw = [n for n in names if n and str(n).strip()]
    keys = sorted({_norm(n) for n in keys_raw})
    if not keys:
        return {}

    sql_with_aliases = """
    WITH keys AS (SELECT unnest($1::text[]) AS k)
    SELECT DISTINCT ON (keys.k)
      keys.k AS match_key,
      p.id, p.ean, p.name, p.size_text, p.net_qty, p.net_unit, p.pack_count
    FROM products p
    LEFT JOIN product_aliases a ON a.product_id = p.id
    JOIN keys ON keys.k = lower(p.name) OR keys.k = lower(a.alias)
    ORDER BY keys.k, p.id
    """
    sql_products_only = """
    WITH keys AS (SELECT unnest($1::text[]) AS k)
    SELECT DISTINCT ON (keys.k)
      keys.k AS match_key,
      p.id, p.ean, p.name, p.size_text, p.net_qty, p.net_unit, p.pack_count
    FROM products p
    JOIN keys ON keys.k = lower(p.name)
    ORDER BY keys.k, p.id
    """

    try:
        rows = await conn.fetch(sql_with_aliases, keys)
    except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError):
        rows = await conn.fetch(sql_products_only, keys)

    by_norm: Dict[str, asyncpg.Record] = { _rv(r, "match_key"): r for r in rows }

    # EAN fallback for unresolved
    unresolved = [k for k in keys if k not in by_norm]
    ean_candidates = [k for k in unresolved if k.isdigit() and 8 <= len(k) <= 14]
    if ean_candidates:
        ean_rows = await conn.fetch(
            """
            WITH keys AS (SELECT unnest($1::text[]) AS ean_norm)
            SELECT DISTINCT ON (keys.ean_norm)
              keys.ean_norm AS match_key,
              p.id, p.ean, p.name, p.size_text, p.net_qty, p.net_unit, p.pack_count
            FROM products p
            JOIN keys ON regexp_replace(p.ean,'\\D','','g') = keys.ean_norm
            ORDER BY keys.ean_norm, p.id
            """,
            ean_candidates,
        )
        for r in ean_rows:
            by_norm[_rv(r, "match_key")] = r

    return by_norm


# ---------------- stores ----------------


async def _candidate_stores(
    conn: asyncpg.Connection,
    lat: Optional[float],
    lon: Optional[float],
    radius_km: float,
    limit: int,
    offset: int,
) -> List[asyncpg.Record]:
    """
    Return stores within radius_km of (lat,lon) using haversine. If lat/lon
    are None, return all stores ordered by id with NULL distance.
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

    # pure trig haversine (no extensions required)
    sql2 = """
    WITH params(lat,lon,radius_km) AS (VALUES ($1::float8, $2::float8, $3::float8)),
    with_dist AS (
      SELECT
        s.id, s.name, s.chain, s.lat, s.lon,
        2*6371*asin(
          sqrt(
            pow(sin(radians((s.lat - (SELECT lat FROM params))/2)),2) +
            cos(radians((SELECT lat FROM params))) * cos(radians(s.lat)) *
            pow(sin(radians((s.lon - (SELECT lon FROM params))/2)),2)
          )
        ) AS distance_km
      FROM stores s
      WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
    )
    SELECT * FROM with_dist
    WHERE distance_km <= (SELECT radius_km FROM params)
    ORDER BY distance_km, chain, name
    OFFSET $4 LIMIT $5;
    """
    return await conn.fetch(sql2, float(lat), float(lon), float(radius_km), int(offset), int(limit))


# ---------------- prices (effective) ----------------


async def _latest_prices(
    conn: asyncpg.Connection,
    product_ids: List[int],
    store_ids: List[int],
) -> List[asyncpg.Record]:
    """
    Latest price per (product_id, *physical* store_id), honoring store_host_map.
    Tries v_latest_store_prices first (if present). Falls back to raw prices.
    """
    if not product_ids or not store_ids:
        return []

    sql_using_view = """
    WITH effective_source AS (
      SELECT s.id AS physical_store_id,
             COALESCE(em.host_store_id, s.id) AS source_store_id
      FROM stores s
      LEFT JOIN (
        SELECT store_id, host_store_id
        FROM (
          SELECT shm.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY shm.store_id
                   ORDER BY (CASE WHEN shm.active THEN 0 ELSE 1 END),
                            COALESCE(shm.priority, 999999),
                            shm.host_store_id
                 ) AS rn
          FROM store_host_map shm
        ) z
        WHERE rn = 1
      ) em ON em.store_id = s.id
      WHERE s.id = ANY($2::int[])
    )
    SELECT lsp.product_id,
           es.physical_store_id AS store_id,
           lsp.price,
           lsp.collected_at
    FROM effective_source es
    JOIN v_latest_store_prices lsp
      ON lsp.store_id = es.source_store_id
     AND lsp.product_id = ANY($1::int[]);
    """

    sql_from_prices = """
    WITH effective_source AS (
      SELECT s.id AS physical_store_id,
             COALESCE(em.host_store_id, s.id) AS source_store_id
      FROM stores s
      LEFT JOIN (
        SELECT store_id, host_store_id
        FROM (
          SELECT shm.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY shm.store_id
                   ORDER BY (CASE WHEN shm.active THEN 0 ELSE 1 END),
                            COALESCE(shm.priority, 999999),
                            shm.host_store_id
                 ) AS rn
          FROM store_host_map shm
        ) z
        WHERE rn = 1
      ) em ON em.store_id = s.id
      WHERE s.id = ANY($2::int[])
    ),
    latest AS (
      SELECT p.product_id, p.store_id, p.price, p.collected_at,
             ROW_NUMBER() OVER (
               PARTITION BY p.product_id, p.store_id
               ORDER BY p.collected_at DESC
             ) AS rn
      FROM prices p
      WHERE p.product_id = ANY($1::int[])
        AND p.store_id IN (SELECT source_store_id FROM effective_source)
    )
    SELECT l.product_id,
           es.physical_store_id AS store_id,
           l.price, l.collected_at
    FROM latest l
    JOIN effective_source es ON es.source_store_id = l.store_id
    WHERE l.rn = 1;
    """

    try:
        return await conn.fetch(sql_using_view, product_ids, store_ids)
    except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError, pgerr.UndefinedObjectError):
        return await conn.fetch(sql_from_prices, product_ids, store_ids)


# ---------------- main service ----------------


async def compare_basket_service(db: Any, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform location-based basket comparison.

    Body structure expected:
    {
      "grocery_list": { "items": [ { "product": "Nutella 400g", "quantity": 1 }, ... ] },
      "lat": 59.4370,
      "lon": 24.7536,
      "radius_km": 8,
      "limit_stores": 50,
      "offset_stores": 0,
      "include_lines": false,
      "require_all_items": false
    }
    """
    conn, should_release = await _acquire(db)
    try:
        gl = (body or {}).get("grocery_list") or {}
        items = gl.get("items") or []
        lat = body.get("lat")
        lon = body.get("lon")
        radius_km = float(body.get("radius_km") or 5.0)
        limit_stores = int(body.get("limit_stores") or 50)
        offset_stores = int(body.get("offset_stores") or 0)
        include_lines = bool(body.get("include_lines") or False)
        require_all = bool(body.get("require_all_items") or False)

        # --- normalize and collapse quantities per input key ---
        wanted: Dict[str, int] = {}
        for it in items:
            name = _norm(str(it.get("product", "")))
            if not name:
                continue
            qty = int(it.get("quantity") or 1)
            wanted[name] = wanted.get(name, 0) + max(qty, 1)

        if not wanted:
            return {
                "results": [],
                "totals": {},
                "stores": [],
                "radius_km": radius_km,
                "missing_products": [],
            }

        # --- resolve products ---
        resolved = await _resolve_products(conn, list(wanted.keys()))
        missing_keys = [k for k in wanted.keys() if k not in resolved]
        missing_products = [{"input": k} for k in missing_keys]

        product_ids: List[int] = [int(_rv(r, "id")) for r in resolved.values()]
        if not product_ids:
            return {
                "results": [],
                "totals": {},
                "stores": [],
                "radius_km": radius_km,
                "missing_products": missing_products,
            }

        # maintain mapping key->product for later
        key_to_product: Dict[str, asyncpg.Record] = resolved

        # --- candidate stores ---
        stores = await _candidate_stores(conn, lat, lon, radius_km, limit_stores, offset_stores)
        if not stores:
            return {
                "results": [],
                "totals": {},
                "stores": [],
                "radius_km": radius_km,
                "missing_products": missing_products,
            }
        store_ids = [int(_rv(s, "id")) for s in stores]

        # --- prices (effective sources) ---
        price_rows = await _latest_prices(conn, product_ids, store_ids)
        # structure: (store_id -> product_id -> price)
        by_store: Dict[int, Dict[int, float]] = {}
        for r in price_rows:
            sid = int(_rv(r, "store_id"))
            pid = int(_rv(r, "product_id"))
            price = float(_rv(r, "price"))
            by_store.setdefault(sid, {})[pid] = price

        # --- per-store totals ---
        required = len(product_ids)  # required lines = resolved items count
        results: List[Dict[str, Any]] = []
        best_total: Optional[float] = None
        best_store_id: Optional[int] = None

        for s in stores:
            sid = int(_rv(s, "id"))
            s_prices = by_store.get(sid, {})
            lines = []
            total = 0.0
            lines_found = 0

            # build lines based on *resolved* products, not raw inputs
            for key, qty in wanted.items():
                pr = key_to_product.get(key)
                if not pr:
                    continue
                pid = int(_rv(pr, "id"))
                unit_price = s_prices.get(pid)
                if unit_price is None:
                    continue
                line_total = unit_price * qty
                lines_found += 1
                total += line_total
                if include_lines:
                    lines.append({
                        "product_id": pid,
                        "ean": _rv(pr, "ean"),
                        "product_name": _rv(pr, "name"),
                        "qty": qty,
                        "unit_price": _round2(unit_price),
                        "line_total": _round2(line_total),
                    })

            total_price: Optional[float] = _round2(total) if (not require_all or lines_found == required) else None

            result = {
                "store_id": sid,
                "chain": _rv(s, "chain"),
                "store_name": _rv(s, "name"),
                "distance_km": _round2(float(_rv(s, "distance_km"))) if _rv(s, "distance_km") is not None else None,
                "lines_found": lines_found,
                "required_lines": required,
                "total_price": total_price,
            }
            if include_lines:
                result["lines"] = lines

            results.append(result)

            if total_price is not None and (best_total is None or total_price < best_total):
                best_total = total_price
                best_store_id = sid

        # sort: complete first, then lines, then price, then distance
        def sort_key(x: Dict[str, Any]) -> Tuple[int, int, float, float]:
            complete = 1 if (x.get("total_price") is not None and x.get("lines_found") == x.get("required_lines")) else 0
            price = x.get("total_price") if x.get("total_price") is not None else float("inf")
            dist = x.get("distance_km") if x.get("distance_km") is not None else float("inf")
            return (-complete, -int(x.get("lines_found", 0)), price, dist)

        results.sort(key=sort_key)

        totals: Dict[str, Any] = {}
        if best_total is not None and best_store_id is not None:
            # include winning store meta
            win = next((r for r in results if r["store_id"] == best_store_id), None)
            if win:
                totals = {
                    "cheapest_store_id": best_store_id,
                    "cheapest_total": win["total_price"],
                    "cheapest_chain": win["chain"],
                    "cheapest_store_name": win["store_name"],
                }

        return {
            "results": results,
            "totals": totals,
            "stores": [
                {
                    "id": int(_rv(s, "id")),
                    "name": _rv(s, "name"),
                    "chain": _rv(s, "chain"),
                    "distance_km": _round2(float(_rv(s, "distance_km"))) if _rv(s, "distance_km") is not None else None,
                    "lat": float(_rv(s, "lat")) if _rv(s, "lat") is not None else None,
                    "lon": float(_rv(s, "lon")) if _rv(s, "lon") is not None else None,
                }
                for s in stores
            ],
            "radius_km": float(radius_km),
            "missing_products": missing_products,
        }
    finally:
        if should_release:
            await db.release(conn)

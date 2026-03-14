# services/compare_service.py
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple, Iterable

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
    try:
        return r[key]
    except Exception:
        return getattr(r, key, None)


async def _acquire(conn_or_pool: Any) -> Tuple[asyncpg.Connection, bool]:
    if isinstance(conn_or_pool, asyncpg.Connection):
        return conn_or_pool, False
    if hasattr(conn_or_pool, "acquire"):
        conn = await conn_or_pool.acquire()
        return conn, True
    raise TypeError("Expected asyncpg Connection or Pool")


# ---------------- product resolution ----------------


async def _resolve_products_by_name(
    conn: asyncpg.Connection,
    names: List[str],
) -> Dict[str, asyncpg.Record]:
    if not names:
        return {}
    keys = sorted({_norm(n) for n in names if n and str(n).strip()})
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

    by_norm: Dict[str, asyncpg.Record] = {_rv(r, "match_key"): r for r in rows}

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


async def _fetch_products_by_id(
    conn: asyncpg.Connection,
    product_ids: Iterable[int],
) -> Dict[int, asyncpg.Record]:
    ids_list = sorted({int(pid) for pid in product_ids if pid is not None})
    if not ids_list:
        return {}
    rows = await conn.fetch(
        "SELECT id, ean, name, size_text, net_qty, net_unit, pack_count "
        "FROM products WHERE id = ANY($1::int[])",
        ids_list,
    )
    return {int(_rv(r, "id")): r for r in rows}


# ---------------- stores ----------------


async def _candidate_stores(
    conn: asyncpg.Connection,
    lat: Optional[float],
    lon: Optional[float],
    radius_km: float,
    limit: int,
    offset: int,
) -> List[asyncpg.Record]:
    host_and_online_filter = """
      AND s.id NOT IN (SELECT DISTINCT host_store_id FROM store_host_map)
      AND COALESCE(s.is_online, false) = false
    """
    online_only_filter = """
      AND COALESCE(s.is_online, false) = false
    """

    async def _no_coords(f: str) -> List[asyncpg.Record]:
        return await conn.fetch(
            f"SELECT id, name, chain, lat, lon, NULL::double precision AS distance_km "
            f"FROM stores s WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL {f} "
            f"ORDER BY id OFFSET $1 LIMIT $2",
            int(offset), int(limit),
        )

    async def _haversine(f: str) -> List[asyncpg.Record]:
        return await conn.fetch(
            f"""
            WITH params(lat,lon,radius_km) AS (VALUES ($1::float8,$2::float8,$3::float8)),
            with_dist AS (
              SELECT s.id, s.name, s.chain, s.lat, s.lon,
                2*6371*asin(sqrt(
                  pow(sin(radians((s.lat-(SELECT lat FROM params))/2)),2)+
                  cos(radians((SELECT lat FROM params)))*cos(radians(s.lat))*
                  pow(sin(radians((s.lon-(SELECT lon FROM params))/2)),2)
                )) AS distance_km
              FROM stores s
              WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL {f}
            )
            SELECT * FROM with_dist
            WHERE distance_km <= (SELECT radius_km FROM params)
            ORDER BY distance_km, chain, name
            OFFSET $4 LIMIT $5
            """,
            float(lat), float(lon), float(radius_km), int(offset), int(limit),
        )

    if lat is None or lon is None:
        try:
            return await _no_coords(host_and_online_filter)
        except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError):
            return await _no_coords(online_only_filter)
    try:
        return await _haversine(host_and_online_filter)
    except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError):
        return await _haversine(online_only_filter)


# ---------------- prices ----------------


async def _latest_prices(
    conn: asyncpg.Connection,
    product_ids: List[int],
    store_ids: List[int],
) -> List[asyncpg.Record]:
    """
    Fetch latest price per (product_id, physical store_id).

    IMPORTANT: we intentionally skip v_latest_store_prices view — it does a
    full 1.7M row scan with no index usage and takes 10+ minutes.

    Instead we use DISTINCT ON with the existing index ix_prices_latest
    (product_id, store_id, collected_at DESC) which is instant.
    """
    if not product_ids or not store_ids:
        return []

    # Fast path: DISTINCT ON uses ix_prices_latest index directly.
    # Resolves store_host_map (source store → physical store mapping).
    sql_distinct_on = """
    WITH effective_source AS (
      SELECT s.id AS physical_store_id,
             COALESCE(em.host_store_id, s.id) AS source_store_id
      FROM stores s
      LEFT JOIN (
        SELECT DISTINCT ON (store_id) store_id, host_store_id
        FROM store_host_map
        WHERE active = true OR active IS NULL
        ORDER BY store_id, COALESCE(priority, 999999), host_store_id
      ) em ON em.store_id = s.id
      WHERE s.id = ANY($2::int[])
    ),
    latest AS (
      SELECT DISTINCT ON (p.product_id, p.store_id)
             p.product_id, p.store_id, p.price, p.collected_at
      FROM prices p
      WHERE p.product_id = ANY($1::int[])
        AND p.store_id IN (SELECT source_store_id FROM effective_source)
      ORDER BY p.product_id, p.store_id, p.collected_at DESC
    )
    SELECT l.product_id,
           es.physical_store_id AS store_id,
           l.price,
           l.collected_at
    FROM latest l
    JOIN effective_source es ON es.source_store_id = l.store_id;
    """

    # Fallback: no store_host_map table at all
    sql_no_host_map = """
    SELECT DISTINCT ON (p.product_id, p.store_id)
           p.product_id, p.store_id, p.price, p.collected_at
    FROM prices p
    WHERE p.product_id = ANY($1::int[])
      AND p.store_id = ANY($2::int[])
    ORDER BY p.product_id, p.store_id, p.collected_at DESC;
    """

    try:
        return await conn.fetch(sql_distinct_on, product_ids, store_ids)
    except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError):
        return await conn.fetch(sql_no_host_map, product_ids, store_ids)


# ---------------- helpers ----------------


def _as_int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    try:
        return int(str(v))
    except Exception:
        return None


# ---------------- main service ----------------


async def compare_basket_service(db: Any, body: Dict[str, Any]) -> Dict[str, Any]:
    conn, should_release = await _acquire(db)
    try:
        # 1. Extract fields
        lat = body.get("lat")
        lon = body.get("lon")
        radius_km = float(body.get("radius_km") or 5.0)
        limit_stores = int(body.get("limit_stores") or 50)
        offset_stores = int(body.get("offset_stores") or 0)
        include_lines = bool(body.get("include_lines") or False)
        require_all = bool(body.get("require_all_items") or False)

        raw_items = body.get("items")
        if raw_items is None and "grocery_list" in body:
            raw_items = (body.get("grocery_list") or {}).get("items")
        raw_items = raw_items or []

        # 2. Normalize
        normalized_items: List[Dict[str, Any]] = []
        for it in raw_items:
            if isinstance(it, dict):
                name = str(it.get("product", "") or "").strip()
                if not name:
                    continue
                qty = int(it.get("quantity") or 1)
                if qty <= 0:
                    continue
                normalized_items.append({
                    "product": name,
                    "quantity": qty,
                    "product_id": _as_int_or_none(it.get("product_id")),
                })
            elif isinstance(it, (list, tuple)) and len(it) >= 1:
                name = str(it[0] or "").strip()
                if not name:
                    continue
                try:
                    qty = int(it[1]) if len(it) > 1 else 1
                except Exception:
                    qty = 1
                if qty <= 0:
                    continue
                normalized_items.append({"product": name, "quantity": qty, "product_id": None})

        if not normalized_items:
            return {"results": [], "totals": {}, "stores": [], "radius_km": radius_km, "missing_products": []}

        # 3. Aggregate by pid or name
        qty_by_pid: Dict[int, int] = {}
        qty_by_name: Dict[str, int] = {}
        for it in normalized_items:
            pid = _as_int_or_none(it.get("product_id"))
            qty = max(int(it.get("quantity") or 1), 1)
            if pid is not None:
                qty_by_pid[pid] = qty_by_pid.get(pid, 0) + qty
            else:
                nm = _norm(str(it.get("product") or ""))
                if nm:
                    qty_by_name[nm] = qty_by_name.get(nm, 0) + qty

        # 4. Resolve names
        resolved_by_name = await _resolve_products_by_name(conn, list(qty_by_name.keys()))
        missing_products = [{"input": k} for k in qty_by_name if k not in resolved_by_name]
        for nm, rec in resolved_by_name.items():
            pid = int(_rv(rec, "id"))
            qty = qty_by_name.get(nm, 0)
            if qty > 0:
                qty_by_pid[pid] = qty_by_pid.get(pid, 0) + qty

        if not qty_by_pid:
            return {"results": [], "totals": {}, "stores": [], "radius_km": radius_km, "missing_products": missing_products}

        # 5. Product metadata
        all_pids = sorted(qty_by_pid.keys())
        metadata = await _fetch_products_by_id(conn, all_pids)
        for nm, rec in resolved_by_name.items():
            pid = int(_rv(rec, "id"))
            if pid not in metadata:
                metadata[pid] = rec

        # 6. Candidate stores
        stores = await _candidate_stores(conn, lat, lon, radius_km, limit_stores, offset_stores)
        if not stores:
            return {"results": [], "totals": {}, "stores": [], "radius_km": radius_km, "missing_products": missing_products}
        store_ids = [int(_rv(s, "id")) for s in stores]

        # 7. Latest prices (fast — skips slow view)
        price_rows = await _latest_prices(conn, all_pids, store_ids)
        by_store: Dict[int, Dict[int, float]] = {}
        for r in price_rows:
            sid = int(_rv(r, "store_id"))
            pid = int(_rv(r, "product_id"))
            by_store.setdefault(sid, {})[pid] = float(_rv(r, "price"))

        # 8. Per-store results
        required = len(all_pids)
        results: List[Dict[str, Any]] = []
        best_total: Optional[float] = None
        best_store_id: Optional[int] = None

        for s in stores:
            sid = int(_rv(s, "id"))
            s_prices = by_store.get(sid, {})
            lines = []
            total = 0.0
            lines_found = 0

            for pid, qty in qty_by_pid.items():
                unit_price = s_prices.get(pid)
                if unit_price is None:
                    continue
                line_total = unit_price * qty
                lines_found += 1
                total += line_total
                if include_lines:
                    meta = metadata.get(pid)
                    lines.append({
                        "product_id": pid,
                        "ean": _rv(meta, "ean") if meta else None,
                        "product_name": _rv(meta, "name") if meta else f"#{pid}",
                        "qty": qty,
                        "unit_price": _round2(unit_price),
                        "line_total": _round2(line_total),
                    })

            total_price = _round2(total) if (not require_all or lines_found == required) else None

            result: Dict[str, Any] = {
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

        # 9. Sort
        def sort_key(x: Dict[str, Any]) -> Tuple[int, int, float, float]:
            complete = 1 if (x.get("total_price") is not None and x.get("lines_found") == x.get("required_lines")) else 0
            price = x.get("total_price") if x.get("total_price") is not None else float("inf")
            dist = x.get("distance_km") if x.get("distance_km") is not None else float("inf")
            return (-complete, -int(x.get("lines_found", 0)), price, dist)

        results.sort(key=sort_key)

        # 10. Totals
        totals: Dict[str, Any] = {}
        if best_total is not None and best_store_id is not None:
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

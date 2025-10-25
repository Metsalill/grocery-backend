# services/compare_service.py
from __future__ import annotations

import asyncio
import math
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
    """
    Safe record value getter. asyncpg.Record lets you use both
    dict-style and attribute-style. We'll try both.
    """
    try:
        return r[key]
    except Exception:
        return getattr(r, key, None)


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
#
# Two paths now:
#   1. We may get explicit product_id directly from the app.
#   2. We may only get free-text names/EANs (legacy / suggestions).
#
# We'll support both. For #1 we just trust product_id and look up its
# metadata by ID. For #2 we try to do fuzzy-ish resolution via aliases.
#


async def _resolve_products_by_name(
    conn: asyncpg.Connection,
    names: List[str],
) -> Dict[str, asyncpg.Record]:
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

    by_norm: Dict[str, asyncpg.Record] = {_rv(r, "match_key"): r for r in rows}

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


async def _fetch_products_by_id(
    conn: asyncpg.Connection,
    product_ids: Iterable[int],
) -> Dict[int, asyncpg.Record]:
    """
    Fetch canonical product metadata for a set of product_ids.
    Returns { product_id -> row }.
    """
    ids_list = sorted({int(pid) for pid in product_ids if pid is not None})
    if not ids_list:
        return {}

    rows = await conn.fetch(
        """
        SELECT id, ean, name, size_text, net_qty, net_unit, pack_count
        FROM products
        WHERE id = ANY($1::int[])
        """,
        ids_list,
    )
    out: Dict[int, asyncpg.Record] = {int(_rv(r, "id")): r for r in rows}
    return out


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
    Return *physical* stores within radius_km of (lat,lon) using haversine,
    excluding:
      - any store used as a host (appears as host_store_id in store_host_map)
      - any store flagged as online (stores.is_online = true)

    If lat/lon are None, return all physical stores (NULL distance) with the same filters.

    Safe fallback: if store_host_map doesn't exist, we still filter by is_online.
    """
    # Common WHERE snippet – we format this into the queries below
    host_and_online_filter = """
      AND s.id NOT IN (SELECT DISTINCT host_store_id FROM store_host_map)
      AND COALESCE(s.is_online, false) = false
    """
    online_only_filter = """
      AND COALESCE(s.is_online, false) = false
    """

    async def _query_no_coords(_filter_sql: str) -> List[asyncpg.Record]:
        return await conn.fetch(
            f"""
            SELECT id, name, chain, lat, lon, NULL::double precision AS distance_km
            FROM stores s
            WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
              {_filter_sql}
            ORDER BY id
            OFFSET $1 LIMIT $2
            """,
            int(offset), int(limit),
        )

    async def _query_haversine(_filter_sql: str) -> List[asyncpg.Record]:
        return await conn.fetch(
            f"""
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
                {_filter_sql}
            )
            SELECT * FROM with_dist
            WHERE distance_km <= (SELECT radius_km FROM params)
            ORDER BY distance_km, chain, name
            OFFSET $4 LIMIT $5;
            """,
            float(lat), float(lon), float(radius_km), int(offset), int(limit),
        )

    # Choose path & handle fallback if store_host_map is missing
    if lat is None or lon is None:
        try:
            return await _query_no_coords(host_and_online_filter)
        except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError):
            # store_host_map missing – filter only online
            return await _query_no_coords(online_only_filter)

    try:
        return await _query_haversine(host_and_online_filter)
    except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError):
        return await _query_haversine(online_only_filter)


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
#
# We now support TWO request shapes:
#
# NEW SHAPE (from /compare endpoint via Flutter):
#   body = {
#     "items": [
#       {"product": "Piim ALMA 2,5%, 0,5L", "quantity": 1, "product_id": 421264},
#       {"product": "Ruks seemnepala Leib, 260g", "quantity": 2, "product_id": 422013},
#       ...
#     ],
#     "lat": ...,
#     "lon": ...,
#     "radius_km": ...,
#     "limit_stores": ...,
#     "offset_stores": ...,
#     "include_lines": ...,
#     "require_all_items": ...
#   }
#
# LEGACY SHAPE (from compute_compare / basket_history etc.):
#   body = {
#     "items": [
#       ("Piim ALMA 2,5%, 0,5L", 1),
#       ("Ruks seemnepala Leib, 260g", 2),
#       ...
#     ],
#     "lat": ...,
#     "lon": ...,
#     "radius_km": ...,
#     ...
#   }
#
# SUPER LEGACY (older code paths we keep graceful fallback for):
#   body = {
#     "grocery_list": {
#        "items": [
#           {"product": "Piim...", "quantity": 1},
#           ...
#        ]
#     },
#     "lat": ...,
#     "lon": ...
#   }
#
# We'll normalize all of these into a single internal basket spec:
#    basket_lines = [ { "pid": <int product_id>, "qty": <int> }, ... ]
# and also collect metadata per pid (name/ean...) for line details.
#


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


async def compare_basket_service(db: Any, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform location-based basket comparison.

    We accept multiple possible shapes (see big comment above),
    normalize them, look up candidate stores and latest prices,
    then compute totals / cheapest store, etc.
    """
    conn, should_release = await _acquire(db)
    try:
        # ---- 1. Extract request fields ----
        # lat/lon/radius/etc.
        lat = body.get("lat")
        lon = body.get("lon")
        radius_km = float(body.get("radius_km") or 5.0)
        limit_stores = int(body.get("limit_stores") or 50)
        offset_stores = int(body.get("offset_stores") or 0)
        include_lines = bool(body.get("include_lines") or False)
        require_all = bool(body.get("require_all_items") or False)

        # items can come in a few formats
        # Prefer body["items"]; fallback to body["grocery_list"]["items"].
        raw_items = body.get("items")
        if raw_items is None and "grocery_list" in body:
            gl = body.get("grocery_list") or {}
            raw_items = gl.get("items")

        raw_items = raw_items or []

        # ---- 2. Normalize items into dicts with product, quantity, product_id ----
        normalized_items: List[Dict[str, Any]] = []

        for it in raw_items:
            # case A: modern dict {product, quantity, product_id?}
            if isinstance(it, dict):
                name = str(it.get("product", "") or "").strip()
                if not name:
                    continue
                qty = int(it.get("quantity") or 1)
                if qty <= 0:
                    continue
                pid = _as_int_or_none(it.get("product_id"))
                normalized_items.append(
                    {
                        "product": name,
                        "quantity": qty,
                        "product_id": pid,
                    }
                )
                continue

            # case B: legacy tuple/list ["Piim ...", 1]
            if isinstance(it, (list, tuple)) and len(it) >= 1:
                name = str(it[0] or "").strip()
                if not name:
                    continue
                qty_raw = it[1] if len(it) > 1 else 1
                try:
                    qty = int(qty_raw)
                except Exception:
                    qty = 1
                if qty <= 0:
                    continue
                normalized_items.append(
                    {
                        "product": name,
                        "quantity": qty,
                        "product_id": None,
                    }
                )
                continue

            # else: ignore unknown shapes

        # short-circuit?
        if not normalized_items:
            return {
                "results": [],
                "totals": {},
                "stores": [],
                "radius_km": radius_km,
                "missing_products": [],
            }

        # ---- 3. Aggregate quantities by either product_id (preferred) or name ----
        #
        # We'll build:
        #   qty_by_pid: { pid:int -> total_qty:int }
        #   qty_by_name: { norm_name:str -> total_qty:int }  # only for lines w/o pid
        #
        qty_by_pid: Dict[int, int] = {}
        qty_by_name: Dict[str, int] = {}
        for it in normalized_items:
            pid = _as_int_or_none(it.get("product_id"))
            qty = int(it.get("quantity") or 1)
            qty = max(qty, 1)

            if pid is not None:
                qty_by_pid[pid] = qty_by_pid.get(pid, 0) + qty
            else:
                nm = _norm(str(it.get("product") or ""))
                if nm:
                    qty_by_name[nm] = qty_by_name.get(nm, 0) + qty

        # ---- 4. Resolve name-only items into product_ids (via aliases/EAN) ----
        resolved_by_name = await _resolve_products_by_name(conn, list(qty_by_name.keys()))

        missing_name_keys = [
            k for k in qty_by_name.keys() if k not in resolved_by_name
        ]
        missing_products = [{"input": k} for k in missing_name_keys]

        # merge resolved name->id into qty_by_pid
        for nm, rec in resolved_by_name.items():
            pid = int(_rv(rec, "id"))
            qty = qty_by_name.get(nm, 0)
            if qty <= 0:
                continue
            qty_by_pid[pid] = qty_by_pid.get(pid, 0) + qty

        # After this, qty_by_pid represents ALL lines we can actually price.
        if not qty_by_pid:
            # nothing priceable
            return {
                "results": [],
                "totals": {},
                "stores": [],
                "radius_km": radius_km,
                "missing_products": missing_products,
            }

        # ---- 5. Fetch product metadata by pid (for *all* pids incl. direct pids) ----
        #
        # We already have some metadata for name-resolved ones in resolved_by_name,
        # but for direct product_id lines we might not. So we load the union.
        all_pids = sorted(qty_by_pid.keys())

        # metadata_from_id: {pid -> row}
        metadata_from_id = await _fetch_products_by_id(conn, all_pids)

        # also fill in from resolved_by_name where we might not have from _fetch_products_by_id
        for nm, rec in resolved_by_name.items():
            pid = int(_rv(rec, "id"))
            if pid not in metadata_from_id:
                metadata_from_id[pid] = rec

        # For any pid where we *still* don't have metadata_from_id
        # (extremely unlikely unless DB is inconsistent), we just won't
        # produce nice line details but we can still total prices.

        # ---- 6. Candidate stores ----
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

        # ---- 7. Latest prices ----
        price_rows = await _latest_prices(conn, all_pids, store_ids)
        # structure: (store_id -> product_id -> price)
        by_store: Dict[int, Dict[int, float]] = {}
        for r in price_rows:
            sid = int(_rv(r, "store_id"))
            pid = int(_rv(r, "product_id"))
            price = float(_rv(r, "price"))
            by_store.setdefault(sid, {})[pid] = price

        # ---- 8. Build per-store results ----
        required = len(all_pids)  # required lines = distinct products we're pricing
        results: List[Dict[str, Any]] = []
        best_total: Optional[float] = None
        best_store_id: Optional[int] = None

        for s in stores:
            sid = int(_rv(s, "id"))
            s_prices = by_store.get(sid, {})
            lines = []
            total = 0.0
            lines_found = 0

            # For each canonical product in the user's basket, compute extended price
            for pid, qty in qty_by_pid.items():
                unit_price = s_prices.get(pid)
                if unit_price is None:
                    continue

                line_total = unit_price * qty
                lines_found += 1
                total += line_total

                if include_lines:
                    meta = metadata_from_id.get(pid)
                    lines.append(
                        {
                            "product_id": pid,
                            "ean": _rv(meta, "ean") if meta else None,
                            "product_name": _rv(meta, "name") if meta else f"#{pid}",
                            "qty": qty,
                            "unit_price": _round2(unit_price),
                            "line_total": _round2(line_total),
                        }
                    )

            total_price: Optional[float] = _round2(total) if (
                not require_all or lines_found == required
            ) else None

            result = {
                "store_id": sid,
                "chain": _rv(s, "chain"),
                "store_name": _rv(s, "name"),
                "distance_km": _round2(float(_rv(s, "distance_km")))
                if _rv(s, "distance_km") is not None
                else None,
                "lines_found": lines_found,
                "required_lines": required,
                "total_price": total_price,
            }
            if include_lines:
                result["lines"] = lines

            results.append(result)

            if total_price is not None and (
                best_total is None or total_price < best_total
            ):
                best_total = total_price
                best_store_id = sid

        # ---- 9. Sort stores ----
        # sort: complete first, then lines_found, then price, then distance
        def sort_key(x: Dict[str, Any]) -> Tuple[int, int, float, float]:
            complete = 1 if (
                x.get("total_price") is not None
                and x.get("lines_found") == x.get("required_lines")
            ) else 0
            price = x.get("total_price") if x.get("total_price") is not None else float("inf")
            dist = x.get("distance_km") if x.get("distance_km") is not None else float("inf")
            return (-complete, -int(x.get("lines_found", 0)), price, dist)

        results.sort(key=sort_key)

        # ---- 10. Totals / winner ----
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

        # ---- 11. Final envelope ----
        return {
            "results": results,
            "totals": totals,
            "stores": [
                {
                    "id": int(_rv(s, "id")),
                    "name": _rv(s, "name"),
                    "chain": _rv(s, "chain"),
                    "distance_km": _round2(float(_rv(s, "distance_km")))
                    if _rv(s, "distance_km") is not None
                    else None,
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

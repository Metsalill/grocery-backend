# services/compare_service.py
from typing import List, Dict, Optional, Tuple, Any
from asyncpg import exceptions as pgerr

# ---------- Helpers ----------
async def _resolve_product_ids_by_name(pool, names: List[str]) -> Dict[str, int]:
    """
    Resolve product names to IDs via `products` table.
    If the table is missing, raise to let caller pick a fallback.
    """
    q = """
      SELECT id AS product_id, product AS name
      FROM products
      WHERE LOWER(product) = ANY($1::text[])
    """
    lowered = [n.strip().lower() for n in names if n and n.strip()]
    async with pool.acquire() as conn:
        rows = await conn.fetch(q, lowered)
    return {r["name"].lower(): int(r["product_id"]) for r in rows}

def _norm_names(items: List[Tuple[str, int]]) -> Tuple[List[str], Dict[str, int]]:
    names = []
    qty_by_lower = {}
    for n, q in items:
        if not n or not str(n).strip():
            continue
        k = str(n).strip().lower()
        names.append(k)
        qty_by_lower[k] = int(q)
    return names, qty_by_lower

# ---------- Main ----------
async def compare_basket_service(
    pool,
    items: List[Tuple[str, int]],
    lat: Optional[float],
    lon: Optional[float],
    radius_km: Optional[float] = 10.0,
    require_all_items: bool = True,
) -> Dict[str, Any]:
    if not items:
        return {"stores": [], "results": [], "totals": {}, "radius_km": radius_km}

    # Normalize input
    names_lower, qty_by_lower = _norm_names(items)
    if not names_lower:
        return {"stores": [], "results": [], "totals": {}, "radius_km": radius_km}

    # -------------------- Path A: ID + view (preferred) --------------------
    # If 'products' or the view/functions are missing, we fall back.
    use_fallback_name_mode = False
    debug_fallback = None

    try:
        name_to_id = await _resolve_product_ids_by_name(pool, names_lower)
    except (pgerr.UndefinedTableError, pgerr.UndefinedObjectError) as e:
        # products table missing -> go to name-based mode
        use_fallback_name_mode = True
        debug_fallback = f"name-mode: {type(e).__name__}: {str(e).splitlines()[0]}"

    if not use_fallback_name_mode:
        missing = [n for n in names_lower if n not in name_to_id]
        if missing:
            # return gracefully (caller may show these to user)
            return {
                "stores": [], "results": [], "totals": {},
                "missing_products": missing, "radius_km": radius_km
            }

        product_ids = [name_to_id[n] for n in names_lower]
        qty_map = {name_to_id[n]: qty_by_lower[n] for n in names_lower}
        needed = len(set(product_ids))

        sql_geo = """
        WITH candidates AS (
          SELECT
            lsp.store_id,
            lsp.product_id,
            lsp.price::numeric AS price
          FROM v_latest_store_prices lsp
          JOIN stores s ON s.id = lsp.store_id
          WHERE lsp.product_id = ANY($1::int[])
            AND earth_distance(ll_to_earth($2::float8, $3::float8),
                               ll_to_earth(s.lat, s.lon)) <= $4::int
        ),
        per_store AS (
          SELECT
            store_id,
            COUNT(DISTINCT product_id) AS have_cnt,
            SUM(price * COALESCE(q.qty, 1))::numeric AS total
          FROM candidates c
          LEFT JOIN (
            SELECT UNNEST($1::int[]) AS pid, UNNEST($5::int[]) AS qty
          ) q ON q.pid = c.product_id
          GROUP BY store_id
        )
        SELECT
          ps.store_id,
          s.name AS store_name,
          earth_distance(ll_to_earth($2::float8, $3::float8),
                         ll_to_earth(s.lat, s.lon)) / 1000.0 AS distance_km,
          ps.total
        FROM per_store ps
        JOIN stores s ON s.id = ps.store_id
        WHERE ($6::bool = FALSE) OR (ps.have_cnt = $7::int)
        ORDER BY ps.total ASC
        """
        params_geo = [
            product_ids,
            float(lat) if lat is not None else None,
            float(lon) if lon is not None else None,
            int((radius_km or 10.0) * 1000),
            [qty_map[pid] for pid in product_ids],
            bool(require_all_items),
            int(needed),
        ]

        sql_nogeo = """
        WITH candidates AS (
          SELECT
            lsp.store_id,
            lsp.product_id,
            lsp.price::numeric AS price
          FROM v_latest_store_prices lsp
          WHERE lsp.product_id = ANY($1::int[])
        ),
        per_store AS (
          SELECT
            store_id,
            COUNT(DISTINCT product_id) AS have_cnt,
            SUM(price * COALESCE(q.qty, 1))::numeric AS total
          FROM candidates c
          LEFT JOIN (
            SELECT UNNEST($1::int[]) AS pid, UNNEST($2::int[]) AS qty
          ) q ON q.pid = c.product_id
          GROUP BY store_id
        )
        SELECT
          ps.store_id,
          s.name AS store_name,
          NULL::float8 AS distance_km,
          ps.total
        FROM per_store ps
        JOIN stores s ON s.id = ps.store_id
        WHERE ($3::bool = FALSE) OR (ps.have_cnt = $4::int)
        ORDER BY ps.total ASC
        """
        params_nogeo = [
            product_ids,
            [qty_map[pid] for pid in product_ids],
            bool(require_all_items),
            int(needed),
        ]

        async with pool.acquire() as conn:
            try:
                if lat is not None and lon is not None and radius_km is not None:
                    rows = await conn.fetch(sql_geo, *params_geo)
                else:
                    rows = await conn.fetch(sql_nogeo, *params_nogeo)
            except (pgerr.UndefinedFunctionError, pgerr.UndefinedTableError, pgerr.UndefinedObjectError) as e:
                # view or earthdistance missing -> fall back to name-based mode
                use_fallback_name_mode = True
                debug_fallback = f"name-mode: {type(e).__name__}: {str(e).splitlines()[0]}"

        if not use_fallback_name_mode:
            # per-item breakdown for BEST store
            breakdown_sql = """
              SELECT product_id, price::numeric AS price
              FROM v_latest_store_prices
              WHERE store_id = $1 AND product_id = ANY($2::int[])
            """
            stores_payload, totals_map = [], {}
            if rows:
                best = rows[0]
                best_store_id = int(best["store_id"])
                async with pool.acquire() as conn:
                    bd = await conn.fetch(breakdown_sql, best_store_id, product_ids)
                price_by_pid = {int(r["product_id"]): float(r["price"]) for r in bd}

                for r in rows:
                    sid = int(r["store_id"])
                    name = r["store_name"]
                    total = float(r["total"])
                    dist = float(r["distance_km"]) if r["distance_km"] is not None else None

                    items_array = []
                    if sid == best_store_id:
                        items_array = [
                            {
                                "product_id": pid,
                                "quantity": next(q for i, pid2 in enumerate(product_ids) if pid2 == pid for q in [params_geo[4][i]]),
                                "price": price_by_pid.get(pid),
                                "line_total": round((price_by_pid.get(pid, 0.0) *
                                                     next(q for i, pid2 in enumerate(product_ids) if pid2 == pid for q in [params_geo[4][i]])), 2),
                            }
                            for pid in product_ids
                        ]

                    stores_payload.append({
                        "store_id": sid,
                        "store_name": name,
                        "total": round(total, 2),
                        "distance_km": None if dist is None else round(dist, 2),
                        "items": items_array,
                    })
                    totals_map[name] = round(total, 2)

            legacy_results = [
                {"store": s["store_name"], "total": s["total"], "distance_km": s["distance_km"]}
                for s in stores_payload
            ]
            out = {
                "stores": stores_payload,
                "results": legacy_results,
                "totals": totals_map,
                "radius_km": radius_km,
            }
            if debug_fallback:
                out["debug_fallback"] = debug_fallback
            return out

    # -------------------- Path B: NAME-based latest prices --------------------
    # Works without `products` table or views.
    # Build latest row per (store_id, product_lower) then sum with quantities.
    qty_array = [qty_by_lower[n] for n in names_lower]

    sql_latest_by_name = """
    WITH latest AS (
      SELECT DISTINCT ON (p.store_id, LOWER(p.product))
        p.store_id,
        LOWER(p.product) AS product_key,
        p.product        AS product_name,
        p.price::numeric AS price,
        p.seen_at
      FROM prices p
      WHERE LOWER(p.product) = ANY($1::text[])
      ORDER BY p.store_id, LOWER(p.product), p.seen_at DESC
    ),
    candidates AS (
      SELECT l.*
      FROM latest l
      JOIN stores s ON s.id = l.store_id
      -- optional geo filter below if we can run it
    )
    SELECT
      c.store_id,
      s.name AS store_name,
      /* distance will be set in outer query if geo works */
      SUM(c.price * q.qty)::numeric AS total
    FROM candidates c
    JOIN stores s ON s.id = c.store_id
    JOIN (
      SELECT UNNEST($1::text[]) AS name_key, UNNEST($2::int[]) AS qty
    ) q ON q.name_key = c.product_key
    GROUP BY c.store_id, s.name
    """
    params = [names_lower, qty_array]

    # Try to apply geo filter via earth_distance; if it fails, we’ll compute without it
    apply_geo_in_sql = (lat is not None and lon is not None and radius_km is not None)
    rows = []
    async with pool.acquire() as conn:
        if apply_geo_in_sql:
            try:
                rows = await conn.fetch(
                    sql_latest_by_name.replace(
                        "-- optional geo filter below if we can run it",
                        "WHERE earth_distance(ll_to_earth($3::float8, $4::float8), ll_to_earth(s.lat, s.lon)) <= $5::int"
                    ),
                    names_lower, qty_array, float(lat), float(lon), int((radius_km or 10.0) * 1000),
                )
                # annotate distance in a second pass
                rows = [
                    dict(r) | {
                        "distance_km": float(await conn.fetchval(
                            "SELECT earth_distance(ll_to_earth($1,$2), ll_to_earth(s.lat, s.lon))/1000.0 FROM stores s WHERE s.id=$3",
                            float(lat), float(lon), r["store_id"]
                        ))
                    }
                    for r in rows
                ]
            except (pgerr.UndefinedFunctionError, pgerr.UndefinedObjectError):
                debug_fallback = (debug_fallback or "") + " | no-earthdistance"
                rows = await conn.fetch(sql_latest_by_name, *params)
        else:
            rows = await conn.fetch(sql_latest_by_name, *params)

    stores_payload, totals_map = [], {}
    for r in rows:
        dist = r.get("distance_km")
        stores_payload.append({
            "store_id": int(r["store_id"]),
            "store_name": r["store_name"],
            "total": round(float(r["total"]), 2),
            "distance_km": None if dist is None else round(float(dist), 2),
            "items": [],  # name-mode keeps it light; can be expanded if needed
        })
        totals_map[r["store_name"]] = round(float(r["total"]), 2)

    # Sort by total asc
    stores_payload.sort(key=lambda x: x["total"])
    legacy_results = [
        {"store": s["store_name"], "total": s["total"], "distance_km": s["distance_km"]}
        for s in stores_payload
    ]
    out = {
        "stores": stores_payload,
        "results": legacy_results,
        "totals": totals_map,
        "radius_km": radius_km,
    }
    if debug_fallback:
        out["debug_fallback"] = debug_fallback.strip()
    return out

# services/compare_service.py
from typing import List, Dict, Optional, Tuple, Any

# --- Resolver: names → product_ids -------------------------------------------
async def resolve_product_ids_by_name(pool, names: List[str]) -> Dict[str, int]:
    """
    Resolve a list of product names to product IDs.
    Currently exact LOWER() matches; you can extend this with fuzzy matching.
    """
    lowered = [n.strip().lower() for n in names if n and n.strip()]
    if not lowered:
        return {}

    q = """
      SELECT id AS product_id, LOWER(product) AS name
      FROM products
      WHERE LOWER(product) = ANY($1::text[])
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(q, lowered)

    return {r["name"]: int(r["product_id"]) for r in rows}


# --- Core service -------------------------------------------------------------
async def compare_basket_service(
    pool,
    items: List[Tuple[str, int]],  # (product_name, quantity)
    lat: Optional[float],
    lon: Optional[float],
    radius_km: Optional[float] = 10.0,
    require_all_items: bool = True,
) -> Dict[str, Any]:
    """
    Compare basket prices across stores using v_latest_store_prices.
    Returns the cheapest store(s), totals, and a breakdown for the best store.
    """

    if not items:
        return {
            "stores": [],
            "results": [],
            "totals": {},
            "radius_km": radius_km,
        }

    # --- 1. Resolve products to IDs ------------------------------------------
    names = [n for (n, _) in items]
    name_to_id = await resolve_product_ids_by_name(pool, names)

    missing_names = [n for n in names if n.strip().lower() not in name_to_id]
    if missing_names:
        # Fail early but report missing names
        return {
            "stores": [],
            "results": [],
            "totals": {},
            "missing_products": missing_names,
            "radius_km": radius_km,
        }

    product_ids = [name_to_id[n.strip().lower()] for n, _ in items]
    qty_map = {name_to_id[n.strip().lower()]: int(q) for n, q in items}
    needed = len(set(product_ids))

    # --- 2. Build query (geo-filtered vs non-geo) ----------------------------
    if lat is not None and lon is not None and radius_km is not None:
        sql = """
        WITH candidates AS (
          SELECT
            lsp.store_id,
            lsp.product_id,
            lsp.price::numeric AS price
          FROM v_latest_store_prices lsp
          JOIN stores s ON s.id = lsp.store_id
          WHERE lsp.product_id = ANY($1::int[])
            AND earth_distance(
                  ll_to_earth($2::float8, $3::float8),
                  ll_to_earth(s.lat, s.lon)
                ) <= $4::int
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
          earth_distance(
            ll_to_earth($2::float8, $3::float8),
            ll_to_earth(s.lat, s.lon)
          ) / 1000.0 AS distance_km,
          ps.total
        FROM per_store ps
        JOIN stores s ON s.id = ps.store_id
        WHERE ($6::bool = FALSE) OR (ps.have_cnt = $7::int)
        ORDER BY ps.total ASC
        """
        params = [
            product_ids,
            float(lat),
            float(lon),
            int((radius_km or 10.0) * 1000),         # meters
            [qty_map[pid] for pid in product_ids],  # quantities aligned
            bool(require_all_items),
            int(needed),
        ]
    else:
        sql = """
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
        params = [
            product_ids,
            [qty_map[pid] for pid in product_ids],
            bool(require_all_items),
            int(needed),
        ]

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    # --- 3. Per-item breakdown for the best store ----------------------------
    stores_payload = []
    totals_map = {}
    if rows:
        best = rows[0]
        best_store_id = int(best["store_id"])

        breakdown_sql = """
          SELECT product_id, price::numeric AS price
          FROM v_latest_store_prices
          WHERE store_id = $1 AND product_id = ANY($2::int[])
        """
        async with pool.acquire() as conn:
            bd = await conn.fetch(breakdown_sql, best_store_id, product_ids)
        price_by_pid = {int(r["product_id"]): float(r["price"]) for r in bd}

        for r in rows:
            sid = int(r["store_id"])
            name = r["store_name"]
            total = float(r["total"])
            dist = float(r["distance_km"]) if r["distance_km"] is not None else None

            if sid == best_store_id:
                items_array = [
                    {
                        "product_id": pid,
                        "quantity": qty_map[pid],
                        "price": price_by_pid.get(pid),
                        "line_total": round((price_by_pid.get(pid, 0.0) * qty_map[pid]), 2),
                    }
                    for pid in product_ids
                ]
            else:
                items_array = []  # lightweight for non-best stores

            stores_payload.append({
                "store_id": sid,
                "store_name": name,
                "total": round(total, 2),
                "distance_km": None if dist is None else round(dist, 2),
                "items": items_array,
            })
            totals_map[name] = round(total, 2)

    # --- 4. Back-compat + response -------------------------------------------
    legacy_results = [
        {
            "store": s["store_name"],
            "total": s["total"],
            "distance_km": s["distance_km"],
        }
        for s in stores_payload
    ]

    return {
        "stores": stores_payload,
        "results": legacy_results,
        "totals": totals_map,
        "radius_km": radius_km,
    }

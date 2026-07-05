# services/compare_service.py
from __future__ import annotations

import os
import json
import httpx
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple, Iterable

import asyncpg
from asyncpg import exceptions as pgerr

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

SKIP_INGREDIENTS = {
    "water", "salt", "black pepper", "pepper", "white pepper",
    "mixed herbs", "seasoning", "oil spray", "to taste",
}

VALID_SUB_CODES = [
    "dry_pasta_rice", "dairy_eggs", "dairy_milk", "dairy_butter_margarine",
    "dairy_cream_sourcream", "dairy_yogurt_kefir", "cheese_regular",
    "cheese_delicatessen", "dairy_cheese_slices", "meat_poultry", "meat_beef_lamb_game",
    "meat_minced", "meat_pork", "meat_hams", "meat_sausages",
    "fish_fresh", "fish_salted_smoked", "fish_other", "fish_processed",
    "produce_root_veg", "produce_mushrooms", "produce_tropical",
    "produce_herbs_salads_sprouts", "produce_smoothies_fresh_juices",
    "dry_flour_sugar_baking", "dry_canned_veg", "dry_other", "dry_ready_meals_jars",
    "frozen_bakery", "frozen_veg", "frozen_berries_fruit", "frozen_ready_meals",
    "frozen_meat", "frozen_other", "frozen_desserts_icecream",
    "oils_olive", "oils_other", "oils_vinegar",
    "sauces_ketchup_mayo", "sauces_pasta_cooking", "sauces_soy_worcester",
    "sauces_other", "sauces_marinades",
    "spices_herbs_spice_mix", "spices_broth_stock",
    "drinks_wine", "drinks_beer_cider", "drinks_soft_soda",
    "sweets_chocolate_bars", "sweets_nuts_driedfruit",
    "bakery_other", "bakery_bread_loaves",
    "dry_canned_fruit",
]

# Barbora tooted on DB-s chain='barbora', aga stores tabelis chain='Maxima'
CHAIN_ALIASES: Dict[str, str] = {
    "barbora": "maxima",
}

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


# ---------------- recipe ingredient resolution ----------------

async def _get_cached_ingredient(conn, ingredient_en: str):
    row = await conn.fetchrow(
        "SELECT search_terms, sub_codes FROM recipe_ingredient_cache WHERE ingredient_en = $1",
        ingredient_en.lower().strip()
    )
    if row:
        return {"search_terms": list(row["search_terms"]), "sub_codes": list(row["sub_codes"])}
    return None


async def _ask_claude_for_ingredient(ingredient_en: str) -> dict:
    if not ANTHROPIC_API_KEY:
        return {"search_terms": [ingredient_en.lower()], "sub_codes": []}

    prompt = f"""You help match recipe ingredients to Estonian grocery store product names.

Ingredient: "{ingredient_en}"

CRITICAL: search_terms MUST be Estonian words used in store databases. sub_codes MUST come from the list.

Valid sub_codes: {json.dumps(VALID_SUB_CODES)}

Return ONLY valid JSON:
{{"search_terms": ["estonian_word"], "sub_codes": ["sub_code"]}}"""

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            }
        )
        data = resp.json()
        text = data["content"][0]["text"].strip()
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)


async def _find_cheapest_per_chain(conn, ingredient_en: str) -> Dict[str, Dict]:
    """Leiab iga keti odavaima toote retsepti koostisosa jaoks."""
    name_lower = ingredient_en.lower().strip()
    if name_lower in SKIP_INGREDIENTS:
        return {}

    cached = await _get_cached_ingredient(conn, name_lower)
    if not cached:
        try:
            cached = await _ask_claude_for_ingredient(ingredient_en)
            await conn.execute(
                """INSERT INTO recipe_ingredient_cache (ingredient_en, search_terms, sub_codes)
                   VALUES ($1, $2, $3) ON CONFLICT (ingredient_en) DO NOTHING""",
                name_lower, cached["search_terms"], cached["sub_codes"]
            )
        except Exception:
            return {}

    search_terms = cached.get("search_terms", [])
    sub_codes = cached.get("sub_codes", [])
    if not search_terms:
        return {}

    raw_by_chain: Dict[str, Dict] = {}

    for term in search_terms:
        if sub_codes:
            rows = await conn.fetch("""
                SELECT p.id, p.name, p.chain, p.image_url, p.brand, p.size_text,
                    MIN(COALESCE(NULLIF(pr.promo_price, 0), pr.price)) as min_price
                FROM products p
                JOIN prices pr ON pr.product_id = p.id
                WHERE p.name ILIKE $1
                  AND p.sub_code = ANY($2::text[])
                  AND pr.price > 0
                  AND pr.collected_at > NOW() - INTERVAL '14 days'
                  AND p.name NOT ILIKE '%kaitstud%'
                  AND p.name NOT ILIKE '%strooganov%'
                  AND p.name NOT ILIKE '%valmistoit%'
                GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
                ORDER BY p.chain, min_price ASC
            """, f"%{term}%", sub_codes)
        else:
            rows = await conn.fetch("""
                SELECT p.id, p.name, p.chain, p.image_url, p.brand, p.size_text,
                    MIN(COALESCE(NULLIF(pr.promo_price, 0), pr.price)) as min_price
                FROM products p
                JOIN prices pr ON pr.product_id = p.id
                WHERE p.name ILIKE $1
                  AND p.sub_code NOT IN (
                    'hh_other','hh_cleaners','hh_laundry','hh_dishwashing',
                    'pcare_oral_care','pcare_other','pcare_feminine_hygiene',
                    'baby_diapers','pet_cat_wet','pet_dog_wet','pet_cat_dry','pet_dog_dry'
                  )
                  AND pr.price > 0
                  AND pr.collected_at > NOW() - INTERVAL '14 days'
                  AND p.name NOT ILIKE '%kaitstud%'
                  AND p.name NOT ILIKE '%strooganov%'
                  AND p.name NOT ILIKE '%valmistoit%'
                GROUP BY p.id, p.name, p.chain, p.image_url, p.brand, p.size_text
                ORDER BY p.chain, min_price ASC
            """, f"%{term}%")

        for r in rows:
            chain = (r["chain"] or "").lower()
            price = float(r["min_price"])
            if chain not in raw_by_chain or price < raw_by_chain[chain]["price"]:
                raw_by_chain[chain] = {
                    "product_id": r["id"],
                    "name": r["name"],
                    "chain": chain,
                    "image_url": r["image_url"] or "",
                    "price": price,
                }

    # Alias barbora -> maxima
    results_by_chain: Dict[str, Dict] = {}
    for chain_key, product in raw_by_chain.items():
        canonical = CHAIN_ALIASES.get(chain_key, chain_key)
        if canonical not in results_by_chain or product["price"] < results_by_chain[canonical]["price"]:
            results_by_chain[canonical] = product

    return results_by_chain


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


# ---------------- product group expansion ----------------

async def _expand_groups(
    conn: asyncpg.Connection,
    basket_pids: List[int],
) -> Dict[int, List[int]]:
    if not basket_pids:
        return {}
    try:
        rows = await conn.fetch(
            """
            SELECT pgm_basket.product_id AS basket_pid, pgm_all.product_id AS member_pid
            FROM product_group_members pgm_basket
            JOIN product_group_members pgm_all ON pgm_all.group_id = pgm_basket.group_id
            WHERE pgm_basket.product_id = ANY($1::int[])
            """,
            basket_pids,
        )
    except (pgerr.UndefinedTableError, pgerr.UndefinedColumnError):
        return {}

    result: Dict[int, List[int]] = {}
    for r in rows:
        result.setdefault(int(r["basket_pid"]), []).append(int(r["member_pid"]))
    return result


# ---------------- stores ----------------

async def _candidate_stores(
    conn, lat, lon, radius_km, limit, offset
) -> List[asyncpg.Record]:
    physical_filter = "AND COALESCE(s.is_online, false) = false"

    if lat is None or lon is None:
        return await conn.fetch(
            f"SELECT id, name, chain, lat, lon, NULL::double precision AS distance_km "
            f"FROM stores s WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL {physical_filter} "
            f"ORDER BY id OFFSET $1 LIMIT $2",
            int(offset), int(limit),
        )

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
          WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL {physical_filter}
        )
        SELECT * FROM with_dist
        WHERE distance_km <= (SELECT radius_km FROM params)
        ORDER BY distance_km, chain, name
        OFFSET $4 LIMIT $5
        """,
        float(lat), float(lon), float(radius_km), int(offset), int(limit),
    )


# ---------------- prices ----------------

async def _latest_prices(conn, product_ids, store_ids):
    if not product_ids or not store_ids:
        return []

    sql = """
    WITH effective_source AS (
      SELECT s.id AS physical_store_id,
             COALESCE(sps.source_store_id, s.id) AS source_store_id
      FROM stores s
      LEFT JOIN (
        SELECT DISTINCT ON (store_id) store_id, source_store_id
        FROM store_price_source
        ORDER BY store_id, source_store_id
      ) sps ON sps.store_id = s.id
      WHERE s.id = ANY($2::int[])
    ),
    latest AS (
      SELECT DISTINCT ON (p.product_id, p.store_id)
             p.product_id, p.store_id,
             COALESCE(NULLIF(p.promo_price, 0), p.price) AS price,
             p.collected_at
      FROM prices p
      WHERE p.product_id = ANY($1::int[])
        AND p.store_id IN (SELECT source_store_id FROM effective_source)
      ORDER BY p.product_id, p.store_id, p.collected_at DESC
    )
    SELECT l.product_id, es.physical_store_id AS store_id, l.price, l.collected_at
    FROM latest l
    JOIN effective_source es ON es.source_store_id = l.store_id;
    """
    try:
        return await conn.fetch(sql, product_ids, store_ids)
    except Exception:
        return await conn.fetch(
            """SELECT DISTINCT ON (p.product_id, p.store_id)
               p.product_id, p.store_id,
               COALESCE(NULLIF(p.promo_price, 0), p.price) AS price,
               p.collected_at
               FROM prices p
               WHERE p.product_id = ANY($1::int[]) AND p.store_id = ANY($2::int[])
               ORDER BY p.product_id, p.store_id, p.collected_at DESC""",
            product_ids, store_ids
        )


def _as_int_or_none(v):
    if v is None: return None
    if isinstance(v, int): return v
    if isinstance(v, float): return int(v)
    try: return int(str(v))
    except: return None


# ---------------- main service ----------------

async def compare_basket_service(db: Any, body: Dict[str, Any]) -> Dict[str, Any]:
    conn, should_release = await _acquire(db)
    try:
        lat = body.get("lat")
        lon = body.get("lon")
        radius_km = float(body.get("radius_km") or 5.0)
        limit_stores = int(body.get("limit_stores") or 50)
        offset_stores = int(body.get("offset_stores") or 0)
        include_lines = bool(body.get("include_lines") or False)
        require_all = bool(body.get("require_all_items") or False)

        raw_items = body.get("items") or []
        if not raw_items and "grocery_list" in body:
            raw_items = (body.get("grocery_list") or {}).get("items") or []

        # Normaliseeri — eralda retsepti koostisosad tavalistest toodetest
        recipe_items: List[Dict] = []
        normal_items: List[Dict] = []

        for it in raw_items:
            if not isinstance(it, dict):
                continue
            name = str(it.get("product", "") or "").strip()
            if not name:
                continue
            qty = float(it.get("quantity") or 1)
            if qty <= 0:
                continue

            ingredient_en = str(it.get("ingredient_name_en", "") or "").strip()
            if ingredient_en:
                recipe_items.append({
                    "product": name,
                    "ingredient_name_en": ingredient_en,
                    "quantity": qty,
                })
            else:
                normal_items.append({
                    "product": name,
                    "quantity": qty,
                    "product_id": _as_int_or_none(it.get("product_id")),
                })

        # --- Retsepti koostisosad ---
        import asyncio
        recipe_by_chain: Dict[str, Dict[str, Dict]] = {}

        async def _resolve_recipe_item(item):
            ing_en = item["ingredient_name_en"]
            ing_et = item["product"]
            async with db.acquire() as recipe_conn:
                per_chain = await _find_cheapest_per_chain(recipe_conn, ing_en)
            return ing_et, per_chain

        if recipe_items:
            tasks = [_resolve_recipe_item(it) for it in recipe_items]
            recipe_results = await asyncio.gather(*tasks)
            for ing_et, per_chain in recipe_results:
                for chain_key, product in per_chain.items():
                    recipe_by_chain.setdefault(chain_key, {})[ing_et] = product

        # --- Tavalised tooted ---
        qty_by_pid: Dict[int, float] = {}
        qty_by_name: Dict[str, float] = {}

        for it in normal_items:
            pid = _as_int_or_none(it.get("product_id"))
            qty = max(float(it.get("quantity") or 1), 0.1)
            if pid is not None:
                qty_by_pid[pid] = qty_by_pid.get(pid, 0.0) + qty
            else:
                nm = _norm(str(it.get("product") or ""))
                if nm:
                    qty_by_name[nm] = qty_by_name.get(nm, 0.0) + qty

        resolved_by_name = await _resolve_products_by_name(conn, list(qty_by_name.keys()))
        missing_products = [{"input": k} for k in qty_by_name if k not in resolved_by_name]
        for nm, rec in resolved_by_name.items():
            pid = int(_rv(rec, "id"))
            qty_by_pid[pid] = qty_by_pid.get(pid, 0.0) + qty_by_name.get(nm, 0.0)

        metadata: Dict[int, asyncpg.Record] = {}
        group_members: Dict[int, List[int]] = {}
        all_pids_for_prices: List[int] = []

        if qty_by_pid:
            basket_pids = sorted(qty_by_pid.keys())
            metadata = await _fetch_products_by_id(conn, basket_pids)
            for nm, rec in resolved_by_name.items():
                pid = int(_rv(rec, "id"))
                if pid not in metadata:
                    metadata[pid] = rec
            group_members = await _expand_groups(conn, basket_pids)
            all_pids_for_prices = sorted({
                mid for pid in basket_pids
                for mid in group_members.get(pid, [pid])
            })
            extra_pids = [p for p in all_pids_for_prices if p not in metadata]
            if extra_pids:
                metadata.update(await _fetch_products_by_id(conn, extra_pids))

        if not qty_by_pid and not recipe_items:
            return {"results": [], "totals": {}, "stores": [], "radius_km": radius_km, "missing_products": missing_products}

        # Candidate stores
        stores = await _candidate_stores(conn, lat, lon, radius_km, limit_stores, offset_stores)
        if not stores:
            return {"results": [], "totals": {}, "stores": [], "radius_km": radius_km, "missing_products": missing_products}
        store_ids = [int(_rv(s, "id")) for s in stores]

        # Hinnad
        by_store: Dict[int, Dict[int, float]] = {}
        if all_pids_for_prices:
            price_rows = await _latest_prices(conn, all_pids_for_prices, store_ids)
            for r in price_rows:
                sid = int(_rv(r, "store_id"))
                pid = int(_rv(r, "product_id"))
                by_store.setdefault(sid, {})[pid] = float(_rv(r, "price"))

        required_normal = len(qty_by_pid)
        required_recipe = len(recipe_items)
        required_total = required_normal + required_recipe

        results: List[Dict] = []
        best_total: Optional[float] = None
        best_store_id: Optional[int] = None

        for s in stores:
            sid = int(_rv(s, "id"))
            chain = (_rv(s, "chain") or "").lower()
            s_prices = by_store.get(sid, {})
            lines = []
            total = 0.0
            lines_found = 0
            not_found = []

            # Tavalised tooted
            for pid, qty in qty_by_pid.items():
                members = group_members.get(pid, [pid])
                best_pid = None
                best_price = None
                for mid in members:
                    p = s_prices.get(mid)
                    if p is not None and (best_price is None or p < best_price):
                        best_price = p
                        best_pid = mid
                if best_price is None:
                    meta = metadata.get(pid)
                    not_found.append(_rv(meta, "name") if meta else f"#{pid}")
                    continue
                lines_found += 1
                total += best_price * qty
                if include_lines:
                    meta = metadata.get(best_pid) if best_pid else metadata.get(pid)
                    is_per_kg = (_rv(meta, "size_text") or "").lower() == "kg" if meta else False
                    lines.append({
                        "product_id": best_pid,
                        "product_name": _rv(meta, "name") if meta else f"#{best_pid}",
                        "qty": qty,
                        "unit_price": _round2(best_price),
                        "line_total": _round2(best_price * qty),
                        "is_per_kg": is_per_kg,
                    })

            # Retsepti koostisosad
            chain_recipe = recipe_by_chain.get(chain, {})
            for item in recipe_items:
                ing_et = item["product"]
                product = chain_recipe.get(ing_et)
                if product is None:
                    not_found.append(ing_et)
                    continue
                lines_found += 1
                total += product["price"] * item["quantity"]
                if include_lines:
                    lines.append({
                        "product_id": product["product_id"],
                        "product_name": product["name"],
                        "qty": item["quantity"],
                        "unit_price": _round2(product["price"]),
                        "line_total": _round2(product["price"] * item["quantity"]),
                        "ingredient": ing_et,
                        "is_per_kg": False,
                    })

            normal_found = sum(1 for pid in qty_by_pid if any(
                by_store.get(sid, {}).get(mid) is not None
                for mid in group_members.get(pid, [pid])
            ))

            if lines_found == 0:
                total_price = None
            elif require_all and qty_by_pid and normal_found < required_normal:
                total_price = None
            else:
                total_price = _round2(total)

            result = {
                "store_id": sid,
                "chain": _rv(s, "chain"),
                "store_name": _rv(s, "name"),
                "distance_km": _round2(float(_rv(s, "distance_km"))) if _rv(s, "distance_km") is not None else None,
                "lines_found": lines_found,
                "required_lines": required_total,
                "total_price": total_price,
                "not_found": not_found,
            }
            if include_lines:
                result["lines"] = lines
            results.append(result)

            if total_price is not None and (best_total is None or total_price < best_total):
                best_total = total_price
                best_store_id = sid

        def sort_key(x):
            complete = 1 if (x.get("total_price") is not None and x.get("lines_found") == x.get("required_lines")) else 0
            price = x.get("total_price") if x.get("total_price") is not None else float("inf")
            dist = x.get("distance_km") if x.get("distance_km") is not None else float("inf")
            return (-complete, -int(x.get("lines_found", 0)), price, dist)

        results = [r for r in results if r.get("lines_found", 0) > 0]
        results.sort(key=sort_key)

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

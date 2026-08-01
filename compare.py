# compare.py
import json
import logging
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from pydantic import BaseModel, confloat, conint
from typing import List, Tuple, Dict, Any, Optional
from utils.throttle import throttle
from services.compare_service import compare_basket_service
from api.analytics_identity import resolve_analytics_identity

logger = logging.getLogger("uvicorn.error")

# v4.6.9/v2 UUS — Etapp 5B shadow mode. Vt substitution_shadow.py
# docstring turvapõhimõtete kohta. Sama kitsas import-kaitse mis
# services/compare_service.py's (ChatGPT leid #8): lubatud on AINULT
# see, et substitution_shadow.py ISE pole veel repos — iga muu
# impordiviga (nt substitution_service.py sisemine katkine dependency)
# tõuseb edasi ja peatab käivituse nähtavalt, mitte ei vaiki.
try:
    import substitution_shadow
except ModuleNotFoundError as _exc:
    if _exc.name != "substitution_shadow":
        raise
    substitution_shadow = None

router = APIRouter(prefix="")

MAX_ITEMS = 50
MIN_RADIUS = 0.1
MAX_RADIUS = 50.0
MAX_STORES = 50


class GroceryItem(BaseModel):
    product: str
    quantity: confloat(ge=0.1) = 1.0
    product_id: int | None = None
    ingredient_name_en: str | None = None


class GroceryList(BaseModel):
    items: List[GroceryItem]


class CompareRequest(BaseModel):
    grocery_list: GroceryList
    lat: Optional[float] = None  # None = ei filtreeri kauguse järgi
    lon: Optional[float] = None
    radius_km: confloat(ge=MIN_RADIUS, le=MAX_RADIUS) = 2.0
    limit_stores: conint(ge=1, le=MAX_STORES) = 50
    offset_stores: conint(ge=0) = 0
    include_lines: bool = True
    require_all_items: bool = True


def _clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return lo if v < lo else hi if v > hi else v


def _normalize_items(gl: GroceryList) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in gl.items:
        name = (it.product or "").strip()
        if not name:
            continue
        item: Dict[str, Any] = {
            "product": name,
            "quantity": float(it.quantity),
            "product_id": it.product_id,
        }
        if it.ingredient_name_en:
            item["ingredient_name_en"] = it.ingredient_name_en
        out.append(item)
    return out


def _build_chain_totals(results: List[Dict[str, Any]]) -> Tuple[Dict[str, float], Dict[str, int]]:
    """Reduces the per-store comparison results to one entry per chain:
    the cheapest complete-basket total found in that chain, and which
    store achieved it. Stores with no total_price (incomplete basket —
    e.g. require_all_items filtered them out, or nothing was found)
    are skipped, since a missing total can't be compared against a
    winner in euros.
    """
    chain_totals: Dict[str, float] = {}
    chain_store_ids: Dict[str, int] = {}
    for r in results:
        total = r.get("total_price")
        if total is None:
            continue
        # A store that didn't find every item still gets a total_price
        # (the sum of what it did find), which would otherwise look
        # artificially cheap and get mistaken for a real win. Only
        # complete baskets are meaningful for a chain-vs-chain price
        # comparison.
        if r.get("lines_found") != r.get("required_lines"):
            continue
        chain = (r.get("chain") or "").lower().strip()
        if not chain:
            continue
        if chain not in chain_totals or total < chain_totals[chain]:
            chain_totals[chain] = float(total)
            store_id = r.get("store_id")
            if store_id is not None:
                chain_store_ids[chain] = int(store_id)
            else:
                chain_store_ids.pop(chain, None)
    return chain_totals, chain_store_ids


async def _log_basket_compare(
    request: Request,
    payload_out: Dict[str, Any],
    basket_size: int,
    radius_km: float,
    user_id: Optional[str],
    device_key: Optional[str],
) -> None:
    """Logs a basket_compare analytics event carrying the per-chain
    totals for this comparison, so the partner dashboard can later
    compute "lost to the winner by less than X€" insights (V2.5), and
    now also identity fields (user_id/device_key) so future analysis
    can look at compares-per-device, guest vs account behaviour, and a
    basket_compare -> basket_win funnel. Chain totals stay the
    aggregate signal the dashboard already reads; identity is
    additional context, not a replacement.

    Identity is resolved via the same server-side resolver used by
    /analytics/event (api/analytics_identity.py) — this endpoint must
    never re-implement its own JWT or HMAC handling, so both code paths
    always agree on how a person is identified.

    Never raises: an analytics failure must not break the actual
    /compare response.
    """
    try:
        results = payload_out.get("results", [])
        chain_totals, chain_store_ids = _build_chain_totals(results)
        if len(chain_totals) < 2:
            return

        # Derived from chain_totals itself (not payload_out["totals"])
        # so cheapest_chain, cheapest_total and chain_totals can never
        # disagree with each other in the stored event.
        cheapest_chain = min(chain_totals, key=chain_totals.get)
        cheapest_total = chain_totals[cheapest_chain]
        stores_compared = sum(1 for r in results if r.get("total_price") is not None)
        required_lines = next(
            (r.get("required_lines") for r in results if r.get("required_lines") is not None),
            basket_size,
        )

        pool = getattr(request.app.state, "db", None)
        if pool is None:
            return

        event_payload = {
            "radius_km": radius_km,
            "required_lines": required_lines,
            "stores_compared": stores_compared,
            "cheapest_chain": cheapest_chain,
            "cheapest_total": cheapest_total,
            "chain_totals": chain_totals,
            "chain_store_ids": chain_store_ids,
            "basket_size": basket_size,
        }

        await pool.execute(
            """
            INSERT INTO analytics_events (event_type, chain, payload, user_id, device_key)
            VALUES ($1, $2, $3::jsonb, $4, $5)
            """,
            "basket_compare",
            cheapest_chain,
            json.dumps(event_payload),
            user_id,
            device_key,
        )
    except Exception as exc:
        # Analytics is best-effort — never let a logging problem affect
        # the person's actual price comparison. Still log the failure
        # (exception type/message only — never the payload, token, or
        # device id) so a broken insert doesn't go unnoticed forever.
        logger.warning("basket_compare analytics logging failed: %s", exc)


async def compute_compare(
    pool,
    items: List[Tuple[str, int]],
    user_lat: float,
    user_lon: float,
    radius_km: float,
):
    # compare_basket_service expects a list of dicts (product/quantity/
    # product_id/ingredient_name_en) — it silently skips anything that
    # isn't a dict. This legacy helper used to pass raw (name, qty)
    # tuples straight through, which meant every row got dropped and
    # the comparison always came back empty. Build the dicts here so
    # this still works if anything still calls compute_compare().
    payload_items = [
        {
            "product": str(name).strip(),
            "quantity": float(quantity),
            "product_id": None,
        }
        for name, quantity in items
        if name and str(name).strip()
    ]

    payload = {
        "items": payload_items,
        "lat": float(user_lat),
        "lon": float(user_lon),
        "radius_km": float(_clamp(float(radius_km), MIN_RADIUS, MAX_RADIUS)),
        "limit_stores": 50,
        "offset_stores": 0,
        "include_lines": True,
        "require_all_items": True,
    }
    result = await compare_basket_service(pool, payload)
    # v3 fix (ChatGPT leid #3) — compare_basket_service() võib lisada
    # sisemised "_shadow_*" võtmed, mida /compare router teadlikult
    # eemaldab kliendivastuse koostamisel. See legacy helper aga
    # tagastas varem tulemuse OTSE, ilma sama filtreerimiseta, mistõttu
    # iga TEINE compute_compare() kutsuja oleks need sisemised võtmed
    # nähtavaks saanud. Eemalda need siin samamoodi.
    result.pop("_shadow_missing_items", None)
    result.pop("_shadow_compare_request_id", None)
    return result


@router.post("/compare")
@throttle(limit=30, window=60)
async def compare_basket(
    body: CompareRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    x_device_id: Optional[str] = Header(default=None, alias="X-Device-Id"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
):
    try:
        if not body.grocery_list.items:
            raise HTTPException(status_code=400, detail="Basket is empty")
        if len(body.grocery_list.items) > MAX_ITEMS:
            raise HTTPException(status_code=400, detail=f"Basket too large (>{MAX_ITEMS} items)")

        items_payload = _normalize_items(body.grocery_list)
        if not items_payload:
            raise HTTPException(status_code=400, detail="All product names are empty")

        pool = getattr(request.app.state, "db", None)
        if pool is None:
            raise HTTPException(status_code=500, detail="DB not ready")

        radius_km = float(_clamp(float(body.radius_km), MIN_RADIUS, MAX_RADIUS))
        limit_stores = int(_clamp_int(int(body.limit_stores), 1, MAX_STORES))
        offset_stores = max(0, int(body.offset_stores))

        # v3 fix (ChatGPT leid #1) — sampling otsustatakse SIIN, ÜKS
        # KORD, ENNE compare_basket_service() kutsumist. Varem tehti
        # see otsus alles background-task'i sees, mis tähendas, et
        # compare_service.py tegi lisapäringu (group_info) IGAL
        # request'il, kui shadow oli globaalselt sisse lülitatud —
        # sõltumata sample rate'ist. Nüüd teeb seda päringut ainult
        # see osa (nt 5%) request'idest, mis on juba sample'itud.
        shadow_sampled = bool(
            substitution_shadow is not None
            and substitution_shadow.shadow_enabled()
            and substitution_shadow.should_sample_this_request()
        )

        payload_in = {
            "items": items_payload,
            "lat": body.lat,  # None lubatud
            "lon": body.lon,  # None lubatud
            "radius_km": radius_km,
            "limit_stores": limit_stores,
            "offset_stores": offset_stores,
            "include_lines": bool(body.include_lines),
            "require_all_items": bool(body.require_all_items),
            "_shadow_sampled": shadow_sampled,
        }

        payload_out = await compare_basket_service(pool, payload_in)

        # v4.6.9/v3 UUS — Etapp 5B shadow mode. compare_service.py lisab
        # (kui shadow-kandidaate leiti JA see request oli sample'itud)
        # sisemised võtmed "_shadow_missing_items"/
        # "_shadow_compare_request_id" — need EI JÕUA kliendini, kuna
        # allpool koostatav vastus valib ainult teadaolevad väljad.
        # Kasutame FastAPI BackgroundTasks't, et shadow käivituks alles
        # PÄRAST vastuse kliendile saatmist — see EI mõjuta /compare
        # latentsust üldse. already_sampled=True, kuna otsus on juba
        # tehtud ülal — run_shadow_batch_safely EI TOHI enam teist
        # korda randomiseerida (topelt-sampling'u vältimiseks).
        shadow_missing_items = payload_out.get("_shadow_missing_items")
        if shadow_sampled and shadow_missing_items:
            background_tasks.add_task(
                substitution_shadow.run_shadow_batch_safely,
                pool,
                payload_out.get("_shadow_compare_request_id"),
                shadow_missing_items,
                already_sampled=True,
            )

        user_id, device_key = await resolve_analytics_identity(request, authorization, x_device_id)

        await _log_basket_compare(
            request,
            payload_out,
            basket_size=len(items_payload),
            radius_km=payload_out.get("radius_km", radius_km),
            user_id=user_id,
            device_key=device_key,
        )

        return {
            "results": payload_out.get("results", []),
            "totals": payload_out.get("totals", {}),
            "stores": payload_out.get("stores", []),
            "radius_km": payload_out.get("radius_km", radius_km),
            "missing_products": payload_out.get("missing_products", []),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

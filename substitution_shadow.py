"""
Seivy — Etapp 5B: asendustoodete SHADOW MODE.

v3 (ChatGPT kolmas ülevaatus — 4 parandust enne 10% liiklust, 3
väiksemat märkust):

1. SAMPLING VARAJASEMAKS: v2's tehti sample-otsus alles background-
   task'i sees, aga _fetch_group_info_for_pids() lisapäring tehti
   compare_service.py's IGAL request'il, kui SHADOW_ENABLED=true —
   sõltumata sample rate'ist. See tähendas 100% lisapäringut DB-le,
   kuigi ainult ~10% oleks tegelikult shadow'd käivitanud. Nüüd
   otsustab compare.py router sample'imise ÜKS KORD, ENNE service'i
   kutsumist, ja annab selle edasi payload_in["_shadow_sampled"]
   kaudu. run_shadow_batch_safely() saab already_sampled=True ega
   randomiseeri enam teist korda (topelt-sampling'u vältimiseks).
2. KONSERVATIIVSED VAIKEVÄÄRTUSED: rollout algab CONCURRENCY=1 (DB-
   ühendus on hõivatud kogu Claude API kutse ajaks — see on POOL
   STARVATION risk, mitte enam connection-concurrency bug, aga
   Railway väikese pooli juures on see siiski oluline). MAX_ITEMS=2,
   SAMPLE_RATE=0.05. Suurendada alles pärast esimese 50-100 shadow
   päringu monitooringut (pool wait time, timeout'ide arv, /compare
   p95).
3. Tabeliveerg "store_id" nimetatud ümber "first_seen_store_id"'ks —
   kuna tabel pole veel tootmises, väldib see tulevast eksitavat
   analüütikat (veerg on INFORMATIIVNE, mitte kinnitatud sihtpood).
4. Item-level vea korral (_evaluate_one sees) logitakse nüüd TEGELIK
   group_id/sub_code/chain/first_seen_store_id, mitte 0/"unknown" —
   viimane jääb AINULT batch-taseme (timeout) vigadele, millel pole
   üksiku toote konteksti.
5. json.dumps(..., default=str) — kaitseb tuleviku eest, kui trace'i
   lisatakse Decimal/datetime/UUID väärtusi, mis muidu paneksid
   õnnestunud otsuse ekslikult "shadow_error"'iks.

TEADLIKULT MITTE LAHENDATUD SELLES VOORUS (ChatGPT ise ütles, et ei
pea esimest etappi blokeerima): DB-ühendus on _evaluate_one() sees
hõivatud kogu Claude API väliskutse ajaks. Õige pikaajaline lahendus
on substitution_service.py refaktoreerimine kolmeastmeliseks (loe
kandidaadid -> vabasta ühendus -> kutsu Claude -> võta uus ühendus
ainult logimiseks) — see on suurem muudatus, jääb järgmiseks vooruks.
CONCURRENCY=1 teeb selle riski esimeses etapis väikeseks.

KESKKONNAMUUTUJAD:
    SUBSTITUTION_SHADOW_ENABLED=true|false   (vaikimisi false)
    SUBSTITUTION_SHADOW_SAMPLE_RATE=0.05      (vaikimisi 0.05, clamp 0.0-1.0)
    SUBSTITUTION_SHADOW_MAX_ITEMS=2           (vaikimisi 2, clamp 0-20)
    SUBSTITUTION_SHADOW_TIMEOUT_SECONDS=2.0   (clamp 0.1-10.0)
    SUBSTITUTION_SHADOW_CONCURRENCY=1         (vaikimisi 1, clamp 1-5)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from substitution_service import get_or_create_substitution, SUBSTITUTION_RULES_VERSION

logger = logging.getLogger("uvicorn.error")


SHADOW_ENABLED_SUB_CODES = {
    "dairy_milk",
    "dairy_yogurt_kefir",
    "cheese_regular",
    "cheese_delicatessen",
    "dairy_cheese_slices",
    "oils_olive",
}


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def shadow_enabled() -> bool:
    return _env_bool("SUBSTITUTION_SHADOW_ENABLED", False)


def _sample_rate() -> float:
    # v3: konservatiivne vaikeväärtus 0.10 -> 0.05.
    return max(0.0, min(1.0, _env_float("SUBSTITUTION_SHADOW_SAMPLE_RATE", 0.05)))


def _max_items() -> int:
    # v3: konservatiivne vaikeväärtus 3 -> 2.
    return max(0, min(20, _env_int("SUBSTITUTION_SHADOW_MAX_ITEMS", 2)))


def _timeout_seconds() -> float:
    return max(0.1, min(10.0, _env_float("SUBSTITUTION_SHADOW_TIMEOUT_SECONDS", 2.0)))


def _concurrency() -> int:
    # v3: konservatiivne vaikeväärtus 3 -> 1 (vt DB-ühenduse hõivatuse
    # märkust docstring'us — pool starvation risk).
    return max(1, min(5, _env_int("SUBSTITUTION_SHADOW_CONCURRENCY", 1)))


def should_sample_this_request() -> bool:
    """v3: kutsu see AINULT üks kord request'i kohta, VÕIMALIKULT
    VARAKULT (compare.py routeris, enne compare_basket_service()
    kutsumist) — mitte background-task'i sees, kus otsus tuleks liiga
    hilja (pärast juba tehtud lisapäringut group_info jaoks)."""
    return random.random() < _sample_rate()


def _safe_uuid(value: Any) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return str(uuid.uuid4())


def _dedupe_missing_items(
    items: List[Tuple[int, str, str, Optional[int]]],
) -> List[Tuple[int, str, str, Optional[int]]]:
    seen = set()
    result = []
    for group_id, sub_code, chain, store_id in items:
        key = (group_id, chain)
        if key in seen:
            continue
        seen.add(key)
        result.append((group_id, sub_code, chain, store_id))
    random.shuffle(result)
    return result


async def _log_shadow_event(conn, event: Dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO substitution_shadow_events (
            compare_request_id, rules_version, chain, first_seen_store_id,
            original_group_id, substitute_group_id, sub_code,
            decision_type, quantity_diff_percent, candidate_price,
            latency_ms, reasoning, rule_flags, trace
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb)
        """,
        event.get("compare_request_id"),
        event.get("rules_version"),
        event.get("chain"),
        event.get("first_seen_store_id"),
        event.get("original_group_id"),
        event.get("substitute_group_id"),
        event.get("sub_code"),
        event.get("decision_type"),
        event.get("quantity_diff_percent"),
        event.get("candidate_price"),
        event.get("latency_ms"),
        event.get("reasoning"),
        # v3: default=str kaitseb tuleviku eest, kui trace'i satub
        # Decimal/datetime/UUID/asyncpg.Record — muidu paneks json.dumps
        # ise TypeError, mis muudaks õnnestunud otsuse "shadow_error"'iks.
        json.dumps(event.get("rule_flags") or [], default=str),
        json.dumps(event.get("trace") or {}, default=str),
    )


async def _log_item_failure(
    pool,
    compare_request_id: str,
    group_id: int,
    sub_code: str,
    chain: str,
    first_seen_store_id: Optional[int],
    message: str,
) -> None:
    """v3 UUS — item-taseme vea korral logitakse TEGELIK kontekst
    (group_id/sub_code/chain/store), mitte 0/"unknown". Kasutab ERALDI
    ühendust, kuna algne ühendus (kust _evaluate_one viga sai) võib
    olla katkises seisus."""
    try:
        async with pool.acquire() as conn:
            await _log_shadow_event(conn, {
                "compare_request_id": compare_request_id,
                "rules_version": SUBSTITUTION_RULES_VERSION,
                "chain": chain,
                "first_seen_store_id": first_seen_store_id,
                "original_group_id": group_id,
                "substitute_group_id": None,
                "sub_code": sub_code,
                "decision_type": "shadow_error",
                "quantity_diff_percent": None,
                "candidate_price": None,
                "latency_ms": None,
                "reasoning": message,
                "rule_flags": [],
                "trace": {},
            })
    except Exception:
        logger.exception("substitution_shadow_item_failure_logging_failed")


async def _log_batch_failure(
    pool,
    compare_request_id: str,
    decision_type: str,
    message: str,
) -> None:
    """Batch-taseme viga (nt kogu partii timeout) — siin PUUDUB
    üksiku toote kontekst, seega group_id=0/sub_code="unknown" on siin
    õigustatud (erinevalt item-taseme veast, vt _log_item_failure)."""
    try:
        async with pool.acquire() as conn:
            await _log_shadow_event(conn, {
                "compare_request_id": compare_request_id,
                "rules_version": SUBSTITUTION_RULES_VERSION,
                "chain": "unknown",
                "first_seen_store_id": None,
                "original_group_id": 0,
                "substitute_group_id": None,
                "sub_code": "unknown",
                "decision_type": decision_type,
                "quantity_diff_percent": None,
                "candidate_price": None,
                "latency_ms": None,
                "reasoning": message,
                "rule_flags": [],
                "trace": {},
            })
    except Exception:
        logger.exception("substitution_shadow_batch_failure_logging_failed")


async def _evaluate_one(
    pool,
    sem: asyncio.Semaphore,
    compare_request_id: str,
    group_id: int,
    sub_code: str,
    chain: str,
    first_seen_store_id: Optional[int],
) -> None:
    async with sem:
        started = time.monotonic()
        try:
            # v3 MÄRKUS (teadlik risk, vt mooduli docstring): see
            # ühendus jääb hõivatuks KOGU get_or_create_substitution()
            # kutse ajaks, sh selle sisemine Claude API väliskutse.
            # CONCURRENCY vaikeväärtus 1 hoiab selle riski esimeses
            # etapis väiksena.
            async with pool.acquire() as conn:
                result = await get_or_create_substitution(
                    conn, group_id, chain, dry_run=True, use_cache=False,
                )
                latency_ms = int((time.monotonic() - started) * 1000)
                if not isinstance(result, dict):
                    result = {}
                trace = dict(result.get("trace", {}) or {})
                trace["decision_scope"] = "chain"
                trace["first_seen_store_id"] = first_seen_store_id

                event = {
                    "compare_request_id": compare_request_id,
                    "rules_version": SUBSTITUTION_RULES_VERSION,
                    "chain": chain,
                    "first_seen_store_id": first_seen_store_id,
                    "original_group_id": group_id,
                    "substitute_group_id": result.get("substitute_group_id"),
                    "sub_code": sub_code,
                    "decision_type": result.get("decision_type", "unknown"),
                    "quantity_diff_percent": result.get("quantity_diff_percent"),
                    "candidate_price": result.get("price"),
                    "latency_ms": latency_ms,
                    "reasoning": result.get("reasoning"),
                    "rule_flags": [],
                    "trace": trace,
                }
                await _log_shadow_event(conn, event)
        except Exception as e:
            logger.exception(
                "substitution_shadow_failed group_id=%s chain=%s", group_id, chain
            )
            await _log_item_failure(
                pool, compare_request_id, group_id, sub_code, chain,
                first_seen_store_id, str(e)[:500],
            )


async def run_shadow_batch(
    pool,
    compare_request_id: str,
    missing_items: List[Tuple[int, str, str, Optional[int]]],
) -> None:
    if not shadow_enabled() or not missing_items:
        return

    allowed_all = _dedupe_missing_items([
        item for item in missing_items if item[1] in SHADOW_ENABLED_SUB_CODES
    ])
    allowed = allowed_all[: _max_items()]
    if not allowed:
        return

    sem = asyncio.Semaphore(_concurrency())
    tasks = [
        _evaluate_one(pool, sem, compare_request_id, group_id, sub_code, chain, store_id)
        for group_id, sub_code, chain, store_id in allowed
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


async def run_shadow_batch_safely(
    pool,
    compare_request_id: Optional[str],
    missing_items: List[Tuple[int, str, str, Optional[int]]],
    already_sampled: bool = False,
) -> None:
    """v3: already_sampled=True tähendab, et kutsuja (compare.py router)
    ON JUBA otsustanud sample'imise ENNE compare_basket_service()
    kutsumist — see funktsioon EI TOHI siis enam teist korda
    randomiseerida (topelt-sampling'u vältimiseks). Kui already_sampled
    on False (nt otsest kasutust dry-run-tüüpi skriptides), tehakse
    sampling siin nagu varem."""
    if not shadow_enabled():
        return
    if not already_sampled and not should_sample_this_request():
        return
    if not missing_items:
        return

    request_id = _safe_uuid(compare_request_id or uuid.uuid4())
    try:
        await asyncio.wait_for(
            run_shadow_batch(pool, request_id, missing_items),
            timeout=_timeout_seconds(),
        )
    except asyncio.TimeoutError:
        logger.warning(
            "substitution_shadow_timeout request_id=%s items=%d",
            request_id, len(missing_items),
        )
        await _log_batch_failure(
            pool, request_id, "shadow_timeout",
            f"batch timeout after {_timeout_seconds()}s, {len(missing_items)} items",
        )
    except Exception as e:
        logger.exception("substitution_shadow_batch_failed request_id=%s", request_id)
        await _log_batch_failure(pool, request_id, "shadow_error", str(e)[:500])

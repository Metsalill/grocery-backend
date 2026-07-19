"""
Seivy — asendustoodete teenus (v4, juuli 2026).

v4 muudatused (ChatGPT teine arvustus):
- dry_run parameeter: kui True, ei kutsuta _save()-i KUNAGI (KIHT 1
  kaitsest). Kuivtesti skript PEAB lisaks käivitama seda ka
  read-only DB transaktsiooni sees (KIHT 2) — kaks sõltumatut kaitset.
- Iga väljakutse tagastab "trace" välja täieliku otsustusahelaga
  (sql_candidate_count, quantity_eligible_count, trait_eligible_count,
  claude_candidate_count jne) monitooringu/veaotsingu jaoks.
- spices_broth_stock EEMALDATUD QUANTITY_RULES-ist — oli omavoliline
  lisandus, mitte teadlikult läbi vaadatud kategooria.

See fail on hetkel ISOLEERITUD — compare_service.py ei impordi seda.
"""

import os
import json
import logging
from datetime import timedelta
from typing import Optional

import httpx

from quantity_service import (
    classify_quantity_match,
    QuantityTier,
    SUBSTITUTION_RULES_VERSION,
)

logger = logging.getLogger("substitution_service")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
API_TIMEOUT_SECONDS = 6.0

MAX_SEMANTIC_CANDIDATES = 8
CANDIDATE_POOL_LIMIT = 150

_TTL_BY_DECISION = {
    "auto_substitute": timedelta(days=7),
    "suggested_substitute": timedelta(days=2),
    "no_quantity_data": timedelta(days=1),
    "no_eligible_candidates": timedelta(days=1),
    "semantic_rejected": timedelta(days=1),
}

REQUIRED_TRAITS: dict[str, tuple[str, ...]] = {
    "lactose_free": ("laktoosivaba", "lactose free", "lactose-free"),
    "gluten_free": ("gluteenivaba", "gluten free", "gluten-free"),
    "alcohol_free": ("alkoholivaba", "alcohol free", "alcohol-free"),
}

import re

IDENTITY_TRAITS: dict[str, tuple[str, ...]] = {
    "plant_based": ("taimne", "vegan"),
}

# Maitsestatud vs maitsestamata — kahesuunaline identity-trait.
# Leitud reaalse vea põhjal (juuli 2026): Cappuccino/Latte piim asendati
# vääralt tavalise piimaga (auto_substitute). Bränd/kogus klappisid,
# aga toote TÜÜP (maitsestatud jook vs tavaline piim) oli erinev —
# see kontroll väldib seda mõlemas suunas.
FLAVOR_KEYWORDS = (
    "cappuccino", "latte", "šokolaadi", "shokolaadi", "vanilje",
    "karamelli", "maasika", "banaani", "kookos",
)


def _is_flavored(text) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in FLAVOR_KEYWORDS)


# Piima rasvaprotsendi kategooria — DETERMINISTLIK, mitte Claude'i
# otsustada. Leitud reaalse vea põhjal: sama originaaltoode (täispiim
# 3,6-4,2%) sai Coopis vale asenduse (2,5% piim, auto_substitute), aga
# Selveris õigesti tagasi lükatud — sama sisend, vastandlik tulemus.
# Regex parsib protsendi tekstist (nt "3,6-4,2%" või "2,5%") ja
# klassifitseerib kategooriasse.
_FAT_RANGE_RE = re.compile(r"(\d+[.,]?\d*)\s*-\s*(\d+[.,]?\d*)\s*%")
_FAT_SINGLE_RE = re.compile(r"(\d+[.,]?\d*)\s*%")


def _milk_fat_bucket(text) -> Optional[str]:
    if not text:
        return None
    normalized = text.replace(",", ".")
    m = _FAT_RANGE_RE.search(normalized)
    if m:
        try:
            pct = (float(m.group(1)) + float(m.group(2))) / 2
        except ValueError:
            return None
    else:
        m = _FAT_SINGLE_RE.search(normalized)
        if not m:
            return None
        try:
            pct = float(m.group(1))
        except ValueError:
            return None

    if pct >= 3.2:
        return "whole"        # täispiim
    if pct >= 2.0:
        return "standard"     # tavaline (2,5%)
    if pct >= 0.5:
        return "low_fat"      # kooritud/vähendatud (1,5%/1,8%)
    return "fat_free"         # rasvatu


def _detect_traits(text, trait_map):
    if not text:
        return set()
    text_lower = text.lower()
    found = set()
    for trait, keywords in trait_map.items():
        if any(kw in text_lower for kw in keywords):
            found.add(trait)
    return found


def _traits_compatible(original_name, candidate_name, sub_code=None):
    original_required = _detect_traits(original_name, REQUIRED_TRAITS)
    candidate_required = _detect_traits(candidate_name, REQUIRED_TRAITS)
    if not original_required.issubset(candidate_required):
        return False

    original_identity = _detect_traits(original_name, IDENTITY_TRAITS)
    candidate_identity = _detect_traits(candidate_name, IDENTITY_TRAITS)
    if original_identity != candidate_identity:
        return False

    # Maitsestatud vs maitsestamata (kahesuunaline) — kontrolli ENNE
    # rasvaprotsendi kontrolli, kuna maitsestatud jookide "rasvaprotsent"
    # ei vasta tavalise piima rasva-kategooriatele (nt Cappuccino "3,5%"
    # ei tähenda sama, mis täispiima "3,5%").
    original_is_flavored = _is_flavored(original_name)
    candidate_is_flavored = _is_flavored(candidate_name)
    if original_is_flavored != candidate_is_flavored:
        return False

    # Piima rasvaprotsendi kategooria (ainult dairy_milk JA ainult
    # maitsestamata piim — maitsestatud jookide puhul jääb see Claude'i
    # semantilise hinnangu kanda, vt ülal olev kommentaar)
    if sub_code == "dairy_milk" and not original_is_flavored:
        o_bucket = _milk_fat_bucket(original_name)
        c_bucket = _milk_fat_bucket(candidate_name)
        if o_bucket is not None:
            if c_bucket is None:
                return False  # kandidaadi rasvaprotsent teadmata -> turvaliselt keeldu
            if o_bucket != c_bucket:
                return False

    return True


BABY_FOOD_SUB_CODES = {
    "baby_porridge_cereal", "baby_diapers", "baby_care", "baby_other", "baby_wipes",
}


class SubstitutionTimeout(Exception):
    pass


async def get_or_create_substitution(conn, group_id, chain, dry_run=False):
    """
    Tagastab dict tulemuse + "trace" alamvõtme täieliku otsustusahelaga,
    või None tehnilise vea korral (timeout, vigane API vastus).

    dry_run=True: _save() EI kutsuta kunagi (KIHT 1 kaitsest). Kuivtesti
    skript peab lisaks avama read-only DB transaktsiooni (KIHT 2).
    """
    chain = chain.lower()
    trace = {
        "original_group_id": group_id,
        "chain": chain,
        "sub_code": None,
        "original_quantity": None,
        "sql_candidate_count": 0,
        "quantity_eligible_count": 0,
        "trait_eligible_count": 0,
        "claude_candidate_count": 0,
        "dry_run": dry_run,
        "database_write_attempted": False,
        "cache_hit": False,
    }

    async def _finish(result, save=True):
        if save:
            trace["database_write_attempted"] = True
            if not dry_run:
                await _save(conn, group_id, chain, result)
        result["trace"] = trace
        return result

    existing = await conn.fetchrow(
        """
        SELECT decision_type, substitute_group_id, included_in_total,
               quantity_diff_percent, reasoning
        FROM product_substitutions
        WHERE original_group_id = $1 AND chain = $2
          AND substitution_rules_version = $3
          AND expires_at > NOW()
        """,
        group_id, chain, SUBSTITUTION_RULES_VERSION,
    )

    if existing:
        trace["cache_hit"] = True
        substitute_id = existing["substitute_group_id"]
        price = None
        if substitute_id:
            price = await _get_group_price_in_chain(conn, substitute_id, chain)
        result = {
            "decision_type": existing["decision_type"],
            "substitute_group_id": substitute_id,
            "price": price,
            "included_in_total": existing["included_in_total"],
            "quantity_diff_percent": (
                float(existing["quantity_diff_percent"])
                if existing["quantity_diff_percent"] is not None else None
            ),
            "reasoning": existing["reasoning"],
        }
        result["trace"] = trace
        return result

    original = await conn.fetchrow(
        "SELECT id, canonical_name, brand, sub_code FROM product_groups WHERE id = $1",
        group_id,
    )
    if not original:
        return None

    trace["sub_code"] = original["sub_code"]

    original_sample = await conn.fetchrow(
        """
        SELECT p.name AS sample_product_name, p.net_qty, p.net_unit
        FROM product_group_members m
        JOIN products p ON p.id = m.product_id
        WHERE m.group_id = $1
          AND p.net_qty IS NOT NULL AND p.net_qty > 0
          AND p.net_unit IS NOT NULL AND BTRIM(p.net_unit) <> ''
        LIMIT 1
        """,
        group_id,
    )
    if not original_sample:
        original_sample = await conn.fetchrow(
            """
            SELECT p.name AS sample_product_name, p.net_qty, p.net_unit
            FROM product_group_members m
            JOIN products p ON p.id = m.product_id
            WHERE m.group_id = $1
            LIMIT 1
            """,
            group_id,
        )

    original_sample_name = original_sample["sample_product_name"] if original_sample else ""
    original_qty = original_sample["net_qty"] if original_sample else None
    original_unit = original_sample["net_unit"] if original_sample else None

    trace["original_quantity"] = (
        {"value": float(original_qty), "unit": original_unit, "status": "known"}
        if original_qty and original_unit
        else {"value": None, "unit": None, "status": "unknown"}
    )

    if not original_qty or not original_unit:
        result = {
            "decision_type": "no_quantity_data",
            "substitute_group_id": None,
            "price": None,
            "included_in_total": False,
            "quantity_diff_percent": None,
            "reasoning": (
                "originaali net_qty/net_unit puudub — koguse-põhine "
                "automaatne asendus pole võimalik (vajab backfill projekti)"
            ),
        }
        return await _finish(result)

    candidates = await conn.fetch(
        """
        SELECT DISTINCT ON (pg.id)
            pg.id, pg.canonical_name, pg.brand,
            p.name AS sample_product_name, p.net_qty, p.net_unit
        FROM product_groups pg
        JOIN product_group_members m ON m.group_id = pg.id
        JOIN products p ON p.id = m.product_id
        JOIN prices pr ON pr.product_id = p.id
        JOIN stores s ON s.id = pr.store_id
        WHERE pg.sub_code = $1
          AND LOWER(s.chain) = $2
          AND pg.id != $3
        ORDER BY
            pg.id,
            CASE WHEN LOWER(BTRIM(p.net_unit)) = LOWER(BTRIM($5)) THEN 0 ELSE 1 END,
            p.id
        LIMIT $4
        """,
        original["sub_code"], chain, group_id, CANDIDATE_POOL_LIMIT, original_unit,
    )
    trace["sql_candidate_count"] = len(candidates)

    if not candidates:
        result = {
            "decision_type": "no_eligible_candidates",
            "substitute_group_id": None,
            "price": None,
            "included_in_total": False,
            "quantity_diff_percent": None,
            "reasoning": "candidates puudusid selles ketis",
        }
        return await _finish(result)

    is_baby_food = original["sub_code"] in BABY_FOOD_SUB_CODES

    quantity_eligible = []
    for c in candidates:
        qmatch = classify_quantity_match(
            original_qty, original_unit, c["net_qty"], c["net_unit"], original["sub_code"],
        )
        if qmatch.tier in (QuantityTier.INCOMPATIBLE, QuantityTier.UNKNOWN):
            continue
        quantity_eligible.append({
            "id": c["id"],
            "canonical_name": c["canonical_name"],
            "brand": c["brand"],
            "sample_product_name": c["sample_product_name"],
            "quantity_tier": qmatch.tier,
            "quantity_diff_percent": qmatch.difference_percent,
        })
    trace["quantity_eligible_count"] = len(quantity_eligible)

    usable_candidates = [
        c for c in quantity_eligible
        if _traits_compatible(original_sample_name, c["sample_product_name"], original["sub_code"])
    ]
    trace["trait_eligible_count"] = len(usable_candidates)

    if not usable_candidates:
        result = {
            "decision_type": "no_eligible_candidates",
            "substitute_group_id": None,
            "price": None,
            "included_in_total": False,
            "quantity_diff_percent": None,
            "reasoning": "ükski kandidaat ei mahtunud koguse/omaduste piiridesse",
        }
        return await _finish(result)

    if is_baby_food:
        usable_candidates = [c for c in usable_candidates if c["quantity_tier"] == QuantityTier.AUTO]
        trace["trait_eligible_count"] = len(usable_candidates)
        if not usable_candidates:
            result = {
                "decision_type": "no_eligible_candidates",
                "substitute_group_id": None,
                "price": None,
                "included_in_total": False,
                "quantity_diff_percent": None,
                "reasoning": "beebitoit — ainult täpne kogusevaste on lubatud, ühtki ei leitud",
            }
            return await _finish(result)

    def _sort_key(c):
        tier_rank = 0 if c["quantity_tier"] == QuantityTier.AUTO else 1
        diff = c["quantity_diff_percent"] if c["quantity_diff_percent"] is not None else 0
        return (tier_rank, diff)

    usable_candidates.sort(key=_sort_key)
    candidates_for_claude = usable_candidates[:MAX_SEMANTIC_CANDIDATES]
    trace["claude_candidate_count"] = len(candidates_for_claude)

    try:
        claude_result = await _ask_claude_for_semantic_match(
            original, original_sample_name, candidates_for_claude
        )
    except SubstitutionTimeout:
        logger.warning(f"Substitution timeout group_id={group_id} chain={chain}")
        return None
    except Exception as e:
        logger.error(f"Substitution error group_id={group_id} chain={chain}: {e}")
        return None

    if claude_result is None:
        return None

    selected_id = claude_result.get("selected_group_id")
    semantic_match = bool(claude_result.get("semantic_match"))
    reasoning = claude_result.get("reason_code", "")

    if not selected_id or not semantic_match:
        result = {
            "decision_type": "semantic_rejected",
            "substitute_group_id": None,
            "price": None,
            "included_in_total": False,
            "quantity_diff_percent": None,
            "reasoning": reasoning or "Claude ei leidnud sisuliselt sobivat kandidaati",
        }
        return await _finish(result)

    matched_candidate = next((c for c in candidates_for_claude if c["id"] == selected_id), None)
    if not matched_candidate:
        result = {
            "decision_type": "semantic_rejected",
            "substitute_group_id": None,
            "price": None,
            "included_in_total": False,
            "quantity_diff_percent": None,
            "reasoning": "Claude valis kandidaadi väljastpoolt lubatud nimekirja — tagasi lükatud",
        }
        return await _finish(result)

    quantity_tier = matched_candidate["quantity_tier"]
    included_in_total = (quantity_tier == QuantityTier.AUTO)
    decision_type = "auto_substitute" if included_in_total else "suggested_substitute"

    price = await _get_group_price_in_chain(conn, selected_id, chain)

    result = {
        "decision_type": decision_type,
        "substitute_group_id": selected_id,
        "price": price,
        "included_in_total": included_in_total,
        "quantity_diff_percent": (
            float(matched_candidate["quantity_diff_percent"])
            if matched_candidate["quantity_diff_percent"] is not None else None
        ),
        "reasoning": reasoning,
    }
    return await _finish(result)


async def _save(conn, group_id, chain, result):
    ttl = _TTL_BY_DECISION.get(result["decision_type"], timedelta(days=1))
    await conn.execute(
        """
        INSERT INTO product_substitutions
            (original_group_id, chain, substitute_group_id, decision_type,
             included_in_total, quantity_diff_percent, reasoning,
             substitution_rules_version, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + $9::interval)
        ON CONFLICT (original_group_id, chain, substitution_rules_version)
        DO UPDATE SET
            substitute_group_id = EXCLUDED.substitute_group_id,
            decision_type = EXCLUDED.decision_type,
            included_in_total = EXCLUDED.included_in_total,
            quantity_diff_percent = EXCLUDED.quantity_diff_percent,
            reasoning = EXCLUDED.reasoning,
            expires_at = EXCLUDED.expires_at
        """,
        group_id, chain, result["substitute_group_id"], result["decision_type"],
        result["included_in_total"], result["quantity_diff_percent"], result["reasoning"],
        SUBSTITUTION_RULES_VERSION, ttl,
    )


async def _get_group_price_in_chain(conn, group_id, chain):
    row = await conn.fetchrow(
        """
        SELECT MIN(pr.price) AS price
        FROM product_group_members m
        JOIN products p ON p.id = m.product_id
        JOIN prices pr ON pr.product_id = p.id
        JOIN stores s ON s.id = pr.store_id
        WHERE m.group_id = $1 AND LOWER(s.chain) = $2
        """,
        group_id, chain,
    )
    return float(row["price"]) if row and row["price"] is not None else None


def _coerce_selected_id(raw):
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw) if raw.is_integer() else None
    if isinstance(raw, str):
        try:
            return int(raw.strip())
        except ValueError:
            return None
    return None


async def _ask_claude_for_semantic_match(original, original_sample_name, candidates):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY puudub keskkonnast")

    candidate_lines = "\n".join(
        f'- id={c["id"]}, grupi_nimi="{c["canonical_name"]}", '
        f'brand="{c["brand"] or ""}", tootenimi="{c["sample_product_name"] or ""}", '
        f'kogus_tier="{c["quantity_tier"].value}"'
        for c in candidates
    )

    prompt = f"""Sa aitad leida asendustoodet Eesti toidupoe hinnavõrdlusrakenduses.

ORIGINAALTOODE (mida kliendi valitud ketis pole saadaval):
grupi_nimi="{original['canonical_name']}", brand="{original['brand'] or ''}", tootenimi="{original_sample_name}"

KANDIDAADID (kogus juba deterministlikult kontrollitud):
{candidate_lines}

SINU ÜLESANNE: otsusta AINULT, kas mõni kandidaat täidab sisuliselt sama
eesmärki (sama toote TÜÜP) kui originaal. ÄRA arvesta kogust. Näiteks:
- täispiim peab asenduma täispiimaga, mitte kohvipiima/keefiri/taimse joogiga
- šokolaadipiim EI ole tavalise piima asendus
- maitsestamata jogurt EI ole maasikajogurti asendus
- kohviuba EI ole jahvatatud kohvi asendus
- värske toode EI ole suitsutatud/külmutatud toote asendus
- kui ükski kandidaat pole sisuliselt sama tüüpi, tagasta selected_group_id: null

Vasta AINULT JSON formaadis, selected_group_id peab olema TÄISARV:
{{"selected_group_id": <täisarv või null>, "semantic_match": true|false, "reason_code": "lühike põhjendus eesti keeles"}}"""

    async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS) as client:
        try:
            response = await client.post(
                ANTHROPIC_API_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        except httpx.TimeoutException:
            raise SubstitutionTimeout()

    response.raise_for_status()
    data = response.json()
    text = "".join(
        block.get("text", "") for block in data.get("content", []) if block.get("type") == "text"
    ).strip()
    text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        logger.error(f"Claude vastas mitte-JSON formaadis: {text[:200]}")
        return None

    if not isinstance(parsed, dict):
        logger.error(f"Claude vastas mitte-dict JSON-iga: {text[:200]}")
        return None

    coerced_id = _coerce_selected_id(parsed.get("selected_group_id"))
    valid_ids = {c["id"] for c in candidates}
    if coerced_id not in valid_ids:
        coerced_id = None
        parsed["semantic_match"] = False

    parsed["selected_group_id"] = coerced_id
    return parsed

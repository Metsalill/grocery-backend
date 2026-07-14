"""
Seivy — asendustoodete teenus.

Kui product_group'il puudub hind mingis ketis, otsib see moodul sama
sub_code kategooria seest selles ketis kandidaadid ja kasutab Claude API't
(Haiku 4.5) parima vaste valimiseks. Tulemus salvestatakse
product_substitutions tabelisse, nii et sama group_id + chain kombinatsiooni
kohta küsitakse Claude'ilt ainult ÜKS kord.

Kasutus compare_service.py-s:

    from substitution_service import get_or_create_substitution

    sub = await get_or_create_substitution(conn, group_id, chain)
    if sub and sub["substitute_group_id"]:
        # kasuta sub["price"] ja märgi is_substitute=True
"""

import os
import json
import logging
from typing import Optional

import httpx

logger = logging.getLogger("substitution_service")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
API_TIMEOUT_SECONDS = 6.0


class SubstitutionTimeout(Exception):
    pass


async def get_or_create_substitution(conn, group_id: int, chain: str) -> Optional[dict]:
    """
    Tagastab dict {"substitute_group_id": int|None, "price": float|None,
    "confidence": str|None} või None kui midagi läks valesti (nt timeout —
    sel juhul EI kirjutata tabelisse, proovitakse järgmisel korral uuesti).
    """
    chain = chain.lower()

    existing = await conn.fetchrow(
        """
        SELECT substitute_group_id, status
        FROM product_substitutions
        WHERE original_group_id = $1 AND chain = $2
        """,
        group_id, chain,
    )

    if existing:
        if existing["status"] == "no_match" or existing["substitute_group_id"] is None:
            return {"substitute_group_id": None, "price": None, "confidence": None}
        price = await _get_group_price_in_chain(conn, existing["substitute_group_id"], chain)
        return {
            "substitute_group_id": existing["substitute_group_id"],
            "price": price,
            "confidence": None,
        }

    original = await conn.fetchrow(
        """
        SELECT id, canonical_name, brand, sub_code
        FROM product_groups
        WHERE id = $1
        """,
        group_id,
    )
    if not original:
        return None

    original_sample_row = await conn.fetchrow(
        """
        SELECT p.name AS sample_product_name
        FROM product_group_members m
        JOIN products p ON p.id = m.product_id
        WHERE m.group_id = $1
        LIMIT 1
        """,
        group_id,
    )
    original_sample_name = original_sample_row["sample_product_name"] if original_sample_row else ""

    candidates = await conn.fetch(
        """
        SELECT DISTINCT ON (pg.id)
            pg.id, pg.canonical_name, pg.brand, p.name AS sample_product_name
        FROM product_groups pg
        JOIN product_group_members m ON m.group_id = pg.id
        JOIN products p ON p.id = m.product_id
        JOIN prices pr ON pr.product_id = p.id
        JOIN stores s ON s.id = pr.store_id
        WHERE pg.sub_code = $1
          AND LOWER(s.chain) = $2
          AND pg.id != $3
        ORDER BY pg.id, p.id
        LIMIT 25
        """,
        original["sub_code"], chain, group_id,
    )

    if not candidates:
        await conn.execute(
            """
            INSERT INTO product_substitutions
                (original_group_id, chain, substitute_group_id, status, reasoning)
            VALUES ($1, $2, NULL, 'no_match', 'candidates puudusid selles ketis')
            ON CONFLICT (original_group_id, chain) DO NOTHING
            """,
            group_id, chain,
        )
        return {"substitute_group_id": None, "price": None, "confidence": None}

    try:
        result = await _ask_claude_for_substitute(original, original_sample_name, candidates)
    except SubstitutionTimeout:
        logger.warning(f"Substitution timeout group_id={group_id} chain={chain}")
        return None
    except Exception as e:
        logger.error(f"Substitution error group_id={group_id} chain={chain}: {e}")
        return None

    substitute_id = result.get("substitute_group_id")
    confidence = result.get("confidence")
    reasoning = result.get("reasoning", "")

    status = "matched" if substitute_id else "no_match"

    await conn.execute(
        """
        INSERT INTO product_substitutions
            (original_group_id, chain, substitute_group_id, confidence, reasoning, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (original_group_id, chain) DO NOTHING
        """,
        group_id, chain, substitute_id, confidence, reasoning, status,
    )

    if not substitute_id:
        return {"substitute_group_id": None, "price": None, "confidence": None}

    price = await _get_group_price_in_chain(conn, substitute_id, chain)
    return {"substitute_group_id": substitute_id, "price": price, "confidence": confidence}


async def _get_group_price_in_chain(conn, group_id: int, chain: str) -> Optional[float]:
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


async def _ask_claude_for_substitute(original, original_sample_name, candidates) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY puudub keskkonnast")

    candidate_lines = "\n".join(
        f'- id={c["id"]}, grupi_nimi="{c["canonical_name"]}", '
        f'brand="{c["brand"] or ""}", tootenimi="{c["sample_product_name"] or ""}"'
        for c in candidates
    )

    prompt = f"""Sa aitad leida asendustoodet Eesti toidupoe hinnavõrdlusrakenduses.

ORIGINAALTOODE (mida kliendi valitud ketis pole saadaval):
grupi_nimi="{original['canonical_name']}", brand="{original['brand'] or ''}", tootenimi="{original_sample_name}"

KANDIDAADID (samas kategoorias, saadaval selles ketis):
{candidate_lines}

MÄRKUS: "brand" väli võib mõnel real olla tühi — sel juhul tuvasta bränd
"tootenimi" väljast (tootenimi sisaldab tavaliselt brändi, nt "Piim 2,5%
pure, ALMA, 1 L" tähendab bränd on Alma).

Vali kandidaatide seast KÕIGE SARNASEM asendus samale originaaltootele:
- sama kogus/pakendisuurus (lubatud erinevus kuni ~20%)
- sama toote tüüp (nt täispiim asendub täispiimaga, mitte kohupiimaga)
- kui ükski kandidaat pole mõistlik asendus, tagasta substitute_group_id: null

Vasta AINULT JSON formaadis, ilma lisatekstita:
{{"substitute_group_id": <id või null>, "confidence": "high"|"medium"|"low", "reasoning": "lühike põhjendus eesti keeles"}}"""

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
        return {"substitute_group_id": None, "confidence": None, "reasoning": "parse error"}

    valid_ids = {c["id"] for c in candidates}
    if parsed.get("substitute_group_id") not in valid_ids:
        parsed["substitute_group_id"] = None

    return parsed

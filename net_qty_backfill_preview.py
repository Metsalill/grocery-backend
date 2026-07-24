"""
Seivy — net_qty/net_unit/pack_count BACKFILL PREVIEW (ei kirjuta midagi).

Piiratud ulatus (ChatGPT soovitus, juuli 2026): AINULT dry_run_test_
shadow_candidates.py 111 testigrupi JA nende grupiliikmete kohta, mitte
kogu kataloog korraga. Kui see väike auditeeritav partii õnnestub, saab
sama loogikat laiendada ülejäänud 8 kategooria täiskataloogile.

KONVENTSIOON (tuvastatud 150 olemasoleva täidetud rea põhjal, juuli
2026 — vt SELECT ... WHERE net_qty IS NOT NULL LIMIT 150):
  - net_qty on ALATI baasühikus: g (mitte kg), ml (mitte L/l).
    "1,5 kg" -> net_qty=1500, net_unit='g'. "1 L" -> net_qty=1000,
    net_unit='ml'.
  - MULTIPAKK: net_qty on ÜHE PAKENDI kogus, MITTE kogusumma.
    "4x100g" -> net_qty=100, pack_count=4 (EI OLE 400).
    "12x100g" -> net_qty=100, pack_count=12.
  - Üksikpakend: pack_count=1.
  - Kg-hinnaga kaalutooted (nimi lõpeb pelgalt "kg"-ga ilma eesoleva
    arvuta, nt "Gouda juust kg") EI SAA net_qty väärtust — need jäävad
    teadlikult NULL, kuna fikseeritud pakendisuurus puudub.

See skript AINULT LOEB ja klassifitseerib, EI TEE ÜHTEGI UPDATE'i.
Väljund näitab täpselt, mida iga toode SAAKS, koos usaldusklassiga,
et saaksid enne tegelikku kirjutamist kõik läbi vaadata ja "kinnitan"
öelda.

KÄIVITAMINE: identne dry_run_test.py-ga (READ ONLY transaktsioon,
ei vaja isegi dry_run=True lippu, kuna siin pole üldse UPDATE-lauseid).
    export DATABASE_URL="postgresql://..."
    python3 net_qty_backfill_preview.py
"""

import asyncio
import decimal
import json
import os
import re
import sys

import asyncpg


# --- Piiratud ulatus: dry_run_test_shadow_candidates.py 111 group_id ---
SHADOW_CANDIDATE_GROUP_IDS = [
    2641, 2608, 2644, 52094, 2639, 2623, 2601, 2602, 2620, 57546, 2593,
    2662, 2675, 2631,
    3397, 3366, 3591, 3515, 51038, 51049, 3258, 3265, 3526, 3404, 3496,
    51040, 3583, 3505, 3437,
    57839, 4660, 5171, 5158, 5305, 5079, 4936, 4841, 4444, 4839, 4473,
    5299, 4924, 51090, 4687,
    4946, 5053, 4968, 4422, 5069, 4768, 4757, 4880, 51089, 4691, 4972,
    5033,
    5152, 4175, 5132, 4199, 5310, 4896, 4894, 5149, 4861, 4169, 56566,
    5337,
    25805, 25830, 25734, 25420, 25419, 25829, 25644, 28645, 25922,
    25726, 25429, 25692, 25743, 25798, 25461,
    52524, 25539, 25554, 28656, 25558, 25842, 28654, 28589, 28668,
    28670, 28351, 29213, 25849, 28584,
    14493, 14561, 14557, 14476, 14212, 14461, 14577, 57076, 14518,
    14483, 52116, 14548, 14449, 14509,
]


# --- Regex-mustrid parsimiseks ---
_MULTIPACK_RE = re.compile(
    r"(\d+)\s*[x×*]\s*(\d+[.,]?\d*)\s*(kg|g|l|ml)\b", re.IGNORECASE
)
_SINGLE_RE = re.compile(
    r"(\d+[.,]?\d*)\s*(kg|g|l|ml)\b", re.IGNORECASE
)
# Kg-hinnaga kaalutoode: nimi lõpeb "kg"-ga ilma vahetult eesoleva
# arvuta (nt "Gouda juust kg", mitte "1kg" ega "500 g kg" - viimast
# ei eksisteeri reaalselt, aga kontroll on range).
_BARE_KG_SUFFIX_RE = re.compile(r"(?<![\d.,])\s*kg\s*$", re.IGNORECASE)


def _to_base_unit(qty: float, unit: str) -> tuple[decimal.Decimal, str]:
    unit_lower = unit.lower()
    if unit_lower == "kg":
        return decimal.Decimal(str(qty)) * 1000, "g"
    if unit_lower == "l":
        return decimal.Decimal(str(qty)) * 1000, "ml"
    return decimal.Decimal(str(qty)), unit_lower  # juba g/ml


def classify_and_parse(name: str) -> dict:
    """Tagastab dict: {status, net_qty, net_unit, pack_count, reason}."""
    if not name:
        return {"status": "SKIP_UNPARSEABLE", "reason": "tühi nimi"}

    if _BARE_KG_SUFFIX_RE.search(name) and not _MULTIPACK_RE.search(name):
        # Nimi lõpeb "kg"-ga, aga kontrolli, et see poleks osa "1kg" mustrist
        # (_SINGLE_RE oleks selle juba varem tabanud "1kg" puhul, kuna
        # \d+ nõuab numbrit VAHETULT enne ühikut, ilma tühikuta või
        # tühikuga - regex katab mõlemad "1kg" ja "1 kg").
        # Kontrolli veel kord otse: kas on numbrit vahetult enne "kg"-d
        # (koos tühikuga või ilma).
        m = _SINGLE_RE.search(name)
        if m and m.group(2).lower() == "kg" and name.rstrip().lower().endswith("kg"):
            # nt "500 g kg" ei juhtu, aga "Gouda 1kg" oleks siin match'inud
            # SINGLE_RE juba - kontrolli kas match lõpeb sõne lõpus
            if name.rstrip()[-len(m.group(0)):].strip() == m.group(0).strip():
                pass  # on tegelikult "1kg" tüüpi, lase edasi tavalisse harusse
        else:
            return {
                "status": "SKIP_KG_PRICED",
                "reason": "nimi lõpeb kaalutoote 'kg'-ga ilma fikseeritud kogusenumbrita",
            }

    m = _MULTIPACK_RE.search(name)
    if m:
        pack_count = int(m.group(1))
        per_unit_qty = float(m.group(2).replace(",", "."))
        unit = m.group(3)
        net_qty, net_unit = _to_base_unit(per_unit_qty, unit)
        return {
            "status": "WOULD_SET_MULTIPACK",
            "net_qty": net_qty,
            "net_unit": net_unit,
            "pack_count": pack_count,
            "reason": f"multipakk-muster '{m.group(0)}'",
        }

    m = _SINGLE_RE.search(name)
    if m:
        qty = float(m.group(1).replace(",", "."))
        unit = m.group(2)
        net_qty, net_unit = _to_base_unit(qty, unit)
        return {
            "status": "WOULD_SET_SINGLE",
            "net_qty": net_qty,
            "net_unit": net_unit,
            "pack_count": 1,
            "reason": f"üksikpakend-muster '{m.group(0)}'",
        }

    return {"status": "SKIP_UNPARSEABLE", "reason": "ühtegi kogusemustrit ei leitud nimest"}


async def run_preview():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("VIGA: DATABASE_URL keskkonnamuutuja puudub.", file=sys.stderr)
        sys.exit(1)

    conn = await asyncpg.connect(database_url)
    try:
        async with conn.transaction(readonly=True):
            rows = await conn.fetch(
                """
                SELECT DISTINCT p.id, p.name, p.sub_code, p.net_qty, p.net_unit,
                       p.pack_count, m.group_id
                FROM products p
                JOIN product_group_members m ON m.product_id = p.id
                WHERE m.group_id = ANY($1::int[])
                  AND p.net_qty IS NULL
                ORDER BY p.sub_code, m.group_id, p.id
                """,
                SHADOW_CANDIDATE_GROUP_IDS,
            )
    finally:
        await conn.close()

    print(f"Leitud {len(rows)} toodet (111 testigrupi liikmed), millel net_qty on NULL.\n")

    results = []
    counts = {}
    for r in rows:
        parsed = classify_and_parse(r["name"])
        counts[parsed["status"]] = counts.get(parsed["status"], 0) + 1
        results.append({
            "product_id": r["id"],
            "group_id": r["group_id"],
            "sub_code": r["sub_code"],
            "name": r["name"],
            "current_pack_count": r["pack_count"],
            **parsed,
        })

    print("=== KOKKUVÕTE ===")
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}")

    print("\n=== NÄIDISED IGAST KLASSIST (kuni 15 igast) ===")
    by_status = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)
    for status, items in sorted(by_status.items()):
        print(f"\n--- {status} ({len(items)}) ---")
        for item in items[:15]:
            if status.startswith("WOULD_SET"):
                print(
                    f"  [{item['sub_code']}] '{item['name']}' "
                    f"-> net_qty={item['net_qty']}, net_unit={item['net_unit']}, "
                    f"pack_count={item['pack_count']}  ({item['reason']})"
                )
            else:
                print(f"  [{item['sub_code']}] '{item['name']}'  ({item['reason']})")

    with open("net_qty_backfill_preview.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print("\nTäisväljund salvestatud: net_qty_backfill_preview.json")
    print("\nEI TEHTUD ÜHTEGI UPDATE'i. See on ainult eelvaade.")


if __name__ == "__main__":
    asyncio.run(run_preview())

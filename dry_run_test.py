"""
Seivy — Etapp 5: reaalsete Railway andmetega READ-ONLY kuivtest.

KAHEKIHILINE KAITSE (ChatGPT nõue):
  KIHT 1: dry_run=True substitution_service'is — _save() ei kutsuta.
  KIHT 2: BEGIN TRANSACTION READ ONLY — DB tasemel keeldub igast
          INSERT/UPDATE/DELETE katsest, isegi kui koodis oleks viga.

/compare EI IMPORDI EGA KUTSU seda skripti — see on täiesti eraldiseisev,
käivitatakse käsitsi (nt Railway shell'i kaudu või lokaalselt
DATABASE_URL keskkonnamuutujaga).

KÄIVITAMINE:
    export DATABASE_URL="postgresql://..."
    export ANTHROPIC_API_KEY="sk-ant-..."
    python3 dry_run_test.py

VÄLJUND: iga testjuhtumi kohta üks struktureeritud JSON (trace), mida
saab hiljem käsitsi klassifitseerida õigeks/valeks ja arvutada
AUTO precision / SUGGESTED precision / false-auto count jne.
"""

import asyncio
import json
import os
import sys

import asyncpg

from substitution_service import get_or_create_substitution


# Testjuhtumid: (original_group_id, chain, kirjeldus/ootus). ASENDA
# reaalsete group_id väärtustega enne käivitamist (vt lõpust
# "TESTJUHTUMITE LEIDMINE" SQL päring).
TEST_CASES = [
    # --- Sama tüüp + kogus teada, puudub 1 ketist -> oodatav auto_substitute ---
    (2611, "rimi", "Tere piim D-vit 1L puudub Rimist — oodatav: auto_substitute sama 1L piimaga"),
    (2616, "maxima", "Tere Cappuccino 1L puudub Maximast — oodatav: auto_substitute"),
    (2617, "maxima", "Tere Latte 1L puudub Maximast — oodatav: auto_substitute"),
    (2604, "rimi", "Farmi täispiim 2L puudub Rimist — oodatav: auto_substitute (2L on suur kogus, kandidaate vähe)"),

    # --- Teadaolev v1 viga, korrektsuse regressioonitest ---
    (2591, "maxima", "Alma piim 1L, v1 valis vääralt 1,5L — v2/v3/v4 peaks valima täpse 1L kandidaadi"),

    # --- Suurem/väiksem kogus, potentsiaalne suggested-vahemik ---
    (2594, "coop", "Alma täispiim 0,5L puudub Coopist — kas leitakse 0,5L vaste või suggested suurem?"),
    (2594, "selver", "Alma täispiim 0,5L puudub Selverist — sama kontroll teises ketis"),
    (2597, "coop", "Alma täispiim 2L (suur pakend) puudub Coopist — kas leitakse suur kandidaat?"),
    (2597, "maxima", "Alma täispiim 2L puudub Maximast"),

    # --- Kogus puudub originaalil endal -> oodatav no_quantity_data, Claude't EI kutsuta ---
    (2636, "maxima", "Piimajook šokolaadi 200ml — net_qty puudub KÕIGIL grupi liikmetel"),
    (2637, "coop", "Piimajook maasika 200ml — net_qty puudub"),

    # --- Laktoosivaba originaal -> ainult laktoosivaba kandidaat sobib ---
    (2615, "maxima", "Tere laktoosivaba 1L puudub Maximast — Maxima kandidaadid (2594,2604,2611) EI OLE laktoosivabad, oodatav: no_eligible_candidates (trait-kaitse töötab)"),
    (2621, "maxima", "Piim laktoosivaba 2,5% 1L puudub Maximast — sama trait-kontroll"),
    (2628, "maxima", "Eila laktoosivaba piimajook 1,5L puudub Maximast"),

    # --- Beebitoit -> ainult täpne kogusevaste, muidu no_eligible_candidates ---
    (7597, "selver", "Kaerapudrupulber BIO 200g puudub Selverist — range beebitoidu kontroll"),
    (7600, "maxima", "Head Ood piimapuder banaaniga 190g puudub Maximast — oodatav tõenäoliselt no_eligible_candidates (auto_pct=0)"),
    (7601, "rimi", "Head Ood piimapuder puuviljadega 190g puudub Rimist"),

    # --- Katmata/uncovered sub_code -> fail-closed (no_quantity_data VÕI no_eligible_candidates) ---
    (3980, "maxima", "Kinder Maxi King 3x35g — sweets_chocolate_bars pole QUANTITY_RULES-is, fail-closed test"),
    (3981, "coop", "Kinder Milk Slice 84g — sama uncovered-kategooria test"),

    # --- Munad (tükikaubad, dairy_eggs) ---
    (2519, "rimi", "Whitepro munavalge 1kg puudub Rimist — mass-põhine tükitoode"),
    (2534, "coop", "Kotimaista Mahe munad M 6tk — ainult Prismas, testib laia kandidaatide otsingut"),
]


class _IntentionalRollback(Exception):
    pass


async def run_dry_run_tests():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("VIGA: DATABASE_URL keskkonnamuutuja puudub.", file=sys.stderr)
        sys.exit(1)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("VIGA: ANTHROPIC_API_KEY keskkonnamuutuja puudub.", file=sys.stderr)
        sys.exit(1)

    conn = await asyncpg.connect(database_url)
    results = []

    try:
        async with conn.transaction(readonly=True):
            for group_id, chain, description in TEST_CASES:
                print(f"\n{'='*70}\nTEST: group_id={group_id}, chain={chain}\n{description}\n{'='*70}")
                try:
                    result = await get_or_create_substitution(conn, group_id, chain, dry_run=True)
                except Exception as e:
                    print(f"TEHNILINE VIGA: {e}")
                    result = {"error": str(e), "trace": {"original_group_id": group_id, "chain": chain}}

                if result is None:
                    result = {
                        "decision_type": "provider_error_or_timeout",
                        "trace": {"original_group_id": group_id, "chain": chain},
                    }

                result["trace"]["test_description"] = description
                results.append(result)
                print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

            print(f"\n{'='*70}\nREAD ONLY transaktsioon lõpetatakse (ROLLBACK, mitte COMMIT)\n{'='*70}")
            raise _IntentionalRollback()

    except _IntentionalRollback:
        pass
    finally:
        await conn.close()

    print(f"\n\n{'#'*70}\nKOKKUVÕTE\n{'#'*70}")
    by_decision = {}
    for r in results:
        dt = r.get("decision_type", "ERROR")
        by_decision[dt] = by_decision.get(dt, 0) + 1
    for dt, count in sorted(by_decision.items()):
        print(f"  {dt}: {count}")

    write_attempts = sum(1 for r in results if r.get("trace", {}).get("database_write_attempted"))
    print(f"\nAndmebaasi kirjutamiskatseid (kõik dry_run poolt tõkestatud): {write_attempts}")
    print("Tegelikke INSERT/UPDATE lauseid EI täidetud (READ ONLY transaktsioon + dry_run=True).")

    with open("dry_run_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print("\nTäisväljund salvestatud: dry_run_results.json")


if __name__ == "__main__":
    asyncio.run(run_dry_run_tests())

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

    # --- Koor/hapukoor (dairy_cream_sourcream, sh 'lakt.vaba' lühendi test) ---
    (2692, "coop", "Hapukoor 20% 250g puudub Coopist"),
    (2692, "maxima", "Hapukoor 20% 250g puudub Maximast"),
    (2694, "coop", "Hapukoor 10% 500g puudub Coopist"),
    (2695, "maxima", "Kohvikoor 10% 200ml puudub Maximast"),
    (2698, "rimi", "Vahukoor 35% 200ml puudub Rimist"),
    (2706, "maxima", "Farmi koogikoor 15% lakt.vaba puudub Maximast — 'lakt.vaba' lühendi tuvastuse test"),
    (2719, "maxima", "Tere hapukoor 20% lakt.vaba puudub Maximast — sama lühendi test"),
    (2724, "coop", "Tere vahukoor 35% lakt.vaba puudub Coopist"),
    (2728, "maxima", "Saare mahe hapukoor 20% lakt.vaba puudub Maximast"),

    # --- Kohv (coffee_beans_ground, caffeine_state kontroll) ---
    (25430, "coop", "Paulig Juhla Mokka kofeiinivaba 270g — ainult Prismas, kofeiinivaba trait test"),
    (25493, "rimi", "Jacobs Kronung filtrikohv 500g puudub Rimist"),
    (25493, "selver", "Jacobs Kronung filtrikohv 500g puudub Selverist"),
    (25639, "maxima", "Merrild In-Cup tassikohv 400g puudub Maximast"),
    (25639, "selver", "Merrild In-Cup tassikohv 400g puudub Selverist"),

    # --- Maitseained (spices_herbs_spice_mix, rangem 10%/25% piir) ---
    (11295, "maxima", "Klassikaline kanamarinaad 75g puudub Maximast"),
    (11295, "rimi", "Klassikaline kanamarinaad 75g puudub Rimist"),
    (24856, "maxima", "Hakklihamaitseaine Santa Maria 30g puudub Maximast"),
    (24866, "maxima", "Kartulimaitseaine Santa Maria 30g puudub Maximast"),

    # --- Joogid (drinks_energy, suhkruvaba trait test) ---
    (17085, "maxima", "Energiajook Red Bull 4x250ml puudub Maximast"),
    (17085, "rimi", "Energiajook Red Bull 4x250ml puudub Rimist"),
    (17086, "coop", "Red Bull Suhkruvaba 250ml puudub Coopist — suhkruvaba trait test"),
    (17086, "rimi", "Red Bull Suhkruvaba 250ml puudub Rimist — sama trait test"),
    (17112, "maxima", "Monster Green Zero 500ml puudub Maximast"),
    (17117, "coop", "Monster Mango Loco 500ml puudub Coopist"),
    (17117, "maxima", "Monster Mango Loco 500ml puudub Maximast"),

    # --- Õlid (oils_olive, katmata kategooria fail-closed test) ---
    (14447, "maxima", "Borges ekstra neitsioliivioli fruity 500ml puudub Maximast — oils_olive pole QUANTITY_RULES-is"),
    (14452, "maxima", "Borges Original ekstra vaarioliivioli 250ml puudub Maximast"),
    (14454, "maxima", "Borges Original ekstra vaarioliivioli 1L puudub Maximast"),

    # --- Leivad (bakery_bread_loaves, katmata kategooria) ---
    (5394, "maxima", "Leibur Kuldne 5-vilja röstsai 525g puudub Maximast"),
    (5394, "rimi", "Leibur Kuldne 5-vilja röstsai 525g puudub Rimist"),
    (5404, "maxima", "Leibur Kaerasuda 380g puudub Maximast"),
    (5434, "maxima", "Juuretise peenleib 500g puudub Maximast"),
    (5448, "coop", "Tõistera Röst 250g puudub Coopist"),

    # --- Küpsised (sweets_biscuits_cookies, katmata kategooria) ---
    (40966, "maxima", "Šokolaadimaitseline küpsis 163g puudub Maximast"),
    (40967, "maxima", "Vanillimaitseline küpsis 163g puudub Maximast"),
    (40973, "maxima", "Mesikäpp Dops küpsis 210g vanilli puudub Maximast"),
    (40984, "maxima", "Väike Võõnik kaeraküpsis rosinatega 250g puudub Maximast"),
    (41004, "maxima", "Marmiton kodune kaerakook 400g puudub Maximast"),

    # --- Liha (meat_beef_lamb_game/meat_minced, animal_type kontroll) ---
    (11571, "maxima", "Rohumaaveise antrekoodi steik 240g puudub Maximast"),
    (11571, "rimi", "Rohumaaveise antrekoodi steik 240g puudub Rimist"),
    (11575, "coop", "Rohumaaveise Picanha steik 220g puudub Coopist"),
    (11575, "maxima", "Rohumaaveise Picanha steik 220g puudub Maximast"),
    (11578, "coop", "Rohumaaveise burgeripihv 170g puudub Coopist"),
    (11580, "coop", "Lihaveise lihaloiked 390g puudub Coopist"),
    (11580, "maxima", "Lihaveise lihaloiked 390g puudub Maximast"),
    (11594, "coop", "Rakvere veiseklops vasardatud 400g puudub Coopist"),
    (11612, "rimi", "Linnamäe hirveliha steik 240g puudub Rimist — 'hirv' pole animal_type sõnastikus, edge case"),
    (11441, "rimi", "Rakvere seahakkliha 400g puudub Rimist — animal_type=pork test"),
    (11441, "selver", "Rakvere seahakkliha 400g puudub Selverist"),

    # --- Jogurt/keefir (fat_class_yogurt + yogurt_form kontroll) ---
    (3193, "coop", "Maitsestamata jogurt 2,5% 1kg puudub Coopist"),
    (3198, "rimi", "Joogijogurt banaani-maasika 1kg puudub Rimist — yogurt_form=drinkable test"),
    (3214, "coop", "Kreeka jogurt maitsestamata 370g puudub Coopist — yogurt_form=greek test"),
    (3271, "maxima", "Hellus keefir laktoosivaba 1kg puudub Maximast"),
    (3284, "rimi", "Farmi keefir 2,5% 1kg puudub Rimist"),
    (3315, "rimi", "Farmi Skyr maasika 300g puudub Rimist"),
    (3333, "maxima", "Tere kreeka jogurt maitsestamata lakt.vaba 350g puudub Maximast"),
    (3351, "maxima", "Profeel proteiinijogurtijook kirsi 275g puudub Maximast — yogurt_form=protein test"),
    (3400, "coop", "Nopri hapupiim 2,5% 1L puudub Coopist"),
    (3445, "selver", "Kreeka stiilis lakt.vaba jogurt 10% 500g puudub Selverist — fat_class_yogurt=greek_high_fat test"),

    # --- Juust (cheese_type + cheese_form kontroll) ---
    (4671, "maxima", "Granarolo Mozzarella 250g puudub Maximast — ChatGPT mozzarella näide"),
    (4705, "coop", "President Brie 200g puudub Coopist"),
    (4392, "maxima", "Atleet Cheddar riivjuust 200g puudub Maximast — cheese_form=grated test"),
    (4410, "maxima", "Royal Gouda Red viilud 150g puudub Maximast — cheese_form=sliced test"),
    (4454, "coop", "Suitsutatud Kadaka riivjuust 200g puudub Coopist"),
    (4485, "maxima", "E-Piim Gouda viilud 300g puudub Maximast"),
    (4986, "selver", "Parmigiano Reggiano 150g puudub Selverist"),
    (5005, "rimi", "Coop Feta laktoosivaba 150g puudub Rimist — lactose_free + cheese_type test"),
    (4963, "coop", "Wyke Farms Ivy Vintage Cheddar 200g puudub Coopist"),

    # --- Kala (fish_species kontroll) ---
    (11910, "maxima", "Coop lohefilee 2x150g puudub Maximast — fish_species=salmon test"),
    (23695, "maxima", "Heeringafilee tükid juurviljadega 400g puudub Maximast — fish_species=herring test"),
    (23767, "maxima", "Heeringafilee sibulaga koorekastmes puudub Maximast"),
    (23986, "coop", "Xtra krevetid 330g puudub Coopist — fish_species=shrimp test"),
    (24084, "maxima", "Krabimaitselised surimi pulgad 150g puudub Maximast — tundmatu liik, edge case"),

    # --- Vein (AUTO_DISABLED kontroll — ei tohi kunagi olla auto_substitute) ---
    (29386, "maxima", "Baron Rosen Vino Tinto 1L puudub Maximast — vein ei tohi olla AUTO"),
    (30052, "maxima", "Maori Bay Sauvignon Blanc 75cl puudub Maximast — vein ei tohi olla AUTO"),
    (31477, "maxima", "Roche Mazet Sauvignon Blanc 187ml puudub Maximast — väike pudel, vein AUTO-keeld"),
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
                    # SAVEPOINT iga testi ümber — kui see test ebaõnnestub
                    # (nt andmeviga), ROLLBACK toimub AINULT selle testi
                    # tasandil, mitte kogu READ ONLY transaktsiooni jaoks.
                    # Ilma selleta rikub üks Postgres-tasandi viga kõik
                    # järgnevad testid ("current transaction is aborted").
                    async with conn.transaction():
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

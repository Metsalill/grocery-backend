"""
Seivy — Etapp 5B eelne sihitud kinnitustest: AINULT need 8 kategooriat,
mida plaanitakse esimesse shadow mode ringi lubada:
    dairy_milk, dairy_yogurt_kefir, cheese_regular, cheese_delicatessen,
    dairy_cheese_slices, coffee_beans_ground, coffee_instant, oils_olive

See fail on TEADLIKULT ERALDI dry_run_test.py-st (mis katab kõiki
sub_code'e, 252 testi). Põhjus (ChatGPT soovitus, juuli 2026): shadow
mode hakkab tööle KOGU reaalse kataloogi peal, mitte ainult käsitsi
valitud "heade" juhtumite peal — seega suurim allesjäänud risk pole
enam teadaolev regressioon, vaid VALIMI KALLUTATUS. Kõik selle faili
group_id'd on UUED (ei kattu 252-testi põhikomplektiga), valitud
mitmekesisuse kriteeriumide järgi:
    - uued originaalgrupid, mida põhivalimis polnud
    - mitu puuduvat ketti sama originaali kohta
    - koguse piiri lähedal olevad juhtumid (nt piiripealsed %-d)
    - tundmatud brändid/tooteseeriad (fail-open riskikoht)
    - multipakid (3x380g, 4x120g, 10x16g jne)
    - ebatavalised vormid/pakendid (spray, metallpurk, klaaspudel)
    - juhtumid, kus oluline tunnus (nt taimne juust, kitsepiim,
      proteiinijook) on canonical_name'is, mitte struktureeritud väljas

KÄIVITAMINE: identne dry_run_test.py-ga.
    export DATABASE_URL="postgresql://..."
    export ANTHROPIC_API_KEY="sk-ant-..."
    python3 dry_run_test_shadow_candidates.py

LÄVEND ENNE SHADOW MODE'i (ChatGPT):
    - 0 kinnitatud false-AUTO
    - 0 provider/test error pärast retry'd
    - kõik AUTO-d käsitsi auditeeritud
    - vähemalt 10-15 sisulist AUTO-t iga suurema kategooria kohta
    - kui oils_olive ei anna piisavalt otsuseid (halb net_qty katvus),
      jäta see esimesest shadow-grupist välja
"""

import asyncio
import json
import os
import sys

import asyncpg

from substitution_service import get_or_create_substitution


TEST_CASES = [
    # --- dairy_milk (14) ---
    (2641, "selver", "Aasa piimajook šokolaadi 450ml puudub Selverist — maitsestatud piimajook"),
    (2608, "rimi", "Piim 3,2% 1L puudub Rimist — piiripealne rasvaprotsent (3,0 bucket)"),
    (2644, "rimi", "Barebells proteiinisheik maasika 330ml puudub Rimist — proteiinijook, mitte tavapiim"),
    (52094, "coop", "Quick Milk piimakoored puuvilja 30g puudub Coopist — ebatavaline vorm/kogus"),
    (2639, "selver", "Piimajook banaani 200ml puudub Selverist — väike pakend"),
    (2623, "coop", "Piim laktoosivaba 3,2% UHT 1L puudub Coopist"),
    (2601, "maxima", "Farmi piim 2,5% 1,5L pure puudub Maximast — vs 2602 (1kg pure) unit_mismatch test"),
    (2602, "maxima", "Farmi piim 2,5% 1kg pure puudub Maximast — kaal vs maht sama toote juures"),
    (2620, "rimi", "Piim 3,5% UHT 1L puudub Rimist"),
    (57546, "maxima", "Pastoriseeritud piim 2% 1L (brändita) puudub Maximast"),
    (2593, "coop", "Piim kile 2,5% 1L puudub Coopist"),
    (2662, "coop", "Andri-Peedo kitsepiim pastöriseeritud 0,96L puudub Coopist — kitsepiim vs lehmapiim, animal_type gap?"),
    (2675, "rimi", "Arla Protein kakaojook 1L UHT puudub Rimist — proteiinijook, kakaomaitseline"),
    (2631, "selver", "Müllermilch piimajook maasika 400g puudub Selverist — kaaluühikus piimajook"),

    # --- dairy_yogurt_kefir (15) ---
    (3397, "rimi", "Saare kreeka jogurt HULGI 3x380g puudub Rimist — multipakk"),
    (3366, "selver", "Activia maasika jogurt 4x120g puudub Selverist — multipakk"),
    (3591, "rimi", "MIO&RIO joogijogurt maasika 1kg puudub Rimist — drinkable"),
    (3515, "coop", "Isey Skyr vanillimaitseline 2% lakt.vaba 400g puudub Coopist — Skyr tüüp"),
    (51038, "rimi", "AB-jogurt teravilja SIPSIKU jäätise 165g puudub Rimist — teravilja-jogurt"),
    (51049, "coop", "Proteiinijogurt WELL DONE mustika 150g puudub Coopist"),
    (3258, "selver", "Gefilus maasika-yuzu jogurt 380g puudub Selverist — ebatavaline maitsekombo"),
    (3265, "rimi", "Gefilus jogurtijook banaani-maasika lakt.vaba 4x100g puudub Rimist — multipakk + drinkable + lakt.vaba"),
    (3526, "maxima", "Isey Skyr Air sidrunimaitseline lakt.vaba 125g puudub Maximast — Skyr Air (teine tekstuur)"),
    (3404, "maxima", "Hapendatud pett 1,5% 1kg puudub Maximast — kohupiimapett, mitte tavajogurt"),
    (3496, "coop", "Andri-Peedo kitsepiimajogurt maitsestamata 500ml puudub Coopist — kitsejogurt"),
    (51040, "coop", "Maitseelamused Proteiini skyr mosli marjadega 235g puudub Coopist — skyr + mosli sisse segatud"),
    (3583, "maxima", "Saidafarm keefir 1L puudub Maximast"),
    (3505, "rimi", "Baltais leivajogurt musta ploomiga 380g puudub Rimist — leivajogurt, ebatavaline"),
    (3437, "coop", "Kreeka jogurt maitsestamata 180g puudub Coopist — väike pakend"),

    # --- cheese_regular (15) ---
    (57839, "coop", "HULGI Valio Atleet Cheddar viilutatud 2x500g puudub Coopist — multipakk/hulgipakend"),
    (4660, "coop", "Piimameister Otto Mozzarella kirsid 125g puudub Coopist — minimozzarella"),
    (5171, "maxima", "Xtra riivjuust taimerasvaga 500g puudub Maximast — TAIMERASVAGA, mitte päris juust"),
    (5158, "rimi", "Xtra Edam viilutatud 150g puudub Rimist"),
    (5305, "maxima", "Nopri grilljuust ürtidega 200g puudub Maximast — halloumi-tüüpi grilljuust"),
    (5079, "coop", "Kotimaista Emmental riivjuust Mustaleima 150g puudub Coopist"),
    (4936, "coop", "Gouda DELI Q tomati-oliividega kg puudub Coopist — maitsestatud gouda"),
    (4841, "rimi", "Coop salatijuust plokina 200g puudub Rimist"),
    (4444, "maxima", "Old Saare Special 12 kuud 280g puudub Maximast — 12 kuud laagerdatud"),
    (4839, "rimi", "Kortos salatijuust ürtidega 160g puudub Rimist"),
    (4473, "coop", "E-Piim Eesti juust light viilud 200g puudub Coopist — kergem rasvasisaldus"),
    (5299, "maxima", "Top Food Juustuniidid suitsutatud 100g puudub Maximast — juustuniidid, ebatavaline vorm"),
    (4924, "coop", "Landana 1000 päeva Gouda 180g puudub Coopist — pika laagerdusega"),
    (51090, "rimi", "Jon-Chedar Juustuämps Tzatziki-tšilli 80g puudub Rimist — väike snäkkpakend"),
    (4687, "rimi", "Coop Mozzarella kirsid 125g puudub Rimist"),

    # --- cheese_delicatessen (12) ---
    (4946, "rimi", "Gourmet Cheese Gouda punane pesto kg puudub Rimist — pesto-gouda"),
    (5053, "coop", "Castelli Parmigiano Reggiano riivjuust 70g puudub Coopist"),
    (4968, "coop", "Old Irish Creamery Cheddar punase veiniga 150g puudub Coopist — veiniga cheddar"),
    (4422, "coop", "Aura sinihallitusjuust 170g puudub Coopist — sinihallitus"),
    (5069, "rimi", "Parrano Robusto riivjuust 60g puudub Rimist"),
    (4768, "rimi", "Formagia Gorgonzola DOP 100g puudub Rimist — DOP-märgistus"),
    (4757, "rimi", "Memel Blue kg puudub Rimist — sinihallitus, tundmatu bränd"),
    (4880, "coop", "Turek kreemjuust kitsepiimast 150g puudub Coopist — kitsepiima kreemjuust"),
    (51089, "rimi", "Hispaania juust DELI Q 130g puudub Rimist"),
    (4691, "maxima", "La Bella Mozzarella di Bufala kirsid 200g puudub Maximast — pühvlimozzarella"),
    (4972, "maxima", "Westminster Cheddar Vintage 150g puudub Maximast"),
    (5033, "maxima", "Kitsejuust viilutatud 150g puudub Maximast — kitsejuust viiludena"),

    # --- dairy_cheese_slices (12) ---
    (5152, "coop", "Creme Bonjour maitsestamata laktoosivaba 200g puudub Coopist"),
    (4175, "rimi", "Sulatatud juust originaal 200g puudub Rimist"),
    (5132, "coop", "Violife Mature Cheddar võileivaviilud 200g puudub Coopist — TAIMNE juust (vegan bränd)"),
    (4199, "coop", "E-Piim lepasuitsu määrdejuust 180g puudub Coopist"),
    (5310, "rimi", "Hiirte Juust sulatatud juust 400g puudub Rimist"),
    (4896, "coop", "ICA toorjuust küüslaugu-ürtidega 200g puudub Coopist"),
    (4894, "coop", "Kreemjas toorjuust 150g puudub Coopist"),
    (5149, "coop", "Creme Bonjour laktoosivaba aiaürtidega 200g puudub Coopist"),
    (4861, "coop", "Piimameister Otto toorjuust 150g puudub Coopist"),
    (4169, "coop", "Sulatatud juust Forte 185g puudub Coopist"),
    (56566, "rimi", "Saaremaa toorjuust mango-tšillipipar 170g puudub Rimist — ebatavaline maitsekombo"),
    (5337, "maxima", "E-Piim sulatatud suitsujuust küüslauguga 200g puudub Maximast"),

    # --- coffee_beans_ground (15) ---
    (25805, "maxima", "Luxus Bodum kannukohv 500g puudub Maximast — presskannu spetsiifiline"),
    (25830, "coop", "Gurmans maitsekohv Irish Creme 125g puudub Coopist — maitsestatud kohv, tundmatu bränd"),
    (25734, "maxima", "Best Beans Espresso Supreme kohvioad 1kg puudub Maximast — tundmatu bränd"),
    (25420, "selver", "Paulig Classic Aromatico kohvioad 1kg puudub Selverist — vs 25419 (jahvatatud) sama seeria, eri vorm"),
    (25419, "maxima", "Paulig Classic Aromatico jahvatatud kohv 500g puudub Maximast — vs 25420 (oad) sama seeria"),
    (25829, "coop", "Gurmans filtrikohv aromatiseeritud Tiramisu 125g puudub Coopist — maitsestatud filtrikohv"),
    (25644, "maxima", "Merrild Barista Cremoso kohvioad 1kg puudub Maximast"),
    (28645, "coop", "Kohvioad Lavazza Qualita Rossa 1kg puudub Coopist — teine Lavazza seeria"),
    (25922, "maxima", "Paulig Presidentti Original Strong kohvioad 1kg puudub Maximast — kolmas Paulig seeria"),
    (25726, "coop", "OA Coffee No1 kohvioad hele röst 1kg puudub Coopist — tundmatu bränd"),
    (25429, "maxima", "Paulig Juhla Mokka filtrikohv 500g puudub Maximast — neljas Paulig seeria"),
    (25692, "coop", "Davidoff Rich Aroma jahvatatud kohv 250g puudub Coopist"),
    (25743, "maxima", "Coffeestar šokolaadi kohvioad 200g puudub Maximast — maitsestatud oad"),
    (25798, "coop", "Kohv röstitud jahvatatud Rimi 500g puudub Coopist — kaubamärgita/private label"),
    (25461, "rimi", "Lavazza Qualita Oro kohvioad 1kg puudub Rimist — regressiooni lisakontroll teise group_id peal"),

    # --- coffee_instant (14) ---
    (52524, "coop", "Nescafe Gold lahustuv kohv 200g puudub Coopist"),
    (25539, "maxima", "Nescafe Classic Strong lahustuv kohv 250g puudub Maximast"),
    (25554, "coop", "Nescafe 3in1 Strong 10x16g puudub Coopist — multipakk + 3in1 (suhkur/koorits sees)"),
    (28656, "coop", "Kohvijook lahustuv 2in1 Nescafe 8g puudub Coopist — 2in1, väga väike kogus"),
    (25558, "coop", "Nescafe Cappuccino Irish Box 8x14g puudub Coopist — cappuccino maitseline multipakk"),
    (25842, "coop", "Lahustuv kohv 200g (Aroma Gold) puudub Coopist — tundmatu bränd"),
    (28654, "coop", "Kohvijook 3in1 NESCAFE Strong karp 28x16g puudub Coopist — suur multipakk"),
    (28589, "coop", "Jacobs lahustuv kohv Kronung 100g puudub Coopist"),
    (28668, "maxima", "Lahustuv kohvijook Jacobs 3in1 Original 10x12,6g puudub Maximast — multipakk"),
    (28670, "coop", "Nescafe 3in1 Creamy Latte 10x15g puudub Coopist — latte maitseline multipakk"),
    (28351, "coop", "Cappuccino La Festa klassik 10x12,5g puudub Coopist — tundmatu bränd"),
    (29213, "maxima", "Xtra lahustuv kohv 200g puudub Maximast — private label"),
    (25849, "maxima", "Vespucci granuleeritud lahustuv kohv 100g puudub Maximast — tundmatu bränd"),
    (28584, "coop", "Jacobs lahustuv kohvijook 2in1 MONTE PERO 14g puudub Coopist"),

    # --- oils_olive (14) ---
    (14493, "coop", "La Espanola oliiviõli 1L klaaspudelis puudub Coopist — klaaspakend"),
    (14561, "maxima", "Salvadori ekstra väärisoliiviõli 500ml puudub Maximast"),
    (14557, "coop", "I Love Eco ekstra väärisoliiviõli Itaalia 500ml puudub Coopist — Rimi private-label bränd"),
    (14476, "rimi", "Coop oliiviõli 500ml puudub Rimist — kvaliteediklass märkimata"),
    (14212, "maxima", "Herkku Organic väärisoliiviõli 500ml puudub Maximast — mahe"),
    (14461, "coop", "Borges Harmony oliiviõli 500ml puudub Coopist — kolmas Borges seeria (vs Fruity/Original)"),
    (14577, "maxima", "Bono Val di Mazara DOP ekstra väärisoliiviõli 500ml puudub Maximast — DOP"),
    (57076, "maxima", "Gustolu Extra Virgin oliiviõli 500ml puudub Maximast — tundmatu bränd"),
    (14518, "maxima", "Filos original väärisoliiviõli 500ml puudub Maximast"),
    (14483, "rimi", "Dava oliiviõli spray 200ml puudub Rimist — PIHUSTUSVORM, ebatavaline"),
    (52116, "coop", "Spartan Treasury ekstra neitsioliiviõli 750ml puudub Coopist — 'neitsi' termin, mitte 'vääris'"),
    (14548, "maxima", "Pons Seleccion Familiar Tradicional ekstra väärisoliiviõli 1L puudub Maximast"),
    (14449, "maxima", "Borges Extra Light oliiviõli 250ml puudub Maximast — LIGHT klass, peaks erinema extra_virgin'ist"),
    (14509, "coop", "Ekstra väärisoliiviõli metallpurk 1L (Filippo Berio) puudub Coopist — metallpakend"),
]


def _deduplicate_test_cases(test_cases):
    seen = set()
    unique = []
    for group_id, chain, description in test_cases:
        key = (group_id, chain.lower())
        if key in seen:
            print(f"HOIATUS: duplikaattest eemaldatud: group_id={group_id}, chain={chain} ({description})")
            continue
        seen.add(key)
        unique.append((group_id, chain, description))
    return unique


TEST_CASES = _deduplicate_test_cases(TEST_CASES)


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
                result = None
                for attempt in range(3):
                    try:
                        async with conn.transaction():
                            result = await get_or_create_substitution(
                                conn, group_id, chain, dry_run=True, use_cache=False
                            )
                    except Exception as e:
                        print(f"TEHNILINE VIGA (katse {attempt + 1}/3): {e}")
                        result = {
                            "decision_type": "test_error",
                            "error_type": type(e).__name__,
                            "reasoning": str(e),
                            "trace": {"original_group_id": group_id, "chain": chain},
                        }
                        break

                    if result is not None and result.get("decision_type") != "provider_error":
                        break
                    if attempt < 2:
                        print(f"provider_error (katse {attempt + 1}/3), proovin uuesti...")

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

    with open("dry_run_results_shadow_candidates.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print("\nTäisväljund salvestatud: dry_run_results_shadow_candidates.json")


if __name__ == "__main__":
    asyncio.run(run_dry_run_tests())

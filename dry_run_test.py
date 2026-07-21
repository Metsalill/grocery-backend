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

v2 muudatus (juuli 2026): TEST_CASES laiendatud 97 -> ~277 juhtumini,
katab 50 sub_code kategooriat (varasem 14). Uued juhtumid keskenduvad
edge-case'idele: rasvaprotsendid, maitsevariandid, lõiketüübid,
kofeiinivaba/laktoosivaba/suhkruvaba/alkoholivaba trait'id, taimne vs
loomne, koguse äärmused. Kandidaadid leitud reaalsest DB-st (gap-päring:
grupid millel on hind mõnes ketis, aga puudub teises).
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

    # ============================================================
    # LAIENDATUD VALIM (juuli 2026) — 50 kategooriat, käsitsi kureeritud
    # ~180 juhtumit, rõhk edge-case'idel: rasvaprotsendid, maitsevariandid,
    # lõiketüübid, kofeiinivaba/laktoosivaba trait'id, koguse äärmused
    # ============================================================

    # --- dairy_milk (fat_class_milk + flavour_state) ---
    (2591, "maxima", "Alma piim 2,5% 1L puudub Maximast"),
    (2594, "coop", "Alma täispiim 3,6-4,2% 0,5L puudub Coopist — väike pakend"),
    (2597, "coop", "Alma täispiim 3,6-4,2% 2L puudub Coopist — suur pakend"),
    (2611, "rimi", "Tere piim 2,5% D-vit 1L puudub Rimist"),
    (2658, "selver", "Kotimaista piim laktoosivaba 1,5% 1L puudub Selverist — lakt.vaba test"),
    (2659, "rimi", "Kotimaista piim laktoosivaba 3% 1L puudub Rimist — teine rasvaprotsent"),

    # --- dairy_yogurt_kefir (flavour_state + fat_class_yogurt + yogurt_form + flavour_variant downgrade) ---
    (3193, "maxima", "Alma maitsestamata jogurt 2,5% 1kg puudub Maximast"),
    (3198, "rimi", "Alma joogijogurt banaani-maasika 1kg puudub Rimist — yogurt_form=drinkable"),
    (3214, "coop", "Alma Kreeka jogurt maitsestamata 370g puudub Coopist — yogurt_form=greek"),
    (3233, "maxima", "Alma jogurt mandlitükid+šokolaad 150g puudub Maximast — mitme koostisosa test"),
    (3325, "rimi", "Tere joogijogurt metsmaasika 900g puudub Rimist"),
    (3343, "rimi", "Tere Emma maasikajogurt lakt.vaba 110g puudub Rimist — lakt.vaba + flavour_variant"),
    (3400, "coop", "Nopri hapupiim 2,5% 1L puudub Coopist"),

    # --- dairy_cream_sourcream ---
    (2692, "coop", "Alma hapukoor 20% 250g puudub Coopist"),
    (2696, "selver", "Alma kohvikoor 10% 380ml puudub Selverist — koor vs vahukoor eristus"),
    (2724, "maxima", "Tere vahukoor 35% lakt.vaba 200ml puudub Maximast"),
    (2741, "maxima", "Kotimaista kohvikoor lakt.vaba 200ml puudub Maximast"),

    # --- cheese_regular (cheese_type + cheese_form + cheese_modifier downgrade) ---
    (4390, "rimi", "Valio Atleet Cheddar 250g puudub Rimist"),
    (4454, "coop", "Mo Saaremaa Suitsutatud Kadaka riivjuust 200g puudub Coopist — form+modifier"),
    (5228, "maxima", "Rambyno BBQ-tšilli juustusnäkk 75g puudub Maximast — modifier test"),
    (5237, "maxima", "Mo Saaremaa juustuampsud tomatitega 200g puudub Maximast — modifier test"),

    # --- cheese_delicatessen ---
    (4705, "maxima", "President Brie 200g puudub Maximast"),
    (4746, "maxima", "Andri-Peedo v/hall.juust kitsepiima 120g puudub Maximast — kitsejuust"),
    (4769, "maxima", "Mauri Bontazola Gorgonzola DOP 200g puudub Maximast"),
    (4795, "selver", "Altenburger kreemjuust kitsepiimast 150g puudub Selverist"),

    # --- dairy_cheese_slices (cheese_form + cheese_modifier) ---
    (4167, "coop", "Valio sulatatud juust 185g puudub Coopist"),
    (4188, "rimi", "Zott Toasty Sandwich viilud 120g puudub Rimist"),
    (4886, "maxima", "Coop toorjuust maitsestamata 200g puudub Maximast"),

    # --- coffee_beans_ground / coffee_instant (caffeine_state) ---
    (25430, "coop", "Paulig Juhla Mokka kofeiinivaba 270g puudub Coopist — caffeine_state=decaf"),
    (25424, "rimi", "Paulig Brazil filtrikohv 500g puudub Rimist"),
    (28605, "coop", "Lavazza Qualita Oro purgis 250g puudub Coopist"),
    (29213, "rimi", "Xtra lahustuv kohv 200g puudub Rimist"),

    # --- tea (caffeine_state) ---
    (28249, "maxima", "Ahmad Earl Grey tee kofeiinivaba 20x2g puudub Maximast — caffeine_state=decaf"),
    (28383, "rimi", "Dilmah Earl Grey must tee 20x1,5g puudub Rimist"),

    # --- meat_beef_lamb_game (animal_type + cut_type) ---
    (11571, "rimi", "Liivimaa Mahe veise antrekoodi steik 240g puudub Rimist — cut_type=antrekoot"),
    (11577, "coop", "Liivimaa Mahe rohumaaveise romsteek 240g puudub Coopist — cut_type=romsteak"),
    (11591, "coop", "Karni antrekoodi viil 200g puudub Coopist"),
    (11612, "selver", "Linnamäe hirveliha steik 240g puudub Selverist — 'hirv' pole animal_type sõnastikus"),

    # --- meat_minced (animal_type) ---
    (11442, "selver", "Rakvere hakkliha sea-veiselihast 400g puudub Selverist — animal_type=mixed"),
    (11467, "rimi", "Liivimaa rohumaaveise proteiinihakkliha 300g puudub Rimist — animal_type=beef"),
    (11484, "rimi", "Kariniemen kalkunihakkliha 400g puudub Rimist — animal_type=poultry"),

    # --- meat_pork (animal_type + cut_type) ---
    (22666, "maxima", "Rakvere BBQ-marinaadis seasisefilee 700g puudub Maximast"),
    (22697, "rimi", "Rakvere seakaelakarbonaad keefirimarinaadis 800g puudub Rimist"),

    # --- meat_poultry (animal_type + plant-based edge case) ---
    (12941, "rimi", "Thormi taimne rebitud kanatu 200g puudub Rimist — TAIMNE toode, plant_based test"),
    (23401, "maxima", "Tallegg Fit BBQ rebitud kanafilee 300g puudub Maximast"),

    # --- meat_sausages (animal_type) ---
    (44137, "coop", "Rakvere Lihakas viiner 260g puudub Coopist"),
    (44173, "selver", "Tallegg kanaviiner 400g puudub Selverist — animal_type=poultry"),

    # --- meat_grill_blood_sausages ---
    (16141, "selver", "Linnamäe sibula-äädika šašlõkk 800g puudub Selverist"),
    (16326, "maxima", "Rakvere verivorst 500g puudub Maximast"),

    # --- fish_fresh / fish_processed / fish_salted_smoked (fish_species) ---
    (11910, "selver", "Coop lõhefilee 2x150g puudub Selverist — fish_species=salmon"),
    (11928, "selver", "Kotimaista tükeldatud vikerforell 180g puudub Selverist — fish_species=trout"),
    (23695, "maxima", "Kaluri heeringafilee tükid juurviljadega 400g puudub Maximast — fish_species=herring"),
    (23996, "coop", "Marwi käsitsi kooritud krevetid 300g puudub Coopist — fish_species=shrimp"),
    (24084, "maxima", "Vici krabimaitselised surimi pulgad 150g puudub Maximast — tundmatu liik edge case"),
    (12190, "maxima", "M.V.Wool graavilõhe viilutatud 200g puudub Maximast"),
    (12300, "maxima", "M.V.Wool külmsuitsulõhe viilutatud 200g puudub Maximast"),
    (12401, "selver", "Saare Hõbe soolaheeringafilee nahata 200g puudub Selverist"),

    # --- spices_herbs_spice_mix (flavour_profile downgrade) ---
    (11295, "maxima", "Santa Maria klassikaline kanamarinaad 75g puudub Maximast"),
    (11301, "coop", "Santa Maria magus tšillimarinaad 75g puudub Coopist — flavour_profile=sweet_chili"),
    (11302, "maxima", "Santa Maria teriyaki marinaad 75g puudub Maximast — flavour_profile=teriyaki"),

    # --- wine_* (AUTO_DISABLED kontroll) ---
    (29429, "rimi", "Le Grand Noir Blanc KGT vein 75cl puudub Rimist — vein ei tohi olla AUTO"),
    (29402, "maxima", "Bellecourt Cremant De Loire Brut Rose KPN vahuvein puudub Maximast"),
    (30462, "maxima", "Maschio Prosecco DOC Extra Dry 75cl puudub Maximast"),
    (29427, "coop", "Black Tower Spritz Mango Passion veinijook 75cl puudub Coopist"),
    (29611, "maxima", "Chapel Hill Pinot Grigio 75cl puudub Maximast"),
    (31477, "selver", "Roche Mazet Sauvignon Blanc 187ml puudub Selverist — väike pudel"),

    # --- spirits_other ---
    (32693, "selver", "Aperol 1L puudub Selverist"),

    # --- drinks_beer_cider (alkoholivaba edge case) ---
    (33976, "maxima", "A.Le Coq Lemon Spritz 330ml puudub Maximast"),
    (34155, "maxima", "Caribba Rum&Cola Cooler 275ml puudub Maximast"),
    (58429, "maxima", "A.Le Coq Virgin Mojito alkoholivaba 330ml puudub Maximast — alcohol_free trait"),

    # --- drinks_energy (flavour_variant downgrade + sugar_free) ---
    (17093, "selver", "Red Bull White Edition 250ml puudub Selverist"),
    (17096, "maxima", "Red Bull aprikoos-maasikas 250ml puudub Maximast — mitmikmaitse"),
    (17169, "coop", "NOCCO BCAA Pomelo 330ml puudub Coopist"),

    # --- drinks_juices / drinks_non_alcoholic / drinks_soft_soda ---
    (26718, "maxima", "Don Simon apelsinimahl 100% 2L puudub Maximast"),
    (26975, "selver", "Froosh mustika-vaarika smuuti 250ml puudub Selverist"),
    (27435, "maxima", "A.Le Coq Fassbrause Mojito alk.vaba 500ml puudub Maximast"),
    (34063, "rimi", "A.Le Coq Virgin Mojito alk.vaba 330ml puudub Rimist"),
    (27589, "selver", "Coca-Cola Zero Caffeine 330ml puudub Selverist — caffeine test soodal"),
    (27681, "maxima", "Fever Tree Indian Tonic Water 500ml puudub Maximast"),

    # --- dry_canned_veg / dry_soups_noodles ---
    (25976, "maxima", "Viibergi hapukurk 400g puudub Maximast"),
    (26088, "rimi", "Eesti And soolakurk küüslauguga 300g puudub Rimist"),
    (26211, "maxima", "Melissa Primo Gusto tomatipüree 500g puudub Maximast"),
    (21337, "coop", "Ajinomoto Oyakata ramen sealihamaitseline 63g puudub Coopist"),
    (21433, "coop", "Mama kiirnuudlid kanamaitselised 55g puudub Coopist"),
    (21551, "maxima", "Salvest 3min hernesupp 500g puudub Maximast"),

    # --- bakery_bread_loaves / bakery_cakes_pastries ---
    (5400, "maxima", "Leibur Mitmevilja röst 470g puudub Maximast"),
    (5502, "coop", "Eesti Pagar Pealinna Peenleib 1kg puudub Coopist"),
    (5668, "rimi", "Tera Suur rängik 375g klassikaline puudub Rimist"),
    (5892, "rimi", "Eesti Pagar Apelsini-šokolaadikeeks 300g puudub Rimist"),
    (6013, "coop", "Eesti Pagar Maasika-Toorjuustusaiake 425g puudub Coopist"),
    (6302, "maxima", "Mamma Kõrvitsa-kohupiimapannkoogid 400g puudub Maximast"),

    # --- sweets_biscuits_cookies / sweets_candies / sweets_chocolate_bars / sweets_nuts_driedfruit / sweets_snacks_salty ---
    (40984, "rimi", "Väike Väänik kaeraküpsis rosinatega 250g puudub Rimist"),
    (41073, "rimi", "Gullon Digestive küpsis 150g gluteenivaba puudub Rimist — gluten_free trait"),
    (38434, "rimi", "Haribo Mahlakarud kummikomm 160g puudub Rimist"),
    (38472, "maxima", "Trolli Wurrli kummikommid 100g puudub Maximast"),
    (40105, "maxima", "Kalev Anneke piimašokolaad 20g puudub Maximast"),
    (40711, "maxima", "M&M piimašokolaad drazeed 70g puudub Maximast"),
    (41896, "rimi", "Germund Premium Kreeka pähkel 200g puudub Rimist"),
    (42106, "maxima", "Pähklinäpp chia seemned 200g puudub Maximast"),
    (24168, "rimi", "MSDM kuivatatud India meretint 36g puudub Rimist"),
    (36159, "rimi", "Balsnack Texas popcorn soolaga 60g puudub Rimist"),

    # --- oils_olive (katmata kategooria fail-closed) ---
    (14447, "rimi", "Borges ekstra neitsioliiviõli fruity 500ml puudub Rimist — oils_olive katmata"),
    (14470, "selver", "Coop basiilikumaitsega oliiviõli 250ml puudub Selverist"),

    # --- pet_cat_wet ---
    (16892, "maxima", "Gourmet Gold kalkunipasteet 85g puudub Maximast"),
    (16926, "rimi", "Gourmet Perle Gravy Delight 85g puudub Rimist"),

    # --- produce_* ---
    (11398, "rimi", "Mahe pirn 500g puudub Rimist"),
    (11515, "rimi", "Coop maasikad 300g puudub Rimist"),
    (11538, "maxima", "Hele viinamari seemneteta Sweet Globe 500g puudub Maximast"),
    (21838, "rimi", "Well Done beebi spinat 100g puudub Rimist"),
    (21929, "selver", "Sprout King brokoli võrsed 30g puudub Selverist"),
    (21741, "coop", "Kirsstomat 250g puudub Coopist"),
    (22480, "coop", "Mahe kartul varajane 1kg puudub Coopist"),
    (14654, "rimi", "Sicilia sidrunimahl 115ml puudub Rimist"),
    (14694, "selver", "Herkku meeldivate marjade smuuti 750ml puudub Selverist"),
    (17699, "rimi", "Avokaado pakitud 700g puudub Rimist"),
    (17750, "maxima", "Nektariin mahe 500g puudub Maximast"),

    # --- baby_porridge_cereal ---
    (7649, "maxima", "Nogel Mahe raudne tatrapuder pirniga 190g puudub Maximast — beebitoit"),
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
                        result = await get_or_create_substitution(
                            conn, group_id, chain, dry_run=True, use_cache=False
                        )
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

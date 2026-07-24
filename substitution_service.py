"""
Seivy — asendustoodete teenus (v4.4, juuli 2026).

v4 muudatused (ChatGPT teine arvustus):
- dry_run parameeter: kui True, ei kutsuta _save()-i KUNAGI (KIHT 1
  kaitsest). Kuivtesti skript PEAB lisaks käivitama seda ka
  read-only DB transaktsiooni sees (KIHT 2) — kaks sõltumatut kaitset.
- Iga väljakutse tagastab "trace" välja täieliku otsustusahelaga
  (sql_candidate_count, quantity_eligible_count, trait_eligible_count,
  claude_candidate_count jne) monitooringu/veaotsingu jaoks.
- spices_broth_stock EEMALDATUD QUANTITY_RULES-ist — oli omavoliline
  lisandus, mitte teadlikult läbi vaadatud kategooria.

v4.1 muudatus (KeyError parandus, juuli 2026): trace'ile lisatud
puuduolev "save_path_reached" väli + database_write_attempted
loogika parandatud nii, et see on True AINULT reaalse DB-kirjutuse
korral (mitte iga kord kui salvestuskohani jõuti).

v4.2 muudatused (juuli 2026, Claude'i enda ülevaatus ChatGPT tagasiside
põhjal):
- CHEESE_MODIFIER_PATTERNS: ingliskeelsed märksõnad lisatud (wine,
  whisky, truffle jne) — Wyke Farms "White Wine Cheddar" vs "Ivy
  Vintage Cheddar" oleks varem mõlemad tagastanud frozenset() ja
  downgrade poleks rakendunud.
- _flavour_state: kasutab nüüd ka _flavour_variants sõnastikku, mitte
  ainult kitsast FLAVOR_KEYWORDS loendit.
- _product_identity_text(): uus abifunktsioon, ühendab canonical_name +
  sample_product_name + brand. Kasutatakse nüüd nii hard_check'ides kui
  downgrade_check'ides sample-nime asemel.
- AUTO_DISABLED_SUB_CODES laiendatud kogu alkoholile.
- BABY_FOOD_SUB_CODES laiendatud (baby_formula jne).

v4.3 muudatused (juuli 2026, dry-run 214-testi jooksu analüüs):
- FLAVOUR_VARIANT_PATTERNS: "pomelo" lisatud (greibi/pomelo/grapefruit).
  Kinnitatud false-AUTO dry-run'ist: NOCCO BCAA Pomelo.
- trace laiendatud kogusekihi läbipaistvusega: quantity_auto_count,
  quantity_suggested_count, quantity_incompatible_count,
  quantity_unknown_count, quantity_rule_found,
  quantity_rejection_reasons (missing_rule/missing_candidate_quantity/
  unit_mismatch/outside_allowed_range).
- get_or_create_substitution EI tagasta enam None tehnilise vea korral
  — tagastab struktureeritud {"decision_type": "provider_error", ...}
  koos kõigi enne erindit kogutud trace-väljadega.

v4.4 muudatus (juuli 2026, QUANTITY_RULES laiendus):
- DOWNGRADE_RULES laiendatud uutele maitsetundlikele kategooriatele
  (küpsised, kommid, šokolaad, pähklid/kuivatatud puuvili, soolased
  snäkid, kiirsupid/nuudlid, mahlad/smuutid, koogid/pirukad) —
  flavour_variant downgrade lisatud, kuna nende kategooriate
  IDENTITY_RULES ei kata toote tüüpi (erinevalt piimast/lihast/
  juustust, kus animal_type/cheese_type juba blokeerib). Kaasneb
  quantity_service.py QUANTITY_RULES laiendusega samade sub_code'ide
  jaoks (vt seal SUBSTITUTION_RULES_VERSION tõus 1 -> 2).

v4.5 muudatused (juuli 2026, 214-testi v4.4 jooksu manuaalne audit +
ChatGPT sõltumatu ülevaatus — 8 kinnitatud false-AUTO juhtumit):
- IDENTITY_RULES UUS hard-check: sweets_nuts_driedfruit (nut_seed_type
  — kreeka pähkel != metspähkel, need on erinevad toidud, mitte
  maitsevariandid), drinks_non_alcoholic (beverage_type — alkoholivaba
  õlu/siider/mocktail/Fassbrause on erinevad tootetüübid, mitte
  omavahel asendatavad lihtsalt sellepärast, et mõlemad on alkoholi-
  vabad), meat_sausages ja meat_grill_blood_sausages (animal_type +
  meat_form — verivorst != šašlõkk, kana != sea).
- DOWNGRADE_RULES UUS: tea (flavour_variant — Earl Grey != mustsõstar,
  muster oli juba olemas FLAVOUR_VARIANT_PATTERNS'is, ainult tea polnud
  DOWNGRADE_RULES's registreeritud), oils_olive (flavour_profile +
  oil_grade — basiilikumaitse pole "kvaliteetne asendus", rafineeritud
  != ekstra vääris; MÄRKUS: "ekstra vääris" ja "ekstra neitsi" on SAMA
  EL-i kategooria (extra virgin), ametlik eestikeelne termin muutus
  2022 — _oil_grade normaliseerib need mõlemad "extra_virgin" alla,
  et vältida valet downgrade't), meat_minced (protein_enriched —
  proteiinihakkliha != tavahakk, sama muster mis Wyke Farms cheddar),
  bakery_bread_loaves (grain_type — täistera/rukki/mitmevilja erinevus
  pole "sama kategooria röstleib"), coffee_beans_ground + coffee_instant
  (coffee_brew_form — In-Cup != presskann, jahvatusaste erineb).
- Kaasneb quantity_service.py QUANTITY_RULES laiendusega 5 uue
  lihakategooria jaoks (vt seal, SUBSTITUTION_RULES_VERSION 2 -> 3).

v4.5.1 muudatused (juuli 2026, ChatGPT sõltumatu koodiülevaatus v4.5
peal, KÕIK kolm leidu kinnitatud otse koodist enne parandamist):
- unit_mismatch trace oli katki: QuantityTier.INCOMPATIBLE loeti ALATI
  "outside_allowed_range" alla, ka siis kui tegu oli päris baasühiku-
  mittevastavusega (g vs ml). Lisatud QuantityRejectionReason enum +
  QuantityMatch.rejection_reason väli (quantity_service.py), mis
  kannab põhjuse otse edasi.
- animal_type kasutas puhast substring-kontrolli ("sea" in text) —
  asendatud \\b sõnapiiriga regex-mustritega. Kana ja kalkun olid
  varem KOKKU "poultry" all (kalkunihakkliha oleks läbinud hard-check'i
  broilerihakkliha vastu) — eraldatud chicken/turkey/duck/
  generic_poultry'ks. Lisatud "deer".
- AUTO_DISABLED_SUB_CODES laiendatud ajutiselt: tea, sweets_candies,
  drinks_non_alcoholic — nende olemasolev tüübisõnastik ei kata
  piisavalt reaalset variatsiooni (bränditud kombinimed, tee TÜÜP
  mitte ainult puuviljamaitse, alkoholivaba joogi tüübi ühesuunaline
  kontroll). Eemaldatavad alles pärast täielikumat identiteedimudelit.

v4.5.2 muudatused (ChatGPT teine ülevaatus, samad kolm leidu edasi
laiendatud): meat_form 4 -> 10 vormi (ribi, steik, toorvorst,
praevorst, verikäkk, marineeritud lõige lisatud). fish_species:
heeringas ja räim eraldi liigid (varem samas "herring" kategoorias) +
parandatud pre-existing viga, kus "heeringas" ei tabanud käändevorme
nagu "heeringafilee" (tüvi "heering" kasutusele).

v4.5.3 muudatused (ChatGPT kolmas ülevaatus):
- SUBSTITUTION_RULES_VERSION tõstetud 3 -> 4 (vt quantity_service.py),
  kuna v4.5.1/v4.5.2 muutsid otsust mõjutavat loogikat pärast esialgset
  2->3 tõusu, aga versiooninumbrit ei uuendatud — vana cache oleks
  jäänud kehtima kuni TTL lõpuni.
- QuantityRejectionReason.UNKNOWN_UNIT lisatud, eraldatud
  UNIT_MISMATCH'ist: UNIT_MISMATCH = kaks TUVASTATUD baasühikut, mis
  erinevad (g vs ml, INCOMPATIBLE tier). UNKNOWN_UNIT = net_unit
  väärtus ise on ebaselge/parsimatu (nt "pack" ilma tükiarvuta,
  UNKNOWN tier) — need läksid varem sama "unit_mismatch" trace-võtme
  alla, kuigi tegu on kahe erineva andmeprobleemiga.

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
    get_rules_for_sub_code,
    QuantityTier,
    QuantityRejectionReason,
    SUBSTITUTION_RULES_VERSION,
)

logger = logging.getLogger("substitution_service")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
API_TIMEOUT_SECONDS = 6.0

MAX_SEMANTIC_CANDIDATES = 8
# Turvapiir (mitte reaalne valikupiir!) — kaitseb ainult haruldase
# extreemselt suure kategooria eest (nt tuhandeid tooteid). ChatGPT
# leid (juuli 2026): kui see oli varem 150 ja rakendus ENNE Python
# koguse-lähedus-sortimist, jäi parim kandidaat mõnikord valimist
# välja. Nüüd on SQL-i ORDER BY ainult deterministlik (pg.id, p.id),
# TEGELIK lähedus-sortimine toimub Pythonis KÕIGI toodud kandidaatide
# peal, alles siis kärbitakse Claude'i jaoks (MAX_SEMANTIC_CANDIDATES).
CANDIDATE_POOL_LIMIT = 2000

_TTL_BY_DECISION = {
    "auto_substitute": timedelta(days=7),
    "suggested_substitute": timedelta(days=2),
    "no_quantity_data": timedelta(days=1),
    "no_eligible_candidates": timedelta(days=1),
    "semantic_rejected": timedelta(days=1),
}

REQUIRED_TRAITS: dict[str, tuple[str, ...]] = {
    # "lakt.vaba" ja "lakt vaba" LISATUD (juuli 2026) — leitud reaalsetest
    # andmetest (nt "Farmi koogikoor 15% lakt.vaba"), mis oleks muidu
    # libisenud läbi laktoosivaba kaitsest, kuna esialgne regex otsis
    # ainult täissõna "laktoosivaba".
    "lactose_free": (
        "laktoosivaba", "lactose free", "lactose-free",
        "lakt.vaba", "lakt. vaba", "lakt vaba",
    ),
    "gluten_free": ("gluteenivaba", "gluten free", "gluten-free"),
    "alcohol_free": ("alkoholivaba", "alcohol free", "alcohol-free"),
    # LISATUD (juuli 2026) — leitud reaalsetest andmetest (Red Bull
    # "Suhkruvaba"). Sama põhimõte: kui originaal on suhkruvaba,
    # kandidaat PEAB olema ka.
    "sugar_free": ("suhkruvaba", "sugar free", "sugar-free"),
}

import re

IDENTITY_TRAITS: dict[str, tuple[str, ...]] = {
    "plant_based": ("taimne", "vegan"),
}

# ---------------- kategooriapõhised identity-kontrollid ----------------
#
# ChatGPT arhitektuur (juuli 2026, kolmas ülevaatus): mitte üks globaalne
# regex-plokk, vaid kategooriapõhine profiil. Iga check-funktsioon
# tuvastab tekstist ühe omaduse väärtuse (või None, kui ei leitud).
# _traits_compatible() nõuab, et originaali ja kandidaadi väärtus
# klapiks AINULT nende check'ide jaoks, mis on IDENTITY_RULES's selle
# sub_code kohta loetletud — nii ei rakendu nt piima rasvareegel
# kogemata jogurtile või lihale.
#
# Reegel iga check'i kohta: kui originaali väärtus on teada, PEAB
# kandidaadi väärtus olema teada JA sama (fail-closed). Kui originaali
# väärtus pole tuvastatav, check ei blokeeri (jääb Claude'i hooleks).

FLAVOR_KEYWORDS = (
    "cappuccino", "latte", "šokolaadi", "shokolaadi", "vanilje",
    "karamelli", "maasika", "banaani", "kookos",
)


def _flavour_state(text) -> Optional[str]:
    """Maitsestatud vs maitsestamata. Leitud reaalse vea põhjal (juuli
    2026): Cappuccino/Latte piim asendati vääralt tavalise piimaga.

    v4.2: kasutab nüüd LISAKS ka _flavour_variants sõnastikku (mets-
    maasika/kirsi/banaani/virsiku jne), mitte ainult kitsast
    FLAVOR_KEYWORDS loendit."""
    if not text:
        return None
    text_lower = text.lower()
    if any(kw in text_lower for kw in FLAVOR_KEYWORDS):
        return "flavored"
    if _flavour_variants(text_lower):
        return "flavored"
    return "plain"


_FAT_RANGE_RE = re.compile(r"(\d+[.,]?\d*)\s*-\s*(\d+[.,]?\d*)\s*%")
_FAT_SINGLE_RE = re.compile(r"(\d+[.,]?\d*)\s*%")


def _extract_percent(text) -> Optional[float]:
    """Jagatud abifunktsioon protsendi eraldamiseks tekstist (nt '3,6-4,2%' -> 3.9)."""
    if not text:
        return None
    normalized = text.replace(",", ".")
    m = _FAT_RANGE_RE.search(normalized)
    if m:
        try:
            return (float(m.group(1)) + float(m.group(2))) / 2
        except ValueError:
            return None
    m = _FAT_SINGLE_RE.search(normalized)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _milk_fat_class(text) -> Optional[str]:
    """Piima rasvaprotsendi kategooria. Leitud reaalse vea põhjal: sama
    originaaltoode sai kahes ketis vastandliku otsuse (Coop lubas,
    Selver keeldus samast kandidaadist), kuni see kontroll lisati.
    EI kasutata maitsestatud jookide peal (vt IDENTITY_RULES allpool —
    fat_class_milk on rakendatud ainult koos flavour_state kontrolliga,
    mis juba eristab need eraldi)."""
    pct = _extract_percent(text)
    if pct is None:
        return None
    if pct >= 3.2:
        return "whole"
    if pct >= 2.0:
        return "standard"
    if pct >= 0.5:
        return "low_fat"
    return "fat_free"


def _yogurt_fat_class(text) -> Optional[str]:
    """Jogurti rasvaprotsendi kategooria — laiemad vahemikud kui piimal,
    kuna Kreeka jogurt (10%+) on tavaline. ChatGPT proaktiivne audit
    (juuli 2026): '10% Kreeka jogurt -> 0% joogijogurt' ei tohi olla
    AUTO — see kontroll väldib seda."""
    pct = _extract_percent(text)
    if pct is None:
        return None
    if pct >= 6.0:
        return "greek_high_fat"
    if pct >= 2.0:
        return "standard"
    if pct >= 0.5:
        return "low_fat"
    return "fat_free"


def _yogurt_form(text) -> Optional[str]:
    """Joogijogurt vs lusikaga söödav vs Kreeka tüüpi. ChatGPT:
    'proteiinijogurt -> tavaline jogurt' ei tohi olla AUTO."""
    if not text:
        return None
    text_lower = text.lower()
    if any(kw in text_lower for kw in ("joogijogurt", "joogi jogurt", "drinking yogurt")):
        return "drinkable"
    if any(kw in text_lower for kw in ("kreeka", "greek")):
        return "greek"
    if any(kw in text_lower for kw in ("proteiini", "protein")):
        return "protein"
    return "regular"


CHEESE_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "gouda": ("gouda",),
    "cheddar": ("cheddar",),
    "mozzarella": ("mozzarella",),
    "feta": ("feta",),
    "halloumi": ("halloumi",),
    "parmesan": ("parmesan",),
    "brie": ("brie",),
    "maasdam": ("maasdam",),
    "suluguni": ("suluguni",),
    "kohupiima": ("kohupiim",),
}


def _cheese_type(text) -> Optional[str]:
    """Juustu tüüp. ChatGPT: 'mozzarella -> Gouda' ei tohi olla AUTO."""
    if not text:
        return None
    text_lower = text.lower()
    for cheese, keywords in CHEESE_TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return cheese
    return None


def _cheese_form(text) -> Optional[str]:
    """Juustu vorm (viilutatud/riivitud/plokk/määrdejuust). ChatGPT:
    'riivjuust -> juustuplokk' ei tohi olla AUTO."""
    if not text:
        return None
    text_lower = text.lower()
    if any(kw in text_lower for kw in ("riivitud", "riiv")):
        return "grated"
    if any(kw in text_lower for kw in ("viil", "sliced", "viilutatud")):
        return "sliced"
    if any(kw in text_lower for kw in ("määrde", "maarde", "spread")):
        return "spread"
    return "block"  # vaikimisi plokk, kui ükski erivorm pole mainitud


FISH_SPECIES_KEYWORDS: dict[str, tuple[str, ...]] = {
    "salmon": ("lõhe", "lohe", "salmon"),
    "cod": ("tursk", "cod"),
    # v4.5.2 (ChatGPT leid): heeringas ja räim on lähiliigid, aga mitte
    # sama toode/toit Eesti kaubandustavas — lahutatud eraldi
    # identiteetideks, et vältida vale AUTO-t nende vahel. "heering"
    # (mitte "heeringas") kui tüvi, et tabada ka käändevorme
    # (heeringafilee, heeringast jne — leitud testimisel, pre-existing
    # bug juba v4.2 algsest nimekirjast).
    "herring": ("heering",),
    "baltic_herring": ("räim", "raim"),
    "trout": ("forell", "trout"),
    "pike": ("haug", "pike"),
    "tuna": ("tuunikala", "tuna"),
    "shrimp": ("krevet", "shrimp"),
}


def _fish_species(text) -> Optional[str]:
    """Kalaliik. ChatGPT proaktiivne audit."""
    if not text:
        return None
    text_lower = text.lower()
    for species, keywords in FISH_SPECIES_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return species
    return None


# v4.5.1 (ChatGPT leid): endine ANIMAL_TYPE_KEYWORDS kasutas puhast
# substring-kontrolli ("sea" in text_lower), mis võib teoreetiliselt
# tabada ka ingliskeelseid sõnu nagu "seasoned"/"sea salt" — regex koos
# \b sõnapiiridega on turvalisem. Kana ja kalkun olid varem KOKKU
# "poultry" all — see tähendas, et "kalkunihakkliha" sai vääralt
# hard-check'i läbida "broilerihakkliha" vastu. Eraldatud: chicken/
# turkey/duck/generic_poultry (viimane, kui liik pole täpsustatud).
# Lisatud "deer" (hirv, esines juba reaalses testis).
ANIMAL_TYPE_PATTERNS: dict[str, tuple[str, ...]] = {
    "mixed": (r"\bsea[\s-]?veise\w*", r"\bveise[\s-]?sea\w*"),
    "beef": (r"\bveise\w*", r"\bhärjaliha\w*", r"\bbeef\b"),
    "pork": (
        r"\bsea(?:liha|hakk|filee|kaela|karbonaad|sisefilee|välisfilee|vorst|kõrvad?)\w*",
        r"\bpork\b",
    ),
    "chicken": (r"\bkana\w*", r"\bbroileri\w*", r"\bchicken\b"),
    "turkey": (r"\bkalkuni?\w*", r"\bturkey\b"),
    "duck": (r"\bpardi\w*", r"\bduck\b"),
    "generic_poultry": (r"\blinnuliha\w*", r"\bpoultry\b"),
    "lamb": (r"\blamba\w*", r"\blambaliha\w*", r"\blamb\b"),
    "deer": (r"\bhirve\w*", r"\bdeer\b", r"\bvenison\b"),
}


def _animal_type(text) -> Optional[str]:
    """Lihaliik (veis/siga/kana/kalkun/part/lammas/hirv/segu). ChatGPT
    näide: 'veiseliha hakkliha 5%' ei tohi asenduda 'sea-veise hakkliha
    20%'-ga lihtsalt kaalu klappimise tõttu. v4.5.1: kana ja kalkun
    eraldi (varem sama 'poultry' kategooria alla kokku pandud)."""
    if not text:
        return None
    text_lower = text.lower()
    for pattern in ANIMAL_TYPE_PATTERNS["mixed"]:
        if re.search(pattern, text_lower):
            return "mixed"
    for animal, patterns in ANIMAL_TYPE_PATTERNS.items():
        if animal == "mixed":
            continue
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return animal
    return None


def _caffeine_state(text) -> Optional[str]:
    """Kofeiiniga vs kofeiinivaba (kohv/tee/joogid)."""
    if not text:
        return None
    text_lower = text.lower()
    if any(kw in text_lower for kw in ("kofeiinivaba", "decaf", "koffeinfri")):
        return "decaf"
    return None  # "kofeiiniga" pole tavaliselt eraldi märgitud, jääb tuvastamata


# Lihalõike tüüp — DETERMINISTLIK. Leitud reaalse vea põhjal (juuli
# 2026): sama originaaltoode (Rohumaaveise antrekoodi steik) sai
# vastandliku otsuse kahes ketis — Maxima kiitis heaks asenduse teise
# lõikega ("erinevus on vaid lõikamisviisis"), Rimi lükkas SAMA
# põhjendusega tagasi ("ei ole sama tüüp"). Täpselt sama muster mis
# piima rasvaprotsendi puhul — vajab deterministlikku kontrolli.
CUT_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "ground": ("hakkliha", "burgeripihv", "klops"),
    "cubes": ("kuubikud", "lõiked", "loiked", "tükid", "tukid"),
    "antrekoot": ("antrekoodi", "antrekoot"),
    "picanha": ("picanha",),
    "fillet": ("valisfilee", "filee"),
    "romsteak": ("romsteek", "romsteegi"),
    "grillsteik": ("grillsteik",),
    "minute_steak": ("minutisteik",),
    "karbonaad": ("karbonaad",),
}


def _meat_cut_type(text) -> Optional[str]:
    if not text:
        return None
    text_lower = text.lower()
    for cut, keywords in CUT_TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return cut
    return None  # tundmatu lõige — ei blokeeri, jääb Claude'i hinnata


# v4.5 UUS — Germund Kreeka pähkel -> metspähkel false-AUTO fix.
# Pähkli/seemne LIIK on erinev toit, mitte maitsevariant — seega
# hard_match (IDENTITY_RULES), mitte downgrade.
NUT_SEED_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "walnut": ("kreeka pähk", "kreeka-pähk", "walnut"),
    "hazelnut": ("sarapuu", "metsapähk", "metspähk", "hazelnut"),
    "almond": ("mandli", "mandel", "almond"),
    "cashew": ("kašu", "cashew"),
    "pistachio": ("pistaatsia", "pistachio"),
    "peanut": ("maapähk", "peanut"),
    "chia": ("chia",),
    "sunflower_seed": ("päevalille", "sunflower"),
    "pumpkin_seed": ("kõrvitsaseemne", "kõrvitsaseeme", "pumpkin seed"),
}


def _nut_seed_type(text) -> Optional[str]:
    if not text:
        return None
    text_lower = text.lower()
    for nut, keywords in NUT_SEED_TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return nut
    return None  # tundmatu pähkel/seeme — ei blokeeri, jääb Claude'i hinnata


# v4.5 UUS — Virgin Mojito -> Corona Cero / Fassbrause Mojito ->
# "alkoholivaba õlu" false-AUTO fix. Kõik on tehniliselt alkoholivabad,
# aga õlu/siider/mocktail/Fassbrause EI ole omavahel asendatavad
# tootetüübid. Parim jõupingutus (mitte täielik kaubamärgi-loend) —
# vt v4.5 changelog docstringis, laiendatakse vajadusel järgmise
# dry-run analüüsi põhjal.
BEVERAGE_NONALC_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "beer_style": ("õlu", "beer", "cero", "pils", "lager"),
    "cider_style": ("siider", "cider"),
    "mocktail": ("mocktail", "virgin", "kokteil"),
    "fassbrause": ("fassbrause",),
}


def _beverage_type_nonalc(text) -> Optional[str]:
    if not text:
        return None
    text_lower = text.lower()
    for bev_type, keywords in BEVERAGE_NONALC_TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return bev_type
    return None  # tuvastamata tüüp — ei blokeeri, jääb Claude'i hinnata


# v4.5 UUS — Rakvere verivorst vs Linnamäe šašlõkk on ühes sub_code'is
# (meat_grill_blood_sausages), aga täiesti erinevad tooted.
# v4.5.2 LAIENDUS (ChatGPT leid): esialgne 4-vormiline nimekiri ei
# katnud ribisid/steike/toor-praevorsti/verikäkki/marineeritud lõikeid,
# mis kõik esinevad samas sub_code'is — laiendatud, endiselt teadlikult
# mitte-ammendav (tundmatu vorm ei blokeeri, jääb Claude'i hooleks).
MEAT_FORM_KEYWORDS: dict[str, tuple[str, ...]] = {
    "blood_sausage": ("verivorst",),
    "blood_dumpling": ("verikäkk", "verikook"),
    "shashlik": ("šašlõkk", "sašlõkk", "saslõkk", "shashlik"),
    "grill_sausage": ("grillvorst", "grill vorst"),
    "raw_sausage": ("toorvorst",),
    "fried_sausage": ("praevorst",),
    "kebab": ("kebab",),
    "ribs": ("ribi", "ribid", "ribiliha", "spare rib"),
    "steak": ("steik", "steek"),
    "marinated_cut": ("marineeritud", "marinaadis"),
}


def _meat_form(text) -> Optional[str]:
    if not text:
        return None
    text_lower = text.lower()
    for form, keywords in MEAT_FORM_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return form
    return None  # tuvastamata vorm — ei blokeeri, jääb Claude'i hinnata


# --- DOWNGRADE check'id: erinevus EI eemalda kandidaati, vaid
# langetab tier'i AUTO'lt SUGGESTED'ile. ChatGPT (viies ülevaatus):
# kõik downgrade check'id tagastavad frozenset (MITTE üksik väärtus —
# esimene versioon tagastas ainult esimese leitud sõna ja lõikas
# valesti, nt "banaani-maasika" ja "maasika-pohla" oleksid mõlemad
# tagastanud "strawberry", kuna funktsioon peatus esimesel leitud
# sõnal). Võrdlus on SÜMMEETRILINE: kui kummalgi poolel on väärtusi
# JA hulgad erinevad, langetatakse tier — mitte ainult siis, kui
# originaalil on väärtus.

FLAVOUR_PROFILE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "classic": (r"\bklassikali\w*",),
    "sweet_chili": (r"\btšilli\w*", r"\btsilli\w*", r"\bchili\w*"),
    "teriyaki": (r"\bteriyaki\b",),
    "bbq": (r"\bbbq\b",),
    "garlic": (r"\bküüslaugu\w*", r"\bkuusklaugu\w*"),
    "lemon_herb": (r"\bsidruni\w*", r"\bürdi\w*", r"\burdi\w*"),
    "smoky": (r"\bsuitsu\w*",),
    "spicy": (r"\bterav\w*", r"\bvürtsika\w*", r"\bvurtsika\w*"),
    "mild": (r"\bmahe\b",),
    # v4.5 UUS — Borges basiilikuõli false-AUTO fix.
    "basil": (r"\bbasiiliku\w*", r"\bbasil\w*"),
    # v4.5 UUS — Balsnack soolane->juustumaitseline popcorn false-AUTO
    # fix. sweets_snacks_salty kasutas v4.4-s ainult flavour_variant't
    # (puuviljamaitsed), mis ei kata soolaste snäkkide päris-maitseid.
    "salted": (r"\bsoola\w*",),
    "cheese": (r"\bjuustu\w*",),
    "paprika": (r"\bpaprika\w*",),
    "sour_cream_onion": (r"\bhapukoore\w*",),
    # v4.5.4 UUS — Borges "Fruity" vs "Original" false-AUTO fix. Claude
    # enda reasoning nimetas seda "maitsevariandiks" — konservatiivne
    # käitumine on downgrade't rakendada, isegi kui "puuviljane" on
    # mõnikord lihtsalt EVOO sensoorne kvaliteedikirjeldus, mitte
    # lisatud maitseaine (vt v4.5 changelog Borges vääris/neitsi
    # täpsustust — SEE on erinev juhtum, kuna "fruity" on eraldi
    # tooteseeria nimi, "vääris"/"neitsi" on sama EL-i kvaliteediklass).
    "fruity": (r"\bfruity\b", r"\bpuuviljane\w*", r"\bpuuvilja\w*"),
    # v4.6.2 UUS — Ajinomoto/Oyakata ramen "sealihamaitseline" vs
    # "kanamaitseline" false-AUTO fix (dry_soups_noodles). Claude enda
    # reasoning tunnistas "erinevus on ainult maitses (kana vs sealiha)"
    # — FLAVOUR_VARIANT_PATTERNS kattis ainult puuviljamaitseid, mitte
    # kiirnuudlite/suppide liha-/puljongimaitseid.
    "chicken_flavour": (r"\bkana\w*", r"\bchicken\b"),
    "pork_flavour": (r"\bsealiha\w*", r"\bpork\b"),
    "beef_flavour": (r"\bveise\w*", r"\bbeef\b"),
    "shrimp_flavour": (r"\bkrevet\w*", r"\bshrimp\b"),
    "miso_flavour": (r"\bmiso\w*",),
    "curry_flavour": (r"\bkarri\w*", r"\bcurry\b"),
}

# Maitsevariandid — regex-mustrid, MITTE lihtsad substring'id, et
# vältida valesid tabamusi liitsõnades (nt "maasika" ei tohi tabada
# "metsmaasika" seest — negative lookbehind väldib seda).
#
# v4.3: pomelo/greibi/grapefruit lisatud — kinnitatud false-AUTO fix.
FLAVOUR_VARIANT_PATTERNS: dict[str, tuple[str, ...]] = {
    "wild_strawberry": (r"\bmetsmaasika\w*",),
    "strawberry": (r"(?<!mets)\bmaasika\w*",),
    "banana": (r"\bbanaani\w*",),
    "blueberry": (r"\bmustika\w*",),
    "peach": (r"\bvirsiku\w*",),
    "apricot": (r"\baprikoosi\w*",),
    "mango": (r"\bmango\w*",),
    "cherry": (r"\bkirsi\w*",),
    "orange": (r"\bapelsini\w*",),
    "lemon": (r"\bsidruni\w*",),
    "raspberry": (r"\bvaarika\w*",),
    "pear": (r"\bpirni\w*",),
    "coconut": (r"\bkookos\w*",),
    "vanilla": (r"\bvanilje\w*",),
    "chocolate": (r"\bšokolaadi\w*", r"\bshokolaadi\w*"),
    "caramel": (r"\bkaramelli\w*",),
    "passion_fruit": (r"\bpassiooni\w*",),
    "kiwi": (r"\bkiivi\w*",),
    "rhubarb": (r"\brabarberi\w*",),
    "pohla": (r"\bpohla\w*",),
    "blackcurrant": (r"\bmustsõstra\w*", r"\bmustsostra\w*"),
    "redcurrant": (r"\bpunas[eõ]stra\w*", r"\bpunase\s+sõstra\w*", r"\bredcurrant\w*"),
    "forest_berries": (r"\bmetsamarja\w*",),
    "apple": (r"\bõuna\w*", r"\bouna\w*"),
    "pineapple": (r"\banan[ae]ssi\w*", r"\bananasi\w*", r"\bpineapple\w*"),
    "grape": (r"\bviinamarja\w*",),
    "watermelon": (r"\barbuusi\w*",),
    "tropical": (r"\btroopili\w*",),
    "pomelo": (r"\bpomelo\w*", r"\bgreibi\w*", r"\bgrapefruit\w*"),
}

# Juustu modifikaatorid — ChatGPT viies ülevaatus: "Kadaka", "viskiga"
# jms ei ole hard_match (ei eemalda kandidaati), vaid downgrade.
#
# v4.2: ingliskeelsed märksõnad lisatud (Wyke Farms wine/whisky fix).
CHEESE_MODIFIER_PATTERNS: dict[str, tuple[str, ...]] = {
    "whisky": (r"\bviski\w*", r"\bwhisky\b", r"\bwhiskey\b"),
    "truffle": (r"\btrühvli\w*", r"\btruffel\w*", r"\btruffle\w*"),
    "juniper": (r"\bkadaka\w*", r"\bjuniper\w*"),
    "jalapeno": (r"\bjalape[nñ]o\w*", r"\btšilli\w*", r"\btsilli\w*", r"\bchili\w*"),
    "smoked": (r"\bsuitsu\w*", r"\bsmoked\b"),
    "herbs": (r"\bürdi\w*", r"\burdi\w*", r"\bherb\w*"),
    "garlic": (r"\bküüslaugu\w*", r"\bkuusklaugu\w*", r"\bgarlic\w*"),
    "pepper": (r"\bpipra\w*", r"\bpepper\w*"),
    "walnut": (r"\bpähkli\w*", r"\bpahkli\w*", r"\bwalnut\w*"),
    "caraway": (r"\bköömne\w*", r"\bkoomne\w*", r"\bcaraway\w*"),
    "wine": (r"\bveini\w*", r"\bportveini\w*", r"\bwine\b", r"\bport wine\b"),
}


def _match_variants(text, patterns: dict[str, tuple[str, ...]]) -> frozenset:
    """Tagastab KÕIK sobivad variandid (mitte ainult esimese)."""
    if not text:
        return frozenset()
    text_lower = text.lower()
    found = set()
    for variant, regex_list in patterns.items():
        for pattern in regex_list:
            if re.search(pattern, text_lower):
                found.add(variant)
                break
    return frozenset(found)


def _flavour_profile_set(text) -> frozenset:
    return _match_variants(text, {k: v for k, v in FLAVOUR_PROFILE_KEYWORDS.items()})


def _flavour_variants(text) -> frozenset:
    return _match_variants(text, FLAVOUR_VARIANT_PATTERNS)


def _cheese_modifiers(text) -> frozenset:
    return _match_variants(text, CHEESE_MODIFIER_PATTERNS)


# v4.5 UUS — Borges "ekstra vääris" vs "ekstra neitsi" pole kaks
# erinevat klassi, vaid SAMA EL-i kategooria (extra virgin olive oil).
# Ametlik eestikeelne termin muutus "neitsioliiviõli" -> "väärisoliiviõli"
# 2022. aastal (Maaeluministeerium/EL turustusstandard). Seetõttu
# normaliseeritakse mõlemad "extra_virgin" alla — need EI tohi
# downgrade't vallandada. Rafineeritud/jääkõli/kerge on tegelikult
# madalam klass ja PEAVAD downgrade'ima.
def _oil_grade(text) -> frozenset:
    if not text:
        return frozenset()
    t = text.lower()
    if re.search(r"\bekstra[\s-]*v[aä]äris\w*", t) or re.search(r"\bekstra[\s-]*neitsi\w*", t) \
            or re.search(r"\bextra\s+virgin\b", t):
        return frozenset({"extra_virgin"})
    if re.search(r"\bv[aä]äris\w*", t) or re.search(r"\bneitsi\w*", t):
        return frozenset({"virgin"})
    if re.search(r"\brafineeritud\w*", t) or re.search(r"\brefined\b", t):
        return frozenset({"refined"})
    if re.search(r"\bj[aä][aä]kõli\w*", t) or re.search(r"\bpomace\b", t):
        return frozenset({"pomace"})
    if re.search(r"\bkerge\w*", t) or re.search(r"\blight\b", t):
        return frozenset({"light"})
    return frozenset()


# v4.5 UUS — Liivimaa "proteiinihakkliha" -> tavahakk false-AUTO fix
# (sama muster mis Wyke Farms wine cheddar). Sümmeetriline downgrade:
# kui KUMMALGI poolel on "proteiini" märgitud ja teisel pole, langeb
# tier AUTO'lt SUGGESTED'ile.
def _protein_enriched(text) -> frozenset:
    if not text:
        return frozenset()
    if re.search(r"\bproteiini\w*", text.lower()) or re.search(r"\bprotein\w*", text.lower()):
        return frozenset({"protein_enriched"})
    return frozenset()


# v4.5 UUS — leiva teraviljakoostise erinevus (rukis/kaer/nisu/täistera/
# mitmevili) ignoreeriti seni täielikult ("5-vilja röst" -> "mitmevilja
# röst" sai vääralt AUTO).
GRAIN_TYPE_PATTERNS: dict[str, tuple[str, ...]] = {
    "rye": (r"\brukki\w*",),
    "wheat": (r"\bnisu\w*",),
    "oat": (r"\bkaera\w*",),
    "barley": (r"\bodra\w*",),
    "spelt": (r"\bspelta\w*",),
    # "tõistera" lisatud (juuli 2026) — leitud reaalsest testandmestikust,
    # ilmselt kodeeringu/andmesisestuse viga ("täistera" asemel), aga
    # kuna see esineb päris tootenimedes, tuleb see ka ära tunda.
    "wholegrain": (r"\bt[äõa]istera\w*",),
    "multigrain": (r"\bmitmevilja\w*", r"\b\d+[\s-]?vilja\w*"),
}


def _grain_type(text) -> frozenset:
    return _match_variants(text, GRAIN_TYPE_PATTERNS)


# v4.5 UUS — Merrild In-Cup -> presskann false-AUTO fix. Jahvatusaste/
# valmistusvorm erineb (in-cup on peenem jahvatus kui presskann),
# seega ei tohi olla AUTO isegi kui bränd ja kogus klapivad.
COFFEE_BREW_FORM_PATTERNS: dict[str, tuple[str, ...]] = {
    "beans": (r"\buba\w*", r"\bbeans?\b"),
    "filter_ground": (r"\bfiltrikohv\w*", r"\bfilter\b"),
    "in_cup": (r"\bin[\s-]?cup\w*", r"\btassikohv\w*"),
    "french_press": (r"\bpresskann\w*", r"\bfrench\s+press\b"),
    "espresso_ground": (r"\bespresso\w*",),
    "instant": (r"\blahustuv\w*", r"\binstant\w*"),
    "capsule": (r"\bkapsli\w*", r"\bcapsule\w*"),
}


def _coffee_brew_form(text) -> frozenset:
    return _match_variants(text, COFFEE_BREW_FORM_PATTERNS)


# v4.5.4 UUS — Lavazza "Qualita Oro" vs "Mountain Grown" false-AUTO
# fix (ChatGPT leid). coffee_brew_form tuvastab ainult valmistusvormi
# (filter/in-cup/press jne), mitte kohvi TOOTESEERIAT/segu — need on
# eri kvaliteedi/maitseprofiiliga tooted samalt brändilt, mitte lihtsalt
# "erinev kirjapilt". TEADLIKULT KITSAS valik: ainult selgelt eristuvad
# Lavazza premium-seeria nimed. "Classic"/"Gold"/"Kronung" jäeti
# TEADLIKULT VÄLJA — need on brändide "põhiliini" nimed (Jacobs
# Kronung, Paulig Classic), mitte tegelik kvaliteeditaseme erinevus;
# esialgne katse neid lisada tekitas regressiooni (Jacobs Kronung ->
# Paulig Classic, mõlemad on lihtsalt oma brändi standardkohv, langes
# vääralt SUGGESTED tasemele). Tundmatu seeria ei blokeeri — jääb
# Claude'i hooleks.
COFFEE_PRODUCT_LINE_PATTERNS: dict[str, tuple[str, ...]] = {
    "qualita_oro": (r"\bqualit[aà]\s*oro\b",),
    "qualita_rossa": (r"\bqualit[aà]\s*rossa\b",),
    "crema_gusto": (r"\bcrema\s*e?\s*gusto\b",),
    "tierra": (r"\btierra\b",),
    "mountain_grown": (r"\bmountain\s*grown\b",),
}


def _coffee_product_line(text) -> frozenset:
    return _match_variants(text, COFFEE_PRODUCT_LINE_PATTERNS)


def _product_identity_text(canonical_name, sample_product_name, brand) -> str:
    """Ühendab kõik saadaval identiteedisignaalid üheks tekstiks
    (v4.2, uus). Kasutatakse hard_check ja downgrade_check funktsioonide
    sisendina sample-nime asemel üksi.

    Põhjendus: identiteeditunnus (nt 'suitsu', 'viski', lõiketüüp) võib
    olla peidus canonical_name'is või brändis, mitte tingimata just
    selles konkreetses tootenimes, mille SQL DISTINCT ON valis. Suund
    on turvaline: rohkem teksti saab check-funktsioonide jaoks ainult
    RANGEMAKS minna, mitte kunagi lubavamaks.
    """
    return " ".join(
        part.strip()
        for part in (canonical_name, sample_product_name, brand)
        if part and part.strip()
    )


# Iga check funktsioon nime järgi, et IDENTITY_RULES saaks neid viidata
IDENTITY_CHECKS = {
    "flavour_state": _flavour_state,
    "fat_class_milk": _milk_fat_class,
    "fat_class_yogurt": _yogurt_fat_class,
    "yogurt_form": _yogurt_form,
    "animal_type": _animal_type,
    "caffeine_state": _caffeine_state,
    "cut_type": _meat_cut_type,
    "cheese_type": _cheese_type,
    "cheese_form": _cheese_form,
    "fish_species": _fish_species,
    "nut_seed_type": _nut_seed_type,
    "beverage_type": _beverage_type_nonalc,
    "meat_form": _meat_form,
}

# hard_match: erinevus EEMALDAB kandidaadi täielikult.
IDENTITY_RULES: dict[str, list[str]] = {
    "dairy_milk": ["flavour_state", "fat_class_milk"],
    "dairy_yogurt_kefir": ["flavour_state", "fat_class_yogurt", "yogurt_form"],
    "dairy_cream_sourcream": [],
    "meat_minced": ["animal_type"],
    "meat_beef_lamb_game": ["animal_type", "cut_type"],
    "meat_pork": ["animal_type", "cut_type"],
    "meat_poultry": ["animal_type", "cut_type"],
    "coffee_beans_ground": ["caffeine_state"],
    "coffee_instant": ["caffeine_state"],
    "tea": ["caffeine_state"],
    "cheese_regular": ["cheese_type", "cheese_form"],
    "dairy_cheese_slices": ["cheese_type", "cheese_form"],
    "cheese_delicatessen": ["cheese_type", "cheese_form"],
    "fish_fresh": ["fish_species"],
    "fish_salted_smoked": ["fish_species"],
    "fish_processed": ["fish_species"],
    # --- v4.5 UUS ---
    "sweets_nuts_driedfruit": ["nut_seed_type"],
    "drinks_non_alcoholic": ["beverage_type"],
    "meat_sausages": ["animal_type"],
    "meat_grill_blood_sausages": ["animal_type", "meat_form"],
}

# Kategooriad, kus AUTO on TÄIELIKULT keelatud, sõltumata kogusest või
# muudest kontrollidest. v4.2 laiendus: kogu alkohol, mitte ainult vein.
AUTO_DISABLED_SUB_CODES = {
    "wine_red", "wine_white", "wine_rose", "wine_sparkling", "wine_sweet",
    "drinks_beer_cider", "drinks_spirits",
    "spirits_vodka", "spirits_whisky", "spirits_gin", "spirits_rum",
    "spirits_cognac", "spirits_liqueur", "spirits_other",
    # v4.5.1 UUS (ChatGPT audit): ajutine turvavõrk kategooriatele, kus
    # olemasolev flavour_variant/beverage_type sõnastik on liiga kitsas,
    # et katta kõiki reaalseid variante (nt tea: ainult puuviljamaitsed,
    # mitte tee TÜÜP musta/rohelise/ürditee vahel; sweets_candies:
    # bränditud/tundmatud maitsenimed jäävad tuvastamata; drinks_non_
    # alcoholic: beverage_type on ühesuunaline kontroll — kui originaali
    # tüüp jääb tuvastamata, ei blokeeru miski). EEMALDA see rida
    # alles pärast täielikumat identiteedimudelit iga kategooria jaoks.
    "tea", "sweets_candies", "drinks_non_alcoholic",
}

DOWNGRADE_CHECKS = {
    "flavour_profile": _flavour_profile_set,
    "flavour_variant": _flavour_variants,
    "cheese_modifier": _cheese_modifiers,
    "oil_grade": _oil_grade,
    "protein_enriched": _protein_enriched,
    "grain_type": _grain_type,
    "coffee_brew_form": _coffee_brew_form,
    "coffee_product_line": _coffee_product_line,
}

# v4.4 LAIENDUS: uued maitsetundlikud kategooriad said flavour_variant
# downgrade'i, kuna nende IDENTITY_RULES ei kata toote tüüpi (erinevalt
# piimast/lihast/juustust, kus animal_type/cheese_type juba blokeerib
# ebasobivad kandidaadid enne, kui maitse üldse arvesse tuleb). Ilma
# selleta oleks nt "vaarika küpsis -> šokolaadi küpsis" sama kogusega
# saanud vääralt AUTO staatuse (sama muster mis Wyke Farms/NOCCO
# juures juba parandatud).
DOWNGRADE_RULES: dict[str, list[str]] = {
    "spices_herbs_spice_mix": ["flavour_profile"],
    "dairy_yogurt_kefir": ["flavour_variant"],
    "drinks_energy": ["flavour_variant"],
    "drinks_soft_soda": ["flavour_variant"],
    "cheese_regular": ["cheese_modifier"],
    "dairy_cheese_slices": ["cheese_modifier"],
    "cheese_delicatessen": ["cheese_modifier"],
    "sweets_biscuits_cookies": ["flavour_variant"],
    "sweets_candies": ["flavour_variant"],
    "sweets_chocolate_bars": ["flavour_variant"],
    "sweets_nuts_driedfruit": ["flavour_variant"],
    "sweets_snacks_salty": ["flavour_variant", "flavour_profile"],
    "dry_soups_noodles": ["flavour_variant", "flavour_profile"],
    "produce_smoothies_fresh_juices": ["flavour_variant"],
    "drinks_juices": ["flavour_variant"],
    "bakery_cakes_pastries": ["flavour_variant"],

    # --- v4.5 UUS ---
    # tea: FLAVOUR_VARIANT_PATTERNS kattis "mustsõstra" juba varem
    # (v4.3 pomelo fixi ajal lisatud üldisemalt), aga tea polnud KUNAGI
    # DOWNGRADE_RULES's registreeritud — Earl Grey -> mustsõstratee sai
    # seetõttu vääralt AUTO.
    "tea": ["flavour_variant"],
    "oils_olive": ["flavour_profile", "oil_grade"],
    "meat_minced": ["protein_enriched"],
    "bakery_bread_loaves": ["grain_type"],
    "coffee_beans_ground": ["coffee_brew_form", "coffee_product_line"],
    "coffee_instant": ["coffee_brew_form", "coffee_product_line"],
}


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
    # Ohutus-trait'id (ühesuunaline): laktoosivaba/gluteenivaba/
    # alkoholivaba — kui originaalil on, kandidaadil PEAB olema.
    original_required = _detect_traits(original_name, REQUIRED_TRAITS)
    candidate_required = _detect_traits(candidate_name, REQUIRED_TRAITS)
    if not original_required.issubset(candidate_required):
        return False

    # Taimne vs loomne (kahesuunaline, kehtib kõikjal)
    original_identity = _detect_traits(original_name, IDENTITY_TRAITS)
    candidate_identity = _detect_traits(candidate_name, IDENTITY_TRAITS)
    if original_identity != candidate_identity:
        return False

    # Kategooriapõhised identity-kontrollid — AINULT need, mis on
    # IDENTITY_RULES's selle sub_code kohta loetletud.
    checks_to_run = list(IDENTITY_RULES.get(sub_code, []))

    # Erand: kui toode on maitsestatud (nt Cappuccino/Latte), ei kehti
    # tavalise piima rasvaprotsendi kategooriad selle peal — "3,5%"
    # Cappuccino peal ei tähenda sama, mis "3,5%" täispiimal. Sellisel
    # juhul jääb täpne maitse-tüübi vaste Claude'i semantilise otsuse
    # kanda (flavour_state check ise juba tagab, et maitsestamata
    # kandidaat ei läbi).
    if "flavour_state" in checks_to_run and "fat_class_milk" in checks_to_run:
        if _flavour_state(original_name) == "flavored":
            checks_to_run.remove("fat_class_milk")

    for check_name in checks_to_run:
        check_fn = IDENTITY_CHECKS[check_name]
        o_val = check_fn(original_name)
        c_val = check_fn(candidate_name)
        if o_val is not None:
            if c_val is None or c_val != o_val:
                return False

    return True


# v4.2 LAIENDUS: baby_formula, baby_food_jars, baby_food_pouches,
# baby_snacks lisatud.
BABY_FOOD_SUB_CODES = {
    "baby_porridge_cereal", "baby_diapers", "baby_care", "baby_other", "baby_wipes",
    "baby_formula", "baby_food_jars", "baby_food_pouches", "baby_snacks",
}


class SubstitutionTimeout(Exception):
    pass


def _empty_quantity_rejection_reasons() -> dict[str, int]:
    return {
        "missing_rule": 0,
        "missing_candidate_quantity": 0,
        "unit_mismatch": 0,
        "unknown_unit": 0,
        "outside_allowed_range": 0,
    }


async def get_or_create_substitution(conn, group_id, chain, dry_run=False, use_cache=True):
    """
    Tagastab dict tulemuse + "trace" alamvõtme täieliku otsustusahelaga.

    dry_run=True: _save() EI kutsuta kunagi (KIHT 1 kaitsest). Kuivtesti
    skript peab lisaks avama read-only DB transaktsiooni (KIHT 2).

    use_cache=False: cache't EI loeta ega kirjutata üldse — iga kutse
    arvutab otsuse täiesti värskelt.

    v4.3: tehnilise vea korral (timeout, HTTP viga, JSON parse viga) EI
    tagastata enam None — tagastatakse struktureeritud
    {"decision_type": "provider_error", "error_type": ..., "trace": ...}
    koos kõigi enne erindit kogutud trace-väljadega.
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
        "quantity_auto_count": 0,
        "quantity_suggested_count": 0,
        "quantity_incompatible_count": 0,
        "quantity_unknown_count": 0,
        "quantity_rule_found": None,
        "quantity_rejection_reasons": _empty_quantity_rejection_reasons(),
        "dry_run": dry_run,
        "cache_enabled": use_cache,
        "database_write_attempted": False,
        "save_path_reached": False,
        "cache_hit": False,
    }

    async def _finish(result, save=True):
        if save:
            trace["save_path_reached"] = True
            if not dry_run and use_cache:
                trace["database_write_attempted"] = True
                await _save(conn, group_id, chain, result)
        result["trace"] = trace
        return result

    def _provider_error_result(error_type, message):
        return {
            "decision_type": "provider_error",
            "substitute_group_id": None,
            "price": None,
            "included_in_total": False,
            "quantity_diff_percent": None,
            "reasoning": message,
            "error_type": error_type,
            "trace": trace,
        }

    existing = None
    if use_cache:
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
    trace["quantity_rule_found"] = get_rules_for_sub_code(original["sub_code"]) is not None

    original_sample = await conn.fetchrow(
        """
        SELECT p.name AS sample_product_name, p.net_qty, p.net_unit
        FROM product_group_members m
        JOIN products p ON p.id = m.product_id
        WHERE m.group_id = $1
          AND p.net_qty IS NOT NULL AND p.net_qty > 0
          AND p.net_unit IS NOT NULL AND BTRIM(p.net_unit) <> ''
        ORDER BY p.id
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
            ORDER BY p.id
            LIMIT 1
            """,
            group_id,
        )

    original_sample_name = original_sample["sample_product_name"] if original_sample else ""
    original_qty = original_sample["net_qty"] if original_sample else None
    original_unit = original_sample["net_unit"] if original_sample else None

    # v4.2: ühendatud identiteeditekst (canonical_name + sample_name + brand)
    original_identity_text = _product_identity_text(
        original["canonical_name"], original_sample_name, original["brand"]
    )

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
          AND pr.price IS NOT NULL AND pr.price > 0
        ORDER BY
            pg.id,
            CASE WHEN LOWER(BTRIM(p.net_unit)) = LOWER(BTRIM($5)) THEN 0 ELSE 1 END,
            CASE WHEN p.net_qty IS NOT NULL AND p.net_qty > 0 THEN 0 ELSE 1 END,
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
    downgrade_checks = DOWNGRADE_RULES.get(original["sub_code"], [])

    quantity_eligible = []
    for c in candidates:
        qmatch = classify_quantity_match(
            original_qty, original_unit, c["net_qty"], c["net_unit"], original["sub_code"],
        )

        # v4.3: kogusekihi läbipaistvus — loendame KÕIK tier'id, mitte
        # ainult neid, mis läbivad. See eristab "sub_code puudub
        # QUANTITY_RULES-ist" (missing_rule) muudest põhjustest.
        if qmatch.tier == QuantityTier.AUTO:
            trace["quantity_auto_count"] += 1
        elif qmatch.tier == QuantityTier.SUGGESTED:
            trace["quantity_suggested_count"] += 1
        elif qmatch.tier == QuantityTier.INCOMPATIBLE:
            trace["quantity_incompatible_count"] += 1
            # v4.5.1 fix (ChatGPT leid): varem loeti KÕIK INCOMPATIBLE
            # tulemused "outside_allowed_range" alla, ka päris
            # baasühiku-mittevastavused (g vs ml) — unit_mismatch oli
            # seetõttu trace's alati 0. qmatch.rejection_reason
            # tuleb nüüd otse quantity_service.py'st.
            reason_key = (
                qmatch.rejection_reason.value
                if qmatch.rejection_reason is not None
                else QuantityRejectionReason.OUTSIDE_ALLOWED_RANGE.value
            )
            trace["quantity_rejection_reasons"][reason_key] += 1
        elif qmatch.tier == QuantityTier.UNKNOWN:
            trace["quantity_unknown_count"] += 1
            if not trace["quantity_rule_found"]:
                trace["quantity_rejection_reasons"]["missing_rule"] += 1
            elif c["net_qty"] is None or c["net_unit"] is None or not str(c["net_unit"]).strip():
                trace["quantity_rejection_reasons"]["missing_candidate_quantity"] += 1
            else:
                # v4.5.3 fix (ChatGPT leid): see EI ole päris "unit_mismatch"
                # (kaks TUVASTATUD baasühikut, mis erinevad — see tuleb
                # INCOMPATIBLE tier'ist ja on juba eraldi loetud ülal).
                # Siin on net_unit väärtus ISE ebaselge/parsimatu kuju
                # (nt "pack" ilma tükiarvuta) — puuduv/parsimatu andmestik,
                # mitte kahe teadaoleva ühiku konflikt.
                trace["quantity_rejection_reasons"]["unknown_unit"] += 1

        if qmatch.tier in (QuantityTier.INCOMPATIBLE, QuantityTier.UNKNOWN):
            continue

        candidate_identity_text = _product_identity_text(
            c["canonical_name"], c["sample_product_name"], c["brand"]
        )

        effective_tier = qmatch.tier
        # downgrade: erinevus ei eemalda kandidaati, vaid langetab
        # tier'i (nt marinaadi maitseprofiil) — EI TÕSTA kunagi üles.
        for check_name in downgrade_checks:
            check_fn = DOWNGRADE_CHECKS[check_name]
            o_values = check_fn(original_identity_text)
            c_values = check_fn(candidate_identity_text)
            # Sümmeetriline võrdlus (ChatGPT viies ülevaatus): kui
            # KUMMALGI poolel on tuvastatud väärtusi JA hulgad
            # erinevad, langetatakse tier — mitte ainult siis, kui
            # originaalil on väärtus.
            if o_values != c_values and (o_values or c_values):
                if effective_tier == QuantityTier.AUTO:
                    effective_tier = QuantityTier.SUGGESTED

        # Kategooriad, kus AUTO on täielikult keelatud (nt vein/õlu/
        # kanged alkoholid) — sõltumata kogusest, langeb alati vähemalt
        # SUGGESTED tasemele.
        if original["sub_code"] in AUTO_DISABLED_SUB_CODES and effective_tier == QuantityTier.AUTO:
            effective_tier = QuantityTier.SUGGESTED

        quantity_eligible.append({
            "id": c["id"],
            "canonical_name": c["canonical_name"],
            "brand": c["brand"],
            "sample_product_name": c["sample_product_name"],
            "identity_text": candidate_identity_text,
            "quantity_tier": effective_tier,
            "quantity_diff_percent": qmatch.difference_percent,
        })
    trace["quantity_eligible_count"] = len(quantity_eligible)

    usable_candidates = [
        c for c in quantity_eligible
        if _traits_compatible(original_identity_text, c["identity_text"], original["sub_code"])
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
        return _provider_error_result("timeout", "Claude API kutse aegus")
    except httpx.HTTPStatusError as e:
        logger.error(f"Substitution HTTP error group_id={group_id} chain={chain}: {e}")
        return _provider_error_result("http_error", f"Claude API HTTP viga: {e}")
    except Exception as e:
        logger.error(f"Substitution error group_id={group_id} chain={chain}: {e}")
        return _provider_error_result("unknown_error", f"Ootamatu viga: {e}")

    if claude_result is None:
        # _ask_claude_for_semantic_match tagastas None (JSON parse ebaõnnestus
        # või vastus polnud dict) — see on juba logitud funktsiooni sees.
        return _provider_error_result("json_parse_error", "Claude vastas mitte-JSON formaadis")

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

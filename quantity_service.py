"""
Seivy — koguse/ühiku sobivuse deterministlik klassifikatsioon.

See moodul EI kutsu Claude API't ega puuduta andmebaasi. Ainus vastutus:
otsustada, kas kahe toote kogused (net_qty + net_unit) on piisavalt
sarnased, et üks saaks teist automaatselt asendada, olla soovitusena
näidatud, või on need lihtsalt sobimatud/teadmata.

Disain ChatGPT arhitektuuriülevaatuse põhjal (juuli 2026):
- Kogus/ühik ON DETERMINISTLIK ARVUTUS, mitte Claude'i hinnang.
- "unknown" (net_qty/net_unit puudub) EI TOHI kunagi minna automaatsesse
  asendusse — see on fail-closed vaikeväärtus.
- "pakk"/"pack" ei teisendata automaatselt tükkideks, kui tükiarv pole
  eraldi teada — jääb unknown.
- l/kg EI teisendata automaatselt ml/g vastu risti (st erinevad
  baasühikute TÜÜBID, mitte lihtsalt eri kordajad).

SUBSTITUTION_RULES_VERSION on kirjas cache-võtme jaoks (kui reegleid
hiljem muudetakse, peavad vanad product_substitutions kirjed aeguma).

v4.4 muudatus (juuli 2026): QUANTITY_RULES laiendatud 214-testi
dry-run analüüsi põhjal. Kategooriad, kus varem oli sql_candidate_count
suur, aga quantity_eligible_count alati 0 (missing_rule) — seega puhas
"puuduv reegel", mitte andmeprobleem. Protsendid valitud riskipõhiselt:
- Kategooriad, kus IDENTITY_RULES juba katab tüübi (kohv, tee, juust) —
  sama muster nagu olemasolevad kaetud kategooriad (15/30).
- Kategooriad ilma identity-kontrollita, aga madala identiteediriskiga
  (puu-/juurvili, kus kaal loomulikult varieerub) — laiem piir (20/40).
- Kategooriad, kus toote identiteet on maitses/variandis (maiustused,
  küpsised, snäkid, supid/nuudlid, mahlad) — kitsam auto_pct (10) JA
  lisatud flavour_variant downgrade DOWNGRADE_RULES'i, sama mehhanism
  mis juba kaitseb jogurtit/energiajooke/marinaade.
- Alkoholikategooriad (õlu/siider, muu kange alkohol) said reeglid,
  kuna AUTO_DISABLED_SUB_CODES juba sunnib need SUGGESTED tasemele
  sõltumata protsendist — turvaline lisada.
SUBSTITUTION_RULES_VERSION tõstetud 1 -> 2, kuna reeglite muutus mõjutab
varasemate cache-kirjete kehtivust.

v4.5 muudatus (juuli 2026, 214-testi v4.4 jooksu analüüs + ChatGPT
audit): 5 sub_code'i lisatud, mis olid seni QUANTITY_RULES-ist puudu
(quantity_rule_found=False kõigil, kinnitatud dry-run trace'idest).
Kõigil viiel on meat_/fish_ mustriga sarnane madal identiteediriski
piir (15/30), sest need on juba (osaliselt) kaetud animal_type/
cut_type/fish_species hard identity kontrollidega
substitution_service.py's — vt sealt IDENTITY_RULES täiendus.
SUBSTITUTION_RULES_VERSION tõstetud 2 -> 3.

v4.5.1-v4.5.2 muudatus (ChatGPT sõltumatu ülevaatus, otsust mõjutav
loogika substitution_service.py's: animal_type regex + kana/kalkuni
eristus, AUTO_DISABLED_SUB_CODES laiendus 3 kategooriale, heeringa/
räime eristus, meat_form laiendus): kõik need muudavad potentsiaalselt
salvestatavat asendusotsust, seega SUBSTITUTION_RULES_VERSION tõstetud
3 -> 4, et vanad (versioon 3 all salvestatud) cache-kirjed aeguksid
korrektselt, mitte ei jääks kehtima kuni TTL-i lõpuni.

v4.5.4 muudatus (ChatGPT neljas ülevaatus, 214-testi v4.5.2 jooksu
audit): kaks veel avastatud false-AUTO't oils_olive/coffee_beans_ground
kategoorias (vt substitution_service.py FLAVOUR_PROFILE_KEYWORDS
"fruity" + uus COFFEE_PRODUCT_LINE_PATTERNS/coffee_product_line).
SUBSTITUTION_RULES_VERSION tõstetud 4 -> 5.

v4.6.2 muudatus (269-testi v4.6.1 jooksu audit): dry_soups_noodles
false-AUTO (Ajinomoto ramen sealihamaitseline -> kanamaitseline) —
FLAVOUR_PROFILE_KEYWORDS laiendatud liha-/puljongimaitsetega (kana/
sealiha/veise/krevett/miso/karri), dry_soups_noodles DOWNGRADE_RULES
lisatud "flavour_profile" (vt substitution_service.py).
SUBSTITUTION_RULES_VERSION tõstetud 5 -> 6.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Optional


SUBSTITUTION_RULES_VERSION = 6


class QuantityTier(StrEnum):
    AUTO = "auto"
    SUGGESTED = "suggested"
    INCOMPATIBLE = "incompatible"
    UNKNOWN = "unknown"


class QuantityRejectionReason(StrEnum):
    """v4.5.1 UUS (ChatGPT leid): eristab INCOMPATIBLE tier'i KAHTE
    erinevat põhjust, mida substitution_service.py trace varem ei
    eristanud — kõik INCOMPATIBLE tulemused (nii "baasühikud ei klapi"
    kui ka "kogus liiga erinev") loeti kokku 'outside_allowed_range'
    alla, mistõttu 'unit_mismatch' oli trace's alati 0, isegi kui
    ühikud tegelikult ei klappinud (nt g vs ml).

    v4.5.3 LISATUD (ChatGPT teine leid): UNKNOWN_UNIT eraldi
    UNIT_MISMATCH'ist. UNIT_MISMATCH tähendab, et originaali ja
    kandidaadi BAASÜHIKUD on tuvastatud, aga erinevad (g vs ml) —
    see tuleb alati INCOMPATIBLE tier'ist. UNKNOWN_UNIT tähendab, et
    net_unit väärtus ise on tundmatu/ebaselge kuju (nt "pack" ilma
    tükiarvuta) — see tuleb UNKNOWN tier'ist ega ole päris "mismatch",
    vaid puuduv/parsimatu andmestik."""
    MISSING_QUANTITY = "missing_candidate_quantity"
    MISSING_RULE = "missing_rule"
    UNIT_MISMATCH = "unit_mismatch"
    UNKNOWN_UNIT = "unknown_unit"
    OUTSIDE_ALLOWED_RANGE = "outside_allowed_range"


@dataclass(frozen=True)
class QuantityMatch:
    tier: QuantityTier
    difference_percent: Optional[Decimal]
    original_base_qty: Optional[Decimal]
    candidate_base_qty: Optional[Decimal]
    base_unit: Optional[str]
    reason: str
    rejection_reason: Optional[QuantityRejectionReason] = None


# ---------------- ühikute normaliseerimine ----------------

_VOLUME_TO_ML = {
    "ml": Decimal(1),
    "l": Decimal(1000),
    "cl": Decimal(10),
}

_MASS_TO_G = {
    "g": Decimal(1),
    "kg": Decimal(1000),
}

_PIECE_UNITS = {"tk", "pcs", "pc", "piece", "pieces"}


def normalize_unit(raw_unit: Optional[str]) -> Optional[tuple[str, Decimal]]:
    """
    Tagastab (baasühik, kordaja) või None kui ühik on tundmatu/ebaselge.
    Kordajaga korrutades saab väärtuse baasühikusse teisendada.
    """
    if not raw_unit:
        return None
    u = raw_unit.strip().lower()
    if not u:
        return None

    if u in _VOLUME_TO_ML:
        return ("ml", _VOLUME_TO_ML[u])
    if u in _MASS_TO_G:
        return ("g", _MASS_TO_G[u])
    if u in _PIECE_UNITS:
        return ("tk", Decimal(1))

    return None


def _to_decimal(value) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if d <= 0:
        return None
    if d > Decimal(100000):
        return None
    return d


def _effective_qty(net_qty, pack_count) -> Optional[Decimal]:
    qty = _to_decimal(net_qty)
    if qty is None:
        return None
    pc = _to_decimal(pack_count) if pack_count is not None else None
    if pc is None or pc <= 0:
        pc = Decimal(1)
    return qty * pc


# ---------------- kategooriapõhised piirid ----------------

QUANTITY_RULES: dict[str, dict[str, int]] = {
    # --- Olemasolevad (v4 algsest versioonist, muutmata) ---
    "dairy_milk": {"auto_pct": 20, "suggested_pct": 50},
    "dairy_yogurt_kefir": {"auto_pct": 20, "suggested_pct": 50},
    "dairy_cream_sourcream": {"auto_pct": 20, "suggested_pct": 50},
    "drinks_soft_soda": {"auto_pct": 20, "suggested_pct": 50},
    "drinks_energy": {"auto_pct": 20, "suggested_pct": 50},
    "spices_herbs_spice_mix": {"auto_pct": 10, "suggested_pct": 25},
    "dairy_eggs": {"auto_pct": 0, "suggested_pct": 40},
    "meat_minced": {"auto_pct": 15, "suggested_pct": 30},
    "meat_beef_lamb_game": {"auto_pct": 15, "suggested_pct": 30},
    "cheese_regular": {"auto_pct": 15, "suggested_pct": 30},
    "cheese_delicatessen": {"auto_pct": 15, "suggested_pct": 30},
    "fish_fresh": {"auto_pct": 15, "suggested_pct": 30},
    "fish_processed": {"auto_pct": 15, "suggested_pct": 30},
    "wine_white": {"auto_pct": 10, "suggested_pct": 20},
    "wine_red": {"auto_pct": 10, "suggested_pct": 20},
    "wine_rose": {"auto_pct": 10, "suggested_pct": 20},
    "baby_porridge_cereal": {"auto_pct": 0, "suggested_pct": 0},
    "baby_diapers": {"auto_pct": 0, "suggested_pct": 0},
    "baby_care": {"auto_pct": 0, "suggested_pct": 0},
    "baby_other": {"auto_pct": 0, "suggested_pct": 0},

    # --- v4.4 UUS: kategooriad, kus IDENTITY_RULES juba katab tüübi
    # (kohv, tee, juustuviilud) — sama piir mis meat/cheese ---
    "coffee_beans_ground": {"auto_pct": 15, "suggested_pct": 30},
    "coffee_instant": {"auto_pct": 15, "suggested_pct": 30},
    "tea": {"auto_pct": 15, "suggested_pct": 30},
    "dairy_cheese_slices": {"auto_pct": 15, "suggested_pct": 30},
    "wine_sparkling": {"auto_pct": 10, "suggested_pct": 20},
    "wine_sweet": {"auto_pct": 10, "suggested_pct": 20},

    # --- v4.4 UUS: madala identiteediriskiga (kaal loomulikult
    # varieerub, identity check pole kriitiline) — laiem piir ---
    "oils_olive": {"auto_pct": 15, "suggested_pct": 30},
    "bakery_bread_loaves": {"auto_pct": 15, "suggested_pct": 35},
    "dry_canned_veg": {"auto_pct": 15, "suggested_pct": 30},
    "produce_apples_pears": {"auto_pct": 20, "suggested_pct": 40},
    "produce_berries": {"auto_pct": 20, "suggested_pct": 40},
    "produce_herbs_salads_sprouts": {"auto_pct": 20, "suggested_pct": 40},
    "produce_root_veg": {"auto_pct": 20, "suggested_pct": 40},
    "produce_tropical": {"auto_pct": 20, "suggested_pct": 40},

    # --- v4.4 UUS: maitsetundlikud kategooriad — kitsam auto_pct,
    # lisatud KOOS vastava flavour_variant downgrade kirjega
    # DOWNGRADE_RULES'is (substitution_service.py) ---
    "bakery_cakes_pastries": {"auto_pct": 10, "suggested_pct": 25},
    "sweets_biscuits_cookies": {"auto_pct": 10, "suggested_pct": 25},
    "sweets_candies": {"auto_pct": 10, "suggested_pct": 25},
    "sweets_chocolate_bars": {"auto_pct": 10, "suggested_pct": 25},
    "sweets_nuts_driedfruit": {"auto_pct": 15, "suggested_pct": 30},
    "sweets_snacks_salty": {"auto_pct": 10, "suggested_pct": 25},
    "dry_soups_noodles": {"auto_pct": 15, "suggested_pct": 30},
    "produce_smoothies_fresh_juices": {"auto_pct": 15, "suggested_pct": 30},
    "drinks_juices": {"auto_pct": 15, "suggested_pct": 30},
    "drinks_non_alcoholic": {"auto_pct": 15, "suggested_pct": 30},

    # --- v4.4 UUS: alkohol — AUTO_DISABLED_SUB_CODES juba sunnib
    # SUGGESTED tasemele, seega turvaline lisada helde piir ---
    "drinks_beer_cider": {"auto_pct": 15, "suggested_pct": 30},
    "spirits_other": {"auto_pct": 10, "suggested_pct": 25},

    # --- v4.4 UUS: lemmikloomatoit — konservatiivne, sama muster
    # nagu snäkid (maitsevariandid olulised, aga vale valik pole
    # tervist ohustav nagu inimtoidus laktoos/gluteen) ---
    "pet_cat_wet": {"auto_pct": 10, "suggested_pct": 25},

    # --- v4.5 UUS: lihakategooriad, mis dry-run analüüsis olid
    # quantity_rule_found=False (missing_rule). Sama piir mis
    # meat_beef_lamb_game/fish_fresh/fish_processed, kuna neil on
    # (nüüdsest, vt substitution_service.py IDENTITY_RULES) juba
    # animal_type/cut_type/fish_species hard identity kontroll ---
    "meat_pork": {"auto_pct": 15, "suggested_pct": 30},
    "meat_poultry": {"auto_pct": 15, "suggested_pct": 30},
    "meat_sausages": {"auto_pct": 15, "suggested_pct": 30},
    "meat_grill_blood_sausages": {"auto_pct": 15, "suggested_pct": 30},
    "fish_salted_smoked": {"auto_pct": 15, "suggested_pct": 30},

    # TEADLIKULT ENDISELT KATMATA: coffee_capsules (kapslisüsteemid
    # pole omavahel asendatavad, vajab eraldi identity-kontrolli enne
    # kui üldse kogusepiiri lisada), kõik ülejäänud sub_code'id, mida
    # praeguses 214-testi valimis ei esinenud.
}


def get_rules_for_sub_code(sub_code: str) -> Optional[dict[str, int]]:
    """
    Tagastab None, kui kategooria pole veel teadlikult üle vaadatud —
    caller peab sel juhul tagastama QuantityTier.UNKNOWN (fail-closed),
    mitte kasutama mingit üldist piiri.
    """
    return QUANTITY_RULES.get(sub_code)


# ---------------- peafunktsioon ----------------

def classify_quantity_match(
    original_qty,
    original_unit: Optional[str],
    candidate_qty,
    candidate_unit: Optional[str],
    sub_code: str,
    original_pack_count=None,
    candidate_pack_count=None,
    apply_pack_count: bool = False,
) -> QuantityMatch:
    if apply_pack_count:
        o_qty = _effective_qty(original_qty, original_pack_count)
        c_qty = _effective_qty(candidate_qty, candidate_pack_count)
    else:
        o_qty = _to_decimal(original_qty)
        c_qty = _to_decimal(candidate_qty)

    if o_qty is None or c_qty is None:
        return QuantityMatch(
            tier=QuantityTier.UNKNOWN,
            difference_percent=None,
            original_base_qty=None,
            candidate_base_qty=None,
            base_unit=None,
            reason="net_qty puudub või on vigane (0/negatiivne/ebarealistlik/tühi)",
        )

    o_norm = normalize_unit(original_unit)
    c_norm = normalize_unit(candidate_unit)

    if o_norm is None or c_norm is None:
        return QuantityMatch(
            tier=QuantityTier.UNKNOWN,
            difference_percent=None,
            original_base_qty=None,
            candidate_base_qty=None,
            base_unit=None,
            reason="net_unit puudub või on ebaselge (nt 'pack'/'pakk' ilma tükiarvuta)",
        )

    o_base_unit, o_factor = o_norm
    c_base_unit, c_factor = c_norm

    if o_base_unit != c_base_unit:
        return QuantityMatch(
            tier=QuantityTier.INCOMPATIBLE,
            difference_percent=None,
            original_base_qty=o_qty * o_factor,
            candidate_base_qty=c_qty * c_factor,
            base_unit=None,
            reason=f"baasühikud ei klapi ({o_base_unit} vs {c_base_unit})",
            rejection_reason=QuantityRejectionReason.UNIT_MISMATCH,
        )

    o_base_qty = o_qty * o_factor
    c_base_qty = c_qty * c_factor

    rules = get_rules_for_sub_code(sub_code)
    if rules is None:
        return QuantityMatch(
            tier=QuantityTier.UNKNOWN,
            difference_percent=abs(c_base_qty - o_base_qty) / o_base_qty * Decimal(100),
            original_base_qty=o_base_qty,
            candidate_base_qty=c_base_qty,
            base_unit=o_base_unit,
            reason=f"sub_code '{sub_code}' pole QUANTITY_RULES-is — fail-closed UNKNOWN",
        )

    diff_percent = abs(c_base_qty - o_base_qty) / o_base_qty * Decimal(100)
    auto_pct = Decimal(rules["auto_pct"])
    suggested_pct = Decimal(rules["suggested_pct"])

    if diff_percent <= auto_pct:
        tier = QuantityTier.AUTO
        reason = f"kogusevahe {diff_percent:.1f}% <= auto-piir {auto_pct}% ({sub_code})"
        rejection_reason = None
    elif diff_percent <= suggested_pct:
        tier = QuantityTier.SUGGESTED
        reason = f"kogusevahe {diff_percent:.1f}% <= soovituse piir {suggested_pct}% ({sub_code})"
        rejection_reason = None
    else:
        tier = QuantityTier.INCOMPATIBLE
        reason = f"kogusevahe {diff_percent:.1f}% > soovituse piir {suggested_pct}% ({sub_code})"
        rejection_reason = QuantityRejectionReason.OUTSIDE_ALLOWED_RANGE

    return QuantityMatch(
        tier=tier,
        difference_percent=diff_percent,
        original_base_qty=o_base_qty,
        candidate_base_qty=c_base_qty,
        base_unit=o_base_unit,
        reason=reason,
        rejection_reason=rejection_reason,
    )

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
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Optional


SUBSTITUTION_RULES_VERSION = 1


class QuantityTier(StrEnum):
    AUTO = "auto"
    SUGGESTED = "suggested"
    INCOMPATIBLE = "incompatible"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class QuantityMatch:
    tier: QuantityTier
    difference_percent: Optional[Decimal]
    original_base_qty: Optional[Decimal]
    candidate_base_qty: Optional[Decimal]
    base_unit: Optional[str]
    reason: str


# ---------------- ühikute normaliseerimine ----------------

# Baasühikud lõplikuks võrdluseks: 'ml' (vedelik), 'g' (mass), 'tk' (tükid).
# l/kg EI teisendata risti ml/g vastu — need on eri baasühiku TÜÜBID.
_VOLUME_TO_ML = {
    "ml": Decimal(1),
    "l": Decimal(1000),
    "cl": Decimal(10),
}

_MASS_TO_G = {
    "g": Decimal(1),
    "kg": Decimal(1000),
}

# "pack"/"pakk" TEADLIKULT välja jäetud — tükiarv pole ühetähenduslik
# (1 pakk mune võib olla 6, 10 või 12 muna). Jääb unknown.
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

    return None  # nt "pack", "pakk", tühjad stringid, tundmatud lühendid


def _to_decimal(value) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if d <= 0:
        return None
    # Sanity-piir ebarealistlike väärtuste vastu (nt andmeviga: net_qty=999999).
    # Ei ürita "parandada", lihtsalt keeldub sellisest väärtusest tuginemast.
    if d > Decimal(100000):
        return None
    return d


def _effective_qty(net_qty, pack_count) -> Optional[Decimal]:
    """
    Arvutab tegeliku võrreldava koguse, arvestades pack_count'i.

    ETTEVAATUST (ChatGPT arvustus, juuli 2026): pole veel kinnitatud, kas
    net_qty tähendab KÕIGIS andmeallikates "üks ühik" (nt üks 350ml pudel
    3-pakis) või mõnel juhul juba kogu pakendi kogust. Enne selle
    korrutamise reaalset kasutamist tuleb auditeerida iga scraperi/keti
    semantikat eraldi. Kuni auditit pole tehtud, kutsutakse seda
    funktsiooni pack_count=1 vaikeväärtusega (vt classify_quantity_match
    parameeter apply_pack_count).
    """
    qty = _to_decimal(net_qty)
    if qty is None:
        return None
    pc = _to_decimal(pack_count) if pack_count is not None else None
    if pc is None or pc <= 0:
        pc = Decimal(1)
    return qty * pc


# ---------------- kategooriapõhised piirid ----------------

# MVP: piirid koodis, mitte DB tabelis (versioonihalduses, lihtsalt
# ülevaadatav). SUBSTITUTION_RULES_VERSION tuleb tõsta, kui neid muudetakse.
#
# TURVALISUSE PÕHIMÕTE (ChatGPT arvustus, juuli 2026): katmata kategooriad
# EI SAA vaikimisi 20%/40% piiri — need on FAIL-CLOSED (UNKNOWN), kuni
# keegi on kategooria jaoks teadliku otsuse teinud ja siia lisanud.
# Üldine piir võib olla ühes kategoorias liiga lõtv (nt hambapasta
# mitmikpakend) ja teises liiga range (nt hakkliha).
QUANTITY_RULES: dict[str, dict[str, int]] = {
    "dairy_milk": {"auto_pct": 20, "suggested_pct": 50},
    "dairy_yogurt_kefir": {"auto_pct": 20, "suggested_pct": 50},
    "dairy_cream_sourcream": {"auto_pct": 20, "suggested_pct": 50},
    "drinks_soft_soda": {"auto_pct": 20, "suggested_pct": 50},
    "drinks_energy": {"auto_pct": 20, "suggested_pct": 50},
    "spices_herbs_spice_mix": {"auto_pct": 10, "suggested_pct": 25},
    # spices_broth_stock (puljongid/fondid) TEADLIKULT VÄLJA JÄETUD —
    # see on erinev tootetüüp (vedel/kuubik) kui kuivmaitseained, pole
    # kellegi poolt teadlikult läbi vaadatud. Fail-closed UNKNOWN, kuni
    # keegi selle kategooria jaoks otsuse teeb.
    "dairy_eggs": {"auto_pct": 0, "suggested_pct": 40},
    # ESIALGNE, vajab teadlikku ülevaatust (ChatGPT tabel viitas
    # lihatoodete puhul rasvaprotsendi/lihaliigi olulisusele, aga
    # täpne piir pole veel eraldi hinnatud) — kasutatud ainult
    # animal_type identity-kontrolli testimiseks.
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
    # TEADLIKULT EI OLE "__default__" — katmata sub_code läheb UNKNOWN'i,
    # vaata get_rules_for_sub_code().
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
    """
    Klassifitseerib, kas candidate saab original'it asendada koguse
    seisukohast. EI tea midagi toote tüübist/brändist — ainult numbrid.

    apply_pack_count=False (vaikimisi): pack_count'i EI kasutata, kuna
    pole veel auditeeritud, kas net_qty tähendab kõigis andmeallikates
    "üks ühik" või mõnikord juba kogu pakendi kogust (vt _effective_qty
    docstring). Kui audit on tehtud ja kinnitatud, kutsuja saab anda
    apply_pack_count=True.
    """
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
        )

    o_base_qty = o_qty * o_factor
    c_base_qty = c_qty * c_factor

    rules = get_rules_for_sub_code(sub_code)
    if rules is None:
        # FAIL-CLOSED: kategooria pole teadlikult üle vaadatud.
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
    elif diff_percent <= suggested_pct:
        tier = QuantityTier.SUGGESTED
        reason = f"kogusevahe {diff_percent:.1f}% <= soovituse piir {suggested_pct}% ({sub_code})"
    else:
        tier = QuantityTier.INCOMPATIBLE
        reason = f"kogusevahe {diff_percent:.1f}% > soovituse piir {suggested_pct}% ({sub_code})"

    return QuantityMatch(
        tier=tier,
        difference_percent=diff_percent,
        original_base_qty=o_base_qty,
        candidate_base_qty=c_base_qty,
        base_unit=o_base_unit,
        reason=reason,
    )

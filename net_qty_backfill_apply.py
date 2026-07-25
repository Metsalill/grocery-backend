"""
Seivy — net_qty/net_unit/pack_count BACKFILL APPLY (kirjutab päriselt).

TAUST: see rakendab TÄPSELT need 165 rida, mis said kinnitatud
net_qty_backfill_preview.py teises (konfliktituvastusega) jooksus.
Väärtused on SIIA FAILI KÕVASTI KIRJUTATUD (APPLY_ROWS), mitte uuesti
käivitusajal parsitud — see väldib igasugust lahknevust preview'is
nähtu ja tegelikult kirjutatava vahel.

ULATUS: AINULT `products` tabel (net_qty, net_unit, pack_count).
`product_groups` tabelis EI OLE neid veerge üldse (kinnitatud
information_schema päringuga) — substitution_service.py loeb kogust
alati otse `products` tabelist (vt sealt rida ~1073, ~1130:
"SELECT p.name, p.net_qty, p.net_unit FROM products p ..."), seega
täiendavat product_groups sammu pole vaja.

VÄLJA JÄETUD (16 rida, EI PUUDUTATA):
  - 11 SKIP_GROUP_CONFLICT (group_id 25734, 28584, 28654, 28670 —
    tõenäoline grupeerimisviga, üksikpakk vs karp samas grupis;
    vajab eraldi käsitsi ülevaatust, mitte backfilli SQL-i)
  - 3 SKIP_KG_PRICED (kaalutooted, fikseeritud pakendisuurus puudub)
  - 1 SKIP_CATEGORY_CONFLICT (product_id=125225, sub_code='spirits_
    liqueur' vs grupi 'coffee_instant' — taksonoomiaviga)
  - 1 SKIP_UNPARSEABLE

KAITSED:
  - Iga UPDATE'i WHERE-klausel nõuab net_qty IS NULL — kui keegi on
    vahepeal selle väärtuse käsitsi täitnud, seda rida EI ÜLE KIRJUTATA
    (rowcount=0 sellel real, logitakse hoiatusena).
  - Käivitub TÕELISES transaktsioonis (mitte READ ONLY, kuna see PEAB
    kirjutama) — kui midagi ebaõnnestub, kogu operatsioon ROLLBACK.
  - Enne COMMIT'i prinditakse täielik enne/pärast kokkuvõte.
  - Kirjutab rollback_log.json faili (kõigi mõjutatud product_id
    vanad väärtused olid NULL — rollback on triviaalne, vt lõpust).
  - Nõuab EXPLICIT kinnitust: käsurea argument --commit. Ilma selleta
    näitab ainult, mida TEHTAIS, ega tee ühtegi UPDATE'i (turvaline
    vaikeväärtus).

KÄIVITAMINE (kõigepealt ILMA --commit, et üle vaadata):
    export DATABASE_URL="postgresql://..."
    python3 net_qty_backfill_apply.py

Kui väljund näeb korras välja, käivita uuesti kinnitusega:
    python3 net_qty_backfill_apply.py --commit
"""

import asyncio
import json
import os
import sys

import asyncpg


# 165 kinnitatud rida: (product_id, net_qty, net_unit, pack_count).
# Pärineb net_qty_backfill_preview.json teisest (konfliktituvastusega)
# jooksust, kus kõik WOULD_SET_SINGLE / WOULD_SET_MULTIPACK /
# WOULD_SET_MULTIPACK_RECOVERED_DECIMAL staatused olid grupi tasemel
# konfliktivabad.
APPLY_ROWS = [
    (583459, '200.0', 'g', 1), (124662, '100.0', 'g', 1), (307486, '100.0', 'g', 1),
    (423911, '150.0', 'g', 1), (634186, '150.0', 'g', 1), (629900, '150.0', 'g', 1),
    (149369, '150.0', 'g', 1), (631289, '150.0', 'g', 1), (630404, '150.0', 'g', 1),
    (656603, '130.0', 'g', 1), (379264, '280.0', 'g', 1), (580781, '280.0', 'g', 1),
    (423820, '200.0', 'g', 1), (625914, '200.0', 'g', 1), (380301, '125.0', 'g', 1),
    (580954, '125.0', 'g', 1), (619112, '125.0', 'g', 1), (622375, '125.0', 'g', 1),
    (582762, '160.0', 'g', 1), (656877, '160.0', 'g', 1), (95202, '200.0', 'g', 1),
    (380303, '180.0', 'g', 1), (638944, '150.0', 'g', 1), (622466, '100.0', 'g', 1),
    (580847, '200.0', 'g', 1), (656113, '80.0', 'g', 1), (666834, '500.0', 'g', 2),
    (375717, '500.0', 'g', 1), (380750, '500.0', 'g', 1), (385783, '500.0', 'g', 1),
    (391007, '500.0', 'g', 1), (624965, '500.0', 'g', 1), (626133, '500.0', 'g', 1),
    (631299, '500.0', 'g', 1), (39326, '1000.0', 'g', 1), (626134, '1000.0', 'g', 1),
    (39623, '500.0', 'g', 1), (125893, '1000.0', 'g', 1), (422408, '1000.0', 'g', 1),
    (624841, '1000.0', 'g', 1), (626395, '1000.0', 'g', 1), (647840, '1000.0', 'g', 1),
    (125271, '1000.0', 'g', 1), (626580, '1000.0', 'g', 1), (628367, '1000.0', 'g', 1),
    (379531, '250.0', 'g', 1), (417579, '250.0', 'g', 1), (420182, '250.0', 'g', 1),
    (41447, '1000.0', 'g', 1), (641366, '200.0', 'g', 1), (343494, '500.0', 'g', 1),
    (125806, '500.0', 'g', 1), (624879, '500.0', 'g', 1), (641297, '125.0', 'g', 1),
    (641385, '125.0', 'g', 1), (125294, '1000.0', 'g', 1), (632084, '1000.0', 'g', 1),
    (651639, '1000.0', 'g', 1), (421018, '250.0', 'g', 1), (624928, '250.0', 'g', 1),
    (628765, '250.0', 'g', 1), (631310, '250.0', 'g', 1), (629511, '16.0', 'g', 10),
    (632611, '14.0', 'g', 8), (376265, '200.0', 'g', 1), (424008, '200.0', 'g', 1),
    (380178, '100.0', 'g', 1), (417493, '12.5', 'g', 10), (419963, '12.5', 'g', 10),
    (422806, '12.5', 'g', 10), (651947, '12.5', 'g', 10), (664684, '12.5', 'g', 10),
    (651887, '100.0', 'g', 1), (376618, '8.0', 'g', 1), (628548, '12.6', 'g', 10),
    (661578, '200.0', 'g', 1), (424044, '185.0', 'g', 1), (617926, '185.0', 'g', 1),
    (619485, '185.0', 'g', 1), (628138, '185.0', 'g', 1), (622594, '200.0', 'g', 1),
    (628660, '200.0', 'g', 1), (644969, '200.0', 'g', 1), (652476, '200.0', 'g', 1),
    (631106, '180.0', 'g', 1), (379717, '150.0', 'g', 1), (342655, '150.0', 'g', 1),
    (342161, '200.0', 'g', 1), (638930, '200.0', 'g', 1), (378964, '200.0', 'g', 1),
    (170029, '400.0', 'g', 1), (376991, '200.0', 'g', 1), (666220, '170.0', 'g', 1),
    (379829, '1000.0', 'ml', 1), (616626, '1000.0', 'ml', 1), (580744, '1500.0', 'ml', 1),
    (629273, '1500.0', 'ml', 1), (644727, '1500.0', 'ml', 1), (644767, '1000.0', 'g', 1),
    (421823, '1000.0', 'ml', 1), (616775, '1000.0', 'ml', 1), (421891, '1000.0', 'ml', 1),
    (617012, '1000.0', 'ml', 1), (423992, '1000.0', 'ml', 1), (617448, '1000.0', 'ml', 1),
    (421254, '400.0', 'g', 1), (616529, '400.0', 'g', 1), (627518, '400.0', 'g', 1),
    (421039, '200.0', 'ml', 1), (616876, '200.0', 'ml', 1), (623192, '200.0', 'ml', 1),
    (380095, '450.0', 'ml', 1), (638704, '450.0', 'ml', 1), (666082, '450.0', 'ml', 1),
    (633213, '330.0', 'ml', 1), (45935, '960.00', 'ml', 1), (377062, '960.0', 'ml', 1),
    (638806, '1000.0', 'ml', 1), (656826, '30.0', 'g', 1), (658581, '1000.0', 'ml', 1),
    (376898, '380.0', 'g', 1), (423309, '380.0', 'g', 1), (580756, '380.0', 'g', 1),
    (617592, '380.0', 'g', 1), (125070, '100.0', 'g', 4), (423360, '100.0', 'g', 4),
    (616218, '100.0', 'g', 4), (125059, '120.0', 'g', 4), (423549, '120.0', 'g', 4),
    (615954, '120.0', 'g', 4), (621305, '120.0', 'g', 4), (627179, '120.0', 'g', 4),
    (631754, '120.0', 'g', 4), (644684, '380.0', 'g', 3), (623160, '1000.0', 'g', 1),
    (627193, '1000.0', 'g', 1), (644569, '1000.0', 'g', 1), (343188, '180.0', 'g', 1),
    (379303, '500.0', 'ml', 1), (581947, '380.0', 'g', 1), (638206, '400.0', 'g', 1),
    (631867, '1000.0', 'g', 1), (423594, '1000.0', 'g', 1), (616846, '1000.0', 'g', 1),
    (657501, '165.0', 'g', 1), (655537, '235.0', 'g', 1), (656979, '150.0', 'g', 1),
    (654799, '500.0', 'ml', 1), (126864, '250.0', 'ml', 1), (622406, '250.0', 'ml', 1),
    (660012, '250.0', 'ml', 1), (646480, '500.0', 'ml', 1), (56061, '500.0', 'ml', 1),
    (622456, '200.0', 'ml', 1), (633553, '1000.0', 'ml', 1), (639171, '1000.0', 'ml', 1),
    (108670, '500.0', 'ml', 1), (377242, '1000.0', 'ml', 1), (343478, '500.0', 'ml', 1),
    (124586, '500.0', 'ml', 1), (622618, '500.0', 'ml', 1), (645885, '500.0', 'ml', 1),
    (375657, '500.0', 'ml', 1), (656595, '750.0', 'ml', 1), (660503, '500.0', 'ml', 1),
]


class _ApplyAborted(Exception):
    pass


async def run_apply(commit: bool):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("VIGA: DATABASE_URL keskkonnamuutuja puudub.", file=sys.stderr)
        sys.exit(1)

    if not APPLY_ROWS:
        print("VIGA: APPLY_ROWS on tühi. Täida see net_qty_backfill_preview.json "
              "WOULD_SET_* ridadega enne käivitamist.", file=sys.stderr)
        sys.exit(1)

    conn = await asyncpg.connect(database_url)
    rollback_log = []
    applied = 0
    skipped_already_set = 0

    try:
        async with conn.transaction():
            for product_id, net_qty, net_unit, pack_count in APPLY_ROWS:
                before = await conn.fetchrow(
                    "SELECT id, name, net_qty, net_unit, pack_count "
                    "FROM products WHERE id = $1",
                    product_id,
                )
                if before is None:
                    print(f"HOIATUS: product_id={product_id} ei leitud, vahele jäetud.")
                    continue

                result = await conn.execute(
                    """
                    UPDATE products
                    SET net_qty = $1, net_unit = $2, pack_count = $3
                    WHERE id = $4 AND net_qty IS NULL
                    """,
                    float(net_qty), net_unit, pack_count, product_id,
                )
                rows_affected = int(result.split()[-1])

                if rows_affected == 0:
                    skipped_already_set += 1
                    print(
                        f"HOIATUS: product_id={product_id} ('{before['name']}') "
                        f"net_qty pole enam NULL (praegu {before['net_qty']} "
                        f"{before['net_unit']}) — ei kirjutatud üle."
                    )
                    continue

                applied += 1
                rollback_log.append({
                    "product_id": product_id,
                    "name": before["name"],
                    "before": {
                        "net_qty": None, "net_unit": None, "pack_count": before["pack_count"],
                    },
                    "after": {
                        "net_qty": net_qty, "net_unit": net_unit, "pack_count": pack_count,
                    },
                })
                print(f"OK: product_id={product_id} ('{before['name']}') -> "
                      f"net_qty={net_qty}, net_unit={net_unit}, pack_count={pack_count}")

            print(f"\n{'='*70}\nKOKKUVÕTE\n{'='*70}")
            print(f"Kirjutatud: {applied}")
            print(f"Vahele jäetud (juba täidetud): {skipped_already_set}")
            print(f"Kokku APPLY_ROWS: {len(APPLY_ROWS)}")

            if not commit:
                print("\n--commit LIPPU EI ANTUD — ROLLBACK, midagi ei salvestatud.")
                raise _ApplyAborted()

            print("\n--commit ANTUD — muudatused salvestatakse (COMMIT).")

    except _ApplyAborted:
        pass
    finally:
        await conn.close()

    if rollback_log:
        with open("net_qty_backfill_rollback_log.json", "w", encoding="utf-8") as f:
            json.dump(rollback_log, f, indent=2, ensure_ascii=False)
        print(f"\nRollback-logi salvestatud: net_qty_backfill_rollback_log.json "
              f"({len(rollback_log)} rida)")
        print(
            "Rollback (kui vajalik): "
            "UPDATE products SET net_qty=NULL, net_unit=NULL, pack_count=NULL "
            "WHERE id = ANY(ARRAY[...product_id'd rollback-logist...]);"
        )


if __name__ == "__main__":
    commit_flag = "--commit" in sys.argv
    asyncio.run(run_apply(commit=commit_flag))

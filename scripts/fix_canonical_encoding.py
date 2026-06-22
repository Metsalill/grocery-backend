#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parandab canonical_name-des eesti tapitahtede puudumise.
"""
import psycopg2
import os

REPLACEMENTS = [
    # oun -> õun (juba tehtud aga kontroll)
    ('oun', '\u00f5un'),
    ('Oun', '\u00d5un'),
    # rost -> röst
    ('rost', 'r\u00f6st'),
    ('Rost', 'R\u00f6st'),
    # maaare -> määre
    ('maaare', 'm\u00e4\u00e4re'),
    # taistera -> täistera
    ('taistera', 't\u00e4istera'),
    ('Taistera', 'T\u00e4istera'),
    # lohna -> lõhna
    ('lohna', 'l\u00f5hna'),
    ('Lohna', 'L\u00f5hna'),
    # lohnaline -> lõhnaline
    ('lohnaline', 'l\u00f5hnaline'),
    # louna -> lõuna
    ('louna', 'l\u00f5una'),
    ('Louna', 'L\u00f5una'),
    # saslokk -> šašlõkk (pikemad enne lühemaid!)
    ('saslokk', '\u0161a\u0161l\u00f5kk'),
    ('Saslokk', '\u0160a\u0161l\u00f5kk'),
    ('saslik', '\u0161a\u0161l\u00f5kk'),
    ('Saslik', '\u0160a\u0161l\u00f5kk'),
    # aadika -> äädika
    ('aadika', '\u00e4\u00e4dika'),
    ('Aadika', '\u00c4\u00e4dika'),
    # olu -> õlu (ainult eraldi sõnana — "olu " ja " olu")
    (' olu ', ' \u00f5lu '),
    (' olu\n', ' \u00f5lu\n'),
    ('Olu ', '\u00d5lu '),
    # olut -> EI MUUDA — Absolut, koolutaja, Coconut sisaldavad olut
    # olle -> EI MUUDA — Danerolles, Stollen, Holle, Collection sisaldavad olle
]

# Sonad mida EI tohi muuta (inglise/prantsuse brandid)
SKIP_WORDS = [
    'bonjour', 'rostov', 'rostbeef', 'rostock',
]

conn = psycopg2.connect(os.environ['DATABASE_URL'])
conn.set_client_encoding('UTF8')
cur = conn.cursor()
cur.execute("SELECT id, canonical_name FROM product_groups ORDER BY id")
rows = cur.fetchall()
print(f"Kokku {len(rows)} gruppi")
updated = 0

for group_id, name in rows:
    if not name:
        continue
    new_name = name

    # Kontrolli kas sisaldab skip soanu
    skip = False
    for sw in SKIP_WORDS:
        if sw in name.lower():
            skip = True
            break
    if skip:
        continue

    for old, new in REPLACEMENTS:
        new_name = new_name.replace(old, new)

    if new_name != name:
        cur.execute(
            "UPDATE product_groups SET canonical_name = %s WHERE id = %s",
            (new_name, group_id)
        )
        print(f"  [{group_id}] {name} -> {new_name}")
        updated += 1

conn.commit()
print(f"\nUuendatud: {updated} gruppi")

# Refresh materialized view
print("Refreshin mv_group_chains...")
cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_group_chains")
conn.commit()
print("Valmis!")

cur.close()
conn.close()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parandab canonical_name-des eesti täpitähtede puudumise.
"""
import psycopg2
import os

REPLACEMENTS = [
    # oun -> õun (juba tehtud aga kontroll)
    ('oun', 'õun'),
    ('Oun', 'Õun'),
    # rost -> röst
    ('rost', 'röst'),
    ('Rost', 'Röst'),
    # maaare -> määre
    ('maaare', 'määre'),
    # taistera -> täistera
    ('taistera', 'täistera'),
    ('Taistera', 'Täistera'),
    # lohna -> lõhna
    ('lohna', 'lõhna'),
    ('Lohna', 'Lõhna'),
    # lohnaline -> lõhnaline
    ('lohnaline', 'lõhnaline'),
    # louna -> lõuna
    ('louna', 'lõuna'),
    ('Louna', 'Lõuna'),
    # pohla -> pohla (pohlakas on õige, pohla = pohlane mari)
    # kohupiim -> ok
    # sulatatud -> ok
    # johupiimahorgutis -> jogurtikohupiim?
    # maitsestamata -> ok
    # laktoosivaba -> ok
]

# Sõnad mida EI tohi muuta (inglise/prantsuse brändid)
SKIP_WORDS = [
    'bonjour', 'rostov', 'rostbeef', 'rostock',
]

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("SELECT id, canonical_name FROM product_groups ORDER BY id")
rows = cur.fetchall()
print(f"Kokku {len(rows)} gruppi")

updated = 0
for group_id, name in rows:
    new_name = name
    
    # Kontrolli kas sisaldab skip sõnu
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
cur.close()
conn.close()

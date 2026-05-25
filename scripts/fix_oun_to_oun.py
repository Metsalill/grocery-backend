#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Asendab canonical_name-des 'oun' -> 'õun' (v.a ingliskeelsed sõnad)
"""
import psycopg2
import os
import re

SKIP_PATTERNS = [
    'ound', 'ounts', 'ounce', 'mountain', 'bounty', 'bouncy',
    'country', 'countdown', 'encounter', 'surrounding', 'Mountain',
    'Bounty', 'Country'
]

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Leia kõik grupid kus 'oun' on nimes
cur.execute("""
    SELECT id, canonical_name FROM product_groups
    WHERE canonical_name ILIKE '%oun%'
    AND canonical_name NOT ILIKE '%ound%'
    AND canonical_name NOT ILIKE '%ounts%'
    AND canonical_name NOT ILIKE '%ounce%'
    AND canonical_name NOT ILIKE '%mountain%'
    AND canonical_name NOT ILIKE '%bounty%'
    AND canonical_name NOT ILIKE '%bouncy%'
    AND canonical_name NOT ILIKE '%country%'
    AND canonical_name NOT ILIKE '%countdown%'
    AND canonical_name NOT ILIKE '%encounter%'
    AND canonical_name NOT ILIKE '%surrounding%'
    ORDER BY canonical_name
""")

rows = cur.fetchall()
print(f"Leitud {len(rows)} gruppi")

updated = 0
for group_id, name in rows:
    # Asenda 'oun' -> 'õun' ja 'Oun' -> 'Õun'
    new_name = name.replace('Oun', 'Õun').replace('oun', 'õun')
    
    if new_name != name:
        cur.execute(
            "UPDATE product_groups SET canonical_name = %s WHERE id = %s",
            (new_name, group_id)
        )
        updated += 1

conn.commit()
print(f"Uuendatud: {updated} gruppi")
cur.close()
conn.close()

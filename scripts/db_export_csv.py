#!/usr/bin/env python3
import os
import csv
import psycopg2
import sys

if len(sys.argv) != 2:
    print("Usage: python scripts/db_export_csv.py output.csv")
    sys.exit(1)

out_file = sys.argv[1]

sql = """
SELECT ean, name, product_name, amount, brand, manufacturer,
       country_of_manufacture, category_1, category_2, category_3,
       image_url, source_url, last_seen_utc
FROM products
WHERE source_url LIKE '%prismamarket.ee%'
ORDER BY last_seen_utc DESC
"""

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute(sql)
cols = [d[0] for d in cur.description]

with open(out_file, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(cols)
    for row in cur:
        w.writerow(row)

cur.close()
conn.close()
print(f"Exported to {out_file}")

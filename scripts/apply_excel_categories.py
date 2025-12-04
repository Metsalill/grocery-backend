# scripts/apply_excel_categories.py

import csv
import os
from pathlib import Path

import psycopg2
import psycopg2.extras


# Directory where the 6 CSVs live.
# Can be overridden by env LABELS_DIR if you want.
LABELS_DIR = Path(os.getenv("LABELS_DIR", "data/product_labels"))

CSV_FILENAMES = [
    "products_dairy_eggs_fats_with_corrected_groups_and_subcodes.csv",
    "products_drinks_with_corrected_groups_and_subcodes.csv",
    "products_dry_preserves_with_corrected_groups_and_subcodes.csv",
    "products_frozen_food_with_corrected_groups_and_subcodes.csv",
    "products_meat_fish_with_corrected_groups_and_subcodes.csv",
    "products_bakery_with_corrected_groups_and_subcodes.csv",
]

# Set to True for a dry-run that only prints a sample of mappings
DRY_RUN = False


def load_labels() -> dict[int, tuple[str, str]]:
    """
    Read all CSV files and build a mapping:
        product_id -> (food_group_corrected, new_sub_code)

    - Skips rows without product_id, food_group_corrected or new_sub_code.
    - If the same product_id appears multiple times with different labels,
      the last one wins (and a warning is printed).
    """
    mapping: dict[int, tuple[str, str]] = {}
    total_rows = 0
    missing_fg_or_sub = 0

    for fname in CSV_FILENAMES:
        path = LABELS_DIR / fname
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        print(f"Reading {path} ...")
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                raw_id = (row.get("product_id") or "").strip()
                if not raw_id:
                    continue

                try:
                    product_id = int(raw_id)
                except ValueError:
                    print(f"  ! Skipping row with non-integer product_id: {raw_id}")
                    continue

                fg = (row.get("food_group_corrected") or "").strip()
                if not fg:
                    # fallback to existing food_group column if corrected missing
                    fg = (row.get("food_group") or "").strip()

                sub = (row.get("new_sub_code") or "").strip()

                if not fg or not sub:
                    missing_fg_or_sub += 1
                    continue

                new_value = (fg, sub)
                old_value = mapping.get(product_id)
                if old_value and old_value != new_value:
                    print(
                        f"  ! WARNING: product_id {product_id} has conflicting labels "
                        f"{old_value} vs {new_value}. Using {new_value}."
                    )

                mapping[product_id] = new_value

    print(f"\nTotal CSV rows read: {total_rows}")
    print(f"Rows skipped due to missing food_group/sub_code: {missing_fg_or_sub}")
    print(f"Unique product_ids with labels: {len(mapping)}")
    return mapping


def main() -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    print(f"Using LABELS_DIR = {LABELS_DIR.resolve()}")
    mapping = load_labels()
    if not mapping:
        print("No mappings loaded from CSVs, exiting.")
        return

    if DRY_RUN:
        print("\nDRY RUN: showing first 15 mappings:")
        for i, (pid, (fg, sub)) in enumerate(mapping.items()):
            print(f"  id={pid} -> food_group={fg}, sub_code={sub}")
            if i >= 14:
                break
        print("No database changes made (DRY_RUN=True).")
        return

    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    cur = conn.cursor()

    # Make sure sub_code column exists (safe, idempotent)
    print("\nEnsuring products.sub_code column exists ...")
    cur.execute(
        """
        ALTER TABLE products
        ADD COLUMN IF NOT EXISTS sub_code text;
        """
    )
    conn.commit()

    # Check how many of the product_ids actually exist in products
    id_list = list(mapping.keys())
    print(f"\nChecking how many of these IDs exist in products ({len(id_list)} total) ...")
    cur.execute("SELECT COUNT(*) FROM products WHERE id = ANY(%s);", (id_list,))
    (existing_count,) = cur.fetchone()
    missing_count = len(id_list) - existing_count
    print(f"Canonical products found: {existing_count}")
    print(f"product_ids from CSV missing in products: {missing_count}")

    # Prepare batched UPDATE
    update_sql = """
        UPDATE products
        SET food_group = %s,
            sub_code   = %s
        WHERE id = %s;
    """
    params = [(fg, sub, pid) for pid, (fg, sub) in mapping.items()]

    print("\nUpdating products table ...")
    psycopg2.extras.execute_batch(cur, update_sql, params, page_size=1000)
    conn.commit()

    # Post-update sanity check: how many of these ids now have sub_code set?
    cur.execute(
        """
        SELECT COUNT(*)
        FROM products
        WHERE id = ANY(%s)
          AND sub_code IS NOT NULL;
        """,
        (id_list,),
    )
    (updated_with_subcode,) = cur.fetchone()

    cur.close()
    conn.close()

    print("\nDone.")
    print(f"Intended updates (unique product_ids in CSV): {len(mapping)}")
    print(f"Rows that now have sub_code among those IDs: {updated_with_subcode}")


if __name__ == "__main__":
    main()

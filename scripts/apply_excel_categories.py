#!/usr/bin/env python
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# <-- THIS is your existing folder with the Excel-edited files
LABEL_DIR = Path("data/product_labels")
PATTERN = "products_*_with_corrected_groups_and_subcodes*.csv"


def load_updates_from_file(path: Path):
    print(f"Reading {path} ...")
    df = pd.read_csv(path)

    if "product_id" not in df.columns:
        raise RuntimeError(f"{path} is missing 'product_id' column")

    # food_group_corrected: what we want to write to products.food_group
    if "food_group_corrected" not in df.columns:
        if "food_group" in df.columns:
            df["food_group_corrected"] = df["food_group"]
        else:
            raise RuntimeError(
                f"{path} is missing 'food_group_corrected' (or 'food_group') column"
            )

    # new_sub_code: what we want to write to products.sub_code
    if "new_sub_code" not in df.columns:
        if "sub_code" in df.columns:
            df["new_sub_code"] = df["sub_code"]
        else:
            df["new_sub_code"] = ""

    df["food_group_corrected"] = (
        df["food_group_corrected"].fillna("").astype(str).str.strip()
    )
    df["new_sub_code"] = df["new_sub_code"].fillna("").astype(str).str.strip()

    updates = []
    for _, r in df.iterrows():
        pid = int(r["product_id"])
        fg = r["food_group_corrected"] or None
        sub = r["new_sub_code"] or None
        updates.append((pid, fg, sub))

    # "Not certain" = still 'other' OR sub_code empty
    uncertain_mask = (df["food_group_corrected"].eq("other")) | (
        df["new_sub_code"].eq("")
    )
    uncertain = df.loc[uncertain_mask].copy()
    uncertain["source_file"] = path.name

    print(
        f"  -> {len(df)} rows, {len(updates)} updates, "
        f"{len(uncertain)} marked as uncertain"
    )

    return updates, uncertain


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL env var is not set", file=sys.stderr)
        sys.exit(1)

    LABEL_DIR.mkdir(exist_ok=True)

    files = sorted(LABEL_DIR.glob(PATTERN))
    if not files:
        print(f"No files matching {LABEL_DIR}/{PATTERN}, nothing to do.")
        return

    all_updates = []
    uncertain_chunks = []

    for path in files:
        updates, uncertain = load_updates_from_file(path)
        all_updates.extend(updates)
        if not uncertain.empty:
            uncertain_chunks.append(uncertain)

    print(f"\nTotal prepared updates: {len(all_updates)} from {len(files)} files\n")

    # --- apply updates to products ---
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            template = "(%s::integer, %s::text, %s::text)"
            psycopg2.extras.execute_values(
                cur,
                """
                UPDATE products AS p
                SET food_group = v.food_group,
                    sub_code   = v.sub_code
                FROM (VALUES %s) AS v(product_id, food_group, sub_code)
                WHERE p.id = v.product_id;
                """,
                all_updates,
                template=template,
            )
        conn.commit()
        print("✅ DB update committed.")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR, rolled back: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

    # --- post-update stats ---
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products;")
            total = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM products
                WHERE food_group IS NULL OR food_group = '' OR food_group = 'other';
                """
            )
            without_fg = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM products WHERE sub_code IS NULL;"
            )
            without_sub = cur.fetchone()[0]

        print("\n-- Post-update stats --")
        print(f"total_products             = {total}")
        print(f"without_food_group/other   = {without_fg}")
        print(f"without_sub_code           = {without_sub}")
    finally:
        conn.close()

    # --- write “not certain” file ---
    if uncertain_chunks:
        uncertain_df = pd.concat(uncertain_chunks, ignore_index=True)
        out_path = LABEL_DIR / "products_uncertain_after_apply.csv"
        uncertain_df.to_csv(out_path, index=False)
        print(f"\n📄 Wrote {len(uncertain_df)} uncertain rows to {out_path}")
    else:
        print("\nNo uncertain rows collected.")


if __name__ == "__main__":
    main()

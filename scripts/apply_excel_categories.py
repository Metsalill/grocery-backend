#!/usr/bin/env python
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

LABEL_DIR = Path("data/product_labels")
PATTERN = "products_all_part*_verified_online_rowbyrow_mincols.csv"


def load_updates_from_file(path: Path):
    print(f"Reading {path} ...")
    df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines='warn', engine='python')

    required = {"product_id", "canonical_main_code", "canonical_sub_code"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{path} is missing columns: {missing}")

    df["canonical_main_code"] = (
        df["canonical_main_code"].fillna("").astype(str).str.strip()
    )
    df["canonical_sub_code"] = (
        df["canonical_sub_code"].fillna("").astype(str).str.strip()
    )

    updates = []
    skipped_pid = []
    for _, r in df.iterrows():
        try:
            pid = int(r["product_id"])
        except (ValueError, TypeError):
            skipped_pid.append(str(r["product_id"])[:60])
            continue
        main = r["canonical_main_code"] or None
        sub = r["canonical_sub_code"] or None
        updates.append((pid, main, sub))

    if skipped_pid:
        print(f"  ⚠️  Skipped {len(skipped_pid)} rows with non-integer product_id: {skipped_pid[:3]}")

    # Flag rows where either code is missing/empty as uncertain
    uncertain_mask = (
        df["canonical_main_code"].eq("")
        | df["canonical_sub_code"].eq("")
        | df["canonical_main_code"].isnull()
        | df["canonical_sub_code"].isnull()
    )
    uncertain = df.loc[uncertain_mask].copy()
    uncertain["source_file"] = path.name

    print(
        f"  -> {len(df)} rows, {len(updates)} updates, "
        f"{len(uncertain)} uncertain (missing main or sub code)"
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
        print("Files present in directory:")
        for f in sorted(LABEL_DIR.iterdir()):
            print(f"  {f.name}")
        return

    print(f"Found {len(files)} files to process:\n")
    for f in files:
        print(f"  {f.name}")
    print()

    all_updates = []
    uncertain_chunks = []

    for path in files:
        updates, uncertain = load_updates_from_file(path)
        all_updates.extend(updates)
        if not uncertain.empty:
            uncertain_chunks.append(uncertain)

    print(f"\nTotal prepared updates: {len(all_updates)} from {len(files)} files\n")

    # --- diagnose ID matching before committing ---
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'products'
                ORDER BY ordinal_position
                LIMIT 10;
            """)
            cols = [row[0] for row in cur.fetchall()]
            print(f"products table columns (first 10): {cols}")

            cur.execute("SELECT id FROM products LIMIT 5;")
            db_ids = [row[0] for row in cur.fetchall()]
            print(f"Sample DB product IDs: {db_ids}")

            sample_file_ids = [u[0] for u in all_updates[:5]]
            print(f"Sample file product_ids: {sample_file_ids}")

            file_ids = [u[0] for u in all_updates]
            cur.execute(
                "SELECT COUNT(*) FROM products WHERE id = ANY(%s::integer[])",
                (file_ids,)
            )
            matched = cur.fetchone()[0]
            print(f"File IDs that exist in DB: {matched} out of {len(file_ids)}\n")
    finally:
        conn.close()

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
            updated = cur.rowcount
        conn.commit()
        print(f"✅ DB update committed. Rows matched and updated: {updated}")
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
                WHERE food_group IS NULL OR food_group = '';
                """
            )
            without_fg = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM products
                WHERE sub_code IS NULL OR sub_code = '';
                """
            )
            without_sub = cur.fetchone()[0]

            cur.execute(
                """
                SELECT food_group, COUNT(*) as n
                FROM products
                WHERE food_group IS NOT NULL AND food_group != ''
                GROUP BY food_group
                ORDER BY n DESC
                LIMIT 20;
                """
            )
            top_groups = cur.fetchall()

        print("\n-- Post-update stats --")
        print(f"total_products           = {total}")
        print(f"without food_group       = {without_fg}")
        print(f"without sub_code         = {without_sub}")
        print(f"\nTop food groups:")
        for group, count in top_groups:
            print(f"  {group:<35} {count}")

    finally:
        conn.close()

    # --- write uncertain rows to file ---
    if uncertain_chunks:
        uncertain_df = pd.concat(uncertain_chunks, ignore_index=True)
        out_path = LABEL_DIR / "products_uncertain_after_apply.csv"
        uncertain_df.to_csv(out_path, index=False)
        print(f"\n📄 Wrote {len(uncertain_df)} uncertain rows to {out_path}")
    else:
        print("\n✅ No uncertain rows — all products have both codes assigned.")


if __name__ == "__main__":
    main()

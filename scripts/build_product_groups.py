#!/usr/bin/env python3
"""
scripts/build_product_groups.py

Clusters kg-priced produce products into canonical groups.
Run once (and re-run safely — it clears and rebuilds groups each time).

Usage:
  DATABASE_URL=... python scripts/build_product_groups.py
"""

import asyncio
import os
import re
import asyncpg
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Chain-specific prefixes to strip when normalizing names
CHAIN_PREFIXES = [
    r"^https\s+barbora\s+ee\s+toode\s+",
    r"^coop\s+",
    r"^rimi\s+",
    r"^selver\s+",
    r"^prisma\s+",
    r"^maxima\s+",
    r"^lidl\s+",
]

# Size/class suffixes that differ between chains but mean the same product
# e.g. "1 kl kg", "1kl., kg", "1 Kl Kg", "I klass" → strip these
SIZE_CLASS_PATTERNS = [
    r"\s+\d+\s*kl\.?,?\s*kg$",   # "1 kl., kg", "1kl kg"
    r"\s+i+\s+klass?\b",          # "I klass", "II klass"
    r"\s+\d+\s*kg$",              # trailing "2 kg", "1 kg"
    r",\s*kg$",                   # trailing ", kg"
    r"\s+kg$",                    # trailing " kg"
    r"\s+kl\s+kg$",               # "kl kg"
    r"\s+pakitud.*$",             # "pakitud ..."
]


def normalize(name: str) -> str:
    """
    Strip chain prefixes, size/class suffixes, and normalize whitespace.
    Returns a lowercased canonical key for grouping.
    """
    s = name.strip().lower()

    # Strip chain prefixes
    for pat in CHAIN_PREFIXES:
        s = re.sub(pat, "", s, flags=re.IGNORECASE)

    # Strip size/class suffixes (apply repeatedly until stable)
    for _ in range(3):
        prev = s
        for pat in SIZE_CLASS_PATTERNS:
            s = re.sub(pat, "", s, flags=re.IGNORECASE).strip()
        if s == prev:
            break

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


async def main():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not set")
        return

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        print("Fetching kg produce products...")
        rows = await conn.fetch("""
            SELECT id, name, sub_code
            FROM products
            WHERE sub_code LIKE 'produce_%'
              AND (
                name ILIKE '% kg%'
                OR name ILIKE '%, kg'
                OR name ILIKE '%kl kg%'
                OR name ILIKE '%kl., kg%'
                OR name ILIKE '% Kg'
              )
            ORDER BY sub_code, name
        """)
        print(f"Found {len(rows)} kg produce products")

        # Group by (normalized_name, sub_code)
        clusters: dict[tuple[str, str], list[int]] = defaultdict(list)
        for r in rows:
            key = (normalize(r["name"]), r["sub_code"] or "")
            clusters[key].append(r["id"])

        # Only keep groups with 2+ members (single-member groups don't need grouping)
        multi = {k: v for k, v in clusters.items() if len(v) >= 2}
        single = {k: v for k, v in clusters.items() if len(v) == 1}

        print(f"Groups with 2+ members: {len(multi)}")
        print(f"Single-member (skipped): {len(single)}")

        # Show preview
        print("\nSample groups:")
        for i, ((canon, sub), pids) in enumerate(list(multi.items())[:10]):
            print(f"  [{sub}] '{canon}' → {pids}")

        # Confirm before writing
        answer = input(f"\nInsert {len(multi)} groups into DB? (y/n): ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

        # Clear existing groups and rebuild
        print("Clearing existing product_group_members and product_groups...")
        await conn.execute("DELETE FROM product_group_members")
        await conn.execute("DELETE FROM product_groups")

        inserted_groups = 0
        inserted_members = 0

        async with conn.transaction():
            for (canon, sub), pids in multi.items():
                # Insert group
                group_id = await conn.fetchval("""
                    INSERT INTO product_groups (canonical_name, sub_code, unit)
                    VALUES ($1, $2, 'kg')
                    RETURNING id
                """, canon, sub or None)

                # Insert members
                for pid in pids:
                    await conn.execute("""
                        INSERT INTO product_group_members (group_id, product_id)
                        VALUES ($1, $2)
                        ON CONFLICT DO NOTHING
                    """, group_id, pid)
                    inserted_members += 1

                inserted_groups += 1

        print(f"\nDone! Inserted {inserted_groups} groups, {inserted_members} member mappings.")

        # Verification
        count = await conn.fetchval("SELECT COUNT(*) FROM product_groups")
        print(f"product_groups table now has {count} rows.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

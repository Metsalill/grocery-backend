#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delete unsupported/bad product images from Cloudflare R2 and (optionally) clear DB URLs.

Heuristics (delete if any is true):
- Content-Type is not one of: image/jpeg, image/png, image/webp, image/gif
- OR Content-Type starts with "text/" (e.g. HTML anti-hotlink pages)
- OR file is tiny (< 1 KB) which strongly suggests an error page
- OR magic bytes don't match JPEG/PNG/WEBP/GIF (checked from the first 32 bytes)

By default runs in DRY-RUN mode (won't delete). Pass --dry-run 0 to actually delete.
Pass --clear-db 1 to set products.image_url='' for deleted objects.

Usage:
  python scripts/r2_delete_unsupported.py --prefix products/prisma/ --dry-run 1
  python scripts/r2_delete_unsupported.py --prefix products/prisma/ --dry-run 0 --clear-db 1
"""
from __future__ import annotations

import argparse
import sys
import io
from typing import List, Tuple
import boto3
from botocore.exceptions import ClientError
import psycopg2
import psycopg2.extras

from settings import (
    DATABASE_URL, DB_CONNECT_TIMEOUT,
    R2_BUCKET, R2_S3_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_REGION,
    r2_public_url,
)

ALLOWED_CT = {"image/jpeg", "image/png", "image/webp", "image/gif"}

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=R2_S3_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION or "auto",
    )

def db_connect():
    return psycopg2.connect(DATABASE_URL, connect_timeout=int(DB_CONNECT_TIMEOUT))

def looks_like_image_magic(head: bytes) -> bool:
    # JPEG
    if head.startswith(b"\xFF\xD8\xFF"): return True
    # PNG
    if head.startswith(b"\x89PNG\r\n\x1a\n"): return True
    # GIF
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"): return True
    # WEBP (RIFF....WEBP)
    if head.startswith(b"RIFF") and b"WEBP" in head[:16]: return True
    return False

def head_bytes(s3, key: str) -> bytes:
    try:
        obj = s3.get_object(Bucket=R2_BUCKET, Key=key, Range="bytes=0-31")
        return obj["Body"].read(32)
    except Exception:
        return b""

def should_delete(s3, key: str) -> Tuple[bool, str]:
    """Return (delete?, reason)."""
    try:
        h = s3.head_object(Bucket=R2_BUCKET, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return True, "missing (404)"
        return False, f"head-failed: {e}"

    ct = (h.get("ContentType") or "").lower()
    size = h.get("ContentLength") or 0

    if ct.startswith("text/"):
        return True, f"bad content-type: {ct}"
    if ct not in ALLOWED_CT:
        # final chance: inspect magic bytes for real image
        hb = head_bytes(s3, key)
        if not looks_like_image_magic(hb):
            return True, f"unsupported content-type: {ct}"
    if size < 1024:  # 1 KB
        return True, f"suspiciously small: {size} bytes"

    # seems fine
    return False, f"ok ({ct}, {size}B)"

def list_keys(s3, prefix: str) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        kw = {"Bucket": R2_BUCKET, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for item in resp.get("Contents", []):
            keys.append(item["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys

def clear_db_urls(keys: List[str]):
    if not keys:
        return
    conn = db_connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        for k in keys:
            url = r2_public_url(k)
            cur.execute("UPDATE products SET image_url='' WHERE image_url = %s", (url,))
    conn.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="products/prisma/", help="Key prefix to scan")
    ap.add_argument("--dry-run", type=int, default=1, help="1=preview only, 0=delete")
    ap.add_argument("--clear-db", type=int, default=0, help="Also blank products.image_url for deleted objects")
    args = ap.parse_args()

    # sanity
    for v in (R2_BUCKET, R2_S3_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY):
        if not v:
            print("R2 is not configured. Check Railway variables.", file=sys.stderr)
            sys.exit(2)

    s3 = get_s3()
    keys = list_keys(s3, args.prefix)
    if not keys:
        print(f"No objects under prefix '{args.prefix}'.")
        return

    print(f"Scanning {len(keys)} objects under '{args.prefix}' ...")
    to_delete: List[str] = []
    kept = 0

    for k in keys:
        delete, reason = should_delete(s3, k)
        if delete:
            print(f"[DEL] {k}  -- {reason}")
            to_delete.append(k)
        else:
            kept += 1

    print(f"\nSummary: {len(to_delete)} to delete, {kept} keep.")

    if not to_delete:
        return

    if args.dry_run:
        print("\nDRY-RUN: nothing deleted. Re-run with --dry-run 0 to apply.")
        return

    # delete in batches of 900 (API limit is 1000 per call)
    for i in range(0, len(to_delete), 900):
        batch = to_delete[i:i+900]
        s3.delete_objects(
            Bucket=R2_BUCKET,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
        )
        print(f"Deleted {len(batch)} objects.")

    if args.clear_db:
        clear_db_urls(to_delete)
        print("Cleared image_url in DB for deleted objects.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mirror existing Prisma-hosted product images → Cloudflare R2

- Selects products whose image_url is non-empty AND hosted on *.prismamarket.ee
  and NOT already on our R2 public base.
- Downloads the image, uploads to R2 as <R2_PREFIX>prisma/<ean or id>.<ext>
- Updates products.image_url to our public R2 URL
- Skips if object key already exists in R2 (HEAD succeeds), unless --overwrite 1

Backfill phase:
  Default limit is 6000 to sweep everything.
  When you’re done backfilling, drop to ~200 per run (set env MIRROR_LIMIT=200 or pass --limit 200).

Run:
  python scripts/mirror_existing_images_to_r2.py --limit 6000 --overwrite 0
"""
from __future__ import annotations

import sys, os, re, time, random
from typing import Optional, Tuple, List, Dict
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timezone
import mimetypes

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn
import httpx
import boto3
from botocore.exceptions import ClientError

from settings import (
    DATABASE_URL, DB_CONNECT_TIMEOUT,
    R2_BUCKET, R2_PREFIX, R2_PUBLIC_BASE, R2_S3_ENDPOINT,
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_REGION,
    r2_public_url,
)

PRISMA_DOMAIN_RE = re.compile(r"https?://([a-z0-9-]+\.)*prismamarket\.ee\b", re.IGNORECASE)

# Backfill default: 6000 (set MIRROR_LIMIT=200 later for steady-state)
ENV_DEFAULT_LIMIT = int(os.getenv("MIRROR_LIMIT", "6000"))
BATCH_SIZE = int(os.getenv("MIRROR_BATCH", "500"))  # number of rows fetched per DB page

mimetypes.init()

def jitter(a=0.6, b=1.4): 
    time.sleep(random.uniform(a, b))

# ---------- MIME / extension helpers (AVIF/WebP aware) ----------
def _ext_from_url(url: str) -> str:
    if not url:
        return ""
    stem = url.split("?", 1)[0].split("#", 1)[0]
    if "." in stem:
        ext = stem.rsplit(".", 1)[1].lower()
        return "jpg" if ext == "jpeg" else ext
    return ""

def guess_ext_from_mime(m: str, url_hint: str = "") -> str:
    """
    Map common image content-types to a sensible extension.
    If mime is unknown, try guessing from the URL; finally default to jpg.
    """
    m = (m or "").lower()
    if "image/jpeg" in m or m == "image/jpg" or "jpeg" in m:
        return "jpg"
    if "image/png" in m or m.endswith("/png") or "png" in m:
        return "png"
    if "image/webp" in m or "webp" in m:
        return "webp"
    if "image/avif" in m or "avif" in m:
        return "avif"
    if "image/gif" in m or "gif" in m:
        return "gif"
    # try URL
    url_ext = _ext_from_url(url_hint)
    if url_ext in {"jpg", "png", "webp", "avif", "gif"}:
        return url_ext
    return "jpg"

def choose_content_type(server_ct: str, ext: str) -> str:
    """
    Prefer server-provided content-type; else derive from ext; fallback to image/jpeg.
    """
    ct = (server_ct or "").lower().strip()
    if ct.startswith("image/"):
        return ct
    if ext:
        guessed = mimetypes.types_map.get("." + ext)
        if guessed and guessed.startswith("image/"):
            return guessed
    return "image/jpeg"
# ---------------------------------------------------------------

def db_connect() -> PGConn:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=int(DB_CONNECT_TIMEOUT))
    conn.autocommit = True
    return conn

def select_to_mirror(conn: PGConn, limit: int) -> List[Dict]:
    """
    Pick rows with a non-empty image_url that:
      - are hosted on *.prismamarket.ee
      - are NOT already on our R2 public base
    """
    r2base = (R2_PUBLIC_BASE or "").strip()
    r2like = f"%{r2base}%" if r2base else ""

    sql = """
        SELECT id, ean, product_name, image_url
        FROM products
        WHERE image_url IS NOT NULL
          AND image_url <> ''
          -- only prisma domain (any subdomain)
          AND image_url ~* '://([a-z0-9-]+\\.)*prismamarket\\.ee'
          -- not already on our R2
          AND (%(r2base)s = '' OR image_url NOT ILIKE %(r2like)s)
        ORDER BY id
        LIMIT %(limit)s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, {"r2base": r2base, "r2like": r2like, "limit": limit})
        return [dict(r) for r in cur.fetchall()]

def update_image_url(conn: PGConn, pid: int, url: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE products SET image_url = %s, last_seen_utc = %s WHERE id = %s",
            (url, datetime.now(timezone.utc), pid),
        )

def get_r2_client():
    if not (R2_S3_ENDPOINT and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_BUCKET):
        raise RuntimeError("R2 not configured")
    return boto3.client(
        "s3",
        endpoint_url=R2_S3_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION or "auto",
    )

def head_exists(client, key: str) -> bool:
    try:
        client.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise

def upload_bytes(client, data: bytes, key: str, content_type: str) -> str:
    client.put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type or "image/jpeg",
        ACL="public-read",
    )
    return r2_public_url(key)

def should_mirror(url: str) -> bool:
    if R2_PUBLIC_BASE and url.startswith(R2_PUBLIC_BASE):
        return False
    return bool(PRISMA_DOMAIN_RE.match(url))

def mirror(limit: int = ENV_DEFAULT_LIMIT, overwrite: bool = False):
    conn = db_connect()
    client = get_r2_client()

    remaining = max(0, limit)
    total_done = total_skipped = total_failed = 0

    headers = {
        "Referer": "https://prismamarket.ee/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }

    with httpx.Client(timeout=25.0, follow_redirects=True, headers=headers) as http:
        batch_no = 1
        while remaining > 0:
            fetch_n = min(BATCH_SIZE, remaining)
            rows = select_to_mirror(conn, fetch_n)
            if not rows:
                if total_done + total_skipped + total_failed == 0:
                    print("Nothing to mirror (no prisma-hosted images or all already on R2).")
                break

            print(f"Batch {batch_no}: processing {len(rows)} images…")
            done = skipped = failed = 0

            for r in rows:
                pid = r["id"]
                ean = (r.get("ean") or "").strip()
                src = r["image_url"]

                if not src or not should_mirror(src):
                    skipped += 1
                    continue

                try:
                    resp = http.get(src)
                    if resp.status_code != 200 or not resp.content:
                        print(f"[{pid}] download failed: {resp.status_code}")
                        failed += 1
                        continue

                    server_ct = resp.headers.get("content-type", "")
                    ext = guess_ext_from_mime(server_ct, src)
                    ctype = choose_content_type(server_ct, ext)

                    fname = f"{ean}.{ext}" if ean else f"id-{pid}.{ext}"
                    key = f"{R2_PREFIX}prisma/{fname}"

                    if not overwrite and head_exists(client, key):
                        public_url = r2_public_url(key)
                        update_image_url(conn, pid, public_url)
                        print(f"[{pid}] ✔ exists → updated DB only")
                        done += 1
                        jitter()
                        continue

                    public_url = upload_bytes(client, resp.content, key, ctype)
                    update_image_url(conn, pid, public_url)
                    print(f"[{pid}] ✅ mirrored → {public_url} [{ctype}]")
                    done += 1

                except Exception as e:
                    print(f"[{pid}] error: {e}")
                    failed += 1

                jitter()

            print(f"Batch {batch_no} complete. Done: {done}, Skipped: {skipped}, Failed: {failed}.")
            total_done += done
            total_skipped += skipped
            total_failed += failed

            remaining -= len(rows)
            batch_no += 1

    print(f"\nMirroring complete. Total Done: {total_done}, Skipped: {total_skipped}, Failed: {total_failed}.")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Mirror prisma-hosted product images to R2")
    ap.add_argument("--limit", type=int, default=ENV_DEFAULT_LIMIT,
                    help="Max rows to process (defaults to env MIRROR_LIMIT or 6000). "
                         "Drop to 200 after backfilling.")
    ap.add_argument("--overwrite", type=int, default=0, help="1 to re-upload even if key exists")
    args = ap.parse_args()
    mirror(limit=args.limit, overwrite=bool(args.overwrite))

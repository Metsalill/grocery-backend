# settings.py
import os
from dotenv import load_dotenv
from fastapi import Request, HTTPException
import asyncpg

load_dotenv()

# -----------------------------------------------------------------------------
# Core app settings
# -----------------------------------------------------------------------------
ENV = (os.getenv("ENV") or "development").lower()
ENABLE_DOCS = ENV != "production"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.getenv("STATIC_DIR", os.path.join(BASE_DIR, "static"))
IMAGES_DIR = os.path.join(STATIC_DIR, "images")  # local fallback/serving path

APP_WEB_ORIGIN = (os.getenv("APP_WEB_ORIGIN") or "").strip()
ALLOW_ORIGINS = [o.strip() for o in APP_WEB_ORIGIN.split(",") if o.strip()] or ["*"]

SWAGGER_USERNAME = os.getenv("SWAGGER_USERNAME")
SWAGGER_PASSWORD = os.getenv("SWAGGER_PASSWORD")

ADMIN_IP_ALLOWLIST = os.getenv("ADMIN_IP_ALLOWLIST", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL")
DB_CONNECT_TIMEOUT = float(os.getenv("DB_CONNECT_TIMEOUT", "8"))

LOG_REQUESTS = (os.getenv("LOG_REQUESTS") or "").lower() in {"1", "true", "yes"}

RATE_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MINUTE", "300"))
REDIS_URL = os.getenv("REDIS_URL")
WINDOW = 60

# Kept for backwards-compat with your current code (CDN for images hosted by you)
CDN_BASE_URL = os.getenv("CDN_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or ""

# Upload limits
MAX_UPLOAD_MB = int(os.getenv("MAX_IMAGE_MB", "6"))
VALID_IMAGE_MIME = {
    "image/jpeg", "image/png", "image/webp", "image/gif",
}

# -----------------------------------------------------------------------------
# Cloudflare R2 / S3-compatible storage
# -----------------------------------------------------------------------------
# If you set these in Railway → Variables, the app will use R2 for hosted images.
# Otherwise it can fall back to serving local files from STATIC_DIR/images.
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "").strip()
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET = os.getenv("R2_BUCKET", "").strip()

# Public base used to construct end-user image URLs (from R2's Public Development URL
# or a custom domain). Example: https://pub-xxxxxx.r2.dev  (no trailing slash)
# If you have a Cloudflare custom domain pointing to the bucket, put that here instead.
R2_PUBLIC_BASE = (os.getenv("R2_PUBLIC_BASE") or CDN_BASE_URL).rstrip("/")

# S3 API endpoint for R2. If not provided, auto-derive from the account id.
# Example: https://<accountid>.r2.cloudflarestorage.com
R2_S3_ENDPOINT = os.getenv("R2_S3_ENDPOINT", "").strip()
if not R2_S3_ENDPOINT and R2_ACCOUNT_ID:
    R2_S3_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# Region is arbitrary for R2 (non-AWS). Keep a sensible default for clients that require it.
R2_REGION = os.getenv("R2_REGION", "auto")

# Feature switch: True only when all required creds are present
USE_R2 = all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_S3_ENDPOINT])

# Where in the bucket to store product images (you can change anytime)
R2_PREFIX = os.getenv("R2_PREFIX", "products/").lstrip("/")

def r2_public_url(key: str) -> str:
    """
    Build a public URL for an object using R2_PUBLIC_BASE.
    Falls back to empty string if not configured.
    """
    if not R2_PUBLIC_BASE:
        return ""
    return f"{R2_PUBLIC_BASE}/{key.lstrip('/')}"

# -----------------------------------------------------------------------------
# Helper for accessing DB pool in routes
# -----------------------------------------------------------------------------
def get_db_pool(request: Request) -> asyncpg.pool.Pool:
    """
    Dependency to fetch the asyncpg pool from app.state.
    Raises HTTPException if the pool is missing (e.g., before startup).
    """
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="DB pool not initialized")
    return pool

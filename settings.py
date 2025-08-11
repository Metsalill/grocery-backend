# settings.py
import os
from dotenv import load_dotenv

load_dotenv()

ENV = (os.getenv("ENV") or "development").lower()
ENABLE_DOCS = ENV != "production"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.getenv("STATIC_DIR", os.path.join(BASE_DIR, "static"))
IMAGES_DIR = os.path.join(STATIC_DIR, "images")

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

CDN_BASE_URL = os.getenv("CDN_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or ""
MAX_UPLOAD_MB = int(os.getenv("MAX_IMAGE_MB", "6"))

import base64
import os
from fastapi import HTTPException, Request, status
from jose import JWTError, jwt
from settings import ADMIN_IP_ALLOWLIST, SWAGGER_USERNAME, SWAGGER_PASSWORD

# A fixed, shared realm string is important here: without it, some
# browsers (notably mobile Safari) treat each admin path as a separate
# "protection space" and won't reuse credentials already entered for
# another /admin/* path, silently failing to send the Authorization
# header on navigation instead of prompting again. Every basic_guard
# response must use the exact same realm so the browser recognizes
# "/", "/admin/partners", etc. as one shared login.
_REALM = 'Basic realm="Seivy Admin"'

# Same JWT secret/algorithm as auth.py — admin session tokens are just
# JWTs with scope="admin_access", so we don't need a second secret.
_JWT_SECRET = os.getenv("JWT_SECRET")
_JWT_ALGORITHM = "HS256"
ADMIN_COOKIE_NAME = "seivy_admin_token"


def _admin_ip_allowed(req: Request) -> bool:
    if not ADMIN_IP_ALLOWLIST:
        return True
    allowed = {ip.strip() for ip in ADMIN_IP_ALLOWLIST.split(",") if ip.strip()}
    return req.client and req.client.host in allowed


def _valid_admin_cookie(req: Request) -> bool:
    """Checks for a short-lived admin JWT in the seivy_admin_token cookie.
    Issued by POST /admin/request-token after the mobile app proves the
    user is the admin via their normal Bearer token. Returning True here
    means the person already has a valid session — no hardcoded password
    is ever shipped inside the Flutter app.
    """
    token = req.cookies.get(ADMIN_COOKIE_NAME)
    if not token or not _JWT_SECRET:
        return False
    try:
        payload = jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALGORITHM])
        return payload.get("scope") == "admin_access"
    except JWTError:
        return False


def basic_guard(req: Request):
    if not _admin_ip_allowed(req):
        raise HTTPException(status_code=403, detail="Admin IP not allowed")

    # New path: valid short-lived admin cookie (issued via the app) —
    # accept immediately, no Basic Auth needed at all.
    if _valid_admin_cookie(req):
        return

    # Existing path: unchanged, still works exactly as before in a
    # regular browser.
    if not (SWAGGER_USERNAME and SWAGGER_PASSWORD):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Admin auth not configured")
    expected = "Basic " + base64.b64encode(f"{SWAGGER_USERNAME}:{SWAGGER_PASSWORD}".encode()).decode()
    auth = req.headers.get("Authorization")
    if auth != expected:
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": _REALM})

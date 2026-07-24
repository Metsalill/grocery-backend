"""Shared server-side identity resolution for analytics events.

Both the explicit /analytics/event endpoint and the /compare endpoint's
basket_compare logging need the same two things: a pseudonymous device
key (HMAC-hashed from X-Device-Id) and, when the person is logged in,
the internal users.id (resolved from a verified JWT). This lives in one
place so both code paths always agree on how a person is identified —
a bug fixed here is fixed everywhere, and there's no risk of one path
validating the JWT differently or hashing the device id with a
different secret than the other.
"""
import hashlib
import hmac
import os
import logging
from typing import Optional

from fastapi import Request
from jose import JWTError, jwt

from auth import SECRET_KEY, ALGORITHM

logger = logging.getLogger("uvicorn.error")

# Server-side secret for hashing device identifiers. The raw device ID
# sent by the client (X-Device-Id header) is NEVER stored — only
# HMAC_SHA256(secret, device_id) is written to the database.
_DEVICE_HMAC_SECRET = os.environ.get("ANALYTICS_DEVICE_HMAC_SECRET", "")


def hash_device_id(raw_device_id: Optional[str]) -> Optional[str]:
    """Returns a stable pseudonymous hash for a raw device ID, or None
    if no device ID was provided or no HMAC secret is configured
    (fail-safe: never falls back to storing the raw ID)."""
    if not raw_device_id or not _DEVICE_HMAC_SECRET:
        return None
    normalized_device_id = raw_device_id.strip()
    if not normalized_device_id:
        return None
    digest = hmac.new(
        _DEVICE_HMAC_SECRET.encode("utf-8"),
        normalized_device_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest


async def resolve_user_id(request: Request, authorization: Optional[str]) -> Optional[str]:
    """Resolves the internal users.id (as text) from the Authorization
    header, if present and valid. Never trusts a client-supplied
    user_id — identity is derived only from a verified JWT, server-side.
    Returns None for guests, expired/invalid tokens, or deleted
    accounts (fail-closed: when in doubt, treat as anonymous rather
    than guessing an identity)."""
    if not authorization:
        return None

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    token = token.strip()

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None

    email = payload.get("sub")
    if not email:
        return None

    db = request.app.state.db
    try:
        row_id = await db.fetchval(
            """
            SELECT id FROM users
            WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL
            """,
            email,
        )
    except Exception as e:
        logger.warning(f"User lookup failed during analytics identity resolve: {e}")
        return None

    return str(row_id) if row_id is not None else None


async def resolve_analytics_identity(
    request: Request,
    authorization: Optional[str],
    x_device_id: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Convenience wrapper returning (user_id, device_key) together, so
    every analytics-writing endpoint resolves identity the exact same
    way with a single call."""
    return (
        await resolve_user_id(request, authorization),
        hash_device_id(x_device_id),
    )

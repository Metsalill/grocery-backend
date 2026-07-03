import hashlib
import hmac
import os
from fastapi import APIRouter, Request, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
import logging

logger = logging.getLogger("uvicorn.error")

router = APIRouter(prefix="/analytics", tags=["analytics"])

# Server-side secret for hashing device identifiers. The raw device ID sent
# by the client (X-Device-Id header) is NEVER stored — only
# HMAC_SHA256(secret, device_id) is written to the database. This means a
# database leak does not expose the raw per-device identifier used in the
# app, and the identifier cannot be recomputed without this secret.
_DEVICE_HMAC_SECRET = os.environ.get("ANALYTICS_DEVICE_HMAC_SECRET", "")


def _hash_device_id(raw_device_id: str) -> Optional[str]:
    """Returns a stable pseudonymous hash for a raw device ID, or None if
    no device ID was provided or no HMAC secret is configured (fail-safe:
    we never fall back to storing the raw ID)."""
    if not raw_device_id or not _DEVICE_HMAC_SECRET:
        return None
    digest = hmac.new(
        _DEVICE_HMAC_SECRET.encode("utf-8"),
        raw_device_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest


class AnalyticsEvent(BaseModel):
    event_type: str  # 'product_view', 'basket_add', 'basket_win'
    product_id: Optional[int] = None
    group_id: Optional[int] = None
    chain: Optional[str] = None
    user_id: Optional[str] = None


@router.post("/event")
async def log_event(
    event: AnalyticsEvent,
    request: Request,
    x_device_id: Optional[str] = Header(default=None, alias="X-Device-Id"),
):
    valid_event_types = {"product_view", "basket_add", "basket_win"}
    if event.event_type not in valid_event_types:
        raise HTTPException(status_code=400, detail=f"Invalid event_type. Must be one of: {valid_event_types}")

    db = request.app.state.db

    # Normaliseeri chain väiketähtedeks (basket_win jms)
    chain_normalized = event.chain.lower() if event.chain else None

    # basket_add: leia odavaim kett automaatselt product_id järgi
    if event.event_type == 'basket_add' and event.product_id and not chain_normalized:
        try:
            cheapest_chain = await db.fetchval("""
                SELECT s.chain
                FROM prices pr
                JOIN stores s ON s.id = pr.store_id
                WHERE pr.product_id = $1
                  AND pr.price IS NOT NULL
                ORDER BY pr.price ASC
                LIMIT 1
            """, event.product_id)
            if cheapest_chain:
                chain_normalized = cheapest_chain.lower()
        except Exception as e:
            logger.warning(f"Could not resolve chain for product {event.product_id}: {e}")

    # Pseudonümiseeritud seadme-ID: ainult HMAC-hash salvestatakse, toores
    # X-Device-Id ei jõua kunagi andmebaasi. Kasutatakse ainult koond-
    # statistika (unikaalsete külastajate) jaoks, mitte kasutajaprofiiliks.
    device_key = _hash_device_id(x_device_id) if x_device_id else None

    try:
        await db.execute(
            """
            INSERT INTO analytics_events (event_type, product_id, group_id, chain, user_id, device_key)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            event.event_type,
            event.product_id,
            event.group_id,
            chain_normalized,
            event.user_id,
            device_key,
        )
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Analytics insert error: {e}")
        raise HTTPException(status_code=500, detail="Failed to log event")


@router.get("/summary")
async def get_summary(request: Request, chain: Optional[str] = None, days: int = 30):
    db = request.app.state.db
    try:
        if chain:
            rows = await db.fetch(
                """
                SELECT event_type, COUNT(*) as count
                FROM analytics_events
                WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
                  AND LOWER(chain) = LOWER($2)
                GROUP BY event_type
                ORDER BY event_type
                """,
                str(days), chain,
            )
        else:
            rows = await db.fetch(
                """
                SELECT chain, event_type, COUNT(*) as count
                FROM analytics_events
                WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
                GROUP BY chain, event_type
                ORDER BY chain, event_type
                """,
                str(days),
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Analytics summary error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch summary")


@router.get("/top-products")
async def get_top_products(request: Request, chain: Optional[str] = None, days: int = 30, limit: int = 10):
    db = request.app.state.db
    try:
        rows = await db.fetch(
            """
            SELECT
                a.product_id, p.name, a.chain, a.event_type, COUNT(*) as count
            FROM analytics_events a
            LEFT JOIN products p ON p.id = a.product_id
            WHERE a.created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND a.product_id IS NOT NULL
              AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
            GROUP BY a.product_id, p.name, a.chain, a.event_type
            ORDER BY count DESC
            LIMIT $3
            """,
            str(days), chain, limit,
        )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Analytics top-products error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top products")

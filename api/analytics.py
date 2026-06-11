from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging

logger = logging.getLogger("uvicorn.error")

router = APIRouter(prefix="/analytics", tags=["analytics"])


class AnalyticsEvent(BaseModel):
    event_type: str  # 'product_view', 'basket_add', 'basket_win'
    product_id: Optional[int] = None
    group_id: Optional[int] = None
    chain: Optional[str] = None
    user_id: Optional[str] = None


@router.post("/event")
async def log_event(event: AnalyticsEvent, request: Request):
    """Log a single analytics event."""
    valid_event_types = {"product_view", "basket_add", "basket_win"}
    if event.event_type not in valid_event_types:
        raise HTTPException(status_code=400, detail=f"Invalid event_type. Must be one of: {valid_event_types}")

    db = request.app.state.db
    try:
        await db.execute(
            """
            INSERT INTO analytics_events (event_type, product_id, group_id, chain, user_id)
            VALUES ($1, $2, $3, $4, $5)
            """,
            event.event_type,
            event.product_id,
            event.group_id,
            event.chain,
            event.user_id,
        )
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Analytics insert error: {e}")
        raise HTTPException(status_code=500, detail="Failed to log event")


@router.get("/summary")
async def get_summary(request: Request, chain: Optional[str] = None, days: int = 30):
    """Get analytics summary. Optionally filter by chain."""
    db = request.app.state.db
    try:
        if chain:
            rows = await db.fetch(
                """
                SELECT
                    event_type,
                    COUNT(*) as count
                FROM analytics_events
                WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
                  AND chain = $2
                GROUP BY event_type
                ORDER BY event_type
                """,
                str(days),
                chain,
            )
        else:
            rows = await db.fetch(
                """
                SELECT
                    chain,
                    event_type,
                    COUNT(*) as count
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
    """Get top viewed/added products, optionally filtered by chain."""
    db = request.app.state.db
    try:
        rows = await db.fetch(
            """
            SELECT
                a.product_id,
                p.name,
                a.chain,
                a.event_type,
                COUNT(*) as count
            FROM analytics_events a
            LEFT JOIN products p ON p.id = a.product_id
            WHERE a.created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND a.product_id IS NOT NULL
              AND ($2::text IS NULL OR a.chain = $2)
            GROUP BY a.product_id, p.name, a.chain, a.event_type
            ORDER BY count DESC
            LIMIT $3
            """,
            str(days),
            chain,
            limit,
        )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Analytics top-products error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top products")

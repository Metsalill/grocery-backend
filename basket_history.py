@router.get("/{basket_id}", response_model=BasketDetailOut)
async def get_basket(
    basket_id: int,
    user=Depends(get_current_user),
    pool: asyncpg.pool.Pool = Depends(get_db_pool),
):
    uid = await resolve_user_id(user, pool)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with pool.acquire() as conn:
            head = await conn.fetchrow(
                """
                SELECT id, created_at, radius_km, winner_store_id, winner_store_name,
                       winner_total, stores, note
                FROM basket_history
                WHERE id=$1 AND user_id=$2::uuid AND deleted_at IS NULL
                """,
                basket_id,
                uid,
            )
            if not head:
                raise HTTPException(status_code=404, detail="Basket not found")

            # Make sure we always return a plain dict for jsonb and not None
            stores_payload = head["stores"] or {}

            items = await conn.fetch(
                """
                SELECT product, quantity, unit, price, line_total, store_id, store_name,
                       image_url, brand, size_text
                FROM basket_items
                WHERE basket_id=$1
                ORDER BY id
                """,
                basket_id,
            )

        return BasketDetailOut(
            id=head["id"],
            created_at=head["created_at"],
            radius_km=float(head["radius_km"]) if head["radius_km"] is not None else None,
            winner_store_id=head["winner_store_id"],
            winner_store_name=head["winner_store_name"],
            winner_total=float(head["winner_total"]) if head["winner_total"] is not None else None,
            stores=stores_payload,
            note=head["note"],
            items=[dict(r) for r in items],
        )

    except HTTPException:
        raise
    except Exception as e:
        # One easy-to-find log line
        print(
            "GET_BASKET_ERROR:",
            type(e).__name__,
            str(e),
            {"basket_id": basket_id, "uid": uid},
        )
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

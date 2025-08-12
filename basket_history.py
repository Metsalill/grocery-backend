# ... imports
import json

# inside save_basket(), after choosing `winner`:
winner_store_id = winner.get("store_id")
winner_store_name = winner.get("store_name")
winner_total = float(winner.get("total") or 0)

# If your DB column is NOT NULL, fallback to 0 when legacy compare lacks IDs.
if winner_store_id is None:
    winner_store_id = 0

# Serialize once for jsonb insert
stores_json = json.dumps(stores, ensure_ascii=False)

# --- INSERT header ---
head = await conn.fetchrow(
    """
    INSERT INTO basket_history (
        user_id, radius_km, winner_store_id, winner_store_name,
        winner_total, stores, note
    ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
    RETURNING id, created_at, winner_store_name, winner_total, radius_km
    """,
    uid,
    payload.radius_km,
    winner_store_id,
    winner_store_name,
    winner_total,
    stores_json,   # <- JSON string for jsonb
    payload.note,
)
basket_id = head["id"]

# ... when inserting items, reuse the same resolved store info:
tasks.append(conn.execute(
    """
    INSERT INTO basket_items (
        basket_id, product, quantity, unit, price, line_total,
        store_id, store_name, image_url, brand, size_text
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    """,
    basket_id,
    it.product,
    float(it.quantity),
    it.unit,
    price,
    line_total,
    winner_store_id,    # <- use resolved id
    winner_store_name,  # <- use resolved name
    it.image_url,
    it.brand,
    it.size_text,
))

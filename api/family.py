# api/family.py

import random
import string
from fastapi import APIRouter, Request, Header, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

router = APIRouter()


async def _get_pool(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    return pool


async def _require_user_id(conn, authorization: Optional[str]) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        import os
        from jose import jwt
        token = authorization.split(" ")[1]
        SECRET_KEY = os.getenv("JWT_SECRET", "super-secret-key")
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        email = (payload.get("sub") or "").lower()
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        row = await conn.fetchrow(
            "SELECT id FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
            email,
        )
        if not row:
            raise HTTPException(status_code=401, detail="User not found")
        return row["id"]
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


def _random_code(length: int = 6) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


# ─── Models ────────────────────────────────────────────────────────────────

class CreateFamilyRequest(BaseModel):
    name: str

class AddItemRequest(BaseModel):
    product_id: Optional[int] = None
    product_name: str
    quantity: float = 1.0
    is_per_kg: bool = False
    kg_quantity: Optional[float] = None
    image_url: Optional[str] = None
    brand: Optional[str] = None
    size_text: Optional[str] = None

class ShareBasketItem(BaseModel):
    product_id: Optional[int] = None
    product_name: str
    quantity: float = 1.0
    is_per_kg: bool = False
    kg_quantity: Optional[float] = None
    image_url: Optional[str] = None
    brand: Optional[str] = None
    size_text: Optional[str] = None

class ShareBasketRequest(BaseModel):
    items: List[ShareBasketItem]

class UpdateItemRequest(BaseModel):
    quantity: Optional[float] = None
    kg_quantity: Optional[float] = None


# ─── Endpoints ─────────────────────────────────────────────────────────────

@router.post("/family")
async def create_family(
    request: Request,
    body: CreateFamilyRequest,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        existing = await conn.fetchrow(
            "SELECT f.id, f.name, f.invite_code FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if existing:
            raise HTTPException(status_code=400, detail="Oled juba pere liige. Lahku kõigepealt perest.")

        for _ in range(10):
            code = _random_code()
            exists = await conn.fetchval("SELECT 1 FROM families WHERE invite_code = $1", code)
            if not exists:
                break

        family = await conn.fetchrow(
            "INSERT INTO families (name, invite_code, created_by) VALUES ($1, $2, $3) "
            "RETURNING id, name, invite_code, created_at",
            body.name.strip(), code, user_id
        )

        await conn.execute(
            "INSERT INTO family_members (family_id, user_id) VALUES ($1, $2)",
            family["id"], user_id
        )

    return {
        "id": family["id"],
        "name": family["name"],
        "invite_code": family["invite_code"],
        "created_at": family["created_at"].isoformat(),
    }


@router.post("/family/join/{invite_code}")
async def join_family(
    request: Request,
    invite_code: str,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        existing = await conn.fetchrow(
            "SELECT f.id FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if existing:
            raise HTTPException(status_code=400, detail="Oled juba pere liige. Lahku kõigepealt perest.")

        family = await conn.fetchrow(
            "SELECT id, name, invite_code FROM families WHERE invite_code = $1",
            invite_code.upper()
        )
        if not family:
            raise HTTPException(status_code=404, detail="Pere ei leitud. Kontrolli koodi.")

        await conn.execute(
            "INSERT INTO family_members (family_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            family["id"], user_id
        )

        count = await conn.fetchval(
            "SELECT COUNT(*) FROM family_members WHERE family_id = $1", family["id"]
        )

    return {
        "id": family["id"],
        "name": family["name"],
        "invite_code": family["invite_code"],
        "member_count": count,
    }


@router.get("/family")
async def get_my_family(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id, f.name, f.invite_code, f.created_by, f.created_at "
            "FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            return {"family": None}

        members = await conn.fetch(
            "SELECT u.id, u.first_name, u.email, fm.joined_at "
            "FROM family_members fm "
            "JOIN users u ON u.id = fm.user_id "
            "WHERE fm.family_id = $1 "
            "ORDER BY fm.joined_at",
            family["id"]
        )

    return {
        "family": {
            "id": family["id"],
            "name": family["name"],
            "invite_code": family["invite_code"],
            "is_admin": family["created_by"] == user_id,
            "created_at": family["created_at"].isoformat(),
            "members": [
                {
                    "id": m["id"],
                    "name": m["first_name"] or m["email"].split("@")[0],
                    "joined_at": m["joined_at"].isoformat(),
                }
                for m in members
            ],
        }
    }


@router.delete("/family/leave")
async def leave_family(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id, f.created_by FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            raise HTTPException(status_code=404, detail="Sa pole ühegi pere liige.")

        await conn.execute(
            "DELETE FROM family_members WHERE family_id = $1 AND user_id = $2",
            family["id"], user_id
        )

        remaining = await conn.fetchval(
            "SELECT COUNT(*) FROM family_members WHERE family_id = $1", family["id"]
        )
        if remaining == 0:
            await conn.execute("DELETE FROM families WHERE id = $1", family["id"])

    return {"success": True}


# ─── Pere korv ─────────────────────────────────────────────────────────────

@router.get("/family/basket")
async def get_family_basket(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            raise HTTPException(status_code=404, detail="Sa pole ühegi pere liige.")

        rows = await conn.fetch(
            "SELECT fbi.*, "
            "COALESCE(u.first_name, SPLIT_PART(u.email, '@', 1)) AS added_by_name "
            "FROM family_basket_items fbi "
            "JOIN users u ON u.id = fbi.user_id "
            "WHERE fbi.family_id = $1 "
            "ORDER BY fbi.added_at DESC",
            family["id"]
        )

    items = []
    for r in rows:
        d = dict(r)
        items.append({
            "id": d["id"],
            "product_id": d["product_id"],
            "product_name": d["product_name"],
            "quantity": float(d["quantity"]),
            "is_per_kg": d["is_per_kg"],
            "kg_quantity": float(d["kg_quantity"]) if d["kg_quantity"] else None,
            "image_url": d["image_url"],
            "brand": d["brand"],
            "size_text": d["size_text"],
            "added_by": d["added_by_name"],
            "added_at": d["added_at"].isoformat(),
        })

    return {"family_id": family["id"], "items": items, "count": len(items)}


@router.post("/family/basket/share")
async def share_basket_to_family(
    request: Request,
    body: ShareBasketRequest,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """
    Jaga isiklik korv perekorvi.

    Reeglid:
    - Tooted, millel puudub product_id, LÜKATAKSE TAGASI (ei salvestata) -
      product_id on kohustuslik, kuna hinnavõrdlus sõltub sellest. Ilma
      selleta salvestunud rida rikub hiljem "Võrdle perekorvi" flow (vaikne
      andmekadu - vt 2026-07 intsident).
    - Dedupe käib EELISTATULT product_id järgi (kindel identiteet), mitte
      ainult toote nime järgi (nimi võib kattuda eri toodete vahel).
    - Kui perekorvis on juba vanem "katkine" rida (product_id IS NULL) sama
      nimega, ja uus jagamine toob korrektse product_id, siis PARANDATAKSE
      (UPDATE) vana rida selle asemel, et see igavesti katki jätta.

    Tagastab: added (uued), skipped (juba olemas), repaired (vana katkine
    rida parandatud), rejected (product_id puudus, ei salvestatud) +
    rejected_items (nimekiri nimedest mis tagasi lükati).
    """
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            raise HTTPException(status_code=404, detail="Sa pole ühegi pere liige.")

        family_id = family["id"]

        existing_rows = await conn.fetch(
            "SELECT id, product_id, LOWER(TRIM(product_name)) AS norm_name "
            "FROM family_basket_items WHERE family_id = $1",
            family_id
        )

        # product_id -> row id, ainult mitte-NULL product_id ridade jaoks
        existing_by_product_id: Dict[int, int] = {
            r["product_id"]: r["id"] for r in existing_rows if r["product_id"] is not None
        }
        # norm_name -> row id, ainult ridade jaoks kus product_id ON NULL
        # (need on parandatavad "katkised" read)
        existing_null_by_name: Dict[str, int] = {
            r["norm_name"]: r["id"] for r in existing_rows if r["product_id"] is None
        }
        # koik nimed, sh juba korras read - vaikimisi fallback duplikaadi kaitseks
        existing_names_all = {r["norm_name"] for r in existing_rows}

        added = 0
        skipped = 0
        repaired = 0
        rejected = 0
        rejected_items: List[str] = []

        new_items = []
        repair_updates = []  # (row_id, item)

        for item in body.items:
            norm_name = item.product_name.strip().lower()

            if item.product_id is None:
                rejected += 1
                rejected_items.append(item.product_name.strip())
                continue

            if item.product_id in existing_by_product_id:
                skipped += 1
                continue

            if norm_name in existing_null_by_name:
                row_id = existing_null_by_name.pop(norm_name)
                repair_updates.append((row_id, item))
                existing_by_product_id[item.product_id] = row_id
                continue

            if norm_name in existing_names_all:
                skipped += 1
                continue

            new_items.append(item)
            existing_names_all.add(norm_name)
            existing_by_product_id[item.product_id] = -1  # kaitseb sama-paketi duplikaate

        if new_items:
            await conn.executemany(
                "INSERT INTO family_basket_items "
                "(family_id, user_id, product_id, product_name, quantity, is_per_kg, "
                "kg_quantity, image_url, brand, size_text) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                [
                    (
                        family_id, user_id, item.product_id, item.product_name.strip(),
                        item.quantity, item.is_per_kg, item.kg_quantity,
                        item.image_url, item.brand, item.size_text,
                    )
                    for item in new_items
                ]
            )
            added = len(new_items)

        if repair_updates:
            await conn.executemany(
                "UPDATE family_basket_items "
                "SET product_id = $1, "
                "    brand = COALESCE(NULLIF(brand, ''), $2), "
                "    size_text = COALESCE(NULLIF(size_text, ''), $3), "
                "    image_url = COALESCE(NULLIF(image_url, ''), $4) "
                "WHERE id = $5 AND family_id = $6",
                [
                    (
                        item.product_id, item.brand, item.size_text, item.image_url,
                        row_id, family_id,
                    )
                    for row_id, item in repair_updates
                ]
            )
            repaired = len(repair_updates)

    return {
        "success": True,
        "added": added,
        "skipped": skipped,
        "repaired": repaired,
        "rejected": rejected,
        "rejected_items": rejected_items,
    }


@router.post("/family/basket")
async def add_to_family_basket(
    request: Request,
    body: AddItemRequest,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    if body.product_id is None:
        raise HTTPException(
            status_code=422,
            detail="product_id on kohustuslik - toode peab tulema tootekataloogist.",
        )

    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            raise HTTPException(status_code=404, detail="Sa pole ühegi pere liige.")

        row = await conn.fetchrow(
            "INSERT INTO family_basket_items "
            "(family_id, user_id, product_id, product_name, quantity, is_per_kg, "
            "kg_quantity, image_url, brand, size_text) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "RETURNING id, added_at",
            family["id"], user_id, body.product_id, body.product_name.strip(),
            body.quantity, body.is_per_kg, body.kg_quantity,
            body.image_url, body.brand, body.size_text,
        )

    return {"success": True, "id": row["id"], "added_at": row["added_at"].isoformat()}


@router.delete("/family/basket/{item_id}")
async def remove_from_family_basket(
    request: Request,
    item_id: int,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            raise HTTPException(status_code=404, detail="Sa pole ühegi pere liige.")

        result = await conn.execute(
            "DELETE FROM family_basket_items "
            "WHERE id = $1 AND family_id = $2",
            item_id, family["id"]
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Toodet ei leitud.")

    return {"success": True, "id": item_id}


@router.delete("/family/basket")
async def clear_family_basket(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    pool = await _get_pool(request)
    async with pool.acquire() as conn:
        user_id = await _require_user_id(conn, authorization)

        family = await conn.fetchrow(
            "SELECT f.id FROM families f "
            "JOIN family_members fm ON fm.family_id = f.id "
            "WHERE fm.user_id = $1",
            user_id
        )
        if not family:
            raise HTTPException(status_code=404, detail="Sa pole ühegi pere liige.")

        await conn.execute(
            "DELETE FROM family_basket_items WHERE family_id = $1", family["id"]
        )

    return {"success": True}

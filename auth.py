from fastapi import APIRouter, Depends, HTTPException, status, Request, Header
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import asyncpg
import os

router = APIRouter()

# JWT settings
SECRET_KEY = os.getenv("JWT_SECRET", "super-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# User model
class UserIn(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str = ""
    phone: str = ""

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

# Helper: create JWT token
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Helper: verify password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Helper: hash password
def get_password_hash(password):
    return pwd_context.hash(password)

# REGISTER
@router.post("/register")
async def register(user: UserIn, request: Request):
    try:
        async with request.app.state.db.acquire() as conn:
            existing = await conn.fetchrow("SELECT * FROM users WHERE email = $1", user.email)
            if existing:
                raise HTTPException(status_code=400, detail="Email already registered")

            hashed_pw = get_password_hash(user.password)
            await conn.execute(
                """
                INSERT INTO users (email, password_hash, first_name, last_name, phone, role)
                VALUES ($1, $2, $3, $4, $5, 'regular')
                """,
                user.email, hashed_pw, user.first_name, user.last_name, user.phone
            )
        return {"status": "success", "message": "User registered successfully"}

    except Exception as e:
        print("❌ REGISTER ERROR:", str(e))  # <- this will show in logs
        raise HTTPException(status_code=500, detail="Internal Server Error")

# LOGIN
@router.post("/login", response_model=TokenOut)
async def login(user: UserIn, request: Request):
    async with request.app.state.db.acquire() as conn:
        db_user = await conn.fetchrow("SELECT * FROM users WHERE email = $1", user.email)
        if not db_user or not verify_password(user.password, db_user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(data={"sub": user.email}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token}

# GET CURRENT USER
async def get_current_user(request: Request, token: str = Depends(lambda: get_token_from_header())):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    async with request.app.state.db.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
    return dict(user)

# Simple token extractor
def get_token_from_header(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    return authorization.split(" ")[1]

@router.get("/me")
async def read_current_user(user=Depends(get_current_user)):
    return {
        "email": user["email"],
        "first_name": user["first_name"],
        "last_name": user["last_name"],
        "phone": user["phone"],
        "role": user["role"],
        "created_at": user["created_at"]
    }

@router.get("/users")
async def list_users(request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    async with request.app.state.db.acquire() as conn:
        users = await conn.fetch("SELECT email, first_name, last_name, phone, role, created_at FROM users")
        return [dict(u) for u in users]

@router.post("/make-superuser")
async def promote_user(email: EmailStr, request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    async with request.app.state.db.acquire() as conn:
        await conn.execute("UPDATE users SET role = 'superuser' WHERE email = $1", email)
    return {"status": "success", "message": f"User {email} promoted to superuser"}

@router.post("/make-regular")
async def demote_user(email: EmailStr, request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    async with request.app.state.db.acquire() as conn:
        await conn.execute("UPDATE users SET role = 'regular' WHERE email = $1", email)
    return {"status": "success", "message": f"User {email} demoted to regular"}

@router.delete("/delete-user")
async def delete_user(request: Request, user=Depends(get_current_user)):
    try:
        async with request.app.state.db.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE email = $1", user["email"])
        return {"status": "success", "message": f"User {user['email']} deleted"}
    except Exception as e:
        print("❌ DELETE ERROR:", str(e))
        raise HTTPException(status_code=500, detail="Failed to delete user")


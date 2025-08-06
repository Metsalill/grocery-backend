from fastapi import APIRouter, Depends, HTTPException, status
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

# DB pool (set from main.py)
db_pool: asyncpg.Pool = None

# User model
class UserIn(BaseModel):
    email: EmailStr
    password: str

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
async def register(user: UserIn):
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM users WHERE email = $1", user.email)
        if existing:
            raise HTTPException(status_code=400, detail="Email already registered")

        hashed_pw = get_password_hash(user.password)
        await conn.execute(
            "INSERT INTO users (email, password_hash) VALUES ($1, $2)",
            user.email, hashed_pw
        )
    return {"status": "success", "message": "User registered successfully"}

# LOGIN
@router.post("/login", response_model=TokenOut)
async def login(user: UserIn):
    async with db_pool.acquire() as conn:
        db_user = await conn.fetchrow("SELECT * FROM users WHERE email = $1", user.email)
        if not db_user or not verify_password(user.password, db_user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(data={"sub": user.email})
    return {"access_token": access_token}

# GET CURRENT USER
async def get_current_user(token: str = Depends(lambda: get_token_from_header())):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
    return dict(user)

# Simple token extractor
def get_token_from_header(authorization: str = Depends(lambda: os.getenv("HTTP_AUTHORIZATION", ""))):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    return authorization.split(" ")[1]

@router.get("/me")
async def read_current_user(user=Depends(get_current_user)):
    return {"email": user["email"], "created_at": user["created_at"]}

from fastapi import APIRouter, Depends, HTTPException, status, Request, Header
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import os

# NEW: Google token verify
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests

router = APIRouter()

# ===== JWT & password settings (top-level so they exist before use) =====
SECRET_KEY = os.getenv("JWT_SECRET", "super-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ===== Models =====
class UserIn(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str = ""
    phone: str = ""

class LoginUser(BaseModel):
    email: EmailStr
    password: str

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

# NEW: Google login input
class GoogleLoginIn(BaseModel):
    id_token: str

# ===== Helpers =====
def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_reset_token(email: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=15)
    return jwt.encode({"sub": email, "exp": expire, "scope": "password_reset"}, SECRET_KEY, algorithm=ALGORITHM)

def verify_password(plain_password, hashed_password) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password) -> str:
    return pwd_context.hash(password)

def _db_pool_or_503(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="Database not ready")
    return pool

# ===== Auth dependency =====
async def get_current_user(request: Request, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")

    token = authorization.split(" ")[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = (payload.get("sub") or "").lower()
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        # God mode
        if email == "marko@minetech.ee":
            return {
                "email": email,
                "role": "superuser",
                "first_name": "Marko",
                "last_name": "",
                "phone": "",
                "created_at": datetime.utcnow()
            }

        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            user = await conn.fetchrow(
                """
                SELECT email, first_name, last_name, phone, role, created_at
                FROM users
                WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL
                """,
                email
            )
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            return dict(user)

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ===== Routes =====
@router.get("/users/me")
async def read_users_me(current_user=Depends(get_current_user)):
    return current_user

@router.post("/register")
async def register(user: UserIn, request: Request):
    try:
        email = user.email.lower()
        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            existing = await conn.fetchrow("SELECT 1 FROM users WHERE LOWER(email) = LOWER($1)", email)
            if existing:
                raise HTTPException(status_code=400, detail="Email already registered")

            hashed_pw = get_password_hash(user.password)
            await conn.execute(
                """
                INSERT INTO users (email, password_hash, first_name, last_name, phone, role)
                VALUES ($1, $2, $3, $4, $5, 'regular')
                """,
                email, hashed_pw, user.first_name, user.last_name, user.phone
            )
        return {"status": "success", "message": "User registered successfully"}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ REGISTER ERROR:", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.post("/login", response_model=TokenOut)
async def login(user: LoginUser, request: Request):
    email = user.email.lower()
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        db_user = await conn.fetchrow(
            "SELECT * FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
            email
        )
        if not db_user or not verify_password(user.password, db_user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(
        data={"sub": email},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token}

# NEW: Google login/registration (mobile-friendly, verifies ID token)
@router.post("/auth/login/google", response_model=TokenOut)
async def login_with_google(payload: GoogleLoginIn, request: Request):
    audience = os.getenv("GOOGLE_AUDIENCE")
    if not audience:
        raise HTTPException(status_code=500, detail="Server missing GOOGLE_AUDIENCE")

    try:
        claims = google_id_token.verify_oauth2_token(
            payload.id_token,
            google_requests.Request(),
            audience,
        )
        # Optional: issuer check
        allowed_issuers = (os.getenv("GOOGLE_ALLOWED_ISSUERS") or "https://accounts.google.com,accounts.google.com").split(",")
        if claims.get("iss") not in allowed_issuers:
            raise ValueError("Invalid token issuer")

        email = (claims.get("email") or "").lower()
        if not email or not claims.get("email_verified", False):
            raise ValueError("Email not present/verified")

        first_name = claims.get("given_name") or (claims.get("name") or "").split(" ")[0] if claims.get("name") else ""
        last_name = claims.get("family_name") or ""

        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT email FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
                email,
            )
            if not existing:
                # passwordless account for Google sign-in
                await conn.execute(
                    """
                    INSERT INTO users (email, password_hash, first_name, last_name, phone, role)
                    VALUES ($1, NULL, $2, $3, '', 'regular')
                    """,
                    email, first_name, last_name,
                )

        access_token = create_access_token(
            data={"sub": email},
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        return {"access_token": access_token}

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
    except Exception as e:
        print("❌ GOOGLE LOGIN ERROR:", str(e))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Google token")

@router.get("/me")
async def read_current_user(user=Depends(get_current_user)):
    return user

@router.get("/users")
async def list_users(request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT email, first_name, last_name, phone, role, created_at FROM users WHERE deleted_at IS NULL"
        )
    return [dict(u) for u in rows]

@router.post("/make-superuser")
async def promote_user(email: EmailStr, request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET role = 'superuser' WHERE LOWER(email) = LOWER($1)", email.lower())
    return {"status": "success", "message": f"User {email} promoted to superuser"}

@router.post("/make-regular")
async def demote_user(email: EmailStr, request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET role = 'regular' WHERE LOWER(email) = LOWER($1)", email.lower())
    return {"status": "success", "message": f"User {email} demoted to regular"}

@router.delete("/delete-user")
async def delete_user(request: Request, user=Depends(get_current_user)):
    try:
        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET deleted_at = NOW() WHERE LOWER(email) = LOWER($1)", user["email"].lower())
        return {"status": "success", "message": f"User {user['email']} soft-deleted"}
    except Exception as e:
        print("❌ DELETE ERROR:", str(e))
        raise HTTPException(status_code=500, detail="Failed to delete user")

@router.post("/request-password-reset")
async def request_password_reset(email: EmailStr, request: Request):
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT 1 FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
            email.lower()
        )
        if not user:
            raise HTTPException(status_code=404, detail="Email not found")

    reset_token = create_reset_token(email.lower())
    return {"reset_token": reset_token}

@router.post("/reset-password")
async def reset_password(data: ResetPasswordRequest, request: Request):
    try:
        payload = jwt.decode(data.token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("scope") != "password_reset":
            raise HTTPException(status_code=401, detail="Invalid token scope")
        email = (payload.get("sub") or "").lower()
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    hashed_pw = get_password_hash(data.new_password)
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET password_hash = $1 WHERE LOWER(email) = LOWER($2)", hashed_pw, email)

    return {"status": "success", "message": "Password reset successful"}

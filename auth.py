from fastapi import APIRouter, Depends, HTTPException, status, Request, Header
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import os
import uuid
import hashlib
import httpx
import asyncpg

# Google token verify
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests

from utils.throttle import throttle

router = APIRouter()

# ===== JWT & password settings =====
SECRET_KEY = os.getenv("JWT_SECRET")
if not SECRET_KEY:
    raise RuntimeError("JWT_SECRET environment variable is not set")
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

class GoogleLoginIn(BaseModel):
    id_token: str

class AppleLoginIn(BaseModel):
    identity_token: str
    first_name: str | None = None
    last_name: str | None = None

# ===== Helpers =====
def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_reset_token(email: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=30)
    return jwt.encode({"sub": email, "exp": expire, "scope": "password_reset"}, SECRET_KEY, algorithm=ALGORITHM)

def verify_password(plain_password, hashed_password) -> bool:
    if not hashed_password:
        return False
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False

def get_password_hash(password) -> str:
    return pwd_context.hash(password)

def _db_pool_or_503(request: Request):
    pool = getattr(request.app.state, "db", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="Database not ready")
    return pool

async def send_reset_email(email: str, reset_token: str):
    """Send password reset email via Resend."""
    resend_api_key = os.getenv("RESEND_API_KEY")
    if not resend_api_key:
        raise HTTPException(status_code=500, detail="Email service not configured")

    reset_link = f"https://seivy.ee/reset-password?token={reset_token}"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; max-width: 480px; margin: 0 auto;">
        <h2 style="color: #FF9100;">Seivy paroolivahetus</h2>
        <p>Tere!</p>
        <p>Parooli vahetamiseks vajuta allolevale nupule. Link kehtib <strong>30 minutit</strong>.</p>
        <a href="{reset_link}"
           style="display:inline-block; background:#FF9100; color:#fff; padding:12px 24px;
                  border-radius:8px; text-decoration:none; font-weight:bold; margin:16px 0;">
            Vaheta parool
        </a>
        <p style="color:#888; font-size:13px;">
            Kui sa ei taotlenud parooli vahetust, ignoreeri seda kirja.
        </p>
        <p style="color:#888; font-size:12px;">
            Kui nupp ei tööta, kopeeri see link brauserisse:<br>
            <a href="{reset_link}" style="color:#FF9100;">{reset_link}</a>
        </p>
    </div>
    """

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {resend_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": "Seivy <noreply@seivy.ee>",
                "to": [email],
                "subject": "Seivy paroolivahetus",
                "html": html_content,
            },
        )
        if resp.status_code not in (200, 201):
            print(f"❌ RESEND ERROR: {resp.status_code} {resp.text}")
            raise HTTPException(status_code=500, detail="Failed to send email")

# ===== Apple token verify =====
async def verify_apple_identity_token(identity_token: str) -> dict:
    """Verify Apple identity token using Apple's public keys."""
    try:
        # Fetch Apple's public keys
        async with httpx.AsyncClient() as client:
            resp = await client.get("https://appleid.apple.com/auth/keys")
            apple_keys = resp.json()

        # Decode header to get kid
        import base64, json as _json
        header_segment = identity_token.split(".")[0]
        # Add padding
        header_segment += "=" * (4 - len(header_segment) % 4)
        header = _json.loads(base64.urlsafe_b64decode(header_segment))
        kid = header.get("kid")

        # Find matching key
        from jose import jwk
        matching_key = None
        for key_data in apple_keys.get("keys", []):
            if key_data.get("kid") == kid:
                matching_key = jwk.construct(key_data)
                break

        if not matching_key:
            raise ValueError("No matching Apple public key found")

        # Verify and decode
        claims = jwt.decode(
            identity_token,
            matching_key,
            algorithms=["RS256"],
            audience=os.getenv("APPLE_BUNDLE_ID", "ee.elynoy.seivy"),
            issuer="https://appleid.apple.com",
        )
        return claims

    except Exception as e:
        raise ValueError(f"Apple token verification failed: {e}")

def _get_google_token_metadata(id_token: str, expected_audiences: list[str]) -> dict:
    """
    DIAGNOSTIKA (2026-08): dekodeerib Google ID tokeni VERIFITSEERIMATA
    claim'id ainult logimise jaoks -- kunagi EI kasutata neid autentimis-
    otsuse tegemiseks (see läbib alati eraldi google_id_token.
    verify_oauth2_token()). Logime ainult tehnilised väljad (aud/azp/iss/
    iat/exp/kid/alg), MITTE email'i, sub'i ega tokeni sisu -- vt ChatGPT
    review 2026-08-02, mis soovitas seda turvalist piirjoont.
    """
    claims = jwt.get_unverified_claims(id_token)
    header = jwt.get_unverified_header(id_token)
    return {
        "token_hash": hashlib.sha256(id_token.encode("utf-8")).hexdigest()[:12],
        "aud": claims.get("aud"),
        "azp": claims.get("azp"),
        "iss": claims.get("iss"),
        "iat": claims.get("iat"),
        "exp": claims.get("exp"),
        "has_hd": bool(claims.get("hd")),
        "kid": header.get("kid"),
        "alg": header.get("alg"),
        "expected_audiences": expected_audiences,
    }


# ===== Auth dependency =====
async def get_current_user(request: Request, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        # DIAGNOSTIKA (2026-08): logi puuduva/vigase Authorization headeriga
        # päringu path, et eristada "äpp ei saatnud tokenit üldse" juhtumeid
        # neist, kus token saadeti aga oli vigane/aegunud (vt allpool).
        print(f"🔒 AUTH 401: missing/invalid Authorization header, path={request.url.path}")
        raise HTTPException(status_code=401, detail="Missing or invalid token")

    token = authorization.split(" ")[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = (payload.get("sub") or "").lower()
        if not email:
            print(f"🔒 AUTH 401: token decoded but 'sub' claim missing, path={request.url.path}")
            raise HTTPException(status_code=401, detail="Invalid token")

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
                # DIAGNOSTIKA (2026-08): see on peamine kahtlusalune "äpp
                # viskab sisselogimisel välja" bugi jaoks -- token oli
                # kehtiv (JWT dekodeerus õigesti), aga vastavat kasutaja
                # rida ei leitud (deleted_at IS NULL filter ei läbinud).
                # Kui see rida hakkab ilmuma Railway logides kohe pärast
                # /login, /auth/login/google või /auth/login/apple 200
                # vastust samalt kliendilt, on põhjus siin, mitte Flutteri
                # poolel.
                exp = payload.get("exp")
                print(
                    f"🔒 AUTH 404: valid JWT for email={email} but no matching "
                    f"non-deleted user row found. token_exp={exp}, path={request.url.path}"
                )
                raise HTTPException(status_code=404, detail="User not found")
            return dict(user)

    except JWTError as e:
        # DIAGNOSTIKA (2026-08): JWTError katab nii "signature verification
        # failed" (nt JWT_SECRET erineb sellest, millega token loodi -- nt
        # Railway env var muutus/deploy vahetas secreti) kui ka "expired
        # signature" (aegunud token) kui ka lihtsalt vigase tokeni. Need on
        # sisuliselt erinevad bugid, seega logi täpne exception-tekst.
        print(f"🔒 AUTH 401: JWTError decoding token -- {type(e).__name__}: {e}, path={request.url.path}")
        raise HTTPException(status_code=401, detail="Invalid token")

# ===== Routes =====
@router.get("/users/me")
@throttle(limit=60, window=60)
async def read_users_me(request: Request, current_user=Depends(get_current_user)):
    return current_user

@router.post("/register", response_model=TokenOut)
@throttle(limit=5, window=60)
async def register(user: UserIn, request: Request):
    """
    Loob konto JA logib kohe sisse (tagastab JWT tokeni).

    Varem tagastas ainult {"status": "success"} ilma tokenita -> kasutaja jai
    parast registreerimist guest-olekusse (token puudus) ja "Kustuta konto"
    andis "Not authenticated". Nuud on kaks selget olekut:
      guest = tokenit pole | registreeritud = token olemas

    Olemasolu-kontroll filtreerib deleted_at IS NULL -- kustutatud konto e-post
    anonumiseeritakse (deleted_..@deleted.invalid), seega originaal-aadress on
    vaba ja sellega saab uuesti registreeruda.
    """
    try:
        email = user.email.lower()
        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT 1 FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
                email
            )
            if existing:
                raise HTTPException(status_code=400, detail="Email already registered")

            hashed_pw = get_password_hash(user.password)
            await conn.execute(
                """
                INSERT INTO users (email, password_hash, first_name, last_name, phone, role, auth_provider, email_verified)
                VALUES ($1, $2, $3, $4, $5, 'regular', 'local', false)
                """,
                email, hashed_pw, user.first_name, user.last_name, user.phone
            )

        access_token = create_access_token(
            data={"sub": email},
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        return {"access_token": access_token}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ REGISTER ERROR:", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/login", response_model=TokenOut)
@throttle(limit=10, window=60)
async def login(user: LoginUser, request: Request):
    email = user.email.lower()
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        db_user = await conn.fetchrow(
            """
            SELECT email, password_hash, auth_provider
            FROM users
            WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL
            """,
            email
        )

        if not db_user:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if db_user.get("auth_provider") != "local":
            raise HTTPException(
                status_code=401,
                detail="This account uses Google sign-in. Use 'Continue with Google' or reset your password."
            )

        if not verify_password(user.password, db_user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(
        data={"sub": email},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token}

@router.post("/auth/login/google", response_model=TokenOut)
@throttle(limit=20, window=60)
async def login_with_google(payload: GoogleLoginIn, request: Request):
    # GOOGLE_AUDIENCE toetab komaga eraldatud lubatud OAuth Client ID'de
    # loendit. Railway diagnostika (2026-08-02) näitas, et osa iOS
    # login'e saadavad Google ID tokeni, mille 'aud' on rakenduse iOS
    # OAuth Client ID, samal ajal kui backend ootas ainult Web/server
    # OAuth Client ID't.
    #
    # NB: Google'i ametliku iOS SDK dokumentatsiooni järgi PEAKS korrektselt
    # rakendatud serverClientId määrama ID tokeni audience'i (vt
    # GIDConfiguration.serverClientID) -- seega see pole "iOS käitubki
    # alati nii", vaid viitab, et serverClientId ei rakendunud mingil
    # põhjusel selle kasutaja build'is (nt plugina versioon, .env
    # laadimise ajastus, või GoogleService-Info.plist konfiguratsioon).
    # Root cause vajab eraldi Flutter-poolset uurimist. Mitme audience'i
    # lubamine on ühilduvusparandus olemasolevatele tootmisversioonidele,
    # mitte lõplik fix.
    #
    # Railway GOOGLE_AUDIENCE väärtus:
    #   <WEB_CLIENT_ID>,<IOS_CLIENT_ID>
    audience_env = os.getenv("GOOGLE_AUDIENCE")
    if not audience_env:
        raise HTTPException(status_code=500, detail="Server missing GOOGLE_AUDIENCE")

    audience = [value.strip() for value in audience_env.split(",") if value.strip()]

    if not audience or any(
        not value.endswith(".apps.googleusercontent.com") for value in audience
    ):
        print(f"❌ GOOGLE CONFIG ERROR: invalid GOOGLE_AUDIENCE format: {audience_env!r}")
        raise HTTPException(status_code=500, detail="Server Google authentication misconfigured")

    # DIAGNOSTIKA (2026-08): dekodeeri metadata ENNE verify't, et see oleks
    # käepärast ka siis kui verify_oauth2_token() kohe ValueError'iga
    # nurjub. Ebaõnnestunud dekodeerimine ei tohi kunagi takistada
    # tavapärast login-voogu -- seepärast oma try/except, mis vaikimisi
    # lihtsalt jätab meta = None.
    token_meta = None
    try:
        token_meta = _get_google_token_metadata(payload.id_token, audience)
    except Exception:
        token_meta = {"metadata_decode": "failed"}

    try:
        claims = google_id_token.verify_oauth2_token(
            payload.id_token,
            google_requests.Request(),
            audience,
        )

        allowed_issuers = (os.getenv("GOOGLE_ALLOWED_ISSUERS") or
                           "https://accounts.google.com,accounts.google.com").split(",")
        if claims.get("iss") not in allowed_issuers:
            raise ValueError("Invalid token issuer")

        email = (claims.get("email") or "").lower()
        if not email or not claims.get("email_verified", False):
            raise ValueError("Email not present/verified")

        first_name = claims.get("given_name") or ((claims.get("name") or "").split(" ")[0] if claims.get("name") else "")
        last_name = claims.get("family_name") or ""

        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (email, password_hash, first_name, last_name, phone, role, auth_provider, email_verified)
                VALUES ($1, NULL, $2, $3, '', 'regular', 'google', true)
                ON CONFLICT (email)
                DO UPDATE SET
                    first_name     = EXCLUDED.first_name,
                    last_name      = EXCLUDED.last_name,
                    auth_provider  = 'google',
                    email_verified = true
                """,
                email, first_name, last_name,
            )

        access_token = create_access_token(
            data={"sub": email},
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        return {"access_token": access_token}

    except ValueError as e:
        # DIAGNOSTIKA (2026-08): see haru oli varem täiesti vaikne -- kõik
        # senised Google login 401'd (vt Railway HTTP logi 2026-08-02)
        # tulid siit, aga Railway Deploy Logs'is polnud selle kohta MITTE
        # ÜHTEGI kirjet, sest siin ei olnud print()'i. google_id_token.
        # verify_oauth2_token() viskab ValueError'i audience/issuer/
        # signature/aegumise probleemide korral -- token_meta annab siia
        # juurde aud/azp/exp, et otsustada KUMB neist see täpselt oli
        # (vt ChatGPT review 2026-08-02).
        #
        # NB (turve): kliendile EI tagastata str(e) enam otse -- see
        # paljastaks oodatud OAuth client ID (audience) igale ründajale,
        # kes login-voogu proovib. Täpne tekst jääb ainult Railway
        # serverilogisse.
        print(f"⚠️ GOOGLE LOGIN 401: {type(e).__name__}: {e}; meta={token_meta}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Google token verification failed",
        )
    except Exception as e:
        print(f"❌ GOOGLE LOGIN ERROR: {type(e).__name__}: {e}; meta={token_meta}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Google token")

@router.post("/auth/login/apple", response_model=TokenOut)
@throttle(limit=20, window=60)
async def login_with_apple(payload: AppleLoginIn, request: Request):
    """
    Apple identity: apple_sub (JWT 'sub' claim) on stabiilne identifikaator
    sama Apple kasutaja, sama developer team'i ja sama rakenduse identiteedi-
    konteksti piires. E-post seevastu voib puududa mone hilisema logini identity token'is (nt kui kasutaja
    muudab Settings > Apple ID > "Share My Email" seadistust parast esimest
    loginit) -- vana kood kasutas e-posti identiteedina, mis tekitas
    duplikaatkontosid kui e-post kadus voi muutus.

    Otsingujarjekord:
      1) apple_sub jargi (stabiilne -- see on peamine tee parast seda fix'i)
      2) e-posti jargi (ainult legacy kontod, mis logisid sisse ENNE seda
         parandust ja millel apple_sub veel puudub -- link'itakse esimesel
         voimalusel)
      3) uus rida (esimene login sellelt Apple kasutajalt uldse)
    """
    try:
        claims = await verify_apple_identity_token(payload.identity_token)

        # Apple'i token PEAB sisaldama valideeritud subject-identifikaatorit,
        # soltumata sellest kas e-post on tokenis olemas.
        apple_sub = (claims.get("sub") or "").strip()
        if not apple_sub:
            raise ValueError("Apple subject identifier missing")

        email = (claims.get("email") or "").strip().lower()
        first_name = payload.first_name or ""
        last_name = payload.last_name or ""

        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1) Stabiilne identiteet -- kui see kasutaja on kunagi varem
                #    apple_sub'iga login'inud, ei puutu tema email-veergu.
                row = await conn.fetchrow(
                    "SELECT id, email FROM users WHERE apple_sub = $1 AND deleted_at IS NULL",
                    apple_sub,
                )

                if row:
                    final_email = row["email"]
                    await conn.execute(
                        """
                        UPDATE users
                        SET auth_provider  = 'apple',
                            email_verified = true,
                            first_name = CASE WHEN $2 != '' THEN $2 ELSE first_name END,
                            last_name  = CASE WHEN $3 != '' THEN $3 ELSE last_name END
                        WHERE id = $1
                        """,
                        row["id"], first_name, last_name,
                    )
                else:
                    # 2) apple_sub veel lingimata. Fallback-email on
                    #    deterministlik apple_sub pohjal (mitte juhuslik), et
                    #    see klapiks ka juba olemasoleva vana-stiilis reaga,
                    #    mis loodi enne seda parandust sama loogika jargi.
                    lookup_email = email or f"apple_{apple_sub}@privaterelay.appleid.com"

                    existing = await conn.fetchrow(
                        """
                        SELECT id, email, apple_sub
                        FROM users
                        WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL
                        FOR UPDATE
                        """,
                        lookup_email,
                    )

                    if existing:
                        existing_sub = (existing["apple_sub"] or "").strip()
                        if existing_sub and existing_sub != apple_sub:
                            # See email on juba seotud TEISE Apple kasutajaga
                            # (apple_sub erineb) -- ei tohi seda identiteeti
                            # pimesi ule kirjutada ega selle konto JWT-d
                            # valjastada.
                            raise ValueError(
                                "This email is already linked to another Apple account"
                            )

                        final_email = existing["email"]
                        await conn.execute(
                            """
                            UPDATE users
                            SET apple_sub      = COALESCE(apple_sub, $2),
                                auth_provider  = 'apple',
                                email_verified = true,
                                first_name = CASE WHEN $3 != '' THEN $3 ELSE first_name END,
                                last_name  = CASE WHEN $4 != '' THEN $4 ELSE last_name END
                            WHERE id = $1
                            """,
                            existing["id"], apple_sub, first_name, last_name,
                        )
                    else:
                        try:
                            async with conn.transaction():
                                new_row = await conn.fetchrow(
                                    """
                                    INSERT INTO users
                                        (email, password_hash, first_name, last_name, phone,
                                         role, auth_provider, email_verified, apple_sub)
                                    VALUES ($1, NULL, $2, $3, '', 'regular', 'apple', true, $4)
                                    RETURNING email
                                    """,
                                    lookup_email, first_name, last_name, apple_sub,
                                )
                            final_email = new_row["email"]
                        except asyncpg.exceptions.UniqueViolationError:
                            # Vaga vaike risk: kaks samaaegset ESIMEST Apple-
                            # loginit samalt kasutajalt voivad molemad missida
                            # apple_sub JA email lookup'i (mõlemad reavabad
                            # hetkel), siis molemad proovivad INSERT'ida --
                            # uks voidab, teine saab unique violation'i.
                            # Selle asemel et 500-ga krahhida, leiame voitja
                            # rea ules (apple_sub on usaldusvaarsem, kuna
                            # lookup_email voib kahe samaaegse paringu vahel
                            # olla identne juba definitsiooni pärast) ja
                            # lingime/uuendame selle asemel, et INSERT'ida.
                            winner = await conn.fetchrow(
                                "SELECT id, email, apple_sub FROM users WHERE apple_sub = $1 AND deleted_at IS NULL",
                                apple_sub,
                            )
                            if not winner:
                                winner = await conn.fetchrow(
                                    """
                                    SELECT id, email, apple_sub
                                    FROM users
                                    WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL
                                    """,
                                    lookup_email,
                                )
                            if not winner:
                                # Ei suutnud voitjat leida (nt kustutati
                                # vahepeal) -- laseme algsel veal labi minna.
                                raise

                            winner_sub = (winner["apple_sub"] or "").strip()
                            if winner_sub and winner_sub != apple_sub:
                                raise ValueError(
                                    "This email is already linked to another Apple account"
                                )

                            final_email = winner["email"]
                            await conn.execute(
                                """
                                UPDATE users
                                SET apple_sub      = COALESCE(apple_sub, $2),
                                    auth_provider  = 'apple',
                                    email_verified = true
                                WHERE id = $1
                                """,
                                winner["id"], apple_sub,
                            )

        access_token = create_access_token(
            data={"sub": final_email},
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        return {"access_token": access_token}

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
    except Exception as e:
        print("❌ APPLE LOGIN ERROR:", str(e))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Apple token")

@router.get("/me")
@throttle(limit=60, window=60)
async def read_current_user(request: Request, user=Depends(get_current_user)):
    return user

@router.get("/users")
@throttle(limit=60, window=60)
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
@throttle(limit=60, window=60)
async def promote_user(email: EmailStr, request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET role = 'superuser' WHERE LOWER(email) = LOWER($1)", email.lower())
    return {"status": "success", "message": f"User {email} promoted to superuser"}

@router.post("/make-regular")
@throttle(limit=60, window=60)
async def demote_user(email: EmailStr, request: Request, user=Depends(get_current_user)):
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Not authorized")
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET role = 'regular' WHERE LOWER(email) = LOWER($1)", email.lower())
    return {"status": "success", "message": f"User {email} demoted to regular"}

@router.delete("/delete-user")
@throttle(limit=60, window=60)
async def delete_user(request: Request, user=Depends(get_current_user)):
    """
    Konto kustutamine (Apple Guideline 5.1.1(v) + GDPR).

    users rida ANONUMISEERITAKSE, mitte ei kustutata pariselt -- families.created_by
    on ON DELETE CASCADE, seega hard delete kustutaks terve pere koos teiste
    liikmete andmetega.

    E-post vabastatakse (deleted_<id>_<uuid>@deleted.invalid):
      - sama aadressiga saab uuesti registreeruda (local)
      - sama Google/Apple kontoga saab uuesti sisse logida (ON CONFLICT ei leia
        vana rida -> luuakse uus rida)
      - valdib vana rea "ellu aratamist" OAuth ON CONFLICT DO UPDATE kaudu

    analytics_events jaab TAIELIKULT puutumata (kontoseost pole: user_id on
    taidetud 1 real 339-st; dashboard kasutab device_key'd).

    JWT tuhistub automaatselt (get_current_user filtreerib deleted_at IS NULL) --
    seega parast esimest edukat kustutamist ei laabi vana token enam siia.
    Allolev "already deleted" haru kaitseb peamiselt samaaegse
    kustutamisparingu (race condition) eest, mitte tavaparast korduskutset.
    """
    email = (user.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=401, detail="Invalid user")

    if email == "marko@minetech.ee":
        raise HTTPException(
            status_code=400,
            detail="System administrator account cannot be deleted here",
        )

    try:
        pool = _db_pool_or_503(request)
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id FROM users
                    WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL
                    FOR UPDATE
                    """,
                    email
                )
                if not row:
                    return {"status": "success", "message": "Account already deleted"}

                uid = row["id"]

                # basket_history.user_id EI ole FK -- see on users.id-st tuletatud
                # UUIDv5 (vt basket_history.py _coerce_to_uuid_str). Sama valem.
                history_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"grocery-user:{uid}"))

                # --- Pere: omand tuleb lahendada ENNE family_members kustutamist ---
                fam_rows = await conn.fetch(
                    "SELECT family_id FROM family_members WHERE user_id = $1", uid
                )
                for f in fam_rows:
                    fid = f["family_id"]

                    # Variant A: kustuta tema lisatud tooted. user_id tahendab
                    # "kes lisas" -- uleandmine voltsiks UI-d ("X lisas piima",
                    # kuigi tegelikult lisas kustutatud kasutaja).
                    await conn.execute(
                        "DELETE FROM family_basket_items WHERE family_id = $1 AND user_id = $2",
                        fid, uid
                    )
                    await conn.execute(
                        "DELETE FROM family_members WHERE family_id = $1 AND user_id = $2",
                        fid, uid
                    )

                    remaining = await conn.fetchval(
                        "SELECT COUNT(*) FROM family_members WHERE family_id = $1", fid
                    )
                    if remaining == 0:
                        await conn.execute(
                            "DELETE FROM family_basket_items WHERE family_id = $1", fid
                        )
                        await conn.execute("DELETE FROM families WHERE id = $1", fid)
                    else:
                        # Kui lahkuja oli omanik, anna omand vanimale allesjaanud
                        # liikmele (family_members.id = liitumise jarjekord).
                        owner = await conn.fetchval(
                            "SELECT created_by FROM families WHERE id = $1", fid
                        )
                        if owner == uid:
                            new_owner = await conn.fetchval(
                                """
                                SELECT user_id FROM family_members
                                WHERE family_id = $1
                                ORDER BY id ASC
                                LIMIT 1
                                """,
                                fid
                            )
                            if new_owner is not None:
                                await conn.execute(
                                    "UPDATE families SET created_by = $1 WHERE id = $2",
                                    new_owner, fid
                                )

                # --- Ulejaanud isikuandmed ---
                await conn.execute(
                    "DELETE FROM basket_history WHERE user_id = $1::uuid", history_uuid
                )
                await conn.execute("DELETE FROM baskets WHERE user_id = $1", uid)
                await conn.execute("DELETE FROM favourite_products WHERE user_id = $1", uid)
                await conn.execute("DELETE FROM user_product_selections WHERE user_id = $1", uid)

                # --- Anonumiseeri users rida ---
                # role = 'regular': role_check CHECK lubab AINULT
                # ('regular','superuser') -- muu vaartus rikuks piirangut.
                # deleted_at on timestamp WITHOUT time zone -> timezone('UTC', now()).
                await conn.execute(
                    """
                    UPDATE users
                    SET email = 'deleted_' || id::text || '_' ||
                                replace(gen_random_uuid()::text, '-', '') ||
                                '@deleted.invalid',
                        first_name     = '',
                        last_name      = NULL,
                        password_hash  = NULL,
                        phone          = NULL,
                        google_sub     = NULL,
                        apple_sub      = NULL,
                        picture_url    = NULL,
                        auth_provider  = NULL,
                        is_superuser   = FALSE,
                        role           = 'regular',
                        email_verified = FALSE,
                        deleted_at     = timezone('UTC', now())
                    WHERE id = $1 AND deleted_at IS NULL
                    """,
                    uid
                )

        return {"status": "success", "message": "Account deleted"}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ DELETE ERROR:", str(e))
        raise HTTPException(status_code=500, detail="Failed to delete user")


@router.post("/request-password-reset")
@throttle(limit=5, window=60)
async def request_password_reset(email: EmailStr, request: Request):
    pool = _db_pool_or_503(request)
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT 1 FROM users WHERE LOWER(email) = LOWER($1) AND deleted_at IS NULL",
            email.lower()
        )
        if not user:
            # Ära paljasta kas email eksisteerib
            return {"status": "success", "message": "If this email exists, a reset link has been sent"}

    reset_token = create_reset_token(email.lower())
    await send_reset_email(email.lower(), reset_token)
    return {"status": "success", "message": "Password reset email sent"}

@router.post("/reset-password")
@throttle(limit=10, window=60)
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
        await conn.execute(
            "UPDATE users SET password_hash = $1, auth_provider = 'local' WHERE LOWER(email) = LOWER($2)",
            hashed_pw, email
        )

    return {"status": "success", "message": "Password reset successful"}

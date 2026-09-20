import os
from pwdlib import PasswordHash
from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


# =========================
# PASSWORD HASHING
# =========================

password_hash = PasswordHash.recommended()


def hash_password(password: str) -> str:
    return password_hash.hash(password)


def verify_password(
    password: str,
    hashed_password: str
) -> bool:
    return password_hash.verify(
        password,
        hashed_password
    )


# =========================
# JWT CONFIG
# =========================

SECRET_KEY = os.getenv("JWT_SECRET")
if not SECRET_KEY or len(SECRET_KEY) < 32:
    raise RuntimeError("JWT_SECRET must be set to a random string of at least 32 characters")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


# =========================
# CREATE TOKEN
# =========================

def create_access_token(
    user_id_or_data = None,
    role: str | None = None
):
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )

    if isinstance(user_id_or_data, dict):
        payload = dict(user_id_or_data)
        if "exp" not in payload:
            payload["exp"] = expire
        if "user_id" in payload and "sub" not in payload:
            payload["sub"] = str(payload["user_id"])
    else:
        payload = {
            "sub": str(user_id_or_data),
            "role": role,
            "exp": expire,
        }

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm=ALGORITHM
    )


# =========================
# AUTHENTICATION
# =========================

security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):

    token = credentials.credentials

    try:

        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )

        user_id = payload.get("sub") or payload.get("user_id")
        role = payload.get("role")

        if user_id is None or role is None:
            raise HTTPException(
                status_code=401,
                detail="Invalid authentication token"
            )

        return {
            "user_id": int(user_id) if str(user_id).isdigit() else user_id,
            "role": role
        }

    except JWTError:

        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token"
        )


# =========================
# ROLE CHECK
# =========================

def require_driver(
    current_user: dict = Depends(get_current_user)
):

    if current_user["role"] != "driver":

        raise HTTPException(
            status_code=403,
            detail="Driver access required"
        )

    return current_user


def require_admin(
    current_user: dict = Depends(get_current_user)
):

    if current_user["role"] not in ("admin", "super_admin"):

        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )

    return current_user


def require_super_admin(
    current_user: dict = Depends(get_current_user)
):

    if current_user["role"] != "super_admin":

        raise HTTPException(
            status_code=403,
            detail="Super-admin access required"
        )

    return current_user


def require_student(
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "student":
        raise HTTPException(
            status_code=403,
            detail="Student access required"
        )

    return current_user
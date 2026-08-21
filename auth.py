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

SECRET_KEY = "KAMBUS_CHANGE_THIS_SECRET_KEY"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


# =========================
# CREATE TOKEN
# =========================

def create_access_token(
    user_id: int,
    role: str
):

    expire = datetime.now(timezone.utc) + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )

    payload = {
        "sub": str(user_id),
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

        user_id = payload.get("sub")
        role = payload.get("role")

        if user_id is None or role is None:
            raise HTTPException(
                status_code=401,
                detail="Invalid authentication token"
            )

        return {
            "user_id": int(user_id),
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

    if current_user["role"] != "admin":

        raise HTTPException(
            status_code=403,
            detail="Admin access required"
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
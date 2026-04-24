import bcrypt
from datetime import datetime, timedelta
from jose import jwt

# ===== CONFIG (giữ nguyên nếu bạn đã có) =====
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


# ===== PASSWORD =====
def hash_password(password: str) -> str:
    if not password:
        return None

    # 🔥 FIX CHÍNH: giới hạn 72 bytes cho bcrypt
    password_bytes = password.encode("utf-8")[:72]

    hashed = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    if not password or not hashed:
        return False

    return bcrypt.checkpw(
        password.encode("utf-8"),
        hashed.encode("utf-8")
    )


# ===== JWT =====
def create_access_token(subject: str, extra: dict = None) -> str:
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": subject,
        "exp": expire
    }
    if extra:
        payload.update(extra)

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(subject: str) -> str:
    expire = datetime.utcnow() + timedelta(days=7)
    payload = {
        "sub": subject,
        "exp": expire
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
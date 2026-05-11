"""
JWT Authentication helpers for Cloud Hub
"""
import jwt
import datetime
from typing import Optional

def create_token(user_id: str, jwt_secret: str, expires_hours: int = 1) -> str:
    """Create a JWT access token."""
    payload = {
        "user_id": user_id,
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=expires_hours)
    }
    return jwt.encode(payload, jwt_secret, algorithm="HS256")

def create_refresh_token(user_id: str, jwt_secret: str, expires_days: int = 7) -> str:
    """Create a JWT refresh token."""
    payload = {
        "user_id": user_id,
        "type": "refresh",
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=expires_days)
    }
    return jwt.encode(payload, jwt_secret, algorithm="HS256")

def verify_token(token: str, jwt_secret: str) -> Optional[dict]:
    """Verify and decode a JWT token. Returns None if invalid."""
    try:
        return jwt.decode(token, jwt_secret, algorithms=["HS256"])
    except jwt.InvalidTokenError:
        return None

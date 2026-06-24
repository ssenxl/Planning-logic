"""
auth.py — ระบบ login แบบเบา ใช้ stdlib ล้วน (ไม่เพิ่ม dependency)
- เก็บ user ที่ users.json: รหัสผ่านเป็น hash PBKDF2-HMAC-SHA256 + salt (ไม่เก็บ plain text)
- ออก token แบบ signed (HMAC-SHA256) มีวันหมดอายุ
- secret key สร้างอัตโนมัติครั้งแรกเก็บที่ auth_secret.txt

user ถูกกำหนดโดยผู้ดูแลผ่าน add_user.py เท่านั้น (ไม่มีสมัครเองผ่านเว็บ)
"""
import os
import json
import hmac
import base64
import hashlib
import secrets
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
USERS_FILE = Path(os.environ.get("KNITPLAN_USERS", str(BASE_DIR / "users.json")))
SECRET_FILE = Path(os.environ.get("KNITPLAN_AUTH_SECRET", str(BASE_DIR / "auth_secret.txt")))

# อายุ token (วินาที) — default 12 ชั่วโมง
TOKEN_TTL = int(os.environ.get("KNITPLAN_TOKEN_TTL", str(12 * 3600)))

PBKDF2_ITERATIONS = 200_000


# ---------------- secret key ----------------
def _get_secret() -> bytes:
    """อ่าน secret key สำหรับ sign token; สร้างใหม่ครั้งแรกถ้ายังไม่มี"""
    if SECRET_FILE.exists():
        val = SECRET_FILE.read_text(encoding="utf-8").strip()
        if val:
            return val.encode("utf-8")
    val = secrets.token_hex(32)
    SECRET_FILE.write_text(val, encoding="utf-8")
    return val.encode("utf-8")


# ---------------- password hashing ----------------
def hash_password(password: str, salt: str = None) -> dict:
    """คืน dict {salt, hash, iterations} สำหรับเก็บใน users.json"""
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), PBKDF2_ITERATIONS
    )
    return {"salt": salt, "hash": dk.hex(), "iterations": PBKDF2_ITERATIONS}


def _verify_password(password: str, record: dict) -> bool:
    iterations = record.get("iterations", PBKDF2_ITERATIONS)
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), record["salt"].encode("utf-8"), iterations
    )
    return hmac.compare_digest(dk.hex(), record.get("hash", ""))


# ---------------- users store ----------------
def load_users() -> dict:
    if USERS_FILE.exists():
        try:
            return json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_users(users: dict) -> None:
    USERS_FILE.write_text(
        json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def verify_login(username: str, password: str) -> bool:
    users = load_users()
    record = users.get(username)
    if not record:
        return False
    return _verify_password(password, record)


# ---------------- token (signed) ----------------
def _b64e(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def make_token(username: str) -> str:
    payload = {"u": username, "exp": int(time.time()) + TOKEN_TTL}
    body = _b64e(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = _b64e(hmac.new(_get_secret(), body.encode("ascii"), hashlib.sha256).digest())
    return f"{body}.{sig}"


def check_token(token: str):
    """คืน username ถ้า token ถูกต้องและยังไม่หมดอายุ ไม่งั้นคืน None"""
    if not token or "." not in token:
        return None
    body, _, sig = token.partition(".")
    expected = _b64e(
        hmac.new(_get_secret(), body.encode("ascii"), hashlib.sha256).digest()
    )
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(_b64d(body))
    except Exception:
        return None
    if payload.get("exp", 0) < int(time.time()):
        return None
    return payload.get("u")

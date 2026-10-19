"""
auth.py — ระบบ login แบบเบา ใช้ stdlib ล้วน (ไม่เพิ่ม dependency)
- เก็บ user ที่ users.json: รหัสผ่านเป็น hash PBKDF2-HMAC-SHA256 + salt (ไม่เก็บ plain text)
- ออก token แบบ signed (HMAC-SHA256) มีวันหมดอายุ
- secret key สร้างอัตโนมัติครั้งแรกเก็บที่ auth_secret.txt

สิทธิ์ 4 ระดับ:
  superadmin — admin ที่แต่งตั้ง/ถอด/ลบ admin คนอื่นได้ ; ตั้งได้ทาง add_user.py บนเซิร์ฟเวอร์เท่านั้น
  admin      — ใช้หน้าหลัก/ตั้งค่าระบบ + เพิ่ม/รีเซ็ตรหัส/ลบ user/yarn ได้
  yarn       — เหมือน user + สิทธิ์เรื่องด้าย (แก้ค่าหน้า Monitor Yarn, ปุ่มดึงข้อมูลด้าย)
               admin จัดการบัญชีนี้ได้เหมือน user ธรรมดา
  user       — ใช้งานทั่วไป
ไม่มีสมัครเองผ่านเว็บ — admin เพิ่มให้ที่ ตั้งค่าระบบ › ผู้ใช้งาน หรือผ่าน add_user.py
"""
import os
import re
import json
import hmac
import base64
import shutil
import hashlib
import secrets
import threading
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
# ไฟล์ที่ติดมากับ image (ส่งขึ้นไปพร้อม deploy) — ใช้เป็นต้นแบบครั้งแรกเท่านั้น
_BUNDLED_USERS = BASE_DIR / "users.json"
_BUNDLED_SECRET = BASE_DIR / "auth_secret.txt"
# บน server ชี้ env ไปที่ volume (docker-compose) — user ที่เพิ่มผ่านเว็บจะไม่หายตอน deploy ใหม่
USERS_FILE = Path(os.environ.get("KNITPLAN_USERS", str(_BUNDLED_USERS)))
SECRET_FILE = Path(os.environ.get("KNITPLAN_AUTH_SECRET", str(_BUNDLED_SECRET)))
USERS_LOG = USERS_FILE.with_name("users_log.jsonl")

ROLES = ("user", "yarn", "admin", "superadmin")
ADMIN_ROLES = ("admin", "superadmin")
YARN_ROLES = ("yarn", "admin", "superadmin")   # ได้สิทธิ์เรื่องด้าย — admin ทำได้ทุกอย่างที่ yarn ทำได้
WEB_ROLES = ("user", "yarn", "admin")          # สิทธิ์ที่ตั้งผ่านหน้าเว็บได้ (superadmin ตั้งได้ทาง add_user.py เท่านั้น)
MIN_PASSWORD = 6
# ชื่อผู้ใช้: ตัวอักษรอังกฤษ/ตัวเลข/._@+- (ครอบคลุมอีเมล) ห้ามมีช่องว่าง — ใช้ใน URL ได้ปลอดภัย
_NAME_RE = re.compile(r"^[A-Za-z0-9._@+\-]{2,100}$")

_lock = threading.Lock()   # กันสองคำขอแก้ users.json ทับกันระหว่างอ่าน-แก้-เขียน


def _seed_from_bundled(target: Path, bundled: Path) -> None:
    """ครั้งแรกที่ย้ายไฟล์ไปอยู่บน volume: คัดลอกของเดิมที่ติดมากับ image ไปก่อน
    (ไม่งั้น user เดิมเข้าไม่ได้ / token ที่ออกไปแล้วใช้ไม่ได้) ; มีไฟล์บน volume แล้วไม่แตะ"""
    try:
        if target.exists() or not bundled.exists() or target.resolve() == bundled.resolve():
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(bundled, target)
    except OSError:
        pass

# อายุ token (วินาที) — default 12 ชั่วโมง
TOKEN_TTL = int(os.environ.get("KNITPLAN_TOKEN_TTL", str(12 * 3600)))

PBKDF2_ITERATIONS = 200_000


# ---------------- secret key ----------------
def _get_secret() -> bytes:
    """อ่าน secret key สำหรับ sign token; สร้างใหม่ครั้งแรกถ้ายังไม่มี"""
    _seed_from_bundled(SECRET_FILE, _BUNDLED_SECRET)
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
_cache = {"mtime": None, "users": {}}


def load_users() -> dict:
    """อ่าน users.json (แคชตาม mtime — middleware เรียกทุกคำขอเพื่อเช็กว่า user ยังอยู่)
    คืนสำเนาเสมอ ผู้เรียกแก้ dict ได้โดยไม่กระทบแคช"""
    _seed_from_bundled(USERS_FILE, _BUNDLED_USERS)
    try:
        mtime = USERS_FILE.stat().st_mtime
    except OSError:
        return {}
    if _cache["mtime"] != mtime:
        try:
            _cache["users"] = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
        _cache["mtime"] = mtime
    return json.loads(json.dumps(_cache["users"]))


def save_users(users: dict) -> None:
    """เขียนลงไฟล์ชั่วคราวแล้วสลับเข้าที่ — ไฟล์ไม่เสียแม้เขียนไม่จบ (ไฟล์เสีย = ทุกคน login ไม่ได้)"""
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = USERS_FILE.with_name(USERS_FILE.name + ".tmp")
    tmp.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, USERS_FILE)
    _cache["mtime"] = None


def verify_login(username: str, password: str) -> bool:
    users = load_users()
    record = users.get(username)
    if not record:
        return False
    return _verify_password(password, record)


def _role_of(record: dict) -> str:
    r = (record or {}).get("role")
    return r if r in ROLES else "user"


def get_role(username: str) -> str:
    """คืน role ของ user ('superadmin' / 'admin' / 'yarn' / 'user'); ไม่มี field role = 'user' ธรรมดา"""
    return _role_of(load_users().get(username))


def is_admin(username: str) -> bool:
    """superadmin นับเป็น admin ด้วย — ทุกด่าน admin เดิมจึงใช้ได้เหมือนเดิม"""
    return get_role(username) in ADMIN_ROLES


def is_superadmin(username: str) -> bool:
    return get_role(username) == "superadmin"


def can_yarn(username: str) -> bool:
    """สิทธิ์เรื่องด้าย (แก้ค่า Monitor Yarn) — yarn / admin / superadmin"""
    return get_role(username) in YARN_ROLES


# ---------------- จัดการผู้ใช้ (หน้า ตั้งค่าระบบ › ผู้ใช้งาน) ----------------
class UserError(Exception):
    """ทำไม่ได้ตามกติกา — status = HTTP code ที่ server ควรตอบ"""

    def __init__(self, msg: str, status: int = 400):
        super().__init__(msg)
        self.status = status


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(actor: str, action: str, target: str, detail: str = "") -> None:
    """จดว่าใครทำอะไรกับบัญชีไหน (ไม่จดรหัสผ่าน)"""
    try:
        with open(USERS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({"at": _now(), "by": actor, "action": action,
                                "user": target, "detail": detail}, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _check_password(pw: str) -> None:
    if not pw or len(pw) < MIN_PASSWORD:
        raise UserError(f"รหัสผ่านต้องยาวอย่างน้อย {MIN_PASSWORD} ตัว")


def _can_manage(actor_role: str, target_role: str) -> bool:
    """admin จัดการได้แค่ user/yarn · superadmin จัดการ admin ได้ด้วย
    · บัญชี superadmin แตะผ่านเว็บไม่ได้เลย (แก้ได้ทาง add_user.py บนเซิร์ฟเวอร์เท่านั้น)"""
    if target_role == "superadmin":
        return False
    if target_role == "admin":
        return actor_role == "superadmin"
    return actor_role in ADMIN_ROLES


def list_users() -> list:
    """รายชื่อสำหรับหน้าเว็บ — ไม่ส่ง hash/salt ออกไป"""
    out = []
    for name, rec in load_users().items():
        out.append({
            "username": name,
            "role": _role_of(rec),
            "created_at": rec.get("created_at", ""),
            "created_by": rec.get("created_by", ""),
            "updated_at": rec.get("updated_at", ""),
        })
    order = {"superadmin": 0, "admin": 1, "yarn": 2, "user": 3}
    out.sort(key=lambda u: (order[u["role"]], u["username"].lower()))
    return out


def create_user(actor: str, username: str, password: str, role: str = "user") -> dict:
    username = (username or "").strip()
    role = role or "user"
    if not _NAME_RE.match(username):
        raise UserError("ชื่อผู้ใช้ต้องยาว 2–100 ตัว ใช้ได้เฉพาะ a-z A-Z 0-9 และ . _ @ + - (ห้ามเว้นวรรค)")
    if role not in WEB_ROLES:
        raise UserError("สิทธิ์ต้องเป็น User, Yarn หรือ Knit")
    _check_password(password)
    with _lock:
        users = load_users()
        actor_role = _role_of(users.get(actor))
        if not _can_manage(actor_role, role):
            raise UserError("เพิ่มผู้ใช้สิทธิ์ Knit ได้เฉพาะ Super Admin", 403)
        # login แยกตัวพิมพ์เล็ก-ใหญ่ → กันชื่อซ้ำแบบต่างแค่ตัวพิมพ์ ไม่งั้นคนสับสนว่าบัญชีไหนของจริง
        dup = next((n for n in users if n.lower() == username.lower()), None)
        if dup:
            raise UserError(f"มีผู้ใช้ชื่อ '{dup}' อยู่แล้ว — ถ้าจะเปลี่ยนรหัสให้กดรีเซ็ตรหัสผ่าน", 409)
        rec = hash_password(password)
        rec.update({"role": role, "created_at": _now(), "created_by": actor})
        users[username] = rec
        save_users(users)
    _log(actor, "create", username, role)
    return {"username": username, "role": role}


def reset_password(actor: str, username: str, password: str) -> dict:
    _check_password(password)
    with _lock:
        users = load_users()
        rec = users.get(username)
        if not rec:
            raise UserError(f"ไม่พบผู้ใช้ '{username}'", 404)
        if actor == username:
            raise UserError("รีเซ็ตรหัสของตัวเองที่หน้านี้ไม่ได้")
        if not _can_manage(_role_of(users.get(actor)), _role_of(rec)):
            raise UserError("ไม่มีสิทธิ์รีเซ็ตรหัสผ่านของบัญชีนี้", 403)
        rec.update(hash_password(password))   # salt ใหม่ → token เดิมของบัญชีนี้ใช้ไม่ได้ทันที
        rec["updated_at"] = _now()
        save_users(users)
    _log(actor, "reset_password", username)
    return {"username": username}


def set_role(actor: str, username: str, role: str) -> dict:
    if role not in WEB_ROLES:
        raise UserError("สิทธิ์ต้องเป็น User, Yarn หรือ Knit")
    with _lock:
        users = load_users()
        rec = users.get(username)
        if not rec:
            raise UserError(f"ไม่พบผู้ใช้ '{username}'", 404)
        if actor == username:
            raise UserError("เปลี่ยนสิทธิ์ของตัวเองไม่ได้")
        actor_role = _role_of(users.get(actor))
        old = _role_of(rec)
        # ต้องจัดการได้ทั้งสิทธิ์เดิมและสิทธิ์ใหม่ → admin สลับ user ↔ yarn ได้
        # แต่เลื่อนเป็น admin หรือถอดจาก admin = ต้องจัดการ admin ได้ → superadmin เท่านั้น
        if not (_can_manage(actor_role, old) and _can_manage(actor_role, role)):
            raise UserError("ไม่มีสิทธิ์เปลี่ยนสิทธิ์นี้ — เลื่อนเป็น/ถอดจาก Knit ได้เฉพาะ Super Admin", 403)
        if old == role:
            return {"username": username, "role": role}
        rec["role"] = role
        rec["updated_at"] = _now()
        save_users(users)
    _log(actor, "set_role", username, f"{old} → {role}")
    return {"username": username, "role": role}


def delete_user(actor: str, username: str) -> dict:
    with _lock:
        users = load_users()
        rec = users.get(username)
        if not rec:
            raise UserError(f"ไม่พบผู้ใช้ '{username}'", 404)
        if actor == username:
            raise UserError("ลบบัญชีของตัวเองไม่ได้")
        if not _can_manage(_role_of(users.get(actor)), _role_of(rec)):
            raise UserError("ไม่มีสิทธิ์ลบบัญชีนี้", 403)
        del users[username]
        save_users(users)
    _log(actor, "delete", username, _role_of(rec))
    return {"username": username}


def change_own_password(username: str, old_password: str, new_password: str) -> dict:
    """เปลี่ยนรหัสตัวเองได้เฉพาะ superadmin — ต้องรู้รหัสเดิม (กันคนที่เดินผ่านจอที่ login ค้างไว้)
    · admin/user เปลี่ยนรหัสเองไม่ได้ ต้องให้ superadmin รีเซ็ตให้ที่หน้าผู้ใช้งาน"""
    _check_password(new_password)
    with _lock:
        users = load_users()
        rec = users.get(username)
        if rec and _role_of(rec) != "superadmin":
            raise UserError("เปลี่ยนรหัสผ่านได้เฉพาะ Super Admin — ติดต่อ Super Admin ให้รีเซ็ตให้", 403)
        if not rec or not _verify_password(old_password or "", rec):
            raise UserError("รหัสผ่านเดิมไม่ถูกต้อง")
        rec.update(hash_password(new_password))
        rec["updated_at"] = _now()
        save_users(users)
    _log(username, "change_own_password", username)
    # salt เปลี่ยน → token ที่ถืออยู่ใช้ไม่ได้แล้ว ออกใบใหม่ให้จอนี้ไม่ต้อง login ซ้ำ
    return {"username": username, "token": make_token(username)}


# ---------------- token (signed) ----------------
def _b64e(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _pw_version(record: dict) -> str:
    """ลายนิ้วมือของรหัสผ่านปัจจุบัน (ตัดจาก salt) — รีเซ็ตรหัสแล้ว salt เปลี่ยน token เก่าจึงตก"""
    return (record or {}).get("salt", "")[:8]


def make_token(username: str) -> str:
    payload = {"u": username, "exp": int(time.time()) + TOKEN_TTL,
               "v": _pw_version(load_users().get(username))}
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
    username = payload.get("u")
    # บัญชีถูกลบ → เข้าไม่ได้ทันที ไม่ต้องรอ token หมดอายุ 12 ชม.
    record = load_users().get(username)
    if not record:
        return None
    # รหัสผ่านถูกเปลี่ยน/รีเซ็ต → token ที่ออกก่อนหน้านั้นใช้ไม่ได้
    # (token รุ่นก่อนไม่มี "v" — ปล่อยผ่านจนหมดอายุเอง ไม่ให้ทุกคนหลุดพร้อมกันตอน deploy)
    if "v" in payload and payload["v"] != _pw_version(record):
        return None
    return username

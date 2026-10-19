"""db_creds.py — บัญชีสำหรับต่อ Oracle (ไม่เก็บรหัสผ่านไว้ในโค้ด)

ทำไมต้องมี
  เดิมทุกไฟล์ View_*.py เขียนรหัสจริงเป็นค่า default ของ os.environ.get("SF5_PASSWORD", ...)
  รวม 6 ที่ ใครเปิดไฟล์ก็เห็น และถูกแพ็กขึ้น image ตอน deploy ไปด้วย
  ไฟล์นี้ย้ายที่มาของรหัสมาไว้จุดเดียว และไม่มีรหัสอยู่ในตัวไฟล์เลย

ลำดับที่มาของค่า (มากไปน้อย)
  1. environment variable SF5_USER / SF5_PASSWORD
     - บน server: docker-compose ส่งให้จาก env_file (/home/scm/knitplan/env/.env)
  2. ไฟล์ .env ข้าง ๆ โปรเจกต์ (c:\\vscode\\AI_plan\\.env) — สำหรับเครื่อง dev
     รูปแบบบรรทัดละ KEY=VALUE เช่น
         SF5_USER=xxxx
         SF5_PASSWORD=xxxx
     ไฟล์นี้อยู่ใน .gitignore แล้ว จึงไม่ถูก commit และไม่ถูกส่งขึ้น server
  3. ไม่มีทั้งสองทาง → คืนค่าว่าง ให้ผู้เรียกแจ้ง error บอกวิธีตั้งค่า (ไม่เดารหัสให้)

ใช้ยังไง
    import db_creds
    USER, PASSWORD = db_creds.oracle()
"""
import os
from pathlib import Path

ENV_FILE = Path(__file__).with_name(".env")

_cache: dict = {}


def _from_file() -> dict:
    """อ่าน .env แบบง่าย ๆ (KEY=VALUE บรรทัดละคู่) — ไม่มีไฟล์/อ่านไม่ได้ = {}
    รองรับคอมเมนต์ (#) และค่าที่ใส่เครื่องหมายคำพูดครอบ"""
    if _cache:
        return _cache
    try:
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]
            _cache[k.strip()] = v
    except OSError:
        pass
    return _cache


def get(key: str, default: str = "") -> str:
    """ค่าจาก env ก่อน ไม่มีค่อยดูไฟล์ .env"""
    v = os.environ.get(key, "").strip()
    return v if v else _from_file().get(key, default)


def oracle() -> tuple:
    """(user, password) สำหรับต่อ Oracle — ไม่ได้ตั้งไว้ = ("", "")"""
    return get("SF5_USER"), get("SF5_PASSWORD")


# ข้อความบอกวิธีตั้งค่า — ใช้ร่วมกันทุกไฟล์ที่ต่อ DB (ข้อความเดียว แก้ที่เดียว)
HOWTO = (
    "ยังไม่ได้ตั้งบัญชีต่อ Oracle — ตั้งอย่างใดอย่างหนึ่ง:\n"
    '  1) ตัวแปรระบบ:  $env:SF5_USER="ชื่อผู้ใช้"  และ  $env:SF5_PASSWORD="รหัสผ่าน"\n'
    f"  2) ไฟล์ {ENV_FILE} (ไม่เข้า git) เขียนบรรทัดละคู่:\n"
    "       SF5_USER=ชื่อผู้ใช้\n"
    "       SF5_PASSWORD=รหัสผ่าน"
)

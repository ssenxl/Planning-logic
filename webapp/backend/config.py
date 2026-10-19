"""
config.py — โหลด path/ตั้งค่ากลางของ webapp
อ่านจาก config.ini เดียวกับ pipeline (ให้ path Master ตรงกันเสมอ)
รองรับ override ผ่าน environment variable สำหรับ container
"""
import os
import json
import configparser
from pathlib import Path

# โครงสร้าง: REPO_DIR/webapp/backend/config.py  → parents[2] = REPO_DIR (ที่มี run_all.py)
REPO_DIR = Path(__file__).resolve().parents[2]

CONFIG_INI = Path(os.environ.get("KNITPLAN_CONFIG", str(REPO_DIR / "config.ini")))
OUTPUT_DIR = Path(os.environ.get("KNITPLAN_OUTPUT", str(REPO_DIR / "data_plan")))
LOGS_DIR = Path(os.environ.get("KNITPLAN_LOGS", str(REPO_DIR / "webapp" / "logs")))
SETTINGS_FILE = Path(os.environ.get("KNITPLAN_SETTINGS", str(REPO_DIR / "webapp" / "backend" / "settings.json")))

LOGS_DIR.mkdir(parents=True, exist_ok=True)


def _load_dotenv() -> None:
    """โหลดค่าจาก webapp/.env เข้าสู่ os.environ (ถ้ายังไม่ถูกตั้ง)
    บน server จริงใช้ env_file ของ docker อยู่แล้ว — ฟังก์ชันนี้ช่วยตอนรัน local
    เท่านั้น และจะไม่ทับค่าที่มีอยู่แล้วใน environment"""
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if not env_file.exists():
        return
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception:
        pass


_load_dotenv()


def llm_config() -> dict:
    """ค่าเชื่อมต่อ OpenAI (อ่านจาก env var; key มาจาก .env/docker env_file)
    → dict; ถ้า api_key ว่าง = ยังไม่ตั้งค่า (ฟีเจอร์ AI จะแจ้งเตือน)
    รองรับ OPENAI_BASE_URL สำหรับ endpoint ภายในบริษัท (proxy) ถ้ามี"""
    return {
        "api_key": os.environ.get("OPENAI_API_KEY", "").strip(),
        "model": os.environ.get("OPENAI_MODEL", "gpt-4o").strip(),
        "base_url": os.environ.get("OPENAI_BASE_URL", "").strip(),
    }


def load_paths() -> dict:
    """อ่าน [paths] จาก config.ini → dict ของ logical name → Path (ขยาย env var แล้ว)"""
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(CONFIG_INI, encoding="utf-8")
    paths = {}
    if cfg.has_section("paths"):
        for key, val in cfg["paths"].items():
            paths[key] = Path(os.path.expandvars(val))
    return paths


def master_files() -> dict:
    """ทะเบียนไฟล์ Master ที่แก้ผ่านเว็บได้ → {logical_name: Path}"""
    p = load_paths()
    reg = {}
    if "master_mc" in p:
        reg["MasterMC"] = p["master_mc"]
    if "calendar" in p:
        reg["Calendar"] = p["calendar"]
    if "target_stock" in p:
        reg["Target_Stock"] = p["target_stock"]
    if "master_item" in p:
        reg["Master_Item"] = p["master_item"]
    return reg




# ---------- settings.json (รอบรันอัตโนมัติ) ----------
# auto_runs = รายการรอบ แต่ละรอบ {id, enabled, hour, minute, mode, days} ; days 0 = จันทร์ … 6 = อาทิตย์
ALL_DAYS = [0, 1, 2, 3, 4, 5, 6]
DEFAULT_SETTINGS = {
    "auto_runs": [
        {"id": "r1", "enabled": True, "hour": 6, "minute": 0, "mode": "full", "days": ALL_DAYS},
    ]
}


def _legacy_runs(schedule: dict) -> list:
    """รูปแบบเดิม (ก่อน 24 ก.ย. 2569) {"schedule": {"full": {enabled, hour, minute}}} = วันละรอบเดียว
    → แปลงเป็นรายการรอบ ให้เวลาที่ตั้งไว้บน server ยังอยู่ ไม่ต้องตั้งใหม่"""
    runs = []
    for mode, cfg in schedule.items():
        if mode not in ("full", "db", "plan") or not isinstance(cfg, dict):
            continue
        runs.append({
            "id": f"r{len(runs) + 1}", "enabled": bool(cfg.get("enabled", True)),
            "hour": cfg.get("hour", 6), "minute": cfg.get("minute", 0),
            "mode": mode, "days": list(ALL_DAYS),
        })
    return runs


def load_settings() -> dict:
    merged = json.loads(json.dumps(DEFAULT_SETTINGS))
    if SETTINGS_FILE.exists():
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # merge กับ default กันค่าหาย
                merged.update(data)
                if "auto_runs" not in data and isinstance(data.get("schedule"), dict):
                    merged["auto_runs"] = _legacy_runs(data["schedule"])
        except Exception:
            pass
    merged.pop("schedule", None)
    return merged


def save_settings(data: dict) -> None:
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

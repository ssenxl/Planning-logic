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
    return reg


# ---------- settings.json (ตั้งเวลา schedule) ----------
DEFAULT_SETTINGS = {
    "schedule": {
        "full": {"enabled": True, "hour": 6, "minute": 0},
    }
}


def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            # merge กับ default กันค่าหาย
            merged = json.loads(json.dumps(DEFAULT_SETTINGS))
            merged.update(data)
            if "schedule" in data:
                merged["schedule"].update(data["schedule"])
            return merged
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_SETTINGS))


def save_settings(data: dict) -> None:
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

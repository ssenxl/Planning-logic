"""
runner.py — รัน pipeline (run_all.py) เป็น subprocess
- ล็อกกันรันซ้อน (รันได้ทีละงาน)
- เก็บ log สดใน buffer + เขียนไฟล์ log ต่อรอบ
- เก็บสถานะรันล่าสุด ให้ frontend polling ได้
"""
import sys
import time
import threading
import subprocess
from datetime import datetime
from pathlib import Path

from config import REPO_DIR, LOGS_DIR

# โหมดรัน → argument ของ run_all.py
MODES = {
    "full": [],
    "db":   ["--from", "Calendar", "--skip", "AVA_MC", "Order", "Planning"],
    "plan": ["--from", "AVA_MC"],
}
MODE_LABELS = {
    "full": "รันทั้ง Pipeline",
    "db":   "ดึงข้อมูลจาก DB",
    "plan": "รันแผนการผลิต",
}

_lock = threading.Lock()
_state = {
    "running": False,
    "mode": None,
    "label": None,
    "started_at": None,
    "finished_at": None,
    "returncode": None,
    "trigger": None,       # "manual" | "schedule"
    "log_file": None,
}
_log_lines: list[str] = []
_log_lock = threading.Lock()


def get_status() -> dict:
    with _log_lock:
        s = dict(_state)
        s["log_count"] = len(_log_lines)
    return s


def get_logs(offset: int = 0) -> dict:
    with _log_lock:
        lines = _log_lines[offset:]
        return {
            "lines": lines,
            "next_offset": offset + len(lines),
            "running": _state["running"],
            "returncode": _state["returncode"],
        }


def _append(line: str):
    with _log_lock:
        _log_lines.append(line.rstrip("\n"))


def _run(mode: str, trigger: str):
    args = MODES[mode]
    started = datetime.now()
    log_path = LOGS_DIR / f"run_{mode}_{started.strftime('%Y%m%d_%H%M%S')}.log"

    with _log_lock:
        _log_lines.clear()
        _state.update({
            "running": True, "mode": mode, "label": MODE_LABELS.get(mode, mode),
            "started_at": started.isoformat(timespec="seconds"),
            "finished_at": None, "returncode": None,
            "trigger": trigger, "log_file": log_path.name,
        })

    _append(f"=== เริ่ม [{MODE_LABELS.get(mode, mode)}] ({trigger}) {started.strftime('%Y-%m-%d %H:%M:%S')} ===")

    env = None
    import os
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"

    returncode = 1
    try:
        with open(log_path, "w", encoding="utf-8") as lf:
            proc = subprocess.Popen(
                [sys.executable, str(REPO_DIR / "run_all.py"), *args],
                cwd=str(REPO_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
            )
            for line in proc.stdout:
                _append(line)
                lf.write(line)
                lf.flush()
            proc.wait()
            returncode = proc.returncode
    except Exception as e:
        _append(f"❌ runner error: {e}")
        returncode = 1

    finished = datetime.now()
    elapsed = (finished - started).total_seconds()
    _append(f"=== จบ returncode={returncode} ใช้เวลา {elapsed:.1f}s ===")

    with _log_lock:
        _state.update({
            "running": False,
            "finished_at": finished.isoformat(timespec="seconds"),
            "returncode": returncode,
        })
    _lock.release()


def start_run(mode: str, trigger: str = "manual") -> dict:
    """เริ่มรัน 1 งาน — คืน {ok, message}. ถ้ามีงานรันอยู่จะไม่เริ่มซ้ำ"""
    if mode not in MODES:
        return {"ok": False, "message": f"โหมดไม่ถูกต้อง: {mode}"}
    if not _lock.acquire(blocking=False):
        return {"ok": False, "message": "มีงานกำลังรันอยู่ — รอให้เสร็จก่อน"}
    t = threading.Thread(target=_run, args=(mode, trigger), daemon=True)
    t.start()
    return {"ok": True, "message": f"เริ่ม {MODE_LABELS.get(mode, mode)} แล้ว"}

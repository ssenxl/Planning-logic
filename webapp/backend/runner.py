"""
runner.py — รัน pipeline (run_all.py) เป็น subprocess
- ล็อกกันรันซ้อน (รันได้ทีละงาน)
- เก็บ log สดใน buffer + เขียนไฟล์ log ต่อรอบ
- เก็บสถานะรันล่าสุด ให้ frontend polling ได้
"""
import re
import sys
import time
import threading
import subprocess
from datetime import datetime
from pathlib import Path

from config import REPO_DIR, LOGS_DIR

# marker ที่ run_all.py พิมพ์ออกมา → ใช้คำนวณ % ความคืบหน้า
_RE_TOTAL = re.compile(r"จำนวน steps\s*:\s*(\d+)")
_RE_STEP = re.compile(r"^\[(\d+)/(\d+)\]\s+(\S+)\s+[–-]\s+(.+?)\s*$")
_RE_DONE = re.compile(r"✅\s+(\S+)\s+เสร็จสิ้น")

# โหมดรัน → argument ของ run_all.py
MODES = {
    "full": [],
    "db":   ["--skip", "Calendar", "Stock", "AVA_MC", "Order", "Planning"],
    "plan": ["--from", "AVA_MC"],
    "map-item": [],
    "stock": [],
    "booking": [],
    "sc": [],
    "datamining": [],
}
MODE_LABELS = {
    "full": "รันทั้ง Pipeline",
    "db":   "ดึงข้อมูลจาก DB",
    "plan": "รันแผนการผลิต",
    "map-item": "รัน Map Item",
    "stock": "ดึง Stock",
    "booking": "ดึง Booking",
    "sc": "ดึง SC",
    "datamining": "ดึง Datamining",
}

# โหมดที่รัน script เดี่ยวตรงๆ (ไม่ผ่าน run_all.py)
SINGLE_SCRIPTS = {
    "map-item": "MapItem.py",
    "stock": "View_Stock.py",
    "booking": "View_Booking.py",
    "sc": "View_SC.py",
    "datamining": "View_Datamining.py",
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
    "total_steps": None,   # จำนวน step ทั้งหมดของรอบนี้
    "step_num": 0,         # step ที่กำลังทำ
    "step_name": None,
    "step_desc": None,     # คำอธิบาย step ปัจจุบัน
    "progress": 0,         # % ความคืบหน้า (0-100)
    "cancelled": False,    # ผู้ใช้กดหยุดรอบนี้หรือไม่
}
_log_lines: list[str] = []
_log_lock = threading.Lock()

_proc = None             # subprocess ของ run_all.py ที่กำลังรัน (None = ไม่มี)
_proc_lock = threading.Lock()


def get_status() -> dict:
    with _log_lock:
        s = dict(_state)
        s["log_count"] = len(_log_lines)
    return s


def get_logs(offset: int = 0) -> dict:
    with _log_lock:
        if offset > len(_log_lines):
            # buffer ถูกล้างเพราะรอบใหม่เริ่ม (offset ของ client ค้างจากรอบก่อน) → resync ทั้ง buffer
            lines = _log_lines[:]
            return {
                "lines": lines,
                "next_offset": len(lines),
                "reset": True,
                "running": _state["running"],
                "returncode": _state["returncode"],
            }
        lines = _log_lines[offset:]
        return {
            "lines": lines,
            "next_offset": offset + len(lines),
            "reset": False,
            "running": _state["running"],
            "returncode": _state["returncode"],
        }


def _append(line: str):
    with _log_lock:
        _log_lines.append(line.rstrip("\n"))


def _parse_progress(line: str):
    """อ่าน marker จาก stdout ของ run_all.py → อัปเดต step_num/total_steps/progress"""
    m = _RE_TOTAL.search(line)
    if m:
        with _log_lock:
            _state["total_steps"] = int(m.group(1))
            _state["step_num"] = 0
            _state["progress"] = 0
            _state["step_desc"] = None
        return
    m = _RE_STEP.match(line.strip())
    if m:
        num, total, name, desc = int(m.group(1)), int(m.group(2)), m.group(3), m.group(4)
        with _log_lock:
            _state["total_steps"] = total
            _state["step_num"] = num
            _state["step_name"] = name
            _state["step_desc"] = desc
            # เริ่ม step ใหม่ → แสดง % ของงานที่ "เสร็จไปแล้ว" (step ก่อนหน้า)
            _state["progress"] = round((num - 1) / total * 100)
        return
    if _RE_DONE.search(line):
        with _log_lock:
            total = _state.get("total_steps") or 1
            num = _state.get("step_num") or 0
            _state["progress"] = round(num / total * 100)


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
            "total_steps": None, "step_num": 0, "step_name": None,
            "step_desc": None, "progress": 0, "cancelled": False,
        })

    _append(f"=== เริ่ม [{MODE_LABELS.get(mode, mode)}] ({trigger}) {started.strftime('%Y-%m-%d %H:%M:%S')} ===")

        
    import os
    env = dict(os.environ)

    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"

    global _proc
    returncode = 1
    try:
        with open(log_path, "w", encoding="utf-8") as lf:
            popen_kw = {}
            if sys.platform == "win32":
                # ตั้งกลุ่ม process ใหม่ → kill ทั้ง tree ด้วย taskkill /T ได้
                popen_kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                # POSIX (Docker/Linux): session ใหม่ → kill ทั้ง group ด้วย os.killpg ได้
                popen_kw["start_new_session"] = True

            cmd = [sys.executable, str(REPO_DIR / SINGLE_SCRIPTS[mode])] \
                if mode in SINGLE_SCRIPTS \
                else [sys.executable, str(REPO_DIR / "run_all.py"), *args]
            proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                **popen_kw,
            )

            with _proc_lock:
                _proc = proc

            if proc.stdout is not None:
                for line in proc.stdout:
                    _append(line)
                    _parse_progress(line)
                    lf.write(line)
                    lf.flush()

            proc.wait()
            returncode = proc.returncode
    except Exception as e:
        _append(f"❌ runner error: {e}")
        returncode = 1
    finally:
        with _proc_lock:
            _proc = None


    finished = datetime.now()
    elapsed = (finished - started).total_seconds()
    cancelled = _state.get("cancelled", False)
    if cancelled:
        _append(f"=== ⛔ ยกเลิกโดยผู้ใช้ ใช้เวลา {elapsed:.1f}s ===")
    else:
        _append(f"=== จบ returncode={returncode} ใช้เวลา {elapsed:.1f}s ===")

    with _log_lock:
        _state.update({
            "running": False,
            "finished_at": finished.isoformat(timespec="seconds"),
            "returncode": returncode,
        })
        if returncode == 0 and not cancelled:
            _state["progress"] = 100   # สำเร็จครบ → เต็มหลอด
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


def stop_run() -> dict:
    """สั่งหยุดงานที่กำลังรัน — kill ทั้ง process tree (run_all.py + step ลูก)
    คืน {ok, message}. ถ้าไม่มีงานรันอยู่คืน ok=False"""
    with _proc_lock:
        proc = _proc
    if proc is None or not _state.get("running"):
        return {"ok": False, "message": "ไม่มีงานกำลังรันอยู่"}

    # ตั้ง flag ก่อน kill เพื่อให้รอบนี้รายงานว่าเป็นการยกเลิก ไม่ใช่ล้มเหลว
    with _log_lock:
        _state["cancelled"] = True
    _append("=== ⛔ ผู้ใช้สั่งหยุด — กำลังปิด process... ===")

    try:
        if sys.platform == "win32":
            # /T = รวม child ทั้ง tree (step script ที่ run_all.py spawn), /F = บังคับ
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        else:
            # POSIX (Docker/Linux): kill ทั้ง process group → รวม step ลูกที่ run_all.py spawn
            import os, signal
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
    except Exception as e:
        return {"ok": False, "message": f"หยุดไม่สำเร็จ: {e}"}

    return {"ok": True, "message": "ส่งคำสั่งหยุดแล้ว"}

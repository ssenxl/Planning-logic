"""
runner.py — รัน pipeline (run_all.py) เป็น subprocess
- ล็อกกันรันซ้อน (รันได้ทีละงาน)
- เก็บ log สดใน buffer + เขียนไฟล์ log ต่อรอบ
- เก็บสถานะรันล่าสุด ให้ frontend polling ได้
- งานดึงข้อมูลด้าย (YARN_MODES) รันใน "คิวด้าย" แยกจากคิวหลัก — ดูหัวข้อคิวด้ายข้างล่าง
"""
import os
import re
import sys
import time
import threading
import subprocess
from datetime import datetime
from pathlib import Path

import livesync
from config import REPO_DIR, LOGS_DIR

# marker ที่ run_all.py พิมพ์ออกมา → ใช้คำนวณ % ความคืบหน้า
_RE_TOTAL = re.compile(r"จำนวน steps\s*:\s*(\d+)")
_RE_STEP = re.compile(r"^\[(\d+)/(\d+)\]\s+(\S+)\s+[–-]\s+(.+?)\s*$")
_RE_DONE = re.compile(r"✅\s+(\S+)\s+เสร็จสิ้น")

# โหมดรัน → argument ของ run_all.py
MODES = {
    "full": [],
    "db":   ["--skip", "Calendar", "Stock", "AVA_MC", "Order", "ProgramLock", "Replan"],
    "plan": ["--from", "AVA_MC"],
    "map-item": [],
    "stock": [],
    "booking": [],
    "sc": [],
    "datamining": [],
    "yarn": [],
    "yarn-usage": [],
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
    "yarn": "ดึง Stock Yarn",
    "yarn-usage": "ดึงข้อมูลด้าย (Yarn Usage)",
}

# โหมดที่รัน script เดี่ยวตรงๆ (ไม่ผ่าน run_all.py) — ค่าเป็น list = รันต่อกันตามลำดับในรอบเดียว
SINGLE_SCRIPTS = {
    "map-item": "MapItem.py",
    "stock": "View_Stock.py",
    "booking": "View_Booking.py",
    "sc": "View_SC.py",
    "datamining": "View_Datamining.py",
    # ข้อมูลด้ายของหน้า Dashboard — ไม่อยู่ใน run_all (แผนไม่ได้ใช้) ดึงแยกตามรอบตั้งเวลา
    # View_Yarn_JoKnit (หน้า Yarn Usage, ไม่กี่วินาที) ขึ้นก่อน View_Yarn_Aging (หน้า Stock Yarn, ~17 นาที)
    # ตัวแรกล้มก็ยังรันตัวถัดไป — สองหน้าไม่ขึ้นต่อกัน
    "yarn": ["View_Yarn_JoKnit.py", "View_Yarn_Aging.py"],
    # ปุ่ม "ดึงข้อมูลด้าย" บนหน้า Yarn Usage — เฉพาะไฟล์ด้ายต่อใบ KP (ไม่กี่วินาที) ไม่ต้องรอรอบ yarn ตอน 06:00
    # และไม่พ่วง View_Yarn_Aging ที่ใช้ ~17 นาที
    "yarn-usage": "View_Yarn_JoKnit.py",
}

# ---- คิวด้าย (user เลือก 15 ต.ค. 2569 "แยกคิว ไม่บล็อกแผน") ----
# ดึงสต๊อกด้ายใช้ ~17 นาที ถ้าอยู่คิวเดียวกับแผน ระหว่างนั้นกดรันแผนไม่ได้ และรอบตั้งเวลาที่ตรงช่วงนั้นถูกข้าม
# → โหมดในชุดนี้รันคิวของตัวเอง (ล็อก/สถานะ/log แยก) ทั้งปุ่มบนเว็บและรอบตั้งเวลา (ผ่าน start_run เหมือนกัน)
# รันพร้อมรอบแผนได้ เพราะไฟล์ด้ายเขียนแบบไฟล์ชั่วคราว → os.replace (View_Yarn_*.save_excel)
# แผนที่อ่านไฟล์ด้ายอยู่ (YarnStockCover ผ่าน yarn_ava_live) จึงเห็นไฟล์เก่าหรือไฟล์ใหม่ทั้งไฟล์ ไม่เจอไฟล์ครึ่ง ๆ
# ไม่มีปุ่มหยุด (stop_run หยุดเฉพาะคิวหลัก) ; ไม่เขียนลง log สดของหน้าหลัก — ดู get_status()["yarn"]["tail"]
YARN_MODES = ("yarn", "yarn-usage")
_YARN_TAIL = 40         # บรรทัดท้ายของ log คิวด้ายที่ส่งให้หน้าเว็บ (โชว์สาเหตุตอนดึงไม่สำเร็จ)
_ylock = threading.Lock()
_ystate = {"running": False, "mode": None, "label": None, "started_at": None, "finished_at": None,
           "returncode": None, "trigger": None, "log_file": None}
_ytail: list[str] = []


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
    """สถานะคิวหลัก (ช่องเดิมทั้งหมด) + "yarn" = สถานะคิวด้าย {running, mode, label, started_at, ..., tail}"""
    with _log_lock:
        s = dict(_state)
        s["log_count"] = len(_log_lines)
        s["yarn"] = {**_ystate, "tail": _ytail[:]}
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


def note(line: str) -> None:
    """แทรกข้อความของระบบลง log สด (เช่น รอบอัตโนมัติถูกข้าม) ให้คนที่ดูงานรันอยู่เห็น"""
    _append(line)


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

    # จดค่าที่ตั้งไว้ต่างจากค่าเริ่มต้นลง log ทุกรอบ → ย้อนดูได้ว่าแผนรอบนั้นรันด้วยกติกาอะไร
    try:
        import plan_config_api
        _cfg_lines = plan_config_api.run_summary_lines()
        if _cfg_lines:
            _append(f"⚙️ Setting ที่ต่างจากค่าเริ่มต้น ({len(_cfg_lines)} รายการ):")
            for _l in _cfg_lines:
                _append(f"   • {_l}")
        else:
            _append("⚙️ Setting: ใช้ค่าเริ่มต้นทั้งหมด")
    except Exception as e:
        _append(f"⚙️ อ่าน Setting ไม่ได้ ({e}) — รันต่อด้วยค่าเริ่มต้น")

        
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

            scripts = SINGLE_SCRIPTS.get(mode)
            if scripts is None:
                cmds = [[sys.executable, str(REPO_DIR / "run_all.py"), *args]]
            else:
                cmds = [[sys.executable, str(REPO_DIR / s)]
                        for s in ([scripts] if isinstance(scripts, str) else scripts)]
            failed = 0      # returncode ของตัวแรกที่ล้ม (0 = ผ่านทุกตัว)
            for cmd in cmds:
                # ผู้ใช้กดหยุดระหว่างตัวก่อนหน้า → ไม่เริ่มตัวถัดไป
                if _state.get("cancelled"):
                    break
                if len(cmds) > 1:
                    head = f"--- {Path(cmd[1]).name} ---"
                    _append(head)
                    lf.write(head + "\n")
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
                failed = failed or proc.returncode
            returncode = failed
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
    # รันจบ = ไฟล์แผน/ข้อมูลเป็นของใหม่หมด → บอกทุกจอให้โหลดใหม่ (ไม่ต้องรอใครกดรีเฟรช)
    if returncode == 0 and not cancelled:
        livesync.bump(livesync.RUN, user=_state.get("trigger") or "",
                      detail=f"รัน {MODE_LABELS.get(mode, mode)} เสร็จแล้ว")
    _lock.release()


def _run_yarn(mode: str, trigger: str):
    """คิวด้าย — รันสคริปต์ของโหมดทีละตัว (ตัวแรกล้มก็ยังรันตัวถัดไป เหมือนคิวหลัก) ; ไม่มีความคืบหน้าเป็น %"""
    started = datetime.now()
    label = MODE_LABELS.get(mode, mode)
    log_path = LOGS_DIR / f"run_{mode}_{started.strftime('%Y%m%d_%H%M%S')}.log"
    with _log_lock:
        _ytail.clear()
        _ystate.update({"running": True, "mode": mode, "label": label,
                        "started_at": started.isoformat(timespec="seconds"), "finished_at": None,
                        "returncode": None, "trigger": trigger, "log_file": log_path.name})

    def keep(line: str):
        with _log_lock:
            _ytail.append(line.rstrip("\n"))
            del _ytail[:-_YARN_TAIL]

    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
    scripts = SINGLE_SCRIPTS[mode]
    failed = 0
    try:
        with open(log_path, "w", encoding="utf-8") as lf:
            def out(line: str):
                keep(line)
                lf.write(line if line.endswith("\n") else line + "\n")
                lf.flush()

            out(f"=== เริ่ม [{label}] ({trigger}) {started.strftime('%Y-%m-%d %H:%M:%S')} — คิวด้าย ===")
            for s in ([scripts] if isinstance(scripts, str) else scripts):
                out(f"--- {s} ---")
                proc = subprocess.Popen(
                    [sys.executable, str(REPO_DIR / s)], cwd=str(REPO_DIR),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                    encoding="utf-8", errors="replace", bufsize=1, env=env,
                )
                if proc.stdout is not None:
                    for line in proc.stdout:
                        out(line)
                proc.wait()
                failed = failed or proc.returncode
            out(f"=== จบ returncode={failed} ใช้เวลา {(datetime.now() - started).total_seconds():.1f}s ===")
    except Exception as e:
        keep(f"❌ runner error: {e}")
        failed = failed or 1
    finally:
        with _log_lock:
            _ystate.update({"running": False, "returncode": failed,
                            "finished_at": datetime.now().isoformat(timespec="seconds")})
        _ylock.release()
    # ไฟล์ด้ายใหม่ → บอกทุกจอให้โหลดใหม่ (ข่าวเดียวกับคิวหลัก — หน้า Stock Yarn / Yarn Usage ฟังข่าวนี้)
    if failed == 0:
        livesync.bump(livesync.RUN, user=trigger, detail=f"รัน {label} เสร็จแล้ว")


def start_run(mode: str, trigger: str = "manual") -> dict:
    """เริ่มรัน 1 งาน — คืน {ok, message}. ถ้ามีงานรันอยู่จะไม่เริ่มซ้ำ
    โหมดด้าย (YARN_MODES) ไปคิวด้าย — ไม่ติดงานในคิวหลัก และงานในคิวหลักก็ไม่ติดคิวด้าย"""
    if mode not in MODES:
        return {"ok": False, "message": f"โหมดไม่ถูกต้อง: {mode}"}
    if mode in YARN_MODES:
        if not _ylock.acquire(blocking=False):
            return {"ok": False, "message": f"กำลัง{_ystate.get('label') or 'ดึงข้อมูลด้าย'} อยู่ — รอให้เสร็จก่อน"}
        threading.Thread(target=_run_yarn, args=(mode, trigger), daemon=True).start()
        return {"ok": True, "message": f"เริ่ม {MODE_LABELS.get(mode, mode)} แล้ว (คิวด้าย — ไม่บล็อกการรันแผน)"}
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

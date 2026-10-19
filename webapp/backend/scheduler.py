"""
scheduler.py — APScheduler: รันอัตโนมัติตามรอบที่ตั้งไว้ (หลายรอบต่อวัน · เลือกโหมด · เลือกวัน)
อ่านรอบจาก settings.json ผ่าน config.load_settings()["auto_runs"]
แก้ได้ที่ ตั้งค่าระบบ › ตั้งเวลารันอัตโนมัติ (admin) — ทุกการแก้จดลงประวัติการแก้ไข
"""
import threading
import uuid
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config import load_settings, save_settings, ALL_DAYS, LOGS_DIR, OUTPUT_DIR
import livesync
import runner
import teams_notify

_scheduler: BackgroundScheduler | None = None

# โหมดที่ตั้งเวลาได้ = ปุ่มสั่งรันบน Dashboard (โหมดดึงทีละ view ไม่เปิดให้ตั้งเวลา)
# ยกเว้น yarn: Stock Yarn ไม่อยู่ใน run_all จึงมีทางเดียวที่ข้อมูลจะสดคือรอบตั้งเวลาของมันเอง
RUN_MODES = ("full", "db", "plan", "yarn")
DAY_NAMES = ("จ.", "อ.", "พ.", "พฤ.", "ศ.", "ส.", "อา.")   # 0 = จันทร์ ตรงกับ APScheduler
_CRON_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MAX_RUNS = 12
_JOB_PREFIX = "auto_"
TEAMS_JOB = "teams_mc_ava"      # การ์ด Machine Availability เข้า Teams ทุกจันทร์ 08:00

_save_lock = threading.Lock()
_last_skip: dict = {}   # id รอบ → เวลาที่ถึงรอบแต่ถูกข้าม (มีงานรันอยู่) ล่าสุด ; หายเมื่อรีสตาร์ต

# ---------- cleanup: ลบไฟล์เก่าเกินกำหนด ----------
LOG_RETENTION_DAYS = 14
OUTPUT_RETENTION_DAYS = 14
# ลบเฉพาะไฟล์ output ที่ "มีวันที่" (กองสะสม) — ไม่แตะไฟล์ทำงานที่ถูกเขียนทับทุกรอบ
_OUTPUT_PATTERNS = ("production_plan_*.xlsx", "booking_final_ready_*.xlsx")


def _delete_older_than(folder, patterns, days: int) -> int:
    cutoff = datetime.now().timestamp() - days * 86400
    removed = 0
    for pat in patterns:
        for f in folder.glob(pat):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError:
                pass
    return removed


def _cleanup():
    n_log = _delete_older_than(LOGS_DIR, ("*.log",), LOG_RETENTION_DAYS)
    n_out = _delete_older_than(OUTPUT_DIR, _OUTPUT_PATTERNS, OUTPUT_RETENTION_DAYS)
    print(f"[cleanup] ลบ log {n_log} ไฟล์, output {n_out} ไฟล์ (เกิน {LOG_RETENTION_DAYS} วัน)")


# ---------- รอบรันอัตโนมัติ ----------
def _hhmm(r: dict) -> str:
    return f"{int(r['hour']):02d}:{int(r['minute']):02d}"


def _days_text(days) -> str:
    days = sorted(days)
    if days == ALL_DAYS:
        return "ทุกวัน"
    if days == ALL_DAYS[:5]:
        return "จ.–ศ."
    if days == ALL_DAYS[:6]:
        return "จ.–ส."
    return " ".join(DAY_NAMES[d] for d in days)


def _mode_label(mode: str) -> str:
    return runner.MODE_LABELS.get(mode, mode)


def normalize(runs) -> tuple:
    """ตรวจ + จัดรูปรายการรอบ → (รอบที่ใช้ได้ เรียงตามเวลา, ข้อผิดพลาดภาษาคน)
    รอบใหม่จากหน้าเว็บยังไม่มี id → ออกให้ที่นี่ (ใช้ id จับคู่รอบเดิม/ใหม่ตอนจดประวัติ)"""
    if not isinstance(runs, list):
        return [], ["รูปแบบข้อมูลรอบรันไม่ถูกต้อง"]
    out, errors, ids = [], [], set()
    if len(runs) > MAX_RUNS:
        errors.append(f"ตั้งได้ไม่เกิน {MAX_RUNS} รอบ")
    for i, r in enumerate(runs, 1):
        if not isinstance(r, dict):
            errors.append(f"รอบที่ {i}: รูปแบบไม่ถูกต้อง")
            continue
        try:
            hour, minute = int(r.get("hour")), int(r.get("minute"))
        except (TypeError, ValueError):
            errors.append(f"รอบที่ {i}: เวลาไม่ถูกต้อง")
            continue
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            errors.append(f"รอบที่ {i}: เวลาต้องอยู่ระหว่าง 00:00–23:59")
            continue
        hhmm = f"{hour:02d}:{minute:02d}"
        mode = str(r.get("mode") or "full")
        if mode not in RUN_MODES:
            errors.append(f"รอบ {hhmm}: ไม่รู้จักงาน '{mode}'")
            continue
        days = r.get("days")
        try:
            days = sorted({int(d) for d in (ALL_DAYS if days is None else days) if 0 <= int(d) <= 6})
        except (TypeError, ValueError):
            days = []
        if not days:
            errors.append(f"รอบ {hhmm}: ยังไม่ได้เลือกวันที่จะรัน")
            continue
        rid = str(r.get("id") or "").strip()
        if not rid or rid in ids:
            rid = uuid.uuid4().hex[:8]
        ids.add(rid)
        out.append({"id": rid, "enabled": bool(r.get("enabled", True)),
                    "hour": hour, "minute": minute, "mode": mode, "days": days})
    # รันได้ทีละงาน → เวลาเดียวกันวันเดียวกัน รอบหลังถูกข้ามแน่นอน
    seen = set()
    for r in out:
        if not r["enabled"]:
            continue
        for d in r["days"]:
            k = (d, r["hour"], r["minute"])
            if k in seen:
                errors.append(f"รอบ {_hhmm(r)} ซ้ำกับอีกรอบในวัน {DAY_NAMES[d]} — ระบบรันได้ทีละงาน รอบหลังจะถูกข้าม")
                break
            seen.add(k)
    out.sort(key=lambda r: (r["hour"], r["minute"]))
    return out, errors


def _runs() -> list:
    return normalize(load_settings().get("auto_runs"))[0]


def _job(run_id: str, mode: str, hhmm: str):
    r = runner.start_run(mode, trigger="schedule")
    if not r.get("ok"):
        # เดิมข้ามเงียบ ๆ → จดให้เห็นทั้งใน log สดของงานที่กำลังรัน และบนหน้าตั้งเวลา
        _last_skip[run_id] = datetime.now().isoformat(timespec="seconds")
        line = f"⏰ รอบอัตโนมัติ {hhmm} ({_mode_label(mode)}) ถูกข้าม — {r.get('message', '')}"
        runner.note(line)
        print(f"[schedule] {line}")


def _apply_jobs():
    sched = _scheduler
    for job in sched.get_jobs():
        # "full" = id ของ job รูปแบบเดิม (ก่อนมีหลายรอบ)
        if job.id.startswith(_JOB_PREFIX) or job.id == "full":
            sched.remove_job(job.id)
    for r in _runs():
        if not r["enabled"]:
            continue
        sched.add_job(
            _job, args=[r["id"], r["mode"], _hhmm(r)], id=_JOB_PREFIX + r["id"],
            trigger=CronTrigger(day_of_week=",".join(_CRON_DAYS[d] for d in r["days"]),
                                hour=r["hour"], minute=r["minute"]),
            replace_existing=True, misfire_grace_time=3600,
        )


def start():
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
        _scheduler.start()
    _apply_jobs()
    # cleanup ไฟล์เก่า: รันทันที 1 ครั้ง + ตั้ง job รายวัน 00:30
    _cleanup()
    _scheduler.add_job(
        _cleanup, id="cleanup_logs",
        trigger=CronTrigger(hour=0, minute=30),
        replace_existing=True, misfire_grace_time=3600,
    )
    # การ์ด Machine Availability + ลิงก์ไม่ต้อง login เข้า Teams (teams_notify.py) — ยังไม่ตั้ง
    # KP_TEAMS_URL ก็แค่จด log ว่าข้าม ; ไม่ใช่รอบ "auto_" จึงไม่ถูก _apply_jobs ลบทิ้งตอนแก้ตารางเวลา
    _scheduler.add_job(
        teams_notify.weekly_job, id=TEAMS_JOB,
        trigger=CronTrigger(day_of_week="mon", hour=8, minute=0),
        replace_existing=True, misfire_grace_time=3600,
    )


def _desc(r: dict) -> str:
    return (f"{_hhmm(r)} · {_mode_label(r['mode'])} · {_days_text(r['days'])}"
            + ("" if r["enabled"] else " (ปิดไว้)"))


def _diff(before: list, after: list) -> list:
    """บรรทัดประวัติ: แก้อะไรไปบ้าง จับคู่รอบเดิม/ใหม่ด้วย id"""
    old = {r["id"]: r for r in before}
    new_ids = {r["id"] for r in after}
    lines = []
    for r in after:
        o = old.get(r["id"])
        if o is None:
            lines.append(f"เพิ่มรอบ {_desc(r)}")
            continue
        parts = []
        if _hhmm(o) != _hhmm(r):
            parts.append(f"เวลา {_hhmm(o)} → {_hhmm(r)}")
        if o["mode"] != r["mode"]:
            parts.append(f"งาน {_mode_label(o['mode'])} → {_mode_label(r['mode'])}")
        if o["days"] != r["days"]:
            parts.append(f"วัน {_days_text(o['days'])} → {_days_text(r['days'])}")
        if o["enabled"] != r["enabled"]:
            parts.append("เปิด → ปิด" if o["enabled"] else "ปิด → เปิด")
        if parts:
            lines.append(f"รอบ {_hhmm(o)}: " + " · ".join(parts))
    for r in before:
        if r["id"] not in new_ids:
            lines.append(f"ลบรอบ {_desc(r)}")
    return lines


def save_runs(runs, user: str = "") -> dict:
    """บันทึกรอบใหม่ทั้งชุด + ตั้ง job ใหม่ + จดประวัติ → {ok, ...status} หรือ {ok: False, errors}"""
    clean, errors = normalize(runs)
    if errors:
        return {"ok": False, "errors": errors}
    with _save_lock:
        settings = load_settings()
        before = normalize(settings.get("auto_runs"))[0]
        settings["auto_runs"] = clean
        save_settings(settings)
        _apply_jobs()
    lines = _diff(before, clean)
    on = [r for r in clean if r["enabled"]]
    livesync.bump(livesync.SCHEDULE, user=user,
                  detail=f"ตั้งเวลารันอัตโนมัติ {len(on)} รอบ",
                  changes={"lines": lines, "total": len(lines)})
    return {"ok": True, **status()}


def status() -> dict:
    """รอบทั้งหมด + เวลารันครั้งถัดไป + รอบที่จะถึงก่อน (หน้าตั้งเวลา / Dashboard / หน้า Data)"""
    items = []
    for r in _runs():
        job = _scheduler.get_job(_JOB_PREFIX + r["id"]) if _scheduler else None
        items.append({
            **r,
            "mode_label": _mode_label(r["mode"]),
            "days_text": _days_text(r["days"]),
            "next_run": job.next_run_time.isoformat() if (job and job.next_run_time) else None,
            "last_skipped": _last_skip.get(r["id"]),
        })
    upcoming = [x for x in items if x["next_run"]]
    nxt = min(upcoming, key=lambda x: x["next_run"]) if upcoming else None
    info = livesync.info(livesync.SCHEDULE)
    return {
        "runs": items,
        "next": nxt,
        "modes": [{"id": m, "label": _mode_label(m)} for m in RUN_MODES],
        "max_runs": MAX_RUNS,
        "updated_at": info.get("at"),
        "updated_by": info.get("by"),
    }

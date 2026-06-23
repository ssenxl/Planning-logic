"""
scheduler.py — APScheduler: 06:00 ดึง DB / 07:00 รันแผน (ปรับเวลา/เปิด-ปิดได้)
อ่านเวลาจาก settings.json ผ่าน config.load_settings()
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config import load_settings, save_settings
import runner

_scheduler: BackgroundScheduler | None = None

JOB_MODE = {"db_pull": "db", "plan": "plan"}


def _job(mode: str):
    runner.start_run(mode, trigger="schedule")


def _apply_jobs():
    sched = _scheduler
    settings = load_settings()["schedule"]
    for job_id, cfg in settings.items():
        mode = JOB_MODE.get(job_id)
        if not mode:
            continue
        # ลบ job เดิมถ้ามี
        if sched.get_job(job_id):
            sched.remove_job(job_id)
        if cfg.get("enabled", True):
            sched.add_job(
                _job, args=[mode], id=job_id,
                trigger=CronTrigger(hour=int(cfg["hour"]), minute=int(cfg["minute"])),
                replace_existing=True, misfire_grace_time=3600,
            )


def start():
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone="Asia/Bangkok")
        _scheduler.start()
    _apply_jobs()


def reschedule(new_schedule: dict) -> dict:
    """อัปเดต settings.json + ตั้ง job ใหม่ คืน next run times"""
    settings = load_settings()
    settings["schedule"].update(new_schedule)
    save_settings(settings)
    _apply_jobs()
    return next_runs()


def next_runs() -> dict:
    out = {}
    settings = load_settings()["schedule"]
    for job_id, cfg in settings.items():
        job = _scheduler.get_job(job_id) if _scheduler else None
        out[job_id] = {
            "enabled": cfg.get("enabled", True),
            "hour": cfg.get("hour"),
            "minute": cfg.get("minute"),
            "next_run": job.next_run_time.isoformat() if (job and job.next_run_time) else None,
        }
    return out

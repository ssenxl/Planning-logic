"""
Run All Pipeline
================
รัน script ทั้งหมดตามลำดับ:
  1. Calendar.py     – ตรวจสอบ Calendar
  2. View_Booking.py – ดึงข้อมูล Booking จาก Oracle DB
  3. View_Stock.py   – ดึงข้อมูล Stock จาก Oracle DB
  4. View_SC.py      – ดึงข้อมูล SC Pending จาก Oracle DB
  5. Stock.py        – ประมวลผลข้อมูล Stock
  6. AVA_MC.py       – คำนวณ Machine Availability
  7. Order.py        – เตรียม Order data
  8. Planning.py     – รัน Planning หลัก
Usage:
    python run_all.py
    python run_all.py --skip View_Stock    # ข้าม View_Stock (ถ้า DB ไม่พร้อม)
    python run_all.py --from AVA_MC        # เริ่มจาก step ที่ระบุ
"""

import subprocess
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# บังคับ stdout/stderr เป็น UTF-8 กัน UnicodeEncodeError จากอิโมจิ (✅❌⛔)
# บน Windows console ที่ใช้ code page cp874 (ไทย)
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except (AttributeError, ValueError):
    pass

# Force PyInstaller to bundle all dependencies used by pipeline scripts
import configparser, re, io, math
import urllib.request, urllib.parse
import pandas, numpy, openpyxl
from Calendar import load_calendar as _lc  # bundles Calendar module + its deps
try:
    import oracledb
except ImportError:
    pass
try:
    import xlrd
except ImportError:
    pass
try:
    import msal
except ImportError:
    pass

BASE_DIR = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent

STEPS = [
    {
        "name": "Calendar",
        "script": BASE_DIR / "Calendar.py",
        "desc": "ตรวจสอบ Calendar (validation)",
    },
    {
        "name": "View_Booking",
        "script": BASE_DIR / "View_Booking.py",
        "desc": "ดึงข้อมูล Booking จาก Oracle DB",
    },
    {
        "name": "View_Stock",
        "script": BASE_DIR / "View_Stock.py",
        "desc": "ดึงข้อมูล Stock จาก Oracle DB",
    },
    {
        "name": "View_SC",
        "script": BASE_DIR / "View_SC.py",
        "desc": "ดึงข้อมูล SC Pending จาก Oracle DB",
    },
    {
        "name": "Stock",
        "script": BASE_DIR / "Stock.py",
        "desc": "ประมวลผลข้อมูล Stock",
    },
    {
        "name": "AVA_MC",
        "script": BASE_DIR / "AVA_MC.py",
        "desc": "คำนวณ Machine Availability",
    },
    {
        "name": "Order",
        "script": BASE_DIR / "Order.py",
        "desc": "เตรียม Order data",
    },
    {
        "name": "Planning",
        "script": BASE_DIR / "Planning.py",
        "desc": "รัน Planning หลัก",
    },
]


def separator(char="=", width=70):
    print(char * width)


def run_step(step: dict, step_num: int, total: int) -> bool:
    name = step["name"]
    script = step["script"]
    desc = step["desc"]

    separator()
    print(f"[{step_num}/{total}] {name}  –  {desc}")
    print(f"      Script : {script}")
    print(f"      Started: {datetime.now().strftime('%H:%M:%S')}")
    separator("-")

    t0 = time.time()
    if getattr(sys, 'frozen', False):
        import runpy, traceback as _tb
        script_in_bundle = Path(sys._MEIPASS) / script.name
        returncode = 0
        try:
            runpy.run_path(str(script_in_bundle), run_name="__main__")
        except SystemExit as e:
            returncode = 0 if (e.code is None or e.code == 0) else int(e.code)
        except Exception:
            _tb.print_exc()
            returncode = 1
    else:
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(BASE_DIR),
            stdout=None,
            stderr=None,
        )
        returncode = result.returncode
    elapsed = time.time() - t0

    separator("-")
    if returncode == 0:
        print(f"✅ {name} เสร็จสิ้น  (ใช้เวลา {elapsed:.1f} วินาที)")
    else:
        print(f"❌ {name} ล้มเหลว  return code={returncode}  (ใช้เวลา {elapsed:.1f} วินาที)")
    separator()
    print()
    return returncode == 0


def parse_args():
    parser = argparse.ArgumentParser(description="Run all planning pipeline steps")
    parser.add_argument(
        "--skip",
        nargs="+",
        metavar="STEP",
        default=[],
        help="ข้าม step ที่ระบุ เช่น --skip View_Stock Calendar",
    )
    parser.add_argument(
        "--from",
        dest="start_from",
        metavar="STEP",
        default=None,
        help="เริ่มจาก step ที่ระบุ เช่น --from AVA_MC",
    )
    parser.add_argument(
        "--ignore-errors",
        action="store_true",
        default=False,
        help="ทำงานต่อแม้ step ก่อนหน้าจะล้มเหลว",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    skip_names = {s.lower() for s in args.skip}
    start_from = args.start_from.lower() if args.start_from else None

    # กรอง steps ตาม --from และ --skip
    steps_to_run = []
    started = start_from is None
    for step in STEPS:
        if not started:
            if step["name"].lower() == start_from:
                started = True
            else:
                continue
        if step["name"].lower() in skip_names:
            continue
        steps_to_run.append(step)

    if not steps_to_run:
        print("❌ ไม่มี step ที่จะรัน กรุณาตรวจสอบ --from และ --skip")
        sys.exit(1)

    total = len(steps_to_run)
    separator("=")
    print("  PLANNING PIPELINE  –  Run All")
    print(f"  จำนวน steps : {total}")
    print(f"  เริ่มต้น     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    separator("=")
    print()

    overall_start = time.time()
    failed_steps = []

    for i, step in enumerate(steps_to_run, start=1):
        success = run_step(step, i, total)
        if not success:
            failed_steps.append(step["name"])
            if not args.ignore_errors:
                print(f"⛔ หยุดการทำงานเนื่องจาก {step['name']} ล้มเหลว")
                print("   ใช้ --ignore-errors เพื่อทำงานต่อแม้มี error")
                break

    overall_elapsed = time.time() - overall_start
    separator("=")
    print(f"  สรุปผล  –  ใช้เวลารวม {overall_elapsed:.1f} วินาที")
    separator("-")
    for step in steps_to_run:
        status = "❌ FAILED" if step["name"] in failed_steps else "✅ OK"
        print(f"  {status}  {step['name']}")
    separator("=")

    if failed_steps:
        sys.exit(1)


if __name__ == "__main__":
    main()

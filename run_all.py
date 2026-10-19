"""
Run All Pipeline
================
รัน script ทั้งหมดตามลำดับ:
  1. Calendar.py        – ตรวจสอบ Calendar
  2. View_Booking.py    – ดึงข้อมูล Booking จาก Oracle DB
  3. View_Stock.py      – ดึงข้อมูล Stock จาก Oracle DB
  4. View_SC.py         – ดึงข้อมูล SC Pending จาก Oracle DB
  5. View_SCFol_Item.py – ดึงยอดที่เปิดสีแล้ว (OPEN_COLOR_LOSS ต่อ SC+ITEM) จาก Oracle DB
  6. View_SCFol_Yarn.py – ดึงด้ายรายบรรทัดใบ SC (% ด้าย + PO + วัน PO เข้า) ให้กติกายืมด้ายสต๊อก
  7. View_PO_Yarn.py    – ดึง PO ด้าย (PO Pending) ให้กติกาวางแผนได้ไม่เกินด้ายที่มี
  8. View_Datamining.py – ดึงข้อมูล Datamining จาก Oracle DB
  9. View_MC_Item.py    – ดึง CAP รายไอเทม-รายเครื่อง จาก Oracle DB (AVA_MC ใช้แทน CAP_KNIT)
 10. Stock.py           – ประมวลผลข้อมูล Stock
 11. AVA_MC.py          – คำนวณ Machine Availability
 12. Order.py           – เตรียม Order data
 13. program_lock.py    – ล็อกตั๋วโปรแกรมที่จองเครื่องแล้ว (ต้องอยู่ก่อน Replan)
 14. Replan.py          – แผนผลิต: booking ที่ปลดล็อก + SC วางพร้อมกัน (เรียก Planning.py โหมด replan)
                          → data_plan/production_plan_*.xlsx ชีท PLAN
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

BASE_DIR = Path(__file__).parent

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
        # แหล่งเดียวของ "ยอดที่เปิดสีแล้ว" (OPEN_COLOR_LOSS ต่อ SC+ITEM) ที่ Planning/webapp ใช้
        # ต้องดึงก่อน Planning — ไม่มีไฟล์นี้ = ไม่มีงานสีในระบบเลย
        "name": "View_SCFol_Item",
        "script": BASE_DIR / "View_SCFol_Item.py",
        "desc": "ดึงยอดที่เปิดสีแล้ว (SC Follow Item) จาก Oracle DB",
    },
    {
        # ด้ายรายบรรทัดใบ SC (% ด้าย + PO + วัน PO เข้า) — Replan ใช้ดูว่าใบที่รอ PO IN
        # รอด้ายเส้นไหน แล้วยืมด้ายในสต๊อกได้ไหม (YarnStockCover.py ; สวิตช์ที่หน้า Monitor Yarn)
        # ล้ม = ใบรอ PO IN ตามเดิม แผนยังออกได้ → ไม่ทำให้ทั้งไปป์ไลน์ fail
        "name": "View_SCFol_Yarn",
        "script": BASE_DIR / "View_SCFol_Yarn.py",
        "desc": "ดึงด้ายรายบรรทัดใบ SC (SC Follow Yarn) จาก Oracle DB",
        "optional": True,
    },
    {
        # PO ด้าย (PO Pending = ยังค้างส่ง + วันเข้าล่าสุด) — Replan ใช้คุมแผนไม่ให้เกินด้ายที่มี
        # (สต๊อก + PO Pending : YarnStockCover.supply_cap ; สวิตช์ที่หน้า Monitor Yarn) ~40 วิ
        # ล้ม = ไม่คุมด้ายรอบนี้ แผนยังออกได้ → ไม่ทำให้ทั้งไปป์ไลน์ fail
        "name": "View_PO_Yarn",
        "script": BASE_DIR / "View_PO_Yarn.py",
        "desc": "ดึง PO ด้าย (PO Pending) จาก Oracle DB",
        "optional": True,
    },
    {
        "name": "View_Datamining",
        "script": BASE_DIR / "View_Datamining.py",
        "desc": "ดึงข้อมูล Datamining จาก Oracle DB",
    },
    {
        "name": "View_MC_Item",
        "script": BASE_DIR / "View_MC_Item.py",
        "desc": "ดึง CAP รายไอเทม-รายเครื่อง จาก Oracle DB",
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
        # ต้องอยู่ "ก่อน Replan" — ล็อกจึงมีผลกับ Replan ของรอบเดียวกันเลย ไม่ต้องรอรอบถัดไป
        # (อ่าน booking_final ที่ AVA_MC เขียนไว้แล้ว ; ไม่แตะฝั่งแผน — ดูหัวไฟล์ program_lock.py)
        "name": "ProgramLock",
        "script": BASE_DIR / "program_lock.py",
        "desc": "ล็อกตั๋วโปรแกรม + core item ที่จองเครื่องแล้ว (ชีท Lock_Plan)",
        # เป็นการเติมล็อกให้ Replan ไม่ใช่ตัวแผนเอง → ล้มแล้วต้องไม่ทำให้ทั้งไปป์ไลน์ fail
        "optional": True,
    },
    {
        # แผนผลิตมีรอบเดียวแล้ว = Replan (booking ที่ปลดล็อก + SC วางพร้อมกัน)
        # Replan.py เรียก Planning.py โหมด replan เอง แล้วเขียนไฟล์ production_plan_*.xlsx (ชีท PLAN)
        # เลิกรัน Planning รอบหลัก (แผน SC ล้วน) ตั้งแต่ 2026-09-11 — ล้ม = ไม่มีแผน → ต้องหยุด ไม่ข้าม
        "name": "Replan",
        "script": BASE_DIR / "Replan.py",
        "desc": "รันแผนการผลิต (booking ที่ปลดล็อก + SC)",
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


def _ensure_calendar():
    """เติมปฏิทินปีถัดไปให้อัตโนมัติก่อนรัน pipeline (idempotent, ไม่ crash ถ้าพลาด)"""
    try:
        from gen_calendar import ensure_calendar_extended
        from Calendar import _CALENDAR_LOCAL_PATH
        res = ensure_calendar_extended(_CALENDAR_LOCAL_PATH)
        if res.get("added"):
            print(f"📅 เติมปฏิทินปีถัดไปอัตโนมัติ: +{res['added']} วัน → ถึงสิ้นปี {res.get('end_year')} "
                  f"(backup: {res.get('backup')})")
    except Exception as e:
        print(f"⚠️ ข้ามการเติมปฏิทินอัตโนมัติ ({e})")


def main():
    args = parse_args()

    _ensure_calendar()

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
    ran_steps = []

    for i, step in enumerate(steps_to_run, start=1):
        ran_steps.append(step["name"])
        success = run_step(step, i, total)
        if not success:
            failed_steps.append(step["name"])
            if step.get("optional"):
                print(f"⚠️ {step['name']} ล้มเหลว แต่เป็น step เสริม → ทำงานต่อ "
                      f"(แผนหลักยังรันได้ แค่ไม่มีชีทของ step นี้)")
                continue
            if not args.ignore_errors:
                print(f"⛔ หยุดการทำงานเนื่องจาก {step['name']} ล้มเหลว")
                print("   ใช้ --ignore-errors เพื่อทำงานต่อแม้มี error")
                break

    overall_elapsed = time.time() - overall_start
    separator("=")
    print(f"  สรุปผล  –  ใช้เวลารวม {overall_elapsed:.1f} วินาที")
    separator("-")
    for step in steps_to_run:
        if step["name"] in failed_steps:
            # step เสริมล้ม = แผนหลักยังใช้ได้ → แยกป้ายให้เห็นชัด ไม่ปนกับความล้มเหลวจริง
            status = "⚠️  ข้าม (เสริม)" if step.get("optional") else "❌ FAILED"
        elif step["name"] in ran_steps:
            status = "✅ OK"
        else:
            # pipeline หยุดกลางทาง → step นี้ยังไม่ถึงคิว (ไม่ใช่ว่าสำเร็จ)
            status = "⏭️  ไม่ได้รัน"
        print(f"  {status}  {step['name']}")
    separator("=")

    # step เสริมที่ล้มไม่ทำให้ทั้งการรันถือว่าล้มเหลว (แผนหลักออกครบแล้ว)
    _optional_names = {s["name"] for s in STEPS if s.get("optional")}
    _hard_failed = [s for s in failed_steps if s not in _optional_names]
    if _hard_failed:
        sys.exit(1)
    print("\n✅ เสร็จทั้งหมดแล้ว")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)

import os
import pandas as pd
import io
import math
import re
import sys
from datetime import date, datetime
import configparser as _cp
from pathlib import Path
from Calendar import load_calendar, calendar_week_map, WeekTimeline, yw_week, yw_year, yw_label
import WorkDay  # ต้อง import ก่อน _base_work_hours ถูกเรียกที่ระดับ module (WorkDay.init เรียกทีหลังตอน master_mc พร้อม)
import ColorOpen  # แหล่งเดียวของ "ยอดที่เปิดสีแล้ว" (view ยอดเปิดสีของใบ SC)
import McGroups  # แหล่งเดียวของ "กลุ่มที่ไม่ใช่เครื่องทอของเรา" (จ้างทออ่านจาก MasterMC)
import plan_config  # ค่าปรับแต่งที่แก้ได้จากหน้า "ตั้งค่าระบบ" (แทน env KP_* เดิม ; env ยังทับได้)


def _base_work_hours(mc_group, gauge=None) -> float:
    """ชั่วโมงฐานของกลุ่มสำหรับแปลง CAP (ฐาน 24 ชม.) → กำลังผลิตจริง
    ชั่วโมงของกลุ่มมาจากชีท Work Day (หน้า WorkDayPanel) เท่านั้น · ไม่ตั้ง = 24 ชม.
    ไม่ใช้ Working Hours. ของ MasterMC แล้ว
    หมายเหตุ: Item Special ยังชนะเสมอ — จัดการที่ adjust_daily_cap_for_item_special แยกต่างหาก"""
    return WorkDay.get_working_hours(mc_group, gauge)


# Set UTF-8 encoding for Windows console

if sys.platform == "win32":
    # ห้าม detach buffer — ตอนรันเป็น exe ทุก step อยู่ใน process เดียวกัน
    # ถ้า detach แล้ว input() ของ run_all จะพังหลัง Planning จบ
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass
# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).parent
_cfg = _cp.ConfigParser(interpolation=None)  # ปิด interpolation ให้ใช้ %USERPROFILE% ใน path ได้
if not _cfg.read(BASE_DIR / "config.ini", encoding="utf-8"):
    raise FileNotFoundError(f"ไม่พบ config.ini ที่ {BASE_DIR / 'config.ini'} — กรุณาสร้างไฟล์ config.ini ก่อนรัน")
TODAY = pd.Timestamp.today().normalize()
DATA_PLAN_DIR = BASE_DIR / "data_plan"
DATA_DIR = BASE_DIR / "data"
ITEMCORE_DIR = DATA_DIR / "Itemcore"
ORDER_FILE = DATA_PLAN_DIR / "order_ready.xlsx"
def _latest_booking_final():
    """หาไฟล์ booking_final_ready ที่ใหม่สุด (ชื่อมีวันที่ พ.ศ.) ; fallback ชื่อเก่า"""
    cands = sorted(DATA_PLAN_DIR.glob("booking_final_ready_*.xlsx"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    if cands:
        return cands[0]
    return DATA_PLAN_DIR / "booking_final_ready25.xlsx"  # backward-compat
MC_REMAIN_FILE = _latest_booking_final()


def _load_outsource_split() -> dict:
    """อ่านการแบ่งงานไปจ้างทอที่ user ยืนยันจากหน้าเว็บ
    → { ITEM_CODE: {"outsource_qty": float, "start_week": int|None} }

    ไฟล์ค้างอยู่จนกว่า user จะลบเอง (งานที่ส่งไปจ้างทอแล้ว แผนรอบถัดไปต้องไม่ดึงกลับมาทอในบ้าน)
    รายการที่ item ไม่มีของค้างแล้วจะถูกข้ามเองตอน apply"""
    import json as _json
    if not OUTSOURCE_SPLIT_FILE.exists():
        return {}
    try:
        data = _json.loads(OUTSOURCE_SPLIT_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[OUTSOURCE] อ่าน {OUTSOURCE_SPLIT_FILE.name} ไม่ได้: {e}")
        return {}
    out = {}
    for k, v in (data.get("items") or {}).items():
        if not isinstance(v, dict):
            continue
        try:
            qty = float(v.get("outsource_qty") or 0)
        except (TypeError, ValueError):
            continue
        if qty <= 0:
            continue
        try:
            wk = int(v.get("start_week")) if v.get("start_week") not in (None, "") else None
        except (TypeError, ValueError):
            wk = None
        out[str(k).strip().upper()] = {"outsource_qty": qty, "start_week": wk}
    return out
ITEMCORE_FILE = ITEMCORE_DIR / "Itemcore.xlsx"
# การแบ่งงานไปจ้างทอที่ user ยืนยันจากหน้าเว็บ (หน้า "จ้างทอ (AI)")
OUTSOURCE_SPLIT_FILE = DATA_PLAN_DIR / "outsource_split.json"
# Change Cylinder ที่ user ติ๊กอนุมัติเองจากหน้าเว็บ (webapp/backend/cyl_approve.py เป็นคนเขียน)
# = เครื่องที่ย้ายเกจไปแล้วจริง → รอบวางแผนต้องเห็นตั้งแต่ต้นรอบ ไม่ต้องให้ user ติ๊กซ้ำ
APPROVED_CYL_FILE = DATA_PLAN_DIR / "cylinder_approved.json"
CALENDAR_FILE = "https://nanyangtextilegroup.sharepoint.com/:x:/s/SCM_Cloud/IQCXP4jH73zhQozDNvw1XF8OAY5m4p-UFv35Tcpza6v8mJo?e=43ffCc"
BOOKING_DIR = BASE_DIR / "Booking"
today = datetime.now()
OUTPUT_FILE = DATA_PLAN_DIR / f"production_plan_{today.day}-{today.month}-{today.year+543}_{today.hour:02d}{today.minute:02d}.xlsx"

# =========================
# REPLAN MODE — Replan.py ตั้ง env ให้ก่อนเรียกไฟล์นี้อีกรอบ
# วางแผนใหม่โดยเอา "งาน booking ตั้งแต่สัปดาห์ที่ปลดล็อก" มาเป็นดีมานด์รวมกับ SC
# ใช้ logic การวางแผนชุดเดียวกับรอบปกติทั้งหมด ต่างแค่ไฟล์ input/output
# → แผนหลัก (production_plan) ไม่ถูกแตะ ผลรอบ replan ไปอยู่ในไฟล์แยกแล้วค่อยแปะเป็นชีท
# =========================
REPLAN_MODE = os.environ.get("KP_REPLAN") == "1"
# ผลลัพธ์รอบ replan — รอบปกติอ่านไฟล์นี้ไปแปะเป็นชีท REPLAN / REPLAN_COMPARE
REPLAN_RESULT_FILE = Path(os.environ.get("KP_REPLAN_OUTPUT") or (DATA_PLAN_DIR / "replan_result.xlsx"))
# แถวที่ Replan.py เตรียมไว้ให้แปะเข้าแผนโดยไม่จัดใหม่ (งานที่ตรึง 🔐 + core ที่เบรกไว้) — ดู section
# "REPLAN: แปะแถวคงแผนเดิม" ท้ายไฟล์ ; ว่าง = ไม่มี (รันโหมด replan เดี่ยว ๆ เพื่อทดสอบ)
REPLAN_APPEND_FILE = os.environ.get("KP_REPLAN_APPEND_FILE") or ""
# งาน booking ที่ต้นทางไม่มีทั้ง FG และ RDD (ชีท NO_FG ของ booking_demand.xlsx) — ไม่วางแผน แต่ต้องขึ้นชีท
# UNPLANNED พร้อมเหตุผล ; ห้ามเติมกำหนดส่งเอง (user 22/9/2569: ไม่มี FG = ไม่ทำแผน)
REPLAN_NO_FG_FILE = os.environ.get("KP_REPLAN_NO_FG_FILE") or ""
if REPLAN_MODE:
    ORDER_FILE = Path(os.environ.get("KP_REPLAN_ORDER_FILE") or (DATA_PLAN_DIR / "order_replan.xlsx"))
    MC_REMAIN_FILE = Path(os.environ.get("KP_REPLAN_MC_FILE") or (DATA_PLAN_DIR / "booking_final_replan.xlsx"))
    OUTPUT_FILE = REPLAN_RESULT_FILE
    print("🔁 REPLAN MODE — วางแผนใหม่รวม booking + SC")
    print(f"   ดีมานด์   : {ORDER_FILE.name}")
    print(f"   เครื่องว่าง : {MC_REMAIN_FILE.name}")
    print(f"   ผลลัพธ์   : {OUTPUT_FILE.name}")

# วันตั้งเครื่อง — ทั้ง 3 ค่าแก้ได้ที่หน้า "ตั้งค่าระบบ" (AVA_MC ใช้ค่าชุดเดียวกัน ห้ามให้ต่างกัน
# ไม่งั้นเครื่องที่ AVA หักไว้กับที่แผนใช้จริงจะไม่ตรงกัน)
SETUP_DAYS = plan_config.get("KP_SETUP_DAYS")
SETUP_DAYS_HEAVY = plan_config.get("KP_SETUP_DAYS_HEAVY")   # POLY ล้วน / ด้าย DTY
# โรงอ้อมน้อย (OMNOI): ทุก item ใช้ setup เท่ากันหมด ไม่ดู prefix ITEM / POLY / DTY
OMNOI_SETUP_DAYS = plan_config.get("KP_OMNOI_SETUP_DAYS")
SETUP_GAP_WEEK = plan_config.get("KP_SETUP_GAP_WEEK")
# สัปดาห์ต้นที่ถือว่าหน้างานเดินไปแล้ว — ห้ามวางงานใหม่ลงไป (Replan ใช้ค่าเดียวกันนี้)
FREEZE_WEEKS = plan_config.get("KP_FREEZE_WEEKS")
# กติกาวันด้ายเข้า (PO IN ของ booking) — ตั้งที่หน้า Monitor Yarn ; ด้ายทุกประเภทใช้กติกาเดียวกัน
#   POIN_ON        = ห้ามเริ่มทอก่อนด้ายเข้า (ปิด = ไม่ดูวันด้ายเข้าเลย)
#   POIN_LAG_WEEKS = เริ่มทอได้หลังสัปดาห์ที่ด้ายเข้ากี่สัปดาห์ (1 = สัปดาห์ถัดไป ; 0 = สัปดาห์เดียวกัน)
# ตัวเตือน ⚠ "วางก่อนด้ายเข้า" ใน KnitPlan.jsx อ่านค่าชุดเดียวกัน — แก้กติกาที่นี่ต้องแก้ที่นั่นด้วย
POIN_ON = bool(plan_config.get("KP_YARN_POIN_ON"))
POIN_LAG_WEEKS = int(plan_config.get("KP_YARN_POIN_LAG_WEEKS"))

# โควตาช่างตั้งเครื่องต่อสัปดาห์ต่อโรง/ชนิดเครื่อง — ตั้งที่หน้า "ตั้งค่าระบบ" › Job Setup/Week
# ทุกจุดในไฟล์นี้ต้องอ่านจาก JOB_CAP ตัวนี้ ห้ามเขียนเลขซ้ำ ไม่งั้นแก้บนเว็บแล้วบางด่านยังใช้ค่าเก่า
# (ฝั่งเว็บ/advisor อ่านค่าชุดเดียวกันผ่าน plan_config.job_cap())
JOB_CAP = plan_config.job_cap()
JOB_CAP_UNLIMITED = 9999      # โรงที่ไม่มีโควตา (จ้างทอ/ไม่รู้จัก) — ผ่านเสมอ
# สวิตช์ "คุมจำนวนงาน Setup ต่อสัปดาห์ตามกำลังช่าง" — ปิดแล้วต้องไม่คุมทุกด่าน
# (เดิมอ่านเฉพาะรอบซ่อมแผนท้ายรอบ ส่วนด่านตอนวางบังคับตลอด = ปิดแล้วก็ยังคุม ไม่ตรงกับหน้าเว็บ)
# ทุกด่านที่จะจำกัดเครื่องด้วยโควตา job ต้องเช็กตัวนี้ก่อน: check_job_capacity_limit ·
# get_remaining_job_slots · _preempt_job_slots (รอบซ่อม gap) · _repair_job_overflow (ท้ายรอบ)
JOB_ENFORCE = bool(plan_config.get("KP_JOB_ENFORCE"))
# สัปดาห์ที่ไม่วางงาน = ปิดวันในปฏิทิน (วันทำงาน 0) หรือยุบเข้าสัปดาห์อื่น (ชีท Week Merge) เท่านั้น
# (เลิกค่า KP_SKIP_WEEKS บนหน้าตั้งค่าแล้ว 30 ก.ย. 2569 — ซ้ำกับปฏิทิน และไม่เคยทำงานหลังเปลี่ยนคีย์เป็น YW)
# MC_GROUP redirect: เมื่อเลือก MC_GROUP+GAUGE นี้ให้เปลี่ยนไปใช้ MC_GROUP+GAUGE ใหม่แทนเสมอ
# key = (mc_group, gauge_str), value = (new_mc_group, new_gauge_str)
# ตั้งที่หน้า "ตั้งค่าระบบ" › เครื่องที่ให้ใช้แทนกัน (เช่น SKP G20 → FA G20 อ้อมน้อยรับผลิตแทน)
def _cfg_gauge(v) -> str:
    """เกจจากหน้าตั้งค่า → รูปแบบเดียวกับ _normalize_gauge (ซึ่งนิยามทีหลังในไฟล์นี้)
    ต้องให้ผลตรงกัน ไม่งั้นคีย์ที่ผู้ใช้กรอกจะไม่ match กับเกจที่ engine ส่งมาตอนเทียบ"""
    s = str(v if v is not None else "").strip().upper().replace("G", "").replace("GAUGE", "")
    return s[:-2] if s.endswith(".0") else s


MC_GROUP_REDIRECT = {
    (str(_r.get("from_mc", "")).strip().upper(), _cfg_gauge(_r.get("from_gauge"))):
        (str(_r.get("to_mc", "")).strip().upper(), _cfg_gauge(_r.get("to_gauge")))
    for _r in (plan_config.get("KP_MC_GROUP_REDIRECT") or [])
    if str(_r.get("from_mc", "")).strip() and str(_r.get("to_mc", "")).strip()
}

# Progressive machine reduction: เริ่มต้นด้วยเครื่องเยอะ แล้วค่อยๆ ลดลงให้ทัน target

USE_PROGRESSIVE_REDUCTION = plan_config.get("KP_PROGRESSIVE_REDUCTION")
# MAX_SETUP_MC แบบ static ถูกยกเลิก → ใช้ _dynamic_setup_limit() แทน (dynamic ตาม urgency RDD)
# จำนวนเครื่อง *ใหม่* (new setup) สูงสุดต่อ item/week — carry-over ไม่นับ
MAX_NEW_SETUP_MC = plan_config.get("KP_MAX_NEW_SETUP_MC")

# เพดานข้างบนเป็นของ "ผ้า 1 ตัวต่อสัปดาห์" ไม่ใช่ของแต่ละใบสั่ง (user ยืนยัน 30 ก.ย. 2569)
# เดิมบังคับรายใบ → ผ้าตัวเดียวมี 3 ใบลงสัปดาห์เดียวกันตั้งเครื่องใหม่ได้ 2+2+2 = 6 ตัว
# ทั้งที่ช่างตั้งได้จริงแค่ 2 (เคสจริง FD5DU34B0 RAO G19 W50: 3 ใบ ตั้งใหม่ 6 ตัว)
# ปิดกลับเป็นพฤติกรรมเดิมได้ที่หน้า "ตั้งค่าระบบ" (หรือ env KP_SETUP_QUOTA_PER_ITEM=0)
SETUP_QUOTA_PER_ITEM = plan_config.get("KP_SETUP_QUOTA_PER_ITEM")
# (idx สัปดาห์, ผ้า, กลุ่มเครื่อง, เกจ) → เครื่องใหม่ที่ตั้งไปแล้วในสัปดาห์นั้น
# คีย์ใช้ "ตำแหน่งบนปฏิทิน" ไม่ใช่เลขสัปดาห์เปล่า — แผนคร่อมปีแล้ว W1 ปีหน้าจะชนกับ W1 ปีนี้
_item_new_mc_week: dict = {}

# ทรงเครื่องของก้อนงานต้องเป็น "ขึ้นให้ครบตอนต้น แล้วไล่ลง" — ห้ามเพิ่มอีกหลังเริ่มลด
# (user 2 ก.ย. 2569: เพิ่มเครื่องตอนท้าย = จ่ายค่า setup ซ้ำเพื่อไล่งานที่ควรเร่งตั้งแต่ต้น)
# บังคับ "ตอนวาง" ไม่ใช่ตามซ่อมทีหลัง ; ปิดได้ที่หน้า "ตั้งค่าระบบ" (หรือ env KP_MC_SHAPE_MONOTONE=0)
MC_SHAPE_MONOTONE = plan_config.get("KP_MC_SHAPE_MONOTONE")

# (item, กลุ่มเครื่อง, เกจ, SC, FG) → (เครื่องสูงสุดที่เคยใช้, เริ่มไล่ลงแล้วหรือยัง)
_mc_shape_state: dict = {}

# ---------- หางงาน: ลดเครื่องเหลือ "ครึ่งหนึ่ง" แทนการตัดฮวบ (user 1 ก.ย. 2569) ----------
# เดิมสัปดาห์สุดท้ายตัดตามยอดที่เหลือ → ได้ทรง 10→0 / 6→2 (หักหัว)
# ใหม่: เวลาจะลดเครื่อง ให้ลงที่ครึ่งหนึ่งของสัปดาห์ก่อน → 10→5, 12→6, 6→3
# ใช้เฉพาะงานที่เครื่องตั้งแต่ MC_TAPER_MIN_MC ตัวขึ้นไป — งานเล็กคงพฤติกรรมเดิม (user ยืนยัน)
# ปิดกลับเป็นพฤติกรรมเดิม: ตั้งค่านี้เป็น 999 (หน้า "ตั้งค่าระบบ")
MC_TAPER_MIN_MC = max(2, int(plan_config.get("KP_TAPER_MIN_MC")))
# ไล่ลงได้กี่ขั้นต่อ 1 ก้อนงาน (8 → 4 → 2 = 2 ขั้น) แล้วจบงานได้เลย
# กันหางยาวเกินไปจนงานจบช้ามาก (แต่ละขั้นกินเวลา 1 สัปดาห์)
# user เคาะ 2 ก.ย. 2569: ขาลง "ลด 50% สองครั้ง" → ค่าเริ่มต้น 2 ขั้น (เดิม 3)
MC_TAPER_MAX_STEPS = max(1, int(plan_config.get("KP_TAPER_STEPS")))

# สัปดาห์สุดท้ายของก้อนงาน "จบที่กี่เครื่อง" ได้ — user เคาะ 2 ก.ย. 2569:
# "2-4-6-8-8-4-2 แล้วจบก็โอเค ไม่จำเป็นต้องลดเหลือ 1" → หางจบที่ 2 เครื่องยอมรับได้
# (ยิ่งบังคับให้ลดถึง 1 ยิ่งยืดงานออกไปอีกสัปดาห์โดยไม่ได้อะไร)
MC_TAIL_MC = max(1, int(plan_config.get("KP_TAIL_MC")))

# ---------- item ที่ "ให้สีแล้ว" ----------
# เซ็ตรหัส item ที่มีงานสีอยู่บ้าง (สร้างโดย _load_color_items() ตอนเรียงคิวออร์เดอร์)
# ใช้ 2 ที่: จัดคิววางแผน (สีมาก่อน) และรอบซ่อม color preempt (ยึดเครื่องคืนให้งานสี)
# ⚠️ เพดานเครื่องใหม่ MAX_NEW_SETUP_MC = 2 ใช้กับงานสีเหมือนกันทุกประการ (user ยืนยัน 18 ส.ค. 2569)
_COLOR_ITEMS: set = set()

# ---------- "งานสี" คิดเป็นยอด ไม่ใช่ทั้ง item (user 18 ส.ค. 2569) ----------
# ยอดทอ 100 เปิดสีมา 50 → 50 กก. แรกเท่านั้นที่เป็นงานสี (ได้สิทธิ์ก่อน/ห้ามถูกถอด)
# อีก 50 กก. ถือเป็นงานที่ยังไม่ให้สี → ถูกถอดเครื่องให้ item สีตัวอื่นได้
#
# ⚠️ คีย์เป็น (SC, ITEM) ไม่ใช่ ITEM เดี่ยว (user ยืนยัน 27 ส.ค. 2569) — ยอดเปิดสีเป็นของ
# "ใบสั่ง" ไม่ใช่ของ item: SC หนึ่งเปิดสีครบ อีก SC ยังไม่เปิดเลย ถ้ารวมเป็นก้อนเดียวจะกลาย
# เป็นว่าทั้งสองใบเป็นงานสีบางส่วนพอ ๆ กัน ทั้งที่ใบแรกควรได้สิทธิ์เต็ม ใบหลังควรถอดได้หมด
# {(SC เลขล้วน, ITEM_CODE upper): กก.ที่เปิดสีแล้ว} — มาจาก ColorOpen (view ยอดเปิดสีของใบ SC)
_COLOR_QTY: dict = {}

# {ITEM_CODE: กก.} จาก env KP_COLOR_QTY เท่านั้น — override มือระดับ item (ไม่รู้ SC)
_COLOR_QTY_ITEM: dict = {}

# คอลัมน์ธง "ก้อนนี้อยู่ในโควตาสีคงเหลือไหม" ที่ _split_orders_by_color_quota แจกให้แถวออร์เดอร์
# (True = ก้อนสี, False = ส่วนเกิน, None = ไม่เกี่ยวกับสี) แล้วเดินทางต่อไปเป็นคอลัมน์
# COLOR_PART ของแถวแผน — รอบซ่อมท้ายแผนต้องเชื่อธงนี้ ไม่ใช่แจกโควตาใหม่เอง
_COLOR_PART_COL = "_COLOR_PART"


def _color_part_text(order) -> str:
    """ธงก้อนสีของออร์เดอร์ → ข้อความที่ลงคอลัมน์ COLOR_PART ของแถวแผน

    ทุกที่ที่สร้างแถวแผนต้องเรียกตัวนี้ (แถวหลัก / แถวเติมเครื่อง / แถว capacity
    optimization) — แถวที่ลืมตั้งธงจะออกมาเป็นว่าง แล้วถูกอ่านเป็น "ไม่ใช่งานสี"
    บนหน้าเว็บ และถอยไปใช้เกณฑ์ระดับ item ใน _main_row_is_color ซึ่งข้ามกติกา
    "คิดต่อใบสั่ง" (ก้อนเดียวกันจึงขึ้นคนละสีคนละสัปดาห์)

    ⚠️ order เป็น Series ได้ → ค่าที่ไม่ได้ติดธงกลับมาเป็น NaN ไม่ใช่ None
    (NaN เป็น truthy จะกลายเป็น "Yes" ผิดใบ) จึงต้องกรองด้วย pd.isna ก่อน
    """
    _f = order.get(_COLOR_PART_COL)
    if _f is None or (not isinstance(_f, bool) and pd.isna(_f)):
        return ""
    return "Yes" if _f else "No"


def _row_sc(row) -> str:
    """ช่อง SC ดิบของแถวออร์เดอร์/แถวแผน — รองรับทุกชื่อคอลัมน์ที่ใช้ในระบบ

    ⚠️ คืนค่าดิบ (อาจยุบหลายใบด้วยคอมมา) ให้ _color_quota_of แตกเอง —
    ห้าม normalize เป็นใบเดียวตรงนี้ ไม่งั้นแถว booking ที่ยุบหลาย SC จะหาโควตาไม่เจอ"""
    for _k in ("SC_SO_NO", "SO_NO", "SC/SO NO", "SC_NO"):
        _v = row.get(_k) if hasattr(row, "get") else None
        if ColorOpen.split_sc(_v):
            return str(_v).strip()
    return ""


def _is_color_item(item_code) -> bool:
    """item นี้มีงานสีอยู่บ้างไหม (ยังไม่บอกว่ากี่กิโล — ดูยอดที่ _color_quota_of)"""
    return str(item_code or "").strip().upper() in _COLOR_ITEMS


def _color_quota_of(item_code, sc=None) -> float:
    """โควตาสีเป็นกิโล — 0 = ไม่ใช่งานสี, inf = เป็นงานสีแต่ยังไม่รู้ยอด (override มือ)

    ส่ง sc มาด้วย = ดูโควตาของใบสั่งนั้นโดยตรง (แม่นสุด) — รองรับช่องที่ยุบหลายใบ
    ด้วยคอมมา โดยบวกโควตาของทุกใบที่แถวนั้นครอบคลุม
    ไม่ส่ง sc = ยอดรวมทุกใบสั่งของ item (ใช้ตอนมีแต่รหัส item ให้ตัดสิน)
    """
    _u = str(item_code or "").strip().upper()
    if not _u or _u not in _COLOR_ITEMS:
        return 0.0
    _scs = ColorOpen.split_sc(sc)
    if _scs:
        _tot = 0.0
        _hit = False
        for _s in _scs:
            _q = _COLOR_QTY.get((_s, _u))
            if _q is not None:
                _tot += float(_q)
                _hit = True
        if _hit:
            return _tot
        # ไม่มีใบไหนอยู่ใน view = ยังไม่เปิดสี (view คือแหล่งเดียว — ไม่มี = ไม่มีสี)
        # ยกเว้น item ที่ถูก override มือด้วย env → ใช้ยอดของ env
        if _u in _COLOR_QTY_ITEM:
            return float(_COLOR_QTY_ITEM[_u])
        return 0.0 if _COLOR_QTY else float("inf")
    if _u in _COLOR_QTY_ITEM:
        return float(_COLOR_QTY_ITEM[_u])
    _tot = sum(_v for (_s, _i), _v in _COLOR_QTY.items() if _i == _u)
    return float(_tot) if _tot > 0 else float("inf")


# ---------- เว้นสัปดาห์รอสีเปิดรอบใหม่ (Color Gap — user 8 ก.ย. 2569) ----------
# เคยทอด้วยสีที่เปิดมาถึง W36 → W37, W38 ทอส่วนที่ยังไม่มีสีต่อได้ → W39 "หยุดรอสีรอบใหม่"
# โดยยังกันเครื่องไว้ (item อื่นแย่งไม่ได้ + กลับมาทอ W40 ไม่ต้อง setup ใหม่) → W40 เป็นต้นไป
# ทอต่อเนื่องจนหมด ไม่เว้นอีก
#
# กติกาที่ user เคาะ:
#   • ขอบเขต = (item, กลุ่มเครื่อง, เกจ) และเฉพาะ item ที่มีโควตาสีใน view เท่านั้น
#     (item ที่ไม่เคยเปิดสีเลย ไม่โดนกติกานี้ — ทอต่อได้ตามปกติ)
#   • เว้นก่อนเสมอแม้ทำให้ชน TARGET/RDD → ทรงเครื่องจะเร่งชดเชยในสัปดาห์ที่เหลือเอง
#   • สัปดาห์ที่เว้นได้แถวแผนชนิด HOLD (PRODUCE_QTY = 0, ACTUAL_MC = เครื่องที่กันไว้)
COLOR_GAP_ENABLED = plan_config.get("KP_COLOR_GAP")
COLOR_GAP_AFTER = max(0, int(plan_config.get("KP_COLOR_GAP_AFTER")))

# ค่าคงที่ของแถวกันเครื่อง — ใช้ร่วมกันทุกจุดที่ต้องแยกแถวนี้ออกจากแถวผลิตจริง
COLOR_GAP_SOURCE = "HOLD_COLOR"        # PLAN_SOURCE ของแถวกันเครื่อง
COLOR_GAP_REMARK = "Hold รอสี"          # ข้อความบนบล็อก Gantt / ชีทแผน

# ---------- กันเครื่องระหว่างรอรอบผลิตถัดไปของผ้า core (Hold รอ target — user 16 ก.ย. 2569) ----------
# core ผลิตเป็นรอบตามสต๊อก (ดู CORE ITEM / _sim_core_batches) ระหว่าง 2 รอบจึงมีสัปดาห์ที่เว้นว่าง
# เดิมเครื่องหลุดคืนพูลตอนเว้น → รอบถัดไปต้องหาเครื่องใหม่ + setup ใหม่ (เหตุผลเดิมที่เคยไป "ล็อก"
# แผน core ทั้งก้อนไว้ ซึ่งทำให้ core ไม่ถูกคิดใหม่ตาม target เลย — เลิกแล้ว ดู program_lock.py)
# → ใช้กลไกเดียวกับ Hold รอสีทุกประการ ต่างแค่ป้าย: กันเครื่องชุดเดิมไว้ทุกสัปดาห์ที่เว้น
CORE_HOLD_ENABLED = plan_config.get("KP_CORE_HOLD_TARGET")
CORE_HOLD_SOURCE = "HOLD_TARGET"
CORE_HOLD_REMARK = "Hold รอ target"

# แถว "กันเครื่อง" ทุกชนิด (ไม่ได้ผลิต ไม่ได้ตั้งเครื่อง) — ด่านซ่อม/carry/Gantt ต้องมองเหมือนกัน
HOLD_SOURCES = {COLOR_GAP_SOURCE, CORE_HOLD_SOURCE}

# {(ITEM, MC_GROUP, GAUGE): idx สัปดาห์สุดท้ายที่ "ก้อนสี" ของคีย์นี้ถูกทอ}
# มาจาก 2 ทาง: booking เดิมของ item ที่เปิดสีแล้ว (seed ตอนเริ่มลูป) + แถวแผนใหม่ COLOR_PART=Yes
_color_end_idx: dict = {}
# {(คีย์, idx สัปดาห์): จำนวนเครื่องที่กันไว้} — กันสร้างแถว HOLD ซ้ำเมื่อหลายใบสั่งของคีย์
# เดียวกันเดินผ่านสัปดาห์เว้น
_color_gap_held: dict = {}
# {(คีย์, idx สัปดาห์): แถว HOLD ที่สร้างไว้} — ใบสั่งใบถัดมาที่เดินผ่านสัปดาห์เว้นเดียวกันจะ
# ไม่สร้างแถวใหม่ (memo ด้านบน) แต่ต้องต่อเลข SC ของตัวเองเข้าไปในแถวเดิม
_color_gap_rows: dict = {}
# {(week, item, mc_group) — คีย์เดียวกับ remaining_week_cap: เครื่องที่ Hold รอ target กันไว้}
# Hold ของ core เปิดกำลังว่างเป็น cap เหลือให้ใบที่เลยกำหนดส่งเติมได้ (ดู _color_gap_hold_mc)
# ตัวนี้บอกจุดหักพูลว่า "เครื่องชุดไหนถูกหักไปแล้วตอนสร้าง Hold" จะได้หักเพิ่มเฉพาะเครื่องใหม่
_HOLD_CAP_MC: dict = {}


def _pool_charge_mc(available_mc, booking_mc, new_mc, hold_mc=0) -> int:
    """เครื่องที่แถวแผนหักจากพูล = เครื่องทั้งแถว − เครื่อง booking (ถูกหักจาก TOTAL_MC_REMAIN แล้ว)

    สัปดาห์ที่วางทับแถว Hold รอ target (hold_mc > 0): เครื่องอุ่นชุดที่กันไว้ถูกหักพูลตอนสร้าง Hold แล้ว
    → หักเฉพาะส่วนที่เกินชุดนั้น ; เครื่องตั้งใหม่เป็นคนละชุดเสมอ จึงหักอย่างน้อยเท่า new_mc
    (เดิมสัปดาห์ที่มี cap เหลือจากแผนถือว่า "นับพูลแล้ว" ทั้งแถว → เครื่องที่ตั้งใหม่ทับ Hold ไม่ถูกหัก
     เจอจริงตอนเปิด cap ของ Hold 30 ก.ย. 2569: พูล RAO G19 W2701 ล้น 2 ตัวจนด่านซ่อมต้องตัด)
    """
    _base = max(0, int(available_mc or 0) - int(booking_mc or 0))
    if hold_mc and int(hold_mc) > 0:
        return max(0, int(new_mc or 0), _base - int(hold_mc))
    return _base


def _color_gap_add_sc(row, sc_so_no, line_id="") -> None:
    """ต่อเลขใบสั่ง (+ เลขบรรทัดใบ) เข้าไปในแถว HOLD — รูปแบบ "720336, 720999" เหมือนชีท SETUP_TRACKING

    แถวกันเครื่องผูกกับคีย์ (item, เครื่อง, เกจ) ไม่ได้ผูกกับใบสั่ง จึงเคยปล่อย SC_SO_NO ว่าง
    แล้วคนอ่านแผนเห็นสัปดาห์ที่ไม่มีเลขใบโผล่กลางก้อนงานของตัวเอง

    SC_LINE_ID เดินคู่กับ SC_SO_NO ด้วยเหตุผลเดียวกัน (user 21/9/2569) — ยกจากใบสั่งที่ทำให้
    ต้องเว้นสัปดาห์ ไม่ได้สร้างเลขขึ้นเอง ; แถวเดียวมีได้หลายใบ → คั่นคอมมาเหมือนกัน
    """
    if row is None:
        return
    for _col, _val in (("SC_SO_NO", sc_so_no), ("SC_LINE_ID", line_id)):
        _new = ColorOpen.so_text(_val).strip()
        if not _new or _new.upper() in ("NAN", "NONE"):
            continue
        _cur = str(row.get(_col, "") or "").strip()
        _parts = [_p.strip() for _p in _cur.split(",") if _p.strip()]
        # ค่าที่ยกมาอาจยุบหลายบรรทัดมาแล้ว ("774607,774621") → แตกก่อนเทียบ ไม่งั้นซ้ำสะสม
        for _t in (_p.strip() for _p in _new.split(",")):
            if _t and _t not in _parts:
                _parts.append(_t)
        row[_col] = ", ".join(_parts)


def _hold_order_fields(order, order_type="", fg_week=None, fg_week_int=None,
                       rdd_idx=None, sub_color="") -> dict:
    """ข้อมูลใบสั่งที่ยกไปใส่แถว HOLD — คอลัมน์เดียวกับแถวผลิตปกติ (user 21/9/2569: "ห้ามขาด")

    แถวกันเครื่องเคยมีแต่ item/เครื่อง/สัปดาห์ คอลัมน์ฝั่งใบสั่งว่างหมด 124 แถว
    → ยกจากใบที่ทำให้ต้องเว้นสัปดาห์นี้ ค่าทุกตัวมาจาก order ตรง ๆ ไม่ได้คำนวณขึ้นใหม่

    ไม่รวมยอด/เครื่อง (ORDERS_QTY, PENDING_PLAN, PRODUCE_QTY...) — สัปดาห์นั้นไม่ได้ทอจริง
    ต้องเป็น 0 ต่อไป ไม่งั้นยอดผลิตรวมของแผนจะบวกเกิน
    """
    # ⚠ order เป็น pandas Series (ไม่ใช่ dict) — `if not order` โยน "truth value is ambiguous"
    if order is None:
        return {}

    def _t(v) -> str:
        """ข้อความสะอาด — NaN ของ pandas ต้องกลายเป็นค่าว่าง ไม่ใช่ตัวหนังสือ "nan" ในชีทแผน"""
        s = "" if v is None else str(v).strip()
        return "" if s.lower() in ("nan", "none", "nat", "<na>") else s

    _item = _t(order.get("Item Code", "")).upper()
    _yd = str(order_type or "").strip().upper() == "YD-ORDERS"
    _po_date, _po_week = po_in_fields(order)
    return {
        "SC_LINE_ID": ColorOpen.so_text(order.get("SC_LINE_ID")),
        "KP_NO": ColorOpen.so_text(order.get("KP_NO")),
        "ORDER_TYPE": _t(order_type),
        "ORDER_DATE": order.get("Date"),
        "CUSTOMER": _t(order.get("Customer", "")),
        "PO_NO": _t(order.get("PO_NO", "")),
        "YARNPO": _t(order.get("YARNPO", "")),
        "MATERIAL_CONTENT": _t(order.get("MATERIAL_CONTENT", "")),
        # กำหนดส่งและเป้าทอของใบ — TARGET_IDX ทำให้ TARGET_YEAR ถูกเติมตอน export เหมือนแถวปกติ
        "FG_WEEK": fg_week,
        "RDD_WEEK": _rdd_week_of(order),
        "TARGET_KNIT": fg_week_int,
        "TARGET_IDX": rdd_idx,
        "SUB_COLOR": _t(sub_color),
        "NAY_COLOR": _t(order.get("NAY_COLOR", "")),
        "COLOR_DESC": _t(order.get("COLOR_DESC", "")),
        "DATE_IN": order.get("DATE_IN"),
        "LT_YARN": order.get("DYE_END_DATE") if _yd else get_yarn_lt_days(_item),
        "EARLIEST_PLAN_WEEK": (order.get("DYE_END_DATE") if _yd
                               else get_yarn_lt_earliest_week(_item, date_in=order.get("DATE_IN"))),
        "YARN_USED": _yarn_used_lookup.get(_item, ""),
        "PO_IN_DATE": _po_date,
        "PO_IN_WEEK": _po_week,
        "YARN_STOCK": yarn_stock_note(order),
    }


def _color_gap_key(item_code, mc_group, gauge) -> tuple:
    """คีย์ขอบเขตของกติกา = (item, MC ตัวแทนของก้อน, เกจ) — รูปเดียวกับ carry key (_ck)

    ต้องใช้ MC ตัวแทน ไม่ใช่ชื่อกลุ่มดิบ: seed จาก booking มาเป็นคีย์ _ck (ตัวแทน), แถวแผน
    ใช้กลุ่มที่แผนเลือก, ใบสั่งถือกลุ่มดิบ — ในพูลหลายกลุ่ม (เช่น IBLTA/RAP/RAP98) สามชื่อนี้
    ต่างกันได้ ถ้าไม่ยุบเป็นตัวเดียวกัน กติกาจะหาคีย์ไม่เจอแล้วไม่ทำงานแบบเงียบ ๆ
    """
    _g = _normalize_gauge(gauge)
    return (
        str(item_code or "").strip().upper(),
        _canon_mc(str(mc_group or "").strip().upper(), _g),
        _g,
    )


def _note_color_end(item_code, mc_group, gauge, week_idx) -> None:
    """จำสัปดาห์ล่าสุดที่ "ก้อนสี" ของคีย์นี้ถูกทอ — จุดตั้งต้นของการนับสัปดาห์เว้น"""
    if not COLOR_GAP_ENABLED or week_idx is None:
        return
    _k = _color_gap_key(item_code, mc_group, gauge)
    if not _k[0] or not _k[1]:
        return
    if int(week_idx) > _color_end_idx.get(_k, -1):
        _color_end_idx[_k] = int(week_idx)


def _color_gap_idx(item_code, mc_group, gauge):
    """index สัปดาห์ที่ต้องเว้นรอสีของคีย์นี้ (None = ไม่เข้าเงื่อนไข)

    สีหมด W36 + ทอต่อได้ 2 สัปดาห์ (COLOR_GAP_AFTER) → เว้น W39 = end + 2 + 1
    """
    if not COLOR_GAP_ENABLED or not _is_color_item(item_code):
        return None
    _end = _color_end_idx.get(_color_gap_key(item_code, mc_group, gauge))
    return None if _end is None else int(_end) + COLOR_GAP_AFTER + 1


def _is_hold_row(row) -> bool:
    """แถวนี้เป็นแถว "กันเครื่อง" ไหม — Hold รอสี หรือ Hold รอ target ของ core
    (ไม่ได้ผลิต ไม่ได้ตั้งเครื่อง — ห้ามด่านซ่อมแปลงเป็น setup)"""
    return str(row.get("PLAN_SOURCE", "")).strip().upper() in HOLD_SOURCES


def _rank_target_knit(row):
    """TARGET_KNIT ที่ใช้ "จัดลำดับเหยื่อ" ตอนยึด/คืนเครื่อง — แถว HOLD คืน NaN เสมอ

    ตั้งแต่ 21/9/2569 แถว HOLD ยกข้อมูลใบมาครบ (รวม TARGET_KNIT) เพื่อให้ชีทแผนไม่มีช่องว่าง
    แต่ต้องไม่ให้ข้อมูลที่เพิ่มมาเปลี่ยนการตัดสินใจวางแผน: ด่านยึด/คืนเครื่องเรียงเหยื่อด้วย
    target ที่ไกลสุด — แถว HOLD เคยไม่มี target จึงถูกจัดเป็น "ไกลสุด" มาตลอด
    (วัดจริง: ถ้าปล่อยให้ใช้ target จริง งาน 27 รหัสสลับสัปดาห์ แถว HOLD หาย 4 แถว)
    → ด่านจัดลำดับใช้ตัวนี้แทนการอ่านคอลัมน์ตรง ๆ ; ชีทยังโชว์ค่าจริงเหมือนเดิม
    """
    if _is_hold_row(row):
        return float("nan")
    return pd.to_numeric(row.get("TARGET_KNIT"), errors="coerce")


def _hold_remark_of(row) -> str:
    """ข้อความบนแถวกันเครื่องตามชนิดของมัน (ด่านซ่อมเขียน REMARK ทับได้ ต้องเขียนของถูกชนิด)"""
    _src = str(row.get("PLAN_SOURCE", "")).strip().upper()
    return CORE_HOLD_REMARK if _src == CORE_HOLD_SOURCE else COLOR_GAP_REMARK


def _color_gap_mc_by_week(plan_rows) -> dict:
    """เครื่องที่แผนเดินจริงต่อ (คีย์ก้อน, idx สัปดาห์) — เกณฑ์เดียวกับตอนสร้าง Hold (_color_gap_hold_mc)

    บวกทุกแถวของคีย์เดียวกัน ; ไม่นับแถว Hold เอง และงานจ้างทอ (เครื่องอยู่ที่ผู้รับจ้าง)
    booking ไม่รวมในนี้ — ผู้เรียกบวก booking_mc_by_week เองตอนอ่าน
    """
    _used: dict = {}
    for _r in plan_rows or []:
        if _is_hold_row(_r) or _is_outsource_row(_r):
            continue
        _n = int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0)
        _widx = _preempt_row_idx(_r)
        if _n <= 0 or _widx is None:
            continue
        _k = (_color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE")), _widx)
        _used[_k] = _used.get(_k, 0) + _n
    return _used


def _hold_mc_by_week(plan_rows) -> dict:
    """เครื่องที่ "แถวกันเครื่อง" ถืออยู่ต่อ (คีย์ก้อน, idx สัปดาห์) — คู่กับ _color_gap_mc_by_week

    ใช้ตอนที่ต้องรู้ว่าสัปดาห์ข้าง ๆ มีเครื่องอุ่นจอดอยู่ไหม ทั้งที่ไม่มีงานเดิน — จำเป็นกับ
    Hold รอ target ของ core ที่กันติดกันหลายสัปดาห์ (ถ้ามองแค่งานจริง สัปดาห์กลางโซ่จะเห็น 0
    แล้วด่านซ่อมจะไล่ตัดเครื่องที่กันไว้ทิ้งทั้งโซ่)
    """
    _held: dict = {}
    for _r in plan_rows or []:
        if not _is_hold_row(_r):
            continue
        _n = int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0)
        _widx = _preempt_row_idx(_r)
        if _n <= 0 or _widx is None:
            continue
        _k = (_color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE")), _widx)
        _held[_k] = _held.get(_k, 0) + _n
    return _held


def _color_gap_hold_mc(item_code, mc_group, gauge, gap_idx, sc_so_no="", fields=None,
                       source=COLOR_GAP_SOURCE, remark=COLOR_GAP_REMARK, tag="COLOR GAP") -> int:
    """กันเครื่องของคีย์นี้ไว้ในสัปดาห์ที่เว้น แล้วใส่แถว HOLD ลงแผน

    ใช้ร่วมกัน 2 กติกา (ต่างแค่ป้ายบนแถว): เว้นรอสีเปิดรอบใหม่ (HOLD_COLOR) และเว้นรอรอบผลิต
    ถัดไปของผ้า core (HOLD_TARGET — source/remark/tag ถูกส่งเข้ามา)

    เครื่องที่กัน = เท่าที่คีย์นี้เดินอยู่ในสัปดาห์ก่อนหน้า (แผนใหม่ + booking) และไม่เกิน
    เครื่องว่างจริงของพูลสัปดาห์นั้น (กันเกิน = พูลติดลบ แล้วด่านซ่อมจะไล่ตัดงานคนอื่นตามมา)
    ⚠️ แถว Hold ของสัปดาห์ก่อนนับเป็น "เครื่องที่เดินอยู่" ด้วย (ลูปด้านล่างไม่ได้กรองแถว Hold ออก)
       — โซ่ Hold หลายสัปดาห์ติดกันของ core จึงต่อกันได้

    แถวที่ได้มี PRODUCE_QTY = 0 แต่ ACTUAL_MC > 0 จึง:
      • ถูกหักออกจากพูลผ่าน _repair_charge_pool → item อื่นใช้เครื่องชุดนี้ไม่ได้
      • ทำให้ _item_producing_at เห็นว่าคีย์นี้ยังถือเครื่องอยู่ → _carry_cap_by_gap ไม่ตัด carry
        สัปดาห์ถัดไปกลับมาทอต่อโดยไม่ต้อง setup ใหม่
      • รอด _row_is_noop (ด่านลบแถวที่ไม่มีทั้งเครื่องและยอด) เพราะยังถือเครื่องจริง

    fields = ข้อมูลใบสั่งที่ยกมาใส่แถว (ดู _hold_order_fields) — ใบแรกที่เดินผ่านสัปดาห์นี้
    เป็นเจ้าของข้อมูล ใบถัดมาต่อแค่เลขใบ/เลขบรรทัดเข้าไป (ข้อมูลใบอื่นจะขัดกันเอง)

    คืนจำนวนเครื่องที่กันได้ (0 = ไม่มีเครื่องให้กัน → เว้นสัปดาห์อย่างเดียว ไม่สร้างแถว)
    """
    fields = fields or {}
    line_id = fields.get("SC_LINE_ID", "")
    _k = _color_gap_key(item_code, mc_group, gauge)
    _memo = _color_gap_held.get((_k, gap_idx))
    if _memo is not None:
        # คีย์นี้กันไปแล้ว (อีกใบสั่งเดินผ่านสัปดาห์เดียวกัน) — ไม่สร้างแถวซ้ำ แต่ต่อเลขใบเข้าไป
        _color_gap_add_sc(_color_gap_rows.get((_k, gap_idx)), sc_so_no, line_id)
        return _memo
    if gap_idx is None or gap_idx <= 0 or gap_idx >= len(calendar_week):
        return 0
    _prev_idx = gap_idx - 1
    _week = int(calendar_week.iloc[gap_idx]["WEEK"])

    # เครื่องที่คีย์นี้เดินอยู่สัปดาห์ก่อนหน้า = เพดานของที่จะกันไว้
    # เทียบด้วยคีย์ก้อน (ไม่ใช่ชื่อกลุ่มดิบ) — งานของ item เดียวกันคนละกลุ่มในก้อนเดียวกันต้องนับด้วย
    _mc_prev = 0
    for _r in _PLANS_REF:
        if (
            week_index(_r.get("PLAN_WEEK")) == _prev_idx
            and _color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE")) == _k
        ):
            _mc_prev += int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0)
    _mc_prev += int(booking_mc_by_week.get(_k, {}).get(_prev_idx, 0) or 0)
    if _mc_prev <= 0:
        _color_gap_held[(_k, gap_idx)] = 0
        return 0

    # แถว HOLD ต้องนั่งกลุ่มเดียวกับที่แผนทอ item นี้จริง (ไม่ใช่กลุ่มดิบของใบสั่ง) — ไม่งั้น
    # บน Gantt ไปอยู่คนละเลน และลูกโซ่ carry ขาด → กลับมาทอสัปดาห์ถัดไปต้อง setup ใหม่
    # (พูลเดียวกัน → เครื่องที่หักจากพูลเท่าเดิม)
    mc_group = _item_plan_mc(item_code, mc_group, gauge)
    _room = int(get_actual_mc_remain(mc_group, _week, gauge, item_code=item_code) or 0)
    _hold = max(0, min(_mc_prev, _room))
    if _hold <= 0:
        _color_gap_held[(_k, gap_idx)] = 0
        print(f"[{tag}] {_k[0]} {_k[1]} W{_week}: พูลไม่เหลือเครื่องให้กัน "
              f"(อยากกัน {_mc_prev}) → เว้นสัปดาห์อย่างเดียว")
        return 0

    _repair_charge_pool(mc_group, gauge, _week, item_code, _hold)
    _wd_hold = get_working_days_by_factory(
        mc_group, _hold, week=_week, item_code=item_code, gauge=gauge
    )
    _PLANS_REF.append({
        "ITEM_CODE": _k[0],
        # ใบสั่งที่ทำให้ต้องเว้นสัปดาห์นี้ — ใบอื่นของคีย์เดียวกันจะถูกต่อท้ายผ่าน _color_gap_add_sc
        "SC_SO_NO": "",
        # เลขบรรทัดใบ เดินคู่กับ SC_SO_NO (เติมพร้อมกันท้ายฟังก์ชัน) — ไม่ใช่คอลัมน์ที่เว้นได้
        "SC_LINE_ID": "",
        "MC_GROUP": mc_group,
        "MC_GUAGE": gauge,
        "FACTORY_TYPE": FACTORY_TYPE_MAP.get(str(mc_group).strip().upper(), "UNKNOWN"),
        "PLAN_WEEK": _week,
        "PLAN_IDX": gap_idx,
        "PRODUCE_QTY": 0,
        "SETUP_DAYS": 0,
        "REQUIRED_MC": 0,
        "ACTUAL_MC": _hold,
        # เครื่องชุดเดิมที่ถือต่อจากสัปดาห์ก่อน — ไม่ใช่เครื่องตั้งใหม่ (NEW_MC = 0)
        "CARRYOVER_MC": _hold,
        "NEW_MC": 0,
        "MC_SHARED": 0,
        "MC_BOOKING": 0,
        "REMARK": remark,
        "LINE_REMARK": remark,
        "FACTORY_WORKING_DAYS": _wd_hold,
        "WD_BASE": _wd_hold,
        "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(_week)),
        "ACTUAL_WORKING_DAYS": 0,          # ไม่ได้ผลิต → วันทำงานจริงเป็น 0
        "DAILY_CAPACITY": 0,
        "REVOLUTION_WEIGHT": 0,
        "AVAILABLE_DAYS": 0,
        "ORDERS_QTY": 0,
        "PENDING_PLAN": 0,
        "PLAN_QTY": 0,
        # คอลัมน์ฝั่งใบสั่ง (ORDER_TYPE / CUSTOMER / กำหนดส่ง / สี / ด้าย ...) ถูกเติมจาก
        # fields ด้านล่าง — ค่าตั้งต้นตรงนี้ไว้กันกรณีไม่ได้ส่ง order มา (เรียกจากที่อื่น)
        "ORDER_TYPE": "",
        "ORDER_DATE": None,
        "FG_WEEK": "",
        "TARGET_KNIT": "",
        "TARGET_IDX": None,
        "MATERIAL_CONTENT": "",
        # Hold รอ target เป็นของผ้า core จริง ๆ → ติดธงให้ตรง (หน้าเว็บโชว์ป้าย core และ
        # ด่านยึดเครื่องใช้ธงนี้เป็นตะแกรงอีกชั้น)
        "IS_CORE_ITEM": "CORE ITEM" if source == CORE_HOLD_SOURCE else "",
        "CUSTOMER": "",
        "PLAN_SOURCE": source,
        "COLOR_PART": "",
        "S9_ROUTING": False,
        "OUTSOURCE": "",
    })
    # ข้อมูลใบสั่งที่ทำให้ต้องกันเครื่อง — ยกมาทั้งชุด ยกเว้น SC_LINE_ID ที่ต้องสะสมหลายใบ
    # ผ่าน _color_gap_add_sc (บรรทัดถัดไป) เหมือน SC_SO_NO
    _PLANS_REF[-1].update({k: v for k, v in fields.items() if k != "SC_LINE_ID"})
    _color_gap_held[(_k, gap_idx)] = _hold
    _color_gap_rows[(_k, gap_idx)] = _PLANS_REF[-1]
    _color_gap_add_sc(_PLANS_REF[-1], sc_so_no, line_id)
    # Hold รอ target ของ core: เครื่องชุดนี้จอดว่างทั้งสัปดาห์ → เปิดเป็น "cap เหลือ" ของสัปดาห์นั้น
    # ให้ใบอื่นของผ้าเดียวกันเติมผ่านลูปเก็บตกได้ ; ด่าน CORE FILL BEFORE TRIGGER ยังคุมเหมือนเดิม
    # = เฉพาะใบที่ RDD อยู่ก่อน trigger (ถึง/เลยกำหนดส่งแล้ว) ใบที่ยังไม่ถึงกำหนดรอรอบสต๊อกตามเดิม
    # (user 30 ก.ย. 2569 — เคสจริง FD5DU34B0 RAO: กันเครื่อง 2 ตัวว่างไว้ W52–W2735 ขณะที่ใบ
    #  722334/723008 ของผ้าเดียวกันเลยกำหนดส่งแล้วต้องรอ trigger ไปทอ W2736 ช้า 36-38 สัปดาห์)
    # เครื่องถูกหักพูลไปแล้วที่ _repair_charge_pool ข้างบน → owner ต้องไม่เป็น None (= นับพูลแล้ว)
    # Hold รอสี (HOLD_COLOR) ไม่เปิด — สัปดาห์นั้นตั้งใจเว้นรอสีรอบใหม่ ห้ามวางงาน
    if source == CORE_HOLD_SOURCE and isinstance(globals().get("remaining_week_cap"), dict):
        _hd = _get_capacity_for_mc_group(item_code, mc_group, gauge)
        if _hd and _hd > 0:
            _hd = adjust_daily_cap_for_item_special(_hd, item_code, mc_group, gauge)
        _hcap = float(_hold) * float(_hd or 0) * float(_wd_hold or 0)
        _hrk = (_week, item_code, mc_group)
        if _hcap > 0 and _hrk not in remaining_week_cap:
            remaining_week_cap[_hrk] = _hcap
            remaining_week_cap_owner[_hrk] = str(sc_so_no or "").strip() or CORE_HOLD_SOURCE
            _HOLD_CAP_MC[_hrk] = _hold
    print(f"[{tag}] {_k[0]} {_k[1]} W{_week}: {remark} "
          f"— กันเครื่องไว้ {_hold} เครื่อง (ไม่ setup ใหม่ตอนกลับมาทอ)")
    return _hold


def _color_gap_warm_split(item_code, mc_group, gauge, week_idx, carry_mc, new_mc) -> tuple:
    """สัปดาห์ที่กลับมาทอหลัง Hold (รอสี / รอ target): เครื่องที่ Hold กันไว้ = เครื่องอุ่น → carry ได้เลย ไม่ต้อง setup

    ลูปวางแผนคิด carry จาก "เครื่องของใบสั่งตัวเอง" (machines_in_use) ซึ่งอาจน้อยกว่าที่ Hold กันไว้
    (item เดียวกันหลายใบเคยเดินรวมกัน) แล้ว GRADUAL BOOST เพิ่มเครื่องเป็น new_mc ทั้งหมด → เสียวัน
    setup + กินโควตา job ทั้งที่เครื่องชุดนั้นจอดอุ่นรออยู่ (เคสจริง 14 ก.ย. 2569 FD6DRTPD68/10A0 FA:
    Hold W42 กัน 24 → W43 carry 11 + setup ใหม่ 2) — user: "กันไว้ 16 เครื่องนั้นก็ carry ได้เลย"

    ย้ายเฉพาะ new → carry (เครื่องรวมเท่าเดิม → พูลไม่เปลี่ยน มีแต่โควตา job ที่หักน้อยลง)
    เครื่องอุ่นที่แถวอื่นของคีย์เดียวกันในสัปดาห์นี้รับ carry ไปแล้วไม่นับซ้ำ (หลายใบกลับมาพร้อมกัน)

    คืน (carry, new) ใหม่ — สัปดาห์ก่อนไม่ใช่ Hold ของคีย์นี้ → คืนค่าเดิม
    """
    carry_mc, new_mc = int(carry_mc or 0), int(new_mc or 0)
    if not (COLOR_GAP_ENABLED or CORE_HOLD_ENABLED) or new_mc <= 0 or week_idx is None or int(week_idx) <= 0:
        return carry_mc, new_mc
    _k = _color_gap_key(item_code, mc_group, gauge)
    _held = int(_color_gap_held.get((_k, int(week_idx) - 1), 0) or 0)
    if _held <= carry_mc:
        return carry_mc, new_mc
    _claimed = 0
    for _r in _PLANS_REF:
        if _is_hold_row(_r) or week_index(_r.get("PLAN_WEEK")) != int(week_idx):
            continue
        if _color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE")) != _k:
            continue
        _claimed += int(pd.to_numeric(_r.get("CARRYOVER_MC", 0), errors="coerce") or 0)
    _move = min(new_mc, max(0, _held - _claimed - carry_mc))
    if _move <= 0:
        return carry_mc, new_mc
    print(f"   ⏸ [HOLD WARM] {_k[0]} {_k[1]} G{_k[2]}: Hold สัปดาห์ก่อนกันไว้ {_held} "
          f"(ใบอื่นรับไปแล้ว {_claimed}) → carry {carry_mc}→{carry_mc + _move}, "
          f"new {new_mc}→{new_mc - _move} (เครื่องอุ่น ไม่ต้อง setup)")
    return carry_mc + _move, new_mc - _move


def _core_hold_gap(item_code, mc_group, gauge, next_idx, sc_so_no="", plans_only=False,
                   fields=None) -> int:
    """กันเครื่องไว้ทุกสัปดาห์ที่เว้นระหว่าง 2 รอบผลิตของผ้า core → คืนจำนวนแถว Hold ที่สร้าง

    core ผลิตเป็นรอบตามสต๊อก: ทอจนสต๊อกเต็มแล้วหยุด รอของลดใกล้ MIN จึงทอรอบใหม่ — ระหว่างนั้น
    เครื่องเคยหลุดคืนพูล ทำให้รอบถัดไปต้องหาเครื่องใหม่ + setup ใหม่ (user 16 ก.ย. 2569:
    "ตอนเว้นต้อง lock เครื่องไว้เลย เหมือน hold สี แต่ของ core เป็น hold target")

    กันทุกสัปดาห์ตั้งแต่ถัดจากสัปดาห์สุดท้ายที่คีย์นี้ถือเครื่องจริง จนถึงก่อนรอบถัดไป —
    ไล่จากสัปดาห์เก่าไปใหม่ เพราะแต่ละแถวดู "เครื่องของสัปดาห์ก่อนหน้า" เป็นเพดานของตัวเอง
    (แถว Hold ที่เพิ่งสร้างจึงต่อโซ่ให้สัปดาห์ถัดไปได้) ; สัปดาห์ไหนพูลไม่เหลือก็กันไม่ได้
    แล้วโซ่ขาด — ตัว trim ท้ายแผนจะคืนเครื่องส่วนที่กันไว้เปล่าให้เอง

    plans_only=True → นับ "สัปดาห์สุดท้ายที่ถือเครื่อง" จากแถวแผนรอบนี้เท่านั้น ไม่นับ booking เดิม
    ใช้ตอนขึ้นใบสั่งใหม่ (ดู CORE ORDER GAP): booking เก่าของ item อาจเดินอยู่หลายสัปดาห์ก่อนหน้า
    แล้วแผนรอบนี้เพิ่งเริ่มวางสัปดาห์ไกล ๆ → ถ้านับ booking ด้วยจะไปกันเครื่องย้อนช่วงที่ไม่ใช่
    "ช่องว่างระหว่าง 2 รอบผลิตของแผน" จริง ๆ
    """
    if not CORE_HOLD_ENABLED or next_idx is None:
        return 0
    _k = _color_gap_key(item_code, mc_group, gauge)
    if not _k[0] or not _k[1]:
        return 0
    _nx = int(next_idx)

    # สัปดาห์ล่าสุดที่คีย์นี้ถือเครื่องอยู่จริงก่อนรอบถัดไป (แถวแผนรอบนี้ + booking เดิม)
    _last = -1
    for _r in _PLANS_REF:
        if _color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE")) != _k:
            continue
        if int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0) <= 0:
            continue
        _i = week_index(_r.get("PLAN_WEEK"))
        if _i is not None and int(_i) < _nx:
            _last = max(_last, int(_i))
    if not plans_only:
        for _i, _n in (booking_mc_by_week.get(_k) or {}).items():
            if int(_n or 0) > 0 and int(_i) < _nx:
                _last = max(_last, int(_i))
    if _last < 0 or _nx - _last <= 1:
        return 0                      # ไม่เคยเดินเครื่องเลย / รอบถัดไปติดกันอยู่แล้ว → ไม่มีอะไรให้กัน

    _rows = 0
    for _i in range(_last + 1, _nx):
        if _color_gap_hold_mc(item_code, mc_group, gauge, _i, sc_so_no, fields,
                              source=CORE_HOLD_SOURCE, remark=CORE_HOLD_REMARK,
                              tag="CORE HOLD") > 0:
            _rows += 1
    return _rows


# Objective: ใช้เครื่องให้มากที่สุด โดยยังจบตรง TARGET_KNIT (หน้า "ตั้งค่าระบบ")
PREFER_FULL_MACHINE_TO_TARGET = plan_config.get("KP_PREFER_FULL_MC_TO_TARGET")
# fallback: ถ้าจบตรง TARGET ไม่ได้ อนุญาต TARGET-1
ALLOW_TARGET_MINUS_ONE = plan_config.get("KP_ALLOW_TARGET_MINUS_ONE")

# TARGET_KNIT offset (สัปดาห์ที่หักจาก FG Week)
# ใช้ร่วมกันทุกจุดที่คำนวณ TARGET_KNIT (วางแผนจริง / sort orders / overlay แผนเก่า)
#
# ลำดับกฎ:
#   1. FOB_TYPE ในตาราง FOB_TARGET_OFFSET_WEEKS → ใช้ค่านั้น (ชนะทุกกฎ)
#   2. MC_CAT = DOUBLE-30 และเกจ 16/18/19        → -3
#   3. Type ขึ้นต้น SINGLE (รวม SINGLE (TUBE)) และ yarn เส้นเดียว (ไม่มี '+') → -3
#   4. อื่น ๆ                                     → -4
# (LAB-DIP ไม่ผ่านทางนี้ — ใช้ TODAY_IDX + 2 แยกต่างหาก)
# FOB_TYPE อื่น (Replacement SO / Make to Order / RESERVOIR-GF ฯลฯ) ไม่ต้องอยู่ในตาราง
# → ตกไปตัดสินด้วยกฎเครื่อง/เส้นด้ายด้านล่าง (-3 หรือ -4)
# ชื่อชนิดใบสั่งเทียบแบบตรงตัวพิมพ์ (ERP ส่ง "Sample"/"Salesman-PO") จึง strip อย่างเดียว ไม่ upper
FOB_TARGET_OFFSET_WEEKS = {
    str(_r.get("fob_type", "")).strip(): int(_r.get("weeks") or 0)
    for _r in (plan_config.get("KP_FOB_TARGET_OFFSET") or [])
    if str(_r.get("fob_type", "")).strip()
}
# ทั้งสองค่าแก้ได้ที่หน้า "ตั้งค่าระบบ"
DEFAULT_TARGET_OFFSET_WEEKS = plan_config.get("KP_TARGET_OFFSET_DEFAULT")  # กรณีทั่วไป → N-4
SHORT_TARGET_OFFSET_WEEKS = plan_config.get("KP_TARGET_OFFSET_SHORT")  # DOUBLE-30 เกจ 16/18/19 และ SINGLE yarn เส้นเดียว → N-3
DOUBLE30_SHORT_GAUGES = {_cfg_gauge(_g) for _g in
                         (plan_config.get("KP_DOUBLE30_SHORT_GAUGES") or []) if _cfg_gauge(_g)}


def target_offset_weeks(
    fob_type, mc_group="", gauge="", yarn_used="", item_code=""
) -> int:
    """คืนจำนวนสัปดาห์ที่ต้องหักจาก FG Week เพื่อได้ TARGET_KNIT"""
    _fob = FOB_TARGET_OFFSET_WEEKS.get(str(fob_type).strip())
    if _fob is not None:
        return _fob

    _mc_u = str(mc_group).strip().upper() if mc_group else ""
    if _mc_u and _mc_u not in ("NAN", "NONE"):
        _g = _normalize_gauge(gauge)
        _cat = str(_mc_to_type1(_mc_u, gauge)).strip().upper()

        # DOUBLE-30 เกจ 16/18/19
        if _cat == "DOUBLE-30" and _g in DOUBLE30_SHORT_GAUGES:
            return SHORT_TARGET_OFFSET_WEEKS

        # SINGLE ทุกเกจ/ทุกโรง (รวม SINGLE (TUBE) และสะกด SINGEL) ที่ใช้ yarn เส้นเดียว
        _type = str(MC_TYPE_MAP.get(_mc_u, "")).strip().upper().replace("SINGEL", "SINGLE")
        _is_single = _type.startswith("SINGLE") or _cat.replace("SINGEL", "SINGLE").startswith("SINGLE")
        if _is_single:
            _yarn = str(yarn_used).strip()
            if not _yarn or _yarn.upper() in ("NAN", "NONE"):
                _yarn = str(
                    _yarn_used_lookup.get(str(item_code).strip().upper(), "")
                ).strip()
            if _yarn and _yarn.upper() not in ("NAN", "NONE") and "+" not in _yarn:
                return SHORT_TARGET_OFFSET_WEEKS

    return DEFAULT_TARGET_OFFSET_WEEKS


def target_offset_weeks_for_row(row) -> int:
    """เรียก target_offset_weeks() จากแถว order (ดึงฟิลด์ให้ครบในที่เดียว)"""
    return target_offset_weeks(
        row.get("FOB_TYPE", ""),
        mc_group=row.get("MC GROUP", "") or row.get("MC_GROUP", ""),
        gauge=row.get("MC_GUAGE", "") or row.get("GUAGE", ""),
        yarn_used=(
            row.get("YARN-USED", "")
            or row.get("YARN_USED", "")
            or row.get("YARN_ITEM", "")
        ),
        item_code=row.get("Item Code", "") or row.get("ITEM_CODE", ""),
    )


def _yw_of(v):
    """ค่ากำหนดส่ง → (ปี, สัปดาห์) ; รับได้ทั้ง YYYYWW และเลขสัปดาห์เปล่า"""
    try:
        s = str(int(float(v)))
    except (TypeError, ValueError):
        return None
    if len(s) >= 5:
        return (int(s[:4]), int(s[4:]))
    # เลขสัปดาห์เปล่า → ปีของครั้งที่ใกล้วันนี้สุด (เดิมใช้ปีของวันนี้ตรง ๆ ซึ่งผิดตอนคร่อมปี)
    _k = _TL.key(int(s))
    return (yw_year(_k), int(s)) if _k is not None and _k >= 100000 else (TODAY.year, int(s))


def _rdd_week_of(order):
    """RDD ของใบจากต้นทางตรง ๆ → YYYYWW (int) ; ไม่มี = None

    PERIOD_WEEK ของ order = RDD_WEEK ของ booking view (Replan แมปมา) / PERIOD_WEEK ของ view_sc
    มาได้ทั้ง '2026/16' และ 202616 ; ห้ามใช้ FG แทน (user 22/9/2569: ห้ามเติมข้อมูลเอง —
    เดิมช่อง RDD_WEEK ของแผนคือ FG ทำให้ใบเดียวกันโชว์ RDD คนละค่าต่อ item)
    """
    if order is None:
        return None
    _s = str(order.get("PERIOD_WEEK", "") or "").strip()
    m = (re.match(r"^(\d{4})\D+(\d{1,2})$", _s)
         or re.match(r"^(\d{4})(\d{2})(?:\.0+)?$", _s))
    if m and 1 <= int(m.group(2)) <= 53:
        return int(m.group(1)) * 100 + int(m.group(2))
    return None


def _target_knit_of(fg, otype, ref_row):
    """FG week → (TARGET_KNIT week, ปี) — เดดไลน์ทอที่ถอยจากกำหนดส่งตาม offset ของงานนั้น

    ref_row = แถวที่มี FOB_TYPE / เครื่อง / เกจ / ด้าย ให้คิด offset (ใช้ได้ทั้งแถว order และ
    แถว booking) ; LAB-DIP ไม่ถอยจาก FG แต่ใช้พื้น TODAY+2 ตามกติกาเดิม
    ใช้ 3 ที่: detect_and_fill_unused_capacity, แถว OLD (booking เดิม) และแถว core ของชีท
    REPLAN ที่ Replan.py เว้นไว้ให้ — วางไว้ต้นไฟล์เพราะจุดแรกถูกเรียกก่อนอีกสองจุด
    """
    _yw = _yw_of(fg)
    if _yw is None:
        return (None, None)
    # คืนค่าเป็น (เลขสัปดาห์เปล่า, ปี) — ผู้เรียกทุกจุดเขียนลงคอลัมน์ TARGET_KNIT / TARGET_YEAR
    _crow = calendar_week[
        (calendar_week["YEAR"] == _yw[0]) & (calendar_week["WEEK_NUM"] == _yw[1])
    ]
    if _crow.empty:
        # กำหนดส่งอยู่นอกช่วงปฏิทิน — ปฏิทินเริ่มต้นปีปัจจุบัน ส่วน booking มีใบค้างจากปีก่อน
        # (จาก booking จริง: FG ปี 2025 อยู่ 33 แถว) → เดิมคืน None ทำให้ TARGET_KNIT ว่างทั้งแถว
        # ก่อนปฏิทิน = เดดไลน์ผ่านไปแล้ว → ยึดสัปดาห์แรกที่ยังมีในปฏิทิน (เร็วที่สุดเท่าที่ทำได้)
        # หลังปฏิทิน = ไกลเกินขอบ → ยึดสัปดาห์สุดท้าย
        _yw_num = _yw[0] * 100 + _yw[1]
        _first = calendar_week.iloc[0]
        _last = calendar_week.iloc[-1]
        _edge = _first if _yw_num < int(_first["WEEK"]) else _last
        return (int(_edge["WEEK_NUM"]), int(_edge["YEAR"]))
    try:
        if str(otype or "").strip().upper() == "LAB-DIP":
            _idx = min(len(calendar_week) - 1, TODAY_IDX + FREEZE_WEEKS)
        else:
            _idx = max(0, _crow.index[0] - target_offset_weeks_for_row(ref_row))
        return (int(calendar_week.iloc[_idx]["WEEK_NUM"]), int(calendar_week.iloc[_idx]["YEAR"]))
    except Exception:
        return (None, None)


def _clean_remark(value) -> str:
    """แปลงหมายเหตุจากไฟล์ order เป็นข้อความสะอาด — ช่องว่างต้องได้ "" ไม่ใช่คำว่า "nan"

    `str(NaN)` = "nan" และ `NaN or ''` ก็ได้ NaN (NaN เป็น truthy) → เดิมคำว่า nan
    หลุดไปโผล่ในคอลัมน์ LINE_REMARK บนหน้าเว็บ
    """
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "" if text.lower() in ("nan", "nat", "none") else text


# Load Balancing Configuration — หน้า "ตั้งค่าระบบ" เก็บเป็น % แล้วแปลงเป็นสัดส่วนที่นี่
USE_LOAD_BALANCING = plan_config.get("KP_LOAD_BALANCING")  # เปิดใช้งาน Load Balancing สำหรับการกระจายงาน
LOAD_BALANCING_WEIGHT = plan_config.ratio("KP_LOAD_BALANCING_WEIGHT_PCT")  # น้ำหนักของ "เครื่องว่าง" ใน score
LOAD_BALANCING_THRESHOLD = plan_config.ratio("KP_LOAD_BALANCING_THRESHOLD_PCT")  # เกินค่าเฉลี่ยกี่ส่วน = ไม่สมดุล



# =========================
# SHARED MACHINE POOL
# กลุ่ม MC ที่ใช้เครื่องร่วมกัน — ต้องตรงกับ AVA_MC.py
# =========================
# โหลด SHARED_POOL_MAP แบบ dynamic จาก MasterMC
_MASTER_MC_PATH = os.path.expandvars(_cfg["paths"]["master_mc"])



# =========================
# LOAD DATA
orders = pd.read_excel(ORDER_FILE)
# Map column names from order_ready.xlsx to expected names
column_mapping = {
    'SC_NO': 'SC/SO NO',
    'FABRIC_ITEM': 'Item Code', 
    'ORDERS_QTY': 'Orders.Qty',
    'PLAN_QTY': 'Plan Qty',
    'PENDING_PLAN': 'Pending Plan',
    'FGWEEK': 'FG Week',
    'CUSTOMER_NAME': 'Customer',
    'ORDER_TYPE': 'Orders Type',
    'NAY_COLOR': 'NAY_COLOR',
    'COLOR_DESC': 'COLOR_DESC'
}

orders = orders.rename(columns=column_mapping)

# ── เลขใบสั่งต้องเป็น "ข้อความสะอาด" ตั้งแต่ตอนโหลด ────────────────────────────
# SO_NO เป็นเลขล้วน + ใบ SC ที่ยังไม่แปลงเป็น SO มีค่าว่าง → pandas อ่านทั้งคอลัมน์
# เป็น float64 แล้วทุกจุดที่ทำ str(order.get("SO_NO", ...)) ได้ "721915.0" / "nan"
# หลุดลงชีท PLAN/SETUP_TRACKING/UNPLANNED และทำให้ join พลาดเงียบ ๆ
# (วัดจริงบนแผนเซิร์ฟเวอร์ 2-9-2569: PLAN 69/69 แถวติด ".0", UNPLANNED 4,521 แถวเป็น "nan")
# แก้ที่จุดโหลดจุดเดียวแทนการไล่แก้ str() ทุกจุด — ค่าว่างคงเป็นค่าว่าง ไม่เติมเลข SC แทน
for _sc_col_norm in ("SO_NO", "SC/SO NO", "SC_NO"):
    if _sc_col_norm in orders.columns:
        orders[_sc_col_norm] = orders[_sc_col_norm].map(ColorOpen.so_text)

# REPLAN: จำ "สัปดาห์เดิมที่ booking จองไว้" ของแต่ละงาน (Replan.py ใส่มาใน order_replan.xlsx)
# เก็บตรงนี้เพราะ orders ถูกกรอง/เรียงใหม่ระหว่าง planning loop — ถ้าไปอ่านทีหลังจะไม่ครบ
# คีย์ = (SO_NO, Item Code) เท่ากับที่ plan row ใช้ (SC_SO_NO + ITEM_CODE)
# แถวที่มาจาก SC ล้วนไม่มีค่านี้ → ไม่อยู่ใน map = ว่างในชีท PLAN (แยกออกจากงานที่ยกมาจาก booking)
_replan_from_week: dict = {}
if REPLAN_MODE and "BOOKING_WEEK_FROM" in orders.columns:
    _bw_from = pd.to_numeric(orders["BOOKING_WEEK_FROM"], errors="coerce")
    _bw_to = pd.to_numeric(orders.get("BOOKING_WEEK_TO", orders["BOOKING_WEEK_FROM"]), errors="coerce")
    for _so, _it, _wf, _wt in zip(
        orders.get("SO_NO", pd.Series([""] * len(orders))), orders["Item Code"], _bw_from, _bw_to
    ):
        if pd.isna(_wf):
            continue
        _k = (str(_so).strip().upper(), str(_it).strip().upper())
        _cur = _replan_from_week.get(_k)
        _lo, _hi = int(_wf), int(_wt) if pd.notna(_wt) else int(_wf)
        if _cur:
            _lo, _hi = min(_lo, _cur[0]), max(_hi, _cur[1])
        _replan_from_week[_k] = (_lo, _hi)
    print(f"🔁 REPLAN: จำสัปดาห์เดิมของงาน booking ที่ยกมาวางใหม่ {len(_replan_from_week)} งาน")


def _due_keys(so, item, fg) -> set:
    """(เลขใบ, รหัสผ้า, FG YYYYWW) ของแถว — ช่องใบที่ยุบหลายเลข (',' / '|') แตกเป็นหลายคีย์"""
    _fg = pd.to_numeric(fg, errors="coerce")
    _it = str(item or "").strip().upper()
    if pd.isna(_fg) or not _it:
        return set()
    _toks = re.split(r"[,|]", str(ColorOpen.so_text(so) or ""))
    return {(t.strip().upper(), _it, int(_fg)) for t in _toks if t.strip()}


# REPLAN: booking ที่ต้นทางไม่มี FG → Replan.py ใส่ RDD ลงช่อง FG Week (ธง DUE_FROM_RDD=1)
# → TARGET_KNIT คิดจาก RDD และช่อง FG_WEEK ของแผนโชว์ค่า RDD (user 24/9/2569: "เอาค่า RDD ไปใส่ใน FG")
# ช่อง RDD_WEEK ยังมาจาก PERIOD_WEEK ตรง ๆ ไม่ถูกแตะ ; ติดป้าย DUE_BASIS='RDD' ไว้ให้รู้ว่า FG แถวนั้น
# ไม่ได้มาจากต้นทาง (ดู _mark_rdd_fg ก่อนเขียนไฟล์)
# จำไว้ตอนนี้เพราะ orders ถูกกรอง/แตกก้อนระหว่างลูป ; คีย์มีสัปดาห์ด้วย เพราะใบ+ผ้าเดียวกัน
# มีได้ทั้งบรรทัดที่มี FG จริงและบรรทัดที่ไม่มี (เคสจริง 24/9/2569: 4 คู่ เช่น 719279 FG W30 / RDD W32)
_DUE_RDD_KEYS: set = set()
if REPLAN_MODE and "DUE_FROM_RDD" in orders.columns:
    _dr_flag = pd.to_numeric(orders["DUE_FROM_RDD"], errors="coerce").fillna(0) == 1
    _dr_real: set = set()
    for _so, _it, _fgv, _is in zip(orders.get("SO_NO", pd.Series([""] * len(orders))),
                                  orders["Item Code"], orders["FG Week"], _dr_flag):
        (_DUE_RDD_KEYS if _is else _dr_real).update(_due_keys(_so, _it, _fgv))
    # ชนกับบรรทัดที่มี FG จริงสัปดาห์เดียวกัน → แถวแผนแยกไม่ออกว่าเป็นของบรรทัดไหน → ไม่ติดป้าย
    _DUE_RDD_KEYS -= _dr_real
    if int(_dr_flag.sum()):
        print(f"📅 REPLAN: งานไม่มี FG ที่วางตาม RDD {int(_dr_flag.sum())} แถว (ช่อง FG_WEEK = RDD)")


def _mark_rdd_fg(df: pd.DataFrame, label: str) -> None:
    """แถวที่ FG มาจาก RDD (ต้นทางไม่มี FG) → DUE_BASIS='RDD' (แก้ใน df ตรง ๆ ; ค่า FG ไม่แตะ)"""
    if not _DUE_RDD_KEYS or not isinstance(df, pd.DataFrame) or df.empty:
        return
    _fgc = next((c for c in ("FG_WEEK", "FG Week") if c in df.columns), None)
    _soc = next((c for c in ("SC_SO_NO", "SO_NO", "SC/SO NO") if c in df.columns), None)
    _itc = next((c for c in ("ITEM_CODE", "Item Code") if c in df.columns), None)
    if not (_fgc and _soc and _itc):
        return
    _hit = pd.Series(
        [bool(_due_keys(s, i, f) & _DUE_RDD_KEYS) for s, i, f in zip(df[_soc], df[_itc], df[_fgc])],
        index=df.index,
    )
    if "DUE_BASIS" not in df.columns:
        df["DUE_BASIS"] = ""
    if _hit.any():
        df.loc[_hit, "DUE_BASIS"] = "RDD"
        print(f"📅 {label}: แถวที่ FG มาจาก RDD (ต้นทางไม่มี FG) {int(_hit.sum())} แถว → DUE_BASIS=RDD")

# แถวที่ Order.py กรองออกก่อนวางแผน (ORDER_TYPE/MC_GROUP ต้องห้าม) — ไม่เคยเข้า planning loop
# ต้องเอามาโชว์ในชีท UNPLANNED ไม่งั้นงานพวกนี้หายเงียบทั้งที่ยังมีของค้าง
# rename ด้วย column_mapping ชุดเดียวกับ orders เพื่อให้คอลัมน์ตรงกันตอนรวมเป็น DataFrame เดียว
_EXCLUDED_ORDER_FILE = DATA_PLAN_DIR / "order_excluded.xlsx"
_excluded_orders = pd.DataFrame()
try:
    if _EXCLUDED_ORDER_FILE.exists():
        _excluded_orders = pd.read_excel(_EXCLUDED_ORDER_FILE)
        _excluded_orders.columns = _excluded_orders.columns.str.strip()
        _excluded_orders = _excluded_orders.rename(columns=column_mapping)
        # แถวพวกนี้ไหลตรงเข้าชีท UNPLANNED → ต้องล้าง ".0"/"nan" แบบเดียวกับ orders
        for _sc_col_norm in ("SO_NO", "SC/SO NO", "SC_NO"):
            if _sc_col_norm in _excluded_orders.columns:
                _excluded_orders[_sc_col_norm] = _excluded_orders[_sc_col_norm].map(ColorOpen.so_text)
        print(f"📄 order ที่ถูกกรองออกก่อนวางแผน: {len(_excluded_orders)} แถว")
except Exception as _e_exc:
    print(f"⚠️ อ่าน {_EXCLUDED_ORDER_FILE.name} ไม่ได้: {_e_exc}")
    _excluded_orders = pd.DataFrame()
# REPLAN: งาน booking ที่ไม่มี FG week จากต้นทาง → เข้าชีท UNPLANNED ทางเดียวกับ order_excluded
if REPLAN_MODE and REPLAN_NO_FG_FILE and Path(REPLAN_NO_FG_FILE).exists():
    try:
        _no_fg_orders = pd.read_excel(REPLAN_NO_FG_FILE, sheet_name="NO_FG")
        _no_fg_orders.columns = _no_fg_orders.columns.str.strip()
        if not _no_fg_orders.empty:
            _no_fg_orders = _no_fg_orders.rename(columns=column_mapping)
            for _sc_col_norm in ("SO_NO", "SC/SO NO", "SC_NO"):
                if _sc_col_norm in _no_fg_orders.columns:
                    _no_fg_orders[_sc_col_norm] = _no_fg_orders[_sc_col_norm].map(ColorOpen.so_text)
            _no_fg_orders["REASON"] = "ไม่มีทั้ง FG และ RDD จากต้นทาง — ไม่วางแผน"
            _excluded_orders = pd.concat([_excluded_orders, _no_fg_orders], ignore_index=True)
            print(f"📄 งาน booking ไม่มีทั้ง FG และ RDD (ไม่วางแผน → UNPLANNED): {len(_no_fg_orders)} แถว")
    except (ValueError, KeyError) as _e_nofg:
        print(f"⚠️ อ่านชีท NO_FG ของ {Path(REPLAN_NO_FG_FILE).name} ไม่ได้: {_e_nofg}")
    # REPLAN: ก้อนที่ไม่มีด้าย (สต๊อก + PO Pending ไม่พอ — YarnStockCover.supply_cap) → UNPLANNED พร้อมเหตุผลต่อแถว
    # ไม่มีชีท = ปิดสวิตช์ / คิดไม่ได้ / ไฟล์รุ่นก่อน → ไม่มีงานที่ถูกตัด
    try:
        _yarn_short = pd.read_excel(REPLAN_NO_FG_FILE, sheet_name="YARN_SHORT")
        _yarn_short.columns = _yarn_short.columns.str.strip()
        if not _yarn_short.empty:
            _ys_reason = _yarn_short["REASON"] if "REASON" in _yarn_short.columns else None
            _yarn_short = _yarn_short.rename(columns=column_mapping)
            for _sc_col_norm in ("SO_NO", "SC/SO NO", "SC_NO"):
                if _sc_col_norm in _yarn_short.columns:
                    _yarn_short[_sc_col_norm] = _yarn_short[_sc_col_norm].map(ColorOpen.so_text)
            _yarn_short["REASON"] = (_ys_reason.fillna("ด้ายไม่พอ (สต๊อก + PO Pending)").astype(str)
                                     if _ys_reason is not None else "ด้ายไม่พอ (สต๊อก + PO Pending)")
            _excluded_orders = pd.concat([_excluded_orders, _yarn_short], ignore_index=True)
            print(f"📄 งานที่ไม่มีด้าย (สต๊อก + PO Pending ไม่พอ → UNPLANNED): {len(_yarn_short)} แถว")
    except (ValueError, KeyError):
        pass
summary_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="SUMMARY_MC_REMAIN")
detail_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="DETAIL")  # โหลด DETAIL
# DETAIL_SC = ยอด/กำหนดส่ง "ต่อใบสั่ง" ของบล็อกเดียวกันใน DETAIL (AVA_MC.py สร้างให้)
# DETAIL ยุบทุก SO ของ (item, สัปดาห์, เครื่อง, เกจ) เป็นแถวเดียว — จำเป็นต่อการนับเครื่อง
# แต่ทำให้แถว OLD มี SC_SO_NO เป็น "708936,719941" และ FG/RDD เหลือใบที่เร็วสุดใบเดียว
# → ใช้ตารางนี้แตกแถว OLD ให้เห็นทีละใบพร้อมกำหนดส่งจริงของใบนั้น
# ไฟล์ booking ที่สร้างก่อนมีชีทนี้ → ตารางว่าง = แถว OLD ยุบใบเหมือนเดิมทุกประการ
try:
    detail_mc_sc = pd.read_excel(MC_REMAIN_FILE, sheet_name="DETAIL_SC")
    detail_mc_sc.columns = detail_mc_sc.columns.str.strip().str.upper()
    print(f"🧾 booking DETAIL_SC (ยอดต่อใบสั่ง): {len(detail_mc_sc)} แถว")
except (ValueError, KeyError):
    detail_mc_sc = pd.DataFrame()
    print("⚠️ ไฟล์ booking ไม่มีชีท DETAIL_SC (สร้างก่อนอัปเดต AVA_MC) → แถว OLD จะยุบใบเหมือนเดิม")
# ฝั่ง booking ก็ต้องสะอาดเหมือนกัน — DETAIL_SC ใช้ SO_NO เป็นคีย์ groupby จึงคงชนิดตัวเลข
# ไว้ตอนเขียน Excel และกลายเป็น ".0" ได้ทันทีที่มีใบไหนไม่มี SO (แถว OLD ในชีท PLAN)
for _bk_df_norm in (detail_mc, detail_mc_sc):
    if not _bk_df_norm.empty and "SO_NO" in _bk_df_norm.columns:
        _bk_df_norm["SO_NO"] = _bk_df_norm["SO_NO"].map(ColorOpen.so_text)
fd6_check = detail_mc[detail_mc["ITEM_CODE"].astype(str).str.upper().str.strip() == "FD6GNTLG27/58A0"]
if not fd6_check.empty:

    print(f"[DEBUG] FD6GNTLG27/58A0 found in detail_mc: {len(fd6_check)} rows")
    print(fd6_check[["ITEM_CODE", "WEEK", "MC_GROUP", "GUAGE", "MC_USE_CEIL"]])
else:

    print(f"[DEBUG] FD6GNTLG27/58A0 NOT found in detail_mc")
item_cap_data = pd.DataFrame(columns=["ITEM_CODE", "MC_GROUP", "CAP ทอ", "REVOLUTION/WEIGHT", "GUAGE"])
master_mc = pd.read_excel(_MASTER_MC_PATH)
# โหลด Itemcore สำหรับเช็ค RTS items
# อ่านทะเบียน Core Item จากไฟล์ Target_Stock (คอลัมน์ Item code + Customer) ซึ่ง user แก้บน server ได้
# → ตัดปัญหาไฟล์ Itemcore.xlsx แยกที่ค้างของเก่า (data drift). ถ้าอ่านไม่ได้ค่อย fallback ไฟล์เดิม
try:

    try:
        _itemcore_src = os.path.expandvars(_cfg["paths"]["target_stock"])
    except KeyError:
        _itemcore_src = None

    if _itemcore_src and Path(_itemcore_src).exists():
        itemcore_df = pd.read_excel(_itemcore_src)
    else:
        itemcore_df = pd.read_excel(ITEMCORE_FILE)

    itemcore_df.columns = itemcore_df.columns.str.strip()

except Exception as e:

    print(f"⚠️ ไม่สามารถโหลด Itemcore: {e}")

    itemcore_df = pd.DataFrame()

calendar = load_calendar(CALENDAR_FILE, sheet_name="Sheet1")

calendar_week = calendar_week_map(calendar)

orders.columns = orders.columns.str.strip()

summary_mc.columns = summary_mc.columns.str.strip().str.upper()

if "MC_CAT" in summary_mc.columns and "TYPE_1" not in summary_mc.columns:
    summary_mc = summary_mc.rename(columns={"MC_CAT": "TYPE_1"})

summary_mc["TYPE_1"] = summary_mc["TYPE_1"].astype(str).str.strip().str.upper()

calendar_week.columns = calendar_week.columns.str.strip().str.upper()
# ── คีย์สัปดาห์ทั้งไฟล์ = YW (ปี*100+สัปดาห์ เช่น W42/2026 → 202642) ─────────────────────────
# เดิมใช้เลขสัปดาห์เปล่า (1–53) ซึ่งซ้ำกันทุกปี → state ที่คีย์ด้วยสัปดาห์ (ยอดใช้เครื่อง, carry,
# โควตา job ...) ชนกันข้ามปี แผนจึงยาวได้ไม่เกิน 1 ปี ; YW ไม่ซ้ำและเรียงตามเวลาได้ตรง ๆ
# calendar_week["WEEK"] = YW ตั้งแต่ตรงนี้ · เลขสัปดาห์เปล่าอยู่ที่ WEEK_NUM
# ตัวเลขที่มาจากชีท/ไฟล์โดยไม่มีปี → แปลงผ่าน _TL.key() (ครั้งที่ใกล้วันนี้สุด) ที่จุดโหลด
# ไฟล์ที่เขียนออกยังแยก PLAN_WEEK + PLAN_YEAR เหมือนเดิม (แปลงตอน export)
calendar_week["WEEK_NUM"] = calendar_week["WEEK"].astype(int)
calendar_week["YEAR"] = calendar_week["YEAR"].astype(int)
calendar_week["WEEK"] = calendar_week["YEAR"] * 100 + calendar_week["WEEK_NUM"]
calendar_week["YW_INT"] = calendar_week["WEEK"]
_TL = WeekTimeline(calendar_week, today=TODAY)
_IDX_BY_YW = {int(_k): _i for _i, _k in enumerate(calendar_week["WEEK"].tolist())}


def _weeks_to_yw(df, week_col="WEEK", year_col="YEAR"):
    """แปลงคอลัมน์สัปดาห์ของตารางที่โหลดเข้ามาเป็น YW (แก้ในที่) — ใช้ปีจากคอลัมน์ YEAR ถ้ามี
    ไม่มีปี = ครั้งที่ใกล้วันนี้สุด ; W99 (งานล้นของ booking) และค่าที่เป็น YW อยู่แล้วคงเดิม"""
    if df is None or df.empty or week_col not in df.columns:
        return df
    _w = pd.to_numeric(df[week_col], errors="coerce")
    _y = pd.to_numeric(df[year_col], errors="coerce") if year_col and year_col in df.columns else None
    _out = []
    for _i, _wv in enumerate(_w.tolist()):
        if pd.isna(_wv):
            _out.append(None)
            continue
        _yv = None if _y is None or pd.isna(_y.iloc[_i]) else int(_y.iloc[_i])
        _out.append(_TL.key(int(_wv), _yv))
    df[week_col] = pd.to_numeric(pd.Series(_out, index=df.index), errors="coerce")
    return df


_weeks_to_yw(summary_mc)

detail_mc.columns = detail_mc.columns.str.strip().str.upper()  # เพิ่ม detail_mc
_weeks_to_yw(detail_mc)
_weeks_to_yw(detail_mc_sc)

master_mc.columns = master_mc.columns.str.strip()

if "MC_CAT" in master_mc.columns and "Type_1" not in master_mc.columns:
    master_mc = master_mc.rename(columns={"MC_CAT": "Type_1"})



# MC → Type_1 lookup: (MC_upper, guage_str) → Type_1

_MC_TYPE1_MAP: dict = {}

for _, _mrow in master_mc.iterrows():

    _mmc = str(_mrow.get("MC", "")).strip().upper()

    _mg = str(_mrow.get("Guage", "")).strip()

    _mt1 = str(_mrow.get("Type_1", "")).strip().upper()

    if _mmc and _mt1:

        _MC_TYPE1_MAP[(_mmc, _mg)] = _mt1

        if (_mmc, "") not in _MC_TYPE1_MAP:

            _MC_TYPE1_MAP[(_mmc, "")] = _mt1

print(f"✅ MC→Type_1 map: {len(_MC_TYPE1_MAP)} entries")

# MC → Factory map: (mc_upper, guage_str) → factory_upper
_mc_factory_map: dict = {}
for _, _mrow in master_mc.iterrows():
    _mmc = str(_mrow.get("MC", "")).strip().upper()
    _mg = str(_mrow.get("Guage", "")).strip()
    _mf = str(_mrow.get("Factory", "")).strip().upper()
    if _mmc and _mf:
        _mc_factory_map[(_mmc, _mg)] = _mf
        if (_mmc, "") not in _mc_factory_map:
            _mc_factory_map[(_mmc, "")] = _mf
print(f"✅ MC→Factory map: {len(_mc_factory_map)} entries")

# MC → Setup time map: mc_upper → setup_days (จาก MasterMC column "Set up time")
_mc_setup_time_map: dict = {}
for _, _mrow in master_mc.iterrows():
    _mmc = str(_mrow.get("MC", "")).strip().upper()
    _st = _mrow.get("Set up time", None)
    if _mmc and _st is not None and str(_st).strip() not in ("", "nan", "NAN", "NaT", "None"):
        try:
            _mc_setup_time_map[_mmc] = int(float(_st))
        except (ValueError, TypeError):
            pass
print(f"✅ MC→SetupTime map: {len(_mc_setup_time_map)} entries")

# Item prefix → Setup days map (จากชีท "Item Setup" ใน MasterMC) — ใช้เฉพาะ Type SINGLE
# format: prefix_upper → setup_days  (เช่น "FD1" → 2)  จับคู่แบบ longest-prefix match
# ช่อง ITEM อาจมีหลาย prefix คั่นด้วย comma (เช่น "F1 , FD1") → แยกเป็นหลาย key
_item_prefix_setup_map: dict = {}
try:
    _isetup_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Item Setup")
    _isetup_df.columns = _isetup_df.columns.str.strip()
    for _, _isrow in _isetup_df.iterrows():
        _iprefix_raw = str(_isrow.get("ITEM", "")).strip()
        _iday = _isrow.get("Set up (day)", None)
        if not _iprefix_raw or _iprefix_raw.upper() in ("", "NAN", "NONE"):
            continue
        if _iday is None or str(_iday).strip() in ("", "nan", "NAN", "NaT", "None"):
            continue  # prefix ว่าง (เช่น F9) → ข้าม ให้ตกไปใช้ default
        try:
            _iday_int = int(float(_iday))
        except (ValueError, TypeError):
            continue
        for _pfx in _iprefix_raw.split(","):
            _pfx = _pfx.strip().upper()
            if _pfx:
                _item_prefix_setup_map[_pfx] = _iday_int
    print(f"✅ Item prefix→Setup map: {len(_item_prefix_setup_map)} entries")
except Exception as _e_isetup:
    print(f"⚠️ ไม่สามารถโหลดชีท Item Setup: {_e_isetup}")

# Item → Team map (จากชีท "Program" ใน MasterMC) — user กำหนดเองว่า item ไหนอยู่ทีมไหน
# ใช้เติมคอลัมน์ TEAM ในไฟล์แผน เพื่อให้หน้าเว็บไฮไลต์ (ITEM+TEAM ต้องตรงคู่กัน) ได้
# item เดียวมีได้หลายทีม → เก็บรวมคั่นด้วย " , " (หน้าเว็บแยกเทียบทีละทีม)
_item_program_map: dict = {}
try:
    _prog_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Program")
    _prog_df.columns = _prog_df.columns.str.strip()
    for _, _prow in _prog_df.iterrows():
        _pitem = str(_prow.get("ITEM_CODE", "")).strip().upper()
        _pteam = str(_prow.get("TEAM_NAME", "")).strip()
        if not _pitem or _pitem in ("NAN", "NONE"):
            continue
        if not _pteam or _pteam.upper() in ("NAN", "NONE"):
            continue
        _teams = _item_program_map.setdefault(_pitem, [])
        if _pteam not in _teams:
            _teams.append(_pteam)
    print(f"✅ Item→Program team map: {len(_item_program_map)} items")
except Exception as _e_prog:
    print(f"⚠️ ไม่สามารถโหลดชีท Program: {_e_prog}")


def _lookup_item_team(item_code) -> str:
    """คืนทีม (จากชีท Program) ของ item — ไม่มีในชีท = คืนค่าว่าง"""
    _ic = str(item_code).strip().upper()
    return " , ".join(_item_program_map.get(_ic, []))


# ── ทีมจริงของงาน (TEAM_NAME) ───────────────────────────────────────────────
# TEAM_NAME มีแค่ฝั่ง booking (View_Booking → AVA_MC ยุบเป็น "RTS,NYK1-1" ในชีท DETAIL)
# และ stock (filtered_stock_data) — view_sc ไม่มีคอลัมน์ทีมเลย งานที่มาจาก SC ล้วน
# จึงต้อง map ย้อน: ใบเดียวกัน (SO_NO+ITEM) ก่อน ไม่เจอค่อยถอยไปดูทีมที่เคยทอ item นั้น
# อ่านทั้งไฟล์ booking ของรอบนี้ (MC_REMAIN_FILE) และ booking_final_ready ตัวเต็ม —
# โหมด replan ตัด booking ที่ปลดล็อกออกจากไฟล์รอบนี้ ทีมของงานพวกนั้นจะหายไปด้วย
# (ทีมเป็นข้อมูลอ้างอิงล้วน ไม่แตะ capacity/เครื่อง จึงอ่านไฟล์เต็มได้โดยไม่กระทบแผน)
_so_team_map: dict = {}     # (SO_NO, ITEM_CODE) → [ทีม, ...]
_item_team_map: dict = {}   # ITEM_CODE → [ทีม, ...]


def _split_teams(v) -> list:
    """แตกค่าทีมที่ยุบมาแล้ว ("RTS,NYK1-1") เป็นลิสต์
    ตัดด้วย "," เท่านั้น — ชื่อทีมมี "/" ได้ (เช่น NYG/TRM) ห้ามตัด"""
    _s = str(v or "").strip()
    if not _s or _s.upper() in ("NAN", "NONE", "NAT"):
        return []
    return [t.strip() for t in _s.replace(" , ", ",").split(",") if t.strip()]


def _add_team(_map: dict, key, team: str) -> None:
    _lst = _map.setdefault(key, [])
    if team not in _lst:
        _lst.append(team)


try:
    _team_src_files = []
    for _tf in (MC_REMAIN_FILE, _latest_booking_final()):
        if _tf is not None and Path(_tf).exists() and _tf not in _team_src_files:
            _team_src_files.append(_tf)
    for _tf in _team_src_files:
        _tdf = pd.read_excel(_tf, sheet_name="DETAIL")
        _tdf.columns = _tdf.columns.str.strip()
        if "TEAM_NAME" not in _tdf.columns or "ITEM_CODE" not in _tdf.columns:
            continue
        _has_so = "SO_NO" in _tdf.columns
        for _, _trow in _tdf.iterrows():
            _titem = str(_trow.get("ITEM_CODE", "")).strip().upper()
            if not _titem or _titem in ("NAN", "NONE"):
                continue
            _tteams = _split_teams(_trow.get("TEAM_NAME"))
            if not _tteams:
                continue
            # SC_SO_NO ฝั่งแผนกับ SO_NO ฝั่ง booking ต้อง normalize ชุดเดียวกัน
            # (split_sc = แตกใบที่ยุบคอมมา + ตัด ".0" + ตัด S นำหน้า) ไม่งั้น join พลาดเงียบ
            _tsos = ColorOpen.split_sc(_trow.get("SO_NO")) if _has_so else []
            for _tt in _tteams:
                _add_team(_item_team_map, _titem, _tt)
                for _tso in _tsos:
                    _add_team(_so_team_map, (_tso, _titem), _tt)
    print(f"✅ ทีมจริงจาก booking: {len(_so_team_map)} (ใบสั่ง+item), {len(_item_team_map)} item "
          f"[{', '.join(Path(f).name for f in _team_src_files)}]")
except Exception as _e_team:
    print(f"⚠️ ไม่สามารถอ่านทีมจาก booking: {_e_team}")

# ชั้นสำรอง: item ที่ไม่เคยมีใน booking แต่มีของในสต๊อก (core) → ทีมจากสต๊อก
_stock_team_map: dict = {}
try:
    _stdf = pd.read_excel(DATA_PLAN_DIR / "filtered_stock_data.xlsx")
    _stdf.columns = _stdf.columns.str.strip()
    if "TEAM_NAME" in _stdf.columns and "ITEM_CODE" in _stdf.columns:
        for _, _strow in _stdf.iterrows():
            _sitem = str(_strow.get("ITEM_CODE", "")).strip().upper()
            if not _sitem or _sitem in ("NAN", "NONE"):
                continue
            for _st in _split_teams(_strow.get("TEAM_NAME")):
                _add_team(_stock_team_map, _sitem, _st)
        print(f"✅ ทีมจริงจากสต๊อก: {len(_stock_team_map)} item")
except Exception as _e_steam:
    print(f"⚠️ ไม่สามารถอ่านทีมจากสต๊อก: {_e_steam}")


def _lookup_row_team(item_code, so_no) -> tuple:
    """ทีมจริงของแถวแผน → (ทีมที่คั่นด้วย " , ", ชั้นที่ได้มา)
    ลำดับ: ใบสั่งเดียวกัน → item ใน booking → item ในสต๊อก → ชีท Program → ว่าง"""
    _ic = str(item_code).strip().upper()
    if not _ic or _ic in ("NAN", "NONE"):
        return "", "blank"
    _hits: list = []
    for _so in ColorOpen.split_sc(so_no):
        for _t in _so_team_map.get((_so, _ic), []):
            if _t not in _hits:
                _hits.append(_t)
    if _hits:
        return " , ".join(_hits), "so"
    if _item_team_map.get(_ic):
        return " , ".join(_item_team_map[_ic]), "item"
    if _stock_team_map.get(_ic):
        return " , ".join(_stock_team_map[_ic]), "stock"
    _prog = _lookup_item_team(_ic)
    return (_prog, "program") if _prog else ("", "blank")


def _lookup_item_prefix_setup(item_code: str):
    """คืน setup days จาก prefix ของ item (longest-prefix match) หรือ None ถ้าไม่ match"""
    _ic = str(item_code).strip().upper()
    if not _ic or not _item_prefix_setup_map:
        return None
    # longest prefix ก่อน (เช่น "FDC" ต้องมาก่อน "FC"/"FD")
    for _pfx in sorted(_item_prefix_setup_map, key=len, reverse=True):
        if _ic.startswith(_pfx):
            return _item_prefix_setup_map[_pfx]
    return None

# Spare cylinder map: (factory_upper, mc_cat_upper, guage_str) → total_spare
_spare_cylinder_map: dict = {}
try:
    _spare_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Spare part")
    _spare_df.columns = _spare_df.columns.str.strip()
    for _, _srow in _spare_df.iterrows():
        _sf = str(_srow.get("Factory", "")).strip().upper()
        _scat_full = str(_srow.get("MC_CAT", "")).strip().upper()  # e.g. "SINGLE-32", "DOUBLE-30"
        _sg_raw = str(_srow.get("Guage", "")).strip()
        # normalize gauge: ลบ "G"/"GAUGE" prefix (ตรงกับ _normalize_gauge)
        _sg = _sg_raw.upper().replace("GAUGE", "").replace("G", "").strip()
        _st = int(_srow.get("Total Spare", 0) or 0)
        if not (_sf and _scat_full and _sg):
            continue
        # เก็บ key เต็ม: ('PHET', 'SINGLE-32', '36')
        _spare_cylinder_map[(_sf, _scat_full, _sg)] = _st
        # เก็บ key ย่อ (ตัด gauge suffix ออก): ('PHET', 'SINGLE', '36')
        # เพราะ TYPE_1 ใน MasterMC/summary_mc อาจใช้แค่ SINGLE/DOUBLE ไม่มี -NN suffix
        _scat_base = _scat_full.split("-")[0].strip()  # "SINGLE-32" → "SINGLE"
        if _scat_base != _scat_full:
            _base_key = (_sf, _scat_base, _sg)
            # รวม spare (ถ้ามีหลาย gauge class ใน factory เดียวกัน ใช้ค่าสูงสุด ไม่ใช่ sum)
            _spare_cylinder_map[_base_key] = max(_spare_cylinder_map.get(_base_key, 0), _st)
    print(f"✅ Spare cylinder map: {len(_spare_cylinder_map)} entries")
    for _k, _v in _spare_cylinder_map.items():
        print(f"   Spare: {_k} = {_v}")
except Exception as _e_spare:
    print(f"⚠️ ไม่สามารถโหลด Spare part: {_e_spare}")

# Job sheet: cylinder changes ที่เกิดขึ้นจริงแล้วในแต่ละ week (จาก MasterMC)
# format: {YW: {group: count}}  group = 'OMNOI' / 'PHET_SINGLE' / 'PHET_DOUBLE'
# ชีท: Type | Year | 1 | 2 | ... | 53  → 1 แถว = 1 โรง × 1 ปี (หน้าเว็บ JobDoneEditor เขียนรูปนี้)
# ไม่มีคอลัมน์ Year / แถวที่ปีว่าง (ชีทรุ่นเก่า) = เลขเปล่า → ครั้งที่ใกล้วันนี้สุดเหมือนเดิม
#   (รุ่นเก่ามีกับดัก: ค่า W21/2026 จะกลายเป็น W21/2027 เมื่อเวลาผ่านไปเกินครึ่งปี)
_job_cylinder_done: dict = {}
try:
    from Calendar import to_int as _job_int
    _job_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Job ", header=None)
    _job_header = [str(_h).strip() for _h in _job_df.iloc[0]]
    _jyear_ci = next((_i for _i, _h in enumerate(_job_header)
                      if _h.upper() in ("YEAR", "ปี")), None)
    for _, _jrow in _job_df.iloc[1:].iterrows():
        _jgroup = str(_jrow.iloc[0]).strip()
        if not _jgroup or _jgroup == "nan":
            continue
        _jyear = _job_int(_jrow.iloc[_jyear_ci]) if _jyear_ci is not None else None
        for _jcol in range(1, len(_job_header)):
            if _jcol == _jyear_ci:
                continue
            _jweek_val = _job_int(_job_header[_jcol])
            _jcount = _job_int(_jrow.iloc[_jcol])
            if _jweek_val is None or _jcount is None:
                continue
            _jweek = _TL.key(_jweek_val, _jyear)
            if _jweek is None:
                continue
            _job_cylinder_done.setdefault(_jweek, {})[_jgroup] = _jcount
    print(f"✅ Job cylinder done: {_job_cylinder_done}")
except Exception as _e_job:
    print(f"⚠️ ไม่สามารถโหลด Job sheet: {_e_job}")

# Lock_MC: เครื่องที่ถูก lock ไม่ให้ใช้วางแผน per (week, mc_cat, gauge)
# Source: MasterMC.xlsx sheet "Lock_MC"
# - _LOCK_MC_MAP: หักจำนวนเครื่องออกจาก pool (กันคนอื่นใช้) — ทำงานเสมอไม่ว่าจะระบุ item หรือไม่
#   คอลัมน์ MC ระบุว่าเครื่องที่กันไว้อยู่กลุ่มไหน → หักเฉพาะ "พูลของกลุ่มนั้น"
#   (ดู _LOCK_MC_ANY / _LOCK_MC_POOL ที่สร้างหลังโหลด MC_POOL_MAP) ; เว้นว่าง = หักทุกพูล
# - _LOCK_ITEM_WARM: ถ้าระบุคอลัมน์ ITEM = "กันเครื่องให้ item นี้ + คงความอุ่น"
#   ทำให้พอ item กลับมาผลิตหลังเว้นช่วง ระบบถือว่าเครื่องยังอุ่น → ไม่ setup ใหม่
#   ⚠️ ต้องใช้กติกาเดียวกับ _LOCK_WARM ใน AVA_MC.py (ฝั่ง OLD) ไม่งั้น NEW/OLD ให้ผลคนละแบบ:
#     - จำนวนที่กัน = จำนวนเครื่องที่อุ่นได้ (กัน 3 → อุ่น 3, ตัวที่ 4 เป็นเครื่องใหม่ต้อง setup)
#     - คอลัมน์ MC: ระบุ = อุ่นเฉพาะกลุ่มเครื่องนั้น, เว้นว่าง = อุ่นทุกกลุ่มใน MC_CAT
_LOCK_MC_MAP: dict = {}
_LOCK_MC_BY_MC: dict = {}   # (week, cat_upper, gauge_norm, mc_upper|"") → จำนวนเครื่องที่กันไว้
_LOCK_ITEM_WARM: dict = {}  # key=(item_upper, cat_upper, gauge_norm, mc_upper|"") → {week: locked_count}
try:
    _lock_mc_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Lock_MC")
    _lock_mc_df.columns = _lock_mc_df.columns.str.strip()
    def _safe_int(v):
        # ทนค่าว่าง/NaN/ทศนิยม/สตริง — คืน None ถ้าแปลงไม่ได้ (กันทั้งชีทพังเพราะแถวเดียว)
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            s = str(v).strip()
            if s == "" or s.lower() == "nan":
                return None
            return int(float(s))
        except (ValueError, TypeError):
            return None
    for _, _lrow in _lock_mc_df.iterrows():
        _lweek = _safe_int(_lrow.get("Week"))
        _lcat = str(_lrow.get("MC_CAT", "")).strip().upper()
        _lg_raw = str(_lrow.get("Guage", "")).strip()
        _lg = _lg_raw.upper().replace("GAUGE", "").replace("G", "").strip()
        _lcount = _safe_int(_lrow.get("MC Lock")) or 0
        # Week To (ไม่บังคับ): กันเครื่องชุดเดิมยาวเป็นช่วง — ไม่มี/น้อยกว่า Week = สัปดาห์เดียว
        # Year / Year To (ไม่บังคับ): ใส่เมื่อช่วงข้ามปี → กางช่วงตาม timeline ปฏิทินจริง
        # ไม่ใส่ Year = ครั้งที่ใกล้วันนี้สุดของเลขสัปดาห์นั้น → คืนเป็น YW (ตรงกับ AVA_MC)
        _lweeks, _lwarn = _TL.span(
            _lweek, _safe_int(_lrow.get("Week To")),
            _safe_int(_lrow.get("Year")), _safe_int(_lrow.get("Year To")),
        )
        if _lwarn:
            print(f"⚠️ Lock_MC W{_lweek} ({_lcat}): {_lwarn}")
        # คอลัมน์ MC (ไม่บังคับ) = กลุ่มเครื่องที่กันไว้ — เก็บคู่กับยอด เพื่อหักเฉพาะพูลของกลุ่มนั้น
        _lmc = str(_lrow.get("MC", "") or "").strip().upper().replace("NAN", "")
        if _lweek is not None and _lcat and _lg and _lcount > 0:
            for _lw in _lweeks:
                _k = (_lw, _lcat, _lg)
                _LOCK_MC_MAP[_k] = _LOCK_MC_MAP.get(_k, 0) + _lcount
                _km = (_lw, _lcat, _lg, _lmc)
                _LOCK_MC_BY_MC[_km] = _LOCK_MC_BY_MC.get(_km, 0) + _lcount
        # ITEM (ไม่บังคับ): กันเครื่องให้ item นี้คงความอุ่นในสัปดาห์ที่ระบุ
        _litem = str(_lrow.get("ITEM", _lrow.get("ITEM_CODE", "")) or "").strip().upper()
        if _litem and _litem != "NAN" and _lweek is not None and _lcount > 0:
            _lk = (_litem, _lcat, _lg, _lmc)
            _lslot = _LOCK_ITEM_WARM.setdefault(_lk, {})
            # คีย์ซ้ำ (สัปดาห์เดียวกัน) → บวกรวม ให้ตรงกับที่หัก pool และที่การ์ดในเว็บเตือนไว้
            for _lw in _lweeks:
                _lslot[_lw] = _lslot.get(_lw, 0) + _lcount
    print(f"✅ Lock_MC map: {_LOCK_MC_MAP}")
    if _LOCK_ITEM_WARM:
        print(f"✅ Lock_MC item-warm: {_LOCK_ITEM_WARM}")
except Exception as _e_lock:
    print(f"⚠️ ไม่สามารถโหลด Lock_MC sheet: {_e_lock}")


# MC_Total: จำนวนเครื่องทั้งหมดของ (สัปดาห์ + MC_CAT + เกจ) ที่ user กำหนดเอง
# Source: MasterMC.xlsx sheet "MC_Total" — AVA_MC เขียนค่านี้ลง SUMMARY_MC_REMAIN ให้แล้ว
# ที่นี่ใช้เฉพาะ "เส้นทาง fallback" ของ _get_available_mc (สัปดาห์ที่ไม่มีแถวใน summary
# → เดิมรวม Total MC จาก MasterMC ซึ่งไม่รู้จัก override) ไม่งั้นสัปดาห์นั้นจะได้ยอดเก่า
_MC_TOTAL_OVERRIDE: dict = {}   # (week, cat_upper, gauge_norm) → total (รวมทุกโรงงาน)
# แถวที่ระบุคอลัมน์ MC = ยอดของ "พูล" เครื่องนั้นเท่านั้น (เช่น SKP ไม่ใช่ทั้ง SINGLE-32 26G)
# → เก็บแยก ไม่ปนกับยอดรวมของ CAT+เกจ ไม่งั้น fallback จะเอายอดพูลเดียวไปแทนทั้ง CAT
_MC_TOTAL_OVERRIDE_MC: dict = {}   # (week, cat_upper, gauge_norm, mc_upper) → total
try:
    _mt_df_plan = pd.read_excel(_MASTER_MC_PATH, sheet_name="MC_Total")
    _mt_df_plan.columns = _mt_df_plan.columns.str.strip()

    def _mt_int_plan(v):
        # ทนค่าว่าง/NaN/ทศนิยม/สตริง (นิยามซ้ำ ไม่พึ่ง _safe_int เผื่อบล็อก Lock_MC พังก่อน)
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            s = str(v).strip()
            if not s or s.lower() == "nan":
                return None
            return int(float(s))
        except (ValueError, TypeError):
            return None

    _mt_by_fac: dict = {}   # (week, cat, gauge) → ผลรวมของแถวที่ระบุโรงงาน
    for _, _mt_r in _mt_df_plan.iterrows():
        _mt_w = _mt_int_plan(_mt_r.get("Week"))
        _mt_tot = _mt_int_plan(_mt_r.get("Total MC"))
        _mt_c = str(_mt_r.get("MC_CAT", "") or "").strip().upper()
        _mt_g_raw = str(_mt_r.get("Guage", "") or "").strip()
        _mt_g = _mt_g_raw.upper().replace("GAUGE", "").replace("G", "").strip()
        if _mt_w is None or _mt_tot is None or _mt_tot < 0 or not _mt_c or not _mt_g:
            continue
        _mt_fac = str(_mt_r.get("Factory", "") or "").strip().upper().replace("NAN", "")
        _mt_mc = str(_mt_r.get("MC", "") or "").strip().upper().replace("NAN", "")
        # Week To (ไม่บังคับ): ระบุช่วงสัปดาห์ที่ใช้ยอดเดียวกัน — ไม่มี/น้อยกว่า Week = สัปดาห์เดียว
        # Year / Year To (ไม่บังคับ): ใส่เมื่อช่วงข้ามปี → กางช่วงตาม timeline ปฏิทินจริง
        # ไม่ใส่ Year = ครั้งที่ใกล้วันนี้สุดของเลขสัปดาห์นั้น → คืนเป็น YW (ตรงกับ AVA_MC)
        _mt_wks, _mt_warn_p = _TL.span(
            _mt_w, _mt_int_plan(_mt_r.get("Week To")),
            _mt_int_plan(_mt_r.get("Year")), _mt_int_plan(_mt_r.get("Year To")),
        )
        if _mt_warn_p:
            print(f"⚠️ MC_Total W{_mt_w} ({_mt_c}): {_mt_warn_p}")
        for _mt_wk in _mt_wks:
            _mt_k = (_mt_wk, _mt_c, _mt_g)
            if _mt_mc:
                # ระบุเครื่อง = ยอดของพูลนั้น → เก็บแยก (ใช้ตอน fallback ที่รู้ว่าเครื่องไหน)
                _MC_TOTAL_OVERRIDE_MC[(_mt_wk, _mt_c, _mt_g, _mt_mc)] = _mt_tot
            elif _mt_fac:
                # ระบุโรงงาน = ยอดของโรงงานนั้น → บวกรวมกันเป็นยอดของ CAT+เกจ
                _mt_by_fac[_mt_k] = _mt_by_fac.get(_mt_k, 0) + _mt_tot
            else:
                # ไม่ระบุ = ยอดรวมทั้ง CAT+เกจ อยู่แล้ว → ใช้ค่านี้เลย (ชนะแถวที่ระบุโรงงาน)
                _MC_TOTAL_OVERRIDE[_mt_k] = _mt_tot
    for _mt_k, _mt_v in _mt_by_fac.items():
        _MC_TOTAL_OVERRIDE.setdefault(_mt_k, _mt_v)
    if _MC_TOTAL_OVERRIDE:
        print(f"✅ MC_Total override: {_MC_TOTAL_OVERRIDE}")
    if _MC_TOTAL_OVERRIDE_MC:
        print(f"✅ MC_Total override (ระบุเครื่อง): {_MC_TOTAL_OVERRIDE_MC}")
except Exception as _e_mt_plan:
    print(f"⚠️ ไม่สามารถโหลด MC_Total sheet: {_e_mt_plan}")


def _lock_warm_weeks_plan(item, mc_group, gauge) -> dict:
    """{week: จำนวนเครื่องที่ lock ไว้ให้ item นี้} — รวม lock ที่ระบุ MC ตรงกับที่เว้นว่าง

    กติกาเดียวกับ _lock_warm_weeks() ใน AVA_MC.py (เว้นว่าง = ครอบคลุมทุกกลุ่มใน MC_CAT
    → ถ้ามีทั้งสองแบบใช้ค่าที่มากกว่า ไม่บวกกัน เพราะอ้างถึงเครื่องชุดเดียวกัน)
    """
    if not _LOCK_ITEM_WARM:
        return {}
    _item = str(item).strip().upper()
    _mc = str(mc_group).strip().upper()
    _cat = _mc_to_type1(_mc, gauge)
    _g = _normalize_gauge(gauge)
    out: dict = {}
    for _k in ((_item, _cat, _g, _mc), (_item, _cat, _g, "")):
        for _w, _c in _LOCK_ITEM_WARM.get(_k, {}).items():
            out[_w] = max(out.get(_w, 0), _c)
    return out


def _bridge_locks(key, current_idx):
    """เลื่อน last_production[key] มาที่สัปดาห์ที่ Lock กันเครื่องให้ item นี้ (ถ้ามี)
    เพื่อให้ทุกจุดเช็ค gap (setup) ถือว่าเครื่องยังอุ่น → ไม่ setup ใหม่ตอน item กลับมาผลิต

    เงื่อนไข: item ต้องเคยผลิตมาก่อน (key อยู่ใน last_production) จึงจะ "อุ่นต่อ" ได้
    (ครั้งแรกสุดยังต้อง setup ตามปกติ) และเลื่อนได้เฉพาะสัปดาห์ lock ที่อยู่ระหว่าง
    สัปดาห์ผลิตล่าสุด..สัปดาห์ปัจจุบัน — ไม่ล้ำอนาคต

    เดินเป็นทอด ๆ ทีละ lock โดยแต่ละช่วงห้ามเกิน SETUP_GAP_WEEK (เครื่องเย็นระหว่างทางไม่ได้)
    — เดิมกระโดดไป lock ที่ไกลสุดเลยโดยไม่เช็คว่าช่วงก่อนหน้าขาดตอนหรือเปล่า

    คืนค่า: จำนวนเครื่องที่อุ่นได้ (= จำนวนที่กันไว้ แคบสุดตลอดเส้นทาง) หรือ None ถ้าไม่มี
    lock มาเกี่ยว → ผู้เรียกต้องเอาไปจำกัดจำนวนเครื่อง carry-over ด้วย
    """
    if not _LOCK_ITEM_WARM or current_idx is None:
        return None
    if not isinstance(key, tuple) or not key or key not in last_production:
        return None
    weeks = _lock_warm_weeks_plan(
        key[0],
        key[1] if len(key) > 1 else "",
        key[2] if len(key) > 2 else "",
    )
    if not weeks:
        return None
    # แปลงสัปดาห์ → index ก่อน (last_production เก็บเป็น index ไม่ใช่เลขสัปดาห์)
    _by_idx: dict = {}
    for _w, _c in weeks.items():
        _wi = week_index(_w)
        if _wi is not None:
            _by_idx[_wi] = max(_by_idx.get(_wi, 0), _c)
    cur = last_production[key]
    start = cur
    cap = None
    for _wi in sorted(_by_idx):
        if cur < _wi < current_idx and (_wi - cur) <= SETUP_GAP_WEEK:
            cur = _wi
            cap = _by_idx[_wi] if cap is None else min(cap, _by_idx[_wi])
    if cur > start:
        last_production[key] = cur
        print(
            f"[LOCK WARM] {key[0]}: คงเครื่องอุ่นถึง idx {cur} (จาก Lock_MC, อุ่นได้ {cap} เครื่อง)"
            f" → ไม่ setup ใหม่ที่ idx {current_idx}"
        )
    return cap


def _hold_mc_at(key, week_idx) -> int:
    """เครื่องที่ "แถวกันเครื่อง" (Hold รอสี / Hold รอ target) ของคีย์นี้ถืออยู่ในสัปดาห์นั้น

    อ่านจากแถวจริงใน _PLANS_REF (ผ่าน _color_gap_rows ที่เก็บ reference ของแถวไว้) ไม่ใช่จาก
    memo _color_gap_held เพียว ๆ — ด่านซ่อมลดเครื่องของแถว Hold ได้ (เคสจริง: กันไว้ 7 เหลือ 3)
    ถ้าเชื่อ memo จะ carry เครื่องที่ไม่มีอยู่จริง
    """
    if (key, week_idx) not in _color_gap_held:
        return 0
    _row = _color_gap_rows.get((key, week_idx))
    if _row is None:
        return int(_color_gap_held.get((key, week_idx), 0) or 0)
    return int(pd.to_numeric(_row.get("ACTUAL_MC", 0), errors="coerce") or 0)


def _bridge_holds(key, current_idx):
    """เลื่อน last_production[key] ข้ามสัปดาห์ที่ "แถวกันเครื่อง" ถือเครื่องของ item นี้ไว้

    คู่แฝดของ _bridge_locks — ต่างกันแค่แหล่งของ "สัปดาห์ที่เครื่องถูกกันไว้" (Lock_MC vs แถว Hold)

    แถว Hold ถือเครื่องจริงตลอดสัปดาห์ที่เว้น (หักออกจากพูลแล้ว งานอื่นเอาไปใช้ไม่ได้) เครื่องจึง
    ยังอุ่น แต่ last_production เก็บเฉพาะ "สัปดาห์ที่ผลิตจริง" → โซ่ Hold ที่ยาวเกิน SETUP_GAP_WEEK
    ทำให้ทุกด่านที่วัด gap คิดว่าเครื่องถูกถอดไปแล้ว แล้วสั่ง setup ใหม่ทั้งที่กันเครื่องไว้ให้
    (เคสจริง 21 ก.ย. 2569 FD6DV24/3A0 RAP98 g22: ผลิต W44 → Hold W47–W50 กันไว้ 3 เครื่อง →
     W51 ห่าง 7 สัปดาห์ > 3 → carry ได้แค่ 2 แล้ว GRADUAL BOOST ตั้งเครื่องที่ 3 ใหม่ 5 วัน
     ขัดกับเจตนาของกติกาเองที่ว่า "กลับมาทอรอบหน้าโดยไม่ต้องตั้งเครื่องใหม่")

    เงื่อนไขเดียวกับ _bridge_locks: item ต้องเคยผลิตมาก่อน (ครั้งแรกสุดยังต้อง setup ตามปกติ),
    เดินเป็นทอด ๆ ทีละสัปดาห์ Hold โดยแต่ละทอดห้ามเกิน SETUP_GAP_WEEK (Hold ขาดตอนยาว =
    เครื่องหลุดคืนพูลจริงระหว่างทาง) และไม่ล้ำสัปดาห์ปัจจุบัน

    คืน cap = เครื่องที่ Hold ไว้แคบสุดตลอดเส้นทาง (None = ไม่มี Hold มาเกี่ยว) — ผู้เรียกต้อง
    เอาไปจำกัดจำนวนเครื่อง carry-over เหมือน _warm_cap ของ Lock ไม่งั้น carry เกินที่กันไว้ = เครื่องผี
    """
    if not (COLOR_GAP_ENABLED or CORE_HOLD_ENABLED) or current_idx is None:
        return None
    if not isinstance(key, tuple) or not key or key not in last_production:
        return None
    # คีย์ของ last_production มาจาก _resolve_carry_key ซึ่ง fallback ไปคีย์ที่มีอยู่จริงได้
    # (เกจอาจต่าง) → ลองคีย์ของแถว Hold ควบคู่ไปด้วย ไม่งั้นกติกาไม่ทำงานแบบเงียบ ๆ
    _keys = [key]
    _alt = _color_gap_key(
        key[0],
        key[1] if len(key) > 1 else "",
        key[2] if len(key) > 2 else "",
    )
    if _alt != key:
        _keys.append(_alt)
    cur = int(last_production[key])
    start = cur
    cap = None
    for _wi in range(cur + 1, int(current_idx)):
        _n = max(_hold_mc_at(_kk, _wi) for _kk in _keys)
        if _n <= 0:
            continue                      # สัปดาห์นี้ไม่มีแถวกันเครื่อง — ข้ามไปดูสัปดาห์ถัดไป
        if _wi - cur > SETUP_GAP_WEEK:
            break                         # เครื่องเย็นไปก่อนถึงสัปดาห์ Hold นี้แล้ว → โซ่ขาด
        cur = _wi
        cap = _n if cap is None else min(cap, _n)
    if cur > start:
        last_production[key] = cur
        print(
            f"[HOLD WARM] {key[0]}: แถวกันเครื่องถือเครื่องถึง idx {cur} (อุ่นได้ {cap} เครื่อง)"
            f" → ไม่ setup ใหม่ที่ idx {current_idx}"
        )
    return cap


def _same_week_lock_mc(key, prod_idx) -> int:
    """จำนวนเครื่องที่ Lock_MC กันไว้ให้ item ใน "สัปดาห์ที่ item ผลิตเอง" (idx = prod_idx)

    ต่างจาก _bridge_locks ที่ใช้ล็อกเชื่อมช่วงที่ item หยุดผลิต — ล็อกที่ทับสัปดาห์ผลิต
    เป็นเครื่องอีกชุดที่กันไว้เพิ่มจากเครื่องที่รันอยู่ (_LOCK_MC_MAP หักออกจาก pool
    ไปแล้วโดยไม่สนว่าสัปดาห์นั้นผลิตหรือไม่) → เครื่องชุดนี้ยังอุ่น ต้องบวกเข้ากับ
    carry-over ไม่ใช่ปล่อยหายแล้วให้สัปดาห์ถัดไป setup ใหม่

    กติกาเดียวกับคอลัมน์ _lock_same_week_mc ใน AVA_MC.py (ฝั่ง OLD)
    """
    if not _LOCK_ITEM_WARM or prod_idx is None:
        return 0
    if not isinstance(key, tuple) or not key:
        return 0
    weeks = _lock_warm_weeks_plan(
        key[0],
        key[1] if len(key) > 1 else "",
        key[2] if len(key) > 2 else "",
    )
    for _w, _c in weeks.items():
        if week_index(_w) == prod_idx:
            return int(_c or 0)
    return 0

# MC Special: per-(Factory, MC_CAT, MC, Gauge) → POLY/COTTON machine count
# Source: MasterMC.xlsx sheet "MC Special"
_PLANS_REF: list = []  # อ้างถึง list แถวแผนที่กำลังสร้างใน _run_planning_loop() (ใช้นับเครื่อง sub-pool จริง)
_MC_SPECIAL_PLAN: dict = {}  # key=(factory_upper, mc_cat_upper, mc_upper, gauge_str) → {"POLY": int, "COTTON": int}
_mc_special_weekly_usage: dict = {}  # (factory, mc_cat, gauge, week, "COTTON"/"POLY") → machines used by new plan
try:
    _mcs_plan_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="MC Special")
    _mcs_plan_df.columns = _mcs_plan_df.columns.str.strip()
    for _, _mcs_row in _mcs_plan_df.iterrows():
        _mcs_f = str(_mcs_row.get("Factory", "")).strip().upper()
        _mcs_cat = str(_mcs_row.get("MC_CAT", "")).strip().upper()
        _mcs_mc_raw = str(_mcs_row.get("MC", "")).strip().upper()
        _mcs_g = str(_mcs_row.get("Guage", "")).strip().upper().replace("GAUGE", "").replace("G", "").strip()
        # ตัด ".0" ท้าย ให้ตรงกับ gauge_str ที่ผ่าน _normalize_gauge (float 36.0 → "36")
        if _mcs_g.endswith(".0"):
            _mcs_g = _mcs_g[:-2]
        if not _mcs_f or not _mcs_cat or not _mcs_g:
            continue
        _mcs_poly_raw = _mcs_row.get("POLY")
        _mcs_cotton_raw = _mcs_row.get("COTTON")
        _mcs_poly = int(float(_mcs_poly_raw)) if pd.notna(_mcs_poly_raw) and str(_mcs_poly_raw).strip() not in ("", "-", "NAN") else 0
        _mcs_cotton = int(float(_mcs_cotton_raw)) if pd.notna(_mcs_cotton_raw) and str(_mcs_cotton_raw).strip() not in ("", "-", "NAN") else 0
        if _mcs_poly <= 0 and _mcs_cotton <= 0:
            continue
        # MC column อาจมีหลายค่าคั่นด้วย comma เช่น "SKPTA,SKPLE"
        _mcs_mc_parts = [p.strip() for p in _mcs_mc_raw.split(",") if p.strip() and p.strip() not in ("NAN", "")]
        if not _mcs_mc_parts:
            _mcs_mc_parts = [""]  # general match (ไม่ระบุ MC เฉพาะ)
        for _mcs_mc in _mcs_mc_parts:
            _MC_SPECIAL_PLAN[(_mcs_f, _mcs_cat, _mcs_mc, _mcs_g)] = {"POLY": _mcs_poly, "COTTON": _mcs_cotton}
    print(f"✅ MC Special (Plan): {len(_MC_SPECIAL_PLAN)} entries loaded")
    for _k, _v in _MC_SPECIAL_PLAN.items():
        print(f"   {_k} → {_v}")
except Exception as _e_mcs:
    print(f"⚠️ ไม่สามารถโหลด MC Special: {_e_mcs}")
    _MC_SPECIAL_PLAN = {}

# Booking ที่กินเครื่อง POLY/COTTON sub-pool ไปแล้ว ต่อ (cat, gauge, week) — อ่านจาก DETAIL
# ของ booking_final (POOL_TYPE ลงท้าย :POLY/:COTTON) เพื่อหักจาก capacity ก่อนวางงานใหม่
# ไม่งั้นจะวางงาน POLY/COTTON ทับเครื่องที่ booking จองเต็มแล้ว (over-plan)
_MC_SPECIAL_BOOKING_USED: dict = {}  # (cat_upper, gauge_str, week, "POLY"/"COTTON") → machines
try:
    if "POOL_TYPE" in detail_mc.columns:
        def _bku_gauge(g):
            try:
                return str(int(float(g)))
            except (TypeError, ValueError):
                return str(g).strip()
        for _, _bk_r in detail_mc.iterrows():
            _bk_pt = str(_bk_r.get("POOL_TYPE", "")).strip()
            if _bk_pt.endswith(":POLY"):
                _bk_type = "POLY"
            elif _bk_pt.endswith(":COTTON"):
                _bk_type = "COTTON"
            else:
                continue
            _bk_cat = str(_bk_r.get("MC_CAT", "")).strip().upper()
            _bk_g = _bku_gauge(_bk_r.get("GUAGE", ""))
            try:
                _bk_wk = int(_bk_r.get("WEEK"))
            except (TypeError, ValueError):
                continue
            _bk_key = (_bk_cat, _bk_g, _bk_wk, _bk_type)
            _MC_SPECIAL_BOOKING_USED[_bk_key] = (
                _MC_SPECIAL_BOOKING_USED.get(_bk_key, 0) + int(_bk_r.get("MC_USE_CEIL", 0) or 0)
            )
    print(f"✅ MC Special booking used: {len(_MC_SPECIAL_BOOKING_USED)} (cat,gauge,week,type)")
except Exception as _e_bku:
    print(f"⚠️ ไม่สามารถคำนวณ MC Special booking used: {_e_bku}")
    _MC_SPECIAL_BOOKING_USED = {}

# TYPE_SPECIAL quota (BABY FRENCH / SINGLE JACQUARD / TWILL)
def _is_description_special_type_plan(desc: str, keywords: list) -> bool:
    """FRENCH TERRY → False เสมอ; ถ้า match keyword ใดๆ → True"""
    d = str(desc).strip().upper()
    if "FRENCH TERRY" in d:
        return False
    return any(kw.lstrip("$") in d for kw in keywords)

_TYPE_DESC_RULES_PLAN: dict = {}  # (factory_upper, type_upper) → {max_mc, keywords, mc_cat}
try:
    _tdr_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="MC Special")
    _tdr_df.columns = _tdr_df.columns.str.strip()
    _kw_col_p = "Unnamed: 8" if "Unnamed: 8" in _tdr_df.columns else None
    _cur_key_p, _cur_kws_p, _cur_max_p, _cur_cat_p = None, [], 0, ""
    for _, _r in _tdr_df.iterrows():
        _fac2 = str(_r.get("Factory", "")).strip().upper()
        _typ2 = str(_r.get("Type",    "")).strip().upper()
        _cat2 = str(_r.get("MC_CAT",  "")).strip().upper()
        if _cat2 == "NAN": _cat2 = ""
        _desc2 = _r.get("DESCRIPTION")
        _kw2   = str(_r.get(_kw_col_p, "") if _kw_col_p else "").strip().upper()
        _has_g2 = pd.notna(_r.get("Guage")) and str(_r.get("Guage", "")).strip() not in ("", "-", "NAN")
        if _fac2 not in ("", "NAN") and _typ2 not in ("", "NAN") and not _has_g2:
            if _cur_key_p and _cur_kws_p:
                _TYPE_DESC_RULES_PLAN[_cur_key_p] = {"max_mc": _cur_max_p, "keywords": _cur_kws_p[:], "mc_cat": _cur_cat_p}
            _cur_key_p = (_fac2, _typ2)
            _cur_cat_p = _cat2
            _cur_max_p = int(_desc2) if pd.notna(_desc2) and str(_desc2).strip() not in ("", "-") else 0
            _cur_kws_p = [_kw2] if _kw2 and _kw2 != "NAN" else []
        elif _fac2 in ("", "NAN") and _typ2 in ("", "NAN") and _kw2 not in ("", "NAN"):
            if _cur_key_p is not None:
                _cur_kws_p.append(_kw2)
    if _cur_key_p and _cur_kws_p:
        _TYPE_DESC_RULES_PLAN[_cur_key_p] = {"max_mc": _cur_max_p, "keywords": _cur_kws_p[:], "mc_cat": _cur_cat_p}
    print(f"✅ TYPE_DESC_RULES (Plan): {len(_TYPE_DESC_RULES_PLAN)} entries")
except Exception as _e_tdrp:
    print(f"⚠️ ไม่สามารถโหลด TYPE_DESC_RULES (Plan): {_e_tdrp}")
    _TYPE_DESC_RULES_PLAN = {}

# (MC_upper, gauge_str) → Type raw (SINGLE / DOUBLE / ...) จาก master_mc
_mc_to_type_raw_plan: dict = {
    (str(_r.get("MC", "")).strip().upper(), str(_r.get("Guage", "")).strip()): str(_r.get("Type", "")).strip().upper()
    for _, _r in master_mc.iterrows()
    if str(_r.get("MC", "")).strip()
}

# item_code → description จาก detail_mc (booking)
_item_desc_map_plan: dict = {}
if "ITEM_CODE" in detail_mc.columns and "DESCRIPTION" in detail_mc.columns:
    for _ic, _dsc in zip(detail_mc["ITEM_CODE"].astype(str).str.strip().str.upper(),
                         detail_mc["DESCRIPTION"].astype(str).str.strip()):
        if _ic and _dsc.upper() not in ("", "NAN"):
            _item_desc_map_plan[_ic] = _dsc

# booking usage ที่ consume TYPE_SPECIAL quota ไปแล้ว: (factory, type, week) → mc_used
_type_special_booking_usage: dict = {}
if "DESC_POOL_TYPE" in detail_mc.columns and "MC_USE_CEIL" in detail_mc.columns:
    _ts_bk_rows = detail_mc[
        detail_mc["DESC_POOL_TYPE"].astype(str).str.strip().str.endswith(":TYPE_SPECIAL", na=False)
    ]
    for _, _br in _ts_bk_rows.iterrows():
        _bp = str(_br["DESC_POOL_TYPE"]).strip()      # "PHET|SINGLE:TYPE_SPECIAL"
        _bparts = _bp.replace(":TYPE_SPECIAL", "").split("|")
        _bf = _bparts[0] if len(_bparts) >= 2 else ""
        _bt = _bparts[1] if len(_bparts) >= 2 else _bparts[0]
        _bw = int(_br.get("WEEK", 0) or 0)
        _bu = int(_br.get("MC_USE_CEIL", 0) or 0)
        if _bw and _bu > 0:
            _type_special_booking_usage[(_bf, _bt, _bw)] = (
                _type_special_booking_usage.get((_bf, _bt, _bw), 0) + _bu
            )

# new-plan usage ต่อ (factory, type, week)
_type_special_weekly_usage: dict = {}

# Cylinder change tracking
# ปิดการเปลี่ยน Cylinder อัตโนมัติ: Planning ไม่ยืมเครื่องข้ามเกจให้เอง
# เพราะการเปลี่ยน Cylinder เป็นการตัดสินใจของคน (มีต้นทุน/เวลา) — จะให้ AI เสนอแนะแยกต่างหาก
# ผลข้างเคียงที่ตั้งใจ: cylinder_adjustments ว่างเสมอ → เครื่องว่างที่ carry gate เห็น
# เป็นเครื่องจริงในพูลล้วนๆ ไม่มีเครื่องที่ "จะได้ถ้าเปลี่ยน Cylinder" ปนมา
# (Job sheet = Cylinder ที่เปลี่ยนจริงไปแล้ว ยังโหลดตามปกติ)
CYLINDER_CHANGE_ENABLED = plan_config.get("KP_CYLINDER_CHANGE")
CYLINDER_CHANGE_LIMIT = plan_config.get("KP_CYLINDER_CHANGE_LIMIT")
cylinder_change_count: dict = {}
cylinder_adjustments: dict = {}
_cylinder_change_for_item: dict = {}
_cylinder_change_start_map: dict = {}  # (factory, mc_cat, tgt_gauge) → (initiation_week, src_g, tgt_g)
_cylinder_change_mc_count: dict = {}  # (factory, mc_cat, tgt_gauge) → จำนวนเครื่องที่เปลี่ยนแล้วสะสม (ห้ามเกิน Total Spare)
_cylinder_change_done: set = set()  # (factory, mc_cat, src_gauge, tgt_gauge, week) — tracking only (not used for blocking)
_carry_cyl_pending: dict = {}  # {(week_int, item_code, mc_cat, tgt_gauge) → count} — per-item, no pool double-count


def _get_spare_info(factory: str, mc_cat: str, tgt_g: str):
    """คืน (spare_count, matched_key) — รองรับ exact match และ prefix match (SINGLE → SINGLE-32)
    เพราะ MasterMC Type_1 อาจเป็น 'SINGLE' แต่ Spare sheet ใช้ 'SINGLE-32', 'SINGLE-36' เป็นต้น
    ถ้าตรง MC_CAT เดียวกัน (prefix) ก็เปลี่ยนได้ — ใช้ key ที่มี Total Spare สูงสุด
    """
    # exact match ก่อน
    exact_val = _spare_cylinder_map.get((factory, mc_cat, tgt_g), -1)
    if exact_val >= 0:
        return exact_val, (factory, mc_cat, tgt_g)
    # prefix match: 'SINGLE' ตรงกับ 'SINGLE-32', 'SINGLE-36' ฯลฯ
    best_val = 0
    best_key = None
    prefix = mc_cat.split("-")[0]  # 'SINGLE-32' → 'SINGLE', 'SINGLE' → 'SINGLE'
    for _k, _v in _spare_cylinder_map.items():
        if _k[0] == factory and _k[2] == tgt_g:
            _k_prefix = _k[1].split("-")[0]
            if _k_prefix == prefix and _v > best_val:
                best_val = _v
                best_key = _k
    return best_val, best_key
_current_order_rdd_idx = None  # rdd_idx ของ order ที่กำลัง plan อยู่ (ใช้ตรวจ JIT timing)



def _mc_to_type1(mc_group: str, gauge=None) -> str:

    """แปลง MC_GROUP + gauge → Type_1 จาก MasterMC"""

    mc_u = str(mc_group).strip().upper() if mc_group else ""

    g_u = _normalize_gauge(gauge)

    t1 = _MC_TYPE1_MAP.get((mc_u, g_u))

    if t1 is None and g_u:

        t1 = _MC_TYPE1_MAP.get((mc_u, ""))

    return t1 if t1 else mc_u


def _mc_to_factory(mc_group: str, gauge=None) -> str:
    """แปลง MC_GROUP + gauge → Factory จาก MasterMC"""
    mc_u = str(mc_group).strip().upper() if mc_group else ""
    g_u = _normalize_gauge(gauge)
    f = _mc_factory_map.get((mc_u, g_u))
    if f is None and g_u:
        f = _mc_factory_map.get((mc_u, ""))
    return f if f else ""


def _cylinder_quota_group(factory: str, mc_cat: str):
    """คืน quota group: 'OMNOI', 'PHET_DOUBLE', 'PHET_SINGLE' หรือ None"""
    f = str(factory).strip().upper()
    cat = str(mc_cat).strip().upper()
    if f == "OMNOI":
        return "OMNOI"
    elif f == "PHET":
        if "DOUBLE" in cat:
            return "PHET_DOUBLE"
        else:
            return "PHET_SINGLE"
    return None


def _find_source_gauge_for_cylinder(factory: str, mc_cat: str, target_gauge: str, week, mc_group: str = "", debug: bool = False) -> str:
    """หา gauge ที่จะเปลี่ยน cylinder มาจาก: AVA > 0 ใน week นั้น และมี TOTAL_MC มากที่สุด
    พูลแยก (SKP vs SKPTA/SKPLE): พิจารณาเฉพาะเครื่องพูลเดียวกับ mc_group เท่านั้น (ไม่มองรวมพูลอื่น)"""
    f_upper = str(factory).strip().upper()
    cat_upper = str(mc_cat).strip().upper()
    if "FACTORY" not in summary_mc.columns:
        if debug: print(f"[SRC GAUGE] ไม่มีคอลัมน์ FACTORY ใน summary_mc")
        return None

    _mc_u = str(mc_group).strip().upper()
    _has_pool_col = "MC_POOL" in summary_mc.columns

    def _split_pool_of(_g):
        # คืน pool label ที่ต้องกรอง ถ้า (cat,g) แยกพูลและรู้ mc_group; ไม่งั้น None (พฤติกรรมเดิม)
        if _mc_u and _has_pool_col and (cat_upper, _g) in _SPLIT_CG:
            return _mc_to_pool_label(_mc_u, _g)
        return None

    def _used_at(_wk, _g):
        _pl = _split_pool_of(_g)
        if _pl:
            return weekly_new_plan_pool_usage.get(_wk, {}).get(_pl, 0)
        return weekly_new_plan_usage.get(_wk, {}).get((cat_upper, _g), 0)
    candidates = summary_mc[
        (summary_mc["FACTORY"].astype(str).str.strip().str.upper() == f_upper)
        & (summary_mc["TYPE_1"] == cat_upper)
        & week_mask(summary_mc, week)
        & (summary_mc["GUAGE"].apply(_normalize_gauge) != target_gauge)
    ]
    # ห้ามใช้ SINGLE-32 Gauge 20 เป็น source cylinder change ไปให้ gauge อื่นเด็ดขาด
    if "SINGLE" in cat_upper and "32" in cat_upper:
        candidates = candidates[candidates["GUAGE"].apply(_normalize_gauge) != "20"]
    if debug:
        all_weeks = summary_mc[
            (summary_mc["FACTORY"].astype(str).str.strip().str.upper() == f_upper)
            & (summary_mc["TYPE_1"] == cat_upper)
        ]["WEEK"].unique().tolist()
        print(f"[SRC GAUGE] {f_upper}/{cat_upper} W{week}: candidates={len(candidates)}, available weeks in summary_mc={sorted(all_weeks)}")
    # pre-build future weeks list (trigger_week+1 onwards) for forward-check
    _fw_list = sorted(int(w) for w in summary_mc["WEEK"].unique() if int(w) >= int(week) + 1)
    # pre-index summary_mc rows per (factory, mc_cat, week, gauge) for fast lookup
    _src_summary_idx = {}
    for _, _sr in summary_mc[
        (summary_mc["FACTORY"].astype(str).str.strip().str.upper() == f_upper)
        & (summary_mc["TYPE_1"] == cat_upper)
    ].iterrows():
        _sw = int(_sr["WEEK"])
        _sg = _normalize_gauge(_sr["GUAGE"])
        _wp = _split_pool_of(_sg)   # พูลแยก: เก็บเฉพาะพูลเดียวกับ mc_group
        if _wp and str(_sr.get("MC_POOL", "")).strip() != _wp:
            continue
        _src_summary_idx[(_sw, _sg)] = int(_sr.get("TOTAL_MC_REMAIN", 0) or 0)

    best_gauge = None
    best_total_mc = -1
    for _, row in candidates.iterrows():
        g = _normalize_gauge(row["GUAGE"])
        # พูลแยก: ข้ามแถวพูลอื่น (เช่น ถ้า mc_group=SKPTA ห้ามเอาเครื่องพูล SKP มาเปลี่ยน cylinder)
        _want_pool = _split_pool_of(g)
        if _want_pool and str(row.get("MC_POOL", "")).strip() != _want_pool:
            continue
        base = int(row.get("TOTAL_MC_REMAIN", 0) or 0)
        used = _used_at(week, g)
        adj = cylinder_adjustments.get((week, f_upper, cat_upper, g), 0)
        # 🔧 FIX: ใช้แค่ physical machines + negative adj เท่านั้น
        # ไม่นับ positive adj (เครื่องที่ถูก cyl change เข้ามา) เป็น source
        # เพราะเครื่อง "virtual" เหล่านั้นไม่ควรถูกเปลี่ยน cylinder ซ้ำอีกครั้ง
        ava = base - used + min(0, adj)
        if debug: print(f"[SRC GAUGE]   G{g}: TOTAL_MC_REMAIN={base}, used={used}, adj={adj}, ava_physical={ava}")
        if g == "26":  # DEBUG: trace G26 source gauge selection
            _mc_grp_dbg = row.get("MC_GROUP", row.get("MC_CAT", row.get("GROUP", "?")))
            _total_mc_dbg = row.get("TOTAL_MC", "?")
            print(f"[DEBUG G26 SRC] W{week} {f_upper}/{cat_upper} G26 MC_GROUP={_mc_grp_dbg} TOTAL_MC={_total_mc_dbg}: base={base}, used={used}, adj={adj}, ava_physical={ava}")
        if ava > 0:
            # 🔧 FIX: Pool rows มี TOTAL_MC = pool total (ทุก gauge รวมกัน) ไม่ใช่ G gauge เฉพาะ
            # ตรวจ non-pool row (FACTORY="") เพื่อดูจำนวนเครื่อง G{g} จริงๆ
            _nonpool_g_rows = summary_mc[
                (summary_mc["FACTORY"].astype(str).str.strip() == "")
                & (summary_mc["TYPE_1"] == cat_upper)
                & week_mask(summary_mc, week)
                & (summary_mc["GUAGE"].apply(_normalize_gauge) == g)
            ]
            if not _nonpool_g_rows.empty:
                _np_total = int(_nonpool_g_rows["TOTAL_MC"].sum())
                _np_remain = int(_nonpool_g_rows["TOTAL_MC_REMAIN"].sum())
                _np_ava = _np_remain - used + min(0, adj)
                if debug or g == "26":
                    print(f"[SRC GAUGE PER-GAUGE]   G{g}: np_total={_np_total}, np_remain={_np_remain}, used={used}, np_ava={_np_ava}")
                if _np_total > 0 and _np_ava <= 0:
                    # เครื่อง G{g} จริงใช้หมดแล้ว — ห้ามเปลี่ยน cylinder จาก gauge นี้
                    if debug: print(f"[SRC GAUGE]   G{g}: BLOCKED (per-gauge physical machines exhausted: np_remain={_np_remain}, np_ava={_np_ava})")
                    continue
            # ตรวจ future weeks: การหัก 1 เครื่องจาก src gauge จะทำให้ week ไหนติดลบไหม
            _future_ok = True
            for _fw in _fw_list:
                _base_fw = _src_summary_idx.get((_fw, g), 0)
                _used_fw = _used_at(_fw, g)
                _adj_fw = cylinder_adjustments.get((_fw, f_upper, cat_upper, g), 0)
                # ใช้ min(0, adj) เช่นเดียวกัน — ไม่นับ incoming virtual machines
                _ava_fw = _base_fw - _used_fw + min(0, _adj_fw) - 1  # -1 สำหรับ cylinder change นี้
                if _ava_fw < 0:
                    if debug: print(f"[SRC GAUGE]   G{g}: BLOCKED — W{_fw} ava_after={_ava_fw} (base={_base_fw}, used={_used_fw}, adj={_adj_fw}) → ติดลบ")
                    _future_ok = False
                    break
            if not _future_ok:
                continue
            total_mc = int(row.get("TOTAL_MC", 0) or 0)
            if total_mc > best_total_mc:
                best_total_mc = total_mc
                best_gauge = g
    if debug: print(f"[SRC GAUGE] result: src_gauge={best_gauge}")
    return best_gauge


def _try_cylinder_change(mc_cat: str, factory: str, target_gauge: str, week, item_code: str = "", mc_group: str = "", debug: bool = False, jit_override: bool = False) -> bool:
    """พยายาม cylinder change (gauge เดิม → target_gauge ภายใน MC_CAT เดิม) คืน True ถ้าสำเร็จ
    jit_override=True: ข้าม JIT 2-week window (ใช้เฉพาะ carryover path ที่ pool หมดและต้องการเครื่องแน่นอน)
    """
    _dbg = debug or bool(item_code)
    # ปิดทั้งระบบ: ไม่เปลี่ยน Cylinder อัตโนมัติในแผน (ดู CYLINDER_CHANGE_ENABLED)
    if not CYLINDER_CHANGE_ENABLED:
        if _dbg: print(f"[CYL OFF] {item_code} W{week}: cylinder change ปิดอยู่ → ไม่เปลี่ยน Cylinder ในแผน")
        return False
    # บล็อค late orders (rdd_idx=None) — ไม่ใช้ spare cylinder กับ order ที่เลย deadline ไปแล้ว
    if _current_order_rdd_idx is None:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: rdd_idx=None (late/no-target order)")
        return False
    _pw_idx = week_index(week)
    if _pw_idx is None:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: week ไม่อยู่ใน calendar")
        return False
    if not jit_override:
        # JIT check: trigger เฉพาะเมื่อ plan_week-1 อยู่ภายใน 2 week ของ target (rdd_idx)
        if _pw_idx + 2 < _current_order_rdd_idx:
            if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: JIT fail (trigger_idx={_pw_idx}+2={_pw_idx+2} < rdd={_current_order_rdd_idx})")
            return False
    # ห้ามเปลี่ยน cylinder เร็วกว่า today+2 weeks (trigger_week ต้อง >= TODAY_IDX+2)
    if _pw_idx < TODAY_IDX + FREEZE_WEEKS:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: too early (trigger_idx={_pw_idx} < พื้น={TODAY_IDX + FREEZE_WEEKS})")
        return False
    f_upper = str(factory).strip().upper()
    cat_upper = str(mc_cat).strip().upper()
    tgt_g = _normalize_gauge(target_gauge)
    if not f_upper or not cat_upper or not tgt_g:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: Factory หรือ MC_CAT หรือ Gauge ว่าง (Factory={f_upper}, MC_CAT={cat_upper}, Gauge={tgt_g})")
        return False
    # ห้าม cylinder change ทุกทิศทางที่เกี่ยวกับ SINGLE-32 Gauge 20 เด็ดขาด
    # (ห้ามเปลี่ยน G20→อื่น และห้ามเปลี่ยน อื่น→G20)
    if "SINGLE" in cat_upper and "32" in cat_upper and tgt_g == "20":
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: SINGLE-32 G20 ห้าม cylinder change ทุกกรณี (target=G20)")
        return False
    if _dbg: print(f"[CYL CHECK] {item_code} W{week}: Factory={f_upper}, MC_CAT={cat_upper}, Gauge={tgt_g} → ค้นหา spare ใน MasterMC[Spare part]...")
    spare, _spare_key = _get_spare_info(f_upper, cat_upper, tgt_g)
    if spare <= 0:
        if _dbg:
            _avail_keys = [k for k in _spare_cylinder_map if k[0] == f_upper]
            print(f"[CYL BLOCKED] {item_code} W{week}: ไม่มี spare สำหรับ Factory={f_upper}, MC_CAT={cat_upper}, Gauge={tgt_g}")
            print(f"[CYL BLOCKED]   spare ที่มีสำหรับ Factory={f_upper}: {_avail_keys if _avail_keys else 'ไม่มีเลย'}")
        return False
    # ใช้ key จริงที่ match (อาจเป็น 'SINGLE-32' แม้ cat_upper='SINGLE')
    _matched_mc_cat = _spare_key[1] if _spare_key else cat_upper
    # ตรวจ Total Spare limit: จำนวนที่เปลี่ยนแล้วสะสมต้องไม่เกิน Total Spare
    _used_so_far = _cylinder_change_mc_count.get((f_upper, _matched_mc_cat, tgt_g), 0)
    if _used_so_far >= spare:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: ใช้ spare ครบแล้ว {_used_so_far}/{spare} สำหรับ {f_upper}/{_matched_mc_cat}/G{tgt_g}")
        return False
    if _dbg: print(f"[CYL CHECK] {item_code} W{week}: spare match → {_spare_key} total={spare}, used={_used_so_far}, remaining={spare-_used_so_far}")
    group = _cylinder_quota_group(f_upper, cat_upper)
    if group is None:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: factory/cat ไม่อยู่ใน quota group ({f_upper}, {cat_upper})")
        return False
    current_count = cylinder_change_count.get(week, {}).get(group, 0)
    if current_count >= CYLINDER_CHANGE_LIMIT:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: quota เต็ม {group}={current_count}/{CYLINDER_CHANGE_LIMIT}")
        return False
    # ใช้ _matched_mc_cat ใน find source เพื่อ filter summary_mc ได้ถูกต้อง
    _src_cat = _matched_mc_cat
    src_g = _find_source_gauge_for_cylinder(f_upper, _src_cat, tgt_g, week, mc_group=mc_group, debug=_dbg)
    if src_g is None:
        if _dbg: print(f"[CYL BLOCKED] {item_code} W{week}: ไม่มี source gauge ที่ว่างสำหรับ {f_upper} {_src_cat} G{tgt_g} ใน W{week}")
        return False
    _done_key = (f_upper, cat_upper, src_g, tgt_g, week)
    if week not in cylinder_change_count:
        cylinder_change_count[week] = {}
    cylinder_change_count[week][group] = current_count + 1
    # Apply adjustment week+1 เป็นต้นไปจนสิ้นสุด horizon (cylinder เปลี่ยนถาวรจนกว่าจะเปลี่ยนอีกครั้ง)
    # ใช้ _src_cat (matched MC_CAT จาก spare sheet) เพื่อ track adjustment ให้ถูก pool
    _future_weeks = sorted(int(w) for w in summary_mc["WEEK"].unique() if int(w) >= int(week) + 1)
    for _fw in _future_weeks:
        _src_key = (_fw, f_upper, _src_cat, src_g)
        _tgt_key = (_fw, f_upper, _src_cat, tgt_g)
        cylinder_adjustments[_src_key] = cylinder_adjustments.get(_src_key, 0) - 1
        cylinder_adjustments[_tgt_key] = cylinder_adjustments.get(_tgt_key, 0) + 1
    _cylinder_change_done.add(_done_key)
    # บันทึก start week ครั้งแรกเท่านั้น
    if (f_upper, _matched_mc_cat, tgt_g) not in _cylinder_change_start_map:
        _cylinder_change_start_map[(f_upper, _matched_mc_cat, tgt_g)] = (int(week), src_g, tgt_g)
    # นับจำนวนเครื่องที่เปลี่ยนสะสม — ใช้ _matched_mc_cat เพื่อ enforce Total Spare ถูก key
    _cylinder_change_mc_count[(f_upper, _matched_mc_cat, tgt_g)] = _cylinder_change_mc_count.get((f_upper, _matched_mc_cat, tgt_g), 0) + 1
    item_key = (week, str(item_code).strip().upper(), str(mc_group).strip().upper())
    _cylinder_change_for_item[item_key] = (src_g, tgt_g)
    print(f"🔄 CYLINDER CHANGE W{week}: {f_upper} {_matched_mc_cat} G{src_g}→G{tgt_g} [{group} {current_count+1}/{CYLINDER_CHANGE_LIMIT}] spare used {_used_so_far+1}/{spare} ({item_code}) → apply weeks {_future_weeks[:3]}...")
    return True


def _apply_approved_cylinder_changes(adjustments: dict) -> int:
    """seed เครื่องที่ user ติ๊ก Change Cylinder ไว้จากหน้าเว็บ (data_plan/cylinder_approved.json)

    ต่างจาก _try_cylinder_change ตรงที่นี่ไม่ใช่ "ขอเปลี่ยนระหว่างวางแผน" แต่เป็นสถานะเครื่องที่
    คนตัดสินใจย้ายไปแล้วก่อนรอบนี้จะเริ่ม → ไม่ผ่านกฎ JIT/โควตา และไม่กิน quota ของสัปดาห์
    (ติ๊กครั้งเดียวมีผลทุกรอบ ทั้ง Planning ปกติและรอบ Replan ที่รันไฟล์นี้เหมือนกัน)

    เดลต้ามีผลตั้งแต่สัปดาห์ที่ "เครื่องพร้อมใช้" (ready_week) เป็นต้นไปจนจบ horizon —
Cylinder ที่เปลี่ยนแล้วอยู่ถาวรจนกว่าจะเปลี่ยนอีกครั้ง (เหมือน _try_cylinder_change:1154)
    คืนจำนวนรายการที่ seed สำเร็จ
    """
    import json as _json
    if not APPROVED_CYL_FILE.exists():
        return 0
    try:
        _data = _json.loads(APPROVED_CYL_FILE.read_text(encoding="utf-8"))
        _rows = _data.get("approvals", []) if isinstance(_data, dict) else (_data or [])
    except Exception as e:
        print(f"⚠️ อ่าน {APPROVED_CYL_FILE.name} ไม่ได้: {e} → ไม่ได้ใช้ Change Cylinder ที่อนุมัติไว้")
        return 0
    if not _rows:
        return 0
    try:
        _all_weeks = sorted(int(w) for w in summary_mc["WEEK"].unique())
    except Exception as e:
        print(f"⚠️ ไม่มีตารางสัปดาห์ให้ apply Change Cylinder ที่อนุมัติไว้: {e}")
        return 0

    _applied = 0
    for _r in _rows:
        try:
            _n = int(float(_r.get("machines", 0) or 0))
        except (TypeError, ValueError):
            _n = 0
        if _n <= 0:
            continue
        _fac = str(_r.get("factory", "")).strip().upper()
        _cat = str(_r.get("cat", "")).strip().upper()
        _src = _normalize_gauge(_r.get("src_gauge"))
        _tgt = _normalize_gauge(_r.get("tgt_gauge"))
        # ready_week (+ ready_year ถ้ามี) → YW ; ไม่มีปี = ครั้งที่ใกล้วันนี้สุด
        _ready = _TL.key(_r.get("ready_week"), _r.get("ready_year"))
        if not _fac or not _cat or not _src or not _tgt or _ready is None or _src == _tgt:
            print(f"⚠️ ข้ามรายการ Change Cylinder ที่ข้อมูลไม่ครบ: {_r.get('id', _r)}")
            continue
        # สัปดาห์ที่ยังไม่ผ่านไปแล้วเท่านั้น (ของเก่าที่ทำไปแล้วสะท้อนอยู่ใน booking/MasterMC อยู่แล้ว)
        _weeks = [w for w in _all_weeks if w >= _ready]
        if not _weeks:
            continue
        for _w in _weeks:
            _sk = (_w, _fac, _cat, _src)
            _tk = (_w, _fac, _cat, _tgt)
            adjustments[_sk] = adjustments.get(_sk, 0) - _n
            adjustments[_tk] = adjustments.get(_tk, 0) + _n
        _applied += 1
        print(f"🔧 CHANGE CYLINDER (user อนุมัติ): {_fac} {_cat} G{_src}→G{_tgt} {_n} เครื่อง "
              f"ตั้งแต่ {yw_label(_ready)} (สั่งเปลี่ยน W{_r.get('set_week', '?')}) "
              f"[{_r.get('by', '')} {_r.get('at', '')}]")
    if _applied:
        print(f"✅ ใช้ Change Cylinder ที่ user ติ๊กไว้ {_applied} รายการ จาก {APPROVED_CYL_FILE.name}")
    return _applied


# รายชื่อลูกค้า core — ⚠ ไม่ใช่ตัวตัดสินว่าอะไรเป็น CORE ITEM อีกแล้ว
# ตัวตัดสินคือคู่ (ผ้า, ลูกค้า) ต้องอยู่แถวเดียวกันใน itemcore_pairs ด้านล่าง
# ทั้ง is_core_item ในลูปวางแผน และ _plan_priority (ลำดับคิว) ใช้ itemcore_pairs เหมือนกัน
# เหลือใช้จุดเดียว: กวาดหาใบสั่งของลูกค้า core เพื่อจับคู่สต๊อก core (ดูตอนหา _core_so_set)
# ตั้งที่หน้า "ตั้งค่าระบบ" — ชื่อเทียบตรงตัวกับที่ ERP ส่งมา (ไม่แปลงตัวพิมพ์)
CORE_CUSTOMERS = {str(_c).strip() for _c in plan_config.get_list("KP_CORE_CUSTOMERS") if str(_c).strip()}

# ทะเบียน CORE ITEM — "core" ตัดสินด้วย ผ้า + ลูกค้า ที่อยู่ "แถวเดียวกัน" ในทะเบียน
# (ทะเบียนตั้ง TARGET/MONTH แยกรายแถว เช่น FD5PRTJZ63A0 → RTS/ที คัลเจอร์ 24,000 กับ
#  NYK1-1/CENTER DOMESTIC 5,000 คนละเป้า จึงใช้สต๊อก/โควตาร่วมกันไม่ได้)
# ⚠ จับได้แค่ ผ้า + ลูกค้า เพราะ order_ready.xlsx ไม่มีคอลัมน์ทีม (ทีมมีแต่ใน stock/booking)
# itemcore_lookup = ด่านแรก "ผ้าตัวนี้อยู่ในทะเบียนไหม" ก่อนค่อยเทียบคู่กับลูกค้า

def _core_ckey(v) -> str:
    """ชื่อลูกค้า/ทีม → คีย์เทียบ (ยุบช่องว่างซ้ำ + พิมพ์ใหญ่) — ชื่อไทยในไฟล์เว้นวรรคไม่เท่ากันได้"""
    return " ".join(str(v or "").split()).upper()


itemcore_lookup = {}
itemcore_pairs = set()      # {(ITEM, ลูกค้า)}

if not itemcore_df.empty:

    for _, row in itemcore_df.iterrows():

        item = str(row.get('Item code', row.get('Item code ', ''))).strip().upper()

        customer = str(row.get('Customer', '')).strip()

        if item and item not in ('NAN', 'NONE'):

            itemcore_lookup[item] = customer

            itemcore_pairs.add((item, _core_ckey(customer)))


# ของที่จะเข้าคลัง core (ตัวจำลองสต๊อก _sim_core_batches) = booking ที่ล็อก/เบรกไว้ (ชีท DETAIL)
# + "แถวที่วางลงแผนรอบนี้ไปแล้วจริง" ของ item เดียวกันจากลูกค้า core — คิดสดตอนเริ่มวางแต่ละใบ
# ⚠ เดิมใช้ยอดของใบ core อื่น ๆ ตามสัปดาห์ booking เดิม (BOOKING_WEEK_FROM) แทน ซึ่งผิด: ใบพวกนั้น
#   กำลังถูกวางใหม่ด้วยตัวจำลองเดียวกัน ทุกใบจึงนับว่า "ใบอื่นจะผลิตตรงเวลาเดิม" แล้วดันตัวเองออกไป
#   ต่อกันทั้งกอง (เคสจริง 14 ก.ย. 2569 FD6DV24/3A0: นับของเข้า 19,332 กก. จาก 8 ใบ → รอบผลิตไปอยู่
#   W19 ปีหน้า ทั้งที่ปี 2026 ไม่ได้ผลิตเลยและสต๊อก 1,945 กก. หมดราว W43)
#   ส่วนปัญหาเดิมที่ตัวนั้นแก้ (ทุกใบเห็นแค่ยอดของตัวเอง → สั่งผลิตสัปดาห์เดียวกันทั้งกอง) หายไป
#   เพราะใบถัดไปเห็นรอบผลิตของใบก่อนหน้าที่วางแล้ว


# Stock Data for Core Item trigger week calculation

_FILTERED_STOCK_FILE = DATA_PLAN_DIR / "filtered_stock_data.xlsx"

stock_inventory_lookup = {}

try:

    _fstock_df = pd.read_excel(_FILTERED_STOCK_FILE)

    _fstock_df.columns = _fstock_df.columns.str.strip()

    for _, _frow in _fstock_df.iterrows():

        _fitem = str(_frow.get('ITEM_CODE', '')).strip().upper()

        _finv = float(_frow.get('Inventory', 0) or 0)

        _fsm = float(_frow.get('STOCK_MIN', 0) or 0)

        _fs5 = float(_frow.get('Stock 5 Week', 0) or 0)

        _fteam = str(_frow.get('TEAM_NAME', '')).strip().upper()

        if _fitem and _fsm > 0:

            # ❗ คีย์ = "รหัสผ้า" อย่างเดียว โดยเจตนา — ห้ามผูกลูกค้าหรือทีมเข้าไปอีก
            # ผ้ารหัสเดียวกัน = สเปคเดียวกัน RTS/NYK1-1 หยิบใช้ข้ามทีมกันได้ จึงเป็น "กองเดียว"
            # ตรงกับแถว "รวม N ทีม" ที่หน้า Dashboard ใช้ตัดสิน (core_stock_live._cross_totals)
            # เดิมคีย์เป็น (ผ้า, ลูกค้า) ซึ่งบังเอิญรวมทีมได้เพราะ filtered_stock_data ไม่มี
            # คอลัมน์ลูกค้า — วันที่ไฟล์มีคอลัมน์นั้นเมื่อไหร่ พฤติกรรมจะพลิกเป็นแยกก้อนเงียบ ๆ
            _prev = stock_inventory_lookup.get(_fitem)

            if _prev is not None:

                # หลายทีม = ของกองเดียวกัน → บวกทั้งยอดของและเป้า
                _finv += _prev['Inventory']

                # ❗ บวก ไม่ใช่ max: STOCK_MIN คือ "ยอดใช้ต่อสัปดาห์" ของแต่ละทีม
                # กองรวมกันแล้วยอดใช้ต่อสัปดาห์ก็บวกกัน (เกณฑ์เดียวกับ _sum_targets ของหน้าเว็บ)
                # ใช้ max ทำให้ตัวหารเล็กเกินจริง → cover สูงลวง → เลื่อนทอช้าจนของขาด
                _fsm += _prev['STOCK_MIN']

                _fs5 += _prev['Stock_5_Week']

                _fteam = f"{_prev['_team']},{_fteam}" if _fteam else _prev['_team']

            stock_inventory_lookup[_fitem] = {'Inventory': _finv, 'STOCK_MIN': _fsm, 'Stock_5_Week': _fs5, '_team': _fteam}

    print(f"Stock Data: {len(stock_inventory_lookup)} รหัสผ้า (กองรวมทุกทีม)")

except Exception as _e_stock:

    print(f"Cannot load Stock Data: {_e_stock}")

# ── ยอดใช้ต่อสัปดาห์ของกองสต๊อก core = รวม STOCK MIN "ทุกทีมในทะเบียน Target_Stock" ──
# ไฟล์สต๊อกมีแถวเฉพาะทีมที่ยังมีของ (View_Stock กรอง BALANCE_KG ≥ 100) → ทีมที่ของหมดหายจากยอดใช้
# = cover สูงลวง ผลิตช้าจนของขาด (เคสจริง 14 ก.ย. 2569 FD6DV24/3A0 ขาด NYK1-1 → คิด 625 แทน 1,375)
# และรหัสที่ไม่มีของเหลือเลยไม่มีคีย์ → ข้ามการจำลองทั้งตัวแบบเงียบ (12/62 รหัส) ทั้งที่ของหมด
# คือเวลาที่ต้องผลิตที่สุด → ของคงคลังยังมาจากไฟล์สต๊อก (ไม่มีแถว = 0) แต่ยอดใช้/ขนาดรอบมาจากทะเบียน
# สูตรเดียวกับ Stock.py: MIN ว่าง → TARGET/MONTH ÷ 4 ; Stock 5 Week = MIN × 5
if not itemcore_df.empty:
    _tc = {str(c).strip().upper(): c for c in itemcore_df.columns}
    _tc_item = _tc.get("ITEM CODE")
    _tc_min = _tc.get("STOCK MIN")
    _tc_tm = next((c for u, c in _tc.items() if "TARGET" in u and "MONTH" in u), None)
    _target_min_by_item: dict = {}
    if _tc_item is not None:
        for _, _tr in itemcore_df.iterrows():
            _ti = str(_tr.get(_tc_item, "")).strip().upper()
            if not _ti or _ti in ("NAN", "NONE"):
                continue
            _tmin = pd.to_numeric(_tr.get(_tc_min), errors="coerce") if _tc_min is not None else float("nan")
            if pd.isna(_tmin) or _tmin <= 0:
                _tmo = pd.to_numeric(_tr.get(_tc_tm), errors="coerce") if _tc_tm is not None else float("nan")
                _tmin = (_tmo / 4) if pd.notna(_tmo) and _tmo > 0 else 0.0
            if _tmin > 0:
                _target_min_by_item[_ti] = _target_min_by_item.get(_ti, 0.0) + float(_tmin)
    _tm_changed, _tm_added = 0, 0
    for _ti, _tmin in _target_min_by_item.items():
        _cur = stock_inventory_lookup.get(_ti)
        if _cur is None:
            _tm_added += 1
        elif abs(float(_cur["STOCK_MIN"]) - _tmin) > 1e-6:
            _tm_changed += 1
        stock_inventory_lookup[_ti] = {
            "Inventory": float(_cur["Inventory"]) if _cur else 0.0,
            "STOCK_MIN": _tmin,
            "Stock_5_Week": _tmin * 5,
            "_team": _cur["_team"] if _cur else "",
        }
    if _target_min_by_item:
        print(f"📘 ยอดใช้ต่อสัปดาห์ core จากทะเบียน Target_Stock ทุกทีม: {len(_target_min_by_item)} รหัส "
              f"(ปรับยอดใช้ {_tm_changed} รหัสที่ไฟล์สต๊อกขาดบางทีม, เพิ่ม {_tm_added} รหัสที่ไม่มีของเหลือเลย)")

# Gauge lookup: (ITEM_CODE, MC_GROUP) → GUAGE string

# ใช้เป็น fallback เมื่อ gauge ไม่ได้มาจาก data source โดยตรง

_item_mc_to_gauge = {}





def _normalize_gauge(gauge) -> str:

    """Normalize gauge string for comparison"""

    if pd.isna(gauge):

        return ""

    gauge_str = str(gauge).strip().upper()

    # Remove common suffixes/prefixes

    gauge_str = gauge_str.replace("G", "").replace("GAUGE", "")

    # ตัด ".0" ท้าย: gauge จาก float (เช่น 36.0) ให้ตรงกับ gauge จาก string ("36")
    # ไม่งั้นคีย์ lookup ไม่ตรง (เช่น MC Special ใน MasterMC เป็น float แต่ DETAIL เป็น string)
    if gauge_str.endswith(".0"):
        gauge_str = gauge_str[:-2]

    return gauge_str





def _get_item_cotton_poly(item_code: str) -> str:
    """คืน 'COTTON' ถ้า prefix FD5/F5, 'POLY' ถ้า prefix FD4/F4, '' ถ้าไม่ตรง"""
    item = str(item_code).strip().upper()
    if item.startswith("FD5") or item.startswith("F5"):
        return "COTTON"
    if item.startswith("FD4") or item.startswith("F4"):
        return "POLY"
    return ""


def _get_subgroup_by_item_prefix(mc_group: str, gauge_str: str, item_code: str) -> str:
    """คืน 'COTTON' หรือ 'POLY' ถ้า (mc_group, gauge) มีใน MC Special และ item prefix ตรง
    อ่านจากชีท MC Special ใน MasterMC.xlsx แทนการ hardcode
    คืน None ถ้าไม่มี sub-pool พิเศษสำหรับ item นี้
    """
    item_type = _get_item_cotton_poly(item_code)
    if not item_type:
        return None

    mc_upper = str(mc_group).strip().upper()
    g_norm = _normalize_gauge(gauge_str)
    factory = _mc_to_factory(mc_upper, g_norm)
    mc_cat = _mc_to_type1(mc_upper, g_norm)

    # ค้นหาใน MC Special: specific MC ก่อน แล้วค่อย general (MC="")
    ms_entry = (
        _MC_SPECIAL_PLAN.get((factory, mc_cat, mc_upper, g_norm))
        or _MC_SPECIAL_PLAN.get((factory, mc_cat, "", g_norm))
    )
    if not ms_entry:
        return None

    if ms_entry.get(item_type, 0) > 0:
        return item_type
    return None





def _apply_mc_redirect(mc_group: str, gauge) -> tuple:
    """ถ้า (mc_group, gauge) อยู่ใน MC_GROUP_REDIRECT ให้เปลี่ยนไปใช้ค่าใหม่แทนเสมอ
    เช่น SKP gauge-20 → FA gauge-20 (อ้อมน้อย)
    คืนค่า (new_mc_group, new_gauge_str)
    """
    gauge_str = _normalize_gauge(gauge)
    key = (str(mc_group).strip().upper() if mc_group else "", gauge_str)
    if key in MC_GROUP_REDIRECT:
        return MC_GROUP_REDIRECT[key]
    return mc_group, gauge_str
for _, _r in item_cap_data.iterrows():
    _ic = str(_r.get("ITEM_CODE", "")).strip().upper()
    _mc = str(_r.get("MC_GROUP", "")).strip().upper()
    _gg = _r.get("GUAGE")
    _gs = _normalize_gauge(_gg)
    if _ic and _mc and _gs and _gs.lower() != "nan":
        _item_mc_to_gauge[(_ic, _mc)] = _gs



# =========================
# ITEM SPECIAL: per-(Item, MC, Guage) override for Working day and Working hour
# Source: MasterMC.xlsx sheet "Item Special"
# =========================
ITEM_SPECIAL_LOOKUP: dict = {}  # key=(item_upper, mc_upper, gauge_str), value=(working_day, working_hour)
try:
    _is_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Item Special")
    _is_df.columns = _is_df.columns.str.strip()
    for _, _is_row in _is_df.iterrows():
        _is_mc = str(_is_row.get("MC", "")).strip().upper()
        _is_guage = _normalize_gauge(_is_row.get("Guage", ""))
        _is_item = str(_is_row.get("Item", "")).strip().upper()
        _is_wd = int(_is_row.get("Working day", 6) or 6)
        _is_wh = int(_is_row.get("Working hour", 20) or 20)
        if _is_item and _is_mc:
            ITEM_SPECIAL_LOOKUP[(_is_item, _is_mc, _is_guage)] = (_is_wd, _is_wh)
    print(f"Item Special: {len(ITEM_SPECIAL_LOOKUP)} entries loaded from MasterMC.xlsx (sheet: Item Special)")
except Exception as _e_is:
    print(f"Cannot load Item Special ({_e_is}) -- using MasterMC defaults")
    ITEM_SPECIAL_LOOKUP = {}


# S9 Logic: โหลด S9 Only + Item S9 + MC S9
#   S9 Only  = item ที่ต้อง route ไป S9 pool เสมอ (ทอในบ้านไม่ได้)
#   Item S9  = (item, เกจ) ที่ "จ้างทอได้" → ใช้กับยอดที่ทอในบ้านล้นเลย TARGET_KNIT (ดู AUTO_S9)
#   MC S9    = pool เครื่องของผู้รับจ้างทอ
_s9_only_items: set = set()       # items ที่ใช้ S9 เสมอ
_s9_only_spec: dict = {}          # item → {"cap": กก./เครื่อง/วัน|None, "gauge": เกจ|None, "cat": CAT|""} จากชีท S9 Only
_s9_eligible_items: set = set()   # (item, gauge) ที่จ้างทอได้ — เกณฑ์เดียว: ไม่ทัน target
_s9_item_cap: dict = {}           # (item, gauge) → CAP กก./เครื่อง/วัน ของผู้รับจ้าง (คอลัมน์ CAP ชีท Item S9)
_mc_s9_df: "pd.DataFrame" = pd.DataFrame()
_s9_weekly_usage: dict = {}  # {(week, s9_pool_id): machines_allocated} — pool_id = 1 แถวในชีท MC S9 (ดู _s9_pool_key)
# ปิดการส่งงานไปจ้างทออัตโนมัติได้ที่หน้า "ตั้งค่าระบบ" (S9 Only / ก้อนที่ user สั่งเอง ยังทำงานปกติ)
AUTO_S9_ENABLED = plan_config.get("KP_AUTO_S9")
# จ้างทอก็ต้องค่อย ๆ เพิ่มเครื่อง (user 10 ก.ย. 2569): เครื่องจ้างทอของผ้าตัวหนึ่งบนพูล·เกจหนึ่ง
# ต่อสัปดาห์ ≤ สัปดาห์ก่อน + S9_RAMP_STEP · ปิดกลับไปใช้เต็มพูลทันทีได้ที่หน้า "ตั้งค่าระบบ"
S9_RAMP_ENABLED = plan_config.get("KP_S9_RAMP")
S9_RAMP_STEP = plan_config.get("KP_S9_RAMP_STEP")
# ก่อนส่งงานออกจ้างทออัตโนมัติ ให้เทียบก่อนว่าทอในบ้านจบช้ากว่าจริงไหม (user 21 ก.ย. 2569)
# เดิมใช้เกณฑ์เดียวคือ "เลย target แล้ว" → ใบที่ด้ายเข้าช้ากว่า target เข้าเงื่อนไขตั้งแต่รอบแรก
# แล้วถูกส่งออกทั้งก้อนทั้งที่ยังไม่ได้วางในบ้านสักกิโลและเครื่องในบ้านว่างเหลือเฟือ
# (เจอจริง: FD6DRTME09A0 SC 722332 ด้ายเข้า W47 target W43 → จ้างทอ 2,622 กก. จบ W49
#  เท่ากับทอในบ้านเป๊ะ ๆ ขณะที่พูล PHET DOUBLE-30 เกจ 22 ว่าง 25–29 เครื่อง)
S9_COMPARE_INHOUSE = plan_config.get("KP_S9_COMPARE_INHOUSE")
# CAT ของพูลผู้รับจ้างต้องตรงกับ CAT ของงาน (user 16 ก.ย. 2569) — ไม่ตรง = จ้างทอไม่ได้
# เดิมเป็นด่านอ่อน: ไม่มี CAT ตรงก็ตกไปใช้แถวอื่นที่เกจเดียวกัน → งานซิงเกิลไปนั่งเครื่องดับเบิลเงียบ ๆ
# (เจอจริง: SINGLE-30/32/34/36 เกจ 24 ตกไปพูล RAP/DOUBLE-30 ที่มีเครื่องเดียว)
# ปิดกลับไปเป็นด่านอ่อนได้ที่หน้า "ตั้งค่าระบบ"
S9_CAT_STRICT = plan_config.get("KP_S9_CAT_STRICT")
# ชื่อ "ถัง" ของงานจ้างทอในคีย์ภายใน (dedup / carry / โควตาสัปดาห์) — ไม่ใช่ชื่อกลุ่มเครื่อง
# และไม่ได้ถูกเขียนลงไฟล์แผน (แถวจริงใช้ชื่อผู้รับจ้าง) : งานจ้างทอของ item+สัปดาห์เดียวกัน
# ต้องอยู่ถังเดียวกันเสมอ แม้จะถูกแตกไปหลายเจ้า ไม่งั้นนับยอด/เครื่องซ้ำ
S9_KEY = "S9"
# ผู้รับจ้างทำงาน 6 วัน ทุกกลุ่มเครื่อง แล้วหยุดตามปฏิทินเหมือนโรงเรา (WorkDay.get_outsource_working_days)
# ไม่ใช้คอลัมน์ Working day ของชีท MC S9 แล้ว
S9_WORK_DAYS = float(plan_config.get("KP_S9_WORK_DAYS"))
# Lead time จ้างทอ (user 16 ก.ย. 2569): ผู้รับจ้างเริ่มทอได้เร็วสุด = สัปดาห์ปัจจุบัน + ค่านี้
# (อยู่ W38 → เร็วสุด W41) · งานในบ้านยังใช้พื้น FREEZE_WEEKS ตามเดิม จึงต้องแยกค่ากัน
# เคยเป็น TODAY+3 แล้วถูกสั่งเลิก 11 ก.ย. → user สั่งกลับมาใช้อีกครั้ง แต่คราวนี้ปรับได้ที่หน้า "ตั้งค่าระบบ"
S9_LEAD_WEEKS = int(plan_config.get("KP_S9_LEAD_WEEKS"))


def _s9_floor_idx() -> int:
    """พื้นสัปดาห์ของงานจ้างทอ = สัปดาห์ปัจจุบัน + S9_LEAD_WEEKS
    ไม่ต่ำกว่าพื้นของงานในบ้าน (FREEZE_WEEKS) เผื่อตั้ง lead time ไว้น้อยกว่า
    """
    return int(globals().get("TODAY_IDX", 0)) + max(int(FREEZE_WEEKS), S9_LEAD_WEEKS)
# จ้างทอไม่หัก setup (user 11 ก.ย. 2569) — เครื่องที่ตั้งใหม่ผลิตเต็ม cap ตั้งแต่สัปดาห์แรก
# (เดิม S9_SETUP_CAP_RATIO = 0.5 ให้เครื่องใหม่ผลิตครึ่ง cap)
_s9_item_week_mc: dict = {}  # {(item, s9_mc_upper, gauge_norm, week_idx): เครื่องจ้างทอรวมของ item สัปดาห์นั้น}
try:
    _s9only_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="S9 Only")
    _s9only_df.columns = _s9only_df.columns.str.strip()
    _s9_only_items = set(str(v).strip().upper() for v in _s9only_df["ITEM_CODE"].dropna())
    # S9 Only อ่านตามที่กรอก (user 24 ก.ย. 2569): Cap/Day ต้องมาจากชีทนี้เท่านั้น ไม่ถอยไปหา Item S9
    #   ไม่มี Cap = ห้ามทำแผน (ลง UNPLANNED) ; Guage / CAT ใช้จับผู้รับจ้าง (CAT ว่าง = ใช้ของเครื่องในบ้าน)
    #   MACHINE_GROUP ไม่ได้ใช้คำนวณ — ชื่อเครื่องจ้างทอมาจากชีท MC S9 ของเจ้าที่ได้งาน
    for _, _s9o_r in _s9only_df.dropna(subset=["ITEM_CODE"]).iterrows():
        _s9o_it = str(_s9o_r["ITEM_CODE"]).strip().upper()
        if not _s9o_it or _s9o_it in _s9_only_spec:
            continue
        _s9o_cap = pd.to_numeric(_s9o_r.get("CAP"), errors="coerce")
        _s9o_cat = _s9o_r.get("CAT")
        _s9_only_spec[_s9o_it] = {
            "cap": float(_s9o_cap) if pd.notna(_s9o_cap) and _s9o_cap > 0 else None,
            "gauge": _normalize_gauge(_s9o_r.get("MC_GAUGE")) or None,
            "cat": str(_s9o_cat).strip().upper() if pd.notna(_s9o_cat) and str(_s9o_cat).strip() else "",
        }
    _mc_s9_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="MC S9")
    _mc_s9_df.columns = _mc_s9_df.columns.str.strip()
    print(f"[S9] Loaded {len(_s9_only_items)} S9-only items "
          f"({sum(1 for _v in _s9_only_spec.values() if _v['cap'])} มี Cap/Day), {len(_mc_s9_df)} MC S9 entries")
except Exception as _s9_err:
    print(f"[WARN] Cannot load S9 data from MasterMC: {_s9_err}")
try:
    _s9elig_df = pd.read_excel(_MASTER_MC_PATH, sheet_name="Item S9")
    _s9elig_df.columns = _s9elig_df.columns.str.strip()
    _s9elig_df = _s9elig_df.dropna(subset=["ITEM_CODE", "MC_GAUGE"])
    _s9_eligible_items = set(
        zip(
            _s9elig_df["ITEM_CODE"].astype(str).str.strip().str.upper(),
            _s9elig_df["MC_GAUGE"].map(_normalize_gauge),
        )
    )
    # CAP ต่อผ้า (user 15 ก.ย. 2569): กำลังผลิตจ้างทอ = CAP ของ (item, เกจ) ในชีทนี้ ใช้ตรง ๆ ไม่คูณ 20/24
    # ไม่มี/ว่าง/≤0 → ถอยไปใช้ Cap/Day ของพูลในชีท MC S9 (ดู _s9_pool_row)
    if "CAP" in _s9elig_df.columns:
        _s9cap_df = _s9elig_df.assign(_cap=pd.to_numeric(_s9elig_df["CAP"], errors="coerce"))
        _s9cap_df = _s9cap_df[_s9cap_df["_cap"] > 0]
        for _it, _g, _c in zip(_s9cap_df["ITEM_CODE"].astype(str).str.strip().str.upper(),
                               _s9cap_df["MC_GAUGE"].map(_normalize_gauge), _s9cap_df["_cap"]):
            _s9_item_cap.setdefault((_it, _g), float(_c))
    print(f"[S9] Loaded {len(_s9_eligible_items)} S9-eligible (item, gauge) pairs, "
          f"{len(_s9_item_cap)} item CAP — auto outsource {'ON' if AUTO_S9_ENABLED else 'OFF'}")
except Exception as _s9e_err:
    print(f"[WARN] Cannot load 'Item S9' sheet from MasterMC: {_s9e_err}")


def get_item_special(item_code, mc_group, gauge=None):
    """Return (working_day, working_hour) from Item Special for (item, MC, gauge), or None if not found."""
    if not item_code or not mc_group:
        return None
    item_u = str(item_code).strip().upper()
    mc_u = str(mc_group).strip().upper()
    g_u = _normalize_gauge(gauge)
    result = ITEM_SPECIAL_LOOKUP.get((item_u, mc_u, g_u))
    if result is None and g_u:
        result = ITEM_SPECIAL_LOOKUP.get((item_u, mc_u, ""))
    return result


def _s9_gauge_num(v) -> str:
    """เลขเกจจากค่าในชีท MC S9 — รูปเดียวกับไฟล์ต้นฉบับ `28G34"` (เกจ 28 กระบอก 34 นิ้ว)

    รับเลขเปล่า (28 / 28.0) ได้ด้วย เผื่อชีทรุ่นเก่าที่เก็บเฉพาะเลขเกจ
    """
    s = str(v if v is not None else "").strip()
    if not s or s.upper() in ("NAN", "NONE"):
        return ""
    m = re.match(r"\s*(\d+)\s*[Gg]", s)
    return m.group(1) if m else _normalize_gauge(s)


def _s9_pool_key(supp, mc_group, gauge, mc_cat="") -> str:
    """รหัสพูลย่อยของเครื่องจ้างทอ = 1 แถวในชีท MC S9 → "ผู้รับจ้าง|กลุ่ม|เกจ|MC_CAT"

    user 16 ก.ย. 2569: เครื่องเป็นของผู้รับจ้างแต่ละเจ้า — SINGLE-32 เกจ 26 จ้างรวมได้ 25 เครื่อง
    แต่ห้ามเกินของแต่ละเจ้า (เซินเปา SKP 3 / SKPTA 2, ดารินทร์ 2, บางบอน 2, วันนิตติ้ง 4, เทียนฝู 12)
    → เพดานแข็งอยู่ที่ "เจ้า + กลุ่มเครื่อง + เกจ + CAT" ไม่ใช่ผลรวมของคู่กลุ่ม·เกจ
    (เดิมคีย์ "กลุ่ม|เกจ|CAT|Remark" — รวมเครื่องข้ามเจ้าเป็นกองเดียว)
    """
    def _t(v):
        s = str(v if v is not None else "").strip()
        return "" if s.upper() in ("NAN", "NONE") else s
    return f"{_t(supp)}|{_t(mc_group).upper()}|{_s9_gauge_num(gauge)}|{_t(mc_cat).upper()}"


def _s9_material_class(material_content) -> str:
    """ชนิดผ้าของงาน → หมวดที่ใช้จับคู่กับผู้รับจ้าง: POLY / COTTON / BLEND

    MATERIAL_CONTENT ที่ระบบผลิตได้มี 5 ค่า (ดู `_get_material_content_from_yarn`)
      COTTON → COTTON ; POLY → POLY ; TC, CD, COTTON/CD → BLEND (ผ้าผสม)
    ว่าง/ไม่รู้ชนิด → BLEND (ฝั่งผู้รับจ้างที่ไม่ได้เลือกก็เป็น BLEND เหมือนกัน — สมมาตรกัน)
    user 16 ก.ย. 2569: เจ้าที่เลือก BLEND รับ "เฉพาะผ้าผสม" ไม่ใช่รับได้ทุกอย่าง
    """
    mat = str(material_content or "").strip().upper()
    if not mat or mat in ("NAN", "NONE"):
        return "BLEND"
    has_cotton = "COTTON" in mat
    has_poly = "POLY" in mat
    if "CD" in mat or "TC" in mat or (has_cotton and has_poly):
        return "BLEND"
    if has_poly:
        return "POLY"
    if has_cotton:
        return "COTTON"
    return "BLEND"


def _s9_group_key(gauge, mc_cat, mat_class) -> str:
    """รหัส "กองที่รับงานนี้ได้" = ทุกเจ้าที่ CAT + เกจ + หมวดผ้าตรงกัน (ใช้ตอนคำนวณ/backward)

    เพดานของกองนี้ = ผลรวมเครื่องว่างของสมาชิก แต่ตอนวางจริงต้องแตกเป็นรายเจ้า (_s9_allocate_supps)
    mat_class = POLY / COTTON / BLEND (ดู `_s9_material_class`) — เดิมเป็นธง POLY จริง/เท็จ
    """
    return f"GRP|{str(mc_cat or '').strip().upper()}|{_s9_gauge_num(gauge)}|{mat_class or 'BLEND'}"


_S9_ROWS: list = []          # แถวชีท MC S9 ที่ normalize แล้ว (1 แถว = เครื่องของ 1 เจ้า)
_S9_POOL_TOTALS: dict = {}   # pool_id (รายเจ้า) → Total MC ของแถวนั้น
_S9_GROUPS: dict = {}        # group_id → [pool_id ของสมาชิก]


def _s9_rows_init() -> None:
    """อ่านชีท MC S9 ครั้งเดียว → _S9_ROWS/_S9_POOL_TOTALS/_S9_GROUPS

    ชีทมีรูปเดียวกับไฟล์สรุปเครื่องจ้างทอ: Supp | Mc | New CAT | New Gauge | Total Mc
    (ไม่มี Cap/Day แล้ว — กำลังผลิตมาจาก CAP ต่อผ้าในชีท Item S9 เท่านั้น ;
     ไม่มี Working day — ผู้รับจ้างทำงาน 6 วันหักวันหยุดตามปฏิทิน ดู _s9_working_days)

    2 คอลัมน์ที่ user เลือกจาก dropdown บนหน้าเว็บ (16 ก.ย. 2569):
      Material : POLY / COTTON / BLEND — ว่าง = BLEND (ผ้าผสม) ; จับคู่กับหมวดของงาน
      Status   : Active / Inactive     — ว่าง = Active ; Inactive = ข้ามแถวนี้ ไม่นับเป็นเครื่อง
    ไฟล์รุ่นเก่าที่ไม่มีคอลัมน์ Material → ถอยไปดูชื่อผู้รับจ้าง "รุ่งสยาม (POLY)" แบบเดิม
    (เจ้า POLY รับเฉพาะงาน POLY ; เจ้าอื่นรับ COTTON + BLEND เหมือนพฤติกรรมก่อนมีคอลัมน์นี้)
    """
    if _S9_ROWS or _mc_s9_df.empty:
        return
    _col = {str(c).strip().lower(): c for c in _mc_s9_df.columns}

    def _pick(*names):
        for n in names:
            if n.lower() in _col:
                return _col[n.lower()]
        return None

    _c_supp = _pick("Supp", "Supplier")
    _c_mc = _pick("Mc", "MC Group")
    _c_cat = _pick("New CAT", "MC_CAT")
    _c_g = _pick("New Gauge", "Guage", "Gauge")
    _c_tot = _pick("Total Mc", "Total MC")
    _c_mat = _pick("Material", "ประเภทผ้า")
    _c_status = _pick("Status", "สถานะ")
    if _c_mc is None or _c_g is None or _c_tot is None:
        print("[WARN] ชีท MC S9 ไม่มีคอลัมน์ที่ต้องใช้ (Mc / New Gauge / Total Mc)")
        return
    _n_off = 0
    for _, r in _mc_s9_df.iterrows():
        supp = str(r[_c_supp]).strip() if _c_supp else ""
        if supp.upper() in ("NAN", "NONE"):
            supp = ""
        mc = str(r[_c_mc]).strip()
        cat = str(r[_c_cat]).strip() if _c_cat else ""
        g = _s9_gauge_num(r[_c_g])
        tot = pd.to_numeric(r[_c_tot], errors="coerce")
        if not mc or not g or pd.isna(tot) or int(tot) <= 0:
            continue
        # Status = Inactive → เครื่องชุดนี้หยุดใช้ ไม่นับเป็นกำลังผลิตเลย (ว่าง = Active)
        if _c_status is not None and str(r[_c_status]).strip().upper() == "INACTIVE":
            _n_off += 1
            continue
        pid = _s9_pool_key(supp, mc, g, cat)
        # หมวดผ้าที่เจ้านี้รับ — จากคอลัมน์ Material ถ้ามี ; ไม่มีคอลัมน์ = ถอยไปดูชื่อเจ้าแบบเดิม
        if _c_mat is not None:
            _mv = str(r[_c_mat]).strip().upper()
            mats = {_mv} if _mv in ("POLY", "COTTON", "BLEND") else {"BLEND"}
        else:
            mats = {"POLY"} if "POLY" in supp.upper() else {"COTTON", "BLEND"}
        _S9_ROWS.append({"supp": supp, "mc": mc, "cat": cat.upper(), "gauge": g,
                         "total": int(tot), "mats": mats, "pool_id": pid})
        _S9_POOL_TOTALS[pid] = _S9_POOL_TOTALS.get(pid, 0) + int(tot)
        for _mc_class in mats:
            _S9_GROUPS.setdefault(_s9_group_key(g, cat, _mc_class), []).append(pid)
    print(f"[S9] พูลจ้างทอ {len(_S9_ROWS)} แถว / {len(set(r['supp'] for r in _S9_ROWS))} เจ้า "
          f"รวม {sum(r['total'] for r in _S9_ROWS)} เครื่อง"
          + (f" (ข้าม Inactive {_n_off} แถว)" if _n_off else ""))


def _s9_pool_rows(gauge, mc_cat="", material_content=""):
    """แถวผู้รับจ้างทุกเจ้าที่รับงาน (เกจ + CAT + ชนิดผ้า) นี้ได้ — เพดานแยกต่อเจ้า

    CAT ต้องตรง (S9_CAT_STRICT) — ผู้รับจ้างไม่มีเครื่องชนิดนี้ = จ้างทอไม่ได้
    ชนิดผ้าต้องตรงหมวดที่เจ้านั้นเลือกไว้ (คอลัมน์ Material) — ไม่มีเจ้าไหนรับ = จ้างทอไม่ได้
    """
    _s9_rows_init()
    g = _normalize_gauge(gauge)
    if not g or not _S9_ROWS:
        return []
    rows = [r for r in _S9_ROWS if r["gauge"] == g]
    if mc_cat:
        cat_u = str(mc_cat).strip().upper()
        _cat_rows = [r for r in rows if r["cat"] == cat_u]
        if not _cat_rows and S9_CAT_STRICT:
            return []
        if _cat_rows:
            rows = _cat_rows
    _cls = _s9_material_class(material_content)
    return [r for r in rows if _cls in r["mats"]]


def _s9_pool_row(gauge, mc_cat="", material_content="", item_code=""):
    """กองเครื่องจ้างทอที่รับงานนี้ได้ (ทุกเจ้ารวมกัน) — ใช้ตอนคำนวณเครื่อง/backward

    คืน (s9_mc_group, cap_per_day, total_mc, working_day, gauge_norm, group_id) หรือ None
    - s9_mc_group = กลุ่มเครื่องของเจ้าที่มีเครื่องมากสุดในกอง (ชื่อตัวแทน — แถวจริงเขียนชื่อเจ้าทีหลัง)
    - total_mc    = ผลรวมของทั้งกอง ; เพดานรายเจ้าบังคับตอนแตกแถว (_s9_allocate_supps)
    - cap_per_day = CAP ของ (item, เกจ) ในชีท Item S9 เท่านั้น — ไม่มี = จ้างทอไม่ได้
      ผ้า S9 Only ใช้ CAP ของชีท S9 Only แทน (ไม่ดู Item S9)
      (user 16 ก.ย. 2569: ชีท MC S9 ไม่มีคอลัมน์ Cap/Day แล้ว จึงไม่มีค่าสำรองให้ถอยไปใช้)
    """
    rows = _s9_pool_rows(gauge, mc_cat, material_content)
    if not rows:
        return None
    g = _normalize_gauge(gauge)
    _item_u = str(item_code or "").strip().upper()
    if _item_u in _s9_only_spec:
        # ผ้า S9 Only: Cap/Day จากชีท S9 Only เท่านั้น (user 24 ก.ย. 2569) — ว่าง = ไม่มี ไม่ถอยไป Item S9
        s9_cap = _s9_only_spec[_item_u]["cap"]
    else:
        s9_cap = _s9_item_cap.get((_item_u, g))
    if not s9_cap or s9_cap <= 0:
        return None
    _rep = max(rows, key=lambda r: r["total"])
    return (
        _rep["mc"],
        float(s9_cap),
        sum(r["total"] for r in rows),
        S9_WORK_DAYS,
        g,
        # กองต้องคีย์ด้วย "หมวดผ้าของงาน" ไม่ใช่ของแถวตัวแทน — ไม่งั้นงานคนละหมวด
        # ที่ลงเจ้าเดียวกันได้จะไปใช้โควตากองเดียวกัน
        _s9_group_key(g, _rep["cat"], _s9_material_class(material_content)),
    )


def _s9_pool_remain(pool_id, week) -> int:
    """เครื่องจ้างทอที่ยังว่างในสัปดาห์นั้น

    pool_id ขึ้นต้น "GRP|" = ทั้งกอง (ผลรวมของทุกเจ้าที่รับงานนี้ได้) — ใช้ตอนคำนวณ/backward
    นอกนั้น = พูลรายเจ้า (1 แถวในชีท) — เพดานแข็งที่ห้ามเกิน ใช้ตอนแตกแถวจริง
    ทุกจุดที่ตัดสินเครื่องจ้างทอต้องผ่านฟังก์ชันนี้ตัวเดียว ไม่งั้นแต่ละจุดเห็นเพดานคนละตัว
    """
    if not pool_id:
        return 0
    _s9_rows_init()
    if not _S9_ROWS:
        return 0
    if str(pool_id).startswith("GRP|"):
        return sum(_s9_pool_remain(_p, week) for _p in _S9_GROUPS.get(pool_id, []))
    return max(0, int(_S9_POOL_TOTALS.get(pool_id, 0)) - int(_s9_weekly_usage.get((week, pool_id), 0)))


# เครื่องของผู้รับจ้างแต่ละเจ้าที่ผ้าตัวนี้ใช้ในสัปดาห์นั้น — ฐานของ "เจ้าเดิมก่อน" + แยก carry/setup รายเจ้า
_s9_supp_week_mc: dict = {}  # {(item_upper, pool_id, week_idx): เครื่อง}


def _s9_allocate_supps(gauge, mc_cat, material_content, need, week, item_code):
    """แตกเครื่องที่ต้องใช้ออกเป็นรายผู้รับจ้าง — 1 แถวแผน = 1 เจ้า

    user 16 ก.ย. 2569: "จ้างรวม ๆ กันได้ แต่ต้องไม่ใช้เครื่องเกินกว่าของแต่ละ SUPP"
    ลำดับ: เจ้าที่ทอผ้าตัวนี้อยู่แล้วสัปดาห์ก่อนมาก่อน (ไม่ต้องย้ายงาน) → เจ้าที่เครื่องว่างมากสุด
    คืน [(pool_id, supp, mc, เครื่อง, เครื่องที่ทอต่อจากสัปดาห์ก่อน)] — ผลรวม ≤ need
    """
    rows = _s9_pool_rows(gauge, mc_cat, material_content)
    need = int(need or 0)
    if not rows or need <= 0:
        return []
    _item_u = str(item_code or "").strip().upper()
    _idx = week_index(week)
    _prev_idx = None
    if _idx is not None:
        _prev_idx = int(_idx) - 1
        while _prev_idx >= 0 and WorkDay.merged_into(int(calendar_week.iloc[_prev_idx]["WEEK"])) is not None:
            _prev_idx -= 1
    cand = []
    for r in rows:
        _remain = _s9_pool_remain(r["pool_id"], week)
        if _remain <= 0:
            continue
        _prev = int(_s9_supp_week_mc.get((_item_u, r["pool_id"], _prev_idx), 0)) if _prev_idx is not None else 0
        cand.append((min(_prev, _remain), _remain, r))
    # เจ้าเดิม (เครื่องที่ยังอุ่น) มากสุดก่อน → เครื่องว่างมากสุด → ชื่อเจ้า (ให้ผลคงที่ทุกรอบ)
    cand.sort(key=lambda x: (-x[0], -x[1], x[2]["supp"]))
    out, left = [], need
    for _prev, _remain, r in cand:
        if left <= 0:
            break
        _take = min(_remain, left)
        out.append((r["pool_id"], r["supp"], r["mc"], _take, min(_take, _prev)))
        left -= _take
    return out


def _s9_working_days(week) -> float:
    """วันทำงานของผู้รับจ้างสัปดาห์นั้น — 6 วันเลื่อนตามปฏิทินเหมือนโรงเรา (สัปดาห์ถูกยุบ = 0)"""
    try:
        return WorkDay.get_outsource_working_days(week, S9_WORK_DAYS)
    except (TypeError, ValueError):
        return S9_WORK_DAYS


def _s9_prev_week_mc(item, mc_group, gauge, week_idx) -> int:
    """เครื่องจ้างทอรวมของ item บนพูล·เกจนี้ ในสัปดาห์ก่อนหน้า (ข้ามสัปดาห์ที่ถูกยุบ)

    เป็นฐานของการไล่เครื่องทีละ S9_RAMP_STEP — สัปดาห์ก่อนไม่มีงาน = 0 (เริ่มไล่ใหม่จาก 2)
    """
    if week_idx is None:
        return 0
    key = (str(item).strip().upper(), str(mc_group or "").strip().upper(), _normalize_gauge(gauge))
    _i = int(week_idx) - 1
    while _i >= 0 and WorkDay.merged_into(int(calendar_week.iloc[_i]["WEEK"])) is not None:
        _i -= 1
    if _i < 0:
        return 0
    return int(_s9_item_week_mc.get(key + (_i,), 0))


def _s9_backward_start_idx(qty, rdd_idx, gauge, mc_cat="", setup_days=SETUP_DAYS,
                           material_content="", floor_idx=0, item_code=""):
    """งานจ้างทอก็วางแบบ backward (JIT): เริ่มช้าที่สุดเท่าที่ยังทอเสร็จทัน RDD

    ถอยจาก rdd_idx กลับมาทีละสัปดาห์ แล้วจำลองกำลังผลิตของ S9 pool จริงแบบเดียวกับตอนวาง:
    เครื่องไล่ขึ้นทีละ S9_RAMP_STEP (ไม่เกินเครื่องที่ยังว่าง) × วันทำงานผู้รับจ้าง × Cap/Day
    จ้างทอไม่หัก setup — เครื่องที่ตั้งใหม่ผลิตเต็ม cap ตั้งแต่สัปดาห์แรก
    (setup_days เก็บไว้ให้ผู้เรียกเดิม ไม่ได้ใช้แล้ว)
    คืน index สัปดาห์เริ่มที่ช้าที่สุดที่ยังผลิตครบ — หรือ None ถ้าถอยถึง floor_idx แล้วยังไม่ทัน
    (ผู้เรียกต้องเริ่มเร็วที่สุดคือ floor_idx = พื้นเดียวกับงานในบ้าน: TODAY+2 / ด้ายเข้า / ย้อมเสร็จ)
    """
    _pool = _s9_pool_row(gauge, mc_cat, material_content, item_code)
    if _pool is None or rdd_idx is None or qty is None or qty <= 0:
        return None
    s9_mc, s9_cap, s9_total, s9_wd, g_norm, s9_pool_id = _pool
    _floor = max(0, int(floor_idx))
    if rdd_idx < _floor:
        return None
    for _start in range(int(rdd_idx), _floor - 1, -1):
        _q = float(qty)
        _prev_mc = 0  # ก้อนใหม่ยังไม่มีเครื่องจ้างทอวิ่งอยู่
        for _idx in range(_start, int(rdd_idx) + 1):
            if _idx >= len(calendar_week):
                break
            _wk = int(calendar_week.iloc[_idx]["WEEK"])
            _wd = _s9_working_days(_wk)
            if _wd <= 0:
                continue  # สัปดาห์ถูกยุบ/หยุดทั้งสัปดาห์ — ไม่ผลิต แต่เครื่องที่ตั้งไว้ยังอยู่
            _remain = _s9_pool_remain(s9_pool_id, _wk)
            _mc = min(_remain, _prev_mc + S9_RAMP_STEP) if S9_RAMP_ENABLED else _remain
            if _mc <= 0:
                _prev_mc = 0  # pool เต็ม — ไม่มีเครื่องวิ่ง สัปดาห์ถัดไปต้องตั้งใหม่
                continue
            _q -= _mc * _wd * s9_cap
            _prev_mc = _mc
            if _q <= 0:
                break
        if _q <= 0:
            return _start
    return None


def _week_txt_at(week_idx) -> str:
    """index สัปดาห์ → ข้อความ "W48" สำหรับ log (นอกช่วงปฏิทิน/None = "ไม่จบ")"""
    if week_idx is None or not (0 <= int(week_idx) < len(calendar_week)):
        return "ไม่จบ"
    return f"W{int(calendar_week.iloc[int(week_idx)]['WEEK_NUM'])}"


def _s9_finish_idx(qty, start_idx, gauge, mc_cat="", material_content="", item_code="", horizon=60):
    """จ้างทอเริ่มที่ start_idx แล้วทอจบที่ index สัปดาห์ไหน

    เดินหน้าด้วยกติกาเดียวกับ _s9_backward_start_idx เป๊ะ: เครื่องไล่ขึ้นทีละ S9_RAMP_STEP
    (ไม่เกินเครื่องที่พูลยังว่าง) × วันทำงานผู้รับจ้าง × Cap/Day และไม่หัก setup
    ใช้ตอบคำถาม "ส่งออกไปจ้างทอแล้วจบเร็วกว่าทอในบ้านจริงไหม" (คู่กับ _inhouse_finish_idx)
    คืน None เมื่อไม่มีพูลจ้างทอที่รับงานนี้ได้ หรือทอไม่จบในกรอบ horizon
    """
    _pool = _s9_pool_row(gauge, mc_cat, material_content, item_code)
    if _pool is None or start_idx is None or qty is None or qty <= 0:
        return None
    s9_mc, s9_cap, s9_total, s9_wd, g_norm, s9_pool_id = _pool
    _q = float(qty)
    _prev_mc = 0
    _start = max(0, int(start_idx))
    for _idx in range(_start, _start + int(horizon)):
        if _idx >= len(calendar_week):
            return None
        _wk = int(calendar_week.iloc[_idx]["WEEK"])
        _wd = _s9_working_days(_wk)
        if _wd <= 0:
            continue  # สัปดาห์ถูกยุบ/หยุดทั้งก้อน — เครื่องที่ตั้งไว้ยังอยู่
        _remain = _s9_pool_remain(s9_pool_id, _wk)
        _mc = min(_remain, _prev_mc + S9_RAMP_STEP) if S9_RAMP_ENABLED else _remain
        if _mc <= 0:
            _prev_mc = 0  # pool เต็ม — ไม่มีเครื่องวิ่ง สัปดาห์ถัดไปต้องตั้งใหม่
            continue
        _q -= _mc * _wd * s9_cap
        _prev_mc = _mc
        if _q <= 0:
            return _idx
    return None


def _inhouse_finish_idx(qty, start_idx, mc_group, gauge, cap_per_day, setup_days,
                        item_code="", rev_weight=0, start_mc=0, horizon=60):
    """ทอในบ้านเริ่มที่ start_idx ด้วยเครื่องที่ว่างจริง แล้วทอจบที่ index สัปดาห์ไหน

    จำลองด้วยกติกาจริงของงานในบ้าน: ตั้งเครื่องใหม่ได้ไม่เกิน MAX_NEW_SETUP_MC ต่อสัปดาห์
    (เพดานเดียวกับที่ด่าน hard cap บังคับตอนวางจริง), ไม่เกินเครื่องที่พูลยังว่าง,
    ไม่เกินโควตา setup job ที่เหลือของโรง·ชนิดเครื่องในสัปดาห์นั้น (ด่าน JOB QUOTA),
    พูลที่ booking จองเกินเครื่องจริงบีบเครื่องที่ carry ต่อได้ด้วย (ด่าน CARRY POOL FULL),
    เครื่องที่ตั้งใหม่เสียวัน setup เฉพาะสัปดาห์แรกของมัน, เครื่องที่วิ่งอยู่แล้วผลิตเต็มสัปดาห์,
    ยอดปัดลงเป็นพับเต็มตาม rev_weight
    start_mc = เครื่องที่ item นี้วิ่งอยู่แล้ว (carry — ผลิตเต็มสัปดาห์ ไม่เสีย setup)
    ผู้เรียกต้องกรองมาแล้วว่ายังอุ่นจริง (ห่างไม่เกิน SETUP_GAP_WEEK) ไม่งั้นจะประเมินในบ้านสูงเกินจริง
    คืน None เมื่อทอไม่จบในกรอบ horizon (พูลไม่ว่างเลย / ยอดใหญ่เกิน)
    """
    if start_idx is None or qty is None or qty <= 0 or not cap_per_day or cap_per_day <= 0:
        return None
    _q = float(qty)
    _prev_mc = max(0, int(start_mc or 0))
    _rw = rev_weight or 0
    _start = max(0, int(start_idx))
    for _idx in range(_start, _start + int(horizon)):
        if _idx >= len(calendar_week):
            return None
        _wk = int(calendar_week.iloc[_idx]["WEEK"])
        # เครื่องว่างจริงของพูล — หัก booking / เครื่องที่แผนจองไปแล้ว / Lock_MC / sub-pool ให้แล้ว
        _free = get_actual_mc_remain(mc_group, _wk, gauge, item_code=item_code)
        # พูลที่ booking จองเกินเครื่องจริง (remain ติดลบ) ยึดเครื่องที่แผนถืออยู่ไปก่อน
        # → carry ต่อได้เท่าที่เหลือ (ด่าน CARRY POOL FULL ตอนวางจริง)
        _free_neg = get_actual_mc_remain(
            mc_group, _wk, gauge, item_code=item_code, allow_negative=True
        )
        if _free_neg < 0:
            _prev_mc = max(0, _prev_mc + int(_free_neg))
        # โควตา setup job ของโรง·ชนิดเครื่องในสัปดาห์นั้น (ด่าน JOB QUOTA ตอนวางจริง)
        _job_room = get_remaining_job_slots(_wk, mc_group)
        # โควตาตั้งเครื่องใหม่ของผ้านี้ในสัปดาห์นั้น (ใบสั่งอื่นของผ้าเดียวกันใช้ไปแล้วเท่าไร)
        _new = min(
            _item_setup_room(item_code, mc_group, gauge, _wk),
            max(0, int(_free)), max(0, int(_job_room)),
        )
        _mc = _prev_mc + _new
        if _mc <= 0:
            continue  # สัปดาห์นี้พูลไม่ว่างเลยและยังไม่มีเครื่องวิ่ง
        _wd = get_working_days_by_factory(mc_group, _mc, week=_wk, gauge=gauge)
        if not _wd or _wd <= 0:
            _prev_mc = _mc
            continue  # สัปดาห์หยุดทั้งก้อน — เครื่องที่ตั้งไว้ยังอยู่
        _prod = (
            _prev_mc * _wd * cap_per_day
            + _new * max(0, _wd - (setup_days or 0)) * cap_per_day
        )
        if _rw > 0 and _prod > 0:
            _prod = (_prod // _rw) * _rw
        _prev_mc = _mc
        if _prod <= 0:
            continue
        _q -= _prod
        if _q <= 0:
            return _idx
    return None


def _calc_s9_required_machines(qty, plan_week, rdd_idx, gauge, mc_cat="", setup_days=SETUP_DAYS, material_content="",
                               item_code=""):
    """คำนวณ required machines จาก S9 MC pool โดย match MC_CAT + gauge
    คืน (s9_mc_group, s9_cap_per_day, req_mc, feasible, gauge, pool_id)
    ถ้า gauge ไม่ตรงกับ MC S9 → คืน (None, None, None, False, gauge, None)
    """
    _pool = _s9_pool_row(gauge, mc_cat, material_content, item_code)
    if _pool is None:
        return None, None, None, False, gauge, None
    s9_mc, s9_cap, s9_total, s9_wd, g_norm, s9_pool_id = _pool
    g = g_norm
    # เพดานตอนคำนวณ = เครื่องว่างรวมของทุกเจ้าที่รับงานนี้ได้ (กอง GRP|…) — ชุดเดียวกับ backward
    # เพดานรายเจ้าบังคับอีกชั้นตอนแตกแถวจริง (_s9_allocate_supps) จึงไม่มีทางใช้เกินของเจ้าไหน
    s9_remain = _s9_pool_remain(s9_pool_id, plan_week)
    s9_used = max(0, int(s9_total) - int(s9_remain))
    # คำนวณ req_mc ตาม qty และ weeks เหลือจนถึง RDD
    _pw_idx = week_index(plan_week)
    weeks_left = max(1, (rdd_idx - _pw_idx)) if rdd_idx is not None and _pw_idx is not None else 1
    week_cap_1mc = s9_cap * s9_wd
    if week_cap_1mc > 0:
        import math as _math
        req_mc = max(1, _math.ceil(qty / (weeks_left * week_cap_1mc)))
    else:
        req_mc = 1
    req_mc = min(req_mc, s9_remain) if s9_remain > 0 else req_mc
    feasible = s9_remain > 0
    # S9: ใช้เครื่องที่ว่างทั้งหมด — ไม่คำนวณเพื่อจบใน 1 สัปดาห์ (แผนปกติยังจบตาม target)
    print(f"[S9 CALC] gauge={g}, MC_CAT={mc_cat}, material={material_content} → MC={s9_mc}, cap={s9_cap} (Item S9), pool={s9_pool_id}, total={s9_total}, used={s9_used}, remain={s9_remain}, req_mc={req_mc}, feasible={feasible}")
    return s9_mc, s9_cap, req_mc, feasible, gauge, s9_pool_id


def _ck(item, mc_group, gauge=None):
    """สร้าง carryover key: (item, MC ตัวแทนของก้อน, gauge)

    เครื่องที่อยู่ "ก้อนเดียวกัน" (โรง + CAT + เกจ + พูล เดียวกัน) ใช้แทนกันได้จริง
    → item เดียวกันที่ถูกวางคนละ MC_GROUP ในก้อนเดียวกันต้องนับเป็นงานก้อนเดียว
    ไม่ใช่คนละลูกโซ่ (ไม่งั้น setup ซ้ำ + ปัดเศษเครื่องหลายรอบ = เครื่องเกินจริง)
    ดู _canon_mc / GROUP_MC_BY_CAT
    """
    item_norm = str(item).strip().upper() if item is not None else ""
    mc_norm = str(mc_group).strip().upper() if mc_group is not None else ""
    g = _normalize_gauge(gauge)
    if not g:
        # หาเกจด้วย MC_GROUP จริงก่อน (map คีย์ด้วยชื่อกลุ่มจริง ไม่ใช่ตัวแทน)
        g = _normalize_gauge(
            _item_mc_to_gauge.get(
                (item_norm, mc_norm), ""
            )
        )
    return (item_norm, _canon_mc(mc_norm, g), g)


def _combine_carry_with_booking(booking_key, carry_mc, prev_week_idx, cur_week_idx):
    """รวมเครื่อง carry ของแผน กับเครื่อง booking ของสัปดาห์ปัจจุบัน — ห้ามนับเครื่อง booking ซ้ำ

    machines_in_use[key] เก็บ "เครื่องรวม" ของสัปดาห์ก่อน (เครื่องแผน + เครื่อง booking)
    ดังนั้นเครื่องที่เป็นของแผนเองจริงๆ = carry_mc − booking(สัปดาห์ก่อน)
    เครื่องรวมสัปดาห์นี้ = เครื่องของแผนเอง + booking(สัปดาห์นี้)

    เดิมใช้ carry_mc + booking(สัปดาห์นี้) ตรงๆ → เครื่อง booking ตัวเดิมถูกบวกเข้าไปใหม่
    ทุกสัปดาห์ ทำให้จำนวนเครื่องโตขึ้นเรื่อยๆ (เช่น 1 → 2 → 3) ทั้งที่เป็นเครื่องตัวเดียวกัน

    คืน (เครื่องรวมสัปดาห์นี้, เครื่องของแผนเอง, booking สัปดาห์ก่อน, booking สัปดาห์นี้)
    """
    _bk = booking_mc_by_week.get(booking_key, {})
    bk_prev = int(_bk.get(prev_week_idx, 0) or 0) if prev_week_idx is not None else 0
    bk_now = int(_bk.get(cur_week_idx, 0) or 0) if cur_week_idx is not None else 0
    plan_own = max(0, int(carry_mc or 0) - bk_prev)
    return plan_own + bk_now, plan_own, bk_prev, bk_now


def _build_plan_remark(mc_group, carryover_mc, new_mc, setup_days, booking_mc=0):
    """สร้างข้อความ REMARK อธิบายที่มาของเครื่องในแถวแผน

    เพื่อให้คนอ่านแผนแยกออกว่าเครื่องที่เห็นเป็น "เครื่องใหม่ที่ต้อง setup" หรือ
    "เครื่องเดิมที่วิ่งอยู่แล้ว (carry / เติม order เข้าไป)" ซึ่งไม่เสียเวลา setup และไม่กินเครื่องเพิ่ม
    """
    parts = []
    if new_mc > 0 and setup_days > 0:
        parts.append(f"setup เครื่องใหม่ {new_mc} ตัว ({setup_days} วัน)")
    elif new_mc > 0:
        parts.append(f"เครื่องใหม่ {new_mc} ตัว (ไม่หัก setup)")  # จ้างทอ
    if carryover_mc > 0:
        if booking_mc > 0:
            _own = max(0, int(carryover_mc) - int(booking_mc))
            if _own > 0:
                parts.append(f"carry เครื่องเดิม {_own} ตัว + เติมเข้าเครื่อง booking ที่ทอไอเทมนี้อยู่ {booking_mc} ตัว")
            else:
                parts.append(f"เติมเข้าเครื่อง booking ที่ทอไอเทมนี้อยู่แล้ว {booking_mc} ตัว (ไม่ใช้เครื่องเพิ่ม)")
        else:
            parts.append(f"carry เครื่องเดิม {carryover_mc} ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup)")
    if not parts:
        parts.append("ไม่ใช้เครื่อง")
    return f"{mc_group}: " + " + ".join(parts)


# เลข SO ในข้อความ "ใช้เครื่องร่วมกับใบอื่น" ที่ระบบเขียนไว้ตอนสร้างแถว — มี 2 แบบ:
#   แถวเติมเข้าเครื่องเดิม     "…ใช้เครื่อง 4 ตัวร่วมกับ SO 718381 (IBLTA) ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup"
#   แถวซ้อนเครื่องสัปดาห์เดียวกัน "…ใช้เครื่อง 2 ตัวร่วมกับ SO 718381, 719000 (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"
_SHARED_SO_RE = re.compile(r"ร่วมกับ SO ([^()|]+?) \(")


def _merge_row_remark(row) -> str:
    """REMARK ของแถวที่ทาง MERGE เพิ่มเครื่องใหม่เข้าไป — สร้างใหม่จากตัวเลขของแถวทุกครั้ง ห้ามต่อท้าย

    เดิมต่อท้ายข้อความเก่า (เฉพาะแถวที่มี MC_SHARED) → แถวเดียวโดน merge 2 รอบได้
    "…ไม่ต้อง setup + RAO: setup เครื่องใหม่ 2 ตัว … + RAO: setup เครื่องใหม่ 4 ตัว …" และเมลแผนเปลี่ยน
    (_setup_note อ่าน "setup เครื่องใหม่" ตัวแรกที่เจอ) นับเครื่องตั้งใหม่ขาด ;
    ส่วนแถวที่ไม่มี MC_SHARED ไม่ถูกแก้ข้อความเลย → เขียน "carry … (ไม่ต้อง setup)" ทั้งที่
    NEW_MC/SETUP_DAYS มีค่า (เจอจริงในแผนจำลอง 1 ต.ค. 2569 เช่น FD6TECNF47A0 IBLTA W45)

    รูปแบบเดียวกับตอนลูปหลักสร้างแถวใหม่: ส่วนเครื่องของแถวเอง [+ ส่วนที่ใช้ร่วมกับใบอื่น]
    ส่วนท้ายหลัง " | " (เหตุผลที่ระบบส่งจ้างทอ) เก็บไว้ตามเดิม
    """
    def _num(key):
        v = pd.to_numeric(row.get(key, 0), errors="coerce")
        if pd.isna(v):
            return 0
        v = float(v)
        return int(v) if v.is_integer() else v

    head, sep, tail = str(row.get("REMARK", "") or "").strip().partition(" | ")
    mc = str(row.get("MC_GROUP", "") or "").strip()
    carry, new = int(_num("CARRYOVER_MC")), int(_num("NEW_MC"))
    text = _build_plan_remark(mc, carry, new, _num("SETUP_DAYS"), booking_mc=int(_num("MC_BOOKING")))
    shared = int(_num("MC_SHARED"))
    if shared > 0:
        m = _SHARED_SO_RE.search(head)
        who = f" SO {m.group(1).strip()}" if m else "ใบอื่น"
        sh_txt = f"ใช้เครื่อง {shared} ตัวร่วมกับ{who} (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"
        text = f"{text} + {sh_txt}" if (carry > 0 or new > 0) else f"{mc}: {sh_txt}"
    return f"{text}{sep}{tail}" if tail.strip() else text


def _resolve_carry_key(item, mc_group, gauge=None):
    """คืน key ที่เหมาะสุดสำหรับ carryover โดย fallback เป็น item+mc แม้ gauge ไม่ตรง"""
    base_key = _ck(item, mc_group, gauge)
    if (
        base_key in last_production
        or base_key in machines_in_use
        or base_key in last_sc_so_no
    ):
        return base_key

    item_norm, mc_norm, _ = base_key
    best_key = None
    best_score = (-1, -1)
    for cand in set(list(last_production.keys()) + list(machines_in_use.keys())):
        if len(cand) < 2:
            continue
        if cand[0] != item_norm or cand[1] != mc_norm:
            continue
        cand_last = int(last_production.get(cand, -1))
        cand_mc = int(machines_in_use.get(cand, 0) or 0)
        score = (cand_last, cand_mc)
        if score > best_score:
            best_score = score
            best_key = cand

    return best_key if best_key is not None else base_key

def _has_item_mc_key(key_set, key):
    """เช็คว่ามี key ของ item+mc อยู่ใน set แล้วหรือไม่ (ไม่สน gauge)"""
    if not key or len(key) < 2:
        return False
    item_norm, mc_norm = key[0], key[1]
    for cand in key_set:
        if len(cand) < 2:
            continue
        if cand[0] == item_norm and cand[1] == mc_norm:
            return True
    return False


# เฉพาะ item ที่จะวางแผน (จาก orders)
_plan_items = set(orders["Item Code"].astype(str).str.strip().str.upper().dropna().unique())
_detail_mc_plan = detail_mc[detail_mc["ITEM_CODE"].astype(str).str.strip().str.upper().isin(_plan_items)] if "ITEM_CODE" in detail_mc.columns else detail_mc


# สร้าง YARN_USED lookup จาก detail_mc (ITEM_CODE → YARN_USED)
_yarn_used_lookup = {}
_yarn_col_mc = next((c for c in _detail_mc_plan.columns if c.strip().upper() in ("YARN-USED", "YARN_USED")), None)
if _yarn_col_mc and "ITEM_CODE" in _detail_mc_plan.columns:
    for _, _row in (
        _detail_mc_plan[["ITEM_CODE", _yarn_col_mc]]
        .dropna()
        .drop_duplicates("ITEM_CODE")
        .iterrows()
    ):
        _yarn_used_lookup[str(_row["ITEM_CODE"]).strip().upper()] = str(
            _row[_yarn_col_mc]
        ).strip()



# สร้าง MATERIAL_CONTENT lookup จาก detail_mc (ITEM_CODE → MATERIAL_CONTENT)

_material_content_lookup = {}

if "MATERIAL_CONTENT" in _detail_mc_plan.columns and "ITEM_CODE" in _detail_mc_plan.columns:

    for _, _row in (

        _detail_mc_plan[["ITEM_CODE", "MATERIAL_CONTENT"]]

        .dropna()

        .drop_duplicates("ITEM_CODE")

        .iterrows()

    ):

        _v = str(_row["MATERIAL_CONTENT"]).strip()

        if _v and _v.upper() != "NAN":

            _material_content_lookup[str(_row["ITEM_CODE"]).strip().upper()] = _v



# Fallback: เติม item ที่ไม่มีใน detail_mc จาก order_ready (YARN_ITEM)

try:

    _order_ready_df = pd.read_excel(ORDER_FILE)

    _order_ready_df.columns = _order_ready_df.columns.str.strip()

    _yi_col = next((c for c in _order_ready_df.columns if c.strip().upper() == "YARN_ITEM"), None)

    # รหัสผ้าต้องรวม 2 คอลัมน์ทีละแถว (FABRIC_ITEM ก่อน ว่างค่อย ITEM_CODE) ห้ามเลือกคอลัมน์เดียว
    # ⚠ order_ready มีทั้งคู่: แถว SC/booking เก็บรหัสที่ FABRIC_ITEM ส่วน ITEM_CODE ติดมากับ
    # view_scfol_item.xlsx ที่ Order.py อ่านปนเข้ามา (ไม่มีด้าย) และอยู่ก่อนในลำดับคอลัมน์ —
    # เคยเลือก "ตัวแรกที่เจอ" แล้วได้ ITEM_CODE → lookup ว่างทั้งชุด ; งาน booking ที่ Replan ยกมาวางใหม่
    # ไม่เหลือใน detail_mc แล้วจึงพึ่งทางนี้ทางเดียว (แผน 1/10/2569: YARN_USED ว่าง 189 แถว 55 ผ้า
    # + LT_YARN = 0 ตาม ทั้งที่ order_replan มีค่าด้ายให้ครบทุกแถว)
    _ic_cols = [c for _name in ("FABRIC_ITEM", "ITEM_CODE")
                for c in _order_ready_df.columns if c.strip().upper() == _name]

    if _yi_col and _ic_cols:

        for _or in _order_ready_df[_ic_cols + [_yi_col]].itertuples(index=False):

            _yi = str(_or[-1]).strip() if pd.notna(_or[-1]) else ""

            if not _yi or _yi.upper() in ("NAN", "NONE"):

                continue

            _ic = next((str(v).strip().upper() for v in _or[:-1]
                        if pd.notna(v) and str(v).strip().upper() not in ("", "NAN", "NONE")), "")

            if _ic and _ic not in _yarn_used_lookup:

                _yarn_used_lookup[_ic] = _yi

except Exception as _e:

    print(f"⚠️ ไม่สามารถโหลด order_ready สำหรับ YARN-USED fallback: {_e}")



# =========================

# LT_YARN LEAD TIME LOOKUP

# =========================

_LT_YARN_FILE = DATA_DIR / "LT_Yarn" / "fmit_yarn_leadtime.xlsx"

try:

    _lt_yarn_df = pd.read_excel(_LT_YARN_FILE)

    _lt_yarn_df.columns = _lt_yarn_df.columns.str.strip()

    _lt_yarn_df["Yarn Item"] = _lt_yarn_df["Yarn Item"].astype(str).str.strip().str.upper()

    _lt_yarn_df["POIN LT"] = pd.to_numeric(_lt_yarn_df["POIN LT"], errors="coerce")

    _lt_yarn_df["Piority"] = pd.to_numeric(_lt_yarn_df["Piority"], errors="coerce")

    print(f"[LT_YARN] โหลด fmit_yarn_leadtime สำเร็จ: {len(_lt_yarn_df)} rows")

except Exception as _e:

    print(f"⚠️ ไม่สามารถโหลด LT_Yarn: {_e}")

    _lt_yarn_df = pd.DataFrame()





def _resolve_yarn_code(yarn_code: str) -> str:

    """
    แปลง yarn code ที่อาจไม่ตรงกับ LT file ให้ตรงขึ้น
    Rules (ลองตามลำดับ จนกว่าจะ match):
    1. ชื่อเดิม (exact)
    2. Strip YD ก่อนตัวเลข: CMYD→CM, DTYYD→DTY, BCIYD→BCI ฯลฯ
    3. Strip trailing single uppercase color suffix (D/S/L/W/M/H)
    4. ทั้ง rule 2 และ 3 รวมกัน
    5. แปลง RPEM → RPE ท้ายชื่อ
    """
    if _lt_yarn_df.empty:
        return yarn_code
    lt_set = set(_lt_yarn_df["Yarn Item"].dropna().unique())
    if yarn_code in lt_set:
        return yarn_code
    # rule 2: strip YD before digit
    s2 = re.sub(r"YD(?=\d)", "", yarn_code)
    if s2 != yarn_code and s2 in lt_set:
        return s2
    # rule 3: strip trailing single uppercase letter
    s3 = re.sub(r"[A-Z]$", "", yarn_code)
    if s3 != yarn_code and s3 in lt_set:
        return s3
    # rule 4: rule2 + rule3
    s4 = re.sub(r"[A-Z]$", "", s2)
    if s4 != s2 and s4 in lt_set:
        return s4
    # rule 5: RPEM → RPE
    s5 = re.sub(r"RPEM$", "RPE", yarn_code)
    if s5 != yarn_code and s5 in lt_set:
        return s5
    s5b = re.sub(r"RPEM$", "RPE", s2)
    if s5b != s2 and s5b in lt_set:
        return s5b
    # no match found — return original (will result in empty lookup)
    return yarn_code

def get_yarn_lt_days(item_code: str) -> int:
    """
    หา Lead Time (วัน) สูงสุดจาก YARN-USED ของ item นั้น
    Logic:
    - แยก YARN-USED ด้วย '+' → ได้ yarn codes A, B, C, …
    - resolve ชื่อ yarn ให้ตรงกับ LT file
    - สำหรับแต่ละ yarn code:
        * ถ้ามี Piority=1 rows → ใช้ max POIN LT จาก Piority=1 เท่านั้น
        * ถ้าไม่มี Piority=1 → fallback ใช้ max POIN LT จากทุก row
    - เปรียบเทียบค่าตัวแทนของแต่ละ yarn แล้วเลือกค่ามากสุด
    - คืน 0 ถ้าหาไม่พบ
    """

    if _lt_yarn_df.empty:

        return 0

    yarn_used = _yarn_used_lookup.get(str(item_code).strip().upper(), "")

    if not yarn_used or str(yarn_used).strip().upper() in ("", "NAN"):

        return 0

    parts = [p.strip().upper() for p in str(yarn_used).split("+") if p.strip()]

    if not parts:

        return 0

    resolved = [_resolve_yarn_code(p) for p in parts]

    representative_lts = []

    for yarn in resolved:

        yarn_rows = _lt_yarn_df[_lt_yarn_df["Yarn Item"] == yarn]

        if yarn_rows.empty:

            continue

        p1_rows = yarn_rows[yarn_rows["Piority"] == 1]

        if not p1_rows.empty:

            rep_lt = p1_rows["POIN LT"].max()

        else:

            rep_lt = yarn_rows["POIN LT"].max()

        if pd.notna(rep_lt):

            representative_lts.append(rep_lt)

    if not representative_lts:

        return 0

    return int(max(representative_lts))





def get_yarn_lt_earliest_week(item_code: str, date_in=None):

    """

    คืนค่า week number ที่เริ่มวางแผนได้เร็วสุด

    สูตร: max(DATE_IN + POIN_LT, TODAY + 2 weeks)

    คืน None ถ้าคำนวณไม่ได้

    """

    min_start_idx = get_yarn_lt_min_start_idx(item_code, date_in=date_in)

    if min_start_idx < len(calendar_week):

        return int(calendar_week.iloc[min_start_idx]["WEEK"])

    return None





def get_yarn_lt_min_start_idx(item_code: str, date_in=None) -> int:

    """

    คำนวณ minimum start index (row index ใน calendar_week) 

    🔧 DISABLED: ไม่พิจารณา Yarn LT แล้ว - ใช้ TODAY+2 weeks เสมอ

    """

    min_two_weeks_idx = TODAY_IDX + FREEZE_WEEKS

    print(f"[YARN LT] {item_code}: DISABLED - using TODAY+2 weeks (idx {min_two_weeks_idx})")

    return min_two_weeks_idx


def get_point_date_start_idx(point_date) -> int | None:
    """POINT DATE (วันด้ายเข้าครบ) → index สัปดาห์แรกที่เริ่มทอได้

    คอลัมน์ POINT_DATE มีเฉพาะแถวที่ Replan.py ยกมาจาก booking (ดู build_booking_demand)
    → SC ไม่มีค่านี้ = คืน None = ไม่ถูกบังคับ (ใช้พื้น TODAY+2 เหมือนเดิม)

    เริ่มได้ = สัปดาห์ที่ด้ายเข้า + POIN_LAG_WEEKS (หน้า Monitor Yarn ; ค่าเริ่มต้น 1 =
    ด้ายเข้ากลางสัปดาห์ สัปดาห์นั้นทอไม่ได้ เพราะต้องตรวจรับ/ขึ้นด้ายก่อน → เริ่มสัปดาห์ถัดไป)
    ด้ายเข้าไปแล้ว (ก่อนวันนี้) → index ต่ำกว่าพื้น TODAY+2 จุดเรียกจึงไม่ดันอะไร = วางได้เลย
    """
    if point_date is None:
        return None
    _d = pd.to_datetime(point_date, errors="coerce")
    if pd.isna(_d):
        return None
    _d = pd.Timestamp(_d).normalize()
    _rows = calendar_week.index[
        (calendar_week["WEEK_START"] <= _d) & (calendar_week["WEEK_END"] >= _d)
    ]
    if len(_rows) > 0:
        return int(_rows[0]) + POIN_LAG_WEEKS
    # นอกช่วงปฏิทิน: ก่อนปฏิทิน = ด้ายเข้านานแล้ว → ไม่บังคับ ;
    # หลังปฏิทิน = นับจากแถวสุดท้าย (จุดใช้งานจะ clamp ไม่ให้เกินขอบปฏิทินเอง)
    # แถวที่เจอที่นี่จบก่อนวันด้ายเข้า → อย่างน้อย +1 แม้ตั้งรอ 0 สัปดาห์ ไม่งั้นได้สัปดาห์ก่อนด้ายเข้า
    _before = calendar_week.index[calendar_week["WEEK_START"] <= _d]
    if len(_before) == 0:
        return None
    return int(_before[-1]) + max(POIN_LAG_WEEKS, 1)


# ── วันด้ายเข้าครบ (PO IN) ของออร์เดอร์ ────────────────────────────────────
# 1 ออร์เดอร์ใช้ด้ายได้หลายล็อต → ทอได้ก็ต่อเมื่อ "ล็อตที่ไกลสุด" มาถึง จึงยึดวันหลังสุดเสมอ
#
# แหล่งเดียว = PO_IN_DATE ของ booking (View_Booking → AVA_MC ชีท DETAIL → Replan.POINT_DATE)
# เลิกใช้ YARN_EST_DELIVERY / YARNPO แล้ว (วันในวงเล็บ เช่น 'DTY75/72/2[13-AUG-26]')
# เพราะเป็นวันประมาณการที่ไม่ได้เอาไปบังคับการวางจริง — โชว์ไว้เฉย ๆ ทำให้เข้าใจผิดว่า
# แผนวางผิดวันด้ายเข้า  งาน SC ที่ไม่มี booking จึงไม่มีวันด้ายเข้า = วางตาม cap ตามปกติ
_PO_IN_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _po_in_date_last(v):
    """PO_IN_DATE ของ booking ('18/06/2026,24/06/2026') → วันหลังสุด (pd.Timestamp) | NaT

    รับปี พ.ศ. ด้วย (>2400 → ลบ 543) เหมือน _po_first ใน webapp/backend/plan_view.py
    — ปล่อยให้ pd.to_datetime อ่านตรง ๆ ไม่ได้ เพราะปี 2569 เกินขอบ Timestamp"""
    s = str(v or "").strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return pd.NaT
    best = None
    for part in s.split(","):
        m = _PO_IN_DATE_RE.match(part.strip())
        if not m:
            continue
        _d, _mo, _y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if _y > 2400:
            _y -= 543
        try:
            _dt = date(_y, _mo, _d)
        except ValueError:
            continue
        if best is None or _dt > best:
            best = _dt
    return pd.Timestamp(best) if best else pd.NaT


def get_po_in_date(order):
    """วันด้ายเข้าครบของออร์เดอร์ (ล็อตที่ไกลสุด) → pd.Timestamp | pd.NaT

    มีเฉพาะงานที่ยกมาจาก booking (POINT_DATE = PO_IN_DATE จริง) — งาน SC คืน NaT"""
    _pt = pd.to_datetime(order.get("POINT_DATE"), errors="coerce")
    return pd.Timestamp(_pt).normalize() if pd.notna(_pt) else pd.NaT


def get_po_in_week(d):
    """วันด้ายเข้าครบ → สัปดาห์ด้ายเข้า (bare week) ; '' ถ้าไม่มีวัน

    กติกา PO_IN เส้นแบ่งสัปดาห์คือ พฤหัส–พุธ (ขยับจากนิยามโปรเจกต์ ศุกร์–พฤหัส 1 วัน)
    → +4 วันก่อนหา ISO week (นิยามปกติของโปรเจกต์คือ +3)"""
    if d is None or pd.isna(d):
        return ""
    return int((pd.Timestamp(d) + pd.Timedelta(days=4)).isocalendar()[1])


def yarn_stock_note(order) -> str:
    """ป้าย "ใช้ด้ายในสต๊อกแทนการรอ PO IN" ของใบ — Replan รอบสองเขียนลงคอลัมน์ YARN_STOCK
    (YarnStockCover.py ; สวิตช์ KP_YARN_STOCK_ON หน้า Monitor Yarn) ; ว่าง = รอ PO IN ตามปกติ
    ใบที่มีป้ายยังโชว์วันด้ายเข้า (PO_IN_DATE) เหมือนเดิม แค่ไม่ใช้เป็นพื้นของการวาง"""
    s = str(order.get("YARN_STOCK", "") if order is not None else "").strip()
    return "" if s.lower() in ("nan", "none", "nat", "<na>") else s


def yarn_ready_idx(order) -> int | None:
    """YARN_READY (วันแรกของสัปดาห์ที่ด้ายของก้อนนี้มาครบ) → index สัปดาห์ ; ว่าง = ด้ายมีแล้ว ไม่บังคับ
    Replan เขียนให้ก้อนที่ได้ด้ายจาก PO ที่ยังไม่เข้า (YarnStockCover.supply_cap — "วางได้ไม่เกินด้ายที่มี
    = สต๊อก + PO Pending" หน้า Monitor Yarn) ; ค่าเป็นสัปดาห์ที่ใช้ได้แล้ว (รวม lag แล้ว) ไม่ต้องบวกอีก"""
    _d = pd.to_datetime(order.get("YARN_READY") if order is not None else None, errors="coerce")
    if pd.isna(_d):
        return None
    _d = pd.Timestamp(_d).normalize()
    _rows = calendar_week.index[(calendar_week["WEEK_START"] <= _d) & (calendar_week["WEEK_END"] >= _d)]
    if len(_rows) > 0:
        return int(_rows[0])
    _before = calendar_week.index[calendar_week["WEEK_START"] <= _d]
    return int(_before[-1]) + 1 if len(_before) else None


def po_in_fields(order):
    """(วันด้ายเข้าครบ 'dd/mm/yyyy', สัปดาห์ด้ายเข้า) — ('', '') ถ้าไม่รู้วันด้ายเข้า"""
    _d = get_po_in_date(order)
    if pd.isna(_d):
        return "", ""
    return f"{_d:%d/%m/%Y}", get_po_in_week(_d)





def get_setup_days_for_item(material_content: str = "", yarn_used: str = "", mc_group: str = "", item_code: str = "") -> int:

    """

    คำนวณ setup days



    Logic:

    - Factory OMNOI (อ้อมน้อย) → OMNOI_SETUP_DAYS ทุก item เสมอ (ไม่ดู prefix / POLY / DTY)

    - Type SINGLE (เช็คจาก MC_TYPE_MAP[mc_group]) → ใช้ prefix ของ ITEM code จับคู่แบบ

      longest-prefix match จากชีท "Item Setup" (ไม่ดูตรรกะ POLY/DTY เดิม)

      ถ้าไม่ match prefix ใดเลย (รวม prefix ที่เว้นว่าง เช่น F9) → default SETUP_DAYS

    - ไม่ใช่ SINGLE → ตรรกะเดิม:

      0. ถ้า mc_group มีค่าใน _mc_setup_time_map → ใช้ค่านั้น

      1. ถ้า MATERIAL_CONTENT เป็น POLY → 5 วัน

      2. ถ้า YARN_USED มี DTY → 5 วัน (ทุก material รวม COTTON)

      3. default → 3 วัน

    """

    _mc_u = str(mc_group).strip().upper() if mc_group else ""

    # โรงอ้อมน้อย → setup คงที่ทุก item (ด่านแรก ชนะทุกกฎอื่น)
    # MasterMC ไม่มี MC group ที่อยู่ 2 โรงงาน → ระบุ factory จาก mc_group ได้เลย ไม่ต้องใช้ gauge

    if _mc_u and _mc_factory_map.get((_mc_u, ""), "") in ("OMNOI", "OM"):

        return OMNOI_SETUP_DAYS

    # เฉพาะ Type SINGLE → ใช้ prefix ของ ITEM (ไม่ดูตรรกะเดิม)
    # ครอบคลุม "SINGLE", "SINGLE (TUBE)" และเผื่อสะกด "SINGEL"

    _mc_type = MC_TYPE_MAP.get(_mc_u, "").replace("SINGEL", "SINGLE")

    if _mc_u and _mc_type.startswith("SINGLE"):

        _pfx_days = _lookup_item_prefix_setup(item_code)

        return _pfx_days if _pfx_days is not None else SETUP_DAYS

    if mc_group:

        if _mc_u in _mc_setup_time_map:

            return _mc_setup_time_map[_mc_u]

    mat = str(material_content).strip().upper() if material_content else ""

    yarn = str(yarn_used).strip().upper() if yarn_used else ""



    if "POLY" in mat and "COTTON" not in mat:

        return SETUP_DAYS_HEAVY

    if "DTY" in yarn:

        return SETUP_DAYS_HEAVY

    return SETUP_DAYS





def _normalize_capacity(item_code: str, mc_group: str, original_cap: float, gauge: str = None) -> float:

    """

    Normalize capacity to ensure consistency across MC_GROUPS

    แปลง CAP ทอ จากฐาน 24 ชั่วโมง → ชั่วโมงทำงานจริงของกลุ่ม

    ปกติ 20 ชม./วัน · ถ้าตั้ง WORK_HOUR override ในชีท Work Day ใช้ค่านั้น

    """

    return original_cap * (_base_work_hours(mc_group, gauge) / 24)





def get_load_balanced_machine(

    item_code,

    plan_week,

    last_production,

    required_machines_info=None,

    urgent_mode=False,

    past_rdd=False,

    force_max_mc=False,

    qty_left=0,

    daily_capacity=0,

    progressive_plan=None,

    current_machines=1,  # จำนวนเครื่องปัจจุบันสำหรับกรณี carryover

    qty_left_current_fg=0,  # qty_left เฉพาะ FG ปัจจุบัน (ไม่รวม FG ถัดไป) สำหรับ bypass check

):

    """

    Load Balancing แบบ Gradual Increase พร้อม Validate:

    1. ใช้เครื่องที่วิ่งจริงเป็นฐาน (carryover baseline)

    2. เพิ่มเครื่อง setup ได้สูงสุด +2 จากฐาน

    3. Validate ทุกครั้งก่อนจองเครื่อง

    """

    # ถ้าปิด Load Balancing ให้ใช้ฟังก์ชันเดิม

    if not USE_LOAD_BALANCING:

        return get_best_machine_for_item(

            item_code,

            plan_week,

            last_production,

            required_machines_info,

            urgent_mode,

            past_rdd,

            force_max_mc,

        )

    # ใช้ logic เดิมในการหาเครื่องที่เหมาะสม

    mc_group, daily_cap, setup_needed, available_machines, item_gauge = get_best_machine_for_item(

        item_code,

        plan_week,

        last_production,

        required_machines_info,

        urgent_mode,

        past_rdd,

        force_max_mc,

    )

    

    if mc_group is None:

        return None, None, None, None, None

    # ถ้า get_best_machine_for_item ทำ cylinder change เพิ่งสำเร็จ → bypass load balancing ทั้งหมด
    # เพราะเครื่อง cylinder change เป็นเครื่องใหม่ที่สร้างมาเพื่อ item นี้โดยเฉพาะ
    _cyl_trigger_for_check = _TL.shift(plan_week, -1)  # สัปดาห์ก่อนหน้าบนปฏิทิน (YW ลบ 1 ข้ามปีไม่ได้)
    _cyl_lb_key = (_cyl_trigger_for_check, str(item_code).strip().upper(), str(mc_group).strip().upper())
    if _cyl_lb_key in _cylinder_change_for_item:
        return mc_group, daily_cap, setup_needed, available_machines, item_gauge


    # ฐานจำนวนเครื่องที่ใช้งานจริงในสัปดาห์นี้ (carryover) เพื่อใช้เป็น baseline +2

    # ALWAYS ใช้ current_machines จริง ถึงแม้ว่า setup_needed=True

    # เพราะ carry machines คือเครื่องที่วิ่งจริง ไม่ว่าจะมี gap หรือไม่

    carryover_base_machines = 0

    try:

        if current_machines is not None:

            carryover_base_machines = max(0, int(current_machines))

    except (ValueError, TypeError):

        carryover_base_machines = 0

    

    # ค่า qty สำหรับใช้ตรวจสอบ weeks_needed: ใช้ current FG qty (ไม่ inflate จาก FG อื่น)

    # เพื่อป้องกันการอนุมัติเพิ่มเครื่องเมื่อ FG ปัจจุบันมี qty เหลือน้อย

    _actual_fg_qty = qty_left_current_fg if qty_left_current_fg > 0 else qty_left



    # 🔧 Bypass Load Balancing restrictions when required_machines_info specifies higher machine count

    # This ensures that when the system calculates more machines are needed, it uses that value

    if required_machines_info and len(required_machines_info) > 2:

        required_mc = required_machines_info[2]  # index 2 = required_machines

        if required_mc > available_machines:

            print(f"   ⚠️ Required machines {required_mc} > available {available_machines}, using available")

            required_mc = available_machines

        if required_mc > carryover_base_machines:

            # 🔧 FIX: ตรวจสอบว่า current FG qty เพียงพอให้ required_mc ทำงานต่อเนื่องใน W+1 หรือไม่

            # ถ้า qty_left_current_fg < required_mc × 5 days × daily_cap → W33 จะผลิตจบหมดแทบหมดใน week เดียว

            # แปลว่า W34 จะเหลือ qty น้อยมาก → ไม่ควรเพิ่มเครื่องใน W33 (setup cost ไม่คุ้ม)

            _can_bypass = True

            if _actual_fg_qty > 0 and daily_cap > 0:

                try:

                    _bp_wk = int(plan_week[1:]) if isinstance(plan_week, str) and plan_week.startswith('W') else int(plan_week)

                    _est_this_wk = required_mc * 5 * daily_cap

                    _est_remaining_next = max(0.0, _actual_fg_qty - _est_this_wk)

                    _est_next_mc = int(_est_remaining_next / (5 * daily_cap)) + (1 if _est_remaining_next % (5 * daily_cap) > 0 else 0)

                    if _est_next_mc < required_mc:

                        _can_bypass = False

                        print(f"   ⚠️ Bypass blocked: current FG qty={_actual_fg_qty:.0f}, after W{_bp_wk} est. remaining={_est_remaining_next:.0f} → W+1 needs {_est_next_mc} < {required_mc} mc → ไม่เพิ่มเครื่อง (setup cost ไม่คุ้ม)")

                except Exception:

                    pass

            if _can_bypass:

                # week แรก (carryover=0): ห้าม setup เกิน 2 เครื่อง
                _bypass_new_setup_cap = carryover_base_machines + 2
                if required_mc > _bypass_new_setup_cap:
                    print(f"   🔒 Bypass new-setup cap: {required_mc} → {_bypass_new_setup_cap} (carryover {carryover_base_machines} + 2)")
                    required_mc = _bypass_new_setup_cap

                print(f"   ✅ Bypass Load Balancing: Using required_machines_info {required_mc} machines (instead of gradual increase)")

                return mc_group, daily_cap, setup_needed, required_mc, item_gauge



    # Gradual Load Balancing Logic

    # แปลง plan_week เป็นตัวเลข (ถ้าเป็น "23" ให้ได้ 23, ถ้าเป็น "W23" ให้ได้ 23)

    if isinstance(plan_week, str):

        if plan_week.startswith('W'):

            current_week_num = int(plan_week[1:])

        else:

            current_week_num = int(plan_week)

    else:

        current_week_num = int(plan_week)

    

    # หาสัปดาห์ล่าสุดที่ item นี้ใช้งาน MC_GROUP นี้ (ไม่ใช่ MC_GROUP ทั้งหมด)

    # เช็คจาก plans ที่ append ไปแล้ว

    item_last_week = -1

    for plan_entry in plans:
        plan_item = plan_entry.get('ITEM_CODE', '')
        plan_mc = plan_entry.get('MC_GROUP', '')
        plan_week_value = plan_entry.get('PLAN_WEEK', '')

        if plan_item == item_code and plan_mc == mc_group:
            try:
                if isinstance(plan_week_value, str):
                    if plan_week_value.startswith('W'):
                        plan_week_num = int(plan_week_value[1:])
                    else:
                        plan_week_num = int(plan_week_value)
                else:
                    plan_week_num = int(plan_week_value)

                if plan_week_num < current_week_num and plan_week_num > item_last_week:
                    item_last_week = plan_week_num
            except (ValueError, TypeError):
                continue

    # เช็คจาก progressive_plan ด้วย (สำหรับ carryover items)
    # progressive_plan มี format: {week_str: machines}
    if progressive_plan:
        for week_str in progressive_plan.keys():
            try:
                if isinstance(week_str, str):
                    if week_str.startswith('W'):
                        prog_week_num = int(week_str[1:])
                    else:
                        prog_week_num = int(week_str)
                else:
                    prog_week_num = int(week_str)
            
                if prog_week_num < current_week_num and prog_week_num > item_last_week:
                    item_last_week = prog_week_num

            except (ValueError, TypeError):

                continue

    

    # หาจำนวนเครื่องรวมที่ใช้จริงในสัปดาห์ก่อนหน้าสำหรับ MC Group เดียวกัน

    # ใช้ weekly_mc_usage ที่เก็บ total machines per (week, mc_group)

    previous_week_mc_usage = {}

    for (week_idx, mc_grp), total_machines in weekly_mc_usage.items():

        # แปลง week index เป็นตัวเลขก่อนเปรียบเทียบ

        try:

            week_num = int(week_idx) if isinstance(week_idx, str) else week_idx

            

            if mc_grp == mc_group and week_num < current_week_num:

                # หาสัปดาห์ล่าสุดที่ใช้ MC Group นี้

                if week_num not in previous_week_mc_usage or week_num > previous_week_mc_usage.get('latest_week', -1):

                    previous_week_mc_usage['latest_week'] = week_num

                    previous_week_mc_usage['machines_used'] = total_machines

        except (ValueError, TypeError):

            # ข้ามถ้าไม่สามารถแปลง week index เป็นตัวเลขได้

            continue

    

    # คำนวณจำนวนเครื่องที่ควรใช้ในสัปดาห์นี้ (Gradual Increase)

    max_allowed_machines = available_machines

    is_first_time_setup = False

    first_time_cap = 0

    

    print(f"🔍 Load Balancing Debug for {mc_group}:")

    print(f"   - Available machines: {available_machines}")

    print(f"   - Current week: {current_week_num} ({plan_week})")

    print(f"   - Item last week: {item_last_week if item_last_week > 0 else 'None'}")

    print(f"   - Carryover base machines: {carryover_base_machines}")

    print(f"   - Previous week usage (MC_GROUP): {previous_week_mc_usage}")

    

    # 🔧 เช็คจำนวนเครื่องรวมที่ใช้ในสัปดาห์ปัจจุบันแล้ว (จาก SC อื่นที่ประมวลผลไปแล้ว)

    current_week_key = (current_week_num, mc_group)

    current_week_total_machines = weekly_mc_usage.get(current_week_key, 0)

    print(f"   - Current week total machines (so far): {current_week_total_machines}")

    

    # ถ้าสัปดาห์ปัจจุบันมีเครื่องใช้แล้ว ต้องนับรวมด้วย

    if current_week_total_machines > 0:

        print(f"   ⚠️ สัปดาห์ปัจจุบันมี SC อื่นใช้ไปแล้ว {current_week_total_machines} เครื่อง")

    

    # 🔧 ใช้ item_last_week แทน previous_week_mc_usage สำหรับการตรวจสอบ gap

    # เพราะต้องเช็ค gap จากการใช้งานของ item นี้โดยเฉพาะ ไม่ใช่ MC_GROUP ทั้งหมด

    if item_last_week > 0:

        # Item นี้เคยใช้งาน MC_GROUP นี้มาก่อน

        previous_week_num = item_last_week

        previous_week_str = f"W{previous_week_num:02d}"

        

        # 🔧 เช็คว่า previous week ห่างจาก current week มากกว่า 1 สัปดาห์หรือไม่

        # ถ้าห่างมากกว่า 1 สัปดาห์ → ถือว่าเป็น "first time use" ใหม่

        # ระยะห่างต้องวัดบน "ตำแหน่งในปฏิทิน" ไม่ใช่ลบเลขสัปดาห์ตรง ๆ
        # ข้ามปีเมื่อไหร่การลบเลขจะพัง: W1 ต่อจาก W52 ได้ 1-52 = -51 (ที่ถูกคือ 1)
        # → ระบบจะคิดว่างานห่างกันเป็นปี แล้วสั่ง setup เครื่องใหม่ทั้งที่ควรรันต่อเนื่อง
        _cur_i, _prev_i = week_index(current_week_num), week_index(previous_week_num)
        week_gap = (_cur_i - _prev_i) if (_cur_i is not None and _prev_i is not None) \
            else (current_week_num - previous_week_num)

        

        # 🔧 หาจำนวนเครื่องที่ ITEM นี้ใช้จริง (รวมทุก SC ของ item เดียวกัน)

        # ดูย้อนหลังสูงสุด 3 สัปดาห์จาก item_last_week เพื่อหา peak

        # ป้องกันกรณี SC เปลี่ยน ทำให้สัปดาห์สุดท้ายเครื่องน้อยกว่าปกติ

        item_prev_mc = 0

        _lookback_detail = {}

        for lookback in range(0, 3):

            check_week = item_last_week - lookback

            if check_week <= 0:

                break

            week_sum = 0

            for pe in plans:

                pi = pe.get('ITEM_CODE', '')

                pm = pe.get('MC_GROUP', '')

                pw = pe.get('PLAN_WEEK', '')

                try:

                    if isinstance(pw, str):

                        pwn = int(pw[1:]) if pw.startswith('W') else int(pw)

                    else:

                        pwn = int(pw)

                    if pi == item_code and pm == mc_group and pwn == check_week:

                        week_sum += pe.get('ACTUAL_MC', 0)

                except (ValueError, TypeError):

                    continue

            if week_sum > 0:

                _lookback_detail[check_week] = week_sum

                item_prev_mc = max(item_prev_mc, week_sum)

            else:

                break  # หยุดถ้าไม่มีข้อมูลในสัปดาห์นี้ (gap)

        # fallback: ลองหาจาก progressive_plan

        if item_prev_mc == 0 and progressive_plan:

            for k in [str(item_last_week), f"W{item_last_week}"]:

                if k in progressive_plan:

                    item_prev_mc = progressive_plan[k]

                    break

        if item_prev_mc == 0:

            item_prev_mc = 1

        previous_machines = item_prev_mc

        print(f"   - Item's own machines (peak of recent weeks): {previous_machines} {_lookback_detail}")

        

        # ประกาศตัวแปรที่จะใช้ใน loop

        next_week_idx = current_week_num + 1

        next_week_str = f"W{next_week_idx:02d}"

        next_week_machines_planned = 0

        next_week_actual_remain = 0

        order_will_continue = False

        next_week_continuity_limit = 0

        try:

            next_week_actual_remain = get_actual_mc_remain(mc_group, next_week_idx, gauge=item_gauge, item_code=item_code)

        except Exception:

            next_week_actual_remain = 0

        print(f"   - {next_week_str} machine remain จริง = {next_week_actual_remain}")



        _next_week_prog_key = None

        if progressive_plan and next_week_str in progressive_plan:

            _next_week_prog_key = next_week_str

        elif progressive_plan and str(next_week_idx) in progressive_plan:

            _next_week_prog_key = str(next_week_idx)

        if _next_week_prog_key is not None:

            order_will_continue = True

            next_week_machines_planned = progressive_plan[_next_week_prog_key]

            try:

                next_week_continuity_limit = int(next_week_machines_planned)

            except (ValueError, TypeError):

                next_week_continuity_limit = 0

            print(f"   - {next_week_str} order นี้จะใช้ต่อ {next_week_machines_planned} เครื่อง (จาก progressive_plan:{_next_week_prog_key})")

        else:

            next_week_machines_planned = 0

            for plan_entry in plans:

                plan_week_value = plan_entry.get('PLAN_WEEK', '')

                plan_mc_group = plan_entry.get('MC_GROUP', '')



                try:

                    plan_week_str = str(plan_week_value)

                    if plan_week_str.startswith('W'):

                        plan_week_num = int(plan_week_str[1:])

                    else:

                        plan_week_num = int(plan_week_value)



                    if plan_mc_group == mc_group and plan_week_num == next_week_idx:

                        next_week_machines_planned += plan_entry.get('ACTUAL_MC', 0)

                except (ValueError, TypeError):

                    continue

            print(f"   - {next_week_str} จะใช้ {next_week_machines_planned} เครื่อง (จาก plans)")

        

        if week_gap > 1:

            print(f"   ⚠️ Previous week {previous_week_str} ห่างจาก current week {week_gap} สัปดาห์ → ถือว่าเป็น first time use ใหม่")

            baseline = carryover_base_machines

            gap_start_cap = min(baseline + 2, available_machines)

            print(f"📝 First time using {mc_group} (after gap) - baseline {baseline} + setup <= 2 → max {gap_start_cap} machines")

            # กรณี after-gap ให้ยึดเครื่องที่วิ่งจริงเป็น baseline และเพิ่ม setup ได้สูงสุด +2

            max_allowed_machines = gap_start_cap

            # ข้ามการลองเพิ่มเครื่อง เพราะเป็นสัปดาห์แรก

        else:

            # week_gap == 1: item ใช้งานต่อเนื่อง → เพิ่มได้สูงสุด +2 จาก item's own previous

            print(f"   - Previous week {previous_week_str}: item ใช้ {previous_machines} เครื่อง → max +2 = {previous_machines + 2}")

            

            # ลองเพิ่มทีละ 1 เครื่อง: ตรวจสอบว่าควรเพิ่ม +1 หรือ +2

            max_allowed_machines = previous_machines  # เริ่มต้นที่จำนวนเดิม

        

        # สำหรับกรณี carryover ที่มีเครื่องว่างและ order ยังเหลือเยอะ → ให้เพิ่มเครื่องได้

        if current_machines > 1 and _actual_fg_qty > 0:

            print(f"   - Carryover mode: มี {current_machines} เครื่องอยู่แล้ว, order เหลือ {_actual_fg_qty:.0f} units (current FG)")

            print(f"   - Daily capacity: {daily_capacity}, Available machines: {available_machines}")

            # เริ่มจากเครื่องที่มีอยู่แล้วและตรวจสอบการเพิ่ม

            max_allowed_machines = current_machines

            

            # ถ้า order เหลือเยอะและมีเครื่องว่าง → ลองเพิ่มเครื่องทันที

            if daily_capacity > 0 and _actual_fg_qty > daily_capacity * 5:  # เหลือมากกว่า 1 สัปดาห์

                for increment in [1, 2]:

                    test_machines = max_allowed_machines + increment

                    if test_machines <= available_machines:

                        if next_week_actual_remain < test_machines:

                            print(f"   ❌ Hard gate (W+1): {next_week_str} machine remain {next_week_actual_remain} < {test_machines} เครื่อง → ไม่อนุมัติเพิ่มใน W{current_week_num}")

                            break

                        if order_will_continue and next_week_machines_planned < test_machines:

                            print(f"   ❌ Hard gate (W+1): {next_week_str} ต่อเนื่องได้แค่ {next_week_machines_planned} < {test_machines} เครื่อง → ไม่อนุมัติเพิ่มใน W{current_week_num}")

                            break

                        if next_week_continuity_limit > 0 and next_week_continuity_limit < test_machines:

                            print(f"   ❌ Hard gate (W+1): {next_week_str} continuity limit {next_week_continuity_limit} < {test_machines} เครื่อง → ไม่อนุมัติเพิ่มใน W{current_week_num}")

                            break

                        weeks_needed = _actual_fg_qty / (test_machines * daily_capacity * 5) if test_machines > 0 else 999

                        if weeks_needed >= 2:  

                            print(f"   ✅ อนุมัติ (carryover): FG qty เหลือ {_actual_fg_qty:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) → เพิ่มเป็น {test_machines} เครื่อง")

                            max_allowed_machines = test_machines

                            break

                        else:

                            print(f"   ❌ ไม่อนุมัติ (carryover): FG qty เหลือ {_actual_fg_qty:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) ต้องใช้อย่างน้อย 2 สัปดาห์")

            else:

                print(f"   - ไม่เพิ่มเครื่อง: daily_capacity={daily_capacity}, qty_left={qty_left}, threshold={daily_capacity * 5 if daily_capacity > 0 else 0}")

        

        for increment in [1, 2]:

            test_machines = max_allowed_machines + increment

            if test_machines > available_machines:

                break  # เกินเครื่องที่มี

            if next_week_actual_remain < test_machines:

                print(f"   ❌ Hard gate (W+1): {next_week_str} machine remain {next_week_actual_remain} < {test_machines} เครื่อง → ไม่อนุมัติเพิ่มใน W{current_week_num}")

                break

            if order_will_continue and next_week_machines_planned < test_machines:

                print(f"   ❌ Hard gate (W+1): {next_week_str} ต่อเนื่องได้แค่ {next_week_machines_planned} < {test_machines} เครื่อง → ไม่อนุมัติเพิ่มใน W{current_week_num}")

                break

            if next_week_continuity_limit > 0 and next_week_continuity_limit < test_machines:

                print(f"   ❌ Hard gate (W+1): {next_week_str} continuity limit {next_week_continuity_limit} < {test_machines} เครื่อง → ไม่อนุมัติเพิ่มใน W{current_week_num}")

                break

            

            print(f"   🔍 ลองเพิ่มเป็น {test_machines} เครื่อง (+{increment}): {next_week_str} ใช้ {next_week_machines_planned} เครื่อง")

            

            # เงื่อนไขการอนุมัติ:

            # 1. Week ถัดไปจะใช้เครื่องเต็มตามจำนวนที่เพิ่ม (มี orders ใหม่)

            # 2. หรือ week ถัดไปยังเป็น order เดียวกันที่ carryover ต่อ (ใช้เครื่องเพิ่มได้)

            # 3. หรือ order ปัจจุบันยังเหลือ qty มากพอที่จะใช้เครื่องเพิ่มได้

            

            # 🔧 กฎใหม่: ตรวจสอบว่าจะใช้เครื่องครบอย่างน้อย 2 สัปดาห์หรือไม่

            # คำนวณจำนวนสัปดาห์ที่จะใช้เครื่องจำนวนนี้

            weeks_will_use = 0

            if _actual_fg_qty > 0 and daily_capacity > 0:

                weeks_will_use = _actual_fg_qty / (test_machines * daily_capacity * 5) if test_machines > 0 else 0

            

            # เช็คว่า week ถัดไปจะใช้เครื่องเต็มหรือไม่

            if next_week_machines_planned >= test_machines:

                # ต้องเช็คว่าจะใช้อย่างน้อย 2 สัปดาห์หรือไม่

                if weeks_will_use >= 2:

                    print(f"   ✅ อนุมัติ: {next_week_str} จะใช้เต็ม {test_machines} เครื่อง (จะใช้ ~{weeks_will_use:.1f} weeks)")

                    max_allowed_machines = test_machines

                else:

                    print(f"   ❌ ไม่อนุมัติ: จะใช้แค่ {weeks_will_use:.1f} weeks ที่ {test_machines} เครื่อง ต้องใช้อย่างน้อย 2 สัปดาห์")

                    break

            elif next_week_machines_planned > 0 and next_week_machines_planned >= max_allowed_machines:

                # Week ถัดไปยังใช้เครื่องอยู่ (carryover) และไม่น้อยกว่าเดิม

                # แสดงว่า order ยังไม่เสร็จ สามารถเพิ่มเครื่องเพื่อทำให้เสร็จเร็วขึ้นได้

                # ต้องเช็คว่าจะใช้อย่างน้อย 2 สัปดาห์หรือไม่

                if weeks_will_use >= 2:

                    print(f"   ✅ อนุมัติ: {next_week_str} ยังมี order ต่อเนื่อง ({next_week_machines_planned} เครื่อง) → เพิ่มเป็น {test_machines} เพื่อเร่งผลิต (จะใช้ ~{weeks_will_use:.1f} weeks)")

                    max_allowed_machines = test_machines

                else:

                    print(f"   ❌ ไม่อนุมัติ: จะใช้แค่ {weeks_will_use:.1f} weeks ต้องใช้อย่างน้อย 2 สัปดาห์")

                    break

            elif _actual_fg_qty > 0 and daily_capacity > 0:

                # เช็คว่า order ยังเหลือ qty มากพอที่จะใช้เครื่องเพิ่มได้หรือไม่

                # ถ้า order ยังไม่เสร็จและมีเครื่องว่าง → อนุมัติให้เพิ่ม

                # 🔧 กฎใหม่: ต้องใช้เครื่องครบอย่างน้อย 2 สัปดาห์ติดต่อกัน (ป้องกัน 2-3-2)

                weeks_needed = _actual_fg_qty / (test_machines * daily_capacity * 5) if test_machines > 0 else 999

                if weeks_needed >= 2:  # ต้องใช้อย่างน้อย 2 สัปดาห์เต็ม

                    print(f"   ✅ อนุมัติ: FG qty เหลือ {_actual_fg_qty:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) → เพิ่มเป็น {test_machines} เพื่อเร่งผลิต")

                    max_allowed_machines = test_machines

                else:

                    print(f"   ❌ ไม่อนุมัติ: FG qty เหลือ {_actual_fg_qty:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) ต้องใช้อย่างน้อย 2 สัปดาห์")

                    break

            else:

                print(f"   ❌ ไม่อนุมัติ: {next_week_str} จะใช้แค่ {next_week_machines_planned}/{test_machines} เครื่อง")

                break  # หยุดลองเพิ่ม

        

        # 🔧 Hard cap: บังคับทุก item ห้ามเพิ่มเกินกฎ

        if week_gap > 1:

            # First time after gap → baseline (เครื่องที่วิ่งจริง) + setup สูงสุด 2

            hard_cap = min(carryover_base_machines + 2, available_machines)

            if max_allowed_machines > hard_cap:

                print(f"   ⚠️ Hard cap (gap>{week_gap}): {max_allowed_machines} → {hard_cap} (carryover {carryover_base_machines} + 2)")

                max_allowed_machines = hard_cap

        else:

            # week_gap == 1 → ห้ามเพิ่มเกิน +2 จากฐานเครื่องที่ใช้งานจริง

            growth_base = carryover_base_machines if carryover_base_machines > 0 else previous_machines

            hard_cap = min(growth_base + 2, available_machines)

            if max_allowed_machines > hard_cap:

                print(f"   ⚠️ Hard cap: {max_allowed_machines} → {hard_cap} (base {growth_base} + 2)")

                max_allowed_machines = hard_cap

        

        print(f"   - Approved machines: {max_allowed_machines}")

    else:

        is_first_time_setup = True

        baseline = carryover_base_machines

        first_time_cap = min(baseline + 2, available_machines)

        next_week_idx = current_week_num + 1

        try:

            next_week_actual_remain = get_actual_mc_remain(mc_group, next_week_idx, gauge=item_gauge, item_code=item_code)

        except Exception:
            next_week_actual_remain = 0

        if first_time_cap > next_week_actual_remain:

            print(f"   ❌ Hard gate (W+1): W{next_week_idx:02d} machine remain {next_week_actual_remain} < {first_time_cap} เครื่อง → จำกัดเครื่องสัปดาห์นี้")

            first_time_cap = next_week_actual_remain

        print(f"📝 First time using {mc_group} - baseline {baseline} + setup <= 2 → max {first_time_cap} machines")

        # สัปดาห์แรกของ item นี้: อนุญาต setup เพิ่มได้สูงสุด +2 จากเครื่องที่วิ่งจริง

        max_allowed_machines = first_time_cap

    

    print(f"   - Final max allowed machines: {max_allowed_machines}")

    

    # ตรวจสอบว่าจะทำงานได้เต็มที่หรือไม่

    if required_machines_info and len(required_machines_info) > 0:

        # required_machines_info is a tuple: (mc_group, daily_cap, required_machines, feasible, gauge)

        # ใช้ required_machines เป็น target สำหรับ Gradual Increase

        required_mc = required_machines_info[2]  # index 2 = required_machines

        if required_mc > max_allowed_machines:

            # target เยอะกว่าที่มี → ใช้ required_mc เป็น target (เพิ่มเครื่องให้ได้)

            max_allowed_machines = required_mc

            print(f"[DEBUG LB] Using required_machines_info: {required_mc} machines as target (increase from {max_allowed_machines})")

        else:

            # target น้อยกว่าที่มี → จำกัดที่ required_mc

            max_allowed_machines = required_mc

            print(f"[DEBUG LB] Using required_machines_info: {required_mc} machines as target")

    # Hard cap สัปดาห์แรก: ห้าม setup เกิน first_time_cap (baseline + 2)
    if is_first_time_setup and first_time_cap > 0 and max_allowed_machines > first_time_cap:
        print(f"   🔒 First-week hard cap: {max_allowed_machines} → {first_time_cap} (new setup ห้ามเกิน 2 เครื่อง)")
        max_allowed_machines = first_time_cap

    # Validate ก่อนจองเครื่อง

    actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge, item_code=item_code)

    

    # ตรวจสอบว่ามีเครื่องว่างพอตามที่กำหนดหรือไม่ — ใช้ actual_remain เสมอ

    if actual_remain < max_allowed_machines:

        print(f"⚠️ Load Balancing Validation: {mc_group} มีเครื่องว่างแค่ {actual_remain} แต่ต้องการ {max_allowed_machines}")

        max_allowed_machines = actual_remain



    # ตรวจสอบ job capacity limit เสมอ

    type_used = get_type_used_jobs(plan_week, mc_group)

    # เครื่องที่ยังอุ่น (get_best_machine_for_item บอก setup_needed=False) ไม่กินโควตา job — ส่งเป็น
    # committed_carryover แบบเดียวกับที่ get_best_machine_for_item เช็คเอง ; เดิมเช็คซ้ำตรงนี้โดยไม่ส่ง
    # → สัปดาห์ที่โควตา setup เต็ม งานที่ทอต่อบนเครื่องอุ่นถูกคืน None แล้วข้ามสัปดาห์เงียบ ๆ
    # (เคส 29 ก.ย. 2569 FD6DRTJC30/1A0 W42: เครื่อง KP W41 อุ่นอยู่ 1 ตัว แต่ไม่ได้วาง)
    _lb_committed = (
        min(int(carryover_base_machines or 0), int(max_allowed_machines or 0))
        if not setup_needed else 0
    )
    final_available = check_job_capacity_limit(

        mc_group, max_allowed_machines, urgent_mode, type_used,

        committed_carryover=_lb_committed,

    )

    

    if final_available > 0:

        print(f"✅ Load Balancing: {mc_group} ใช้ {final_available} เครื่อง (Gradual Increase)")

        print("   ℹ️ Deferred weekly_mc_usage update until plan commit to avoid duplicate-count drift")

        

        return mc_group, daily_cap, setup_needed, final_available, item_gauge

    

    return None, None, None, None, None





def _analyze_and_balance_load(plan_week, current_plan):

    """

    วิเคราะห์ความสมดุลของ ACTUAL_MC ในสัปดาห์ที่กำหนดและปรับจำนวนเครื่องให้สมดุล

    โดยการปรับจำนวนเครื่องที่ใช้จริงในแต่ละ MC_GROUP

    """

    if not USE_LOAD_BALANCING:

        return current_plan

    

    print(f"🔍 Analyzing and balancing ACTUAL_MC for week {plan_week}...")

    

    # สร้างข้อมูลการใช้งานเครื่องจักรปัจจุบันจาก current_plan

    mc_load = {}

    mc_actual = {}

    for row in current_plan:

        mc_group = row['MC_GROUP']

        # ใช้ REQUIRED_MC แทน ALLOCATED_MC ตามข้อมูลจริง

        allocated_machines = row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1))

        if isinstance(allocated_machines, str):

            allocated_machines = float(allocated_machines.replace(',', ''))

        allocated_machines = float(allocated_machines)

        

        if mc_group not in mc_load:

            mc_load[mc_group] = 0

            mc_actual[mc_group] = []

        mc_load[mc_group] += allocated_machines

        mc_actual[mc_group].append(row)



    # หาค่าเฉลี่ยโหลดต่อเครื่องจักรในสัปดาห์นั้นๆ

    if not mc_load:

        print(f"ℹ️ No machine load to balance for week {plan_week}.")

        return current_plan



    total_allocated_mc = sum(mc_load.values())

    avg_load_per_mc = total_allocated_mc / len(mc_load)

    print(f"  Average ACTUAL_MC load for week {plan_week}: {avg_load_per_mc:.2f}")

    print(f"  Current ACTUAL_MC distribution: {mc_load}")



    # ระบุเครื่องจักรที่มีโหลดสูงและต่ำ

    hot_spots = {mc: load for mc, load in mc_load.items() 

                 if load > avg_load_per_mc * (1 + LOAD_BALANCING_THRESHOLD)}

    cold_spots = {mc: load for mc, load in mc_load.items() 

                  if load < avg_load_per_mc * (1 - LOAD_BALANCING_THRESHOLD)}



    if not hot_spots and not cold_spots:

        print(f"✅ ACTUAL_MC load for week {plan_week} is already balanced within {LOAD_BALANCING_THRESHOLD*100:.0f}% threshold.")

        return current_plan



    print(f"  Hot spots (> {avg_load_per_mc * (1 + LOAD_BALANCING_THRESHOLD):.2f}): {hot_spots}")

    print(f"  Cold spots (< {avg_load_per_mc * (1 - LOAD_BALANCING_THRESHOLD):.2f}): {cold_spots}")



    # ปรับจำนวน ACTUAL_MC ให้สมดุล

    new_plan = list(current_plan)  # ทำสำเนาเพื่อแก้ไข

    changes_made = False



    # คำนวณจำนวนเครื่องที่ควรมีในแต่ละ MC_GROUP เพื่อความสมดุล

    target_mc_per_group = avg_load_per_mc



    # ปรับ hot spots ให้ลดลง

    for hot_mc, hot_load in sorted(hot_spots.items(), key=lambda x: x[1], reverse=True):

        if hot_mc in mc_actual:

            excess_load = hot_load - target_mc_per_group

            if excess_load > 0.1:  # ปรับเฉพาะที่เกินเกินมากพอ

                # กระจาย excess_load ไปยัง cold spots

                excess_to_distribute = excess_load * 0.5  # กระจาย 50% ของ excess

                

                # หา cold spots ที่สามารถรับเพิ่มได้

                available_cold_spots = []

                for cold_mc, cold_load in cold_spots.items():

                    if cold_mc in mc_actual:

                        capacity_for_increase = target_mc_per_group - cold_load

                        if capacity_for_increase > 0.1:

                            available_cold_spots.append((cold_mc, capacity_for_increase))

                

                if available_cold_spots:

                    # กระจาย excess ไปยัง cold spots ตามสัดส่วน

                    total_capacity = sum(cap for _, cap in available_cold_spots)

                    

                    for cold_mc, capacity in available_cold_spots:

                        portion = (capacity / total_capacity) * excess_to_distribute

                        if portion > 0.01:  # ปรับเฉพาะที่มีความเปลี่ยนแปลงมากพอ

                            # ปรับจำนวนเครื่องใน cold_mc

                            cold_rows = mc_actual[cold_mc]

                            total_current_cold = sum(float(r.get('REQUIRED_MC', r.get('ALLOCATED_MC', 1))) for r in cold_rows)

                            

                            # กระจายการเพิ่มไปยัง rows ต่างๆ ตามสัดส่วน

                            for row in cold_rows:

                                current_mc = float(row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1)))

                                if total_current_cold > 0:

                                    increase_ratio = portion / total_current_cold

                                    new_mc = current_mc * (1 + increase_ratio)

                                    row['REQUIRED_MC'] = new_mc

                            

                            print(f"  📈 Increased ACTUAL_MC in {cold_mc} by {portion:.2f} machines")

                            changes_made = True

                    

                    # ปรับจำนวนเครื่องใน hot_mc ให้ลดลง

                    hot_rows = mc_actual[hot_mc]

                    total_current_hot = sum(float(r.get('REQUIRED_MC', r.get('ALLOCATED_MC', 1))) for r in hot_rows)

                    

                    for row in hot_rows:

                        current_mc = float(row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1)))

                        if total_current_hot > 0:

                            decrease_ratio = excess_to_distribute / total_current_hot

                            new_mc = current_mc * (1 - decrease_ratio)

                            row['REQUIRED_MC'] = max(0.1, new_mc)  # ไม่ให้น้อยกว่า 0.1

                    

                    print(f"  📉 Decreased ACTUAL_MC in {hot_mc} by {excess_to_distribute:.2f} machines")

                    changes_made = True

    

    if changes_made:

        print(f"✅ ACTUAL_MC balancing completed for week {plan_week}")

        

        # แสดงผลลัพธ์หลังการปรับ

        final_mc_load = {}

        for row in new_plan:

            mc_group = row['MC_GROUP']

            allocated_machines = float(row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1)))

            if mc_group not in final_mc_load:

                final_mc_load[mc_group] = 0

            final_mc_load[mc_group] += allocated_machines

        

        print(f"  Final ACTUAL_MC distribution: {final_mc_load}")

        new_avg = sum(final_mc_load.values()) / len(final_mc_load)

        print(f"  New average: {new_avg:.2f}")

    else:

        print(f"ℹ️ No ACTUAL_MC balancing adjustments needed for week {plan_week}")

    

    return new_plan





# ชั่วโมงทำงานอ่านจากแผง Work Day ผ่าน _base_work_hours() เท่านั้น
# (WORKING_HOURS_MAP / _get_working_hours_for_mc ที่ hard-code 20 ชม. ถูกลบแล้ว — ไม่มีใครเรียก
#  และขัดกับแหล่งเดียว)



def _get_capacity_for_mc_group(item_code: str, mc_group: str, gauge: str = None) -> float:

    """Get daily capacity for a specific MC_GROUP (capacity ดูต่อ MC, availability ดูต่อ Type_1+Gauge pool)"""

    item_rows = item_cap_data[

        (item_cap_data["ITEM_CODE"] == item_code) &

        (item_cap_data["MC_GROUP"] == mc_group)

    ]

    if not item_rows.empty:

        return _normalize_capacity(item_code, mc_group, item_rows["CAP ทอ"].iloc[0], gauge)



    # ไม่มีแถวของกลุ่มนี้ตรง ๆ → ใช้ค่าสูงสุดของ "ก้อนเดียวกัน" (โรง+CAT+เกจ+พูล)
    # เครื่องในก้อนเดียวกันใช้แทนกันได้ และ user กำหนดให้ยึด cap ของตัวที่ทอได้มากที่สุด

    _grp_members = _canon_members(mc_group, gauge)

    if len(_grp_members) > 1:

        _grp_caps = item_cap_data[
            (item_cap_data["ITEM_CODE"] == item_code)
            & (item_cap_data["MC_GROUP"].astype(str).str.strip().str.upper().isin(_grp_members))
        ]["CAP ทอ"]

        _grp_caps = pd.to_numeric(_grp_caps, errors="coerce").dropna()
        _grp_caps = _grp_caps[_grp_caps > 0]

        if not _grp_caps.empty:

            return _normalize_capacity(item_code, mc_group, _grp_caps.max(), gauge)



    # Fallback: ใช้ capacity ต่ำสุดของ item นี้

    all_item_caps = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]["CAP ทอ"]

    if not all_item_caps.empty:

        return _normalize_capacity(item_code, mc_group, all_item_caps.min(), gauge)



    return 0



# =========================

# BOOKING RAW DATA LOADER

# =========================





def load_all_booking_data() -> pd.DataFrame:

    """โหลดข้อมูล booking ทั้งหมดจาก Booking/ directory (ประวัติการผลิตจริง)"""

    if not BOOKING_DIR.exists():

        return pd.DataFrame()



    all_files = [

        f for f in BOOKING_DIR.iterdir() if f.suffix.lower() in (".xlsx", ".xls")

    ]

    if not all_files:

        return pd.DataFrame()



    dfs = []

    for f in all_files:

        try:

            raw = f.read_bytes()

            is_zip = raw[:2] == b"PK"

            is_biff = raw[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

            df = None

            if is_zip:

                df = pd.read_excel(io.BytesIO(raw), engine="openpyxl")

            elif is_biff:

                df = pd.read_excel(io.BytesIO(raw), engine="xlrd")

            else:

                for enc in ("cp874", "utf-8-sig", "latin-1"):

                    try:

                        text = raw.decode(enc, errors="replace")

                        df = pd.read_csv(

                            io.StringIO(text), sep="\t", on_bad_lines="skip"

                        )

                        if df.shape[1] > 3:

                            break



                    except Exception:

                        continue



            if df is not None:

                df.columns = df.columns.str.strip().str.upper()

                dfs.append(df)

        except Exception as e:

            print(f"⚠️ ไม่สามารถโหลด booking {f.name}: {e}")

    if not dfs:

        return pd.DataFrame()



    return pd.concat(dfs, ignore_index=True)



orders["Date"] = pd.to_datetime(orders["DATE_IN"], errors="coerce")

orders["DYE_END_DATE"] = pd.to_datetime(

    orders.get("DYE_END_DATE", orders.get("YARN_DYE_FINISH_DATE", orders.get("วันที่ย้อมเส้นด้ายจบ"))), errors="coerce"

)

orders["Item Code"] = orders["Item Code"].astype(str).str.upper().str.strip()

orders["Orders Type"] = orders["Orders Type"].astype(str).str.upper().str.strip()

orders["MC GROUP"] = orders["MC_GROUP"].astype(str).str.upper().str.strip()

orders["Orders.Qty"] = pd.to_numeric(orders["Orders.Qty"], errors="coerce").fillna(0)

orders["Plan Qty"] = pd.to_numeric(orders["Plan Qty"], errors="coerce").fillna(0)

orders["FG Week"] = orders["FG Week"]  # Already mapped

orders["SC/SO NO"] = orders["SC/SO NO"]  # Already mapped

orders["Pending Plan"] = pd.to_numeric(orders["Pending Plan"], errors="coerce").fillna(0)

orders["Confirm"] = pd.to_numeric(orders["CONFIRM_KNIT_PLAN_SC"], errors="coerce").fillna(0)

summary_mc["WEEK"] = summary_mc["WEEK"].astype(int)

# สำเนา "เครื่องว่างตามไฟล์" ก่อนเริ่มวางแผน — ระหว่างรัน summary_mc ถูกบวกคืนได้
# (_release_booking_mc ตอนงานสียึดเครื่องของ booking) แต่ไฟล์ booking_final ที่หน้าเว็บอ่าน
# ไม่ได้ถูกแก้ตาม → ต้องเก็บยอดตั้งต้นไว้ให้ด่านตรวจท้ายรอบเทียบ ไม่งั้นแผนวางเกินแล้วเงียบ
# (เคส 29 ก.ย. 2569: ตรวจพูลบอก "ใช้เกินพูล 0 ช่อง" ทั้งที่หน้าเว็บขึ้น "ไม่ว่าง (เกิน 1)")
summary_mc_base = summary_mc.copy()

# calendar_week["WEEK"] / ["YW_INT"] เป็น YW แล้วตั้งแต่ตอนโหลด (ดูหัวข้อ "คีย์สัปดาห์ทั้งไฟล์")



# =========================

# BUILD item_cap_data จาก CAP_PER_DAY ทุก row ใน orders

# dedup ด้วย (ITEM_CODE, MC_GROUP) — ใช้ค่าแรกที่พบสำหรับแต่ละ pair

# =========================

_no_cap_items = set()

_no_cap_order_rows = []  # เก็บ order rows ที่ไม่มี CAP_PER_DAY สำหรับสร้าง sheet แยก

_seen_item_mc: set = set()  # dedup key: (ITEM_CODE, MC_GROUP)

_existing_cap_items: set = set()  # items ที่มี cap อย่างน้อย 1 MC_GROUP



for _, _ord_row in orders.iterrows():

    _ord_item = str(_ord_row.get("Item Code", "")).strip().upper()

    if not _ord_item:

        continue



    _ord_mc = str(_ord_row.get("MC GROUP", "")).strip().upper()

    _ord_sc = str(_ord_row.get("SC/SO NO", "")).strip()

    _ord_cap_per_day = _ord_row.get("CAP_PER_DAY", 0)



    # ตรวจสอบว่ามี CAP_PER_DAY หรือไม่

    if pd.isna(_ord_cap_per_day) or float(_ord_cap_per_day) == 0:

        if _ord_item not in _existing_cap_items:

            if _ord_item not in _no_cap_items:

                print(

                    f"[SKIP] {_ord_item} (SC {_ord_sc}): ไม่มี CAP_PER_DAY — ไม่วางแผนผลิต"

                )

            _no_cap_items.add(_ord_item)

            _no_cap_order_rows.append(_ord_row)

        continue



    raw_cap = float(_ord_cap_per_day)

    _pair = (_ord_item, _ord_mc)



    # เพิ่มเข้า item_cap_data เฉพาะ pair ใหม่ (ไม่ซ้ำ)

    if _pair not in _seen_item_mc:

        _seen_item_mc.add(_pair)

        new_cap_row = {

            "ITEM_CODE": _ord_item,

            "MC_GROUP": _ord_mc,

            "CAP ทอ": raw_cap,

            "REVOLUTION/WEIGHT": 1.0,

            "GUAGE": str(_ord_row.get("MC_GUAGE", "")).strip().upper(),

        }

        item_cap_data = pd.concat(

            [item_cap_data, pd.DataFrame([new_cap_row])], ignore_index=True

        )

        converted_cap = raw_cap * (_base_work_hours(_ord_mc, _ord_row.get("MC_GUAGE")) / 24)

        print(

            f"[ADD CAP] {_ord_item} (SC {_ord_sc}) MC={_ord_mc}: "

            f"CAP_PER_DAY {_ord_cap_per_day} (24hr) → {converted_cap:.2f} (20hr)"

        )



    _existing_cap_items.add(_ord_item)



_no_cap_df = pd.DataFrame(_no_cap_order_rows) if _no_cap_order_rows else pd.DataFrame()



# Rebuild gauge lookup หลัง CAP_PER_DAY ถูก populate เข้า item_cap_data แล้ว

for _, _r in item_cap_data.iterrows():

    _ic = str(_r.get("ITEM_CODE", "")).strip().upper()

    _mc = str(_r.get("MC_GROUP", "")).strip().upper()

    _gg = _r.get("GUAGE")

    _gs = _normalize_gauge(_gg)

    if _ic and _mc and _gs and _gs.lower() != "nan":

        _item_mc_to_gauge[(_ic, _mc)] = _gs



# =========================

# TRACK ITEMS WITH MULTIPLE CAPACITIES (SINGLE MC_TYPE and OM/OMNOI)



# =========================

_multi_cap_items = set()

_multi_cap_order_rows = []  # เก็บ order rows ที่มี multiple capacities



def _check_multiple_capacities(item_code: str, mc_group: str) -> bool:

    """ตรวจสอบว่า MC_GROUP นั้นๆ มีหลายค่า capacity ที่ไม่เหมือนกัน สำหรับ SINGLE MC_TYPE หรือ OM/OMNOI หรือไม่"""

    # หาข้อมูล MC_GROUP จาก Master_MC_5

    mc_info = master_mc[master_mc["MC"] == mc_group]

    if mc_info.empty:

        return False

    

    factory = str(mc_info.iloc[0]["Factory"]).strip().upper()

    mc_type = str(mc_info.iloc[0].get("Type", "")).strip().upper()

    

    # Debug: แสดงข้อมูล MC_GROUP

    if factory in ("OM", "OMNOI") or mc_type == "SINGLE":

        print(f"[DEBUG MC_INFO] {item_code} in {mc_group}: Factory={factory}, Type={mc_type}")

    

    # ตรวจสอบสำหรับ SINGLE MC_TYPE หรือ OM/OMNOI factories

    if mc_type == "SINGLE" or factory in ("OM", "OMNOI"):

        # ตรวจสอบว่า MC_GROUP นั้นๆ มีหลายค่า capacity หรือไม่

        mc_group_caps = item_cap_data[

            (item_cap_data["ITEM_CODE"] == item_code) &

            (item_cap_data["MC_GROUP"] == mc_group)

        ]

        print(f"[DEBUG CAP_COUNT] {item_code} in {mc_group}: {len(mc_group_caps)} entries found")

        

        if len(mc_group_caps) > 1:

            # ตรวจสอบว่าค่า capacity ไม่เหมือนกันหรือไม่

            unique_caps = set(mc_group_caps["CAP ทอ"].tolist())

            if len(unique_caps) > 1:

                # Debug: แสดงข้อมูล capacity ที่ไม่เหมือนกัน

                caps_list = mc_group_caps["CAP ทอ"].tolist()

                print(f"[DEBUG MULTI_CAP] {item_code} ใน {mc_group} มี {len(mc_group_caps)} entries ค่าต่างกัน: {caps_list}")

                return True

            else:

                # มีหลาย entries แต่ค่าเหมือนกัน - เป็นข้อมูลซ้ำ

                caps_list = mc_group_caps["CAP ทอ"].tolist()

                print(f"[DEBUG DUPLICATE] {item_code} ใน {mc_group} มี {len(mc_group_caps)} entries ค่าเหมือนกัน: {caps_list}")

    

    return False



# ตรวจสอบ items ที่มี multiple capacities

print("🔍 Starting MULTI_CAP detection...")

debug_count = 0

for _, _ord_row in orders.iterrows():

    _ord_item = str(_ord_row.get("Item Code", "")).strip().upper()

    _ord_mc = str(_ord_row.get("MC GROUP", "")).strip().upper()

    

    if _ord_item and _ord_mc:

        debug_count += 1

        if debug_count <= 5:  # Debug แค่ 5 rows แรก

            print(f"[DEBUG CHECKING] {_ord_item} with {_ord_mc}")

        

        if _check_multiple_capacities(_ord_item, _ord_mc):

            if _ord_item not in _multi_cap_items:

                print(f"[MULTI_CAP] {_ord_item} (MC {_ord_mc}): มีหลาย capacities - SINGLE MC_TYPE หรือ OM/OMNOI")

            _multi_cap_items.add(_ord_item)

            _multi_cap_order_rows.append(_ord_row)



print(f"🔍 Checked {debug_count} order rows for MULTI_CAP detection")



_multi_cap_df = pd.DataFrame(_multi_cap_order_rows) if _multi_cap_order_rows else pd.DataFrame()



# =========================

# FACTORY TYPE CONFIGURATION



# =========================

# สร้าง Factory Type mapping จาก Master_MC_5.xlsx

FACTORY_TYPE_MAP = {}

MC_TYPE_MAP = {}  # mc_group → Type (DOUBLE / SINGLE / etc.)

# วันทำงานย้ายไปอ่านจากชีท Work Day (Calendar.xlsx) ผ่าน WorkDay.get_working_days()
# — ไม่ใช้ Working Day ของ MasterMC และไม่มีกฎพิเศษ Week 17/32 อีกต่อไป

for _, row in master_mc.iterrows():

    mc_name = str(row["MC"]).strip().upper()  # ใช้คอลัมน์ MC

    factory_type = str(row["Factory"]).strip().upper()

    mc_type = str(row.get("Type", "")).strip().upper()  # คอลัมน์ Type

    # ข้าม OUTSOURCE เพราะเป็นการจ้างงานภายนอก

    if factory_type == "OUTSOURCE":

        continue

    # ใช้ MC name โดยตรง

    main_mc_group = mc_name

    FACTORY_TYPE_MAP[main_mc_group] = factory_type

    MC_TYPE_MAP[main_mc_group] = mc_type


# วันทำงานรายสัปดาห์ (แหล่งเดียว: ชีท Work Day / Week Merge ใน Calendar.xlsx)
# ส่ง lambda นับวันทำงานตามปฏิทิน (get_working_days_in_week นิยามด้านล่าง — resolve ตอนเรียกใช้)
# หมายเหตุ: import WorkDay + def _base_work_hours ย้ายไปไว้ต้นไฟล์แล้ว (ถูกเรียกที่ระดับ module ก่อนถึงจุดนี้)
WorkDay.init(master_mc, lambda w: len(get_working_days_in_week(_TL.key(w))), timeline=_TL)
# สอนคู่ (เครื่อง, เกจ) → กลุ่มในแผง จาก booking (คีย์แผงคือ FACTORY·MC_CAT·เกจ)
# ครอบคลุมเครื่องที่ไม่มีใน MasterMC เช่น SKPHF/SKPSL/SKPAO — ไม่งั้นได้ 0 วันทั้งที่ตั้งค่าไว้แล้ว
_wd_alias_cnt = WorkDay.register_aliases(detail_mc)
if _wd_alias_cnt:
    print(f"✅ จับคู่เครื่อง→กลุ่มในแผงวันทำงานเพิ่ม {_wd_alias_cnt} คู่ (จาก FACTORY+MC_CAT+เกจ)")


# set คู่ (MC_GROUP, gauge_normalized) ที่มีจริงใน MasterMC — ใช้ skip order ที่ไม่มีใน Master
_VALID_MC_GAUGE_SET: set = set()
for _, _vmrow in master_mc.iterrows():
    _vmc = str(_vmrow.get("MC", "")).strip().upper()
    _vg = _normalize_gauge(_vmrow.get("Guage", ""))
    if _vmc:
        _VALID_MC_GAUGE_SET.add((_vmc, _vg))
        _VALID_MC_GAUGE_SET.add((_vmc, ""))  # fallback ไม่เช็ค gauge
print(f"✅ Valid MC+Gauge set: {len(_VALID_MC_GAUGE_SET)} entries")

# =========================

# TODAY (ห้ามวางย้อนหลัง)



# =========================





def get_week_from_date(date):

    if pd.isna(date):

        return None



    row = calendar_week[

        (calendar_week["WEEK_START"] <= date) & (calendar_week["WEEK_END"] >= date)

    ]

    return None if row.empty else int(row.iloc[0]["WEEK"])





def wk_year(week):
    """คืนปีของค่าสัปดาห์: composite YYYYWW -> YYYY, bare -> None (ไม่รู้ปี)"""
    if week is None:
        return None
    try:
        w = int(week)
    except (TypeError, ValueError):
        return None
    return w // 100 if w >= 100000 else None


def wk_num(week):
    """คืนเลขสัปดาห์ 1-53: composite YYYYWW -> WW, bare -> คงเดิม"""
    if week is None:
        return None
    try:
        w = int(week)
    except (TypeError, ValueError):
        return None
    return w % 100 if w >= 100000 else w


_week_anchor_idx = None  # ตำแหน่งที่แผนกำลังเดินอยู่ (None = ใช้ TODAY_IDX) — ดู week_index()
_BARE_WEEK_HITS: dict = {}  # บรรทัดที่ยังเรียก week_index ด้วยเลขสัปดาห์เปล่า → จำนวนครั้ง

# bare week ที่อ้างถึงระหว่างวางแผนย้อนหลังจาก anchor ได้ไม่เกินกี่สัปดาห์
# ต่ำกว่านี้ = ตีความเป็น occurrence ของปีถัดไปแทน (กันจับผิดปีย้อนอดีต)
# 8 สัปดาห์เผื่อไว้สำหรับ cross-SC fill / สัปดาห์ที่เพิ่งผลิต ซึ่งอยู่ก่อน anchor เล็กน้อย
WEEK_LOOKBACK_LIMIT = plan_config.get("KP_WEEK_LOOKBACK_LIMIT")


def set_week_anchor(idx):
    """ตั้งจุดอ้างอิงของ week_index ให้ตามตำแหน่งที่แผนกำลังเดินอยู่

    ต้องเลื่อนตาม plan_week เสมอ เพื่อให้ bare week ที่อ้างถึงระหว่างวางแผน
    ถูกตีความเป็นปีที่ถูกต้อง แม้แผนจะยืดข้ามปีไปไกลจาก TODAY
    ส่ง None เพื่อรีเซ็ตกลับไปยึด TODAY_IDX (ใช้ตอนจบ order)
    """
    global _week_anchor_idx
    _week_anchor_idx = int(idx) if idx is not None else None


def week_index(week, anchor=None):
    """แปลงค่าสัปดาห์ -> row index ใน calendar_week (ตำแหน่งบน timeline)

    รองรับ 2 รูปแบบ:
      - composite YYYYWW (>= 100000) : จับคู่ YW_INT แบบ unique -> ถูกต้องข้ามปีเสมอ
      - bare week 1-53   (< 100000)  : เลือก occurrence ที่ใกล้ TODAY_IDX ที่สุด
                                        (กันเลข week ซ้ำข้ามปีจับผิดปี; ถูกทั้งอดีตใกล้ ๆ และอนาคตใกล้ ๆ)
    หมายเหตุ: ตอน bootstrap (คำนวณ TODAY_IDX ครั้งแรก) TODAY_IDX ยังไม่มี -> fallback แถวแรกสุด
    """
    if week is None:
        return None
    try:
        w = int(week)
    except (TypeError, ValueError):
        return None

    if w >= 100000:  # YW — คีย์หลักของทั้งไฟล์ ตรงตัวเสมอ
        return _IDX_BY_YW.get(w)

    # ── เลขสัปดาห์เปล่า: ไม่ควรเหลือแล้ว (ทุกแหล่งถูกแปลงเป็น YW ตอนโหลด) ──
    # นับไว้ว่ามาจากบรรทัดไหน เพื่อไล่เก็บจุดที่ยังส่งเลขเปล่ามา (สรุปพิมพ์ตอนจบรอบ)
    if w != 99:
        _f = sys._getframe(1)
        _BARE_WEEK_HITS[_f.f_lineno] = _BARE_WEEK_HITS.get(_f.f_lineno, 0) + 1
    idx = list(calendar_week.index[calendar_week["WEEK_NUM"] == w])
    if not idx:
        return None
    # anchor = จุดอ้างอิงสำหรับเลือก occurrence เมื่อ week number ซ้ำข้ามปี
    #   1) anchor ที่ caller ส่งมา (ชัดเจนที่สุด)
    #   2) _week_anchor_idx = ตำแหน่งที่แผน "กำลังเดินอยู่" (เลื่อนตาม plan_week)
    #   3) TODAY_IDX (ค่าเริ่มต้นตอน bootstrap / นอกบริบทการวางแผน)
    # ทำไมต้องมี (2): anchor ที่ตรึงไว้ที่ TODAY จะพังเมื่อแผนยาวเกิน ~26 สัปดาห์
    # เช่น แผนเดินถึง W48 แล้วอ้าง W20 (ตั้งใจหมายถึงปีหน้า) — วัดจาก TODAY จะได้
    # W20 ปีปัจจุบัน (ย้อนอดีต) แต่วัดจากตำแหน่งแผนจริงจะได้ปีหน้าถูกต้อง
    base = anchor
    if base is None:
        base = globals().get("_week_anchor_idx")
    if base is None:
        base = globals().get("TODAY_IDX")
    if base is None:
        return int(idx[0])
    # กันการ "ย้อนอดีตข้ามปี": ถ้าเลือกแบบใกล้สุดล้วน ๆ สัปดาห์ที่อยู่เกินครึ่งปี
    # ข้างหน้า anchor จะถูกจับเป็น occurrence ของปีก่อนแทน (เช่น anchor W48 ปีนี้
    # อ้าง W25 → ได้ W25 ปีนี้ซึ่งผ่านไปแล้ว ทั้งที่ต้องหมายถึง W25 ปีหน้า)
    # จึงคัดเฉพาะ occurrence ที่ไม่ย้อนหลังเกิน WEEK_LOOKBACK_LIMIT ก่อน
    # (เผื่อไว้สำหรับ cross-SC fill / สัปดาห์ที่เพิ่งผลิต ซึ่งอยู่ก่อน anchar ไม่กี่สัปดาห์)
    _fwd = [i for i in idx if int(i) >= base - WEEK_LOOKBACK_LIMIT]
    if _fwd:
        idx = _fwd
    # เลือกแถวที่ใกล้ anchor ที่สุด (tie -> แถวก่อน)
    return int(min(idx, key=lambda i: (abs(int(i) - base), int(i))))


# cache: ลูปวางแผนเรียก week_mask หลักหมื่นครั้ง ถ้าแปลงปีใหม่ทุกครั้งแผนจะช้าลงมาก
_YEAR_OF_WEEK_CACHE: dict = {}


def year_of_week(week, anchor=None):
    """ปีจริงของ 'สัปดาห์ที่แผนกำลังอ้างถึง' — แปลงผ่านตำแหน่งบนปฏิทิน (week_index)

    เลขสัปดาห์เปล่าเดียวกันมีได้ทุกปี ; ตัวตัดสินว่าเป็นปีไหนคือ anchor = ตำแหน่งที่แผนเดินอยู่
    (anchor เลื่อนตาม plan_week — ดู _week_anchor_idx) จึงต้องใส่ anchor ในคีย์ cache ด้วย
    """
    # YW มีปีในตัวอยู่แล้ว
    _yy = yw_year(week)
    if _yy is not None:
        return _yy
    # เลขสัปดาห์เปล่า (ข้อมูลเก่า) — ใช้ "กรอบเวลาจริง" ก่อน แล้วค่อยพึ่ง anchor
    _win = globals().get("_YEAR_BY_WEEK_WINDOW")
    if anchor is None and _win:
        try:
            _wi = int(week)
        except (TypeError, ValueError):
            _wi = None
        if _wi is not None and _wi in _win:
            return _win[_wi]

    _key = (week, anchor if anchor is not None else globals().get("_week_anchor_idx"))
    if _key in _YEAR_OF_WEEK_CACHE:
        return _YEAR_OF_WEEK_CACHE[_key]
    i = week_index(week, anchor)
    try:
        y = int(calendar_week.iloc[i]["YEAR"]) if i is not None else None
    except (KeyError, IndexError, TypeError, ValueError):
        y = None
    _YEAR_OF_WEEK_CACHE[_key] = y
    return y


def _year_num(df):
    """คอลัมน์ YEAR เป็นตัวเลข (แปลงครั้งเดียวต่อตาราง)

    เก็บ cache ไว้ใน df.attrs ไม่ใช่ dict ที่คีย์ด้วย id(df) — id ของ DataFrame ชั่วคราวที่ถูก
    เก็บกวาดไปแล้วถูกนำมาใช้ซ้ำได้ จะทำให้หยิบคอลัมน์ปีของตารางอื่นมาใช้โดยไม่รู้ตัว
    """
    s = df.attrs.get("_YEAR_NUM")
    if s is None or len(s) != len(df):
        s = pd.to_numeric(df["YEAR"], errors="coerce")
        df.attrs["_YEAR_NUM"] = s
    return s


def week_mask(df, week, anchor=None):
    """mask แถวของตารางเครื่อง (summary_mc / detail_mc) ที่เป็น "สัปดาห์นั้นของปีที่ถูกต้อง"

    ทำไมต้องมี: ตารางเครื่องคีย์ด้วยเลขสัปดาห์เปล่า ถ้าเทียบ WEEK อย่างเดียว แผนที่เดินไปถึง
    W29 ของปีหน้าจะไปหยิบแถวเครื่องของ W29 ปีนี้ซึ่งผ่านไปแล้ว (booking หมดอายุ = ว่างทั้งพูล)
    → เห็นเครื่องว่างที่ไม่มีอยู่จริง

    ตารางที่มีคอลัมน์ YEAR (AVA_MC ติดป้ายให้) → เทียบทั้งปีและสัปดาห์
    ตารางเก่าที่ไม่มี YEAR หรือแปลงปีไม่ได้ → เทียบเลขสัปดาห์อย่างเดียวเหมือนเดิม
    """
    m = df["WEEK"] == week
    # ตารางที่โหลดเข้ามาถูกแปลง WEEK เป็น YW แล้ว (ดู _weeks_to_yw) → เทียบตรง ๆ ก็แยกปีเอง
    if "YEAR" not in df.columns or yw_year(week) is not None:
        return m
    y = year_of_week(week, anchor)
    if y is None:
        return m
    return m & (_year_num(df) == y)


def _week_gap_of(week_a, week_b):
    """จำนวนสัปดาห์จาก week_b ถึง week_a บนปฏิทิน (YW ข้ามปีถูกต้อง)
    หาตำแหน่งไม่เจอ (เช่น W99) → ผลต่างของเลขสัปดาห์ (พฤติกรรมเดิม)"""
    _ia, _ib = week_index(week_a), week_index(week_b)
    if _ia is not None and _ib is not None:
        return _ia - _ib
    return int(yw_week(week_a) or 0) - int(yw_week(week_b) or 0)


def _rem_cap_week_order(kv):
    """ลำดับของ remaining_week_cap สำหรับลูปเก็บตก cap เหลือ

    คีย์เก็บ "เลขสัปดาห์เปล่า" (ไม่มีปี) — เรียงด้วยเลขดิบจะสลับลำดับเมื่อแผนคร่อมปี
    (W40 ของปีนี้ต้องมาก่อน W24 ของปีหน้า แต่ 40 > 24) จึงต้องเรียงตาม index บนปฏิทิน
    สัปดาห์ที่หา index ไม่ได้ให้ไปท้ายสุด เพื่อไม่ให้แซงสัปดาห์ที่รู้ตำแหน่งจริง"""
    _i = week_pos(kv[0][0])
    return (0, _i) if _i is not None else (1, int(kv[0][0]))





def get_revolution_weight_from_orders(item_code, mc_group):

    """ค้นหา REVOLUTION_WEIGHT ของ item จาก orders DataFrame (order_ready)"""

    # หาข้อมูลของ item นี้จาก orders DataFrame

    item_rows = orders[orders['Item Code'] == item_code]

    if not item_rows.empty:

        # ใช้ REVOLUTION_WEIGHT จาก order_ready โดยตรง

        rev_weight = item_rows.iloc[0].get('REVOLUTION_WEIGHT', 0)

        return rev_weight if rev_weight and rev_weight > 0 else 0

    return 0





def get_working_days_by_factory(mc_group, available_machines_count=None, week=None, item_code=None, gauge=None):

    """คืนวันทำงานของ (mc_group, gauge) ในสัปดาห์ที่ระบุ จากชีท Work Day (แหล่งเดียว)

    ค่านี้เป็น "ผลลัพธ์สุดท้าย" แล้ว — ใช้ค่าที่กรอกตรง ๆ ไม่หักวันหยุดต่อ,
    รวม logic ยุบสัปดาห์ + สัปดาห์ที่ปฏิทินหยุดทั้งก้อน (=0) ไว้ให้แล้ว

    week=None → คืนค่ามาตรฐานของกลุ่ม (ไม่อิงสัปดาห์)
    (available_machines_count / item_code เก็บพารามิเตอร์ไว้เพื่อความเข้ากันได้กับ caller เดิม)
    """
    return WorkDay.get_working_days(mc_group, gauge=gauge, week=week)





def adjust_daily_cap_for_item_special(daily_cap, item_code, mc_group, gauge=None, base_working_hour=None):

    """ปรับ daily_cap ตาม Working hour จาก Item Special (Item Special ชนะเสมอ)

    daily_cap เข้ามาถูกแปลงด้วยชั่วโมงของกลุ่มจากแผง Work Day แล้ว (base)
    ถ้า item อยู่ใน Item Special และ Working hour ต่างจาก base → rescale เป็นชั่วโมงของ Item Special
    → ผลลัพธ์ = cap(24hr) * (item_special_wh / 24) เสมอ ไม่ว่ากลุ่มจะตั้งไว้เท่าใด
    """

    if not item_code or not daily_cap:

        return daily_cap

    _is = get_item_special(item_code, mc_group, gauge)

    if _is is not None:

        base = base_working_hour if base_working_hour is not None else _base_work_hours(mc_group, gauge)

        _wh = _is[1]  # working_hour

        if base > 0 and _wh != base:

            return daily_cap * (_wh / base)

    return daily_cap





def _dynamic_setup_limit(

    plan_week: int, rdd_idx, required_mc: int, remaining_job_slots: int

) -> int:

    """คืนจำนวน new machines สูงสุดที่ควร setup ใน week นี้ ตาม urgency ของ RDD

    - ห่าง RDD >= 2 week : ใช้แค่ required_mc  (ประหยัด job slot ไว้ให้ order อื่น)

    - ห่าง RDD == 1 week : ใช้แค่ required_mc (จำกัดตาม required_mc)

    - plan_week >= RDD   : ใช้แค่ required_mc (จำกัดตาม required_mc)

    ทุก case ยังต้องผ่าน check_job_capacity_limit อีกรอบเสมอ

    rdd_idx = row index ใน calendar_week (ใช้แทน fg_week_int เพื่อรองรับข้ามปี)"""

    fallback = required_mc  # เปลี่ยนจาก remaining_job_slots เป็น required_mc

    if not required_mc:

        required_mc = fallback

    if rdd_idx is None:

        # ไม่มี RDD → conservative = required_mc เท่านั้น

        return required_mc



    plan_idx = week_index(plan_week)

    if plan_idx is None:

        return required_mc



    weeks_to_rdd = rdd_idx - plan_idx

    if weeks_to_rdd <= 0:

        # urgent / เลยกำหนดแล้ว → ใช้ required_mc

        return required_mc



    elif weeks_to_rdd == 1:

        # สัปดาห์สุดท้ายก่อน RDD → ใช้ required_mc

        return required_mc



    else:

        # ยังเหลือเวลา → ใช้แค่เท่าที่จำเป็นเพื่อทัน RDD

        return required_mc





def check_job_capacity_limit(

    mc_group,

    available_machines_count,

    urgent_mode=False,

    current_week_jobs=None,

    committed_carryover=0,

):

    """ตรวจสอบว่าจำนวนเครื่องไม่เกิน job/week capacity

    committed_carryover: จำนวนเครื่อง carry-over ที่ผูกพันแล้ว (ต้องผ่านเสมอ, ห้าม cap)

    """

    # ปิดสวิตช์ที่หน้า "ตั้งค่าระบบ" › Job Setup/Week = ไม่คุมโควตาช่างเลย
    if not JOB_ENFORCE:

        return available_machines_count

    # หาข้อมูล MC_GROUP จาก Master_MC_5

    mc_info = master_mc[master_mc["MC"] == mc_group]

    if mc_info.empty:

        # ถ้าไม่เจอใน Master_MC_5 ใช้ค่า default

        factory = "PHET"

        mc_type = "DOUBLE"

    else:

        # ดูว่า MC_GROUP นี้อยู่ Factory ไหน และเป็น Type อะไร

        factory = str(mc_info.iloc[0]["Factory"]).strip().upper()

        mc_type = str(mc_info.iloc[0].get("Type", "DOUBLE")).strip().upper()

    # กำหนด job/week capacity ตาม FAC และ Type

    # ตัด (TUBE) ออก — "SINGLE (TUBE)" ถือเป็น SINGLE เหมือนกัน

    mc_type_clean = mc_type.replace("(TUBE)", "").strip()

    if factory == "PHET":

        if mc_type_clean == "DOUBLE":

            max_jobs = JOB_CAP["PHET_DOUBLE"]

        elif mc_type_clean == "SINGLE":

            max_jobs = JOB_CAP["PHET_SINGLE"]

        else:

            max_jobs = JOB_CAP["PHET_DOUBLE"]  # default PHET

    elif factory in ("OM", "OMNOI"):

        max_jobs = JOB_CAP["OM"]

    else:

        # OUTSOURCE หรือ factory อื่นๆ ไม่มี job cap → ผ่านเสมอ

        return available_machines_count



    # ห้ามเกิน cap เด็ดขาด (urgent mode ก็ใช้ cap เดิม)

    max_jobs_effective = max_jobs

    # Normal/urgent: ห้ามเกิน cap เด็ดขาด

    if current_week_jobs is not None:

        remaining_jobs = max(0, max_jobs_effective - current_week_jobs)

        if committed_carryover > 0:

            # Carryover ไม่กิน job slot เลย — cap เฉพาะ new machines เท่านั้น

            # remaining_jobs = slots ที่ยังว่างสำหรับ new setups (carryover ไม่นับ)

            new_mc = max(0, available_machines_count - committed_carryover)

            allowed_new = min(new_mc, remaining_jobs)  # ต้องไม่เกิน slot ที่เหลือ

            result = committed_carryover + allowed_new

            return result



        return min(available_machines_count, remaining_jobs)



    # ถ้าไม่มีข้อมูล current_week_jobs ให้จำกัดตาม max_jobs_effective

    return min(available_machines_count, max_jobs_effective)


# ---------- โควตาตั้งเครื่องใหม่ของ "ผ้า 1 ตัว" ต่อสัปดาห์ ----------
# ช่างตั้งเครื่องเป็นรายผ้า ไม่ได้ตั้งเป็นรายใบสั่ง → ผ้าตัวเดียวกันบนกลุ่มเครื่อง+เกจเดียวกัน
# ทุกใบสั่งต้องแบ่งโควตาก้อนเดียวกัน (MAX_NEW_SETUP_MC) ; carry-over ไม่กินโควตา
# ทุกด่านที่จะตั้งเครื่องใหม่ต้องถาม _item_setup_room() ก่อน และหักด้วย _item_setup_charge()
# ตอนเขียนแถวจริงเท่านั้น (จุดเดียวกับที่หัก weekly_job_usage — ไม่งั้นหักซ้ำ/หักลม)
# สมุดนี้ต้องเดินคู่กับ weekly_job_usage ทุกจุด: หักตรงไหน หักที่นี่ด้วย · คืนตรงไหน คืนที่นี่ด้วย
# (_item_setup_refund) · สมุดยึดเครื่อง (_grab_txn_*) ย้อนทั้งสองเล่มพร้อมกัน
# เครื่องที่ booking ล็อกไว้ (ชีท DETAIL) และงานที่ตรึงไว้ (ชีท PINNED_JOBS_ITEM) ตั้งใหม่ในสัปดาห์ไหน
# ก็กินโควตาของผ้านั้นด้วย (user 30 ก.ย. 2569: ช่างตั้งผ้า A ครบ 2 ตัวแล้ว ใบใหม่ของผ้า A ตั้งเพิ่มไม่ได้)


def _setup_quota_key(item_code, mc_group, gauge, week):
    """คีย์โควตา: (idx สัปดาห์, ผ้า, กลุ่มเครื่อง, เกจ) — None เมื่อระบุสัปดาห์บนปฏิทินไม่ได้"""
    _idx = week_index(week) if week is not None else None
    if _idx is None:
        return None
    return (
        int(_idx),
        str(item_code or "").strip().upper(),
        str(mc_group or "").strip().upper(),
        _normalize_gauge(gauge) if gauge is not None else "",
    )


def _item_setup_room(item_code, mc_group, gauge, week):
    """เครื่องใหม่ที่ผ้านี้ยังตั้งได้อีกกี่ตัวในสัปดาห์นี้ (ปิดสวิตช์ = เพดานเต็มเหมือนเดิม)"""
    if not SETUP_QUOTA_PER_ITEM:
        return int(MAX_NEW_SETUP_MC)
    _k = _setup_quota_key(item_code, mc_group, gauge, week)
    if _k is None:
        return int(MAX_NEW_SETUP_MC)
    return max(0, int(MAX_NEW_SETUP_MC) - int(_item_new_mc_week.get(_k, 0)))


def _item_setup_charge(item_code, mc_group, gauge, week, n_new):
    """หักโควตาหลังเขียนแถวที่ตั้งเครื่องใหม่จริง n_new ตัว"""
    if not SETUP_QUOTA_PER_ITEM or not n_new or n_new <= 0:
        return
    _k = _setup_quota_key(item_code, mc_group, gauge, week)
    if _k is None:
        return
    _item_new_mc_week[_k] = int(_item_new_mc_week.get(_k, 0)) + int(n_new)


def _item_setup_refund(item_code, mc_group, gauge, week, n_back):
    """คืนโควตาเมื่อเครื่องใหม่ที่หักไปแล้วถูกปลดออก (เช่น งานสียึดเครื่องไป) — ไม่ติดลบ"""
    if not SETUP_QUOTA_PER_ITEM or not n_back or n_back <= 0:
        return
    _k = _setup_quota_key(item_code, mc_group, gauge, week)
    if _k is None or _k not in _item_new_mc_week:
        return
    _left = int(_item_new_mc_week[_k]) - int(n_back)
    if _left > 0:
        _item_new_mc_week[_k] = _left
    else:
        _item_new_mc_week.pop(_k, None)


def _cap_new_by_item_quota(new_mc, item_code, mc_group, gauge, week, tag=""):
    """ตัด new_mc ให้ไม่เกินโควตาที่ผ้านี้เหลือในสัปดาห์นี้ (พิมพ์เหตุผลเมื่อโดนตัด)"""
    _new = int(new_mc or 0)
    if _new <= 0:
        return _new
    _room = _item_setup_room(item_code, mc_group, gauge, week)
    if _new > _room:
        print(
            f"🔒 [ITEM SETUP QUOTA{'/' + tag if tag else ''}] {item_code} W{week} {mc_group}: "
            f"ตั้งเครื่องใหม่ {_new} → {_room} (ผ้านี้ใช้โควตาสัปดาห์นี้ไปแล้ว "
            f"{int(MAX_NEW_SETUP_MC) - _room}/{int(MAX_NEW_SETUP_MC)} ตัว)"
        )
        return _room
    return _new



# ยกเลิกกฎพิเศษราย week ของ "ปฏิทิน" — วันทำงานอ่านจากชีท Work Day อย่างเดียว
WEEK_DAYS_OVERRIDE = {}





def get_working_days_in_week(week):

    """Get working days for a specific week from calendar (กรองวันหยุดออก)

    นับวันทำงานตามปฏิทินจริง (is_working_day == 1) ทุกวัน รวมวันศุกร์ด้วย

    ถ้า calendar บอก status=1 (ทำงาน) จะนับเป็นวันทำงานไม่ว่าจะเป็นวันใด

    """

    # ตรวจสอบ override ก่อน

    if week in WEEK_DAYS_OVERRIDE:

        override_days = WEEK_DAYS_OVERRIDE[week]

        week_data = calendar_week[calendar_week["WEEK"] == week]

        if not week_data.empty:

            week_start = week_data.iloc[0]["WEEK_START"]

            week_end = week_data.iloc[0]["WEEK_END"]

            mask = (

                (calendar["DATE"] >= week_start)

                & (calendar["DATE"] <= week_end)

                & (calendar["is_working_day"] == 1)

            )

            base_days = calendar.loc[mask, "DATE"].tolist()

        else:

            base_days = []

        # เติมให้ครบตาม override (pad ด้วย None สำหรับวันพิเศษ)

        while len(base_days) < override_days:

            base_days.append(None)

        return base_days[:override_days]



    week_data = calendar_week[calendar_week["WEEK"] == week]

    if week_data.empty:

        return []



    week_start = week_data.iloc[0]["WEEK_START"]

    week_end = week_data.iloc[0]["WEEK_END"]

    # กรองเฉพาะวันที่ is_working_day == 1 จาก daily calendar

    mask = (

        (calendar["DATE"] >= week_start)

        & (calendar["DATE"] <= week_end)

        & (calendar["is_working_day"] == 1)

    )

    working_days = calendar.loc[mask, "DATE"].tolist()

    return working_days





def _sp_used_from_plans(factory, cat, gauge_str, week, sp_type):
    """นับเครื่อง sub-pool (POLY/COTTON) ที่แถวแผน "วางไปแล้วจริง" ใช้อยู่ในสัปดาห์นั้น

    ตัวนับ incremental (_mc_special_weekly_usage) ข้ามแถว FG-split และแถว SC อื่นของ item
    เดียวกันในสัปดาห์เดียวกัน (ถือว่าใช้เครื่องร่วมกัน) แต่บางแถวใช้เครื่องคนละชุดจริง
    → ตัวนับต่ำกว่าความจริง hard cap เลยปล่อยงานเกินโควตา
    ที่นี่นับจาก plans ตรงๆ: dedupe เฉพาะแถวที่ใช้เครื่องชุดเดียวกัน (item+SC+เครื่อง+เกจ)
    โดยเอาค่าสูงสุด — แถวที่ใช้เครื่องร่วมกันจริงจะมี ACTUAL_MC = 0 อยู่แล้ว
    """
    if not _PLANS_REF:
        return 0
    seen = {}
    for _r in _PLANS_REF:
        if _r.get("PLAN_WEEK") != week:
            continue
        _it = str(_r.get("ITEM_CODE", "")).strip().upper()
        if _get_item_cotton_poly(_it) != sp_type:
            continue
        _mc = str(_r.get("MC_GROUP", "")).strip().upper()
        _g = _normalize_gauge(_r.get("MC_GUAGE"))
        if _g != gauge_str:
            continue
        if _mc_to_factory(_mc, _g) != factory or _mc_to_type1(_mc, _g) != cat:
            continue
        _k = (_it, str(_r.get("SC_SO_NO", "")).strip(), _mc, _g)
        _v = int(_r.get("ACTUAL_MC", 0) or 0)
        seen[_k] = max(seen.get(_k, 0), _v)
    return sum(seen.values())


# =========================
# Pool split (SINGLE-32): SKP vs SKPTA/SKPLE เป็นคนละพูล (นิยามตามคอลัมน์ Group ใน MasterMC)
# อ่าน MC_POOL_MAP + MC_POOL จาก booking_final (สร้างโดย AVA_MC) เพื่อให้ label ตรงกันเป๊ะ
# get_actual_mc_remain จะคิดเครื่องเฉพาะพูลของเครื่องนั้น ไม่ยืมข้ามพูล
# =========================
_MC_POOL_LABEL: dict = {}   # (mc_upper, gauge_str) → pool label เช่น "PHET|SKPLE , SKPTA 26G"
_SPLIT_CG: set = set()      # (type_1_upper, gauge_str) ที่แยกพูล → คิดเครื่องต่อพูล
try:
    _pm_df = pd.read_excel(MC_REMAIN_FILE, sheet_name="MC_POOL_MAP")
    _pm_df.columns = _pm_df.columns.str.strip().str.upper()
    for _, _pm in _pm_df.iterrows():
        _pm_mc = str(_pm.get("MC_GROUP", "")).strip().upper()
        _pm_g = _normalize_gauge(_pm.get("GUAGE", ""))
        _pm_pool = str(_pm.get("MC_POOL", "")).strip()
        if _pm_mc and _pm_g and _pm_pool:
            _MC_POOL_LABEL[(_pm_mc, _pm_g)] = _pm_pool
    if "MC_POOL" in summary_mc.columns:
        _pool_by_cg: dict = {}
        for _, _sr in summary_mc.iterrows():
            _cat = str(_sr["TYPE_1"]).strip().upper()
            # แยกพูลเฉพาะ SINGLE-32 (SKP vs SKPTA/SKPLE) — cat อื่นคงพฤติกรรมเดิม (รวมทั้ง cat|gauge)
            if _cat != "SINGLE-32":
                continue
            _sp = str(_sr.get("MC_POOL", "")).strip()
            if not _sp:
                continue
            _cg = (_cat, _normalize_gauge(_sr["GUAGE"]))
            _pool_by_cg.setdefault(_cg, set()).add(_sp)
        _SPLIT_CG = {cg for cg, pools in _pool_by_cg.items() if len(pools) > 1}
    print(f"✅ Pool split: {len(_MC_POOL_LABEL)} MC labels, {len(_SPLIT_CG)} split (cat,gauge): {sorted(_SPLIT_CG)}")
except Exception as _e_pm:
    print(f"⚠️ ไม่มีชีท MC_POOL_MAP (ข้าม pool split, ใช้พฤติกรรมเดิม): {_e_pm}")


def _mc_to_pool_label(mc_group, gauge):
    """pool label ต่อเครื่อง (เช่น 'PHET|SKPLE , SKPTA 26G') สำหรับกลุ่มที่แยกพูล
    คืน None ถ้าเครื่องไม่ได้อยู่ในกลุ่มที่แยกพูล → get_actual_mc_remain ใช้พฤติกรรมเดิม"""
    return _MC_POOL_LABEL.get((str(mc_group).strip().upper(), _normalize_gauge(gauge)))


# --- Lock_MC × พูล: แถวที่ระบุคอลัมน์ MC หักเฉพาะพูลของกลุ่มเครื่องนั้น ---
# ทำที่นี่เพราะต้องรอ _MC_POOL_LABEL / _SPLIT_CG (โหลดจาก MC_POOL_MAP ด้านบน) ก่อน
#   _LOCK_MC_ANY  = แถวที่เว้น MC ว่าง (หรือแปลงเป็นพูลไม่ได้) → หักจากทุกพูลของ CAT+เกจ เหมือนเดิม
#   _LOCK_MC_POOL = แถวที่ระบุ MC ใน CAT+เกจ ที่แยกพูล → หักเฉพาะพูลนั้นพูลเดียว
# เดิมหักด้วยคีย์ (สัปดาห์, CAT, เกจ) ล้วน ทำให้ล็อก 11 เครื่องที่ SKP 22G ไปหักพูล SKPTA 22G
# (มี 3 เครื่อง) ด้วย → เหลือ −8 ทั้งที่ไม่มีงาน แล้ววางงานลงพูลนั้นไม่ได้เลยทั้งสัปดาห์
_LOCK_MC_ANY: dict = {}    # (week, cat, gauge) → จำนวนที่หักทุกพูล
_LOCK_MC_POOL: dict = {}   # (week, cat, gauge, pool_label) → จำนวนที่หักเฉพาะพูลนั้น
for (_lw, _lcat, _lg_raw2, _lmc), _lv in _LOCK_MC_BY_MC.items():
    # เกจต้องผ่าน _normalize_gauge ให้ตรงกับคีย์ที่ get_actual_mc_remain ใช้ค้น ('22.0' → '22')
    _lg = _normalize_gauge(_lg_raw2)
    _lpool = _mc_to_pool_label(_lmc, _lg) if _lmc else None
    if _lmc and (_lcat, _lg) in _SPLIT_CG and _lpool:
        _lkp = (_lw, _lcat, _lg, _lpool)
        _LOCK_MC_POOL[_lkp] = _LOCK_MC_POOL.get(_lkp, 0) + _lv
    else:
        if _lmc and (_lcat, _lg) in _SPLIT_CG:
            # ระบุ MC ไว้แต่ไม่รู้จักเครื่องตัวนั้น (พิมพ์ผิด/ไม่มีใน MasterMC) → หักทุกพูลแบบเดิม
            print(f"⚠️ Lock_MC W{_lw} {_lcat} {_lg}G: ไม่รู้จักเครื่อง '{_lmc}' → หักทุกพูล")
        _LOCK_MC_ANY[(_lw, _lcat, _lg)] = _LOCK_MC_ANY.get((_lw, _lcat, _lg), 0) + _lv
if _LOCK_MC_POOL:
    print(f"✅ Lock_MC ต่อพูล: {_LOCK_MC_POOL}")


# =========================
# ก้อนเครื่องที่ใช้แทนกันได้ = โรง + CAT (Type_1) + เกจ + พูล เดียวกัน
#
# ทำไมต้องมี: item เดียวกันอาจถูกวางคนละ MC_GROUP ในก้อนเดียวกัน (เช่น SKPTA กับ SKPLE)
# ซึ่งเป็นเครื่องชุดเดียวกันในทางปฏิบัติ ถ้าคิดแยกกันจะเกิด 2 ปัญหา:
#   1) ปัดเศษเครื่องคนละรอบ (ceil สองครั้ง) → เครื่องรวมเกินความจริง
#   2) สลับกลุ่มแล้วถือเป็นลูกโซ่ใหม่ → setup ซ้ำทั้งที่เครื่องยังอุ่น
# → ใช้ "MC ตัวแทน" เป็นคีย์ภายใน (carry/setup/booking) และ CAP ของก้อนใช้ค่าสูงสุด
#   (เครื่องที่ทอได้เร็วสุดเป็นตัวกำหนด ตามที่ user กำหนด)
# ปิดกลับพฤติกรรมเดิมได้ที่หน้า "ตั้งค่าระบบ"
# =========================
GROUP_MC_BY_CAT = plan_config.get("KP_GROUP_BY_CAT")
_MC_CANON: dict = {}        # (mc_upper, gauge_str) → MC ตัวแทนของก้อน
_MC_CANON_MEMBERS: dict = {}  # (mc ตัวแทน, gauge_str) → รายชื่อ MC ในก้อน

if GROUP_MC_BY_CAT:
    _canon_groups: dict = {}
    for _, _cr in master_mc.iterrows():
        _c_mc = str(_cr.get("MC", "")).strip().upper()
        _c_cat = str(_cr.get("Type_1", "")).strip().upper()
        if not _c_mc or not _c_cat:
            continue
        _c_g = _normalize_gauge(_cr.get("Guage", ""))
        _c_fac = str(_cr.get("Factory", "")).strip().upper()
        # ป้ายพูลจากชีท MC_POOL_MAP (AVA_MC สร้าง) เป็นแหล่งความจริงเดียวกันทั้งสองฝั่ง —
        # ในนั้นมี Factory อยู่แล้ว (เช่น "PHET|SKPLE , SKPTA 26G" / "PHET|DOUBLE-30:22")
        # ไม่มีป้าย (เครื่องไม่อยู่ใน MasterMC ตอน AVA รัน) → ถอยไปใช้ โรง+CAT+เกจ
        _c_pool = _MC_POOL_LABEL.get((_c_mc, _c_g)) or f"{_c_fac}|{_c_cat}"
        _canon_groups.setdefault((_c_pool, _c_g), set()).add(_c_mc)
    for (_c_pool, _c_g), _c_mcs in _canon_groups.items():
        _rep = sorted(_c_mcs)[0]
        _MC_CANON_MEMBERS[(_rep, _c_g)] = sorted(_c_mcs)
        _MC_CANON_MEMBERS.setdefault((_rep, ""), sorted(_c_mcs))
        for _c_mc in _c_mcs:
            _MC_CANON[(_c_mc, _c_g)] = _rep
            # ไม่รู้เกจ (คีย์เก่า/แถวที่ไม่มีเกจ) → ใช้ก้อนแรกที่เจอของเครื่องนี้
            _MC_CANON.setdefault((_c_mc, ""), _rep)
    _multi = {k: v for k, v in _MC_CANON_MEMBERS.items() if len(v) > 1}
    print(f"✅ ก้อนเครื่อง (โรง+CAT+เกจ+พูล): {len(_MC_CANON_MEMBERS)} ก้อน, รวมหลายกลุ่ม {len(_multi)} ก้อน")
else:
    print("ℹ️ KP_GROUP_BY_CAT=0 → คิดเครื่องแยกต่อ MC_GROUP เหมือนเดิม")


def _canon_mc(mc_group, gauge=None) -> str:
    """คืน MC ตัวแทนของก้อนที่เครื่องนี้อยู่ (ไม่รู้จัก/ปิดฟีเจอร์ → คืนชื่อเดิม)"""
    mc = str(mc_group).strip().upper() if mc_group is not None else ""
    if not GROUP_MC_BY_CAT or not mc:
        return mc
    g = _normalize_gauge(gauge)
    return _MC_CANON.get((mc, g)) or _MC_CANON.get((mc, "")) or mc


def _canon_members(mc_group, gauge=None) -> list:
    """คืนรายชื่อ MC_GROUP ทั้งหมดในก้อนเดียวกับเครื่องนี้ (รวมตัวมันเอง)"""
    mc = str(mc_group).strip().upper() if mc_group is not None else ""
    if not GROUP_MC_BY_CAT or not mc:
        return [mc] if mc else []
    g = _normalize_gauge(gauge)
    return _MC_CANON_MEMBERS.get((_canon_mc(mc, g), g)) or [mc]


_ITEM_PLAN_MC_CACHE: dict = {}


def _item_plan_mc(item_code, mc_group, gauge=None) -> str:
    """กลุ่มเครื่องที่แผน "จะใช้จริง" ให้ item นี้ในก้อนเดียวกับ mc_group

    = กลุ่มที่ CAP ของ item สูงสุดในก้อน (เท่ากัน → ชื่อเรียงก่อน) ซึ่งเป็นตัวเดียวกับที่
    _dedup_mc_by_group เลือกไว้ตอนหาเครื่อง → คีย์ที่ยังอิงชื่อ MC_GROUP ตรง ๆ
    (remaining_week_cap ฯลฯ) ที่ seed มาจาก booking จะตรงกับแถวแผนเสมอ
    """
    mc = str(mc_group).strip().upper() if mc_group is not None else ""
    if not GROUP_MC_BY_CAT or not mc:
        return mc
    it = str(item_code).strip().upper() if item_code is not None else ""
    g = _normalize_gauge(gauge)
    _key = (it, _canon_mc(mc, g), g)
    if _key in _ITEM_PLAN_MC_CACHE:
        return _ITEM_PLAN_MC_CACHE[_key]
    _members = _canon_members(mc, g)
    _pick = mc
    if len(_members) > 1:
        _rows = item_cap_data[
            (item_cap_data["ITEM_CODE"].astype(str).str.strip().str.upper() == it)
            & (item_cap_data["MC_GROUP"].astype(str).str.strip().str.upper().isin(_members))
        ]
        _picked = _dedup_mc_by_group(_rows)
        if _picked is not None and not _picked.empty:
            _pick = str(_picked.iloc[0]["MC_GROUP"]).strip().upper()
    _ITEM_PLAN_MC_CACHE[_key] = _pick
    return _pick


def _dedup_mc_by_group(cap_rows):
    """ยุบผู้สมัครเครื่องให้เหลือกลุ่มเดียวต่อก้อน — เก็บกลุ่มที่ CAP สูงสุดไว้

    งานของ item เดียวกันในก้อนเดียวกันต้องอยู่กลุ่มเดียวตลอด ไม่งั้นยอดถูกแตกไปคนละกลุ่ม
    แล้วปัดเศษเครื่องคนละรอบ (คีย์ภายในหลายตัวยังอิงชื่อ MC_GROUP ตรง ๆ)
    """
    if not GROUP_MC_BY_CAT or cap_rows is None or cap_rows.empty:
        return cap_rows
    _best: dict = {}
    for _ix, _r in cap_rows.iterrows():
        _mc = str(_r.get("MC_GROUP", "")).strip().upper()
        _g = _normalize_gauge(_r.get("GUAGE"))
        _raw = float(pd.to_numeric(_r.get("CAP ทอ"), errors="coerce") or 0)
        _eff = _raw * (_base_work_hours(_mc, _g) / 24) if _raw > 0 else 0
        _key = (_canon_mc(_mc, _g), _g)
        _cur = _best.get(_key)
        # เท่ากัน → เลือกชื่อกลุ่มที่เรียงก่อน เพื่อให้ผลรันซ้ำเหมือนเดิมทุกครั้ง
        if _cur is None or _eff > _cur[0] + 1e-9 or (abs(_eff - _cur[0]) <= 1e-9 and _mc < _cur[1]):
            _best[_key] = (_eff, _mc, _ix)
    _keep = [_v[2] for _v in _best.values()]
    return cap_rows.loc[_keep] if len(_keep) < len(cap_rows) else cap_rows


# CAP ของก้อน = ค่าสูงสุดของ item นั้นในก้อน (เทียบกันที่ "กำลังผลิตจริงต่อวัน"
# คือหลังคูณชั่วโมงทำงานของกลุ่มแล้ว) จากนั้นเขียนกลับเป็นฐาน 24 ชม.ของแต่ละกลุ่ม
# → ทุกจุดที่อ่าน item_cap_data / _get_capacity_for_mc_group ได้ค่าเดียวกันทั้งก้อน
if GROUP_MC_BY_CAT and not item_cap_data.empty:
    _eff_max: dict = {}
    for _, _cap_r in item_cap_data.iterrows():
        _cp_it = str(_cap_r.get("ITEM_CODE", "")).strip().upper()
        _cp_mc = str(_cap_r.get("MC_GROUP", "")).strip().upper()
        _cp_g = _normalize_gauge(_cap_r.get("GUAGE"))
        _cp_raw = float(pd.to_numeric(_cap_r.get("CAP ทอ"), errors="coerce") or 0)
        if not _cp_it or not _cp_mc or _cp_raw <= 0:
            continue
        _cp_eff = _cp_raw * (_base_work_hours(_cp_mc, _cp_g) / 24)
        _cp_key = (_cp_it, _canon_mc(_cp_mc, _cp_g), _cp_g)
        if _cp_eff > _eff_max.get(_cp_key, 0):
            _eff_max[_cp_key] = _cp_eff

    _cap_bumped = 0
    for _ix, _cap_r in item_cap_data.iterrows():
        _cp_it = str(_cap_r.get("ITEM_CODE", "")).strip().upper()
        _cp_mc = str(_cap_r.get("MC_GROUP", "")).strip().upper()
        _cp_g = _normalize_gauge(_cap_r.get("GUAGE"))
        _cp_raw = float(pd.to_numeric(_cap_r.get("CAP ทอ"), errors="coerce") or 0)
        _cp_eff_max = _eff_max.get((_cp_it, _canon_mc(_cp_mc, _cp_g), _cp_g), 0)
        if _cp_eff_max <= 0:
            continue
        _cp_hours = _base_work_hours(_cp_mc, _cp_g)
        if _cp_hours <= 0:
            continue
        _cp_new = _cp_eff_max / (_cp_hours / 24)
        if _cp_new > _cp_raw + 1e-6:
            item_cap_data.at[_ix, "CAP ทอ"] = _cp_new
            _cap_bumped += 1
            print(f"[CAP ก้อน] {_cp_it} {_cp_mc} G{_cp_g}: CAP {_cp_raw:.2f} → {_cp_new:.2f} (ใช้ค่าสูงสุดของก้อน)")
    print(f"✅ ปรับ CAP ให้เท่ากันทั้งก้อน: {_cap_bumped} แถว")


def _mc_special_remain(mc_group, week, gauge, item_code):
    """คืนจำนวนเครื่อง POLY/COTTON sub-pool ที่ยัง setup ใหม่ได้ = กันไว้ − booking − แผน(new)
    คืน None ถ้า item นี้ไม่อยู่ในกลุ่มที่กันเครื่อง (ไม่ต้อง hard cap)
    ใช้ hard-block: setup ใหม่ห้ามเกินเครื่องที่กันไว้ (carryover เครื่องเดิมไม่นับตรงนี้)"""
    if not _MC_SPECIAL_PLAN or not item_code:
        return None
    sp_type = _get_item_cotton_poly(item_code)
    if not sp_type:
        return None
    gauge_str = _normalize_gauge(gauge)
    mc_u = str(mc_group).strip().upper()
    factory = _mc_to_factory(mc_u, gauge_str)
    cat = _mc_to_type1(mc_u, gauge_str)
    entry = (
        _MC_SPECIAL_PLAN.get((factory, cat, mc_u, gauge_str))
        or _MC_SPECIAL_PLAN.get((factory, cat, "", gauge_str))
    )
    if not entry:
        return None
    reserved = entry.get(sp_type, 0)
    if reserved <= 0:
        return None
    used = max(
        _mc_special_weekly_usage.get((factory, cat, gauge_str, week, sp_type), 0),
        _sp_used_from_plans(factory, cat, gauge_str, week, sp_type),
    )
    bk = _MC_SPECIAL_BOOKING_USED.get((cat, gauge_str, week, sp_type), 0)
    return max(0, reserved - bk - used)


def get_actual_mc_remain(mc_group, week, gauge, item_code=None, allow_negative=False):

    """คืนค่าจำนวนเครื่องว่างจริงของ pool = TOTAL_MC_REMAIN จาก summary_mc

    ดูจาก Type_1 + GUAGE (รวมเครื่องทุก MC ใน Type_1+Gauge เดียวกัน)

    allow_negative=True → คืนค่าติดลบได้เมื่อ booking จองเกินเครื่องจริง (จองเกิน 5 → -5)
    ค่าปกติ (False) ตัดที่ 0 ทำให้ "ว่างพอดี 0" กับ "จองเกิน 5" แยกกันไม่ออก
    ใช้ True เฉพาะจุดที่ต้องรู้ว่าพูลเต็มแค่ไหน เช่น carry gate / look-ahead
    """

    gauge_str = _normalize_gauge(gauge)

    if not gauge_str:

        return 0



    # MC_GROUP_REDIRECT ยังคงใช้ได้ (redirect ก่อนแปลง Type_1)

    mc_group, gauge_str = _apply_mc_redirect(mc_group, gauge_str)



    # แปลง MC_GROUP → Type_1 เพื่อดูเครื่องระดับ pool

    type_1 = _mc_to_type1(mc_group, gauge_str)

    # พูลแยก (เช่น SINGLE-32: SKP vs SKPTA/SKPLE เป็นคนละชุดเครื่อง ใช้แทนกันไม่ได้)
    # → คิดเครื่องเฉพาะพูลของเครื่องนี้ ไม่รวมพูลอื่นใน Type_1+Gauge เดียวกัน
    _pool_label = _mc_to_pool_label(mc_group, gauge_str)
    _is_split = (
        _pool_label is not None
        and (type_1, gauge_str) in _SPLIT_CG
        and "MC_POOL" in summary_mc.columns
    )

    mc_rows = summary_mc[

        week_mask(summary_mc, week)

        & (summary_mc["TYPE_1"] == type_1)

        & (summary_mc["GUAGE"].apply(_normalize_gauge) == gauge_str)

    ]

    if mc_rows.empty:

        # Fallback: รวม Total MC จาก master_mc ทุก MC ใน Type_1+Gauge
        # (MC_Total ของสัปดาห์นี้ ถ้ามี ให้ชนะค่าจาก master — ไม่งั้นสัปดาห์ที่ไม่มีแถวใน
        #  summary จะได้จำนวนเครื่องเก่า ขัดกับที่ user กำหนดไว้)
        _mt_ov = _MC_TOTAL_OVERRIDE.get((int(week), type_1, gauge_str))
        # แถวที่ระบุเครื่อง (เช่น SKP) = ยอดของพูลนั้น → ใช้เมื่อเครื่องที่กำลังถามอยู่พูลเดียวกัน
        # (ชนะยอดระดับ CAT เพราะเจาะจงกว่า) — ถ้าไม่ใช่พูลเดียวกัน ปล่อยให้ใช้ค่าเดิม
        if _MC_TOTAL_OVERRIDE_MC:
            _mc_u_now = str(mc_group).strip().upper()
            for (_ovw, _ovc, _ovg, _ovmc), _ovv in _MC_TOTAL_OVERRIDE_MC.items():
                if (_ovw, _ovc, _ovg) != (int(week), type_1, gauge_str):
                    continue
                if _ovmc == _mc_u_now or (
                    _pool_label is not None
                    and _mc_to_pool_label(_ovmc, gauge_str) == _pool_label
                ):
                    _mt_ov = _ovv
                    break

        _mm_filter = (

            (master_mc["Type_1"].astype(str).str.strip() == type_1)

            & (master_mc["Guage"].apply(_normalize_gauge) == gauge_str)

        )

        _mm_rows = master_mc[_mm_filter]

        if _mm_rows.empty and _mt_ov is None:
            # gauge นี้ไม่มีใน master (เช่น gauge ใหม่จาก cylinder change)
            # ต้องตรวจ cylinder adjustments ก่อน return 0
            _cyl_adj_only = sum(
                v for (w, _f, cat, g), v in cylinder_adjustments.items()
                if w == week and cat == type_1 and g == gauge_str
            )
            return max(0, _cyl_adj_only)

        base_remain = (
            int(_mt_ov) if _mt_ov is not None
            else int(pd.to_numeric(_mm_rows["Total MC"], errors="coerce").fillna(0).sum())
        )

    else:

        # พูลแยก → คิดเฉพาะพูลของเครื่องนี้ (SKP กับ SKPTA/SKPLE เป็นคนละชุด ใช้แทนกันไม่ได้)
        # กลุ่มที่ไม่แยก → รวมทุกพูลย่อยของ Type_1+Gauge เหมือนเดิม (พูลติดลบ = จองเกิน ดึงยอดรวมลง)
        _pool_rows = (
            mc_rows[mc_rows["MC_POOL"].astype(str).str.strip() == _pool_label]
            if _is_split else mc_rows
        )
        base_remain = int(_pool_rows["TOTAL_MC_REMAIN"].sum())



    # key สำหรับ pool tracking — พูลแยกใช้ pool label, อื่นๆ ใช้ (type_1, gauge)
    if _is_split:
        already_used = weekly_new_plan_pool_usage.get(week, {}).get(_pool_label, 0)
    else:
        already_used = weekly_new_plan_usage.get(week, {}).get((type_1, gauge_str), 0)

    # รวม cylinder adjustments (factory-agnostic: รวม delta ทุก factory ที่มี mc_cat+gauge เดียวกัน)
    _cyl_adj = sum(
        v for (w, _f, cat, g), v in cylinder_adjustments.items()
        if w == week and cat == type_1 and g == gauge_str
    )

    # _floor: ตัดที่ 0 ตามปกติ แต่ปล่อยค่าติดลบผ่านเมื่อ allow_negative
    _floor = (lambda v: v) if allow_negative else (lambda v: max(0, v))

    result = _floor(base_remain - already_used + _cyl_adj)

    # หักเครื่อง Lock_MC (เครื่องที่ถูก lock ไม่ให้วางแผน)
    # พูลแยก → หักเฉพาะล็อกที่ไม่ระบุกลุ่มเครื่อง + ล็อกที่ชี้มาที่พูลนี้
    # (ล็อกของพูลพี่น้อง เช่น SKP 22G ไม่มีผลกับ SKPTA 22G เพราะคนละชุดเครื่อง)
    # ไม่แยกพูล/แมปพูลไม่ได้ = เครื่องก้อนเดียว → หักทั้งหมดเหมือนเดิม
    _lock_key = (int(week), type_1, gauge_str)
    if _is_split:
        _lock_count = (_LOCK_MC_ANY.get(_lock_key, 0)
                       + _LOCK_MC_POOL.get(_lock_key + (_pool_label,), 0))
    else:
        _lock_count = _LOCK_MC_MAP.get(_lock_key, 0)
    if _lock_count > 0:
        result = _floor(result - _lock_count)

    # MC Special cap: แบ่ง COTTON/POLY sub-pool ตามชีท MC Special
    if _MC_SPECIAL_PLAN:
        _sp_mc_u = str(mc_group).strip().upper()
        _sp_factory = _mc_to_factory(_sp_mc_u, gauge_str)
        _sp_cat = type_1
        _ms_entry = (
            _MC_SPECIAL_PLAN.get((_sp_factory, _sp_cat, _sp_mc_u, gauge_str))
            or _MC_SPECIAL_PLAN.get((_sp_factory, _sp_cat, "", gauge_str))
        )
        if _ms_entry:
            _sp_type = _get_item_cotton_poly(item_code) if item_code else ""
            _cotton_reserved = _ms_entry.get("COTTON", 0)
            _poly_reserved = _ms_entry.get("POLY", 0)
            # โมเดลเดียวกับ AVA_MC: 1 item อยู่ได้พูลเดียว ห้ามล้นข้ามพูล
            # base_remain (SUMMARY_MC_REMAIN) = พูลทั่วไปที่หัก reserved ออกให้แล้ว
            # → item ทั่วไปใช้ base_remain ตรงๆ, COTTON/POLY ใช้เฉพาะ sub-pool ของตัวเอง
            if _sp_type == "COTTON" and _cotton_reserved > 0:
                _sp_used = max(
                    _mc_special_weekly_usage.get((_sp_factory, _sp_cat, gauge_str, week, "COTTON"), 0),
                    _sp_used_from_plans(_sp_factory, _sp_cat, gauge_str, week, "COTTON"),
                )
                _sp_bk = _MC_SPECIAL_BOOKING_USED.get((_sp_cat, gauge_str, week, "COTTON"), 0)
                result = max(0, _cotton_reserved - _sp_bk - _sp_used)
            elif _sp_type == "POLY" and _poly_reserved > 0:
                _sp_used = max(
                    _mc_special_weekly_usage.get((_sp_factory, _sp_cat, gauge_str, week, "POLY"), 0),
                    _sp_used_from_plans(_sp_factory, _sp_cat, gauge_str, week, "POLY"),
                )
                _sp_bk = _MC_SPECIAL_BOOKING_USED.get((_sp_cat, gauge_str, week, "POLY"), 0)
                result = max(0, _poly_reserved - _sp_bk - _sp_used)

    # TYPE_SPECIAL quota check (BABY FRENCH / SINGLE JACQUARD / TWILL)
    if _TYPE_DESC_RULES_PLAN and item_code:
        _ts_mc_u = str(mc_group).strip().upper()
        _ts_fac  = _mc_to_factory(_ts_mc_u, gauge_str)
        _ts_type = _mc_to_type_raw_plan.get((_ts_mc_u, gauge_str), "").strip().upper()
        _ts_rule_key = (_ts_fac.upper(), _ts_type)
        if _ts_rule_key in _TYPE_DESC_RULES_PLAN:
            _ts_rule   = _TYPE_DESC_RULES_PLAN[_ts_rule_key]
            _ts_mc_cat = _ts_rule.get("mc_cat", "")
            _ts_t1     = _mc_to_type1(_ts_mc_u, gauge_str)
            if not ((_ts_mc_cat and _ts_t1 != _ts_mc_cat) or gauge_str == "20"):
                _ts_desc = _item_desc_map_plan.get(str(item_code).strip().upper(), "")
                if _is_description_special_type_plan(_ts_desc, _ts_rule["keywords"]):
                    _ts_max      = _ts_rule["max_mc"]
                    _ts_bk_used  = _type_special_booking_usage.get((_ts_fac, _ts_type, week), 0)
                    _ts_new_used = _type_special_weekly_usage.get((_ts_fac, _ts_type, week), 0)
                    result = min(result, max(0, _ts_max - _ts_bk_used - _ts_new_used))

    return result





def _item_producing_at(item_code, mc_key, mc_group, w_idx) -> bool:
    """item นี้ (บนกลุ่มเครื่องนี้) มีงานเดินอยู่ในสัปดาห์ w_idx ไหม — จาก booking หรือแผนที่วางไปแล้ว"""
    if booking_mc_by_week.get(mc_key, {}).get(w_idx, 0) > 0:
        return True
    if mc_key in booking_active_week_set and w_idx in booking_active_week_set[mc_key]:
        return True
    for pe in plans:
        if pe.get("ITEM_CODE") == item_code and pe.get("MC_GROUP") == mc_group:
            if week_index(pe.get("PLAN_WEEK")) == w_idx:
                # นับเฉพาะแถวที่ยังถือเครื่องจริง — แถวที่รอบซ่อมตัดเครื่องเหลือ 0 (ยังค้างในแผนจนถูกลบท้ายรอบ)
                # ไม่ได้เดินเครื่องแล้ว ถ้านับจะข้ามการตรวจพูลสัปดาห์กลาง → carry ข้ามสัปดาห์ที่เต็มได้
                # (เคสจริง 14 ก.ย. 2569 FD1FA54/08B0 RAOO: W42 ถูกตัดเหลือ 0 แล้ว W43 carry ข้าม W42 ที่เต็ม)
                _pe_mc = pd.to_numeric(pe.get("ACTUAL_MC", 0), errors="coerce")
                if pd.notna(_pe_mc) and _pe_mc > 0:
                    return True
    return False


def _carry_cap_by_gap(item_code, mc_key, mc_group, item_gauge, prev_week_idx, current_week_idx):
    """เพดานจำนวนเครื่องที่ carry ข้าม "สัปดาห์ที่เว้น" ได้ = เครื่องว่างต่ำสุดของสัปดาห์กลาง

    เครื่องจะ "ยังอุ่น" ข้าม gap ได้ ก็ต่อเมื่อมันจอดว่างอยู่จริงตลอดสัปดาห์ที่ไม่ได้ผลิต
    ถ้าสัปดาห์กลางถูก item อื่นใช้จนพูลว่างเหลือน้อยกว่าเครื่องที่จะ carry แปลว่าเครื่องชุดนั้น
    ถูกยึดไปแล้ว → กลับมาผลิตอีกครั้งต้อง setup ใหม่ตามส่วนที่ขาด

    (เดิมเป็น _carry_blocked_by_gap ที่ตัด carry เฉพาะตอนว่าง = 0 เป๊ะ ทำให้เคส
     "จะ carry 3 แต่สัปดาห์กลางว่าง 1" ยัง carry ครบ 3 ทั้งที่เครื่องหายไป 2)

    Returns:
      None = ไม่มีสัปดาห์กลาง (สัปดาห์ติดกัน/สัปดาห์เดียวกัน) → ไม่จำกัด
      int  = จำนวนเครื่องสูงสุดที่ carry ได้ (0 = ต้อง setup ใหม่ทั้งหมด)
    """
    if prev_week_idx is None or current_week_idx is None:
        return None

    if current_week_idx - prev_week_idx <= 1:
        return None  # ติดกัน หรือ same week → ไม่มี intermediate weeks

    cap = None
    for w_idx in range(prev_week_idx + 1, current_week_idx):
        if w_idx >= len(calendar_week):
            break

        # item นี้เองยังเดินอยู่ในสัปดาห์กลาง → เครื่องไม่ได้ถูกปล่อย ไม่ต้องแย่งพูล
        if _item_producing_at(item_code, mc_key, mc_group, w_idx):
            continue

        w_num = int(calendar_week.iloc[w_idx]["WEEK"])
        remaining = get_actual_mc_remain(mc_group, w_num, item_gauge, item_code=item_code)
        cap = remaining if cap is None else min(cap, remaining)

        if cap <= 0:
            prev_w_num = (
                int(calendar_week.iloc[prev_week_idx]["WEEK"])
                if prev_week_idx < len(calendar_week) else "?"
            )
            print(
                f"[CARRY BLOCKED GAP] {item_code} W{w_num}: เครื่องว่าง=0 จาก item อื่น "
                f"→ ตัด carry จาก W{prev_w_num} (ต้อง setup ใหม่)"
            )
            return 0

    return cap





def get_total_pending_qty_for_item(item_code, current_sc_so, current_fg_week, orders_df, same_sc_only=False):

    """รวม Pending Plan ของทุก FG สำหรับ item เดียวกัน (รวมทุก SC/SO)

    ใช้สำหรับคำนวณ machine allocation จาก total demand แทนที่จะดูแค่ FG เดียว

    

    Args:

        same_sc_only: ถ้า True จะรวมเฉพาะ SC/SO เดียวกัน (ใช้สำหรับ carry optimization)

    

    คืนค่า: total_pending_qty รวมของ FG ปัจจุบัน + FG ถัดไปทั้งหมด

    (เฉพาะ FG Week >= current_fg_week เพื่อไม่นับ FG ที่ผ่านไปแล้ว)

    """

    if orders_df is None or orders_df.empty:

        return 0.0

    

    same_item_orders = orders_df[orders_df["Item Code"] == item_code]

    if same_item_orders.empty:

        return 0.0

    

    try:

        current_fg_int = int(current_fg_week) if pd.notna(current_fg_week) else 0

    except (ValueError, TypeError):

        current_fg_int = 0

    

    total_qty = 0.0

    for _, row in same_item_orders.iterrows():

        # ถ้า same_sc_only=True → กรองเฉพาะ SC/SO เดียวกัน

        if same_sc_only:

            row_sc = str(row.get("SO_NO", row.get("SC/SO NO", ""))).strip()

            if row_sc != current_sc_so:

                continue

        

        fg_week = row.get("FG Week")

        try:

            fg_int = int(fg_week) if pd.notna(fg_week) else 0

        except (ValueError, TypeError):

            continue

        

        # รวมเฉพาะ FG Week >= current (FG ปัจจุบัน + อนาคต)

        if fg_int >= current_fg_int:

            pending = pd.to_numeric(row.get("Pending Plan", 0), errors="coerce")

            if not pd.isna(pending) and pending > 0:

                total_qty += float(pending)

    

    return total_qty





# =========================
# FOLD ROUNDING — จำนวนพับ = qty ÷ REVOLUTION_WEIGHT
# ทอครึ่งพับไม่ได้ → ทุกกลุ่มปัดลงเป็นจำนวนเต็มพับ
# กลุ่ม IRMT/SJT ปัดที่ระดับ "ต่อเครื่อง" (เครื่องละกี่พับก็ได้ ขอให้เต็มพับ) แล้วคูณจำนวนเครื่อง
#
# เงื่อนไข "เป็นพับคู่ (หาร 2 ลงตัว)" เป็นเรื่องของ "ยอดรวมทั้ง order" ไม่ใช่ของแต่ละสัปดาห์
# → ตรวจที่ ORDERS_QTY ด้วย fold_check_order() แล้วเตือน (ดู FOLD_WARN ในชีท PLAN)
#   สัปดาห์ไหนจะทอกี่พับก็ได้ ขอแค่รวมกันครบยอด order
# =========================
FOLD_ROUND_MC_GROUPS = set(plan_config.get_list("KP_FOLD_ROUND_MC_GROUPS", upper=True))
FOLD_MULTIPLE = plan_config.get("KP_FOLD_MULTIPLE")


def fold_round(total_cap, rev_weight, mc_group, per_mc_cap=None, mc_count=None):
    """ปัด capacity ลงตามจำนวนพับ (จำนวนเต็มพับ — สัปดาห์ละกี่พับก็ได้)
    - กลุ่มอื่น: (total_cap // rev_weight) * rev_weight
    - IRMT/SJT (ต้องส่ง per_mc_cap + mc_count): ปัดจำนวนพับ "ต่อเครื่อง" ลงเป็นจำนวนเต็ม
      แล้วคูณจำนวนเครื่อง
    """
    if not rev_weight or rev_weight <= 0:
        return total_cap
    if (
        per_mc_cap is not None
        and mc_count
        and str(mc_group).strip().upper() in FOLD_ROUND_MC_GROUPS
    ):
        folds = int(per_mc_cap // rev_weight)
        return folds * rev_weight * int(mc_count)
    return int(total_cap // rev_weight) * rev_weight


def fold_round_mixed(
    total_cap, rev_weight, mc_group,
    carry_cap_per_mc, carry_mc, new_cap_per_mc, new_mc,
):
    """เหมือน fold_round แต่รองรับเครื่อง 2 กลุ่มที่ cap/เครื่องไม่เท่ากัน
    (carry = วิ่งเต็มสัปดาห์, new = ถูกหักวัน setup)
    - IRMT/SJT: ปัดจำนวนพับต่อเครื่องเป็นจำนวนเต็ม แยกกลุ่ม carry/new แล้วรวมกัน
    - กลุ่มอื่น: ปัดยอดรวมทั้งก้อนเป็นจำนวนพับเต็ม
    """
    if not rev_weight or rev_weight <= 0:
        return total_cap
    if str(mc_group).strip().upper() in FOLD_ROUND_MC_GROUPS:
        return (
            fold_round(carry_cap_per_mc * carry_mc, rev_weight, mc_group, carry_cap_per_mc, carry_mc)
            + fold_round(new_cap_per_mc * new_mc, rev_weight, mc_group, new_cap_per_mc, new_mc)
        )
    return int(total_cap // rev_weight) * rev_weight


def fold_keep_qty(qty, cap, rev_weight):
    """ยอดที่แถวเดิม "เก็บไว้ทอ" ได้ เมื่อเครื่องของแถวถูกตัดออกบางส่วน (รอบซ่อม / งานสียึดเครื่อง)

    แหล่งเดียวของกติกานี้ — ทุกรอบซ่อมต้องเรียกตัวนี้ ห้ามปัดเอง:
      1. เครื่องที่เหลือทอยอดเดิมได้ครบ → คืนยอดเดิมทั้งก้อน (ห้ามปัด ไม่งั้นสร้างเศษขึ้นมาเอง)
      2. ทอไม่ครบจริง → ปัดยอดที่เก็บลงเป็นม้วนเต็ม ให้ส่วนที่ตัดออกเป็นม้วนเต็มไปทอต่อที่อื่นได้
      3. แต่ถ้าส่วนที่จะตัดออก "ไม่ถึง 1 ม้วน" → ทอให้จบที่แถวเดิม
         เศษขนาดนั้นแยกไปทอสัปดาห์อื่นไม่ได้จริง (ต้องกินเครื่องทั้งตัว) เดิมมันไหลไปเกาะ
         แถวของใบอื่น → ใบนี้ขึ้น "วางไม่ครบ" ใบนั้นขึ้น "เกิน"
         (เคสจริง 16 ก.ย. 2569 FD4DRTPG03/09A0 SC 715028: ขาด 1.21 / เกิน 1.21)
    """
    _q = float(qty or 0)
    _keep = min(_q, float(cap or 0))
    _rw = float(rev_weight or 0)
    if _keep <= 0:
        return 0.0
    if _rw > 0 and _keep + 1e-6 < _q:
        _rounded = (_keep // _rw) * _rw
        _keep = _rounded if (_q - _rounded) >= _rw else _q
    return max(0.0, _keep)


def fold_check_order(orders_qty, rev_weight, mc_groups=None):
    """ตรวจยอดที่ลูกค้าเปิดมาว่าแบ่งเป็นพับแล้วเป็นพับคู่ (หาร 2 ลงตัว) หรือไม่

    ⚠️ กฎ "พับคู่" ใช้เฉพาะกลุ่ม IRMT / SJT เท่านั้น — กลุ่มอื่นไม่มีข้อบังคับนี้
    (mc_groups = MC_GROUP ทั้งหมดที่ order นี้ใช้; ไม่ส่ง = ไม่เช็ค)

    คืน (folds, remainder, warn)
      folds     = จำนวนพับของยอด order (ปัดทศนิยม 2 ตำแหน่ง — เศษพับก็ต้องเห็น)
      remainder = เศษพับที่ไม่เป็นคู่ (0 = เป็นพับคู่พอดี)
      warn      = True เมื่อเป็น IRMT/SJT แล้วหารไม่ลงตัว
    แผนยังวางเต็มตามยอดจริงเสมอ — ค่านี้ใช้เตือน planner ให้ไปแก้ที่ต้นทาง (ยอดเปิด order)
    """
    # ⚠️ ผู้เรียกส่ง MC_GROUP.unique() (numpy array) มา — ห้ามใช้ `mc_groups or []`
    # เพราะ `or` ต้องแปลง array เป็น bool ซึ่งพังทันทีเมื่อ SC นั้นกระจายงานลง ≥2 MC_GROUP
    # (ValueError: truth value of an array with more than one element is ambiguous)
    if mc_groups is None:
        _mcs = []
    elif isinstance(mc_groups, str):
        _mcs = [mc_groups]          # ส่งมาค่าเดียว → ห่อเป็น list (list() จะแตกเป็นตัวอักษร)
    else:
        try:
            _mcs = list(mc_groups)  # numpy array / Series / tuple → แปลงตรง ๆ ไม่ผ่าน bool()
        except TypeError:
            _mcs = [mc_groups]
    if not any(
        str(_m).strip().upper() in FOLD_ROUND_MC_GROUPS for _m in _mcs
    ):
        return (0.0, 0.0, False)
    try:
        _qty = float(orders_qty or 0)
        _rw = float(rev_weight or 0)
    except (TypeError, ValueError):
        return (0.0, 0.0, False)
    if _qty <= 0 or _rw <= 0:
        return (0.0, 0.0, False)
    folds = round(_qty / _rw, 2)
    remainder = round(folds % FOLD_MULTIPLE, 2)
    return (folds, remainder, remainder != 0)


def compute_backward_start_idx(
    item_code, order_qty, rdd_idx, mc_group, daily_cap, item_gauge, n_machines,
    setup_days=SETUP_DAYS, rev_weight=None, floor_idx=0
):
    """Backward scheduling: หา week index ที่ "ช้าที่สุด" ที่ยังผลิตจบทัน rdd_idx

    ไล่ถอยหลังจาก rdd_idx ลงมาถึง floor_idx สะสม capacity สัปดาห์ต่อสัปดาห์
    จนกว่าจะพอกับ order_qty → คืน index นั้น (= JIT start)

    - target = rdd_idx ตรง ๆ (ไม่มี safety buffer)
    - setup_days หักเฉพาะสัปดาห์แรกที่เริ่มผลิต
    - floor_idx = พื้นแข็ง (TODAY_IDX+2) ห้ามถอยเลยไปกว่านี้
    - คืน None ถ้าไม่ทันแม้เริ่มที่ floor_idx → caller ใช้ ASAP แทน
    """
    if rdd_idx is None or not mc_group or not n_machines or not daily_cap:
        return None
    try:
        _qty_need = float(order_qty or 0)
    except (TypeError, ValueError):
        return None
    if _qty_need <= 0:
        return None

    floor_idx = max(0, int(floor_idx))
    rdd_idx = int(rdd_idx)
    if rdd_idx < floor_idx or rdd_idx >= len(calendar_week):
        return None

    # cache ข้อมูลรายสัปดาห์ (avail / working days) ช่วง floor_idx..rdd_idx ครั้งเดียว
    _wk_info = {}
    for _i in range(floor_idx, rdd_idx + 1):
        _wk = int(calendar_week.iloc[_i]["WEEK"])
        _wk_info[_i] = {
            "week": _wk,
            # ก้อนสี: รวมเครื่องที่ยึดคืนได้ (สิทธิ์เดียวกับตอนวางจริง) ไม่งั้น backward
            # สรุปว่า "เริ่มช้าไม่ได้เพราะพูลว่าง 0" ทั้งที่ยึดเครื่องมาได้
            "avail": _avail_mc_for_order(mc_group, _wk, item_gauge, item_code),
            "wd": get_working_days_by_factory(mc_group, 1, week=_wk, item_code=item_code, gauge=item_gauge),
        }

    # ไล่ถอยหลัง: หา _start ที่ช้าที่สุดที่ capacity สะสม (_start..rdd_idx) >= qty
    #
    # ⚠️ ต้องคิดตาม "ทรงเครื่องที่ขึ้นได้จริง" ไม่ใช่สมมติว่ามี n_machines ครบตั้งแต่สัปดาห์แรก
    # ตั้งเครื่องใหม่ได้ไม่เกิน MAX_NEW_SETUP_MC ตัวต่อสัปดาห์ (กติกาเดียวกับลูปวางแผน)
    # ถ้าไม่คิดตรงนี้ backward จะสรุปว่า "เริ่มสัปดาห์เดียวก่อน target ด้วย 6 เครื่องก็ทัน"
    # แล้วพอวางจริงเครื่องขึ้นได้ทีละ 2 → ไม่ทัน ต้องเร่ง/เว้นสัปดาห์ แล้ว setup ซ้ำ
    # (เคสจริง FD4DRTRN74: JIT สั่งเริ่ม W44 ด้วย required 6 แต่ได้จริง 2→4 → เลย target)
    for _start in range(rdd_idx, floor_idx - 1, -1):
        _total_cap = 0.0
        _run = 0                       # เครื่องที่เดินอยู่ของก้อนนี้ (สะสมทีละสัปดาห์)
        for _i in range(_start, rdd_idx + 1):
            _w = _wk_info[_i]
            if _w["wd"] <= 0:
                continue
            _carry = min(_run, _w["avail"])
            _new = max(0, min(
                _item_setup_room(item_code, mc_group, item_gauge, _w["week"]),
                _w["avail"] - _carry, int(n_machines) - _carry,
            ))
            if _carry + _new <= 0:
                continue
            _pd_new = max(0, _w["wd"] - setup_days)      # เครื่องที่เพิ่งตั้งเสียวัน setup
            _cap_w = _carry * _w["wd"] * daily_cap + _new * _pd_new * daily_cap
            _total_cap += fold_round(
                _cap_w, rev_weight, mc_group,
                per_mc_cap=_w["wd"] * daily_cap, mc_count=_carry + _new,
            )
            _run = _carry + _new
        if _total_cap >= _qty_need:
            return _start

    return None  # ไม่ทันแม้เริ่มที่ floor_idx → ต้อง ASAP


# ทรงเครื่องแบบ backward: ไล่ทรงจาก "สัปดาห์ target" ย้อนขึ้นมา แทนการอัดเต็มแล้วตัดหาง
# ปิดได้ที่หน้า "ตั้งค่าระบบ" (ใช้ทรงเดิม: เครื่องคงที่ + taper ตอนท้าย)
BACKWARD_RAMP_ENABLED = plan_config.get("KP_BACKWARD_RAMP")


def _backward_ramp_shape(week_info, order_qty, daily_cap, setup_days, rev_weight, mc_group,
                         carry_start=0, carry_valid_to=0):
    """ทรงจำนวนเครื่องแบบ backward — วางจากท้ายมาหน้าให้ "จบพอดี target" และไล่ลงครึ่ง

    user เคาะ 1 ก.ย. 2569: ทรงต้องเป็น "ขึ้นทีละ ≤2 → เดินเต็ม → ไล่ลงครึ่ง ๆ" และต้องจบ
    พอดี TARGET_KNIT ไม่ใช่ทอบาง ๆ ยาว ๆ แล้วมาเพิ่มเครื่อง 3-4 ตัวสัปดาห์สุดท้าย

    วิธี: ไล่ย้อนจากสัปดาห์ target — สัปดาห์สุดท้ายใช้ 1 เครื่อง สัปดาห์ก่อนหน้าใช้ได้ถึง
    2 เท่า (มองจากหน้าไปหลัง = ลดลงครึ่งหนึ่งทุกสัปดาห์) แล้วค่อย cap ด้วยเพดานขึ้นเครื่อง
    (ตั้งใหม่ ≤ MAX_NEW_SETUP_MC ตัว/สัปดาห์) และเครื่องว่างจริงของแต่ละสัปดาห์
    ถ้ากำลังรวมยังไม่พอ ก็ถอยจุดเริ่มไปอีกสัปดาห์แล้วคำนวณใหม่

    คืน {index สัปดาห์: จำนวนเครื่อง} ครอบคลุมทุกสัปดาห์ (0 = ไม่ผลิต) หรือ None ถ้าไม่ไหว
    """
    if not week_info or daily_cap <= 0 or order_qty <= 0:
        return None
    _n = len(week_info)
    _target = _n - 1
    _max_avail = max((int(w.get("avail", 0) or 0) for w in week_info), default=0)
    if _max_avail <= 0:
        return None

    for _start in range(_target, -1, -1):
        # 1) ทรงย้อน: สัปดาห์ target ใช้ได้ถึง MC_TAIL_MC เครื่อง → ย้อนขึ้นได้ไม่เกินเท่าตัว
        #    (= ขาลงลดครึ่งเมื่อมองจากหน้า) และต้องไม่เกิน "เครื่องที่ยอดคงเหลือต้องใช้จริง"
        #    — ไต่ขึ้นเรื่อย ๆ จนเต็มพูลจะกินเครื่องเกินจำเป็นแล้วไปเบียดงานอื่น
        #    (รอบทดสอบ: งานอื่นช้าลง 25 ก้อน)
        _shape = {}
        _mc_back = MC_TAIL_MC
        _acc_back = 0.0
        # ขาลง "ลดครึ่ง" ได้กี่ขั้น (user เคาะ 2 ก.ย. 2569: 50% สองครั้ง) — ครบขั้นแล้ว
        # ช่วงก่อนหน้าเดินระดับเดิม (plateau) แทนการไต่ขึ้นเป็นเท่าตัวไปเรื่อย ๆ
        # → ทรงจากหน้าไปหลัง: ไต่ ≤2/สัปดาห์ → เดินเต็ม → ครึ่ง → ครึ่ง → จบพอดี target
        _steps_back = 0
        for _i in range(_target, _start - 1, -1):
            _w = week_info[_i]
            _wd_b = float(_w.get("wd", 0) or 0)
            if _wd_b <= 0 or int(_w.get("avail", 0) or 0) <= 0:
                _shape[_i] = 0
                continue
            if _acc_back + 1e-6 >= order_qty:
                _shape[_i] = 0          # ยอดครบแล้ว สัปดาห์ก่อนหน้านี้ไม่ต้องเดินเครื่อง
                continue
            _need_b = max(1, int(-(-(order_qty - _acc_back) // (_wd_b * daily_cap))))
            # ทรง "อุดมคติ" ตอนไล่ย้อน ห้าม cap ด้วยเครื่องว่างรายสัปดาห์ (จะได้ทรงฟันปลา)
            # — ความเป็นจริงรายสัปดาห์ไปคุมที่ขั้นตอนไต่ขึ้นข้างล่างแทน
            _use_b = max(1, min(_mc_back, _max_avail, _need_b))
            _shape[_i] = _use_b
            _acc_back += _use_b * _wd_b * daily_cap
            if _steps_back < MC_TAPER_MAX_STEPS:
                _next_back = min(_use_b * 2, _max_avail)
                if _next_back > _use_b:
                    _steps_back += 1
                _mc_back = _next_back
            else:
                _mc_back = _use_b       # ลดครบจำนวนขั้นแล้ว → ช่วงก่อนหน้าเดินระดับเดียวกัน

        # 2) cap ด้วยทรงขาขึ้นจริง (ตั้งใหม่ได้ทีละ MAX_NEW_SETUP_MC) แล้ววัดกำลังรวม
        # ⚠️ สัปดาห์ต้นที่ทรงย้อนบอกว่า "ไม่ต้องใช้" (เพราะยอดครบก่อนไล่มาถึง) ต้องใช้เป็น
        # ช่วง "ไต่เครื่อง + ทอเลี้ยง" ไม่ใช่ข้ามไป — ถ้าข้าม พอถึงสัปดาห์ที่ต้องใช้จริงจะ
        # ตั้งเครื่องได้แค่ 2 ตัว (เริ่มจาก 0) แล้วกำลังไม่พอ → ฟังก์ชันตอบ "ทำไม่ได้" ทุกก้อน
        # (บั๊กนี้ทำให้รอบจัดทรงไม่ทำงานเลยสักก้อนในรอบทดสอบ)
        _peak = max(_shape.values()) if _shape else 0
        # ⚠️ เครื่องที่วิ่งอยู่ตอนนี้ (carry_start) ใช้เป็นฐานขาขึ้นได้เฉพาะเมื่อทรงเริ่มเร็วพอ
        # ที่เครื่องยังอุ่นอยู่ (ภายใน SETUP_GAP_WEEK) — ถ้าทรงถูกเลื่อนไปไกลกว่านั้น เครื่อง
        # ถูกงานอื่นยึดไปแล้ว ต้องเริ่มจาก 0 ไม่งั้นทรงสั่ง 3 เครื่องตั้งแต่สัปดาห์แรกทั้งที่
        # ตั้งใหม่ได้แค่ 2 (เคสจริง FD4DRTRN74/03A0: ทรง 3-4-2-1 แต่วางจริงได้ 1-1-1)
        _run = max(0, int(carry_start)) if _start <= int(carry_valid_to) else 0
        _total = 0.0
        for _i in range(_start, _target + 1):
            _w = week_info[_i]
            _want = int(_shape.get(_i, 0))
            # สัปดาห์เว้นรอสี (Color Gap): ไม่วางงาน แต่ _run คงเดิม — เครื่องถูกกันไว้ให้ก้อนนี้
            # (แถว HOLD ในแผน) จึงกลับมาเดินต่อสัปดาห์ถัดไปได้โดยไม่นับเป็นเครื่องตั้งใหม่
            if _w.get("blocked"):
                _shape[_i] = 0
                continue
            if float(_w.get("wd", 0) or 0) <= 0:
                _shape[_i] = 0
                continue
            # ⚠️ "เครื่องว่าง" จำกัดได้แค่ "เครื่องที่จะเพิ่มใหม่" — เครื่องที่ก้อนนี้เดินอยู่แล้ว
            # (_run) เป็นของตัวเองอยู่แล้ว ไม่ต้องขอจากพูลซ้ำทุกสัปดาห์
            # ถ้า cap ด้วย avail ทั้งก้อน ทรงจะกระโดดขึ้น-ลงตามเครื่องว่างรายสัปดาห์
            # (เคสจริง FD4DRTRN74/03A0 ได้ทรง 2-1-3-1-3-2 = ขึ้นลงสลับ เสีย setup ทุกครั้ง)
            _cap_i = _run + max(0, min(MAX_NEW_SETUP_MC, int(_w["avail"])))
            if _want <= 0:
                # ช่วงไต่ขึ้นก่อนถึงจุดที่ทรงย้อนต้องการ — เดินเท่าที่ไต่ได้ ไม่เกิน peak
                _want = min(_cap_i, _peak)
                if _want <= 0:
                    _shape[_i] = 0
                    continue
            _use = min(_want, _cap_i)
            if _use <= 0:
                _shape[_i] = 0
                continue
            _new = max(0, _use - _run)
            _carry = _use - _new
            _pd_new = max(0, float(_w["wd"]) - setup_days)
            _total += fold_round(
                _carry * float(_w["wd"]) * daily_cap + _new * _pd_new * daily_cap,
                rev_weight, mc_group,
                per_mc_cap=float(_w["wd"]) * daily_cap, mc_count=_use,
            )
            _shape[_i] = _use
            _run = _use

        if _total + 1e-6 >= order_qty:
            # 3) ทรงต้อง "ขึ้นแล้วนิ่งแล้วลง" ห้ามขึ้น ๆ ลง ๆ (user ย้ำ 2 ก.ย. 2569:
            #    2-3-3-2 หรือ 1-2-2 ก็ได้ ขอแค่เพิ่มไม่เกิน 2 ต่อสัปดาห์และห้ามเด้งกลับขึ้น)
            #    ถ้าสัปดาห์ไหนถูกกดลงเพราะเครื่องไม่พอ สัปดาห์หลังจากนั้นห้ามเพิ่มกลับ
            #    (เพิ่มกลับ = จ่ายค่า setup ซ้ำ ซึ่งคือทรงฟันปลาที่ user ไม่เอา)
            _out = {_i: int(_shape.get(_i, 0)) for _i in range(_n)}
            _declined = False
            _prev_m = 0
            for _i in range(_n):
                _m = _out[_i]
                # สัปดาห์เว้นรอสีไม่ใช่ "ขาลง" — หลังกลับมาทอต่อ เพิ่มเครื่องชดเชยได้
                # (user เคาะ 8 ก.ย. 2569: เว้นก่อน แล้วเร่งเครื่องให้ทัน RDD)
                if week_info[_i].get("blocked"):
                    _declined = False
                    continue
                if _m <= 0:
                    continue
                if _declined and _m > _prev_m:
                    _m = _prev_m
                    _out[_i] = _m
                elif _m < _prev_m:
                    _declined = True
                _prev_m = _m
            return _out

    return None


def calculate_progressive_reduction(

    item_code, order_qty, start_week, fg_week, mc_group, daily_cap, item_gauge,

    setup_days=SETUP_DAYS, rev_weight=None

):

    """คำนวณจำนวนเครื่องแต่ละ week แบบประหยัดที่สุด แต่ให้เสร็จพอดี target week

    Strategy:

    1. หาจำนวนเครื่องน้อยที่สุดที่ทัน target (fixed machines)

    2. ถ้าเสร็จเร็วกว่า target → กระจายเครื่องให้น้อยลงในแต่ละ week

    3. เริ่มต้นด้วยเครื่องมากกว่า แล้วค่อยๆ ลดลงให้เสร็จพอดี target week

    Returns: list of (week, machines) หรือ None ถ้าไม่สามารถทันได้

    """

    weeks_until_target = []

    current_week = start_week

    target_week_index = fg_week  # TARGET_KNIT



    # สร้าง list ของ weeks จาก start_week ถึง TARGET_KNIT โดยเด็ดขาด
    # target_week_index เป็น None ได้ (RDD นอกช่วง calendar) → ข้าม loop ให้คืน None ตาม logic เดิม

    _wut_guard = 0

    while current_week is not None and target_week_index is not None and week_index(current_week) <= target_week_index:

        _wut_guard += 1  # SAFETY: กัน infinite loop ตอนข้ามปลายปี (week_index จับผิดปี → ไม่เดินหน้า)

        if _wut_guard > 300:

            break

        weeks_until_target.append(current_week)

        current_week = next_week(current_week)

    if not weeks_until_target:

        return None



    # เก็บ availability และ working days ของแต่ละ week

    week_info = []

    # สัปดาห์ที่ต้องเว้นรอสีของคีย์นี้ (Color Gap) — ทรงต้องรู้ตั้งแต่ตอนคำนวณ ไม่งั้นจะแจก
    # เครื่องลงสัปดาห์ที่ลูปหลักจะข้ามอยู่ดี แล้วยอดไหลไปท้ายจนไม่ทัน target
    _cg_block_idx = (
        _color_gap_idx(item_code, mc_group, item_gauge)
        if COLOR_GAP_ENABLED and not _CUR_ORDER_COLOR_PART else None
    )

    for week in weeks_until_target:

        # ก้อนสี: นับ "เครื่องที่ยึดคืนได้จากงานที่ยังไม่ให้สี" ด้วย ไม่ใช่พูลว่างอย่างเดียว
        # (ไม่งั้นทรง backward คำนวณไม่ได้ทุกก้อนที่พูลตึง แล้วตกไปใช้ทรงแบน 1 เครื่องยาว ๆ)
        actual_remain = _avail_mc_for_order(mc_group, week, item_gauge, item_code)

        # วันทำงานจากชีท Work Day เป็นค่าสุดท้ายแล้ว (ไม่หักวันหยุด/ไม่ cap ด้วยปฏิทิน)
        actual_wd = get_working_days_by_factory(mc_group, 1, week=week, item_code=item_code, gauge=item_gauge)

        _cg_blocked = _cg_block_idx is not None and week_index(week) == _cg_block_idx

        week_info.append({

            'week': week,

            'avail': 0 if _cg_blocked else actual_remain,

            'wd': actual_wd,

            # สัปดาห์เว้นรอสี: ห้ามวางงาน แต่เครื่องยังเป็นของก้อนนี้อยู่ (ไม่ต้องตั้งใหม่)
            'blocked': _cg_blocked

        })

    # ── ทรงแบบ backward: วางจากท้ายมาหน้าให้จบพอดี target แล้วไล่ลงครึ่ง ─────────────
    # ใช้ก่อนทรงเดิม (เครื่องคงที่ + taper ท้าย) เพราะทรงเดิมทอบาง ๆ ยาว ๆ แล้วต้องเพิ่ม
    # เครื่องตอนท้าย = เสียวัน setup ฟรีและไปแย่งเครื่องคนอื่นในสัปดาห์ที่งานแน่นที่สุด
    if BACKWARD_RAMP_ENABLED:
        # เครื่องที่ item นี้เดินอยู่แล้ว (ยังอุ่น) = จุดตั้งต้นของขาขึ้น — ถ้าไม่ส่งเข้าไป
        # ทรงจะคิดว่าต้องเริ่มจาก 0 ทุกครั้ง แล้วไต่ไม่ทัน (ตอบ "ทำไม่ได้" ทั้งที่ทำได้)
        _carry_start_bw = 0
        _carry_valid_bw = -1          # ทรงเริ่มช้ากว่านี้ = เครื่องเดิมไม่อุ่นแล้ว
        _key_bw = _resolve_carry_key(item_code, mc_group, item_gauge)
        _last_bw = last_production.get(_key_bw)
        _start_idx_bw = week_index(start_week)
        if (
            _last_bw is not None
            and _start_idx_bw is not None
            and 0 <= _start_idx_bw - _last_bw <= SETUP_GAP_WEEK
        ):
            _carry_start_bw = max(0, int(machines_in_use.get(_key_bw, 0) or 0))
            # เครื่องอุ่นข้ามได้ไม่เกิน SETUP_GAP_WEEK นับจากสัปดาห์ที่ผลิตล่าสุด
            _carry_valid_bw = (_last_bw + SETUP_GAP_WEEK) - _start_idx_bw
        _bw_shape = _backward_ramp_shape(
            week_info, order_qty, daily_cap, setup_days, rev_weight, mc_group,
            carry_start=_carry_start_bw, carry_valid_to=_carry_valid_bw,
        )
        if _bw_shape:
            _bw_result = [(w["week"], int(_bw_shape.get(_i, 0))) for _i, w in enumerate(week_info)]
            _bw_start_w = next((_w for _w, _m in _bw_result if _m > 0), None)
            if _bw_start_w is not None:
                print(
                    f"[BACKWARD RAMP] {item_code}: ทรงเครื่อง "
                    + " ".join(str(_m) for _, _m in _bw_result if _m > 0)
                    + f" (เริ่ม W{_bw_start_w} → จบ W{week_info[-1]['week']})"
                )
                return _bw_result

    # Step 1: หาจำนวนเครื่องน้อยที่สุดที่ทัน (fixed machines ทุก week)

    min_machines = None

    for try_mc in range(1, max(w['avail'] for w in week_info) + 1):

        qty_left = order_qty

        for i, w in enumerate(week_info):

            if qty_left <= 0:

                break



            prod_days = max(0, w['wd'] - setup_days) if i == 0 else w['wd']

            if prod_days <= 0:

                continue



            use_mc = min(try_mc, w['avail'])

            prod = fold_round(
                use_mc * prod_days * daily_cap, rev_weight, mc_group,
                per_mc_cap=prod_days * daily_cap, mc_count=use_mc,
            )

            qty_left -= prod

        if qty_left <= 0:

            min_machines = try_mc

            break



    if min_machines is None:

        return None  # ไม่ทันแม้ใช้เครื่องเต็มที่



    # Step 1b: หา optimal_start_idx — เลื่อน start week ให้ช้าที่สุดที่ยังจบทัน TARGET_KNIT

    # ผลลัพธ์: carryover-first จะผลิตตั้งแต่ optimal_start_idx ถึง TARGET_KNIT พอดี

    target_idx = len(week_info) - 1

    optimal_start_idx = 0  # default: เริ่มจาก week แรก

    for _start in range(target_idx, -1, -1):

        _total_cap = 0

        # ⚠️ เครื่องขึ้นได้ทีละ MAX_NEW_SETUP_MC ตัวต่อสัปดาห์ — ห้ามสมมติว่ามี min_machines
        # ครบตั้งแต่สัปดาห์แรก ไม่งั้นจุดเริ่มที่คำนวณได้จะสายเกินจริง แล้วงานไม่ทัน target
        # (กติกาเดียวกับ compute_backward_start_idx และลูปวางแผน)
        _run_i = 0

        for _i in range(_start, target_idx + 1):

            _carry_i = min(_run_i, week_info[_i]['avail'])
            _new_i = max(0, min(MAX_NEW_SETUP_MC, week_info[_i]['avail'] - _carry_i,
                                min_machines - _carry_i))
            _use_i = _carry_i + _new_i
            if _use_i <= 0:
                continue

            _wd_i = week_info[_i]['wd']
            _pd_new_i = max(0, _wd_i - setup_days)      # เครื่องที่เพิ่งตั้งเสียวัน setup

            _p = fold_round(
                _carry_i * _wd_i * daily_cap + _new_i * _pd_new_i * daily_cap,
                rev_weight, mc_group,
                per_mc_cap=_wd_i * daily_cap, mc_count=_use_i,
            )

            _total_cap += _p
            _run_i = _use_i

        if _total_cap >= order_qty:

            # หางลดครึ่ง (ดูด้านล่าง) กินเวลาเพิ่ม 1 สัปดาห์ → ถอย start มา 1 สัปดาห์เพื่อชดเชย
            # ไม่งั้นงานที่เคยจบพอดี TARGET_KNIT จะกลายเป็นสายเพราะหางที่ยืดออกมา
            optimal_start_idx = (
                max(0, _start - 1) if min_machines >= MC_TAPER_MIN_MC else _start
            )

            break  # พบ start ช้าที่สุดที่ยังทัน TARGET_KNIT

    if optimal_start_idx > 0:

        print(f"[TARGET_KNIT START] {item_code}: เลื่อนเริ่มจาก W{week_info[0]['week']} → W{week_info[optimal_start_idx]['week']} (TARGET W{week_info[target_idx]['week']})")



    # Step 2: Carryover-first strategy - ใช้เครื่องเต็มจำนวนจนกว่า qty จะเหลือน้อย

    # ลดเครื่องเฉพาะเมื่อ qty ที่เหลือน้อยกว่า capacity ของ 1 สัปดาห์

    # เริ่มผลิตจาก optimal_start_idx (ไม่ใช่ week 0) เพื่อให้จบที่ TARGET_KNIT

    result = []

    qty_left = order_qty

    num_weeks = len(week_info)

    prev_week_mc = min_machines  # เริ่มต้นด้วย min_machines

    _taper_steps = 0  # ไล่ลงไปแล้วกี่ขั้น (เพดาน MC_TAPER_MAX_STEPS)

    for i, w in enumerate(week_info):

        if i < optimal_start_idx:

            # ยังไม่ถึง optimal start → ไม่ผลิต

            result.append((w['week'], 0))

            continue



        if qty_left <= 0:

            # เสร็จแล้ว แต่ยังมี week เหลือ → ไม่ผลิต

            result.append((w['week'], 0))

            continue



        # Setup days ใช้เฉพาะ week แรกที่ผลิตจริง (optimal_start_idx)

        prod_days = max(0, w['wd'] - setup_days) if i == optimal_start_idx else w['wd']

        if prod_days <= 0 or w['avail'] <= 0:

            result.append((w['week'], 0))

            continue



        # Strategy: ใช้เครื่องเต็มจำนวน (min_machines) จนกว่า qty จะเหลือน้อย

        # ลดเครื่องเฉพาะเมื่อ qty ที่เหลือผลิตไม่เต็ม capacity ของเครื่องทั้งหมด

        

        # คำนวณ capacity ต่อสัปดาห์ของเครื่องทั้งหมด

        full_week_capacity = fold_round(
            min_machines * prod_days * daily_cap, rev_weight, mc_group,
            per_mc_cap=prod_days * daily_cap, mc_count=min_machines,
        )

        

        if qty_left >= full_week_capacity:

            # qty เหลือมาก → ใช้เครื่องเต็มจำนวน

            needed_mc = max(1, int(qty_left / (prod_days * daily_cap)) + 1)

            use_mc = min(min_machines, needed_mc, w['avail'])

            if use_mc < min_machines:

                print(f"[PROGRESSIVE REDUCTION FIX] Week {w['week']}: min_machines={min_machines} > needed_mc={needed_mc} → use {use_mc}")

        else:

            # qty เหลือน้อย → คำนวณเครื่องที่ต้องการจริงๆ

            needed_mc = max(1, int(qty_left / (prod_days * daily_cap)) + 1)

            use_mc = min(needed_mc, w['avail'], min_machines)

            # ===== ขาลง =====
            # งานที่ใช้เครื่อง >= MC_TAPER_MIN_MC → ลดลงได้แค่ "ครึ่งหนึ่ง" ของสัปดาห์ก่อน
            #   (10→5, 12→6, 6→3) แทนการตัดฮวบตามยอดที่เหลือ
            # งานเล็กกว่านั้น → กฎเดิม "ลดได้ไม่เกิน 2 เครื่อง/สัปดาห์"

            if i > optimal_start_idx and use_mc < prev_week_mc:

                if prev_week_mc >= MC_TAPER_MIN_MC and _taper_steps < MC_TAPER_MAX_STEPS:

                    _floor_half = max(1, int(math.ceil(prev_week_mc * 0.5)))

                elif prev_week_mc >= MC_TAPER_MIN_MC:

                    _floor_half = 1   # ไล่ลงครบ MC_TAPER_MAX_STEPS ขั้นแล้ว → จบงานได้เลย

                else:

                    _floor_half = max(1, prev_week_mc - 2)

                if use_mc < _floor_half:

                    use_mc = _floor_half

                    _taper_steps += 1

        # ===== ห้ามดับจากเครื่องเยอะ ๆ: สัปดาห์ปิดงานต้องผ่านขั้น "ครึ่ง" ก่อน =====
        # ถ้าสัปดาห์นี้จะทอจบทั้งที่ยังใช้เครื่องมากกว่าครึ่งของสัปดาห์ก่อน → ลดเหลือครึ่ง
        # แล้วยกเศษไปจบสัปดาห์ถัดไป (ทรง 10→5→3 แทน 10→0)
        if (
            i > optimal_start_idx
            and prev_week_mc >= MC_TAPER_MIN_MC
            and _taper_steps < MC_TAPER_MAX_STEPS
            and use_mc * prod_days * daily_cap >= qty_left
        ):
            _end_half = max(1, int(math.ceil(prev_week_mc * 0.5)))
            if use_mc > _end_half and i < num_weeks - 1:   # ต้องมีสัปดาห์เหลือให้ยกเศษไป
                print(
                    f"[MC TAPER] {item_code} W{w['week']}: {use_mc} → {_end_half} เครื่อง "
                    f"(ไล่ลงขั้นที่ {_taper_steps + 1}/{MC_TAPER_MAX_STEPS} — "
                    f"ครึ่งหนึ่งของ {prev_week_mc} แทนการดับทั้งชุด)"
                )
                use_mc = _end_half
                _taper_steps += 1

        # คำนวณ production จริง

        # เครื่องใหม่ที่เพิ่มกลางแผน (ไม่ใช่สัปดาห์แรก) ต้องหัก setup_days ของตัวเอง

        if i > optimal_start_idx and use_mc > prev_week_mc:

            carry_in_wk = prev_week_mc

            added_in_wk = use_mc - carry_in_wk

            prod = fold_round_mixed(
                (carry_in_wk * prod_days + added_in_wk * max(0, prod_days - setup_days)) * daily_cap,
                rev_weight, mc_group,
                prod_days * daily_cap, carry_in_wk,
                max(0, prod_days - setup_days) * daily_cap, added_in_wk,
            )

        else:

            prod = fold_round(
                use_mc * prod_days * daily_cap, rev_weight, mc_group,
                per_mc_cap=prod_days * daily_cap, mc_count=use_mc,
            )

        result.append((w['week'], use_mc))

        qty_left -= prod

        prev_week_mc = use_mc  # บันทึกเครื่องของ week นี้

    # ถ้ายังเหลือ qty หลังจาก loop ครบ → ไม่ทัน (แต่ไม่น่าเกิดเพราะ min_machines ทันแล้ว)

    if qty_left > 0:

        return None



    return result





def calculate_required_machines(

    item_code, order_qty, start_week, fg_week, setup_days=SETUP_DAYS, only_mc_group=None,

    order_type="", sub_color="", dye_end_date=None,

):

    print(f"[DEBUG CALC] calculate_required_machines called: {item_code}, qty={order_qty}, start={start_week}, target={fg_week}")

    """คำนวณจำนวนเครื่องขั้นต่ำที่ต้องการเพื่อทัน RDD

    หลักการ: ใช้เครื่องน้อยแต่ผลิตหลาย week ดีกว่าใช้เครื่องเยอะแค่ 1 week

    - setup เป็น per-machine: 3mc setup = เสีย 3×3=9 mc-days

    - week 2+ ไม่ต้อง setup → ได้ผลิตเต็มที่

    - simulate per-week ด้วยเครื่องว่างจริงของแต่ละ week (cap at n_mc)

    ตัวอย่าง order 3277.5, cap=163, factory 7d, เครื่องว่าง [6, 1, 5]:

      6mc×3wk: wk1=6×4×163=3912, wk2=1×7×163=1141, wk3=5×7×163=5705 → setup_waste=18

      2mc×3wk: wk1=2×4×163=1304, wk2=1×7×163=1141, wk3=2×7×163=2282 → setup_waste=6

    """

    # fg_week (RDD index) เป็น None ได้เมื่อ RDD อยู่นอกช่วง calendar
    # → ใช้ start_week + 3 เป็น target แทน (เร่งจบ เหมือนกรณี past RDD)
    if fg_week is None:
        _sw_idx_norm = week_index(start_week)
        if _sw_idx_norm is None:
            return None, None, None, None, None
        fg_week = _sw_idx_norm + 3
        print(f"[DEBUG CALC] {item_code}: fg_week=None (RDD นอกช่วง calendar) → ใช้ start+3 = {fg_week}")

    # หา MC_GROUP ที่สามารถผลิต item นี้ได้ (ยุบเหลือกลุ่มเดียวต่อก้อน — ใช้ตัวที่ CAP สูงสุด)

    available_machines = _dedup_mc_by_group(
        item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    )

    if available_machines.empty:

        return None, None, None, None, None



    # ใช้ความจุตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน

    # และ FA 20 มี priority สูงกว่า SKP 20

    # ใช้ความจุของ MC_GROUP แรกที่พบเป็นค่าเริ่มต้น และจะปรับเมื่อวนลูปแต่ละ MC_GROUP

    daily_cap = None

    # เรียงตาม MC_GROUP ที่มีเครื่องเหลือมากที่สุดก่อน (start_week)

    available_machines = available_machines.copy()

    available_machines["_mc_remain"] = available_machines.apply(

        lambda r: get_actual_mc_remain(r["MC_GROUP"], start_week, gauge=r.get("GUAGE"), item_code=item_code),

        axis=1,

    )

    available_machines = available_machines.sort_values("_mc_remain", ascending=False)

    # ถ้ามี only_mc_group → บังคับใช้ MC_GROUP นั้น (lock สำหรับ SC/SO+Item เดิม)

    if only_mc_group is not None:

        _filt = available_machines[available_machines["MC_GROUP"] == only_mc_group]

        if not _filt.empty:

            available_machines = _filt

    # คำนวณจำนวนสัปดาห์ที่เหลือถึง TARGET_KNIT (บังคับให้จบตรง TARGET_KNIT)

    # fg_week คือ TARGET_KNIT index (row index ใน calendar_week) เพื่อรองรับข้ามปีได้

    weeks_until_target = []

    current_week = start_week

    target_week_index = fg_week  # TARGET_KNIT

    start_week_idx = week_index(start_week)

    

    # สร้าง list ของ weeks จาก start_week ถึง TARGET_KNIT โดยเด็ดขาด
    # target_week_index เป็น None ได้ (RDD นอกช่วง calendar) → ข้าม loop ให้เข้า branch past RDD ด้านล่าง

    # เดินด้วย row index ตรง ๆ (idx เพิ่มขึ้นเสมอ → จบแน่นอน)
    # กัน infinite loop ที่เกิดจาก week_index(bare week) จับผิดปีตอนข้ามปลายปี
    _iter_idx = start_week_idx
    while (_iter_idx is not None and target_week_index is not None
           and _iter_idx <= target_week_index and _iter_idx < len(calendar_week)):
        weeks_until_target.append(int(calendar_week.iloc[_iter_idx]["WEEK"]))
        _iter_idx += 1



    # ถ้า target อยู่ในอดีต (past RDD) → ใช้ weeks จาก start_week ถึง plan_week+3 เพื่อให้รีบเสร็จ

    # เพื่อให้คำนวณ required machines สูงขึ้นและเพิ่มเครื่องค่อยๆ (Gradual Increase)

    if not weeks_until_target:

        print(f"[DEBUG PAST RDD] Target {target_week_index} < Start {start_week_idx} → using weeks to plan_week+3 for faster completion")

        target_for_past_rdd = start_week_idx + 3  # จำกัด 3 weeks เพื่อให้รีบเสร็จ → คำนวณเครื่องสูงขึ้น

        # เดินด้วย row index ตรง ๆ (กัน infinite loop จาก week_index จับผิดปี)
        _iter_idx = start_week_idx
        while (_iter_idx is not None and _iter_idx <= target_for_past_rdd
               and _iter_idx < len(calendar_week)):
            weeks_until_target.append(int(calendar_week.iloc[_iter_idx]["WEEK"]))
            _iter_idx += 1

    

    # ตรวจสอบว่า week สุดท้ายคือ TARGET_KNIT จริงๆ

    last_week_index = week_index(weeks_until_target[-1])

    if last_week_index != target_week_index:

        print(f"⚠️  TARGET_KNIT mismatch: last week {last_week_index} != target {target_week_index}")

    

    num_weeks = len(weeks_until_target)

    # ลองแต่ละ MC_GROUP ที่สามารถผลิตได้ (เรียงตาม cap น้อยไปมาก — ใช้ cap ต่ำสุดในการคำนวณ)

    for _, machine_row in available_machines.iterrows():

        mc_group = machine_row["MC_GROUP"]

        # ใช้ความจุตาม logic ใหม่สำหรับ MC_GROUP นี้

        item_gauge = machine_row["GUAGE"] if "GUAGE" in machine_row else None

        daily_cap = _get_capacity_for_mc_group(item_code, mc_group, item_gauge)

        daily_cap = adjust_daily_cap_for_item_special(daily_cap, item_code, mc_group, item_gauge)

        if daily_cap <= 0:

            continue  # ข้ามถ้าไม่มีความจุ

        # เก็บจำนวนเครื่องว่างจริงของแต่ละ week

        avail_per_week = []

        has_any_machine = False

        for week in weeks_until_target:

            # ก้อนสี: พูลว่าง + เครื่องที่ยึดคืนได้ (ดู _avail_mc_for_order) — ต้องคิดเหมือนกัน
            # ทั้ง required_machines / progressive / backward ไม่งั้นสามจุดตอบคนละอย่าง
            actual_remain = _avail_mc_for_order(mc_group, week, item_gauge, item_code)

            avail_per_week.append(actual_remain)

            if actual_remain > 0:

                has_any_machine = True

        # เครื่องที่วิ่งอยู่แล้ว (carry-over) ถือว่า "มี" เครื่องพร้อมผลิตโดยไม่ต้องดู actual_remain

        _key_check = _resolve_carry_key(item_code, mc_group, item_gauge)

        if not has_any_machine and machines_in_use.get(_key_check, 0) <= 0:

            continue



        # ---- Setup-aware: ตรวจสอบว่าต้อง setup หรือไม่ ----

        key = _resolve_carry_key(item_code, mc_group, item_gauge)

        setup_needed = True

        start_week_idx = week_index(start_week)

        _bridge_locks(key, start_week_idx)
        # แถวกันเครื่อง (Hold รอสี / Hold รอ target) = เครื่องจอดอุ่นรอเจ้าของ → นับเป็นสัปดาห์
        # ที่เครื่องยังอยู่ในมือ เหมือน Lock_MC (ดู _bridge_holds)
        _bridge_holds(key, start_week_idx)

        if key in last_production:

            last_week_idx = last_production[key]

            if start_week_idx - last_week_idx <= SETUP_GAP_WEEK:

                setup_needed = False

        # เครื่องที่วิ่งอยู่แล้ว (carry-over จาก booking/old plan)

        # ถ้า setup_needed=False = เครื่องยังอุ่นอยู่ → ใช้เป็น committed_mc ตั้งต้น

        carryover_start = machines_in_use.get(key, 0) if not setup_needed else 0

        # YD-ORDERS: ถ้า วันนัดย้อม เปลี่ยน → carryover ต้อง setup เพิ่ม 1 วัน (เฉพาะ week แรก)

        _yd_color_setup_days = 0

        if order_type == "YD-ORDERS" and carryover_start > 0:

            _dye_end_date = dye_end_date

            if pd.notna(_dye_end_date):

                _prev_dye_date = last_dye_end_date.get(key, None)

                if _prev_dye_date and _prev_dye_date != _dye_end_date:

                    _yd_color_setup_days = 1

        factory_wd = get_working_days_by_factory(mc_group, 1, week=start_week, item_code=item_code, gauge=item_gauge)

        # หาจำนวนเครื่องสูงสุดที่สามารถลองได้ (จาก week ที่มีเครื่องมากที่สุด)

        max_possible = max(avail_per_week)

        # จำกัดตาม job/week capacity (รวม type ทั้งหมด ไม่ใช่แค่ MC_GROUP เดียว)

        type_used_start = get_type_used_jobs(start_week, mc_group)

        max_try = check_job_capacity_limit(

            mc_group,

            int(max_possible),

            urgent_mode=False,

            current_week_jobs=type_used_start,

        )

        # carry-over machines ไม่ต้อง setup ไม่ต้องนับเป็น new job

        # ดังนั้น max_try ต้องอย่างน้อย = carryover_start

        if carryover_start > 0 and max_try < carryover_start:

            max_try = carryover_start



        # ไม่ใช้ tolerance แบบ 1 batch

        # ต้องผลิตให้ครบจริง ถ้าไม่พอต้องเพิ่มเครื่อง

        _rw_tol = 0.0



        # ---- เปรียบเทียบทุก option ด้วย per-week simulation ----

        best_option = None  # (n_machines, weeks_needed, setup_waste, efficiency)

        

        # คำนวณเครื่องที่เหมาะสมเพื่อจบตรง TARGET_KNIT

        weeks_available = target_week_index - week_index(weeks_until_target[0]) + 1

        # ถ้า weeks_available เป็นค่าลบ (past RDD) → ใช้จำนวน weeks จริงที่มี

        if weeks_available <= 0:

            weeks_available = len(weeks_until_target)

        if weeks_available > 0:

            # คำนวณเครื่องที่ต้องการเพื่อกระจายผลิตจนถึง TARGET_KNIT

            total_production_needed = order_qty

            total_days_available = weeks_available * factory_wd

            # factory_wd = 0 ได้จริง เมื่อสัปดาห์เริ่มเป็นสัปดาห์ที่ปฏิทินหยุดทั้งก้อน (เช่น W31)
            # → ตัวหารเป็น 0 แล้ว int(inf) จะ OverflowError ล้มทั้งการรัน
            # ค่านี้เป็นแค่ตัวประมาณเริ่มต้น/ตัวสำรองตอนไม่มี option ไหนทัน — ของจริงมาจาก
            # per-week simulation ด้านล่างที่ใช้วันทำงานรายสัปดาห์อยู่แล้ว จึงตั้ง 1 แล้วเดินต่อได้
            _denom = total_days_available * daily_cap

            if _denom > 0:

                optimal_mc = max(1, int(total_production_needed / _denom * 1.1))  # +10% buffer

                print(f"[DEBUG TARGET] Optimal machines for {item_code}: {optimal_mc} (target {target_week_index}, weeks {weeks_available})")

            else:

                optimal_mc = 1

                print(f"[DEBUG TARGET] {item_code}: วันทำงานสัปดาห์เริ่ม W{start_week} = {factory_wd} "
                      f"(โรงหยุดทั้งสัปดาห์) → ข้ามการประมาณ ใช้ผลจาก simulation รายสัปดาห์แทน")

        

        for n_mc in range(1, int(max_try) + 1):

            # Simulate: ต้องการ n_mc เครื่อง แต่ละ week อาจได้ไม่ครบตาม availability

            # เครื่องที่เพิ่มใหม่ต้อง setup, เครื่องที่ carry-over ไม่ต้อง setup

            qty_remaining = order_qty

            weeks_needed = 0

            # เริ่มต้น simulation ด้วยเครื่องที่วิ่งอยู่แล้ว (ถ้า setup_needed=False)

            committed_mc = min(carryover_start, n_mc)  # ไม่เกิน target n_mc

            total_setup_mc_days = 0

            actual_use_list = []

            actual_wd_list = []  # เก็บ actual working days ของแต่ละ week

            for w_idx, week in enumerate(weeks_until_target):

                if qty_remaining <= _rw_tol:

                    break



                # วันทำงานจากชีท Work Day เป็นค่าสุดท้าย (จัดการยุบสัปดาห์ + สัปดาห์ปิดทั้งก้อน=0 แล้ว)

                actual_wd = factory_wd

                # ถ้า summary_mc ไม่มีข้อมูลในสัปดาห์นี้ แต่เครื่องกำลังวิ่งอยู่ (carry-over)

                # ให้เครื่องเดิมยังคงผลิตต่อได้ (ไม่ต้องมีข้อมูลใน summary_mc)

                avail_this_week = avail_per_week[w_idx]

                if avail_this_week <= 0 and committed_mc > 0:

                    avail_this_week = committed_mc  # carry-over เท่านั้น ไม่เพิ่มเครื่องใหม่

                # จำนวนเครื่องที่ต้องการใน week นี้ (ไม่เกิน availability)

                want_mc = min(n_mc, avail_this_week)

                if want_mc <= 0:

                    actual_use_list.append(0)

                    actual_wd_list.append(actual_wd)

                    continue



                # แยก carry-over vs ใหม่

                carryover = min(committed_mc, want_mc)

                new_added = (

                    want_mc - carryover

                )  # ไม่มี MAX_SETUP_MC → job/week cap ควบคุมแทน

                want_mc = carryover + new_added

                if committed_mc == 0 and setup_needed:

                    # week แรกที่เริ่มผลิต (cold start): ทุกเครื่องต้อง setup

                    setup_mc = want_mc

                    want_mc = setup_mc

                    prod_days_carry = 0

                    prod_days_new = max(0, actual_wd - setup_days)

                elif new_added > 0 and (setup_needed or committed_mc > 0):

                    # มีเครื่องเพิ่มใหม่นอกเหนือจาก carryover → เฉพาะเครื่องใหม่ต้อง setup

                    setup_mc = new_added if (setup_needed or committed_mc > 0) else 0

                    prod_days_carry = actual_wd

                    prod_days_new = (

                        max(0, actual_wd - setup_days) if setup_mc > 0 else actual_wd

                    )

                else:

                    # carry-over ล้วน หรือ warm start (setup_needed=False, committed_mc=0)

                    setup_mc = 0

                    prod_days_carry = actual_wd

                    prod_days_new = actual_wd  # warm → ผลิตเต็มสัปดาห์ที่เปิดจริง

                # YD-ORDERS: SUB_COLOR เปลี่ยน → carryover หัก 1 วัน (เฉพาะ week แรก)

                if _yd_color_setup_days > 0 and w_idx == 0 and carryover > 0:

                    prod_days_carry = max(0, prod_days_carry - _yd_color_setup_days)

                total_setup_mc_days += setup_mc * setup_days

                committed_mc = want_mc  # อัปเดตเครื่องที่ใช้จริง

                weeks_needed += 1

                actual_use_list.append(want_mc)

                actual_wd_list.append(actual_wd)

                week_production = (

                    carryover * prod_days_carry + new_added * prod_days_new

                ) * daily_cap

                qty_remaining -= week_production

            finished = qty_remaining <= _rw_tol



            setup_waste = total_setup_mc_days  # mc-days ที่เสียไปกับ setup

            # คำนวณ efficiency (ใช้ actual working days ของแต่ละ week)

            total_machine_days = sum(

                mc * wd for mc, wd in zip(actual_use_list, actual_wd_list) if mc > 0
            )
            productive_days = max(0, total_machine_days - setup_waste)
            efficiency = (
                (productive_days / total_machine_days * 100)
                if total_machine_days > 0
                else 0
            )

            if finished:

                # ตรวจสอบว่าจบตรง TARGET_KNIT หรือ TARGET_KNIT-1 หรือไม่

                last_week_produced = weeks_until_target[w_idx]

                last_week_index = week_index(last_week_produced)

                target_week_index = fg_week  # TARGET_KNIT

                

                print(f"[DEBUG TARGET] {item_code}: finished at week {last_week_index}, target {target_week_index}")

                

                if last_week_index == target_week_index or last_week_index == target_week_index - 1:

                    # จบตรง TARGET_KNIT หรือ TARGET_KNIT-1 → ยอมรับ option นี้

                    print(f"[DEBUG TARGET] ✅ Acceptable match: {item_code} finishes at {last_week_index} (target {target_week_index}, tolerance -1)")

                    if best_option is None or n_mc < best_option[0]:

                        # เก็บสัปดาห์ที่ทอเสร็จของ option นี้ไว้ด้วย (ตัวแปร last_week_index
                        # ถูกเขียนทับในรอบ n_mc ถัดไป → อ่านค่าหลังลูปไม่ได้)
                        best_option = (n_mc, weeks_needed, total_setup_mc_days, efficiency, last_week_index)

                    continue

                elif last_week_index < target_week_index - 1:

                    # จบก่อน TARGET_KNIT-1 → ต้องใช้เครื่องน้อยลงเพื่อกระจายไปจนถึง TARGET_KNIT

                    print(f"[DEBUG TARGET] ❌ Too early: {item_code} finishes at {last_week_index}, target {target_week_index}")

                    # คำนวณว่าต้องลดเครื่องกี่เครื่องให้จบตรง TARGET_KNIT

                    weeks_available = target_week_index - week_index(weeks_until_target[0]) + 1

                    if weeks_available > 0:

                        # คำนวณเครื่องที่ต้องการเพื่อกระจายผลิตจนถึง TARGET_KNIT

                        avg_daily_needed = order_qty / (weeks_available * factory_wd * daily_cap)

                        suggested_mc = max(1, int(avg_daily_needed * 0.8))  # ใช้ 80% เพื่อความปลอดภัย

                        if suggested_mc < n_mc:

                            # ลองใหม่ด้วยเครื่องน้อยลง

                            continue

                    continue

                else:

                    # เลย TARGET_KNIT → ไม่ใช่ option นี้

                    print(f"[DEBUG TARGET] ❌ Too late: {item_code} finishes at {last_week_index}, target {target_week_index}")

                    continue



        if best_option:

            required_machines = best_option[0]

            return mc_group, daily_cap, required_machines, True, item_gauge  # feasible



        else:

            # ไม่มี option ที่จบตรง TARGET_KNIT → หา option ที่ใกล้เคียงที่สุด

            print(f"[DEBUG TARGET] ⚠️ No perfect match for {item_code}, finding closest option...")

            

            # คำนวณ option ที่ใกล้เคียงที่สุด

            best_option = None

            min_distance = float('inf')

            

            for n_mc in range(1, int(max_try) + 1):

                # Simulate อีกครั้งเพื่อหา option ที่ใกล้เคียงที่สุด

                qty_remaining = order_qty

                weeks_needed = 0

                committed_mc = min(carryover_start, n_mc)

                total_setup_mc_days = 0

                

                for w_idx, week in enumerate(weeks_until_target):

                    if qty_remaining <= _rw_tol:

                        break

                    

                    actual_wd = factory_wd  # ค่าสุดท้ายจากชีท Work Day

                    avail_this_week = avail_per_week[w_idx]

                    if avail_this_week <= 0 and committed_mc > 0:

                        avail_this_week = committed_mc

                    

                    want_mc = min(n_mc, avail_this_week)

                    if want_mc <= 0:

                        continue

                    

                    # คำนวณ production และ setup

                    new_added = max(0, want_mc - committed_mc)

                    if new_added > 0:

                        setup_mc_days = new_added * setup_days

                        if setup_mc_days > actual_wd * want_mc:

                            continue

                        total_setup_mc_days += setup_mc_days

                        committed_mc = want_mc

                    

                    prod_days_new = actual_wd - setup_days

                    prod_days_carry = actual_wd

                    # YD-ORDERS: SUB_COLOR เปลี่ยน → carryover หัก 1 วัน (เฉพาะ week แรก)

                    if _yd_color_setup_days > 0 and w_idx == 0 and committed_mc > 0:

                        prod_days_carry = max(0, prod_days_carry - _yd_color_setup_days)

                    week_production = (

                        committed_mc * prod_days_carry + new_added * prod_days_new

                    ) * daily_cap

                    qty_remaining -= week_production

                    weeks_needed += 1

                

                if qty_remaining <= _rw_tol:

                    last_week_index = week_index(weeks_until_target[min(w_idx, len(weeks_until_target)-1)])

                    distance = abs(last_week_index - target_week_index)

                    

                    if distance < min_distance:

                        min_distance = distance

                        best_option = (n_mc, weeks_needed, total_setup_mc_days, 0, last_week_index)

            

            if best_option:

                print(f"[DEBUG TARGET] 🎯 Using closest option for {item_code}: distance {min_distance} weeks")

                required_machines = best_option[0]

                return mc_group, daily_cap, required_machines, True, item_gauge  # feasible (closest)

            else:

                # ไม่ทันทุก option → ใช้ optimal_mc ที่คำนวณไว้แล้ว + บอก caller ว่า NOT feasible

                print(f"[DEBUG TARGET] ❌ No feasible option for {item_code}, using optimal machines")

                required_machines = max(1, int(optimal_mc)) if 'optimal_mc' in locals() else int(max_try)

                return (

                    mc_group,

                    daily_cap,

                    required_machines,

                    False,

                    item_gauge,

                )  # not feasible

    return None, None, None, None, None





def get_best_machine_for_item(

    item_code,

    plan_week,

    last_production,

    required_machines_info=None,

    urgent_mode=False,

    past_rdd=False,

    force_max_mc=False,

):

    print(f"[DEBUG BEST MC] Processing {item_code} for week {plan_week}")

    print(f"[DEBUG BEST MC] Total items processed: {len([x for x in globals().get('_processed_items', [])])}")

    if '_processed_items' not in globals():

        globals()['_processed_items'] = []

    globals()['_processed_items'].append(f"{item_code}_{plan_week}")

    

    # ถ้ามีการคำนวณจำนวนเครื่องที่ต้องการมาแล้ว ให้ใช้ค่านั้น

    if required_machines_info is not None:

        mc_group, daily_cap, required_machines, *_ = required_machines_info

        print(f"[DEBUG BEST MC] Using pre-calculated: {mc_group}, {required_machines} machines")

        # ดึง gauge จาก required_machines_info (ตำแหน่งที่ 4 ถ้ามี)

        _rmi_gauge = (

            required_machines_info[4] if len(required_machines_info) > 4 else None

        )

        if mc_group and required_machines > 0:

            # หา GUAGE ของ item นี้

            item_machine_info = item_cap_data[

                (item_cap_data["ITEM_CODE"] == item_code)

                & (item_cap_data["MC_GROUP"] == mc_group)

            ]

            item_gauge = (

                item_machine_info.iloc[0]["GUAGE"]

                if not item_machine_info.empty

                else _rmi_gauge

            )

            # ดูเครื่องว่างจริง (หักที่จองไปแล้ว)

            actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge, item_code=item_code)

            # ตรวจสอบว่าเคยผลิต item นี้ใน week ก่อน (= เครื่องเดิม carry over)

            key = _resolve_carry_key(item_code, mc_group, item_gauge)

            setup_needed = True

            current_week_idx = week_index(plan_week)

            is_continuing = False  # order เดิมกำลังผลิตต่อจาก week ก่อน

            _bridge_locks(key, current_week_idx)
            _bridge_holds(key, current_week_idx)   # แถวกันเครื่อง = เครื่องยังอุ่น (ดู _bridge_holds)

            if key in last_production:

                last_week_idx = last_production[key]

                if current_week_idx - last_week_idx <= SETUP_GAP_WEEK:

                    setup_needed = False

                if current_week_idx - last_week_idx == 1:

                    is_continuing = True  # week ติดกัน = เครื่องเดิม carry over

                # carry-over จาก old plan: SC/SO NO เดิม และยังไม่ได้เริ่มผลิตใน new plan

                same_sc = last_sc_so_no.get(key) == sc_so_no

                if same_sc:  # SC/SO เดิม → carry over เสมอ (รวมกรณี FG Week ต่างกัน)

                    is_continuing = True

                    setup_needed = False

            # เครื่อง booking/KP ของสัปดาห์ก่อน (เว้นไม่เกิน SETUP_GAP_WEEK) ยังอุ่น → carry ได้
            # (last_production อาจชี้ booking ช่วงถัดไปที่อยู่หลังสัปดาห์นี้ → ข้างบนตีเป็นเครื่องใหม่)
            _bk_warm_mc = 0
            if not is_continuing:
                _bk_warm_mc, _bk_warm_idx = _booking_warm_mc(key, current_week_idx)
                if _bk_warm_mc > 0:
                    is_continuing = True
                    setup_needed = False
                    print(f"[BOOKING WARM] {item_code} W{plan_week}: เครื่อง booking {_bk_warm_mc} ตัวจาก idx {_bk_warm_idx} ยังอุ่น → ทอต่อไม่ต้อง setup")

            if is_continuing:

                # เครื่องเดิมจาก week ก่อน carry over โดยไม่ต้องเช็ค actual_remain

                # ใช้ fallback=0 ตรงกับ main loop เพื่อป้องกัน committed_carryover ผิด

                prev_mc = _bk_warm_mc or machines_in_use.get(key, 0)

                carryover = prev_mc  # เครื่องทั้งหมดจาก week ก่อนวิ่งต่อได้เลย

                # ถ้า feasible=True: cap ที่ required_machines (คำนวณมาแล้วว่า N เครื่องพอตั้งแต่ต้น)

                # ถ้า feasible=False: ใช้เต็มที่ (ไม่ทันด้วย N เครื่อง เปิดทุกสล็อที่มี)

                _is_feasible = (

                    required_machines_info[3]

                    if required_machines_info and len(required_machines_info) > 3

                    else True

                )

                # เครื่อง carry ยังไม่ถูกหักจากบัญชีพูลของสัปดาห์นี้ (บัญชีคิดรายสัปดาห์ หักตอนเขียนแถว)
                # → actual_remain คือที่ยืนที่เครื่อง carry เองต้องใช้ด้วย ห้ามนับเป็นเครื่องเพิ่มทั้งก้อน
                # (เจอจริง 14 ก.ย. 2569 FD5BASPG10A0 RAO G19 W44: carry 2 + ว่าง 2 → วาง 4 เกินพูล 2
                #  แล้วรอบซ่อม POOL OVERFLOW ตัด → ยอดไหลเลย target) ; เครื่องที่ booking ถืออยู่
                # ถูกหักจาก TOTAL_MC_REMAIN แล้ว → ไม่หักซ้ำ (สูตรเดียวกับตอนหักบัญชี: ACTUAL_MC − MC_BOOKING)
                _bk_now = int(booking_mc_by_week.get(key, {}).get(current_week_idx, 0) or 0)
                extra_avail = max(0, actual_remain - max(0, carryover - _bk_now))

                # เพดานเครื่องใหม่ = โควตาที่ผ้านี้ยังเหลือในสัปดาห์นี้ (ใบสั่งอื่นของผ้าเดียวกัน
                # ที่วางไปแล้วกินโควตาไปด้วย — ดู _item_setup_room)
                _setup_room = _item_setup_room(item_code, mc_group, item_gauge, plan_week)

                if _is_feasible:

                    can_add = max(0, required_machines - carryover)

                    new_additions = min(extra_avail, can_add, _setup_room)

                else:

                    new_additions = min(extra_avail, _setup_room)

                available_machines_count = carryover + new_additions

                type_used = get_type_used_jobs(plan_week, mc_group)

                # ส่ง committed_carryover=carryover เพื่อให้ carryover ผ่าน cap เสมอ

                available_machines_count = check_job_capacity_limit(

                    mc_group,

                    available_machines_count,

                    urgent_mode,

                    type_used,

                    committed_carryover=carryover,

                )

                return (



                    mc_group,

                    daily_cap,

                    setup_needed,

                    available_machines_count,

                    item_gauge,

                )

            if actual_remain > 0:

                _is_feasible = (

                    required_machines_info[3]

                    if required_machines_info and len(required_machines_info) > 3

                    else True

                )

                if _is_feasible:

                    # feasible: cap ที่ required_machines เพื่อไม่ใช้เครื่องเกินความจำเป็น

                    available_machines_count = min(required_machines, actual_remain)

                else:

                    # not feasible: ใช้เต็มที่ + main loop จะใช้ _forward_sim หา minimum

                    available_machines_count = actual_remain

                type_used = get_type_used_jobs(plan_week, mc_group)
                available_machines_count = check_job_capacity_limit(

                    mc_group, available_machines_count, urgent_mode, type_used

                )

                if available_machines_count <= 0:

                    # MC_GROUP จาก required_machines_info เต็ม → ลอง MC_GROUP อื่นที่ item มี CAP

                    # (fallthrough ไปใช้ logic ด้านล่างที่ลองทุก MC_GROUP)

                    pass



                else:

                    return (



                        mc_group,

                        daily_cap,

                        setup_needed,

                        available_machines_count,

                        item_gauge,

                    )

    # เครื่องสำรอง: ใช้ logic เดิมถ้าไม่มีการคำนวณล่วงหน้า
    # (ยุบเหลือกลุ่มเดียวต่อก้อน — งานก้อนเดียวต้องไม่ถูกแตกไปคนละกลุ่มในพูลเดียวกัน)

    available_machines = _dedup_mc_by_group(
        item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    )

    if available_machines.empty:

        return None, None, None, None, None



    # ใช้ความจุตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน

    # และ FA 20 มี priority สูงกว่า SKP 20

    # เรียงตาม MC_GROUP ที่มีเครื่องเหลือมากที่สุดก่อน (plan_week)

    available_machines = available_machines.copy()

    available_machines["_mc_remain"] = available_machines.apply(

        lambda r: get_actual_mc_remain(r["MC_GROUP"], plan_week, gauge=r.get("GUAGE"), item_code=item_code),

        axis=1,

    )

    available_machines = available_machines.sort_values("_mc_remain", ascending=False)

    current_week_idx = week_index(plan_week)

    # 1. ลองเครื่องที่ว่างในสัปดาห์นี้ก่อน

    for _, machine_row in available_machines.iterrows():

        mc_group = machine_row["MC_GROUP"]

        # ใช้ความจุตาม logic ใหม่สำหรับ MC_GROUP นี้

        item_gauge = machine_row["GUAGE"] if "GUAGE" in machine_row else None

        daily_cap = _get_capacity_for_mc_group(item_code, mc_group, item_gauge)

        daily_cap = adjust_daily_cap_for_item_special(daily_cap, item_code, mc_group, item_gauge)

        if daily_cap <= 0:

            continue  # ข้ามถ้าไม่มีความจุ

        # ดูเครื่องว่างจริง (หักที่จองไปแล้ว)

        actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge, item_code=item_code)

        # 🔧 FIX: เช็ค booking_mc_by_week (old booking) เพื่อป้องกันเกิน capacity

        # ถ้า old booking ใช้เครื่องไปแล้วใน week นี้ → หักออกจาก actual_remain

        mc_key = _resolve_carry_key(item_code, mc_group, item_gauge)

        _current_week_idx = week_index(plan_week)

        _booking_mc_used = 0

        if mc_key in booking_mc_by_week and _current_week_idx is not None:

            _booking_mc_used = booking_mc_by_week.get(mc_key, {}).get(_current_week_idx, 0)

        # หัก booking_mc_used จาก actual_remain

        actual_remain = max(0, actual_remain - _booking_mc_used)

        # Cylinder change: ถ้า pool ว่าง ลองเปลี่ยน cylinder จาก gauge อื่นใน MC_CAT เดียวกัน
        _cylinder_changed = False
        if actual_remain == 0:
            # ตรวจว่า item มี booking active สัปดาห์นี้หรือไม่
            # ถ้ามี → booking machines ถูกหักจาก TOTAL_MC_REMAIN ไปแล้ว จึงไม่ต้อง cylinder change
            _has_booking_this_week = (
                _booking_mc_used > 0
                and mc_key in booking_active_week_set
                and _current_week_idx is not None
                and _current_week_idx in booking_active_week_set.get(mc_key, set())
            )
            if _has_booking_this_week:
                # booking machines คือเครื่องที่มีอยู่แล้ว (ถูกหักจาก TOTAL_MC_REMAIN แล้ว)
                # ไม่ต้อง cylinder change — restore actual_remain กลับเป็น booking count
                actual_remain = _booking_mc_used
                print(f"[BOOKING RESTORE] {item_code} W{plan_week}: actual_remain=0 เพราะ double-subtract booking → restore to {_booking_mc_used}")
            elif str(item_code).strip().upper() in _s9_only_items:
                # S9 Only: ข้าม cylinder change เสมอ (ทั้ง Pass 1 และ Pass 2) — ใช้ MC จาก S9 pool เท่านั้น
                pass
            else:
                _cyl_mc_cat = _mc_to_type1(mc_group, item_gauge)
                _cyl_factory = _mc_to_factory(mc_group, item_gauge)
                _cyl_tgt_g = _normalize_gauge(item_gauge)
                _cyl_trigger_week = _TL.shift(plan_week, -1)  # (YW: เดินบนปฏิทิน) สั่งเปลี่ยน 1 week ก่อนผลิต → machine พร้อมทัน week ที่ผลิต
                # ตรวจว่าถ้ารอ JIT window ปกติ (rdd-2) จะยังมีเวลาผลิตพอไหม
                # ถ้าไม่พอ → override JIT เพื่อเริ่มผลิตทันเวลา
                _pw_idx_p0 = week_index(plan_week)
                _req_mc_p0 = required_machines_info[2] if required_machines_info and len(required_machines_info) > 2 else 1
                _weeks_to_rdd_p0 = (
                    (_current_order_rdd_idx - _pw_idx_p0)
                    if (_pw_idx_p0 is not None and _current_order_rdd_idx is not None)
                    else 99
                )
                # ถ้า deadline อยู่ภายใน (required_machines + 2) สัปดาห์ → ต้องเริ่มตอนนี้ → override JIT
                # _weeks_to_rdd > 0: ป้องกัน PAST_RDD (negative weeks) ที่ทำให้ override เสมอ
                _cyl_jit_override_p0 = 0 < _weeks_to_rdd_p0 <= _req_mc_p0 + 2
                if _cyl_jit_override_p0:
                    print(f"[CYL JIT OVERRIDE] {item_code} W{plan_week}: pool=0, weeks_to_rdd={_weeks_to_rdd_p0} <= req_mc={_req_mc_p0}+2 → jit_override=True")
                if _try_cylinder_change(_cyl_mc_cat, _cyl_factory, _cyl_tgt_g, _cyl_trigger_week, item_code, mc_group, jit_override=_cyl_jit_override_p0):
                    # cylinder change สำเร็จ → เครื่องใหม่ 1 ตัวพร้อมใช้ (ไม่ขึ้นกับ pool ที่อาจถูกใช้ไปแล้ว)
                    actual_remain = 1
                    _cylinder_changed = True  # เครื่องที่เพิ่งเปลี่ยน cylinder ต้อง setup ใหม่เสมอ
        if actual_remain > 0:
            if _cylinder_changed:
                # cylinder เพิ่งถูกเปลี่ยนมาเพื่อ item นี้โดยเฉพาะ → bypass job cap
                available_machines_count = actual_remain
            else:
                type_used = get_type_used_jobs(plan_week, mc_group)

                available_machines_count = check_job_capacity_limit(

                    mc_group, actual_remain, urgent_mode, type_used

                )

                if available_machines_count <= 0:

                    continue  # ลอง MC_GROUP ถัดไป



            key = _resolve_carry_key(item_code, mc_group, item_gauge)

            setup_needed = True

            _bridge_locks(key, current_week_idx)
            _bridge_holds(key, current_week_idx)   # แถวกันเครื่อง = เครื่องยังอุ่น (ดู _bridge_holds)
            _prev_cyl_key = (_TL.shift(plan_week, -1), str(item_code).strip().upper(), str(mc_group).strip().upper())
            if not _cylinder_changed and (_prev_cyl_key not in _cylinder_change_for_item) and key in last_production:

                last_week_idx = last_production[key]

                if current_week_idx - last_week_idx <= SETUP_GAP_WEEK:

                    setup_needed = False

            return (



                mc_group,

                daily_cap,

                setup_needed,

                available_machines_count,

                item_gauge,

            )

    # 2. ถ้าไม่มีเครื่องว่าง ลอง MC ที่เคยผลิต item เดียวกัน

    previous_mcs = [key[1] for key in last_production if key[0] == item_code]

    for prev_mc in previous_mcs:

        prev_mc_row = available_machines[available_machines["MC_GROUP"] == prev_mc]

        if not prev_mc_row.empty:

            mc_group = prev_mc

            # ใช้ความจุตาม logic ใหม่สำหรับ MC_GROUP นี้

            item_gauge = (

                prev_mc_row.iloc[0]["GUAGE"] if "GUAGE" in prev_mc_row.iloc[0] else None

            )

            daily_cap = _get_capacity_for_mc_group(item_code, mc_group, item_gauge)

            daily_cap = adjust_daily_cap_for_item_special(daily_cap, item_code, mc_group, item_gauge)

            if daily_cap <= 0:

                continue  # ข้ามถ้าไม่มีความจุ

            actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge, item_code=item_code)

            if actual_remain > 0:

                type_used = get_type_used_jobs(plan_week, mc_group)

                available_machines_count = check_job_capacity_limit(

                    mc_group, actual_remain, urgent_mode, type_used

                )

                if available_machines_count <= 0:

                    continue



                setup_needed = False

                return (



                    mc_group,

                    daily_cap,

                    setup_needed,

                    available_machines_count,

                    item_gauge,

                )

    return None, None, None, None, None





def next_week(week):

    idx = week_index(week)

    if idx is None:

        return None



    idx += 1

    # ห้ามเดินเลย horizon ที่ตารางเครื่องครอบคลุม (ดู PLAN_HORIZON_IDX) — เลยไปคือไปอ่าน
    # เครื่องว่างของสัปดาห์เลขเดียวกันในปีที่ผ่านมาแล้ว ; ยังไม่ถูกคำนวณ (bootstrap) = ไม่กัน
    _horizon = globals().get("PLAN_HORIZON_IDX")

    if idx >= len(calendar_week):

        return None

    if _horizon is not None and idx > _horizon:

        return None

    return int(calendar_week.iloc[idx]["WEEK"])



TODAY_WEEK = get_week_from_date(TODAY)

# TODAY_IDX: หา row บน timeline ที่ครอบ TODAY โดยตรงจากวันที่ (กันยึดผิดปี)
# เดิมใช้ week_index(TODAY_WEEK) แต่ TODAY_WEEK เป็น bare week และตอน bootstrap
# TODAY_IDX ยังไม่มี → week_index คืน idx[0] = ปีแรกสุดในปฏิทิน (เช่นปี 2026 แทนที่จะเป็น 2027)
_today_rows = calendar_week.index[
    (calendar_week["WEEK_START"] <= TODAY) & (calendar_week["WEEK_END"] >= TODAY)
]
if len(_today_rows) > 0:
    TODAY_IDX = int(_today_rows[0])
else:
    # TODAY อยู่นอกช่วงปฏิทิน → ใช้ row สุดท้ายที่เริ่มก่อน/ตรงกับ TODAY (fallback = แถวแรก)
    _before = calendar_week.index[calendar_week["WEEK_START"] <= TODAY]
    TODAY_IDX = int(_before[-1]) if len(_before) > 0 else week_index(TODAY_WEEK)

# ── ขอบ horizon ที่ตารางเครื่องครอบคลุมจริง ─────────────────────────────────────
# ตารางเครื่องคีย์ด้วย YW แล้ว (ไม่ชนข้ามปี) → horizon = สัปดาห์ติดกันจากวันนี้ที่มีแถวในตาราง
# AVA_MC เติมแถวไว้ถึงสุดปฏิทิน จึงวางได้ยาวเท่าที่ Calendar.xlsx มี ไม่มีเพดาน 1 ปีอีกแล้ว
# เลยขอบนี้ next_week คืน None = หมดสัปดาห์ให้วาง
_CAP_WEEKS = {int(w) for w in summary_mc["WEEK"].unique() if int(w) != 99}

PLAN_HORIZON_IDX = TODAY_IDX
for _hi in range(TODAY_IDX, len(calendar_week)):
    if int(calendar_week.iloc[_hi]["WEEK"]) not in _CAP_WEEKS:
        break
    PLAN_HORIZON_IDX = _hi
if PLAN_HORIZON_IDX <= TODAY_IDX:
    # ตารางเครื่องไม่ครอบสัปดาห์ปัจจุบัน (ข้อมูลผิดปกติ) → ไม่กัน ดีกว่าวางแผนไม่ได้เลย
    PLAN_HORIZON_IDX = len(calendar_week) - 1
    print("⚠️ ตารางเครื่องไม่ครอบสัปดาห์ปัจจุบัน — ข้ามการกัน horizon")
else:
    print(f"📅 horizon ที่วางแผนได้: {yw_label(calendar_week.iloc[TODAY_IDX]['WEEK'])} → "
          f"{yw_label(calendar_week.iloc[PLAN_HORIZON_IDX]['WEEK'])} "
          f"({PLAN_HORIZON_IDX - TODAY_IDX + 1} สัปดาห์) — เลยจากนี้ = งานล้น (UNPLANNED)")
    # เตือนล่วงหน้าเมื่อ Calendar.xlsx เหลือไม่ถึง 2 ปีจากวันนี้ — ช่วงที่เลยปฏิทิน = งานล้น (UNPLANNED)
    if len(calendar_week) - 1 - TODAY_IDX < 104:
        print(f"⚠️ ปฏิทินมีถึง {yw_label(calendar_week.iloc[-1]['WEEK'])} เหลือไม่ถึง 2 ปีจากวันนี้ "
              f"— ควรเติมปีถัดไปใน Calendar.xlsx (งานที่ต้องวางเลยสุดปฏิทินจะตก UNPLANNED)")

# แผนที่ "เลขสัปดาห์ → ปี" ของกรอบเวลาที่แผนอ้างถึงจริง = อดีตใกล้ ๆ (warm carry / booking
# ที่เพิ่งผ่าน) ถึงสุด horizon ; กรอบนี้กว้างไม่เกิน 52 สัปดาห์ เลขสัปดาห์จึงไม่ซ้ำ = แปลงปีได้
# แน่นอน ไม่ต้องพึ่ง anchor (ดู year_of_week) ซึ่งให้คำตอบต่างกันตามตำแหน่งที่แผนเดินอยู่
_YEAR_WINDOW_BACK = 26
_YEAR_BY_WEEK_WINDOW: dict = {}
_IDX_BY_WEEK_WINDOW: dict = {}


def _claim_week(_yi: int) -> None:
    """จองเลขสัปดาห์เปล่านี้ให้ตำแหน่งปฏิทิน _yi (ตำแหน่งแรกที่จองได้ชนะ) — ใช้กับค่าเก่าที่ไม่มีปีเท่านั้น"""
    _yw = int(calendar_week.iloc[_yi]["WEEK_NUM"])
    _YEAR_BY_WEEK_WINDOW.setdefault(_yw, int(calendar_week.iloc[_yi]["YEAR"]))
    _IDX_BY_WEEK_WINDOW.setdefault(_yw, int(_yi))


# ⚠️ ลำดับสำคัญ: ไล่ "อนาคต (วันนี้ → สุด horizon)" ก่อน แล้วค่อยย้อนอดีตใกล้ ๆ
# เพราะพอ horizon ยาวข้ามปี กรอบรวม (อดีต+อนาคต) จะเกิน 52 สัปดาห์ = เลขสัปดาห์ซ้ำ
# ถ้าให้อดีตจองก่อน W6 จะกลายเป็น W6 ของปีนี้ (ผ่านไปแล้ว) ทั้งที่แผนหมายถึง W6 ปีหน้า
for _yi in range(TODAY_IDX, min(PLAN_HORIZON_IDX + 1, len(calendar_week))):
    _claim_week(_yi)
# อดีตใกล้ ๆ = เอาไว้อ้างงานที่เพิ่งผลิต/booking ที่ยังอุ่น (เฉพาะเลขที่อนาคตไม่ได้จองไว้)
for _yi in range(TODAY_IDX - 1, max(-1, TODAY_IDX - _YEAR_WINDOW_BACK - 1), -1):
    _claim_week(_yi)


def week_pos(week, anchor=None):
    """ตำแหน่งบนปฏิทินของสัปดาห์ — เหมือน week_index แต่ยึด "กรอบเวลาจริง" ก่อน

    week_index เลือก occurrence ตาม anchor (ตำแหน่งที่แผนเดินอยู่) ซึ่งให้คำตอบต่างกันได้
    เช่น ตอนแผนอยู่ปลายปีแล้วอ้าง W36 จะได้ W36 ของ "ปีหน้า" ทั้งที่หมายถึงปีนี้
    → PLAN_IDX เพี้ยน แล้วทั้ง PLAN_YEAR และลูกโซ่ carry/setup ที่คีย์ด้วย index ก็เพี้ยนตาม
    """
    try:
        _w = int(week)
    except (TypeError, ValueError):
        _w = None
    if _w is not None and _w >= 100000:
        return _IDX_BY_YW.get(_w)
    if anchor is None and _w is not None and _w in _IDX_BY_WEEK_WINDOW:
        _f = sys._getframe(1)
        _BARE_WEEK_HITS[_f.f_lineno] = _BARE_WEEK_HITS.get(_f.f_lineno, 0) + 1
        return _IDX_BY_WEEK_WINDOW[_w]
    return week_index(week, anchor)





def _make_type_key(factory: str, mc_type: str) -> str:

    """สร้าง type_key: OM/OMNOI ไม่มี Type ใช้ชื่อ factory อย่างเดียว

    SINGLE (TUBE) รวมกับ SINGLE เป็นตัวเดียวกัน"""

    if factory in ("OM", "OMNOI"):

        return "OM"

    

    # ตัด (TUBE) ออก - SINGLE (TUBE) = SINGLE

    mc_type_clean = mc_type.replace(" (TUBE)", "").strip() if mc_type else ""

    

    return f"{factory}_{mc_type_clean}" if mc_type_clean else factory





def _get_type_key_for_mc(mc_group: str) -> str:

    """คืน type_key ของ MC_GROUP จาก master_mc"""

    _info = master_mc[master_mc["MC"] == mc_group]

    if _info.empty:

        return "PHET_DOUBLE"



    _fac = str(_info.iloc[0]["Factory"]).strip().upper()

    _raw = _info.iloc[0].get("Type", "")

    _typ = "" if pd.isna(_raw) else str(_raw).strip().upper()

    return _make_type_key(_fac, _typ)





def get_type_used_jobs(plan_week: int, mc_group: str) -> int:

    """คืนจำนวน jobs ที่ใช้ไปแล้วใน week นั้น รวมทุก MC_GROUP ใน factory type เดียวกัน cap PHET_DOUBLE=33, PHET_SINGLE=44, OM=13 นับรวม factory-wide ทุก MC_GROUP ใน type นั้น"""

    _target_type = _get_type_key_for_mc(mc_group)

    _week_usage = weekly_job_usage.get(plan_week, {})

    _total = 0

    for _mc, _jobs in _week_usage.items():

        if _get_type_key_for_mc(_mc) == _target_type:

            _total += _jobs

    return _total





def get_remaining_job_slots(plan_week: int, mc_group: str) -> int:

    """คืน job slots ที่เหลืออยู่สำหรับ factory type ของ mc_group ใน week นั้น"""

    # ปิดสวิตช์คุมโควตาช่าง = ไม่จำกัด
    if not JOB_ENFORCE:

        return JOB_CAP_UNLIMITED

    mc_info = master_mc[master_mc["MC"] == mc_group]

    if mc_info.empty:

        factory, mc_type = "PHET", "DOUBLE"

    else:

        factory = str(mc_info.iloc[0]["Factory"]).strip().upper()

        mc_type = str(mc_info.iloc[0].get("Type", "DOUBLE")).strip().upper()

    if factory == "PHET":

        max_jobs = JOB_CAP["PHET_DOUBLE"] if mc_type == "DOUBLE" else JOB_CAP["PHET_SINGLE"]

    elif factory in ("OM", "OMNOI"):

        max_jobs = JOB_CAP["OM"]

    else:

        return JOB_CAP_UNLIMITED  # OUTSOURCE → unlimited



    used = get_type_used_jobs(plan_week, mc_group)

    return max(0, max_jobs - used)





def detect_and_fill_unused_capacity(plans_list, orders_df, summary_mc):

    """

    Detect unused machine capacity and fill with same-item orders from different SCs.

    Optimizes machine utilization by identifying week-item-mc-gauge combinations 

    with unused capacity and filling them with pending orders of the same item.

    """

    if not plans_list:

        return plans_list



    print("🔍 Detecting and filling unused capacity...")

    # Convert plans to DataFrame for easier analysis

    plan_df = pd.DataFrame(plans_list)

    

    # 🔧 FIX: Include OLD bookings (detail_mc) in current_usage calculation

    # to prevent filling capacity already used by old bookings

    old_booking_usage = []

    if not detail_mc.empty:

        for _, row in detail_mc.iterrows():

            item_code = str(row.get("ITEM_CODE", "")).strip().upper()

            mc_group = str(row.get("MC_GROUP", "")).strip().upper()

            gauge = str(row.get("GUAGE", "")).strip()

            week = row.get("WEEK")

            kp_weight = row.get("KP_WEIGHT", 0)

            cap_tor = row.get("CAP ทอ", 0)

            

            if not item_code or not mc_group or pd.isna(week) or pd.isna(kp_weight):

                continue

            

            try:

                week = int(week)

                kp_weight = float(kp_weight)

                cap_tor = float(cap_tor) if not pd.isna(cap_tor) else 0

            except (ValueError, TypeError):

                continue

            

            if kp_weight <= 0:

                continue

            

            # Calculate daily capacity from CAP ทอ (แปลงฐาน 24 ชม. → ชั่วโมงจริงของกลุ่ม)

            daily_cap = cap_tor * (_base_work_hours(mc_group, gauge) / 24) if cap_tor > 0 else 0

            

            old_booking_usage.append({

                'PLAN_WEEK': week,

                'ITEM_CODE': item_code,

                'MC_GROUP': mc_group,

                'MC_GUAGE': gauge,

                'PRODUCE_QTY': kp_weight,

                'REQUIRED_MC': 0,  # Will be calculated from grouping

                'DAILY_CAPACITY': daily_cap

            })

    

    # Combine NEW plans and OLD bookings

    if old_booking_usage:

        old_booking_df = pd.DataFrame(old_booking_usage)

        combined_df = pd.concat([plan_df, old_booking_df], ignore_index=True)

    else:

        combined_df = plan_df.copy()

    

    # Group by week, item, mc_group, gauge to find current usage (NEW + OLD)

    current_usage = combined_df.groupby(['PLAN_WEEK', 'ITEM_CODE', 'MC_GROUP', 'MC_GUAGE']).agg({

        'PRODUCE_QTY': 'sum',

        'REQUIRED_MC': 'max',

        'DAILY_CAPACITY': 'first'

    }).reset_index()

    # Calculate theoretical full capacity per week-item-mc-gauge

    # Use actual available machines in that week, not just REQUIRED_MC

    def get_available_machines_for_week_mc(week, mc_group, gauge=None):

        """Get actual available machines for specific week, Type_1+Gauge pool"""

        type_1 = _mc_to_type1(mc_group, gauge)

        gauge_str = _normalize_gauge(gauge) if gauge else ""

        mask = week_mask(summary_mc, week) & (summary_mc["TYPE_1"] == type_1)

        if gauge_str:

            mask = mask & (summary_mc["GUAGE"].apply(_normalize_gauge) == gauge_str)

        mc_rows = summary_mc[mask]

        if mc_rows.empty:

            return 0

        # พูลแยก (SKP/SKPTA·SKPLE): คิดเฉพาะพูลของเครื่องนี้ ไม่รวมพูลอื่น
        _pool_label = _mc_to_pool_label(mc_group, gauge)
        if (_pool_label and (type_1, gauge_str) in _SPLIT_CG
                and "MC_POOL" in summary_mc.columns):
            mc_rows = mc_rows[mc_rows["MC_POOL"].astype(str).str.strip() == _pool_label]

        return int(mc_rows["TOTAL_MC_REMAIN"].sum())

    

    # Add available machines column

    current_usage['AVAILABLE_MC'] = current_usage.apply(

        lambda row: get_available_machines_for_week_mc(row['PLAN_WEEK'], row['MC_GROUP'], row.get('MC_GUAGE')),

        axis=1

    )

    

    current_usage['FULL_CAPACITY'] = (

        current_usage['AVAILABLE_MC'] * 

        current_usage['DAILY_CAPACITY'] * 

        7  # Assuming 7 working days for full capacity calculation

    )

    # Find combinations with unused capacity (less than 95% utilization)

    unused_capacity = current_usage[

        current_usage['PRODUCE_QTY'] < (current_usage['FULL_CAPACITY'] * 0.95)

    ].copy()

    if unused_capacity.empty:

        print("✅ No unused capacity detected")

        return plans_list

    
    # 🔧 FIX: Filter out weeks where OLD bookings exist for same item/MC
    # to prevent filling capacity already used by old bookings
    unused_capacity_filtered = []
    for _, row in unused_capacity.iterrows():
        week = row['PLAN_WEEK']
        item = row['ITEM_CODE']
        mc_group = row['MC_GROUP']
        # Check if OLD booking exists for this item/MC/week in detail_mc
        old_check = detail_mc[
            (detail_mc["ITEM_CODE"].astype(str).str.upper() == item.upper()) &
            (detail_mc["MC_GROUP"].astype(str).str.upper() == mc_group.upper()) &
            week_mask(detail_mc, week)
        ]
        if not old_check.empty:

            old_mc_ceil = old_check.iloc[0].get("MC_USE_CEIL", 0)

            if old_mc_ceil > 0:

                print(f"  🔧 SKIP CAPACITY OPT: {item}+{week}+{mc_group}: OLD booking using {old_mc_ceil} machines, skip to avoid exceeding capacity")

                continue

        unused_capacity_filtered.append(row)

    

    if not unused_capacity_filtered:

        print("✅ No unused capacity after filtering OLD booking weeks")

        return plans_list

    

    unused_capacity = pd.DataFrame(unused_capacity_filtered)



    print(f"📊 Found {len(unused_capacity)} week-item-mc combinations with unused capacity")

    # Get pending orders that could fill the capacity

    pending_orders = orders_df[

        (orders_df['Pending Plan'] > 0) &

        (~orders_df['SC/SO NO'].isin(plan_df['SC_SO_NO'].unique()))

    ].copy()

    

    # 🔧 FIX: สร้าง set ของ Item+Week+MC_GROUP ที่มีอยู่แล้ว เพียงครั้งเดียว

    existing_item_week_mc = set()

    for existing_plan in plans_list:

        existing_item = existing_plan.get('ITEM_CODE', '')

        existing_week = existing_plan.get('PLAN_WEEK', 0)

        existing_mc = existing_plan.get('MC_GROUP', '')

        if existing_item and existing_week and existing_mc:

            existing_item_week_mc.add((existing_item, existing_week, existing_mc))

    

    additional_plans = []

    for _, usage_row in unused_capacity.iterrows():

        week = usage_row['PLAN_WEEK']

        item = usage_row['ITEM_CODE']

        mc_group = usage_row['MC_GROUP']

        gauge = usage_row['MC_GUAGE']

        unused_qty = usage_row['FULL_CAPACITY'] - usage_row['PRODUCE_QTY']

        if unused_qty <= 0:

            continue

        

        # 🔧 FIX: Skip weeks <= TODAY+2 to prevent planning before yarn arrives

        # เทียบตำแหน่งบนปฏิทิน (YW บวก 2 ตรง ๆ ข้ามปีไม่ได้ ; เดิมเลขเปล่า W4 ปีหน้าถูกมองว่าเป็นอดีต)
        _week_pos_opt = week_index(week)
        if _week_pos_opt is None or _week_pos_opt <= TODAY_IDX + FREEZE_WEEKS:

            print(f"  🔧 SKIP CAPACITY OPT: {item}+{week}+{mc_group}: week <= TODAY+2 ({yw_label(_TL.at(TODAY_IDX + FREEZE_WEEKS))}), skip")

            continue



        # Find pending orders of same item (different SC)

        matching_orders = pending_orders[

            (pending_orders['Item Code'] == item) &

            (pending_orders['MC GROUP'] == mc_group)

        ]

        if matching_orders.empty:

            continue



        # Sort by RDD (urgent first)

        matching_orders = matching_orders.copy()

        # FG Week เป็น composite YYYYWW แต่คอลัมน์เป็น float เพราะเกือบทุกแถวว่าง
        # (order_ready จริง: 6537/6579 ว่าง) → .astype(str) ได้ '202639.0' แล้ว [-2:] = '.0'
        # ส่วนแถวว่างได้ 'an' — int() พังทั้งคู่ = Planning ตายทั้งรอบ
        #
        # ใบที่ไม่มีกำหนดส่ง "ข้ามไป" ไม่ใช่ยัดลงแผน: โค้ดนี้เขียนบนสมมติฐานว่า FG Week มีค่าเสมอ
        # ถ้าปล่อยผ่าน แถวที่ได้จะมี TARGET_KNIT / RDD_WEEK ว่างติดไปทั้งแถว
        _fg_num = pd.to_numeric(matching_orders['FG Week'], errors='coerce')

        matching_orders = matching_orders[_fg_num.notna()].copy()

        if matching_orders.empty:

            continue

        # เรียงด้วยเลข composite เต็ม (ข้ามปีถูก — เดิมตัด 2 หลักท้ายทำให้ W52 มาก่อน W01 ปีหน้า)
        matching_orders['FG_WEEK_NUM'] = _fg_num.loc[matching_orders.index].astype('int64')

        matching_orders = matching_orders.sort_values('FG_WEEK_NUM')

        # ต้องวางให้ตรง TARGET_KNIT — ฟังก์ชันนี้ยัดงานลง "สัปดาห์ที่มีเครื่องเหลือ" (week) ซึ่งเลือก
        # ไม่ได้ ต่างจาก planner หลักที่ถอยหลังจาก target มาเอง → ถ้าสัปดาห์นั้นเลยเดดไลน์ทอของใบ
        # ไปแล้ว ต้องข้ามใบนั้น ไม่ใช่วางแล้วปล่อยให้ขึ้นเป็นงานไม่ทันบนหน้าแผน
        _week_idx = week_index(week)

        for _, order in matching_orders.iterrows():

            if unused_qty <= 0:

                break

            # TARGET_KNIT = FG Week − offset (FOB_TYPE / DOUBLE-30 เกจ 16-19 / SINGLE ด้ายเส้นเดียว)
            # ต้องคิดแบบเดียวกับ planner หลัก ไม่งั้นเดดไลน์ทอช้ากว่าจริง 3-4 สัปดาห์
            _tk_week, _tk_year = _target_knit_of(
                order['FG Week'], order.get('Orders Type'), order
            )

            if _tk_week is None:

                continue

            # เทียบด้วย index บน timeline ไม่ใช่เลขสัปดาห์เปล่า — W52 vs W01 ปีหน้าเทียบตรง ๆ ไม่ได้
            # สัปดาห์ที่มีเครื่องเหลือเลย TARGET_KNIT ไปแล้ว = ทอไม่ทัน → ข้ามใบนี้ (เลื่อนไป
            # สัปดาห์อื่นไม่ได้ เพราะเครื่องที่ว่างอยู่คือของสัปดาห์นี้บล็อกนี้เท่านั้น)
            _tk_idx = week_index(_tk_year * 100 + _tk_week)

            if _tk_idx is None or _week_idx is None:

                continue

            if _tk_idx < _week_idx:

                print(f"  🔧 SKIP CAPACITY OPT: {item}+{week}+{mc_group}: "
                      f"เลย TARGET_KNIT W{_tk_week} ของ SC "
                      f"{str(order.get('SO_NO', order.get('SC/SO NO', ''))).strip()} แล้ว")

                continue

            sc_so_no = str(order.get('SO_NO', order.get('SC/SO NO', ''))).strip()

            pending_qty = order['Pending Plan']

            # Calculate how much we can produce in remaining capacity

            produce_qty = min(unused_qty, pending_qty)

            if produce_qty > 0:

                # 🔧 FIX: ตรวจสอบว่า Item+Week+MC_GROUP นี้มีการ plan ไปแล้วหรือไม่

                # เพื่อป้องกันการซ้ำซ้อนกับ plans ที่มีอยู่แล้ว

                current_item_week_mc = (item, week, mc_group)

                if current_item_week_mc in existing_item_week_mc:

                    print(f"  ⚠️  SKIPPED capacity optimization: {item}+{week}+{mc_group} already planned!")

                    break

                

                # Create additional plan entry

                additional_plan = {

                    'ITEM_CODE': item,

                    'SC_SO_NO': sc_so_no,

                    'MC_GROUP': mc_group,

                    'MC_GUAGE': gauge,

                    'FACTORY_TYPE': FACTORY_TYPE_MAP.get(mc_group, "UNKNOWN"),

                    'PLAN_WEEK': week,

                    'PRODUCE_QTY': produce_qty,

                    'SETUP_DAYS': 0,  # No setup needed - same item/mc/gauge

                    'REQUIRED_MC': usage_row['REQUIRED_MC'],

                    'ACTUAL_MC': usage_row['REQUIRED_MC'],

                    'CARRYOVER_MC': usage_row['REQUIRED_MC'],

                    'NEW_MC': 0,

                    'FACTORY_WORKING_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week, gauge=gauge),

                    'CALENDAR_WORKING_DAYS': len(get_working_days_in_week(week)),

                    'ACTUAL_WORKING_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week, gauge=gauge),

                    'DAILY_CAPACITY': usage_row['DAILY_CAPACITY'],

                    'REVOLUTION_WEIGHT': get_revolution_weight_from_orders(item, mc_group),

                    'AVAILABLE_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week, gauge=gauge),

                    'ORDERS_QTY': order['Orders.Qty'],

                    'PENDING_PLAN': pending_qty - produce_qty,

                    'PLAN_QTY': pending_qty - produce_qty,

                    'ORDER_TYPE': order['Orders Type'],

                    'ORDER_DATE': order['Date'],

                    'FG_WEEK': order['FG Week'],

                    'TARGET_KNIT': _tk_week,  # FG Week − offset (คิดไว้ตอนเช็คด่าน target ด้านบน)

                    'MATERIAL_CONTENT': str(order.get('MATERIAL_CONTENT', '')).strip(),

                    'IS_CORE_ITEM': '',

                    'CUSTOMER': str(order.get('Customer', '')).strip(),

                    'PLAN_SOURCE': 'NEW',

                    'LT_YARN': order.get('DYE_END_DATE') if str(order.get('Orders Type', '')).strip() == 'YD-ORDERS' else get_yarn_lt_days(item),

                    'YARN_USED': _yarn_used_lookup.get(str(item).strip().upper(), ''),

                    'DATE_IN': order.get('DATE_IN'),

                    'EARLIEST_PLAN_WEEK': order.get('DYE_END_DATE') if str(order.get('Orders Type', '')).strip() == 'YD-ORDERS' else get_yarn_lt_earliest_week(item, date_in=order.get('DATE_IN')),

                    'SUB_COLOR': str(order.get('SUB_COLOR', '')).strip(),

                    'PO_NO': str(order.get('PO_NO', '')).strip(),

                    'YARNPO': str(order.get('YARNPO', '') or '').strip(),

                    'RDD_WEEK': _rdd_week_of(order),

                    'SC_LINE_ID': ColorOpen.so_text(order.get('SC_LINE_ID')),  # ว่างจริง = "" ไม่ใช่ "nan"

                    # เลขใบจอง — มีเฉพาะงานที่มาจาก booking ; แถวที่มาจากใบ SC ไม่มีคอลัมน์นี้
                    # ต้องผ่าน so_text: ค่าที่หายเป็น NaN ซึ่ง `x or ''` ไม่กรองให้ (NaN เป็น truthy)
                    # แล้ว str() จะได้ข้อความ "nan" หลุดลงชีทแผน
                    'KP_NO': ColorOpen.so_text(order.get('KP_NO')),

                }

                additional_plan['NAY_COLOR'] = str(order.get('NAY_COLOR', '')).strip()

                additional_plan['COLOR_DESC'] = str(order.get('COLOR_DESC', '')).strip()

                additional_plan['LINE_REMARK'] = _clean_remark(order.get('LINE_REMARK', ''))

                # วัน/สัปดาห์ด้ายเข้าครบ (ล็อตที่ไกลสุด) — หน้าเว็บใช้โชว์บนบล็อก Gantt
                additional_plan['PO_IN_DATE'], additional_plan['PO_IN_WEEK'] = po_in_fields(order)
                additional_plan['YARN_STOCK'] = yarn_stock_note(order)

                # ธงก้อนสีของออร์เดอร์ (เหมือนแถวหลัก/แถวเติมเครื่อง) — แถวที่ไม่มีธงจะถูก
                # _main_row_is_color ตีความด้วยเกณฑ์ระดับ item ซึ่งข้ามกติกา "คิดต่อใบสั่ง"
                additional_plan['COLOR_PART'] = _color_part_text(order)

                additional_plans.append(additional_plan)

                unused_qty -= produce_qty

                print(f"  📈 Added {produce_qty:.2f} units of {item} (SC: {sc_so_no}) to week {week}")

    if additional_plans:

        print(f"✅ Added {len(additional_plans)} capacity optimization plans")

        plans_list.extend(additional_plans)

    else:

        print("ℹ️  No suitable pending orders found for capacity optimization")

    return plans_list



# =========================

# LOAD OLD PRODUCTION PLAN FOR VALIDATION

# =========================

# ❌ DISABLED: old-plan carryover จาก committed_plan.xlsx
# เดิมโค้ดตรงนี้อ่าน committed_plan.xlsx (= ผลลัพธ์รอบก่อน) มาเป็น "old plan carryover"
# แล้วเอาไป pre-load เครื่อง/หัก qty/ล็อกเครื่องเดิม → ทำให้ "รันแผนใหม่" ยังหยิบแผนเก่ามาปน
# เกิด feedback loop: ผลลัพธ์ไม่นิ่ง (booking/order เดิมเป๊ะ ๆ แต่รันได้คนละสัปดาห์ W34↔W36)
# บังคับ old_plan ให้ว่างเสมอ → รันแผนใหม่คำนวณสดล้วนจาก booking + order (deterministic)
# หมายเหตุ: ฝั่ง "เขียน" committed_plan ก็ถูกปิดแล้ว (ดู block ปิด auto-backup ท้ายไฟล์)
#   สำคัญบน server (deploy): แม้ committed_plan.xlsx เก่าจะค้างอยู่ ก็จะไม่ถูกอ่านมาใช้อีก
#   ถ้าอนาคตจะทำ carryover จาก "แผนที่ user กด commit จริง" ค่อยเปิดอ่านเฉพาะไฟล์นั้น
old_plan_df = pd.DataFrame()

# สร้าง dict: {sc_so_no_upper: total_produce_qty} จาก old plan (NEW) ที่วาง week >= TODAY

# ใช้หัก qty_left เพื่อไม่วางซ้ำ (carry-over qty)

old_plan_produced_qty = {}

if (

    not old_plan_df.empty

    and "PRODUCE_QTY" in old_plan_df.columns

    and "PLAN_WEEK" in old_plan_df.columns

):

    _sc_col = "SC_SO_NO" if "SC_SO_NO" in old_plan_df.columns else None

    if _sc_col:

        for _, _opr in old_plan_df.iterrows():

            _opw = _opr.get("PLAN_WEEK")

            if pd.isna(_opw):

                continue



            _opw_idx = week_index(int(_opw))

            if _opw_idx is None or _opw_idx < TODAY_IDX:

                continue  # เฉพาะ week อนาคต



            _op_sc = str(_opr[_sc_col]).strip().upper()

            _op_qty = pd.to_numeric(_opr.get("PRODUCE_QTY", 0), errors="coerce")

            if pd.isna(_op_qty):

                _op_qty = 0

            if _op_sc and _op_sc != "NAN":

                # handle comma-separated SC/SO (e.g. "715033,S716032")

                for _sc_part in _op_sc.split(","):

                    _sc_part = _sc_part.strip().upper().lstrip("S")

                    if _sc_part:

                        old_plan_produced_qty[_sc_part] = old_plan_produced_qty.get(

                            _sc_part, 0

                        ) + float(_op_qty)

    print(

        f"📋 Old plan carry-over qty (week>={TODAY_WEEK}): {len(old_plan_produced_qty)} SOs"

    )



# =========================

# LOAD BOOKING DATA (ประวัติการผลิตจริง)

# =========================

# กลุ่มที่ไม่กินเครื่องทอของเรา — แหล่งเดียวกับ AVA_MC.py/Order.py/workday.py คือ McGroups.py
# (กลุ่มจ้างทออ่านจาก MasterMC: Factory = OUTSOURCE หรือไม่มีเครื่อง — ไม่ hard-code ชื่อ)
_BOOKING_EXCLUDE_MC = set(McGroups.non_knit_groups(_MASTER_MC_PATH))

# กลุ่มที่ "ไม่ใช่เครื่องทอ" → ห้ามนับเป็น setup job ต่อสัปดาห์ (ตั้งที่หน้า "ตั้งค่าระบบ")
# IMPORT = ผ้านำเข้า ไม่ได้ทอเอง (ไม่มี Factory/TOTAL_MC ใน MasterMC) แต่ MC_USE_CEIL
# ถูกคำนวณจากน้ำหนักผ้าล้วนๆ จนได้เลขหลักร้อย แล้วไปกินโควตา job ของ PHET_DOUBLE (default)
# ⚠️ ต้องตรงกับ Replan.JOB_EXCLUDE_MC และ webapp/backend/job_setup_live.JOB_EXCLUDE_MC
_JOB_EXCLUDE_MC = set(plan_config.get_list("KP_JOB_EXCLUDE_MC", upper=True))

booking_last_production = {}  # {(item, mc_group): week_index} — week สุดท้ายที่ผลิตจริง

booking_last_so = {}  # {(item, mc_group): so_no_normalized} — SO หมายเลขสุดท้าย

booking_produced_qty = {}  # {so_no_upper: total_knit_weight} — ผลิตไปแล้วทั้งหมด

# ⚠️ สวิตช์ "นับยอดทอสะสมด้วย KP_NO" — ปิดไว้ = พฤติกรรมเดิม (sum ต่อ SO)
# view booking มี KP_NO มานานแล้ว แต่ View_Booking ไม่เคย SELECT มา โค้ดข้างล่างจึงตกสาขา
# else (sum) มาตลอด ; พอเพิ่ม KP_NO เข้า SELECT_COLUMNS (16/9/2026) เงื่อนไขเดิมจะสลับไป
# ใช้ max ต่อ (KP_NO, SO_NO) เองทันที = ยอด "ทอไปแล้ว" ลดลงทั้งระบบ (เลิกนับซ้ำรายสัปดาห์)
# ซึ่งไปกระทบโควตาสีและยอดคงค้าง → ล็อกไว้ก่อน เปิดด้วย env ตอนรันเทียบผลเก่า/ใหม่
_USE_KPNO_PRODUCED = os.environ.get("KP_PRODUCED_BY_KPNO", "").strip().lower() in (
    "1", "true", "yes", "on"
)

# {(so_no_upper, item_upper): knit_weight} — ยอดที่ทอไปแล้วแยกราย item ของใบสั่ง
# ใช้จุดเดียว: หักออกจากโควตาสี (OPEN_COLOR_LOSS เป็นยอดทั้งใบ รวมส่วนที่ทอไปแล้ว)
# ⚠️ แยก dict ต่างหาก ห้ามยัด ITEM_CODE เข้า group key ของ booking_produced_qty เดิม —
#    ตัวเดิม dedup ด้วย max ต่อ (KP_NO, SO_NO) ถ้าซอยละเอียดขึ้นผลรวมต่อใบจะโตขึ้นทันที
booking_produced_qty_item = {}

booking_raw = load_all_booking_data()

if not booking_raw.empty:

    for col in ["ITEM_CODE", "MC_GROUP", "SO_NO", "TYPE", "KP_NO"]:

        if col in booking_raw.columns:

            booking_raw[col] = booking_raw[col].astype(str).str.strip().str.upper()

    booking_raw["WEEK"] = pd.to_numeric(booking_raw.get("WEEK"), errors="coerce")

    booking_raw["YEAR"] = pd.to_numeric(booking_raw.get("YEAR"), errors="coerce")

    _weeks_to_yw(booking_raw)  # WEEK → YW ด้วยปีจริงของ booking

    if "KNIT WEIGHT" not in booking_raw.columns:

        booking_raw["KNIT WEIGHT"] = 0

    else:

        booking_raw["KNIT WEIGHT"] = pd.to_numeric(

            booking_raw["KNIT WEIGHT"], errors="coerce"

        ).fillna(0)

    if "MC_GROUP" in booking_raw.columns:

        booking_raw = booking_raw[~booking_raw["MC_GROUP"].isin(_BOOKING_EXCLUDE_MC)]

    if "TYPE" in booking_raw.columns:

        booking_raw = booking_raw[booking_raw["TYPE"] != "COLLAR"]

    # เฉพาะแถวที่ผลิตจริง (KNIT WEIGHT > 0) และ YEAR 2025-2026

    _produced = booking_raw[

        (booking_raw["KNIT WEIGHT"] > 0) & (booking_raw["YEAR"].isin([2025, 2026]))

    ].copy()

    # สร้าง last_production จาก booking

    for _, _row in _produced.iterrows():

        _bi = str(_row.get("ITEM_CODE", "")).strip().upper()

        _bm = str(_row.get("MC_GROUP", "")).strip().upper()

        _bw = _row.get("WEEK")

        _bs = str(_row.get("SO_NO", "")).strip().upper()

        if not _bi or not _bm or pd.isna(_bw):

            continue



        _bw = int(_bw)

        _wi = week_index(_bw)

        if _wi is None:

            continue



        _key = _ck(_bi, _bm)

        if _key not in booking_last_production or _wi > booking_last_production[_key]:

            booking_last_production[_key] = _wi

            booking_last_so[_key] = _bs

    # สร้าง produced_qty per SO — ใช้ KP_NO เพื่อหลีกเลี่ยงการนับซ้ำรายสัปดาห์

    if "KP_NO" in _produced.columns and _USE_KPNO_PRODUCED:

        _kp_latest = (

            _produced.groupby(["KP_NO", "SO_NO"])["KNIT WEIGHT"].max().reset_index()

        )

        for _, _r in _kp_latest.iterrows():

            _so = str(_r["SO_NO"]).strip().upper()

            if _so and _so != "NAN":

                booking_produced_qty[_so] = (

                    booking_produced_qty.get(_so, 0) + _r["KNIT WEIGHT"]

                )

    else:

        if "SO_NO" in _produced.columns:

            for _so, _grp in _produced.groupby("SO_NO"):

                _so_key = str(_so).strip().upper()

                if _so_key and _so_key != "NAN":

                    booking_produced_qty[_so_key] = _grp["KNIT WEIGHT"].sum()

    # ยอดที่ทอไปแล้วแยกราย item ของใบสั่ง — ใช้หักโควตาสีเท่านั้น
    # dedup ด้วย max ต่อ (KP_NO, SO_NO, ITEM_CODE) แบบเดียวกับตัวรวมข้างบน (KP เดียวกัน
    # ถูกบันทึกซ้ำหลายสัปดาห์ ค่าล่าสุดคือยอดสะสม ไม่ใช่ยอดของสัปดาห์นั้น)
    if "ITEM_CODE" in _produced.columns and "SO_NO" in _produced.columns:
        _by_kp = "KP_NO" in _produced.columns and _USE_KPNO_PRODUCED
        _grp_keys_it = ["KP_NO", "SO_NO", "ITEM_CODE"] if _by_kp else ["SO_NO", "ITEM_CODE"]
        _agg_it = "max" if _by_kp else "sum"
        _it_latest = _produced.groupby(_grp_keys_it)["KNIT WEIGHT"].agg(_agg_it).reset_index()
        for _, _r in _it_latest.iterrows():
            _so_i = str(_r["SO_NO"]).strip().upper()
            _it_i = str(_r["ITEM_CODE"]).strip().upper()
            if not _so_i or _so_i == "NAN" or not _it_i or _it_i == "NAN":
                continue
            _k_i = (_so_i, _it_i)
            booking_produced_qty_item[_k_i] = (
                booking_produced_qty_item.get(_k_i, 0.0) + float(_r["KNIT WEIGHT"])
            )

    print(

        f"📚 Booking history: {len(booking_last_production)} (item,mc) records, {len(booking_produced_qty)} unique SOs"
        f", {len(booking_produced_qty_item)} (SO,item) pairs"

    )

else:

    print("📚 ไม่พบข้อมูล booking history")



# =========================

# TRACK LAST PRODUCTION

# =========================

last_production = {}

machines_in_use = {}  # {(item, mc_group): จำนวนเครื่องที่ใช้จริงใน week ล่าสุด}

last_sc_machines = {}  # {(item, mc_group, gauge, sc_so_no): จำนวนเครื่องล่าสุดต่อ SC — ป้องกัน SC อื่นทับข้อมูล}

last_sc_week = {}  # {(item, mc_group, gauge, sc_so_no): week_index ล่าสุดต่อ SC — ใช้แทน last_sc_so_no ที่ถูก SC อื่นทับ

weekly_mc_usage = {}  # {(week, mc_group): total machines} สำหรับ gradual increase

last_sc_so_no = (

    {}

)  # {(item, mc_group): SC/SO NO ของ order ที่ผลิตล่าสุด — ป้องกัน carry-over ข้าม color/order}

last_dye_end_date = {}  # {carry_key: DYE_END_DATE} — สำหรับ YD-ORDERS: ถ้า วันนัดย้อม เปลี่ยน ต้อง setup เพิ่ม 1 วัน

last_sub_color = {}  # {carry_key: sub_color} — สีล่าสุดที่ผลิตต่อ YD-ORDERS item

_yd_week_color_setups = {}  # {(carry_key, plan_week): cumulative color setup days} — สะสมวัน setup สีใน week เดียวกัน

booking_production_keys = set()  # keys ที่มาจาก detail_mc (booking จริง) — ใช้อนุญาต carryover จาก booking แม้ยังไม่มีใน new plan

booking_active_week_set = {}  # {(item, mc, gauge): set of w_idx ที่ booking active} — ใช้เช็ค carryover เมื่อ booking ครอบหลาย week

booking_mc_by_week = {}  # {(item, mc, gauge): {w_idx: mc_used}} — จำนวนเครื่องจาก booking รายสัปดาห์

# งานที่วางไม่ได้เพราะพูลเครื่องเต็ม (booking จองเกิน) — สรุปให้ผู้ใช้ท้ายการรัน
# ไม่งั้นงานจะหายจากแผนเงียบๆ โดยไม่รู้ว่าติดสัปดาห์ไหน
no_mc_skips = []  # [{"item", "sc", "mc_group", "gauge", "setup_week", "blocked_week", "remain", "need"}]

# Pre-populate last_production จาก detail_mc (DETAIL sheet ของ booking_final_ready25)

# ใช้ข้อมูลจริงจาก booking เพื่อรู้ว่า item นี้ถูก book ถึง week ไหน

for _, row in detail_mc.iterrows():

    item_code = str(row.get("ITEM_CODE", "")).strip().upper()

    mc_group = str(row.get("MC_GROUP", "")).strip().upper()

    plan_week = row.get("WEEK")

    mc_used = row.get("MC_USE_CEIL", 0)

    if not item_code or not mc_group or pd.isna(plan_week) or pd.isna(mc_used):

        continue



    if int(mc_used) == 0:

        continue



    plan_week = int(plan_week)

    # ใช้ index เปรียบเทียบแทน raw week number เพื่อรองรับข้ามปี

    # เช่น booking week 50 ปี 2025 vs TODAY_WEEK=10 ปี 2026: 50 > 10 → ผิด ถ้าใช้ raw number

    w_idx = week_index(plan_week)

    if w_idx is None:

        continue  # ข้ามเฉพาะ week ที่ไม่มีใน calendar (ไม่กรองตาม TODAY อีกต่อไป → ให้ดู last booking week จริงๆ)



    _det_gauge = str(row.get("GUAGE", "")).strip()

    key = _ck(item_code, mc_group, _det_gauge)

    booking_production_keys.add(key)  # track ว่า key นี้มาจาก booking จริง

    # 🔧 NEW: เก็บทุก week ที่ booking active เพื่อให้ carryover ตรวจได้ครอบคลุม

    # (last_production เก็บเฉพาะ week ล่าสุด ไม่พอถ้า plan_week อยู่ก่อน last booking week)

    booking_active_week_set.setdefault(key, set()).add(w_idx)

    _week_mc_map = booking_mc_by_week.setdefault(key, {})

    try:

        _mc_used_int = int(mc_used)

    except (ValueError, TypeError):

        _mc_used_int = 0

    # 🔧 FIX: ไม่ปรับ machines_in_use ตาม working days

    # MC_USE คือจำนวนเครื่องจริงที่ใช้ ไม่ใช่ capacity calculation

    # ถ้า booking บันทึก MC_USE=1 แปลว่าใช้เครื่อง 1 เครื่องจริงๆ ไม่ว่า working days จะเท่าไหร่

    if _mc_used_int > _week_mc_map.get(w_idx, 0):

        _week_mc_map[w_idx] = _mc_used_int



    if key not in last_production or w_idx > last_production[key]:

        last_production[key] = w_idx

        machines_in_use[key] = _mc_used_int

print(

    f"📋 โหลด last_production จาก detail_mc (booking_final_ready25): {len(last_production)} รายการ"

)


def _booking_warm_mc(key, cur_idx):
    """เครื่อง booking/KP ที่ยังอุ่นอยู่ ณ สัปดาห์ cur_idx → (จำนวนเครื่อง, idx สัปดาห์ booking) หรือ (0, None)

    = booking สัปดาห์ล่าสุด "ก่อน" cur_idx ที่เว้นไม่เกิน SETUP_GAP_WEEK — ทอต่อได้ไม่ต้อง setup
    (user 29 ก.ย. 2569: KP W40-41 แล้ว W42 ไม่มี KP ก็ทอต่อบนเครื่องนั้นได้)
    last_production เก็บแค่ booking สัปดาห์ท้ายสุด ถ้า booking มีต่ออีกช่วงข้างหน้า (เช่น KP W43) ค่านั้น
    จะอยู่ "หลัง" สัปดาห์ที่วาง → ตัวเช็ค carry เดิมตีเป็นเครื่องใหม่ แล้วไปติดโควตา setup ทั้งที่เครื่องอุ่นอยู่
    """
    if key is None or cur_idx is None:
        return 0, None
    _bw = booking_mc_by_week.get(key) or {}
    for _back in range(1, int(SETUP_GAP_WEEK) + 1):
        _mc = int(_bw.get(cur_idx - _back, 0) or 0)
        if _mc > 0:
            return _mc, cur_idx - _back
    return 0, None

# Merge old plan → ใช้เป็น fallback สำหรับ carryover ถ้า detail_mc/booking ไม่มี

if not old_plan_df.empty:

    for _, _row in old_plan_df.iterrows():

        # หาชื่อคอลัมน์ที่เป็นไปได้

        item_code = (

            _row.get("ITEM")

            or _row.get("Item")

            or _row.get("ITEM_CODE")

            or _row.get("Item Code")

        )

        mc_group = _row.get("MC_GROUP") or _row.get("MC GROUP") or _row.get("MC")

        plan_week = (

            _row.get("PLAN_WEEK") or _row.get("PLAN WEEK") or _row.get("PLAN_WEEK")

        )

        sc_no = (

            _row.get("SC_SO_NO")

            or _row.get("SC/SO NO")

            or _row.get("SC SO NO")

            or _row.get("SC/SO")

            or _row.get("SC")

        )

        if pd.isna(item_code) or pd.isna(mc_group) or pd.isna(plan_week):

            continue



        try:

            item_code = str(item_code).strip().upper()

            mc_group = str(mc_group).strip().upper()

            plan_week = int(plan_week)

        except Exception:

            continue



        # ใช้ index เปรียบเทียบแทน raw week number เพื่อรองรับข้ามปี

        w_idx = week_index(plan_week)

        if w_idx is None:

            continue



        # ใช้ ACTUAL_MC ก่อน (จำนวนเครื่องจริงที่ผลิต) — ถ้า = 0 แปลว่า week นั้นไม่ได้ผลิต → ข้าม

        _old_actual_mc = _row.get("ACTUAL_MC", None)

        try:

            _old_actual_mc_int = int(_old_actual_mc) if _old_actual_mc is not None and not pd.isna(_old_actual_mc) else 0

        except Exception:

            _old_actual_mc_int = 0

        if _old_actual_mc_int == 0:

            continue



        _old_gauge = _row.get("MC_GUAGE") or _row.get("MC GUAGE") or _row.get("GUAGE")

        _old_gauge_str = (

            str(_old_gauge).strip() if _old_gauge and not pd.isna(_old_gauge) else None

        )

        key = _ck(item_code, mc_group, _old_gauge_str)

        # old plan ใช้เป็น fallback เท่านั้น: ห้าม override baseline จาก detail_mc/booking

        # หา week ล่าสุดที่ ACTUAL_MC > 0 (ไม่ใช่แค่แถวแรก) และไม่ override ถ้า key มีข้อมูล booking จริงอยู่แล้ว

        if key not in booking_production_keys and (key not in last_production or w_idx > last_production[key]):

            last_production[key] = w_idx

            machines_in_use[key] = _old_actual_mc_int

            # Normalize SC/SO NO เล็กน้อย

            if sc_no and not pd.isna(sc_no):

                s = str(sc_no).strip().upper()

                if s.startswith("S") and s[1:].isdigit():

                    s = s[1:]

                # ตั้งค่าแค่ถ้ายังไม่มี

                if key not in last_sc_so_no:

                    last_sc_so_no[key] = s

    print(

        f"📋 เติม last_production จาก old_plan: {len([k for k in last_production])} รายการ (รวม)"

    )

# Merge booking history → last_production (booking ข้อมูลจริงแทนถ้า recent กว่า)

for _bk_key, _bk_widx in booking_last_production.items():

    if _bk_key not in last_production or _bk_widx > last_production[_bk_key]:

        last_production[_bk_key] = _bk_widx

        _raw_so = booking_last_so.get(_bk_key, "")

        # Normalize booking SO_NO: "S717492" → "717492" เพื่อให้ตรงกับ order SC/SO NO

        if _raw_so.startswith("S") and _raw_so[1:].isdigit():

            last_sc_so_no[_bk_key] = _raw_so[1:]

        else:

            last_sc_so_no[_bk_key] = _raw_so

    # เสมอ: ถ้ายังไม่มี last_sc_so_no ให้เซ็ตจาก booking (detail_mc ไม่มี SO info)

    if _bk_key not in last_sc_so_no or not last_sc_so_no.get(_bk_key):

        _raw_so = booking_last_so.get(_bk_key, "")

        if _raw_so.startswith("S") and _raw_so[1:].isdigit():

            last_sc_so_no[_bk_key] = _raw_so[1:]

        elif _raw_so:

            last_sc_so_no[_bk_key] = _raw_so

print(f"📚 last_production หลัง merge booking: {len(last_production)} รายการรวม")



# =========================

# TRACK WEEKLY JOB USAGE

# =========================

weekly_job_usage = {}  # {week: {mc_group: jobs_used}}

# Pre-populate weekly_job_usage จาก booking_final_ready25 (DETAIL sheet)
# ใช้ _IS_NEW_SETUP / _MC_INCREASE ที่ AVA_MC.py คำนวณถูกต้องแล้ว
# new_jobs ต่อ row:
#   _is_new_setup=True  → MC_USE_CEIL (ทุกเครื่องต้อง setup ใหม่)
#   _is_new_setup=False → _mc_increase (เฉพาะส่วนที่เพิ่ม)

if (
    not detail_mc.empty
    and "WEEK" in detail_mc.columns
    and "ITEM_CODE" in detail_mc.columns
    and "MC_USE_CEIL" in detail_mc.columns
    and "MC_GROUP" in detail_mc.columns
):
    _det = detail_mc.copy()
    _det["WEEK"] = pd.to_numeric(_det["WEEK"], errors="coerce")
    _det["MC_USE_CEIL"] = pd.to_numeric(_det["MC_USE_CEIL"], errors="coerce").fillna(0).astype(int)
    _det = _det.dropna(subset=["WEEK", "ITEM_CODE", "MC_GROUP"])
    _det["WEEK"] = _det["WEEK"].astype(int)
    _det["ITEM_CODE"] = _det["ITEM_CODE"].astype(str).str.strip().str.upper()
    _det["MC_GROUP"] = _det["MC_GROUP"].astype(str).str.strip().str.upper()

    _has_ava_flags = "_IS_NEW_SETUP" in _det.columns and "_MC_INCREASE" in _det.columns
    if _has_ava_flags:
        _det["_IS_NEW_SETUP"] = _det["_IS_NEW_SETUP"].fillna(True).astype(bool)
        _det["_MC_INCREASE"] = pd.to_numeric(_det["_MC_INCREASE"], errors="coerce").fillna(0).astype(int)
        print("✅ Pre-populate weekly_job_usage: ใช้ _IS_NEW_SETUP / _MC_INCREASE จาก AVA_MC")
    else:
        print("⚠️  _IS_NEW_SETUP ไม่อยู่ใน DETAIL — fallback: นับ MC_USE_CEIL ทั้งหมด (อาจ overcount)")

    _det_active = _det[_det["MC_USE_CEIL"] > 0].copy()
    # ตัดกลุ่มที่ไม่ใช่เครื่องทอ (IMPORT = ผ้านำเข้า) ออกก่อนนับ job — ไม่งั้นโควตา setup เพี้ยน
    _det_active = _det_active[~_det_active["MC_GROUP"].isin(_JOB_EXCLUDE_MC)]

    for _, _drow in _det_active.iterrows():
        _wk = int(_drow["WEEK"])
        _wk_idx = week_index(_wk)
        if _wk_idx is None or _wk_idx < TODAY_IDX:
            continue

        _mc_key = str(_drow["MC_GROUP"]).strip().upper()
        _mc_ceil = int(_drow["MC_USE_CEIL"])
        _item = str(_drow["ITEM_CODE"]).strip().upper()

        if _has_ava_flags:
            _is_new = bool(_drow["_IS_NEW_SETUP"])
            _mc_inc = int(_drow["_MC_INCREASE"])
            _new_jobs = _mc_ceil if _is_new else _mc_inc
        else:
            _new_jobs = _mc_ceil

        if _new_jobs > 0:
            if _wk not in weekly_job_usage:
                weekly_job_usage[_wk] = {}
            weekly_job_usage[_wk][_mc_key] = weekly_job_usage[_wk].get(_mc_key, 0) + _new_jobs
            # เครื่องที่ booking ตั้งใหม่สัปดาห์นี้กินโควตาตั้งเครื่องของ "ผ้านี้" ด้วย
            # (DETAIL 1 แถว = 1 บล็อก item×เครื่อง×เกจ×สัปดาห์ — ไม่มีแถวซ้ำ บวกตรงได้)
            _item_setup_charge(_item, _mc_key, _drow.get("GUAGE"), _wk, _new_jobs)

total_booked = sum(sum(v.values()) for v in weekly_job_usage.values())

print(

    f"📋 Pre-loaded weekly_job_usage จาก booking_final_ready25 DETAIL"

    f" (new setup + เพิ่มเครื่อง, week>={TODAY_WEEK}): {total_booked} jobs"

)

# REPLAN: งานในแผนที่ผู้ใช้กดล็อกไว้ (Lock_Plan SOURCE=PLAN) ไม่ได้อยู่ในชีท DETAIL เพราะมัน
# มาจากไฟล์แผน ไม่ใช่ booking — เครื่องของมันถูกกันไว้แล้วใน SUMMARY_MC_REMAIN (Replan.pinned_mc_use)
# แต่โควตา job/สัปดาห์เป็นคนละบัญชี ถ้าไม่บวกตรงนี้ รอบ replan จะเห็น slot ว่างเกินจริง
# แล้วแจก setup เกิน cap เท่ากับจำนวน setup ของงานที่ตรึง → Replan ส่งมาทางชีท PINNED_JOBS
# (ยุบด้วย max ต่อ item/เครื่อง/เกจ มาแล้วฝั่ง Replan — ที่นี่บวกตรง ๆ ได้เลย)

if REPLAN_MODE:
    try:
        _pin_jobs = pd.read_excel(MC_REMAIN_FILE, sheet_name="PINNED_JOBS")
    except (ValueError, KeyError):
        _pin_jobs = pd.DataFrame()          # ไฟล์จาก Replan รุ่นเก่าไม่มีชีทนี้
    if not _pin_jobs.empty:
        _pin_jobs.columns = _pin_jobs.columns.str.strip().str.upper()
        _pin_added, _pin_skip = 0, 0
        for _, _pr in _pin_jobs.iterrows():
            _pwk = pd.to_numeric(_pr.get("WEEK"), errors="coerce")
            _pjb = pd.to_numeric(_pr.get("JOBS"), errors="coerce")
            _pmc = str(_pr.get("MC_GROUP", "")).strip().upper()
            if pd.isna(_pwk) or pd.isna(_pjb) or int(_pjb) <= 0 or not _pmc:
                continue
            if _pmc in _JOB_EXCLUDE_MC:
                continue
            _pwk = _TL.key(int(_pwk), _pr.get("YEAR"))  # → YW (มีคอลัมน์ YEAR ใช้ปีนั้น)
            _pidx = week_index(_pwk)
            if _pidx is None or _pidx < TODAY_IDX:
                _pin_skip += int(_pjb)      # สัปดาห์ที่ผ่านไปแล้ว — ไม่มีโควตาให้กินอีก
                continue
            weekly_job_usage.setdefault(_pwk, {})
            weekly_job_usage[_pwk][_pmc] = weekly_job_usage[_pwk].get(_pmc, 0) + int(_pjb)
            _pin_added += int(_pjb)
        print(f"📌 REPLAN: หักโควตา job ของงานที่ตรึงไว้ {_pin_added} jobs "
              f"จาก {len(_pin_jobs)} แถวชีท PINNED_JOBS"
              + (f" (ข้าม {_pin_skip} jobs ที่อยู่ก่อนสัปดาห์ปัจจุบัน)" if _pin_skip else ""))

    # งานที่ตรึงไว้ตั้งเครื่องใหม่สัปดาห์ไหน → กินโควตาตั้งเครื่องของผ้านั้นด้วย (แยกรายผ้า)
    # ชีท PINNED_JOBS ข้างบนยุบเป็นสัปดาห์×กลุ่มเครื่องแล้ว ไม่รู้ว่าเป็นผ้าไหน → Replan ส่งชีทนี้มาแยก
    # (ไฟล์จาก Replan รุ่นเก่าไม่มีชีทนี้ = ไม่หัก เหมือนพฤติกรรมเดิม)
    try:
        _pin_item = pd.read_excel(MC_REMAIN_FILE, sheet_name="PINNED_JOBS_ITEM")
    except (ValueError, KeyError):
        _pin_item = pd.DataFrame()
    if not _pin_item.empty:
        _pin_item.columns = _pin_item.columns.str.strip().str.upper()
        _pin_item_n = 0
        for _, _pr in _pin_item.iterrows():
            _pwk = pd.to_numeric(_pr.get("WEEK"), errors="coerce")
            _pjb = pd.to_numeric(_pr.get("JOBS"), errors="coerce")
            _pmc = str(_pr.get("MC_GROUP", "")).strip().upper()
            _pit = str(_pr.get("ITEM_CODE", "")).strip().upper()
            if pd.isna(_pwk) or pd.isna(_pjb) or int(_pjb) <= 0 or not _pmc or not _pit:
                continue
            if _pmc in _JOB_EXCLUDE_MC:
                continue
            _pwk = _TL.key(int(_pwk), _pr.get("YEAR"))
            _pidx = week_index(_pwk)
            if _pidx is None or _pidx < TODAY_IDX:
                continue
            _item_setup_charge(_pit, _pmc, _pr.get("GUAGE"), _pwk, int(_pjb))
            _pin_item_n += int(_pjb)
        print(f"📌 REPLAN: หักโควตาตั้งเครื่องรายผ้าของงานที่ตรึงไว้ {_pin_item_n} เครื่อง "
              f"จาก {len(_pin_item)} แถวชีท PINNED_JOBS_ITEM")

print(f"🧵 โควตาตั้งเครื่องรายผ้าที่ booking/งานตรึงใช้ไปแล้ว: "
      f"{sum(_item_new_mc_week.values())} เครื่อง ใน {len(_item_new_mc_week)} บล็อก "
      f"(ผ้า×เครื่อง×เกจ×สัปดาห์)" + ("" if SETUP_QUOTA_PER_ITEM else " — ปิดสวิตช์อยู่ ไม่หัก"))

# Snapshot ค่า OLD ก่อนเริ่ม loop ใหม่ (deep copy)

weekly_job_usage_old = {wk: dict(mc_dict) for wk, mc_dict in weekly_job_usage.items()}

# weekly_new_plan_usage: เฉพาะงานที่วางแผนใหม่ในรอบนี้ (ใช้กับ get_actual_mc_remain)

# แยกจาก weekly_job_usage ที่รวม booking เก่าด้วย (TOTAL_MC_REMAIN หักเก่าไปแล้ว)


# =========================
# ชีท UNPLANNED — คอลัมน์ชุดเดิม + DATE_IN เท่านั้น (ไม่เอาทุกคอลัมน์ของ order)
# =========================
UNPLANNED_COLS = ["SC_SO_NO", "ITEM_CODE", "ORDER_TYPE", "MC_GROUP", "GAUGE", "ORDERS_QTY",
                  "PENDING_PLAN", "FG_WEEK", "DATE_IN", "UNPLANNED_QTY", "REASON"]


def _unplanned_row(order, reason, qty=None, gauge=None) -> dict:
    """สร้างแถวชีท UNPLANNED จาก order (pandas Series) — ดึงเฉพาะคอลัมน์ใน UNPLANNED_COLS

    qty   = จำนวนที่ยังไม่ได้วาง (ไม่ส่ง = ใช้ Pending Plan ทั้งก้อน)
    gauge = เกจที่ต้องการระบุเอง (ไม่ส่ง = ใช้ MC_GUAGE ของ order)
    """
    _pend = pd.to_numeric(order.get("Pending Plan", 0), errors="coerce")
    _pend = 0.0 if pd.isna(_pend) else float(_pend)
    # 'MC GROUP' (เว้นวรรค) เป็นคอลัมน์ที่ Planning สร้างเพิ่มให้ orders เท่านั้น
    # แถวที่มาจาก order_excluded.xlsx เป็นข้อมูลดิบ มีแค่ 'MC_GROUP' (ขีดล่าง) → ต้อง fallback
    _mc = order.get("MC GROUP", "")
    if pd.isna(_mc) or str(_mc).strip().lower() in ("", "nan", "none"):
        _mc = order.get("MC_GROUP", "")
    if pd.isna(_mc):
        _mc = ""
    # ORDER_TYPE: orders ที่ผ่าน column_mapping จะเป็น 'Orders Type'
    # (order_excluded.xlsx ก็ rename ชุดเดียวกัน) — เผื่อ fallback ชื่อดิบไว้ด้วย
    _otype = order.get("Orders Type", "")
    if pd.isna(_otype) or str(_otype).strip().lower() in ("", "nan", "none"):
        _otype = order.get("ORDER_TYPE", "")
    if pd.isna(_otype):
        _otype = ""
    return {
        "SC_SO_NO": str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip(),
        "ITEM_CODE": order.get("Item Code", ""),
        "ORDER_TYPE": str(_otype).strip(),
        "MC_GROUP": str(_mc).strip().upper(),
        "GAUGE": order.get("MC_GUAGE", "") if gauge is None else gauge,
        "ORDERS_QTY": order.get("Orders.Qty", ""),
        "PENDING_PLAN": _pend,
        "FG_WEEK": order.get("FG Week", ""),
        "DATE_IN": order.get("DATE_IN", ""),
        "UNPLANNED_QTY": _pend if qty is None else qty,
        "REASON": reason,
    }


def _is_blank_order_row(item, qty) -> bool:
    """แถวว่างที่ไม่ใช่ใบสั่งจริง = ไม่มีรหัส item และยอดค้าง <= 0 (หรืออ่านเป็นตัวเลขไม่ได้)"""
    _it = "" if item is None or (isinstance(item, float) and pd.isna(item)) else str(item).strip().upper()
    if _it not in ("", "NAN", "NONE"):
        return False
    _q = pd.to_numeric(qty, errors="coerce")
    return pd.isna(_q) or float(_q) <= 0


def _calc_first_time_mc_use(candidate_mc, current_week_num, mc_usage) -> bool:
    """MC_GROUP นี้เพิ่งถูกใช้ครั้งแรกในสัปดาห์นี้หรือไม่ (ยังไม่เคยใช้ในสัปดาห์ก่อนหน้า)

    แยกออกมาเป็นฟังก์ชันเพราะทั้งสาย Load Balancing และสาย mc_group=None (งาน S9)
    ต้องใช้ค่านี้ — เดิมคำนวณไว้ในสาย LB สายเดียว อีกสายเลยอ่านค่าค้างจาก order ก่อนหน้า
    (หรือ UnboundLocalError ถ้าสาย LB ยังไม่เคยรันเลยในรอบนั้น)
    """
    for (week_idx, mc_grp), _total_machines in mc_usage.items():
        try:
            week_num = int(week_idx) if isinstance(week_idx, str) else week_idx
            if candidate_mc and mc_grp != candidate_mc:
                continue
            if week_num < current_week_num:
                return False  # มีการใช้ MC_GROUP นี้มาก่อนในสัปดาห์ที่ผ่านมา
        except (ValueError, TypeError):
            continue
    return True


# =============================================================================
# งานสี "ยึดเครื่องคืน" ได้ตั้งแต่รอบวางแผนหลัก (user เคาะ 2 ก.ย. 2569)
# =============================================================================
# เดิมสิทธิ์ยึดเครื่องอยู่แต่ในรอบซ่อมท้ายแผน (_repair_color_preempt) รอบหลักจึงเห็นแค่
# "พูลว่าง" ซึ่งมักเป็น 0 เพราะงานที่ครบกำหนดก่อนจองไปหมดแล้ว → งานสีที่ TARGET ไกล
# ได้เครื่องแค่ตัวเดียว ทรงออกมาแบนยาว แล้วรอบซ่อมค่อยมาปะเป็นหย่อม ๆ
# (เคสจริง FD4DRTRN74/03A0 SC 719405: ทรง 3-1-1-1-1-1-1-2 · setup 10 วันที่ W38 ทิ้งเปล่า
#  เพราะสัปดาห์ถัดไปพูลว่าง 0 → เครื่องที่เพิ่งตั้งหลุดทันที)
#
# user สั่ง: "ทรงควรถูกตั้งแต่รอบแรก ไม่ต้องมีด่านซ่อมมาปะ" → ย้ายสิทธิ์เข้ารอบหลัก 2 จุด
#   1. ตอนคำนวณทรง/จำนวนเครื่อง: avail ของก้อนสี = พูลว่าง + เครื่องที่ยึดคืนได้
#      (_avail_mc_for_order → ใช้ใน calculate_required_machines / progressive / backward)
#   2. ก่อนวางแต่ละสัปดาห์: ยึดจริงเท่าที่ทรงต้องการ (_main_color_grab)
#
# ใช้เครื่องมือชุดเดียวกับรอบซ่อม (คืนพูล / ไหลยอดไปข้างหน้า / ตัด carry ต่อเนื่อง) ซึ่งนิยาม
# ไว้ท้ายไฟล์ — เรียกได้เพราะลูปหลักถูกรัน "หลัง" โหลดโมดูลจบ (plans = _run_planning_loop())
MAIN_COLOR_GRAB_ENABLED = plan_config.get("KP_MAIN_COLOR_GRAB")

# เครื่องที่ตั้งใหม่ต้องมี "ที่ยืน" ในสัปดาห์ถัดไป (พูลว่างพอให้ carry ต่อ) ไม่งั้นลดลง
# — user เคาะ 2 ก.ย. 2569 "เพิ่มเครื่องขึ้นไปต้องใช้ 2 week" ; ปิดได้ที่หน้า "ตั้งค่าระบบ"
NEW_MC_MUST_STAND = plan_config.get("KP_NEW_MC_STAND")

_MAIN_GRAB_POOL_INDEX: dict = {}      # (mc, gauge, week_idx) → [แถวแผนที่วางแล้ว]
_MAIN_GRAB_CHAIN_INDEX: dict = {}     # (item, mc, gauge) → {week_idx: [แถวแผน]}
_MAIN_GRAB_INDEX_N = 0                # เติม index ถึงแถวที่เท่าไรแล้ว (เติมต่อ ไม่สแกนซ้ำ)
_CUR_ORDER_COLOR_PART = False         # ออร์เดอร์ที่กำลังวางเป็น "ยอดที่เปิดสีแล้ว" ไหม

# ยึดเครื่องแบบ "จองชั่วคราว" แล้วเก็บไว้เท่าที่แถวใช้จริง (ดู _grab_txn_settle) ; ปิดได้ที่หน้า "ตั้งค่าระบบ"
MAIN_GRAB_SETTLE_ENABLED = plan_config.get("KP_GRAB_SETTLE")
_GRAB_TXN = None                      # สมุดจดการยึดของรอบสัปดาห์ที่กำลังวาง (None = ไม่จด)


def _grab_num(row, key) -> float:
    return float(pd.to_numeric(row.get(key, 0), errors="coerce") or 0)


# ---------- เครื่อง booking ที่ "ใบคงแผนเดิม" ถือไว้ (โหมด replan) ----------
# แถวที่ตรึง 🔐 / core ที่เบรกไว้ / ใบที่ออกเลข KP 🎫 ถูกแปะกลับเข้าแผน "หลัง" ลูปวางแผนจบ
# (Replan.py เตรียมไว้ใน replan_append.xlsx) → ระหว่างวาง Planning มองไม่เห็นแถวพวกนี้
# ทั้งที่เครื่อง booking ของมันถูกจองไว้จริงและยังวิ่งงานอยู่
# ถ้าปล่อยให้งานสียึดเครื่องชุดนั้นแล้วคืนเข้าพูล (_release_booking_mc) จะได้ "เครื่องผี"
# = พูลว่างเพิ่มขึ้นทั้งที่เครื่องไม่ว่าง → แผนวางเกินเครื่องที่มีจริง
# เคสจริง 29 ก.ย. 2569: W202642 PHET|DOUBLE-30:19 ว่าง 7 แต่แผนใช้ 8 — งานสี FD5BASPG10A0
# ยึดเครื่องจาก FD5BASJT17A0 RAO G19 ซึ่งเป็นเครื่อง booking ของ KP2642CK0034XXXNA
_KEEP_BOOKING_MC_MAP: dict = {}
_KEEP_BOOKING_MC_READY = False


def _load_keep_booking_mc() -> None:
    """อ่าน replan_append.xlsx → {(สัปดาห์ YW, item, กลุ่มเครื่อง, เกจ): เครื่อง booking ที่ใบคงแผนเดิมถือ}

    MC_BOOKING ถูกแปะซ้ำทุกแถวของเครื่องชุดเดียวกัน → ยุบด้วย max ต่อบล็อก
    (สูตรเดียวกับ _pool_blocks_from_plan ที่หัก MC_BOOKING ด้วย max ไม่ใช่ sum)
    """
    global _KEEP_BOOKING_MC_READY
    _KEEP_BOOKING_MC_READY = True
    if not (REPLAN_MODE and REPLAN_APPEND_FILE and Path(REPLAN_APPEND_FILE).exists()):
        return
    try:
        _x = pd.ExcelFile(REPLAN_APPEND_FILE)
        for _sh in ("PINNED", "CORE", "KP"):
            if _sh not in _x.sheet_names:
                continue
            _d = _x.parse(_sh)
            if _d.empty:
                continue
            _d.columns = _d.columns.astype(str).str.strip().str.upper()
            if "MC_BOOKING" not in _d.columns:
                continue
            _weeks_to_yw(_d, "PLAN_WEEK", "PLAN_YEAR")
            for _r in _d.to_dict("records"):
                _bk = float(pd.to_numeric(_r.get("MC_BOOKING", 0), errors="coerce") or 0)
                _w = pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce")
                if _bk <= 0 or pd.isna(_w):
                    continue
                _k = (
                    int(_w),
                    str(_r.get("ITEM_CODE", "")).strip().upper(),
                    str(_r.get("MC_GROUP", "")).strip().upper(),
                    _normalize_gauge(_r.get("MC_GUAGE")),
                )
                _KEEP_BOOKING_MC_MAP[_k] = max(_KEEP_BOOKING_MC_MAP.get(_k, 0), int(_bk))
    except Exception as _e_kb:
        print(f"⚠️ อ่านเครื่อง booking ของใบที่คงแผนเดิมไม่ได้ ({_e_kb}) "
              f"→ งานสีอาจยึดเครื่องของใบที่ออก KP แล้ววางเกินพูล")
        return
    if _KEEP_BOOKING_MC_MAP:
        print(f"🔒 เครื่อง booking ของใบที่คงแผนเดิม (ยึด/คืนเข้าพูลไม่ได้): "
              f"{len(_KEEP_BOOKING_MC_MAP)} บล็อก รวม {sum(_KEEP_BOOKING_MC_MAP.values())} เครื่อง-สัปดาห์")


def _keep_booking_mc(week, item, mc_group, gauge) -> int:
    """เครื่อง booking ของบล็อกนี้ที่ใบคงแผนเดิมถือไว้ (0 = ไม่มี / ไม่ใช่โหมด replan)"""
    if not _KEEP_BOOKING_MC_READY:
        _load_keep_booking_mc()
    if not _KEEP_BOOKING_MC_MAP:
        return 0
    _w = pd.to_numeric(week, errors="coerce")
    if pd.isna(_w):
        return 0
    return int(_KEEP_BOOKING_MC_MAP.get((
        int(_w),
        str(item).strip().upper(),
        str(mc_group).strip().upper(),
        _normalize_gauge(gauge),
    ), 0))


def _main_grab_indexes():
    """index แถวแผนที่วางไปแล้ว — เติมทีละก้อนท้าย list (ห้ามสแกนกองเดิมซ้ำ ไม่งั้นช้ามาก)"""
    global _MAIN_GRAB_INDEX_N
    _rows = _PLANS_REF
    if _MAIN_GRAB_INDEX_N > len(_rows):        # plans ถูกสร้างใหม่ (รันรอบใหม่) → ล้าง index
        _MAIN_GRAB_POOL_INDEX.clear()
        _MAIN_GRAB_CHAIN_INDEX.clear()
        _MAIN_GRAB_INDEX_N = 0
    for _r in _rows[_MAIN_GRAB_INDEX_N:]:
        _k = _preempt_row_key(_r)
        _i = _preempt_row_idx(_r)
        if not _k[1] or _i is None:
            continue
        _MAIN_GRAB_POOL_INDEX.setdefault((_k[1], _k[2], _i), []).append(_r)
        _MAIN_GRAB_CHAIN_INDEX.setdefault(_k, {}).setdefault(_i, []).append(_r)
    _MAIN_GRAB_INDEX_N = len(_rows)
    return _MAIN_GRAB_POOL_INDEX, _MAIN_GRAB_CHAIN_INDEX


def _main_row_is_color(row) -> bool:
    """แถวที่วางแล้วเป็น "ยอดที่เปิดสีแล้ว" ไหม — เชื่อธง COLOR_PART ที่แจกตอนจัดคิว

    (รอบซ่อมใช้ _build_color_budget ซึ่งต้องเห็นแผนทั้งกองก่อน — ระหว่างวางยังไม่มี
    จึงใช้ธงรายแถวที่ _split_orders_by_color_quota แจกไว้ตั้งแต่ต้นรอบแทน)

    ผ้า core ไม่ใช่งานสีเสมอ (user 16 ก.ย. 2569) — ต้องกันไว้ก่อนถึง fallback ข้างล่าง
    ไม่งั้น core ของ item ที่มีคนอื่นเปิดสีไว้จะถูกนับเป็นงานสีทางอ้อม

    แถวไม่มีธง (booking / แผนเก่า / ใบที่ไม่มีโควตาสีของตัวเอง) → ดูโควตาสี "ของใบนั้น"
    ไม่ใช่ดูแค่ว่า item มีคนเปิดสีไว้บ้างไหม (กติกางานสีคิดต่อใบสั่ง — user 27 ส.ค. 2569)
    ไม่งั้นใบที่ยังไม่เปิดสีเลยของผ้าที่ใบอื่นเปิดสีไว้ จะได้เกราะกันโดนยึดเครื่องของงานสี
    ทั้งที่หน้าเว็บแสดงว่า "ยังไม่ให้สี" ; แถวที่ไม่มีช่อง SC เลยยังถอยไปใช้ยอดรวมทุกใบ
    ของ item เหมือนเดิม (_color_quota_of คืน inf เมื่อไม่รู้ใบ)
    """
    if _row_is_core(row):
        return False
    _f = str(row.get("COLOR_PART", "")).strip().lower()
    if _f in ("yes", "true"):
        return True
    if _f in ("no", "false"):
        return False
    return _color_quota_of(row.get("ITEM_CODE"), _row_sc(row)) > 0


def _hold_extra_mc(row) -> int:
    """เครื่องที่แถว "ก้อนสี" ถือเกินยอดของตัวเอง เพราะกันไว้ให้ก้อนพี่น้องที่ยังไม่ให้สี

    ธง MC_HOLD_SPLIT ติดมาจากลูปวางแผน (ก้อนสีที่ถูกแตกตามโควตาสี แล้วด่าน SPLIT HOLD
    ห้ามลดเครื่อง) — เครื่องส่วนนี้ต้องยึดคืนได้ ไม่งั้น "การถือเครื่องเผื่อพี่น้อง" จะกลายเป็น
    เกราะกันงานสีของ item อื่น = ผิดกติกางานสีมาก่อน
    ยอดของแถวเองไม่หายไปเมื่อถูกยึด เพราะเครื่องที่เหลือยังผลิตครบ (ดู _keep_qty ใน grab)
    """
    if str(row.get("MC_HOLD_SPLIT", "")).strip().lower() not in ("yes", "true"):
        return 0
    _tot = int(_grab_num(row, "ACTUAL_MC"))
    _days = _grab_num(row, "AVAILABLE_DAYS")
    _daily = _daily_of_row(row)
    if _tot <= 1 or _days <= 0 or _daily <= 0:
        return 0
    _need = max(1, int(math.ceil(_grab_num(row, "PRODUCE_QTY") / (_days * _daily))))
    return max(0, _tot - _need)


def _grab_cap_mc(row) -> int:
    """ยึดเครื่องจากแถวนี้ได้มากสุดกี่ตัว — แถวสียึดได้เฉพาะส่วนที่กันไว้ให้ก้อนไม่มีสี

    เครื่อง booking ที่ "ใบคงแผนเดิม" (ตรึง/core ที่เบรก/ใบที่ออก KP) ถือไว้ ยึดไม่ได้เลย —
    แถวพวกนั้นถูกแปะกลับเข้าแผนหลังลูปจบ เครื่องจึงยังวิ่งงานอยู่จริง (ดู _keep_booking_mc)
    """
    _cap = _hold_extra_mc(row) if _main_row_is_color(row) else int(_grab_num(row, "ACTUAL_MC"))
    _keep_bk = _keep_booking_mc(
        row.get("PLAN_WEEK"), row.get("ITEM_CODE"), row.get("MC_GROUP"), row.get("MC_GUAGE")
    )
    if _keep_bk > 0:
        _cap = min(_cap, int(_grab_num(row, "ACTUAL_MC")) - _keep_bk)
    return max(0, _cap)


def _main_grab_victims(item, mc_group, gauge, week_idx):
    """แถวที่ยึดเครื่องคืนได้ในสัปดาห์นั้น เรียงตามลำดับที่ควรโดนยึดก่อน

    กติกาเดียวกับรอบซ่อม: ยึดจากงานที่ยังไม่ให้สีเท่านั้น · ห้ามยึดจาก core (กติกายืมแล้วคืน
    ถูกลบไปเมื่อ 2 ก.ย. ; core ที่จองแล้วถูกล็อก AUTO-CORE ด้วย) · เรียงงานที่ "สายอยู่แล้ว"
    และ "ไม่ได้ใช้เครื่อง booking" ก่อน
    ข้อยกเว้นเดียวของ "ยึดจากงานไม่มีสีเท่านั้น" = เครื่องที่แถวสีถือเผื่อก้อนพี่น้องไว้
    (MC_HOLD_SPLIT → _hold_extra_mc) ซึ่งเนื้องานส่วนนั้นคือยอดที่ยังไม่ให้สีอยู่แล้ว
    """
    _pool_idx, _ = _main_grab_indexes()
    _rows = _pool_idx.get(
        (str(mc_group).strip().upper(), _normalize_gauge(gauge), int(week_idx)), []
    )
    _it_u = str(item).strip().upper()
    _out = []
    for _r in _rows:
        if str(_r.get("ITEM_CODE", "")).strip().upper() == _it_u:
            continue                       # ของตัวเอง
        if int(_grab_num(_r, "ACTUAL_MC")) <= 0:
            continue
        if _row_is_core(_r):
            continue
        # แถวกันเครื่อง (Hold รอสี / Hold รอ target) — เครื่องชุดนั้นจอดอุ่นรอเจ้าของกลับมาทอต่อ
        # ยึดไปแล้วเจ้าของต้อง setup ใหม่ทั้งชุด = เสียมากกว่าได้ (Hold รอสีรอดมาได้เพราะ item
        # ของมันเป็น item สีเสมอ แต่ Hold รอ target ของ core ที่ไม่มีสีจะหลุดตะแกรงนั้น)
        if _is_hold_row(_r):
            continue
        if _main_row_is_color(_r) and _hold_extra_mc(_r) <= 0:
            continue
        if str(_r.get("S9_ROUTING", "")).strip().lower() in ("true", "yes"):
            continue                       # จ้างทอข้างนอก ไม่ใช่เครื่องในพูล
        if str(_r.get("OUTSOURCE", "")).strip().lower() == "yes":
            continue
        _tk = _rank_target_knit(_r)   # แถว HOLD = NaN (ตรึงลำดับเดิม ดู _rank_target_knit)
        _tk_idx = None if pd.isna(_tk) else week_pos(int(_tk))
        _late = 1 if (_tk_idx is not None and int(week_idx) > _tk_idx) else 0
        _bk = 1 if int(_grab_num(_r, "MC_BOOKING")) > 0 else 0
        # เครื่องที่เหยื่อ "ยังไม่ได้ตั้ง" (NEW_MC) โดนก่อน — ปลดแล้วได้คืนทั้งเครื่องและ
        # ช่องโควตา setup ของสัปดาห์นั้น และไม่ทิ้งค่า setup ที่จ่ายไปแล้วเหมือนการแตะ carry
        _has_new = 1 if int(_grab_num(_r, "NEW_MC")) > 0 else 0
        _out.append((_bk, -_has_new, -_late, -(_tk_idx if _tk_idx is not None else 10 ** 6), _r))
    _out.sort(key=lambda t: (t[0], t[1], t[2], t[3]))
    return [_t[4] for _t in _out]


def _main_takeable_mc(item, mc_group, gauge, week_idx) -> int:
    """เครื่องที่ก้อนสีนี้ "ยึดคืนได้" ในสัปดาห์นั้น (ยังไม่ยึดจริง — ใช้ตอนคำนวณทรง)"""
    if not MAIN_COLOR_GRAB_ENABLED or week_idx is None:
        return 0
    _n = sum(
        _grab_cap_mc(_r)
        for _r in _main_grab_victims(item, mc_group, gauge, week_idx)
    )
    if _n <= 0 or week_idx >= len(calendar_week):
        return max(0, _n)
    # 🔒 พูลย่อย POLY/COTTON (ชีท MC Special) เป็นโควตาแยก — ยึดเครื่องของงานทั่วไป
    # มาใช้ไม่ได้ (กติกาเดียวกับรอบซ่อม ดูหมายเหตุใน _repair_color_preempt)
    _wk = int(calendar_week.iloc[int(week_idx)]["WEEK"])
    _sp = _mc_special_remain(str(mc_group).strip().upper(), _wk, _normalize_gauge(gauge), item)
    if _sp is not None:
        _n = min(_n, max(0, int(_sp)))
    return max(0, _n)


def _avail_mc_for_order(mc_group, week, gauge, item_code) -> int:
    """เครื่องที่ "ก้อนงานที่กำลังวาง" ใช้ได้จริงในสัปดาห์นั้น

    ก้อนสี  = พูลว่าง + เครื่องที่ยึดคืนได้จากงานที่ยังไม่ให้สี (สิทธิ์เดิมของรอบซ่อม)
    ก้อนอื่น = พูลว่างอย่างเดียว (เหมือนเดิมทุกประการ)
    """
    _free = int(get_actual_mc_remain(mc_group, week, gauge=gauge, item_code=item_code) or 0)
    if not (MAIN_COLOR_GRAB_ENABLED and _CUR_ORDER_COLOR_PART):
        return _free
    _wi = week_pos(week)
    if _wi is None:
        return _free
    return _free + _main_takeable_mc(item_code, mc_group, gauge, _wi)


def _main_color_grab_apply(item, mc_group, gauge, week_idx, need_mc, tk_week=None) -> int:
    """ยึดเครื่องคืนจากงานที่ยังไม่ให้สี แล้วคืนเข้าพูลให้ลูปหลักหยิบไปใช้ — คืนจำนวนที่ยึดได้

    ทำ "ระหว่างวาง" (เส้นทางเดียวกับ _repair_color_preempt แต่ไม่รอจบรอบ):
      ลดเครื่องของแถวเหยื่อ → ยอดที่ผลิตไม่ได้ไหลไปสัปดาห์ถัดไปของใบเดียวกัน →
      คืนเครื่อง/โควตา job เข้าบัญชีพูล → ลูปหลักเห็นเครื่องว่างเพิ่มทันทีในบรรทัดถัดไป
    """
    if not MAIN_COLOR_GRAB_ENABLED or need_mc <= 0 or week_idx is None:
        return 0
    if week_idx < 0 or week_idx >= len(calendar_week):
        return 0
    _wk = int(calendar_week.iloc[int(week_idx)]["WEEK"])
    _mc = str(mc_group).strip().upper()
    _g = _normalize_gauge(gauge)
    _wd = float(get_working_days_by_factory(_mc, 1, week=_wk, gauge=_g) or 0)
    if _wd <= 0:
        return 0
    # โควตาพูลย่อย: ยึดได้ไม่เกินที่กันไว้ให้ชนิดของ item นี้
    _sp_left = _mc_special_remain(_mc, _wk, _g, item)
    if _sp_left is not None:
        need_mc = min(need_mc, max(0, int(_sp_left)))
        if need_mc <= 0:
            return 0

    _, _chain_idx = _main_grab_indexes()
    _got = 0
    for _vr in _main_grab_victims(item, _mc, _g, week_idx):
        if _got >= need_mc:
            break
        _v_item = str(_vr.get("ITEM_CODE", "")).strip()
        _v_total = int(_grab_num(_vr, "ACTUAL_MC"))
        _v_carry = int(_grab_num(_vr, "CARRYOVER_MC"))
        _v_new = int(_grab_num(_vr, "NEW_MC"))
        _v_qty = _grab_num(_vr, "PRODUCE_QTY")
        # แถวสีที่ถือเครื่องเผื่อก้อนพี่น้อง → ยึดได้แค่ส่วนเกินของมัน (ไม่แตะเครื่องที่งานสีใช้จริง)
        _cut = min(_grab_cap_mc(_vr), need_mc - _got)
        if _cut <= 0:
            continue
        _v_days = _grab_num(_vr, "AVAILABLE_DAYS") or _wd
        _v_daily = _daily_of_row(_vr)
        _rev_v = _grab_num(_vr, "REVOLUTION_WEIGHT")
        _keep_mc = _v_total - _cut

        # ปลดเครื่องที่ "ยังไม่ได้ตั้ง" (NEW) ก่อน แล้วค่อยแตะ carry ที่วิ่งอยู่จริง
        # ⚠️ เดิมเขียนกลับด้าน (เก็บ NEW ไว้ก่อน) → เหยื่อที่มี carry 1 + new 1 โดนยึด 1 เหลือ new 1
        #   ต้อง setup ใหม่ทั้งที่เครื่องสัปดาห์ก่อนยังอุ่น (เคสจริง 14 ก.ย. 2569 FD5GNTLM70/12A0 RAOO W41)
        _carry_keep = max(0, min(_v_carry, _keep_mc))
        _new_keep = max(0, min(_v_new, _keep_mc - _carry_keep))
        # AVAILABLE_DAYS ของแถวที่มีเครื่องใหม่ = วันทอหลังหัก setup → เครื่อง carry ที่เหลือทอได้เต็มสัปดาห์
        _wd_full = _grab_num(_vr, "ACTUAL_WORKING_DAYS") or _wd
        _days_new = _v_days if _v_new > 0 else _wd_full
        _days_carry = _v_days if _v_new <= 0 else _wd_full
        _cap_keep = _v_daily * (_carry_keep * _days_carry + _new_keep * _days_new)
        _keep_qty = fold_keep_qty(_v_qty, _cap_keep, _rev_v)
        _spill = max(0.0, _v_qty - _keep_qty)
        # ⚠️ เครื่องชุดนี้ไม่ได้ทอให้แถวเหยื่อแถวเดียว — ใบอื่นของ item เดียวกันที่ "ขี่เครื่อง
        # ชุดเดียวกัน" (ACTUAL_MC = 0 + MC_SHARED) ทอยอดของตัวเองอยู่บนเครื่องชุดนี้ด้วย
        # เดิมด่านนี้คิดแค่ยอดของแถวเหยื่อ → ยึดเครื่องไปแล้วยอดของใบที่ขี่อยู่ไม่ถูกลด
        # กลายเป็นทอเกินกำลังเครื่องที่เหลือ (เคสจริง 21 ก.ย. 2569 FD4DRTRN74/03A0 RAP W43:
        # 3 เครื่องทอได้ 2,439 kg แต่แผนวางไว้ 3,251 kg)
        _riders = [
            _r2 for _r2 in _chain_idx.get(_preempt_row_key(_vr), {}).get(int(week_idx), [])
            if _r2 is not _vr
            and int(_grab_num(_r2, "ACTUAL_MC")) <= 0
            and _grab_num(_r2, "PRODUCE_QTY") > 0
        ]
        _rider_room = max(0.0, _cap_keep - _keep_qty)
        _rider_spills = []                 # (แถว, ยอดที่ต้องไล่ไปวางต่อ)
        for _rd in _riders:
            _rd_qty = _grab_num(_rd, "PRODUCE_QTY")
            _rd_keep = fold_keep_qty(_rd_qty, _rider_room, _grab_num(_rd, "REVOLUTION_WEIGHT") or _rev_v)
            _rd_spill = max(0.0, _rd_qty - _rd_keep)
            _rider_room = max(0.0, _rider_room - _rd_keep)
            _rd["MC_SHARED"] = _keep_mc
            if _rd_spill > 1e-6:
                _rd["PRODUCE_QTY"] = _rd_keep
                _rd["PLAN_QTY"] = max(0.0, _grab_num(_rd, "PLAN_QTY") + _rd_spill)
                _rider_spills.append((_rd, _rd_spill))
        _v_bk = int(_grab_num(_vr, "MC_BOOKING"))
        _bk_cut = min(_v_bk, max(0, _v_carry - _carry_keep))
        # เครื่อง booking ที่ใบคงแผนเดิมถือไว้ ห้ามคืนเข้าพูล — เครื่องยังทอใบนั้นอยู่จริง
        # (ชั้นที่ 2 ต่อจาก _grab_cap_mc: บล็อกที่มีเครื่องมากกว่าที่ใบคงแผนเดิมถือ ยังยึดได้บางส่วน
        #  แต่ส่วนที่ปลดต้องไม่ไปกินโควตา booking ของใบนั้น)
        _v_keep_bk = _keep_booking_mc(_wk, _v_item, _mc, _g)
        if _bk_cut > 0 and _v_keep_bk > 0:
            _bk_cut_ok = min(_bk_cut, max(0, _v_bk - _v_keep_bk))
            if _bk_cut_ok < _bk_cut:
                print(
                    f"   🔒 [KEEP BOOKING MC] {_v_item} {_mc} G{_g} W{_wk}: ไม่คืนเครื่อง booking "
                    f"{_bk_cut - _bk_cut_ok} ตัวเข้าพูล (ใบที่คงแผนเดิมยังใช้เครื่องชุดนี้อยู่)"
                )
            _bk_cut = _bk_cut_ok
        _v_setup = get_setup_days_for_item(
            str(_vr.get("MATERIAL_CONTENT", "") or ""),
            str(_vr.get("YARN_USED", "") or ""),
            _mc, _v_item,
        )
        _vr["ACTUAL_MC"] = _keep_mc
        _vr["CARRYOVER_MC"] = _carry_keep
        _vr["NEW_MC"] = _new_keep
        _vr["PRODUCE_QTY"] = _keep_qty
        _vr["PLAN_QTY"] = max(0.0, _grab_num(_vr, "PLAN_QTY") + _spill)
        _vr["SETUP_DAYS"] = _v_setup * _new_keep if _new_keep > 0 else 0
        # นิยามเดียวกับลูปหลัก: มีเครื่องใหม่ = วันทอหลังหัก setup ; เหลือแต่ carry = วันทอเต็ม
        if _keep_mc > 0:
            _vr["AVAILABLE_DAYS"] = _days_new if _new_keep > 0 else _days_carry
        if _bk_cut > 0:
            _vr["MC_BOOKING"] = max(0, _v_bk - _bk_cut)
            # เครื่อง booking ถูกหักจาก TOTAL_MC_REMAIN ตั้งแต่ AVA_MC → ถอยงานออกต้องคืน
            _release_booking_mc(_mc, _g, _wk, _bk_cut)
        _vr["REMARK"] = _build_plan_remark(
            _mc, _carry_keep, _new_keep, _vr["SETUP_DAYS"], int(_grab_num(_vr, "MC_BOOKING"))
        )
        _vr["LINE_REMARK"] = f"เลื่อนออกให้งานสี {item} ทอทัน Target Knit" + (
            f" W{int(tk_week)}" if tk_week else ""
        )
        # โควตา job คืนเฉพาะเครื่องที่แถวนี้ "ตั้งใหม่เอง" (carry ไม่ได้กินโควตา)
        _release_new = max(0, _v_new - _new_keep)
        if _release_new > 0:
            weekly_job_usage.setdefault(int(_wk), {})[_mc] = max(
                0, weekly_job_usage.setdefault(int(_wk), {}).get(_mc, 0) - _release_new
            )
            # โควตาตั้งเครื่องรายผ้าของเหยื่อก็ต้องคืนด้วย — เครื่องใหม่ชุดนั้นไม่ได้ตั้งแล้ว
            _item_setup_refund(_v_item, _mc, _g, _wk, _release_new)
        # ⚠️ บัญชีพูลถูกหักด้วย "เครื่องทั้งหมดของแถว (carry+new) − เครื่อง booking" ตอนเขียนแถว
        # และบางเส้นทาง (FG split / เติมยอดเข้าเครื่องเดิม) ไม่ได้หักเลย → คำนวณคืนเองเสี่ยงเพี้ยน
        # ใช้วิธีสร้างบัญชีใหม่จากแถวแผนจริง (เส้นทางเดียวกับที่ทุกรอบซ่อมใช้) จะตรงเสมอ
        # ต้องทำ "ก่อน" ไล่ยอดที่ถูกปลดไปวางต่อ เพราะ _repair_spill_forward อ่านพูลจากบัญชีนี้
        _resync_pool_ledger()
        # เหยื่อถือเครื่องน้อยลง → บัญชี carry ของลูปหลักต้องรู้ ไม่งั้นสัปดาห์ถัดไป
        # มันจะ carry เครื่องที่ไม่ใช่ของมันแล้ว (เครื่องผี)
        _v_key = _resolve_carry_key(_v_item, _mc, _g)
        if last_production.get(_v_key) == int(week_idx):
            machines_in_use[_v_key] = _keep_mc

        # 🧹 "ห้องว่างบนเครื่อง" ของสัปดาห์นี้ต้องหดตามเครื่องที่เหลือด้วย
        # สมุด remaining_week_cap คือ cap ที่ยังว่างอยู่บนเครื่องของผ้าตัวนี้ในสัปดาห์นั้น
        # ยึดเครื่องไปแล้วแต่ไม่ล้างสมุด → ใบอื่นของผ้าตัวเดียวกันเข้ามาเก็บตก cap ก้อนนี้
        # ได้แถว "เติมเข้าเครื่องที่วิ่งอยู่ 0 เครื่อง" = ผลิตโดยไม่มีเครื่องรองรับ
        # (เคสจริง 30 ก.ย. 2569 FD3BASKD35A0 W44 RAO: งานสี FD5BASPG10A0 ยึดไป 2 เครื่อง
        #  แล้ว SC 721917 เก็บตก 1,057.5 กก. ด้วย 0 เครื่อง — เกินกำลัง 1 เครื่อง 977.4 ด้วยซ้ำ)
        # _rider_room = ห้องว่างที่เหลือจริงหลังหักยอดที่เก็บไว้ได้ทั้งของแถวเหยื่อและใบที่ขี่เครื่องร่วม
        # ⚠️ คีย์ของสมุดเป็นเลขสัปดาห์เปล่าบ้าง YW บ้าง (ดู BOOKING CARRY seed) → เทียบด้วย
        #    ตำแหน่งบนปฏิทิน ไม่ใช่ตัวเลขตรง ๆ
        _rwc = globals().get("remaining_week_cap")
        _rwc_own = globals().get("remaining_week_cap_owner")
        if isinstance(_rwc, dict):
            for _rem_k in [
                _k for _k in list(_rwc)
                if _k[1] == _v_item and _k[2] == _mc and week_index(_k[0]) == int(week_idx)
            ]:
                _room_left = min(float(_rwc.get(_rem_k, 0) or 0), max(0.0, float(_rider_room)))
                if _room_left > 1e-6:
                    _rwc[_rem_k] = _room_left
                else:
                    _rwc.pop(_rem_k, None)
                    if isinstance(_rwc_own, dict):
                        _rwc_own.pop(_rem_k, None)
                    print(
                        f"   🧹 [COLOR GRAB] {_v_item} {_mc} G{_g} W{_wk}: ล้าง cap ค้างของเครื่องที่ถูกยึด "
                        f"(เหลือ {_keep_mc} เครื่อง) — กันใบอื่นมาเก็บตกแบบไม่มีเครื่อง"
                    )

        # ยอดที่ถูกปลด → ไล่ไปวางต่อสัปดาห์ถัดไปของ "ใบเดียวกัน" (ไม่ทิ้ง ไม่ข้ามใบ)
        if _spill > 1e-6:
            _v_sc = str(_vr.get("SC_SO_NO", "")).strip()
            _v_chain = {
                _ci: [
                    _r2 for _r2 in _rows2
                    if str(_r2.get("SC_SO_NO", "")).strip() == _v_sc
                ]
                for _ci, _rows2 in _chain_idx.get(_preempt_row_key(_vr), {}).items()
            }
            _v_chain = {_ci: _rr for _ci, _rr in _v_chain.items() if _rr}
            _placed, _left, _trace = _repair_spill_forward(
                _vr, _v_item, _mc, _g, int(week_idx), _spill, _keep_mc,
                _v_chain, _PLANS_REF, _v_setup, _v_daily, _rev_v,
                line_remark=f"เลื่อนออกให้งานสี {item} (main color grab)",
            )
            if _placed > 0:
                _vr["PLAN_QTY"] = max(0.0, _grab_num(_vr, "PLAN_QTY") - _placed)
            print(
                f"   ↩️ [COLOR GRAB] {_v_item} {_mc} G{_g} W{_wk}: ปลด {_cut} เครื่อง "
                f"ให้งานสี {item} | ยอด {_spill:,.0f} kg → วางต่อ {_placed:,.0f} kg"
                + (f" ({_trace})" if _trace else "")
                + (f" | ค้าง {_left:,.0f} kg" if _left > 1e-6 else "")
            )
        else:
            print(
                f"   ↩️ [COLOR GRAB] {_v_item} {_mc} G{_g} W{_wk}: ปลด {_cut} เครื่อง "
                f"ให้งานสี {item} (ยอดเดิมยังผลิตครบด้วยเครื่องที่เหลือ)"
            )

        # ยอดของ "ใบที่ขี่เครื่องชุดเดียวกัน" ที่เกินกำลังเครื่องที่เหลือ → ไล่ไปวางต่อของใบตัวเอง
        for _rd, _rd_spill in _rider_spills:
            _rd_sc = str(_rd.get("SC_SO_NO", "")).strip()
            _rd_chain = {
                _ci: [_r2 for _r2 in _rows2 if str(_r2.get("SC_SO_NO", "")).strip() == _rd_sc]
                for _ci, _rows2 in _chain_idx.get(_preempt_row_key(_rd), {}).items()
            }
            _rd_chain = {_ci: _rr for _ci, _rr in _rd_chain.items() if _rr}
            _rd_placed, _rd_left, _rd_trace = _repair_spill_forward(
                _rd, _v_item, _mc, _g, int(week_idx), _rd_spill, _keep_mc,
                _rd_chain, _PLANS_REF, _v_setup,
                _daily_of_row(_rd) or _v_daily,
                _grab_num(_rd, "REVOLUTION_WEIGHT") or _rev_v,
                line_remark=f"เลื่อนออกให้งานสี {item} (ใบที่ใช้เครื่องร่วม)",
            )
            if _rd_placed > 0:
                _rd["PLAN_QTY"] = max(0.0, _grab_num(_rd, "PLAN_QTY") - _rd_placed)
            print(
                f"   ↩️ [COLOR GRAB SHARED] {_v_item} SC {_rd_sc} {_mc} G{_g} W{_wk}: "
                f"ใบที่ขี่เครื่องชุดนี้เกินกำลังที่เหลือ {_rd_spill:,.0f} kg → วางต่อ {_rd_placed:,.0f} kg"
                + (f" ({_rd_trace})" if _rd_trace else "")
                + (f" | ค้าง {_rd_left:,.0f} kg" if _rd_left > 1e-6 else "")
            )
        _got += _cut

    return _got


def _main_grab_for_week(item, mc_group, gauge, week, want_mc, own_mc=0, tk_week=None) -> int:
    """เตรียมเครื่องให้พอตามทรงก่อนวางสัปดาห์นั้น — คืนจำนวนเครื่องที่ยึดมาเพิ่ม

    ยึดเฉพาะส่วนที่ขาด: ต้องการ want_mc − (พูลว่าง + เครื่องที่ก้อนนี้ถืออยู่แล้ว)
    """
    if not (MAIN_COLOR_GRAB_ENABLED and _CUR_ORDER_COLOR_PART):
        return 0
    if not mc_group or want_mc is None or want_mc <= 0 or week is None:
        return 0
    _wi = week_pos(week)
    if _wi is None:
        return 0
    _free = int(get_actual_mc_remain(mc_group, week, gauge=gauge, item_code=item) or 0)
    _short = int(want_mc) - _free - max(0, int(own_mc))
    if _short <= 0:
        return 0
    return _main_color_grab(item, mc_group, gauge, _wi, _short, tk_week=tk_week)


# ===== ยึดเครื่องแบบ "จองชั่วคราว" แล้วเก็บไว้เท่าที่แถวใช้จริง =====
# การยึดเครื่องให้งานสีเกิด "ก่อน" ด่านที่ตัดสินจำนวนเครื่องจริง (Load Balancing / hard cap /
# NEW MC STAND / CARRY ROOM TRIM) ยึดตามทรงที่อยากได้ แล้วด่านตัดเครื่องใหม่ทิ้งทีหลัง →
# เครื่องที่ยึดมาว่างเปล่า ขณะที่งานเจ้าของเดิมถูกดันไปสัปดาห์หลังแล้ว
# (เคสจริง 14 ก.ย. 2569 RAOO G16 W41: FD1BASMK32/01B0 ขาดแค่ 1 เครื่อง แต่ยึด 4 ตามทรง 6
#  → ทอจริง 2 ว่าง 3 ; FD3BASDP23/05B0 2,260 กก. ถูกดันจาก W41 ไป W43)
# ⚠️ ไม่ใช่การจำกัดจำนวนที่ยึดล่วงหน้า (ลองแล้วแย่ลง — ตัวเลขประเมินไม่ตรงกับที่ด่านตัดจริง)
#   ด่านยังตัดสินบนพูลที่ยึดมาแล้วเหมือนเดิมทุกประการ ต่างแค่ "หลังตัดสิน" คืนส่วนที่ไม่ได้ใช้
# วงจร: หัวรอบสัปดาห์เปิดสมุด → _main_color_grab จดทุกอย่างที่เปลี่ยน → ก่อนเขียนแถว
#   _grab_txn_settle ย้อนทั้งหมดแล้วยึดใหม่เท่าที่แถวใช้ ; รอบที่ไม่ได้เขียนแถว = ย้อนคืนทั้งหมด


def _grab_txn_new() -> dict:
    return {"calls": [], "rows": {}, "added": [], "job": {}, "bk": {}, "miu": {}, "rwc": {},
            "iq": {}}


def _grab_txn_open() -> None:
    global _GRAB_TXN
    _GRAB_TXN = _grab_txn_new() if MAIN_GRAB_SETTLE_ENABLED else None


def _grab_same(a, b) -> bool:
    try:
        if pd.isna(a) and pd.isna(b):
            return True
        return bool(a == b)
    except (TypeError, ValueError):
        return False


def _grab_scalar_num(v) -> float:
    _v = pd.to_numeric(v, errors="coerce")
    return 0.0 if pd.isna(_v) else float(_v)


def _grab_txn_capture(item, mc_group, gauge, week_idx) -> dict:
    """ถ่ายสถานะทุกอย่างที่การยึดครั้งนี้แตะได้ ก่อนยึดจริง

    แถวเหยื่อ + ลูกโซ่ของเหยื่อทุกสัปดาห์ (ยอดที่ถูกปลดไหลไปลงแถวพวกนี้) · ความยาวแผน
    (แถวที่งอกใหม่) · โควตา job · ช่อง booking ใน summary_mc · machines_in_use
    """
    _, _chain_idx = _main_grab_indexes()
    _rows = {}
    for _vr in _main_grab_victims(item, mc_group, gauge, week_idx):
        _rows.setdefault(id(_vr), (_vr, dict(_vr)))
        for _wrows in _chain_idx.get(_preempt_row_key(_vr), {}).values():
            for _r in _wrows:
                _rows.setdefault(id(_r), (_r, dict(_r)))
    return {
        "rows": _rows,
        "n": len(_PLANS_REF),
        "job": {_w: dict(_m) for _w, _m in weekly_job_usage.items()},
        # โควตาตั้งเครื่องรายผ้า — การยึดหักเพิ่ม (ยอดของเหยื่อไหลไปตั้งเครื่องสัปดาห์ถัดไป)
        # และคืน (เครื่องใหม่ของเหยื่อถูกปลด) ; ย้อนการยึดแล้วต้องกลับเป็นค่าเดิมเหมือนโควตา job
        "iq": dict(_item_new_mc_week),
        "bk": {
            _c: summary_mc[_c].to_numpy(copy=True)
            for _c in ("TOTAL_MC_REMAIN", "MC_USE_CEIL")
            if _c in summary_mc.columns
        },
        "bk_len": len(summary_mc),
        "miu": dict(machines_in_use),
        # สมุด "cap ที่เหลือของสัปดาห์" — การยึดเครื่องล้าง/ลดคีย์ของเหยื่อทิ้ง (ดู COLOR GRAB)
        # ถ้าไม่ถ่ายไว้ รอบที่ย้อนการยึด (abort/settle) จะคืนเครื่องให้เจ้าของแต่ห้องว่างหายฟรี
        "rwc": (lambda _d: dict(_d) if isinstance(_d, dict) else None)(
            globals().get("remaining_week_cap")
        ),
        "rwc_own": (lambda _d: dict(_d) if isinstance(_d, dict) else None)(
            globals().get("remaining_week_cap_owner")
        ),
    }


def _grab_txn_record(txn, pre, call) -> None:
    """เทียบสถานะหลังยึดกับที่ถ่ายไว้ แล้วจดเฉพาะส่วนที่เปลี่ยน (ค่าก่อนยึดครั้งแรกเท่านั้น)"""
    txn["calls"].append(call)
    for _rid, _pair in pre["rows"].items():
        txn["rows"].setdefault(_rid, _pair)
    txn["added"].extend(_PLANS_REF[pre["n"]:])
    for _w, _m in weekly_job_usage.items():
        _old_m = pre["job"].get(_w, {})
        for _k, _v in _m.items():
            _d = int(_v or 0) - int(_old_m.get(_k, 0) or 0)
            if _d:
                txn["job"][(_w, _k)] = txn["job"].get((_w, _k), 0) + _d
    # โควตารายผ้า: จดเป็นส่วนต่าง (แบบเดียวกับ job) — ย้อนด้วยการหักส่วนต่างออก ไม่ทับค่าทั้งก้อน
    # เพราะระหว่างรอบอาจมีการหัก/คืนทางอื่นในสัปดาห์เดียวกันซ้อนเข้ามา
    _piq = pre.get("iq") or {}
    for _k in set(_piq) | set(_item_new_mc_week):
        _d = int(_item_new_mc_week.get(_k, 0) or 0) - int(_piq.get(_k, 0) or 0)
        if _d:
            txn["iq"][_k] = txn["iq"].get(_k, 0) + _d
    if len(summary_mc) == pre["bk_len"]:
        for _c, _before in pre["bk"].items():
            if _c not in summary_mc.columns:
                continue
            _after = summary_mc[_c].to_numpy()
            for _pos in range(len(_after)):
                if not _grab_same(_after[_pos], _before[_pos]):
                    _slot = txn["bk"].setdefault((_pos, _c), [_before[_pos], None])
                    _slot[1] = _after[_pos]
    _pm = pre["miu"]
    for _k, _v in machines_in_use.items():
        if _k not in _pm or _pm[_k] != _v:
            txn["miu"].setdefault(_k, (_k in _pm, _pm.get(_k)))
    # cap ที่เหลือของสัปดาห์: จดคีย์ที่การยึดครั้งนี้ลด/ล้างทิ้ง (ค่าก่อนยึดครั้งแรกเท่านั้น)
    _prwc, _rwc_now = pre.get("rwc"), globals().get("remaining_week_cap")
    if isinstance(_prwc, dict) and isinstance(_rwc_now, dict):
        _prwc_own = pre.get("rwc_own") or {}
        for _k in set(_prwc) | set(_rwc_now):
            if _prwc.get(_k) != _rwc_now.get(_k):
                txn["rwc"].setdefault(_k, (_k in _prwc, _prwc.get(_k), _prwc_own.get(_k)))


def _grab_txn_rollback(txn) -> None:
    """ย้อนทุกอย่างที่การยึดในสมุดนี้เปลี่ยน กลับเป็นสถานะก่อนยึดครั้งแรก"""
    global _MAIN_GRAB_INDEX_N
    for _r, _snap in txn["rows"].values():
        _r.clear()
        _r.update(_snap)
    if txn["added"]:
        _ids = {id(_r) for _r in txn["added"]}
        _PLANS_REF[:] = [_r for _r in _PLANS_REF if id(_r) not in _ids]
    for (_w, _k), _d in txn["job"].items():
        _m = weekly_job_usage.setdefault(_w, {})
        _m[_k] = int(_m.get(_k, 0) or 0) - _d
    # โควตาตั้งเครื่องรายผ้า — คืนส่วนที่การยึดหัก/คืนไป (เดิมไม่ย้อน → ผ้าที่ถูกยึดโดนหักซ้ำทุกรอบ settle)
    for _k, _d in txn.get("iq", {}).items():
        _v = int(_item_new_mc_week.get(_k, 0) or 0) - int(_d)
        if _v > 0:
            _item_new_mc_week[_k] = _v
        else:
            _item_new_mc_week.pop(_k, None)
    for (_pos, _c), (_before, _after) in txn["bk"].items():
        if _c not in summary_mc.columns or _pos >= len(summary_mc):
            continue
        _ci = summary_mc.columns.get_loc(_c)
        _cur = summary_mc.iat[_pos, _ci]
        if _grab_same(_cur, _after):
            summary_mc.iat[_pos, _ci] = _before
        else:
            # มีเส้นทางอื่นแก้ช่องเดียวกันต่อหลังยึด → ย้อนเฉพาะส่วนที่การยึดเปลี่ยน
            summary_mc.iat[_pos, _ci] = _grab_scalar_num(_cur) - (
                _grab_scalar_num(_after) - _grab_scalar_num(_before)
            )
    for _k, (_had, _old) in txn["miu"].items():
        if _had:
            machines_in_use[_k] = _old
        else:
            machines_in_use.pop(_k, None)
    # คืนห้องว่างที่การยึดล้างไป — ไม่งั้นรอบที่ย้อนการยึดจะทำให้ cap ของเจ้าของเดิมหายฟรี
    _rwc = globals().get("remaining_week_cap")
    _rwc_own = globals().get("remaining_week_cap_owner")
    if isinstance(_rwc, dict):
        for _k, (_had, _old, _old_own) in txn.get("rwc", {}).items():
            if _had:
                _rwc[_k] = _old
                if isinstance(_rwc_own, dict):
                    _rwc_own[_k] = _old_own
            else:
                _rwc.pop(_k, None)
                if isinstance(_rwc_own, dict):
                    _rwc_own.pop(_k, None)
    # index ของการยึดยังถือแถวที่ถูกลบ → ล้างให้สร้างใหม่ตอนยึดครั้งถัดไป
    _MAIN_GRAB_POOL_INDEX.clear()
    _MAIN_GRAB_CHAIN_INDEX.clear()
    _MAIN_GRAB_INDEX_N = 0
    _resync_pool_ledger()


def _grab_txn_abort(reason="") -> None:
    """รอบสัปดาห์จบโดยไม่ได้เขียนแถว → ย้อนการยึดทั้งหมด (ไม่ได้ใช้เครื่อง ห้ามดันงานคนอื่นทิ้ง)"""
    global _GRAB_TXN
    _txn, _GRAB_TXN = _GRAB_TXN, None
    if not _txn or not _txn["calls"]:
        return
    _grab_txn_rollback(_txn)
    _c0 = _txn["calls"][0]
    print(
        f"   ↪️ [GRAB SETTLE] {_c0['item']} {_c0['mc']}: คืนเครื่องที่ยึดไว้ "
        f"{sum(_c['got'] for _c in _txn['calls'])} เครื่องให้งานเจ้าของเดิม — {reason}"
    )


def _main_color_grab(item, mc_group, gauge, week_idx, need_mc, tk_week=None) -> int:
    """ยึดเครื่องคืนให้งานสี (ตัวจริงคือ _main_color_grab_apply) — สมุดเปิดอยู่ = จดไว้ให้ย้อนได้"""
    _txn = _GRAB_TXN
    if _txn is None or not need_mc or need_mc <= 0 or week_idx is None:
        return _main_color_grab_apply(item, mc_group, gauge, week_idx, need_mc, tk_week=tk_week)
    _pre = _grab_txn_capture(item, mc_group, gauge, week_idx)
    _got = _main_color_grab_apply(item, mc_group, gauge, week_idx, need_mc, tk_week=tk_week)
    if _got > 0:
        _grab_txn_record(_txn, _pre, {
            "item": item, "mc": mc_group, "g": gauge, "wi": int(week_idx),
            "need": int(need_mc), "got": int(_got), "tk": tk_week,
        })
    return _got


def _grab_txn_settle(item, mc_group, gauge, week, use_mc, span_fn=None, job_fn=None,
                     next_room=None) -> None:
    """ก่อนเขียนแถว: ย้อนการยึดทั้งหมดของรอบสัปดาห์นี้ แล้วยึดใหม่เท่าที่แถวใช้จริง

    use_mc    = เครื่องที่แถวนี้จะหักจากพูลของสัปดาห์นี้ (สูตรเดียวกับตอนบันทึกแผน)
    span_fn   = ตัวตรวจ "เครื่องชุดนี้วิ่งต่อได้ครบทุกสัปดาห์" → (สัปดาห์ที่ติด, เหลือ, ต้องใช้) | None
    job_fn    = จำนวนเครื่องใหม่ที่ยังขาดช่องโควตา setup (0 = พอ)
    next_room = (สัปดาห์ถัดไป, เครื่องที่ต้องมีที่ยืน) เมื่องานยังไม่จบ | None
                ⚠️ ขาดไม่ได้: ด่าน CARRY POOL FULL ของสัปดาห์ถัดไปตรวจ "ก่อน" จุดยึดเครื่อง
                ถ้าคืนที่ยืนสัปดาห์หน้าทิ้ง เครื่องชุดนี้จะ carry ไม่ได้ → ลูกโซ่ขาด ต้อง setup ใหม่
                (เจอตอนทดสอบ: FD1BASMK32/01B0 RAOO W42 carry 0 → ตั้งใหม่ W43/W45)

    ยึดใหม่จากรายการยึดเดิมตามลำดับเดิม ไม่เกินจำนวนที่แต่ละครั้งเคยยึด (ไม่ลามไปแตะงานอื่น)
    ถ้ายึดใหม่แล้วเงื่อนไขใดแย่กว่าตอนก่อนย้อน → ย้อนอีกรอบแล้วยึดตามเดิมทุกครั้ง = ผลเท่าเดิม
    """
    global _GRAB_TXN
    _txn, _GRAB_TXN = _GRAB_TXN, None
    if not _txn or not _txn["calls"]:
        return

    def _free() -> int:
        return int(get_actual_mc_remain(
            mc_group, week, gauge=gauge, item_code=item, allow_negative=True
        ) or 0)

    _wi = week_pos(week)
    _use = max(0, int(use_mc or 0))
    _pre_free = _free()
    _pre_span_ok = span_fn is None or span_fn() is None
    _pre_job = int(job_fn() or 0) if job_fn else 0

    _nwk, _nneed = next_room if next_room else (None, 0)
    _nwi = week_pos(_nwk) if _nwk is not None else None
    _nneed = max(0, int(_nneed or 0)) if _nwi is not None else 0

    def _nfree() -> int:
        return int(get_actual_mc_remain(
            mc_group, _nwk, gauge=gauge, item_code=item, allow_negative=True
        ) or 0)

    _pre_nfree = _nfree() if _nneed > 0 else 0
    _grabbed = sum(_c["got"] for _c in _txn["calls"])
    _grab_txn_rollback(_txn)

    _replay = _grab_txn_new()
    _left = [_c["got"] for _c in _txn["calls"]]

    def _take(wi, short_fn) -> None:
        for _i, _c in enumerate(_txn["calls"]):
            if _c["wi"] != wi or _left[_i] <= 0:
                continue
            _short = int(short_fn() or 0)
            if _short <= 0:
                return
            _got = _main_color_grab(
                _c["item"], _c["mc"], _c["g"], wi, min(_left[_i], _short), tk_week=_c["tk"],
            )
            _left[_i] -= max(0, int(_got))

    _GRAB_TXN = _replay                    # จดการยึดรอบใหม่ไว้ เผื่อต้องย้อนกลับไปใช้ผลเดิม
    try:
        if _wi is not None:
            if job_fn is not None:
                _take(_wi, job_fn)
            _take(_wi, lambda: _use - _free())
        if _nneed > 0:
            _take(_nwi, lambda: _nneed - _nfree())
        if span_fn is not None:
            _done = set()
            for _ in range(12):
                _blk = span_fn()
                if _blk is None:
                    break
                _bwi = week_pos(_blk[0])
                if _bwi is None or _bwi in _done:
                    break
                _done.add(_bwi)

                def _span_short(_bwi=_bwi) -> int:
                    _b = span_fn()
                    if _b is None or week_pos(_b[0]) != _bwi:
                        return 0
                    return int(_b[2]) - max(0, int(_b[1]))

                _take(_bwi, _span_short)
        _ok = (
            _free() >= min(_use, _pre_free)
            and (_nneed <= 0 or _nfree() >= min(_nneed, _pre_nfree))
            and (not _pre_span_ok or span_fn is None or span_fn() is None)
            and (job_fn is None or int(job_fn() or 0) <= max(0, _pre_job))
        )
    finally:
        _GRAB_TXN = None

    _regot = sum(_c["got"] for _c in _replay["calls"])
    if not _ok:
        if _replay["calls"]:
            _grab_txn_rollback(_replay)
        for _c in _txn["calls"]:
            _main_color_grab_apply(_c["item"], _c["mc"], _c["g"], _c["wi"], _c["need"], tk_week=_c["tk"])
        print(
            f"   ⚠️ [GRAB SETTLE] {item} {mc_group} W{week}: ยึดใหม่แล้วเงื่อนไขไม่ครบเท่าเดิม "
            f"→ ใช้การยึดเดิม {_grabbed} เครื่อง"
        )
        return
    if _regot < _grabbed:
        print(
            f"   ↪️ [GRAB SETTLE] {item} {mc_group} W{week}: ยึดไว้ {_grabbed} เครื่อง "
            f"แถวใช้จริงต้องยึด {_regot} → คืนงานเจ้าของเดิม {_grabbed - _regot} เครื่อง"
        )


# =========================
# ยอดฐานที่ "ต้องวางในแผนรอบนี้" ต่อใบ+item+สี — เขียนลงคอลัมน์ ORDER_PENDING ท้ายชีท PLAN
# =========================
# ORDERS_QTY คือยอดสั่งทั้งใบ แต่แผนวางแค่ Pending Plan (ยอดที่ยังไม่เคยถูกวางในระบบ SC)
# หักส่วนที่ booking ทอไปแล้ว → หน้าเว็บที่เทียบยอดวางกับ ORDERS_QTY จึงขึ้น "ขาด" ทั้งที่แผนถูก
# (เคสจริง 23/9/2569 SC 722807 FD3CS45/1A0 สั่ง 1,440 วางแผนไปแล้ว 984 ต้องวางรอบนี้ 456)
#
# จดตอนเริ่มวางแต่ละก้อน (qty_left) แล้วบวกสะสม — จุดเดียวนี้ครอบทุกทางที่ยอดถูกแบ่ง:
#   · ก้อนสีที่ถูกแตกตามโควตา (_split_orders_by_color_quota) → พี่น้องทุกก้อนบวกกลับเป็นยอดใบ
#   · ก้อนจ้างทอ (OUTSOURCE_FORCE) → ยอดที่ตัดจากใบเดิมไป clone บวกกลับรวมกัน
#     (ยกเว้นก้อนที่ส่งออกไปทอแล้ว _os_shipped ซึ่งไม่ต้องวางในแผนนี้ → ฐานลดลงตามจริง)
#   · ส่วนที่ booking ทอไปแล้ว (already_made) ถูกหักออกไปแล้วตั้งแต่ qty_left
_ORDER_PENDING_BASE: dict = {}


def _key_txt(v) -> str:
    """ค่าคีย์ → ข้อความที่เทียบกันได้ไม่ว่ามาจากฝั่งใบสั่งหรือฝั่งแถวแผน
    (202648.0 กับ 202648 กับ "202648" ต้องได้คีย์เดียวกัน)"""
    if v is None:
        return ""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if pd.isna(v):
            return ""
        _f = float(v)
        return str(int(round(_f))) if abs(_f - round(_f)) < 1e-9 else f"{_f:.2f}"
    _t = str(v).strip().upper()
    return "" if _t in ("NAN", "NONE", "NAT", "<NA>") else _t


def _order_base_key(so, item, sub_color, line_id, fg_week, orders_qty, po_no) -> tuple:
    """คีย์ฐานยอด — ต้องละเอียดเท่ากับที่หน้าเว็บใช้จับกลุ่มออร์เดอร์ (ORDER_KEY_COLS + ITEM_CODE)

    ⚠️ ใบเดียว ผ้าเดียว แต่คนละสัปดาห์ส่ง (FG_WEEK) = คนละบรรทัดใบสั่ง มียอดสั่งของตัวเอง
    (เคสจริง 23/9/2569 SC 720970 FD6DRTKT66/02A0: FG 202648 ยอด 2,300 + FG 202652 ยอด 4,900)
    ถ้าคีย์หยาบกว่าฝั่งหน้าเว็บ ทั้งสองบรรทัดจะได้ฐานรวม 7,200 แล้วขึ้น "ขาด" ทั้งคู่"""
    return (
        _key_txt(so), _key_txt(item), _key_txt(sub_color), _key_txt(line_id),
        _key_txt(fg_week), _key_txt(orders_qty), _key_txt(po_no),
    )


def _run_planning_loop() -> list:
    global weekly_new_plan_usage, weekly_new_plan_pool_usage, remaining_week_cap, remaining_week_cap_owner
    global _type_special_weekly_usage
    global cylinder_change_count, cylinder_adjustments, _cylinder_change_for_item
    global _cylinder_change_start_map, _cylinder_change_mc_count, _cylinder_change_done
    global _carry_cyl_pending, _current_order_rdd_idx, _s9_weekly_usage, _s9_item_week_mc
    global orders, sc_so_no, plans, _skip_no_cap, _skip_no_mc_group, _skip_no_factory, _skip_not_in_master, _skip_qty_short
    global _blank_order_rows
    global _COLOR_ITEMS, _COLOR_QTY, _COLOR_QTY_ITEM
    global _CUR_ORDER_COLOR_PART

    # Reset state ที่ถูก init ก่อน line 6737 (ต้อง reset ทุก pass)
    _type_special_weekly_usage = {}
    cylinder_change_count = {}
    # Pre-load จำนวน cylinder ที่เปลี่ยนจริงไปแล้วจาก Job sheet (MasterMC)
    for _jw, _jgroups in _job_cylinder_done.items():
        for _jg, _jc in _jgroups.items():
            cylinder_change_count.setdefault(_jw, {})[_jg] = _jc
    cylinder_adjustments = {}
    # Change Cylinder ที่ user ติ๊กอนุมัติเองจากหน้าเว็บ = เครื่องถูกย้ายเกจไปแล้วจริง
    # → seed ตั้งแต่ต้นรอบ ไม่ต้องผ่าน _try_cylinder_change (ที่ยังปิดอยู่) และไม่ต้องผ่าน
    # กฎ JIT/โควตาของการเปลี่ยนอัตโนมัติ เพราะนี่ไม่ใช่ "ขอเปลี่ยนระหว่างวางแผน"
    # แต่เป็นสถานะเครื่องที่คนตัดสินใจไปแล้วก่อนรอบนี้จะเริ่ม (ติ๊กครั้งเดียว มีผลทุกรอบ)
    _apply_approved_cylinder_changes(cylinder_adjustments)
    _cylinder_change_for_item = {}
    _cylinder_change_start_map = {}
    _cylinder_change_mc_count = {}
    _cylinder_change_done = set()
    _carry_cyl_pending = {}
    _current_order_rdd_idx = None
    _s9_weekly_usage = {}
    _s9_item_week_mc = {}
    # ล้างสี YD ค้างจาก pass ก่อน (กัน color-change setup ผิดข้าม pass)
    # last_sub_color ไม่ได้ pre-load จาก booking จึงล้างทิ้งได้ปลอดภัย (ต่างจาก last_production/machines_in_use)
    last_sub_color.clear()

    weekly_new_plan_usage = {}  # {week: {(type_1,gauge): new_plan_machines}}
    weekly_new_plan_pool_usage = {}  # {week: {pool_label: new_plan_machines}} — สำหรับพูลแยก (SKP/SKPTA·SKPLE)

    # Pre-populate weekly_new_plan_usage จาก old plan (NEW plans รอบก่อน)

    # เพื่อให้ get_actual_mc_remain หัก capacity ที่ old plan ใช้ไปแล้ว

    if not old_plan_df.empty:

        _old_plan_preloaded = 0

        for _, _row in old_plan_df.iterrows():

            # Skip OLD plan rows if PLAN_SOURCE exists (safety check)

            # 🔧 FIX: Pre-load เฉพาะ NEW plan เพื่อป้องกัน double-count

            # summary_mc.TOTAL_MC_REMAIN ถูกหัก MC_USE_CEIL (booking เก่า) อยู่แล้ว

            # ถ้า pre-load OLD plan เข้า weekly_new_plan_usage → get_actual_mc_remain() หักซ้ำ

            if "PLAN_SOURCE" in old_plan_df.columns:

                if str(_row.get("PLAN_SOURCE", "")).strip().upper() != "NEW":
                    continue
            _op_week = _row.get("PLAN_WEEK") or _row.get("PLAN WEEK")
            _op_mc = _row.get("MC_GROUP") or _row.get("MC GROUP") or _row.get("MC")
            _op_machines = (
                _row.get("ACTUAL_MC")
                or _row.get("AVAILABLE_MACHINES")
                or _row.get("REQUIRED_MC")
                or _row.get("AVAILABLE_MACHINES")
            )

            _op_gauge = (

                _row.get("MC_GUAGE")

                or _row.get("MC GUAGE")

                or _row.get("GUAGE")

                or ""

            )

            if pd.isna(_op_week) or pd.isna(_op_mc) or pd.isna(_op_machines):

                continue

            try:

                _op_week = int(_op_week)

                _op_mc = str(_op_mc).strip().upper()

                _op_machines = int(float(_op_machines))

                _op_gauge = str(_op_gauge).strip() if _op_gauge and not pd.isna(_op_gauge) else ""

            except Exception:

                continue

            # Apply same redirect logic as main loop for pool deduction

            _redirected_mc, _redirected_gauge = _apply_mc_redirect(_op_mc, _op_gauge)

            _wpu_key = (_mc_to_type1(_redirected_mc, _redirected_gauge), _redirected_gauge)

            if _op_week not in weekly_new_plan_usage:

                weekly_new_plan_usage[_op_week] = {}

            weekly_new_plan_usage[_op_week][_wpu_key] = (

                weekly_new_plan_usage[_op_week].get(_wpu_key, 0) + _op_machines

            )

            # พูลแยก (SKP/SKPTA·SKPLE): นับต่อ pool label ด้วย (ให้ get_actual_mc_remain หักถูกพูล)
            _op_pool = _mc_to_pool_label(_redirected_mc, _redirected_gauge)
            if _op_pool:
                weekly_new_plan_pool_usage.setdefault(_op_week, {})[_op_pool] = (
                    weekly_new_plan_pool_usage.setdefault(_op_week, {}).get(_op_pool, 0) + _op_machines
                )

            _old_plan_preloaded += _op_machines

        print(

            f"📋 Pre-loaded weekly_new_plan_usage จาก old plan (NEW): "

            f"{_old_plan_preloaded} machines across {len(weekly_new_plan_usage)} weeks"

        )

    # cap ที่เหลือในสัปดาห์เมื่อ order จบก่อนใช้สุด — ใช้ผลิต FG ถัดไป (item+machine เดียวกัน)

    remaining_week_cap = {}  # {(week, item_code, mc_group): remaining_capacity_units}

    remaining_week_cap_owner = {}  # {(week, item_code, mc_group): owner marker (item-level carry)}
    _HOLD_CAP_MC.clear()           # คู่กับ remaining_week_cap — เครื่อง Hold ที่เปิดเป็น cap เหลือ



    # =========================

    # MERGE SAME SC + SAME ITEM (+ FG Week เดียวกัน)

    # =========================

    # ถ้า SC/SO NO เหมือนกัน + SO_NO เหมือนกัน + Item Code เหมือนกัน + FG Week เดียวกัน → รวมเป็น 1 row ผลิตทีเดียว

    # ต้องใช้ SO_NO ด้วยเพราะ SC เดียวกันอาจมีหลาย SO (คนละ order จริง) → ห้ามรวม qty

    # ถ้า FG Week ต่างกัน → คง row แยกไว้ (deadline ต่างกัน → plan แยก)

    _so_no_col = "SO_NO" if "SO_NO" in orders.columns else None
    _po_no_col = "PO_NO" if "PO_NO" in orders.columns else None

    _grp_keys = ["SC/SO NO"] + (["SO_NO"] if _so_no_col else []) + (["PO_NO"] if _po_no_col else []) + ["Item Code", "MC GROUP", "MC_GUAGE"]

    # YD-ORDERS: คนละ SUB_COLOR = คนละด้าย → ห้ามยุบรวมเป็นแถวเดียว (ทอต่อกันรวดเดียวไม่ได้ ต้องเปลี่ยนด้าย)
    # ถ้าไม่ใส่สีใน key: SO เดียวกัน 10 สี จะถูกยุบเหลือ 1 แถว, qty ถูก sum, SUB_COLOR เหลือสีแรกสีเดียว
    # order type อื่นเป็นผ้าดิบย้อมทีหลัง สีไม่มีผลตอนทอ → key = "" ทุกแถว = merge เหมือนเดิม
    if "SUB_COLOR" in orders.columns:
        _yd_merge_mask = (
            orders["Orders Type"].astype(str).str.strip() == "YD-ORDERS"
            if "Orders Type" in orders.columns
            else pd.Series(False, index=orders.index)
        )
        orders["_MERGE_COLOR_KEY"] = (
            orders["SUB_COLOR"].fillna("").astype(str).str.strip().where(_yd_merge_mask, "")
        )
        _grp_keys = _grp_keys + ["_MERGE_COLOR_KEY"]

    if "FG Week" in orders.columns:

        _grp_keys = _grp_keys + ["FG Week"]

    _sum_cols = [

        c

        for c in ["Orders.Qty", "Plan Qty", "Pending Plan", "Confirm"]

        if c in orders.columns

    ]

    _min_cols = [c for c in ["DYE_END_DATE"] if c in orders.columns]

    _first_cols = [c for c in orders.columns if c not in _grp_keys + _sum_cols + _min_cols]

    _agg_dict = {}

    _agg_dict.update({c: "sum" for c in _sum_cols})

    _agg_dict.update({c: "min" for c in _min_cols})

    _agg_dict.update({c: "first" for c in _first_cols})

    _orders_before = len(orders)

    # dropna=False สำคัญ: ค่าว่างในคีย์ (เช่น PO_NO ที่ booking ไม่มี) ทำให้ groupby ทิ้งแถวนั้นทั้งแถว
    # = งานหายจากแผนเงียบ ๆ ไม่โผล่แม้แต่ในชีท UNPLANNED
    orders = orders.groupby(_grp_keys, sort=False, dropna=False).agg(_agg_dict).reset_index()

    if "_MERGE_COLOR_KEY" in orders.columns:
        orders = orders.drop(columns=["_MERGE_COLOR_KEY"])

    print(

        f"✅ รวม orders same SC+Item: {_orders_before} → {len(orders)} rows (merged {_orders_before - len(orders)} rows)"

    )



    # =========================

    # MAIN PLANNING

    # =========================

    plans = []
    # ให้ _sp_used_from_plans() เห็นแถวที่วางไปแล้ว (ใช้ list เดียวกัน ไม่ใช่ copy)
    global _PLANS_REF
    _PLANS_REF = plans
    # ยอดตั้งต้น (old plan) ที่ _resync_pool_ledger ต้องบวกกลับทุกครั้ง
    _snapshot_pool_ledger_base()

    _skip_no_cap = []  # เก็บ item ที่ไม่มี cap เพื่อแสดงรวมท้ายสุด

    _skip_qty_short = []  # เก็บ order ที่เข้า loop วางแผนแล้วแต่วางไม่ครบ (เครื่องไม่พอ/หมดสัปดาห์/ชน safety guard)

    _skip_no_mc_group = []  # เก็บ order ที่ไม่มี MC GROUP → ไม่วางแผน
    _blank_order_rows = 0  # นับแถวว่าง (ไม่มี item + ยอด 0) ที่ข้ามทิ้ง

    _skip_no_factory = []  # เก็บ order ที่ MC GROUP ไม่มี FACTORY_TYPE → ไม่วางแผน

    _skip_not_in_master = []  # เก็บ order ที่ MC+Gauge ไม่มีใน MasterMC → ไม่วางแผน

    new_plan_started_items = set()  # ติดตาม (item, mc_group) ที่เริ่มการผลิตใน new plan แล้ว

    # YD-ORDERS: ล็อกจำนวนเครื่องต่อ (item, mc_group, plan_week)

    # ภายใน week เดียวกัน ห้ามเพิ่ม/ลดเครื่อง — SO ถัดไปใช้เครื่องเท่าเดิม

    _yd_week_locked_mc: dict = {}  # {(item, mc_group, plan_week): (actual_mc, carryover_mc, new_mc)}

    locked_mc_group_for: dict = (

        {}

    )  # ล็อก MC_GROUP (highest-cap) ต่อ (sc_so_no, item) ให้ FG Week ต่างๆ ใช้ร่วมกัน

    # ติดตาม last plan week index ต่อ (sc_so_no, item) เพื่อบังคับให้ FG_WEEK ถัดไป

    # เริ่มหลัง FG_WEEK ก่อนหน้าจบ (ไม่ผลิตซ้อนกัน)

    _last_fg_plan_idx: dict = {}  # {(sc_so_no, item): last_week_index}

    # เรียง orders ตาม TARGET_KNIT (rdd_idx) จริง ไม่ใช่ FG Week

    # เพราะ order type ต่างกัน TARGET_KNIT ต่างกัน (LAB-DIP = FG-1, SC-ORDERS = FG-offset ตาม FOB_TYPE)





    def _order_rdd_idx(row):

        """คำนวณ rdd_idx (TARGET_KNIT index) สำหรับ sort เท่านั้น"""

        try:

            fg_w = row.get("FG Week")

            o_type = str(row.get("Orders Type", "")).strip()

            if pd.isna(fg_w) or fg_w is None:

                return 99999



            fg_w_str = str(int(fg_w)).strip()

            if len(fg_w_str) >= 6:

                _yr = int(fg_w_str[:4])

                _wk = int(fg_w_str[4:])

            elif len(fg_w_str) == 5:

                _yr = int(fg_w_str[:4])

                _wk = int(fg_w_str[4:])

            else:

                _yr = TODAY.year

                _wk = int(fg_w_str)

            _row = calendar_week[

                (calendar_week["YEAR"] == _yr) & (calendar_week["WEEK_NUM"] == _wk)

            ]

            if _row.empty:

                return 99999



            _raw_idx = _row.index[0]

            _rdd = max(0, _raw_idx - target_offset_weeks_for_row(row))

            if o_type == "LAB-DIP":

                # LAB-DIP: sort เรียงตาม TODAY_IDX + 2 (เริ่มและเสร็จใน week +2)

                _rdd = min(len(calendar_week) - 1, TODAY_IDX + FREEZE_WEEKS)

            return _rdd



        except Exception:

            return 99999



    orders["_sort_rdd_idx"] = orders.apply(_order_rdd_idx, axis=1)

    # ---------- สำรวจกระดานก่อนวาง: งานไหน "ยืดหยุ่นน้อย" ต้องได้คิวก่อน ----------
    # (user เคาะ 2 ก.ย. 2569: ต้องดูทั้งกระดานก่อนว่าจะวางอะไรบ้าง แล้วค่อยเลือกวาง)
    #
    # ปัญหาเดิม: ระบบวางทีละใบตามคิว โดยตอนตัดสินใจใบแรก ๆ ยังไม่รู้ว่าข้างหลังมีงานอะไรรอ
    # → งานยอดใหญ่ที่ต้องใช้เวลาหลายสัปดาห์ (และเครื่องขึ้นได้ทีละ 2 ตัวเท่านั้น) มักได้คิวช้า
    #   แล้วเหลือเวลาไม่พอ ต้องไปเร่งเครื่องตอนท้าย/ทอเลยกำหนด
    #
    # slack = "จำนวนสัปดาห์ที่เหลือให้เล่น" = ช่วงเวลาที่วางได้ − สัปดาห์ที่ต้องใช้จริง
    #   ยิ่งน้อย = ยิ่งบีบ = ต้องได้คิวก่อน (ใส่เป็นตัวตัดสินรองจากสัปดาห์ที่ต้องผลิต+priority
    #   จึงไม่แซงข้ามสัปดาห์ ปลอดภัยกับกติกาเดิม)
    _PRESCAN_MAX_MC = 12          # เพดานเครื่องที่ใช้ประเมิน (แค่จัดลำดับ ไม่ใช่การจอง)

    def _order_slack(row):
        try:
            _qty = float(pd.to_numeric(row.get("Pending Plan", 0), errors="coerce") or 0)
            if _qty <= 0:
                return 999
            _mcg = str(row.get("MC GROUP", "") or row.get("MC_GROUP", "")).strip().upper()
            _gau = _normalize_gauge(row.get("MC_GUAGE", ""))
            _item = str(row.get("Item Code", "")).strip().upper()
            # ใช้ CAP_PER_DAY ของแถวออร์เดอร์ (ฐาน 24 ชม.) — รอบนี้ใช้แค่ "จัดลำดับ"
            # ไม่ได้เอาไปคิดกำลังจริง จึงไม่ต้องแปลงชั่วโมง/หักวัน setup ให้ละเอียด
            _cap = float(pd.to_numeric(row.get("CAP_PER_DAY", 0), errors="coerce") or 0)
            if _cap <= 0:
                return 999
            _wd = 6.0
            try:
                _wd = float(get_working_days_by_factory(_mcg, 1, item_code=_item, gauge=_gau) or 6.0)
            except Exception:
                _wd = 6.0
            if _wd <= 0:
                return 999
            # ไต่เครื่องทีละ MAX_NEW_SETUP_MC ต่อสัปดาห์ → กี่สัปดาห์ถึงจะทอครบ
            _acc, _mc_run, _weeks = 0.0, 0, 0
            while _acc < _qty and _weeks < 60:
                _weeks += 1
                _mc_run = min(_mc_run + MAX_NEW_SETUP_MC, _PRESCAN_MAX_MC)
                _acc += _mc_run * _wd * _cap
            _rdd_i = row.get("_sort_rdd_idx", 99999)
            if _rdd_i is None or _rdd_i >= 99999:
                return 999
            _window = max(0, int(_rdd_i) - (TODAY_IDX + FREEZE_WEEKS) + 1)
            return int(_window - _weeks)
        except Exception:
            return 999

    _slack_raw = orders.apply(_order_slack, axis=1)
    # ⚠️ งานที่ slack ติดลบ = "เวลาไม่พอตั้งแต่ต้น ยังไงก็ไม่ทัน" — ห้ามให้คิวก่อน
    # (ทดสอบแล้ว: ให้คิวก่อนกับกลุ่มนี้ = เอาเครื่องไปให้งานที่ยังไงก็สาย แล้วงานที่ยังทันได้
    #  กลายเป็นสายแทน — งานถูกดันช้าลง 29 ก้อน setup ประหยัดได้น้อยลงเหลือ 8 job)
    # ลำดับที่ถูก: งานที่ "บีบแต่ยังทันได้" (slack น้อยแต่ ≥ 0) ก่อน → งานที่มีเวลาเหลือเยอะ
    #             → งานที่ไม่มีทางทันไว้ท้ายสุด (จะได้ไม่ไปแย่งเครื่องของงานที่ยังกู้ได้)
    orders["_sort_slack"] = [
        (1, 0) if _s < 0 else (0, int(_s)) for _s in _slack_raw
    ]
    _tight_cnt = int((_slack_raw == 0).sum() + (_slack_raw == 1).sum())
    _no_hope = int((_slack_raw < 0).sum())
    print(
        f"🔎 สำรวจกระดานก่อนวาง: บีบแต่ยังทันได้ (slack 0-1) {_tight_cnt} แถว → ได้คิวก่อน | "
        f"เวลาไม่พอตั้งแต่ต้น {_no_hope} แถว → ไว้ท้ายแถว (จาก {len(orders)} แถว)"
    )

    # ---------- item ที่ "มีสีแล้ว" ต้องได้คิววางแผนก่อน ----------
    # แหล่งเดียว = view ยอดเปิดสีของใบ SC (Order/view_scfol_item.xlsx ผ่านโมดูล ColorOpen)
    # คีย์ SC_NO + ITEM_CODE → OPEN_COLOR_LOSS = ยอดที่เปิดสีแล้ว, KNIT_KGS = ยอดทอทั้งใบ
    #
    # ⚠️ เลิกใช้วิธีเดิมทั้งหมดแล้ว (user ยืนยัน 27 ส.ค. 2569) — ที่ถูกถอดออก:
    #   1. W## ใน LOAD_DYE ของ datamining_booking_mapped.xlsx  → บอกได้แค่ "มี/ไม่มี" ระดับ item
    #   2. คอลัมน์ NAY_COLOR / COLOR_DESC / SUB_COLOR ในออร์เดอร์ → นั่นคือสีที่ลูกค้าสั่ง
    #      ไม่ใช่สีที่เปิดจริง ทำให้ item ถูกนับเป็นงานสีเกินจริง
    #   3. NAY_COLOR / COLOR ในชีท DETAIL ของ booking            → เหตุผลเดียวกับ 2
    # เหลือ env KP_COLOR_ITEMS / KP_COLOR_QTY ไว้เป็น override มือตอนข้อมูล view ยังไม่ทัน
    def _load_color_open() -> tuple:
        """→ (เซ็ต item ที่มีงานสี, {(SC, ITEM): กก.เปิดสีแล้ว}, {ITEM: กก. จาก env})"""
        _open = ColorOpen.quota_by_sc_item()      # {} ถ้ายังไม่มีไฟล์
        if _open:
            _by_item: dict = {}
            for (_s, _i), _q in _open.items():
                _by_item[_i] = _by_item.get(_i, 0.0) + _q
            print(f"🎨 งานสีจาก {ColorOpen.DEFAULT_PATH.name}: "
                  f"{len(_open):,} ใบสั่ง×item / {len(_by_item):,} รหัส "
                  f"รวม {sum(_open.values()):,.0f} กก.ที่เปิดสีแล้ว")
        else:
            print(f"⚠️ ยังไม่มี {ColorOpen.DEFAULT_PATH.name} (รัน View_SCFol_Item ก่อน) "
                  f"→ ไม่มีข้อมูลงานสี ใช้ได้เฉพาะ override จาก env")

        codes = {_i for (_s, _i) in _open}

        # override มือ: บังคับให้เป็นงานสี / กำหนดยอดเอง (ชนะข้อมูลจาก view)
        _env_qty: dict = {}
        for _pair in str(os.environ.get("KP_COLOR_QTY", "")).split(","):
            if ":" not in _pair:
                continue
            _k, _v = _pair.split(":", 1)
            try:
                _env_qty[_k.strip().upper()] = float(_v)
            except ValueError:
                continue
        codes.update(_env_qty)
        for _c in str(os.environ.get("KP_COLOR_ITEMS", "")).split(","):
            _c = _c.strip().upper()
            if _c:
                codes.add(_c)
        if _env_qty or len(codes) > len({_i for (_s, _i) in _open}):
            print(f"🎨 override จาก env: KP_COLOR_ITEMS/KP_COLOR_QTY → รวมเป็น {len(codes):,} รหัส")

        codes.discard("")
        return codes, _open, _env_qty

    _COLOR_ITEMS, _COLOR_QTY, _COLOR_QTY_ITEM = _load_color_open()

    # ---------- seed "สัปดาห์สีสุดท้าย" จาก booking เดิม (Color Gap) ----------
    # booking ไม่มีธง COLOR_PART ติดมา (แตกโควตาสีทำกับออร์เดอร์ที่รอวางเท่านั้น) → ถ้าไม่ seed
    # ตรงนี้ งานที่ก้อนสีเป็น booking ล้วนจะไม่มีจุดตั้งต้นให้นับสัปดาห์เว้นเลย
    # เกณฑ์: item ที่เปิดสีแล้ว → สัปดาห์สุดท้ายที่ booking ของคีย์นั้นยังเดินเครื่อง = สีถึงสัปดาห์นั้น
    if COLOR_GAP_ENABLED and _COLOR_ITEMS:
        for _cg_key, _cg_weeks in booking_active_week_set.items():
            if not _cg_weeks or str(_cg_key[0]).strip().upper() not in _COLOR_ITEMS:
                continue
            _note_color_end(_cg_key[0], _cg_key[1], _cg_key[2], max(_cg_weeks))
        for _cg_key, _cg_by_w in booking_mc_by_week.items():
            if str(_cg_key[0]).strip().upper() not in _COLOR_ITEMS:
                continue
            _cg_act = [_w for _w, _m in _cg_by_w.items() if (_m or 0) > 0]
            if _cg_act:
                _note_color_end(_cg_key[0], _cg_key[1], _cg_key[2], max(_cg_act))
        if _color_end_idx:
            print(f"🎨 Color Gap: seed สัปดาห์สีสุดท้ายจาก booking {len(_color_end_idx):,} คีย์ "
                  f"(item+เครื่อง+เกจ) → เว้น 1 สัปดาห์หลังทอต่ออีก {COLOR_GAP_AFTER} สัปดาห์")

    # ---------- แตกยอดตาม "โควตาสีคงเหลือ" ก่อนจัดคิว (user 27 ส.ค. 2569) ----------
    # เดิมเกณฑ์งานสีเป็น boolean ("ใบนี้เปิดสีแล้วไหม") → ใบที่เปิดสี 1,000 กก. แต่มีของ
    # ค้างวาง 3,000 กก. ได้คิว Color ทั้ง 3,000 ทั้งที่ย้อมได้จริงแค่ 1,000 และไม่มีการหัก
    # ยอดที่สัปดาห์ก่อน ๆ กินไปแล้ว (W35 100 + W36 200 → W37 ต้องเหลือ 700 ไม่ใช่ 1,000)
    #
    # ตอนนี้แจกโควตาเป็นกิโลต่อ (SC, ITEM):
    #   โควตาคงเหลือ = OPEN_COLOR_LOSS − ที่ booking ทอไปแล้ว − ที่แถวก่อนหน้าของใบนี้กินไป
    # แถวที่โควตาไม่คลุมทั้งก้อน → แตกเป็น 2 แถว: ก้อนสี (priority 0) / ส่วนเกิน (priority ปกติ)
    def _order_produced_qty(row, item_code: str) -> float:
        """ยอดที่ booking ทอไปแล้วของ (ใบสั่งนี้, item นี้) — ใช้หักโควตาสี

        OPEN_COLOR_LOSS เป็นยอดเปิดสีของทั้งใบ (รวมส่วนที่ทอไปแล้ว) ส่วน Pending Plan
        เป็นยอดที่ยังไม่ได้วาง → ถ้าไม่หักออก ใบที่ทอไปเกือบครบจะได้โควตาสีเกินของจริง
        (เกณฑ์เดียวกับ already_made ในลูปวางแผน แต่แยกราย item ไม่ใช่ทั้ง SO)"""
        _so = str(row.get("SO_NO", row.get("SC/SO NO", ""))).strip().upper()
        if not _so or not item_code:
            return 0.0
        _bare = _so.lstrip("S")
        for _k in ((_so, item_code), ("S" + _bare, item_code), (_bare, item_code)):
            if _k in booking_produced_qty_item:
                return float(booking_produced_qty_item[_k])
        return 0.0

    def _split_orders_by_color_quota(df):
        """แตกแถวออร์เดอร์ตามโควตาสีคงเหลือ → คืน DataFrame ใหม่พร้อมคอลัมน์ _COLOR_PART

        ⚠️ ต้องเรียก "หลัง" merge orders เท่านั้น — เรียกก่อนจะถูก groupby ยุบกลับเป็นแถวเดียว
        ⚠️ ก้อนที่เล็กกว่ากำลังผลิต 1 วันของ item นั้นไม่แตก (ยกทั้งแถวเป็นฝั่งที่ใหญ่กว่า)
           เพราะแถวจิ๋วต้องขอเครื่อง + setup ของตัวเองเต็มตัว = จองเครื่องทั้งสัปดาห์เพื่อ 24 กก.
        """
        df = df.copy()
        df[_COLOR_PART_COL] = None
        df["_SPLIT_PEND_TOTAL"] = float("nan")   # ยอดของแถวก่อนถูกแตก (ไม่แตก = NaN)
        if not _COLOR_ITEMS:
            return df

        def _is_core_row(_i) -> bool:
            """แถวออร์เดอร์นี้เป็นผ้า core ไหม (เกณฑ์เดียวกับ _plan_priority / is_core_item)

            user 16 ก.ย. 2569: **ผ้า core ไม่เกี่ยวกับกติกาสีเลย** — ไม่ติดธงงานสี ไม่กินโควตาสี
            (โควตาที่ core เคยกินไปคือยอดที่งานสีจริงควรได้ใช้ แล้ว core ยังได้สิทธิ์ยึดเครื่อง
            ในนามงานสีอีกต่อหนึ่ง) ; ธงว่างยังทำให้บล็อกบน Gantt ไม่ขึ้นเป็นงานสีด้วย
            """
            _it_c = str(df.at[_i, "Item Code"]).strip().upper()
            if not _it_c:
                return False
            return (_it_c, _core_ckey(df.at[_i, "Customer"] if "Customer" in df.columns else "")) \
                in itemcore_pairs
        # kill-switch: ปิดแล้วกลับไปใช้เกณฑ์เดิม (ใบไหนเปิดสีแล้ว = งานสีทั้งก้อน)
        # ชุดเดียวกับด่านพูล/ด่านโควตา setup — ไว้เทียบผลและกันไว้ตอนแผนเพี้ยน
        if not plan_config.get("KP_COLOR_SPLIT"):
            for _i in df.index:
                _it0 = str(df.at[_i, "Item Code"]).strip().upper()
                _sc0 = df.at[_i, "SC/SO NO"] if "SC/SO NO" in df.columns else df.at[_i, "SO_NO"]
                if _it0 and not _is_core_row(_i) and _color_quota_of(_it0, _sc0) > 0:
                    df.at[_i, _COLOR_PART_COL] = True
            print("🎨 ปิด 'แตกยอดงานตามโควตาสีที่เปิดจริง' ไว้ → ใช้เกณฑ์เดิม (ใบไหนเปิดสีแล้ว = งานสีทั้งก้อน)")
            return df

        # จัดกลุ่มด้วยคีย์ (SC normalize, ITEM) — รูปเดียวกับ _build_color_budget ในรอบซ่อม
        # เพื่อให้ "ใบเดียวกันที่เขียน SC คนละรูป" อยู่กลุ่มเดียวกัน ไม่แบ่งโควตาซ้ำ
        _groups: dict = {}
        _n_core_skip = 0
        for _i in df.index:
            _it = str(df.at[_i, "Item Code"]).strip().upper()
            if not _it or _it not in _COLOR_ITEMS:
                continue
            if _is_core_row(_i):
                _n_core_skip += 1        # ผ้า core ไม่เข้ากติกาสี (ดู _is_core_row)
                continue
            _pend = pd.to_numeric(df.at[_i, "Pending Plan"], errors="coerce")
            if pd.isna(_pend) or float(_pend) <= 0:
                continue
            _sc_raw = df.at[_i, "SC/SO NO"] if "SC/SO NO" in df.columns else df.at[_i, "SO_NO"]
            _groups.setdefault((",".join(ColorOpen.split_sc(_sc_raw)), _it), []).append(_i)

        _clones, _n_split, _n_small_color, _n_small_over = [], 0, 0, 0
        for (_sc_key, _it), _idxs in _groups.items():
            _quota = _color_quota_of(_it, _sc_key)
            if _quota <= 0:
                continue
            if _quota == float("inf"):
                # override มือโดยไม่ระบุกิโล → ถือเป็นงานสีทั้งก้อนเหมือนเดิม
                for _i in _idxs:
                    df.at[_i, _COLOR_PART_COL] = True
                continue

            # หักยอดที่ทอไปแล้ว (นับใบละครั้ง — หลายแถวของใบเดียวกันใช้ยอดก้อนเดียว)
            _seen_so, _produced = set(), 0.0
            for _i in _idxs:
                _so = str(df.at[_i, "SO_NO"] if "SO_NO" in df.columns else "").strip().upper()
                if _so in _seen_so:
                    continue
                _seen_so.add(_so)
                _produced += _order_produced_qty(df.loc[_i], _it)
            _left = max(0.0, float(_quota) - _produced)

            # deadline เร็วก่อนได้โควตาก่อน (ของด่วนต้องได้ทอด้วยสีที่เปิดมาแล้ว)
            _idxs = sorted(
                _idxs,
                key=lambda _x: (
                    float(pd.to_numeric(df.at[_x, "_sort_rdd_idx"], errors="coerce") or 99999),
                    float(pd.to_numeric(df.at[_x, "FG Week"], errors="coerce") or 99999999)
                    if "FG Week" in df.columns else 0.0,
                ),
            )
            for _i in _idxs:
                _pend = float(pd.to_numeric(df.at[_i, "Pending Plan"], errors="coerce") or 0)
                _take = max(0.0, min(_pend, _left))
                # ขั้นต่ำของก้อนที่จะแตกออกมา = กำลังผลิต 1 วันของ item (ฐาน 24 ชม. พอสำหรับกันเศษ)
                _min_piece = float(pd.to_numeric(df.at[_i, "CAP_PER_DAY"], errors="coerce") or 0) \
                    if "CAP_PER_DAY" in df.columns else 0.0
                if _take <= 1e-6:
                    df.at[_i, _COLOR_PART_COL] = False        # โควตาหมดแล้ว → ไม่ใช่งานสี
                    continue
                if _take >= _pend - 1e-6:
                    df.at[_i, _COLOR_PART_COL] = True         # โควตาคลุมทั้งก้อน
                    _left -= _pend
                    continue
                if _take < _min_piece:
                    # ก้อนสีเล็กเกินกว่าจะคุ้มเครื่อง → ไม่แตก และไม่กินโควตา (ให้ใบ/แถวถัดไปใช้)
                    df.at[_i, _COLOR_PART_COL] = False
                    _n_small_color += 1
                    continue
                if (_pend - _take) < _min_piece:
                    # ส่วนเกินเล็กเกิน → ยกทั้งแถวเป็นงานสี (โควตาถูกใช้จนหมด)
                    df.at[_i, _COLOR_PART_COL] = True
                    _left = 0.0
                    _n_small_over += 1
                    continue
                # แตกจริง: แถวเดิม = ก้อนสี, clone = ส่วนเกิน (แพตเทิร์นเดียวกับ outsource split)
                # เก็บยอดก่อนแตกไว้ด้วย — รายงานที่ map ใบ→แถวเดียว (แถว booking เก่าในชีท PLAN)
                # ต้องใช้ยอดทั้งใบ ไม่ใช่ยอดของก้อนที่มันบังเอิญชี้ไปเจอ
                df.at[_i, "Pending Plan"] = _take
                df.at[_i, _COLOR_PART_COL] = True
                df.at[_i, "_SPLIT_PEND_TOTAL"] = _pend
                _cl = df.loc[_i].copy()
                _cl["Pending Plan"] = _pend - _take
                _cl[_COLOR_PART_COL] = False
                _cl["_SPLIT_PEND_TOTAL"] = _pend
                _clones.append(_cl)
                _left -= _take
                _n_split += 1

        if _clones:
            df = pd.concat([df, pd.DataFrame(_clones)], ignore_index=True)
        else:
            df = df.reset_index(drop=True)

        _n_color_rows = int((df[_COLOR_PART_COL] == True).sum())   # noqa: E712
        print(
            f"🎨 โควตาสีคงเหลือ: แตกแถว {_n_split} ใบ (ก้อนสี {_n_color_rows} แถว) "
            f"| ไม่แตกเพราะก้อนสีเล็กกว่ากำลัง 1 วัน {_n_small_color} | "
            f"ส่วนเกินเล็กกว่ากำลัง 1 วัน {_n_small_over}"
            + (f" | ข้ามผ้า core {_n_core_skip} แถว (ไม่เข้ากติกาสี)" if _n_core_skip else "")
        )
        return df

    orders = _split_orders_by_color_quota(orders)

    def _has_color_data(row, item_code: str) -> bool:
        """แถวนี้ได้คิว "งานสีมาก่อน" ไหม

        เกณฑ์ = ก้อนนี้ยังอยู่ใน "โควตาสีคงเหลือ" ของใบสั่งนั้น (ธง _COLOR_PART ที่
        _split_orders_by_color_quota แจกไว้) ไม่ใช่แค่ "ใบนี้เคยเปิดสีไหม" อีกแล้ว —
        ยอดส่วนที่เกินโควตา (รวมที่สัปดาห์ก่อน ๆ กินไปแล้ว) ตกไปใช้ priority ปกติ

        ⚠️ ถอดเกณฑ์เดิมที่ดู NAY_COLOR / COLOR_DESC ออกแล้ว (user 27 ส.ค. 2569) — คอลัมน์
        พวกนั้นคือ "สีที่ลูกค้าสั่ง" ไม่ใช่ "สีที่เปิดแล้ว" ทำให้ได้คิวก่อนทั้งที่ยังย้อมไม่ได้

        คงไว้เฉพาะ LAB-DIP / YD-ORDERS: ไม่ใช่การเดาสี แต่เป็นชนิดใบสั่งที่สีมากับด้าย
        อยู่แล้ว (ย้อมด้าย/ตัวอย่างสี) จึงไม่มีวันโผล่ใน OPEN_COLOR_LOSS
        """
        _part = row.get(_COLOR_PART_COL)
        if _part is True:
            return True
        if _part is False:
            return False                 # โควตาสีของใบนี้หมดแล้ว → ไม่ใช่งานสี

        order_type = str(row.get("Orders Type", "")).strip().upper()

        return order_type in ("LAB-DIP", "YD-ORDERS")

    # ---------- ลำดับความสำคัญ 4 ระดับ (user 26 ส.ค. 2569) ----------
    # ใช้ตัดสิน "ใครได้เครื่องก่อน" เมื่อหลายงานต้องผลิตสัปดาห์เดียวกันแต่เครื่องไม่พอ
    # ไม่ใช่การยกคิวข้ามสัปดาห์ → sort เรียงสัปดาห์ที่ต้องผลิต (grp_rdd) ก่อน แล้วค่อยใช้ค่านี้
    # เข้าหลายกลุ่มพร้อมกัน → ยึดระดับสูงสุด (เช็คไล่จากบนลงล่างแล้ว return ทันที)
    #   เช่น item ในชีท Program ที่เปิดสีแล้ว = ระดับ 0 Color ไม่ใช่ 1 Program
    _PRIORITY_LABELS = {0: "Color", 1: "Program", 2: "Core", 3: "SC"}

    def _plan_priority(row) -> int:

        item_code = str(row.get("Item Code", "")).strip().upper()

        # core = ผ้า + ลูกค้า ต้องอยู่ "แถวเดียวกัน" ในทะเบียน (เกณฑ์เดียวกับ is_core_item ในลูปวางแผน)
        # ไม่ได้กรองด้วย CORE_CUSTOMERS — ทะเบียนเป็นตัวกำหนดเองว่าคู่ไหนเป็น core
        # ⚠️ ต้องตรวจ "ก่อน" งานสี (user 16 ก.ย. 2569: ผ้า core ไม่เกี่ยวกับกติกาสีเลย) — เดิม core
        # ที่บังเอิญมียอดเปิดสีจะกระโดดขึ้นเป็นระดับ 1 แล้วแย่งเครื่องในนามงานสี ทั้งที่มันเดินตาม
        # รอบสต๊อกของตัวเอง ไม่ได้รอส่งลูกค้า
        if item_code and (item_code, _core_ckey(row.get("Customer", ""))) in itemcore_pairs:
            return 2                                   # 3. CORE ITEM

        if _has_color_data(row, item_code):
            return 0                                   # 1. งานที่เปิดสีแล้ว

        if item_code and item_code in _item_program_map:
            return 1                                   # 2. item ในชีท Program (MasterMC)

        return 3                                       # 4. งาน SC ทั่วไป

    orders["_sort_priority"] = orders.apply(_plan_priority, axis=1)
    # สำเนาไว้ใช้ในลูปวางแผน (_sort_priority ถูก drop ก่อนเข้าลูป) — ด่านจ้างทอ AUTO อ่านค่านี้
    orders["PLAN_PRIORITY"] = orders["_sort_priority"]

    _prio_counts = orders["_sort_priority"].value_counts().to_dict()
    print(
        "🥇 ลำดับแย่งเครื่องในสัปดาห์เดียวกัน: "
        + " | ".join(
            f"{_k+1}.{_PRIORITY_LABELS[_k]} {int(_prio_counts.get(_k, 0))} แถว" for _k in (0, 1, 2, 3)
        )
        + f" (จาก {len(orders)} แถว, item ที่ให้สีแล้ว {len(_COLOR_ITEMS)} รหัส)"
    )

    # tiebreaker 1: FG Week raw (น้อยกว่า = ก่อน) → หาก rdd_idx เท่ากัน ให้ FG เร็วกว่าก่อน

    # tiebreaker 2: PENDING_PLAN (น้อยกว่า = ก่อน) → ของน้อยจบไว ปล่อยเครื่องให้ item เดียวกัน carry ต่อ

    _fg_week_col = "FG Week" if "FG Week" in orders.columns else None

    _pending_col = "Pending Plan" if "Pending Plan" in orders.columns else None

    if _fg_week_col:

        orders["_sort_fg_week"] = pd.to_numeric(orders[_fg_week_col], errors="coerce").fillna(99999999)

    if _pending_col:

        orders["_sort_pending"] = pd.to_numeric(orders[_pending_col], errors="coerce").fillna(0)

    # YD-ORDERS: สีเดียวกัน (Item + SUB_COLOR) ต้องวางติดกันแม้คนละ SO / คนละ FG Week
    # → เครื่องเดินต่อได้เลย ไม่ต้องเสีย 1 วันเปลี่ยนสีซ้ำ
    # ตำแหน่งของ "กลุ่มสี" ใช้ RDD/FG ที่เร็วสุดในกลุ่มเป็นตัวกำหนด แต่ละใบยังเช็ค deadline ตัวเองในลูปวางแผน
    # order type อื่น key = "" ทั้งหมด → เปรียบเทียบเท่ากันเสมอ ตกไปใช้ลำดับเดิม (rdd → fg → pending)
    _yd_sort_mask = (
        orders["Orders Type"].astype(str).str.strip() == "YD-ORDERS"
        if "Orders Type" in orders.columns
        else pd.Series(False, index=orders.index)
    )

    _yd_color_key = (
        orders["Item Code"].astype(str).str.strip().str.upper()
        + "|"
        + orders.get("SUB_COLOR", pd.Series("", index=orders.index)).fillna("").astype(str).str.strip()
    )

    orders["_sort_yd_color_grp"] = _yd_color_key.where(_yd_sort_mask, "")

    orders["_sort_grp_rdd"] = (
        orders.groupby("_sort_yd_color_grp")["_sort_rdd_idx"]
        .transform("min")
        .where(_yd_sort_mask, orders["_sort_rdd_idx"])
    )

    # สัปดาห์ที่ต้องผลิต (grp_rdd) มาก่อน priority เสมอ:
    # priority เป็นตัวเลือกว่า "ในสัปดาห์ที่ต้องผลิตเดียวกัน ใครได้เครื่องก่อน" ไม่ใช่ตั๋วแซงข้ามสัปดาห์
    # (เดิม _sort_color_priority อยู่หน้าสุด → งานสีที่ target ยังอีกไกลไปจองเครื่องก่อนงานที่ครบกำหนดแล้ว)
    # slack มาหลัง priority: ในสัปดาห์ที่ต้องผลิตเดียวกันและ priority เดียวกัน
    # งานที่ "เวลาไม่พอ" ต้องได้เครื่องก่องานที่ยัดตรงไหนก็ได้ (ผลจากการสำรวจกระดาน)
    _sort_cols = ["_sort_grp_rdd", "_sort_priority", "_sort_slack"]

    if _fg_week_col:

        orders["_sort_grp_fg"] = (
            orders.groupby("_sort_yd_color_grp")["_sort_fg_week"]
            .transform("min")
            .where(_yd_sort_mask, orders["_sort_fg_week"])
        )

        _sort_cols.append("_sort_grp_fg")

    # YD-ORDERS: กลุ่มสีที่ย้อมเสร็จก่อน ต้องได้คิวก่อน
    # ถ้าเรียงตามชื่อสีเฉย ๆ กลุ่มที่ DYE_END ช้า (เริ่มทอไม่ได้จนกว่าจะย้อมเสร็จ) จะไปจองเครื่องก่อน
    # แล้วกลุ่มที่ย้อมเสร็จแล้วจะถูก carry ตามเครื่องนั้นไปวางช้าเกิน deadline
    # order type อื่น = ค่าคงที่ (-1) → ไม่มีผลต่อลำดับเดิม
    if "DYE_END_DATE" in orders.columns:

        _dye_ts = pd.to_datetime(orders["DYE_END_DATE"], errors="coerce")

        # NaT (ไม่รู้วันย้อม) → ให้ไปท้ายสุดของกลุ่ม YD ด้วยกัน
        _dye_num = _dye_ts.map(lambda _t: _t.value if pd.notna(_t) else 9_999_999_999_999_999)

        orders["_sort_grp_dye"] = (
            _dye_num.groupby(orders["_sort_yd_color_grp"]).transform("min").where(_yd_sort_mask, -1)
        )

        _sort_cols.append("_sort_grp_dye")

    _sort_cols += ["_sort_yd_color_grp", "_sort_rdd_idx"]

    if _fg_week_col:

        _sort_cols.append("_sort_fg_week")

    if _pending_col:

        _sort_cols.append("_sort_pending")

    orders_sorted = orders.sort_values(_sort_cols, na_position="last")

    _drop_cols = [c for c in ["_sort_priority", "_sort_slack", "_sort_rdd_idx", "_sort_fg_week", "_sort_pending", "_sort_yd_color_grp", "_sort_grp_rdd", "_sort_grp_fg", "_sort_grp_dye"] if c in orders_sorted.columns]

    orders_sorted = orders_sorted.drop(columns=_drop_cols)

    orders = orders.drop(columns=[c for c in _drop_cols if c in orders.columns])

    print(f"[DEBUG ORDERS] Total orders: {len(orders)}")

    # ---------- จ้างทอ (Outsource split) ----------
    # user แบ่ง qty ของ item ไปจ้างทอจากหน้าเว็บ → data_plan/outsource_split.json
    # ตัด Pending Plan ออกจาก order ของ item นั้น (ใบ FG/RDD เร็วสุดก่อน) แล้ว clone เป็น order ใหม่
    # ที่ติดธง OUTSOURCE_FORCE → planning loop จะ route ก้อนนี้เข้า S9 pool (COMKN) ซึ่งเป็นเครื่อง
    # จ้างทอ แยกพูลจากเครื่องในบ้าน ส่วนที่เหลือ (pending ที่ถูกตัดแล้ว) วางแผนในบ้านตาม logic เดิม
    _outsource_split = _load_outsource_split()
    if _outsource_split:
        orders_sorted = orders_sorted.copy()
        orders_sorted["OUTSOURCE_FORCE"] = False
        orders_sorted["OUTSOURCE_WEEK"] = None
        _os_clones = []
        _os_item_col = orders_sorted["Item Code"].astype(str).str.strip().str.upper()
        for _os_item, _os_cfg in _outsource_split.items():
            _os_qty = float(_os_cfg["outsource_qty"])
            # start_week (+ start_year ถ้ามี) → YW ; ไม่มีปี = ครั้งที่ใกล้วันนี้สุด
            _os_week = _os_cfg.get("start_week")
            if _os_week not in (None, ""):
                _os_week = _TL.key(_os_week, _os_cfg.get("start_year"))
            # สัปดาห์ที่ user กดจ้างทอผ่านไปแล้ว (< TODAY+2 = สัปดาห์แรกที่วางงานได้) = ก้อนนี้
            # ถูกส่งออกไปทอข้างนอกเรียบร้อยแล้ว → หักยอดออกจาก pending เหมือนเดิม แต่ "ไม่ clone"
            # เป็นงานให้วางใหม่ ; ไฟล์ outsource_split.json ค้างอยู่จนกว่า user จะลบเอง ถ้ายังวางซ้ำ
            # ทุกรอบ จะได้แถวจ้างทอโผล่ในสัปดาห์อดีตที่ freeze ไปแล้ว (ดู/ลากไม่ได้) ทุกครั้งที่รัน
            _os_idx = week_index(int(_os_week)) if _os_week not in (None, "") else None
            _os_shipped = _os_idx is not None and _os_idx < TODAY_IDX + FREEZE_WEEKS
            _os_mask = (_os_item_col == _os_item) & (
                pd.to_numeric(orders_sorted["Pending Plan"], errors="coerce").fillna(0) > 0
            )
            _os_rows = orders_sorted[_os_mask]
            if _os_rows.empty:
                print(f"[OUTSOURCE] {_os_item}: ไม่มีของค้างในแผนแล้ว → ข้ามการจ้างทอ")
                continue
            # ตัดจากใบที่ FG/RDD เร็วที่สุดก่อน (งานด่วนได้จ้างทอช่วยก่อน)
            if "FG Week" in _os_rows.columns:
                _os_rows = _os_rows.assign(
                    _os_fg=pd.to_numeric(_os_rows["FG Week"], errors="coerce").fillna(99999999)
                ).sort_values("_os_fg")
            _left = _os_qty
            for _oi in _os_rows.index:
                if _left <= 0:
                    break
                _pend = pd.to_numeric(orders_sorted.at[_oi, "Pending Plan"], errors="coerce")
                _pend = 0.0 if pd.isna(_pend) else float(_pend)
                _cut = min(_pend, _left)
                if _cut <= 0:
                    continue
                orders_sorted.at[_oi, "Pending Plan"] = _pend - _cut
                if not _os_shipped:
                    _clone = orders_sorted.loc[_oi].copy()
                    _clone["Pending Plan"] = _cut
                    _clone["OUTSOURCE_FORCE"] = True
                    _clone["OUTSOURCE_WEEK"] = _os_week
                    _os_clones.append(_clone)
                _left -= _cut
            _done = _os_qty - _left
            if _os_shipped:
                print(f"[OUTSOURCE] {_os_item}: W{_os_week} ผ่านไปแล้ว (สัปดาห์แรกที่วางได้คือ "
                      f"W{int(calendar_week.iloc[TODAY_IDX + FREEZE_WEEKS]['WEEK']) if TODAY_IDX + FREEZE_WEEKS < len(calendar_week) else '?'}) "
                      f"→ ถือว่าส่งไปทอข้างนอกแล้ว หักยอด {_done:.0f} กก. ออกจากแผนในบ้าน (ไม่วางแถวจ้างทอซ้ำ)")
            else:
                print(f"[OUTSOURCE] {_os_item}: จ้างทอ {_done:.0f} กก. "
                      f"(ขอ {_os_qty:.0f}) W{_os_week} → ที่เหลือวางแผนในบ้านตามปกติ")
            if _left > 0:
                print(f"[OUTSOURCE] {_os_item}: ของค้างไม่พอ — จ้างทอได้แค่ {_done:.0f} จาก {_os_qty:.0f} กก.")
        if _os_clones:
            orders_sorted = pd.concat(
                [orders_sorted, pd.DataFrame(_os_clones)], ignore_index=True
            )
            print(f"[OUTSOURCE] สร้าง order จ้างทอ {len(_os_clones)} ใบ")



    # 🔧 FIX: ตรวจสอบว่า Item+Week+MC_GROUP นี้มีการ plan ไปแล้วหรือไม่

    # เพื่อป้องกันการซ้ำซ้อนของ orders ที่มี Item+MC_GROUP เดียวกันใน week เดียวกัน

    # โดยไม่สนใจว่าจะเป็น SC หรือ TARGET_KNIT อะไร (เพราะอาจมีการ merge SC)

    _existing_item_week_mc = set()

    # 🔧 FIX: ติดตาม qty ที่วางแผนไปแล้วสำหรับแต่ละ item (สะสมข้าม order/SC/FG)

    # ใช้สำหรับคำนวณ next_fg_qty ที่แม่นยำ — ไม่นับ demand จาก order ที่ plan ไปแล้ว

    _item_cumulative_planned = {}  # {item_code: total_qty_planned_so_far}

    print(f"[DEBUG DUPLICATE] Starting with {len(plans)} existing plans")



    for _, order in orders_sorted.iterrows():

        # ปรับบัญชีพูลให้ตรงกับแถวแผนจริงก่อนวาง order ถัดไป — กันบัญชีเพี้ยนสะสม
        # แล้วด่านเห็นพูลเต็มทั้งที่ยังว่าง (ดู _resync_pool_ledger)
        _resync_pool_ledger()

        item = order["Item Code"]

        order_qty = order["Orders.Qty"]  # ปริมาณที่สั่งทั้งหมด

        pending_plan = pd.to_numeric(order.get("Pending Plan", 0), errors="coerce")

        pending_plan = 0.0 if pd.isna(pending_plan) else float(pending_plan)

        sc_so_no = str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip()  # ใช้แยก order ต่างสี

        sub_color = str(order.get("SUB_COLOR", "")).strip()  # SUB_COLOR สำหรับ YD-ORDERS carry logic

        # ก้อนนี้เป็น "ยอดที่เปิดสีแล้ว" ไหม (ธงจาก _split_orders_by_color_quota)
        # → ตัวกำหนดว่ามีสิทธิ์ยึดเครื่องคืนจากงานที่ยังไม่ให้สีระหว่างวางหรือไม่
        # (ดูหมายเหตุที่ MAIN_COLOR_GRAB_ENABLED — สิทธิ์นี้ย้ายมาจากรอบซ่อมท้ายแผน)
        _CUR_ORDER_COLOR_PART = bool(order.get(_COLOR_PART_COL))

        # ก้อนสีที่ถูก "แตกตามโควตาสี" (_split_orders_by_color_quota) — พี่น้องก้อนที่ยังไม่ให้สี
        # ของใบ+item เดียวกันรอวางต่อในคิว (คิวเรียง deadline ก่อน แล้วจึง priority → ก้อนสีมาก่อน
        # เสมอ) ⇒ ห้ามลดเครื่องตอนก้อนสีใกล้จบ ไม่งั้นพี่น้องต้องตั้งเครื่องใหม่กลับมาสัปดาห์ถัดไป
        # = จ่ายวัน setup ทิ้ง + กินโควตา job (เคสจริง FD4BASRG25/07A0 SC719829: ทรง 2-2-2-1-2
        #   W43 ตั้งเครื่องใหม่ setup 5 วันเพื่อทอ 2 วัน ทั้งที่ถือเครื่องเดิมไว้ไม่ต้องเสียอะไรเลย)
        # ⚠️ เครื่องที่ถือเผื่อไว้ต้องยังถูก "งานสีของ item อื่น" ยึดคืนได้ (ธง MC_HOLD_SPLIT
        #    + _hold_extra_mc) ไม่งั้นการถือเครื่องกลายเป็นเกราะกันงานสี = ผิดกติกางานสีมาก่อน
        _split_hold_mc = bool(_CUR_ORDER_COLOR_PART) and pd.notna(
            order.get("_SPLIT_PEND_TOTAL")
        )

        # แถวว่าง (ไม่มี item และไม่มียอดค้าง) ไม่ใช่ใบสั่งจริง → ข้ามเงียบ ไม่ลงชีท UNPLANNED
        # ไม่งั้นทุกแถวว่างตกด่าน "ไม่มี MC GROUP" ข้างล่าง แล้วโผล่เป็นแถว NAN/0 เต็มหน้าเว็บ
        # (เคสจริงแผนเซิร์ฟเวอร์ 14-9-2569) ; มียอดค้างแต่ไม่มี item ยังไปแจ้งตามปกติ
        if _is_blank_order_row(item, pending_plan):
            _blank_order_rows += 1
            continue

        # ผ้า S9 Only = จ้างทอเท่านั้น (user 24 ก.ย. 2569) — ไม่ต้องผ่านด่านของงานทอในบ้าน
        # (MC GROUP / FACTORY / MasterMC / CAP ทอในบ้าน) ตัดสินด้วยชีท S9 Only อย่างเดียว (ด่านถัดจาก CAP ข้างล่าง)
        _s9o_item = str(item).strip().upper() in _s9_only_spec

        # ถ้าไม่มี MC GROUP → เก็บไว้แจ้ง ไม่วางแผน

        _ord_mc_grp = str(order.get("MC GROUP", "")).strip().upper()

        if not _s9o_item and (not _ord_mc_grp or _ord_mc_grp in ("", "NAN", "NONE")):

            _skip_no_mc_group.append(_unplanned_row(order, "ไม่มี MC GROUP"))

            continue

        # MC_GROUP_REDIRECT ต้องทำตรงนี้ — ก่อนด่าน FACTORY/MasterMC ข้างล่าง
        # ไม่งั้นคู่ที่ถูกยุบ (เช่น RAO G16 ที่ไม่มีใน Master MC แล้ว) จะตกเป็น UNPLANNED
        # ตั้งแต่ด่านแรก โดยไม่มีวันไปถึงจุด redirect ตอนเลือกเครื่อง
        # เขียนค่ากลับลง order ด้วย เพื่อให้ FACTORY/วันทำงาน/setup ใช้เครื่องปลายทางทั้งหมด
        _redir_mc0, _redir_g0 = _apply_mc_redirect(_ord_mc_grp, order.get("MC_GUAGE", ""))
        if _redir_mc0 != _ord_mc_grp:
            print(f"[MC_REDIRECT ORDER] {_ord_mc_grp}/{_normalize_gauge(order.get('MC_GUAGE', ''))} "
                  f"→ {_redir_mc0}/{_redir_g0} ({sc_so_no} {item})")
            _ord_mc_grp = _redir_mc0
            order["MC GROUP"] = _redir_mc0
            order["MC_GUAGE"] = _redir_g0

        # ถ้า MC GROUP ไม่มีใน FACTORY_TYPE_MAP → เก็บไว้แจ้ง ไม่วางแผน

        _ord_factory = FACTORY_TYPE_MAP.get(_ord_mc_grp, "")

        if not _s9o_item and (not _ord_factory or _ord_factory in ("", "UNKNOWN")):

            _skip_no_factory.append(
                _unplanned_row(order, f"MC GROUP '{_ord_mc_grp}' ไม่มี FACTORY_TYPE")
            )

            continue

        # ถ้า (MC GROUP, Gauge) ไม่มีใน MasterMC → เก็บไว้แจ้ง ไม่วางแผน
        _ord_gauge_norm = _normalize_gauge(order.get("MC_GUAGE", ""))
        if not _s9o_item and (_ord_mc_grp, _ord_gauge_norm) not in _VALID_MC_GAUGE_SET:
            _skip_not_in_master.append(_unplanned_row(
                order,
                f"MC GROUP '{_ord_mc_grp}' Gauge '{_ord_gauge_norm}' ไม่มีใน MasterMC",
                gauge=_ord_gauge_norm,
            ))
            continue

        # ถ้า Pending Plan = 0 แสดงว่า order นี้วางแผนครบแล้ว ไม่ต้องวางแผนซ้ำ

        print(f"[DEBUG ORDERS] Order {sc_so_no}: item={item}, pending_plan={pending_plan}")

        if pending_plan <= 0:

            print(f"[DEBUG ORDERS] Skipping order {sc_so_no} - pending_plan <= 0")

            continue



        order_type = order["Orders Type"]

        fg_week = order.get("FG Week")

        # ตรวจสอบว่า SO นี้ผลิตไปแล้วบางส่วนใน booking จริงหรือไม่

        _so_try = ["S" + sc_so_no.lstrip("S"), sc_so_no.lstrip("S")]

        already_made = 0.0

        for _s in _so_try:

            _s_up = _s.upper()

            if _s_up in booking_produced_qty:

                already_made = booking_produced_qty[_s_up]

                break



        # qty_left = Pending Plan (ยังไม่ได้วางแผน) หักส่วนที่ผลิตจริงไปแล้วจาก booking

        qty_left = max(0.0, pending_plan - already_made)

        # ก้อนจ้างทอ (clone จาก order เดิม) — pending คือยอดที่ user แบ่งไว้แล้ว
        # ไม่ต้องหัก already_made ซ้ำ เพราะถูกหักที่ order ต้นฉบับ (ก้อนในบ้าน) ไปแล้ว
        _outsource_force = bool(order.get("OUTSOURCE_FORCE", False))
        _outsource_week = order.get("OUTSOURCE_WEEK")
        if _outsource_force:
            qty_left = max(0.0, pending_plan)

        # ยอดฐานของใบนี้ = ยอดที่ก้อนนี้ตั้งใจวาง (ดู _ORDER_PENDING_BASE)
        # ต้องจดตรงนี้ — หลัง qty_left นิ่งแล้ว แต่ก่อนทุกด่านที่อาจวางไม่ครบ (เครื่องไม่พอ/
        # core batch) เพราะฐานคือ "ต้องวางเท่าไหร่" ไม่ใช่ "วางได้เท่าไหร่" — ใบที่วางไม่หมด
        # ต้องยังขึ้นเตือนว่าขาดตามเดิม
        # ทุกค่าต้องเป็นค่าเดียวกับที่เขียนลงแถวแผน (ดูจุดสร้างแถว: FG_WEEK=fg_week,
        # ORDERS_QTY=order_qty, SC_LINE_ID/PO_NO จาก order ชุดเดียวกัน) ไม่งั้นคีย์ไม่ตรงตอน map กลับ
        _base_key = _order_base_key(
            sc_so_no, item, sub_color, order.get("SC_LINE_ID"),
            order.get("FG Week"), order_qty, order.get("PO_NO"),
        )
        _ORDER_PENDING_BASE[_base_key] = _ORDER_PENDING_BASE.get(_base_key, 0.0) + qty_left

        # Special rule: เช็คจาก Itemcore และ Customer (CORE ITEM)

        # ถ้าคู่ (ผ้า, ลูกค้า) ของใบสั่งนี้อยู่แถวเดียวกันในทะเบียน -> ผลิตต่อท้าย (กฎ CORE ITEM)

        # ถ้า item อยู่ในทะเบียนแต่คนละลูกค้ากับแถวนั้น -> ผลิตตาม target ปกติ

        # เกณฑ์เดียวกับ _plan_priority (ระดับ 3.Core ในลำดับคิว) — ใช้ itemcore_pairs ทั้งคู่

        rts_local_force = None

        is_core_item = False  # flag สำหรับ output column

        # เช็คว่า item อยู่ใน Itemcore หรือไม่

        item_upper = str(item).strip().upper()

        item_in_itemcore = item_upper in itemcore_lookup

        if item_in_itemcore:

            # Item อยู่ใน Itemcore - เช็คว่า customer เป็น core customer หรือไม่

            actual_customer = str(order.get("Customer", "")).strip()

            # ต้องเป็น "ผ้า + ลูกค้า" คู่ที่มีอยู่จริงในทะเบียน — เดิมเช็คแยกกัน (ผ้าอยู่แถวไหน
            # ก็ได้ + ลูกค้าเป็นลูกค้า core คนไหนก็ได้) ทำให้ผ้าของ RTS ที่สั่งโดย CENTER DOMESTIC
            # ถูกนับเป็น core ทั้งที่คนละเป้า คนละก้อนสต๊อก
            customer_match = (item_upper, _core_ckey(actual_customer)) in itemcore_pairs

            if customer_match:

                # Item อยู่ใน Itemcore + Customer เป็น core -> ใช้กฎ CORE ITEM (ผลิตต่อท้าย)

                is_core_item = True

                try:

                    dm = detail_mc[

                        detail_mc["ITEM_CODE"].astype(str).str.upper().str.strip()

                        == str(item).upper()

                    ]

                    if not dm.empty:

                        last_w = int(dm["WEEK"].dropna().astype(int).max())

                        start_after = next_week(last_w)

                        # Build maps per MC_GROUP: last booked week and machines used

                        last_old_by_mc = {}

                        machines_by_mc = {}

                        daily_cap_by_mc = {}

                        booking_remain_cap_by_mc = {}  # remaining cap ในสัปดาห์สุดท้าย booking

                        for mc, grp in dm.groupby("MC_GROUP"):

                            try:

                                w = int(grp["WEEK"].dropna().astype(int).max())

                            except Exception:

                                continue



                            last_old_by_mc[str(mc).strip().upper()] = w

                            # get MC_USE_CEIL from the last week that had > 0 machines

                            # (week order may have 0 at the end e.g. paused week → skip those)

                            grp_active = grp[

                                pd.to_numeric(grp["MC_USE_CEIL"], errors="coerce").fillna(0)

                                > 0

                            ]

                            if not grp_active.empty:

                                w_active = int(

                                    grp_active["WEEK"].dropna().astype(int).max()

                                )

                                last_active_row = grp_active[

                                    grp_active["WEEK"] == w_active

                                ].iloc[-1]

                            else:

                                last_active_row = grp[grp["WEEK"] == w].iloc[-1]

                            try:

                                machines_by_mc[str(mc).strip().upper()] = int(

                                    last_active_row.get("MC_USE_CEIL", 0) or 0

                                )

                            except Exception:

                                machines_by_mc[str(mc).strip().upper()] = 0

                            # try to get daily cap from item_cap_data per mc

                            # ใช้ cap น้อยที่สุดของ item นี้ในการคำนวณ

                            try:

                                _all_cap_for_item = item_cap_data[

                                    item_cap_data["ITEM_CODE"] == item

                                ]

                                if not _all_cap_for_item.empty:

                                    daily_cap_by_mc[str(mc).strip().upper()] = float(

                                        _all_cap_for_item["CAP ทอ"].min()

                                    )

                                else:

                                    cap_row2 = item_cap_data[

                                        item_cap_data["MC_GROUP"] == str(mc).strip().upper()

                                    ]

                                    if not cap_row2.empty:

                                        daily_cap_by_mc[str(mc).strip().upper()] = (

                                            cap_row2.iloc[0].get("CAP ทอ", None)

                                        )

                            except Exception:

                                daily_cap_by_mc[str(mc).strip().upper()] = None

                            # คำนวณ remaining capacity ในสัปดาห์สุดท้ายของ booking

                            # เพื่อให้ new SO สามารถ carry ต่อในสัปดาห์เดียวกันได้

                            # โดยจำกัด qty ตาม old item ที่ใช้เครื่องร่วมกัน (MC_USE fraction)

                            try:

                                _last_bk_row = grp[grp["WEEK"] == w]

                                if not _last_bk_row.empty:

                                    _mc_use_ceil = float(_last_bk_row.iloc[0].get("MC_USE_CEIL", 0) or 0)

                                    _bk_kp_weight = float(_last_bk_row.iloc[0].get("KP_WEIGHT", 0) or 0)

                                    # WORKING_DAY มาจากชีท Work Day แล้ว — ใช้ตรง ๆ (float: 5.5 วันต้องไม่ปัดเป็น 5)
                                    _bk_wd = float(_last_bk_row.iloc[0].get("WORKING_DAY", 0) or 0)

                                    _bk_gauge = str(_last_bk_row.iloc[0].get("GUAGE", "")).strip()

                                    _bk_plan_cap = _get_capacity_for_mc_group(

                                        item, str(mc).strip().upper(), _bk_gauge

                                    )

                                    # remaining = total machine capacity − qty ที่ old item ผลิตจริง (KP_WEIGHT)

                                    # สูตร: (MC_USE_CEIL × wd × daily_cap) − KP_WEIGHT

                                    # เช่น FD4DRTPC88A0 w17: (2×8×103.06)−1143 = 1648−1143 = 505 units

                                    # 🔧 FIX: ตรวจสอบว่า booking week นี้มีการ setup เพิ่มเครื่องหรือไม่

                                    # โดยเปรียบเทียบ MC_USE_CEIL กับ week ก่อนหน้าใน grp

                                    _sorted_bk_weeks = sorted(grp["WEEK"].dropna().astype(int).tolist())

                                    _w_pos_bk = _sorted_bk_weeks.index(w) if w in _sorted_bk_weeks else -1

                                    _prev_bk_mc = 0

                                    _prev_w_bk_val = None

                                    if _w_pos_bk > 0:

                                        _prev_w_bk = _sorted_bk_weeks[_w_pos_bk - 1]

                                        _prev_w_bk_val = _prev_w_bk

                                        _prev_bk_row = grp[grp["WEEK"] == _prev_w_bk]

                                        if not _prev_bk_row.empty:

                                            _prev_bk_mc = float(_prev_bk_row.iloc[0].get("MC_USE_CEIL", 0) or 0)

                                    # เว้นไม่เกิน SETUP_GAP_WEEK = เครื่องยังอุ่น ไม่ต้อง setup (กติกาเดียวกับทาง non-CORE)

                                    # ระยะห่างวัดบนปฏิทิน (WEEK เป็น YW — ลบตรง ๆ ข้ามปีจะได้ ~48)
                                    _bk_week_gap = _week_gap_of(w, _prev_w_bk_val) if _prev_w_bk_val is not None else 999

                                    if _bk_week_gap > SETUP_GAP_WEEK:

                                        _bk_new_mc = float(_mc_use_ceil)  # all machines are new setup

                                        _bk_carry_mc = 0.0

                                    else:

                                        _bk_new_mc = max(0.0, _mc_use_ceil - _prev_bk_mc)

                                        _bk_carry_mc = _mc_use_ceil - _bk_new_mc

                                    # คำนวณ total_cap โดยหัก setup days สำหรับเครื่องใหม่

                                    _total_cap = (_bk_carry_mc * _bk_wd + _bk_new_mc * max(0, _bk_wd - SETUP_DAYS)) * _bk_plan_cap

                                    if _bk_new_mc > 0:

                                        print(f"[BOOKING SETUP DETECT] {item} W{w} MC={mc}: new_mc={_bk_new_mc:.0f} carry_mc={_bk_carry_mc:.0f} → adjusted total_cap={_total_cap:.2f} (naive={_mc_use_ceil*_bk_wd*_bk_plan_cap:.2f})")

                                    if _bk_plan_cap > 0 and _mc_use_ceil > 0:

                                        _bk_rem = max(0.0, _total_cap - _bk_kp_weight)

                                        if _bk_rem > 0:

                                            booking_remain_cap_by_mc[str(mc).strip().upper()] = _bk_rem

                            except Exception:

                                pass

                        rts_local_force = {

                            "last_old_by_mc": last_old_by_mc,

                            "machines_by_mc": machines_by_mc,

                            "daily_cap_by_mc": daily_cap_by_mc,

                            "booking_remain_by_mc": booking_remain_cap_by_mc,

                        }

                except Exception:

                    rts_local_force = None



        # ----------------------

        # RDD Check and Urgent Planning

        # ----------------------

        # rdd_idx = row index ใน calendar_week ของ RDD จริง (ใช้แทน week number

        # เพื่อรองรับการเปรียบเทียบข้ามปีได้ถูกต้อง เช่น order FG ปี 2027)

        rdd_idx = None  # row index ใน calendar_week สำหรับ comparison

        fg_week_int = None  # week number (1-53) สำหรับ output/display เท่านั้น

        fg_week_num = None  # FG week number (1-53) สำหรับ FG constraint check

        if pd.notna(fg_week):

            fg_week_str = str(int(fg_week))

            if len(fg_week_str) >= 6:

                fg_year = int(fg_week_str[:4])

                fg_week_num = int(fg_week_str[4:])

            elif len(fg_week_str) == 5:

                fg_year = int(fg_week_str[:4])

                fg_week_num = int(fg_week_str[4:])

            elif len(fg_week_str) <= 2:  # รูปแบบ WW (เช่น 13) → ใช้ปีปัจจุบัน

                fg_year = TODAY.year

                fg_week_num = int(fg_week_str)

            else:

                fg_year = TODAY.year

                fg_week_num = int(fg_week)

            # หา row index ใน calendar_week ด้วย YEAR + WEEK (รองรับข้ามปี)

            _fg_row = calendar_week[

                (calendar_week["YEAR"] == fg_year) & (calendar_week["WEEK_NUM"] == fg_week_num)

            ]

            _fg_raw_idx = None

            if not _fg_row.empty:

                _fg_raw_idx = int(_fg_row.index[0])

            else:

                # FALLBACK: FG week ไม่มีในปฏิทิน เพราะปฏิทินบริษัท "ยุบ" สัปดาห์หยุดยาว
                # รวมเข้าสัปดาห์ถัดไป (เช่น ปี 2026 ไม่มี W16 → รวมใน W17 สงกรานต์,
                # ไม่มี W31 → รวมใน W32 หยุดบริษัท) แต่ฝ่ายขายยังกรอก FG = 202631 ได้
                # เดิมเงียบ ๆ ปล่อย rdd_idx = None → order หลุด logic RDD ทั้งชุด
                # (ไม่คำนวณเครื่อง, ไม่ทำ backward, ใช้ spare cylinder ไม่ได้)
                # วิธีแก้: แปลง (year, week) กลับเป็นวันที่จริง แล้วหาสัปดาห์ที่ครอบวันนั้น
                # สัปดาห์ของบริษัทเริ่มวันศุกร์ = ISO Monday ของสัปดาห์นั้น - 3 วัน
                try:

                    _fg_anchor = pd.Timestamp(date.fromisocalendar(fg_year, fg_week_num, 1)) - pd.Timedelta(days=3)

                    _fg_hit = calendar_week.index[
                        (calendar_week["WEEK_START"] <= _fg_anchor)
                        & (calendar_week["WEEK_END"] >= _fg_anchor)
                    ]

                    if len(_fg_hit) > 0:

                        _fg_raw_idx = int(_fg_hit[0])

                        print(
                            f"[FG WEEK FALLBACK] {item} SC {sc_so_no}: FG {fg_year}W{fg_week_num} "
                            f"ไม่มีในปฏิทิน (สัปดาห์ถูกยุบรวม) → ใช้สัปดาห์ที่ครอบ "
                            f"{_fg_anchor.date()} = idx {_fg_raw_idx} "
                            f"(W{int(calendar_week.iloc[_fg_raw_idx]['WEEK'])})"
                        )

                except (ValueError, TypeError):

                    pass  # ISO ไม่มีสัปดาห์นี้จริง (เช่น W53 ของปีที่มี 52 สัปดาห์) → คง None

                if _fg_raw_idx is None:

                    # กำหนดส่งอยู่นอกช่วงปฏิทิน — ปฏิทินเริ่มต้นปีปัจจุบัน แต่ booking มีใบค้าง
                    # ส่งจากปีก่อน (เช่น SO 706596 FG 2025W45) เดิมปล่อย rdd_idx=None แปลว่า
                    # "ไม่รู้ deadline" → ไม่ทำ backward, ไม่เข้าเส้นทาง urgent แล้วงานที่ค้างส่ง
                    # มานานไปกองปลายปี (พบจริง: วางไว้ W42–W46 ทั้งที่เลยกำหนดมาเกือบปี)
                    # ก่อนปฏิทิน = เลยกำหนดแล้ว → ยึดสัปดาห์แรก (urgent วางเร็วสุดเท่าที่ทำได้)
                    # หลังปฏิทิน = ไกลเกินขอบ → ยึดสัปดาห์สุดท้าย
                    _fg_yw_num = fg_year * 100 + fg_week_num
                    _cal_lo_yw = int(calendar_week.iloc[0]["WEEK"])  # WEEK เป็น YW แล้ว
                    _fg_raw_idx = 0 if _fg_yw_num < _cal_lo_yw else len(calendar_week) - 1
                    print(
                        f"⚠️  [FG WEEK CLAMP] {item} SC {sc_so_no}: FG {fg_week} อยู่นอกช่วงปฏิทิน "
                        f"→ ยึด W{int(calendar_week.iloc[_fg_raw_idx]['WEEK'])} "
                        f"({'เลยกำหนดส่งแล้ว' if _fg_raw_idx == 0 else 'ไกลเกินขอบปฏิทิน'})"
                    )

            if _fg_raw_idx is not None:

                # คำนวณ offset (FOB_TYPE / DOUBLE-30 เกจ 16-19 / SINGLE yarn เส้นเดียว)

                offset_weeks = target_offset_weeks_for_row(order)



                # หัก offset สัปดาห์ (RDD = FG Week - offset ตาม FOB_TYPE) ด้วย index arithmetic

                rdd_idx = max(0, _fg_raw_idx - offset_weeks)

                fg_week_int = int(calendar_week.iloc[rdd_idx]["WEEK"])  # สำหรับ display

        # LAB-DIP: deadline = TODAY + 2 weeks (ต้องเสร็จภายใน week ที่เริ่มผลิต)

        if order_type == "LAB-DIP":

            rdd_idx = min(len(calendar_week) - 1, TODAY_IDX + FREEZE_WEEKS)

            fg_week_int = int(calendar_week.iloc[rdd_idx]["WEEK"])  # อัพเดท display

        if rdd_idx is not None and rdd_idx < TODAY_IDX:

            # RDD ผ่านไปแล้ว = URGENT!

            # สำหรับ urgent order ต้องใช้ความสามารถสูงสุด

            # อาจจะต้องเพิ่มเครื่อง แต่ต้องไม่เกิน job/day capacity

            # urgent_mode = True  # DISABLED

            urgent_mode = False

        else:

            urgent_mode = False



        # ----------------------

        # determine order week based on order type



        # ----------------------

        # YD-ORDERS: รู้วันย้อมด้ายเสร็จจริงหรือไม่ — ตัวนี้ตัดสินว่าจะใช้พื้น "หลังย้อมเสร็จ"
        # (กฎของ YD เอง) หรือต้องถอยไปใช้พื้น POINT DATE (วันด้ายเข้า) แทน
        # งาน YD ที่ยกมาจาก booking อาจไม่มี DYE_END_DATE เลย (view booking ไม่มีฟิลด์นี้ —
        # Replan เติมให้เท่าที่จับคู่ใบ SC ได้) → ห้ามปล่อยให้เหลือพื้นแค่ TODAY+2
        _yd_dye_known = False

        if order_type == "LAB-DIP":

            # LAB-DIP: +2 weeks from planning date (TODAY) และต้องเสร็จภายใน week นั้น

            if TODAY_IDX + FREEZE_WEEKS < len(calendar_week):

                order_week = calendar_week.iloc[TODAY_IDX + FREEZE_WEEKS]["WEEK"]

            else:

                _skip_qty_short.append(_unplanned_row(
                    order, "ปฏิทินไม่มีข้อมูลถึง TODAY+2 สัปดาห์ (LAB-DIP)", qty=qty_left
                ))

                continue



        elif order_type in ("SC-ORDERS", "ผ้าเติม"):

            # SC-ORDERS / ผ้าเติม: เริ่มวางแผนที่ TODAY+2 โดยปกติ

            # แต่ถ้า RDD ผ่านไปแล้ว → เริ่มผลิตทันทีที่ TODAY_WEEK (urgent)

            if rdd_idx is not None and rdd_idx < TODAY_IDX:

                # RDD ผ่านไปแล้ว = URGENT! เริ่มผลิตทันที

                order_week = calendar_week.iloc[TODAY_IDX]["WEEK"]

                urgent_mode = True  # เปิด urgent mode เพื่อใช้ความจุสูงสุด

                print(f"[URGENT RDD] {item} SC {sc_so_no}: RDD passed (Week {calendar_week.iloc[rdd_idx]['WEEK']}) → start TODAY Week {order_week}")

            elif TODAY_IDX + FREEZE_WEEKS < len(calendar_week):

                order_week = calendar_week.iloc[TODAY_IDX + FREEZE_WEEKS]["WEEK"]

            else:

                _skip_qty_short.append(_unplanned_row(
                    order, "ปฏิทินไม่มีข้อมูลถึง TODAY+2 สัปดาห์ (SC-ORDERS)", qty=qty_left
                ))

                continue



        elif order_type == "YD-ORDERS":

            yd_week = get_week_from_date(order["DYE_END_DATE"])

            if yd_week is not None:

                _yd_dye_known = True

                order_week = next_week(yd_week)  # +1 week หลังวันย้อมเสร็จ

            else:

                # ถ้าไม่มี DYE_END_DATE ให้ใช้ FG Week แทน (fallback)

                if pd.notna(fg_week) and fg_week is not None:

                    fg_week_str = str(int(fg_week)).strip()

                    if len(fg_week_str) >= 6:

                        year = int(fg_week_str[:4])

                        week = int(fg_week_str[4:])

                    elif len(fg_week_str) == 5:

                        year = int(fg_week_str[:4])

                        week = int(fg_week_str[4:])

                    else:

                        year = TODAY.year

                        week = int(fg_week_str)

                    idx = calendar_week[

                        (calendar_week["YEAR"] == year) & (calendar_week["WEEK_NUM"] == week)

                    ].index

                    if len(idx) > 0 and idx[0] >= 3:

                        order_week = int(calendar_week.iloc[idx[0] - 3]["WEEK"])

                    else:

                        order_week = None

                else:

                    order_week = None

        else:

            _skip_qty_short.append(_unplanned_row(
                order, f"ORDER_TYPE '{order_type}' ไม่รองรับ (routing ไม่ครอบคลุม)", qty=qty_left
            ))

            continue



        if order_week is None:

            _skip_qty_short.append(_unplanned_row(
                order, "คำนวณสัปดาห์เริ่มผลิตไม่ได้ (order_week=None)", qty=qty_left
            ))

            continue



        # ❗ ห้ามวางย้อนหลัง - เริ่มเร็วสุดที่ TODAY+2

        start_idx = TODAY_IDX + FREEZE_WEEKS

        # ❗ POINT DATE (เฉพาะรอบ replan): ห้ามเริ่มทอก่อนสัปดาห์ที่ด้ายเข้าครบ
        # มีเฉพาะงานที่ยกมาจาก booking — SC ไม่มีคอลัมน์นี้ → _point_idx = None = ไม่บังคับ
        #
        # YD-ORDERS ก็ต้องโดนพื้นนี้ด้วย (ไม่มีข้อยกเว้น): ด้ายที่ย้อมแล้วต้องมาถึงโรงทอก่อน
        # ถึงจะทอได้ — ย้อมเสร็จแต่ของยังไม่เข้าก็ทำอะไรไม่ได้ ; PO_IN_DATE ของ booking เป็น
        # แหล่งเดียวที่มีครบทุกแถว (ตรวจจริง 23/23) ต่างจากวันย้อมที่ใบซึ่ง confirm ไปแล้ว
        # หลุดจาก view_sc จนหาไม่เจอ
        # ถ้าใบไหนมี DYE_END_DATE ด้วย ตัวที่ช้ากว่าจะชนะเอง — ด่าน YD (order_week) ที่อยู่
        # ถัดลงไปจะดันขึ้นอีกชั้นถ้าวันย้อมช้ากว่าวันด้ายเข้า
        # ปิดกติกาที่หน้า Monitor Yarn (POIN_ON) = ไม่ดูวันด้ายเข้าเลย เหลือพื้น TODAY+2 / วันย้อม
        # ใบที่ Replan รอบสองให้ยืมด้ายในสต๊อก (YARN_STOCK) = ด้ายมีแล้ว ไม่ต้องรอ PO IN
        _yarn_stock = yarn_stock_note(order)
        _point_idx = (
            get_point_date_start_idx(order.get("POINT_DATE"))
            if REPLAN_MODE and POIN_ON and not _yarn_stock else None
        )
        if REPLAN_MODE and POIN_ON and _yarn_stock:
            print(f"[YARN STOCK] {item} SC {sc_so_no}: {_yarn_stock} → ไม่รอ PO IN")
        if REPLAN_MODE and order_type == "YD-ORDERS" and not _yd_dye_known:
            print(
                f"[YD] {item} SC {sc_so_no}: ไม่มีวันย้อมด้ายในข้อมูล "
                + ("→ ยึดวันด้ายเข้า (POINT DATE) เป็นพื้นอย่างเดียว" if POIN_ON
                   else "และปิดกติกาวันด้ายเข้าไว้ (หน้า Monitor Yarn) → ไม่มีพื้นจากด้าย")
            )
        if _point_idx is not None:
            _point_idx = min(_point_idx, len(calendar_week) - 1)
            if _point_idx > start_idx:
                print(
                    f"[POINT DATE] {item} SC {sc_so_no}: ด้ายเข้า "
                    f"{pd.to_datetime(order.get('POINT_DATE'), errors='coerce'):%d/%m/%Y} → เริ่มเร็วสุด "
                    f"W{int(calendar_week.iloc[_point_idx]['WEEK'])} "
                    f"(จาก W{int(calendar_week.iloc[start_idx]['WEEK'])})"
                )
                start_idx = _point_idx
        # ❗ ด้ายพร้อม (เฉพาะรอบ replan): Replan แบ่งด้ายที่มี (สต๊อก + PO Pending) ให้ใบก่อนวาง
        # ก้อนที่ได้ด้ายจาก PO ที่ยังไม่เข้ามี YARN_READY → ห้ามวางก่อนสัปดาห์นั้น (แผนด้ายจะได้ไม่ติดลบ)
        # ไม่ข้ามด้วยป้าย YARN_STOCK: ยืมสต๊อกแทน PO ของตัวเองได้ แต่ด้ายเส้นที่ต้องใช้จากกองกลางยังมาไม่ถึง
        _yr_idx = yarn_ready_idx(order) if REPLAN_MODE else None
        if _yr_idx is not None:
            _yr_idx = min(_yr_idx, len(calendar_week) - 1)
            if _yr_idx > start_idx:
                print(
                    f"[YARN READY] {item} SC {sc_so_no}: ด้ายมาครบ W{int(calendar_week.iloc[_yr_idx]['WEEK'])} "
                    f"→ เริ่มเร็วสุด W{int(calendar_week.iloc[_yr_idx]['WEEK'])} "
                    f"(จาก W{int(calendar_week.iloc[start_idx]['WEEK'])})"
                )
                start_idx = _yr_idx
            # รวมเข้าพื้นวันด้ายเข้า (_point_idx) — กลไกที่ดึงงานกลับมาเริ่มก่อน (ทอต่อเนื่องบนเครื่องอุ่น / backward /
            # ramp / จ้างทอ) เช็ก _point_idx ซ้ำอีกราว 10 จุด ; แก้แค่ start_idx ไม่พอ — ทดสอบข้อมูล server 15 ต.ค. เจอ
            # SC 722978 FD6TECNF47A0 ด้ายมา W45 แต่ถูกดึงกลับไปวาง W44 (แผนด้ายติดลบ)
            _point_idx = _yr_idx if _point_idx is None else max(_point_idx, _yr_idx)
        # พื้นแข็งของใบ (TODAY+2 / ด้ายเข้า) — start_idx ถูกขยับต่อด้วยเหตุผลอื่น (backward / FG ก่อนหน้า)
        # จึงเก็บแยกไว้ให้ _swap_color_same_item รู้ว่าย้ายยอดของใบนี้ขึ้นมาได้เร็วสุดสัปดาห์ไหน
        _hard_floor_idx = start_idx

        # รีเซ็ต anchor ของ week_index ทุก order ใหม่ (กันค่าค้างจาก order ก่อนหน้า
        # ที่แผนอาจเดินไปไกลข้ามปี) — จากนี้ anchor จะเลื่อนตาม plan_week เอง
        set_week_anchor(start_idx)

        plan_week = int(calendar_week.iloc[start_idx]["WEEK"])

        # YD-ORDERS: ห้ามวางก่อนย้อมเสร็จ — plan_week ต้องไม่ก่อน order_week (next_week หลัง DYE_END_DATE)
        if order_type == "YD-ORDERS" and order_week is not None:
            _yd_start_idx = week_index(order_week)
            if _yd_start_idx is not None and _yd_start_idx > start_idx:
                start_idx = _yd_start_idx
                plan_week = order_week



        # warm-carry flag: True เมื่อ plan_week ถูก "ดึงกลับ" มาเริ่มเร็วขึ้นเพื่อใช้เครื่องที่ยังอุ่น
        # (WARM GAP FILL / FG CONTINUATION) → backward/JIT ต้องไม่ shift ทับ เพราะ warm-carry ชนะ
        _warm_carry_applied = False

        # ❗ ถ้า booking ของ item+mc_group นี้ยังวิ่งถึง week ≥ plan_week → จัดการ 2 กรณี:

        # กรณี 1: plan_week == last booking week → plan ลงได้ใน week เดียวกัน แต่จำกัด qty ตาม remaining cap

        #          (remaining_week_cap ถูก seed ไว้แล้วจาก booking_remain_by_mc)
        #          ไม่ต้อง push plan_week ออก เพราะ cross-SC fill จะจัดการ qty เอง
        # กรณี 2: last booking week > plan_week → push ออกไปหลัง booking (ถ้ายังทัน RDD)
        _bk_mc_grp = str(order.get("MC GROUP", "")).strip().upper()
        if _bk_mc_grp:
            _bk_last_idx = last_production.get(_ck(item, _bk_mc_grp))
            if _bk_last_idx is not None and _bk_last_idx >= start_idx:
                _bk_last_week = int(calendar_week.iloc[_bk_last_idx]["WEEK"])
                _after_bk_idx = _bk_last_idx + 1
                if plan_week == _bk_last_week:
                    # plan_week == last booking week: ปล่อยให้ plan_week อยู่ตรงนั้น
                    # cross-SC fill จะใช้ remaining_week_cap ที่ seed ไว้ (idle capacity หลัง old item)
                    pass
                elif _after_bk_idx < len(calendar_week):
                    # plan_week < last booking week: push ออกไปหลัง booking ถ้าทัน RDD
                    if rdd_idx is None or _after_bk_idx <= rdd_idx:
                        start_idx = _after_bk_idx
                        plan_week = int(calendar_week.iloc[start_idx]["WEEK"])

                    # ถ้าขยับแล้วไม่ทัน RDD → ใช้ start_idx เดิม (ผลิตซ้อน booking ได้)
            elif _bk_last_idx is not None:
                # _bk_last_idx < start_idx: booking จบก่อน plan_week
                # ถ้าเครื่องยังอุ่น (gap ≤ SETUP_GAP_WEEK) และกำลังวิ่งอยู่ → pull plan_week กลับมา carry ต่อ
                _bk_warm_key = _ck(item, _bk_mc_grp)
                _bk_mc_running = machines_in_use.get(_bk_warm_key, 0)
                _bk_pull_plan_idx = week_index(plan_week)
                if _bk_pull_plan_idx is not None:
                    _bk_warm_gap = _bk_pull_plan_idx - _bk_last_idx
                    if (
                        1 < _bk_warm_gap <= SETUP_GAP_WEEK
                        and _bk_mc_running > 0
                    ):
                        # ❗ floor: ห้ามดึงกลับเร็วกว่า TODAY+2 (สัปดาห์ที่ผลิตจริงได้)
                        # และ (รอบ replan) ห้ามเร็วกว่าสัปดาห์ที่ด้ายเข้าครบ — เครื่องอุ่นแต่ไม่มีด้ายก็ทอไม่ได้
                        _bk_pull_idx = max(_bk_last_idx + 1, TODAY_IDX + FREEZE_WEEKS, _point_idx or 0)
                        if _bk_pull_idx < len(calendar_week) and _bk_pull_idx < _bk_pull_plan_idx:
                            _bk_pull_week = int(calendar_week.iloc[_bk_pull_idx]["WEEK"])
                            if _bk_pull_week >= TODAY_WEEK:
                                print(
                                    f"[WARM GAP FILL] {item}: machine last active W{int(calendar_week.iloc[_bk_last_idx]['WEEK'])} "
                                    f"→ pull plan_week W{plan_week}→W{_bk_pull_week} "
                                    f"(gap={_bk_warm_gap}, mc_running={_bk_mc_running}, no setup needed)"
                                )
                                start_idx = _bk_pull_idx
                                plan_week = _bk_pull_week
                                _warm_carry_applied = True  # warm-carry ชนะ backward

        # 🔧 FIX: Skip weeks where OLD bookings already exist for same item/MC/Gauge
        # to prevent exceeding capacity that old bookings already use
        _plan_idx = week_index(plan_week)
        if _plan_idx is not None and _bk_mc_grp:
            # Get gauge for this item/MC_GROUP
            _cap_row_check = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item) &
                (item_cap_data["MC_GROUP"] == _bk_mc_grp)
            ]
            _plan_gauge = None
            if not _cap_row_check.empty:
                _plan_gauge = str(_cap_row_check.iloc[0].get("GUAGE", "")).strip()
            _plan_ck = _ck(item, _bk_mc_grp, _plan_gauge) if _plan_gauge else None

            # Check if OLD booking exists in plan_week and skip to next week
            if _plan_ck and _plan_ck in booking_mc_by_week and booking_mc_by_week[_plan_ck].get(_plan_idx, 0) > 0:
                print(f"[SKIP OLD BOOKING] {item}+{plan_week}+{_bk_mc_grp}: OLD booking already using machines, skip to next week")
                # Skip to next week (max 10 weeks to prevent infinite loop)
                _max_skip = 10
                _skipped = 0
                while (_plan_idx is not None and 
                       _plan_ck in booking_mc_by_week and 
                       booking_mc_by_week[_plan_ck].get(_plan_idx, 0) > 0 and
                       _skipped < _max_skip):
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break
                    _plan_idx = week_index(plan_week)
                    _skipped += 1

                if plan_week is None:
                    continue
        # ❗ ถ้า SC/SO+Item เดิมเคยวาง FG_WEEK ก่อนหน้าแล้ว → ให้เริ่มจาก week ที่ FG แรกเสร็จ
        # เพื่อให้ FG ที่ 2 สามารถ carry over ใช้ capacity ที่เหลือจาก FG แรกได้
        # ยกเว้น: ถ้า item เดียวกัน มี cap เหลือจาก week นั้น → เริ่มใน week เดิมได้
        # ⚠️ แต่ถ้าต่อท้ายแล้วไม่ทัน RDD → อนุญาตให้ผลิตซ้อน FG ก่อนหน้าได้
        # 🔧 FIX: อนุญาตให้ FG ที่ 2 carry over ไปใช้ capacity ที่เหลือจาก FG แรก
        _prev_fg_idx = _last_fg_plan_idx.get((sc_so_no, item))
        if _prev_fg_idx is not None:
            _prev_fg_week = int(calendar_week.iloc[_prev_fg_idx]["WEEK"])
            # ตรวจว่ามี remaining cap ใน week นั้นสำหรับ item นี้หรือไม่
            # หมายเหตุ: remaining_week_cap จะถูกสร้างขึ้นหลังจาก FG แรกผลิตเสร็จ
            # ดังนั้น FG ที่ 2 สามารถใช้ capacity ที่เหลือได้ผ่าน cross-SC carryover logic (line 2802-2907)
            _item_mcs = set(
                str(r).strip().upper()
                for r in item_cap_data.loc[
                    item_cap_data["ITEM_CODE"] == str(item).strip().upper(), "MC_GROUP"
                ]
            )
            # ตรวจ remaining cap ใน ทุก week ตั้งแต่ start_idx จนถึง _prev_fg_idx
            # (ไม่ใช่แค่ week สุดท้ายของ FG แรก เพราะ FG แรกอาจทิ้ง cap ไว้ใน week กลางๆ)
            # 🔧 FIX: รวม _prev_fg_idx ในการตรวจด้วย แม้ _prev_fg_idx < start_idx
            # เพราะ target_start_idx = rdd_idx + 1 อาจข้าม week ที่ FG ก่อนหน้าจบ (มี remaining cap)
            _rem_check_start = min(start_idx, _prev_fg_idx)
            _has_rem_cap = any(
                remaining_week_cap.get((int(calendar_week.iloc[_wi]["WEEK"]), item, _mc), 0) > 0
                for _wi in range(_rem_check_start, _prev_fg_idx + 1)
                if _wi < len(calendar_week)
                for _mc in _item_mcs
            )
            if _has_rem_cap:
                # 🔧 FIX: เริ่ม FG ถัดไปจาก week สุดท้ายของ FG ก่อนหน้า (_prev_fg_idx)
                # ห้ามเริ่มจาก week กลางๆ ที่มี remaining cap จาก rev_weight rounding
                # เพราะ FG ก่อนหน้ายังผลิตไม่จบ → ต้องรอให้จบก่อนค่อย carry ต่อ
                _prev_fg_w = int(calendar_week.iloc[_prev_fg_idx]["WEEK"])
                _has_rem_at_last = any(
                    remaining_week_cap.get((_prev_fg_w, item, _mc), 0) > 0
                    for _mc in _item_mcs
                )
                if _has_rem_at_last:
                    # FG ก่อนหน้าจบใน week นี้และมี cap เหลือ → เริ่ม FG ถัดไปที่นี่
                    start_idx = _prev_fg_idx
                    plan_week = _prev_fg_w
                    _warm_carry_applied = True  # warm-carry ชนะ backward
                    print(
                        f"[FG CONTINUATION] {item}: FG ถัดไป start at W{_prev_fg_w} "
                        f"(remaining cap from previous FG last week)"
                    )
                else:
                    # FG ก่อนหน้าใช้ cap หมดใน week สุดท้าย → เริ่ม week ถัดไป
                    _after_prev = _prev_fg_idx + 1
                    if _after_prev < len(calendar_week):
                        start_idx = _after_prev
                        plan_week = int(calendar_week.iloc[_after_prev]["WEEK"])
                        _warm_carry_applied = True  # warm-carry ชนะ backward
                        print(
                            f"[FG CONTINUATION] {item}: FG ถัดไป start at W{plan_week} "
                            f"(next week after previous FG finished at W{_prev_fg_w})"
                        )
            else:
                if _prev_fg_idx >= start_idx:
                    # FG แรกเสร็จหลัง start_idx ของ FG ที่ 2
                    # สำหรับ SC-ORDERS ให้เริ่มจาก earliest feasible week เดิม
                    # ไม่บังคับรอ FG ก่อนหน้าจบทั้งก้อน เพื่อให้ทอได้ไวขึ้น (เช่นเริ่มที่ W19)
                    if order_type in ("SC-ORDERS", "ผ้าเติม"):
                        print(
                            f"[KEEP EARLIEST START] {item}: keep start week {int(calendar_week.iloc[start_idx]['WEEK'])} "
                            f"instead of pushing to previous FG week {_prev_fg_week}"
                        )
                    # non SC-ORDERS ยังใช้กฎเดิม
                    elif rdd_idx is not None and _prev_fg_idx > rdd_idx:
                        pass  # ไม่ push — ใช้ start_idx เดิม (อนุญาต overlap กับ FG แรก)
                    else:
                        start_idx = _prev_fg_idx
                        plan_week = _prev_fg_week
                # else: ใช้ start_idx เดิม (FG ที่ 2 เริ่มก่อน FG แรกเสร็จ → ผลิตซ้อนกันได้)


        # YD-ORDERS: re-clamp ห้ามวางก่อนวันย้อมเสร็จ (order_week = next_week หลัง DYE_END_DATE)
        # gate แรก (บรรทัด ~7826) อาจถูก override โดย pull-back ทีหลัง:
        # WARM GAP FILL / FG CONTINUATION / booking pull-back → ดึง plan_week กลับมาก่อน order_week
        # โดยไม่เช็ควันนัดย้อม ทำให้แผนวางก่อนย้อมเสร็จ จึงต้อง clamp ซ้ำครั้งสุดท้าย
        if order_type == "YD-ORDERS" and order_week is not None:
            _yd_floor_idx = week_index(order_week)
            _yd_cur_idx = week_index(plan_week)
            if _yd_floor_idx is not None and _yd_cur_idx is not None and _yd_cur_idx < _yd_floor_idx:
                start_idx = _yd_floor_idx
                plan_week = order_week

        # POINT DATE: re-clamp ครั้งสุดท้าย ด้วยเหตุผลเดียวกับ YD ข้างบน —
        # WARM GAP FILL / FG CONTINUATION / booking pull-back ดึง plan_week กลับมาได้โดยไม่ดูวันด้ายเข้า
        if _point_idx is not None:
            _pt_cur_idx = week_index(plan_week)
            if _pt_cur_idx is not None and _pt_cur_idx < _point_idx:
                _pt_week = int(calendar_week.iloc[_point_idx]["WEEK"])
                print(f"[POINT DATE CLAMP] {item}: W{plan_week}→W{_pt_week} (ด้ายยังไม่เข้า)")
                start_idx = _point_idx
                plan_week = _pt_week
                set_week_anchor(_point_idx)

        # ----------------------
        # weekly allocation with best machine selection
        # ----------------------
        # คำนวณจำนวนเครื่องที่ต้องการตั้งแต่แรก (ถ้าทัน RDD)
        required_machines_info = None
        # S9 Logic: per-order flags
        _s9_active = False   # True เมื่อ S9 routing ถูก activate สำหรับ order นี้
        _s9_pool_id_cur = None  # กองเครื่องจ้างทอ (GRP|CAT|เกจ|POLY) ที่ order นี้ลง — ตั้งตอน activate
        # CAT + ชนิดเส้นที่ใช้จับกอง — เก็บไว้ให้ตอนแตกแถวรายผู้รับจ้างใช้ชุดเดียวกับตอนคำนวณ
        _s9_cat_cur = ""
        _s9_mat_cur = ""
        _s9_no_cap_weeks = set()  # weeks ที่ S9 ลองแล้วได้ 0 เครื่อง → ห้าม S9 reset กลับ week เหล่านี้
        _s9_auto_blocked_logged = False  # กัน log ซ้ำทุก iteration เมื่อ S9 pool ไม่ว่าง
        _s9_week_locked = False  # True = plan_week ถูก set ไปสัปดาห์เริ่มของจ้างทอแล้ว → ครั้งถัดไปไม่ reset กลับ
        # คำนวณ setup days ล่วงหน้า (ใช้ใน calculate_required_machines ด้วย)
        order_material_content = str(order.get("MATERIAL_CONTENT", "")).strip() or _material_content_lookup.get(str(item).strip().upper(), "")
        _order_yarn_used = str(order.get("YARN-USED", "") or order.get("YARN_USED", "") or order.get("YARN_ITEM", "") or _yarn_used_lookup.get(str(item).strip().upper(), "")).strip()
        order_setup_days = get_setup_days_for_item(order_material_content, _order_yarn_used, item_code=item)
        # ❗ ตรวจสอบว่า item นี้มี cap data หรือไม่ — ถ้าไม่มีให้ข้ามทันที
        #   ผ้า S9 Only ไม่ใช้ CAP ทอในบ้าน → ข้ามด่านนี้ ไปตัดสินด้วย Cap ของชีท S9 Only ข้างล่าง
        _item_cap_rows = item_cap_data[item_cap_data["ITEM_CODE"] == str(item).strip().upper()]
        if _item_cap_rows.empty and not _s9o_item:
            _skip_no_cap.append(
                _unplanned_row(order, "ไม่พบ CAP data (item_cap2025.xlsx)", qty=qty_left)
            )
            print(f"⚠️  ไม่พบ CAP data สำหรับ item '{item}' (SC/SO:{sc_so_no}) → ข้ามการวางแผน")
            continue
        # S9 Only (user 24 ก.ย. 2569): ต้องจ้างทอเท่านั้น ข้อมูลในชีท S9 Only ไม่พอ = ห้ามทำแผน
        # (เดิมหา Cap/ผู้รับจ้างไม่เจอแล้วตกกลับไปทอในบ้านเงียบ ๆ) — ด่านที่ตัดสินได้ตั้งแต่ต้นใบ
        _s9o_spec = _s9_only_spec.get(str(item).strip().upper())
        _s9o_stop = ""   # เหตุผลที่ต้องหยุดใบนี้กลางลูป (ใช้ตอนเขียน UNPLANNED ท้ายใบ)
        if _s9o_spec is not None:
            _s9o_why = ""
            if not _s9o_spec["cap"]:
                _s9o_why = "S9 Only ไม่มี Cap/Day (กรอกที่หน้าจ้างทอ › S9 Only)"
            elif not _s9o_spec["gauge"]:
                _s9o_why = "S9 Only ไม่มี Guage (กรอกที่หน้าจ้างทอ › S9 Only)"
            elif not _s9o_spec["cat"]:
                # CAT = ขนาดกระบอก ห้ามเดา/ห้ามเอาของเครื่องในบ้านมาแทน (user 24 ก.ย. 2569)
                _s9o_why = "S9 Only ไม่มี CAT (กรอกที่หน้าจ้างทอ › S9 Only)"
            elif not _s9_pool_rows(_s9o_spec["gauge"], _s9o_spec["cat"], order_material_content):
                _s9o_why = (
                    f"S9 Only: ไม่มีผู้รับจ้างที่รับ Guage {_s9o_spec['gauge']} CAT {_s9o_spec['cat']}"
                    f" ผ้า {_s9_material_class(order_material_content)} (ชีท MC S9)"
                )
            if _s9o_why:
                _skip_no_cap.append(_unplanned_row(order, _s9o_why, qty=qty_left, gauge=_s9o_spec["gauge"]))
                print(f"⚠️  [S9 ONLY] {item} (SC/SO:{sc_so_no}): {_s9o_why} → ไม่ทำแผน")
                continue


        # คำนวณ machine allocation ล่วงหน้า
        progressive_plan = None  # {week: machines} สำหรับแต่ละ week
        # สัปดาห์แรกที่ progressive_plan "ผลิตจริง" (mc > 0) = จุดเริ่ม backward/JIT
        # ห้ามใช้ min(progressive_plan.keys()) เพราะ dict มี key ของสัปดาห์ที่ mc=0 อยู่ด้วย
        # (จะได้ plan_week เดิมเสมอ) และ min() บน week number พังเมื่อแผนคร่อมปี
        _prog_start_week = None
        # Initialize core production schedule variable before use
        _core_production_schedule = []
        # 🔧 FIX: รวม Pending Plan ของทุก FG สำหรับ item เดียวกัน (ทุก SC/SO)
        # เพื่อให้คำนวณจำนวนเครื่องจาก total demand ไม่ใช่แค่ FG ปัจจุบัน
        # ป้องกันการลดเครื่องเมื่อ FG หนึ่งจบ ทั้งที่ยังมี FG ถัดไปรอผลิต
        _total_qty_all_fg = get_total_pending_qty_for_item(item, sc_so_no, fg_week, orders_sorted)
        _qty_for_machine_calc = max(qty_left, _total_qty_all_fg)
        _initial_qty_for_mc_calc = qty_left  # qty_left ตอนเริ่ม order — ใช้คำนวณ adjusted demand
        # demand เฉพาะ SC ปัจจุบัน (ไม่รวม SC อื่นของ item เดียวกัน) — แสดงใน log [TOTAL FG QTY] ข้างล่าง
        _total_qty_same_sc = get_total_pending_qty_for_item(item, sc_so_no, fg_week, orders_sorted, same_sc_only=True)
        if _total_qty_all_fg > qty_left:
            print(f"[TOTAL FG QTY] {item} SC {sc_so_no}: qty_left={qty_left:.0f}, total_all_fg={_total_qty_all_fg:.0f}, same_sc={_total_qty_same_sc:.0f} → ใช้ {_qty_for_machine_calc:.0f} สำหรับคำนวณเครื่อง")
        if (rdd_idx is not None and rdd_idx >= week_index(plan_week)) or _core_production_schedule:
            _locked_mc = locked_mc_group_for.get((sc_so_no, item))
            if _locked_mc:
                _locked_mc_setup = get_setup_days_for_item(order_material_content, _order_yarn_used, mc_group=_locked_mc, item_code=item)
                if _locked_mc_setup != order_setup_days:
                    order_setup_days = _locked_mc_setup
            print(f"[DEBUG START] Processing order: {sc_so_no}, item: {item}, qty: {order_qty}, FG week: {fg_week}")


            # คำนวณจำนวนเครื่องที่ต้องการสำหรับ item นี้ (เพื่อจบก่อน TARGET_KNIT/RDD)
            # ใช้ rdd_idx เป็น target เพราะต้องทอเสร็จก่อน RDD (FG Week - offset ตาม FOB_TYPE)
            target_knit_idx = rdd_idx
            # CORE ITEM: ไม่สน RDD — ใช้ plan_week+5 เป็น target
            if _core_production_schedule:
                target_knit_idx = week_index(plan_week) + 5
            mc_group_calc, daily_cap_calc, required_machines, feasible_calc, _gauge_calc = (
                calculate_required_machines(
                    item,
                    _qty_for_machine_calc,
                    plan_week,
                    target_knit_idx,  # TARGET_KNIT/RDD
                    setup_days=order_setup_days,
                    only_mc_group=_locked_mc,
                    order_type=order_type,
                    sub_color=sub_color,
                    dye_end_date=order.get("DYE_END_DATE"),
                )
            )
            print(f"[DEBUG CALC] {item}: mc_group={mc_group_calc}, required_machines={required_machines}, feasible={feasible_calc}")
            if required_machines:
                required_machines_info = (
                    mc_group_calc,
                    daily_cap_calc,
                    required_machines,
                    feasible_calc,
                    _gauge_calc,
                )
                # ล็อก MC_GROUP highest-cap นี้ไว้ให้ FG Week ถัดไปของ SC/SO+Item เดิมใช้ต่อ
                if mc_group_calc and (sc_so_no, item) not in locked_mc_group_for:
                    locked_mc_group_for[(sc_so_no, item)] = mc_group_calc
                # ถ้าเปิดใช้ progressive reduction → คำนวณเครื่องทุก week ล่วงหน้า
                if USE_PROGRESSIVE_REDUCTION and mc_group_calc and feasible_calc:
                    prog_result = calculate_progressive_reduction(
                        item,
                        _qty_for_machine_calc,
                        plan_week,
                        rdd_idx,
                        mc_group_calc,
                        daily_cap_calc,
                        _gauge_calc,
                        setup_days=order_setup_days,
                        rev_weight=get_revolution_weight_from_orders(item, mc_group_calc)
                    )
                    if prog_result:
                        progressive_plan = {wk: mc for wk, mc in prog_result}
                        # prog_result เรียงตาม week index อยู่แล้ว → tuple แรกที่ mc>0 คือ start จริง
                        _prog_start_week = next((wk for wk, mc in prog_result if mc > 0), None)

        # ============================================================
        # AUTO OUTSOURCE (S9): ทอในบ้านให้เต็มที่ก่อน → เฉพาะยอดที่ล้นเลย TARGET_KNIT ค่อยส่งจ้างทอ
        # ============================================================
        # (user 14 ก.ย. 2569) เดิมตัดสินครั้งเดียวตอนเริ่มใบแล้วส่ง "ทั้งใบ" ออกไป + วัดด้วยยอดรวม
        # ทุกใบของ item → ใบที่ในบ้านทำทันถูกส่งจ้างทอฟุ่มเฟือย (เจอจริง: FD5BASPG10A0 SC 722389
        # วัดด้วย 35,415 กก. ทั้งที่ใบมี 17,707.5 → "ไม่ครบ" แต่พอถึง W44 ในบ้านจบ W49 ทัน target)
        # ตอนนี้: ใบวางในบ้านตามปกติ → ลูปเดินเลย target แล้วยังมียอดค้าง จึงเปิด S9 เฉพาะยอดค้าง
        # แล้วย้อนไปวางช่วงก่อน target ด้วยกำลังเครื่อง S9 (ดู [S9 AUTO TAIL] ในลูป)
        # ด่านที่รู้ได้ตั้งแต่ตอนนี้ — priority / เกจในชีท Item S9 เช็คตอนเปิดจริง
        _s9_auto = False        # เปิดจ้างทอส่วนที่ล้น target แล้ว (เปิดแล้วคงไว้ทั้งใบ)
        _s9_auto_reason = ""    # เหตุผลที่ออกไปข้างนอก — เขียนลง REMARK ให้ตรวจย้อนได้
        _s9_auto_base = (
            AUTO_S9_ENABLED
            and not _outsource_force            # ก้อนที่ user แบ่งเองมีเส้นทางของมันอยู่แล้ว
            and not is_core_item                # core ไม่ผูก RDD (ใช้ plan_week+5) → ไม่มี target ให้วัด
            and rdd_idx is not None
            and item_upper not in _s9_only_items  # S9 Only เข้า S9 อยู่แล้วทุกกรณี
        )
        _s9_auto_tail_idx = None       # สัปดาห์เริ่มของส่วนที่ล้น (ถอยจาก target ด้วยกำลัง S9)
        _s9_auto_skip_logged = False   # กัน log "ล้น target แต่ไม่เข้าเกณฑ์" ซ้ำทุกสัปดาห์
        _s9_cmp_cache = {}             # {(plan_idx, ยอดก้อนนี้, ยอดทั้ง item): (สัปดาห์ที่ในบ้านจบ, สัปดาห์ที่จ้างทอจบ)}
        _s9_cmp_logged = False         # กัน log "จ้างแล้วไม่ได้เร็วกว่า" ซ้ำทุกสัปดาห์
        # ธง "ในบ้านเอาไม่อยู่" (user 21 ก.ย. 2569): ด่านเทียบใช้ภาพพูล ณ วินาทีที่ตัดสิน ซึ่งยังไม่มี
        # งานของใบที่จะถูกวางทีหลัง → พูลเล็กที่ดูว่างตอนนั้นเต็มทีหลัง แล้วงานค้างยาว
        # (เจอจริง: FD1BASRN127A0 พูล DOUBLE-34 เกจ 28 มี 9 เครื่อง — ประเมินว่าจบ W43 แต่ไหลไป W2 ปีหน้า)
        # พอเห็นของจริงว่าเครื่องหลุดมือ/ไม่มีเครื่องให้ใช้ → ยอดที่เหลือข้ามด่านเทียบไปจ้างทอได้เลย
        _inhouse_stuck = False
        _inhouse_stuck_why = ""

        _produced_week = None  # init สำหรับ track FG_WEEK sequential
        _taper_steps_used = 0  # หางลดครึ่ง: ไล่ลงไปแล้วกี่ขั้นในก้อนงานนี้ (เพดาน MC_TAPER_MAX_STEPS)
        _earliest_backshift_done = False  # ป้องกันการย้อนกลับ EARLIEST_PLAN_WEEK ซ้ำใน order เดียวกัน
        # [FORCE START EARLIEST] ถูกลบออก → วางแผนตาม TARGET_KNIT (JIT) แทน
        # ============================================================
        # BACKWARD SCHEDULING (JIT START) — ใช้กับทุก order
        # ============================================================
        # หลักการ: เริ่มผลิต "ช้าที่สุดเท่าที่ยังทอเสร็จทัน RDD พอดี" (ไม่กันเผื่อ)
        #   start_idx = max(backward_idx, TODAY_IDX+2, yd_floor)
        # ลำดับความสำคัญของแหล่งที่มา backward_idx:
        #   1) progressive_plan (ถ้าคำนวณได้) → min(week) คือ optimal start ที่ผ่าน Step 1b มาแล้ว
        #   2) compute_backward_start_idx() → fallback สำหรับ order ที่ progressive_plan คำนวณไม่ได้
        # ข้อยกเว้น: ถ้า warm-carry (WARM GAP FILL / FG CONTINUATION) ดึง plan_week มาแล้ว
        #            → ข้าม backward ทั้งหมด เพราะเครื่องยังอุ่น ไม่ต้อง setup ใหม่ (คุ้มกว่า JIT)
        # สัปดาห์เริ่ม "ก่อน JIT" (ผ่านพื้นแข็ง TODAY+2 / วันด้ายเข้า / booking / warm carry มาแล้ว)
        # งาน core ที่มีรอบผลิตจากสต๊อกใช้ตัวนี้เป็นพื้นแทนสัปดาห์ของ JIT — core ไม่วางตาม target
        # (ดู CORE BATCH INIT)
        _pre_jit_idx = week_index(plan_week) if plan_week is not None else None
        if _warm_carry_applied:
            if progressive_plan:
                print(f"[BACKWARD SKIP] {item}: warm-carry ใช้งานอยู่ → คง plan_week W{plan_week} (ไม่ shift JIT)")
        elif plan_week is not None:
            _bw_idx = None
            _bw_src = ""
            if progressive_plan and _prog_start_week is not None:
                _bw_idx = week_index(_prog_start_week)
                _bw_src = "progressive"
            elif required_machines_info and rdd_idx is not None:
                _bw_mc_grp, _bw_daily_cap, _bw_req_mc, _bw_feasible, _bw_gauge = required_machines_info
                _bw_idx = compute_backward_start_idx(
                    item,
                    _qty_for_machine_calc,
                    rdd_idx,
                    _bw_mc_grp,
                    _bw_daily_cap,
                    _bw_gauge,
                    _bw_req_mc,
                    setup_days=order_setup_days,
                    rev_weight=get_revolution_weight_from_orders(item, _bw_mc_grp),
                    floor_idx=max(TODAY_IDX + FREEZE_WEEKS, _point_idx or 0),
                )
                _bw_src = "backward"

            if _bw_idx is not None:
                # พื้นแข็ง: ห้ามถอยเร็วกว่า TODAY+2, (replan) ก่อนด้ายเข้า และ (YD) ก่อนย้อมเสร็จ
                _bw_floor = max(TODAY_IDX + FREEZE_WEEKS, _point_idx or 0)
                if order_type == "YD-ORDERS" and order_week is not None:
                    _bw_yd_idx = week_index(order_week)
                    if _bw_yd_idx is not None:
                        _bw_floor = max(_bw_floor, _bw_yd_idx)
                _bw_idx = max(_bw_idx, _bw_floor)

                _plan_cur_idx = week_index(plan_week)
                # เลื่อนได้ทางเดียว: ไปข้างหน้าเท่านั้น (ห้ามดึงกลับ — จะทับ push ของ booking/RDD gate)
                if _plan_cur_idx is not None and _bw_idx > _plan_cur_idx and _bw_idx < len(calendar_week):
                    _bw_week = int(calendar_week.iloc[_bw_idx]["WEEK"])
                    print(f"[JIT START/{_bw_src}] {item}: shift plan_week W{plan_week}→W{_bw_week} (TARGET_KNIT W{fg_week_int})")
                    plan_week = _bw_week
                    start_idx = _bw_idx
                    set_week_anchor(_bw_idx)  # backward กระโดดได้หลายสัปดาห์ → ย้าย anchor ตาม
        # Seed remaining_week_cap จาก booking สำหรับ carry-over ในสัปดาห์สุดท้ายของ old booking
        # ให้ new SO สามารถใช้ capacity ที่เหลืออยู่บน machine ของ old item ในสัปดาห์นั้นได้
        # สูตร: (MC_USE_CEIL - MC_USE) × working_days × daily_cap
        # = เครื่องที่ idle × วันทำงาน × cap ต่อวัน → qty ที่ new item สามารถผลิตได้ใน week เดียวกัน
        # --- Core Item trigger week simulation & batch schedule ---
        _core_trigger_week = None
        _core_production_schedule = []
        # สต๊อกที่ใช้ตัดสิน trigger = กองรวมทุกทีมของรหัสผ้านี้ (ดูหมายเหตุตอนสร้าง lookup)
        # ไม่แยกตามลูกค้า/ทีม เพราะผ้ารหัสเดียวกันโยกข้ามทีมมาใช้แทนกันได้
        if is_core_item and item_upper in stock_inventory_lookup:
            _csd = stock_inventory_lookup[item_upper]
            _csm = _csd.get('STOCK_MIN', 0)
            _cs5 = _csd.get('Stock_5_Week', 0)
            if _csm > 0 and _cs5 > 0:
                _core_so_set = set()
                for _, _co in orders_sorted.iterrows():
                    if str(_co.get("Item Code","")).strip().upper() == item_upper:
                        if str(_co.get("Customer","")).strip() in CORE_CUSTOMERS:
                            _core_so_set.add("S" + str(_co.get("SC/SO NO","")).strip().lstrip("S"))
                            _core_so_set.add(str(_co.get("SC/SO NO","")).strip().lstrip("S"))
                # ❗ ห้ามผลิต week ที่น้อยกว่า TODAY_WEEK + 2 ทุกกรณี
                _ccw0 = int(TODAY_WEEK)
                _ccw0 = next_week(_ccw0)  # TODAY_WEEK + 1
                if _ccw0 is not None:
                    _ccw0 = next_week(_ccw0)  # TODAY_WEEK + 2
                if _ccw0 is None:
                    _ccw0 = int(TODAY_WEEK) + 2  # Fallback

                # ของ core ที่วางลงแผนรอบนี้ไปแล้ว (item เดียวกัน ลูกค้า core) → {สัปดาห์: กก.}
                # คิดครั้งเดียวต่อใบ — ใบก่อนหน้าที่วางแล้วคือของที่จะเข้าคลังจริง ใบที่ยังไม่วางไม่นับ
                _placed_core_by_week: dict = {}
                for _pr in plans:
                    if str(_pr.get("ITEM_CODE", "")).strip().upper() != item_upper:
                        continue
                    if (item_upper, _core_ckey(_pr.get("CUSTOMER", ""))) not in itemcore_pairs:
                        continue
                    _pr_q = pd.to_numeric(_pr.get("PRODUCE_QTY", 0), errors="coerce")
                    _pr_w = pd.to_numeric(_pr.get("PLAN_WEEK"), errors="coerce")
                    if pd.isna(_pr_q) or _pr_q <= 0 or pd.isna(_pr_w):
                        continue
                    _placed_core_by_week[int(_pr_w)] = _placed_core_by_week.get(int(_pr_w), 0.0) + float(_pr_q)

                def _sim_core_batches(_hold_from=None):
                    """จำลองสต๊อกรายสัปดาห์ → (trigger week, [(week, batch)], cover ต่ำสุดตอนรอ, log)

                    _hold_from = สัปดาห์ที่ยังไม่อนุญาตให้เริ่มผลิตก่อนถึง (ใช้ตรึงรอบผลิตเดิม
                    ตอน replan — ดู CORE HOLD WEEK ด้านล่าง) ; None = คิดสดตามสต๊อกล้วน ๆ
                    cover ต่ำสุดตอนรอ = ตัวบอกว่าการรั้งไว้ทำให้ของขาดหรือไม่ (None = ไม่ต้องรอเลย)
                    log = บรรทัด [CORE BATCH] ของรอบนี้ — คืนออกไปให้ผู้เรียกพิมพ์เฉพาะรอบที่ใช้จริง
                    (ฟังก์ชันถูกเรียกสองรอบ ถ้าพิมพ์เองจะได้ log ของรอบที่ถูกทิ้งปนมาด้วย)
                    """
                    _cinv = _csd.get('Inventory', 0)
                    _ccw = _ccw0
                    _remaining_order = qty_left
                    _trig, _sched, _wait_cover, _log = None, [], None, []
                    _hold_idx = week_index(_hold_from) if _hold_from is not None else None
                    for _sim_i in range(104):
                        _ckp_rows = detail_mc[
                            (detail_mc["ITEM_CODE"].astype(str).str.upper().str.strip() == item_upper)
                            & (detail_mc["WEEK"].astype(int) == int(_ccw))
                        ]
                        for _, _kr in _ckp_rows.iterrows():
                            _so_str = str(_kr.get("SO_NO", ""))
                            _so_list = [s.strip() for s in _so_str.split(",") if s.strip()]
                            _total_so = max(len(_so_list), 1)
                            _match_so = sum(1 for s in _so_list if s in _core_so_set)
                            if _match_so > 0:
                                _cinv += float(_kr.get("KP_WEIGHT", 0)) * _match_so / _total_so
                        # ของที่วางลงแผนรอบนี้ไปแล้วจริง (ดูหมายเหตุตอนนิยามตัวจำลองท้ายส่วนโหลดทะเบียน core)
                        _cinv += _placed_core_by_week.get(int(_ccw), 0.0)
                        # ลด STOCK_MIN ทุก week (เริ่มจาก TODAY_WEEK + 2 แล้ว)
                        _cinv -= _csm
                        _cover = _cinv / _csm if _csm > 0 else 999
                        if _cover < 2 and _remaining_order > 0:
                            _cur_idx = week_index(_ccw)
                            if _hold_idx is not None and _cur_idx is not None and _cur_idx < _hold_idx:
                                # ถึงเวลาผลิตตามสต๊อกแล้ว แต่ถูกรั้งไว้รอสัปดาห์เดิมของ booking
                                # → จำ cover ที่บางที่สุดระหว่างรอ ไว้ตัดสินว่ารั้งไหวไหม
                                _wait_cover = _cover if _wait_cover is None else min(_wait_cover, _cover)
                            else:
                                if _trig is None:
                                    _trig = _ccw
                                _batch = min(_remaining_order, _cs5)
                                _sched.append((_ccw, _batch))
                                _remaining_order -= _batch
                                _cinv += _batch
                                _log.append(f"[CORE BATCH] {item}: W{_ccw} inv={_cinv-_batch:.0f} "
                                            f"produce={_batch:.0f} remain_order={_remaining_order:.0f}")
                                if _remaining_order <= 0:
                                    break
                        _ccw = next_week(_ccw)
                        if _ccw is None:
                            break
                    return _trig, _sched, _wait_cover, _log

                _core_trigger_week, _core_production_schedule, _, _core_batch_log = _sim_core_batches()

                # ── CORE HOLD WEEK: ตรึงรอบผลิตเดิมตอน replan ────────────────────
                # trigger คิดสดจากสต๊อกทุกครั้ง สต๊อกขยับนิดเดียวรอบผลิตก็เลื่อน — แผน core ที่
                # โรงงานเห็นเมื่อวานกลายเป็นคนละสัปดาห์วันนี้ทั้งที่ไม่มีอะไรผิด (เหตุผลเดิมที่
                # Replan เคยกัน core ไว้ทั้งหมด) → ถ้ารอบนี้ trigger สดเร็วกว่าสัปดาห์ที่ booking
                # จองไว้เดิม ให้รั้งกลับไปที่สัปดาห์เดิม **ตราบใดที่ระหว่างรอของไม่ต่ำกว่า MIN**
                # (cover ≥ 1 = ยังมีของใช้อย่างน้อย 1 สัปดาห์ ; ต่ำกว่านั้นถือว่าขาด ต้องยอมให้ขยับ)
                # ทิศตรงข้าม (สต๊อกดีขึ้น trigger สดช้ากว่าเดิม) ปล่อยตามสด — ไม่ดึงงานมาผลิตเร็ว
                # เกินจำเป็น เพราะเท่ากับล็อกเครื่องไว้เฉย ๆ ทั้งที่งานสีรออยู่
                _core_hold_w = None
                if REPLAN_MODE and _replan_from_week:
                    _core_hold_rv = _replan_from_week.get(
                        (str(sc_so_no).strip().upper(), item_upper)
                    )
                    if _core_hold_rv:
                        _core_hold_w = int(_core_hold_rv[0])
                if _core_hold_w is not None and _core_trigger_week is not None:
                    _hold_idx = week_index(_core_hold_w)
                    _trig_idx = week_index(_core_trigger_week)
                    if (_hold_idx is not None and _trig_idx is not None
                            and _hold_idx >= TODAY_IDX + FREEZE_WEEKS and _hold_idx > _trig_idx):
                        _t2, _s2, _wc2, _log2 = _sim_core_batches(_core_hold_w)
                        if _s2 and (_wc2 is None or _wc2 >= 1.0):
                            _cover_txt = "ไม่ต้องรอ" if _wc2 is None else f"{_wc2:.2f}"
                            print(f"[CORE HOLD WEEK] {item}: ตรึงรอบผลิตเดิมของ booking W{_core_hold_w} "
                                  f"(trigger สด W{_core_trigger_week}) — ระหว่างรอสต๊อกต่ำสุด cover={_cover_txt}")
                            _core_trigger_week, _core_production_schedule = _t2, _s2
                            _core_batch_log = _log2
                        elif not _s2:
                            print(f"[CORE HOLD SKIP] {item}: ตรึงถึง W{_core_hold_w} แล้วไม่เหลือรอบผลิต "
                                  f"ในกรอบจำลอง → ผลิตตาม trigger สด W{_core_trigger_week}")
                        else:
                            print(f"[CORE HOLD SKIP] {item}: ตรึงถึง W{_core_hold_w} ไม่ได้ ของจะขาด "
                                  f"(cover ต่ำสุด {_wc2:.2f} < 1) → ผลิตตาม trigger สด W{_core_trigger_week}")
                for _line in _core_batch_log:
                    print(_line)
        if rts_local_force:
            _bkr_data = rts_local_force.get("booking_remain_by_mc", {})
            _last_old = rts_local_force.get("last_old_by_mc", {})
            for _bkr_mc, _bkr_cap in _bkr_data.items():
                _bkr_w = _last_old.get(_bkr_mc)
                if _bkr_w is not None and _bkr_cap > 0:
                    # ❗ ห้าม seed week เก่ากว่า TODAY_WEEK + 2 ทุกกรณี
                    if _bkr_w < int(TODAY_WEEK) + 2:
                        continue
                    # CORE ITEM: skip seed if booking week < trigger week
                    if _core_trigger_week is not None and _bkr_w < _core_trigger_week:
                        print(f"[CORE SKIP SEED] {item}: W{_bkr_w} < trigger W{_core_trigger_week}")
                        continue
                    # ห้าม seed ถ้า gap ระหว่าง booking week กับ plan_week เกิน SETUP_GAP_WEEK
                    if plan_week is not None:
                        _bkr_plan_idx = week_index(plan_week)
                        _bkr_seed_idx = week_index(_bkr_w)
                        if _bkr_plan_idx is not None and _bkr_seed_idx is not None and (_bkr_plan_idx - _bkr_seed_idx) > SETUP_GAP_WEEK:
                            print(f"[BOOKING CARRY SKIP] {item} booking W{_bkr_w} → plan W{plan_week}: gap {_bkr_plan_idx - _bkr_seed_idx} > {SETUP_GAP_WEEK} weeks → ไม่ seed")
                            continue
                    # booking อาจอยู่คนละกลุ่มกับที่แผนจะใช้ (ก้อนเดียวกัน) → คีย์ต้องเป็นกลุ่มของแผน
                    _bkr_mc = _item_plan_mc(item, _bkr_mc)
                    if (item, _bkr_w, _bkr_mc) not in _existing_item_week_mc:
                        _bkr_key = (_bkr_w, item, _bkr_mc)
                        if _bkr_key not in remaining_week_cap:
                            remaining_week_cap[_bkr_key] = _bkr_cap
                            remaining_week_cap_owner[_bkr_key] = None
                            print(f"[BOOKING CARRY] Seeded remaining_week_cap[({_bkr_w}, {item}, {_bkr_mc})] = {_bkr_cap:.2f} from booking")
        else:
            # Non-CORE items: seed จาก detail_mc โดยตรง (ไม่ผ่าน rts_local_force)
            if _bk_mc_grp:
                _dm_item = detail_mc[
                    detail_mc["ITEM_CODE"].astype(str).str.upper().str.strip() == item_upper
                ]
                if not _dm_item.empty:
                    # booking ของ item เดียวกันอาจกระจายอยู่หลายกลุ่มในก้อนเดียวกัน → cap ที่เหลือ
                    # ของทุกกลุ่มต้องรวมเป็นกองเดียวใต้กลุ่มที่แผนใช้ ไม่ใช่ตัวแรกชนะแล้วที่เหลือหาย
                    _dm_seeded: set = set()
                    for _dm_mc, _dm_grp in _dm_item.groupby("MC_GROUP"):
                        _dm_mc_str = str(_dm_mc).strip().upper()
                        # หา last active week
                        _dm_active = _dm_grp[
                            pd.to_numeric(_dm_grp["MC_USE_CEIL"], errors="coerce").fillna(0) > 0
                        ]
                        if _dm_active.empty:
                            continue
                        # สร้าง sorted week list สำหรับตรวจสอบ setup (เปรียบเทียบกับ week ก่อนหน้า)
                        _dm_sorted_weeks = sorted(_dm_grp["WEEK"].dropna().astype(int).tolist())
                        for _, _dm_last_row in _dm_active.iterrows():
                            _dm_last_w = int(_dm_last_row.get("WEEK", 0) or 0)
                            if _dm_last_w <= 0:
                                continue
                            _dm_mc_ceil = float(_dm_last_row.get("MC_USE_CEIL", 0) or 0)
                            _dm_kp_weight = float(_dm_last_row.get("KP_WEIGHT", 0) or 0)
                            # ใช้ค่าจากชีท Work Day ตรง ๆ (float: 5.5 วันต้องไม่ปัดเป็น 5)
                            _dm_wd = float(_dm_last_row.get("WORKING_DAY", 0) or 0)
                            _dm_gauge = str(_dm_last_row.get("GUAGE", "")).strip()
                            _dm_cap = _get_capacity_for_mc_group(item, _dm_mc_str, _dm_gauge)
                            # 🔧 FIX: ตรวจสอบว่า booking week นี้มีการ setup เพิ่มเครื่องหรือไม่
                            # โดยเปรียบเทียบ MC_USE_CEIL กับ week ก่อนหน้าใน _dm_grp
                            _dm_w_pos = _dm_sorted_weeks.index(_dm_last_w) if _dm_last_w in _dm_sorted_weeks else -1
                            _dm_prev_mc = 0
                            _dm_prev_w_val = None
                            if _dm_w_pos > 0:
                                _dm_prev_w = _dm_sorted_weeks[_dm_w_pos - 1]
                                _dm_prev_w_val = _dm_prev_w
                                _dm_prev_row = _dm_grp[_dm_grp["WEEK"] == _dm_prev_w]
                                if not _dm_prev_row.empty:
                                    _dm_prev_mc = float(_dm_prev_row.iloc[0].get("MC_USE_CEIL", 0) or 0)
                            # เว้นไม่เกิน SETUP_GAP_WEEK = เครื่องยังอุ่น ทอต่อไม่ต้อง setup (กติกาเดียวกับแถวแผน
                            # และ AVA_MC) — เดิมเว้นแค่ 1 สัปดาห์ก็นับเป็นเครื่องใหม่ทั้งหมด → เครื่อง KP ที่ยัง
                            # ว่างครึ่งตัวถูกมองว่าเต็ม (เคส 29 ก.ย. 2569 FD6DRTJC30/1A0 KP W41 → W43: เหลือ
                            # 432 กก. แต่คิดเป็น setup 3 วัน = ติดลบ → ส่ง 495 กก. ไปจ้างทอ)
                            # ระยะห่างวัดบนปฏิทิน (WEEK เป็น YW — ลบตรง ๆ ข้ามปีจะได้ ~48)
                            _dm_week_gap = _week_gap_of(_dm_last_w, _dm_prev_w_val) if _dm_prev_w_val is not None else 999
                            if _dm_week_gap > SETUP_GAP_WEEK:
                                _dm_new_mc = float(_dm_mc_ceil)  # all machines are new setup
                                _dm_carry_mc = 0.0
                            else:
                                _dm_new_mc = max(0.0, _dm_mc_ceil - _dm_prev_mc)
                                _dm_carry_mc = _dm_mc_ceil - _dm_new_mc
                            _dm_setup = order_setup_days  # setup days สำหรับ item นี้
                            # remaining = (MC_USE_CEIL - MC_USE) × working_days × daily_cap
                            # = เครื่องที่ idle × วันทำงาน × cap ต่อวัน → qty ที่ new item สามารถผลิตได้ใน week เดียวกัน
                            _dm_total_cap = (_dm_carry_mc * _dm_wd + _dm_new_mc * max(0, _dm_wd - _dm_setup)) * _dm_cap
                            if _dm_new_mc > 0 and _dm_cap > 0:
                                print(f"[BOOKING SETUP DETECT] {item} W{_dm_last_w} MC={_dm_mc_str}: new_mc={_dm_new_mc:.0f} carry_mc={_dm_carry_mc:.0f} → adjusted total_cap={_dm_total_cap:.2f} (naive={_dm_mc_ceil*_dm_wd*_dm_cap:.2f})")
                            # กลุ่มที่แผนใช้จริงของก้อนนี้ (คีย์ remaining_week_cap ต้องตรงกับแถวแผน)
                            _dm_plan_mc = _item_plan_mc(item, _dm_mc_str, _dm_gauge)
                            if _dm_cap > 0 and _dm_mc_ceil > 0:
                                _dm_rem = max(0.0, _dm_total_cap - _dm_kp_weight)
                                if _dm_rem > 0 and (item, _dm_last_w, _dm_plan_mc) not in _existing_item_week_mc:
                                    # ❗ ห้ามใช้ week เก่ากว่า TODAY_WEEK + 2 ทุกกรณี
                                    if _dm_last_w < int(TODAY_WEEK) + 2:
                                        continue
                                    if plan_week is not None:
                                        _dm_plan_idx = week_index(plan_week)
                                        _dm_seed_idx = week_index(_dm_last_w)
                                        if _dm_plan_idx is not None and _dm_seed_idx is not None and abs(_dm_plan_idx - _dm_seed_idx) > SETUP_GAP_WEEK:
                                            continue
                                    _dm_key = (_dm_last_w, item, _dm_plan_mc)
                                    if _dm_key not in remaining_week_cap:
                                        remaining_week_cap[_dm_key] = _dm_rem
                                        remaining_week_cap_owner[_dm_key] = None
                                        _dm_seeded.add(_dm_key)
                                        print(f"[BOOKING CARRY] Seeded remaining_week_cap[({_dm_last_w}, {item}, {_dm_plan_mc})] = {_dm_rem:.2f} from booking (non-CORE)")
                                    elif _dm_key in _dm_seeded:
                                        # อีกกลุ่มในก้อนเดียวกัน → เครื่องคนละตัว cap ที่เหลือต้องบวกเพิ่ม
                                        remaining_week_cap[_dm_key] += _dm_rem
                                        print(f"[BOOKING CARRY] รวม cap ก้อนเดียวกัน ({_dm_last_w}, {item}, {_dm_plan_mc}) += {_dm_rem:.2f} (จาก {_dm_mc_str})")


        # CORE ITEM: batch production — cap qty_left to first batch size
        _core_real_qty = qty_left
        _core_batch_idx = 0
        if _core_production_schedule:
            _bw0, _bq0 = _core_production_schedule[0]
            qty_left = min(qty_left, _bq0)
            if plan_week is not None:
                # core ไม่วางตาม target → เริ่มที่ "รอบผลิตตามสต๊อก" แต่ไม่เร็วกว่าพื้นแข็งก่อน JIT
                # (เดิมเลื่อนได้แค่ไปข้างหน้า → สต๊อกต้องการ W41 แต่ JIT ดันไป W46 ก็วาง W46 = ของขาด)
                # ⚠️ เทียบด้วยตำแหน่งบนปฏิทินจริง (week_pos) — เดิมใช้ week_index ที่เดาปีจาก anchor
                #   พอ JIT ย้าย anchor ไปไกล รอบผลิตที่อยู่ก่อน anchor เกิน WEEK_LOOKBACK_LIMIT ถูกแปล
                #   เป็นปีหน้า → นอก horizon → ลูปหยุดเงียบ งานหายเป็น UNPLANNED
                #   (เคสจริง 14 ก.ย. 2569 FD3SPSPV167/01A0 SC 722126: JIT W50, รอบผลิต W40 → หาย 6,056 กก.)
                _tw_idx = week_pos(_bw0)
                _core_floor_idx = _pre_jit_idx if _pre_jit_idx is not None else week_pos(plan_week)
                if _tw_idx is not None and _core_floor_idx is not None:
                    _core_start_idx = max(int(_tw_idx), int(_core_floor_idx))
                    if 0 <= _core_start_idx < len(calendar_week):
                        plan_week = int(calendar_week.iloc[_core_start_idx]["WEEK"])
                        set_week_anchor(_core_start_idx)
            print(f"[CORE BATCH INIT] {item}: {len(_core_production_schedule)} batches, B1 W{_bw0} qty={qty_left:.0f} (total={_core_real_qty:.0f}), plan_week=W{plan_week}")

        # ---------- CORE ORDER GAP: ช่องว่างระหว่าง "คนละใบสั่ง" ของผ้า core ตัวเดียวกัน ----------
        # เดิมกันเครื่องเฉพาะตอนข้าม batch ในใบเดียวกัน (CORE NEXT BATCH) แต่ core ตัวหนึ่งมีหลายใบ
        # และแต่ละใบมีรอบผลิตของตัวเอง → ช่องว่างระหว่างใบไม่มีแถว Hold เครื่องจึงหลุดคืนพูล
        # ทั้งที่สัปดาห์ที่กลับมาทอยัง carry เครื่องชุดเดิม (gap ≤ SETUP_GAP_WEEK) = เครื่องผี
        # เคสจริง 20 ก.ย. 2569 F6DRYTECH201A0 RAP98: SO 720592 จบ W43 → SO 722128 เริ่ม W46
        # แถว W46 เขียน "carry 12 ตัว" แต่ W44/W45 บน Gantt ขึ้นว่าง 20/25 เครื่อง
        # ต้องทำ "ก่อน" วางใบนี้ เพื่อให้แถวแรกของใบ carry เครื่องอุ่นชุดนี้ต่อได้
        if is_core_item and plan_week is not None:
            _core_hold_gap(item, _ord_mc_grp, order.get("MC_GUAGE", ""),
                           week_index(plan_week), sc_so_no, plans_only=True,
                           fields=_hold_order_fields(order, order_type, fg_week,
                                                     fg_week_int, rdd_idx, sub_color))

        _mainloop_guard = 0
        while (qty_left > 0 or (_core_batch_idx + 1 < len(_core_production_schedule))) and plan_week is not None:
            # งานสีที่ยึดเครื่องไว้รอบสัปดาห์ก่อนแต่ไม่ได้เขียนแถว (ข้าม/เลื่อนสัปดาห์) → คืนงานเจ้าของเดิม
            # แล้วเปิดสมุดจดการยึดของรอบนี้ (ดู _grab_txn_settle)
            _grab_txn_abort("รอบสัปดาห์ก่อนไม่ได้เขียนแถว")
            _grab_txn_open()
            # SAFETY: กัน infinite loop ตอนข้ามปลายปี (next_week คืน bare week แล้ว week_index
            # อาจ resolve ถอยปีทำให้ plan_week ไม่เดินหน้า) — เกินจำนวนสัปดาห์*fill ที่เป็นไปได้จริง → หยุด
            _mainloop_guard += 1
            if _mainloop_guard > 3000:
                print(f"[SAFETY YEAR-END] {item} SC {sc_so_no}: main placement loop เกิน 3000 รอบ → break (qty เหลือจะเข้า UNPLANNED)")
                break
            # FIX: กัน floating-point residual qty ที่น้อยเกินไปจาก booking deduction
            # qty ที่น้อยกว่า 1.0 ไม่สามารถผลิตได้จริง และจะทำลาย booking carryover seed ของ order อื่น
            if qty_left < 1.0:
                # CORE ITEM: batch continuation — check for next batch
                if _core_production_schedule:
                    _produced_this_batch = _core_production_schedule[_core_batch_idx][1] - max(0, qty_left)
                    _core_real_qty -= _produced_this_batch
                    _core_batch_idx += 1
                    if _core_batch_idx < len(_core_production_schedule) and _core_real_qty > 1.0:
                        _nbw, _nbq = _core_production_schedule[_core_batch_idx]
                        # ตำแหน่งจริงบนปฏิทิน + ย้าย anchor ตาม (เหตุผลเดียวกับ CORE BATCH INIT)
                        _nb_idx = week_pos(_nbw)
                        if _nb_idx is not None and 0 <= _nb_idx < len(calendar_week):
                            plan_week = int(calendar_week.iloc[_nb_idx]["WEEK"])
                            set_week_anchor(_nb_idx)
                        else:
                            plan_week = int(_nbw)
                        # สัปดาห์ที่เว้นรอรอบผลิตถัดไป → กันเครื่องชุดเดิมไว้ (Hold รอ target)
                        # ต้องทำ "ก่อน" วาง batch ถัดไป เพื่อให้แถวถัดไป carry เครื่องอุ่นชุดนี้ต่อได้
                        _core_hold_gap(item, _ord_mc_grp, order.get("MC_GUAGE", ""),
                                       week_index(plan_week), sc_so_no,
                                       fields=_hold_order_fields(order, order_type, fg_week,
                                                                 fg_week_int, rdd_idx, sub_color))
                        qty_left = min(_core_real_qty, _nbq)
                        required_machines_info = None
                        print(f"[CORE NEXT BATCH] {item}: W{_nbw} batch={qty_left:.0f} total_remaining={_core_real_qty:.0f}")
                        continue
                    else:
                        qty_left = _core_real_qty
                break

            # ---------- เว้น 1 สัปดาห์รอสีเปิดรอบใหม่ (Color Gap) ----------
            # คีย์นี้ทอด้วยสีที่เปิดไว้จบไปแล้ว + ทอส่วนที่ยังไม่มีสีต่อมาครบ COLOR_GAP_AFTER
            # สัปดาห์ → สัปดาห์นี้ห้ามวางงาน แต่กันเครื่องไว้รอสีรอบใหม่ แล้วข้ามไปสัปดาห์ถัดไป
            # (ก้อนสีไม่โดน — มีสีอยู่แล้วทอได้ตามปกติ)
            # ผ้า core ไม่เข้ากติกาสีเลย (user 16 ก.ย. 2569) — รอบผลิตของมันมาจากสต๊อก
            # และมี "Hold รอ target" เป็นตัวคุมการเว้นของตัวเองอยู่แล้ว
            if COLOR_GAP_ENABLED and not _CUR_ORDER_COLOR_PART and not is_core_item:
                _cg_gauge = order.get("MC_GUAGE", "")
                _cg_gap = _color_gap_idx(item, _ord_mc_grp, _cg_gauge)
                if _cg_gap is not None and week_index(plan_week) == _cg_gap:
                    _color_gap_hold_mc(item, _ord_mc_grp, _cg_gauge, _cg_gap, sc_so_no,
                                       _hold_order_fields(order, order_type, fg_week,
                                                          fg_week_int, rdd_idx, sub_color))
                    _cg_next = next_week(plan_week)
                    print(f"[COLOR GAP] {item} SC {sc_so_no}: ข้าม W{plan_week} (รอสี) → W{_cg_next}")
                    plan_week = _cg_next
                    continue

            # ถ้า FG ใหม่ (SC/SO ใหม่) เริ่มใน week เดิมและมี cap เหลือ ให้ผลิตใน week เดิมจน cap หมดก่อนข้ามไป week ถัดไป
            _fill_last_week = None  # track week สุดท้ายที่ fill cross-SC
            _already_filled_this_sc = set()  # track (item, week, mc) ที่ SC นี้ fill ไปแล้ว (ป้องกัน duplicate ของ SC เดียวกัน)
            # ก้อนจ้างทอ (OUTSOURCE_FORCE) ห้ามเข้า loop นี้ — remaining cap เป็นของ "เครื่องในบ้าน"
            # ที่แถวปกติของ item เดียวกันเปิดไว้ ถ้าเติมตรงนี้ก้อนจ้างทอจะไปกินเครื่องในบ้าน
            while qty_left > 0 and not _outsource_force:
                # ค้นหา remaining capacity สำหรับ ITEM เดียวกัน ในทุก week (เรียง week น้อยสุดก่อน)
                _found_rem_mc = None
                _found_rem_cap = 0
                _found_rem_week = None
                # ⚠️ คีย์ของ remaining_week_cap เป็นเลขสัปดาห์เปล่า (ไม่มีปี) — ทุกการเทียบในลูปนี้
                # ต้องแปลงเป็น index บนปฏิทินก่อน ไม่งั้น W40 ของปีนี้จะถูกมองว่า "อยู่หลัง" W24
                # ของปีหน้า (40 > 24) แล้วถูกข้ามทิ้ง ทั้งที่เป็น cap ของเครื่องที่วิ่งอยู่จริง
                _plan_idx_fill = week_index(plan_week) if plan_week is not None else None
                _core_trig_idx_fill = (
                    week_index(_core_trigger_week) if _core_trigger_week is not None else None
                )
                _today_floor_idx = globals().get("TODAY_IDX")
                for _rk, _rv in sorted(remaining_week_cap.items(), key=_rem_cap_week_order):
                    if _rk[1] == item and _rv > 0:
                        _rk_idx_fill = week_index(_rk[0])
                        # 🔧 FIX: ไม่ดึง cap ของ "future week" มาใช้ก่อนเวลา
                        # ให้เริ่มผลิตตาม plan_week ปัจจุบันก่อน แล้วค่อยใช้ carry เมื่อถึง week นั้นจริง
                        if _plan_idx_fill is not None and _rk_idx_fill is not None:
                            if _rk_idx_fill > _plan_idx_fill:
                                continue
                        elif plan_week is not None and _rk[0] > plan_week:
                            continue
                        # ❗ ห้ามใช้ week เก่ากว่า TODAY_WEEK + 2 ทุกกรณี
                        # (เทียบด้วย index — เลขดิบจะ pop คีย์ของปีหน้าทิ้ง เช่น W5 ปีหน้า < 33)
                        if _today_floor_idx is not None and _rk_idx_fill is not None:
                            _too_old = _rk_idx_fill < int(_today_floor_idx) + 2
                        else:
                            _too_old = _rk[0] < int(TODAY_WEEK) + 2
                        if _too_old:
                            remaining_week_cap.pop(_rk, None)
                            remaining_week_cap_owner.pop(_rk, None)
                            continue
                        # CORE ITEM: skip fill if week < trigger week
                        #
                        # ข้อยกเว้น: งานที่เลยกำหนดส่งแล้ว (RDD อยู่ก่อน trigger) — trigger มาจากการ
                        # จำลองสต๊อกซึ่งไปไกลได้ถึงปีหน้า ถ้ายึดตามนั้นจะไปเปิดเครื่องใหม่ (เสียวัน setup
                        # + กินเครื่องจากพูล) ทั้งที่เครื่องของ booking ยังวิ่ง item นี้อยู่และมี cap เหลือ
                        # → เติมเข้าเครื่องเดิมก่อน trigger ได้ ไม่ทำให้สต๊อกล้นเพราะเป็นของที่ค้างส่งอยู่แล้ว
                        if _core_trig_idx_fill is not None and _rk_idx_fill is not None:
                            _before_trigger = _rk_idx_fill < _core_trig_idx_fill
                        else:
                            _before_trigger = (
                                _core_trigger_week is not None and _rk[0] < _core_trigger_week
                            )
                        if _before_trigger:
                            _rdd_before_trigger = (
                                rdd_idx is not None
                                and _core_trig_idx_fill is not None
                                and int(rdd_idx) < int(_core_trig_idx_fill)
                            )
                            if not _rdd_before_trigger:
                                continue
                            print(
                                f"[CORE FILL BEFORE TRIGGER] {item} SC {sc_so_no}: เติมเข้า cap เหลือ "
                                f"W{_rk[0]} (ก่อน trigger W{_core_trigger_week}) เพราะ RDD อยู่ก่อน trigger "
                                f"→ ใช้เครื่องเดิมที่วิ่งอยู่ ไม่เปิดเครื่องใหม่"
                            )
                        # YD-ORDERS: ห้ามเก็บตกย้อนไปก่อนวันย้อมเสร็จ — ด้ายยังไม่ย้อมก็ทอไม่ได้
                        # (order_week = สัปดาห์ถัดจาก DYE_END_DATE) เส้นทางปกติ clamp ไว้แล้ว 2 ชั้น
                        # แต่ลูปเก็บตก cap เหลือนี้เลือก week เองจาก remaining_week_cap จึงต้อง gate ซ้ำ
                        if order_type == "YD-ORDERS" and order_week is not None:
                            _yd_fill_floor = week_index(order_week)
                            _rk_idx = week_index(_rk[0])
                            if _yd_fill_floor is not None and _rk_idx is not None and _rk_idx < _yd_fill_floor:
                                continue
                        # POINT DATE (รอบ replan): ห้ามเก็บตกย้อนไปก่อนสัปดาห์ที่ด้ายเข้าครบ
                        # ด้วยเหตุผลเดียวกับ YD ข้างบน — ลูปนี้เลือก week เองจาก remaining_week_cap
                        # จึงข้าม gate ตอนตั้ง start_idx ไปได้ (เป็นทางที่ทำให้แผนหลุดไปก่อนด้ายเข้า)
                        if _point_idx is not None:
                            _pt_rk_idx = week_index(_rk[0])
                            if _pt_rk_idx is not None and _pt_rk_idx < _point_idx:
                                continue
                        _found_rem_week = _rk[0]
                        _found_rem_mc = _rk[2]
                        _found_rem_cap = _rv
                        break

                if _found_rem_mc is None or _found_rem_cap <= 0:
                    break

                # ถ้า SC ปัจจุบันเคย fill item+week+mc นี้ไปแล้ว → หยุดเพื่อป้องกัน duplicate ของ SC เดียวกัน
                # ไม่ใช้ _existing_item_week_mc เพราะ set นั้นมี SC อื่น (เช่น SC ก่อนหน้า) อยู่ด้วย
                # ซึ่งจะ block SC ปัจจุบันไม่ให้ใช้ remaining cap ที่ SC ก่อนหน้าทิ้งไว้
                if (_found_rem_week, item, _found_rem_mc) in _already_filled_this_sc:
                    print(
                        f"[DEBUG DUPLICATE] SC {sc_so_no} already filled {item}+{_found_rem_week}+{_found_rem_mc}, stop"
                    )
                    break


                # ตั้งค่า mc_group และตัวแปรที่เกี่ยวข้องจาก remaining capacity ที่พบ
                _fill_mc_group = _found_rem_mc
                _fill_week = _found_rem_week
                _rem_cap_key = (_fill_week, item, _fill_mc_group)
                _cap_row = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == _fill_mc_group)
                ]
                if _cap_row.empty:
                    remaining_week_cap.pop(_rem_cap_key, None)
                    remaining_week_cap_owner.pop(_rem_cap_key, None)
                    break

                # ใช้ capacity ตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน
                _fill_gauge = _cap_row.iloc[0].get("GUAGE")
                _fill_daily_cap = _get_capacity_for_mc_group(item, _fill_mc_group, _fill_gauge)
                if _fill_daily_cap <= 0:
                    # Fallback: use original capacity if new logic returns 0 (normalize 24→20hr)
                    _fill_daily_cap = _normalize_capacity(item, _fill_mc_group, float(_cap_row.iloc[0]["CAP ทอ"]))
                _fill_daily_cap = adjust_daily_cap_for_item_special(_fill_daily_cap, item, _fill_mc_group, _fill_gauge)
                _fill_rev_weight = get_revolution_weight_from_orders(item, _fill_mc_group)
                _fill_ck = _ck(item, _fill_mc_group, _fill_gauge)
                _fill_avail_mc = machines_in_use.get(_fill_ck, 1)
                # 🔧 FIX: ถ้า new plan มี carry machine จาก week ก่อนหน้าติดกัน (gap=1)
                # และ fill week นี้มี old booking machines → รวม cap ทั้งสองชุด
                # เช่น W23 setup 1 mc (new plan) + W24 old booking 3 mc → fill row W24 = 4 mc
                _fill_w_idx = week_index(_fill_week)
                _prev_lp_at_fill = last_production.get(_fill_ck)
                _new_carry_at_fill = machines_in_use.get(_fill_ck, 0)
                _bk_mc_at_fill_week = booking_mc_by_week.get(_fill_ck, {}).get(_fill_w_idx, 0)
                _is_new_plan_carry_fill = (
                    _fill_ck in new_plan_started_items
                    and _prev_lp_at_fill is not None
                    and _fill_w_idx is not None
                    and _fill_w_idx - _prev_lp_at_fill == 1
                    and _new_carry_at_fill > 0
                    and _bk_mc_at_fill_week > 0
                )
                if _is_new_plan_carry_fill:
                    _fill_avail_mc, _fill_own_mc, _fill_bk_prev, _bk_mc_at_fill_week = _combine_carry_with_booking(
                        _fill_ck, _new_carry_at_fill, _prev_lp_at_fill, _fill_w_idx
                    )
                    machines_in_use[_fill_ck] = _fill_avail_mc  # อัปเดตก่อน implied_mc cap
                    print(f"[CARRY+BK FILL] {item} W{_fill_week}: plan-own={_fill_own_mc} (carry {_new_carry_at_fill} − booking W ก่อน {_fill_bk_prev}) + booking={_bk_mc_at_fill_week} → combined={_fill_avail_mc} mc")
                _fill_actual_wd = get_working_days_by_factory(
                    _fill_mc_group, _fill_avail_mc, week=_fill_week, gauge=_fill_gauge
                )
                # 🔧 FIX: ถ้า remaining capacity มากกว่า 1 เครื่อง → คำนวณเครื่องจาก capacity
                # เพื่อรองรับกรณี freed capacity จากเครื่องที่ปล่อย (tail end)
                # ⚠️ ห้าม implied_mc เกินกว่าเครื่องที่ setup จริง (machines_in_use) เพราะ remaining cap
                # อาจมาจาก freed capacity ซึ่งยังนับเป็นเครื่องที่ active อยู่ ไม่ใช่เครื่องใหม่
                if _fill_daily_cap > 0 and _fill_actual_wd > 0:
                    _mc_one_cap = _fill_actual_wd * _fill_daily_cap
                    if _mc_one_cap > 0:
                        _implied_mc = int(_found_rem_cap / _mc_one_cap) + (1 if _found_rem_cap % _mc_one_cap > 0 else 0)
                        # cap ที่ machines_in_use จริง (ห้ามเกิน)
                        _actual_mc_in_use = machines_in_use.get(_fill_ck, _fill_avail_mc)
                        _implied_mc = min(_implied_mc, _actual_mc_in_use)
                        if _implied_mc > _fill_avail_mc:
                            _fill_avail_mc = _implied_mc
                # 🔧 เครื่องที่แถวเติมใช้ร่วม = เครื่องที่ "มีอยู่จริงในสัปดาห์ที่เติม" ไม่ใช่ค่าล่าสุดของ
                # machines_in_use — ลูปนี้ถอยไปเติมสัปดาห์ก่อนหน้าได้ ขณะที่ machines_in_use เป็นสถานะ
                # ของสัปดาห์ที่วางไปไกลสุดแล้ว และข้างล่างเขียนค่านี้ย้อนกลับเป็นลูกโซ่ carry ของสัปดาห์ที่เติม
                # (เคสจริง 30 ก.ย. 2569 FD5DU34B0 RAO: ใบ 722939 ตั้งเครื่องที่ W2702 จนจำไว้ 4 ตัว
                #  → ใบ 721274 ถอยมาเติม W50 ที่มีเครื่องจริง 2 แต่จดว่า "ร่วม 4 ตัว" แล้ว W51 ยก carry 4
                #  = เครื่องผี 2 ตัว ทอ 4,895 กก. บนเครื่องจริง 2 ตัว)
                # นับแบบเดียวกับบัญชีพูล (_pool_blocks_from_plan): max ในใบสั่งเดียวกัน แล้วบวกข้ามใบ
                # รวมแถว Hold ที่กันเครื่องไว้ + เครื่อง booking ของสัปดาห์นั้น (บวกไว้ก่อน = ไม่ตัดต่ำกว่าจริง)
                # ยังไม่มีแถวแผนในสัปดาห์นั้นเลย (cap มาจาก booking seed) → คงค่าเดิม
                # เคส carry+booking (_is_new_plan_carry_fill) แถวเติมคือตัวประกาศเครื่องของแผนเอง → ไม่แตะ
                if not _is_new_plan_carry_fill and _fill_w_idx is not None:
                    _wk_mc_by_sc = {}
                    for _p in plans:
                        if _p.get("ITEM_CODE") != item or _p.get("MC_GROUP") != _fill_mc_group:
                            continue
                        _p_idx = _p.get("PLAN_IDX")
                        if _p_idx is None or (isinstance(_p_idx, float) and pd.isna(_p_idx)):
                            _p_idx = week_pos(_p.get("PLAN_WEEK"))
                        if _p_idx is None or int(_p_idx) != int(_fill_w_idx):
                            continue
                        _p_sc = str(_p.get("SC_SO_NO", "")).strip()
                        _wk_mc_by_sc[_p_sc] = max(
                            _wk_mc_by_sc.get(_p_sc, 0), int(_p.get("ACTUAL_MC", 0) or 0)
                        )
                    _wk_mc_real = sum(_wk_mc_by_sc.values()) + int(_bk_mc_at_fill_week or 0)
                    if _wk_mc_real > 0 and _fill_avail_mc > _wk_mc_real:
                        print(
                            f"[FILL MC OF WEEK] {item} SC {sc_so_no} W{_fill_week} {_fill_mc_group}: "
                            f"เครื่องที่จำไว้ {_fill_avail_mc} ตัวเป็นของสัปดาห์อื่น → ใช้เครื่องที่มีจริง "
                            f"ในสัปดาห์นี้ {_wk_mc_real} ตัว"
                        )
                        _fill_avail_mc = _wk_mc_real
                _rem_cap = _found_rem_cap
                # เพิ่ม cap จาก new plan carry machine (full week capacity) เข้า _rem_cap
                # ⚠️ ใช้ _fill_own_mc (เครื่องของแผนเอง) ไม่ใช่ _new_carry_at_fill (รวมเครื่อง booking)
                # เพราะ capacity ที่เหลือของเครื่อง booking อยู่ใน _found_rem_cap อยู่แล้ว → บวกซ้ำไม่ได้
                if _is_new_plan_carry_fill and _fill_actual_wd > 0 and _fill_daily_cap > 0 and _fill_own_mc > 0:
                    _extra_carry_cap = _fill_own_mc * _fill_daily_cap * _fill_actual_wd
                    _rem_cap += _extra_carry_cap
                    print(f"[CARRY+BK FILL] {item} W{_fill_week}: +carry cap {_fill_own_mc}mc × {_fill_daily_cap:.2f} × {_fill_actual_wd}วัน = +{_extra_carry_cap:.2f} → total _rem_cap={_rem_cap:.2f}")
                # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → หัก capacity ออก 1 วัน (สะสมในสัปดาห์เดียวกัน)
                if order_type == "YD-ORDERS" and sub_color:
                    _fill_prev_color = last_sub_color.get(_fill_ck, "")
                    if _fill_prev_color and _fill_prev_color != sub_color:
                        _wcs_fill_key = (_fill_ck, _fill_week)
                        _yd_week_color_setups[_wcs_fill_key] = _yd_week_color_setups.get(_wcs_fill_key, 0) + 1
                        _color_deduct = _fill_avail_mc * 1 * _fill_daily_cap  # 1 วัน × เครื่อง × cap (หักเต็มวัน ไม่ round ลง rev_weight)
                        _rem_cap = max(0, _rem_cap - _color_deduct)
                        print(f"[YD SUB_COLOR FILL] {item} W{_fill_week}: SUB_COLOR {_fill_prev_color}→{sub_color} หัก {_color_deduct:.0f} units (color setup #{_yd_week_color_setups[_wcs_fill_key]})")
                        # 🔧 FIX: ถ้าหัก color setup จน cap หมด → ลูปในจะไม่ผลิต ทำให้ last_sub_color/remaining_week_cap
                        # ไม่ถูกอัปเดต และ qty_left ไม่ลด → ลูปนอก while qty_left>0 วนซ้ำ key เดิมไม่รู้จบ (infinite loop)
                        # จึงต้องอัปเดต last_sub_color, ล้าง remaining cap ก้อนนี้ แล้ว break ออก
                        if _rem_cap <= 0:
                            last_sub_color[_fill_ck] = sub_color
                            remaining_week_cap.pop(_rem_cap_key, None)
                            remaining_week_cap_owner.pop(_rem_cap_key, None)
                            print(f"[YD SUB_COLOR FILL] {item} W{_fill_week}: color setup หัก cap จนหมด → ข้าม fill week นี้ (กัน infinite loop)")
                            break
                # เครื่องของ fill row นี้ใช้ร่วมกับแถวที่ครองเครื่องอยู่แล้วใน (item, mc, week) เดียวกันหรือไม่
                # ถ้าใช่ → แถวนี้ต้องรายงาน ACTUAL_MC = 0 มิฉะนั้นผลรวมเครื่องต่อสัปดาห์จะนับซ้ำ
                # (เช่น SO A 3 เครื่อง + SO B 3 เครื่อง = 6 ทั้งที่เป็นเครื่องชุดเดียวกัน 3 ตัว)
                _fill_shared_owner = next(
                    (
                        str(p.get("SC_SO_NO", "")).strip()
                        for p in plans
                        if p.get("ITEM_CODE") == item
                        and p.get("MC_GROUP") == _fill_mc_group
                        and p.get("PLAN_WEEK") == _fill_week
                        and int(p.get("ACTUAL_MC", 0) or 0) > 0
                    ),
                    None,
                )
                # 🛑 ด่านกัน: ไม่มีเครื่องรองรับจริง → ห้ามเขียนแถวผลิต
                # เครื่องของสัปดาห์นี้อาจถูกปลด/ยึดไปหลัง cap ถูกจดไว้ (งานสียึดเครื่อง, ลดเครื่อง
                # ฯลฯ) ถ้ายังเขียนแถวจะได้ "เติมเข้าเครื่องที่วิ่งอยู่ 0 เครื่อง" = ยอดผลิตที่ไม่มี
                # เครื่องรองรับ หน้า Gantt ไม่โชว์เลขเครื่องและช่องยังขึ้นว่าง ทั้งที่มีงานวางอยู่
                # (เจ้าของเครื่องร่วม/เครื่อง booking ถือว่ามีเครื่องจริง — ไม่เข้าด่านนี้)
                if (
                    int(_fill_avail_mc or 0) <= 0
                    and int(_bk_mc_at_fill_week or 0) <= 0
                    and not _fill_shared_owner
                ):
                    remaining_week_cap.pop(_rem_cap_key, None)
                    remaining_week_cap_owner.pop(_rem_cap_key, None)
                    print(
                        f"[FILL NO MC] {item} W{_fill_week} {_fill_mc_group}: ไม่เหลือเครื่องแล้ว "
                        f"(ถูกยึด/ปลดไป) → ล้าง cap ค้าง {_rem_cap:.0f} หน่วย ไม่เขียนแถว"
                    )
                    break
                while qty_left > 0 and _rem_cap > 0:
                    if _fill_rev_weight and _fill_rev_weight > 0:
                        _fill_mc_cnt = max(1, int(_fill_avail_mc or 1))
                        produce = min(
                            qty_left,
                            fold_round(
                                _rem_cap, _fill_rev_weight, _fill_mc_group,
                                per_mc_cap=_rem_cap / _fill_mc_cnt, mc_count=_fill_mc_cnt,
                            ),
                        )
                    else:
                        produce = min(qty_left, _rem_cap)

                    if produce > 0:
                        plans.append({
                            "ITEM_CODE": item,
                            "SC_SO_NO": str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip(),
                            "MC_GROUP": _fill_mc_group,
                            "MC_GUAGE": order["MC_GUAGE"],
                            "FACTORY_TYPE": FACTORY_TYPE_MAP.get(_fill_mc_group, "UNKNOWN"),
                            "PLAN_WEEK": _fill_week,
                            # เก็บ row index บน timeline ไว้ด้วย — PLAN_WEEK เป็น bare week
                            # ที่ระบุปีไม่ได้เมื่อแผนคร่อมปี (ใช้เติม PLAN_YEAR ตอน export)
                            "PLAN_IDX": week_pos(_fill_week),
                            "PRODUCE_QTY": produce,
                            "SETUP_DAYS": 0,
                            "REQUIRED_MC": _fill_avail_mc,
                            "ACTUAL_MC": 0 if _fill_shared_owner else _fill_avail_mc,
                            "CARRYOVER_MC": 0 if _fill_shared_owner else _fill_avail_mc,
                            "NEW_MC": 0,
                            "MC_SHARED": _fill_avail_mc if _fill_shared_owner else 0,
                            "MC_BOOKING": _bk_mc_at_fill_week,  # เครื่องที่ booking ทอไอเทมนี้อยู่แล้ว
                            "REMARK": (
                                f"เติมเข้าเครื่องเดิม — ใช้เครื่อง {_fill_avail_mc} ตัวร่วมกับ SO {_fill_shared_owner} ({_fill_mc_group}) ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup"
                                if _fill_shared_owner
                                else f"เติมเข้าเครื่องที่วิ่งอยู่ {_fill_mc_group} {_fill_avail_mc} เครื่อง (ใช้ capacity ที่เหลือ) ไม่ต้อง setup"
                            ),
                            "FACTORY_WORKING_DAYS": get_working_days_by_factory(_fill_mc_group, _fill_avail_mc, week=_fill_week, gauge=_fill_gauge),
                            "WD_BASE": get_working_days_by_factory(_fill_mc_group, _fill_avail_mc, week=0, item_code=item, gauge=_fill_gauge),
                            "WD_W32": get_working_days_by_factory(_fill_mc_group, _fill_avail_mc, week=32, gauge=_fill_gauge),
                            "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(_fill_week)),
                            "ACTUAL_WORKING_DAYS": _fill_actual_wd,
                            "DAILY_CAPACITY": _fill_daily_cap,
                            "REVOLUTION_WEIGHT": _fill_rev_weight if _fill_rev_weight is not None else 0,
                            "AVAILABLE_DAYS": _fill_actual_wd,
                            "ORDERS_QTY": order_qty,
                            "PENDING_PLAN": pending_plan,
                            "PLAN_QTY": qty_left - produce,
                            "ORDER_TYPE": order_type,
                            "ORDER_DATE": order["Date"],
                            "FG_WEEK": fg_week,
                            "TARGET_KNIT": fg_week_int,
                            # เก็บ row index บน timeline ของเดดไลน์ด้วย — TARGET_KNIT เป็น bare
                            # week เหมือน PLAN_WEEK จึงเทียบ "ทัน/ไม่ทัน" ข้ามปีไม่ได้
                            # (ใช้เติม TARGET_YEAR ตอน export)
                            "TARGET_IDX": rdd_idx,
                            "FLOOR_IDX": _hard_floor_idx,
                            "MATERIAL_CONTENT": str(order.get("MATERIAL_CONTENT", "")).strip(),
                            "IS_CORE_ITEM": "CORE ITEM" if is_core_item else "",
                            "CUSTOMER": str(order.get("Customer", "")).strip(),
                            "PLAN_SOURCE": "NEW",
                            "LT_YARN": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_days(item),
                            "YARN_USED": _yarn_used_lookup.get(str(item).strip().upper(), ""),
                            "DATE_IN": order.get("DATE_IN"),
                            "EARLIEST_PLAN_WEEK": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_earliest_week(item, date_in=order.get("DATE_IN")),
                            "SUB_COLOR": sub_color,
                            "PO_NO": str(order.get("PO_NO", "")).strip(),
                            "YARNPO": str(order.get("YARNPO", "") or "").strip(),
                            "RDD_WEEK": _rdd_week_of(order),
                            "SC_LINE_ID": ColorOpen.so_text(order.get("SC_LINE_ID")),
                            "KP_NO": ColorOpen.so_text(order.get("KP_NO")),  # กัน "nan" ของแถวฝั่ง SC
                        })
                        plans[-1]["NAY_COLOR"] = str(order.get("NAY_COLOR", "")).strip()
                        plans[-1]["COLOR_DESC"] = str(order.get("COLOR_DESC", "")).strip()
                        plans[-1]["LINE_REMARK"] = _clean_remark(order.get("LINE_REMARK", ""))
                        # วัน/สัปดาห์ด้ายเข้าครบ (ล็อตที่ไกลสุด) — หน้าเว็บใช้โชว์บนบล็อก Gantt
                        plans[-1]["PO_IN_DATE"], plans[-1]["PO_IN_WEEK"] = po_in_fields(order)
                        plans[-1]["YARN_STOCK"] = yarn_stock_note(order)
                        # ธงก้อนสีของออร์เดอร์ต้องติดมากับแถวเติมเครื่องด้วย (เดิมไม่ได้ตั้ง = ว่าง)
                        # ก้อนเดียวกันที่ถูกวางสองสัปดาห์จึงมีทั้ง "Yes" และ "ว่าง" → หน้าเว็บระบาย
                        # คนละสี และ _main_row_is_color ถอยไปใช้เกณฑ์ระดับ item ผิดใบ
                        plans[-1]["COLOR_PART"] = _color_part_text(order)
                        # แถวแรกประกาศเครื่องไปแล้ว → แถวถัดไปใน week เดียวกันถือว่าใช้เครื่องร่วม
                        if not _fill_shared_owner:
                            _fill_shared_owner = str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip()
                        qty_left -= produce
                        _rem_cap -= produce
                        # อัปเดต remaining cap หลังใช้งาน
                        _new_rem = max(0, _rem_cap)
                        if _new_rem > 0:
                            remaining_week_cap[_rem_cap_key] = _new_rem
                            # 🔧 FIX: ถ้า owner เดิมเป็น None (จาก booking seed) ให้คงไว้
                            # เพื่อไม่ให้ main loop คิดว่าเป็น new plan cap แล้ว override
                            if remaining_week_cap_owner.get(_rem_cap_key) is not None:
                                remaining_week_cap_owner[_rem_cap_key] = sc_so_no
                        else:
                            remaining_week_cap.pop(_rem_cap_key, None)
                            remaining_week_cap_owner.pop(_rem_cap_key, None)
                        # อัปเดต tracking สำหรับ cross-SC carryover
                        last_production[_fill_ck] = week_index(_fill_week)
                        machines_in_use[_fill_ck] = _fill_avail_mc
                        last_sc_so_no[_fill_ck] = sc_so_no
                        if order_type == "YD-ORDERS" and sub_color:
                            last_sub_color[_fill_ck] = sub_color
                        new_plan_started_items.add(_fill_ck)
                        # บันทึก cross-SC fill — ทั้งใน _existing_item_week_mc และ _already_filled_this_sc
                        _existing_item_week_mc.add((item, _fill_week, _fill_mc_group))
                        _already_filled_this_sc.add((_fill_week, item, _fill_mc_group))
                        _produced_week = _fill_week
                        _fill_last_week = _fill_week
                    else:
                        # cap เหลือน้อยเกินไป (rev_weight rounding) → mark week เต็ม (=0)
                        remaining_week_cap[_rem_cap_key] = 0
                        # 🔧 FIX: ถ้า owner เดิมเป็น None (จาก booking seed) ให้คงไว้
                        # เพื่อไม่ให้ main loop คิดว่าเป็น new plan cap แล้ว override ด้วย full capacity
                        if remaining_week_cap_owner.get(_rem_cap_key) is not None:
                            remaining_week_cap_owner[_rem_cap_key] = sc_so_no
                        break

                if qty_left <= 0:
                    break

            # หลัง item-carry fill: ถ้า fill เกิดที่ plan_week เดิม → ขยับ plan_week ไปสัปดาห์ถัดไป
            # เพื่อให้ main loop ไม่ setup เครื่องใหม่ทับในสัปดาห์ที่ item อื่นยังผลิตอยู่
            if qty_left > 0 and _fill_last_week is not None and _fill_last_week == plan_week:
                _next_after_fill = next_week(_fill_last_week)
                if _next_after_fill is not None:
                    print(f"[ITEM CARRY] {item}: advance plan_week W{plan_week} → W{_next_after_fill} (filled same week)")
                    plan_week = _next_after_fill
            # ถ้า fill เกิดที่สัปดาห์ก่อนหน้า plan_week → ย้อนกลับมา plan ต่อที่สัปดาห์นั้น (เดิม)
            elif qty_left > 0 and _fill_last_week is not None and not _earliest_backshift_done:
                _fl_idx = week_index(_fill_last_week)
                _pw_idx = week_index(plan_week)
                # ❗ floor: ห้าม backshift ย้อนเร็วกว่า TODAY+2 (สัปดาห์ที่ผลิตจริงได้)
                # และ (รอบ replan) ห้ามย้อนก่อนสัปดาห์ที่ด้ายเข้าครบ
                _bs_floor = max(TODAY_IDX + FREEZE_WEEKS, _point_idx or 0)
                if _fl_idx is not None and _pw_idx is not None and _fl_idx < _pw_idx and _fl_idx >= _bs_floor:
                    print(
                        f"[DEBUG BACKSHIFT] {item}: cross-SC filled W{_fill_last_week} while plan_week=W{plan_week} -> backshift to W{_fill_last_week}"
                    )
                    plan_week = _fill_last_week
                    _earliest_backshift_done = True

            # ยอดหมดตั้งแต่ลูปเก็บตก → กลับหัวลูป ให้ด่าน qty_left < 1 จบใบ / ไปรอบ core ถัดไปตามปกติ
            # เดิมเดินต่อไปขั้นวางด้วยยอด 0 → _bridge_holds เลื่อนโซ่ carry ไปถึงปลาย Hold ของรอบที่ไม่ต้องทอแล้ว
            # (เคสจริง 30 ก.ย. 2569 FD1BASMK32B0 RAOO: ใบ 719941 เติมเครื่อง Hold ครบที่ W44-45 แต่โซ่ถูกเลื่อน
            #  ไป W2702 → ใบ 708936 เห็น "เครื่องไม่ว่างถึง W2702" ทั้งที่ว่างตั้งแต่ W46 → ช้าลง 10 สัปดาห์)
            if qty_left < 1.0 and _fill_last_week is not None:
                continue

            # 🔧 REMOVED: Aggressive week-skipping when cross-SC fill marked week as full
            # This was preventing main planning loop from scheduling items in that week
            # with different mc_groups. Cross-SC fill should not block main planning.
            # if qty_left > 0:
            #     _items_in_week = [
            #         (_rk, _rv) for _rk, _rv in remaining_week_cap.items()
            #         if _rk[0] == plan_week and _rk[1] == item
            #     ]
            #     if _items_in_week and all(_rv == 0 for _, _rv in _items_in_week):
            #         # week full → skip to next week
            #         plan_week = next_week(plan_week)
            #         if plan_week is None:
            #             break
            #         continue

            # ⚠️ ตรวจสอบ RDD ก่อนว่าทันหรือไม่
            _current_order_rdd_idx = rdd_idx
            _plan_idx = week_index(plan_week)
            # เลื่อน anchor ตามตำแหน่งที่แผนเดินไปจริง — ทำให้ bare week ที่อ้างถึง
            # หลังจากนี้ (ทั้งในลูปและ helper) ถูกตีความเป็นปีที่ถูกต้องเมื่อแผนข้ามปี
            if _plan_idx is not None:
                set_week_anchor(_plan_idx)
            past_rdd = bool(
                rdd_idx is not None and _plan_idx is not None and _plan_idx >= rdd_idx
            )
            # CORE ITEM: ไม่สน RDD
            if _core_production_schedule:
                past_rdd = False
            if rdd_idx is not None and _plan_idx is not None and _plan_idx > rdd_idx:
                # urgent_mode = True  # DISABLED
                urgent_mode = False
            # ถ้ายังไม่ได้คำนวณ required_machines (เพราะตอนแรก avail=0 ทุก week)
            # ให้ลองคำนวณใหม่ด้วย plan_week ปัจจุบันที่มีเครื่องว่างจริง
            # 🔧 แก้ไข: คำนวณ required machines เสมอแม้ past RDD เพื่อให้มีข้อมูลสำหรับ planning
            print(f"[DEBUG CALC CHECK] required_machines_info={required_machines_info}, past_rdd={past_rdd}, rdd_idx={rdd_idx}, _plan_idx={_plan_idx}")
            if rdd_idx is not None and _plan_idx is not None:
                print(f"[DEBUG CALC CHECK] _plan_idx <= rdd_idx: {_plan_idx <= rdd_idx}")
            if (
                (rdd_idx is not None or _core_production_schedule)
                and _plan_idx is not None
            ):
                _locked_mc2 = locked_mc_group_for.get((sc_so_no, item))
                # CORE ITEM: ไม่สน RDD — ใช้ plan_week+5
                if _core_production_schedule:
                    target_knit_idx2 = _plan_idx + 5
                else:
                    target_knit_idx2 = rdd_idx
                _already_planned_item_qty = _item_cumulative_planned.get(item, 0)
                _dynamic_qty_for_calc = max(qty_left, _qty_for_machine_calc - _already_planned_item_qty)
                _mc_r, _cap_r, _req_r, _feas_r, _gauge_r = calculate_required_machines(
                    item,
                    _dynamic_qty_for_calc,
                    plan_week,
                    target_knit_idx2,  # TARGET_KNIT
                    setup_days=order_setup_days,
                    only_mc_group=_locked_mc2,
                    order_type=order_type,
                    sub_color=sub_color,
                    dye_end_date=order.get("DYE_END_DATE"),
                )
                if _req_r:
                    required_machines_info = (_mc_r, _cap_r, _req_r, _feas_r, _gauge_r)
                    if _mc_r and (sc_so_no, item) not in locked_mc_group_for:
                        locked_mc_group_for[(sc_so_no, item)] = _mc_r
                # S9 Logic: reset flag ต่อ iteration แล้วตรวจสอบใหม่
                _s9_active = False
                # AUTO (ส่วนที่ล้น target): ในบ้านวางมาเต็มที่จนถึง target แล้วยังมียอดค้าง
                # → เปิดจ้างทอเฉพาะยอดค้างนี้ (เปิดครั้งเดียว คงไว้จนจบใบ)
                if (
                    _s9_auto_base and not _s9_auto and qty_left > 0
                    and _plan_idx > rdd_idx and _gauge_r is not None
                ):
                    _tail_tgt_txt = (
                        f"W{int(calendar_week.iloc[rdd_idx]['WEEK_NUM'])}"
                        if 0 <= rdd_idx < len(calendar_week) else f"idx {rdd_idx}"
                    )
                    # ออกจ้างทอ AUTO ได้เฉพาะ priority Color (0) / Program (1) — Core / SC ทอในบ้านต่อแม้สาย
                    # (user 11 ก.ย. 2569) ; ไม่มีค่า priority = ไม่ให้ออก
                    _tail_prio_ok = order.get("PLAN_PRIORITY") in (0, 1)
                    _tail_elig = (item_upper, _normalize_gauge(_gauge_r)) in _s9_eligible_items
                    # ด่านเทียบความเร็ว (user 21 ก.ย. 2569): "เลย target" อย่างเดียวไม่พอ —
                    # ใบที่ด้ายเข้าช้ากว่า target เข้าเงื่อนไขตั้งแต่รอบแรกทั้งที่ยังไม่ได้วางในบ้านเลย
                    # จึงต้องถามต่อว่า "จ้างแล้วได้ของเร็วกว่าจริงไหม" ; ในบ้านจบไม่ช้ากว่า = ไม่ต้องจ้าง
                    _tail_in_fin = _tail_s9_fin = None
                    # ในบ้านพิสูจน์แล้วว่าเอาไม่อยู่ (เครื่องหลุดมือ/ไม่มีเครื่อง) → ไม่ต้องเทียบอีก
                    # การจำลองชุดเดิมเชื่อไม่ได้แล้วสำหรับใบนี้ ปล่อยยอดที่เหลือไปตามเกณฑ์จ้างทอปกติ
                    if _inhouse_stuck and not _s9_cmp_logged:
                        _s9_cmp_logged = True
                        print(f"[S9 AUTO TAIL] {item} SC {sc_so_no}: ข้ามด่านเทียบ — {_inhouse_stuck_why}")
                    if (
                        S9_COMPARE_INHOUSE and not _inhouse_stuck
                        and _tail_prio_ok and _tail_elig and _mc_r is not None
                    ):
                        # คีย์ต้องมียอดด้วย — สัปดาห์เดียวกันวางหลายรอบได้ (ยอดค้างลดระหว่างรอบ)
                        # ถ้าคีย์แค่สัปดาห์ จะตัดสินด้วยยอดเก่าที่มากกว่าจริง
                        _cmp_key = (
                            _plan_idx, round(float(qty_left), 1),
                            round(float(max(qty_left, _dynamic_qty_for_calc)), 1),
                        )
                        _cmp_hit = _s9_cmp_cache.get(_cmp_key)
                        if _cmp_hit is None:
                            # พื้นสัปดาห์เดียวกับที่ก้อนจ้างทอจะใช้จริง (lead time / ด้ายเข้า / ย้อมเสร็จ)
                            _cmp_floor = _s9_floor_idx()
                            if _point_idx is not None and _point_idx > _cmp_floor:
                                _cmp_floor = _point_idx
                            if order_type == "YD-ORDERS" and order_week is not None:
                                _cmp_yd = week_index(order_week)
                                if _cmp_yd is not None and _cmp_yd > _cmp_floor:
                                    _cmp_floor = _cmp_yd
                            # เครื่องในบ้านที่ item นี้วิ่งอยู่แล้วและยังอุ่น — นับเป็นฐานของการจำลอง
                            # (ไม่งั้นรอบถัดไปจะคิดว่าในบ้านต้องตั้งเครื่องใหม่จากศูนย์ทุกสัปดาห์
                            #  แล้วประเมินในบ้านต่ำเกินจริงจนส่งยอดที่เหลือออกไปจ้างทอ)
                            _cmp_ck = _resolve_carry_key(item, _mc_r, _gauge_r)
                            _cmp_last = last_production.get(_cmp_ck)
                            _cmp_prev_mc = 0
                            if _cmp_last is not None and 0 <= (_plan_idx - int(_cmp_last)) <= SETUP_GAP_WEEK:
                                _cmp_prev_mc = int(
                                    last_sc_machines.get((item, _mc_r, _gauge_r, sc_so_no),
                                                         machines_in_use.get(_cmp_ck, 0)) or 0
                                )
                            # ฝั่งในบ้านต้องแบก "ยอดคงเหลือของ item ทั้งหมด" ไม่ใช่แค่ก้อนนี้ —
                            # ก้อนอื่นของ item เดียวกัน (เช่น ส่วนที่ยังไม่เปิดสี priority 3)
                            # จ้างทอไม่ได้ จึงต้องลงพูลเดียวกันแน่นอน แล้วแย่งเครื่องกันเอง
                            # (เจอจริง: FD1BASRN127A0 ก้อนสี 2,544 + ก้อนไม่สี 2,496 บนพูล 9 เครื่อง
                            #  เทียบด้วยก้อนเดียวบอกว่า "ในบ้านทัน" แต่พอรวมสองก้อนคือแบกไม่ไหว)
                            # ฝั่งจ้างทอยังใช้ qty_left — ออกไปข้างนอกได้แค่ก้อนนี้ก้อนเดียว
                            _cmp_hit = (
                                _inhouse_finish_idx(
                                    max(qty_left, _dynamic_qty_for_calc),
                                    _plan_idx, _mc_r, _gauge_r, _cap_r,
                                    order_setup_days, item_code=item_upper,
                                    rev_weight=get_revolution_weight_from_orders(item, _mc_r),
                                    start_mc=_cmp_prev_mc,
                                ),
                                _s9_finish_idx(
                                    qty_left, _cmp_floor, _gauge_r,
                                    _mc_to_type1(_mc_r, _gauge_r), order_material_content,
                                    item_code=item_upper,
                                ),
                            )
                            _s9_cmp_cache[_cmp_key] = _cmp_hit
                        _tail_in_fin, _tail_s9_fin = _cmp_hit
                    # ในบ้านทอจบไม่ช้ากว่าจ้างทอ (หรือจ้างทอไม่จบเลย) → เก็บงานไว้ในบ้าน
                    _tail_inhouse_wins = (
                        _tail_in_fin is not None
                        and (_tail_s9_fin is None or _tail_in_fin <= _tail_s9_fin)
                    )
                    if _tail_prio_ok and _tail_elig and not _tail_inhouse_wins:
                        _s9_auto = True
                        _s9_auto_reason = f"จ้างทอ (auto: ส่วนที่ทอในบ้านไม่ทัน target {_tail_tgt_txt})"
                        print(f"[S9 AUTO TAIL] {item} SC {sc_so_no}: ในบ้านวางถึง target {_tail_tgt_txt} แล้ว "
                              f"ยังค้าง {qty_left:.1f} กก. → ส่งจ้างทอเฉพาะส่วนนี้")
                    elif _tail_inhouse_wins:
                        if not _s9_cmp_logged:
                            _s9_cmp_logged = True
                            print(f"[S9 AUTO SKIP] {item} SC {sc_so_no}: ค้าง {qty_left:.1f} กก. — "
                                  f"ในบ้านจบ {_week_txt_at(_tail_in_fin)} / จ้างทอจบ {_week_txt_at(_tail_s9_fin)} "
                                  f"→ จ้างแล้วไม่ได้เร็วกว่า ทอในบ้านต่อ")
                    elif not _s9_auto_skip_logged:
                        _s9_auto_skip_logged = True
                        if not _tail_elig:
                            print(f"[S9 AUTO] {item}: ไม่ทัน target แต่ (item, เกจ {_normalize_gauge(_gauge_r)}) "
                                  f"ไม่อยู่ในชีท Item S9 → ทอในบ้านต่อ")
                        else:
                            print(f"[S9 AUTO] {item} SC {sc_so_no}: ไม่ทัน target แต่ priority ไม่ใช่ Color/Program "
                                  f"({order.get('PLAN_PRIORITY')}) → ทอในบ้านต่อ")
                # S9 trigger ทุก iteration — 3 ทาง:
                #   1) item ใน "S9 Only" (MasterMC บังคับ ทอในบ้านไม่ได้)
                #   2) ก้อนที่ user สั่งจ้างทอเองจากหน้าเว็บ (_outsource_force)
                #   3) AUTO: ส่วนที่ล้นเลย TARGET_KNIT + (item, เกจ) อยู่ในชีท "Item S9" (เปิดข้างบน)
                # ผ้า S9 Only ที่มีข้อมูลในชีท S9 Only: เกจ/CAT มาจากชีทนั้น ไม่ต้องรอผลคำนวณเครื่องในบ้าน
                if required_machines_info or _s9o_spec is not None:
                    _item_u_s9 = str(item).strip().upper()
                    _s9_trigger = _item_u_s9 in _s9_only_items or _outsource_force or _s9_auto
                    if _s9_trigger and (_s9o_spec is not None or (_gauge_r is not None and _mc_r is not None)):
                        _is_s9_only = _item_u_s9 in _s9_only_items
                        if _s9o_spec is not None:
                            # user 24 ก.ย. 2569: S9 Only จ้างทอเท่านั้น — เกจ/CAT จากชีท S9 Only อย่างเดียว
                            # ไม่อิงเครื่องในบ้าน (ด่านต้นใบรับประกันแล้วว่ามีครบ)
                            _s9_g_t = _s9o_spec["gauge"]
                            _s9_mc_cat_t = _s9o_spec["cat"]
                        else:
                            _s9_g_t = _gauge_r
                            _s9_mc_cat_t = _mc_to_type1(_mc_r, _gauge_r)
                        _s9_auto_only = _s9_auto and not _is_s9_only and not _outsource_force
                        # ก้อน AUTO คิดจากยอดค้างของใบนี้เท่านั้น — _dynamic_qty_for_calc รวมยอดทุกใบของ item
                        _s9_qty_t = qty_left if _s9_auto_only else _dynamic_qty_for_calc
                        _s9_calc_week = plan_week
                        if _s9_auto_only and not _s9_week_locked:
                            # สัปดาห์เริ่มของส่วนที่ล้น: ถอยจาก target ด้วยกำลังเครื่อง S9
                            # พื้น: lead time จ้างทอ (TODAY+S9_LEAD_WEEKS) / ด้ายเข้า (POINT DATE) / ย้อมเสร็จ (YD)
                            _tail_floor = _s9_floor_idx()
                            if _point_idx is not None and _point_idx > _tail_floor:
                                _tail_floor = _point_idx
                            if order_type == "YD-ORDERS" and order_week is not None:
                                _tail_yd = week_index(order_week)
                                if _tail_yd is not None and _tail_yd > _tail_floor:
                                    _tail_floor = _tail_yd
                            _tail_bw = _s9_backward_start_idx(
                                _s9_qty_t, target_knit_idx2, _s9_g_t, _s9_mc_cat_t,
                                order_setup_days, order_material_content, floor_idx=_tail_floor,
                                item_code=_item_u_s9,
                            )
                            _tail_start = _tail_bw if _tail_bw is not None else _tail_floor
                            # สัปดาห์เริ่มพูลเต็ม → ไล่หาสัปดาห์แรกที่ยังมีเครื่อง S9 (ไม่เกินสัปดาห์ปัจจุบัน)
                            # ไม่งั้นด่าน blocked ข้างล่างดูแค่สัปดาห์ปัจจุบัน แล้วตกกลับไปทอในบ้านเลย target
                            _tail_pool = _s9_pool_row(_s9_g_t, _s9_mc_cat_t, order_material_content, _item_u_s9)
                            _s9_auto_tail_idx = None
                            if _tail_pool is not None:
                                for _ti in range(_tail_start, max(_tail_start, _plan_idx) + 1):
                                    if _ti >= len(calendar_week):
                                        break
                                    _twk = int(calendar_week.iloc[_ti]["WEEK"])
                                    if _twk in _s9_no_cap_weeks:
                                        continue
                                    if _s9_working_days(_twk) > 0 and _s9_pool_remain(_tail_pool[5], _twk) > 0:
                                        _s9_auto_tail_idx = _ti
                                        break
                            if _s9_auto_tail_idx is not None:
                                _s9_calc_week = int(calendar_week.iloc[_s9_auto_tail_idx]["WEEK"])
                                print(f"[S9 AUTO TAIL] {item}: เริ่มจ้างทอ W{_s9_calc_week} "
                                      f"(backward idx {_tail_bw}, พื้น idx {_tail_floor}, target idx {target_knit_idx2})")
                        _s9r = _calc_s9_required_machines(
                            _s9_qty_t, _s9_calc_week, target_knit_idx2,
                            _s9_g_t, _s9_mc_cat_t, order_setup_days, order_material_content,
                            item_code=_item_u_s9,
                        )
                        # ก้อน AUTO เท่านั้น: ถ้า S9 pool ไม่ว่าง/ไม่มีเกจตรง → ไม่ดันออกไปข้างนอก
                        # ทอในบ้านต่อยอมสายเหมือนเดิม (S9 Only / ที่ user สั่ง ยังบังคับเข้า S9)
                        _s9_auto_blocked = (
                            _s9_auto and not _is_s9_only and not _outsource_force
                            and (_s9r[0] is None or not _s9r[3])
                        )
                        if _s9_auto_blocked:
                            # ตกกลับมาทอในบ้าน: ไม่ได้ทำ backward ปกติไว้ (ข้ามไปตอน [BACKWARD SKIP])
                            # แต่ก้อนนี้ไม่ทัน target อยู่แล้ว → เริ่มเร็วที่สุดคือสิ่งที่ต้องการอยู่ดี
                            if not _s9_auto_blocked_logged:
                                print(f"[S9 AUTO] {item} W{plan_week}: S9 pool ไม่ว่าง/ไม่มีเกจตรง "
                                      f"→ ทอในบ้านต่อ (ยอมสาย)")
                                _s9_auto_blocked_logged = True
                        elif _s9r[0] is not None:
                            _s9_mc_g, _s9_cap_g, _s9_req_g, _s9_feas_g, _s9_gauge_g, _s9_pool_id_cur = _s9r
                            required_machines_info = (_s9_mc_g, _s9_cap_g, _s9_req_g, _s9_feas_g, _s9_gauge_g)
                            locked_mc_group_for[(sc_so_no, item)] = _s9_mc_g
                            _s9_active = True
                            _s9_cat_cur = _s9_mc_cat_t
                            _s9_mat_cur = order_material_content
                            # ===== สัปดาห์เริ่มของงานจ้างทอ =====
                            # พื้น = lead time จ้างทอ TODAY+S9_LEAD_WEEKS (user 16 ก.ย. 2569: อยู่ W38 → เร็วสุด W41)
                            # แล้วห้ามเริ่มก่อนด้ายเข้า (POINT DATE) / ก่อนย้อมเสร็จ (YD) เหมือนงานในบ้าน
                            # ต้องใส่ซ้ำตรงนี้เพราะบล็อกนี้ตั้ง plan_week ทับด่าน clamp ก่อนเข้าลูป
                            # ครั้งแรก → set plan_week ตามที่คำนวณได้  |  ครั้งถัดไป → advance อย่างเดียว
                            _s9_min_idx = _s9_floor_idx()
                            if _point_idx is not None and _point_idx > _s9_min_idx:
                                _s9_min_idx = _point_idx
                            if order_type == "YD-ORDERS" and order_week is not None:
                                _s9_yd_idx = week_index(order_week)
                                if _s9_yd_idx is not None and _s9_yd_idx > _s9_min_idx:
                                    _s9_min_idx = _s9_yd_idx
                            # backward (JIT) ของงานจ้างทอ: ถอยจาก target ด้วยกำลังผลิตของ S9 pool จริง
                            # → เริ่มช้าที่สุดเท่าที่ยังส่งทัน ไม่ยัดต้นตารางเมื่อ target ยังไกล
                            # คำนวณครั้งเดียวตอนตั้งสัปดาห์เริ่ม (_s9_week_locked) — รอบถัด ๆ ไป
                            # plan_week เดินตามลูปปกติแล้ว (advance อย่างเดียว ไม่ต่ำกว่าพื้น)
                            if not _s9_week_locked and _s9_auto_only:
                                # ส่วนที่ล้น target: ย้อนไปเริ่มสัปดาห์ที่หาไว้ข้างบน (ช่วงก่อน target ที่ S9 ยังว่าง)
                                _s9_min_idx = _s9_auto_tail_idx if _s9_auto_tail_idx is not None else _plan_idx
                            elif not _s9_week_locked and not (_outsource_force and _outsource_week is not None):
                                _s9_bw_idx = _s9_backward_start_idx(
                                    _dynamic_qty_for_calc,
                                    target_knit_idx2,
                                    _s9_g_t,
                                    _s9_mc_cat_t,
                                    order_setup_days,
                                    order_material_content,
                                    floor_idx=_s9_min_idx,
                                    item_code=_item_u_s9,
                                )
                                if _s9_bw_idx is not None and _s9_bw_idx > _s9_min_idx:
                                    print(f"[S9 JIT] {item}: backward → เริ่ม idx {_s9_bw_idx} "
                                          f"(พื้น lead จ้างทอ/ด้ายเข้า = idx {_s9_min_idx}, target idx {target_knit_idx2})")
                                    _s9_min_idx = _s9_bw_idx
                                elif _s9_bw_idx is None:
                                    print(f"[S9 JIT] {item}: ถอยเท่าไรก็ไม่ทัน target → เริ่มเร็วสุด "
                                          f"idx {_s9_min_idx} (พื้น lead จ้างทอ/ด้ายเข้า)")
                            # ก้อนจ้างทอ: user เป็นคนกำหนดสัปดาห์เอง → เริ่มที่สัปดาห์ที่กรอก
                            # (ถ้า S9 pool สัปดาห์นั้นเครื่องไม่พอ จะไหลต่อสัปดาห์ถัดไปตาม logic เดิม)
                            if _outsource_force and _outsource_week is not None:
                                _os_idx = week_index(int(_outsource_week))
                                if _os_idx is not None:
                                    _s9_min_idx = _os_idx
                            if _s9_min_idx < len(calendar_week):
                                _s9_target_week = int(calendar_week.iloc[_s9_min_idx]["WEEK"])
                                if not _s9_week_locked:
                                    if _s9_target_week not in _s9_no_cap_weeks:
                                        plan_week = _s9_target_week
                                        print(f"[S9] {item}: plan_week set to W{plan_week} (S9 first)")
                                    _s9_week_locked = True
                                else:
                                    _s9_pw_idx = week_index(plan_week) if plan_week else None
                                    # เทียบด้วย index ตรง ๆ — ห้ามแปลงเลขสัปดาห์เปล่ากลับ: เมื่อ plan_week อยู่ห่างพื้น
                                    # เกิน WEEK_LOOKBACK_LIMIT, week_index(พื้น) จะจับเป็นสัปดาห์ของปีหน้า → "advance"
                                    # ย้อน plan_week ไปปีหน้าจนหลุด horizon ลูปจบเงียบ ยอดค้างตก UNPLANNED
                                    # (เจอจริง 14 ก.ย. 2569 FD5BASPG10A0 SC 722428: W49 → "W40" ปีหน้า ค้าง 900 กก.)
                                    _s9_tgt_idx = _s9_min_idx
                                    if _s9_pw_idx is not None and _s9_tgt_idx is not None and _s9_pw_idx < _s9_tgt_idx:
                                        if _s9_target_week not in _s9_no_cap_weeks:
                                            plan_week = _s9_target_week
                                            print(f"[S9] {item}: plan_week set to W{plan_week} (S9 advance)")
                            _s9_tag = (
                                "S9_ONLY" if _is_s9_only
                                else ("USER_FORCE" if _outsource_force
                                      else ("AUTO+LATE" if past_rdd else "AUTO"))
                            )
                            print(f"[S9] {item}: S9 activated ({_s9_tag}) → MC={_s9_mc_g}, cap={_s9_cap_g}, req={_s9_req_g}")
                        elif _s9o_spec is not None:
                            # S9 Only หาผู้รับจ้างไม่ได้ (กันไว้ — ด่านต้นใบเช็คพูลแล้ว ปกติไม่ควรมาถึงตรงนี้)
                            # ห้ามตกกลับไปทอในบ้าน (user 24 ก.ย. 2569) → หยุดใบนี้ ยอดที่เหลือลง UNPLANNED
                            _s9o_stop = (
                                f"S9 Only: ไม่มีผู้รับจ้างที่รับ Guage {_s9_g_t} CAT {_s9_mc_cat_t}"
                                f" ผ้า {_s9_material_class(order_material_content)} (ชีท MC S9)"
                            )
                            print(f"⚠️  [S9 ONLY] {item} (SC/SO:{sc_so_no}): {_s9o_stop} → หยุดวางแผนใบนี้")
                            break
            # S9 Only ทอในบ้านไม่ได้ — รอบนี้เข้าเส้นทางจ้างทอไม่ได้ (เช่น ไม่มีกำหนดส่งให้คำนวณ) ก็ห้ามวางในบ้าน
            if _s9o_spec is not None and not _s9_active:
                _s9o_stop = "S9 Only: เข้าเส้นทางจ้างทอไม่ได้ (ไม่มีสัปดาห์กำหนดส่ง)"
                print(f"⚠️  [S9 ONLY] {item} (SC/SO:{sc_so_no}): {_s9o_stop} → หยุดวางแผนใบนี้")
                break
            # เลือกเครื่องที่เหมาะสมที่สุดสำหรับ item นี้
            # ถ้าเป็นกรณี RTS+LOCAL ให้บังคับใช้ MC เดิมและเริ่มหลัง old สุดท้าย
            mc_group = daily_capacity = available_machines = _sel_gauge = (
                None
            )
            # RTS: บังคับ MC_GROUP ตาม booking เดิม (detail_mc)
            # เพราะ get_best_machine_for_item อาจเลือก MC_GROUP อื่น (เช่น SKPTA→SKP)
            if rts_local_force:
                print(f"[DEBUG RTS] Processing RTS for {item} in week {plan_week}")
                _rts_old_mc = rts_local_force.get("last_old_by_mc", {})
                if _rts_old_mc:
                    # เลือก MC_GROUP ที่มี last week ล่าสุด
                    _rts_mc = max(_rts_old_mc, key=_rts_old_mc.get)
                    _rts_machines = rts_local_force.get("machines_by_mc", {}).get(
                        _rts_mc, 0
                    )
                    _rts_cap = rts_local_force.get("daily_cap_by_mc", {}).get(_rts_mc)
                    print(f"[DEBUG RTS] RTS MC: {_rts_mc}, machines: {_rts_machines}")
                    if _rts_machines > 0:
                        mc_group = _rts_mc
                        available_machines = _rts_machines
                        _sel_gauge = _item_mc_to_gauge.get(
                            (str(item).strip().upper(), str(_rts_mc).strip().upper()), None
                        )
                        # daily_capacity: ใช้ capacity ตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน
                        daily_capacity = _get_capacity_for_mc_group(item, _rts_mc, _sel_gauge)
                        if daily_capacity <= 0:
                            # Fallback: use original logic if new logic returns 0
                            _all_cap_rts = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                            if not _all_cap_rts.empty:
                                daily_capacity = _normalize_capacity(item, _rts_mc, float(_all_cap_rts["CAP ทอ"].min()))
                            elif _rts_cap and not pd.isna(_rts_cap) and float(_rts_cap) > 0:
                                daily_capacity = _normalize_capacity(item, _rts_mc, float(_rts_cap))
                            else:
                                _rts_cap_row = item_cap_data[
                                    (item_cap_data["ITEM_CODE"] == item)
                                    & (item_cap_data["MC_GROUP"] == _rts_mc)
                                ]
                                if not _rts_cap_row.empty:
                                    daily_capacity = _normalize_capacity(
                                        item, _rts_mc,
                                        float(_rts_cap_row.iloc[0].get("CAP ทอ", 0) or 0)
                                    )
                        daily_capacity = adjust_daily_cap_for_item_special(daily_capacity, item, _rts_mc, _sel_gauge)
                        #  FIX: Recalculate required_machines_info for current week in RTS logic
                        #  This prevents using cached values from previous weeks (e.g., week 26 values in week 27)
                        _locked_mc2 = locked_mc_group_for.get((sc_so_no, item))
                        target_knit_idx2 = rdd_idx if not _core_production_schedule else (week_index(plan_week) + 5)
                        _already_planned_item_qty = _item_cumulative_planned.get(item, 0)
                        _dynamic_qty_for_calc = max(qty_left, _qty_for_machine_calc - _already_planned_item_qty)
                        _mc_r, _cap_r, _req_r, _feas_r, _gauge_r = calculate_required_machines(
                            item,
                            _dynamic_qty_for_calc,
                            plan_week,
                            target_knit_idx2,
                            setup_days=order_setup_days,
                            only_mc_group=_locked_mc2,
                            order_type=order_type,
                            sub_color=sub_color,
                            dye_end_date=order.get("DYE_END_DATE"),
                        )
                        if _req_r:
                            required_machines_info = (_mc_r, _cap_r, _req_r, _feas_r, _gauge_r)
                            print(f"[DEBUG RTS RECALC] Week {plan_week}: updated required_machines_info=({_mc_r}, {_cap_r}, {_req_r}, {_feas_r}, {_gauge_r})")
            # ===== งานสี: เตรียมเครื่องตามทรงก่อนเลือกเครื่อง (main color grab) =====
            # ยึดเครื่องคืนจากงานที่ยังไม่ให้สีให้พอตามทรงของสัปดาห์นี้ "และสัปดาห์ถัดไป"
            # เครื่องที่ยึดได้จะถูกคืนเข้าบัญชีพูลทันที → get_best_machine_for_item /
            # get_load_balanced_machine เห็นเครื่องว่างจริงในบรรทัดถัดไปโดยไม่ต้องแก้ทางเดิน
            #
            # ⚠️ ต้องยึดของสัปดาห์ถัดไปด้วย ไม่งั้นติดด่าน "Hard gate (W+1)" ของ Load Balancing
            # และเครื่องที่เพิ่งตั้งจะ carry ต่อไม่ได้ = จ่ายค่า setup ทิ้ง (เคส FD4DRTRN74/03A0
            # W38 ตั้ง 2 เครื่อง setup 10 วัน แล้ว W39 พูลว่าง 0 → หลุดทั้งชุด)
            if (
                MAIN_COLOR_GRAB_ENABLED
                and _CUR_ORDER_COLOR_PART
                and not _s9_active
                and not _outsource_force
                and required_machines_info
            ):
                _grab_mc = required_machines_info[0] or mc_group or _ord_mc_grp
                _grab_g = required_machines_info[4]
                # หมายเหตุ: พูลคิดรายสัปดาห์ — เครื่องที่ item นี้ carry มายังไม่ถูกหักออกจาก
                # พูลของสัปดาห์นี้ (หักตอนเขียนแถว) → เครื่องว่างที่เห็นรวมของตัวเองอยู่แล้ว
                # จึงต้องคิดส่วนที่ขาดจาก "ทรง − เครื่องว่าง" ตรง ๆ ห้ามลบ carry ซ้ำ
                _grab_tk = (
                    int(calendar_week.iloc[rdd_idx]["WEEK"])
                    if rdd_idx is not None and 0 <= rdd_idx < len(calendar_week)
                    else None
                )
                _gw_prev_want = 0
                for _gw_off in (0, 1):
                    _gw = plan_week if _gw_off == 0 else next_week(plan_week)
                    if _gw is None:
                        break
                    _gw_want = 0
                    if progressive_plan and _gw in progressive_plan:
                        _gw_want = int(progressive_plan.get(_gw) or 0)
                    elif _gw_off == 0:
                        _gw_want = int(required_machines_info[2] or 0)
                    # สัปดาห์ถัดไปที่ทรงไม่ได้ระบุ (แต่งานยังไม่จบ) ต้องมีที่ยืนให้ carry
                    # อย่างน้อยเท่าสัปดาห์นี้ ไม่งั้นด่าน NO CARRY ROOM จะเด้งการวางทิ้ง
                    if _gw_off == 1 and _gw_want <= 0:
                        _gw_want = _gw_prev_want
                    if _gw_want <= 0:
                        continue
                    _gw_prev_want = _gw_want
                    _gw_got = _main_grab_for_week(
                        item, _grab_mc, _grab_g, _gw, _gw_want, tk_week=_grab_tk,
                    )
                    if _gw_got > 0:
                        print(
                            f"   🎨 [COLOR GRAB] {item} {_grab_mc} G{_grab_g} W{_gw}: "
                            f"ยึดเครื่องคืน {_gw_got} เครื่อง (ทรงต้องการ {_gw_want})"
                        )

            # คำนวณ _req_feasible ก่อนเพื่อส่งเข้า get_best_machine_for_item
            _req_feasible = (
                required_machines_info[3]
                if required_machines_info and len(required_machines_info) > 3
                else True
            )
            # 🔧 แก้ไข: ให้ Load Balancing ทำงานทุกครั้ง ไม่ใช่แค่ครั้งแรก
            # ถ้ามี required_machines_info ให้ใช้ Load Balancing ทุกครั้งเพื่อจัดการ Gradual Increase
            print(f"[DEBUG LB CHECK] {item} week {plan_week}: required_machines_info={required_machines_info}, USE_LOAD_BALANCING={USE_LOAD_BALANCING}")
            print(f"[DEBUG LB CHECK] {item} week {plan_week}: mc_group={mc_group}, rts_local_force={bool(rts_local_force)}")

            # ⚠️ ตรวจสอบว่าเป็น carryover หรือไม่ก่อนเรียก Load Balancing
            # ถ้าเป็น carryover → ใช้เครื่องเดิมโดยไม่ต้องเช็ค available_machines
            # ต้องหา mc_group จาก last_production ก่อน เพราะ mc_group อาจยัง None
            # 🔧 FIX: validate ว่า _prev_mc_group ต้องตรงกับ MC_GROUP จาก order หรือมีใน item_cap_data
            #   เพื่อป้องกัน carryover จาก MC_GROUP ผิดที่ค้างอยู่ใน old plan (เช่น SKP แทน SKPTA)
            _valid_mc_groups_for_item = set(
                str(r).strip().upper()
                for r in item_cap_data.loc[
                    item_cap_data["ITEM_CODE"] == item, "MC_GROUP"
                ]
            )
            _prev_mc_key = None
            _prev_mc_group = None
            for _key, _week_idx in last_production.items():
                if _key[0] == item and _week_idx is not None:
                    # 🔧 FIX: ข้าม MC_GROUP ที่ไม่ตรงกับ order และไม่มีใน item_cap_data
                    _candidate_mc = _key[1]
                    if _candidate_mc != _ord_mc_grp and _candidate_mc not in _valid_mc_groups_for_item:
                        continue
                    current_week_idx = week_index(plan_week)
                    _gap_week = (
                        current_week_idx - _week_idx
                        if current_week_idx is not None
                        else None
                    )
                    # ต้องอยู่ภายใน SETUP_GAP_WEEK ของ plan_week
                    # AND ต้องไม่เก่ากว่า TODAY_WEEK − SETUP_GAP_WEEK (ไม่ carry จาก booking เก่าเกินไป)
                    # AND ต้องเป็นสัปดาห์ก่อนหน้าเท่านั้น (ห้ามดึงสัปดาห์อนาคตมาเป็น carry)
                    if (
                        current_week_idx is not None
                        and _gap_week is not None
                        and _gap_week >= 0
                        and _gap_week <= SETUP_GAP_WEEK
                        and _week_idx >= TODAY_IDX
                    ):
                        _prev_mc_key = _key
                        _prev_mc_group = _key[1]
                        break
                    # 🔧 NEW: ถ้า last_production เป็นสัปดาห์อนาคต (gap < 0) แต่ booking active
                    # ครอบคลุม plan_week ด้วย (booking หลาย week ต่อเนื่อง) → อนุญาต carry
                    # เพราะเครื่องกำลังวิ่งอยู่ใน plan_week แล้วจาก booking เดิม
                    if (
                        current_week_idx is not None
                        and _gap_week is not None
                        and _gap_week < 0
                        and _key in booking_active_week_set
                        and current_week_idx in booking_active_week_set[_key]
                    ):
                        _prev_mc_key = _key
                        _prev_mc_group = _key[1]
                        print(f"[CARRY BOOKING SPAN] {item} week {plan_week}: booking active ครอบ plan_week (last_production=W{int(calendar_week.iloc[_week_idx]['WEEK'])}, booking weeks={sorted(booking_active_week_set[_key])}) → อนุญาต carry")
                        break

            _is_carryover = _prev_mc_key is not None and _prev_mc_group is not None
            # CORE ITEM: always fresh setup — no carry history
            if _core_production_schedule:
                _is_carryover = False

            # S9 จ้างทอ: ข้ามการเลือกเครื่องปกติทั้งหมด → ใช้ข้อมูล S9 pool โดยตรง
            _s9_carry_now = 0  # เครื่องจ้างทอที่ทอต่อจากสัปดาห์ก่อน (ไม่ต้องตั้งใหม่)
            if _s9_active and required_machines_info and required_machines_info[0] and required_machines_info[2] > 0:
                mc_group = required_machines_info[0]
                daily_capacity = required_machines_info[1]
                available_machines = int(required_machines_info[2])
                _sel_gauge = required_machines_info[4]
                _is_carryover = False  # S9 จ้างทอ: เครื่องแยกจากแผนปกติ ไม่ carry
                # เพดานของสัปดาห์ที่กำลังวางจริง — required_machines_info ถูกคำนวณตอน plan_week
                # ก่อนหน้า (ก่อนถูกเลื่อนไปสัปดาห์เริ่มจ้างทอ / backward) ถ้าไม่ตัดซ้ำที่นี่จะจองเกิน
                # Total MC ของ pool (เจอจริง: W40 RAP เกจ 22 จอง 38 ตัวจากที่มี 18)
                _s9_remain_now = _s9_pool_remain(_s9_pool_id_cur, plan_week)
                if available_machines > _s9_remain_now:
                    print(f"[S9 CAP] {item} W{plan_week} พูล {_s9_pool_id_cur}: "
                          f"ขอ {available_machines} เครื่อง แต่ pool เหลือ {_s9_remain_now} → ตัดลง")
                    available_machines = _s9_remain_now
                # ค่อย ๆ เพิ่มเครื่องทีละ S9_RAMP_STEP เหมือนงานในบ้าน (user 10 ก.ย. 2569)
                # นับรวมทุกใบสั่งของ item เดียวกันบนพูล·เกจนี้ — แตกหลายใบก็หนีเพดานไม่ได้
                if S9_RAMP_ENABLED:
                    _s9_g_rp = _normalize_gauge(_sel_gauge)
                    _s9_idx_rp = week_index(plan_week)
                    _s9_idx_rp = int(_s9_idx_rp) if _s9_idx_rp is not None else None
                    _s9_prev_rp = _s9_prev_week_mc(item, mc_group, _s9_g_rp, _s9_idx_rp)
                    _s9_now_rp = int(_s9_item_week_mc.get(
                        (str(item).strip().upper(), str(mc_group).strip().upper(), _s9_g_rp, _s9_idx_rp), 0
                    ))
                    _s9_room_rp = max(0, _s9_prev_rp + S9_RAMP_STEP - _s9_now_rp)
                    if available_machines > _s9_room_rp:
                        print(f"[S9 RAMP] {item} W{plan_week} {mc_group} g{_s9_g_rp}: "
                              f"ขอ {available_machines} เครื่อง แต่สัปดาห์ก่อนมี {_s9_prev_rp} "
                              f"(ใบอื่นของ item นี้ใช้แล้ว {_s9_now_rp}) → ได้ {_s9_room_rp}")
                        available_machines = _s9_room_rp
                    _s9_carry_now = min(available_machines, max(0, _s9_prev_rp - _s9_now_rp))
                carryover_mc = _s9_carry_now
                new_mc = available_machines - _s9_carry_now
                print(f"[S9 MACHINE] {item} W{plan_week}: MC={mc_group}, cap={daily_capacity}, avail={available_machines}, gauge={_sel_gauge}")

            # ก้อนจ้างทอ: ถ้าสัปดาห์นี้ S9 pool ไม่ว่าง (S9 ไม่ activate) → เลื่อนไปสัปดาห์ถัดไป
            # ห้าม fallback ไปเลือกเครื่องในบ้านเด็ดขาด (user แบ่งก้อนนี้ออกไปทอข้างนอกแล้ว)
            if _outsource_force and not _s9_active:
                print(f"[OUTSOURCE] {item} W{plan_week}: เครื่องจ้างทอ (S9) ไม่ว่าง → เลื่อนไปสัปดาห์ถัดไป")
                plan_week = next_week(plan_week)
                if plan_week is None:
                    break
                continue
            # ส่วนที่ล้น target ที่ย้อนมาวางจ้างทอช่วงก่อน/ถึง target แต่สัปดาห์นี้ S9 ไม่ว่าง
            # → เลื่อนไปสัปดาห์ถัดไป ห้ามตกกลับไปเลือกเครื่องในบ้าน (ในบ้านวางช่วงนี้ไปเต็มที่แล้ว
            # วางซ้ำจะได้งานในบ้านซ้อนสัปดาห์เดิม) ; เลย target แล้ว S9 ไม่ว่าง = ทอในบ้านต่อยอมสาย
            if _s9_auto and not _s9_active and _s9_week_locked and rdd_idx is not None:
                _tail_pw_idx = week_index(plan_week)
                if _tail_pw_idx is not None and _tail_pw_idx <= rdd_idx:
                    print(f"[S9 AUTO TAIL] {item} W{plan_week}: เครื่องจ้างทอไม่ว่าง (ช่วงก่อน target) "
                          f"→ เลื่อนไปสัปดาห์ถัดไป")
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break
                    continue

            # ถ้าเป็น carryover แต่มีเครื่องว่างมากและ order ยังเหลือเยอะ → ให้ Load Balancing ทำงาน
            # ถ้าเป็น carryover ปกติ → ใช้เครื่องเดิมต่อไป
            # ตรวจสอบว่ามีเครื่องว่างพอที่จะเพิ่มหรือไม่
            _has_capacity_for_increase = False
            if _is_carryover and _prev_mc_group:
                # หา gauge จาก last_production key
                _carry_gauge = _prev_mc_key[2] if len(_prev_mc_key) > 2 else None
                _actual_remain = get_actual_mc_remain(_prev_mc_group, plan_week, gauge=_carry_gauge, item_code=item)
                _current_machines = machines_in_use.get(_prev_mc_key, 1)
                _has_capacity_for_increase = _actual_remain > 0
            # ตรวจสอบว่า order เหลือเยอะพอที่จะเพิ่มเครื่องหรือไม่
            _needs_increase = False
            _total_item_demand = max(qty_left, _qty_for_machine_calc - (order_qty - qty_left))

            if _is_carryover and qty_left > 0:
                # ประมาณว่าถ้าเหลือมากกว่า 2 สัปดาห์ อาจต้องการเพิ่มเครื่อง
                _cap_row = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == _prev_mc_group)
                ]
                if not _cap_row.empty:
                    _ni_gauge = _cap_row.iloc[0].get("GUAGE")
                    _daily_cap = _get_capacity_for_mc_group(item, _prev_mc_group, _ni_gauge)
                    if _daily_cap <= 0:
                        _daily_cap = _normalize_capacity(item, _prev_mc_group, float(_cap_row.iloc[0]["CAP ทอ"]))
                    if _daily_cap > 0:
                        # 🔧 FIX: หัก production ที่ผลิตไปแล้วออกจาก _qty_for_machine_calc
                        # รวมทั้ง SC อื่นของ item เดียวกันที่วางแผนเสร็จแล้ว (_item_cumulative_planned)
                        # เพื่อไม่ให้นับ demand ซ้ำจาก SC ที่ planned ไปแล้ว
                        _already_produced_this_sc = order_qty - qty_left
                        _adjusted_machine_calc = max(0, _qty_for_machine_calc - _already_produced_this_sc)
                        _total_item_demand = max(qty_left, _adjusted_machine_calc)
                        # ใช้ effective machines = min(เครื่องเดิม, cap ที่เหลือ) ในการคำนวณว่าทันไหม
                        _effective_machines = min(_current_machines, _actual_remain) if _actual_remain > 0 else 0
                        if _effective_machines > 0:
                            _weeks_remaining = _total_item_demand / (_effective_machines * _daily_cap * 5)
                            _needs_increase = _weeks_remaining > 2.0  # เหลือมากกว่า 2 สัปดาห์
                        else:
                            _needs_increase = True  # ไม่มี cap เหลือ → ยังไม่ทันแน่นอน
            # 🔧 แก้ไข: แยก 3 กรณี
            # 1. Carryover ที่มีเครื่องว่างและต้องการเพิ่ม → ส่งไป Load Balancing
            # 2. Carryover ที่ไม่มีเครื่องว่างหรือไม่ต้องการเพิ่ม → ใช้เครื่องเดิมต่อไป
            # 3. ไม่ใช่ carryover → ส่งไป Load Balancing ปกติ

            should_check_increase = _is_carryover and _has_capacity_for_increase and _needs_increase

            # [SETUP EARLY PRIORITY] และ [SETUP EARLY] ถูกลบออก → วางแผนตาม TARGET_KNIT (JIT) แทน
            # 🔧 สำคัญ: ถ้าเป็น carryover ให้จัดการก่อน mc_group check
            # กรณีที่ 1 & 2: Carryover ทุกกรณีที่ไม่ต้องการเพิ่มเครื่อง (ไม่มีเครื่องว่างหรือไม่จำเป็น)
            if _is_carryover and not should_check_increase:
                # Carryover แบบไม่ต้องเพิ่มเครื่อง: ใช้เครื่องเดิม แต่ต้องเช็ค actual_remain
                # ถ้า old plan จองเครื่องไปหมดแล้ว (คนละ SC) → ไม่ควร carry เพิ่ม
                mc_group = _prev_mc_group
                # ใช้ค่าจริงจาก plan ก่อนหน้าสำหรับ carryover
                _requested_machines = machines_in_use.get(_prev_mc_key, 1)
                # required_machines_info ใช้เฉพาะสำหรับ target gradual increase ไม่ใช้ค่า carryover จริง
                if required_machines_info and len(required_machines_info) > 0:
                    _required_mc = required_machines_info[2]
                    if _required_mc > 3:
                        print(f"[DEBUG CARRY] Actual carry from previous week: {_requested_machines} machines (target for gradual increase: {_required_mc})")
                _carry_gauge = _prev_mc_key[2] if len(_prev_mc_key) > 2 else None
                _actual_remain = get_actual_mc_remain(_prev_mc_group, plan_week, gauge=_carry_gauge, item_code=item)
                # Same SC ใช้เครื่องเดิมได้เต็มจำนวน (เครื่องเป็นของ SC นั้น) แต่ต้องเช็ค actual_remain ด้วย
                # เพราะ actual_remain คือ cap จริงที่เหลือหลังหัก old booking + new plan อื่นๆ
                _carry_key_for_sc = _resolve_carry_key(item, _prev_mc_group, _carry_gauge)
                _last_sc = last_sc_so_no.get(_carry_key_for_sc)
                _same_sc_carry = (_last_sc == sc_so_no)
                if _requested_machines > _actual_remain:
                    print(
                        f"[CARRY CLAMP] {item} SC {sc_so_no} W{plan_week}: "
                        f"requested {_requested_machines} > actual_remain {_actual_remain} "
                        f"(same_sc={_same_sc_carry}, last SC {_last_sc}) → clamp to {max(0, _actual_remain)}"
                    )
                    _requested_machines = max(0, int(_actual_remain))
                available_machines = _requested_machines
                # ใช้ daily_capacity เดิม
                _cap_row = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == mc_group)
                ]
                if not _cap_row.empty:
                    _sel_gauge = _cap_row.iloc[0].get("GUAGE")
                    daily_capacity = _get_capacity_for_mc_group(item, mc_group, _sel_gauge)
                    if daily_capacity <= 0:
                        daily_capacity = _normalize_capacity(item, mc_group, float(_cap_row.iloc[0]["CAP ทอ"]))
                else:
                    # Fallback: หา capacity จาก item ทั่วไป
                    _all_cap = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                    if not _all_cap.empty:
                        _sel_gauge = _all_cap.iloc[0].get("GUAGE")
                        daily_capacity = _get_capacity_for_mc_group(item, mc_group, _sel_gauge)
                        if daily_capacity is None or daily_capacity <= 0:
                            daily_capacity = _normalize_capacity(item, mc_group, float(_all_cap["CAP ทอ"].min()))
                    else:
                        # ไม่มี capacity เลย → ข้ามไป
                        print(f"[WARNING] No capacity found for {item} in {mc_group}, skipping to next week")
                        plan_week = next_week(plan_week)
                        if plan_week is None:
                            break      # หมดสัปดาห์ให้วาง (สุด horizon) — ที่เหลือเป็นงานล้น
                        continue
                daily_capacity = adjust_daily_cap_for_item_special(daily_capacity, item, mc_group, _sel_gauge)
            elif (USE_LOAD_BALANCING or should_check_increase) and not _s9_active:
                print(f"[DEBUG LB] Using Load Balancing for {item} in week {plan_week}")
                # ดึง daily_capacity จาก required_machines_info ถ้ามี
                _input_daily_cap = required_machines_info[1] if required_machines_info and len(required_machines_info) > 1 else 0
                # ส่งข้อมูลเครื่องปัจจุบันสำหรับกรณี carryover ที่ต้องการเพิ่ม
                _current_machines = machines_in_use.get(_prev_mc_key, 1) if _prev_mc_key else 1

                # 🔧 ตรวจสอบว่าเป็นการใช้ครั้งแรกของ MC_GROUP นี้หรือไม่
                current_week_idx = week_index(plan_week)
                current_week_num = int(current_week_idx) if isinstance(current_week_idx, str) else current_week_idx
                _candidate_mc_for_first_use = (
                    _prev_mc_group
                    if (_is_carryover and _prev_mc_group)
                    else (
                        required_machines_info[0]
                        if required_machines_info and len(required_machines_info) > 0
                        else None
                    )
                )
                is_first_time_use = _calc_first_time_mc_use(
                    _candidate_mc_for_first_use, current_week_num, weekly_mc_usage
                )
                # ถ้าเป็น carryover ให้ใช้ daily_capacity จากเครื่องปัจจุบัน
                if _is_carryover and _input_daily_cap == 0:
                    _carry_cap_row = item_cap_data[
                        (item_cap_data["ITEM_CODE"] == item)
                        & (item_cap_data["MC_GROUP"] == _prev_mc_group)
                    ]
                    if not _carry_cap_row.empty:
                        _carry_gauge = _carry_cap_row.iloc[0].get("GUAGE")
                        _input_daily_cap = _get_capacity_for_mc_group(item, _prev_mc_group, _carry_gauge)
                        if _input_daily_cap <= 0:
                            _input_daily_cap = _normalize_capacity(item, _prev_mc_group, float(_carry_cap_row.iloc[0]["CAP ทอ"]))
                    _input_daily_cap = adjust_daily_cap_for_item_special(_input_daily_cap, item, _prev_mc_group, _carry_gauge)
                    print(f"[DEBUG CARRY] Using daily_capacity from carryover: {_input_daily_cap}")


                # 🔧 สำคัญ: สำหรับ carryover ที่ต้องการเพิ่มเครื่อง ต้องรักษา mc_group เดิม
                if _is_carryover and should_check_increase:
                    # บังคับให้ใช้ mc_group เดิม ไม่ให้ Load Balancing เปลี่ยน
                    print(f"[DEBUG CARRY] Force using original mc_group: {_prev_mc_group} for carryover increase")
                    # ส่ง mc_group เดิมไปให้ Load Balancing และบอกว่าเป็น carryover
                    # 🔧 FIX: ใช้ _total_item_demand แทน qty_left เพื่อรวม demand จาก FG ถัดไปของ item เดียวกัน
                    # ป้องกันกรณีที่ FG ปัจจุบันเหลือน้อย แต่ยังมี FG อื่นรอผลิต → ควรเพิ่มเครื่องได้
                    _already_produced_this_sc = order_qty - qty_left
                    _already_produced_other_sc = _item_cumulative_planned.get(item, 0)
                    _already_produced = _already_produced_this_sc + _already_produced_other_sc
                    _adjusted_machine_calc = max(0, _qty_for_machine_calc - _already_produced)
                    _total_item_demand = max(qty_left, _adjusted_machine_calc)
                    mc_group_result, daily_capacity, _, available_machines, _sel_gauge = (
                        get_load_balanced_machine(
                            item,
                            plan_week,
                            last_production,
                            required_machines_info,
                            urgent_mode,
                            past_rdd,
                            force_max_mc=(not _req_feasible and not past_rdd and not is_first_time_use),
                            qty_left=_total_item_demand,
                            daily_capacity=_input_daily_cap,
                            progressive_plan=progressive_plan,
                            current_machines=_current_machines,
                            qty_left_current_fg=qty_left,
                        )
                    )
                    # รักษา mc_group เดิมเสมอ
                    mc_group = _prev_mc_group
                    # ถ้า Load Balancing คืนค่า machines มา ให้ใช้ค่านั้น
                    if available_machines is not None and available_machines > _current_machines:
                        print(f"[DEBUG CARRY] Load Balancing approved increase: {_current_machines} → {available_machines} machines")
                    else:
                        print(f"[DEBUG CARRY] Load Balancing no increase: keep {_current_machines} machines")
                        available_machines = _current_machines
                else:
                    mc_group, daily_capacity, _, available_machines, _sel_gauge = (
                        get_load_balanced_machine(
                            item,
                            plan_week,
                            last_production,
                            required_machines_info,
                            urgent_mode,
                            past_rdd,
                            force_max_mc=(not _req_feasible and not past_rdd and not is_first_time_use),
                            qty_left=_total_item_demand,
                            daily_capacity=_input_daily_cap,
                            progressive_plan=progressive_plan,
                            current_machines=_current_machines,
                        )
                    )
            elif mc_group is None:
                # ดึง daily_capacity จาก required_machines_info ถ้ามี
                _input_daily_cap = required_machines_info[1] if required_machines_info and len(required_machines_info) > 1 else 0
                _current_machines = machines_in_use.get(_prev_mc_key, 1) if _prev_mc_key else 1
                # สายนี้ (งาน S9) ไม่ได้ผ่านสาย Load Balancing → ต้องคำนวณเองไม่งั้นได้ค่าค้าง
                _cur_widx_ftu = week_index(plan_week)
                _candidate_mc_for_first_use = (
                    _prev_mc_group
                    if (_is_carryover and _prev_mc_group)
                    else (
                        required_machines_info[0]
                        if required_machines_info and len(required_machines_info) > 0
                        else None
                    )
                )
                is_first_time_use = _calc_first_time_mc_use(
                    _candidate_mc_for_first_use,
                    int(_cur_widx_ftu) if isinstance(_cur_widx_ftu, str) else _cur_widx_ftu,
                    weekly_mc_usage,
                )
                mc_group, daily_capacity, _, available_machines, _sel_gauge = (
                    get_load_balanced_machine(
                        item,
                        plan_week,
                        last_production,
                        required_machines_info,
                        urgent_mode,
                        past_rdd,
                        force_max_mc=(not _req_feasible and not past_rdd and not is_first_time_use),
                        qty_left=_total_item_demand,
                        daily_capacity=_input_daily_cap,
                        progressive_plan=progressive_plan,
                        current_machines=_current_machines,
                    )
                )
            if mc_group is None:
                plan_week = next_week(plan_week)
                if plan_week is None:
                    break          # หมดสัปดาห์ให้วาง (สุด horizon) — ที่เหลือเป็นงานล้น
                continue

            # ถ้า mc_group+gauge อยู่ใน MC_GROUP_REDIRECT → เปลี่ยนไปใช้ target แทนเสมอ
            # เช่น SKP 20 → FA 20 (อ้อมน้อย) เพื่อให้วางแผนและหักเครื่องถูกที่
            _redir_mc, _redir_gauge = _apply_mc_redirect(mc_group, _sel_gauge)
            if _redir_mc != mc_group:
                print(f"[MC_REDIRECT] {mc_group}/{_sel_gauge} → {_redir_mc}/{_redir_gauge} for item {item} week {plan_week}")
                mc_group = _redir_mc
                _sel_gauge = _redir_gauge

            # Fallback: ถ้า daily_capacity ยัง None ให้หาจาก item_cap_data
            if daily_capacity is None or daily_capacity <= 0:
                _fallback_cap_row = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == mc_group)
                ]
                if not _fallback_cap_row.empty:
                    _sel_gauge = _fallback_cap_row.iloc[0].get("GUAGE")
                    daily_capacity = _get_capacity_for_mc_group(item, mc_group, _sel_gauge)
                    if daily_capacity is None or daily_capacity <= 0:
                        daily_capacity = _normalize_capacity(item, mc_group, float(_fallback_cap_row.iloc[0]["CAP ทอ"]))
                else:
                    # หาจาก item ทั่วไป
                    _fallback_all = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                    if not _fallback_all.empty:
                        daily_capacity = _normalize_capacity(item, mc_group, float(_fallback_all["CAP ทอ"].min()))
                    else:
                        print(f"[WARNING] No capacity found for {item} in {mc_group}, skipping to next week")
                        plan_week = next_week(plan_week)
                        if plan_week is None:
                            break      # หมดสัปดาห์ให้วาง (สุด horizon) — ที่เหลือเป็นงานล้น
                        continue
                daily_capacity = adjust_daily_cap_for_item_special(daily_capacity, item, mc_group, _sel_gauge)
            # ถ้ามี progressive_plan → ใช้จำนวนเครื่องที่คำนวณไว้ล่วงหน้า
            # S9: ไม่ใช้ progressive_plan — S9 pool เป็น independent จาก normal MC plan
            if progressive_plan and plan_week in progressive_plan and not _s9_active:
                available_machines = progressive_plan[plan_week]
            elif required_machines_info and not past_rdd and _req_feasible and not rts_local_force:
                # Cap available_machines ตาม required_mc เมื่อ feasible=True
                # (คำนวณมาแล้วว่า N เครื่องพอตั้งแต่ต้น ไม่ต้องเพิ่มบนกลางคัน)
                req_mc = required_machines_info[2]
                # 🔧 FIX: ถ้า SC เดียวกันมีเครื่องวิ่งอยู่แล้ว (last_sc_machines) อย่าลดต่ำกว่านั้น
                # ป้องกันกรณี FG ถัดไปของ SC เดียวกันใน week เดียวกัน ถูก cap ลงทั้งที่เครื่องยังวิ่งอยู่
                _sc_key_cap = (item, mc_group, _sel_gauge, sc_so_no)
                _sc_existing_mc = last_sc_machines.get(_sc_key_cap, 0)
                if _sc_existing_mc > 0 and req_mc < _sc_existing_mc:
                    print(f"[SC CAP PREVENT] {item} SC {sc_so_no} W{plan_week}: req_mc={req_mc} < existing={_sc_existing_mc} → keep {_sc_existing_mc}")
                    req_mc = _sc_existing_mc
    # ?? REMOVED:             if available_machines > req_mc:
    # ?? REMOVED:                 available_machines = req_mc

            # ถ้า plan_week เกิน target แล้ว → ยังสามารถเพิ่มเครื่องได้ (ไม่ cap ที่ required_mc)
            # 🔧 แก้ไข: บังคับกฎ "สัปดาห์แรกไม่เกิน 2 เครื่อง" เสมอ
            # ตรวจสอบว่าเป็นการใช้ครั้งแรกของ MC_GROUP นี้หรือไม่
            _is_first_week = True
            _current_week_idx = week_index(plan_week)
            _current_week_num = int(_current_week_idx) if isinstance(_current_week_idx, str) else _current_week_idx
            for (week_idx, mc_grp), total_machines in weekly_mc_usage.items():
                try:
                    week_num = int(week_idx) if isinstance(week_idx, str) else week_idx
                    if mc_grp == mc_group and week_num < _current_week_num:
                        _is_first_week = False
                        break
                except (ValueError, TypeError):
                    continue

            # ถ้าเป็นสัปดาห์แรก -> ใช้เครื่องจริง (carry) + ที่ setup ได้สูงสุด 2 เครื่อง
            # S9 จ้างทอ: ไม่มี gradual ramp-up — ใช้เครื่องได้เต็มที่ทันที
            if _is_first_week and not _s9_active:
                # 🔧 FIX: นับ carry เฉพาะจาก item ที่เคยผลิตใน new plan บน MC_GROUP นี้เท่านั้น
                # booking เก่าไม่นับเป็น carry (ต้อง setup ใหม่)
                _fwl_key = _resolve_carry_key(item, mc_group, _sel_gauge)
                if _has_item_mc_key(new_plan_started_items, _fwl_key):
                    carry_machines = machines_in_use.get(_fwl_key, 0)
                else:
                    carry_machines = 0
                carry_machines = max(0, int(carry_machines) if carry_machines else 0)
                # สูงสุด = carry + 2 setup (Gradual Increase)
                first_week_limit = carry_machines + 2
                if available_machines > first_week_limit:
                    print(f"🔒 ENFORCING First week limit for {mc_group} in week {plan_week}: carry={carry_machines} + setup<=2 = {first_week_limit}, reducing from {available_machines} to {first_week_limit} machines")
                    available_machines = first_week_limit
            # Calculate available capacity considering setup days and factory type
            factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week, gauge=_sel_gauge)
            # วันทำงานจากชีท Work Day เป็นค่าสุดท้าย (ไม่ cap/หักวันหยุด)
            actual_working_days = factory_working_days
            # หา REVOLUTION/WEIGHT ที่มากที่สุด
            rev_weight = get_revolution_weight_from_orders(item, mc_group)
            # กำหนด setup days จาก MasterMC "Set up time" ก่อน ถ้า blank ใช้ MATERIAL_CONTENT/YARN_ITEM
            item_material_content = str(order.get("MATERIAL_CONTENT", "")).strip() or _material_content_lookup.get(str(item).strip().upper(), "")
            _item_yarn_used = str(order.get("YARN-USED", "") or order.get("YARN_USED", "") or order.get("YARN_ITEM", "") or _yarn_used_lookup.get(str(item).strip().upper(), "")).strip()
            item_setup_days = get_setup_days_for_item(item_material_content, _item_yarn_used, mc_group=mc_group, item_code=item)
            # หมายเหตุ: เดิมมีบล็อก urgent mode ตรงนี้เพื่อ "เพิ่มวันทำงาน" ให้ order ที่เลย RDD
            # แต่ถูกยกเลิกถาวรเพราะขัดความเป็นจริง — โรงงานเปิดกี่วันก็ผลิตได้เท่านั้น
            # เร่งด้วยการเสกวันทำงานเพิ่มไม่ได้ (ดู urgent_mode ที่เหลือเป็น no-op ทั้งหมด)
            # การเร่งงานค้างทำผ่าน PAST RDD window (plan_week+3) ใน calculate_required_machines แทน

            factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week, gauge=_sel_gauge)
            # จ้างทอ: ผู้รับจ้างทำงาน 6 วันทุกกลุ่มเครื่อง หยุดตามปฏิทินเหมือนโรงเรา — ห้ามอ่านแผงของเรา
            # (ชื่อพูลผู้รับจ้าง RAP/SKP ซ้ำกับเครื่องในบ้าน → get_working_days_by_factory ได้วันของโรงเรา)
            if _s9_active:
                factory_working_days = actual_working_days = _s9_working_days(plan_week)
            # แยกเครื่อง carry-over (ไม่ต้อง setup) vs เครื่องใหม่ (ต้อง setup)
            mc_key = _resolve_carry_key(item, mc_group, _sel_gauge)
            _sc_key_init = (item, mc_group, _sel_gauge, sc_so_no)
            prev_machines = last_sc_machines.get(_sc_key_init, machines_in_use.get(mc_key, 0))
            # If RTS+LOCAL rule applies and the selected mc_group matches, force carryover-only
            if rts_local_force:
                last_old_by_mc = rts_local_force.get("last_old_by_mc", {})
                machines_by_mc = rts_local_force.get("machines_by_mc", {})
                if str(mc_group).strip().upper() in last_old_by_mc:
                    last_w = last_old_by_mc.get(str(mc_group).strip().upper())
                    start_after = next_week(last_w)
                    # เทียบด้วย "ตำแหน่งบน timeline" ไม่ใช่เลขสัปดาห์เปล่า —
                    # เมื่อแผนคร่อมปี W1 ของปีหน้าจะ "น้อยกว่า" W53 ของปีนี้เสมอ
                    # ทำให้ plan_week ถูกดึงกลับ W53 ทุกรอบ → วนไม่รู้จบจนชน SAFETY YEAR-END
                    # แล้ว qty ที่เหลือตกไป UNPLANNED (เคสจริง: FD6DV24/3A0 SC 721744)
                    _rts_pw_idx = week_index(plan_week) if plan_week is not None else None
                    _rts_sa_idx = week_index(start_after) if start_after is not None else None
                    if plan_week is None or (
                        _rts_pw_idx is not None
                        and _rts_sa_idx is not None
                        and _rts_pw_idx < _rts_sa_idx
                    ):
                        plan_week = start_after
                        # Recalculate working days for updated plan_week
                        factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week, gauge=_sel_gauge)
                        actual_working_days = factory_working_days
                    # ใช้ machines_by_mc (จาก booking_final_ready25 ทุก week) เป็น primary
                    # เพราะ machines_in_use มีแค่ week <= TODAY_WEEK ซึ่งอาจเป็น SO เก่า/เครื่องมากกว่าจริง
                    # ⚠️ แต่เครื่องของ booking เป็น "เครื่องเดิมที่ยังอุ่น" ได้เฉพาะตอนวางต่อจาก booking จริง ๆ:
                    #   1) item+เครื่องนี้ยังไม่เคยถูกวางในแผนใหม่ — ถ้าวางแล้ว เครื่องที่แผนตั้งจริงคือความจริง
                    #   2) สัปดาห์ที่วางห่างจากสัปดาห์สุดท้ายที่ booking รันไม่เกิน SETUP_GAP_WEEK
                    # ไม่งั้นเลข booking เก่าจะทับ machines_in_use ทุกสัปดาห์ตลอดกรอบ → carry เครื่องที่ไม่มีอยู่
                    # (เคสจริงรอบ replan FD4BASRS88/01A0 RAP98: booking จบ W38 ด้วย 6 เครื่อง แผน W46
                    #  ตั้งได้จริง 2 เครื่อง แต่ W47-W50 กลับ carry 6 + setup ใหม่ 2 ซ้ำทุกสัปดาห์
                    #  = เครื่องผี 4 ตัว + เสียวัน setup และโควตา job ซ้ำ 5 รอบ)
                    _bk_mc = machines_by_mc.get(str(mc_group).strip().upper())
                    _rts_cur_idx = week_index(plan_week) if plan_week is not None else None
                    _rts_last_idx = week_index(last_w) if last_w is not None else None
                    _rts_gap_ok = (
                        _rts_cur_idx is not None
                        and _rts_last_idx is not None
                        and (_rts_cur_idx - _rts_last_idx) <= SETUP_GAP_WEEK
                    )
                    _rts_started = _has_item_mc_key(new_plan_started_items, mc_key)
                    if _bk_mc is not None and _bk_mc > 0 and _rts_gap_ok and not _rts_started:
                        forced_m = _bk_mc
                    else:
                        if _bk_mc:
                            print(
                                f"[RTS PREV FROM PLAN] {item} W{plan_week}: ไม่ใช้เครื่อง booking {_bk_mc} "
                                f"(booking จบ W{last_w}"
                                + (", วางในแผนใหม่แล้ว" if _rts_started else "")
                                + (f", ห่าง {_rts_cur_idx - _rts_last_idx} สัปดาห์ > {SETUP_GAP_WEEK}"
                                   if (_rts_cur_idx is not None and _rts_last_idx is not None
                                       and not _rts_gap_ok) else "")
                                + f") → ใช้เครื่องที่แผนตั้งจริง {int(prev_machines or 0)}"
                            )
                        forced_m = prev_machines
                    prev_machines = int(forced_m or 0)
            current_week_idx = week_index(plan_week)
            # อ่าน last_production ก่อน _bridge_locks เพราะ _bridge_locks จะเลื่อนค่าไปที่สัปดาห์ lock
            _same_lock = _same_week_lock_mc(mc_key, last_production.get(mc_key))
            _warm_cap = _bridge_locks(mc_key, current_week_idx)
            # แถวกันเครื่อง (Hold รอสี / Hold รอ target) ถือเครื่องชุดเดิมไว้จริงตลอดสัปดาห์ที่เว้น
            # → เดินโซ่ต่อจาก Lock ให้ถึงสัปดาห์ปัจจุบัน (ดู _bridge_holds) ; เจอคอขวดกี่เครื่อง
            # ตลอดเส้นทางก็อุ่นได้แค่นั้น → cap แคบสุดของทั้งสองแหล่ง
            _hold_cap = _bridge_holds(mc_key, current_week_idx)
            if _hold_cap is not None:
                _warm_cap = _hold_cap if _warm_cap is None else min(_warm_cap, _hold_cap)
            # ล็อกที่ทับสัปดาห์ผลิต = เครื่องอีกชุดที่ยังอุ่น → บวกเพิ่มก่อนค่อยไปเจอเพดาน
            if _same_lock > 0:
                print(
                    f"[LOCK WARM] {item} W{plan_week}: carry-over {prev_machines}"
                    f" + {_same_lock} เครื่องที่กันไว้สัปดาห์ผลิตก่อนหน้า = {prev_machines + _same_lock}"
                )
                prev_machines += _same_lock
            # กันไว้กี่เครื่อง = อุ่นได้แค่นั้น — ที่เกินต้องนับเป็นเครื่องใหม่ (setup)
            # กติกาเดียวกับฝั่ง OLD ใน AVA_MC.py (_solve_carry_group)
            if _warm_cap is not None and prev_machines > _warm_cap:
                _cap_src = "แถวกันเครื่อง" if _hold_cap is not None and _warm_cap == _hold_cap else "Lock_MC"
                print(
                    f"[LOCK WARM] {item} W{plan_week}: carry-over {prev_machines} → {_warm_cap} เครื่อง"
                    f" ({_cap_src} กันไว้ {_warm_cap})"
                )
                prev_machines = _warm_cap
            prev_week_idx = last_production.get(mc_key)
            _booking_week_mc = (
                booking_mc_by_week.get(mc_key, {}).get(current_week_idx, 0)
                if current_week_idx is not None
                else 0
            )
            # ดึง TOTAL_MC_REMAIN จาก booking เพื่อเช็ค remaining capacity
            _booking_total_mc_remain = 0
            if mc_key in booking_mc_by_week and current_week_idx in booking_mc_by_week[mc_key]:
                # เกจต้องแปลงรูปก่อนเทียบ — _sel_gauge มาเป็น "22.0" แต่ชีท booking เป็น "22"
                # (เดิมเทียบสตริงตรง ๆ ไม่เคยเจอ → TOTAL_MC_REMAIN = 0 เสมอ ขึ้น NO REMAINING CAP ทุกเคส)
                _sel_gauge_n = _normalize_gauge(_sel_gauge)
                for _, row in detail_mc.iterrows():
                    if (str(row.get("ITEM_CODE", "")).strip().upper() == item and
                        str(row.get("MC_GROUP", "")).strip().upper() == mc_group and
                        _normalize_gauge(row.get("GUAGE", "")) == _sel_gauge_n and
                        week_index(int(row.get("WEEK"))) == current_week_idx):
                        _booking_total_mc_remain = int(row.get("TOTAL_MC_REMAIN", 0))
                        break
            if _booking_week_mc > 0:
                # ตรวจสอบ remaining capacity ก่อนบังคับใช้ booking machines
                if _booking_total_mc_remain > 0:
                    # มี remaining capacity → ใช้ booking machines
                    prev_machines = max(prev_machines, _booking_week_mc)
                    if available_machines < _booking_week_mc:
                        available_machines = _booking_week_mc
                        print(f"[CARRY BOOKING MC] {item} W{plan_week}: use booking machines={_booking_week_mc} (remaining cap={_booking_total_mc_remain})")
                else:
                    # ไม่มี remaining capacity → ไม่บังคับใช้ booking machines (ให้ไป week ถัดไป)
                    print(f"[NO REMAINING CAP] {item} W{plan_week}: booking {_booking_week_mc} mc but no remaining cap (TOTAL_MC_REMAIN={_booking_total_mc_remain}) → skip forcing, will try next week")
            # is_continuing = week ติดกัน AND เป็น SC/SO NO เดียวกัน (ต่างสี = เริ่มใหม่)
            # For RTS_LOCAL rule we must continue from old regardless of SC/SO, so
            # treat same_order as True when rts_local_force applies for this mc_group.
            # 🔧 FIX: ใช้ last_sc_week (per-SC) แทน last_sc_so_no (global) เพื่อป้องกัน SC อื่นทับ
            _sc_key_same = (item, mc_group, _sel_gauge, sc_so_no)
            _same_sc_last_week_idx = last_sc_week.get(_sc_key_same)
            # ถ้าเป็นกรณี RTS+LOCAL และ mc_group นี้มี old booking ให้บังคับต่อจาก old (ไม่สน SC/SO)
            if rts_local_force and str(mc_group).strip().upper() in rts_local_force.get(
                "last_old_by_mc", {}
            ):
                same_order = True
            else:
                # เปรียบเทียบ SC/SO
                same_order = last_sc_so_no.get(mc_key) == sc_so_no
                # ถ้า SC นี้เคยผลิตเอง (last_sc_week มีข้อมูล) และ gap ไม่เกิน → same_order = True
                if not same_order and _same_sc_last_week_idx is not None and current_week_idx is not None:
                    if (current_week_idx - _same_sc_last_week_idx) <= SETUP_GAP_WEEK:
                        print(f"[SAME SC CARRY] {item} SC {sc_so_no}: last SC week idx {_same_sc_last_week_idx} → current {current_week_idx} (gap {current_week_idx - _same_sc_last_week_idx} ≤ {SETUP_GAP_WEEK}) → same_order=True")
                        same_order = True
                # Item carry: FG ถัดไปของ item เดียวกัน (คนละ SC/FG) → carry เครื่องต่อ ไม่ต้อง setup ใหม่
                # ทำให้ SC ที่มี TARGET_KNIT ช้ากว่า รอ carry ต่อจาก SC ที่เร็วกว่า
                if not same_order:
                    _item_prev_m = machines_in_use.get(mc_key, 0)
                    _item_prev_widx = last_production.get(mc_key)
                    if (
                        _item_prev_m > 0
                        and _item_prev_widx is not None
                        and current_week_idx is not None
                        and (current_week_idx - _item_prev_widx) <= SETUP_GAP_WEEK
                    ):
                        same_order = True  # item-level carry: inherit machines จาก FG ก่อนหน้า
            # Item carry: ถ้า item+mc กำลังวิ่งอยู่ใน future week (prev_week_idx > current_week_idx)
            # หมายความว่า SC อื่นของ item เดียวกันยังผลิตอยู่ → ดัน plan_week ไปต่อท้าย แล้ว carry
            if (
                not _s9_active  # S9 ใช้เครื่อง commission knitting คนละ pool — ไม่ต้องรอ normal machine carryover
                and prev_week_idx is not None
                and current_week_idx is not None
                and prev_week_idx > current_week_idx
                and _has_item_mc_key(new_plan_started_items, mc_key)
            ):
                # เครื่องยังวิ่งอยู่ใน future → ข้ามไป week ถัดจาก week สุดท้ายที่ผลิต
                _after_prev = None
                _prev_cal_week = int(calendar_week.iloc[prev_week_idx]["WEEK"]) if prev_week_idx < len(calendar_week) else None
                if _prev_cal_week is not None:
                    _after_prev = _prev_cal_week  # เริ่มที่ week เดียวกับที่ SC ก่อนหน้าจบ → carry เข้าได้เลย ไม่ลดเครื่อง
                if _after_prev is not None:
                    print(f"[ITEM CARRY WAIT] {item} SC {sc_so_no}: machine busy until W{_prev_cal_week} → advance plan_week W{plan_week}→W{_after_prev} (carry in same week)")
                    plan_week = _after_prev
                    current_week_idx = week_index(plan_week)
                    # 🔧 FIX: After advancing plan_week, this is now a carryover situation.
                    # available_machines was set by LB for the ORIGINAL plan_week (possibly first-time use)
                    # but after ITEM CARRY WAIT it should respect existing carry machines.
                    # ป้องกัน LB สั่งเพิ่มเครื่องทั้งที่ carry เดิมเพียงพอผลิตได้ใน 1-2 weeks ถัดไป
                    _sc_key_wait = (item, mc_group, _sel_gauge, sc_so_no)
                    _carry_mc_after_wait = last_sc_machines.get(_sc_key_wait, machines_in_use.get(mc_key, 0))
                    if _carry_mc_after_wait > 0 and available_machines > _carry_mc_after_wait:
                        print(f"[ITEM CARRY WAIT] Capping available_machines {available_machines} → {_carry_mc_after_wait} (existing carry machines)")
                        available_machines = _carry_mc_after_wait
                    # วันทำงานต้องเป็นของสัปดาห์ที่ย้ายไป — เดิมยังใช้วันของสัปดาห์ตั้งต้น
                    # (เคสจริง 30 ก.ย. 2569 FD3BASRI58/01B0 เลื่อน W49→W53: คิดกำลังจาก 7.5 วัน
                    #  ทั้งที่ W53 มีแค่ 2.5 วัน → ผลิต 2,458 กก. บนกำลังจริงราว 910 กก.)
                    factory_working_days = get_working_days_by_factory(
                        mc_group, available_machines, week=plan_week, gauge=_sel_gauge
                    )
                    actual_working_days = factory_working_days
                same_order = True  # carry จาก week สุดท้ายของ SC ก่อนหน้า

            # ถ้าเงื่อนไข carryover อนุญาต (same_order=True) ให้ same-week ต่อได้
            _week_ok = (
                current_week_idx is not None
                and prev_week_idx is not None
                and (current_week_idx >= prev_week_idx)
            )
            is_continuing = (
                prev_week_idx is not None
                and current_week_idx is not None
                and same_order
                and _week_ok
                and (
                    current_week_idx - prev_week_idx <= 1  # week ติดกัน หรือ same week
                    or (current_week_idx - prev_week_idx <= SETUP_GAP_WEEK)
                )
            )  # ต้องไม่ห่างเกิน gap
            # 🔧 NEW: ถ้า booking active ครอบ plan_week (แต่ last_production เป็นสัปดาห์อนาคต)
            # → ถือว่า is_continuing เพราะเครื่องกำลังวิ่งอยู่ใน plan_week จาก booking เดิม
            if (
                not is_continuing
                and current_week_idx is not None
                and mc_key in booking_active_week_set
                and current_week_idx in booking_active_week_set[mc_key]
            ):
                is_continuing = True
                # ยก prev_machines ให้ตรงกับ booking machine count จริง เพื่อให้ carryover_mc ถูกต้อง
                if _booking_week_mc > prev_machines:
                    print(f"[IS_CONTINUING BOOKING SPAN] {item} W{plan_week}: booking active → is_continuing=True, raise prev_machines {prev_machines}→{_booking_week_mc}")
                    prev_machines = _booking_week_mc
                else:
                    print(f"[IS_CONTINUING BOOKING SPAN] {item} W{plan_week}: booking active ครอบ plan_week → is_continuing=True")
            # เครื่อง booking/KP ของสัปดาห์ก่อน (เว้นไม่เกิน SETUP_GAP_WEEK) ยังอุ่น → ทอต่อไม่ต้อง setup
            # เช่น KP W41 + KP W43 แล้ววาง W42: last_production = W43 (อยู่หลังสัปดาห์นี้) เดิมเลยตีเป็นเครื่องใหม่
            # ต้องใช้โควตา setup → โควตาเต็มก็ข้ามสัปดาห์เงียบ ๆ (เคส 29 ก.ย. 2569 FD6DRTJC30/1A0)
            if (
                not is_continuing
                and not _s9_active
                and current_week_idx is not None
                and not _has_item_mc_key(new_plan_started_items, mc_key)
            ):
                _bk_warm_mc, _bk_warm_idx = _booking_warm_mc(mc_key, current_week_idx)
                if _bk_warm_mc > 0:
                    is_continuing = True
                    prev_week_idx = _bk_warm_idx
                    print(f"[BOOKING WARM] {item} W{plan_week}: เครื่อง booking {_bk_warm_mc} ตัว (idx {_bk_warm_idx}) ยังอุ่น → is_continuing=True, prev_machines {prev_machines}→{_bk_warm_mc}")
                    prev_machines = _bk_warm_mc
            # ❗ ถ้า item+mc นี้ยังไม่เคยผลิตใน new plan → บังคับ setup (ไม่อ้าง old plan)
            # ยกเว้น: ถ้ามีข้อมูลจาก booking จริง (detail_mc) → อนุญาต carryover จาก booking ได้
            if not _has_item_mc_key(new_plan_started_items, mc_key) and not _has_item_mc_key(booking_production_keys, mc_key):
                is_continuing = False

            # 🔧 ตรวจ intermediate weeks — เครื่องอุ่นข้าม gap ได้เท่าที่สัปดาห์กลาง "ยังว่างจริง"
            # Rule: เครื่องว่างต่ำสุดของสัปดาห์ที่เว้น = เพดาน carry
            #   ว่าง 0 → carry ไม่ได้เลย (เครื่องถูก item อื่นยึดครบ) ต้อง setup ใหม่
            #   ว่าง 1 แต่จะ carry 3 → carry ได้ 1 อีก 2 เป็นเครื่องใหม่ (cap ที่จุดคำนวณ carryover_mc)
            # Exception: ถ้า item เดิมผลิตอยู่เองในสัปดาห์นั้น → เครื่องไม่ได้ถูกปล่อย ไม่นับ
            _gap_carry_cap = None
            if is_continuing and prev_week_idx is not None and current_week_idx is not None:
                _gap_carry_cap = _carry_cap_by_gap(
                    item, mc_key, mc_group, _sel_gauge, prev_week_idx, current_week_idx
                )
                if _gap_carry_cap is not None and _gap_carry_cap <= 0:
                    is_continuing = False

            # S9: ใช้เครื่อง commission knitting แยก pool — ไม่ carry จาก normal
            if _s9_active:
                is_continuing = False
            _cyl_pending_cnt = 0  # จำนวนเครื่องที่ถูก cylinder change และพร้อมใช้ week นี้ (committed)
            if is_continuing:
                # เครื่อง carry = เครื่องที่วิ่งอยู่แล้วจากสัปดาห์ก่อน จึงไม่ต้อง setup ใหม่
                # แต่เครื่องนั้นต้อง "ยังอยู่ในพูล" ของสัปดาห์นี้ด้วย — ถ้า booking ของสัปดาห์นี้
                # โตขึ้นจนจองเกินเครื่องจริง (remain ติดลบ) เครื่องที่แผนถืออยู่ถูกยึดไปแล้ว
                # → carry ต่อไม่ได้ (เดิมยก available_machines = prev_machines ทับ [CARRY CLAMP])
                # เครื่องของ booking ที่ทอไอเทมนี้อยู่ ถูกหักจาก remain ไปแล้ว → บวกกลับก่อนเทียบ
                _carry_pool_remain = get_actual_mc_remain(
                    mc_group, plan_week, gauge=_sel_gauge, item_code=item, allow_negative=True
                ) + int(_booking_week_mc or 0)
                if _carry_pool_remain < prev_machines:
                    _carry_cap = max(0, min(prev_machines, _carry_pool_remain))
                    print(
                        f"[CARRY POOL FULL] {item} SC {sc_so_no} W{plan_week}: "
                        f"pool remain={_carry_pool_remain} < prev={prev_machines} "
                        f"→ carry ได้ {_carry_cap} เครื่อง (booking จองเกินพูล)"
                    )
                    available_machines = min(available_machines, _carry_cap)
                    # เครื่องที่ถืออยู่ถูกพูลยึดคืน = ในบ้านเอาไม่อยู่จริง (ไม่ใช่การคาดเดา)
                    if not _s9_active and not _inhouse_stuck:
                        _inhouse_stuck = True
                        _inhouse_stuck_why = f"W{plan_week} เครื่องถูกพูลยึดคืน (carry {prev_machines}→{_carry_cap})"
                        print(f"[INHOUSE STUCK] {item} SC {sc_so_no}: {_inhouse_stuck_why} "
                              f"→ ยอดที่เหลือข้ามด่านเทียบ ไปจ้างทอได้ถ้าเข้าเกณฑ์")
                # 🔧 FIX: ถ้า SC เดียวกันกำลัง carry ต่อ อย่าลดเครื่องต่ำกว่าที่วิ่งอยู่แล้ว
                # ป้องกันกรณี FG ถัดไปของ SC เดียวกัน ถูก cap ลงทั้งที่เครื่องยังวิ่งอยู่
                elif prev_machines > available_machines:
                    print(f"[SC CONTINUING RAISE] {item} SC {sc_so_no} W{plan_week}: prev={prev_machines} > avail={available_machines} → raise to {prev_machines}")
                    available_machines = prev_machines
                # Cyl-changed machines ready THIS week (triggered in carry path of previous week)
                # เครื่องเหล่านี้เป็น NEW (ต้อง setup) ไม่ใช่ carry — เพิ่ม available แบบ additive
                if _sel_gauge:
                    _cyl_t1_k = _mc_to_type1(mc_group, _sel_gauge)
                    _cyl_g_k = _normalize_gauge(_sel_gauge)
                    _item_cyl_k = str(item).strip().upper()
                    _cyl_pending_cnt = _carry_cyl_pending.get((int(plan_week), _item_cyl_k, _cyl_t1_k, _cyl_g_k), 0)
                    if _cyl_pending_cnt > 0:
                        available_machines = max(available_machines, prev_machines) + _cyl_pending_cnt
                        print(f"[CARRY CYL READY] {item} W{plan_week}: +{_cyl_pending_cnt} cyl machine(s) ready → available={available_machines}")
                # ถ้า required_mc > available_machines (carry) → trigger cylinder change เท่าที่ทำได้ใน week นี้
                # machine พร้อมใน NEXT week — จำนวนที่ trigger ขึ้นกับ quota (CYLINDER_CHANGE_LIMIT)
                _req_mc_carry = required_machines_info[2] if required_machines_info and len(required_machines_info) > 2 else 0
                _cyl_this_wk_cap = (available_machines * daily_capacity * get_working_days_by_factory(mc_group, 1, week=plan_week, gauge=_sel_gauge)) if daily_capacity else 0
                # เช็คว่า native gauge pool ยังมีเครื่องเหลืออยู่ไหม — ถ้ายังมี ไม่ต้อง cylinder change
                # (available_machines อาจต่ำกว่า required เพราะ MAX_NEW_SETUP_MC cap ไม่ใช่เพราะ pool หมด)
                _cyl_native_pool = get_actual_mc_remain(mc_group, plan_week, gauge=_sel_gauge, item_code=item) if _sel_gauge else 0
                if _req_mc_carry > available_machines and _sel_gauge and not past_rdd and qty_left > _cyl_this_wk_cap and _cyl_native_pool <= 0:
                    _cyl_cat_carry = _mc_to_type1(mc_group, _sel_gauge)
                    _cyl_fact_carry = _mc_to_factory(mc_group, _sel_gauge)
                    _cyl_tgt_g_carry = _normalize_gauge(_sel_gauge)
                    _cyl_base_trigger = int(plan_week)  # trigger week นี้ → machine พร้อม week ถัดไป
                    print(f"[CARRY CYL ATTEMPT] {item} W{plan_week}: carry={available_machines} < required={_req_mc_carry}, trigger W{_cyl_base_trigger} ({_cyl_fact_carry}/{_cyl_cat_carry}/G{_cyl_tgt_g_carry}) [jit_override=True]")
                    _cyl_pending_added = 0
                    while _req_mc_carry > available_machines + _cyl_pending_added:
                        if _try_cylinder_change(_cyl_cat_carry, _cyl_fact_carry, _cyl_tgt_g_carry, _cyl_base_trigger, item, mc_group, debug=True, jit_override=True):
                            _cyl_pending_added += 1
                            _next_cyl_w = next_week(plan_week)
                            if _next_cyl_w is not None:
                                _pk = (int(_next_cyl_w), str(item).strip().upper(), _cyl_cat_carry, _cyl_tgt_g_carry)
                                _carry_cyl_pending[_pk] = _carry_cyl_pending.get(_pk, 0) + 1
                                _cyl_f_up_c = str(_cyl_fact_carry).strip().upper()
                                for _fw_u_c in sorted(int(w) for w in summary_mc["WEEK"].unique() if int(w) >= int(_next_cyl_w)):
                                    _undo_k_c = (_fw_u_c, _cyl_f_up_c, _cyl_cat_carry, _cyl_tgt_g_carry)
                                    if cylinder_adjustments.get(_undo_k_c, 0) > 0:
                                        cylinder_adjustments[_undo_k_c] -= 1
                            print(f"[CARRY CYL CHANGE] {item} W{plan_week}: trigger W{_cyl_base_trigger} → +{_cyl_pending_added} machine(s) ready W{_next_cyl_w}")
                        else:
                            break  # quota เต็มสำหรับ week นี้
                carryover_mc = min(prev_machines, available_machines)

                # เพดาน carry ข้ามสัปดาห์ที่เว้น — เครื่องที่จอดว่างจริงกลาง gap เท่านั้นที่ยังอุ่น
                # ส่วนที่เกินเพดานตกเป็นเครื่องใหม่ (new_mc = available − carryover) จึงเสียวัน setup
                # และกินโควตา job setup ตามจริง
                if _gap_carry_cap is not None and carryover_mc > _gap_carry_cap:
                    print(
                        f"[CARRY CAP GAP] {item} W{plan_week}: carry {carryover_mc} → {_gap_carry_cap} "
                        f"(สัปดาห์ที่เว้นเหลือเครื่องว่างแค่ {_gap_carry_cap} ที่เหลือต้อง setup ใหม่)"
                    )
                    carryover_mc = max(0, int(_gap_carry_cap))

                # 🔧 FIX: ถ้า qty_left น้อย → ลด carryover_mc ให้พอเหมาะกับ qty_left
                # ป้องกันกรณี carryover เครื่องเยอะแต่ qty_left น้อย (เช่น FD1BASMZ26B0 W23)
                if daily_capacity and qty_left > 0 and carryover_mc > 1:
                    # คำนวณ weekly capacity สำหรับ 1 เครื่อง
                    factory_wd = get_working_days_by_factory(mc_group, 1, week=plan_week, gauge=_sel_gauge)
                    weekly_cap_per_mc = daily_capacity * factory_wd
                    # สัปดาห์ที่โรงหยุดทั้งสัปดาห์ → วันทำงาน = 0 → weekly_cap_per_mc = 0
                    # หารต่อจะได้ inf แล้ว int(inf) โยน OverflowError ทำ Planning ล้มทั้งรอบ
                    # (เจอจริงในรอบ replan: booking ที่ RDD ย้อนหลังถูกลองวางที่ W31 ซึ่งเป็นสัปดาห์หยุด
                    #  → Replan ล้มทุกวันตั้งแต่ 15/08/2026 ชีท REPLAN ค้างเป็นแผนเก่า)
                    # คำนวณไม่ได้ = ไม่ลด carry ปล่อยตามเดิม (สัปดาห์นั้นผลิตอะไรไม่ได้อยู่แล้ว)
                    if weekly_cap_per_mc > 0:
                        # 🔧 FIX: ใช้ _qty_for_machine_calc (รวมทุก FG ของ item) ไม่ใช่แค่ qty_left (FG ปัจจุบัน)
                        # ป้องกันลดเครื่องทั้งที่ item ยังมี FG ถัดไปรอผลิตอยู่
                        _reduce_qty = max(qty_left, _qty_for_machine_calc)
                        # คำนวณจำนวนเครื่องที่ต้องการจริงๆ ตาม working days จริง
                        needed_mc = max(1, int(_reduce_qty / weekly_cap_per_mc) + 1)
                        # ถ้า needed_mc < carryover_mc → ลด carryover_mc
                        if needed_mc * 2 < carryover_mc:
                            print(f"[CARRYOVER REDUCE] {item} W{plan_week}: carryover_mc {carryover_mc} → {needed_mc} (qty_left={qty_left:.0f}, factory_wd={factory_wd}, weekly_cap_per_mc={weekly_cap_per_mc:.0f})")
                            carryover_mc = needed_mc
                            # อัปเดต available_machines ตาม carryover_mc ที่ลดลง
                            # 🔧 FIX: รักษา _cyl_pending_cnt — เครื่องที่ถูก cylinder change ไปแล้วต้องใช้เสมอ
                            available_machines = min(available_machines, carryover_mc + _cyl_pending_cnt)
                    else:
                        print(f"[CARRYOVER REDUCE] {item} W{plan_week}: ข้าม — วันทำงาน 0 (โรงหยุดทั้งสัปดาห์) คำนวณเครื่องที่ต้องใช้ไม่ได้")

                # เครื่องใหม่ = available_machines - carryover
                # 🔧 FIX: บังคับให้ available_machines >= carryover_mc เพื่อป้องกัน new_mc ติดลบ
                available_machines = max(available_machines, carryover_mc)
                new_mc = max(0, available_machines - carryover_mc)
            else:
                carryover_mc = 0
                new_mc = available_machines  # ทุกเครื่องต้อง setup
                # Trigger cylinder change ใน setup week — trigger เท่าที่ทำได้เพื่อทัน target
                # machine พร้อมใน NEXT week — จำนวนขึ้นกับ quota (CYLINDER_CHANGE_LIMIT)
                _req_mc_setup = required_machines_info[2] if required_machines_info and len(required_machines_info) > 2 else 0
                _cyl_this_wk_cap_s = (available_machines * daily_capacity * get_working_days_by_factory(mc_group, 1, week=plan_week, gauge=_sel_gauge)) if daily_capacity else 0
                # เช็คว่า native gauge pool ยังมีเครื่องเหลืออยู่ไหม — ถ้ายังมี ไม่ต้อง cylinder change
                # (new_mc อาจต่ำกว่า required เพราะ MAX_NEW_SETUP_MC cap ไม่ใช่เพราะ pool หมด)
                _cyl_native_pool_s = get_actual_mc_remain(mc_group, plan_week, gauge=_sel_gauge, item_code=item) if _sel_gauge else 0
                if _req_mc_setup > new_mc and _sel_gauge and not past_rdd and qty_left > _cyl_this_wk_cap_s and _cyl_native_pool_s <= 0:
                    _cyl_cat_s = _mc_to_type1(mc_group, _sel_gauge)
                    _cyl_fact_s = _mc_to_factory(mc_group, _sel_gauge)
                    _cyl_tgt_g_s = _normalize_gauge(_sel_gauge)
                    _cyl_base_s = int(plan_week)
                    _cyl_pending_added_s = 0
                    while _req_mc_setup > new_mc + _cyl_pending_added_s:
                        if _try_cylinder_change(_cyl_cat_s, _cyl_fact_s, _cyl_tgt_g_s, _cyl_base_s, item, mc_group, debug=True, jit_override=True):
                            _cyl_pending_added_s += 1
                            _next_cyl_w_s = next_week(plan_week)
                            if _next_cyl_w_s is not None:
                                _pk_s = (int(_next_cyl_w_s), str(item).strip().upper(), _cyl_cat_s, _cyl_tgt_g_s)
                                _carry_cyl_pending[_pk_s] = _carry_cyl_pending.get(_pk_s, 0) + 1
                                _cyl_f_up_s = str(_cyl_fact_s).strip().upper()
                                for _fw_u_s in sorted(int(w) for w in summary_mc["WEEK"].unique() if int(w) >= int(_next_cyl_w_s)):
                                    _undo_k_s = (_fw_u_s, _cyl_f_up_s, _cyl_cat_s, _cyl_tgt_g_s)
                                    if cylinder_adjustments.get(_undo_k_s, 0) > 0:
                                        cylinder_adjustments[_undo_k_s] -= 1
                            print(f"[SETUP CYL CHANGE] {item} W{plan_week}: setup trigger W{_cyl_base_s} → +{_cyl_pending_added_s} machine(s) ready W{_next_cyl_w_s}")
                        else:
                            break  # quota เต็มสำหรับ week นี้
            # จ้างทอ: ทางบนตั้งทุกเครื่องเป็นเครื่องใหม่ (is_continuing=False เพราะคนละพูลกับในบ้าน)
            # → คืนส่วนที่ทอต่อจากสัปดาห์ก่อนให้เป็น carry (ไม่เสีย setup) ตามที่คิดไว้ตอนไล่เครื่อง
            if _s9_active:
                carryover_mc = min(_s9_carry_now, available_machines)
                new_mc = max(0, available_machines - carryover_mc)
            # Hard cap POLY/COTTON: เครื่องที่แผนใช้ (carryover + new) ห้ามเกินที่กันไว้
            # remain = กันไว้ − booking − แผนที่ใช้ไปแล้วในสัปดาห์นั้น
            # ถ้าไม่มีเครื่องเหลือ → carry ก็ทำไม่ได้ (ไม่มีเครื่องให้ carry) ตามที่ user ยืนยัน
            # ตัด new ก่อน แล้วค่อยตัด carry (รักษาเครื่องที่วิ่งอยู่ไว้ก่อน)
            _sp_remain = _mc_special_remain(mc_group, plan_week, _sel_gauge, item)
            if _sp_remain is not None and (carryover_mc + new_mc) > _sp_remain:
                _sp_old = (carryover_mc, new_mc)
                carryover_mc = min(carryover_mc, _sp_remain)
                new_mc = max(0, min(new_mc, _sp_remain - carryover_mc))
                available_machines = carryover_mc + new_mc
                print(f"[POLY/COTTON CAP] {item} W{plan_week}: carry/new {_sp_old} → ({carryover_mc}, {new_mc}) remain={_sp_remain} (เครื่องกันไว้เต็ม)")

            # Enforce RTS+LOCAL: use existing carryover machines only (no new setup)
            # prev_machines comes from machines_in_use (last active week, MC_USE_CEIL>0)
            # 🔧 FIX: เช็ค is_continuing ก่อน — ถ้า gap > SETUP_GAP_WEEK ต้อง setup ใหม่
            if rts_local_force and str(mc_group).strip().upper() in rts_local_force.get(
                "last_old_by_mc", {}
            ) and is_continuing:
                # 🔧 FIX: จำกัด carryover ไม่ให้เกิน available_machines
                carryover_mc = min(int(prev_machines or 0), available_machines)
                new_mc = 0
                available_machines = carryover_mc
            # ===== Carryover-first: ตรวจว่า carryover เพียงพอทัน rdd ไหม =====
            # Simulate production จาก plan_week ถึง rdd_idx ด้วย carry เครื่อง
            # Week 1: carry ผลิตเต็ม, new ผลิตหัก setup  |  Week 2+: ทุกเครื่องเป็น carry
            # YD-ORDERS: คำนวณ SUB_COLOR setup days ก่อน forward sim
            _yd_sub_color_setup = 0
            if order_type == "YD-ORDERS" and carryover_mc > 0:
                _prev_sub_color = last_sub_color.get(mc_key, "")
                if _prev_sub_color and sub_color and _prev_sub_color != sub_color:
                    _wcs_key = (mc_key, plan_week)
                    _yd_week_color_setups[_wcs_key] = _yd_week_color_setups.get(_wcs_key, 0) + 1
                    _yd_sub_color_setup = 1  # หัก 1 วันต่อครั้งที่เปลี่ยนสี (ไม่ใช้ cumulative เพราะ remaining cap หักไปแล้วจากครั้งก่อน)

            def _forward_sim(carry, new, q_left):
                q = q_left
                wk = plan_week
                first = True
                _rw = rev_weight or 0
                # เมื่อ plan_week เลย rdd แล้ว (past_rdd) → ไม่ตัด sim ที่ rdd_idx
                # เพราะต้องจำลองต่อเพื่อเช็คว่า carry-only เพียงพอผลิตให้เสร็จหรือไม่
                # ป้องกัน setup เครื่องใหม่ที่ใช้แค่ 1 week แล้วหยุด
                _is_past_rdd = (
                    rdd_idx is not None
                    and current_week_idx is not None
                    and current_week_idx > rdd_idx
                )
                _sim_count = 0
                while wk is not None and q > _rw:
                    if _sim_count > 300:  # SAFETY: กัน infinite loop ข้ามปลายปี
                        break
                    w_idx_sim = week_index(wk)
                    if w_idx_sim is None:
                        break
                    # ยังไม่เลย rdd → หยุดจำลองที่ rdd เหมือนเดิม
                    if not _is_past_rdd and rdd_idx is not None and w_idx_sim > rdd_idx:
                        break
                    # เลย rdd แล้ว → จำกัดจำลอง 2 สัปดาห์
                    # setup เครื่องใหม่ที่ใช้ < 2 สัปดาห์ไม่คุ้ม setup days
                    # ถ้า carry-only ผลิตครบภายใน 2 สัปดาห์ → ไม่ต้อง setup เพิ่ม
                    if _is_past_rdd and _sim_count >= 2:
                        break
                    # หักวันหยุดตามปฏิทินให้แล้วใน WorkDay.get_working_days() — ห้าม min ซ้ำ
                    wd = get_working_days_by_factory(mc_group, carry + new, week=wk, gauge=_sel_gauge)
                    if first:
                        # YD-ORDERS: SUB_COLOR เปลี่ยน → carryover ต้องเสีย 1 วัน setup
                        _carry_wd = max(0, wd - _yd_sub_color_setup) if carry > 0 else wd
                        c_prod = carry * _carry_wd * daily_capacity
                        n_prod = new * max(0, wd - item_setup_days) * daily_capacity
                        first = False
                        total = fold_round_mixed(
                            c_prod + n_prod, rev_weight, mc_group,
                            _carry_wd * daily_capacity, carry,
                            max(0, wd - item_setup_days) * daily_capacity, new,
                        )
                    else:
                        c_prod = (carry + new) * wd * daily_capacity
                        n_prod = 0
                        total = fold_round(
                            c_prod, rev_weight, mc_group,
                            per_mc_cap=wd * daily_capacity, mc_count=carry + new,
                        )
                    q -= total
                    wk = next_week(wk)
                    _sim_count += 1
                return q

            def _carry_span_blocked(carry, new, with_takeable=False):
                """หาสัปดาห์แรกที่งานนี้จะ carry ต่อไม่ได้เพราะพูลไม่มีเครื่องเหลือ

                จำลองการผลิตด้วยเครื่องชุดนี้ไปข้างหน้า สัปดาห์ไหนที่ยังผลิตไม่จบต้อง carry ต่อ
                → สัปดาห์นั้นต้องมีเครื่องในพูลพอ ถ้า booking จองเกินพูลแล้ว (remain ติดลบ)
                เครื่องที่แผนถืออยู่จะถูกยึด → setup ที่สัปดาห์นี้ไม่มีประโยชน์ (เสียวัน setup เปล่า)

                with_takeable=True → นับเครื่องที่ก้อนสียึดคืนได้เป็นเครื่องเหลือด้วย (ยังไม่ยึดจริง)

                คืน (สัปดาห์ที่ติด, เครื่องเหลือจริง, เครื่องที่ต้องใช้) หรือ None ถ้าวิ่งได้ครบ
                """
                total_mc = carry + new
                if total_mc <= 0 or not daily_capacity:
                    return None
                q = qty_left
                wk = plan_week
                first = True
                _rw = rev_weight or 0
                _guard = 0
                while wk is not None and q > _rw and _guard < 26:
                    _guard += 1
                    # หักวันหยุดตามปฏิทินให้แล้วใน WorkDay.get_working_days() — ห้าม min ซ้ำ
                    wd = get_working_days_by_factory(mc_group, total_mc, week=wk, gauge=_sel_gauge)
                    if first:
                        prod = (
                            carry * wd * daily_capacity
                            + new * max(0, wd - item_setup_days) * daily_capacity
                        )
                        first = False
                    else:
                        prod = total_mc * wd * daily_capacity
                    if _rw > 0 and prod > 0:
                        prod = (prod // _rw) * _rw
                    q -= prod
                    if q <= _rw:
                        return None  # ผลิตจบในสัปดาห์นี้ → ไม่ต้อง carry ต่อ
                    wk = next_week(wk)
                    if wk is None:
                        return None  # หมด horizon → ปล่อยให้ logic เดิมจัดการ
                    # สัปดาห์ถัดไปต้อง carry ต่อ: เครื่อง booking ของไอเทมนี้ถูกหักจาก remain
                    # ไปแล้ว จึงนับเป็นเครื่องที่ใช้ได้ (ไม่ต้องขอจากพูลซ้ำ)
                    _bk_next = int(
                        booking_mc_by_week.get(_ck(item, mc_group, _sel_gauge), {}).get(
                            week_index(wk), 0
                        )
                        or 0
                    )
                    _need = max(0, total_mc - _bk_next)
                    if _need <= 0:
                        continue
                    _rem_next = get_actual_mc_remain(
                        mc_group, wk, gauge=_sel_gauge, item_code=item, allow_negative=True
                    )
                    if with_takeable and MAIN_COLOR_GRAB_ENABLED and _CUR_ORDER_COLOR_PART:
                        _rem_next += _main_takeable_mc(item, mc_group, _sel_gauge, week_pos(wk))
                    if _rem_next < _need:
                        return (wk, _rem_next, _need)
                return None

            def _span_block_with_grab(carry, new):
                """เหมือน _carry_span_blocked แต่ก้อนสีได้ยึดเครื่องคืนในสัปดาห์ที่ติดก่อนยอมแพ้

                ยึดเฉพาะเมื่อ "ยึดแล้ววิ่งได้ครบทุกสัปดาห์" — ยึดไปครึ่งทางแล้วยังติดสัปดาห์ถัดไป
                = ปลดงานคนอื่นทิ้งเปล่า (การยึดย้อนคืนไม่ได้)
                """
                _blk = _carry_span_blocked(carry, new)
                if _blk is None or not (MAIN_COLOR_GRAB_ENABLED and _CUR_ORDER_COLOR_PART):
                    return _blk
                if _carry_span_blocked(carry, new, with_takeable=True) is not None:
                    return _blk
                _blk_guard = 0
                while _blk is not None and _blk_guard < 6:
                    _blk_guard += 1
                    _bw, _brem, _bneed = _blk
                    if _main_color_grab(
                        item, mc_group, _sel_gauge, week_pos(_bw),
                        int(_bneed) - max(0, int(_brem)),
                        tk_week=int(calendar_week.iloc[rdd_idx]["WEEK"])
                        if rdd_idx is not None and 0 <= rdd_idx < len(calendar_week) else None,
                    ) <= 0:
                        break                      # ไม่มีเครื่องให้ยึดแล้ว → ปล่อยตามด่านเดิม
                    _blk = _carry_span_blocked(carry, new)
                return _blk

            if rdd_idx is not None and not rts_local_force and new_mc > 0 and not _s9_active:
                _rw_tol = 0.0
                # demand ของ item ที่ยังรออยู่ นอกเหนือจาก order line นี้
                # (_qty_for_machine_calc ตั้งตอนเริ่ม order → หัก _initial ออกกันนับซ้ำ)
                _future_qty_ln = max(0.0, _qty_for_machine_calc - _initial_qty_for_mc_calc)
                # ตรวจก่อน: carry เพียงพอจบ FG ปัจจุบันหรือไม่ (qty_left FG นี้เท่านั้น)
                # ป้องกันการเพิ่มเครื่องเพราะ total demand สูง ทั้งที่ carryover จบ FG นี้ได้พอ
                # ⚠️ ใช้ทางลัดนี้ได้เฉพาะเมื่อไม่มี demand ของ FG/SC อื่นรออยู่ — ไม่งั้น order
                # ที่ถูกซอยเป็นไลน์เล็ก ๆ จะ "พอ" ทุกไลน์ แล้วไม่เพิ่มเครื่องเลยทั้งที่รวมกันไม่ทัน
                if (
                    carryover_mc > 0
                    and _future_qty_ln <= 1.0
                    and _forward_sim(carryover_mc, 0, qty_left) <= _rw_tol
                ):
                    new_mc = max(0, _cyl_pending_cnt)  # 🔧 FIX: รักษาเครื่องที่ committed cylinder change
                    if new_mc == 0:
                        print(f"   [NO NEW MC] Carryover {carryover_mc} mc เพียงพอจบ FG ปัจจุบัน (qty={qty_left:.0f}) → ไม่เพิ่มเครื่องใหม่")
                    else:
                        print(f"   [NO NEW MC KEEP CYL] Carryover {carryover_mc} mc เพียงพอ แต่รักษา {new_mc} cyl machine(s) → new_mc={new_mc}")
                else:
                    # ใช้ demand ระดับ item (ทุก FG ของ item) เพื่อคงเครื่องไว้เมื่อยังมี FG เหลือของ item
                    # ใช้ค่า adjusted (qty_left ปัจจุบัน + demand อนาคต) แทน _qty_for_machine_calc
                    # ที่ stale — ไม่งั้นจะนับ demand ที่วางแผนไปแล้วซ้ำ
                    _fwd_qty = qty_left + _future_qty_ln
                    if carryover_mc > 0 and _forward_sim(carryover_mc, 0, _fwd_qty) <= _rw_tol:
                        # carryover เพียงพอทัน → ไม่ต้อง setup เพิ่ม
                        new_mc = max(0, _cyl_pending_cnt)  # 🔧 FIX: รักษาเครื่องที่ committed cylinder change
                    else:
                        # หา new_mc น้อยสุดที่ทัน (จาก carryover + new)
                        found_n = new_mc  # fallback = ทั้งหมด
                        for try_n in range(1, new_mc + 1):
                            if _forward_sim(carryover_mc, try_n, _fwd_qty) <= _rw_tol:
                                found_n = try_n
                                break
                        new_mc = found_n
                available_machines = carryover_mc + new_mc
            # จำนวน new_mc ที่ _forward_sim หาได้ = น้อยสุดที่ทัน → เป็น lower bound
            _forward_min_new = new_mc

            # ===== Dynamic setup limit ตาม urgency RDD =====
            # S9: ข้าม limit ทั้งหมด — ใช้เต็ม pool ที่เหลือ
            if not _s9_active:
                _remaining_slots = get_remaining_job_slots(plan_week, mc_group)
                _req_mc_dyn = required_machines_info[2] if required_machines_info else new_mc
                # ถ้า simulate แล้วไม่ทัน → เปิดเต็ม slots (ไม่ cap ที่ required_mc)
                if not _req_feasible:
                    _dyn_limit = _remaining_slots
                else:
                    _dyn_limit = _dynamic_setup_limit(
                        plan_week, rdd_idx, _req_mc_dyn, _remaining_slots
                    )
                # ห้าม cap ต่ำกว่า _forward_min_new — ไม่งั้น forward sim ไม่มีความหมาย
                _dyn_limit = max(_dyn_limit, _forward_min_new)
                if new_mc > _dyn_limit:
                    new_mc = _dyn_limit
                # ===== Hard cap: เครื่องใหม่ (new setup) ≤ โควตาที่ผ้านี้เหลือในสัปดาห์นี้ =====
                new_mc = _cap_new_by_item_quota(
                    new_mc, item, mc_group, _sel_gauge, plan_week, tag="hard cap"
                )
                available_machines = carryover_mc + new_mc
            def _weeks_to_finish_with(total_mc, q_start):
                """ใช้เครื่องรวม total_mc เดินต่อจากสัปดาห์นี้ ต้องใช้กี่สัปดาห์จึงทอจบ

                ใช้ตอบคำถาม "เพิ่มเครื่องแล้วส่งเร็วขึ้นจริงไหม" — งานที่เลย target ไปแล้ว
                ถ้าเพิ่มเครื่องแล้วยังจบสัปดาห์เดิม = เพิ่มไปก็เปล่าประโยชน์ (เสีย setup + โควตา job)
                คืน 999 เมื่อคำนวณไม่ได้ (กันเอาไปเทียบแล้วตัดสินผิด)
                """
                if total_mc <= 0 or not daily_capacity:
                    return 999
                _q = float(q_start or 0)
                if _q <= 0:
                    return 0
                _wk_i = plan_week
                _cnt = 0
                _rw_w = rev_weight or 0
                while _wk_i is not None and _cnt < 60:
                    _wd_i = get_working_days_by_factory(
                        mc_group, total_mc, week=_wk_i, gauge=_sel_gauge
                    )
                    # สัปดาห์แรกของการเพิ่ม: เครื่องใหม่เสียวัน setup (ประเมินแบบระวัง)
                    _pd_i = max(0, _wd_i - item_setup_days) if _cnt == 0 else _wd_i
                    _prod_i = total_mc * _pd_i * daily_capacity
                    if _rw_w > 0 and _prod_i > 0:
                        _prod_i = (_prod_i // _rw_w) * _rw_w
                    _cnt += 1
                    _q -= _prod_i
                    if _q <= _rw_w:
                        return _cnt
                    _wk_i = next_week(_wk_i)
                return 999

            # ===== Setup payback gate: เครื่องใหม่ต้องได้วิ่งเต็ม >= 2 สัปดาห์ =====
            # เครื่อง setup ใหม่จ่าย setup days ในสัปดาห์แรก ถ้าสัปดาห์ถัดไปงานเหลือน้อย
            # จน carry เดิมทำคนเดียวได้ เครื่องนั้นจะถูกปลดทันที → เสีย setup ฟรี
            # และแผนจบช้ากว่าการไม่ setup เลย
            def _new_mc_full_weeks(carry, new, q_start):
                """นับสัปดาห์ (หลังสัปดาห์ setup) ที่งานยังเหลือมากพอให้เครื่องใหม่วิ่งเต็ม"""
                if new <= 0 or not daily_capacity:
                    return 0
                _rw_pb = rev_weight or 0

                def _wd_pb(wk_, mc_):
                    # หักวันหยุดตามปฏิทินให้แล้วใน WorkDay.get_working_days() — ห้าม min ซ้ำ
                    return get_working_days_by_factory(mc_group, mc_, week=wk_, gauge=_sel_gauge)

                def _round_pb(v):
                    if _rw_pb > 0 and v > 0:
                        return (v // _rw_pb) * _rw_pb
                    return v

                q_pb = float(q_start)
                wk_pb = plan_week
                _wd0 = _wd_pb(wk_pb, carry + new)
                # วันผลิตของเครื่องใหม่ในสัปดาห์ setup — สูตรเดียวกับ prod_days_new ด้านล่าง
                if _wd0 < item_setup_days:
                    _pdn0 = max(0, _wd0 - 0.5)
                else:
                    _pdn0 = max(0.5, _wd0 - item_setup_days)
                q_pb -= _round_pb(
                    carry * _wd0 * daily_capacity + new * _pdn0 * daily_capacity
                )
                _full_pb = 0
                _guard_pb = 0
                while q_pb > _rw_pb and _guard_pb < 26:
                    _guard_pb += 1
                    wk_pb = next_week(wk_pb)
                    if wk_pb is None:
                        break
                    _wd_n = _wd_pb(wk_pb, carry + new)
                    _carry_only_pb = _round_pb(carry * _wd_n * daily_capacity)
                    if q_pb <= _carry_only_pb:
                        break  # carry เดิมพอแล้ว → เครื่องใหม่ไม่ได้วิ่งเต็มสัปดาห์นี้
                    _full_pb += 1
                    q_pb -= _round_pb((carry + new) * _wd_n * daily_capacity)
                return _full_pb

            # (ด่าน payback ถูกบังคับใช้หลัง GRADUAL BOOST — จุดสุดท้ายที่ยังเพิ่มเครื่องได้)

            # ใช้ actual_working_days (หักวันหยุดแล้ว) แทน factory_working_days แบบ static
            prod_days_old = actual_working_days  # เครื่อง carry-over ผลิตตามวันเปิดจริง
            # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → carryover machines เสีย 1 วัน setup จากที่เหลือ
            if _yd_sub_color_setup > 0:
                prod_days_old = max(0, prod_days_old - 1)
                _total_color_setups = _yd_week_color_setups.get((mc_key, plan_week), 0)
                print(f"[YD SUB_COLOR] {item} W{plan_week}: SUB_COLOR เปลี่ยนครั้งที่ {_total_color_setups} → prod_days_old={prod_days_old} (หัก 1 วันจากที่เหลือ)")
            # เครื่องใหม่ (new_mc) ต้อง setup เสมอ แม้ item จะ warm บนเครื่องเดิม
            # (warm/carry หมายถึงเครื่องที่วิ่งอยู่แล้ว ไม่ใช่เครื่องที่เพิ่งเพิ่มมา)
            # 🔧 FIX: ถ้าวันทำงานน้อยกว่า setup days → ใช้ 0.5 วัน setup แทน (เพื่อให้มีวันผลิตบ้าง)
            # S9: จ้างทอไม่หัก setup — เครื่องใหม่ผลิตเต็มวันทำงานของผู้รับจ้าง
            if _s9_active:
                prod_days_new = actual_working_days
            elif new_mc > 0 and actual_working_days < item_setup_days:
                # วันทำงานน้อยเกินไป → ลด setup เป็น 0.5 วัน
                prod_days_new = max(0, actual_working_days - 0.5)
            elif new_mc > 0:
                # ปกติ แต่ถ้า actual_working_days == item_setup_days → ผลิต 0 วัน → บังคับ 0.5 วันขั้นต่ำ
                prod_days_new = max(0.5, actual_working_days - item_setup_days)
            else:
                prod_days_new = max(0, actual_working_days - item_setup_days)

            # ===== NEW: Multi-FG min-mc — ลดเครื่องตาม demand รวมของทุก FG ที่เหลือ =====
            # ถ้า total item demand ผลิตครบได้ภายใน latest FG target ด้วยเครื่องน้อยกว่า
            # ปัจจุบัน → ลดลง เพื่อ free เครื่องให้ item อื่น
            # (ทำนอกบล็อก _sim_produce > qty_left เพราะ FG ปัจจุบันอาจใหญ่กว่า 1 week cap)
            # ⚠️ guard ต้องเป็น >= 1 (ไม่ใช่ > 1) และไม่ผูกกับ "มี FG อื่นรออยู่" —
            # บล็อกนี้คือที่เดียวที่ GRADUAL BOOST จะเพิ่มเครื่องคืนเมื่อไม่ทัน target
            # ถ้า guard เดิม (> 1 + มี demand อื่น) งานที่เหลือ 1 เครื่อง หรือ order
            # สุดท้ายที่เครื่องเดียวไม่ทัน จะไม่มีทางได้เครื่องเพิ่มเลย
            # (เงื่อนไข demand ย้ายไปไว้ที่ฝั่ง "ลดเครื่อง" MIN MC FIT เท่านั้น)
            if carryover_mc + new_mc >= 1 and daily_capacity:
                _cur_fg_int_m = 0
                try:
                    _cur_fg_int_m = int(fg_week or 0)
                except Exception:
                    pass
                # ===== เก็บ target ทุกตัวของ item (หลาย FG = หลาย deadline) =====
                # เดิมยุบเหลือ "deadline ช้าสุด" ตัวเดียว → ก้อนที่ due เร็วถูกตัดสินว่า
                # ทันเสมอ (MIN MC FIT ลดเครื่องทุกสัปดาห์ ทั้งที่ก้อนต้น ๆ เลยกำหนดแล้ว)
                # ตอนนี้เก็บเป็น [(deadline_idx, qty), ...] เพื่อเช็คแบบสะสมทีละ deadline
                _latest_tgt_idx_m = rdd_idx if rdd_idx is not None else current_week_idx
                _tgt_sched_m = []  # [[deadline_idx, qty, is_current], ...]
                if rdd_idx is not None and qty_left > 0:
                    _tgt_sched_m.append([rdd_idx, float(qty_left), True])
                _cur_sc_m = str(sc_so_no).strip()
                for _oi_m, _or_m in orders_sorted.iterrows():
                    if str(_or_m.get("Item Code", "")).strip() != str(item).strip():
                        continue
                    _ofg_m = _or_m.get("FG Week")
                    if not pd.notna(_ofg_m):
                        continue
                    try:
                        _ofgi_m = int(_ofg_m)
                    except Exception:
                        continue
                    if _ofgi_m < _cur_fg_int_m:
                        continue
                    _ofs_m = str(_ofgi_m)
                    try:
                        if len(_ofs_m) >= 5:
                            _oy_m = int(_ofs_m[:4])
                            _owk_m = int(_ofs_m[4:])
                        elif len(_ofs_m) <= 2:
                            _oy_m = TODAY.year
                            _owk_m = int(_ofs_m)
                        else:
                            _oy_m = TODAY.year
                            _owk_m = int(_ofs_m)
                        _orow_m = calendar_week[
                            (calendar_week["YEAR"] == _oy_m)
                            & (calendar_week["WEEK_NUM"] == _owk_m)
                        ]
                        if _orow_m.empty:
                            continue
                        _ofb_m = str(_or_m.get("FOB_TYPE", "")).strip()
                        if _ofb_m in ["PILOT_RUN", "Salesman", "Salesman-PO", "Sample"]:
                            _ooff_m = 1
                        else:
                            _ooff_m = 4
                        _ordd_m = max(0, int(_orow_m.index[0]) - _ooff_m)
                        if _latest_tgt_idx_m is None or _ordd_m > _latest_tgt_idx_m:
                            _latest_tgt_idx_m = _ordd_m
                        # ก้อนที่กำลังผลิตอยู่ใส่ไปแล้วด้วย qty_left → ห้ามนับซ้ำ
                        _osc_m = str(
                            _or_m.get("SO_NO", _or_m.get("SC/SO NO", ""))
                        ).strip()
                        if _ofgi_m == _cur_fg_int_m and _osc_m == _cur_sc_m:
                            continue
                        _opend_m = pd.to_numeric(
                            _or_m.get("Pending Plan", 0), errors="coerce"
                        )
                        if pd.isna(_opend_m) or _opend_m <= 0:
                            continue
                        _tgt_sched_m.append([_ordd_m, float(_opend_m), False])
                    except Exception:
                        continue
                _tgt_sched_m.sort(key=lambda _s: _s[0])


                def _sim_total_fit_multi(total_mc, qty_override=None):
                    """เครื่อง total_mc เครื่อง ทำให้ทัน "ทุก target" ของ item หรือไม่

                    เช็คแบบสะสมทีละ deadline: ณ สิ้นสัปดาห์ของ deadline แต่ละตัว
                    ผลผลิตสะสมต้อง >= ยอดสะสมของก้อนที่ครบกำหนดภายในนั้น
                    (เดิมเช็คแค่ "ยอดรวมทั้งหมดจบภายใน deadline ช้าสุด" → ก้อนที่ due
                    เร็วถูกมองว่าทันเสมอ ทั้งที่ของจริงส่งไม่ทัน)
                    """
                    if total_mc <= 0 or daily_capacity is None:
                        return False
                    _q_all = float(qty_override if qty_override is not None else _qty_for_machine_calc)
                    if _q_all <= 0:
                        return True
                    _pw_idx_fit = week_index(plan_week)
                    # ปรับ schedule ให้ยอดรวมตรงกับ demand จริงที่เหลือ
                    # ⚠️ ห้าม scale ก้อนที่กำลังผลิต — qty_left ของมันแม่นอยู่แล้ว
                    # (scale ทั้งชุดจะทำให้ก้อนที่ due แล้วเล็กลงจนดู "ทัน" ทั้งที่ไม่ทัน)
                    # scale เฉพาะก้อนอนาคต เพราะ Pending Plan ใน orders_sorted อาจ stale
                    if _tgt_sched_m:
                        _cur_q_fit = sum(_s[1] for _s in _tgt_sched_m if _s[2])
                        _fut_tot_fit = sum(_s[1] for _s in _tgt_sched_m if not _s[2])
                        _fut_target_fit = max(0.0, _q_all - _cur_q_fit)
                        _scale_m = (
                            (_fut_target_fit / _fut_tot_fit) if _fut_tot_fit > 0 else 0.0
                        )
                        _sched_fit = [
                            [_s[0], _s[1] if _s[2] else _s[1] * _scale_m]
                            for _s in _tgt_sched_m
                        ]
                    else:
                        _fallback_d = (
                            _latest_tgt_idx_m if _latest_tgt_idx_m is not None else _pw_idx_fit
                        )
                        _sched_fit = [[_fallback_d, _q_all]]
                    # deadline ที่เลยไปแล้ว = สายแล้ว → ถือว่าต้องเสร็จ "สัปดาห์นี้" ให้ได้
                    # (ไม่ปล่อยผ่าน แต่ก็ไม่ไล่เพิ่มเครื่องไม่จบ — พอผลิตครบในสัปดาห์นี้ได้ก็ผ่าน)
                    if _pw_idx_fit is not None:
                        _sched_fit = [
                            [max(_d, _pw_idx_fit) if _d is not None else _pw_idx_fit, _qq]
                            for _d, _qq in _sched_fit
                        ]
                    _sched_fit.sort(key=lambda _s: _s[0])
                    _last_ddl_fit = _sched_fit[-1][0]

                    _wk = plan_week
                    _first = True
                    _cum_prod = 0.0   # ผลผลิตสะสม
                    _cum_need = 0.0   # ยอดที่ต้องส่งสะสม (ตาม deadline ที่ผ่านมาแล้ว)
                    _si = 0           # ตัวชี้ deadline ตัวถัดไปที่ยังไม่ตรวจ
                    _sim_cnt_fit = 0
                    while _wk is not None and _si < len(_sched_fit):
                        if _sim_cnt_fit > 300:  # SAFETY: กัน infinite loop ข้ามปลายปี
                            break
                        _wi = week_index(_wk)
                        if _wi is None:
                            break
                        # หักวันหยุดตามปฏิทินให้แล้วใน WorkDay.get_working_days() — ห้าม min ซ้ำ
                        _wd = get_working_days_by_factory(mc_group, total_mc, week=_wk, gauge=_sel_gauge)
                        if _first:
                            _c_cnt = min(total_mc, carryover_mc)
                            _n_cnt = max(0, total_mc - _c_cnt)
                            _c_prod = _c_cnt * _wd * daily_capacity
                            _n_prod = _n_cnt * max(0, _wd - item_setup_days) * daily_capacity
                            _first = False
                            _prod_wk = fold_round_mixed(
                                _c_prod + _n_prod, rev_weight, mc_group,
                                _wd * daily_capacity, _c_cnt,
                                max(0, _wd - item_setup_days) * daily_capacity, _n_cnt,
                            )
                        else:
                            _prod_wk = fold_round(
                                total_mc * _wd * daily_capacity, rev_weight, mc_group,
                                per_mc_cap=_wd * daily_capacity, mc_count=total_mc,
                            )
                        _cum_prod += _prod_wk
                        # ถึง deadline ตัวไหนบ้างในสัปดาห์นี้ → ตรวจยอดสะสม
                        while _si < len(_sched_fit) and _sched_fit[_si][0] <= _wi:
                            _cum_need += _sched_fit[_si][1]
                            if _cum_prod + 1e-6 < _cum_need:
                                return False  # ก้อนนี้ส่งไม่ทัน
                            _si += 1
                        if _wi >= _last_ddl_fit:
                            break
                        _wk = next_week(_wk)
                        _sim_cnt_fit += 1
                    return _si >= len(_sched_fit)
                # demand ที่เหลือจริง = qty_left ของ order นี้ + future orders
                # (แก้ _qty_for_machine_calc ที่ stale เพราะตั้งตอนเริ่ม order ด้วย original qty)
                _adjusted_qty_m = qty_left + max(0, _qty_for_machine_calc - _initial_qty_for_mc_calc)
                _cur_tot_m = carryover_mc + new_mc
                _min_tot_m = _cur_tot_m
                if not _s9_active:
                    for _tt in range(1, _cur_tot_m):
                        if _sim_total_fit_multi(_tt, qty_override=_adjusted_qty_m):
                            _min_tot_m = _tt
                            break
                # 🧷 ก้อนสีที่มีพี่น้อง (ก้อนไม่มีสีของใบเดียวกัน) รออยู่ → ห้ามลดเครื่อง
                # ด่านนี้ตัดสินจาก "ทันเดดไลน์ไหม" อย่างเดียว จึงยอมลดเหลือ 1 เครื่องได้ แล้วพอ
                # พี่น้องมาวางต่อ ยอดไม่พอในสัปดาห์เดียว ต้องตั้งเครื่องใหม่กลับ = เสีย setup ฟรี
                # ถือเครื่องเฉพาะเมื่อยอดที่รออยู่ยังเกินกำลัง 1 เครื่อง/สัปดาห์ (ก้อนจิ๋วไม่ต้องถือ)
                _split_hold_now = _split_hold_mc and max(
                    0.0, _qty_for_machine_calc - _initial_qty_for_mc_calc
                ) > (daily_capacity or 0) * (actual_working_days or 0)
                # ฝั่ง "ลดเครื่อง" ยังคงเงื่อนไขเดิม: ทำเฉพาะเมื่อมี demand ของ FG/SC อื่น
                # รออยู่ (การลดเครื่องตาม qty_left ล้วน มีบล็อก Optimize ด้านล่างจัดการแล้ว)
                if (
                    not _s9_active
                    and _min_tot_m < _cur_tot_m
                    and _qty_for_machine_calc > qty_left + 1.0
                    and not _split_hold_now
                ):
                    _new_carry = min(carryover_mc, _min_tot_m)
                    _new_new = max(0, _min_tot_m - _new_carry)
                    print(
                        f"[MIN MC FIT] {item} W{plan_week}: ลดเครื่อง "
                        f"{_cur_tot_m}→{_min_tot_m} (carry {carryover_mc}→{_new_carry}, "
                        f"new {new_mc}→{_new_new}, "
                        f"targets={[(int(_s[0]), int(_s[1])) for _s in _tgt_sched_m]}, "
                        f"adj_qty={_adjusted_qty_m:.0f}, total_demand={_qty_for_machine_calc:.0f})"
                    )
                    carryover_mc = _new_carry
                    new_mc = _new_new
                    available_machines = carryover_mc + new_mc
                    # recompute prod_days_new ตามจำนวนเครื่องใหม่
                    if new_mc > 0 and actual_working_days < item_setup_days:
                        prod_days_new = max(0, actual_working_days - 0.5)
                    elif new_mc > 0:
                        prod_days_new = max(0.5, actual_working_days - item_setup_days)
                    else:
                        prod_days_new = max(0, actual_working_days - item_setup_days)
                elif not _s9_active and _min_tot_m < _cur_tot_m and _split_hold_now:
                    print(
                        f"   🧷 [SPLIT HOLD] {item} W{plan_week}: ไม่ลดเครื่อง "
                        f"{_cur_tot_m}→{_min_tot_m} — ก้อนที่ยังไม่ให้สีของใบเดียวกันรออีก "
                        f"{max(0.0, _qty_for_machine_calc - _initial_qty_for_mc_calc):,.0f} กก. "
                        f"(ลดแล้วต้องตั้งเครื่องใหม่กลับ = เสีย setup ฟรี) "
                        f"| เครื่องส่วนเกินยังถูกงานสี item อื่นยึดคืนได้"
                    )
                # ===== GRADUAL MACHINE BOOST =====
                # หลัง reduction: ถ้าเครื่องยังไม่พอจบทัน latest target
                # → เพิ่ม new_mc ได้อีก <= MAX_NEW_SETUP_MC ต่อ week (gradual build-up)
                if not _s9_active and _latest_tgt_idx_m is not None:
                    _post_red_mc = carryover_mc + new_mc
                    if not _sim_total_fit_multi(_post_red_mc, qty_override=_adjusted_qty_m):
                        _avail_pool = get_actual_mc_remain(
                            mc_group, plan_week, gauge=_sel_gauge, item_code=item
                        )
                        # เพิ่มได้เท่าโควตาที่ผ้านี้ยังเหลือในสัปดาห์นี้ (ไม่ใช่เพดานเต็มทุกใบ)
                        _boost_cap = min(
                            _post_red_mc + _item_setup_room(item, mc_group, _sel_gauge, plan_week),
                            _avail_pool,
                            _post_red_mc + _remaining_slots,
                        )
                        _boosted = False
                        _bt = _post_red_mc + 1
                        while _bt <= int(_boost_cap):
                            if _sim_total_fit_multi(_bt, qty_override=_adjusted_qty_m):
                                new_mc = _bt - carryover_mc
                                available_machines = carryover_mc + new_mc
                                _boosted = True
                                break
                            _bt += 1
                        if not _boosted and int(_boost_cap) > _post_red_mc:
                            # ===== งานที่ยังไงก็ไม่ทัน target =====
                            # เดิม: "ไม่ทันแล้ว งั้นคว้าเครื่องให้มากที่สุดเท่าที่พูล/โควตาให้"
                            # → งานสายไปดูดเครื่อง+โควตา job จากงานที่ยังทันกำหนด ทั้งที่ไม่ได้ส่งเร็วขึ้นเลย
                            #   (เคสจริง: 10→12 เครื่องที่สัปดาห์ก่อนสุดท้าย แล้วจบสัปดาห์เดิมอยู่ดี)
                            # ใหม่: เพิ่มเฉพาะจำนวนที่ "ร่นสัปดาห์ที่จะทอจบได้จริง" และเอาน้อยที่สุดที่ร่นได้
                            _bst_cur_wks = _weeks_to_finish_with(_post_red_mc, _adjusted_qty_m)
                            _bst_pick = _post_red_mc
                            if _bst_cur_wks > 2:
                                # งานค้างยาว (เหลืออีก > 2 สัปดาห์) → เครื่องที่เพิ่มได้ใช้เต็มทุกตัวแน่นอน
                                # ไม่ถูกปล่อยกลางคัน และผลผลิตต่อสัปดาห์เพิ่มจริง → เปิดเต็มเหมือนเดิม
                                # (ตรวจเข้มเฉพาะ "ช่วงใกล้จบ" ซึ่งเป็นจุดที่เปิดเครื่องแล้วเสียเปล่า)
                                _bst_pick = int(_boost_cap)
                            else:
                                for _bst_try in range(_post_red_mc + 1, int(_boost_cap) + 1):
                                    if _weeks_to_finish_with(_bst_try, _adjusted_qty_m) < _bst_cur_wks:
                                        _bst_pick = _bst_try
                                        break
                            if _bst_pick > _post_red_mc:
                                new_mc = _bst_pick - carryover_mc
                                available_machines = carryover_mc + new_mc
                                _boosted = True
                            else:
                                print(
                                    f"   [BOOST SKIP] {item} W{plan_week}: เลย target แล้วและเพิ่มเครื่องถึง "
                                    f"{int(_boost_cap)} ก็ยังทอจบสัปดาห์เดิม ({_bst_cur_wks} สัปดาห์) "
                                    f"→ คงไว้ {_post_red_mc} เครื่อง (คืนเครื่อง/โควตาให้งานที่ยังทัน)"
                                )
                        if _boosted and available_machines > _post_red_mc:
                            print(
                                f"[GRADUAL BOOST] {item} W{plan_week}: "
                                f"{_post_red_mc}->{available_machines} MC "
                                f"(target W{_latest_tgt_idx_m})"
                            )
                            if new_mc > 0 and actual_working_days < item_setup_days:
                                prod_days_new = max(0, actual_working_days - 0.5)
                            elif new_mc > 0:
                                prod_days_new = max(0.5, actual_working_days - item_setup_days)
                            else:
                                prod_days_new = max(0, actual_working_days - item_setup_days)
            # เครื่องอุ่นจาก Hold รอสีสัปดาห์ก่อน → carry (ก่อน hard cap/SHAPE/payback ซึ่งตัดสินจาก new_mc
            # ไม่งั้นด่านพวกนี้ตัดเครื่องอุ่นทิ้งเพราะนึกว่าเป็นเครื่อง setup ใหม่)
            if is_continuing and not _s9_active and not rts_local_force and new_mc > 0:
                carryover_mc, new_mc = _color_gap_warm_split(
                    item, mc_group, _sel_gauge, week_index(plan_week), carryover_mc, new_mc
                )
                available_machines = carryover_mc + new_mc
                if new_mc > 0 and actual_working_days < item_setup_days:
                    prod_days_new = max(0, actual_working_days - 0.5)
                elif new_mc > 0:
                    prod_days_new = max(0.5, actual_working_days - item_setup_days)
                else:
                    prod_days_new = max(0, actual_working_days - item_setup_days)

            # Hard cap จริง: ห้าม new_mc เกินโควตาที่ผ้านี้เหลือในสัปดาห์นี้ แม้หลัง boost
            if not _s9_active:
                _new_capped = _cap_new_by_item_quota(
                    new_mc, item, mc_group, _sel_gauge, plan_week, tag="post-boost"
                )
                if _new_capped != new_mc:
                    new_mc = _new_capped
                    available_machines = carryover_mc + new_mc
                    if new_mc > 0 and actual_working_days < item_setup_days:
                        prod_days_new = max(0, actual_working_days - 0.5)
                    elif new_mc > 0:
                        prod_days_new = max(0.5, actual_working_days - item_setup_days)

            # ===== ทรงเครื่องต้องเป็น "ขึ้นให้ครบตอนต้น แล้วไล่ลง" (user 2 ก.ย. 2569) =====
            # ห้ามเพิ่มเครื่องหลังจากก้อนนี้เริ่มไล่ลงแล้ว — การเพิ่มกลางทาง/ตอนท้ายคือการ
            # จ่ายค่า setup ซ้ำเพื่อไล่งานที่ควรเร่งตั้งแต่ต้น (ทรง 2 2 1 2 3 = ผิดรูป)
            # ถ้ายังไม่ทัน target ต้องไปเพิ่มเครื่องตั้งแต่สัปดาห์แรก ไม่ใช่มาเพิ่มตอนหลัง
            # ไม่บังคับกับ S9/จ้างทอ (ไม่ใช้เครื่องในบ้าน) และ RTS (จำนวนล็อกมาจาก booking)
            if (
                MC_SHAPE_MONOTONE
                and not _s9_active
                and not rts_local_force
                and new_mc > 0
            ):
                _shape_key = (item, mc_group, str(order.get("MC_GUAGE", "")), sc_so_no, str(order.get("FG Week", "")))
                _peak_seen, _has_declined = _mc_shape_state.get(_shape_key, (0, False))
                if _has_declined and (carryover_mc + new_mc) > _peak_seen:
                    _new_allow = max(0, max(carryover_mc, _peak_seen) - carryover_mc)
                    if _new_allow < new_mc:
                        print(
                            f"📐 [SHAPE] {item} W{plan_week}: ทรงเริ่มไล่ลงแล้ว (peak {_peak_seen}) "
                            f"→ ห้ามเพิ่มเครื่องอีก new_mc {new_mc} → {_new_allow}"
                        )
                        new_mc = _new_allow
                        available_machines = carryover_mc + new_mc
                        if new_mc > 0 and actual_working_days < item_setup_days:
                            prod_days_new = max(0, actual_working_days - 0.5)
                        elif new_mc > 0:
                            prod_days_new = max(0.5, actual_working_days - item_setup_days)
                        else:
                            prod_days_new = max(0, actual_working_days - item_setup_days)

            # ===== Setup payback gate: เครื่องใหม่ต้องได้วิ่งเต็ม >= 2 สัปดาห์ =====
            # วางไว้ท้ายสุดของฝั่ง "เพิ่มเครื่อง" (หลัง GRADUAL BOOST + hard cap)
            # เพราะ boost สามารถเพิ่ม new_mc กลับมาได้ — โค้ดหลังจุดนี้มีแต่ลดเครื่อง
            if new_mc > 0 and not _s9_active and not rts_local_force:
                _pb_floor = max(0, _cyl_pending_cnt)  # เครื่อง cylinder change ที่ user อนุมัติ
                if carryover_mc <= 0:
                    _pb_floor = max(_pb_floor, 1)  # ไม่มี carry → ต้องเหลือเครื่องผลิตอย่างน้อย 1
                # demand ที่เหลือจริง = qty_left ของ FG นี้ + demand ของ FG อนาคต
                # (_qty_for_machine_calc ตั้งตอนเริ่ม order ด้วย original qty → stale)
                _pb_qty = qty_left + max(
                    0.0, _qty_for_machine_calc - _initial_qty_for_mc_calc
                )
                # ⚠️ payback เป็นเกณฑ์ "ประหยัด setup" ไม่ใช่เกณฑ์ "ทันงาน"
                # ถ้าเครื่องชุดนี้ยังจบไม่ทัน target → ห้ามลดเครื่องด้วยเกณฑ์นี้
                # (เศษ < 1 พับ ผลิตไม่ได้อยู่แล้ว → ใช้ rev_weight เป็น tolerance
                #  ให้ตรงกับเงื่อนไขหยุดลูปใน _forward_sim)
                _pb_late = _forward_sim(carryover_mc, new_mc, _pb_qty) > (rev_weight or 0)
                # 🎨 ก้อนที่ยังไม่ให้สี: ห้ามแลกวัน setup ทิ้งเพื่อร่นสัปดาห์ (user เคาะ 7 ก.ย. 2569)
                # เครื่องที่ตั้งในสัปดาห์ที่เหลือวันทอน้อยกว่าวัน setup = จ่าย 5 วันเพื่อทอ 2 วัน
                # และเครื่องตัวนั้นควรถูกเก็บไว้ให้ item ที่เปิดสีแล้วมากกว่า → ให้เศษไหลไปสัปดาห์
                # ถัดไปแทน  (ยกเว้นเมื่อยังไม่มีเครื่องเดินเลย — _pb_floor กันไว้ให้อย่างน้อย 1 ตัว
                #  ไม่งั้นงานไม่มีสีจะตั้งเครื่องไม่ได้เลยทุกสัปดาห์แล้วตกเป็นงานล้น)
                if (
                    _pb_late
                    and not _CUR_ORDER_COLOR_PART
                    and carryover_mc > 0
                    and prod_days_new < item_setup_days
                ):
                    print(
                        f"   🎨 [SETUP PAYBACK NC] {item} W{plan_week}: ก้อนที่ยังไม่ให้สี — "
                        f"เครื่องใหม่ได้ทอ {prod_days_new:.1f} วัน < setup {item_setup_days} วัน "
                        f"→ ไม่ยกเว้นด่านคุ้ม setup (carry={carryover_mc})"
                    )
                    _pb_late = False
                if _pb_late:
                    # ⚠️ เดิม "ไม่ทัน target = ข้ามด่าน payback ทั้งด่าน" ซึ่งเหมารวมเกินไป
                    # งานสายจึงเปิดเครื่องใหม่ได้เรื่อย ๆ โดยไม่มีใครถามว่าเครื่องนั้นได้ใช้จริงไหม
                    # (เคสจริง: เปิด 2 เครื่องสัปดาห์ก่อนสุดท้าย วิ่งสัปดาห์เดียวแล้วปล่อย = เสีย setup ฟรี)
                    # ยกเว้นให้เฉพาะเครื่องที่ "ร่นสัปดาห์ที่จะทอจบได้จริง" เท่านั้น
                    _pb_cur_w = _weeks_to_finish_with(carryover_mc, _pb_qty)
                    _pb_new_w = _weeks_to_finish_with(carryover_mc + new_mc, _pb_qty)
                    # ตรวจความคุ้ม setup เฉพาะ "ช่วงใกล้จบงาน" (เหลือ ≤ 2 สัปดาห์)
                    # ซึ่งเป็นจุดเดียวที่เครื่องเปิดใหม่เสี่ยงวิ่งไม่ถึง 2 สัปดาห์แล้วถูกปล่อย
                    # งานค้างยาว (ยังเหลืออีกหลายสัปดาห์) เครื่องได้ใช้เต็มแน่นอน → ไม่ต้องตรวจ
                    _pb_help = (_pb_new_w > 2) or (_pb_new_w < _pb_cur_w)
                    if _pb_help:
                        print(
                            f"   [SETUP PAYBACK SKIP] {item} W{plan_week}: ยังไม่ทัน target แต่เครื่องใหม่"
                            f"ร่นสัปดาห์ที่จบได้จริง (carry={carryover_mc}, new={new_mc}, "
                            f"qty={_pb_qty:.0f}) → คงเครื่องใหม่ไว้"
                        )
                    else:
                        print(
                            f"   [SETUP PAYBACK] {item} W{plan_week}: เลย target แล้ว แต่เครื่องใหม่ "
                            f"{new_mc} ตัวไม่ได้ทำให้จบเร็วขึ้น → ตรวจความคุ้ม setup ตามปกติ"
                        )
                        _pb_late = False   # ไม่ช่วยร่น → ต้องผ่านด่านคุ้ม setup เหมือนงานปกติ
                while new_mc > _pb_floor and not _pb_late:
                    _pb_weeks = _new_mc_full_weeks(carryover_mc, new_mc, _pb_qty)
                    if _pb_weeks >= 2:
                        break
                    print(
                        f"   [SETUP PAYBACK] {item} W{plan_week}: new {new_mc}→{new_mc - 1} "
                        f"(เครื่องใหม่ได้วิ่งเต็มแค่ {_pb_weeks} สัปดาห์ < 2 → setup ไม่คุ้ม, "
                        f"carry={carryover_mc}, qty={_pb_qty:.0f})"
                    )
                    new_mc -= 1

                # ===== เครื่องใหม่ต้อง "มีที่ยืน" สัปดาห์ถัดไปด้วย (user เคาะ 2 ก.ย. 2569) =====
                # ตั้งเครื่องแล้วสัปดาห์ถัดไปพูลไม่เหลือให้ carry = จ่ายค่า setup ทิ้งเปล่า
                # และลูกโซ่ขาดทันทีตั้งแต่สัปดาห์ที่ 2 (เคสจริง FD4DRTRN74/03A0 W38:
                # ตั้ง 2 เครื่อง setup 10 วัน แต่ W39 พูลว่าง 0 → เหลือ 1 เครื่องลากยาว 6
                # สัปดาห์ แล้วยังต้องตั้งเครื่องใหม่อีกตัวที่ W45)
                # ก้อนสีนับ "เครื่องที่ยึดคืนได้" ด้วย (สิทธิ์เดียวกับตอนวางจริง)
                if NEW_MC_MUST_STAND and new_mc > 0 and not _core_production_schedule:
                    _pb_next_w = next_week(plan_week)
                    _pb_cap_now = (daily_capacity or 0) * (
                        carryover_mc * actual_working_days + new_mc * prod_days_new
                    )
                    # งานจบในสัปดาห์นี้แล้ว → ไม่ต้องมีที่ยืนสัปดาห์หน้า
                    if _pb_next_w is not None and (_pb_qty - _pb_cap_now) > (rev_weight or 0):
                        _pb_next_avail = int(
                            _avail_mc_for_order(mc_group, _pb_next_w, _sel_gauge, item) or 0
                        )
                        if _pb_next_avail < carryover_mc + new_mc:
                            _pb_allow = max(_pb_floor, _pb_next_avail - carryover_mc)
                            if _pb_allow < new_mc:
                                print(
                                    f"   🧱 [NEW MC STAND] {item} W{plan_week}: new {new_mc}→{_pb_allow} "
                                    f"(สัปดาห์หน้า W{_pb_next_w} เครื่องว่าง {_pb_next_avail} < "
                                    f"{carryover_mc + new_mc} → เครื่องที่ตั้งจะอยู่ต่อไม่ได้)"
                                )
                                new_mc = _pb_allow

                # ===== เครื่องชุดนี้ต้องวิ่งต่อได้ "ทุกสัปดาห์" ที่งานยังไม่จบ → ลดเครื่องใหม่ก่อน =====
                # เดิมด่าน NO CARRY ROOM (ท้ายลูป) ข้ามทั้งสัปดาห์เมื่อติด แม้เครื่องเดิมทอต่อได้
                # ไม่ต้อง setup (เคสจริง 14 ก.ย. 2569 FD1BASMK32/01B0 RAOO W42: carry 2 + new 2
                # ติด W44 → W42 ว่างทั้งแถว) ตอนนี้ลดเครื่องใหม่ลงทีละตัวก่อน ; ถ้ายังไม่มี carry
                # (_pb_floor = 1) และยังติด → ปล่อยให้ด่านเดิมข้ามสัปดาห์เหมือนเดิม
                if new_mc > _pb_floor and not _core_production_schedule:
                    _cs_from = new_mc
                    _cs_block = _span_block_with_grab(carryover_mc, new_mc)
                    while _cs_block is not None and new_mc > _pb_floor:
                        new_mc -= 1
                        _cs_block = _carry_span_blocked(carryover_mc, new_mc) if new_mc > 0 else None
                    if new_mc < _cs_from:
                        print(
                            f"   🧱 [CARRY ROOM TRIM] {item} W{plan_week}: new {_cs_from}→{new_mc} "
                            f"(carry={carryover_mc}) — เครื่องใหม่วิ่งต่อไม่ได้ครบทุกสัปดาห์ "
                            f"→ ทอด้วยเครื่องที่วิ่งต่อได้แทนการข้ามทั้งสัปดาห์"
                        )

                available_machines = carryover_mc + new_mc
                if new_mc > 0 and actual_working_days < item_setup_days:
                    prod_days_new = max(0, actual_working_days - 0.5)
                elif new_mc > 0:
                    prod_days_new = max(0.5, actual_working_days - item_setup_days)
                else:
                    prod_days_new = max(0, actual_working_days - item_setup_days)

            # ===== Optimize: ลดเครื่องให้น้อยสุดที่ยังผลิตพอครอบคลุม qty_left =====
            # เช่น week15 carry=3 แต่ qty_left น้อย → ใช้แค่ 1 เครื่องก็เสร็จใน week นี้
            # ใช้การจำลองผลิตจริง (รวม rev_weight rounding) เพื่อความแม่นยำ
            def _sim_produce(c_mc, n_mc):
                if daily_capacity is None:
                    return 0
                c_cap = daily_capacity * prod_days_old * c_mc
                n_cap = daily_capacity * prod_days_new * n_mc
                total_cap = c_cap + n_cap
                if rev_weight and rev_weight > 0 and total_cap > 0:
                    return fold_round_mixed(
                        total_cap, rev_weight, mc_group,
                        daily_capacity * prod_days_old, c_mc,
                        daily_capacity * prod_days_new, n_mc,
                    )
                return total_cap

            if carryover_mc + new_mc > 0 and _sim_produce(carryover_mc, new_mc) > qty_left:
                # ใช้ demand ระดับ item เพื่อไม่ลดเครื่องถ้ายังมี FG ของ item เดียวกันรอผลิต
                # 🔧 FIX: คำนวณ demand ที่ยังไม่ได้วางแผน = total demand - ที่ plan ไปแล้ว - qty_left ปัจจุบัน
                # ครอบคลุมทั้ง 2 กรณี:
                #   1. คนละ SC แต่ item เดียวกัน (SC อื่น plan ไปแล้ว → ไม่ hold เครื่อง)
                #   2. SC เดียวกัน คนละ FG (FG ก่อนหน้า plan ไปแล้ว → ไม่ hold เครื่อง)
                # _qty_for_machine_calc ครอบเฉพาะ FG ที่ยังไม่ plan อยู่แล้ว
                # หักแค่ order_qty ของ FG ปัจจุบัน = demand ของ FG ถัดไป
                next_fg_qty = max(0, _qty_for_machine_calc - order_qty)

                opt_carry, opt_new = carryover_mc, new_mc  # fallback = ไม่ลด

                # ถ้ามี FG ถัดไปใน SC เดียวกัน → ไม่ลดเครื่อง เพื่อ carry ต่อในสัปดาห์เดียวกัน/สัปดาห์ถัดไป
                # 🔧 FIX: ใช้ threshold > 1.0 แทน > 0 เพื่อป้องกัน floating-point residual
                # จากการหัก qty ที่มีทศนิยม (เช่น 2530 - 687.89 - 1841.77 = 0.34)
                if next_fg_qty > 1.0:
                    # 🔧 หา min mc ที่ยังผลิตทัน "ทุก target" ของ FG ที่เหลือ
                    # เพื่อ free เครื่องให้ item อื่น
                    # ⚠️ เดิมบล็อกนี้มี _sim_total_fit ของตัวเอง ที่เช็คแค่ deadline ช้าสุด
                    # (_latest_tgt_idx_opt) → ลดเครื่องทั้งที่ก้อนที่ due เร็วส่งไม่ทัน
                    # ตอนนี้ใช้เกณฑ์เดียวกับบล็อก Multi-FG ด้านบน (เช็คสะสมทีละ deadline)
                    def _sim_total_fit(total_mc):
                        if total_mc <= 0 or daily_capacity is None:
                            return False
                        try:
                            return _sim_total_fit_multi(
                                total_mc, qty_override=_adjusted_qty_m
                            )
                        except NameError:
                            # บล็อก Multi-FG ไม่ได้รัน → ไม่มีเกณฑ์ target ให้เช็ค
                            # เลือกทางปลอดภัย: ไม่ลดเครื่อง
                            return False

                    _current_total_opt = carryover_mc + new_mc
                    _min_total_opt = _current_total_opt
                    for _try_t in range(1, _current_total_opt):
                        if _sim_total_fit(_try_t):
                            _min_total_opt = _try_t
                            break

                    # 🧷 เกณฑ์เดียวกับด่าน MIN MC FIT ด้านบน — ก้อนสีที่มีพี่น้อง (ก้อนไม่มีสี
                    # ของใบเดียวกัน) รออยู่ ห้ามลดเครื่อง ไม่งั้นพี่น้องต้องตั้งเครื่องใหม่กลับ
                    _split_hold_opt = _split_hold_mc and max(
                        0.0, _qty_for_machine_calc - _initial_qty_for_mc_calc
                    ) > (daily_capacity or 0) * (actual_working_days or 0)
                    if _min_total_opt < _current_total_opt and _split_hold_opt:
                        print(
                            f"   🧷 [SPLIT HOLD] {item} W{plan_week}: ไม่ลดเครื่อง "
                            f"{_current_total_opt}→{_min_total_opt} (Optimize) — ก้อนที่ยังไม่ให้สี"
                            f"ของใบเดียวกันรออีก "
                            f"{max(0.0, _qty_for_machine_calc - _initial_qty_for_mc_calc):,.0f} กก."
                        )
                        _min_total_opt = _current_total_opt

                    if _min_total_opt < _current_total_opt:
                        opt_carry = min(carryover_mc, _min_total_opt)
                        opt_new = max(0, _min_total_opt - opt_carry)
                        print(
                            f"[MIN MC FIT] {item} W{plan_week}: ลดเครื่อง "
                            f"{_current_total_opt}→{_min_total_opt} "
                            f"(targets={[(int(_s[0]), int(_s[1])) for _s in _tgt_sched_m]}, "
                            f"demand={_qty_for_machine_calc:.0f})"
                        )
                    else:
                        opt_carry = carryover_mc
                        opt_new = new_mc
                else:
                    # ไม่มี FG ถัดไป หรือ capacity เพียงพอสำหรับทั้ง FG ปัจจุบันและถัดไป
                    # ให้ลดเครื่องตามปกติ
                    # ขั้นที่ 1: ลอง carry-only (ไม่ต้อง setup เพิ่ม) หาน้อยสุดที่ produce ≥ qty_left
                    found = False
                    for try_c in range(1, carryover_mc + 1):
                        if _sim_produce(try_c, 0) >= qty_left:
                            opt_carry = try_c
                            opt_new = 0
                            found = True
                            break
                    if not found and new_mc > 0:
                        # ขั้นที่ 2: ต้องมี new ด้วย → ลด new ให้น้อยสุด
                        # ถ้า carryover_mc=0 ต้องเริ่มจาก 1 เพราะต้องมีเครื่องอย่างน้อย 1 เครื่อง
                        _try_n_start = 0 if carryover_mc > 0 else 1
                        for try_n in range(_try_n_start, new_mc + 1):
                            if _sim_produce(carryover_mc, try_n) >= qty_left:
                                opt_carry = carryover_mc
                                opt_new = try_n
                                break

                if opt_carry + opt_new < available_machines:
                    carryover_mc = opt_carry
                    new_mc = opt_new
                    available_machines = opt_carry + opt_new

            # ===== หางลดครึ่ง: ห้ามลดต่ำกว่าครึ่งของสัปดาห์ก่อนในทีเดียว =====
            # ด่านลดเครื่องด้านบน (CARRYOVER REDUCE / MIN MC FIT / Optimize) ตัดตามยอดที่เหลือล้วน ๆ
            # ไม่ดูสัปดาห์ก่อนเลย → ได้ทรง 10→0 / 6→2 (หักหัว)
            # ตรงนี้ยกกลับให้อยู่ที่ "ครึ่งหนึ่งของสัปดาห์ก่อน" — เครื่องที่ยกกลับเป็น carry ทั้งหมด
            # (อุ่นอยู่แล้ว ไม่เสีย setup ไม่กินโควตา job) และไม่เกินเครื่องว่างจริงในพูล
            # ใช้เฉพาะงานที่เครื่อง >= MC_TAPER_MIN_MC — งานเล็กคงพฤติกรรมเดิม
            if (
                not _s9_active
                and not rts_local_force
                and is_continuing
                and qty_left > 0
                and int(prev_machines or 0) >= MC_TAPER_MIN_MC
                # ⚠️ ห้ามใส่เพดานจำนวนขั้นตรงนี้ — ด่านนี้คือ "กันหักหัวกลางคัน" ไม่ใช่การยืดหาง
                # เคยลองใส่แล้วงานค้างใหญ่ถูกตัดเหลือ 1 เครื่องกลางแผนแล้วต้องไต่ใหม่ (เสีย 12.6 ตัน)
                # เพดานขั้น (MC_TAPER_MAX_STEPS) คุมเฉพาะความยาวหางในแผนล่วงหน้าเท่านั้น
                # ⚠️ ข้ามแถวที่ "เติมเข้าเครื่องที่วิ่งอยู่แล้ว" ในสัปดาห์เดียวกัน (SC อื่นของ item เดียวกัน)
                # แถวพวกนี้ใช้เครื่องร่วมกับแถวแรก ไม่ได้ประกาศเครื่องเอง — ถ้ายกเครื่องให้จะนับซ้ำ
                # (เจอจริง: W43 DOUBLE-40|18 บัญชีภายในสูงกว่าแผนจริง 3 เครื่อง)
            ):
                _tp_half = max(1, int(math.ceil(int(prev_machines) * 0.5)))
                _tp_cur = carryover_mc + new_mc
                if _tp_cur < _tp_half:
                    _tp_room = max(
                        0,
                        get_actual_mc_remain(
                            mc_group, plan_week, gauge=_sel_gauge, item_code=item
                        ),
                    )
                    _tp_add = min(_tp_half - _tp_cur, int(prev_machines) - carryover_mc, _tp_room)
                    # โควตา POLY/COTTON เป็นคนละกระเป๋ากับพูลรวม — ต้องเช็คด้วย
                    # ไม่งั้นยกเครื่องเกินที่กันไว้ (เจอจริง: W40 SINGLE-32|36 [POLY] ใช้ 9 จากงบ 8)
                    _tp_sp = _mc_special_remain(mc_group, plan_week, _sel_gauge, item)
                    if _tp_sp is not None:
                        _tp_add = min(_tp_add, max(0, int(_tp_sp) - _tp_cur))
                    if _tp_add > 0:
                        carryover_mc += _tp_add
                        available_machines = carryover_mc + new_mc
                        _taper_steps_used += 1
                        print(
                            f"[MC TAPER] {item} W{plan_week}: เครื่อง {_tp_cur}→{available_machines} "
                            f"(สัปดาห์ก่อน {int(prev_machines)} → ลดได้อย่างมากครึ่งหนึ่ง = {_tp_half}"
                            f", ขั้นที่ {_taper_steps_used}/{MC_TAPER_MAX_STEPS})"
                        )

            # ===== [S9 TAIL TAPER] ใบที่ส่วนล้นจะไปจ้างทอ → ในบ้านต้องไล่ลงให้จบภายใน target =====
            # (user 15 ก.ย. 2569) เดิมในบ้านวางเต็มจนถึงสัปดาห์ target แล้ว [S9 AUTO TAIL] ยกยอดค้าง
            # ทั้งหมดไปจ้างทอ → ในบ้านจบฮวบ (เจอจริง: FD5BASPG10A0 SC 722428 RAO g19 W49 = 10 เครื่อง
            # ตั้งใหม่ 2 แล้วหายไปเลย) ; ด่าน MC TAPER ข้างบนไม่ช่วยเพราะยอดถูกยกไปข้างนอกหมดแล้ว
            # ตอนนี้: ในช่วงหาง (MC_TAPER_MAX_STEPS สัปดาห์สุดท้ายถึง target) ถ้าให้เต็มที่แล้วก็ยังไม่ทันแน่
            # และส่วนที่ล้นจะออกจ้างทอได้จริง (เกณฑ์เดียวกับ S9 AUTO TAIL) → เครื่อง ≤ ครึ่งของสัปดาห์ก่อน
            # carry ล้วน ไม่ตั้งเครื่องใหม่ ; ยอดที่ลดไปค้างอยู่ใน qty_left ให้ S9 AUTO TAIL รับไปเอง
            # ตัดสินทีละใบ — ไม่ลดเครื่องของใบอื่นที่ทอทันในตัวเอง ; ข้ามแถวเติมเข้าเครื่องของ SC อื่นสัปดาห์เดียวกัน
            _tt_idx = week_index(plan_week) if plan_week is not None else None
            _tt_steps = (rdd_idx - _tt_idx) if (rdd_idx is not None and _tt_idx is not None) else None
            if (
                _s9_auto_base
                and not _s9_auto
                and not _s9_active
                and not rts_local_force
                and is_continuing
                and qty_left > 0
                and _tt_steps is not None
                and 0 <= _tt_steps < MC_TAPER_MAX_STEPS
                and int(prev_machines or 0) >= MC_TAPER_MIN_MC
                and (carryover_mc + new_mc) > max(1, int(math.ceil(int(prev_machines) * 0.5)))
                and daily_capacity
                and order.get("PLAN_PRIORITY") in (0, 1)
                and (item_upper, _normalize_gauge(_sel_gauge)) in _s9_eligible_items
                and remaining_week_cap.get((plan_week, item, mc_group)) is None
                and (item, plan_week, mc_group) not in _existing_item_week_mc
            ):
                # ให้เต็มที่: สัปดาห์นี้ตามเครื่องที่ได้ + สัปดาห์ถัดไปถึง target ทอเต็มวันและตั้งเพิ่มได้เต็มเพดาน
                _tt_best = _sim_produce(carryover_mc, new_mc)
                for _k in range(1, _tt_steps + 1):
                    _tt_best += (
                        (carryover_mc + new_mc + MAX_NEW_SETUP_MC * _k)
                        * daily_capacity * actual_working_days
                    )
                _tt_pool = _s9_pool_row(
                    _sel_gauge, _mc_to_type1(mc_group, _sel_gauge), order_material_content, item
                )
                _tt_s9_room = False
                if _tt_pool is not None:
                    _tt_weeks = [plan_week]
                    if 0 <= rdd_idx < len(calendar_week):
                        _tt_weeks.append(int(calendar_week.iloc[rdd_idx]["WEEK"]))
                    _tt_s9_room = any(_s9_pool_remain(_tt_pool[5], _w) > 0 for _w in _tt_weeks)
                if qty_left > _tt_best + (rev_weight or 0) and _tt_s9_room:
                    _tt_half = max(1, int(math.ceil(int(prev_machines) * 0.5)))
                    _tt_from = (carryover_mc, new_mc)
                    carryover_mc = min(carryover_mc, _tt_half)
                    new_mc = 0
                    available_machines = carryover_mc + new_mc
                    prod_days_new = max(0, actual_working_days - item_setup_days)
                    print(
                        f"[S9 TAIL TAPER] {item} SC {sc_so_no} W{plan_week}: carry/new {_tt_from} → "
                        f"({carryover_mc}, 0) — ยังค้าง {qty_left:,.0f} กก. ให้เต็มที่ถึง target ได้ "
                        f"{_tt_best:,.0f} → ส่วนล้นไปจ้างทอ ในบ้านไล่ลงครึ่งของสัปดาห์ก่อน ({int(prev_machines)})"
                    )

            # เก็บตกเครื่องอุ่นจาก Hold รอสีอีกรอบ — Optimize / MC TAPER ด้านบนเปลี่ยน carry/new ได้
            # ต้องทำก่อน JOB CAP ไม่งั้นเครื่องอุ่นถูกนับเป็นโควตา setup แล้วโดนตัด
            if is_continuing and not _s9_active and not rts_local_force and new_mc > 0:
                carryover_mc, new_mc = _color_gap_warm_split(
                    item, mc_group, _sel_gauge, week_index(plan_week), carryover_mc, new_mc
                )
                available_machines = carryover_mc + new_mc
                if new_mc > 0 and actual_working_days < item_setup_days:
                    prod_days_new = max(0, actual_working_days - 0.5)
                elif new_mc > 0:
                    prod_days_new = max(0.5, actual_working_days - item_setup_days)
                else:
                    prod_days_new = max(0, actual_working_days - item_setup_days)

            # ===== Hard-cap: enforce job cap ก่อนคำนวณ produce เสมอ =====
            # S9: ไม่หัก job cap และไม่มี limit เครื่อง (ใช้เต็ม pool ที่มี)
            if not _s9_active:
                _type_used_now = get_type_used_jobs(plan_week, mc_group)
                # carryover ไม่ควรนับเป็น new setup jobs เสมอ
                _committed_carryover = carryover_mc
                # check_job_capacity_limit รับ "จำนวนเครื่องรวม" (carryover+new) แล้วคืนจำนวนรวมที่อนุญาต
                # ต้องส่ง carryover_mc + new_mc ไม่ใช่ new_mc เดี่ยว ไม่งั้น carryover จะถูกลบซ้ำ → cap ไม่ทำงาน
                _total_allowed = check_job_capacity_limit(mc_group, carryover_mc + new_mc, False, _type_used_now, committed_carryover=_committed_carryover)
                _allowed_new = max(0, _total_allowed - carryover_mc)
                # 🎨 งานสี: โควตา setup ของสัปดาห์เต็ม ≠ จบเห่ — ยึด "ช่อง setup" คืนจาก
                # งานที่ยังไม่ให้สีที่กำลังจะตั้งเครื่องใหม่สัปดาห์นั้นได้ (ปลดเครื่องใหม่ของมัน
                # = คืนทั้งเครื่องและโควตา job) แล้วคิดโควตาใหม่อีกครั้ง
                # ⚠️ ด่านนี้คือจุดที่ทำให้เคส FD4DRTRN74/03A0 ตกทุกสัปดาห์ต้น: เครื่องยึดมาได้
                # แล้วแต่ไม่มีช่อง setup → new_mc ถูกตัดเป็น 0 แล้วข้ามสัปดาห์แบบเงียบ ๆ
                if (
                    _allowed_new < new_mc
                    and MAIN_COLOR_GRAB_ENABLED
                    and _CUR_ORDER_COLOR_PART
                    and not rts_local_force
                ):
                    if _main_color_grab(
                        item, mc_group, _sel_gauge, week_pos(plan_week),
                        new_mc - _allowed_new,
                        tk_week=int(calendar_week.iloc[rdd_idx]["WEEK"])
                        if rdd_idx is not None and 0 <= rdd_idx < len(calendar_week) else None,
                    ) > 0:
                        _type_used_now = get_type_used_jobs(plan_week, mc_group)
                        _total_allowed = check_job_capacity_limit(
                            mc_group, carryover_mc + new_mc, False, _type_used_now,
                            committed_carryover=_committed_carryover,
                        )
                        _allowed_new = max(0, _total_allowed - carryover_mc)
                if _allowed_new < new_mc:
                    print(
                        f"   🔒 [JOB CAP] {item} W{plan_week} {mc_group}: โควตา setup เหลือ "
                        f"{_allowed_new} → ลดเครื่องใหม่ {new_mc} → {_allowed_new}"
                    )
                    new_mc = _allowed_new
                available_machines = carryover_mc + new_mc
            # ===== [POOL GATE] ด่านสุดท้ายของลูปหลัก: เครื่องทั้งแถว (carry + ใหม่) ห้ามเกินพูลว่างจริง =====
            # user 14 ก.ย. 2569: "ต้องแก้ที่ลูปหลัก ไม่ต้องมีรอบซ่อมแก้ให้" — เดิมหลายเส้นทางตั้งเครื่อง
            # โดยไม่เช็คพูลรวม (keep เครื่อง carry แล้วด่าน gap แปลงเป็นเครื่องใหม่ / บัญชีภายในใบเพี้ยน ฯลฯ)
            # → วางเกินพูล 9–12 จุดต่อรอบ แล้ว _repair_pool_overflow ตัดทีหลังจนยอดไหลเลย target
            # จุดนี้อยู่หลังทุกด่านที่ "เพิ่ม" เครื่อง (โค้ดหลังจากนี้มีแต่ลด) → ตัดตรงนี้ครอบทุกเส้นทาง
            # สร้างบัญชีพูลใหม่จากแถวแผนจริงก่อนเทียบ (บัญชีสะสมระหว่างวางใบเดียวเพี้ยนได้)
            # เครื่องที่ booking ถืออยู่ถูกหักจาก TOTAL_MC_REMAIN แล้ว → บวกกลับ (สูตรเดียวกับตอนหักบัญชี)
            # ยกเว้นแถวที่เครื่องนับไปแล้ว (เติมเข้าเครื่องเดิมสัปดาห์เดียวกัน / FG split) · จ้างทอ · RTS
            # เปิดถาวร — ไม่มีสวิตช์บนหน้า "ตั้งค่าระบบ" (user 24 ก.ย. 2569)
            if (
                not _s9_active
                and not rts_local_force
                and (carryover_mc + new_mc) > 0
                and remaining_week_cap.get((plan_week, item, mc_group)) is None
                and (item, plan_week, mc_group) not in _existing_item_week_mc
            ):
                _resync_pool_ledger()
                _pg_room = max(0, int(get_actual_mc_remain(
                    mc_group, plan_week, gauge=_sel_gauge, item_code=item, allow_negative=True
                ) or 0) + int(_booking_week_mc or 0))
                if carryover_mc + new_mc > _pg_room:
                    _pg_from = (carryover_mc, new_mc)
                    new_mc = max(0, min(new_mc, _pg_room - carryover_mc))   # ตัดเครื่องใหม่ก่อน
                    carryover_mc = max(0, min(carryover_mc, _pg_room - new_mc))
                    available_machines = carryover_mc + new_mc
                    print(f"   🧱 [POOL GATE] {item} SC {sc_so_no} W{plan_week} {mc_group}: "
                          f"carry/new {_pg_from} → ({carryover_mc}, {new_mc}) — พูลว่างจริง {_pg_room}")
            if new_mc == 0 and carryover_mc == 0:
                # ไม่มีเครื่องเลย ข้ามไป week ถัดไป
                print(
                    f"[NO MC/JOB] {item} SC {sc_so_no} W{plan_week}: ไม่มีทั้งเครื่องเดิมและช่อง "
                    f"setup ใหม่ → ข้ามไปสัปดาห์ถัดไป"
                )
                if _s9_active:
                    _s9_no_cap_weeks.add(plan_week)  # บันทึก week ที่ S9 ลองแล้วได้ 0 เครื่อง
                plan_week = next_week(plan_week)
                if plan_week is None:
                    break              # หมดสัปดาห์ให้วาง (สุด horizon) — ที่เหลือเป็นงานล้น
                continue
            # YD-ORDERS: ล็อกจำนวนเครื่องภายใน week เดียวกัน
            # SO แรกกำหนดจำนวนเครื่อง → SO ถัดไปในสัปดาห์เดียวกันใช้เท่าเดิม
            _yd_lock_key = (item, mc_group, plan_week)
            if order_type == "YD-ORDERS" and _yd_lock_key in _yd_week_locked_mc:
                _locked = _yd_week_locked_mc[_yd_lock_key]
                available_machines = _locked[0]
                carryover_mc = _locked[1]
                new_mc = 0  # เครื่อง setup ไปแล้วจาก SO แรก ไม่ต้อง setup ซ้ำ
                print(f"[YD LOCK] {item} W{plan_week} SO={sc_so_no}: ใช้เครื่องเท่าเดิม actual={available_machines} carry={carryover_mc}")
            # 🔧 FIX: ถ้า qty_left < rev_weight × จำนวนเครื่อง → ลดเครื่องลง
            # เพราะไม่สามารถแบ่ง qty น้อยกว่า rev_weight ลงหลายเครื่องได้
            _next_fg_qty_rev = max(0, _qty_for_machine_calc - order_qty)
            if (
                rev_weight and rev_weight > 0
                and available_machines > 1
                and qty_left < rev_weight * available_machines
                and _next_fg_qty_rev <= 1.0
            ):
                _max_mc_for_qty = max(1, int(qty_left // rev_weight))
                if _max_mc_for_qty < available_machines:
                    print(f"[REV_WEIGHT CAP] {item} W{plan_week}: qty_left={qty_left:.0f} < rev_weight({rev_weight})×mc({available_machines})={rev_weight*available_machines:.0f} → ลดเครื่องจาก {available_machines} เป็น {_max_mc_for_qty}")
                    # ลด available_machines (total) โดยตรง แล้วคำนวณ new_mc/carryover_mc ใหม่
                    available_machines = _max_mc_for_qty
                    # คำนวณ new_mc และ carryover_mc ใหม่จาก available_machines ที่ลดลงแล้ว
                    # พยายามรักษา carryover_mc ให้มากที่สุด (ลด new_mc ก่อน)
                    new_mc = max(0, min(new_mc, available_machines - carryover_mc))
                    carryover_mc = available_machines - new_mc

            # setup_days_used สำหรับ log — เครื่องใหม่ต้อง setup เสมอ (คูณจำนวนเครื่องใหม่)
            setup_days_used = item_setup_days * new_mc if new_mc > 0 else 0
            if _s9_active:
                setup_days_used = 0  # จ้างทอไม่หัก setup
            # available_days สำหรับ log (ใช้เครื่องใหม่เป็นหลักถ้ามี)
            available_days = prod_days_new if new_mc > 0 else prod_days_old

            # === Same-week remaining cap: item+machine เดียวกัน ต่อจาก FG ก่อนหน้า ===
            # ใช้ remaining_week_cap ตาม item+machine ในสัปดาห์เดียวกัน (ไม่ผูก SC)
            # S9: track cap แยกจาก normal โดยใช้ S9_KEY เป็นถังเดียวของงานจ้างทอทั้งหมด
            _same_week_key = (plan_week, item, S9_KEY if _s9_active else mc_group)
            _same_week_rem_cap = remaining_week_cap.get(_same_week_key, None)
            _same_week_total_cap = None
            # 🔧 FIX: remaining_week_cap override logic
            # - ถ้า remaining มาจาก new plan (owner != None): remaining คำนวณถูกต้องจาก production จริง
            #   ใน week เดียวกัน → ต้องเคารพ (ห้าม override) เพราะเครื่องที่ setup ไปแล้ว
            #   ไม่มี productive days เหลือใน week นั้น
            # - ถ้า remaining มาจาก booking (owner = None): booking production เกิดขึ้นจริงแล้ว
            #   ต้องเคารพ remaining ที่เหลือเช่นกัน
            _rem_cap_owner = remaining_week_cap_owner.get(_same_week_key, None)
            _rem_from_new_plan = _rem_cap_owner is not None  # owner=None → จาก booking seed
            # เครื่องของสัปดาห์นี้ถูกหักพูลไปแล้วหรือยัง: ที่เหลือจากแถวแผน (FG ก่อนหน้า) = หักแล้ว ห้ามซ้ำ ;
            # ที่เหลือจากเครื่อง booking/KP = ยังไม่ได้หักเครื่องของแผน → ต้องหัก (carry+new − booking) ตามปกติ
            # cap ที่มาจาก Hold รอ target: หักพูลไปแล้วเฉพาะเครื่องที่กันไว้ ไม่ใช่ทั้งแถว → ยังต้องหักพูล
            # ตามปกติ แต่ลบเครื่องชุดที่กันไว้ออก (ดู _pool_charge_mc) ไม่งั้นเครื่องที่ตั้งใหม่ทับ Hold ไม่ถูกหัก
            _rem_hold_mc = (
                int(_HOLD_CAP_MC.get(_same_week_key, 0) or 0) if _same_week_rem_cap is not None else 0
            )
            _rem_mc_counted = (
                _same_week_rem_cap is not None and _rem_from_new_plan and _rem_hold_mc <= 0
            )
            # คำนวณ PRODUCE_QTY ตามสูตรที่แม่นยำ
            if _same_week_rem_cap is not None:
                # ผลิตด้วย cap ที่เหลือจาก FG ก่อนหน้า + cap incremental จากเครื่องใหม่ในสัปดาห์เดียวกัน
                _same_week_base_cap = max(0.0, float(_same_week_rem_cap))
                # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → หัก 1 วัน × เครื่อง carry × daily_cap จาก remaining cap
                if _yd_sub_color_setup > 0 and carryover_mc > 0 and daily_capacity:
                    _color_cap_deduct = carryover_mc * 1 * daily_capacity  # หัก 1 วันเต็ม ไม่ round ลง rev_weight
                    _same_week_base_cap = max(0.0, _same_week_base_cap - _color_cap_deduct)
                    _total_cs = _yd_week_color_setups.get((mc_key, plan_week), 0)
                    print(f"[YD SUB_COLOR SAME-WEEK] {item} W{plan_week}: หัก {_color_cap_deduct:.0f} units จาก remaining cap (เปลี่ยนสีครั้งที่ {_total_cs}, remaining={_same_week_base_cap:.0f})")
                _same_week_new_cap = max(0.0, float(daily_capacity * prod_days_new * new_mc))
                # FIX: ห้ามเพิ่ม carryover capacity เมื่อมี remaining_week_cap
                # เพราะ remaining_week_cap คำนวณจากวันทำงานของสัปดาห์ปัจจุบันแล้ว
                # การเพิ่ม carryover capacity จะทำให้นับซ้ำ (double counting)
                _same_week_carry_cap = 0.0
                # ไม่เพิ่ม carry capacity เมื่อมี remaining cap อยู่แล้ว
                _same_week_total_cap = _same_week_base_cap + _same_week_new_cap + _same_week_carry_cap
                # cap_old/cap_new ถูกใช้ต่อด้านล่าง (เพดาน clamp ยอด + remaining_week_cap ที่ SC อื่น
                # เอาไป carry ต่อ) แต่เดิมสาขานี้ไม่ได้ตั้งค่า → ไปหยิบค่าค้างของแถวก่อนหน้ามาใช้
                # ตั้งให้ผลรวมเท่ากับกำลังผลิตจริงของสัปดาห์นี้ (carry + ที่เหลือจาก SC ก่อน + เครื่องใหม่)
                cap_old = _same_week_carry_cap
                cap_new = _same_week_base_cap + _same_week_new_cap

                if rev_weight and rev_weight > 0:
                    _swt_mc = max(1, int(available_machines or 1))
                    produce = min(
                        qty_left,
                        fold_round(
                            _same_week_total_cap, rev_weight, mc_group,
                            per_mc_cap=_same_week_total_cap / _swt_mc, mc_count=_swt_mc,
                        ),
                    )
                else:
                    produce = min(qty_left, _same_week_total_cap)
            elif rev_weight is not None and rev_weight > 0:
                cap_old = daily_capacity * prod_days_old * carryover_mc
                cap_new = daily_capacity * prod_days_new * new_mc
                produce = min(
                    qty_left,
                    fold_round_mixed(
                        cap_old + cap_new, rev_weight, mc_group,
                        daily_capacity * prod_days_old, carryover_mc,
                        daily_capacity * prod_days_new, new_mc,
                    ),
                )
            else:
                cap_old = daily_capacity * prod_days_old * carryover_mc
                cap_new = daily_capacity * prod_days_new * new_mc
                produce = min(qty_left, cap_old + cap_new)

            # Hard clamp: ห้ามยอดรวม item+week+mc_group เกิน capacity ของสัปดาห์นี้
            # ป้องกันกรณี merge ข้าม FG แล้วถูกบวก produce ซ้ำจนเกินเพดาน
            _already_planned_qty = 0.0
            for _p in plans:
                if (
                    _p.get("ITEM_CODE") == item
                    and _p.get("PLAN_WEEK") == plan_week
                    # แถวจ้างทอมี MC_GROUP เป็นชื่อผู้รับจ้าง (คนละชื่อต่อเจ้า) เทียบชื่อตรง ๆ ไม่ได้
                    # ต้องนับ "ทุกแถวจ้างทอของ item+สัปดาห์นี้" รวมกันเป็นก้อนเดียว
                    and (_is_outsource_row(_p) if _s9_active else _p.get("MC_GROUP") == mc_group)
                ):
                    _already_planned_qty += float(_p.get("PRODUCE_QTY", 0) or 0)

            if _same_week_rem_cap is not None:
                _max_additional_qty = max(0.0, float(_same_week_total_cap or 0.0))
            else:
                # 🔧 FIX: ตรวจสอบว่า booking ในสัปดาห์เดียวกันมีการ setup เพิ่มเครื่องหรือไม่
                # ถ้ามี → true_week_cap ต้องหัก setup days จากเครื่องใหม่ใน booking ออกก่อน
                # ป้องกัน new order ผลิตเกินกว่าที่เครื่องสามารถทำได้จริงใน week ที่ old booking ต้อง setup
                _bk_cur_w_idx = week_index(plan_week)
                _bk_mc_cur = booking_mc_by_week.get(mc_key, {}).get(_bk_cur_w_idx, 0)
                # เครื่อง booking ที่ยังอุ่น = booking สัปดาห์ล่าสุดที่เว้นไม่เกิน SETUP_GAP_WEEK
                # (เดิมดูแค่สัปดาห์ก่อนติดกัน → KP W41 แล้ว W43 ถูกนับเป็น setup ใหม่)
                _bk_mc_prev = 0
                if _bk_cur_w_idx is not None:
                    for _bk_back in range(1, int(SETUP_GAP_WEEK) + 1):
                        _bk_v = booking_mc_by_week.get(mc_key, {}).get(_bk_cur_w_idx - _bk_back, 0)
                        if _bk_v:
                            _bk_mc_prev = _bk_v
                            break
                _bk_new_mc_cnt = max(0, _bk_mc_cur - _bk_mc_prev)   # เครื่องใหม่ที่ setup ใน booking week นี้
                _bk_carry_mc_cnt = _bk_mc_cur - _bk_new_mc_cnt        # เครื่องที่ carry-over จาก booking week ก่อนหน้า
                if _bk_new_mc_cnt > 0 and daily_capacity is not None and daily_capacity > 0:
                    # คำนวณ true_week_cap โดยหัก setup days จากเครื่องใหม่ใน old booking
                    _true_week_cap = (
                        _bk_carry_mc_cnt * actual_working_days
                        + _bk_new_mc_cnt * max(0, actual_working_days - item_setup_days)
                    ) * daily_capacity
                    _week_capacity = max(0.0, _true_week_cap)
                    print(
                        f"[OLD SETUP CLAMP] {item} W{plan_week} {mc_group}: "
                        f"bk_new_mc={_bk_new_mc_cnt:.0f} bk_carry_mc={_bk_carry_mc_cnt:.0f} "
                        f"→ true_cap={_week_capacity:.2f} (naive={float(cap_old + cap_new):.2f})"
                    )
                else:
                    _week_capacity = max(0.0, float(cap_old + cap_new))
                _max_additional_qty = max(0.0, _week_capacity - _already_planned_qty)
            if produce > _max_additional_qty:
                print(
                    f"[CAP CLAMP] {item} W{plan_week} {mc_group}: "
                    f"produce {produce:.2f} -> {_max_additional_qty:.2f} "
                    f"(already={_already_planned_qty:.2f})"
                )
                _clamped_qty = max(0.0, float(_max_additional_qty))
                if rev_weight and rev_weight > 0:
                    _clamp_mc = max(1, int(available_machines or 1))
                    _rounded_cap = fold_round(
                        _clamped_qty, rev_weight, mc_group,
                        per_mc_cap=_clamped_qty / _clamp_mc, mc_count=_clamp_mc,
                    )
                    if _rounded_cap > 0:
                        _clamped_qty = _rounded_cap
                    elif _clamped_qty < rev_weight:
                        _clamped_qty = 0
                produce = _clamped_qty

            # ไม่เพิ่มแถวถ้าไม่มีการผลิต — แต่ถ้า setup ไปแล้ว (new_mc>0) ให้บันทึกว่าเครื่อง setup เสร็จ
            # เพื่อให้ week ถัดไปนับเป็น carryover (ไม่ต้อง setup ซ้ำ) → ผลิตได้เต็มจำนวนวัน
            if produce <= 0:
                if _s9_active:
                    print(f"[S9 FULL] {item} W{plan_week} MC={mc_group}: S9 pool เต็ม (produce=0) → next week")
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break
                    continue
                if new_mc > 0:
                    _plan_ck_setup = _resolve_carry_key(item, mc_group, _sel_gauge)
                    machines_in_use[_plan_ck_setup] = available_machines
                    last_sc_machines[(item, mc_group, _sel_gauge, sc_so_no)] = available_machines
                    last_sc_week[(item, mc_group, _sel_gauge, sc_so_no)] = week_index(plan_week)
                    # 🔧 FIX: SETUP ONLY (ไม่มีการผลิต) ควรบันทึก last_production
                    # เพื่อให้ week ถัดไปรู้ว่าเครื่อง setup แล้ว (ไม่ต้อง setup ซ้ำ)
                    # แต่ต้องบันทึกเฉพาะกรณีที่ week นี้ >= week ที่บันทึกไว้
                    _cur_lp = last_production.get(_plan_ck_setup)
                    if _cur_lp is None or week_index(plan_week) >= _cur_lp:
                        last_production[_plan_ck_setup] = week_index(plan_week)
                    new_plan_started_items.add(_plan_ck_setup)
                    # บันทึก job usage สำหรับ week นี้ (เครื่อง setup แล้วแม้ยังไม่ผลิต)
                    if plan_week not in weekly_job_usage:
                        weekly_job_usage[plan_week] = {}
                    weekly_job_usage[plan_week][mc_group] = (
                        weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
                    )
                    # ช่างตั้งเครื่องให้ผ้านี้ไปแล้ว → กินโควตารายผ้าของสัปดาห์นี้ด้วย
                    # (เดิมไม่หัก → ใบอื่นของผ้าเดียวกันตั้งเพิ่มได้อีกเต็มโควตา = เกิน 2 ตัว/สัปดาห์)
                    _item_setup_charge(item, mc_group, _sel_gauge, plan_week, new_mc)
                    _week_mc_key_setup = (week_index(plan_week), mc_group)
                    weekly_mc_usage[_week_mc_key_setup] = weekly_mc_usage.get(_week_mc_key_setup, 0) + available_machines
                    last_sc_so_no[_plan_ck_setup] = sc_so_no
                    if order_type == "YD-ORDERS" and sub_color:
                        last_sub_color[_plan_ck_setup] = sub_color
                    # 🔧 FIX: บันทึก SETUP ONLY ลง _existing_item_week_mc
                    # เพื่อป้องกัน SETUP EARLY PRIORITY ดึงกลับมา week เดิมซ้ำ
                    # ซึ่งทำให้ double-count วันทำงาน (setup กิน 5 วัน แล้วมาผลิตอีก 5 วัน)
                    _existing_item_week_mc.add((item, plan_week, S9_KEY if _s9_active else mc_group))
                    print(f"[SETUP ONLY] {item} W{plan_week} MC={mc_group}: setup {new_mc} machines (0 produce, setup ate all {actual_working_days} working days)")

                plan_week = next_week(plan_week)
                if plan_week is None:
                    break
                continue

            # จำนวนเครื่องที่วางแผนไว้ (จาก calculate_required_machines)
            # แสดงจำนวนเครื่องจริงตาม required_mc หรือ available_machines
            planned_mc = (
                required_machines_info[2]
                if required_machines_info
                else available_machines
            )
            # ===== งานสี: คืนเครื่องที่ยึดเกินก่อนเขียนแถว (ดู _grab_txn_settle) =====
            # เรียกทั้งทาง MERGE และทางเพิ่มแถวใหม่ — ณ จุดนั้นจำนวนเครื่องของแถวถูกตัดสินครบแล้ว
            def _settle_grab():
                _live = not _s9_active
                # เครื่องที่แถวนี้หักจากพูล — สูตรเดียวกับตอนบันทึกแผน (weekly_new_plan_usage)
                _use = (
                    _pool_charge_mc(available_machines, _booking_week_mc, new_mc, _rem_hold_mc)
                    if (_live and not _rem_mc_counted and not _is_fg_split)
                    else 0
                )
                _span = _job = None
                if _live and new_mc > 0 and not rts_local_force:
                    def _span():
                        return _carry_span_blocked(carryover_mc, new_mc)

                    def _job():
                        _tot = check_job_capacity_limit(
                            mc_group, carryover_mc + new_mc, False,
                            get_type_used_jobs(plan_week, mc_group),
                            committed_carryover=carryover_mc,
                        )
                        return max(0, new_mc - max(0, int(_tot) - carryover_mc))
                # งานยังไม่จบ (ใบนี้หรือยอดถัดไปของ item) → สัปดาห์หน้าต้องมีที่ยืนให้เครื่องชุดนี้ carry ต่อ
                _next = None
                _nw = next_week(plan_week)
                _more = (
                    max(0.0, float(qty_left or 0) - float(produce or 0)) > float(rev_weight or 0)
                    or max(0.0, float(_qty_for_machine_calc or 0) - float(_initial_qty_for_mc_calc or 0)) > 1.0
                )
                if _live and _nw is not None and _more:
                    _nwi_bk = week_index(_nw)
                    _bk_next = int(
                        booking_mc_by_week.get(_ck(item, mc_group, _sel_gauge), {}).get(_nwi_bk, 0) or 0
                    ) if _nwi_bk is not None else 0
                    _next = (_nw, max(0, int(available_machines or 0) - _bk_next))
                _grab_txn_settle(
                    item, mc_group, _sel_gauge, plan_week, _use, span_fn=_span, job_fn=_job,
                    next_room=_next,
                )

            # 🔧 FIX: ตรวจสอบว่า Item+Week+MC_GROUP นี้มีการ plan ไปแล้วหรือไม่
            # เพื่อป้องกันการซ้ำซ้อนของ orders ที่มี Item+MC_GROUP เดียวกันใน week เดียวกัน
            # โดยไม่สนใจว่าจะเป็น SC หรือ TARGET_KNIT อะไร (เพราะอาจมีการ merge SC)
            # S9: ใช้ S9_KEY เป็น key เพื่อไม่ merge เข้าแถวเครื่องในบ้าน
            _current_item_week_mc = (item, plan_week, S9_KEY if _s9_active else mc_group)
            _is_fg_split = False  # Flag: สร้าง row ใหม่สำหรับ FG_WEEK ที่ต่างกัน (ไม่ merge)

            # 🔧 FIX: Skip weeks where OLD bookings already exist for same item/MC/Gauge
            # to prevent exceeding capacity that old bookings already use
            _ck_key = _ck(item, mc_group, _sel_gauge)
            _pw_idx = week_index(plan_week)
            if not _s9_active and _ck_key and _pw_idx is not None and _ck_key in booking_mc_by_week:
                if booking_mc_by_week[_ck_key].get(_pw_idx, 0) > 0:
                    # ถ้า cylinder change เพิ่งทำสำหรับ item นี้ใน plan_week นี้ → ไม่ skip
                    # เพราะ cylinder เพิ่มเครื่องใหม่ที่ไม่ใช่ old booking เดิม
                    _cyl_done_key = (_TL.shift(plan_week, -1), item.strip().upper(), mc_group.strip().upper() if mc_group else "")
                    # ตรวจว่า new plan มี carry machine จาก week ก่อนหน้าติดกัน (gap=1)
                    # ถ้าใช่ → ไม่ skip เพราะเครื่องนั้นเป็นคนละชุดกับ old booking (setup ต่างรอบ)
                    # เช่น W23 setup ใหม่ 1 เครื่อง → W24 carry ต่อ แม้ old booking W24 เป็น SETUP (N เครื่อง)
                    _carry_ck_skip = _resolve_carry_key(item, mc_group, _sel_gauge)
                    _prev_lp_skip = last_production.get(_carry_ck_skip)
                    _is_new_plan_carry = (
                        _carry_ck_skip in new_plan_started_items
                        and _prev_lp_skip is not None
                        and _pw_idx - _prev_lp_skip == 1
                        and machines_in_use.get(_carry_ck_skip, 0) > 0
                    )
                    # เครื่อง booking/KP ยังเหลือที่ (seed ไว้ใน remaining_week_cap ตอนต้นใบ) → เติมผ้าลงเครื่องนั้นได้
                    # ยอดสัปดาห์นี้คิดจากที่เหลือ + เครื่องใหม่ (สาขา _same_week_rem_cap ข้างบน) ไม่นับกำลัง
                    # เครื่อง booking ซ้ำ → ไม่ต้องข้ามทั้งสัปดาห์ (user 29 ก.ย. 2569: "เครื่องที่ KP แล้ว
                    # ยังไม่ครบ เติมผ้าเข้าไปได้")
                    _bk_topup = (
                        _same_week_rem_cap is not None
                        and float(_same_week_rem_cap) > 0
                        and not _rem_from_new_plan
                        and produce > 0
                    )
                    if _bk_topup:
                        print(f"[BOOKING TOP-UP] {item}+{plan_week}+{mc_group}: เครื่อง booking {booking_mc_by_week[_ck_key].get(_pw_idx, 0)} ตัวยังเหลือ {float(_same_week_rem_cap):.2f} → เติมลงเครื่องเดิม (ไม่ข้ามสัปดาห์)")
                    elif _cyl_done_key not in _cylinder_change_for_item and not _is_new_plan_carry:
                        print(f"[SKIP OLD BOOKING IN MAIN LOOP] {item}+{plan_week}+{mc_group}: OLD booking using {booking_mc_by_week[_ck_key].get(_pw_idx, 0)} machines, skip to next week")
                        plan_week = next_week(plan_week)
                        if plan_week is None:
                            break
                        continue
                    if _is_new_plan_carry:
                        # รวม carry จาก new plan (W23) + old booking (W24) เป็น machines_in_use เดียวกัน
                        # เพื่อให้ planning loop เห็น cap รวม = N+1 เครื่อง แทนที่จะเห็นแค่ 1
                        # ⚠️ ต้องหักเครื่อง booking ของสัปดาห์ก่อนออกจาก carry ก่อน มิฉะนั้นเครื่อง booking
                        # ตัวเดิมจะถูกบวกซ้ำทุกสัปดาห์ (เครื่องโตขึ้นเรื่อยๆ ทั้งที่เป็นเครื่องตัวเดียวกัน)
                        _new_carry_mc = machines_in_use.get(_carry_ck_skip, 0)
                        _combined_carry, _skip_own_mc, _skip_bk_prev, _old_bk_mc_here = _combine_carry_with_booking(
                            _ck_key, _new_carry_mc, _prev_lp_skip, _pw_idx
                        )
                        if _old_bk_mc_here > 0:
                            machines_in_use[_carry_ck_skip] = _combined_carry
                        print(f"[CARRY THROUGH OLD BOOKING] {item}+{plan_week}+{mc_group}: plan-own={_skip_own_mc} (carry {_new_carry_mc} − booking W ก่อน {_skip_bk_prev}) + old booking W{plan_week}={_old_bk_mc_here} → combined={_combined_carry} mc")
            # YD-ORDERS: ห้าม merge — แต่ละ SO ต้องแยกเป็น row ของตัวเอง
            if order_type == "YD-ORDERS":
                _is_fg_split = True  # บังคับสร้าง row ใหม่เสมอ
            elif _current_item_week_mc in _existing_item_week_mc:
                # Merge ลงแถวเดิมของ Item+Week+MC_GROUP เฉพาะ FG_WEEK + SC เดียวกันเท่านั้น
                # ⚠️ ห้าม merge ข้าม SC — มิฉะนั้น production ของ SC อื่นจะถูกนับรวม
                # ทำให้ PRODUCE_QTY ของแถวเกิน ORDER_QTY ของ SC นั้น
                _merged = False
                if _s9_active:
                    # งานจ้างทอถูกแตกเป็นหลายแถว (แถวละผู้รับจ้าง) — เทียบด้วยชื่อกลุ่มไม่ได้
                    # และจะยัดยอดลงแถวเดียวไม่ได้ ไม่งั้นยอดกับเครื่องของเจ้านั้นไม่สัมพันธ์กัน
                    # → เกลี่ยยอดที่เพิ่มตามสัดส่วนเครื่องของแต่ละเจ้า (แถวท้ายรับเศษ)
                    _s9_hits = [
                        _p for _p in plans
                        if _p.get("ITEM_CODE") == item
                        and _p.get("PLAN_WEEK") == plan_week
                        and _p.get("FG_WEEK") == fg_week
                        and _p.get("SC_SO_NO") == sc_so_no
                        and _is_outsource_row(_p)
                        # แถว HOLD มี FG_WEEK/SC ของใบแล้ว (21/9/2569) — ห้ามถูกเติมยอดผลิต
                        and not _is_hold_row(_p)
                    ]
                    _s9_hit_mc = sum(float(_p.get("ACTUAL_MC", 0) or 0) for _p in _s9_hits)
                    if _s9_hits and _s9_hit_mc > 0:
                        _add_left = float(produce)
                        for _i, _p in enumerate(_s9_hits):
                            _add = _add_left if _i == len(_s9_hits) - 1 else round(
                                float(produce) * float(_p.get("ACTUAL_MC", 0) or 0) / _s9_hit_mc, 2)
                            _add_left = round(_add_left - _add, 2)
                            _p["PRODUCE_QTY"] = float(_p.get("PRODUCE_QTY", 0) or 0) + _add
                            _p["PLAN_QTY"] = max(0, float(qty_left - produce))
                        _merged = True
                else:
                    for _p in plans:
                        if (
                            _p.get("ITEM_CODE") == item
                            and _p.get("PLAN_WEEK") == plan_week
                            and _p.get("MC_GROUP") == mc_group
                            and _p.get("FG_WEEK") == fg_week
                            and _p.get("SC_SO_NO") == sc_so_no
                            # แถว HOLD ยกข้อมูลใบมาครบแล้ว (21/9/2569) เลยตรงคีย์นี้ได้ — เดิมไม่เคย
                            # ตรงเพราะ FG_WEEK ว่าง ; ห้ามยัดยอดผลิตลงแถวกันเครื่อง (ตรึงพฤติกรรมเดิม)
                            and not _is_hold_row(_p)
                        ):
                            _p["PRODUCE_QTY"] = float(_p.get("PRODUCE_QTY", 0) or 0) + float(produce)
                            _p["PLAN_QTY"] = max(0, float(qty_left - produce))
                            _merged = True
                            break

                if _merged:
                    _settle_grab()
                    print(
                        f"[DEBUG DUPLICATE] MERGED: {item}+{plan_week}+{mc_group} FG={fg_week} +{produce:.1f}"
                    )


                    qty_left -= produce
                    if qty_left <= 0:
                        qty_left = 0

                    # บันทึก week ที่ผลิตจริงก่อนเปลี่ยน plan_week
                    _produced_week = plan_week

                    # อัปเดต remaining cap แบบเดียวกับเส้นทาง append ใหม่
                    if _same_week_rem_cap is not None:
                        _new_rem = max(0, float(_same_week_total_cap or 0.0) - produce)
                        if _new_rem > 0:
                            remaining_week_cap[(_produced_week, item, mc_group)] = _new_rem
                            remaining_week_cap_owner[(_produced_week, item, mc_group)] = sc_so_no
                        else:
                            remaining_week_cap.pop((_produced_week, item, mc_group), None)
                            remaining_week_cap_owner.pop((_produced_week, item, mc_group), None)
                    else:
                        _full_week_cap = cap_old + cap_new
                        _rem = max(0, _full_week_cap - produce)
                        remaining_week_cap[(_produced_week, item, mc_group)] = _rem
                        remaining_week_cap_owner[(_produced_week, item, mc_group)] = sc_so_no

                    _plan_ck_merge = _resolve_carry_key(item, mc_group, _sel_gauge)
                    last_production[_plan_ck_merge] = week_index(plan_week)
                    machines_in_use[_plan_ck_merge] = available_machines
                    last_sc_machines[(item, mc_group, _sel_gauge, sc_so_no)] = available_machines
                    last_sc_week[(item, mc_group, _sel_gauge, sc_so_no)] = week_index(plan_week)
                    # FIX: ถ้า MERGE iteration นำเครื่องใหม่เข้ามา (new_mc > 0)
                    # ต้องอัปเดต row + tracking ให้สะท้อนจำนวนเครื่องจริง
                    # เช่น cross-SC fill สร้าง row carry=2,new=0 → MERGE เพิ่ม new=2 →
                    # row ต้องแสดง actual=4 เพื่อไม่ให้ overcapacity ในผลลัพธ์
                    _existing_actual_mc = int(_p.get("ACTUAL_MC", 0) or 0)
                    _existing_new_mc = int(_p.get("NEW_MC", 0) or 0)
                    _existing_carry_mc = int(_p.get("CARRYOVER_MC", 0) or 0)
                    _existing_shared_mc = int(_p.get("MC_SHARED", 0) or 0)
                    if new_mc > 0 and available_machines > _existing_actual_mc:
                        # ⚠️ ACTUAL_MC = เครื่องที่ "แถวนี้ประกาศเอง" (carry ของแถว + เครื่องใหม่) เท่านั้น
                        # ห้ามใช้ available_machines ซึ่งรวมเครื่องของใบสั่งอื่นที่แถวนี้ไปเกาะอยู่ (MC_SHARED)
                        # เดิมเขียนทับด้วย available_machines → Gantt/บัญชีพูลนับเครื่องชุดเดียวกันซ้ำ
                        # (เคสจริง 30 ก.ย. 2569 FD5DU34B0 RAO G19 W50: 3 ใบเขียน 2+6+3 = 11 เครื่อง
                        #  ทั้งที่ตั้งจริงใบละ 2 และพูลทั้งก้อนมีแค่ 11 ตัว)
                        _merged_new = _existing_new_mc + new_mc
                        _merged_actual = _existing_carry_mc + _merged_new
                        _p["ACTUAL_MC"] = _merged_actual
                        _p["NEW_MC"] = _merged_new
                        _p["REQUIRED_MC"] = max(int(_p.get("REQUIRED_MC", 0) or 0), _merged_actual)
                        # สะสม SETUP_DAYS ตาม NEW_MC ที่เพิ่มเข้ามา
                        _p["SETUP_DAYS"] = int(_p.get("SETUP_DAYS", 0) or 0) + setup_days_used
                        _p["AVAILABLE_DAYS"] = available_days
                        # เครื่อง/วัน setup ของแถวเพิ่งเปลี่ยน → หมายเหตุต้องตามทุกแถว (ไม่ใช่เฉพาะแถวที่
                        # ใช้เครื่องร่วม) และต้อง "สร้างใหม่" ไม่ใช่ต่อท้าย — แถวเดียวโดน merge ได้หลายรอบ
                        # (ดู _merge_row_remark) ; ไม่งั้นแถวเขียน "ไม่ต้อง setup" ทั้งที่มี SETUP_DAYS
                        _p["REMARK"] = _merge_row_remark(_p)
                        # ⚠️ ห้ามหักโควตา job / นับเครื่องที่นี่ — ทางนี้เดินต่อไปถึงจุดหักรวม
                        # ข้างล่าง (weekly_job_usage / weekly_mc_usage) ทุกครั้ง ถ้าหักตรงนี้ด้วย
                        # จะนับซ้ำ 2 เท่า (โควตาช่างถูกกินเกินจริง + ชีท REMAINING_JOBS เพี้ยน)
                        print(
                            f"[MERGE MC UPDATE] {item} W{plan_week}: "
                            f"actual {_existing_actual_mc}→{_merged_actual} "
                            f"(carry={_p.get('CARRYOVER_MC')}, new={_merged_new})"
                        )
                    # FIX: ถ้า new_mc = 0 (ใช้ remaining cap) ไม่ต้องอัปเดต machines_in_use
                    # เพราะถูกตั้งค่าไว้อย่างถูกต้องแล้วที่ line 5492 = available_machines
                    # ห้าม overwrite ด้วย _existing_actual_mc เพราะจะทำให้ week ถัดไปคิดว่าไม่มี carryover
                    else:
                        # machines_in_use already set correctly at line 5492
                        # Only update SC-specific tracking if needed
                        if _existing_actual_mc > 0:
                            last_sc_machines[(item, mc_group, _sel_gauge, sc_so_no)] = _existing_actual_mc
                        last_sc_week[(item, mc_group, _sel_gauge, sc_so_no)] = week_index(plan_week)
                    # FIX: ถ้า available_machines > 0 แต่ _existing_actual_mc = 0
                    # หมายถึง MERGE ครั้งแรกที่ใช้ remaining cap จาก booking → ต้องบันทึก available_machines
                    if available_machines > 0 and _existing_actual_mc == 0:
                        machines_in_use[_plan_ck_merge] = available_machines
                        last_sc_machines[(item, mc_group, _sel_gauge, sc_so_no)] = available_machines
                        last_sc_week[(item, mc_group, _sel_gauge, sc_so_no)] = week_index(plan_week)
                        print(
                            f"[MERGE FIRST TIME] {item} W{plan_week}: "
                            f"set machines_in_use={available_machines} (from booking remaining cap)"
                        )
                    last_sc_so_no[_plan_ck_merge] = sc_so_no  # บันทึก SC/SO NO ล่าสุดที่ผลิต
                    if order_type == "YD-ORDERS" and sub_color:
                        last_sub_color[_plan_ck_merge] = sub_color
                    new_plan_started_items.add(_plan_ck_merge)
                    # บันทึก total machines per (week, mc_group) สำหรับ gradual increase
                    # 🔧 FG SPLIT: ถ้าเป็น FG split row → เครื่องนับไปแล้วจาก FG แรก ห้ามนับซ้ำ
                    # งานจ้างทอ (S9) ไม่ได้ใช้เครื่องในบ้าน — mc_group ตอนนี้ยังเป็นพูลผู้รับจ้าง (เช่น RAO)
                    # ถ้านับเข้าตัวนับในบ้านจะหักพูลเราซ้ำ (มี _s9_weekly_usage นับแยกอยู่แล้ว)
                    if not _is_fg_split and not _s9_active:
                        _week_mc_key = (week_index(plan_week), mc_group)
                        # ⚠️ MERGE = ยัดยอดลงแถวที่มีอยู่แล้วของสัปดาห์นี้ → เครื่องที่แถวนั้นประกาศ
                        # (ACTUAL_MC) กับเครื่องที่ไปเกาะของใบอื่น (MC_SHARED) ถูกนับตอนวางรอบก่อน
                        # ไปแล้ว บวกได้เฉพาะ "เครื่องที่เพิ่มขึ้นจริงจากการ merge" (ปกติ = new_mc)
                        # เดิมบวก available_machines ทั้งก้อน (และบวก new_mc อีกรอบข้างบน) = นับซ้ำ
                        # 2 ชั้น → ตัวคุมการค่อยเพิ่มเครื่อง (gradual increase) เห็นเครื่องบวมกว่าจริง
                        _merge_mc_added = max(
                            0, available_machines - _existing_actual_mc - _existing_shared_mc
                        )
                        weekly_mc_usage[_week_mc_key] = weekly_mc_usage.get(_week_mc_key, 0) + _merge_mc_added
                    # อัพเดท job usage สำหรับสัปดาห์นี้ (นับเฉพาะ new_mc = machines ที่ setup ใหม่)
                    # เครื่อง carry ต่อ (carryover_mc) ไม่ต้องนับเป็น job ใหม่เพราะไม่ต้อง setup
                    # ⚠️ ต้องหักนอกเงื่อนไข _is_fg_split — FG split / YD-ORDERS ก็ตั้งเครื่องใหม่จริง
                    # (SETUP_TRACKING นับทุกแถวที่ NEW_MC>0 ถ้าไม่หักตรงนี้ job/week cap จะรั่ว → แผนเกินโควตา)
                    # ⚠️ ต้อง setdefault เสมอ (แม้ new_mc=0) — โค้ดตรวจ overload ท้ายลูปอ่าน
                    # weekly_job_usage[_produced_week] ตรงๆ ถ้าไม่มี key จะ KeyError
                    weekly_job_usage.setdefault(plan_week, {})
                    if new_mc > 0 and not _s9_active:
                        weekly_job_usage[plan_week][mc_group] = (
                            weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
                        )
                        # หักโควตาตั้งเครื่องใหม่ของ "ผ้านี้" ในสัปดาห์นี้ — จุดเดียวกับที่หัก
                        # weekly_job_usage (ทางนี้คือทาง MERGE ; ทางวางแถวใหม่หักที่ปลายลูป)
                        # ต้องอยู่ใต้ not _s9_active: งานจ้างทอใช้ช่างของผู้รับจ้าง ไม่กินโควตาช่างเรา
                        _item_setup_charge(item, mc_group, _sel_gauge, plan_week, new_mc)
                    # อัพเดต new plan usage (นับทั้ง carryover+new สำหรับ get_actual_mc_remain)
                    # key ต้องเป็น (mc_group, gauge_str) เสมอ — ห้าม pool ข้าม GUAGE
                    # ถ้าเป็น same-week continuation หรือ FG split → เครื่องนับไปแล้วจาก FG ก่อนหน้า ห้ามนับซ้ำ
                    if not _rem_mc_counted and not _is_fg_split and not _s9_active:
                        if plan_week not in weekly_new_plan_usage:
                            weekly_new_plan_usage[plan_week] = {}
                        _wpu_cap = item_cap_data[
                            (item_cap_data["ITEM_CODE"] == item)
                            & (item_cap_data["MC_GROUP"] == mc_group)
                        ]
                        _wpu_gauge_raw = _wpu_cap.iloc[0]["GUAGE"] if not _wpu_cap.empty else None
                        _wpu_gauge_str = (
                            str(_wpu_gauge_raw).strip()
                            if _wpu_gauge_raw is not None
                            and not (isinstance(_wpu_gauge_raw, float) and pd.isna(_wpu_gauge_raw))
                            else ""
                        )
                        # ถ้า mc_group+gauge อยู่ใน MC_GROUP_REDIRECT → หักเครื่องจาก target แทน
                        # เช่น SKP 20 → หักจาก FA 20 เสมอ
                        _wpu_mc_r, _wpu_gauge_r = _apply_mc_redirect(mc_group, _wpu_gauge_str)
                        _wpu_key = (_mc_to_type1(_wpu_mc_r, _wpu_gauge_r), _wpu_gauge_r)
                        _wpu_added = _pool_charge_mc(
                            available_machines, _booking_week_mc, new_mc, _rem_hold_mc
                        )
                        # MC Special: item ที่มี sub-pool (COTTON/POLY) กินเฉพาะ sub-pool ของตัวเอง
                        # ไม่ไปกินพูลทั่วไปด้วย (ไม่งั้นหักซ้ำ 2 พูล)
                        _wpu_sp_type = _get_subgroup_by_item_prefix(mc_group, _wpu_gauge_str, item)
                        if _wpu_sp_type:
                            if _wpu_added > 0:
                                _wpu_sp_f = _mc_to_factory(str(mc_group).strip().upper(), _wpu_gauge_str)
                                _wpu_sp_cat = _mc_to_type1(str(mc_group).strip().upper(), _wpu_gauge_str)
                                _wpu_sp_key = (_wpu_sp_f, _wpu_sp_cat, _wpu_gauge_r, plan_week, _wpu_sp_type)
                                _mc_special_weekly_usage[_wpu_sp_key] = _mc_special_weekly_usage.get(_wpu_sp_key, 0) + _wpu_added
                        else:
                            weekly_new_plan_usage[plan_week][_wpu_key] = (
                                weekly_new_plan_usage[plan_week].get(_wpu_key, 0) + _wpu_added
                            )
                            # พูลแยก (SKP/SKPTA·SKPLE): นับต่อ pool label ให้หักถูกพูล
                            _wpu_pool = _mc_to_pool_label(_wpu_mc_r, _wpu_gauge_r)
                            if _wpu_pool:
                                weekly_new_plan_pool_usage.setdefault(plan_week, {})[_wpu_pool] = (
                                    weekly_new_plan_pool_usage.setdefault(plan_week, {}).get(_wpu_pool, 0) + _wpu_added
                                )
                        # TYPE_SPECIAL quota tracking
                        if _wpu_added > 0 and _TYPE_DESC_RULES_PLAN:
                            _ts_mc_u3 = str(mc_group).strip().upper()
                            _ts_fac3  = _mc_to_factory(_ts_mc_u3, _wpu_gauge_str)
                            _ts_typ3  = _mc_to_type_raw_plan.get((_ts_mc_u3, _wpu_gauge_str), "").strip().upper()
                            _ts_rk3   = (_ts_fac3.upper(), _ts_typ3)
                            if _ts_rk3 in _TYPE_DESC_RULES_PLAN:
                                _ts_rule3  = _TYPE_DESC_RULES_PLAN[_ts_rk3]
                                _ts_mcat3  = _ts_rule3.get("mc_cat", "")
                                _ts_t13    = _mc_to_type1(_ts_mc_u3, _wpu_gauge_str)
                                if not ((_ts_mcat3 and _ts_t13 != _ts_mcat3) or _wpu_gauge_str == "20"):
                                    _ts_desc3 = _item_desc_map_plan.get(str(item).strip().upper(), "")
                                    if _is_description_special_type_plan(_ts_desc3, _ts_rule3["keywords"]):
                                        _ts_uk3 = (_ts_fac3, _ts_typ3, plan_week)
                                        _type_special_weekly_usage[_ts_uk3] = _type_special_weekly_usage.get(_ts_uk3, 0) + _wpu_added
                    # ก้าวไป week ถัดไปเสมอหลัง produce (ห้าม plan item เดิมใน week เดิมซ้ำ)
                    _produced_week = plan_week
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break          # หมดสัปดาห์ให้วาง (สุด horizon) — ที่เหลือเป็นงานล้น
                    continue

                # ตรวจว่ามี row ของ FG อื่นอยู่หรือไม่ → ถ้ามี = FG split (สร้าง row ใหม่แทน merge)
                _has_other_fg = any(
                    _p.get("ITEM_CODE") == item
                    and _p.get("PLAN_WEEK") == plan_week
                    and _p.get("MC_GROUP") == mc_group
                    for _p in plans
                )
                if _has_other_fg:
                    # FG_WEEK ต่างกัน → สร้าง row ใหม่ (fall through ไปที่ append ด้านล่าง)
                    _is_fg_split = True
                    print(
                        f"[DEBUG FG SPLIT] {item}+{plan_week}+{mc_group}: existing FG differs → new row for FG {fg_week}"
                    )
                else:
                    print(
                        f"[DEBUG DUPLICATE] FALLBACK SKIP: {item}+{plan_week}+{mc_group} already planned but merge target not found"
                    )
                    # ข้ามไป week ถัดไป
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break
                    continue

            # carry ถูกตัดจนไม่เหลือเครื่อง ([CARRY POOL FULL]) → วางสัปดาห์นี้ไม่ได้
            # ห้ามสร้างแถวที่ใช้เครื่อง 0 (ผลิตไม่ได้ แต่กินที่ในแผน)
            if available_machines <= 0 and not _s9_active:
                # โควตาตั้งเครื่องของ "ผ้านี้" เต็มเพราะใบสั่งอื่นของผ้าเดียวกันใช้ไปแล้ว
                # (พูลยังมีเครื่องว่าง) = รอสัปดาห์หน้าแล้วตั้งได้ ไม่ใช่ในบ้านเอาไม่อยู่
                # → ห้ามติดธง _inhouse_stuck ไม่งั้นยอดไหลไปจ้างทอทั้งที่แค่ต้องรอคิวตั้งเครื่อง
                _quota_full_only = (
                    _item_setup_room(item, mc_group, _sel_gauge, plan_week) <= 0
                    and int(get_actual_mc_remain(
                        mc_group, plan_week, gauge=_sel_gauge, item_code=item
                    ) or 0) > 0
                )
                print(
                    f"[NO MC] {item} SC {sc_so_no} W{plan_week}: "
                    + ("โควตาตั้งเครื่องของผ้านี้เต็มแล้วในสัปดาห์นี้" if _quota_full_only
                       else "ไม่มีเครื่องให้ใช้")
                    + " → ข้ามไปสัปดาห์ถัดไป"
                )
                # พูลไม่เหลือเครื่องให้งานนี้จริง ๆ — สัญญาณเดียวกับ CARRY POOL FULL
                if not _inhouse_stuck and not _quota_full_only:
                    _inhouse_stuck = True
                    _inhouse_stuck_why = f"W{plan_week} ไม่มีเครื่องในพูลให้ใช้"
                    print(f"[INHOUSE STUCK] {item} SC {sc_so_no}: {_inhouse_stuck_why} "
                          f"→ ยอดที่เหลือข้ามด่านเทียบ ไปจ้างทอได้ถ้าเข้าเกณฑ์")
                plan_week = next_week(plan_week)
                if plan_week is None:
                    break
                continue

            # ===== Look-ahead: setup ใหม่ต้องมีเครื่องพอ "ทุกสัปดาห์" ที่งานจะวิ่ง =====
            # ถ้างานต้องวิ่งข้ามไปสัปดาห์ที่ booking จองเกินพูลแล้ว → carry ต่อไม่ได้
            # การ setup ที่สัปดาห์นี้จะเสียวัน setup เปล่าแล้วค้างกลางคัน → ไม่วางตั้งแต่แรก
            # (เลื่อนไปลองสัปดาห์ถัดไป; ถ้าไม่มีสัปดาห์ไหนวิ่งได้เลย งานจะไม่ถูกวาง → รายงานท้ายรัน)
            if new_mc > 0 and not _s9_active and not rts_local_force:
                # งานสี: สัปดาห์ที่ติดยังยึดเครื่องคืนจากงานที่ยังไม่ให้สีได้ ก่อนจะยอมแพ้
                # (ไม่งั้นก้อนสีถูกเด้งออกจากทุกสัปดาห์ต้น ๆ แล้วไปกองท้ายจนเลย target —
                #  เคสจริง FD4DRTRN74/03A0: ถูกเด้งที่ W40 เพราะ W41 พูลเหลือ 0 ทั้งที่
                #  งานที่ยังไม่ให้สีถือเครื่องอยู่ในสัปดาห์นั้น)
                _span_block = _span_block_with_grab(carryover_mc, new_mc)
                if _span_block is not None:
                    _blk_wk, _blk_rem, _blk_need = _span_block
                    print(
                        f"[NO CARRY ROOM] {item} SC {sc_so_no}: setup W{plan_week} "
                        f"({carryover_mc}+{new_mc} เครื่อง) แต่ W{_blk_wk} วิ่งต่อไม่ได้ "
                        f"(พูลเหลือ {_blk_rem} ต้องการ {_blk_need}) → ไม่วางที่ W{plan_week}"
                    )
                    no_mc_skips.append({
                        "item": item, "sc": sc_so_no, "mc_group": mc_group,
                        "gauge": _sel_gauge, "setup_week": plan_week,
                        "blocked_week": _blk_wk, "remain": _blk_rem, "need": _blk_need,
                        "order_qty": order_qty, "pending_plan": pending_plan, "fg_week": fg_week,
                    })
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break
                    continue

            _settle_grab()
            item_gauge = str(order.get("MC_GUAGE", "")).strip()
            print(f"[DEBUG DUPLICATE] Adding plan for {item}+{plan_week}+{mc_group}")

            # เครื่องที่ booking ทอไอเทมนี้อยู่แล้วในสัปดาห์นี้ (carry มาจาก booking ไม่ใช่ setup ใหม่)
            _row_booking_mc = (
                0
                if _s9_active
                else int(
                    booking_mc_by_week.get(_ck(item, mc_group, item_gauge), {}).get(
                        week_index(plan_week), 0
                    )
                    or 0
                )
            )

            # จำทรงเครื่องของก้อนนี้ไว้ — สัปดาห์ถัดไปห้ามเพิ่มถ้าเริ่มไล่ลงแล้ว
            if MC_SHAPE_MONOTONE and not _s9_active:
                _sk = (item, mc_group, str(order.get("MC_GUAGE", "")), sc_so_no, str(order.get("FG Week", "")))
                _pk, _dec = _mc_shape_state.get(_sk, (0, False))
                _now_total = int(available_machines or 0)
                if _now_total > 0:
                    if _now_total < _pk:
                        _dec = True                    # เริ่มไล่ลงแล้ว
                    _mc_shape_state[_sk] = (max(_pk, _now_total), _dec)

            # ===== เครื่องชุดเดียวกับใบสั่งอื่นของ item นี้ในสัปดาห์เดียวกัน =====
            # ใบที่ไม่มีลูกโซ่ของตัวเอง (ITEM CARRY WAIT "carry in same week") ได้ carry มาจาก
            # machines_in_use ของ item = เครื่องที่ใบอื่นเพิ่งประกาศไว้ในสัปดาห์นี้ → เครื่องชุดนั้น
            # เป็นของแถวใบอื่นแล้ว แถวนี้ต้องประกาศเฉพาะเครื่องที่เพิ่มจริง ส่วนที่ซ้อนเขียนเป็น
            # MC_SHARED (กติกาเดียวกับทาง "เติมเข้าเครื่องเดิม") ไม่งั้นบัญชีพูล (_pool_blocks_from_plan)
            # และ Gantt ที่บวกข้ามใบสั่งจะนับเครื่องชุดเดียวกันซ้ำ
            # (เคสจริง 14 ก.ย. 2569 FD3BASDP23/05B0 RAOO G16 W42: 3 ใบ 2+3+3 = 8 ทั้งที่มีจริง 3)
            # ใบที่ทอต่อจากลูกโซ่ของตัวเอง (สัปดาห์ก่อน) ถือเครื่องคนละชุดจริง → ไม่แตะ
            _row_sc = str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip()
            _row_shared_mc = 0
            _row_shared_with = []
            if not _s9_active and carryover_mc > 0:
                _sh_cur_idx = week_index(plan_week)
                _sh_own_idx = last_sc_week.get((item, mc_group, _sel_gauge, sc_so_no))
                _sh_own_chain = (
                    _sh_own_idx is not None
                    and _sh_cur_idx is not None
                    and 0 < _sh_cur_idx - _sh_own_idx <= SETUP_GAP_WEEK
                )
                if not _sh_own_chain:
                    _sh_other = {}
                    for _p in plans:
                        if (
                            _p.get("ITEM_CODE") == item
                            and _p.get("MC_GROUP") == mc_group
                            and _p.get("PLAN_WEEK") == plan_week
                        ):
                            _p_sc = str(_p.get("SC_SO_NO", "")).strip()
                            _p_mc = int(_p.get("ACTUAL_MC", 0) or 0)
                            if _p_sc != _row_sc and _p_mc > 0:
                                # หลายแถวของใบเดียวกันเขียนเครื่องชุดเดียวซ้ำ → max ; ข้ามใบ → บวก
                                _sh_other[_p_sc] = max(_sh_other.get(_p_sc, 0), _p_mc)
                    _row_shared_mc = min(int(carryover_mc), sum(_sh_other.values()))
                    _row_shared_with = list(_sh_other)
                    if _row_shared_mc > 0:
                        print(
                            f"[SAME WEEK SHARED MC] {item} SC {sc_so_no} W{plan_week}: carry {carryover_mc} "
                            f"ซ้อนกับเครื่องของ SO {', '.join(_row_shared_with)} {_row_shared_mc} ตัว "
                            f"→ ACTUAL_MC {available_machines}→{available_machines - _row_shared_mc}"
                        )
            _row_actual_mc = available_machines - _row_shared_mc
            _row_carry_mc = carryover_mc - _row_shared_mc
            if _row_shared_mc > 0:
                _sh_txt = (
                    f"ใช้เครื่อง {_row_shared_mc} ตัวร่วมกับ SO {', '.join(_row_shared_with)} "
                    f"(ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"
                )
                _row_remark = (
                    _build_plan_remark(mc_group, _row_carry_mc, new_mc, setup_days_used, booking_mc=_row_booking_mc)
                    + " + " + _sh_txt
                    if (_row_carry_mc > 0 or new_mc > 0)
                    else f"{mc_group}: {_sh_txt}"
                )
            else:
                _row_remark = _build_plan_remark(
                    S9_KEY if _s9_active else mc_group,
                    carryover_mc,
                    new_mc,
                    setup_days_used,
                    booking_mc=_row_booking_mc,
                )

            plans.append(
                {
                    "ITEM_CODE": item,
                    "SC_SO_NO": str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip(),
                    "MC_GROUP": S9_KEY if _s9_active else mc_group,
                    "MC_GUAGE": "C0" if _s9_active else order["MC_GUAGE"],
                    "FACTORY_TYPE": "OUTSOURCE" if _s9_active else FACTORY_TYPE_MAP.get(mc_group, "UNKNOWN"),
                    "PLAN_WEEK": plan_week,
                    # เก็บ row index บน timeline ไว้ด้วย — PLAN_WEEK เป็น bare week
                    # ที่ระบุปีไม่ได้เมื่อแผนคร่อมปี (ใช้เติม PLAN_YEAR ตอน export)
                    "PLAN_IDX": week_pos(plan_week),
                    "PRODUCE_QTY": produce,
                    "SETUP_DAYS": setup_days_used,
                    "REQUIRED_MC": planned_mc,  # เครื่องที่คำนวณไว้ล่วงหน้า (RDD target) หรือ "Maxmc" ถ้าไม่ทัน RDD
                    # เครื่องที่แถวนี้ประกาศเอง — ไม่รวมเครื่องที่ใช้ร่วมกับใบอื่นในสัปดาห์เดียวกัน (MC_SHARED)
                    "ACTUAL_MC": _row_actual_mc,
                    "CARRYOVER_MC": _row_carry_mc,  # เครื่องที่ carry-over จาก week ก่อน
                    "NEW_MC": new_mc,  # เครื่องใหม่ที่ setup week นี้
                    "MC_SHARED": _row_shared_mc,  # เครื่องที่ใช้ร่วมกับ SO อื่นของ item นี้ใน week เดียวกัน
                    "MC_BOOKING": _row_booking_mc,  # เครื่องที่ booking ทอไอเทมนี้อยู่แล้วในสัปดาห์นี้
                    "REMARK": _row_remark,
                    "FACTORY_WORKING_DAYS": _s9_working_days(plan_week) if _s9_active else get_working_days_by_factory(
                        mc_group, available_machines, week=plan_week, gauge=item_gauge
                    ),
                    # วันทำงานฐานของกลุ่ม (ค่ามาตรฐาน ไม่อิงสัปดาห์) — ให้หน้าเว็บใช้ fallback
                    # เมื่อลากงานข้ามสัปดาห์ (ค่ารายสัปดาห์จริงหน้าเว็บดึงจาก /api/workday)
                    "WD_BASE": S9_WORK_DAYS if _s9_active else get_working_days_by_factory(
                        mc_group, available_machines, week=None, item_code=item, gauge=item_gauge
                    ),
                    "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(plan_week)),
                    "ACTUAL_WORKING_DAYS": _s9_working_days(plan_week) if _s9_active else get_working_days_by_factory(mc_group, available_machines, week=plan_week, gauge=item_gauge),
                    "DAILY_CAPACITY": daily_capacity,
                    "REVOLUTION_WEIGHT": rev_weight if rev_weight is not None else 0,
                    "AVAILABLE_DAYS": available_days,
                    "ORDERS_QTY": order_qty,
                    "PENDING_PLAN": pending_plan,
                    "PLAN_QTY": qty_left - produce,
                    "ORDER_TYPE": order_type,
                    "ORDER_DATE": order["Date"],
                    "FG_WEEK": fg_week,
                    "TARGET_KNIT": fg_week_int,
                    # เก็บ row index บน timeline ของเดดไลน์ด้วย — TARGET_KNIT เป็น bare week
                    # เหมือน PLAN_WEEK จึงเทียบ "ทัน/ไม่ทัน" ข้ามปีไม่ได้ (ใช้เติม TARGET_YEAR)
                    "TARGET_IDX": rdd_idx,
                    "FLOOR_IDX": _hard_floor_idx,  # พื้นแข็งของใบ (ดู _swap_color_same_item)
                    "MATERIAL_CONTENT": str(order.get("MATERIAL_CONTENT", "")).strip(),
                    "IS_CORE_ITEM": "CORE ITEM" if is_core_item else "",
                    "CUSTOMER": str(order.get("Customer", "")).strip(),
                    "PLAN_SOURCE": "NEW",
                    "LT_YARN": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_days(item),
                    "YARN_USED": _yarn_used_lookup.get(str(item).strip().upper(), ""),
                    "DATE_IN": order.get("DATE_IN"),
                    "EARLIEST_PLAN_WEEK": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_earliest_week(item, date_in=order.get("DATE_IN")),
                    "SUB_COLOR": sub_color,
                    "PO_NO": str(order.get("PO_NO", "")).strip(),
                    "YARNPO": str(order.get("YARNPO", "") or "").strip(),
                    "RDD_WEEK": _rdd_week_of(order),
                    "SC_LINE_ID": ColorOpen.so_text(order.get("SC_LINE_ID")),
                    "KP_NO": ColorOpen.so_text(order.get("KP_NO")),  # กัน "nan" ของแถวฝั่ง SC
                }
            )
            plans[-1]["NAY_COLOR"] = str(order.get("NAY_COLOR", "")).strip()
            plans[-1]["COLOR_DESC"] = str(order.get("COLOR_DESC", "")).strip()
            plans[-1]["LINE_REMARK"] = _clean_remark(order.get("LINE_REMARK", ""))
            # วัน/สัปดาห์ด้ายเข้าครบ (ล็อตที่ไกลสุด) — หน้าเว็บใช้โชว์บนบล็อก Gantt
            plans[-1]["PO_IN_DATE"], plans[-1]["PO_IN_WEEK"] = po_in_fields(order)
            plans[-1]["YARN_STOCK"] = yarn_stock_note(order)
            plans[-1]["S9_ROUTING"] = _s9_active
            # พูลเครื่องจริงของผู้รับจ้าง — MC_GUAGE ของแถวถูกเขียนทับเป็น C0
            # (เพื่อไม่ให้ปนพูลในบ้าน) หน้าเว็บจึงไม่มีทางรู้ว่าก้อนนี้ลงเครื่องพูลไหน
            # → เก็บไว้อีกคอลัมน์ ให้ Gantt จ้างทอแยกแถวตามพูล + เทียบ Total MC ของชีท MC S9 ได้
            plans[-1]["S9_MC"] = str(mc_group).strip() if _s9_active else ""
            plans[-1]["S9_GAUGE"] = _normalize_gauge(_sel_gauge) if _s9_active else ""
            # พูลย่อยที่งานนี้นั่ง (1 แถวในชีท MC S9) — S9_MC/S9_GAUGE เดียวกันมีได้หลายพูล (ทั่วไป / Poly Only)
            plans[-1]["S9_POOL"] = (_s9_pool_id_cur or "") if _s9_active else ""
            # OUTSOURCE = ก้อนที่ user สั่งจ้างทอเอง (แยกจาก S9 Only ที่ MasterMC บังคับ)
            plans[-1]["OUTSOURCE"] = "Yes" if _outsource_force else ""
            # ก้อนที่ระบบส่งออกไปจ้างทอเอง — ต่อเหตุผลไว้ท้าย REMARK ให้ตรวจย้อนได้ว่าทำไมออกไปข้างนอก
            if _s9_active and _s9_auto and _s9_auto_reason:
                _s9_rmk = str(plans[-1].get("REMARK", "") or "").strip()
                plans[-1]["REMARK"] = f"{_s9_rmk} | {_s9_auto_reason}" if _s9_rmk else _s9_auto_reason
            # ก้อนนี้อยู่ในโควตาสีคงเหลือของใบสั่งไหม (ธงจาก _split_orders_by_color_quota)
            # ต้องติดไปกับแถวแผน ไม่งั้นรอบซ่อมท้ายแผนจะแจกโควตาใหม่ตามลำดับสัปดาห์
            # แล้วยกสิทธิ์งานสีให้ "ก้อนส่วนเกิน" ที่บังเอิญถูกวางสัปดาห์เร็วกว่า
            plans[-1]["COLOR_PART"] = _color_part_text(order)
            _cp_flag = plans[-1]["COLOR_PART"] == "Yes"
            # Color Gap: ก้อนสีที่เพิ่งวาง = จุดตั้งต้นใหม่ของ "สีถึงสัปดาห์ไหน" ของคีย์นี้
            # (ก้อนสีมี priority 0 วางก่อนก้อนไม่มีสีเสมอ → พอถึงคิวก้อนไม่มีสี ทะเบียนพร้อมแล้ว)
            # งานจ้างทอไม่นับ — ไม่ได้ใช้เครื่องในบ้าน จึงไม่มีเครื่องให้กัน
            if _cp_flag and not _s9_active:
                _note_color_end(item, mc_group, order.get("MC_GUAGE"), week_index(plan_week))
            # ก้อนสีที่ถือเครื่องเผื่อก้อนพี่น้อง (ยอดที่ยังไม่ให้สีของใบเดียวกัน) — งานสีของ
            # item อื่นยึดเครื่องส่วนเกินคืนได้ (ดู _hold_extra_mc) เพื่อไม่ให้ผิดกติกางานสีมาก่อน
            plans[-1]["MC_HOLD_SPLIT"] = "Yes" if _split_hold_mc else ""
            qty_left -= produce
            # ===== แตกแถวจ้างทอเป็นรายผู้รับจ้าง (user 16 ก.ย. 2569) =====
            # เครื่องเป็นของแต่ละเจ้า: จ้างรวมกันได้ แต่ห้ามใช้เกินเครื่องของเจ้านั้น
            # เครื่อง/ยอดผลิตที่คำนวณไว้เป็นของ "ทั้งกอง" → หารตามเครื่องที่แต่ละเจ้ารับไป
            # (แถวสุดท้ายรับเศษ เพื่อให้ผลรวมเท่ากับก้อนเดิมเป๊ะ ไม่งอกไม่หาย)
            if _s9_active:
                _s9_alloc = _s9_allocate_supps(
                    _sel_gauge, _s9_cat_cur, _s9_mat_cur, available_machines, plan_week, item,
                )
                _s9_base = plans.pop()
                _s9_tot_mc = sum(_a[3] for _a in _s9_alloc)
                if not _s9_alloc or _s9_tot_mc <= 0:
                    # ไม่มีเจ้าไหนว่างเลย — ไม่ควรเกิด (เช็คเพดานกองมาแล้ว) แต่ถ้าเกิดต้องไม่เขียนแถวผี
                    print(f"[S9 SUPP] {item} W{plan_week}: ไม่มีผู้รับจ้างว่าง → ไม่เขียนแถว")
                    qty_left += produce
                else:
                    _s9_qty_left = float(_s9_base.get("PRODUCE_QTY") or 0)
                    for _i, (_pid, _supp, _mcname, _n, _carry_n) in enumerate(_s9_alloc):
                        _r = dict(_s9_base)
                        _last = _i == len(_s9_alloc) - 1
                        _q = _s9_qty_left if _last else round(
                            float(_s9_base.get("PRODUCE_QTY") or 0) * _n / _s9_tot_mc, 2)
                        _s9_qty_left = round(_s9_qty_left - _q, 2)
                        # MC_GROUP = ชื่อผู้รับจ้าง (user 16 ก.ย. 2569) — เลิกใช้ "COMKN" เป็นชื่อกลุ่ม
                        # ตัวบอกว่า "แถวนี้เป็นงานจ้างทอ" ย้ายไปที่ธง S9_ROUTING แล้ว (_is_outsource_row)
                        _r["MC_GROUP"] = _supp or _mcname
                        _r["S9_SUPP"] = _supp
                        _r["S9_MC"] = _mcname
                        _r["S9_POOL"] = _pid
                        _r["ACTUAL_MC"] = _n
                        _r["CARRYOVER_MC"] = _carry_n
                        _r["NEW_MC"] = _n - _carry_n
                        _r["MC_SHARED"] = 0
                        _r["PRODUCE_QTY"] = _q
                        _r["REMARK"] = _build_plan_remark(_supp or _mcname, _carry_n, _n - _carry_n, 0)
                        if _s9_auto and _s9_auto_reason:
                            _r["REMARK"] = f'{_r["REMARK"]} | {_s9_auto_reason}'
                        plans.append(_r)
                        # เพดานรายเจ้า: จดเครื่องที่เจ้านี้ถูกใช้ไปในสัปดาห์นี้ (คีย์เดียวกับ _s9_pool_remain)
                        _s9_uk = (plan_week, _pid)
                        _s9_weekly_usage[_s9_uk] = _s9_weekly_usage.get(_s9_uk, 0) + _n
                        _s9_idx_t = week_index(plan_week)
                        if _s9_idx_t is not None:
                            _s9_sk = (str(item).strip().upper(), _pid, int(_s9_idx_t))
                            _s9_supp_week_mc[_s9_sk] = _s9_supp_week_mc.get(_s9_sk, 0) + _n
                    print(f"[S9 SUPP] {item} W{plan_week} {_s9_tot_mc} เครื่อง → "
                          + ", ".join(f"{_a[1]} {_a[3]}" for _a in _s9_alloc))
                    # ฐานของการไล่เครื่องสัปดาห์ถัดไป — ยังนับรวมทั้งกอง (ทรงเครื่องเท่าเดิมกับก่อนแยกเจ้า)
                    _s9_idx_t = week_index(plan_week)
                    if _s9_idx_t is not None:
                        _s9_ik = (str(item).strip().upper(), str(mc_group).strip().upper(),
                                  _normalize_gauge(_sel_gauge) if _sel_gauge is not None else "",
                                  int(_s9_idx_t))
                        _s9_item_week_mc[_s9_ik] = _s9_item_week_mc.get(_s9_ik, 0) + _s9_tot_mc
            if qty_left <= 0:
                qty_left = 0  # ป้องกันค่าติดลบ


            # 🔧 FIX: บันทึก Item+Week+MC_GROUP ที่ได้วางแผนไปแล้ว
            _existing_item_week_mc.add((item, plan_week, S9_KEY if _s9_active else mc_group))
            print(f"[DEBUG DUPLICATE] Added to existing set: {item}+{plan_week}+{S9_KEY if _s9_active else mc_group}")
            # บันทึก/อัปเดต remaining cap สำหรับ FG ถัดไปของ item+machine เดียวกัน
            _cap_key = _same_week_key  # same_week_key ใช้ S9_KEY สำหรับงานจ้างทอแล้ว
            if _same_week_rem_cap is not None:
                # อัปเดต remaining cap หลังใช้งาน
                _new_rem = max(0, float(_same_week_total_cap or 0.0) - produce)
                if _new_rem > 0:
                    remaining_week_cap[_cap_key] = _new_rem
                    remaining_week_cap_owner[_cap_key] = sc_so_no
                else:
                    remaining_week_cap.pop(_cap_key, None)
                    remaining_week_cap_owner.pop(_cap_key, None)
            else:
                # บันทึกส่วนที่เหลือสำหรับ FG ถัดไป (ไม่ว่า order จะจบหรือไม่)
                # เพื่อให้ SC/SO อื่นสามารถ carry ต่อใน week เดียวกันได้
                _full_week_cap = cap_old + cap_new
                _rem = max(0, _full_week_cap - produce)
                # NOTE: ห้ามเพิ่ม freed capacity จากเครื่องที่ลดลง เพราะเครื่องที่ "ปล่อย"
                # ไม่ได้วิ่งอยู่ใน week นี้แล้ว → capacity ต้องอิงจากเครื่องที่ใช้จริงเท่านั้น
                # เครื่องที่ปล่อยจะว่างให้ item อื่นใช้ผ่าน machine pool แทน
                remaining_week_cap[_cap_key] = _rem
                remaining_week_cap_owner[_cap_key] = sc_so_no
            # S9: ใช้ S9_KEY ใน last_production/machines_in_use เพื่อแยกพูลจ้างทอออกจากเครื่องในบ้าน
            _plan_ck = _resolve_carry_key(item, S9_KEY if _s9_active else mc_group, _sel_gauge)
            last_production[_plan_ck] = week_index(plan_week)
            machines_in_use[_plan_ck] = available_machines  # บันทึกจำนวนเครื่องที่ใช้จริง
            last_sc_machines[(item, S9_KEY if _s9_active else mc_group, _sel_gauge, sc_so_no)] = available_machines
            last_sc_week[(item, S9_KEY if _s9_active else mc_group, _sel_gauge, sc_so_no)] = week_index(plan_week)
            last_sc_so_no[_plan_ck] = sc_so_no  # บันทึก SC/SO NO ล่าสุดที่ผลิต
            if order_type == "YD-ORDERS" and sub_color:

                last_sub_color[_plan_ck] = sub_color
            # YD-ORDERS: ล็อกจำนวนเครื่องสำหรับ SO ถัดไปใน week เดียวกัน
            if order_type == "YD-ORDERS":
                _yd_lock_key = (item, mc_group, plan_week)
                if _yd_lock_key not in _yd_week_locked_mc:
                    _yd_week_locked_mc[_yd_lock_key] = (available_machines, carryover_mc, new_mc)
            new_plan_started_items.add(_plan_ck)  # บันทึกว่า item นี้เริ่ม new plan แล้ว

            # บันทึก total machines per (week, mc_group) สำหรับ gradual increase
            # 🔧 FG SPLIT: ถ้าเป็น FG split row → เครื่องนับไปแล้วจาก FG แรก ห้ามนับซ้ำ
            # งานจ้างทอ (S9) ไม่ได้ใช้เครื่องในบ้าน — mc_group ตอนนี้ยังเป็นพูลผู้รับจ้าง (เช่น RAO)
            # ถ้านับเข้าตัวนับในบ้านจะหักพูลเราซ้ำ (มี _s9_weekly_usage นับแยกอยู่แล้ว)
            if not _is_fg_split and not _s9_active:
                _week_mc_key = (week_index(plan_week), mc_group)
                # เครื่องที่ใช้ร่วมกับใบอื่นในสัปดาห์เดียวกันถูกนับไปแล้วตอนวางใบนั้น → บวกเฉพาะส่วนที่เพิ่มจริง
                weekly_mc_usage[_week_mc_key] = weekly_mc_usage.get(_week_mc_key, 0) + (available_machines - _row_shared_mc)
            # อัพเดท job usage สำหรับสัปดาห์นี้ (นับเฉพาะ new_mc = machines ที่ setup ใหม่)
            # เครื่อง carry ต่อ (carryover_mc) ไม่ต้องนับเป็น job ใหม่เพราะไม่ต้อง setup
            # ⚠️ ต้องหักนอกเงื่อนไข _is_fg_split — FG split / YD-ORDERS ก็ตั้งเครื่องใหม่จริง
            # (SETUP_TRACKING นับทุกแถวที่ NEW_MC>0 ถ้าไม่หักตรงนี้ job/week cap จะรั่ว → แผนเกินโควตา)
            # ⚠️ ต้อง setdefault เสมอ (แม้ new_mc=0) — โค้ดตรวจ overload ท้ายลูปอ่าน
            # weekly_job_usage[_produced_week] ตรงๆ ถ้าไม่มี key จะ KeyError
            weekly_job_usage.setdefault(plan_week, {})
            if new_mc > 0 and not _s9_active:
                weekly_job_usage[plan_week][mc_group] = (
                    weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
                )
                # หักโควตาตั้งเครื่องใหม่ของ "ผ้านี้" ในสัปดาห์นี้ — ใบสั่งอื่นของผ้าเดียวกัน
                # ที่วางต่อจากนี้ในสัปดาห์เดียวกันจะเหลือโควตาน้อยลง (ช่างตั้งเครื่องเป็นรายผ้า)
                _item_setup_charge(item, mc_group, _sel_gauge, plan_week, new_mc)
            # อัพเดท new plan usage (นับทั้ง carryover+new สำหรับ get_actual_mc_remain)
            # key ต้องเป็น (mc_group, gauge_str) เสมอ — ห้าม pool ข้าม GUAGE
            # ถ้าเป็น same-week continuation หรือ FG split → เครื่องนับไปแล้วจาก FG ก่อนหน้า ห้ามนับซ้ำ
            if not _rem_mc_counted and not _is_fg_split and not _s9_active:
                if plan_week not in weekly_new_plan_usage:
                    weekly_new_plan_usage[plan_week] = {}
                _wpu_cap = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == mc_group)
                ]
                _wpu_gauge_raw = _wpu_cap.iloc[0]["GUAGE"] if not _wpu_cap.empty else None
                _wpu_gauge_str = (
                    str(_wpu_gauge_raw).strip()
                    if _wpu_gauge_raw is not None
                    and not (isinstance(_wpu_gauge_raw, float) and pd.isna(_wpu_gauge_raw))
                    else ""
                )
                # ถ้า mc_group+gauge อยู่ใน MC_GROUP_REDIRECT → หักเครื่องจาก target แทน
                # เช่น SKP 20 → หักจาก FA 20 เสมอ
                _wpu_mc_r, _wpu_gauge_r = _apply_mc_redirect(mc_group, _wpu_gauge_str)
                _wpu_key = (_mc_to_type1(_wpu_mc_r, _wpu_gauge_r), _wpu_gauge_r)
                _wpu_added = _pool_charge_mc(
                    available_machines, _booking_week_mc, new_mc, _rem_hold_mc
                )
                # MC Special: item ที่มี sub-pool (COTTON/POLY) กินเฉพาะ sub-pool ของตัวเอง
                # ไม่ไปกินพูลทั่วไปด้วย (ไม่งั้นหักซ้ำ 2 พูล)
                _wpu_sp_type = _get_subgroup_by_item_prefix(mc_group, _wpu_gauge_str, item)
                if _wpu_sp_type:
                    if _wpu_added > 0:
                        _wpu_sp_f = _mc_to_factory(str(mc_group).strip().upper(), _wpu_gauge_str)
                        _wpu_sp_cat = _mc_to_type1(str(mc_group).strip().upper(), _wpu_gauge_str)
                        _wpu_sp_key = (_wpu_sp_f, _wpu_sp_cat, _wpu_gauge_r, plan_week, _wpu_sp_type)
                        _mc_special_weekly_usage[_wpu_sp_key] = _mc_special_weekly_usage.get(_wpu_sp_key, 0) + _wpu_added
                else:
                    weekly_new_plan_usage[plan_week][_wpu_key] = (
                        weekly_new_plan_usage[plan_week].get(_wpu_key, 0) + _wpu_added
                    )
                    # พูลแยก (SKP/SKPTA·SKPLE): นับต่อ pool label ให้หักถูกพูล
                    _wpu_pool = _mc_to_pool_label(_wpu_mc_r, _wpu_gauge_r)
                    if _wpu_pool:
                        weekly_new_plan_pool_usage.setdefault(plan_week, {})[_wpu_pool] = (
                            weekly_new_plan_pool_usage.setdefault(plan_week, {}).get(_wpu_pool, 0) + _wpu_added
                        )
                # TYPE_SPECIAL quota tracking
                if _wpu_added > 0 and _TYPE_DESC_RULES_PLAN:
                    _ts_mc_u2 = str(mc_group).strip().upper()
                    _ts_fac2  = _mc_to_factory(_ts_mc_u2, _wpu_gauge_str)
                    _ts_typ2  = _mc_to_type_raw_plan.get((_ts_mc_u2, _wpu_gauge_str), "").strip().upper()
                    _ts_rk2   = (_ts_fac2.upper(), _ts_typ2)
                    if _ts_rk2 in _TYPE_DESC_RULES_PLAN:
                        _ts_rule2  = _TYPE_DESC_RULES_PLAN[_ts_rk2]
                        _ts_mcat2  = _ts_rule2.get("mc_cat", "")
                        _ts_t12    = _mc_to_type1(_ts_mc_u2, _wpu_gauge_str)
                        if not ((_ts_mcat2 and _ts_t12 != _ts_mcat2) or _wpu_gauge_str == "20"):
                            _ts_desc2 = _item_desc_map_plan.get(str(item).strip().upper(), "")
                            if _is_description_special_type_plan(_ts_desc2, _ts_rule2["keywords"]):
                                _ts_uk2 = (_ts_fac2, _ts_typ2, plan_week)
                                _type_special_weekly_usage[_ts_uk2] = _type_special_weekly_usage.get(_ts_uk2, 0) + _wpu_added
            # ก้าวไป week ถัดไปเสมอหลัง produce (ห้าม plan item เดิมใน week เดิมซ้ำ)
            _produced_week = plan_week
            plan_week = next_week(plan_week)
            if plan_week is None:
                break                  # หมดสัปดาห์ให้วาง (สุด horizon) — ที่เหลือเป็นงานล้น
            # ตรวจสอบว่าแผนใหม่ไม่เกิน capacity ที่เหลือหลังจากแผนเก่า
            if not old_plan_df.empty and "PLAN_WEEK" in old_plan_df.columns:
                # หาจำนวน jobs ของแผนเก่าในสัปดาห์นี้
                old_week_jobs = old_plan_df[old_plan_df["PLAN_WEEK"] == _produced_week]
                if not old_week_jobs.empty:
                    mc_col = (
                        "REQUIRED_MC"
                        if "REQUIRED_MC" in old_week_jobs.columns
                        else "AVAILABLE_MACHINES"
                    )
                    # Ensure the column is numeric before summing to avoid string concatenation
                    old_jobs_by_mc = (
                        old_week_jobs.assign(
                            _mc_num=pd.to_numeric(
                                old_week_jobs[mc_col], errors="coerce"
                            ).fillna(0)
                        )
                        .groupby("MC_GROUP")["_mc_num"]
                        .sum()
                        .astype(int)
                        .to_dict()
                    )
                    # ตรวจสอบแต่ละ MC_GROUP ว่าเกินแผนเก่าหรือไม่
                    for used_mc_group, new_jobs in weekly_job_usage.get(_produced_week, {}).items():
                        old_jobs = old_jobs_by_mc.get(used_mc_group, 0)
                        if new_jobs > old_jobs:
                            print(
                                f"⚠️  OVER OLD PLAN: Week {_produced_week} {used_mc_group} ใช้ {new_jobs} jobs (เกินแผนเก่า {old_jobs} jobs)"
                            )
                    # ตรวจสอบ capacity ที่เหลือหลังจากแผนเก่า
                    remaining_capacity_by_type = {}
                    for used_mc_group in weekly_job_usage.get(_produced_week, {}):
                        mc_info = master_mc[master_mc["MC"] == used_mc_group]
                        if not mc_info.empty:
                            factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
                            _raw_t = mc_info.iloc[0].get("Type", "")
                            mc_type = "" if pd.isna(_raw_t) else str(_raw_t).strip().upper()
                            type_key = _make_type_key(factory, mc_type)
                            # หาจำนวน jobs ของแผนเก่าใน type เดียวกัน
                            old_type_jobs = 0
                            for old_mc_group, old_jobs in old_jobs_by_mc.items():
                                old_mc_info = master_mc[master_mc["MC"] == old_mc_group]
                                if not old_mc_info.empty:
                                    old_factory = (
                                        str(old_mc_info.iloc[0]["Factory"]).strip().upper()
                                    )
                                    _raw_ot = old_mc_info.iloc[0].get("Type", "")
                                    old_mc_type = (
                                        ""
                                        if pd.isna(_raw_ot)
                                        else str(_raw_ot).strip().upper()
                                    )
                                    old_type_key = _make_type_key(old_factory, old_mc_type)
                                    if old_type_key == type_key:
                                        old_type_jobs += old_jobs
                            # คำนวณ capacity ที่เหลือ — type_key เป็นคีย์เดียวกับ JOB_CAP
                            # (อ่านตรง ๆ ไม่แยกโรง/ชนิดเครื่องซ้ำ ไม่งั้น "SINGLE (TUBE)" หลุดเป็นไม่จำกัด)
                            if type_key not in remaining_capacity_by_type:
                                max_capacity = JOB_CAP.get(type_key, JOB_CAP_UNLIMITED)
                                remaining_capacity_by_type[type_key] = (
                                    max_capacity - old_type_jobs
                                )
                    # ตรวจสอบว่าเกิน capacity ที่เหลือหรือไม่
                    for type_key, remaining_capacity in remaining_capacity_by_type.items():
                        # หาจำนวน jobs ใหม่ใน type เดียวกัน
                        new_type_jobs = 0
                        for used_mc_group, new_jobs in weekly_job_usage.get(
                            _produced_week, {}
                        ).items():
                            mc_info = master_mc[master_mc["MC"] == used_mc_group]
                            if not mc_info.empty:
                                factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
                                _raw_t2 = mc_info.iloc[0].get("Type", "")
                                mc_type = (
                                    "" if pd.isna(_raw_t2) else str(_raw_t2).strip().upper()
                                )
                                current_type_key = _make_type_key(factory, mc_type)
                                if current_type_key == type_key:
                                    new_type_jobs += new_jobs
                        if new_type_jobs > remaining_capacity:
                            print(
                                f"⚠️  OVER REMAINING CAPACITY: Week {_produced_week} {type_key} ใช้ {new_type_jobs} jobs (เกิน capacity ที่เหลือ {remaining_capacity} jobs, แผนเก่าใช้ไปแล้ว)"
                            )
            # ตรวจสอบและแสดงผลการทับซ้อนในสัปดาห์นี้ (ตาม factory type)
            total_jobs_by_type = {}
            max_capacity_by_type = {}
            # คำนวณ jobs และ capacity ตาม factory type
            for used_mc_group in weekly_job_usage.get(_produced_week, {}):
                mc_info = master_mc[master_mc["MC"] == used_mc_group]
                if not mc_info.empty:
                    factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
                    _raw_t = mc_info.iloc[0].get("Type", "")
                    mc_type = "" if pd.isna(_raw_t) else str(_raw_t).strip().upper()
                    type_key = _make_type_key(factory, mc_type)
                    # บวก jobs ที่ใช้
                    if type_key not in total_jobs_by_type:
                        total_jobs_by_type[type_key] = 0
                    total_jobs_by_type[type_key] += weekly_job_usage[_produced_week][
                        used_mc_group
                    ]
                    # กำหนด capacity ตาม type — type_key ("PHET_DOUBLE" / "PHET_SINGLE" / "OM")
                    # เป็นคีย์เดียวกับ JOB_CAP อยู่แล้ว จึงอ่านตรง ๆ ไม่ต้องแยกโรง/ชนิดเครื่องซ้ำ
                    # (ของเดิมเทียบ mc_type == "SINGLE" ตรงตัว → "SINGLE (TUBE)" ไม่เข้าเงื่อนไขไหนเลย
                    #  แล้วกลายเป็นไม่จำกัด ทำให้บรรทัดเตือน OVERLOAD ของ PHET_SINGLE ไม่ขึ้น)
                    if type_key not in max_capacity_by_type:
                        max_capacity_by_type[type_key] = JOB_CAP.get(
                            type_key, JOB_CAP_UNLIMITED
                        )
            # ตรวจสอบและแสดงผลเฉพาะตอนที่เกิน (เตือนอย่างเดียว ไม่ได้ตัดงาน)
            for type_key, jobs_used in total_jobs_by_type.items():
                capacity = max_capacity_by_type.get(type_key, JOB_CAP_UNLIMITED)
                if jobs_used > capacity:
                    print(
                        f"⚠️  OVERLOAD: Week {_produced_week} {type_key} ใช้ {jobs_used} jobs (เกิน capacity {capacity} jobs)"
                    )
        # ออกจากลูปสัปดาห์ (break/หมดยอด) โดยรอบสุดท้ายยึดเครื่องไว้แต่ไม่ได้เขียนแถว → คืนงานเจ้าของเดิม
        _grab_txn_abort("จบออร์เดอร์โดยรอบสัปดาห์สุดท้ายไม่ได้เขียนแถว")
        # ออร์เดอร์ที่เข้า loop วางแผนแล้วแต่วางไม่ครบ (เครื่องไม่พอ/หมดสัปดาห์ในปี/ชน safety guard)
        # หมายเหตุ: กรณี CORE ITEM (batch production) qty_left ที่จับได้คือของ batch ปัจจุบันเท่านั้น
        # ถ้ายังมี batch ถัดไปที่ยังไม่ถึงคิวค้างอยู่ ตัวเลขนี้จะนับยอดคงเหลือต่ำกว่าความจริง
        if qty_left > 0.5:
            _skip_qty_short.append(_unplanned_row(
                order,
                # S9 Only ที่ถูกหยุดกลางลูป (ห้ามทอในบ้าน) → บอกเหตุผลจริง
                _s9o_stop or "วางแผนไม่ครบ (เครื่องไม่พอ/หมดสัปดาห์ที่วางได้/ชน safety guard)",
                qty=qty_left,
            ))
        # 🔧 FIX: อัปเดต cumulative planned qty สำหรับ item นี้
        # ใช้ order_qty (ไม่ใช่ qty_left) เพราะ order ถูก process แล้วไม่ว่าผลิตครบหรือไม่
        _item_cumulative_planned[item] = _item_cumulative_planned.get(item, 0) + order_qty
        # บันทึก last plan week สำหรับ SC/SO+Item → บังคับ FG_WEEK ถัดไปเริ่มหลังนี้
        if _produced_week is not None:
            _lpw_idx = week_index(_produced_week)
            if _lpw_idx is not None:
                _prev = _last_fg_plan_idx.get((sc_so_no, item))
                if _prev is None or _lpw_idx > _prev:
                    _last_fg_plan_idx[(sc_so_no, item)] = _lpw_idx

    # =========================
    # CAPACITY OPTIMIZATION
    # =========================
    # จบลูปวางแผนแล้ว → ล้างธง "ออร์เดอร์ที่กำลังวางเป็นก้อนสี" ไม่งั้นด่านหลังลูป
    # (เก็บตก capacity) จะยังมองเห็นเครื่องที่ยึดได้ของออร์เดอร์สุดท้ายค้างอยู่
    _grab_txn_abort("จบลูปวางแผน")
    _CUR_ORDER_COLOR_PART = False
    # Optimize machine utilization by filling unused capacity with same-item orders from different SCs
    plans = detect_and_fill_unused_capacity(plans, orders, summary_mc)
    return plans

# =========================
# ซ่อมแผนท้ายรอบ — carry ข้ามสัปดาห์ที่เว้นต้องมีเครื่องจอดว่างจริงกลาง gap
#
# กติกาตอนวาง (_carry_cap_by_gap) เช็คพูล ณ ตอนวาง item นั้น แต่ item ที่วาง "ทีหลัง"
# มายึดสัปดาห์กลางจนเต็มได้ → carry ที่อนุมัติไปแล้วกลายเป็นเครื่องผี
# (เช่น W36 ทอ 2 เครื่อง, W37 เครื่องถูก item อื่นใช้หมด, W38 ยังอ้างว่า carry ต่อ)
# รอบซ่อมนี้จึงตรวจแผนสุดท้ายอีกครั้งด้วยพูลที่สะสมครบทุก item แล้ว
#
# เจอเครื่องผี → ย้ายจาก CARRYOVER_MC ไป NEW_MC (เสียวัน setup + กินโควตา job setup)
# วันผลิตที่หายไปชดเชยด้วยการเพิ่มเครื่องได้ไม่เกิน MAX_NEW_SETUP_MC และไม่เกินพูลที่ว่างจริง
# ที่ยังผลิตไม่ทัน → ตัดยอดออกจากสัปดาห์นั้น แล้วรายงานในชีท UNPLANNED
# =========================
GAP_REPAIR_MAX_ADD_MC = MAX_NEW_SETUP_MC

# ยอดที่วางไม่พอในสัปดาห์ที่ซ่อม ไหลต่อไปข้างหน้าได้ไกลสุดกี่สัปดาห์ (กันวนไม่รู้จบ)
GAP_REPAIR_MAX_SPILL_WEEKS = plan_config.get("KP_GAP_REPAIR_MAX_SPILL_WEEKS")


def _repair_prod_days_new(wd, setup_days):
    """วันผลิตของเครื่องที่เพิ่ง setup — สูตรเดียวกับลูปหลัก (Planning.py prod_days_new)"""
    if wd < setup_days:
        return max(0, wd - 0.5)
    return max(0.5, wd - setup_days)


def _repair_charge_pool(mc_group, gauge, week, item_code, n_mc):
    """หักเครื่องที่เพิ่มระหว่างซ่อมออกจากพูล เพื่อไม่ให้แถวที่ซ่อมทีหลังใช้เครื่องตัวเดิมซ้ำ
    (กติกาเดียวกับตอนบันทึกแผนในลูปหลัก: (Type_1, gauge) + pool label + sub-pool MC Special)"""
    if n_mc <= 0:
        return
    _mc_r, _g_r = _apply_mc_redirect(str(mc_group).strip().upper(), gauge)
    _sp_type = _get_subgroup_by_item_prefix(_mc_r, _g_r, item_code)
    if _sp_type:
        _f = _mc_to_factory(_mc_r, _g_r)
        _cat = _mc_to_type1(_mc_r, _g_r)
        _k = (_f, _cat, _g_r, int(week), _sp_type)
        _mc_special_weekly_usage[_k] = _mc_special_weekly_usage.get(_k, 0) + n_mc
        return
    _key = (_mc_to_type1(_mc_r, _g_r), _g_r)
    weekly_new_plan_usage.setdefault(int(week), {})[_key] = (
        weekly_new_plan_usage.setdefault(int(week), {}).get(_key, 0) + n_mc
    )
    _pool = _mc_to_pool_label(_mc_r, _g_r)
    if _pool:
        weekly_new_plan_pool_usage.setdefault(int(week), {})[_pool] = (
            weekly_new_plan_pool_usage.setdefault(int(week), {}).get(_pool, 0) + n_mc
        )


def _is_outsource_row(row) -> bool:
    """แถวนี้เป็นงานจ้างทอ (S9 / COMKN) ไหม — เครื่องอยู่ที่ผู้รับจ้าง ไม่ใช่พูลของโรงงานเรา

    รอบซ่อมที่คิดจาก "เครื่องในบ้าน" ต้องข้ามแถวพวกนี้ทั้งหมด เพราะ
      - พูล COMKN ในระบบเรามี 0 เครื่อง (อยู่ใน _BOOKING_EXCLUDE_MC ด้วย)
        → get_actual_mc_remain() ติดลบเสมอ แล้วรอบซ่อมพูลจะตัดงานจ้างทอทิ้งทั้งหมด
      - _get_type_key_for_mc("COMKN") ไม่พบใน master_mc → คืนค่า default "PHET_DOUBLE"
        → งานจ้างทอไปกินโควตา setup ของโรงเพชรทั้งที่ไม่ได้ตั้งเครื่องในบ้านเลย
    เพดานของงานจ้างทอคือ Total MC ในชีท MC S9 ซึ่งคุมด้วย _s9_weekly_usage ตอนวางแผนแล้ว
    """
    # ธง S9_ROUTING คือตัวชี้ขาด — ตั้งแต่ 16 ก.ย. 2569 MC_GROUP ของแถวจ้างทอเป็น "ชื่อผู้รับจ้าง"
    # (เซินเปา / วันนิตติ้ง / …) ไม่ใช่ "COMKN" อีกแล้ว ถ้าดูแค่ MC_GROUP จะมองไม่เห็นว่าเป็นงานจ้างทอ
    # แล้วรอบซ่อมจะตัดงานจ้างทอทิ้ง + งานจ้างทอไปกินโควตา setup ของโรงเรา
    # ⚠️ ค่าว่างมาได้ 2 หน้าตา: "" (แถวที่เป็น dict ตอนวางแผน) และ NaN (แถวที่อ่านจาก DataFrame
    # เช่น plan_frame ใน _new_setup_rows / _build_remaining_df) — NaN เป็น truthy แล้ว
    # str(NaN) = "nan" ซึ่งไม่ใช่สตริงว่าง → เกณฑ์เดิมตีว่า "มีชื่อผู้รับจ้าง" ทุกแถว
    # (เคสจริง 21 ก.ย. 2569: SETUP_TRACKING ฝั่ง NEW 323 แถวกลายเป็น TYPE=OUTSOURCE ทั้งหมด
    #  แล้วแถบ Set up Job/Week โชว์ W41 PHET_DOUBLE = 1 ทั้งที่ตั้งเครื่องจริง 32)
    def _blank(v) -> bool:
        return v is None or str(v).strip().lower() in ("", "nan", "none", "nat")

    _f = row.get("S9_ROUTING")
    if isinstance(_f, bool):
        if _f:
            return True
    elif not _blank(_f):
        _fs = str(_f).strip().lower()
        # ไฟล์แผนเขียนธงนี้เป็นตัวเลข (0.0 / 1.0) — "1.0" ไม่เท่ากับ "1" จึงต้องอ่านเป็นเลขด้วย
        try:
            if float(_fs) != 0:
                return True
        except ValueError:
            if _fs in ("true", "yes", "1"):
                return True
    if not _blank(row.get("S9_SUPP", "")):
        return True
    # ไฟล์แผนรุ่นเก่า (ก่อน 16 ก.ย. 2569) ยังเขียน MC_GROUP = COMKN — ต้องอ่านออกต่อไป
    return str(row.get("MC_GROUP", "")).strip().upper() == "COMKN"


def _repair_gap_carry(plan_rows) -> int:
    """ตรวจ+ซ่อม carry ข้าม gap ที่ไม่มีเครื่องจอดว่างจริง — คืนจำนวนสัปดาห์ที่ซ่อม"""
    if not plan_rows:
        return 0

    # จัดแถวเป็นลูกโซ่ (item + กลุ่มเครื่อง + เกจ) → สัปดาห์ → แถว
    chains: dict = {}
    for _r in plan_rows:
        _item = str(_r.get("ITEM_CODE", "")).strip()
        _mc = str(_r.get("MC_GROUP", "")).strip().upper()
        _g = _normalize_gauge(_r.get("MC_GUAGE", ""))
        if not _item or not _mc:
            continue
        if _is_outsource_row(_r):
            continue  # จ้างทอ: เครื่องอยู่ที่ผู้รับจ้าง — ไม่มีเครื่องในพูลเราให้ตรวจว่าจอดว่างไหม
        _widx = _r.get("PLAN_IDX")
        if _widx is None or (isinstance(_widx, float) and pd.isna(_widx)):
            _widx = week_pos(_r.get("PLAN_WEEK"))
        if _widx is None:
            continue
        chains.setdefault((_item, _mc, _g), {}).setdefault(int(_widx), []).append(_r)

    _fixed_weeks = 0
    _cg_used = None                    # เครื่องที่แผนเดินต่อ (คีย์ก้อน, สัปดาห์) — สร้างเมื่อเจอแถว Hold
    _cg_hold = None                    # เครื่องที่แถว Hold ถือไว้ (คีย์ก้อน, สัปดาห์) — คู่กับ _cg_used
    for (_item, _mc, _g), _weeks in chains.items():
        _mc_key = _ck(_item, _mc, _g)
        _prev_idx = None
        _prev_total = 0
        _prev_is_hold = False
        # ไล่สัปดาห์แบบอ่านลำดับใหม่ทุกรอบ — รอบซ่อมอาจแทรกแถวของลูกโซ่ในสัปดาห์ถัดไป
        # (ยอดที่ไหลต่อ) ถ้า iterate จาก snapshot เดิม สัปดาห์หลังจะมองไม่เห็นแถวที่เพิ่ง
        # เติมช่องว่าง แล้วเข้าใจผิดว่ายังมี gap → ตัด carry ที่มีเครื่องจริงทิ้ง
        _visited: set = set()
        for _ in range(len(_weeks) + GAP_REPAIR_MAX_SPILL_WEEKS + 5):
            _todo = [_w for _w in sorted(_weeks) if _w not in _visited]
            if not _todo:
                break
            _widx = _todo[0]
            _visited.add(_widx)
            _group = _weeks[_widx]
            # แถวที่ใช้เครื่องร่วมกับแถวอื่น (MC_SHARED) ไม่ประกาศเครื่องเอง — ไม่นับซ้ำ
            _owners = [_r for _r in _group if int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0) > 0]
            if not _owners:
                continue
            _total = max(int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0) for _r in _owners)
            _carry = max(int(pd.to_numeric(_r.get("CARRYOVER_MC", 0), errors="coerce") or 0) for _r in _owners)
            _week = int(pd.to_numeric(_owners[0].get("PLAN_WEEK"), errors="coerce") or 0)

            # แถว Hold รอสี: กันได้ไม่เกินเครื่องที่ item นี้เดินจริง "สัปดาห์ก่อน" ในแผนตอนนี้
            # Hold นับสัปดาห์ก่อนไว้ตอนสร้าง แต่งานสี item อื่นยึดเครื่องสัปดาห์ก่อนทีหลังได้ → กันเกินเครื่องอุ่นจริง
            # (เคสจริง 14 ก.ย. 2569 FD5GNTLM70/12A0 RAOO: W42 เดิน 2 → Hold W43 กัน 2 → FD1BASMK32/01B0
            #  ยึด W42 เหลือ 1 แต่ Hold ยังกัน 2 แล้ว W44 carry 2) ; ลดตรงนี้ก่อนด่านพูล/โควตา job
            # → สัปดาห์ถัดไปถูกด่าน _after_hold ด้านล่างจำกัด carry แล้วแปลงส่วนเกินเป็น setup ใหม่ตามโควตา
            if all(_is_hold_row(_r) for _r in _owners):
                if _cg_used is None:
                    _cg_used = _color_gap_mc_by_week(plan_rows)
                    _cg_hold = _hold_mc_by_week(plan_rows)
                _cg_k = _color_gap_key(_item, _mc, _g)
                _warm = _cg_used.get((_cg_k, _widx - 1), 0) + int(
                    booking_mc_by_week.get(_cg_k, {}).get(_widx - 1, 0) or 0
                )
                # สัปดาห์ก่อนเป็นแถว Hold เองด้วย (โซ่ Hold รอ target ของ core กันติดกันหลายสัปดาห์)
                # → เครื่องชุดนั้นจอดอุ่นอยู่จริงและถูกหักพูลไปแล้ว ต้องนับเป็นเครื่องอุ่น
                # ไม่งั้นสัปดาห์กลางโซ่เห็นงานจริง 0 แล้วโซ่ถูกไล่ตัดทิ้งทั้งเส้น
                _warm = max(_warm, int(_cg_hold.get((_cg_k, _widx - 1), 0) or 0))
                if _total > _warm:
                    for _r in _owners:
                        _r["ACTUAL_MC"] = int(_warm)
                        _r["CARRYOVER_MC"] = int(_warm)
                        _r["NEW_MC"] = 0
                        _r["SETUP_DAYS"] = 0
                    _repair_release_pool(_mc, _g, _week, _item, int(_total) - int(_warm))
                    print(f"   ⏸ [{_hold_remark_of(_owners[0])}] {_item} {_mc} G{_g} W{_week}: "
                          f"สัปดาห์ก่อนเดินจริง {_warm} เครื่อง → ลดที่กันไว้ {_total} → {_warm}")
                    # สมุดเครื่อง Hold ต้องสดตาม — สัปดาห์ถัดไปของโซ่อ่านค่านี้เป็นเครื่องอุ่นของมัน
                    _cg_hold[(_cg_k, _widx)] = int(_warm)
                    _total = _carry = int(_warm)
                    if _total <= 0:
                        continue       # ไม่ได้ถือเครื่องแล้ว → ไม่นับเป็นสัปดาห์ที่เดินเครื่องของลูกโซ่

            # สัปดาห์แรกของลูกโซ่ carry มาจากนอกแผน (booking/แผนรอบก่อน) — ไม่ตรวจย้อนหลัง
            _gap = (_widx - _prev_idx) if _prev_idx is not None else None
            # ต่อจากแถวกันเครื่องรอสี (Color Gap): สัปดาห์ติดกันก็ต้อง carry ไม่เกินเครื่องที่กันไว้จริง
            # (เคสจริง 14 ก.ย. 2569 FD5GNTLM70/12A0 RAOO: Hold W43 ถูกลดเหลือ 1/0 แต่ W44 ยัง carry 2)
            # จำกัดเฉพาะหลังแถว Hold — สัปดาห์ติดกันทั่วไปหลายใบสั่งแบ่งเครื่องกัน (MC_SHARED)
            # max ของ ACTUAL_MC จึงต่ำกว่าเครื่องจริง ถ้าตรวจด้วยเกณฑ์นี้จะซ่อมผิด
            _after_hold = _gap == 1 and _prev_is_hold
            if _gap is not None and _carry > 0 and (_gap >= 2 or _after_hold):
                if _gap >= 2:
                    _cap = _carry_cap_by_gap(_item, _mc_key, _mc, _g, _prev_idx, _widx)
                    if _cap is None:
                        _cap = _carry
                    # carry เกินเครื่องที่ลูกโซ่มีจริงในสัปดาห์ก่อนหน้าไม่ได้
                    _cap = max(0, min(int(_cap), int(_prev_total) if _prev_total else int(_cap)))
                else:
                    _cap = max(0, int(_prev_total))
                if _carry > _cap:
                    # แถวกันเครื่องรอสี (Color Gap): ไม่ได้ผลิตและไม่ได้ตั้งเครื่อง — carry ไม่ถึง
                    # ก็แค่ "กันได้น้อยลง" ห้ามแปลงเป็น setup ใหม่ (เสียวัน setup + กินโควตา job
                    # ฟรีทั้งที่สัปดาห์นั้นไม่มีงานเดินเลย ; เจอจริงตอนทดสอบ 9 ก.ย. 2569
                    # FD4360PN48/03A0 RAP98 W44 → ถูกเขียนเป็น setup 4 ตัว 12 วัน)
                    if all(_is_hold_row(_r) for _r in _owners):
                        _rel = max(0, int(_total) - int(_cap))
                        for _r in _owners:
                            _r["ACTUAL_MC"] = int(_cap)
                            _r["CARRYOVER_MC"] = int(_cap)
                            _r["NEW_MC"] = 0
                            _r["SETUP_DAYS"] = 0
                            _r["REMARK"] = _hold_remark_of(_r)
                        if _cg_hold is not None:
                            _cg_hold[(_color_gap_key(_item, _mc, _g), _widx)] = int(_cap)
                        if _rel > 0:
                            _repair_release_pool(_mc, _g, _week, _item, _rel)
                            print(f"   ⏸ [{_hold_remark_of(_owners[0])}] {_item} {_mc} G{_g} W{_week}: "
                                  f"เครื่องอุ่นไม่ถึง → ลดที่กันไว้ {_total} → {_cap} เครื่อง")
                        # กันไว้เหลือ 0 = สัปดาห์นี้ลูกโซ่ไม่ได้ถือเครื่องเลย → ไม่นับเป็นสัปดาห์ที่เดินเครื่อง
                        # สัปดาห์ถัดไปจะตรวจย้อนข้ามไปถึงสัปดาห์ที่ทอจริง (รวมพูลของสัปดาห์กลาง)
                        if int(_cap) > 0:
                            _prev_idx = _widx
                            _prev_total = int(_cap)
                            _prev_is_hold = True
                        continue
                    _fixed_weeks += 1
                    # ส่ง timeline + ลูกโซ่ + list แผน เข้าไปด้วย เพื่อให้ยอดที่ผลิตไม่ทัน
                    # ไหลไปวางต่อสัปดาห์ถัดไปได้ (แทนการทิ้งเป็น UNPLANNED)
                    _repair_one_week(
                        _item, _mc, _g, _week, _owners, _total, _carry, _cap, _prev_idx,
                        week_idx=_widx, chain_weeks=_weeks, plan_rows=plan_rows,
                    )
                    _total = max(
                        int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0) for _r in _owners
                    )

            _prev_idx = _widx
            _prev_total = _total
            _prev_is_hold = all(_is_hold_row(_r) for _r in _owners)

    return _fixed_weeks


def _warm_mc_prev_week(plan_rows, item, mc_group, gauge, week_idx) -> int:
    """เครื่องที่ item นี้เดินอยู่ "สัปดาห์ก่อนหน้า" และยังว่างพอจะทอต่อได้โดยไม่ต้อง setup ซ้ำ

    เครื่องที่เพิ่งทอ item เดียวกันสัปดาห์ก่อน = Cylinder/เข็มตรงอยู่แล้ว ต่อด้ายทอต่อได้เลย
    ลูปวางแผนหลักรู้เรื่องนี้ (จาก last_production ที่ preload มาจาก booking) แต่เส้นทางซ่อม/
    ไหลยอดเดิมรู้แค่ "เครื่องที่รอดจากการถูกตัด" → ไปตั้งเครื่องใหม่ทั้งที่ยังมีเครื่องอุ่นอยู่
    (เจอกับ FD3BASLA49/01B0 W39: booking เดิน 1 เครื่องอยู่ W38 แต่แผนสั่ง setup ใหม่ 3 วัน)

    ย้อนหาสัปดาห์ล่าสุดที่ item นี้เดินเครื่องจริงได้ไม่เกิน SETUP_GAP_WEEK (กติกาเดียวกับลูปหลัก)
    ถ้าเว้นสัปดาห์ ต้องผ่าน _carry_cap_by_gap() ก่อน — เครื่องจะยังอุ่นข้าม gap ได้ก็ต่อเมื่อ
    "จอดว่างอยู่จริงตลอดสัปดาห์ที่ไม่ได้ผลิต" ถ้า item อื่นยึดไปใช้แล้วต้อง setup ใหม่
    (เช่น มี 10 เครื่อง W1: A5 B3 C2 → W2: A5 B5 C0 → W3 C กลับมาต้อง setup เพราะเครื่อง
     ของ C ถูก B เอาไปตั้งใหม่ที่ W2 แล้ว)

    สุดท้ายหักเครื่องที่ item นี้ถืออยู่แล้วในสัปดาห์เป้าหมายออก กันนับซ้ำ (เครื่องผี)
    """
    if week_idx is None or week_idx <= 0:
        return 0
    _ck_warm = _resolve_carry_key(item, mc_group, gauge)
    _it_u = str(item).strip().upper()
    _mc_u = str(mc_group).strip().upper()
    _g_u = _normalize_gauge(gauge)

    # เครื่องที่ item นี้เดินอยู่รายสัปดาห์ (booking + แถวแผน)
    _runs: dict = {}
    for _w, _n in (booking_mc_by_week.get(_ck_warm, {}) or {}).items():
        try:
            _runs[int(_w)] = max(_runs.get(int(_w), 0), int(_n or 0))
        except (TypeError, ValueError):
            continue
    for _r in plan_rows or []:
        if str(_r.get("ITEM_CODE", "")).strip().upper() != _it_u:
            continue
        if str(_r.get("MC_GROUP", "")).strip().upper() != _mc_u:
            continue
        if _normalize_gauge(_r.get("MC_GUAGE")) != _g_u:
            continue
        _pw = int(pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce") or 0)
        if _pw <= 0:
            continue
        _wi = week_index(_pw)
        if _wi is None:
            continue
        # แถวแผนเขียนเครื่องของทั้งบล็อกซ้ำทุกแถว → ใช้ max ไม่ใช่บวก
        _runs[_wi] = max(_runs.get(_wi, 0), int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0))

    _used_now = _runs.get(week_idx, 0)
    _prev_idx = None
    for _wi in range(week_idx - 1, max(-1, week_idx - 1 - SETUP_GAP_WEEK), -1):
        if _runs.get(_wi, 0) > 0:
            _prev_idx = _wi
            break
    if _prev_idx is None:
        return 0                       # ไม่ได้เดินเครื่องมาเลยในช่วงที่ยังอุ่น → ต้อง setup ใหม่

    _warm = _runs[_prev_idx]
    if week_idx - _prev_idx >= 2:
        _cap = _carry_cap_by_gap(item, _ck_warm, mc_group, gauge, _prev_idx, week_idx)
        if _cap is not None:
            _warm = min(_warm, max(0, int(_cap)))
    return max(0, _warm - _used_now)


def _plan_order_key(row):
    """ตัวระบุ "ใบสั่ง 1 ใบ" ของแถวแผน — SC เดียวกันมีได้หลายใบ (คนละ PO / คนละ FG / คนละสี)

    นิยามเดียวกับ ORDER_KEY_COLS ของหน้าเว็บ (KnitPlan.jsx) ที่ใช้เทียบ "สั่ง vs วาง"
    → ยอดที่รอบซ่อมย้ายไปมาต้องอยู่ในใบเดิมเสมอ ไม่งั้นใบหนึ่งขึ้นขาด อีกใบขึ้นเกิน
    """
    def _s(_k):
        _v = row.get(_k, "")
        if _v is None or (isinstance(_v, float) and pd.isna(_v)):
            return ""
        return str(_v).strip()

    _oq = pd.to_numeric(row.get("ORDERS_QTY", 0), errors="coerce")
    return (
        _s("SC_SO_NO"), _s("SC_LINE_ID"), _s("PO_NO"),
        round(float(_oq), 2) if pd.notna(_oq) else 0.0,
        _s("FG_WEEK"), _s("SUB_COLOR"),
    )


def _repair_spill_forward(
    template, item, mc_group, gauge, start_idx, qty, carry_mc,
    chain_weeks, plan_rows, setup_per, daily, rev_w, allow_same_week=False,
    stop_idx=None, line_remark=None, no_new_mc=False,
):
    """ยอดที่วางไม่พอในสัปดาห์ที่ซ่อม → ไล่ไปวางต่อสัปดาห์ถัดไป จนหมดยอด

    คืน (ยอดที่วางต่อได้, ยอดที่ยังวางไม่ได้, ข้อความสรุปว่าไปลงสัปดาห์ไหนบ้าง)

    กติกาต่อสัปดาห์ (ไล่ทีละสัปดาห์บน timeline):
      1. ลูกโซ่มีแถวอยู่แล้วในสัปดาห์นั้น → เติมยอดลง "กำลังเครื่องที่ยังว่าง" ของแถวนั้นก่อน
      2. ยังไม่หมด → ขอเครื่องเพิ่ม: ใช้เครื่องที่ carry มาก่อน (ไม่กินโควตา setup)
         ไม่พอค่อยตั้งเครื่องใหม่เท่าที่พูลว่างและโควตา job/week ยังเหลือ
      3. ไม่มีแถวเลย → สร้างแถวใหม่ของลูกโซ่ในสัปดาห์นั้น (carry + new ตามข้อ 2)
    เครื่องทุกเครื่องที่ใช้ต้องหักพูลของสัปดาห์นั้นเสมอ (พูลคิดแยกรายสัปดาห์)
    ส่วนโควตา job หักเฉพาะเครื่องที่ตั้งใหม่จริง

    stop_idx    = สัปดาห์สุดท้าย (index) ที่ยอมให้วาง — เลยจากนี้หยุด ไม่ไล่ต่อ
                  ใช้โดยรอบซ่อมความต่อเนื่อง (_repair_run_continuity) ที่ต้องเติมยอดเฉพาะ
                  ช่วงสัปดาห์ที่เว้น หรือเติมทีละสัปดาห์ตอนขยับงานให้ทอเร็วขึ้น
    line_remark = ข้อความ LINE_REMARK ของแถวที่สร้างใหม่ (ไม่ระบุ = ข้อความของ gap repair)
    no_new_mc   = ห้ามตั้งเครื่องใหม่ ใช้ได้เฉพาะเครื่องที่เดินอยู่/ยังอุ่นเท่านั้น
                  (รอบเกลี่ยกระดานใช้โหมดนี้ — ย้ายงานมาให้ทันได้โดยไม่เพิ่มค่า setup)
    """
    _remaining = float(qty)
    _carry = max(0, int(carry_mc))
    _placed_total = 0.0
    _trace = []

    def _num(_r, _k):
        return float(pd.to_numeric(_r.get(_k, 0), errors="coerce") or 0)

    def _fit(want, cap):
        """ยอดที่ใส่ได้จริง — ถ้าใส่ได้ไม่หมดให้ปัดลงตามน้ำหนักม้วน (ผลิตครึ่งม้วนไม่ได้)

        ⚠️ ยกเว้นเศษที่ขาดไป "ไม่ถึง 1 ม้วน" = ก้อนสุดท้ายของยอดใบนั้น → ใส่ให้ครบ ห้ามปัดทิ้ง
        เศษขนาดนั้นแยกไปทอสัปดาห์อื่นไม่ได้จริง (ต้องกินเครื่องทั้งตัว) เดิมมันถูกทิ้งไว้แล้ว
        ไหลไปเกาะแถวของใบอื่นในสัปดาห์ถัดไป → ใบนี้ขึ้น "วางไม่ครบ" ส่วนใบนั้นขึ้น "เกิน"
        (เคสจริง 16 ก.ย. 2569 FD4DRTPG03/09A0 SC 715028: ใบ FG 202609 ขาด 1.21 / FG 202610 เกิน 1.21)
        """
        want = float(want)
        v = min(want, float(cap))
        if v <= 0:
            return 0.0
        if rev_w > 0 and v + 1e-6 < want:
            if want - v < rev_w:
                return want            # ขาดไม่ถึง 1 ม้วน → ปิดยอดใบนี้ให้ครบ
            v = int(v // rev_w) * rev_w
        return max(0.0, v)

    # แถวแผน "ทุกใบ" ของลูกโซ่นี้ (item + เครื่อง + เกจ) แยกตามสัปดาห์
    # chain_weeks ที่ผู้เรียกส่งมาถูกกรองเหลือใบเดียว → ใช้ตัวนั้นคำนวณกำลังเครื่องที่ว่างไม่ได้
    # (มองไม่เห็นยอดของใบอื่นที่นั่งเครื่องชุดเดียวกันในสัปดาห์นั้น แล้วจะวางเกินกำลังจริง)
    _chain_key = _preempt_row_key(template)
    _chain_all_weeks = {}
    for _r0 in plan_rows:
        if _preempt_row_key(_r0) != _chain_key:
            continue
        _ri0 = _preempt_row_idx(_r0)
        if _ri0 is not None:
            _chain_all_weeks.setdefault(int(_ri0), []).append(_r0)

    # ใบสั่งของยอดก้อนนี้ — ยอดต้องลงแถวของใบนี้เท่านั้น ห้ามเทลงแถวของใบอื่น
    _my_order = _plan_order_key(template)

    def _shared_row(_wk_, _idx_, _host, _mc_n, _days_, _wd_):
        """เปิดแถวของใบเราในสัปดาห์ที่มีเครื่องของ item นี้เดินอยู่แล้ว (ของใบอื่น)

        ประกาศเครื่อง 0 ตัว + MC_SHARED = เครื่องของแถวเจ้าของ — กติกาเดียวกับทาง
        "เติมเข้าเครื่องเดิม" ของลูปหลัก เพื่อไม่ให้บัญชีพูล/Gantt นับเครื่องชุดเดียวกันซ้ำ
        """
        _r_new = dict(template)
        _r_new.update({
            "PLAN_WEEK": _wk_,
            "PLAN_IDX": _idx_,
            "PRODUCE_QTY": 0.0,
            "PLAN_QTY": 0.0,
            "ACTUAL_MC": 0,
            "REQUIRED_MC": 0,
            "CARRYOVER_MC": 0,
            "NEW_MC": 0,
            "MC_SHARED": int(_mc_n),
            "MC_BOOKING": 0,
            "SETUP_DAYS": 0,
            "AVAILABLE_DAYS": _days_,
            "ACTUAL_WORKING_DAYS": _wd_,
            "FACTORY_WORKING_DAYS": _wd_,
            "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(_wk_)),
            "REMARK": (
                f"เติมเข้าเครื่องเดิม — ใช้เครื่อง {int(_mc_n)} ตัวร่วมกับ SO "
                f"{str(_host.get('SC_SO_NO', '')).strip()} ({mc_group}) ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup"
            ),
            "LINE_REMARK": line_remark or "ไหลต่อจากสัปดาห์ที่ซ่อม carry (gap repair)",
        })
        plan_rows.append(_r_new)
        chain_weeks.setdefault(_idx_, []).append(_r_new)
        _chain_all_weeks.setdefault(_idx_, []).append(_r_new)
        return _r_new

    # allow_same_week: เริ่มลองที่ "สัปดาห์ที่เพิ่งถูกตัด" ก่อน แล้วค่อยไล่ไปข้างหน้า
    # ใช้กับ cascade ที่ตัดเครื่องเพราะลูกโซ่ขาดต้นทาง (ไม่ใช่เพราะสัปดาห์นั้นเครื่องเต็ม)
    # → สัปดาห์นั้นอาจยังมีเครื่องว่าง/เครื่องอุ่นรับงานได้ ไม่ต้องดันออกไปอีกสัปดาห์
    # (เจอกับ FD3BASLA49/01B0: W39 พูลว่าง 2 มีเครื่องอุ่นจาก W38 แต่ยอดถูกดันไป W40)
    _idx0 = int(start_idx) - 1 if allow_same_week else int(start_idx)
    _steps = GAP_REPAIR_MAX_SPILL_WEEKS + (1 if allow_same_week else 0)
    # ไล่ 2 รอบ: รอบแรก "ห้ามตั้งเครื่องใหม่เพื่อเศษ" ในสัปดาห์ที่งานเดียวกันเดินเครื่องเต็มอยู่แล้ว
    # → ปล่อยยอดไหลไปหาสัปดาห์ที่ยังมีที่ว่างบนเครื่องเดิมก่อน (ไม่เสียวัน setup เปล่า)
    # ไล่จนจบแล้วยังเหลือ → รอบสองยอมตั้งเครื่องใหม่ เพราะยอดต้องไม่ค้าง (ห้ามทิ้งเศษ)
    _pass, _deferred = 0, False
    _idx = _idx0
    for _ in range(_steps * 2):
        if _remaining <= 1e-6:
            break
        _idx += 1
        if (
            _idx > _idx0 + _steps                                   # ครบจำนวนสัปดาห์ที่ไล่ได้
            or _idx >= len(calendar_week)                           # หมด horizon ปฏิทิน
            or (stop_idx is not None and _idx > int(stop_idx))      # เลยกรอบที่ผู้เรียกอนุญาต
        ):
            if _pass == 0 and _deferred:
                _pass, _idx, _carry = 1, _idx0, max(0, int(carry_mc))
                continue
            break
        _wk = int(calendar_week.iloc[_idx]["WEEK"])
        # สัปดาห์เว้นรอสี (Color Gap): เครื่องที่นั่นถูกกันไว้เฉย ๆ ห้ามไหลยอดเข้าไป
        # ไม่งั้นแถว HOLD (AVAILABLE_DAYS = 0) จะถูกเติมยอดจนกลายเป็นสัปดาห์ผลิตปกติ
        if _color_gap_idx(item, mc_group, gauge) == _idx:
            continue
        _wd = float(get_working_days_by_factory(mc_group, max(1, _carry), week=_wk, gauge=gauge) or 0)
        if _wd <= 0:
            continue                   # สัปดาห์หยุดทั้งสัปดาห์
        _prod_new = _repair_prod_days_new(_wd, setup_per)

        # แถวของสัปดาห์นี้: ทุกใบ (ใช้คิดกำลังเครื่อง) กับเฉพาะใบเรา (ที่ยอดจะลงไป)
        _week_rows = _chain_all_weeks.get(_idx, [])
        _rows_here = [_r for _r in _week_rows if int(_num(_r, "ACTUAL_MC")) > 0]
        _mine = [_r for _r in _week_rows if _plan_order_key(_r) == _my_order]
        # ⚠️ ยอดของใบเราต้องลงแถวของใบเราเท่านั้น — เดิมหยิบ "แถวแรกของสัปดาห์นั้น"
        # ซึ่งเป็นของใบอื่นได้ (SC เดียวกันแต่คนละ FG/PO) → ใบเราขาด ใบนั้นเกิน
        _owner = next((_r for _r in _mine if int(_num(_r, "ACTUAL_MC")) > 0), None)
        if _owner is None and _mine:
            _owner = _mine[0]          # ใบเรามีแถวแล้วแต่เป็นแถวที่ใช้เครื่องร่วม (MC_SHARED)
        _placed_here = 0.0

        # ── 1) เติมลงกำลังเครื่องที่ยังว่างของแถวเดิม ──────────────────────────
        if _rows_here:
            _mc_here = max(int(_num(_r, "ACTUAL_MC")) for _r in _rows_here)
            _days_here = _num(_owner if _owner is not None else _rows_here[0], "AVAILABLE_DAYS") or _wd
            # ยอดที่เครื่องชุดนี้รับไปแล้ว = ของ "ทุกใบ" ในสัปดาห์นั้น ไม่ใช่เฉพาะใบเรา
            _used_here = sum(_num(_r, "PRODUCE_QTY") for _r in _week_rows)
            _free = daily * _days_here * _mc_here - _used_here
            _put = _fit(_remaining, _free)
            if _put > 0:
                if _owner is None:
                    # ใบเรายังไม่มีแถวในสัปดาห์นี้ → เปิดแถวของใบเราแบบใช้เครื่องร่วม
                    _owner = _shared_row(_wk, _idx, _rows_here[0], _mc_here, _days_here, _wd)
                _owner["PRODUCE_QTY"] = _num(_owner, "PRODUCE_QTY") + _put
                _owner["PLAN_QTY"] = max(0.0, _num(_owner, "PLAN_QTY") - _put)
                _remaining -= _put
                _placed_here += _put
            _carry = _mc_here

        # ── 2) ขอเครื่องเพิ่ม (carry ก่อน → ไม่พอค่อย setup ใหม่ตามโควตา) ──────
        if _remaining > 1e-6:
            _pool_free = int(get_actual_mc_remain(mc_group, _wk, gauge, item_code=item) or 0)
            # เครื่องของ item นี้เดินอยู่แล้วในสัปดาห์นี้ (ใบไหนก็ตาม) → เครื่องชุดนั้นถูกนับไปแล้ว
            # ห้าม carry ซ้ำ (เดิมเช็คแค่ "ใบเรามีแถวไหม" → ใบอื่นถือเครื่องอยู่ก็ยัง carry ทับ)
            if _rows_here:
                _carry_use = 0
            else:
                # เครื่องที่ item นี้เพิ่งเดินสัปดาห์ก่อน (แผนหรือ booking) ยังอุ่นอยู่ → ทอต่อได้
                # โดยไม่ต้อง setup ไม่กินโควตา job — เดิมนับเฉพาะเครื่องที่รอดจากการถูกตัด
                # ⚠️ ต้องหยิบ "เท่าที่ยอดต้องใช้" เท่านั้น ไม่งั้นไปกินเครื่องในพูลเกินจำเป็น
                # แล้วงานอื่นในสัปดาห์นั้นหาเครื่องไม่ได้ (รอบทดสอบ: เคส carry 21 เครื่อง
                # ทำให้ 3 SC จบช้าลง 1-2 สัปดาห์)
                _warm_prev = _warm_mc_prev_week(plan_rows, item, mc_group, gauge, _idx)
                _need_mc = 1
                if daily * _wd > 0:
                    _need_mc = max(1, int(-(-_remaining // (daily * _wd))))     # ceil
                _carry_use = min(max(_carry, min(_warm_prev, _need_mc)), _pool_free)
                if _carry_use > _carry:
                    print(
                        f"   ♻️ [WARM CARRY] {item} {mc_group} W{_wk}: เครื่องเดิมจากสัปดาห์ก่อน "
                        f"{_warm_prev} ตัว → carry {_carry_use} เครื่อง (ไม่ต้อง setup)"
                    )
            _cap_carry = daily * _wd * _carry_use
            _need_more = _remaining - _cap_carry
            _new_use = 0
            # สัปดาห์ที่เพิ่งถูกตัด: อนุญาตแค่ carry เครื่องอุ่นกลับเข้ามา ห้ามตั้งเครื่องใหม่
            # เพราะการตัดเพิ่งคืนเครื่องเข้าพูล ถ้าปล่อยให้ setup ใหม่ทันทีจะไปหยิบเครื่อง
            # ของพูลย่อย (POLY/COTTON) ที่ item นี้ใช้ไม่ได้ → แผนใช้เครื่องเกินพูลจริง
            _same_week_retry = allow_same_week and _idx == int(start_idx)
            # เศษเล็ก ๆ ในสัปดาห์ที่งานเดียวกันเดินเครื่องอยู่แล้ว (แต่เต็ม) — ตั้งเครื่องใหม่ไม่คุ้ม
            # (ยอดที่เหลือน้อยกว่ากำลังที่ต้องจ่ายเป็นวัน setup) → รอบแรกข้ามไปหาสัปดาห์ที่มีที่ว่าง
            _skip_new = bool(_pass == 0 and _rows_here and _need_more < daily * setup_per)
            if _skip_new:
                _deferred = True
            if (
                _need_more > 1e-6 and daily * _prod_new > 0
                and not _same_week_retry and not no_new_mc and not _skip_new
            ):
                # นับ job จาก NEW_MC ของแผนปัจจุบัน (เกณฑ์เดียวกับชีท REMAINING_JOBS) —
                # weekly_job_usage เป็นตัวนับระหว่างวาง ซึ่งต่ำกว่าจริงหลังรอบซ่อมย้ายเครื่องไปมา
                _slots = _preempt_job_slots(plan_rows, _wk, mc_group)
                _want = int(-(-_need_more // (daily * _prod_new)))     # ceil
                # เพดานเครื่องใหม่ = โควตาที่ผ้านี้ยังเหลือในสัปดาห์นั้น (รวมที่ลูปหลักใช้ไปแล้ว)
                _new_use = max(0, min(
                    _want, _pool_free - _carry_use, _slots,
                    _item_setup_room(item, mc_group, gauge, _wk),
                ))
            _mc_use = _carry_use + _new_use
            if _mc_use > 0:
                _cap_add = daily * (_carry_use * _wd + _new_use * _prod_new)
                _put = _fit(_remaining, _cap_add)
                if _put > 0:
                    _repair_charge_pool(mc_group, gauge, _wk, item, _mc_use)
                    if _new_use > 0:
                        weekly_job_usage.setdefault(int(_wk), {})[mc_group] = (
                            weekly_job_usage.setdefault(int(_wk), {}).get(mc_group, 0) + _new_use
                        )
                        _item_setup_charge(item, mc_group, gauge, _wk, _new_use)
                    if _owner is not None:
                        # แถวเดิมมีอยู่แล้ว → เพิ่มเครื่องเข้าแถวนั้น (ไม่สร้างแถวซ้อน)
                        _owner["ACTUAL_MC"] = int(pd.to_numeric(_owner.get("ACTUAL_MC", 0), errors="coerce") or 0) + _new_use
                        _owner["NEW_MC"] = int(pd.to_numeric(_owner.get("NEW_MC", 0), errors="coerce") or 0) + _new_use
                        _owner["SETUP_DAYS"] = float(pd.to_numeric(_owner.get("SETUP_DAYS", 0), errors="coerce") or 0) + setup_per * _new_use
                        _owner["PRODUCE_QTY"] = float(pd.to_numeric(_owner.get("PRODUCE_QTY", 0), errors="coerce") or 0) + _put
                        _owner["PLAN_QTY"] = max(
                            0.0, float(pd.to_numeric(_owner.get("PLAN_QTY", 0), errors="coerce") or 0) - _put
                        )
                        _owner["REMARK"] = _build_plan_remark(
                            mc_group,
                            int(pd.to_numeric(_owner.get("CARRYOVER_MC", 0), errors="coerce") or 0),
                            int(pd.to_numeric(_owner.get("NEW_MC", 0), errors="coerce") or 0),
                            float(pd.to_numeric(_owner.get("SETUP_DAYS", 0), errors="coerce") or 0),
                            int(pd.to_numeric(_owner.get("MC_BOOKING", 0), errors="coerce") or 0),
                        )
                        _carry = int(pd.to_numeric(_owner.get("ACTUAL_MC", 0), errors="coerce") or 0)
                    else:
                        _row = dict(template)
                        _row.update({
                            "PLAN_WEEK": _wk,
                            "PLAN_IDX": _idx,
                            "PRODUCE_QTY": _put,
                            "PLAN_QTY": max(0.0, _remaining - _put),
                            "ACTUAL_MC": _mc_use,
                            "REQUIRED_MC": _mc_use,
                            "CARRYOVER_MC": _carry_use,
                            "NEW_MC": _new_use,
                            "MC_SHARED": 0,
                            "MC_BOOKING": 0,
                            "SETUP_DAYS": setup_per * _new_use if _new_use > 0 else 0,
                            "AVAILABLE_DAYS": _prod_new if _new_use > 0 else _wd,
                            "ACTUAL_WORKING_DAYS": _wd,
                            "FACTORY_WORKING_DAYS": _wd,
                            "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(_wk)),
                            "REMARK": _build_plan_remark(
                                mc_group, _carry_use, _new_use,
                                setup_per * _new_use if _new_use > 0 else 0, 0,
                            ),
                            "LINE_REMARK": line_remark or "ไหลต่อจากสัปดาห์ที่ซ่อม carry (gap repair)",
                        })
                        plan_rows.append(_row)
                        chain_weeks.setdefault(_idx, []).append(_row)
                        _chain_all_weeks.setdefault(_idx, []).append(_row)
                        _carry = _mc_use
                    _remaining -= _put
                    _placed_here += _put

        if _placed_here > 0:
            _trace.append(f"W{_wk} {_placed_here:.0f} kg")

    _placed_total = float(qty) - _remaining
    return _placed_total, max(0.0, _remaining), ", ".join(_trace)


def _repair_one_week(item, mc_group, gauge, week, owners, total, carry_old, carry_cap, prev_idx,
                     week_idx=None, chain_weeks=None, plan_rows=None):
    """เปลี่ยน carry ที่ไม่มีเครื่องจริงให้เป็นเครื่อง setup ใหม่ของสัปดาห์นั้น + ปรับยอด/วันผลิตให้ตรง

    ยอดที่สัปดาห์นี้ผลิตไม่ทัน ไม่ทิ้งเป็น UNPLANNED ทันที — ส่งให้ _repair_spill_forward
    ไล่วางต่อในสัปดาห์ถัดไปก่อน เหลือจริง ๆ (หมดพูล/หมด horizon) ค่อยลง UNPLANNED
    """
    _first = owners[0]
    _wd = float(pd.to_numeric(_first.get("ACTUAL_WORKING_DAYS", 0), errors="coerce") or 0)
    if _wd <= 0:
        _wd = float(get_working_days_by_factory(mc_group, total, week=week, gauge=gauge) or 0)
    _daily = max(
        float(pd.to_numeric(_r.get("DAILY_CAPACITY", 0), errors="coerce") or 0) for _r in owners
    )
    _setup_per = get_setup_days_for_item(
        str(_first.get("MATERIAL_CONTENT", "") or ""),
        str(_first.get("YARN_USED", "") or ""),
        mc_group,
        item,
    )
    _rev_w = float(pd.to_numeric(_first.get("REVOLUTION_WEIGHT", 0), errors="coerce") or 0)

    _carry_new = int(carry_cap)
    _new_mc = max(0, int(total) - _carry_new)
    _need = sum(float(pd.to_numeric(_r.get("PRODUCE_QTY", 0), errors="coerce") or 0) for _r in owners)

    def _cap_of(c, n):
        return _daily * (c * _wd + n * _repair_prod_days_new(_wd, _setup_per))

    # ===== โควตา job/week: เครื่องที่แปลงจาก carry เป็น setup ใหม่ก็กินโควตาเหมือน setup ปกติ =====
    # รอบซ่อมนี้รันหลังลูปวางแผน (weekly_job_usage ปิดบัญชีไปแล้ว) ถ้าไม่เช็คตรงนี้
    # job ที่เพิ่มจะไม่ผ่าน check_job_capacity_limit เลย → แผนเกินโควตา setup ของสัปดาห์นั้น
    # _new_old = job ที่แถวนี้กินโควตาไปแล้วตอนวาง (ยังนับอยู่ใน weekly_job_usage)
    # _slots   = โควตาที่ยังว่างของ factory type นั้น → เพิ่มได้อีกเท่านี้
    _new_old = max(0, int(total) - int(carry_old))
    _slots = get_remaining_job_slots(week, mc_group)
    # carry หมด (0 เครื่อง) ต้องเหลืออย่างน้อย 1 เครื่อง ไม่งั้นแถวนี้ผลิตไม่ได้เลยทั้งที่งานยังอยู่
    _max_new = max(_new_old + _slots, 1 if _carry_new == 0 else 0)
    # setup ใหม่ต่อสัปดาห์ต้องไม่เกินโควตาของ "ผ้านี้" เหมือนลูปหลัก — ไม่ตัดของที่ลูปหลักอนุมัติไว้แล้ว (เช่น S9)
    # (เคสจริง 14 ก.ย. 2569 FD5GNTLM70/12A0 RAOO W44 หลัง Hold: carry 2→1 แปลงเป็น setup 3 + ชดเชย +1 = setup 4)
    # _new_old ถูกหักเข้าโควตาไปแล้วตอนวาง → บวกกลับก่อน ไม่งั้นกลายเป็นตัดของเดิมทิ้ง
    _max_new = min(_max_new, _new_old + _item_setup_room(item, mc_group, gauge, week))
    if _new_mc > _max_new:
        print(
            f"   🔒 [GAP CARRY] {item} {mc_group} G{gauge} W{week}: โควตา setup เต็ม "
            f"→ ลดเครื่องใหม่ {_new_mc} → {_max_new} (เดิมกินโควตา {_new_old} + ว่างอีก {_slots})"
        )
        _new_mc = _max_new

    # ชดเชยวัน setup ที่เสียไป — เพิ่มเครื่องได้ไม่เกิน MAX_NEW_SETUP_MC, ไม่เกินพูลที่ว่างจริง
    # และไม่เกินโควตา job/week ที่เหลือ
    _added = 0
    _pool_free = int(get_actual_mc_remain(mc_group, week, gauge, item_code=item) or 0)
    while (
        _cap_of(_carry_new, _new_mc) + 1e-6 < _need
        and _added < GAP_REPAIR_MAX_ADD_MC
        and _pool_free > 0
        and _new_mc < _max_new
    ):
        _new_mc += 1
        _added += 1
        _pool_free -= 1
    if _added > 0:
        _repair_charge_pool(mc_group, gauge, week, item, _added)

    # หักโควตา job ตามส่วนต่างที่เพิ่มจริง — ให้ตัวนับสอดคล้องกับแผนสุดท้าย
    # (แถวที่ซ่อมทีหลังในสัปดาห์เดียวกันจะเห็นโควตาที่เหลือถูกต้อง)
    _job_delta = _new_mc - _new_old
    if _job_delta > 0:
        weekly_job_usage.setdefault(int(week), {})[mc_group] = (
            weekly_job_usage.setdefault(int(week), {}).get(mc_group, 0) + _job_delta
        )
        _item_setup_charge(item, mc_group, gauge, week, _job_delta)
        if _job_delta > _slots:
            print(
                f"   ⚠️ [GAP CARRY] {item} {mc_group} G{gauge} W{week}: setup เกินโควตา "
                f"{_job_delta - _slots} job (carry หมดแต่งานยังต้องเดิน)"
            )

    _total_new = _carry_new + _new_mc
    _prod_days = _repair_prod_days_new(_wd, _setup_per) if _new_mc > 0 else _wd
    _capacity = _cap_of(_carry_new, _new_mc)

    # ยอดที่ผลิตไม่ทันหลังเสียวัน setup → ตัดออกจากสัปดาห์นี้ (ปัดลงตามน้ำหนักม้วน)
    _cut_total = 0.0
    if _capacity + 1e-6 < _need:
        # ตัดออกเป็นม้วนเต็ม — แต่ถ้าส่วนที่เกินกำลังไม่ถึง 1 ม้วน ให้ทอจบที่สัปดาห์นี้
        # (fold_keep_qty = กติกาเดียวกับทุกรอบซ่อม: ห้ามทิ้งเศษให้ไปเกาะแถวของใบอื่น)
        _keep = fold_keep_qty(_need, _capacity, _rev_w)
        _cut_total = max(0.0, _need - _keep)
        _ratio = (_keep / _need) if _need > 0 else 0
        for _r in owners:
            _q = float(pd.to_numeric(_r.get("PRODUCE_QTY", 0), errors="coerce") or 0)
            _q_new = _q * _ratio
            _cut = _q - _q_new
            _r["PRODUCE_QTY"] = _q_new
            # ยอดคงเหลือของ SC นั้นโตขึ้นตามที่ตัดออก
            _r["PLAN_QTY"] = float(pd.to_numeric(_r.get("PLAN_QTY", 0), errors="coerce") or 0) + _cut
            if _cut <= 0:
                continue

            # ยอดที่ตัดออก = งานที่ยังต้องทำ → ไล่วางต่อสัปดาห์ถัดไปก่อน (ไม่ทิ้งเป็น UNPLANNED)
            _spilled = 0.0
            _left = _cut
            _trace = ""
            if chain_weeks is not None and plan_rows is not None and week_idx is not None:
                _spilled, _left, _trace = _repair_spill_forward(
                    _r, item, mc_group, gauge, int(week_idx), _cut, _total_new,
                    chain_weeks, plan_rows, _setup_per, _daily, _rev_w,
                )
                if _spilled > 0:
                    # ยอดที่ไหลไปวางต่อได้แล้ว ไม่ใช่ยอดค้างของแถวนี้อีกต่อไป
                    _r["PLAN_QTY"] = max(
                        0.0, float(pd.to_numeric(_r.get("PLAN_QTY", 0), errors="coerce") or 0) - _spilled
                    )
                    print(
                        f"   ↪️ [GAP CARRY] {item} {mc_group} G{gauge} W{week}: ยอดที่ผลิตไม่ทัน "
                        f"{_cut:,.0f} kg → วางต่อ {_spilled:,.0f} kg ({_trace})"
                        + (f" | ค้าง {_left:,.0f} kg" if _left > 1e-6 else "")
                    )
            if _left > 1e-6:
                _skip_qty_short.append({
                    "SC_SO_NO": str(_r.get("SC_SO_NO", "")),
                    "ITEM_CODE": str(_r.get("ITEM_CODE", "")),
                    "MC_GROUP": str(mc_group),
                    "GAUGE": gauge,
                    "ORDERS_QTY": _r.get("ORDERS_QTY", ""),
                    "PENDING_PLAN": _r.get("PENDING_PLAN", ""),
                    "FG_WEEK": _r.get("FG_WEEK", ""),
                    "DATE_IN": _r.get("DATE_IN", ""),
                    "UNPLANNED_QTY": _left,
                    "REASON": (
                        f"W{week}: เครื่องที่ carry ข้ามสัปดาห์ที่เว้นถูก item อื่นใช้ไปแล้ว "
                        f"→ setup ใหม่ {_new_mc} เครื่อง เสียวัน setup {_setup_per} วัน ผลิตไม่ทัน"
                        + (f" ; วางต่อได้ {_spilled:,.0f} kg ({_trace}) ที่เหลือหมดพูลเครื่อง/โควตา"
                           if _spilled > 0 else " ; สัปดาห์ถัดไปไม่มีเครื่อง/โควตาเหลือ")
                    ),
                })

    # เขียนเครื่อง/วัน/หมายเหตุกลับทุกแถวที่ประกาศเครื่องเองในสัปดาห์นี้
    for _r in owners:
        _r["ACTUAL_MC"] = _total_new
        _r["CARRYOVER_MC"] = _carry_new
        _r["NEW_MC"] = _new_mc
        _r["SETUP_DAYS"] = _setup_per * _new_mc if _new_mc > 0 else 0
        _r["AVAILABLE_DAYS"] = _prod_days
        _r["REMARK"] = _build_plan_remark(
            mc_group, _carry_new, _new_mc, _setup_per * _new_mc if _new_mc > 0 else 0,
            int(pd.to_numeric(_r.get("MC_BOOKING", 0), errors="coerce") or 0),
        )

    _prev_week = (
        int(calendar_week.iloc[prev_idx]["WEEK"]) if prev_idx is not None and prev_idx < len(calendar_week) else "?"
    )
    print(
        f"[GAP CARRY FIX] {item} {mc_group} G{gauge} W{week}: carry {carry_old} → {_carry_new} "
        f"(สัปดาห์กลางว่าง {carry_cap}) | setup ใหม่ {_new_mc} เครื่อง"
        + (f" (+{_added} เครื่องชดเชย)" if _added else "")
        + (f" | ตัดยอด {_cut_total:.0f} kg" if _cut_total > 0 else "")
        + f" | ต่อจาก W{_prev_week}"
    )



def _repair_release_pool(mc_group, gauge, week, item_code, n_mc):
    """คืนเครื่องเข้าพูล — ตรงข้ามกับ _repair_charge_pool (ใช้ตอนปลดเครื่องออกจากแถวแผน)"""
    if n_mc <= 0:
        return
    _mc_r, _g_r = _apply_mc_redirect(str(mc_group).strip().upper(), gauge)
    _sp_type = _get_subgroup_by_item_prefix(_mc_r, _g_r, item_code)
    if _sp_type:
        _f = _mc_to_factory(_mc_r, _g_r)
        _cat = _mc_to_type1(_mc_r, _g_r)
        _k = (_f, _cat, _g_r, int(week), _sp_type)
        _mc_special_weekly_usage[_k] = max(0, _mc_special_weekly_usage.get(_k, 0) - n_mc)
        return
    _key = (_mc_to_type1(_mc_r, _g_r), _g_r)
    _wk_map = weekly_new_plan_usage.setdefault(int(week), {})
    _wk_map[_key] = max(0, _wk_map.get(_key, 0) - n_mc)
    _pool = _mc_to_pool_label(_mc_r, _g_r)
    if _pool:
        _pl_map = weekly_new_plan_pool_usage.setdefault(int(week), {})
        _pl_map[_pool] = max(0, _pl_map.get(_pool, 0) - n_mc)


def _build_color_budget(plan_rows) -> dict:
    """แบ่งว่าแต่ละแถวของแผน "เป็นงานสีกี่กิโล" ตามโควตาที่เปิดสีแล้วของ "ใบสั่งนั้น"

    กติกา (user 18 ส.ค. 2569): ยอดทอ 100 เปิดสีมา 50 → 50 กก. แรกคืองานสี ที่เหลือไม่ใช่
    จัดสรรโดยเรียงตามสัปดาห์เร็วก่อน (ยอดสีต้องได้ทอก่อน)

    ⚠️ จัดกลุ่มด้วย (SC, ITEM) ไม่ใช่ ITEM เดี่ยว (user 27 ส.ค. 2569) — OPEN_COLOR_LOSS
    เป็นยอดของใบสั่ง ถ้ารวมข้าม SC จะเอาโควตาของใบที่เปิดสีครบไปอุ้มใบที่ยังไม่เปิดสีเลย
    (แถวไม่มี SC → ตกไปกลุ่ม ("", ITEM) ซึ่งใช้ยอดรวมทุกใบสั่งของ item เป็นโควตา)

    ⚠️ แถวที่มีธง COLOR_PART (มาจาก _split_orders_by_color_quota ตอนจัดคิว) เชื่อธงนั้นเลย
    ไม่แจกโควตาใหม่ — ไม่งั้นการแจกรอบนี้ (สัปดาห์เร็วก่อน) จะยกสิทธิ์งานสีให้ "ก้อนส่วนเกิน"
    ที่บังเอิญถูกวางสัปดาห์เร็วกว่า แล้วก้อนสีจริงกลายเป็นงานไม่มีสีจนโดนยึดเครื่องทิ้ง
    แถวไม่มีธง (booking / แผนเก่า / รอบ replan) ยังใช้การแจกแบบเดิม

    คืน {id(row): กก.ที่เป็นงานสีของแถวนั้น}
    """
    _budget: dict = {}
    if not plan_rows:
        return _budget
    _by_key: dict = {}
    _used_by_flag: dict = {}      # โควตาที่แถวติดธงกินไปแล้ว ต่อ (SC, ITEM)
    for _r in plan_rows:
        _it = str(_r.get("ITEM_CODE", "")).strip().upper()
        if not _it or _it not in _COLOR_ITEMS:
            continue
        _gk = (",".join(ColorOpen.split_sc(_row_sc(_r))), _it)
        _flag = str(_r.get("COLOR_PART", "") or "").strip().upper()
        if _flag == "YES":
            _qy = float(pd.to_numeric(_r.get("PRODUCE_QTY", 0), errors="coerce") or 0)
            _budget[id(_r)] = _qy
            _used_by_flag[_gk] = _used_by_flag.get(_gk, 0.0) + _qy
            continue
        if _flag == "NO":
            _budget[id(_r)] = 0.0
            continue
        # คีย์กลุ่มใช้รูป normalize (แตกคอมมา เรียงคงลำดับ) — แถวที่เขียน SC คนละรูป
        # แต่ครอบใบเดียวกันจะได้อยู่กลุ่มเดียวกัน ไม่แบ่งโควตาซ้ำ
        _by_key.setdefault(_gk, []).append(_r)
    for (_sc, _it), _rows in _by_key.items():
        _left = _color_quota_of(_it, _sc)
        # ใบเดียวกันมีทั้งแถวติดธงและแถว booking/แผนเก่า → แถวไม่มีธงได้แค่โควตาที่เหลือ
        if 0 < _left < float("inf"):
            _left = max(0.0, _left - _used_by_flag.get((_sc, _it), 0.0))
        if _left <= 0:
            for _r in _rows:
                _budget[id(_r)] = 0.0        # ใบสั่งนี้ยังไม่เปิดสี → ถอดได้ทั้งก้อน
            continue
        if _left == float("inf"):
            for _r in _rows:
                _budget[id(_r)] = float(pd.to_numeric(_r.get("PRODUCE_QTY", 0), errors="coerce") or 0)
            continue
        for _r in sorted(_rows, key=lambda _x: (_preempt_row_idx(_x) if _preempt_row_idx(_x) is not None else 10**6)):
            _q = float(pd.to_numeric(_r.get("PRODUCE_QTY", 0), errors="coerce") or 0)
            _take = max(0.0, min(_q, _left))
            _budget[id(_r)] = _take
            _left -= _take
    return _budget


def _row_color_qty(row, budget) -> float:
    """ยอดของแถวนี้ที่นับเป็นงานสี (กก.)"""
    if budget is None:
        return (
            float(pd.to_numeric(row.get("PRODUCE_QTY", 0), errors="coerce") or 0)
            if _is_color_item(row.get("ITEM_CODE"))
            else 0.0
        )
    return float(budget.get(id(row), 0.0))


def _row_is_color(row, budget) -> bool:
    """แถวนี้ถือเป็นงานสีไหม — งานสีคือส่วนที่อยู่ในโควตาที่เปิดสีแล้วของ "ใบสั่งนั้น" เท่านั้น

    ⚠️ แถวที่ยังไม่รู้ยอดสี (override มือโดยไม่ระบุ กก.) ถือเป็นงานสีทั้งก้อน รวมแถวที่ยอด
    เป็น 0 ซึ่งถือเครื่องไว้ให้ carry ต่อ — ไม่งั้นแถวเหล่านั้นจะถูกมองว่า "ไม่มีสี"
    แล้วโดนยึดเครื่องทิ้ง
    """
    if _color_quota_of(row.get("ITEM_CODE"), _row_sc(row)) == float("inf"):
        return True
    return _row_color_qty(row, budget) > 1e-6


_RELEASED_BOOKING_MC: dict = {}   # (week YW, TYPE_1, เกจ, พูล) → เครื่อง booking ที่คืนเข้าพูลแล้ว


def _release_booking_mc(mc_group, gauge, week, n_mc) -> bool:
    """คืนเครื่องที่ booking จองไว้เข้าพูล (ใช้ตอนถอยงาน booking ออกให้งานสี)

    เครื่องของ booking ถูกหักจาก TOTAL_MC_REMAIN ตั้งแต่ AVA_MC → ถ้าถอยงานออกโดยไม่คืน
    เครื่องตัวนั้นจะหายจากระบบ (งานสีที่มารับช่วงต้องหักพูลใหม่ = นับซ้ำ พูลติดลบ)
    แก้ที่ summary_mc โดยตรงเพราะ get_actual_mc_remain() อ่านพูลจากตารางนี้

    ⚠️ การแก้นี้อยู่ในหน่วยความจำ — ไฟล์ booking_final ที่หน้าเว็บอ่านยังเป็นเลขเดิม
    จึงต้องจด (`_RELEASED_BOOKING_MC`) แล้วเขียนลงชีท MC_RELEASED ของไฟล์แผน
    ให้หน้าเว็บบวกกลับเข้า "เครื่องว่าง" ไม่งั้น Gantt จะขึ้น "ไม่ว่าง (เกิน N)" ทั้งที่เครื่องว่างจริง
    """
    if n_mc <= 0:
        return False
    _mc_r, _g_r = _apply_mc_redirect(str(mc_group).strip().upper(), gauge)
    _type_1 = _mc_to_type1(_mc_r, _g_r)
    _mask = (
        week_mask(summary_mc, week)
        & (summary_mc["TYPE_1"] == _type_1)
        & (summary_mc["GUAGE"].apply(_normalize_gauge) == _g_r)
    )
    _pool_label = _mc_to_pool_label(_mc_r, _g_r)
    if (
        _pool_label is not None
        and (_type_1, _g_r) in _SPLIT_CG
        and "MC_POOL" in summary_mc.columns
    ):
        _mask = _mask & (summary_mc["MC_POOL"].astype(str).str.strip() == _pool_label)
    _idx = summary_mc.index[_mask]
    if len(_idx) == 0:
        return False
    _i0 = _idx[0]
    _rem0 = int(pd.to_numeric(summary_mc.loc[_i0, "TOTAL_MC_REMAIN"], errors="coerce") or 0)
    summary_mc.loc[_i0, "TOTAL_MC_REMAIN"] = _rem0 + int(n_mc)
    # จดไว้ให้ไฟล์แผน (ชีท MC_RELEASED) — พูลที่จดต้องเป็นพูลของ "แถวที่ถูกแก้จริง" (_i0)
    # ไม่ใช่ป้ายพูลที่คำนวณจาก mc_group เฉย ๆ เพราะกลุ่มที่ไม่ split จะหักที่แถวแรกของ cat+เกจ
    _rel_pool = str(summary_mc.loc[_i0].get("MC_POOL", "") or "").strip()
    _rel_key = (int(week), _type_1, _g_r, _rel_pool)
    _RELEASED_BOOKING_MC[_rel_key] = _RELEASED_BOOKING_MC.get(_rel_key, 0) + int(n_mc)
    # ⚠️ การคืนนี้อยู่ในหน่วยความจำเท่านั้น — ไฟล์ booking_final ที่หน้าเว็บอ่านยังเป็นเลขเดิม
    # ถ้าคืนผิด (เครื่องยังถูกใช้อยู่) แผนจะวางเกินแล้ว Gantt ขึ้น "ไม่ว่าง (เกิน N)" → ต้องไล่ได้จาก log
    print(f"   ↩️ [RELEASE BOOKING MC] {_mc_r} G{_g_r} W{week}: คืนเครื่อง booking {int(n_mc)} ตัว "
          f"เข้าพูล (ว่าง {_rem0} → {_rem0 + int(n_mc)})")
    if "MC_USE_CEIL" in summary_mc.columns:
        summary_mc.loc[_i0, "MC_USE_CEIL"] = max(
            0,
            int(pd.to_numeric(summary_mc.loc[_i0, "MC_USE_CEIL"], errors="coerce") or 0) - int(n_mc),
        )
    return True


def _preempt_row_key(row):
    """(item, mc_group, gauge) ของแถวแผน — คีย์ลูกโซ่เดียวกับ _repair_gap_carry"""
    return (
        str(row.get("ITEM_CODE", "")).strip(),
        str(row.get("MC_GROUP", "")).strip().upper(),
        _normalize_gauge(row.get("MC_GUAGE", "")),
    )


def _preempt_row_idx(row):
    _i = row.get("PLAN_IDX")
    if _i is None or (isinstance(_i, float) and pd.isna(_i)):
        _i = week_pos(row.get("PLAN_WEEK"))
    return None if _i is None else int(_i)


def _row_is_core(row) -> bool:
    """แถวแผนนี้เป็นงาน core item ไหม — ธงเดียวกับคอลัมน์ IS_CORE_ITEM ของชีท PLAN

    ใช้กันไม่ให้งานสียึดเครื่องของงาน core ระหว่างวาง (core เดินตามรอบสต๊อก ไม่ผูก RDD)
    """
    return str(row.get("IS_CORE_ITEM", "") or "").strip().upper().startswith("CORE")


# โควตา setup ต่อสัปดาห์ต่อ factory type (ชุดเดียวกับ get_remaining_job_slots / ชีท REMAINING_JOBS)
_PREEMPT_JOB_CAP = dict(JOB_CAP)


def _preempt_job_slots(plan_rows, week, mc_group) -> int:
    """job slot ที่ยังว่างจริงของสัปดาห์นั้น — นับ NEW_MC จาก "แผนปัจจุบัน" ไม่ใช่ weekly_job_usage

    weekly_job_usage เป็นตัวนับระหว่างวาง ซึ่งไม่ตรงกับ NEW_MC ที่เหลือในแผนสุดท้าย
    (ชีท REMAINING_JOBS ก็นับจาก NEW_MC ด้วยเหตุผลเดียวกัน) — ถ้ารอบซ่อมนี้เชื่อตัวนับเก่า
    จะแจก setup เกินโควตาโดยไม่รู้ตัว (รอบทดสอบก่อนหน้า OM W36-40 เกิน 8-12 job)
    ยุบด้วย max ต่อ (item, กลุ่มเครื่อง, เกจ) เพราะ Planning เขียน NEW_MC ซ้ำทุกแถวของเครื่องชุดเดียวกัน
    """
    if not JOB_ENFORCE:
        return JOB_CAP_UNLIMITED         # ปิดสวิตช์คุมโควตาช่าง → ไม่จำกัด
    _tk = _get_type_key_for_mc(mc_group)
    _cap = _PREEMPT_JOB_CAP.get(_tk)
    if _cap is None:
        return JOB_CAP_UNLIMITED         # OUTSOURCE / นอกโรง → ไม่จำกัด
    _old = sum(
        _j for _mc_o, _j in weekly_job_usage_old.get(int(week), {}).items()
        if _get_type_key_for_mc(_mc_o) == _tk
    )
    _new_by_key: dict = {}
    for _r in plan_rows:
        if int(pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce") or 0) != int(week):
            continue
        # จ้างทอไม่กินโควตาของโรงเรา (ชื่อผู้รับจ้างไม่มีใน master_mc → ถูกตีเป็น PHET_DOUBLE)
        if _is_outsource_row(_r):
            continue
        _mcg = str(_r.get("MC_GROUP", "")).strip().upper()
        if _get_type_key_for_mc(_mcg) != _tk:
            continue
        _nm = int(pd.to_numeric(_r.get("NEW_MC", 0), errors="coerce") or 0)
        if _nm <= 0:
            continue
        _k = (
            str(_r.get("ITEM_CODE", "")).strip().upper(),
            _mcg,
            _normalize_gauge(_r.get("MC_GUAGE", "")),
        )
        _new_by_key[_k] = max(_new_by_key.get(_k, 0), _nm)
    return max(0, _cap - _old - sum(_new_by_key.values()))




def _daily_of_row(row) -> float:
    """DAILY_CAPACITY ของแถวแผน (0 ถ้าอ่านไม่ได้)"""
    return float(pd.to_numeric(row.get("DAILY_CAPACITY", 0), errors="coerce") or 0)


def _cascade_carry_cut(plan_rows, chains, key, from_idx) -> int:
    """ตัดเครื่องสัปดาห์หนึ่งแล้ว ต้องไล่ลดสัปดาห์ถัดไปที่ carry ต่อจากมันด้วย

    ไม่งั้นจะเหลือ "เครื่องผี": สัปดาห์นี้เครื่องถูกยึด/ตัดไปแล้ว แต่สัปดาห์ถัดไปยังอ้าง
    CARRYOVER_MC เท่าเดิม → แผนใช้เครื่องเกินจริงทั้งลูกโซ่ คืนจำนวนแถวที่แก้
    """
    def _num(r, k):
        return float(pd.to_numeric(r.get(k, 0), errors="coerce") or 0)

    _weeks = sorted(w for w in chains.get(key, {}) if w > from_idx)
    _prev_total = max(
        (int(_num(_r, "ACTUAL_MC")) for _r in chains.get(key, {}).get(from_idx, [])),
        default=0,
    )
    _fixed = 0
    for _wi in _weeks:
        _rows = [_r for _r in chains[key][_wi] if int(_num(_r, "ACTUAL_MC")) > 0]
        if not _rows:
            break                      # ลูกโซ่ขาดตรงนี้ ไม่ต้องไล่ต่อ
        _changed = False
        for _r in _rows:
            _carry = int(_num(_r, "CARRYOVER_MC"))
            _bk = int(_num(_r, "MC_BOOKING"))
            _cap = _prev_total + _bk    # carry ได้ไม่เกินเครื่องที่ลูกโซ่มีจริงสัปดาห์ก่อน
            if _carry <= _cap:
                continue
            _cut = _carry - _cap
            _keep = max(0, int(_num(_r, "ACTUAL_MC")) - _cut)
            _keep_new = max(0, _keep - _cap)
            _daily = _daily_of_row(_r)
            _rev = _num(_r, "REVOLUTION_WEIGHT")
            _qty = _num(_r, "PRODUCE_QTY")
            # กำลังผลิตต้องคิดด้วย "วันของชุดเครื่องใหม่" — carry ได้เต็มสัปดาห์
            # ส่วนเครื่องที่ต้องตั้งใหม่เสียวัน setup (ถ้าคิดด้วยวันเดิมจะเหลือยอดเกินกำลังจริง)
            _wd0 = _num(_r, "ACTUAL_WORKING_DAYS") or _num(_r, "AVAILABLE_DAYS")
            _setup0 = get_setup_days_for_item(
                str(_r.get("MATERIAL_CONTENT", "") or ""),
                str(_r.get("YARN_USED", "") or ""),
                str(_r.get("MC_GROUP", "")).strip().upper(),
                str(_r.get("ITEM_CODE", "")).strip(),
            )
            _cap_keep = _daily * (
                _wd0 * min(_keep, _cap) + _repair_prod_days_new(_wd0, _setup0) * _keep_new
            )
            _keep_qty = fold_keep_qty(_qty, _cap_keep, _rev)
            _r["ACTUAL_MC"] = _keep
            _r["CARRYOVER_MC"] = _cap
            _r["NEW_MC"] = _keep_new
            _r["PRODUCE_QTY"] = _keep_qty
            _r["PLAN_QTY"] = max(0.0, _num(_r, "PLAN_QTY") + (_qty - _keep_qty))
            # ⚠️ ห้ามใช้ `x or ''` ตรง ๆ — LINE_REMARK ที่เป็น NaN นับเป็น truthy
            # แล้ว f-string จะได้คำว่า "nan | ..." โผล่ขึ้นหน้าเว็บ
            _prev_lr = _clean_remark(_r.get("LINE_REMARK"))
            _r["LINE_REMARK"] = f"{_prev_lr} | ลด carry ตามเครื่องที่ถูกตัดสัปดาห์ก่อน".strip(" |")
            _wd_c = _num(_r, "ACTUAL_WORKING_DAYS")
            _item_c = str(_r.get("ITEM_CODE", "")).strip()
            _mc_c = str(_r.get("MC_GROUP", "")).strip().upper()
            _g_c = _normalize_gauge(_r.get("MC_GUAGE", ""))
            _setup_c = get_setup_days_for_item(
                str(_r.get("MATERIAL_CONTENT", "") or ""),
                str(_r.get("YARN_USED", "") or ""),
                _mc_c, _item_c,
            )
            if _wd_c > 0:
                _r["AVAILABLE_DAYS"] = (
                    _repair_prod_days_new(_wd_c, _setup_c) if _r["NEW_MC"] > 0 else _wd_c
                )
            _r["SETUP_DAYS"] = _setup_c * _r["NEW_MC"] if _r["NEW_MC"] > 0 else 0
            # ⚠️ ยอดที่ตัดออกต้องไม่หายไปเฉย ๆ — ไล่วางต่อสัปดาห์ถัดไป เหลือจริงค่อยลง UNPLANNED
            _spill_c = max(0.0, _qty - _keep_qty)
            if _spill_c > 1e-6:
                _sc_c = str(_r.get("SC_SO_NO", "")).strip()
                _chain_c = {
                    _ci: _cr
                    for _ci, _cr in (
                        (_ci2, [_x for _x in _rows2 if str(_x.get("SC_SO_NO", "")).strip() == _sc_c])
                        for _ci2, _rows2 in chains.get(key, {}).items()
                    )
                    if _cr
                }
                # ⚠️ เพิ่งตัดเครื่องออกจากแถวไปหมาด ๆ แต่บัญชีพูลยังนับเครื่องนั้นว่าถูกใช้อยู่
                # (ต่างจาก _repair_pool_overflow / _repair_color_preempt ที่คืนเครื่องเข้าพูลทุกครั้ง)
                # ถ้าไม่ sync ก่อน _repair_spill_forward จะถามพูลแล้วได้ 0 ทั้งที่ยังว่าง
                # → ยอดถูกดันข้ามสัปดาห์ที่วางได้จริง (เจอกับ FD3BASLA49/01B0 W39: ว่างจริง 1 บัญชีบอก 0)
                _resync_pool_ledger()
                _placed_c, _left_c, _trace_c = _repair_spill_forward(
                    _r, _item_c, _mc_c, _g_c, int(_wi), _spill_c, _keep,
                    _chain_c, plan_rows, _setup_c, _daily, _rev,
                    allow_same_week=True,
                )
                if _placed_c > 0:
                    _r["PLAN_QTY"] = max(0.0, _num(_r, "PLAN_QTY") - _placed_c)
                if _left_c > 1e-6:
                    _skip_qty_short.append({
                        "SC_SO_NO": str(_r.get("SC_SO_NO", "")),
                        "ITEM_CODE": _item_c,
                        "MC_GROUP": _mc_c,
                        "GAUGE": _g_c,
                        "ORDERS_QTY": _r.get("ORDERS_QTY", ""),
                        "PENDING_PLAN": _r.get("PENDING_PLAN", ""),
                        "FG_WEEK": _r.get("FG_WEEK", ""),
                        "DATE_IN": _r.get("DATE_IN", ""),
                        "UNPLANNED_QTY": _left_c,
                        "REASON": (
                            f"W{int(_num(_r, 'PLAN_WEEK'))}: เครื่องที่ carry มาถูกตัดไปแล้วสัปดาห์ก่อน "
                            f"→ สัปดาห์ถัดไปไม่มีเครื่อง/โควตาเหลือรับยอดต่อ"
                        ),
                    })
            _changed = True
            _fixed += 1
        _prev_total = max(int(_num(_r, "ACTUAL_MC")) for _r in _rows)
        if not _changed and _prev_total > 0:
            break                      # สัปดาห์นี้ไม่ต้องแก้แล้ว → สัปดาห์ถัดไปก็ไม่กระทบ
    return _fixed



def _repair_pool_overflow(plan_rows, enforce: bool = True) -> int:
    """ตรวจทุก (กลุ่มเครื่อง, เกจ, สัปดาห์) ว่าแผนใช้เครื่องเกินที่มีจริงไหม — เกินให้ตัดออก

    นับด้วย get_actual_mc_remain() ซึ่งเป็นตัวนับพูลของระบบเอง (รวม booking + แผน,
    รู้เรื่องพูลแยก SKP/SKPTA·SKPLE, MC redirect และ sub-pool POLY/COTTON)
    ลำดับการตัด: ยอดที่ยังไม่เปิดสีก่อน (สายอยู่แล้ว → target ไกล) แล้วค่อยยอดที่เปิดสีแล้ว
    ยอดที่ถูกตัดไหลไปวางต่อสัปดาห์ถัดไปด้วย _repair_spill_forward เหมือนรอบซ่อมอื่น
    """
    if not plan_rows:
        return 0

    _budget = _build_color_budget(plan_rows)

    def _num(r, k):
        return float(pd.to_numeric(r.get(k, 0), errors="coerce") or 0)

    _chains: dict = {}
    for _r in plan_rows:
        _k = _preempt_row_key(_r)
        _i = _preempt_row_idx(_r)
        if not _k[0] or not _k[1] or _i is None:
            continue
        _chains.setdefault(_k, {}).setdefault(_i, []).append(_r)

    # กลุ่มเครื่อง+เกจ+สัปดาห์ ที่แผนแตะ
    _cells: dict = {}
    for _r in plan_rows:
        if int(_num(_r, "ACTUAL_MC")) <= 0:
            continue
        if _is_outsource_row(_r):
            continue  # งานจ้างทอไม่กินพูลเครื่องในบ้าน — มีเพดานของตัวเองที่ชีท MC S9
        _mc = str(_r.get("MC_GROUP", "")).strip().upper()
        _g = _normalize_gauge(_r.get("MC_GUAGE", ""))
        _wk = int(_num(_r, "PLAN_WEEK"))
        if not _mc or not _g or _wk <= 0:
            continue
        _cells.setdefault((_mc, _g, _wk), []).append(_r)

    _cut_mc_total = 0
    for (_mc, _g, _wk), _rows in sorted(_cells.items(), key=lambda kv: (kv[0][2], kv[0][0], kv[0][1])):
        _rem = get_actual_mc_remain(_mc, _wk, _g, allow_negative=True)
        try:
            _rem = int(_rem)
        except Exception:
            continue
        if _rem >= 0:
            continue
        _over = -_rem
        # ผู้ถูกตัด: งานที่ยังไม่ให้สีก่อน → ในกลุ่มเดียวกันเอาตัวที่สายอยู่แล้ว/target ไกลก่อน
        _cands = []
        for _r in _rows:
            _item_r = str(_r.get("ITEM_CODE", "")).strip()
            _tk_r = _rank_target_knit(_r)   # แถว HOLD = NaN (ตรึงลำดับเดิม)
            _tk_i = None if pd.isna(_tk_r) else week_pos(int(_tk_r))
            _i_r = _preempt_row_idx(_r)
            _late_r = 1 if (_tk_i is not None and _i_r is not None and _i_r > _tk_i) else 0
            _cands.append((
                0 if not _row_is_color(_r, _budget) else 1,  # ยอดที่ยังไม่เปิดสีถูกตัดก่อน
                -_late_r,                                   # สายอยู่แล้วถูกตัดก่อน
                -(_tk_i or 10**6),                          # target ไกลถูกตัดก่อน
                _r,
            ))
        _cands.sort(key=lambda t: (t[0], t[1], t[2]))

        for _, _, _, _r in _cands:
            if _over <= 0:
                break
            if not enforce:
                break
            _mc_have = int(_num(_r, "ACTUAL_MC"))
            if _mc_have <= 0:
                continue
            _take = min(_mc_have, _over)
            _keep = _mc_have - _take
            _item_r = str(_r.get("ITEM_CODE", "")).strip()
            _daily_r = _daily_of_row(_r)
            _rev_r = _num(_r, "REVOLUTION_WEIGHT")
            _qty_r = _num(_r, "PRODUCE_QTY")

            _carry_r = int(_num(_r, "CARRYOVER_MC"))
            _new_r = int(_num(_r, "NEW_MC"))
            _new_keep = max(0, min(_new_r, _keep))         # ตัดเครื่องใหม่ก่อน (ยังไม่ได้ตั้งจริง)
            _carry_keep = max(0, _keep - _new_keep)
            _setup_r = get_setup_days_for_item(
                str(_r.get("MATERIAL_CONTENT", "") or ""),
                str(_r.get("YARN_USED", "") or ""),
                _mc, _item_r,
            )
            # กำลังผลิตต้องแยกวันของ carry (เต็มสัปดาห์) กับเครื่องที่ตั้งใหม่ (หักวัน setup)
            # ถ้าคิดวันเดียวเหมือนกันทั้งแถว ยอดที่เหลือจะเกินกำลังจริง
            _wd_r = _num(_r, "ACTUAL_WORKING_DAYS") or _num(_r, "AVAILABLE_DAYS")
            _keep_qty = fold_keep_qty(
                _qty_r,
                _daily_r * (_wd_r * _carry_keep + _repair_prod_days_new(_wd_r, _setup_r) * _new_keep),
                _rev_r,
            )
            _spill = max(0.0, _qty_r - _keep_qty)
            _r["ACTUAL_MC"] = _keep
            _r["CARRYOVER_MC"] = min(_carry_r, _carry_keep)
            _r["NEW_MC"] = _new_keep
            _r["PRODUCE_QTY"] = _keep_qty
            _r["PLAN_QTY"] = max(0.0, _num(_r, "PLAN_QTY") + _spill)
            _r["SETUP_DAYS"] = _setup_r * _new_keep if _new_keep > 0 else 0
            # วันผลิตต้องเปลี่ยนตามด้วย — ไม่มีเครื่องใหม่แล้วก็ไม่เสียวัน setup อีก
            # (ถ้าไม่อัปเดต แถวจะยังคิดกำลังผลิตด้วยวันของชุดเครื่องเดิม = ผลิตเกินจริง)
            if _wd_r > 0:
                _r["AVAILABLE_DAYS"] = (
                    _repair_prod_days_new(_wd_r, _setup_r) if _new_keep > 0 else _wd_r
                )
            _r["REMARK"] = _build_plan_remark(
                _mc, int(_num(_r, "CARRYOVER_MC")), _new_keep,
                _setup_r * _new_keep if _new_keep > 0 else 0,
                int(_num(_r, "MC_BOOKING")),
            )
            _r["LINE_REMARK"] = f"ตัดเครื่องส่วนเกินพูล W{_wk} ({_mc} G{_g})"
            # คืนเครื่องเข้าพูลเฉพาะส่วนที่แผนเคยหักไว้ (เครื่องใหม่)
            _rel_new = max(0, _new_r - _new_keep)
            if _rel_new > 0:
                _repair_release_pool(_mc, _g, _wk, _item_r, _rel_new)
                weekly_job_usage.setdefault(int(_wk), {})[_mc] = max(
                    0, weekly_job_usage.setdefault(int(_wk), {}).get(_mc, 0) - _rel_new
                )
            if _spill > 1e-6:
                _sc_r = str(_r.get("SC_SO_NO", "")).strip()
                _chain_r = {
                    _ci: _cr
                    for _ci, _cr in (
                        (_ci2, [_x for _x in _rows2 if str(_x.get("SC_SO_NO", "")).strip() == _sc_r])
                        for _ci2, _rows2 in _chains.get(_preempt_row_key(_r), {}).items()
                    )
                    if _cr
                }
                _placed, _left, _trace = _repair_spill_forward(
                    _r, _item_r, _mc, _g, int(_preempt_row_idx(_r) or 0), _spill, _keep,
                    _chain_r, plan_rows, _setup_r, _daily_r, _rev_r,
                )
                if _placed > 0:
                    _r["PLAN_QTY"] = max(0.0, _num(_r, "PLAN_QTY") - _placed)
                if _left > 1e-6:
                    _skip_qty_short.append({
                        "SC_SO_NO": str(_r.get("SC_SO_NO", "")),
                        "ITEM_CODE": _item_r,
                        "MC_GROUP": _mc,
                        "GAUGE": _g,
                        "ORDERS_QTY": _r.get("ORDERS_QTY", ""),
                        "PENDING_PLAN": _r.get("PENDING_PLAN", ""),
                        "FG_WEEK": _r.get("FG_WEEK", ""),
                        "DATE_IN": _r.get("DATE_IN", ""),
                        "UNPLANNED_QTY": _left,
                        "REASON": f"W{_wk}: แผนใช้เครื่องเกินพูล {_mc} G{_g} → ตัดออก แล้วสัปดาห์ถัดไปไม่มีเครื่องเหลือ",
                    })
            # สัปดาห์ถัดไปที่ carry ต่อจากเครื่องชุดนี้ ต้องลดตามด้วย (กันเครื่องผี)
            _casc = _cascade_carry_cut(plan_rows, _chains, _preempt_row_key(_r), _preempt_row_idx(_r) or 0)
            print(
                f"   ⛔ [POOL OVERFLOW] {_item_r} {_mc} G{_g} W{_wk}: ตัด {_take} เครื่อง "
                f"(เหลือ {_keep}) | ยอด {_spill:,.0f} kg ไหลไปสัปดาห์ถัดไป"
                + (f" | ลด carry ต่อเนื่อง {_casc} แถว" if _casc else "")
            )
            _over -= _take
            _cut_mc_total += _take

        if _over > 0:
            print(f"   ⚠️ [POOL OVERFLOW] {_mc} G{_g} W{_wk}: ยังเกินพูลอีก {_over} เครื่อง "
                  f"(booking จองเกินเครื่องจริง — แผนตัดช่วยไม่ได้)")

    if _cut_mc_total:
        print(f"\n⛔ POOL OVERFLOW: ตัดเครื่องส่วนเกินพูลรวม {_cut_mc_total} เครื่อง-สัปดาห์\n")
    return _cut_mc_total


def _pool_blocks_from_plan(plan_rows) -> dict:
    """แถวแผน → เครื่องที่ "แต่ละบล็อกงาน" กินจากพูลจริง

    1 บล็อก = (สัปดาห์, item, กลุ่มเครื่อง, เกจ) ใช้เครื่องชุดเดียว แต่ Planning เขียนหลายแถว
    (แถวละใบสั่ง) โดยแปะ ACTUAL_MC/MC_BOOKING ของทั้งบล็อกซ้ำทุกแถว → ต้องยุบด้วย max
    (พิสูจน์กับลูกโซ่ carry: W36 มี 6 เครื่อง, W37 สองแถวเขียน 8 ทั้งคู่ = carry 6 + ใหม่ 2
     ถ้าบวกจะได้ 16 ซึ่งเป็นไปไม่ได้) — สูตรเดียวกับ planMcAgg ใน PlanGantt.jsx

    เครื่องที่ booking ถืออยู่แล้ว (MC_BOOKING) ถูกหักจาก TOTAL_MC_REMAIN ไปแล้ว
    → "ที่แผนกินเพิ่ม" = max(0, ACTUAL_MC − MC_BOOKING)

    คืน {(week, item, mc_upper, gauge): เครื่องที่กินเพิ่ม}
    """
    # ⚠️ max อย่างเดียวไม่พอ — บล็อกเดียวกัน "แตกกันคนละใบสั่ง" ได้
    # เช่น W42 ทั้งบล็อกมี 7 เครื่อง พอ W43 ใบสั่ง A ถือ 4 ใบสั่ง B ถือ 3 (คนละชุดเครื่อง)
    #   max → อ่านได้ 4 ทั้งที่จริง 7 → บัญชีพูลกับแผนไม่ตรง (เจอจริง W43 DOUBLE-40|18)
    # สูตรที่ถูก: ยุบด้วย max "ภายในใบสั่งเดียวกัน" (แถวซ้ำของ SC เดิมเขียนเลขเดียวกัน)
    #             แล้ว "บวกข้ามใบสั่ง" (เครื่องของคนละ SC คือคนละชุด)
    # แถวที่ใช้เครื่องร่วมกับแถวอื่น (MC_SHARED > 0) เขียน ACTUAL_MC = 0 อยู่แล้ว → บวกไม่โดนซ้ำ
    per_sc: dict = {}
    for _r in plan_rows or []:
        _wk = pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce")
        if pd.isna(_wk):
            continue
        _mc = str(_r.get("MC_GROUP", "")).strip().upper()
        _g = _normalize_gauge(_r.get("MC_GUAGE"))
        if not _mc or not _g:
            continue
        _k = (int(_wk), str(_r.get("ITEM_CODE", "")).strip().upper(), _mc, _g)
        _sc = str(_r.get("SC_SO_NO", "")).strip()
        _a = float(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0)
        _b = float(pd.to_numeric(_r.get("MC_BOOKING", 0), errors="coerce") or 0)
        _cur = per_sc.setdefault(_k, {}).get(_sc)
        per_sc[_k][_sc] = (max(_cur[0], _a), max(_cur[1], _b)) if _cur else (_a, _b)
    blocks: dict = {}
    for _k, _scs in per_sc.items():
        _a = sum(v[0] for v in _scs.values())
        # MC_BOOKING = เครื่องที่ booking ถือของ "ทั้งบล็อก" (แปะซ้ำทุกแถว) → ใช้ max ไม่ใช่ sum
        _b = max((v[1] for v in _scs.values()), default=0.0)
        blocks[_k] = (_a, _b)
    return {k: max(0, int(a - b)) for k, (a, b) in blocks.items() if a - b > 0}


_POOL_LEDGER_BASE: dict = {}   # ยอดตั้งต้นของบัญชีพูลที่ไม่ได้มาจากแถวแผนรอบนี้ (old plan)


def _snapshot_pool_ledger_base() -> None:
    """เก็บยอดตั้งต้นของบัญชีพูลไว้ก่อนเริ่มวางแผน

    ยอดนี้มาจากแผนรอบก่อน (old plan) ซึ่งไม่มีแถวอยู่ใน _PLANS_REF → ตอน resync
    ต้องบวกกลับเข้าไป ไม่งั้นเครื่องที่แผนเก่าจองไว้จะหายไปจากบัญชี
    """
    global _POOL_LEDGER_BASE
    _POOL_LEDGER_BASE = {
        "gen": {w: dict(m) for w, m in weekly_new_plan_usage.items()},
        "pool": {w: dict(m) for w, m in weekly_new_plan_pool_usage.items()},
        "sp": dict(_mc_special_weekly_usage),
    }


def _resync_pool_ledger() -> None:
    """สร้างบัญชีเครื่องของพูลใหม่จาก "แถวแผนจริง" แทนการเชื่อยอดที่บวกสะสมมา

    ทำไม: บัญชีถูกบวก/ลบจากหลายเส้นทาง (ลูปหลัก, รอบซ่อม, การยุบแถว, การปลดเครื่อง)
    แล้วค่อย ๆ เพี้ยนจากแถวแผนจริง — รอบทดสอบพบ 56 ช่องที่บัญชี "สูงกว่า" ของจริง 1-7 เครื่อง
    (W41 DOUBLE-30|28 งบ 71 ใช้จริง 64 บัญชีบอก 71 / W36 PHET|SKP 28G งบ 15 บัญชีบอก 16
     ซึ่งเกินจำนวนเครื่องที่มีจริงด้วยซ้ำ) → ด่านคิดว่าพูลเต็มแล้วเลื่อนงานออกไป
    ทั้งที่ยังมีเครื่องว่างจริง

    เรียกที่หัวการวางแต่ละ order และก่อนรอบซ่อมแต่ละรอบ — ระหว่างวาง order เดียวยังใช้
    การบวกสะสมตามเดิม (เร็วกว่า และเพี้ยนไม่ทันในช่วงสั้น ๆ)
    """
    if not _POOL_LEDGER_BASE:
        return
    _gen = {w: dict(m) for w, m in _POOL_LEDGER_BASE["gen"].items()}
    _pool = {w: dict(m) for w, m in _POOL_LEDGER_BASE["pool"].items()}
    _sp = dict(_POOL_LEDGER_BASE["sp"])

    for (_wk, _it, _mc, _g), _n in _pool_blocks_from_plan(_PLANS_REF).items():
        _mc_r, _g_r = _apply_mc_redirect(_mc, _g)
        _sp_type = _get_subgroup_by_item_prefix(_mc_r, _g_r, _it)
        if _sp_type:
            _k = (_mc_to_factory(_mc_r, _g_r), _mc_to_type1(_mc_r, _g_r), _g_r, _wk, _sp_type)
            _sp[_k] = _sp.get(_k, 0) + _n
            continue
        _key = (_mc_to_type1(_mc_r, _g_r), _g_r)
        _gen.setdefault(_wk, {})[_key] = _gen.setdefault(_wk, {}).get(_key, 0) + _n
        _pl = _mc_to_pool_label(_mc_r, _g_r)
        if _pl:
            _pool.setdefault(_wk, {})[_pl] = _pool.setdefault(_wk, {}).get(_pl, 0) + _n

    # แทนที่ในตัวเดิม (ห้ามผูก object ใหม่ — ลูปหลักถือ reference ของ dict พวกนี้อยู่)
    weekly_new_plan_usage.clear()
    weekly_new_plan_usage.update(_gen)
    weekly_new_plan_pool_usage.clear()
    weekly_new_plan_pool_usage.update(_pool)
    _mc_special_weekly_usage.clear()
    _mc_special_weekly_usage.update(_sp)


def _audit_pool_usage(plan_rows) -> int:
    """เทียบ 3 ตัวเลขต่อ (พูล × สัปดาห์): บัญชีภายใน vs เครื่องในแผนจริง vs งบพูล

    ไม่แก้อะไร — ใช้จับว่ามีเส้นทางไหน "เติมเครื่องเข้าแผนโดยไม่หักพูล" หรือไม่
    (ถ้าบัญชีภายในต่ำกว่าของจริง ด่าน get_actual_mc_remain และรอบซ่อม _repair_pool_overflow
     ที่เชื่อบัญชีนั้นจะปล่อยงานเกินพูลโดยไม่รู้ตัว)

    งาน POLY (FD4/F4) / COTTON (FD5/F5) ที่พูลนั้นกันเครื่องไว้ให้ ไม่ได้กินพูลทั่วไป
    → แยกไปเทียบกับโควตา sub-pool ของตัวเอง (โมเดลเดียวกับ get_actual_mc_remain)
    """
    used_gen: dict = {}          # key → {week: เครื่องที่แผนกินจากพูลทั่วไป}
    used_sp: dict = {}           # (factory, cat, gauge, week, ชนิด) → เครื่องที่กิน sub-pool
    for (_wk, _it, _mc, _g), _n in _pool_blocks_from_plan(plan_rows).items():
        _sp = _get_subgroup_by_item_prefix(_mc, _g, _it)
        if _sp:
            # ไปกิน sub-pool POLY/COTTON ของตัวเอง ไม่ใช่พูลทั่วไป
            _mc_s, _g_s = _apply_mc_redirect(_mc, _g)
            _k_sp = (_mc_to_factory(_mc_s, _g_s), _mc_to_type1(_mc_s, _g_s), _g_s, _wk, _sp)
            used_sp[_k_sp] = used_sp.get(_k_sp, 0) + _n
            continue
        _mc_r, _g_r = _apply_mc_redirect(_mc, _g)
        _t1 = _mc_to_type1(_mc_r, _g_r)
        _pl = _mc_to_pool_label(_mc_r, _g_r)
        _key = _pl if (_pl and (_t1, _g_r) in _SPLIT_CG) else (_t1, _g_r)
        used_gen.setdefault(_key, {})[_wk] = used_gen.setdefault(_key, {}).get(_wk, 0) + _n

    def _budget_of(_df, _key_in, _is_pool_in, _wk_in):
        """งบพูลของช่องนี้จากตารางเครื่องที่ส่งมา (summary_mc = ปัจจุบัน / base = ตามไฟล์)"""
        if _is_pool_in:
            _rw = _df[week_mask(_df, _wk_in) & (_df["MC_POOL"].astype(str).str.strip() == _key_in)]
        else:
            _t1_in, _g_in = _key_in
            _rw = _df[
                week_mask(_df, _wk_in)
                & (_df["TYPE_1"] == _t1_in)
                & (_df["GUAGE"].apply(_normalize_gauge) == _g_in)
            ]
        if _rw.empty:
            return None
        return int(pd.to_numeric(_rw["TOTAL_MC_REMAIN"], errors="coerce").fillna(0).sum())

    _bad = []
    for _key, _weeks in used_gen.items():
        _is_pool = isinstance(_key, str)
        for _wk, _used in sorted(_weeks.items()):
            if _is_pool:
                _ledger = weekly_new_plan_pool_usage.get(_wk, {}).get(_key, 0)
                _label = _key
            else:
                _t1, _g_r = _key
                _ledger = weekly_new_plan_usage.get(_wk, {}).get((_t1, _g_r), 0)
                _label = f"{_t1}|{_g_r}"
            _budget = _budget_of(summary_mc, _key, _is_pool, _wk)
            if _budget is None:
                continue
            # งบ "ตามไฟล์" = เลขที่หน้าเว็บกับโรงงานเห็น (summary_mc ระหว่างรันอาจถูกบวกคืน
            # จากการถอยงาน booking ออก ซึ่งไฟล์ booking_final ไม่รู้เรื่องด้วย)
            _budget0 = _budget_of(summary_mc_base, _key, _is_pool, _wk)
            _budget0 = _budget if _budget0 is None else _budget0
            if _used > _budget or _used > _budget0 or _used != _ledger:
                _bad.append((_wk, _label, _budget, _used, int(_ledger), _budget0))

    # เครื่องที่กันไว้ให้ POLY/COTTON — โควตาแยก ห้ามเกินเช่นกัน
    for (_f, _cat, _g_r, _wk, _sp), _used in sorted(used_sp.items()):
        _entry = (_MC_SPECIAL_PLAN.get((_f, _cat, "", _g_r))
                  or next((v for (f2, c2, m2, g2), v in _MC_SPECIAL_PLAN.items()
                           if (f2, c2, g2) == (_f, _cat, _g_r)), None))
        if not _entry:
            continue
        _reserved = int(_entry.get(_sp, 0))
        if _reserved <= 0:
            continue
        _budget = max(0, _reserved - _MC_SPECIAL_BOOKING_USED.get((_cat, _g_r, _wk, _sp), 0))
        _ledger = _mc_special_weekly_usage.get((_f, _cat, _g_r, _wk, _sp), 0)
        if _used > _budget or _used != _ledger:
            # sub-pool อ่านโควตาจาก MasterMC ตรง ๆ (ไม่ได้ถูกบวกคืนระหว่างรัน) → งบตามไฟล์ = งบเดียวกัน
            _bad.append((_wk, f"{_cat}|{_g_r} [{_sp}]", _budget, _used, int(_ledger), _budget))

    if not _bad:
        print("✅ ตรวจพูลเครื่อง: บัญชีภายในตรงกับแผนจริง และไม่มีสัปดาห์ไหนใช้เกินพูล")
        return 0
    # "เกิน" = เกินงบใดงบหนึ่ง — งบตามไฟล์คือเลขที่หน้าเว็บ/โรงงานเห็น จึงนับเป็นเกินด้วย
    _over = [b for b in _bad if b[3] > b[2] or b[3] > b[5]]
    print("\n" + "=" * 90)
    print(f"⚠️  ตรวจพูลเครื่อง: พบ {len(_bad)} ช่องที่ตัวเลขไม่ตรง (ใช้เกินพูล {len(_over)} ช่อง)")
    print(f"{'W':>6} {'พูล':<28} {'งบ':>5} {'งบตามไฟล์':>10} {'แผนใช้จริง':>10} {'บัญชีภายใน':>10}  หมายเหตุ")
    print("=" * 90)
    for _wk, _label, _budget, _used, _ledger, _budget0 in sorted(
        _bad, key=lambda b: (b[3] - min(b[2], b[5])), reverse=True
    )[:25]:
        _note = []
        if _used > _budget:
            _note.append(f"เกินพูล {_used - _budget}")
        if _used > _budget0 and _budget0 != _budget:
            # เกิดเมื่อระหว่างรันมีการคืนเครื่อง booking เข้าพูล (ดู RELEASE BOOKING MC ใน log)
            # ไฟล์ booking_final ไม่ได้ถูกแก้ตาม → หน้าเว็บจะขึ้น "ไม่ว่าง (เกิน N)" ที่ช่องนี้
            _note.append(f"เกินงบตามไฟล์ {_used - _budget0} (หน้าเว็บจะขึ้นเกิน)")
        if _used != _ledger:
            _note.append(f"บัญชีต่ำกว่าจริง {_used - _ledger}" if _used > _ledger
                         else f"บัญชีสูงกว่าจริง {_ledger - _used}")
        print(f"{_wk:>6} {_label:<28} {_budget:>5} {_budget0:>10} {_used:>10} {_ledger:>10}  {' , '.join(_note)}")
    print("=" * 90 + "\n")
    return len(_over)


def _repair_job_overflow(plan_rows) -> int:
    """โควตา setup ต่อสัปดาห์ต่อโรง (OM 13 / PHET_DOUBLE 33 / PHET_SINGLE 44) ห้ามเกิน

    เกิน → ลดเครื่อง "ที่ตั้งใหม่" ของงานที่ยังไม่ให้สีก่อน (สายอยู่แล้ว → target ไกล)
    ยอดที่ตัดไหลไปสัปดาห์ถัดไปเหมือนรอบซ่อมอื่น ; ไม่แตะเครื่อง carry เพราะไม่กินโควตา setup
    """
    if not plan_rows:
        return 0

    def _num(r, k):
        return float(pd.to_numeric(r.get(k, 0), errors="coerce") or 0)

    _chains: dict = {}
    for _r in plan_rows:
        _k = _preempt_row_key(_r)
        _i = _preempt_row_idx(_r)
        if not _k[0] or not _k[1] or _i is None:
            continue
        _chains.setdefault(_k, {}).setdefault(_i, []).append(_r)

    _budget = _build_color_budget(plan_rows)
    _weeks = sorted({int(_num(_r, "PLAN_WEEK")) for _r in plan_rows if int(_num(_r, "PLAN_WEEK")) > 0})
    _cut_total = 0
    for _wk in _weeks:
        for _type, _cap in _PREEMPT_JOB_CAP.items():
            # แถวของโรง/ชนิดนี้ในสัปดาห์นี้ที่มีเครื่องตั้งใหม่
            _rows = [
                _r for _r in plan_rows
                if int(_num(_r, "PLAN_WEEK")) == _wk
                and int(_num(_r, "NEW_MC")) > 0
                and not _is_outsource_row(_r)  # จ้างทอไม่ได้ตั้งเครื่องในบ้าน → ไม่กินโควตา setup
                and _get_type_key_for_mc(str(_r.get("MC_GROUP", "")).strip().upper()) == _type
            ]
            if not _rows:
                continue
            # _preempt_job_slots ตัดค่าที่ 0 (ไม่บอกว่าเกินเท่าไร) → คำนวณส่วนเกินเอง
            # เกณฑ์เดียวกับชีท REMAINING_JOBS: OLD จาก booking + NEW จากแผน (ยุบ max ต่อเครื่องชุดเดียวกัน)
            _old = sum(
                _j for _m, _j in weekly_job_usage_old.get(int(_wk), {}).items()
                if _get_type_key_for_mc(_m) == _type
            )
            _new_by_key: dict = {}
            for _r in _rows:
                _k = (
                    str(_r.get("ITEM_CODE", "")).strip().upper(),
                    str(_r.get("MC_GROUP", "")).strip().upper(),
                    _normalize_gauge(_r.get("MC_GUAGE", "")),
                )
                _new_by_key[_k] = max(_new_by_key.get(_k, 0), int(_num(_r, "NEW_MC")))
            _over = _old + sum(_new_by_key.values()) - _cap
            if _over <= 0:
                continue

            _cands = []
            for _r in _rows:
                _item_r = str(_r.get("ITEM_CODE", "")).strip()
                _tk_r = _rank_target_knit(_r)   # แถว HOLD = NaN (ตรึงลำดับเดิม)
                _tk_i = None if pd.isna(_tk_r) else week_pos(int(_tk_r))
                _i_r = _preempt_row_idx(_r)
                _late_r = 1 if (_tk_i is not None and _i_r is not None and _i_r > _tk_i) else 0
                _cands.append((
                    0 if not _row_is_color(_r, _budget) else 1,  # ยอดที่ยังไม่เปิดสีถูกตัดก่อน
                    -_late_r,
                    -(_tk_i or 10**6),
                    _r,
                ))
            _cands.sort(key=lambda t: (t[0], t[1], t[2]))

            for _, _, _, _r in _cands:
                if _over <= 0:
                    break
                _new_r = int(_num(_r, "NEW_MC"))
                if _new_r <= 0:
                    continue
                _take = min(_new_r, _over)
                _keep_new = _new_r - _take
                _item_r = str(_r.get("ITEM_CODE", "")).strip()
                _mc_r = str(_r.get("MC_GROUP", "")).strip().upper()
                _g_r = _normalize_gauge(_r.get("MC_GUAGE", ""))
                _carry_r = int(_num(_r, "CARRYOVER_MC"))
                _keep_mc = _carry_r + _keep_new
                _daily_r = _daily_of_row(_r)
                _rev_r = _num(_r, "REVOLUTION_WEIGHT")
                _qty_r = _num(_r, "PRODUCE_QTY")
                _setup_r = get_setup_days_for_item(
                    str(_r.get("MATERIAL_CONTENT", "") or ""),
                    str(_r.get("YARN_USED", "") or ""),
                    _mc_r, _item_r,
                )
                # แยกวันของ carry (เต็มสัปดาห์) กับเครื่องที่ตั้งใหม่ (หักวัน setup)
                _wd_r = _num(_r, "ACTUAL_WORKING_DAYS") or _num(_r, "AVAILABLE_DAYS")
                _keep_qty = fold_keep_qty(
                    _qty_r,
                    _daily_r * (_wd_r * _carry_r + _repair_prod_days_new(_wd_r, _setup_r) * _keep_new),
                    _rev_r,
                )
                _spill = max(0.0, _qty_r - _keep_qty)
                _r["ACTUAL_MC"] = _keep_mc
                _r["NEW_MC"] = _keep_new
                _r["PRODUCE_QTY"] = _keep_qty
                _r["PLAN_QTY"] = max(0.0, _num(_r, "PLAN_QTY") + _spill)
                _r["SETUP_DAYS"] = _setup_r * _keep_new if _keep_new > 0 else 0
                # ลดเครื่องใหม่แล้ว วันผลิตต้องคิดใหม่ (ไม่งั้นคิดกำลังผลิตเกินจริง)
                _wd_j = _num(_r, "ACTUAL_WORKING_DAYS")
                if _wd_j > 0:
                    _r["AVAILABLE_DAYS"] = (
                        _repair_prod_days_new(_wd_j, _setup_r) if _keep_new > 0 else _wd_j
                    )
                _r["REMARK"] = _build_plan_remark(
                    _mc_r, _carry_r, _keep_new,
                    _setup_r * _keep_new if _keep_new > 0 else 0,
                    int(_num(_r, "MC_BOOKING")),
                )
                _r["LINE_REMARK"] = f"ตัดเครื่องตั้งใหม่ให้อยู่ในโควตา setup ({_type} W{_wk})"
                _repair_release_pool(_mc_r, _g_r, _wk, _item_r, _take)
                weekly_job_usage.setdefault(int(_wk), {})[_mc_r] = max(
                    0, weekly_job_usage.setdefault(int(_wk), {}).get(_mc_r, 0) - _take
                )
                if _spill > 1e-6:
                    _sc_r = str(_r.get("SC_SO_NO", "")).strip()
                    _chain_r = {
                        _ci: _cr
                        for _ci, _cr in (
                            (_ci2, [_x for _x in _rows2 if str(_x.get("SC_SO_NO", "")).strip() == _sc_r])
                            for _ci2, _rows2 in _chains.get(_preempt_row_key(_r), {}).items()
                        )
                        if _cr
                    }
                    _placed, _left, _trace = _repair_spill_forward(
                        _r, _item_r, _mc_r, _g_r, int(_preempt_row_idx(_r) or 0), _spill, _keep_mc,
                        _chain_r, plan_rows, _setup_r, _daily_r, _rev_r,
                    )
                    if _placed > 0:
                        _r["PLAN_QTY"] = max(0.0, _num(_r, "PLAN_QTY") - _placed)
                    if _left > 1e-6:
                        _skip_qty_short.append({
                            "SC_SO_NO": str(_r.get("SC_SO_NO", "")),
                            "ITEM_CODE": _item_r,
                            "MC_GROUP": _mc_r,
                            "GAUGE": _g_r,
                            "ORDERS_QTY": _r.get("ORDERS_QTY", ""),
                            "PENDING_PLAN": _r.get("PENDING_PLAN", ""),
                            "FG_WEEK": _r.get("FG_WEEK", ""),
                            "DATE_IN": _r.get("DATE_IN", ""),
                            "UNPLANNED_QTY": _left,
                            "REASON": f"W{_wk}: โควตา setup ของ {_type} เต็ม ({_cap} job) → ตัดเครื่องตั้งใหม่ออก",
                        })
                print(
                    f"   🔧 [JOB QUOTA] {_item_r} {_mc_r} G{_g_r} W{_wk}: ลดเครื่องตั้งใหม่ "
                    f"{_new_r}→{_keep_new} ({_type} เกินโควตา) | ยอด {_spill:,.0f} kg ไหลไปสัปดาห์ถัดไป"
                )
                _over -= _take
                _cut_total += _take

            if _over > 0:
                print(f"   ⚠️ [JOB QUOTA] {_type} W{_wk}: ยังเกินโควตาอีก {_over} job "
                      f"(เป็น setup ของ booking เดิม แผนลดไม่ได้)")

    if _cut_total:
        print(f"\n🔧 JOB QUOTA: ลดเครื่องตั้งใหม่ให้อยู่ในโควตา setup รวม {_cut_total} เครื่อง-สัปดาห์\n")
    return _cut_total


def _trim_color_gap_hold(plan_rows) -> int:
    """ลดเครื่องที่กันไว้ในแถว Hold ให้ไม่เกินเครื่องที่คีย์เดียวกันใช้ "สัปดาห์ถัดไป"

    แถว Hold ถูกสร้างตอนลูปวางแผนเดินถึงสัปดาห์เว้น ซึ่งสัปดาห์ถัดไปยังไม่ถูกวาง → จึงกันไว้
    เท่าเครื่องสัปดาห์ก่อนหน้า (_color_gap_hold_mc) ; พอกลับมาทอจริงใช้น้อยกว่า (ทรงไล่ลง /
    ก้อนสุดท้าย) เครื่องส่วนเกินถูกกันไว้เปล่า ๆ — เคสจริง 14 ก.ย. 2569 FD1BASMK32/01B0:
    W43 เดิน 3 → Hold W44 กัน 3 → W45 ใช้แค่ 2
    กติกา user: Hold = min(เครื่องสัปดาห์ก่อน, เครื่องสัปดาห์ถัดไป)

    ⚠️ Hold รอ target ของ core กันติดกันได้หลายสัปดาห์ → สัปดาห์ข้าง ๆ ของแถว Hold มักเป็น
    แถว Hold ด้วยกันเอง ซึ่ง "ไม่มีงานเดิน" แต่ถือเครื่องจริง ถ้านับแต่งานจริงจะได้ next = 0
    แล้วโซ่ทั้งเส้นถูกตัดเหลือ 0 → ต้องนับเครื่องของแถว Hold ข้างเคียงด้วย และไล่ **จากสัปดาห์
    ท้ายมาหน้า** เพื่อให้ค่าที่ลดแล้วของสัปดาห์ถัดไปไหลย้อนมาเป็นเพดานของสัปดาห์ก่อนหน้า

    ต้องรันหลังด่านซ่อมทุกด่าน (pool/job overflow ยังตัดเครื่องสัปดาห์ถัดไปได้) และก่อนลบแถวผี
    — Hold ที่เหลือ 0 (ไม่มีงานกลับมาทอ) จะถูกด่านแถวผีลบทิ้งเอง
    carry ของสัปดาห์ถัดไป ≤ ACTUAL_MC ของสัปดาห์นั้น → ลดตามสัปดาห์ถัดไปลูกโซ่ carry ไม่ขาด
    ส่วนที่ลดตามสัปดาห์ก่อน (โควตา job ตัดเครื่องสัปดาห์ก่อนทีหลัง) ต้องไล่ลด carry สัปดาห์ถัดไปด้วย
    ⚠️ เครื่องที่คืนเป็น "ว่าง" บนแผน แต่รอบนี้ไม่มีงานอื่นเข้ามาใช้ (วางแผนจบไปแล้ว)

    คืนจำนวนเครื่อง-สัปดาห์ที่คืนเข้าพูล
    """
    if not (COLOR_GAP_ENABLED or CORE_HOLD_ENABLED) or not plan_rows:
        return 0

    def _row_idx(_r):
        _widx = _r.get("PLAN_IDX")
        if _widx is None or (isinstance(_widx, float) and pd.isna(_widx)):
            _widx = week_pos(_r.get("PLAN_WEEK"))
        return None if _widx is None else int(_widx)

    _holds = [_r for _r in plan_rows if _is_hold_row(_r)]
    if not _holds:
        return 0
    # ท้าย → หน้า: แถว Hold ที่ติดกันเป็นโซ่ต้องเห็นค่าที่ลดแล้วของสัปดาห์ถัดไปเป็นเพดานของตัวเอง
    _holds.sort(key=lambda _r: (_row_idx(_r) is None, -(_row_idx(_r) or 0)))

    # เครื่องที่แผนใช้ต่อ (คีย์ก้อน, idx สัปดาห์) — นับแบบเดียวกับ _color_gap_hold_mc (รวมทุกแถว)
    _used = _color_gap_mc_by_week(plan_rows)
    _held_by_week = _hold_mc_by_week(plan_rows)   # เครื่องที่แถว Hold ถืออยู่ (อัปเดตสดเมื่อลด)
    _chains = None                     # ลูกโซ่แถวแผน (สำหรับลด carry สัปดาห์ถัดไป) — สร้างเมื่อต้องใช้
    # แถวงานที่ "เกาะเครื่อง" ในสัปดาห์เดียวกับ Hold (ACTUAL_MC = 0, MC_SHARED > 0) — เครื่องที่มันเกาะ
    # อาจเป็นชุดที่ Hold นี้กันไว้ (Hold รอ target เปิด cap ให้ใบที่เลยกำหนดส่งเติมได้ — ดู _color_gap_hold_mc)
    # → ห้ามตัด Hold ต่ำกว่าที่แถวเหล่านี้ใช้ ไม่งั้นเหลือแต่ป้าย "ใช้เครื่องร่วม" โดยไม่มีใครถือเครื่องจริง
    # (ส่วนที่แถวงานถือเองเป็น carry อยู่แล้ว _absorb_hold_into_production หักออกให้ ไม่นับซ้ำ)
    _riders: dict = {}
    for _pr in plan_rows:
        if _is_hold_row(_pr) or _is_outsource_row(_pr):
            continue
        if int(pd.to_numeric(_pr.get("ACTUAL_MC", 0), errors="coerce") or 0) > 0:
            continue
        if float(pd.to_numeric(_pr.get("PRODUCE_QTY", 0), errors="coerce") or 0) <= 0:
            continue
        _sh = int(pd.to_numeric(_pr.get("MC_SHARED", 0), errors="coerce") or 0)
        _pi = _row_idx(_pr)
        if _sh <= 0 or _pi is None:
            continue
        _rk = (_color_gap_key(_pr.get("ITEM_CODE"), _pr.get("MC_GROUP"), _pr.get("MC_GUAGE")), _pi)
        _riders[_rk] = max(_riders.get(_rk, 0), _sh)

    _released = 0
    for _r in _holds:
        _held = int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0)
        _widx = _row_idx(_r)
        if _held <= 0 or _widx is None:
            continue
        _k = _color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE"))
        _next = _used.get((_k, _widx + 1), 0)
        _next += int(booking_mc_by_week.get(_k, {}).get(_widx + 1, 0) or 0)
        # สัปดาห์ก่อนตรวจซ้ำด้วย — _repair_gap_carry ลดไว้แล้ว แต่ด่านโควตา job (รันทีหลัง) ตัดเครื่องใหม่
        # ของสัปดาห์ก่อนได้โดยไม่ไล่ลด Hold ตาม → Hold กันเกินเครื่องอุ่นจริงอีกรอบ
        _prev = _used.get((_k, _widx - 1), 0)
        _prev += int(booking_mc_by_week.get(_k, {}).get(_widx - 1, 0) or 0)
        # สัปดาห์ข้าง ๆ ที่เป็นแถว Hold ด้วยกัน = เครื่องชุดเดียวกันจอดอุ่นอยู่ (ดูหมายเหตุหัวฟังก์ชัน)
        _next = max(_next, int(_held_by_week.get((_k, _widx + 1), 0) or 0))
        _prev = max(_prev, int(_held_by_week.get((_k, _widx - 1), 0) or 0))
        _keep = max(0, min(_held, _prev, _next))
        # สัปดาห์นี้มีงานเกาะเครื่องอยู่ → กันไว้อย่างน้อยเท่าที่มันใช้ (ดู _riders ข้างบน)
        _keep = max(_keep, min(_held, int(_riders.get((_k, _widx), 0) or 0)))
        _held_by_week[(_k, _widx)] = _keep
        if _keep >= _held:
            continue
        _rel = _held - _keep
        _r["ACTUAL_MC"] = _keep
        _r["CARRYOVER_MC"] = _keep
        _r["NEW_MC"] = 0
        _r["SETUP_DAYS"] = 0
        _week = int(pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce") or 0)
        _repair_release_pool(_r.get("MC_GROUP"), _r.get("MC_GUAGE"), _week, _r.get("ITEM_CODE"), _rel)
        _released += _rel
        # ลดเพราะสัปดาห์ก่อน → สัปดาห์ถัดไปอาจ carry เกินที่ Hold เหลือ ต้องไล่ลด carry ตาม (กันเครื่องผี)
        _casc = 0
        if _prev < _next:
            if _chains is None:
                _chains = {}
                for _cr in plan_rows:
                    _ck_r = _preempt_row_key(_cr)
                    _ci_r = _preempt_row_idx(_cr)
                    if _ck_r[0] and _ck_r[1] and _ci_r is not None:
                        _chains.setdefault(_ck_r, {}).setdefault(_ci_r, []).append(_cr)
            _casc = _cascade_carry_cut(plan_rows, _chains, _preempt_row_key(_r), _widx)
        print(f"   ⏸ [{_hold_remark_of(_r)} TRIM] {_k[0]} {_r.get('MC_GROUP')} G{_k[2]} W{_week}: "
              f"สัปดาห์ก่อนใช้ {_prev} / สัปดาห์ถัดไปใช้ {_next} เครื่อง → ลดที่กันไว้ {_held} → {_keep}"
              + (f" | ลด carry ต่อเนื่อง {_casc} แถว" if _casc else ""))
    if _released:
        print(f"\n⏸ Hold: คืนเครื่องที่กันเกินสัปดาห์ถัดไป {_released} เครื่อง-สัปดาห์\n")
    return _released


def _absorb_hold_into_production(plan_rows) -> int:
    """สัปดาห์ที่คีย์เดียวกันมีงานทอจริง → ไม่ใช่สัปดาห์ Hold: ย้ายเครื่องที่กันไว้ให้แถวงาน แล้วลบแถว Hold

    แถว Hold ถูกสร้างตอนวางใบแรกของ item (รู้แค่รอบผลิตของใบตัวเอง) ใบอื่นของ item เดียวกัน
    ที่วางทีหลังจึงลงมาทอสัปดาห์เดียวกันได้ ทั้งทางใช้เครื่องร่วม / เติม cap เหลือ / ด่านซ่อม
    — ทุกทางมองแถว Hold เป็น "เครื่องของ item นี้ที่เดินอยู่" แล้วเขียนแถวตัวเองเป็น ACTUAL_MC = 0
    ผลคือบน Gantt เห็นป้าย Hold ทับงานที่ทออยู่จริงบนเครื่องชุดเดียวกัน
    (เคสจริง 21 ก.ย. 2569 FD5DU34B0 RAO W49: Hold รอ target ของ 721274 กัน 2 เครื่อง
     + SC 722334 ทอ 1,215 kg ร่วมเครื่องชุดนั้นเพราะ RDD ก่อนรอบสต๊อก)
    กติกา user: "ต้องเช็คก่อนว่าจะต้องทอหรือ hold" → สัปดาห์มีงานทอ = สัปดาห์ผลิต

    เครื่องรวมของคีย์ในสัปดาห์นั้นเท่าเดิม (ย้ายจากแถว Hold ไปแถวงาน) → พูลไม่เปลี่ยน
    และลูกโซ่ carry สัปดาห์ถัดไปยังเห็นเครื่องจำนวนเดิม ; เครื่องที่ย้ายไปเป็นเครื่องอุ่น
    (CARRYOVER) ไม่ใช่ setup ใหม่
    ต้องรันหลัง _trim_color_gap_hold (ตัวนั้นใช้แถว Hold ข้างเคียงเป็นเพดานของโซ่)

    คืนจำนวนแถว Hold ที่ถูกรวมเข้าแถวงาน
    """
    if not (COLOR_GAP_ENABLED or CORE_HOLD_ENABLED) or not plan_rows:
        return 0

    def _num(_r, _c):
        return float(pd.to_numeric(_r.get(_c, 0), errors="coerce") or 0)

    # แถวงานจริงต่อ (คีย์ก้อน, idx สัปดาห์) — ไม่นับงานจ้างทอ (เครื่องอยู่ที่ผู้รับจ้าง)
    _prod = {}
    for _r in plan_rows:
        if _is_hold_row(_r) or _is_outsource_row(_r):
            continue
        if _num(_r, "PRODUCE_QTY") <= 0 and _num(_r, "ACTUAL_MC") <= 0:
            continue
        _widx = _preempt_row_idx(_r)
        if _widx is None:
            continue
        _k = (_color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE")), _widx)
        _prod.setdefault(_k, []).append(_r)

    _drop = set()
    for _h in plan_rows:
        if not _is_hold_row(_h):
            continue
        _widx = _preempt_row_idx(_h)
        if _widx is None:
            continue
        _k = (_color_gap_key(_h.get("ITEM_CODE"), _h.get("MC_GROUP"), _h.get("MC_GUAGE")), _widx)
        _rows = _prod.get(_k)
        if not _rows:
            continue
        _held = int(_num(_h, "ACTUAL_MC"))
        # เจ้าของเครื่อง = แถวที่ประกาศเครื่องมากสุด ไม่มีก็แถวที่ทอยอดมากสุด
        _owner = max(_rows, key=lambda _r: (_num(_r, "ACTUAL_MC"), _num(_r, "PRODUCE_QTY")))
        # เครื่อง "carry" ที่แถวงานประกาศไว้แล้วในสัปดาห์นี้ — ยุบด้วย max ภายในใบสั่งเดียวกัน
        # แล้วบวกข้ามใบ (สูตรเดียวกับบัญชีพูล _pool_blocks_from_plan แต่นับเฉพาะ carry)
        _by_sc = {}
        for _r in _rows:
            _sc_r = str(_r.get("SC_SO_NO", "")).strip()
            _by_sc[_sc_r] = max(_by_sc.get(_sc_r, 0), int(_num(_r, "CARRYOVER_MC")))
        _prod_carry = sum(_by_sc.values())
        # ⚠️ แถว Hold กับเครื่อง carry ของแถวงานสัปดาห์เดียวกันคือ "เครื่องชุดเดียวกัน" (ทั้งคู่ถือต่อ
        # จากสัปดาห์ก่อน) → ย้ายเฉพาะส่วนที่ carry ยังไม่ครอบ ไม่ใช่บวกทั้งก้อน
        # ไม่งั้นเครื่องของ item นั้นในสัปดาห์นั้นถูกนับเกิน แล้วด่านพูลไปตัดงานคนอื่นตามมา
        # เครื่องตั้งใหม่ (NEW_MC) เป็นคนละชุดกับเครื่องที่กันไว้เสมอ — เดิมเทียบกับ ACTUAL_MC ทั้งก้อน
        # (เคสจริง 30 ก.ย. 2569 FD5DU34B0 RAO W51: Hold 2 + ใบ 721274 ตั้งใหม่ 2 = ใช้จริง 4 ตัวตามยอด
        #  3,665 กก. แต่ถูกมองว่าครอบกันแล้ว ย้าย 0 ตัว → นับเหลือ 2 พูลว่างเกินจริง 2 ตัว)
        _held = max(0, _held - _prod_carry)
        if _held > 0:
            _had_mc = _num(_owner, "ACTUAL_MC") > 0
            _owner["ACTUAL_MC"] = int(_num(_owner, "ACTUAL_MC")) + _held
            _owner["CARRYOVER_MC"] = int(_num(_owner, "CARRYOVER_MC")) + _held
            _owner["REQUIRED_MC"] = max(int(_num(_owner, "REQUIRED_MC")), int(_owner["ACTUAL_MC"]))
            # แถวนี้ประกาศเครื่องเองแล้ว ไม่ได้ขี่เครื่องแถวอื่น
            _owner["MC_SHARED"] = 0
            _txt = f"ทอต่อบนเครื่องที่กันไว้ {_held} ตัว (เครื่องอุ่น ไม่ต้อง setup)"
            # แถวที่ประกาศเครื่องเองอยู่แล้ว (carry/setup) → ต่อท้ายข้อความเดิม ไม่ทับที่มาของเครื่องชุดเดิม
            _owner["REMARK"] = (
                f"{str(_owner.get('REMARK', '') or '').strip()} + {_txt}" if _had_mc
                else f"{_owner.get('MC_GROUP')}: {_txt}"
            )
            _own_sc = str(_owner.get("SC_SO_NO", "")).strip()
            _mc_now = int(_owner["ACTUAL_MC"])
            for _r in _rows:
                if _r is _owner or _num(_r, "ACTUAL_MC") > 0:
                    continue
                # แถวที่ขี่เครื่องแถว Hold อยู่ → ตอนนี้ขี่เครื่องของแถวเจ้าของแทน
                _r["MC_SHARED"] = _mc_now
                _r["REMARK"] = (
                    f"เติมเข้าเครื่องเดิม — ใช้เครื่อง {_mc_now} ตัวร่วมกับ SO {_own_sc} "
                    f"({_r.get('MC_GROUP')}) ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup"
                )
        _drop.add(id(_h))
        print(f"   ⏸ [HOLD→ทอ] {_k[0][0]} {_h.get('MC_GROUP')} G{_k[0][2]} W{_h.get('PLAN_WEEK')}: "
              f"สัปดาห์นี้มีงานทอจริง {len(_rows)} แถว → ลบ {_hold_remark_of(_h)} "
              f"ย้ายเครื่อง {_held} ตัวให้ SC {_owner.get('SC_SO_NO')}")

    if _drop:
        plan_rows[:] = [_r for _r in plan_rows if id(_r) not in _drop]
        print(f"\n⏸ Hold: สัปดาห์ที่มีงานทอจริง ไม่ใช่ Hold — รวมเข้าแถวงาน {len(_drop)} แถว\n")
    return len(_drop)


def _hold_warm_carry_fix(plan_rows) -> int:
    """สัปดาห์ที่กลับมาทอหลัง Hold ห้าม setup ใหม่เกินเครื่องที่กันไว้ — ย้าย new → carry

    เจตนาของแถว Hold คือ "กลับมาทอรอบหน้าโดยไม่ต้องตั้งเครื่องใหม่" แต่ลูปหลักคิด carry จาก
    เครื่องของใบตัวเอง (machines_in_use) ซึ่งน้อยกว่าที่ Hold กันไว้ได้ แล้วด่านเพิ่มเครื่อง
    (GRADUAL BOOST ฯลฯ) เติมส่วนต่างเป็น "เครื่องตั้งใหม่" ทั้งที่เครื่องชุดนั้นจอดอุ่นรออยู่
    (_color_gap_warm_split ในลูปหลักครอบได้เฉพาะเส้นทางที่ is_continuing — เคสจริง
     21 ก.ย. 2569 FD5DU34B0 RAO W16/2027: Hold กัน 2 → carry 1 + setup ใหม่ 1 เสีย 3 วัน)

    เครื่องรวมของแถวไม่เปลี่ยน (ย้ายจาก new ไป carry) → พูลเท่าเดิม ที่ลดคือวัน setup
    และโควตา job ของสัปดาห์นั้น ; ยอดผลิตไม่แตะ (เครื่องอุ่นทอได้มากกว่าเดิมเท่านั้น)

    คืนจำนวนเครื่องที่เปลี่ยนจาก "ตั้งใหม่" เป็น "เครื่องอุ่น"
    """
    if not (COLOR_GAP_ENABLED or CORE_HOLD_ENABLED) or not plan_rows:
        return 0

    def _num(_r, _c):
        return float(pd.to_numeric(_r.get(_c, 0), errors="coerce") or 0)

    _held_prev, _by_week = {}, {}
    for _r in plan_rows:
        _widx = _preempt_row_idx(_r)
        if _widx is None:
            continue
        _k = _color_gap_key(_r.get("ITEM_CODE"), _r.get("MC_GROUP"), _r.get("MC_GUAGE"))
        if _is_hold_row(_r):
            _held_prev[(_k, _widx)] = _held_prev.get((_k, _widx), 0) + int(_num(_r, "ACTUAL_MC"))
        else:
            _by_week.setdefault((_k, _widx), []).append(_r)

    _moved = 0
    for (_k, _widx), _rows in _by_week.items():
        _held = int(_held_prev.get((_k, _widx - 1), 0) or 0)
        if _held <= 0:
            continue
        _news = [_r for _r in _rows if _num(_r, "NEW_MC") > 0]
        if not _news:
            continue
        # เครื่องอุ่นที่แถวอื่นของสัปดาห์นี้รับไปแล้ว — ห้ามนับซ้ำ (หลายใบกลับมาทอพร้อมกัน)
        _free = _held - sum(int(_num(_r, "CARRYOVER_MC")) for _r in _rows)
        if _free <= 0:
            continue
        for _r in sorted(_news, key=lambda _x: -_num(_x, "NEW_MC")):
            if _free <= 0:
                break
            _mv = min(int(_num(_r, "NEW_MC")), _free)
            if _mv <= 0:
                continue
            _r["CARRYOVER_MC"] = int(_num(_r, "CARRYOVER_MC")) + _mv
            _r["NEW_MC"] = int(_num(_r, "NEW_MC")) - _mv
            _free -= _mv
            _moved += _mv
            if _num(_r, "NEW_MC") <= 0:
                # ไม่เหลือเครื่องตั้งใหม่ → ไม่เสียวัน setup และทอได้เต็มสัปดาห์
                _r["SETUP_DAYS"] = 0
                _wd_full = _num(_r, "ACTUAL_WORKING_DAYS") or _num(_r, "FACTORY_WORKING_DAYS")
                if _wd_full > 0:
                    _r["AVAILABLE_DAYS"] = _wd_full
            _r["REMARK"] = _build_plan_remark(
                _r.get("MC_GROUP"), int(_num(_r, "CARRYOVER_MC")), int(_num(_r, "NEW_MC")),
                _num(_r, "SETUP_DAYS"), booking_mc=int(_num(_r, "MC_BOOKING")),
            )
            print(f"   ⏸ [HOLD WARM FIX] {_k[0]} {_r.get('MC_GROUP')} G{_k[2]} "
                  f"W{_r.get('PLAN_WEEK')} SC {_r.get('SC_SO_NO')}: Hold สัปดาห์ก่อนกันไว้ {_held} "
                  f"→ ย้าย {_mv} เครื่องจาก 'ตั้งใหม่' เป็นเครื่องอุ่น (ไม่ต้อง setup)")

    if _moved:
        print(f"\n⏸ Hold: กลับมาทอบนเครื่องที่กันไว้ — ไม่ต้องตั้งเครื่องใหม่ {_moved} เครื่อง\n")
    return _moved


COLOR_SWAP_ENABLED = plan_config.get("KP_COLOR_SWAP")

# ช่องของแถวแผนที่เป็น "ของใบสั่ง" — ตอนสลับยอดระหว่างใบ ช่องเหล่านี้ย้ายตามยอด
# ช่องที่เหลือ (สัปดาห์ / เครื่อง / setup / วันทำงาน / cap) เป็นของ "ช่องเครื่อง" คงอยู่กับแถว
_SWAP_ORDER_FIELDS = (
    "SC_SO_NO", "ORDERS_QTY", "PENDING_PLAN", "ORDER_TYPE", "ORDER_DATE", "FG_WEEK",
    "TARGET_KNIT", "TARGET_IDX", "FLOOR_IDX", "IS_CORE_ITEM", "CUSTOMER", "LT_YARN",
    "DATE_IN", "EARLIEST_PLAN_WEEK", "SUB_COLOR", "PO_NO", "YARNPO", "RDD_WEEK",
    "SC_LINE_ID", "KP_NO", "NAY_COLOR", "COLOR_DESC", "LINE_REMARK", "PO_IN_DATE",
    "PO_IN_WEEK", "COLOR_PART", "YARN_STOCK",
)


def _swap_color_same_item(plan_rows) -> int:
    """งานสีที่ต้องรอคิวต่อท้ายงานไม่มีสีของผ้าตัวเดียวกันจนเลย target → สลับยอดบนเครื่องชุดเดิม

    ลำดับคิวเรียงสัปดาห์ target ก่อน priority (user 26 ส.ค. 2569) งานไม่มีสีที่ target เร็วกว่า
    จึงจองเครื่องของ item ไว้ยาว แล้วงานสีของ item เดียวกันโดน [ITEM CARRY WAIT] ดันไปต่อท้าย
    ส่วนการยึดเครื่องให้งานสี (_main_color_grab) ทำได้แค่ข้าม item — เคสนี้จึงไม่มีด่านไหนช่วย
    (เคสจริง 21 ก.ย. 2569 FD3BASKD35A0 RAO: 721918 งานสี TG W48 ได้ 90 กก. ที่ W50 เพราะ
     721917/722289 ที่ยังไม่เปิดสีจองเครื่องถึง W50)

    user เลือก (21 ก.ย. 2569): สลับเฉพาะตอนงานสีจะเลย target — ไม่เปลี่ยนลำดับคิวทั้งระบบ
    วิธี: ย้าย "เจ้าของยอด" ไม่ย้ายเครื่อง — แถวงานไม่มีสีสัปดาห์ ≤ target ของงานสีกลายเป็นของงานสี
    ส่วนแถวงานสีที่เลย target กลายเป็นของงานไม่มีสีแทน ยอดเท่ากันทั้งสองฝั่ง
    → เครื่อง / setup / พูล ทุกสัปดาห์เท่าเดิม ยอดรวมต่อใบเท่าเดิม (งานไม่มีสีแค่ช้าลง)
    แถวที่ถูกแบ่งยอด ส่วนที่แยกออกมาประกาศเครื่อง 0 + MC_SHARED (กติกาเดียวกับ "เติมเข้าเครื่องเดิม")

    ด่าน: เฉพาะงานในบ้าน (ไม่ใช่จ้างทอ / Hold) ; งานสี = COLOR_PART Yes ไม่ใช่ YD-ORDERS ;
    ผู้ให้ยอด = ไม่ใช่งานสี / core / LAB-DIP / YD ; สัปดาห์ผู้ให้อยู่ใน [พื้นแข็งของงานสี, target]
    และไม่อยู่ในช่วง freeze ; เลือกสัปดาห์ผู้ให้ที่ช้าสุดก่อน (งานไม่มีสีถูกเลื่อนน้อยสุด)
    คืนจำนวนครั้งที่สลับ
    """
    if not COLOR_SWAP_ENABLED or not plan_rows:
        return 0

    def _num(_r, _c):
        return float(pd.to_numeric(_r.get(_c, 0), errors="coerce") or 0)

    def _ival(_v):
        _n = pd.to_numeric(_v, errors="coerce")
        return None if pd.isna(_n) else int(_n)

    def _otype(_r):
        return str(_r.get("ORDER_TYPE", "") or "").strip().upper()

    def _is_color(_r):
        return str(_r.get("COLOR_PART", "") or "").strip() == "Yes" and _otype(_r) != "YD-ORDERS"

    def _is_donor(_r):
        return (
            str(_r.get("COLOR_PART", "") or "").strip() != "Yes"
            and _otype(_r) not in ("LAB-DIP", "YD-ORDERS")
            and not _row_is_core(_r)
        )

    def _split(_r, _qty, _fields):
        """แยกยอด _qty ออกจากแถว _r เป็นแถวใหม่ของใบ _fields (เครื่อง 0 ใช้ร่วมกับแถวเดิม)"""
        _mc_host = int(_num(_r, "ACTUAL_MC")) or int(_num(_r, "MC_SHARED"))
        _new = dict(_r)
        _new.update(_fields)
        _new.update({
            "PRODUCE_QTY": _qty,
            "PLAN_QTY": 0.0,
            "ACTUAL_MC": 0, "REQUIRED_MC": 0, "CARRYOVER_MC": 0, "NEW_MC": 0,
            "MC_BOOKING": 0, "SETUP_DAYS": 0, "MC_SHARED": _mc_host, "MC_HOLD_SPLIT": "",
            "REMARK": (
                f"เติมเข้าเครื่องเดิม — ใช้เครื่อง {_mc_host} ตัวร่วมกับ SO "
                f"{str(_r.get('SC_SO_NO', '')).strip()} ({_r.get('MC_GROUP')}) ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup"
            ),
        })
        _r["PRODUCE_QTY"] = _num(_r, "PRODUCE_QTY") - _qty
        return _new

    _freeze_idx = TODAY_IDX + FREEZE_WEEKS
    _groups: dict = {}
    for _r in plan_rows:
        if _is_hold_row(_r) or _is_outsource_row(_r) or _num(_r, "PRODUCE_QTY") <= 0:
            continue
        if _preempt_row_idx(_r) is None:
            continue
        _groups.setdefault(_preempt_row_key(_r), []).append(_r)

    _swaps = 0
    _added = []
    _touched = set()
    for _key, _rows in _groups.items():
        _late = sorted(
            (
                _r for _r in _rows
                if _is_color(_r) and _ival(_r.get("TARGET_IDX")) is not None
                and _preempt_row_idx(_r) > _ival(_r.get("TARGET_IDX"))
            ),
            key=lambda _r: (_ival(_r.get("TARGET_IDX")), _preempt_row_idx(_r)),
        )
        for _c in _late:
            _tgt = _ival(_c.get("TARGET_IDX"))
            _floor = max(_freeze_idx, _ival(_c.get("FLOOR_IDX")) or _freeze_idx)
            _donors = sorted(
                (
                    _d for _d in _rows
                    if _is_donor(_d) and _num(_d, "PRODUCE_QTY") > 1e-6
                    and _floor <= _preempt_row_idx(_d) <= _tgt
                ),
                key=lambda _d: -_preempt_row_idx(_d),
            )
            for _d in _donors:
                _need = _num(_c, "PRODUCE_QTY")
                if _need <= 1e-6:
                    break
                _x = min(_need, _num(_d, "PRODUCE_QTY"))
                _c_fields = {_f: _c.get(_f) for _f in _SWAP_ORDER_FIELDS}
                _d_fields = {_f: _d.get(_f) for _f in _SWAP_ORDER_FIELDS}
                _d_sc = str(_d.get("SC_SO_NO", "")).strip()
                _d_week = _d.get("PLAN_WEEK")
                # ฝั่งช่องที่ทันเป้า: ยอด _x ของงานไม่มีสี → เป็นของงานสี
                if _num(_d, "PRODUCE_QTY") - _x <= 1e-6:
                    _d.update(_c_fields)
                    _to_color = _d
                else:
                    _to_color = _split(_d, _x, _c_fields)
                    _added.append(_to_color)
                # ฝั่งช่องที่เลยเป้า: ยอด _x ของงานสี → เป็นของงานไม่มีสี (ที่ถูกเลื่อน)
                if _need - _x <= 1e-6:
                    _c.update(_d_fields)
                    _to_plain = _c
                else:
                    _to_plain = _split(_c, _x, _d_fields)
                    _added.append(_to_plain)
                for _r_sw, _why in (
                    (_to_color, f"งานสีแซงคิว SO {_d_sc} (ยังไม่เปิดสี) ให้ทันเป้า"),
                    (_to_plain, f"ถูกเลื่อนให้งานสี SO {_c_fields['SC_SO_NO']} ทอก่อน"),
                ):
                    _prev_lr = _clean_remark(_r_sw.get("LINE_REMARK"))
                    _r_sw["LINE_REMARK"] = f"{_prev_lr} | {_why}".strip(" |")
                print(
                    f"   🎨 [COLOR SWAP] {_key[0]} {_key[1]} G{_key[2]}: งานสี SO {_c_fields['SC_SO_NO']} "
                    f"(TG idx {_tgt}) {_x:,.1f} กก. W{_c.get('PLAN_WEEK')} → W{_d_week} "
                    f"แลกกับ SO {_d_sc} (ไม่มีสี) → W{_c.get('PLAN_WEEK')}"
                )
                _swaps += 1
                _touched.add(_plan_order_key(_to_color))
                _touched.add(_plan_order_key(_to_plain))
                # แถวที่แยกออกมาต้องอยู่ในกลุ่มด้วย ให้งานสีใบถัดไปเห็นเป็นผู้ให้ได้
                for _r_new in (_to_color, _to_plain):
                    if not any(_r_new is _r_old for _r_old in _rows):
                        _rows.append(_r_new)
                # ยอดของงานสีก้อนนี้ถูกยกไปช่องที่ทันเป้าหมดแล้ว (แถวเดิมกลายเป็นของใบไม่มีสี)
                if _to_plain is _c:
                    break

    if _added:
        plan_rows.extend(_added)
    if _swaps:
        # PLAN_QTY = "ยอดคงเหลือหลังแถวนี้" ของใบสั่ง (advisor อ่านเป็นของค้าง) — แถวที่เปลี่ยน
        # เจ้าของแล้วยังถือเลขของใบเดิมอยู่ จึงต้องไล่คิดใหม่ทั้งใบที่ถูกแตะ
        for _ok in _touched:
            _o_rows = sorted(
                (_r for _r in plan_rows if _plan_order_key(_r) == _ok
                 and _preempt_row_idx(_r) is not None and not _is_hold_row(_r)),
                key=_preempt_row_idx,
            )
            if not _o_rows:
                continue
            _left = _num(_o_rows[0], "PENDING_PLAN")
            for _r in _o_rows:
                _left = max(0.0, _left - _num(_r, "PRODUCE_QTY"))
                _r["PLAN_QTY"] = _left
        print(f"\n🎨 งานสีแซงคิวงานไม่มีสีของผ้าตัวเดียวกัน {_swaps} ครั้ง (เครื่อง/setup เท่าเดิม)\n")
    return _swaps


# =========================
# รัน planning รอบเดียว (S9 เหลือเฉพาะ routing ของ item ใน "S9 Only")
# =========================
plans = _run_planning_loop()

# ⚠️ user สั่ง 2 ก.ย. 2569: **ตัดด่านซ่อมที่ "จัดทรง" ออกจากระบบทั้งหมด** — ทรงเครื่อง
# ต้องถูกตั้งแต่รอบวางแผนหลัก ไม่ใช่ให้ด่านท้ายมาปะทีหลัง (โค้ดถูกลบทิ้งแล้ว ไม่ได้แค่ปิด)
#   ลบ: color preempt (+ core lending) / run continuity / board balance / ramp shape
#       → สิทธิ์ "งานสียึดเครื่องจากงานที่ยังไม่ให้สี" ย้ายเข้าลูปหลักแล้ว
#         (MAIN_COLOR_GRAB_ENABLED / _main_color_grab)
#   เหลือเฉพาะด่านที่คุม "กติกาแข็ง" ซึ่งไม่ได้จัดทรงงาน:
#       gap carry     — carry ข้ามสัปดาห์ที่เว้นต้องมีเครื่องจอดว่างจริง ไม่งั้นเป็น setup
#       pool overflow — เครื่องที่แผนใช้ห้ามเกินจำนวนเครื่องที่มีจริง
#       job overflow  — setup ต่อสัปดาห์ห้ามเกินโควตา (33/44/13)
# ทุกด่านตัดสินใจจากบัญชีพูล → ปรับบัญชีให้ตรงกับแถวแผนจริงก่อนเข้าแต่ละด่าน
# ตัดสินว่าสัปดาห์ไหน "ทอ" หรือ "Hold" หลังวางครบทุกใบ — ก่อนด่านซ่อมทุกด่าน เพื่อให้ด่านซ่อม
# เห็นเครื่องอยู่บนแถวงานที่ทอจริง ไม่ใช่บนแถว Hold ที่ตัวเองตัดทิ้งได้ (ดู _absorb_hold_into_production)
_hold_absorbed = _absorb_hold_into_production(plans)
# กลับมาทอหลัง Hold ต้องใช้เครื่องที่กันไว้ ไม่ใช่ตั้งเครื่องใหม่ (ทำก่อนด่านโควตา job
# เพื่อให้โควตา setup ของสัปดาห์นั้นถูกคืนให้งานอื่นได้)
_hold_warm_fixed = _hold_warm_carry_fix(plans)
_resync_pool_ledger()
_gap_fixed = _repair_gap_carry(plans)

_resync_pool_ledger()
_pool_cut = _repair_pool_overflow(plans, enforce=True)   # เปิดถาวร ไม่มีสวิตช์บนเว็บ
_job_cut = _repair_job_overflow(plans) if JOB_ENFORCE else 0
# Hold รอสี: ลดให้เท่าที่สัปดาห์ถัดไปใช้จริง — หลังด่านซ่อมทุกด่าน เพราะด่านซ่อมยังตัดเครื่อง
# สัปดาห์ถัดไปได้ ; ก่อน audit เพื่อให้ตรวจพูลด้วยตัวเลขหลังคืนเครื่องแล้ว
# สัปดาห์ Hold ที่ด่านซ่อมข้างบนเพิ่งเทงานลงมา (gap repair เปิดแถวใช้เครื่องร่วม) → รวมอีกรอบ
# ต้องก่อน trim: trim นับเฉพาะ ACTUAL_MC ของแถวงาน แถวที่ขี่เครื่อง Hold อยู่ (ACTUAL_MC = 0)
# จะทำให้มันเห็นสัปดาห์ถัดไปเป็น 0 แล้วตัด Hold ทิ้ง → งานเหลือทอบนเครื่อง 0 ตัว (เครื่องผี)
_hold_absorbed += _absorb_hold_into_production(plans)
_cg_trimmed = _trim_color_gap_hold(plans)
# งานสีที่ต่อคิวหลังงานไม่มีสีของผ้าตัวเดียวกันจนเลย target → สลับยอดบนเครื่องเดิม
# หลังด่านซ่อมทุกด่าน (ยอด/เครื่องนิ่งแล้ว) ; ไม่แตะเครื่อง จึงไม่กระทบ audit พูลข้างล่าง
_color_swapped = _swap_color_same_item(plans)

# ตรวจซ้ำด้วยเครื่องที่อยู่ในแผนจริง (ไม่ใช่บัญชีสะสม) — รอบซ่อมข้างบนเชื่อบัญชีภายใน
# ถ้าบัญชีต่ำกว่าจริงมันจะมองไม่เห็นว่าเกิน ตัวนี้จับให้เห็นก่อนเขียนไฟล์
_audit_over = _audit_pool_usage(plans)

if _gap_fixed:
    print(f"\n🔧 ซ่อม carry ข้ามสัปดาห์ที่เว้น {_gap_fixed} จุด (เครื่องกลาง gap ถูกใช้ไปแล้ว → setup ใหม่)\n")

# =========================
# เก็บกวาดแถวที่ไม่ได้ผลิตจริง ("แถวผี")
# ด่านซ่อมข้างบน (color preempt / pool overflow / job quota / cascade carry) ตัดเครื่องโดย
# "แก้ค่าในแถวเดิมให้เป็น 0" แต่ไม่ลบแถวทิ้ง → เหลือแถวที่เครื่อง 0 ผลิต 0 setup 0 ค้างอยู่
# ซึ่งไม่ใช่แผนจริงเลย แต่หลุดเข้าไฟล์แผนและไปโผล่บนหน้าเว็บ ทำให้อ่านแล้วสับสน
# (ลูปวางแผนหลักไม่เคยสร้างแถวแบบนี้ — มีกติกา produce <= 0 แล้วไม่เพิ่มแถว)
#
# ⚠️ ลบได้ตรงนี้เท่านั้น ระหว่างรอบซ่อมยังลบไม่ได้ เพราะแถวกลางลูกโซ่ทำหน้าที่ "สะพาน carry"
# ถ้าลบผิดจังหวะ _repair_gap_carry จะเห็นเป็น gap แล้วแปลง carry ทั้งชุดเป็น setup ใหม่
# (ดูคอมเมนต์เตือนใน _repair_color_preempt) — ตอนนี้รอบซ่อมจบครบทุกด่านแล้วจึงปลอดภัย
# =========================
def _row_is_noop(_r) -> bool:
    """แถวที่ไม่ได้ทำอะไรเลย: ไม่มีเครื่อง ไม่ผลิต ไม่ setup"""
    for _k in ("ACTUAL_MC", "PRODUCE_QTY", "NEW_MC", "CARRYOVER_MC", "SETUP_DAYS"):
        if float(pd.to_numeric(_r.get(_k, 0), errors="coerce") or 0) > 1e-6:
            return False
    return True


# แถวกันเครื่อง (Hold รอสี / Hold รอ target) ต้องไม่มีทั้งยอดผลิตและวัน setup ไม่ว่าด่านซ่อมไหนจะแตะมัน
# — เป็นตาข่ายกันพลาดชั้นสุดท้าย (ด่านที่รู้จักแถวนี้อยู่แล้วจัดการถูกตั้งแต่ต้น)
_cg_fix = 0
for _r in plans:
    if not _is_hold_row(_r):
        continue
    if (float(pd.to_numeric(_r.get("NEW_MC", 0), errors="coerce") or 0) > 0
            or float(pd.to_numeric(_r.get("SETUP_DAYS", 0), errors="coerce") or 0) > 0
            or float(pd.to_numeric(_r.get("PRODUCE_QTY", 0), errors="coerce") or 0) > 0):
        _cg_fix += 1
    _r["NEW_MC"] = 0
    _r["SETUP_DAYS"] = 0
    _r["PRODUCE_QTY"] = 0
    _r["CARRYOVER_MC"] = int(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0)
    _r["REMARK"] = _hold_remark_of(_r)
    _r["LINE_REMARK"] = _hold_remark_of(_r)
if _cg_fix:
    print(f"⏸ Hold: ปรับแถวกันเครื่องกลับเป็น 'ไม่ผลิต/ไม่ setup' {_cg_fix} แถว")

_noop_rows = [_r for _r in plans if _row_is_noop(_r)]
if _noop_rows:
    plans = [_r for _r in plans if not _row_is_noop(_r)]
    print(f"\n🧹 ลบแถวที่ไม่ได้ผลิตจริงออก {len(_noop_rows)} แถว (ถูกด่านซ่อมตัดเครื่องจนเหลือ 0)")
    for _r in _noop_rows[:20]:
        _w_noop = int(float(pd.to_numeric(_r.get("PLAN_WEEK", 0), errors="coerce") or 0))
        _why_noop = str(_r.get("LINE_REMARK") or _r.get("REMARK") or "").strip()
        print(
            f"   • {_r.get('ITEM_CODE', '')} SC {_r.get('SC_SO_NO', '')} "
            f"{_r.get('MC_GROUP', '')} W{_w_noop}"
            + (f" — {_why_noop}" if _why_noop and _why_noop.lower() != "nan" else "")
        )
    if len(_noop_rows) > 20:
        print(f"   ... และอีก {len(_noop_rows) - 20} แถว")
    print()

# =========================
# สรุปงานที่วางไม่ได้เพราะพูลเครื่องเต็ม — ไม่งั้นงานหายจากแผนเงียบๆ
# ❌ = ไม่ได้วางเลยทั้ง SC  |  ↪️ = เลื่อนไปวางสัปดาห์อื่นได้
# =========================
if no_mc_skips:
    _planned_keys = {
        (str(_p.get("ITEM_CODE", "")), str(_p.get("SC_SO_NO", ""))) for _p in plans
    }
    _skips_by_key = {}
    for _s in no_mc_skips:
        _skips_by_key.setdefault((str(_s["item"]), str(_s["sc"])), []).append(_s)
    print("\n" + "=" * 78)
    print("⚠️  งานที่เครื่องไม่พอ (booking จองเกินเครื่องจริง → carry ต่อไม่ได้)")
    print("=" * 78)
    for (_it, _sc), _rows in sorted(_skips_by_key.items()):
        _never_planned = (_it, _sc) not in _planned_keys
        _mark = "❌ ไม่ได้วางเลย" if _never_planned else "↪️  เลื่อนสัปดาห์"
        print(f"  {_mark} | {_it} SC {_sc} | {_rows[0]['mc_group']} เกจ {_rows[0]['gauge']}")
        for _r in _rows[:4]:
            print(
                f"      setup W{_r['setup_week']} → ติดที่ W{_r['blocked_week']} "
                f"(พูลเหลือ {_r['remain']} ต้องใช้ {_r['need']})"
            )
        if len(_rows) > 4:
            print(f"      ... และอีก {len(_rows) - 4} สัปดาห์ที่ลองแล้วไม่ได้")
    print("=" * 78 + "\n")

# EXPORT
# =========================
plan_df = pd.DataFrame(plans)
DATA_PLAN_DIR.mkdir(exist_ok=True)


def _split_yw_out(_df, _pairs):
    """ก่อนเขียนไฟล์: แยกคอลัมน์สัปดาห์ที่เป็น YW กลับเป็นเลขสัปดาห์ + คอลัมน์ปี (แก้ในที่)

    ภายในไฟล์นี้คีย์สัปดาห์เป็น YW ทั้งหมด แต่ไฟล์แผนที่ Replan/หน้าเว็บอ่านยังเป็นรูปเดิม
    (PLAN_WEEK + PLAN_YEAR, TARGET_KNIT + TARGET_YEAR ...) ; ค่าที่ไม่ใช่ YW (เลขเปล่า/วันที่/ว่าง) คงเดิม
    _pairs = [(คอลัมน์สัปดาห์, คอลัมน์ปี หรือ None)]"""
    if not isinstance(_df, pd.DataFrame) or _df.empty:
        return _df
    for _wc, _yc in _pairs:
        if _wc not in _df.columns:
            continue
        _vals = _df[_wc].tolist()
        _nums = pd.to_numeric(_df[_wc], errors="coerce").tolist()
        _is = [pd.notna(_n) and _n >= 100000 for _n in _nums]
        if not any(_is):
            continue
        if _yc:
            _old_y = _df[_yc].tolist() if _yc in _df.columns else [None] * len(_df)
            _df[_yc] = pd.array(
                [int(_n) // 100 if _f else (None if _y is None or pd.isna(_y) else int(_y))
                 for _n, _f, _y in zip(_nums, _is, _old_y)],
                dtype="Int64",
            )
        _df[_wc] = [int(_n) % 100 if _f else _v for _n, _f, _v in zip(_nums, _is, _vals)]
    return _df


def _kp_no_last(_df):
    """ย้าย KP_NO ไปคอลัมน์ท้ายสุด — ผู้ใช้ภายนอกอ่านชีท PLAN ตามตำแหน่งคอลัมน์ (format เดิมจบที่ PLAN_YEAR = AN)
    KP_NO ที่แทรกหลัง SC_LINE_ID ทำให้คอลัมน์ถัดไปเลื่อน 1 ช่อง → ไว้ท้ายสุดไม่ให้อะไรขยับ (user 22 ก.ย. 2569)"""
    if not isinstance(_df, pd.DataFrame) or "KP_NO" not in _df.columns:
        return _df
    return _df[[c for c in _df.columns if c != "KP_NO"] + ["KP_NO"]]


_PLAN_YW_COLS = [("PLAN_WEEK", "PLAN_YEAR"), ("TARGET_KNIT", "TARGET_YEAR"),
                 ("EARLIEST_PLAN_WEEK", None), ("BOOKING_WEEK_FROM", None), ("BOOKING_WEEK_TO", None)]

# สร้าง week -> year lookup จาก calendar_week (รองรับข้ามปี)
_week_year_lookup = {}
for _, row in calendar_week.iterrows():
    w = int(row["WEEK"])
    y = int(row["YEAR"])
    if w not in _week_year_lookup:
        _week_year_lookup[w] = y
    else:
        # ถ้า week ซ้ำข้ามปี → ใช้ปีที่ใกล้ TODAY.year ที่สุด
        if abs(y - TODAY.year) < abs(_week_year_lookup[w] - TODAY.year):
            _week_year_lookup[w] = y



# เพิ่ม PLAN_YEAR ให้ new plan_df
# ใช้ PLAN_IDX (row index บน timeline ที่เก็บไว้ตอนวางแผน) เป็นหลัก เพราะรู้ปีแน่นอน
# _week_year_lookup เป็น fallback เท่านั้น — มันเดาปีจาก "ใกล้ TODAY.year ที่สุด"
# ซึ่งผิดกับแผนที่คร่อมปี (เช่น แผนเดิน W46→W53 แล้วต่อ W1 = ปีหน้า
# แต่ lookup จะตอบปีปัจจุบัน → Gantt/รายงานย้อนอดีต 1 ปี)
if not plan_df.empty and "PLAN_WEEK" in plan_df.columns:
    # PLAN_WEEK เป็น YW → ปีอยู่ในตัว ; เลขเปล่า (ไม่ควรมี) ค่อยเดาจาก lookup
    _fallback_year = plan_df["PLAN_WEEK"].map(
        lambda w: yw_year(w) if yw_year(w) is not None else _week_year_lookup.get(w)
    ).fillna(TODAY.year)
    if "PLAN_IDX" in plan_df.columns:
        _idx_year = plan_df["PLAN_IDX"].map(
            lambda i: int(calendar_week.iloc[int(i)]["YEAR"])
            if pd.notna(i) and 0 <= int(i) < len(calendar_week)
            else None
        )
        plan_df["PLAN_YEAR"] = _idx_year.fillna(_fallback_year).astype(int)
    else:
        plan_df["PLAN_YEAR"] = _fallback_year.astype(int)

# เพิ่ม TARGET_YEAR = ปีของ TARGET_KNIT (เดดไลน์ทอ)
# TARGET_KNIT เก็บเป็นเลขสัปดาห์เปล่า เทียบกับ PLAN_WEEK ตรง ๆ จะพังเมื่อแผนคร่อมปี:
#   target W46/2026 แต่วางงานที่ W2/2027 → 2 > 46 เป็นเท็จ → หลุดเป็น "ทัน" ทั้งที่สาย 8 สัปดาห์
#   target W2/2027 แต่วางที่ W50/2026   → 50 > 2 เป็นจริง → ขึ้น "ไม่ทัน" ทั้งที่ยังทัน
# TARGET_IDX = index บน timeline ที่คำนวณตอนหา RDD จึงรู้ปีแน่นอน (ไม่ต้องเดาจากเลขสัปดาห์)
if not plan_df.empty and "TARGET_IDX" in plan_df.columns:
    plan_df["TARGET_YEAR"] = plan_df["TARGET_IDX"].map(
        lambda i: int(calendar_week.iloc[int(i)]["YEAR"])
        if pd.notna(i) and 0 <= int(i) < len(calendar_week)
        else None
    ).astype("Int64")

# PLAN_IDX / TARGET_IDX เป็นคอลัมน์ใช้ภายในล้วน (มีไว้เติม PLAN_YEAR / TARGET_YEAR ข้างบน)
# ทิ้งทันทีหลังใช้เสร็จ ไม่ให้หลุดไปโผล่ในชีท PLAN / หน้าจอ ; FLOOR_IDX ใช้แค่ใน _swap_color_same_item
plan_df = plan_df.drop(columns=["PLAN_IDX", "TARGET_IDX", "FLOOR_IDX"], errors="ignore")


# เพิ่ม CAT column (= Type_1 ของ MC_GROUP จาก MasterMC)
if not plan_df.empty and "MC_GROUP" in plan_df.columns:
    plan_df["CAT"] = plan_df.apply(
        lambda r: _mc_to_type1(str(r.get("MC_GROUP", "")), r.get("MC_GUAGE")), axis=1
    )
    # S9 routing: CAT ของแถวจ้างทอเป็นค่าคงที่ (ไม่ใช่ CAT เครื่องในบ้าน) — ตัวชี้จริงคือ S9_ROUTING
    if "S9_ROUTING" in plan_df.columns:
        plan_df.loc[plan_df["S9_ROUTING"] == True, "CAT"] = "COMKN"

# เพิ่ม CYLINDER_CHANGE column — mark เฉพาะ item ที่ trigger การเปลี่ยนจริง
plan_df["CYLINDER_CHANGE"] = ""
_cyl_trigger_keys = set()
for (_ciw, _cii, _cimg) in _cylinder_change_for_item:
    # สัปดาห์ถัดจากที่สั่งเปลี่ยน — เดินบนปฏิทิน (YW บวก 1 ตรง ๆ ข้ามปีไม่ได้)
    _cyl_trigger_keys.add((_TL.shift(_ciw, 1), _cii, str(_cimg).strip().upper()))
for _ci, _cr in plan_df.iterrows():
    _ck_week = int(_cr.get("PLAN_WEEK") or 0)
    _ck_item = str(_cr.get("ITEM_CODE", "")).strip().upper()
    _ck_mc = str(_cr.get("MC_GROUP", "")).strip().upper()
    if (_ck_week, _ck_item, _ck_mc) in _cyl_trigger_keys:
        plan_df.at[_ci, "CYLINDER_CHANGE"] = "Yes"

# =========================
# LOAD BALANCING - Apply to final plan
# =========================
# ปิดการใช้ Load Balancing ตอนสรุปผลลัพธ์เพื่อรักษาความสามารถในการผลิตตาม ORDERS_QTY
# Load Balancing ทำงานเฉพาะตอนเลือกเครื่องเพื่อกระจายงานอย่างสมดุล
if False and USE_LOAD_BALANCING and not plan_df.empty:
    # Group plans by week for load balancing
    balanced_plans = []
    for week in sorted(plan_df['PLAN_WEEK'].unique()):
        week_plans = plan_df[plan_df['PLAN_WEEK'] == week].to_dict('records')

    
        # Apply load balancing to this week's plans
        balanced_week_plans = _analyze_and_balance_load(week, week_plans)
        balanced_plans.extend(balanced_week_plans)
    # Replace original plans with balanced plans
    plan_df = pd.DataFrame(balanced_plans)
    print(f"✅ Load balancing completed. Total plans after balancing: {len(balanced_plans)}")

# สรุปแผนใหม่
if not plan_df.empty:
    print("📊 สรุปแผนการผลิตใหม่:")
    # Coerce `REQUIRED_MC` to numeric to avoid string concatenation when summing
    new_summary = (
        plan_df.assign(
            _req_mc=pd.to_numeric(plan_df["REQUIRED_MC"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        .groupby(["MC_GROUP", "PLAN_WEEK"])["_req_mc"]
        .sum()
        .reset_index()
        .rename(columns={"_req_mc": "REQUIRED_MC"})
    )
    for week in sorted(new_summary["PLAN_WEEK"].unique()):
        week_data = new_summary[new_summary["PLAN_WEEK"] == week]
        week_sum = week_data["REQUIRED_MC"].sum()
        if week == 19:  # Debug Week 19
            print(f"🔍 Week {week} details:")
            for _, row in week_data.iterrows():
                print(f"   {row['MC_GROUP']}: {row['REQUIRED_MC']} machines")
            print(f"   Total: {week_sum}")
        print(f"  Week {week}: REQUIRED_MC = {week_sum}")
else:
    print("⚠️ ไม่มีแผนการผลิตที่สามารถสร้างได้")
    print()

# =========================
# REMAINING JOBS PER WEEK
# =========================
_CAPACITY_MAP = dict(JOB_CAP)

def _sum_by_type(job_dict_by_week, week):
    """รวม jobs ตาม factory_type สำหรับ week ที่ระบุ (factory-wide รวมทุก MC_GROUP ใน type)"""
    result: dict = {}
    for _mc_group, _jobs in job_dict_by_week.get(week, {}).items():
        _type_key = _get_type_key_for_mc(_mc_group)
        result[_type_key] = result.get(_type_key, 0) + _jobs
    return result

# 🔧 FIX: NEW_JOBS = setup จริงจากแผนสุดท้าย (plan_df.NEW_MC) เพื่อให้ตรงกับ SETUP_TRACKING
# เดิมคิดจาก weekly_job_usage (ตัวนับระหว่างวาง) ซึ่งไม่สะท้อน balancing ท้ายสุด → นับ job เกินจริง
def _build_remaining_df(plan_frame):
    """สร้าง REMAINING_JOBS จากแผนที่ระบุ — NEW จาก NEW_MC ของ frame, OLD จาก booking baseline
    (weekly_job_usage_old เป็น booking baseline ตัวเดียว ใช้ร่วมได้ทั้ง with-S9 และ no-S9)"""
    # ⚠️ item เดียวกัน เครื่อง+เกจเดียวกัน สัปดาห์เดียวกัน แต่คนละ SC = "เครื่องชุดเดียวกัน"
    # ที่ Planning เขียนค่า NEW_MC ซ้ำทุกแถว (ไม่ได้แยกเครื่องต่อแถว) → ต้องยุบเป็นค่าเดียว (max)
    # ก่อนรวม ไม่งั้นนับ job เป็นเท่าตัว (เกณฑ์เดียวกับที่หน้าเว็บใช้ใน PlanGantt.planNewByWeekType)
    _plan_new_by_key: dict = {}
    if not plan_frame.empty and "NEW_MC" in plan_frame.columns:
        _pnm_series = pd.to_numeric(plan_frame["NEW_MC"], errors="coerce").fillna(0).astype(int)
        for _i, _pr in plan_frame.iterrows():
            _nm = int(_pnm_series.loc[_i])
            if _nm <= 0:
                continue
            # งานจ้างทอไม่ได้ตั้งเครื่องในบ้าน → ไม่กินโควตา setup (เกณฑ์เดียวกับ _repair_job_overflow)
            if _is_outsource_row(_pr):
                continue
            _w = int(pd.to_numeric(_pr.get("PLAN_WEEK"), errors="coerce") or 0)
            _mcg = str(_pr.get("MC_GROUP", "")).strip().upper()
            _tk = _get_type_key_for_mc(_mcg)
            _k = (
                _w,
                _tk,
                str(_pr.get("ITEM_CODE", "")).strip().upper(),
                _mcg,
                _normalize_gauge(_pr.get("MC_GUAGE", "")),
            )
            _plan_new_by_key[_k] = max(_plan_new_by_key.get(_k, 0), _nm)
    _plan_new_by_wt: dict = {}
    for (_w, _tk, _it, _mcg, _gg), _nm in _plan_new_by_key.items():
        _plan_new_by_wt[(_w, _tk)] = _plan_new_by_wt.get((_w, _tk), 0) + _nm

    # OLD จาก booking baseline (weekly_job_usage_old) — ตรงกับ SETUP_TRACKING OLD
    _all_weeks = sorted(
        set(list(weekly_job_usage_old.keys()) + [_w for (_w, _t) in _plan_new_by_wt])
    )
    _remaining_rows = []
    for _week in _all_weeks:
        _old_by_type = _sum_by_type(weekly_job_usage_old, _week)
        _new_by_type = {}
        for (_w, _tk), _v in _plan_new_by_wt.items():
            if _w == _week:
                _new_by_type[_tk] = _new_by_type.get(_tk, 0) + _v
        _all_types = set(list(_old_by_type.keys()) + list(_new_by_type.keys()))
        for _type_key in sorted(_all_types):
            _old_used = _old_by_type.get(_type_key, 0)
            _new_used = _new_by_type.get(_type_key, 0)
            _total_used = _old_used + _new_used
            _cap = _CAPACITY_MAP.get(_type_key, None)
            _rem = (_cap - _total_used) if _cap is not None else None
            _remaining_rows.append(
                {
                    "WEEK": _week,
                    "TYPE": _type_key,
                    "OLD_JOBS": _old_used,
                    "NEW_JOBS": _new_used,
                    "TOTAL_JOBS": _total_used,
                    "CAPACITY": _cap,
                    "REMAINING_JOBS": _rem,
                }
            )
    return (
        pd.DataFrame(_remaining_rows)
        if _remaining_rows
        else pd.DataFrame(
            columns=[
                "WEEK",
                "TYPE",
                "OLD_JOBS",
                "NEW_JOBS",
                "TOTAL_JOBS",
                "CAPACITY",
                "REMAINING_JOBS",
            ]
        )
    )

remaining_df = _build_remaining_df(plan_df)
print("📋 สรุป Remaining Jobs ต่อ Week (factory-wide ต่อ Type, OLD + NEW):")
if not remaining_df.empty:
    for _week in sorted(remaining_df["WEEK"].unique()):
        _wdf = remaining_df[remaining_df["WEEK"] == _week]
        print(f"   Week {_week}:")
        for _, _row in _wdf.iterrows():
            _cap_s = str(int(_row["CAPACITY"])) if pd.notna(_row["CAPACITY"]) else "-"
            _rem_v = _row["REMAINING_JOBS"] if pd.notna(_row["REMAINING_JOBS"]) else 1
            _rem_s = str(int(_rem_v)) if pd.notna(_row["REMAINING_JOBS"]) else "-"
            _icon = "🔴" if _rem_v < 0 else ("🟡" if _rem_v <= 5 else "🟢")
            print(
                f"     {_icon} {_row['TYPE']}: old={int(_row['OLD_JOBS'])} + new={int(_row['NEW_JOBS'])} = {int(_row['TOTAL_JOBS'])}/{_cap_s}  เหลือ {_rem_s} jobs"
            )
    print()
else:
    print("   (ไม่มีข้อมูล)")
    print()
# แสดง item ที่ไม่มี CAP
if _skip_no_cap:
    print(f"\n⚠️  Items ที่ไม่พบ CAP data ({len(_skip_no_cap)} รายการ) → ไม่ได้วางแผน:")
    for _s in _skip_no_cap:
        print(f"   - {_s['ITEM_CODE']} (SC/SO:{_s['SC_SO_NO']})")
    print(f"   กรุณาเพิ่มใน item_cap2025.xlsx")
    print()

# แสดง orders ที่ไม่มี MC GROUP
if _skip_no_mc_group:
    print(f"\n⚠️  Orders ที่ไม่มี MC GROUP ({len(_skip_no_mc_group)} รายการ) → ไม่ได้วางแผน:")
    for _s in _skip_no_mc_group:
        print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']}")
    print()

# แสดง orders ที่ MC GROUP ไม่มี FACTORY_TYPE
if _skip_no_factory:
    print(f"\n⚠️  Orders ที่ MC GROUP ไม่มี FACTORY_TYPE ({len(_skip_no_factory)} รายการ) → ไม่ได้วางแผน:")
    for _s in _skip_no_factory:
        print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']} | MC={_s['MC_GROUP']}")

# แสดง orders ที่ MC+Gauge ไม่มีใน MasterMC
if _skip_not_in_master:
    print(f"\n⚠️  Orders ที่ MC+Gauge ไม่มีใน MasterMC ({len(_skip_not_in_master)} รายการ) → ไม่ได้วางแผน:")
    for _s in _skip_not_in_master:
        print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']} | MC={_s['MC_GROUP']} G{_s['GAUGE']}")
    print()

# แสดง orders ที่เข้า loop วางแผนแล้วแต่วางไม่ครบ (เครื่องไม่พอ/หมดสัปดาห์/ชน safety guard)
if _skip_qty_short:
    print(f"\n⚠️  Orders ที่วางแผนไม่ครบ ({len(_skip_qty_short)} รายการ) → เหลือ qty ที่ยังไม่ได้วาง:")
    for _s in _skip_qty_short:
        print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']} | เหลือ {_s['UNPLANNED_QTY']:.0f} | {_s['REASON']}")
    print()

# แถวที่ถูกกรองออกตั้งแต่ Order.py (ไม่เคยเข้า planning loop) — เอาเฉพาะที่ยังมีของค้าง
# กติกาเดียวกับ order ปกติ: Pending Plan <= 0 = วางแผนครบแล้ว ไม่ถือว่าค้าง
_excluded_rows = []
if not _excluded_orders.empty and "Pending Plan" in _excluded_orders.columns:
    _exc_pend = pd.to_numeric(_excluded_orders["Pending Plan"], errors="coerce").fillna(0)
    for _, _er in _excluded_orders[_exc_pend > 0].iterrows():
        _excluded_rows.append(
            _unplanned_row(_er, _er.get("REASON", "ถูกกรองออกก่อนวางแผน"))
        )
    if _excluded_rows:
        print(f"\n⚠️  Orders ที่ถูกกรองออกก่อนวางแผน ({len(_excluded_rows)} รายการ) → เข้าชีท UNPLANNED:")
        for _s in _excluded_rows:
            print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']} | เหลือ {_s['UNPLANNED_QTY']:.0f} | {_s['REASON']}")
        print()

# รวม unplanned orders เป็น DataFrame — คอลัมน์ตาม UNPLANNED_COLS (ชุดเดิม + DATE_IN)
_unplanned_rows = (
    _skip_no_mc_group + _skip_no_factory + _skip_not_in_master
    + _skip_no_cap + _skip_qty_short + _excluded_rows
)
_unplanned_rows_all = len(_unplanned_rows)
_unplanned_rows = [
    _r for _r in _unplanned_rows
    if not _is_blank_order_row(_r.get("ITEM_CODE"), _r.get("UNPLANNED_QTY"))
]
_blank_order_rows += _unplanned_rows_all - len(_unplanned_rows)
if _blank_order_rows:
    print(f"🧹 ข้ามแถวว่าง (ไม่มี item + ยอด 0) {_blank_order_rows} แถว → ไม่ลงชีท UNPLANNED "
          f"(ข้อมูลต้นทางมีแถวว่างหลุดมา)")
_unplanned_df = pd.DataFrame(_unplanned_rows, columns=UNPLANNED_COLS)
print("Weekly production planning completed")
print(f"Output: {OUTPUT_FILE}")
print(f"Total rows: {len(plan_df)}")
# =========================
# EXPORT COMBINED (OLD + NEW)
# =========================
# รอบ replan เขียนไฟล์ combined แยก — ไม่ทับของรอบปกติที่หน้าเว็บ/ไฟล์แผนหลักอ้างถึง
COMBINED_FILE = DATA_PLAN_DIR / (
    "weekly_production_plan_combined_replan.xlsx" if REPLAN_MODE
    else "weekly_production_plan_combined_filtered.xlsx"
)
new_df = plan_df.copy()
if "PLAN_SOURCE" not in new_df.columns:
    new_df.insert(0, "PLAN_SOURCE", "NEW")
# _yw_of / _target_knit_of ย้ายขึ้นไปอยู่ใต้ target_offset_weeks_for_row (ต้นไฟล์)
# เพราะ detect_and_fill_unused_capacity ก็ต้องใช้ และมันถูกเรียกก่อนบรรทัดนี้


# แถว OLD สร้างจาก DETAIL_SC (ทีละใบสั่ง) ถ้ามี ไม่งั้นใช้ DETAIL เดิม (ยุบใบ)
# คอลัมน์เครื่อง/setup/วันทำงานคำนวณหลังยุบใบ จึงมีเฉพาะใน DETAIL → ยกมาด้วยคีย์บล็อก
# ค่าเดียวซ้ำทุกใบในบล็อกนั้น (กติกา max ไม่บวก — ดู PlanGantt.planMcAgg / plan_view.setup_jobs)
_OLD_BLOCK_KEY = ["MC_GROUP", "GUAGE", "ITEM_CODE", "WEEK"]
_old_src = detail_mc
if not detail_mc_sc.empty and all(c in detail_mc_sc.columns for c in _OLD_BLOCK_KEY):
    _blk_add = [
        c for c in detail_mc.columns
        if c not in detail_mc_sc.columns and c not in _OLD_BLOCK_KEY
    ]
    _blk = detail_mc[_OLD_BLOCK_KEY + _blk_add].copy()
    _sc = detail_mc_sc.copy()
    for _d in (_blk, _sc):
        _d["WEEK"] = pd.to_numeric(_d["WEEK"], errors="coerce")
        _d["GUAGE"] = _d["GUAGE"].astype(str).str.strip()
        for _c in ("MC_GROUP", "ITEM_CODE"):
            _d[_c] = _d[_c].astype(str).str.strip().str.upper()
    _old_src = _sc.merge(_blk.drop_duplicates(_OLD_BLOCK_KEY), on=_OLD_BLOCK_KEY, how="left")
    # เครื่องของบล็อกต้องคิดจากยอด "ทั้งบล็อก" ไม่ใช่ยอดของใบเดียว (ดู _recalc_old_mc)
    _old_src["_BLOCK_QTY"] = (
        pd.to_numeric(_old_src["KP_WEIGHT"], errors="coerce").fillna(0)
        .groupby([_old_src[c] for c in _OLD_BLOCK_KEY]).transform("sum")
    )
    print(f"🧾 แถว OLD แตกตามใบสั่ง: {len(_old_src)} แถว (จาก {len(detail_mc)} บล็อก)")

old_booking_df = pd.DataFrame()
if not _old_src.empty:
    # โหลดเฉพาะ item ที่มีออเดอร์ใหม่ (วางแผนอยู่) เท่านั้น
    if "ITEM_CODE" in _old_src.columns:
        old_booking_df = _old_src[_old_src["ITEM_CODE"].astype(str).str.strip().str.upper().isin(_plan_items)].copy()
    else:
        old_booking_df = _old_src.copy()
    # Rename detail_mc columns → ชื่อเดียวกับ new plan
    # แต่ยังไม่ rename FIBER_TYPE เพื่อใช้ตรวจสอบ setup days
    old_booking_df = old_booking_df.rename(
        columns={
            "GUAGE": "MC_GUAGE",
            "WEEK": "PLAN_WEEK",
            "KP_WEIGHT": "PRODUCE_QTY",
            "MC_USE_CEIL": "REQUIRED_MC",
            "MC_USE": "ACTUAL_MC",
            "CAP ทอ": "DAILY_CAPACITY",
            "REVOLUTION/WEIGHT": "REVOLUTION_WEIGHT",
            "SO_NO": "SC_SO_NO",
        }
    )
    # เติม MATERIAL_CONTENT จาก FIBER_TYPE ของ booking ก่อน (เพื่อให้ setup days ถูกต้อง)
    # ถ้าไม่มี FIBER_TYPE ค่อย lookup จาก orders
    _mat_item_lookup = {}
    if "MATERIAL_CONTENT" in orders.columns:
        for _, _or in orders.iterrows():
            _ic = str(_or.get("Item Code", "")).strip().upper()
            _mat = str(_or.get("MATERIAL_CONTENT", "")).strip()
            if _ic and _mat and _mat.upper() not in ("NAN", ""):
                _mat_item_lookup[_ic] = _mat
    if "ITEM_CODE" in old_booking_df.columns:
        def _get_mat_from_booking(row):
            _ic = str(row.get("ITEM_CODE", "")).strip().upper()
            # 1. ใช้ MATERIAL_CONTENT จาก booking โดยตรง (detail_mc มีคอลัมน์นี้)
            _mat = str(row.get("MATERIAL_CONTENT", "")).strip()
            if _mat and _mat.upper() not in ("NAN", ""):
                return _mat
            # 2. ใช้ FIBER_TYPE จาก booking (legacy)
            _fiber = str(row.get("FIBER_TYPE", "")).strip()
            if _fiber and _fiber.upper() not in ("NAN", ""):
                return _fiber
            # 3. fallback: lookup จาก orders
            _v = _mat_item_lookup.get(_ic)
            if _v:
                return _v
            return ""
        old_booking_df["MATERIAL_CONTENT"] = old_booking_df.apply(_get_mat_from_booking, axis=1)
    # แปลง DAILY_CAPACITY จากฐาน 24 ชั่วโมง → ตาม Item Special working hour (หรือ 20 ชั่วโมง default)
    if "DAILY_CAPACITY" in old_booking_df.columns:
        def _adj_old_cap(row):
            cap = pd.to_numeric(row["DAILY_CAPACITY"], errors="coerce")
            if pd.isna(cap):
                return cap
            _is = get_item_special(
                str(row.get("ITEM_CODE", "")).strip().upper(),
                str(row.get("MC_GROUP", "")).strip().upper(),
                str(row.get("MC_GUAGE", "")).strip(),
            )
            if _is is not None:
                return cap * (_is[1] / 24)  # Item Special working_hour override (ชนะเสมอ)
            # ไม่มีใน Item Special → ชั่วโมงของกลุ่มจากแผง Work Day (ไม่ใช้ MasterMC)
            return cap * (_base_work_hours(row.get("MC_GROUP"), row.get("MC_GUAGE")) / 24)
        old_booking_df["DAILY_CAPACITY"] = old_booking_df.apply(_adj_old_cap, axis=1)
    # ตัด S นำหน้า SC_SO_NO (เช่น "S717455" → "717455") + ล้าง ".0"/"nan" ที่ติดมากับ
    # คอลัมน์ตัวเลขของ booking (norm_sc = มาตรฐานกลาง เลขล้วนไม่มี S)
    if "SC_SO_NO" in old_booking_df.columns:
        old_booking_df["SC_SO_NO"] = old_booking_df["SC_SO_NO"].map(ColorOpen.norm_sc)
    old_booking_df.insert(0, "PLAN_SOURCE", "OLD")
    # แปลง PLAN_WEEK เป็นตัวเลขถ้ามี
    if "PLAN_WEEK" in old_booking_df.columns:
        old_booking_df["PLAN_WEEK"] = pd.to_numeric(
            old_booking_df["PLAN_WEEK"], errors="coerce"
        )
    # 🔧 FIX: Recalculate ACTUAL_MC / REQUIRED_MC สำหรับ OLD rows ตาม calendar จริง
    # (booking เดิมคำนวณด้วย factory days 6 วัน แต่บาง week เช่น W19 มี 5 วันทำงานจริง)
    if all(
        c in old_booking_df.columns
        for c in [
            "PRODUCE_QTY",
            "DAILY_CAPACITY",
            "PLAN_WEEK",
            "MC_GROUP",
            "ACTUAL_MC",
            "REQUIRED_MC",
        ]
    ):
        # 🔧 FIX: หา week แรกของแต่ละ (item, mc_group, gauge) เพื่อคำนวณ setup เฉพาะ week แรก
        _first_week_map = {}
        if "ITEM_CODE" in old_booking_df.columns and "MC_GUAGE" in old_booking_df.columns:
            for _, grp in old_booking_df.groupby(["ITEM_CODE", "MC_GROUP", "MC_GUAGE"]):
                weeks = sorted(grp["PLAN_WEEK"].dropna().unique())
                if weeks:
                    _first_week_map[(grp.iloc[0]["ITEM_CODE"], grp.iloc[0]["MC_GROUP"], grp.iloc[0]["MC_GUAGE"])] = min(weeks)

        def _recalc_old_mc(row):
            try:
                # ยอดที่ใช้คิดเครื่อง = ยอดของทั้งบล็อก (ทุกใบในสัปดาห์นั้นใช้เครื่องชุดเดียวกัน)
                # ไม่ใช่ยอดของใบเดียว ไม่งั้นบล็อกที่แตกเป็นหลายใบจะได้เครื่องต่ำกว่าจริง
                qty = float(row.get("_BLOCK_QTY", 0) or 0) or float(row.get("PRODUCE_QTY", 0) or 0)
                cap = float(row.get("DAILY_CAPACITY", 0) or 0)
                week_val = row.get("PLAN_WEEK")
                mc_group = str(row.get("MC_GROUP", "")).strip()
                item_code = str(row.get("ITEM_CODE", "")).strip().upper()
                item_material = str(row.get("MATERIAL_CONTENT", "")).strip()
                item_gauge = str(row.get("MC_GUAGE", "")).strip()
                if qty <= 0 or cap <= 0 or pd.isna(week_val):
                    return row.get("ACTUAL_MC", 0), row.get("REQUIRED_MC", 0)
                week = int(week_val)
                # วันทำงานจากชีท Work Day เป็นค่าสุดท้าย (ไม่ cap/หักวันหยุด)
                actual_wd = get_working_days_by_factory(mc_group, 1, week=week, item_code=item_code, gauge=item_gauge)
                if actual_wd <= 0:
                    return row.get("ACTUAL_MC", 0), row.get("REQUIRED_MC", 0)
                # 🔧 FIX: คำนวณ setup days เฉพาะ week แรกของ item เท่านั้น
                # Week ถัดไป = carryover ไม่ต้อง setup (ใช้ capacity เต็ม)
                _key = (item_code, mc_group, item_gauge)
                _first_week = _first_week_map.get(_key)
                is_first_week = (_first_week is not None and week == _first_week)
                if is_first_week:
                    _row_yarn = str(row.get("YARN-USED", "") or row.get("YARN_USED", "") or _yarn_used_lookup.get(item_code, "")).strip()
                    _row_mat = item_material or _material_content_lookup.get(item_code, "")
                    setup_days = get_setup_days_for_item(_row_mat, _row_yarn, mc_group=mc_group, item_code=item_code)
                    productive_wd = max(0, actual_wd - setup_days)
                else:
                    # Week ถัดไป = carryover ไม่ต้อง setup
                    productive_wd = actual_wd
                if productive_wd <= 0:
                    # ถ้า setup days มากกว่า working days → ไม่สามารถผลิตได้
                    return row.get("ACTUAL_MC", 0), row.get("REQUIRED_MC", 0)
                needed = qty / (productive_wd * cap)
                mc = max(
                    1, int(needed) + (1 if (needed - int(needed)) > 1e-9 else 0)
                )
                return mc, mc
            except Exception:
                return row.get("ACTUAL_MC", 0), row.get("REQUIRED_MC", 0)
        old_booking_df[["ACTUAL_MC", "REQUIRED_MC"]] = old_booking_df.apply(
            lambda r: pd.Series(_recalc_old_mc(r)), axis=1
        )

    # เก็บแถว OLD ทั้งหมด (ไม่จำกัดสัปดาห์)
    old_booking_df = old_booking_df.sort_values(
        ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK"], na_position="last"
    ).reset_index(drop=True)
    # เพิ่ม PLAN_YEAR ให้ old booking rows
    if "YEAR" in old_booking_df.columns:
        old_booking_df["PLAN_YEAR"] = pd.to_numeric(old_booking_df["YEAR"], errors="coerce").fillna(TODAY.year).astype(int)
    elif "PLAN_WEEK" in old_booking_df.columns:
        old_booking_df["PLAN_YEAR"] = old_booking_df["PLAN_WEEK"].map(_week_year_lookup).fillna(TODAY.year).astype(int)
    print(
        f"📦 OLD rows จาก booking_final_ready25 (ทั้งหมด): {len(old_booking_df)} rows"
    )
else:
    print("⚠️ ไม่พบข้อมูลใน detail_mc → ข้าม OLD")
# รวม OLD + NEW โดยใช้ common columns เรียงตาม ITEM_CODE, MC_GROUP, PLAN_WEEK
# เพื่อให้เห็น OLD vs NEW week-by-week ของ item เดียวกันติดกัน
if not old_booking_df.empty:
    # booking เก็บกำหนดส่งเป็น (FG_YEAR, FG_WEEK) แยกคอลัมน์ ส่วนชีทแผนใช้ YYYYWW ค่าเดียว
    # → รวมก่อน ไม่งั้นแถว OLD จะได้เลขสัปดาห์เปล่า (39) ปนกับ YYYYWW (202539) ของแถว NEW
    if {"FG_YEAR", "FG_WEEK"} <= set(old_booking_df.columns):
        _ofy = pd.to_numeric(old_booking_df["FG_YEAR"], errors="coerce")
        _ofw = pd.to_numeric(old_booking_df["FG_WEEK"], errors="coerce")
        old_booking_df["FG_WEEK"] = (_ofy * 100 + _ofw).where(_ofy.notna() & _ofw.notna())
    common_cols = ["PLAN_SOURCE"] + [
        c for c in new_df.columns if c in old_booking_df.columns and c != "PLAN_SOURCE"
    ]
    # บังคับให้ ITEM_CODE, MC_GROUP, PLAN_WEEK อยู่ใน common_cols เสมอ (ถ้ามีใน old)
    for _must in ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK"]:
        if _must not in common_cols and _must in old_booking_df.columns:
            common_cols.append(_must)
    if "TARGET_KNIT" not in common_cols and "TARGET_KNIT" in new_df.columns:
        common_cols.append("TARGET_KNIT")
    # ปีของเดดไลน์ — คู่กับ TARGET_KNIT เสมอ (คำนวณพร้อมกันด้านล่างใน _lookup_target_knit_old)
    if "TARGET_YEAR" not in common_cols and "TARGET_YEAR" in new_df.columns:
        common_cols.append("TARGET_YEAR")
    # วันด้ายเข้าครบของแถว OLD — detail_mc มี PO_IN_DATE เป็นวันหลายล็อตคั่นคอมมาอยู่แล้ว
    # ยุบเหลือ "วันหลังสุด" (ล็อตที่ไกลสุด = วันที่ด้ายเข้าครบจริง) ให้รูปแบบเดียวกับแถว NEW
    # ถ้าไม่ทำ คอลัมน์นี้จะยังเป็นสตริงยาวหลายวัน หน้าเว็บอ่านเป็นสัปดาห์ไม่ได้
    if "PO_IN_DATE" in old_booking_df.columns:
        _old_poin = old_booking_df["PO_IN_DATE"].map(_po_in_date_last)
        old_booking_df["PO_IN_DATE"] = [
            f"{_d:%d/%m/%Y}" if pd.notna(_d) else "" for _d in _old_poin
        ]
        old_booking_df["PO_IN_WEEK"] = [get_po_in_week(_d) for _d in _old_poin]
    # PO_IN_WEEK เพิ่งสร้างหลัง common_cols (และไฟล์ booking เก่าอาจไม่มี PO_IN_DATE เลย)
    # → ต่อท้ายเอง ไม่งั้นคอลัมน์ของแถว NEW หลุดหายตอน concat (common_cols = ส่วนที่ซ้อนกัน)
    for _poc in ["PO_IN_DATE", "PO_IN_WEEK"]:
        if _poc not in common_cols and _poc in new_df.columns:
            common_cols.append(_poc)
    for _cyl_col in ["CYLINDER_CHANGE"]:
        if _cyl_col not in common_cols and _cyl_col in new_df.columns:
            common_cols.append(_cyl_col)

    # ── ข้อมูลออร์เดอร์ของแถว OLD ────────────────────────────────────────────────
    # booking (DETAIL_SC) มีกำหนดส่ง / ประเภท / ลูกค้า / PO ของใบนั้นมาด้วยอยู่แล้ว → ใช้ค่านั้นก่อน
    # (ใบที่ปิดหรือถูกกรองออกจาก order_ready ก็ยังมีข้อมูลครบ) ส่วนยอดทั้งใบ ORDERS_QTY /
    # PENDING_PLAN และ SC_LINE_ID มีเฉพาะในไฟล์ออร์เดอร์ → lookup มาเติม
    #
    # ⚠️ คีย์ที่เคยพลาด: SC_SO_NO ของ booking เป็นเลข SO ล้วน ("708936") ส่วน orders["SC/SO NO"]
    # แมปมาจากคอลัมน์ SC_NO ซึ่งมี S นำหน้า ("S708936") — โค้ดเดิม lstrip("Ss") ฝั่ง booking แล้ว
    # เทียบตรง ๆ จึงไม่ match สักแถว ทำให้ FG_WEEK / TARGET_KNIT / ORDERS_QTY / ORDER_TYPE /
    # CUSTOMER ของแถว OLD ว่างทั้งไฟล์ → normalize ทั้งสองฝั่ง แล้วทำ index ครั้งเดียว
    # (เดิม filter ทั้งตาราง 8 รอบต่อแถว OLD)
    def _so_key(v) -> str:
        t = str(v or "").strip().upper()
        if t.startswith("S"):
            t = t[1:]
        return t.split(".")[0] if re.fullmatch(r"\d+\.0+", t) else t

    def _so_keys(v) -> list:
        """ไฟล์ booking เก่ายุบหลายใบไว้ในเซลล์เดียว ("708936,719941") → แตกก่อนเทียบ"""
        return [k for k in (_so_key(p) for p in str(v or "").replace("|", ",").split(",")) if k]

    _ord_by_so: dict = {}
    for _oc in ("SO_NO", "SC/SO NO"):
        if _oc not in orders.columns:
            continue
        for _oi, _ov in orders[_oc].items():
            _ok = _so_key(_ov)
            if _ok:
                _ord_by_so.setdefault(_ok, _oi)

    def _ord_rows_of(row) -> list:
        """แถว orders ของทุกใบในแถว booking นี้ (ว่าง = ใบปิดแล้ว/ถูกกรองออกก่อนวางแผน)"""
        _idx = dict.fromkeys(
            _ord_by_so[k] for k in _so_keys(row.get("SC_SO_NO")) if k in _ord_by_so
        )
        return [orders.loc[i] for i in _idx]

    # ยอดของใบนั้นตาม booking — ใช้เมื่อหาใบใน order_ready ไม่เจอ (ผลรวมทุกสัปดาห์ของ SO+item)
    _bk_order_qty = (
        pd.to_numeric(old_booking_df["PRODUCE_QTY"], errors="coerce").fillna(0)
        .groupby([
            old_booking_df["SC_SO_NO"].astype(str).map(lambda v: ",".join(_so_keys(v))),
            old_booking_df["ITEM_CODE"].astype(str).str.strip().str.upper(),
        ]).transform("sum")
    )

    _old_vals = {k: [] for k in (
        "TARGET_KNIT", "TARGET_YEAR", "CUSTOMER", "ORDER_TYPE", "FG_WEEK", "ORDERS_QTY",
        "PENDING_PLAN", "ORDER_DATE", "PO_NO", "RDD_WEEK", "SC_LINE_ID",
    )}
    _old_no_match = 0
    for _pos, (_, _brow) in enumerate(old_booking_df.iterrows()):
        _ords = _ord_rows_of(_brow)
        if not _ords:
            _old_no_match += 1
        _first = _ords[0] if _ords else None

        # ประเภทออร์เดอร์: FR_CODE ของ booking ('S{SC}-{ORDER_TYPE}') มาก่อน แล้วค่อยใช้ของ order
        _fr = str(_brow.get("FR_CODE", "") or "")
        _otype = ""
        if "-" in _fr:
            _otype = _fr.split("|")[0].split("-", 1)[1].strip()
        if not _otype and _first is not None:
            _otype = str(_first.get("Orders Type", "") or "").strip()

        # กำหนดส่ง: ของ booking เป็นของใบนั้นจริง (order_ready อาจไม่มีใบที่ปิดแล้ว)
        _fg = _brow.get("FG_WEEK")
        if (_fg is None or pd.isna(_fg)) and _first is not None:
            _fg = _first.get("FG Week")
        _fg = None if (_fg is None or pd.isna(_fg)) else _fg
        # RDD ของ booking ตรง ๆ — ว่างก็ว่าง ห้ามเอา FG มาใส่แทน (user 22/9/2569)
        _rdd = _brow.get("RDD_WEEK")
        if _rdd is None or pd.isna(_rdd):
            _rdd = None

        _cust = str(_brow.get("KNIT_CUS_NAME", "") or "").strip()
        if not _cust and _first is not None:
            _cust = str(_first.get("Customer", "") or "").strip()
        _po = str(_brow.get("PO_NO", "") or "").strip()
        if (not _po or _po.lower() == "nan") and _first is not None:
            _po = str(_first.get("PO_NO", "") or "").strip()

        # ยอดทั้งใบ: รวมทุกใบที่แถวนี้ครอบ (ไฟล์เก่าที่ยังยุบใบอยู่จะได้ยอดรวมถูกต้อง)
        # ⚠️ ใช้ยอดก่อนแตกก้อนสี (_SPLIT_PEND_TOTAL) เพราะ _ord_by_so เก็บ index เดียวต่อใบ
        # ถ้าอ่าน Pending Plan ตรง ๆ จะได้ยอดของก้อนเดียว (ก้อนสี) ไม่ใช่ของทั้งใบ
        _oq = sum(float(pd.to_numeric(r.get("Orders.Qty"), errors="coerce") or 0) for r in _ords)
        _pp = sum(
            float(pd.to_numeric(
                r.get("_SPLIT_PEND_TOTAL") if pd.notna(r.get("_SPLIT_PEND_TOTAL")) else r.get("Pending Plan"),
                errors="coerce",
            ) or 0)
            for r in _ords
        )
        if not _ords:
            _oq = _pp = float(_bk_order_qty.iloc[_pos])

        _odate = _first.get("Date") if _first is not None else None
        if _odate is None or pd.isna(_odate):
            _odate = _po_in_date_last(_brow.get("PO_IN_DATE"))

        _tk, _ty = _target_knit_of(_fg, _otype, _first if _first is not None else _brow)

        _old_vals["TARGET_KNIT"].append(_tk)
        _old_vals["TARGET_YEAR"].append(_ty)
        _old_vals["CUSTOMER"].append(_cust)
        _old_vals["ORDER_TYPE"].append(_otype)
        _old_vals["FG_WEEK"].append(_fg)
        _old_vals["ORDERS_QTY"].append(_oq or None)
        _old_vals["PENDING_PLAN"].append(_pp or None)
        _old_vals["ORDER_DATE"].append(_odate if _odate is not None and not pd.isna(_odate) else None)
        _old_vals["PO_NO"].append(_po)
        _old_vals["RDD_WEEK"].append(_rdd)
        # เลขบรรทัดใบ ไม่ใช่เลข SC (เดิมใส่ "SC/SO NO" ผิดช่อง) ; แถวที่ครอบหลายใบ → รวมทุกบรรทัด
        _old_vals["SC_LINE_ID"].append(",".join(dict.fromkeys(
            _ln for _ln in (ColorOpen.so_text(r.get("SC_LINE_ID")) for r in _ords) if _ln
        )))

    old_booking_df["TARGET_KNIT"] = _old_vals["TARGET_KNIT"]
    old_booking_df["TARGET_YEAR"] = pd.array(_old_vals["TARGET_YEAR"], dtype="Int64")
    if "IS_CORE_ITEM" not in common_cols and "IS_CORE_ITEM" in new_df.columns:
        common_cols.append("IS_CORE_ITEM")
    old_booking_df["IS_CORE_ITEM"] = ""
    for _oc_name in ("CUSTOMER", "ORDER_TYPE", "FG_WEEK", "ORDERS_QTY", "PENDING_PLAN",
                     "ORDER_DATE", "PO_NO", "RDD_WEEK", "SC_LINE_ID"):
        if _oc_name not in common_cols and _oc_name in new_df.columns:
            common_cols.append(_oc_name)
        old_booking_df[_oc_name] = _old_vals[_oc_name]
    if _old_no_match:
        print(f"ℹ️ แถว OLD ที่หาใบสั่งใน order_ready ไม่เจอ: {_old_no_match}/{len(old_booking_df)} "
              f"(ใบที่ปิดแล้ว/ถูกกรองออก — ใช้กำหนดส่งกับยอดจาก booking แทน)")
    combined_df = pd.concat(
        [
            old_booking_df[[c for c in common_cols if c in old_booking_df.columns]],
            new_df[[c for c in common_cols if c in new_df.columns]],
        ],
        ignore_index=True,
    )
    _sort_cols = [
        c
        for c in ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK", "PLAN_SOURCE"]
        if c in combined_df.columns
    ]
    combined_df = combined_df.sort_values(_sort_cols, ignore_index=True)
else:
    combined_df = new_df
# =========================
# คอลัมน์ TARGET_STATUS: ทัน / ไม่ทัน ตาม TARGET_KNIT
# ดูว่า **ผลิตจนครบ order qty** ทันหรือไม่ (ใช้ PLAN_WEEK สุดท้ายของ order)
# ไม่ใช่ดูแค่แต่ละ row — ต้องดูว่า row สุดท้ายของ order ทันตาม target ถึงจะ "ทัน"
# =========================
_grp_cols = ["SC_SO_NO", "ITEM_CODE", "FG_WEEK"]
_grp_cols_available = [c for c in _grp_cols if c in combined_df.columns]

# (ปี, สัปดาห์) → row index บน timeline — ใช้แทนการเดาปีจากเลขสัปดาห์เปล่า
_YW_IDX = {}
for _i, _crow in calendar_week.iterrows():
    try:
        _YW_IDX.setdefault((int(_crow["YEAR"]), int(_crow["WEEK_NUM"])), int(_i))
    except (TypeError, ValueError):
        continue  # แถวปฏิทินที่ปี/สัปดาห์ว่าง — ข้ามไป ไม่ให้ล้มทั้งไฟล์


def _timeline_idx(week, year=None):
    """ตำแหน่งบน timeline ของ (ปี, สัปดาห์)

    มีปี → ชี้ occurrence ที่ถูกต้องแน่นอน (สำคัญเมื่อแผนคร่อมปี: W2 ของปีหน้าต้องอยู่
    "หลัง" W50 ของปีนี้ ไม่ใช่ก่อน) ; ไม่มีปี → ตกไปใช้ week_pos ที่เดาจากกรอบแผนเหมือนเดิม"""
    try:
        _w = int(week)
    except (TypeError, ValueError):
        return None
    if _w >= 100000:  # YW อยู่แล้ว
        return _IDX_BY_YW.get(_w)
    try:
        _y = int(year) if year is not None and not pd.isna(year) else None
    except (TypeError, ValueError):
        _y = None
    if _y is not None and (_y, _w) in _YW_IDX:
        return _YW_IDX[(_y, _w)]
    return week_pos(_w)


def _last_finish_of_group(_grp):
    """(PLAN_WEEK, index บน timeline) ของสัปดาห์ผลิต "สุดท้าย" ในกลุ่ม

    เดิมใช้ PLAN_WEEK.max() ซึ่งเป็นเลขสัปดาห์เปล่า → แผนคร่อมปีจะเลือกผิด
    (W50 ปีนี้ ถูกมองว่ามาหลัง W2 ปีหน้า) จึงต้องจัดอันดับด้วย index บน timeline"""
    _best = None
    _has_year = "PLAN_YEAR" in _grp.columns
    for _, _r in _grp.iterrows():
        _w = pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce")
        if pd.isna(_w):
            continue
        _idx = _timeline_idx(int(_w), _r.get("PLAN_YEAR") if _has_year else None)
        if _idx is None:
            continue
        if _best is None or _idx > _best[1]:
            _best = (int(_w), int(_idx))
    return _best


# หา PLAN_WEEK สุดท้าย (ผลิตเสร็จ) ของแต่ละ order group
# ใช้ NEW rows เป็นหลัก (plan ใหม่) — ถ้าไม่มี NEW ถึงใช้ OLD
_last_pw_map = {}  # {normalized_str_key: (last_plan_week, last_plan_week_index)}
if _grp_cols_available:
    # Step 1: คำนวณจาก NEW rows ก่อน (source of truth)
    if "PLAN_SOURCE" in combined_df.columns:
        _new_only = combined_df[combined_df["PLAN_SOURCE"].astype(str).str.upper() == "NEW"]
    else:
        _new_only = combined_df
    if not _new_only.empty:
        for _key, _grp in _new_only.groupby(_grp_cols_available):
            try:
                _finish = _last_finish_of_group(_grp)
                if _finish is not None:
                    _norm_key = tuple(str(v).strip() for v in (_key if isinstance(_key, tuple) else (_key,)))
                    _last_pw_map[_norm_key] = _finish
            except Exception:
                continue
    # Step 2: เติม order ที่ไม่มี NEW rows → ใช้ OLD rows
    if "PLAN_SOURCE" in combined_df.columns:
        _old_only = combined_df[combined_df["PLAN_SOURCE"].astype(str).str.upper() != "NEW"]
        if not _old_only.empty:
            for _key, _grp in _old_only.groupby(_grp_cols_available):
                _norm_key = tuple(str(v).strip() for v in (_key if isinstance(_key, tuple) else (_key,)))
                if _norm_key in _last_pw_map:
                    continue  # มี NEW แล้ว ไม่ต้อง overwrite
                try:
                    _finish = _last_finish_of_group(_grp)
                    if _finish is not None:
                        _last_pw_map[_norm_key] = _finish
                except Exception:
                    continue

def _target_status(row) -> str:
    # CORE ITEM ไม่ต้องบอกทัน/ไม่ทัน
    if str(row.get("IS_CORE_ITEM", "")).strip().upper() == "CORE ITEM":
        return ""

    _tk = row.get("TARGET_KNIT")
    _pw = row.get("PLAN_WEEK")
    if pd.isna(_pw) or pd.isna(_tk):
        return "-"

    try:
        # ใช้ TARGET_YEAR คู่กับ TARGET_KNIT — เดดไลน์กับสัปดาห์ผลิตอาจอยู่คนละปี
        _tk_idx = _timeline_idx(int(_tk), row.get("TARGET_YEAR"))
        if _tk_idx is None:
            return "-"

        # สร้าง key (str) เพื่อ lookup PLAN_WEEK สุดท้ายของ order นี้
        _key_vals = tuple(str(row.get(c, "")).strip() for c in _grp_cols_available)
        _finish = _last_pw_map.get(_key_vals)
        if _finish is not None:
            _last_pw_val, _last_pw_idx = _finish
            if _last_pw_idx <= _tk_idx:
                return "ทัน"
            else:
                return f"ไม่ทัน (+{_last_pw_idx - _tk_idx} wk)"

        # fallback: ถ้าไม่มี group → เทียบ row เดียว
        _pw_idx = _timeline_idx(int(_pw), row.get("PLAN_YEAR"))
        if _pw_idx is None:
            return "-"
        if _pw_idx <= _tk_idx:
            return "ทัน"
        else:
            return f"ไม่ทัน (+{_pw_idx - _tk_idx} wk)"
    except Exception:
        return "-"

combined_df["TARGET_STATUS"] = combined_df.apply(_target_status, axis=1)
# กันซ้ำ: แผนเก่าที่ถูก gen ด้วยเวอร์ชันที่ยังไม่ทิ้ง PLAN_IDX/TARGET_IDX อาจพา column กลับเข้ามาทาง old plan
combined_df = combined_df.drop(columns=["PLAN_IDX", "TARGET_IDX", "FLOOR_IDX"], errors="ignore")
# Reorder columns: move PLAN_YEAR to column AL (index 37) and COLOR_DESC after it
print(f"DEBUG: Total columns before reordering: {len(combined_df.columns)}")
print(f"DEBUG: Columns before reordering: {list(combined_df.columns)}")
if "PLAN_YEAR" in combined_df.columns:
    cols = list(combined_df.columns)
    plan_year_idx = cols.index("PLAN_YEAR")
    print(f"DEBUG: PLAN_YEAR is currently at index {plan_year_idx}")
    # Remove PLAN_YEAR from current position
    cols.remove("PLAN_YEAR")
    print(f"DEBUG: After removing PLAN_YEAR, total columns: {len(cols)}")
    # Insert PLAN_YEAR at index 37 (column AL)
    # If we have fewer than 38 columns, add padding columns first
    if len(cols) < 38:
        print(f"DEBUG: Only {len(cols)} columns, adding padding to reach 38")
        for i in range(38 - len(cols)):
            cols.append(f"_PAD_{i}")
    target_idx = 37
    cols.insert(target_idx, "PLAN_YEAR")
    # Move COLOR_DESC after PLAN_YEAR if it exists
    if "COLOR_DESC" in cols:
        cols.remove("COLOR_DESC")
        cols.insert(target_idx + 1, "COLOR_DESC")

    # Remove padding columns
    cols = [c for c in cols if not c.startswith("_PAD_")]
    combined_df = combined_df[cols]
    print(f"DEBUG: Columns after reordering: {list(combined_df.columns)}")
    print(f"DEBUG: PLAN_YEAR is now at index {cols.index('PLAN_YEAR')} (column {chr(65 + cols.index('PLAN_YEAR'))})")
else:
    print(f"DEBUG: PLAN_YEAR not found in columns")
# =========================
# Write COMBINED_FILE
# =========================
# ❌ DISABLED: auto-backup ผลรอบก่อน → committed_plan.xlsx (feedback loop)
# เดิมโค้ดตรงนี้ก็อป COMBINED_FILE (= ผลลัพธ์รอบก่อน) มาเป็น committed_plan.xlsx
# แล้วรอบถัดไปจะอ่าน committed_plan.xlsx กลับเข้ามาเป็น old-plan carryover
# → เกิด feedback loop: แผนรอบใหม่ขึ้นกับแผนรอบก่อน ทำให้ผลลัพธ์ "ไม่นิ่ง"
#   (booking/order เดิมเป๊ะ ๆ แต่รันคนละครั้งได้คนละสัปดาห์ เช่น W34 บ้าง W36 บ้าง)
# ตัดออกเพื่อให้ทุกครั้งคำนวณสดจาก booking + order → deterministic
# ⚠️ ถ้าจะทำฟีเจอร์ "commit แผน" จริง ให้เขียน committed_plan.xlsx เฉพาะตอน user กด commit เท่านั้น
# ไม่ใช่ auto ทุกครั้งที่รัน Planning
for _out_df_norm in (combined_df, _no_cap_df, _multi_cap_df):
    for _c_norm in ("SC_SO_NO", "SC/SO NO", "SO_NO"):
        if isinstance(_out_df_norm, pd.DataFrame) and _c_norm in _out_df_norm.columns:
            _out_df_norm[_c_norm] = _out_df_norm[_c_norm].map(ColorOpen.so_text)

with pd.ExcelWriter(COMBINED_FILE, engine="openpyxl") as writer:
    # เขียนสำเนาที่แยก YW แล้ว — combined_df ตัวจริงไม่ได้ถูกใช้ต่อ แต่กันไว้ไม่ให้แก้ของที่อาจอ้างถึง
    _kp_no_last(_split_yw_out(combined_df.copy(), _PLAN_YW_COLS)).to_excel(
        writer, sheet_name="PLAN", index=False)
    if not _no_cap_df.empty:
        _no_cap_df.to_excel(writer, sheet_name="NO_CAP", index=False)
    if not _multi_cap_df.empty:
        _multi_cap_df.to_excel(writer, sheet_name="MULTI_CAP", index=False)
print(f"Combined (OLD+NEW): {COMBINED_FILE}")
print(f"  OLD rows: {len(old_booking_df) if not old_booking_df.empty else 0}")
print(f"  NEW rows: {len(plan_df)}")
if not _no_cap_df.empty:
    print(f"  NO_CAP items: {_no_cap_df['Item Code'].nunique()} items, {len(_no_cap_df)} orders → sheet 'NO_CAP'")
if not _multi_cap_df.empty:
        print(f"  MULTI_CAP items: {_multi_cap_df['Item Code'].nunique()} items, {len(_multi_cap_df)} orders → sheet 'MULTI_CAP' (SINGLE MC_TYPE หรือ OM/OMNOI)")



# =========================
# SETUP_TRACKING sheet — แสดงเฉพาะ ITEM ที่หัก Job พร้อม Week, จำนวน Job และจำนวน MC ที่ Setup
# เกณฑ์: หัก Job เมื่อ NEW_MC > 0 (เครื่องใหม่ที่ต้อง setup = 1 job ต่อ 1 เครื่อง)
# =========================
# --- NEW PLAN: หัก Job เมื่อ NEW_MC > 0 (แยกเป็นฟังก์ชันเพื่อสร้างทั้ง with-S9 และ no-S9) ---
def _new_setup_rows(plan_frame):
    """สร้าง setup rows ฝั่ง NEW จากแผนที่ระบุ (NEW_MC > 0)

    ⚠️ แถวของ item เดียวกัน เครื่อง+เกจเดียวกัน สัปดาห์เดียวกัน (คนละ SC / FG split) ใช้
    "เครื่องชุดเดียวกัน" — Planning เขียน NEW_MC/CARRYOVER_MC ค่าเดียวกันซ้ำทุกแถว
    → ต้องยุบเป็นแถวเดียว (เครื่อง=max, ยอด=sum, SC รวมกัน) ไม่งั้น JOBS_DEDUCTED ถูกนับ
    เป็นเท่าตัว แล้วแถบโหลด job/week บนหน้าเว็บโชว์เกินโควตาทั้งที่ตั้งเครื่องจริงไม่เกิน
    """
    _by_key: dict = {}
    if not plan_frame.empty and "NEW_MC" in plan_frame.columns:
        _new_setup = plan_frame[pd.to_numeric(plan_frame["NEW_MC"], errors="coerce").fillna(0) > 0].copy()
        for _, row in _new_setup.iterrows():
            _new_mc_val = int(pd.to_numeric(row["NEW_MC"], errors="coerce") or 0)
            _carryover_val = int(pd.to_numeric(row.get("CARRYOVER_MC", 0), errors="coerce") or 0)
            _mc_grp_n = str(row["MC_GROUP"]).strip().upper()
            # งานจ้างทอตั้งเครื่องที่ผู้รับจ้าง ไม่กินโควตา setup ของโรงเรา — ต้องแยกประเภทออก
            # ไม่งั้น _get_type_key_for_mc() หาชื่อผู้รับจ้าง (วันนิตติ้ง/รุ่งสยาม/…) ใน master_mc
            # ไม่เจอ แล้วคืน default "PHET_DOUBLE" → แถบ Set up Job/Week ของโรงเพชรโชว์เกินโควตา
            # ทั้งที่ในบ้านไม่ได้ตั้งเครื่องเกิน (เคสจริง 20 ก.ย. 2569 W42: 44 job ในนั้นจ้างทอ 15)
            _type_key_new = (
                "OUTSOURCE" if _is_outsource_row(row) else _get_type_key_for_mc(_mc_grp_n)
            )
            _wk_n = pd.to_numeric(row["PLAN_WEEK"], errors="coerce")
            _key_n = (
                int(_wk_n) if pd.notna(_wk_n) else row["PLAN_WEEK"],
                str(row["ITEM_CODE"]).strip().upper(),
                _mc_grp_n,
                _normalize_gauge(row.get("MC_GUAGE", "")),
            )
            _qty_n = pd.to_numeric(row.get("PRODUCE_QTY", 0), errors="coerce") or 0
            _exist = _by_key.get(_key_n)
            if _exist is not None:
                # เครื่องชุดเดียวกัน → เก็บค่าสูงสุด ; ยอดผลิตเป็นของแต่ละ SC จริง → บวกสะสม
                _exist["JOBS_DEDUCTED"] = max(_exist["JOBS_DEDUCTED"], _new_mc_val)
                _exist["SETUP_MC"] = _exist["JOBS_DEDUCTED"]
                _exist["CARRYOVER_MC"] = max(_exist["CARRYOVER_MC"], _carryover_val)
                _exist["MC_THIS_WEEK"] = _exist["SETUP_MC"] + _exist["CARRYOVER_MC"]
                _exist["MC_PREV_WEEK"] = _exist["CARRYOVER_MC"]
                _exist["KP_WEIGHT"] = _exist["KP_WEIGHT"] + _qty_n
                _so_n = str(row["SC_SO_NO"]).strip()
                if _so_n and _so_n not in str(_exist["SC_SO_NO"]).split(", "):
                    _exist["SC_SO_NO"] = f"{_exist['SC_SO_NO']}, {_so_n}" if _exist["SC_SO_NO"] else _so_n
                continue
            _by_key[_key_n] = {
                "PLAN_SOURCE": "NEW",
                "PLAN_WEEK": row["PLAN_WEEK"],
                "PLAN_YEAR": row.get("PLAN_YEAR"),
                "ITEM_CODE": row["ITEM_CODE"],
                "SC_SO_NO": str(row["SC_SO_NO"]).strip(),
                "MC_GROUP": row["MC_GROUP"],
                "MC_GUAGE": row.get("MC_GUAGE", ""),
                "TYPE": _type_key_new,
                "JOBS_DEDUCTED": _new_mc_val,
                "SETUP_MC": _new_mc_val,
                "CARRYOVER_MC": _carryover_val,
                "MC_THIS_WEEK": _new_mc_val + _carryover_val,
                "MC_PREV_WEEK": _carryover_val,
                "SETUP_DAYS": row.get("SETUP_DAYS", 0),
                "KP_WEIGHT": _qty_n,
                "DAILY_CAPACITY": row.get("DAILY_CAPACITY", 0),
            }
    return list(_by_key.values())

# --- OLD PLAN (BOOKING): ใช้ _IS_NEW_SETUP / _MC_INCREASE จาก AVA_MC.py ---
# OLD มาจาก booking baseline (detail_mc) เหมือนกันทั้ง 2 pass → สร้างครั้งเดียว ใช้ร่วม
_old_setup_rows = []
# ตรงกับ logic ของ weekly_job_usage_old เพื่อให้ตัวเลขสอดคล้องกับ REMAINING_JOBS
if (
    not detail_mc.empty
    and "WEEK" in detail_mc.columns
    and "ITEM_CODE" in detail_mc.columns
    and "MC_USE_CEIL" in detail_mc.columns
    and "MC_GROUP" in detail_mc.columns
):
    _det_trk = detail_mc.copy()
    _det_trk["WEEK"] = pd.to_numeric(_det_trk["WEEK"], errors="coerce")
    _det_trk["MC_USE_CEIL"] = (
        pd.to_numeric(_det_trk["MC_USE_CEIL"], errors="coerce").fillna(0).astype(int)
    )
    _det_trk = _det_trk.dropna(subset=["WEEK", "ITEM_CODE", "MC_GROUP"])
    _det_trk["WEEK"] = _det_trk["WEEK"].astype(int)
    _det_trk["ITEM_CODE"] = _det_trk["ITEM_CODE"].astype(str).str.strip().str.upper()
    _det_trk["MC_GROUP"] = _det_trk["MC_GROUP"].astype(str).str.strip().str.upper()
    _det_trk_active = _det_trk[_det_trk["MC_USE_CEIL"] > 0].copy()
    # ตัด IMPORT ออกด้วยเกณฑ์เดียวกับ weekly_job_usage — SETUP_TRACKING ต้องนับตรงกับตัวคุมโควตา
    _det_trk_active = _det_trk_active[~_det_trk_active["MC_GROUP"].isin(_JOB_EXCLUDE_MC)]

    _has_ava_flags_trk = "_IS_NEW_SETUP" in _det_trk.columns and "_MC_INCREASE" in _det_trk.columns

    if _has_ava_flags_trk:
        _det_trk_active["_IS_NEW_SETUP"] = _det_trk_active["_IS_NEW_SETUP"].fillna(True).astype(bool)
        _det_trk_active["_MC_INCREASE"] = pd.to_numeric(_det_trk_active["_MC_INCREASE"], errors="coerce").fillna(0).astype(int)

        for _, _drow_t in _det_trk_active.iterrows():
            _is_new_t = bool(_drow_t["_IS_NEW_SETUP"])
            _mc_ceil_t = int(_drow_t["MC_USE_CEIL"])
            _mc_inc_t = int(_drow_t["_MC_INCREASE"])
            _new_jobs_t = _mc_ceil_t if _is_new_t else _mc_inc_t
            if _new_jobs_t <= 0:
                continue

            _carryover_t = 0 if _is_new_t else (_mc_ceil_t - _mc_inc_t)
            _wk_t = int(_drow_t["WEEK"])
            _mc_grp_t = str(_drow_t["MC_GROUP"]).strip().upper()
            _item_t = str(_drow_t["ITEM_CODE"]).strip().upper()

            _gauge_t = str(_drow_t.get("GUAGE", "")).strip() if "GUAGE" in _drow_t.index else ""
            _so_t = ""
            for _so_col in ("SC_SO_NO", "SO_NO"):
                if _so_col in _drow_t.index:
                    _so_t = ColorOpen.norm_sc(_drow_t.get(_so_col, ""))
                    break

            # (เดิมมี lookup ลูกค้าตรงนี้ แต่ค่าไม่ได้ถูกใส่ลงแถว SETUP_TRACKING เลย —
            #  เป็นการสแกน orders ทั้งตารางทุกแถวเปล่า ๆ และคีย์ก็ผิดด้าน S/ไม่ S จึงตัดออก)
            _raw_prev = pd.to_numeric(_drow_t.get("_PREV_MC_USE_CEIL", _carryover_t), errors="coerce")
            _prev_mc_t = 0 if pd.isna(_raw_prev) else int(_raw_prev)
            _kp_weight_t = pd.to_numeric(_drow_t.get("KP_WEIGHT", 0), errors="coerce") or 0
            _type_key_old = _get_type_key_for_mc(_mc_grp_t)
            _old_setup_rows.append({
                "PLAN_SOURCE": "OLD",
                "PLAN_WEEK": _wk_t,
                "PLAN_YEAR": _week_year_lookup.get(_wk_t, TODAY.year),
                "ITEM_CODE": _item_t,
                "SC_SO_NO": _so_t,
                "MC_GROUP": _mc_grp_t,
                "MC_GUAGE": _gauge_t,
                "TYPE": _type_key_old,
                "JOBS_DEDUCTED": _new_jobs_t,
                "SETUP_MC": _new_jobs_t,
                "CARRYOVER_MC": _carryover_t,
                "MC_THIS_WEEK": _mc_ceil_t,
                "MC_PREV_WEEK": _prev_mc_t,
                "SETUP_DAYS": 0,
                "KP_WEIGHT": _kp_weight_t,
                "DAILY_CAPACITY": 0,
            })
    else:
        # Fallback: week-over-week comparison (กรณีไม่มี AVA_MC.py flags)
        for _mc_grp, _grp_df in _det_trk_active.groupby("MC_GROUP"):
            _all_weeks_trk = sorted(_grp_df["WEEK"].unique())
            _week_item_mc_trk: dict = {}
            for _wk_t in _all_weeks_trk:
                _wk_rows_t = _grp_df[_grp_df["WEEK"] == _wk_t]
                _week_item_mc_trk[_wk_t] = (
                    _wk_rows_t.groupby("ITEM_CODE")["MC_USE_CEIL"].sum().to_dict()
                )
            for _i_t, _wk_t in enumerate(_all_weeks_trk):
                _curr_items_t = _week_item_mc_trk[_wk_t]
                _prev_items_t = (
                    _week_item_mc_trk.get(_all_weeks_trk[_i_t - 1], {}) if _i_t > 0 else {}
                )
                for _item_t, _mc_t in _curr_items_t.items():
                    _prev_mc_t = _prev_items_t.get(_item_t, 0)
                    if _prev_mc_t == 0:
                        _new_jobs_t = _mc_t
                    elif _mc_t > _prev_mc_t:
                        _new_jobs_t = _mc_t - _prev_mc_t
                    else:
                        continue
                    _item_rows_t = _grp_df[
                        (_grp_df["WEEK"] == _wk_t) & (_grp_df["ITEM_CODE"] == _item_t)
                    ]
                    _gauge_t = ""
                    _so_t = ""
                    if not _item_rows_t.empty:
                        if "GUAGE" in _item_rows_t.columns:
                            _gauge_t = str(_item_rows_t.iloc[0].get("GUAGE", "")).strip()
                        for _so_col in ("SC_SO_NO", "SO_NO"):
                            if _so_col in _item_rows_t.columns:
                                _so_t = ColorOpen.norm_sc(_item_rows_t.iloc[0].get(_so_col, ""))
                                break
                    # lookup ลูกค้าถูกตัดออกด้วยเหตุผลเดียวกับ pass ด้านบน (ค่าไม่ได้ถูกใช้)
                    _kp_weight_fb = pd.to_numeric(_item_rows_t.iloc[0].get("KP_WEIGHT", 0), errors="coerce") if not _item_rows_t.empty and "KP_WEIGHT" in _item_rows_t.columns else 0
                    _type_key_old = _get_type_key_for_mc(str(_mc_grp).strip().upper())
                    _old_setup_rows.append({
                        "PLAN_SOURCE": "OLD",
                        "PLAN_WEEK": _wk_t,
                        "PLAN_YEAR": _week_year_lookup.get(int(_wk_t), TODAY.year),
                        "ITEM_CODE": _item_t,
                        "SC_SO_NO": _so_t,
                        "MC_GROUP": str(_mc_grp).strip().upper(),
                        "MC_GUAGE": _gauge_t,
                        "TYPE": _type_key_old,
                        "JOBS_DEDUCTED": _new_jobs_t,
                        "SETUP_MC": _new_jobs_t,
                        "CARRYOVER_MC": _prev_mc_t,
                        "MC_THIS_WEEK": _mc_t,
                        "MC_PREV_WEEK": _prev_mc_t,
                        "SETUP_DAYS": 0,
                        "KP_WEIGHT": _kp_weight_fb or 0,
                        "DAILY_CAPACITY": 0,
                    })



def _finalize_setup_df(rows):
    """ประกอบ setup_tracking_df จาก rows (NEW + OLD): + PLAN_YEAR, sort, เติม DAILY_CAPACITY"""
    _df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(columns=[
            "PLAN_SOURCE", "PLAN_WEEK", "PLAN_YEAR", "ITEM_CODE", "SC_SO_NO",
            "MC_GROUP", "MC_GUAGE", "TYPE", "JOBS_DEDUCTED", "SETUP_MC", "CARRYOVER_MC",
            "MC_THIS_WEEK", "MC_PREV_WEEK", "SETUP_DAYS", "KP_WEIGHT", "DAILY_CAPACITY",
        ])
    )
    if not _df.empty and "PLAN_WEEK" in _df.columns:
        _df["PLAN_YEAR"] = _df["PLAN_WEEK"].map(
            lambda w: yw_year(w) if yw_year(w) is not None else _week_year_lookup.get(w)
        ).fillna(TODAY.year).astype(int)
    if not _df.empty:
        _df = _df.sort_values(["PLAN_WEEK", "PLAN_SOURCE", "ITEM_CODE"], ignore_index=True)
    # เติม DAILY_CAPACITY สำหรับ OLD PLAN (จาก detail_mc) ที่ยังไม่มีค่า
    if not _df.empty and "DAILY_CAPACITY" in _df.columns:
        _df["DAILY_CAPACITY"] = _df["DAILY_CAPACITY"].fillna(0)
    return _df

setup_tracking_df = _finalize_setup_df(_new_setup_rows(plan_df) + _old_setup_rows)


if not setup_tracking_df.empty:
    print(f"\nDEBUG: setup_tracking_df shape = {setup_tracking_df.shape}")

print("📋 สรุป SETUP_TRACKING (ITEM ที่หัก Job) ต่อ Week:")
if not setup_tracking_df.empty:
    for _wk_s in sorted(setup_tracking_df["PLAN_WEEK"].unique()):
        _wdf_s = setup_tracking_df[setup_tracking_df["PLAN_WEEK"] == _wk_s]
        _total_jobs_s = int(_wdf_s["JOBS_DEDUCTED"].sum())
        _total_mc_s = int(_wdf_s["SETUP_MC"].sum())
        print(f"   Week {_wk_s}: {len(_wdf_s)} items — หัก {_total_jobs_s} Jobs, Setup {_total_mc_s} MC")
        for _, _r_s in _wdf_s.iterrows():
            _src_icon = "🆕" if _r_s["PLAN_SOURCE"] == "NEW" else "📋"
            print(
                f"     {_src_icon} [{_r_s['PLAN_SOURCE']}] {_r_s['ITEM_CODE']}"
                f" ({_r_s.get('SC_SO_NO', '')}) - {_r_s['MC_GROUP']}"
                f" — หัก {_r_s['JOBS_DEDUCTED']} Job, Setup {_r_s['SETUP_MC']} MC"
            )
    print()
else:
    print("   (ไม่มีการหัก Job ในแผนนี้)")
    print()

# =========================
# สรุป CYLINDER CHANGE — แสดงการเปลี่ยน gauge ของเครื่องที่ว่าง (factory-level)
# =========================
# สร้าง reverse lookup: (week, factory, mc_cat, tgt_g) → [item_codes]
_cyl_item_lookup: dict = {}
_cyl_src_lookup: dict = {}  # (week, factory, mc_cat, tgt_g) → src_g
for (_ciw, _cii, _cimg), (_cisrc, _citgt) in _cylinder_change_for_item.items():
    _cif = _mc_to_factory(_cimg, _citgt)
    _cicat = _mc_to_type1(_cimg, _citgt)
    _lk = (int(_ciw), _cif, _cicat, _citgt)
    _cyl_item_lookup.setdefault(_lk, []).append(_cii)
    _cyl_src_lookup[_lk] = _cisrc

print("🔄 สรุป CYLINDER CHANGE (เปลี่ยนเครื่องที่ว่าง):")
if _cyl_item_lookup:
    for _lk in sorted(_cyl_item_lookup.keys()):
        _ciw2, _cif2, _cicat2, _citgt2 = _lk
        _cisrc2 = _cyl_src_lookup.get(_lk, "?")
        _n_mc = len(_cyl_item_lookup[_lk])
        _items_for = ", ".join(_cyl_item_lookup[_lk])
        print(f"   W{_ciw2}  Factory: {_cif2}  MC_CAT: {_cicat2}  Gauge {_cisrc2} → Gauge {_citgt2}  ({_n_mc} เครื่อง)  → {_items_for}")
else:
    print("   ✅ ไม่มี CYLINDER CHANGE ในแผนนี้")
print()

# บันทึกไฟล์ใหม่ (รวม SETUP_TRACKING)
if not plan_df.empty:
    _dbg_item = "FD6PRTPG99A0"
    _dbg_rows = plan_df[
        plan_df["ITEM_CODE"].astype(str).str.strip().str.upper() == _dbg_item
    ]
    if not _dbg_rows.empty:
        _dbg_view = _dbg_rows[[
            "SC_SO_NO",
            "PLAN_WEEK",
            "MC_GROUP",
            "PRODUCE_QTY",
            "SETUP_DAYS",
            "CARRYOVER_MC",
            "NEW_MC",
            "ACTUAL_MC",
        ]].sort_values(["PLAN_WEEK", "SC_SO_NO"])
        print(_dbg_view.to_string(index=False))



# สร้าง DataFrame สำหรับ CYLINDER_CHANGE sheet (factory-level) — per trigger week
_cyl_rows = []
for _lk in sorted(_cyl_item_lookup.keys()):
    _ciw2, _cif2, _cicat2, _citgt2 = _lk
    _cisrc2 = _cyl_src_lookup.get(_lk, "?")
    _cyl_rows.append({
        "WEEK": _ciw2,
        "FACTORY": _cif2,
        "MC_CAT": _cicat2,
        "GAUGE_FROM": _cisrc2,
        "GAUGE_TO": _citgt2,
        "MC_CHANGED": len(_cyl_item_lookup[_lk]),
        "ITEM_CODE": ", ".join(_cyl_item_lookup[_lk]),
    })
_cylinder_change_df = pd.DataFrame(_cyl_rows) if _cyl_rows else pd.DataFrame(
    columns=["WEEK", "FACTORY", "MC_CAT", "GAUGE_FROM", "GAUGE_TO", "MC_CHANGED", "ITEM_CODE"]
)

# ===== FOLD CHECK: ยอดที่ลูกค้าเปิดมา แบ่งเป็นพับแล้วเป็นพับคู่ (หาร 2 ลงตัว) หรือไม่ =====
# เช็คที่ "ยอดรวมทั้ง order" (ITEM_CODE + SC_SO_NO) — ไม่ใช่รายสัปดาห์
# → SC ไหนหารไม่ลงตัว ทุกสัปดาห์ของ SC นั้นติดธงเตือนเหมือนกันหมด
# เฉพาะกลุ่ม IRMT/SJT เท่านั้น (กลุ่มอื่นไม่มีกฎพับคู่)
# แผนวางเต็มตามยอดจริงเสมอ ค่าพวกนี้เป็นแค่ธงเตือนให้ planner ไปแก้ที่ยอดเปิด order
if not plan_df.empty and {"ORDERS_QTY", "REVOLUTION_WEIGHT", "MC_GROUP"} <= set(plan_df.columns):
    _fold_cache = {}
    for _fo_key, _fo_grp in plan_df.groupby(["ITEM_CODE", "SC_SO_NO"], dropna=False):
        _fold_cache[_fo_key] = fold_check_order(
            _fo_grp["ORDERS_QTY"].max(),
            _fo_grp["REVOLUTION_WEIGHT"].max(),
            _fo_grp["MC_GROUP"].unique(),
        )
    _fold_vals = [
        _fold_cache.get((_r_item, _r_sc), (0.0, 0.0, False))
        for _r_item, _r_sc in zip(plan_df["ITEM_CODE"], plan_df["SC_SO_NO"])
    ]
    plan_df["FOLD_QTY"] = [_v[0] for _v in _fold_vals]
    plan_df["FOLD_REMAINDER"] = [_v[1] for _v in _fold_vals]
    plan_df["FOLD_WARN"] = [1 if _v[2] else 0 for _v in _fold_vals]

    _fold_bad = sorted(k for k, v in _fold_cache.items() if v[2])
    _fold_scope = sum(
        1 for _k, _g in plan_df.groupby(["ITEM_CODE", "SC_SO_NO"], dropna=False)
        if any(str(_m).strip().upper() in FOLD_ROUND_MC_GROUPS for _m in _g["MC_GROUP"].unique())
    )
    print(f"\n📏 FOLD CHECK ({'/'.join(sorted(FOLD_ROUND_MC_GROUPS))}): "
          f"order ที่แบ่งพับแล้วหาร {FOLD_MULTIPLE} ไม่ลงตัว "
          f"{len(_fold_bad)}/{_fold_scope} order")
    for _fb_item, _fb_sc in _fold_bad:
        _fb_folds, _fb_rem, _ = _fold_cache[(_fb_item, _fb_sc)]
        print(f"   ⚠️  {_fb_item} SC {_fb_sc}: {_fb_folds:g} พับ → เหลือเศษ {_fb_rem:g} พับ")
    print()

# ทีมของแถวแผน — TEAM = "ทีมจริง" (TEAM_NAME จาก booking/สต๊อก) ไม่ใช่ทีมตามชีท Program
# งานจาก SC ล้วนไม่มีทีมติดมา (view_sc ไม่มีคอลัมน์นี้) จึง map ย้อนตามลำดับใน
# _lookup_row_team: ใบสั่งเดียวกัน → item ใน booking → item ในสต๊อก → ชีท Program
# TEAM_PROGRAM แยกไว้ต่างหาก (ทีมตามชีท Program ล้วน) — หน้าเว็บใช้คู่ ITEM_CODE+TEAM_PROGRAM
# ตัดสินไฮไลต์ตัวหนังสือ ถ้าปนกับทีมจริงในคอลัมน์เดียวไฮไลต์จะเพี้ยนตามข้อมูล booking
_team_src_count: dict = {}
for _pdf in (plan_df, setup_tracking_df, _unplanned_df):
    if _pdf is None or _pdf.empty or "ITEM_CODE" not in _pdf.columns:
        continue
    _so_series = _pdf["SC_SO_NO"] if "SC_SO_NO" in _pdf.columns else pd.Series([""] * len(_pdf), index=_pdf.index)
    _pairs = [_lookup_row_team(_it, _so) for _it, _so in zip(_pdf["ITEM_CODE"], _so_series)]
    _pdf["TEAM"] = [p[0] for p in _pairs]
    _pdf["TEAM_PROGRAM"] = _pdf["ITEM_CODE"].map(_lookup_item_team)
    if _pdf is plan_df:
        for _p in _pairs:
            _team_src_count[_p[1]] = _team_src_count.get(_p[1], 0) + 1
_SRC_LABEL = {"so": "ใบสั่งเดียวกัน", "item": "item ใน booking",
              "stock": "item ในสต๊อก", "program": "ชีท Program", "blank": "ไม่พบทีม"}
print(f"🏷️  เติมคอลัมน์ TEAM (ทีมจริง) ในชีท PLAN: "
      f"{int((plan_df['TEAM'] != '').sum()) if 'TEAM' in plan_df.columns else 0}/{len(plan_df)} แถว — "
      + ", ".join(f"{_SRC_LABEL.get(_k, _k)} {_v}"
                  for _k, _v in sorted(_team_src_count.items(), key=lambda kv: -kv[1])))

# REPLAN: สัปดาห์เดิมที่ booking จองไว้ → หน้าเว็บใช้บอกบนบล็อกว่างานนี้ "Move มาจาก W ไหน"
# ว่าง = งานจาก SC ล้วน (ไม่เคยอยู่ใน booking) ; FROM != TO = booking เดิมกระจายหลายสัปดาห์
if REPLAN_MODE and _replan_from_week and not plan_df.empty:
    _bw_pairs = [
        _replan_from_week.get((str(_sc).strip().upper(), str(_it).strip().upper()))
        for _sc, _it in zip(plan_df["SC_SO_NO"], plan_df["ITEM_CODE"])
    ]
    plan_df["BOOKING_WEEK_FROM"] = pd.array([p[0] if p else None for p in _bw_pairs], dtype="Int64")
    plan_df["BOOKING_WEEK_TO"] = pd.array([p[1] if p else None for p in _bw_pairs], dtype="Int64")
    print(f"🔁 REPLAN: เติมสัปดาห์เดิมของ booking ลงชีท PLAN "
          f"{int(plan_df['BOOKING_WEEK_FROM'].notna().sum())}/{len(plan_df)} แถว")


def _fill_kept_rows(_df: pd.DataFrame, _label: str) -> pd.DataFrame:
    """เติมคอลัมน์ที่แถว "คงแผนเดิม" ยังขาด — แถว core ที่ Replan แปะกลับ (PLAN_SOURCE=CORE_KEEP)
    และงานที่ตรึงไว้ (🔐) ไม่ได้ผ่านลูปวางแผน จึงไม่มีค่าที่ต้องใช้ข้อมูลของไฟล์นี้:
      · TARGET_KNIT — ต้องใช้ปฏิทิน + offset ตาม FOB_TYPE (มี FG_WEEK ของใบนั้นมาแล้ว)
        (core ไม่ได้ถูกจัดตาม target — ค่านี้ไว้ให้ดูกำหนดส่งเฉย ๆ ดู IS_CORE_ITEM ที่หน้าเว็บ)
      · TEAM ที่ยังว่าง + TEAM_PROGRAM (Replan ไม่ได้อ่านชีท Program)
    ใช้ 2 ที่: แปะแถวคงแผนเดิมในรอบ replan (ทางปัจจุบัน) และแปะชีท REPLAN ของรอบหลัก (ทางเก่า)"""
    if {"FG_WEEK", "TARGET_KNIT"} <= set(_df.columns):
        _need_tk = _df["TARGET_KNIT"].isna() & _df["FG_WEEK"].notna()
        if int(_need_tk.sum()):
            _tk_vals = [
                _target_knit_of(_r.get("FG_WEEK"), _r.get("ORDER_TYPE"), _r)
                for _, _r in _df[_need_tk].iterrows()
            ]
            _df.loc[_need_tk, "TARGET_KNIT"] = [t[0] for t in _tk_vals]
            if "TARGET_YEAR" in _df.columns:
                _df.loc[_need_tk, "TARGET_YEAR"] = [t[1] for t in _tk_vals]
            print(f"🎯 เติม TARGET_KNIT ให้แถวใน{_label} ที่ยังว่าง {int(_need_tk.sum())} แถว "
                  f"(ส่วนใหญ่คือแถว core ที่คงแผนเดิม)")
    # พร้อมอุดแถวที่ TEAM ยังว่าง (แถวจากไฟล์ที่รันก่อนมีฟีเจอร์ทีม)
    if "ITEM_CODE" in _df.columns:
        _so = (_df["SC_SO_NO"] if "SC_SO_NO" in _df.columns
               else pd.Series([""] * len(_df), index=_df.index))
        if "TEAM" not in _df.columns:
            _df["TEAM"] = ""
        _blank = _df["TEAM"].fillna("").astype(str).str.strip() == ""
        if int(_blank.sum()):
            _df.loc[_blank, "TEAM"] = [
                _lookup_row_team(_it, _s)[0]
                for _it, _s in zip(_df.loc[_blank, "ITEM_CODE"], _so[_blank])
            ]
        _df["TEAM_PROGRAM"] = _df["ITEM_CODE"].map(_lookup_item_team)
        print(f"🏷️  {_label}: เติม TEAM ให้แถวที่ยังว่าง {int(_blank.sum())} แถว + คอลัมน์ TEAM_PROGRAM")
    return _df


def _share_kept_booking_mc(plan_df: pd.DataFrame, kept_df: pd.DataFrame) -> pd.DataFrame:
    """แถวแผนที่ "เติมเข้าเครื่อง booking" ซ้อนกับแถวคงแผนเดิม (KP/core/ตรึง) ที่ถือเครื่องชุดเดียวกัน
    → ลด ACTUAL_MC ของแถวแผนลงเท่าส่วนที่ซ้อน ให้เครื่องชุดนั้นถูกนับครั้งเดียว

    ตอนลูปวางแผนเขียนแถวเติม (หา `_fill_shared_owner` ใน plans) แถวคงแผนเดิมยังไม่ถูกแปะ
    → หาเจ้าของเครื่องไม่เจอ → แถวเติมเขียน ACTUAL_MC = เครื่อง booking เอง ; พอแปะแถว KP
    (ใบสั่งอื่น ACTUAL_MC = MC_BOOKING = เครื่องชุดเดียวกัน) สูตร "max ในใบ → บวกข้ามใบ − max MC_BOOKING"
    (_pool_blocks_from_plan / PlanGantt.planMcAgg) นับเครื่องชุดนั้นสองรอบ → Gantt ขึ้น "เกิน N" ปลอม
    (เคสจริง 28 ก.ย. 2569: W42 DOUBLE-30 เกจ 19 ใช้จริง 7/7 แต่โชว์เกิน 3 ; ทั้งแผนซ้ำ 29 ตัว 12 บล็อก)

    ส่วนซ้อนต่อบล็อก = เครื่องที่แผนกินเพิ่ม "หลังแปะ" − "ก่อนแปะ" (แถวคงแผนเดิมไม่กินพูลเพิ่มเอง
    เพราะเครื่องถูกกันไว้ใน capacity แล้ว) → ลดแถวแผนที่ไม่ได้ตั้งเครื่องใหม่ก่อน
    ห้ามลดต่ำกว่า NEW_MC ; ลดจนเหลือ 0 → ประกาศ MC_SHARED (หน้าเว็บถือเป็นแถวเกาะเครื่อง 📦)
    ลดไม่หมดแถว → ไม่ตั้ง MC_SHARED (mcRecalc.isPassenger ข้ามทั้งแถวที่ MC_SHARED > 0)
    """
    if plan_df.empty or kept_df is None or kept_df.empty:
        return plan_df
    if not {"PLAN_WEEK", "ITEM_CODE", "MC_GROUP", "MC_GUAGE", "ACTUAL_MC"} <= set(plan_df.columns):
        return plan_df
    _before = _pool_blocks_from_plan(plan_df.to_dict("records"))
    _after = _pool_blocks_from_plan(plan_df.to_dict("records") + kept_df.to_dict("records"))
    _excess = {k: v - _before.get(k, 0) for k, v in _after.items() if v > _before.get(k, 0)}
    if not _excess:
        return plan_df

    df = plan_df
    for _c in ("NEW_MC", "CARRYOVER_MC", "MC_SHARED", "MC_BOOKING"):
        if _c not in df.columns:
            df[_c] = 0
    _num = lambda c: pd.to_numeric(df[c], errors="coerce").fillna(0)
    _a, _n, _cy, _sh, _bk = (_num(c) for c in ("ACTUAL_MC", "NEW_MC", "CARRYOVER_MC", "MC_SHARED", "MC_BOOKING"))
    _src = (df["PLAN_SOURCE"].fillna("").astype(str).str.strip().str.upper()
            if "PLAN_SOURCE" in df.columns else pd.Series("", index=df.index))
    _keys = [
        (int(w), str(it).strip().upper(), str(m).strip().upper(), _normalize_gauge(g))
        if pd.notna(w) else None
        for w, it, m, g in zip(pd.to_numeric(df["PLAN_WEEK"], errors="coerce"),
                               df["ITEM_CODE"], df["MC_GROUP"], df["MC_GUAGE"])
    ]
    _so = (df["SC_SO_NO"].astype(str).str.strip() if "SC_SO_NO" in df.columns
           else pd.Series("", index=df.index))
    _kept_so = {}
    for _r in kept_df.to_dict("records"):
        _w = pd.to_numeric(_r.get("PLAN_WEEK"), errors="coerce")
        if pd.isna(_w) or float(pd.to_numeric(_r.get("ACTUAL_MC", 0), errors="coerce") or 0) <= 0:
            continue
        _k = (int(_w), str(_r.get("ITEM_CODE", "")).strip().upper(),
              str(_r.get("MC_GROUP", "")).strip().upper(), _normalize_gauge(_r.get("MC_GUAGE")))
        _kept_so.setdefault(_k, str(_r.get("SC_SO_NO", "")).strip())

    _by_key: dict = {}
    for _i, _k in zip(df.index, _keys):
        if _k in _excess and not _src[_i].startswith("HOLD"):
            _by_key.setdefault(_k, []).append(_i)

    _fixed_rows, _fixed_mc, _left = 0, 0, 0
    for _k, _ex in _excess.items():
        _idx = _by_key.get(_k, [])
        # ใบสั่งที่ไม่ได้ตั้งเครื่องใหม่ + เกาะเครื่อง booking มาก่อน
        _sos = sorted({_so[i] for i in _idx},
                      key=lambda s: (max(_n[i] for i in _idx if _so[i] == s) > 0,
                                     -max(_bk[i] for i in _idx if _so[i] == s)))
        for _s in _sos:
            if _ex <= 0:
                break
            _rows = [i for i in _idx if _so[i] == _s and _a[i] > 0]
            if not _rows:
                continue
            _old = max(_a[i] for i in _rows)
            _d = min(_ex, max(0, _old - max(_n[i] for i in _rows)))
            if _d <= 0:
                continue
            for i in _rows:
                _new_a = max(_n[i], _a[i] - _d)
                _cut = _a[i] - _new_a
                if _cut <= 0:
                    continue
                df.at[i, "ACTUAL_MC"] = int(_new_a)
                df.at[i, "CARRYOVER_MC"] = int(max(0, min(_cy[i], _new_a - _n[i])))
                if _new_a == 0:
                    df.at[i, "MC_SHARED"] = int(_sh[i] + _cut)
                if "REMARK" in df.columns:
                    _rm = "" if pd.isna(df.at[i, "REMARK"]) else str(df.at[i, "REMARK"])
                    df.at[i, "REMARK"] = (
                        f"{_rm} | ใช้เครื่อง booking {int(_cut)} ตัวร่วมกับ SO {_kept_so.get(_k, '?')} "
                        f"(ใบที่คงแผนเดิม) ไม่ใช้เครื่องเพิ่ม"
                    ).lstrip(" |")
                _fixed_rows += 1
            _ex -= _old - max(max(_n[i], _a[i] - _d) for i in _rows)
            _fixed_mc += int(_old - max(max(_n[i], _a[i] - _d) for i in _rows))
        _left += max(0, int(_ex))
    print(f"🔧 เครื่อง booking ซ้อนกับแถวคงแผนเดิม: {len(_excess)} บล็อก ลด ACTUAL_MC {_fixed_rows} แถว "
          f"รวม {_fixed_mc} เครื่อง (กันหน้าเว็บนับเครื่องชุดเดียวกันสองรอบ)"
          + (f" ⚠️ ลดไม่ได้อีก {_left} เครื่อง" if _left else ""))
    return df


# =========================
# REPLAN: แปะแถวคงแผนเดิม (งานที่ตรึง 🔐 + core ที่เบรกไว้ + ใบที่ออกเลข KP 🎫)
# =========================
# Replan.py เตรียมแถวไว้ใน replan_append.xlsx ก่อนเรียกไฟล์นี้ — ไม่ผ่านลูปวางแผน (เครื่องถูกกันไว้ใน
# capacity แล้ว, ยอดถูกหักออกจากดีมานด์แล้ว) แค่ต้องโผล่เป็นแถวแผนให้ลาก/แก้บนหน้าเว็บได้
# แปะหลัง SETUP_TRACKING / BOOKING_WEEK_FROM เหมือนตอนที่ Replan เคยแปะหลัง Planning จบ
# → ตัวเลขชีทอื่นไม่เปลี่ยน ; ต่างจากเดิมแค่ได้ TARGET_KNIT / ทีมครบในรอบเดียว (เดิมรอบหลักเติมให้)
if REPLAN_MODE and REPLAN_APPEND_FILE and Path(REPLAN_APPEND_FILE).exists():
    try:
        _ap_x = pd.ExcelFile(REPLAN_APPEND_FILE)
        _ap_pin = _ap_x.parse("PINNED") if "PINNED" in _ap_x.sheet_names else pd.DataFrame()
        _ap_core = _ap_x.parse("CORE") if "CORE" in _ap_x.sheet_names else pd.DataFrame()
        # ชีท KP = ใบที่ออกเลขใบจองแล้ว (Replan.py: APPEND_KP_SHEET) — ไฟล์ที่เขียนก่อน
        # 23 ก.ย. 2569 ยังไม่มีชีทนี้ → ว่าง = พฤติกรรมเดิม ไม่ล้ม
        _ap_kp = _ap_x.parse("KP") if "KP" in _ap_x.sheet_names else pd.DataFrame()
    except Exception as _e_ap:
        print(f"⚠️ อ่าน {Path(REPLAN_APPEND_FILE).name} ไม่ได้ → ไม่ได้แปะงานที่ตรึง/core/KP กลับ: {_e_ap}")
        _ap_pin, _ap_core, _ap_kp = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    for _ap_df in (_ap_pin, _ap_core, _ap_kp):
        _ap_df.columns = _ap_df.columns.astype(str).str.strip().str.upper()
        # แถวจาก Replan เป็นรูปไฟล์ (สัปดาห์ + ปีแยกคอลัมน์) → YW ให้ตรงกับแถวแผนในไฟล์นี้ก่อนรวม
        _weeks_to_yw(_ap_df, "PLAN_WEEK", "PLAN_YEAR")
        _weeks_to_yw(_ap_df, "TARGET_KNIT", "TARGET_YEAR")
    # DUE_BASIS (FG มาจาก RDD) ติดมากับแถว core/KP — ต้องมีคอลัมน์ใน plan_df ก่อน reindex ไม่งั้นหลุด
    # (แผนว่างไม่มีคอลัมน์ = ข้าม — ด้านล่างใช้คอลัมน์ของ core เองอยู่แล้ว)
    if ((not _ap_core.empty or not _ap_kp.empty) and len(plan_df.columns)
            and "DUE_BASIS" not in plan_df.columns):
        plan_df["DUE_BASIS"] = ""
    _ap_parts = [plan_df]
    if not _ap_pin.empty:
        # งานที่ตรึงมาจากไฟล์แผนรอบก่อน → คอลัมน์เกินได้ เก็บไว้ต่อท้าย
        _ap_parts.append(_ap_pin)
    if not _ap_core.empty:
        # core สร้างจาก booking → เอาเฉพาะคอลัมน์ของแผน (ที่ไม่มีค่า = ว่าง)
        # (แผนว่างทั้งแผน = ไม่มีคอลัมน์ให้ยึด → ใช้คอลัมน์ของ core เอง ไม่งั้นแถวหายหมด)
        _ap_parts.append(_ap_core.reindex(columns=plan_df.columns) if len(plan_df.columns) else _ap_core)
    if not _ap_kp.empty:
        # ใบที่ออกเลข KP — สร้างจาก booking ด้วยฟังก์ชันเดียวกับ core → จัดคอลัมน์แบบเดียวกัน
        _ap_parts.append(_ap_kp.reindex(columns=plan_df.columns) if len(plan_df.columns) else _ap_kp)
    if len(_ap_parts) > 1:
        # ต้องทำก่อนรวม — ตอนนี้ยังแยกได้ว่าแถวไหนมาจากลูปวางแผน แถวไหนคงแผนเดิม
        # (แถวตรึงเก็บ PLAN_SOURCE เดิมจากไฟล์แผน = 'NEW' ได้ แยกด้วยคอลัมน์ไม่ได้)
        plan_df = _share_kept_booking_mc(
            plan_df, pd.concat([_p for _p in _ap_parts[1:] if not _p.empty], ignore_index=True))
        _ap_parts[0] = plan_df
        _ap_qty = sum(
            float(pd.to_numeric(_p["PRODUCE_QTY"], errors="coerce").fillna(0).sum())
            for _p in _ap_parts[1:] if "PRODUCE_QTY" in _p.columns
        )
        _ap_cols = plan_df.columns.tolist()
        for _p in _ap_parts[1:]:
            _ap_cols += [c for c in _p.columns if c not in _ap_cols]
        plan_df = pd.concat(_ap_parts, ignore_index=True)[_ap_cols]
        if "PLAN_WEEK" in plan_df.columns:
            plan_df = plan_df.sort_values(
                by=["PLAN_WEEK"], key=lambda s: pd.to_numeric(s, errors="coerce"), kind="stable"
            ).reset_index(drop=True)
        print(f"📌 แปะแถวคงแผนเดิมเข้าชีท PLAN: งานที่ตรึงไว้ {len(_ap_pin)} แถว + core ที่เบรก {len(_ap_core)} แถว "
              f"+ ใบที่ออกเลข KP {len(_ap_kp)} แถว (รวม {_ap_qty:,.0f} กก.)")
        plan_df = _fill_kept_rows(plan_df, "ชีท PLAN")

# ORDER_PENDING = ยอดที่ใบนี้ "ต้องวางในแผนรอบนี้" (ดู _ORDER_PENDING_BASE)
# ต้องทำหลังแปะแถวคงแผนเดิม เพราะแถวที่ตรึงไว้ (🔐) / core ที่เบรก ไม่ได้ผ่านลูปวางแผน
# (Replan หักยอดของมันออกจากดีมานด์ไปแล้ว) แต่แถวมันอยู่ในชีทพร้อม PRODUCE_QTY
# → ต้องบวกกลับเข้าฐาน ไม่งั้นหน้าเว็บอ่านว่าใบนั้น "วางเกิน" เท่ากับยอดที่ตรึงไว้พอดี
if not plan_df.empty and {"SC_SO_NO", "ITEM_CODE"} <= set(plan_df.columns):
    def _op_col(_name):
        """คอลัมน์ที่ใช้ทำคีย์ — ไฟล์ที่ไม่มีคอลัมน์นั้นถือว่าว่างทั้งคอลัมน์ (ไม่ใช่ KeyError)"""
        return plan_df[_name] if _name in plan_df.columns else [""] * len(plan_df)

    _op_keys = [
        _order_base_key(_sc, _it, _sb, _ln, _fg, _oq, _po)
        for _sc, _it, _sb, _ln, _fg, _oq, _po in zip(
            plan_df["SC_SO_NO"], plan_df["ITEM_CODE"], _op_col("SUB_COLOR"),
            _op_col("SC_LINE_ID"), _op_col("FG_WEEK"), _op_col("ORDERS_QTY"), _op_col("PO_NO"),
        )
    ]
    _op_kept: dict = {}
    if "PLAN_SOURCE" in plan_df.columns and "PRODUCE_QTY" in plan_df.columns:
        _op_src = plan_df["PLAN_SOURCE"].fillna("").astype(str).str.strip().str.upper()
        _op_qty = pd.to_numeric(plan_df["PRODUCE_QTY"], errors="coerce").fillna(0)
        for _k, _s, _q in zip(_op_keys, _op_src, _op_qty):
            # แถวที่ไม่ได้มาจากลูปวางแผนรอบนี้ = ยอดที่ยกมาคงไว้ตามแผนเดิม (ลูปเขียน PLAN_SOURCE
            # ได้แค่ NEW กับ HOLD_* เท่านั้น — PINNED/CORE_KEEP มาจาก replan_append ล้วน)
            # ต้องบวกเสมอแม้ใบนั้นจะมีงานวางใหม่ด้วย: Replan หักยอดที่ตรึงออกจากดีมานด์ไปแล้ว
            # ฐานจากลูปจึงเป็นแค่ส่วนที่วางใหม่ (เคสทดสอบ 23/9/2569 SC 722754: วางใหม่ 7,723.26
            # + ตรึงไว้ 1,708.26 = 9,431.52 ; ถ้าไม่บวกจะขึ้น "เกิน" เท่ายอดที่ตรึงพอดี)
            # แถว HOLD_* (กันเครื่องรอ target / รอสี) ไม่ใช่ยอดที่ยกมา — ยอดเป็น 0 อยู่แล้ว
            # และใบของมันอาจเป็นใบที่ยุบหลาย SC ไว้ช่องเดียว ซึ่งไม่ตรงกับคีย์ใบไหน
            if _s not in ("", "NEW") and not _s.startswith("HOLD"):
                _op_kept[_k] = _op_kept.get(_k, 0.0) + float(_q)
    plan_df["ORDER_PENDING"] = [
        round(_ORDER_PENDING_BASE.get(_k, 0.0) + _op_kept.get(_k, 0.0), 2) or None
        for _k in _op_keys
    ]
    _op_n = int(plan_df["ORDER_PENDING"].notna().sum())
    print(f"📐 คอลัมน์ ORDER_PENDING (ยอดที่ต้องวางจริงต่อใบ): {_op_n}/{len(plan_df)} แถว"
          + (f" — ยกยอดที่ตรึงไว้กลับเข้าฐาน {len(_op_kept)} ใบ" if _op_kept else ""))

# ย้ายคอลัมน์เสริมที่เพิ่มเข้ามา (วันทำงาน + ประเภทเครื่อง + REMARK) + S9_ROUTING
# ไปไว้ท้ายสุด เพื่อไม่ให้แทรกกลางคอลัมน์หลักที่คนอ่านแผนใช้ประจำ
_tail_cols = [
    c
    for c in ["WD_BASE", "WD_W32", "MC_SHARED", "MC_BOOKING", "REMARK", "LINE_REMARK", "S9_ROUTING", "S9_SUPP", "S9_MC", "S9_GAUGE", "S9_POOL", "OUTSOURCE", "COLOR_PART", "MC_HOLD_SPLIT", "YARNPO",
              "PO_IN_DATE", "PO_IN_WEEK", "YARN_STOCK",
              "FOLD_QTY", "FOLD_REMAINDER", "FOLD_WARN", "TEAM", "TEAM_PROGRAM",
              "BOOKING_WEEK_FROM", "BOOKING_WEEK_TO", "ORDER_PENDING", "DUE_BASIS"]
    if c in plan_df.columns
]
if _tail_cols:
    _other_cols = [c for c in plan_df.columns if c not in _tail_cols]
    plan_df = plan_df[_other_cols + _tail_cols]

# ผลรอบ replan (booking ที่ปลดล็อก + SC วางพร้อมกัน) — Replan.py รันมาก่อนหน้าแล้วเก็บไว้ในไฟล์แยก
# แปะเป็นชีทในไฟล์แผนตรงนี้ตอนเขียนไฟล์ครั้งแรก → โผล่ในดรอปดาวน์เลือกชีทบนหน้าเว็บ
_replan_sheets: dict = {}
if not REPLAN_MODE and REPLAN_RESULT_FILE.exists():
    try:
        _rp_age_h = (datetime.now().timestamp() - REPLAN_RESULT_FILE.stat().st_mtime) / 3600
        _rp_x = pd.ExcelFile(REPLAN_RESULT_FILE)
        if "PLAN" in _rp_x.sheet_names:
            _rp_plan = _rp_x.parse("PLAN")
            # replan_result รุ่นเก่ามีแถว core ที่ยังไม่มี TARGET_KNIT/TEAM_PROGRAM (รุ่นใหม่รอบ replan
            # เติมเองแล้ว เรียกซ้ำก็ไม่เปลี่ยนค่า) — ทางนี้ใช้เฉพาะตอนรัน Planning.py รอบหลักด้วยมือ
            _rp_plan = _fill_kept_rows(_rp_plan, "ชีท REPLAN")
            _replan_sheets["REPLAN"] = _rp_plan
        if "COMPARE" in _rp_x.sheet_names:
            _replan_sheets["REPLAN_COMPARE"] = _rp_x.parse("COMPARE")
        if _rp_age_h > 12:
            print(f"⚠️ {REPLAN_RESULT_FILE.name} เก่า {_rp_age_h:.0f} ชั่วโมง — ชีท REPLAN อาจไม่ตรงกับรอบนี้ "
                  f"(รัน Replan.py ใหม่ก่อน Planning ถ้าต้องการให้ตรง)")
        print(f"🔁 แปะผลรอบ replan: {', '.join(f'{k}({len(v)} แถว)' for k, v in _replan_sheets.items())}")
    except Exception as _e_rp:
        print(f"⚠️ อ่าน {REPLAN_RESULT_FILE.name} ไม่ได้ → ไม่มีชีท REPLAN ในไฟล์แผน: {_e_rp}")
        _replan_sheets = {}

# งานไม่มี FG ที่วางตาม RDD: ติดป้าย DUE_BASIS='RDD' ให้รู้ว่าช่อง FG ของแถวนั้นมาจาก RDD
_mark_rdd_fg(plan_df, "ชีท PLAN")
_mark_rdd_fg(_unplanned_df, "ชีท UNPLANNED")

# ตาข่ายกันพลาดด่านสุดท้าย: ไม่ว่าแถวถูกสร้างจากทางไหน (มีหลายสิบจุด) เลขใบสั่งที่เขียน
# ลงไฟล์ต้องเป็นข้อความสะอาดเสมอ — ไม่มี "721915.0" / "nan" ให้ join พลาดต่อ
for _out_df_norm in (plan_df, setup_tracking_df, _unplanned_df, *_replan_sheets.values()):
    if isinstance(_out_df_norm, pd.DataFrame) and "SC_SO_NO" in _out_df_norm.columns:
        _out_df_norm["SC_SO_NO"] = _out_df_norm["SC_SO_NO"].map(ColorOpen.so_text)

# คีย์ YW ภายใน → รูปไฟล์เดิม (สัปดาห์ + ปี) — ต้องทำหลังสุด หลังทุกขั้นที่คำนวณด้วยสัปดาห์เสร็จแล้ว
_split_yw_out(plan_df, _PLAN_YW_COLS)
_split_yw_out(remaining_df, [("WEEK", "YEAR")])
_split_yw_out(setup_tracking_df, [("PLAN_WEEK", "PLAN_YEAR")])
_split_yw_out(_cylinder_change_df, [("WEEK", "YEAR")])
for _rp_df_s in _replan_sheets.values():
    _split_yw_out(_rp_df_s, _PLAN_YW_COLS)
plan_df = _kp_no_last(plan_df)
_replan_sheets = {_k: _kp_no_last(_v) for _k, _v in _replan_sheets.items()}

# เครื่อง booking ที่ถูกคืนเข้าพูลระหว่างวาง (ถอยงาน booking ออกให้งานสี) — ไฟล์ booking_final
# ที่หน้าเว็บอ่านไม่รู้เรื่องนี้ ถ้าไม่ส่งต่อ Gantt จะหักเครื่องชุดเดียวกัน 2 รอบ (booking + แผน)
# แล้วขึ้น "ไม่ว่าง (เกิน N)" ทั้งที่เครื่องว่างจริง → plan_view.ava_by_week บวกกลับจากชีทนี้
_released_df = pd.DataFrame(
    [
        {"WEEK": _rw, "YEAR": None, "MC_CAT": _rc, "GUAGE": _rg, "MC_POOL": _rp, "RELEASED": int(_rn)}
        for (_rw, _rc, _rg, _rp), _rn in sorted(_RELEASED_BOOKING_MC.items())
        if int(_rn) > 0
    ],
    columns=["WEEK", "YEAR", "MC_CAT", "GUAGE", "MC_POOL", "RELEASED"],
)
_split_yw_out(_released_df, [("WEEK", "YEAR")])
if not _released_df.empty:
    print(f"↩️  เครื่อง booking ที่คืนเข้าพูลระหว่างวาง: {len(_released_df)} ช่อง "
          f"รวม {int(_released_df['RELEASED'].sum())} เครื่อง → ชีท MC_RELEASED (หน้าเว็บบวกกลับให้)")

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as _writer:
    plan_df.to_excel(_writer, sheet_name="PLAN", index=False)
    remaining_df.to_excel(_writer, sheet_name="REMAINING_JOBS", index=False)
    setup_tracking_df.to_excel(_writer, sheet_name="SETUP_TRACKING", index=False)
    _unplanned_df.to_excel(_writer, sheet_name="UNPLANNED", index=False)
    _cylinder_change_df.to_excel(_writer, sheet_name="CYLINDER_CHANGE", index=False)
    _released_df.to_excel(_writer, sheet_name="MC_RELEASED", index=False)
    for _rp_name, _rp_df in _replan_sheets.items():
        _rp_df.to_excel(_writer, sheet_name=_rp_name, index=False)

# ใส่สีให้ PLAN sheet: เหลือง=CYLINDER_CHANGE, แดง=S9_ROUTING
_need_color = _cyl_trigger_keys or (
    "S9_ROUTING" in plan_df.columns and plan_df["S9_ROUTING"].any()
)
if _need_color:
    from openpyxl import load_workbook
    from openpyxl.styles import PatternFill
    _yellow = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    _red_fill = PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid")
    _wb = load_workbook(OUTPUT_FILE)
    _ws = _wb["PLAN"]
    _hdr = {cell.value: cell.column for cell in _ws[1]}
    _cyl_col_idx = _hdr.get("CYLINDER_CHANGE")
    _s9_col_idx = _hdr.get("S9_ROUTING")
    for _row in _ws.iter_rows(min_row=2, max_row=_ws.max_row):
        _is_s9_row = _s9_col_idx and str(_row[_s9_col_idx - 1].value).strip() in ("True", "TRUE", "1")
        _is_cyl_row = _cyl_col_idx and str(_row[_cyl_col_idx - 1].value).strip() == "Yes"
        if _is_s9_row:
            for _cell in _row:
                _cell.fill = _red_fill
        elif _is_cyl_row:
            for _cell in _row:
                _cell.fill = _yellow
    _wb.save(OUTPUT_FILE)
# (ชีท PIVOT_PLAN — Excel PivotTable ผ่าน win32com — เอาออกแล้วโดยตั้งใจ 2026-09-11:
#  ดูสรุปผ่านหน้า Pivot ของเว็บ (PlanPivot.jsx) แทน)
# ตรวจความสะอาดของคีย์สัปดาห์: ทุกแหล่งควรถูกแปลงเป็น YW ตอนโหลดแล้ว
# ถ้ายังมีจุดส่ง "เลขสัปดาห์เปล่า" เข้า week_index / week_pos จะเดาปีเอา (ถูกแค่ในกรอบ ±ครึ่งปี)
if _BARE_WEEK_HITS:
    print(f"⚠️ [BARE WEEK] ยังมีจุดที่ส่งเลขสัปดาห์เปล่า {sum(_BARE_WEEK_HITS.values())} ครั้ง — บรรทัด: "
          + ", ".join(f"{_ln}×{_n}" for _ln, _n in sorted(_BARE_WEEK_HITS.items(), key=lambda kv: -kv[1])[:30]))
else:
    print("✅ [BARE WEEK] ไม่มีจุดไหนส่งเลขสัปดาห์เปล่า — คีย์สัปดาห์เป็น YW ทั้งหมด")

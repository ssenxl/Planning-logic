"""
plan_view.py — อ่าน/เขียนไฟล์แผนผลิต "ล่าสุด" (production_plan_*.xlsx) สำหรับหน้า "แผนผลิต"
- หาไฟล์ production_plan_*.xlsx ที่ mtime ใหม่สุดใน OUTPUT_DIR
- อ่านรายชีท → คืน columns + rows "ครบทุกแถว" (ไม่ตัด เพราะต้องบันทึกกลับได้)
- เขียนกลับ: สำรองไฟล์เดิมเป็น .bak ก่อน แล้วแก้เฉพาะชีทเป้าหมาย (ชีทอื่นคงเดิม)
"""
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from openpyxl import load_workbook

import config
import core_registry
import cyl_approve
import livesync

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน — ไฟล์อยู่ราก repo คู่กับ Planning.py)

_INT_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")
_FLOAT_RE = re.compile(r"^-?(0|[1-9][0-9]*|0?)\.[0-9]+$")


# ── ค่าจากหน้า "ตั้งค่าระบบ" ที่หน้านี้ต้องคิดเองให้ตรงกับ engine ────────────────
# webapp เป็น process ที่รันค้าง — ถ้าอ่านค่าตอน import จะค้างค่าเก่าจนกว่าจะ restart container
# จึงอ่านใหม่เมื่อไฟล์ config เปลี่ยนเวลา (ครอบคลุมทั้งคนกดบันทึกบนเว็บและ pipeline ที่เป็น process แยก)
_CFG_CACHE = {"mtime": None, "val": None}


def _cfg() -> dict:
    try:
        mtime = plan_config.CONFIG_FILE.stat().st_mtime
    except OSError:
        mtime = 0
    if _CFG_CACHE["mtime"] != mtime or _CFG_CACHE["val"] is None:
        plan_config.reload()
        _CFG_CACHE["val"] = {
            # ชนิดใบสั่ง → สัปดาห์ที่หักจากกำหนดส่ง (เทียบตรงตัวพิมพ์เหมือน Planning)
            "fob": {str(r.get("fob_type", "")).strip(): int(r.get("weeks") or 0)
                    for r in (plan_config.get("KP_FOB_TARGET_OFFSET") or [])
                    if str(r.get("fob_type", "")).strip()},
            "default_offset": int(plan_config.get("KP_TARGET_OFFSET_DEFAULT")),
            "short_offset": int(plan_config.get("KP_TARGET_OFFSET_SHORT")),
            "d30_gauges": {_gkey(g) for g in (plan_config.get("KP_DOUBLE30_SHORT_GAUGES") or [])},
            "core_customers": {str(c).strip()
                               for c in plan_config.get_list("KP_CORE_CUSTOMERS") if str(c).strip()},
            # วันทำงานของผู้รับจ้างทอ + สัปดาห์ต้นที่แก้แผนไม่ได้ (ชุดเดียวกับ engine)
            "s9_work_days": float(plan_config.get("KP_S9_WORK_DAYS")),
            "freeze_weeks": int(plan_config.get("KP_FREEZE_WEEKS")),
        }
        _CFG_CACHE["mtime"] = mtime
    return _CFG_CACHE["val"]


# ชีทแผนหลักในไฟล์ production_plan_*.xlsx
# (เดิมมี PLAN_NO_S9 เป็นชีทซ้ำจาก pass ที่ปิด S9 — เลิกใช้ S9 fallback แล้วจึงเหลือชีทเดียว)
_MAIN_SHEET = "PLAN"

# แผน replan = วางใหม่โดยเอางาน booking ตั้งแต่สัปดาห์ปลดล็อก มารวมกับ SC (ดู Replan.py)
# "โลกของเครื่อง" ของแผน replan ต่างจากแผน SC ล้วน:
#   แผน SC ล้วน : เครื่องที่ booking จองไว้ถูกหักออกจาก TOTAL_MC_REMAIN แล้ว (งาน booking ไม่ใช่แถวในแผน)
#   แผน replan  : เครื่องของสัปดาห์ที่ปลดล็อกถูกคืนเป็นว่าง เพราะงานนั้นกลายเป็นแถวในแผนแทน
# → ทุก endpoint ที่ให้ตัวเลขเครื่อง/โควตา setup ต้องรู้ว่าชีทที่เปิดอยู่เป็นโลกไหน ไม่งั้นหักเครื่องซ้ำสองรอบ
#
# ไฟล์มี 2 รุ่น (ตัดสินด้วย _is_legacy_file):
#   รุ่นใหม่ (ตั้งแต่ 2026-09-11) : Replan.py เขียนเอง มีแผนเดียวชื่อชีท PLAN = แผน replan
#                                (เลิกรัน Planning รอบหลักแล้ว)
#   รุ่นเก่า                     : Planning รอบหลักเขียน — PLAN = แผน SC ล้วน, REPLAN = แผน replan
_REPLAN_SHEET = "REPLAN"   # ชื่อชีทแผน replan ในไฟล์รุ่นเก่า
_REPLAN_BOOKING_FILE = "booking_final_replan.xlsx"   # capacity คู่ขนาน (Replan.py สร้าง)
_REPLAN_RESULT_FILE = "replan_result.xlsx"           # ผล Planning รอบ replan (SETUP_TRACKING/REMAINING_JOBS)

# หน้าต่างสัปดาห์รอบ "สัปดาห์ปัจจุบัน" ที่ใช้เดาปีของเลขสัปดาห์เปล่า (ดู week_year_map)
_YEAR_WINDOW = 26


def _sheet_key(sheet) -> str:
    return str(sheet or "").strip().upper().replace(" ", "_")


_FORMAT_CACHE: dict = {"key": None, "legacy": False}


def _is_legacy_file(p) -> bool:
    """ไฟล์แผนรุ่นเก่า = มีชีท REPLAN แยกจาก PLAN (ชีท PLAN ในไฟล์นั้นคือแผน SC ล้วน)
    เปิดดูแค่รายชื่อชีท + cache ตาม mtime — ถูกเรียกแทบทุก endpoint ของหน้าแผน"""
    if p is None:
        return False
    try:
        ck = (str(p), p.stat().st_mtime)
    except OSError:
        return False
    if _FORMAT_CACHE["key"] == ck:
        return _FORMAT_CACHE["legacy"]
    try:
        wb = load_workbook(p, read_only=True)
        legacy = _REPLAN_SHEET in wb.sheetnames
        wb.close()
    except Exception:
        legacy = False
    _FORMAT_CACHE["key"], _FORMAT_CACHE["legacy"] = ck, legacy
    return legacy


def _is_replan(sheet) -> bool:
    """ชีทนี้ใช้ "โลกของเครื่อง" แบบแผน replan หรือไม่
    - REPLAN (มีแต่ในไฟล์รุ่นเก่า)       → ใช่
    - PLAN / ว่าง (= ชีทเริ่มต้น PLAN) → ใช่ ถ้าไฟล์ล่าสุดเป็นรุ่นใหม่ ; รุ่นเก่า = แผน SC ล้วน → ไม่ใช่
    - ชีทอื่น (UNPLANNED ฯลฯ)          → ไม่ใช่"""
    k = _sheet_key(sheet)
    if k == _REPLAN_SHEET:
        return True
    if k in ("", _MAIN_SHEET):
        return not _is_legacy_file(latest_path())
    return False


def _replan_file(name: str) -> Path | None:
    p = config.OUTPUT_DIR / name
    return p if p.exists() else None


def _display_sheets(sheets) -> list:
    """รายชื่อชีทสำหรับ dropdown — เอา PLAN ขึ้นก่อน"""
    out = [s for s in sheets if s == _MAIN_SHEET]
    out += [s for s in sheets if s != _MAIN_SHEET]
    return out


def latest_path() -> Path | None:
    """ไฟล์ production_plan_*.xlsx ที่แก้ไขล่าสุด (None = ยังไม่มี)"""
    d = config.OUTPUT_DIR
    if not d.exists():
        return None
    files = sorted(d.glob("production_plan_*.xlsx"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


# production_plan_<วัน>-<เดือน>-<ปี พ.ศ.>_<HHMM>.xlsx — วัน/เดือนไม่เติมศูนย์ (ดู Replan.py:1135)
# ไฟล์รุ่นเก่าบางใบไม่มีส่วน _HHMM → ไม่เข้าแพตเทิร์นนี้ ถอยไปใช้ mtime ตามเดิม
# (ใส่แค่วันแล้วโชว์ 00:00 น. อ่านแล้วเข้าใจผิดว่ารันตอนเที่ยงคืน)
_PLAN_NAME_RE = re.compile(r"^production_plan_(\d{1,2})-(\d{1,2})-(\d{4})_(\d{2})(\d{2})", re.I)


def _run_at(p: Path) -> float | None:
    """เวลาที่ "รันแผนรอบนี้" — อ่านจากชื่อไฟล์ ไม่ใช่ mtime

    ต้องแยกจาก mtime เพราะตั้งแต่เซฟอัตโนมัติ การลาก Gantt เขียนทับไฟล์เดิมทุกครั้ง
    → mtime = เวลาที่แก้ล่าสุด ซึ่งไม่ใช่สิ่งที่หัวจอหมายถึง: แผนใบหนึ่งคือผลของการรัน
    รอบหนึ่ง ส่วนการแก้คือการแก้ "ของรอบนั้น" เวลารอบรันจึงต้องนิ่งจนกว่าจะรันใหม่
    ชื่อไฟล์อ่านไม่ออก (ไฟล์เปลี่ยนชื่อ/รุ่นเก่า) → None ให้ผู้เรียกถอยไปใช้ mtime
    """
    m = _PLAN_NAME_RE.match(p.name)
    if not m:
        return None
    d, mo, by, hh, mi = m.groups()
    try:
        return datetime(int(by) - 543, int(mo), int(d), int(hh), int(mi)).timestamp()
    except ValueError:      # เช่น 31-2-2569 หรือปีที่ลบ 543 แล้วไม่เข้าช่วง
        return None


def latest_meta() -> dict:
    """ข้อมูลไฟล์แผนล่าสุด + รายชื่อชีท (ให้ frontend โชว์ header)

    mtime = เวลาที่ไฟล์ถูกเขียนล่าสุด (ใช้กันแคชตอนดาวน์โหลด)
    run_at = เวลาที่รันแผนรอบนี้ (หัวจอโชว์ตัวนี้ — การแก้แผนไม่ทำให้ขยับ)
    """
    p = latest_path()
    if p is None:
        return {"exists": False, "name": None, "size": 0, "mtime": None,
                "run_at": None, "sheets": []}
    st = p.stat()
    run_at = _run_at(p)
    try:
        wb = load_workbook(p, read_only=True, data_only=True)
        sheets = _display_sheets(wb.sheetnames)
        wb.close()
    except Exception as e:
        return {"exists": True, "name": p.name, "size": st.st_size,
                "mtime": st.st_mtime, "run_at": run_at, "sheets": [], "error": str(e)}
    return {"exists": True, "name": p.name, "size": st.st_size,
            "mtime": st.st_mtime, "run_at": run_at, "sheets": sheets}


def _round2(v):
    """ปัดลง (ตัดทศนิยมทิ้ง) เหลือ 2 ตำแหน่งสำหรับแสดงผล (จำนวนเต็ม/ชนิดอื่นคงเดิม)
    ใช้ Decimal จาก str เพื่อเลี่ยง error จาก float เช่น 1.13*100 = 112.999..."""
    if isinstance(v, float) and not v.is_integer():
        from decimal import Decimal, ROUND_DOWN
        return float(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_DOWN))
    return v


def _cell(v):
    """ค่าเซลล์สำหรับส่งขึ้นหน้าเว็บ
    - วันที่/เวลา → 'YYYY-MM-DD' (ตัด 00:00:00 ทิ้ง) หรือ 'YYYY-MM-DD HH:MM' ถ้ามีเวลาจริง
      (ไม่งั้น FastAPI จะแปลง datetime เป็น ISO '2026-08-26T00:00:00' โชว์เต็มในตาราง)
    - ค่าอื่น → _round2 ตามเดิม"""
    if isinstance(v, datetime):
        if (v.hour, v.minute, v.second) == (0, 0, 0):
            return v.strftime("%Y-%m-%d")
        return v.strftime("%Y-%m-%d %H:%M")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    return _round2(v)


def read_grid(sheet: str = None) -> dict:
    """อ่านชีทของไฟล์แผนล่าสุด → columns + rows (แถวแรก = header)
    data_only=True: คืนค่าที่คำนวณแล้วของเซลล์สูตร"""
    p = latest_path()
    if p is None:
        raise FileNotFoundError("ยังไม่มีไฟล์แผนผลิต — กรุณากดรันแผนก่อน")
    wb = load_workbook(p, read_only=True, data_only=True)
    sheets = _display_sheets(wb.sheetnames)
    mtime = p.stat().st_mtime
    if not sheets:
        wb.close()
        return {"name": p.name, "mtime": mtime, "sheet": None,
                "sheets": [], "columns": [], "rows": []}
    # ค่าเริ่มต้น = ชีทแรกใน dropdown (= 'PLAN')
    if sheet is None or sheet not in sheets:
        sheet = sheets[0]
    ws = wb[sheet]
    rows = []
    for r in ws.iter_rows(values_only=True):
        rows.append(["" if v is None else _cell(v) for v in r])
    wb.close()

    # replan = ชีทนี้เป็นแผน replan (ไฟล์รุ่นใหม่ = PLAN, รุ่นเก่า = REPLAN) — หน้าเว็บใช้ตัดสิน
    # ปุ่มแจ้งเมล / ตารางเทียบ booking เดิม / โหลด booking เต็ม แทนการเช็คชื่อชีทเอง
    replan = _is_replan(sheet)
    if not rows:
        return {"name": p.name, "mtime": mtime, "sheet": sheet, "replan": replan,
                "sheets": sheets, "columns": [], "rows": []}

    header = [str(c) if c != "" else f"Col{i+1}" for i, c in enumerate(rows[0])]
    width = len(header)
    data = [list(row)[:width] + [""] * (width - len(row)) for row in rows[1:]]
    return {"name": p.name, "mtime": mtime, "sheet": sheet, "replan": replan,
            "sheets": sheets, "columns": header, "rows": data}


def _num(v):
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return None


def _f(v) -> float:
    """ตัวเลขจากเซลล์ Excel → float ; ว่าง/อ่านไม่ออก/NaN = 0.0

    ต้องใช้ตัวนี้แทน `_num(x) or 0` กับคอลัมน์ที่มีช่องว่างได้ — เซลล์ว่างใน pandas
    คืน NaN ซึ่ง `_num` ส่งกลับมาตรง ๆ และ NaN เป็น truthy → `or 0` ไม่ช่วย
    แล้ว int(NaN) จะ ValueError (เคยทำ /api/plan/booking-items พังทั้ง endpoint)"""
    n = _num(v)
    if n is None or n != n:          # n != n → NaN
        return 0.0
    return float(n)


def _wk_str(v):
    """แปลงค่า WEEK → string เลขสัปดาห์ (29.0 → '29')"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return str(int(v))
        except (ValueError, OverflowError):
            return None
    s = str(v).strip()
    return s or None


def _cur_plan_week(today: date | None = None) -> int:
    """เลขสัปดาห์ปัจจุบันตามนิยาม Fri–Thu (บวก 3 วันแล้วคิด ISO week) — ตรงกับ currentPlanWeek() ใน weekKeys.js"""
    d = (today or date.today()) + timedelta(days=3)
    return int(d.isocalendar()[1])


def week_key(week, year=None) -> str | None:
    """(เลขสัปดาห์, ปี) → คีย์คอลัมน์สัปดาห์ของหน้าเว็บ — ต้องให้ผลเหมือน weekKey() ใน weekKeys.js ทุกตัวอักษร

    pipeline คีย์สัปดาห์ด้วยปี+สัปดาห์แล้ว แผนยาวเกิน 1 ปีได้ ; หน้าเว็บคงเลขเปล่าไว้กับ
    "ครั้งที่ใกล้วันนี้สุด" ของเลขนั้น (±26 สัปดาห์ — เกณฑ์ isNextYearWeek) แล้วติดปีทุกครั้งที่ไกลกว่า:
      ปีก่อน → '45@2025' · ปีนี้ → '45' (อดีตไกลเกินครึ่งปี → '5@2026')
      ปีหน้าส่วนที่อยู่ในครึ่งปีข้างหน้า → '12' · ไกลกว่านั้น / ปีถัดไป → '45@2027'
    ไม่รู้ปี / W99 → เลขเปล่าเหมือนเดิม"""
    wk = _wk_str(week)
    if wk is None or "@" in wk:
        return wk
    try:
        n = int(float(wk))
    except (TypeError, ValueError):
        return wk
    y = _num(year)
    if y is None or y != y or y <= 0 or n == 99:
        return str(n)
    y = int(y)
    cy = date.today().year
    tag = f"{n}@{y}"
    if y < cy:
        return tag
    cur = _cur_plan_week()
    # ตรงกับ isNextYearWeek(n, cur) ใน weekKeys.js
    next_year = n < cur and (n + 52 - cur) < (cur - n)
    if y == cy:
        return tag if next_year else str(n)
    if y == cy + 1:
        return str(n) if next_year else tag
    return tag


def key_week_year(key) -> tuple:
    """คีย์คอลัมน์ → (สัปดาห์, ปี) — กลับด้านของ week_key (ตรงกับ keyToWeekYear ใน weekKeys.js)
    คีย์ติดปี = ปีตามป้าย ; เลขเปล่า = ปีของครั้งที่ใกล้วันนี้สุด ; อ่านไม่ออก = (None, None)"""
    s = _wk_str(key)
    if s is None:
        return None, None
    if "@" in s:
        w, _, y = s.partition("@")
        try:
            return int(float(w)), int(float(y))
        except (TypeError, ValueError):
            return None, None
    try:
        n = int(float(s))
    except (TypeError, ValueError):
        return None, None
    cur = _cur_plan_week()
    next_year = n != 99 and n < cur and (n + 52 - cur) < (cur - n)
    return n, date.today().year + (1 if next_year else 0)


def _classify_type(factory, cat) -> str:
    """จำแนกแถว PLAN เป็นประเภท job (ตรงกับ REMAINING_JOBS): OM / PHET_DOUBLE / PHET_SINGLE
    None = ไม่นับ (เช่น OUTSOURCE) — ต้องตรงกับ classifyType() ใน PlanGantt.jsx"""
    f = str(factory).strip().upper()
    if f.startswith("OM"):
        return "OM"
    if f == "PHET":
        return "PHET_DOUBLE" if "DOUBLE" in str(cat).upper() else "PHET_SINGLE"
    return None


def _is_outsource_setup(mc_group, typ=None) -> bool:
    """แถว SETUP_TRACKING นี้เป็นงานจ้างทอไหม — จ้างทอตั้งเครื่องที่ผู้รับจ้าง ไม่กินโควตาของโรงเรา

    ไฟล์แผนตั้งแต่ 20 ก.ย. 2569 เขียน TYPE = "OUTSOURCE" มาให้แล้ว (Planning._new_setup_rows)
    แต่ไฟล์เก่ากว่านั้นเขียน TYPE = PHET_DOUBLE (เพราะชื่อผู้รับจ้างหาใน MasterMC ไม่เจอ แล้ว
    _get_type_key_for_mc คืน default) → ต้องดู MC_GROUP ด้วย ไม่งั้นแถบโหลดของไฟล์เดิมยังโชว์เกิน
    เกณฑ์: COMKN (ชื่อเก่า) หรือชื่อผู้รับจ้างที่ไม่ใช่ ASCII (วันนิตติ้ง / รุ่งสยาม (POLY) / ดารินทร์)
    — เครื่องในบ้านทุกกลุ่มเป็นรหัส ASCII ล้วน (IIP, RAP98, SBP, …) จึงไม่โดนกรองผิด
    """
    if str(typ or "").strip().upper() == "OUTSOURCE":
        return True
    mc = str(mc_group or "").strip().upper()
    if not mc or mc == "NAN":
        return False
    return mc == "COMKN" or any(ord(ch) > 127 for ch in mc)


def _sits_on_booking_mc(row, ami: int, bki: int) -> bool:
    """แถวนี้ "นั่งบนเครื่องที่ booking จองไว้ครบแล้ว" ไหม (MC_BOOKING ≥ ACTUAL_MC > 0)

    เครื่องพวกนี้ booking ตั้งไปแล้ว → job ถูกนับในช่อง "เก่า" (SETUP_TRACKING แถว OLD) แล้ว
    ถ้านับ NEW_MC ของแถวซ้ำอีก แถบโหลด setup จะขึ้นเกินจริง (เจอ 23 ก.ย. 2569: แถว KP_KEEP
    ของ W41 ทำให้ job เกินจริง 8 job) — กติกาเดียวกับที่งานจ้างทอไม่กินโควตาของโรงเรา
    ต้องตรงกับ PlanGantt.planNewByWeekType เป๊ะ ๆ ไม่งั้นเลขตอนโหลดกับตอนลากไม่ตรงกัน
    ⚠️ ดูที่เครื่อง ไม่ใช่ PLAN_SOURCE — พอ user ลากงาน (core ที่เบรก ลากได้) mcRecalc จะ
    อัปเดต MC_BOOKING ให้เอง แล้ว job กลับมาถูกนับใหม่อัตโนมัติตามที่ควรเป็น
    """
    if ami < 0 or bki < 0 or max(ami, bki) >= len(row):
        return False
    a = _num(row[ami]) or 0
    b = _num(row[bki]) or 0
    return a > 0 and b >= a


def _plan_new_mc_by_type(sheet=None) -> dict:
    """ผลรวม NEW_MC (job = setup 1 เครื่อง) ต่อ (สัปดาห์×ประเภท) จากชีทแผนที่เปิดอยู่
    → { (week, TYPE): sum_new_mc }  ใช้แยกส่วน "job จาก booking" ออกจาก NEW_JOBS"""
    p = latest_path()
    if p is None:
        return {}
    try:
        wb = load_workbook(p, read_only=True, data_only=True)
    except Exception:
        return {}
    # ชื่อชีทจริงที่ต้องอ่าน (ไม่ใช่โลกของเครื่อง) — ไฟล์รุ่นใหม่แผน replan อยู่ในชีท PLAN
    _sheet = _REPLAN_SHEET if _sheet_key(sheet) == _REPLAN_SHEET else _MAIN_SHEET
    if _sheet not in wb.sheetnames:
        wb.close()
        return {}
    rows = list(wb[_sheet].iter_rows(values_only=True))
    wb.close()
    if not rows:
        return {}
    header = [str(c).strip().upper() if c is not None else "" for c in rows[0]]

    def col(name):
        return header.index(name) if name in header else -1
    wi, fi, cati, nmi = col("PLAN_WEEK"), col("FACTORY_TYPE"), col("CAT"), col("NEW_MC")
    if wi < 0 or fi < 0 or cati < 0 or nmi < 0:
        return {}
    iti, mgi, gi = col("ITEM_CODE"), col("MC_GROUP"), col("MC_GUAGE")
    yi = col("PLAN_YEAR")
    ami, bki = col("ACTUAL_MC"), col("MC_BOOKING")

    # ⚠️ item เดียวกัน เครื่อง+เกจเดียวกัน สัปดาห์เดียวกัน แต่คนละ SC = เครื่องชุดเดียวกัน
    # (Planning เขียน NEW_MC ค่าเดียวกันซ้ำทุกแถว) → ยุบด้วย max ก่อนรวม ให้ตรงกับ
    # SETUP_TRACKING และ PlanGantt.planNewByWeekType ไม่งั้น bookingNew ติดลบ/นับซ้ำ
    by_key: dict = {}
    for r in rows[1:]:
        if max(wi, fi, cati, nmi) >= len(r):
            continue
        wk = week_key(r[wi], r[yi] if 0 <= yi < len(r) else None)
        typ = _classify_type(r[fi], r[cati])
        if wk is None or typ is None:
            continue
        if _sits_on_booking_mc(r, ami, bki):
            continue
        nm = int(_num(r[nmi]) or 0)
        key = (
            wk, typ,
            str(r[iti]).strip().upper() if 0 <= iti < len(r) else "",
            str(r[mgi]).strip().upper() if 0 <= mgi < len(r) else "",
            _gkey(r[gi]) if 0 <= gi < len(r) else "",
        )
        by_key[key] = max(by_key.get(key, 0), nm)

    out: dict = {}
    for (wk, typ, _it, _mg, _g), nm in by_key.items():
        out[(wk, typ)] = out.get((wk, typ), 0) + nm
    return out


_LOAD_TYPES = {"OM", "PHET_DOUBLE", "PHET_SINGLE"}


def load_by_week(sheet=None) -> dict:
    """โหลดงานแยกตามประเภท (OM/PHET_DOUBLE/PHET_SINGLE) ต่อสัปดาห์
    → { week(str): { TYPE: {"old": int, "cap": number|None, "bookingNew": int} } }
    - old / new = "setup จริง" จากชีท SETUP_TRACKING (แยก PLAN_SOURCE OLD/NEW) — ตรงกับแผนจริง
      (ไม่ใช้ REMAINING_JOBS เพราะนับ job เกินจริง ไม่ตรงกับ SETUP_TRACKING)
    - cap มาจาก REMAINING_JOBS.CAPACITY (คงที่ 13/33/44)
    - bookingNew = new(SETUP_TRACKING) − ผลรวม NEW_MC ในแผน  → frontend คิด live:
      ใหม่ = bookingNew + ผลรวม NEW_MC ของแถวที่วางจริง (ตอนโหลดตรง setup จริง)

    sheet=REPLAN (ไฟล์รุ่นเก่า) → อ่าน SETUP_TRACKING/REMAINING_JOBS ของรอบ replan (replan_result.xlsx)
    ไม่ใช่ของแผนปกติ ไม่งั้นโควตา setup ที่โชว์เป็นของอีกแผนหนึ่งคนละใบ
    ไฟล์รุ่นใหม่: ชีทในไฟล์แผนเองก็คือของรอบ replan อยู่แล้ว → อ่านจากไฟล์แผนตรง ๆ"""
    p = latest_path()
    if p is None:
        return {}
    src = p
    if _sheet_key(sheet) == _REPLAN_SHEET:
        rp = _replan_file(_REPLAN_RESULT_FILE)
        if rp is not None:
            src = rp
    try:
        import pandas as pd
        xls = pd.ExcelFile(src)
    except Exception:
        return {}
    sheets = xls.sheet_names
    _rj_sheet = "REMAINING_JOBS"
    _st_sheet = "SETUP_TRACKING"

    # capacity (คงที่) จาก REMAINING_JOBS
    cap: dict = {}
    if _rj_sheet in sheets:
        try:
            rj = xls.parse(_rj_sheet)
        except Exception:
            rj = None
        if rj is not None and {"WEEK", "TYPE", "CAPACITY"} <= set(rj.columns):
            for _, r in rj.iterrows():
                wk = week_key(r["WEEK"], r.get("YEAR"))
                t = str(r["TYPE"]).strip().upper()
                if wk is None or t not in _LOAD_TYPES:
                    continue
                cap[(wk, t)] = _num(r["CAPACITY"])

    # old + new = setup จริงจาก SETUP_TRACKING (แยก PLAN_SOURCE)
    old: dict = {}
    new_base: dict = {}
    if _st_sheet in sheets:
        try:
            st = xls.parse(_st_sheet)
        except Exception:
            st = None
        need = {"PLAN_WEEK", "TYPE", "PLAN_SOURCE", "JOBS_DEDUCTED"}
        if st is not None and need <= set(st.columns):
            # ฝั่ง NEW: item+เครื่อง+เกจ+สัปดาห์เดียวกัน = เครื่องชุดเดียวกัน (คนละ SC) → ยุบด้วย max
            # ก่อนรวม ให้ตรงกับ _plan_new_mc_by_type ; ทำที่ฝั่งอ่านด้วยเพื่อให้ไฟล์แผนรุ่นเก่า
            # (ที่ชีทยังมีแถวซ้ำ) แสดงโควตาถูกต้องโดยไม่ต้องรันแผนใหม่
            new_by_key: dict = {}
            for _, r in st.iterrows():
                wk = week_key(r["PLAN_WEEK"], r.get("PLAN_YEAR"))
                t = str(r["TYPE"]).strip().upper()
                if wk is None or t not in _LOAD_TYPES:
                    continue
                if _is_outsource_setup(r.get("MC_GROUP"), t):
                    continue
                j = int(_num(r["JOBS_DEDUCTED"]) or 0)
                src = str(r["PLAN_SOURCE"]).strip().upper()
                if src == "OLD":
                    old[(wk, t)] = old.get((wk, t), 0) + j
                elif src == "NEW":
                    k = (
                        wk, t,
                        str(r.get("ITEM_CODE", "")).strip().upper(),
                        str(r.get("MC_GROUP", "")).strip().upper(),
                        _gkey(r.get("MC_GUAGE", "")),
                    )
                    new_by_key[k] = max(new_by_key.get(k, 0), j)
            for (wk, t, _it, _mg, _g), j in new_by_key.items():
                new_base[(wk, t)] = new_base.get((wk, t), 0) + j

    plan_new = _plan_new_mc_by_type(sheet)

    out: dict = {}
    for key in set(cap) | set(old) | set(new_base):
        wk, t = key
        # ติดลบไม่ได้: bookingNew = "job ที่ booking จองไว้แต่ไม่มีแถวบน Gantt" จึงต่ำสุดได้แค่ 0
        # ถ้า SETUP_TRACKING นับ NEW ได้น้อยกว่าแผน (เช่นไฟล์แผนที่แถว NEW ถูกตีเป็น OUTSOURCE
        # ทั้งหมดจากบั๊ก S9_SUPP=NaN ก่อน 21 ก.ย. 2569) ค่าลบจะไปหักล้าง NEW_MC ที่หน้าเว็บบวก
        # เข้ามาสด ๆ จนแถบโชว์ setup เป็น 0 — clamp ไว้ให้ไฟล์แผนที่ออกไปแล้วยังอ่านได้ถูก
        booking_new = max(0, int(new_base.get(key, 0)) - int(plan_new.get(key, 0)))
        out.setdefault(wk, {})[t] = {
            "old": int(old.get(key, 0)),
            "cap": cap.get(key),
            "bookingNew": booking_new,
        }
    return out


def setup_jobs_by_week(sheet=None) -> list:
    """รายการ job ที่ตั้งเครื่องใหม่ ต่อสัปดาห์ (ชีท SETUP_TRACKING ของไฟล์แผนล่าสุด)
    → [ {week, type, source, item, so, mcg, gauge, jobs, setup, carry, mc, days, qty} ]

    ใช้เปิดการ์ดรายละเอียดเมื่อคลิกแถบโหลด (Set up Job/Week) บน Gantt — แถบบอกแค่ยอดรวม
    ว่าใช้ไปกี่ job จากโควตา การ์ดต้องบอกได้ว่า job เหล่านั้นเป็น item ไหนบ้าง
    source = OLD (แผนเดิม/booking — ย้ายไม่ได้) / NEW (แผนรอบนี้)
    ยอดรวมของ source=OLD ตรงกับ old ใน load_by_week() เพราะอ่านชีทเดียวกัน
    sheet=REPLAN (ไฟล์รุ่นเก่า) → อ่านของรอบ replan (replan_result.xlsx) ให้ตรงกับแถวที่โชว์อยู่
    ไฟล์รุ่นใหม่: SETUP_TRACKING ในไฟล์แผนคือของรอบ replan อยู่แล้ว"""
    p = latest_path()
    if p is None:
        return []
    if _sheet_key(sheet) == _REPLAN_SHEET:
        rp = _replan_file(_REPLAN_RESULT_FILE)
        if rp is not None:
            p = rp
    try:
        import pandas as pd
        st = pd.read_excel(p, sheet_name="SETUP_TRACKING")
    except Exception:
        return []
    need = {"PLAN_WEEK", "TYPE", "PLAN_SOURCE", "ITEM_CODE", "JOBS_DEDUCTED"}
    if not need <= set(st.columns):
        return []

    def _c(name):
        return name if name in st.columns else None
    c_mcg, c_g = _c("MC_GROUP"), _c("MC_GUAGE")
    c_so, c_qty, c_days = _c("SC_SO_NO"), _c("KP_WEIGHT"), _c("SETUP_DAYS")
    c_setup, c_carry, c_mc = _c("SETUP_MC"), _c("CARRYOVER_MC"), _c("MC_THIS_WEEK")

    out = []
    for _, r in st.iterrows():
        wk = week_key(r["PLAN_WEEK"], r.get("PLAN_YEAR"))
        typ = str(r["TYPE"]).strip().upper()
        if wk is None or typ not in _LOAD_TYPES:
            continue
        # การ์ดรายละเอียดต้องตรงกับแถบโหลด → กรองจ้างทอด้วยเกณฑ์เดียวกับ load_by_week()
        if _is_outsource_setup(r[c_mcg] if c_mcg else "", typ):
            continue
        out.append({
            "week": wk,                      # คีย์คอลัมน์ (ติดปีเมื่อไกลเกิน 1 ปี — ดู week_key)
            "year": int(_f(r.get("PLAN_YEAR"))) or None,
            "type": typ,
            "source": str(r["PLAN_SOURCE"]).strip().upper(),
            "item": str(r["ITEM_CODE"]).strip(),
            "so": str(r[c_so]).strip() if c_so else "",
            "mcg": str(r[c_mcg]).strip() if c_mcg else "",
            "gauge": _gkey(r[c_g]) if c_g else "",
            "jobs": int(_num(r["JOBS_DEDUCTED"]) or 0),
            "setup": int(_num(r[c_setup]) or 0) if c_setup else 0,
            "carry": int(_num(r[c_carry]) or 0) if c_carry else 0,
            "mc": int(_num(r[c_mc]) or 0) if c_mc else 0,
            "days": _num(r[c_days]) or 0 if c_days else 0,
            "qty": round(float(_num(r[c_qty]) or 0), 2) if c_qty else 0.0,
        })
    return out


def _booking_ok(p: Path) -> bool:
    """ไฟล์ booking_final ใช้ได้ = เปิดเป็น xlsx ได้ + มีชีท SUMMARY_MC_REMAIN
    บาง run ของ AVA_MC ล้มเหลว เขียนไฟล์เปล่า ~2KB (ไม่ใช่ zip ด้วยซ้ำ) → ต้องข้าม
    ไม่งั้น _latest คว้าไฟล์เสียมา แล้ว ava/reserved ว่างทั้งที่ไฟล์ก่อนหน้าดีอยู่
    → เครื่องว่าง + คอลัมน์ 🔒 หายไปทั้งหน้า"""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(p, read_only=True)
        try:
            return "SUMMARY_MC_REMAIN" in wb.sheetnames
        finally:
            wb.close()
    except Exception:
        return False


def _latest_booking_path(sheet=None, full=False) -> Path | None:
    """ไฟล์ booking_final ที่ "ใช้ได้" ใหม่สุด (ผลจาก AVA_MC — มีชีท SUMMARY_MC_REMAIN)
    ไล่จากใหม่→เก่า ข้ามไฟล์เสีย/เขียนไม่ครบ แล้วถอยไปไฟล์ดีที่ใหม่รองลงมา

    sheet=REPLAN → ใช้ booking_final_replan.xlsx (เครื่องของสัปดาห์ปลดล็อกถูกคืนเป็นว่างแล้ว)
    ไม่มีไฟล์นั้น (ยังไม่ได้รัน Replan) → ตกกลับไปไฟล์ปกติ ตัวเลขจะตึงกว่าจริงแต่ไม่พัง

    full=True → booking เต็มทุกสัปดาห์เสมอ (ข้ามไฟล์ replan)
    ❗ sheet=None ไม่ได้แปลว่า "booking เต็ม" : ไฟล์แผนรุ่นใหม่ถือว่าชีทว่าง = PLAN = replan
    (_is_replan) → ได้ไฟล์ที่ตัดเหลือแต่สัปดาห์ล็อก ; ต้องการของเต็มต้องขอด้วย full
    """
    d = config.OUTPUT_DIR
    if not d.exists():
        return None
    if not full and _is_replan(sheet):
        rp = _replan_file(_REPLAN_BOOKING_FILE)
        if rp is not None and _booking_ok(rp):
            return rp
    files = sorted(d.glob("booking_final_ready*.xlsx"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files:
        if _booking_ok(p):
            return p
    return None


def _gkey(g) -> str:
    """normalize เกจ: 22.0 / '22 ' → '22'"""
    try:
        return str(int(g))
    except (TypeError, ValueError):
        return str(g).strip()


# ── งานที่ Replan ยกไปวางใหม่แล้ว ────────────────────────────────────────────
# booking_final_replan.xlsx (Replan.py สร้าง) เก็บ DETAIL ไว้ "เฉพาะสัปดาห์ที่ล็อก"
# → คีย์ที่มีในไฟล์ booking เต็มแต่ไม่มีในไฟล์นี้ = งานที่ถูกยกออกไปวางใหม่แล้ว
#   ของเดิมที่สัปดาห์นั้นไม่เป็นความจริงอีกต่อไป แต่ชีท PLAN ยังอ่าน booking เต็มมา overlay
#   ทำให้ planner เห็นบล็อกเก่าค้างอยู่ทั้งที่งานย้ายไปแล้ว → ติดธงไว้ให้หน้าเว็บซ่อน/ทำจาง
_REPLAN_KEYS_CACHE: dict = {"key": None, "keys": None}


def _replan_locked_keys(booking_path):
    """set ของคีย์ (สัปดาห์, CAT, เกจ, เครื่อง, item) ที่ Replan **ไม่ได้** ยกไปวางใหม่

    None = ตัดสินไม่ได้ → ผู้เรียกต้องไม่ติดธงให้บล็อกไหนเลย (ซ่อนงานที่ยังอยู่จริงแย่กว่า
    โชว์ของเก่าเกินมา) ; เกิดเมื่อ
      · ยังไม่เคยรัน Replan — ไม่มีไฟล์
      · ไฟล์เก่ากว่า booking ที่กำลังใช้ = ผลรอบนั้นไม่ตรงกับ booking ชุดนี้แล้ว
      · อ่านไม่ได้ / ไม่มีคอลัมน์ที่ต้องใช้
    """
    rp = _replan_file(_REPLAN_BOOKING_FILE)
    if rp is None or booking_path is None:
        return None
    try:
        rp_m = rp.stat().st_mtime
        bk_m = Path(booking_path).stat().st_mtime
    except OSError:
        return None
    if rp_m < bk_m:
        return None
    ck = (str(rp), rp_m)
    if _REPLAN_KEYS_CACHE["key"] == ck:
        return _REPLAN_KEYS_CACHE["keys"]
    try:
        import pandas as pd
        df = pd.read_excel(rp, sheet_name="DETAIL")
    except Exception:
        return None
    need = {"WEEK", "CAT", "GUAGE", "MC_GROUP", "ITEM_CODE"}
    if not need <= set(df.columns):
        return None
    keys = set()
    _cols = list(need) + (["YEAR"] if "YEAR" in df.columns else [])
    for r in df[_cols].to_dict("records"):
        # สัปดาห์ในคีย์ = คีย์คอลัมน์ที่ติดปี (ดู week_key) — ตรงกับคีย์ของ booking_items_by_week
        wk = week_key(r["WEEK"], r.get("YEAR"))
        if wk is None:
            continue
        # ต้อง normalize ชุดเดียวกับคีย์ใน booking_items_by_week เป๊ะ ไม่งั้นเทียบไม่ติด
        # แล้วจะติดธง "replan แล้ว" ให้ทุกบล็อก = ซ่อนแผนเก่าทั้งกระดาน
        keys.add((wk, str(r["CAT"]).strip(), _gkey(r["GUAGE"]),
                  str(r["MC_GROUP"]).strip(), str(r["ITEM_CODE"]).strip()))
    _REPLAN_KEYS_CACHE["key"], _REPLAN_KEYS_CACHE["keys"] = ck, keys
    return keys


# ── TARGET_KNIT ของงาน booking (แผนเก่า) ────────────────────────────────────
# กฎหักสัปดาห์ต้องตรงกับ target_offset_weeks() ใน Planning.py:
#   1. FOB_TYPE ในตาราง → ใช้ค่านั้น (ชนะทุกกฎ)
#   2. DOUBLE-30 + เกจในรายการ             → offset สั้น
#   3. SINGLE* + ด้ายเส้นเดียว (ไม่มี '+')  → offset สั้น
#   4. อื่น ๆ                               → offset ทั่วไป
# ทั้งสี่ค่าอ่านจากหน้า "ตั้งค่าระบบ" ชุดเดียวกับ Planning.py แล้ว (ไม่ต้องแก้สองที่อีก)
# booking ไม่ได้ผ่าน Planning จึงยังต้องคิดกฎเดียวกันซ้ำที่นี่
def _target_offset_weeks(fob_type, cat, gauge, yarn_used) -> int:
    cfg = _cfg()
    fob = cfg["fob"].get(str(fob_type or "").strip())
    if fob is not None:
        return fob
    c = str(cat or "").strip().upper().replace("SINGEL", "SINGLE")
    if c.startswith("DOUBLE-30") and _gkey(gauge) in cfg["d30_gauges"]:
        return cfg["short_offset"]
    if c.startswith("SINGLE"):
        y = str(yarn_used or "").strip()
        if y and y.upper() not in ("NAN", "NONE") and "+" not in y:
            return cfg["short_offset"]
    return cfg["default_offset"]


def _week_minus(year: int, week: int, weeks_back: int):
    """ถอย N สัปดาห์จาก (ปี, สัปดาห์) แบบ ISO → คืน (เลขสัปดาห์เปล่า, ปีของสัปดาห์นั้น)
    ใช้วันพฤหัสของสัปดาห์เป็นหมุด → ข้ามปี 52/53 สัปดาห์ได้ถูกต้องโดยไม่ต้องพึ่ง Calendar.xlsx

    ต้องคืนปีมาด้วย เพราะเดดไลน์ที่ถอยข้ามปี (FG W2/2027 → target W50/2026) ถ้าเหลือแต่
    เลขสัปดาห์เปล่า หน้าเว็บจะเทียบ "50 > 2" แล้วตัดสินผิดว่างานที่วางไว้ปลายปีนี้ไม่ทัน"""
    try:
        anchor = date.fromisocalendar(int(year), int(week), 4) - timedelta(weeks=weeks_back)
        iso = anchor.isocalendar()
        return int(iso[1]), int(iso[0])
    except (ValueError, TypeError, OverflowError):
        return None, None


def _fg_target_week(row, has_fg: bool, has_fob: bool):
    """(fg_week เปล่า, target_knit เปล่า, ปีของ target) ของแถว booking
    → (None, None, None) ถ้าไม่มี FG week"""
    if not has_fg:
        return None, None, None
    # ใช้ _f: แถวที่ไม่มี FG week คืน NaN ซึ่ง truthy → int(NaN) จะ ValueError
    y, w = _f(row.get("FG_YEAR")), _f(row.get("FG_WEEK"))
    if not y or not w:
        return None, None, None
    y = int(y)
    if y > 2400:            # เผื่อ view ส่งเป็น พ.ศ. (2569) → แปลงเป็น ค.ศ.
        y -= 543
    off = _target_offset_weeks(
        row.get("FOB_TYPE") if has_fob else "",
        row.get("CAT"), row.get("GUAGE"),
        row.get("YARN_USED") or row.get("YARN-USED"),
    )
    _tk, _tky = _week_minus(y, int(w), off)
    return int(w), _tk, _tky


def booking_mc_by_item_week(sheet=None) -> dict:
    """เครื่องที่ booking ทอไอเทมนั้นอยู่แล้ว ต่อ (สัปดาห์ × ITEM|MC_GROUP|GUAGE)
    → { week(str): { "ITEM|MC_GROUP|GAUGE": int } }

    ใช้ให้ Gantt แยกออกว่าเครื่องของแถวแผนเป็น "เครื่อง booking" (อยู่กับสัปดาห์นั้น
    ไม่ย้ายตามงานที่ลาก และถูกหักจาก TOTAL_MC_REMAIN ไปแล้ว) หรือเป็นเครื่องที่แผน
    จองเพิ่มจากพูล (ต้องหักออกจากเครื่องว่าง และย้ายตามงาน)

    sheet=REPLAN → ใช้ DETAIL ของ capacity คู่ขนาน = เหลือแต่สัปดาห์ที่ล็อก
    (สัปดาห์ปลดล็อกไม่มีเครื่อง booking แล้ว เพราะงานนั้นกลายเป็นแถวแผนที่ลากได้)"""
    p = _latest_booking_path(sheet)
    if p is None:
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="DETAIL")
    except Exception:
        return {}
    if not {"ITEM_CODE", "MC_GROUP", "GUAGE", "WEEK", "MC_USE_CEIL"} <= set(df.columns):
        return {}
    out: dict = {}
    for _, r in df.iterrows():
        wk = week_key(r["WEEK"], r.get("YEAR"))   # สัปดาห์ต่างปีต้องไม่รวมกัน
        if wk is None:
            continue
        mc = int(_num(r["MC_USE_CEIL"]) or 0)
        if mc <= 0:
            continue
        key = "{}|{}|{}".format(
            str(r["ITEM_CODE"]).strip().upper(),
            str(r["MC_GROUP"]).strip().upper(),
            _gkey(r["GUAGE"]),
        )
        slot = out.setdefault(wk, {})
        slot[key] = slot.get(key, 0) + mc
    return out


_PO_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _po_first(v) -> str:
    """PO_IN_DATE ของชีท DETAIL เก็บเป็นข้อความ 'dd/mm/yyyy' ต่อกันด้วยจุลภาค
    (1 แถวยุบมาจากหลาย PO) → คืนวันที่ "เร็วสุด" ในรูป 'YYYY-MM-DD'

    รับทั้ง ค.ศ. และ พ.ศ. (>2400 → ลบ 543) ; อ่านไม่ออก/ว่าง → ''"""
    s = str(v).strip()
    if not s or s.lower() in ("nat", "nan", "none"):
        return ""
    best = ""
    for part in s.split(","):
        m = _PO_DATE_RE.match(part.strip())
        if not m:
            continue
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y > 2400:
            y -= 543
        iso = f"{y:04d}-{mo:02d}-{d:02d}"
        if not best or iso < best:
            best = iso
    return best


def _po_last(v) -> str:
    """เหมือน _po_first แต่คืนวัน "หลังสุด" = วันที่ด้ายเข้าครบทุกล็อตของแถวนั้น

    ทอได้จริงต้องรอด้ายครบ ไม่ใช่ล็อตแรก → Change Cylinder ยึดวันนี้ในการเลือกสัปดาห์
    ที่จะเปลี่ยนเครื่องรอไว้"""
    s = str(v).strip()
    if not s or s.lower() in ("nat", "nan", "none"):
        return ""
    best = ""
    for part in s.split(","):
        m = _PO_DATE_RE.match(part.strip())
        if not m:
            continue
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y > 2400:
            y -= 543
        iso = f"{y:04d}-{mo:02d}-{d:02d}"
        if iso > best:
            best = iso
    return best


def _order_type(fr_code) -> str:
    """FR_CODE = 'S{SC_NO}-{ORDER_TYPE}' (เช่น 'S720733-SC-ORDERS') → คืน ORDER_TYPE
    เป็นแหล่งเดียวใน booking ที่มีประเภทออร์เดอร์

    AVA_MC ยุบหลาย SO มาไว้ช่องเดียวคั่นด้วย ' | ' (comma ใช้ไม่ได้ ชื่อลูกค้ามีจุลภาคในตัว)
    — รับทั้งสองแบบเผื่อไฟล์ booking ที่สร้างด้วยโค้ดรุ่นก่อน"""
    out = []
    for part in re.split(r"[|,]", str(fr_code or "")):
        p = part.strip()
        if not p or p.lower() == "nan":
            continue
        t = p.split("-", 1)[1].strip() if "-" in p else ""
        if t and t not in out:
            out.append(t)
    return ", ".join(out)


def _wk_of_yyyyww(v):
    """RDD_WEEK / FG_WEEK เก็บเป็น YYYYWW (เช่น 202629) → คืนเลขสัปดาห์เปล่า (29)
    ทั้ง pipeline ใช้ bare week ไม่มีปี ; ค่าที่เป็นเลขสัปดาห์อยู่แล้ว (<=53) คืนตามเดิม"""
    n = int(_f(v))
    if not n:
        return 0
    return n % 100 if n > 100 else n


def booking_items_by_week(sheet=None, path=None, full=False) -> list:
    """item ทั้งหมดจาก booking (ชีท DETAIL ของ booking_final ล่าสุด) — "แผนเก่า" ที่ commit แล้ว
    → [ {week, cat, gauge, mc_group, item, mc, setup, carry, qty, so, material, ...} ]

    setup (_mc_increase) = เครื่องที่ต้องตั้งใหม่, carry (_prev_mc_use_ceil) = เครื่องอุ่นที่ยกมา
    ทั้งคู่มาจากลูกโซ่ carry/setup ของ AVA_MC และรวมกัน = mc (MC_USE_CEIL) เสมอ

    ฟิลด์ที่เหลือมีไว้ให้การ์ดรายละเอียดบน Gantt (คลิกบล็อก History) อ่านได้เท่ากับการ์ด
    ของงานแผน — ไฟล์ booking ที่รันก่อนเพิ่มคอลัมน์จะไม่มี จึงเช็คทีละคอลัมน์แล้วส่ง ''/0
    แทน (หน้าเว็บซ่อนบรรทัดที่ว่างเอง) ไม่ทำให้ API พังทั้งก้อน

    หน้าเว็บ overlay รายการนี้เป็นบล็อกอ่านอย่างเดียวบน Gantt (map เข้าแถว CAT|เกจ|เครื่อง
    และช่องสัปดาห์เดียวกับแผน) เพื่อให้ planner เห็นว่าเครื่องไหน/สัปดาห์ไหนมีงาน booking อยู่แล้ว
    รวมยอดต่อ (สัปดาห์×CAT×เกจ×เครื่อง×item) — DETAIL แตกได้หลายแถวต่อ item เดียวกัน

    sheet=REPLAN → โชว์บล็อกเก่าเฉพาะสัปดาห์ที่ล็อก ส่วนสัปดาห์ที่ปลดล็อกไปเป็นแถวแผนแล้ว
    (ถ้ายังใช้ไฟล์ปกติจะเห็นทั้งบล็อกเก่าและแถวแผนของงานเดียวกันซ้อนกัน = งานเดียวโผล่สองที่)

    replanned = 1 → บล็อกนี้ถูก Replan ยกไปวางใหม่ที่อื่นแล้ว (ชีท PLAN ที่อ่าน booking เต็ม
    ยังส่งมาด้วย เพราะเลขเครื่องฝั่ง PLAN ยังคิดจาก booking เต็มอยู่) — หน้าเว็บซ่อนไว้เป็น
    ค่าเริ่มต้น เปิดดูได้ด้วยปุ่ม ♻ ; 0 = ยังอยู่ที่เดิมจริง หรือยังไม่ได้รัน Replan

    path = อ่านไฟล์ booking ที่ระบุแทนไฟล์ล่าสุด (ข้าม sheet) — ให้ plan_history เก็บทะเบียน
    ประวัติจาก snapshot เก่าด้วย "ตัวแปลงชุดเดียวกัน" กับที่หน้าเว็บใช้ ไม่ต้องเขียน parser ซ้ำ

    full = booking เต็มทุกสัปดาห์ (ตารางเทียบในการ์ด item) — ดู _latest_booking_path"""
    p = Path(path) if path is not None else _latest_booking_path(sheet, full=full)
    if p is None:
        return []
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="DETAIL")
    except Exception:
        return []
    need = {"MC_GROUP", "GUAGE", "ITEM_CODE", "WEEK", "CAT", "MC_USE_CEIL"}
    if not need <= set(df.columns):
        return []
    # แถว core ที่ Replan แปะกลับเป็น "แถวแผน" แล้ว (ธง _CORE_PINNED ในไฟล์ capacity คู่ขนาน)
    # → ไม่ต้อง overlay เป็นบล็อกเก่าอีก ไม่งั้นงานเดียวโผล่สองใบซ้อนช่องเดียวกัน
    # ตัดที่นี่ที่เดียว — booking_mc_by_item_week() ยังต้องนับแถวพวกนี้ เพราะเป็นตัวบอกว่า
    # เครื่องของแถวแผนคือเครื่องที่ booking จองไว้แล้ว (ไม่ใช่เครื่องที่ขอเพิ่มจากพูล)
    _pin_col = next((c for c in df.columns if str(c).strip().upper() == "_CORE_PINNED"), None)
    if _pin_col is not None:
        import pandas as pd  # noqa: F811 — ใช้ซ้ำในสโคปนี้ (import อยู่ใน try ด้านบน)
        df = df[pd.to_numeric(df[_pin_col], errors="coerce").fillna(0) == 0]
        if df.empty:
            return []
    cols = set(df.columns)
    has_qty = "KP_WEIGHT" in cols
    has_so = "SO_NO" in cols
    has_mat = "MATERIAL_CONTENT" in cols
    # FG week ของ booking (จาก View_Booking) → คิด TARGET_KNIT ให้บล็อกแผนเก่าบน Gantt
    # ไฟล์ booking ที่รันก่อนเพิ่มคอลัมน์นี้จะไม่มี → ส่ง fg/tk = 0 หน้าเว็บก็ไม่โชว์เดดไลน์
    has_fg = {"FG_YEAR", "FG_WEEK"} <= cols
    has_fob = "FOB_TYPE" in cols
    # ไฟล์ booking เก่าอาจไม่มีคอลัมน์ลูกโซ่ carry/setup → ส่ง 0 ไป หน้าเว็บซ่อนคอลัมน์เอง
    has_setup = "_mc_increase" in cols
    has_carry = "_prev_mc_use_ceil" in cols

    # ── ฟิลด์ของการ์ดรายละเอียด (คีย์ที่ส่งขึ้นเว็บ → ชื่อคอลัมน์ในชีท DETAIL) ──
    # ข้อความ: ค่าแรกที่ไม่ว่างของกลุ่ม (ทุกแถวใน key เดียวกันมักเป็นค่าเดียวกัน)
    TXT = [("desc", "DESCRIPTION"), ("yarn", "YARN_USED"), ("team", "TEAM_NAME"),
           ("mc_cat", "MC_CAT"), ("factory", "FACTORY"), ("customer", "KNIT_CUS_NAME"),
           ("po", "PO_NO"), ("color", "COLOR"), ("nay_color", "NAY_COLOR")]
    txt_cols = [(k, c) for k, c in TXT if c in cols]
    # ตัวเลขประจำ item/เครื่อง (ไม่ใช่ยอดที่ต้องบวก) → เอาค่ามากสุดของกลุ่ม
    MAXN = [("setup_days", "_setup_days"), ("cap", "CAP_KNIT"), ("rev_weight", "REVOLUTION_WEIGHT"),
            ("wdays", "_effective_working_days" if "_effective_working_days" in cols else "WORKING_DAY"),
            ("cal_days", "WORKING_DAY")]
    max_cols = [(k, c) for k, c in MAXN if c in cols]
    # ยอดรายสัปดาห์ → รวมกันเหมือน KP_WEIGHT
    SUMN = [("knit", "KNIT_WEIGHT"), ("outstanding", "OUTSTANDING")]
    sum_cols = [(k, c) for k, c in SUMN if c in cols]
    # ปีของสัปดาห์ที่ทอ — AVA_MC ติดคอลัมน์ YEAR ให้ชีท DETAIL จากปฏิทิน (AVA_MC._with_year)
    # ส่งขึ้นเว็บด้วย เพื่อให้แยก W45 ปีนี้ ออกจาก W45 ปีก่อนได้ตอนดูประวัติย้อนหลังลึก
    # (ไฟล์รุ่นเก่าที่ยังไม่มีคอลัมน์นี้ → year = 0 หน้าเว็บถือว่า "ปีปัจจุบัน" เหมือนเดิม)
    has_year = "YEAR" in cols
    has_mcuse = "MC_USE" in cols
    has_poin = "PO_IN_DATE" in cols
    has_rdd = "RDD_WEEK" in cols
    has_fr = "FR_CODE" in cols

    agg: dict = {}
    for _, r in df.iterrows():
        wk = _wk_str(r["WEEK"])
        if wk is None:
            continue
        cat = str(r["CAT"]).strip()
        gauge = _gkey(r["GUAGE"])
        mcg = str(r["MC_GROUP"]).strip()
        item = str(r["ITEM_CODE"]).strip()
        # คีย์รวมบล็อกต้องแยกปี (สัปดาห์เลขเดียวกันคนละปี = คนละบล็อก) ; slot["week"] ยังเป็นเลขเปล่า
        # คู่กับ slot["year"] — หน้าเว็บสร้างคีย์คอลัมน์เองด้วย weekKey(week, year)
        key = (week_key(r["WEEK"], r["YEAR"] if has_year else None), cat, gauge, mcg, item)
        slot = agg.get(key)
        if slot is None:
            slot = {"week": wk, "year": 0, "cat": cat, "gauge": gauge, "mc_group": mcg,
                    "item": item, "mc": 0, "setup": 0, "carry": 0, "qty": 0.0,
                    "fg": 0, "tk": 0, "tky": 0, "rdd": 0, "mc_use": 0.0,
                    "po_in": "", "po_in_full": "", "otype": "",
                    "so": str(r["SO_NO"]).strip() if has_so else "",
                    "material": str(r["MATERIAL_CONTENT"]).strip() if has_mat else ""}
            slot.update({k: "" for k, _c in txt_cols})
            slot.update({k: 0.0 for k, _c in max_cols})
            slot.update({k: 0.0 for k, _c in sum_cols})
            # SO ทั้งหมดของบล็อก — 1 บล็อกยุบมาจากหลาย SO ได้ (so ข้างบนเป็นตัวแรกเท่านั้น)
            slot["_so"] = []
            agg[key] = slot
        # deadline ของช่อง = ตัวที่บีบสุด (AVA_MC ยุบ FG week ต่อ item ด้วย min มาแล้ว
        # แต่ DETAIL ยังแตกได้หลายแถวต่อคีย์เดียวกัน จึงต้องเลือกตัวเร็วสุดอีกชั้น)
        _fg, _tk, _tky = _fg_target_week(r, has_fg, has_fob)
        if _tk:
            # "เร็วสุด" ต้องเทียบเป็นคู่ (ปี, สัปดาห์) — เทียบเลขสัปดาห์ล้วนจะเลือก W2 ของปีหน้า
            # ว่าเร็วกว่า W50 ของปีนี้
            _cur = (slot["tky"] or 0, slot["tk"])
            _new = (_tky or 0, _tk)
            if not slot["tk"] or _new < _cur:
                slot["tk"], slot["tky"], slot["fg"] = _tk, _tky or 0, _fg or 0
        # ทุกช่องตัวเลขต้องผ่าน _f — เซลล์ว่าง = NaN ซึ่ง truthy, int(NaN)/JSON จะพัง
        if has_year and not slot["year"]:
            slot["year"] = int(_f(r["YEAR"]) or 0)
        slot["mc"] += int(_f(r["MC_USE_CEIL"]))
        if has_setup:
            slot["setup"] += int(_f(r["_mc_increase"]))
        if has_carry:
            slot["carry"] += int(_f(r["_prev_mc_use_ceil"]))
        if has_qty:
            slot["qty"] += _f(r["KP_WEIGHT"])
        if has_mcuse:
            slot["mc_use"] += _f(r["MC_USE"])
        for k, c in sum_cols:
            slot[k] += _f(r[c])
        for k, c in max_cols:
            slot[k] = max(slot[k], _f(r[c]))
        for k, c in txt_cols:
            if not slot[k]:
                v = str(r[c]).strip()
                if v and v.lower() != "nan":
                    slot[k] = v
        # RDD ของช่อง = เร็วสุด (เหตุผลเดียวกับ TARGET_KNIT — ยึด deadline ที่บีบสุด)
        if has_rdd:
            _rdd = _wk_of_yyyyww(r["RDD_WEEK"])
            if _rdd and (not slot["rdd"] or _rdd < slot["rdd"]):
                slot["rdd"] = _rdd
        if has_fr and not slot["otype"]:
            slot["otype"] = _order_type(r["FR_CODE"])
        if has_poin:
            d = _po_first(r["PO_IN_DATE"])
            # PO เข้าเร็วสุดของบล็อก = วันที่งานนี้เริ่มถูกจอง
            if d and (not slot["po_in"] or d < slot["po_in"]):
                slot["po_in"] = d
            # ด้ายเข้า "ครบทุกล็อต" = วันหลังสุด — วันที่ทอได้จริง (กติกาเดียวกับหน้า Order Color)
            # ใช้ตอนตัดสินใจ Change Cylinder: เปลี่ยนเครื่องรอไว้ให้พร้อมสัปดาห์ที่ด้ายมาครบ
            dl = _po_last(r["PO_IN_DATE"])
            if dl and dl > slot["po_in_full"]:
                slot["po_in_full"] = dl
        if has_so:
            # SO_NO 1 ช่องเก็บได้หลาย SO ต่อกันด้วยจุลภาค → แตกก่อนค่อยรวม
            for s in str(r["SO_NO"]).split(","):
                s = s.strip()
                if s and s.lower() != "nan" and s not in slot["_so"]:
                    slot["_so"].append(s)

    # ธง "ถูก Replan ยกไปวางใหม่แล้ว" — ติดเฉพาะตอนอ่าน booking เต็ม (ชีท PLAN)
    #   · ชีท REPLAN อ่านไฟล์ที่เหลือแต่สัปดาห์ล็อกอยู่แล้ว ไม่มีอะไรให้ติดธง
    #   · path ที่ระบุมาเอง = snapshot ประวัติของรอบอื่น ไม่ใช่ของรอบปัจจุบัน จึงไม่ตัดสิน
    locked_keys = None
    if path is None and not _is_replan(sheet):
        locked_keys = _replan_locked_keys(p)

    # Core Greige ตัดสินจากทะเบียน (ผ้า + ลูกค้า + ทีม) ไม่ใช่จากแผนรอบไหน
    # ไฟล์ booking เก่าไม่มีคอลัมน์ลูกค้า/ทีม → core_registry.is_core ผ่อนเป็นเทียบเท่าที่มี
    out = []
    for _key, slot in agg.items():
        slot["replanned"] = 1 if (locked_keys is not None and _key not in locked_keys) else 0
        slot["core"] = core_registry.is_core(
            slot.get("item", ""), slot.get("customer", ""), slot.get("team", ""))
        slot["qty"] = round(slot["qty"], 2)
        slot["mc_use"] = round(slot["mc_use"], 2)
        for k, _c in max_cols + sum_cols:
            slot[k] = round(slot[k], 2)
        # ส่ง SO ไม่เกิน 30 ตัว (บล็อกที่ยุบมาเยอะมากไม่ต้องพองไฟล์ JSON) + จำนวนจริง
        # so = ตัวแรกตัวเดียว (ใช้ใน tooltip บนบล็อก) ; so_list = ทั้งหมด (ใช้ในการ์ด)
        so_all = slot.pop("_so")
        slot["so_n"] = len(so_all)
        slot["so_list"] = so_all[:30]
        if so_all:
            slot["so"] = so_all[0]
        out.append(slot)
    return out


# หมายเหตุ: week_days() (อ่านชีท WEEK_DAYS จากไฟล์แผน) ถูกลบแล้ว —
# หน้าเว็บอ่านวันที่ปฏิทินเปิดสดจาก Calendar.xlsx ผ่าน /api/workday (cal_days) แทน
# เพื่อให้แก้ปฏิทินบน server แล้วเห็นผลทันทีโดยไม่ต้องรันแผนใหม่


# ---------- Lock_MC: เครื่องที่ผู้ใช้ "กันไว้" ไม่ให้แผนเอาไปใช้ ----------
# Planning หักเครื่องชุดนี้ออกตอนคิดเครื่องว่าง (Planning.py:4673) แต่ชีท SUMMARY_MC_REMAIN
# ที่ AVA_MC สร้างไม่เคยหักให้ → หน้าเว็บต้องหักเอง ไม่งั้นเลข "ว่าง" สูงกว่าที่แผนใช้ได้จริง
# (จริงทั้งชีท PLAN และ REPLAN — Replan.py ก็ปล่อยให้ Planning หักเองเหมือนกัน)
_LOCK_MC_CACHE = {"key": None, "map": {}}


def _lock_gkey(g) -> str:
    """เกจใน Lock_MC → คีย์เดียวกับ ava ('20' / '20G' / 'Gauge 20' → '20')

    Planning ตัด G/GAUGE ทิ้ง (Planning.py:566) ส่วน ava ใช้ int → ต้องผ่านทั้งสองขั้น
    ไม่งั้นคีย์ไม่ตรงแล้วหักไม่ติดแบบเงียบ ๆ"""
    s = str(g if g is not None else "").strip().upper().replace("GAUGE", "").replace("G", "").strip()
    return _pool_gkey(s)


def _calendar_weekly():
    """ตารางสัปดาห์จาก Calendar.xlsx (YEAR/WEEK เรียงตามเวลาจริง) — ใช้กางช่วงข้ามปีของ Lock_MC
    ให้ได้ลำดับสัปดาห์ชุดเดียวกับ pipeline ; อ่านไม่ได้ = None (ถอยไปใช้ช่วงแบบไม่มีปี)"""
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        cal = config.master_files().get("Calendar")
        if cal is None or not cal.exists():
            return None
        return calendar_week_map(load_calendar(cal, sheet_name="Sheet1"))
    except Exception:
        return None


def week_year_map() -> dict:
    """{เลขสัปดาห์เปล่า: ปีจริง} จาก timeline ของ Calendar.xlsx

    คอลัมน์ WEEK ในไฟล์ booking เป็นเลขสัปดาห์เปล่า (ทั้ง pipeline เป็นแบบนี้) แต่ข้อมูลจริง
    มีทั้งงานค้างของสัปดาห์ที่ผ่านมาแล้ว (W27-34 = ปีนี้) และงานปีหน้า (W1-4) ปนกัน
    → เดาด้วยกติกา "เลขน้อยกว่าสัปดาห์ปัจจุบัน = ปีหน้า" ไม่ได้ จะตีงานค้างเป็นปีหน้าหมด

    จึงกางหน้าต่าง ±26 สัปดาห์รอบสัปดาห์ปัจจุบันบน timeline ปฏิทินจริง — ในช่วง 53 สัปดาห์นี้
    เลขสัปดาห์แต่ละเลขปรากฏครั้งเดียว (เลขที่ซ้ำ = ตัวที่ใกล้สัปดาห์ปัจจุบันกว่าชนะ)
    อ่านปฏิทินไม่ได้ → คืน {} แล้วปล่อยให้ผู้เรียกตกไปเทียบเลขสัปดาห์ล้วนเหมือนเดิม
    """
    weekly = _calendar_weekly()
    if weekly is None or len(weekly) == 0:
        return {}
    cols = {str(c).strip().upper(): c for c in weekly.columns}
    yc, wc, sc = cols.get("YEAR"), cols.get("WEEK"), cols.get("WEEK_START")
    if yc is None or wc is None or sc is None:
        return {}

    today = date.today()
    idx = 0
    for i in range(len(weekly)):
        ws = weekly.iloc[i][sc]
        try:
            d = ws.date() if hasattr(ws, "date") else ws
        except Exception:
            continue
        if d is None or d > today:
            break
        idx = i

    lo = max(0, idx - _YEAR_WINDOW)
    hi = min(len(weekly), idx + _YEAR_WINDOW + 1)
    out: dict = {}
    # ใกล้สัปดาห์ปัจจุบันก่อน → เลขที่ปรากฏสองครั้งในหน้าต่างได้ปีของตัวที่ใกล้กว่า
    for i in sorted(range(lo, hi), key=lambda k: abs(k - idx)):
        r = weekly.iloc[i]
        try:
            w, y = int(r[wc]), int(r[yc])
        except (TypeError, ValueError):
            continue
        out.setdefault(w, y)
    return out


def _lock_span(weekly, w0, w1, y0, y1) -> list:
    """ช่วงสัปดาห์ของแถว Lock_MC → [คีย์สัปดาห์ (week_key)] ด้วย Calendar.WeekTimeline ตัวเดียวกับ
    Planning/AVA_MC (ไม่ใส่ปี = ครั้งที่ใกล้วันนี้สุด) — คีย์ติดปีเมื่อไกลเกินครึ่งปี จึงไม่ชนข้ามปี
    (import/ปฏิทินอ่านไม่ได้ = เดินเลขสัปดาห์ตรง ๆ เหมือนกติกา 'ไม่ระบุปี')"""
    w0 = int(w0)
    w1i = int(w1) if w1 is not None else w0
    try:
        from Calendar import WeekTimeline
        if weekly is None:
            raise ValueError("no calendar")
        yws, warn = WeekTimeline(weekly).span(w0, w1i, y0, y1)
        if warn:
            print(f"⚠️ Lock_MC W{w0}: {warn}")
        return [week_key(int(k) % 100, int(k) // 100) if int(k) >= 100000 else str(int(k)) for k in yws]
    except Exception:
        return [str(w) for w in (range(w0, w1i + 1) if w1i >= w0 else [w0])]


def _lock_mc_by_week() -> dict:
    """ชีท Lock_MC ของ MasterMC.xlsx → {(คีย์สัปดาห์, MC_CAT, เกจ): จำนวนเครื่องที่กันไว้}
    คีย์สัปดาห์ = week_key (ตรงกับคีย์ของ ava_by_week / ตาราง mc_ava_live)

    กติกาต้องตรงกับ Planning._LOCK_MC_MAP (Planning.py:562-579) เป๊ะ:
      - Week To / Year / Year To = ช่วง → กางด้วย Calendar.week_span (timeline ปฏิทินจริง)
      - คีย์ซ้ำ (สัปดาห์+กลุ่ม+เกจ เดียวกัน) = บวกรวม
      - คอลัมน์ MC = กลุ่มเครื่องที่กันไว้ → หักเฉพาะพูลของกลุ่มนั้น (ดู lock_mc_for_pool)
        ; เว้นว่าง = หักทุกพูลของ CAT+เกจ ; คอลัมน์ ITEM ไม่มีผลกับการหัก (แค่ "คงความอุ่น")
    cache ตาม (path, mtime) เพราะ ava_by_week ถูกเรียกซ้ำหลายรอบต่อ request (advisors)

    ชื่อกลุ่มเครื่องในคอลัมน์ MC เก็บคู่กันไว้ที่ _LOCK_MC_CACHE["mc"] (อ่านผ่าน
    lock_mc_groups_by_week) — ไม่มีผลกับการหัก แต่หน้า Gantt ต้องบอกได้ว่าเครื่องที่หายไป
    ถูกกันไว้ที่กลุ่มไหน ไม่งั้นพูลอื่นของเกจเดียวกันโดนหักด้วยแล้วไม่มีใครอธิบายได้"""
    p = config.master_files().get("MasterMC")
    if p is None or not p.exists():
        return {}
    try:
        ck = (str(p), p.stat().st_mtime)
    except OSError:
        return {}
    if _LOCK_MC_CACHE["key"] == ck:
        return _LOCK_MC_CACHE["map"]

    rows = []
    try:
        wb = load_workbook(p, read_only=True, data_only=True)
        try:
            if "Lock_MC" in wb.sheetnames:
                rows = [list(r) for r in wb["Lock_MC"].iter_rows(values_only=True)]
        finally:
            wb.close()
    except Exception:
        return {}

    out: dict = {}
    mc_out: dict = {}   # (week, cat, เกจ) → [ชื่อกลุ่มเครื่องที่ระบุในคอลัมน์ MC] (ไม่ซ้ำ เรียงตามที่เจอ)
    by_mc: dict = {}    # (week, cat, เกจ) → {กลุ่มเครื่อง|"": จำนวน} — ใช้หักแยกตามพูล
    if len(rows) > 1:
        # หัวคอลัมน์เขียนได้หลายแบบ (MC Lock / MC_LOCK) → ยุบช่องว่าง+ขีดล่างก่อนเทียบ
        hdr: dict = {}
        for i, c in enumerate(rows[0]):
            hdr.setdefault(re.sub(r"[\s_]+", " ", str(c or "")).strip().upper(), i)

        def cell(r, name):
            i = hdr.get(name)
            return r[i] if i is not None and i < len(r) else None

        weekly = _calendar_weekly()
        for r in rows[1:]:
            wk = _num(cell(r, "WEEK"))
            cat = str(cell(r, "MC CAT") or "").strip().upper()
            g = _lock_gkey(cell(r, "GUAGE"))
            n = _num(cell(r, "MC LOCK")) or 0
            if wk is None or not cat or not g or n <= 0:
                continue
            # ชื่อกลุ่มเครื่อง (ไม่บังคับ) — เว้นว่าง = กันจากทุกกลุ่มของ CAT+เกจ นั้น
            mc = str(cell(r, "MC") or "").strip().upper()
            if mc in ("NAN", "NONE"):
                mc = ""
            for w in _lock_span(weekly, wk, _num(cell(r, "WEEK TO")),
                                _num(cell(r, "YEAR")), _num(cell(r, "YEAR TO"))):
                k = (str(w), cat, g)      # w = คีย์สัปดาห์ (ดู _lock_span)
                out[k] = out.get(k, 0) + int(n)
                slot = by_mc.setdefault(k, {})
                slot[mc] = slot.get(mc, 0) + int(n)
                if mc:
                    names = mc_out.setdefault(k, [])
                    if mc not in names:
                        names.append(mc)

    _LOCK_MC_CACHE.update(key=ck, map=out, mc=mc_out, by_mc=by_mc)
    return out


def lock_mc_for_pool(week, cat, gauge, pool="", pool_of=None) -> int:
    """เครื่องที่ Lock_MC กันไว้ "ซึ่งมีผลกับพูลที่ระบุ" — กติกาเดียวกับ Planning._LOCK_MC_POOL

    - แถวที่เว้นคอลัมน์ MC = กันจากทุกพูลของ CAT+เกจ (พฤติกรรมเดิม)
    - แถวที่ระบุ MC = กันเฉพาะพูลที่กลุ่มเครื่องนั้นอยู่ ; แปลงเป็นพูลไม่ได้ = กันทุกพูลเหมือนเดิม
    pool="" (คีย์เป็น CAT|เกจ ไม่ได้แยกพูล) = เครื่องก้อนเดียว → รวมทุกแถว
    pool_of = ฟังก์ชัน (mc, cat, gauge) → ป้ายพูล|None (ดู lock_pool_resolver)
    """
    _lock_mc_by_week()                     # ให้ cache ตรงกับไฟล์ปัจจุบันก่อนอ่าน by_mc
    slot = (_LOCK_MC_CACHE.get("by_mc") or {}).get((str(week), str(cat).strip().upper(),
                                                   _lock_gkey(gauge)))
    if not slot:
        return 0
    base = pool_base(pool)
    n = 0
    for mc, v in slot.items():
        if not mc or not base or pool_of is None:
            n += v                          # ไม่ระบุกลุ่ม / ก้อนเดียว / ไม่รู้จักพูล → หักเต็ม
            continue
        p = pool_of(mc, cat, gauge)
        if p is None or pool_base(p) == base:
            n += v
    return int(n)


def pool_base(pool) -> str:
    """ป้ายพูลย่อยของเครื่องที่กันไว้ (":POLY" / ":COTTON" / ":TYPE_SPECIAL") → พูลแม่
    เพื่อจับคู่กับ MC_POOL_MAP ที่เก็บแต่พูลแม่ ; ป้ายปกติ (เช่น 'PHET|DOUBLE-30:22') ไม่แตะ"""
    s = str(pool or "").strip()
    for sfx in (":POLY", ":COTTON", ":TYPE_SPECIAL"):
        if s.upper().endswith(sfx):
            return s[: -len(sfx)]
    return s


def lock_pool_resolver(path=None):
    """ฟังก์ชันแปลง (กลุ่มเครื่อง, CAT, เกจ) → ป้ายพูล สำหรับใช้กับ lock_mc_for_pool
    path = ไฟล์ booking_final ที่จะอ่าน MC_POOL_MAP (None = ใบล่าสุดของ pipeline)
    ไม่มีแมป → คืนฟังก์ชันที่ตอบ None เสมอ = หักทุกพูลเหมือนเดิม"""
    m = {k.upper(): v for k, v in mc_group_map(path).items()}

    def _of(mc, cat, gauge):
        return m.get(f"{str(cat).strip().upper()}|{_pool_gkey(gauge)}|{str(mc).strip().upper()}")
    return _of


def lock_mc_groups_by_week() -> dict:
    """{(week, MC_CAT, เกจ): ["SKP", ...]} — กลุ่มเครื่องที่ระบุไว้ในคอลัมน์ MC ของ Lock_MC

    ใช้อธิบายบนหน้าเว็บเท่านั้น (ไม่ได้ใช้หักเครื่อง) ; ต้องเรียก _lock_mc_by_week ก่อน
    เพื่อให้ cache ตรงกับไฟล์ปัจจุบัน แล้วค่อยอ่านชุดชื่อที่เก็บคู่กันไว้"""
    _lock_mc_by_week()
    return _LOCK_MC_CACHE.get("mc") or {}


def ava_by_week(sheet=None) -> dict:
    """เครื่องว่าง (AVA) ต่อ (CAT×เกจ×สัปดาห์) จากชีท SUMMARY_MC_REMAIN ของ booking_final ล่าสุด
    → { week(str): { "CAT|GUAGE": {"remain": int, "total": int, "used": int} } }
    - remain = TOTAL_MC_REMAIN − เครื่องที่ Lock_MC กันไว้ (ยังไม่หักเครื่องที่แผนจอง)
      → เครื่องที่กันไว้ถูกบวกเข้า used ด้วย และแนบ lock ไว้ให้หน้าเว็บอธิบายที่มาได้
    - frontend หักเครื่องที่แผนจองเพิ่มเอง = ACTUAL_MC − เครื่อง booking ของ item นั้น
      (ดู booking_mc_by_item_week) จึงไม่ต้องมี planBase อีก

    sheet=REPLAN → อ่านจาก capacity คู่ขนาน: สัปดาห์ที่ปลดล็อก remain = เครื่องจริงเต็มจำนวน
    (ถ้าใช้ไฟล์ปกติจะหักเครื่องซ้ำสองรอบ — ทั้งที่ booking จองไว้และที่แถวแผนจองใหม่)"""
    p = _latest_booking_path(sheet)
    if p is None:
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="SUMMARY_MC_REMAIN")
    except Exception:
        return {}
    need = {"MC_CAT", "GUAGE", "WEEK", "TOTAL_MC", "MC_USE_CEIL", "TOTAL_MC_REMAIN"}
    if not need <= set(df.columns):
        return {}

    def gkey(g):
        try:
            return str(int(g))
        except (TypeError, ValueError):
            return str(g).strip()

    # แยกแถว pool (FACTORY มีค่า = เครื่องจริงต่อ pool ไม่ซ้ำ) ออกจากแถว non-pool
    # (FACTORY ว่าง = grand-total ที่เป็นเครื่องชุดเดียวกับ pool → นับซ้ำ)
    # ถ้ามี pool row ให้ใช้ pool อย่างเดียว; ไม่มีค่อยใช้ non-pool
    has_fac = "FACTORY" in df.columns
    has_pool = "MC_POOL" in df.columns
    split = _split_pool_set()   # (cat|gauge) ที่แยกพูล → key ต่อพูล (เช่น SKP vs SKPTA/SKPLE)
    pool_acc: dict = {}
    np_acc: dict = {}
    key_cg: dict = {}   # key ของ ava → (MC_CAT, เกจ) ไว้จับคู่กับ Lock_MC ที่คีย์ด้วย cat+เกจ
    for _, r in df.iterrows():
        # ตารางเครื่องยาวหลายปีแล้ว — สัปดาห์เลขเดียวกันคนละปีต้องไม่รวมกัน (ดู week_key)
        wk = week_key(r["WEEK"], r.get("YEAR"))
        if wk is None:
            continue
        cat = str(r["MC_CAT"]).strip()
        cg = f"{cat}|{gkey(r['GUAGE'])}"
        # กลุ่มที่แยกพูล → key = MC_POOL (เช่น "PHET|SKPLE , SKPTA 26G"); อื่นๆ = cat|gauge เดิม
        mc_pool = str(r.get("MC_POOL", "")).strip() if has_pool else ""
        key = mc_pool if (cg in split and mc_pool) else cg
        # พูลที่แยก (SINGLE-32) ทุกแถวในพูลเป็น cat+เกจ เดียวกัน → เก็บของแถวแรกพอ
        # ตัวที่ 3 = ป้ายพูล (ว่าง = คีย์ระดับ cat|เกจ) ไว้บอก Lock_MC ว่าหักพูลไหน
        key_cg.setdefault(key, (cat.upper(), gkey(r["GUAGE"]),
                                mc_pool if (cg in split and mc_pool) else ""))
        fac = str(r["FACTORY"]).strip() if has_fac else ""
        is_np = fac == "" or fac.lower() == "nan"
        acc = np_acc if is_np else pool_acc
        slot = acc.setdefault(wk, {}).setdefault(key, {"remain": 0, "total": 0, "used": 0})
        slot["remain"] += int(_num(r["TOTAL_MC_REMAIN"]) or 0)
        slot["total"] += int(_num(r["TOTAL_MC"]) or 0)
        slot["used"] += int(_num(r["MC_USE_CEIL"]) or 0)

    out: dict = {}
    for wk in set(pool_acc) | set(np_acc):
        for key in set(pool_acc.get(wk, {})) | set(np_acc.get(wk, {})):
            src = pool_acc.get(wk, {}).get(key) or np_acc.get(wk, {}).get(key)
            out.setdefault(wk, {})[key] = dict(src)

    # หักเครื่องที่ Lock_MC กันไว้ — ต้องทำ "หลัง" รวมแถวเสร็จ เพราะ key เดียวมาจากหลายแถวได้
    # (พูลหนึ่งมีหลายกลุ่มเครื่อง) หักตอน loop จะหักซ้ำเท่าจำนวนแถว
    # แถวล็อกที่ระบุคอลัมน์ MC หักเฉพาะพูลของกลุ่มเครื่องนั้น (กติกาเดียวกับ Planning
    # _LOCK_MC_POOL) — พูลพี่น้องของเกจเดียวกันเป็นคนละชุดเครื่อง จึงต้องไม่โดนหักตาม
    lock = _lock_mc_by_week()
    lock_mc = lock_mc_groups_by_week()
    pool_of = lock_pool_resolver(p)
    if lock:
        for wk, keys in out.items():
            w = str(wk)
            for key, slot in keys.items():
                cg = key_cg.get(key)
                n = lock_mc_for_pool(w, cg[0], cg[1], cg[2], pool_of) if cg else 0
                if n <= 0:
                    continue
                slot["lock"] = n
                # กลุ่มเครื่องที่ผู้ใช้เขียนไว้ในคอลัมน์ MC — หน้า Gantt เอาไปโชว์คู่กับเลขที่หัก
                # ให้เห็นว่าเครื่องถูกกันไว้ที่ไหน (สำคัญกับพูลอื่นของเกจเดียวกันที่โดนหักตามไปด้วย)
                names = lock_mc.get((w, cg[0], cg[1])) if cg else None
                if names:
                    slot["lock_mc"] = ", ".join(names)
                slot["remain"] -= n   # เครื่องที่กันไว้ = แผนเอาไปใช้ไม่ได้
                slot["used"] += n     # คงสมการ total − used = remain ให้การ์ดบนเว็บไม่ขัดกันเอง

    # แนบเครื่องที่กันไว้ (POLY/COTTON) — ยอดกันไว้ไม่ขึ้นกับสัปดาห์ แต่ "ที่ booking ใช้ไป"
    # ขึ้นกับสัปดาห์ → แนบทั้ง poly/cotton (กันไว้) และ poly_used/cotton_used (booking ใช้ต่อสัปดาห์)
    # เพื่อให้ frontend คำนวณเครื่องกันไว้ที่เหลือจริง = กันไว้ − booking − แผน
    # ⚠️ ต้องส่ง sheet ต่อ — โหมด REPLAN ต้องอ่านโควตา/ที่ booking ใช้ไปจาก booking_final_replan
    # (งานที่ถูกยกออกมาวางใหม่ไม่มีเครื่อง booking แล้ว) ไม่งั้นงานชุดเดียวถูกนับซ้ำ
    # ทั้งฝั่ง booking และฝั่งแผน → ชิป 🔒 โชว์ "เกิน" ทั้งที่แผนไม่ได้วางเกิน
    # เครื่องที่ user ติ๊ก Change Cylinder ไว้ (data_plan/cylinder_approved.json) = ย้ายเกจแล้วจริง
    # → บวก/ลบทับตอนอ่าน ให้เลขบนหน้าเว็บเปลี่ยนทันทีที่ติ๊กโดยไม่ต้องรันแผนใหม่ และไม่ต้องแตะ
    # booking_final_*.xlsx ที่ AVA_MC เป็นเจ้าของ ; Planning seed cylinder_adjustments จากไฟล์
    # เดียวกันตอนรันจริง (ดู cyl_approve.py) → ตัวเลขสองทางจึงตรงกันเสมอ
    # ⚠️ ava ไม่แยก factory (key = cat|gauge) → รวมเดลต้าทุกโรงงานของ cat+เกจ นั้น ; และ cat+เกจ
    # ที่แยกพูล (SINGLE-32) ไม่รู้ว่าเครื่องอยู่พูลไหน จึงใส่ทุกพูลแบบเดียวกับ Lock_MC ด้านบน
    try:
        _cyl_deltas = cyl_approve.machine_deltas(out.keys())
    except Exception as e:
        _cyl_deltas = {}
        print(f"[CYL APPROVE] อ่านการอนุมัติ Change Cylinder ไม่ได้: {e}")
    if _cyl_deltas:
        by_cg: dict = {}
        for (dw, _fac, dcat, dg), v in _cyl_deltas.items():
            k = (dw, dcat, dg)
            by_cg[k] = by_cg.get(k, 0) + v
        for wk, keys in out.items():
            w = str(wk)
            for key, slot in keys.items():
                cg = key_cg.get(key)
                if not cg:
                    continue
                d = by_cg.get((w, cg[0], cg[1]), 0)
                if not d:
                    continue
                slot["total"] += d
                slot["remain"] += d     # เครื่องที่ย้ายมายังไม่มีงานจอง = ว่างให้แผนใช้ได้เลย
                slot["cyl"] = slot.get("cyl", 0) + d   # ให้หน้าเว็บอธิบายได้ว่าเลขนี้มาจากไหน

    # เครื่อง booking ที่ตัววางแผนคืนเข้าพูลระหว่างวาง (ถอยงาน booking ออกให้งานสี) —
    # ไฟล์ booking_final ยังหักเครื่องชุดนั้นไว้ ส่วนแถวแผนที่มารับช่วงไม่มี MC_BOOKING แล้ว
    # → ถ้าไม่บวกกลับ เครื่องตัวเดียวถูกหัก 2 รอบ แล้ว Gantt ขึ้น "ไม่ว่าง (เกิน N)" ทั้งที่ไม่เกิน
    # (Planning จดไว้ในชีท MC_RELEASED ของไฟล์แผน — ดู _release_booking_mc ใน Planning.py)
    for (rw, rkey), rn in _released_mc_by_week().items():
        slot = out.get(rw, {}).get(rkey)
        if not slot or rn <= 0:
            continue
        slot["remain"] += rn
        slot["used"] = max(0, slot["used"] - rn)   # คงสมการ total − used = remain
        slot["released"] = slot.get("released", 0) + rn

    reserved = _reserved_by_cat(sheet)
    reserved_used = _reserved_used_by_cat_week(sheet)
    if reserved:
        for wk, keys in out.items():
            for key, slot in keys.items():
                rv = reserved.get(key)
                if rv and (rv["poly"] or rv["cotton"]):
                    used = reserved_used.get(wk, {}).get(key) or {}
                    slot["reserved"] = {
                        "poly": rv["poly"], "cotton": rv["cotton"],
                        "poly_used": int(used.get("poly_used", 0)),
                        "cotton_used": int(used.get("cotton_used", 0)),
                    }

    return out


def _released_mc_by_week(path=None) -> dict:
    """เครื่อง booking ที่ตัววางแผนคืนเข้าพูล → {(สัปดาห์, key ของ ava): จำนวนเครื่อง}

    อ่านชีท MC_RELEASED ของไฟล์แผนล่าสุด (Planning เขียนไว้ตอนรัน) — ไฟล์แผนรุ่นเก่า
    ไม่มีชีทนี้ = {} (พฤติกรรมเดิมทุกประการ)
    key ต้องคิดแบบเดียวกับ ava_by_week เป๊ะ: กลุ่มที่แยกพูลใช้ MC_POOL, ที่เหลือใช้ CAT|เกจ
    """
    p = Path(path) if path is not None else latest_path()
    if p is None or not p.exists():
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="MC_RELEASED")
    except Exception:
        return {}          # ไม่มีชีท = ไฟล์แผนรุ่นก่อนหน้านี้
    need = {"WEEK", "MC_CAT", "GUAGE", "RELEASED"}
    if df.empty or not need <= set(df.columns):
        return {}
    split = _split_pool_set()
    out: dict = {}
    for _, r in df.iterrows():
        wk = week_key(r["WEEK"], r.get("YEAR"))
        n = int(_num(r["RELEASED"]) or 0)
        if wk is None or n <= 0:
            continue
        cg = f"{str(r['MC_CAT']).strip()}|{_pool_gkey(r['GUAGE'])}"
        pool = str(r.get("MC_POOL", "") or "").strip()
        key = pool if (cg in split and pool) else cg
        out[(wk, key)] = out.get((wk, key), 0) + n
    return out


def _pool_gkey(g):
    try:
        return str(int(g))
    except (TypeError, ValueError):
        return str(g).strip()


def _split_pool_set() -> set:
    """(cat|gauge) ที่ถูกแยกเป็นหลายพูล (เช่น SINGLE-32|26 = SKP vs SKPTA/SKPLE)
    เฉพาะกลุ่มนี้ที่ consumer ต้อง key ต่อพูล (MC_POOL); กลุ่มอื่น key = cat|gauge เหมือนเดิม
    → backward-compatible: cat/gauge ที่ไม่ได้แยกพูลทำงานเหมือนเดิมทุกอย่าง"""
    p = _latest_booking_path()
    if p is None:
        return set()
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="SUMMARY_MC_REMAIN")
    except Exception:
        return set()
    if "MC_POOL" not in df.columns:
        return set()
    pools_by_cg: dict = {}
    for _, r in df.iterrows():
        cat = str(r["MC_CAT"]).strip().upper()
        # จำกัดการแยกพูลไว้ที่ SINGLE-32 (กลุ่ม SKP vs SKPTA/SKPLE) เท่านั้น
        # cat อื่น (เช่น DOUBLE-34 ที่มีพูลข้ามโรงงาน) คงพฤติกรรมเดิม = รวม cat|gauge
        if cat != "SINGLE-32":
            continue
        pool = str(r.get("MC_POOL", "")).strip()
        if not pool:
            continue
        cg = f"{str(r['MC_CAT']).strip()}|{_pool_gkey(r['GUAGE'])}"
        pools_by_cg.setdefault(cg, set()).add(pool)
    return {cg for cg, pools in pools_by_cg.items() if len(pools) > 1}


def pool_map() -> dict:
    """{ "CAT|GUAGE|MC_GROUP": pool_key } เฉพาะ (cat,gauge) ที่แยกพูล
    ให้หน้าเว็บ map เครื่องแต่ละตัว (SKP/SKPTA/SKPLE) → key ของ ava/reserved ที่ถูกพูล
    (เครื่องที่ไม่มี entry = ใช้ key เดิม cat|gauge)"""
    p = _latest_booking_path()
    if p is None:
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="MC_POOL_MAP")
    except Exception:
        return {}
    if not {"MC_CAT", "GUAGE", "MC_GROUP", "MC_POOL"} <= set(df.columns):
        return {}
    split = _split_pool_set()
    out: dict = {}
    for _, r in df.iterrows():
        cg = f"{str(r['MC_CAT']).strip()}|{_pool_gkey(r['GUAGE'])}"
        if cg not in split:
            continue
        mg = str(r["MC_GROUP"]).strip().upper()
        pool = str(r["MC_POOL"]).strip()
        if mg and pool:
            out[f"{cg}|{mg}"] = pool
    return out


def mc_group_map(path=None) -> dict:
    """{ "CAT|GUAGE|MC_GROUP": pool_key } ของ **ทุก** เครื่อง (ไม่กรองเฉพาะพูลที่แยก)

    ใช้บอกว่าเครื่องกลุ่มไหน "อยู่ก้อนเดียวกัน" (โรง+CAT+เกจ+Group) — เครื่องในก้อนเดียวกัน
    ใช้แทนกันได้ งานของ item เดียวกันจึงเป็นลูกโซ่เดียว ไม่ใช่คนละลูกโซ่ตามชื่อกลุ่ม
    (ต่างจาก pool_map ที่คืนเฉพาะพูลที่แยก เพราะอันนั้นใช้เป็นคีย์ของ ava/reserved)

    path = ไฟล์ที่จะอ่าน MC_POOL_MAP (None = booking_final ล่าสุดของ pipeline) — หน้า
    Machine Availability คำนวณจากไฟล์ของตัวเอง จึงต้องอ่านแมปจากใบเดียวกับที่มันใช้"""
    p = Path(path) if path is not None else _latest_booking_path()
    if p is None:
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="MC_POOL_MAP")
    except Exception:
        return {}
    if not {"MC_CAT", "GUAGE", "MC_GROUP", "MC_POOL"} <= set(df.columns):
        return {}
    out: dict = {}
    for _, r in df.iterrows():
        cg = f"{str(r['MC_CAT']).strip()}|{_pool_gkey(r['GUAGE'])}"
        mg = str(r["MC_GROUP"]).strip().upper()
        pool = str(r["MC_POOL"]).strip()
        if mg and pool:
            out[f"{cg}|{mg}"] = pool
    return out


def _reserved_by_cat(sheet=None) -> dict:
    """เครื่องที่กันไว้ให้ POLY/COTTON ต่อ key พูล จากชีท MC_RESERVED ของ booking_final ล่าสุด
    → { key: {"poly": int, "cotton": int} }  key = MC_POOL ถ้าแยกพูล ไม่งั้น CAT|GUAGE
    (เครื่องกลุ่มนี้ใช้แทนงานปกติไม่ได้)

    sheet=REPLAN → อ่านจาก booking_final_replan (ชุดเดียวกับเครื่องว่าง/booking_mc ของหน้าเดียวกัน)"""
    p = _latest_booking_path(sheet)
    if p is None:
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="MC_RESERVED")
    except Exception:
        return {}
    if not {"MC_CAT", "GUAGE", "POLY", "COTTON"} <= set(df.columns):
        return {}

    gkey = _pool_gkey
    has_pool = "MC_POOL" in df.columns
    split = _split_pool_set()
    out: dict = {}
    for _, r in df.iterrows():
        cg = f"{str(r['MC_CAT']).strip()}|{gkey(r['GUAGE'])}"
        mc_pool = str(r.get("MC_POOL", "")).strip() if has_pool else ""
        key = mc_pool if (cg in split and mc_pool) else cg
        slot = out.setdefault(key, {"poly": 0, "cotton": 0})
        slot["poly"] += int(_num(r["POLY"]) or 0)
        slot["cotton"] += int(_num(r["COTTON"]) or 0)
    return out


def _reserved_used_by_cat_week(sheet=None) -> dict:
    """เครื่อง POLY/COTTON ที่ booking ใช้ไปแล้ว 'ต่อสัปดาห์' จากชีท MC_RESERVED_WEEKLY
    → { week(str): { "CAT|GUAGE": {"poly_used": int, "cotton_used": int} } }
    ใช้คู่กับ _reserved_by_cat() เพื่อรู้เครื่องกันไว้ที่เหลือจริง = กันไว้ − booking

    sheet=REPLAN → อ่านจาก booking_final_replan ซึ่งนับเฉพาะงานที่ยัง "ตรึง" อยู่
    (Replan.py คำนวณชีทนี้ใหม่จาก locked_detail) ; ถ้าอ่านไฟล์ปกติจะได้ booking ก่อนยกงานออก
    แล้วงานชุดเดียวกันถูกนับซ้ำกับแถวแผน"""
    p = _latest_booking_path(sheet)
    if p is None:
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="MC_RESERVED_WEEKLY")
    except Exception:
        return {}
    if not {"MC_CAT", "GUAGE", "WEEK", "POLY_USED", "COTTON_USED"} <= set(df.columns):
        return {}

    gkey = _pool_gkey
    has_pool = "MC_POOL" in df.columns
    split = _split_pool_set()
    out: dict = {}
    for _, r in df.iterrows():
        wk = week_key(r["WEEK"], r.get("YEAR"))
        if wk is None:
            continue
        cg = f"{str(r['MC_CAT']).strip()}|{gkey(r['GUAGE'])}"
        mc_pool = str(r.get("MC_POOL", "")).strip() if has_pool else ""
        key = mc_pool if (cg in split and mc_pool) else cg
        slot = out.setdefault(wk, {}).setdefault(key, {"poly_used": 0, "cotton_used": 0})
        slot["poly_used"] += int(_num(r["POLY_USED"]) or 0)
        slot["cotton_used"] += int(_num(r["COTTON_USED"]) or 0)
    return out


def _coerce(v):
    """แปลงค่าจาก grid (string) → ชนิดที่เหมาะ; เก็บรหัสที่มี leading zero เป็น string"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip()
    if s == "":
        return None
    if _INT_RE.match(s):
        try:
            return int(s)
        except ValueError:
            return s
    if _FLOAT_RE.match(s):
        try:
            return float(s)
        except ValueError:
            return s
    return s


# ลูกค้าที่ทำให้ item ใน Itemcore ถือเป็น "CORE ITEM" — ชุดเดียวกับ CORE_CUSTOMERS ใน Planning.py
# (item อยู่ใน Itemcore + ลูกค้าของ order เป็นรายชื่อนี้ → IS_CORE_ITEM)
# อ่านจากหน้า "ตั้งค่าระบบ" ผ่าน _cfg() เพื่อให้ตรงกับ engine เสมอ — เรียกเป็นฟังก์ชัน
# เพราะค่า module-level จะค้างค่าเก่าไว้จน restart container
def core_customers() -> set:
    return _cfg()["core_customers"]


def core_item_codes() -> set:
    """ITEM_CODE ทั้งหมดที่มีชื่อในทะเบียน Core Greige (หลวมที่สุด — ดูแค่รหัสผ้า)

    ใช้ได้เฉพาะตอนที่ข้อมูลมีแค่รหัสผ้าจริง ๆ เพราะ core ตัวจริงตัดสินด้วย
    ผ้า + ลูกค้า + ทีม ครบสามอย่าง (ดู core_registry.is_core) — รหัสผ้าเดียวกัน
    ที่ผูกกับลูกค้าอื่นหรืออยู่คนละทีมไม่ใช่ core ของแถวนั้น
    """
    return core_registry.items()


def is_core_row(item, cus="", team="") -> bool:
    """แถวนี้เป็น Core Greige ไหม — ส่งลูกค้า/ทีมมาด้วยได้เท่าที่ข้อมูลมี
    (ทางผ่านไป core_registry เพื่อให้โมดูลที่ import plan_view อยู่แล้วไม่ต้อง import เพิ่ม)"""
    return core_registry.is_core(item, cus, team)


def program_map() -> dict:
    """ชีท "Program" ใน MasterMC (user กำหนดเอง) → { ITEM_CODE(upper): [TEAM, ...] }
    หน้าแผนใช้คู่กับคอลัมน์ TEAM ของแถว: ตรงทั้ง item และทีม → ทำตัวหนังสือเป็นสีเหลือง
    อ่านสดทุกครั้ง (ไม่ cache) เพื่อให้แก้ Master แล้วเห็นผลโดยไม่ต้องรันแผนใหม่"""
    p = config.master_files().get("MasterMC")
    if p is None or not p.exists():
        return {}
    try:
        wb = load_workbook(p, read_only=True, data_only=True)
    except Exception:
        return {}
    if "Program" not in wb.sheetnames:
        wb.close()
        return {}
    rows = list(wb["Program"].iter_rows(values_only=True))
    wb.close()
    if not rows:
        return {}
    header = [str(c).strip().upper() if c is not None else "" for c in rows[0]]
    ii = header.index("ITEM_CODE") if "ITEM_CODE" in header else -1
    ti = header.index("TEAM_NAME") if "TEAM_NAME" in header else -1
    if ii < 0 or ti < 0:
        return {}
    out: dict = {}
    for r in rows[1:]:
        item = str(r[ii]).strip().upper() if ii < len(r) and r[ii] is not None else ""
        team = str(r[ti]).strip() if ti < len(r) and r[ti] is not None else ""
        if not item or not team:
            continue
        teams = out.setdefault(item, [])
        if team not in teams:
            teams.append(team)
    return out


class PlanFileChanged(livesync.Conflict):
    """แผนบนเซิร์ฟเวอร์เป็นไฟล์คนละใบกับที่จอนั้นเปิดอยู่ (มีการรันแผนรอบใหม่คั่นกลาง)

    ด่าน base_rev จับเคสนี้ไม่ได้ เพราะเลขรุ่นผูกกับ "ชีท" (plan/PLAN) ไม่ได้ผูกกับ "ไฟล์"
    และตอนรันเสร็จ runner bump แค่ชุด run — เลขรุ่นของชีทจึงยังเท่าเดิมทั้งที่ไฟล์เปลี่ยนใบแล้ว
    """

    def __init__(self, resource: str, want: str, got: str):
        Exception.__init__(
            self, f"มีการรันแผนรอบใหม่ไปแล้ว (ตอนนี้คือ {got}) — จอนี้ยังถือแผนใบเก่า "
                  f"({want}) อยู่ ถ้าบันทึกทับ แผนที่เพิ่งรันจะหายทั้งใบ "
                  'กด "โหลดแผนล่าสุด" ก่อนแล้วค่อยจัดใหม่')
        self.resource = resource
        self.expected = want
        # file_changed ให้หน้าเว็บแยกได้ว่าเป็นคนละไฟล์ ไม่ใช่แค่มีคนเซฟแซงในไฟล์เดียวกัน
        self.current = {"file": got, "file_changed": True}


def write_grid(sheet: str, columns: list, rows: list,
               user: str = "", base_rev=None, base_file: str = "") -> dict:
    """เขียนชีทกลับไฟล์แผนล่าสุด (แก้เฉพาะชีทนี้ ชีทอื่นคงเดิม) + สำรอง .bak

    ทั้งก้อนอยู่ใต้ล็อกไฟล์แผน — การลาก Gantt คือส่งตารางทั้งชีทกลับมาเขียนทับ
    ถ้าสองคนลากพร้อมกันโดยไม่เข้าคิว งานของคนแรกจะหายไปกับตารางฉบับเก่าของคนที่สอง
    base_rev ไม่ตรง = มีคนแก้แซงไปแล้ว → Conflict (หน้าเว็บจะโหลดใหม่ให้เห็นของจริงก่อน)
    base_file = ชื่อไฟล์แผนที่จอนั้นโหลดไป ; ไม่ตรงกับไฟล์ล่าสุด = คนละใบ → PlanFileChanged
    """
    p = latest_path()
    if p is None:
        raise FileNotFoundError("ไม่พบไฟล์แผนผลิตให้บันทึก")
    res = livesync.plan_res(sheet)

    with livesync.file_lock(p):
        # ต้องเทียบ "ไฟล์" ก่อนเทียบ "เลขรุ่น" — ไฟล์ใบใหม่เริ่มต้นด้วยเลขรุ่นเดิม ด่านล่างจึงผ่าน
        if base_file and str(base_file) != p.name:
            raise PlanFileChanged(res, str(base_file), p.name)
        livesync.check_rev(res, base_rev)
        bak = livesync.backup_throttled(p)

        wb = load_workbook(p, data_only=False)
        if sheet not in wb.sheetnames:
            wb.close()
            raise KeyError(f"ไม่พบชีท '{sheet}' ในไฟล์แผน")
        ws = wb[sheet]

        if ws.max_row > 0:
            ws.delete_rows(1, ws.max_row)
        ws.append([str(c) for c in columns])
        for row in rows:
            ws.append([_coerce(v) for v in row])

        with livesync.atomic_save(p) as tmp:
            wb.save(tmp)
        wb.close()
        ent = livesync.bump(res, user=user, detail=f"แผน {sheet} {len(rows)} แถว")
    return {"ok": True, "backup": bak, "rows": len(rows), "name": p.name, "rev": ent["rev"]}


# ---------- พูลเครื่องจ้างทอ (ชีท MC S9 ของ MasterMC) ----------
# Gantt จ้างทอต้องมีเพดานเครื่องของผู้รับจ้างเป็นของตัวเอง — พูลในบ้าน (ava_by_week) ใช้ไม่ได้
# เพราะแถวจ้างทอถูกเขียนเป็น MC_GROUP=COMKN ซึ่งไม่มีเครื่องอยู่ในโรงงานเราเลย
def _s9_gauge_num(v) -> str:
    """เลขเกจจากค่าในชีท MC S9 — รูปเดียวกับไฟล์ต้นฉบับ `28G34"` (ตรงกับ _s9_gauge_num ใน Planning.py)"""
    import re as _re
    s = str(v if v is not None else "").strip()
    if not s or s.upper() in ("NAN", "NONE"):
        return ""
    m = _re.match(r"\s*(\d+)\s*[Gg]", s)
    return m.group(1) if m else _gkey(s)


def _s9_pool_id(supp, mc, g, cat="") -> str:
    """รหัสพูลรายผู้รับจ้าง = 1 แถวในชีท MC S9 — รูปเดียวกับ _s9_pool_key ใน Planning.py

    "ผู้รับจ้าง|กลุ่ม|เกจ|CAT" (คอลัมน์ S9_POOL ของแผน) — เครื่องเป็นของแต่ละเจ้า เพดานแยกกัน
    """
    def _t(v):
        s = str(v if v is not None else "").strip()
        return "" if s.upper() in ("NAN", "NONE") else s
    return f"{_t(supp)}|{_t(mc).upper()}|{_s9_gauge_num(g)}|{_t(cat).upper()}"


def s9_work_days() -> float:
    """วันทำงานต่อสัปดาห์ของผู้รับจ้างทอ — ตั้งที่หน้า "ตั้งค่าระบบ" › จ้างทอข้างนอก
    ค่าเดียวกับ Planning.S9_WORK_DAYS (หักวันหยุดตามปฏิทินอีกชั้น)
    เป็นฟังก์ชันเพราะ webapp รันค้าง ถ้าเก็บเป็นค่าคงที่จะค้างค่าเก่าจน restart container"""
    return float(_cfg()["s9_work_days"])


def s9_pool() -> dict:
    """เครื่องของผู้รับจ้างทอ → { "<Supp>|<MC>|<เกจ>": {supp, mc, gauge, total, cats, subpools, …} }

    user 16 ก.ย. 2569: เครื่องเป็นของผู้รับจ้างแต่ละเจ้า — 1 แถวในชีท = 1 เจ้า = 1 พูล เพดานแข็ง
    (SINGLE-32 เกจ 26 จ้างรวมได้ 25 เครื่อง แต่ห้ามเกินของแต่ละเจ้า)
    คีย์ = รหัสพูลรายเจ้า เดียวกับคอลัมน์ S9_POOL ของแผน · subpools มีตัวเดียว (ตัวเอง)
    เพื่อให้หน้าเว็บที่คิดแบบ "พูลย่อย" อยู่แล้วใช้ต่อได้โดยไม่ต้องแยกสองทาง
    cap = 0 เสมอ — กำลังผลิตมาจาก CAP ต่อผ้าในชีท Item S9 (ชีทนี้ไม่มี Cap/Day แล้ว)
    """
    reg = config.master_files()
    path = reg.get("MasterMC")
    if path is None or not Path(path).exists():
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(path, sheet_name="MC S9")
    except Exception:
        return {}
    df.columns = df.columns.str.strip()
    _col = {str(c).strip().lower(): c for c in df.columns}

    def _pick(*names):
        for n in names:
            if n.lower() in _col:
                return _col[n.lower()]
        return None

    c_supp = _pick("Supp", "Supplier")
    c_mc = _pick("Mc", "MC Group")
    c_cat = _pick("New CAT", "MC_CAT")
    c_g = _pick("New Gauge", "Guage", "Gauge")
    c_tot = _pick("Total Mc", "Total MC")
    c_mat = _pick("Material", "ประเภทผ้า")     # POLY / COTTON / BLEND — ว่าง = BLEND
    c_status = _pick("Status", "สถานะ")        # Inactive = เครื่องหยุดใช้ ไม่นับเป็นเพดาน
    if c_mc is None or c_g is None or c_tot is None:
        return {}
    s9_wd = s9_work_days()      # อ่านครั้งเดียวต่อรอบ (ค่าจากหน้าตั้งค่าระบบ)
    out: dict = {}
    for _, r in df.iterrows():
        supp = str(r[c_supp]).strip() if c_supp else ""
        if supp.upper() in ("NAN", "NONE"):
            supp = ""
        mc = str(r[c_mc]).strip().upper()
        g = _s9_gauge_num(r[c_g])
        tot = int(_f(r[c_tot]))
        if not mc or not g or tot <= 0:
            continue
        if c_status is not None and str(r[c_status]).strip().upper() == "INACTIVE":
            continue
        cat = str(r[c_cat]).strip() if c_cat else ""
        if cat.upper() in ("NAN", "NONE"):
            cat = ""
        # หมวดผ้าที่เจ้านี้รับ — ตรงกับ _s9_rows_init ใน Planning.py
        # ไฟล์รุ่นเก่าที่ไม่มีคอลัมน์ Material → ถอยไปดูชื่อเจ้า "รุ่งสยาม (POLY)" แบบเดิม
        if c_mat is not None:
            mv = str(r[c_mat]).strip().upper()
            mats = [mv] if mv in ("POLY", "COTTON", "BLEND") else ["BLEND"]
        else:
            mats = ["POLY"] if "POLY" in supp.upper() else ["COTTON", "BLEND"]
        poly = mats == ["POLY"]                 # คงไว้ให้หน้าเว็บรุ่นเก่าที่ยังอ่าน poly
        pid = _s9_pool_id(supp, mc, g, cat)
        e = out.setdefault(pid, {
            "supp": supp, "mc": mc, "gauge": g, "total": 0, "cap": 0.0,
            "wd": s9_wd, "poly": poly, "mats": mats, "cats": [], "subpools": [],
        })
        e["total"] += tot
        if cat and cat not in e["cats"]:
            e["cats"].append(cat)
        if not e["subpools"]:
            e["subpools"].append({"id": pid, "supp": supp, "mc": mc, "cat": cat, "remark": "",
                                  "poly": poly, "mats": mats, "total": 0, "cap": 0.0,
                                  "wd": s9_wd})
        e["subpools"][0]["total"] = e["total"]
    return out


def s9_item_cap() -> dict:
    """CAP ของผู้รับจ้างต่อผ้า จากชีท Item S9 → { "ITEM|GAUGE": กก./เครื่อง/วัน }

    user 15 ก.ย. 2569: กำลังผลิตจ้างทอใช้ CAP ของผ้าตัวนั้นตรง ๆ (ตรงกับ _s9_item_cap ใน Planning.py)
    ไม่มีคู่นี้ / CAP ว่าง → หน้าเว็บถอยไปใช้ cap ของพูลย่อยในชีท MC S9
    """
    reg = config.master_files()
    path = reg.get("MasterMC")
    if path is None or not Path(path).exists():
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(path, sheet_name="Item S9")
    except Exception:
        return {}
    df.columns = df.columns.str.strip()
    if not {"ITEM_CODE", "MC_GAUGE", "CAP"} <= set(df.columns):
        return {}
    out: dict = {}
    for it, g, c in zip(df["ITEM_CODE"], df["MC_GAUGE"], df["CAP"]):
        cap = _f(c)
        it_s = str(it if it is not None else "").strip().upper()
        if cap <= 0 or not it_s or it_s == "NAN":
            continue
        out.setdefault(f"{it_s}|{_gkey(g)}", cap)
    return out


def s9_eligible() -> list:
    """คู่ (ITEM|เกจ) ที่จ้างทอได้ = ทุกแถวในชีท Item S9 (ตรงกับ _s9_eligible_items ใน Planning.py)

    หน้าเว็บใช้เปิดปุ่ม "🧵 ส่งไปจ้างทอ" ในการ์ดงาน — แยกจาก s9_item_cap เพราะตัวนั้นข้ามแถวที่ CAP ว่าง
    (ผ้าที่จ้างทอได้แต่ยังไม่กรอก CAP ต้องยังมีปุ่ม)
    """
    reg = config.master_files()
    path = reg.get("MasterMC")
    if path is None or not Path(path).exists():
        return []
    try:
        import pandas as pd
        df = pd.read_excel(path, sheet_name="Item S9")
    except Exception:
        return []
    df.columns = df.columns.str.strip()
    if not {"ITEM_CODE", "MC_GAUGE"} <= set(df.columns):
        return []
    out: set = set()
    for it, g in zip(df["ITEM_CODE"], df["MC_GAUGE"]):
        it_s = str(it if it is not None else "").strip().upper()
        g_s = _gkey(g)
        if not it_s or it_s == "NAN" or not g_s or g_s.upper() == "NAN":
            continue
        out.add(f"{it_s}|{g_s}")
    return sorted(out)

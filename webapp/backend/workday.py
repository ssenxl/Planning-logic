"""
workday.py — API สำหรับ "วันทำงานตามกลุ่มเครื่อง" (Factory/MC_CAT/Guage)

แหล่งเดียวของความจริง = Calendar.xlsx
  ชีท "Work Day"   : Factory | MC_CAT | Guage | WEEK | WORK_DAY | WORK_HOUR | YEAR
                     WEEK ว่าง = ค่ามาตรฐานของกลุ่ม · WEEK มีเลข = เฉพาะสัปดาห์นั้นของปี YEAR
  ชีท "Week Merge" : WEEK | MERGE_TO | YEAR | MERGE_TO_YEAR  (ยุบสัปดาห์ของปีนั้น ใช้ทั้งระบบ)

ค่ารายสัปดาห์และการยุบต้องมีปีเสมอ — เลขสัปดาห์เปล่าถูกตีความเป็น "ครั้งที่ใกล้วันนี้สุด"
พอเวลาผ่านไปครึ่งปีจะไหลไปชี้ปีหน้าเอง (เคย: ยุบ 31→32 ของปี 2026 ทำให้ W31/2027 ได้ 0 วัน)
ภายในโมดูลนี้และ API คีย์สัปดาห์ = YW (ปี*100+สัปดาห์) ; แถวเก่าที่ไม่มีปี ensure_years() เขียนปีให้

ครั้งแรกที่ยังไม่มีชีท Work Day → seed จากคอลัมน์ Working Day ของ MasterMC ให้อัตโนมัติ
เพื่อให้แผนไม่เปลี่ยนทันทีตอนย้ายมาใช้ระบบใหม่
"""
import re
import sys
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

import config
import edit_diff
import livesync
from config import master_files

WORKDAY_SHEET = "Work Day"
WEEK_MERGE_SHEET = "Week Merge"
WORKDAY_HEADER = ["Factory", "MC_CAT", "Guage", "WEEK", "WORK_DAY", "WORK_HOUR", "YEAR"]
WEEK_MERGE_HEADER = ["WEEK", "MERGE_TO", "YEAR", "MERGE_TO_YEAR"]
DEFAULT_WORK_DAYS = 6.0     # หากลุ่มไม่เจอ → 6 วัน (ต้องตรงกับ Calendar.DEFAULT_WORK_DAYS)
DEFAULT_WORK_HOURS = 24.0   # ไม่ตั้ง = 24 ชม. (ไม่ลดกำลังผลิต)
SEED_WORK_DAYS = DEFAULT_WORK_DAYS   # ใช้ตอน seed จาก MasterMC (แถวที่ MasterMC ไม่มีค่า)

# กลุ่มที่ไม่ใช่เครื่องทอ (ตรวจ/ตัด/จ้างทอ) — แหล่งเดียวกับ AVA_MC.py/Order.py คือ McGroups.py
# กลุ่มจ้างทออ่านจาก MasterMC (Factory = OUTSOURCE / ไม่มีเครื่อง) ไม่ hard-code ชื่อไว้ในโค้ด
# import ไม่ได้ (รันนอก repo) → ใช้ชุดพื้นเท่ากับของเดิม เพื่อไม่ให้หน้าเว็บล้ม
def _non_knit_mc() -> set:
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        import McGroups
        return set(McGroups.non_knit_groups(master_files().get("MasterMC")))
    except Exception as e:
        print(f"[workday] โหลด McGroups ไม่ได้ ({e}) — ใช้ชุดพื้น")
        return {"CL-NP", "CL-OM", "COMKN", "F-CL", "CL", "FQCCL-NP", "FQCCL-OM",
                "FQC-OMNOI", "FQC-PHET", "FQC", "F-TSD"}


NON_KNIT_MC = _non_knit_mc()


def _norm(v) -> str:
    s = "" if v is None else str(v).strip()
    if s.lower() in ("nan", "none"):
        return ""
    if re.fullmatch(r"\d+\.0+", s):  # 12.0 → 12 (Excel เก็บ Guage เป็น float)
        return s.split(".", 1)[0]
    return s


def _num(v):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _path(key: str) -> Path:
    p = master_files().get(key)
    if p is None or not p.exists():
        raise FileNotFoundError(f"ไม่พบไฟล์ {key}")
    return p


def _sheet_rows(path: Path, sheet: str) -> tuple:
    """(หัวคอลัมน์ตัวใหญ่ → ตำแหน่ง, แถวข้อมูล) ของชีท — ไม่มีชีท = ({}, [])"""
    wb = load_workbook(path, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        wb.close()
        return {}, []
    rows = [list(r) for r in wb[sheet].iter_rows(values_only=True)]
    wb.close()
    if not rows:
        return {}, []
    hdr = {str(c).strip().upper(): i for i, c in enumerate(rows[0]) if c is not None}
    return hdr, rows[1:]


def _calendar_weekly():
    """calendar_week_map ของ Calendar.xlsx (1 แถวต่อ ปี+สัปดาห์) ; อ่านไม่ได้ = None"""
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        return calendar_week_map(load_calendar(_path("Calendar"), sheet_name="Sheet1"))
    except Exception:
        return None


def _timeline(weekly=None):
    """WeekTimeline ตัวเดียวกับ pipeline (แปลงเลขสัปดาห์เปล่า → YW) ; อ่านปฏิทินไม่ได้ = None"""
    if weekly is None:
        weekly = _calendar_weekly()
    if weekly is None:
        return None
    try:
        from Calendar import WeekTimeline
        tl = WeekTimeline(weekly)
        return tl if tl.keys else None
    except Exception:
        return None


def _cal_days_yw(weekly=None) -> dict:
    """วันที่ปฏิทินเปิดรายสัปดาห์ {YW: วัน} — เพดานของสัปดาห์ยุบคิดจากปีนั้นจริง"""
    if weekly is None:
        weekly = _calendar_weekly()
    if weekly is None:
        return {}
    out: dict = {}
    for r in weekly.itertuples():
        yw = int(r.year) * 100 + int(r.week)
        out[yw] = out.get(yw, 0) + int(r.working_days)
    return out


def _wlabel(n) -> str:
    """202632 → 'W32/2026' ; เลขเปล่า → 'W32'"""
    n = int(n)
    return f"W{n % 100}/{n // 100}" if n >= 100000 else f"W{n}"


def _yw_of(week, year, tl):
    """สัปดาห์ของแถว → YW : WEEK เป็น YW อยู่แล้ว / มีปี → ปีนั้น / ไม่มีปี → ครั้งที่ใกล้วันนี้สุด
    (อ่านปฏิทินไม่ได้หรือไม่มีเลขนั้นในปฏิทิน → คืนเลขเดิม)"""
    w = int(week)
    if w >= 100000:
        return w
    if year is not None:
        return int(year) * 100 + w
    k = tl.resolve_bare(w) if tl is not None else None
    return int(k) if k is not None else w


def _pin_merges(raw: dict, tl) -> dict:
    """{ต้นทาง: ปลายทาง} → YW ทั้งคู่ (แถวไม่มีปี: ต้นทางปีเดียวกับปลายทาง — กติกาเดียวกับ pipeline)
    ไม่คลาย chain — แผงแสดง/เขียนกลับทีละคู่ตามที่ผู้ใช้กด"""
    if tl is None:
        return dict(raw)
    from Calendar import pin_merge_years
    return pin_merge_years(raw, tl, unchain=False)


def mc_groups() -> list:
    """กลุ่มเครื่องทั้งหมดจาก MasterMC → [{factory, mc_cat, guage, mc, master_work_day}]
    ใช้เป็นรายการให้ user เลือกใน UI (unique ตาม Factory+MC_CAT+Guage)"""
    path = _path("MasterMC")
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb["Master MC"] if "Master MC" in wb.sheetnames else wb[wb.sheetnames[0]]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    wb.close()
    if not rows:
        return []

    hdr = {str(c).strip().upper(): i for i, c in enumerate(rows[0]) if c is not None}
    fi, ci, gi = hdr.get("FACTORY"), hdr.get("MC_CAT"), hdr.get("GUAGE")
    mi, wi, ti = hdr.get("MC"), hdr.get("WORKING DAY"), hdr.get("TOTAL MC")
    if fi is None or ci is None or gi is None:
        return []

    def _cell(r, i):
        return r[i] if i is not None and i < len(r) else None

    # จำนวนเครื่องรวมต่อกอง — หน้าเว็บใช้แยก "กองที่มีเครื่องจริง" ออกจากกองเปล่า
    # ต้องนับให้ตรงกับ mc_hub._keys_with_machines() ไม่งั้นแถบเตือนสองหน้าบอกเลขคนละชุด
    totals: dict = {}
    for r in rows[1:]:
        key = (_norm(_cell(r, fi)).upper(), _norm(_cell(r, ci)).upper(), _norm(_cell(r, gi)).upper())
        if any(key):
            totals[key] = totals.get(key, 0) + (_num(_cell(r, ti)) or 0)

    out, seen = [], set()
    for r in rows[1:]:
        def get(i):
            return _cell(r, i)
        key = (_norm(get(fi)).upper(), _norm(get(ci)).upper(), _norm(get(gi)).upper())
        if not any(key) or key in seen:
            continue
        seen.add(key)
        out.append({
            "factory": _norm(get(fi)), "mc_cat": _norm(get(ci)), "guage": _norm(get(gi)),
            "mc": _norm(get(mi)),
            "master_work_day": _num(get(wi)),   # ค่าเดิมใน MasterMC — ใช้ seed เท่านั้น
            "total": totals.get(key, 0),        # จำนวนเครื่องรวมของกอง (0 = กองเปล่า)
        })
    return out


def mc_group_map() -> dict:
    """ทุกเครื่องใน MasterMC → กลุ่มในแผง : {"MC|GUAGE": "FACTORY|MC_CAT|GUAGE"}

    ต่างจาก mc_groups() ที่ยุบเหลือกลุ่มละ 1 เครื่อง (ไว้ทำ tree) — อันนี้เก็บ "ทุกเครื่อง"
    ให้ Gantt แปลง (เครื่อง, เกจ) → กลุ่มได้ครบ เช่น TSB/26 → PHET|SINGEL-18|26
    ไม่งั้นเครื่องที่ไม่ใช่ตัวแทนกลุ่มจะหาไม่เจอ → ตกไปใช้ fallback 6 ผิด"""
    path = _path("MasterMC")
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb["Master MC"] if "Master MC" in wb.sheetnames else wb[wb.sheetnames[0]]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    wb.close()
    if not rows:
        return {}
    hdr = {str(c).strip().upper(): i for i, c in enumerate(rows[0]) if c is not None}
    fi, ci, gi, mi = hdr.get("FACTORY"), hdr.get("MC_CAT"), hdr.get("GUAGE"), hdr.get("MC")
    if None in (fi, ci, gi, mi):
        return {}
    out = {}
    for r in rows[1:]:
        def g(i):
            return _norm(r[i]).upper() if i < len(r) else ""
        mc, guage = g(mi), g(gi)
        if not mc:
            continue
        out.setdefault(f"{mc}|{guage}", f"{g(fi)}|{g(ci)}|{guage}")
    return out


def mc_rows() -> list:
    """ทุกกลุ่มเครื่องใน MasterMC → [{cat, gauge, mc_group}] (ไม่ยุบรวมเหมือน mc_groups)

    ใช้ตั้ง "แถว" ของ Gantt ในโหมด Item (ทั้งหมด) — ให้เห็นทุกกลุ่มเครื่องทุก CAT
    แม้สัปดาห์นั้นไม่มีทั้งงานแผนและงาน booking (เดิมแถวมาจากงานที่มีอยู่เท่านั้น)

    - MasterMC สะกด SINGEL-xx แต่ booking/แผนใช้ SINGLE-xx → แปลงให้ตรงกัน ไม่งั้นได้แถวซ้ำ
    - ตัดกลุ่มที่ไม่ใช่เครื่องทอ (NON_KNIT_MC) ออก ให้ตรงกับที่ AVA_MC ตัด"""
    path = _path("MasterMC")
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb["Master MC"] if "Master MC" in wb.sheetnames else wb[wb.sheetnames[0]]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    wb.close()
    if not rows:
        return []
    hdr = {str(c).strip().upper(): i for i, c in enumerate(rows[0]) if c is not None}
    ci, gi, mi, fi = hdr.get("MC_CAT"), hdr.get("GUAGE"), hdr.get("MC"), hdr.get("FACTORY")
    if None in (ci, gi, mi):
        return []
    out, seen = [], set()
    for r in rows[1:]:
        def g(i):
            return _norm(r[i]).upper() if i < len(r) else ""
        cat, guage, mc = g(ci).replace("SINGEL-", "SINGLE-"), g(gi), g(mi)
        factory = g(fi) if fi is not None else ""
        if not mc or mc in NON_KNIT_MC:
            continue
        key = (cat, guage, mc)
        if key in seen:
            continue
        seen.add(key)
        out.append({"cat": cat, "gauge": guage, "mc_group": mc, "factory": factory})
    return out


def calendar_week_days() -> dict:
    """วันที่ปฏิทินเปิดของแต่ละสัปดาห์ — อ่านสดจาก Calendar.xlsx บน server → {week(str): วัน}

    ใช้เป็นเพดานเดียวกับฝั่ง pipeline (WorkDay.get_working_days) เพื่อให้หน้าเว็บกับแผน
    เห็นวันหยุดชุดเดียวกัน — แก้ปฏิทินบน server แล้วหน้าเว็บเห็นทันทีโดยไม่ต้องรันแผนใหม่
    อ่านไม่ได้ → {} (หน้าเว็บจะไม่ cap ด้วยปฏิทิน)

    ⚠️ ปฏิทินมีหลายปี (2026–2031) และทั้งระบบใช้เลขสัปดาห์เปล่า ๆ ไม่มีปี → ต้องนับ
    ต่อ (ปี, สัปดาห์) ก่อน แล้วเลือกปีที่ใกล้ปีปัจจุบันที่สุด เหมือน _week_year_lookup
    ใน Planning.py  เดิม groupby("WEEK").sum() รวมทุกปีเข้าด้วยกัน → W41 = 30 วัน (6×5 ปี)
    ทำให้หน้าเว็บคิดว่าสัปดาห์นั้นทำงาน 30 วัน เครื่องจึงดูพอตลอด ไม่ว่าจะย้ายงานไปไหน"""
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        df = load_calendar(_path("Calendar"), sheet_name="Sheet1")
        weekly = calendar_week_map(df)          # 1 แถวต่อ (ปี, สัปดาห์) พร้อม working_days
        this_year = datetime.now().year
        out, best = {}, {}
        for r in weekly.itertuples():
            w, y = int(r.week), int(r.year)
            gap = abs(y - this_year)
            if w not in best or gap < best[w]:
                best[w] = gap
                out[str(w)] = int(r.working_days)
        return out
    except Exception:
        return {}


def calendar_week_days_by_key() -> dict:
    """วันที่ปฏิทินเปิด "ทุกสัปดาห์ทุกปี" → {คีย์คอลัมน์ของหน้าเว็บ: วัน}

    คีย์ = plan_view.week_key (เลขเปล่า = ครั้งที่ใกล้วันนี้สุด / ติดปีเมื่อไกลกว่านั้น) จึงไม่ชนกันเอง
    แผนยาวข้ามปีแล้ว Gantt ต้องรู้วันหยุดของ W35/2028 แยกจาก W35/2027 — calendar_week_days()
    เลือกปีเดียวต่อเลขสัปดาห์ (ยังเก็บไว้ให้การ์ด Lock_MC/MC_Total ที่หน้า Master ใช้แบบเดิม)
    อ่านไม่ได้ → {}"""
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        import plan_view
        weekly = calendar_week_map(load_calendar(_path("Calendar"), sheet_name="Sheet1"))
        return {plan_view.week_key(int(r.week), int(r.year)): int(r.working_days)
                for r in weekly.itertuples()}
    except Exception:
        return {}


def calendar_week_timeline() -> list:
    """ลำดับ (ปี, สัปดาห์) ตามเวลาจริงจาก Calendar.xlsx → [[2026, 1], [2026, 2], ...]

    การ์ด Lock_MC / MC_Total บนเว็บใช้กางช่วงที่ข้ามปี (เช่น W50/2026 → W3/2027) ให้ได้
    ลำดับสัปดาห์ชุดเดียวกับ Calendar.week_span ที่ pipeline ใช้ — ไม่ต้องเดาว่าปีไหนมี
    52 หรือ 53 สัปดาห์  อ่านไม่ได้ → [] (การ์ดจะถอยไปใช้ช่วงแบบไม่มีปีเหมือนเดิม)"""
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        weekly = calendar_week_map(load_calendar(_path("Calendar"), sheet_name="Sheet1"))
        return [[int(r.year), int(r.week)] for r in weekly.itertuples()]
    except Exception:
        return []


def calendar_week_dates() -> list:
    """ช่วงวันที่ของแต่ละสัปดาห์ → [["2026-01-02", "2026-01-08"], ...] เรียงคู่กับ cal_weeks

    หน้าเว็บใช้แปลง "วันที่" (เช่นวันด้ายเข้า DATE_IN / PO_IN_DATE) → เลขสัปดาห์ตามปฏิทินจริง
    ต้องแยก key จาก cal_weeks เพราะของเดิมส่งแค่ [ปี, สัปดาห์] และมีคนใช้อยู่แล้ว (Masters.jsx)
    อ่านไม่ได้ → [] (หน้าเว็บถอยไปคำนวณเองด้วยกติกา ISO week + 3 วัน = ศุกร์–พฤหัส)"""
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        weekly = calendar_week_map(load_calendar(_path("Calendar"), sheet_name="Sheet1"))
        return [
            [r.week_start.strftime("%Y-%m-%d"), r.week_end.strftime("%Y-%m-%d")]
            for r in weekly.itertuples()
        ]
    except Exception:
        return []


def _aliases(known: set) -> dict:
    """คู่ (เครื่อง|เกจ) → กลุ่มในแผง จาก booking_final ล่าสุด → {"MC|GUAGE": "FAC|CAT|GUAGE"}

    ต้องมีเพราะ booking มีชื่อเครื่องที่ไม่มีใน MasterMC (SKPHF/SKPSL/SKPAO) หรือคู่
    (เครื่อง, เกจ) ไม่ตรง — แต่ CAT+เกจ ตรงกับกลุ่มในแผงพอดี (ตรรกะเดียวกับ
    WorkDay.register_aliases ฝั่ง pipeline) · หาไฟล์ไม่ได้ → {}"""
    if not known:
        return {}
    try:
        import pandas as pd
        import plan_view
        p = plan_view._latest_booking_path()
        if p is None:
            return {}
        df = pd.read_excel(p, sheet_name="DETAIL")
    except Exception:
        return {}

    by_cat: dict = {}
    for k in known:
        parts = k.split("|")
        if len(parts) == 3:
            by_cat.setdefault((parts[1], parts[2]), set()).add(k)

    cols = {c: c for c in df.columns}
    out: dict = {}
    for r in df.to_dict("records"):
        mc = _norm(r.get("MC_GROUP")).upper()
        g = _norm(r.get("GUAGE")).upper()
        if not mc or f"{mc}|{g}" in out:
            continue
        cat = _norm(r.get("MC_CAT")).upper() or _norm(r.get("CAT")).upper()
        fac = _norm(r.get("FACTORY")).upper()
        key = None
        if fac and f"{fac}|{cat}|{g}" in known:
            key = f"{fac}|{cat}|{g}"
        else:
            cands = by_cat.get((cat, g), set())
            if len(cands) == 1:
                key = next(iter(cands))
        if key:
            out[f"{mc}|{g}"] = key
    return out


def _parse_workday(cal, tl=None) -> tuple:
    """อ่านชีท Work Day + Week Merge → (defaults, weeks, hours, week_hours, merges, legacy)

    weeks/week_hours คีย์เดียวกัน ("FAC|CAT|G|YW") — แถวหนึ่งตั้งวันอย่างเดียว ชั่วโมง
    อย่างเดียว หรือทั้งคู่ก็ได้ (ต้องตรงกับ Calendar.load_workday ฝั่ง pipeline)
    merges = {YW ต้นทาง: YW ปลายทาง} ; legacy = จำนวนแถวในไฟล์ที่ยังไม่มีปี (ตีความให้แล้ว)
    อ่านตามชื่อหัวคอลัมน์ — ไฟล์รุ่นก่อนไม่มีคอลัมน์ YEAR / WORK_HOUR"""
    if tl is None:
        tl = _timeline()
    defaults, weeks, hours, week_hours = {}, {}, {}, {}
    legacy = 0

    hdr, rows = _sheet_rows(cal, WORKDAY_SHEET)
    fi, ci, gi = hdr.get("FACTORY"), hdr.get("MC_CAT"), hdr.get("GUAGE")
    wi, di, hi, yi = hdr.get("WEEK"), hdr.get("WORK_DAY"), hdr.get("WORK_HOUR"), hdr.get("YEAR")
    if fi is not None and ci is not None and gi is not None:
        for r in rows:
            def cell(i):
                return r[i] if i is not None and i < len(r) else None
            key = "|".join(_norm(cell(i)).upper() for i in (fi, ci, gi))
            if key == "||":
                continue
            wk, days, hr = _num(cell(wi)), _num(cell(di)), _num(cell(hi))
            if wk is None:
                # ค่ามาตรฐานของกลุ่ม: วัน + ชั่วโมง (ใช้ทุกสัปดาห์ที่ไม่ได้ตั้งค่าเฉพาะไว้)
                if days is not None:
                    defaults[key] = days
                if hr is not None:
                    hours[key] = hr
                continue
            if days is None and hr is None:
                continue
            yr = _num(cell(yi))
            if wk < 100000 and yr is None:
                legacy += 1
            wkey = f"{key}|{_yw_of(wk, yr, tl)}"
            if days is not None:
                weeks[wkey] = days
            if hr is not None:
                week_hours[wkey] = hr

    raw = {}
    hdr, rows = _sheet_rows(cal, WEEK_MERGE_SHEET)
    si, ti = hdr.get("WEEK"), hdr.get("MERGE_TO")
    ysi, yti = hdr.get("YEAR"), hdr.get("MERGE_TO_YEAR")
    if si is not None and ti is not None:
        for r in rows:
            def cell(i):
                return r[i] if i is not None and i < len(r) else None
            src, dst = _num(cell(si)), _num(cell(ti))
            if src is None or dst is None:
                continue
            ys, yt = _num(cell(ysi)), _num(cell(yti))
            # YEAR = ปีของต้นทาง ; MERGE_TO_YEAR ว่าง = ปีเดียวกัน (กติกาเดียวกับ Calendar.load_week_merge)
            if ys is not None:
                src = src if src >= 100000 else ys * 100 + src
                dst = dst if dst >= 100000 else (yt if yt is not None else ys) * 100 + dst
            elif src < 100000 or dst < 100000:
                legacy += 1
            if int(src) != int(dst):
                raw[int(src)] = int(dst)
    merges = _pin_merges(raw, tl)
    return defaults, weeks, hours, week_hours, merges, legacy


def merge_day_gaps(weeks: dict, merges: dict, groups: list = None,
                   cal_yw: dict = None, tl=None) -> list:
    """สัปดาห์ที่ยุบรวมมา ยังไม่ได้ตั้ง "วันทำงาน" หรือตั้งเกินเพดาน
    → [{"week": 202632, "label": "W32/2026", "cap": 12, "missing": [key...], "over": [{key, days}]}]

    เพดาน = จำนวนวันที่ปฏิทินเปิดรวมทั้งก้อนของปีนั้น (ยุบ 2 สัปดาห์ที่เปิด 6+6 → ไม่เกิน 12) —
    กรอกน้อยกว่าได้ถ้าเครื่องเดินได้จริงไม่ถึง แต่เกินกว่านี้คือโรงไม่ได้เปิดถึง
    นับเฉพาะกองที่มีเครื่องจริง (ตรงกับ mc_hub._keys_with_machines)
    ตรวจเฉพาะสัปดาห์ยุบตั้งแต่สัปดาห์ปัจจุบัน — ของปีที่ผ่านไปแล้วอยู่ในไฟล์ตลอด ถ้าตรวจด้วย
    กองที่เพิ่มทีหลังจะทำให้แผงบันทึกไม่ได้อีกเลย"""
    if tl is None:
        tl = _timeline()
    targets = sorted({int(d) for d in (merges or {}).values()})
    if tl is not None:
        targets = [d for d in targets if tl.pos.get(d, len(tl.keys)) >= tl.today_idx]
    if not targets:
        return []
    if groups is None:
        groups = mc_groups()
    if cal_yw is None:
        cal_yw = _cal_days_yw()
    keys = sorted({"|".join(x.upper() for x in (g["factory"], g["mc_cat"], g["guage"]))
                   for g in groups if (g.get("total") or 0) > 0})
    src_of: dict = {}
    for s, d in (merges or {}).items():
        src_of.setdefault(int(d), []).append(int(s))

    out = []
    for w in targets:
        bucket = [w] + src_of.get(w, [])
        cap = sum(float(cal_yw.get(b, 0) or 0) for b in bucket) or None
        missing, over = [], []
        for k in keys:
            v = (weeks or {}).get(f"{k}|{w}")
            if v is None:
                missing.append(k)
            elif cap is not None and float(v) > cap + 1e-9:
                over.append({"key": k, "days": float(v)})
        if missing or over:
            out.append({"week": w, "label": _wlabel(w), "cap": cap, "missing": missing, "over": over})
    return out


def get_workday() -> dict:
    """คืนค่าที่ตั้งไว้ + รายการกลุ่ม + การยุบสัปดาห์
    {groups: [...], defaults: {"FAC|CAT|G": วัน}, weeks: {"FAC|CAT|G|W": วัน},
     hours: {"FAC|CAT|G": ชั่วโมง}, week_hours: {"FAC|CAT|G|W": ชั่วโมง}, merges: {week: to}}
    W ของ weeks/week_hours/merges = YW (ปี*100+สัปดาห์) เสมอ"""
    cal = _path("Calendar")
    groups = mc_groups()
    weekly = _calendar_weekly()
    tl = _timeline(weekly)
    cal_yw = _cal_days_yw(weekly)
    defaults, weeks, hours, week_hours, merges, legacy = _parse_workday(cal, tl)

    known = set(defaults) | set(hours) | {"|".join(k.split("|")[:3]) for k in weeks}
    cal_days = calendar_week_days()
    return {"groups": groups, "defaults": defaults, "weeks": weeks, "hours": hours,
            # สัปดาห์ที่ยุบรวมมาแล้วยังกรอก "วัน" ไม่ครบ/เกินเพดาน (แผงนี้เป็นที่แก้)
            "day_gaps": merge_day_gaps(weeks, merges, groups, cal_yw, tl),
            # วันที่ปฏิทินเปิดราย YW — แผงวันทำงานใช้คิดเพดานของปีที่กำลังดู
            "cal_days_yw": {str(k): v for k, v in cal_yw.items()},
            "today_yw": tl.at(tl.today_idx) if tl is not None else None,
            # แถวในไฟล์ที่ยังไม่มีปี (ปกติ 0 — backend ใส่ปีให้ตอนเริ่ม)
            "legacy_rows": legacy,
            # ชั่วโมง/วัน เฉพาะบางสัปดาห์ (เช่น สัปดาห์ที่ยุบรวมแล้วเร่งเป็น 24 ชม.)
            # แก้ที่หน้า "เครื่องจักร" ที่เดียว — วันทำงานจะถูกคูณ ชม.สัปดาห์นั้น ÷ ชม.ของกอง
            "week_hours": week_hours,
            "merges": merges, "fallback_days": DEFAULT_WORK_DAYS,
            "fallback_hours": DEFAULT_WORK_HOURS,
            # วันที่ปฏิทินเปิดต่อสัปดาห์ (สดจาก server) — เพดานของวันทำงาน
            "cal_days": cal_days,
            # วันที่ปฏิทินเปิดรายคีย์คอลัมน์ ครอบทุกปี — Gantt ใช้ตัวนี้ (แผนยาวข้ามปี)
            "cal_days_key": calendar_week_days_by_key(),
            # ลำดับ (ปี, สัปดาห์) ตามเวลาจริง — การ์ด Lock_MC/MC_Total ใช้กางช่วงข้ามปี
            # ให้ตรงกับ Calendar.week_span ฝั่ง pipeline เป๊ะ (ไม่ต้องเดา 52/53 สัปดาห์)
            "cal_weeks": calendar_week_timeline(),
            # ช่วงวันที่ของแต่ละสัปดาห์ (เรียงคู่กับ cal_weeks) — ใช้แปลงวันด้ายเข้า → สัปดาห์
            "cal_week_dates": calendar_week_dates(),
            # ทุกเครื่องใน MasterMC → กลุ่มในแผง (ให้ Gantt แปลงเครื่องไม่ใช่ตัวแทนได้ถูก)
            "mc_map": mc_group_map(),
            # ทุกกลุ่มเครื่องใน MasterMC (CAT|เกจ|เครื่อง) — Gantt โหมด Item (ทั้งหมด) ใช้ตั้งแถว
            "mc_rows": mc_rows(),
            # เครื่องที่ไม่มีใน MasterMC → กลุ่มในแผง (ให้ Gantt คำนวณตรงกับ pipeline)
            "aliases": _aliases(known)}


def _write_sheet(wb, sheet: str, header: list, rows: list) -> None:
    ws = wb[sheet] if sheet in wb.sheetnames else wb.create_sheet(sheet)
    if ws.max_row > 0:
        ws.delete_rows(1, ws.max_row)
    ws.append(header)
    for r in rows:
        ws.append(r)


class MergeDayGap(Exception):
    """บันทึกไม่ได้เพราะสัปดาห์ที่ยุบรวมมายังกรอกวันไม่ครบ/เกินเพดาน"""

    def __init__(self, gaps):
        self.gaps = gaps
        parts = []
        for g in gaps:
            cap = f" (เพดาน {g['cap']:g} วัน)" if g.get("cap") else ""
            wl = g.get("label") or _wlabel(g["week"])
            if g["missing"]:
                parts.append(f"{wl}{cap} ยังไม่ได้ตั้งวันทำงาน {len(g['missing'])} Cat")
            if g["over"]:
                parts.append(f"{wl}{cap} ตั้งวันเกินเพดาน {len(g['over'])} Cat")
        super().__init__("บันทึกไม่ได้ — " + " · ".join(parts))


def workday_changes(before: tuple, after: tuple) -> dict:
    """ผลของ _parse_workday ก่อน/หลังเซฟ → รายการที่เปลี่ยน (รูปแบบเดียวกับ edit_diff)
    เช่น "วันทำงาน [F1 · SINGLE · 28]: 6 → 5.5 วัน" · "ยุบ W31 → W32" """
    def fmt(v, unit):
        return "(ค่ามาตรฐาน)" if v is None else f"{v:g} {unit}"

    def grp(key):
        parts = key.split("|")
        head = "[" + " · ".join(parts[:3]) + "]"
        if len(parts) <= 3:
            return head
        n = _num(parts[3])
        return f"{_wlabel(n) if n is not None else 'W' + parts[3]} {head}"

    lines = []
    for i, (what, unit) in enumerate([("วันทำงาน", "วัน"), ("วันทำงาน", "วัน"),
                                      ("ชั่วโมง", "ชม."), ("ชั่วโมง", "ชม.")]):
        old, new = before[i] or {}, after[i] or {}
        for k in sorted(set(old) | set(new)):
            a, b = old.get(k), new.get(k)
            if a != b:
                lines.append(f"{what} {grp(k)}: {fmt(a, unit)} → {fmt(b, unit)}")

    old_m, new_m = before[4] or {}, after[4] or {}
    for w in sorted(set(old_m) | set(new_m)):
        a, b = old_m.get(w), new_m.get(w)
        if a == b:
            continue
        if a is None:
            lines.append(f"ยุบ {_wlabel(w)} → {_wlabel(b)}")
        elif b is None:
            lines.append(f"เลิกยุบ {_wlabel(w)} (เดิมรวมไป {_wlabel(a)})")
        else:
            lines.append(f"ยุบ {_wlabel(w)}: เดิมรวมไป {_wlabel(a)} → เปลี่ยนเป็น {_wlabel(b)}")
    return {"lines": lines[:edit_diff.MAX_LINES], "total": len(lines)}


def _yw_week_keys(d: dict, tl) -> dict:
    """คีย์ "FAC|CAT|G|W" → W เป็น YW เสมอ (เลขเปล่าจากผู้เรียกรุ่นเก่า → ครั้งที่ใกล้วันนี้สุด)"""
    out = {}
    for k, v in (d or {}).items():
        parts = str(k).split("|")
        w = _num(parts[3]) if len(parts) == 4 else None
        if w is None:
            continue
        out[f"{parts[0]}|{parts[1]}|{parts[2]}|{_yw_of(w, None, tl)}"] = v
    return out


def _yw_merges(merges: dict, tl) -> dict:
    raw = {}
    for s, d in (merges or {}).items():
        s, d = _num(s), _num(d)
        if s is not None and d is not None and int(s) != int(d):
            raw[int(s)] = int(d)
    return _pin_merges(raw, tl)


def _week_cols(yw: int) -> tuple:
    """YW → (WEEK, YEAR) สำหรับเขียนลงชีท ; เลขเปล่า (อ่านปฏิทินไม่ได้) → (เลขเดิม, ว่าง)"""
    return (yw % 100, yw // 100) if yw >= 100000 else (yw, None)


def save_workday(defaults: dict, weeks: dict, hours: dict = None, merges: dict = None,
                 week_hours: dict = None, user: str = "", base_rev=None,
                 enforce_merge_days: bool = False, note_lines: list = None) -> dict:
    """เขียนชีท Work Day + Week Merge กลับ Calendar.xlsx (สำรอง .bak ก่อน)
    defaults: {"FAC|CAT|G": วัน} · weeks: {"FAC|CAT|G|YW": วัน}
    hours: {"FAC|CAT|G": ชั่วโมง} (ค่ามาตรฐานต่อกลุ่ม) · merges: {"202631": 202632}
    สัปดาห์ส่งเป็นเลขเปล่ามาได้ (ผู้เรียกรุ่นเก่า) → ตีความเป็นครั้งที่ใกล้วันนี้สุดแล้วเขียนปีลงไฟล์
    week_hours: {"FAC|CAT|G|YW": ชั่วโมง} — None = คงของเดิมในไฟล์ไว้ (ผู้เรียกที่ยังไม่รู้จักช่องนี้
    จะได้ไม่ล้างชั่วโมงรายสัปดาห์ทิ้งโดยไม่ตั้งใจ) · {} = ล้างทั้งหมดจริง ๆ
    note_lines: บรรทัดที่ต่อหน้าประวัติการแก้ไข (เช่น ensure_years ที่ค่าไม่เปลี่ยนแต่ต้องจดไว้)

    enforce_merge_days: ด่าน "สัปดาห์ที่ยุบรวมมาต้องตั้งวันทำงานเองและไม่เกินเพดานปฏิทิน"
    เปิดเฉพาะเส้นทางแผงวันทำงาน (/api/workday) ซึ่งเป็นที่เดียวที่แก้วันรายสัปดาห์ได้ —
    ถ้าเปิดกับผู้เรียกอื่น (หน้าเครื่องจักร/เพิ่มเครื่อง) จะกลายเป็นทางตัน: เขาแก้วันไม่ได้
    แต่ถูกห้ามบันทึกชั่วโมงเพราะวันไม่ครบ

    หน้าเว็บส่งแผงทั้งแผงกลับมาเขียนทับ → ต้องเทียบเลขรุ่นก่อน (base_rev) ไม่งั้นวันทำงาน
    ที่อีกคนเพิ่งแก้จะถูกกลืนหายไปกับแผงฉบับเก่าที่คนนี้ถืออยู่
    """
    cal = _path("Calendar")
    tl = _timeline()

    if week_hours is None:
        week_hours = _parse_workday(cal, tl)[3]
    defaults, hours = defaults or {}, hours or {}
    weeks, week_hours = _yw_week_keys(weeks, tl), _yw_week_keys(week_hours, tl)
    merges = _yw_merges(merges, tl)

    if enforce_merge_days:
        gaps = merge_day_gaps(weeks, merges, tl=tl)
        if gaps:
            raise MergeDayGap(gaps)

    wd_rows = []
    # แถวค่ามาตรฐานของกลุ่ม (WEEK ว่าง) = union ของกลุ่มที่ตั้งวัน และ/หรือ ชั่วโมง
    for key in sorted(set(defaults) | set(hours)):
        fac, cat, g = (key.split("|") + ["", "", ""])[:3]
        d, h = _num(defaults.get(key)), _num(hours.get(key))
        if d is not None or h is not None:
            wd_rows.append([fac, cat, g, None, d, h, None])
    # แถวรายสัปดาห์ = union ของสัปดาห์ที่ตั้งวัน และ/หรือ ชั่วโมง (ตั้งอย่างใดอย่างหนึ่งก็ได้)
    for key in sorted(set(weeks) | set(week_hours),
                      key=lambda k: (k.rsplit("|", 1)[0], int(k.rsplit("|", 1)[1]))):
        parts = key.split("|")
        d, h = _num(weeks.get(key)), _num(week_hours.get(key))
        if d is not None or h is not None:
            wk, yr = _week_cols(int(parts[3]))
            wd_rows.append([parts[0], parts[1], parts[2], wk, d, h, yr])

    mg_rows = []
    for s, t in sorted(merges.items()):
        (sw, sy), (tw, ty) = _week_cols(s), _week_cols(t)
        mg_rows.append([sw, tw, sy, ty])

    with livesync.file_lock(cal):
        livesync.check_rev(livesync.WORKDAY, base_rev)
        # ค่าก่อนแก้ — ไว้จดประวัติว่าเปลี่ยนอะไร (อ่านไม่ได้ = จดแค่จำนวนแถวแบบเดิม)
        try:
            before = _parse_workday(cal, tl)
        except Exception:
            before = None
        bak = livesync.backup_throttled(cal)
        wb = load_workbook(cal, data_only=False)
        _write_sheet(wb, WORKDAY_SHEET, WORKDAY_HEADER, wd_rows)
        _write_sheet(wb, WEEK_MERGE_SHEET, WEEK_MERGE_HEADER, mg_rows)
        with livesync.atomic_save(cal) as tmp:
            wb.save(tmp)
        wb.close()
        changes = None
        if before is not None:
            try:
                changes = workday_changes(before, _parse_workday(cal, tl))
            except Exception as e:
                print(f"[WORKDAY] เทียบก่อน/หลังแก้ไม่ได้: {e}")
        if note_lines:
            base = changes or {"lines": [], "total": 0}
            changes = {"lines": list(note_lines) + base["lines"],
                       "total": len(note_lines) + base["total"]}
        ent = livesync.bump(livesync.WORKDAY, user=user,
                            detail=f"วันทำงาน {len(wd_rows)} แถว", changes=changes)
    return {"ok": True, "backup": bak, "rows": len(wd_rows), "merges": len(mg_rows),
            "rev": ent["rev"]}


def extend_calendar(year: int, user: str = "") -> dict:
    """เติมปฏิทิน (Calendar.xlsx ชีท Sheet1) ให้ถึงสิ้นปี year — ปุ่ม "เพิ่มปี" ในหน้าปฏิทิน

    ใช้ gen_calendar.ensure_calendar_extended ตัวเดียวกับที่ต่ออัตโนมัติ (+3 ปี) ตอน server เริ่ม / ก่อนรัน
    pipeline → วันจันทร์–เสาร์ทำงาน หยุดวันอาทิตย์ แล้วผู้ใช้มากำหนดวันหยุด/ยุบสัปดาห์เอง
    ไม่จำกัดปี (user สั่ง 29 ก.ย. 2569) — แต่แผนวางยาวถึงสุดปฏิทิน เพิ่มไกลมาก = รันนานขึ้น
    (จำกัดได้ที่ตั้งค่า KP_HORIZON_WEEKS)
    ต้องเลื่อนเลขรุ่นของชีท Sheet1 — จอที่เปิดปฏิทินค้างไว้จะโหลดใหม่ ไม่เอาตารางเก่าไปเซฟทับปีที่เพิ่งเพิ่ม"""
    year = int(year)
    this_year = datetime.now().year
    if not this_year <= year <= this_year + 100:
        raise ValueError(f"ปีต้องอยู่ระหว่าง {this_year}–{this_year + 100}")
    cal = _path("Calendar")
    if str(config.REPO_DIR) not in sys.path:
        sys.path.insert(0, str(config.REPO_DIR))
    from gen_calendar import ensure_calendar_extended
    with livesync.file_lock(cal):
        r = ensure_calendar_extended(cal, end_year=year)
        if not r.get("ok", True):
            raise RuntimeError(r.get("reason") or "เติมปฏิทินไม่สำเร็จ")
        if r.get("added"):
            line = (f"เพิ่มปฏิทิน {r['from']} ถึง {r['to']} ({r['added']} วัน) "
                    f"— จันทร์–เสาร์ทำงาน หยุดวันอาทิตย์ (backup: {Path(r['backup']).name if r.get('backup') else '-'})")
            ent = livesync.bump(livesync.masters_res("Calendar", "Sheet1"), user=user,
                                detail=f"เพิ่มปฏิทินถึงปี {year}",
                                changes={"lines": [line], "total": 1})
            r["rev"] = ent["rev"]
            # เพดานวัน/สัปดาห์ของแผงยุบสัปดาห์มาจากปฏิทิน → ให้จอที่เปิดแผงอยู่โหลดใหม่ (ไม่จดประวัติซ้ำ)
            livesync.bump(livesync.WORKDAY, user=user, detail=f"เพิ่มปฏิทินถึงปี {year}",
                          changes={"lines": [], "total": 0})
    return r


def save_merges(merges: dict, user: str = "", base_rev=None) -> dict:
    """บันทึกเฉพาะการยุบสัปดาห์ (แผงในหน้าปฏิทิน) — วัน/ชั่วโมงของกองคงตามไฟล์

    สัปดาห์ที่เพิ่งถูกยุบเข้าสัปดาห์อื่นไม่มีวันทำงานของตัวเองแล้ว → ล้างค่ารายสัปดาห์ของสัปดาห์นั้นทุกกอง
    ไม่มีด่านกรอกครบที่นี่: วัน/ชั่วโมงของสัปดาห์ยุบกรอกที่หน้าเครื่องจักร ถ้าบล็อกตรงนี้จะยุบสัปดาห์ไม่ได้เลย
    (หน้าเครื่องจักรกับ Replan เป็นด่านแทน)"""
    cal = _path("Calendar")
    tl = _timeline()
    defaults, weeks, hours, week_hours, old, _ = _parse_workday(cal, tl)
    new = _yw_merges(merges, tl)
    gone = {s for s in new if s not in old}
    if gone:
        weeks = {k: v for k, v in weeks.items() if int(k.rsplit("|", 1)[1]) not in gone}
        week_hours = {k: v for k, v in week_hours.items() if int(k.rsplit("|", 1)[1]) not in gone}
    return save_workday(defaults, weeks, hours, new, week_hours=week_hours,
                        user=user, base_rev=base_rev)


def ensure_years(user: str = "system") -> dict:
    """แถวเก่าในชีท Work Day / Week Merge ที่ยังไม่มีปี → เขียนปีลงไฟล์ถาวร (backend เรียกตอนเริ่ม)

    ปีที่ใส่ = กติกาเดียวกับ _parse_workday: ค่ารายสัปดาห์ = ครั้งที่ใกล้วันนี้สุด ;
    การยุบ = หาปีของปลายทางก่อน แล้วต้นทางปีเดียวกัน
    ต้องเขียนลงไฟล์ ไม่ใช่แค่ตีความตอนอ่าน — ไม่งั้นพอเวลาผ่านไปครึ่งปี เลขเดิมจะไหลไปชี้ปีหน้าเอง
    ไม่มีแถวไม่มีปี = ไม่แตะไฟล์"""
    cal = _path("Calendar")
    tl = _timeline()
    if tl is None:
        return {"ok": False, "fixed": 0, "reason": "อ่านปฏิทินไม่ได้"}
    defaults, weeks, hours, week_hours, merges, legacy = _parse_workday(cal, tl)
    if not legacy:
        return {"ok": True, "fixed": 0}
    lines = [f"ใส่ปีให้แถวเดิมที่ไม่มีปี {legacy} แถว (ตีความ ณ {datetime.now():%Y-%m-%d}) "
             f"— กันค่าไหลไปใช้กับปีถัดไปเอง"]
    lines += [f"ยุบ {_wlabel(s)} → {_wlabel(d)}" for s, d in sorted(merges.items())]
    r = save_workday(defaults, weeks, hours, merges, week_hours=week_hours,
                     user=user, note_lines=lines)
    r["fixed"] = legacy
    return r


def seed_from_mastermc(user: str = "") -> dict:
    """สร้างชีท Work Day ครั้งแรกจากคอลัมน์ Working Day ของ MasterMC (ไม่ทับของเดิมที่ตั้งไว้)"""
    cur = get_workday()
    defaults = dict(cur["defaults"])
    added = 0
    for g in cur["groups"]:
        key = "|".join(x.upper() for x in (g["factory"], g["mc_cat"], g["guage"]))
        if key in defaults:
            continue
        defaults[key] = g["master_work_day"] if g["master_work_day"] is not None else SEED_WORK_DAYS
        added += 1
    if not added:
        return {"ok": True, "added": 0}
    r = save_workday(defaults, cur["weeks"], cur.get("hours"), cur["merges"], user=user)
    r["added"] = added
    return r

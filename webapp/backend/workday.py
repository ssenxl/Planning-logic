"""
workday.py — API สำหรับ "วันทำงานตามกลุ่มเครื่อง" (Factory/MC_CAT/Guage)

แหล่งเดียวของความจริง = Calendar.xlsx
  ชีท "Work Day"   : Factory | MC_CAT | Guage | WEEK | WORK_DAY
                     WEEK ว่าง = ค่ามาตรฐานของกลุ่ม · WEEK มีเลข = เฉพาะสัปดาห์นั้น
  ชีท "Week Merge" : WEEK | MERGE_TO  (ยุบสัปดาห์ ใช้ทั้งระบบ)

ครั้งแรกที่ยังไม่มีชีท Work Day → seed จากคอลัมน์ Working Day ของ MasterMC ให้อัตโนมัติ
เพื่อให้แผนไม่เปลี่ยนทันทีตอนย้ายมาใช้ระบบใหม่
"""
import re
import shutil
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

from config import master_files

WORKDAY_SHEET = "Work Day"
WEEK_MERGE_SHEET = "Week Merge"
WORKDAY_HEADER = ["Factory", "MC_CAT", "Guage", "WEEK", "WORK_DAY"]
WEEK_MERGE_HEADER = ["WEEK", "MERGE_TO"]
DEFAULT_WORK_DAYS = 6.0


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


def _rows_of(path: Path, sheet: str) -> list:
    """คืนแถวข้อมูล (ไม่รวม header) ของชีท — ไม่มีชีท = []"""
    wb = load_workbook(path, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        wb.close()
        return []
    out = [list(r) for r in wb[sheet].iter_rows(values_only=True)][1:]
    wb.close()
    return out


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
    mi, wi = hdr.get("MC"), hdr.get("WORKING DAY")
    if fi is None or ci is None or gi is None:
        return []

    out, seen = [], set()
    for r in rows[1:]:
        def get(i):
            return r[i] if i is not None and i < len(r) else None
        key = (_norm(get(fi)).upper(), _norm(get(ci)).upper(), _norm(get(gi)).upper())
        if not any(key) or key in seen:
            continue
        seen.add(key)
        out.append({
            "factory": _norm(get(fi)), "mc_cat": _norm(get(ci)), "guage": _norm(get(gi)),
            "mc": _norm(get(mi)),
            "master_work_day": _num(get(wi)),   # ค่าเดิมใน MasterMC — ใช้ seed เท่านั้น
        })
    return out


def get_workday() -> dict:
    """คืนค่าที่ตั้งไว้ + รายการกลุ่ม + การยุบสัปดาห์
    {groups: [...], defaults: {"FAC|CAT|G": วัน}, weeks: {"FAC|CAT|G|W": วัน}, merges: {week: to}}"""
    cal = _path("Calendar")
    groups = mc_groups()

    defaults, weeks = {}, {}
    for r in _rows_of(cal, WORKDAY_SHEET):
        r = list(r) + [None] * (5 - len(r))
        key = "|".join(_norm(x).upper() for x in r[:3])
        days, wk = _num(r[4]), _num(r[3])
        if days is None or key == "||":
            continue
        if wk is None:
            defaults[key] = days
        else:
            weeks[f"{key}|{int(wk)}"] = days

    merges = {}
    for r in _rows_of(cal, WEEK_MERGE_SHEET):
        r = list(r) + [None] * (2 - len(r))
        src, dst = _num(r[0]), _num(r[1])
        if src is None or dst is None or int(src) == int(dst):
            continue
        merges[int(src)] = int(dst)

    return {"groups": groups, "defaults": defaults, "weeks": weeks, "merges": merges,
            "fallback_days": DEFAULT_WORK_DAYS}


def _write_sheet(wb, sheet: str, header: list, rows: list) -> None:
    ws = wb[sheet] if sheet in wb.sheetnames else wb.create_sheet(sheet)
    if ws.max_row > 0:
        ws.delete_rows(1, ws.max_row)
    ws.append(header)
    for r in rows:
        ws.append(r)


def save_workday(defaults: dict, weeks: dict, merges: dict) -> dict:
    """เขียนชีท Work Day + Week Merge กลับ Calendar.xlsx (สำรอง .bak ก่อน)
    defaults: {"FAC|CAT|G": วัน} · weeks: {"FAC|CAT|G|W": วัน} · merges: {"31": 32}"""
    cal = _path("Calendar")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = cal.with_suffix(cal.suffix + f".{ts}.bak")
    shutil.copy2(cal, bak)

    wd_rows = []
    for key, days in sorted((defaults or {}).items()):
        fac, cat, g = (key.split("|") + ["", "", ""])[:3]
        d = _num(days)
        if d is not None:
            wd_rows.append([fac, cat, g, None, d])
    for key, days in sorted((weeks or {}).items()):
        parts = key.split("|")
        if len(parts) != 4:
            continue
        d, wk = _num(days), _num(parts[3])
        if d is not None and wk is not None:
            wd_rows.append([parts[0], parts[1], parts[2], int(wk), d])

    mg_rows = []
    for src, dst in sorted((merges or {}).items(), key=lambda kv: int(kv[0])):
        s, t = _num(src), _num(dst)
        if s is not None and t is not None and int(s) != int(t):
            mg_rows.append([int(s), int(t)])

    wb = load_workbook(cal, data_only=False)
    _write_sheet(wb, WORKDAY_SHEET, WORKDAY_HEADER, wd_rows)
    _write_sheet(wb, WEEK_MERGE_SHEET, WEEK_MERGE_HEADER, mg_rows)
    wb.save(cal)
    wb.close()
    return {"ok": True, "backup": bak.name, "rows": len(wd_rows), "merges": len(mg_rows)}


def seed_from_mastermc() -> dict:
    """สร้างชีท Work Day ครั้งแรกจากคอลัมน์ Working Day ของ MasterMC (ไม่ทับของเดิมที่ตั้งไว้)"""
    cur = get_workday()
    defaults = dict(cur["defaults"])
    added = 0
    for g in cur["groups"]:
        key = "|".join(x.upper() for x in (g["factory"], g["mc_cat"], g["guage"]))
        if key in defaults:
            continue
        defaults[key] = g["master_work_day"] if g["master_work_day"] is not None else DEFAULT_WORK_DAYS
        added += 1
    if not added:
        return {"ok": True, "added": 0}
    r = save_workday(defaults, cur["weeks"], cur["merges"])
    r["added"] = added
    return r

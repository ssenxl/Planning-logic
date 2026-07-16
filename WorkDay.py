"""
WorkDay.py — วันทำงานรายสัปดาห์ของแต่ละกลุ่มเครื่อง (แหล่งเดียว)

อ่านจาก Calendar.xlsx เท่านั้น:
  ชีท "Work Day"   : Factory | MC_CAT | Guage | WEEK | WORK_DAY
                     WEEK ว่าง = ค่ามาตรฐานของกลุ่ม · WEEK มีเลข = เฉพาะสัปดาห์นั้น
  ชีท "Week Merge" : WEEK | MERGE_TO  (ยุบสัปดาห์ ใช้ทั้งระบบ เช่น W31 → W32)

กฎ:
- ค่าที่กรอกใช้ตรง ๆ ไม่หักวันหยุดซ้ำ (user เห็นวันหยุดในปฏิทินตอนกรอกอยู่แล้ว)
- สัปดาห์ที่ถูกยุบเข้าสัปดาห์อื่น → วันทำงาน = 0 (ห้ามวางแผน)
- สัปดาห์ปลายทางของการยุบ → วันทำงาน = ผลรวมของทั้งก้อน
- สัปดาห์ที่ปฏิทินหยุดทั้งสัปดาห์ (ไม่มีวันทำงานเลย) → 0 กันวางแผนลงสัปดาห์ที่ปิดโรงงาน
- ไม่เจอกลุ่มในชีท → DEFAULT_WORK_DAYS (6 วัน)

ไม่มีกฎพิเศษ Week 17/32 · ไม่ใช้ Working Day ของ MasterMC · ไม่ใช้ working_day ของ Item Special
(Working Hours. ของ MasterMC และ working_hour ของ Item Special ยังใช้ตามเดิม)
"""
import re

import pandas as pd

from Calendar import DEFAULT_WORK_DAYS, load_week_merge, load_workday

_WORKDAY: dict = {"default": {}, "week": {}}
_MERGE: dict = {}                 # week ต้นทาง → week ปลายทาง
_MERGE_SOURCES: dict = {}         # week ปลายทาง → [week ต้นทาง ...]
_GROUP_OF: dict = {}              # (MC, GUAGE) → (Factory, MC_CAT, Guage)
_GROUP_OF_MC: dict = {}           # MC → (Factory, MC_CAT, Guage)  (fallback ไม่ระบุ gauge)
_CAL_WD = None                    # callable: week → จำนวนวันทำงานตามปฏิทิน (int)


def norm_key(v) -> str:
    """ทำให้คีย์เทียบกันได้: ตัดช่องว่าง → ตัวใหญ่, 12.0 → 12 (Excel อ่าน Guage เป็น float)"""
    s = str(v).strip().upper()
    if s in ("NAN", "NONE"):
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def init(master_mc_df: pd.DataFrame, cal_working_days) -> None:
    """โหลดชีท Work Day / Week Merge + ทำ index จาก MasterMC เพื่อแปลง MC → กลุ่ม

    master_mc_df : DataFrame ของชีท Master MC (ต้องมีคอลัมน์ Factory, MC_CAT, MC, Guage)
    cal_working_days : ฟังก์ชัน week → จำนวนวันทำงานตามปฏิทินของสัปดาห์นั้น
    """
    global _WORKDAY, _MERGE, _MERGE_SOURCES, _GROUP_OF, _GROUP_OF_MC, _CAL_WD
    _WORKDAY = load_workday()
    _MERGE = load_week_merge()
    _CAL_WD = cal_working_days

    _MERGE_SOURCES = {}
    for src, dst in _MERGE.items():
        _MERGE_SOURCES.setdefault(dst, []).append(src)

    _GROUP_OF, _GROUP_OF_MC = {}, {}
    for _, r in master_mc_df.iterrows():
        grp = (norm_key(r.get("Factory")), norm_key(r.get("MC_CAT")), norm_key(r.get("Guage")))
        mc = norm_key(r.get("MC"))
        if not mc:
            continue
        _GROUP_OF[(mc, grp[2])] = grp
        _GROUP_OF_MC.setdefault(mc, grp)


def _group(mc_group, gauge) -> tuple | None:
    mc, g = norm_key(mc_group), norm_key(gauge)
    return _GROUP_OF.get((mc, g)) or _GROUP_OF_MC.get(mc)


def _raw_days(grp, week: int) -> float:
    """วันทำงานของกลุ่มในสัปดาห์นั้นตามชีท (ยังไม่คิดเรื่องยุบสัปดาห์/ปฏิทิน)"""
    if grp is None:
        return DEFAULT_WORK_DAYS
    wk = _WORKDAY["week"].get(grp + (int(week),))
    if wk is not None:
        return float(wk)
    return float(_WORKDAY["default"].get(grp, DEFAULT_WORK_DAYS))


def merged_into(week) -> int | None:
    """สัปดาห์นี้ถูกยุบเข้าสัปดาห์ไหน (None = ไม่ได้ถูกยุบ)"""
    try:
        return _MERGE.get(int(week))
    except (TypeError, ValueError):
        return None


def get_working_days(mc_group, gauge=None, week=None) -> float:
    """วันทำงานจริงที่ใช้วางแผน — ผลลัพธ์สุดท้าย ไม่ต้องหักวันหยุดต่อ"""
    if week is None:
        return float(_WORKDAY["default"].get(_group(mc_group, gauge), DEFAULT_WORK_DAYS))

    week = int(week)
    if merged_into(week) is not None:
        return 0.0  # ถูกยุบเข้าสัปดาห์อื่น → ห้ามวางแผนลงสัปดาห์นี้

    grp = _group(mc_group, gauge)
    sources = _MERGE_SOURCES.get(week, [])           # สัปดาห์ที่ยุบเข้ามาที่ปลายทางนี้
    bucket = [week] + sources                         # สัปดาห์นี้ + สัปดาห์ที่ยุบเข้ามา

    # ปฏิทินหยุดทั้งก้อน → ผลิตไม่ได้
    if _CAL_WD is not None and sum(int(_CAL_WD(w)) for w in bucket) == 0:
        return 0.0

    # สัปดาห์ปลายทางของการยุบ: ถ้า user ตั้งค่ารายสัปดาห์ไว้ = ยอดรวมทั้งก้อน (แก้เอง)
    # เช่นยุบได้ 10 วันตามปฏิทิน แต่เครื่องเดินได้จริง 8 วัน → กรอก 8
    if sources and grp is not None:
        override = _WORKDAY["week"].get(grp + (week,))
        if override is not None:
            return float(override)

    return float(sum(_raw_days(grp, w) for w in bucket))

"""
WorkDay.py — วันทำงานรายสัปดาห์ของแต่ละกลุ่มเครื่อง (แหล่งเดียว)

อ่านจาก Calendar.xlsx เท่านั้น:
  ชีท "Work Day"   : Factory | MC_CAT | Guage | WEEK | WORK_DAY
                     WEEK ว่าง = ค่ามาตรฐานของกลุ่ม · WEEK มีเลข = เฉพาะสัปดาห์นั้น
  ชีท "Week Merge" : WEEK | MERGE_TO  (ยุบสัปดาห์ ใช้ทั้งระบบ เช่น W31 → W32)

กฎ (จุดคำนวณเดียวของทั้งระบบ — ที่อื่นห้ามคำนวณเอง):

    วันทำงาน = ค่าที่ตั้งในแผง + (ปฏิทินเปิด − ปกติ 6)   ← เลื่อนตามปฏิทิน ทั้งขึ้นและลง

  เช่น ตั้ง 5.5 · W33 เปิด 5 → 4.5 · W17 เปิด 8 → 7.5 · ปิดทั้งสัปดาห์ → 0
       จากนั้นผู้เรียกค่อยหัก setup ต่อเอง (4.5 − setup 2 = 2.5)

- ค่าที่ตั้งในแผง = รูปแบบการทำงานของกลุ่ม (เช่น 5.5 วัน/สัปดาห์) — เป็น float ห้ามปัดเป็น int
- ตั้งค่า "รายสัปดาห์" ไว้เอง → ใช้ตรง ๆ ไม่หักวันหยุดซ้ำ (user กรอกโดยเห็นปฏิทินอยู่แล้ว)
- สัปดาห์ที่ถูกยุบเข้าสัปดาห์อื่น → 0 (ห้ามวางแผน) · สัปดาห์ปลายทาง → รวมทั้งก้อน (หักวันหยุดรายสัปดาห์ก่อนรวม)
- ไม่เจอกลุ่มในแผง (รวมกรณีเกจไม่ตรง) → DEFAULT_WORK_DAYS (6 วัน) แล้วหักวันหยุดเหมือนกัน
  (AVA_MC พิมพ์เตือนรายชื่อคู่ (เครื่อง,เกจ) ที่หาไม่เจอไว้ให้ไล่เก็บ)

ค่าประจำกลุ่ม (วัน/ชั่วโมง) มาจากแผง "วันทำงานตามกลุ่มเครื่อง" เท่านั้น:
ไม่มีกฎพิเศษ Week 17/32 · ไม่ใช้ Working Day/Working Hours. ของ MasterMC
(working_hour ของ Item Special ยังชนะเสมอ — จัดการที่ Planning/AVA_MC ระดับ item)
"""
import re

import pandas as pd

from Calendar import (DEFAULT_WORK_DAYS, DEFAULT_WORK_HOURS, NORMAL_CAL_DAYS,
                      load_week_merge, load_workday)

_WORKDAY: dict = {"default": {}, "week": {}}
_MERGE: dict = {}                 # week ต้นทาง → week ปลายทาง
_MERGE_SOURCES: dict = {}         # week ปลายทาง → [week ต้นทาง ...]
_GROUP_OF: dict = {}              # (MC, GUAGE) → (Factory, MC_CAT, Guage)
_SOLO_MC: dict = {}               # MC → กลุ่ม (เฉพาะเครื่องที่อยู่กลุ่มเดียว — ใช้ตอนไม่รู้เกจ)
_CAL_WD = None                    # callable: week → จำนวนวันที่ปฏิทินเปิดของสัปดาห์นั้น


def norm_key(v) -> str:
    """ทำให้คีย์เทียบกันได้: ตัดช่องว่าง → ตัวใหญ่, 12.0 → 12 (Excel อ่าน Guage เป็น float)"""
    s = str(v).strip().upper()
    if s in ("NAN", "NONE"):
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def init(master_mc_df: pd.DataFrame, cal_working_days) -> None:
    """โหลดชีท Work Day / Week Merge + ทำ index จาก MasterMC เพื่อแปลง (MC, เกจ) → กลุ่ม

    master_mc_df : DataFrame ของชีท Master MC (ต้องมีคอลัมน์ Factory, MC_CAT, MC, Guage)
    cal_working_days : ฟังก์ชัน week → จำนวนวันที่ปฏิทินเปิดของสัปดาห์นั้น (ใช้เป็นเพดาน)
    """
    global _WORKDAY, _MERGE, _MERGE_SOURCES, _GROUP_OF, _SOLO_MC, _CAL_WD
    _WORKDAY = load_workday()
    _MERGE = load_week_merge()
    _CAL_WD = cal_working_days

    _MERGE_SOURCES = {}
    for src, dst in _MERGE.items():
        _MERGE_SOURCES.setdefault(dst, []).append(src)

    def _cat_of(r):
        # Planning rename MC_CAT → Type_1 ก่อนส่งเข้ามา (ดู Planning.py) จึงต้องรับทั้งสองชื่อ
        v = r.get("MC_CAT")
        if v is None or (isinstance(v, float) and pd.isna(v)):
            v = r.get("Type_1")
        return v

    _GROUP_OF = {}
    _by_mc: dict = {}
    for _, r in master_mc_df.iterrows():
        grp = (norm_key(r.get("Factory")), norm_key(_cat_of(r)), norm_key(r.get("Guage")))
        mc = norm_key(r.get("MC"))
        if not mc:
            continue
        _GROUP_OF[(mc, grp[2])] = grp
        _by_mc.setdefault(mc, set()).add(grp)

    # เครื่องที่อยู่กลุ่มเดียวเท่านั้น (เช่น TSA, TSB) → รู้กลุ่มได้แม้ผู้เรียกไม่ส่งเกจมา
    # เครื่องที่อยู่หลายกลุ่ม (SKP, RAP, FA ...) ไม่ใส่ เพราะเดาเกจแทน user ไม่ได้
    _SOLO_MC = {mc: next(iter(gs)) for mc, gs in _by_mc.items() if len(gs) == 1}


def _panel_keys() -> set:
    """กลุ่มที่ "มีอยู่จริงในแผง" (ตั้งค่าวัน/ชั่วโมง/รายสัปดาห์ไว้อย่างน้อยหนึ่งอย่าง)"""
    return (set(_WORKDAY.get("default", {}))
            | set(_WORKDAY.get("hour", {}))
            | {k[:3] for k in _WORKDAY.get("week", {})})


def register_aliases(rows) -> int:
    """สอนคู่ (MC_GROUP, เกจ) → กลุ่มในแผง จากข้อมูล booking

    จำเป็นเพราะ booking มีชื่อเครื่องที่ไม่มีใน MasterMC (เช่น SKPHF, SKPSL, SKPAO)
    หรือมีเครื่องแต่คู่ (เครื่อง, เกจ) ไม่ตรง (เช่น SKPTA เกจ 24) — ทั้งที่ CAT+เกจ
    ตรงกับกลุ่มในแผงพอดี  คีย์จริงของแผงคือ Factory·MC_CAT·Guage จึงจับจากตรงนั้น

    rows : DataFrame/iterable ที่มีคอลัมน์ FACTORY, CAT (หรือ MC_CAT), MC_GROUP, GUAGE
    เพิ่มเฉพาะกลุ่มที่มีอยู่จริงในแผงเท่านั้น — ไม่สร้างกลุ่มใหม่เอง · คืนจำนวนคู่ที่เพิ่ม
    """
    known = _panel_keys()
    if not known:
        return 0
    # (MC_CAT, เกจ) → กลุ่ม ถ้าไม่กำกวม (มีโรงเดียว) ใช้ตอนไม่รู้ Factory
    by_cat: dict = {}
    for k in known:
        by_cat.setdefault((k[1], k[2]), set()).add(k)

    if hasattr(rows, "to_dict"):
        rows = rows.to_dict("records")
    added = 0
    for r in rows:
        mc, g = norm_key(r.get("MC_GROUP")), norm_key(r.get("GUAGE"))
        if not mc or (mc, g) in _GROUP_OF:
            continue
        cat = norm_key(r.get("MC_CAT")) or norm_key(r.get("CAT"))
        fac = norm_key(r.get("FACTORY"))
        grp = None
        if fac and (fac, cat, g) in known:          # ตรงคีย์แผงเป๊ะ
            grp = (fac, cat, g)
        else:
            cands = by_cat.get((cat, g), set())      # ไม่รู้โรง → ต้องไม่กำกวม
            if len(cands) == 1:
                grp = next(iter(cands))
        if grp is not None:
            _GROUP_OF[(mc, g)] = grp
            added += 1
    return added


def _group(mc_group, gauge) -> tuple | None:
    """(MC, เกจ) → กลุ่มในแผง

    - ระบุเกจมา → ต้องตรงทั้งคู่ (เกจไม่ตรงแล้วหยิบค่าเกจอื่นคือของเดิมที่ผิดเงียบ ๆ)
    - ไม่ระบุเกจ → ใช้ได้เฉพาะเครื่องที่อยู่กลุ่มเดียว (_SOLO_MC) เพราะไม่มีความกำกวม
      (ผู้เรียกบางจุดใน Planning ไม่มีเกจในบริบท — ถ้าไม่มีทางนี้จะตกไป fallback 6 ผิด)
    ชื่อเครื่องที่ไม่มีใน MasterMC ให้ใช้ register_aliases() สอนจาก booking ก่อน"""
    mc, g = norm_key(mc_group), norm_key(gauge)
    hit = _GROUP_OF.get((mc, g))
    if hit is not None:
        return hit
    return _SOLO_MC.get(mc) if not g else None


def _cal_of(week: int):
    """วันที่ปฏิทินเปิดของสัปดาห์นั้น (float) · None = ยังไม่ได้ส่งปฏิทินเข้ามา"""
    if _CAL_WD is None:
        return None
    try:
        return float(_CAL_WD(week))
    except (TypeError, ValueError):
        return None


def _week_days(grp, week: int) -> float:
    """วันทำงานของกลุ่มในสัปดาห์เดียว (ยังไม่คิดเรื่องยุบสัปดาห์)

    - ตั้งค่ารายสัปดาห์ไว้เอง → ใช้ตรง ๆ ไม่ปรับตามปฏิทิน (user กรอกโดยเห็นปฏิทินแล้ว)
    - ไม่ได้ตั้ง → ค่ามาตรฐาน + (ปฏิทินเปิด − ปกติ) = เลื่อนตามผลต่างปฏิทิน ทั้งขึ้นและลง
        · W17 เปิด 8 (มากกว่าปกติ 2) → 5.5 + 2 = 7.5
        · W33 เปิด 5 (น้อยกว่าปกติ 1) → 5.5 − 1 = 4.5 · ปิดทั้งก้อน → 0
    """
    week = int(week)
    if grp is not None:
        wk = _WORKDAY["week"].get(grp + (week,))
        if wk is not None:
            return float(wk)
    base = float(_WORKDAY["default"].get(grp, DEFAULT_WORK_DAYS)) if grp is not None \
        else float(DEFAULT_WORK_DAYS)
    cal = _cal_of(week)
    if cal is None:
        return base
    if cal <= 0:
        return 0.0                                    # โรงปิดทั้งสัปดาห์ → ผลิตไม่ได้
    over = base - NORMAL_CAL_DAYS
    if cal > NORMAL_CAL_DAYS and over > 0:
        # กลุ่มที่เดินเกินสัปดาห์ปกติ (เช่น 6.5) เจอสัปดาห์ที่ปฏิทินเปิดเกินปกติ (เช่น 8)
        # → กลับเป็น "เดินขาด" จากปฏิทิน เท่ากับส่วนเกินนั้น (8 − 0.5 = 7.5)
        return max(0.0, cal - over)
    return max(0.0, base + (cal - NORMAL_CAL_DAYS))   # เลื่อนตามผลต่างปฏิทิน (บวก/ลบ)


def has_group(mc_group, gauge=None) -> bool:
    """จับคู่ (เครื่อง, เกจ) เข้ากลุ่มในแผงได้ไหม — False = จะได้ค่า fallback ไม่ใช่ค่าที่ตั้งไว้"""
    return _group(mc_group, gauge) is not None


def get_working_hours(mc_group, gauge=None) -> float:
    """ชั่วโมงทำงานของกลุ่ม (Factory·MC_CAT·Guage) จากชีท Work Day — แหล่งเดียว
    ไม่ได้ตั้ง / ไม่เจอกลุ่ม → DEFAULT_WORK_HOURS (24 ชม. = ไม่ลดกำลังผลิต)"""
    grp = _group(mc_group, gauge)
    hr = _WORKDAY.get("hour", {}).get(grp) if grp is not None else None
    return float(hr) if hr is not None else DEFAULT_WORK_HOURS


def merged_into(week) -> int | None:
    """สัปดาห์นี้ถูกยุบเข้าสัปดาห์ไหน (None = ไม่ได้ถูกยุบ)"""
    try:
        return _MERGE.get(int(week))
    except (TypeError, ValueError):
        return None


def get_working_days(mc_group, gauge=None, week=None) -> float:
    """วันทำงานจริงที่ใช้วางแผน = ค่าที่ตั้งในแผง − วันหยุดส่วนเกินของสัปดาห์นั้น

    ผลลัพธ์สุดท้าย — ผู้เรียกห้ามเอาไปหักวันหยุด/min กับปฏิทินซ้ำอีก"""
    if week is None:
        # ค่ามาตรฐานของกลุ่ม (ไม่อิงสัปดาห์) → ไม่มีวันหยุดให้หัก
        return float(_WORKDAY["default"].get(_group(mc_group, gauge), DEFAULT_WORK_DAYS))

    week = int(week)
    if merged_into(week) is not None:
        return 0.0  # ถูกยุบเข้าสัปดาห์อื่น → ห้ามวางแผนลงสัปดาห์นี้

    grp = _group(mc_group, gauge)
    sources = _MERGE_SOURCES.get(week, [])           # สัปดาห์ที่ยุบเข้ามาที่ปลายทางนี้

    # สัปดาห์ปลายทางของการยุบ: ถ้า user ตั้งค่ารายสัปดาห์ไว้ = ยอดรวมทั้งก้อน ใช้ตรง ๆ
    # เช่นยุบได้ 10 วันตามปฏิทิน แต่เครื่องเดินได้จริง 8 วัน → กรอก 8
    if sources and grp is not None:
        override = _WORKDAY["week"].get(grp + (week,))
        if override is not None:
            return float(override)

    # สัปดาห์นี้ + สัปดาห์ที่ยุบเข้ามา (แต่ละสัปดาห์หักวันหยุดของตัวเองก่อนค่อยรวม)
    return float(sum(_week_days(grp, w) for w in [week] + sources))

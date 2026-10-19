"""
WorkDay.py — วันทำงานรายสัปดาห์ของแต่ละกลุ่มเครื่อง (แหล่งเดียว)

อ่านจาก Calendar.xlsx เท่านั้น:
  ชีท "Work Day"   : Factory | MC_CAT | Guage | WEEK | WORK_DAY | WORK_HOUR
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

ชั่วโมงรายสัปดาห์ (WORK_HOUR ของแถวที่ระบุ WEEK) — ใช้ตอนสัปดาห์นั้นเดินไม่เท่าปกติ
เช่น สัปดาห์ที่ยุบรวมแล้วเร่งเป็น 24 ชม. ทั้งที่กองปกติเดิน 20 ชม. :

    วันทำงานที่คืนออกไป = วันจริง × (ชั่วโมงของสัปดาห์นั้น ÷ ชั่วโมงมาตรฐานของกอง)

  เช่น ยุบได้ 12 วัน กอง 20 ชม. สัปดาห์นี้ 24 ชม. → 12 × 24/20 = 14.4 "วันเทียบเท่า"
  ทำที่นี่จุดเดียวเพราะ CAP ถูกคูณชั่วโมงของกองไว้ตั้งแต่ต้น (Planning._base_work_hours)
  และไม่รู้จักสัปดาห์ — คูณกลับที่วันจึงได้กำลังผลิตของสัปดาห์นั้นถูกต้องโดยไม่ต้องแก้ทั้ง engine
  setup ยังหัก 3/5 วันตามปฏิทินจริงเท่าเดิม (ตั้งเครื่องใช้เวลาเท่าเดิมไม่ว่าจะเดินกี่ชั่วโมง)
  ⚠️ แก้กติกานี้ต้องแก้ webapp/frontend/src/workday.js ให้ตรงกันด้วย (Gantt คำนวณฝั่งจอเอง)

ค่าประจำกลุ่ม (วัน/ชั่วโมง) มาจากแผง "วันทำงานตามกลุ่มเครื่อง" เท่านั้น:
ไม่มีกฎพิเศษ Week 17/32 · ไม่ใช้ Working Day/Working Hours. ของ MasterMC
(working_hour ของ Item Special ยังชนะเสมอ — จัดการที่ Planning/AVA_MC ระดับ item)
"""
import re

import pandas as pd

from Calendar import (DEFAULT_WORK_DAYS, DEFAULT_WORK_HOURS, NORMAL_CAL_DAYS,
                      load_week_merge, load_workday, pin_merge_years)

_WORKDAY: dict = {"default": {}, "week": {}}
_MERGE: dict = {}                 # week ต้นทาง → week ปลายทาง
_MERGE_SOURCES: dict = {}         # week ปลายทาง → [week ต้นทาง ...]
_GROUP_OF: dict = {}              # (MC, GUAGE) → (Factory, MC_CAT, Guage)
_SOLO_MC: dict = {}               # MC → กลุ่ม (เฉพาะเครื่องที่อยู่กลุ่มเดียว — ใช้ตอนไม่รู้เกจ)
_CAL_WD = None                    # callable: week → จำนวนวันที่ปฏิทินเปิดของสัปดาห์นั้น
_TL = None                        # Calendar.WeekTimeline — มี = คีย์สัปดาห์ทั้งหมดเป็น YW (ปี*100+สัปดาห์)


def _wk(week) -> int:
    """คีย์สัปดาห์ที่ใช้ภายใน: มีไทม์ไลน์ = YW (เลขเปล่าถูกตีความเป็นครั้งที่ใกล้วันนี้สุด) ; ไม่มี = เลขเดิม"""
    w = int(week)
    if _TL is not None and w < 100000:
        k = _TL.resolve_bare(w)
        return int(k) if k is not None else w
    return w


def norm_key(v) -> str:
    """ทำให้คีย์เทียบกันได้: ตัดช่องว่าง → ตัวใหญ่, 12.0 → 12 (Excel อ่าน Guage เป็น float)"""
    s = str(v).strip().upper()
    if s in ("NAN", "NONE"):
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def init(master_mc_df: pd.DataFrame, cal_working_days, timeline=None) -> None:
    """โหลดชีท Work Day / Week Merge + ทำ index จาก MasterMC เพื่อแปลง (MC, เกจ) → กลุ่ม

    master_mc_df : DataFrame ของชีท Master MC (ต้องมีคอลัมน์ Factory, MC_CAT, MC, Guage)
    cal_working_days : ฟังก์ชัน week → จำนวนวันที่ปฏิทินเปิดของสัปดาห์นั้น (ใช้เป็นเพดาน)
    timeline : Calendar.WeekTimeline (ไม่บังคับ) — ส่งมา = คีย์สัปดาห์เป็น YW ทั้งหมด
               แถวในชีทที่ไม่ใส่ปีจะถูกผูกกับ "ครั้งที่ใกล้วันนี้สุด" ของเลขสัปดาห์นั้น (ไม่ซ้ำทุกปี)
    """
    global _WORKDAY, _MERGE, _MERGE_SOURCES, _GROUP_OF, _SOLO_MC, _CAL_WD, _TL
    _TL = timeline
    _WORKDAY = load_workday()
    _MERGE = load_week_merge()
    _CAL_WD = cal_working_days
    if _TL is not None:
        _bare = (len({k for _sh in ("week", "week_hour") for k in _WORKDAY.get(_sh, {}) if int(k[3]) < 100000})
                 + sum(1 for s, d in _MERGE.items() if int(s) < 100000 or int(d) < 100000))
        if _bare:
            # หน้าเว็บใส่ปีให้เองตอน backend เริ่ม (workday.ensure_years) — เหลือแปลว่าแก้ไฟล์มือโดยไม่ใส่ปี
            print(f"⚠️ [WORKDAY] ชีท Work Day/Week Merge มีแถวที่ไม่ระบุปี {_bare} แถว "
                  f"→ ตีความเป็นครั้งที่ใกล้วันนี้สุด (ควรใส่คอลัมน์ YEAR)")
        for _sh in ("week", "week_hour"):
            _WORKDAY[_sh] = {k[:3] + (_wk(k[3]),): v for k, v in _WORKDAY.get(_sh, {}).items()}
        _MERGE = pin_merge_years(_MERGE, _TL)

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
            | {k[:3] for k in _WORKDAY.get("week", {})}
            | {k[:3] for k in _WORKDAY.get("week_hour", {})})


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
    week = _wk(week)
    if grp is not None:
        wk = _WORKDAY["week"].get(grp + (week,))
        if wk is not None:
            return float(wk)
    base = float(_WORKDAY["default"].get(grp, DEFAULT_WORK_DAYS)) if grp is not None \
        else float(DEFAULT_WORK_DAYS)
    return _shift_by_calendar(base, week)


def _shift_by_calendar(base: float, week: int) -> float:
    """ค่ามาตรฐาน base วัน/สัปดาห์ เลื่อนตามปฏิทินของสัปดาห์นั้น (กติกาเดียวของทั้งระบบ)"""
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


def _base_hours(grp) -> float:
    """ชั่วโมงมาตรฐานของกลุ่ม (ไม่อิงสัปดาห์) — ฐานที่ CAP ถูกคูณไว้แล้วตั้งแต่ต้น"""
    hr = _WORKDAY.get("hour", {}).get(grp) if grp is not None else None
    return float(hr) if hr is not None else DEFAULT_WORK_HOURS


def get_working_hours(mc_group, gauge=None, week=None) -> float:
    """ชั่วโมงทำงานของกลุ่ม (Factory·MC_CAT·Guage) จากชีท Work Day — แหล่งเดียว

    ระบุ week มา และสัปดาห์นั้นตั้งชั่วโมงไว้เอง → ใช้ค่าของสัปดาห์นั้น
    ไม่ได้ตั้ง / ไม่เจอกลุ่ม → DEFAULT_WORK_HOURS (24 ชม. = ไม่ลดกำลังผลิต)"""
    grp = _group(mc_group, gauge)
    if grp is not None and week is not None:
        wh = _WORKDAY.get("week_hour", {}).get(grp + (_wk(week),))
        if wh is not None:
            return float(wh)
    return _base_hours(grp)


def _hour_ratio(grp, week) -> float:
    """ชั่วโมงของสัปดาห์นั้น ÷ ชั่วโมงมาตรฐานของกอง (1.0 = เดินเท่าปกติ)

    ใช้คูณ "วันทำงาน" เพื่อชดเชยชั่วโมงที่ต่างจากปกติ เพราะ CAP ถูกคูณชั่วโมงของกอง
    ไว้ตั้งแต่ต้นและไม่รู้จักสัปดาห์ (ดู docstring หัวไฟล์)"""
    if grp is None or week is None:
        return 1.0
    wh = _WORKDAY.get("week_hour", {}).get(grp + (week,))
    if wh is None:
        return 1.0
    base = _base_hours(grp)
    if base <= 0:
        return 1.0          # กองตั้งชั่วโมงมาตรฐานเป็น 0 → หารไม่ได้ ไม่ปรับอะไร
    return float(wh) / base


def hour_ratio(mc_group, gauge=None, week=None) -> float:
    """ตัวคูณชั่วโมงของสัปดาห์นั้นสำหรับ (เครื่อง, เกจ) — ให้หน้าเว็บ/ตัวตรวจเรียกดูได้
    get_working_days() คูณให้แล้ว ผู้เรียกห้ามเอาไปคูณซ้ำ"""
    return _hour_ratio(_group(mc_group, gauge), _wk(week) if week is not None else None)


def merged_into(week) -> int | None:
    """สัปดาห์นี้ถูกยุบเข้าสัปดาห์ไหน (None = ไม่ได้ถูกยุบ)"""
    try:
        return _MERGE.get(_wk(week))
    except (TypeError, ValueError):
        return None


def merge_targets() -> list:
    """สัปดาห์ "ปลายทาง" ของการยุบทั้งหมด (สัปดาห์ที่รับก้อนอื่นมารวม)"""
    return sorted(_MERGE_SOURCES)


def merge_cap(week) -> float | None:
    """เพดานวันของสัปดาห์ที่รวบแล้ว = จำนวนวันที่ปฏิทินเปิดรวมทั้งก้อน

    ยุบ W31 (เปิด 6) เข้า W32 (เปิด 6) → กรอกได้ไม่เกิน 12 วัน
    (กรอกน้อยกว่าได้ถ้าเครื่องเดินได้จริงไม่ถึง แต่เกินกว่านี้คือโรงไม่ได้เปิดถึง)
    None = ไม่มีข้อมูลปฏิทินของสัปดาห์นั้น → ไม่ตัดสินว่าเกิน"""
    w = _wk(week)
    bucket = [w] + _MERGE_SOURCES.get(w, [])
    vals = [_cal_of(b) for b in bucket]
    if all(v is None for v in vals):
        return None
    return float(sum(v or 0 for v in vals))


def merge_gaps() -> list:
    """สัปดาห์ที่ยุบรวมแล้ว ยังกรอกไม่ครบ/กรอกเกินเพดานของกองไหนบ้าง
    → [(สัปดาห์, กลุ่ม, ["วัน", ...])]

    ยุบสัปดาห์เข้าด้วยกันแล้ว "วันที่เดินได้จริง" กับ "ชั่วโมงต่อวัน" มักไม่เท่าสัปดาห์ปกติ
    ปล่อยว่างไว้ระบบจะรวมวันให้เองและใช้ชั่วโมงมาตรฐานของกองเงียบ ๆ ซึ่งแยกไม่ออกจาก
    "ผู้วางแผนตั้งใจใช้ค่านั้น" — จึงบังคับให้กรอกทั้งสองช่องก่อน (user สั่ง 24 ก.ย. 2569)
    ด่าน: แผงวันทำงาน (วัน) · หน้าเครื่องจักร (ชั่วโมง) · ก่อนรัน Replan (ทั้งคู่)
    ตรวจเฉพาะสัปดาห์ยุบตั้งแต่สัปดาห์ปัจจุบัน — การยุบของปีที่ผ่านไปแล้วอยู่ในไฟล์ตลอด
    (ผูกปีถาวร) ถ้าตรวจด้วย กองเครื่องที่เพิ่มทีหลังจะทำให้ด่านไม่มีวันผ่าน"""
    wk, wh = _WORKDAY.get("week", {}), _WORKDAY.get("week_hour", {})
    out = []
    for w in sorted(_MERGE_SOURCES):
        if _TL is not None and _TL.pos.get(w, len(_TL.keys)) < _TL.today_idx:
            continue
        cap = merge_cap(w)
        for grp in sorted(_panel_keys()):
            days = wk.get(grp + (w,))
            miss = []
            if days is None:
                miss.append("วัน")
            elif cap is not None and float(days) > cap + 1e-9:
                miss.append(f"วันเกินเพดาน {cap:g}")
            if grp + (w,) not in wh:
                miss.append("ชั่วโมง")
            if miss:
                out.append((w, grp, miss))
    return out


def get_working_days(mc_group, gauge=None, week=None) -> float:
    """วันทำงานจริงที่ใช้วางแผน = ค่าที่ตั้งในแผง − วันหยุดส่วนเกินของสัปดาห์นั้น
    แล้วคูณตัวคูณชั่วโมงของสัปดาห์นั้น (ตั้งชั่วโมงไว้เอง → เป็น "วันเทียบเท่า")

    ผลลัพธ์สุดท้าย — ผู้เรียกห้ามเอาไปหักวันหยุด/min กับปฏิทิน หรือคูณชั่วโมงซ้ำอีก"""
    if week is None:
        # ค่ามาตรฐานของกลุ่ม (ไม่อิงสัปดาห์) → ไม่มีวันหยุดให้หัก
        return float(_WORKDAY["default"].get(_group(mc_group, gauge), DEFAULT_WORK_DAYS))

    week = _wk(week)
    if merged_into(week) is not None:
        return 0.0  # ถูกยุบเข้าสัปดาห์อื่น → ห้ามวางแผนลงสัปดาห์นี้

    grp = _group(mc_group, gauge)
    sources = _MERGE_SOURCES.get(week, [])           # สัปดาห์ที่ยุบเข้ามาที่ปลายทางนี้

    ratio = _hour_ratio(grp, week)   # ชั่วโมงสัปดาห์นี้ต่างจากปกติ → คูณชดเชยที่วัน

    # สัปดาห์ปลายทางของการยุบ: ถ้า user ตั้งค่ารายสัปดาห์ไว้ = ยอดรวมทั้งก้อน ใช้ตรง ๆ
    # เช่นยุบได้ 10 วันตามปฏิทิน แต่เครื่องเดินได้จริง 8 วัน → กรอก 8
    if sources and grp is not None:
        override = _WORKDAY["week"].get(grp + (week,))
        if override is not None:
            return float(override) * ratio

    # สัปดาห์นี้ + สัปดาห์ที่ยุบเข้ามา (แต่ละสัปดาห์หักวันหยุดของตัวเองก่อนค่อยรวม)
    return float(sum(_week_days(grp, w) for w in [week] + sources)) * ratio


def get_outsource_working_days(week, base: float = DEFAULT_WORK_DAYS) -> float:
    """วันทำงานของผู้รับจ้างทอ (S9) ในสัปดาห์นั้น

    ผู้รับจ้างทำงาน base วัน/สัปดาห์ (6 วัน ทุกกลุ่มเครื่อง) และหยุดตามปฏิทินเหมือนโรงเรา
    → ใช้กติกาเลื่อนตามปฏิทิน + ยุบสัปดาห์ชุดเดียวกับกลุ่มในบ้าน แต่ไม่อ่านแผงวันทำงานของเรา
    (ชื่อพูลผู้รับจ้าง เช่น RAP/SKP ซ้ำกับเครื่องในบ้าน ถ้าไปหาในแผงจะได้วันของโรงเรามาแทน)
    ผลลัพธ์สุดท้าย — ผู้เรียกห้ามหักวันหยุดซ้ำ"""
    week = _wk(week)
    if merged_into(week) is not None:
        return 0.0
    return float(sum(_shift_by_calendar(float(base), w)
                     for w in [week] + _MERGE_SOURCES.get(week, [])))

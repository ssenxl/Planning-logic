"""
job_setup_live.py — "โควตา Job Setup" สด ๆ จาก booking สำหรับหน้า Dashboard → Job Setup

แหล่งข้อมูล: ชีท DETAIL ของไฟล์ผลลัพธ์ AVA_MC ใบเดียวกับที่หน้า "เครื่องว่าง" ใช้
             (data_plan/_ava_live.xlsx หรือ booking_final_ready*.xlsx — ใบที่ mtime ใหม่กว่า)
             จึงไม่ต้องรัน AVA_MC ซ้ำ และปุ่ม "คำนวณใหม่" ใช้ตัวเดียวกับหน้าเครื่องว่าง

วิธีนับ job: ลอกสูตรจาก Planning.py (pre-populate weekly_job_usage) แบบตรงตัว
  - _IS_NEW_SETUP = True  → ตั้งเครื่องใหม่ทั้งชุด นับ MC_USE_CEIL
  - _IS_NEW_SETUP = False → เครื่องเดิมยังอุ่น นับเฉพาะ _MC_INCREASE (เครื่องที่เพิ่มจากสัปดาห์ก่อน)
  - MC_GROUP ใน _JOB_EXCLUDE (IMPORT = ผ้านำเข้า) / MC_USE_CEIL = 0 / W99 → ไม่นับ
⚠️ ถ้าคิดสูตรใหม่เองที่นี่ ตัวเลขจะหลุดจากโควตาที่ Planning ใช้จริงเมื่อไหร่ก็ได้

โควตา job/สัปดาห์ อ่านจากหน้า "ตั้งค่าระบบ" (plan_config.job_cap()) — ค่าชุดเดียวกับที่
Planning ใช้จริง (_PREEMPT_JOB_CAP / check_job_capacity_limit) จึงไม่มีเลขซ้ำให้หลุดจากกันอีก

ตัวเลขหน้านี้ = setup ที่ booking ผูกไว้แล้วเท่านั้น ยังไม่รวมงานใหม่ที่ Planning จะวางเพิ่ม
(ต่างจากแถบโหลดบน Gantt ที่รวม NEW ของรอบแผนด้วย)
"""
import sys
from datetime import date, timedelta
from pathlib import Path

import config
import plan_view
import mc_ava_live

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน — ไฟล์อยู่ราก repo)

DETAIL_SHEET = "DETAIL"
OVERFLOW_WEEK = "99"        # sentinel ของ AVA_MC/Planning = "งานล้น" ไม่ใช่สัปดาห์จริง

# กลุ่มที่ไม่ใช่เครื่องทอ — ต้องตัดก่อนนับ ไม่งั้นโควตาเพี้ยน (ตรงกับ _JOB_EXCLUDE_MC ใน Planning)
# อ่านค่าเดียวกันจากหน้า "ตั้งค่าระบบ" ; เป็นฟังก์ชันเพราะ webapp รันค้าง ค่าที่อ่านตอน import
# จะค้างจน restart container
def job_exclude_mc() -> set:
    plan_config.reload()
    return set(plan_config.get_list("KP_JOB_EXCLUDE_MC", upper=True))

# โควตา job ต่อสัปดาห์ต่อประเภทโรงงาน — ค่าชุดเดียวกับที่ Planning ใช้ (plan_config.job_cap())
# เป็นฟังก์ชันเพราะ webapp รันค้าง ถ้าอ่านตอน import จะค้างค่าเก่าจน restart container
def job_cap() -> dict:
    plan_config.reload()
    return plan_config.job_cap()

# MC_GROUP ที่ไม่มีใน MasterMC — Planning (_get_type_key_for_mc / check_job_capacity_limit)
# ถือเป็น PHET_DOUBLE เสมอ จึงต้องนับแบบเดียวกัน ไม่งั้นตัวเลขไม่ตรงกับโควตาที่ระบบใช้จริง
# (แต่ติดธง unknown_mc ไว้ในราย job เพื่อให้เห็นว่ากลุ่มนี้ยังไม่ได้ลงทะเบียนใน MasterMC)
DEFAULT_TYPE = "PHET_DOUBLE"

# ป้ายไทย + ลำดับที่อยากให้โผล่บนตาราง (ประเภทนอกลิสต์ต่อท้ายตามตัวอักษร)
TYPE_LABEL = {
    "PHET_SINGLE": "เพชรบุรี · SINGLE",
    "PHET_DOUBLE": "เพชรบุรี · DOUBLE",
    "OM": "อ้อมน้อย (OMNOI)",
}
TYPE_ORDER = ["PHET_SINGLE", "PHET_DOUBLE", "OM"]

# แคชผลที่แปลงแล้ว: (path, mtime) → dict (อ่าน Excel ทุกครั้งที่เปิดหน้าไม่ไหว)
_cache: dict = {}
# แคชแมป MC_GROUP → type key จาก MasterMC: (path, mtime) → dict
_mc_type_cache: dict = {}


# ---------------------------------------------------------------- MasterMC → type
def _make_type_key(factory: str, mc_type: str) -> str:
    """กติกาเดียวกับ Planning._make_type_key(): OM/OMNOI ไม่มี Type ใช้ชื่อโรงงานอย่างเดียว,
    SINGLE (TUBE) นับรวมกับ SINGLE"""
    if factory in ("OM", "OMNOI"):
        return "OM"
    clean = mc_type.replace("(TUBE)", "").strip() if mc_type else ""
    return f"{factory}_{clean}" if clean else factory


def _mc_type_map() -> dict:
    """{MC_GROUP(upper): type_key} จากชีทแรกของ MasterMC (คอลัมน์ MC / Factory / Type)"""
    p = config.master_files().get("MasterMC")
    if not p or not Path(p).exists():
        return {}
    key = (str(p), Path(p).stat().st_mtime)
    cached = _mc_type_cache.get(key)
    if cached is not None:
        return cached
    out: dict = {}
    try:
        import pandas as pd
        df = pd.read_excel(p)
        cols = {str(c).strip().upper(): c for c in df.columns}
        c_mc, c_fac, c_typ = cols.get("MC"), cols.get("FACTORY"), cols.get("TYPE")
        if c_mc and c_fac:
            for _, r in df.iterrows():
                mc = str(r[c_mc]).strip().upper()
                if not mc or mc == "NAN":
                    continue
                fac = str(r[c_fac]).strip().upper()
                typ = "" if c_typ is None else str(r[c_typ]).strip().upper()
                if typ == "NAN":
                    typ = ""
                out.setdefault(mc, _make_type_key(fac, typ))
    except Exception:
        return {}                     # อ่าน MasterMC ไม่ได้ → ปล่อยว่าง (แถวจะไปกอง UNKNOWN)
    _mc_type_cache.clear()
    _mc_type_cache[key] = out
    return out


# ---------------------------------------------------------------- DETAIL → ตาราง
def _wk_int(w) -> int:
    """เลขสัปดาห์ของคีย์ ('40' / '12@2028' → 40 / 12) ; อ่านไม่ออก = 9999"""
    try:
        return int(str(w).split("@", 1)[0])
    except (TypeError, ValueError):
        return 9999


def _int(v) -> int:
    n = plan_view._num(v)
    if n is None or n != n:           # n != n → NaN
        return 0
    return int(n)


def _today_week() -> tuple:
    """(ปี, สัปดาห์) ของวันนี้ตามนิยามสัปดาห์ของโปรเจกต์ (ศุกร์–พฤหัส → บวก 3 วันก่อน ISO week)"""
    iso = (date.today() + timedelta(days=3)).isocalendar()
    return int(iso[0]), int(iso[1])


def _cell(d: dict, wk: str) -> dict:
    return d.setdefault(wk, {"jobs": 0, "setup": 0, "inc": 0, "rows": 0})


def _build(path: Path) -> dict:
    """อ่าน DETAIL → {weeks, years, types, jobs}

    types = แถวตาราง (ประเภทโรงงาน) พร้อม cells ต่อสัปดาห์ และ groups (แตกย่อยรายกลุ่มเครื่อง)
    jobs  = ราย job ทั้งหมด ให้หน้าเว็บ filter เองตอนคลิกช่อง (ไม่ต้องยิง API ซ้ำ)
    """
    import pandas as pd
    df = pd.read_excel(path, sheet_name=DETAIL_SHEET)
    df.columns = df.columns.astype(str).str.strip().str.upper()
    need = {"MC_GROUP", "WEEK", "ITEM_CODE", "MC_USE_CEIL"}
    if not need <= set(df.columns):
        return {"weeks": [], "types": [], "jobs": [],
                "error": f"ชีท {DETAIL_SHEET} ไม่มีคอลัมน์ที่ต้องใช้"}

    has_flags = "_IS_NEW_SETUP" in df.columns and "_MC_INCREASE" in df.columns
    has_year = "YEAR" in df.columns

    def _c(name):
        return name if name in df.columns else None
    c_g, c_so, c_team = _c("GUAGE"), _c("SO_NO"), _c("TEAM_NAME")
    c_qty, c_desc, c_cat = _c("KP_WEIGHT"), _c("DESCRIPTION"), _c("MC_CAT")
    c_prev, c_days = _c("_PREV_MC_USE_CEIL"), _c("_SETUP_DAYS")

    tmap = _mc_type_map()
    exclude_mc = job_exclude_mc()      # อ่านครั้งเดียวต่อรอบ — ไม่อ่านไฟล์ config ซ้ำทุกแถว
    caps = job_cap()                   # โควตา job ที่ผู้ใช้ตั้งไว้ (ชุดเดียวกับ engine)
    types: dict = {}
    weeks: set = set()
    years: dict = {}
    jobs: list = []

    for _, r in df.iterrows():
        # คีย์สัปดาห์ติดปีเมื่อไกล (plan_view.week_key) — booking ยาวหลายปีแล้ว เลขเปล่าจะรวม W40 สองปีเป็นช่องเดียว
        wk = plan_view.week_key(r["WEEK"], r.get("YEAR") if has_year else None)
        if wk is None or wk == OVERFLOW_WEEK:
            continue                  # W99 = งานล้น ไม่ใช่สัปดาห์จริง
        mcg = str(r["MC_GROUP"]).strip().upper()
        if not mcg or mcg == "NAN" or mcg in exclude_mc:
            continue
        mc_ceil = _int(r["MC_USE_CEIL"])
        if mc_ceil <= 0:
            continue

        # --- จำนวน job ของแถวนี้ (สูตรเดียวกับ Planning) ---
        if has_flags:
            is_new = bool(r["_IS_NEW_SETUP"]) if r["_IS_NEW_SETUP"] == r["_IS_NEW_SETUP"] else True
            n = mc_ceil if is_new else _int(r["_MC_INCREASE"])
        else:
            # ไฟล์รุ่นเก่าไม่มีธง → นับ MC_USE_CEIL ทั้งหมด (overcount) เหมือน fallback ของ Planning
            is_new, n = True, mc_ceil
        if n <= 0:
            continue

        tk = tmap.get(mcg, DEFAULT_TYPE)
        weeks.add(wk)
        if has_year:
            y = _int(r.get("YEAR"))
            if y and wk not in years:
                years[wk] = y

        row = types.setdefault(tk, {
            "type": tk,
            "label": TYPE_LABEL.get(tk, tk),
            "cap": caps.get(tk),
            "cells": {},
            "groups": {},
        })
        c = _cell(row["cells"], wk)
        g = row["groups"].setdefault(mcg, {"mcg": mcg, "cells": {}})
        gc = _cell(g["cells"], wk)
        for t in (c, gc):
            t["jobs"] += n
            t["rows"] += 1
            if is_new:
                t["setup"] += n
            else:
                t["inc"] += n

        jobs.append({
            "week": wk,
            "type": tk,
            "mcg": mcg,
            "gauge": plan_view._gkey(r[c_g]) if c_g else "",
            "cat": str(r[c_cat]).strip() if c_cat else "",
            "item": str(r["ITEM_CODE"]).strip().upper(),
            "so": str(r[c_so]).strip().lstrip("Ss") if c_so else "",
            "team": str(r[c_team]).strip() if c_team else "",
            "desc": str(r[c_desc]).strip() if c_desc else "",
            "jobs": n,
            "kind": "setup" if is_new else "inc",
            "unknown_mc": mcg not in tmap,
            "mc": mc_ceil,
            "prev": _int(r[c_prev]) if c_prev else 0,
            "days": _int(r[c_days]) if c_days else 0,
            "qty": round(float(plan_view._num(r[c_qty]) or 0), 1) if c_qty else 0.0,
        })

    # เรียงสัปดาห์ตามเวลาจริง — กติกาเดียวกับ mc_ava_live._build_table()
    # ไม่มี YEAR (ไฟล์รุ่นเก่า) → สัปดาห์ต้นปีที่โผล่คู่กับสัปดาห์ปลายปี ถือเป็นปีถัดไป ให้ไปอยู่ท้าย
    wrap = (not years
            and any(_wk_int(w) <= 13 for w in weeks)
            and any(_wk_int(w) >= 40 for w in weeks))

    def _wnum(w):
        n = _wk_int(w)
        y = years.get(w)
        return (y, n) if y else (1 if (wrap and n <= 13) else 0, n)

    def _tnum(t):
        return (TYPE_ORDER.index(t["type"]) if t["type"] in TYPE_ORDER else len(TYPE_ORDER),
                t["type"])

    out_types = []
    for row in sorted(types.values(), key=_tnum):
        row["groups"] = sorted(row["groups"].values(), key=lambda g: g["mcg"])
        out_types.append(row)

    jobs.sort(key=lambda j: (_wnum(j["week"]), j["type"], -j["jobs"], j["item"]))

    # ตำแหน่งของ "สัปดาห์นี้" บนแกนที่เรียงแล้ว — หน้าเว็บใช้ตัดสัปดาห์ที่ผ่านมาไปแล้วออก
    # (Planning นับโควตาเฉพาะสัปดาห์ >= TODAY_WEEK ; สัปดาห์เก่าเกินโควตาได้ เพราะจองไปแล้ว)
    # ต้องคิดฝั่ง backend เพราะกติกาเรียงข้ามปีอยู่ใน _wnum — ไฟล์ที่ไม่มีคอลัมน์ YEAR
    # ใช้ heuristic wrap ซึ่งหน้าเว็บคำนวณเองไม่ได้ (W2 ของปีหน้าจะถูกตัดทิ้งผิด ๆ)
    ty, tw = _today_week()
    today_key = (ty, tw) if years else (1 if (wrap and tw <= 13) else 0, tw)
    ordered = sorted(weeks, key=_wnum)
    today_index = sum(1 for w in ordered if _wnum(w) < today_key)
    return {
        "weeks": ordered,
        "today_week": str(tw),
        "today_year": ty,
        "today_index": today_index,
        "years": years,
        "caps": caps,
        "has_flags": has_flags,
        "types": out_types,
        "jobs": jobs,
    }


def table() -> dict:
    """ตารางโควตา Job Setup ล่าสุด + meta บอกที่มา/ความสดของตัวเลข (รูปแบบเดียวกับ mc_ava_live)"""
    # ใช้ตัวเลือกไฟล์ตัวเดียวกับหน้าเครื่องว่าง → สองหน้าอ้างผลลัพธ์ใบเดียวกันเสมอ
    src, path = mc_ava_live._current_source()
    if path is None:
        return {"ok": False, "source": None, "weeks": [], "types": [], "jobs": [],
                "booking": mc_ava_live.booking_meta(),
                "reason": "ยังไม่เคยคำนวณ — กดปุ่ม \"คำนวณใหม่\" เพื่อสรุป Job Setup จาก booking"}

    mtime = path.stat().st_mtime
    cached = _cache.get((str(path), mtime))
    if cached is None:
        try:
            cached = _build(path)
        except Exception as e:                    # noqa: BLE001 — รายงานให้หน้าเว็บทุกกรณี
            return {"ok": False, "source": src, "weeks": [], "types": [], "jobs": [],
                    "booking": mc_ava_live.booking_meta(),
                    "reason": f"อ่านไฟล์ผลลัพธ์ไม่ได้: {e}"}
        _cache.clear()                            # เก็บแค่ผลล่าสุดพอ
        _cache[(str(path), mtime)] = cached

    bk = mc_ava_live.booking_meta()
    stale = bool(bk and bk["mtime"] > mtime)
    out = dict(cached)
    out.update({
        "ok": True,
        "source": src,
        "file": path.name,
        "computed_at": mtime,
        "booking": bk,
        "stale": stale,
        # booking ใหม่กว่าตัวเลข → สั่งคำนวณให้เอง (ใช้กลไก/สถานะร่วมกับหน้าเครื่องว่าง)
        "auto_started": mc_ava_live._maybe_auto_refresh(bk, mtime) if stale else False,
    })
    if not out["types"]:
        out["reason"] = ("ไฟล์ booking ยังไม่มีข้อมูล — ดึง Booking ใหม่แล้วกดคำนวณอีกครั้ง"
                         if (bk and bk["size"] < 20_000) else
                         "ไม่พบงานที่ต้อง Setup ในผลลัพธ์ที่คำนวณได้")
    return out

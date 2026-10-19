"""
program_lock.py — ล็อก "ตั๋วโปรแกรม" อัตโนมัติลงชีท Lock_Plan (MasterMC.xlsx)
=============================================================================================

กติกา (user 8 ก.ย. 2569): งานของ item ที่อยู่ในชีท "Program" ของ MasterMC ให้ถือว่า
**ล็อกแผนไว้ก่อนเสมอ** — Replan ห้ามรื้อไปวางใหม่ จนกว่า user จะไปปลดล็อกเอง
ปลดล็อกได้ "เป็นช่วงสัปดาห์" (W ไหนถึง W ไหน) แล้วรอบถัดไปจะไม่ล็อกกลับให้ในช่วงนั้น

⛔ core item **ไม่ล็อกแล้ว** (user 16 ก.ย. 2569 — กลับคำจาก 15 ก.ย.): ล็อก core ทั้งก้อน
   เท่ากับแผน core ไม่เคยถูกคิดใหม่ตามสต๊อกจริง ซึ่งขัดกับหัวใจของ core เอง (ผ้าสต๊อกต้อง
   **เช็ค target** ทุกรอบแล้วผลิตเมื่อของใกล้ MIN) → core กลับเข้ารอบ replan ตามปกติ
   สิ่งที่เคยหวังจากล็อก (เครื่องไม่หลุดมือระหว่างรอรอบผลิตถัดไป) ย้ายไปทำด้วย "Hold รอ target"
   ในแผนแทน — กันเครื่องไว้ทุกสัปดาห์ที่เว้นระหว่าง 2 รอบผลิต (Planning.py: CORE_HOLD_SOURCE)
   แถว LOCKED_BY = AUTO-CORE ที่ค้างอยู่ในชีทจะถูกเก็บกวาดออกให้เองรอบแรกที่รันไฟล์นี้
   (ชีท Core_Unlock ไม่ถูกแตะ — เป็นบันทึกว่า user เคยปลดอะไรไว้)

⚠️ ล็อกต้อง "เหนียว" (sticky) — user 9 ก.ย. 2569: ตั๋วโปรแกรมต้องออกมา **เหมือนเดิมทุกรอบ**
   ไม่ใช่แค่ไม่ถูกรื้อรอบเดียว ดังนั้นแถว AUTO-* ที่เขียนไปแล้ว **ห้ามเขียนทับด้วย
   ตำแหน่งใหม่** (เดิมลบทิ้งทั้งชุดทุกรอบแล้ว seed ใหม่จากแผนล่าสุด = ล็อกวิ่งตามแผนที่ขยับ
   แทนที่จะตรึงของเดิม → พอแผนขยับไปแล้วรอบหนึ่ง ล็อกก็ไปประทับตำแหน่งใหม่ให้ ไม่เคยยึดอะไรไว้ได้)
   แถวเดิมถูกลบได้แค่ 3 กรณี: user ปลดเอง (ชีททะเบียนปลดล็อก) / สัปดาห์เลยเส้น freeze ไปแล้ว
   (Replan รื้อไม่ได้อยู่แล้ว เก็บไว้ก็รกชีท) / item ถูกเอาออกจากชีท Program / ทะเบียน core

⚠️ ล็อกเฉพาะ "ของที่จองเครื่องแล้ว" — user 9 ก.ย. 2569: งาน SC ที่ยังไม่เข้า booking
   **ห้ามล็อกอัตโนมัติ** ปล่อยให้ Replan เกลี่ยได้ทุกรอบจนกว่ามันจะกลายเป็น booking
   (หรือ user กดล็อกเอง) เหตุผล: ถ้าล็อก SC ใหม่ตั้งแต่รอบแรกที่มันโผล่ มันจะถูกตรึงอยู่
   ตรงที่ระบบวางให้ครั้งเดียวตลอดไป — กระดานตอนนั้นเครื่องถูกบล็อกเก่ากินไปแล้ว SC ใหม่
   จึงมักตกไปคนละสัปดาห์กับก้อนเดิม แล้วค้างอยู่แบบนั้น = ทอไม่ต่อเนื่อง ต้อง setup ซ้ำ
   และไม่มีรอบไหนมาแก้ให้เอง (ดู [[program_auto_lock]] / run_continuity_repair)
   core ใช้นโยบายเดียวกัน (user 15 ก.ย.) — รอบผลิตใหม่ที่ Planning เพิ่งวางยังขยับได้

ตัดสินว่าเป็นตั๋วโปรแกรมด้วย **ITEM_CODE + ทีม ต้องตรงคู่กัน** (กฎเดียวกับที่หน้าเว็บ
ใช้ไฮไลต์ — ดู programTeam() ใน KnitPlan.jsx / bkProgTeam() ใน PlanGantt.jsx)

เขียนอะไรบ้าง (ทุกชีทอยู่ใน MasterMC.xlsx → แก้มือได้จากหน้า Master ของเว็บ)
  Lock_Plan       – แถวล็อกที่ระบบเติมให้ LOCKED_BY = "AUTO-PROGRAM" / "AUTO-CORE"
                    (คอลัมน์เหมือน webapp/backend/plan_lock.py ทุกประการ)
  Program_Unlock  – ทะเบียน "ช่วงสัปดาห์ที่ user ปลดแล้ว" ของตั๋วโปรแกรม → ห้ามล็อกกลับ
  Core_Unlock     – ทะเบียนแบบเดียวกันของ core item
                    ITEM_CODE | WEEK_FROM | WEEK_TO | MC_GROUP | GUAGE | UNLOCKED_BY |
                    UNLOCKED_AT | REMARK
                    - WEEK_FROM/WEEK_TO ว่าง = ไม่จำกัดหัว/ท้ายช่วง
                    - MC_GROUP/GUAGE ว่าง = ทุกเครื่อง (เว็บเขียนว่างไว้เสมอ)
                    เว็บเติมแถวให้เองตอน user กดปลดล็อก (plan_lock.py)

ล็อกฝั่งเดียว — SOURCE=BOOKING:
  บล็อกที่จองเครื่องไว้แล้วในชีท DETAIL ของ booking_final_ready เฉพาะสัปดาห์ที่ Replan
  จะรื้อจริง (idx >= TODAY+2) — ก่อนหน้านั้น Replan ล็อกด้วยเส้นเวลาอยู่แล้ว ไม่ต้องเขียนซ้ำ
  แถว AUTO ที่เป็น SOURCE=PLAN (ของเดิมสมัยที่ยังล็อกฝั่งแผน) จะถูกเก็บกวาดออกให้เอง
  ⚠️ ล็อก SOURCE=PLAN ที่ **user กดเอง** ไม่ถูกแตะ — ของ user คงไว้เสมอ

⚠️ ตำแหน่งในไปป์ไลน์: ต้องรัน **ก่อน Replan** (ดู STEPS ใน run_all.py) — booking_final
   ถูกเขียนโดย AVA_MC ซึ่งอยู่ก่อนหน้าแล้ว ล็อกจึงมีผลกับ Replan ของรอบเดียวกันเลย
   ไม่ต้องรอรอบถัดไป ; เขียนแบบ sticky (ดูด้านบน) จึงรันซ้ำกี่รอบก็ไม่ทำให้ล็อกเดิมเคลื่อน

ปิดการทำงานชั่วคราวได้ทีละชนิดที่หน้า "ตั้งค่าระบบ" (ล็อกตั๋วโปรแกรม / ล็อก core item)
(ชนิดที่ปิด = ไม่เพิ่มล็อกใหม่ และไม่แตะแถวล็อกเดิมของชนิดนั้นเลย)

Usage:
    python program_lock.py              # เขียนชีทจริง
    python program_lock.py --dry-run    # คิดให้ดูอย่างเดียว ไม่แตะ MasterMC
"""
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

import file_guard

import plan_config  # ค่าปรับแต่งที่แก้ได้จากหน้า "ตั้งค่าระบบ" (เปิด/ปิดล็อกอัตโนมัติรายชนิด)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# ใช้ตัวช่วยของ Replan ตรง ๆ (ไฟล์เดียวกัน โฟลเดอร์เดียวกัน) — เส้นเวลาสัปดาห์/การ
# normalize เกจ/การหาไฟล์ล่าสุด ต้องเป็นตรรกะ "ชุดเดียวกัน" กับฝั่งที่ใช้ล็อกจริง
# ไม่งั้นล็อกที่เขียนไปจะจับคู่ไม่ติดตอน Replan อ่าน (Replan import แล้วไม่มี side effect
# — โมดูลระดับบนมีแต่ค่าคงที่ ส่วน main() อยู่ใต้ if __name__)
import Replan

AUTO_BY = "AUTO-PROGRAM"                 # LOCKED_BY ของแถวที่ระบบเติมให้ (ต้องตรงกับ plan_lock.py)
AUTO_REMARK = "ล็อกอัตโนมัติ: ตั๋วโปรแกรม (ชีท Program)"
AUTO_CORE_BY = "AUTO-CORE"               # เลิกล็อกแล้ว — คงชื่อไว้เพื่อ "เก็บกวาด" แถวเก่าออกจากชีท

LOCK_SHEET = "Lock_Plan"
# YEAR ต่อท้าย (ไม่แทรกกลาง — ของเดิมที่อ่านตามตำแหน่งคอลัมน์ยังใช้ได้) ; ว่าง = ครั้งที่ใกล้วันนี้สุด
LOCK_COLUMNS = ["WEEK", "ITEM_CODE", "MC_GROUP", "GUAGE", "SO_NO", "QTY", "MC",
                "SOURCE", "SHEET", "LOCKED_BY", "LOCKED_AT", "REMARK", "YEAR"]

UNLOCK_SHEET = "Program_Unlock"
CORE_UNLOCK_SHEET = "Core_Unlock"        # ไม่เขียนแล้ว — คงชื่อไว้ให้เว็บ/ชีทเก่าอ้างอิงได้
UNLOCK_COLUMNS = ["ITEM_CODE", "WEEK_FROM", "WEEK_TO", "MC_GROUP", "GUAGE",
                  "UNLOCKED_BY", "UNLOCKED_AT", "REMARK", "YEAR_FROM", "YEAR_TO"]

# ชนิดล็อกอัตโนมัติ — ลำดับมีผล: ช่องที่ชนิดก่อนหน้าล็อกไว้แล้ว ชนิดหลังไม่เขียนแถวซ้ำ
# (เหลือชนิดเดียวตั้งแต่เลิกล็อก core — 16 ก.ย. 2569 ดูหัวไฟล์)
KINDS = [
    {"by": AUTO_BY, "label": "ตั๋วโปรแกรม", "remark": AUTO_REMARK,
     "unlock": UNLOCK_SHEET, "env": "KP_PROGRAM_AUTOLOCK",
     "notmember": "ไม่อยู่ในชีท Program แล้ว"},
]
# แถวที่ถือว่า "ระบบเขียนเอง" → ไม่ใช่ล็อกของ user ; AUTO-CORE อยู่ในชุดนี้ทั้งที่ไม่มีชนิดแล้ว
# เพราะไม่มีใครเขียนกลับ = แถวเก่าถูกทิ้งไปเอง (ถ้าไม่ใส่ จะถูกนับเป็นล็อกของ user แล้วค้างตลอดไป)
AUTO_BYS = {k["by"] for k in KINDS} | {AUTO_CORE_BY}

BOOKING_DETAIL_SHEET = "DETAIL"
SRC_BOOKING = "BOOKING"                  # ฝั่งเดียวที่ล็อกอัตโนมัติ (ดูหัวไฟล์)


# ---------------------------------------------------------------- normalize
def _norm(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat", "<na>") else s


def _wk(v):
    """สัปดาห์ → int ('29.0' → 29) ; อ่านไม่ออก = None"""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _yw(week, year=None):
    """(สัปดาห์[, ปี]) → YW (ปี*100+สัปดาห์) — คีย์สัปดาห์ของไฟล์นี้ ; ไม่มีปี = ครั้งที่ใกล้วันนี้สุด"""
    w = _wk(week)
    if w is None:
        return None
    return Replan._timeline().key(w, _wk(year))


def _teams(v) -> list:
    """ช่องทีม ('RTS,NYK1-1') → ['RTS','NYK1-1'] (ตัวใหญ่)"""
    return [t.strip().upper() for t in _norm(v).split(",") if t.strip()]


def _key(week, item, mc_group, gauge, source, year=None) -> tuple:
    """คีย์บล็อก — ต้องตรงกับ key_of() ใน plan_lock.py ทุกตัวอักษร (สัปดาห์เป็น YW)"""
    _k = _yw(week, year)
    return (str(_k if _k is not None else _norm(week)),
            _norm(item).upper(), _norm(mc_group).upper(),
            Replan._norm_gauge(gauge),
            "PLAN" if _norm(source).upper() == "PLAN" else "BOOKING")


def _row_key(r) -> tuple:
    """แถวในชีท Lock_Plan → คีย์บล็อก"""
    return _key(r.get("WEEK"), r.get("ITEM_CODE"), r.get("MC_GROUP"),
                r.get("GUAGE"), r.get("SOURCE"), r.get("YEAR"))


# ---------------------------------------------------------------- อ่านชีทใน MasterMC
def _read_grid(path: Path, sheet: str) -> list:
    """ชีทหนึ่งใน MasterMC → [dict{HEADER: value}] ; ไม่มีชีท = []

    อ่านด้วย openpyxl read_only (ไม่ผ่าน pandas) เพราะแถวล็อกต้องเขียนกลับตามเดิม —
    pandas จะเปลี่ยน WEEK/SO_NO ที่เป็นเลขให้กลายเป็น float ('720188.0') แล้วชีทเพี้ยน
    """
    try:
        wb = load_workbook(path, read_only=True, data_only=True)
    except Exception as e:
        print(f"⚠️ เปิด {path.name} ไม่ได้ ({e})")
        return []
    try:
        if sheet not in wb.sheetnames:
            return []
        rows = list(wb[sheet].iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return []
    header = [_norm(c).upper().replace(" ", "_") for c in rows[0]]
    out = []
    for r in rows[1:]:
        d = {h: (r[i] if i < len(r) else None) for i, h in enumerate(header) if h}
        if any(_norm(v) for v in d.values()):
            out.append(d)
    return out


def _has_sheet(path: Path, sheet: str) -> bool:
    """ไฟล์มีชีทนี้ไหม — ใช้แยก "ชีทว่าง" (user เอา item ออกหมด) ออกจาก "ไม่มีชีท/อ่านไม่ได้"
    สองอย่างนี้ _read_grid คืน [] เหมือนกัน แต่ต้องทำคนละอย่าง (ดู seed)"""
    try:
        wb = load_workbook(path, read_only=True, data_only=True)
    except Exception:
        return False
    try:
        return sheet in wb.sheetnames
    finally:
        wb.close()


def program_map(path: Path) -> dict:
    """ชีท Program → {ITEM_CODE: [ทีม, ...]} (กฎเดียวกับ Planning._item_program_map)"""
    out: dict = {}
    for r in _read_grid(path, "Program"):
        item = _norm(r.get("ITEM_CODE")).upper()
        team = _norm(r.get("TEAM_NAME"))
        if not item or not team:
            continue
        teams = out.setdefault(item, [])
        if team not in teams:
            teams.append(team)
    return out


def load_unlocks(path: Path, sheet: str = UNLOCK_SHEET) -> list:
    """ชีททะเบียนปลดล็อก → [{item, mcg, gauge, wk_from, wk_to}] (ช่วงที่ห้ามล็อกกลับ)"""
    out = []
    for r in _read_grid(path, sheet):
        item = _norm(r.get("ITEM_CODE")).upper()
        if not item:
            continue
        out.append({
            "item": item,
            "mcg": _norm(r.get("MC_GROUP")).upper(),
            "gauge": Replan._norm_gauge(r.get("GUAGE")) if _norm(r.get("GUAGE")) else "",
            "from": _yw(r.get("WEEK_FROM"), r.get("YEAR_FROM")),
            "to": _yw(r.get("WEEK_TO"), r.get("YEAR_TO")),
        })
    return out


# ---------------------------------------------------------------- ช่วงสัปดาห์ที่ปลดแล้ว
def _unlocked(rules: list, item: str, mcg: str, gauge: str, week: int,
              idx_by_week=None) -> bool:
    """บล็อกนี้อยู่ในช่วงที่ user ปลดล็อกไว้ไหม (อยู่ = ห้ามล็อกอัตโนมัติกลับ)

    week / ช่วงในทะเบียน เป็น YW ทั้งหมด → เทียบตำแหน่งบนปฏิทิน (ช่วงคร่อมปีใหม่ถูกต้อง)
    สัปดาห์ที่ไม่อยู่บนปฏิทิน = ถอยไปเทียบค่าตรง ๆ (YW เรียงตามเวลาอยู่แล้ว)
    """
    idx_by_week = idx_by_week or _IdxOf()
    w_idx = idx_by_week.get(week)
    for r in rules:
        if r["item"] != item:
            continue
        if r["mcg"] and r["mcg"] != mcg:
            continue
        if r["gauge"] and r["gauge"] != gauge:
            continue
        lo, hi = r["from"], r["to"]
        lo_i = idx_by_week.get(lo) if lo is not None else None
        hi_i = idx_by_week.get(hi) if hi is not None else None
        if w_idx is not None and (lo is None or lo_i is not None) and (hi is None or hi_i is not None):
            if (lo_i is None or w_idx >= lo_i) and (hi_i is None or w_idx <= hi_i):
                return True
            continue
        if (lo is None or week >= lo) and (hi is None or week <= hi):
            return True
    return False


class _IdxOf:
    """ตัวแทน dict {สัปดาห์: ตำแหน่งบนปฏิทิน} เดิม — รับ YW แล้วถามปฏิทินตรง ๆ"""
    def get(self, week, default=None):
        i = Replan._timeline().idx(week) if week is not None else None
        return default if i is None else i

    def __getitem__(self, week):
        return self.get(week)


# ---------------------------------------------------------------- หาบล็อกที่ต้องล็อก
def _match_team(members: dict, item: str, team_cell) -> str:
    """item + ทีมของแถว ตรงกับทะเบียน (ชีท Program / ทะเบียน core) ไหม → คืนชื่อทีมที่ match

    ทั้งสองฝั่งเก็บหลายทีมคั่นด้วย , ได้ → ตรงกันสักทีมก็ถือว่าใช่ (กฎเดียวกับหน้าเว็บ)
    ทีมของแถวว่าง = ไม่ match (ไม่รู้ว่าเป็นทีมไหน จึงไม่เดาแทน user)
    """
    teams = members.get(item)
    if not teams:
        return ""
    mine = _teams(team_cell)
    if not mine:
        return ""
    hit = [t for t in teams if t.strip().upper() in mine]
    return " , ".join(hit)


def _blocks(df: pd.DataFrame, members: dict, cols: dict, source: str, sheet: str) -> list:
    """DataFrame ของงาน → บล็อกที่ต้องล็อก [{key..., qty, mc, team}] (ยุบตามคีย์บล็อก)

    cols = ชื่อคอลัมน์ของไฟล์นั้น (booking กับไฟล์แผนตั้งชื่อไม่เหมือนกัน)
    QTY รวมทั้งบล็อก ; MC ใช้ค่ามากสุด — เครื่องเป็น "จำนวนเครื่องของบล็อก" ไม่ใช่ยอดสะสม
    (กฎเดียวกับ planMcAgg บนหน้า Gantt ; บวกกันจะได้เลขเครื่องเป็นเท่าตัว)
    """
    if df is None or df.empty:
        return []
    agg: dict = {}
    for r in df.to_dict("records"):
        item = _norm(r.get(cols["item"])).upper()
        if not item:
            continue
        team = _match_team(members, item, r.get(cols["team"]) if cols.get("team") else "")
        if not team:
            continue
        week = _wk(r.get(cols["week"]))
        if week is None:
            continue
        k = (week, item, _norm(r.get(cols["mcg"])).upper(),
             Replan._norm_gauge(r.get(cols["gauge"])))
        try:
            qty = float(r.get(cols["qty"]) or 0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            mc = float(r.get(cols["mc"]) or 0)
        except (TypeError, ValueError):
            mc = 0.0
        cur = agg.setdefault(k, {"week": week, "item": item, "mcg": k[2], "gauge": k[3],
                                 "qty": 0.0, "mc": 0.0, "team": team,
                                 "source": source, "sheet": sheet})
        cur["qty"] += qty
        cur["mc"] = max(cur["mc"], mc)
    return list(agg.values())


def load_booking_detail():
    """ชีท DETAIL ของ booking_final_ready ล่าสุด (WEEK เป็น YW แล้ว) ; ไม่มีไฟล์ = None

    อ่านครั้งเดียวแล้วส่งให้ทุกชนิดล็อก — ไฟล์ booking ใหญ่ ไม่ควรเปิดซ้ำต่อชนิด
    """
    src = Replan._latest_booking_final()
    if not src.exists():
        print(f"⚠️ ไม่พบไฟล์ booking_final ({src.name}) → ข้ามล็อกฝั่ง booking")
        return None
    df = pd.read_excel(src, sheet_name=BOOKING_DETAIL_SHEET)
    df.columns = df.columns.str.strip().str.upper()
    Replan._to_yw(df)  # WEEK + YEAR → YW
    df.attrs["source_name"] = src.name
    return df


def booking_blocks(members: dict, idx_by_week, floor_idx: int, df, label: str) -> list:
    """บล็อกของทะเบียนนี้ในชีท DETAIL ของ booking_final_ready (เฉพาะสัปดาห์ที่ Replan จะรื้อ)"""
    if df is None:
        return []
    blocks = _blocks(df, members, {"week": "WEEK", "item": "ITEM_CODE", "mcg": "MC_GROUP",
                                   "gauge": "GUAGE", "team": "TEAM_NAME",
                                   "qty": "KP_WEIGHT", "mc": "MC_USE_CEIL"},
                     source=SRC_BOOKING, sheet="")
    keep = [b for b in blocks
            if idx_by_week.get(b["week"]) is not None and idx_by_week[b["week"]] >= floor_idx]
    print(f"📄 booking {df.attrs.get('source_name', '')}: บล็อก{label} {len(blocks)} บล็อก "
          f"→ อยู่ในช่วงที่ Replan รื้อได้ {len(keep)} บล็อก")
    return keep


# ---------------------------------------------------------------- เขียนชีท
def _coerce(v):
    """ค่าที่เขียนลงเซลล์ — เลขเก็บเป็นเลข (ชีทอ่านง่าย + pandas ไม่ต้องเดา dtype)"""
    s = _norm(v)
    if s == "":
        return ""
    try:
        f = float(s)
        return int(f) if f == int(f) else f
    except ValueError:
        return s


def _backup_once_a_day(path: Path) -> None:
    """สำรอง MasterMC วันละครั้ง (รอบแรกของวัน) — รันไปป์ไลน์วันละหลายรอบไม่ควรได้ .bak กองโต"""
    bak = path.with_suffix(path.suffix + f".autoprogram.{datetime.now():%Y%m%d}.bak")
    if not bak.exists():
        shutil.copy2(path, bak)


def _write(path: Path, rows: list) -> None:
    """เขียนชีท Lock_Plan ใหม่ทั้งชีท + สร้างชีททะเบียนปลดล็อกที่ยังไม่มี (ชีทอื่นคงเดิม)

    เข้าคิวกับหน้าเว็บก่อน (file_guard) — ชีทนี้คือชีทเดียวกับที่ผู้ใช้กดล็อก/ปลดล็อกบน Gantt
    ถ้าเขียนพร้อมกันโดยไม่เข้าคิว ล็อกฝั่งหนึ่งจะหายทั้งชุด หรือไฟล์ MasterMC พังทั้งไฟล์
    """
    with file_guard.lock(path):
        _write_locked(path, rows)


def _write_locked(path: Path, rows: list) -> None:
    _backup_once_a_day(path)
    wb = load_workbook(path, data_only=False)
    try:
        ws = wb[LOCK_SHEET] if LOCK_SHEET in wb.sheetnames else wb.create_sheet(LOCK_SHEET)
        if ws.max_row > 0:
            ws.delete_rows(1, ws.max_row)
        ws.append(LOCK_COLUMNS)
        for r in rows:
            ws.append([_coerce(r.get(c, "")) for c in LOCK_COLUMNS])
        # ให้ชีททะเบียนปลดล็อกโผล่ในหน้า Master ตั้งแต่ยังไม่มีใครปลด (user แก้มือได้เลย)
        for sheet in (k["unlock"] for k in KINDS):
            if sheet not in wb.sheetnames:
                wb.create_sheet(sheet).append(UNLOCK_COLUMNS)
        with file_guard.atomic_save(path) as tmp:
            wb.save(tmp)
    finally:
        wb.close()


# ---------------------------------------------------------------- แถวล็อกเดิม (sticky)
def _keep_old_auto(rows: list, members: dict, rules: list, idx_by_week,
                   floor_idx: int, taken_keys: set) -> tuple:
    """แถว AUTO-* เดิมของชนิดหนึ่ง → (แถวที่ยังต้องล็อกต่อ, สถิติที่ถูกปลดออก)

    หัวใจของ "ล็อกเหนียว": แถวที่เคยล็อกไว้ต้องคงตำแหน่งเดิมไว้เรื่อย ๆ ไม่ให้ถูกคำนวณใหม่
    จากแผนล่าสุด — ไม่งั้นพอแผนขยับไปแล้วรอบหนึ่ง ล็อกจะย้ายตามไปประทับตำแหน่งใหม่ให้
    (ดูหัวไฟล์) ; ปลดออกได้แค่กรณีที่ "ล็อกต่อไม่มีความหมายแล้ว" เท่านั้น
    taken_keys = ช่องที่ล็อกไว้แล้ว (ของ user + ชนิดก่อนหน้า) → แถวซ้ำช่องถูกเก็บกวาดออก
    """
    keep, drop = [], {"unlock": 0, "past": 0, "notmember": 0, "manual": 0, "broken": 0,
                      "plan": 0}
    for r in rows:
        item = _norm(r.get("ITEM_CODE")).upper()
        week = _yw(r.get("WEEK"), r.get("YEAR"))
        mcg = _norm(r.get("MC_GROUP")).upper()
        gauge = Replan._norm_gauge(r.get("GUAGE"))
        if _norm(r.get("SOURCE")).upper() == "PLAN":
            drop["plan"] += 1                # เลิกล็อกฝั่งแผนแล้ว — งาน SC ต้องเกลี่ยได้ทุกรอบ
            continue                         # (ล็อกฝั่งแผนที่ user กดเองไม่เข้ามาที่นี่ — อยู่ใน manual)
        if item not in members:
            drop["notmember"] += 1           # เอาออกจากทะเบียนแล้ว = ไม่ใช่ของชนิดนี้อีกต่อไป
            continue
        if week is None:
            drop["broken"] += 1              # WEEK อ่านไม่ออก (ถูกพิมพ์มือผิด) — จับคู่ booking ไม่ได้อยู่ดี
            continue
        if _row_key(r) in taken_keys:
            drop["manual"] += 1              # ช่องนี้มีล็อกอยู่แล้ว (user กดเอง / ชนิดก่อนหน้า) — ไม่ต้องมีแถวซ้ำ
            continue
        if _unlocked(rules, item, mcg, gauge, week, idx_by_week):
            drop["unlock"] += 1              # user ปลดช่วงนี้เอง — ห้ามล็อกกลับ
            continue
        idx = idx_by_week.get(week)
        if idx is not None and idx < floor_idx:
            drop["past"] += 1                # เลยเส้น freeze แล้ว Replan รื้อไม่ได้อยู่แล้ว
            continue
        keep.append(r)
    return keep, drop


def _seed_kind(kind: dict, members, old: list, path: Path, idx_by_week, floor_idx: int,
               taken: set, detail, now: str) -> dict:
    """ล็อกอัตโนมัติชนิดหนึ่ง → {rows: แถว AUTO ของชนิดนี้ที่จะเขียน, new: บล็อกใหม่, summary}

    members None = ชนิดนี้ปิดอยู่/อ่านทะเบียนไม่ได้ → คืนแถวเดิมทุกตัวอักษร ไม่เพิ่มไม่ลด
    """
    label = kind["label"]
    if members is None:
        return {"rows": list(old), "new": [],
                "summary": f"{label}: ไม่แตะแถวเดิม {len(old)} แถว"}

    rules = load_unlocks(path, kind["unlock"])
    if rules:
        print(f"🔓 ทะเบียน {kind['unlock']}: {len(rules)} ช่วงที่ user ปลดไว้ → ไม่ล็อกกลับ ("
              + ", ".join(f"{r['item']} {Replan.yw_label(r['from']) if r['from'] is not None else '…'}"
                          f"-{Replan.yw_label(r['to']) if r['to'] is not None else '…'}" for r in rules[:8])
              + (" ..." if len(rules) > 8 else "") + ")")

    # ทะเบียนว่าง = ไม่มีอะไรให้ล็อกเพิ่ม แต่ยังต้องเดินต่อไปจนถึงขั้นปลดล็อกแถวเก่า
    cands = booking_blocks(members, idx_by_week, floor_idx, detail, label) if members else []

    kept, skipped_unlock = [], 0
    for b in cands:
        if _unlocked(rules, b["item"], b["mcg"], b["gauge"], b["week"], idx_by_week):
            skipped_unlock += 1
            continue
        kept.append(b)

    # แถวเดิมของชนิดนี้คงไว้แบบ sticky (ดู _keep_old_auto) — เติมเฉพาะบล็อกที่ยังไม่มีแถวล็อก
    auto_keep, dropped = _keep_old_auto(old, members, rules, idx_by_week, floor_idx, taken)
    auto_keys = {_row_key(r) for r in auto_keep}

    # QTY/MC เป็นข้อมูลอ้างอิงเฉย ๆ (Replan จับคู่ด้วยคีย์ช่อง ไม่ได้ใช้ตัวเลขพวกนี้) —
    # บล็อกไหนยังอยู่ที่เดิม อัปเดตยอดล่าสุดให้ชีทอ่านแล้วตรงกับความจริง แต่ "ตำแหน่ง" ไม่ขยับ
    cand_by_key = {_key(b["week"], b["item"], b["mcg"], b["gauge"], b["source"]): b
                   for b in kept}
    refreshed = 0
    for r in auto_keep:
        b = cand_by_key.get(_row_key(r))
        if b is None:
            continue
        r["QTY"] = round(b["qty"], 2) if b["qty"] else ""
        r["MC"] = int(b["mc"]) if b["mc"] else ""
        refreshed += 1

    rows = [{c: r.get(c, "") for c in LOCK_COLUMNS} for r in auto_keep]
    new_blocks = []
    for b in kept:
        k = _key(b["week"], b["item"], b["mcg"], b["gauge"], b["source"])
        if k in taken:
            continue                        # user/ชนิดก่อนหน้าล็อกช่องนี้อยู่แล้ว — ไม่เขียนทับ
        if k in auto_keys:
            continue                        # ล็อกไว้แล้วรอบก่อน — คงแถวเดิม (sticky)
        new_blocks.append(b)
        rows.append({
            "WEEK": Replan.yw_week(b["week"]), "YEAR": Replan.yw_year(b["week"]),
            "ITEM_CODE": b["item"], "MC_GROUP": b["mcg"],
            "GUAGE": b["gauge"],
            "SO_NO": "",                    # ว่าง = ล็อกทุกใบสั่งในช่องนั้น (ใบสั่งย้ายได้ คีย์ช่องไม่ย้าย)
            "QTY": round(b["qty"], 2) if b["qty"] else "",
            "MC": int(b["mc"]) if b["mc"] else "",
            "SOURCE": b["source"], "SHEET": b["sheet"],
            "LOCKED_BY": kind["by"], "LOCKED_AT": now,
            "REMARK": f"{kind['remark']} — ทีม {b['team']}",
        })

    summary = (f"{label}: คงล็อกเดิม {len(auto_keep)} แถว (จาก {len(old)}) | เพิ่มใหม่ {len(new_blocks)} บล็อก | "
               f"ปลดออก: user ปลดเอง {dropped['unlock']} + เลยเส้น freeze {dropped['past']} + "
               f"{kind['notmember']} {dropped['notmember']} + ช่องมีล็อกอื่นแล้ว {dropped['manual']} + "
               f"เป็นล็อกฝั่งแผน (เลิกล็อกแล้ว) {dropped['plan']} + แถวเสีย {dropped['broken']} | "
               f"ข้ามบล็อกในช่วงที่ user ปลดไว้ {skipped_unlock} | อัปเดตยอดอ้างอิง {refreshed} แถว")
    return {"rows": rows, "new": new_blocks, "summary": summary}


# ---------------------------------------------------------------- ตัวหลัก
def _members_of(kind: dict, path: Path):
    """ทะเบียนของชนิดล็อก → {ITEM: [ทีม]} ; None = ห้ามแตะล็อกชนิดนี้ (ปิดไว้/อ่านทะเบียนไม่ได้)"""
    if not plan_config.get(kind["env"]):
        print(f"⏭️ ปิดการล็อก{kind['label']}อัตโนมัติไว้ (หน้าตั้งค่าระบบ) → ไม่แตะล็อกชนิดนี้")
        return None
    prog = program_map(path)
    if not prog:
        # ไม่มีชีท/อ่านไม่ได้ ≠ ไม่มีตั๋วโปรแกรม — ห้ามเอาไปล้างล็อกที่ค้างอยู่ทิ้ง
        if not _has_sheet(path, "Program"):
            print(f"⚠️ ไม่พบชีท Program ใน {path.name} → ไม่แตะล็อกตั๋วโปรแกรมเดิม")
            return None
        # ชีทมีอยู่แต่ว่าง = user เอา item ออกหมดเอง → เดินต่อเพื่อ "ปลด" ล็อกที่ค้าง
        print(f"⚠️ ชีท Program ใน {path.name} ว่าง → ปลดล็อกตั๋วโปรแกรมที่ค้างอยู่ทั้งหมด")
    else:
        print(f"✅ ชีท Program: {len(prog)} รหัสผ้า")
    return prog


def seed(dry_run: bool = False) -> int:
    path = Replan._master_mc_path()
    if path is None or not path.exists():
        print("❌ ไม่พบ MasterMC.xlsx (config.ini → paths.master_mc) → ข้ามการล็อกอัตโนมัติ")
        return 1

    members = {k["by"]: _members_of(k, path) for k in KINDS}
    # ชีทปัจจุบัน — อ่านก่อนตัดสินใจ "ไม่ทำอะไร" เพราะแถว AUTO-CORE ที่เลิกใช้แล้วต้องถูกกวาดออก
    # แม้ชนิดที่เหลือจะถูกปิดไว้ (ไม่งั้น user ที่ปิดล็อกตั๋วโปรแกรมจะเหลือล็อก core ค้างตลอดไป)
    existing = _read_grid(path, LOCK_SHEET)
    stale_core = [r for r in existing if _norm(r.get("LOCKED_BY")).upper() == AUTO_CORE_BY]
    if all(m is None for m in members.values()) and not stale_core:
        return 0

    tl = Replan._timeline()
    idx_by_week = _IdxOf()
    today_idx = tl.today_idx
    floor_idx = today_idx + Replan.FREEZE_WEEKS_AHEAD
    print(f"📅 สัปดาห์ปัจจุบัน {Replan.yw_label(tl.at(today_idx))} → ล็อกฝั่ง booking ตั้งแต่ "
          f"{Replan.yw_label(tl.at(floor_idx))} เป็นต้นไป")

    # แถวเดิมในชีท: ของ user เก็บไว้ทั้งหมด / ของระบบ (AUTO-*) คงไว้แบบ sticky ต่อชนิด
    manual = [r for r in existing if _norm(r.get("LOCKED_BY")).upper() not in AUTO_BYS]
    taken = {_row_key(r) for r in manual}
    if stale_core:
        print(f"🧹 เก็บกวาดล็อก core อัตโนมัติที่เลิกใช้แล้ว {len(stale_core)} แถว "
              f"(core กลับไปวางตาม target — กันเครื่องช่วงเว้นด้วย 'Hold รอ target' ในแผนแทน)")

    # ทะเบียนว่างทุกชนิด = ไม่มีอะไรให้ล็อกเพิ่ม (ไม่ต้องเสียเวลาอ่าน booking ซึ่งเป็นไฟล์ใหญ่)
    detail = load_booking_detail() if any(members.values()) else None

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    rows = [{c: r.get(c, "") for c in LOCK_COLUMNS} for r in manual]
    results = []
    for kind in KINDS:
        old = [r for r in existing if _norm(r.get("LOCKED_BY")).upper() == kind["by"]]
        res = _seed_kind(kind, members[kind["by"]], old, path, idx_by_week, floor_idx,
                         taken, detail, now)
        rows.extend(res["rows"])
        taken |= {_row_key(r) for r in res["rows"]}
        results.append((kind, res))

    # แถวเดิมที่ยังไม่มีปี → เติมปีที่ตีความได้ตอนนี้ลงชีท (เวลาผ่านไปแล้วจะไม่กลับไปกำกวมว่าปีไหน)
    for r in rows:
        if _norm(r.get("YEAR")) == "":
            _k = _yw(r.get("WEEK"))
            if _k is not None and _k >= 100000:
                r["YEAR"] = _k // 100

    rows.sort(key=lambda r: (str(_yw(r.get("WEEK"), r.get("YEAR")) or 0).zfill(6),
                             _norm(r.get("ITEM_CODE")).upper(),
                             _norm(r.get("SOURCE")).upper()))

    summary = " || ".join(res["summary"] for _k, res in results) + \
        (f" || กวาดล็อก core เก่าออก {len(stale_core)} แถว" if stale_core else "") + \
        f" || ล็อกที่ user กดเอง {len(manual)} แถว คงไว้"

    if dry_run:
        print(f"🧪 dry-run: จะเขียน {len(rows)} แถวลงชีท {LOCK_SHEET} — {summary} "
              f"(ไม่ได้แตะไฟล์จริง)")
        for kind, res in results:
            for b in res["new"][:15]:
                print(f"   + [{kind['label']}] {Replan.yw_label(b['week'])} {b['item']} {b['mcg']} "
                      f"G{b['gauge']} [{b['source']}] ทีม {b['team']} {b['qty']:,.0f} กก.")
            if len(res["new"]) > 15:
                print(f"   ... {kind['label']} อีก {len(res['new']) - 15} บล็อก")
        return 0

    try:
        _write(path, rows)
    except PermissionError:
        print(f"❌ เขียน {path.name} ไม่ได้ (ไฟล์ถูกเปิดค้างใน Excel?) → ล็อกอัตโนมัติไม่ถูกอัปเดต")
        return 1

    print(f"🔐 ล็อกอัตโนมัติ (sticky) รวม {len(rows) - len(manual)} แถว — {summary} "
          f"→ {path.name} ชีท {LOCK_SHEET}")
    return 0


def main() -> int:
    start = datetime.now()
    # --dry-run = คิดให้ดูอย่างเดียว ไม่แตะ MasterMC (ใช้ตรวจก่อนเปิดใช้จริง)
    rc = seed(dry_run="--dry-run" in sys.argv)
    print(f"[DONE] ProgramLock ใช้เวลา {datetime.now() - start}")
    return rc


if __name__ == "__main__":
    sys.exit(main())

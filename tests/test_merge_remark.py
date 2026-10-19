"""
Unit test: REMARK ของแถวที่ทาง MERGE (ยัดยอดลงแถวเดิมของสัปดาห์) เพิ่มเครื่องใหม่เข้าไป

ปัญหาที่เทสต์นี้กัน (เจอ 1 ต.ค. 2569):
  1) แถวที่ใช้เครื่องร่วมกับใบอื่น (MC_SHARED > 0) ถูก "ต่อท้าย" หมายเหตุทุกรอบที่ merge
     → โดน merge 2 รอบได้ "…ไม่ต้อง setup + RAO: setup เครื่องใหม่ 2 ตัว + RAO: setup เครื่องใหม่ 4 ตัว"
     และเมลแผนเปลี่ยน (_setup_note อ่านตัวแรกที่เจอ) นับเครื่องตั้งใหม่ขาด
  2) แถวที่ไม่มี MC_SHARED ไม่ถูกแก้หมายเหตุเลย → เขียน "carry … (ไม่ต้อง setup)"
     ทั้งที่ NEW_MC/SETUP_DAYS มีค่า (แผนจำลอง 1 ต.ค. 2569: FD6TECNF47A0 IBLTA W45 ฯลฯ 4 แถว)
     เมลจึงไม่นับ setup ของแถวพวกนี้เลย

รันแบบ standalone (ไม่ต้องต่อ Oracle / ไม่โหลด Planning.py ทั้งไฟล์):
    python tests/test_merge_remark.py
"""
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PLANNING = ROOT / "Planning.py"
NOTIFY = ROOT / "webapp" / "backend" / "replan_notify.py"

FAILED = []


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)
    assert got == want, name


def _block(src: str, start: str, end: str) -> str:
    return src[src.index(start):src.index(end)]


def _load_planning():
    """ดึง _build_plan_remark + _merge_row_remark ออกมาจาก Planning.py (ไม่ exec ทั้งไฟล์)"""
    ns = {"pd": pd, "re": re}
    exec(_block(PLANNING.read_text(encoding="utf-8"),  # noqa: S102
                "def _build_plan_remark", "def _resolve_carry_key"), ns)
    return ns["_build_plan_remark"], ns["_merge_row_remark"]


def _top_level(src: str, start: str) -> str:
    """โค้ดตั้งแต่ start ถึงก่อนบรรทัดถัดไปที่ไม่เยื้อง (= จบฟังก์ชันระดับบนสุด)"""
    i = src.index(start)
    m = re.search(r"\n(?=\S)", src[i + len(start):])
    return src[i:i + len(start) + m.start()] if m else src[i:]


def _load_setup_note():
    """ตัวสรุป setup ของเมลแผนเปลี่ยน (อ่านจาก REMARK) — ของจริงจาก replan_notify.py"""
    src = NOTIFY.read_text(encoding="utf-8")
    ns = {"re": re}
    exec(_top_level(src, "def _s(v)"), ns)                    # noqa: S102
    exec(_block(src, "_SETUP_RE = ", "def _reasons"), ns)    # noqa: S102
    return ns["_setup_note"], ns["_SETUP_RE"]


_build, merge_remark = _load_planning()
setup_note, SETUP_RE = _load_setup_note()


def _merge(row, new_mc, setup_used, available_machines):
    """จำลองตัวเลขที่ทาง MERGE เขียนลงแถว — สูตรเดียวกับ Planning.py บล็อก [MERGE MC UPDATE]"""
    ex_actual = int(row.get("ACTUAL_MC", 0) or 0)
    if new_mc > 0 and available_machines > ex_actual:
        merged_new = int(row.get("NEW_MC", 0) or 0) + new_mc
        row["ACTUAL_MC"] = int(row.get("CARRYOVER_MC", 0) or 0) + merged_new
        row["NEW_MC"] = merged_new
        row["SETUP_DAYS"] = int(row.get("SETUP_DAYS", 0) or 0) + setup_used
        row["REMARK"] = merge_remark(row)
    return row


def _setup_count(remark):
    return len(re.findall(r"setup เครื่องใหม่", remark))


# ── 1) แถวไม่มี MC_SHARED — เคสที่เจอจริง (เดิมหมายเหตุไม่ถูกแก้เลย) ────────────────
def test_plain_carry_row_gets_setup_text():
    row = {"MC_GROUP": "IBLTA", "ACTUAL_MC": 4, "CARRYOVER_MC": 4, "NEW_MC": 0, "MC_SHARED": 0,
           "MC_BOOKING": 0, "SETUP_DAYS": 0,
           "REMARK": "IBLTA: carry เครื่องเดิม 4 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup)"}
    _merge(row, new_mc=2, setup_used=10, available_machines=6)
    check("แถว carry ล้วน → บอก setup ใหม่ 2 ตัว 10 วัน",
          row["REMARK"],
          "IBLTA: setup เครื่องใหม่ 2 ตัว (10 วัน) + carry เครื่องเดิม 4 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup)")
    check("เมลนับ setup ตรงกับแถว", setup_note([row]), "ต้อง Setup ใหม่ 2 ตัว (เสียเวลา setup 10 วัน)")


# ── 2) แถวเติมเข้าเครื่องเดิมที่ใช้เครื่องร่วม (MC_SHARED) ─────────────────────────────
FILL_REMARK = "เติมเข้าเครื่องเดิม — ใช้เครื่อง 4 ตัวร่วมกับ SO 718381 (RAO) ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup"


def _fill_row():
    return {"MC_GROUP": "RAO", "ACTUAL_MC": 0, "CARRYOVER_MC": 0, "NEW_MC": 0, "MC_SHARED": 4,
            "MC_BOOKING": 0, "SETUP_DAYS": 0, "REMARK": FILL_REMARK}


def test_fill_shared_row_once():
    row = _merge(_fill_row(), new_mc=2, setup_used=4, available_machines=6)
    check("แถวเติมเครื่อง merge 1 รอบ",
          row["REMARK"],
          "RAO: setup เครื่องใหม่ 2 ตัว (4 วัน) + ใช้เครื่อง 4 ตัวร่วมกับ SO 718381 "
          "(ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)")
    check("ACTUAL_MC = เครื่องของแถวเอง (ไม่รวมเครื่องร่วม)", row["ACTUAL_MC"], 2)


def test_fill_shared_row_twice_no_stacking():
    """ตัวที่รายงานมา: merge 2 รอบห้ามซ้อนข้อความ และตัวเลขต้องเป็นยอดสะสมล่าสุด"""
    row = _merge(_fill_row(), new_mc=2, setup_used=4, available_machines=6)
    row = _merge(row, new_mc=2, setup_used=4, available_machines=8)
    check("merge 2 รอบ → ข้อความเดียว ตัวเลขสะสม",
          row["REMARK"],
          "RAO: setup เครื่องใหม่ 4 ตัว (8 วัน) + ใช้เครื่อง 4 ตัวร่วมกับ SO 718381 "
          "(ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)")
    check("มี 'setup เครื่องใหม่' แค่ครั้งเดียว", _setup_count(row["REMARK"]), 1)
    check("ไม่มีข้อความ 'ไม่ใช้เครื่องเพิ่ม' ลอยอยู่หน้าข้อความ setup (ขัดกันเอง)",
          row["REMARK"].startswith("เติมเข้าเครื่องเดิม"), False)
    check("เมลนับ setup = 4 ตัว (เดิมได้ 2 จากตัวแรกที่เจอ)",
          setup_note([row]), "ต้อง Setup ใหม่ 4 ตัว (เสียเวลา setup 8 วัน)")
    # merge ซ้ำ 5 รอบ ข้อความต้องไม่ยาวขึ้นเรื่อย ๆ
    for k in range(3):
        row = _merge(row, new_mc=1, setup_used=2, available_machines=99)
    check("merge 5 รอบ → ยังมี setup ข้อความเดียว", _setup_count(row["REMARK"]), 1)
    check("ตัวเลขสุดท้ายตรงกับคอลัมน์", SETUP_RE.search(row["REMARK"]).groups(),
          (str(row["NEW_MC"]), str(row["SETUP_DAYS"])))


# ── 3) แถวซ้อนเครื่องสัปดาห์เดียวกัน (มีเครื่องของตัวเอง + ร่วมกับหลายใบ) ──────────────
def test_same_week_shared_row_keeps_so_list():
    row = {"MC_GROUP": "SBP", "ACTUAL_MC": 1, "CARRYOVER_MC": 1, "NEW_MC": 0, "MC_SHARED": 2,
           "MC_BOOKING": 0, "SETUP_DAYS": 0,
           "REMARK": "SBP: carry เครื่องเดิม 1 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup) + ใช้เครื่อง 2 ตัวร่วมกับ "
                     "SO 711111, 722222 (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"}
    _merge(row, new_mc=2, setup_used=6, available_machines=5)
    check("คงรายชื่อ SO ที่ใช้เครื่องร่วม",
          row["REMARK"],
          "SBP: setup เครื่องใหม่ 2 ตัว (6 วัน) + carry เครื่องเดิม 1 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup)"
          " + ใช้เครื่อง 2 ตัวร่วมกับ SO 711111, 722222 (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)")


def test_shared_row_without_own_machines():
    """MC_SHARED > 0 แต่ไม่มีเครื่องของตัวเอง = รูปแบบ '<MC>: ใช้เครื่อง …' แบบตอนสร้างแถว"""
    row = {"MC_GROUP": "RAO", "CARRYOVER_MC": 0, "NEW_MC": 0, "MC_SHARED": 3,
           "REMARK": "RAO: ใช้เครื่อง 3 ตัวร่วมกับ SO 700001 (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"}
    check("ไม่มีเครื่องของตัวเอง → หัวเป็นชื่อกลุ่ม", merge_remark(row), row["REMARK"])


def test_shared_count_follows_column():
    """MC_SHARED ถูกปรับทีหลัง (เช่นงานสียึดเครื่อง) → ตัวเลขในข้อความต้องตามคอลัมน์ ไม่ใช่ข้อความเก่า"""
    row = _fill_row()
    row["MC_SHARED"] = 2
    row["NEW_MC"], row["SETUP_DAYS"] = 1, 3
    check("ใช้เลข MC_SHARED ปัจจุบัน",
          merge_remark(row),
          "RAO: setup เครื่องใหม่ 1 ตัว (3 วัน) + ใช้เครื่อง 2 ตัวร่วมกับ SO 718381 "
          "(ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)")


def test_shared_without_so_in_text():
    row = {"MC_GROUP": "RAO", "CARRYOVER_MC": 0, "NEW_MC": 2, "SETUP_DAYS": 4, "MC_SHARED": 3, "REMARK": ""}
    check("หาเลข SO ในข้อความไม่เจอ → 'ใบอื่น'",
          merge_remark(row),
          "RAO: setup เครื่องใหม่ 2 ตัว (4 วัน) + ใช้เครื่อง 3 ตัวร่วมกับใบอื่น (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)")


# ── 4) แถวจ้างทอ — ไม่หัก setup + เหตุผลท้าย " | " ต้องอยู่ครบ ─────────────────────
def test_outsource_reason_suffix_kept():
    row = {"MC_GROUP": "ผู้รับจ้าง A", "ACTUAL_MC": 1, "CARRYOVER_MC": 1, "NEW_MC": 0, "MC_SHARED": 0,
           "SETUP_DAYS": 0,
           "REMARK": "ผู้รับจ้าง A: carry เครื่องเดิม 1 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup) | ส่งจ้างทอ: เครื่องในบ้านเต็ม"}
    _merge(row, new_mc=2, setup_used=0, available_machines=3)
    want = ("ผู้รับจ้าง A: เครื่องใหม่ 2 ตัว (ไม่หัก setup) + carry เครื่องเดิม 1 ตัวจากสัปดาห์ก่อน "
            "(ไม่ต้อง setup) | ส่งจ้างทอ: เครื่องในบ้านเต็ม")
    check("จ้างทอ: ไม่หัก setup + คงเหตุผลท้าย", row["REMARK"], want)
    _merge(row, new_mc=1, setup_used=0, available_machines=9)
    check("จ้างทอ merge ซ้ำ: เหตุผลไม่ซ้อน", row["REMARK"].count("| ส่งจ้างทอ"), 1)
    check("เมลไม่นับจ้างทอเป็น setup", setup_note([row]), "")


# ── 5) เครื่อง booking + ชนิดตัวเลขแปลก ๆ จาก DataFrame ─────────────────────────────
def test_booking_text_same_as_row_creation():
    row = {"MC_GROUP": "SKP", "CARRYOVER_MC": 3, "NEW_MC": 1, "SETUP_DAYS": 3, "MC_BOOKING": 2,
           "MC_SHARED": 0, "REMARK": "x"}
    check("แยก carry ของแผนเอง กับเครื่อง booking เหมือนตอนสร้างแถว",
          merge_remark(row),
          _build("SKP", 3, 1, 3, booking_mc=2))


def test_numeric_types():
    row = {"MC_GROUP": "FA", "CARRYOVER_MC": np.int64(1), "NEW_MC": np.float64(2.0),
           "SETUP_DAYS": np.float64(6.0), "MC_BOOKING": float("nan"), "MC_SHARED": None, "REMARK": None}
    check("numpy/NaN/None → ตัวเลขสะอาด (6 วัน ไม่ใช่ 6.0)",
          merge_remark(row),
          "FA: setup เครื่องใหม่ 2 ตัว (6 วัน) + carry เครื่องเดิม 1 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup)")
    row2 = {"MC_GROUP": "FA", "CARRYOVER_MC": "0", "NEW_MC": "1", "SETUP_DAYS": 2.5, "REMARK": ""}
    check("วัน setup ไม่ลงตัวยังอ่านได้ในเมล", setup_note([{"REMARK": merge_remark(row2)}]),
          "ต้อง Setup ใหม่ 1 ตัว (เสียเวลา setup 2.5 วัน)")


# ── 6) idempotent: เรียกซ้ำกับผลของตัวเองต้องได้ข้อความเดิมทุกเคส ──────────────────────
def test_idempotent_everywhere():
    rows = [
        {"MC_GROUP": "IBLTA", "CARRYOVER_MC": 4, "NEW_MC": 2, "SETUP_DAYS": 10, "MC_SHARED": 0,
         "REMARK": "IBLTA: carry เครื่องเดิม 4 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup)"},
        dict(_fill_row(), NEW_MC=2, SETUP_DAYS=4),
        {"MC_GROUP": "SBP", "CARRYOVER_MC": 1, "NEW_MC": 2, "SETUP_DAYS": 6, "MC_SHARED": 2,
         "REMARK": "SBP: carry เครื่องเดิม 1 ตัวจากสัปดาห์ก่อน (ไม่ต้อง setup) + ใช้เครื่อง 2 ตัวร่วมกับ "
                   "SO 711111, 722222 (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"},
        {"MC_GROUP": "ผู้รับจ้าง A", "CARRYOVER_MC": 1, "NEW_MC": 2, "SETUP_DAYS": 0,
         "REMARK": "x | ส่งจ้างทอ: เครื่องในบ้านเต็ม"},
        {"MC_GROUP": "RAO", "CARRYOVER_MC": 0, "NEW_MC": 2, "SETUP_DAYS": 4, "MC_SHARED": 3, "REMARK": ""},
        {"MC_GROUP": "RAO", "CARRYOVER_MC": 0, "NEW_MC": 0, "MC_SHARED": 3,
         "REMARK": "RAO: ใช้เครื่อง 3 ตัวร่วมกับ SO 700001 (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)"},
    ]
    for i, r in enumerate(rows):
        once = merge_remark(r)
        twice = merge_remark(dict(r, REMARK=once))
        check(f"เรียกซ้ำได้ผลเดิม #{i + 1}", twice, once)


# ── 7) ตรึงโครงสร้างโค้ดจริง: ทาง MERGE ต้องเรียก helper ทุกแถว และห้ามกลับไปต่อท้าย ────────
def test_merge_block_uses_helper():
    src = PLANNING.read_text(encoding="utf-8")
    blk = _block(src, "if new_mc > 0 and available_machines > _existing_actual_mc:", "[MERGE MC UPDATE]")
    check("บล็อก MERGE เรียก _merge_row_remark(_p)", '_p["REMARK"] = _merge_row_remark(_p)' in blk, True)
    check("ไม่มีเงื่อนไขจำกัดเฉพาะแถว MC_SHARED", "if _existing_shared_mc > 0" in blk, False)
    check("ไม่มีการต่อท้าย REMARK เดิมในบล็อก MERGE",
          bool(re.search(r"_p\[\"REMARK\"\]\s*=\s*\(\s*f[\"']\{str\(_p\.get\(['\"]REMARK", blk)), False)


def main():
    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    for t in tests:
        print(f"\n▶ {t.__name__}")
        try:
            t()
        except AssertionError:
            pass
    print(f"\n{'❌ ไม่ผ่าน ' + str(len(FAILED)) + ' ข้อ' if FAILED else '✅ ผ่านทั้งหมด'}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())

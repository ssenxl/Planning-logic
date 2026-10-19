"""
Unit test ของธง COLOR_PART (ก้อนสี) บนแถวแผน

รันแบบ standalone (ไม่ต้องต่อ Oracle / ไม่โหลด Planning.py ทั้งไฟล์):
    python tests/test_color_part_flag.py

ที่ต้องมี test นี้: แถวแผนถูกสร้าง 3 ทาง (แถวหลัก / แถวเติมเข้าเครื่องที่วิ่งอยู่ /
แถว capacity optimization) แต่เดิมมีแค่ทางแรกที่ตั้ง COLOR_PART → ก้อนเดียวกันที่ถูก
วางสองสัปดาห์ได้ธงคนละแบบ ("Yes" กับว่าง) ทำให้
  • หน้าเว็บระบายบล็อกคนละสีทั้งที่เป็นงานก้อนเดียวกัน
  • _main_row_is_color ถอยไปใช้เกณฑ์ระดับ item ซึ่งข้ามกติกา "คิดต่อใบสั่ง"
"""
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PLANNING = ROOT / "Planning.py"
sys.path.insert(0, str(ROOT))

import ColorOpen  # noqa: E402 — ต้องใส่ ROOT ใน path ก่อน


def _src() -> str:
    return PLANNING.read_text(encoding="utf-8")


def _block(src: str, start: str, end: str) -> str:
    return src[src.index(start):src.index(end)]


def _load_color_part_text():
    """ดึงเฉพาะ _color_part_text ออกมาจาก Planning.py (ไม่ exec ทั้งไฟล์)"""
    ns = {"pd": pd, "_COLOR_PART_COL": "_COLOR_PART"}
    exec(_block(_src(), "def _color_part_text", "def _row_sc"), ns)   # noqa: S102
    return ns["_color_part_text"]


def _load_main_row_is_color(color_items, color_qty):
    """ดึง _main_row_is_color + ตัวช่วยที่มันเรียก มาทดสอบบนยอดเปิดสีจำลอง

    _row_is_core ถูก stub (อ่านคอลัมน์ IS_CORE_ITEM ตรง ๆ) เพราะของจริงต้องใช้
    ทะเบียน core ที่โหลดจากไฟล์ภายนอก
    """
    src = _src()
    ns = {
        "pd": pd, "ColorOpen": ColorOpen, "_COLOR_PART_COL": "_COLOR_PART",
        "_COLOR_ITEMS": color_items, "_COLOR_QTY": color_qty, "_COLOR_QTY_ITEM": {},
        "_row_is_core": lambda r: str(r.get("IS_CORE_ITEM", "")).strip() == "CORE ITEM",
    }
    exec(_block(src, "def _row_sc", "def _is_color_item"), ns)                       # noqa: S102
    exec(_block(src, "def _is_color_item", "# ---------- เว้นสัปดาห์รอสีเปิดรอบใหม่"), ns)  # noqa: S102
    exec(_block(src, "def _main_row_is_color", "def _hold_extra_mc"), ns)            # noqa: S102
    return ns["_main_row_is_color"]


_color_part_text = _load_color_part_text()


def test_flag_values():
    """ธง True/False/ไม่มีธง → "Yes"/"No"/ว่าง"""
    assert _color_part_text({}) == ""
    assert _color_part_text({"_COLOR_PART": None}) == ""
    assert _color_part_text({"_COLOR_PART": True}) == "Yes"
    assert _color_part_text({"_COLOR_PART": False}) == "No"
    assert _color_part_text({"_COLOR_PART": np.bool_(True)}) == "Yes"
    assert _color_part_text({"_COLOR_PART": np.bool_(False)}) == "No"


def test_nan_is_blank_not_yes():
    """กับดัก: order เป็น Series ค่าที่ไม่ติดธงกลับมาเป็น NaN ซึ่ง truthy

    เช็คด้วย `is None` อย่างเดียวจะได้ "Yes" ผิดใบ (ใบที่ไม่มีโควตาสีกลายเป็นงานสี
    แล้วได้เกราะกันโดนยึดเครื่องของงานสีไปด้วย)
    """
    assert _color_part_text({"_COLOR_PART": float("nan")}) == ""
    assert _color_part_text({"_COLOR_PART": pd.NA}) == ""
    assert _color_part_text({"_COLOR_PART": pd.NaT}) == ""


def test_series_rows():
    """อ่านจากแถว DataFrame ได้ ทั้งคอลัมน์ object และคอลัมน์ที่ dtype กลายเป็น float"""
    obj = pd.DataFrame({"_COLOR_PART": [True, False, None]})
    assert [_color_part_text(obj.loc[i]) for i in range(3)] == ["Yes", "No", ""]
    flt = pd.DataFrame({"_COLOR_PART": pd.Series([1.0, 0.0, np.nan])})
    assert [_color_part_text(flt.loc[i]) for i in range(3)] == ["Yes", "No", ""]


def test_every_plan_row_sets_the_flag():
    """ทุกที่ที่สร้างแถวแผนต้องตั้ง COLOR_PART ผ่าน _color_part_text

    นับจากจำนวนที่ append แถวแผนจริง ๆ: plans.append / additional_plans.append
    (แถว HOLD สร้างจาก _hold_row ซึ่งตั้ง COLOR_PART = "" ไว้แล้วในตัว)
    """
    src = _src()
    n_calls = len(re.findall(r"_color_part_text\(order\)", src))
    assert n_calls >= 3, (
        f"มีจุดที่เรียก _color_part_text แค่ {n_calls} จุด — "
        "แถวแผนถูกสร้าง 3 ทาง (แถวหลัก / เติมเข้าเครื่องเดิม / capacity optimization) "
        "ทุกทางต้องตั้งธง ไม่งั้นก้อนเดียวกันจะได้ธงคนละแบบ"
    )
    # แถว HOLD ยังต้องมีคอลัมน์ติดไปด้วย (ว่าง = ไม่ใช่งานสี)
    assert '"COLOR_PART": ""' in src
    # ทางที่ 4: แถวที่ยกจาก booking ตรง ๆ (KP / core) สร้างที่ Replan.core_plan_rows
    rp = (ROOT / "Replan.py").read_text(encoding="utf-8")
    assert '"COLOR_PART":' in rp and "_color_part_of_orders" in rp, (
        "Replan.core_plan_rows ต้องตั้ง COLOR_PART ให้แถว KP/core ด้วย — "
        "แถวพวกนี้ไม่ผ่านลูปวางของ Planning จึงไม่มีใครแจกธงให้"
    )


def test_booking_rows_flag_matches_quota():
    """ธงของแถวที่ยกจาก booking = ใบนั้นมียอดเปิดสีไหม (เกณฑ์เดียวกับ _main_row_is_color)"""
    rp = (ROOT / "Replan.py").read_text(encoding="utf-8")
    ns = {"pd": pd, "ColorOpen": ColorOpen}
    exec(rp[rp.index("def _color_part_of_orders"):rp.index("def core_plan_rows")], ns)  # noqa: S102
    f = ns["_color_part_of_orders"]

    quota = {("721436", "ITEMA"): 500.0}
    ColorOpen.quota_by_sc_item = lambda path=None: quota      # แทนที่แหล่งยอดเปิดสีชั่วคราว
    try:
        so = pd.Series(["721436", "721574", "S721436", "721574,721436", ""])
        it = pd.Series(["ITEMA"] * 5)
        assert list(f(so, it)) == ["Yes", "", "Yes", "Yes", ""]
        # ผ้าที่ไม่มีใครเปิดสี → ว่างหมด
        assert list(f(so, pd.Series(["ITEMB"] * 5))) == [""] * 5
        # ยังไม่มีไฟล์ view (quota ว่าง) → ว่างหมด เหมือนพฤติกรรมเดิม
        ColorOpen.quota_by_sc_item = lambda path=None: {}
        assert list(f(so, it)) == [""] * 5
    finally:
        del ColorOpen.quota_by_sc_item   # คืนฟังก์ชันจริงของโมดูล


def test_row_is_color_falls_back_per_order():
    """แถวไม่มีธง → ดูโควตาสี "ของใบนั้น" ไม่ใช่แค่ว่า item มีคนเปิดสีไว้ไหม

    ใบที่ยังไม่เปิดสีเลยของผ้าที่ใบอื่นเปิดสีไว้ ต้องไม่ได้เกราะกันโดนยึดเครื่อง
    ของงานสี (ดู _grab_cap_mc / _main_grab_victims)
    """
    f = _load_main_row_is_color({"ITEMA"}, {("721436", "ITEMA"): 500.0})

    # ธงชนะเสมอ ไม่ว่าโควตาของใบจะเป็นยังไง
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721574", "COLOR_PART": "Yes"}) is True
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721436", "COLOR_PART": "No"}) is False
    # ผ้า core ไม่ใช่งานสีเสมอ
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721436", "IS_CORE_ITEM": "CORE ITEM"}) is False
    # ไม่มีธง → รายใบ
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721436"}) is True       # ใบนี้เปิดสีแล้ว
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721574"}) is False      # ใบนี้ยังไม่เปิด
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "S721436"}) is True      # S นำหน้าก็ต้องเจอ
    assert f({"ITEM_CODE": "ITEMB", "SC_SO_NO": "721436"}) is False      # item ไม่มีสีเลย


def test_row_is_color_keeps_item_level_when_no_sc():
    """แถวที่ไม่มีช่อง SC (booking เก่า) ยังถอยไปใช้ยอดรวมทุกใบของ item เหมือนเดิม

    และช่อง SC ที่ยุบหลายใบด้วยคอมมาต้องถูกแตกก่อนเทียบ — ไม่งั้นแถว booking
    ที่เปิดสีแล้วจะถูกตัดสินว่าไม่ใช่งานสีแบบเงียบ ๆ
    """
    f = _load_main_row_is_color({"ITEMA"}, {("721436", "ITEMA"): 500.0})
    assert f({"ITEM_CODE": "ITEMA"}) is True
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": ""}) is True
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721574,721436"}) is True
    assert f({"ITEM_CODE": "ITEMA", "SC_SO_NO": "721574,721999"}) is False


if __name__ == "__main__":
    test_flag_values()
    test_nan_is_blank_not_yes()
    test_series_rows()
    test_every_plan_row_sets_the_flag()
    test_booking_rows_flag_matches_quota()
    test_row_is_color_falls_back_per_order()
    test_row_is_color_keeps_item_level_when_no_sc()
    print("✅ COLOR_PART flag: ผ่านทุกเคส")

"""
Unit test: นับเครื่องของแผน "ต่อบล็อก" ไม่ใช่ต่อแถว (ฝั่ง webapp)

1 บล็อก = (สัปดาห์, ITEM_CODE, MC_GROUP, MC_GUAGE) ใช้เครื่องชุดเดียว แต่ Planning.py เขียน
เป็นหลายแถว (แถวละใบสั่ง) โดยแปะ ACTUAL_MC / NEW_MC / MC_BOOKING ของทั้งบล็อกซ้ำทุกแถว
→ ตัวไหนบวกทุกแถวจะได้เครื่องเกินจริงเป็นเท่าตัว แล้วเห็นเกจนั้น "ตัน" ทั้งที่ยังว่าง
(ยืนยันกับแผนจริง: F6DRYTECH201/05A0 IBLTA G22 — W36 มี 6 เครื่อง, W37 สองแถวเขียน 8 ทั้งคู่
 → ของจริง 8 = carry 6 + setup ใหม่ 2 ; บวกตรง ๆ ได้ 16 ซึ่งเป็นไปไม่ได้)

รันแบบ standalone:
    python tests/test_mc_block_count.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "webapp" / "backend"))

import cylinder_advisor          # noqa: E402
import plan_view                 # noqa: E402

FAILED = []


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got}\n     want = {want}")
        FAILED.append(name)


COLS = ["PLAN_WEEK", "ITEM_CODE", "MC_GROUP", "CAT", "MC_GUAGE", "ACTUAL_MC", "MC_BOOKING"]
# เคสจริง: บล็อก IBLTA W37 เขียน 2 แถว ACTUAL_MC=8 ทั้งคู่ + แถว MC_SHARED (0)
ROWS = [
    [37, "F6DRY/05A0", "IBLTA", "DOUBLE-30", 22, 8, 0],
    [37, "F6DRY/05A0", "IBLTA", "DOUBLE-30", 22, 8, 0],
    [37, "F6DRY/05A0", "IBLTA", "DOUBLE-30", 22, 0, 0],
    # item อื่น กลุ่มเครื่องเดียวกัน = คนละบล็อก → บวกเพิ่ม
    [37, "OTHER", "IBLTA", "DOUBLE-30", 22, 3, 0],
    # item เดียวกันแต่คนละกลุ่มเครื่อง = คนละบล็อก → บวกเพิ่ม
    [37, "F6DRY/05A0", "SYN", "DOUBLE-30", 22, 2, 0],
    # นั่งบนเครื่อง booking: ACTUAL_MC 4 − MC_BOOKING 4 = ไม่กินพูลเพิ่ม
    [38, "BKITEM", "IBLTA", "DOUBLE-30", 22, 4, 4],
    [38, "BKITEM", "IBLTA", "DOUBLE-30", 22, 4, 4],
]


def test_remain_live():
    print("\n[1] cylinder_advisor._remain_live_by_week")
    # ava: {week: {"CAT|GAUGE": {"remain": n}}}
    ava = {"37": {"DOUBLE-30|22": {"remain": 46}}, "38": {"DOUBLE-30|22": {"remain": 20}}}
    out = cylinder_advisor._remain_live_by_week(COLS, ROWS, ava)
    # W37 ที่แผนจองเพิ่ม = 8 (IBLTA บล็อกซ้ำ) + 3 (OTHER) + 2 (SYN) = 13 → เหลือ 46 − 13 = 33
    check("W37 เหลือ 33 (ไม่ใช่ 25 จากการนับซ้ำ)", out["37"]["DOUBLE-30|22"], 33)
    # W38 นั่งบนเครื่อง booking → ไม่กินพูลเพิ่ม เหลือเท่าเดิม
    check("W38 นั่งบนเครื่อง booking → เหลือ 20 เท่าเดิม", out["38"]["DOUBLE-30|22"], 20)


#  (เคยมีเทสต์ที่ 2 ของ order_color_advisor._plan_by_group ด้วย — ถอดออกพร้อมหน้า Order Color
#   เมื่อ 30 ก.ย. 2569 ; ตรรกะ "max ต่อบล็อก แล้วบวกข้ามบล็อก" ที่ยังใช้จริงถูกคุมด้วยเทสต์ข้างบน
#   และ tests/test_pinned_setup_jobs.py::test_pinned_mc_use)


def main():
    print("=" * 70)
    print("TEST: นับเครื่องต่อบล็อก (ACTUAL_MC/NEW_MC ซ้ำทุกแถวของบล็อกเดียวกัน)")
    print("=" * 70)
    test_remain_live()
    print("\n" + "=" * 70)
    if FAILED:
        print(f"❌ FAIL {len(FAILED)} เคส: {', '.join(FAILED)}")
        return 1
    print("✅ ผ่านทั้งหมด")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Unit test: กติกาวันด้ายเข้า (PO IN ของ booking) ที่ตั้งได้จากหน้า Monitor Yarn

กติกา (user 14 ต.ค. 2569 — ด้ายทุกประเภทเหมือนกัน ไม่แยก local/นำเข้า):
  - PO IN ผ่านไปแล้ว = วางได้เลย (index ต่ำกว่าพื้น TODAY+2 จุดเรียกจึงไม่ดันอะไร)
  - PO IN ยังไม่ถึง  = เริ่มทอได้ที่ "สัปดาห์ที่ด้ายเข้า + KP_YARN_POIN_LAG_WEEKS"
  - KP_YARN_POIN_LAG_WEEKS = 1 (ค่าเริ่มต้น) ต้องได้ผลเท่าโค้ดเดิมที่ฝัง +1 ไว้ทุกกรณี

ดึง get_point_date_start_idx ตัวจริงออกมาจาก Planning.py (ไม่ exec ทั้งไฟล์ — ไม่ต้องต่อ Oracle)
รันแบบ standalone:
    python tests/test_poin_rule.py
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PLANNING = ROOT / "Planning.py"
sys.path.insert(0, str(ROOT))
import plan_config  # noqa: E402

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


# ปฏิทินจำลอง 6 สัปดาห์ ศุกร์–พฤหัส: index 0 = 2/10/2026 (ศุกร์) – 8/10/2026 (พฤหัส)
_starts = pd.date_range("2026-10-02", periods=6, freq="7D")
CAL = pd.DataFrame({
    "WEEK": [40, 41, 42, 43, 44, 45],
    "WEEK_START": _starts,
    "WEEK_END": _starts + pd.Timedelta(days=6),
})


def _load(lag: int):
    ns = {"pd": pd, "calendar_week": CAL, "POIN_LAG_WEEKS": lag}
    exec(_block(PLANNING.read_text(encoding="utf-8"),  # noqa: S102
                "def get_point_date_start_idx", "# ── วันด้ายเข้าครบ (PO IN) ของออร์เดอร์"), ns)
    return ns["get_point_date_start_idx"]


def _old_rule(d):
    """โค้ดเดิมก่อนยกขึ้นหน้า Monitor Yarn: สัปดาห์ที่ด้ายเข้า +1 เสมอ"""
    d = pd.Timestamp(d).normalize()
    rows = CAL.index[(CAL["WEEK_START"] <= d) & (CAL["WEEK_END"] >= d)]
    if len(rows):
        return int(rows[0]) + 1
    before = CAL.index[CAL["WEEK_START"] <= d]
    return None if len(before) == 0 else int(before[-1]) + 1


print("ค่าเริ่มต้นในทะเบียนต้องเท่ากับพฤติกรรมเดิม")
check("KP_YARN_POIN_ON default = เปิด", plan_config.BY_KEY["KP_YARN_POIN_ON"]["default"], True)
check("KP_YARN_POIN_LAG_WEEKS default = 1", plan_config.BY_KEY["KP_YARN_POIN_LAG_WEEKS"]["default"], 1)
check("อยู่กลุ่ม Monitor Yarn", {plan_config.BY_KEY[k]["group"] for k in
                                  ("KP_YARN_POIN_ON", "KP_YARN_POIN_LAG_WEEKS")}, {"Monitor Yarn"})

DATES = ["2026-09-01",   # ก่อนปฏิทิน = ด้ายเข้านานแล้ว
         "2026-10-02",   # วันแรกของ W40 (ศุกร์)
         "2026-10-08",   # วันสุดท้ายของ W40 (พฤหัส)
         "2026-10-13",   # กลาง W41 (อังคาร)
         "2026-11-12",   # วันสุดท้ายของ W45 (ขอบปฏิทิน)
         "2026-12-25"]   # หลังปฏิทิน

print("lag = 1 ต้องได้ผลเท่าโค้ดเดิมทุกวัน")
f1 = _load(1)
for d in DATES:
    check(f"lag1 {d}", f1(d), _old_rule(d))

print("lag = 0 / 2")
f0, f2 = _load(0), _load(2)
check("lag0 ด้ายเข้ากลาง W41 → เริ่ม W41 ได้เลย", f0("2026-10-13"), 1)
check("lag2 ด้ายเข้ากลาง W41 → เริ่ม W43", f2("2026-10-13"), 3)
check("lag0 หลังปฏิทิน → อย่างน้อยแถวถัดจากแถวสุดท้าย (ห้ามก่อนด้ายเข้า)", f0("2026-12-25"), 6)
check("lag2 หลังปฏิทิน → แถวสุดท้าย +2", f2("2026-12-25"), 7)
check("ก่อนปฏิทิน = ไม่บังคับ ไม่ว่า lag เท่าไหร่", (f0("2026-09-01"), f2("2026-09-01")), (None, None))
check("ไม่มีวัน = ไม่บังคับ", (f1(None), f1("nan"), f1("")), (None, None, None))

print("จุดเรียกในรอบ replan ต้องเช็กสวิตช์ POIN_ON ก่อน")
_src = PLANNING.read_text(encoding="utf-8")
check("get_point_date_start_idx ถูกเรียกเฉพาะเมื่อ REPLAN_MODE and POIN_ON และใบไม่ได้ยืมด้ายสต๊อก",
      'get_point_date_start_idx(order.get("POINT_DATE"))\n'
      '            if REPLAN_MODE and POIN_ON and not _yarn_stock else None' in _src, True)
check("Planning อ่านค่าจาก plan_config",
      ('plan_config.get("KP_YARN_POIN_ON")' in _src, 'plan_config.get("KP_YARN_POIN_LAG_WEEKS")' in _src),
      (True, True))

if FAILED:
    print(f"\n❌ ไม่ผ่าน {len(FAILED)} ข้อ")
    sys.exit(1)
print("\n✅ ผ่านทุกข้อ")

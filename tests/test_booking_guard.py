"""
Unit test: ด่านกัน "ดึง booking ตอน view ยังเติมไม่ครบ" ใน View_Booking.py

ปัญหาที่เทสต์นี้กัน (เจอ 9 ต.ค. 2569): view booking ถูกล้างแล้วเติมใหม่ทีละน้อย รอบละ ~70 นาที
  1) ดึงตอนเพิ่งล้าง → 0 แถว → เขียนไฟล์ว่างทับของดี → AVA_MC ล้ม ไม่มีแผนเช้า (2, 5, 7, 8, 9 ต.ค.)
  2) ดึงตอนกำลังเติม → ได้บางส่วน (รอบ 12:00 ได้ 949 จาก ~4,135 แถว) → pipeline ออกแผนจาก booking 23% เงียบ ๆ
ต้องการ: ไม่ผ่านด่าน = exit 1 และ "ไฟล์เดิมต้องอยู่ครบ" ; ผ่าน = เขียนไฟล์ตามปกติ

ใช้ DB ปลอม (ไม่ต่อ Oracle) รันแบบ standalone:
    python tests/test_booking_guard.py
"""
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

import View_Booking as vb  # noqa: E402

FAILED = []
T1 = datetime(2026, 10, 9, 15, 30)
T2 = datetime(2026, 10, 9, 15, 31)


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)


class _Cur:
    """cursor ปลอม: คำสั่งนับแถวคืนสถานะ view ตามลำดับที่ตั้งไว้ · คำสั่งดึงจริงคืนแถวตามจำนวนที่ตั้งไว้"""

    def __init__(self, states, n_fetch):
        self.states, self.n_fetch, self._rows, self._one = list(states), n_fetch, [], None
        self.arraysize = 0

    def execute(self, sql, *a, **k):
        if sql.startswith("SELECT COUNT(*)"):
            self._one = self.states.pop(0)
        else:
            self._rows = [tuple(f"v{i}" for _ in vb.SELECT_COLUMNS) for i in range(self.n_fetch)]

    def fetchone(self):
        return self._one

    def fetchmany(self, n):
        out, self._rows = self._rows[:n], self._rows[n:]
        return out

    def close(self):
        pass


class _Conn:
    def __init__(self, states, n_fetch):
        self._cur = _Cur(states, n_fetch)

    def cursor(self):
        return self._cur

    def close(self):
        pass


def _run(tmp, states, n_fetch, prev_rows=None, force=False, history=False):
    """รัน vb.main() กับ view ปลอม → (exit code, จำนวนแถวของไฟล์หลังรัน หรือ None ถ้าไม่มีไฟล์)"""
    out = Path(tmp) / "view_booking.xlsx"
    if out.exists():
        out.unlink()
    if prev_rows is not None:
        pd.DataFrame({"CAT": ["old"] * prev_rows}).to_excel(out, index=False)
    vb.OUTPUT_FILE = str(out)
    vb._SETTLE_SEC = 0
    vb._FORCE = force
    vb._ENV_OUT = str(out) if history else ""
    vb._connect = lambda: _Conn(states, n_fetch)
    rc = vb.main()
    return rc, (len(pd.read_excel(out)) if out.exists() else None)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        print("view ว่าง / กำลังเติม → ไม่ทับไฟล์เดิม")
        check("0 แถว → exit 1, ไฟล์เดิม 4,000 แถวอยู่ครบ",
              _run(tmp, [(0, None)], 0, prev_rows=4000), (1, 4000))
        check("0 แถว และยังไม่เคยมีไฟล์ → exit 1, ไม่สร้างไฟล์ว่าง",
              _run(tmp, [(0, None)], 0), (1, None))
        check("จำนวนแถวขยับระหว่างรอ (949 → 1,010) → exit 1, ไฟล์เดิมอยู่ครบ",
              _run(tmp, [(949, T1), (1010, T2)], 1010, prev_rows=4000), (1, 4000))
        check("จำนวนแถวเท่าเดิมแต่ ENTRY_DATE ล่าสุดขยับ → exit 1",
              _run(tmp, [(4000, T1), (4000, T2)], 4000, prev_rows=4000), (1, 4000))
        check("นับได้ 4,000 แต่ดึงได้ 3,100 (view ถูกล้างระหว่างดึง) → exit 1, ไฟล์เดิมอยู่ครบ",
              _run(tmp, [(4000, T1), (4000, T1)], 3100, prev_rows=4000), (1, 4000))

        print("view นิ่งและครบ → เขียนไฟล์")
        check("นิ่ง 4,135 แถว ทับไฟล์เดิม 4,532 แถว (ขึ้นสัปดาห์ใหม่ ยอดลด 9%) → exit 0",
              _run(tmp, [(4135, T1), (4135, T1)], 4135, prev_rows=4532), (0, 4135))
        check("นิ่ง 4,135 แถว ยังไม่เคยมีไฟล์ → exit 0",
              _run(tmp, [(4135, T1), (4135, T1)], 4135), (0, 4135))
        check("ไฟล์เดิมเป็นไฟล์ว่าง (0 แถว จากบั๊กเดิม) → ทับได้",
              _run(tmp, [(4135, T1), (4135, T1)], 4135, prev_rows=0), (0, 4135))
        check("ไฟล์เดิมเป็นไฟล์ไม่ครบ 949 แถว → ของครบ 4,135 แถวทับได้",
              _run(tmp, [(4135, T1), (4135, T1)], 4135, prev_rows=949), (0, 4135))

        print("นิ่งแต่น้อยกว่าครึ่งของไฟล์เดิม (เติมค้างกลางทาง)")
        check("นิ่ง 949 แถว เทียบไฟล์เดิม 4,532 → exit 1, ไฟล์เดิมอยู่ครบ",
              _run(tmp, [(949, T1), (949, T1)], 949, prev_rows=4532), (1, 4532))
        check("เหมือนกันแต่ตั้ง KP_BOOKING_FORCE → ทับได้",
              _run(tmp, [(949, T1), (949, T1)], 949, prev_rows=4532, force=True), (0, 949))
        check("โหมดดึงย้อนหลัง (KP_BOOKING_OUT) ไม่เทียบกับไฟล์เดิม → ทับได้",
              _run(tmp, [(949, T1), (949, T1)], 949, prev_rows=4532, history=True), (0, 949))
        check("โหมดดึงย้อนหลังก็ยังต้องนิ่ง: แถวขยับ → exit 1",
              _run(tmp, [(949, T1), (1010, T2)], 1010, prev_rows=4532, history=True), (1, 4532))

    print()
    if FAILED:
        print(f"❌ ไม่ผ่าน {len(FAILED)} ข้อ: {FAILED}")
        sys.exit(1)
    print("✅ ผ่านทุกข้อ")


if __name__ == "__main__":
    main()

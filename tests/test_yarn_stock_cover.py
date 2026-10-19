"""
Unit test: ใช้ด้ายในสต๊อกแทนการรอ PO IN (YarnStockCover.py)

กติกาที่ user ตัดสิน 14 ต.ค. 2569:
  - ใบต้องได้ด้าย "ทุกเส้นที่ยังรอ" ทั้งใบ ขาดเส้นเดียว = ไม่ยืมเลย
  - งานสีได้ก่อน แล้วกำหนดทอเสร็จเร็วก่อน
  - "ต้องไม่แย่งกัน เส้นด้ายจะเปลี่ยนไปตามแผน" — สต๊อกลดลงตามสัปดาห์ที่แผนเอาไปใช้
    ใบที่ยืมต้องถือยอดไว้ตั้งแต่สัปดาห์แรกที่วางได้จนก่อน PO ของตัวเองใช้ได้ คงเหลือห้ามติดลบ

รันแบบ standalone (ไม่ต้องต่อ Oracle):
    python tests/test_yarn_stock_cover.py
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import YarnStockCover as ysc  # noqa: E402

FAILED = []


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)
    assert got == want, name


def cand(key, needs, color=False, due=50):
    return {"key": key, "color": color, "due": due, "needs": needs}


F = 10      # สัปดาห์แรกที่วางได้

print("select_borrowers — ยืมได้เมื่อคงเหลือไม่ติดลบตลอดช่วงที่ถือ")
# สต๊อก A 1,000 ; แผนกิน A 600 ที่สัปดาห์ 12
cons = {"A": {12: 600.0}}
check("ยืม 400 ถือถึงสัปดาห์ 13 ได้ (คงเหลือ 1000-600-400 = 0)",
      list(ysc.select_borrowers([cand("x", {"A": (400.0, 13)})], cons, {"A": 1000.0}, F)), ["x"])
check("ยืม 401 ไม่ได้ (สัปดาห์ 12 ติดลบ)",
      list(ysc.select_borrowers([cand("x", {"A": (401.0, 13)})], cons, {"A": 1000.0}, F)), [])
check("ยืม 900 ได้ถ้า PO ตัวเองมาก่อนแผนกิน (ถือถึงสัปดาห์ 11)",
      list(ysc.select_borrowers([cand("x", {"A": (900.0, 11)})], cons, {"A": 1000.0}, F)), ["x"])

print("ไม่แย่งกัน — ใบหลังเห็นยอดที่ใบก่อนยืมไปแล้ว")
cs = [cand("x", {"A": (300.0, 13)}, due=20), cand("y", {"A": (300.0, 13)}, due=30)]
check("x ได้ y ไม่ได้ (1000-600-300 = 100 < 300)",
      list(ysc.select_borrowers(cs, cons, {"A": 1000.0}, F)), ["x"])
check("ช่วงที่ถือไม่ซ้อนกัน ยืมได้ทั้งคู่",
      sorted(ysc.select_borrowers([cand("x", {"A": (500.0, 10)}), cand("y", {"A": (500.0, 11)})],
                                  {}, {"A": 1000.0}, F)), ["x", "y"])

print("ลำดับ: งานสีก่อน แล้วกำหนดส่งเร็วก่อน")
cs = [cand("late", {"A": (800.0, 12)}, due=5), cand("color", {"A": (800.0, 12)}, color=True, due=40)]
check("งานสีได้ก่อนแม้กำหนดส่งช้ากว่า", list(ysc.select_borrowers(cs, {}, {"A": 1000.0}, F)), ["color"])
cs = [cand("b", {"A": (800.0, 12)}, due=30), cand("a", {"A": (800.0, 12)}, due=20)]
check("ไม่ใช่งานสีทั้งคู่ — กำหนดส่งเร็วได้ก่อน", list(ysc.select_borrowers(cs, {}, {"A": 1000.0}, F)), ["a"])

print("ต้องได้ทุกเส้น")
c = cand("x", {"A": (100.0, 12), "B": (100.0, 12)})
check("B ไม่มีในสต๊อก → ไม่ยืมเลย", list(ysc.select_borrowers([c], {}, {"A": 1000.0}, F)), [])
check("B ไม่พอ → ไม่ยืมเลย", list(ysc.select_borrowers([c], {}, {"A": 1000.0, "B": 50.0}, F)), [])
r = ysc.select_borrowers([c], {}, {"A": 1000.0, "B": 500.0}, F)
check("พอทั้งสองเส้น → ยืมทั้งคู่", r, {"x": {"A": 100.0, "B": 100.0}})
check("เส้นที่ช่วงถือว่าง (สุดท้าย < สัปดาห์แรก) ไม่ต้องเช็ก",
      list(ysc.select_borrowers([cand("x", {"A": (100.0, 12), "B": (9e9, F - 1)})], {}, {"A": 1000.0}, F)), ["x"])
r = ysc.select_borrowers([cand("x", {"A": (100.0, 12), "B": (9e9, F - 1)})], {}, {"A": 1000.0}, F)
check("ยอดยืมไม่รวมเส้นที่ช่วงถือว่าง", r, {"x": {"A": 100.0}})


print("plan_consumption — แถวงานใหม่กินด้ายจากสต๊อก ยกเว้นเส้นที่มี PO ของตัวเองกำลังมา")


class TL:
    """ไทม์ไลน์จำลอง: สัปดาห์ W (ปี 2026) → index W"""
    def idx(self, week, year=None):
        try:
            return int(week)
        except (TypeError, ValueError):
            return None


A, B = ("A", ""), ("B", "")
TD1, TD2 = ("PCTD", "BPM8364"), ("PCTD", "BPM4659")
ylines = {
    111: {A: {"pct": 60.0, "loss": 1.0, "po_in": None},
          B: {"pct": 40.0, "loss": 1.1, "po_in": pd.Timestamp("2026-11-11")}},
    222: {A: {"pct": 100.0, "loss": 1.0, "po_in": None}},
    333: {TD1: {"pct": 100.0, "loss": 1.0, "po_in": None}},
}
plan = pd.DataFrame([
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 1000, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "111"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 200, "PLAN_WEEK": 13, "PLAN_YEAR": 2026, "SC_LINE_ID": "111,222"},
    {"PLAN_SOURCE": "KP_KEEP", "PRODUCE_QTY": 999, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "222"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 999, "PLAN_WEEK": 9, "PLAN_YEAR": 2026, "SC_LINE_ID": "222"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 50, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "333"},
])
cons = ysc.plan_consumption(plan, ylines, TL().idx, F)
check("A: W12 = 1000×60% ; W13 = 100×60% + 100×100% (แถวยุบ 2 บรรทัดแบ่งครึ่ง)",
      {w: round(q, 2) for w, q in cons[A].items()}, {12: 600.0, 13: 160.0})
check("B มี PO ตัวเองกำลังมา → ไม่กินสต๊อก ; KP_KEEP / ก่อนสัปดาห์แรก ไม่นับ", B in cons, False)
check("ด้าย TD กินสต๊อกของสีตัวเองเท่านั้น", (cons.get(TD1), TD2 in cons), ({12: 50.0}, False))

print("pick_lots — 1 ใบ = 1 lot เท่าที่ทำได้ (user 16 ต.ค. 2569 ; เดิมเก่าก่อนผสมได้ไม่จำกัด)")
P = ysc.pick_lots
check("lot เก่าสุดที่พอทั้งใบ (ข้าม lot เก่าที่เล็กเกิน)", P([18.0, 45.0, 2636.0, 10007.0], 1186.0), [(2, 1186.0)])
check("ใบเล็กได้เศษ lot เก่าที่พอดี", P([18.0, 45.0, 2636.0], 15.0), [(0, 15.0)])
check("ไม่มี lot ไหนพอ → น้อย lot ที่สุด: ใหญ่สุดทั้ง lot + ที่เหลือจาก lot เก่าสุดที่พอ",
      P([30.0, 100.0, 250.0, 200.0], 300.0), [(1, 50.0), (2, 250.0)])
check("ต้อง 3 lot: 120 + 100 ทั้ง lot + 80 จาก lot ที่เหลือ", P([100.0, 120.0, 90.0], 300.0),
      [(0, 100.0), (1, 120.0), (2, 80.0)])
check("รวมทุก lot ไม่พอ → ได้เท่าที่มี", P([10.0, 0.0, 20.0], 50.0), [(0, 10.0), (2, 20.0)])
check("ไม่ต้องใช้ → ว่าง", P([10.0], 0.0), [])

print("allocate_lots — lot เก่าสุดที่พอทั้งใบ, ไม่มี lot ไหนพอ = น้อย lot ที่สุด, lot เดียวห้ามแบ่งเกินยอด")
lots = {TD1: [{"lot": "OLD", "kg": 100.0, "age": 300}, {"lot": "MID", "kg": 200.0, "age": 100},
              {"lot": "NEW", "kg": 500.0, "age": 5}]}
r = ysc.allocate_lots(["x", "y"], {"x": {TD1: 250.0}, "y": {TD1: 300.0}}, lots)
check("x ได้ NEW 250 lot เดียว (OLD / MID ไม่พอทั้งใบ)", r["x"], {TD1: [("NEW", 250.0)]})
check("y: ไม่มี lot ไหนพอ 300 → NEW ที่เหลือ 250 + OLD 50 (2 lot ไม่แบ่ง lot ซ้ำ)", r["y"],
      {TD1: [("OLD", 50.0), ("NEW", 250.0)]})
check("ของต้นทางไม่ถูกแก้", [d["kg"] for d in lots[TD1]], [100.0, 200.0, 500.0])
r = ysc.allocate_lots(["x"], {"x": {TD2: 10.0}}, lots)
check("สีที่ไม่มี lot ในสต๊อก → ไม่ได้", r, {})
print("ป้ายต้องบอก lot")
n = ysc.note_of({TD1: [("OLD", 100.0), ("MID", 150.0)], A: [("L1", 20.0)]}, {TD1: pd.Timestamp("2026-11-21")})
check("ข้อความป้าย", n, "ใช้ด้ายสต๊อก: PCTD สี BPM8364 lot OLD 100 kg + lot MID 150 kg (แทน PO 21/11/2026)"
      " · A lot L1 20 kg")

print("mark — ติดป้ายเฉพาะแถวที่มีวันด้ายเข้าของใบที่ยืมได้")
d = pd.DataFrame([
    {"SO_NO": "722049", "FABRIC_ITEM": "FD1", "POINT_DATE": "2026-11-03"},
    {"SO_NO": "722049.0", "FABRIC_ITEM": "fd1", "POINT_DATE": "2026-11-05"},
    {"SO_NO": "722049", "FABRIC_ITEM": "FD1", "POINT_DATE": None},
    {"SO_NO": "999999", "FABRIC_ITEM": "FD1", "POINT_DATE": "2026-11-03"},
])
m = ysc.mark(d, {("722049", "FD1"): "ใช้ด้ายสต๊อก: A 100 kg"})
check("2 แถวแรกได้ป้าย (SO .0 / ตัวพิมพ์เล็ก ก็จับคู่ได้) แถวไม่มีวันด้ายเข้า/ใบอื่น ไม่ได้",
      m["YARN_STOCK"].tolist(), ["ใช้ด้ายสต๊อก: A 100 kg", "ใช้ด้ายสต๊อก: A 100 kg", "", ""])

print("lines_of")
check("รูปแบบต่าง ๆ", [ysc.lines_of("775834"), ysc.lines_of(775834.0), ysc.lines_of("1, 2"), ysc.lines_of(None)],
      [[775834], [775834], [1, 2], []])

if FAILED:
    print(f"\n❌ ไม่ผ่าน {len(FAILED)} ข้อ")
    sys.exit(1)
print("\n✅ ผ่านทุกข้อ")

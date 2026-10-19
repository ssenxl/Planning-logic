"""
Unit test: หน้า 🧶 แผนด้าย (webapp/backend/yarn_plan.py — build)

user ขอ 15 ต.ค. 2569: เห็นเลยว่าแต่ละสัปดาห์แผนทอใช้ด้ายอะไร เท่าไหร่ มาจากแผนทอ
  ช่อง = kg ที่ใช้ + มาจากไหน (สต๊อก / ล็อก / PO ของใบ / ออก KP แล้ว) + คงเหลือ

รันแบบ standalone (ไม่ต้องมีไฟล์ข้อมูล):
    python tests/test_yarn_plan.py
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "webapp" / "backend"))
import yarn_plan  # noqa: E402

FAILED = []


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)
    assert got == want, name


A, B = ("A", ""), ("B", "")
PIN = pd.Timestamp("2026-11-20")
YL = {
    1: {A: {"pct": 50.0, "loss": 1.0, "po_in": None}, B: {"pct": 50.0, "loss": 1.0, "po_in": PIN, "pos": ["PB"]}},
    2: {A: {"pct": 100.0, "loss": 1.1, "po_in": None}},
}


def row(qty, wk, line, so="100", item="F1", src="NEW", ys="", yr=2026, ot="SC-ORDERS"):
    return {"PRODUCE_QTY": qty, "PLAN_WEEK": wk, "PLAN_YEAR": yr, "SC_LINE_ID": line, "SC_SO_NO": so,
            "ITEM_CODE": item, "PLAN_SOURCE": src, "YARN_STOCK": ys, "CUSTOMER": "ลูกค้า", "ORDER_TYPE": ot}


plan = pd.DataFrame([
    row(1000, 44, "1"),                                   # A 500 สต๊อก · B 500 PO ของใบ
    row(200, 45, "1", ys="ใช้ด้ายสต๊อก: B lot X"),          # แผนให้ยืมสต๊อก → B นับเป็นสต๊อก
    row(100, 45, "2", so="200", item="F2"),               # A 110 (สูญเสีย 1.1)
    row(400, 46, "2", so="200", item="F2", src="KP_KEEP"),  # ออก KP แล้ว
    row(300, 46, "", so="300", item="F3", ot="YD-ORDERS"),  # ไม่มีบรรทัดใบ = ไม่รู้ด้าย
    row(0, 46, "1"),                                      # ไม่ได้ผลิต = ข้าม
    row(50, 1, "2", so="200", item="F2", yr=2027),        # ข้ามปี → เรียงท้าย
])
LOTS = {A: [{"lot": "LA1", "kg": 550.0, "age": 300}, {"lot": "LA2", "kg": 100.0, "age": 10}]}   # รวม 650 > ใช้ได้ 600
d = yarn_plan.build(plan, YL, {A: 600.0}, {("200", "F2"): {A: 50.0}}, lots=LOTS)

print("สัปดาห์ / ด้าย")
check("สัปดาห์เรียงตามปี", [(w["y"], w["w"]) for w in d["weeks"]], [(2026, 44), (2026, 45), (2026, 46), (2027, 1)])
ys = {(y["yarn"], y["color"]): y for y in d["yarns"]}
a, b = ys[A], ys[B]
print("ที่มาของด้าย")
check("A ใช้ต่อสัปดาห์ (W45 = 100 ยืม + 110 ; W46 = KP 440 ; 2027W1 = 55)", a["use"], [500.0, 210.0, 440.0, 55.0])
check("A สต๊อก — ใบ 200 กินล็อก 50 ก่อน (W45 110 → 60)", a["stock"], [500.0, 160.0, 0.0, 55.0])
check("A ล็อก", a["lock"], [0.0, 50.0, 0.0, 0.0])
check("A ออก KP แล้ว", a["kp"], [0.0, 0.0, 440.0, 0.0])
check("B: W44 PO ของใบ 500 ; W45 แผนให้ยืมสต๊อก → สต๊อก 100", (b["po"], b["stock"]),
      ([500.0, 0.0, 0.0, 0.0], [0.0, 100.0, 0.0, 0.0]))
print("คงเหลือ = ใช้ได้ − สต๊อกสะสม")
check("A คงเหลือ 600 → 100 → −60 → −60 → −115", a["bal"], [100.0, -60.0, -60.0, -115.0])
check("A ไม่พอตั้งแต่สัปดาห์ index 1 (W45)", a["short"], 1)
check("B ไม่มีในสต๊อก: ใช้ได้ None, ไม่พอตั้งแต่ W45", (b["usable"], b["short"]), (None, 1))
check("รวมทั้งด้าย", a["total"], 1205.0)
print("รายใบ / ยอดที่ยังล็อกได้ / ไม่รู้ด้าย")
r = next(x for x in d["rows"] if x["so"] == "100" and x["w"] == 0 and d["yarns"][x["y"]]["yarn"] == "B")
check("แถวรายใบมีวัน PO เข้า", (r["po"], r["po_in"]), (500.0, "20/11/2026"))
check("ยอดที่ล็อกได้ของใบ 200 = ทุกสัปดาห์ ไม่รวม KP (110 + 55)",
      d["orders"]["200|F2"]["need"][d["yarns"].index(a)], 165.0)
check("ลูกค้า", d["orders"]["100|F1"]["cus"], "ลูกค้า")
check("ไม่รู้ด้าย: YD-ORDERS 300 kg", (d["unknown"]["rows"], d["unknown"]["kg"], d["unknown"]["types"]),
      (1, 300.0, {"YD-ORDERS": 300.0}))
check("ชีทไม่มีคอลัมน์แผน → ว่าง", yarn_plan.build(pd.DataFrame({"X": [1]}), YL, {}, {})["yarns"], [])

print("PO Pending เข้า → คงเหลือ = ใช้ได้ + PO ที่เข้าสะสม − ที่แผนดึงจากสต๊อกสะสม")
po = pd.DataFrame([
    # A: PO ค้างส่ง 300 เข้า 29/10 + รอ 1 สัปดาห์ = 05/11 → W45
    {"PO": "PA", "ITEM": "A", "COLOR": "", "ORDER": 300.0, "REC": 0.0, "REMAIN": 300.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-10-29"), "DATE": pd.Timestamp("2026-09-01"), "SUPPLIER": "x"},
    # B: PO 600 ที่แถว W44 รออยู่ (500) → กันไว้ ที่เหลือ 100 เข้า 27/11 → สัปดาห์แผนถัดไปที่มี = 2027 W1
    {"PO": "PB", "ITEM": "B", "COLOR": "", "ORDER": 600.0, "REC": 0.0, "REMAIN": 600.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-11-20"), "DATE": pd.Timestamp("2026-09-01"), "SUPPLIER": "นันยางเท็กซ์ไทล์"},
    {"PO": "PC", "ITEM": "A", "COLOR": "", "ORDER": 900.0, "REC": 900.0, "REMAIN": 0.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-10-29"), "DATE": pd.Timestamp("2026-09-01"), "SUPPLIER": "x"},
])
d2 = yarn_plan.build(plan, YL, {A: 600.0}, {("200", "F2"): {A: 50.0}}, po=po, today=pd.Timestamp("2026-10-15"),
                     lag=1, internal={B: "Internal"}, lots=LOTS)
y2 = {(y["yarn"], y["color"]): y for y in d2["yarns"]}
check("A: PO เข้า W45 300 (PO ที่รับครบไม่นับ)", y2[A]["inbound"], [0.0, 300.0, 0.0, 0.0])
check("A คงเหลือ 100 → 240 → 240 → 185 ไม่ติดลบ", (y2[A]["bal"], y2[A]["short"]), ([100.0, 240.0, 240.0, 185.0], None))
check("B: PO ที่บรรทัดรออยู่ถูกกันไว้ เหลือ 100 เข้าสัปดาห์แผน 2027 W1", y2[B]["inbound"], [0.0, 0.0, 0.0, 100.0])
check("B คงเหลือ 0 → −100 → −100 → 0 ; ด้ายนันยางผลิต", (y2[B]["bal"], y2[B]["internal"], y2[A]["internal"]),
      ([0.0, -100.0, -100.0, 0.0], True, False))
check("ป้ายแหล่งด้ายที่ไม่คุม (Internal / Local) ; ด้ายที่คุม = None", (y2[B]["src"], y2[A]["src"]), ("Internal", None))

print("แตกที่ดึงจากกองกลาง: ของในคลังก่อน → PO ที่เข้ามาแล้ว → ไม่พอ (user 16 ต.ค. 2569: ใช้เกิน stock ได้ยังไง)")
check("ไม่มี PO: A ในคลัง 600 → ใช้สต๊อก 500 + 100 ที่เหลือไม่พอ",
      (a["from_stk"], a["from_po"], a["gap"]), ([500.0, 100.0, 0.0, 0.0], [0.0] * 4, [0.0, 60.0, 0.0, 55.0]))
check("มี PO: A W45 ในคลังเหลือ 100 + PO 60 ; 2027W1 ใช้ PO 55",
      (y2[A]["from_stk"], y2[A]["from_po"], y2[A]["gap"]),
      ([500.0, 100.0, 0.0, 0.0], [0.0, 60.0, 0.0, 55.0], [0.0] * 4))
check("B ไม่มีในคลัง: W45 ไม่พอ 100 — PO ที่เข้า 2027W1 จ่ายหนี้ ไม่โชว์เป็นใช้ PO",
      (y2[B]["from_stk"], y2[B]["from_po"], y2[B]["gap"]), ([0.0] * 4, [0.0] * 4, [0.0, 100.0, 0.0, 0.0]))
for y in d["yarns"] + d2["yarns"]:
    for i, s in enumerate(y["stock"]):
        assert abs(y["from_stk"][i] + y["from_po"][i] + y["gap"][i] - s) < 0.15, (y["yarn"], i)
check("ทุกด้าย ใช้สต๊อก + ใช้ PO + ไม่พอ = ที่ดึงจากกองกลาง", True, True)

print("ใช้ lot ไหน / PO ไหน (user 16 ต.ค. 2569: \"ต้องบอกด้วยว่าเป็น lot อะไรใช้ lot ไหน ทั้งส่วนของ stock และ PO pending\")")
T = yarn_plan._trace
t = T(3213.0, [{"lot": "L1", "kg": 2000.0}, {"lot": "L2", "kg": 1500.0}], [{"po": "YRK691001", "kg": 9072.0, "wi": 0}],
      [[("A", 1473.0)], [("B", 1000.0), ("C", 1335.0)], [("A", 1125.0)]])
check("เคส C16/1OE: ในคลัง 3,213 หมดสัปดาห์ 2 แล้วใช้ PO 595 → 1,125", (t["from_stk"], t["from_po"], t["gap"]),
      ([1473.0, 1740.0, 0.0], [0.0, 595.0, 1125.0], [0.0, 0.0, 0.0]))
check("lot รวมเกินยอดใช้ได้ → ตัด lot ใหม่สุด (ใบ KP จองไม่ระบุ lot)", t["pool"], [("L1", 2000.0), ("L2", 1213.0)])
check("สัปดาห์ 2 ใช้ L1 ที่เหลือ 527 + L2 1,213 (คลังหมด)", t["lot_w"][1], {"L1": 527.0, "L2": 1213.0})
check("1 ใบ 1 lot: A = L1 ; B = L2 (L1 เหลือไม่พอทั้งใบ)", (t["by"][(0, "A")]["lots"], t["by"][(1, "B")]["lots"]),
      ({"L1": 1473.0}, {"L2": 1000.0}))
check("ใบ C (มาทีหลัง คลังเหลือแต่เศษ) ต้องผสม L1 527 + L2 213 + PO YRK691001 595",
      (t["by"][(1, "C")]["lots"], t["by"][(1, "C")]["pos"]), ({"L1": 527.0, "L2": 213.0}, {"YRK691001": 595.0}))
check("สัปดาห์ 3 ใช้ PO ล้วน", t["po_w"][2], {"YRK691001": 1125.0})
t = T(-100.0, [], [{"po": "P", "kg": 300.0, "wi": 1}], [[("A", 50.0)], [], [("A", 40.0)]])
check("ใช้ได้ติดลบ = หนี้ ; PO เข้าจ่ายหนี้ 150 ก่อน เหลือ 150 ให้ใช้", (t["from_stk"], t["from_po"], t["gap"]),
      ([0.0, 0.0, 0.0], [0.0, 0.0, 40.0], [50.0, 0.0, 0.0]))
t = T(500.0, [{"lot": "L1", "kg": 300.0}, {"lot": "L2", "kg": 200.0}], [], [[("A", 250.0)], [("B", 150.0)]],
      noted={"B": {"L1": 100.0}})
check("ใบอื่นเลี่ยง lot ที่แผนให้ใบ B ยืมไว้ (L1 100) ; B ได้ L1 ตามป้าย",
      (t["by"][(0, "A")]["lots"], t["by"][(1, "B")]["lots"]), ({"L1": 200.0, "L2": 50.0}, {"L1": 100.0, "L2": 50.0}))
t = T(30.3, [{"lot": "L1", "kg": 13.3}, {"lot": "L2", "kg": 17.0}], [], [[("A", 10.0)], [("B", 13.0)]],
      noted={"B": {"L1": 13.0}})
check("ป้ายปัดเป็นจำนวนเต็ม (L1 มี 13.3 ป้าย 13): ใบอื่นไม่ได้เศษ 0.3 ; เจ้าของป้ายได้ทั้ง lot",
      (t["by"][(0, "A")]["lots"], t["by"][(1, "B")]["lots"]), ({"L2": 10.0}, {"L1": 13.0}))
check("เศษ 0.3 ของ L1 ยังอยู่ในกอง (ไม่หาย)", round(t["from_stk"][0] + t["from_stk"][1], 1), 23.0)
t = T(100.0, [{"lot": "L1", "kg": 60.0}], [], [[("A", 100.0)]])
check("lot ไม่ครบยอดใช้ได้ → ส่วนที่เหลือเป็น lot ว่าง (ไม่รู้ lot)", t["by"][(0, "A")]["lots"], {"L1": 60.0, "": 40.0})

print("1 ใบ = 1 lot ทั้งใบ (user 16 ต.ค. 2569 — เคส CORG60/1R : ใบได้เศษ lot เก่า 4 lot / เปลี่ยน lot กลางใบ)")
t = T(12000.0, [{"lot": "S1", "kg": 20.0}, {"lot": "S2", "kg": 90.0}, {"lot": "P", "kg": 2600.0},
                {"lot": "Q", "kg": 9290.0}], [],
      [[("X", 1160.0), ("Y", 585.0)], [("X", 25.0), ("Z", 15.0)], [("Y", 1170.0)], [("Y", 4358.0)]])
check("X 1,185 ทั้งใบ = P (S1 / S2 เก่ากว่าแต่ไม่พอ) ทั้ง 2 สัปดาห์",
      (t["by"][(0, "X")]["lots"], t["by"][(1, "X")]["lots"]), ({"P": 1160.0}, {"P": 25.0}))
check("Y 6,113 = Q ทุกสัปดาห์ ไม่เปลี่ยน lot กลางใบ",
      [t["by"][(w, "Y")]["lots"] for w in (0, 2, 3)], [{"Q": 585.0}, {"Q": 1170.0}, {"Q": 4358.0}])
check("ใบเล็ก Z 15 ได้เศษ lot เก่า S1 ที่พอดี", t["by"][(1, "Z")]["lots"], {"S1": 15.0})
check("ยอดรายสัปดาห์ไม่ขึ้นกับการแจก lot", t["from_stk"], [1745.0, 40.0, 1170.0, 4358.0])
t = T(12000.0, [{"lot": "S1", "kg": 20.0}, {"lot": "S2", "kg": 90.0}, {"lot": "P", "kg": 2600.0},
                {"lot": "Q", "kg": 9290.0}], [], [[("X", 1160.0), ("Y", 585.0)], [("Y", 1170.0)]], order=["Y", "X"])
check("order: ใบที่มาก่อนได้เลือกก่อน — Y 1,755 ได้ P ; X 1,160 P เหลือ 845 ไม่พอ → Q",
      (t["by"][(0, "Y")]["lots"], t["by"][(0, "X")]["lots"]), ({"P": 585.0}, {"Q": 1160.0}))
t = T(500.0, [{"lot": "L1", "kg": 300.0}, {"lot": "L2", "kg": 200.0}], [], [[("A", 100.0)], [("B", 150.0)]],
      noted={"B": {"L1": 100.0}})
check("ป้ายได้ L1 100 แต่ใบใช้ 150 → เติมจาก L1 ที่ถืออยู่ (ไม่เปิด lot ใหม่)", t["by"][(1, "B")]["lots"], {"L1": 150.0})

ya, ya2 = ys[A], y2[A]
check("แผน: lot ต่อสัปดาห์ (LA1 เก่าก่อน ; ตัด LA2 เหลือ 50)", ya["lot_w"],
      [[["LA1", 500.0]], [["LA1", 50.0], ["LA2", 50.0]], [], []])
check("แผน: lot ที่ประกอบเป็นยอดใช้ได้ + อายุ", ya["lots"], [["LA1", 550.0, 300], ["LA2", 50.0, 10]])
rw = {(x["so"], x["w"]): x for x in d["rows"] if x["y"] == d["yarns"].index(ya)}
check("แผน: W45 ใบ 100 ได้ LA1 50 + LA2 50 ; ใบ 200 (เลขมากกว่า) ไม่มี lot เหลือ ไม่พอ 60",
      (rw[("100", 1)]["lots"], rw[("200", 1)]["lots"], rw[("200", 1)]["gap"]),
      ([["LA1", 50.0], ["LA2", 50.0]], [], 60.0))
rw2 = {(x["so"], x["w"]): x for x in d2["rows"] if x["y"] == d2["yarns"].index(ya2)}
check("แผน + PO: ใบ 200 ใช้ PO PA 60 (W45) แล้ว 55 (2027W1)", (rw2[("200", 1)]["pos"], rw2[("200", 3)]["pos"]),
      ([["PA", 60.0]], [["PA", 55.0]]))
check("ป้าย PO: ผู้ขาย + วันกำหนดเข้า + เลยกำหนด", (ya2["pom"], y2[B]["pom"]),
      ({"PA": ["x", "29/10/2026", False]}, {"PB": ["นันยางเท็กซ์ไทล์", "20/11/2026", False]}))
check("PO ที่เข้าต่อสัปดาห์ (แถว PO เข้า)", ya2["in_w"], [[], [["PA", 300.0]], [], []])
rb = next(x for x in d2["rows"] if x["so"] == "100" and x["w"] == 0 and d2["yarns"][x["y"]]["yarn"] == "B")
check("แถวรอ PO ของใบบอกเลข PO", rb["po_nos"], ["PB"])
pc = plan.copy()
pc["COLOR_PART"] = ["No", "No", "Yes", "No", "No", "No", "No"]   # ใบ 200 W45 เป็นงานสี
dc = yarn_plan.build(pc, YL, {A: 600.0}, {("200", "F2"): {A: 50.0}}, lots=LOTS)
rc = {(x["so"], x["w"]): x for x in dc["rows"] if dc["yarns"][x["y"]]["yarn"] == "A"}
check("งานสีได้ของในคลังก่อน + ได้เลือก lot ก่อน: ใบ 200 LA1 60 lot เดียว ; ใบ 100 W45 LA2 40 ไม่พอ 60",
      (rc[("200", 1)]["lots"], rc[("100", 1)]["lots"], rc[("100", 1)]["gap"]),
      ([["LA1", 60.0]], [["LA2", 40.0]], 60.0))
check("… ใบ 100 (540 ไม่มี lot ไหนพอแล้ว) = LA1 490 + LA2 50 ไล่ตามสัปดาห์ : W44 LA1 490 + LA2 10",
      rc[("100", 0)]["lots"], [["LA1", 490.0], ["LA2", 10.0]])
pf = plan.copy()
pf["FG_WEEK"] = [202652, 202652, 202650, "", "", "", ""]          # ใบ 200 FG เร็วกว่า
rf = {(x["so"], x["w"]): x for x in yarn_plan.build(pf, YL, {A: 600.0}, {("200", "F2"): {A: 50.0}}, lots=LOTS)["rows"]
      if x["y"] == 0}
check("FG เร็วกว่าได้เลือก lot ก่อน", rf[("200", 1)]["lots"], [["LA1", 60.0]])

print("lot ที่คนล็อกให้ SC / Item (user 16 ต.ค. 2569: \"คนต้องสามารถ config ได้ด้วย ว่าอยากให้ใช้ lot ไหน\")")
OWN = {("200", "F2"): {A: [{"lot": "LK1", "kg": 30.0, "age": 50}, {"lot": "LK2", "kg": 20.0, "age": 5}]}}
dl = yarn_plan.build(plan, YL, {A: 600.0}, {("200", "F2"): {A: 50.0}}, lots=LOTS, own_lots=OWN)
yl = dl["yarns"][0]
check("ล็อก SC 50 ใช้ W45 : LK1 30 + LK2 20 (เก่าก่อน)", (yl["lock"], yl["lock_w"]),
      ([0.0, 50.0, 0.0, 0.0], [[], [["LK1", 30.0], ["LK2", 20.0]], [], []]))
r200 = next(x for x in dl["rows"] if x["so"] == "200" and x["w"] == 1 and x["y"] == 0)
check("แถวใบ 200 W45 บอก lot ที่ล็อก + ส่วนที่เหลือใช้กองกลาง", (r200["lock_lots"], r200["stock"]),
      ([["LK1", 30.0], ["LK2", 20.0]], 60.0))
check("lot ที่ล็อกไม่ปนแถว lot ของกองกลาง", yl["lot_w"], ya["lot_w"])
di = yarn_plan.build(plan, YL, {A: 600.0}, {}, lots=LOTS, item_kg={"F1": {A: 520.0}},
                     item_lots={"F1": {A: [{"lot": "LI1", "kg": 500.0}, {"lot": "LI2", "kg": 20.0}]}})
check("ล็อก Item ผ้า F1 520: W44 LI1 500 ; W45 LI2 20", di["yarns"][0]["ilock_w"],
      [[["LI1", 500.0]], [["LI2", 20.0]], [], []])

print("อ่านป้าย lot ของใบที่แผนให้ยืมสต๊อก (YARN_STOCK)")
check("หลายด้าย / หลาย lot / คอมมาในตัวเลข / ข้าม lot ล็อก / ด้ายสี",
      yarn_plan._noted_lots("ใช้ด้ายสต๊อก: DTY75/72/1 lot TIN-G573012E-Z33 1,414 kg + lot TIN-E878/M01W 248 kg "
                            "(แทน PO 03/11/2026) · PC50/1R lot TRT-4X1-2609 3,623 kg · "
                            "CTD20/1R สี B10 lot X (ล็อก) 20 kg + lot Y 5 kg"),
      {("DTY75/72/1", ""): {"TIN-G573012E-Z33": 1414.0, "TIN-E878/M01W": 248.0},
       ("PC50/1R", ""): {"TRT-4X1-2609": 3623.0}, ("CTD20/1R", "B10"): {"Y": 5.0}})
check("ไม่ใช่ป้ายยืมสต๊อก → ว่าง", yarn_plan._noted_lots("รอ PO"), {})

if FAILED:
    print(f"\n❌ ไม่ผ่าน {len(FAILED)} ข้อ")
    sys.exit(1)
print("\n✅ ผ่านทุกข้อ")

"""
Unit test: แผนวางได้ไม่เกินด้ายที่มี = สต๊อก + PO Pending (YarnStockCover.supply_cap)

กติกาที่ user ตัดสิน 15 ต.ค. 2569:
  - "ต้องไม่มีติดลบ วางแผนแค่ที่มีใน stock ที่เหลือไปตาม POIN" + "ต้องดู PO Pending ด้วย"
  - ด้ายที่นันยางผลิตเอง (ผู้ขายใน PO) ไม่ต้องคุม
  - ด้ายพอบางส่วน = แตกใบ วางส่วนที่มีด้าย · ไม่มีด้ายและไม่มี PO = UNPLANNED
  - PO ที่เลยกำหนดแต่ยังค้างส่ง = นับว่าเข้าสัปดาห์นี้ + lag (ข้อ ก)
  - ลำดับ: งานสีก่อน แล้วกำหนดส่ง

รันแบบ standalone (ไม่ต้องมีไฟล์ข้อมูล):
    python tests/test_yarn_supply.py
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import YarnStockCover as ysc  # noqa: E402

FAILED = []


def check(name, got, want):
    ok = abs(got - want) < 1e-6 if isinstance(want, float) and isinstance(got, (int, float)) else got == want
    if ok:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)
    assert ok, name


A, B = ("A", ""), ("B", "")
F = 2      # สัปดาห์แรกที่วางได้

print("allocate_supply — งานสีก่อน แล้วกำหนดส่ง ; ใช้ล็อกของใบก่อน")
ev = {A: [[F, 100.0, "สต๊อก"], [5, 300.0, "PO1"]]}
r = ysc.allocate_supply([
    {"key": "late", "color": False, "due": 202650, "needs": {A: 200.0}},
    {"key": "color", "color": True, "due": 202660, "needs": {A: 100.0}},
    {"key": "early", "color": False, "due": 202645, "needs": {A: 200.0}},
], ev, F)
check("งานสีได้สต๊อกตอนนี้ทั้งหมด", (r["color"]["fa"], r["color"]["ready"]), (1.0, None))
check("ใบกำหนดส่งเร็วได้ PO สัปดาห์ 5 ทั้งใบ", (r["early"]["f"], r["early"]["fa"], r["early"]["ready"]), (1.0, 0.0, 5))
check("ใบสุดท้ายได้ PO ที่เหลือ 100/200 → ครึ่งเดียว ที่เหลือขาด", (r["late"]["f"], r["late"]["short"]), (0.5, {A: 100.0}))
check("ป้ายที่มาของด้าย", r["early"]["src"], {A: [("PO1", 200.0)]})
ev = {A: [[F, 50.0, "สต๊อก"]]}
r = ysc.allocate_supply([{"key": "x", "color": False, "due": 1, "needs": {A: 100.0}, "own": {A: 30.0}}], ev, F)
check("ล็อก 30 + สต๊อก 50 = 80%", (round(r["x"]["f"], 3), round(r["x"]["fa"], 3)), (0.8, 0.8))
print("ต้องได้ทุกเส้นในสัดส่วนเดียวกัน — เส้นที่พอไม่ถูกกักเกิน")
ev = {A: [[F, 1000.0, "สต๊อก"]], B: [[F, 10.0, "สต๊อก"], [6, 40.0, "PO-B"]]}
r = ysc.allocate_supply([{"key": "x", "color": False, "due": 1, "needs": {A: 100.0, B: 100.0}}], ev, F)
check("B มีแค่ 50/100 → ทั้งใบได้ 50%, ตอนนี้ 10%, ที่เหลือมากับ PO-B สัปดาห์ 6",
      (r["x"]["f"], round(r["x"]["fa"], 3), r["x"]["ready"]), (0.5, 0.1, 6))
check("A ถูกแบ่งไปแค่ 50 (ไม่กัก 100)", ev[A][0][1], 950.0)

print("split_rows — แถวกำหนดส่งเร็วได้ด้ายก่อน แถวที่คร่อมถูกแตก")
rows = [{"ID": 1, "PENDING_PLAN": 100.0}, {"ID": 2, "PENDING_PLAN": 100.0}, {"ID": 3, "PENDING_PLAN": 100.0}]
a, b, u = ysc.split_rows(rows, 150.0, 100.0)
check("ก้อนมีด้าย", [(x["ID"], x["PENDING_PLAN"]) for x in a], [(1, 100.0), (2, 50.0)])
check("ก้อนรอ PO", [(x["ID"], x["PENDING_PLAN"]) for x in b], [(2, 50.0), (3, 50.0)])
check("ก้อนไม่มีด้าย", [(x["ID"], x["PENDING_PLAN"]) for x in u], [(3, 50.0)])

print("pending_events — PO Pending, เลยกำหนด = วันนี้ + lag, กันให้บรรทัดที่รอ PO")
TODAY = pd.Timestamp("2026-10-15")
po = pd.DataFrame([
    {"PO": "P1", "ITEM": "A", "COLOR": "", "ORDER": 500.0, "REC": 100.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-10-01"), "DATE": pd.Timestamp("2026-09-01"), "SUPPLIER": "ไทยนิกเท็กซ์ บจก."},
    {"PO": "P2", "ITEM": "A", "COLOR": "", "ORDER": 300.0, "REC": 0.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-11-05"), "DATE": pd.Timestamp("2026-09-10"), "SUPPLIER": "ไทยนิกเท็กซ์ บจก."},
    {"PO": "P3", "ITEM": "A", "COLOR": "", "ORDER": 300.0, "REC": 300.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-10-20"), "DATE": pd.Timestamp("2026-09-10"), "SUPPLIER": "ไทยนิกเท็กซ์ บจก."},
    {"PO": "P4", "ITEM": "A", "COLOR": "", "ORDER": 300.0, "REC": 0.0, "STATUS": "CLOSED",
     "PIN": pd.Timestamp("2026-10-20"), "DATE": pd.Timestamp("2026-09-10"), "SUPPLIER": "ไทยนิกเท็กซ์ บจก."},
    {"PO": "N1", "ITEM": "N", "COLOR": "", "ORDER": 900.0, "REC": 0.0, "STATUS": "OPEN",
     "PIN": pd.Timestamp("2026-10-25"), "DATE": pd.Timestamp("2026-09-10"), "SUPPLIER": "นันยางเท็กซ์ไทล์ บจก."},
])
po["REMAIN"] = (po["ORDER"] - po["REC"]).clip(lower=0)
check("ด้ายนันยางผลิต = ไม่คุม", ysc.exempt_keys(po, ["นันยาง"], today=TODAY), {("N", ""): "Internal"})
check("ไม่มีคอลัมน์สกุลเงิน → เปิด local ก็ไม่มี Local เพิ่ม", ysc.exempt_keys(po, ["นันยาง"], True, TODAY),
      {("N", ""): "Internal"})
evs = ysc.pending_events(po, TODAY, 1, {("N", "")}, reserve=[(["P2"], A, 120.0)])
got = sorted((e["po"], e["kg"], f"{e['avail']:%d/%m}", e["late"]) for e in evs)
check("P1 ค้าง 400 เลยกำหนด → 22/10 ; P2 300 − กันให้บรรทัดที่รอ 120 = 180 → 12/11 ; รับครบ/ปิด/นันยาง ไม่นับ",
      got, [("P1", 400.0, "22/10", True), ("P2", 180.0, "12/11", False)])


class TL:
    """ไทม์ไลน์จำลอง สัปดาห์ศุกร์–พฤหัส เริ่ม 2 ต.ค. 2569 ; วันนี้ 15 ต.ค. อยู่ index 1"""
    def __init__(self, n=12):
        s0 = pd.Timestamp("2026-10-02")
        self.starts = [s0 + pd.Timedelta(weeks=i) for i in range(n)]
        self.ends = [s + pd.Timedelta(days=6) for s in self.starts]
        self.keys = [202641 + i for i in range(n)]
        self.today_idx = 1

    def idx_of_date(self, d):
        d = pd.Timestamp(d)
        for i, (s, e) in enumerate(zip(self.starts, self.ends)):
            if s <= d <= e + pd.Timedelta(hours=23, minutes=59):
                return i
        return None


print("supply_cap — ทั้งก้อน: ดีมานด์ → แตกใบ / รอ PO / UNPLANNED")
tl = TL()
ylines = {
    111: {A: {"pct": 100.0, "loss": 1.0, "po_in": None, "pos": []}},
    222: {A: {"pct": 50.0, "loss": 1.0, "po_in": None, "pos": []},
          ("N", ""): {"pct": 50.0, "loss": 1.0, "po_in": None, "pos": []}},
    333: {A: {"pct": 100.0, "loss": 1.0, "po_in": pd.Timestamp("2026-11-05"), "pos": ["P2"]}},
    444: {("N", ""): {"pct": 100.0, "loss": 1.0, "po_in": None, "pos": []}},
}
dem = pd.DataFrame([
    {"SC_NO": "S9", "SO_NO": "9", "FABRIC_ITEM": "F9", "PENDING_PLAN": 50.0, "FGWEEK": 202640, "SC_LINE_ID": "444"},
    {"SC_NO": "S1", "SO_NO": "1", "FABRIC_ITEM": "F1", "PENDING_PLAN": 300.0, "FGWEEK": 202648, "SC_LINE_ID": "111"},
    {"SC_NO": "S1", "SO_NO": "1", "FABRIC_ITEM": "F1", "PENDING_PLAN": 500.0, "FGWEEK": 202650, "SC_LINE_ID": "111"},
    {"SC_NO": "S2", "SO_NO": "2", "FABRIC_ITEM": "F2", "PENDING_PLAN": 200.0, "FGWEEK": 202646, "SC_LINE_ID": "222"},
    {"SC_NO": "S3", "SO_NO": "3", "FABRIC_ITEM": "F3", "PENDING_PLAN": 120.0, "FGWEEK": 202645, "SC_LINE_ID": "333"},
    {"SC_NO": "S4", "SO_NO": "4", "FABRIC_ITEM": "F4", "PENDING_PLAN": 0.0, "FGWEEK": 202645, "SC_LINE_ID": "111"},
])
stock = ({}, {A: 200.0}, "15/10", "", {})
res = ysc.supply_cap(dem, tl, freeze_weeks=1, lag=1, exempt_kw=["นันยาง"], today=TODAY,
                     ylines=ylines, po=po, stock=stock, color_keys=set())
st = res["stats"]
# ด้าย A ที่มี: สต๊อก 200 (index 2) + P1 400 (เลยกำหนด → 22/10 = index 2) + P2 เหลือ 180 (เข้า 05/11 + 1 สัปดาห์
# = 12/11 อยู่สัปดาห์ index 5 ที่เริ่ม 06/11 — ตรงกับ Planning.get_point_date_start_idx: สัปดาห์ของวันเข้า + lag)
# S2 (กำหนด 46) ต้องการ A 100 → ได้สต๊อก ; S1 (กำหนด 48) ต้องการ 800 → สต๊อก 100 + P1 400 ตอนนี้ + P2 180 = 680
check("S2 ด้ายพอ ไม่แตะ ; S1 แตกใบ ; S3 รอ PO ของตัวเอง ไม่คุม ; S9 ด้ายนันยางล้วน ไม่คุม",
      (st["orders"], st["full"], st["split"], st["wait"], st["short_orders"]), (2, 1, 1, 1, 1))
d = res["demand"]
s1 = d[d["SO_NO"] == "1"]
check("S1 ก้อนมีด้าย 500 (แถวกำหนด 48 ทั้งแถว + 200 ของแถว 50) ; ก้อนรอ PO 180 วางได้ตั้งแต่สัปดาห์ 06/11",
      [(r.PENDING_PLAN, None if pd.isna(r.YARN_READY) else f"{r.YARN_READY:%d/%m}") for r in s1.itertuples()],
      [(300.0, None), (200.0, None), (180.0, "06/11")])
check("S1 ส่วนที่ไม่มีด้าย 120 → UNPLANNED พร้อมเหตุผล",
      [(r["SO_NO"], r["PENDING_PLAN"], r["REASON"]) for r in res["short"].to_dict("records")],
      [("1", 120.0, "ด้ายไม่พอ (สต๊อก + PO Pending): A ขาด 120 kg")])
check("ลำดับแถวเดิมไม่เปลี่ยน (S9 → S1… → S2 → S3 → S4)", list(d["SO_NO"]), ["9", "1", "1", "1", "2", "3", "4"])
check("ใบที่ไม่แตะ ยอดเท่าเดิม (S9, S2, S3)", list(d[d["SO_NO"].isin(["2", "3", "9"])]["PENDING_PLAN"]),
      [50.0, 200.0, 120.0])
check("ยอดรวมไม่หาย (วาง + UNPLANNED = เดิม)", float(d["PENDING_PLAN"].sum() + res["short"]["PENDING_PLAN"].sum()),
      float(dem["PENDING_PLAN"].sum()))
check("ป้ายที่มาของด้ายติดไปกับแถว", d[d["SO_NO"] == "1"]["YARN_SUPPLY"].iloc[2].startswith("รอด้าย: วางได้ตั้งแต่ W46/2026"),
      True)
r0 = ysc.supply_cap(dem, tl, 1, 1, today=TODAY, ylines=ylines, po=pd.DataFrame(), stock=stock, color_keys=set())
check("ไม่มีไฟล์ PO → ไม่คุม ดีมานด์เดิม + บอกเหตุผล", (r0["demand"] is dem, "stats" in r0, "View_PO_Yarn" in r0["note"]),
      (True, False, True))

print("ด้ายในประเทศ (Internal + Local) ไม่คุม — user 16 ต.ค. 2569 ; แยกตามสี ; ไม่มี PO 1 ปีดูประวัติ")


def _po(no, item, color, cur, sup, date, order=100.0, rec=0.0, pin="2026-11-20"):
    return {"PO": no, "ITEM": item, "COLOR": color, "ORDER": order, "REC": rec, "REMAIN": max(order - rec, 0.0),
            "STATUS": "OPEN", "PIN": pd.Timestamp(pin), "DATE": pd.Timestamp(date), "SUPPLIER": sup, "CUR": cur}


po2 = pd.DataFrame([
    _po("Q1", "L", "", "THB", "ไทยนิกเท็กซ์ บจก.", "2026-08-01"),
    _po("Q2", "I", "", "USD", "BROTEX(VIETNAM) CO.,LTD.", "2026-08-01"),
    _po("Q3", "T", "B09", "USD", "BROTEX(VIETNAM) CO.,LTD.", "2026-08-01", order=100.0),
    _po("Q4", "T", "H120", "THB", "นันยางเท็กซ์ไทล์ บจก.", "2026-08-01", order=900.0),
    _po("Q5", "O", "", "THB", "เนย์ลิทท์ บจก.", "2024-05-01"),            # ไม่มี PO ใน 1 ปี → ดูประวัติ
    _po("Q6", "M", "", "THB", "ไทยนิกเท็กซ์ บจก.", "2026-08-01", order=300.0),
    _po("Q7", "M", "", "USD", "SHINKONG", "2026-09-01", order=700.0),    # บาทแค่ 30% → คุม
    _po("Q8", "M", "", "THB", "ไทยนิกเท็กซ์ บจก.", "2024-01-01", order=5000.0),  # PO เก่าไม่นับ เมื่อมี PO ใน 1 ปีแล้ว
])
ex = ysc.exempt_keys(po2, ["นันยาง"], True, TODAY)
check("L บาท = Local ; T สี H120 นันยาง = Internal แต่สี B09 USD คุม ; O ดูประวัติ ; I/M นำเข้าคุม",
      ex, {("L", ""): "Local", ("T", "H120"): "Internal", ("O", ""): "Local"})
check("ปิด local → เหลือแค่ผู้ขายในรายชื่อ", ysc.exempt_keys(po2, ["นันยาง"], False, TODAY), {("T", "H120"): "Internal"})
po3 = pd.concat([po2, pd.DataFrame([_po("Q9", "R", "", "THB", "นันยางเท็กซ์ไทล์ บจก.", "2026-08-01")])], ignore_index=True)
ex3 = ysc.exempt_keys(po3, ["นันยาง"], True, TODAY, keys={("R", "B50"), ("T", "B09"), ("T", "ZZ"), ("L", "")})
check("ด้ายสีที่ไม่เคยมี PO สีนั้น → ใช้ป้ายด้ายดิบ (R สี B50 = Internal) ; สีที่มี PO นำเข้า / ไม่มีด้ายดิบ → คุม",
      (ex3.get(("R", "B50")), ex3.get(("T", "B09")), ex3.get(("T", "ZZ")), ex3.get(("L", ""))),
      ("Internal", None, None, "Local"))
check("PO Pending ของด้ายในประเทศไม่นับ (ไม่คุมอยู่แล้ว)",
      sorted(e["po"] for e in ysc.pending_events(po2, TODAY, 1, ex, [])), ["Q2", "Q3", "Q6", "Q7", "Q8"])
yl2 = {501: {("L", ""): {"pct": 100.0, "loss": 1.0, "po_in": None, "pos": []}},
       502: {("I", ""): {"pct": 100.0, "loss": 1.0, "po_in": None, "pos": []}},
       503: {("T", "B09"): {"pct": 100.0, "loss": 1.0, "po_in": None, "pos": []}}}
dem2 = pd.DataFrame([
    {"SC_NO": "S51", "SO_NO": "51", "FABRIC_ITEM": "FL", "PENDING_PLAN": 500.0, "FGWEEK": 202646, "SC_LINE_ID": "501"},
    {"SC_NO": "S52", "SO_NO": "52", "FABRIC_ITEM": "FI", "PENDING_PLAN": 500.0, "FGWEEK": 202646, "SC_LINE_ID": "502"},
    {"SC_NO": "S53", "SO_NO": "53", "FABRIC_ITEM": "FT", "PENDING_PLAN": 500.0, "FGWEEK": 202646, "SC_LINE_ID": "503"},
])
r2 = ysc.supply_cap(dem2, tl, 1, 1, exempt_kw=["นันยาง"], today=TODAY, ylines=yl2, po=po2,
                    stock=({}, {}, "15/10", "", {}), color_keys=set(), local=True)
d2 = r2["demand"]
check("ใบด้าย Local ไม่มีสต๊อก/PO → วางได้เลย ไม่แตะ ; ใบด้ายนำเข้า → รอ PO / ขาด",
      (float(d2[d2["SO_NO"] == "51"]["PENDING_PLAN"].sum()), d2[d2["SO_NO"] == "51"].get("YARN_READY", pd.Series([None])).isna().all(),
       sorted(r2["short"]["SO_NO"].unique()), r2["stats"]["exempt_yarns"], r2["stats"]["exempt_local"]),
      (500.0, True, ["52", "53"], 3, 2))
check("ยอดรวมไม่หาย", float(d2["PENDING_PLAN"].sum() + r2["short"]["PENDING_PLAN"].sum()), 1500.0)

print("load_yarn_lines — นับเฉพาะเลขบรรทัดใบ SC (6 หลัก) ; เลขเล็กเป็นของระบบอื่น (เอกสาร D… / FQC)")
import tempfile  # noqa: E402
tmp = Path(tempfile.mkdtemp()) / "scfol.xlsx"
pd.DataFrame([
    {"SC_NO": "S721621", "LINE_ID": 773847, "Y_YARN_ITEM": "DTY140", "Y_CONTAIN_PCT": 100, "PO_NO": "", "PO_IN_DATE": None},
    {"SC_NO": "D25010082-1", "LINE_ID": 1, "Y_YARN_ITEM": "BCI10/1R", "Y_CONTAIN_PCT": 60, "PO_NO": "", "PO_IN_DATE": None},
    {"SC_NO": "D25010124-2", "LINE_ID": 1, "Y_YARN_ITEM": "DTY75", "Y_CONTAIN_PCT": 40, "PO_NO": "", "PO_IN_DATE": None},
]).assign(Y_TOPDYED_COLOR="NO-COLOR", Y_KNIT_QTY=100.0, Y_YARN_QTY_LOST=100.0).to_excel(tmp, index=False)
yl = ysc.load_yarn_lines(tmp, today=TODAY)
check("เอกสาร D… LINE_ID 1 ไม่เข้า ylines", sorted(yl), [773847])
fqc = pd.DataFrame([{"SC_NO": "S721621", "SO_NO": "721621", "FABRIC_ITEM": "FQ", "PENDING_PLAN": 624.0,
                     "FGWEEK": 202637, "SC_LINE_ID": "1"}])
r1 = ysc.supply_cap(fqc, tl, 1, 1, today=TODAY, ylines=yl, po=po, stock=({}, {}, "15/10", "", {}), color_keys=set())
check("ใบ FQC (SC_LINE_ID '1') ไม่ถูกตัดเป็นด้ายไม่พอ", (r1["stats"]["orders"], len(r1["short"]),
      float(r1["demand"]["PENDING_PLAN"].sum())), (0, 0, 624.0))

if FAILED:
    print(f"\n❌ ไม่ผ่าน {len(FAILED)} ข้อ")
    sys.exit(1)
print("\n✅ ผ่านทุกข้อ")

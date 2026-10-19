"""
Unit test: ล็อกด้ายให้ Item ผ้า (webapp/backend/yarn_item_lock.py + yarn_ava_live.py + YarnStockCover.py + yarn_plan.py)

user 15 ต.ค. 2569: "ต้องมีอีกหน้านึงสำหรับให้ Yarn ไว้ล็อกเส้นด้ายตัวนั้นๆ ให้สำหรับ Item นั้นๆ จำนวนเท่าไหร่ ...
                    แผนทอก็ต้องดูจากตัวนี้ไม่สามารถเอาด้ายตัวที่ล็อกไปให้ตัวอื่นได้"
กติกา: ระบุ lot หรือ "ทุก lot" (lot ที่ด้ายเข้าก่อนก่อน) · kg ด้าย · ทุกใบของ Item ใช้ร่วมกัน (ล็อกของใบ → ล็อก Item → กองกลาง)
       · ผ้าอื่นใช้ไม่ได้ · ออกใบ KP แล้วยอดลดเอง (ส่วนที่ตัดล็อก SC ไปแล้วไม่นับซ้ำ)

รันแบบ standalone (ไม่ต้องมีไฟล์ข้อมูล / ไม่ต้องต่อ Oracle — ไฟล์ล็อกลงโฟลเดอร์ชั่วคราว):
    python tests/test_yarn_item_lock.py
"""
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "webapp" / "backend"))
import YarnStockCover as ysc  # noqa: E402
import yarn_item_lock  # noqa: E402
import yarn_lock  # noqa: E402
import yarn_ava_live  # noqa: E402
import yarn_plan  # noqa: E402
import yarn_stock_live  # noqa: E402
import livesync  # noqa: E402

FAILED = []


def check(name, got, want):
    ok = abs(got - want) < 1e-6 if isinstance(want, float) and isinstance(got, (int, float)) else got == want
    if ok:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)
    assert ok, name


def rnd(d):
    return {k: round(v, 3) for k, v in d.items()}


A = ("A", "")
F = 10

# =============================================================================
print("allocate_supply — ล็อก Item: ใบของผ้านั้นใช้ร่วมกันตามลำดับ ผ้าอื่นไม่เห็น")
pool = {"FX": {A: 500.0}}
ev = {A: [[F, 100.0, "สต๊อก"]]}
r = ysc.allocate_supply([
    {"key": ("1", "FX"), "item": "FX", "color": False, "due": 10, "needs": {A: 200.0}},
    {"key": ("2", "FX"), "item": "FX", "color": False, "due": 20, "needs": {A: 250.0}},
    {"key": ("3", "FY"), "item": "FY", "color": False, "due": 30, "needs": {A: 150.0}},
], ev, F, pool)
check("ใบ 1 / 2 ของ FX ได้ล็อก Item เต็มยอด", (r[("1", "FX")]["src"], r[("2", "FX")]["src"]),
      ({A: [("ล็อก Item", 200.0)]}, {A: [("ล็อก Item", 250.0)]}))
check("FX ไม่แตะสต๊อกกองกลาง → FY ได้สต๊อก 100 แต่ไม่ได้ล็อก Item ที่เหลือ 50", (round(r[("3", "FY")]["f"], 4),
      r[("3", "FY")]["short"], pool["FX"][A]), (round(100 / 150, 4), {A: 50.0}, 50.0))
check("ล็อกนับว่ามีแล้วตอนนี้ (ไม่ต้องรอ)", (r[("1", "FX")]["fa"], r[("1", "FX")]["ready"]), (1.0, None))
pool = {"FX": {A: 100.0}}
r = ysc.allocate_supply([
    {"key": ("1", "FX"), "item": "FX", "color": False, "due": 1, "needs": {A: 100.0}},
    {"key": ("2", "FX"), "item": "FX", "color": True, "due": 50, "needs": {A: 100.0}},
], {}, F, pool)
check("ใบของผ้าเดียวกันแย่งล็อก — งานสีได้ก่อน", (r[("2", "FX")]["f"], r[("1", "FX")]["f"]), (1.0, 0.0))
r = ysc.allocate_supply([{"key": ("1", "FX"), "item": "FX", "color": False, "due": 1, "needs": {A: 100.0},
                          "own": {A: 30.0}}], {}, F, {"FX": {A: 100.0}})
check("ล็อกของใบ (SC) ก่อน แล้วล็อก Item", r[("1", "FX")]["src"], {A: [("ล็อก", 30.0), ("ล็อก Item", 70.0)]})

# =============================================================================
print("plan_consumption — ล็อกของใบ → ล็อก Item (ทุกใบของผ้า ตามสัปดาห์) → กองกลาง")
ylines = {111: {A: {"pct": 100.0, "loss": 1.0, "po_in": None}},
          222: {A: {"pct": 100.0, "loss": 1.0, "po_in": None}}}
plan = pd.DataFrame([
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 300, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "111",
     "SC_SO_NO": "100", "ITEM_CODE": "F1"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 200, "PLAN_WEEK": 13, "PLAN_YEAR": 2026, "SC_LINE_ID": "111",
     "SC_SO_NO": "101", "ITEM_CODE": "f1"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 100, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "222",
     "SC_SO_NO": "200", "ITEM_CODE": "F2"},
])


class TL:
    def idx(self, week, year=None):
        return int(week)


used, item_used = {}, {}
cons = ysc.plan_consumption(plan, ylines, TL().idx, F, own={("100", "F1"): {A: 100.0}}, used=used,
                            item={"F1": {A: 250.0}}, item_used=item_used)
check("กองกลาง: W12 = ผ้า F2 100 ; W13 = ใบ 101 ส่วนที่ล็อก Item หมดแล้ว 150", rnd(cons[A]), {12: 100.0, 13: 150.0})
check("ล็อกของใบ 100 กินที่ W12", used, {("100", "F1"): {A: {12: 100.0}}})
check("ล็อก Item 250: W12 ส่วนเกินของใบ 100 = 200 ; W13 ใบ 101 = 50", item_used, {"F1": {A: {12: 200.0, 13: 50.0}}})
check("ไม่มีล็อก = เท่าเดิม", rnd(ysc.plan_consumption(plan, ylines, TL().idx, F)[A]), {12: 400.0, 13: 200.0})

# =============================================================================
print("select_borrowers — ใบที่รอ PO ใช้ล็อก Item ของผ้าตัวเองร่วมกัน ; ผ้าอื่นยืมไม่ได้")


def cand(key, needs, due=50, color=False):
    return {"key": key, "item": key[1], "color": color, "due": due, "needs": needs}


ITEM = {"FX": {A: {"kg": 400.0, "used": {}}}}
x, y, z = cand(("1", "FX"), {A: (300.0, 13)}, 1), cand(("2", "FX"), {A: (300.0, 13)}, 2), \
    cand(("3", "FZ"), {A: (100.0, 13)}, 3)
check("x ใช้ล็อก 300 ; y ใช้ล็อกที่เหลือ 100 + กองกลาง 200 ; z (ผ้าอื่น) กองกลางเหลือ 50 ไม่พอ",
      sorted(ysc.select_borrowers([x, y, z], {}, {A: 250.0}, F, item=ITEM)), [("1", "FX"), ("2", "FX")])
check("z คนเดียว กองกลาง 50 — ล็อก FX 400 ไม่ช่วย", list(ysc.select_borrowers([z], {}, {A: 50.0}, F, item=ITEM)), [])
ITEM2 = {"FX": {A: {"kg": 400.0, "used": {11: 300.0}}}}
check("แผนกินล็อกไปแล้ว 300 ที่ W11 → ต้องยืมกองกลาง 200 : มี 150 ไม่พอ",
      list(ysc.select_borrowers([x], {}, {A: 150.0}, F, item=ITEM2)), [])
check("… มี 200 พอ", list(ysc.select_borrowers([x], {}, {A: 200.0}, F, item=ITEM2)), [("1", "FX")])

print("allocate_lots — lot ที่ล็อกให้ Item ใช้ร่วมกันในผ้าเดียว (ป้าย '(ล็อก Item)')")
lots = {A: [{"lot": "G1", "kg": 400.0}]}
ilots = {"FX": {A: [{"lot": "I1", "kg": 350.0}]}}
r = ysc.allocate_lots([("1", "FX"), ("2", "FY"), ("3", "FX")],
                      {("1", "FX"): {A: 300.0}, ("2", "FY"): {A: 300.0}, ("3", "FX"): {A: 150.0}}, lots, None, ilots)
check("ใบ 1 (FX) = I1 300", r[("1", "FX")], {A: [("I1 (ล็อก Item)", 300.0)]})
check("ใบ 2 (FY) ไม่ได้แตะ I1", r[("2", "FY")], {A: [("G1", 300.0)]})
check("ใบ 3 (FX) = I1 ที่เหลือ 50 + G1 100", r[("3", "FX")], {A: [("I1 (ล็อก Item)", 50.0), ("G1", 100.0)]})
check("ของต้นทางไม่ถูกแก้", ilots["FX"][A][0]["kg"], 350.0)

# =============================================================================
print("statuses — ใบ KP ของผ้า + ด้ายเดียวกัน หลังเวลาล็อก ตัดยอด ; ส่วนที่ตัดล็อก SC แล้วไม่นับซ้ำ")
LOCKS = [
    {"id": "I1", "yarn": "Y", "lot": "", "fabric": "F", "yarn_kg": 300.0, "at": "2026-10-15 10:00:00"},
    {"id": "I2", "yarn": "y", "lot": "L9", "fabric": "f", "yarn_kg": 200.0, "at": "2026-10-15 10:30:00"},
    {"id": "I3", "yarn": "Y", "lot": "", "fabric": "G", "yarn_kg": 100.0, "at": "2026-10-15 10:00:00"},
]
KPY = {("KP-OLD", "F", "Y"): {"kg": 150.0, "t": datetime(2026, 10, 15, 9)},
       ("KP-A", "F", "Y"): {"kg": 250.0, "t": datetime(2026, 10, 15, 10, 15)},
       ("KP-B", "F", "Y"): {"kg": 100.0, "t": datetime(2026, 10, 15, 11)},
       ("KP-C", "G", "Z"): {"kg": 500.0, "t": datetime(2026, 10, 15, 11)},
       ("KP-D", "G", "Y"): {"kg": 500.0, "t": None}}
orig = (yarn_lock.kp_yarn, yarn_lock.kp_taken)
yarn_lock.kp_yarn, yarn_lock.kp_taken = (lambda: KPY), (lambda: {("KP-B", "F", "Y"): 30.0})
try:
    st = yarn_item_lock.statuses(LOCKS)
finally:
    yarn_lock.kp_yarn, yarn_lock.kp_taken = orig
check("KP ออกก่อนล็อกไม่นับ ; KP-A 250 ตัด I1 ; KP-B 100 − ล็อก SC 30 = 70 → I1 50 + I2 20",
      [(st[i]["rem"], st[i]["state"]) for i in ("I1", "I2")], [(0.0, "done"), (180.0, "active")])
check("ใบ KP ที่ตัดแต่ละล็อก", [st["I1"]["kps"], st["I2"]["kps"]], [["KP-A", "KP-B"], ["KP-B"]])
check("ผ้า G: ใบ KP ด้ายอื่น / ไม่รู้วันออกใบ ไม่ตัด", (st["I3"]["rem"], st["I3"]["state"]), (100.0, "active"))

# =============================================================================
print("yarn_ava_live.table — ล็อก Item ระบุ lot เรียงเวลารวมกับล็อก SC ; ไม่ระบุ lot แบ่งลง lot ที่เข้าก่อน")


def srow(item, lot, w, y):
    return {"i": item, "l": lot, "p": "YRK1", "w": w, "y": y, "yw": round(w / y, 2), "b": ""}


STOCK = [srow("Y", "LOT1", 100.0, 50), srow("Y", "LOT2", 100.0, 50), srow("Y", "LOT3", 100.0, 50),
         srow("T", "T1", 50.0, 25), srow("T", "T2", 50.0, 25)]
RES = {("Y", None): {"kg": 30.0, "kps": {"KP9": 30.0}}}      # ใบ KP จองแต่ยังไม่ระบุ lot
SCLOCK = {("Y", "LOT1"): [{"id": "s1", "kind": "sc", "sc": "S1", "so": "1", "fabric": "F", "rem": 40.0, "at": "2"}]}
ILOCK = [{"id": "i1", "yarn": "Y", "color": "", "lot": "LOT1", "fabric": "FX", "rem": 50.0, "at": "1"},
         {"id": "i2", "yarn": "Y", "color": "", "lot": "", "fabric": "FX", "rem": 200.0, "at": "3"},
         {"id": "i3", "yarn": "T", "color": "BLUE", "lot": "", "fabric": "FT", "rem": 30.0, "at": "4"}]
META = ({("Y", "LOT1"): "", ("Y", "LOT2"): "", ("Y", "LOT3"): "", ("T", "T1"): "RED", ("T", "T2"): "BLUE"},
        {("Y", "LOT1"): 50, ("Y", "LOT2"): 100, ("Y", "LOT3"): 200, ("T", "T1"): 9, ("T", "T2"): 1}, "")
RULES = [{"customer": "NIKE", "item": "Y", "match": "exact", "lot": "LOT3"}]
yarn_ava_live._cache.clear()
orig = (yarn_stock_live.table, yarn_ava_live._reserved, yarn_lock.active_by_lot, yarn_item_lock.active,
        yarn_item_lock.state_key, ysc.lot_meta)
yarn_stock_live.table = lambda: {"source": {"file": "x.xlsx", "mtime": 1_800_000_000.0}, "rows": STOCK}
yarn_ava_live._reserved = lambda stock_at, notes: (RES, {"kp_reserved": 1, "kp_after_stock": 0, "kp_stale": 0})
yarn_lock.active_by_lot = lambda: {k: [dict(d) for d in v] for k, v in SCLOCK.items()}
yarn_item_lock.active = lambda: [dict(d) for d in ILOCK]
yarn_item_lock.state_key = lambda: ("t",)
ysc.lot_meta = lambda hide: META
try:
    out = yarn_ava_live.table((), 0.3, RULES)
finally:
    (yarn_stock_live.table, yarn_ava_live._reserved, yarn_lock.active_by_lot, yarn_item_lock.active,
     yarn_item_lock.state_key, _lm) = orig
    yarn_ava_live._cache.clear()
rows = {r["l"]: r for r in out["rows"]}
lk = lambda l: [(x["id"], x["kind"], x["kg"]) for x in rows[l]["locks"]]
check("LOT1: ล็อก Item i1 (เวลา 1) ได้ก่อนล็อก SC s1 (เวลา 2) → ใช้ได้ 10",
      (lk("LOT1"), rows["LOT1"]["ava"]), ([("i1", "item", 50.0), ("s1", "sc", 40.0)], 10.0))
check("i2 ทุก lot: lot เก่าสุดที่ไม่สงวน = LOT2 ; กันได้ = (10 + 100) − KP ไม่ระบุ lot 30 = 80",
      (lk("LOT2"), rows["LOT2"]["ava"]), ([("i2", "item", 80.0)], 20.0))
check("LOT3 สงวนให้ NIKE — ล็อกทุก lot ไม่แตะ", (lk("LOT3"), rows["LOT3"]["ava"], rows["LOT3"]["own"]),
      ([], 100.0, ["NIKE"]))
check("ด้าย TD: ล็อกสี BLUE ได้เฉพาะ lot สี BLUE", (lk("T1"), lk("T2")), ([], [("i3", "item", 30.0)]))
check("ยอดรวมล็อก = 50 + 40 + 80 + 30", out["stats"]["lock"], 200.0)

print("usable_stock — ล็อก Item แยกเป็นของผ้า (item) ไม่อยู่ในกองกลาง / ไม่ปนกับ own ของใบ")
orig = (yarn_ava_live.table, ysc.lot_meta)
yarn_ava_live.table = lambda *a, **k: out
ysc.lot_meta = lambda hide: META
try:
    lots, totals, _at, _note, own, item = ysc.usable_stock()
finally:
    yarn_ava_live.table, ysc.lot_meta = orig
check("กองกลาง Y = LOT2 20 + LOT1 10 (LOT3 สงวน) ; รวม − KP ไม่ระบุ lot 30 = 0",
      ([(d["lot"], d["kg"]) for d in lots[("Y", "")]], totals[("Y", "")]), ([("LOT2", 20.0), ("LOT1", 10.0)], 0.0))
check("item FX = LOT2 80 + LOT1 50 (lot เก่าก่อน)", [(d["lot"], d["kg"]) for d in item["FX"][("Y", "")]],
      [("LOT2", 80.0), ("LOT1", 50.0)])
check("item FT สี BLUE", [(d["lot"], d["kg"]) for d in item["FT"][("T", "BLUE")]], [("T2", 30.0)])
check("own ของใบมีแค่ล็อก SC", {k: [(d["lot"], d["kg"]) for d in v[("Y", "")]] for k, v in own.items()},
      {("1", "F"): [("LOT1", 40.0)]})

print("usable_stock(detail) — ยอดใช้ได้มาจากไหน (หน้าแผนด้าย): ในคลัง − ลูกเล็ก − สงวนลูกค้า − KP จอง − ล็อก = ใช้ได้")
detail = {}
orig = (yarn_ava_live.table, ysc.lot_meta)
yarn_ava_live.table = lambda *a, **k: out
ysc.lot_meta = lambda hide: META
try:
    _l, totals2, *_rest = ysc.usable_stock(detail)
finally:
    yarn_ava_live.table, ysc.lot_meta = orig
rnd = lambda d: {f: round(v, 3) for f, v in d.items()}
check("Y: ในคลัง 300 = ล็อก 50+40+80 + สงวน NIKE 100 + KP ไม่ระบุ lot 30 → ใช้ได้ 0", rnd(detail[("Y", "")]),
      {"stock": 300.0, "small": 0.0, "own": 100.0, "kp": 30.0, "lock": 170.0, "usable": 0.0})
check("T สี BLUE: ในคลัง 50 − ล็อก 30 = 20 ; สี RED ไม่หักอะไร", (rnd(detail[("T", "BLUE")]), rnd(detail[("T", "RED")])),
      ({"stock": 50.0, "small": 0.0, "own": 0.0, "kp": 0.0, "lock": 30.0, "usable": 20.0},
       {"stock": 50.0, "small": 0.0, "own": 0.0, "kp": 0.0, "lock": 0.0, "usable": 50.0}))
check("ใช้ได้ของ detail = totals ทุกด้าย", {k: round(d["usable"], 3) for k, d in detail.items()},
      {k: round(v, 3) for k, v in totals2.items()})
d = dict.fromkeys(ysc._DETAIL, 0.0)
ysc._split_lot(d, {"w": 40.0, "low": True, "small": 40.0}, -5.0)       # ลูกเล็กทั้ง lot (มีใบ KP จองด้วย)
ysc._split_lot(d, {"w": 100.0, "small": 10.0, "lk": 0.0}, -20.0)       # ใบ KP จอง 110 เกินที่ lot มี 90 → นับ 90
ysc._split_lot(d, {"w": 100.0, "small": 5.0, "lk": 15.0}, 60.0)        # ใบ KP จอง 20
check("ลูกเล็กทั้ง lot = ลูกเล็กทั้งก้อน ; KP จองเกิน lot นับเท่าที่ lot มี ; บวกกลับได้พอดี", rnd(d),
      {"stock": 240.0, "small": 55.0, "own": 0.0, "kp": 110.0, "lock": 15.0, "usable": 60.0})

# =============================================================================
print("add / remove — ด่าน: เกินยอด / ลูกเล็ก / ด้ายหลายสีต้องเลือก / ผ้าไม่ใช้ด้ายนี้ ; ไฟล์ + ประวัติ")
tmp = Path(tempfile.mkdtemp())
TABLE = {"yarns": [{"name": "Y", "ava": 50.0}, {"name": "T", "ava": 90.0}],
         "rows": [{"g": 0, "l": "LOT1", "w": 100.0, "low": False, "own": [], "ava": 60.0},
                  {"g": 0, "l": "SMALL", "w": 5.0, "low": True, "own": [], "ava": 0.0},
                  {"g": 0, "l": None, "w": None, "low": False, "own": [], "ava": -10.0},
                  {"g": 1, "l": "T1", "w": 50.0, "low": False, "own": [], "ava": 50.0},
                  {"g": 1, "l": "T2", "w": 50.0, "low": False, "own": [], "ava": 40.0}]}
FABS = {"Y": {"FA": {"pct": 50.0, "basis": "BOM ใบ KP", "scs": ["1"], "pending": 100.0, "need": 50.0}},
        "T": {"FT": {"pct": 30.0, "basis": "BOM ใบ KP", "scs": [], "pending": 0.0, "need": 0.0}}}
orig = (yarn_item_lock.LOCK_FILE, yarn_item_lock.LOG_FILE, yarn_item_lock.fabrics_for, yarn_item_lock._open_by_fabric,
        yarn_item_lock.statuses, ysc.lot_meta, livesync.bump, yarn_lock._SCFOL)
yarn_item_lock.LOCK_FILE, yarn_item_lock.LOG_FILE = tmp / "yarn_item_locks.json", tmp / "yarn_item_lock_log.jsonl"
yarn_item_lock.fabrics_for = lambda yarn, color="": FABS.get(str(yarn).upper(), {})
yarn_item_lock._open_by_fabric = lambda: {"FA": {"1"}}
yarn_item_lock.statuses = lambda locks=None: {}
ysc.lot_meta = lambda hide: ({("Y", "LOT1"): "", ("T", "T1"): "RED", ("T", "T2"): "BLUE"}, {}, "")
livesync.bump = lambda *a, **k: {}
yarn_lock._SCFOL = tmp / "ไม่มีไฟล์.xlsx"


def err(fn):
    try:
        fn()
    except yarn_item_lock.LockError as e:
        return str(e)
    return ""


def add(yarn, lot, color, fab, kg, note=""):
    return yarn_item_lock.add("yarnman", yarn, lot, color, fab, kg, note, lambda: TABLE)


try:
    check("lot LOT1 ขอ 70 > ใช้ได้ 60", err(lambda: add("Y", "LOT1", "", "FA", 70)).startswith("ด้ายไม่พอ"), True)
    check("lot ลูกเล็กทั้ง lot", "ลูกเล็ก" in err(lambda: add("Y", "SMALL", "", "FA", 1)), True)
    check("ทุก lot ขอ 55 > (60 − KP ไม่ระบุ lot 10 = 50)", err(lambda: add("Y", "", "", "FA", 55)).startswith("ด้ายไม่พอ"),
          True)
    check("ด้าย TD หลายสี ไม่ระบุ lot ต้องเลือกสี", "ต้องเลือกสี" in err(lambda: add("T", "", "", "FT", 10)), True)
    check("สีที่ไม่มี", "ไม่มี lot สี" in err(lambda: add("T", "", "GREEN", "FT", 10)), True)
    check("ผ้าที่ไม่ใช้ด้ายนี้", "ไม่ได้ใช้ด้าย" in err(lambda: add("Y", "LOT1", "", "ZZ", 10)), True)
    check("ไม่ระบุผ้า / kg ≤ 0", (err(lambda: add("Y", "LOT1", "", "", 10)), err(lambda: add("Y", "LOT1", "", "FA", 0))),
          ("ต้องเลือก Item ผ้า", "จำนวน kg ด้ายต้องมากกว่า 0"))
    r1 = add("y", "", "", "fa", 40, "เตรียมให้ลูกค้า")
    L1 = r1["lock"]
    check("ล็อกทุก lot ได้: ชื่อด้ายตามสต๊อก, lot ว่าง, ผ้าพิมพ์ใหญ่, kg ด้าย, คนล็อก, ไม่มีเตือน",
          (L1["yarn"], L1["lot"], L1["color"], L1["fabric"], L1["yarn_kg"], L1["by"], r1["warn"]),
          ("Y", "", "", "FA", 40.0, "yarnman", []))
    r2 = add("T", "", "blue", "FT", 30)
    check("ด้าย TD ล็อกทุก lot สี BLUE ได้ + เตือนไม่มีใบค้าง", (r2["lock"]["color"], len(r2["warn"]),
          "ไม่มีใบค้าง" in r2["warn"][0]), ("BLUE", 1, True))
    r3 = add("T", "T1", "", "FT", 10)
    check("ระบุ lot → สีตาม lot", r3["lock"]["color"], "RED")
    r4 = add("Y", "LOT1", "", "FA", 60)
    check("ล็อกเกินที่ใบค้างต้องใช้ (50) → เตือนส่วนเกิน", any("มากกว่าที่ใบค้าง" in w for w in r4["warn"]), True)
    check("ไฟล์ล็อกมี 4 ตัว", len(yarn_item_lock.read_locks()), 4)
    yarn_item_lock.remove("admin", L1["id"])
    check("ปลดแล้วเหลือ 3", [d["id"] for d in yarn_item_lock.read_locks()].count(L1["id"]), 0)
    check("ประวัติ: ปลด (ใหม่สุด) แล้ว ล็อก", [(e["action"], e["by"]) for e in yarn_item_lock.read_log()][:2],
          [("unlock", "admin"), ("lock", "yarnman")])
    check("ปลดซ้ำ → error", "ไม่พบล็อก" in err(lambda: yarn_item_lock.remove("admin", L1["id"])), True)
    yarn_item_lock.LOCK_FILE.write_text("{เสีย", encoding="utf-8")
    check("ไฟล์ล็อกเสีย → ไม่บันทึกทับ", "ยังไม่บันทึกทับ" in err(lambda: add("Y", "LOT1", "", "FA", 1)), True)
finally:
    (yarn_item_lock.LOCK_FILE, yarn_item_lock.LOG_FILE, yarn_item_lock.fabrics_for, yarn_item_lock._open_by_fabric,
     yarn_item_lock.statuses, ysc.lot_meta, livesync.bump, yarn_lock._SCFOL) = orig

# =============================================================================
print("yarn_plan.build — แผนด้าย: ล็อกของใบก่อน แล้วล็อก Item (ทุกใบของผ้า ตามสัปดาห์) + ยอดแผนใช้ต่อล็อก")


def prow(qty, wk, so, item):
    return {"PRODUCE_QTY": qty, "PLAN_WEEK": wk, "PLAN_YEAR": 2026, "SC_LINE_ID": "5", "SC_SO_NO": so,
            "ITEM_CODE": item, "PLAN_SOURCE": "NEW", "YARN_STOCK": "", "CUSTOMER": "", "ORDER_TYPE": "SC-ORDERS"}


YL = {5: {A: {"pct": 100.0, "loss": 1.0, "po_in": None}}}
plan = pd.DataFrame([prow(300, 44, "100", "FX"), prow(200, 44, "101", "FX"), prow(100, 45, "100", "FX"),
                     prow(100, 44, "300", "FY")])
d = yarn_plan.build(plan, YL, {A: 1000.0}, {("100", "FX"): {A: 50.0}}, item_kg={"FX": {A: 400.0}})
a = d["yarns"][0]
check("ล็อก SC 50 (W44) · ล็อก Item 400 (W44: ใบ 100 250 + ใบ 101 150) · สต๊อก = ใบ 101 50 + FY 100 ; W45 100",
      (a["lock"], a["ilock"], a["stock"], a["use"]), ([50.0, 0.0], [400.0, 0.0], [150.0, 100.0], [600.0, 100.0]))
check("คงเหลือนับเฉพาะที่ดึงกองกลาง", a["bal"], [850.0, 750.0])
check("ยอดแผนใช้ต่อล็อก", d["lock_use"], {"sc": {"100|FX|A|": [50.0, 0.0]}, "item": {"FX|A|": [400.0, 0.0]}})
r101 = next(x for x in d["rows"] if x["so"] == "101")
check("แถวรายใบมีช่อง ilock", (r101["ilock"], r101["stock"]), (150.0, 50.0))
check("ยอดที่ยังล็อกได้ของใบรวมส่วนล็อก Item", d["orders"]["101|FX"]["need"][0], 200.0)

# =============================================================================
print("supply_cap — ด้ายที่ล็อกให้ Item: ใบของ Item นั้นได้ ; ใบผ้าอื่นที่กำหนดส่งเร็วกว่าก็เอาไปไม่ได้")


class TL2:
    def __init__(self, n=12):
        s0 = pd.Timestamp("2026-10-02")
        self.starts = [s0 + pd.Timedelta(weeks=i) for i in range(n)]
        self.ends = [s + pd.Timedelta(days=6) for s in self.starts]
        self.keys = [202641 + i for i in range(n)]
        self.today_idx = 1

    def idx_of_date(self, dt):
        dt = pd.Timestamp(dt)
        for i, (s, e) in enumerate(zip(self.starts, self.ends)):
            if s <= dt <= e + pd.Timedelta(hours=23, minutes=59):
                return i
        return None


TODAY = pd.Timestamp("2026-10-15")
po = pd.DataFrame([{"PO": "P0", "ITEM": "A", "COLOR": "", "ORDER": 10.0, "REC": 10.0, "STATUS": "CLOSED",
                    "PIN": TODAY, "DATE": TODAY, "SUPPLIER": "ผู้ขาย"}])
po["REMAIN"] = 0.0
yl = {111: {A: {"pct": 100.0, "loss": 1.0, "po_in": None, "pos": []}}}
dem = pd.DataFrame([
    {"SC_NO": "S1", "SO_NO": "1", "FABRIC_ITEM": "FX", "PENDING_PLAN": 300.0, "FGWEEK": 202648, "SC_LINE_ID": "111"},
    {"SC_NO": "S2", "SO_NO": "2", "FABRIC_ITEM": "FY", "PENDING_PLAN": 200.0, "FGWEEK": 202646, "SC_LINE_ID": "111"},
])
stock = ({}, {A: 100.0}, "15/10", "", {}, {"FX": {A: [{"lot": "I1", "kg": 300.0, "age": 5}]}})
res = ysc.supply_cap(dem, TL2(), 1, 1, today=TODAY, ylines=yl, po=po, stock=stock, color_keys=set())
st = res["stats"]
check("S1 (FX) ด้ายพอจากล็อก Item ; S2 (FY กำหนดส่งเร็วกว่า) ได้แค่สต๊อก 100 ที่เหลือ UNPLANNED",
      (st["orders"], st["full"], st["short_orders"], st["item_lock_kg"], st["item_lock_used"]), (2, 1, 1, 300.0, 300.0))
check("UNPLANNED: S2 100", [(r["SO_NO"], r["PENDING_PLAN"]) for r in res["short"].to_dict("records")], [("2", 100.0)])
res0 = ysc.supply_cap(dem, TL2(), 1, 1, today=TODAY, ylines=yl, po=po, stock=stock[:5], color_keys=set())
check("เทียบ: ไม่ส่งล็อก Item (ข้อมูลรุ่น 5 ค่า) → S1 ไม่มีด้ายทั้งใบ (ด้ายถูกกันออกจากกองกลางแล้ว)",
      sorted((r["SO_NO"], r["PENDING_PLAN"]) for r in res0["short"].to_dict("records")), [("1", 300.0), ("2", 100.0)])

if FAILED:
    print(f"\n❌ ไม่ผ่าน {len(FAILED)} ข้อ")
    sys.exit(1)
print("\n✅ ผ่านทุกข้อ")

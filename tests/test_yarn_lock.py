"""
Unit test: ล็อกด้าย lot ให้ SC + ผ้า (webapp/backend/yarn_lock.py + YarnStockCover.py + yarn_ava_live.py)

กติกาที่ user ตัดสิน 15 ต.ค. 2569:
  - ล็อก = กันด้ายอย่างเดียว ไม่เปลี่ยนวัน ; กันเท่ายอดที่ล็อก ที่เหลือของ lot ใบอื่นใช้ได้
  - กรอก kg ผ้า → kg ด้าย = ผ้า × % × สูญเสีย
  - ตอนเช็กยืมสต๊อกแทนรอ PO ด้ายที่ล็อกนับเป็นของใบนั้น (ข้อ ก) — ใบอื่นใช้ไม่ได้
  - ออกใบ KP แล้วยอดล็อกลดเอง ; booking ปิดหมด = ไม่นับ

รันแบบ standalone (ไม่ต้องมีไฟล์ข้อมูล / ไม่ต้องต่อ Oracle — ไฟล์ล็อกลงโฟลเดอร์ชั่วคราว):
    python tests/test_yarn_lock.py
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
import yarn_lock  # noqa: E402
import yarn_ava_live  # noqa: E402
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
print("plan_consumption — ใบที่มีล็อกกินล็อกตัวเองก่อนตามสัปดาห์ ส่วนเกินค่อยกินกองกลาง")
ylines = {111: {A: {"pct": 100.0, "loss": 1.0, "po_in": None}},
          222: {A: {"pct": 100.0, "loss": 1.0, "po_in": None}}}
plan = pd.DataFrame([
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 300, "PLAN_WEEK": 13, "PLAN_YEAR": 2026, "SC_LINE_ID": "111",
     "SC_SO_NO": "100", "ITEM_CODE": "f1"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 600, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "111",
     "SC_SO_NO": "100.0", "ITEM_CODE": "F1"},
    {"PLAN_SOURCE": "NEW", "PRODUCE_QTY": 200, "PLAN_WEEK": 12, "PLAN_YEAR": 2026, "SC_LINE_ID": "222",
     "SC_SO_NO": "200", "ITEM_CODE": "F2"},
])


class TL:
    def idx(self, week, year=None):
        return int(week)


used = {}
cons = ysc.plan_consumption(plan, ylines, TL().idx, F, own={("100", "F1"): {A: 500.0}}, used=used)
check("กองกลาง: W12 = (600−500 ล็อก) + 200 ใบอื่น ; W13 = 300 (ล็อกหมดแล้ว)", rnd(cons[A]), {12: 300.0, 13: 300.0})
check("ล็อกถูกกินที่สัปดาห์แรกก่อน", used, {("100", "F1"): {A: {12: 500.0}}})
check("ไม่มีล็อก = เท่าเดิม", rnd(ysc.plan_consumption(plan, ylines, TL().idx, F)[A]), {12: 800.0, 13: 300.0})

# =============================================================================
print("select_borrowers — ข้อ ก: ด้ายที่ล็อกนับเป็นของใบนั้น ใบอื่นใช้ไม่ได้")


def cand(key, needs, own=None, color=False, due=50):
    c = {"key": key, "color": color, "due": due, "needs": needs}
    if own:
        c["own"] = own
    return c


x = cand("x", {A: (400.0, 13)}, own={A: {"kg": 350.0, "used": {}}})
check("ล็อก 350 + กองกลาง 100 พอ 400 → ยืมได้", list(ysc.select_borrowers([x], {}, {A: 100.0}, F)), ["x"])
check("ไม่มีล็อก กองกลาง 100 ไม่พอ", list(ysc.select_borrowers([cand("x", {A: (400.0, 13)})], {}, {A: 100.0}, F)), [])
check("ล็อกพอทั้งยอด ไม่ต้องมีกองกลางเลย",
      list(ysc.select_borrowers([cand("x", {A: (400.0, 13)}, own={A: {"kg": 500.0, "used": {}}})], {}, {}, F)), ["x"])
y = cand("y", {A: (100.0, 13)}, due=60)
check("ใบอื่นเอาด้ายที่ล็อกไม่ได้: x ยืมกองกลาง 50 → เหลือ 50 < 100 ของ y",
      list(ysc.select_borrowers([x, y], {}, {A: 100.0}, F)), ["x"])
check("ล็อกที่แผนกินไปแล้ว (300 ที่ W11) เหลือเครดิต 100 → ต้องยืม 300 ช่วง W11–13 : กองกลาง 250 ไม่พอ",
      list(ysc.select_borrowers([cand("x", {A: (400.0, 13)}, own={A: {"kg": 400.0, "used": {11: 300.0}}})],
                                {}, {A: 250.0}, F)), [])
check("… กองกลาง 300 พอ",
      list(ysc.select_borrowers([cand("x", {A: (400.0, 13)}, own={A: {"kg": 400.0, "used": {11: 300.0}}})],
                                {}, {A: 300.0}, F)), ["x"])

print("allocate_lots — lot ที่ล็อกให้ใบใช้ก่อน (ป้าย '(ล็อก)') แล้วค่อยกองกลาง")
r = ysc.allocate_lots(["x", "y"], {"x": {A: 400.0}, "y": {A: 400.0}}, {A: [{"lot": "G1", "kg": 500.0}]},
                      {"x": {A: [{"lot": "L1", "kg": 300.0}]}})
check("x = L1 (ล็อก) 300 + G1 100", r["x"], {A: [("L1 (ล็อก)", 300.0), ("G1", 100.0)]})
check("y ได้ G1 ที่เหลือ 400 (ไม่ได้แตะ L1)", r["y"], {A: [("G1", 400.0)]})

print("own_by_order — คีย์ล็อก (เลข SC) → คีย์ใบของแผน (SO_NO)")
dem = pd.DataFrame({"SC_NO": ["S708940", "S721655", "S721655"], "SO_NO": [625060500.0, 721655.0, 721655.0]})
o = ysc.own_by_order({("708940", "F"): {A: [{"lot": "L", "kg": 1.0}]},
                      ("721655", "G"): {A: [{"lot": "M", "kg": 2.0}]},
                      ("999999", "H"): {A: [{"lot": "N", "kg": 3.0}]}}, dem)
check("SO อีกระบบ / SO = SC / ไม่มีในดีมานด์", sorted(o), [("625060500", "F"), ("721655", "G"), ("999999", "H")])
check("_own_left หัก lot เก่าก่อน",
      ysc._own_left({"x": {A: [{"lot": "L1", "kg": 100.0}, {"lot": "L2", "kg": 100.0}]}}, {"x": {A: {12: 150.0}}}),
      {"x": {A: [{"lot": "L2", "kg": 50.0}]}})

# =============================================================================
print("yarn_lock ตัวช่วย")
check("sc_digits", [yarn_lock.sc_digits(v) for v in ("S721655", "721655", 721655.0, "s1", "X1", None)],
      ["721655", "721655", "721655", "1", "", ""])
check("_fr_scs แตก ' | ' ก่อน", yarn_lock._fr_scs("S721282-SC-ORDERS | S721257-CL-ORDERS | -SC-ORDERS"),
      ["721282", "721257"])

print("statuses — ใบ KP ที่ออกหลังล็อกตัดยอด (ล็อกเก่าก่อน) ; booking ปิดหมด = closed")
LOCKS = [
    {"id": "L1", "yarn": "Y", "lot": "a", "so": "100", "fabric": "F", "yarn_kg": 300.0, "at": "2026-10-15 10:00:00"},
    {"id": "L2", "yarn": "y", "lot": "b", "so": "100", "fabric": "f", "yarn_kg": 200.0, "at": "2026-10-15 10:30:00"},
    {"id": "L3", "yarn": "Y", "lot": "a", "so": "200", "fabric": "F", "yarn_kg": 50.0, "at": "2026-10-15 10:00:00"},
]
EV = {("100", "F", "Y"): [(datetime(2026, 10, 15, 9), "KP-OLD", 150.0),
                          (datetime(2026, 10, 15, 10, 15), "KP-A", 250.0),
                          (datetime(2026, 10, 15, 11), "KP-B", 100.0)]}
BK = {"rows": {("100", "F"): {"lines": set(), "pending": 0.0, "st": {"pending"}, "cus": ""},
               ("200", "F"): {"lines": set(), "pending": 0.0, "st": {"closed"}, "cus": ""}},
      "kp_sc": {}, "n": 3}
orig = (yarn_lock._booking, yarn_lock._kp_events)
yarn_lock._booking, yarn_lock._kp_events = (lambda: BK), (lambda: EV)
try:
    st = yarn_lock.statuses(LOCKS)
finally:
    yarn_lock._booking, yarn_lock._kp_events = orig
check("KP ออกก่อนล็อกไม่นับ ; KP-A 250 ตัด L1 ; KP-B 100 ตัด L1 50 + L2 50",
      [(st[i]["rem"], st[i]["state"]) for i in ("L1", "L2")], [(0.0, "done"), (150.0, "active")])
check("ใบ KP ที่ตัดแต่ละล็อก", [st["L1"]["kps"], st["L2"]["kps"]], [["KP-A", "KP-B"], ["KP-B"]])
check("booking ปิดหมด → closed", st["L3"]["state"], "closed")

print("_factor — แปลง kg ผ้า → kg ด้าย")
SC_LINES = {773910: (4.3, 1.05), 773911: (10.0, 1.0)}
orig = (yarn_lock._booking, yarn_lock._bom)
yarn_lock._booking = lambda: {"rows": {("1", "FA"): {"lines": {773910}, "pending": 0, "st": {"pending"}, "cus": ""},
                                       ("1", "FX"): {"lines": {999999}, "pending": 0, "st": {"pending"}, "cus": ""}},
                              "kp_sc": {}, "n": 2}
yarn_lock._bom = lambda: {("FB", "Y"): 20.0}
try:
    f = yarn_lock._factor("1", "FA", "Y", SC_LINES, 3)
    check("บรรทัดใบของ booking", (f["pct"], f["loss"], f["basis"]), (4.3, 1.05, "บรรทัดใบ 773910"))
    check("factor = % × สูญเสีย", f["factor"], 4.3 / 100 * 1.05)
    check("บรรทัดของผ้านี้ไม่ใช้ด้ายนี้ → None", yarn_lock._factor("1", "FX", "Y", SC_LINES, 3), None)
    f = yarn_lock._factor("1", "FB", "Y", SC_LINES, 3)
    check("ไม่มีบรรทัด → % จาก BOM, สูญเสียสูงสุดของใบ", (f["pct"], f["loss"]), (20.0, 1.05))
    check("หลายผ้า ไม่มี BOM → None", yarn_lock._factor("1", "FC", "Y", SC_LINES, 3), None)
    f = yarn_lock._factor("1", "FC", "Y", {5: (7.5, 1.0), 6: (7.5, 1.1)}, 1)
    check("ใบมีผ้าตัวเดียว % ค่าเดียว", (f["pct"], f["loss"]), (7.5, 1.1))
finally:
    yarn_lock._booking, yarn_lock._bom = orig

# =============================================================================
print("add / remove — ด่านกันล็อกเกินยอดใช้ได้ / lot ลูกเล็ก ; ไฟล์ + ประวัติ")
tmp = Path(tempfile.mkdtemp())
orig = (yarn_lock.LOCK_FILE, yarn_lock.LOG_FILE, yarn_lock.options, yarn_lock.statuses, livesync.bump)
yarn_lock.LOCK_FILE, yarn_lock.LOG_FILE = tmp / "yarn_locks.json", tmp / "yarn_lock_log.jsonl"
yarn_lock.options = lambda yarn, lot, hide=(): {
    "yarn": yarn, "lot": lot, "color": "", "note": "",
    "scs": [{"sc": "S721655", "so": "721655", "cus": "", "fabrics": [
        {"fabric": "FA", "pct": 50.0, "loss": 1.0, "factor": 0.5, "basis": "บรรทัดใบ 1"}]}]}
yarn_lock.statuses = lambda locks=None: {}
livesync.bump = lambda *a, **k: {}
TABLE = {"yarns": [{"name": "Y"}],
         "rows": [{"g": 0, "l": "LOT1", "w": 500.0, "low": False, "own": [], "ava": 100.0},
                  {"g": 0, "l": "SMALL", "w": 5.0, "low": True, "own": [], "ava": 0.0}]}


def err(fn):
    try:
        fn()
    except yarn_lock.LockError as e:
        return str(e)
    return ""


try:
    check("ผ้า 300 = ด้าย 150 > ใช้ได้ 100 → ไม่ให้ล็อก",
          err(lambda: yarn_lock.add("u", "Y", "LOT1", "S721655", "FA", 300, "", lambda: TABLE)).startswith("ด้ายไม่พอ"),
          True)
    check("lot ลูกเล็กทั้ง lot → ไม่ให้ล็อก",
          "ลูกเล็ก" in err(lambda: yarn_lock.add("u", "Y", "SMALL", "S721655", "FA", 10, "", lambda: TABLE)), True)
    check("ผ้าที่ไม่ใช้ด้ายนี้ → ไม่ให้ล็อก",
          "ไม่ได้ใช้ด้าย" in err(lambda: yarn_lock.add("u", "Y", "LOT1", "721655", "ZZ", 10, "", lambda: TABLE)), True)
    res = yarn_lock.add("u", "y", "LOT1", "721655", "fa", 200, "ลูกค้าเร่ง", lambda: TABLE)
    L = res["lock"]
    check("ล็อกได้: เก็บ SC/SO, kg ผ้า/ด้าย, คนล็อก",
          (L["sc"], L["so"], L["fabric"], L["fabric_kg"], L["yarn_kg"], L["by"]),
          ("S721655", "721655", "FA", 200.0, 100.0, "u"))
    check("ไฟล์ล็อกมี 1 ตัว", [d["id"] for d in yarn_lock.read_locks()], [L["id"]])
    yarn_lock.remove("admin", L["id"])
    check("ปลดแล้วไฟล์ว่าง", yarn_lock.read_locks(), [])
    check("ประวัติ: ปลด (ใหม่สุด) แล้ว ล็อก", [(e["action"], e["by"]) for e in yarn_lock.read_log()],
          [("unlock", "admin"), ("lock", "u")])
    check("ปลดซ้ำ → error", "ไม่พบล็อก" in err(lambda: yarn_lock.remove("admin", L["id"])), True)
    yarn_lock.LOCK_FILE.write_text("{เสีย", encoding="utf-8")
    check("ไฟล์ล็อกเสีย → ไม่บันทึกทับ",
          "ยังไม่บันทึกทับ" in err(lambda: yarn_lock.add("u", "Y", "LOT1", "S721655", "FA", 10, "", lambda: TABLE)), True)
finally:
    (yarn_lock.LOCK_FILE, yarn_lock.LOG_FILE, yarn_lock.options, yarn_lock.statuses, livesync.bump) = orig

# =============================================================================
print("yarn_ava_live.table — ใช้ได้ = w − ลูกเล็ก − จองไว้ − ล็อก ; กันได้ไม่เกินที่ lot มี")


def srow(item, lot, w, y):
    return {"i": item, "l": lot, "p": "YRK1", "w": w, "y": y, "yw": round(w / y, 2), "b": ""}


STOCK = [srow("Y", "LOT1", 100.0, 50), srow("Y", "LOT2", 100.0, 50), srow("Y", "SMALL", 5.0, 50)]
RES = {("Y", "LOT1"): {"kg": 20.0, "kps": {"KP1": 20.0}}}
LOCKMAP = {("Y", "LOT1"): [{"id": "a", "sc": "S1", "so": "1", "fabric": "F", "rem": 50.0, "at": "1"},
                           {"id": "b", "sc": "S2", "so": "2", "fabric": "F", "rem": 50.0, "at": "2"}],
           ("Y", "SMALL"): [{"id": "c", "sc": "S3", "so": "3", "fabric": "F", "rem": 5.0, "at": "1"}]}
yarn_ava_live._cache.clear()
orig = (yarn_stock_live.table, yarn_ava_live._reserved, yarn_lock.active_by_lot, yarn_lock.state_key)
yarn_stock_live.table = lambda: {"source": {"file": "x.xlsx", "mtime": 1_800_000_000.0}, "rows": STOCK}
yarn_ava_live._reserved = lambda stock_at, notes: (RES, {"kp_reserved": 1, "kp_after_stock": 0, "kp_stale": 0})
yarn_lock.active_by_lot = lambda: LOCKMAP
yarn_lock.state_key = lambda: ("t",)
try:
    out = yarn_ava_live.table((), 0.3, [])
    t_used = out
finally:
    yarn_stock_live.table, yarn_ava_live._reserved, yarn_lock.active_by_lot, yarn_lock.state_key = orig
    yarn_ava_live._cache.clear()
rows = {r["l"]: r for r in out["rows"]}
check("LOT1: 100 − จอง 20 = 80 → ล็อก a 50 เต็ม, b ได้ 30 ; ใช้ได้ 0",
      (rows["LOT1"]["lk"], rows["LOT1"]["lk_want"], rows["LOT1"]["ava"], [x["kg"] for x in rows["LOT1"]["locks"]]),
      (80.0, 100.0, 0.0, [50.0, 30.0]))
check("LOT2 ไม่มีล็อก ใช้ได้ 100", (rows["LOT2"]["lk"], rows["LOT2"]["ava"]), (0.0, 100.0))
check("lot ลูกเล็กทั้ง lot กันไม่ได้", (rows["SMALL"]["lk"], rows["SMALL"]["locks"][0]["kg"]), (0.0, 0.0))
check("ยอดรวม: ล็อก 80 · ใช้ได้รวม 100", (out["stats"]["lock"], out["stats"]["ava"]), (80.0, 100.0))

print("usable_stock — กองกลางหักล็อกแล้ว + own ของใบ")
orig = (yarn_ava_live.table, ysc.lot_meta)
yarn_ava_live.table = lambda *a, **k: t_used
ysc.lot_meta = lambda hide: ({}, {}, "")
try:
    lots, totals, _at, _note, own, _item = ysc.usable_stock()
finally:
    yarn_ava_live.table, ysc.lot_meta = orig
check("กองกลาง: เหลือ LOT2 100 (LOT1 ใช้ได้ 0)", lots, {("Y", ""): [{"lot": "LOT2", "kg": 100.0, "age": 0.0}]})
check("own ของใบ 1 / 2", (own[("1", "F")][("Y", "")][0]["kg"], own[("2", "F")][("Y", "")][0]["kg"]), (50.0, 30.0))
check("lot ลูกเล็กไม่ให้ own", ("3", "F") in own, False)

if FAILED:
    print(f"\n❌ ไม่ผ่าน {len(FAILED)} ข้อ")
    sys.exit(1)
print("\n✅ ผ่านทุกข้อ")

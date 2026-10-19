"""
Unit test: ลูกเล็กของหน้า Yarn Ava ตัด "ทีละแถว" (webapp/backend/yarn_ava_live.py)

user เปลี่ยนจาก "เฉลี่ยทั้ง lot" 15 ต.ค. 2569 — หลอดเกือบเปล่าที่คืนจากใบ KP (PO_NO = เลข KP) ดึงค่าเฉลี่ยของ lot ลง
จนลูกเต็มจาก PO ซื้อถูกตัดไปด้วย (PC24/1R TRT-4X1-2608: YRK691626 285.9 kg 2.27 kg/ลูก + ด้ายคืน 25.9 kg 1,370 ลูก
→ เฉลี่ย 0.208 → เดิมใช้ได้ 0)
  - แถวที่ YARN_WEIGHT < ขั้นต่ำ ไม่นับ kg ของแถวนั้น ; แถวอื่นใน lot เดียวกันยังใช้ได้
  - ใช้ได้ = TOTAL_WEIGHT − ลูกเล็ก − จองไว้ (ติดลบได้ = จองเกินด้ายที่ใช้ได้)
  - low = ทั้ง lot เป็นลูกเล็ก (YarnStockCover ใช้ข้าม lot)

รันแบบ standalone (ไม่ต้องมีไฟล์ข้อมูล / ไม่ต้องต่อ Oracle):
    python tests/test_yarn_ava_small_rows.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "webapp" / "backend"))
import yarn_ava_live  # noqa: E402
import yarn_stock_live  # noqa: E402

FAILED = []


def check(name, got, want):
    ok = abs(got - want) < 1e-6 if isinstance(want, float) and got is not None else got == want
    if ok:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got!r}\n     want = {want!r}")
        FAILED.append(name)
    assert ok, name


def row(item, lot, po, w, y, yw="auto"):
    return {"i": item, "l": lot, "p": po, "w": w, "y": y,
            "yw": (round(w / y, 2) if y else None) if yw == "auto" else yw, "b": ""}


STOCK = [
    # PC24/1R TRT-4X1-2608 จากไฟล์จริง 15 ต.ค. — ลูกเต็มจาก PO ซื้อ + หลอดเปล่าที่คืนจากใบ KP 2 แถว
    row("PC24/1R", "TRT-4X1-2608", "YRK691626", 285.8842, 126),
    row("PC24/1R", "TRT-4X1-2608", "KP2641CK0149XXXNA", 15.93, 840),
    row("PC24/1R", "TRT-4X1-2608", "KP2641CK0149XXXNA", 10.0, 530),
    # lot ที่เหลือแต่ลูกเล็ก
    row("PC24/1R", "TRT-OLD", "KP2601CK0001REV", 5.0, 50),
    # ขอบ: YARN_WEIGHT = 0.30 พอดี → ไม่ใช่ลูกเล็ก (หน้า Stock Yarn ลงสีเขียว)
    row("PC24/1R", "TRT-EDGE", "KP2602CK0001REV", 3.0, 10, yw=0.3),
    # ไฟล์ไม่มีค่า YARN_WEIGHT ในแถว → คิด kg ÷ ลูก เอง (2 ÷ 20 = 0.1)
    row("PC24/1R", "TRT-NOYW", "KP2603CK0001REV", 2.0, 20, yw=None),
    # จองเกินด้ายที่ใช้ได้
    row("CM30/1R", "NYT-1", "YRK1", 100.0, 50),
    row("CM30/1R", "NYT-1", "KP2604CK0001REV", 1.0, 10),
    # PO ที่ซ่อน (-D) ไม่นับทั้งยอดและลูกเล็ก
    row("CM30/1R", "NYT-1", "YRK2-D", 4.0, 400),
]
RES = {
    ("CM30/1R", "NYT-1"): {"kg": 120.0, "kps": {"KP2644CK0001REV": 120.0}},
    ("CM30/1R", None): {"kg": 30.0, "kps": {"KP2644CK0002REV": 30.0}},
}


def run(stock_rows, res, cone_min=0.3):
    yarn_ava_live._cache.clear()
    orig_t, orig_r = yarn_stock_live.table, yarn_ava_live._reserved
    yarn_stock_live.table = lambda: {"source": {"file": "x.xlsx", "mtime": 1_800_000_000.0}, "rows": stock_rows}
    yarn_ava_live._reserved = lambda stock_at, notes: (res, {"kp_reserved": 2, "kp_after_stock": 0, "kp_stale": 0})
    try:
        return yarn_ava_live.table(("-D", "-T"), cone_min, [])
    finally:
        yarn_stock_live.table, yarn_ava_live._reserved = orig_t, orig_r
        yarn_ava_live._cache.clear()


out = run(STOCK, RES)
names = [y["name"] for y in out["yarns"]]
lot = {(names[r["g"]], r["l"]): r for r in out["rows"]}

print("ลูกเต็มที่ปนหลอดเปล่าในก้อนเดียวกัน ยังใช้ได้")
r = lot[("PC24/1R", "TRT-4X1-2608")]
check("ลูกเล็ก = 2 แถวด้ายคืน 25.93 kg", r["small"], 25.93)
check("จำนวนแถวลูกเล็ก", r["small_n"], 2)
check("จำนวนลูกของแถวลูกเล็ก", r["small_cones"], 1370.0)
check("ใช้ได้ = ลูกเต็ม 285.884 kg (เกณฑ์เดิมเฉลี่ยทั้ง lot 0.208 → 0)", r["ava"], 285.884)
check("ไม่ใช่ lot ลูกเล็กทั้ง lot", r["low"], False)

print("lot ที่เหลือแต่ลูกเล็ก")
r = lot[("PC24/1R", "TRT-OLD")]
check("ใช้ได้ 0", r["ava"], 0.0)
check("low = ทั้ง lot เป็นลูกเล็ก", r["low"], True)

print("ขอบและแถวที่ไม่มีค่า YARN_WEIGHT")
check("YARN_WEIGHT 0.30 พอดี ไม่ใช่ลูกเล็ก", lot[("PC24/1R", "TRT-EDGE")]["ava"], 3.0)
check("ไม่มี YARN_WEIGHT → 2 ÷ 20 = 0.1 เป็นลูกเล็ก", lot[("PC24/1R", "TRT-NOYW")]["small"], 2.0)

print("จองเกินด้ายที่ใช้ได้ + PO ที่ซ่อน")
r = lot[("CM30/1R", "NYT-1")]
check("TOTAL_WEIGHT ไม่นับแถว -D", r["w"], 101.0)
check("ลูกเล็กไม่นับแถว -D (เหลือ 1 kg)", r["small"], 1.0)
check("ใช้ได้ = 101 − 1 − 120 = −20 (ติดลบให้เห็น)", r["ava"], -20.0)
check("ใบที่ยังไม่ระบุ lot หักที่ด้าย", lot[("CM30/1R", None)]["ava"], -30.0)
cm = out["yarns"][names.index("CM30/1R")]
check("รวมทั้งด้าย CM30/1R = −50", cm["ava"], -50.0)
check("ลูกเล็กรวมทั้งด้าย", cm["small"], 1.0)

print("ยอดรวมหัวตาราง: ใช้ได้ = TOTAL_WEIGHT − ลูกเล็ก − จองไว้ ลงตัว")
st = out["stats"]
check("TOTAL_WEIGHT", st["stock"], 422.814)
check("ลูกเล็ก kg", st["small_kg"], 33.93)
check("ลูกเล็ก แถว", st["small_rows"], 5)
check("ลูกเล็ก lot", st["small_lots"], 4)
check("ใช้ได้รวม", st["ava"], round(st["stock"] - st["small_kg"] - st["res"], 3))

print("ขั้นต่ำมาจากหน้า Monitor Yarn")
out2 = run(STOCK, {}, cone_min=2.5)
lot2 = {([y["name"] for y in out2["yarns"]][r["g"]], r["l"]): r for r in out2["rows"]}
check("ขั้นต่ำ 2.5 → ลูก 2.27 kg ก็เป็นลูกเล็ก ทั้ง lot ใช้ไม่ได้", lot2[("PC24/1R", "TRT-4X1-2608")]["low"], True)

print("\nผ่านทุกข้อ" if not FAILED else f"\nไม่ผ่าน {len(FAILED)} ข้อ")

"""
Unit test: จำนวนเครื่องรายสัปดาห์ (ชีท MC_Total) = ยอด "ทั้งกอง" รวมเครื่องที่ MC Special กันไว้

บั๊กที่เทสต์นี้กันไม่ให้กลับมา (2 ต.ค. 2569 — PHET DOUBLE-34 28G: IRM 9 + IRMPL 4 = 13, กัน COTTON 4)
  1. กรอก 14 ที่หน้าเครื่องจักร → AVA_MC เขียน 14 ลง "พูลปกติ" ตรง ๆ ทั้งที่เครื่อง COTTON 4 ตัว
     ยังนับแยกอยู่ → ระบบเห็นเครื่อง 14 + 4 = 18 (ของจริง 14) แผนวางงานลงเครื่องที่ไม่มี
  2. สัปดาห์ที่ยังไม่มี booking (W53/2026) ไม่มีแถวในตารางเครื่องตอนเขียนทับ → ค่าที่ตั้งไว้
     ถูกข้ามเงียบ ๆ แล้วได้ยอดทะเบียนแทน — ต้องเขียนทับ "หลัง" เติมสัปดาห์อนาคต

รันแบบ standalone:
    python tests/test_mc_total_pile.py
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import McTotal                   # noqa: E402

FAILED = []


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got}\n     want = {want}")
        FAILED.append(name)


def gauge(v):
    return str(v).strip().upper().replace("GAUGE", "").replace("G", "").replace(".0", "").strip()


# พูลจำลอง (ตัวเลขจากทะเบียนจริง ณ 2 ต.ค. 2569)
P34 = "PHET|DOUBLE-34:28"                 # IRM 9 + IRMPL 4 = 13, กัน COTTON 4 → ปกติ 9
P36 = "PHET|SKPLE , SKPTA 36G"            # 20 ตัว, กัน POLY 13 → ปกติ 7
SKP = "PHET|SKP 26G"                      # 5 ตัว ไม่มีกันไว้
SKX = "PHET|SKPLE , SKPTA 26G"            # 49 ตัว ไม่มีกันไว้
XC = "PHET|IIP 20G , RL 18G"              # พูลข้าม cat: โผล่ทั้ง DOUBLE-36 20 และ DOUBLE-40 18
TOTALS = {P34: 9, f"{P34}:COTTON": 4, P36: 7, f"{P36}:POLY": 13, SKP: 5, SKX: 49, XC: 12}
MC_TO_POOL = {("IRM", "28"): P34, ("IRMPL", "28"): P34, ("SKP", "26"): SKP,
              ("SKPLE", "26"): SKX, ("SKPTA", "26"): SKX, ("SKPLE", "36"): P36, ("SKPTA", "36"): P36}

ROWS = [   # (FACTORY, MC_CAT, GUAGE, MC_POOL, booking ใช้)
    ("PHET", "DOUBLE-34", 28, P34, 9),
    ("PHET", "SINGLE-32", 36, P36, 3),
    ("PHET", "SINGLE-32", 26, SKP, 2),
    ("PHET", "SINGLE-32", 26, SKX, 30),
    ("PHET", "DOUBLE-36", 20, XC, 4),
    ("PHET", "DOUBLE-40", 18, XC, 4),
]


def make_summary(weeks):
    out = []
    for w in weeks:
        for fac, cat, g, pool, use in ROWS:
            tot = TOTALS[pool]
            out.append({"FACTORY": fac, "MC_CAT": cat, "GUAGE": g, "MC_POOL": pool,
                        "TOTAL_MC": tot, "WEEK": w, "MC_USE_CEIL": use, "TOTAL_MC_REMAIN": tot - use})
    return pd.DataFrame(out)


def normal(df, week, pool):
    """ยอดพูลปกติของ (สัปดาห์, พูล) — ทุกแถวของพูลต้องถือค่าเดียวกัน ไม่งั้นคืนทั้งชุดให้เทสต์ฟ้อง"""
    v = sorted(set(df[(df["WEEK"] == week) & (df["MC_POOL"] == pool)]["TOTAL_MC"].astype(int)))
    return v[0] if len(v) == 1 else v


def run(overrides, weeks=(202642, 202643)):
    df = make_summary(weeks)
    delta, applied, skipped = McTotal.apply_override(
        df, overrides, MC_TO_POOL, TOTALS, gauge, log=lambda *_: None)
    return df, delta, applied, skipped


def test_split_pile():
    print("\n[1] split_pile — ยอดที่กรอก = ทั้งกอง รวมเครื่องกันไว้")
    check("กรอก 14, กัน 4 → พูลปกติ 10 (ไม่ใช่ 14)", McTotal.split_pile(14, [9], [4]), ([14], [10]))
    check("กรอกเท่ายอดทะเบียน 13 → พูลปกติ 9 เท่าไม่กรอก", McTotal.split_pile(13, [9], [4]), ([13], [9]))
    check("ไม่มีเครื่องกันไว้ → ได้เท่าที่กรอก", McTotal.split_pile(72, [69], [0]), ([72], [72]))
    check("กรอกน้อยกว่าที่กันไว้ → พูลปกติ 0 ไม่ติดลบ", McTotal.split_pile(3, [9], [4]), ([3], [0]))
    # 2 พูล: ปกติ 5 (ไม่กัน) + ปกติ 7 กัน 13 (เต็ม 20) รวม 25 → กรอก 30 กระจายตามยอดเต็ม 6 + 24
    check("หลายพูล กระจายตามยอดเต็ม แล้วหักกันไว้ของพูลนั้น",
          McTotal.split_pile(30, [5, 7], [0, 13]), ([6, 24], [6, 11]))
    full, norm = McTotal.split_pile(27, [5, 7], [0, 13])
    check("หลายพูล: ผลรวมยอดเต็ม = ยอดที่กรอกเสมอ", sum(full), 27)


def test_apply():
    print("\n[2] apply_override — เขียนทับตารางเครื่อง")
    df, delta, applied, _ = run({(202642, "DOUBLE-34", "28", "PHET", ""): 14})
    check("W42 พูลปกติ = 10", normal(df, 202642, P34), 10)
    check("W42 ปกติ + COTTON = 14 (ไม่ใช่ 18)", normal(df, 202642, P34) + TOTALS[f"{P34}:COTTON"], 14)
    check("W42 เครื่องเหลือ = 10 − 9 = 1",
          int(df[(df["WEEK"] == 202642) & (df["MC_POOL"] == P34)]["TOTAL_MC_REMAIN"].iloc[0]), 1)
    check("W43 ไม่ได้ตั้ง → ยอดทะเบียน 9 เหมือนเดิม", normal(df, 202643, P34), 9)
    check("ส่วนต่างให้ชีทรายงาน = +1 (13 → 14) ไม่ใช่ +5", delta[(202642, "DOUBLE-34", "28")], 1)
    check("พูลอื่นไม่ถูกแตะ", [normal(df, 202642, p) for p in (P36, SKP, SKX, XC)], [7, 5, 49, 12])

    df, *_ = run({(202642, "DOUBLE-34", "28", "PHET", ""): 13})
    check("กรอกเท่ายอดทะเบียน → ตารางเท่าไม่กรอก", normal(df, 202642, P34), 9)

    df, *_ = run({(202642, "SINGLE-32", "36", "PHET", "SKPTA"): 22})
    check("ระบุเครื่อง (SKPTA 36G: 20 → 22, กัน POLY 13) → พูลปกติ 9", normal(df, 202642, P36), 9)

    df, *_ = run({(202642, "SINGLE-32", "26", "PHET", "SKP"): 3})
    check("ระบุเครื่อง SKP → แตะเฉพาะพูล SKP", [normal(df, 202642, SKP), normal(df, 202642, SKX)], [3, 49])

    df, *_ = run({(202642, "DOUBLE-36", "20", "PHET", ""): 10})
    check("พูลข้าม cat → ทุกแถวของพูลได้ยอดเดียวกัน", normal(df, 202642, XC), 10)


def test_future_week():
    print("\n[3] สัปดาห์ที่ยังไม่มี booking — ต้องเขียนทับหลังเติมสัปดาห์อนาคต")
    ov = {(w, "DOUBLE-34", "28", "PHET", ""): 14 for w in (202652, 202653)}
    hz = [202652, 202653]
    # ตารางก่อนเติม: มีแค่ W52 (W53 ยังไม่มี booking) = ลำดับเก่าที่เป็นบั๊ก
    df, _d, applied, skipped = run(ov, weeks=(202652,))
    bad = McTotal.verify(df, applied, skipped, TOTALS, gauge, horizon=hz)
    check("เขียนทับก่อนเติม → ตัวตรวจต้องฟ้องว่า W53 ไม่ถูกใช้", len(bad), 1)
    # ตารางหลังเติมครบ horizon = ลำดับที่ถูก
    df, _d, applied, skipped = run(ov, weeks=(202652, 202653))
    check("เขียนทับหลังเติม → W53 พูลปกติ 10", normal(df, 202653, P34), 10)
    check("เขียนทับหลังเติม → ตัวตรวจผ่าน",
          McTotal.verify(df, applied, skipped, TOTALS, gauge, horizon=hz), [])
    # สัปดาห์ที่ผ่านไปแล้ว (นอก horizon) หาแถวไม่เจอได้ ไม่ถือว่าผิด
    df, _d, applied, skipped = run({(202632, "DOUBLE-34", "28", "PHET", ""): 14}, weeks=(202652,))
    check("สัปดาห์ในอดีตหาแถวไม่เจอ → ไม่ฟ้อง",
          McTotal.verify(df, applied, skipped, TOTALS, gauge, horizon=hz), [])


def test_verify_catches_double_count():
    print("\n[4] ตัวตรวจจับบั๊กนับเครื่องกันไว้ซ้ำ")
    df, _d, applied, skipped = run({(202642, "DOUBLE-34", "28", "PHET", ""): 14})
    check("ผลที่ถูก → ตัวตรวจผ่าน", McTotal.verify(df, applied, skipped, TOTALS, gauge), [])
    # จำลองบั๊กเดิม: เขียนยอดที่กรอกลงพูลปกติตรง ๆ (14 + COTTON 4 = 18)
    df.loc[(df["WEEK"] == 202642) & (df["MC_POOL"] == P34), "TOTAL_MC"] = 14
    bad = McTotal.verify(df, applied, skipped, TOTALS, gauge)
    check("พูลปกติ = ยอดที่กรอก (บั๊กเดิม) → ตัวตรวจฟ้อง", len(bad), 1)
    # รายการที่ทับกันโดยตั้งใจ (ไม่ระบุเครื่อง + ระบุเครื่อง สัปดาห์เดียวกัน) ต้องไม่ฟ้องผิด
    df, _d, applied, skipped = run({(202642, "SINGLE-32", "26", "PHET", ""): 50,
                                    (202642, "SINGLE-32", "26", "PHET", "SKP"): 3})
    check("แถวระบุเครื่องชนะเฉพาะพูลนั้น", normal(df, 202642, SKP), 3)
    check("รายการทับกันโดยตั้งใจ → ไม่ฟ้อง", McTotal.verify(df, applied, skipped, TOTALS, gauge), [])
    df, _d, applied, skipped = run({(202642, "DOUBLE-34", "28", "PHET", ""): 3})
    check("กรอกน้อยกว่าที่กันไว้ → พูลปกติ 0 และไม่ฟ้อง",
          [normal(df, 202642, P34), McTotal.verify(df, applied, skipped, TOTALS, gauge)], [0, []])


def test_ava_mc_order():
    print("\n[5] AVA_MC.py — ลำดับขั้นตอน")
    src = (ROOT / "AVA_MC.py").read_text(encoding="utf-8")
    fill = src.find("# เติมสัปดาห์อนาคตให้ครบ horizon")
    call = src.find("McTotal.apply_override(")
    check("AVA_MC เรียก McTotal.apply_override", call > 0, True)
    check("เขียนทับ MC_Total อยู่หลังบล็อกเติมสัปดาห์อนาคต", 0 < fill < call, True)
    check("AVA_MC เรียกตัวตรวจ McTotal.verify หลังเขียนทับ", src.find("McTotal.verify(") > call, True)


def main():
    print("=" * 70)
    print("TEST: MC_Total = ยอดทั้งกอง (รวมเครื่องกันไว้ POLY/COTTON) + สัปดาห์ที่ยังไม่มี booking")
    print("=" * 70)
    test_split_pile()
    test_apply()
    test_future_week()
    test_verify_catches_double_count()
    test_ava_mc_order()
    print("\n" + "=" * 70)
    if FAILED:
        print(f"❌ FAIL {len(FAILED)} เคส: {', '.join(FAILED)}")
        return 1
    print("✅ ผ่านทั้งหมด")
    return 0


if __name__ == "__main__":
    sys.exit(main())

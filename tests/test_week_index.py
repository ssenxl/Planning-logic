"""
Unit test สำหรับ primitive ข้ามปี: wk_year / wk_num / week_index (+ next_week ใน Stage 2)

รันแบบ standalone (ไม่ต้องต่อ Oracle / ไม่ต้องโหลด Planning.py ทั้งไฟล์):
    python tests/test_week_index.py

ทดสอบ logic ของ week_index บน synthetic calendar 2 ปี (เลข week ซ้ำข้ามปี)
เพื่อยืนยันว่าเลือกปีถูก ทั้งแบบ composite (YYYYWW) และ bare week (nearest-TODAY)
"""
import pandas as pd


def _make_calendar():
    """calendar_week จำลอง: ปี 2026 W30-W52 + ปี 2027 W1-W52 (timeline ต่อเนื่องตามวันที่)"""
    rows = []
    for wk in range(30, 53):           # 2026 W30..W52
        rows.append((2026, wk))
    for wk in range(1, 53):            # 2027 W1..W52
        rows.append((2027, wk))
    df = pd.DataFrame(rows, columns=["YEAR", "WEEK"]).reset_index(drop=True)
    df["YW_INT"] = df["YEAR"] * 100 + df["WEEK"]
    return df


# ---- reimplement primitives standalone (ตรรกะเดียวกับ Planning.py Stage 1) ----
def wk_year(week):
    if week is None:
        return None
    w = int(week)
    return w // 100 if w >= 100000 else None


def wk_num(week):
    if week is None:
        return None
    w = int(week)
    return w % 100 if w >= 100000 else w


def make_week_index(calendar_week, today_idx):
    def week_index(week):
        if week is None:
            return None
        w = int(week)
        if w >= 100000:
            idx = calendar_week.index[calendar_week["YW_INT"] == w]
            return None if len(idx) == 0 else int(idx[0])
        idx = list(calendar_week.index[calendar_week["WEEK"] == w])
        if not idx:
            return None
        if today_idx is None:
            return int(idx[0])
        return int(min(idx, key=lambda i: (abs(int(i) - today_idx), int(i))))
    return week_index


def run():
    cw = _make_calendar()
    # TODAY = 2026 W30 -> row 0
    today_idx = int(cw.index[cw["YW_INT"] == 202630][0])
    wi = make_week_index(cw, today_idx)

    def row_yw(idx):
        return int(cw.iloc[idx]["YEAR"]), int(cw.iloc[idx]["WEEK"])

    fails = []

    def check(desc, got, expect):
        if got != expect:
            fails.append(f"[FAIL] {desc}: got {got}, expect {expect}")
        else:
            print(f"[ok] {desc}: {got}")

    # 1) composite -> ปีถูกต้องเสมอ
    check("composite 202705 -> (2027,5)", row_yw(wi(202705)), (2027, 5))
    check("composite 202652 -> (2026,52)", row_yw(wi(202652)), (2026, 52))
    check("composite 202730 -> (2027,30)", row_yw(wi(202730)), (2027, 30))
    check("composite 202650 -> (2026,50)", row_yw(wi(202650)), (2026, 50))

    # 2) bare week ที่มีปีเดียว (ไม่ชน) -> ตรงตัว
    check("bare 1 (มีแต่ 2027) -> (2027,1)", row_yw(wi(1)), (2027, 1))
    check("bare 15 (มีแต่ 2027) -> (2027,15)", row_yw(wi(15)), (2027, 15))
    check("bare 30 -> nearest TODAY = (2026,30)", row_yw(wi(30)), (2026, 30))

    # 3) bare week ที่ชนข้ามปี -> เลือกใกล้ TODAY (2026 ฝั่ง W30-52)
    check("bare 46 (ชน 2026/2027) -> nearest = (2026,46)", row_yw(wi(46)), (2026, 46))
    check("bare 52 (ชน) -> nearest = (2026,52)", row_yw(wi(52)), (2026, 52))

    # 4) helpers
    check("wk_year(202705)", wk_year(202705), 2027)
    check("wk_num(202705)", wk_num(202705), 5)
    check("wk_year(46) bare", wk_year(46), None)
    check("wk_num(46) bare", wk_num(46), 46)

    # 5) ไม่มีในปฏิทิน
    check("composite 209901 -> None", wi(209901), None)

    print("\n" + ("=" * 50))
    if fails:
        print(f"FAILED {len(fails)}:")
        for f in fails:
            print("  " + f)
        return 1
    print("ALL PASSED")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(run())

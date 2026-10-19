"""
Unit test: โควตา job setup ของ "งานในแผนที่ผู้ใช้ตรึงไว้" (Lock_Plan SOURCE=PLAN)

ปัญหาที่เทสต์นี้กัน:
    รอบ replan คืนเครื่อง+โควตา job ของสัปดาห์ที่ปลดล็อกให้หมด แล้ววางใหม่บนส่วนที่เหลือ
    งานที่ผู้ใช้ตรึงไว้ถูกกัน "เครื่อง" ผ่าน SUMMARY_MC_REMAIN แล้ว แต่ "โควตา job" เป็นคนละ
    บัญชี ซึ่ง Planning pre-populate จากชีท DETAIL เท่านั้น — งานที่ตรึงไม่ได้อยู่ใน DETAIL
    → ถ้าไม่ส่งผ่านชีท PINNED_JOBS รอบ replan จะแจก setup เกิน cap ของสัปดาห์นั้น

รันแบบ standalone (ไม่ต้องต่อ Oracle / ไม่ต้องรัน Planning ทั้งไฟล์):
    python tests/test_pinned_setup_jobs.py
"""
import functools
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import Replan


FAILED = []

# ไฟล์ที่ test_write_sheet เขียนไว้ให้ test_planning_side ใช้ต่อ
_WRITTEN: Path = None


def _tmp_dir(tmp: Path = None) -> Path:
    """โฟลเดอร์ชั่วคราวของเทสต์

    รับ tmp มา (โหมด standalone ที่ main() ส่งให้) ก็ใช้อันนั้น ; ไม่ส่งมา (pytest เรียกตรง ๆ)
    ก็สร้างเอง — พารามิเตอร์ต้องมีค่า default ไม่งั้น pytest ตีความว่าเป็น fixture ชื่อ 'tmp'
    แล้ว error ทุกครั้งเพราะไม่มี fixture ชื่อนี้ (เคยเป็นแบบนั้น: pytest ไม่เคยรันสองเทสต์นี้เลย)"""
    if tmp is not None:
        tmp.mkdir(parents=True, exist_ok=True)
        return tmp
    tmp = Path(__file__).resolve().parent / "_tmp_pinned_jobs"
    tmp.mkdir(exist_ok=True)
    return tmp


def check(name, got, want):
    if got == want:
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}\n     got  = {got}\n     want = {want}")
        FAILED.append(name)


# รันใต้ pytest อยู่หรือเปล่า — check() แค่เก็บผลไว้สรุปตอนท้าย (โหมด standalone อยากเห็นครบทุกเคส)
# แต่ใต้ pytest ถ้าไม่ assert เทสต์จะขึ้น "ผ่าน" ทั้งที่ค่าผิด
_UNDER_PYTEST = "pytest" in sys.modules


def _checked(fn):
    """ใต้ pytest: เคสไหนไม่ผ่านให้เทสต์ fail จริง ; รันเดี่ยว ๆ: เก็บไว้สรุปตอนจบเหมือนเดิม"""
    @functools.wraps(fn)
    def wrap(*a, **kw):
        start = len(FAILED)
        out = fn(*a, **kw)
        bad = FAILED[start:]
        if _UNDER_PYTEST and bad:
            raise AssertionError("เคสที่ไม่ผ่าน: " + " · ".join(bad))
        return out
    return wrap


# =========================================================================
# 1) pinned_setup_jobs — นับ job จากแถวแผนที่ตรึงไว้
# =========================================================================
@_checked
def test_pinned_setup_jobs():
    print("\n[1] pinned_setup_jobs")

    pinned = pd.DataFrame([
        # งานเดียวกัน 3 แถว (Planning เขียน NEW_MC ซ้ำทุกแถวของเครื่องชุดเดียวกัน)
        # → ต้องได้ 4 ไม่ใช่ 12
        {"PLAN_WEEK": 35, "ITEM_CODE": "AAA", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 4},
        {"PLAN_WEEK": 35, "ITEM_CODE": "AAA", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 4},
        {"PLAN_WEEK": 35, "ITEM_CODE": "AAA", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 4},
        # item อื่น เครื่องกลุ่มเดียวกัน สัปดาห์เดียวกัน → บวกเพิ่ม
        {"PLAN_WEEK": 35, "ITEM_CODE": "BBB", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 2},
        # เกจต่างกัน = คนละงาน → บวกเพิ่ม (แต่รวมลงกลุ่มเครื่องเดียวกัน)
        {"PLAN_WEEK": 35, "ITEM_CODE": "AAA", "MC_GROUP": "IIP", "MC_GUAGE": "24", "NEW_MC": 1},
        # carryover ล้วน ไม่ต้อง setup → ไม่กินโควตา
        {"PLAN_WEEK": 35, "ITEM_CODE": "CCC", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 0},
        # ผ้านำเข้า ไม่ใช่เครื่องทอ → ไม่กินโควตา
        {"PLAN_WEEK": 35, "ITEM_CODE": "DDD", "MC_GROUP": "IMPORT", "MC_GUAGE": "20", "NEW_MC": 9},
        # คนละสัปดาห์ / คนละกลุ่มเครื่อง → คนละช่อง
        {"PLAN_WEEK": 36, "ITEM_CODE": "AAA", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 3},
        {"PLAN_WEEK": 35, "ITEM_CODE": "EEE", "MC_GROUP": "RL", "MC_GUAGE": "18", "NEW_MC": 5},
        # NEW_MC ว่าง → ข้าม (ไม่ระเบิด)
        {"PLAN_WEEK": 35, "ITEM_CODE": "FFF", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": None},
        # สัปดาห์ว่าง → ข้าม
        {"PLAN_WEEK": None, "ITEM_CODE": "GGG", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 7},
    ])

    got = Replan.pinned_setup_jobs(pinned)
    check("รวมต่อ (สัปดาห์, กลุ่มเครื่อง) ถูกต้อง", got, {
        (35, "IIP"): 7,     # AAA G20 max=4 + BBB G20 2 + AAA G24 1 ; CCC=0, IMPORT/None ตัดทิ้ง
        (35, "RL"): 5,
        (36, "IIP"): 3,
    })

    check("pinned ว่าง → {}", Replan.pinned_setup_jobs(pd.DataFrame()), {})

    # กันชื่อกลุ่มเครื่องพิมพ์เล็ก/มีช่องว่าง (ไฟล์แผนเขียนมาไม่คงรูป)
    messy = pd.DataFrame([
        {"PLAN_WEEK": 40, "ITEM_CODE": "xxx", "MC_GROUP": " iip ", "MC_GUAGE": "20", "NEW_MC": 2},
        {"PLAN_WEEK": 40, "ITEM_CODE": "XXX", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 2},
    ])
    check("normalize เครื่อง/item แล้วยุบเป็นงานเดียว", Replan.pinned_setup_jobs(messy), {(40, "IIP"): 2})


# =========================================================================
# 2) write_replan_capacity — ยอดถูกเขียนลงชีท PINNED_JOBS จริง
# =========================================================================
def _fake_booking_file(tmp: Path) -> Path:
    """ไฟล์ booking_final ปลอมขั้นต่ำเท่าที่ write_replan_capacity ต้องใช้"""
    src = tmp / "fake_booking_final.xlsx"
    summary = pd.DataFrame([
        {"YEAR": 2026, "WEEK": 35, "MC_CAT": "DOUBLE", "GUAGE": "20", "MC_POOL": "",
         "TOTAL_MC": 30, "MC_USE_CEIL": 10, "TOTAL_MC_REMAIN": 20},
        {"YEAR": 2026, "WEEK": 36, "MC_CAT": "DOUBLE", "GUAGE": "20", "MC_POOL": "",
         "TOTAL_MC": 30, "MC_USE_CEIL": 8, "TOTAL_MC_REMAIN": 22},
    ])
    detail = pd.DataFrame([
        {"YEAR": 2026, "WEEK": 34, "ITEM_CODE": "ZZZ", "MC_GROUP": "IIP", "GUAGE": "20",
         "MC_CAT": "DOUBLE", "POOL_TYPE": "", "MC_USE_CEIL": 3},
    ])
    with pd.ExcelWriter(src, engine="openpyxl") as w:
        detail.to_excel(w, sheet_name="DETAIL", index=False)
        summary.to_excel(w, sheet_name="SUMMARY_MC_REMAIN", index=False)
    return src


@_checked
def test_write_sheet(tmp: Path = None):
    print("\n[2] write_replan_capacity → ชีท PINNED_JOBS")

    tmp = _tmp_dir(tmp)
    src = _fake_booking_file(tmp)
    out = tmp / "booking_final_replan_TEST.xlsx"
    _orig = Replan.BOOKING_REPLAN_FILE
    Replan.BOOKING_REPLAN_FILE = out          # กันไม่ให้ทับไฟล์จริงใน data_plan
    try:
        Replan.write_replan_capacity(
            src,
            locked_detail=pd.read_excel(src, sheet_name="DETAIL"),
            unlocked_weeks={35, 36},
            unlocked_yw={(2026, 35), (2026, 36)},
            pinned_mc=None,
            pinned_jobs={(35, "IIP"): 7, (36, "IIP"): 3},
        )
    finally:
        Replan.BOOKING_REPLAN_FILE = _orig

    xls = pd.ExcelFile(out)
    check("มีชีท PINNED_JOBS", "PINNED_JOBS" in xls.sheet_names, True)
    pj = xls.parse("PINNED_JOBS")
    check("คอลัมน์ครบ", list(pj.columns), ["WEEK", "MC_GROUP", "JOBS"])
    check("ค่าที่เขียนถูกต้อง",
          {(int(r.WEEK), r.MC_GROUP): int(r.JOBS) for r in pj.itertuples()},
          {(35, "IIP"): 7, (36, "IIP"): 3})

    # ไม่มีงานตรึง → ยังต้องมีชีท (ว่าง) ให้ Planning เห็นรูปเดิมทุกครั้ง
    Replan.BOOKING_REPLAN_FILE = out
    try:
        Replan.write_replan_capacity(src, pd.read_excel(src, sheet_name="DETAIL"),
                                     {35, 36}, {(2026, 35), (2026, 36)})
    finally:
        Replan.BOOKING_REPLAN_FILE = _orig
    pj2 = pd.ExcelFile(out).parse("PINNED_JOBS")
    check("ไม่มีงานตรึง → ชีทว่างแต่มีหัวคอลัมน์", (len(pj2), list(pj2.columns)),
          (0, ["WEEK", "MC_GROUP", "JOBS"]))
    # เก็บไว้ให้ขั้นถัดไปใช้ต่อ — ไม่ return เพราะ pytest เตือนเมื่อฟังก์ชันเทสต์คืนค่า
    global _WRITTEN
    _WRITTEN = out


# =========================================================================
# 3) ฝั่ง Planning — บวกเข้า weekly_job_usage ถูกช่อง
#    (reimplement ตรรกะเดียวกับ Planning.py บล็อก REPLAN_MODE / PINNED_JOBS
#     เพราะ import Planning.py จะรันแผนทั้งไฟล์)
# =========================================================================
_JOB_EXCLUDE_MC = {"IMPORT"}


def ingest_pinned_jobs(pin_jobs: pd.DataFrame, weekly_job_usage: dict,
                       week_index, today_idx: int) -> int:
    added = 0
    if pin_jobs.empty:
        return 0
    pin_jobs.columns = pin_jobs.columns.str.strip().str.upper()
    for _, r in pin_jobs.iterrows():
        wk = pd.to_numeric(r.get("WEEK"), errors="coerce")
        jb = pd.to_numeric(r.get("JOBS"), errors="coerce")
        mc = str(r.get("MC_GROUP", "")).strip().upper()
        if pd.isna(wk) or pd.isna(jb) or int(jb) <= 0 or not mc:
            continue
        if mc in _JOB_EXCLUDE_MC:
            continue
        wk = int(wk)
        idx = week_index(wk)
        if idx is None or idx < today_idx:
            continue
        weekly_job_usage.setdefault(wk, {})
        weekly_job_usage[wk][mc] = weekly_job_usage[wk].get(mc, 0) + int(jb)
        added += int(jb)
    return added


@_checked
def test_planning_side(out: Path = None):
    print("\n[3] Planning ingest PINNED_JOBS → weekly_job_usage")

    if out is None:                       # รันเดี่ยว ๆ ใต้ pytest → สร้างไฟล์ของขั้นก่อนเอง
        test_write_sheet()
        out = _WRITTEN

    # ปฏิทินจำลอง: W34 = สัปดาห์ปัจจุบัน
    weeks = list(range(30, 45))
    idx_of = {w: i for i, w in enumerate(weeks)}
    today_idx = idx_of[34]

    # ✅ กรณีจริง: DETAIL ของงานที่ล็อกกินไป 5 job ที่ W35 อยู่แล้ว
    usage = {35: {"IIP": 5}}
    pj = pd.DataFrame([
        {"WEEK": 35, "MC_GROUP": "IIP", "JOBS": 7},
        {"WEEK": 36, "MC_GROUP": "IIP", "JOBS": 3},
    ])
    added = ingest_pinned_jobs(pj, usage, lambda w: idx_of.get(w), today_idx)
    check("บวกทับของเดิมในสัปดาห์เดียวกัน", usage, {35: {"IIP": 12}, 36: {"IIP": 3}})
    check("จำนวน job ที่บวกเข้าไป", added, 10)

    # cap PHET_DOUBLE = 33 → เหลือให้ replan วางใหม่ 21 ไม่ใช่ 28
    check("job slot ที่เหลือของ W35 หลังหักงานตรึง", 33 - usage[35]["IIP"], 21)

    # สัปดาห์ที่ผ่านไปแล้ว / ไม่มีในปฏิทิน → ไม่กินโควตา
    usage2 = {}
    added2 = ingest_pinned_jobs(
        pd.DataFrame([
            {"WEEK": 31, "MC_GROUP": "IIP", "JOBS": 4},     # ก่อน TODAY
            {"WEEK": 99, "MC_GROUP": "IIP", "JOBS": 4},     # ไม่มีในปฏิทิน
            {"WEEK": 35, "MC_GROUP": "IMPORT", "JOBS": 9},  # ไม่ใช่เครื่องทอ
            {"WEEK": 35, "MC_GROUP": "", "JOBS": 2},        # กลุ่มเครื่องว่าง
            {"WEEK": 35, "MC_GROUP": "IIP", "JOBS": 0},     # ศูนย์
        ]),
        usage2, lambda w: idx_of.get(w), today_idx,
    )
    check("แถวที่ต้องข้ามถูกข้ามทั้งหมด", (usage2, added2), ({}, 0))

    # อ่านจากไฟล์ที่ Replan เขียนจริง แล้วเข้า Planning ได้ครบ
    usage3 = {}
    added3 = ingest_pinned_jobs(pd.ExcelFile(out).parse("PINNED_JOBS"), usage3,
                                lambda w: idx_of.get(w), today_idx)
    check("ชีทว่างจากไฟล์จริง → ไม่บวกอะไร", (usage3, added3), ({}, 0))


# =========================================================================
# 4) pinned_mc_use — เครื่องของงานที่ตรึงต้องนับ "ต่อบล็อก" ไม่ใช่ต่อแถว
#    1 บล็อก = (สัปดาห์, item, กลุ่มเครื่อง, เกจ) ใช้เครื่องชุดเดียว แต่ Planning เขียน
#    หลายแถว (แถวละใบสั่ง) แล้วแปะ ACTUAL_MC ของทั้งบล็อกซ้ำทุกแถว
# =========================================================================
@_checked
def test_pinned_mc_use():
    print("\n[4] pinned_mc_use — ยุบแถวซ้ำของบล็อกเดียวกัน")

    _orig = Replan._mc_cat_pool_maps
    # IIP G22 อยู่ในพูล PHET ; RAOO G16 ไม่อยู่ในพูล (ใช้ MC_CAT)
    Replan._mc_cat_pool_maps = lambda src: (
        {("IIP", "22"): "DOUBLE-30", ("RAOO", "16"): "SINGLE-32"},   # cat_map
        {("IIP", "22"): "PHET|IIP 22G"},                             # pool_map
    )
    try:
        # เคสจริงจาก replan_result.xlsx: W36 มี 6 เครื่อง → W37 carry 6 + setup 2 = 8
        # สองแถว (คนละใบสั่ง) เขียน ACTUAL_MC = 8 ทั้งคู่ → ต้องได้ 8 ไม่ใช่ 16
        pinned = pd.DataFrame([
            {"PLAN_WEEK": 37, "ITEM_CODE": "F6DRY/05A0", "MC_GROUP": "IIP", "MC_GUAGE": 22,
             "ACTUAL_MC": 8, "PRODUCE_QTY": 2365.12},
            {"PLAN_WEEK": 37, "ITEM_CODE": "F6DRY/05A0", "MC_GROUP": "IIP", "MC_GUAGE": 22,
             "ACTUAL_MC": 8, "PRODUCE_QTY": 2860.02},
            # แถว MC_SHARED (ACTUAL_MC=0) — ใช้เครื่องร่วม ไม่ประกาศเครื่องเอง
            {"PLAN_WEEK": 37, "ITEM_CODE": "F6DRY/05A0", "MC_GROUP": "IIP", "MC_GUAGE": 22,
             "ACTUAL_MC": 0, "PRODUCE_QTY": 435.11},
            # item อื่นในพูลเดียวกัน สัปดาห์เดียวกัน → บวกเพิ่ม (คนละบล็อก)
            {"PLAN_WEEK": 37, "ITEM_CODE": "OTHER", "MC_GROUP": "IIP", "MC_GUAGE": 22,
             "ACTUAL_MC": 3, "PRODUCE_QTY": 900},
            # นอกพูล → เข้าฝั่ง MC_CAT ; สองแถวคนละใบ เครื่องชุดเดียวกัน
            {"PLAN_WEEK": 38, "ITEM_CODE": "FD3BAS", "MC_GROUP": "RAOO", "MC_GUAGE": 16,
             "ACTUAL_MC": 1, "PRODUCE_QTY": 1219.16},
            {"PLAN_WEEK": 38, "ITEM_CODE": "FD3BAS", "MC_GROUP": "RAOO", "MC_GUAGE": 16,
             "ACTUAL_MC": 1, "PRODUCE_QTY": 342.52},
        ])
        pool, nonpool = Replan.pinned_mc_use(pinned, Path("dummy"))
        check("พูล: 8 (บล็อกที่ซ้ำ) + 3 (อีก item) = 11 ไม่ใช่ 22",
              pool, {("PHET|IIP 22G", 37): 11.0})
        check("นอกพูล: 1 เครื่องต่อบล็อก ไม่ใช่ 2",
              nonpool, {("SINGLE-32", "16", 38): 1.0})

        check("pinned ว่าง → ({}, {})", Replan.pinned_mc_use(pd.DataFrame(), Path("x")), ({}, {}))

        # เครื่องต่างสัปดาห์ = คนละบล็อก ต้องไม่ยุบข้ามสัปดาห์
        two_weeks = pd.DataFrame([
            {"PLAN_WEEK": 37, "ITEM_CODE": "A", "MC_GROUP": "IIP", "MC_GUAGE": 22, "ACTUAL_MC": 4},
            {"PLAN_WEEK": 38, "ITEM_CODE": "A", "MC_GROUP": "IIP", "MC_GUAGE": 22, "ACTUAL_MC": 4},
        ])
        p2, _ = Replan.pinned_mc_use(two_weeks, Path("x"))
        check("คนละสัปดาห์ = คนละบล็อก",
              p2, {("PHET|IIP 22G", 37): 4.0, ("PHET|IIP 22G", 38): 4.0})
    finally:
        Replan._mc_cat_pool_maps = _orig


# =========================================================================
# 5) pinned_setup_jobs_by_item + ชีท PINNED_JOBS_ITEM — โควตาตั้งเครื่องรายผ้า
#    (user 30 ก.ย. 2569: งานตรึงตั้งผ้า A ไปแล้ว ใบใหม่ของผ้า A สัปดาห์เดียวกันต้องเห็นโควตาที่ใช้ไป)
# =========================================================================
@_checked
def test_pinned_jobs_by_item(tmp: Path = None):
    print("\n[5] pinned_setup_jobs_by_item → ชีท PINNED_JOBS_ITEM")

    pinned = pd.DataFrame([
        # แถวซ้ำของบล็อกเดียวกัน → max ไม่ใช่ผลรวม
        {"PLAN_WEEK": 35, "ITEM_CODE": "aaa", "MC_GROUP": " iip ", "MC_GUAGE": "20.0", "NEW_MC": 2},
        {"PLAN_WEEK": 35, "ITEM_CODE": "AAA", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 2},
        {"PLAN_WEEK": 35, "ITEM_CODE": "BBB", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 1},
        {"PLAN_WEEK": 35, "ITEM_CODE": "CCC", "MC_GROUP": "IIP", "MC_GUAGE": "20", "NEW_MC": 0},
        {"PLAN_WEEK": 35, "ITEM_CODE": "DDD", "MC_GROUP": "IMPORT", "MC_GUAGE": "20", "NEW_MC": 9},
        # งานจ้างทอ (ชื่อผู้รับจ้างภาษาไทย) ไม่ได้ใช้ช่างเรา → ไม่นับ
        {"PLAN_WEEK": 35, "ITEM_CODE": "EEE", "MC_GROUP": "ผู้รับจ้าง", "MC_GUAGE": "20", "NEW_MC": 3},
    ])
    by_item = Replan.pinned_setup_jobs_by_item(pinned)
    check("แยกรายผ้า ยุบแถวซ้ำด้วย max", by_item,
          {(35, "AAA", "IIP", "20"): 2, (35, "BBB", "IIP", "20"): 1})
    # ผลรวมรายผ้าต้องเท่ากับชีทเดิม (สัปดาห์×กลุ่มเครื่อง) เสมอ — สองชีทมาจากชุดเดียวกัน
    check("รวมแล้วเท่ากับ pinned_setup_jobs", Replan.pinned_setup_jobs(pinned), {(35, "IIP"): 3})
    check("pinned ว่าง → {}", Replan.pinned_setup_jobs_by_item(pd.DataFrame()), {})

    tmp = _tmp_dir(tmp)
    src = _fake_booking_file(tmp)
    out = tmp / "booking_final_replan_ITEM_TEST.xlsx"
    _orig = Replan.BOOKING_REPLAN_FILE
    Replan.BOOKING_REPLAN_FILE = out
    try:
        Replan.write_replan_capacity(
            src, pd.read_excel(src, sheet_name="DETAIL"), {35, 36}, {(2026, 35), (2026, 36)},
            pinned_jobs={(35, "IIP"): 3}, pinned_item_jobs=by_item,
        )
        pji = pd.ExcelFile(out).parse("PINNED_JOBS_ITEM")
        check("คอลัมน์ชีทใหม่", [c for c in pji.columns if c != "YEAR"],
              ["WEEK", "ITEM_CODE", "MC_GROUP", "GUAGE", "JOBS"])
        check("ค่าที่เขียน",
              {(int(r.WEEK), r.ITEM_CODE, r.MC_GROUP, str(r.GUAGE)): int(r.JOBS)
               for r in pji.itertuples()},
              {(35, "AAA", "IIP", "20"): 2, (35, "BBB", "IIP", "20"): 1})
        check("ชีทเดิม PINNED_JOBS ไม่เปลี่ยนรูป",
              list(pd.ExcelFile(out).parse("PINNED_JOBS").columns), ["WEEK", "MC_GROUP", "JOBS"])

        # ไม่มีงานตรึง → ชีทใหม่ยังต้องมี (ว่าง) ให้ Planning เห็นรูปเดิม
        Replan.write_replan_capacity(src, pd.read_excel(src, sheet_name="DETAIL"),
                                     {35, 36}, {(2026, 35), (2026, 36)})
        pji2 = pd.ExcelFile(out).parse("PINNED_JOBS_ITEM")
        check("ไม่มีงานตรึง → ชีทว่างมีหัวคอลัมน์", (len(pji2), "ITEM_CODE" in pji2.columns), (0, True))
    finally:
        Replan.BOOKING_REPLAN_FILE = _orig


def main():
    print("=" * 70)
    print("TEST: งานที่ผู้ใช้ล็อกไว้ (Lock_Plan) — เครื่อง + โควตา job ที่ถูกกันไว้")
    print("=" * 70)

    tmp = _tmp_dir()

    test_pinned_setup_jobs()
    test_write_sheet(tmp)
    test_planning_side(_WRITTEN)
    test_pinned_mc_use()
    test_pinned_jobs_by_item(tmp)

    print("\n" + "=" * 70)
    if FAILED:
        print(f"❌ FAIL {len(FAILED)} เคส: {', '.join(FAILED)}")
        return 1
    print("✅ ผ่านทั้งหมด")
    return 0


if __name__ == "__main__":
    sys.exit(main())

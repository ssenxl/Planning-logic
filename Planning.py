import pandas as pd
import io
import re
from datetime import date
from pathlib import Path
from Calendar import load_calendar, calendar_week_map
from ITEM_Cap import load_item_cap_data
from Yarn_Master import load_yarn_master

# =========================
# CONFIG

# =========================
BASE_DIR = Path(__file__).parent
TODAY = pd.Timestamp.now().normalize()
DATA_PLAN_DIR = BASE_DIR / "data_plan"
DATA_DIR = BASE_DIR / "data"
CAP_DIR = DATA_DIR / "Cap"
ITEMCORE_DIR = DATA_DIR / "Itemcore"
ORDER_FILE = DATA_PLAN_DIR / "order_ready.xlsx"
MC_REMAIN_FILE = DATA_PLAN_DIR / "booking_final_ready25.xlsx"
ITEM_CAP_FILE = CAP_DIR / "item_cap2025.xlsx"
ITEMCORE_FILE = ITEMCORE_DIR / "Itemcore.xlsx"
CALENDAR_FILE = BASE_DIR / "Calendar.xlsx"
BOOKING_DIR = BASE_DIR / "Booking"
OUTPUT_FILE = DATA_PLAN_DIR / "weekly_production_plan.xlsx"
SETUP_DAYS = 3
SETUP_GAP_WEEK = 2
# Week ที่ไม่ต้องการวางแผน (เช่น สัปดาห์หยุด/ปิดโรงงาน)
SKIP_WEEKS = {16}
# Allow carryover even when SC/SO changes (user option)
ALLOW_CARRYOVER_ACROSS_SO = True
# Progressive machine reduction: เริ่มต้นด้วยเครื่องเยอะ แล้วค่อยๆ ลดลงให้ทัน target
USE_PROGRESSIVE_REDUCTION = False
# MAX_SETUP_MC แบบ static ถูกยกเลิก → ใช้ _dynamic_setup_limit() แทน (dynamic ตาม urgency RDD)

# =========================
# SHARED MACHINE POOL
# กลุ่ม MC ที่ใช้เครื่องร่วมกัน — ต้องตรงกับ AVA_MC.py

# =========================
SHARED_POOL_MAP = {
    "SKP_SKPTA_14_POOL": (5, [("SKP", "14"), ("SKPTA", "14")]),
    "SKPLE_SKPTA_26_POOL": (40, [("SKPLE", "26"), ("SKPTA", "26")]),
    "SKPLE_SKPTA_36_POOL": (19, [("SKPLE", "36"), ("SKPTA", "36")]),
    "IIP_RL_POOL": (10, [("IIP", "20"), ("RL", "18")]),
    "RAO_RAP_19_POOL": (14, [("RAO", "19"), ("RAP", "19")]),
    "IIP_II_24_POOL": (3, [("IIP", "24"), ("II", "24")]),
    "GAUGE28_POOL": (
        47,
        [
            ("IBLTA", "28"),
            ("RAP", "28"),
            ("RAP60", "28"),
            ("RAP98", "28"),
            ("SYN", "28"),
        ],
    ),
    "GAUGE22_POOL": (
        65,
        [
            ("IBLTA", "22"),
            ("IBP", "22"),
            ("RAO", "22"),
            ("RAP", "22"),
            ("RAP60", "22"),
            ("RAP98", "22"),
            ("SYN", "22"),
        ],
    ),
}
# สร้าง lookup: (MC_GROUP, GUAGE) → list of all pool members
_POOL_MEMBER_LOOKUP: dict = {}
for _pname, (_ptotal, _pmembers) in SHARED_POOL_MAP.items():
    for _mk in _pmembers:
        _POOL_MEMBER_LOOKUP[_mk] = _pmembers

# =========================
# LOAD DATA

# =========================
orders = pd.read_excel(ORDER_FILE)
summary_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="SUMMARY_MC_REMAIN")
detail_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="DETAIL")  # โหลด DETAIL
item_cap_data = load_item_cap_data()
master_mc = pd.read_excel(BASE_DIR / "data" / "MC" / "Master_MC_5.xlsx")
# โหลด Itemcore สำหรับเช็ค RTS items
try:
    itemcore_df = pd.read_excel(ITEMCORE_FILE)
    itemcore_df.columns = itemcore_df.columns.str.strip()
except Exception as e:
    print(f"⚠️ ไม่สามารถโหลด Itemcore: {e}")
    itemcore_df = pd.DataFrame()
calendar = load_calendar(CALENDAR_FILE, sheet_name="Sheet1")
calendar_week = calendar_week_map(calendar)
orders.columns = orders.columns.str.strip()
summary_mc.columns = summary_mc.columns.str.strip().str.upper()
calendar_week.columns = calendar_week.columns.str.strip().str.upper()
item_cap_data.columns = item_cap_data.columns.str.strip()
# Normalize ITEM_CODE values to strip whitespace for consistent matching
item_cap_data["ITEM_CODE"] = item_cap_data["ITEM_CODE"].astype(str).str.strip()
detail_mc.columns = detail_mc.columns.str.strip().str.upper()  # เพิ่ม detail_mc
master_mc.columns = master_mc.columns.str.strip()
# สร้าง lookup dictionary สำหรับ Itemcore: {item_code: customer}
itemcore_lookup = {}
if not itemcore_df.empty:
    for _, row in itemcore_df.iterrows():
        item = str(row.get('Item code', row.get('Item code ', ''))).strip().upper()
        customer = str(row.get('Customer', '')).strip()
        if item:
            itemcore_lookup[item] = customer
# Gauge lookup: (ITEM_CODE, MC_GROUP) → GUAGE string
# ใช้เป็น fallback เมื่อ gauge ไม่ได้มาจาก data source โดยตรง
_item_mc_to_gauge = {}


def _normalize_gauge(gauge) -> str:
    """Normalize gauge key so values like 22 and 22.0 are treated as the same."""
    if gauge is None or (isinstance(gauge, float) and pd.isna(gauge)):
        return ""

    s = str(gauge).strip()
    if not s or s.lower() == "nan":
        return ""

    # Canonicalize numeric-like values (e.g. 22.0 -> 22)
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]

    return s

for _, _r in item_cap_data.iterrows():
    _ic = str(_r.get("ITEM_CODE", "")).strip().upper()
    _mc = str(_r.get("MC_GROUP", "")).strip().upper()
    _gg = _r.get("GUAGE")
    _gs = _normalize_gauge(_gg)
    if _ic and _mc and _gs and _gs.lower() != "nan":
        _item_mc_to_gauge[(_ic, _mc)] = _gs


def _ck(item, mc_group, gauge=None):
    """สร้าง carryover key: (item, mc_group, gauge) — match ITEM+MC_GROUP+GUAGE"""
    g = _normalize_gauge(gauge)
    if not g:
        g = _normalize_gauge(
            _item_mc_to_gauge.get(
                (str(item).strip().upper(), str(mc_group).strip().upper()), ""
            )
        )
    return (item, mc_group, g)

# =========================
# FIBER TYPE LOOKUP

# =========================
_yarn_df = load_yarn_master()
_fiber_lookup = dict(zip(_yarn_df["ITEM_CODE"], _yarn_df["FIBER_TYPE"]))
# สร้าง YARN-USED lookup จาก detail_mc (ITEM_CODE → YARN-USED)
_yarn_used_lookup = {}
if "YARN-USED" in detail_mc.columns and "ITEM_CODE" in detail_mc.columns:
    for _, _row in (
        detail_mc[["ITEM_CODE", "YARN-USED"]]
        .dropna()
        .drop_duplicates("ITEM_CODE")
        .iterrows()
    ):
        _yarn_used_lookup[str(_row["ITEM_CODE"]).strip().upper()] = str(
            _row["YARN-USED"]
        ).strip()


def get_fiber_type_for_item(item_code: str) -> str:
    """หา FIBER_TYPE ของ item โดยดึง YARN-USED จาก detail_mc แล้วแยก '+' เช็คแต่ละ code"""
    yarn_used = _yarn_used_lookup.get(str(item_code).strip().upper(), "")
    if not yarn_used:
        return "None POLY"

    parts = [p.strip() for p in yarn_used.split("+") if p.strip()]
    for part in parts:
        if _fiber_lookup.get(part, "None POLY") == "POLY":
            return "POLY"

    return "None POLY"

# =========================
# BOOKING RAW DATA LOADER

# =========================


def load_all_booking_data() -> pd.DataFrame:
    """โหลดข้อมูล booking ทั้งหมดจาก Booking/ directory (ประวัติการผลิตจริง)"""
    if not BOOKING_DIR.exists():
        return pd.DataFrame()

    all_files = [
        f for f in BOOKING_DIR.iterdir() if f.suffix.lower() in (".xlsx", ".xls")
    ]
    if not all_files:
        return pd.DataFrame()

    dfs = []
    for f in all_files:
        try:
            raw = f.read_bytes()
            is_zip = raw[:2] == b"PK"
            is_biff = raw[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
            df = None
            if is_zip:
                df = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
            elif is_biff:
                df = pd.read_excel(io.BytesIO(raw), engine="xlrd")
            else:
                for enc in ("cp874", "utf-8-sig", "latin-1"):
                    try:
                        text = raw.decode(enc, errors="replace")
                        df = pd.read_csv(
                            io.StringIO(text), sep="\t", on_bad_lines="skip"
                        )
                        if df.shape[1] > 3:
                            break

                    except Exception:
                        continue

            if df is not None:
                df.columns = df.columns.str.strip().str.upper()
                dfs.append(df)
        except Exception as e:
            print(f"⚠️ ไม่สามารถโหลด booking {f.name}: {e}")
    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

orders["Date"] = pd.to_datetime(orders["Date"], errors="coerce")
orders["YARN_DYE_FINISH_DATE"] = pd.to_datetime(
    orders.get("YARN_DYE_FINISH_DATE", orders.get("วันที่ย้อมเส้นด้ายจบ")), errors="coerce"
)
orders["Item Code"] = orders["Item Code"].astype(str).str.upper().str.strip()
orders["Orders Type"] = orders["Orders Type"].astype(str).str.upper().str.strip()
orders["MC GROUP"] = orders["MC GROUP"].astype(str).str.upper().str.strip()
orders["Orders.Qty"] = pd.to_numeric(orders["Orders.Qty"], errors="coerce").fillna(0)
orders["Plan Qty"] = pd.to_numeric(orders["Plan Qty"], errors="coerce").fillna(0)
summary_mc["WEEK"] = summary_mc["WEEK"].astype(int)
calendar_week["WEEK"] = calendar_week["WEEK"].astype(int)

# =========================
# CHECK: items without CAP data → skip planning

# =========================
_existing_cap_items = set(item_cap_data["ITEM_CODE"].str.upper())
# ตรวจสอบ items ที่ไม่มี cap data → แจ้งเตือนและข้ามการวางแผน (ไม่สร้าง fallback)
_no_cap_items = set()
_no_cap_order_rows = []  # เก็บ order rows ทั้งหมดที่ไม่มี cap สำหรับสร้าง sheet แยก
for _, _ord_row in orders.iterrows():
    _ord_item = str(_ord_row.get("Item Code", "")).strip().upper()
    if not _ord_item or _ord_item in _existing_cap_items:
        continue

    _ord_mc = str(_ord_row.get("MC GROUP", "")).strip().upper()
    _ord_sc = str(_ord_row.get("SC/SO NO", "")).strip()
    if _ord_item not in _no_cap_items:
        print(
            f"[SKIP] {_ord_item} (SC {_ord_sc}): ไม่มี CAP data — ไม่วางแผนผลิต"
        )
    _no_cap_items.add(_ord_item)
    _no_cap_order_rows.append(_ord_row)
_no_cap_df = pd.DataFrame(_no_cap_order_rows) if _no_cap_order_rows else pd.DataFrame()

# =========================
# FACTORY TYPE CONFIGURATION

# =========================
# สร้าง Factory Type mapping จาก Master_MC_5.xlsx
FACTORY_TYPE_MAP = {}
FACTORY_WORKING_DAYS_MAP = {}
MC_TYPE_MAP = {}  # mc_group → Type (DOUBLE / SINGLE / etc.)
for _, row in master_mc.iterrows():
    mc_name = str(row["MC"]).strip().upper()  # ใช้คอลัมน์ MC
    factory_type = str(row["Factory"]).strip().upper()
    mc_type = str(row.get("Type", "")).strip().upper()  # คอลัมน์ Type
    # ข้าม OUTSOURCE เพราะเป็นการจ้างงานภายนอก
    if factory_type == "OUTSOURCE":
        continue

    # ใช้ MC name โดยตรง
    main_mc_group = mc_name
    FACTORY_TYPE_MAP[main_mc_group] = factory_type
    MC_TYPE_MAP[main_mc_group] = mc_type
    # กำหนดวันทำงานตาม FAC และ Type
    if factory_type == "PHET":
        if mc_type == "DOUBLE":
            FACTORY_WORKING_DAYS_MAP[main_mc_group] = 7
        else:  # SINGLE หรืออื่นๆ
            FACTORY_WORKING_DAYS_MAP[main_mc_group] = 6
    elif factory_type == "OM":
        FACTORY_WORKING_DAYS_MAP[main_mc_group] = 6
    else:
        FACTORY_WORKING_DAYS_MAP[main_mc_group] = 6  # default

# =========================
# TODAY (ห้ามวางย้อนหลัง)

# =========================


def get_week_from_date(date):
    if pd.isna(date):
        return None

    row = calendar_week[
        (calendar_week["WEEK_START"] <= date) & (calendar_week["WEEK_END"] >= date)
    ]
    return None if row.empty else int(row.iloc[0]["WEEK"])


def week_index(week):
    idx = calendar_week.index[calendar_week["WEEK"] == week]
    return None if idx.empty else idx[0]


def get_revolution_weight(item_code, mc_group, plan_week):
    """ค้นหา REVOLUTION/WEIGHT ของ item จาก item_cap_data ที่โหลดไว้"""
    # หาข้อมูลของ item นี้จาก item_cap_data
    item_rows = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    if not item_rows.empty:
        mc_rows = item_rows[item_rows["MC_GROUP"] == mc_group]
        if not mc_rows.empty:
            rev_weight = mc_rows.iloc[0].get("REVOLUTION/WEIGHT", 0)
            return rev_weight

        else:
            # ถ้าไม่เจอ MC_GROUP ตรงๆ ให้ใช้ค่าแรกของ item
            rev_weight = item_rows.iloc[0].get("REVOLUTION/WEIGHT", 0)
            return rev_weight

    return None


def get_working_days_by_factory(mc_group, available_machines_count, week=None):
    """คืนค่าจำนวนวันทำงานของโรงงานตาม MC_GROUP
    เงื่อนไขพิเศษ Week 17:
      - Factory PHET + Type DOUBLE → 10 วัน
      - อื่นๆ → 8 วัน
    """
    # เงื่อนไขพิเศษ Week 17
    if week == 17:
        if FACTORY_TYPE_MAP.get(mc_group, "") == "PHET" and MC_TYPE_MAP.get(mc_group, "") == "DOUBLE":
            return 10

        return 8

    # หาวันทำงานจาก FACTORY_WORKING_DAYS_MAP
    working_days = FACTORY_WORKING_DAYS_MAP.get(mc_group, 6)  # default = 6 วัน
    return working_days


def _dynamic_setup_limit(
    plan_week: int, rdd_idx, required_mc: int, remaining_job_slots: int
) -> int:
    """คืนจำนวน new machines สูงสุดที่ควร setup ใน week นี้ ตาม urgency ของ RDD
    - ห่าง RDD >= 2 week : ใช้แค่ required_mc  (ประหยัด job slot ไว้ให้ order อื่น)
    - ห่าง RDD == 1 week : ใช้เต็ม remaining_job_slots (เร่งให้ทัน)
    - plan_week >= RDD   : ไม่มี cap เลย  (urgent, ใช้ทุก slot ที่เหลือ)
    ทุก case ยังต้องผ่าน check_job_capacity_limit อีกรอบเสมอ
    rdd_idx = row index ใน calendar_week (ใช้แทน fg_week_int เพื่อรองรับข้ามปี)"""
    fallback = remaining_job_slots  # ถ้าไม่มีข้อมูลให้ใช้เต็มที่
    if not required_mc:
        required_mc = fallback
    if rdd_idx is None:
        # ไม่มี RDD → conservative = required_mc เท่านั้น
        return required_mc

    plan_idx = week_index(plan_week)
    if plan_idx is None:
        return required_mc

    weeks_to_rdd = rdd_idx - plan_idx
    if weeks_to_rdd <= 0:
        # urgent / เลยกำหนดแล้ว → ไม่มี cap
        return fallback

    elif weeks_to_rdd == 1:
        # สัปดาห์สุดท้ายก่อน RDD → เปิดเต็มที่
        return fallback

    else:
        # ยังเหลือเวลา → ใช้แค่เท่าที่จำเป็นเพื่อทัน RDD
        return required_mc


def check_job_capacity_limit(
    mc_group,
    available_machines_count,
    urgent_mode=False,
    current_week_jobs=None,
    committed_carryover=0,
):
    """ตรวจสอบว่าจำนวนเครื่องไม่เกิน job/week capacity
    committed_carryover: จำนวนเครื่อง carry-over ที่ผูกพันแล้ว (ต้องผ่านเสมอ, ห้าม cap)
    """
    # หาข้อมูล MC_GROUP จาก Master_MC_5
    mc_info = master_mc[master_mc["MC"] == mc_group]
    if mc_info.empty:
        # ถ้าไม่เจอใน Master_MC_5 ใช้ค่า default
        factory = "PHET"
        mc_type = "DOUBLE"
    else:
        # ดูว่า MC_GROUP นี้อยู่ Factory ไหน และเป็น Type อะไร
        factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
        mc_type = str(mc_info.iloc[0].get("Type", "DOUBLE")).strip().upper()
    # กำหนด job/week capacity ตาม FAC และ Type
    if factory == "PHET":
        if mc_type == "DOUBLE":
            max_jobs = 33
        elif mc_type == "SINGLE":
            max_jobs = 44
        else:
            max_jobs = 33  # default PHET
    elif factory in ("OM", "OMNOI"):
        max_jobs = 13
    else:
        # OUTSOURCE หรือ factory อื่นๆ ไม่มี job cap → ผ่านเสมอ
        return available_machines_count

    # ห้ามเกิน cap เด็ดขาด (urgent mode ก็ใช้ cap เดิม)
    max_jobs_effective = max_jobs
    # Normal/urgent: ห้ามเกิน cap เด็ดขาด
    if current_week_jobs is not None:
        remaining_jobs = max(0, max_jobs_effective - current_week_jobs)
        if committed_carryover > 0:
            # Carryover ไม่กิน job slot เลย — cap เฉพาะ new machines เท่านั้น
            # remaining_jobs = slots ที่ยังว่างสำหรับ new setups (carryover ไม่นับ)
            new_mc = max(0, available_machines_count - committed_carryover)
            allowed_new = min(new_mc, remaining_jobs)  # ต้องไม่เกิน slot ที่เหลือ
            result = committed_carryover + allowed_new
            return result

        return min(available_machines_count, remaining_jobs)

    # ถ้าไม่มีข้อมูล current_week_jobs ให้จำกัดตาม max_jobs_effective
    return min(available_machines_count, max_jobs_effective)


def get_working_days_in_week(week):
    """Get working days for a specific week from calendar (กรองวันหยุดออก)"""
    week_data = calendar_week[calendar_week["WEEK"] == week]
    if week_data.empty:
        return []

    week_start = week_data.iloc[0]["WEEK_START"]
    week_end = week_data.iloc[0]["WEEK_END"]
    # กรองเฉพาะวันที่ is_working_day == 1 จาก daily calendar (ไม่รวมวันหยุด)
    mask = (
        (calendar["DATE"] >= week_start)
        & (calendar["DATE"] <= week_end)
        & (calendar["is_working_day"] == 1)
    )
    working_days = calendar.loc[mask, "DATE"].tolist()
    return working_days


def get_actual_mc_remain(mc_group, week, gauge):
    """คืนค่าจำนวนเครื่องว่างจริง = TOTAL_MC_REMAIN จาก summary_mc หัก weekly_job_usage ที่จองไปแล้ว
    ต้อง match ทั้ง MC_GROUP และ GUAGE เสมอ — ห้าม pool ข้าม GUAGE
    """
    # gauge ต้องระบุและต้องเป็น value จริง (ไม่ใช่ None / NaN)
    gauge_str = _normalize_gauge(gauge)
    if not gauge_str:
        return 0

    mc_rows = summary_mc[
        (summary_mc["WEEK"] == week)
        & (summary_mc["MC_GROUP"] == mc_group)
        & (summary_mc["GUAGE"].apply(_normalize_gauge) == gauge_str)
    ]
    if mc_rows.empty:
        return 0

    # TOTAL_MC_REMAIN = TOTAL_MC - MC_USE_CEIL (หักการจองเก่าออกแล้ว)
    base_remain = mc_rows[mc_rows["TOTAL_MC_REMAIN"] > 0]["TOTAL_MC_REMAIN"].sum()
    # key = (mc_group, gauge_str) เพื่อแยก GUAGE ไม่ให้หักข้าม gauge
    _gk = (mc_group, gauge_str)
    # ถ้า mc_group อยู่ใน shared pool ให้หัก usage ของทุก member ในกลุ่มด้วย
    _pool_members = _POOL_MEMBER_LOOKUP.get(_gk)
    if _pool_members:
        already_used = sum(
            weekly_new_plan_usage.get(week, {}).get(m, 0) for m in _pool_members
        )
    else:
        already_used = weekly_new_plan_usage.get(week, {}).get(_gk, 0)
    return max(0, base_remain - already_used)


def calculate_progressive_reduction(
    item_code, order_qty, start_week, fg_week, mc_group, daily_cap, item_gauge, 
    setup_days=SETUP_DAYS, rev_weight=None
):
    """คำนวณจำนวนเครื่องแต่ละ week แบบประหยัดที่สุด แต่ให้เสร็จพอดี target week
    Strategy:
    1. หาจำนวนเครื่องน้อยที่สุดที่ทัน target (fixed machines)
    2. ถ้าเสร็จเร็วกว่า target → กระจายเครื่องให้น้อยลงในแต่ละ week
    3. เริ่มต้นด้วยเครื่องมากกว่า แล้วค่อยๆ ลดลงให้เสร็จพอดี target week
    Returns: list of (week, machines) หรือ None ถ้าไม่สามารถทันได้
    """
    weeks_until_rdd = []
    current_week = start_week
    while current_week is not None and (
        fg_week is None or week_index(current_week) <= fg_week
    ):
        weeks_until_rdd.append(current_week)
        current_week = next_week(current_week)
    if not weeks_until_rdd:
        return None

    # เก็บ availability และ working days ของแต่ละ week
    week_info = []
    for week in weeks_until_rdd:
        actual_remain = get_actual_mc_remain(mc_group, week, gauge=item_gauge)
        cal_wd = len(get_working_days_in_week(week))
        fac_wd = get_working_days_by_factory(mc_group, 1, week=week)
        actual_wd = min(cal_wd, fac_wd)
        week_info.append({
            'week': week,
            'avail': actual_remain,
            'wd': actual_wd
        })
    # Step 1: หาจำนวนเครื่องน้อยที่สุดที่ทัน (fixed machines ทุก week)
    min_machines = None
    for try_mc in range(1, max(w['avail'] for w in week_info) + 1):
        qty_left = order_qty
        for i, w in enumerate(week_info):
            if qty_left <= 0:
                break

            prod_days = max(0, w['wd'] - setup_days) if i == 0 else w['wd']
            if prod_days <= 0:
                continue

            use_mc = min(try_mc, w['avail'])
            prod = use_mc * prod_days * daily_cap
            if rev_weight and rev_weight > 0:
                prod = (prod // rev_weight) * rev_weight
            qty_left -= prod
        if qty_left <= 0:
            min_machines = try_mc
            break

    if min_machines is None:
        return None  # ไม่ทันแม้ใช้เครื่องเต็มที่

    # Step 2: กระจายเครื่องให้เสร็จพอดี target week (ไม่เร็วเกินไป)
    # ใช้ min_machines แต่กระจายให้ครบทุก week จนถึง target
    result = []
    qty_left = order_qty
    num_weeks = len(week_info)
    for i, w in enumerate(week_info):
        if qty_left <= 0:
            # เสร็จแล้ว แต่ยังมี week เหลือ → ไม่ผลิต (เพื่อให้เสร็จพอดี target)
            result.append((w['week'], 0))
            continue

        prod_days = max(0, w['wd'] - setup_days) if i == 0 else w['wd']
        if prod_days <= 0 or w['avail'] <= 0:
            result.append((w['week'], 0))
            continue

        # คำนวณเครื่องที่ต้องการให้ผลิตกระจายไปจนถึง target week
        weeks_remaining = num_weeks - i
        if weeks_remaining == 1:
            # Week สุดท้าย: ผลิตให้หมดพอดี
            needed_mc = max(1, int(qty_left / (prod_days * daily_cap)) + 1)
            use_mc = min(needed_mc, w['avail'], min_machines)
        else:
            # Week ก่อนหน้า: ผลิตให้พอดีกับสัดส่วน (กระจายเท่าๆ กัน)
            avg_qty_per_week = qty_left / weeks_remaining
            needed_mc = max(1, int(avg_qty_per_week / (prod_days * daily_cap)))
            # ไม่เกิน min_machines และไม่เกิน availability
            use_mc = min(needed_mc, w['avail'], min_machines)
        # คำนวณ production จริง
        prod = use_mc * prod_days * daily_cap
        if rev_weight and rev_weight > 0:
            prod = (prod // rev_weight) * rev_weight
        result.append((w['week'], use_mc))
        qty_left -= prod
    # ถ้ายังเหลือ qty หลังจาก loop ครบ → ไม่ทัน (แต่ไม่น่าเกิดเพราะ min_machines ทันแล้ว)
    if qty_left > 0:
        return None

    return result


def calculate_required_machines(
    item_code, order_qty, start_week, fg_week, setup_days=SETUP_DAYS, only_mc_group=None
):
    """คำนวณจำนวนเครื่องขั้นต่ำที่ต้องการเพื่อทัน RDD
    หลักการ: ใช้เครื่องน้อยแต่ผลิตหลาย week ดีกว่าใช้เครื่องเยอะแค่ 1 week
    - setup เป็น per-machine: 3mc setup = เสีย 3×3=9 mc-days
    - week 2+ ไม่ต้อง setup → ได้ผลิตเต็มที่
    - simulate per-week ด้วยเครื่องว่างจริงของแต่ละ week (cap at n_mc)
    ตัวอย่าง order 3277.5, cap=163, factory 7d, เครื่องว่าง [6, 1, 5]:
      6mc×3wk: wk1=6×4×163=3912, wk2=1×7×163=1141, wk3=5×7×163=5705 → setup_waste=18
      2mc×3wk: wk1=2×4×163=1304, wk2=1×7×163=1141, wk3=2×7×163=2282 → setup_waste=6 ✅
    """
    # หา MC_GROUP ที่สามารถผลิต item นี้ได้
    available_machines = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    if available_machines.empty:
        return None, None, None, None, None

    # ใช้ CAP ทอ ที่น้อยที่สุดในการคำนวณ (conservative planning)
    min_daily_cap = available_machines["CAP ทอ"].min()
    # เรียงตาม MC_GROUP ที่มีเครื่องเหลือมากที่สุดก่อน (start_week)
    available_machines = available_machines.copy()
    available_machines["_mc_remain"] = available_machines.apply(
        lambda r: get_actual_mc_remain(r["MC_GROUP"], start_week, gauge=r.get("GUAGE")),
        axis=1,
    )
    available_machines = available_machines.sort_values("_mc_remain", ascending=False)
    # ถ้ามี only_mc_group → บังคับใช้ MC_GROUP นั้น (lock สำหรับ SC/SO+Item เดิม)
    if only_mc_group is not None:
        _filt = available_machines[available_machines["MC_GROUP"] == only_mc_group]
        if not _filt.empty:
            available_machines = _filt
    # คำนวณจำนวนสัปดาห์ที่เหลือถึง RDD
    # fg_week คือ rdd_idx (row index ใน calendar_week) เพื่อรองรับข้ามปีได้
    weeks_until_rdd = []
    current_week = start_week
    while current_week is not None and (
        fg_week is None or week_index(current_week) <= fg_week
    ):
        weeks_until_rdd.append(current_week)
        current_week = next_week(current_week)
    if not weeks_until_rdd:
        return None, None, None, None, None

    num_weeks = len(weeks_until_rdd)
    # ลองแต่ละ MC_GROUP ที่สามารถผลิตได้ (เรียงตาม cap น้อยไปมาก — ใช้ cap ต่ำสุดในการคำนวณ)
    for _, machine_row in available_machines.iterrows():
        mc_group = machine_row["MC_GROUP"]
        daily_cap = min_daily_cap  # ใช้ cap น้อยที่สุดในการคำนวณ
        # หา GUAGE ที่ item นี้ใช้
        item_gauge = machine_row["GUAGE"] if "GUAGE" in machine_row else None
        # เก็บจำนวนเครื่องว่างจริงของแต่ละ week
        avail_per_week = []
        has_any_machine = False
        for week in weeks_until_rdd:
            actual_remain = get_actual_mc_remain(mc_group, week, gauge=item_gauge)
            avail_per_week.append(actual_remain)
            if actual_remain > 0:
                has_any_machine = True
        # เครื่องที่วิ่งอยู่แล้ว (carry-over) ถือว่า "มี" เครื่องพร้อมผลิตโดยไม่ต้องดู actual_remain
        _key_check = _ck(item_code, mc_group, item_gauge)
        if not has_any_machine and machines_in_use.get(_key_check, 0) <= 0:
            continue

        # ---- Setup-aware: ตรวจสอบว่าต้อง setup หรือไม่ ----
        key = _ck(item_code, mc_group, item_gauge)
        setup_needed = True
        start_week_idx = week_index(start_week)
        if key in last_production:
            last_week_idx = last_production[key]
            if start_week_idx - last_week_idx <= SETUP_GAP_WEEK:
                setup_needed = False
        # เครื่องที่วิ่งอยู่แล้ว (carry-over จาก booking/old plan)
        # ถ้า setup_needed=False = เครื่องยังอุ่นอยู่ → ใช้เป็น committed_mc ตั้งต้น
        carryover_start = machines_in_use.get(key, 0) if not setup_needed else 0
        factory_wd = get_working_days_by_factory(mc_group, 1, week=start_week)
        # หาจำนวนเครื่องสูงสุดที่สามารถลองได้ (จาก week ที่มีเครื่องมากที่สุด)
        max_possible = max(avail_per_week)
        # จำกัดตาม job/week capacity (รวม type ทั้งหมด ไม่ใช่แค่ MC_GROUP เดียว)
        type_used_start = get_type_used_jobs(start_week, mc_group)
        max_try = check_job_capacity_limit(
            mc_group,
            int(max_possible),
            urgent_mode=False,
            current_week_jobs=type_used_start,
        )
        # carry-over machines ไม่ต้อง setup ไม่ต้องนับเป็น new job
        # ดังนั้น max_try ต้องอย่างน้อย = carryover_start
        if carryover_start > 0 and max_try < carryover_start:
            max_try = carryover_start

        # ---- เปรียบเทียบทุก option ด้วย per-week simulation ----
        best_option = None  # (n_machines, weeks_needed, setup_waste, efficiency)
        for n_mc in range(1, int(max_try) + 1):
            # Simulate: ต้องการ n_mc เครื่อง แต่ละ week อาจได้ไม่ครบตาม availability
            # เครื่องที่เพิ่มใหม่ต้อง setup, เครื่องที่ carry-over ไม่ต้อง setup
            qty_remaining = order_qty
            weeks_needed = 0
            # เริ่มต้น simulation ด้วยเครื่องที่วิ่งอยู่แล้ว (ถ้า setup_needed=False)
            committed_mc = min(carryover_start, n_mc)  # ไม่เกิน target n_mc
            total_setup_mc_days = 0
            actual_use_list = []
            actual_wd_list = []  # เก็บ actual working days ของแต่ละ week
            for w_idx, week in enumerate(weeks_until_rdd):
                if qty_remaining <= 0:
                    break

                # คำนวณวันทำงานจริงของ week นี้ (หักวันหยุดจาก calendar)
                # cal_wd = 0 หมายถึงสัปดาห์นั้นหยุดทั้งสัปดาห์ → ผลิตได้ 0 วัน ห้าม fallback factory_wd
                cal_wd = len(get_working_days_in_week(week))
                actual_wd = min(cal_wd, factory_wd)
                # ถ้า summary_mc ไม่มีข้อมูลในสัปดาห์นี้ แต่เครื่องกำลังวิ่งอยู่ (carry-over)
                # ให้เครื่องเดิมยังคงผลิตต่อได้ (ไม่ต้องมีข้อมูลใน summary_mc)
                avail_this_week = avail_per_week[w_idx]
                if avail_this_week <= 0 and committed_mc > 0:
                    avail_this_week = committed_mc  # carry-over เท่านั้น ไม่เพิ่มเครื่องใหม่
                # จำนวนเครื่องที่ต้องการใน week นี้ (ไม่เกิน availability)
                want_mc = min(n_mc, avail_this_week)
                if want_mc <= 0:
                    actual_use_list.append(0)
                    actual_wd_list.append(actual_wd)
                    continue

                # แยก carry-over vs ใหม่
                carryover = min(committed_mc, want_mc)
                new_added = (
                    want_mc - carryover
                )  # ไม่มี MAX_SETUP_MC → job/week cap ควบคุมแทน
                want_mc = carryover + new_added
                if committed_mc == 0 and setup_needed:
                    # week แรกที่เริ่มผลิต (cold start): ทุกเครื่องต้อง setup
                    setup_mc = want_mc
                    want_mc = setup_mc
                    prod_days_carry = 0
                    prod_days_new = max(0, actual_wd - setup_days)
                elif new_added > 0 and (setup_needed or committed_mc > 0):
                    # มีเครื่องเพิ่มใหม่นอกเหนือจาก carryover → เฉพาะเครื่องใหม่ต้อง setup
                    setup_mc = new_added if (setup_needed or committed_mc > 0) else 0
                    prod_days_carry = actual_wd
                    prod_days_new = (
                        max(0, actual_wd - setup_days) if setup_mc > 0 else actual_wd
                    )
                else:
                    # carry-over ล้วน หรือ warm start (setup_needed=False, committed_mc=0)
                    setup_mc = 0
                    prod_days_carry = actual_wd
                    prod_days_new = actual_wd  # warm → ผลิตเต็มสัปดาห์ที่เปิดจริง
                total_setup_mc_days += setup_mc * setup_days
                committed_mc = want_mc  # อัปเดตเครื่องที่ใช้จริง
                weeks_needed += 1
                actual_use_list.append(want_mc)
                actual_wd_list.append(actual_wd)
                week_production = (
                    carryover * prod_days_carry + new_added * prod_days_new
                ) * daily_cap
                qty_remaining -= week_production
            finished = qty_remaining <= 0
            setup_waste = total_setup_mc_days  # mc-days ที่เสียไปกับ setup
            # คำนวณ efficiency (ใช้ actual working days ของแต่ละ week)
            total_machine_days = sum(
                mc * wd for mc, wd in zip(actual_use_list, actual_wd_list) if mc > 0
            )
            productive_days = max(0, total_machine_days - setup_waste)
            efficiency = (
                (productive_days / total_machine_days * 100)
                if total_machine_days > 0
                else 0
            )
            if finished:
                # พบจำนวนเครื่องน้อยสุดที่ทัน RDD แล้ว → หยุดทันที
                # (ประหยัด slot เครื่องให้ order อื่น)
                best_option = (n_mc, weeks_needed, total_setup_mc_days, efficiency)
                break

        if best_option:
            required_machines = best_option[0]
            return mc_group, daily_cap, required_machines, True, item_gauge  # feasible

        else:
            # ไม่ทันทุก option → ใช้เครื่องเต็มที่ + บอก caller ว่า NOT feasible
            required_machines = int(max_try)
            return (

                mc_group,
                daily_cap,
                required_machines,
                False,
                item_gauge,
            )  # not feasible
    return None, None, None, None, None


def get_best_machine_for_item(
    item_code,
    plan_week,
    last_production,
    required_machines_info=None,
    urgent_mode=False,
    past_rdd=False,
    force_max_mc=False,
):
    """เลือกเครื่องที่เหมาะสมที่สุดสำหรับ item นี้ (ใช้ get_actual_mc_remain หักเครื่องที่จองไปแล้ว)"""
    # ถ้ามีการคำนวณจำนวนเครื่องที่ต้องการมาแล้ว ให้ใช้ค่านั้น
    if required_machines_info is not None:
        mc_group, daily_cap, required_machines, *_ = required_machines_info
        # ดึง gauge จาก required_machines_info (ตำแหน่งที่ 4 ถ้ามี)
        _rmi_gauge = (
            required_machines_info[4] if len(required_machines_info) > 4 else None
        )
        if mc_group and required_machines > 0:
            # หา GUAGE ของ item นี้
            item_machine_info = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item_code)
                & (item_cap_data["MC_GROUP"] == mc_group)
            ]
            item_gauge = (
                item_machine_info.iloc[0]["GUAGE"]
                if not item_machine_info.empty
                else _rmi_gauge
            )
            # ดูเครื่องว่างจริง (หักที่จองไปแล้ว)
            actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
            # ตรวจสอบว่าเคยผลิต item นี้ใน week ก่อน (= เครื่องเดิม carry over)
            key = _ck(item_code, mc_group, item_gauge)
            setup_needed = True
            current_week_idx = week_index(plan_week)
            is_continuing = False  # order เดิมกำลังผลิตต่อจาก week ก่อน
            if key in last_production:
                last_week_idx = last_production[key]
                if current_week_idx - last_week_idx <= SETUP_GAP_WEEK:
                    setup_needed = False
                if current_week_idx - last_week_idx == 1:
                    is_continuing = True  # week ติดกัน = เครื่องเดิม carry over
                # carry-over จาก old plan: SC/SO NO เดิม และยังไม่ได้เริ่มผลิตใน new plan
                same_sc = last_sc_so_no.get(key) == sc_so_no
                if same_sc:  # SC/SO เดิม → carry over เสมอ (รวมกรณี FG Week ต่างกัน)
                    is_continuing = True
                    setup_needed = False
                else:
                    # Optionally allow carryover across different SC/SO if configured
                    if ALLOW_CARRYOVER_ACROSS_SO:
                        prev_m = machines_in_use.get(key, 0)
                        last_idx = last_production.get(key)
                        if (
                            prev_m > 0
                            and last_idx is not None
                            and current_week_idx - last_idx <= SETUP_GAP_WEEK
                        ):
                            is_continuing = True
                            setup_needed = False
            if is_continuing:
                # เครื่องเดิมจาก week ก่อน carry over โดยไม่ต้องเช็ค actual_remain
                # ใช้ fallback=0 ตรงกับ main loop เพื่อป้องกัน committed_carryover ผิด
                prev_mc = machines_in_use.get(_ck(item_code, mc_group, item_gauge), 0)
                carryover = prev_mc  # เครื่องทั้งหมดจาก week ก่อนวิ่งต่อได้เลย
                # ถ้า feasible=True: cap ที่ required_machines (คำนวณมาแล้วว่า N เครื่องพอตั้งแต่ต้น)
                # ถ้า feasible=False: ใช้เต็มที่ (ไม่ทันด้วย N เครื่อง เปิดทุกสล็อที่มี)
                _is_feasible = (
                    required_machines_info[3]
                    if required_machines_info and len(required_machines_info) > 3
                    else True
                )
                extra_avail = max(0, actual_remain)
                if _is_feasible:
                    can_add = max(0, required_machines - carryover)
                    new_additions = min(extra_avail, can_add)
                else:
                    new_additions = extra_avail
                available_machines_count = carryover + new_additions
                type_used = get_type_used_jobs(plan_week, mc_group)
                # ส่ง committed_carryover=carryover เพื่อให้ carryover ผ่าน cap เสมอ
                available_machines_count = check_job_capacity_limit(
                    mc_group,
                    available_machines_count,
                    urgent_mode,
                    type_used,
                    committed_carryover=carryover,
                )
                return (

                    mc_group,
                    daily_cap,
                    setup_needed,
                    available_machines_count,
                    item_gauge,
                )
            if actual_remain > 0:
                _is_feasible = (
                    required_machines_info[3]
                    if required_machines_info and len(required_machines_info) > 3
                    else True
                )
                if _is_feasible:
                    # feasible: cap ที่ required_machines เพื่อไม่ใช้เครื่องเกินความจำเป็น
                    available_machines_count = min(required_machines, actual_remain)
                else:
                    # not feasible: ใช้เต็มที่ + main loop จะใช้ _forward_sim หา minimum
                    available_machines_count = actual_remain
                type_used = get_type_used_jobs(plan_week, mc_group)
                available_machines_count = check_job_capacity_limit(
                    mc_group, available_machines_count, urgent_mode, type_used
                )
                if available_machines_count <= 0:
                    # MC_GROUP จาก required_machines_info เต็ม → ลอง MC_GROUP อื่นที่ item มี CAP
                    # (fallthrough ไปใช้ logic ด้านล่างที่ลองทุก MC_GROUP)
                    pass

                else:
                    return (

                        mc_group,
                        daily_cap,
                        setup_needed,
                        available_machines_count,
                        item_gauge,
                    )
    # เครื่องสำรอง: ใช้ logic เดิมถ้าไม่มีการคำนวณล่วงหน้า
    available_machines = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    if available_machines.empty:
        return None, None, None, None, None

    # ใช้ CAP ทอ ที่น้อยที่สุดในการคำนวณ (conservative planning)
    min_daily_cap = available_machines["CAP ทอ"].min()
    # เรียงตาม MC_GROUP ที่มีเครื่องเหลือมากที่สุดก่อน (plan_week)
    available_machines = available_machines.copy()
    available_machines["_mc_remain"] = available_machines.apply(
        lambda r: get_actual_mc_remain(r["MC_GROUP"], plan_week, gauge=r.get("GUAGE")),
        axis=1,
    )
    available_machines = available_machines.sort_values("_mc_remain", ascending=False)
    current_week_idx = week_index(plan_week)
    # 1. ลองเครื่องที่ว่างในสัปดาห์นี้ก่อน
    for _, machine_row in available_machines.iterrows():
        mc_group = machine_row["MC_GROUP"]
        daily_cap = min_daily_cap  # ใช้ cap น้อยที่สุดในการคำนวณ
        item_gauge = machine_row["GUAGE"] if "GUAGE" in machine_row else None
        # ดูเครื่องว่างจริง (หักที่จองไปแล้ว)
        actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
        if actual_remain > 0:
            type_used = get_type_used_jobs(plan_week, mc_group)
            available_machines_count = check_job_capacity_limit(
                mc_group, actual_remain, urgent_mode, type_used
            )
            if available_machines_count <= 0:
                continue  # ลอง MC_GROUP ถัดไป

            key = _ck(item_code, mc_group, item_gauge)
            setup_needed = True
            if key in last_production:
                last_week_idx = last_production[key]
                if current_week_idx - last_week_idx <= SETUP_GAP_WEEK:
                    setup_needed = False
            return (

                mc_group,
                daily_cap,
                setup_needed,
                available_machines_count,
                item_gauge,
            )
    # 2. ถ้าไม่มีเครื่องว่าง ลอง MC ที่เคยผลิต item เดียวกัน
    previous_mcs = [key[1] for key in last_production if key[0] == item_code]
    for prev_mc in previous_mcs:
        prev_mc_row = available_machines[available_machines["MC_GROUP"] == prev_mc]
        if not prev_mc_row.empty:
            mc_group = prev_mc
            daily_cap = min_daily_cap  # ใช้ cap น้อยที่สุดในการคำนวณ
            item_gauge = (
                prev_mc_row.iloc[0]["GUAGE"] if "GUAGE" in prev_mc_row.iloc[0] else None
            )
            actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
            if actual_remain > 0:
                type_used = get_type_used_jobs(plan_week, mc_group)
                available_machines_count = check_job_capacity_limit(
                    mc_group, actual_remain, urgent_mode, type_used
                )
                if available_machines_count <= 0:
                    continue

                setup_needed = False
                return (

                    mc_group,
                    daily_cap,
                    setup_needed,
                    available_machines_count,
                    item_gauge,
                )
    return None, None, None, None, None


def next_week(week):
    idx = week_index(week)
    if idx is None:
        return None

    idx += 1
    # ข้าม SKIP_WEEKS
    while idx < len(calendar_week):
        w = int(calendar_week.iloc[idx]["WEEK"])
        if w not in SKIP_WEEKS:
            return w

        idx += 1
    return None

TODAY_WEEK = get_week_from_date(TODAY)
TODAY_IDX = week_index(TODAY_WEEK)


def _make_type_key(factory: str, mc_type: str) -> str:
    """สร้าง type_key: OM/OMNOI ไม่มี Type ใช้ชื่อ factory อย่างเดียว"""
    if factory in ("OM", "OMNOI"):
        return "OM"

    return f"{factory}_{mc_type}" if mc_type else factory


def _get_type_key_for_mc(mc_group: str) -> str:
    """คืน type_key ของ MC_GROUP จาก master_mc"""
    _info = master_mc[master_mc["MC"] == mc_group]
    if _info.empty:
        return "PHET_DOUBLE"

    _fac = str(_info.iloc[0]["Factory"]).strip().upper()
    _raw = _info.iloc[0].get("Type", "")
    _typ = "" if pd.isna(_raw) else str(_raw).strip().upper()
    return _make_type_key(_fac, _typ)


def get_type_used_jobs(plan_week: int, mc_group: str) -> int:
    """คืนจำนวน jobs ที่ใช้ไปแล้วใน week นั้น รวมทุก MC_GROUP ใน factory type เดียวกัน cap PHET_DOUBLE=33, PHET_SINGLE=44, OM=13 นับรวม factory-wide ทุก MC_GROUP ใน type นั้น"""
    _target_type = _get_type_key_for_mc(mc_group)
    _week_usage = weekly_job_usage.get(plan_week, {})
    _total = 0
    for _mc, _jobs in _week_usage.items():
        if _get_type_key_for_mc(_mc) == _target_type:
            _total += _jobs
    return _total


def get_remaining_job_slots(plan_week: int, mc_group: str) -> int:
    """คืน job slots ที่เหลืออยู่สำหรับ factory type ของ mc_group ใน week นั้น"""
    mc_info = master_mc[master_mc["MC"] == mc_group]
    if mc_info.empty:
        factory, mc_type = "PHET", "DOUBLE"
    else:
        factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
        mc_type = str(mc_info.iloc[0].get("Type", "DOUBLE")).strip().upper()
    if factory == "PHET":
        max_jobs = 33 if mc_type == "DOUBLE" else 44
    elif factory in ("OM", "OMNOI"):
        max_jobs = 13
    else:
        return 9999  # OUTSOURCE → unlimited

    used = get_type_used_jobs(plan_week, mc_group)
    return max(0, max_jobs - used)


def detect_and_fill_unused_capacity(plans_list, orders_df):
    """
    Detect unused machine capacity and fill with same-item orders from different SCs.
    Optimizes machine utilization by identifying week-item-mc-gauge combinations 
    with unused capacity and filling them with pending orders of the same item.
    """
    if not plans_list:
        return plans_list

    print("🔍 Detecting and filling unused capacity...")
    # Convert plans to DataFrame for easier analysis
    plan_df = pd.DataFrame(plans_list)
    # Group by week, item, mc_group, gauge to find current usage
    current_usage = plan_df.groupby(['PLAN_WEEK', 'ITEM_CODE', 'MC_GROUP', 'MC_GUAGE']).agg({
        'PRODUCE_QTY': 'sum',
        'REQUIRED_MC': 'max',
        'DAILY_CAPACITY': 'first'
    }).reset_index()
    # Calculate theoretical full capacity per week-item-mc-gauge
    current_usage['FULL_CAPACITY'] = (
        current_usage['REQUIRED_MC'] * 
        current_usage['DAILY_CAPACITY'] * 
        7  # Assuming 7 working days for full capacity calculation
    )
    # Find combinations with unused capacity (less than 95% utilization)
    unused_capacity = current_usage[
        current_usage['PRODUCE_QTY'] < (current_usage['FULL_CAPACITY'] * 0.95)
    ].copy()
    if unused_capacity.empty:
        print("✅ No unused capacity detected")
        return plans_list

    print(f"📊 Found {len(unused_capacity)} week-item-mc combinations with unused capacity")
    # Get pending orders that could fill the capacity
    pending_orders = orders_df[
        (orders_df['Pending Plan'] > 0) &
        (~orders_df['SC/SO NO'].isin(plan_df['SC_SO_NO'].unique()))
    ].copy()
    additional_plans = []
    for _, usage_row in unused_capacity.iterrows():
        week = usage_row['PLAN_WEEK']
        item = usage_row['ITEM_CODE']
        mc_group = usage_row['MC_GROUP']
        gauge = usage_row['MC_GUAGE']
        unused_qty = usage_row['FULL_CAPACITY'] - usage_row['PRODUCE_QTY']
        if unused_qty <= 0:
            continue

        # Find pending orders of same item (different SC)
        matching_orders = pending_orders[
            (pending_orders['Item Code'] == item) &
            (pending_orders['MC GROUP'] == mc_group)
        ]
        if matching_orders.empty:
            continue

        # Sort by RDD (urgent first)
        matching_orders = matching_orders.copy()
        matching_orders['FG_WEEK_NUM'] = matching_orders['FG Week'].astype(str).str[-2:].astype(int)
        matching_orders = matching_orders.sort_values('FG_WEEK_NUM')
        for _, order in matching_orders.iterrows():
            if unused_qty <= 0:
                break

            sc_so_no = order['SC/SO NO']
            pending_qty = order['Pending Plan']
            # Calculate how much we can produce in remaining capacity
            produce_qty = min(unused_qty, pending_qty)
            if produce_qty > 0:
                # Create additional plan entry
                additional_plan = {
                    'ITEM_CODE': item,
                    'SC_SO_NO': sc_so_no,
                    'MC_GROUP': mc_group,
                    'MC_GUAGE': gauge,
                    'FACTORY_TYPE': FACTORY_TYPE_MAP.get(mc_group, "UNKNOWN"),
                    'PLAN_WEEK': week,
                    'PRODUCE_QTY': produce_qty,
                    'SETUP_DAYS': 0,  # No setup needed - same item/mc/gauge
                    'REQUIRED_MC': usage_row['REQUIRED_MC'],
                    'ACTUAL_MC': usage_row['REQUIRED_MC'],
                    'CARRYOVER_MC': usage_row['REQUIRED_MC'],
                    'NEW_MC': 0,
                    'FACTORY_WORKING_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week),
                    'CALENDAR_WORKING_DAYS': len(get_working_days_in_week(week)),
                    'ACTUAL_WORKING_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week),
                    'DAILY_CAPACITY': usage_row['DAILY_CAPACITY'],
                    'REVOLUTION_WEIGHT': plan_df[plan_df['ITEM_CODE'] == item]['REVOLUTION_WEIGHT'].iloc[0] if not plan_df[plan_df['ITEM_CODE'] == item].empty else 0,
                    'AVAILABLE_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week),
                    'ORDERS_QTY': order['Orders.Qty'],
                    'PENDING_PLAN': pending_qty - produce_qty,
                    'PLAN_QTY': pending_qty - produce_qty,
                    'ORDER_TYPE': order['Orders Type'],
                    'ORDER_DATE': order['Date'],
                    'FG_WEEK': order['FG Week'],
                    'TARGET_KNIT': order['FG Week'],  # Simplified
                    'FIBER_TYPE': get_fiber_type_for_item(item),
                    'IS_CORE_ITEM': '',
                    'CUSTOMER': str(order.get('Customer', '')).strip(),
                    'PLAN_SOURCE': 'NEW',
                }
                additional_plans.append(additional_plan)
                unused_qty -= produce_qty
                print(f"  📈 Added {produce_qty:.2f} units of {item} (SC: {sc_so_no}) to week {week}")
    if additional_plans:
        print(f"✅ Added {len(additional_plans)} capacity optimization plans")
        plans_list.extend(additional_plans)
    else:
        print("ℹ️  No suitable pending orders found for capacity optimization")
    return plans_list

# =========================
# LOAD OLD PRODUCTION PLAN FOR VALIDATION

# =========================
OLD_PLAN_FILE = DATA_PLAN_DIR / "weekly_production_plan_combined_filtered.xlsx"
try:
    old_plan_df = pd.read_excel(OLD_PLAN_FILE)
    # เอาเฉพาะแถว NEW จาก old plan (carry-over จาก plan ก่อนหน้า)
    if "PLAN_SOURCE" in old_plan_df.columns:
        old_plan_df = old_plan_df[old_plan_df["PLAN_SOURCE"] == "NEW"].copy()
    print(f"📋 โหลดแผนการผลิตเก่าสำหรับ validation: {len(old_plan_df)} แผน")
except FileNotFoundError:
    print("📋 ไม่พบแผนการผลิตเก่า")
    old_plan_df = pd.DataFrame()
# สร้าง dict: {sc_so_no_upper: total_produce_qty} จาก old plan (NEW) ที่วาง week >= TODAY
# ใช้หัก qty_left เพื่อไม่วางซ้ำ (carry-over qty)
old_plan_produced_qty = {}
if (
    not old_plan_df.empty
    and "PRODUCE_QTY" in old_plan_df.columns
    and "PLAN_WEEK" in old_plan_df.columns
):
    _sc_col = "SC_SO_NO" if "SC_SO_NO" in old_plan_df.columns else None
    if _sc_col:
        for _, _opr in old_plan_df.iterrows():
            _opw = _opr.get("PLAN_WEEK")
            if pd.isna(_opw):
                continue

            _opw_idx = week_index(int(_opw))
            if _opw_idx is None or _opw_idx < TODAY_IDX:
                continue  # เฉพาะ week อนาคต

            _op_sc = str(_opr[_sc_col]).strip().upper()
            _op_qty = pd.to_numeric(_opr.get("PRODUCE_QTY", 0), errors="coerce")
            if pd.isna(_op_qty):
                _op_qty = 0
            if _op_sc and _op_sc != "NAN":
                # handle comma-separated SC/SO (e.g. "715033,S716032")
                for _sc_part in _op_sc.split(","):
                    _sc_part = _sc_part.strip().upper().lstrip("S")
                    if _sc_part:
                        old_plan_produced_qty[_sc_part] = old_plan_produced_qty.get(
                            _sc_part, 0
                        ) + float(_op_qty)
    print(
        f"📋 Old plan carry-over qty (week>={TODAY_WEEK}): {len(old_plan_produced_qty)} SOs"
    )

# =========================
# LOAD BOOKING DATA (ประวัติการผลิตจริง)

# =========================
_BOOKING_EXCLUDE_MC = {
    "CL-NP",
    "CL-OM",
    "COMKN",
    "F-CL",
    "CL",
    "FQCCL-NP",
    "FQCCL-OM",
    "FQC-OMNOI",
    "FQC-PHET",
    "FQC",
    "F-TSD",
}
booking_last_production = {}  # {(item, mc_group): week_index} — week สุดท้ายที่ผลิตจริง
booking_last_so = {}  # {(item, mc_group): so_no_normalized} — SO หมายเลขสุดท้าย
booking_produced_qty = {}  # {so_no_upper: total_knit_weight} — ผลิตไปแล้วทั้งหมด
booking_raw = load_all_booking_data()
if not booking_raw.empty:
    for col in ["ITEM_CODE", "MC_GROUP", "SO_NO", "TYPE", "KP_NO"]:
        if col in booking_raw.columns:
            booking_raw[col] = booking_raw[col].astype(str).str.strip().str.upper()
    booking_raw["WEEK"] = pd.to_numeric(booking_raw.get("WEEK"), errors="coerce")
    booking_raw["YEAR"] = pd.to_numeric(booking_raw.get("YEAR"), errors="coerce")
    if "KNIT WEIGHT" not in booking_raw.columns:
        booking_raw["KNIT WEIGHT"] = 0
    else:
        booking_raw["KNIT WEIGHT"] = pd.to_numeric(
            booking_raw["KNIT WEIGHT"], errors="coerce"
        ).fillna(0)
    if "MC_GROUP" in booking_raw.columns:
        booking_raw = booking_raw[~booking_raw["MC_GROUP"].isin(_BOOKING_EXCLUDE_MC)]
    if "TYPE" in booking_raw.columns:
        booking_raw = booking_raw[booking_raw["TYPE"] != "COLLAR"]
    # เฉพาะแถวที่ผลิตจริง (KNIT WEIGHT > 0) และ YEAR 2025-2026
    _produced = booking_raw[
        (booking_raw["KNIT WEIGHT"] > 0) & (booking_raw["YEAR"].isin([2025, 2026]))
    ].copy()
    # สร้าง last_production จาก booking
    for _, _row in _produced.iterrows():
        _bi = str(_row.get("ITEM_CODE", "")).strip().upper()
        _bm = str(_row.get("MC_GROUP", "")).strip().upper()
        _bw = _row.get("WEEK")
        _bs = str(_row.get("SO_NO", "")).strip().upper()
        if not _bi or not _bm or pd.isna(_bw):
            continue

        _bw = int(_bw)
        _wi = week_index(_bw)
        if _wi is None:
            continue

        _key = _ck(_bi, _bm)
        if _key not in booking_last_production or _wi > booking_last_production[_key]:
            booking_last_production[_key] = _wi
            booking_last_so[_key] = _bs
    # สร้าง produced_qty per SO — ใช้ KP_NO เพื่อหลีกเลี่ยงการนับซ้ำรายสัปดาห์
    if "KP_NO" in _produced.columns:
        _kp_latest = (
            _produced.groupby(["KP_NO", "SO_NO"])["KNIT WEIGHT"].max().reset_index()
        )
        for _, _r in _kp_latest.iterrows():
            _so = str(_r["SO_NO"]).strip().upper()
            if _so and _so != "NAN":
                booking_produced_qty[_so] = (
                    booking_produced_qty.get(_so, 0) + _r["KNIT WEIGHT"]
                )
    else:
        if "SO_NO" in _produced.columns:
            for _so, _grp in _produced.groupby("SO_NO"):
                _so_key = str(_so).strip().upper()
                if _so_key and _so_key != "NAN":
                    booking_produced_qty[_so_key] = _grp["KNIT WEIGHT"].sum()
    print(
        f"📚 Booking history: {len(booking_last_production)} (item,mc) records, {len(booking_produced_qty)} unique SOs"
    )
else:
    print("📚 ไม่พบข้อมูล booking history")

# =========================
# TRACK LAST PRODUCTION

# =========================
last_production = {}
machines_in_use = {}  # {(item, mc_group): จำนวนเครื่องที่ใช้จริงใน week ล่าสุด}
last_sc_so_no = (
    {}
)  # {(item, mc_group): SC/SO NO ของ order ที่ผลิตล่าสุด — ป้องกัน carry-over ข้าม color/order}
# Pre-populate last_production จาก detail_mc (DETAIL sheet ของ booking_final_ready25)
# ใช้ข้อมูลจริงจาก booking เพื่อรู้ว่า item นี้ถูก book ถึง week ไหน
for _, row in detail_mc.iterrows():
    item_code = str(row.get("ITEM_CODE", "")).strip().upper()
    mc_group = str(row.get("MC_GROUP", "")).strip().upper()
    plan_week = row.get("WEEK")
    mc_used = row.get("MC_USE_CEIL", 0)
    if not item_code or not mc_group or pd.isna(plan_week) or pd.isna(mc_used):
        continue

    if int(mc_used) == 0:
        continue

    plan_week = int(plan_week)
    # ใช้ index เปรียบเทียบแทน raw week number เพื่อรองรับข้ามปี
    # เช่น booking week 50 ปี 2025 vs TODAY_WEEK=10 ปี 2026: 50 > 10 → ผิด ถ้าใช้ raw number
    w_idx = week_index(plan_week)
    if w_idx is None:
        continue  # ข้ามเฉพาะ week ที่ไม่มีใน calendar (ไม่กรองตาม TODAY อีกต่อไป → ให้ดู last booking week จริงๆ)

    _det_gauge = str(row.get("GUAGE", "")).strip()
    key = _ck(item_code, mc_group, _det_gauge)
    if key not in last_production or w_idx > last_production[key]:
        last_production[key] = w_idx
        try:
            machines_in_use[key] = int(mc_used)
        except (ValueError, TypeError):
            machines_in_use[key] = 0
print(
    f"📋 โหลด last_production จาก detail_mc (booking_final_ready25): {len(last_production)} รายการ"
)
# Merge old plan → ใช้เป็น fallback สำหรับ carryover ถ้า detail_mc/booking ไม่มี
if not old_plan_df.empty:
    for _, _row in old_plan_df.iterrows():
        # หาชื่อคอลัมน์ที่เป็นไปได้
        item_code = (
            _row.get("ITEM")
            or _row.get("Item")
            or _row.get("ITEM_CODE")
            or _row.get("Item Code")
        )
        mc_group = _row.get("MC_GROUP") or _row.get("MC GROUP") or _row.get("MC")
        plan_week = (
            _row.get("PLAN_WEEK") or _row.get("PLAN WEEK") or _row.get("PLAN_WEEK")
        )
        machines = (
            _row.get("AVAILABLE_MACHINES")
            or _row.get("REQUIRED_MC")
            or _row.get("AVAILABLE_MACHINES")
        )
        sc_no = (
            _row.get("SC_SO_NO")
            or _row.get("SC/SO NO")
            or _row.get("SC SO NO")
            or _row.get("SC/SO")
            or _row.get("SC")
        )
        if pd.isna(item_code) or pd.isna(mc_group) or pd.isna(plan_week):
            continue

        try:
            item_code = str(item_code).strip().upper()
            mc_group = str(mc_group).strip().upper()
            plan_week = int(plan_week)
        except Exception:
            continue

        # ใช้ index เปรียบเทียบแทน raw week number เพื่อรองรับข้ามปี
        w_idx = week_index(plan_week)
        if w_idx is None:
            continue

        _old_gauge = _row.get("MC_GUAGE") or _row.get("MC GUAGE") or _row.get("GUAGE")
        _old_gauge_str = (
            str(_old_gauge).strip() if _old_gauge and not pd.isna(_old_gauge) else None
        )
        key = _ck(item_code, mc_group, _old_gauge_str)
        # old plan ใช้เป็น fallback เท่านั้น: ห้าม override baseline จาก detail_mc/booking
        if key not in last_production:
            last_production[key] = w_idx
            # machines_in_use: ถ้ามีค่าให้บันทึก (int)
            try:
                machines_in_use[key] = (
                    int(machines)
                    if not pd.isna(machines)
                    else machines_in_use.get(key, 0)
                )
            except Exception:
                machines_in_use[key] = machines_in_use.get(key, 0)
            # Normalize SC/SO NO เล็กน้อย
            if sc_no and not pd.isna(sc_no):
                s = str(sc_no).strip().upper()
                if s.startswith("S") and s[1:].isdigit():
                    s = s[1:]
                # ตั้งค่าแค่ถ้ายังไม่มี
                if key not in last_sc_so_no:
                    last_sc_so_no[key] = s
    print(
        f"📋 เติม last_production จาก old_plan: {len([k for k in last_production])} รายการ (รวม)"
    )
# Merge booking history → last_production (booking ข้อมูลจริงแทนถ้า recent กว่า)
for _bk_key, _bk_widx in booking_last_production.items():
    if _bk_key not in last_production or _bk_widx > last_production[_bk_key]:
        last_production[_bk_key] = _bk_widx
        _raw_so = booking_last_so.get(_bk_key, "")
        # Normalize booking SO_NO: "S717492" → "717492" เพื่อให้ตรงกับ order SC/SO NO
        if _raw_so.startswith("S") and _raw_so[1:].isdigit():
            last_sc_so_no[_bk_key] = _raw_so[1:]
        else:
            last_sc_so_no[_bk_key] = _raw_so
    # เสมอ: ถ้ายังไม่มี last_sc_so_no ให้เซ็ตจาก booking (detail_mc ไม่มี SO info)
    if _bk_key not in last_sc_so_no or not last_sc_so_no.get(_bk_key):
        _raw_so = booking_last_so.get(_bk_key, "")
        if _raw_so.startswith("S") and _raw_so[1:].isdigit():
            last_sc_so_no[_bk_key] = _raw_so[1:]
        elif _raw_so:
            last_sc_so_no[_bk_key] = _raw_so
print(f"📚 last_production หลัง merge booking: {len(last_production)} รายการรวม")

# =========================
# TRACK WEEKLY JOB USAGE

# =========================
weekly_job_usage = {}  # {week: {mc_group: jobs_used}}
# Pre-populate weekly_job_usage จาก booking_final_ready25 (DETAIL sheet) เท่านั้น
# Logic: เปรียบเทียบ week ปัจจุบัน (W) กับ week ก่อนหน้าในข้อมูล:
#   - item ไม่มีใน week ก่อนหน้า (หรือเครื่อง=0) → new setup → นับเครื่องทั้งหมดเป็น job
#   - item มีใน week ก่อนหน้า แต่เครื่องเพิ่มขึ้น      → นับเฉพาะส่วนที่เพิ่มเป็น job
#   - item มีใน week ก่อนหน้า เครื่องเท่าเดิมหรือน้อย  → 0 (carryover ไม่นับ job)
if (
    not detail_mc.empty
    and "WEEK" in detail_mc.columns
    and "ITEM_CODE" in detail_mc.columns
    and "MC_USE_CEIL" in detail_mc.columns
    and "MC_GROUP" in detail_mc.columns
):
    _det = detail_mc.copy()
    _det["WEEK"] = pd.to_numeric(_det["WEEK"], errors="coerce")
    _det["MC_USE_CEIL"] = (
        pd.to_numeric(_det["MC_USE_CEIL"], errors="coerce").fillna(0).astype(int)
    )
    _det = _det.dropna(subset=["WEEK", "ITEM_CODE", "MC_GROUP"])
    _det["WEEK"] = _det["WEEK"].astype(int)
    _det["ITEM_CODE"] = _det["ITEM_CODE"].astype(str).str.strip().str.upper()
    _det["MC_GROUP"] = _det["MC_GROUP"].astype(str).str.strip().str.upper()
    # ดึงเฉพาะแถวที่มีเครื่อง > 0
    _det_active = _det[_det["MC_USE_CEIL"] > 0].copy()
    for _mc_grp, _grp_df in _det_active.groupby("MC_GROUP"):
        # สร้าง lookup: week → {item_code: mc_count}  (รวมถ้ามีหลายแถวต่อ item ใน week เดียวกัน)
        _all_weeks_det = sorted(_grp_df["WEEK"].unique())
        _week_item_mc: dict = {}
        for _wk_d in _all_weeks_det:
            _wk_rows = _grp_df[_grp_df["WEEK"] == _wk_d]
            _week_item_mc[_wk_d] = (
                _wk_rows.groupby("ITEM_CODE")["MC_USE_CEIL"].sum().to_dict()
            )
        for _i, _wk in enumerate(_all_weeks_det):
            _wk_idx = week_index(_wk)
            if _wk_idx is None or _wk_idx < TODAY_IDX:
                continue  # week ก่อน TODAY ใช้แค่เป็น baseline ไม่นับ usage

            _curr_items: dict = _week_item_mc[_wk]
            # week ก่อนหน้าในข้อมูล (อาจไม่ใช่ _wk-1 แต่เป็น entry ก่อนหน้าที่มีข้อมูล)
            _prev_items: dict = (
                _week_item_mc.get(_all_weeks_det[_i - 1], {}) if _i > 0 else {}
            )
            _new_jobs = 0
            for _item, _mc in _curr_items.items():
                _prev_mc = _prev_items.get(_item, 0)
                if _prev_mc == 0:
                    # ไม่มีใน week ก่อนหน้า → new setup → นับเครื่องทั้งหมด
                    _new_jobs += _mc
                elif _mc > _prev_mc:
                    # เพิ่มเครื่องใน item เดิม → นับเฉพาะส่วนที่เพิ่ม
                    _new_jobs += _mc - _prev_mc
                # else: carryover หรือลดลง → 0
            if _new_jobs > 0:
                _mc_key = str(_mc_grp).strip().upper()
                if _wk not in weekly_job_usage:
                    weekly_job_usage[_wk] = {}
                weekly_job_usage[_wk][_mc_key] = (
                    weekly_job_usage[_wk].get(_mc_key, 0) + _new_jobs
                )
total_booked = sum(sum(v.values()) for v in weekly_job_usage.values())
print(
    f"📋 Pre-loaded weekly_job_usage จาก booking_final_ready25 DETAIL"
    f" (new setup + เพิ่มเครื่อง, week>={TODAY_WEEK}): {total_booked} jobs"
)
# Snapshot ค่า OLD ก่อนเริ่ม loop ใหม่ (deep copy)
weekly_job_usage_old = {wk: dict(mc_dict) for wk, mc_dict in weekly_job_usage.items()}
# weekly_new_plan_usage: เฉพาะงานที่วางแผนใหม่ในรอบนี้ (ใช้กับ get_actual_mc_remain)
# แยกจาก weekly_job_usage ที่รวม booking เก่าด้วย (TOTAL_MC_REMAIN หักเก่าไปแล้ว)
weekly_new_plan_usage = {}  # {week: {mc_group: new_plan_machines}}
# cap ที่เหลือในสัปดาห์เมื่อ order จบก่อนใช้สุด — ใช้ผลิต FG ถัดไป (item+machine เดียวกัน)
remaining_week_cap = {}  # {(week, item_code, mc_group): remaining_capacity_units}

# =========================
# MERGE SAME SC + SAME ITEM (+ FG Week เดียวกัน)

# =========================
# ถ้า SC/SO NO เหมือนกัน + Item Code เหมือนกัน + FG Week เดียวกัน → รวมเป็น 1 row ผลิตทีเดียว
# ถ้า FG Week ต่างกัน → คง row แยกไว้ (deadline ต่างกัน → plan แยก)
orders["Pending Plan"] = pd.to_numeric(
    orders["Pending Plan"] if "Pending Plan" in orders.columns else 0, errors="coerce"
).fillna(0)
_grp_keys = ["SC/SO NO", "Item Code", "MC GROUP", "MC_GUAGE"]
if "FG Week" in orders.columns:
    _grp_keys = _grp_keys + ["FG Week"]
_sum_cols = [
    c
    for c in ["Orders.Qty", "Plan Qty", "Pending Plan", "Confirm"]
    if c in orders.columns
]
_min_cols = [c for c in ["YARN_DYE_FINISH_DATE"] if c in orders.columns]
_first_cols = [c for c in orders.columns if c not in _grp_keys + _sum_cols + _min_cols]
_agg_dict = {}
_agg_dict.update({c: "sum" for c in _sum_cols})
_agg_dict.update({c: "min" for c in _min_cols})
_agg_dict.update({c: "first" for c in _first_cols})
_orders_before = len(orders)
orders = orders.groupby(_grp_keys, sort=False).agg(_agg_dict).reset_index()
print(
    f"✅ รวม orders same SC+Item: {_orders_before} → {len(orders)} rows (merged {_orders_before - len(orders)} rows)"
)

# =========================
# MAIN PLANNING

# =========================
plans = []
_skip_no_cap = []  # เก็บ item ที่ไม่มี cap เพื่อแสดงรวมท้ายสุด
new_plan_started_items = set()  # ติดตาม (item, mc_group) ที่เริ่มการผลิตใน new plan แล้ว
locked_mc_group_for: dict = (
    {}
)  # ล็อก MC_GROUP (highest-cap) ต่อ (sc_so_no, item) ให้ FG Week ต่างๆ ใช้ร่วมกัน
# ติดตาม last plan week index ต่อ (sc_so_no, item) เพื่อบังคับให้ FG_WEEK ถัดไป
# เริ่มหลัง FG_WEEK ก่อนหน้าจบ (ไม่ผลิตซ้อนกัน)
_last_fg_plan_idx: dict = {}  # {(sc_so_no, item): last_week_index}
# เรียง orders ตาม TARGET_KNIT (rdd_idx) จริง ไม่ใช่ FG Week
# เพราะ order type ต่างกัน TARGET_KNIT ต่างกัน (LAB-DIP = FG-1, SC-ORDERS = FG-3)


def _order_rdd_idx(row):
    """คำนวณ rdd_idx (TARGET_KNIT index) สำหรับ sort เท่านั้น"""
    try:
        fg_w = row.get("FG Week")
        o_type = str(row.get("Orders Type", "")).strip()
        if pd.isna(fg_w) or fg_w is None:
            return 99999

        fg_w_str = str(int(fg_w)).strip()
        if len(fg_w_str) >= 6:
            _yr = int(fg_w_str[:4])
            _wk = int(fg_w_str[4:])
        elif len(fg_w_str) == 5:
            _yr = int(fg_w_str[:4])
            _wk = int(fg_w_str[4:])
        else:
            _yr = TODAY.year
            _wk = int(fg_w_str)
        _row = calendar_week[
            (calendar_week["YEAR"] == _yr) & (calendar_week["WEEK"] == _wk)
        ]
        if _row.empty:
            return 99999

        _raw_idx = _row.index[0]
        _rdd = max(0, _raw_idx - 3)
        if o_type == "LAB-DIP":
            # LAB-DIP: sort เรียงตาม TODAY_IDX + 2 (เริ่มและเสร็จใน week +2)
            _rdd = min(len(calendar_week) - 1, TODAY_IDX + 2)
        return _rdd

    except Exception:
        return 99999

orders["_sort_rdd_idx"] = orders.apply(_order_rdd_idx, axis=1)
orders_sorted = orders.sort_values("_sort_rdd_idx", na_position="last")
orders_sorted = orders_sorted.drop(columns=["_sort_rdd_idx"])
orders = orders.drop(columns=["_sort_rdd_idx"])
for _, order in orders_sorted.iterrows():
    item = order["Item Code"]
    order_qty = order["Orders.Qty"]  # ปริมาณที่สั่งทั้งหมด
    plan_qty = order["Plan Qty"]  # ปริมาณที่วางแผนไปแล้ว (รอ approve)
    pending_plan = pd.to_numeric(order.get("Pending Plan", 0), errors="coerce")
    pending_plan = 0.0 if pd.isna(pending_plan) else float(pending_plan)
    # ถ้า Pending Plan = 0 แสดงว่า order นี้วางแผนครบแล้ว ไม่ต้องวางแผนซ้ำ
    if pending_plan <= 0:
        continue

    order_type = order["Orders Type"]
    fg_week = order.get("FG Week")
    sc_so_no = str(order.get("SC/SO NO", "")).strip()  # ใช้แยก order ต่างสี
    # ตรวจสอบว่า SO นี้ผลิตไปแล้วบางส่วนใน booking จริงหรือไม่
    _so_try = ["S" + sc_so_no.lstrip("S"), sc_so_no.lstrip("S")]
    already_made = 0.0
    for _s in _so_try:
        _s_up = _s.upper()
        if _s_up in booking_produced_qty:
            already_made = booking_produced_qty[_s_up]
            break

    # qty_left = Pending Plan (ยังไม่ได้วางแผน) หักส่วนที่ผลิตจริงไปแล้วจาก booking
    qty_left = max(0.0, pending_plan - already_made)
    # Special rule: เช็คจาก Itemcore และ Customer (CORE ITEM)
    # ถ้า item อยู่ใน Itemcore และ customer เป็น "ที คัลเจอร์ บจ." หรือ "CENTER DOMESTIC" -> ผลิตต่อท้าย
    # ถ้า item อยู่ใน Itemcore แต่ customer ไม่ตรง -> ผลิตตาม target ปกติ
    CORE_CUSTOMERS = {"ที คัลเจอร์ บจ.", "CENTER DOMESTIC"}
    rts_local_force = None
    is_core_item = False  # flag สำหรับ output column
    # เช็คว่า item อยู่ใน Itemcore หรือไม่
    item_upper = str(item).strip().upper()
    item_in_itemcore = item_upper in itemcore_lookup
    if item_in_itemcore:
        # Item อยู่ใน Itemcore - เช็คว่า customer เป็น core customer หรือไม่
        actual_customer = str(order.get("Customer", "")).strip()
        customer_match = (actual_customer in CORE_CUSTOMERS)
        if customer_match:
            # Item อยู่ใน Itemcore + Customer เป็น core -> ใช้กฎ CORE ITEM (ผลิตต่อท้าย)
            is_core_item = True
            try:
                dm = detail_mc[
                    detail_mc["ITEM_CODE"].astype(str).str.upper().str.strip()
                    == str(item).upper()
                ]
                if not dm.empty:
                    last_w = int(dm["WEEK"].dropna().astype(int).max())
                    row_last = dm[dm["WEEK"] == last_w].iloc[-1]
                    sel_mc = str(row_last.get("MC_GROUP", "")).strip().upper()
                    sel_mc_used = int(row_last.get("MC_USE_CEIL", 0) or 0)
                    start_after = next_week(last_w)
                    # Build maps per MC_GROUP: last booked week and machines used
                    last_old_by_mc = {}
                    machines_by_mc = {}
                    daily_cap_by_mc = {}
                    for mc, grp in dm.groupby("MC_GROUP"):
                        try:
                            w = int(grp["WEEK"].dropna().astype(int).max())
                        except Exception:
                            continue

                        last_old_by_mc[str(mc).strip().upper()] = w
                        # get MC_USE_CEIL from the last week that had > 0 machines
                        # (week order may have 0 at the end e.g. paused week → skip those)
                        grp_active = grp[
                            pd.to_numeric(grp["MC_USE_CEIL"], errors="coerce").fillna(0)
                            > 0
                        ]
                        if not grp_active.empty:
                            w_active = int(
                                grp_active["WEEK"].dropna().astype(int).max()
                            )
                            last_active_row = grp_active[
                                grp_active["WEEK"] == w_active
                            ].iloc[-1]
                        else:
                            last_active_row = grp[grp["WEEK"] == w].iloc[-1]
                        try:
                            machines_by_mc[str(mc).strip().upper()] = int(
                                last_active_row.get("MC_USE_CEIL", 0) or 0
                            )
                        except Exception:
                            machines_by_mc[str(mc).strip().upper()] = 0
                        # try to get daily cap from item_cap_data per mc
                        # ใช้ cap น้อยที่สุดของ item นี้ในการคำนวณ
                        try:
                            _all_cap_for_item = item_cap_data[
                                item_cap_data["ITEM_CODE"] == item
                            ]
                            if not _all_cap_for_item.empty:
                                daily_cap_by_mc[str(mc).strip().upper()] = float(
                                    _all_cap_for_item["CAP ทอ"].min()
                                )
                            else:
                                cap_row2 = item_cap_data[
                                    item_cap_data["MC_GROUP"] == str(mc).strip().upper()
                                ]
                                if not cap_row2.empty:
                                    daily_cap_by_mc[str(mc).strip().upper()] = (
                                        cap_row2.iloc[0].get("CAP ทอ", None)
                                    )
                        except Exception:
                            daily_cap_by_mc[str(mc).strip().upper()] = None
                    rts_local_force = {
                        "last_old_by_mc": last_old_by_mc,
                        "machines_by_mc": machines_by_mc,
                        "daily_cap_by_mc": daily_cap_by_mc,
                    }
            except Exception:
                rts_local_force = None

    # ----------------------
    # RDD Check and Urgent Planning

    # ----------------------
    # rdd_idx = row index ใน calendar_week ของ RDD จริง (ใช้แทน week number
    # เพื่อรองรับการเปรียบเทียบข้ามปีได้ถูกต้อง เช่น order FG ปี 2027)
    rdd_idx = None  # row index ใน calendar_week สำหรับ comparison
    fg_week_int = None  # week number (1-53) สำหรับ output/display เท่านั้น
    if pd.notna(fg_week):
        fg_week_str = str(int(fg_week))
        if len(fg_week_str) == 6:  # รูปแบบ YYYYWW (เช่น 202613)
            fg_year = int(fg_week_str[:4])
            fg_week_num = int(fg_week_str[4:])
        elif len(fg_week_str) == 5:  # รูปแบบ YYYYW (เช่น 20265)
            fg_year = int(fg_week_str[:4])
            fg_week_num = int(fg_week_str[4:])
        elif len(fg_week_str) <= 2:  # รูปแบบ WW (เช่น 13) → ใช้ปีปัจจุบัน
            fg_year = TODAY.year
            fg_week_num = int(fg_week_str)
        else:
            fg_year = TODAY.year
            fg_week_num = int(fg_week)
        # หา row index ใน calendar_week ด้วย YEAR + WEEK (รองรับข้ามปี)
        _fg_row = calendar_week[
            (calendar_week["YEAR"] == fg_year) & (calendar_week["WEEK"] == fg_week_num)
        ]
        if not _fg_row.empty:
            _fg_raw_idx = _fg_row.index[0]
            # หัก 3 สัปดาห์ (RDD = FG Week - 3) ด้วย index arithmetic
            rdd_idx = max(0, _fg_raw_idx - 3)
            fg_week_int = int(calendar_week.iloc[rdd_idx]["WEEK"])  # สำหรับ display
    # LAB-DIP: deadline = TODAY + 2 weeks (ต้องเสร็จภายใน week ที่เริ่มผลิต)
    if order_type == "LAB-DIP":
        rdd_idx = min(len(calendar_week) - 1, TODAY_IDX + 2)
        fg_week_int = int(calendar_week.iloc[rdd_idx]["WEEK"])  # อัพเดท display
    if rdd_idx is not None and rdd_idx < TODAY_IDX:
        # RDD ผ่านไปแล้ว = URGENT!
        # สำหรับ urgent order ต้องใช้ความสามารถสูงสุด
        # อาจจะต้องเพิ่มเครื่อง แต่ต้องไม่เกิน job/day capacity
        urgent_mode = True
    else:
        urgent_mode = False

    # ----------------------
    # determine order week based on order type

    # ----------------------
    if order_type == "LAB-DIP":
        # LAB-DIP: +2 weeks from planning date (TODAY) และต้องเสร็จภายใน week นั้น
        if TODAY_IDX + 2 < len(calendar_week):
            order_week = calendar_week.iloc[TODAY_IDX + 2]["WEEK"]
        else:
            continue

    elif order_type == "SC-ORDERS":
        # SC-ORDERS: +2 weeks จาก TODAY เสมอ (ต้องรอ lead time เตรียมงาน)
        base_week = TODAY_WEEK
        idx = week_index(base_week)
        if idx is not None and idx + 2 < len(calendar_week):
            order_week = calendar_week.iloc[idx + 2]["WEEK"]
        else:
            continue

    elif order_type == "YD-ORDERS":
        yd_week = get_week_from_date(order["YARN_DYE_FINISH_DATE"])
        if yd_week is not None:
            order_week = next_week(yd_week)  # +1 week หลังวันย้อมเสร็จ
        else:
            order_week = None
    else:
        continue

    if order_week is None:
        continue

    # ❗ ห้ามวางย้อนหลัง
    start_idx = max(week_index(order_week), TODAY_IDX)
    plan_week = int(calendar_week.iloc[start_idx]["WEEK"])
    # ข้าม SKIP_WEEKS สำหรับ plan_week เริ่มต้น
    while plan_week in SKIP_WEEKS:
        plan_week = next_week(plan_week)
        if plan_week is None:
            break

    if plan_week is None:
        continue

    # ❗ ถ้า booking ของ item+mc_group นี้ยังวิ่งถึง week ≥ plan_week → เริ่มหลัง booking สุดท้าย
    # แต่ถ้าขยับแล้วไม่ทัน RDD ให้ผลิตซ้อน booking แทน (ไม่บังคับต่อท้าย)
    _bk_mc_grp = str(order.get("MC GROUP", "")).strip().upper()
    if _bk_mc_grp:
        _bk_last_idx = last_production.get(_ck(item, _bk_mc_grp))
        if _bk_last_idx is not None and _bk_last_idx >= start_idx:
            _after_bk_idx = _bk_last_idx + 1
            if _after_bk_idx < len(calendar_week):
                # ขยับต่อท้าย booking เฉพาะเมื่อยังทัน RDD
                if rdd_idx is None or _after_bk_idx <= rdd_idx:
                    start_idx = _after_bk_idx
                    plan_week = int(calendar_week.iloc[start_idx]["WEEK"])
                    # ข้าม SKIP_WEEKS หลัง booking check
                    while plan_week in SKIP_WEEKS:
                        plan_week = next_week(plan_week)
                        if plan_week is None:
                            break

                # ถ้าขยับแล้วไม่ทัน RDD → ใช้ start_idx เดิม (ผลิตซ้อน booking ได้)
    # ❗ ถ้า SC/SO+Item เดิมเคยวาง FG_WEEK ก่อนหน้าแล้ว → ต้องเริ่มหลัง FG_WEEK นั้นจบ
    # เพื่อให้จบ FG_WEEK แต่ละตัวก่อนเริ่มตัวถัดไป (ไม่ผลิตซ้อน FG_WEEK)
    # ยกเว้น: ถ้า item เดียวกัน มี cap เหลือจาก week นั้น → เริ่มใน week เดิมได้
    _prev_fg_idx = _last_fg_plan_idx.get((sc_so_no, item))
    if _prev_fg_idx is not None and _prev_fg_idx >= start_idx:
        _prev_fg_week = int(calendar_week.iloc[_prev_fg_idx]["WEEK"])
        # ตรวจว่ามี remaining cap ใน week นั้นสำหรับ item นี้หรือไม่
        _item_mcs = set(
            str(r).strip().upper()
            for r in item_cap_data.loc[
                item_cap_data["ITEM_CODE"] == str(item).strip().upper(), "MC_GROUP"
            ]
        )
        _has_rem_cap = any(
            remaining_week_cap.get((_prev_fg_week, item, _mc), 0) > 0
            for _mc in _item_mcs
        )
        if _has_rem_cap:
            # ใช้ cap ที่เหลือใน week เดิม (ผลิต FG ถัดไปในสัปดาห์เดียวกัน)
            start_idx = _prev_fg_idx
            plan_week = _prev_fg_week
        else:
            _after_fg_idx = _prev_fg_idx + 1
            if _after_fg_idx < len(calendar_week):
                start_idx = _after_fg_idx
                plan_week = int(calendar_week.iloc[start_idx]["WEEK"])
                while plan_week in SKIP_WEEKS:
                    plan_week = next_week(plan_week)
                    if plan_week is None:
                        break

            if plan_week is None:
                continue

    # ----------------------
    # weekly allocation with best machine selection

    # ----------------------
    # คำนวณจำนวนเครื่องที่ต้องการตั้งแต่แรก (ถ้าทัน RDD)
    required_machines_info = None
    # คำนวณ setup days ล่วงหน้า (ใช้ใน calculate_required_machines ด้วย)
    order_fiber_type = get_fiber_type_for_item(item)
    order_setup_days = 5 if order_fiber_type == "POLY" else SETUP_DAYS
    # ❗ ตรวจสอบว่า item นี้มี cap data หรือไม่ — ถ้าไม่มีให้ข้ามทันที
    _item_cap_rows = item_cap_data[item_cap_data["ITEM_CODE"] == str(item).strip().upper()]
    if _item_cap_rows.empty:
        _skip_no_cap.append(f"{item} (SC/SO:{sc_so_no})")
        print(f"⚠️  ไม่พบ CAP data สำหรับ item '{item}' (SC/SO:{sc_so_no}) → ข้ามการวางแผน")
        continue

    # คำนวณ machine allocation ล่วงหน้า
    progressive_plan = None  # {week: machines} สำหรับแต่ละ week
    if rdd_idx is not None and rdd_idx >= week_index(plan_week):
        _locked_mc = locked_mc_group_for.get((sc_so_no, item))
        mc_group_calc, daily_cap_calc, required_machines, feasible_calc, _gauge_calc = (
            calculate_required_machines(
                item,
                qty_left,
                plan_week,
                rdd_idx,
                setup_days=order_setup_days,
                only_mc_group=_locked_mc,
            )
        )
        if required_machines:
            required_machines_info = (
                mc_group_calc,
                daily_cap_calc,
                required_machines,
                feasible_calc,
                _gauge_calc,
            )
            # ล็อก MC_GROUP highest-cap นี้ไว้ให้ FG Week ถัดไปของ SC/SO+Item เดิมใช้ต่อ
            if mc_group_calc and (sc_so_no, item) not in locked_mc_group_for:
                locked_mc_group_for[(sc_so_no, item)] = mc_group_calc
            # ถ้าเปิดใช้ progressive reduction → คำนวณเครื่องทุก week ล่วงหน้า
            if USE_PROGRESSIVE_REDUCTION and mc_group_calc and feasible_calc:
                prog_result = calculate_progressive_reduction(
                    item,
                    qty_left,
                    plan_week,
                    rdd_idx,
                    mc_group_calc,
                    daily_cap_calc,
                    _gauge_calc,
                    setup_days=order_setup_days,
                    rev_weight=get_revolution_weight(item, mc_group_calc, plan_week)
                )
                if prog_result:
                    progressive_plan = {wk: mc for wk, mc in prog_result}
    _produced_week = None  # init สำหรับ track FG_WEEK sequential
    while qty_left > 0 and plan_week is not None:
        # ถ้า FG ใหม่ (SC/SO ใหม่) เริ่มใน week เดิมและมี cap เหลือ ให้ผลิตใน week เดิมจน cap หมดก่อนข้ามไป week ถัดไป
        _fill_last_week = None  # track week สุดท้ายที่ fill cross-SC
        while qty_left > 0 and ALLOW_CARRYOVER_ACROSS_SO:
            # ค้นหา remaining capacity สำหรับ ITEM เดียวกัน ในทุก week (เรียง week น้อยสุดก่อน)
            _found_rem_mc = None
            _found_rem_cap = 0
            _found_rem_week = None
            for _rk, _rv in sorted(remaining_week_cap.items(), key=lambda x: x[0][0]):
                if _rk[1] == item and _rv > 0:
                    _found_rem_week = _rk[0]
                    _found_rem_mc = _rk[2]
                    _found_rem_cap = _rv
                    break

            if _found_rem_mc is None or _found_rem_cap <= 0:
                break

            # ตั้งค่า mc_group และตัวแปรที่เกี่ยวข้องจาก remaining capacity ที่พบ
            _fill_mc_group = _found_rem_mc
            _fill_week = _found_rem_week
            _rem_cap_key = (_fill_week, item, _fill_mc_group)
            _cap_row = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item)
                & (item_cap_data["MC_GROUP"] == _fill_mc_group)
            ]
            if _cap_row.empty:
                remaining_week_cap.pop(_rem_cap_key, None)
                break

            # ใช้ cap น้อยที่สุดของ item นี้ในการคำนวณ
            _all_cap_fill = item_cap_data[item_cap_data["ITEM_CODE"] == item]
            _fill_daily_cap = float(_all_cap_fill["CAP ทอ"].min()) if not _all_cap_fill.empty else float(_cap_row.iloc[0]["CAP ทอ"])
            _fill_gauge = _cap_row.iloc[0].get("GUAGE")
            _fill_rev_weight = get_revolution_weight(item, _fill_mc_group, _fill_week)
            _fill_ck = _ck(item, _fill_mc_group, _fill_gauge)
            _fill_avail_mc = machines_in_use.get(_fill_ck, 1)
            if _fill_week == 17:
                _fill_actual_wd = get_working_days_by_factory(
                    _fill_mc_group, _fill_avail_mc, week=_fill_week
                )
            else:
                _fill_actual_wd = min(
                    len(get_working_days_in_week(_fill_week)),
                    get_working_days_by_factory(_fill_mc_group, _fill_avail_mc, week=_fill_week),
                )
            _rem_cap = _found_rem_cap
            while qty_left > 0 and _rem_cap > 0:
                if _fill_rev_weight and _fill_rev_weight > 0:
                    _rem_batches = int(_rem_cap // _fill_rev_weight)
                    produce = min(qty_left, _rem_batches * _fill_rev_weight)
                else:
                    produce = min(qty_left, _rem_cap)
                if produce > 0:
                    plans.append({
                        "ITEM_CODE": item,
                        "SC_SO_NO": order["SC/SO NO"],
                        "MC_GROUP": _fill_mc_group,
                        "MC_GUAGE": order["MC_GUAGE"],
                        "FACTORY_TYPE": FACTORY_TYPE_MAP.get(_fill_mc_group, "UNKNOWN"),
                        "PLAN_WEEK": _fill_week,
                        "PRODUCE_QTY": produce,
                        "SETUP_DAYS": 0,
                        "REQUIRED_MC": _fill_avail_mc,
                        "ACTUAL_MC": _fill_avail_mc,
                        "CARRYOVER_MC": _fill_avail_mc,
                        "NEW_MC": 0,
                        "FACTORY_WORKING_DAYS": get_working_days_by_factory(_fill_mc_group, _fill_avail_mc, week=_fill_week),
                        "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(_fill_week)),
                        "ACTUAL_WORKING_DAYS": _fill_actual_wd,
                        "DAILY_CAPACITY": _fill_daily_cap,
                        "REVOLUTION_WEIGHT": _fill_rev_weight if _fill_rev_weight is not None else 0,
                        "AVAILABLE_DAYS": _fill_actual_wd,
                        "ORDERS_QTY": order_qty,
                        "PENDING_PLAN": pending_plan,
                        "PLAN_QTY": qty_left - produce,
                        "ORDER_TYPE": order_type,
                        "ORDER_DATE": order["Date"],
                        "FG_WEEK": fg_week,
                        "TARGET_KNIT": fg_week_int,
                        "FIBER_TYPE": get_fiber_type_for_item(item),
                        "IS_CORE_ITEM": "CORE ITEM" if is_core_item else "",
                        "CUSTOMER": str(order.get("Customer", "")).strip(),
                        "PLAN_SOURCE": "NEW",
                    })
                    qty_left -= produce
                    _rem_cap -= produce
                    # อัปเดต remaining cap หลังใช้งาน
                    _new_rem = max(0, _rem_cap)
                    if _new_rem > 0:
                        remaining_week_cap[_rem_cap_key] = _new_rem
                    else:
                        remaining_week_cap.pop(_rem_cap_key, None)
                    # อัปเดต tracking สำหรับ cross-SC carryover
                    last_production[_fill_ck] = week_index(_fill_week)
                    machines_in_use[_fill_ck] = _fill_avail_mc
                    last_sc_so_no[_fill_ck] = sc_so_no
                    new_plan_started_items.add(_fill_ck)
                    _produced_week = _fill_week
                    _fill_last_week = _fill_week
                else:
                    # cap เหลือน้อยเกินไป (rev_weight rounding) → mark week เต็ม (=0)
                    remaining_week_cap[_rem_cap_key] = 0
                    break

            if qty_left <= 0:
                break

        # หลัง cross-SC fill: เลื่อน plan_week ข้าม week ที่ fill ไปแล้ว เพื่อไม่ให้ plan ซ้ำ
        if qty_left > 0 and _fill_last_week is not None:
            _fl_idx = week_index(_fill_last_week)
            _pw_idx = week_index(plan_week)
            if _fl_idx is not None and _pw_idx is not None and _fl_idx >= _pw_idx:
                plan_week = next_week(_fill_last_week)
                if plan_week is None:
                    break

        # ตรวจสอบว่า plan_week ถูกใช้เต็มแล้ว (remaining=0) โดย FG ก่อนหน้าของ item เดียวกัน
        if qty_left > 0:
            _items_in_week = [
                (_rk, _rv) for _rk, _rv in remaining_week_cap.items()
                if _rk[0] == plan_week and _rk[1] == item
            ]
            if _items_in_week and all(_rv == 0 for _, _rv in _items_in_week):
                # week เต็ม → ข้ามไป week ถัดไป
                plan_week = next_week(plan_week)
                if plan_week is None:
                    break

                continue

        # ⚠️ ตรวจสอบ RDD ก่อนว่าทันหรือไม่
        _plan_idx = week_index(plan_week)
        past_rdd = bool(
            rdd_idx is not None and _plan_idx is not None and _plan_idx >= rdd_idx
        )
        if rdd_idx is not None and _plan_idx is not None and _plan_idx > rdd_idx:
            urgent_mode = True
        # ถ้ายังไม่ได้คำนวณ required_machines (เพราะตอนแรก avail=0 ทุก week)
        # ให้ลองคำนวณใหม่ด้วย plan_week ปัจจุบันที่มีเครื่องว่างจริง
        if (
            required_machines_info is None
            and not past_rdd
            and rdd_idx is not None
            and _plan_idx is not None
            and _plan_idx <= rdd_idx
        ):
            _locked_mc2 = locked_mc_group_for.get((sc_so_no, item))
            _mc_r, _cap_r, _req_r, _feas_r, _gauge_r = calculate_required_machines(
                item,
                qty_left,
                plan_week,
                rdd_idx,
                setup_days=order_setup_days,
                only_mc_group=_locked_mc2,
            )
            if _req_r:
                required_machines_info = (_mc_r, _cap_r, _req_r, _feas_r, _gauge_r)
                if _mc_r and (sc_so_no, item) not in locked_mc_group_for:
                    locked_mc_group_for[(sc_so_no, item)] = _mc_r
        # เลือกเครื่องที่เหมาะสมที่สุดสำหรับ item นี้
        # ถ้าเป็นกรณี RTS+LOCAL ให้บังคับใช้ MC เดิมและเริ่มหลัง old สุดท้าย
        mc_group = daily_capacity = setup_needed = available_machines = _sel_gauge = (
            None
        )
        # RTS: บังคับ MC_GROUP ตาม booking เดิม (detail_mc)
        # เพราะ get_best_machine_for_item อาจเลือก MC_GROUP อื่น (เช่น SKPTA→SKP)
        if rts_local_force:
            _rts_old_mc = rts_local_force.get("last_old_by_mc", {})
            if _rts_old_mc:
                # เลือก MC_GROUP ที่มี last week ล่าสุด
                _rts_mc = max(_rts_old_mc, key=_rts_old_mc.get)
                _rts_machines = rts_local_force.get("machines_by_mc", {}).get(
                    _rts_mc, 0
                )
                _rts_cap = rts_local_force.get("daily_cap_by_mc", {}).get(_rts_mc)
                if _rts_machines > 0:
                    mc_group = _rts_mc
                    available_machines = _rts_machines
                    setup_needed = False
                    _sel_gauge = _item_mc_to_gauge.get(
                        (str(item).strip().upper(), str(_rts_mc).strip().upper()), None
                    )
                    # daily_capacity: ใช้ cap น้อยที่สุดของ item นี้ในการคำนวณ
                    _all_cap_rts = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                    if not _all_cap_rts.empty:
                        daily_capacity = float(_all_cap_rts["CAP ทอ"].min())
                    elif _rts_cap and not pd.isna(_rts_cap) and float(_rts_cap) > 0:
                        daily_capacity = float(_rts_cap)
                    else:
                        _rts_cap_row = item_cap_data[
                            (item_cap_data["ITEM_CODE"] == item)
                            & (item_cap_data["MC_GROUP"] == _rts_mc)
                        ]
                        if not _rts_cap_row.empty:
                            daily_capacity = float(
                                _rts_cap_row.iloc[0].get("CAP ทอ", 0) or 0
                            )
        # คำนวณ _req_feasible ก่อน เพื่อส่งเข้า get_best_machine_for_item
        _req_feasible = (
            required_machines_info[3]
            if required_machines_info and len(required_machines_info) > 3
            else True
        )
        if mc_group is None:
            mc_group, daily_capacity, setup_needed, available_machines, _sel_gauge = (
                get_best_machine_for_item(
                    item,
                    plan_week,
                    last_production,
                    required_machines_info,
                    urgent_mode,
                    past_rdd,
                    force_max_mc=(not _req_feasible and not past_rdd),
                )
            )
        if mc_group is None:
            plan_week = next_week(plan_week)
            continue

        # ถ้ามี progressive_plan → ใช้จำนวนเครื่องที่คำนวณไว้ล่วงหน้า
        if progressive_plan and plan_week in progressive_plan:
            available_machines = progressive_plan[plan_week]
        elif required_machines_info and not past_rdd and _req_feasible:
            # Cap available_machines ตาม required_mc เมื่อ feasible=True
            # (คำนวณมาแล้วว่า N เครื่องพอตั้งแต่ต้น ไม่ต้องเพิ่มบนกลางคัน)
            req_mc = required_machines_info[2]
            if available_machines > req_mc:
                available_machines = req_mc
        # ถ้า plan_week เกิน target แล้ว → จำกัดเครื่องไม่เกิน 10
        if past_rdd and available_machines > 10:
            available_machines = 10
        # Calculate available capacity considering setup days and factory type
        working_days = get_working_days_in_week(plan_week)
        factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week)
        # Week 17: ใช้ factory_working_days โดยตรง (ไม่ cap ด้วย calendar)
        if plan_week == 17:
            actual_working_days = factory_working_days
        else:
            # ใช้จำนวนวันทำงานที่น้อยกว่าระหว่าง calendar และ factory capacity
            actual_working_days = min(len(working_days), factory_working_days)
        # หา REVOLUTION/WEIGHT ที่มากที่สุด
        rev_weight = get_revolution_weight(item, mc_group, plan_week)
        # กำหนด setup days ตาม FIBER_TYPE (POLY = 5 วัน, อื่นๆ = 3 วัน)
        item_fiber_type = get_fiber_type_for_item(item)
        item_setup_days = 5 if item_fiber_type == "POLY" else SETUP_DAYS
        # ถ้าเป็น urgent หรือใกล้ RDD ให้ใช้ความสามารถสูงสุด
        if urgent_mode or (
            rdd_idx is not None and _plan_idx is not None and _plan_idx >= rdd_idx - 1
        ):
            # ใช้วันทำงานตามที่โรงงานกำหนด (ไม่เปลี่ยนแปลง)
            # urgent mode ไม่สามารถเพิ่มวันทำงานเกินที่โรงงานเปิดได้
            pass

        # ตรวจสอบว่าสัปดาห์นี้เคยใช้ setup ไปแล้วหรือไม่
        week_key = (plan_week, mc_group)
        factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week)
        # แยกเครื่อง carry-over (ไม่ต้อง setup) vs เครื่องใหม่ (ต้อง setup)
        mc_key = _ck(item, mc_group, _sel_gauge)
        prev_machines = machines_in_use.get(mc_key, 0)
        # If RTS+LOCAL rule applies and the selected mc_group matches, force carryover-only
        if rts_local_force:
            last_old_by_mc = rts_local_force.get("last_old_by_mc", {})
            machines_by_mc = rts_local_force.get("machines_by_mc", {})
            if str(mc_group).strip().upper() in last_old_by_mc:
                last_w = last_old_by_mc.get(str(mc_group).strip().upper())
                start_after = next_week(last_w)
                if plan_week is None or plan_week < start_after:
                    plan_week = start_after
                # ใช้ machines_by_mc (จาก booking_final_ready25 ทุก week) เป็น primary
                # เพราะ machines_in_use มีแค่ week <= TODAY_WEEK ซึ่งอาจเป็น SO เก่า/เครื่องมากกว่าจริง
                _bk_mc = machines_by_mc.get(str(mc_group).strip().upper())
                if _bk_mc is not None and _bk_mc > 0:
                    forced_m = _bk_mc
                else:
                    forced_m = machines_in_use.get(mc_key, 0)
                prev_machines = int(forced_m or 0)
        current_week_idx = week_index(plan_week)
        prev_week_idx = last_production.get(mc_key)
        # is_continuing = week ติดกัน AND เป็น SC/SO NO เดียวกัน (ต่างสี = เริ่มใหม่)
        # For RTS_LOCAL rule we must continue from old regardless of SC/SO, so
        # treat same_order as True when rts_local_force applies for this mc_group.
        # ถ้าเป็นกรณี RTS+LOCAL และ mc_group นี้มี old booking ให้บังคับต่อจาก old (ไม่สน SC/SO)
        if rts_local_force and str(mc_group).strip().upper() in rts_local_force.get(
            "last_old_by_mc", {}
        ):
            same_order = True
        else:
            # เปรียบเทียบ SC/SO
            same_order = last_sc_so_no.get(mc_key) == sc_so_no
            # ถ้าอนุญาต carryover ข้าม SO ให้ตรวจเงื่อนไขเพิ่มเติม
            if not same_order and ALLOW_CARRYOVER_ACROSS_SO:
                prev_m = machines_in_use.get(mc_key, 0)
                prev_week_idx = last_production.get(mc_key)
                if (
                    prev_m > 0
                    and prev_week_idx is not None
                    and current_week_idx is not None
                    and (current_week_idx - prev_week_idx) <= SETUP_GAP_WEEK
                ):
                    same_order = True
                # เพิ่มเติม: ถ้า ALLOW_CARRYOVER_ACROSS_SO ให้ same_order = True เสมอ (ข้าม SC/SO ได้)
                if ALLOW_CARRYOVER_ACROSS_SO:
                    same_order = True
        _is_same_sc = last_sc_so_no.get(mc_key) == sc_so_no
        # ถ้าเงื่อนไข carryover อนุญาต (same_order=True) ให้ same-week ต่อได้
        _week_ok = (
            current_week_idx is not None
            and prev_week_idx is not None
            and (current_week_idx >= prev_week_idx)
        )
        is_continuing = (
            prev_week_idx is not None
            and current_week_idx is not None
            and same_order
            and _week_ok
            and (
                current_week_idx - prev_week_idx <= 1  # week ติดกัน หรือ same week
                or (current_week_idx - prev_week_idx <= SETUP_GAP_WEEK)
            )
        )  # ต้องไม่ห่างเกิน gap
        # ❗ ถ้า item+mc นี้ยังไม่เคยผลิตใน new plan → บังคับ setup (ไม่อ้าง old plan)
        if mc_key not in new_plan_started_items:
            is_continuing = False
        if is_continuing:
            carryover_mc = min(prev_machines, available_machines)  # เครื่องที่ผลิตต่อ
            new_mc = max(0, available_machines - carryover_mc)
        else:
            carryover_mc = 0
            new_mc = available_machines  # ทุกเครื่องต้อง setup
        # Enforce RTS+LOCAL: use existing carryover machines only (no new setup)
        # prev_machines comes from machines_in_use (last active week, MC_USE_CEIL>0)
        if rts_local_force and str(mc_group).strip().upper() in rts_local_force.get(
            "last_old_by_mc", {}
        ):
            carryover_mc = int(prev_machines or 0)
            new_mc = 0
            available_machines = carryover_mc
            setup_needed = False

        # ===== Carryover-first: ตรวจว่า carryover เพียงพอทัน rdd ไหม =====
        # Simulate production จาก plan_week ถึง rdd_idx ด้วย carry เครื่อง
        # Week 1: carry ผลิตเต็ม, new ผลิตหัก setup  |  Week 2+: ทุกเครื่องเป็น carry

        def _forward_sim(carry, new, q_left):
            q = q_left
            wk = plan_week
            first = True
            while wk is not None and q > 0:
                w_idx_sim = week_index(wk)
                if w_idx_sim is None or (rdd_idx is not None and w_idx_sim > rdd_idx):
                    break

                cal = len(get_working_days_in_week(wk))
                fac = get_working_days_by_factory(mc_group, carry + new, week=wk)
                wd = min(cal, fac)
                if first:
                    c_prod = carry * wd * daily_capacity
                    n_prod = new * max(0, wd - item_setup_days) * daily_capacity
                    first = False
                else:
                    c_prod = (carry + new) * wd * daily_capacity
                    n_prod = 0
                total = c_prod + n_prod
                if rev_weight and rev_weight > 0 and total > 0:
                    total = (total // rev_weight) * rev_weight
                q -= total
                wk = next_week(wk)
            return q  # <= 0 หมายถึงทัน

        if not past_rdd and rdd_idx is not None and not rts_local_force and new_mc > 0:
            if carryover_mc > 0 and _forward_sim(carryover_mc, 0, qty_left) <= 0:
                # carryover เพียงพอทัน → ไม่ต้อง setup เพิ่ม
                new_mc = 0
            else:
                # หา new_mc น้อยสุดที่ทัน (จาก carryover + new)
                found_n = new_mc  # fallback = ทั้งหมด
                for try_n in range(1, new_mc + 1):
                    if _forward_sim(carryover_mc, try_n, qty_left) <= 0:
                        found_n = try_n
                        break

                new_mc = found_n
            available_machines = carryover_mc + new_mc
        # จำนวน new_mc ที่ _forward_sim หาได้ = น้อยสุดที่ทัน → เป็น lower bound
        _forward_min_new = new_mc

        # ===== Dynamic setup limit ตาม urgency RDD =====
        _remaining_slots = get_remaining_job_slots(plan_week, mc_group)
        _req_mc_dyn = required_machines_info[2] if required_machines_info else new_mc
        # ถ้า simulate แล้วไม่ทัน → เปิดเต็ม slots (ไม่ cap ที่ required_mc)
        if not _req_feasible:
            _dyn_limit = _remaining_slots
        else:
            _dyn_limit = _dynamic_setup_limit(
                plan_week, rdd_idx, _req_mc_dyn, _remaining_slots
            )
        # ห้าม cap ต่ำกว่า _forward_min_new — ไม่งั้น forward sim ไม่มีความหมาย
        _dyn_limit = max(_dyn_limit, _forward_min_new)
        if new_mc > _dyn_limit:
            new_mc = _dyn_limit
        available_machines = carryover_mc + new_mc
        # ใช้ actual_working_days (หักวันหยุดแล้ว) แทน factory_working_days แบบ static
        prod_days_old = actual_working_days  # เครื่อง carry-over ผลิตตามวันเปิดจริง
        # เครื่องใหม่ (new_mc) ต้อง setup เสมอ แม้ item จะ warm บนเครื่องเดิม (setup_needed=False)
        # setup_needed=False หมายถึงเครื่องที่วิ่งอยู่แล้ว ไม่ใช่เครื่องที่เพิ่งเพิ่มมา
        prod_days_new = max(0, actual_working_days - item_setup_days)

        # ===== Optimize: ลดเครื่องให้น้อยสุดที่ยังผลิตพอครอบคลุม qty_left =====
        # เช่น week15 carry=3 แต่ qty_left น้อย → ใช้แค่ 1 เครื่องก็เสร็จใน week นี้
        # ใช้การจำลองผลิตจริง (รวม rev_weight rounding) เพื่อความแม่นยำ

        def _sim_produce(c_mc, n_mc):
            c_cap = daily_capacity * prod_days_old * c_mc
            n_cap = daily_capacity * prod_days_new * n_mc
            total_cap = c_cap + n_cap
            if rev_weight and rev_weight > 0 and total_cap > 0:
                return (total_cap // rev_weight) * rev_weight

            return total_cap

        if carryover_mc + new_mc > 0 and _sim_produce(carryover_mc, new_mc) > qty_left:
            opt_carry, opt_new = carryover_mc, new_mc  # fallback = ไม่ลด
            # ขั้นที่ 1: ลอง carry-only (ไม่ต้อง setup เพิ่ม) หาน้อยสุดที่ produce ≥ qty_left
            found = False
            for try_c in range(1, carryover_mc + 1):
                if _sim_produce(try_c, 0) >= qty_left:
                    opt_carry = try_c
                    opt_new = 0
                    found = True
                    break

            if not found and new_mc > 0:
                # ขั้นที่ 2: ต้องมี new ด้วย → ลด new ให้น้อยสุด
                for try_n in range(0, new_mc + 1):
                    if _sim_produce(carryover_mc, try_n) >= qty_left:
                        opt_carry = carryover_mc
                        opt_new = try_n
                        break

            if opt_carry + opt_new < available_machines:
                carryover_mc = opt_carry
                new_mc = opt_new
                available_machines = opt_carry + opt_new

        # ===== Hard-cap: enforce job cap ก่อนคำนวณ produce =====
        # ตรวจเด็ดขาดว่า new_mc ที่จะ setup ไม่เกิน remaining capacity
        _type_used_now = get_type_used_jobs(plan_week, mc_group)
        # บังคับ committed_carryover = carryover_mc เสมอเมื่อ ALLOW_CARRYOVER_ACROSS_SO
        _committed_carryover = carryover_mc if ALLOW_CARRYOVER_ACROSS_SO else 0
        _allowed_new = check_job_capacity_limit(mc_group, new_mc, False, _type_used_now, committed_carryover=_committed_carryover)
        if _allowed_new < new_mc:
            new_mc = _allowed_new
            available_machines = carryover_mc + new_mc
        if new_mc == 0 and carryover_mc == 0:
            # ไม่มีเครื่องเลย ข้ามไป week ถัดไป
            plan_week = next_week(plan_week)
            continue

        # setup_days_used สำหรับ log — เครื่องใหม่ต้อง setup เสมอ
        setup_days_used = item_setup_days if new_mc > 0 else 0
        # available_days สำหรับ log (ใช้เครื่องใหม่เป็นหลักถ้ามี)
        available_days = prod_days_new if new_mc > 0 else prod_days_old

        # === Same-week remaining cap: item+machine เดียวกัน ต่อจาก FG ก่อนหน้า ===
        # ใช้ remaining_week_cap ข้าม SC/SO ได้ (key = (plan_week, item, mc_group))
        _same_week_rem_cap = remaining_week_cap.get((plan_week, item, mc_group), None)
        # ถ้า FG ใหม่ (SC/SO ใหม่) เริ่มใน week เดิมและ cap ยังเหลือ ให้ใช้ cap นี้
        if _same_week_rem_cap is None and ALLOW_CARRYOVER_ACROSS_SO:
            _same_week_rem_cap = remaining_week_cap.get((plan_week, item, mc_group), None)
        # คำนวณ PRODUCE_QTY ตามสูตรที่แม่นยำ
        if _same_week_rem_cap is not None:
            # ผลิตด้วย cap ที่เหลือจาก FG ก่อนหน้าในสัปดาห์เดียวกัน
            if rev_weight and rev_weight > 0:
                _rem_batches = int(_same_week_rem_cap // rev_weight)
                produce = min(qty_left, _rem_batches * rev_weight)
            else:
                produce = min(qty_left, _same_week_rem_cap)
            if produce <= 0:
                # cap เหลือน้อยเกินไป (rev_weight rounding) → ข้ามไป week ถัดไป
                remaining_week_cap.pop((plan_week, item, mc_group), None)
                plan_week = next_week(plan_week)
                continue

            cap_old, cap_new = produce, 0  # สำหรับ remaining cap tracking
        elif rev_weight is not None and rev_weight > 0:
            cap_old = daily_capacity * prod_days_old * carryover_mc
            cap_new = daily_capacity * prod_days_new * new_mc
            max_capacity = cap_old + cap_new
            max_batches = max_capacity // rev_weight
            produce = min(qty_left, max_batches * rev_weight)
        else:
            cap_old = daily_capacity * prod_days_old * carryover_mc
            cap_new = daily_capacity * prod_days_new * new_mc
            produce = min(qty_left, cap_old + cap_new)
        # ไม่เพิ่มแถวถ้าไม่มีการผลิต
        if produce <= 0:
            break

        # จำนวนเครื่องที่วางแผนไว้ (จาก calculate_required_machines)
        # ถ้าไม่ทัน RDD (past_rdd) → แสดง "Maxmc" แทนจำนวนเครื่อง
        prev_week_mc = machines_in_use.get(
            _ck(item, mc_group, _sel_gauge), available_machines
        )
        planned_mc = (
            prev_week_mc
            if past_rdd
            else (
                required_machines_info[2]
                if required_machines_info
                else available_machines
            )
        )
        plans.append(
            {
                "ITEM_CODE": item,
                "SC_SO_NO": order["SC/SO NO"],
                "MC_GROUP": mc_group,
                "MC_GUAGE": order["MC_GUAGE"],
                "FACTORY_TYPE": FACTORY_TYPE_MAP.get(mc_group, "UNKNOWN"),
                "PLAN_WEEK": plan_week,
                "PRODUCE_QTY": produce,
                "SETUP_DAYS": setup_days_used,
                "REQUIRED_MC": planned_mc,  # เครื่องที่คำนวณไว้ล่วงหน้า (RDD target) หรือ "Maxmc" ถ้าไม่ทัน RDD
                "ACTUAL_MC": available_machines,  # เครื่องที่ใช้จริง week นี้
                "CARRYOVER_MC": carryover_mc,  # เครื่องที่ carry-over จาก week ก่อน
                "NEW_MC": new_mc,  # เครื่องใหม่ที่ setup week นี้
                "FACTORY_WORKING_DAYS": get_working_days_by_factory(
                    mc_group, available_machines, week=plan_week
                ),
                "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(plan_week)),
                "ACTUAL_WORKING_DAYS": get_working_days_by_factory(mc_group, available_machines, week=plan_week)
                if plan_week == 17
                else min(
                    len(get_working_days_in_week(plan_week)),
                    get_working_days_by_factory(mc_group, available_machines, week=plan_week),
                ),
                "DAILY_CAPACITY": daily_capacity,
                "REVOLUTION_WEIGHT": rev_weight if rev_weight is not None else 0,
                "AVAILABLE_DAYS": available_days,
                "ORDERS_QTY": order_qty,
                "PENDING_PLAN": pending_plan,
                "PLAN_QTY": qty_left - produce,
                "ORDER_TYPE": order_type,
                "ORDER_DATE": order["Date"],
                "FG_WEEK": fg_week,
                "TARGET_KNIT": fg_week_int,
                "FIBER_TYPE": get_fiber_type_for_item(item),
                "IS_CORE_ITEM": "CORE ITEM" if is_core_item else "",
                "CUSTOMER": str(order.get("Customer", "")).strip(),
                "PLAN_SOURCE": "NEW",
            }
        )
        qty_left -= produce
        if qty_left <= 0:
            qty_left = 0  # ป้องกันค่าติดลบ
        # บันทึก/อัปเดต remaining cap สำหรับ FG ถัดไปของ item+machine เดียวกัน
        if _same_week_rem_cap is not None:
            # อัปเดต remaining cap หลังใช้งาน
            _new_rem = max(0, _same_week_rem_cap - produce)
            if _new_rem > 0:
                remaining_week_cap[(plan_week, item, mc_group)] = _new_rem
            else:
                remaining_week_cap.pop((plan_week, item, mc_group), None)
        elif qty_left <= 0:
            # Order จบ — บันทึกส่วนที่เหลือสำหรับ FG ถัดไป (รวม 0 = week เต็ม)
            _full_week_cap = cap_old + cap_new
            _rem = max(0, _full_week_cap - produce)
            remaining_week_cap[(plan_week, item, mc_group)] = _rem
        _plan_ck = _ck(item, mc_group, _sel_gauge)
        last_production[_plan_ck] = week_index(plan_week)
        machines_in_use[_plan_ck] = available_machines  # บันทึกจำนวนเครื่องที่ใช้จริง
        last_sc_so_no[_plan_ck] = sc_so_no  # บันทึก SC/SO NO ล่าสุดที่ผลิต
        new_plan_started_items.add(_plan_ck)  # บันทึกว่า item นี้เริ่ม new plan แล้ว
        # อัพเดท job usage สำหรับสัปดาห์นี้ (นับเฉพาะ new_mc = machines ที่ setup ใหม่)
        if plan_week not in weekly_job_usage:
            weekly_job_usage[plan_week] = {}
        weekly_job_usage[plan_week][mc_group] = (
            weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
        )
        # อัพเดท new plan usage (นับทั้ง carryover+new สำหรับ get_actual_mc_remain)
        # key ต้องเป็น (mc_group, gauge_str) เสมอ — ห้าม pool ข้าม GUAGE
        # ถ้าเป็น same-week continuation → เครื่องนับไปแล้วจาก FG ก่อนหน้า ห้ามนับซ้ำ
        if _same_week_rem_cap is None:
            if plan_week not in weekly_new_plan_usage:
                weekly_new_plan_usage[plan_week] = {}
            _wpu_cap = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item)
                & (item_cap_data["MC_GROUP"] == mc_group)
            ]
            _wpu_gauge_raw = _wpu_cap.iloc[0]["GUAGE"] if not _wpu_cap.empty else None
            _wpu_gauge_str = (
                str(_wpu_gauge_raw).strip()
                if _wpu_gauge_raw is not None
                and not (isinstance(_wpu_gauge_raw, float) and pd.isna(_wpu_gauge_raw))
                else ""
            )
            _wpu_key = (mc_group, _wpu_gauge_str)
            weekly_new_plan_usage[plan_week][_wpu_key] = (
                weekly_new_plan_usage[plan_week].get(_wpu_key, 0) + available_machines
            )
        # ก้าวไป week ถัดไปเสมอหลัง produce (ห้าม plan item เดิมใน week เดิมซ้ำ)
        _produced_week = plan_week
        plan_week = next_week(plan_week)
        # ตรวจสอบว่าแผนใหม่ไม่เกิน capacity ที่เหลือหลังจากแผนเก่า
        if not old_plan_df.empty and "PLAN_WEEK" in old_plan_df.columns:
            # หาจำนวน jobs ของแผนเก่าในสัปดาห์นี้
            old_week_jobs = old_plan_df[old_plan_df["PLAN_WEEK"] == _produced_week]
            if not old_week_jobs.empty:
                mc_col = (
                    "REQUIRED_MC"
                    if "REQUIRED_MC" in old_week_jobs.columns
                    else "AVAILABLE_MACHINES"
                )
                # Ensure the column is numeric before summing to avoid string concatenation
                old_jobs_by_mc = (
                    old_week_jobs.assign(
                        _mc_num=pd.to_numeric(
                            old_week_jobs[mc_col], errors="coerce"
                        ).fillna(0)
                    )
                    .groupby("MC_GROUP")["_mc_num"]
                    .sum()
                    .astype(int)
                    .to_dict()
                )
                # ตรวจสอบแต่ละ MC_GROUP ว่าเกินแผนเก่าหรือไม่
                for used_mc_group, new_jobs in weekly_job_usage[_produced_week].items():
                    old_jobs = old_jobs_by_mc.get(used_mc_group, 0)
                    if new_jobs > old_jobs:
                        print(
                            f"⚠️  OVER OLD PLAN: Week {_produced_week} {used_mc_group} ใช้ {new_jobs} jobs (เกินแผนเก่า {old_jobs} jobs)"
                        )
                # ตรวจสอบ capacity ที่เหลือหลังจากแผนเก่า
                remaining_capacity_by_type = {}
                for used_mc_group in weekly_job_usage[_produced_week]:
                    mc_info = master_mc[master_mc["MC"] == used_mc_group]
                    if not mc_info.empty:
                        factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
                        _raw_t = mc_info.iloc[0].get("Type", "")
                        mc_type = "" if pd.isna(_raw_t) else str(_raw_t).strip().upper()
                        type_key = _make_type_key(factory, mc_type)
                        # หาจำนวน jobs ของแผนเก่าใน type เดียวกัน
                        old_type_jobs = 0
                        for old_mc_group, old_jobs in old_jobs_by_mc.items():
                            old_mc_info = master_mc[master_mc["MC"] == old_mc_group]
                            if not old_mc_info.empty:
                                old_factory = (
                                    str(old_mc_info.iloc[0]["Factory"]).strip().upper()
                                )
                                _raw_ot = old_mc_info.iloc[0].get("Type", "")
                                old_mc_type = (
                                    ""
                                    if pd.isna(_raw_ot)
                                    else str(_raw_ot).strip().upper()
                                )
                                old_type_key = _make_type_key(old_factory, old_mc_type)
                                if old_type_key == type_key:
                                    old_type_jobs += old_jobs
                        # คำนวณ capacity ที่เหลือ
                        if type_key not in remaining_capacity_by_type:
                            if factory == "PHET":
                                if mc_type == "DOUBLE":
                                    max_capacity = 33
                                elif mc_type == "SINGLE":
                                    max_capacity = 44
                            elif factory == "OM":
                                max_capacity = 13
                            remaining_capacity_by_type[type_key] = (
                                max_capacity - old_type_jobs
                            )
                # ตรวจสอบว่าเกิน capacity ที่เหลือหรือไม่
                for type_key, remaining_capacity in remaining_capacity_by_type.items():
                    # หาจำนวน jobs ใหม่ใน type เดียวกัน
                    new_type_jobs = 0
                    for used_mc_group, new_jobs in weekly_job_usage[
                        _produced_week
                    ].items():
                        mc_info = master_mc[master_mc["MC"] == used_mc_group]
                        if not mc_info.empty:
                            factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
                            _raw_t2 = mc_info.iloc[0].get("Type", "")
                            mc_type = (
                                "" if pd.isna(_raw_t2) else str(_raw_t2).strip().upper()
                            )
                            current_type_key = _make_type_key(factory, mc_type)
                            if current_type_key == type_key:
                                new_type_jobs += new_jobs
                    if new_type_jobs > remaining_capacity:
                        print(
                            f"⚠️  OVER REMAINING CAPACITY: Week {_produced_week} {type_key} ใช้ {new_type_jobs} jobs (เกิน capacity ที่เหลือ {remaining_capacity} jobs, แผนเก่าใช้ไปแล้ว)"
                        )
        # ตรวจสอบและแสดงผลการทับซ้อนในสัปดาห์นี้ (ตาม factory type)
        total_jobs_by_type = {}
        max_capacity_by_type = {}
        # คำนวณ jobs และ capacity ตาม factory type
        for used_mc_group in weekly_job_usage[_produced_week]:
            mc_info = master_mc[master_mc["MC"] == used_mc_group]
            if not mc_info.empty:
                factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
                _raw_t = mc_info.iloc[0].get("Type", "")
                mc_type = "" if pd.isna(_raw_t) else str(_raw_t).strip().upper()
                type_key = _make_type_key(factory, mc_type)
                # บวก jobs ที่ใช้
                if type_key not in total_jobs_by_type:
                    total_jobs_by_type[type_key] = 0
                total_jobs_by_type[type_key] += weekly_job_usage[_produced_week][
                    used_mc_group
                ]
                # กำหนด capacity ตาม type
                if type_key not in max_capacity_by_type:
                    if factory == "PHET":
                        if mc_type == "DOUBLE":
                            max_capacity_by_type[type_key] = 33
                        elif mc_type == "SINGLE":
                            max_capacity_by_type[type_key] = 44
                    elif factory in ("OM", "OMNOI"):
                        max_capacity_by_type[type_key] = 13
                    else:
                        max_capacity_by_type[type_key] = (
                            9999  # ไม่รู้จัก type นี้ ให้ใช้ค่า max เพื่อไม่ overload
                        )
        # ตรวจสอบและแสดงผลเฉพาะตอนที่เกิน
        for type_key, jobs_used in total_jobs_by_type.items():
            capacity = max_capacity_by_type.get(type_key, 9999)
            if jobs_used > capacity:
                print(
                    f"⚠️  OVERLOAD: Week {_produced_week} {type_key} ใช้ {jobs_used} jobs (เกิน capacity {capacity} jobs)"
                )
    # บันทึก last plan week สำหรับ SC/SO+Item → บังคับ FG_WEEK ถัดไปเริ่มหลังนี้
    if _produced_week is not None:
        _lpw_idx = week_index(_produced_week)
        if _lpw_idx is not None:
            _prev = _last_fg_plan_idx.get((sc_so_no, item))
            if _prev is None or _lpw_idx > _prev:
                _last_fg_plan_idx[(sc_so_no, item)] = _lpw_idx

# =========================
# CAPACITY OPTIMIZATION

# =========================
# Optimize machine utilization by filling unused capacity with same-item orders from different SCs
plans = detect_and_fill_unused_capacity(plans, orders)

# =========================
# EXPORT

# =========================
plan_df = pd.DataFrame(plans)
DATA_PLAN_DIR.mkdir(exist_ok=True)
# สรุปแผนใหม่
if not plan_df.empty:
    print("📊 สรุปแผนการผลิตใหม่:")
    # Coerce `REQUIRED_MC` to numeric to avoid string concatenation when summing
    new_summary = (
        plan_df.assign(
            _req_mc=pd.to_numeric(plan_df["REQUIRED_MC"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        .groupby(["MC_GROUP", "PLAN_WEEK"])["_req_mc"]
        .sum()
        .reset_index()
        .rename(columns={"_req_mc": "REQUIRED_MC"})
    )
    for week in sorted(new_summary["PLAN_WEEK"].unique()):
        week_data = new_summary[new_summary["PLAN_WEEK"] == week]
        week_sum = week_data["REQUIRED_MC"].sum()
        if week == 19:  # Debug Week 19
            print(f"🔍 Week {week} details:")
            for _, row in week_data.iterrows():
                print(f"   {row['MC_GROUP']}: {row['REQUIRED_MC']} machines")
            print(f"   Total: {week_sum}")
        print(f"  Week {week}: REQUIRED_MC = {week_sum}")
else:
    print("⚠️ ไม่มีแผนการผลิตที่สามารถสร้างได้")
    print()

# =========================
# REMAINING JOBS PER WEEK

# =========================
_CAPACITY_MAP = {
    "PHET_DOUBLE": 33,
    "PHET_SINGLE": 44,
    "OM": 13,
}


def _sum_by_type(job_dict_by_week, week):
    """รวม jobs ตาม factory_type สำหรับ week ที่ระบุ (factory-wide รวมทุก MC_GROUP ใน type)"""
    result: dict = {}
    for _mc_group, _jobs in job_dict_by_week.get(week, {}).items():
        _type_key = _get_type_key_for_mc(_mc_group)
        result[_type_key] = result.get(_type_key, 0) + _jobs
    return result

# รวม weeks จากทั้ง old และ new
_all_weeks = sorted(
    set(list(weekly_job_usage.keys()) + list(weekly_job_usage_old.keys()))
)
_remaining_rows = []
for _week in _all_weeks:
    _total_by_type = _sum_by_type(weekly_job_usage, _week)
    _old_by_type = _sum_by_type(weekly_job_usage_old, _week)
    _all_types = set(list(_total_by_type.keys()) + list(_old_by_type.keys()))
    for _type_key in sorted(_all_types):
        _total_used = _total_by_type.get(_type_key, 0)
        _old_used = _old_by_type.get(_type_key, 0)
        _new_used = _total_used - _old_used
        _cap = _CAPACITY_MAP.get(_type_key, None)
        _rem = (_cap - _total_used) if _cap is not None else None
        _remaining_rows.append(
            {
                "WEEK": _week,
                "TYPE": _type_key,
                "OLD_JOBS": _old_used,
                "NEW_JOBS": _new_used,
                "TOTAL_JOBS": _total_used,
                "CAPACITY": _cap,
                "REMAINING_JOBS": _rem,
            }
        )
remaining_df = (
    pd.DataFrame(_remaining_rows)
    if _remaining_rows
    else pd.DataFrame(
        columns=[
            "WEEK",
            "TYPE",
            "OLD_JOBS",
            "NEW_JOBS",
            "TOTAL_JOBS",
            "CAPACITY",
            "REMAINING_JOBS",
        ]
    )
)
print("📋 สรุป Remaining Jobs ต่อ Week (factory-wide ต่อ Type, OLD + NEW):")
if not remaining_df.empty:
    for _week in sorted(remaining_df["WEEK"].unique()):
        _wdf = remaining_df[remaining_df["WEEK"] == _week]
        print(f"   Week {_week}:")
        for _, _row in _wdf.iterrows():
            _cap_s = str(int(_row["CAPACITY"])) if pd.notna(_row["CAPACITY"]) else "-"
            _rem_v = _row["REMAINING_JOBS"] if pd.notna(_row["REMAINING_JOBS"]) else 1
            _rem_s = str(int(_rem_v)) if pd.notna(_row["REMAINING_JOBS"]) else "-"
            _icon = "🔴" if _rem_v < 0 else ("🟡" if _rem_v <= 5 else "🟢")
            print(
                f"     {_icon} {_row['TYPE']}: old={int(_row['OLD_JOBS'])} + new={int(_row['NEW_JOBS'])} = {int(_row['TOTAL_JOBS'])}/{_cap_s}  เหลือ {_rem_s} jobs"
            )
    print()
else:
    print("   (ไม่มีข้อมูล)")
    print()
# แสดง item ที่ไม่มี CAP
if _skip_no_cap:
    print(f"\n⚠️  Items ที่ไม่พบ CAP data ({len(_skip_no_cap)} รายการ) → ไม่ได้วางแผน:")
    for _s in sorted(set(_skip_no_cap)):
        print(f"   - {_s}")
    print(f"   กรุณาเพิ่มใน item_cap2025.xlsx")
    print()
# บันทึกไฟล์ใหม่
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as _writer:
    plan_df.to_excel(_writer, sheet_name="PLAN", index=False)
    remaining_df.to_excel(_writer, sheet_name="REMAINING_JOBS", index=False)

# =========================
# PIVOT_PLAN sheet — Excel PivotTable จริง (ผ่าน win32com)
# Filters: PLAN_SOURCE | Rows: ITEM_CODE, SC_SO_NO, FG_WEEK | Cols: PLAN_WEEK | Values: Sum PRODUCE_QTY

# =========================
if not plan_df.empty:
    try:
        import win32com.client
        import pythoncom
        pythoncom.CoInitialize()
        _excel_app = None
        try:
            _excel_app = win32com.client.Dispatch("Excel.Application")
            _excel_app.Visible = False
            _excel_app.DisplayAlerts = False
            _abs_path = str(OUTPUT_FILE.resolve())
            _wb_com = _excel_app.Workbooks.Open(_abs_path)
            # กำหนด data range บน PLAN sheet
            _plan_ws_com = _wb_com.Sheets("PLAN")
            _n_rows = plan_df.shape[0] + 1  # รวม header
            _n_cols = plan_df.shape[1]
            _data_range = _plan_ws_com.Range(
                _plan_ws_com.Cells(1, 1),
                _plan_ws_com.Cells(_n_rows, _n_cols),
            )
            # สร้าง PivotCache จาก PLAN sheet
            _pivot_cache = _wb_com.PivotCaches().Create(
                SourceType=1,  # xlDatabase
                SourceData=_data_range,
            )
            # ลบ PIVOT_PLAN sheet เดิม (ถ้ามี) แล้วสร้างใหม่
            for _sh in list(_wb_com.Sheets):
                if _sh.Name == "PIVOT_PLAN":
                    _sh.Delete()
                    break

            _pivot_ws_com = _wb_com.Sheets.Add(
                After=_wb_com.Sheets(_wb_com.Sheets.Count)
            )
            _pivot_ws_com.Name = "PIVOT_PLAN"
            # สร้าง PivotTable บนเซลล์ A3
            _pt = _pivot_cache.CreatePivotTable(
                TableDestination=_pivot_ws_com.Range("A3"),
                TableName="PivotPlan",
            )
            # Filter field: PLAN_SOURCE
            _pt.PivotFields("PLAN_SOURCE").Orientation = 3  # xlPageField
            _pt.PivotFields("PLAN_SOURCE").Position = 1
            # Row fields: ITEM_CODE → MC_GROUP → SC_SO_NO → FG_WEEK → ACTUAL_MC → DAILY_CAPACITY → SETUP_DAYS → ACTUAL_WORKING_DAYS → AVAILABLE_DAYS
            for _pos, _fname in enumerate([
                "ITEM_CODE", "MC_GROUP", "SC_SO_NO", "FG_WEEK",
                "ACTUAL_MC", "DAILY_CAPACITY", "SETUP_DAYS",
                "ACTUAL_WORKING_DAYS", "AVAILABLE_DAYS",
            ], start=1):
                _pt.PivotFields(_fname).Orientation = 1  # xlRowField
                _pt.PivotFields(_fname).Position = _pos
            # Column field: PLAN_WEEK
            _pt.PivotFields("PLAN_WEEK").Orientation = 2  # xlColumnField
            _pt.PivotFields("PLAN_WEEK").Position = 1
            # Value field: Sum of PRODUCE_QTY
            _pt.AddDataField(
                _pt.PivotFields("PRODUCE_QTY"),
                "Sum of PRODUCE_QTY",
                -4157,  # xlSum
            )
            _wb_com.Save()
            _wb_com.Close(False)
            print("✅ สร้าง PIVOT_PLAN (Excel PivotTable) สำเร็จ")
        finally:
            if _excel_app:
                _excel_app.Quit()
            pythoncom.CoUninitialize()
    except ImportError:
        print("⚠️ ไม่พบ win32com → ข้าม PivotTable")
    except Exception as _e:
        print(f"⚠️ สร้าง PivotTable ไม่สำเร็จ: {_e}")
print("Weekly production planning completed")
print(f"Output: {OUTPUT_FILE}")
print(f"Total rows: {len(plan_df)}")

# =========================
# EXPORT COMBINED (OLD + NEW)

# =========================
COMBINED_FILE = DATA_PLAN_DIR / "weekly_production_plan_combined_filtered.xlsx"
new_df = plan_df.copy()
if "PLAN_SOURCE" not in new_df.columns:
    new_df.insert(0, "PLAN_SOURCE", "NEW")
# สร้าง OLD rows จาก detail_mc โดยเทียบ ITEM_CODE + MC_GROUP กับ items ที่อยู่ใน new plan
# (ไม่ต้อง match SO_NO เพราะต้องการดู old plan ของ item นั้นทั้งหมดเพื่อเทียบ CARRYOVER_MC)
new_item_mc_keys = set(
    zip(
        plan_df["ITEM_CODE"].astype(str).str.strip().str.upper(),
        plan_df["MC_GROUP"].astype(str).str.strip().str.upper(),
    )
)
old_booking_df = pd.DataFrame()
if not detail_mc.empty and new_item_mc_keys:
    _det = detail_mc.copy()
    _det["_ITEM_U"] = _det["ITEM_CODE"].astype(str).str.strip().str.upper()
    _det["_MC_U"] = _det["MC_GROUP"].astype(str).str.strip().str.upper()
    _mask = _det.apply(lambda r: (r["_ITEM_U"], r["_MC_U"]) in new_item_mc_keys, axis=1)
    old_booking_df = _det[_mask].drop(columns=["_ITEM_U", "_MC_U"]).copy()
    # Rename detail_mc columns → ชื่อเดียวกับ new plan
    old_booking_df = old_booking_df.rename(
        columns={
            "GUAGE": "MC_GUAGE",
            "WEEK": "PLAN_WEEK",
            "KP_WEIGHT": "PRODUCE_QTY",
            "MC_USE_CEIL": "REQUIRED_MC",
            "MC_USE": "ACTUAL_MC",
            "CAP ทอ": "DAILY_CAPACITY",
            "REVOLUTION/WEIGHT": "REVOLUTION_WEIGHT",
            "SO_NO": "SC_SO_NO",
        }
    )
    # ตัด S นำหน้า SC_SO_NO (เช่น "S717455" → "717455")
    if "SC_SO_NO" in old_booking_df.columns:
        old_booking_df["SC_SO_NO"] = (
            old_booking_df["SC_SO_NO"].astype(str).str.lstrip("Ss")
        )
    old_booking_df.insert(0, "PLAN_SOURCE", "OLD")
    # แปลง PLAN_WEEK เป็นตัวเลขถ้ามี
    if "PLAN_WEEK" in old_booking_df.columns:
        old_booking_df["PLAN_WEEK"] = pd.to_numeric(
            old_booking_df["PLAN_WEEK"], errors="coerce"
        )
    # เก็บแถว OLD ทั้งหมดที่ match กับ new plan (ไม่จำกัดสัปดาห์)
    old_booking_df = old_booking_df.sort_values(
        ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK"], na_position="last"
    ).reset_index(drop=True)
    print(
        f"📦 OLD rows จาก booking_final_ready25 (match ITEM+MC, ทั้งหมด): {len(old_booking_df)} rows"
    )
else:
    print("⚠️ ไม่พบข้อมูลใน detail_mc หรือไม่มี new plan → ข้าม OLD")
# รวม OLD + NEW โดยใช้ common columns เรียงตาม ITEM_CODE, MC_GROUP, PLAN_WEEK
# เพื่อให้เห็น OLD vs NEW week-by-week ของ item เดียวกันติดกัน
if not old_booking_df.empty:
    common_cols = ["PLAN_SOURCE"] + [
        c for c in new_df.columns if c in old_booking_df.columns and c != "PLAN_SOURCE"
    ]
    # บังคับให้ ITEM_CODE, MC_GROUP, PLAN_WEEK อยู่ใน common_cols เสมอ (ถ้ามีใน old)
    for _must in ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK"]:
        if _must not in common_cols and _must in old_booking_df.columns:
            common_cols.append(_must)
    if "TARGET_KNIT" not in common_cols and "TARGET_KNIT" in new_df.columns:
        common_cols.append("TARGET_KNIT")
        # คำนวณ TARGET_KNIT สำหรับ OLD rows โดย lookup จาก orders (FG Week → TARGET_KNIT week)

        def _lookup_target_knit_old(row):
            _so = str(row.get("SC_SO_NO", "")).strip().lstrip("Ss")
            _item = str(row.get("ITEM_CODE", "")).strip().upper()
            _match = orders[orders["SC/SO NO"].astype(str).str.strip() == _so]
            if _match.empty:
                return None

            _r = _match.iloc[0]
            _fg = _r.get("FG Week")
            _otype = str(_r.get("Orders Type", "")).strip().upper()
            if pd.isna(_fg):
                return None

            try:
                _fg_str = str(int(_fg))
                if len(_fg_str) >= 5:
                    _yr, _wk = int(_fg_str[:4]), int(_fg_str[4:])
                else:
                    _yr, _wk = TODAY.year, int(_fg_str)
                _crow = calendar_week[
                    (calendar_week["YEAR"] == _yr) & (calendar_week["WEEK"] == _wk)
                ]
                if _crow.empty:
                    return None

                _raw_idx = _crow.index[0]
                if _otype == "LAB-DIP":
                    _rdd_idx = min(len(calendar_week) - 1, TODAY_IDX + 2)
                else:
                    _rdd_idx = max(0, _raw_idx - 3)
                return int(calendar_week.iloc[_rdd_idx]["WEEK"])

            except Exception:
                return None

        old_booking_df["TARGET_KNIT"] = old_booking_df.apply(
            _lookup_target_knit_old, axis=1
        )
    if "IS_CORE_ITEM" not in common_cols and "IS_CORE_ITEM" in new_df.columns:
        common_cols.append("IS_CORE_ITEM")
        old_booking_df["IS_CORE_ITEM"] = ""
    if "CUSTOMER" not in common_cols and "CUSTOMER" in new_df.columns:
        common_cols.append("CUSTOMER")

        def _lookup_customer_old(row):
            _so = str(row.get("SC_SO_NO", "")).strip().lstrip("Ss")
            _match = orders[orders["SC/SO NO"].astype(str).str.strip() == _so]
            if _match.empty:
                return ""

            return str(_match.iloc[0].get("Customer", "")).strip()

        old_booking_df["CUSTOMER"] = old_booking_df.apply(
            _lookup_customer_old, axis=1
        )
    if "ORDER_TYPE" not in common_cols and "ORDER_TYPE" in new_df.columns:
        common_cols.append("ORDER_TYPE")

        def _lookup_order_type_old(row):
            _so = str(row.get("SC_SO_NO", "")).strip().lstrip("Ss")
            _match = orders[orders["SC/SO NO"].astype(str).str.strip() == _so]
            if _match.empty:
                return ""

            return str(_match.iloc[0].get("Orders Type", "")).strip()

        old_booking_df["ORDER_TYPE"] = old_booking_df.apply(
            _lookup_order_type_old, axis=1
        )
    if "FG_WEEK" not in common_cols and "FG_WEEK" in new_df.columns:
        common_cols.append("FG_WEEK")

        def _lookup_fg_week_old(row):
            _so = str(row.get("SC_SO_NO", "")).strip().lstrip("Ss")
            _match = orders[orders["SC/SO NO"].astype(str).str.strip() == _so]
            if _match.empty:
                return None

            _fg = _match.iloc[0].get("FG Week")
            return None if pd.isna(_fg) else _fg

        old_booking_df["FG_WEEK"] = old_booking_df.apply(_lookup_fg_week_old, axis=1)
    if "ORDERS_QTY" not in common_cols and "ORDERS_QTY" in new_df.columns:
        common_cols.append("ORDERS_QTY")

        def _lookup_orders_qty_old(row):
            _so = str(row.get("SC_SO_NO", "")).strip().lstrip("Ss")
            _match = orders[orders["SC/SO NO"].astype(str).str.strip() == _so]
            if _match.empty:
                return None

            return pd.to_numeric(_match.iloc[0].get("Orders.Qty"), errors="coerce")

        old_booking_df["ORDERS_QTY"] = old_booking_df.apply(
            _lookup_orders_qty_old, axis=1
        )
    combined_df = pd.concat(
        [
            old_booking_df[common_cols],
            new_df[[c for c in common_cols if c in new_df.columns]],
        ],
        ignore_index=True,
    )
    _sort_cols = [
        c
        for c in ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK", "PLAN_SOURCE"]
        if c in combined_df.columns
    ]
    combined_df = combined_df.sort_values(_sort_cols, ignore_index=True)
else:
    combined_df = new_df
# =========================
# คอลัมน์ TARGET_STATUS: ทัน / ไม่ทัน ตาม TARGET_KNIT
# เปรียบเทียบด้วย calendar index (รองรับข้ามปี)
# =========================
def _target_status(row) -> str:
    _src = str(row.get("PLAN_SOURCE", "")).strip().upper()
    _pw = row.get("PLAN_WEEK")
    _tk = row.get("TARGET_KNIT")
    if pd.isna(_pw) or pd.isna(_tk):
        return "-"

    try:
        _pw_idx = week_index(int(_pw))
        _tk_idx = week_index(int(_tk))
        if _pw_idx is None or _tk_idx is None:
            return "-"

        if _pw_idx <= _tk_idx:
            return "ทัน"

        else:
            return f"ไม่ทัน (+{_pw_idx - _tk_idx} wk)"

    except Exception:
        return "-"

combined_df["TARGET_STATUS"] = combined_df.apply(_target_status, axis=1)
with pd.ExcelWriter(COMBINED_FILE, engine="openpyxl") as writer:
    combined_df.to_excel(writer, sheet_name="PLAN", index=False)
    if not _no_cap_df.empty:
        _no_cap_df.to_excel(writer, sheet_name="NO_CAP", index=False)
print(f"Combined (OLD+NEW): {COMBINED_FILE}")
print(f"  OLD rows: {len(old_booking_df) if not old_booking_df.empty else 0}")
print(f"  NEW rows: {len(plan_df)}")
if not _no_cap_df.empty:
    print(f"  NO_CAP items: {_no_cap_df['Item Code'].nunique()} items, {len(_no_cap_df)} orders → sheet 'NO_CAP'")

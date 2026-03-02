import pandas as pd
import io
import re
from pathlib import Path
from Calendar import load_calendar, calendar_week_map
from ITEM_Cap import load_item_cap_data
from Yarn_Master import load_yarn_master

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).parent
DATA_PLAN_DIR = BASE_DIR / "data_plan"
DATA_DIR = BASE_DIR / "data"
CAP_DIR = DATA_DIR / "Cap"
ORDER_FILE = DATA_PLAN_DIR / "order_ready.xlsx"
MC_REMAIN_FILE = DATA_PLAN_DIR / "booking_final_ready25.xlsx"
ITEM_CAP_FILE = CAP_DIR / "item_cap2025.xlsx"
CALENDAR_FILE = BASE_DIR / "Calendar.xlsx"
BOOKING_DIR = BASE_DIR / "Booking"
OUTPUT_FILE = DATA_PLAN_DIR / "weekly_production_plan.xlsx"
SETUP_DAYS = 3
SETUP_GAP_WEEK = 2
# Allow carryover even when SC/SO changes (user option)
ALLOW_CARRYOVER_ACROSS_SO = True
# MAX_SETUP_MC แบบ static ถูกยกเลิก → ใช้ _dynamic_setup_limit() แทน (dynamic ตาม urgency RDD)


# =========================

# LOAD DATA

# =========================

orders = pd.read_excel(ORDER_FILE)
summary_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="SUMMARY_MC_REMAIN")
detail_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="DETAIL")  # โหลด DETAIL
item_cap_data = load_item_cap_data()
master_mc = pd.read_excel(BASE_DIR / "data" / "MC" / "Master_MC_5.xlsx")
calendar = load_calendar(CALENDAR_FILE, sheet_name="Sheet1")
calendar_week = calendar_week_map(calendar)
orders.columns = orders.columns.str.strip()
summary_mc.columns = summary_mc.columns.str.strip().str.upper()
calendar_week.columns = calendar_week.columns.str.strip().str.upper()
item_cap_data.columns = item_cap_data.columns.str.strip()
detail_mc.columns = detail_mc.columns.str.strip().str.upper()  # เพิ่ม detail_mc
master_mc.columns = master_mc.columns.str.strip()

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
# FACTORY TYPE CONFIGURATION
# =========================
# สร้าง Factory Type mapping จาก Master_MC_5.xlsx
FACTORY_TYPE_MAP = {}
FACTORY_WORKING_DAYS_MAP = {}
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
TODAY = pd.Timestamp.today().normalize()


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


def get_working_days_by_factory(mc_group, available_machines_count):
    """คืนค่าจำนวนวันทำงานของโรงงานตาม MC_GROUP"""
    # หาวันทำงานจาก FACTORY_WORKING_DAYS_MAP
    working_days = FACTORY_WORKING_DAYS_MAP.get(mc_group, 6)  # default = 6 วัน
    return working_days


def _dynamic_setup_limit(
    plan_week: int, fg_week_int, required_mc: int, remaining_job_slots: int
) -> int:
    """คืนจำนวน new machines สูงสุดที่ควร setup ใน week นี้ ตาม urgency ของ RDD
    - ห่าง RDD >= 2 week : ใช้แค่ required_mc  (ประหยัด job slot ไว้ให้ order อื่น)
    - ห่าง RDD == 1 week : ใช้เต็ม remaining_job_slots (เร่งให้ทัน)
    - plan_week >= RDD   : ไม่มี cap เลย  (urgent, ใช้ทุก slot ที่เหลือ)
    ทุก case ยังต้องผ่าน check_job_capacity_limit อีกรอบเสมอ"""
    fallback = remaining_job_slots  # ถ้าไม่มีข้อมูลให้ใช้เต็มที่
    if not required_mc:
        required_mc = fallback
    if fg_week_int is None:
        # ไม่มี RDD → conservative = required_mc เท่านั้น
        return required_mc
    weeks_to_rdd = fg_week_int - plan_week
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
    """Get working days for a specific week from calendar"""
    week_data = calendar_week[calendar_week["WEEK"] == week]
    if week_data.empty:
        return []
    week_start = week_data.iloc[0]["WEEK_START"]
    week_end = week_data.iloc[0]["WEEK_END"]
    # Get working days (ทำงาน 7 วัน)
    working_days = []
    current_date = week_start
    while current_date <= week_end:
        working_days.append(current_date)
        current_date += pd.Timedelta(days=1)
    return working_days


def get_actual_mc_remain(mc_group, week, gauge=None):
    """คืนค่าจำนวนเครื่องว่างจริง = TOTAL_MC_REMAIN จาก summary_mc หัก weekly_job_usage ที่จองไปแล้ว
    ป้องกันไม่ให้ order หลายตัวจองเครื่องเกินจำนวนจริง
    """
    mc_rows = summary_mc[
        (summary_mc["WEEK"] == week) & (summary_mc["MC_GROUP"] == mc_group)
    ]
    if mc_rows.empty:
        return 0
    # กรอง GUAGE ถ้าระบุ
    if gauge is not None:
        mc_rows = mc_rows[
            mc_rows["GUAGE"].astype(str).str.contains(str(gauge), na=False)
        ]
        if mc_rows.empty:
            return 0
    # เครื่องว่างจาก summary_mc (ค่าเริ่มต้น)
    # TOTAL_MC_REMAIN = TOTAL_MC - MC_USE_CEIL (หักการจองเก่าออกแล้ว)
    base_remain = mc_rows[mc_rows["TOTAL_MC_REMAIN"] > 0]["TOTAL_MC_REMAIN"].sum()
    # หักเฉพาะ NEW plan ที่วางในรอบนี้ (ไม่หักซ้ำ booking เก่าที่หัก TOTAL_MC_REMAIN ไปแล้ว)
    already_used = weekly_new_plan_usage.get(week, {}).get(mc_group, 0)
    actual_remain = max(0, base_remain - already_used)
    return actual_remain


def calculate_required_machines(
    item_code, order_qty, start_week, fg_week, setup_days=SETUP_DAYS
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
        return None, None, None, None
    # เรียงลำดับตามความจุจากมากไปน้อย
    available_machines = available_machines.sort_values("CAP ทอ", ascending=False)
    # คำนวณจำนวนสัปดาห์ที่เหลือถึง RDD
    weeks_until_rdd = []
    current_week = start_week
    while current_week is not None and (fg_week is None or current_week <= fg_week):
        weeks_until_rdd.append(current_week)
        current_week = next_week(current_week)
    if not weeks_until_rdd:
        return None, None, None, None
    num_weeks = len(weeks_until_rdd)
    # ลองแต่ละ MC_GROUP ที่สามารถผลิตได้ (เรียงตามความจุจากมากไปน้อย)

    for _, machine_row in available_machines.iterrows():
        mc_group = machine_row["MC_GROUP"]
        daily_cap = machine_row["CAP ทอ"]

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
        if not has_any_machine:
            continue
        # ---- Setup-aware: ตรวจสอบว่าต้อง setup หรือไม่ ----
        key = (item_code, mc_group)
        setup_needed = True
        start_week_idx = week_index(start_week)

        if key in last_production:
            last_week_idx = last_production[key]
            if start_week_idx - last_week_idx <= SETUP_GAP_WEEK:
                setup_needed = False

        # เครื่องที่วิ่งอยู่แล้ว (carry-over จาก booking/old plan)
        # ถ้า setup_needed=False = เครื่องยังอุ่นอยู่ → ใช้เป็น committed_mc ตั้งต้น

        carryover_start = machines_in_use.get(key, 0) if not setup_needed else 0
        factory_wd = get_working_days_by_factory(mc_group, 1)
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
            for w_idx, week in enumerate(weeks_until_rdd):
                if qty_remaining <= 0:
                    break
                # ถ้า summary_mc ไม่มีข้อมูลในสัปดาห์นี้ แต่เครื่องกำลังวิ่งอยู่ (carry-over)
                # ให้เครื่องเดิมยังคงผลิตต่อได้ (ไม่ต้องมีข้อมูลใน summary_mc)
                avail_this_week = avail_per_week[w_idx]
                if avail_this_week <= 0 and committed_mc > 0:
                    avail_this_week = committed_mc  # carry-over เท่านั้น ไม่เพิ่มเครื่องใหม่
                # จำนวนเครื่องที่ต้องการใน week นี้ (ไม่เกิน availability)
                want_mc = min(n_mc, avail_this_week)
                if want_mc <= 0:
                    actual_use_list.append(0)
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
                    prod_days_new = max(0, factory_wd - setup_days)
                elif new_added > 0 and (setup_needed or committed_mc > 0):
                    # มีเครื่องเพิ่มใหม่นอกเหนือจาก carryover → เฉพาะเครื่องใหม่ต้อง setup
                    # (ถ้า setup_needed=False และ committed_mc=0 = cold-warm: ไม่ setup)
                    setup_mc = new_added if (setup_needed or committed_mc > 0) else 0
                    prod_days_carry = factory_wd
                    prod_days_new = (
                        max(0, factory_wd - setup_days) if setup_mc > 0 else factory_wd
                    )
                else:
                    # carry-over ล้วน หรือ warm start (setup_needed=False, committed_mc=0)
                    setup_mc = 0
                    prod_days_carry = factory_wd
                    prod_days_new = factory_wd  # warm → ผลิตเต็มสัปดาห์
                total_setup_mc_days += setup_mc * setup_days
                committed_mc = want_mc  # อัปเดตเครื่องที่ใช้จริง
                weeks_needed += 1
                actual_use_list.append(want_mc)
                week_production = (
                    carryover * prod_days_carry + new_added * prod_days_new
                ) * daily_cap
                qty_remaining -= week_production
            finished = qty_remaining <= 0
            setup_waste = total_setup_mc_days  # mc-days ที่เสียไปกับ setup
            # คำนวณ efficiency
            total_machine_days = sum(
                mc * factory_wd for mc in actual_use_list if mc > 0
            )
            productive_days = max(0, total_machine_days - setup_waste)
            efficiency = (
                (productive_days / total_machine_days * 100)
                if total_machine_days > 0
                else 0
            )
            if finished:
                # พบจำนวนเครื่องน้อยสุดที่ทัน RDD แล้ว → หยุดทันที
                best_option = (n_mc, weeks_needed, total_setup_mc_days, efficiency)
                break

        if best_option:
            required_machines = best_option[0]
            return mc_group, daily_cap, required_machines, None
        else:
            # ไม่ทันทุก option → ใช้เครื่องเต็มที่
            required_machines = int(max_try)
            return mc_group, daily_cap, required_machines, None
    return None, None, None, None


def get_best_machine_for_item(
    item_code,
    plan_week,
    last_production,
    required_machines_info=None,
    urgent_mode=False,
    past_rdd=False,
):
    """เลือกเครื่องที่เหมาะสมที่สุดสำหรับ item นี้ (ใช้ get_actual_mc_remain หักเครื่องที่จองไปแล้ว)"""

    # ถ้ามีการคำนวณจำนวนเครื่องที่ต้องการมาแล้ว ให้ใช้ค่านั้น
    if required_machines_info is not None:
        mc_group, daily_cap, required_machines = required_machines_info

        if mc_group and required_machines > 0:
            # หา GUAGE ของ item นี้
            item_machine_info = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item_code)
                & (item_cap_data["MC_GROUP"] == mc_group)
            ]
            item_gauge = (
                item_machine_info.iloc[0]["GUAGE"]
                if not item_machine_info.empty
                else None
            )
            # ดูเครื่องว่างจริง (หักที่จองไปแล้ว)
            actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
            # ตรวจสอบว่าเคยผลิต item นี้ใน week ก่อน (= เครื่องเดิม carry over)
            key = (item_code, mc_group)
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
                if same_sc and key not in new_plan_started_items:
                    is_continuing = True
                    setup_needed = False
                else:
                    # Optionally allow carryover across different SC/SO if configured
                    if ALLOW_CARRYOVER_ACROSS_SO:
                        last_sc = last_sc_so_no.get(key)
                        prev_m = machines_in_use.get(key, 0)
                        last_idx = last_production.get(key)
                        if (
                            last_sc
                            and prev_m > 0
                            and key not in new_plan_started_items
                            and last_idx is not None
                            and current_week_idx - last_idx <= SETUP_GAP_WEEK
                        ):
                            is_continuing = True
                            setup_needed = False

            if is_continuing:

                # เครื่องเดิมจาก week ก่อน carry over โดยไม่ต้องเช็ค actual_remain
                # ใช้ fallback=0 ตรงกับ main loop เพื่อป้องกัน committed_carryover ผิด
                prev_mc = machines_in_use.get((item_code, mc_group), 0)
                carryover = prev_mc  # เครื่องทั้งหมดจาก week ก่อนวิ่งต่อได้เลย

                # เพิ่มเครื่องใหม่ได้ถ้า:
                # - ก่อน RDD: เพิ่มได้ถ้ายังไม่ครบ required_machines
                # - หลัง RDD: เพิ่มได้เต็มที่ตาม actual_remain
                extra_avail = max(0, actual_remain)

                if past_rdd:
                    new_additions = extra_avail
                else:
                    # เพิ่มได้ถ้ายังไม่ถึง required_machines
                    can_add = max(0, required_machines - carryover)
                    new_additions = min(extra_avail, can_add)
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
                return mc_group, daily_cap, setup_needed, available_machines_count
            if actual_remain > 0:
                # ถ้า past_rdd ให้ใช้เครื่องว่างจริงทั้งหมด ไม่ cap ที่ required_machines
                if past_rdd:
                    available_machines_count = actual_remain
                else:
                    available_machines_count = min(required_machines, actual_remain)
                type_used = get_type_used_jobs(plan_week, mc_group)
                available_machines_count = check_job_capacity_limit(
                    mc_group, available_machines_count, urgent_mode, type_used
                )
                if available_machines_count <= 0:
                    return None, None, None, None
                return mc_group, daily_cap, setup_needed, available_machines_count
    # เครื่องสำรอง: ใช้ logic เดิมถ้าไม่มีการคำนวณล่วงหน้า
    available_machines = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    if available_machines.empty:
        return None, None, None, None
    available_machines = available_machines.sort_values("CAP ทอ", ascending=False)
    current_week_idx = week_index(plan_week)

    # 1. ลองเครื่องที่ว่างในสัปดาห์นี้ก่อน

    for _, machine_row in available_machines.iterrows():
        mc_group = machine_row["MC_GROUP"]
        daily_cap = machine_row["CAP ทอ"]
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
            key = (item_code, mc_group)
            setup_needed = True
            if key in last_production:
                last_week_idx = last_production[key]
                if current_week_idx - last_week_idx <= SETUP_GAP_WEEK:
                    setup_needed = False
            return mc_group, daily_cap, setup_needed, available_machines_count

    # 2. ถ้าไม่มีเครื่องว่าง ลอง MC ที่เคยผลิต item เดียวกัน
    previous_mcs = [key[1] for key in last_production if key[0] == item_code]
    for prev_mc in previous_mcs:
        prev_mc_row = available_machines[available_machines["MC_GROUP"] == prev_mc]
        if not prev_mc_row.empty:
            mc_group = prev_mc
            daily_cap = prev_mc_row.iloc[0]["CAP ทอ"]
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
                return mc_group, daily_cap, setup_needed, available_machines_count
    return None, None, None, None


def next_week(week):
    idx = week_index(week)
    if idx is None or idx + 1 >= len(calendar_week):
        return None
    return int(calendar_week.iloc[idx + 1]["WEEK"])


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


# =========================
# LOAD OLD PRODUCTION PLAN FOR VALIDATION
# =========================
try:
    old_plan_df = pd.read_excel(OUTPUT_FILE)
    print(f"📋 โหลดแผนการผลิตเก่าสำหรับ validation: {len(old_plan_df)} แผน")
except FileNotFoundError:
    print("📋 ไม่พบแผนการผลิตเก่า")
    old_plan_df = pd.DataFrame()
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
    booking_raw["KNIT WEIGHT"] = pd.to_numeric(
        booking_raw.get("KNIT WEIGHT"), errors="coerce"
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
        _key = (_bi, _bm)
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
    # ใช้เฉพาะ week ที่ผ่านมาแล้ว (≤ TODAY_WEEK) เพื่อตัดสิน setup
    # week อนาคจาก booking plan ไม่ควรนับว่า "เพิ่งผลิต" → จะทำให้ setup_needed = False ผิดพลาด
    if plan_week > TODAY_WEEK:
        continue
    w_idx = week_index(plan_week)
    if w_idx is None:
        continue
    key = (item_code, mc_group)
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
            _row.get("SC/SO NO")
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
        # ใช้เฉพาะ week ที่ผ่านมาแล้ว (≤ TODAY_WEEK)
        if plan_week > TODAY_WEEK:
            continue
        w_idx = week_index(plan_week)
        if w_idx is None:
            continue
        key = (item_code, mc_group)
        # เติมเฉพาะถ้ายังไม่มีข้อมูลหรือข้อมูลจาก old plan ใหม่กว่า detail_mc ที่มีอยู่
        if key not in last_production or w_idx > last_production.get(key, -1):
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
            if _wk < TODAY_WEEK:
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

# =========================
# MERGE SAME SC + SAME ITEM
# =========================
# ถ้า SC/SO NO เหมือนกันและ Item Code เหมือนกัน → รวมเป็น 1 row ผลิตทีเดียว

orders["Pending Plan"] = pd.to_numeric(
    orders["Pending Plan"] if "Pending Plan" in orders.columns else 0, errors="coerce"
).fillna(0)
_grp_keys = ["SC/SO NO", "Item Code", "MC GROUP", "MC_GUAGE"]
_sum_cols = [
    c
    for c in ["Orders.Qty", "Plan Qty", "Pending Plan", "Confirm"]
    if c in orders.columns
]
_min_cols = [c for c in ["FG Week", "YARN_DYE_FINISH_DATE"] if c in orders.columns]
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

# เรียง orders ตาม FG Week (RDD) โดยเรียงจากน้อยไปมาก (urgent ก่อน)
orders_sorted = orders.sort_values("FG Week", na_position="last")
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

    # Special rule: if Cust.PO NO == "RTS" and CENTER == "LOCAL" -> schedule only after last OLD
    # and do NOT increase machine count (use existing machines only)
    rts_local_force = None
    # Use exact column 'Cust.PO NO' only: if Cust.PO NO == 'RTS' then enforce continue-from-old
    if "Cust.PO NO" in orders.columns:
        cust_val = str(order.get("Cust.PO NO", "")).strip().upper()
        # Apply RTS/local rule for RTS or CENTER LOCAL customers
        if cust_val in ("RTS", "CENTER LOCAL"):
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
                        try:
                            cap_row = item_cap_data[
                                (item_cap_data["ITEM_CODE"] == item)
                                & (item_cap_data["MC_GROUP"] == str(mc).strip().upper())
                            ]
                            if not cap_row.empty:
                                daily_cap_by_mc[str(mc).strip().upper()] = cap_row.iloc[
                                    0
                                ].get("CAP ทอ", None)
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
    current_week = TODAY_WEEK
    fg_week_int = None
    if pd.notna(fg_week):
        fg_week_str = str(int(fg_week))
        if len(fg_week_str) == 6:  # รูปแบบ YYYYWW (เช่น 202613)
            fg_year = int(fg_week_str[:4])
            fg_week_num = int(fg_week_str[4:])
            fg_week_int = fg_week_num
        elif len(fg_week_str) == 5:  # รูปแบบ YYYYW (เช่น 20265)
            fg_year = int(fg_week_str[:4])
            fg_week_num = int(fg_week_str[4:])
            fg_week_int = fg_week_num
        elif len(fg_week_str) <= 2:  # รูปแบบ WW (เช่น 13)
            fg_week_int = int(fg_week_str)
        else:
            fg_week_int = int(fg_week)  # ใช้ค่าเดิมถ้าเป็นรูปแบบอื่น
    # หัก 3 สัปดาห์ (RDD = FG Week - 3)
    if fg_week_int is not None:
        fg_week_int = fg_week_int - 3
    # LAB-DIP: deadline คือ FG week + 2 (ต้องเสร็จก่อน SC ตัวหลักจะทำ)
    if order_type == "LAB-DIP" and fg_week_int:
        lab_deadline = fg_week_int
        for _ in range(2):
            nw = next_week(lab_deadline)
            if nw:
                lab_deadline = nw
        fg_week_int = lab_deadline
    if fg_week_int and fg_week_int < current_week:
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
        # LAB-DIP: 0 week - can start immediately from planning date (TODAY)
        order_week = TODAY_WEEK
    elif order_type == "SC-ORDERS":
        # SC-ORDERS: +2 weeks from planning date (TODAY)
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

    plan_week = calendar_week.iloc[start_idx]["WEEK"]

    # ----------------------

    # weekly allocation with best machine selection

    # ----------------------

    # คำนวณจำนวนเครื่องที่ต้องการตั้งแต่แรก (ถ้าทัน RDD)

    required_machines_info = None

    # คำนวณ setup days ล่วงหน้า (ใช้ใน calculate_required_machines ด้วย)

    order_fiber_type = get_fiber_type_for_item(item)

    order_setup_days = 5 if order_fiber_type == "POLY" else SETUP_DAYS

    if fg_week_int and fg_week_int >= plan_week:

        mc_group_calc, daily_cap_calc, required_machines, _ = (
            calculate_required_machines(
                item, qty_left, plan_week, fg_week_int, setup_days=order_setup_days
            )
        )

        if required_machines:
            required_machines_info = (mc_group_calc, daily_cap_calc, required_machines)

    while qty_left > 0 and plan_week is not None:

        # ⚠️ ตรวจสอบ RDD ก่อนว่าทันหรือไม่

        past_rdd = bool(fg_week_int and plan_week >= fg_week_int)

        if fg_week_int and plan_week > fg_week_int:
            urgent_mode = True

        # ถ้ายังไม่ได้คำนวณ required_machines (เพราะตอนแรก avail=0 ทุก week)
        # ให้ลองคำนวณใหม่ด้วย plan_week ปัจจุบันที่มีเครื่องว่างจริง
        if (
            required_machines_info is None
            and not past_rdd
            and fg_week_int
            and plan_week <= fg_week_int
        ):
            _mc_r, _cap_r, _req_r, _ = calculate_required_machines(
                item, qty_left, plan_week, fg_week_int, setup_days=order_setup_days
            )
            if _req_r:
                required_machines_info = (_mc_r, _cap_r, _req_r)

        # เลือกเครื่องที่เหมาะสมที่สุดสำหรับ item นี้
        # ถ้าเป็นกรณี RTS+LOCAL ให้บังคับใช้ MC เดิมและเริ่มหลัง old สุดท้าย
        mc_group = daily_capacity = setup_needed = available_machines = None
        # Do not force a single mc_group here. We'll enforce carryover after a mc_group is selected
        # using per-MC last-old data stored in rts_local_force.
        if rts_local_force:
            lg = rts_local_force.get("last_old_by_mc", {})
            if lg:
                min_start = min([next_week(w) for w in lg.values()])
                if plan_week is None or plan_week < min_start:
                    plan_week = min_start
        # Default: ให้ฟังก์ชันช่วยเลือกถ้ายังไม่กำหนด
        if mc_group is None:
            mc_group, daily_capacity, setup_needed, available_machines = (
                get_best_machine_for_item(
                    item,
                    plan_week,
                    last_production,
                    required_machines_info,
                    urgent_mode,
                    past_rdd,
                )
            )
        if mc_group is None:

            plan_week = next_week(plan_week)

            continue

        # Cap available_machines ตาม required_machines ที่คำนวณไว้

        # เพื่อไม่ให้ใช้เครื่องเกินที่วางแผน

        # แต่ถ้าถึงหรือผ่าน RDD แล้ว ให้ใช้เครื่องเต็มที่ไม่ cap

        if required_machines_info and not past_rdd:
            req_mc = required_machines_info[2]
            if available_machines > req_mc:
                available_machines = req_mc
        # Calculate available capacity considering setup days and factory type
        working_days = get_working_days_in_week(plan_week)
        factory_working_days = get_working_days_by_factory(mc_group, available_machines)
        # ใช้จำนวนวันทำงานที่น้อยกว่าระหว่าง calendar และ factory capacity
        actual_working_days = min(len(working_days), factory_working_days)
        # หา REVOLUTION/WEIGHT ที่มากที่สุด
        rev_weight = get_revolution_weight(item, mc_group, plan_week)
        # กำหนด setup days ตาม FIBER_TYPE (POLY = 5 วัน, อื่นๆ = 3 วัน)
        item_fiber_type = get_fiber_type_for_item(item)
        item_setup_days = 5 if item_fiber_type == "POLY" else SETUP_DAYS
        # ถ้าเป็น urgent หรือใกล้ RDD ให้ใช้ความสามารถสูงสุด
        if urgent_mode or (fg_week_int and plan_week >= fg_week_int - 1):
            # ใช้วันทำงานตามที่โรงงานกำหนด (ไม่เปลี่ยนแปลง)
            # urgent mode ไม่สามารถเพิ่มวันทำงานเกินที่โรงงานเปิดได้
            pass

        # ตรวจสอบว่าสัปดาห์นี้เคยใช้ setup ไปแล้วหรือไม่
        week_key = (plan_week, mc_group)
        factory_working_days = get_working_days_by_factory(mc_group, available_machines)
        # แยกเครื่อง carry-over (ไม่ต้อง setup) vs เครื่องใหม่ (ต้อง setup)
        mc_key = (item, mc_group)
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
                last_sc = last_sc_so_no.get(mc_key)
                prev_m = machines_in_use.get(mc_key, 0)
                prev_week_idx = last_production.get(mc_key)
                if (
                    last_sc
                    and prev_m > 0
                    and mc_key not in new_plan_started_items
                    and prev_week_idx is not None
                    and current_week_idx is not None
                    and (current_week_idx - prev_week_idx) <= SETUP_GAP_WEEK
                ):
                    same_order = True

        is_continuing = (
            prev_week_idx is not None
            and current_week_idx is not None
            and same_order
            and current_week_idx
            > prev_week_idx  # ห้ามนับถ้า plan week ก่อน/เท่ากับ booking week
            and (
                current_week_idx - prev_week_idx == 1  # week ติดกัน (new plan)
                or (
                    mc_key
                    not in new_plan_started_items  # carry-over จาก old plan (item ยังไม่ได้เริ่ม new plan)
                    and current_week_idx - prev_week_idx <= SETUP_GAP_WEEK
                )
            )
        )  # ต้องไม่ห่างเกิน gap
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

        # ===== Dynamic setup limit ตาม urgency RDD (แนวทาง B) =====
        # - ห่าง RDD >= 2 week → cap = required_mc (ประหยัด slot สำหรับ order อื่น)
        # - ห่าง RDD == 1 week → cap = remaining_job_slots (เต็มที่)
        # - past RDD           → ไม่มี cap (urgent)
        _remaining_slots = get_remaining_job_slots(plan_week, mc_group)
        _req_mc_dyn = required_machines_info[2] if required_machines_info else new_mc
        _dyn_limit = _dynamic_setup_limit(
            plan_week, fg_week_int, _req_mc_dyn, _remaining_slots
        )

        if new_mc > _dyn_limit:
            new_mc = _dyn_limit
        available_machines = carryover_mc + new_mc
        prod_days_old = factory_working_days  # เครื่อง carry-over ผลิตเต็มสัปดาห์
        # เครื่องใหม่ (new_mc) ต้อง setup เสมอ แม้ item จะ warm บนเครื่องเดิม (setup_needed=False)
        # setup_needed=False หมายถึงเครื่องที่วิ่งอยู่แล้ว ไม่ใช่เครื่องที่เพิ่งเพิ่มมา
        prod_days_new = max(0, factory_working_days - item_setup_days)

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
        _allowed_new = check_job_capacity_limit(mc_group, new_mc, False, _type_used_now)
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

        # คำนวณ PRODUCE_QTY ตามสูตรที่แม่นยำ
        if rev_weight is not None and rev_weight > 0:
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
        prev_week_mc = machines_in_use.get((item, mc_group), available_machines)
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
                    mc_group, available_machines
                ),
                "CALENDAR_WORKING_DAYS": len(get_working_days_in_week(plan_week)),
                "ACTUAL_WORKING_DAYS": min(
                    len(get_working_days_in_week(plan_week)),
                    get_working_days_by_factory(mc_group, available_machines),
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
                "RDD_WEEK": fg_week_int,
                "FIBER_TYPE": get_fiber_type_for_item(item),
            }
        )
        qty_left -= produce
        if qty_left <= 0:
            qty_left = 0  # ป้องกันค่าติดลบ
        last_production[(item, mc_group)] = week_index(plan_week)
        machines_in_use[(item, mc_group)] = available_machines  # บันทึกจำนวนเครื่องที่ใช้จริง
        last_sc_so_no[(item, mc_group)] = sc_so_no  # บันทึก SC/SO NO ล่าสุดที่ผลิต
        new_plan_started_items.add((item, mc_group))  # บันทึกว่า item นี้เริ่ม new plan แล้ว
        # อัพเดท job usage สำหรับสัปดาห์นี้ (นับเฉพาะ new_mc = machines ที่ setup ใหม่)
        if plan_week not in weekly_job_usage:
            weekly_job_usage[plan_week] = {}
        weekly_job_usage[plan_week][mc_group] = (
            weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
        )

        # อัพเดท new plan usage (นับทั้ง carryover+new สำหรับ get_actual_mc_remain)
        if plan_week not in weekly_new_plan_usage:
            weekly_new_plan_usage[plan_week] = {}
        weekly_new_plan_usage[plan_week][mc_group] = (
            weekly_new_plan_usage[plan_week].get(mc_group, 0) + available_machines
        )
        # ก้าวไป week ถัดไปเสมอหลัง produce (ห้าม plan item เดิมใน week เดิมซ้ำ)
        _produced_week = plan_week
        plan_week = next_week(plan_week)
        # ตรวจสอบว่าแผนใหม่ไม่เกิน capacity ที่เหลือหลังจากแผนเก่า
        if not old_plan_df.empty:
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
        week_sum = new_summary[new_summary["PLAN_WEEK"] == week]["REQUIRED_MC"].sum()
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
print("Weekly production planning completed")
print(f"Output: {OUTPUT_FILE}")
print(f"Total rows: {len(plan_df)}")

# =========================
# EXPORT COMBINED (OLD + NEW)
# =========================
COMBINED_FILE = DATA_PLAN_DIR / "weekly_production_plan_combined_filtered.xlsx"
new_df = plan_df.copy()
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
    if "RDD_WEEK" not in common_cols and "RDD_WEEK" in new_df.columns:
        common_cols.append("RDD_WEEK")
        old_booking_df["RDD_WEEK"] = None
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
combined_df.to_excel(COMBINED_FILE, index=False)
print(f"Combined (OLD+NEW): {COMBINED_FILE}")
print(f"  OLD rows: {len(old_booking_df) if not old_booking_df.empty else 0}")
print(f"  NEW rows: {len(plan_df)}")

import pandas as pd
import io
import re
import sys
from datetime import date
from pathlib import Path
from Calendar import load_calendar, calendar_week_map
from ITEM_Cap import load_item_cap_data
from Yarn_Master import load_yarn_master

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).parent
TODAY = pd.Timestamp.today().normalize()
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
OUTPUT_FILE = DATA_PLAN_DIR / "weekly_production_plan2.xlsx"
SETUP_DAYS = 3
SETUP_GAP_WEEK = 2
# Week ที่ไม่ต้องการวางแผน (เช่น สัปดาห์หยุด/ปิดโรงงาน)
SKIP_WEEKS = {16}
# Allow carryover even when SC/SO changes (user option)
ALLOW_CARRYOVER_ACROSS_SO = False
# อนุญาต carry เฉพาะ FG ถัดไปของ item เดียวกันภายในสัปดาห์เดียวกัน
ALLOW_SAME_ITEM_WEEK_CARRY = True
# MC_GROUP redirect: เมื่อเลือก MC_GROUP+GAUGE นี้ให้เปลี่ยนไปใช้ MC_GROUP+GAUGE ใหม่แทนเสมอ
# SKP gauge-20 → FA gauge-20 เพราะ FA (อ้อมน้อย) รับผลิตแทน SKP gauge-20
# key = (mc_group, gauge_str), value = (new_mc_group, new_gauge_str)
MC_GROUP_REDIRECT = {
    ("SKP", "20"): ("FA", "20"),
}
# Progressive machine reduction: เริ่มต้นด้วยเครื่องเยอะ แล้วค่อยๆ ลดลงให้ทัน target
USE_PROGRESSIVE_REDUCTION = False
# MAX_SETUP_MC แบบ static ถูกยกเลิก → ใช้ _dynamic_setup_limit() แทน (dynamic ตาม urgency RDD)
# จำนวนเครื่อง *ใหม่* (new setup) สูงสุดต่อ item/week — carry-over ไม่นับ
MAX_NEW_SETUP_MC = 2
# Load Balancing Configuration
USE_LOAD_BALANCING = True  # เปิดใช้งาน Load Balancing สำหรับการกระจายงาน
LOAD_BALANCING_WEIGHT = 0.1  # น้ำหนักสำหรับการพิจารณาเครื่องว่างใน load balancing score
LOAD_BALANCING_THRESHOLD = 0.2  # เกณฑ์ที่ใช้พิจารณาว่าโหลดไม่สมดุล (20% จากค่าเฉลี่ย)

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
# Map column names from order_ready.xlsx to expected names
column_mapping = {
    'SC_NO': 'SC/SO NO',
    'FABRIC_ITEM': 'Item Code', 
    'ORDERS_QTY': 'Orders.Qty',
    'PLAN_QTY': 'Plan Qty',
    'PENDING_PLAN': 'Pending Plan',
    'FGWEEK': 'FG Week',
    'CUSTOMER_NAME': 'Customer',
    'ORDER_TYPE': 'Orders Type'
}
orders = orders.rename(columns=column_mapping)
summary_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="SUMMARY_MC_REMAIN")
detail_mc = pd.read_excel(MC_REMAIN_FILE, sheet_name="DETAIL")  # โหลด DETAIL
item_cap_data = pd.DataFrame(columns=["ITEM_CODE", "MC_GROUP", "CAP ทอ", "REVOLUTION/WEIGHT", "GUAGE"])
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


def _apply_mc_redirect(mc_group: str, gauge) -> tuple:
    """ถ้า (mc_group, gauge) อยู่ใน MC_GROUP_REDIRECT ให้เปลี่ยนไปใช้ค่าใหม่แทนเสมอ
    เช่น SKP gauge-20 → FA gauge-20 (อ้อมน้อย)
    คืนค่า (new_mc_group, new_gauge_str)
    """
    gauge_str = _normalize_gauge(gauge)
    key = (str(mc_group).strip().upper() if mc_group else "", gauge_str)
    if key in MC_GROUP_REDIRECT:
        return MC_GROUP_REDIRECT[key]
    return mc_group, gauge_str

for _, _r in item_cap_data.iterrows():
    _ic = str(_r.get("ITEM_CODE", "")).strip().upper()
    _mc = str(_r.get("MC_GROUP", "")).strip().upper()
    _gg = _r.get("GUAGE")
    _gs = _normalize_gauge(_gg)
    if _ic and _mc and _gs and _gs.lower() != "nan":
        _item_mc_to_gauge[(_ic, _mc)] = _gs


def _ck(item, mc_group, gauge=None):
    """สร้าง carryover key: (item, mc_group, gauge) — match ITEM+MC_GROUP+GUAGE"""
    item_norm = str(item).strip().upper() if item is not None else ""
    mc_norm = str(mc_group).strip().upper() if mc_group is not None else ""
    g = _normalize_gauge(gauge)
    if not g:
        g = _normalize_gauge(
            _item_mc_to_gauge.get(
                (item_norm, mc_norm), ""
            )
        )
    return (item_norm, mc_norm, g)


def _resolve_carry_key(item, mc_group, gauge=None):
    """คืน key ที่เหมาะสุดสำหรับ carryover โดย fallback เป็น item+mc แม้ gauge ไม่ตรง"""
    base_key = _ck(item, mc_group, gauge)
    if (
        base_key in last_production
        or base_key in machines_in_use
        or base_key in last_sc_so_no
    ):
        return base_key

    item_norm, mc_norm, _ = base_key
    best_key = None
    best_score = (-1, -1)
    for cand in set(list(last_production.keys()) + list(machines_in_use.keys())):
        if len(cand) < 2:
            continue
        if cand[0] != item_norm or cand[1] != mc_norm:
            continue
        cand_last = int(last_production.get(cand, -1))
        cand_mc = int(machines_in_use.get(cand, 0) or 0)
        score = (cand_last, cand_mc)
        if score > best_score:
            best_score = score
            best_key = cand

    return best_key if best_key is not None else base_key


def _has_item_mc_key(key_set, key):
    """เช็คว่ามี key ของ item+mc อยู่ใน set แล้วหรือไม่ (ไม่สน gauge)"""
    if not key or len(key) < 2:
        return False
    item_norm, mc_norm = key[0], key[1]
    for cand in key_set:
        if len(cand) < 2:
            continue
        if cand[0] == item_norm and cand[1] == mc_norm:
            return True
    return False

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

# Fallback: เติม item ที่ไม่มีใน detail_mc จาก order_ready (YARN_ITEM)
try:
    _order_ready_df = pd.read_excel(ORDER_FILE)
    _order_ready_df.columns = _order_ready_df.columns.str.strip()
    _yi_col = next((c for c in _order_ready_df.columns if c.strip().upper() == "YARN_ITEM"), None)
    _ic_col = next((c for c in _order_ready_df.columns if c.strip().upper() in ("FABRIC_ITEM", "ITEM_CODE")), None)
    if _yi_col and _ic_col:
        for _, _or in _order_ready_df[[_ic_col, _yi_col]].dropna().drop_duplicates(_ic_col).iterrows():
            _ic = str(_or[_ic_col]).strip().upper()
            _yi = str(_or[_yi_col]).strip()
            if _ic not in _yarn_used_lookup and _yi and _yi.upper() != "NAN":
                _yarn_used_lookup[_ic] = _yi
except Exception as _e:
    print(f"⚠️ ไม่สามารถโหลด order_ready สำหรับ YARN-USED fallback: {_e}")

# =========================
# LT_YARN LEAD TIME LOOKUP
# =========================
_LT_YARN_FILE = DATA_DIR / "LT_Yarn" / "fmit_yarn_leadtime.xlsx"
try:
    _lt_yarn_df = pd.read_excel(_LT_YARN_FILE)
    _lt_yarn_df.columns = _lt_yarn_df.columns.str.strip()
    _lt_yarn_df["Yarn Item"] = _lt_yarn_df["Yarn Item"].astype(str).str.strip().str.upper()
    _lt_yarn_df["POIN LT"] = pd.to_numeric(_lt_yarn_df["POIN LT"], errors="coerce")
    _lt_yarn_df["Piority"] = pd.to_numeric(_lt_yarn_df["Piority"], errors="coerce")
    print(f"[LT_YARN] โหลด fmit_yarn_leadtime สำเร็จ: {len(_lt_yarn_df)} rows")
except Exception as _e:
    print(f"⚠️ ไม่สามารถโหลด LT_Yarn: {_e}")
    _lt_yarn_df = pd.DataFrame()


def _resolve_yarn_code(yarn_code: str) -> str:
    """
    แปลง yarn code ที่อาจไม่ตรงกับ LT file ให้ตรงขึ้น
    Rules (ลองตามลำดับ จนกว่าจะ match):
    1. ชื่อเดิม (exact)
    2. Strip YD ก่อนตัวเลข: CMYD→CM, DTYYD→DTY, BCIYD→BCI ฯลฯ
    3. Strip trailing single uppercase color suffix (D/S/L/W/M/H)
    4. ทั้ง rule 2 และ 3 รวมกัน
    5. แปลง RPEM → RPE ท้ายชื่อ
    """
    if _lt_yarn_df.empty:
        return yarn_code
    lt_set = set(_lt_yarn_df["Yarn Item"].dropna().unique())
    if yarn_code in lt_set:
        return yarn_code
    # rule 2: strip YD before digit
    s2 = re.sub(r"YD(?=\d)", "", yarn_code)
    if s2 != yarn_code and s2 in lt_set:
        return s2
    # rule 3: strip trailing single uppercase letter
    s3 = re.sub(r"[A-Z]$", "", yarn_code)
    if s3 != yarn_code and s3 in lt_set:
        return s3
    # rule 4: rule2 + rule3
    s4 = re.sub(r"[A-Z]$", "", s2)
    if s4 != s2 and s4 in lt_set:
        return s4
    # rule 5: RPEM → RPE
    s5 = re.sub(r"RPEM$", "RPE", yarn_code)
    if s5 != yarn_code and s5 in lt_set:
        return s5
    s5b = re.sub(r"RPEM$", "RPE", s2)
    if s5b != s2 and s5b in lt_set:
        return s5b
    # no match found — return original (will result in empty lookup)
    return yarn_code


def get_yarn_lt_days(item_code: str) -> int:
    """
    หา Lead Time (วัน) สูงสุดจาก YARN-USED ของ item นั้น
    Logic:
    - แยก YARN-USED ด้วย '+' → ได้ yarn codes A, B, C, …
    - resolve ชื่อ yarn ให้ตรงกับ LT file
    - สำหรับแต่ละ yarn code:
        * ถ้ามี Piority=1 rows → ใช้ max POIN LT จาก Piority=1 เท่านั้น
        * ถ้าไม่มี Piority=1 → fallback ใช้ max POIN LT จากทุก row
    - เปรียบเทียบค่าตัวแทนของแต่ละ yarn แล้วเลือกค่ามากสุด
    - คืน 0 ถ้าหาไม่พบ
    """
    if _lt_yarn_df.empty:
        return 0
    yarn_used = _yarn_used_lookup.get(str(item_code).strip().upper(), "")
    if not yarn_used or str(yarn_used).strip().upper() in ("", "NAN"):
        return 0
    parts = [p.strip().upper() for p in str(yarn_used).split("+") if p.strip()]
    if not parts:
        return 0
    resolved = [_resolve_yarn_code(p) for p in parts]
    representative_lts = []
    for yarn in resolved:
        yarn_rows = _lt_yarn_df[_lt_yarn_df["Yarn Item"] == yarn]
        if yarn_rows.empty:
            continue
        p1_rows = yarn_rows[yarn_rows["Piority"] == 1]
        if not p1_rows.empty:
            rep_lt = p1_rows["POIN LT"].max()
        else:
            rep_lt = yarn_rows["POIN LT"].max()
        if pd.notna(rep_lt):
            representative_lts.append(rep_lt)
    if not representative_lts:
        return 0
    return int(max(representative_lts))


def get_yarn_lt_earliest_week(item_code: str, date_in=None):
    """
    คืนค่า week number ที่เริ่มวางแผนได้เร็วสุด
    สูตร: max(DATE_IN + POIN_LT, TODAY + 2 weeks)
    คืน None ถ้าคำนวณไม่ได้
    """
    min_start_idx = get_yarn_lt_min_start_idx(item_code, date_in=date_in)
    if min_start_idx < len(calendar_week):
        return int(calendar_week.iloc[min_start_idx]["WEEK"])
    return None


def get_yarn_lt_min_start_idx(item_code: str, date_in=None) -> int:
    """
    คำนวณ minimum start index (row index ใน calendar_week) โดยพิจารณา Yarn LT
    สูตร: DATE_IN + POIN_LT days → ใช้ Thu-Wed boundary หา week ที่ yarn มาถึง
             → วางแผนได้ Thu-Wed week ถัดไป (เริ่มวันศุกร์แรกหลังพฤหัสที่ yarn มาถึง)
    ถ้าไม่มี DATE_IN จะใช้ TODAY แทน
    แต่ต้องไม่เร็วกว่า TODAY_IDX + 2 weeks เสมอ
    """
    min_two_weeks_idx = TODAY_IDX + 2
    lt_days = get_yarn_lt_days(item_code)
    if lt_days <= 0:
        return min_two_weeks_idx
    base_date = pd.to_datetime(date_in, errors="coerce") if date_in is not None else pd.NaT
    if pd.isna(base_date):
        base_date = TODAY
    earliest_date = base_date + pd.Timedelta(days=lt_days)
    # --- Thu-Wed week boundary (calendar plan ใช้ Fri-Thu) ---
    # Thu-Wed week: Thu=0, Fri=1, Sat=2, Sun=3, Mon=4, Tue=5, Wed=6
    # day_in_thu_wed=0 คือวันพฤหัส = เริ่ม week ใหม่ → ต้องวางแผนอีก 7 วัน
    # days_to_next_thu = จำนวนวันถึงพฤหัสถัดไป (เริ่ม Thu-Wed week ถัดไป)
    # +1 อีกวัน = ศุกร์ที่เริ่มต้น Fri-Thu week ใน calendar
    wd = earliest_date.weekday()             # 0=Mon … 3=Thu … 6=Sun
    day_in_thu_wed = (wd - 3) % 7           # Thu=0, Fri=1, …, Wed=6
    days_to_next_thu = 7 - day_in_thu_wed  # วันพฤหัสที่เริ่ม Thu-Wed week ถัดไป
    plan_start_date = earliest_date + pd.Timedelta(days=days_to_next_thu + 1)  # Thu+1 = Fri
    lt_plan_week = get_week_from_date(plan_start_date)
    if lt_plan_week is None:
        return min_two_weeks_idx
    lt_week_idx = week_index(lt_plan_week)
    if lt_week_idx is None:
        return min_two_weeks_idx
    result_idx = max(int(lt_week_idx), min_two_weeks_idx)
    print(f"[YARN LT] {item_code}: base={base_date.date()} POIN LT={lt_days} วัน"
          f" → yarn arrival={earliest_date.date()} (day_in_thu_wed={day_in_thu_wed})"
          f" → plan starts {plan_start_date.date()} (week {lt_plan_week} idx {result_idx}) → min_start_idx={result_idx}")
    return result_idx


def get_setup_days_for_item(item_code: str, material_content: str = None) -> int:
    """
    คำนวณ setup days ตาม MATERIAL_CONTENT และ YARN_ITEM
    
    Logic:
    0. ถ้า MATERIAL_CONTENT เป็น pure COTTON (ไม่มี CD/POLY) → 3 วัน (ไม่สนใจ YARN_ITEM)
    1. ถ้า MATERIAL_CONTENT เป็น POLY / CD / TC → 5 วัน
    2. ถ้าไม่มี MATERIAL_CONTENT → เช็ค YARN_ITEM: DTY หรือหลายเส้น → 5 วัน
    3. ถ้าไม่มีทั้งสองอย่าง → default 3 วัน
    """
    # 0. ตรวจสอบ MATERIAL_CONTENT ก่อน — pure COTTON → 3 วันเสมอ
    if material_content:
        material_upper = str(material_content).strip().upper()
        # pure COTTON เท่านั้น (ไม่มี CD, POLY, TC ผสม)
        if ("COTTON" in material_upper or "COT" in material_upper) and \
           "CD" not in material_upper and "POLY" not in material_upper and "TC" not in material_upper:
            return 3
        elif "POLY" in material_upper or "CD" in material_upper or "TC" in material_upper:
            return 5

    # 1. ถ้าไม่มี MATERIAL_CONTENT หรือไม่ตรงเงื่อนไข → ตรวจ YARN_ITEM
    try:
        master_item_df = pd.read_excel(BASE_DIR / "data" / "MC" / "DataITEM_Master.xlsx")
        item_row = master_item_df[master_item_df["ITEM_CODE"] == str(item_code).strip().upper()]
        if not item_row.empty:
            yarn_item = str(item_row.iloc[0]["YARN_ITEM"]).upper()
            
            # ถ้าเจอ DTY → 5 วัน
            if "DTY" in yarn_item:
                return 5
            
            # ตรวจสอบว่ามีหลายเส้น (มี + หรือ ,)
            if "+" in yarn_item or "," in yarn_item:
                return 5
    except Exception as e:
        print(f"⚠️ ไม่สามารถโหลด DataITEM_Master: {e}")
    
    return SETUP_DAYS


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


# สร้าง lookup: yarn ITEM_CODE → ITEM_DESC (uppercase) สำหรับ material detection
_yarn_desc_lookup = dict(zip(_yarn_df["ITEM_CODE"], _yarn_df["ITEM_DESC"].str.upper()))


def _get_material_content_from_yarn(item_code: str) -> str:
    """ดึง MATERIAL_CONTENT โดยดูจาก YARN-USED ของ item นั้น → เช็ค ITEM_DESC ของแต่ละ yarn code
    ผลลัพธ์: COTTON / POLY / CD / COTTON/CD / TC / หรือ '' ถ้าหาไม่ได้
    """
    yarn_used = _yarn_used_lookup.get(str(item_code).strip().upper(), "")
    if not yarn_used or str(yarn_used).strip().upper() in ("", "NAN"):
        return ""

    has_cotton = False
    has_poly = False
    has_cd = False

    parts = [p.strip() for p in str(yarn_used).split("+") if p.strip()]
    for part in parts:
        desc = _yarn_desc_lookup.get(part, "")
        if "COTTON" in desc:
            has_cotton = True
        if "POLYESTER" in desc or "DTY" in desc or "FDY" in desc:
            has_poly = True
        if "CD" in desc or "CATIONIC" in desc:
            has_cd = True

    if has_cd and has_cotton:
        return "COTTON/CD"
    elif has_cd:
        return "CD"
    elif has_cotton and has_poly:
        return "TC"
    elif has_cotton:
        return "COTTON"
    elif has_poly:
        return "POLY"
    return ""


def _normalize_capacity(item_code: str, mc_group: str, original_cap: float) -> float:
    """
    Normalize capacity to ensure consistency across MC_GROUPS
    แปลง CAP ทอ จากฐาน 24 ชั่วโมง เป็น 20 ชั่วโมง
    เพราะเครื่องทอทำงานจริง 20 ชั่วโมง/วัน
    """
    return original_cap * (20 / 24)


def get_load_balanced_machine(
    item_code,
    plan_week,
    last_production,
    required_machines_info=None,
    urgent_mode=False,
    past_rdd=False,
    force_max_mc=False,
    qty_left=0,
    daily_capacity=0,
    progressive_plan=None,
    current_machines=1,  # จำนวนเครื่องปัจจุบันสำหรับกรณี carryover
):
    """
    Load Balancing แบบ Gradual Increase พร้อม Validate:
    1. ใช้เครื่องที่วิ่งจริงเป็นฐาน (carryover baseline)
    2. เพิ่มเครื่อง setup ได้สูงสุด +2 จากฐาน
    3. Validate ทุกครั้งก่อนจองเครื่อง
    """
    # ถ้าปิด Load Balancing ให้ใช้ฟังก์ชันเดิม
    if not USE_LOAD_BALANCING:
        return get_best_machine_for_item(
            item_code,
            plan_week,
            last_production,
            required_machines_info,
            urgent_mode,
            past_rdd,
            force_max_mc,
        )
    # ใช้ logic เดิมในการหาเครื่องที่เหมาะสม
    mc_group, daily_cap, setup_needed, available_machines, item_gauge = get_best_machine_for_item(
        item_code,
        plan_week,
        last_production,
        required_machines_info,
        urgent_mode,
        past_rdd,
        force_max_mc,
    )
    
    if mc_group is None:
        return None, None, None, None, None

    # ฐานจำนวนเครื่องที่ใช้งานจริงในสัปดาห์นี้ (carryover) เพื่อใช้เป็น baseline +2
    # ALWAYS ใช้ current_machines จริง ถึงแม้ว่า setup_needed=True
    # เพราะ carry machines คือเครื่องที่วิ่งจริง ไม่ว่าจะมี gap หรือไม่
    carryover_base_machines = 0
    try:
        if current_machines is not None:
            carryover_base_machines = max(0, int(current_machines))
    except (ValueError, TypeError):
        carryover_base_machines = 0
    
    # Gradual Load Balancing Logic
    # แปลง plan_week เป็นตัวเลข (ถ้าเป็น "23" ให้ได้ 23, ถ้าเป็น "W23" ให้ได้ 23)
    if isinstance(plan_week, str):
        if plan_week.startswith('W'):
            current_week_num = int(plan_week[1:])
        else:
            current_week_num = int(plan_week)
    else:
        current_week_num = int(plan_week)
    
    # หาสัปดาห์ล่าสุดที่ item นี้ใช้งาน MC_GROUP นี้ (ไม่ใช่ MC_GROUP ทั้งหมด)
    # เช็คจาก plans ที่ append ไปแล้ว
    item_last_week = -1
    for plan_entry in plans:
        plan_item = plan_entry.get('ITEM_CODE', '')
        plan_mc = plan_entry.get('MC_GROUP', '')
        plan_week_value = plan_entry.get('PLAN_WEEK', '')

        if plan_item == item_code and plan_mc == mc_group:
            try:
                if isinstance(plan_week_value, str):
                    if plan_week_value.startswith('W'):
                        plan_week_num = int(plan_week_value[1:])
                    else:
                        plan_week_num = int(plan_week_value)
                else:
                    plan_week_num = int(plan_week_value)

                if plan_week_num < current_week_num and plan_week_num > item_last_week:
                    item_last_week = plan_week_num
            except (ValueError, TypeError):
                continue

    # เช็คจาก progressive_plan ด้วย (สำหรับ carryover items)
    # progressive_plan มี format: {week_str: machines}
    if progressive_plan:
        for week_str in progressive_plan.keys():
            try:
                if isinstance(week_str, str):
                    if week_str.startswith('W'):
                        prog_week_num = int(week_str[1:])
                    else:
                        prog_week_num = int(week_str)
                else:
                    prog_week_num = int(week_str)
                
                if prog_week_num < current_week_num and prog_week_num > item_last_week:
                    item_last_week = prog_week_num
            except (ValueError, TypeError):
                continue
    
    # หาจำนวนเครื่องรวมที่ใช้จริงในสัปดาห์ก่อนหน้าสำหรับ MC Group เดียวกัน
    # ใช้ weekly_mc_usage ที่เก็บ total machines per (week, mc_group)
    previous_week_mc_usage = {}
    for (week_idx, mc_grp), total_machines in weekly_mc_usage.items():
        # แปลง week index เป็นตัวเลขก่อนเปรียบเทียบ
        try:
            week_num = int(week_idx) if isinstance(week_idx, str) else week_idx
            
            if mc_grp == mc_group and week_num < current_week_num:
                # หาสัปดาห์ล่าสุดที่ใช้ MC Group นี้
                if week_num not in previous_week_mc_usage or week_num > previous_week_mc_usage.get('latest_week', -1):
                    previous_week_mc_usage['latest_week'] = week_num
                    previous_week_mc_usage['machines_used'] = total_machines
        except (ValueError, TypeError):
            # ข้ามถ้าไม่สามารถแปลง week index เป็นตัวเลขได้
            continue
    
    # คำนวณจำนวนเครื่องที่ควรใช้ในสัปดาห์นี้ (Gradual Increase)
    max_allowed_machines = available_machines
    
    print(f"🔍 Load Balancing Debug for {mc_group}:")
    print(f"   - Available machines: {available_machines}")
    print(f"   - Current week: {current_week_num} ({plan_week})")
    print(f"   - Item last week: {item_last_week if item_last_week > 0 else 'None'}")
    print(f"   - Carryover base machines: {carryover_base_machines}")
    print(f"   - Previous week usage (MC_GROUP): {previous_week_mc_usage}")
    
    # 🔧 เช็คจำนวนเครื่องรวมที่ใช้ในสัปดาห์ปัจจุบันแล้ว (จาก SC อื่นที่ประมวลผลไปแล้ว)
    current_week_key = (current_week_num, mc_group)
    current_week_total_machines = weekly_mc_usage.get(current_week_key, 0)
    print(f"   - Current week total machines (so far): {current_week_total_machines}")
    
    # ถ้าสัปดาห์ปัจจุบันมีเครื่องใช้แล้ว ต้องนับรวมด้วย
    if current_week_total_machines > 0:
        print(f"   ⚠️ สัปดาห์ปัจจุบันมี SC อื่นใช้ไปแล้ว {current_week_total_machines} เครื่อง")
    
    # 🔧 ใช้ item_last_week แทน previous_week_mc_usage สำหรับการตรวจสอบ gap
    # เพราะต้องเช็ค gap จากการใช้งานของ item นี้โดยเฉพาะ ไม่ใช่ MC_GROUP ทั้งหมด
    if item_last_week > 0:
        # Item นี้เคยใช้งาน MC_GROUP นี้มาก่อน
        previous_week_num = item_last_week
        previous_week_str = f"W{previous_week_num:02d}"
        
        # 🔧 เช็คว่า previous week ห่างจาก current week มากกว่า 1 สัปดาห์หรือไม่
        # ถ้าห่างมากกว่า 1 สัปดาห์ → ถือว่าเป็น "first time use" ใหม่
        week_gap = current_week_num - previous_week_num
        
        # 🔧 หาจำนวนเครื่องที่ ITEM นี้ใช้จริง (รวมทุก SC ของ item เดียวกัน)
        # ดูย้อนหลังสูงสุด 3 สัปดาห์จาก item_last_week เพื่อหา peak
        # ป้องกันกรณี SC เปลี่ยน ทำให้สัปดาห์สุดท้ายเครื่องน้อยกว่าปกติ
        item_prev_mc = 0
        _lookback_detail = {}
        for lookback in range(0, 3):
            check_week = item_last_week - lookback
            if check_week <= 0:
                break
            week_sum = 0
            for pe in plans:
                pi = pe.get('ITEM_CODE', '')
                pm = pe.get('MC_GROUP', '')
                pw = pe.get('PLAN_WEEK', '')
                try:
                    if isinstance(pw, str):
                        pwn = int(pw[1:]) if pw.startswith('W') else int(pw)
                    else:
                        pwn = int(pw)
                    if pi == item_code and pm == mc_group and pwn == check_week:
                        week_sum += pe.get('ACTUAL_MC', 0)
                except (ValueError, TypeError):
                    continue
            if week_sum > 0:
                _lookback_detail[check_week] = week_sum
                item_prev_mc = max(item_prev_mc, week_sum)
            else:
                break  # หยุดถ้าไม่มีข้อมูลในสัปดาห์นี้ (gap)
        # fallback: ลองหาจาก progressive_plan
        if item_prev_mc == 0 and progressive_plan:
            for k in [str(item_last_week), f"W{item_last_week}"]:
                if k in progressive_plan:
                    item_prev_mc = progressive_plan[k]
                    break
        if item_prev_mc == 0:
            item_prev_mc = 1
        previous_machines = item_prev_mc
        print(f"   - Item's own machines (peak of recent weeks): {previous_machines} {_lookback_detail}")
        
        # ประกาศตัวแปรที่จะใช้ใน loop
        next_week_idx = current_week_num + 1
        next_week_str = f"W{next_week_idx:02d}"
        next_week_machines_planned = 0
        
        if week_gap > 1:
            print(f"   ⚠️ Previous week {previous_week_str} ห่างจาก current week {week_gap} สัปดาห์ → ถือว่าเป็น first time use ใหม่")
            baseline = carryover_base_machines
            gap_start_cap = min(baseline + 2, available_machines)
            print(f"📝 First time using {mc_group} (after gap) - baseline {baseline} + setup <= 2 → max {gap_start_cap} machines")
            # กรณี after-gap ให้ยึดเครื่องที่วิ่งจริงเป็น baseline และเพิ่ม setup ได้สูงสุด +2
            max_allowed_machines = gap_start_cap
            # ข้ามการลองเพิ่มเครื่อง เพราะเป็นสัปดาห์แรก
        else:
            # week_gap == 1: item ใช้งานต่อเนื่อง → เพิ่มได้สูงสุด +2 จาก item's own previous
            print(f"   - Previous week {previous_week_str}: item ใช้ {previous_machines} เครื่อง → max +2 = {previous_machines + 2}")
            
            # วิธีที่ 1: ดูจาก progressive_plan (ถ้ามี) - รู้ว่า order นี้จะใช้เครื่องต่อใน next week หรือไม่
            order_will_continue = False
            if progressive_plan and next_week_str in progressive_plan:
                order_will_continue = True
                next_week_machines_planned = progressive_plan[next_week_str]
                print(f"   - {next_week_str} order นี้จะใช้ต่อ {next_week_machines_planned} เครื่อง (จาก progressive_plan)")
            else:
                # วิธีที่ 2: ดูจาก plans ที่ถูก append แล้ว (สำหรับ orders อื่นใน MC_GROUP เดียวกัน)
                next_week_machines_planned = 0
                for plan_entry in plans:
                    plan_week_value = plan_entry.get('PLAN_WEEK', '')
                    plan_mc_group = plan_entry.get('MC_GROUP', '')
                    
                    try:
                        plan_week_str = str(plan_week_value)
                        if plan_week_str.startswith('W'):
                            plan_week_num = int(plan_week_str[1:])
                        else:
                            plan_week_num = int(plan_week_value)
                        
                        if plan_mc_group == mc_group and plan_week_num == next_week_idx:
                            # รวม ACTUAL_MC ของทุก items (ไม่ใช่ max) เพราะแต่ละ item ใช้เครื่องแยกกัน
                            next_week_machines_planned += plan_entry.get('ACTUAL_MC', 0)
                    except (ValueError, TypeError):
                        continue
                
                print(f"   - {next_week_str} จะใช้ {next_week_machines_planned} เครื่อง (จาก plans)")
            
            # ลองเพิ่มทีละ 1 เครื่อง: ตรวจสอบว่าควรเพิ่ม +1 หรือ +2
            max_allowed_machines = previous_machines  # เริ่มต้นที่จำนวนเดิม
        
        # สำหรับกรณี carryover ที่มีเครื่องว่างและ order ยังเหลือเยอะ → ให้เพิ่มเครื่องได้
        if current_machines > 1 and qty_left > 0:
            print(f"   - Carryover mode: มี {current_machines} เครื่องอยู่แล้ว, order เหลือ {qty_left:.0f} units")
            print(f"   - Daily capacity: {daily_capacity}, Available machines: {available_machines}")
            # เริ่มจากเครื่องที่มีอยู่แล้วและตรวจสอบการเพิ่ม
            max_allowed_machines = current_machines
            
            # ถ้า order เหลือเยอะและมีเครื่องว่าง → ลองเพิ่มเครื่องทันที
            if daily_capacity > 0 and qty_left > daily_capacity * 5:  # เหลือมากกว่า 1 สัปดาห์
                for increment in [1, 2]:
                    test_machines = max_allowed_machines + increment
                    if test_machines <= available_machines:
                        weeks_needed = qty_left / (test_machines * daily_capacity * 5) if test_machines > 0 else 999
                        if weeks_needed >= 2:  
                            print(f"   ✅ อนุมัติ (carryover): Order เหลือ {qty_left:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) → เพิ่มเป็น {test_machines} เครื่อง")
                            max_allowed_machines = test_machines
                            break
                        else:
                            print(f"   ❌ ไม่อนุมัติ (carryover): Order เหลือ {qty_left:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) ต้องใช้อย่างน้อย 2 สัปดาห์")
            else:
                print(f"   - ไม่เพิ่มเครื่อง: daily_capacity={daily_capacity}, qty_left={qty_left}, threshold={daily_capacity * 5 if daily_capacity > 0 else 0}")
        
        for increment in [1, 2]:
            test_machines = max_allowed_machines + increment
            if test_machines > available_machines:
                break  # เกินเครื่องที่มี
            
            print(f"   🔍 ลองเพิ่มเป็น {test_machines} เครื่อง (+{increment}): {next_week_str} ใช้ {next_week_machines_planned} เครื่อง")
            
            # เงื่อนไขการอนุมัติ:
            # 1. Week ถัดไปจะใช้เครื่องเต็มตามจำนวนที่เพิ่ม (มี orders ใหม่)
            # 2. หรือ week ถัดไปยังเป็น order เดียวกันที่ carryover ต่อ (ใช้เครื่องเพิ่มได้)
            # 3. หรือ order ปัจจุบันยังเหลือ qty มากพอที่จะใช้เครื่องเพิ่มได้
            
            # 🔧 กฎใหม่: ตรวจสอบว่าจะใช้เครื่องครบอย่างน้อย 2 สัปดาห์หรือไม่
            # คำนวณจำนวนสัปดาห์ที่จะใช้เครื่องจำนวนนี้
            weeks_will_use = 0
            if qty_left > 0 and daily_capacity > 0:
                weeks_will_use = qty_left / (test_machines * daily_capacity * 5) if test_machines > 0 else 0
            
            # เช็คว่า week ถัดไปจะใช้เครื่องเต็มหรือไม่
            if next_week_machines_planned >= test_machines:
                # ต้องเช็คว่าจะใช้อย่างน้อย 2 สัปดาห์หรือไม่
                if weeks_will_use >= 2 or next_week_machines_planned >= test_machines:
                    print(f"   ✅ อนุมัติ: {next_week_str} จะใช้เต็ม {test_machines} เครื่อง (จะใช้ ~{weeks_will_use:.1f} weeks)")
                    max_allowed_machines = test_machines
                else:
                    print(f"   ❌ ไม่อนุมัติ: จะใช้แค่ {weeks_will_use:.1f} weeks ต้องใช้อย่างน้อย 2 สัปดาห์")
                    break
            elif next_week_machines_planned > 0 and next_week_machines_planned >= max_allowed_machines:
                # Week ถัดไปยังใช้เครื่องอยู่ (carryover) และไม่น้อยกว่าเดิม
                # แสดงว่า order ยังไม่เสร็จ สามารถเพิ่มเครื่องเพื่อทำให้เสร็จเร็วขึ้นได้
                # ต้องเช็คว่าจะใช้อย่างน้อย 2 สัปดาห์หรือไม่
                if weeks_will_use >= 2:
                    print(f"   ✅ อนุมัติ: {next_week_str} ยังมี order ต่อเนื่อง ({next_week_machines_planned} เครื่อง) → เพิ่มเป็น {test_machines} เพื่อเร่งผลิต (จะใช้ ~{weeks_will_use:.1f} weeks)")
                    max_allowed_machines = test_machines
                else:
                    print(f"   ❌ ไม่อนุมัติ: จะใช้แค่ {weeks_will_use:.1f} weeks ต้องใช้อย่างน้อย 2 สัปดาห์")
                    break
            elif qty_left > 0 and daily_capacity > 0:
                # เช็คว่า order ยังเหลือ qty มากพอที่จะใช้เครื่องเพิ่มได้หรือไม่
                # ถ้า order ยังไม่เสร็จและมีเครื่องว่าง → อนุมัติให้เพิ่ม
                # 🔧 กฎใหม่: ต้องใช้เครื่องครบอย่างน้อย 2 สัปดาห์ติดต่อกัน (ป้องกัน 2-3-2)
                weeks_needed = qty_left / (test_machines * daily_capacity * 5) if test_machines > 0 else 999
                if weeks_needed >= 2:  # ต้องใช้อย่างน้อย 2 สัปดาห์เต็ม
                    print(f"   ✅ อนุมัติ: Order ยังเหลือ {qty_left:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) → เพิ่มเป็น {test_machines} เพื่อเร่งผลิต")
                    max_allowed_machines = test_machines
                else:
                    print(f"   ❌ ไม่อนุมัติ: Order เหลือแค่ {qty_left:.0f} units (~{weeks_needed:.1f} weeks ที่ {test_machines} เครื่อง) ต้องใช้อย่างน้อย 2 สัปดาห์")
                    break
            else:
                print(f"   ❌ ไม่อนุมัติ: {next_week_str} จะใช้แค่ {next_week_machines_planned}/{test_machines} เครื่อง")
                break  # หยุดลองเพิ่ม
        
        # 🔧 Hard cap: บังคับทุก item ห้ามเพิ่มเกินกฎ
        if week_gap > 1:
            # First time after gap → baseline (เครื่องที่วิ่งจริง) + setup สูงสุด 2
            hard_cap = min(carryover_base_machines + 2, available_machines)
            if max_allowed_machines > hard_cap:
                print(f"   ⚠️ Hard cap (gap>{week_gap}): {max_allowed_machines} → {hard_cap} (carryover {carryover_base_machines} + 2)")
                max_allowed_machines = hard_cap
        else:
            # week_gap == 1 → ห้ามเพิ่มเกิน +2 จากฐานเครื่องที่ใช้งานจริง
            growth_base = carryover_base_machines if carryover_base_machines > 0 else previous_machines
            hard_cap = min(growth_base + 2, available_machines)
            if max_allowed_machines > hard_cap:
                print(f"   ⚠️ Hard cap: {max_allowed_machines} → {hard_cap} (base {growth_base} + 2)")
                max_allowed_machines = hard_cap
        
        print(f"   - Approved machines: {max_allowed_machines}")
    else:
        baseline = carryover_base_machines
        first_time_cap = min(baseline + 2, available_machines)
        print(f"📝 First time using {mc_group} - baseline {baseline} + setup <= 2 → max {first_time_cap} machines")
        # สัปดาห์แรกของ item นี้: อนุญาต setup เพิ่มได้สูงสุด +2 จากเครื่องที่วิ่งจริง
        max_allowed_machines = first_time_cap
    
    print(f"   - Final max allowed machines: {max_allowed_machines}")
    
    # Validate ก่อนจองเครื่อง
    actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
    
    # ตรวจสอบว่ามีเครื่องว่างพอตามที่กำหนดหรือไม่
    if actual_remain < max_allowed_machines:
        print(f"⚠️ Load Balancing Validation: {mc_group} มีเครื่องว่างแค่ {actual_remain} แต่ต้องการ {max_allowed_machines}")
        # ใช้จำนวนเครื่องที่มีจริง
        max_allowed_machines = actual_remain
    
    # ตรวจสอบว่าจะทำงานได้เต็มที่หรือไม่
    if required_machines_info and len(required_machines_info) > 0:
        # required_machines_info is a tuple: (mc_group, daily_cap, required_machines, feasible, gauge)
        # We need to get the order quantity from the main order data, not from this tuple
        # Skip this validation for now since we don't have access to order qty here
        # required_qty = required_machines_info[0].get('Orders.Qty', 0)  # This was wrong
        pass
    
    # ตรวจสอน job capacity limit
    type_used = get_type_used_jobs(plan_week, mc_group)
    final_available = check_job_capacity_limit(
        mc_group, max_allowed_machines, urgent_mode, type_used
    )
    
    if final_available > 0:
        print(f"✅ Load Balancing: {mc_group} ใช้ {final_available} เครื่อง (Gradual Increase)")
        print("   ℹ️ Deferred weekly_mc_usage update until plan commit to avoid duplicate-count drift")
        
        return mc_group, daily_cap, setup_needed, final_available, item_gauge
    
    return None, None, None, None, None


def distribute_load_across_machines(
    item_code,
    plan_week,
    required_machines,
    available_machines_list,
    last_production,
    item_gauge=None,
):
    """
    กระจายงานไปยังเครื่องจักรหลายๆ เครื่องอย่างสมดุล
    สำหรับกรณีที่ต้องการใช้เครื่องจักรมากกว่า 1 เครื่อง
    """
    if required_machines <= 1 or len(available_machines_list) <= 1:
        # ถ้าต้องการเครื่องเดียวหรือมีเครื่องว่างเครื่องเดียว ให้ใช้เครื่องแรก
        return available_machines_list[:1]
    
    # รวบรวมข้อมูลการใช้เครื่องจักรปัจจุบัน
    machine_usage_history = {}
    for key, machines in machines_in_use.items():
        mc_group = key[1]
        if mc_group not in machine_usage_history:
            machine_usage_history[mc_group] = 0
        machine_usage_history[mc_group] += machines
    
    # จัดเรียงเครื่องจักรตาม load balancing score
    scored_machines = []
    for mc_group in available_machines_list:
        usage_score = machine_usage_history.get(mc_group, 0)
        actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
        
        if actual_remain > 0:
            # คำนวณ load score: น้อยกว่า = โหลดน้อยกว่า = ดีกว่า
            load_score = usage_score
            scored_machines.append((mc_group, load_score, actual_remain))
    
    # เรียงตาม load score (น้อยไปมาก)
    scored_machines.sort(key=lambda x: x[1])
    
    # กระจายงานไปยังเครื่องที่โหลดน้อยที่สุดก่อน
    selected_machines = []
    remaining_machines_needed = required_machines
    
    for mc_group, load_score, available_count in scored_machines:
        if remaining_machines_needed <= 0:
            break
        
        # จัดสรรเครื่องตามที่มีว่าง แต่ไม่เกินที่ต้องการ
        machines_to_allocate = min(available_count, remaining_machines_needed)
        if machines_to_allocate > 0:
            selected_machines.extend([mc_group] * machines_to_allocate)
            remaining_machines_needed -= machines_to_allocate
    
    return selected_machines


def _analyze_and_balance_load(plan_week, current_plan):
    """
    วิเคราะห์ความสมดุลของ ACTUAL_MC ในสัปดาห์ที่กำหนดและปรับจำนวนเครื่องให้สมดุล
    โดยการปรับจำนวนเครื่องที่ใช้จริงในแต่ละ MC_GROUP
    """
    if not USE_LOAD_BALANCING:
        return current_plan
    
    print(f"🔍 Analyzing and balancing ACTUAL_MC for week {plan_week}...")
    
    # สร้างข้อมูลการใช้งานเครื่องจักรปัจจุบันจาก current_plan
    mc_load = {}
    mc_actual = {}
    for row in current_plan:
        mc_group = row['MC_GROUP']
        # ใช้ REQUIRED_MC แทน ALLOCATED_MC ตามข้อมูลจริง
        allocated_machines = row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1))
        if isinstance(allocated_machines, str):
            allocated_machines = float(allocated_machines.replace(',', ''))
        allocated_machines = float(allocated_machines)
        
        if mc_group not in mc_load:
            mc_load[mc_group] = 0
            mc_actual[mc_group] = []
        mc_load[mc_group] += allocated_machines
        mc_actual[mc_group].append(row)

    # หาค่าเฉลี่ยโหลดต่อเครื่องจักรในสัปดาห์นั้นๆ
    if not mc_load:
        print(f"ℹ️ No machine load to balance for week {plan_week}.")
        return current_plan

    total_allocated_mc = sum(mc_load.values())
    avg_load_per_mc = total_allocated_mc / len(mc_load)
    print(f"  Average ACTUAL_MC load for week {plan_week}: {avg_load_per_mc:.2f}")
    print(f"  Current ACTUAL_MC distribution: {mc_load}")

    # ระบุเครื่องจักรที่มีโหลดสูงและต่ำ
    hot_spots = {mc: load for mc, load in mc_load.items() 
                 if load > avg_load_per_mc * (1 + LOAD_BALANCING_THRESHOLD)}
    cold_spots = {mc: load for mc, load in mc_load.items() 
                  if load < avg_load_per_mc * (1 - LOAD_BALANCING_THRESHOLD)}

    if not hot_spots and not cold_spots:
        print(f"✅ ACTUAL_MC load for week {plan_week} is already balanced within {LOAD_BALANCING_THRESHOLD*100:.0f}% threshold.")
        return current_plan

    print(f"  Hot spots (> {avg_load_per_mc * (1 + LOAD_BALANCING_THRESHOLD):.2f}): {hot_spots}")
    print(f"  Cold spots (< {avg_load_per_mc * (1 - LOAD_BALANCING_THRESHOLD):.2f}): {cold_spots}")

    # ปรับจำนวน ACTUAL_MC ให้สมดุล
    new_plan = list(current_plan)  # ทำสำเนาเพื่อแก้ไข
    changes_made = False

    # คำนวณจำนวนเครื่องที่ควรมีในแต่ละ MC_GROUP เพื่อความสมดุล
    target_mc_per_group = avg_load_per_mc
    total_adjustment = 0

    # ปรับ hot spots ให้ลดลง
    for hot_mc, hot_load in sorted(hot_spots.items(), key=lambda x: x[1], reverse=True):
        if hot_mc in mc_actual:
            excess_load = hot_load - target_mc_per_group
            if excess_load > 0.1:  # ปรับเฉพาะที่เกินเกินมากพอ
                # กระจาย excess_load ไปยัง cold spots
                excess_to_distribute = excess_load * 0.5  # กระจาย 50% ของ excess
                
                # หา cold spots ที่สามารถรับเพิ่มได้
                available_cold_spots = []
                for cold_mc, cold_load in cold_spots.items():
                    if cold_mc in mc_actual:
                        capacity_for_increase = target_mc_per_group - cold_load
                        if capacity_for_increase > 0.1:
                            available_cold_spots.append((cold_mc, capacity_for_increase))
                
                if available_cold_spots:
                    # กระจาย excess ไปยัง cold spots ตามสัดส่วน
                    total_capacity = sum(cap for _, cap in available_cold_spots)
                    
                    for cold_mc, capacity in available_cold_spots:
                        portion = (capacity / total_capacity) * excess_to_distribute
                        if portion > 0.01:  # ปรับเฉพาะที่มีความเปลี่ยนแปลงมากพอ
                            # ปรับจำนวนเครื่องใน cold_mc
                            cold_rows = mc_actual[cold_mc]
                            total_current_cold = sum(float(r.get('REQUIRED_MC', r.get('ALLOCATED_MC', 1))) for r in cold_rows)
                            
                            # กระจายการเพิ่มไปยัง rows ต่างๆ ตามสัดส่วน
                            for row in cold_rows:
                                current_mc = float(row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1)))
                                if total_current_cold > 0:
                                    increase_ratio = portion / total_current_cold
                                    new_mc = current_mc * (1 + increase_ratio)
                                    row['REQUIRED_MC'] = new_mc
                            
                            print(f"  📈 Increased ACTUAL_MC in {cold_mc} by {portion:.2f} machines")
                            changes_made = True
                    
                    # ปรับจำนวนเครื่องใน hot_mc ให้ลดลง
                    hot_rows = mc_actual[hot_mc]
                    total_current_hot = sum(float(r.get('REQUIRED_MC', r.get('ALLOCATED_MC', 1))) for r in hot_rows)
                    
                    for row in hot_rows:
                        current_mc = float(row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1)))
                        if total_current_hot > 0:
                            decrease_ratio = excess_to_distribute / total_current_hot
                            new_mc = current_mc * (1 - decrease_ratio)
                            row['REQUIRED_MC'] = max(0.1, new_mc)  # ไม่ให้น้อยกว่า 0.1
                    
                    print(f"  📉 Decreased ACTUAL_MC in {hot_mc} by {excess_to_distribute:.2f} machines")
                    changes_made = True
    
    if changes_made:
        print(f"✅ ACTUAL_MC balancing completed for week {plan_week}")
        
        # แสดงผลลัพธ์หลังการปรับ
        final_mc_load = {}
        for row in new_plan:
            mc_group = row['MC_GROUP']
            allocated_machines = float(row.get('REQUIRED_MC', row.get('ALLOCATED_MC', 1)))
            if mc_group not in final_mc_load:
                final_mc_load[mc_group] = 0
            final_mc_load[mc_group] += allocated_machines
        
        print(f"  Final ACTUAL_MC distribution: {final_mc_load}")
        new_avg = sum(final_mc_load.values()) / len(final_mc_load)
        print(f"  New average: {new_avg:.2f}")
    else:
        print(f"ℹ️ No ACTUAL_MC balancing adjustments needed for week {plan_week}")
    
    return new_plan


# Create working hours lookup from Master_MC_5
# All machines now work 20 hours
WORKING_HOURS_MAP = {}
for _, row in master_mc.iterrows():
    mc_name = str(row["MC"]).strip().upper()
    WORKING_HOURS_MAP[mc_name] = 20  # All machines work 20 hours

def _get_working_hours_for_mc(mc_group: str) -> int:
    """Get working hours for MC_GROUP (20 or 24)"""
    mc_group_upper = mc_group.strip().upper()
    
    # Direct lookup first
    if mc_group_upper in WORKING_HOURS_MAP:
        return int(WORKING_HOURS_MAP[mc_group_upper])
    
    # Try to match by prefix (e.g., "SKP 20G" should match "SKP")
    for mc_key, hours in WORKING_HOURS_MAP.items():
        if mc_group_upper.startswith(mc_key + " ") or mc_group_upper.startswith(mc_key):
            return int(hours)
    
    # Default to 20 hours (all machines work 20 hours)
    return 20

def _convert_cap_per_day(cap_per_day: float, mc_group: str) -> float:
    """
    Convert CAP_PER_DAY to usable capacity based on working hours
    - All CAP_PER_DAY values are for 24 hours and must be converted to 20 hours
    - Convert: CAP_PER_DAY × (20/24) for all machines
    """
    # All machines work 20 hours, so convert all CAP_PER_DAY from 24-hour basis
    return cap_per_day * (20 / 24)

def _get_capacity_for_mc_group(item_code: str, mc_group: str, gauge: str = None) -> float:
    """
    Get daily capacity for a specific MC_GROUP with new selection logic:
    1. If MC_GROUP is in SHARED_POOL_MAP, use minimum capacity from machines within the same group
    2. FA 20 has priority over SKP 20 when both are available
    3. Otherwise, use the item's specific capacity for that MC_GROUP
    4. All capacities are normalized to ensure consistency
    """
    # Check if this MC_GROUP is part of a shared pool
    mc_gauge_key = (mc_group, _normalize_gauge(gauge))
    pool_members = _POOL_MEMBER_LOOKUP.get(mc_gauge_key)
    
    if pool_members:
        # For shared pools, find all machines in the pool that can produce this item
        pool_capacities = []
        pool_mc_groups = []
        
        for member_mc, member_gauge in pool_members:
            # Check if this item can be produced on this member MC_GROUP
            member_rows = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item_code) &
                (item_cap_data["MC_GROUP"] == member_mc)
            ]
            if not member_rows.empty:
                cap_value = member_rows["CAP ทอ"].iloc[0]
                # Normalize capacity for consistency
                normalized_cap = _normalize_capacity(item_code, member_mc, cap_value)
                pool_capacities.append(normalized_cap)
                pool_mc_groups.append(member_mc)
        
        if pool_capacities:
            # Priority: FA 20 over SKP 20
            fa_20_groups = [mc for mc in pool_mc_groups if mc.startswith("FA") and "20" in mc]
            skp_20_groups = [mc for mc in pool_mc_groups if mc.startswith("SKP") and "20" in mc]
            
            if fa_20_groups and skp_20_groups:
                # Use FA 20 capacity when both are available
                fa_20_caps = [cap for mc, cap in zip(pool_mc_groups, pool_capacities) if mc in fa_20_groups]
                return min(fa_20_caps)  # Use minimum among FA 20 groups
            
            # Use minimum capacity from the pool (all normalized)
            return min(pool_capacities)
    
    # Default: use item's specific capacity for this MC_GROUP
    item_rows = item_cap_data[
        (item_cap_data["ITEM_CODE"] == item_code) &
        (item_cap_data["MC_GROUP"] == mc_group)
    ]
    if not item_rows.empty:
        original_cap = item_rows["CAP ทอ"].iloc[0]
        return _normalize_capacity(item_code, mc_group, original_cap)
    
    # Fallback: use minimum capacity across all MC_GROUPS for this item
    all_item_caps = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]["CAP ทอ"]
    if not all_item_caps.empty:
        min_cap = all_item_caps.min()
        return _normalize_capacity(item_code, mc_group, min_cap)
    
    return 0  # Default if no capacity found

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

orders["Date"] = pd.to_datetime(orders["DATE_IN"], errors="coerce")
orders["DYE_END_DATE"] = pd.to_datetime(
    orders.get("DYE_END_DATE", orders.get("YARN_DYE_FINISH_DATE", orders.get("วันที่ย้อมเส้นด้ายจบ"))), errors="coerce"
)
orders["Item Code"] = orders["Item Code"].astype(str).str.upper().str.strip()
orders["Orders Type"] = orders["Orders Type"].astype(str).str.upper().str.strip()
orders["MC GROUP"] = orders["MC_GROUP"].astype(str).str.upper().str.strip()
orders["Orders.Qty"] = pd.to_numeric(orders["Orders.Qty"], errors="coerce").fillna(0)
orders["Plan Qty"] = pd.to_numeric(orders["Plan Qty"], errors="coerce").fillna(0)
orders["FG Week"] = orders["FG Week"]  # Already mapped
orders["SC/SO NO"] = orders["SC/SO NO"]  # Already mapped
orders["Pending Plan"] = pd.to_numeric(orders["Pending Plan"], errors="coerce").fillna(0)
orders["Confirm"] = pd.to_numeric(orders["CONFIRM_KNIT_PLAN_SC"], errors="coerce").fillna(0)
summary_mc["WEEK"] = summary_mc["WEEK"].astype(int)
calendar_week["WEEK"] = calendar_week["WEEK"].astype(int)

# =========================
# BUILD item_cap_data จาก CAP_PER_DAY ทุก row ใน orders
# dedup ด้วย (ITEM_CODE, MC_GROUP) — ใช้ค่าแรกที่พบสำหรับแต่ละ pair
# =========================
_no_cap_items = set()
_no_cap_order_rows = []  # เก็บ order rows ที่ไม่มี CAP_PER_DAY สำหรับสร้าง sheet แยก
_seen_item_mc: set = set()  # dedup key: (ITEM_CODE, MC_GROUP)
_existing_cap_items: set = set()  # items ที่มี cap อย่างน้อย 1 MC_GROUP

for _, _ord_row in orders.iterrows():
    _ord_item = str(_ord_row.get("Item Code", "")).strip().upper()
    if not _ord_item:
        continue

    _ord_mc = str(_ord_row.get("MC GROUP", "")).strip().upper()
    _ord_sc = str(_ord_row.get("SC/SO NO", "")).strip()
    _ord_cap_per_day = _ord_row.get("CAP_PER_DAY", 0)

    # ตรวจสอบว่ามี CAP_PER_DAY หรือไม่
    if pd.isna(_ord_cap_per_day) or float(_ord_cap_per_day) == 0:
        if _ord_item not in _existing_cap_items:
            if _ord_item not in _no_cap_items:
                print(
                    f"[SKIP] {_ord_item} (SC {_ord_sc}): ไม่มี CAP_PER_DAY — ไม่วางแผนผลิต"
                )
            _no_cap_items.add(_ord_item)
            _no_cap_order_rows.append(_ord_row)
        continue

    raw_cap = float(_ord_cap_per_day)
    _pair = (_ord_item, _ord_mc)

    # เพิ่มเข้า item_cap_data เฉพาะ pair ใหม่ (ไม่ซ้ำ)
    if _pair not in _seen_item_mc:
        _seen_item_mc.add(_pair)
        new_cap_row = {
            "ITEM_CODE": _ord_item,
            "MC_GROUP": _ord_mc,
            "CAP ทอ": raw_cap,
            "REVOLUTION/WEIGHT": 1.0,
            "GUAGE": str(_ord_row.get("MC_GUAGE", "")).strip().upper(),
        }
        item_cap_data = pd.concat(
            [item_cap_data, pd.DataFrame([new_cap_row])], ignore_index=True
        )
        converted_cap = raw_cap * (20 / 24)
        print(
            f"[ADD CAP] {_ord_item} (SC {_ord_sc}) MC={_ord_mc}: "
            f"CAP_PER_DAY {_ord_cap_per_day} (24hr) → {converted_cap:.2f} (20hr)"
        )

    _existing_cap_items.add(_ord_item)

_no_cap_df = pd.DataFrame(_no_cap_order_rows) if _no_cap_order_rows else pd.DataFrame()

# Rebuild gauge lookup หลัง CAP_PER_DAY ถูก populate เข้า item_cap_data แล้ว
for _, _r in item_cap_data.iterrows():
    _ic = str(_r.get("ITEM_CODE", "")).strip().upper()
    _mc = str(_r.get("MC_GROUP", "")).strip().upper()
    _gg = _r.get("GUAGE")
    _gs = _normalize_gauge(_gg)
    if _ic and _mc and _gs and _gs.lower() != "nan":
        _item_mc_to_gauge[(_ic, _mc)] = _gs

# =========================
# TRACK ITEMS WITH MULTIPLE CAPACITIES (SINGLE MC_TYPE and OM/OMNOI)

# =========================
_multi_cap_items = set()
_multi_cap_order_rows = []  # เก็บ order rows ที่มี multiple capacities

def _check_multiple_capacities(item_code: str, mc_group: str) -> bool:
    """ตรวจสอบว่า MC_GROUP นั้นๆ มีหลายค่า capacity ที่ไม่เหมือนกัน สำหรับ SINGLE MC_TYPE หรือ OM/OMNOI หรือไม่"""
    # หาข้อมูล MC_GROUP จาก Master_MC_5
    mc_info = master_mc[master_mc["MC"] == mc_group]
    if mc_info.empty:
        return False
    
    factory = str(mc_info.iloc[0]["Factory"]).strip().upper()
    mc_type = str(mc_info.iloc[0].get("Type", "")).strip().upper()
    
    # Debug: แสดงข้อมูล MC_GROUP
    if factory in ("OM", "OMNOI") or mc_type == "SINGLE":
        print(f"[DEBUG MC_INFO] {item_code} in {mc_group}: Factory={factory}, Type={mc_type}")
    
    # ตรวจสอบสำหรับ SINGLE MC_TYPE หรือ OM/OMNOI factories
    if mc_type == "SINGLE" or factory in ("OM", "OMNOI"):
        # ตรวจสอบว่า MC_GROUP นั้นๆ มีหลายค่า capacity หรือไม่
        mc_group_caps = item_cap_data[
            (item_cap_data["ITEM_CODE"] == item_code) &
            (item_cap_data["MC_GROUP"] == mc_group)
        ]
        print(f"[DEBUG CAP_COUNT] {item_code} in {mc_group}: {len(mc_group_caps)} entries found")
        
        if len(mc_group_caps) > 1:
            # ตรวจสอบว่าค่า capacity ไม่เหมือนกันหรือไม่
            unique_caps = set(mc_group_caps["CAP ทอ"].tolist())
            if len(unique_caps) > 1:
                # Debug: แสดงข้อมูล capacity ที่ไม่เหมือนกัน
                caps_list = mc_group_caps["CAP ทอ"].tolist()
                print(f"[DEBUG MULTI_CAP] {item_code} ใน {mc_group} มี {len(mc_group_caps)} entries ค่าต่างกัน: {caps_list}")
                return True
            else:
                # มีหลาย entries แต่ค่าเหมือนกัน - เป็นข้อมูลซ้ำ
                caps_list = mc_group_caps["CAP ทอ"].tolist()
                print(f"[DEBUG DUPLICATE] {item_code} ใน {mc_group} มี {len(mc_group_caps)} entries ค่าเหมือนกัน: {caps_list}")
    
    return False

# ตรวจสอบ items ที่มี multiple capacities
print("🔍 Starting MULTI_CAP detection...")
debug_count = 0
for _, _ord_row in orders.iterrows():
    _ord_item = str(_ord_row.get("Item Code", "")).strip().upper()
    _ord_mc = str(_ord_row.get("MC GROUP", "")).strip().upper()
    
    if _ord_item and _ord_mc:
        debug_count += 1
        if debug_count <= 5:  # Debug แค่ 5 rows แรก
            print(f"[DEBUG CHECKING] {_ord_item} with {_ord_mc}")
        
        if _check_multiple_capacities(_ord_item, _ord_mc):
            if _ord_item not in _multi_cap_items:
                print(f"[MULTI_CAP] {_ord_item} (MC {_ord_mc}): มีหลาย capacities - SINGLE MC_TYPE หรือ OM/OMNOI")
            _multi_cap_items.add(_ord_item)
            _multi_cap_order_rows.append(_ord_row)

print(f"🔍 Checked {debug_count} order rows for MULTI_CAP detection")

_multi_cap_df = pd.DataFrame(_multi_cap_order_rows) if _multi_cap_order_rows else pd.DataFrame()

# =========================
# TRACK RESERVOIR-GF ORDERS (Default N-3 Offset)

# =========================
_reservoir_items = set()
_reservoir_order_rows = []  # เก็บ order rows สำหรับ RESERVOIR-GF


for _, _ord_row in orders.iterrows():
    _ord_fob_type = str(_ord_row.get("FOB_TYPE", "")).strip()
    
    if _ord_fob_type == "RESERVOIR-GF":
        _ord_item = str(_ord_row.get("Item Code", "")).strip().upper()
        _ord_sc = str(_ord_row.get("SC/SO NO", "")).strip()
        
        _reservoir_items.add(_ord_item)
        _reservoir_order_rows.append(_ord_row)

_reservoir_df = pd.DataFrame(_reservoir_order_rows) if _reservoir_order_rows else pd.DataFrame()

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
    # วันศุกร์หยุดเสมอ → max working days ต่อสัปดาห์ = 6 (เสาร์-พฤหัส)
    if factory_type == "PHET":
        if mc_type == "DOUBLE":
            FACTORY_WORKING_DAYS_MAP[main_mc_group] = 6
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


def get_revolution_weight_from_orders(item_code, mc_group):
    """ค้นหา REVOLUTION_WEIGHT ของ item จาก orders DataFrame (order_ready)"""
    # หาข้อมูลของ item นี้จาก orders DataFrame
    item_rows = orders[orders['Item Code'] == item_code]
    if not item_rows.empty:
        # ใช้ REVOLUTION_WEIGHT จาก order_ready โดยตรง
        rev_weight = item_rows.iloc[0].get('REVOLUTION_WEIGHT', 0)
        return rev_weight if rev_weight and rev_weight > 0 else 0
    return 0


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
    เงื่อนไขพิเศษ Week 17: ทุก group → 8 วัน
    """
    # เงื่อนไขพิเศษ Week 17 - ทุก group ทำงาน 8 วัน
    if week == 17:
        return 8

    # หาวันทำงานจาก FACTORY_WORKING_DAYS_MAP
    working_days = FACTORY_WORKING_DAYS_MAP.get(mc_group, 6)  # default = 6 วัน (วันศุกร์หยุดเสมอ)
    return working_days


def _dynamic_setup_limit(
    plan_week: int, rdd_idx, required_mc: int, remaining_job_slots: int
) -> int:
    """คืนจำนวน new machines สูงสุดที่ควร setup ใน week นี้ ตาม urgency ของ RDD
    - ห่าง RDD >= 2 week : ใช้แค่ required_mc  (ประหยัด job slot ไว้ให้ order อื่น)
    - ห่าง RDD == 1 week : ใช้แค่ required_mc (จำกัดตาม required_mc)
    - plan_week >= RDD   : ใช้แค่ required_mc (จำกัดตาม required_mc)
    ทุก case ยังต้องผ่าน check_job_capacity_limit อีกรอบเสมอ
    rdd_idx = row index ใน calendar_week (ใช้แทน fg_week_int เพื่อรองรับข้ามปี)"""
    fallback = required_mc  # เปลี่ยนจาก remaining_job_slots เป็น required_mc
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
        # urgent / เลยกำหนดแล้ว → ใช้ required_mc
        return required_mc

    elif weeks_to_rdd == 1:
        # สัปดาห์สุดท้ายก่อน RDD → ใช้ required_mc
        return required_mc

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
    # ตัด (TUBE) ออก — "SINGLE (TUBE)" ถือเป็น SINGLE เหมือนกัน
    mc_type_clean = mc_type.replace("(TUBE)", "").strip()
    if factory == "PHET":
        if mc_type_clean == "DOUBLE":
            max_jobs = 33
        elif mc_type_clean == "SINGLE":
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


WEEK_DAYS_OVERRIDE = {
    17: 8,  # week 17 มี 8 วันทำงาน (รวมวันพิเศษ)
}


def get_working_days_in_week(week):
    """Get working days for a specific week from calendar (กรองวันหยุดออก)
    วันศุกร์เป็นวันหยุดประจำสัปดาห์เสมอ → กรองออกทุกกรณี
    ถ้ามีวันหยุดในปฏิทินตรงกับวันศุกร์ก็ไม่ต้องหักเพิ่ม (หักแล้ว)
    แต่ถ้าหยุดวันอื่น จะหักออกตามปกติ
    """
    # ตรวจสอบ override ก่อน
    if week in WEEK_DAYS_OVERRIDE:
        override_days = WEEK_DAYS_OVERRIDE[week]
        week_data = calendar_week[calendar_week["WEEK"] == week]
        if not week_data.empty:
            week_start = week_data.iloc[0]["WEEK_START"]
            week_end = week_data.iloc[0]["WEEK_END"]
            mask = (
                (calendar["DATE"] >= week_start)
                & (calendar["DATE"] <= week_end)
                & (calendar["is_working_day"] == 1)
                & (calendar["DATE"].dt.dayofweek != 4)  # 4 = Friday (วันศุกร์หยุดเสมอ)
            )
            base_days = calendar.loc[mask, "DATE"].tolist()
        else:
            base_days = []
        # เติมให้ครบตาม override (pad ด้วย None สำหรับวันพิเศษ)
        while len(base_days) < override_days:
            base_days.append(None)
        return base_days[:override_days]

    week_data = calendar_week[calendar_week["WEEK"] == week]
    if week_data.empty:
        return []

    week_start = week_data.iloc[0]["WEEK_START"]
    week_end = week_data.iloc[0]["WEEK_END"]
    # กรองเฉพาะวันที่ is_working_day == 1 จาก daily calendar
    # และกรองวันศุกร์ออกเสมอ (วันศุกร์หยุดประจำสัปดาห์)
    mask = (
        (calendar["DATE"] >= week_start)
        & (calendar["DATE"] <= week_end)
        & (calendar["is_working_day"] == 1)
        & (calendar["DATE"].dt.dayofweek != 4)  # 4 = Friday (วันศุกร์หยุดเสมอ)
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

    # ถ้า mc_group+gauge อยู่ใน MC_GROUP_REDIRECT → ให้ตรวจ availability ของ target แทน
    # เช่น SKP 20 → ตรวจ FA 20 เสมอ (เพราะ FA รับผลิตแทน)
    mc_group, gauge_str = _apply_mc_redirect(mc_group, gauge_str)

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


def get_next_fg_orders_for_item(item_code, current_sc_so, current_fg_week, orders_df):
    """ตรวจสอบว่ามี FG ถัดไปของ item เดียวกันหรือไม่ และคืนค่า ORDER_QTY รวม
    Args:
        item_code: รหัส item ที่ต้องการตรวจสอบ
        current_sc_so: SC/SO NO ปัจจุบัน
        current_fg_week: FG Week ปัจจุบัน (YYYYWW format)
        orders_df: DataFrame ของ orders ทั้งหมด
    Returns:
        total_next_qty: ปริมาณรวมของ FG ถัดไปที่ต้องผลิต
    """
    if orders_df is None or orders_df.empty:
        return 0.0
    
    # กรอง order ของ item เดียวกันที่มี FG Week มากกว่า current_fg_week
    same_item_orders = orders_df[orders_df["Item Code"] == item_code].copy()
    
    if same_item_orders.empty:
        return 0.0
    
    total_next_qty = 0.0
    
    for _, next_order in same_item_orders.iterrows():
        next_sc_so = str(next_order.get("SO_NO", next_order.get("SC/SO NO", ""))).strip()
        next_fg_week = next_order.get("FG Week")
        
        # ข้าม row ที่เป็น SC+FG เดียวกันกับปัจจุบัน (exact same row)
        # ไม่ skip same-SC ที่มี FG ต่างกัน เพราะต้องนับ FG ถัดไปของ SC เดียวกัน
        try:
            _curr_fg_int = int(current_fg_week) if pd.notna(current_fg_week) else 0
            _next_fg_int = int(next_fg_week) if pd.notna(next_fg_week) else 0
        except (ValueError, TypeError):
            _curr_fg_int = 0
            _next_fg_int = 0
        if next_sc_so == current_sc_so and _next_fg_int == _curr_fg_int:
            continue
        
        # ตรวจสอบว่า FG Week ถัดไปมากกว่า current หรือไม่
        if pd.notna(next_fg_week):
            try:
                next_fg_int = int(next_fg_week)
                current_fg_int = int(current_fg_week) if pd.notna(current_fg_week) else 0
                
                # ถ้า FG Week ถัดไปมากกว่า current (ผลิตหลัง)
                if next_fg_int > current_fg_int:
                    pending_plan = pd.to_numeric(next_order.get("Pending Plan", 0), errors="coerce")
                    if not pd.isna(pending_plan) and pending_plan > 0:
                        total_next_qty += float(pending_plan)
            except (ValueError, TypeError):
                continue
    
    return total_next_qty


def get_total_pending_qty_for_item(item_code, current_sc_so, current_fg_week, orders_df, same_sc_only=False):
    """รวม Pending Plan ของทุก FG สำหรับ item เดียวกัน (รวมทุก SC/SO)
    ใช้สำหรับคำนวณ machine allocation จาก total demand แทนที่จะดูแค่ FG เดียว
    
    Args:
        same_sc_only: ถ้า True จะรวมเฉพาะ SC/SO เดียวกัน (ใช้สำหรับ carry optimization)
    
    คืนค่า: total_pending_qty รวมของ FG ปัจจุบัน + FG ถัดไปทั้งหมด
    (เฉพาะ FG Week >= current_fg_week เพื่อไม่นับ FG ที่ผ่านไปแล้ว)
    """
    if orders_df is None or orders_df.empty:
        return 0.0
    
    same_item_orders = orders_df[orders_df["Item Code"] == item_code]
    if same_item_orders.empty:
        return 0.0
    
    try:
        current_fg_int = int(current_fg_week) if pd.notna(current_fg_week) else 0
    except (ValueError, TypeError):
        current_fg_int = 0
    
    total_qty = 0.0
    for _, row in same_item_orders.iterrows():
        # ถ้า same_sc_only=True → กรองเฉพาะ SC/SO เดียวกัน
        if same_sc_only:
            row_sc = str(row.get("SO_NO", row.get("SC/SO NO", ""))).strip()
            if row_sc != current_sc_so:
                continue
        
        fg_week = row.get("FG Week")
        try:
            fg_int = int(fg_week) if pd.notna(fg_week) else 0
        except (ValueError, TypeError):
            continue
        
        # รวมเฉพาะ FG Week >= current (FG ปัจจุบัน + อนาคต)
        if fg_int >= current_fg_int:
            pending = pd.to_numeric(row.get("Pending Plan", 0), errors="coerce")
            if not pd.isna(pending) and pending > 0:
                total_qty += float(pending)
    
    return total_qty


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
    weeks_until_target = []
    current_week = start_week
    target_week_index = fg_week  # TARGET_KNIT
    
    # สร้าง list ของ weeks จาก start_week ถึง TARGET_KNIT โดยเด็ดขาด
    while current_week is not None and week_index(current_week) <= target_week_index:
        weeks_until_target.append(current_week)
        current_week = next_week(current_week)
    if not weeks_until_target:
        return None

    # เก็บ availability และ working days ของแต่ละ week
    week_info = []
    for week in weeks_until_target:
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
    item_code, order_qty, start_week, fg_week, setup_days=SETUP_DAYS, only_mc_group=None,
    order_type="", sub_color="",
):
    print(f"[DEBUG CALC] calculate_required_machines called: {item_code}, qty={order_qty}, start={start_week}, target={fg_week}")
    """คำนวณจำนวนเครื่องขั้นต่ำที่ต้องการเพื่อทัน RDD
    หลักการ: ใช้เครื่องน้อยแต่ผลิตหลาย week ดีกว่าใช้เครื่องเยอะแค่ 1 week
    - setup เป็น per-machine: 3mc setup = เสีย 3×3=9 mc-days
    - week 2+ ไม่ต้อง setup → ได้ผลิตเต็มที่
    - simulate per-week ด้วยเครื่องว่างจริงของแต่ละ week (cap at n_mc)
    ตัวอย่าง order 3277.5, cap=163, factory 7d, เครื่องว่าง [6, 1, 5]:
      6mc×3wk: wk1=6×4×163=3912, wk2=1×7×163=1141, wk3=5×7×163=5705 → setup_waste=18
      2mc×3wk: wk1=2×4×163=1304, wk2=1×7×163=1141, wk3=2×7×163=2282 → setup_waste=6 
    """
    # หา MC_GROUP ที่สามารถผลิต item นี้ได้
    available_machines = item_cap_data[item_cap_data["ITEM_CODE"] == item_code]
    if available_machines.empty:
        return None, None, None, None, None

    # ใช้ความจุตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน
    # และ FA 20 มี priority สูงกว่า SKP 20
    # ใช้ความจุของ MC_GROUP แรกที่พบเป็นค่าเริ่มต้น และจะปรับเมื่อวนลูปแต่ละ MC_GROUP
    daily_cap = None
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
    # คำนวณจำนวนสัปดาห์ที่เหลือถึง TARGET_KNIT (บังคับให้จบตรง TARGET_KNIT)
    # fg_week คือ TARGET_KNIT index (row index ใน calendar_week) เพื่อรองรับข้ามปีได้
    weeks_until_target = []
    current_week = start_week
    target_week_index = fg_week  # TARGET_KNIT
    
    # สร้าง list ของ weeks จาก start_week ถึง TARGET_KNIT โดยเด็ดขาด
    while current_week is not None and week_index(current_week) <= target_week_index:
        weeks_until_target.append(current_week)
        current_week = next_week(current_week)
    
    if not weeks_until_target:
        return None, None, None, None, None
    
    # ตรวจสอบว่า week สุดท้ายคือ TARGET_KNIT จริงๆ
    last_week_index = week_index(weeks_until_target[-1])
    if last_week_index != target_week_index:
        print(f"⚠️  TARGET_KNIT mismatch: last week {last_week_index} != target {target_week_index}")
    
    num_weeks = len(weeks_until_target)
    # ลองแต่ละ MC_GROUP ที่สามารถผลิตได้ (เรียงตาม cap น้อยไปมาก — ใช้ cap ต่ำสุดในการคำนวณ)
    for _, machine_row in available_machines.iterrows():
        mc_group = machine_row["MC_GROUP"]
        # ใช้ความจุตาม logic ใหม่สำหรับ MC_GROUP นี้
        item_gauge = machine_row["GUAGE"] if "GUAGE" in machine_row else None
        daily_cap = _get_capacity_for_mc_group(item_code, mc_group, item_gauge)
        if daily_cap <= 0:
            continue  # ข้ามถ้าไม่มีความจุ
        # เก็บจำนวนเครื่องว่างจริงของแต่ละ week
        avail_per_week = []
        has_any_machine = False
        for week in weeks_until_target:
            actual_remain = get_actual_mc_remain(mc_group, week, gauge=item_gauge)
            avail_per_week.append(actual_remain)
            if actual_remain > 0:
                has_any_machine = True
        # เครื่องที่วิ่งอยู่แล้ว (carry-over) ถือว่า "มี" เครื่องพร้อมผลิตโดยไม่ต้องดู actual_remain
        _key_check = _resolve_carry_key(item_code, mc_group, item_gauge)
        if not has_any_machine and machines_in_use.get(_key_check, 0) <= 0:
            continue

        # ---- Setup-aware: ตรวจสอบว่าต้อง setup หรือไม่ ----
        key = _resolve_carry_key(item_code, mc_group, item_gauge)
        setup_needed = True
        start_week_idx = week_index(start_week)
        if key in last_production:
            last_week_idx = last_production[key]
            if start_week_idx - last_week_idx <= SETUP_GAP_WEEK:
                setup_needed = False
        # เครื่องที่วิ่งอยู่แล้ว (carry-over จาก booking/old plan)
        # ถ้า setup_needed=False = เครื่องยังอุ่นอยู่ → ใช้เป็น committed_mc ตั้งต้น
        carryover_start = machines_in_use.get(key, 0) if not setup_needed else 0
        # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → carryover ต้อง setup เพิ่ม 1 วัน (เฉพาะ week แรก)
        _yd_color_setup_days = 0
        if order_type == "YD-ORDERS" and carryover_start > 0 and sub_color:
            _prev_sc = last_sub_color.get(key, "")
            if _prev_sc and _prev_sc != sub_color:
                _yd_color_setup_days = 1
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

        # tolerance: ถ้า qty_remaining ≤ 1 batch (rev_weight) ให้ถือว่าผลิตครบด้วย MC จำนวนนั้น
        # ป้องกันการเพิ่ม MC แค่เพราะ overflow 1 batch (เศษที่เกินจาก rounding)
        _rw_tol = float(get_revolution_weight(item_code, mc_group, start_week) or 0)

        # ---- เปรียบเทียบทุก option ด้วย per-week simulation ----
        best_option = None  # (n_machines, weeks_needed, setup_waste, efficiency)
        
        # คำนวณเครื่องที่เหมาะสมเพื่อจบตรง TARGET_KNIT
        weeks_available = target_week_index - week_index(weeks_until_target[0]) + 1
        if weeks_available > 0:
            # คำนวณเครื่องที่ต้องการเพื่อกระจายผลิตจนถึง TARGET_KNIT
            total_production_needed = order_qty
            total_days_available = weeks_available * factory_wd
            optimal_mc = max(1, int(total_production_needed / (total_days_available * daily_cap) * 1.1))  # +10% buffer
            print(f"[DEBUG TARGET] Optimal machines for {item_code}: {optimal_mc} (target {target_week_index}, weeks {weeks_available})")
        
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
            for w_idx, week in enumerate(weeks_until_target):
                if qty_remaining <= _rw_tol:
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
                # YD-ORDERS: SUB_COLOR เปลี่ยน → carryover หัก 1 วัน (เฉพาะ week แรก)
                if _yd_color_setup_days > 0 and w_idx == 0 and carryover > 0:
                    prod_days_carry = max(0, prod_days_carry - _yd_color_setup_days)
                total_setup_mc_days += setup_mc * setup_days
                committed_mc = want_mc  # อัปเดตเครื่องที่ใช้จริง
                weeks_needed += 1
                actual_use_list.append(want_mc)
                actual_wd_list.append(actual_wd)
                week_production = (
                    carryover * prod_days_carry + new_added * prod_days_new
                ) * daily_cap
                qty_remaining -= week_production
            finished = qty_remaining <= _rw_tol  # ≤ 1 batch = ถือว่าครบ ไม่เพิ่ม MC
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
                # ตรวจสอบว่าจบตรง TARGET_KNIT หรือ TARGET_KNIT-1 หรือไม่
                last_week_produced = weeks_until_target[w_idx]
                last_week_index = week_index(last_week_produced)
                target_week_index = fg_week  # TARGET_KNIT
                
                print(f"[DEBUG TARGET] {item_code}: finished at week {last_week_index}, target {target_week_index}")
                
                if last_week_index == target_week_index or last_week_index == target_week_index - 1:
                    # จบตรง TARGET_KNIT หรือ TARGET_KNIT-1 → ยอมรับ option นี้
                    print(f"[DEBUG TARGET] ✅ Acceptable match: {item_code} finishes at {last_week_index} (target {target_week_index}, tolerance -1)")
                    best_option = (n_mc, weeks_needed, total_setup_mc_days, efficiency)
                    break
                elif last_week_index < target_week_index - 1:
                    # จบก่อน TARGET_KNIT-1 → ต้องใช้เครื่องน้อยลงเพื่อกระจายไปจนถึง TARGET_KNIT
                    print(f"[DEBUG TARGET] ❌ Too early: {item_code} finishes at {last_week_index}, target {target_week_index}")
                    # คำนวณว่าต้องลดเครื่องกี่เครื่องให้จบตรง TARGET_KNIT
                    weeks_available = target_week_index - week_index(weeks_until_target[0]) + 1
                    if weeks_available > 0:
                        # คำนวณเครื่องที่ต้องการเพื่อกระจายผลิตจนถึง TARGET_KNIT
                        avg_daily_needed = order_qty / (weeks_available * factory_wd * daily_cap)
                        suggested_mc = max(1, int(avg_daily_needed * 0.8))  # ใช้ 80% เพื่อความปลอดภัย
                        if suggested_mc < n_mc:
                            # ลองใหม่ด้วยเครื่องน้อยลง
                            continue
                    continue
                else:
                    # เลย TARGET_KNIT → ไม่ใช่ option นี้
                    print(f"[DEBUG TARGET] ❌ Too late: {item_code} finishes at {last_week_index}, target {target_week_index}")
                    continue

        if best_option:
            required_machines = best_option[0]
            return mc_group, daily_cap, required_machines, True, item_gauge  # feasible

        else:
            # ไม่มี option ที่จบตรง TARGET_KNIT → หา option ที่ใกล้เคียงที่สุด
            print(f"[DEBUG TARGET] ⚠️ No perfect match for {item_code}, finding closest option...")
            
            # คำนวณ option ที่ใกล้เคียงที่สุด
            best_option = None
            min_distance = float('inf')
            
            for n_mc in range(1, int(max_try) + 1):
                # Simulate อีกครั้งเพื่อหา option ที่ใกล้เคียงที่สุด
                qty_remaining = order_qty
                weeks_needed = 0
                committed_mc = min(carryover_start, n_mc)
                total_setup_mc_days = 0
                
                for w_idx, week in enumerate(weeks_until_target):
                    if qty_remaining <= _rw_tol:
                        break
                    
                    cal_wd = len(get_working_days_in_week(week))
                    actual_wd = min(cal_wd, factory_wd)
                    avail_this_week = avail_per_week[w_idx]
                    if avail_this_week <= 0 and committed_mc > 0:
                        avail_this_week = committed_mc
                    
                    want_mc = min(n_mc, avail_this_week)
                    if want_mc <= 0:
                        continue
                    
                    # คำนวณ production และ setup
                    new_added = max(0, want_mc - committed_mc)
                    if new_added > 0:
                        setup_mc_days = new_added * setup_days
                        if setup_mc_days > actual_wd * want_mc:
                            continue
                        total_setup_mc_days += setup_mc_days
                        committed_mc = want_mc
                    
                    prod_days_new = actual_wd - setup_days
                    prod_days_carry = actual_wd
                    # YD-ORDERS: SUB_COLOR เปลี่ยน → carryover หัก 1 วัน (เฉพาะ week แรก)
                    if _yd_color_setup_days > 0 and w_idx == 0 and committed_mc > 0:
                        prod_days_carry = max(0, prod_days_carry - _yd_color_setup_days)
                    week_production = (
                        committed_mc * prod_days_carry + new_added * prod_days_new
                    ) * daily_cap
                    qty_remaining -= week_production
                    weeks_needed += 1
                
                if qty_remaining <= _rw_tol:  # ≤ 1 batch = ถือว่าครบ
                    last_week_index = week_index(weeks_until_target[min(w_idx, len(weeks_until_target)-1)])
                    distance = abs(last_week_index - target_week_index)
                    
                    if distance < min_distance:
                        min_distance = distance
                        best_option = (n_mc, weeks_needed, total_setup_mc_days, 0)
            
            if best_option:
                print(f"[DEBUG TARGET] 🎯 Using closest option for {item_code}: distance {min_distance} weeks")
                required_machines = best_option[0]
                return mc_group, daily_cap, required_machines, True, item_gauge  # feasible (closest)
            else:
                # ไม่ทันทุก option → ใช้เครื่องเต็มที่ + บอก caller ว่า NOT feasible
                print(f"[DEBUG TARGET] ❌ No feasible option for {item_code}, using max machines")
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
    print(f"[DEBUG BEST MC] Processing {item_code} for week {plan_week}")
    print(f"[DEBUG BEST MC] Total items processed: {len([x for x in globals().get('_processed_items', [])])}")
    if '_processed_items' not in globals():
        globals()['_processed_items'] = []
    globals()['_processed_items'].append(f"{item_code}_{plan_week}")
    
    # ถ้ามีการคำนวณจำนวนเครื่องที่ต้องการมาแล้ว ให้ใช้ค่านั้น
    if required_machines_info is not None:
        mc_group, daily_cap, required_machines, *_ = required_machines_info
        print(f"[DEBUG BEST MC] Using pre-calculated: {mc_group}, {required_machines} machines")
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
            key = _resolve_carry_key(item_code, mc_group, item_gauge)
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
                prev_mc = machines_in_use.get(key, 0)
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
                    new_additions = min(extra_avail, can_add, MAX_NEW_SETUP_MC)
                else:
                    new_additions = min(extra_avail, MAX_NEW_SETUP_MC)
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

    # ใช้ความจุตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน
    # และ FA 20 มี priority สูงกว่า SKP 20
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
        # ใช้ความจุตาม logic ใหม่สำหรับ MC_GROUP นี้
        item_gauge = machine_row["GUAGE"] if "GUAGE" in machine_row else None
        daily_cap = _get_capacity_for_mc_group(item_code, mc_group, item_gauge)
        if daily_cap <= 0:
            continue  # ข้ามถ้าไม่มีความจุ
        # ดูเครื่องว่างจริง (หักที่จองไปแล้ว)
        actual_remain = get_actual_mc_remain(mc_group, plan_week, gauge=item_gauge)
        if actual_remain > 0:
            type_used = get_type_used_jobs(plan_week, mc_group)
            available_machines_count = check_job_capacity_limit(
                mc_group, actual_remain, urgent_mode, type_used
            )
            if available_machines_count <= 0:
                continue  # ลอง MC_GROUP ถัดไป

            key = _resolve_carry_key(item_code, mc_group, item_gauge)
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
            # ใช้ความจุตาม logic ใหม่สำหรับ MC_GROUP นี้
            item_gauge = (
                prev_mc_row.iloc[0]["GUAGE"] if "GUAGE" in prev_mc_row.iloc[0] else None
            )
            daily_cap = _get_capacity_for_mc_group(item_code, mc_group, item_gauge)
            if daily_cap <= 0:
                continue  # ข้ามถ้าไม่มีความจุ
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
    """สร้าง type_key: OM/OMNOI ไม่มี Type ใช้ชื่อ factory อย่างเดียว
    SINGLE (TUBE) รวมกับ SINGLE เป็นตัวเดียวกัน"""
    if factory in ("OM", "OMNOI"):
        return "OM"
    
    # ตัด (TUBE) ออก - SINGLE (TUBE) = SINGLE
    mc_type_clean = mc_type.replace(" (TUBE)", "").strip() if mc_type else ""
    
    return f"{factory}_{mc_type_clean}" if mc_type_clean else factory


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


def detect_and_fill_unused_capacity(plans_list, orders_df, summary_mc):
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
    # Use actual available machines in that week, not just REQUIRED_MC
    def get_available_machines_for_week_mc(week, mc_group):
        """Get actual available machines for specific week and MC_GROUP"""
        # Use summary_mc to get total available machines (before booking)
        mc_rows = summary_mc[
            (summary_mc["WEEK"] == week) &
            (summary_mc["MC_GROUP"] == mc_group)
        ]
        if mc_rows.empty:
            return 0
        # Sum all TOTAL_MC_REMAIN for this MC_GROUP in this week
        return int(mc_rows["TOTAL_MC_REMAIN"].sum())
    
    # Add available machines column
    current_usage['AVAILABLE_MC'] = current_usage.apply(
        lambda row: get_available_machines_for_week_mc(row['PLAN_WEEK'], row['MC_GROUP']), 
        axis=1
    )
    
    current_usage['FULL_CAPACITY'] = (
        current_usage['AVAILABLE_MC'] * 
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
    
    # 🔧 FIX: สร้าง set ของ Item+Week+MC_GROUP ที่มีอยู่แล้ว เพียงครั้งเดียว
    existing_item_week_mc = set()
    for existing_plan in plans_list:
        existing_item = existing_plan.get('ITEM_CODE', '')
        existing_week = existing_plan.get('PLAN_WEEK', 0)
        existing_mc = existing_plan.get('MC_GROUP', '')
        if existing_item and existing_week and existing_mc:
            existing_item_week_mc.add((existing_item, existing_week, existing_mc))
    
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

            sc_so_no = str(order.get('SO_NO', order.get('SC/SO NO', ''))).strip()
            pending_qty = order['Pending Plan']
            # Calculate how much we can produce in remaining capacity
            produce_qty = min(unused_qty, pending_qty)
            if produce_qty > 0:
                # 🔧 FIX: ตรวจสอบว่า Item+Week+MC_GROUP นี้มีการ plan ไปแล้วหรือไม่
                # เพื่อป้องกันการซ้ำซ้อนกับ plans ที่มีอยู่แล้ว
                current_item_week_mc = (item, week, mc_group)
                if current_item_week_mc in existing_item_week_mc:
                    print(f"  ⚠️  SKIPPED capacity optimization: {item}+{week}+{mc_group} already planned!")
                    break
                
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
                    'REVOLUTION_WEIGHT': get_revolution_weight_from_orders(item, mc_group),
                    'AVAILABLE_DAYS': get_working_days_by_factory(mc_group, usage_row['REQUIRED_MC'], week=week),
                    'ORDERS_QTY': order['Orders.Qty'],
                    'PENDING_PLAN': pending_qty - produce_qty,
                    'PLAN_QTY': pending_qty - produce_qty,
                    'ORDER_TYPE': order['Orders Type'],
                    'ORDER_DATE': order['Date'],
                    'FG_WEEK': order['FG Week'],
                    'TARGET_KNIT': order['FG Week'],  # Simplified
                    'MATERIAL_CONTENT': str(order.get('MATERIAL_CONTENT', '')).strip(),
                    'IS_CORE_ITEM': '',
                    'CUSTOMER': str(order.get('Customer', '')).strip(),
                    'PLAN_SOURCE': 'NEW',
                    'LT_YARN': order.get('DYE_END_DATE') if str(order.get('Orders Type', '')).strip() == 'YD-ORDERS' else get_yarn_lt_days(item),
                    'YARN_USED': _yarn_used_lookup.get(str(item).strip().upper(), ''),
                    'DATE_IN': order.get('DATE_IN'),
                    'EARLIEST_PLAN_WEEK': order.get('DYE_END_DATE') if str(order.get('Orders Type', '')).strip() == 'YD-ORDERS' else get_yarn_lt_earliest_week(item, date_in=order.get('DATE_IN')),
                    'SUB_COLOR': str(order.get('SUB_COLOR', '')).strip(),
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
weekly_mc_usage = {}  # {(week, mc_group): total machines} สำหรับ gradual increase
last_sc_so_no = (
    {}
)  # {(item, mc_group): SC/SO NO ของ order ที่ผลิตล่าสุด — ป้องกัน carry-over ข้าม color/order}
last_sub_color = {}  # {carry_key: SUB_COLOR} — สำหรับ YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน ต้อง setup เพิ่ม 1 วัน
_yd_week_color_setups = {}  # {(carry_key, plan_week): cumulative color setup days} — สะสมวัน setup สีใน week เดียวกัน
booking_production_keys = set()  # keys ที่มาจาก detail_mc (booking จริง) — ใช้อนุญาต carryover จาก booking แม้ยังไม่มีใน new plan
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
    booking_production_keys.add(key)  # track ว่า key นี้มาจาก booking จริง
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
remaining_week_cap_owner = {}  # {(week, item_code, mc_group): owner marker (item-level carry)}

# =========================
# MERGE SAME SC + SAME ITEM (+ FG Week เดียวกัน)

# =========================
# ถ้า SC/SO NO เหมือนกัน + SO_NO เหมือนกัน + Item Code เหมือนกัน + FG Week เดียวกัน → รวมเป็น 1 row ผลิตทีเดียว
# ต้องใช้ SO_NO ด้วยเพราะ SC เดียวกันอาจมีหลาย SO (คนละ order จริง) → ห้ามรวม qty
# ถ้า FG Week ต่างกัน → คง row แยกไว้ (deadline ต่างกัน → plan แยก)
_so_no_col = "SO_NO" if "SO_NO" in orders.columns else None
_grp_keys = ["SC/SO NO"] + (["SO_NO"] if _so_no_col else []) + ["Item Code", "MC GROUP", "MC_GUAGE"]
if "FG Week" in orders.columns:
    _grp_keys = _grp_keys + ["FG Week"]
_sum_cols = [
    c
    for c in ["Orders.Qty", "Plan Qty", "Pending Plan", "Confirm"]
    if c in orders.columns
]
_min_cols = [c for c in ["DYE_END_DATE"] if c in orders.columns]
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
_skip_no_mc_group = []  # เก็บ order ที่ไม่มี MC GROUP → ไม่วางแผน
_skip_no_factory = []  # เก็บ order ที่ MC GROUP ไม่มี FACTORY_TYPE → ไม่วางแผน
new_plan_started_items = set()  # ติดตาม (item, mc_group) ที่เริ่มการผลิตใน new plan แล้ว
# YD-ORDERS: ล็อกจำนวนเครื่องต่อ (item, mc_group, plan_week)
# ภายใน week เดียวกัน ห้ามเพิ่ม/ลดเครื่อง — SO ถัดไปใช้เครื่องเท่าเดิม
_yd_week_locked_mc: dict = {}  # {(item, mc_group, plan_week): (actual_mc, carryover_mc, new_mc)}
locked_mc_group_for: dict = (
    {}
)  # ล็อก MC_GROUP (highest-cap) ต่อ (sc_so_no, item) ให้ FG Week ต่างๆ ใช้ร่วมกัน
# ติดตาม last plan week index ต่อ (sc_so_no, item) เพื่อบังคับให้ FG_WEEK ถัดไป
# เริ่มหลัง FG_WEEK ก่อนหน้าจบ (ไม่ผลิตซ้อนกัน)
_last_fg_plan_idx: dict = {}  # {(sc_so_no, item): last_week_index}
# เรียง orders ตาม TARGET_KNIT (rdd_idx) จริง ไม่ใช่ FG Week
# เพราะ order type ต่างกัน TARGET_KNIT ต่างกัน (LAB-DIP = FG-1, SC-ORDERS = FG-offset ตาม FOB_TYPE)


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
# tiebreaker 1: FG Week raw (น้อยกว่า = ก่อน) → หาก rdd_idx เท่ากัน ให้ FG เร็วกว่าก่อน
# tiebreaker 2: PENDING_PLAN (น้อยกว่า = ก่อน) → ของน้อยจบไว ปล่อยเครื่องให้ item เดียวกัน carry ต่อ
_fg_week_col = "FG Week" if "FG Week" in orders.columns else None
_pending_col = "Pending Plan" if "Pending Plan" in orders.columns else None
_sort_cols = ["_sort_rdd_idx"]
if _fg_week_col:
    orders["_sort_fg_week"] = pd.to_numeric(orders[_fg_week_col], errors="coerce").fillna(99999999)
    _sort_cols.append("_sort_fg_week")
if _pending_col:
    orders["_sort_pending"] = pd.to_numeric(orders[_pending_col], errors="coerce").fillna(0)
    _sort_cols.append("_sort_pending")
orders_sorted = orders.sort_values(_sort_cols, na_position="last")
_drop_cols = [c for c in ["_sort_rdd_idx", "_sort_fg_week", "_sort_pending"] if c in orders_sorted.columns]
orders_sorted = orders_sorted.drop(columns=_drop_cols)
orders = orders.drop(columns=[c for c in _drop_cols if c in orders.columns])
print(f"[DEBUG ORDERS] Total orders: {len(orders)}")

# 🔧 FIX: ตรวจสอบว่า Item+Week+MC_GROUP นี้มีการ plan ไปแล้วหรือไม่
# เพื่อป้องกันการซ้ำซ้อนของ orders ที่มี Item+MC_GROUP เดียวกันใน week เดียวกัน
# โดยไม่สนใจว่าจะเป็น SC หรือ TARGET_KNIT อะไร (เพราะอาจมีการ merge SC)
_existing_item_week_mc = set()
# 🔧 FIX: ติดตาม qty ที่วางแผนไปแล้วสำหรับแต่ละ item (สะสมข้าม order/SC/FG)
# ใช้สำหรับคำนวณ next_fg_qty ที่แม่นยำ — ไม่นับ demand จาก order ที่ plan ไปแล้ว
_item_cumulative_planned = {}  # {item_code: total_qty_planned_so_far}
print(f"[DEBUG DUPLICATE] Starting with {len(plans)} existing plans")

for _, order in orders_sorted.iterrows():
    item = order["Item Code"]
    order_qty = order["Orders.Qty"]  # ปริมาณที่สั่งทั้งหมด
    plan_qty = order["Plan Qty"]  # ปริมาณที่วางแผนไปแล้ว (รอ approve)
    pending_plan = pd.to_numeric(order.get("Pending Plan", 0), errors="coerce")
    pending_plan = 0.0 if pd.isna(pending_plan) else float(pending_plan)
    sc_so_no = str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip()  # ใช้แยก order ต่างสี
    sub_color = str(order.get("SUB_COLOR", "")).strip()  # SUB_COLOR สำหรับ YD-ORDERS carry logic
    # ถ้าไม่มี MC GROUP → เก็บไว้แจ้ง ไม่วางแผน
    _ord_mc_grp = str(order.get("MC GROUP", "")).strip().upper()
    if not _ord_mc_grp or _ord_mc_grp in ("", "NAN", "NONE"):
        _skip_no_mc_group.append({
            "SC_SO_NO": sc_so_no, "ITEM_CODE": item,
            "ORDERS_QTY": order_qty, "PENDING_PLAN": pending_plan,
            "FG_WEEK": order.get("FG Week", ""),
            "REASON": "ไม่มี MC GROUP"
        })
        continue
    # ถ้า MC GROUP ไม่มีใน FACTORY_TYPE_MAP → เก็บไว้แจ้ง ไม่วางแผน
    _ord_factory = FACTORY_TYPE_MAP.get(_ord_mc_grp, "")
    if not _ord_factory or _ord_factory in ("", "UNKNOWN"):
        _skip_no_factory.append({
            "SC_SO_NO": sc_so_no, "ITEM_CODE": item,
            "MC_GROUP": _ord_mc_grp, "ORDERS_QTY": order_qty,
            "PENDING_PLAN": pending_plan, "FG_WEEK": order.get("FG Week", ""),
            "REASON": f"MC GROUP '{_ord_mc_grp}' ไม่มี FACTORY_TYPE"
        })
        continue
    # ถ้า Pending Plan = 0 แสดงว่า order นี้วางแผนครบแล้ว ไม่ต้องวางแผนซ้ำ
    print(f"[DEBUG ORDERS] Order {sc_so_no}: item={item}, pending_plan={pending_plan}")
    if pending_plan <= 0:
        print(f"[DEBUG ORDERS] Skipping order {sc_so_no} - pending_plan <= 0")
        continue

    order_type = order["Orders Type"]
    fg_week = order.get("FG Week")
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
                    booking_remain_cap_by_mc = {}  # remaining cap ในสัปดาห์สุดท้าย booking
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
                        # คำนวณ remaining capacity ในสัปดาห์สุดท้ายของ booking
                        # เพื่อให้ new SO สามารถ carry ต่อในสัปดาห์เดียวกันได้
                        # โดยจำกัด qty ตาม old item ที่ใช้เครื่องร่วมกัน (MC_USE fraction)
                        try:
                            _last_bk_row = grp[grp["WEEK"] == w]
                            if not _last_bk_row.empty:
                                _mc_use_ceil = float(_last_bk_row.iloc[0].get("MC_USE_CEIL", 0) or 0)
                                _bk_kp_weight = float(_last_bk_row.iloc[0].get("KP_WEIGHT", 0) or 0)
                                _bk_wd = int(_last_bk_row.iloc[0].get("WORKING_DAY", 5) or 5)
                                _bk_gauge = str(_last_bk_row.iloc[0].get("GUAGE", "")).strip()
                                _bk_plan_cap = _get_capacity_for_mc_group(
                                    item, str(mc).strip().upper(), _bk_gauge
                                )
                                # remaining = total machine capacity − qty ที่ old item ผลิตจริง (KP_WEIGHT)
                                # สูตร: (MC_USE_CEIL × wd × daily_cap) − KP_WEIGHT
                                # เช่น FD4DRTPC88A0 w17: (2×8×103.06)−1143 = 1648−1143 = 505 units
                                _total_cap = _mc_use_ceil * _bk_wd * _bk_plan_cap
                                if _bk_plan_cap > 0 and _mc_use_ceil > 0:
                                    _bk_rem = max(0.0, _total_cap - _bk_kp_weight)
                                    if _bk_rem > 0:
                                        booking_remain_cap_by_mc[str(mc).strip().upper()] = _bk_rem
                        except Exception:
                            pass
                    rts_local_force = {
                        "last_old_by_mc": last_old_by_mc,
                        "machines_by_mc": machines_by_mc,
                        "daily_cap_by_mc": daily_cap_by_mc,
                        "booking_remain_by_mc": booking_remain_cap_by_mc,
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
            # คำนวณ offset ตาม FOB_TYPE
            fob_type = str(order.get("FOB_TYPE", "")).strip()
            if fob_type in ["PILOT_RUN", "Salesman", "Salesman-PO", "Sample"]:
                offset_weeks = 1  # N-1
            elif fob_type in ["Replacement SO", "Make to Order","RESERVOIR-GF"]:
                offset_weeks = 3  # N-3
            else:
                offset_weeks = 3  # Default to N-3 for other types
            
            # หัก offset สัปดาห์ (RDD = FG Week - offset) ด้วย index arithmetic
            rdd_idx = max(0, _fg_raw_idx - offset_weeks)
            fg_week_int = int(calendar_week.iloc[rdd_idx]["WEEK"])  # สำหรับ display
    # LAB-DIP: deadline = TODAY + 2 weeks (ต้องเสร็จภายใน week ที่เริ่มผลิต)
    if order_type == "LAB-DIP":
        rdd_idx = min(len(calendar_week) - 1, TODAY_IDX + 2)
        fg_week_int = int(calendar_week.iloc[rdd_idx]["WEEK"])  # อัพเดท display
    if rdd_idx is not None and rdd_idx < TODAY_IDX:
        # RDD ผ่านไปแล้ว = URGENT!
        # สำหรับ urgent order ต้องใช้ความสามารถสูงสุด
        # อาจจะต้องเพิ่มเครื่อง แต่ต้องไม่เกิน job/day capacity
        # urgent_mode = True  # DISABLED
        urgent_mode = False
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
        # SC-ORDERS: คำนวณ start week ให้ทอจบก่อน TARGET_KNIT เสมอ
        # ถ้า TARGET_KNIT = 20, ต้องจบทอภายใน week 19 (คือสัปดาห์ก่อน TARGET_KNIT)
        if rdd_idx is not None and rdd_idx >= 0:
            # ใช้ rdd_idx (คือ FG Week - offset ตาม FOB_TYPE) เป็นพื้นฐาน แล้ว +1 สัปดาห์เพื่อให้แน่ใจว่าจบก่อน TARGET_KNIT-1
            # สูตร: start_week = (FG Week - offset) + 1
            # ถ้า FG Week = 20, offset = 3, start_week = 18 (ทอ week 18-19, จบก่อน week 19)
            target_start_idx = rdd_idx + 1

            # ต้องรอ POIN พร้อมก่อนเสมอ แม้จะเลย deadline
            # สูตร: max(target, POIN_week, TODAY+2)
            min_start_idx = get_yarn_lt_min_start_idx(item, date_in=order.get("DATE_IN"))
            start_idx = max(target_start_idx, min_start_idx)

            if start_idx < len(calendar_week):
                order_week = calendar_week.iloc[start_idx]["WEEK"]
            else:
                continue
        else:
            # fallback: ใช้ Yarn LT หรือ TODAY + 2 ถ้าไม่มี rdd_idx
            min_start_idx = get_yarn_lt_min_start_idx(item, date_in=order.get("DATE_IN"))
            if min_start_idx < len(calendar_week):
                order_week = calendar_week.iloc[min_start_idx]["WEEK"]
            else:
                continue

    elif order_type == "YD-ORDERS":
        yd_week = get_week_from_date(order["DYE_END_DATE"])
        if yd_week is not None:
            order_week = next_week(yd_week)  # +1 week หลังวันย้อมเสร็จ
        else:
            # ถ้าไม่มี DYE_END_DATE ให้ใช้ FG Week แทน (fallback)
            if pd.notna(fg_week) and fg_week is not None:
                fg_week_str = str(int(fg_week)).strip()
                if len(fg_week_str) >= 6:
                    year = int(fg_week_str[:4])
                    week = int(fg_week_str[4:])
                elif len(fg_week_str) == 5:
                    year = int(fg_week_str[:4])
                    week = int(fg_week_str[4:])
                else:
                    year = TODAY.year
                    week = int(fg_week_str)
                idx = calendar_week[
                    (calendar_week["YEAR"] == year) & (calendar_week["WEEK"] == week)
                ].index
                if len(idx) > 0 and idx[0] >= 3:
                    order_week = int(calendar_week.iloc[idx[0] - 3]["WEEK"])
                else:
                    order_week = None
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

    # ❗ ถ้า booking ของ item+mc_group นี้ยังวิ่งถึง week ≥ plan_week → จัดการ 2 กรณี:
    # กรณี 1: plan_week == last booking week → plan ลงได้ใน week เดียวกัน แต่จำกัด qty ตาม remaining cap
    #          (remaining_week_cap ถูก seed ไว้แล้วจาก booking_remain_by_mc)
    #          ไม่ต้อง push plan_week ออก เพราะ cross-SC fill จะจัดการ qty เอง
    # กรณี 2: last booking week > plan_week → push ออกไปหลัง booking (ถ้ายังทัน RDD)
    _bk_mc_grp = str(order.get("MC GROUP", "")).strip().upper()
    if _bk_mc_grp:
        _bk_last_idx = last_production.get(_ck(item, _bk_mc_grp))
        if _bk_last_idx is not None and _bk_last_idx >= start_idx:
            _bk_last_week = int(calendar_week.iloc[_bk_last_idx]["WEEK"])
            _after_bk_idx = _bk_last_idx + 1
            if plan_week == _bk_last_week:
                # plan_week == last booking week: ปล่อยให้ plan_week อยู่ตรงนั้น
                # cross-SC fill จะใช้ remaining_week_cap ที่ seed ไว้ (idle capacity หลัง old item)
                pass
            elif _after_bk_idx < len(calendar_week):
                # plan_week < last booking week: push ออกไปหลัง booking ถ้าทัน RDD
                if rdd_idx is None or _after_bk_idx <= rdd_idx:
                    start_idx = _after_bk_idx
                    plan_week = int(calendar_week.iloc[start_idx]["WEEK"])
                    # ข้าม SKIP_WEEKS หลัง booking check
                    while plan_week in SKIP_WEEKS:
                        plan_week = next_week(plan_week)
                        if plan_week is None:
                            break

                # ถ้าขยับแล้วไม่ทัน RDD → ใช้ start_idx เดิม (ผลิตซ้อน booking ได้)
    # ❗ ถ้า SC/SO+Item เดิมเคยวาง FG_WEEK ก่อนหน้าแล้ว → ให้เริ่มจาก week ที่ FG แรกเสร็จ
    # เพื่อให้ FG ที่ 2 สามารถ carry over ใช้ capacity ที่เหลือจาก FG แรกได้
    # ยกเว้น: ถ้า item เดียวกัน มี cap เหลือจาก week นั้น → เริ่มใน week เดิมได้
    # ⚠️ แต่ถ้าต่อท้ายแล้วไม่ทัน RDD → อนุญาตให้ผลิตซ้อน FG ก่อนหน้าได้
    # 🔧 FIX: อนุญาตให้ FG ที่ 2 carry over ไปใช้ capacity ที่เหลือจาก FG แรก
    _prev_fg_idx = _last_fg_plan_idx.get((sc_so_no, item))
    if _prev_fg_idx is not None:
        _prev_fg_week = int(calendar_week.iloc[_prev_fg_idx]["WEEK"])
        # ตรวจว่ามี remaining cap ใน week นั้นสำหรับ item นี้หรือไม่
        # หมายเหตุ: remaining_week_cap จะถูกสร้างขึ้นหลังจาก FG แรกผลิตเสร็จ
        # ดังนั้น FG ที่ 2 สามารถใช้ capacity ที่เหลือได้ผ่าน cross-SC carryover logic (line 2802-2907)
        _item_mcs = set(
            str(r).strip().upper()
            for r in item_cap_data.loc[
                item_cap_data["ITEM_CODE"] == str(item).strip().upper(), "MC_GROUP"
            ]
        )
        # ตรวจ remaining cap ใน ทุก week ตั้งแต่ start_idx จนถึง _prev_fg_idx
        # (ไม่ใช่แค่ week สุดท้ายของ FG แรก เพราะ FG แรกอาจทิ้ง cap ไว้ใน week กลางๆ)
        # 🔧 FIX: รวม _prev_fg_idx ในการตรวจด้วย แม้ _prev_fg_idx < start_idx
        # เพราะ target_start_idx = rdd_idx + 1 อาจข้าม week ที่ FG ก่อนหน้าจบ (มี remaining cap)
        _rem_check_start = min(start_idx, _prev_fg_idx)
        _has_rem_cap = any(
            remaining_week_cap.get((int(calendar_week.iloc[_wi]["WEEK"]), item, _mc), 0) > 0
            for _wi in range(_rem_check_start, _prev_fg_idx + 1)
            if _wi < len(calendar_week)
            for _mc in _item_mcs
        )
        if _has_rem_cap:
            # 🔧 FIX: เริ่ม FG ถัดไปจาก week สุดท้ายของ FG ก่อนหน้า (_prev_fg_idx)
            # ห้ามเริ่มจาก week กลางๆ ที่มี remaining cap จาก rev_weight rounding
            # เพราะ FG ก่อนหน้ายังผลิตไม่จบ → ต้องรอให้จบก่อนค่อย carry ต่อ
            _prev_fg_w = int(calendar_week.iloc[_prev_fg_idx]["WEEK"])
            _has_rem_at_last = any(
                remaining_week_cap.get((_prev_fg_w, item, _mc), 0) > 0
                for _mc in _item_mcs
            )
            if _has_rem_at_last:
                # FG ก่อนหน้าจบใน week นี้และมี cap เหลือ → เริ่ม FG ถัดไปที่นี่
                start_idx = _prev_fg_idx
                plan_week = _prev_fg_w
                print(
                    f"[FG CONTINUATION] {item}: FG ถัดไป start at W{_prev_fg_w} "
                    f"(remaining cap from previous FG last week)"
                )
            else:
                # FG ก่อนหน้าใช้ cap หมดใน week สุดท้าย → เริ่ม week ถัดไป
                _after_prev = _prev_fg_idx + 1
                if _after_prev < len(calendar_week):
                    start_idx = _after_prev
                    plan_week = int(calendar_week.iloc[_after_prev]["WEEK"])
                    print(
                        f"[FG CONTINUATION] {item}: FG ถัดไป start at W{plan_week} "
                        f"(next week after previous FG finished at W{_prev_fg_w})"
                    )
        else:
            if _prev_fg_idx >= start_idx:
                # FG แรกเสร็จหลัง start_idx ของ FG ที่ 2
                # สำหรับ SC-ORDERS ให้เริ่มจาก earliest feasible week เดิม
                # ไม่บังคับรอ FG ก่อนหน้าจบทั้งก้อน เพื่อให้ทอได้ไวขึ้น (เช่นเริ่มที่ W19)
                if order_type == "SC-ORDERS":
                    print(
                        f"[KEEP EARLIEST START] {item}: keep start week {int(calendar_week.iloc[start_idx]['WEEK'])} "
                        f"instead of pushing to previous FG week {_prev_fg_week}"
                    )
                # non SC-ORDERS ยังใช้กฎเดิม
                elif rdd_idx is not None and _prev_fg_idx > rdd_idx:
                    pass  # ไม่ push — ใช้ start_idx เดิม (อนุญาต overlap กับ FG แรก)
                else:
                    start_idx = _prev_fg_idx
                    plan_week = _prev_fg_week
            # else: ใช้ start_idx เดิม (FG ที่ 2 เริ่มก่อน FG แรกเสร็จ → ผลิตซ้อนกันได้)

    # ----------------------
    # weekly allocation with best machine selection

    # ----------------------
    # คำนวณจำนวนเครื่องที่ต้องการตั้งแต่แรก (ถ้าทัน RDD)
    required_machines_info = None
    # คำนวณ setup days ล่วงหน้า (ใช้ใน calculate_required_machines ด้วย)
    order_material_content = str(order.get("MATERIAL_CONTENT", "")).strip()
    order_setup_days = get_setup_days_for_item(item, order_material_content)
    # ❗ ตรวจสอบว่า item นี้มี cap data หรือไม่ — ถ้าไม่มีให้ข้ามทันที
    _item_cap_rows = item_cap_data[item_cap_data["ITEM_CODE"] == str(item).strip().upper()]
    if _item_cap_rows.empty:
        _skip_no_cap.append(f"{item} (SC/SO:{sc_so_no})")
        print(f"⚠️  ไม่พบ CAP data สำหรับ item '{item}' (SC/SO:{sc_so_no}) → ข้ามการวางแผน")
        continue

    # คำนวณ machine allocation ล่วงหน้า
    progressive_plan = None  # {week: machines} สำหรับแต่ละ week
    # 🔧 FIX: รวม Pending Plan ของทุก FG สำหรับ item เดียวกัน (ทุก SC/SO)
    # เพื่อให้คำนวณจำนวนเครื่องจาก total demand ไม่ใช่แค่ FG ปัจจุบัน
    # ป้องกันการลดเครื่องเมื่อ FG หนึ่งจบ ทั้งที่ยังมี FG ถัดไปรอผลิต
    _total_qty_all_fg = get_total_pending_qty_for_item(item, sc_so_no, fg_week, orders_sorted)
    _qty_for_machine_calc = max(qty_left, _total_qty_all_fg)
    # 🔧 FIX: คำนวณ demand เฉพาะ SC ปัจจุบัน (ไม่รวม SC อื่นของ item เดียวกัน)
    # ใช้สำหรับ carry optimization เพื่อป้องกันการ hold เครื่องเกินจำเป็นจาก cross-SC demand
    _total_qty_same_sc = get_total_pending_qty_for_item(item, sc_so_no, fg_week, orders_sorted, same_sc_only=True)
    _qty_for_current_sc = max(qty_left, _total_qty_same_sc)
    if _total_qty_all_fg > qty_left:
        print(f"[TOTAL FG QTY] {item} SC {sc_so_no}: qty_left={qty_left:.0f}, total_all_fg={_total_qty_all_fg:.0f}, same_sc={_total_qty_same_sc:.0f} → ใช้ {_qty_for_machine_calc:.0f} สำหรับคำนวณเครื่อง")
    if rdd_idx is not None and rdd_idx >= week_index(plan_week):
        _locked_mc = locked_mc_group_for.get((sc_so_no, item))
        print(f"[DEBUG START] Processing order: {sc_so_no}, item: {item}, qty: {order_qty}, FG week: {fg_week}")
        
        # คำนวณจำนวนเครื่องที่ต้องการสำหรับ item นี้ (เพื่อจบก่อน TARGET_KNIT/RDD)
        # ใช้ rdd_idx เป็น target เพราะต้องทอเสร็จก่อน RDD (FG Week - offset ตาม FOB_TYPE)
        target_knit_idx = rdd_idx
        mc_group_calc, daily_cap_calc, required_machines, feasible_calc, _gauge_calc = (
            calculate_required_machines(
                item,
                _qty_for_machine_calc,
                plan_week,
                target_knit_idx,  # TARGET_KNIT/RDD
                setup_days=order_setup_days,
                only_mc_group=_locked_mc,
                order_type=order_type,
                sub_color=sub_color,
            )
        )
        print(f"[DEBUG CALC] {item}: mc_group={mc_group_calc}, required_machines={required_machines}, feasible={feasible_calc}")
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
                    _qty_for_machine_calc,
                    plan_week,
                    rdd_idx,
                    mc_group_calc,
                    daily_cap_calc,
                    _gauge_calc,
                    setup_days=order_setup_days,
                    rev_weight=get_revolution_weight_from_orders(item, mc_group_calc)
                )
                if prog_result:
                    progressive_plan = {wk: mc for wk, mc in prog_result}
    
    _produced_week = None  # init สำหรับ track FG_WEEK sequential
    _earliest_backshift_done = False  # ป้องกันการย้อนกลับ EARLIEST_PLAN_WEEK ซ้ำใน order เดียวกัน
    # คำนวณ EARLIEST_PLAN_WEEK สำหรับ order นี้ (ใช้ตรวจสอบ carry-over และ cross-SC fill ด้วย)
    # เส้นด้ายต้องเข้าก่อนถึงจะ carry/produce ได้ ไม่ว่าจะเป็น new setup หรือ carryover
    _yarn_min_start_idx = get_yarn_lt_min_start_idx(item, date_in=order.get("DATE_IN"))
    _yarn_earliest_plan_week = (
        int(calendar_week.iloc[_yarn_min_start_idx]["WEEK"])
        if _yarn_min_start_idx < len(calendar_week)
        else None
    )
    # YD-ORDERS: earliest plan week ต้องไม่เร็วกว่า DYE_END_DATE +1 week
    # เพราะ yarn ต้องย้อมเสร็จก่อนถึงจะทอได้ (order_week = next_week หลัง DYE_END_DATE)
    if order_type == "YD-ORDERS" and _yarn_earliest_plan_week is not None and order_week is not None:
        if _yarn_earliest_plan_week < order_week:
            print(f"[YD GUARD] {item}: _yarn_earliest_plan_week W{_yarn_earliest_plan_week} < DYE+1 W{order_week} → clamp to W{order_week}")
            _yarn_earliest_plan_week = order_week
    # บังคับเริ่มวางแผนจาก EARLIEST_PLAN_WEEK ถ้า plan_week ถูกดันไปช้ากว่า
    # เพื่อให้ใช้ capacity ต้นช่วงได้ก่อน (เช่น ต้องเริ่มที่ W19)
    # 🔧 FIX: ถ้ามี _prev_fg_idx (FG ถัดไปของ SC เดิม) → ไม่ force กลับ earliest
    # เพราะ FG ถัดไปควร carry ต่อจาก week ที่ FG ก่อนหน้าเหลือ cap (waterfall flow)
    if (
        _yarn_earliest_plan_week is not None
        and plan_week is not None
        and plan_week > _yarn_earliest_plan_week
        and _yarn_earliest_plan_week >= TODAY_WEEK
        and _prev_fg_idx is None
    ):
        print(
            f"[FORCE START EARLIEST] {item}: plan_week W{plan_week} -> W{_yarn_earliest_plan_week}"
        )
        plan_week = _yarn_earliest_plan_week
    # Seed remaining_week_cap จาก booking สำหรับ carry-over ในสัปดาห์สุดท้ายของ old booking
    # ให้ new SO สามารถใช้ capacity ที่เหลืออยู่บน machine ของ old item ในสัปดาห์นั้นได้
    # สูตร: (MC_USE_CEIL - MC_USE) × working_days × daily_cap
    # = เครื่องที่ idle × วันทำงาน × cap ต่อวัน → qty ที่ new item สามารถผลิตได้ใน week เดียวกัน
    if rts_local_force:
        _bkr_data = rts_local_force.get("booking_remain_by_mc", {})
        _last_old = rts_local_force.get("last_old_by_mc", {})
        for _bkr_mc, _bkr_cap in _bkr_data.items():
            _bkr_w = _last_old.get(_bkr_mc)
            if _bkr_w is not None and _bkr_cap > 0:
                # ห้าม seed week เก่ากว่า TODAY_WEEK − carry/setup ไม่ได้ย้อนหลัง
                if _bkr_w < TODAY_WEEK:
                    continue
                # ห้าม seed ถ้า gap ระหว่าง booking week กับ plan_week เกิน SETUP_GAP_WEEK
                if plan_week is not None:
                    _bkr_plan_idx = week_index(plan_week)
                    _bkr_seed_idx = week_index(_bkr_w)
                    if _bkr_plan_idx is not None and _bkr_seed_idx is not None and (_bkr_plan_idx - _bkr_seed_idx) > SETUP_GAP_WEEK:
                        print(f"[BOOKING CARRY SKIP] {item} booking W{_bkr_w} → plan W{plan_week}: gap {_bkr_plan_idx - _bkr_seed_idx} > {SETUP_GAP_WEEK} weeks → ไม่ seed")
                        continue
                if (item, _bkr_w, _bkr_mc) not in _existing_item_week_mc:
                    _bkr_key = (_bkr_w, item, _bkr_mc)
                    if _bkr_key not in remaining_week_cap:
                        remaining_week_cap[_bkr_key] = _bkr_cap
                        remaining_week_cap_owner[_bkr_key] = None
                        print(f"[BOOKING CARRY] Seeded remaining_week_cap[({_bkr_w}, {item}, {_bkr_mc})] = {_bkr_cap:.2f} from booking")
    else:
        # Non-CORE items: seed จาก detail_mc โดยตรง (ไม่ผ่าน rts_local_force)
        if _bk_mc_grp:
            _dm_item = detail_mc[
                detail_mc["ITEM_CODE"].astype(str).str.upper().str.strip() == item_upper
            ]
            if not _dm_item.empty:
                for _dm_mc, _dm_grp in _dm_item.groupby("MC_GROUP"):
                    _dm_mc_str = str(_dm_mc).strip().upper()
                    # หา last active week
                    _dm_active = _dm_grp[
                        pd.to_numeric(_dm_grp["MC_USE_CEIL"], errors="coerce").fillna(0) > 0
                    ]
                    if _dm_active.empty:
                        continue
                    _dm_last_w = int(_dm_active["WEEK"].dropna().astype(int).max())
                    _dm_last_row = _dm_active[_dm_active["WEEK"] == _dm_last_w].iloc[-1]
                    _dm_mc_ceil = float(_dm_last_row.get("MC_USE_CEIL", 0) or 0)
                    _dm_kp_weight = float(_dm_last_row.get("KP_WEIGHT", 0) or 0)
                    _dm_wd = int(_dm_last_row.get("WORKING_DAY", 5) or 5)
                    _dm_gauge = str(_dm_last_row.get("GUAGE", "")).strip()
                    _dm_cap = _get_capacity_for_mc_group(item, _dm_mc_str, _dm_gauge)
                    # remaining = (MC_USE_CEIL × wd × cap) − KP_WEIGHT
                    _dm_total_cap = _dm_mc_ceil * _dm_wd * _dm_cap
                    if _dm_cap > 0 and _dm_mc_ceil > 0:
                        _dm_rem = max(0.0, _dm_total_cap - _dm_kp_weight)
                        if _dm_rem > 0 and (item, _dm_last_w, _dm_mc_str) not in _existing_item_week_mc:
                            # ห้าม seed week เก่ากว่า TODAY_WEEK − carry/setup ไม่ได้ย้อนหลัง
                            if _dm_last_w < TODAY_WEEK:
                                continue
                            # ห้าม seed ถ้า gap ระหว่าง booking week กับ plan_week เกิน SETUP_GAP_WEEK
                            if plan_week is not None:
                                _dm_plan_idx = week_index(plan_week)
                                _dm_seed_idx = week_index(_dm_last_w)
                                if _dm_plan_idx is not None and _dm_seed_idx is not None and (_dm_plan_idx - _dm_seed_idx) > SETUP_GAP_WEEK:
                                    print(f"[BOOKING CARRY SKIP] {item} booking W{_dm_last_w} → plan W{plan_week}: gap {_dm_plan_idx - _dm_seed_idx} > {SETUP_GAP_WEEK} weeks → ไม่ seed")
                                    continue
                            _dm_key = (_dm_last_w, item, _dm_mc_str)
                            if _dm_key not in remaining_week_cap:
                                remaining_week_cap[_dm_key] = _dm_rem
                                remaining_week_cap_owner[_dm_key] = None
                                print(f"[BOOKING CARRY] Seeded remaining_week_cap[({_dm_last_w}, {item}, {_dm_mc_str})] = {_dm_rem:.2f} from booking (non-CORE)")

    while qty_left > 0 and plan_week is not None:
        # 🔧 FIX: กัน floating-point residual qty ที่น้อยเกินไปจาก booking deduction
        # qty ที่น้อยกว่า 1.0 ไม่สามารถผลิตได้จริง และจะทำลาย booking carryover seed ของ order อื่น
        if qty_left < 1.0:
            break

        # ถ้า FG ใหม่ (SC/SO ใหม่) เริ่มใน week เดิมและมี cap เหลือ ให้ผลิตใน week เดิมจน cap หมดก่อนข้ามไป week ถัดไป
        _fill_last_week = None  # track week สุดท้ายที่ fill cross-SC
        while qty_left > 0 and ALLOW_SAME_ITEM_WEEK_CARRY:
            # ค้นหา remaining capacity สำหรับ ITEM เดียวกัน ในทุก week (เรียง week น้อยสุดก่อน)
            _found_rem_mc = None
            _found_rem_cap = 0
            _found_rem_week = None
            for _rk, _rv in sorted(remaining_week_cap.items(), key=lambda x: x[0][0]):
                if _rk[1] == item and _rv > 0:
                    # 🔧 FIX: ไม่ดึง cap ของ "future week" มาใช้ก่อนเวลา
                    # ให้เริ่มผลิตตาม plan_week ปัจจุบันก่อน แล้วค่อยใช้ carry เมื่อถึง week นั้นจริง
                    if plan_week is not None and _rk[0] > plan_week:
                        continue
                    # ห้ามใช้ week เก่ากว่า TODAY_WEEK − ไม่วางแผนย้อนหลัง
                    if _rk[0] < TODAY_WEEK:
                        remaining_week_cap.pop(_rk, None)
                        remaining_week_cap_owner.pop(_rk, None)
                        continue
                    # ข้าม week ที่เส้นด้ายยังไม่เข้า (< EARLIEST_PLAN_WEEK ของ order นี้)
                    # ไม่ pop เพราะ order อื่น (SC/SO อื่น) อาจมี EARLIEST_PLAN_WEEK ต่างกัน
                    if _yarn_earliest_plan_week and _rk[0] < _yarn_earliest_plan_week:
                        continue
                    _found_rem_week = _rk[0]
                    _found_rem_mc = _rk[2]
                    _found_rem_cap = _rv
                    break

            if _found_rem_mc is None or _found_rem_cap <= 0:
                break

            # ถ้า week เดียวกันมีแถว Item+Week+MC_GROUP อยู่แล้ว
            # ให้ไป merge ใน main planning loop แทน (ห้าม pop cap ทิ้ง)
            if (_found_rem_week, item, _found_rem_mc) in {(w, i, m) for (i, w, m) in _existing_item_week_mc}:
                print(
                    f"[DEBUG DUPLICATE] Cross-SC fill deferred merge: {item}+{_found_rem_week}+{_found_rem_mc} already planned"
                )
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
                remaining_week_cap_owner.pop(_rem_cap_key, None)
                break

            # ใช้ capacity ตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน
            _fill_gauge = _cap_row.iloc[0].get("GUAGE")
            _fill_daily_cap = _get_capacity_for_mc_group(item, _fill_mc_group, _fill_gauge)
            if _fill_daily_cap <= 0:
                # Fallback: use original capacity if new logic returns 0 (normalize 24→20hr)
                _fill_daily_cap = _normalize_capacity(item, _fill_mc_group, float(_cap_row.iloc[0]["CAP ทอ"]))
            _fill_rev_weight = get_revolution_weight_from_orders(item, _fill_mc_group)
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
            # 🔧 FIX: ถ้า remaining capacity มากกว่า 1 เครื่อง → คำนวณเครื่องจาก capacity
            # เพื่อรองรับกรณี freed capacity จากเครื่องที่ปล่อย (tail end)
            # ⚠️ ห้าม implied_mc เกินกว่าเครื่องที่ setup จริง (machines_in_use) เพราะ remaining cap
            # อาจมาจาก freed capacity ซึ่งยังนับเป็นเครื่องที่ active อยู่ ไม่ใช่เครื่องใหม่
            if _fill_daily_cap > 0 and _fill_actual_wd > 0:
                _mc_one_cap = _fill_actual_wd * _fill_daily_cap
                if _mc_one_cap > 0:
                    _implied_mc = int(_found_rem_cap / _mc_one_cap) + (1 if _found_rem_cap % _mc_one_cap > 0 else 0)
                    # cap ที่ machines_in_use จริง (ห้ามเกิน)
                    _actual_mc_in_use = machines_in_use.get(_fill_ck, _fill_avail_mc)
                    _implied_mc = min(_implied_mc, _actual_mc_in_use)
                    if _implied_mc > _fill_avail_mc:
                        _fill_avail_mc = _implied_mc
            _rem_cap = _found_rem_cap
            # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → หัก capacity ออก 1 วัน (สะสมในสัปดาห์เดียวกัน)
            if order_type == "YD-ORDERS" and sub_color:
                _fill_prev_color = last_sub_color.get(_fill_ck, "")
                if _fill_prev_color and _fill_prev_color != sub_color:
                    _wcs_fill_key = (_fill_ck, _fill_week)
                    _yd_week_color_setups[_wcs_fill_key] = _yd_week_color_setups.get(_wcs_fill_key, 0) + 1
                    _color_deduct = _fill_avail_mc * 1 * _fill_daily_cap  # 1 วัน × เครื่อง × cap (หักเต็มวัน ไม่ round ลง rev_weight)
                    _rem_cap = max(0, _rem_cap - _color_deduct)
                    print(f"[YD SUB_COLOR FILL] {item} W{_fill_week}: SUB_COLOR {_fill_prev_color}→{sub_color} หัก {_color_deduct:.0f} units (color setup #{_yd_week_color_setups[_wcs_fill_key]})")
            while qty_left > 0 and _rem_cap > 0:
                if _fill_rev_weight and _fill_rev_weight > 0:
                    _rem_batches = int(_rem_cap // _fill_rev_weight)
                    produce = min(qty_left, _rem_batches * _fill_rev_weight)
                    # ✅ ถ้า qty ที่เหลือหลัง rounding ≤ 1 พับ → ดูดเข้าสัปดาห์นี้เลย
                    if produce < qty_left and (qty_left - produce) <= _fill_rev_weight:
                        produce = qty_left
                else:
                    produce = min(qty_left, _rem_cap)
                if produce > 0:
                    plans.append({
                        "ITEM_CODE": item,
                        "SC_SO_NO": str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip(),
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
                        "MATERIAL_CONTENT": str(order.get("MATERIAL_CONTENT", "")).strip(),
                        "IS_CORE_ITEM": "CORE ITEM" if is_core_item else "",
                        "CUSTOMER": str(order.get("Customer", "")).strip(),
                        "PLAN_SOURCE": "NEW",
                        "LT_YARN": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_days(item),
                        "YARN_USED": _yarn_used_lookup.get(str(item).strip().upper(), ""),
                        "DATE_IN": order.get("DATE_IN"),
                        "EARLIEST_PLAN_WEEK": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_earliest_week(item, date_in=order.get("DATE_IN")),
                        "SUB_COLOR": sub_color,
                    })
                    qty_left -= produce
                    _rem_cap -= produce
                    # อัปเดต remaining cap หลังใช้งาน
                    _new_rem = max(0, _rem_cap)
                    if _new_rem > 0:
                        remaining_week_cap[_rem_cap_key] = _new_rem
                        # 🔧 FIX: ถ้า owner เดิมเป็น None (จาก booking seed) ให้คงไว้
                        # เพื่อไม่ให้ main loop คิดว่าเป็น new plan cap แล้ว override
                        if remaining_week_cap_owner.get(_rem_cap_key) is not None:
                            remaining_week_cap_owner[_rem_cap_key] = sc_so_no
                    else:
                        remaining_week_cap.pop(_rem_cap_key, None)
                        remaining_week_cap_owner.pop(_rem_cap_key, None)
                    # อัปเดต tracking สำหรับ cross-SC carryover
                    last_production[_fill_ck] = week_index(_fill_week)
                    machines_in_use[_fill_ck] = _fill_avail_mc
                    last_sc_so_no[_fill_ck] = sc_so_no
                    if order_type == "YD-ORDERS" and sub_color:
                        last_sub_color[_fill_ck] = sub_color
                    new_plan_started_items.add(_fill_ck)
                    # 🔧 FIX: บันทึก cross-SC fill เข้า _existing_item_week_mc ป้องกัน duplicate
                    _existing_item_week_mc.add((item, _fill_week, _fill_mc_group))
                    _produced_week = _fill_week
                    _fill_last_week = _fill_week
                else:
                    # cap เหลือน้อยเกินไป (rev_weight rounding) → mark week เต็ม (=0)
                    remaining_week_cap[_rem_cap_key] = 0
                    # 🔧 FIX: ถ้า owner เดิมเป็น None (จาก booking seed) ให้คงไว้
                    # เพื่อไม่ให้ main loop คิดว่าเป็น new plan cap แล้ว override ด้วย full capacity
                    if remaining_week_cap_owner.get(_rem_cap_key) is not None:
                        remaining_week_cap_owner[_rem_cap_key] = sc_so_no
                    break

            if qty_left <= 0:
                break

        # หลัง item-carry fill: ถ้า fill เกิดที่ plan_week เดิม → ขยับ plan_week ไปสัปดาห์ถัดไป
        # เพื่อให้ main loop ไม่ setup เครื่องใหม่ทับในสัปดาห์ที่ item อื่นยังผลิตอยู่
        if qty_left > 0 and _fill_last_week is not None and _fill_last_week == plan_week:
            _next_after_fill = next_week(_fill_last_week)
            if _next_after_fill is not None:
                print(f"[ITEM CARRY] {item}: advance plan_week W{plan_week} → W{_next_after_fill} (filled same week)")
                plan_week = _next_after_fill
        # ถ้า fill เกิดที่สัปดาห์ก่อนหน้า plan_week → ย้อนกลับมา plan ต่อที่สัปดาห์นั้น (เดิม)
        elif qty_left > 0 and _fill_last_week is not None and not _earliest_backshift_done:
            _fl_idx = week_index(_fill_last_week)
            _pw_idx = week_index(plan_week)
            if _fl_idx is not None and _pw_idx is not None and _fl_idx < _pw_idx:
                print(
                    f"[DEBUG BACKSHIFT] {item}: cross-SC filled W{_fill_last_week} while plan_week=W{plan_week} -> backshift to W{_fill_last_week}"
                )
                plan_week = _fill_last_week
                _earliest_backshift_done = True

        # 🔧 REMOVED: Aggressive week-skipping when cross-SC fill marked week as full
        # This was preventing main planning loop from scheduling items in that week
        # with different mc_groups. Cross-SC fill should not block main planning.
        # if qty_left > 0:
        #     _items_in_week = [
        #         (_rk, _rv) for _rk, _rv in remaining_week_cap.items()
        #         if _rk[0] == plan_week and _rk[1] == item
        #     ]
        #     if _items_in_week and all(_rv == 0 for _, _rv in _items_in_week):
        #         # week full → skip to next week
        #         plan_week = next_week(plan_week)
        #         if plan_week is None:
        #             break
        #         continue

        # ⚠️ ตรวจสอบ RDD ก่อนว่าทันหรือไม่
        _plan_idx = week_index(plan_week)
        past_rdd = bool(
            rdd_idx is not None and _plan_idx is not None and _plan_idx >= rdd_idx
        )
        if rdd_idx is not None and _plan_idx is not None and _plan_idx > rdd_idx:
            # urgent_mode = True  # DISABLED
            urgent_mode = False
        # ถ้ายังไม่ได้คำนวณ required_machines (เพราะตอนแรก avail=0 ทุก week)
        # ให้ลองคำนวณใหม่ด้วย plan_week ปัจจุบันที่มีเครื่องว่างจริง
        # 🔧 แก้ไข: คำนวณ required machines เสมอแม้ past RDD เพื่อให้มีข้อมูลสำหรับ planning
        print(f"[DEBUG CALC CHECK] required_machines_info={required_machines_info}, past_rdd={past_rdd}, rdd_idx={rdd_idx}, _plan_idx={_plan_idx}")
        if rdd_idx is not None and _plan_idx is not None:
            print(f"[DEBUG CALC CHECK] _plan_idx <= rdd_idx: {_plan_idx <= rdd_idx}")
        if (
            required_machines_info is None
            and rdd_idx is not None
            and _plan_idx is not None
        ):
            _locked_mc2 = locked_mc_group_for.get((sc_so_no, item))
            # ใช้ rdd_idx เป็น target เพราะต้องทอเสร็จก่อน RDD (TARGET_KNIT)
            target_knit_idx2 = rdd_idx
            _mc_r, _cap_r, _req_r, _feas_r, _gauge_r = calculate_required_machines(
                item,
                max(qty_left, _qty_for_machine_calc),
                plan_week,
                target_knit_idx2,  # TARGET_KNIT
                setup_days=order_setup_days,
                only_mc_group=_locked_mc2,
                order_type=order_type,
                sub_color=sub_color,
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
            print(f"[DEBUG RTS] Processing RTS for {item} in week {plan_week}")
            _rts_old_mc = rts_local_force.get("last_old_by_mc", {})
            if _rts_old_mc:
                # เลือก MC_GROUP ที่มี last week ล่าสุด
                _rts_mc = max(_rts_old_mc, key=_rts_old_mc.get)
                _rts_machines = rts_local_force.get("machines_by_mc", {}).get(
                    _rts_mc, 0
                )
                _rts_cap = rts_local_force.get("daily_cap_by_mc", {}).get(_rts_mc)
                print(f"[DEBUG RTS] RTS MC: {_rts_mc}, machines: {_rts_machines}")
                if _rts_machines > 0:
                    mc_group = _rts_mc
                    available_machines = _rts_machines
                    setup_needed = False
                    _sel_gauge = _item_mc_to_gauge.get(
                        (str(item).strip().upper(), str(_rts_mc).strip().upper()), None
                    )
                    # daily_capacity: ใช้ capacity ตาม logic ใหม่: สำหรับ SHARED_POOL_MAP ให้เลือกจาก cap ที่น้อยที่สุดในกลุ่มเดียวกัน
                    daily_capacity = _get_capacity_for_mc_group(item, _rts_mc, _sel_gauge)
                    if daily_capacity <= 0:
                        # Fallback: use original logic if new logic returns 0
                        _all_cap_rts = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                        if not _all_cap_rts.empty:
                            daily_capacity = _normalize_capacity(item, _rts_mc, float(_all_cap_rts["CAP ทอ"].min()))
                        elif _rts_cap and not pd.isna(_rts_cap) and float(_rts_cap) > 0:
                            daily_capacity = _normalize_capacity(item, _rts_mc, float(_rts_cap))
                        else:
                            _rts_cap_row = item_cap_data[
                                (item_cap_data["ITEM_CODE"] == item)
                                & (item_cap_data["MC_GROUP"] == _rts_mc)
                            ]
                            if not _rts_cap_row.empty:
                                daily_capacity = _normalize_capacity(
                                    item, _rts_mc,
                                    float(_rts_cap_row.iloc[0].get("CAP ทอ", 0) or 0)
                                )
        # คำนวณ _req_feasible ก่อนเพื่อส่งเข้า get_best_machine_for_item
        _req_feasible = (
            required_machines_info[3]
            if required_machines_info and len(required_machines_info) > 3
            else True
        )
        # 🔧 แก้ไข: ให้ Load Balancing ทำงานทุกครั้ง ไม่ใช่แค่ครั้งแรก
        # ถ้ามี required_machines_info ให้ใช้ Load Balancing ทุกครั้งเพื่อจัดการ Gradual Increase
        print(f"[DEBUG LB CHECK] {item} week {plan_week}: required_machines_info={required_machines_info}, USE_LOAD_BALANCING={USE_LOAD_BALANCING}")
        print(f"[DEBUG LB CHECK] {item} week {plan_week}: mc_group={mc_group}, rts_local_force={bool(rts_local_force)}")
        
        # ⚠️ ตรวจสอบว่าเป็น carryover หรือไม่ก่อนเรียก Load Balancing
        # ถ้าเป็น carryover → ใช้เครื่องเดิมโดยไม่ต้องเช็ค available_machines
        # ต้องหา mc_group จาก last_production ก่อน เพราะ mc_group อาจยัง None
        # 🔧 FIX: validate ว่า _prev_mc_group ต้องตรงกับ MC_GROUP จาก order หรือมีใน item_cap_data
        #   เพื่อป้องกัน carryover จาก MC_GROUP ผิดที่ค้างอยู่ใน old plan (เช่น SKP แทน SKPTA)
        _valid_mc_groups_for_item = set(
            str(r).strip().upper()
            for r in item_cap_data.loc[
                item_cap_data["ITEM_CODE"] == item, "MC_GROUP"
            ]
        )
        _prev_mc_key = None
        _prev_mc_group = None
        for _key, _week_idx in last_production.items():
            if _key[0] == item and _week_idx is not None:
                # 🔧 FIX: ข้าม MC_GROUP ที่ไม่ตรงกับ order และไม่มีใน item_cap_data
                _candidate_mc = _key[1]
                if _candidate_mc != _ord_mc_grp and _candidate_mc not in _valid_mc_groups_for_item:
                    continue
                current_week_idx = week_index(plan_week)
                _gap_week = (
                    current_week_idx - _week_idx
                    if current_week_idx is not None
                    else None
                )
                # ต้องอยู่ภายใน SETUP_GAP_WEEK ของ plan_week
                # AND ต้องไม่เก่ากว่า TODAY_WEEK − SETUP_GAP_WEEK (ไม่ carry จาก booking เก่าเกินไป)
                # AND ต้องเป็นสัปดาห์ก่อนหน้าเท่านั้น (ห้ามดึงสัปดาห์อนาคตมาเป็น carry)
                if (
                    current_week_idx is not None
                    and _gap_week is not None
                    and _gap_week >= 0
                    and _gap_week <= SETUP_GAP_WEEK
                    and _week_idx >= TODAY_IDX
                ):
                    _prev_mc_key = _key
                    _prev_mc_group = _key[1]
                    break
        
        _is_carryover = _prev_mc_key is not None and _prev_mc_group is not None
        
        if item == "FD6PRTPG99A0" or item == "FD1BASFZ15/1A0" or item == "FD4DRTIT49/08A0":
            print(f"[DEBUG CARRY CHECK] Week {plan_week}: _is_carryover={_is_carryover}, _prev_mc_group={_prev_mc_group}, mc_group={mc_group}")
            print(f"[DEBUG CARRY CHECK] Week {plan_week}: qty_left={qty_left}, pending_plan={order.get('PENDING_PLAN', 0)}")
        
        # 🔧 แก้ไข: แยกกรณี carryover ปกติ vs carryover ที่ต้องการเพิ่มเครื่อง
        # ถ้าเป็น carryover แต่มีเครื่องว่างมากและ order ยังเหลือเยอะ → ให้ Load Balancing ทำงาน
        # ถ้าเป็น carryover ปกติ → ใช้เครื่องเดิมต่อไป
        
        # ตรวจสอบว่ามีเครื่องว่างพอที่จะเพิ่มหรือไม่
        _has_capacity_for_increase = False
        if _is_carryover and _prev_mc_group:
            # หา gauge จาก last_production key
            _carry_gauge = _prev_mc_key[2] if len(_prev_mc_key) > 2 else None
            _actual_remain = get_actual_mc_remain(_prev_mc_group, plan_week, gauge=_carry_gauge)
            _current_machines = machines_in_use.get(_prev_mc_key, 1)
            _has_capacity_for_increase = _actual_remain > _current_machines
            if item == "FD6PRTPG99A0" or item == "FD4DRTIT49/08A0":
                print(f"[DEBUG CAPACITY] {_prev_mc_group} week {plan_week}: gauge={_carry_gauge}, actual_remain={_actual_remain}, current_machines={_current_machines}, can_increase={_has_capacity_for_increase}")
        
        # ตรวจสอบว่า order เหลือเยอะพอที่จะเพิ่มเครื่องหรือไม่
        _needs_increase = False
        if _is_carryover and qty_left > 0:
            # ประมาณว่าถ้าเหลือมากกว่า 2 สัปดาห์ อาจต้องการเพิ่มเครื่อง
            _cap_row = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item)
                & (item_cap_data["MC_GROUP"] == _prev_mc_group)
            ]
            if not _cap_row.empty:
                _ni_gauge = _cap_row.iloc[0].get("GUAGE")
                _daily_cap = _get_capacity_for_mc_group(item, _prev_mc_group, _ni_gauge)
                if _daily_cap <= 0:
                    _daily_cap = _normalize_capacity(item, _prev_mc_group, float(_cap_row.iloc[0]["CAP ทอ"]))
                if _daily_cap > 0:
                    # 🔧 FIX: หัก production ที่ผลิตไปแล้วออกจาก _qty_for_machine_calc
                    # รวมทั้ง SC อื่นของ item เดียวกันที่วางแผนเสร็จแล้ว (_item_cumulative_planned)
                    # เพื่อไม่ให้นับ demand ซ้ำจาก SC ที่ planned ไปแล้ว
                    _already_produced_this_sc = order_qty - qty_left
                    _already_produced_other_sc = _item_cumulative_planned.get(item, 0)
                    _already_produced = _already_produced_this_sc + _already_produced_other_sc
                    _adjusted_machine_calc = max(0, _qty_for_machine_calc - _already_produced)
                    _total_item_demand = max(qty_left, _adjusted_machine_calc)
                    _weeks_remaining = _total_item_demand / (_current_machines * _daily_cap * 5)
                    _needs_increase = _weeks_remaining > 2.0  # เหลือมากกว่า 2 สัปดาห์
        
        # 🔧 แก้ไข: แยก 3 กรณี
        # 1. Carryover ที่มีเครื่องว่างและต้องการเพิ่ม → ส่งไป Load Balancing
        # 2. Carryover ที่ไม่มีเครื่องว่างหรือไม่ต้องการเพิ่ม → ใช้เครื่องเดิมต่อไป
        # 3. ไม่ใช่ carryover → ส่งไป Load Balancing ปกติ
        
        should_check_increase = _is_carryover and _has_capacity_for_increase and _needs_increase
        
        if item == "FD6PRTPG99A0" or item == "FD1BASFZ15/1A0" or item == "FD4DRTIT49/08A0":
            print(f"[DEBUG CARRY CHECK] Week {plan_week}: _is_carryover={_is_carryover}, _has_capacity_for_increase={_has_capacity_for_increase}, _needs_increase={_needs_increase}")
            print(f"[DEBUG CARRY CHECK] Week {plan_week}: should_check_increase={should_check_increase}, qty_left={qty_left}")
        
        _plan_week_idx = week_index(plan_week)
        _earliest_week_idx = (
            week_index(_yarn_earliest_plan_week)
            if _yarn_earliest_plan_week is not None
            else None
        )

        # ❗ ตรวจสอบ EARLIEST_PLAN_WEEK ก่อน carryover: ถ้าเส้นด้ายยังไม่เข้าให้ยกเลิก carryover
        # เพราะแม้เครื่องพร้อม แต่ถ้า yarn ยังไม่มาก็ผลิตไม่ได้อยู่ดี
        if (
            _is_carryover
            and _plan_week_idx is not None
            and _earliest_week_idx is not None
            and _plan_week_idx < _earliest_week_idx
        ):
            print(f"⚠️ [CARRY SKIP] {item} week {plan_week} < EARLIEST_PLAN_WEEK {_yarn_earliest_plan_week} → ยกเลิก carryover (yarn ยังไม่เข้า)")
            _is_carryover = False
            should_check_increase = False

        # ❗ Priority rule: ถ้า carry ได้ แต่มี EARLIEST_PLAN_WEEK ที่เร็วกว่าปัจจุบันและไม่ย้อนหลัง
        # ให้ setup ที่ earliest ก่อนเสมอ แทนการลาก carry ต่อใน week หลัง
        if (
            _is_carryover
            and not _earliest_backshift_done
            and _plan_week_idx is not None
            and _earliest_week_idx is not None
            and _plan_week_idx > _earliest_week_idx
            and _earliest_week_idx >= TODAY_IDX
            and _prev_mc_group
        ):
            _early_plan_key = (item, _yarn_earliest_plan_week, _prev_mc_group)
            if _early_plan_key in _existing_item_week_mc:
                print(
                    f"⚠️ [SETUP EARLY PRIORITY] {item}: earliest week {_yarn_earliest_plan_week} ถูกวางแผนแล้ว "
                    f"({item}+{_yarn_earliest_plan_week}+{_prev_mc_group}) → คง carry week {plan_week}"
                )
            else:
                print(
                    f"⚠️ [SETUP EARLY PRIORITY] {item}: carry week {plan_week} > EARLIEST_PLAN_WEEK "
                    f"{_yarn_earliest_plan_week} → ย้ายไป setup ที่ earliest ก่อน"
                )
                plan_week = _yarn_earliest_plan_week
                start_idx = _earliest_week_idx
                _is_carryover = False
                should_check_increase = False
            _earliest_backshift_done = True

        # ❗ ถ้า plan_week > EARLIEST_PLAN_WEEK และเวลาจาก carry week ไม่พอทัน RDD
        # → setup ที่ EARLIEST_PLAN_WEEK แทนเพื่อให้ได้สัปดาห์ผลิตเพิ่มขึ้น
        if (
            _is_carryover
            and not _earliest_backshift_done
            and _plan_week_idx is not None
            and _earliest_week_idx is not None
            and _plan_week_idx > _earliest_week_idx
            and rdd_idx is not None
            and _prev_mc_group
        ):
            _plan_idx_est = _plan_week_idx
            _earliest_idx_est = _earliest_week_idx
            if _plan_idx_est is not None and _earliest_idx_est is not None and _plan_idx_est > _earliest_idx_est:
                _cap_row_est = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == _prev_mc_group)
                ]
                if not _cap_row_est.empty:
                    _g_est = str(_cap_row_est.iloc[0].get("GUAGE", "")).strip()
                    _dc_est = _get_capacity_for_mc_group(item, _prev_mc_group, _g_est)
                    if _dc_est <= 0:
                        _dc_est = _normalize_capacity(item, _prev_mc_group, float(_cap_row_est.iloc[0]["CAP ทอ"]))
                    _mc_est = machines_in_use.get(_prev_mc_key, 1)
                    if _dc_est > 0 and _mc_est > 0:
                        _weeks_needed_est = qty_left / (_mc_est * _dc_est * 5)
                        _weeks_from_carry = rdd_idx - _plan_idx_est
                        _weeks_from_earliest = rdd_idx - _earliest_idx_est
                        # ถ้า carry week ไม่ทัน RDD แต่ EARLIEST_PLAN_WEEK ทัน → setup ก่อน
                        if _weeks_from_carry < _weeks_needed_est and _weeks_from_earliest >= _weeks_needed_est:
                            _early_plan_key = (item, _yarn_earliest_plan_week, _prev_mc_group)
                            if _early_plan_key in _existing_item_week_mc:
                                print(
                                    f"⚠️ [SETUP EARLY] {item}: earliest week {_yarn_earliest_plan_week} ถูกวางแผนแล้ว "
                                    f"({item}+{_yarn_earliest_plan_week}+{_prev_mc_group}) → ไม่ย้อนสัปดาห์เพื่อกัน loop"
                                )
                            else:
                                print(f"⚠️ [SETUP EARLY] {item}: carry week {plan_week} ไม่ทัน RDD "
                                      f"(ต้อง {_weeks_needed_est:.1f}w, carry มี {_weeks_from_carry:.1f}w, earliest มี {_weeks_from_earliest:.1f}w) "
                                      f"→ setup ที่ EARLIEST_PLAN_WEEK {_yarn_earliest_plan_week} แทน")
                                plan_week = _yarn_earliest_plan_week
                                start_idx = _earliest_idx_est
                                _is_carryover = False
                                should_check_increase = False
                                _earliest_backshift_done = True

        # 🔧 สำคัญ: ถ้าเป็น carryover ให้จัดการก่อน mc_group check
        # กรณีที่ 1 & 2: Carryover ทุกกรณีที่ไม่ต้องการเพิ่มเครื่อง (ไม่มีเครื่องว่างหรือไม่จำเป็น)
        if _is_carryover and not should_check_increase:
            # Carryover แบบไม่ต้องเพิ่มเครื่อง: ใช้เครื่องเดิมโดยไม่ต้องเช็ค available_machines
            mc_group = _prev_mc_group
            available_machines = machines_in_use.get(_prev_mc_key, 1)
            setup_needed = False
            # ใช้ daily_capacity เดิม
            _cap_row = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item)
                & (item_cap_data["MC_GROUP"] == mc_group)
            ]
            if not _cap_row.empty:
                _sel_gauge = _cap_row.iloc[0].get("GUAGE")
                daily_capacity = _get_capacity_for_mc_group(item, mc_group, _sel_gauge)
                if daily_capacity <= 0:
                    daily_capacity = _normalize_capacity(item, mc_group, float(_cap_row.iloc[0]["CAP ทอ"]))
            else:
                # Fallback: หา capacity จาก item ทั่วไป
                _all_cap = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                if not _all_cap.empty:
                    _sel_gauge = _all_cap.iloc[0].get("GUAGE")
                    daily_capacity = _get_capacity_for_mc_group(item, mc_group, _sel_gauge)
                    if daily_capacity is None or daily_capacity <= 0:
                        daily_capacity = _normalize_capacity(item, mc_group, float(_all_cap["CAP ทอ"].min()))
                else:
                    # ไม่มี capacity เลย → ข้ามไป
                    print(f"[WARNING] No capacity found for {item} in {mc_group}, skipping to next week")
                    plan_week = next_week(plan_week)
                    continue
            
            if item == "FD1BASFZ15/1A0":
                print(f"[DEBUG CARRY] Week {plan_week}: Using carryover - mc_group={mc_group}, machines={available_machines}, daily_cap={daily_capacity}")
        elif USE_LOAD_BALANCING or should_check_increase:
            print(f"[DEBUG LB] Using Load Balancing for {item} in week {plan_week}")
            # ดึง daily_capacity จาก required_machines_info ถ้ามี
            _input_daily_cap = required_machines_info[1] if required_machines_info and len(required_machines_info) > 1 else 0
            # ส่งข้อมูลเครื่องปัจจุบันสำหรับกรณี carryover ที่ต้องการเพิ่ม
            _current_machines = machines_in_use.get(_prev_mc_key, 1) if _prev_mc_key else 1
            
            # 🔧 ตรวจสอบว่าเป็นการใช้ครั้งแรกของ MC_GROUP นี้หรือไม่
            is_first_time_use = True
            current_week_idx = week_index(plan_week)
            current_week_num = int(current_week_idx) if isinstance(current_week_idx, str) else current_week_idx
            _candidate_mc_for_first_use = (
                _prev_mc_group
                if (_is_carryover and _prev_mc_group)
                else (
                    required_machines_info[0]
                    if required_machines_info and len(required_machines_info) > 0
                    else None
                )
            )
            for (week_idx, mc_grp), total_machines in weekly_mc_usage.items():
                try:
                    week_num = int(week_idx) if isinstance(week_idx, str) else week_idx
                    if _candidate_mc_for_first_use and mc_grp != _candidate_mc_for_first_use:
                        continue
                    if week_num < current_week_num:
                        # มีการใช้ MC_GROUP นี้มาก่อนในสัปดาห์ที่ผ่านมา
                        is_first_time_use = False
                        break
                except (ValueError, TypeError):
                    continue
            # ถ้าเป็น carryover ให้ใช้ daily_capacity จากเครื่องปัจจุบัน
            if _is_carryover and _input_daily_cap == 0:
                _carry_cap_row = item_cap_data[
                    (item_cap_data["ITEM_CODE"] == item)
                    & (item_cap_data["MC_GROUP"] == _prev_mc_group)
                ]
                if not _carry_cap_row.empty:
                    _carry_gauge = _carry_cap_row.iloc[0].get("GUAGE")
                    _input_daily_cap = _get_capacity_for_mc_group(item, _prev_mc_group, _carry_gauge)
                    if _input_daily_cap <= 0:
                        _input_daily_cap = _normalize_capacity(item, _prev_mc_group, float(_carry_cap_row.iloc[0]["CAP ทอ"]))
                print(f"[DEBUG CARRY] Using daily_capacity from carryover: {_input_daily_cap}")
            
            # 🔧 สำคัญ: สำหรับ carryover ที่ต้องการเพิ่มเครื่อง ต้องรักษา mc_group เดิม
            if _is_carryover and should_check_increase:
                # บังคับให้ใช้ mc_group เดิม ไม่ให้ Load Balancing เปลี่ยน
                print(f"[DEBUG CARRY] Force using original mc_group: {_prev_mc_group} for carryover increase")
                # ส่ง mc_group เดิมไปให้ Load Balancing และบอกว่าเป็น carryover
                # 🔧 FIX: ใช้ _total_item_demand แทน qty_left เพื่อรวม demand จาก FG ถัดไปของ item เดียวกัน
                # ป้องกันกรณีที่ FG ปัจจุบันเหลือน้อย แต่ยังมี FG อื่นรอผลิต → ควรเพิ่มเครื่องได้
                mc_group_result, daily_capacity, setup_needed, available_machines, _sel_gauge = (
                    get_load_balanced_machine(
                        item,
                        plan_week,
                        last_production,
                        required_machines_info,
                        urgent_mode,
                        past_rdd,
                        force_max_mc=(not _req_feasible and not past_rdd and not is_first_time_use),
                        qty_left=_total_item_demand,
                        daily_capacity=_input_daily_cap,
                        progressive_plan=progressive_plan,
                        current_machines=_current_machines,
                    )
                )
                # รักษา mc_group เดิมเสมอ
                mc_group = _prev_mc_group
                # ถ้า Load Balancing คืนค่า machines มา ให้ใช้ค่านั้น
                if available_machines is not None and available_machines > _current_machines:
                    print(f"[DEBUG CARRY] Load Balancing approved increase: {_current_machines} → {available_machines} machines")
                else:
                    print(f"[DEBUG CARRY] Load Balancing no increase: keep {_current_machines} machines")
                    available_machines = _current_machines
            else:
                mc_group, daily_capacity, setup_needed, available_machines, _sel_gauge = (
                    get_load_balanced_machine(
                        item,
                        plan_week,
                        last_production,
                        required_machines_info,
                        urgent_mode,
                        past_rdd,
                        force_max_mc=(not _req_feasible and not past_rdd and not is_first_time_use),
                        qty_left=qty_left,
                        daily_capacity=_input_daily_cap,
                        progressive_plan=progressive_plan,
                        current_machines=_current_machines,
                    )
                )
        elif mc_group is None:
            # ดึง daily_capacity จาก required_machines_info ถ้ามี
            _input_daily_cap = required_machines_info[1] if required_machines_info and len(required_machines_info) > 1 else 0
            _current_machines = machines_in_use.get(_prev_mc_key, 1) if _prev_mc_key else 1
            mc_group, daily_capacity, setup_needed, available_machines, _sel_gauge = (
                get_load_balanced_machine(
                    item,
                    plan_week,
                    last_production,
                    required_machines_info,
                    urgent_mode,
                    past_rdd,
                    force_max_mc=(not _req_feasible and not past_rdd and not is_first_time_use),
                    qty_left=qty_left,
                    daily_capacity=_input_daily_cap,
                    progressive_plan=progressive_plan,
                    current_machines=_current_machines,
                )
            )
        if mc_group is None:
            plan_week = next_week(plan_week)
            continue

        # ถ้า mc_group+gauge อยู่ใน MC_GROUP_REDIRECT → เปลี่ยนไปใช้ target แทนเสมอ
        # เช่น SKP 20 → FA 20 (อ้อมน้อย) เพื่อให้วางแผนและหักเครื่องถูกที่
        _redir_mc, _redir_gauge = _apply_mc_redirect(mc_group, _sel_gauge)
        if _redir_mc != mc_group:
            print(f"[MC_REDIRECT] {mc_group}/{_sel_gauge} → {_redir_mc}/{_redir_gauge} for item {item} week {plan_week}")
            mc_group = _redir_mc
            _sel_gauge = _redir_gauge

        # Fallback: ถ้า daily_capacity ยัง None ให้หาจาก item_cap_data
        if daily_capacity is None or daily_capacity <= 0:
            _fallback_cap_row = item_cap_data[
                (item_cap_data["ITEM_CODE"] == item)
                & (item_cap_data["MC_GROUP"] == mc_group)
            ]
            if not _fallback_cap_row.empty:
                _sel_gauge = _fallback_cap_row.iloc[0].get("GUAGE")
                daily_capacity = _get_capacity_for_mc_group(item, mc_group, _sel_gauge)
                if daily_capacity is None or daily_capacity <= 0:
                    daily_capacity = _normalize_capacity(item, mc_group, float(_fallback_cap_row.iloc[0]["CAP ทอ"]))
            else:
                # หาจาก item ทั่วไป
                _fallback_all = item_cap_data[item_cap_data["ITEM_CODE"] == item]
                if not _fallback_all.empty:
                    daily_capacity = _normalize_capacity(item, mc_group, float(_fallback_all["CAP ทอ"].min()))
                else:
                    print(f"[WARNING] No capacity found for {item} in {mc_group}, skipping to next week")
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
        # ถ้า plan_week เกิน target แล้ว → ยังสามารถเพิ่มเครื่องได้ (ไม่ cap ที่ required_mc)
        
        # 🔧 แก้ไข: บังคับกฎ "สัปดาห์แรกไม่เกิน 2 เครื่อง" เสมอ
        # ตรวจสอบว่าเป็นการใช้ครั้งแรกของ MC_GROUP นี้หรือไม่
        _is_first_week = True
        _current_week_idx = week_index(plan_week)
        _current_week_num = int(_current_week_idx) if isinstance(_current_week_idx, str) else _current_week_idx
        for (week_idx, mc_grp), total_machines in weekly_mc_usage.items():
            try:
                week_num = int(week_idx) if isinstance(week_idx, str) else week_idx
                if mc_grp == mc_group and week_num < _current_week_num:
                    _is_first_week = False
                    break
            except (ValueError, TypeError):
                continue
        
        # ถ้าเป็นสัปดาห์แรก -> ใช้เครื่องจริง (carry) + ที่ setup ได้สูงสุด 2 เครื่อง
        if _is_first_week:
            # 🔧 FIX: นับ carry เฉพาะจาก item ที่เคยผลิตใน new plan บน MC_GROUP นี้เท่านั้น
            # booking เก่าไม่นับเป็น carry (ต้อง setup ใหม่)
            _fwl_key = _resolve_carry_key(item, mc_group, _sel_gauge)
            if _has_item_mc_key(new_plan_started_items, _fwl_key):
                carry_machines = machines_in_use.get(_fwl_key, 0)
            else:
                carry_machines = 0
            carry_machines = max(0, int(carry_machines) if carry_machines else 0)
            # สูงสุด = carry + 2 setup
            first_week_limit = carry_machines + 2
            if available_machines > first_week_limit:
                print(f"🔒 ENFORCING First week limit for {mc_group} in week {plan_week}: carry={carry_machines} + setup<=2 = {first_week_limit}, reducing from {available_machines} to {first_week_limit} machines")
                available_machines = first_week_limit
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
        rev_weight = get_revolution_weight_from_orders(item, mc_group)
        # กำหนด setup days ตาม MATERIAL_CONTENT และ YARN_ITEM
        item_material_content = str(order.get("MATERIAL_CONTENT", "")).strip()
        item_setup_days = get_setup_days_for_item(item, item_material_content)
        # ถ้าเป็น urgent หรือใกล้ RDD ให้ใช้ความสามารถสูงสุด
        # urgent_mode disabled - always use normal capacity
        if False:  # urgent_mode or (rdd_idx is not None and _plan_idx is not None and _plan_idx >= rdd_idx - 1)
            # ใช้วันทำงานตามที่โรงงานกำหนด (ไม่เปลี่ยนแปลง)
            # urgent mode ไม่สามารถเพิ่มวันทำงานเกินที่โรงงานเปิดได้
            pass

        # ตรวจสอบว่าสัปดาห์นี้เคยใช้ setup ไปแล้วหรือไม่
        week_key = (plan_week, mc_group)
        factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week)
        # แยกเครื่อง carry-over (ไม่ต้อง setup) vs เครื่องใหม่ (ต้อง setup)
        mc_key = _resolve_carry_key(item, mc_group, _sel_gauge)
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
                    # Recalculate working days for updated plan_week
                    working_days = get_working_days_in_week(plan_week)
                    factory_working_days = get_working_days_by_factory(mc_group, available_machines, week=plan_week)
                    if plan_week == 17:
                        actual_working_days = factory_working_days
                    else:
                        actual_working_days = min(len(working_days), factory_working_days)
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
            # Item carry: FG ถัดไปของ item เดียวกัน (คนละ SC/FG) → carry เครื่องต่อ ไม่ต้อง setup ใหม่
            # ทำให้ SC ที่มี TARGET_KNIT ช้ากว่า รอ carry ต่อจาก SC ที่เร็วกว่า
            if not same_order:
                _item_prev_m = machines_in_use.get(mc_key, 0)
                _item_prev_widx = last_production.get(mc_key)
                if (
                    _item_prev_m > 0
                    and _item_prev_widx is not None
                    and current_week_idx is not None
                    and (current_week_idx - _item_prev_widx) <= SETUP_GAP_WEEK
                ):
                    same_order = True  # item-level carry: inherit machines จาก FG ก่อนหน้า
        _is_same_sc = last_sc_so_no.get(mc_key) == sc_so_no
        # Item carry: ถ้า item+mc กำลังวิ่งอยู่ใน future week (prev_week_idx > current_week_idx)
        # หมายความว่า SC อื่นของ item เดียวกันยังผลิตอยู่ → ดัน plan_week ไปต่อท้าย แล้ว carry
        if (
            prev_week_idx is not None
            and current_week_idx is not None
            and prev_week_idx > current_week_idx
            and _has_item_mc_key(new_plan_started_items, mc_key)
        ):
            # เครื่องยังวิ่งอยู่ใน future → ข้ามไป week ถัดจาก week สุดท้ายที่ผลิต
            _after_prev = None
            _prev_cal_week = int(calendar_week.iloc[prev_week_idx]["WEEK"]) if prev_week_idx < len(calendar_week) else None
            if _prev_cal_week is not None:
                _after_prev = _prev_cal_week  # เริ่มที่ week เดียวกับที่ SC ก่อนหน้าจบ → carry เข้าได้เลย ไม่ลดเครื่อง
            if _after_prev is not None:
                print(f"[ITEM CARRY WAIT] {item} SC {sc_so_no}: machine busy until W{_prev_cal_week} → advance plan_week W{plan_week}→W{_after_prev} (carry in same week)")
                plan_week = _after_prev
                current_week_idx = week_index(plan_week)
                # 🔧 FIX: After advancing plan_week, this is now a carryover situation.
                # available_machines was set by LB for the ORIGINAL plan_week (possibly first-time use)
                # but after ITEM CARRY WAIT it should respect existing carry machines.
                # ป้องกัน LB สั่งเพิ่มเครื่องทั้งที่ carry เดิมเพียงพอผลิตได้ใน 1-2 weeks ถัดไป
                _carry_mc_after_wait = machines_in_use.get(mc_key, 0)
                if _carry_mc_after_wait > 0 and available_machines > _carry_mc_after_wait:
                    print(f"[ITEM CARRY WAIT] Capping available_machines {available_machines} → {_carry_mc_after_wait} (existing carry machines)")
                    available_machines = _carry_mc_after_wait
            same_order = True  # carry จาก week สุดท้ายของ SC ก่อนหน้า

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
        # ยกเว้น: ถ้ามีข้อมูลจาก booking จริง (detail_mc) → อนุญาต carryover จาก booking ได้
        if not _has_item_mc_key(new_plan_started_items, mc_key) and not _has_item_mc_key(booking_production_keys, mc_key):
            is_continuing = False
        if is_continuing:
            # เครื่อง carry ต่อใช้ได้เต็ม prev_machines (ไม่ต้องเช็คเครื่องว่าง)
            # เพราะเป็นเครื่องที่ใช้อยู่แล้ว ไม่ใช่เครื่องใหม่
            carryover_mc = prev_machines
            # เครื่องใหม่ = available_machines - carryover (อาจเป็น 0 หรือติดลบ)
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

        # YD-ORDERS: คำนวณ SUB_COLOR setup days ก่อน forward sim
        _yd_sub_color_setup = 0
        if order_type == "YD-ORDERS" and carryover_mc > 0:
            _prev_sub_color = last_sub_color.get(mc_key, "")
            if _prev_sub_color and sub_color and _prev_sub_color != sub_color:
                _wcs_key = (mc_key, plan_week)
                _yd_week_color_setups[_wcs_key] = _yd_week_color_setups.get(_wcs_key, 0) + 1
                _yd_sub_color_setup = 1  # หัก 1 วันต่อครั้งที่เปลี่ยนสี (ไม่ใช้ cumulative เพราะ remaining cap หักไปแล้วจากครั้งก่อน)

        def _forward_sim(carry, new, q_left):
            q = q_left
            wk = plan_week
            first = True
            _rw = rev_weight or 0
            # เมื่อ plan_week เลย rdd แล้ว (past_rdd) → ไม่ตัด sim ที่ rdd_idx
            # เพราะต้องจำลองต่อเพื่อเช็คว่า carry-only เพียงพอผลิตให้เสร็จหรือไม่
            # ป้องกัน setup เครื่องใหม่ที่ใช้แค่ 1 week แล้วหยุด
            _is_past_rdd = (
                rdd_idx is not None
                and current_week_idx is not None
                and current_week_idx > rdd_idx
            )
            _sim_count = 0
            while wk is not None and q > _rw:
                w_idx_sim = week_index(wk)
                if w_idx_sim is None:
                    break
                # ยังไม่เลย rdd → หยุดจำลองที่ rdd เหมือนเดิม
                if not _is_past_rdd and rdd_idx is not None and w_idx_sim > rdd_idx:
                    break
                # เลย rdd แล้ว → จำกัดจำลอง 2 สัปดาห์
                # setup เครื่องใหม่ที่ใช้ < 2 สัปดาห์ไม่คุ้ม setup days
                # ถ้า carry-only ผลิตครบภายใน 2 สัปดาห์ → ไม่ต้อง setup เพิ่ม
                if _is_past_rdd and _sim_count >= 2:
                    break

                cal = len(get_working_days_in_week(wk))
                fac = get_working_days_by_factory(mc_group, carry + new, week=wk)
                wd = min(cal, fac)
                if first:
                    # YD-ORDERS: SUB_COLOR เปลี่ยน → carryover ต้องเสีย 1 วัน setup
                    _carry_wd = max(0, wd - _yd_sub_color_setup) if carry > 0 else wd
                    c_prod = carry * _carry_wd * daily_capacity
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
                _sim_count += 1
            return q  # <= rev_weight หมายถึงทัน (ไม่เกิน 1 พับ ถือว่าครบ)

        if rdd_idx is not None and not rts_local_force and new_mc > 0:
            _rw_tol = rev_weight or 0
            # ใช้ demand ระดับ item (ทุก FG ของ item) เพื่อคงเครื่องไว้เมื่อยังมี FG เหลือของ item
            _fwd_qty = _qty_for_machine_calc if _qty_for_machine_calc > qty_left else qty_left
            if carryover_mc > 0 and _forward_sim(carryover_mc, 0, _fwd_qty) <= _rw_tol:
                # carryover เพียงพอทัน → ไม่ต้อง setup เพิ่ม
                new_mc = 0
            else:
                # หา new_mc น้อยสุดที่ทัน (จาก carryover + new)
                found_n = new_mc  # fallback = ทั้งหมด
                for try_n in range(1, new_mc + 1):
                    if _forward_sim(carryover_mc, try_n, _fwd_qty) <= _rw_tol:
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
        # ===== Hard cap: เครื่องใหม่ (new setup) ≤ MAX_NEW_SETUP_MC =====
        # carry-over ไม่นับ — เพิ่มใหม่ได้ไม่เกิน 2 เครื่องเสมอ
        if new_mc > MAX_NEW_SETUP_MC:
            new_mc = MAX_NEW_SETUP_MC
        available_machines = carryover_mc + new_mc
        # ใช้ actual_working_days (หักวันหยุดแล้ว) แทน factory_working_days แบบ static
        prod_days_old = actual_working_days  # เครื่อง carry-over ผลิตตามวันเปิดจริง
        # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → carryover machines เสีย 1 วัน setup จากที่เหลือ
        if _yd_sub_color_setup > 0:
            prod_days_old = max(0, prod_days_old - 1)
            _total_color_setups = _yd_week_color_setups.get((mc_key, plan_week), 0)
            print(f"[YD SUB_COLOR] {item} W{plan_week}: SUB_COLOR เปลี่ยนครั้งที่ {_total_color_setups} → prod_days_old={prod_days_old} (หัก 1 วันจากที่เหลือ)")
        # เครื่องใหม่ (new_mc) ต้อง setup เสมอ แม้ item จะ warm บนเครื่องเดิม (setup_needed=False)
        # setup_needed=False หมายถึงเครื่องที่วิ่งอยู่แล้ว ไม่ใช่เครื่องที่เพิ่งเพิ่มมา
        prod_days_new = max(0, actual_working_days - item_setup_days)

        # ===== Optimize: ลดเครื่องให้น้อยสุดที่ยังผลิตพอครอบคลุม qty_left =====
        # เช่น week15 carry=3 แต่ qty_left น้อย → ใช้แค่ 1 เครื่องก็เสร็จใน week นี้
        # ใช้การจำลองผลิตจริง (รวม rev_weight rounding) เพื่อความแม่นยำ

        def _sim_produce(c_mc, n_mc):
            if daily_capacity is None:
                return 0
            c_cap = daily_capacity * prod_days_old * c_mc
            n_cap = daily_capacity * prod_days_new * n_mc
            total_cap = c_cap + n_cap
            if rev_weight and rev_weight > 0 and total_cap > 0:
                return (total_cap // rev_weight) * rev_weight

            return total_cap

        if carryover_mc + new_mc > 0 and _sim_produce(carryover_mc, new_mc) > qty_left:
            # ใช้ demand ระดับ item เพื่อไม่ลดเครื่องถ้ายังมี FG ของ item เดียวกันรอผลิต
            # 🔧 FIX: คำนวณ demand ที่ยังไม่ได้วางแผน = total demand - ที่ plan ไปแล้ว - qty_left ปัจจุบัน
            # ครอบคลุมทั้ง 2 กรณี:
            #   1. คนละ SC แต่ item เดียวกัน (SC อื่น plan ไปแล้ว → ไม่ hold เครื่อง)
            #   2. SC เดียวกัน คนละ FG (FG ก่อนหน้า plan ไปแล้ว → ไม่ hold เครื่อง)
            _already_planned_for_item = _item_cumulative_planned.get(item, 0)
            next_fg_qty = max(0, _qty_for_machine_calc - _already_planned_for_item - order_qty)  # demand จาก order ที่ยังไม่ได้ plan

            # ถ้ามี FG ถัดไป ให้คำนึงถึง ORDER_QTY รวมของ item ในการตัดสินใจลดเครื่อง
            total_qty_to_consider = _qty_for_machine_calc  # total ของ item (current + future FG)

            opt_carry, opt_new = carryover_mc, new_mc  # fallback = ไม่ลด

            # ถ้ามี FG ถัดไปใน SC เดียวกัน → ไม่ลดเครื่อง เพื่อ carry ต่อในสัปดาห์เดียวกัน/สัปดาห์ถัดไป
            # 🔧 FIX: ใช้ threshold > 1.0 แทน > 0 เพื่อป้องกัน floating-point residual
            # จากการหัก qty ที่มีทศนิยม (เช่น 2530 - 687.89 - 1841.77 = 0.34)
            if next_fg_qty > 1.0:
                opt_carry = carryover_mc
                opt_new = new_mc
            else:
                # ไม่มี FG ถัดไป หรือ capacity เพียงพอสำหรับทั้ง FG ปัจจุบันและถัดไป
                # ให้ลดเครื่องตามปกติ
                # ขั้นที่ 1: ลอง carry-only (ไม่ต้อง setup เพิ่ม) หาน้อยสุดที่ produce ≥ qty_left
                # ถ้า qty ที่เหลือ (qty_left - produce) ≤ REVOLUTION_WEIGHT → ถือว่า 1 เครื่องพอ (ไม่เกิน 1 พับ)
                _rw_opt = rev_weight or 0
                found = False
                for try_c in range(1, carryover_mc + 1):
                    if _sim_produce(try_c, 0) + _rw_opt >= qty_left:
                        opt_carry = try_c
                        opt_new = 0
                        found = True
                        break

                if not found and new_mc > 0:
                    # ขั้นที่ 2: ต้องมี new ด้วย → ลด new ให้น้อยสุด
                    # ถ้า carryover_mc=0 ต้องเริ่มจาก 1 เพราะต้องมีเครื่องอย่างน้อย 1 เครื่อง
                    _try_n_start = 0 if carryover_mc > 0 else 1
                    for try_n in range(_try_n_start, new_mc + 1):
                        if _sim_produce(carryover_mc, try_n) + _rw_opt >= qty_left:
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
        # carryover ไม่ควรนับเป็น new setup jobs เสมอ
        _committed_carryover = carryover_mc
        _allowed_new = check_job_capacity_limit(mc_group, new_mc, False, _type_used_now, committed_carryover=_committed_carryover)
        if _allowed_new < new_mc:
            new_mc = _allowed_new
            available_machines = carryover_mc + new_mc
        if new_mc == 0 and carryover_mc == 0:
            # ไม่มีเครื่องเลย ข้ามไป week ถัดไป
            plan_week = next_week(plan_week)
            continue

        # YD-ORDERS: ล็อกจำนวนเครื่องภายใน week เดียวกัน
        # SO แรกกำหนดจำนวนเครื่อง → SO ถัดไปในสัปดาห์เดียวกันใช้เท่าเดิม
        _yd_lock_key = (item, mc_group, plan_week)
        if order_type == "YD-ORDERS" and _yd_lock_key in _yd_week_locked_mc:
            _locked = _yd_week_locked_mc[_yd_lock_key]
            available_machines = _locked[0]
            carryover_mc = _locked[1]
            new_mc = 0  # เครื่อง setup ไปแล้วจาก SO แรก ไม่ต้อง setup ซ้ำ
            print(f"[YD LOCK] {item} W{plan_week} SO={sc_so_no}: ใช้เครื่องเท่าเดิม actual={available_machines} carry={carryover_mc}")

        # 🔧 FIX: ถ้า qty_left < rev_weight × จำนวนเครื่อง → ลดเครื่องลง
        # เพราะไม่สามารถแบ่ง qty น้อยกว่า rev_weight ลงหลายเครื่องได้
        if rev_weight and rev_weight > 0 and available_machines > 1 and qty_left < rev_weight * available_machines:
            _max_mc_for_qty = max(1, int(qty_left // rev_weight))
            if _max_mc_for_qty < available_machines:
                print(f"[REV_WEIGHT CAP] {item} W{plan_week}: qty_left={qty_left:.0f} < rev_weight({rev_weight})×mc({available_machines})={rev_weight*available_machines:.0f} → ลดเครื่องจาก {available_machines} เป็น {_max_mc_for_qty}")
                # ลด new_mc ก่อน แล้วค่อยลด carryover_mc
                _reduce = available_machines - _max_mc_for_qty
                _new_reduce = min(_reduce, new_mc)
                new_mc -= _new_reduce
                _reduce -= _new_reduce
                if _reduce > 0:
                    carryover_mc = max(0, carryover_mc - _reduce)
                available_machines = carryover_mc + new_mc

        # setup_days_used สำหรับ log — เครื่องใหม่ต้อง setup เสมอ
        setup_days_used = item_setup_days if new_mc > 0 else 0
        # available_days สำหรับ log (ใช้เครื่องใหม่เป็นหลักถ้ามี)
        available_days = prod_days_new if new_mc > 0 else prod_days_old

        # === Same-week remaining cap: item+machine เดียวกัน ต่อจาก FG ก่อนหน้า ===
        # ใช้ remaining_week_cap ตาม item+machine ในสัปดาห์เดียวกัน (ไม่ผูก SC)
        _same_week_key = (plan_week, item, mc_group)
        _same_week_rem_cap = remaining_week_cap.get(_same_week_key, None)
        _same_week_total_cap = None
        # 🔧 FIX: remaining_week_cap override logic
        # - ถ้า remaining มาจาก new plan (owner != None): remaining คำนวณถูกต้องจาก production จริง
        #   ใน week เดียวกัน → ต้องเคารพ (ห้าม override) เพราะเครื่องที่ setup ไปแล้ว
        #   ไม่มี productive days เหลือใน week นั้น
        # - ถ้า remaining มาจาก booking (owner = None): booking production เกิดขึ้นจริงแล้ว
        #   ต้องเคารพ remaining ที่เหลือเช่นกัน
        _rem_cap_owner = remaining_week_cap_owner.get(_same_week_key, None)
        _rem_from_new_plan = _rem_cap_owner is not None  # owner=None → จาก booking seed
        # คำนวณ PRODUCE_QTY ตามสูตรที่แม่นยำ
        if _same_week_rem_cap is not None:
            # ผลิตด้วย cap ที่เหลือจาก FG ก่อนหน้า + cap incremental จากเครื่องใหม่ในสัปดาห์เดียวกัน
            _same_week_base_cap = max(0.0, float(_same_week_rem_cap))
            # YD-ORDERS: ถ้า SUB_COLOR เปลี่ยน → หัก 1 วัน × เครื่อง carry × daily_cap จาก remaining cap
            if _yd_sub_color_setup > 0 and carryover_mc > 0 and daily_capacity:
                _color_cap_deduct = carryover_mc * 1 * daily_capacity  # หัก 1 วันเต็ม ไม่ round ลง rev_weight
                _same_week_base_cap = max(0.0, _same_week_base_cap - _color_cap_deduct)
                _total_cs = _yd_week_color_setups.get((mc_key, plan_week), 0)
                print(f"[YD SUB_COLOR SAME-WEEK] {item} W{plan_week}: หัก {_color_cap_deduct:.0f} units จาก remaining cap (เปลี่ยนสีครั้งที่ {_total_cs}, remaining={_same_week_base_cap:.0f})")
            _same_week_new_cap = max(0.0, float(daily_capacity * prod_days_new * new_mc))
            # 🔧 FIX: ถ้า remaining_week_cap มาจาก booking (owner=None) และ new plan มี carry-over machine
            # → carry-over machine เป็นเครื่องอิสระจาก booking → ต้องเพิ่ม capacity ของ carry-over ด้วย
            # ไม่งั้น carry-over machine จะถูกจำกัดโดย booking remaining ทั้งที่เป็นคนละเครื่อง
            _same_week_carry_cap = 0.0
            if carryover_mc > 0 and _rem_cap_owner is None:
                _same_week_carry_cap = max(0.0, float(daily_capacity * prod_days_old * carryover_mc))
            _same_week_total_cap = _same_week_base_cap + _same_week_new_cap + _same_week_carry_cap

            if rev_weight and rev_weight > 0:
                _rem_batches = int(_same_week_total_cap // rev_weight)
                produce = min(qty_left, _rem_batches * rev_weight)
                # ✅ ถ้า qty ที่เหลือหลัง rounding ≤ 1 พับ → ดูดเข้าสัปดาห์นี้เลย
                if produce < qty_left and (qty_left - produce) <= rev_weight:
                    produce = qty_left
            else:
                produce = min(qty_left, _same_week_total_cap)
            if produce <= 0:
                # cap เหลือน้อยเกินไป (rev_weight rounding) → ข้ามไป week ถัดไป
                remaining_week_cap.pop((plan_week, item, mc_group), None)
                remaining_week_cap_owner.pop((plan_week, item, mc_group), None)
                plan_week = next_week(plan_week)
                continue

            _used_from_base = min(produce, _same_week_base_cap + _same_week_carry_cap)
            _used_from_new = max(0.0, produce - _used_from_base)
            cap_old, cap_new = _used_from_base, _used_from_new  # สำหรับ remaining cap tracking
        elif rev_weight is not None and rev_weight > 0:
            cap_old = daily_capacity * prod_days_old * carryover_mc
            cap_new = daily_capacity * prod_days_new * new_mc
            max_capacity = cap_old + cap_new
            max_batches = max_capacity // rev_weight
            produce = min(qty_left, max_batches * rev_weight)
            # ✅ ถ้า qty ที่เหลือหลัง rounding ≤ 1 พับ → ดูดเข้าสัปดาห์นี้เลย (ไม่เพิ่มเครื่อง)
            if produce < qty_left and (qty_left - produce) <= rev_weight:
                produce = qty_left
        else:
            cap_old = daily_capacity * prod_days_old * carryover_mc
            cap_new = daily_capacity * prod_days_new * new_mc
            produce = min(qty_left, cap_old + cap_new)

        # Hard clamp: ห้ามยอดรวม item+week+mc_group เกิน capacity ของสัปดาห์นี้
        # ป้องกันกรณี merge ข้าม FG แล้วถูกบวก produce ซ้ำจนเกินเพดาน
        _already_planned_qty = 0.0
        for _p in plans:
            if (
                _p.get("ITEM_CODE") == item
                and _p.get("PLAN_WEEK") == plan_week
                and _p.get("MC_GROUP") == mc_group
            ):
                _already_planned_qty += float(_p.get("PRODUCE_QTY", 0) or 0)

        if _same_week_rem_cap is not None:
            _max_additional_qty = max(0.0, float(_same_week_total_cap or 0.0))
        else:
            _week_capacity = max(0.0, float(cap_old + cap_new))
            _max_additional_qty = max(0.0, _week_capacity - _already_planned_qty)

        if produce > _max_additional_qty:
            print(
                f"[CAP CLAMP] {item} W{plan_week} {mc_group}: "
                f"produce {produce:.2f} -> {_max_additional_qty:.2f} "
                f"(already={_already_planned_qty:.2f})"
            )
            _clamped_qty = max(0.0, float(_max_additional_qty))
            if rev_weight and rev_weight > 0:
                _rounded_cap = int(_clamped_qty // rev_weight) * rev_weight
                if qty_left <= _clamped_qty and (qty_left - _rounded_cap) <= rev_weight:
                    _clamped_qty = qty_left
                elif _rounded_cap > 0:
                    _clamped_qty = _rounded_cap
                elif _clamped_qty < rev_weight:
                    _clamped_qty = 0
            produce = _clamped_qty

        # ไม่เพิ่มแถวถ้าไม่มีการผลิต — แต่ถ้า setup ไปแล้ว (new_mc>0) ให้บันทึกว่าเครื่อง setup เสร็จ
        # เพื่อให้ week ถัดไปนับเป็น carryover (ไม่ต้อง setup ซ้ำ) → ผลิตได้เต็มจำนวนวัน
        if produce <= 0:
            if new_mc > 0:
                _plan_ck_setup = _resolve_carry_key(item, mc_group, _sel_gauge)
                machines_in_use[_plan_ck_setup] = available_machines
                # 🔧 FIX: อย่าเขียนทับ last_production ด้วย week ที่เก่ากว่า (จาก SETUP ONLY)
                # เช่น booking อยู่ที่ week 22 แล้ว SETUP ONLY ที่ week 19 ไม่ควร override
                _cur_lp = last_production.get(_plan_ck_setup)
                if _cur_lp is None or week_index(plan_week) >= _cur_lp:
                    last_production[_plan_ck_setup] = week_index(plan_week)
                new_plan_started_items.add(_plan_ck_setup)
                # บันทึก job usage สำหรับ week นี้ (เครื่อง setup แล้วแม้ยังไม่ผลิต)
                if plan_week not in weekly_job_usage:
                    weekly_job_usage[plan_week] = {}
                weekly_job_usage[plan_week][mc_group] = (
                    weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
                )
                _week_mc_key_setup = (week_index(plan_week), mc_group)
                weekly_mc_usage[_week_mc_key_setup] = weekly_mc_usage.get(_week_mc_key_setup, 0) + available_machines
                last_sc_so_no[_plan_ck_setup] = sc_so_no
                if order_type == "YD-ORDERS" and sub_color:
                    last_sub_color[_plan_ck_setup] = sub_color
                # 🔧 FIX: บันทึก SETUP ONLY ลง _existing_item_week_mc
                # เพื่อป้องกัน SETUP EARLY PRIORITY ดึงกลับมา week เดิมซ้ำ
                # ซึ่งทำให้ double-count วันทำงาน (setup กิน 5 วัน แล้วมาผลิตอีก 5 วัน)
                _existing_item_week_mc.add((item, plan_week, mc_group))
                print(f"[SETUP ONLY] {item} W{plan_week} MC={mc_group}: setup {new_mc} machines (0 produce, setup ate all {actual_working_days} working days)")
            plan_week = next_week(plan_week)
            if plan_week is None:
                break
            continue

        # จำนวนเครื่องที่วางแผนไว้ (จาก calculate_required_machines)
        # แสดงจำนวนเครื่องจริงตาม required_mc หรือ available_machines
        prev_week_mc = machines_in_use.get(
            _resolve_carry_key(item, mc_group, _sel_gauge), available_machines
        )
        planned_mc = (
            required_machines_info[2]
            if required_machines_info
            else available_machines
        )
        # 🔧 FIX: ตรวจสอบว่า Item+Week+MC_GROUP นี้มีการ plan ไปแล้วหรือไม่
        # เพื่อป้องกันการซ้ำซ้อนของ orders ที่มี Item+MC_GROUP เดียวกันใน week เดียวกัน
        # โดยไม่สนใจว่าจะเป็น SC หรือ TARGET_KNIT อะไร (เพราะอาจมีการ merge SC)
        _current_item_week_mc = (item, plan_week, mc_group)
        _is_fg_split = False  # Flag: สร้าง row ใหม่สำหรับ FG_WEEK ที่ต่างกัน (ไม่ merge)
        # YD-ORDERS: ห้าม merge — แต่ละ SO ต้องแยกเป็น row ของตัวเอง
        if order_type == "YD-ORDERS":
            _is_fg_split = True  # บังคับสร้าง row ใหม่เสมอ
        elif _current_item_week_mc in _existing_item_week_mc:
            # Merge ลงแถวเดิมของ Item+Week+MC_GROUP เฉพาะ FG_WEEK เดียวกันเท่านั้น
            _merged = False
            for _p in plans:
                if (
                    _p.get("ITEM_CODE") == item
                    and _p.get("PLAN_WEEK") == plan_week
                    and _p.get("MC_GROUP") == mc_group
                    and _p.get("FG_WEEK") == fg_week
                ):
                    _p["PRODUCE_QTY"] = float(_p.get("PRODUCE_QTY", 0) or 0) + float(produce)
                    _p["PLAN_QTY"] = max(0, float(qty_left - produce))
                    _merged = True
                    break

            if _merged:
                print(
                    f"[DEBUG DUPLICATE] MERGED: {item}+{plan_week}+{mc_group} FG={fg_week} +{produce:.1f}"
                )

                qty_left -= produce
                if qty_left <= 0:
                    qty_left = 0

                # อัปเดต remaining cap แบบเดียวกับเส้นทาง append ใหม่
                if _same_week_rem_cap is not None:
                    _new_rem = max(0, float(_same_week_total_cap or 0.0) - produce)
                    if _new_rem > 0:
                        remaining_week_cap[(plan_week, item, mc_group)] = _new_rem
                        remaining_week_cap_owner[(plan_week, item, mc_group)] = sc_so_no
                    else:
                        remaining_week_cap.pop((plan_week, item, mc_group), None)
                        remaining_week_cap_owner.pop((plan_week, item, mc_group), None)
                else:
                    _full_week_cap = cap_old + cap_new
                    _rem = max(0, _full_week_cap - produce)
                    remaining_week_cap[(plan_week, item, mc_group)] = _rem
                    remaining_week_cap_owner[(plan_week, item, mc_group)] = sc_so_no

                _plan_ck_merge = _resolve_carry_key(item, mc_group, _sel_gauge)
                last_production[_plan_ck_merge] = week_index(plan_week)
                # 🔧 FIX: ถ้า MERGE iteration นำเครื่องใหม่เข้ามา (new_mc > 0)
                # ต้องอัปเดต row + tracking ให้สะท้อนจำนวนเครื่องจริง
                # เช่น cross-SC fill สร้าง row carry=2,new=0 → MERGE เพิ่ม new=2 →
                # row ต้องแสดง actual=4 เพื่อไม่ให้ overcapacity ในผลลัพธ์
                _existing_actual_mc = int(_p.get("ACTUAL_MC", 0) or 0)
                _existing_new_mc = int(_p.get("NEW_MC", 0) or 0)
                if new_mc > 0 and available_machines > _existing_actual_mc:
                    _merged_actual = available_machines
                    _merged_new = _existing_new_mc + new_mc
                    _p["ACTUAL_MC"] = _merged_actual
                    _p["NEW_MC"] = _merged_new
                    _p["REQUIRED_MC"] = _merged_actual
                    # เปลี่ยน SETUP_DAYS เป็นค่าจาก iteration ที่ setup จริง
                    _p["SETUP_DAYS"] = setup_days_used
                    _p["AVAILABLE_DAYS"] = available_days
                    machines_in_use[_plan_ck_merge] = _merged_actual
                    # อัพเดท tracking สำหรับเครื่องใหม่ที่ MERGE เพิ่มเข้ามา
                    _week_mc_key_m = (week_index(plan_week), mc_group)
                    weekly_mc_usage[_week_mc_key_m] = weekly_mc_usage.get(_week_mc_key_m, 0) + new_mc
                    if plan_week not in weekly_job_usage:
                        weekly_job_usage[plan_week] = {}
                    weekly_job_usage[plan_week][mc_group] = (
                        weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
                    )
                    print(
                        f"[MERGE MC UPDATE] {item} W{plan_week}: "
                        f"actual {_existing_actual_mc}→{_merged_actual} "
                        f"(carry={_p.get('CARRYOVER_MC')}, new={_merged_new})"
                    )
                else:
                    machines_in_use[_plan_ck_merge] = _existing_actual_mc
                last_sc_so_no[_plan_ck_merge] = sc_so_no
                if order_type == "YD-ORDERS" and sub_color:
                    last_sub_color[_plan_ck_merge] = sub_color
                new_plan_started_items.add(_plan_ck_merge)

                _produced_week = plan_week
                plan_week = next_week(plan_week)
                continue

            # ตรวจว่ามี row ของ FG อื่นอยู่หรือไม่ → ถ้ามี = FG split (สร้าง row ใหม่แทน merge)
            _has_other_fg = any(
                _p.get("ITEM_CODE") == item
                and _p.get("PLAN_WEEK") == plan_week
                and _p.get("MC_GROUP") == mc_group
                for _p in plans
            )
            if _has_other_fg:
                # FG_WEEK ต่างกัน → สร้าง row ใหม่ (fall through ไปที่ append ด้านล่าง)
                _is_fg_split = True
                print(
                    f"[DEBUG FG SPLIT] {item}+{plan_week}+{mc_group}: existing FG differs → new row for FG {fg_week}"
                )
            else:
                print(
                    f"[DEBUG DUPLICATE] FALLBACK SKIP: {item}+{plan_week}+{mc_group} already planned but merge target not found"
                )
                # ข้ามไป week ถัดไป
                plan_week = next_week(plan_week)
                if plan_week is None:
                    break
                continue
        
        print(f"[DEBUG DUPLICATE] Adding plan for {item}+{plan_week}+{mc_group}")
        
        plans.append(
            {
                "ITEM_CODE": item,
                "SC_SO_NO": str(order.get("SO_NO", order.get("SC/SO NO", ""))).strip(),
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
                "MATERIAL_CONTENT": str(order.get("MATERIAL_CONTENT", "")).strip(),
                "IS_CORE_ITEM": "CORE ITEM" if is_core_item else "",
                "CUSTOMER": str(order.get("Customer", "")).strip(),
                "PLAN_SOURCE": "NEW",
                "LT_YARN": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_days(item),
                "YARN_USED": _yarn_used_lookup.get(str(item).strip().upper(), ""),
                "DATE_IN": order.get("DATE_IN"),
                "EARLIEST_PLAN_WEEK": order.get("DYE_END_DATE") if order_type == "YD-ORDERS" else get_yarn_lt_earliest_week(item, date_in=order.get("DATE_IN")),
                "SUB_COLOR": sub_color,
            }
        )
        qty_left -= produce
        if qty_left <= 0:
            qty_left = 0  # ป้องกันค่าติดลบ
        
        # 🔧 FIX: บันทึก Item+Week+MC_GROUP ที่ได้วางแผนไปแล้ว
        _existing_item_week_mc.add((item, plan_week, mc_group))
        print(f"[DEBUG DUPLICATE] Added to existing set: {item}+{plan_week}+{mc_group}")
        
        # บันทึก/อัปเดต remaining cap สำหรับ FG ถัดไปของ item+machine เดียวกัน
        if _same_week_rem_cap is not None:
            # อัปเดต remaining cap หลังใช้งาน
            _new_rem = max(0, float(_same_week_total_cap or 0.0) - produce)
            if _new_rem > 0:
                remaining_week_cap[(plan_week, item, mc_group)] = _new_rem
                remaining_week_cap_owner[(plan_week, item, mc_group)] = sc_so_no
            else:
                remaining_week_cap.pop((plan_week, item, mc_group), None)
                remaining_week_cap_owner.pop((plan_week, item, mc_group), None)
        else:
            # บันทึกส่วนที่เหลือสำหรับ FG ถัดไป (ไม่ว่า order จะจบหรือไม่)
            # เพื่อให้ SC/SO อื่นสามารถ carry ต่อใน week เดียวกันได้
            _full_week_cap = cap_old + cap_new
            _rem = max(0, _full_week_cap - produce)
            # 🔧 FIX: ถ้า item ลดเครื่องจากสัปดาห์ก่อน (tail end) → เพิ่ม capacity ของเครื่องที่ปล่อย
            # เพื่อให้ SC/SO อื่นของ item เดียวกันสามารถใช้เครื่องต่อโดยไม่ drop
            # ⚠️ ต้องมี carryover_mc > 0 (คือลดจริงจาก week ก่อนใน new plan) เท่านั้น
            # ถ้า carryover_mc == 0 คือ cold start → ไม่มีเครื่องที่ "ปล่อย" จริง
            mc_key = _resolve_carry_key(item, mc_group, _sel_gauge)
            _prev_mc_for_item = machines_in_use.get(mc_key, 0)
            if _prev_mc_for_item > available_machines and available_machines > 0 and carryover_mc > 0:
                _freed_mc = _prev_mc_for_item - available_machines
                _freed_cap = _freed_mc * actual_working_days * daily_capacity
                if rev_weight and rev_weight > 0:
                    _freed_cap = (int(_freed_cap // rev_weight)) * rev_weight
                _rem += _freed_cap
                print(f"🔧 Freed capacity: {item} W{plan_week} reduced {_prev_mc_for_item}→{available_machines} mc, freed {_freed_mc} mc = {_freed_cap:.0f} units")
            remaining_week_cap[(plan_week, item, mc_group)] = _rem
            remaining_week_cap_owner[(plan_week, item, mc_group)] = sc_so_no
        _plan_ck = _resolve_carry_key(item, mc_group, _sel_gauge)
        last_production[_plan_ck] = week_index(plan_week)
        machines_in_use[_plan_ck] = available_machines  # บันทึกจำนวนเครื่องที่ใช้จริง
        last_sc_so_no[_plan_ck] = sc_so_no  # บันทึก SC/SO NO ล่าสุดที่ผลิต
        if order_type == "YD-ORDERS" and sub_color:
            last_sub_color[_plan_ck] = sub_color
        # YD-ORDERS: ล็อกจำนวนเครื่องสำหรับ SO ถัดไปใน week เดียวกัน
        if order_type == "YD-ORDERS":
            _yd_lock_key = (item, mc_group, plan_week)
            if _yd_lock_key not in _yd_week_locked_mc:
                _yd_week_locked_mc[_yd_lock_key] = (available_machines, carryover_mc, new_mc)
        new_plan_started_items.add(_plan_ck)  # บันทึกว่า item นี้เริ่ม new plan แล้ว
        
        # บันทึก total machines per (week, mc_group) สำหรับ gradual increase
        # 🔧 FG SPLIT: ถ้าเป็น FG split row → เครื่องนับไปแล้วจาก FG แรก ห้ามนับซ้ำ
        if not _is_fg_split:
            _week_mc_key = (week_index(plan_week), mc_group)
            weekly_mc_usage[_week_mc_key] = weekly_mc_usage.get(_week_mc_key, 0) + available_machines
            # อัพเดท job usage สำหรับสัปดาห์นี้ (นับเฉพาะ new_mc = machines ที่ setup ใหม่)
            # เครื่อง carry ต่อ (carryover_mc) ไม่ต้องนับเป็น job ใหม่เพราะไม่ต้อง setup
            if plan_week not in weekly_job_usage:
                weekly_job_usage[plan_week] = {}
            weekly_job_usage[plan_week][mc_group] = (
                weekly_job_usage[plan_week].get(mc_group, 0) + new_mc
            )
        # อัพเดท new plan usage (นับทั้ง carryover+new สำหรับ get_actual_mc_remain)
        # key ต้องเป็น (mc_group, gauge_str) เสมอ — ห้าม pool ข้าม GUAGE
        # ถ้าเป็น same-week continuation หรือ FG split → เครื่องนับไปแล้วจาก FG ก่อนหน้า ห้ามนับซ้ำ
        if _same_week_rem_cap is None and not _is_fg_split:
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
            # ถ้า mc_group+gauge อยู่ใน MC_GROUP_REDIRECT → หักเครื่องจาก target แทน
            # เช่น SKP 20 → หักจาก FA 20 เสมอ
            _wpu_mc_r, _wpu_gauge_r = _apply_mc_redirect(mc_group, _wpu_gauge_str)
            _wpu_key = (_wpu_mc_r, _wpu_gauge_r)
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
    # 🔧 FIX: อัปเดต cumulative planned qty สำหรับ item นี้
    # ใช้ order_qty (ไม่ใช่ qty_left) เพราะ order ถูก process แล้วไม่ว่าผลิตครบหรือไม่
    _item_cumulative_planned[item] = _item_cumulative_planned.get(item, 0) + order_qty
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
plans = detect_and_fill_unused_capacity(plans, orders, summary_mc)

# =========================
# EXPORT

# =========================
plan_df = pd.DataFrame(plans)
DATA_PLAN_DIR.mkdir(exist_ok=True)

# =========================
# LOAD BALANCING - Apply to final plan
# =========================
# ปิดการใช้ Load Balancing ตอนสรุปผลลัพธ์เพื่อรักษาความสามารถในการผลิตตาม ORDERS_QTY
# Load Balancing ทำงานเฉพาะตอนเลือกเครื่องเพื่อกระจายงานอย่างสมดุล
if False and USE_LOAD_BALANCING and not plan_df.empty:
    print("🔄 Applying Load Balancing to final plan...")
    
    # Group plans by week for load balancing
    balanced_plans = []
    for week in sorted(plan_df['PLAN_WEEK'].unique()):
        week_plans = plan_df[plan_df['PLAN_WEEK'] == week].to_dict('records')
        
        # Apply load balancing to this week's plans
        balanced_week_plans = _analyze_and_balance_load(week, week_plans)
        balanced_plans.extend(balanced_week_plans)
    
    # Replace original plans with balanced plans
    plan_df = pd.DataFrame(balanced_plans)
    print(f"✅ Load balancing completed. Total plans after balancing: {len(balanced_plans)}")

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

# แสดง orders ที่ไม่มี MC GROUP
if _skip_no_mc_group:
    print(f"\n⚠️  Orders ที่ไม่มี MC GROUP ({len(_skip_no_mc_group)} รายการ) → ไม่ได้วางแผน:")
    for _s in _skip_no_mc_group:
        print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']}")
    print()

# แสดง orders ที่ MC GROUP ไม่มี FACTORY_TYPE
if _skip_no_factory:
    print(f"\n⚠️  Orders ที่ MC GROUP ไม่มี FACTORY_TYPE ({len(_skip_no_factory)} รายการ) → ไม่ได้วางแผน:")
    for _s in _skip_no_factory:
        print(f"   - {_s['SC_SO_NO']} | {_s['ITEM_CODE']} | MC={_s['MC_GROUP']}")
    print()

# รวม unplanned orders เป็น DataFrame
_unplanned_rows = _skip_no_mc_group + _skip_no_factory
_unplanned_df = pd.DataFrame(_unplanned_rows) if _unplanned_rows else pd.DataFrame(
    columns=["SC_SO_NO", "ITEM_CODE", "MC_GROUP", "ORDERS_QTY", "PENDING_PLAN", "FG_WEEK", "REASON"]
)

# บันทึกไฟล์ใหม่
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as _writer:
    plan_df.to_excel(_writer, sheet_name="PLAN", index=False)
    remaining_df.to_excel(_writer, sheet_name="REMAINING_JOBS", index=False)
    _unplanned_df.to_excel(_writer, sheet_name="UNPLANNED", index=False)

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
if not detail_mc.empty:
    # โหลดทั้งหมดจาก booking เพื่อแสดง setup ทั้งหมด
    old_booking_df = detail_mc.copy()
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
            "FIBER_TYPE": "MATERIAL_CONTENT",
        }
    )
    # เติม MATERIAL_CONTENT จาก orders โดย lookup ITEM_CODE (เหมือน NEW rows)
    _mat_item_lookup = {}
    if "MATERIAL_CONTENT" in orders.columns:
        for _, _or in orders.iterrows():
            _ic = str(_or.get("Item Code", "")).strip().upper()
            _mat = str(_or.get("MATERIAL_CONTENT", "")).strip()
            if _ic and _mat and _mat.upper() not in ("NAN", ""):
                _mat_item_lookup[_ic] = _mat
    if "ITEM_CODE" in old_booking_df.columns:
        def _get_mat_from_order(row):
            _ic = str(row.get("ITEM_CODE", "")).strip().upper()
            _v = _mat_item_lookup.get(_ic)
            if _v:
                return _v
            # fallback: ใช้ FIBER_TYPE (POLY / None POLY) จาก YARN-USED ของ item
            return get_fiber_type_for_item(_ic)
        old_booking_df["MATERIAL_CONTENT"] = old_booking_df.apply(_get_mat_from_order, axis=1)
    # แปลง DAILY_CAPACITY จากฐาน 24 ชั่วโมง → 20 ชั่วโมง (เหมือน _normalize_capacity)
    if "DAILY_CAPACITY" in old_booking_df.columns:
        old_booking_df["DAILY_CAPACITY"] = pd.to_numeric(
            old_booking_df["DAILY_CAPACITY"], errors="coerce"
        ) * (20 / 24)
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
    # เก็บแถว OLD ทั้งหมด (ไม่จำกัดสัปดาห์)
    old_booking_df = old_booking_df.sort_values(
        ["ITEM_CODE", "MC_GROUP", "PLAN_WEEK"], na_position="last"
    ).reset_index(drop=True)
    print(
        f"📦 OLD rows จาก booking_final_ready25 (ทั้งหมด): {len(old_booking_df)} rows"
    )
else:
    print("⚠️ ไม่พบข้อมูลใน detail_mc → ข้าม OLD")
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
# ดูว่า **ผลิตจนครบ order qty** ทันหรือไม่ (ใช้ PLAN_WEEK สุดท้ายของ order)
# ไม่ใช่ดูแค่แต่ละ row — ต้องดูว่า row สุดท้ายของ order ทันตาม target ถึงจะ "ทัน"
# =========================
_grp_cols = ["SC_SO_NO", "ITEM_CODE", "FG_WEEK"]
_grp_cols_available = [c for c in _grp_cols if c in combined_df.columns]

# หา PLAN_WEEK สุดท้าย (ผลิตเสร็จ) ของแต่ละ order group
# ใช้ NEW rows เป็นหลัก (plan ใหม่) — ถ้าไม่มี NEW ถึงใช้ OLD
_last_pw_map = {}  # {normalized_str_key: (last_plan_week, last_plan_week_index)}
if _grp_cols_available:
    # Step 1: คำนวณจาก NEW rows ก่อน (source of truth)
    if "PLAN_SOURCE" in combined_df.columns:
        _new_only = combined_df[combined_df["PLAN_SOURCE"].astype(str).str.upper() == "NEW"]
    else:
        _new_only = combined_df
    if not _new_only.empty:
        for _key, _grp in _new_only.groupby(_grp_cols_available):
            _pws = _grp["PLAN_WEEK"].dropna()
            if _pws.empty:
                continue
            try:
                _last_pw = int(_pws.max())
                _last_idx = week_index(_last_pw)
                if _last_idx is not None:
                    _norm_key = tuple(str(v).strip() for v in (_key if isinstance(_key, tuple) else (_key,)))
                    _last_pw_map[_norm_key] = (_last_pw, _last_idx)
            except Exception:
                continue
    # Step 2: เติม order ที่ไม่มี NEW rows → ใช้ OLD rows
    if "PLAN_SOURCE" in combined_df.columns:
        _old_only = combined_df[combined_df["PLAN_SOURCE"].astype(str).str.upper() != "NEW"]
        if not _old_only.empty:
            for _key, _grp in _old_only.groupby(_grp_cols_available):
                _norm_key = tuple(str(v).strip() for v in (_key if isinstance(_key, tuple) else (_key,)))
                if _norm_key in _last_pw_map:
                    continue  # มี NEW แล้ว ไม่ต้อง overwrite
                _pws = _grp["PLAN_WEEK"].dropna()
                if _pws.empty:
                    continue
                try:
                    _last_pw = int(_pws.max())
                    _last_idx = week_index(_last_pw)
                    if _last_idx is not None:
                        _last_pw_map[_norm_key] = (_last_pw, _last_idx)
                except Exception:
                    continue


def _target_status(row) -> str:
    # CORE ITEM ไม่ต้องบอกทัน/ไม่ทัน
    if str(row.get("IS_CORE_ITEM", "")).strip().upper() == "CORE ITEM":
        return ""

    _tk = row.get("TARGET_KNIT")
    _pw = row.get("PLAN_WEEK")
    if pd.isna(_pw) or pd.isna(_tk):
        return "-"

    try:
        _tk_idx = week_index(int(_tk))
        if _tk_idx is None:
            return "-"

        # สร้าง key (str) เพื่อ lookup PLAN_WEEK สุดท้ายของ order นี้
        _key_vals = tuple(str(row.get(c, "")).strip() for c in _grp_cols_available)
        _finish = _last_pw_map.get(_key_vals)
        if _finish is not None:
            _last_pw_val, _last_pw_idx = _finish
            if _last_pw_idx <= _tk_idx:
                return "ทัน"
            else:
                return f"ไม่ทัน (+{_last_pw_idx - _tk_idx} wk)"

        # fallback: ถ้าไม่มี group → เทียบ row เดียว
        _pw_idx = week_index(int(_pw))
        if _pw_idx is None:
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
    if not _multi_cap_df.empty:
        _multi_cap_df.to_excel(writer, sheet_name="MULTI_CAP", index=False)
print(f"Combined (OLD+NEW): {COMBINED_FILE}")
print(f"  OLD rows: {len(old_booking_df) if not old_booking_df.empty else 0}")
print(f"  NEW rows: {len(plan_df)}")
if not _no_cap_df.empty:
    print(f"  NO_CAP items: {_no_cap_df['Item Code'].nunique()} items, {len(_no_cap_df)} orders → sheet 'NO_CAP'")
if not _multi_cap_df.empty:
    print(f"  MULTI_CAP items: {_multi_cap_df['Item Code'].nunique()} items, {len(_multi_cap_df)} orders → sheet 'MULTI_CAP' (SINGLE MC_TYPE หรือ OM/OMNOI)")

# =========================
# SETUP_TRACKING sheet — แสดงเฉพาะ ITEM ที่หัก Job พร้อม Week, จำนวน Job และจำนวน MC ที่ Setup
# เกณฑ์: หัก Job เมื่อ NEW_MC > 0 (เครื่องใหม่ที่ต้อง setup = 1 job ต่อ 1 เครื่อง)
# =========================
setup_tracking_rows = []

# --- NEW PLAN: หัก Job เมื่อ NEW_MC > 0 ---
if not plan_df.empty and "NEW_MC" in plan_df.columns:
    _new_setup = plan_df[pd.to_numeric(plan_df["NEW_MC"], errors="coerce").fillna(0) > 0].copy()
    for _, row in _new_setup.iterrows():
        _new_mc_val = int(pd.to_numeric(row["NEW_MC"], errors="coerce") or 0)
        _carryover_val = int(pd.to_numeric(row.get("CARRYOVER_MC", 0), errors="coerce") or 0)
        _type_key_new = _get_type_key_for_mc(str(row["MC_GROUP"]).strip().upper())
        setup_tracking_rows.append({
            "PLAN_SOURCE": "NEW",
            "PLAN_WEEK": row["PLAN_WEEK"],
            "ITEM_CODE": row["ITEM_CODE"],
            "SC_SO_NO": row["SC_SO_NO"],
            "CUSTOMER": row.get("CUSTOMER", ""),
            "MC_GROUP": row["MC_GROUP"],
            "MC_GUAGE": row.get("MC_GUAGE", ""),
            "TYPE": _type_key_new,
            "JOBS_DEDUCTED": _new_mc_val,    # กี่ Job ที่หัก (1 เครื่องใหม่ = 1 job)
            "SETUP_MC": _new_mc_val,          # กี่ MC ที่ Setup
            "CARRYOVER_MC": _carryover_val,   # เครื่อง Carryover (ไม่หัก job)
            "SETUP_DAYS": row.get("SETUP_DAYS", 0),
            "ORDER_TYPE": row.get("ORDER_TYPE", ""),
            "FG_WEEK": row.get("FG_WEEK", ""),
            "TARGET_KNIT": row.get("TARGET_KNIT", ""),
        })

# --- OLD PLAN (BOOKING): หัก Job เมื่อ item ใหม่หรือเพิ่มเครื่องจาก week ก่อน ---
# คำนวณจาก detail_mc โดยใช้ logic เดียวกับ weekly_job_usage แต่เก็บต่อ item ด้วย
if (
    not detail_mc.empty
    and "WEEK" in detail_mc.columns
    and "ITEM_CODE" in detail_mc.columns
    and "MC_USE_CEIL" in detail_mc.columns
    and "MC_GROUP" in detail_mc.columns
):
    _det_trk = detail_mc.copy()
    _det_trk["WEEK"] = pd.to_numeric(_det_trk["WEEK"], errors="coerce")
    _det_trk["MC_USE_CEIL"] = (
        pd.to_numeric(_det_trk["MC_USE_CEIL"], errors="coerce").fillna(0).astype(int)
    )
    _det_trk = _det_trk.dropna(subset=["WEEK", "ITEM_CODE", "MC_GROUP"])
    _det_trk["WEEK"] = _det_trk["WEEK"].astype(int)
    _det_trk["ITEM_CODE"] = _det_trk["ITEM_CODE"].astype(str).str.strip().str.upper()
    _det_trk["MC_GROUP"] = _det_trk["MC_GROUP"].astype(str).str.strip().str.upper()
    _det_trk_active = _det_trk[_det_trk["MC_USE_CEIL"] > 0].copy()

    for _mc_grp, _grp_df in _det_trk_active.groupby("MC_GROUP"):
        _all_weeks_trk = sorted(_grp_df["WEEK"].unique())
        # สร้าง lookup: week → {item: mc_count}
        _week_item_mc_trk: dict = {}
        for _wk_t in _all_weeks_trk:
            _wk_rows_t = _grp_df[_grp_df["WEEK"] == _wk_t]
            _week_item_mc_trk[_wk_t] = (
                _wk_rows_t.groupby("ITEM_CODE")["MC_USE_CEIL"].sum().to_dict()
            )
        for _i_t, _wk_t in enumerate(_all_weeks_trk):
            _curr_items_t = _week_item_mc_trk[_wk_t]
            _prev_items_t = (
                _week_item_mc_trk.get(_all_weeks_trk[_i_t - 1], {}) if _i_t > 0 else {}
            )
            for _item_t, _mc_t in _curr_items_t.items():
                _prev_mc_t = _prev_items_t.get(_item_t, 0)
                if _prev_mc_t == 0:
                    _new_jobs_t = _mc_t        # item ใหม่ → นับทุกเครื่อง
                elif _mc_t > _prev_mc_t:
                    _new_jobs_t = _mc_t - _prev_mc_t  # เพิ่มเครื่อง → นับส่วนที่เพิ่ม
                else:
                    continue  # carryover หรือลดลง → ไม่หัก job

                # หา gauge และ SO_NO จาก detail_mc
                _item_rows_t = _grp_df[
                    (_grp_df["WEEK"] == _wk_t) & (_grp_df["ITEM_CODE"] == _item_t)
                ]
                _gauge_t = ""
                _so_t = ""
                if not _item_rows_t.empty:
                    if "GUAGE" in _item_rows_t.columns:
                        _gauge_t = str(_item_rows_t.iloc[0].get("GUAGE", "")).strip()
                    for _so_col in ("SC_SO_NO", "SO_NO"):
                        if _so_col in _item_rows_t.columns:
                            _raw_so_t = str(_item_rows_t.iloc[0].get(_so_col, "")).strip()
                            _so_t = _raw_so_t.lstrip("Ss")
                            break

                # lookup order info
                _cust_t = _otype_t = _fg_t = _tknit_t = ""
                if _so_t:
                    _ord_t = orders[orders["SC/SO NO"].astype(str).str.strip() == _so_t]
                    if not _ord_t.empty:
                        _cust_t = str(_ord_t.iloc[0].get("Customer", "")).strip()
                        _otype_t = str(_ord_t.iloc[0].get("Orders Type", "")).strip()
                        _fg_t = _ord_t.iloc[0].get("FG Week", "")

                _type_key_old = _get_type_key_for_mc(str(_mc_grp).strip().upper())
                setup_tracking_rows.append({
                    "PLAN_SOURCE": "OLD",
                    "PLAN_WEEK": _wk_t,
                    "ITEM_CODE": _item_t,
                    "SC_SO_NO": _so_t,
                    "CUSTOMER": _cust_t,
                    "MC_GROUP": str(_mc_grp).strip().upper(),
                    "MC_GUAGE": _gauge_t,
                    "TYPE": _type_key_old,
                    "JOBS_DEDUCTED": _new_jobs_t,
                    "SETUP_MC": _new_jobs_t,
                    "CARRYOVER_MC": _prev_mc_t,
                    "SETUP_DAYS": 0,
                    "ORDER_TYPE": _otype_t,
                    "FG_WEEK": _fg_t,
                    "TARGET_KNIT": _tknit_t,
                })

setup_tracking_df = (
    pd.DataFrame(setup_tracking_rows)
    if setup_tracking_rows
    else pd.DataFrame(columns=[
        "PLAN_SOURCE", "PLAN_WEEK", "ITEM_CODE", "SC_SO_NO", "CUSTOMER",
        "MC_GROUP", "MC_GUAGE", "TYPE", "JOBS_DEDUCTED", "SETUP_MC", "CARRYOVER_MC",
        "SETUP_DAYS", "ORDER_TYPE", "FG_WEEK", "TARGET_KNIT",
    ])
)

if not setup_tracking_df.empty:
    setup_tracking_df = setup_tracking_df.sort_values(
        ["PLAN_WEEK", "PLAN_SOURCE", "ITEM_CODE"], ignore_index=True
    )

print("📋 สรุป SETUP_TRACKING (ITEM ที่หัก Job) ต่อ Week:")
if not setup_tracking_df.empty:
    for _wk_s in sorted(setup_tracking_df["PLAN_WEEK"].unique()):
        _wdf_s = setup_tracking_df[setup_tracking_df["PLAN_WEEK"] == _wk_s]
        _total_jobs_s = int(_wdf_s["JOBS_DEDUCTED"].sum())
        _total_mc_s = int(_wdf_s["SETUP_MC"].sum())
        print(f"   Week {_wk_s}: {len(_wdf_s)} items — หัก {_total_jobs_s} Jobs, Setup {_total_mc_s} MC")
        for _, _r_s in _wdf_s.iterrows():
            _src_icon = "🆕" if _r_s["PLAN_SOURCE"] == "NEW" else "📋"
            print(
                f"     {_src_icon} [{_r_s['PLAN_SOURCE']}] {_r_s['ITEM_CODE']}"
                f" ({_r_s.get('SC_SO_NO', '')}) - {_r_s['MC_GROUP']}"
                f" — หัก {_r_s['JOBS_DEDUCTED']} Job, Setup {_r_s['SETUP_MC']} MC"
            )
    print()
else:
    print("   (ไม่มีการหัก Job ในแผนนี้)")
    print()

# บันทึกไฟล์ใหม่ (รวม SETUP_TRACKING)
if not plan_df.empty:
    _dbg_item = "FD6PRTPG99A0"
    _dbg_rows = plan_df[
        plan_df["ITEM_CODE"].astype(str).str.strip().str.upper() == _dbg_item
    ]
    if not _dbg_rows.empty:
        _dbg_view = _dbg_rows[[
            "SC_SO_NO",
            "PLAN_WEEK",
            "MC_GROUP",
            "PRODUCE_QTY",
            "SETUP_DAYS",
            "CARRYOVER_MC",
            "NEW_MC",
            "ACTUAL_MC",
        ]].sort_values(["PLAN_WEEK", "SC_SO_NO"])
        print("[DEBUG EXPORT] FD6PRTPG99A0 rows before writing OUTPUT_FILE:")
        print(_dbg_view.to_string(index=False))

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as _writer:
    plan_df.to_excel(_writer, sheet_name="PLAN", index=False)
    remaining_df.to_excel(_writer, sheet_name="REMAINING_JOBS", index=False)
    setup_tracking_df.to_excel(_writer, sheet_name="SETUP_TRACKING", index=False)
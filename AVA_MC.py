import re
import pandas as pd
import numpy as np
from pathlib import Path
from Yarn_Master import load_yarn_master
from Calendar import load_calendar

# Load calendar from SharePoint URL (auto-sync)
CALENDAR_FILE = "https://nanyangtextilegroup.sharepoint.com/:x:/s/SCM_Cloud/IQCXP4jH73zhQozDNvw1XF8OAY5m4p-UFv35Tcpza6v8mJo?e=43ffCc"
_calendar_df = load_calendar(CALENDAR_FILE, sheet_name="Sheet1")
_calendar_df.columns = _calendar_df.columns.str.strip()
_calendar_df["DATE"] = pd.to_datetime(_calendar_df["DATE"], errors="coerce")
_calendar_df = _calendar_df[_calendar_df["DATE"].notna()].copy()
_calendar_df["is_working_day"] = _calendar_df["status"].map({1: 1, 0: 0}).fillna(0)

# Calculate week ranges
_shifted = _calendar_df["DATE"] + pd.Timedelta(days=3)
iso = _shifted.dt.isocalendar()
_calendar_df["WEEK"] = iso["week"].astype(int)

def get_working_days_in_week(week):
    """Get working days for a specific week from calendar"""
    week_data = _calendar_df[_calendar_df["WEEK"] == week]
    if week_data.empty:
        return 6  # fallback
    return int(week_data["is_working_day"].sum())

# =========================
# CONFIG
# =========================
SETUP_DAYS = 5  # default setup days
BASE_DIR = Path(__file__).parent

def get_setup_days_for_item(yarn_used: str) -> int:
    """
    คำนวณ setup days ตาม YARN-USED (เหมือน Planning.py แต่ใช้ YARN-USED แทน MATERIAL_CONTENT)
    
    Logic:
    0. ถ้า YARN-USED เป็น pure COTTON (ไม่มี CD/POLY) → 3 วัน
    1. ถ้า YARN-USED เป็น POLY / CD / TC → 5 วัน
    2. ถ้า YARN-USED มีหลายเส้น (+) → 5 วัน
    3. default → 3 วัน
    """
    if pd.isna(yarn_used) or str(yarn_used).strip() == "":
        return SETUP_DAYS
    
    yarn_upper = str(yarn_used).strip().upper()
    
    # pure COTTON เท่านั้น (ไม่มี CD, POLY, TC ผสม)
    if ("COTTON" in yarn_upper or "COT" in yarn_upper) and \
       "CD" not in yarn_upper and "POLY" not in yarn_upper and "TC" not in yarn_upper:
        return 3
    elif "POLY" in yarn_upper or "CD" in yarn_upper or "TC" in yarn_upper:
        return 5
    
    # ตรวจสอบว่ามีหลายเส้น (มี +)
    if "+" in yarn_upper:
        return 5
    
    return SETUP_DAYS
BOOKING_DIR = BASE_DIR / "Booking"
MASTER_MC_FILE = BASE_DIR / "data" / "MC" / "Master_MC_5.xlsx"
OUTPUT_DIR = BASE_DIR / "data_plan"
OUTPUT_FILE = OUTPUT_DIR / "booking_final_ready25.xlsx"

EXCLUDE_MC_GROUP = [
    "CL-NP",
    "CL-OM",
    "COMKN",
    "F-CL",
    "CL",
    "FQCCL-NP",
    "FQCCL-OM",
    "FQC-Omnoi",
    "FQC-Phet",
    "FQC",
    "F-TSD",
]

KEEP_COLUMNS = [
    "MC_GROUP",
    "GUAGE",
    "ITEM_CODE",
    "SO_NO",
    "CAP ทอ",
    "REVOLUTION/WEIGHT",
    "KP_WEIGHT",
    "WEEK",
    "TYPE",
    "YARN-USED",
    "STRUCTURE",
]


# =========================
# LOAD CAPABILITY GROUP
# =========================
def load_capability_groups(file_path: str) -> pd.DataFrame:
    with pd.ExcelFile(file_path) as xls:
        records = []
        for sheet in xls.sheet_names:
            df_sheet = pd.read_excel(xls, sheet_name=sheet)
            df_sheet.columns = df_sheet.columns.str.strip()

            if {"MC_GROUP", "GUAGE", "Capability Group"}.issubset(df_sheet.columns):
                records.append(
                    df_sheet[["MC_GROUP", "GUAGE", "Capability Group"]]
                    .dropna()
                    .drop_duplicates()
                )

    if not records:
        return pd.DataFrame(columns=["MC_GROUP", "GUAGE", "Capability Group"])

    master = pd.concat(records, ignore_index=True)
    master["MC_GROUP"] = master["MC_GROUP"].astype(str).str.strip()
    master["GUAGE"] = master["GUAGE"].astype(str).str.strip()
    return master.drop_duplicates()


# =========================
# LOAD MASTER MC FROM FILE
# =========================
_MASTER_MC_PATH = Path(r"C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\MasterMC.xlsx")

def _load_master_mc(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip()
    df["MC"] = df["MC"].astype(str).str.strip().str.upper()
    df["Guage"] = df["Guage"].astype(str).str.strip()
    return df

try:
    _master_mc_df = _load_master_mc(_MASTER_MC_PATH)
    print(f"✅ โหลด MasterMC สำเร็จ: {len(_master_mc_df)} แถว จาก {_MASTER_MC_PATH}")
except Exception as _e_mmc:
    print(f"⚠️ โหลด MasterMC ไม่ได้ ({_e_mmc}) — ใช้ค่า default")
    _master_mc_df = pd.DataFrame(columns=["MC", "Guage", "Total MC", "Working Hours."])

# =========================
# 20 / 24 RULE  (Working Hours. == 20 → ต้องคูณ 20/24)
# =========================
MULTIPLY_RULES = {
    (row["MC"], row["Guage"])
    for _, row in _master_mc_df.iterrows()
    if str(row.get("Working Hours.", "")).strip() == "20"
}

# =========================
# MC กลุ่มที่ห้าม *20/24 อย่างชัดเจน (PHET DOUBLE + รายการพิเศษ)
# =========================
# ทุก MC group ต้องคูณ 20/24 หมดแล้ว
NO_MULTIPLY_RULES = set()  # ไม่มีข้อยกเว้น

# =========================
# WORKING DAY = 6
# =========================
WORKING_DAY_6 = set(MULTIPLY_RULES) | NO_MULTIPLY_RULES

# =========================
# ITEM SPECIAL: per-(Item, MC, Guage) override for Working day and Working hour
# Source: C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\Item Special.xlsx
# =========================
_ITEM_SPECIAL_FILE_AVA = Path(r"C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\Item Special.xlsx")
_ITEM_SPECIAL_LOOKUP_AVA: dict = {}  # key=(item_upper, mc_upper, gauge_str), value=(working_day, working_hour)


def _norm_gauge_ava(gauge) -> str:
    """Normalize gauge: 22.0 → 22"""
    if gauge is None or (isinstance(gauge, float) and pd.isna(gauge)):
        return ""
    s = str(gauge).strip()
    if not s or s.lower() == "nan":
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def _get_item_special_ava(item_code, mc_group, gauge=None):
    """Return (working_day, working_hour) from Item Special for (item, MC, gauge), or None."""
    if not item_code or not mc_group:
        return None
    item_u = str(item_code).strip().upper()
    mc_u = str(mc_group).strip().upper()
    g_u = _norm_gauge_ava(gauge)
    result = _ITEM_SPECIAL_LOOKUP_AVA.get((item_u, mc_u, g_u))
    if result is None and g_u:
        result = _ITEM_SPECIAL_LOOKUP_AVA.get((item_u, mc_u, ""))
    return result


try:
    _is_df_ava = pd.read_excel(_ITEM_SPECIAL_FILE_AVA)
    _is_df_ava.columns = _is_df_ava.columns.str.strip()
    for _, _is_row in _is_df_ava.iterrows():
        _is_mc = str(_is_row.get("MC", "")).strip().upper()
        _is_guage = _norm_gauge_ava(_is_row.get("Guage", ""))
        _is_item = str(_is_row.get("Item", "")).strip().upper()
        _is_wd = int(_is_row.get("Working day", 6) or 6)
        _is_wh = int(_is_row.get("Working hour", 20) or 20)
        if _is_item and _is_mc:
            _ITEM_SPECIAL_LOOKUP_AVA[(_is_item, _is_mc, _is_guage)] = (_is_wd, _is_wh)
    print(f"Item Special (AVA): {len(_ITEM_SPECIAL_LOOKUP_AVA)} entries loaded")
except Exception as _e_is_ava:
    print(f"Cannot load Item Special ({_e_is_ava}) -- using MasterMC defaults")
    _ITEM_SPECIAL_LOOKUP_AVA = {}

# =========================
# TOTAL MC MASTER  (โหลดจาก MasterMC.xlsx)
# =========================
TOTAL_MC_MAP = {
    (row["MC"], row["Guage"]): int(row["Total MC"])
    for _, row in _master_mc_df.iterrows()
    if pd.notna(row.get("Total MC")) and str(row.get("Total MC", "")).strip() != ""
}

# =========================
# SHARED POOL
# กลุ่ม MC ที่ใช้เครื่องร่วมกัน (pool) → TOTAL_MC_REMAIN จะถูกปรับให้สะท้อนเครื่องว่างรวม
# key = pool name, value = (total_machines, [(MC_GROUP, GUAGE), ...])
# =========================
SHARED_POOL_MAP = {
    "SKP_SKPTA_14_POOL": (5, [("SKP", "14"), ("SKPTA", "14")]),
    "SKPLE_SKPTA_26_POOL": (41, [("SKPLE", "26"), ("SKPTA", "26")]),
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
# =========================
import io


def fix_thai(s):
    """แก้ double-encoding: latin-1 → cp874"""
    try:
        return s.encode("latin-1").decode("cp874")
    except Exception:
        return s


def load_booking_file(file: Path) -> pd.DataFrame:
    raw_bytes = file.read_bytes()
    is_zip = raw_bytes[:2] == b"PK"
    is_biff = raw_bytes[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

    _df = None
    if is_zip:
        _tmp = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")
        if "\t" in str(_tmp.columns[0]):
            col_names = _tmp.columns[0].split("\t")
            data = _tmp.iloc[:, 0].astype(str).str.split("\t", expand=True)
            n = min(len(col_names), data.shape[1])
            data = data.iloc[:, :n]
            data.columns = [fix_thai(c) for c in col_names[:n]]
            data = data.map(lambda x: fix_thai(x) if isinstance(x, str) else x)
            _df = data
        else:
            _df = _tmp
    elif is_biff:
        _df = pd.read_excel(io.BytesIO(raw_bytes), engine="xlrd")
    else:
        for enc in ("cp874", "tis-620", "utf-8-sig", "latin-1"):
            try:
                raw_text = raw_bytes.decode(enc, errors="replace")
                _tmp = pd.read_csv(io.StringIO(raw_text), sep="\t", on_bad_lines="skip")
                if _tmp.shape[1] > 3:
                    _df = _tmp
                    break
            except Exception:
                continue

    if _df is None:
        raise ValueError(f"❌ ไม่สามารถอ่านไฟล์: {file.name}")
    return _df


all_files = [f for f in BOOKING_DIR.iterdir() if f.suffix.lower() in (".xlsx", ".xls")]
if not all_files:
    raise FileNotFoundError(f"❌ ไม่พบไฟล์ใน {BOOKING_DIR}")

df_list = []
for file in all_files:
    print(f"📄 Loading: {file.name}")
    _df = load_booking_file(file)
    _df["SOURCE_FILE"] = file.name
    df_list.append(_df)
    print(f"   ✅ rows={len(_df)}, columns={_df.shape[1]}")

df = pd.concat(df_list, ignore_index=True)
df.columns = df.columns.str.strip().str.upper()
print("📋 Columns:", df.columns.tolist())

# =========================
# CLEAN (❌ COLLAR ถูกลบถาวร)
# =========================
if "MC_GROUP" not in df.columns:
    raise KeyError(f"❌ ไม่พบ column 'MC_GROUP' - columns ที่มี: {df.columns.tolist()}")

df = df[~df["MC_GROUP"].isin(EXCLUDE_MC_GROUP)]

df["TYPE"] = df["TYPE"].astype(str).str.strip().str.upper()
df = df[df["TYPE"] != "COLLAR"]

df = df[[c for c in KEEP_COLUMNS if c in df.columns]]

df["GUAGE"] = df["GUAGE"].astype(str).str.strip()
df["CAP ทอ"] = pd.to_numeric(df["CAP ทอ"], errors="coerce")
df["KP_WEIGHT"] = pd.to_numeric(df["KP_WEIGHT"], errors="coerce")

# =========================
# APPLY 20/24 (เฉพาะ MC group ที่มี Working Hours. == 20 ใน MasterMC.xlsx)
# =========================
def _apply_cap_adj(r):
    _is = _get_item_special_ava(r["ITEM_CODE"], r["MC_GROUP"], r["GUAGE"])
    if _is is not None:
        return r["CAP ทอ"] * (_is[1] / 24)  # Item Special working_hour override
    elif (r["MC_GROUP"], r["GUAGE"]) in MULTIPLY_RULES:
        return r["CAP ทอ"] * (20 / 24)
    else:
        return r["CAP ทอ"]

df["_CAP_ADJ"] = df.apply(_apply_cap_adj, axis=1)

# =========================
# GROUP ITEM
# =========================
agg_dict = {
    "KP_WEIGHT": "sum",
    "CAP ทอ": "first",       # raw cap จาก booking (ไม่ปรับ 20/24)
    "_CAP_ADJ": "first",     # adjusted cap สำหรับคำนวณ MC_USE
    "REVOLUTION/WEIGHT": "first",
}
if "SO_NO" in df.columns:
    agg_dict["SO_NO"] = lambda x: ",".join(x.dropna().astype(str).unique())
for col in ["YARN-USED", "STRUCTURE"]:
    if col in df.columns:
        agg_dict[col] = "first"

df = df.groupby(["MC_GROUP", "GUAGE", "ITEM_CODE", "WEEK"], as_index=False).agg(
    agg_dict
)

# =========================
# FIBER TYPE (จาก YARN-USED)
# =========================
_yarn_df = load_yarn_master()
_fiber_lookup = dict(zip(_yarn_df["ITEM_CODE"], _yarn_df["FIBER_TYPE"]))


def get_fiber_type(yarn_used: str) -> str:
    """แยก YARN-USED ด้วย '+' แล้วเช็ค FIBER_TYPE แต่ละตัว
    ถ้ามีตัวใดเป็น POLY → POLY, ไม่งั้น None POLY"""
    if pd.isna(yarn_used) or str(yarn_used).strip() == "":
        return "None POLY"
    parts = [p.strip() for p in str(yarn_used).split("+") if p.strip()]
    for part in parts:
        if _fiber_lookup.get(part, "None POLY") == "POLY":
            return "POLY"
    return "None POLY"


if "YARN-USED" in df.columns:
    df["FIBER_TYPE"] = df["YARN-USED"].apply(get_fiber_type)

# =========================
# WORKING DAY
# =========================
def _get_working_day_for_row(r):
    cal_wd = get_working_days_in_week(int(r["WEEK"]))
    _is = _get_item_special_ava(r["ITEM_CODE"], r["MC_GROUP"], r["GUAGE"])
    if _is is not None:
        return min(cal_wd, _is[0])  # Item Special working_day override
    return cal_wd

df["WORKING_DAY"] = df.apply(_get_working_day_for_row, axis=1)

# =========================
# SETUP DETECTION (เหมือน Planning.py: SETUP_GAP_WEEK = 3)
# =========================
SETUP_GAP_WEEK = 3
df = df.sort_values(["MC_GROUP", "GUAGE", "ITEM_CODE", "WEEK"])

# สร้าง key สำหรับเช็ค carryover
df["_carry_key"] = df["MC_GROUP"] + "_" + df["GUAGE"].astype(str) + "_" + df["ITEM_CODE"]

# เช็คว่า item+MC_GROUP+GUAGE เดียวกันมีใน week ก่อนหน้าหรือไม่
df["_prev_week"] = df.groupby("_carry_key")["WEEK"].shift(1)
df["_week_gap"] = df["WEEK"] - df["_prev_week"]

# ถ้า gap > SETUP_GAP_WEEK หรือไม่มี week ก่อนหน้า → setup ใหม่
df["_is_new_setup"] = (df["_week_gap"] > SETUP_GAP_WEEK) | (df["_prev_week"].isna())

# คำนวณ setup days สำหรับแต่ละ item - ใช้ค่าคงที่ SETUP_DAYS = 5
df["_setup_days"] = SETUP_DAYS

# =========================
# MC USE (คำนวณเบื้องต้นด้วย WORKING_DAY เต็ม)
# =========================
df["MC_USE"] = np.where(
    df["_CAP_ADJ"] > 0,
    df["KP_WEIGHT"] / (df["_CAP_ADJ"] * df["WORKING_DAY"]),
    0
)

df["MC_USE_CEIL"] = np.ceil(df["MC_USE"]).fillna(0).astype(int)

# เช็ค MC_USE_CEIL ของ week ก่อนหน้า
df["_prev_mc_use_ceil"] = df.groupby("_carry_key")["MC_USE_CEIL"].shift(1)

# เช็คว่ามีการเพิ่มเครื่องหรือไม่
df["_mc_increase"] = df["MC_USE_CEIL"] - df["_prev_mc_use_ceil"]
df["_has_mc_increase"] = df["_mc_increase"] > 0

# =========================
# RECALCULATE MC USE (หัก SETUP_DAYS สำหรับ setup ใหม่และการเพิ่มเครื่อง)
# =========================
# Logic:
# - ถ้า _is_new_setup = True → ทุกเครื่องต้อง setup → หัก SETUP_DAYS ทั้งหมด
# - ถ้า _has_mc_increase = True → เครื่องที่เพิ่มต้อง setup → หัก SETUP_DAYS สำหรับเครื่องที่เพิ่ม
# - ถ้า carryover และไม่เพิ่มเครื่อง → ใช้ WORKING_DAY เต็ม

# คำนวณ effective working days
# - setup ใหม่: WORKING_DAY - SETUP_DAYS (ทุกเครื่องต้อง setup)
# - เพิ่มเครื่อง: WORKING_DAY - (SETUP_DAYS * (เครื่องที่เพิ่ม / เครื่องทั้งหมด))
# - carryover: WORKING_DAY เต็ม

df["_effective_working_days"] = np.where(
    df["_is_new_setup"],
    df["WORKING_DAY"] - df["_setup_days"],  # setup ใหม่: หัก setup days ทั้งหมด
    np.where(
        df["_has_mc_increase"] & (df["MC_USE_CEIL"] > 0),
        df["WORKING_DAY"] - (df["_setup_days"] * df["_mc_increase"] / df["MC_USE_CEIL"]),  # เพิ่มเครื่อง: หัก setup days สำหรับเครื่องที่เพิ่ม
        df["WORKING_DAY"]  # carryover: ใช้เต็ม
    )
)

# คำนวณ MC_USE ใหม่ด้วย _effective_working_days
df["MC_USE"] = np.where(
    (df["_CAP_ADJ"] > 0) & (df["_effective_working_days"] > 0),
    df["KP_WEIGHT"] / (df["_CAP_ADJ"] * df["_effective_working_days"]),
    0
)

df["MC_USE_CEIL"] = np.ceil(df["MC_USE"]).fillna(0).astype(int)

# drop _CAP_ADJ ก่อน save (ไม่ส่งออก)
df = df.drop(columns=["_CAP_ADJ"])

# =========================
# TOTAL MC
# =========================
df["TOTAL_MC"] = df.apply(
    lambda r: TOTAL_MC_MAP.get((r["MC_GROUP"], r["GUAGE"]), 0), axis=1
)

# =========================
# MC CUMULATIVE
# =========================
df = df.sort_values(["MC_GROUP", "GUAGE", "WEEK"])
df["MC_USE_CUM"] = df.groupby(["MC_GROUP", "GUAGE", "WEEK"])["MC_USE_CEIL"].cumsum()

df["TOTAL_MC_REMAIN"] = df["TOTAL_MC"] - df["MC_USE_CUM"]

# =========================
# SUMMARY
# =========================
mc_master = pd.DataFrame(
    [(k[0], k[1], v) for k, v in TOTAL_MC_MAP.items()],
    columns=["MC_GROUP", "GUAGE", "TOTAL_MC"],
)

week_master = pd.DataFrame({"WEEK": sorted(df["WEEK"].unique())})
mc_master["key"] = 1
week_master["key"] = 1

summary_base = mc_master.merge(week_master, on="key").drop(columns="key")

mc_use_week = df.groupby(["MC_GROUP", "GUAGE", "WEEK"], as_index=False).agg(
    {"MC_USE_CEIL": "sum"}
)

summary = summary_base.merge(mc_use_week, on=["MC_GROUP", "GUAGE", "WEEK"], how="left")

summary["MC_USE_CEIL"] = summary["MC_USE_CEIL"].fillna(0).astype(int)
summary["TOTAL_MC_REMAIN"] = summary["TOTAL_MC"] - summary["MC_USE_CEIL"]

# =========================
# APPLY SHARED POOL: ปรับ TOTAL_MC_REMAIN ให้สะท้อนเครื่องว่างรวมของ pool
# pool_remain = pool_total - sum(MC_USE_CEIL ของทุก member ใน week นั้น)
# ทุก member ใน pool จะเห็น TOTAL_MC_REMAIN = pool_remain เท่ากัน
# =========================
for _pool_name, (_pool_total, _pool_members) in SHARED_POOL_MAP.items():
    for _week in summary["WEEK"].unique():
        _week_mask = summary["WEEK"] == _week
        _member_mask = _week_mask & summary.apply(
            lambda r: (r["MC_GROUP"], str(r["GUAGE"])) in _pool_members, axis=1
        )
        _total_used = summary.loc[_member_mask, "MC_USE_CEIL"].sum()
        _pool_remain = max(0, _pool_total - _total_used)
        summary.loc[_member_mask, "TOTAL_MC"] = _pool_total
        summary.loc[_member_mask, "TOTAL_MC_REMAIN"] = _pool_remain

capability_groups = load_capability_groups(MASTER_MC_FILE)
summary = summary.merge(capability_groups, on=["MC_GROUP", "GUAGE"], how="left")

summary["CAPABILITY_TOTAL_MC_REMAIN"] = summary.groupby(["Capability Group", "WEEK"])[
    "TOTAL_MC_REMAIN"
].transform("sum")

summary = summary[
    [
        "MC_GROUP",
        "Capability Group",
        "GUAGE",
        "TOTAL_MC",
        "WEEK",
        "MC_USE_CEIL",
        "TOTAL_MC_REMAIN",
        "CAPABILITY_TOTAL_MC_REMAIN",
    ]
]

# =========================
# SAVE
# =========================
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="DETAIL", index=False)
    summary.to_excel(writer, sheet_name="SUMMARY_MC_REMAIN", index=False)

print("✅ AVA MC FINAL COMPLETE (COLLAR REMOVED)")
print("Saved:", OUTPUT_FILE)
print("DETAIL rows:", len(df))
print("SUMMARY rows:", len(summary))

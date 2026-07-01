import os
import re
import pandas as pd
import numpy as np
import sys
import configparser as _cp
from pathlib import Path
from Calendar import load_calendar

# Load calendar from SharePoint URL (auto-sync)
CALENDAR_FILE = "https://nanyangtextilegroup.sharepoint.com/:x:/s/SCM_Cloud/IQCXP4jH73zhQozDNvw1XF8OAY5m4p-UFv35Tcpza6v8mJo?e=43ffCc"
_calendar_df = load_calendar(CALENDAR_FILE, sheet_name="Sheet1")
_calendar_df.columns = _calendar_df.columns.str.strip()
_calendar_df["DATE"] = pd.to_datetime(_calendar_df["DATE"], errors="coerce")
_calendar_df = _calendar_df[_calendar_df["DATE"].notna()].copy()
_calendar_df["is_working_day"] = _calendar_df["status"].map({1: 1, 0: 0}).fillna(0)


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
BASE_DIR = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent
_cfg = _cp.ConfigParser(interpolation=None)  # ปิด interpolation ให้ใช้ %USERPROFILE% ใน path ได้
if not _cfg.read(BASE_DIR / "config.ini", encoding="utf-8"):
    raise FileNotFoundError(f"ไม่พบ config.ini ที่ {BASE_DIR / 'config.ini'} — กรุณาสร้างไฟล์ config.ini ก่อนรัน")

def _get_item_cotton_poly(item_code: str) -> str:
    """Return 'COTTON' if FD5/F5 prefix, 'POLY' if FD4/F4 prefix, '' otherwise."""
    item = str(item_code).strip().upper()
    if item.startswith("FD5") or item.startswith("F5"):
        return "COTTON"
    if item.startswith("FD4") or item.startswith("F4"):
        return "POLY"
    return ""


_mc_setup_time_map: dict = {}  # populated after _master_mc_df is loaded below

def get_setup_days_for_item(material_content: str, yarn_used: str, mc_group: str = "") -> int:
    """
    คำนวณ setup days โดยดู MasterMC column "Set up time" ก่อน ถ้า blank ใช้ MATERIAL_CONTENT/YARN_USED

    Logic:
    0. ถ้า mc_group มีค่าใน _mc_setup_time_map → ใช้ค่านั้น
    1. ถ้า MATERIAL_CONTENT เป็น COTTON → 3 วัน (ไม่สนใจ YARN_USED)
    2. ถ้า MATERIAL_CONTENT เป็น POLY → 5 วัน
    3. ถ้า MATERIAL_CONTENT เป็นอื่นๆ (CD, TC, CVC, CT, ฯลฯ) → เช็ค YARN_USED: DTY → 5 วัน
    4. ถ้าไม่มีทั้งสองอย่าง → default 3 วัน
    """
    if mc_group:
        _mc_u = str(mc_group).strip().upper()
        if _mc_u in _mc_setup_time_map:
            return _mc_setup_time_map[_mc_u]
    mat = str(material_content).strip().upper() if not pd.isna(material_content) else ""
    yarn = str(yarn_used).strip().upper() if not pd.isna(yarn_used) else ""

    if mat == "COTTON":
        if "DTY" in yarn:
            return 5
        return 3
    if mat == "POLY":
        return 5
    if mat:  # CD, TC, CVC, CT, ฯลฯ
        if "DTY" in yarn:
            return 5
        return 3
    return 3
BOOKING_DIR = BASE_DIR / "Booking"
MASTER_MC_FILE = Path(os.path.expandvars(_cfg["paths"]["master_mc"]))
if not MASTER_MC_FILE.exists():
    raise FileNotFoundError(
        f"ไม่พบ MasterMC.xlsx ที่ {MASTER_MC_FILE}\n"
        f"กรุณาแก้ path ใน config.ini หัวข้อ [paths] master_mc ให้ตรงกับเครื่องของคุณ"
    )
OUTPUT_DIR = BASE_DIR / "data_plan"
from datetime import date as _date_today
_d = _date_today.today()
OUTPUT_FILE = OUTPUT_DIR / f"booking_final_ready_{_d.day}-{_d.month}-{_d.year+543}.xlsx"

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
    "CAT",
    "MC_GROUP",
    "GUAGE",
    "ITEM_CODE",
    "SO_NO",
    "CAP_KNIT",
    "KP_WEIGHT",
    "WEEK",
    "TYPE",
    "YARN-USED",
    "YARN_USED",
    "STRUCTURE",
    "MATERIAL_CONTENT",
    "DESCRIPTION",
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

            # Rename columns to match expected format
            if "MC" in df_sheet.columns and "Guage" in df_sheet.columns and "Group" in df_sheet.columns:
                df_sheet = df_sheet.rename(columns={"MC": "MC_GROUP", "Guage": "GUAGE", "Group": "Capability Group"})
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
_MASTER_MC_PATH = MASTER_MC_FILE  # path จาก config.ini [paths] master_mc

def _load_master_mc(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip()
    # forward-fill merged cells (Excel merged → NaN in non-first rows)
    for _col in ["Factory", "Type", "MC_CAT", "MC"]:
        if _col in df.columns:
            df[_col] = df[_col].ffill()
    df["MC"] = df["MC"].astype(str).str.strip().str.upper()
    df["Guage"] = df["Guage"].astype(str).str.strip()
    return df

try:
    _master_mc_df = _load_master_mc(_MASTER_MC_PATH)
    print(f"✅ โหลด MasterMC สำเร็จ: {len(_master_mc_df)} แถว จาก {_MASTER_MC_PATH}")
except Exception as _e_mmc:
    print(f"⚠️ โหลด MasterMC ไม่ได้ ({_e_mmc}) — ใช้ค่า default")
    _master_mc_df = pd.DataFrame(columns=["MC", "Guage", "Total MC", "Working Hours."])

# MC → Setup time map: mc_upper → setup_days (จาก MasterMC column "Set up time")
for _, _mrow in _master_mc_df.iterrows():
    _mmc = str(_mrow.get("MC", "")).strip().upper()
    _st = _mrow.get("Set up time", None)
    if _mmc and _st is not None and str(_st).strip() not in ("", "nan", "NAN", "NaT", "None"):
        try:
            _mc_setup_time_map[_mmc] = int(float(_st))
        except (ValueError, TypeError):
            pass
print(f"✅ MC→SetupTime map: {len(_mc_setup_time_map)} entries")

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

# =========================
# WORKING HOURS MAP  (โหลดค่า Working Hours. จริงจาก MasterMC.xlsx)
# key=(MC, Guage), value=ชั่วโมง (int), default=24
# =========================
WORKING_HOURS_MAP: dict = {}
for _, _wh_row in _master_mc_df.iterrows():
    _wh_raw = _wh_row.get("Working Hours.", "")
    if pd.notna(_wh_raw) and str(_wh_raw).strip() not in ("", "-", "nan"):
        try:
            WORKING_HOURS_MAP[(_wh_row["MC"], _norm_gauge_ava(_wh_row["Guage"]))] = int(float(str(_wh_raw).strip()))
        except (ValueError, TypeError):
            pass

# =========================
# WORKING DAYS MAP  (โหลดค่า Working Day จริงจาก MasterMC.xlsx)
# key=(MC, Guage), value=วัน (float — รองรับทศนิยม เช่น 5.5), default=6
# =========================
WORKING_DAYS_MAP: dict = {}
_WORKING_DAYS_MC_ONLY: dict = {}  # fallback key=MC อย่างเดียว
MC_WEEK32_DAYS_MAP: dict = {}  # mc → วันทำงาน week 32 (REMARK blank=8, ไม่ blank=10) — ให้ตรงกับ Planning.py
for _, _wd_row in _master_mc_df.iterrows():
    _wd_raw = _wd_row.get("Working Day", "")
    if pd.notna(_wd_raw) and str(_wd_raw).strip() not in ("", "-", "nan"):
        try:
            _wd_val = float(str(_wd_raw).strip())
            _wd_mc = _wd_row["MC"]
            _wd_g = _norm_gauge_ava(_wd_row["Guage"])
            WORKING_DAYS_MAP[(_wd_mc, _wd_g)] = _wd_val
            if _wd_mc not in _WORKING_DAYS_MC_ONLY:
                _WORKING_DAYS_MC_ONLY[_wd_mc] = _wd_val
        except (ValueError, TypeError):
            pass
    # REMARK สำหรับ week 32 (ให้ตรงกับ Planning.py): blank → 8 วัน, ไม่ blank (เช่น "10 Days") → 10 วัน
    # ข้าม OUTSOURCE และใช้ค่าจากแถวแรกของแต่ละ MC (REMARK อยู่แถว merge แรก)
    if str(_wd_row.get("Factory", "")).strip().upper() != "OUTSOURCE":
        _w32_mc = str(_wd_row.get("MC", "")).strip().upper()
        if _w32_mc and _w32_mc not in MC_WEEK32_DAYS_MAP:
            _remark_raw = _wd_row.get("REMARK", "")
            _remark_blank = pd.isna(_remark_raw) or str(_remark_raw).strip().lower() in ("", "nan", "-")
            MC_WEEK32_DAYS_MAP[_w32_mc] = 8 if _remark_blank else 10

# =========================
# ITEM SPECIAL: per-(Item, MC, Guage) override for Working day and Working hour
# Source: MasterMC.xlsx sheet "Item Special"
# =========================
_ITEM_SPECIAL_LOOKUP_AVA: dict = {}  # key=(item_upper, mc_upper, gauge_str), value=(working_day, working_hour)


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
    _is_df_ava = pd.read_excel(MASTER_MC_FILE, sheet_name="Item Special")
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
# MC SPECIAL: per-(Factory, MC_CAT, MC, Gauge) → POLY/COTTON machine count
# Source: MasterMC.xlsx sheet "MC Special"
# ถ้า ITEM_CODE ขึ้นต้น FD5/F5 → ใช้ pool COTTON, FD4/F4 → ใช้ pool POLY
# =========================
_MC_SPECIAL_LOOKUP: dict = {}  # key=(factory_upper, mc_cat_upper, mc_upper, gauge_str) → {"POLY": int, "COTTON": int}

try:
    _mcs_df = pd.read_excel(MASTER_MC_FILE, sheet_name="MC Special")
    _mcs_df.columns = _mcs_df.columns.str.strip()
    for _, _mcs_row in _mcs_df.iterrows():
        _mcs_fac = str(_mcs_row.get("Factory", "")).strip().upper()
        _mcs_cat = str(_mcs_row.get("MC_CAT", "")).strip().upper()
        _mcs_mc_raw = str(_mcs_row.get("MC", "")).strip().upper()
        _mcs_mc = "" if _mcs_mc_raw in ("NAN", "NONE", "") else _mcs_mc_raw
        _mcs_g = _norm_gauge_ava(_mcs_row.get("Guage", ""))
        if not _mcs_fac or not _mcs_cat or not _mcs_g:
            continue
        _mcs_poly_raw = _mcs_row.get("POLY")
        _mcs_cotton_raw = _mcs_row.get("COTTON")
        _mcs_poly = int(_mcs_poly_raw) if pd.notna(_mcs_poly_raw) and str(_mcs_poly_raw).strip() not in ("", "-") else 0
        _mcs_cotton = int(_mcs_cotton_raw) if pd.notna(_mcs_cotton_raw) and str(_mcs_cotton_raw).strip() not in ("", "-") else 0
        if _mcs_poly > 0 or _mcs_cotton > 0:
            _MC_SPECIAL_LOOKUP[(_mcs_fac, _mcs_cat, _mcs_mc, _mcs_g)] = {"POLY": _mcs_poly, "COTTON": _mcs_cotton}
    print(f"MC Special: {len(_MC_SPECIAL_LOOKUP)} entries loaded")
    for _k, _v in _MC_SPECIAL_LOOKUP.items():
        print(f"  {_k} → {_v}")
except Exception as _e_mcs:
    print(f"Cannot load MC Special ({_e_mcs}) -- no Cotton/Poly split")
    _MC_SPECIAL_LOOKUP = {}

# --- Type-based description rules (Factory+Type rows ใน MC Special) ---
# key=(factory_upper, type_upper) → {max_mc: int, keywords: list[str]}
_TYPE_DESC_RULES: dict = {}

def _is_description_special_type(desc: str, keywords: list) -> bool:
    """Return True ถ้า DESCRIPTION match keyword ใดๆ แต่ถ้ามี FRENCH TERRY → False เสมอ"""
    d = str(desc).strip().upper()
    if "FRENCH TERRY" in d:
        return False
    base_kws = [kw.lstrip("$") for kw in keywords]
    return any(kw in d for kw in base_kws)

try:
    _mcs_df2 = pd.read_excel(MASTER_MC_FILE, sheet_name="MC Special")
    _mcs_df2.columns = _mcs_df2.columns.str.strip()
    _kw_col = "Unnamed: 8" if "Unnamed: 8" in _mcs_df2.columns else None
    _cur_key = None
    _cur_kws: list = []
    _cur_max = 0
    _cur_cat = ""
    for _, _mcs2_row in _mcs_df2.iterrows():
        _mcs2_fac  = str(_mcs2_row.get("Factory", "")).strip().upper()
        _mcs2_type = str(_mcs2_row.get("Type",    "")).strip().upper()
        _mcs2_cat  = str(_mcs2_row.get("MC_CAT",  "")).strip().upper()
        if _mcs2_cat == "NAN":
            _mcs2_cat = ""
        _mcs2_desc = _mcs2_row.get("DESCRIPTION")
        _mcs2_kw   = str(_mcs2_row.get(_kw_col, "") if _kw_col else "").strip().upper()
        _mcs2_guage_raw = _mcs2_row.get("Guage")
        _mcs2_has_guage = pd.notna(_mcs2_guage_raw) and str(_mcs2_guage_raw).strip() not in ("", "-", "NAN")
        # TYPE_SPECIAL row: มี Factory+Type แต่ไม่มี Guage
        if (_mcs2_fac not in ("", "NAN")
                and _mcs2_type not in ("", "NAN")
                and not _mcs2_has_guage):
            if _cur_key and _cur_kws:
                _TYPE_DESC_RULES[_cur_key] = {"max_mc": _cur_max, "keywords": _cur_kws[:], "mc_cat": _cur_cat}
            _cur_key  = (_mcs2_fac, _mcs2_type)
            _cur_cat  = _mcs2_cat  # เก็บ MC_CAT จาก rule (เช่น SINGLE-32)
            _cur_max  = int(_mcs2_desc) if pd.notna(_mcs2_desc) and str(_mcs2_desc).strip() not in ("", "-") else 0
            _cur_kws  = [_mcs2_kw] if _mcs2_kw and _mcs2_kw != "NAN" else []
        elif _mcs2_fac in ("", "NAN") and _mcs2_type in ("", "NAN") and _mcs2_kw not in ("", "NAN"):
            if _cur_key is not None:
                _cur_kws.append(_mcs2_kw)
    if _cur_key and _cur_kws:
        _TYPE_DESC_RULES[_cur_key] = {"max_mc": _cur_max, "keywords": _cur_kws[:], "mc_cat": _cur_cat}
    print(f"Type DESC rules: {len(_TYPE_DESC_RULES)} entries")
    for _k, _v in _TYPE_DESC_RULES.items():
        print(f"  {_k} → mc_cat={_v['mc_cat']!r}, max_mc={_v['max_mc']}, keywords={_v['keywords']}")
except Exception as _e_tdr:
    print(f"Cannot parse Type DESC rules ({_e_tdr})")
    _TYPE_DESC_RULES = {}

# =========================
# TOTAL MC MASTER  (โหลดจาก MasterMC.xlsx)
# =========================
TOTAL_MC_MAP = {
    (row["MC"], row["Guage"]): int(row["Total MC"])
    for _, row in _master_mc_df.iterrows()
    if pd.notna(row.get("Total MC")) and str(row.get("Total MC", "")).strip() not in ("", "-")
}

# TOTAL_MC รวมตาม Type_1 + Guage (จาก MasterMC)
# แก้บั๊ก sum ซ้ำซ้อน กรณี MC_CAT/GUAGE เดียวกันมีหลายแถวในไฟล์ และบังคับแปลง Total MC เป็นตัวเลข
_mc_cat_g_sum = (
    _master_mc_df.dropna(subset=["MC_CAT", "Guage", "Total MC"])
    .copy()
)
_mc_cat_g_sum["Total MC"] = pd.to_numeric(_mc_cat_g_sum["Total MC"], errors='coerce')
_mc_cat_g_sum = (
    _mc_cat_g_sum.dropna(subset=["Total MC"])
    .groupby(["MC_CAT", "Guage"], as_index=False)["Total MC"]
    .sum()
)
TOTAL_MC_MAP_TYPE1 = {
    (str(row["MC_CAT"]).strip(), str(row["Guage"]).strip()): int(row["Total MC"])
    for _, row in _mc_cat_g_sum.iterrows()
}



# =========================
# SHARED POOL — รวมเครื่องตาม Type ใน MasterMC
# แยกตาม Factory: เครื่องคนละ Factory ไม่นำมารวมกัน
# pool_key format: "Factory|Type_1:Guage" หรือ "Factory|Group"
# =========================
_MC_TO_POOL: dict = {}      # (MC_upper, norm_guage) → pool_key
_MC_TO_FACTORY: dict = {}   # (MC_upper, norm_guage) → factory_str
_MC_TO_TYPE: dict = {}      # (MC_upper, norm_guage) → type_str (SINGLE/DOUBLE/...)
_TOTAL_MC_BY_TYPE: dict = {}   # pool_key → total machines

# รวบรวมข้อมูล MasterMC
_t1g_total: dict = {}    # (factory, Type_1_upper, norm_guage) → total
_group_t1gs: dict = {}   # group_str → set of (factory, Type_1_upper, norm_guage)
_mc_to_group: dict = {}  # (MC_upper, norm_guage) → group_str
_mc_to_t1g: dict = {}    # (MC_upper, norm_guage) → (Type_1_upper, norm_guage)

for _, _row in _master_mc_df.iterrows():
    _mc  = str(_row.get("MC",      "")).strip().upper()
    _t1  = str(_row.get("MC_CAT",  "")).strip().upper()
    _g   = _norm_gauge_ava(_row.get("Guage", ""))
    _grp = str(_row.get("Group",   "")).strip()
    _fac = str(_row.get("Factory", "")).strip()
    _tot = _row.get("Total MC")
    if not _mc or not _t1 or not _g:
        continue
    _tot_int = 0
    if pd.notna(_tot) and str(_tot).strip() not in ("", "-"):
        try:
            _tot_int = int(_tot)
        except (ValueError, TypeError):
            pass
    if _tot_int > 0:
        _t1g_total[(_fac, _t1, _g)] = _t1g_total.get((_fac, _t1, _g), 0) + _tot_int
    if _grp:
        _group_t1gs.setdefault(_grp, set()).add((_fac, _t1, _g))
        _mc_to_group[(_mc, _g)] = _grp
    _mc_to_t1g[(_mc, _g)] = (_t1, _g)
    _MC_TO_FACTORY[(_mc, _g)] = _fac
    _MC_TO_TYPE[(_mc, _g)] = str(_row.get("Type", "")).strip().upper()

# Groups ที่ span ข้าม Type_1+Guage ภายใน Factory เดียวกัน (cross-pool)
_cross_groups = set()
for _grp, _fac_t1gs in _group_t1gs.items():
    _per_fac: dict = {}
    for (_fac, _t1, _g) in _fac_t1gs:
        _per_fac.setdefault(_fac, set()).add((_t1, _g))
    if any(len(v) > 1 for v in _per_fac.values()):
        _cross_groups.add(_grp)

# Build pool key ต่อ MC (รวม Factory เสมอ):
# - cross-pool → "Factory|Group name"
# - T1G pool   → "Factory|Type_1:Guage"
# Special: SINGLE-32 MC SKP → แยก pool ไม่รวมกับ SINGLE-32 MC อื่น
#   - SKP ทุก Guage → "Factory|SINGLE-32:SKP:Guage"  (ไม่รวมกับ SINGLE-32 MC อื่น)
#   - SKP Guage 20  → "Factory|SINGLE-32:SKP:20"     (โดดเดี่ยว ไม่รวมกับใคร)
for (_mc, _g), (_t1, _gt) in _mc_to_t1g.items():
    _grp = _mc_to_group.get((_mc, _g), "")
    _fac = _MC_TO_FACTORY.get((_mc, _g), "")
    _fac_pfx = f"{_fac}|" if _fac else ""
    if _t1 == "SINGLE-32" and _mc == "SKP":
        _MC_TO_POOL[(_mc, _g)] = f"{_fac_pfx}SINGLE-32:SKP:{_gt}"
    else:
        _MC_TO_POOL[(_mc, _g)] = (
            f"{_fac_pfx}{_grp}" if (_grp and _grp in _cross_groups)
            else f"{_fac_pfx}{_t1}:{_gt}"
        )

# Build _TOTAL_MC_BY_TYPE
for (_fac, _t1, _g), _tot in _t1g_total.items():
    _fac_pfx = f"{_fac}|" if _fac else ""
    _TOTAL_MC_BY_TYPE[f"{_fac_pfx}{_t1}:{_g}"] = _tot    # T1G pool

for _grp in _cross_groups:
    _per_fac_tots: dict = {}
    for (_fac, _t1, _g) in _group_t1gs[_grp]:
        _fac_pfx = f"{_fac}|" if _fac else ""
        _pk = f"{_fac_pfx}{_grp}"
        _per_fac_tots[_pk] = _per_fac_tots.get(_pk, 0) + _t1g_total.get((_fac, _t1, _g), 0)
    _TOTAL_MC_BY_TYPE.update(_per_fac_tots)

# === Special: ตัด SINGLE-32 SKP ออกจาก pool รวม และสร้าง pool แยกต่างหาก ===
# SKP ทุก Guage → pool แยกตาม Guage เฉพาะ (ไม่รวมกับ SINGLE-32 MC อื่น)
# SKP Guage 20  → pool โดดเดี่ยวสมบูรณ์ (pool key ต่างจาก SKP Guage อื่น)
_skp_by_fac_g: dict = {}
for _, _skp_row in _master_mc_df.iterrows():
    if (str(_skp_row.get("MC", "")).strip().upper() == "SKP"
            and str(_skp_row.get("MC_CAT", "")).strip().upper() == "SINGLE-32"):
        _sg = _norm_gauge_ava(_skp_row.get("Guage", ""))
        _sf = str(_skp_row.get("Factory", "")).strip()
        _st_raw = _skp_row.get("Total MC")
        _st = int(_st_raw) if pd.notna(_st_raw) and str(_st_raw).strip() not in ("", "-") else 0
        if _st > 0:
            _skp_by_fac_g[(_sf, _sg)] = _skp_by_fac_g.get((_sf, _sg), 0) + _st
for (_sf, _sg), _skp_tot in _skp_by_fac_g.items():
    _sf_pfx = f"{_sf}|" if _sf else ""
    _skp_pk = f"{_sf_pfx}SINGLE-32:SKP:{_sg}"
    _orig_pk = f"{_sf_pfx}SINGLE-32:{_sg}"
    _TOTAL_MC_BY_TYPE[_skp_pk] = _skp_tot
    if _orig_pk in _TOTAL_MC_BY_TYPE:
        _TOTAL_MC_BY_TYPE[_orig_pk] = max(0, _TOTAL_MC_BY_TYPE[_orig_pk] - _skp_tot)

# === Type-based DESC special pools ===
for (_tdr_fac, _tdr_type), _tdr_val in _TYPE_DESC_RULES.items():
    _tdr_pfx = f"{_tdr_fac}|" if _tdr_fac else ""
    _TOTAL_MC_BY_TYPE[f"{_tdr_pfx}{_tdr_type}:TYPE_SPECIAL"] = _tdr_val["max_mc"]

# === MC Special sub-pools: สร้าง COTTON/POLY sub-pool และลด normal pool ===
for (_ms_fac, _ms_cat, _ms_mc, _ms_g), _ms_vals in _MC_SPECIAL_LOOKUP.items():
    _ms_fac_pfx = f"{_ms_fac}|" if _ms_fac else ""
    _ms_normal_pk = f"{_ms_fac_pfx}{_ms_cat}:{_ms_g}"
    _ms_sub_base = f"{_ms_fac_pfx}{_ms_mc}:{_ms_g}" if _ms_mc else f"{_ms_fac_pfx}{_ms_cat}:{_ms_g}"
    if _ms_vals["COTTON"] > 0:
        _TOTAL_MC_BY_TYPE[f"{_ms_sub_base}:COTTON"] = _ms_vals["COTTON"]
        if _ms_normal_pk in _TOTAL_MC_BY_TYPE:
            _TOTAL_MC_BY_TYPE[_ms_normal_pk] = max(0, _TOTAL_MC_BY_TYPE[_ms_normal_pk] - _ms_vals["COTTON"])
    if _ms_vals["POLY"] > 0:
        _TOTAL_MC_BY_TYPE[f"{_ms_sub_base}:POLY"] = _ms_vals["POLY"]
        if _ms_normal_pk in _TOTAL_MC_BY_TYPE:
            _TOTAL_MC_BY_TYPE[_ms_normal_pk] = max(0, _TOTAL_MC_BY_TYPE[_ms_normal_pk] - _ms_vals["POLY"])

print(f"📊 Pools by Factory (sample): { dict(list(_TOTAL_MC_BY_TYPE.items())[:10]) }")
if not _master_mc_df.empty and "Group" in _master_mc_df.columns:
    _show_cols = [c for c in ["Factory", "Type", "MC_CAT", "MC", "Guage", "Total MC", "Group"] if c in _master_mc_df.columns]
    print(_master_mc_df[_show_cols].head(15).to_string())

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
if "CAT" in df.columns:
    print(f"📦 CAT unique values:\n{df['CAT'].dropna().unique().tolist()}")
if "MC_GROUP" in df.columns:
    print(f"🔧 MC_GROUP unique values:\n{df['MC_GROUP'].dropna().unique().tolist()}")

# =========================
# CLEAN (❌ COLLAR ถูกลบถาวร)
# =========================
if "MC_GROUP" not in df.columns:
    raise KeyError(f"❌ ไม่พบ column 'MC_GROUP' - columns ที่มี: {df.columns.tolist()}")

df = df[~df["MC_GROUP"].isin(EXCLUDE_MC_GROUP)]

df["TYPE"] = df["TYPE"].astype(str).str.strip().str.upper()
df = df[df["TYPE"] != "COLLAR"]

df = df[[c for c in KEEP_COLUMNS if c in df.columns]]

# ใช้ CAT จาก booking โดยตรง (ตรงกับ Type_1 ใน MasterMC)
df["MC_CAT"] = df["CAT"].astype(str).str.strip().str.upper() if "CAT" in df.columns else df["MC_GROUP"].astype(str).str.strip().str.upper()

df["GUAGE"] = df["GUAGE"].astype(str).str.strip()
df["CAP_KNIT"] = pd.to_numeric(df["CAP_KNIT"], errors="coerce")
df["KP_WEIGHT"] = pd.to_numeric(df["KP_WEIGHT"], errors="coerce")

# =========================
# APPLY 20/24 (เฉพาะ MC group ที่มี Working Hours. == 20 ใน MasterMC.xlsx)
# =========================
def _apply_cap_adj(r):
    _is = _get_item_special_ava(r["ITEM_CODE"], r["MC_GROUP"], r["GUAGE"])
    if _is is not None:
        return r["CAP_KNIT"] * (_is[1] / 24)  # Item Special working_hour override
    wh = WORKING_HOURS_MAP.get((r["MC_GROUP"], _norm_gauge_ava(r["GUAGE"])), 24)
    return r["CAP_KNIT"] * (wh / 24)

df["_CAP_ADJ"] = df.apply(_apply_cap_adj, axis=1)

# =========================
# GROUP ITEM
# =========================
agg_dict = {
    "KP_WEIGHT": "sum",
    "CAP_KNIT": "first",
    "_CAP_ADJ": "first",
    "MC_CAT": "first",
}
if "CAT" in df.columns:
    agg_dict["CAT"] = "first"
if "SO_NO" in df.columns:
    agg_dict["SO_NO"] = lambda x: ",".join(x.dropna().astype(str).unique())
for col in ["YARN-USED", "YARN_USED", "STRUCTURE", "MATERIAL_CONTENT", "DESCRIPTION"]:
    if col in df.columns:
        agg_dict[col] = "first"

df = df.groupby(["MC_GROUP", "GUAGE", "ITEM_CODE", "WEEK"], as_index=False).agg(
    agg_dict
)

# =========================
# WORKING DAY
# =========================
def _get_working_day_for_row(r):
    _week = int(r["WEEK"])
    # เงื่อนไขพิเศษ week 17/32 — override ทุกอย่าง ไม่หัก holiday (ให้ตรงกับ Planning.py)
    if _week == 17:
        return 8.0  # week 17 ทุก group ทำงาน 8 วัน
    if _week == 32:
        # REMARK ใน MasterMC: blank=8 วัน, ไม่ blank=10 วัน
        return float(MC_WEEK32_DAYS_MAP.get(r["MC_GROUP"], 8))

    cal_wd = get_working_days_in_week(_week)
    holiday_count = max(0, 6 - cal_wd)  # วันหยุดพิเศษ (อาทิตย์หยุดปกติอยู่แล้ว → base=6)

    _is = _get_item_special_ava(r["ITEM_CODE"], r["MC_GROUP"], r["GUAGE"])
    if _is is not None:
        return float(max(1, _is[0] - holiday_count))  # Item Special working_day หักวันหยุด

    _g = _norm_gauge_ava(r["GUAGE"])
    mc_wd = WORKING_DAYS_MAP.get((r["MC_GROUP"], _g),
            _WORKING_DAYS_MC_ONLY.get(r["MC_GROUP"], 6))
    return float(max(1, mc_wd - holiday_count))

df["WORKING_DAY"] = df.apply(_get_working_day_for_row, axis=1, result_type="reduce")

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

# คำนวณ setup days สำหรับแต่ละ item ตาม MATERIAL_CONTENT และ YARN-USED
df["_setup_days"] = df.apply(
    lambda r: get_setup_days_for_item(
        r.get("MATERIAL_CONTENT", ""),
        r.get("YARN-USED", "") or r.get("YARN_USED", ""),
        mc_group=r.get("MC_GROUP", "")
    ), axis=1
)

# =========================
# MC USE — INITIAL (คำนวณด้วย WORKING_DAY เต็ม)
# =========================
df["MC_USE"] = np.where(
    df["_CAP_ADJ"] > 0,
    df["KP_WEIGHT"] / (df["_CAP_ADJ"] * df["WORKING_DAY"]),
    0
)
df["MC_USE_CEIL"] = np.ceil(df["MC_USE"]).fillna(0).astype(int)

# =========================
# PASS 1: eff_days หัก setup เฉพาะ _is_new_setup
# → ได้ FINAL MC_USE_CEIL ที่สะท้อนเครื่องจริงที่รันใน setup week
# =========================
_eff_pass1 = np.where(
    df["_is_new_setup"],
    np.maximum(df["WORKING_DAY"] - df["_setup_days"], 0.5),
    df["WORKING_DAY"]
)
df["_mc_use_ceil_pass1"] = np.ceil(
    np.where(
        (df["_CAP_ADJ"] > 0) & (_eff_pass1 > 0),
        df["KP_WEIGHT"] / (df["_CAP_ADJ"] * _eff_pass1),
        0
    )
).astype(float)
df["_mc_use_ceil_pass1"] = df["_mc_use_ceil_pass1"].fillna(0).astype(int)

# =========================
# PASS 2: mc_increase เทียบกับ FINAL ของ week ก่อนหน้า (ไม่ใช่ INITIAL)
# carry machines = เครื่องที่รันจริงใน prev week (FINAL)
# new machines   = INITIAL ปัจจุบัน - FINAL prev (ส่วนที่เพิ่มขึ้นจริง)
# =========================
# carry-over machines = เครื่องจาก "สัปดาห์ล่าสุดที่ยังรันจริง (pass1 > 0)" ที่ยังอยู่ใน SETUP_GAP_WEEK
# เดิมใช้ shift(1) มองแค่สัปดาห์ติดกัน → ถ้ามีสัปดาห์ KP=0 คั่นกลาง เครื่องที่กลับมารัน (rerun)
# จะถูกนับเป็น "เครื่องใหม่" ผิด ทำให้โดน setup penalty ซ้ำและพองจำนวนเครื่องเกินจริง
# (เช่น item รัน week 25 → ว่าง 26,27 → กลับมา week 28 ควร carry 2 เครื่องเดิม ไม่ใช่ setup ใหม่)
def _carry_prev_active_ceil(g):
    prev_vals = []
    last_week = None
    last_ceil = 0
    for wk, c in zip(g["WEEK"].tolist(), g["_mc_use_ceil_pass1"].tolist()):
        if last_week is not None and (wk - last_week) <= SETUP_GAP_WEEK:
            prev_vals.append(last_ceil)   # ยังอยู่ใน gap → carry เครื่องจากสัปดาห์ active ล่าสุด
        else:
            prev_vals.append(np.nan)      # นอก gap / ครั้งแรก → ไม่มี carry (= setup ใหม่)
        if c > 0:                          # อัปเดต active week เฉพาะสัปดาห์ที่มีเครื่องรันจริง
            last_week = wk
            last_ceil = c
    return pd.Series(prev_vals, index=g.index)

df["_prev_mc_use_ceil"] = df.groupby("_carry_key", group_keys=False).apply(_carry_prev_active_ceil)
df["_mc_increase"] = np.maximum(
    df["MC_USE_CEIL"] - df["_prev_mc_use_ceil"].fillna(0), 0
).astype(int)
df["_has_mc_increase"] = (df["_mc_increase"] > 0) & (~df["_is_new_setup"])

# =========================
# คำนวณ effective working days
# - new_setup : ทุกเครื่องต้อง setup → WORKING_DAY - setup_days
# - mc_increase: เฉพาะเครื่องที่เพิ่ม setup → weighted average
# - carry       : วันเต็ม
# =========================
df["_effective_working_days"] = np.where(
    df["_is_new_setup"],
    np.maximum(df["WORKING_DAY"] - df["_setup_days"], 0.5),
    np.where(
        df["_has_mc_increase"] & (df["MC_USE_CEIL"] > 0),
        np.maximum(df["WORKING_DAY"] - (df["_setup_days"] * df["_mc_increase"] / df["MC_USE_CEIL"]), 0.5),
        df["WORKING_DAY"]
    )
)

df["MC_USE"] = np.where(
    (df["_CAP_ADJ"] > 0) & (df["_effective_working_days"] > 0),
    df["KP_WEIGHT"] / (df["_CAP_ADJ"] * df["_effective_working_days"]),
    0
)
df["MC_USE_CEIL"] = np.ceil(df["MC_USE"]).fillna(0).astype(int)

# drop temp columns
df = df.drop(columns=["_CAP_ADJ", "_mc_use_ceil_pass1"])

# =========================
# TOTAL MC
# =========================
# POOL_TYPE: ดูจาก Type ใน MasterMC ว่า (MC_GROUP, GUAGE) อยู่ใน pool ไหน
# ถ้า MC อยู่ใน MC Special → แยก sub-pool ตาม ITEM_CODE prefix (FD5/F5=COTTON, FD4/F4=POLY)
def _get_pool_type_for_row(r):
    mc_u = str(r["MC_GROUP"]).strip().upper()
    g_norm = _norm_gauge_ava(r["GUAGE"])
    normal_pool = _MC_TO_POOL.get((mc_u, g_norm), "")
    if not normal_pool:
        return normal_pool
    fac_u = _MC_TO_FACTORY.get((mc_u, g_norm), "").strip().upper()
    t1, _ = _mc_to_t1g.get((mc_u, g_norm), ("", ""))
    fac_pfx = f"{fac_u}|" if fac_u else ""

    # 1. MC Special: POLY/COTTON split ตาม ITEM_CODE prefix (priority สูงสุด)
    ms_key_specific = (fac_u, t1, mc_u, g_norm)
    ms_key_general  = (fac_u, t1, "", g_norm)
    if ms_key_specific in _MC_SPECIAL_LOOKUP:
        ms_entry = _MC_SPECIAL_LOOKUP[ms_key_specific]
        sub_base = f"{fac_pfx}{mc_u}:{g_norm}"
    elif ms_key_general in _MC_SPECIAL_LOOKUP:
        ms_entry = _MC_SPECIAL_LOOKUP[ms_key_general]
        sub_base = f"{fac_pfx}{t1}:{g_norm}"
    else:
        ms_entry = None
        sub_base = ""
    if ms_entry:
        item_type = _get_item_cotton_poly(r.get("ITEM_CODE", ""))
        if item_type == "COTTON" and ms_entry["COTTON"] > 0:
            return f"{sub_base}:COTTON"
        if item_type == "POLY" and ms_entry["POLY"] > 0:
            return f"{sub_base}:POLY"

    # 2. Type-based description rules (Factory+Type ใน MC Special)
    mc_type = _MC_TO_TYPE.get((mc_u, g_norm), "").strip().upper()
    tdr_key = (fac_u, mc_type)
    if tdr_key in _TYPE_DESC_RULES:
        rule = _TYPE_DESC_RULES[tdr_key]
        rule_mc_cat = rule.get("mc_cat", "")
        # ตรวจ MC_CAT ตรงกับ rule และ ยกเว้น Gauge 20
        if (rule_mc_cat and t1 != rule_mc_cat) or g_norm == "20":
            return normal_pool
        booking_desc = str(r.get("DESCRIPTION", ""))
        if _is_description_special_type(booking_desc, rule["keywords"]):
            return f"{fac_pfx}{mc_type}:TYPE_SPECIAL"

    return normal_pool

df["POOL_TYPE"] = df.apply(_get_pool_type_for_row, axis=1)

# DESC_POOL_TYPE: secondary pool สำหรับ description-based rules
# กำหนดให้ทุก row ที่ match TYPE_SPECIAL description (ไม่ว่า POOL_TYPE หลักจะเป็นอะไร)
def _get_desc_pool_for_row(r):
    mc_u = str(r["MC_GROUP"]).strip().upper()
    g_norm = _norm_gauge_ava(r["GUAGE"])
    fac_u = _MC_TO_FACTORY.get((mc_u, g_norm), "").strip().upper()
    mc_type = _MC_TO_TYPE.get((mc_u, g_norm), "").strip().upper()
    tdr_key = (fac_u, mc_type)
    if tdr_key in _TYPE_DESC_RULES:
        rule = _TYPE_DESC_RULES[tdr_key]
        rule_mc_cat = rule.get("mc_cat", "")
        t1, _ = _mc_to_t1g.get((mc_u, g_norm), ("", ""))
        if (rule_mc_cat and t1 != rule_mc_cat) or g_norm == "20":
            return ""
        if _is_description_special_type(str(r.get("DESCRIPTION", "")), rule["keywords"]):
            fac_pfx = f"{fac_u}|" if fac_u else ""
            return f"{fac_pfx}{mc_type}:TYPE_SPECIAL"
    return ""

df["DESC_POOL_TYPE"] = df.apply(_get_desc_pool_for_row, axis=1)

df["FACTORY"] = df.apply(
    lambda r: _MC_TO_FACTORY.get((str(r["MC_GROUP"]).strip().upper(), _norm_gauge_ava(r["GUAGE"])), ""), axis=1
)
print(f"🔍 POOL_TYPE found: {(df['POOL_TYPE'] != '').sum()} rows / {len(df)} total")
print(f"🔍 Sample FACTORY+MC_CAT+GUAGE → POOL_TYPE:\n{df[['FACTORY','MC_CAT','GUAGE','POOL_TYPE']].drop_duplicates().head(15).to_string()}")

# TOTAL_MC: pool → ใช้ pool total; ไม่มี pool → ใช้ per-(MC_GROUP, GUAGE)
df["TOTAL_MC"] = df.apply(
    lambda r: _TOTAL_MC_BY_TYPE[r["POOL_TYPE"]]
              if r["POOL_TYPE"] and r["POOL_TYPE"] in _TOTAL_MC_BY_TYPE
              else TOTAL_MC_MAP.get((r["MC_GROUP"], r["GUAGE"]), 0),
    axis=1
)

# =========================
# MC CUMULATIVE
# =========================
df = df.sort_values(["MC_GROUP", "GUAGE", "WEEK"])
df["MC_USE_CUM"] = df.groupby(["MC_GROUP", "GUAGE", "WEEK"])["MC_USE_CEIL"].cumsum()

# Pool-level usage per WEEK: รวม MC_USE_CEIL ทุก MC_GROUP ใน pool เดียวกัน
_pool_use = (
    df[df["POOL_TYPE"] != ""]
    .groupby(["POOL_TYPE", "WEEK"])["MC_USE_CEIL"]
    .sum()
    .reset_index()
    .rename(columns={"MC_USE_CEIL": "_POOL_USE_TOTAL"})
)
df = df.merge(_pool_use, on=["POOL_TYPE", "WEEK"], how="left")

# DESC pool usage per WEEK: รวม MC_USE_CEIL จากทุก row ที่ match TYPE_SPECIAL
# (รวมทั้ง POLY primary + TYPE_SPECIAL primary เพื่อหักเครื่องทั้ง 2 pool พร้อมกัน)
_desc_pool_use = (
    df[df["DESC_POOL_TYPE"] != ""]
    .groupby(["DESC_POOL_TYPE", "WEEK"])["MC_USE_CEIL"]
    .sum()
    .reset_index()
    .rename(columns={"MC_USE_CEIL": "_DESC_POOL_USE_TOTAL", "DESC_POOL_TYPE": "_dpk"})
)
df = df.merge(
    _desc_pool_use.rename(columns={"_dpk": "DESC_POOL_TYPE"}),
    on=["DESC_POOL_TYPE", "WEEK"],
    how="left"
)

# Non-pool rows: total MC_USE_CEIL per MC_CAT+GUAGE+WEEK (เหมือน pool rows)
_nonpool_use = (
    df[df["POOL_TYPE"] == ""]
    .groupby(["MC_CAT", "GUAGE", "WEEK"])["MC_USE_CEIL"]
    .sum()
    .reset_index()
    .rename(columns={"MC_USE_CEIL": "_NONPOOL_USE_TOTAL"})
)
df = df.merge(_nonpool_use, on=["MC_CAT", "GUAGE", "WEEK"], how="left")

# TOTAL_MC_REMAIN:
# - TYPE_SPECIAL primary rows → ใช้ desc_pool_use (รวม POLY+TYPE_SPECIAL ทั้งหมด)
# - pool rows ที่ match TYPE_SPECIAL description (เช่น FD4/FD5 ใน POLY pool) → min(pool remain, TYPE_SPECIAL remain)
# - pool rows อื่น → pool total - _pool_use
# - non-pool rows → TOTAL_MC - MC_USE_CUM
_desc_pool_total_s = df["DESC_POOL_TYPE"].map(
    lambda pk: _TOTAL_MC_BY_TYPE.get(pk, 0) if pk else 0
)
_type_special_remain = _desc_pool_total_s - df["_DESC_POOL_USE_TOTAL"].fillna(0)

df["POOL_TYPE"] = df["POOL_TYPE"].fillna("").astype(str)
df["DESC_POOL_TYPE"] = df["DESC_POOL_TYPE"].fillna("").astype(str)

df["TOTAL_MC_REMAIN"] = np.where(
    df["POOL_TYPE"].str.endswith(":TYPE_SPECIAL", na=False),
    df["TOTAL_MC"] - df["_DESC_POOL_USE_TOTAL"].fillna(0),
    np.where(
        (df["POOL_TYPE"] != "") & (df["DESC_POOL_TYPE"] != ""),
        np.minimum(
            df["TOTAL_MC"] - df["_POOL_USE_TOTAL"].fillna(0),
            _type_special_remain
        ),
        np.where(
            df["POOL_TYPE"] != "",
            df["TOTAL_MC"] - df["_POOL_USE_TOTAL"].fillna(0),
            df["TOTAL_MC"] - df["_NONPOOL_USE_TOTAL"].fillna(0)
        )
    )
)
df = df.drop(columns=["_POOL_USE_TOTAL", "_NONPOOL_USE_TOTAL"])

# DESC_MC_REMAIN: เครื่องคงเหลือใน TYPE_SPECIAL pool สำหรับ row ที่ match description
# (แสดงเฉพาะ row ที่มี DESC_POOL_TYPE — ทั้ง POLY primary และ TYPE_SPECIAL primary)
df["DESC_POOL_TOTAL"] = df["DESC_POOL_TYPE"].map(
    lambda pk: _TOTAL_MC_BY_TYPE.get(pk, 0) if pk else 0
)
df["DESC_MC_REMAIN"] = np.where(
    df["DESC_POOL_TYPE"] != "",
    df["DESC_POOL_TOTAL"] - df["_DESC_POOL_USE_TOTAL"].fillna(0),
    np.nan
)
df = df.drop(columns=["_DESC_POOL_USE_TOTAL", "DESC_POOL_TOTAL"])

# =========================
# SUMMARY
# - Pool groups  → group by POOL_TYPE (Type ใน MasterMC), TOTAL_MC = pool total
# - Non-pool groups → group by MC_CAT + GUAGE เหมือนเดิม
# =========================
weeks = sorted(df["WEEK"].unique())
summary_parts = []

# --- Pool summary: แสดง MC_CAT+GUAGE ทุกตัวใน MasterMC (รวมที่ไม่มี booking) ---
pool_df = df[df["POOL_TYPE"] != ""]
# Pool usage from booking
if not pool_df.empty:
    pool_use_week = pool_df.groupby(["POOL_TYPE", "WEEK"], as_index=False)["MC_USE_CEIL"].sum()
else:
    pool_use_week = pd.DataFrame(columns=["POOL_TYPE", "WEEK", "MC_USE_CEIL"])
# Pool total per POOL_TYPE
pool_total_df = pd.DataFrame(list(_TOTAL_MC_BY_TYPE.items()), columns=["POOL_TYPE", "TOTAL_MC"])
# Build pool combos จาก MasterMC ทั้งหมด (ใช้ Type_1 raw = CAT ตรงกับ booking)
_pool_base_records = []
for _, _mmc_row in _master_mc_df.iterrows():
    _mc_u = str(_mmc_row.get("MC", "")).strip().upper()
    _g_norm = _norm_gauge_ava(_mmc_row.get("Guage", ""))
    _t1_raw = str(_mmc_row.get("MC_CAT", "")).strip()
    _g_raw = str(_mmc_row.get("Guage", "")).strip()
    _fac_raw = str(_mmc_row.get("Factory", "")).strip()
    if not _mc_u or not _t1_raw or not _g_norm:
        continue
    _pk = _MC_TO_POOL.get((_mc_u, _g_norm), "")
    if _pk:
        _pool_base_records.append({"FACTORY": _fac_raw, "MC_CAT": _t1_raw, "GUAGE": _g_raw, "POOL_TYPE": _pk})
pool_combos = (
    pd.DataFrame(_pool_base_records).drop_duplicates()
    if _pool_base_records
    else pd.DataFrame(columns=["FACTORY", "MC_CAT", "GUAGE", "POOL_TYPE"])
)
pool_combos = pool_combos.merge(pool_total_df, on="POOL_TYPE", how="left").dropna(subset=["TOTAL_MC"])
if not pool_combos.empty:
    week_df = pd.DataFrame({"WEEK": weeks})
    pool_combos["key"] = 1
    week_df["key"] = 1
    pool_base = pool_combos.merge(week_df, on="key").drop(columns="key")
    pool_sum = pool_base.merge(pool_use_week, on=["POOL_TYPE", "WEEK"], how="left")
    pool_sum["MC_USE_CEIL"] = pool_sum["MC_USE_CEIL"].fillna(0).astype(int)
    pool_sum["TOTAL_MC_REMAIN"] = pool_sum["TOTAL_MC"] - pool_sum["MC_USE_CEIL"]
    summary_parts.append(pool_sum[["FACTORY", "MC_CAT", "GUAGE", "TOTAL_MC", "WEEK", "MC_USE_CEIL", "TOTAL_MC_REMAIN"]])

# --- Non-pool summary: เฉพาะ MC_CAT+GUAGE ที่มี data จริงใน nonpool_df เท่านั้น ---
nonpool_df = df[df["POOL_TYPE"] == ""]
if not nonpool_df.empty:
    mc_use_week = nonpool_df.groupby(["MC_CAT", "GUAGE", "WEEK"], as_index=False)["MC_USE_CEIL"].sum()
    # ดึง TOTAL_MC เฉพาะ combo ที่มีใน nonpool_df จริง
    _np_combos = set(zip(nonpool_df["MC_CAT"].str.strip(), nonpool_df["GUAGE"].astype(str).str.strip()))
    mc_master_np = pd.DataFrame(
        [(k[0], k[1], v) for k, v in TOTAL_MC_MAP_TYPE1.items()
         if (k[0], k[1]) in _np_combos],
        columns=["MC_CAT", "GUAGE", "TOTAL_MC"],
    )
    if not mc_master_np.empty:
        week_df2 = pd.DataFrame({"WEEK": weeks})
        mc_master_np["key"] = 1
        week_df2["key"] = 1
        summary_base = mc_master_np.merge(week_df2, on="key").drop(columns="key")
        summary_np = summary_base.merge(mc_use_week, on=["MC_CAT", "GUAGE", "WEEK"], how="left")
        summary_np["MC_USE_CEIL"] = summary_np["MC_USE_CEIL"].fillna(0).astype(int)
        summary_np["TOTAL_MC_REMAIN"] = summary_np["TOTAL_MC"] - summary_np["MC_USE_CEIL"]
        summary_np["FACTORY"] = ""
        summary_parts.append(summary_np[["FACTORY", "MC_CAT", "GUAGE", "TOTAL_MC", "WEEK", "MC_USE_CEIL", "TOTAL_MC_REMAIN"]])

summary = (
    pd.concat(summary_parts, ignore_index=True)
    if summary_parts
    else pd.DataFrame(columns=["FACTORY", "MC_CAT", "GUAGE", "TOTAL_MC", "WEEK", "MC_USE_CEIL", "TOTAL_MC_REMAIN"])
)

# =========================
# AVAILABILITY SUMMARY: WEEK 25-35 (แยกรายสัปดาห์ เรียงไปทางขวา)
# =========================
from datetime import date as _date, timedelta as _td
# สัปดาห์ปัจจุบัน (ศุกร์–พฤหัส = ISO week ของ วันที่+3) → ย้อนหลัง 5 สัปดาห์ ไปข้างหน้าจนหมด
_AVA_CUR_WEEK = (_date.today() + _td(days=3)).isocalendar()[1]
_ava_all_weeks = set(summary["WEEK"].unique())

def _ava_week_delta(_w):
    # ระยะห่าง(สัปดาห์)จากสัปดาห์ปัจจุบัน รองรับข้ามปี (ISO week วน 1..52)
    _d = int(_w) - _AVA_CUR_WEEK
    if _d > 26:
        _d -= 52
    elif _d < -26:
        _d += 52
    return _d

# ย้อนหลัง 5 สัปดาห์ (รวมข้ามปี) → ไปข้างหน้าจนหมด, ตัด week 99
ava_weeks = sorted(
    (w for w in _ava_all_weeks if w != 99 and _ava_week_delta(w) >= -5),
    key=_ava_week_delta,
)
AVA_WK_START = ava_weeks[0]  if ava_weeks else _AVA_CUR_WEEK
AVA_WK_END   = ava_weeks[-1] if ava_weeks else _AVA_CUR_WEEK
AVA_SHEET = "AVA_MC"

# map หมวดที่ต้องรวมแสดง: SYN-30 / SYN-28 → DOUBLE-30
_AVA_CAT_MAP = {"SYN-30": "DOUBLE-30", "SYN-28": "DOUBLE-30"}
def _ava_cat_disp(_s):
    # MC_CAT ที่ใช้แสดง/จัดกลุ่ม (upper + รวม SYN เข้า DOUBLE-30)
    return _s.astype(str).str.strip().str.upper().replace(_AVA_CAT_MAP)
def _ava_catkey(_s):
    # key match ระหว่าง df (SINGLE) กับ summary/MasterMC (SINGEL)
    return _s.str.replace("SINGEL", "SINGLE", regex=False)

# AVA ใช้ Total mc ตรงจาก MasterMC โดยตรง
# เพื่อไม่ให้โดนบวกซ้ำจาก SUMMARY ที่มีทั้ง normal pool / special pool / non-pool พร้อมกัน
_ava_master = _master_mc_df[["MC_CAT", "Guage", "Total MC"]].copy()
_ava_master["MC_CAT"] = _ava_cat_disp(_ava_master["MC_CAT"])
_ava_master["GUAGE"] = _ava_master["Guage"].map(_norm_gauge_ava)
_ava_master["Total mc"] = pd.to_numeric(_ava_master["Total MC"], errors="coerce").fillna(0)
_ava_master_tot = (
    _ava_master.groupby(["MC_CAT", "GUAGE"], as_index=False)["Total mc"]
    .sum()
)
_ava_master_tot["_CATKEY"] = _ava_catkey(_ava_master_tot["MC_CAT"])
_ava_master_tot["_GKEY"] = _ava_master_tot["GUAGE"].astype(str).str.strip()

# MC_USE_Cat = ผลรวมจำนวนเครื่องที่ใช้จริง ต่อ MC_CAT(display) + GUAGE ต่อ WEEK (จาก DETAIL)
_udf = df[["MC_CAT", "GUAGE", "WEEK", "MC_USE_CEIL"]].copy()
_udf["MC_CAT"] = _ava_cat_disp(_udf["MC_CAT"])
_udf["GUAGE"] = _udf["GUAGE"].map(_norm_gauge_ava)
_use_all = (
    _udf.groupby(["MC_CAT", "GUAGE", "WEEK"], as_index=False)["MC_USE_CEIL"]
    .sum()
    .rename(columns={"MC_USE_CEIL": "MC_USE_Cat"})
)
_use_all["_CATKEY"] = _ava_catkey(_use_all["MC_CAT"])
_use_all["_GKEY"] = _use_all["GUAGE"].astype(str).str.strip()

# KP_Cat = ผลรวม KP_WEIGHT ต่อ MC_CAT(display) + GUAGE ต่อ WEEK (จาก DETAIL)
_kpdf = df[["MC_CAT", "GUAGE", "WEEK", "KP_WEIGHT"]].copy()
_kpdf["MC_CAT"] = _ava_cat_disp(_kpdf["MC_CAT"])
_kpdf["GUAGE"] = _kpdf["GUAGE"].map(_norm_gauge_ava)
_kp_all = (
    _kpdf.groupby(["MC_CAT", "GUAGE", "WEEK"], as_index=False)["KP_WEIGHT"].sum()
    .rename(columns={"KP_WEIGHT": "KP_Cat"})
)
_kp_all["_CATKEY"] = _ava_catkey(_kp_all["MC_CAT"])
_kp_all["_GKEY"] = _kp_all["GUAGE"].astype(str).str.strip()

# row index ร่วม = union ของ MasterMC และข้อมูลจริงใน booking
_ava_idx_df = pd.concat(
    [
        _ava_master_tot[["MC_CAT", "GUAGE"]],
        _use_all[["MC_CAT", "GUAGE"]],
        _kp_all[["MC_CAT", "GUAGE"]],
    ],
    ignore_index=True,
).drop_duplicates().sort_values(["MC_CAT", "GUAGE"])
_ava_idx = pd.MultiIndex.from_frame(_ava_idx_df)

_ava_base = _ava_idx_df.copy()
_ava_base["_CATKEY"] = _ava_catkey(_ava_base["MC_CAT"])
_ava_base["_GKEY"] = _ava_base["GUAGE"].astype(str).str.strip()
_ava_base = _ava_base.merge(
    _ava_master_tot[["_CATKEY", "_GKEY", "Total mc"]],
    on=["_CATKEY", "_GKEY"],
    how="left"
)
_ava_base["Total mc"] = _ava_base["Total mc"].fillna(0)

_ava_blocks = []
for _wk in ava_weeks:
    _usew = _use_all[_use_all["WEEK"] == _wk][["_CATKEY", "_GKEY", "MC_USE_Cat"]]
    _kpw = _kp_all[_kp_all["WEEK"] == _wk][["_CATKEY", "_GKEY", "KP_Cat"]]
    _m = _ava_base.merge(_usew, on=["_CATKEY", "_GKEY"], how="left")
    _m = _m.merge(_kpw, on=["_CATKEY", "_GKEY"], how="left")
    _m["MC_USE_Cat"] = _m["MC_USE_Cat"].fillna(0)
    _m["KP_Cat"] = _m["KP_Cat"].fillna(0)
    # Dif/%ava คิดราย gauge: Total mc − MC_USE_Cat
    _m["Dif"] = _m["Total mc"] - _m["MC_USE_Cat"]
    _m["%ava"] = (_m["Dif"] / _m["Total mc"]).where(_m["Total mc"] != 0)
    _blk = (
        _m.set_index(["MC_CAT", "GUAGE"])[["Total mc", "KP_Cat", "MC_USE_Cat", "Dif", "%ava"]]
        .reindex(_ava_idx)
    )
    _blk.columns = pd.MultiIndex.from_product([[f"WEEK {_wk}"], _blk.columns])
    _ava_blocks.append(_blk)

ava_data = (
    pd.concat(_ava_blocks, axis=1)
    if _ava_blocks
    else pd.DataFrame(index=_ava_idx)
)

# ---------- แทรกแถว subtotal แยกกลุ่ม DOUBLE / SINGEL ----------
def _ava_group(_cat):
    _c = str(_cat).strip().upper().replace("SINGEL", "SINGLE")
    if _c.startswith("DOUBLE"):
        return "DOUBLE"
    if _c.startswith("SINGLE"):
        return "SINGLE"
    return "OTHER"

def _ava_subtotal(_members):
    _sub = ava_data.loc[_members]
    _vals = []
    for _col in ava_data.columns:
        _w, _metric = _col
        if _metric == "%ava":
            _st = _sub[(_w, "Total mc")].sum()
            _d = _sub[(_w, "Dif")].sum()
            _vals.append((_d / _st) if _st else None)
        else:
            _vals.append(_sub[_col].sum())
    return pd.Series(_vals, index=ava_data.columns)

_ord_idx, _ord_rows, _sub_pos = [], [], []
_cur_g, _cur_mem = None, []
for _idx in ava_data.index:
    _g2 = _ava_group(_idx[0])
    if _cur_g is None:
        _cur_g = _g2
    if _g2 != _cur_g:
        _ord_idx.append((f"{_cur_g} Total", ""))
        _ord_rows.append(_ava_subtotal(_cur_mem))
        _sub_pos.append(len(_ord_idx) - 1)
        _cur_g, _cur_mem = _g2, []
    _ord_idx.append(_idx)
    _ord_rows.append(ava_data.loc[_idx])
    _cur_mem.append(_idx)
if _cur_mem:
    _ord_idx.append((f"{_cur_g} Total", ""))
    _ord_rows.append(_ava_subtotal(_cur_mem))
    _sub_pos.append(len(_ord_idx) - 1)

if _ord_rows:
    ava_wide = pd.DataFrame(_ord_rows)
    ava_wide.index = pd.MultiIndex.from_tuples(_ord_idx, names=["MC_CAT", "GUAGE"])
    ava_wide.columns = ava_data.columns
else:
    ava_wide = ava_data
# แถว subtotal (1-based excel row): ข้อมูลเริ่มแถว 4
_ava_sub_excel_rows = [4 + _p for _p in _sub_pos]

# =========================
# SAVE
# =========================
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="DETAIL", index=False)
    summary.to_excel(writer, sheet_name="SUMMARY_MC_REMAIN", index=False)
    ava_wide.to_excel(writer, sheet_name=AVA_SHEET)
    # freeze คอลัมน์ A-B (MC_CAT, GUAGE) + แถวหัวตาราง ให้ค้างเวลาเลื่อนดู week ทางขวา
    _ws = writer.sheets[AVA_SHEET]
    _ws.freeze_panes = "C4"

    # ---------- จัดรูปแบบชีท AVA ----------
    from openpyxl.styles import Font, Border, Side, PatternFill
    _n_rows = len(ava_wide)
    _n_weeks = len(ava_weeks)
    _first_row = 4                       # แถวข้อมูลแรก (1-2 หัว, 3 ชื่อ index)
    _last_row = 3 + _n_rows
    _NUM_FMT = "#,##0"                   # ไม่มีทศนิยม + ลูกน้ำ
    _PCT_FMT = "0%"                      # % ไม่มีทศนิยม
    _bold = Font(bold=True)
    _rb = Side(style="medium")
    _sub_fill = PatternFill("solid", fgColor="FFD9E1F2")
    _sub_set = set(_ava_sub_excel_rows)

    # 4) number format + 3) %ava เป็น % และ <=20% สีแดง + bold แถว subtotal
    for _r in range(_first_row, _last_row + 1):
        _is_sub = _r in _sub_set
        for _wi in range(_n_weeks):
            _base = 3 + _wi * 5
            for _c in range(_base, _base + 4):       # Total mc, KP_Cat, MC_USE_Cat, Dif
                _cell = _ws.cell(row=_r, column=_c)
                _cell.number_format = _NUM_FMT
                if _is_sub:
                    _cell.font = _bold
            _pa = _ws.cell(row=_r, column=_base + 4)  # %ava
            _pa.number_format = _PCT_FMT
            _low = isinstance(_pa.value, (int, float)) and _pa.value <= 0.20
            if _low or _is_sub:
                _pa.font = Font(bold=_is_sub, color=("FFFF0000" if _low else "FF000000"))

    # แถว subtotal: ตัวหนา + พื้นสี
    for _r in _sub_set:
        for _c in range(1, 3 + _n_weeks * 5):
            _cell = _ws.cell(row=_r, column=_c)
            _cell.fill = _sub_fill
            if _c <= 2:
                _cell.font = _bold

    # 2) เส้นขอบขวาปิดท้ายทุก week (คอลัมน์ %ava) ตลอดทั้งตาราง
    for _wi in range(_n_weeks):
        _bcol = 3 + _wi * 5 + 4
        for _r in range(1, _last_row + 1):
            _cell = _ws.cell(row=_r, column=_bcol)
            _e = _cell.border
            _cell.border = Border(left=_e.left, top=_e.top, bottom=_e.bottom, right=_rb)

print("✅ AVA MC FINAL COMPLETE (COLLAR REMOVED)")
print("Saved:", OUTPUT_FILE)
print("DETAIL rows:", len(df))
print("SUMMARY rows:", len(summary))
print(f"{AVA_SHEET} rows:", len(ava_wide), "weeks:", ava_weeks)

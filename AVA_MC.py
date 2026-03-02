import pandas as pd
import numpy as np
from pathlib import Path
from Yarn_Master import load_yarn_master

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).parent
BOOKING_DIR = BASE_DIR / "Booking"
MASTER_MC_FILE = BASE_DIR / "data" / "MC" / "Master_MC_5.xlsx"
OUTPUT_DIR = BASE_DIR / "data_plan"
OUTPUT_FILE = OUTPUT_DIR / "booking_final_ready25.xlsx"

EXCLUDE_MC_GROUP = [
    "CL-NP", "CL-OM", "COMKN", "F-CL", "CL", "FQCCL-NP",
    "FQCCL-OM", "FQC-Omnoi", "FQC-Phet", "FQC","F-TSD"
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
    "STRUCTURE"
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
# 20 / 24 RULE
# =========================
MULTIPLY_RULES = {
    ("SKP","14"),("SKP","18"),("SKP","20"),("SKP","22"),("SKP","24"),("SKP","26"),("SKP","28"),
    ("SKPTA","14"),("SKPTA","22"),("SKPTA","26"),("SKPTA","28"),("SKPTA","36"),
    ("SKPLE","26"),("SKPLE","36"),
    ("SBP","21"),("SBP","22"),("SBP","26"),("SBP","28"),
    ("TSA","26"),("TSB","26"),("TSC","26"),("TSD","26"),
    ("TSE","22"),("TSE","26"),
    ("TSF","22"),("TSF","24"),("TSF","26"),
    ("TSFLE","22"),("TSFLE","26"),("TSFLE","30"),
}

# =========================
# WORKING DAY = 6
# =========================
WORKING_DAY_6 = set(MULTIPLY_RULES) | {
    ("RAOO", "16"), ("IRMT", "28"), ("IRMT", "24"),
    ("FA", "18"), ("FA", "20"), ("SJT", "28"),
}

# =========================
# TOTAL MC MASTER
# =========================
TOTAL_MC_MAP = {
    ("RAO","16"):3, ("RAO","18"):1, ("RAO","19"):6,
    ("RAP","19"):8,
    ("IIP","20"):5, ("RL","18"):4,
    ("IBLTA","22"):16, ("IBP","22"):18, ("RAO","22"):5,
    ("RAP","22"):3, ("RAP60","22"):3, ("RAP98","22"):6, ("SYN","22"):6,
    ("IIP","24"):2, ("II","24"):1,
    ("IBLTA","28"):5, ("RAP","28"):1, ("RAP60","28"):4,
    ("RAP98","28"):25, ("SYN","28"):10,
    ("IRM","28"):11, ("IRMPL","28"):4, ("IRMLE","40"):4,
    ("SKP","14"):1, ("SKP","18"):1, ("SKP","20"):20, ("SKP","22"):13,
    ("SKP","24"):1, ("SKP","26"):7, ("SKP","28"):14,
    ("SKPTA","14"):2, ("SKPTA","22"):3, ("SKPTA","26"):21,
    ("SKPTA","28"):5, ("SKPTA","36"):12,
    ("SKPLE","26"):13, ("SKPLE","36"):9,
    ("SBP","21"):1, ("SBP","22"):1, ("SBP","26"):14, ("SBP","28"):7,
    ("TSA","26"):1, ("TSB","26"):1, ("TSC","26"):3, ("TSD","26"):3,
    ("TSE","22"):1, ("TSE","26"):8,
    ("TSF","22"):8, ("TSF","24"):0, ("TSF","26"):0,
    ("TSFLE","22"):1, ("TSFLE","26"):5, ("TSFLE","30"):2,
    ("RAOO","16"):8, ("IRMT","28"):7, ("IRMT","24"):1,
    ("FA","18"):1, ("FA","20"):47,
    ("SJT","28"):4,
}

# =========================
# LOAD
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
    is_zip = raw_bytes[:2] == b'PK'
    is_biff = raw_bytes[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'

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
# APPLY 20/24
# =========================
mask_2024 = df.apply(
    lambda r: (r["MC_GROUP"], r["GUAGE"]) in MULTIPLY_RULES,
    axis=1
)
df.loc[mask_2024, "CAP ทอ"] *= (20 / 24)

# =========================
# GROUP ITEM
# =========================
agg_dict = {
    "KP_WEIGHT": "sum",
    "CAP ทอ": "first",
    "REVOLUTION/WEIGHT": "first",
}
if "SO_NO" in df.columns:
    agg_dict["SO_NO"] = lambda x: ",".join(x.dropna().astype(str).unique())
for col in ["YARN-USED", "STRUCTURE"]:
    if col in df.columns:
        agg_dict[col] = "first"

df = (
    df.groupby(["MC_GROUP", "GUAGE", "ITEM_CODE", "WEEK"], as_index=False)
      .agg(agg_dict)
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
df["WORKING_DAY"] = df.apply(
    lambda r: 6 if (r["MC_GROUP"], r["GUAGE"]) in WORKING_DAY_6 else 7,
    axis=1
)

# =========================
# MC USE
# =========================
df["MC_USE"] = np.where(
    df["CAP ทอ"] > 0,
    df["KP_WEIGHT"] / (df["CAP ทอ"] * df["WORKING_DAY"]),
    0
)

df["MC_USE_CEIL"] = np.ceil(df["MC_USE"]).astype(int)

# =========================
# TOTAL MC
# =========================
df["TOTAL_MC"] = df.apply(
    lambda r: TOTAL_MC_MAP.get((r["MC_GROUP"], r["GUAGE"]), 0),
    axis=1
)

# =========================
# MC CUMULATIVE
# =========================
df = df.sort_values(["MC_GROUP", "GUAGE", "WEEK"])
df["MC_USE_CUM"] = df.groupby(
    ["MC_GROUP", "GUAGE", "WEEK"]
)["MC_USE_CEIL"].cumsum()

df["TOTAL_MC_REMAIN"] = df["TOTAL_MC"] - df["MC_USE_CUM"]

# =========================
# SUMMARY
# =========================
mc_master = pd.DataFrame(
    [(k[0], k[1], v) for k, v in TOTAL_MC_MAP.items()],
    columns=["MC_GROUP", "GUAGE", "TOTAL_MC"]
)

week_master = pd.DataFrame({"WEEK": sorted(df["WEEK"].unique())})
mc_master["key"] = 1
week_master["key"] = 1

summary_base = mc_master.merge(week_master, on="key").drop(columns="key")

mc_use_week = (
    df.groupby(["MC_GROUP", "GUAGE", "WEEK"], as_index=False)
      .agg({"MC_USE_CEIL": "sum"})
)

summary = summary_base.merge(
    mc_use_week,
    on=["MC_GROUP", "GUAGE", "WEEK"],
    how="left"
)

summary["MC_USE_CEIL"] = summary["MC_USE_CEIL"].fillna(0).astype(int)
summary["TOTAL_MC_REMAIN"] = summary["TOTAL_MC"] - summary["MC_USE_CEIL"]

capability_groups = load_capability_groups(MASTER_MC_FILE)
summary = summary.merge(
    capability_groups,
    on=["MC_GROUP", "GUAGE"],
    how="left"
)

summary["CAPABILITY_TOTAL_MC_REMAIN"] = summary.groupby(
    ["Capability Group", "WEEK"]
)["TOTAL_MC_REMAIN"].transform("sum")

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

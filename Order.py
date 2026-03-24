import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
ORDER_DIR = Path(r"C:\vscode\AI_plan\Order")
OUTPUT_DIR = Path(r"C:\vscode\AI_plan\data_plan")
OUTPUT_FILE = OUTPUT_DIR / "order_ready.xlsx"

EXCLUDE_ORDER_TYPES = [
    "CL-ORDERS",
    "FQC"
]

EXCLUDE_MC_GROUPS = [
    "F-CL",
    "COMKN"
]

# =========================
# FUNCTIONS
# =========================
def load_all_orders(order_dir: Path) -> pd.DataFrame:
    """
    อ่านไฟล์ Excel และ CSV ทุกไฟล์ในโฟลเดอร์ Order (รองรับ .xlsx, .xls และ .csv)
    """
    all_files = [
        f for f in order_dir.iterdir()
        if f.suffix.lower() in (".xlsx", ".xls", ".csv")
    ]

    if not all_files:
        raise FileNotFoundError("❌ ไม่พบไฟล์ Excel หรือ CSV ในโฟลเดอร์ Order")

    df_list = []
    for file in all_files:
        print(f"📄 Loading: {file.name}")
        try:
            if file.suffix.lower() == ".csv":
                # Handle CSV files with different encodings
                for enc in ("utf-8-sig", "cp874", "tis-620", "latin-1"):
                    try:
                        df = pd.read_csv(file, encoding=enc)
                        print(f"   ✅ อ่าน CSV สำเร็จด้วย encoding: {enc}")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError(f"❌ ไม่สามารถอ่านไฟล์ CSV ได้: {file.name}")
            else:
                # Handle Excel files
                engine = "xlrd" if file.suffix.lower() == ".xls" else "openpyxl"
                df = pd.read_excel(file, engine=engine)
        except Exception:
            # ไฟล์อาจเป็น TSV ที่บันทึกด้วยนามสกุล .xls
            print(f"⚠️  ไม่ใช่ Excel จริง ลองอ่านเป็น TSV: {file.name}")
            for enc in ("cp874", "tis-620", "utf-8-sig", "latin-1"):
                try:
                    df = pd.read_csv(file, sep="\t", encoding=enc)
                    print(f"   ✅ อ่านสำเร็จด้วย encoding: {enc}")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError(f"❌ ไม่สามารถอ่านไฟล์ได้: {file.name}")
        df["SOURCE_FILE"] = file.name
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True)


def filter_order_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter Orders Type
    """
    # Check for both possible column names
    order_type_col = None
    for col in ["Orders Type", "ORDER_TYPE"]:
        if col in df.columns:
            order_type_col = col
            break
    
    if order_type_col is None:
        print("⚠️ ไม่พบ column 'Orders Type' หรือ 'ORDER_TYPE' - ข้ามการ filter")
        return df

    before = len(df)
    df = df[~df[order_type_col].isin(EXCLUDE_ORDER_TYPES)]
    after = len(df)

    print(f"🧹 Orders Type: {before} → {after}")
    return df


def filter_mc_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter MC GROUP
    """
    # Check for both possible column names
    mc_group_col = None
    for col in ["MC GROUP", "MC_GROUP"]:
        if col in df.columns:
            mc_group_col = col
            break
    
    if mc_group_col is None:
        print("⚠️ ไม่พบ column 'MC GROUP' หรือ 'MC_GROUP' - ข้ามการ filter")
        return df

    before = len(df)
    df = df[~df[mc_group_col].isin(EXCLUDE_MC_GROUPS)]
    after = len(df)

    print(f"🧹 MC GROUP: {before} → {after}")
    return df


def prepare_order_data(export_excel: bool = True) -> pd.DataFrame:
    """
    เตรียม Order Data และ export เป็น Excel
    """
    df = load_all_orders(ORDER_DIR)
    df = filter_order_type(df)
    df = filter_mc_group(df)

    df.reset_index(drop=True, inplace=True)

    if export_excel:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"✅ Export Excel สำเร็จ: {OUTPUT_FILE}")

    return df


# =========================
# TEST RUN
# =========================
if __name__ == "__main__":
    order_df = prepare_order_data()
    print("📊 Sample Output")
    print(order_df.head())
    print(f"Total rows: {len(order_df)}")
    
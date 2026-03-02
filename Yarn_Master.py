import pandas as pd
from pathlib import Path


DATA_DIR = Path(r"C:\vscode\AI_plan\data\Yarn")


def load_yarn_master() -> pd.DataFrame:
    """
    อ่านไฟล์ Excel ทุกไฟล์ใน Yarn_Master folder
    คืนค่า DataFrame รวมของ Yarn master data
    Columns: ITEM_CODE, ITEM_DESC
    """
    dfs = []

    print(f"Scanning folder: {DATA_DIR}")

    for file in DATA_DIR.glob("*.xlsx"):
        if file.name.startswith("~$"):
            print(f"[SKIP] temp file: {file.name}")
            continue

        try:
            df = pd.read_excel(file, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()

            # Rename to standard column names
            df = df.rename(columns={
                "Item Code": "ITEM_CODE",
                "Item Desc": "ITEM_DESC",
            })

            required_cols = {"ITEM_CODE", "ITEM_DESC"}
            if not required_cols.issubset(df.columns):
                print(f"[SKIP] {file.name} (missing columns: {required_cols - set(df.columns)})")
                continue

            # Drop rows with no ITEM_CODE
            df = df.dropna(subset=["ITEM_CODE"])
            df["ITEM_CODE"] = df["ITEM_CODE"].astype(str).str.strip()
            df["ITEM_DESC"] = df["ITEM_DESC"].astype(str).str.strip()

            # เพิ่ม column FIBER_TYPE: ถ้า ITEM_DESC มีคำว่า poly (ไม่สนใจตัวพิมพ์) ให้เป็น "POLY" ไม่งั้นเป็น "None POLY"
            df["FIBER_TYPE"] = df["ITEM_DESC"].apply(
                lambda x: "POLY" if "poly" in x.lower() else "None POLY"
            )

            dfs.append(df[["ITEM_CODE", "ITEM_DESC", "FIBER_TYPE"]])
            print(f"[OK] Loaded: {file.name} ({len(df)} rows)")

        except Exception as e:
            print(f"[ERROR] {file.name}: {e}")

    if not dfs:
        raise ValueError("[ERROR] No valid Excel files found in Yarn_Master folder")

    result = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["ITEM_CODE"])
    print(f"[DONE] Total yarn items loaded: {len(result)}")

    return result


# ============================================================
# Public API — เรียกใช้จากไฟล์อื่นได้เลย
# ============================================================
# from Yarn_Master import load_yarn_master
# yarn_df = load_yarn_master()

if __name__ == "__main__":
    yarn_df = load_yarn_master()

    print("\n=== YARN MASTER ===")
    print(yarn_df.head(20).to_string(index=False))
    print(f"\nShape: {yarn_df.shape}")
    print(f"\n--- FIBER_TYPE counts ---")
    print(yarn_df["FIBER_TYPE"].value_counts(dropna=False))
    print(f"\n--- ตัวอย่างที่มี FIBER_TYPE = POLY ---")
    print(yarn_df[yarn_df["FIBER_TYPE"] == "POLY"].head(10).to_string(index=False))

    # Export to Excel
    output_path = Path(r"C:\vscode\AI_plan\Yarn_Master\Yarn_Master_output.xlsx")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    yarn_df.to_excel(output_path, index=False)
    print(f"\n[SAVED] {output_path}")

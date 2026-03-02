import pandas as pd

from pathlib import Path


DATA_DIR = Path(r"C:\vscode\AI_plan\data\Cap")


def load_item_cap_data():
    dfs = []

    print(f"Scanning folder: {DATA_DIR}")

    for file in DATA_DIR.glob("*.xlsx"):
        if file.name.startswith("~$"):
            print(f"Skip temp file: {file.name}")
            continue

        try:
            df = pd.read_excel(file)
            df.columns = df.columns.str.strip().str.upper()

            required_cols = {
                "ITEM_CODE", "MC_GROUP", "CAP ทอ", "REVOLUTION/WEIGHT", "GUAGE"
            }
            if not required_cols.issubset(df.columns):
                print(f"[WARN] Skip {file.name} (missing columns)")
                continue

            dfs.append(df)
            print(f"[OK] Loaded: {file.name} ({len(df)} rows)")

        except Exception as e:
            print(f"[ERROR] {file.name}: {e}")

    if not dfs:
        raise ValueError("No valid Excel files found")

    result = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Total rows loaded: {len(result)}")

    return result[
        ["ITEM_CODE", "MC_GROUP", "CAP ทอ", "REVOLUTION/WEIGHT", "GUAGE"]
    ]


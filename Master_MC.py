import pandas as pd
from pathlib import Path

# =========================
# LOAD MASTER MC
# =========================
def load_all_master_mc(file_path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(file_path)
    dfs = []

    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet)
            df.columns = df.columns.str.strip()

            if "Capability Group" in df.columns and "Capability_Group" not in df.columns:
                df["Capability_Group"] = df["Capability Group"]

            for c in ["MC", "Guage", "Capability_Group"]:
                df[c] = df[c].astype(str).str.strip()

            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Skip sheet {sheet}: {e}")

    return pd.concat(dfs, ignore_index=True)

# =========================
# CORE FUNCTION (FINAL)
# =========================
def get_mc_names_by_mc_and_guage(
    mc: str,
    guage: str,
    df: pd.DataFrame
) -> dict:
    """
    Input: MC + Guage
    Output: ชื่อ MC ที่อยู่ใน Capability Group เดียวกัน
    """

    mc = mc.strip()
    guage = guage.strip()

    rows = df[
        (df["MC"] == mc) &
        (df["Guage"] == guage)
    ]

    if rows.empty:
        raise ValueError(
            f"❌ Not found MC='{mc}' with Guage='{guage}'"
        )

    cap_groups = rows["Capability_Group"].unique().tolist()

    result = {}
    for cap in cap_groups:
        mc_names = df[
            df["Capability_Group"] == cap
        ]["MC"].unique().tolist()

        result[cap] = mc_names

    return {
        "Input_MC": mc,
        "Guage": guage,
        "Capability_Groups": cap_groups,
        "MC_Names": result
    }

# =========================
# RUN / TEST
# =========================
def list_mc_names(df: pd.DataFrame) -> None:
    print("\nAvailable MC groups:")
    for mc in sorted(df["MC"].dropna().unique().tolist()):
        print("  ", mc)


if __name__ == "__main__":

    BASE_DIR = Path(r"C:\vscode")
    EXCEL_FILE = BASE_DIR / "Master_MC.xlsx"

    mc_df = load_all_master_mc(EXCEL_FILE)

    list_mc_names(mc_df)

    while True:
        mc = input("\nEnter MC (empty to quit): ").strip()
        if not mc:
            print("bye")
            break
        guage = input("Enter Guage: ").strip()
        if not guage:
            print("Guage required, try again")
            continue

        try:
            result = get_mc_names_by_mc_and_guage(mc=mc, guage=guage, df=mc_df)
        except ValueError as exc:
            print(exc)
            continue

        print("\n===== RESULT =====")
        print("Input MC:", result["Input_MC"])
        print("Guage:", result["Guage"])
        for cap, names in result["MC_Names"].items():
            print(f"Capability Group {cap}: {names}")

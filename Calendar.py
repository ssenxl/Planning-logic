import pandas as pd
from pathlib import Path


def load_calendar(file_path: Path, sheet_name: str = "Calendar") -> pd.DataFrame:
    """
    Calendar master (Daily level)

    status = 1 : holiday (ห้ามวางแผน)
    status = 0 : working day
    """

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # clean column names
    df.columns = df.columns.str.strip()

    # standard datetime
    df["DATE"] = pd.to_datetime(df["DATE"])

    # map working day flag
    # 1 = working day, 0 = holiday
    df["is_working_day"] = df["status"].map({0: 1, 1: 0})

    # sort for safety
    df = df.sort_values("DATE").reset_index(drop=True)

    # derive helpful calendar fields used by planner
    # Week definition: Friday–Thursday (เริ่มวันศุกร์ จบวันพฤหัสบดี)
    # เทคนิค: บวก 3 วัน → ศุกร์กลายเป็นจันทร์ → ใช้ ISO week ปกติได้เลย
    # ตัวอย่าง: ศ. 2 ม.ค. +3 = จ. 5 ม.ค. → ISO W2  /  พฤ. 8 ม.ค. +3 = อา. 11 ม.ค. → ISO W2
    _shifted = df["DATE"] + pd.Timedelta(days=3)
    iso = _shifted.dt.isocalendar()
    df["YEAR"] = iso["year"].astype(int)
    df["WEEK"] = iso["week"].astype(int)
    df["MONTH"] = df["DATE"].dt.month
    # Year-Week key (e.g., 2026-02) for grouping
    df["YW"] = df["YEAR"].astype(str) + "-" + df["WEEK"].astype(str).str.zfill(2)
    # Short day name used in plan (Mon, Tue, ...)
    df["DAY"] = df["DATE"].dt.day_name().str[:3]

    return df


def calendar_week_map(calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Weekly calendar summary
    ใช้สำหรับ logic ระดับสัปดาห์
    """

    weekly = (
        calendar_df
        .groupby("YW", as_index=False)
        .agg(
            year=("YEAR", "first"),
            month=("MONTH", "first"),
            week=("WEEK", "first"),
            week_start=("DATE", "min"),
            week_end=("DATE", "max"),
            working_days=("is_working_day", "sum"),
            total_days=("DATE", "count")
        )
        .sort_values(["year", "week"])
        .reset_index(drop=True)
    )

    return weekly


# =========================
# Run only for validation
# =========================
BASE_DIR = Path(r"C:\vscode\AI_plan")
CALENDAR_FILE = BASE_DIR / "Calendar.xlsx"
cal_df = load_calendar(CALENDAR_FILE, sheet_name="Sheet1")
cal_week_df = calendar_week_map(cal_df)

if __name__ == "__main__":
    print("=== DAILY CALENDAR ===")
    print(cal_df.head(10))

    print("\n=== WEEKLY CALENDAR ===")
    print(cal_week_df.head())

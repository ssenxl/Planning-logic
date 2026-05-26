"""
ดึงข้อมูลจาก View `BI_DATA_KP_BOOKING` บน Oracle Database (172.16.7.55:1521)
แล้วบันทึกเป็นไฟล์ Excel

Requirements:
    pip install oracledb pandas openpyxl

Credentials:
    $env:SF5_USER="your_user"
    $env:SF5_PASSWORD="your_pass"

Usage:
    python View_Booking.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import oracledb
import pandas as pd


def get_last_n_weeks(n: int = 5) -> list[tuple[int, int]]:
    today = pd.Timestamp.today()
    pairs = []
    ref = today
    for _ in range(n):
        shifted = ref + pd.Timedelta(days=3)  # same rule as Calendar.py (Fri-Thu week)
        iso = shifted.isocalendar()
        pairs.append((int(iso.year), int(iso.week)))
        ref -= pd.Timedelta(days=7)
    return pairs


# ---------- CONFIG ----------
HOST = "172.16.7.55"
PORT = 1521
DB_NAME = "NYTG"
VIEW_NAME = "nyf.DFIV_KP_BOOKING@NYKPB.WORLD"
OUTPUT_FILE = str((Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent) / "Booking" / "view_booking.xlsx")

USER = os.environ.get("SF5_USER", "hctr")
PASSWORD = os.environ.get("SF5_PASSWORD", "HCTR#23")

SELECT_COLUMNS = [
    "CAT", "MC_GROUP", "GUAGE","MC_NEEDLE", "YEAR", "WEEK",
    "ITEM_CODE", "DESCRIPTION", "KP_WEIGHT", "COLOR", "TYPE",
    "SO_NO", "YARN_USED", "ITEM_STRUCTURE",
    "TEAM_NAME", "ORDER_WEIGHT", "SCHEDULE_WEIGHT", "KNIT_WEIGHT",
    "OUTSTANDING", "FACTORY", "CAP_KNIT", "RDD_WEEK",
    "NAY_COLOR", "MATERIAL_CONTENT", 
]


def _connect() -> "oracledb.Connection":
    if not USER or not PASSWORD:
        raise RuntimeError(
            "ต้องกำหนด Oracle credentials ก่อน:\n"
            '  $env:SF5_USER="your_user"\n'
            '  $env:SF5_PASSWORD="your_pass"'
        )

    try:
        print(f"[INFO] Trying host={HOST} port={PORT} service_name={DB_NAME} ...")
        return oracledb.connect(
            user=USER, password=PASSWORD,
            host=HOST, port=PORT, service_name=DB_NAME
        )
    except oracledb.DatabaseError as e:
        print(f"[WARN] service_name failed: {e}")

    dsn_full = (
        f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={HOST})(PORT={PORT}))"
        f"(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME={DB_NAME})))"
    )
    try:
        print(f"[INFO] Trying full DSN string ...")
        return oracledb.connect(user=USER, password=PASSWORD, dsn=dsn_full)
    except oracledb.DatabaseError as e:
        print(f"[WARN] full DSN failed: {e}")

    dsn_sid = f"{HOST}:{PORT}/{DB_NAME}"
    print(f"[INFO] Trying Easy Connect SID={DB_NAME} ...")
    return oracledb.connect(user=USER, password=PASSWORD, dsn=dsn_sid)


def print_columns() -> None:
    print(f"[INFO] Connecting to {HOST}:{PORT}/{DB_NAME} ...")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {VIEW_NAME} WHERE ROWNUM <= 1")
        columns = [col[0] for col in cur.description]
        cur.close()
    finally:
        conn.close()
    print(f"[INFO] Columns in {VIEW_NAME}:")
    for i, col in enumerate(columns, 1):
        print(f"  {i:3d}. {col}")


def fetch_view() -> pd.DataFrame:
    print(f"[INFO] Connecting to {HOST}:{PORT}/{DB_NAME} via oracledb (thin mode)")
    conn = _connect()
    try:
        week_pairs = get_last_n_weeks(3)
        oldest_y, oldest_w = week_pairs[-1]
        week_filter = f"(YEAR * 100 + WEEK) >= {oldest_y * 100 + oldest_w}"
        col_list = ", ".join(SELECT_COLUMNS)
        sql = f"SELECT {col_list} FROM {VIEW_NAME} WHERE {week_filter}"
        print(f"[INFO] Fetching from week {oldest_y}-W{oldest_w:02d} onwards ...")
        print(f"[INFO] Fetching {len(SELECT_COLUMNS)} columns ...")
        cur = conn.cursor()
        cur.arraysize = 10000
        cur.execute(sql)
        rows = []
        while True:
            batch = cur.fetchmany(10000)
            if not batch:
                break
            rows.extend(batch)
            print(f"[INFO] Fetched so far: {len(rows):,} rows...", flush=True)
        cur.close()
        df = pd.DataFrame(rows, columns=SELECT_COLUMNS)
    finally:
        conn.close()
    print(f"[INFO] Fetched {len(df):,} rows x {len(df.columns)} cols")
    return df


def save_excel(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="BI_DATA_KP_BOOKING", index=False)
    print(f"[OK] Saved -> {path}")


def main() -> int:
    start = datetime.now()
    try:
        df = fetch_view()
        save_excel(df, OUTPUT_FILE)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1
    print(f"[DONE] elapsed {datetime.now() - start}")
    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--columns-only", action="store_true", help="แสดงแค่ชื่อ columns")
    args = parser.parse_args()
    if args.columns_only:
        print_columns()
    else:
        sys.exit(main())

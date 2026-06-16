"""
MapItem — เชื่อม (map) ข้อมูล item ระหว่าง 2 ไฟล์:

    Datamining/view_datamining.xlsx   (คอลัมน์ ITEM = ระดับ "ITEM Color")
            join  key: ITEM = "ITEM  Color"
    Master_Item.xlsx                  (ITEM  Color -> ORA_ITEM_CODE รหัสเต็ม A0/B0)

ผลลัพธ์: ตาราง view_datamining เดิม + คอลัมน์:
    ORA_ITEM_CODE   = รหัสเต็มทั้งหมดของ item คั่นด้วย ", " (เช่น F100413A0, F100413B0)
    ORA_ITEM_COUNT  = จำนวนรหัสเต็ม
1 datamining item = 1 แถว (ไม่แตกแถว)

Output:
    data_plan/datamining_mapped.xlsx

Usage:
    python MapItem.py
"""

import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------- CONFIG ----------
BASE = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent

DATAMINING_FILE = str(BASE / "Datamining" / "view_datamining.xlsx")
MASTER_ITEM_FILE = r"C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\Master_Item.xlsx"
BOOKING_FILE = str(BASE / "Booking" / "view_booking.xlsx")
OUTPUT_FILE = str(BASE / "data_plan" / "datamining_mapped.xlsx")
OUTPUT_BOOKING_FILE = str(BASE / "data_plan" / "datamining_booking_mapped.xlsx")

# ชื่อคอลัมน์ใน Master_Item (มี 2 ช่องว่างใน "ITEM  Color" ตามไฟล์ต้นทาง)
COL_COLOR = "ITEM  Color"          # key: = ITEM ใน datamining
COL_ORA = "ORA_ITEM_CODE"          # รหัสเต็มที่มี suffix A0/B0

# คอลัมน์น้ำหนักใน booking ที่จะรวมยอด (sum) ต่อ item
BOOKING_SUM_COLS = ["KP_WEIGHT", "ORDER_WEIGHT", "SCHEDULE_WEIGHT", "KNIT_WEIGHT", "OUTSTANDING"]


def _read_excel_safe(path: str, **kwargs) -> pd.DataFrame:
    """อ่าน Excel แบบทนต่อกรณีไฟล์ถูกล็อก (เปิดค้างใน Excel) โดย copy ไป temp ก่อน"""
    try:
        return pd.read_excel(path, **kwargs)
    except PermissionError:
        print(f"[WARN] '{path}' ถูกล็อก (เปิดใน Excel?) — copy ไป temp แล้วอ่านแทน")
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            shutil.copy2(path, tmp_path)
            return pd.read_excel(tmp_path, **kwargs)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def load_datamining() -> pd.DataFrame:
    print(f"[INFO] โหลด datamining: {DATAMINING_FILE}")
    dm = _read_excel_safe(DATAMINING_FILE)
    print(f"[INFO]   {len(dm):,} แถว, คอลัมน์: {list(dm.columns)}")
    dm["ITEM"] = dm["ITEM"].astype(str).str.strip()
    return dm


def build_ora_map() -> pd.DataFrame:
    """map ITEM (=ITEM Color) -> ORA_ITEM_CODE (รหัสเต็ม A0/B0 รวมในเซลล์เดียว) + จำนวน"""
    print(f"[INFO] โหลด Master_Item: {MASTER_ITEM_FILE}")
    mi = _read_excel_safe(MASTER_ITEM_FILE, usecols=[COL_COLOR, COL_ORA])
    mi[COL_COLOR] = mi[COL_COLOR].astype(str).str.strip()
    mi[COL_ORA] = mi[COL_ORA].astype(str).str.strip()

    pairs = mi[[COL_COLOR, COL_ORA]].drop_duplicates()
    ora_map = (
        pairs.groupby(COL_COLOR)[COL_ORA]
        .agg(lambda s: ", ".join(sorted(s.astype(str).unique())))
        .rename("ORA_ITEM_CODE")
        .reset_index()
        .rename(columns={COL_COLOR: "ITEM"})
    )
    ora_cnt = (
        pairs.groupby(COL_COLOR)[COL_ORA].nunique()
        .rename("ORA_ITEM_COUNT")
        .reset_index()
        .rename(columns={COL_COLOR: "ITEM"})
    )
    ora_map = ora_map.merge(ora_cnt, on="ITEM")
    print(f"[INFO]   ITEM Color ที่ map ได้ทั้งหมด: {len(ora_map):,}")
    return ora_map


def _report_ora_quality(merged: pd.DataFrame) -> None:
    unmatched = sorted(merged.loc[merged["ORA_ITEM_CODE"].isna(), "ITEM"].unique())
    if unmatched:
        print(f"[WARN] item ที่ map ไม่เจอใน Master_Item: {len(unmatched)} ตัว -> {unmatched[:20]}")
    else:
        print("[OK] map ครบทุก item")
    multi = merged.loc[merged["ORA_ITEM_COUNT"] > 1, "ITEM"].nunique()
    if multi:
        print(f"[INFO] item ที่มีรหัสเต็มหลายตัว (ORA_ITEM_COUNT>1): {multi} ตัว")


def build_mapping(dm: pd.DataFrame = None, ora_map: pd.DataFrame = None) -> pd.DataFrame:
    """datamining + ORA_ITEM_CODE (1 แถว/item-week เดิม)"""
    if dm is None:
        dm = load_datamining()
    if ora_map is None:
        ora_map = build_ora_map()
    merged = dm.merge(ora_map, on="ITEM", how="left")
    print(f"[INFO] merge ORA: {len(merged):,} แถว (เท่าเดิม — ไม่แตกแถว)")
    _report_ora_quality(merged)
    return merged


def build_booking_weekly() -> pd.DataFrame:
    """สรุป booking ต่อ (item, สัปดาห์)

    key: booking ITEM_CODE ตัด suffix "ตัวอักษร+เลข" ท้ายสุด (เช่น A0/B0) -> ITEM
    (รหัส collar/cuff เช่น CR.../CCV... ลงท้ายด้วยเลขล้วน จึงไม่ถูกตัด)
    คืน DataFrame: ITEM, FG_WEEK, BK_YEAR, BK_<col> (sum), BOOKING_ROWS
    """
    print(f"[INFO] โหลด booking: {BOOKING_FILE}")
    bk = _read_excel_safe(BOOKING_FILE)
    print(f"[INFO]   {len(bk):,} แถว")

    bk["ITEM"] = (
        bk["ITEM_CODE"].astype(str).str.strip().str.replace(r"[A-Z]\d$", "", regex=True)
    )
    bk["FG_WEEK"] = pd.to_numeric(bk["WEEK"], errors="coerce").astype("Int64")

    sum_cols = [c for c in BOOKING_SUM_COLS if c in bk.columns]
    for c in sum_cols:
        bk[c] = pd.to_numeric(bk[c], errors="coerce").fillna(0)

    agg_map = {c: "sum" for c in sum_cols}
    agg_map["YEAR"] = "first"  # (item,week) ไม่มีข้ามปี จึงใช้ค่าแรกได้
    weekly = bk.groupby(["ITEM", "FG_WEEK"], as_index=False).agg(agg_map)
    weekly = weekly.rename(columns={c: f"BK_{c}" for c in sum_cols}).rename(
        columns={"YEAR": "BK_YEAR"}
    )
    weekly["BOOKING_ROWS"] = (
        bk.groupby(["ITEM", "FG_WEEK"]).size()
        .reindex(list(zip(weekly["ITEM"], weekly["FG_WEEK"]))).values
    )
    print(f"[INFO]   สรุป booking รายสัปดาห์: {len(weekly):,} (item,week)")
    return weekly


def build_booking_file(
    dm: pd.DataFrame, ora_map: pd.DataFrame, weekly: pd.DataFrame
) -> pd.DataFrame:
    """ดูว่า item ที่ map มาแต่ละตัว -> booking มีแผนในสัปดาห์ไหนบ้าง

    เริ่มจากรายการ item ที่ map ได้ (unique จาก datamining + ORA_ITEM_CODE)
    แล้ว left join กับ booking รายสัปดาห์:
      - item ที่มี booking  -> 1 แถวต่อ 1 สัปดาห์ที่ booking มีแผน
      - item ที่ไม่มี booking -> 1 แถว (FG_WEEK ว่าง, HAS_BOOKING = N)
    """
    items = (
        dm[["ITEM"]].drop_duplicates()
        .merge(ora_map, on="ITEM", how="left")
    )

    weekly = weekly[weekly["ITEM"].isin(set(items["ITEM"]))].copy()

    combined = items.merge(weekly, on="ITEM", how="left")
    combined["HAS_BOOKING"] = combined["FG_WEEK"].notna().map({True: "Y", False: "N"})
    combined["BOOKING_ROWS"] = combined["BOOKING_ROWS"].fillna(0).astype(int)

    combined = combined.sort_values(
        ["ITEM", "FG_WEEK"], na_position="last"
    ).reset_index(drop=True)

    n_items_bk = combined.loc[combined["HAS_BOOKING"] == "Y", "ITEM"].nunique()
    n_items_no = combined.loc[combined["HAS_BOOKING"] == "N", "ITEM"].nunique()
    print(f"[INFO] item ที่มี booking: {n_items_bk} (รวม {(combined['HAS_BOOKING']=='Y').sum():,} สัปดาห์)")
    print(f"[INFO] item ที่ไม่มี booking: {n_items_no}")
    return combined


def save_excel(df: pd.DataFrame, path: str, sheet: str = "datamining_mapped") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet, index=False)
    print(f"[OK] บันทึก -> {path}  ({len(df):,} แถว)")


def main() -> int:
    start = datetime.now()
    try:
        dm = load_datamining()
        ora_map = build_ora_map()

        # ---- ไฟล์ 1: datamining + ORA_ITEM_CODE ----
        df = build_mapping(dm, ora_map)
        save_excel(df, OUTPUT_FILE)

        # ---- ไฟล์ 2: datamining + booking รายสัปดาห์ -> ไฟล์ใหม่แยก ----
        weekly = build_booking_weekly()
        df_bk = build_booking_file(dm, ora_map, weekly)
        save_excel(df_bk, OUTPUT_BOOKING_FILE, sheet="datamining_booking")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1
    print(f"[DONE] ใช้เวลา {datetime.now() - start}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

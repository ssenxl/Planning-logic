"""
map_item_view.py — อ่านไฟล์ผลลัพธ์ map item ระหว่าง Datamining กับ Booking แบบ read-only
"""
from pathlib import Path

import pandas as pd

import config


ROW_CAP = 5000
FILES = [
    {
        "id": "datamining_mapped.xlsx",
        "label": "Datamining → ORA Item",
        "desc": "ITEM จาก datamining ที่ map ไปเป็น ORA_ITEM_CODE จาก Master_Item",
    },
    {
        "id": "datamining_booking_mapped.xlsx",
        "label": "Datamining → Booking",
        "desc": "ITEM จาก datamining ที่เชื่อมกับ booking รายสัปดาห์",
    },
]


def _files_map() -> dict:
    return {f["id"]: f for f in FILES}


def _resolve(file_id: str) -> Path:
    meta = _files_map().get(Path(file_id).name)
    if not meta:
        raise KeyError(f"ไม่รู้จักไฟล์ '{file_id}'")
    p = config.OUTPUT_DIR / meta["id"]
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"ไม่พบไฟล์ {meta['id']}")
    return p


def list_files() -> list:
    out = []
    for meta in FILES:
        p = config.OUTPUT_DIR / meta["id"]
        item = {**meta, "exists": p.exists() and p.is_file()}
        if item["exists"]:
            st = p.stat()
            item.update({"name": p.name, "size": st.st_size, "mtime": st.st_mtime})
        else:
            item.update({"name": meta["id"], "size": 0, "mtime": None})
        out.append(item)
    return out


def read_grid(file_id: str, sheet: str = None) -> dict:
    p = _resolve(file_id)
    xls = pd.ExcelFile(p)
    sheets = list(xls.sheet_names)
    if not sheets:
        return {"sheet": None, "sheets": [], "columns": [], "rows": [], "total": 0, "truncated": False}
    if sheet is None or sheet not in sheets:
        sheet = sheets[0]

    df = xls.parse(sheet, dtype=object)
    total = len(df)
    truncated = total > ROW_CAP
    if truncated:
        df = df.head(ROW_CAP)
    df = df.where(pd.notna(df), "")

    return {
        "sheet": sheet,
        "sheets": sheets,
        "columns": [str(c) for c in df.columns],
        "rows": df.values.tolist(),
        "total": total,
        "truncated": truncated,
    }

"""
database.py — อ่านไฟล์ข้อมูลในโปรเจกต์แบบ read-only สำหรับหน้า "ฐานข้อมูล"
- list_datasets(): รายการไฟล์แยกกลุ่ม (Stock / Booking / แผนการผลิต / ข้อมูลอื่น)
- read_grid(file, sheet): อ่านไฟล์เป็น {columns, rows, sheets, total} (cap แถวเพื่อแสดงผล)
- _resolve(file): แปลง file id → Path พร้อมกันไม่ให้ออกนอกโฟลเดอร์ที่อนุญาต
หมายเหตุ: หน้านี้ "ดูอย่างเดียว" ไม่มีการเขียนทับไฟล์ — การอัปเดตข้อมูลมาจาก pipeline (ดึง DB)
"""
from pathlib import Path

import pandas as pd

import config

REPO_DIR = config.REPO_DIR
STOCK_DIR = REPO_DIR / "Stock"
BOOKING_DIR = REPO_DIR / "Booking"
ORDER_DIR = REPO_DIR / "Order"
DATAMINING_DIR = REPO_DIR / "Datamining"


# จำนวนแถวสูงสุดที่ส่งไปแสดงบน grid (ดาวน์โหลดได้เต็มไฟล์เสมอ)
ROW_CAP = 5000

# นิยามกลุ่มไฟล์ที่จะแสดง — ข้อมูลที่ pipeline ใช้งานจริง (Stock / Booking / SC / Datamining)
#   match: ฟังก์ชันคัดชื่อไฟล์ (รับ filename → bool)
GROUPS = [
    {"id": "stock", "label": "Stock", "dir": STOCK_DIR,
     "match": lambda n: n.lower().endswith((".xlsx", ".xls"))},
    {"id": "booking", "label": "Booking", "dir": BOOKING_DIR,
     "match": lambda n: n.lower().endswith((".xlsx", ".xls"))},
    {"id": "sc", "label": "SC", "dir": ORDER_DIR,
     "match": lambda n: n.lower().endswith((".xlsx", ".xls"))},
    {"id": "datamining", "label": "Datamining", "dir": DATAMINING_DIR,
     "match": lambda n: n.lower().endswith((".xlsx", ".xls"))},
]



def _groups_map() -> dict:
    return {g["id"]: g for g in GROUPS}


def list_datasets() -> list:
    """คืนรายการไฟล์แยกกลุ่ม → [{id,label,files:[{id,name,size,mtime}]}]"""
    out = []
    for g in GROUPS:
        files = []
        d: Path = g["dir"]
        if d.exists():
            cands = [f for f in d.iterdir()
                     if f.is_file() and not f.name.startswith("~$") and g["match"](f.name)]
            cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for f in cands:
                st = f.stat()
                files.append({"id": f"{g['id']}/{f.name}", "name": f.name,
                              "size": st.st_size, "mtime": st.st_mtime})
        out.append({"id": g["id"], "label": g["label"], "files": files})
    return out


def _resolve(file_id: str) -> Path:
    """file id = '<group>/<filename>' → Path (กัน path traversal: เอาเฉพาะ basename + ต้องอยู่ในโฟลเดอร์กลุ่ม)"""
    gid, _, fname = file_id.partition("/")
    g = _groups_map().get(gid)
    if not g:
        raise KeyError(f"ไม่รู้จักกลุ่ม '{gid}'")
    fname = Path(fname).name  # basename เท่านั้น กัน ../
    if not fname or not g["match"](fname):
        raise KeyError(f"ชื่อไฟล์ไม่ถูกต้อง '{fname}'")
    p = (g["dir"] / fname)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"ไม่พบไฟล์ {fname}")
    return p


def read_grid(file_id: str, sheet: str = None) -> dict:
    """อ่านไฟล์ → {sheet, sheets, columns, rows, total, truncated}
    ใช้ pandas (รองรับ .xlsx/.xls) คืนค่าที่คำนวณแล้ว; NaN → '' """
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

    columns = [str(c) for c in df.columns]
    rows = df.values.tolist()
    return {"sheet": sheet, "sheets": sheets, "columns": columns,
            "rows": rows, "total": total, "truncated": truncated}

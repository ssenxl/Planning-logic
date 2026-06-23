"""
masters.py — อ่าน/เขียนไฟล์ Master (.xlsx) รายชีท สำหรับ grid editor
- อ่าน: คืน columns + rows เป็น JSON
- เขียน: สำรองไฟล์เดิมเป็น .bak ก่อน, แก้เฉพาะชีทเป้าหมาย (ชีทอื่นคงเดิม)
"""
import re
import shutil
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

from config import master_files

_INT_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")
_FLOAT_RE = re.compile(r"^-?(0|[1-9][0-9]*|0?)\.[0-9]+$")


def list_masters() -> list:
    """คืนรายการไฟล์ Master + ชีท + สถานะมีไฟล์จริงไหม"""
    out = []
    for name, path in master_files().items():
        info = {"name": name, "path": str(path), "exists": path.exists(), "sheets": []}
        if path.exists():
            try:
                wb = load_workbook(path, read_only=True, data_only=False)
                info["sheets"] = wb.sheetnames
                wb.close()
            except Exception as e:
                info["error"] = str(e)
        out.append(info)
    return out


def _resolve(name: str) -> Path:
    reg = master_files()
    if name not in reg:
        raise KeyError(f"ไม่รู้จัก Master '{name}'")
    p = reg[name]
    if not p.exists():
        raise FileNotFoundError(f"ไม่พบไฟล์ {p}")
    return p


def read_sheet(name: str, sheet: str) -> dict:
    """อ่านชีท → {columns:[...], rows:[[...]]}  (แถวแรก = header)"""
    path = _resolve(name)
    wb = load_workbook(path, read_only=True, data_only=False)
    if sheet not in wb.sheetnames:
        wb.close()
        raise KeyError(f"ไม่พบชีท '{sheet}' ใน {name}")
    ws = wb[sheet]
    rows = []
    for r in ws.iter_rows(values_only=True):
        rows.append(["" if v is None else v for v in r])
    wb.close()

    if not rows:
        return {"columns": [], "rows": []}

    header = [str(c) if c != "" else f"Col{i+1}" for i, c in enumerate(rows[0])]
    data = rows[1:]
    # pad ให้ทุกแถวยาวเท่า header
    width = len(header)
    norm = [list(row) + [""] * (width - len(row)) for row in data]
    norm = [row[:width] for row in norm]
    return {"columns": header, "rows": norm}


def _coerce(v):
    """แปลงค่าจาก grid (string) → ชนิดที่เหมาะ; เก็บรหัสที่มี leading zero เป็น string"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip()
    if s == "":
        return None
    if _INT_RE.match(s):
        try:
            return int(s)
        except ValueError:
            return s
    if _FLOAT_RE.match(s):
        try:
            return float(s)
        except ValueError:
            return s
    return s


def write_sheet(name: str, sheet: str, columns: list, rows: list) -> dict:
    """เขียนชีทกลับ (แก้เฉพาะชีทนี้ ชีทอื่นคงเดิม) + สำรอง .bak"""
    path = _resolve(name)

    # สำรองไฟล์เดิม
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_suffix(path.suffix + f".{ts}.bak")
    shutil.copy2(path, bak)

    wb = load_workbook(path, data_only=False)
    if sheet not in wb.sheetnames:
        wb.close()
        raise KeyError(f"ไม่พบชีท '{sheet}' ใน {name}")
    ws = wb[sheet]

    # ล้างค่าทุกเซลล์เดิมในชีท แล้วเขียนใหม่ (header + rows)
    if ws.max_row > 0:
        ws.delete_rows(1, ws.max_row)

    ws.append([str(c) for c in columns])
    for row in rows:
        ws.append([_coerce(v) for v in row])

    wb.save(path)
    wb.close()
    return {"ok": True, "backup": bak.name, "rows": len(rows)}

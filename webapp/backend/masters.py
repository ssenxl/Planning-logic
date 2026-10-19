"""
masters.py — อ่าน/เขียนไฟล์ Master (.xlsx) รายชีท สำหรับ grid editor
- อ่าน: คืน columns + rows เป็น JSON
- เขียน: สำรองไฟล์เดิมเป็น .bak ก่อน, แก้เฉพาะชีทเป้าหมาย (ชีทอื่นคงเดิม)
"""
import re
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook

import edit_diff
import livesync
from config import master_files

_INT_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")
_FLOAT_RE = re.compile(r"^-?(0|[1-9][0-9]*|0?)\.[0-9]+$")
# ค่าวันที่ที่ backend ส่งให้ frontend เป็น ISO string — ต้องแปลงกลับเป็นวันที่จริงตอนเขียน
_ISO_DT_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?)?$"
)
# ชีทที่ตัดคอลัมน์หัวว่างทิ้ง (ในไฟล์เป็นแค่ตัวเลขทด/โน้ต ไม่มีโค้ดอ่าน) — เซฟครั้งถัดไปหายจากไฟล์
# ห้ามใส่ MasterMC: AVA_MC/Planning อ่านคอลัมน์หัวว่าง "Unnamed: 8" อยู่
_DROP_BLANK_COLS = {"Target_Stock"}
# คอลัมน์มีชื่อที่เลิกใช้แล้ว ตัดทิ้งแบบเดียวกัน (เทียบด้วย _norm_hdr)
# Target_Stock: ห้ามตัด "Type 1" — ใช้คิด STOCK MAX (RTS + CG) ใน _recompute_target_stock/Stock.py/core_registry
_DROP_NAMED_COLS = {"Target_Stock": {"TYPE", "TYPE 2"}}
# หัวที่ระบบตั้งให้เองตอนหัวว่าง (Col13) — เคยถูกเซฟกลับลงไฟล์ได้ จึงนับว่าว่างด้วย
_PLACEHOLDER_HDR_RE = re.compile(r"^Col\d+$")


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
    """อ่านชีท → {columns:[...], rows:[[...]]}  (แถวแรก = header)
    data_only=True: คืน "ค่าที่คำนวณแล้ว" ของเซลล์สูตร (ไม่ใช่ข้อความสูตร =...)
    เพื่อให้ grid โชว์ตัวเลขจริง และบน server ไม่พึ่ง Excel"""
    path = _resolve(name)
    wb = load_workbook(path, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        wb.close()
        raise KeyError(f"ไม่พบชีท '{sheet}' ใน {name}")
    rows = [["" if v is None else v for v in r] for r in wb[sheet].iter_rows(values_only=True)]
    wb.close()
    return _grid_of(rows, drop_blank=name in _DROP_BLANK_COLS, drop=_DROP_NAMED_COLS.get(name))


def parse_upload(content: bytes, sheet: str, name: str = "") -> dict:
    """ไฟล์ .xlsx ที่อัปโหลดมานำเข้าทับชีท → {sheet, columns, rows} (ยังไม่บันทึก)
    ใช้ชีทชื่อเดียวกับชีทปลายทางถ้ามี ไม่งั้นชีทแรก — จับคู่คอลัมน์ทำที่หน้าเว็บ"""
    import io
    try:
        wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception:
        raise ValueError("เปิดไฟล์ไม่ได้ — รองรับเฉพาะ .xlsx")
    want = _norm_hdr(sheet)
    ws = next((w for w in wb.worksheets if _norm_hdr(w.title) == want), wb.worksheets[0])
    rows = [["" if v is None else v for v in r] for r in ws.iter_rows(values_only=True)]
    wb.close()
    # แถวว่างท้ายชีทที่ Excel จำขอบเขตไว้ ไม่ใช่ข้อมูล
    while rows and not any(str(v).strip() for v in rows[-1]):
        rows.pop()
    if not rows:
        raise ValueError("ไฟล์ว่าง")
    return {"sheet": ws.title, **_grid_of(rows, drop_blank=name in _DROP_BLANK_COLS,
                                          drop=_DROP_NAMED_COLS.get(name))}


def _grid_of(rows: list, drop_blank: bool = False, drop=None) -> dict:
    """แถวดิบจาก openpyxl (แถวแรก = header) → {columns, rows} ที่ทุกแถวยาวเท่า header
    drop_blank=True: ตัดคอลัมน์ที่หัวว่าง/หัว ColNN ทิ้งทั้งคอลัมน์
    drop: ชื่อหัว (แบบ _norm_hdr) ที่ตัดทิ้งทั้งคอลัมน์"""
    if not rows:
        return {"columns": [], "rows": []}

    header = [str(c) if c != "" else f"Col{i+1}" for i, c in enumerate(rows[0])]
    data = rows[1:]
    # pad ให้ทุกแถวยาวเท่า header
    width = len(header)
    norm = [list(row) + [""] * (width - len(row)) for row in data]
    norm = [row[:width] for row in norm]
    if drop_blank or drop:
        def _keep(c) -> bool:
            s = str(c).strip()
            if drop_blank and (not s or _PLACEHOLDER_HDR_RE.match(s)):
                return False
            return not (drop and _norm_hdr(s) in drop)
        keep = [i for i, c in enumerate(rows[0]) if _keep(c)]
        header = [header[i] for i in keep]
        norm = [[row[i] for i in keep] for row in norm]
    return {"columns": header, "rows": norm}


def _coerce(v):
    """แปลงค่าจาก grid (string) → ชนิดที่เหมาะ; เก็บรหัสที่มี leading zero เป็น string"""
    if v is None:
        return None
    if isinstance(v, (int, float, date, datetime)):
        return v
    s = str(v).strip()
    if s == "":
        return None
    m = _ISO_DT_RE.match(s)
    if m:
        y, mo, d, hh, mi, ss = m.groups()
        # ไม่มีเวลา หรือเวลาเป็น 00:00:00 → เก็บเป็น date (Excel โชว์เป็นวันที่ ไม่ใช่ข้อความ)
        if not hh or (int(hh), int(mi), int(ss or 0)) == (0, 0, 0):
            return date(int(y), int(mo), int(d))
        return datetime(int(y), int(mo), int(d), int(hh), int(mi), int(ss or 0))
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


def _norm_hdr(s) -> str:
    return re.sub(r"\s+", " ", str(s).strip()).upper()


def _to_num(v):
    """แปลงเป็น float ถ้าได้ ไม่งั้นคืน None"""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except ValueError:
        return None


def _clean_num(x):
    """ทำให้เลขจำนวนเต็มเป็น int (กัน 2500.0)"""
    if isinstance(x, float) and x.is_integer():
        return int(x)
    return x


def _recompute_target_stock(columns: list, rows: list) -> list:
    """คำนวณคอลัมน์สูตรของ Target_Stock ใหม่จากคอลัมน์ดิบ (จับด้วย "ชื่อหัวคอลัมน์")
    เพื่อให้ไฟล์เก็บ "ค่าตัวเลขจริง" ไม่พึ่ง Excel cache → pandas บน server อ่านได้ถูกต้อง
      Match core   = Item code & Team Name
      TARGET SCM   = TARGET/MONTH / 4 * 2
      STOCK MIN    = TARGET/MONTH / 4
      STOCK MAX    = IF Team=RTS: (Type1=CG ? TARGET/MONTH : TARGET SCM*1.5) else TARGET SCM*1.5
      Stock 5 Week = STOCK MIN * 5
    คำนวณเฉพาะคอลัมน์ที่มีอยู่ (รองรับการเพิ่ม/ลบ/เปลี่ยนชื่อคอลัมน์)"""
    idx = {_norm_hdr(c): i for i, c in enumerate(columns)}
    item_i = idx.get("ITEM CODE")
    team_i = idx.get("TEAM NAME")
    type1_i = idx.get("TYPE 1")
    target_i = idx.get("TARGET/MONTH")
    match_i = idx.get("MATCH CORE")
    scm_i = idx.get("TARGET SCM")
    min_i = idx.get("STOCK MIN")
    max_i = idx.get("STOCK MAX")
    week_i = idx.get("STOCK 5 WEEK")

    def get(r, i):
        return r[i] if (i is not None and i < len(r)) else None

    out = []
    for row in rows:
        r = list(row)
        target = _to_num(get(r, target_i))

        # Match core = Item code & Team Name (ต่อข้อความ)
        if match_i is not None and (item_i is not None or team_i is not None):
            a = "" if get(r, item_i) is None else str(get(r, item_i))
            b = "" if get(r, team_i) is None else str(get(r, team_i))
            r[match_i] = a + b

        # TARGET SCM = TARGET/MONTH / 4 * 2
        scm = None
        if scm_i is not None:
            if target is not None:
                scm = target / 4 * 2
                r[scm_i] = _clean_num(scm)
            else:
                scm = _to_num(get(r, scm_i))
        else:
            scm = (target / 4 * 2) if target is not None else None

        # STOCK MIN = TARGET/MONTH / 4
        if min_i is not None and target is not None:
            r[min_i] = _clean_num(target / 4)

        # STOCK MAX
        if max_i is not None:
            team = str(get(r, team_i)).strip().upper() if get(r, team_i) is not None else ""
            t1 = str(get(r, type1_i)).strip().upper() if get(r, type1_i) is not None else ""
            if team == "RTS" and t1 == "CG":
                mx = target
            else:
                mx = (scm * 1.5) if scm is not None else None
            if mx is not None:
                r[max_i] = _clean_num(mx)

        # Stock 5 Week = STOCK MIN * 5
        if week_i is not None:
            mn = (target / 4) if target is not None else _to_num(get(r, min_i))
            if mn is not None:
                r[week_i] = _clean_num(mn * 5)

        out.append(r)
    return out


def write_sheet(name: str, sheet: str, columns: list, rows: list,
                user: str = "", base_rev=None) -> dict:
    """เขียนชีทกลับ (แก้เฉพาะชีทนี้ ชีทอื่นคงเดิม) + สำรอง .bak

    base_rev = เลขรุ่นที่หน้าเว็บโหลดไป ; ไม่ตรงกับของจริง = มีคนแก้แซง → โยน Conflict
    (ไม่เขียนทับเงียบ ๆ) ; การเขียนทั้งก้อนอยู่ใต้ล็อกไฟล์เดียวกันกับคนอื่นที่แก้ไฟล์นี้
    """
    path = _resolve(name)
    res = livesync.masters_res(name, sheet)

    # Target_Stock: คำนวณคอลัมน์สูตรใหม่ก่อนเขียน (เก็บค่าจริง ใช้บน server ได้)
    if name == "Target_Stock":
        rows = _recompute_target_stock(columns, rows)

    with livesync.file_lock(path):
        livesync.check_rev(res, base_rev)
        # ค่าก่อนแก้ — อ่านแบบเดียวกับที่หน้าเว็บเห็น (ค่าคำนวณแล้ว ไม่ใช่สูตร) ไว้จดประวัติว่าเปลี่ยนอะไร
        try:
            before = read_sheet(name, sheet)
        except Exception:
            before = None
        # สำรองไฟล์เดิม — ชั่วโมงละครั้งพอ (เซฟอัตโนมัติทุกการขยับทำให้ .bak ทุกครั้งไม่ไหว)
        bak = livesync.backup_throttled(path)

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

        with livesync.atomic_save(path) as tmp:
            wb.save(tmp)
        wb.close()
        changes = None
        if before is not None:
            try:
                changes = edit_diff.sheet_changes(before["columns"], before["rows"],
                                                  [str(c) for c in columns],
                                                  [[_coerce(v) for v in r] for r in rows])
            except Exception as e:
                print(f"[MASTERS] เทียบก่อน/หลังแก้ {name}/{sheet} ไม่ได้: {e}")
        ent = livesync.bump(res, user=user, detail=f"{name}/{sheet} {len(rows)} แถว",
                            changes=changes)

    return {"ok": True, "backup": bak, "rows": len(rows), "rev": ent["rev"]}

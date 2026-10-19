"""Blocklist — รายการใบสั่ง/item ที่ "ห้ามนำมาวางแผน" (มองข้ามไปเลย)

ไฟล์รายการ: Blocklist/blocked_orders.xlsx (แก้ได้ด้วย Excel ไม่ต้องแก้โค้ด)
คอลัมน์ที่ใช้จริง: SO_NO, ITEM_CODE, ORDER_TYPE  (REASON / DATE_ADDED เป็นบันทึกช่วยจำเฉย ๆ)

กติกาจับคู่ต่อ 1 แถวของ blocklist:
  - มีทั้ง SO_NO และ ITEM_CODE → บล็อกเฉพาะคู่นั้น (ใบนี้ + item นี้)
  - มีแต่ SO_NO            → บล็อกทั้งใบ (ทุก item ของใบนั้น)
  - มีแต่ ITEM_CODE        → บล็อก item นั้นทุกใบ
  - มีแต่ ORDER_TYPE       → บล็อกทุกแถวของ type นั้น (เช่น 'ผ้าเติม' = ไม่วางแผนงานผ้าเติมเลย)
  - ระบุ ORDER_TYPE คู่กับ SO/item → ต้องตรง type นั้นด้วยถึงบล็อก ; เว้นว่าง = ทุก type
    (จำเป็น: ใบ+item เดียวกันมีได้ทั้ง 'ผ้าเติม' และ 'SC-ORDERS' คนละแถว เช่น
     721558 / FD3FLATKNIT103/05B0 — ถ้าไม่ดู type จะตัด SC-ORDERS ที่ยังต้องผลิตไปด้วย)
เลขใบเทียบแบบ normalize: ตัด 'S' นำหน้า, ตัด '.0' ท้าย (pandas อ่านเลขใบเป็น float),
upper/strip — เพราะ view_sc เก็บ SC_NO='S720398' แต่ SO_NO=720398 (คนละรูปของใบเดียวกัน)

หมายเหตุขอบเขต: กรองเฉพาะฝั่ง "ดีมานด์ SC" (Order.py) เท่านั้น ไม่แตะ booking —
แถว booking หนึ่งบล็อกมี SO_NO ปนหลายใบได้ (เช่น '714812,717525') ตัดทั้งแถวจะกระทบใบที่ไม่ได้บล็อก
"""

import re
from pathlib import Path

import pandas as pd

_APP_DIR = Path(__file__).parent
BLOCKLIST_DIR = _APP_DIR / "Blocklist"
BLOCKLIST_FILE = BLOCKLIST_DIR / "blocked_orders.xlsx"

# ชื่อคอลัมน์ที่เป็นไปได้ของ input (raw export ใช้ 'SC/SO NO'/'Item Code' ส่วน view_sc ใช้ SO_NO/SC_NO/FABRIC_ITEM)
_SO_COLS = ("SO_NO", "SC_NO", "SC/SO NO", "SC/SO_NO", "SCSO_NO")
_ITEM_COLS = ("ITEM_CODE", "FABRIC_ITEM", "ITEM CODE", "ITEM_NO")
_TYPE_COLS = ("ORDER_TYPE", "ORDERS TYPE", "ORDERS_TYPE")


def _norm_so(v) -> str:
    """เลขใบสั่ง → รูปเทียบมาตรฐาน: 'S720398' / '720398.0' / ' 720398 ' → '720398'"""
    s = str(v).strip().upper()
    if s in ("", "NAN", "NONE", "NAT"):
        return ""
    s = re.sub(r"\.0+$", "", s)
    return s.lstrip("S")


def _norm_item(v) -> str:
    s = str(v).strip().upper()
    return "" if s in ("", "NAN", "NONE", "NAT") else s


def _so_tokens(v) -> set:
    """ช่อง SO อาจยุบหลายใบไว้ด้วยกัน ('714812,717525' / 'A | B') → แตกเป็นชุดเลขใบ"""
    parts = re.split(r"[|,;/\s]+", str(v))
    return {t for t in (_norm_so(p) for p in parts) if t}


def _hit(container: set, key: tuple, types) -> bool:
    """เช็คคีย์ในเซ็ตที่ช่องสุดท้ายเป็น ORDER_TYPE

    types=None → ข้อมูลฝั่ง input ไม่มีคอลัมน์ ORDER_TYPE ให้เทียบ → เทียบข้าม type ไปเลย
    (เจตนาคือบล็อก ถ้าแยก type ไม่ได้ก็ถือว่าเข้าข่าย)
    """
    if types is None:
        return any(k[:-1] == key for k in container)
    return any(key + (t,) in container for t in types)


def _pick_col(df: pd.DataFrame, candidates) -> str | None:
    lookup = {str(c).strip().upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in lookup:
            return lookup[cand.upper()]
    return None


def load_blocklist(path: Path = None) -> tuple:
    """อ่านไฟล์ blocklist → (คู่ (so,item,type), เซ็ต (ใบ,type), เซ็ต (item,type), เซ็ต type ล้วน)

    type = "" หมายถึงบล็อกทุก ORDER_TYPE
    ไม่มีไฟล์ = ไม่บล็อกอะไรเลย (pipeline ต้องรันได้ปกติแม้ไม่มีไฟล์นี้)
    อ่านเฉพาะชีทแรก — ชีทอื่นในไฟล์เป็นรายการอ้างอิงที่ไม่ได้บังคับใช้
    """
    path = Path(path) if path else BLOCKLIST_FILE
    empty = (set(), set(), set(), set())
    if not path.exists():
        return empty

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, encoding="utf-8-sig")
    else:
        df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]

    so_col = _pick_col(df, _SO_COLS)
    item_col = _pick_col(df, _ITEM_COLS)
    type_col = _pick_col(df, _TYPE_COLS)
    if so_col is None and item_col is None and type_col is None:
        print(f"⚠️ blocklist: ไม่พบคอลัมน์ SO_NO/ITEM_CODE/ORDER_TYPE ใน {path.name} — ข้ามการบล็อก")
        return empty

    pairs, sos, items, otypes = set(), set(), set(), set()
    for _, r in df.iterrows():
        so = _norm_so(r[so_col]) if so_col else ""
        item = _norm_item(r[item_col]) if item_col else ""
        otype = _norm_item(r[type_col]) if type_col else ""
        if so and item:
            pairs.add((so, item, otype))
        elif so:
            sos.add((so, otype))
        elif item:
            items.add((item, otype))
        elif otype:
            otypes.add(otype)          # ระบุแค่ type = บล็อกทั้งประเภท
    return pairs, sos, items, otypes


def is_blocked(so_no, item_code, blocklist=None, order_type=None) -> bool:
    """เช็คใบเดียว — ใช้ได้จากโมดูลอื่น (เช่น Replan) โดยส่ง blocklist ที่โหลดไว้แล้วมา

    order_type=None → ไม่รู้ type ของแถวนี้ → เทียบข้าม type
    """
    pairs, sos, items, otypes = blocklist if blocklist is not None else load_blocklist()
    item = _norm_item(item_code)
    # "" = รายการบล็อกที่ไม่ระบุ type (บล็อกทุก type) ; ot = ตรง type ของแถวนี้
    ot = _norm_item(order_type) if order_type is not None else None
    types = None if ot is None else ({""} | ({ot} if ot else set()))
    if ot and ot in otypes:            # บล็อกทั้งประเภท
        return True
    if item and _hit(items, (item,), types):
        return True
    for tok in _so_tokens(so_no):
        if _hit(sos, (tok,), types):
            return True
        if item and _hit(pairs, (tok, item), types):
            return True
    return False


def filter_blocked(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """ตัดแถวที่อยู่ใน blocklist ออกจาก DataFrame ของออร์เดอร์

    งานที่ถูกตัดตรงนี้ "หายเงียบ" ตามที่ตกลงไว้ — ไม่เข้าชีท UNPLANNED
    (ต่างจาก filter_order_type/filter_mc_group ที่เก็บไว้โชว์) แต่ยัง print สรุปให้ตรวจย้อนหลังจาก log ได้
    """
    pairs, sos, items, otypes = load_blocklist()
    if not (pairs or sos or items or otypes):
        if verbose:
            print("ℹ️ blocklist: ไม่มีรายการบล็อก (ไม่พบไฟล์หรือไฟล์ว่าง)")
        return df

    so_col = _pick_col(df, _SO_COLS)
    sc_col = _pick_col(df, ("SC_NO",))
    item_col = _pick_col(df, _ITEM_COLS)
    type_col = _pick_col(df, _TYPE_COLS)
    if so_col is None and sc_col is None and item_col is None and type_col is None:
        if verbose:
            print("⚠️ blocklist: input ไม่มีคอลัมน์ใบสั่ง/item/ORDER_TYPE — ข้ามการบล็อก")
        return df

    if type_col is None and verbose:
        msg = "⚠️ blocklist: input ไม่มีคอลัมน์ ORDER_TYPE — จะบล็อกโดยไม่แยก type"
        if otypes:
            msg += f" และบล็อกทั้งประเภท ({', '.join(sorted(otypes))}) ไม่ทำงาน"
        print(msg)

    def _row_blocked(row) -> bool:
        item = _norm_item(row[item_col]) if item_col else ""
        ot = _norm_item(row[type_col]) if type_col else None
        types = None if ot is None else ({""} | ({ot} if ot else set()))
        if ot and ot in otypes:        # บล็อกทั้งประเภท (เช่น ผ้าเติม)
            return True
        if item and _hit(items, (item,), types):
            return True
        toks = set()
        for c in (so_col, sc_col):
            if c is not None:
                toks |= _so_tokens(row[c])
        for tok in toks:
            if _hit(sos, (tok,), types):
                return True
            if item and _hit(pairs, (tok, item), types):
                return True
        return False

    before = len(df)
    mask = df.apply(_row_blocked, axis=1) if before else pd.Series([], dtype=bool)
    out = df[~mask]
    if verbose:
        _rules = f"{len(pairs)} คู่ / {len(sos)} ใบ / {len(items)} item"
        if otypes:
            _rules += f" / ทั้งประเภท: {', '.join(sorted(otypes))}"
        print(f"🚫 Blocklist: {before} → {len(out)} (ตัดออก {int(mask.sum())} แถว, รายการบล็อก {_rules})")
    return out

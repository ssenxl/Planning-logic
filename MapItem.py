"""
MapItem — เชื่อม (map) ข้อมูล item ระหว่าง 2 ไฟล์:

    Datamining/view_datamining.xlsx   (คอลัมน์ ITEM = ระดับ "ITEM Color")
            join  key: ITEM = รหัสเต็มที่ตัด suffix A0/B0 ออก
    Master_Item.xlsx ชีท "Master_Item V2"
            1 แถว = 1 spec เครื่องถัก, ITEM_LIST = รหัสเต็มทุกตัวที่ "ใช้แทนกันได้"

ผลลัพธ์: ตาราง view_datamining เดิม + คอลัมน์:
    ORA_ITEM_CODE       = รหัสเต็มของ item คั่นด้วย ", " (เช่น F100413A0, F100413B0)
    ORA_ITEM_COUNT      = จำนวนรหัสเต็ม
    STOCK_BALANCE_KG    = stock ของรหัสตัวเอง (QA ว่าง)
    SPEC_KEY            = คีย์กลุ่มทดแทน (spec เครื่อง+ด้ายเหมือนกัน)
    SUB_GROUP_COUNT     = จำนวนรหัส "อื่น" ที่ใช้แทนกันได้
    SUB_GROUP_ITEMS     = รายการรหัสอื่นที่ใช้แทนกันได้
    SUB_GROUP_STOCK_KG  = stock รวมทั้งกลุ่มทดแทน (รวมตัวเอง)
    MC_GROUP, KNIT_MC_CAT, MC_GAUGE, YARN_ITEM, MC_NEEDLE, YARN_SL
1 datamining item = 1 แถว (ไม่แตกแถว)

Output:
    data_plan/datamining_mapped.xlsx

Usage:
    python MapItem.py
"""

import os
import re
import shutil
import sys
import tempfile
import configparser
from datetime import datetime
from pathlib import Path

import pandas as pd


# ---------- CONFIG ----------
BASE = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
CONFIG_INI = Path(os.environ.get("KNITPLAN_CONFIG", str(BASE / "config.ini")))


def _resolve_master_item_file() -> str:
    """หา path ของ Master_Item.xlsx ตามลำดับ:
    1) env KNITPLAN_MASTER_ITEM
    2) config.ini [paths] master_item
    3) path เดิมบน Windows (backward compatible)
    """
    env_path = os.environ.get("KNITPLAN_MASTER_ITEM")
    if env_path:
        return os.path.expandvars(env_path)

    cfg = configparser.ConfigParser(interpolation=None)
    try:
        cfg.read(CONFIG_INI, encoding="utf-8")
        if cfg.has_section("paths") and cfg.has_option("paths", "master_item"):
            return os.path.expandvars(cfg.get("paths", "master_item"))
    except Exception:
        pass

    return r"C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\Master_Item.xlsx"


DATAMINING_FILE = str(BASE / "Datamining" / "view_datamining.xlsx")
MASTER_ITEM_FILE = _resolve_master_item_file()
BOOKING_FILE = str(BASE / "Booking" / "view_booking.xlsx")
STOCK_FILE = str(BASE / "Stock" / "view_stock.xlsx")
OUTPUT_FILE = str(BASE / "data_plan" / "datamining_mapped.xlsx")
OUTPUT_BOOKING_FILE = str(BASE / "data_plan" / "datamining_booking_mapped.xlsx")


# ---------- Master_Item ----------
SHEET_V2 = "Master_Item V2"        # ชีทหลัก: 1 แถว = 1 spec, ITEM_LIST = รหัสที่ใช้แทนกันได้
SHEET_V1 = "Master Item"           # ชีทเดิม (fallback): ITEM  Color -> ORA_ITEM_CODE

# ชื่อคอลัมน์ในชีทเดิม (มี 2 ช่องว่างใน "ITEM  Color" ตามไฟล์ต้นทาง)
COL_COLOR = "ITEM  Color"          # key: = ITEM ใน datamining
COL_ORA = "ORA_ITEM_CODE"          # รหัสเต็มที่มี suffix A0/B0

# ชื่อคอลัมน์ในชีท V2
COL_ITEM_LIST = "ITEM_LIST"        # รหัสเต็มทุกตัวที่ใช้แทนกันได้ คั่นด้วย ", "
COL_SUFFIX = "ITEM_SUFFIX"         # A0 (อบผ่า) / B0 (อบกลม)
COL_SPEC_KEY = "SPEC_KEY"          # คีย์กลุ่มทดแทน (unique 1 แถว)
V2_SPEC_COLS = ["MC_GROUP", "KNIT_MC_CAT", "MC_GAUGE", "YARN_ITEM", "MC_NEEDLE", "YARN_SL"]

# คอลัมน์กลุ่มทดแทนที่เติมเข้าไปในผลลัพธ์ (ลำดับคงที่)
GROUP_OUT_COLS = [
    COL_SPEC_KEY, "SUB_GROUP_COUNT", "SUB_GROUP_ITEMS", "SUB_GROUP_STOCK_KG", *V2_SPEC_COLS
]

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


def _split_codes(cell) -> list:
    """แยกรหัสเต็มจากเซลล์ที่คั่นด้วย ',' (ตัดค่าว่าง/nan ทิ้ง)"""
    if pd.isna(cell):
        return []
    return [c.strip() for c in str(cell).split(",") if c.strip() and c.strip().lower() != "nan"]


def _derive_item(code, suffix="") -> str:
    """รหัสเต็ม -> ITEM Color (ตัด suffix A0/B0 ท้ายออก)
    ใช้ ITEM_SUFFIX จากไฟล์ก่อน ถ้าไม่ตรงค่อย fallback regex 'ตัวอักษร+เลข' ท้ายสุด
    """
    c = str(code).strip()
    suf = str(suffix).strip().upper()
    if suf and suf != "NAN" and c.upper().endswith(suf):
        return c[: -len(suf)]
    return re.sub(r"[A-Z]\d$", "", c)


def _pairs_to_ora_map(pairs: pd.DataFrame) -> pd.DataFrame:
    """คู่ (ITEM, ORA_ITEM_CODE) -> 1 แถว/ITEM: รหัสเต็มรวมในเซลล์เดียว + จำนวน"""
    pairs = pairs.drop_duplicates()
    ora_map = (
        pairs.groupby("ITEM")[COL_ORA]
        .agg(lambda s: ", ".join(sorted(s.astype(str).unique())))
        .rename("ORA_ITEM_CODE")
        .reset_index()
    )
    ora_cnt = (
        pairs.groupby("ITEM")[COL_ORA].nunique()
        .rename("ORA_ITEM_COUNT")
        .reset_index()
    )
    return ora_map.merge(ora_cnt, on="ITEM")


def _load_master_v2() -> tuple:
    """อ่านชีท 'Master_Item V2' -> (ora_map, spec_by_code, group_codes)

    V2: 1 แถว = 1 spec เครื่องถัก, ITEM_LIST = รหัสเต็มทุกตัวที่ "ใช้แทนกันได้"
      spec_by_code : รหัสเต็ม -> {SPEC_KEY, MC_GROUP, ...}
      group_codes  : SPEC_KEY -> [รหัสเต็มทั้งกลุ่ม]
    """
    cols = [COL_ITEM_LIST, COL_SUFFIX, COL_SPEC_KEY, *V2_SPEC_COLS]
    mi = _read_excel_safe(MASTER_ITEM_FILE, sheet_name=SHEET_V2, usecols=cols)

    codes_per_row = mi[COL_ITEM_LIST].apply(_split_codes)

    group_codes = {}
    spec_by_code = {}
    pairs = []
    for codes, rec in zip(codes_per_row, mi.to_dict("records")):
        if not codes:
            continue
        key = str(rec[COL_SPEC_KEY]).strip()
        group_codes.setdefault(key, []).extend(codes)
        attrs = {COL_SPEC_KEY: key, **{c: rec.get(c) for c in V2_SPEC_COLS}}
        for code in codes:
            spec_by_code[code] = attrs
            pairs.append((_derive_item(code, rec.get(COL_SUFFIX)), code))

    ora_map = _pairs_to_ora_map(pd.DataFrame(pairs, columns=["ITEM", COL_ORA]))
    n_multi = sum(1 for v in group_codes.values() if len(v) > 1)
    print(f"[INFO]   ชีท '{SHEET_V2}': {len(mi):,} spec | รหัสเต็ม {len(spec_by_code):,} "
          f"| ITEM Color {len(ora_map):,} | กลุ่มที่ใช้แทนกันได้ (>1 รหัส) {n_multi:,}")
    return ora_map, spec_by_code, group_codes


def _load_master_v1() -> pd.DataFrame:
    """fallback: ชีทเดิม 'Master Item' (ITEM  Color -> ORA_ITEM_CODE) — ไม่มีข้อมูลกลุ่มทดแทน"""
    mi = _read_excel_safe(MASTER_ITEM_FILE, sheet_name=SHEET_V1, usecols=[COL_COLOR, COL_ORA])
    mi[COL_COLOR] = mi[COL_COLOR].astype(str).str.strip()
    mi[COL_ORA] = mi[COL_ORA].astype(str).str.strip()
    ora_map = _pairs_to_ora_map(
        mi[[COL_COLOR, COL_ORA]].rename(columns={COL_COLOR: "ITEM"})
    )
    print(f"[INFO]   ชีท '{SHEET_V1}': ITEM Color ที่ map ได้ {len(ora_map):,}")
    return ora_map


_MASTER_CACHE = None


def load_master_item(force: bool = False) -> tuple:
    """โหลด Master_Item -> (ora_map, spec_by_code, group_codes) พร้อม cache

    ใช้ชีท 'Master_Item V2' เป็นหลัก; ถ้าไฟล์ยังไม่มีชีทนี้ (เครื่องอื่น/ไฟล์เก่า)
    จะถอยกลับไปใช้ชีท 'Master Item' แบบเดิม โดยคอลัมน์กลุ่มทดแทนจะว่าง
    """
    global _MASTER_CACHE
    if _MASTER_CACHE is not None and not force:
        return _MASTER_CACHE

    print(f"[INFO] โหลด Master_Item: {MASTER_ITEM_FILE}")
    if not os.path.exists(MASTER_ITEM_FILE):
        raise FileNotFoundError(
            f"ไม่พบไฟล์ Master_Item.xlsx ที่ {MASTER_ITEM_FILE} "
            f"(กำหนดได้ผ่าน config.ini [paths] master_item หรือ env KNITPLAN_MASTER_ITEM)"
        )
    try:
        _MASTER_CACHE = _load_master_v2()
    except (ValueError, KeyError) as e:
        print(f"[WARN] อ่านชีท '{SHEET_V2}' ไม่ได้ ({e}) — ถอยไปใช้ชีท '{SHEET_V1}' แบบเดิม "
              f"(คอลัมน์กลุ่มทดแทนจะว่าง)")
        _MASTER_CACHE = (_load_master_v1(), {}, {})
    return _MASTER_CACHE


def build_ora_map() -> pd.DataFrame:
    """map ITEM (=ITEM Color) -> ORA_ITEM_CODE (รหัสเต็ม A0/B0 รวมในเซลล์เดียว) + จำนวน"""
    return load_master_item()[0]


def _report_ora_quality(merged: pd.DataFrame) -> None:
    unmatched = sorted(merged.loc[merged["ORA_ITEM_CODE"].isna(), "ITEM"].unique())
    if unmatched:
        print(f"[WARN] item ที่ map ไม่เจอใน Master_Item: {len(unmatched)} ตัว -> {unmatched[:20]}")
    else:
        print("[OK] map ครบทุก item")
    multi = merged.loc[merged["ORA_ITEM_COUNT"] > 1, "ITEM"].nunique()
    if multi:
        print(f"[INFO] item ที่มีรหัสเต็มหลายตัว (ORA_ITEM_COUNT>1): {multi} ตัว")


def _suffix_for_tubular(desc) -> str | None:
    """แปลง TUBULAR_TYPE_DESC -> suffix รหัสที่ต้องเลือก
    อบกลม -> B0, อบผ่า -> A0, อื่น ๆ/ว่าง -> None (ไม่เจาะจง)
    """
    s = str(desc)
    if "กลม" in s:
        return "B0"
    if "ผ่า" in s:
        return "A0"
    return None


def _select_codes(codes: list, desc) -> list:
    """เลือกรหัสเต็มตาม TUBULAR_TYPE_DESC
    - ระบุ อบกลม/อบผ่า และมีรหัส suffix ตรง -> เลือกเฉพาะตัวนั้น
    - ระบุ แต่หา suffix ไม่เจอ / ไม่ได้ระบุ -> คืนทั้งหมด (fallback)
    """
    suf = _suffix_for_tubular(desc)
    if suf:
        sel = [c for c in codes if str(c).upper().endswith(suf)]
        if sel:
            return sel
    return codes


def _ora_map_to_lookup(ora_map: pd.DataFrame) -> dict:
    """ITEM -> list ของรหัสเต็ม (แยกจากสตริงที่ join ด้วย ', ')"""
    lookup = {}
    for item, codes in zip(ora_map["ITEM"], ora_map["ORA_ITEM_CODE"]):
        parts = [c.strip() for c in str(codes).split(",")]
        lookup[item] = [c for c in parts if c and c.lower() != "nan"]
    return lookup


def build_stock_balance() -> dict:
    """dict: ITEM_CODE (รหัสเต็ม A0/B0) -> รวม BALANCE_KG เฉพาะแถวที่ QA_REASON ว่าง
    (QA_REASON มีค่า = ของเสีย/ติด QA ไม่นับ)
    ถ้าไม่พบไฟล์ stock จะคืน dict ว่าง เพื่อให้ Map Item รันต่อได้
    """
    print(f"[INFO] โหลด stock: {STOCK_FILE}")
    if not os.path.exists(STOCK_FILE):
        print(f"[WARN] ไม่พบไฟล์ stock: {STOCK_FILE} — STOCK_BALANCE_KG = 0")
        return {}

    st = _read_excel_safe(STOCK_FILE, usecols=["ITEM_CODE", "BALANCE_KG", "QA_REASON"])
    st["ITEM_CODE"] = st["ITEM_CODE"].astype(str).str.strip()
    st["BALANCE_KG"] = pd.to_numeric(st["BALANCE_KG"], errors="coerce").fillna(0)

    qa = st["QA_REASON"].astype(str).str.strip()
    blank = st["QA_REASON"].isna() | qa.eq("") | qa.str.lower().eq("nan")
    good = st[blank]
    bal = good.groupby("ITEM_CODE")["BALANCE_KG"].sum()
    print(f"[INFO]   stock QA ว่าง: {len(good):,}/{len(st):,} แถว -> {len(bal):,} รหัส")
    return bal.to_dict()


def _distinct_join_cells(series) -> str:
    """รวมทุกค่าที่ไม่ซ้ำจากหลายเซลล์ (แต่ละเซลล์อาจมีหลายค่าคั่นด้วย ', ' อยู่แล้ว)
    -> สตริงเดียวคั่นด้วย ', ' เรียงลำดับ (ใช้กับ LOAD_DYE รวมข้ามสัปดาห์ต่อ item)
    """
    vals = []
    for cell in series:
        if pd.isna(cell):
            continue
        for part in str(cell).split(","):
            p = part.strip()
            if p and p.lower() != "nan" and p not in vals:
                vals.append(p)
    return ", ".join(sorted(vals))


def _sum_balance(ora_code, bal_map: dict) -> float:
    """รวม BALANCE_KG ของรหัสเต็มใน ORA_ITEM_CODE (รองรับหลายรหัสคั่นด้วย ', ')"""
    if bal_map is None or pd.isna(ora_code):
        return 0.0
    codes = [c.strip() for c in str(ora_code).split(",") if c.strip()]
    return float(sum(bal_map.get(c, 0.0) for c in codes))


def _fmt_val(v) -> str:
    """ค่าจาก Excel -> ข้อความ (เลขจำนวนเต็มไม่ให้ติด .0)"""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


def _join_distinct(vals, sep=" | ") -> str:
    """รวมค่าที่ไม่ซ้ำ (ไม่ split ค่าภายใน เพราะ YARN_ITEM มี ',' อยู่แล้ว)"""
    out = []
    for v in vals:
        s = _fmt_val(v)
        if s and s not in out:
            out.append(s)
    return sep.join(out)


def _group_fields(sel: list, spec_by_code: dict, group_codes: dict, bal_map: dict) -> dict:
    """ข้อมูลกลุ่มทดแทนของรหัสที่เลือกไว้ (sel)

    รหัสที่อยู่ SPEC_KEY เดียวกัน = spec เครื่อง/ด้ายเหมือนกัน -> ใช้แทนกันได้
      SUB_GROUP_ITEMS    = รหัส "อื่น" ในกลุ่มเดียวกัน (ไม่รวมของตัวเอง)
      SUB_GROUP_STOCK_KG = stock รวมทั้งกลุ่ม (รวมของตัวเอง)
    """
    empty = {c: "" for c in GROUP_OUT_COLS}
    empty["SUB_GROUP_COUNT"] = 0
    empty["SUB_GROUP_STOCK_KG"] = 0.0
    if not sel or not spec_by_code:
        return empty

    keys, all_codes = [], []
    attrs = {c: [] for c in V2_SPEC_COLS}
    for code in sel:
        rec = spec_by_code.get(code)
        if not rec:
            continue
        key = rec[COL_SPEC_KEY]
        if key not in keys:
            keys.append(key)
            all_codes.extend(group_codes.get(key, [code]))
        for c in V2_SPEC_COLS:
            attrs[c].append(rec.get(c))
    if not keys:
        return empty

    all_codes = list(dict.fromkeys(all_codes))
    own = set(sel)
    others = [c for c in all_codes if c not in own]

    out = {
        COL_SPEC_KEY: _join_distinct(keys),
        "SUB_GROUP_COUNT": len(others),
        "SUB_GROUP_ITEMS": ", ".join(others),
        "SUB_GROUP_STOCK_KG": float(sum((bal_map or {}).get(c, 0.0) for c in all_codes)),
    }
    out.update({c: _join_distinct(attrs[c]) for c in V2_SPEC_COLS})
    return out


def _add_group_cols(df: pd.DataFrame, selected, spec_by_code: dict,
                    group_codes: dict, bal_map: dict) -> pd.DataFrame:
    """เติมคอลัมน์กลุ่มทดแทนลง df ตามรหัสที่เลือกไว้ของแต่ละแถว"""
    fields = [_group_fields(list(cs), spec_by_code, group_codes, bal_map) for cs in selected]
    for col in GROUP_OUT_COLS:
        df[col] = [f[col] for f in fields]
    return df


def build_mapping(
    dm: pd.DataFrame = None, ora_map: pd.DataFrame = None, bal_map: dict = None
) -> pd.DataFrame:
    """datamining + ORA_ITEM_CODE (1 แถว/item-week เดิม)

    เลือกรหัสเต็มตาม TUBULAR_TYPE_DESC ของแต่ละแถว:
        อบกลม -> รหัสลงท้าย B0, อบผ่า -> รหัสลงท้าย A0
    """
    if dm is None:
        dm = load_datamining()
    if ora_map is None:
        ora_map = build_ora_map()
    if bal_map is None:
        bal_map = build_stock_balance()

    lookup = _ora_map_to_lookup(ora_map)
    merged = dm.copy()
    has_tub = "TUBULAR_TYPE_DESC" in merged.columns
    if not has_tub:
        print("[WARN] ไม่พบคอลัมน์ TUBULAR_TYPE_DESC ใน datamining "
              "— จะแสดงรหัสทั้งหมด (A0/B0) ตามเดิม (ลองรัน View_Datamining ใหม่)")

    n_round = n_split = n_fallback = 0

    def _pick(row) -> list:
        nonlocal n_round, n_split, n_fallback
        codes = lookup.get(row["ITEM"], [])
        desc = row["TUBULAR_TYPE_DESC"] if has_tub else None
        suf = _suffix_for_tubular(desc)
        sel = _select_codes(codes, desc)
        if suf == "B0":
            n_round += 1
        elif suf == "A0":
            n_split += 1
        if suf and codes and not any(str(c).upper().endswith(suf) for c in codes):
            n_fallback += 1
        return sel

    selected = merged.apply(_pick, axis=1)
    merged["ORA_ITEM_CODE"] = selected.apply(
        lambda cs: ", ".join(cs) if cs else pd.NA
    )
    merged["ORA_ITEM_COUNT"] = selected.apply(len)
    merged["STOCK_BALANCE_KG"] = merged["ORA_ITEM_CODE"].apply(
        lambda c: _sum_balance(c, bal_map)
    )

    # ข้อมูลกลุ่มทดแทนจากชีท Master_Item V2 (รหัสใน ITEM_LIST เดียวกัน = ใช้แทนกันได้)
    _, spec_by_code, group_codes = load_master_item()
    merged = _add_group_cols(merged, selected, spec_by_code, group_codes, bal_map)

    # LOAD_DYE ให้แสดงเฉพาะไฟล์ booking เท่านั้น — ตัดออกจาก datamining_mapped
    merged = merged.drop(columns=["LOAD_DYE"], errors="ignore")

    print(f"[INFO] merge ORA: {len(merged):,} แถว (เท่าเดิม — ไม่แตกแถว)")
    if has_tub:
        print(f"[INFO]   อบกลม (B0): {n_round:,} | อบผ่า (A0): {n_split:,} "
              f"| fallback (หา suffix ตรงไม่เจอ): {n_fallback:,}")
    _report_ora_quality(merged)

    n_sub = int((merged["SUB_GROUP_COUNT"] > 0).sum())
    if n_sub:
        extra = merged["SUB_GROUP_STOCK_KG"] - merged["STOCK_BALANCE_KG"]
        print(f"[INFO] แถวที่มีรหัสอื่นใช้แทนกันได้: {n_sub:,}/{len(merged):,} "
              f"| stock ที่เพิ่มมาจากกลุ่มทดแทน: {extra.clip(lower=0).sum():,.0f} kg")
    return merged


def build_booking_weekly() -> pd.DataFrame:
    """สรุป booking ต่อ (item, สัปดาห์)

    key: booking ITEM_CODE ตัด suffix "ตัวอักษร+เลข" ท้ายสุด (เช่น A0/B0) -> ITEM
    (รหัส collar/cuff เช่น CR.../CCV... ลงท้ายด้วยเลขล้วน จึงไม่ถูกตัด)
    คืน DataFrame: ITEM, FG_WEEK, BK_YEAR, BK_<col> (sum), BOOKING_ROWS
    ถ้าไม่พบไฟล์ booking จะคืนตารางว่าง เพื่อให้ Map Item รันต่อได้
    """
    print(f"[INFO] โหลด booking: {BOOKING_FILE}")
    if not os.path.exists(BOOKING_FILE):
        print(f"[WARN] ไม่พบไฟล์ booking: {BOOKING_FILE} — จะสร้างผลลัพธ์แบบไม่มี booking ให้")
        cols = ["ITEM", "FG_WEEK", "BK_YEAR", "BOOKING_ROWS", *[f"BK_{c}" for c in BOOKING_SUM_COLS]]
        return pd.DataFrame(columns=cols)

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


def _select_items_ora(
    dm: pd.DataFrame, ora_map: pd.DataFrame, bal_map: dict = None
) -> pd.DataFrame:
    """คืน DataFrame ราย ITEM (unique) พร้อมรหัสเต็มที่เลือกตาม TUBULAR_TYPE_DESC
    คอลัมน์: ITEM, TUBULAR_TYPE_DESC, ORA_ITEM_CODE, ORA_ITEM_COUNT, TOTAL_QTY, STOCK_BALANCE_KG
    (แต่ละ ITEM มี TUBULAR_TYPE_DESC ค่าเดียว จึงใช้ค่าแรกได้)
    """
    lookup = _ora_map_to_lookup(ora_map)
    has_tub = "TUBULAR_TYPE_DESC" in dm.columns
    tub = dm.groupby("ITEM")["TUBULAR_TYPE_DESC"].first() if has_tub else None
    has_qty = "TOTAL_QTY" in dm.columns
    qty = (
        dm.assign(TOTAL_QTY=pd.to_numeric(dm["TOTAL_QTY"], errors="coerce"))
        .groupby("ITEM")["TOTAL_QTY"].sum()
        if has_qty else None
    )
    has_dye = "LOAD_DYE" in dm.columns
    dye = dm.groupby("ITEM")["LOAD_DYE"].agg(_distinct_join_cells) if has_dye else None

    rows, sels = [], []
    for item in dm["ITEM"].drop_duplicates():
        codes = lookup.get(item, [])
        desc = tub.get(item) if has_tub else None
        total_qty = qty.get(item) if has_qty else pd.NA
        load_dye = dye.get(item) if has_dye else ""
        sel = _select_codes(codes, desc)
        ora_code = ", ".join(sel) if sel else pd.NA
        balance = _sum_balance(ora_code, bal_map)
        rows.append((item, desc, ora_code, len(sel), total_qty, balance, load_dye))
        sels.append(sel)
    out = pd.DataFrame(
        rows,
        columns=[
            "ITEM", "TUBULAR_TYPE_DESC", "ORA_ITEM_CODE", "ORA_ITEM_COUNT",
            "TOTAL_QTY", "STOCK_BALANCE_KG", "LOAD_DYE",
        ],
    )
    _, spec_by_code, group_codes = load_master_item()
    return _add_group_cols(out, sels, spec_by_code, group_codes, bal_map)


def build_booking_file(
    dm: pd.DataFrame, ora_map: pd.DataFrame, weekly: pd.DataFrame, bal_map: dict = None
) -> pd.DataFrame:
    """ดูว่า item ที่ map มาแต่ละตัว -> booking มีแผนในสัปดาห์ไหนบ้าง

    เริ่มจากรายการ item ที่ map ได้ (unique จาก datamining + ORA_ITEM_CODE
    ที่เลือกตาม TUBULAR_TYPE_DESC: อบกลม->B0, อบผ่า->A0)
    แล้ว left join กับ booking รายสัปดาห์:
      - item ที่มี booking  -> 1 แถวต่อ 1 สัปดาห์ที่ booking มีแผน
      - item ที่ไม่มี booking -> 1 แถว (FG_WEEK ว่าง, HAS_BOOKING = N)
    """
    items = _select_items_ora(dm, ora_map, bal_map)

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
        bal_map = build_stock_balance()

        # ---- ไฟล์ 1: datamining + ORA_ITEM_CODE ----
        df = build_mapping(dm, ora_map, bal_map)
        save_excel(df, OUTPUT_FILE)

        # ---- ไฟล์ 2: datamining + booking รายสัปดาห์ -> ไฟล์ใหม่แยก ----
        weekly = build_booking_weekly()
        df_bk = build_booking_file(dm, ora_map, weekly, bal_map)
        save_excel(df_bk, OUTPUT_BOOKING_FILE, sheet="datamining_booking")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1
    print(f"[DONE] ใช้เวลา {datetime.now() - start}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""ColorOpen.py — แหล่งข้อมูล "ยอดที่เปิดสีแล้ว" จุดเดียวของทั้งระบบ

ที่มา: view ยอดเปิดสีของใบ SC (ดึงด้วย View_SCFol_Item.py → Order/view_scfol_item.xlsx)

กติกา (user ยืนยัน 27 ส.ค. 2569):
    ดูที่คีย์ SC_NO + ITEM_CODE
      OPEN_COLOR_LOSS = ยอดที่ "เปิดสีแล้ว" (กก.)  ← โควตางานสี
      KNIT_KGS        = ยอดทอทั้งใบสั่งของ item นั้น
    เปิดสี 4,158 จากยอดทอ 11,000 → งานสี 4,158 กก. ที่เหลือ 6,842 เป็นงานยังไม่ให้สี
    (ถูกถอดเครื่องให้ item สีตัวอื่นได้)

⚠️ นี่คือ "แหล่งเดียว" — เลิกใช้วิธีเดิมทั้งหมดแล้ว (มี W## ใน LOAD_DYE / คอลัมน์
   NAY_COLOR / COLOR_DESC / SUB_COLOR ในออร์เดอร์ / สีในชีท DETAIL ของ booking)
   วิธีเดิมบอกได้แค่ "มี/ไม่มีสี" ระดับ item และตีความ "สีที่ลูกค้าสั่ง" เป็น "สีที่เปิดแล้ว"
   ซึ่งทำให้ item ถูกนับเป็นงานสีเกินจริง

กับดักที่ต้องระวัง:
  • SC_NO ใน view มี "S" นำหน้า ("S714546") แต่ SC_SO_NO ฝั่ง booking/แผนเป็นเลขล้วน
    ("714546") → ต้อง normalize ก่อน join เสมอ (ใช้ norm_sc())
  • SC+ITEM ซ้ำได้ (แตกบรรทัดย่อยในใบสั่งเดียว) → ต้อง sum ไม่ใช่หยิบแถวแรก
  • QN_TYPE_SUB = Dye / DO คือ งานย้อม/ส่งของ ที่ KNIT_KGS = 0 ไม่ใช่งานทอ → ตัดทิ้ง
"""

from pathlib import Path

import pandas as pd

_BASE_DIR = Path(__file__).parent

# ไฟล์ที่ View_SCFol_Item.py เขียนไว้
DEFAULT_PATH = _BASE_DIR / "Order" / "view_scfol_item.xlsx"

# ประเภทใบสั่งที่ "มีการทอ" เท่านั้น — Dye / DO เป็นงานย้อม/ส่งของ (KNIT_KGS = 0)
KNIT_TYPES = ("KNIT-DYE", "DYE-KNIT", "KNIT")

_cache: dict = {}          # {str(path): (mtime, data)}


def so_text(v) -> str:
    """เลข SC/SO เป็น "ข้อความ" ที่เอาไปเขียนลงชีทได้ — คง S นำหน้าไว้

    ⚠️ กับดักที่ทำให้ ".0" หลุดเข้าชีทแผน: คอลัมน์ SO_NO เป็นเลขล้วน pandas จึงอ่านเป็น
    ตัวเลข และถ้ามีแถวไหนว่างแม้แถวเดียว (order_ready จริงมีใบ SC ที่ยังไม่มี SO เกิน
    6,000 แถว) ทั้งคอลัมน์กลายเป็น float64 → str() ได้ "721915.0" / "nan"
    ค่าพวกนี้ทำให้ join กับ booking/งานสี/ล็อกแผน พลาดเงียบ ๆ ไม่ใช่แค่ดูไม่สวย

    ใช้กับคอลัมน์ที่ต้องคง S ("S714546" ในชีท NO_CAP / SC/SO NO) ; ถ้าต้องการเลขล้วน
    เป็นมาตรฐานกลางให้ใช้ norm_sc()"""
    s = str(v if v is not None else "").strip().upper()
    if s in ("", "NAN", "NONE", "NAT", "<NA>"):
        return ""
    # แถวที่ยุบหลายใบไว้ในช่องเดียว ("708936.0,719941.0") ต้องล้างทีละใบ
    if "," in s:
        return ",".join(x for x in (so_text(p) for p in s.split(",")) if x)
    if s.endswith(".0") and s[:-2].lstrip("S").isdigit():
        s = s[:-2]
    return s


def norm_sc(v) -> str:
    """SC ให้อยู่รูปเดียวกันทุกฝั่ง — เลขล้วน ไม่มี S นำหน้า

    view = "S714546" / order_ready.SC_NO = "S714546" / SO_NO = "720398" (เลขล้วน)
    แผน (SC_SO_NO) ใช้เลขล้วน → ตัด S ทิ้งเป็นมาตรฐานกลาง"""
    return so_text(v).lstrip("S")


def split_sc(v) -> list:
    """แตกช่อง SC ที่ยุบหลายใบไว้ → list ของ SC normalize แล้ว

    ⚠️ กับดักที่ทำให้ join พลาดเงียบ ๆ: SC_SO_NO ของแถวที่มาจาก booking ยุบหลายใบสั่ง
    ไว้ในช่องเดียว ("708936,719941" / "6695107100,6695107099") — ถ้าไม่แตกก่อน
    จะหาไม่เจอใน view แล้วถูกตัดสินว่า "ไม่ใช่งานสี" ทั้งที่เปิดสีแล้ว
    (วัดจริงบน replan_result: แตกคอมมาแล้วกู้ได้ 151 จาก 154 แถวที่หลุด)"""
    out, seen = [], set()
    for part in str(v or "").replace("|", ",").replace(";", ",").split(","):
        s = norm_sc(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def norm_item(v) -> str:
    s = str(v or "").strip().upper()
    return "" if s in ("NAN", "NONE", "NAT") else s


def load(path=None) -> dict:
    """อ่านไฟล์ view → {(sc, item): {"open": กก.เปิดสีแล้ว, "knit": กก.ยอดทอ}}

    ไม่มีไฟล์ / อ่านไม่ได้ → {} (ผู้เรียกตัดสินเองว่าจะถือว่าไม่มีงานสี)
    cache ตาม mtime — เรียกซ้ำในรอบเดียวกันไม่อ่าน Excel ใหม่"""
    p = Path(path) if path else DEFAULT_PATH
    key = str(p)
    try:
        mtime = p.stat().st_mtime
    except OSError:
        _cache.pop(key, None)
        return {}
    hit = _cache.get(key)
    if hit and hit[0] == mtime:
        return hit[1]

    try:
        df = pd.read_excel(p)
    except Exception:
        return {}
    df.columns = [str(c).strip().upper() for c in df.columns]
    need = {"SC_NO", "ITEM_CODE", "OPEN_COLOR_LOSS", "KNIT_KGS"}
    if not need <= set(df.columns):
        return {}

    if "QN_TYPE_SUB" in df.columns:
        _t = df["QN_TYPE_SUB"].astype(str).str.strip().str.upper()
        df = df[_t.isin(KNIT_TYPES)]

    out: dict = {}
    _open = pd.to_numeric(df["OPEN_COLOR_LOSS"], errors="coerce").fillna(0.0)
    _knit = pd.to_numeric(df["KNIT_KGS"], errors="coerce").fillna(0.0)
    for _sc, _it, _o, _k in zip(df["SC_NO"], df["ITEM_CODE"], _open, _knit):
        sc, it = norm_sc(_sc), norm_item(_it)
        if not sc or not it:
            continue
        rec = out.setdefault((sc, it), {"open": 0.0, "knit": 0.0})
        rec["open"] += float(_o)      # SC+ITEM ซ้ำ = บรรทัดย่อยในใบเดียวกัน → บวกรวม
        rec["knit"] += float(_k)

    _cache[key] = (mtime, out)
    return out


def quota_by_sc_item(path=None) -> dict:
    """{(sc, item): กก.ที่เปิดสีแล้ว} — เฉพาะที่ > 0 (คือ "งานสี" ตามนิยามใหม่)"""
    return {k: v["open"] for k, v in load(path).items() if v["open"] > 0}


def quota_by_item(path=None) -> dict:
    """{item: กก.ที่เปิดสีแล้วรวมทุก SC} — ใช้ตอนไม่มี SC ให้จับคู่ (เช่น Gantt ที่คีย์ด้วย item)"""
    out: dict = {}
    for (_sc, it), v in load(path).items():
        if v["open"] > 0:
            out[it] = out.get(it, 0.0) + v["open"]
    return out


def color_item_codes(path=None) -> set:
    """เซ็ตรหัส item ที่มีงานสีอยู่บ้าง (OPEN_COLOR_LOSS > 0 อย่างน้อย 1 ใบสั่ง)"""
    return set(quota_by_item(path).keys())


def source_info(path=None) -> dict:
    """ชื่อไฟล์ + เวลาแก้ไขล่าสุด — ให้หน้าเว็บบอก user ได้ว่าใช้ข้อมูลรอบไหน"""
    p = Path(path) if path else DEFAULT_PATH
    try:
        return {"name": p.name, "mtime": p.stat().st_mtime}
    except OSError:
        return {"name": None, "mtime": None}


if __name__ == "__main__":
    d = load()
    if not d:
        print(f"❌ ไม่พบ/อ่านไม่ได้: {DEFAULT_PATH}")
        raise SystemExit(1)
    color = {k: v for k, v in d.items() if v["open"] > 0}
    print(f"ไฟล์      : {DEFAULT_PATH}")
    print(f"คีย์ทั้งหมด : {len(d):,} (SC + ITEM, เฉพาะใบสั่งที่มีการทอ)")
    print(f"เป็นงานสี  : {len(color):,} คีย์ / {len(color_item_codes()):,} รหัส item")
    print(f"ยอดเปิดสีรวม: {sum(v['open'] for v in color.values()):,.0f} กก. "
          f"จากยอดทอ {sum(v['knit'] for v in color.values()):,.0f} กก.")
    print("\nตัวอย่าง 10 รายการ (เปิดสีไม่เต็มยอดทอ):")
    part = [(k, v) for k, v in color.items() if 0 < v["open"] < v["knit"]]
    for (sc, it), v in sorted(part, key=lambda x: -x[1]["knit"])[:10]:
        print(f"  SC {sc:<10} {it:<26} เปิดสี {v['open']:>10,.0f} / ทอ {v['knit']:>10,.0f} กก.")

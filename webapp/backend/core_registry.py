"""
core_registry.py — ทะเบียน Core Greige จุดเดียวของทั้งเว็บ (อ่าน Target_Stock.xlsx)

**นิยาม core = ผ้า + ลูกค้า + ทีม ครบทั้งสามอย่าง**
รหัสผ้าเดียวกันที่ผูกกับลูกค้าอื่น หรืออยู่คนละทีม = คนละก้อน ไม่ใช่ core ของแถวนั้น
(ทะเบียนตั้ง TARGET/MONTH แยกรายแถว เช่น FD5PRTJZ63A0 → RTS/ที คัลเจอร์ 24,000
 กับ NYK1-1/CENTER DOMESTIC 5,000 — คนละเป้า จึงใช้สต๊อกร่วมกันไม่ได้)

ทุกหน้าที่ตัดสินว่า "อันนี้ core ไหม" ต้องเรียก is_core() ตัวนี้ตัวเดียว ไม่งั้นแต่ละหน้า
จะตอบไม่ตรงกันเมื่อทะเบียนเปลี่ยน — เดิมแต่ละหน้าดูแค่รหัสผ้า ทำให้ของ
ลูกค้าอื่นที่บังเอิญเป็นผ้ารหัสเดียวกันถูกนับเป็น core ไปด้วย

⚠ ข้อมูลบางแหล่งไม่มีครบสามช่อง (เช่นชีท DETAIL ของ booking เก่าไม่มีคอลัมน์ลูกค้า,
order_ready ไม่มีคอลัมน์ทีมเลย) → is_core() จะ "ผ่อน" ลงตามข้อมูลที่มี:
ครบสาม → เทียบสาม ; มีแค่ลูกค้า → เทียบผ้า+ลูกค้า ; มีแค่ทีม → เทียบผ้า+ทีม ; ไม่มีเลย → เทียบผ้า
เข้มขึ้นเมื่อข้อมูลพร้อม แต่ไม่ทำให้หน้าที่ข้อมูลไม่ครบพังหรือกลายเป็นไม่ core ทั้งกระดาน

ไฟล์ Target_Stock ของจริงเก็บ STOCK MIN/MAX เป็นสูตร Excel → pandas อ่านได้ NaN
จึงคำนวณคืนด้วย masters._recompute_target_stock (สูตรชุดเดียวกับตอนบันทึกผ่านหน้าเว็บ)
"""
from pathlib import Path

import config
import masters

# ทะเบียนสำรองของเครื่อง dev (บน server มีแต่ Target_Stock ที่ mount มาที่ /data/knitplan/masters)
_ITEMCORE_FILE = config.REPO_DIR / "data" / "Itemcore" / "Itemcore.xlsx"

_cache: dict | None = None      # {"mtime":…, "pairs":…, "by_item":…, "meta":…, index…}


def ckey(v) -> str:
    """ชื่อลูกค้า → คีย์เทียบ (ยุบช่องว่างซ้ำ + ตัวพิมพ์ใหญ่)

    ชื่อไทยในไฟล์พิมพ์เว้นวรรคไม่เท่ากันได้ เช่น "นันยางการ์เม้นท์··บจก." (เว้น 2 ช่อง)
    เทียบดิบ ๆ จะกลายเป็นคนละลูกค้าแล้วสต๊อกหลุดจากเป้าไปเงียบ ๆ
    """
    return " ".join(str(v or "").split()).upper()


tkey = ckey                      # ทีมใช้กติกา normalize เดียวกัน


def _s(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "nat", "none") else s


def _f(v) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if f != f else f       # NaN → 0


def path() -> Path | None:
    """ไฟล์ทะเบียนที่ต้องเป็นใบเดียวกับที่ Planning ใช้ (config.ini → paths.target_stock)"""
    try:
        p = config.master_files().get("Target_Stock")
        if p and Path(p).exists():
            return Path(p)
    except Exception:
        pass
    return _ITEMCORE_FILE if _ITEMCORE_FILE.exists() else None


def _blank(item, cus, team) -> dict:
    return {"item": item, "cus": cus, "team": team,
            "teams": [], "customers": [], "type1": "",
            "target_month": 0.0, "target_scm": 0.0,
            "stock_min": 0.0, "stock_max": 0.0, "stock5": 0.0}


def load() -> dict:
    """อ่านทะเบียน (แคชตาม mtime ของไฟล์ — แก้ผ่านหน้า Master Data แล้วเห็นผลทันที)

    คืน dict:
      pairs   {(ITEM, ckey(ลูกค้า), tkey(ทีม)): เป้าของแถวนั้น}
      by_item {ITEM: เป้ารวมทุกแถวของผ้านั้น}   ← ใช้ตอนมองคลังเป็นก้อนเดียว
      meta    {file, mtime}
      ดัชนีสำหรับ is_core(): full / item_cus / item_team / items
    """
    global _cache
    p = path()
    mtime = None
    if p is not None:
        try:
            mtime = p.stat().st_mtime
        except OSError:
            mtime = None
    if _cache is not None and _cache["mtime"] == mtime and _cache["meta"]["file"] == (p.name if p else None):
        return _cache

    meta = {"file": p.name if p else None, "mtime": mtime}
    empty = {"mtime": mtime, "meta": meta, "pairs": {}, "by_item": {},
             "full": set(), "item_cus": set(), "item_team": set(), "items": set()}
    if p is None:
        _cache = empty
        return _cache

    import pandas as pd
    try:
        df = pd.read_excel(p)
    except Exception:
        _cache = empty
        return _cache

    cols = [str(c).strip() for c in df.columns]
    idx = {c.upper(): i for i, c in enumerate(cols)}
    # ⚠ เทียบ None ตรง ๆ — คอลัมน์ Item code เป็นคอลัมน์แรก (index 0) ซึ่ง falsy
    i_item = next((idx[k] for k in ("ITEM CODE", "ITEM_CODE", "ITEMCODE") if k in idx), None)
    if i_item is None:
        _cache = empty
        return _cache

    raw = df.values.tolist()
    try:
        calc = masters._recompute_target_stock(cols, raw)
    except Exception:
        calc = raw

    i_team, i_cus = idx.get("TEAM NAME"), idx.get("CUSTOMER")
    i_t1, i_tm = idx.get("TYPE 1"), idx.get("TARGET/MONTH")
    i_min, i_max = idx.get("STOCK MIN"), idx.get("STOCK MAX")
    i_scm, i_w5 = idx.get("TARGET SCM"), idx.get("STOCK 5 WEEK")

    def pick(rrow, crow, i) -> float:
        """ค่าจากไฟล์ก่อน (user อาจแก้มือ) ไม่มี/ว่างค่อยใช้ค่าที่คำนวณคืนจาก TARGET/MONTH"""
        if i is None:
            return 0.0
        v = _f(rrow[i]) if i < len(rrow) else 0.0
        return v if v else (_f(crow[i]) if i < len(crow) else 0.0)

    pairs: dict = {}
    by_item: dict = {}
    for rrow, crow in zip(raw, calc):
        item = _s(rrow[i_item]).upper()
        if not item:
            continue
        team = _s(rrow[i_team]) if i_team is not None else ""
        cus = _s(rrow[i_cus]) if i_cus is not None else ""
        t1 = _s(rrow[i_t1]) if i_t1 is not None else ""
        vals = {"target_month": pick(rrow, crow, i_tm), "target_scm": pick(rrow, crow, i_scm),
                "stock_min": pick(rrow, crow, i_min), "stock_max": pick(rrow, crow, i_max),
                "stock5": pick(rrow, crow, i_w5)}

        # ทะเบียนซ้ำ (ผ้า+ลูกค้า+ทีม ชุดเดียวกันหลายแถว) = เป้าของชุดนั้นบวกกัน
        k = (item, ckey(cus), tkey(team))
        r = pairs.get(k) or pairs.setdefault(k, _blank(item, cus, team))
        a = by_item.get(item) or by_item.setdefault(item, _blank(item, "", ""))
        for tgt in (r, a):
            if cus and cus not in tgt["customers"]:
                tgt["customers"].append(cus)
            if not tgt["type1"]:
                tgt["type1"] = t1
            for kk, v in vals.items():
                tgt[kk] += v
            if team and team not in tgt["teams"]:
                tgt["teams"].append(team)

    _cache = {
        "mtime": mtime, "meta": meta, "pairs": pairs, "by_item": by_item,
        "full": set(pairs),
        "item_cus": {(i, c) for i, c, _t in pairs},
        "item_team": {(i, t) for i, _c, t in pairs},
        "items": {i for i, _c, _t in pairs},
    }
    return _cache


def items() -> set:
    """ITEM_CODE ทั้งหมดในทะเบียน (ใช้ตอนที่ข้อมูลมีแค่รหัสผ้า — หลวมที่สุด)"""
    return load()["items"]


def pairs() -> dict:
    """{(ITEM, ลูกค้า, ทีม): เป้า} — หน่วยจริงของ core"""
    return load()["pairs"]


def is_core(item, cus="", team="") -> bool:
    """แถวนี้เป็น Core Greige ไหม — เทียบเข้มเท่าที่ข้อมูลที่ส่งมามี (ดูหมายเหตุหัวไฟล์)"""
    it = str(item or "").strip().upper()
    if not it:
        return False
    d = load()
    c, t = ckey(cus), tkey(team)
    if c and t:
        return (it, c, t) in d["full"]
    if c:
        return (it, c) in d["item_cus"]
    if t:
        return (it, t) in d["item_team"]
    return it in d["items"]

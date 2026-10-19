"""
core_stock_live.py — Dashboard › Item Core Stock : ผ้าดิบตัวไหนมีสต๊อกเท่าไหร่ (สดจากไฟล์ Stock)

2 โหมด (พารามิเตอร์ mode):
  core = ทะเบียน Core Greige (Target_Stock.xlsx ใบเดียวกับที่ Planning ใช้)
         **1 แถว = 1 item × ทีม** — แต่ละทีมมีสต๊อกและเป้า (TARGET/MONTH) ของตัวเอง
         ไม่ใช้สต๊อกร่วมกัน รหัสเดียวกันคนละทีมจึงเป็นคนละแถว คนละสถานะ
         → รวมแถวที่ "ไม่มีของเหลือเลย" ด้วย เพราะนั่นคือตัวที่ต้องรีบผลิตที่สุด
         → ทีมที่ถือของ core ไว้แต่ไม่มีชื่อในทะเบียน ขึ้นเป็นแถวสถานะ "ไม่มีเป้า"
  all  = 1 แถว = 1 item รวมทุกทีม (มองคลังทั้งโรงงานเป็นก้อนเดียว)
         ทุก item ที่มีของอยู่ในคลังตามไฟล์ Stock (นอกทะเบียนไม่มี MIN/MAX → สถานะ "ไม่มีเป้า")

โหมด core ส่ง `totals` มาด้วย = แถวสรุป "รวมทุกทีม core" ของผ้าที่ตั้งเป้าไว้หลายทีม
(ดู _cross_totals) หน้าเว็บแทรกต่อท้ายแถวรายทีมของผ้านั้น — ทีมหนึ่งขาดแต่อีกทีมมีเหลือ
= โยกของข้ามทีมแทนสั่งทอใหม่ได้ ซึ่งแถวรายทีมล้วน ๆ มองไม่เห็น

ตัวเลขทุกช่องยึดกติกาเดียวกับ pipeline ไม่งั้นหน้าเว็บจะเถียงกับแผนที่ Planning วางไว้:
  สต๊อกที่ใช้ได้ = BALANCE_KG ของล็อตที่ QA ว่าง และ ≥ 100 kg     (Stock.py read_stock_data)
                   BALANCE_KG = ของในคลังหักยอดจอง (RESERVE_KG) แล้ว
  Inventory      = สต๊อกที่ใช้ได้ + งานที่จะทอเข้าอีก 2 สัปดาห์    (Stock.py add_outstanding_from_booking)
  cover (สัปดาห์) = Inventory / STOCK MIN — STOCK MIN คือยอดใช้ต่อสัปดาห์ (= TARGET/MONTH ÷ 4)
  cover < 2      = ถึงจุดสั่งผลิต ตรงกับ trigger ผลิต core ใน Planning.py (_cover < 2)

ไฟล์ Target_Stock ของจริงเก็บ STOCK MIN/MAX เป็น "สูตร Excel" → pandas อ่านได้ NaN
จึงคำนวณคืนด้วย masters._recompute_target_stock (สูตรชุดเดียวกับตอนบันทึกผ่านหน้าเว็บ)
"""
from pathlib import Path

import config
import core_registry
import plan_view

_STOCK_FILE = config.REPO_DIR / "Stock" / "view_stock.xlsx"
_STOCK_SHEET = 0   # ชีทแรก — ไฟล์มีชีทเดียว (ชื่อชีท = ชื่อ view ไม่อ้างในโค้ด)
# ทะเบียน core สำรองของเครื่อง dev (บน server มีแต่ Target_Stock ที่ mount มา)
_ITEMCORE_FILE = config.REPO_DIR / "data" / "Itemcore" / "Itemcore.xlsx"

# ล็อตเล็กกว่านี้ Stock.py ตัดทิ้งก่อนรวมยอด (เศษผ้าที่ขายไม่ได้) — แยกโชว์เป็นตัวเลขของมันเอง
MIN_LOT_KG = 100
# เกณฑ์สถานะ (สัปดาห์ที่สต๊อกพอใช้) — 2 = trigger ผลิตของ Planning, 4 = เริ่มเฝ้าระวัง
COVER_CRIT = 2.0
COVER_WARN = 4.0
# จำนวนสัปดาห์ข้างหน้าที่ดึงยอด "งานที่จะทอเข้าสต๊อก" มาแสดง (2 สัปดาห์แรกเข้าสูตร Inventory)
INCOMING_WEEKS = 8
# เพดานแถวที่ส่งขึ้นหน้าเว็บ (โหมด all มี ~1,700 item — หน้าเว็บคิด KPI เองจากแถวที่กรอง จึงต้องส่งให้ครบ)
MAX_ROWS = 4000
# W99 = sentinel "งานล้น" ของ AVA_MC ไม่ใช่สัปดาห์จริง (AVA_MC ตัดออกจาก timeline เช่นกัน)
_SENTINEL_WEEK = 99

_cache: dict = {}


# ---------------------------------------------------------------- helpers
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


def _mtime(p: Path):
    try:
        return p.stat().st_mtime
    except OSError:
        return None


# ---------------------------------------------------------------- ทะเบียน core
# ย้ายไปอยู่ core_registry.py แล้ว — ทุกหน้าที่ตัดสิน "core ไหม" ต้องใช้ทะเบียนใบเดียวกัน
_ckey, _tkey = core_registry.ckey, core_registry.tkey


# ---------------------------------------------------------------- สต๊อกในคลัง
def _stock() -> tuple[dict, dict]:
    """(สต๊อกต่อ ITEM_CODE แยกราย (ลูกค้า, ทีม), meta ของไฟล์)

    ยอดทุกก้อนนับจาก BALANCE_KG = ของที่เหลือหลังหักยอดจอง (RESERVE_KG) แล้ว
    ไม่ใช่ ONHAND_KG ที่เป็นของในคลังก่อนหักจอง — ของที่ถูกจองไปแล้วเอามาวางแผนซ้ำไม่ได้

    แยกยอดเป็น 3 ก้อนตามที่ pipeline ตัดสิน เพื่อให้เห็นว่าทำไมยอดบนหน้าเว็บไม่เท่ายอดรวมดิบ:
      onhand  = ล็อต QA ว่าง และ ≥ 100 kg  → ก้อนที่ Planning เอาไปคิด trigger
      small   = ล็อต QA ว่าง แต่ < 100 kg   → เศษ, Stock.py ตัดทิ้ง
      blocked = ล็อตที่ติด QA               → ของมีอยู่แต่ใช้ไม่ได้

    เก็บแยกราย (ลูกค้า, ทีม) ของล็อต เพราะ core ตัดสินด้วย item + ลูกค้า + ทีม พร้อมกัน
    ผ้ารหัสเดียวกันที่ผูกกับลูกค้าอื่นหรืออยู่คนละทีม = ของก้อนอื่น ไม่ใช่สต๊อก core ของแถวนั้น
    """
    meta = {"file": _STOCK_FILE.name, "mtime": _mtime(_STOCK_FILE)}
    if not _STOCK_FILE.exists():
        return {}, meta

    import pandas as pd
    try:
        df = pd.read_excel(_STOCK_FILE, sheet_name=_STOCK_SHEET)
    except Exception:
        try:
            df = pd.read_excel(_STOCK_FILE)          # ชีทแรกก็ยังดีกว่าไม่มีข้อมูล
        except Exception:
            return {}, meta

    cols = set(df.columns)
    if "ITEM_CODE" not in cols:
        return {}, meta

    def g(r, name):
        return r.get(name) if name in cols else None

    out: dict = {}
    for r in df.to_dict("records"):
        item = _s(g(r, "ITEM_CODE")).upper()
        if not item:
            continue
        rec = out.get(item)
        if rec is None:
            rec = out[item] = {"by_key": {}, "family": "", "structure": "", "yarn": "",
                               "gm2": "", "mc_group": "", "gauge": "", "customer": ""}
        team = _s(g(r, "TEAM_NAME")) or "-"
        cus = _s(g(r, "CUSTOMER_NAME")) or "-"
        k = (_ckey(cus), _tkey(team))
        t = rec["by_key"].get(k)
        if t is None:
            t = rec["by_key"][k] = {"cus": cus, "team": team, "onhand": 0.0, "small": 0.0,
                                    "blocked": 0.0, "reserved": 0.0,
                                    "lots": 0, "aging_max": 0.0, "aging_wsum": 0.0}
        # จองเกินของที่มี → BALANCE_KG ติดลบ ปัดเป็น 0 ไม่งั้นไปหักยอดล็อตอื่นในก้อนเดียวกัน
        kg = max(_f(g(r, "BALANCE_KG")), 0.0)
        qa = _s(g(r, "QA_REASON")) or _s(g(r, "QA_REMARK"))
        if qa:
            t["blocked"] += kg
            continue
        if kg < MIN_LOT_KG:
            t["small"] += kg
            continue

        t["onhand"] += kg
        t["lots"] += 1
        t["reserved"] += _f(g(r, "RESERVE_KG"))
        aging = _f(g(r, "AGING_WW"))     # หน่วยเป็น "สัปดาห์" (ตรงกับ KNIT_WW/KNIT_YEAR ของล็อต)
        t["aging_max"] = max(t["aging_max"], aging)
        t["aging_wsum"] += aging * kg
        # สเปกผ้าเอาจากล็อตแรกที่เจอ (ล็อตของ item เดียวกันสเปกเหมือนกัน)
        for key, col in (("family", "ITEM_FAMILY"), ("structure", "ITEM_STRUCTURE"),
                         ("yarn", "MATERIAL_YARN"), ("gm2", "GM2"),
                         ("mc_group", "MC_GROUP"), ("gauge", "GAUGE"),
                         ("customer", "CUSTOMER_NAME")):
            if not rec[key]:
                rec[key] = _s(g(r, col))
    return out, meta


def _combine(rec: dict, keys: set | None) -> dict:
    """รวมยอดของ (ลูกค้า, ทีม) ที่ระบุเป็นก้อนเดียว (keys=None = ทั้งคลัง)

    โหมด core ส่งมาชุดเดียว = ของ item + ลูกค้า + ทีม นั้นเท่านั้น ไม่ปนก้อนอื่น
    โหมด all ส่ง None = มองคลังทั้งโรงงานเป็นก้อนเดียว
    """
    tot = {"onhand": 0.0, "small": 0.0, "blocked": 0.0, "reserved": 0.0,
           "lots": 0, "aging_max": 0.0, "aging_avg": 0, "_w": 0.0,
           "teams": [], "_tmap": {}}
    if not rec:
        tot.pop("_w"); tot.pop("_tmap")
        return tot
    for k, t in rec["by_key"].items():
        if keys is not None and k not in keys:
            continue
        kg_t = t["onhand"]
        if kg_t > 0:
            tot["_tmap"][t["team"]] = tot["_tmap"].get(t["team"], 0.0) + kg_t
        tot["onhand"] += t["onhand"]
        tot["small"] += t["small"]
        tot["blocked"] += t["blocked"]
        tot["reserved"] += t["reserved"]
        tot["lots"] += t["lots"]
        tot["aging_max"] = max(tot["aging_max"], t["aging_max"])
        tot["_w"] += t["aging_wsum"]
    tot["teams"] = [{"team": t, "kg": round(k, 1)}
                    for t, k in tot.pop("_tmap").items() if k > 0]
    tot["teams"].sort(key=lambda x: -x["kg"])
    tot["aging_avg"] = round(tot.pop("_w") / tot["onhand"]) if tot["onhand"] else 0
    return tot


# ---------------------------------------------------------------- งานที่จะทอเข้าสต๊อก
def _upcoming_weeks(n: int) -> list:
    """[(ปี, สัปดาห์), ...] n สัปดาห์ข้างหน้า (เริ่มสัปดาห์ถัดจากสัปดาห์ปัจจุบัน) จากปฏิทินจริง

    ต้องอ่านจากปฏิทินไม่ใช่บวกเลขเอา เพราะแผนคร่อมปีได้ (W52 → W1 ปีหน้า)
    """
    from datetime import date
    weekly = plan_view._calendar_weekly()
    if weekly is None or len(weekly) == 0:
        return []
    cols = {str(c).strip().upper(): c for c in weekly.columns}
    yc, wc, sc = cols.get("YEAR"), cols.get("WEEK"), cols.get("WEEK_START")
    if yc is None or wc is None or sc is None:
        return []

    today, idx = date.today(), 0
    for i in range(len(weekly)):
        ws = weekly.iloc[i][sc]
        try:
            d = ws.date() if hasattr(ws, "date") else ws
        except Exception:
            continue
        if d is None or d > today:
            break
        idx = i

    out = []
    for i in range(idx + 1, min(len(weekly), idx + 1 + n)):
        r = weekly.iloc[i]
        try:
            out.append((int(r[yc]), int(r[wc])))
        except (TypeError, ValueError):
            continue
    return out


def _incoming(weeks: list) -> tuple[dict, dict, dict]:
    """({(ITEM, ลูกค้า, ทีม): {(ปี,สัปดาห์): kg}}, {ITEM: {(ปี,สัปดาห์): kg}}, meta ไฟล์ booking)

    ยอดที่จองเครื่องไว้แล้ว = ผ้าที่กำลังจะทอเสร็จเข้าสต๊อก (KP_WEIGHT)

    แยกราย (ลูกค้า, ทีม) ให้ตรงกับหน่วยของโหมด core:
      ใช้ชีท DETAIL_SC ก่อน (1 แถว = 1 ใบ → มีทั้งลูกค้าและทีมของใบนั้น)
      ไม่มีค่อยใช้ DETAIL ที่ยุบใบแล้ว — ไม่มีคอลัมน์ลูกค้า จึงเก็บลูกค้าเป็น "*"
      (ตอนประกอบตารางจะถอยมาจับด้วย item+ทีม แทน ดีกว่าปล่อยยอดเป็น 0 ทั้งคอลัมน์)
      TEAM_NAME ของ DETAIL อาจเป็น "RTS,NYK1-1" (บล็อกเดียวหลายทีม) → เฉลี่ย kg เท่า ๆ กัน
      ตามจำนวนทีมในช่อง เพราะไม่มียอดรายใบให้แบ่งตามจริง
    """
    p = plan_view._latest_booking_path()
    meta = {"file": p.name if p else None, "mtime": _mtime(p) if p else None}
    if p is None or not weeks:
        return {}, {}, meta

    import pandas as pd
    try:
        xls = pd.ExcelFile(p)
        sheet = "DETAIL_SC" if "DETAIL_SC" in xls.sheet_names else (
            "DETAIL" if "DETAIL" in xls.sheet_names else None)
        if sheet is None:
            return {}, {}, meta
        df = xls.parse(sheet)
    except Exception:
        return {}, {}, meta

    meta["sheet"] = sheet
    cols = set(df.columns)
    if not {"ITEM_CODE", "WEEK", "KP_WEIGHT"} <= cols:
        return {}, {}, meta

    wyear = plan_view.week_year_map()
    want = {w: y for y, w in weeks}          # เลขสัปดาห์เปล่า → ปีที่ต้องการ (8 สัปดาห์ข้างหน้าไม่ซ้ำเลข)
    cus_col = ("KNIT_CUS_NAME" if "KNIT_CUS_NAME" in cols else
               "CUSTOMER_NAME" if "CUSTOMER_NAME" in cols else None)
    meta["per_customer"] = cus_col is not None
    by_pair: dict = {}
    by_item: dict = {}
    for r in df.to_dict("records"):
        item = _s(r.get("ITEM_CODE")).upper()
        if not item:
            continue
        wk = plan_view._num(r.get("WEEK"))
        if wk is None or wk != wk:
            continue
        wk = int(wk)
        if wk == _SENTINEL_WEEK or wk not in want:
            continue
        # ปีของแถว: เดาจากปฏิทิน — ถ้าเดาได้แล้วไม่ตรงปีของสัปดาห์ที่ต้องการ = งานคนละปี ข้ามไป
        # มีคอลัมน์ YEAR (booking รุ่นใหม่) ใช้ปีจริงของแถว ; ไม่มีค่อยเดาจากปฏิทินแบบเดิม
        y = int(plan_view._num(r.get("YEAR")) or 0) or wyear.get(wk)
        if y is not None and y != want[wk]:
            continue
        kg = _f(r.get("KP_WEIGHT"))
        if not kg:
            continue
        key = (want[wk], wk)
        d = by_item.setdefault(item, {})
        d[key] = d.get(key, 0.0) + kg

        teams = [t.strip() for t in _s(r.get("TEAM_NAME")).split(",") if t.strip()]
        if not teams:
            continue
        ck = _ckey(r.get(cus_col)) if cus_col else "*"
        share = kg / len(teams)
        for t in teams:
            d = by_pair.setdefault((item, ck, _tkey(t)), {})
            d[key] = d.get(key, 0.0) + share
    return by_pair, by_item, meta


# ---------------------------------------------------------------- ยุบเป็นราย "ทีม" (โหมด cross)
def _team_targets(reg_pair: dict) -> dict:
    """{(ITEM, tkey(ทีม)): เป้ารวมทุกลูกค้าในทีมนั้น}

    โหมด cross มองยอด "ทั้งทีม" ไม่แยกลูกค้า เพราะของในทีมเดียวกันโยกกันเองได้อยู่แล้ว
    ทีมเดียวที่ทะเบียนตั้งเป้าไว้หลายลูกค้า จึงเอาเป้ามาบวกกันเป็นเป้าของทีม
    """
    out: dict = {}
    for (item, _ck, tk), r in reg_pair.items():
        d = out.get((item, tk))
        if d is None:
            d = out[(item, tk)] = {"team": r["team"], "customers": [], "type1": r["type1"],
                                   "target_month": 0.0, "target_scm": 0.0,
                                   "stock_min": 0.0, "stock_max": 0.0, "stock5": 0.0}
        for c in r["customers"]:
            if c not in d["customers"]:
                d["customers"].append(c)
        if not d["type1"]:
            d["type1"] = r["type1"]
        for k in ("target_month", "target_scm", "stock_min", "stock_max", "stock5"):
            d[k] += r[k]
    return out


def _team_incoming(inc_pair: dict) -> dict:
    """{(ITEM, tkey(ทีม)): {(ปี,สัปดาห์): kg}} — งานที่จะทอเข้า รวมทุกลูกค้าในทีมเดียวกัน

    รวมคีย์ลูกค้า "*" (booking เก่าที่ไม่มีคอลัมน์ลูกค้า) ไปด้วยโดยไม่นับซ้ำ —
    ไฟล์หนึ่งใบมีอย่างใดอย่างหนึ่งเท่านั้น ไม่มีทางมีทั้งลูกค้าจริงและ "*" พร้อมกัน
    """
    out: dict = {}
    for (item, _ck, tk), d in inc_pair.items():
        t = out.setdefault((item, tk), {})
        for k, v in d.items():
            t[k] = t.get(k, 0.0) + v
    return out


def _sum_targets(parts: list) -> dict:
    """เป้าของก้อนรวม = บวกเป้าของทุกทีมในแถวนั้น (MIN รวม / MAX รวม)

    เลือกวิธี "บวก" เพราะแต่ละทีมมียอดใช้ต่อสัปดาห์ของตัวเอง — พอโยกของมากองรวมกัน
    ยอดใช้ต่อสัปดาห์ของก้อนรวมก็คือผลบวก cover รวมจึงเทียบได้ตรงกับ cover รายทีม
    """
    tot = {"team": "", "customers": [], "type1": "",
           "target_month": 0.0, "target_scm": 0.0,
           "stock_min": 0.0, "stock_max": 0.0, "stock5": 0.0}
    for p in parts:
        for c in (p.get("customer") or "").split(", "):
            if c and c not in tot["customers"]:
                tot["customers"].append(c)
        if not tot["type1"]:
            tot["type1"] = p.get("type1") or ""
        tot["target_month"] += p["target_month"]
        tot["target_scm"] += p["target_scm"]
        tot["stock_min"] += p["stock_min"]
        tot["stock_max"] += p["stock_max"]
        tot["stock5"] += p["stock5"]
    return tot


def _cross_totals(stk: dict, reg_pair: dict, inc_pair: dict, make_row) -> dict:
    """{ITEM: แถวสรุป "รวมทุกทีม core"} — เฉพาะผ้าที่ทะเบียนตั้งเป้าไว้ตั้งแต่ 2 ทีมขึ้นไป

    ใช้ต่อท้ายแถวรายทีมในโหมด core: ทีมหนึ่งอาจขาดแต่อีกทีมมีเหลือ — มองรวมทั้งรหัสแล้ว
    อาจ "โยกของข้ามทีม" แทนการสั่งทอใหม่ได้ ซึ่งแถวรายทีมล้วน ๆ ไม่มีทางบอกได้

    **นับเฉพาะทีมที่เป็น core ของรหัสนั้น** — ทีมนอกทะเบียนที่บังเอิญถือผ้ารหัสเดียวกันไว้
    ไม่ใช่ของก้อนนี้ ตัดทิ้งทั้งสต๊อกและงานที่จะทอเข้า (ดูของพวกนั้นได้ในโหมดสต๊อกทั้งหมด)

    ยอดรวม = ทุกทีม core บวกกัน · MIN/MAX รวม = เป้าของทีมเหล่านั้นบวกกัน
    (เลือกวิธีบวกเพราะแต่ละทีมมียอดใช้ต่อสัปดาห์ของตัวเอง — cover รวมจึงเทียบได้กับ cover รายทีม)
    """
    reg_team = _team_targets(reg_pair)
    inc_team = _team_incoming(inc_pair)
    core_teams: dict = {}
    for (it, tk) in reg_team:
        core_teams.setdefault(it, set()).add(tk)

    out: dict = {}
    for item, tks in core_teams.items():
        if len(tks) < 2:
            continue
        rec = stk.get(item)
        # ยุบคีย์ (ลูกค้า, ทีม) ของสต๊อกให้เหลือราย "ทีม" — ในทีมเดียวกันโยกของกันเองได้อยู่แล้ว
        keys_by_team: dict = {}
        for k in (rec["by_key"] if rec else {}):
            if k[1] in tks:
                keys_by_team.setdefault(k[1], []).append(k)

        parts = []
        for tk in tks:
            r = reg_team[(item, tk)]
            p = make_row(item, ", ".join(r["customers"]) if r["customers"] else "",
                         r["team"] or tk, r,
                         _combine(rec, set(keys_by_team.get(tk, []))),
                         inc_team.get((item, tk)))
            p["key"] = f"{item}|{tk}"
            parts.append(p)
        parts.sort(key=lambda p: -p["onhand"])

        all_keys = {k for ks in keys_by_team.values() for k in ks}
        inc_all: dict = {}
        for tk in tks:
            for wk, kg in (inc_team.get((item, tk)) or {}).items():
                inc_all[wk] = inc_all.get(wk, 0.0) + kg
        row = make_row(item, "", "", _sum_targets(parts),
                       _combine(rec, all_keys), inc_all)
        # โยกข้ามทีมได้เท่าไหร่: ทีมที่ขาดต้องเติมให้ถึงกรอบ 2 สัปดาห์เท่าไหร่ (need)
        # เทียบกับของที่ทีมอื่นมีเกินกรอบนั้นและปล่อยออกได้ (spare) — โยกจริงได้ = ค่าที่น้อยกว่า
        need = sum(max(0.0, p["stock_min"] * COVER_CRIT - p["inv"])
                   for p in parts if p["status"] == "crit")
        spare = sum(max(0.0, p["inv"] - p["stock_min"] * COVER_CRIT)
                    for p in parts if p["stock_min"] > 0 and p["status"] != "crit")
        row["key"] = f"{item}|TOTAL"
        row["parts"] = parts
        row["n_teams"] = len(parts)
        row["n_stock_teams"] = sum(1 for p in parts if p["onhand"] > 0)
        row["n_crit"] = sum(1 for p in parts if p["status"] == "crit")
        row["need"] = round(need, 1)
        row["spare"] = round(spare, 1)
        row["movable"] = round(min(need, spare), 1)
        # rescue = แยกทีมแล้วมีทีมขาด แต่พอรวมทั้งรหัสกลับอยู่ในกรอบ → โยกแทนสั่งทอใหม่ได้
        row["rescue"] = bool(need > 0 and spare > 0 and row["stock_min"] > 0
                             and row["status"] != "crit")
        out[item] = row
    return out


# ---------------------------------------------------------------- ประกอบตาราง
def _status(cover: float, inv: float, smin: float, smax: float) -> str:
    """สถานะสต๊อกของ item

    ไล่ตามลำดับความสำคัญ: ขาดก่อน → ล้น → ปกติ
      crit = cover < 2 สัปดาห์ — เกณฑ์เดียวกับ trigger ผลิต core ของ Planning (_cover < 2)
      over = Inventory เกิน STOCK MAX (เพดานที่ตั้งไว้ในทะเบียน) = จมทุน
      ok   = อยู่ในกรอบ MIN–MAX ที่ตั้งใจไว้
    ⚠ ไม่มีขั้น "เฝ้าระวัง" แยก เพราะ STOCK MAX = 3–4 เท่าของ STOCK MIN อยู่แล้ว
      ช่วง cover 2–4 จึงทับกับ over จนไม่เหลือช่วงของตัวเอง
    """
    if smin <= 0:
        return "none"                       # ไม่มีเป้าให้เทียบ (item นอกทะเบียน core)
    if cover < COVER_CRIT:
        return "crit"
    if smax > 0 and inv > smax:
        return "over"
    return "ok"


def table(mode: str = "core") -> dict:
    """ตารางสต๊อกผ้าดิบสำหรับหน้า Dashboard — mode: core | all"""
    mode = "all" if str(mode).strip().lower() == "all" else "core"

    key = (mode, _mtime(_STOCK_FILE), _mtime(core_registry.path() or _STOCK_FILE),
           _mtime(plan_view._latest_booking_path() or _STOCK_FILE))
    hit = _cache.get(key)
    if hit is not None:
        return hit

    reg = core_registry.load()
    reg_pair, reg_item, reg_meta = reg["pairs"], reg["by_item"], reg["meta"]
    stk, stk_meta = _stock()
    weeks = _upcoming_weeks(INCOMING_WEEKS)
    inc_pair, inc_item, bk_meta = _incoming(weeks)
    core_items = {it for it, _c, _t in reg_pair}

    def make_row(item, cus, team, r, s, inc) -> dict:
        """1 แถวของตาราง — cus/team = '' คือโหมด all (รวมทั้งคลัง)
        r = เป้าจากทะเบียน (None = ไม่มีเป้า) · s = สต๊อกที่รวมมาแล้ว · inc = งานเข้ารายสัปดาห์"""
        smin = r["stock_min"] if r else 0.0
        smax = r["stock_max"] if r else 0.0
        onhand = s["onhand"]

        wk_list = [{"w": w, "y": y, "kg": round((inc or {}).get((y, w), 0.0), 1)}
                   for y, w in weeks]
        in1 = wk_list[0]["kg"] if len(wk_list) > 0 else 0.0
        in2 = wk_list[1]["kg"] if len(wk_list) > 1 else 0.0
        inv = onhand + in1 + in2               # สูตรเดียวกับคอลัมน์ Inventory ของ Stock.py
        cover = (inv / smin) if smin > 0 else 0.0

        spec = stk.get(item) or {}
        return {
            "key": f"{item}|{cus}|{team}",
            "item": item,
            "cus": cus,
            "team": team,
            "is_core": item in core_items,
            "family": spec.get("family", ""),
            "structure": spec.get("structure", ""),
            "yarn": spec.get("yarn", ""),
            "gm2": spec.get("gm2", ""),
            "mc_group": spec.get("mc_group", ""),
            "gauge": plan_view._gkey(spec.get("gauge", "")) if spec.get("gauge") else "",
            "teams": s["teams"],
            "customer": (cus or (", ".join(r["customers"]) if r and r["customers"]
                                 else spec.get("customer", ""))),
            "type1": r["type1"] if r else "",
            "onhand": round(onhand, 1),
            "small": round(s["small"], 1),
            "blocked": round(s["blocked"], 1),
            "reserved": round(s["reserved"], 1),
            "lots": s["lots"],
            "aging_max": int(s["aging_max"]),
            "aging_avg": int(s["aging_avg"]),
            "in1": round(in1, 1), "in2": round(in2, 1),
            "in_total": round(sum(w["kg"] for w in wk_list), 1),
            # ส่งไทม์ไลน์เฉพาะแถวที่มีงานเข้าจริง — โหมด "ทั้งหมด" มี ~1,700 แถว
            # ถ้าส่งอาเรย์ 8 ช่องที่เป็น 0 ทุกแถว payload บวมขึ้นอีก ~300 KB ฟรี ๆ
            "weeks": wk_list if any(w["kg"] for w in wk_list) else [],
            "inv": round(inv, 1),
            "target_month": round(r["target_month"], 1) if r else 0.0,
            "target_scm": round(r["target_scm"], 1) if r else 0.0,
            "stock_min": round(smin, 1), "stock_max": round(smax, 1),
            "stock5": round(r["stock5"], 1) if r else 0.0,
            "cover": round(cover, 2),
            "status": _status(cover, inv, smin, smax),
        }

    rows = []
    totals: dict = {}
    if mode == "core":
        # 1 แถว = 1 (item + ลูกค้า + ทีม) ตามทะเบียน — ครบสามอย่างเท่านั้นถึงเป็นสต๊อก core
        # ของรหัสเดียวกันที่ผูกลูกค้าอื่นหรืออยู่คนละทีม = ไม่ใช่ core (ดูได้ในโหมดสต๊อกทั้งหมด)
        for (item, ck, tk), r in sorted(reg_pair.items()):
            # ยอดงานเข้า: จับด้วยลูกค้า+ทีม ถ้าไฟล์ booking ไม่มีคอลัมน์ลูกค้า ("*") ถอยมาจับด้วยทีม
            inc = inc_pair.get((item, ck, tk)) or inc_pair.get((item, "*", tk))
            rows.append(make_row(item, r["cus"], r["team"], r,
                                 _combine(stk.get(item), {(ck, tk)}), inc))
        totals = _cross_totals(stk, reg_pair, inc_pair, make_row)
    else:
        for item in sorted(set(stk) | set(reg_item)):
            rows.append(make_row(item, "", "", reg_item.get(item),
                                 _combine(stk.get(item), None), inc_item.get(item)))

    # เรียงจาก "ขาดมากสุด" ก่อน: มีเป้าและ cover น้อยสุดขึ้นก่อน / ไม่มีเป้าเรียงตามยอดมากสุด
    rows.sort(key=lambda x: (0 if x["stock_min"] > 0 else 1,
                             x["cover"] if x["stock_min"] > 0 else -x["onhand"]))

    out = {
        "mode": mode,
        "source": {"stock": stk_meta, "registry": reg_meta, "booking": bk_meta},
        "weeks": [{"w": w, "y": y} for y, w in weeks],
        "core_total": len(reg_pair),
        "core_items": len(core_items),
        "stock_items": len(stk),
        "rows": rows[:MAX_ROWS],
        "truncated": max(0, len(rows) - MAX_ROWS),
        # แถวสรุป "รวมทุกทีม core" ต่อรหัสผ้า — หน้าเว็บเอาไปแทรกท้ายกลุ่มของผ้านั้น
        # แยกออกจาก rows เพื่อไม่ให้ KPI / ชิปสถานะ / กราฟกระจายสถานะ นับซ้ำกับแถวรายทีม
        "totals": totals,
    }
    if not stk:
        out["note"] = (f"ยังไม่มีไฟล์ {_STOCK_FILE.name} — กรุณาดึงข้อมูล Stock ก่อน"
                       if not _STOCK_FILE.exists() else
                       f"อ่านไฟล์ {_STOCK_FILE.name} ไม่ได้")
    elif mode == "core" and not reg_pair:
        out["note"] = "อ่านทะเบียน Core Greige (Target_Stock) ไม่ได้ — ตรวจ path ใน config.ini"
    elif not bk_meta.get("file"):
        out["note"] = "ยังไม่มีไฟล์ booking — ยอด 'งานที่จะทอเข้า' เป็น 0 ทั้งตาราง"

    _cache.clear()          # แคชไว้ชุดเดียวพอ (ไฟล์เปลี่ยน = คีย์เปลี่ยน)
    _cache[key] = out
    return out

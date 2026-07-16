"""
order_color_advisor.py — วิเคราะห์ "item ที่มีสี (ต้องย้อม)" แล้วแนะนำการปรับแผนถัก
(advisory เท่านั้น — ไม่เขียนทับไฟล์แผนจริง)

หลักการ (Python คำนวณ deterministic ทั้งหมด):
  1. คัด item ที่ "มีสี" = คอลัมน์ LOAD_DYE ไม่ว่าง (เช่น "LOAD DYE W29")
  2. แปลง LOAD_DYE → สัปดาห์ย้อม (dye_week) ใช้ค่าที่เร็วสุด
     → deadline ถักเสร็จ = dye_week − N   (N = LEAD_WEEKS, default 2)
  3. หา item ในแผนล่าสุด (match ORA_ITEM_CODE ↔ PLAN.ITEM_CODE) → PLAN_WEEK ปัจจุบัน
  4. ตัดสินสถานะ: OK (ถักทันย้อม) / LATE (ในแผนแต่ช้าเกิน) / MISSING (ยังไม่มีในแผน)
  5. หาเครื่องว่าง (AVA) ที่ week ≤ deadline:
       - ว่าง  → แนะ "วางที่ W__"
       - เต็ม  → หา item "ไม่มีสี" CAT|เกจเดียวกันในสัปดาห์นั้น แนะ "ขยับออกแล้ววางสีนี้แทน"
       - ไม่ได้เลย → flag ให้ระบุเอง
"""
import json
import re
from datetime import date, datetime, timedelta

import config
import plan_view
import order_color_view

# item ต้องถักเสร็จก่อนสัปดาห์ย้อม (LOAD_DYE) กี่สัปดาห์
LEAD_WEEKS = 2

# สัปดาห์ที่ freeze (แก้แผนไม่ได้) = สัปดาห์ปัจจุบัน + FREEZE_WEEKS → เริ่มปรับได้ที่สัปดาห์นี้
# ต้องตรงกับ currentPlanWeek()+2 ใน PlanGantt.jsx
FREEZE_WEEKS = 2

_W_RE = re.compile(r"W\s*(\d{1,2})", re.IGNORECASE)


def _to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        s = str(v).strip()
        try:
            return int(float(s))
        except (TypeError, ValueError):
            return None


def _to_num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _gnorm(g):
    """normalize เกจ (เลขล้วน) ให้ตรงกับ ava key"""
    s = str(g).strip().upper().replace("GAUGE", "").replace("G", "")
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def _pool_key(pm, cat, gauge, mcgroup):
    """key ที่ตรงกับ ava/plan_rows — พูลแยก (SKP vs SKPTA/SKPLE) ใช้ pool key เฉพาะพูล
    ไม่งั้น = cat|gauge (pm = pool_map(): {'cat|gauge|MCGROUP': pool})"""
    cg = f"{str(cat).strip()}|{_gnorm(gauge)}"
    m = str(mcgroup or "").strip().upper()
    return (pm.get(f"{cg}|{m}") if m else None) or cg


def _col_index(columns, *names):
    up = [str(c).strip().upper() for c in columns]
    for n in names:
        if n in up:
            return up.index(n)
    return -1


def _codes(ora):
    """แยก ORA_ITEM_CODE (คั่นด้วย ,) → list รหัสเต็ม (upper)"""
    if ora is None:
        return []
    out = []
    for c in str(ora).split(","):
        c = c.strip().upper()
        if c and c != "NAN":
            out.append(c)
    return out


def _po_week(v):
    """PO_IN_DATE ("18/06/2026,24/06/2026,...") → สัปดาห์ที่ด้ายเข้า "ครบทุกล็อต" (วันหลังสุด)
    กติกา PO_IN: เส้นแบ่งสัปดาห์คือ พฤหัส–พุธ (ขยับจากนิยามศุกร์–พฤหัส 1 วัน) —
    ด้ายที่เข้าวันพฤหัสนับเป็นด้ายเข้าของ "สัปดาห์ถัดไป" → สูตร = สัปดาห์ของ(วันที่+1)
    = +4 วันก่อนหา ISO week (นิยามโปรเจกต์ +3) — "" ถ้าไม่มีวันที่"""
    if v is None:
        return ""
    last = None
    for part in str(v).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            d = datetime.strptime(part, "%d/%m/%Y").date()
        except ValueError:
            continue
        if last is None or d > last:
            last = d
    if last is None:
        return ""
    return (last + timedelta(days=4)).isocalendar()[1]


def _dye_weeks(load_dye):
    """แปลง LOAD_DYE ("LOAD DYE W29, LOAD DYE W32") → [29, 32] เรียงจากน้อยไปมาก"""
    if not load_dye:
        return []
    return sorted({int(m) for m in _W_RE.findall(str(load_dye))})


# ---------- แหล่งข้อมูลแผน / เครื่อง ----------
def _plan_index():
    """อ่านชีท PLAN ล่าสุด → (plan_name, by_code, rows)
      by_code : {ITEM_CODE(upper): [ {week,cat,gauge,mcgroup} ] }
      rows    : list ของ {code,week,cat,gauge,catgauge}  (ใช้หา item ขยับ)
    """
    grid = plan_view.read_grid()
    cols = grid.get("columns", [])
    data = grid.get("rows", [])
    name = grid.get("name")
    i_item = _col_index(cols, "ITEM_CODE")
    i_week = _col_index(cols, "PLAN_WEEK")
    i_cat = _col_index(cols, "CAT")
    i_gauge = _col_index(cols, "MC_GUAGE", "GUAGE")
    i_mcg = _col_index(cols, "MC_GROUP")
    pm = plan_view.pool_map()   # map เครื่อง→พูล (แยก SKP vs SKPTA/SKPLE)
    by_code, rows = {}, []
    if min(i_item, i_week) < 0:
        return name, by_code, rows
    for r in data:
        if i_item >= len(r):
            continue
        code = str(r[i_item]).strip().upper()
        if not code:
            continue
        week = _to_int(r[i_week]) if i_week < len(r) else None
        cat = str(r[i_cat]).strip() if 0 <= i_cat < len(r) else ""
        gauge = _gnorm(r[i_gauge]) if 0 <= i_gauge < len(r) else ""
        mcg = str(r[i_mcg]).strip() if 0 <= i_mcg < len(r) else ""
        rec = {"week": week, "cat": cat, "gauge": gauge, "mcgroup": mcg}
        by_code.setdefault(code, []).append(rec)
        rows.append({"code": code, "week": week, "cat": cat, "gauge": gauge,
                     "catgauge": _pool_key(pm, cat, gauge, mcg)})
    return name, by_code, rows


def _detail_info():
    """lookup CAT/เกจ/เครื่องต่อ item จากชีท DETAIL ของ booking_final ล่าสุด
    → {ITEM_CODE(upper): (cat, gauge, mc_group)}  (ใช้กับ item ที่ยังไม่มีในแผน)"""
    p = plan_view._latest_booking_path()
    out = {}
    if p is None:
        return out
    try:
        import pandas as pd
        df = pd.read_excel(p, sheet_name="DETAIL")
    except Exception:
        return out
    if not {"ITEM_CODE", "CAT", "GUAGE"} <= set(df.columns):
        return out
    has_mcg = "MC_GROUP" in df.columns
    for _, r in df.iterrows():
        code = str(r["ITEM_CODE"]).strip().upper()
        if code and code not in out:
            mcg = str(r["MC_GROUP"]).strip() if has_mcg else ""
            out[code] = (str(r["CAT"]).strip(), _gnorm(r["GUAGE"]), mcg)
    return out


def _aggregate_items(cols, data):
    """รวมข้อมูลไฟล์ Order Color ต่อ ITEM (ไฟล์แตกแถวตาม FG_WEEK)"""
    i_item = _col_index(cols, "ITEM")
    i_ora = _col_index(cols, "ORA_ITEM_CODE")
    i_dye = _col_index(cols, "LOAD_DYE")
    i_qty = _col_index(cols, "TOTAL_QTY")
    i_stock = _col_index(cols, "STOCK_BALANCE_KG")
    i_fg = _col_index(cols, "FG_WEEK")
    i_tub = _col_index(cols, "TUBULAR_TYPE_DESC")
    if min(i_item, i_dye) < 0:
        return None

    def cell(r, i):
        return r[i] if (0 <= i < len(r)) else ""

    items = {}
    for r in data:
        item = str(cell(r, i_item)).strip()
        if not item:
            continue
        a = items.setdefault(item, {
            "item": item, "ora": str(cell(r, i_ora)).strip(),
            "load_dye": str(cell(r, i_dye)).strip(),
            "qty": _to_num(cell(r, i_qty)), "stock": _to_num(cell(r, i_stock)),
            "tubular": str(cell(r, i_tub)).strip(), "fg_weeks": set(),
        })
        fg = _to_int(cell(r, i_fg))
        if fg is not None:
            a["fg_weeks"].add(fg)
        if not a["ora"]:
            a["ora"] = str(cell(r, i_ora)).strip()
        if not a["load_dye"]:
            a["load_dye"] = str(cell(r, i_dye)).strip()
    return items


# ---------- วิเคราะห์หลัก ----------
def analyze() -> dict:
    """endpoint หลัก — คืนผลวิเคราะห์ + คำแนะนำต่อ item ที่มีสี"""
    grid = order_color_view.read_grid()  # อาจ raise FileNotFoundError
    cols = grid.get("columns", [])
    data = grid.get("rows", [])
    oc_name = grid.get("name")

    empty = {"order_color_name": oc_name, "plan_name": None,
             "lead_weeks": LEAD_WEEKS, "items": [], "summary": {}}
    if not cols or not data:
        return {**empty, "note": "ยังไม่มีข้อมูลในไฟล์ Order Color"}

    items = _aggregate_items(cols, data)
    if items is None:
        return {**empty, "note": "ไฟล์ Order Color ไม่มีคอลัมน์ ITEM/LOAD_DYE ที่จำเป็น"}

    # เฉพาะ item ที่ "มีสี" (LOAD_DYE ไม่ว่าง)
    color_items = [v for v in items.values() if _dye_weeks(v["load_dye"])]

    plan_name, by_code, plan_rows = _plan_index()
    ava = plan_view.ava_by_week()  # {week: {key: {"remain":..}}}  key = พูล (แยก SKP/SKPTA·SKPLE) หรือ cat|gauge
    pm = plan_view.pool_map()      # map เครื่อง→พูล
    detail = _detail_info()

    # เซตของรหัสเต็มที่เป็น "งานสี" (ใช้ระบุ item ไม่มีสีตอนหาตัวขยับ)
    color_codes = set()
    for v in color_items:
        color_codes.update(_codes(v["ora"]))

    ava_weeks = sorted(w for w in (_to_int(k) for k in ava.keys()) if w is not None)

    def _remain(week, catgauge):
        slot = ava.get(str(week), {}).get(catgauge)
        return int(slot.get("remain", 0)) if slot else 0

    def _find_target(deadline, catgauge):
        """สัปดาห์ที่วางได้ (≤ deadline) ใกล้ deadline สุดที่เครื่องว่าง → (week, free) | (None, 0)"""
        for w in [x for x in ava_weeks if x <= deadline][::-1]:
            free = _remain(w, catgauge)
            if free > 0:
                return w, free
        return None, 0

    def _find_displace(deadline, catgauge):
        """item 'ไม่มีสี' CAT|เกจเดียวกันในสัปดาห์ ≤ deadline (ใกล้ deadline สุด) → (week, code) | (None, None)"""
        for w in [x for x in ava_weeks if x <= deadline][::-1]:
            for pr in plan_rows:
                if (pr["week"] == w and pr["catgauge"] == catgauge
                        and pr["code"] not in color_codes):
                    return w, pr["code"]
        return None, None

    out_items = []
    n_ok = n_late = n_missing = n_manual = 0
    for v in color_items:
        dyes = _dye_weeks(v["load_dye"])
        dye_week = dyes[0]
        deadline = dye_week - LEAD_WEEKS
        codes = _codes(v["ora"])

        plan_recs = []
        for c in codes:
            plan_recs.extend(by_code.get(c, []))
        plan_weeks = sorted({p["week"] for p in plan_recs if p["week"] is not None})

        # CAT/เกจ/เครื่อง: จากแผนก่อน ไม่งั้นจาก DETAIL
        cat = gauge = mcg = ""
        if plan_recs:
            cat, gauge, mcg = plan_recs[0]["cat"], plan_recs[0]["gauge"], plan_recs[0].get("mcgroup", "")
        if not (cat and gauge):
            for c in codes:
                if c in detail:
                    cat, gauge, mcg = detail[c]
                    break
        # key พูล (แยก SKP vs SKPTA/SKPLE) ให้ตรง ava/plan_rows — ไม่มองรวมกัน
        catgauge = _pool_key(pm, cat, gauge, mcg)

        rec = {
            "item": v["item"], "ora": v["ora"], "tubular": v["tubular"],
            "dye_weeks": dyes, "dye_week": dye_week, "deadline": deadline,
            "plan_weeks": plan_weeks, "cat": cat, "gauge": gauge,
            "qty": round(v["qty"], 1), "stock": round(v["stock"], 1),
            "fg_weeks": sorted(v["fg_weeks"]),
        }

        if plan_weeks and min(plan_weeks) <= deadline:
            rec.update(status="OK", action="ok",
                       advice=f"ถักทันย้อม (แผน W{min(plan_weeks)} ≤ กำหนด W{deadline})")
            n_ok += 1
        else:
            in_plan = bool(plan_weeks)
            rec["status"] = "LATE" if in_plan else "MISSING"
            if not (cat and gauge):
                rec.update(action="manual",
                           advice="ไม่พบ CAT/เกจของ item (ยังไม่มีในแผน/DETAIL) — ต้องระบุเครื่องเอง")
                n_manual += 1
            else:
                tw, free = _find_target(deadline, catgauge)
                if tw is not None:
                    src = f"เลื่อนจาก W{min(plan_weeks)} " if in_plan else "วางใหม่ "
                    rec.update(action="place", target_week=tw, free=free,
                               advice=f"{src}→ วางที่ W{tw} (เครื่องว่าง {free} เครื่อง, {catgauge})")
                else:
                    dw, ditem = _find_displace(deadline, catgauge)
                    if dw is not None:
                        rec.update(action="displace", target_week=dw, displace_item=ditem,
                                   advice=(f"W{dw} เครื่องเต็ม → ขยับ {ditem} (ไม่มีสี) "
                                           f"ออกไป week ถัดไป แล้ววางสีนี้แทน ({catgauge})"))
                    else:
                        rec.update(action="manual",
                                   advice=(f"ไม่มีเครื่องว่างและไม่มี item ไม่มีสีให้ขยับที่ W≤{deadline} "
                                           f"({catgauge}) — ต้องพิจารณาเอง"))
                        n_manual += 1
            if in_plan:
                n_late += 1
            else:
                n_missing += 1

        out_items.append(rec)

    # เรียง: MISSING/LATE ก่อน OK แล้วตาม deadline ใกล้สุด
    _order = {"MISSING": 0, "LATE": 1, "OK": 2}
    out_items.sort(key=lambda x: (_order.get(x["status"], 3), x["deadline"], x["item"]))

    summary = {
        "total_color": len(color_items),
        "ok": n_ok, "late": n_late, "missing": n_missing,
        "need_action": n_late + n_missing, "manual": n_manual,
    }
    note = "" if plan_name else "ยังไม่มีไฟล์แผนผลิต — สถานะจะเป็น 'ยังไม่มีในแผน' ทั้งหมด"
    return {"order_color_name": oc_name, "plan_name": plan_name,
            "lead_weeks": LEAD_WEEKS, "items": out_items,
            "summary": summary, "note": note}


# ---------- Gantt จาก booking (ทุก item ต่อสัปดาห์) ----------
def build_booking_gantt() -> dict:
    """คืน grid schema เดียวกับ PLAN แต่ 'ทุก item' มาจาก booking DETAIL (ไม่ใช่แผนที่บาง)
    → PlanGantt เรนเดอร์ได้ทันที (ลาก/ถอดได้) เห็นทุกงานที่จองในแต่ละสัปดาห์ต่อ CAT×เกจ×เครื่อง
      งานสี (LOAD_DYE) ถูก mark ด้วย color_codes/color_meta เพื่อไฮไลต์ + ทำ swap"""
    import pandas as pd

    # รหัสงานสี + สัปดาห์ย้อม/กำหนดถัก จากไฟล์ Order Color
    ocgrid = order_color_view.read_grid()  # อาจ raise FileNotFoundError
    items = _aggregate_items(ocgrid.get("columns", []), ocgrid.get("rows", [])) or {}
    color_codes = set()
    code_meta = {}
    code_fg = {}
    code_need = {}          # รหัส → จำนวน กก. ที่ต้องถักเพื่อย้อม (None = ไม่รู้ → นับทุกแถว)
    code_info = {}          # รหัส → {qty: TOTAL_QTY (จำนวนให้สี), stock: STOCK_BALANCE_KG} โชว์ในตารางเทียบแผน
    stock_covered = set()   # รหัสงานสีที่มีผ้าใน stock พอ → ไม่ต้องถัก (ตัดออกจาก Gantt)
    n_stock_skip = 0
    for v in items.values():
        dyes = _dye_weeks(v["load_dye"])
        if not dyes:
            continue
        # เช็ค stock: ถ้า STOCK_BALANCE_KG ≥ ยอดที่ต้องผลิต (TOTAL_QTY) → เอาจาก stock ได้ ไม่ต้องถัก
        if v["qty"] > 0 and v["stock"] >= v["qty"]:
            n_stock_skip += 1
            for c in _codes(v["ora"]):
                stock_covered.add(c)
            continue
        dw = dyes[0]
        fw = sorted(w for w in v.get("fg_weeks", []) if 1 <= w <= 53)
        # deadline (ทัน/ไม่ทัน) ตัดสินจาก FG_WEEK เร็วสุด — ไม่ใช่สัปดาห์ย้อม
        deadline = min(fw) if fw else (dw - LEAD_WEEKS)
        # จำนวนที่ต้องถักเพื่อย้อม = จำนวนที่ให้สี (TOTAL_QTY) − stock ที่มี
        # (ถักเกินจากนี้ก็ไม่มีสีย้อม → ส่วนเกินถือเป็นงานไม่มีสีตามปกติ)
        need_qty = max(0.0, v["qty"] - max(0.0, v["stock"])) if v["qty"] > 0 else None
        for c in _codes(v["ora"]):
            color_codes.add(c)
            code_meta[c] = (dw, deadline)
            code_fg[c] = fw
            code_need[c] = need_qty
            code_info[c] = {"qty": round(v["qty"], 1), "stock": round(v["stock"], 1)}

    p = plan_view._latest_booking_path()
    if p is None:
        raise FileNotFoundError("ยังไม่มีไฟล์ booking_final — กรุณากดรัน AVA_MC ก่อน")
    df = pd.read_excel(p, sheet_name="DETAIL")
    need = {"ITEM_CODE", "WEEK", "CAT", "GUAGE", "MC_GROUP", "KP_WEIGHT", "MC_USE_CEIL"}
    if not need <= set(df.columns):
        raise KeyError("ชีท DETAIL ไม่มีคอลัมน์ที่จำเป็น (ITEM_CODE/WEEK/CAT/GUAGE/MC_GROUP/KP_WEIGHT/MC_USE_CEIL)")

    cur_week = _current_week()
    # โชว์ตั้งแต่สัปดาห์ปัจจุบัน แต่ W ปัจจุบัน..edit_from-1 เป็นช่วง freeze (ล็อก แก้ไม่ได้)
    # edit_from = แก้แผนได้เร็วสุด = สัปดาห์ปัจจุบัน + FREEZE (ตรงกับ currentPlanWeek()+2 ใน PlanGantt.jsx)
    edit_from = cur_week + FREEZE_WEEKS
    has_fac = "FACTORY" in df.columns
    has_mu = "MC_USE" in df.columns
    has_wd = "WORKING_DAY" in df.columns
    has_eff = "_effective_working_days" in df.columns
    has_setup = {"_is_new_setup", "_setup_days"} <= set(df.columns)
    has_inc = {"_has_mc_increase", "_mc_increase"} <= set(df.columns)

    # วันทำงานสูงสุดต่อสัปดาห์ (จาก calendar ผ่าน DETAIL) — เช่น W32 = 10 วัน (ควบสัปดาห์หยุด W31)
    week_days = {}
    if has_wd:
        for w, v in df.groupby("WEEK")["WORKING_DAY"].max().items():
            wi_ = _to_int(w)
            if wi_ is not None:
                week_days[str(wi_)] = _to_num(v)

    # คอลัมน์เสริมท้าย: ใช้คำนวณจำนวนเครื่องใหม่เมื่อย้ายสัปดาห์ (วันทำงานต่างกัน)
    # MC_DAYS = MC_USE × วันทำงาน effective (machine-days ที่งานต้องใช้ — ค่าคงที่ไม่ขึ้นกับสัปดาห์)
    # WD_FRAC = สัดส่วนวันทำงานของแถวเทียบ max ของสัปดาห์ (แต่ละโรงวันทำงานไม่เท่ากัน)
    # SETUP_DAYS = วัน setup ของแถว (ถ้าเป็น setup ใหม่) — หักออกจากวันทำงานปลายทาง
    cols = ["ITEM_CODE", "PLAN_WEEK", "CAT", "MC_GUAGE", "MC_GROUP",
            "ACTUAL_MC", "PRODUCE_QTY", "FG_WEEK", "FACTORY_TYPE", "NEW_MC",
            "MC_DAYS", "WD_FRAC", "SETUP_DAYS", "SO_NO", "TEAM_NAME", "PO_WEEK"]
    has_so = "SO_NO" in df.columns
    has_team = "TEAM_NAME" in df.columns   # ไฟล์ booking_final เก่าอาจยังไม่มี → ส่งค่าว่าง
    has_po = "PO_IN_DATE" in df.columns    # สัปดาห์ด้ายเข้าครบ — ไฟล์เก่าไม่มี → ส่งค่าว่าง
    rows = []
    for _, r in df.iterrows():
        w = _to_int(r["WEEK"])
        if w is None or w < cur_week or w > 53:
            continue
        code = str(r["ITEM_CODE"]).strip().upper()
        if not code:
            continue
        if code in stock_covered:      # มีผ้าใน stock พอ → ไม่ต้องถัก
            continue
        wd = _to_num(r["WORKING_DAY"]) if has_wd else 0.0
        eff = _to_num(r["_effective_working_days"]) if has_eff else wd
        mu = _to_num(r["MC_USE"]) if has_mu else 0.0
        wmax = week_days.get(str(w), 0.0)
        is_new = bool(r["_is_new_setup"]) if has_setup else False
        setup = _to_num(r["_setup_days"]) if (has_setup and is_new) else 0.0
        mc = int(_to_num(r["MC_USE_CEIL"]) or 0)
        # job = เครื่องที่ต้อง setup ใหม่ในสัปดาห์นั้น:
        #   setup ครั้งแรกของ run = ทั้งจำนวนเครื่อง | carry ที่เครื่องเพิ่มขึ้น = เฉพาะส่วนเพิ่ม (_mc_increase)
        # (ตรงกับ SETUP_TRACKING.JOBS_DEDUCTED ฝั่ง booking)
        if is_new:
            new_mc = mc
        elif has_inc and bool(r["_has_mc_increase"]):
            new_mc = int(_to_num(r["_mc_increase"]) or 0)
        else:
            new_mc = 0
        rows.append([
            code, w, str(r["CAT"]).strip(), _gnorm(r["GUAGE"]), str(r["MC_GROUP"]).strip(),
            mc, round(_to_num(r["KP_WEIGHT"]), 1), w,
            str(r["FACTORY"]).strip() if has_fac else "",
            new_mc,
            round(mu * (eff if eff > 0 else wd), 3),
            round(wd / wmax, 3) if wmax > 0 else 1.0,
            setup,
            str(r["SO_NO"]).strip() if has_so and pd.notna(r["SO_NO"]) else "",
            str(r["TEAM_NAME"]).strip() if has_team and pd.notna(r["TEAM_NAME"]) else "",
            _po_week(r["PO_IN_DATE"]) if has_po and pd.notna(r["PO_IN_DATE"]) else "",
        ])

    # ---- จัดกลุ่ม "run" = แผนต่อเนื่อง (carry) ของ item×เครื่อง สัปดาห์ติดกัน (gap ≤ 3 ไม่ setup ใหม่)
    # การย้ายงานสีต้องย้าย "ทั้ง run" เพื่อคงความต่อเนื่อง ไม่สร้าง setup เพิ่ม
    by_key: dict = {}
    for i, rr in enumerate(rows):
        by_key.setdefault((rr[0], rr[4]), []).append((rr[1], i))
    run_of = [None] * len(rows)
    run_rows_map: dict = {}
    rid = 0
    for lst in by_key.values():
        lst.sort()
        prev_w = None
        for w, i in lst:
            if prev_w is None or (w - prev_w) > 3:
                rid += 1
            run_of[i] = rid
            run_rows_map.setdefault(rid, []).append(i)
            prev_w = w

    # แนบ RUN_ID เข้าทุกแถว (รวมงานไม่มีสี) — frontend ใช้เลื่อน "หางของ run" แบบ ripple
    # ตอนถอดงานออก (แถวที่ถูกถอด + แถวถัดๆ ไปของ run เลื่อนต่อกัน ไม่กองสัปดาห์เดียว)
    cols.append("RUN_ID")
    for i, rr in enumerate(rows):
        rr.append(run_of[i])

    # ---- งานสี = เฉพาะ run แรกๆ ที่สะสม KP ครบ "จำนวนที่ให้สี" (need_qty)
    # run ถัดไป (ถักเกินจำนวนให้สี) = งานไม่มีสีตามปกติ (ถอด/เลื่อนได้)
    color_idx = []
    color_meta = []
    code_rows: dict = {}
    for i, rr in enumerate(rows):
        if rr[0] in color_codes:
            code_rows.setdefault(rr[0], []).append(i)
    for code, idxs in code_rows.items():
        need_qty = code_need.get(code)
        # เรียง run ของรหัสนี้ตามสัปดาห์เริ่ม
        rids = sorted({run_of[i] for i in idxs},
                      key=lambda r: min(rows[i][1] for i in run_rows_map[r]))
        cum = 0.0
        for r in rids:
            if need_qty is not None and cum >= need_qty:
                break  # ครบจำนวนให้สีแล้ว — run ที่เหลือเป็นงานไม่มีสี
            rrows = sorted(run_rows_map[r], key=lambda i: rows[i][1])
            cum += sum(rows[i][6] for i in rrows)
            color_idx.extend(rrows)
            first = rrows[0]
            dw, dl = code_meta.get(code, (None, None))
            color_meta.append({
                "idx": first, "code": code, "cat": rows[first][2], "gauge": rows[first][3],
                "dye_week": dw, "deadline": dl, "fg_weeks": code_fg.get(code, []),
                "run_rows": rrows,
                "run_weeks": [rows[i][1] for i in rrows],
            })
    color_idx.sort()

    return {"columns": cols, "rows": rows, "color_idx": color_idx,
            "color_codes": sorted(color_codes), "color_meta": color_meta,
            "code_info": code_info,
            "plan_name": p.name, "injected": 0, "week_days": week_days,
            "stock_skipped": n_stock_skip, "edit_from": edit_from,
            "note": (f"ทุก item จาก booking {p.name} — ตั้งแต่ W{cur_week} "
                     f"(W{cur_week}–{edit_from - 1} 🔒 freeze แก้ไม่ได้ • ปรับได้ตั้งแต่ W{edit_from} • ★ = งานสี"
                     + (f" • ตัดงานสี {n_stock_skip} ตัวที่มีผ้าใน stock พอ" if n_stock_skip else "")
                     + ")")}


_JOB_TYPES = ("OM", "PHET_DOUBLE", "PHET_SINGLE")
_DEFAULT_JOB_CAP = {"OM": 13, "PHET_DOUBLE": 33, "PHET_SINGLE": 44}


def booking_load() -> dict:
    """โควตา setup job ต่อสัปดาห์สำหรับ Gantt booking
    - job จาก booking คิด live จาก NEW_MC ของแถว (ขยับตามเมื่อ user ย้ายงาน)
    - old = job "งานใหม่ที่ Planning วาง" (SETUP_TRACKING PLAN_SOURCE=NEW) — กินโควตาจริงแต่ไม่อยู่ใน booking
      → ยอดรวมบนแถบ = live(booking) + old(plan-new) ตรงกับหน้าแผนผลิต (ยกเว้นส่วน item ที่ stock พอซึ่งตัดออก)
    - cap จาก REMAINING_JOBS ของแผนล่าสุด (ไม่มีไฟล์แผน → ค่ามาตรฐาน OM 13 / PHET_DOUBLE 33 / PHET_SINGLE 44)"""
    import pandas as pd
    caps: dict = {}
    plan_new: dict = {}
    weeks: set = set()
    try:
        pp = plan_view.latest_path()
        if pp is not None:
            xls = pd.ExcelFile(pp)
            sheet = plan_view._sheet_variant(xls.sheet_names, "REMAINING_JOBS")
            if sheet in xls.sheet_names:
                rj = xls.parse(sheet)
                if {"WEEK", "TYPE", "CAPACITY"} <= set(rj.columns):
                    for _, r in rj.iterrows():
                        w = _to_int(r["WEEK"])
                        t = str(r["TYPE"]).strip().upper()
                        c = _to_int(r["CAPACITY"])
                        if w is None or c is None or t not in _JOB_TYPES:
                            continue
                        caps.setdefault(str(w), {})[t] = c
                        weeks.add(str(w))
            # job งานใหม่จากแผน (นอก booking) — บวกเป็น baseline คงที่
            st_sheet = plan_view._sheet_variant(xls.sheet_names, "SETUP_TRACKING")
            if st_sheet in xls.sheet_names:
                st = xls.parse(st_sheet)
                need = {"PLAN_WEEK", "TYPE", "PLAN_SOURCE", "JOBS_DEDUCTED"}
                if need <= set(st.columns):
                    for _, r in st.iterrows():
                        if str(r["PLAN_SOURCE"]).strip().upper() != "NEW":
                            continue
                        w = _to_int(r["PLAN_WEEK"])
                        t = str(r["TYPE"]).strip().upper()
                        if w is None or t not in _JOB_TYPES:
                            continue
                        j = _to_int(r["JOBS_DEDUCTED"]) or 0
                        plan_new[(str(w), t)] = plan_new.get((str(w), t), 0) + j
                        weeks.add(str(w))
    except Exception:
        pass
    # ครอบทุกสัปดาห์ของ booking (จาก DETAIL)
    pb = plan_view._latest_booking_path()
    if pb is not None:
        try:
            df = pd.read_excel(pb, sheet_name="DETAIL", usecols=["WEEK"])
            for w in df["WEEK"].unique():
                wi = _to_int(w)
                if wi is not None and wi <= 53:
                    weeks.add(str(wi))
        except Exception:
            pass
    out = {}
    for w in weeks:
        out[w] = {t: {"old": plan_new.get((w, t), 0),
                      "cap": caps.get(w, {}).get(t, _DEFAULT_JOB_CAP[t]),
                      "bookingNew": 0} for t in _JOB_TYPES}
    return out


def booking_ava() -> dict:
    """เครื่องว่างต่อสัปดาห์แบบ 'สอดคล้อง booking' — เหมือน ava_by_week แต่ตั้ง planBase = used
    (เพราะแถวใน Gantt = งาน booking ทั้งหมด ซึ่งใช้เครื่องเท่ากับ used ตอนโหลด)
    → เครื่องว่าง live = remain − (เครื่องที่วางปัจจุบัน − used)"""
    ava = plan_view.ava_by_week()
    for wk, keys in ava.items():
        for key, slot in keys.items():
            slot["planBase"] = int(slot.get("used", 0))
    return ava


# ---------- AI แนะนำการปรับงานสี (จัดอันดับ + อธิบายผลกระทบ) ----------
_COLOR_SYSTEM_PROMPT = (
    "คุณเป็นผู้ช่วยวางแผนการถักผ้าย้อมสีของโรงงานทอผ้า "
    "หลักการ: งานสี (ต้องย้อม) ต้องได้ทอก่อนงานไม่มีสี — งานสีที่ตอนนี้ทออยู่สัปดาห์ไกล (cur_week สูง) "
    "ควรถูกดึงเข้ามาทอสัปดาห์ที่เร็วขึ้น (gain_weeks = เร็วขึ้นได้กี่สัปดาห์) "
    "ถ้าสัปดาห์เป้าหมายเครื่องว่างก็ย้ายเข้าได้เลย แต่ถ้าเครื่อง/job ไม่พอ ต้องเลือกถอดงานไม่มีสีออก "
    "หน้าที่ของคุณ: จัดอันดับว่าควรดึงงานสีตัวไหนเข้ามาก่อน และควรถอดงานไม่มีสีตัวไหนออก โดยพิจารณา "
    "(1) gain_weeks มาก = คุ้มที่จะขยับ (2) late_weeks — เลยกำหนดส่ง FG แล้วยิ่งเร่งด่วน "
    "(3) ผลกระทบต่องานที่ถูกถอด (displaced_late — งานที่ถอดออกไปแผนใหม่แล้วสายไหม เลือกตัวที่สายน้อยสุด) "
    "(4) โควตา setup job ของสัปดาห์เป้าหมาย (setup_load: used/cap) — สัปดาห์ที่ setup เต็ม (used≥cap) "
    "ไม่ควรเพิ่มงานใหม่เข้าไป "
    "หมายเหตุ: run_weeks = งานสีนั้นเป็นแผนต่อเนื่อง (carry ข้ามสัปดาห์ ไม่ต้อง setup ใหม่) กี่สัปดาห์ "
    "การย้ายจะย้ายทั้งชุดพร้อมกัน — run ยาวยิ่งกระทบเครื่องหลายสัปดาห์ ให้ชั่งน้ำหนักด้วย "
    "และอธิบาย 'ผลกระทบ' ให้ชัด: ถอดงานไหนออกจากสัปดาห์ไหน งานนั้นได้แผนใหม่สัปดาห์ไหน สายกี่สัปดาห์ "
    "ตัวเลขทั้งหมดคำนวณมาแล้ว ห้ามแก้หรือสร้างใหม่ ใช้ตามที่ให้เท่านั้น "
    "ตอบเป็นภาษาไทยสั้นๆ เข้าใจง่าย และตอบกลับเป็น JSON เท่านั้น รูปแบบ: "
    '{"summary": "สรุปภาพรวม 1-2 ประโยค", '
    '"ranking": [{"color_item": "รหัส", "rank": 1, "reason": "ทำไมควรดึงเข้าก่อน", "impact": "ถอดอะไรออก → งานนั้นไปสัปดาห์ไหน"}]}'
)


def _call_color_llm(cat: str, gauge: str, items: list, setup_load: dict) -> dict:
    """เรียก OpenAI จัดอันดับงานสี + อธิบายผลกระทบ — คืน {"summary":.., "ranking":[..]}"""
    cfg = config.llm_config()
    if not cfg["api_key"]:
        raise RuntimeError("ยังไม่ได้ตั้งค่า OpenAI (OPENAI_API_KEY)")

    from openai import OpenAI
    kwargs = {"api_key": cfg["api_key"]}
    if cfg["base_url"]:
        kwargs["base_url"] = cfg["base_url"]
    client = OpenAI(**kwargs)
    user_msg = (
        f"กลุ่มเครื่อง CAT {cat} เกจ {gauge}\n"
        f"โควตา setup job ต่อสัปดาห์ (used/cap ต่อประเภทเครื่อง): "
        f"{json.dumps(setup_load, ensure_ascii=False)}\n"
        f"งานสีที่ต้องจัด (คำนวณตัวเลขมาแล้ว):\n"
        f"{json.dumps(items, ensure_ascii=False)}"
    )
    resp = client.chat.completions.create(
        model=cfg["model"],
        messages=[
            {"role": "system", "content": _COLOR_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        # หมายเหตุ: ไม่ส่ง temperature — โมเดล reasoning (เช่น gpt-5/o-series) ไม่รองรับค่า custom
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


def advise_color_moves(payload: dict) -> dict:
    """รับ candidate ที่ frontend คำนวณแล้ว (งานสี + ตัวเลือกถอด/ผลกระทบ ของกลุ่ม CAT×เกจ ที่เลือก)
    → ให้ LLM จัดอันดับว่าควรทำตัวไหนก่อน + อธิบายผลกระทบเป็นไทย (มี fallback ถ้า AI ไม่พร้อม)
    → {"summary":.., "ranking":[{color_item, rank, reason, impact}], "ai": bool, "note":..}"""
    cat = str(payload.get("cat", "")).strip()
    gauge = str(payload.get("gauge", "")).strip()
    items = payload.get("items", []) or []
    setup_load = payload.get("setup_load", {}) or {}

    # จัดเฉพาะงานสีที่ยัง "ไม่พอเครื่อง" (fits=false) — ที่พอแล้ววางได้เลย ไม่ต้องแนะ
    need = [it for it in items if not it.get("fits")]
    if not need:
        return {"summary": "งานสีในกลุ่มนี้พอเครื่องทุกตัว — วางได้เลย ไม่ต้องขยับ",
                "ranking": [], "ai": False, "note": ""}

    try:
        llm = _call_color_llm(cat, gauge, need, setup_load)
        ranking = []
        for r in llm.get("ranking", []):
            code = str(r.get("color_item", "")).strip()
            if code:
                ranking.append({
                    "color_item": code,
                    "rank": r.get("rank"),
                    "reason": str(r.get("reason", "")).strip(),
                    "impact": str(r.get("impact", "")).strip(),
                })
        ranking.sort(key=lambda x: (x.get("rank") is None, x.get("rank") or 999))
        return {"summary": str(llm.get("summary", "")).strip(),
                "ranking": ranking, "ai": True, "note": ""}
    except Exception as e:
        # fallback: เรียงจากเลยกำหนดส่งมากสุด → ดึงเข้ามาได้เร็วขึ้นมากสุด
        need2 = sorted(need, key=lambda x: (
            -(x.get("late_weeks") or 0),
            -(x.get("gain_weeks") or 0)))
        ranking = []
        for i, it in enumerate(need2, 1):
            bm = it.get("best_move") or {}
            if not bm:
                impact = "ไม่มีที่ให้ดึงเข้า — ต้องจัดเอง"
            elif bm.get("type") == "เครื่องว่าง":
                impact = f"วางที่ W{bm.get('place_at')} (เครื่องว่าง ไม่ต้องถอดใคร)"
            else:
                impact = (f"ถอด {bm.get('item')} (W{bm.get('from_week')}) → งานนั้นไป W{bm.get('to_week')}"
                          + (f" สาย {bm.get('displaced_late')} สัปดาห์" if bm.get("displaced_late") else " ทันเวลา")
                          + f" แล้ววางงานสีที่ W{bm.get('place_at')}")
            ranking.append({"color_item": it.get("color_item"), "rank": i,
                            "reason": f"ถักอยู่ W{it.get('cur_week')} เกินกำหนดส่ง FG W{it.get('deadline')} (สาย {it.get('late_weeks', 0)} สัปดาห์)",
                            "impact": impact})
        return {"summary": "AI ไม่พร้อมใช้งาน — จัดอันดับจากการคำนวณ (สายมากสุด + ใกล้กำหนดส่งสุด)",
                "ranking": ranking, "ai": False, "note": f"({e})"}


# ---------- สร้างแผน what-if (แผนจริง + แทรก item สี) ----------
def build_plan() -> dict:
    """คืน grid แผน (schema เดียวกับชีท PLAN) = แผนจริง + แทรกแถว item สีที่ 'ยังไม่มีในแผน'
    → ให้ frontend เอาไปเรนเดอร์ PlanGantt แล้ว user ลาก/ลบเองได้ (what-if ไม่แตะไฟล์จริง)
      color_idx : index แถวที่เป็นงานสี (ทั้งที่อยู่ในแผนเดิม + ที่แทรกใหม่) → ใช้ไฮไลต์
    """
    ocgrid = order_color_view.read_grid()  # อาจ raise FileNotFoundError
    items = _aggregate_items(ocgrid.get("columns", []), ocgrid.get("rows", []))
    color_items = [v for v in (items or {}).values() if _dye_weeks(v["load_dye"])]

    pgrid = plan_view.read_grid()
    pcols = pgrid.get("columns", [])
    prows = [list(r) for r in pgrid.get("rows", [])]
    plan_name = pgrid.get("name")

    ci_item = _col_index(pcols, "ITEM_CODE")
    ci_week = _col_index(pcols, "PLAN_WEEK")
    ci_cat = _col_index(pcols, "CAT")
    ci_gauge = _col_index(pcols, "MC_GUAGE", "GUAGE")
    ci_mcg = _col_index(pcols, "MC_GROUP")
    ci_qty = _col_index(pcols, "PRODUCE_QTY", "PLAN_QTY")
    ci_amc = _col_index(pcols, "ACTUAL_MC")
    ci_nmc = _col_index(pcols, "NEW_MC")
    if min(ci_item, ci_week, ci_cat, ci_gauge) < 0:
        return {"columns": pcols, "rows": prows, "color_idx": [],
                "plan_name": plan_name, "injected": 0,
                "note": "ชีท PLAN ไม่มีคอลัมน์ ITEM_CODE/PLAN_WEEK/CAT/MC_GUAGE — เรนเดอร์ Gantt ไม่ได้"}

    # รหัสเต็มที่มีอยู่แล้วในแผน (ใช้ตัดสิน 'ยังไม่มีในแผน')
    in_plan = set()
    for r in prows:
        if ci_item < len(r):
            code = str(r[ci_item]).strip().upper()
            if code:
                in_plan.add(code)

    ava = plan_view.ava_by_week()
    ava_weeks = sorted(w for w in (_to_int(k) for k in ava.keys()) if w is not None)
    detail = _detail_info()

    # map รหัสเต็ม → (สัปดาห์ย้อมเร็วสุด, deadline ถัก) + สัปดาห์ booking (FG_WEEK) ของงานสี
    code_meta = {}
    code_fg = {}
    for v in color_items:
        dyes = _dye_weeks(v["load_dye"])
        if not dyes:
            continue
        dw = dyes[0]
        fw = sorted(w for w in v.get("fg_weeks", []) if 1 <= w <= 53)  # ตัด sentinel (เช่น 99)
        for c in _codes(v["ora"]):
            code_meta[c] = (dw, dw - LEAD_WEEKS)
            code_fg[c] = fw

    def _remain(week, catgauge):
        slot = ava.get(str(week), {}).get(catgauge)
        return int(slot.get("remain", 0)) if slot else 0

    def _target(deadline, catgauge):
        """สัปดาห์ที่จะวาง: ใกล้ deadline สุดที่เครื่องว่าง; ไม่งั้นสัปดาห์ ≤ deadline ที่ท้ายสุด;
        ถ้า deadline ต่ำกว่าขอบเขต → สัปดาห์แรกที่ทำแผนได้"""
        le = [x for x in ava_weeks if x <= deadline]
        for w in le[::-1]:
            if _remain(w, catgauge) > 0:
                return w
        if le:
            return le[-1]
        return ava_weeks[0] if ava_weeks else deadline

    injected = 0
    skipped_manual = 0
    for v in color_items:
        codes = _codes(v["ora"])
        if not codes:
            skipped_manual += 1
            continue
        if any(c in in_plan for c in codes):
            continue  # อยู่ในแผนแล้ว → ไฮไลต์อย่างเดียว ไม่แทรกซ้ำ

        cat = gauge = mcg = ""
        for c in codes:
            if c in detail:
                cat, gauge, mcg = detail[c]
                break
        if not (cat and gauge):
            skipped_manual += 1
            continue  # ไม่รู้เครื่อง → วางบน Gantt ไม่ได้

        dyes = _dye_weeks(v["load_dye"])
        deadline = dyes[0] - LEAD_WEEKS
        tw = _target(deadline, f"{cat}|{gauge}")

        row = [""] * len(pcols)
        row[ci_item] = codes[0]
        row[ci_week] = tw
        row[ci_cat] = cat
        row[ci_gauge] = gauge
        if ci_mcg >= 0:
            row[ci_mcg] = mcg
        if ci_qty >= 0:
            row[ci_qty] = round(v["qty"], 1)
        if ci_amc >= 0:
            row[ci_amc] = 1          # ประมาณการ 1 เครื่อง (what-if)
        if ci_nmc >= 0:
            row[ci_nmc] = 1
        prows.append(row)
        injected += 1

    # รหัสสีทั้งหมด (ไฮไลต์แถวที่ ITEM_CODE เป็นงานสี ทั้งเดิมและที่แทรก)
    color_codes = set()
    for v in color_items:
        color_codes.update(_codes(v["ora"]))
    color_idx = [i for i, r in enumerate(prows)
                 if ci_item < len(r) and str(r[ci_item]).strip().upper() in color_codes]

    # metadata งานสีต่อแถว (idx ในตาราง = baseIdx ที่ frontend ใช้) → ทำ dropdown/แนะนำ
    color_meta = []
    for i in color_idx:
        code = str(prows[i][ci_item]).strip().upper()
        dw, dl = code_meta.get(code, (None, None))
        color_meta.append({
            "idx": i, "code": code,
            "cat": str(prows[i][ci_cat]).strip(),
            "gauge": str(prows[i][ci_gauge]).strip() if ci_gauge >= 0 else "",
            "dye_week": dw, "deadline": dl,
            "fg_weeks": code_fg.get(code, []),
        })

    note = ""
    _color_codes_out = sorted(color_codes)
    if skipped_manual:
        note = (f"มี item สี {skipped_manual} ตัวที่ไม่รู้ CAT/เครื่อง (ไม่มีใน DETAIL) "
                f"— ยังวางบน Gantt ไม่ได้ ต้องจัดเอง")
    return {"columns": pcols, "rows": prows, "color_idx": color_idx,
            "color_codes": _color_codes_out, "color_meta": color_meta,
            "plan_name": plan_name, "injected": injected, "note": note}


# ---------- ประวัติ booking ต่อ CAT (เฉพาะ CAT ที่มี item สี) ----------
def _current_week() -> int:
    """สัปดาห์ปัจจุบันตามนิยามโปรเจกต์ (ศุกร์–พฤหัส: บวก 3 วันก่อนหา ISO week)"""
    return (date.today() + timedelta(days=3)).isocalendar()[1]


def _plan_by_group() -> dict:
    """แผนปัจจุบันจากชีท PLAN ล่าสุด แยกตามกลุ่มเครื่อง (CAT × เกจ)
    → { (cat, gauge_norm): { ITEM_CODE(upper): { week(int): actual_mc } } }
    (รวมทั้ง item มีสีและไม่มีสี — item ไม่มีสีคือตัวที่ถอดออกได้)"""
    try:
        grid = plan_view.read_grid()
    except FileNotFoundError:
        return {}
    cols = grid.get("columns", [])
    data = grid.get("rows", [])
    i_item = _col_index(cols, "ITEM_CODE")
    i_week = _col_index(cols, "PLAN_WEEK")
    i_cat = _col_index(cols, "CAT")
    i_g = _col_index(cols, "MC_GUAGE", "GUAGE")
    i_amc = _col_index(cols, "ACTUAL_MC")
    out: dict = {}
    if min(i_item, i_week, i_cat, i_g) < 0:
        return out
    for r in data:
        item = str(r[i_item]).strip().upper() if i_item < len(r) else ""
        if not item:
            continue
        w = _to_int(r[i_week]) if i_week < len(r) else None
        if w is None:
            continue
        cat = str(r[i_cat]).strip() if i_cat < len(r) else ""
        g = _gnorm(r[i_g]) if i_g < len(r) else ""
        amc = _to_num(r[i_amc]) if 0 <= i_amc < len(r) else 0.0
        grp = out.setdefault((cat, g), {})
        it = grp.setdefault(item, {})
        it[w] = it.get(w, 0.0) + amc
    return out


def cat_history() -> dict:
    """endpoint หลักของหน้า Order Color (default view)
    จัดกลุ่ม booking ตาม CAT — โชว์เฉพาะ CAT ที่มี item สี (LOAD_DYE)
    แต่ละ CAT = ทุก item ใน CAT นั้น (มีสี/ไม่มีสี) พร้อม booking รายสัปดาห์
    ตั้งแต่สัปดาห์ปัจจุบันเป็นต้นไป (ค่าที่โชว์ = BK_KP_WEIGHT น้ำหนักตามแผนถัก)
    """
    grid = order_color_view.read_grid()  # อาจ raise FileNotFoundError
    cols = grid.get("columns", [])
    data = grid.get("rows", [])
    oc_name = grid.get("name")

    cur_week = _current_week()
    empty = {"order_color_name": oc_name, "current_week": cur_week,
             "value_col": "BK_KP_WEIGHT", "weeks": [], "groups": []}
    if not cols or not data:
        return {**empty, "note": "ยังไม่มีข้อมูลในไฟล์ Order Color"}

    i_item = _col_index(cols, "ITEM")
    i_ora = _col_index(cols, "ORA_ITEM_CODE")
    i_dye = _col_index(cols, "LOAD_DYE")
    i_qty = _col_index(cols, "TOTAL_QTY")
    i_stock = _col_index(cols, "STOCK_BALANCE_KG")
    i_fg = _col_index(cols, "FG_WEEK")
    i_tub = _col_index(cols, "TUBULAR_TYPE_DESC")
    i_kp = _col_index(cols, "BK_KP_WEIGHT")
    if min(i_item, i_dye, i_fg) < 0:
        return {**empty, "note": "ไฟล์ Order Color ไม่มีคอลัมน์ ITEM/LOAD_DYE/FG_WEEK ที่จำเป็น"}

    def cell(r, i):
        return r[i] if (0 <= i < len(r)) else ""

    detail = _detail_info()  # {ITEM_CODE(upper): (cat, gauge, mc_group)}

    # รวมข้อมูลต่อ ITEM + เก็บ booking รายสัปดาห์ (FG_WEEK → BK_KP_WEIGHT)
    items = {}
    for r in data:
        item = str(cell(r, i_item)).strip()
        if not item:
            continue
        a = items.setdefault(item, {
            "item": item, "ora": str(cell(r, i_ora)).strip(),
            "load_dye": str(cell(r, i_dye)).strip(),
            "tubular": str(cell(r, i_tub)).strip(),
            "qty": _to_num(cell(r, i_qty)), "stock": _to_num(cell(r, i_stock)),
            "weeks": {},
        })
        if not a["ora"]:
            a["ora"] = str(cell(r, i_ora)).strip()
        if not a["load_dye"]:
            a["load_dye"] = str(cell(r, i_dye)).strip()
        fg = _to_int(cell(r, i_fg))
        kp = _to_num(cell(r, i_kp))
        if fg is not None and kp:
            a["weeks"][fg] = a["weeks"].get(fg, 0.0) + kp

    # หา CAT/เกจ ต่อ item จาก DETAIL (ผ่าน ORA_ITEM_CODE) + สถานะสี
    for a in items.values():
        cat = gauge = ""
        for c in _codes(a["ora"]):
            if c in detail:
                cat, gauge, _mcg = detail[c]
                break
        a["cat"], a["gauge"] = cat, gauge
        dyes = _dye_weeks(a["load_dye"])
        a["is_color"] = bool(dyes)
        a["dye_week"] = dyes[0] if dyes else None

    # กลุ่มที่ต้องโชว์ = (CAT × เกจ) ที่มี item สี (และรู้ CAT) — เกจคือ pool เครื่องจริง
    def _gkey(a):
        return (a["cat"], _gnorm(a["gauge"]))
    color_groups = {_gkey(a) for a in items.values() if a["is_color"] and a["cat"]}
    n_color_nocat = sum(1 for a in items.values() if a["is_color"] and not a["cat"])

    # เซตรหัสงานสี (ใช้ mark item ในแผนว่า มีสี/ไม่มีสี)
    color_code_set = set()
    for a in items.values():
        if a["is_color"]:
            color_code_set.update(_codes(a["ora"]))

    # แผนปัจจุบัน + เครื่องว่าง ต่อกลุ่ม
    plan_grp = _plan_by_group()          # {(cat,gauge): {item: {week: mc}}}
    ava = plan_view.ava_by_week()        # {week(str): {"CAT|GUAGE": {"remain":..}}}

    # สัปดาห์ที่จะโชว์ = สัปดาห์ ≥ ปัจจุบัน ของ booking + แผน + เครื่องว่าง (ในกลุ่มที่โชว์)
    week_set = set()
    for a in items.values():
        if _gkey(a) in color_groups:
            week_set.update(w for w in a["weeks"] if w >= cur_week)
    for (cat, g) in color_groups:
        for wkm in plan_grp.get((cat, g), {}).values():
            week_set.update(w for w in wkm if w >= cur_week)
        key = f"{cat}|{g}"
        for wstr, keys in ava.items():
            if key in keys:
                wi = _to_int(wstr)
                if wi is not None and wi >= cur_week:
                    week_set.add(wi)
    weeks = sorted(w for w in week_set if w <= 53)  # ตัดสัปดาห์ sentinel (เช่น 99)

    def _gnum(g):
        """เกจ → ตัวเลขสำหรับเรียง (เกจที่ไม่ใช่เลขไปท้ายสุด)"""
        try:
            return (0, int(g))
        except (TypeError, ValueError):
            return (1, 0)

    groups_out = []
    for (cat, gauge) in color_groups:
        members = [a for a in items.values() if _gkey(a) == (cat, gauge)]

        # เครื่องว่าง (AVA) ตั้งต้นต่อสัปดาห์ของกลุ่มนี้ (ไม่แก้ — ใช้โชว์ในส่วน ②)
        key = f"{cat}|{gauge}"
        grp_ava = {}
        for w in weeks:
            slot = ava.get(str(w), {}).get(key)
            if slot is not None:
                grp_ava[str(w)] = int(slot.get("remain", 0))

        # ---- จัดคิวถักงานสีแบบ greedy ----
        # แกน = "สัปดาห์ถัก" (= ย้อม − LEAD) แล้วดึงเข้าสัปดาห์ว่างที่เร็วที่สุด ≤ กำหนด
        # deadline ใกล้ก่อน, หักเครื่องว่างไปเรื่อยๆ (work_ava สำเนา) ให้คิวไม่ทับเครื่องเกินจริง
        work_ava = dict(grp_ava)

        def _earliest_free(lo, hi):
            for w in weeks:
                if w < lo or (hi is not None and w > hi):
                    continue
                if work_ava.get(str(w), 0) > 0:
                    return w
            return None

        prepared = []
        for a in members:
            dye = a["dye_week"]
            deadline = (dye - LEAD_WEEKS) if dye is not None else None
            knit_kg = round(sum(a["weeks"].values()), 1)  # น้ำหนักถักรวมทุกงวด
            prepared.append({"a": a, "dye": dye, "deadline": deadline, "knit_kg": knit_kg})
        # deadline ใกล้สุดจัดก่อน (เร่งด่วนกว่า)
        prepared.sort(key=lambda x: (x["deadline"] if x["deadline"] is not None else 999,
                                     x["a"]["item"]))

        rows = []
        for p in prepared:
            a = p["a"]
            deadline = p["deadline"]
            target = None
            if deadline is None:
                status = "manual"          # ไม่รู้สัปดาห์ย้อม
            elif deadline < cur_week:
                status = "late"            # เลยกำหนดถักแล้ว — ทำได้เร็วสุดเท่าที่ว่าง
                target = _earliest_free(cur_week, None)
            else:
                target = _earliest_free(cur_week, deadline)
                if target is not None:
                    status = "fit"         # ดึงเข้าได้ทันกำหนด
                else:
                    status = "full"        # เครื่องเต็ม ≤ กำหนด — ต้องถอดงาน/เลื่อนเกินกำหนด
                    target = _earliest_free(cur_week, None)
            if target is not None:
                work_ava[str(target)] = work_ava.get(str(target), 0) - 1
            rows.append({
                "item": a["item"], "ora": a["ora"], "gauge": a["gauge"],
                "is_color": a["is_color"], "dye_week": a["dye_week"],
                "deadline": deadline, "place_week": target, "status": status,
                "knit_kg": p["knit_kg"],
            })
        # เรียงแสดงตามสัปดาห์ถักที่แนะนำ แล้ว deadline
        rows.sort(key=lambda x: (x["place_week"] if x["place_week"] is not None else 999,
                                 x["deadline"] if x["deadline"] is not None else 999,
                                 x["item"]))

        # แผนปัจจุบันของกลุ่มนี้ (item × สัปดาห์ = จำนวนเครื่อง) รวมทั้งมีสี/ไม่มีสี
        plan_rows = []
        for item_code, wkm in plan_grp.get((cat, gauge), {}).items():
            wcells = {str(w): int(round(mc)) for w, mc in wkm.items()
                      if w >= cur_week and round(mc)}
            if not wcells:
                continue
            plan_rows.append({
                "item": item_code,
                "is_color": item_code in color_code_set,
                "weeks": wcells,
            })
        # เรียง: งานสีขึ้นก่อน แล้วตามชื่อ (งานไม่มีสี = ตัวที่ถอดได้)
        plan_rows.sort(key=lambda x: (not x["is_color"], x["item"]))

        groups_out.append({
            "cat": cat, "gauge": gauge,
            "n_items": len(rows),
            "n_color": sum(1 for x in rows if x["is_color"]),
            "items": rows, "ava": grp_ava, "plan": plan_rows,
        })

    # เรียงตาม CAT → เกจ (น้อย→มาก)
    groups_out.sort(key=lambda g: (str(g["cat"]), _gnum(g["gauge"])))

    note = ""
    if not detail:
        note = "ยังไม่มีไฟล์ booking_final (DETAIL) — หา CAT ของ item ไม่ได้ จึงยังไม่มีกลุ่มให้โชว์"
    elif n_color_nocat:
        note = f"มี item สี {n_color_nocat} ตัวที่หา CAT ไม่ได้ (ไม่มีใน DETAIL) — ไม่ถูกจัดกลุ่ม"
    return {"order_color_name": oc_name, "current_week": cur_week,
            "lead_weeks": LEAD_WEEKS,
            "value_col": "BK_KP_WEIGHT", "weeks": weeks, "groups": groups_out,
            "note": note}

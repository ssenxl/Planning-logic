"""
cylinder_advisor.py — แนะนำว่าควรทำ "Change Cylinder" ให้ item ไหน
เพื่อปลดล็อกงานที่วางแผนไม่ได้/วางแล้วสาย

Planning.py ไม่ทำ Change Cylinder ให้เองอีกแล้ว (CYLINDER_CHANGE_ENABLED = False) —
Change Cylinder มีต้นทุนและเวลา จึงต้องให้คนตัดสินใจ โมดูลนี้ทำหน้าที่ "เสนอ" เท่านั้น

หลักการเดียวกับ outsource_advisor: Python คำนวณตัวเลขทั้งหมดเอง (กัน LLM มั่วตัวเลข)
แล้วส่ง shortlist ให้ LLM จัดอันดับ + อธิบายเหตุผลเป็นภาษาไทย

งานที่หยิบมาเสนอ:
  1. สาย       — PLAN_WEEK ช้ากว่า RDD_WEEK (งานถูกเลื่อนเพราะเครื่องไม่พอ)
  2. ค้างผลิต  — PLAN_QTY > 0 (เครื่องไม่พอจนผลิตไม่ครบ)

สำหรับแต่ละงาน หา "เกจต้นทาง" ที่มีเครื่องว่างจริงในสัปดาห์ที่ต้องการ (ภายใน Factory+MC_CAT
เดียวกัน) และเช็ค Cylinder สำรองจาก MasterMC[Spare part] — ถ้าไม่มีแหล่งหรือ Cylinder หมด
ก็ยังแสดงพร้อมเหตุผล เพราะนั่นบอกว่าต้องไปซื้อ Cylinder เพิ่มตรงไหน
"""
import json
import sys
from datetime import datetime

import config
import cyl_approve
import plan_view

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน — ไฟล์อยู่ราก repo)

# ผลวิเคราะห์ล่าสุด (แยกตามชีท) — หน้า Change Cylinder เป็น modal พอปิดแล้ว state ฝั่ง React
# หายทั้งก้อน ถ้าไม่เก็บไว้ user ต้องกดวิเคราะห์ใหม่ทุกครั้งที่เปิดหน้า (รอ LLM + จ่ายค่าเรียกซ้ำ)
# เก็บที่ backend ไม่ใช่ browser เพราะผลวิเคราะห์เป็นของทีม — คนละเครื่อง/คนละคนต้องเห็นชุดเดียวกัน
CACHE_FILE = config.OUTPUT_DIR / "cylinder_advice_cache.json"

_W_LATE = 10.0        # ต่อสัปดาห์ที่สาย
_W_PENDING = 1.0      # ต่อ 1000 กก. ค้างผลิต
_W_FEASIBLE = 15.0    # โบนัสถ้าทำ Change Cylinder ได้จริง (มี source + spare)
_MAX_SHORTLIST = 20
# job setup เต็ม → มองหาสัปดาห์ถัดไปได้ไกลสุดกี่สัปดาห์ (ไกลกว่านี้เปลี่ยนไปงานก็สายอยู่ดี)
_JOB_LOOKAHEAD = 8


def _to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _week_of(v):
    """RDD_WEEK/FG_WEEK เก็บเป็น YYYYWW (เช่น 202629 = ปี 2026 สัปดาห์ 29) → คืนเลขสัปดาห์
    ค่าที่เป็นเลขสัปดาห์ล้วน (เช่น 32) คืนตามเดิม"""
    n = _to_int(v)
    if n is None:
        return None
    s = str(n)
    return int(s[4:]) if len(s) >= 5 else n


def _year_of(v):
    """ปีของค่า YYYYWW — เลขสัปดาห์ล้วนไม่มีปีให้ (คืน None)"""
    n = _to_int(v)
    if n is None:
        return None
    s = str(n)
    return int(s[:4]) if len(s) >= 5 else None


def _iso_weeks_in_year(y: int) -> int:
    """52 หรือ 53 สัปดาห์ — ใช้นับระยะห่างสัปดาห์ข้ามปี ไม่ใช่บวก 52 ตายตัว"""
    import datetime
    return int(datetime.date(y, 12, 28).isocalendar()[1])


def _week_delta(a_y, a_w, b_y, b_w) -> int:
    """a − b เป็นจำนวนสัปดาห์ (บวก = a มาหลัง b) ; ปีไหนไม่รู้ → เทียบเลขสัปดาห์ล้วนเหมือนเดิม

    จำเป็นเพราะแผนคร่อมปีได้: งานที่วางไว้ W2 ของปีถัดไปต้องนับว่าเลยเดดไลน์ W46 ของปีนี้
    ไม่ใช่ 2 − 46 = ติดลบ (= ยังทัน)"""
    if not a_y or not b_y:
        return int(a_w) - int(b_w)
    base = min(int(a_y), int(b_y))

    def _abs(y, w):
        n = int(w)
        for k in range(base, int(y)):
            n += _iso_weeks_in_year(k)
        return n

    return _abs(a_y, a_w) - _abs(b_y, b_w)


def _to_num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _gnorm(g):
    """normalize เกจ: 28.0 / '28 ' → '28'"""
    s = str(g).strip().upper().replace("GAUGE", "").replace("G", "")
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


# การนับ Cylinder สำรองอยู่ที่ cyl_approve ที่เดียว — ฝั่งเสนอ (ไฟล์นี้) กับฝั่งบันทึกต้องเห็น
# ยอดเดียวกันเป๊ะ ไม่งั้นหน้าเว็บบอกว่าเหลือ 2 แต่บันทึกได้ 16
_catnorm = cyl_approve.catnorm


def _col_index(columns, *names):
    up = [str(c).strip().upper() for c in columns]
    for n in names:
        if n in up:
            return up.index(n)
    return -1


def _remain_live_by_week(cols, rows, ava) -> dict:
    """เครื่องว่างจริงหลังหักงานที่แผนจองไปแล้ว → {week(str): {"CAT|GUAGE": remain}}

    ava.remain หักเฉพาะ booking; เครื่องที่แผนจองเพิ่ม = ACTUAL_MC − MC_BOOKING
    (เครื่อง booking ถูกหักจาก remain ไปแล้ว จึงไม่หักซ้ำ)

    ⚠️ ต้องนับ "ต่อบล็อก" ไม่ใช่ต่อแถว — 1 บล็อก = (สัปดาห์, item, กลุ่มเครื่อง, เกจ) ใช้เครื่อง
    ชุดเดียว แต่ Planning เขียนเป็นหลายแถว (แถวละใบสั่ง) โดยแปะ ACTUAL_MC/MC_BOOKING ของ
    ทั้งบล็อกซ้ำไว้ทุกแถว → บวกทุกแถวจะได้เครื่องที่แผนใช้เกินจริง แล้วเห็นเกจนั้น "ตัน"
    ทั้งที่ยังว่าง (วัดกับแผนจริง: รวมทั้งแผน 1766 → 1603 ตัว ; W38 DOUBLE-30 G22 73 → 46)
    max คือสูตรเดียวกับที่ Planning ใช้อ่านจำนวนเครื่องของลูกโซ่ และที่ agg["machines"]
    ด้านล่างใช้อยู่แล้ว
    """
    i_week = _col_index(cols, "PLAN_WEEK")
    i_cat = _col_index(cols, "CAT")
    i_gauge = _col_index(cols, "MC_GUAGE", "GUAGE")
    i_amc = _col_index(cols, "ACTUAL_MC")
    i_bmc = _col_index(cols, "MC_BOOKING")
    i_item = _col_index(cols, "ITEM_CODE")
    i_mcg = _col_index(cols, "MC_GROUP")

    used: dict = {}
    if min(i_week, i_cat, i_gauge, i_amc) >= 0:
        # ยุบแถวของบล็อกเดียวกันก่อน → {(week, item, mc_group, gauge): [cat, max_amc, max_bmc]}
        blocks: dict = {}
        for r in rows:
            if i_week >= len(r):
                continue
            wk = _to_int(r[i_week])
            if wk is None:
                continue
            g = _gnorm(r[i_gauge])
            bkey = (
                wk,
                str(r[i_item]).strip().upper() if 0 <= i_item < len(r) else "",
                str(r[i_mcg]).strip().upper() if 0 <= i_mcg < len(r) else "",
                g,
            )
            amc = _to_num(r[i_amc]) if i_amc < len(r) else 0
            bmc = _to_num(r[i_bmc]) if 0 <= i_bmc < len(r) else 0
            b = blocks.get(bkey)
            if b is None:
                blocks[bkey] = [f"{str(r[i_cat]).strip()}|{g}", amc, bmc]
            else:
                b[1] = max(b[1], amc)
                b[2] = max(b[2], bmc)

        for (wk, _item, _mcg, _g), (key, amc, bmc) in blocks.items():
            net = max(0, int(amc) - int(bmc))
            if net:
                used.setdefault(str(wk), {})[key] = used.get(str(wk), {}).get(key, 0) + net

    out: dict = {}
    for wk, slots in ava.items():
        for key, slot in slots.items():
            out.setdefault(wk, {})[key] = int(slot.get("remain", 0)) - used.get(wk, {}).get(key, 0)
    return out


def _earliest_ready_week() -> int:
    """สัปดาห์แรกที่ "เครื่องที่เปลี่ยน Cylinder แล้ว" พร้อมผลิตได้

    เปลี่ยน Cylinder ได้เร็วสุดที่สัปดาห์ปัจจุบัน+2 (สองสัปดาห์แรกถูก freeze) แล้วเครื่องพร้อมใช้
    สัปดาห์ถัดไป → เสนอ Change Cylinder ให้สัปดาห์ที่เร็วกว่านี้ไม่ได้ ทำจริงไม่ทัน
    (กติกาเดียวกับที่หน้าเว็บใช้ตอนติ๊ก — อยู่ที่ cyl_approve.timing() ที่เดียว)"""
    return int(cyl_approve.timing()["earliest_ready"])


def _bottleneck_week(remain_live, cat, gauge, upto_week, from_week):
    """สัปดาห์แรกที่เครื่องในเกจนี้หมด (remain ≤ 0) ในช่วงที่ยังแก้แผนได้ จนถึงสัปดาห์ที่งานถูกวางไว้
    = สัปดาห์ที่งานอยากผลิตแต่ไม่มีเครื่อง → เป็นสัปดาห์ที่ควรทำ Change Cylinder ให้
    ข้ามสัปดาห์ที่ผ่านไปแล้ว/freeze เพราะทำ Change Cylinder ย้อนหลังไม่ได้
    คืน None ถ้าไม่มีสัปดาห์ไหนเครื่องหมดเลย (แปลว่าไม่ใช่ปัญหาเครื่อง เช่น รอด้าย)"""
    key = f"{cat}|{gauge}"
    weeks = sorted(int(w) for w in remain_live if str(w).isdigit())
    for w in weeks:
        if w < from_week:
            continue
        if upto_week is not None and w > upto_week:
            break
        slot = remain_live.get(str(w), {})
        if key in slot and slot[key] <= 0:
            return w
    return None


def _find_sources(remain_live, factory_cats, week, cat, target_gauge) -> list:
    """เกจอื่นใน MC_CAT เดียวกันที่ยังมีเครื่องว่างในสัปดาห์นั้น → [{"gauge","free"}]
    เรียงจากว่างมากสุด (เปลี่ยนแล้วกระทบงานอื่นน้อยสุด)"""
    out = []
    for key, free in remain_live.get(str(week), {}).items():
        k_cat, _, k_g = key.partition("|")
        if k_cat.strip().upper() != cat.strip().upper() or k_g == target_gauge:
            continue
        if k_cat not in factory_cats:
            continue
        if free > 0:
            out.append({"gauge": k_g, "free": int(free)})
    out.sort(key=lambda s: s["free"], reverse=True)
    return out


def _yarn_week_by_item(sheet) -> dict:
    """สัปดาห์ที่ด้ายเข้าครบของแต่ละ item → {ITEM: week}

    เอาบล็อกที่ด้ายมาครบ "เร็วที่สุด" ของ item นั้น — item เดียวมีหลายใบ ถ้ามีสักใบที่ด้าย
    พร้อมแล้ว การเปลี่ยน Cylinder ก็มีของให้ทอทันที (ไม่ต้องรอใบที่ด้ายมาช้าสุด)

    ⚠️ ชีทแผนไม่มีคอลัมน์ PO_IN_DATE เลย (มีแต่ DATE_IN ซึ่งคือ "วันรับ order" ห้ามใช้แทน) —
    วันด้ายเข้าอยู่ในชีท DETAIL ของ booking เท่านั้น จึงต้องดึงผ่าน plan_view
    """
    out: dict = {}
    try:
        blocks = plan_view.booking_items_by_week(sheet)
    except Exception as e:
        print(f"[CYL ADVISE] อ่านวันด้ายเข้าจาก booking ไม่ได้ ({e}) → ไม่เช็ค POIN DATE")
        return out
    for b in blocks or []:
        item = str(b.get("item", "")).strip().upper()
        d = str(b.get("po_in_full") or b.get("po_in") or "").strip()
        if not item or not d:
            continue
        wk = cyl_approve.week_of_date(d)
        if wk is None:
            continue
        if item not in out or wk < out[item]:
            out[item] = wk
    return out


def _job_slots_by_week(sheet) -> dict:
    """job setup ที่ยังว่าง ต่อ (สัปดาห์, ประเภท) → {(week:int, TYPE): {cap, used, left}}

    เปลี่ยน Cylinder แล้วเอางานลงเครื่องนั้น = "ตั้งเครื่องใหม่" กินโควตา job ของสัปดาห์ที่
    เครื่องพร้อมใช้ (PHET DOUBLE 33 · PHET SINGLE 44 · OM 13 job/สัปดาห์) — เสนอเปลี่ยนให้
    พร้อม W38 ทั้งที่ W38 job เต็มแล้ว = ทำไม่ได้จริง

    ใช้ชุดตัวเลขเดียวกับแถบ "Set up Job/Week" บน Gantt: old + bookingNew + NEW_MC ของแผน
    """
    try:
        load = plan_view.load_by_week(sheet)
        plan_new = plan_view._plan_new_mc_by_type(sheet)
    except Exception as e:
        print(f"[CYL ADVISE] อ่านโควตา job setup ไม่ได้ ({e}) → ไม่เช็ค job/สัปดาห์")
        return {}
    # สัปดาห์ที่ผ่านไปแล้วไม่มี CAPACITY ในชีท REMAINING_JOBS → ใช้ค่าสูงสุดที่เคยเห็นของประเภทนั้น
    # (กติกาเดียวกับ capOfType() ฝั่งหน้าเว็บ — โควตาคงที่ 13/33/44 อยู่แล้ว)
    cap_by_type: dict = {}
    for slots in load.values():
        for t, v in (slots or {}).items():
            c = _to_int(v.get("cap"))
            if c:
                cap_by_type[t] = max(cap_by_type.get(t, 0), c)
    out: dict = {}
    for wk, slots in load.items():
        w = _to_int(wk)
        if w is None:
            continue
        for t, v in (slots or {}).items():
            cap = _to_int(v.get("cap")) or cap_by_type.get(t)
            if not cap:
                continue
            used = (int(v.get("old") or 0) + int(v.get("bookingNew") or 0)
                    + int(plan_new.get((str(wk), t), 0)))
            out[(w, t)] = {"cap": int(cap), "used": used, "left": max(0, int(cap) - used)}
    return out


def _limits() -> dict:
    """เพดานที่ผู้ใช้ตั้งไว้ที่หน้า "ตั้งค่าระบบ" — หน้าเว็บใช้อธิบายกติกาด้วยเลขชุดนี้

    ⚠️ ห้ามให้ jsx เขียนเลขตรึงไว้เอง (เดิม "2 เครื่อง/สัปดาห์" กับ "33 · 44 · 13") —
    ผู้ใช้เปลี่ยนค่าบนหน้าตั้งค่าระบบแล้วคำอธิบายบนจอจะบอกเลขที่ไม่ใช่เลขที่ระบบใช้จริง
    """
    return {
        "cylinder_per_week": cyl_approve.change_limit_per_week(),
        "job_cap": plan_config.job_cap(),
        "freeze_weeks": cyl_approve.freeze_weeks(),
    }


def _is_pinned(remark) -> bool:
    """แถวงานที่ user ตรึงไว้ในรอบ Replan (Replark.py แปะ REMARK ขึ้นต้น 🔐)

    ตรึงไว้ = รอบ replan ถัดไปจะไม่จัดงานนี้ใหม่อยู่แล้ว → เปลี่ยน Cylinder ให้ก็ไม่ทำให้เร็วขึ้น
    จึงไม่เอามาเสนอ (ไม่งั้นรายการแนะนำเต็มไปด้วยงานที่ทำอะไรกับมันไม่ได้)"""
    return str(remark or "").strip().startswith("🔐")


def gather_candidates(sheet: str = None) -> dict:
    """งานที่ควรพิจารณา Change Cylinder + ตัวเลือกการเปลี่ยน (deterministic — ไม่พึ่ง LLM)

    sheet = ชีทที่ user เปิดอยู่ (PLAN / REPLAN) — ต้องส่งต่อให้ plan_view ทุกจุด ไม่งั้นโหมด
    Replan จะวิเคราะห์จากแถวแผนเก่าและ capacity เก่า (เครื่องที่ replan คืนให้ว่างไม่ถูกนับ)
    แล้วรายงานว่าเกจตันทั้งที่ว่างอยู่
    """
    grid = plan_view.read_grid(sheet)
    cols = grid.get("columns", [])
    rows = grid.get("rows", [])
    plan_name = grid.get("name")
    sheet_name = grid.get("sheet") or sheet or ""
    # ไฟล์รุ่นใหม่ชีท PLAN คือแผน replan — ตัดสินจากรุ่นไฟล์ ไม่ใช่ชื่อชีทอย่างเดียว
    replan_mode = plan_view._is_replan(sheet_name)
    if not cols or not rows:
        return {"plan_name": plan_name, "candidates": [], "limits": _limits(),
                "note": "ยังไม่มีข้อมูลแผนผลิต"}

    i_item = _col_index(cols, "ITEM_CODE")
    i_week = _col_index(cols, "PLAN_WEEK")
    i_rdd = _col_index(cols, "RDD_WEEK", "FG_WEEK", "TARGET_KNIT")
    # ปีของสัปดาห์ผลิต/เดดไลน์ — ต้องมีคู่กับเลขสัปดาห์ ไม่งั้นเทียบสาย/ไม่สายข้ามปีไม่ได้
    i_year = _col_index(cols, "PLAN_YEAR")
    i_tgy = _col_index(cols, "TARGET_YEAR")
    i_cat = _col_index(cols, "CAT")
    i_gauge = _col_index(cols, "MC_GUAGE", "GUAGE")
    i_mcg = _col_index(cols, "MC_GROUP")
    i_fac = _col_index(cols, "FACTORY_TYPE")
    i_pend = _col_index(cols, "PLAN_QTY")
    i_amc = _col_index(cols, "ACTUAL_MC")
    i_cust = _col_index(cols, "CUSTOMER")
    i_sc = _col_index(cols, "SC_SO_NO")
    i_rem = _col_index(cols, "REMARK")
    i_core = _col_index(cols, "IS_CORE_ITEM")
    if min(i_item, i_week, i_cat, i_gauge) < 0:
        return {"plan_name": plan_name, "candidates": [], "limits": _limits(),
                "note": "ไฟล์แผนไม่มีคอลัมน์ ITEM_CODE/PLAN_WEEK/CAT/MC_GUAGE ที่จำเป็น"}

    ava = plan_view.ava_by_week(sheet)
    remain_live = _remain_live_by_week(cols, rows, ava)
    # รายการที่ติ๊กไว้ก่อนระบบเปลี่ยนมาหักยอดลงไฟล์ ต้องถูกหักครั้งแรกก่อนอ่านยอด (no-op หลังจากนั้น)
    try:
        cyl_approve.sync_spare_file()
    except Exception as e:
        print(f"[CYL ADVISE] sync ยอดสำรองลงไฟล์ไม่ได้ ({e}) → ยอดที่แสดงอาจสูงกว่าจริง")
    spare_map = cyl_approve.spare_map()
    yarn_week_by_item = _yarn_week_by_item(sheet)
    job_slots = _job_slots_by_week(sheet)

    # MC_CAT ที่มีอยู่จริงต่อ factory (กันเสนอเปลี่ยนข้ามโรงงาน)
    factory_cats: dict = {}
    for r in rows:
        if 0 <= i_fac < len(r) and 0 <= i_cat < len(r):
            factory_cats.setdefault(str(r[i_fac]).strip().upper(), set()).add(str(r[i_cat]).strip())

    # PLAN_QTY = ยอดคงเหลือ "หลังผลิตแถวนั้น" (ไล่ลงเรื่อยๆ) → ของค้างจริงคือค่าของแถว
    # สัปดาห์สุดท้ายของแต่ละ (item, SC) เท่านั้น ถ้ารวมทุกแถวจะได้ยอดที่พองเกินจริงหลายเท่า
    last_row: dict = {}
    for r in rows:
        if i_item >= len(r) or i_week >= len(r):
            continue
        wk = _to_int(r[i_week])
        if wk is None:
            continue
        k = (str(r[i_item]).strip().upper(),
             str(r[i_sc]).strip() if 0 <= i_sc < len(r) else "")
        if k not in last_row or wk > last_row[k][0]:
            last_row[k] = (wk, r)
    pending_by_key = {}
    for k, (_wk, r) in last_row.items():
        pend = _to_num(r[i_pend]) if 0 <= i_pend < len(r) else 0.0
        if pend > 0:
            pending_by_key[k] = pend

    agg: dict = {}
    pinned_skipped = 0
    core_skipped = 0
    for r in rows:
        if i_item >= len(r):
            continue
        item = str(r[i_item]).strip().upper()
        if not item:
            continue
        # โหมด Replan: งานที่ตรึงไว้จะไม่ถูกจัดใหม่ → เปลี่ยน Cylinder ให้ก็ไม่ช่วย ข้ามไป
        if replan_mode and 0 <= i_rem < len(r) and _is_pinned(r[i_rem]):
            pinned_skipped += 1
            continue
        # core item ไม่ยึด TARGET_KNIT (ผลิตเป็นรอบตาม trigger สต๊อก) → "สาย" ของ core ไม่มีความหมาย
        # ตั้งแต่มีการเติม FG/Target ให้แถว core ถ้าไม่กันไว้ รายการแนะนำจะเต็มไปด้วยงาน core
        if 0 <= i_core < len(r) and str(r[i_core]).strip():
            core_skipped += 1
            continue
        sc = str(r[i_sc]).strip() if 0 <= i_sc < len(r) else ""
        week = _to_int(r[i_week]) if i_week < len(r) else None
        rdd = _week_of(r[i_rdd]) if 0 <= i_rdd < len(r) else None
        # ปีของเดดไลน์: ถ้าคอลัมน์ที่ใช้เป็น YYYYWW ก็ถอดปีจากค่าเองได้ ไม่งั้นใช้ TARGET_YEAR
        rdd_y = (_year_of(r[i_rdd]) if 0 <= i_rdd < len(r) else None) \
            or (_to_int(r[i_tgy]) if 0 <= i_tgy < len(r) else None)
        week_y = _to_int(r[i_year]) if 0 <= i_year < len(r) else None
        cat = str(r[i_cat]).strip()
        gauge = _gnorm(r[i_gauge])
        fac = str(r[i_fac]).strip().upper() if 0 <= i_fac < len(r) else ""
        mcg = str(r[i_mcg]).strip() if 0 <= i_mcg < len(r) else ""
        cust = str(r[i_cust]).strip() if 0 <= i_cust < len(r) else ""
        amc = int(_to_num(r[i_amc])) if 0 <= i_amc < len(r) else 0

        late = (
            max(0, _week_delta(week_y, week, rdd_y, rdd))
            if (week is not None and rdd is not None)
            else 0
        )
        pend = pending_by_key.get((item, sc), 0.0)
        if late <= 0 and pend <= 0:
            continue  # ส่งทัน + ผลิตครบ → ไม่ต้องทำ Change Cylinder

        k = (item, cat, gauge, fac)
        a = agg.setdefault(k, {
            "item_code": item, "cat": cat, "gauge": gauge, "factory": fac,
            "mc_group": mcg, "customer": cust, "late_weeks": 0, "pending_qty": 0.0,
            "machines": 0, "plan_week": week, "plan_week_last": week,
            "rdd_week": rdd, "scs": set(),
        })
        if week is not None and (a["plan_week_last"] is None or week > a["plan_week_last"]):
            a["plan_week_last"] = week
        a["late_weeks"] = max(a["late_weeks"], late)
        if (item, sc) not in a["scs"]:
            a["pending_qty"] += pend       # นับของค้างครั้งเดียวต่อ SC
            a["scs"].add((item, sc))
        a["machines"] = max(a["machines"], amc)
        if week is not None and (a["plan_week"] is None or week < a["plan_week"]):
            a["plan_week"] = week
        if rdd is not None and a["rdd_week"] is None:
            a["rdd_week"] = rdd
        if not a["customer"] and cust:
            a["customer"] = cust

    earliest_ready = _earliest_ready_week()
    # เครื่องที่ติ๊กอนุมัติไปแล้ว — ยอดที่ ava ให้มาก็รวมเครื่องพวกนี้แล้ว (plan_view บวกทับให้)
    # ตรงนี้ใช้แค่บอกบนหน้าเว็บว่าเคยติ๊กไปกี่เครื่อง จะได้ไม่ติ๊กซ้ำโดยไม่รู้ตัว
    approved_by_key = cyl_approve.approved_by_target()
    # โควตาแรงงานที่ถูกใช้ไปแล้วในแต่ละสัปดาห์ (เพดานของคนที่ลงมือเปลี่ยน — ตั้งที่หน้าตั้งค่าระบบ)
    quota_used = cyl_approve.approved_by_week_group()
    quota_limit = cyl_approve.change_limit_per_week()   # อ่านครั้งเดียวต่อรอบ ใช้ทุกแถวข้างล่าง

    candidates = []
    for a in agg.values():
        # สัปดาห์ที่ควรทำ Change Cylinder = สัปดาห์แรกที่เครื่องเกจนี้หมด ในช่วงที่งานวิ่งอยู่
        # ถ้าไม่มีสัปดาห์ไหนเครื่องหมดเลย → งานสาย/ค้างด้วยเหตุอื่น (เช่น รอด้าย) ไม่ใช่เรื่อง Cylinder
        need_week = _bottleneck_week(
            remain_live, a["cat"], a["gauge"], a["plan_week_last"], earliest_ready
        )
        if need_week is None:
            continue
        # ด้ายยังไม่เข้า = เปลี่ยนเครื่องให้ก็ทอไม่ได้ → เลื่อนไป "เปลี่ยนรอไว้" ให้เครื่องพร้อม
        # สัปดาห์ที่ด้ายเข้าครบพอดี (ด้ายเข้า W30 → สั่งเปลี่ยน W29 พร้อมใช้ W30)
        # ไม่ตัดทิ้ง เพราะการเปลี่ยนล่วงหน้ายังมีประโยชน์ แค่ต้องไปอยู่สัปดาห์ที่ถูก
        yarn_week = yarn_week_by_item.get(a["item_code"])
        yarn_shifted = yarn_week is not None and yarn_week > need_week
        if yarn_shifted:
            need_week = max(yarn_week, earliest_ready)
        need_mc = max(1, a["machines"])
        cats_in_fac = factory_cats.get(a["factory"], set())

        # job setup ของสัปดาห์ที่เครื่องจะพร้อม — ตั้งเครื่องใหม่ 1 เครื่อง = 1 job
        # เต็มแล้ว → เลื่อนไปสัปดาห์ถัดไปที่ยังมี job ว่าง "และ" มีเครื่องต้นทางให้ถอด
        # (มองไปข้างหน้าไม่เกิน _JOB_LOOKAHEAD สัปดาห์ — ไกลกว่านั้นเปลี่ยนไปงานก็สายอยู่ดี)
        job_type = plan_view._classify_type(a["factory"], a["cat"])
        job_shifted = False
        if job_type:
            weeks_ahead = [w for w in sorted(int(x) for x in remain_live if str(x).isdigit())
                           if w >= need_week][:_JOB_LOOKAHEAD]
            for w in weeks_ahead:
                slot = job_slots.get((w, job_type))
                if slot and slot["left"] <= 0:
                    continue                      # สัปดาห์นี้ตั้งเครื่องใหม่ไม่ได้แล้ว
                if not _find_sources(remain_live, cats_in_fac, w, a["cat"], a["gauge"]):
                    continue                      # ไม่มีเครื่องว่างให้ถอดมาเปลี่ยนในสัปดาห์นี้
                job_shifted = w != need_week
                need_week = w
                break

        sources = _find_sources(remain_live, cats_in_fac, need_week, a["cat"], a["gauge"])
        job_slot = job_slots.get((need_week, job_type)) if job_type else None
        job_left = job_slot["left"] if job_slot else None      # None = ไม่รู้โควตา → ไม่จำกัด
        spare = cyl_approve.spare_total(a["factory"], a["cat"], a["gauge"])
        # เครื่องพร้อมใช้ที่ need_week → ต้องลงมือเปลี่ยนสัปดาห์ก่อนหน้าบนแกนปฏิทินจริง
        set_week = cyl_approve.set_week_of(need_week)
        approved_mc = approved_by_key.get(
            (a["factory"], _catnorm(a["cat"]), a["gauge"]), 0)

        # ยอดในไฟล์ MasterMC = คงเหลือจริงอยู่แล้ว (ติ๊กแล้วหักลงไฟล์ทันที ยกเลิกแล้วคืนให้)
        # ห้ามลบ approved ซ้ำอีกชั้น ไม่งั้นหักสองรอบ — approved_mc ใช้แค่บอกว่าเคยติ๊กไปเท่าไร
        spare_left = max(0, spare)
        # โควตาแรงงานของสัปดาห์ที่ต้องลงมือเปลี่ยน — ต่างจากสำรองตรงที่สัปดาห์หน้าเต็มใหม่
        q_group = cyl_approve.quota_group(a["factory"], a["cat"])
        q_used = quota_used.get((set_week, q_group), 0) if q_group else 0
        q_left = max(0, quota_limit - q_used) if q_group else quota_limit

        if not sources and spare_left <= 0:
            blocker = (f"ไม่มีเครื่องว่างใน Guage อื่นของ {a['cat']} ที่ W{need_week} "
                       f"และไม่มี Cylinder สำรอง Guage {a['gauge']} เหลือ")
        elif not sources:
            blocker = (f"มี Cylinder สำรอง Guage {a['gauge']} เหลือ {spare_left} อัน "
                       f"แต่ไม่มีเครื่องว่างใน Guage อื่นให้ถอดมาเปลี่ยนที่ W{need_week}")
        elif spare_left <= 0:
            blocker = (f"มีเครื่องว่างให้เปลี่ยน แต่ Cylinder สำรอง Guage {a['gauge']} หมดแล้ว"
                       + (f" (ติ๊กใช้ไปแล้ว {approved_mc} อัน — ยกเลิกรายการเดิมเพื่อคืนของได้)"
                          if approved_mc else " (Total Spare ในไฟล์ = 0)")
                       + " → ต้องซื้อ Cylinder เพิ่ม")
        elif q_left <= 0:
            blocker = (f"โควตาเปลี่ยน Cylinder ของ {q_group} ที่ W{set_week} เต็มแล้ว "
                       f"({q_used}/{quota_limit} เครื่อง) "
                       f"— ต้องเลื่อนไปสั่งเปลี่ยนสัปดาห์อื่น")
        elif job_left is not None and job_left <= 0:
            blocker = (f"job setup ของ {job_type} ที่ W{need_week} เต็มแล้ว "
                       f"({job_slot['used']}/{job_slot['cap']} job) — Setup ใหม่ไม่ได้ "
                       f"และอีก {_JOB_LOOKAHEAD} สัปดาห์ข้างหน้าก็ไม่มีสัปดาห์ไหนว่างพร้อมเครื่องต้นทาง")
        else:
            blocker = ""

        feasible = (bool(sources) and spare_left > 0 and q_left > 0
                    and (job_left is None or job_left > 0))
        # ติ๊กได้ไม่เกิน: เครื่องว่างเกจต้นทาง / Cylinder สำรองที่เหลือ / โควตาเปลี่ยนของสัปดาห์ /
        # job setup ที่ยังตั้งเครื่องใหม่ได้ในสัปดาห์ที่เครื่องพร้อม
        max_mc = (min(need_mc, spare_left, sources[0]["free"], q_left,
                      job_left if job_left is not None else need_mc)
                  if feasible else 0)
        src_gauge = sources[0]["gauge"] if sources else ""
        score = (a["late_weeks"] * _W_LATE
                 + (a["pending_qty"] / 1000.0) * _W_PENDING
                 + (_W_FEASIBLE if feasible else 0.0))

        # ประโยคปฏิบัติการ — Python เขียนเอง ไม่ให้ LLM ประกอบตัวเลข/เกจเองแล้วมั่ว
        if feasible:
            src_txt = " หรือ ".join(f"Guage {s['gauge']}" for s in sources[:2])
            action = (f"Change Cylinder {max_mc} เครื่อง จาก {src_txt} → Guage {a['gauge']} "
                      f"ที่ {a['factory']} {a['cat']} — สั่งเปลี่ยน W{set_week} "
                      f"เครื่องพร้อมใช้ W{need_week}"
                      + (f" (เปลี่ยนรอไว้ให้ตรงกับด้ายเข้าครบ W{yarn_week})" if yarn_shifted else "")
                      + (" (เลื่อนสัปดาห์เพราะ job setup สัปดาห์เดิมเต็ม)" if job_shifted else ""))
        else:
            action = f"ยังเปลี่ยนไม่ได้ที่ W{need_week} — {blocker}"

        candidates.append({
            "item_code": a["item_code"],
            "customer": a["customer"],
            "cat": a["cat"],
            "gauge": a["gauge"],             # = เกจปลายทาง (เกจที่งานนี้ต้องใช้)
            "factory": a["factory"],
            "mc_group": a["mc_group"],
            "late_weeks": a["late_weeks"],
            "pending_qty": round(a["pending_qty"], 1),
            "plan_week": a["plan_week"],
            "rdd_week": a["rdd_week"],
            "need_week": need_week,          # = ready_week (สัปดาห์ที่ต้องใช้เครื่อง)
            "set_week": set_week,            # สัปดาห์ที่ต้องลงมือเปลี่ยน Cylinder
            "yarn_week": yarn_week,          # สัปดาห์ที่ด้ายเข้าครบ (None = ไม่รู้ ไม่บล็อก)
            "yarn_shifted": yarn_shifted,    # True = เลื่อนสัปดาห์เปลี่ยนให้รอด้าย
            "job_type": job_type or "",      # ประเภทโควตา job setup (OM/PHET_DOUBLE/PHET_SINGLE)
            "job_cap": job_slot["cap"] if job_slot else None,
            "job_used": job_slot["used"] if job_slot else None,
            "job_left": job_left,            # job setup ที่ยังว่างใน W{need_week} (None = ไม่รู้)
            "job_shifted": job_shifted,      # True = เลื่อนสัปดาห์เพราะ job สัปดาห์เดิมเต็ม
            "src_gauge": src_gauge,          # เกจต้นทางที่แนะนำ (ว่างมากสุด)
            "machines_needed": need_mc,
            "machines_max": max_mc,          # ติ๊กได้มากสุดเท่านี้ (สำรอง/เครื่องว่างจำกัด)
            "spare_available": spare,
            "spare_left": spare_left,
            "approved_machines": approved_mc,   # ติ๊กไปแล้วกี่เครื่องสำหรับเกจนี้
            "quota_group": q_group or "",       # กลุ่มโควตาแรงงาน (PHET_DOUBLE/PHET_SINGLE/OMNOI)
            "quota_limit": quota_limit,
            "quota_used": q_used,               # ติ๊กบันทึกไปแล้วกี่เครื่องใน W{set_week} ของกลุ่มนี้
            "quota_left": q_left,
            "sources": sources[:3],          # เกจต้นทางที่ถอดเครื่องมาเปลี่ยนได้
            "feasible": feasible,
            "blocker": blocker,
            "action": action,
            "score": round(score, 1),
        })

    # สรุป Cylinder สำรอง "ทุกเกจที่มีสำรอง" (ไม่ใช่เฉพาะแถวที่ AI เสนอ)
    # ยอดในไฟล์ MasterMC = คงเหลือจริง (ติ๊กแล้วหักลงไฟล์ทันที) → left = ไฟล์
    # "มีทั้งหมด" ที่โชว์ = คงเหลือ + ที่ติ๊กไปแล้ว เพื่อให้เห็นยอดตั้งต้นก่อนเปลี่ยน Cylinder
    # (Cylinder เก่าที่ถอดออกไม่นับเป็นสำรองเพิ่มของเกจต้นทาง — ยืนยันจากหน้างาน)
    spare_summary = []
    for (s_fac, s_cat, s_g), s_left in spare_map.items():
        used = approved_by_key.get((s_fac, s_cat, s_g), 0)
        if int(s_left) <= 0 and used <= 0:
            continue
        spare_summary.append({
            "factory": s_fac, "cat": s_cat, "gauge": s_g,
            "total": int(s_left) + int(used), "used": int(used),
            "left": int(s_left),
        })
    spare_summary.sort(key=lambda s: (s["factory"], s["cat"],
                                      int(s["gauge"]) if s["gauge"].isdigit() else 999))

    candidates.sort(key=lambda c: c["score"], reverse=True)
    note = ""
    if replan_mode and pinned_skipped:
        note = (f"โหมด Replan: ข้ามงานที่ตรึงไว้ (🔐) {pinned_skipped} แถว "
                "เพราะรอบ replan จะไม่จัดงานพวกนี้ใหม่อยู่แล้ว")
    if core_skipped:
        note = (note + " · " if note else "") + (
            f"ข้ามงาน core item {core_skipped} แถว (ผลิตเป็นรอบตามสต๊อก ไม่ได้ยึด Target Knit)")
    return {"plan_name": plan_name, "sheet": sheet_name,
            "candidates": candidates[:_MAX_SHORTLIST],
            "total_found": len(candidates),
            "timing": cyl_approve.timing(),
            "approvals": cyl_approve.list_approvals(),
            "spare_summary": spare_summary,
            "limits": _limits(),
            "note": note}


_SYSTEM_PROMPT = (
    "คุณเป็นผู้ช่วยวางแผนการผลิตผ้าทอ หน้าที่คือ 'จัดอันดับ' ว่าควรทำ Change Cylinder "
    "ให้ item ใดก่อน เพื่อปลดล็อกงานที่ส่งไม่ทันหรือผลิตไม่ครบเพราะเครื่องใน Guage นั้นหมด "
    "Change Cylinder คือถอดเครื่องที่ว่างใน Guage หนึ่ง มาเปลี่ยน Cylinder เป็นอีก Guage หนึ่ง (ภายใน MC_CAT และโรงงานเดียวกัน) "
    "แต่ละรายการมีฟิลด์ action ที่เขียนไว้แล้วว่าต้องทำอะไร — ห้ามแต่งวิธีทำใหม่ "
    "เรียกขนาดเข็มของเครื่องว่า 'Guage' เสมอ ห้ามเขียนคำนี้เป็นภาษาไทย "
    "\n\nกฎเหล็ก: ห้ามพูดถึงตัวเลข Guage สัปดาห์ หรือจำนวนเครื่องใดๆ ในเหตุผลของคุณเด็ดขาด "
    "เพราะระบบแสดง action กับตัวเลขให้ผู้ใช้เห็นอยู่แล้ว หน้าที่คุณคืออธิบายว่า 'ทำไมควรทำอันนี้ก่อน/หลัง' เท่านั้น "
    "เช่น 'สายมากที่สุดและมีของค้างเยอะ ทำได้ทันที' หรือ 'ยังทำไม่ได้เพราะ Cylinder สำรองหมด ต้องสั่งซื้อก่อน' "
    "\n\nเกณฑ์จัดอันดับ: งานที่ feasible=true มาก่อน feasible=false เสมอ; ในกลุ่มเดียวกันเรียงตามความเร่งด่วน "
    "(late_weeks = สายกี่สัปดาห์) และปริมาณค้างผลิต (pending_qty) "
    "ตอบกลับเป็น JSON เท่านั้น รูปแบบ: "
    '{"summary": "สรุปภาพรวม 1-2 ประโยค (ห้ามใส่ตัวเลข)", '
    '"ranking": [{"item_code": "...", "rank": 1, "reason": "เหตุผลไทยสั้นๆ ไม่มีตัวเลข"}]}'
)


def _call_llm(candidates: list) -> dict:
    """เรียก LLM จัดอันดับ + อธิบาย — โยน exception ถ้าเรียกไม่สำเร็จ (ให้ชั้นบน fallback)"""
    cfg = config.llm_config()
    if not cfg["api_key"]:
        raise RuntimeError("ยังไม่ได้ตั้งค่า OpenAI (OPENAI_API_KEY)")

    from openai import OpenAI
    kwargs = {"api_key": cfg["api_key"]}
    if cfg["base_url"]:
        kwargs["base_url"] = cfg["base_url"]
    client = OpenAI(**kwargs)
    user_msg = (
        "รายการงานที่ติดเครื่อง/ส่งไม่ทัน พร้อมตัวเลือก Change Cylinder (คำนวณตัวเลขมาแล้ว) "
        "— โปรดจัดอันดับว่าควรทำ Change Cylinder ให้ item ไหนก่อน:\n"
        + json.dumps(candidates, ensure_ascii=False)
    )
    # ไม่ส่ง temperature — โมเดลรุ่นใหม่ (เช่น gpt-5.x) รับเฉพาะค่า default และจะ error 400
    resp = client.chat.completions.create(
        model=cfg["model"],
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


# ---------------- cache ผลวิเคราะห์ล่าสุด ----------------
def _cache_key(sheet, fallback="") -> str:
    """คีย์ของ cache = ชื่อชีท — PLAN กับ REPLAN คนละแผน ผลวิเคราะห์ต้องไม่ทับกัน"""
    for v in (sheet, fallback):
        s = str(v or "").strip().upper().replace(" ", "_")
        if s:
            return s
    return "PLAN"


def _plan_stamp():
    """(ชื่อไฟล์แผน, เวลาที่แก้ไขล่าสุด) — รันแผนใหม่/กดบันทึกแผนแล้วค่าคู่นี้เปลี่ยน"""
    p = plan_view.latest_path()
    if p is None:
        return "", 0.0
    try:
        return p.name, round(p.stat().st_mtime, 3)
    except OSError:
        return p.name, 0.0


def _approvals_stamp() -> str:
    """ลายเซ็นของรายการที่ติ๊กไว้ — ติ๊กเพิ่ม/ยกเลิก/เพิ่มจำนวนเครื่อง ทำให้ค่านี้เปลี่ยน
    (เครื่องว่างเปลี่ยน → candidates/spare_left ที่คำนวณไว้ใช้ไม่ได้แล้ว)"""
    return "|".join(sorted(
        f"{str(a.get('id') or '').strip()}:{_to_int(a.get('machines')) or 0}"
        for a in cyl_approve.list_approvals()))


def _sig(sheet_key: str) -> dict:
    """ลายเซ็น "ผลนี้วิเคราะห์จากอะไร" — ใช้ตัดสินตอนเปิดหน้าว่าผลเก่ายังตรงกับความจริงไหม"""
    name, mtime = _plan_stamp()
    return {"plan_name": name, "plan_mtime": mtime,
            "sheet": sheet_key, "approvals": _approvals_stamp()}


def _stale_reasons(old: dict, new: dict) -> list:
    """อะไรเปลี่ยนไปหลังจากที่วิเคราะห์ไว้ (ว่าง = ผลเก่ายังใช้ได้)"""
    out = []
    if str(old.get("plan_name") or "") != str(new.get("plan_name") or ""):
        out.append(f"ไฟล์แผนเปลี่ยนเป็น {new.get('plan_name') or '-'}")
    elif float(old.get("plan_mtime") or 0) != float(new.get("plan_mtime") or 0):
        out.append("ไฟล์แผนถูกบันทึกใหม่หลังจากที่วิเคราะห์ไว้")
    if str(old.get("approvals") or "") != str(new.get("approvals") or ""):
        out.append("มีการติ๊ก/ยกเลิก Change Cylinder หลังจากที่วิเคราะห์ไว้")
    return out


def _read_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[CYL ADVISE] อ่าน {CACHE_FILE.name} ไม่ได้: {e}")
        return {}
    entries = data.get("entries") if isinstance(data, dict) else None
    return entries if isinstance(entries, dict) else {}


def _save_cache(sheet_key: str, result: dict, user: str = "") -> None:
    entries = _read_cache()
    entries[sheet_key] = {
        "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "by": str(user or "").strip(),
        "sig": _sig(sheet_key),
        "result": result,
    }
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(json.dumps({"entries": entries}, ensure_ascii=False, indent=2),
                              encoding="utf-8")
    except Exception as e:
        # วิเคราะห์สำเร็จไปแล้ว — เขียน cache ไม่ได้ต้องไม่ทำให้ทั้ง request ล้ม
        print(f"[CYL ADVISE] เขียน {CACHE_FILE.name} ไม่ได้: {e}")


def last_advice(sheet: str = None) -> dict:
    """ผลวิเคราะห์ล่าสุดที่เก็บไว้ (ไม่เรียก LLM ใหม่) — หน้าเว็บโหลดตอนเปิด modal

    ผลเก่าที่ไม่ตรงกับแผนปัจจุบันก็ยังส่งกลับไป (พร้อม stale=True) เพื่อให้หน้าเว็บมีอะไรให้ดู
    ระหว่างที่มันสั่งวิเคราะห์ใหม่ให้เอง ไม่ใช่จอว่างเปล่า
    """
    key = _cache_key(sheet)
    ent = _read_cache().get(key)
    if not isinstance(ent, dict) or not isinstance(ent.get("result"), dict):
        return {"cached": False, "sheet": key}
    reasons = _stale_reasons(ent.get("sig") or {}, _sig(key))
    res = dict(ent["result"])
    if reasons:
        # ผลวิเคราะห์เก่ายังมีค่าให้ดู แต่ "เครื่องที่ติ๊กย้ายเกจไปแล้ว" คือสถานะจริงของวันนี้
        # ต้องเป็นของสดเสมอ ไม่งั้น user เห็นรายการที่ยกเลิกไปแล้วค้างอยู่แล้วติ๊กซ้ำ
        res["approvals"] = cyl_approve.list_approvals()
        res["timing"] = cyl_approve.timing()
    return {"cached": True, "stale": bool(reasons),
            "stale_reason": " · ".join(reasons),
            "at": ent.get("at"), "by": ent.get("by", ""),
            "sheet": key, "data": res}


def advise(sheet: str = None, user: str = "") -> dict:
    """endpoint หลัก: รวบรวมงานที่ติด → ให้ LLM จัดอันดับ → รวมผล (แล้วเก็บ cache ไว้)

    sheet = ชีทที่เปิดอยู่ (PLAN/REPLAN) — ต้องวิเคราะห์แผนใบเดียวกับที่ user เห็นบนจอ"""
    base = gather_candidates(sheet)
    cands = base["candidates"]
    if not cands:
        out = {**base, "summary": "", "ai": False,
               "note": base.get("note") or "ไม่มีงานที่ต้อง Change Cylinder — แผนปัจจุบันไม่มีงานสายหรือค้างผลิต"}
        # เก็บผล "ไม่มีงานต้องเปลี่ยน" ด้วย — เป็นคำตอบเหมือนกัน ไม่ควรต้องกดวิเคราะห์ซ้ำเพื่อรู้
        _save_cache(_cache_key(sheet, base.get("sheet")), out, user)
        return out

    ai_ok = False
    summary = ""
    note = ""
    try:
        llm = _call_llm(cands)
        rank_map = {}
        for r in llm.get("ranking", []):
            code = str(r.get("item_code", "")).strip().upper()
            if code:
                rank_map[code] = r
        for c in cands:
            r = rank_map.get(c["item_code"])
            c["rank"] = r.get("rank") if r else None
            c["reason"] = r.get("reason", "") if r else ""
        cands.sort(key=lambda c: (c.get("rank") is None, c.get("rank") or 999))
        summary = str(llm.get("summary", "")).strip()
        ai_ok = True
    except Exception as e:
        # fallback: ใช้อันดับจากคะแนน deterministic ที่คำนวณไว้แล้ว
        for idx, c in enumerate(cands, 1):
            c["rank"] = idx
            c["reason"] = ""
        note = f"AI ไม่พร้อมใช้งาน — แสดงอันดับจากการคำนวณแทน ({e})"

    # หมายเหตุจากการรวบรวม (เช่น โหมด Replan ข้ามงานที่ตรึงไว้) ต้องไม่หายไปเพราะข้อความของ AI
    if base.get("note"):
        note = (note + " · " if note else "") + base["note"]
    out = {"plan_name": base["plan_name"], "sheet": base.get("sheet", ""),
           "candidates": cands,
           "total_found": base.get("total_found", len(cands)),
           "timing": base.get("timing", {}),
           "approvals": base.get("approvals", []),
           "spare_summary": base.get("spare_summary", []),
           "summary": summary, "ai": ai_ok, "note": note}
    _save_cache(_cache_key(sheet, base.get("sheet")), out, user)
    return out

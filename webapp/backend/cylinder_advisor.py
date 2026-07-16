"""
cylinder_advisor.py — แนะนำว่าควร "เปลี่ยนกระบอก (cylinder change)" ให้ item ไหน
เพื่อปลดล็อกงานที่วางแผนไม่ได้/วางแล้วสาย

Planning.py ไม่เปลี่ยนกระบอกให้เองอีกแล้ว (CYLINDER_CHANGE_ENABLED = False) —
การเปลี่ยนกระบอกมีต้นทุนและเวลา จึงต้องให้คนตัดสินใจ โมดูลนี้ทำหน้าที่ "เสนอ" เท่านั้น

หลักการเดียวกับ outsource_advisor: Python คำนวณตัวเลขทั้งหมดเอง (กัน LLM มั่วตัวเลข)
แล้วส่ง shortlist ให้ LLM จัดอันดับ + อธิบายเหตุผลเป็นภาษาไทย

งานที่หยิบมาเสนอ:
  1. สาย       — PLAN_WEEK ช้ากว่า RDD_WEEK (งานถูกเลื่อนเพราะเครื่องไม่พอ)
  2. ค้างผลิต  — PLAN_QTY > 0 (เครื่องไม่พอจนผลิตไม่ครบ)

สำหรับแต่ละงาน หา "เกจต้นทาง" ที่มีเครื่องว่างจริงในสัปดาห์ที่ต้องการ (ภายใน Factory+MC_CAT
เดียวกัน) และเช็คกระบอกสำรองจาก MasterMC[Spare part] — ถ้าไม่มีแหล่งหรือกระบอกหมด
ก็ยังแสดงพร้อมเหตุผล เพราะนั่นบอกว่าต้องไปซื้อกระบอกเพิ่มตรงไหน
"""
import json

import config
import plan_view

_W_LATE = 10.0        # ต่อสัปดาห์ที่สาย
_W_PENDING = 1.0      # ต่อ 1000 กก. ค้างผลิต
_W_FEASIBLE = 15.0    # โบนัสถ้าเปลี่ยนกระบอกได้จริง (มี source + spare)
_MAX_SHORTLIST = 20


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


def _col_index(columns, *names):
    up = [str(c).strip().upper() for c in columns]
    for n in names:
        if n in up:
            return up.index(n)
    return -1


def _load_spare_map() -> dict:
    """กระบอกสำรองจาก MasterMC[Spare part] → {(FACTORY, MC_CAT, GUAGE): total_spare}"""
    reg = config.master_files()
    path = reg.get("MasterMC")
    out: dict = {}
    if path is None or not path.exists():
        return out
    try:
        import pandas as pd
        df = pd.read_excel(path, sheet_name="Spare part")
        df.columns = df.columns.str.strip()
    except Exception:
        return out
    for _, r in df.iterrows():
        fac = str(r.get("Factory", "")).strip().upper()
        cat = str(r.get("MC_CAT", "")).strip().upper()
        g = _gnorm(r.get("Guage", ""))
        if not fac or not cat or not g:
            continue
        try:
            out[(fac, cat, g)] = int(r.get("Total Spare", 0) or 0)
        except (TypeError, ValueError):
            continue
    return out


def _remain_live_by_week(cols, rows, ava) -> dict:
    """เครื่องว่างจริงหลังหักงานที่แผนจองไปแล้ว → {week(str): {"CAT|GUAGE": remain}}

    ava.remain หักเฉพาะ booking; เครื่องที่แผนจองเพิ่ม = ACTUAL_MC − MC_BOOKING
    (เครื่อง booking ถูกหักจาก remain ไปแล้ว จึงไม่หักซ้ำ)
    """
    i_week = _col_index(cols, "PLAN_WEEK")
    i_cat = _col_index(cols, "CAT")
    i_gauge = _col_index(cols, "MC_GUAGE", "GUAGE")
    i_amc = _col_index(cols, "ACTUAL_MC")
    i_bmc = _col_index(cols, "MC_BOOKING")

    used: dict = {}
    if min(i_week, i_cat, i_gauge, i_amc) >= 0:
        for r in rows:
            if i_week >= len(r):
                continue
            wk = _to_int(r[i_week])
            if wk is None:
                continue
            key = f"{str(r[i_cat]).strip()}|{_gnorm(r[i_gauge])}"
            amc = _to_num(r[i_amc]) if i_amc < len(r) else 0
            bmc = _to_num(r[i_bmc]) if 0 <= i_bmc < len(r) else 0
            net = max(0, int(amc) - int(bmc))
            if net:
                used.setdefault(str(wk), {})[key] = used.get(str(wk), {}).get(key, 0) + net

    out: dict = {}
    for wk, slots in ava.items():
        for key, slot in slots.items():
            out.setdefault(wk, {})[key] = int(slot.get("remain", 0)) - used.get(wk, {}).get(key, 0)
    return out


def _editable_from_week() -> int:
    """สัปดาห์แรกที่ยังแก้แผนได้ = สัปดาห์ปัจจุบัน + 2 (สัปดาห์นี้กับสัปดาห์หน้าถูก freeze)
    นิยามสัปดาห์แบบโรงงาน: ศุกร์–พฤหัส → บวก 3 วันก่อนคิด ISO week (ตรงกับ Calendar.py)"""
    from datetime import date, timedelta
    return (date.today() + timedelta(days=3)).isocalendar()[1] + 2


def _bottleneck_week(remain_live, cat, gauge, upto_week, from_week):
    """สัปดาห์แรกที่เครื่องในเกจนี้หมด (remain ≤ 0) ในช่วงที่ยังแก้แผนได้ จนถึงสัปดาห์ที่งานถูกวางไว้
    = สัปดาห์ที่งานอยากผลิตแต่ไม่มีเครื่อง → เป็นสัปดาห์ที่ควรเปลี่ยนกระบอกให้
    ข้ามสัปดาห์ที่ผ่านไปแล้ว/freeze เพราะเปลี่ยนกระบอกย้อนหลังไม่ได้
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


def gather_candidates() -> dict:
    """งานที่ควรพิจารณาเปลี่ยนกระบอก + ตัวเลือกการเปลี่ยน (deterministic — ไม่พึ่ง LLM)"""
    grid = plan_view.read_grid()
    cols = grid.get("columns", [])
    rows = grid.get("rows", [])
    plan_name = grid.get("name")
    if not cols or not rows:
        return {"plan_name": plan_name, "candidates": [], "note": "ยังไม่มีข้อมูลแผนผลิต"}

    i_item = _col_index(cols, "ITEM_CODE")
    i_week = _col_index(cols, "PLAN_WEEK")
    i_rdd = _col_index(cols, "RDD_WEEK", "FG_WEEK", "TARGET_KNIT")
    i_cat = _col_index(cols, "CAT")
    i_gauge = _col_index(cols, "MC_GUAGE", "GUAGE")
    i_mcg = _col_index(cols, "MC_GROUP")
    i_fac = _col_index(cols, "FACTORY_TYPE")
    i_pend = _col_index(cols, "PLAN_QTY")
    i_amc = _col_index(cols, "ACTUAL_MC")
    i_cust = _col_index(cols, "CUSTOMER")
    i_sc = _col_index(cols, "SC_SO_NO")
    if min(i_item, i_week, i_cat, i_gauge) < 0:
        return {"plan_name": plan_name, "candidates": [],
                "note": "ไฟล์แผนไม่มีคอลัมน์ ITEM_CODE/PLAN_WEEK/CAT/MC_GUAGE ที่จำเป็น"}

    ava = plan_view.ava_by_week()
    remain_live = _remain_live_by_week(cols, rows, ava)
    spare_map = _load_spare_map()

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
    for r in rows:
        if i_item >= len(r):
            continue
        item = str(r[i_item]).strip().upper()
        if not item:
            continue
        sc = str(r[i_sc]).strip() if 0 <= i_sc < len(r) else ""
        week = _to_int(r[i_week]) if i_week < len(r) else None
        rdd = _week_of(r[i_rdd]) if 0 <= i_rdd < len(r) else None
        cat = str(r[i_cat]).strip()
        gauge = _gnorm(r[i_gauge])
        fac = str(r[i_fac]).strip().upper() if 0 <= i_fac < len(r) else ""
        mcg = str(r[i_mcg]).strip() if 0 <= i_mcg < len(r) else ""
        cust = str(r[i_cust]).strip() if 0 <= i_cust < len(r) else ""
        amc = int(_to_num(r[i_amc])) if 0 <= i_amc < len(r) else 0

        late = max(0, week - rdd) if (week is not None and rdd is not None) else 0
        pend = pending_by_key.get((item, sc), 0.0)
        if late <= 0 and pend <= 0:
            continue  # ส่งทัน + ผลิตครบ → ไม่ต้องเปลี่ยนกระบอก

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

    editable_from = _editable_from_week()
    candidates = []
    for a in agg.values():
        # สัปดาห์ที่ควรเปลี่ยนกระบอกให้ = สัปดาห์แรกที่เครื่องเกจนี้หมด ในช่วงที่งานวิ่งอยู่
        # ถ้าไม่มีสัปดาห์ไหนเครื่องหมดเลย → งานสาย/ค้างด้วยเหตุอื่น (เช่น รอด้าย) ไม่ใช่เรื่องกระบอก
        need_week = _bottleneck_week(
            remain_live, a["cat"], a["gauge"], a["plan_week_last"], editable_from
        )
        if need_week is None:
            continue
        need_mc = max(1, a["machines"])
        cats_in_fac = factory_cats.get(a["factory"], set())
        sources = _find_sources(remain_live, cats_in_fac, need_week, a["cat"], a["gauge"])
        spare = spare_map.get((a["factory"], a["cat"].upper(), a["gauge"]), 0)

        if not sources and spare <= 0:
            blocker = f"ไม่มีเครื่องว่างในเกจอื่นของ {a['cat']} ที่ W{need_week} และไม่มีกระบอกสำรองเกจ {a['gauge']}"
        elif not sources:
            blocker = f"มีกระบอกสำรองเกจ {a['gauge']} {spare} อัน แต่ไม่มีเครื่องว่างในเกจอื่นให้ถอดมาเปลี่ยนที่ W{need_week}"
        elif spare <= 0:
            blocker = f"มีเครื่องว่างให้เปลี่ยน แต่ไม่มีกระบอกสำรองเกจ {a['gauge']} (Total Spare = 0) → ต้องซื้อกระบอกเพิ่ม"
        else:
            blocker = ""

        feasible = bool(sources) and spare > 0
        score = (a["late_weeks"] * _W_LATE
                 + (a["pending_qty"] / 1000.0) * _W_PENDING
                 + (_W_FEASIBLE if feasible else 0.0))

        # ประโยคปฏิบัติการ — Python เขียนเอง ไม่ให้ LLM ประกอบตัวเลข/เกจเองแล้วมั่ว
        if feasible:
            src_txt = " หรือ ".join(f"เกจ {s['gauge']}" for s in sources[:2])
            action = (f"เปลี่ยนกระบอก {need_mc} เครื่อง จาก{src_txt} → เกจ {a['gauge']} "
                      f"ที่ {a['factory']} {a['cat']} สัปดาห์ W{need_week}")
        else:
            action = f"ยังเปลี่ยนไม่ได้ที่ W{need_week} — {blocker}"

        candidates.append({
            "item_code": a["item_code"],
            "customer": a["customer"],
            "cat": a["cat"],
            "gauge": a["gauge"],
            "factory": a["factory"],
            "mc_group": a["mc_group"],
            "late_weeks": a["late_weeks"],
            "pending_qty": round(a["pending_qty"], 1),
            "plan_week": a["plan_week"],
            "rdd_week": a["rdd_week"],
            "need_week": need_week,
            "machines_needed": need_mc,
            "spare_available": spare,
            "sources": sources[:3],          # เกจต้นทางที่ถอดเครื่องมาเปลี่ยนได้
            "feasible": feasible,
            "blocker": blocker,
            "action": action,
            "score": round(score, 1),
        })

    candidates.sort(key=lambda c: c["score"], reverse=True)
    return {"plan_name": plan_name, "candidates": candidates[:_MAX_SHORTLIST],
            "total_found": len(candidates), "note": ""}


_SYSTEM_PROMPT = (
    "คุณเป็นผู้ช่วยวางแผนการผลิตผ้าถัก หน้าที่คือ 'จัดอันดับ' ว่าควรเปลี่ยนกระบอก (cylinder change) "
    "ให้ item ใดก่อน เพื่อปลดล็อกงานที่ส่งไม่ทันหรือผลิตไม่ครบเพราะเครื่องในเกจนั้นหมด "
    "การเปลี่ยนกระบอกคือถอดเครื่องที่ว่างในเกจหนึ่ง มาเปลี่ยนกระบอกเป็นอีกเกจหนึ่ง (ภายใน MC_CAT และโรงงานเดียวกัน) "
    "แต่ละรายการมีฟิลด์ action ที่เขียนไว้แล้วว่าต้องทำอะไร — ห้ามแต่งวิธีทำใหม่ "
    "\n\nกฎเหล็ก: ห้ามพูดถึงตัวเลข เกจ สัปดาห์ หรือจำนวนเครื่องใดๆ ในเหตุผลของคุณเด็ดขาด "
    "เพราะระบบแสดง action กับตัวเลขให้ผู้ใช้เห็นอยู่แล้ว หน้าที่คุณคืออธิบายว่า 'ทำไมควรทำอันนี้ก่อน/หลัง' เท่านั้น "
    "เช่น 'สายมากที่สุดและมีของค้างเยอะ ทำได้ทันที' หรือ 'ยังทำไม่ได้เพราะกระบอกสำรองหมด ต้องสั่งซื้อก่อน' "
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
        "รายการงานที่ติดเครื่อง/ส่งไม่ทัน พร้อมตัวเลือกการเปลี่ยนกระบอก (คำนวณตัวเลขมาแล้ว) "
        "— โปรดจัดอันดับว่าควรเปลี่ยนกระบอกให้ item ไหนก่อน:\n"
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


def advise() -> dict:
    """endpoint หลัก: รวบรวมงานที่ติด → ให้ LLM จัดอันดับ → รวมผล"""
    base = gather_candidates()
    cands = base["candidates"]
    if not cands:
        return {**base, "summary": "", "ai": False,
                "note": base.get("note") or "ไม่มีงานที่ต้องเปลี่ยนกระบอก — แผนปัจจุบันไม่มีงานสายหรือค้างผลิต"}

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

    return {"plan_name": base["plan_name"], "candidates": cands,
            "total_found": base.get("total_found", len(cands)),
            "summary": summary, "ai": ai_ok, "note": note}

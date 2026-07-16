"""
outsource_advisor.py — แนะนำว่า item ไหน "คุ้มค่าที่สุดที่จะจ้างทอ (S9)"

หลักการ: Python คำนวณตัวเลขทั้งหมดเอง (กัน LLM มั่วตัวเลข) แล้วส่ง shortlist
ให้ Azure OpenAI ทำหน้าที่ "จัดอันดับ + อธิบายเหตุผลเป็นภาษาไทย" เท่านั้น

เกณฑ์คุ้มค่า (ตามที่ user กำหนด):
  1. เร่งด่วน delivery — วางแผน (PLAN_WEEK) ช้ากว่ากำหนดส่ง (RDD_WEEK)
  2. คอขวดเครื่อง    — เกจ/CAT ของ item นั้นเครื่องว่าง (remain) ≤ 0 ในสัปดาห์ที่ต้องผลิต
  3. backlog/ค้าง    — ปริมาณค้างผลิต (PLAN_QTY) สูง

คัดเฉพาะ item ที่ "จ้างทอได้จริง" (S9 eligible / S9 only จาก MasterMC) เท่านั้น
"""
import json
from datetime import datetime

import config
import plan_view

# ไฟล์การแบ่งงานไปจ้างทอที่ user ยืนยัน — Planning.py อ่านไฟล์นี้ตอนรันแผน (ดู _load_outsource_split)
# ค้างอยู่จนกว่า user จะลบเอง (งานที่ส่งไปจ้างทอแล้ว แผนรอบถัดไปต้องไม่ดึงกลับมาทอในบ้าน)
SPLIT_FILE = config.OUTPUT_DIR / "outsource_split.json"

# น้ำหนักคะแนน (ใช้ shortlist เบื้องต้นก่อนส่งให้ LLM จัดอันดับสุดท้าย)
_W_LATE = 10.0        # ต่อสัปดาห์ที่สาย
_W_SHORTAGE = 6.0     # ต่อเครื่องที่ขาด (remain ติดลบ)
_W_QTY = 1.0          # ต่อ 1000 กก. ค้าง
_MAX_SHORTLIST = 25   # จำนวน candidate สูงสุดที่ส่งให้ LLM


def _to_int(v):
    """แปลงค่า week/ตัวเลขเป็น int (คืน None ถ้าไม่ได้)"""
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
    """normalize เกจให้ตรงกับ ava key (เลขล้วน) และ S9 eligibility"""
    s = str(g).strip().upper().replace("GAUGE", "").replace("G", "")
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def _load_s9_eligibility() -> tuple:
    """อ่าน S9 eligibility จาก MasterMC → (s9_only:set[item], s9_eligible:set[(item,gauge)])"""
    reg = config.master_files()
    path = reg.get("MasterMC")
    s9_only, s9_elig = set(), set()
    if path is None or not path.exists():
        return s9_only, s9_elig
    try:
        import pandas as pd
        elig_df = pd.read_excel(path, sheet_name="Item S9")
        elig_df.columns = elig_df.columns.str.strip()
        tmp = elig_df.dropna(subset=["ITEM_CODE", "MC_GAUGE"])
        s9_elig = set(
            zip(
                tmp["ITEM_CODE"].astype(str).str.strip().str.upper(),
                tmp["MC_GAUGE"].map(_gnorm),
            )
        )
    except Exception:
        pass
    try:
        import pandas as pd
        only_df = pd.read_excel(path, sheet_name="S9 Only")
        only_df.columns = only_df.columns.str.strip()
        s9_only = set(str(v).strip().upper() for v in only_df["ITEM_CODE"].dropna())
    except Exception:
        pass
    return s9_only, s9_elig


def load_split() -> dict:
    """การแบ่งจ้างทอที่บันทึกไว้ → { ITEM: {"outsource_qty":float,"start_week":int|None,"at":str} }"""
    if not SPLIT_FILE.exists():
        return {}
    try:
        data = json.loads(SPLIT_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    items = data.get("items") or {}
    return {str(k).strip().upper(): v for k, v in items.items() if isinstance(v, dict)}


def _write_split(items: dict) -> None:
    SPLIT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SPLIT_FILE.write_text(
        json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")


def save_split(item_code: str, outsource_qty, start_week, user: str = "") -> dict:
    """บันทึก/แก้การแบ่งจ้างทอของ item หนึ่ง (qty ≤ 0 = ลบรายการนั้น)"""
    item = str(item_code).strip().upper()
    if not item:
        raise ValueError("ไม่ได้ระบุ item")
    qty = _to_num(outsource_qty)
    items = load_split()
    if qty <= 0:
        items.pop(item, None)
        _write_split(items)
        return {"ok": True, "removed": item, "items": items}

    wk = _to_int(start_week)
    if wk is None:
        raise ValueError("ต้องระบุสัปดาห์ที่จ้างทอ")

    # จ้างทอเกินของค้างจริงไม่ได้ (ค้างรวมของ item จากแผนล่าสุด)
    pending = pending_by_item().get(item)
    if pending is not None and qty > pending + 1e-6:
        raise ValueError(f"จ้างทอ {qty:,.0f} กก. เกินของค้างของ {item} ({pending:,.0f} กก.)")

    items[item] = {
        "outsource_qty": round(qty, 2),
        "start_week": wk,
        "at": datetime.now().isoformat(timespec="seconds"),
        "by": user or "",
    }
    _write_split(items)
    return {"ok": True, "saved": item, "items": items}


def delete_split(item_code: str) -> dict:
    items = load_split()
    items.pop(str(item_code).strip().upper(), None)
    _write_split(items)
    return {"ok": True, "items": items}


def pending_by_item(grid: dict = None) -> dict:
    """ปริมาณค้างผลิตรวมต่อ item จากชีท PLAN ของแผนล่าสุด → {ITEM: qty}

    ⚠️ PLAN_QTY ของแต่ละแถว = ของที่ "ยังเหลือหลังแถวนี้" (ลดลงเรื่อยๆ ตามสัปดาห์)
    ห้ามบวกรวมทุกแถว (จะนับซ้ำ) — ของค้างจริงต่อ order = ค่าสูงสุดของ (PLAN_QTY + PRODUCE_QTY)
    คือยอดก่อนผลิตแถวแรก แล้วค่อยรวมทุก order (SC/SO) ของ item นั้น"""
    if grid is None:
        try:
            grid = plan_view.read_grid()
        except Exception:
            return {}
    cols, rows = grid.get("columns", []), grid.get("rows", [])
    i_item = _col_index(cols, "ITEM_CODE")
    i_qty = _col_index(cols, "PLAN_QTY", "PENDING_PLAN")
    i_prod = _col_index(cols, "PRODUCE_QTY")
    i_sc = _col_index(cols, "SC_SO_NO")
    if i_item < 0 or i_qty < 0:
        return {}
    by_order: dict = {}
    for r in rows:
        if max(i_item, i_qty) >= len(r):
            continue
        item = str(r[i_item]).strip().upper()
        if not item:
            continue
        sc = str(r[i_sc]).strip() if 0 <= i_sc < len(r) else ""
        prod = _to_num(r[i_prod]) if 0 <= i_prod < len(r) else 0.0
        total = _to_num(r[i_qty]) + prod
        key = (item, sc)
        by_order[key] = max(by_order.get(key, 0.0), total)
    out: dict = {}
    for (item, _sc), qty in by_order.items():
        out[item] = out.get(item, 0.0) + qty
    return out


def plan_weeks() -> list:
    """สัปดาห์ที่วางแผนจ้างทอได้ (จากเครื่องว่างของแผนล่าสุด) — ให้ frontend ทำ dropdown"""
    try:
        weeks = [int(w) for w in plan_view.ava_by_week().keys()]
    except Exception:
        return []
    return sorted(set(weeks))


def _col_index(columns, *names):
    """หา index คอลัมน์แรกที่ชื่อ (upper) ตรงกับ names — คืน -1 ถ้าไม่พบ"""
    up = [str(c).strip().upper() for c in columns]
    for n in names:
        if n in up:
            return up.index(n)
    return -1


def gather_candidates() -> dict:
    """รวบรวม candidate + คำนวณ metric ต่อ item (deterministic — ไม่พึ่ง LLM)
    → {"plan_name":.., "candidates":[...], "note":..}"""
    grid = plan_view.read_grid()  # ชีท PLAN (= PLAN_NO_S9: สภาพถ้าไม่จ้างทอ)
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
    i_qty = _col_index(cols, "PLAN_QTY", "PENDING_PLAN", "PRODUCE_QTY")
    i_cust = _col_index(cols, "CUSTOMER")
    i_mcg = _col_index(cols, "MC_GROUP")
    if min(i_item, i_week) < 0:
        return {"plan_name": plan_name, "candidates": [],
                "note": "ไฟล์แผนไม่มีคอลัมน์ ITEM_CODE/PLAN_WEEK ที่จำเป็น"}

    s9_only, s9_elig = _load_s9_eligibility()
    ava = plan_view.ava_by_week()  # {week: {"CAT|GUAGE": {"remain":..}}}
    # ของค้างจริงต่อ item (ห้ามบวก PLAN_QTY ทุกแถว — นับซ้ำ ดู pending_by_item)
    pending = pending_by_item(grid)

    agg: dict = {}
    for r in rows:
        if i_item >= len(r):
            continue
        item = str(r[i_item]).strip().upper()
        if not item:
            continue
        gauge = _gnorm(r[i_gauge]) if 0 <= i_gauge < len(r) else ""
        eligible = item in s9_only or (item, gauge) in s9_elig
        if not eligible:
            continue

        week = _to_int(r[i_week]) if i_week < len(r) else None
        rdd = _to_int(r[i_rdd]) if 0 <= i_rdd < len(r) else None
        cat = str(r[i_cat]).strip() if 0 <= i_cat < len(r) else ""
        cust = str(r[i_cust]).strip() if 0 <= i_cust < len(r) else ""
        mcg = str(r[i_mcg]).strip() if 0 <= i_mcg < len(r) else ""

        late = max(0, week - rdd) if (week is not None and rdd is not None) else 0

        # คอขวด: เครื่องว่างในสัปดาห์ที่ต้องผลิต (remain ต่ำ/ติดลบ)
        remain = None
        if week is not None and cat and gauge:
            slot = ava.get(str(week), {}).get(f"{cat}|{gauge}")
            if slot is not None:
                remain = int(slot.get("remain", 0))
        shortage = max(0, -remain) if remain is not None else 0

        a = agg.setdefault(item, {
            "item_code": item, "customer": cust, "cat": cat, "gauge": gauge,
            "mc_group": mcg, "qty": 0.0, "late_weeks": 0, "shortage": 0,
            "s9_only": item in s9_only, "weeks": set(),
        })
        a["qty"] = pending.get(item, 0.0)
        a["late_weeks"] = max(a["late_weeks"], late)
        a["shortage"] = max(a["shortage"], shortage)
        if week is not None:
            a["weeks"].add(week)
        if not a["customer"] and cust:
            a["customer"] = cust

    candidates = []
    for a in agg.values():
        # คัดเฉพาะที่มีเหตุผลจ้างทอจริง: สาย หรือ คอขวด หรือ S9-only
        if not (a["late_weeks"] > 0 or a["shortage"] > 0 or a["s9_only"]):
            continue
        score = (a["late_weeks"] * _W_LATE
                 + a["shortage"] * _W_SHORTAGE
                 + (a["qty"] / 1000.0) * _W_QTY)
        candidates.append({
            "item_code": a["item_code"],
            "customer": a["customer"],
            "cat": a["cat"],
            "gauge": a["gauge"],
            "mc_group": a["mc_group"],
            "qty": round(a["qty"], 1),
            "late_weeks": a["late_weeks"],
            "machine_shortage": a["shortage"],
            "s9_only": a["s9_only"],
            "plan_weeks": sorted(a["weeks"]),
            "score": round(score, 1),
        })

    candidates.sort(key=lambda c: c["score"], reverse=True)

    # แนบการแบ่งจ้างทอที่ user บันทึกไว้ (ให้ช่องกรอกในหน้าเว็บโชว์ค่าที่ใช้อยู่)
    split = load_split()
    for c in candidates:
        s = split.get(c["item_code"]) or {}
        c["outsource_qty"] = _to_num(s.get("outsource_qty"))
        c["start_week"] = _to_int(s.get("start_week"))

    return {"plan_name": plan_name, "candidates": candidates[:_MAX_SHORTLIST],
            "total_eligible": len(candidates), "note": "",
            "split": split, "weeks": plan_weeks()}


_SYSTEM_PROMPT = (
    "คุณเป็นผู้ช่วยวางแผนการผลิตผ้าถักของโรงงานทอผ้า หน้าที่ของคุณคือแนะนำว่า "
    "ควรส่ง item ใดไป 'จ้างทอข้างนอก (outsource/S9)' ก่อน เพื่อให้คุ้มค่าที่สุด "
    "โดยพิจารณา 3 ปัจจัย: (1) ความเร่งด่วน — วางแผนช้ากว่ากำหนดส่งกี่สัปดาห์ (late_weeks) "
    "(2) การปลดคอขวดเครื่อง — เครื่องในเกจนั้นขาดกี่ตัว (machine_shortage) "
    "(3) ปริมาณค้างผลิต (qty กก.) "
    "ตัวเลขทั้งหมดถูกคำนวณมาให้แล้ว ห้ามแก้ไขหรือสร้างตัวเลขใหม่ ใช้ตามที่ให้เท่านั้น "
    "จัดอันดับจากคุ้มค่าที่สุดไปน้อยสุด และอธิบายเหตุผลสั้นๆ เป็นภาษาไทยที่เข้าใจง่าย "
    "ตอบกลับเป็น JSON เท่านั้น รูปแบบ: "
    '{"summary": "สรุปภาพรวม 1-2 ประโยค", '
    '"ranking": [{"item_code": "...", "rank": 1, "reason": "เหตุผลไทยสั้นๆ"}]}'
)


def _call_llm(candidates: list) -> dict:
    """เรียก OpenAI จัดอันดับ + อธิบาย — คืน {"summary":.., "ranking":[..]}
    โยน exception ถ้าเรียกไม่สำเร็จ (ให้ชั้นบน fallback)"""
    cfg = config.llm_config()
    if not cfg["api_key"]:
        raise RuntimeError("ยังไม่ได้ตั้งค่า OpenAI (OPENAI_API_KEY)")

    from openai import OpenAI
    kwargs = {"api_key": cfg["api_key"]}
    if cfg["base_url"]:
        kwargs["base_url"] = cfg["base_url"]
    client = OpenAI(**kwargs)
    user_msg = (
        "รายการ item ที่จ้างทอได้และมีแรงกดดัน (คำนวณตัวเลขมาแล้ว) — โปรดจัดอันดับความคุ้มค่า:\n"
        + json.dumps(candidates, ensure_ascii=False)
    )
    # ไม่ส่ง temperature — โมเดลรุ่นใหม่ (เช่น gpt-5.x) รับเฉพาะค่า default และจะ error 400
    # ทำให้ตกไปใช้ fallback เงียบๆ ทั้งที่ AI ใช้งานได้
    resp = client.chat.completions.create(
        model=cfg["model"],
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    return json.loads(content)


def advise() -> dict:
    """endpoint หลัก: รวบรวม candidate → ให้ LLM จัดอันดับ → รวมผล
    → {"plan_name":.., "candidates":[..พร้อม rank/reason..], "summary":.., "ai": bool, "note":..}"""
    base = gather_candidates()
    cands = base["candidates"]
    if not cands:
        return {**base, "summary": "", "ai": False,
                "note": base.get("note") or "ไม่พบ item ที่ควรจ้างทอในแผนปัจจุบัน"}

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
        # เรียงตามอันดับที่ LLM ให้ (ที่ไม่มีอันดับไปท้าย)
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
            "total_eligible": base.get("total_eligible", len(cands)),
            "summary": summary, "ai": ai_ok, "note": note,
            "split": base.get("split", {}), "weeks": base.get("weeks", [])}

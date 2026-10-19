"""
s9_ava_live.py — Outsource Availability: เครื่องของผู้รับจ้างทอที่เหลือรายสัปดาห์ (หน้า Dashboard)

หน้าตา/ตัวเลขชุดเดียวกับหน้า Machine Availability แต่เป็นเครื่องของผู้รับจ้าง:
  เครื่องทั้งหมด = Total Mc ของแต่ละแถวในชีท MC S9 (plan_view.s9_pool — ข้ามแถว Inactive แล้ว)
  ใช้ไป         = ผลรวม ACTUAL_MC ของแถวจ้างทอในแผนล่าสุด ต่อ (S9_POOL, สัปดาห์)

ทำไมไม่สรุปจาก booking แบบหน้าในบ้าน: booking ไม่มีข้อมูลว่างานไหนจ้างเจ้าไหน
(AVA_MC ตัดกลุ่ม COMKN ทิ้งด้วยซ้ำ) — เครื่องผู้รับจ้างถูกจองในแผนเท่านั้น และ Planning
ก็เช็คเพดานด้วยตัวเลขชุดนี้ (_s9_weekly_usage คีย์ (สัปดาห์, pool_id)) ส่วน Gantt จ้างทอ
บนหน้าแผนนับด้วย s9UsedAt แบบเดียวกัน → สามจุดเห็นเลขตรงกัน

1 แถวของตาราง = 1 แถวในชีท MC S9 (ผู้รับจ้าง + กลุ่มเครื่อง + เกจ + CAT) เพราะเพดานแข็ง
อยู่ที่ระดับนี้ (user 16 ก.ย. 2569: จ้างรวมกันได้แต่ห้ามเกินเครื่องของแต่ละเจ้า)
"""
from datetime import date
from pathlib import Path

import config
import plan_view

# แคช: (ไฟล์แผน, mtime, MasterMC mtime, วันทำงานผู้รับจ้าง) → ผลที่แปลงแล้ว
# หน้าเว็บโหลดซ้ำทุกครั้งที่มีคนเซฟแผน — อ่าน Excel ~1,000 แถวซ้ำทุกครั้งไม่จำเป็น
_cache: dict = {}


def _s(v) -> str:
    s = str(v if v is not None else "").strip()
    return "" if s.lower() in ("nan", "nat", "none") else s


def _is_s9_row(r) -> bool:
    """แถวจ้างทอ — ตรงกับ isS9Row ใน KnitPlan.jsx / _is_outsource_row ใน Planning.py
    S9_ROUTING ในไฟล์เป็น 1.0/0.0 (ห้ามเช็ค truthy ของ string — "0.0" เป็น true)
    ไฟล์รุ่นเก่าไม่มีธง → ดู MC_GROUP = COMKN แทน"""
    v = _s(r.get("S9_ROUTING")).upper()
    if v in ("TRUE", "YES"):
        return True
    try:
        if float(v) > 0:
            return True
    except ValueError:
        pass
    return _s(r.get("MC_GROUP")).upper() == "COMKN"


def _ukey(pid: str) -> str:
    """คีย์ไว้จับคู่รหัสพูล — ไม่สนตัวพิมพ์/ช่องว่างรอบ ๆ แต่ละส่วน"""
    return "|".join(p.strip().upper() for p in str(pid).split("|"))


def _plan_sheet(p: Path) -> str:
    # ไฟล์รุ่นเก่า (ก่อน 11 ก.ย. 2569): ชีท PLAN = แผน SC ล้วน ส่วนแผนจริงอยู่ชีท REPLAN
    return plan_view._REPLAN_SHEET if plan_view._is_legacy_file(p) else plan_view._MAIN_SHEET


def _week_span(first: tuple, last: tuple) -> list:
    """(ปี, สัปดาห์) ทุกสัปดาห์ตั้งแต่ first ถึง last ตามปฏิทินจริง (Calendar.xlsx)
    ต้องเดินบนปฏิทิน ไม่ใช่ +1 เอง — ปีที่มี W53 (2026) กับปีที่ไม่มี ต่างกัน
    อ่านปฏิทินไม่ได้ → คืน [] ให้ผู้เรียกถอยไปใช้เฉพาะสัปดาห์ที่มีในแผน"""
    weekly = plan_view._calendar_weekly()
    if weekly is None or len(weekly) == 0:
        return []
    cols = {str(c).strip().upper(): c for c in weekly.columns}
    yc, wc = cols.get("YEAR"), cols.get("WEEK")
    if yc is None or wc is None:
        return []
    out, seen = [], set()
    for y, w in zip(weekly[yc], weekly[wc]):
        try:
            yw = (int(y), int(w))
        except (TypeError, ValueError):
            continue
        if first <= yw <= last and yw not in seen:
            seen.add(yw)
            out.append(yw)
    return sorted(out)


def _build(path: Path, pools: dict) -> dict:
    import pandas as pd
    df = pd.read_excel(path, sheet_name=_plan_sheet(path))
    if "PLAN_WEEK" not in df.columns:
        return {"weeks": [], "years": {}, "rows": [], "jobs": [],
                "error": "ไฟล์แผนไม่มีคอลัมน์ PLAN_WEEK"}
    has_year = "PLAN_YEAR" in df.columns

    # พูลจากชีท MC S9 → แถวตาราง (คีย์ = รหัสพูลรายเจ้า เดียวกับคอลัมน์ S9_POOL ของแผน)
    rows: dict = {}
    by_u: dict = {}           # คีย์จับคู่ → คีย์จริง
    by_smg: dict = {}         # (เจ้า, กลุ่ม, เกจ) → [คีย์จริง] — ใช้กับแถวแผนรุ่นที่ไม่มี S9_POOL
    for pid, p in pools.items():
        cat = (p.get("cats") or [""])[0]
        mats = p.get("mats") or []
        rows[pid] = {
            "key": pid, "factory": p.get("supp") or "", "supp": p.get("supp") or "",
            "mc": p.get("mc") or "", "gauge": p.get("gauge") or "", "mc_cat": cat,
            "total": int(p.get("total") or 0), "missing": False,
            # ป้ายหมวดผ้า — โชว์เฉพาะเจ้าที่ระบุไว้ (ว่าง = BLEND ค่าเริ่มต้น ไม่ต้องรก)
            "pool_label": ("รับเฉพาะ " + "/".join(mats)) if mats and mats != ["BLEND"] else "",
        }
        by_u[_ukey(pid)] = pid
        by_smg.setdefault((_s(p.get("supp")).upper(), _s(p.get("mc")).upper(), str(p.get("gauge"))),
                          []).append(pid)

    def _pool_of(r) -> str:
        pid = _s(r.get("S9_POOL"))
        if pid and _ukey(pid) in by_u:
            return by_u[_ukey(pid)]
        supp, mc = _s(r.get("S9_SUPP")), _s(r.get("S9_MC")).upper()
        g = plan_view._s9_gauge_num(r.get("S9_GAUGE"))
        cand = by_smg.get((supp.upper(), mc, g), [])
        if not pid and len(cand) == 1:
            return cand[0]
        # พูลที่แผนอ้างแต่ไม่อยู่ในชีทแล้ว (ถูกลบ/ตั้ง Inactive หลังรันแผน) → แถวเครื่อง 0
        # ต้องโชว์ให้เห็น ไม่งั้นงานที่ยังวางอยู่บนเครื่องที่ไม่มีจริงจะหายไปจากหน้านี้เงียบ ๆ
        key = pid or plan_view._s9_pool_id(supp, mc, g, "")
        if key not in rows:
            parts = key.split("|")
            rows[key] = {
                "key": key, "factory": supp or (parts[0] if parts else ""),
                "supp": supp or (parts[0] if parts else ""), "mc": mc or (parts[1] if len(parts) > 1 else ""),
                "gauge": g or (parts[2] if len(parts) > 2 else ""),
                "mc_cat": parts[3] if len(parts) > 3 else "",
                "total": 0, "missing": True, "pool_label": "ไม่มีในชีท MC S9",
            }
            by_u[_ukey(key)] = key
        return key

    used: dict = {}           # (key, สัปดาห์) → เครื่อง
    jobs = []
    yw_all, yw_s9 = set(), set()
    for r in df.to_dict("records"):
        wk = plan_view._wk_str(r.get("PLAN_WEEK"))
        try:
            wn = int(float(wk)) if wk else None
        except ValueError:
            wn = None
        if wn is None or wn == 99:                  # W99 = sentinel งานล้น ไม่ใช่สัปดาห์จริง
            continue
        yr = plan_view._num(r.get("PLAN_YEAR")) if has_year else None
        yr = int(yr) if (yr is not None and yr == yr and yr > 0) else None
        if yr is None:
            yr = plan_view.key_week_year(str(wn))[1]
        yw_all.add((yr, wn))
        if not _is_s9_row(r):
            continue
        key = _pool_of(r)
        mc = int(plan_view._f(r.get("ACTUAL_MC")))
        yw_s9.add((yr, wn))
        wkey = plan_view.week_key(wn, yr)
        used[(key, wkey)] = used.get((key, wkey), 0) + mc
        new = int(plan_view._f(r.get("NEW_MC")))
        jobs.append({
            "pool": key, "week": wkey,
            "item": _s(r.get("ITEM_CODE")),
            "so": _s(r.get("SC_SO_NO")),
            "team": _s(r.get("TEAM")),
            "customer": _s(r.get("CUSTOMER")),
            "material": _s(r.get("MATERIAL_CONTENT")),
            "kp": round(plan_view._f(r.get("PRODUCE_QTY")), 1),
            "cap": round(plan_view._f(r.get("DAILY_CAPACITY")), 1),
            "mc": mc, "new": new, "carry": int(plan_view._f(r.get("CARRYOVER_MC"))),
            "target": _s(r.get("TARGET_KNIT")),
            # ที่มาของก้อน: OUTSOURCE=Yes = user สั่งเองจากหน้าเว็บ ; นอกนั้นระบบส่งเอง (S9 Only / ไม่ทัน target)
            "forced": _s(r.get("OUTSOURCE")).upper() == "YES",
            "remark": _s(r.get("REMARK")),
        })

    # ช่วงสัปดาห์: ตั้งแต่สัปดาห์นี้ (หรืองานจ้างทอที่เก่ากว่า) ถึงสัปดาห์สุดท้ายของทั้งแผน
    # — กางทุกสัปดาห์ในช่วงแม้ไม่มีงาน เพราะหน้านี้มีไว้หาว่า "เจ้าไหนยังว่างตอนไหน"
    cw, cy = plan_view.key_week_year(str(plan_view._cur_plan_week()))
    cur = (cy, cw)
    first = min([cur] + sorted(yw_s9)[:1])
    last = max(yw_all | {cur})
    span = _week_span(first, last) or sorted(yw_all | yw_s9 | {cur})
    weeks = [plan_view.week_key(w, y) for y, w in span]
    years = {plan_view.week_key(w, y): y for y, w in span}

    out_rows = []
    for row in sorted(rows.values(),
                      key=lambda r: (r["missing"], r["supp"] or "zz", r["mc_cat"], r["mc"],
                                     int(r["gauge"]) if str(r["gauge"]).isdigit() else 999)):
        cells = {}
        for wk in weeks:
            u = used.get((row["key"], wk), 0)
            cells[wk] = {"total": row["total"], "used": u, "remain": row["total"] - u}
        g = row["gauge"]
        cg = f"{row['mc_cat']} {g}G" if row["mc_cat"] else f"{row['mc']} {g}G"
        out_rows.append({
            **row,
            # ชื่อแถว = กลุ่มเครื่อง · CAT เกจ (ผู้รับจ้างอยู่บรรทัดรองของหัวแถว เหมือนโรงงานในหน้าในบ้าน)
            "cat": f"{row['mc']} · {cg}",
            "cats": [cg],
            "cells": cells,
        })

    jobs.sort(key=lambda j: (-j["mc"], -j["kp"]))
    return {"weeks": weeks, "years": years, "rows": out_rows, "jobs": jobs}


def table() -> dict:
    """ตาราง Outsource Availability จากแผนล่าสุด + ชีท MC S9 (อ่านสดทุกครั้งที่ไฟล์เปลี่ยน)"""
    p = plan_view.latest_path()
    mm = config.master_files().get("MasterMC")
    if p is None:
        return {"ok": False, "weeks": [], "rows": [], "jobs": [],
                "reason": "ยังไม่มีไฟล์แผนผลิต — รันแผนก่อน หน้านี้นับเครื่องจ้างทอจากงานในแผน"}
    try:
        st = p.stat()
        mm_mtime = Path(mm).stat().st_mtime if mm is not None and Path(mm).exists() else None
    except OSError as e:
        return {"ok": False, "weeks": [], "rows": [], "jobs": [], "reason": f"อ่านไฟล์ไม่ได้: {e}"}

    ck = (str(p), st.st_mtime, mm_mtime, date.today())
    cached = _cache.get(ck)
    if cached is None:
        pools = plan_view.s9_pool()
        if not pools:
            return {"ok": False, "weeks": [], "rows": [], "jobs": [],
                    "reason": "ไม่พบเครื่องผู้รับจ้างในชีท MC S9 (MasterMC) — เพิ่มที่หน้าเครื่องจักร › จ้างทอ"}
        try:
            cached = _build(p, pools)
        except Exception as e:                       # noqa: BLE001 — รายงานให้หน้าเว็บทุกกรณี
            return {"ok": False, "weeks": [], "rows": [], "jobs": [],
                    "reason": f"อ่านไฟล์แผนไม่ได้: {e}"}
        _cache.clear()                               # เก็บแค่ผลล่าสุดพอ
        _cache[ck] = cached

    out = dict(cached)
    out.update({
        "ok": not cached.get("error"),
        "reason": cached.get("error"),
        "plan": {"name": p.name, "mtime": st.st_mtime, "run_at": plan_view._run_at(p)},
        "work_days": plan_view.s9_work_days(),
    })
    return out

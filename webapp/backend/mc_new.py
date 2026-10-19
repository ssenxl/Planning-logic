"""
mc_new.py — ตัวช่วย "เพิ่มเครื่องใหม่" + ไฟตรวจความพร้อมของเครื่องแต่ละกลุ่ม

ทำไมต้องมี: การเพิ่มเครื่องไม่ใช่การกรอกช่องเดียว ต้องครบ 3 ที่ถึงจะเข้าแผนได้จริง
  1. ชีท "Master MC"                → มีเครื่องกี่ตัว (เพดานของแผน)
  2. แผงวันทำงาน (Calendar/Work Day) → สัปดาห์หนึ่งเดินกี่วัน ; ไม่ตั้ง = ระบบใช้ 6 วันเงียบ ๆ
  3. view_mc_item (มาจาก Oracle)     → กำลังผลิตต่อวันของคู่ (ผ้า, เครื่อง, เกจ)
                                       ไม่มี = งานไม่มี CAP → ตกไปชีท NO_CAP วางไม่ได้
ข้อ 3 แก้บนเว็บไม่ได้ (ต้องให้ฝ่ายข้อมูลเพิ่มใน view) — หน้าที่ของไฟล์นี้คือ "บอกให้รู้ก่อน"
ไม่ใช่รอให้ไปเจอตอนแผนออกมาแปลก

⚠️ ห้ามเดาค่าให้ผู้ใช้: ฟังก์ชัน check ตอบเฉพาะสิ่งที่อ่านได้จริงจากไฟล์/วิว
   ถ้าอ่านไฟล์ไม่ได้ ให้บอกว่า "ตรวจไม่ได้" ไม่ใช่ตอบว่าผ่าน
"""
from pathlib import Path

import config
import livesync
import masters
import workday
from openpyxl import load_workbook

MASTER_SHEET = "Master MC"
MC_TOTAL_SHEET = "MC_Total"

# คอลัมน์ที่ต้องมีค่าเสมอเวลาเพิ่มแถวใหม่ (ชื่อหัวคอลัมน์จริงในไฟล์)
REQUIRED = ("Factory", "Type", "MC_CAT", "MC", "Guage", "Total MC")


def _norm(v) -> str:
    return str(v if v is not None else "").strip()


def _key(v) -> str:
    """คีย์เทียบแบบให้อภัย: ตัดช่องว่าง ตัวพิมพ์ใหญ่ และเกจ 22.0 → 22"""
    s = _norm(v).upper().replace("\xa0", "")
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _gauge_key(v) -> str:
    """เกจสำหรับเทียบกับ view_mc_item — กติกาเดียวกับ AVA_MC._norm_gauge_key
    (เกจ 'C0' เลขศูนย์ในวิว = 'CO' ตัวโอฝั่ง booking)"""
    s = _key(v).replace("GAUGE", "").replace("G", "").strip()
    return "CO" if s == "C0" else s


# MasterMC สะกด SINGEL-16..26 แต่ booking/วิวสะกด SINGLE-* และ SYN-28/30 ถูกรวมเป็น DOUBLE-30
# ลอกกติกาจาก AVA_MC.py:2025-2037 — ถ้าไม่ทำ ไฟตรวจจะฟ้องว่า "ไม่มีกำลังผลิต" ทั้งที่มี
_CAT_MAP = {"SYN-30": "DOUBLE-30", "SYN-28": "DOUBLE-30"}


def _cat_key(v) -> str:
    s = _key(v).replace("SINGEL", "SINGLE")
    return _CAT_MAP.get(s, s)


# ---------------------------------------------------------------- Master MC
def _master_rows() -> tuple:
    """(หัวคอลัมน์, แถวทั้งหมด) ของชีท Master MC"""
    d = masters.read_sheet("MasterMC", MASTER_SHEET)
    return d["columns"], d["rows"], d.get("rev")


def _col(columns, name) -> int:
    up = [_key(c) for c in columns]
    try:
        return up.index(_key(name))
    except ValueError:
        return -1


def mc_rows() -> list:
    """แถวเครื่องทั้งหมดในรูปแบบที่ใช้ง่าย — ใช้ทั้งตอน check และ audit"""
    columns, rows, _ = _master_rows()
    ci = {n: _col(columns, n) for n in
          ("Factory", "Type", "MC_CAT", "MC", "Guage", "Group", "Total MC")}
    out = []
    for r in rows:
        def g(n):
            i = ci[n]
            return r[i] if 0 <= i < len(r) else None
        if not any(_norm(g(n)) for n in ("MC_CAT", "MC", "Guage")):
            continue
        out.append({
            "factory": _norm(g("Factory")), "type": _norm(g("Type")),
            "mc_cat": _norm(g("MC_CAT")), "mc": _norm(g("MC")),
            "guage": _norm(g("Guage")), "group": _norm(g("Group")),
            "total": _norm(g("Total MC")),
        })
    return out


# ------------------------------------------------------------ กำลังผลิต (CAP)
_cap_cache = {"mtime": None, "keys": None, "by_mc": None, "err": ""}


def _cap_keys() -> tuple:
    """กำลังผลิตที่มีในวิว → ({(cat,mc,gauge): จำนวนผ้า}, {(mc,gauge): {cat: จำนวน}})

    ต้องดู 2 ระดับ เพราะ MC_CAT ของ MasterMC กับของวิว/booking ไม่จำเป็นต้องสะกดเหมือนกัน
    (เช่น IRMT เกจ 28 — MasterMC อยู่ใต้ DOUBLE-34 แต่ในวิวอยู่ใต้ DOUBLE KNIT JAQ-28)
      • คีย์ 3 ตัว = คีย์ที่ AVA_MC ใช้ override CAP จริง
      • คีย์ 2 ตัว = "เครื่อง+เกจนี้มีผ้าที่ทอได้ไหม" — ใช้แยกว่าไม่มีจริง หรือแค่จัดหมวดคนละชื่อ
    คืน (None, error) เมื่ออ่านไม่ได้ — ต้องบอกว่า "ตรวจไม่ได้" ไม่ใช่ "ไม่มี" """
    path = Path(config.REPO_DIR) / "MC_Item" / "view_mc_item.xlsx"
    if not path.exists():
        return None, f"ไม่พบไฟล์ {path.name} (ดึงด้วย View_MC_Item.py)"
    try:
        mtime = path.stat().st_mtime
        if _cap_cache["mtime"] == mtime and _cap_cache["keys"] is not None:
            return (_cap_cache["keys"], _cap_cache["by_mc"]), ""
        import pandas as pd
        df = pd.read_excel(path, usecols=lambda c: str(c).strip().upper() in (
            "ITEM_CODE", "KNIT_MC_CAT", "KNIT_MC_GROUP", "KNIT_MC_GUAGE", "ACTIVE_STATUS"))
        df.columns = [str(c).strip().upper() for c in df.columns]
        need = {"KNIT_MC_CAT", "KNIT_MC_GROUP", "KNIT_MC_GUAGE"}
        if not need <= set(df.columns):
            return None, "ไฟล์กำลังผลิตขาดคอลัมน์ที่ต้องใช้"
        if "ACTIVE_STATUS" in df.columns:
            df = df[df["ACTIVE_STATUS"].astype(str).str.strip().str.upper() == "Y"]
        keys, by_mc = {}, {}
        for cat, grp, g in zip(df["KNIT_MC_CAT"], df["KNIT_MC_GROUP"], df["KNIT_MC_GUAGE"]):
            ck, mk, gk = _cat_key(cat), _key(grp), _gauge_key(g)
            keys[(ck, mk, gk)] = keys.get((ck, mk, gk), 0) + 1
            slot = by_mc.setdefault((mk, gk), {})
            slot[_norm(cat)] = slot.get(_norm(cat), 0) + 1
        _cap_cache.update(mtime=mtime, keys=keys, by_mc=by_mc, err="")
        return (keys, by_mc), ""
    except Exception as e:                                   # noqa: BLE001
        return None, f"อ่านไฟล์กำลังผลิตไม่ได้: {e}"


def _calendar_last_week() -> tuple:
    """(สัปดาห์สุดท้าย, ปีของสัปดาห์นั้น) ที่ปฏิทินมีอยู่ — ใช้กางช่วงของ MC_Total ให้ถึงปลายแผน
    อ่านไม่ได้ → (None, None) แล้วผู้เรียกไม่ต้องใส่ Week To (ผู้ใช้ตามแก้เองได้)"""
    try:
        weeks = workday.calendar_week_timeline() or []
        if not weeks:
            return None, None
        y, w = weeks[-1][0], weeks[-1][1]
        return int(w), int(y)
    except Exception:                                        # noqa: BLE001
        return None, None


# ------------------------------------------------------------------- ตรวจ
def options() -> dict:
    """รายการค่าที่ "มีอยู่จริง" ของแต่ละช่อง — ให้หน้าเว็บทำเป็นตัวเลือกแทนช่องพิมพ์อิสระ

    พิมพ์ชื่อกลุ่มเองแล้วสะกดต่างนิดเดียว (SINGEL/SINGLE) = ค่าที่ตั้งในแผงวันทำงานหลุดทันที
    จึงต้องให้เลือกจากของจริงเป็นค่าเริ่มต้น ส่วนการสร้างชื่อใหม่ต้องตั้งใจเลือกเอง
    cats_by_factory ใช้กรองตัวเลือกให้เหลือเฉพาะของโรงนั้น
    """
    rows = mc_rows()
    def uniq(vals):
        return sorted({_norm(v) for v in vals if _norm(v)},
                      key=lambda s: (s.upper(),))
    cats_by_fac, gauges_by_cat, groups_by_cg = {}, {}, {}
    for r in rows:
        cats_by_fac.setdefault(_norm(r["factory"]), set()).add(_norm(r["mc_cat"]))
        gauges_by_cat.setdefault(_norm(r["mc_cat"]), set()).add(_norm(r["guage"]))
        if r["group"]:
            groups_by_cg.setdefault(f"{_norm(r['mc_cat'])}|{_norm(r['guage'])}", set()).add(_norm(r["group"]))
    # ชื่อกลุ่มที่ "แผงวันทำงานรู้จัก" — ใช้เตือนตอนผู้ใช้จะตั้งชื่อใหม่ที่แผงไม่มี
    try:
        panel = sorted({k.split("|")[1] for k in (workday.get_workday().get("defaults") or {})
                        if len(k.split("|")) == 3})
    except Exception:                                        # noqa: BLE001
        panel = []
    return {
        "factories": uniq(r["factory"] for r in rows),
        "types": uniq(r["type"] for r in rows),
        "cats": uniq(r["mc_cat"] for r in rows),
        "mcs": uniq(r["mc"] for r in rows),
        "gauges": uniq(r["guage"] for r in rows),
        "groups": uniq(r["group"] for r in rows),
        "cats_by_factory": {k: sorted(v) for k, v in cats_by_fac.items()},
        "gauges_by_cat": {k: sorted(v) for k, v in gauges_by_cat.items()},
        "groups_by_cat_gauge": {k: sorted(v) for k, v in groups_by_cg.items()},
        "panel_cats": panel,
    }


def check(factory: str, mc_cat: str, mc: str, guage: str) -> dict:
    """ตรวจว่าเครื่องกลุ่มนี้ "พร้อมเข้าแผน" หรือยัง — 3 ด่าน + ข้อมูลช่วยกรอก

    ทุกค่าที่ตอบมาจากไฟล์จริงเท่านั้น ; ด่านไหนตรวจไม่ได้ตอบ status='unknown'
    """
    rows = mc_rows()
    fk, ck, mk, gk = _key(factory), _cat_key(mc_cat), _key(mc), _key(guage)

    # ---- 1) มีแถวนี้ใน Master MC แล้วหรือยัง (คีย์ที่ระบบใช้จริง = CAT + MC + เกจ)
    same = [r for r in rows if _cat_key(r["mc_cat"]) == ck and _key(r["mc"]) == mk
            and _key(r["guage"]) == gk]
    exists = bool(same)

    # ---- Group ที่ควรใช้: ดูจากแถวที่อยู่ "กองเดียวกัน" อยู่แล้ว
    # กติกาเดียวกับที่หน้าเว็บ/AVA_MC ใช้: กองคิดจาก (CAT, เกจ) ในโรงเดียวกัน
    pool = [r for r in rows if _cat_key(r["mc_cat"]) == ck and _key(r["guage"]) == gk
            and (not fk or _key(r["factory"]) == fk) and r["group"]]
    group_suggest = pool[0]["group"] if pool else ""
    # ชนิดเครื่อง (Type) เดาจากกลุ่มเดียวกันที่มีอยู่แล้ว
    same_cat = [r for r in rows if _cat_key(r["mc_cat"]) == ck and r["type"]]
    type_suggest = same_cat[0]["type"] if same_cat else ""

    # ---- 2) วันทำงานของกลุ่มนี้ (คีย์ FACTORY|MC_CAT|GUAGE — ตรงกับ WorkDay.py)
    # ⚠️ ต้องใช้ชื่อกลุ่ม "ตามที่สะกดใน MasterMC" (_key) ไม่ใช่ชื่อที่ normalize แล้ว (_cat_key)
    #    แผงวันทำงานสร้างคีย์จาก MasterMC ตรง ๆ → SINGEL-16 ยังเป็น SINGEL-16 ที่นั่น
    #    ใช้ _cat_key ตรงนี้จะได้ SINGLE-16 แล้วหาไม่เจอ = ฟ้องผิดว่า "ยังไม่ได้ตั้ง"
    wd_key = f"{fk}|{_key(mc_cat)}|{_key(guage)}"
    try:
        wd = workday.get_workday()
        has_wd = wd_key in (wd.get("defaults") or {})
        wd_val = (wd.get("defaults") or {}).get(wd_key)
        wd_status = "ok" if has_wd else "warn"
        wd_err = ""
    except Exception as e:                                   # noqa: BLE001
        wd_val, wd_status, wd_err = None, "unknown", str(e)

    # ---- 3) กำลังผลิตในวิว — ดู 2 ระดับ (ดูคำอธิบายที่ _cap_keys)
    caps, cap_err = _cap_keys()
    cap_other = {}
    if caps is None:
        cap_status, cap_items, cap_detail = "unknown", 0, f"ตรวจไม่ได้: {cap_err}"
    else:
        keys, by_mc = caps
        cap_items = keys.get((ck, mk, _gauge_key(guage)), 0)
        cap_other = by_mc.get((mk, _gauge_key(guage)), {})
        if cap_items:
            cap_status = "ok"
            cap_detail = f"มีผ้า {cap_items} รายการที่ทอบนเครื่องนี้ได้"
        elif cap_other:
            # มีผ้าผูกกับเครื่อง+เกจนี้ แต่ฐานข้อมูลจัดไว้คนละกลุ่มกับที่กรอก
            names = ", ".join(sorted(cap_other)[:3])
            cap_status = "info"
            cap_detail = (f"มีผ้า {sum(cap_other.values())} รายการผูกกับเครื่องนี้ "
                          f"แต่ฐานข้อมูลจัดอยู่กลุ่ม {names} — ใบสั่งที่มาจากกลุ่มนั้นวางได้ตามปกติ")
        else:
            cap_status = "warn"
            cap_detail = ("ยังไม่มีผ้าที่ผูกกับเครื่อง+Guage นี้ในฐานข้อมูลเลย — ถ้าใบสั่งไม่ได้แนบ"
                          "กำลังผลิตมาเอง งานจะตกไปรายการ 'วางไม่ได้' (แก้บนเว็บไม่ได้ "
                          "ต้องให้ฝ่ายข้อมูลเพิ่มในระบบ ERP)")

    return {
        "exists": exists,
        "existing": same[0] if same else None,
        "group_suggest": group_suggest,
        "type_suggest": type_suggest,
        "pool_mates": sorted({r["mc"] for r in pool if _key(r["mc"]) != mk}),
        "checks": [
            {
                "id": "master", "label": "มีในทะเบียนเครื่องแล้ว",
                "status": "ok" if exists else "todo",
                "detail": (f"มีอยู่แล้ว {same[0]['total']} เครื่อง" if exists
                           else "ยังไม่มีแถวนี้ — ตัวช่วยจะเพิ่มให้"),
            },
            {
                "id": "workday", "label": "ตั้งวันทำงานของกลุ่มนี้แล้ว",
                "status": wd_status,
                "detail": (f"ตั้งไว้ {wd_val} วัน/สัปดาห์" if wd_status == "ok"
                           else f"ตรวจไม่ได้: {wd_err}" if wd_status == "unknown"
                           else "ยังไม่ได้ตั้ง — ระบบจะใช้ค่าพื้นฐาน 6 วัน/สัปดาห์"),
            },
            {
                "id": "cap", "label": "มีกำลังผลิตในระบบ (จากฐานข้อมูล)",
                "status": cap_status, "detail": cap_detail,
            },
        ],
    }


def audit() -> dict:
    """ไฟตรวจของทั้งทะเบียน — กลุ่มไหนยังขาดวันทำงาน / ขาดกำลังผลิต
    ใช้โชว์แถบสรุปบนหน้า "เครื่องทั้งหมดที่โรงงานมี" """
    rows = mc_rows()
    try:
        wd = workday.get_workday()
        defaults = wd.get("defaults") or {}
        wd_ok = True
    except Exception:                                        # noqa: BLE001
        defaults, wd_ok = {}, False
    caps, cap_err = _cap_keys()

    # กลุ่มที่ไม่ใช่เครื่องทอของเรา (จ้างทอ/ตรวจ/ตัด) ไม่ต้องมีวันทำงาน-กำลังผลิต
    try:
        import sys
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        import McGroups
        skip = {_key(x) for x in McGroups.non_knit_groups(config.master_files().get("MasterMC"))}
    except Exception:                                        # noqa: BLE001
        skip = set()

    # คีย์ในแผงจัดตาม (โรงงาน, เกจ) ไว้ก่อน — ใช้เดาว่ากลุ่มที่หาไม่เจอ "เปลี่ยนชื่อ" มาหรือเปล่า
    # เคสจริง 24 ก.ย. 2569: MasterMC แก้สะกด SINGEL-16 → SINGLE-16 แต่แผงยังเป็นชื่อเก่า
    # ผล: WorkDay หาค่าไม่เจอ → ใช้ 6 วันแทน 5.5 เงียบ ๆ (pipeline ไม่เตือนเพราะ has_group ยัง True)
    panel_by_fg = {}
    for k in defaults:
        p = k.split("|")
        if len(p) == 3:
            panel_by_fg.setdefault((p[0], p[2]), []).append(p[1])

    no_wd, no_cap, cat_mismatch, renamed = [], [], [], []
    for r in rows:
        if _key(r["mc"]) in skip or _key(r["mc_cat"]) in skip:
            continue
        if not _norm(r["total"]) or _norm(r["total"]) in ("-", "0"):
            continue                                          # ไม่มีเครื่อง = ไม่ต้องตรวจ
        label = f"{r['mc_cat']} · {r['mc']} Guage {r['guage']}"
        # วันทำงาน: คีย์ใช้ค่าตามที่สะกดในไฟล์ (WorkDay.py เทียบกับ MasterMC ตรง ๆ ไม่ normalize)
        if wd_ok and f"{_key(r['factory'])}|{_key(r['mc_cat'])}|{_key(r['guage'])}" not in defaults:
            no_wd.append(label)
            # ในแผงมีกลุ่มของโรง+เกจเดียวกันที่ชื่อ "ใกล้เคียง" ไหม → น่าจะเป็นชื่อที่เปลี่ยนไป
            near = [c for c in panel_by_fg.get((_key(r["factory"]), _key(r["guage"])), [])
                    if _cat_key(c) == _cat_key(r["mc_cat"])]
            if near:
                renamed.append(f"{label} → ในแผงยังใช้ชื่อเดิมว่า \"{near[0]}\"")
        if caps is not None:
            keys, by_mc = caps
            gk = _gauge_key(r["guage"])
            if (_cat_key(r["mc_cat"]), _key(r["mc"]), gk) in keys:
                pass                                          # ตรงทั้ง 3 คีย์ = พร้อม
            elif by_mc.get((_key(r["mc"]), gk)):
                # มีผ้าผูกกับเครื่องนี้ แต่ฐานข้อมูลจัดคนละกลุ่ม — ไม่ใช่ "ขาด" แต่ควรรู้ไว้
                cat_mismatch.append(
                    f"{label} (ฐานข้อมูลจัดอยู่ "
                    f"{', '.join(sorted(by_mc[(_key(r['mc']), gk)]))[:60]})")
            else:
                no_cap.append(label)

    return {
        "total_rows": len(rows),
        "no_workday": sorted(set(no_wd)),
        "no_cap": sorted(set(no_cap)),
        "cat_mismatch": sorted(set(cat_mismatch)),
        # กลุ่มที่ "น่าจะเปลี่ยนชื่อ" — แผงยังเก็บค่าไว้ใต้ชื่อเดิม ค่าที่ตั้งไว้จึงไม่ถูกใช้
        "renamed": sorted(set(renamed)),
        "workday_ok": wd_ok,
        "cap_err": cap_err if caps is None else "",
    }


def rename_map() -> list:
    """คู่ "ชื่อกลุ่มในแผง (เก่า) → ชื่อในทะเบียนตอนนี้ (ใหม่)" ที่ทำให้ค่าวันทำงานหลุด

    เกิดเมื่อมีคนแก้ชื่อกลุ่มในทะเบียนเครื่อง แต่แผงวันทำงานยังเก็บค่าไว้ใต้ชื่อเดิม
    → WorkDay หาไม่เจอ ใช้ค่าพื้นฐานแทนค่าที่ตั้งไว้ (และ pipeline ไม่เตือน)
    คืนเฉพาะคู่ที่ "ชื่อเก่าไม่มีแถวไหนในทะเบียนใช้แล้ว" — ชื่อที่ยังมีของใช้อยู่ ห้ามย้าย
    """
    try:
        wd = workday.get_workday()
    except Exception:                                        # noqa: BLE001
        return []
    defaults = wd.get("defaults") or {}
    rows = mc_rows()
    live = {f"{_key(r['factory'])}|{_key(r['mc_cat'])}|{_key(r['guage'])}" for r in rows}

    # คีย์ในแผงจัดตาม (โรงงาน, เกจ) ไว้หาชื่อที่ "ใกล้เคียง"
    panel_by_fg = {}
    for k in defaults:
        p = k.split("|")
        if len(p) == 3:
            panel_by_fg.setdefault((p[0], p[2]), []).append(p[1])

    out, seen = [], set()
    for r in rows:
        new_key = f"{_key(r['factory'])}|{_key(r['mc_cat'])}|{_key(r['guage'])}"
        if new_key in defaults or new_key in seen:
            continue
        for cat_old in panel_by_fg.get((_key(r["factory"]), _key(r["guage"])), []):
            old_key = f"{_key(r['factory'])}|{cat_old}|{_key(r['guage'])}"
            # ชื่อเก่าต้องไม่มีแถวไหนในทะเบียนใช้อยู่แล้ว และต้องเป็นชื่อที่ "แปลงแล้วตรงกัน"
            if old_key in live:
                continue
            if _cat_key(cat_old) != _cat_key(r["mc_cat"]):
                continue
            out.append({
                "old": old_key, "new": new_key,
                "label": f"{r['mc_cat']} · {r['mc']} Guage {r['guage']}",
                "old_cat": cat_old, "new_cat": r["mc_cat"],
                "days": defaults.get(old_key),
            })
            seen.add(new_key)
            break
    return out


def fix_workday_names(user: str = "") -> dict:
    """ย้ายค่าในแผงวันทำงานจากชื่อกลุ่มเดิมไปชื่อปัจจุบัน (วัน + ชั่วโมง + ค่ารายสัปดาห์)

    ย้ายเฉพาะคู่ที่ rename_map() ยืนยันว่าชื่อเก่าไม่มีใครใช้แล้ว — ไม่เดาเอง
    คีย์เก่าถูกลบหลังย้าย เพื่อไม่ให้เหลือค่าค้างที่ไม่มีวันถูกใช้
    """
    pairs = rename_map()
    if not pairs:
        return {"ok": True, "moved": 0, "detail": []}
    wd = workday.get_workday()
    defaults = dict(wd.get("defaults") or {})
    hours = dict(wd.get("hours") or {})
    weeks = dict(wd.get("weeks") or {})
    week_hours = dict(wd.get("week_hours") or {})
    detail = []
    for p in pairs:
        old, new = p["old"], p["new"]
        moved = []
        if old in defaults:
            defaults[new] = defaults.pop(old)
            moved.append(f"{defaults[new]} วัน")
        if old in hours:
            hours[new] = hours.pop(old)
            moved.append(f"{hours[new]} ชม.")
        n_wk = 0
        for wk_key in [k for k in weeks if k.rsplit("|", 1)[0] == old]:
            weeks[f"{new}|{wk_key.rsplit('|', 1)[1]}"] = weeks.pop(wk_key)
            n_wk += 1
        if n_wk:
            moved.append(f"ค่ารายสัปดาห์ {n_wk} สัปดาห์")
        # ชั่วโมงเฉพาะสัปดาห์ต้องย้ายตามด้วย ไม่งั้นค้างอยู่ใต้ชื่อเก่าที่ไม่มีใครอ่านแล้ว
        n_wh = 0
        for wh_key in [k for k in week_hours if k.rsplit("|", 1)[0] == old]:
            week_hours[f"{new}|{wh_key.rsplit('|', 1)[1]}"] = week_hours.pop(wh_key)
            n_wh += 1
        if n_wh:
            moved.append(f"ชั่วโมงรายสัปดาห์ {n_wh} สัปดาห์")
        if moved:
            detail.append(f"{p['old_cat']} → {p['new_cat']} ({', '.join(moved)})")
    if not detail:
        return {"ok": True, "moved": 0, "detail": []}
    workday.save_workday(defaults, weeks, hours, wd.get("merges") or {},
                         week_hours=week_hours,
                         user=user, base_rev=livesync.rev(livesync.WORKDAY))
    return {"ok": True, "moved": len(detail), "detail": detail}


# ------------------------------------------------------------------- เพิ่ม
def create(data: dict, user: str = "") -> dict:
    """เพิ่มเครื่องใหม่ — เขียน Master MC (+ MC_Total ถ้าเริ่มมีกลางปี, + วันทำงานถ้าระบุ)

    ทำทีละขั้นและรายงานว่าทำอะไรไปแล้วบ้าง ถ้าขั้นหลังล้ม ขั้นก่อนหน้าที่สำเร็จยังอยู่
    (ไม่ rollback — ผู้ใช้เห็นรายงานแล้วตามแก้ได้ ดีกว่าลบของที่เพิ่งเขียนสำเร็จทิ้ง)
    """
    factory = _norm(data.get("factory"))
    mc_cat = _norm(data.get("mc_cat"))
    mc = _norm(data.get("mc"))
    guage = _norm(data.get("guage"))
    mtype = _norm(data.get("type"))
    group = _norm(data.get("group"))
    total = _norm(data.get("total_mc"))
    remark = _norm(data.get("remark"))
    start_week = data.get("start_week")
    work_days = data.get("work_days")
    work_hours = data.get("work_hours")

    miss = [n for n, v in (("โรงงาน", factory), ("ชนิดเครื่อง", mtype), ("กลุ่มเครื่อง", mc_cat),
                           ("เครื่อง", mc), ("Guage", guage), ("จำนวนเครื่อง", total)) if not v]
    if miss:
        return {"ok": False, "error": "ยังไม่ได้กรอก: " + ", ".join(miss)}
    try:
        total_n = int(float(total))
        if total_n < 0:
            raise ValueError
    except (TypeError, ValueError):
        return {"ok": False, "error": "จำนวนเครื่องต้องเป็นตัวเลข"}

    # สัปดาห์เริ่มมี (ถ้าส่งมา) ต้องเป็นเลขสัปดาห์ที่ใช้ได้จริง — ส่งค่าเพี้ยนมาแล้วปล่อยผ่าน
    # จะได้แถวที่ Master MC = 0 เครื่อง แต่ไม่มีสัปดาห์ไหนเปิดจำนวนให้เลย = เครื่องหายทั้งปี
    if start_week not in (None, ""):
        try:
            _w = int(float(start_week))
            if not 1 <= _w <= 53:
                raise ValueError
        except (TypeError, ValueError):
            return {"ok": False, "error": "สัปดาห์ที่เริ่มมีต้องเป็นตัวเลข 1-53"}
    else:
        start_week = None

    # กองใหม่ต้องมีวันทำงาน + ชั่วโมงเสมอ (user สั่ง 24 ก.ย. 2569)
    # ไม่งั้นได้กองที่ระบบใช้ 6 วัน 24 ชม. เงียบ ๆ แล้วไม่มีใครรู้ว่าไม่เคยตั้ง
    try:
        wd_now = workday.get_workday()
        pool_key = f"{_key(factory)}|{_key(mc_cat)}|{_key(guage)}"
        has_days = pool_key in (wd_now.get("defaults") or {})
        has_hours = pool_key in (wd_now.get("hours") or {})
    except Exception:                                        # noqa: BLE001
        has_days = has_hours = False
    need = []
    if not has_days and _norm(work_days) == "":
        need.append("วันทำงานต่อสัปดาห์")
    if not has_hours and _norm(work_hours) == "":
        need.append("ชั่วโมงทำงานต่อวัน")
    if need:
        return {"ok": False,
                "error": f"Cat {mc_cat} Guage {guage} ของโรง {factory} ยังไม่เคยตั้งค่า — "
                         f"ต้องกรอก {', '.join(need)} ด้วย"}
    for v, label, lo, hi in ((work_days, "วันทำงาน", 0, 7), (work_hours, "ชั่วโมงทำงาน", 0, 24)):
        if _norm(v) == "":
            continue
        try:
            if not lo <= float(v) <= hi:
                raise ValueError
        except (TypeError, ValueError):
            return {"ok": False, "error": f"{label}ต้องเป็นตัวเลข {lo}-{hi}"}

    columns, rows, rev = _master_rows()
    ck, mk, gk = _key(mc_cat), _key(mc), _key(guage)
    ci = {n: _col(columns, n) for n in columns}
    idx = {n: _col(columns, n) for n in
           ("Factory", "Type", "MC_CAT", "MC", "Guage", "Group", "SUBGroup", "Total MC", "REMARK")}
    for need in REQUIRED:
        if idx.get(need, -1) < 0:
            return {"ok": False, "error": f"ชีท Master MC ไม่มีคอลัมน์ '{need}'"}

    # กันเพิ่มซ้ำ: คีย์ CAT + MC + เกจ ซ้ำ = ระบบจะนับเครื่องซ้ำสองเท่า
    for r in rows:
        def g(n):
            i = idx[n]
            return r[i] if 0 <= i < len(r) else None
        if _key(g("MC_CAT")) == ck and _key(g("MC")) == mk and _key(g("Guage")) == gk:
            return {"ok": False, "error": f"มีเครื่องนี้อยู่แล้ว ({mc_cat} · {mc} Guage {guage}) — "
                                          "ถ้าจะเพิ่มจำนวน ให้แก้ช่อง 'จำนวนเครื่อง' ของแถวเดิมแทน"}

    new = [""] * len(columns)
    new[idx["Factory"]] = factory
    new[idx["Type"]] = mtype
    new[idx["MC_CAT"]] = mc_cat
    new[idx["MC"]] = mc
    new[idx["Guage"]] = guage
    # เริ่มมีกลางปี → ทะเบียนหลักใส่ 0 แล้วไปเปิดจำนวนจริงที่ "จำนวนเครื่องเฉพาะบางสัปดาห์"
    # ไม่งั้นระบบจะคิดว่ามีเครื่องครบตั้งแต่สัปดาห์แรกของแผน
    new[idx["Total MC"]] = 0 if start_week else total_n
    if idx["Group"] >= 0 and group:
        new[idx["Group"]] = group
    if idx["SUBGroup"] >= 0 and group:
        new[idx["SUBGroup"]] = group
    if idx["REMARK"] >= 0 and remark:
        new[idx["REMARK"]] = remark

    done, warn = [], []
    res = masters.write_sheet("MasterMC", MASTER_SHEET, columns, rows + [new],
                              user=user, base_rev=rev)
    done.append("เพิ่มลงทะเบียนเครื่อง (Master MC)")

    # ---- จำนวนเครื่องเฉพาะบางสัปดาห์ (เครื่องเข้ากลางปี)
    if start_week:
        try:
            wk = int(float(start_week))
            d = masters.read_sheet("MasterMC", MC_TOTAL_SHEET)
            cols2, rows2 = d["columns"], d["rows"]
            i2 = {n: _col(cols2, n) for n in ("Week", "Factory", "MC_CAT", "MC", "Guage",
                                              "Total MC", "Remark", "Year")}
            # ชีทนี้เดิมไม่มีคอลัมน์ MC / Year → เติมให้ (แถวเก่าเว้นว่าง = พฤติกรรมเดิม)
            for name in ("MC", "Year"):
                if i2[name] < 0:
                    cols2 = list(cols2) + [name]
                    rows2 = [list(r) + [""] for r in rows2]
                    i2[name] = len(cols2) - 1
            # ⚠️ MC_Total มีผล "เฉพาะสัปดาห์ที่ระบุ" — ใส่สัปดาห์เดียวแล้วสัปดาห์ถัดไปจะกลับไปใช้
            # Total MC ของ Master MC (ที่เราใส่ 0 ไว้) = เครื่องหายทั้งที่ของมาถึงแล้ว
            # จึงต้องกางช่วงถึงสัปดาห์สุดท้ายของปฏิทิน (คอลัมน์ Week To / Year To ที่ AVA_MC อ่าน)
            for name in ("Week To", "Year To"):
                if _col(cols2, name) < 0:
                    cols2 = list(cols2) + [name]
                    rows2 = [list(r) + [""] for r in rows2]
                i2[name] = _col(cols2, name)
            last_w, last_y = _calendar_last_week()
            start_y = _norm(data.get("start_year"))
            row2 = [""] * len(cols2)
            for name, val in (("Week", wk), ("Factory", factory), ("MC_CAT", mc_cat),
                              ("MC", mc), ("Guage", guage), ("Total MC", total_n),
                              ("Year", start_y),
                              ("Week To", last_w or ""), ("Year To", last_y or ""),
                              ("Remark", remark or f"เครื่องใหม่เข้า W{wk}")):
                if i2.get(name, -1) >= 0:
                    row2[i2[name]] = val
            masters.write_sheet("MasterMC", MC_TOTAL_SHEET, cols2, rows2 + [row2],
                                user=user, base_rev=d.get("rev"))
            done.append(f"ตั้งจำนวนเครื่อง {total_n} ตัว ตั้งแต่สัปดาห์ {wk}")
        except Exception as e:                               # noqa: BLE001
            warn.append(f"เพิ่มจำนวนเครื่องรายสัปดาห์ไม่สำเร็จ: {e}")

    # ---- วันทำงาน + ชั่วโมงของกลุ่มใหม่
    if _norm(work_days) != "" or _norm(work_hours) != "":
        try:
            wd = workday.get_workday()
            defaults = dict(wd.get("defaults") or {})
            hrs = dict(wd.get("hours") or {})
            pk = f"{_key(factory)}|{_key(mc_cat)}|{_key(guage)}"
            if _norm(work_days) != "":
                defaults[pk] = float(work_days)
            if _norm(work_hours) != "":
                hrs[pk] = float(work_hours)
            # ส่งเลขรุ่นไปด้วย — แผงทั้งแผงถูกเขียนทับ ถ้าอีกคนเพิ่งแก้ต้องไม่ถูกกลืนหาย
            workday.save_workday(defaults, wd.get("weeks") or {}, hrs,
                                 wd.get("merges") or {}, user=user,
                                 base_rev=livesync.rev(livesync.WORKDAY))
            got = []
            if _norm(work_days) != "":
                got.append(f"{work_days} วัน/สัปดาห์")
            if _norm(work_hours) != "":
                got.append(f"{work_hours} ชม./วัน")
            done.append("ตั้ง" + " · ".join(got))
        except Exception as e:                               # noqa: BLE001
            warn.append(f"ตั้งวันทำงาน/ชั่วโมงไม่สำเร็จ: {e}")

    after = check(factory, mc_cat, mc, guage)
    return {"ok": True, "done": done, "warn": warn, "check": after, "rev": res.get("rev")}

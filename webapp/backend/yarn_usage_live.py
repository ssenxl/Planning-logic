"""
yarn_usage_live.py — Dashboard › Yarn Usage : ด้ายแต่ละตัว (เช่น TC20/1R) lot ไหน ใช้ไปกี่ kg ในสัปดาห์ไหน

ด้ายเป็นตัวตั้ง (user สั่ง 9 ต.ค. 2569: "ไม่ใช่มาตาม Item แต่ต้องแยกด้วย yarn" + "ต้องเป็นตาม week")
→ แถว = ด้าย × lot รวมจากผ้าทุก ITEM ที่ใช้ด้ายนั้น · ยอดแตกรายสัปดาห์

ยอดทุกตัวมาจาก booking (Booking/view_booking.xlsx — ไฟล์เดียวกับที่ pipeline ดึง, 7 สัปดาห์ล่าสุด)
เฉพาะแถวที่ออกใบ KP แล้ว (แถวที่ยังไม่มี KP_NO ไม่มี lot และยอดทอ = 0 ทุกแถว) ไม่รวมงานปก/ขอบ (COLLAR)
การแยกว่าใบ KP ใช้ด้ายอะไร lot อะไร กี่ % มาจาก Yarn/view_yarn_by_joknit.xlsx (View_Yarn_JoKnit.py)
— booking เองมีแค่รายชื่อด้าย/รายชื่อ lot ที่จับคู่กันไม่ได้ และไม่มี % (ดูหัวไฟล์ View_Yarn_JoKnit.py)

ต่อ 1 ใบ KP ใน 1 สัปดาห์ (ยอดผ้า kg จาก booking) × % ของด้ายแต่ละเส้น:
    ต้องใช้ = KP_WEIGHT × %      ใช้ไปแล้ว = KNIT_WEIGHT × %      เหลือต้องใช้ = ยอดที่ยังไม่ได้ทอ × %
ไม่บวก % สูญเสียตอนทอ (view มี PERCENT_LOSS_KNIT ~0.4%)
⚠️ "สัปดาห์" = สัปดาห์ของแถว booking ที่ยอดทอถูกลงไว้ — booking ไม่มีวันที่เดินเครื่องจริง

ใบ KP ที่ view ด้ายต่อใบ KP ไม่มี → ใช้ % จาก BOM แทน (Yarn/view_bom_yarn_pct.xlsx, ดึงโดย View_Yarn_JoKnit.py เหมือนกัน)
— view ด้ายต่อใบ KP ไม่เก็บใบจ้างทอ (KCK… ของ COMKN) เลยสักใบ แต่ BOM มีครบ ; BOM ไม่มี lot → ลงแถว lot "-"
(user ถาม 9 ต.ค. 2569 "ทำไมแยกไม่ได้" แล้วให้ใช้ BOM เป็นตัวสำรอง — ก่อนหน้านั้นงานจ้างทอ 82,008 kg ค้างไม่เข้าด้ายตัวไหน)
ใบ KP ที่ไม่มีทั้งสองที่ (ใบใหม่ที่ยังไม่จัดด้าย) ไม่ถูกทิ้ง: ลงเป็นกลุ่ม "ยังแยกเส้นไม่ได้"
ยอด = ยอดผ้าทั้งใบ ชื่อ = YARN_USED ของ booking ตามที่เขียนมา (ไม่เดา % เอง — booking ลงด้ายไม่ครบทุกเส้นก็มี
เช่น ไม่มีสแปนเด็กซ์) → ยอดรวมของทุกแถว = ยอดผ้าใน booking เสมอ
"""
import config

_BOOKING = config.REPO_DIR / "Booking" / "view_booking.xlsx"
_SPLIT = config.REPO_DIR / "Yarn" / "view_yarn_by_joknit.xlsx"
_SPLIT_SHEET = 0   # ชีทแรก — ไฟล์มีชีทเดียว (ชื่อชีท = ชื่อ view ไม่อ้างในโค้ด)
_BOM = config.REPO_DIR / "Yarn" / "view_bom_yarn_pct.xlsx"
_BOM_SHEET = 0
_NEED_BK = ("KP_NO", "ITEM_CODE", "YEAR", "WEEK", "KP_WEIGHT", "KNIT_WEIGHT")
# คอลัมน์ของไฟล์แยกด้าย เรียง (ใบ KP, ITEM ผ้า, ด้าย, lot, %) — BOM ไม่มี lot
_COLS_SP = ("KP_NO", "ITEM_CODE", "YARN_CODE", "YARN_LOT", "YARN_CONTAIN")
_COLS_BOM = ("KP_NO", "KNIT_ITEM", "YARN_ITEM", None, "ITEM_CONTAIN")
# ใบ KP ที่ยอดทอมากกว่ายอดใบเกินเท่านี้ (%) = นับเป็น "ทอเกินยอดใบ" ให้หน้าเว็บลงสีเตือน
# booking มีจริง ~5% ของใบ (ทอเกินแผน หรือยอดใบถูกแก้ลดหลังทอ เช่น ใบ 1,560 kg ทอไป 3,114 kg)
# โชว์ตัวเลขตาม booking ไม่แก้ให้ — แค่บอกว่ามี จะได้ไม่งงว่าทำไม "ใช้ไปแล้ว" มากกว่า "ต้องใช้ทั้งใบ"
_OVER_PCT = 10

_cache: dict = {}


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
    return 0.0 if f != f else f      # NaN → 0


def _mtime(p):
    try:
        return p.stat().st_mtime
    except OSError:
        return None


def _group_knit(kpw: list, sch: list, knit: list) -> float:
    """ยอดทอของใบ KP ในสัปดาห์เดียว จากแถว booking ของกลุ่ม (KP_NO, YEAR, WEEK)

    booking แตกใบ KP เป็นหลายแถวในสัปดาห์เดียวกันได้ (คนละ SO / คนละส่วน) แล้วเขียน KNIT_WEIGHT กับ
    SCHEDULE_WEIGHT เป็น "ยอดทั้งกลุ่ม" ซ้ำทุกแถว → รวมตรง ๆ ได้ 2–3 เท่า
    (วัด 9 ต.ค. 2569: 88 จาก 184 ใบที่มีหลายแถว ยอดทอเกินยอดใบ > 8% ; ใช้กติกานี้แล้วเหลือ 8 ใบ = ทอเกินจริง)
    แยกสองแบบด้วย SCHEDULE_WEIGHT: เท่ากับ KP_WEIGHT ของแถวตัวเอง = ยอดรายแถว (งานจ้างทอ KCK…) → รวมได้
    ไม่เท่า = ยอดทั้งกลุ่ม → นับครั้งเดียว ; ใบที่ลากหลายสัปดาห์เป็นยอดรายสัปดาห์อยู่แล้ว จึงรวมข้ามสัปดาห์ตามปกติ"""
    if len(knit) > 1 and len(set(knit)) == 1 and len(set(sch)) == 1:
        per_row = sch[0] is not None and all(abs(w - sch[0]) <= 0.5 for w in kpw)
        if not per_row:
            return knit[0]
    return sum(knit)


def _kp_weeks(df) -> list:
    """booking → ยอดผ้า (kg) ต่อ (ใบ KP, สัปดาห์): [{kp, yw, item, yarn, kpw, knit, rem}]
    yw = 'YYYY/WW' (มีปีเสมอ — เลขสัปดาห์เปล่าปนกันข้ามปี)
    rem = ยอดที่ยังไม่ได้ทอ: ใบที่ปิดแล้ว = 0 · ทอเกินยอดใบ = 0 (ไม่ติดลบไปหักสัปดาห์/ใบอื่น)"""
    has_sch, has_st = "SCHEDULE_WEIGHT" in df.columns, "STATUS" in df.columns
    groups: dict = {}
    for r in df.to_dict("records"):
        kp, year, week = _s(r.get("KP_NO")).upper(), int(_f(r.get("YEAR"))), int(_f(r.get("WEEK")))
        if not kp or not year or not week:
            continue
        g = groups.setdefault((kp, f"{year}/{week:02d}"), {
            "item": _s(r.get("ITEM_CODE")), "yarn": _s(r.get("YARN_USED")),
            "kpw": [], "sch": [], "knit": [], "open": False})
        g["kpw"].append(_f(r.get("KP_WEIGHT")))
        g["knit"].append(_f(r.get("KNIT_WEIGHT")))
        sch = r.get("SCHEDULE_WEIGHT") if has_sch else None
        g["sch"].append(None if sch is None or sch != sch else _f(sch))
        # ไม่มีคอลัมน์ STATUS (ไฟล์รุ่นเก่า) = ถือว่ายังไม่ปิดทุกใบ
        if not has_st or _s(r.get("STATUS")).lower() != "closed":
            g["open"] = True

    out = []
    for (kp, yw), g in groups.items():
        kpw, knit = sum(g["kpw"]), _group_knit(g["kpw"], g["sch"], g["knit"])
        out.append({"kp": kp, "yw": yw, "item": g["item"], "yarn": g["yarn"], "kpw": kpw, "knit": knit,
                    "rem": max(kpw - knit, 0.0) if g["open"] else 0.0})
    return out


def _splits(df, cols=_COLS_SP) -> dict:
    """ไฟล์แยกด้าย → {(KP_NO, ITEM ผ้า): [(ด้าย, lot, %), …]} ; cols = ชื่อคอลัมน์ (ใบ KP, ITEM ผ้า, ด้าย, lot, %)
    ไม่มีคอลัมน์ lot (BOM) หรือช่อง lot ว่าง = lot "-" """
    c_kp, c_item, c_yarn, c_lot, c_pct = cols
    out: dict = {}
    seen = set()
    for r in df.to_dict("records"):
        kp, item = _s(r.get(c_kp)).upper(), _s(r.get(c_item)).upper()
        yarn, pct = _s(r.get(c_yarn)), _f(r.get(c_pct))
        lot = (_s(r.get(c_lot)) if c_lot else "") or "-"
        k = (kp, item, yarn, lot, pct)
        if not kp or not yarn or k in seen:
            continue
        seen.add(k)
        out.setdefault((kp, item), []).append((yarn, lot, pct))
    return out


def _read_splits(path, sheet, cols: tuple, notes: list, what: str) -> dict:
    """อ่านไฟล์แยกด้าย 1 ไฟล์ — ไม่มีไฟล์/อ่านไม่ได้/คอลัมน์ไม่ครบ = {} พร้อมจดเหตุผลลง notes (หน้าเว็บโชว์เป็นแถบเตือน)"""
    import pandas as pd

    if _mtime(path) is None:
        # ปุ่มอยู่บนหน้า Yarn Usage (runner โหมด yarn-usage) — บนเว็บไม่มีปุ่มสั่งโหมด yarn ตรง ๆ
        notes.append(f"ยังไม่มีไฟล์ {path.name} ({what}) — กดปุ่ม “ดึงข้อมูลด้าย” บนหน้านี้ (admin) หรือรอรอบดึงอัตโนมัติ")
        return {}
    try:
        try:
            df = pd.read_excel(path, sheet_name=sheet)
        except ValueError:
            df = pd.read_excel(path)       # ชื่อชีทเปลี่ยน → ชีทแรกก็ยังดีกว่าไม่มีข้อมูล
    except Exception as e:
        notes.append(f"อ่านไฟล์ {path.name} ไม่ได้: {e}")
        return {}
    miss = [c for c in cols if c and c not in df.columns]
    if miss:
        notes.append(f"ไฟล์ {path.name} ไม่มีคอลัมน์ {', '.join(miss)} ({what})")
        return {}
    return _splits(df, cols)


def _acc() -> dict:
    return {"need": 0.0, "used": 0.0, "rem": 0.0, "kps": set(), "w": {}}


def _add(a: dict, g: dict, pct: float) -> None:
    a["need"] += g["kpw"] * pct / 100
    a["used"] += g["knit"] * pct / 100
    a["rem"] += g["rem"] * pct / 100
    a["kps"].add(g["kp"])
    w = a["w"].setdefault(g["yw"], [0.0, 0.0])
    w[0] += g["knit"] * pct / 100
    w[1] += g["kpw"] * pct / 100


def _cells(a: dict, over_kps: set) -> dict:
    """ยอดของ 1 แถว: need/used/rem รวมทุกสัปดาห์ · k = จำนวนใบ KP · o = ใบที่ทอเกินยอดใบ
    w = {สัปดาห์: [ใช้ไปแล้ว, ต้องใช้]} เฉพาะสัปดาห์ที่มีใบ KP (ไม่มี = หน้าเว็บโชว์ '-')"""
    return {
        "need": round(a["need"], 3), "used": round(a["used"], 3), "rem": round(a["rem"], 3),
        "k": len(a["kps"]), "o": len(a["kps"] & over_kps),
        "w": {yw: [round(u, 3), round(n, 3)] for yw, (u, n) in sorted(a["w"].items())},
    }


def table() -> dict:
    import pandas as pd

    bk_m, sp_m, bom_m = _mtime(_BOOKING), _mtime(_SPLIT), _mtime(_BOM)
    src = {"booking": {"file": _BOOKING.name, "mtime": bk_m}, "split": {"file": _SPLIT.name, "mtime": sp_m},
           "bom": {"file": _BOM.name, "mtime": bom_m}}
    empty = {"source": src, "weeks": [], "yarns": [], "rows": [], "stats": {}}
    if bk_m is None:
        return {**empty, "note": f"ยังไม่มีไฟล์ {_BOOKING.name} — รอรอบดึงอัตโนมัติ หรือสั่ง “ดึง Booking”"}

    key = (bk_m, sp_m, bom_m)
    hit = _cache.get(key)
    if hit is not None:
        return hit

    try:
        bk = pd.read_excel(_BOOKING)
    except Exception as e:
        return {**empty, "note": f"อ่านไฟล์ {_BOOKING.name} ไม่ได้: {e}"}
    miss = [c for c in _NEED_BK if c not in bk.columns]
    if miss:
        return {**empty, "note": f"ไฟล์ {_BOOKING.name} ไม่มีคอลัมน์ {', '.join(miss)}"}
    # งานปก/ขอบ (COLLAR) ไม่เอา: booking เก็บยอดเป็น "ชิ้น" ไม่ใช่ kg และ KNIT_WEIGHT = 0 ทุกแถว
    # ดู TYPE อย่างเดียวไม่พอ — ใบปกของ FQC (FQCCL-OM) ลง TYPE เป็น YARN-DYE (วัด 9 ต.ค. 2569: 20 ใบ)
    # → ตัดใบที่เลข KP ขึ้นต้น CP ด้วย (ใบปกทุกใบขึ้นต้น CP ; ใบผ้าขึ้นต้น KP / KCK)
    collar = bk["KP_NO"].astype(str).str.strip().str.upper().str.startswith("CP")
    if "TYPE" in bk.columns:
        collar |= bk["TYPE"].astype(str).str.strip().str.upper() == "COLLAR"
    groups = _kp_weeks(bk[~collar])
    if not groups:
        return {**empty, "note": f"ไฟล์ {_BOOKING.name} ไม่มีแถวที่ออกใบ KP แล้ว"}

    notes: list = []
    splits = _read_splits(_SPLIT, _SPLIT_SHEET, _COLS_SP, notes, "ด้าย + lot + % ต่อใบ KP")
    # ตัวสำรอง: เอาเฉพาะใบที่ % รวม = 100 (วัด 9 ต.ค. 2569 ครบ 100 ทั้ง 9,073 ใบ — กันไว้เผื่อ view เปลี่ยน
    # ใบที่รวมไม่ครบถ้าเอามาใช้ ยอดด้ายจะหาย/งอกจากยอดผ้า)
    bom = {k: v for k, v in _read_splits(_BOM, _BOM_SHEET, _COLS_BOM, notes, "ตัวสำรอง % ด้ายของใบจ้างทอ").items()
           if abs(sum(p for _, _, p in v) - 100) <= 0.5}

    # ใบ KP ที่ทอเกินยอดใบ ตัดสินจากยอดทั้งใบ (รวมทุกสัปดาห์) ไม่ใช่รายสัปดาห์
    kp_tot: dict = {}
    for g in groups:
        t = kp_tot.setdefault(g["kp"], [0.0, 0.0])
        t[0] += g["kpw"]
        t[1] += g["knit"]
    over_kps = {kp for kp, (w, k) in kp_tot.items() if k > w * (1 + _OVER_PCT / 100) + 1}   # +1 kg กันใบเล็กที่เกินแค่เศษ

    lots: dict = {}       # (แยกเส้นไม่ได้, ด้าย, lot) → ยอด
    yarns: dict = {}      # (แยกเส้นไม่ได้, ด้าย) → ยอดรวมทุก lot ของด้ายนั้น
    nosplit_kps: set = set()
    bom_kps: set = set()
    st = {"need": 0.0, "used": 0.0, "nosplit_need": 0.0, "nosplit_used": 0.0, "bom_need": 0.0, "bom_used": 0.0}
    for g in groups:
        st["need"] += g["kpw"]
        st["used"] += g["knit"]
        k = (g["kp"], g["item"].upper())
        parts = splits.get(k)
        if not parts and k in bom:
            # view ด้ายต่อใบ KP ไม่มีใบนี้ (งานจ้างทอ) → % จาก BOM, ไม่ทราบ lot
            parts = bom[k]
            bom_kps.add(g["kp"])
            st["bom_need"] += g["kpw"]
            st["bom_used"] += g["knit"]
        nosplit = not parts
        if nosplit:
            # ไม่มีทั้งสองที่ → ยอดผ้าทั้งใบลงกลุ่ม "ยังแยกเส้นไม่ได้" (ชื่อตามที่ booking เขียน)
            parts = [(g["yarn"] or "-", "-", 100.0)]
            nosplit_kps.add(g["kp"])
            st["nosplit_need"] += g["kpw"]
            st["nosplit_used"] += g["knit"]
        for yarn, lot, p in parts:
            _add(lots.setdefault((nosplit, yarn, lot), _acc()), g, p)
            _add(yarns.setdefault((nosplit, yarn), _acc()), g, p)

    # ด้ายจริงเรียง A→Z ก่อน แล้วค่อยกลุ่มที่ยังแยกเส้นไม่ได้ (False < True)
    ykeys = sorted(yarns)
    yidx = {k: i for i, k in enumerate(ykeys)}
    out = {
        "source": src,
        "weeks": sorted({g["yw"] for g in groups}),
        # x = ยังแยกเส้นไม่ได้ (ชื่อเป็นข้อความ YARN_USED ของ booking ไม่ใช่รหัสด้ายตัวเดียว)
        "yarns": [{"y": y, "x": x, **_cells(yarns[(x, y)], over_kps)} for x, y in ykeys],
        # g = ลำดับใน yarns ของด้ายที่แถว lot นี้สังกัด
        "rows": [{"g": yidx[(x, y)], "l": lot, **_cells(a, over_kps)} for (x, y, lot), a in sorted(lots.items())],
        # kp_bom = ใบที่ใช้ % จาก BOM (อยู่ในแถว lot "-") · kp_nosplit = ใบที่ยังแยกเส้นไม่ได้
        "stats": {"kp": len(kp_tot), "kp_nosplit": len(nosplit_kps), "kp_bom": len(bom_kps),
                  **{k: round(v, 3) for k, v in st.items()}},
        "over_pct": _OVER_PCT,
    }
    if notes:
        out["note"] = " · ".join(notes)
    _cache.clear()          # แคชไว้ชุดเดียวพอ (ไฟล์เปลี่ยน = mtime เปลี่ยน)
    _cache[key] = out
    return out

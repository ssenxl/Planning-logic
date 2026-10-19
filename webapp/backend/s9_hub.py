"""
s9_hub.py — "จ้างทอ" ที่เดียวจบ: เครื่องของผู้รับจ้าง · ผ้าที่จ้างทอได้ · ผ้าที่ต้องจ้างเสมอ

เดิมกระจายอยู่ 3 ชีทในไฟล์ MasterMC ที่ผู้ใช้ต้องรู้ชื่อเองว่าอะไรอยู่ไหน:
  MC S9    เครื่องของผู้รับจ้างแต่ละเจ้า (Supp · Mc · New CAT · New Gauge · Total Mc · ...)
  Item S9  ผ้าที่ "จ้างทอได้" ถ้าในบ้านไม่ทัน — 5 พันกว่าแถว ; ปกติเอาข้อมูลชุดใหม่มาวางทับทั้งชีท
           (นำเข้าทับทั้งชีท) + เพิ่ม/ลบ/แก้ CAP ทีละแถวได้ — แถวที่เติมเองหายตอนนำเข้ารอบถัดไป
  S9 Only  ผ้าที่ "ต้องจ้างทอเสมอ" ทอในบ้านไม่ได้ — ไม่กี่สิบแถว กรอกมือ

หน้ารวมจัดตาม "ผู้รับจ้าง" (Supp) เพราะเป็นหน่วยที่คนใช้งานคิดจริง แล้วแยกส่วนผ้าไว้ต่างหาก
เขียนกลับลงชีทเดิมทั้งหมด — Planning.py อ่านที่เดิม ไม่ต้องแก้ engine
  Planning.py:2393 (S9 Only) · :2396 (MC S9) · :2402 (Item S9)
"""
import re

import livesync
import masters

MC_FILE = "MasterMC"
SH_MC = "MC S9"
SH_ITEM = "Item S9"
SH_ONLY = "S9 Only"


def _s(v) -> str:
    return str(v if v is not None else "").strip()


def _key(v) -> str:
    s = _s(v).upper().replace("\xa0", "")
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _num(v):
    s = _s(v)
    if not s or s in ("-", "nan", "NaN"):
        return None
    try:
        f = float(s)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return None


def _cols(columns, *names):
    up = [_key(c) for c in columns]
    out = []
    for n in names:
        try:
            out.append(up.index(_key(n)))
        except ValueError:
            out.append(-1)
    return out


def _get(row, i):
    return row[i] if 0 <= i < len(row) else None


def _mc_gauge(v, cat=""):
    """ช่องเกจของชีท MC S9 → (เลขเกจ, ข้อผิด|None) — เก็บเลขเกจอย่างเดียวแบบหน้าเครื่องในบ้าน (user 2 ต.ค. 2569)

    ไฟล์สรุปเครื่องจ้างทอเขียนกระบอกรวมมาด้วย `28G34"` (เกจ 28 กระบอก 34 นิ้ว) แต่ระบบวางแผนใช้แค่เลขหน้า G
    (Planning._s9_gauge_num) ขนาดกระบอกที่ใช้จริงมาจาก New CAT (DOUBLE-34) → ตัดส่วนกระบอกทิ้ง
    กระบอกขัดกับ CAT = ข้อผิด ห้ามบันทึก (เคยเจอ: รุ่งสยาม CAT DOUBLE-30 แต่เกจ 28G34" — ระบบเชื่อ CAT เงียบ ๆ)
    """
    s = _s(v).replace("\xa0", "").replace(" ", "")
    if not s or s.upper() in ("NAN", "NONE"):
        return "", None
    m = re.fullmatch(r'(\d+)(?:\.0+)?(?:[Gg](\d+)?["”″]*)?', s)
    if not m:
        return s, f"Guage {s} ต้องเป็นตัวเลข เช่น 28"
    g, cyl = int(m.group(1)), m.group(2)
    c = re.search(r"(\d+)\s*$", _s(cat))
    if cyl and c and int(c.group(1)) != int(cyl):
        return g, f'Guage {s} บอกกระบอก {cyl}" แต่กลุ่มเครื่องเป็น {_s(cat)} — แก้ให้ตรงกันก่อน'
    return g, None


def _gsort(g):
    """เรียงเกจแบบตัวเลข (9 มาก่อน 22) — ค่าที่ไม่ใช่เลขไปท้าย"""
    s = _s(g)
    return (0, int(s), "") if s.isdigit() else (1, 0, s)


# ---- ชีท Item S9: ชื่อหัวคอลัมน์ที่ Planning.py อ่านตรงตัว (ตัวพิมพ์ต้องตรง) + ชื่ออื่นที่ยอมรับตอนนำเข้า
#   หัวภาษาไทยตรงกับหัวตารางบนเว็บ (user 24 ก.ย. 2569: ไฟล์ที่ user เตรียมใช้หัวชุดนี้)
ITEM_HDR = {
    "ITEM_CODE": ("ITEM_CODE", "ITEM CODE", "ITEM", "รหัสผ้า"),
    "MACHINE_GROUP": ("MACHINE_GROUP", "MACHINE GROUP", "MC GROUP", "MC", "กลุ่มเครื่อง"),
    "CAT": ("CAT", "MC_CAT", "กลุ่ม"),
    "MC_GAUGE": ("MC_GAUGE", "MC GAUGE", "GAUGE", "GUAGE", "MC_GUAGE", "เกจ"),
    "CAP": ("CAP", "CAP/DAY", "CAP / DAY", "กำลังผลิต/วัน"),
}
# ข้อมูลไม่ครบห้ามบันทึก (user 24 ก.ย. 2569) — ต้องมีครบ 5 คอลัมน์ และทุกแถวกรอกครบ
ITEM_NEED = ("ITEM_CODE", "MACHINE_GROUP", "CAT", "MC_GAUGE", "CAP")
ITEM_LABEL = {"ITEM_CODE": "Item", "MACHINE_GROUP": "MC", "CAT": "CAT",
              "MC_GAUGE": "Guage", "CAP": "Cap/Day"}
# ฟิลด์ที่หน้าเว็บส่งมาตอนแก้ทีละแถว (แก้ได้ทุกคอลัมน์) → ชื่อบนจอ
ITEM_FIELDS = (("item", "Item"), ("mc_group", "MC"), ("mc_cat", "CAT"),
               ("guage", "Guage"), ("cap", "Cap/Day"))


def _canon_cols(columns) -> list:
    """หัวคอลัมน์ที่นำเข้า → ชื่อมาตรฐาน (ITEM_CODE / MC_GAUGE / CAP ...) คอลัมน์อื่นคงชื่อเดิม"""
    alias = {_key(a): std for std, names in ITEM_HDR.items() for a in names}
    return [alias.get(_key(c), _s(c)) for c in columns]


def _pos(v):
    """ตัวเลข > 0 ไม่งั้น None (CAP ว่าง/0/ติดลบ = Planning ถือว่าไม่มี)"""
    n = _num(v)
    return n if n is not None and n > 0 else None


def check_items(columns, rows) -> dict:
    """ตรวจข้อมูลชุดที่จะวางทับชีท Item S9 — ใช้ทั้งตอนดูตัวอย่างและตอนบันทึกจริง"""
    cols = _canon_cols(columns)
    miss = [n for n in ITEM_NEED if n not in cols]
    if miss:
        return {"ok": False, "error": "ไม่พบคอลัมน์ " + ", ".join(ITEM_LABEL[n] for n in miss)
                + " — แถวแรกต้องเป็นหัวคอลัมน์: Item · MC · CAT · Guage · กำลังผลิต/วัน"}
    at = {n: cols.index(n) for n in ITEM_NEED}
    keep, seen, bad = [], set(), []
    dup = 0
    for n, r in enumerate(rows, start=2):          # เลขแถวตาม Excel (แถว 1 = หัวคอลัมน์)
        r = (list(r) + [""] * (len(cols) - len(r)))[:len(cols)]
        if not any(_s(v) for v in r):
            continue                               # แถวว่างทั้งแถว ข้าม
        item = _key(r[at["ITEM_CODE"]])
        r[at["ITEM_CODE"]] = item
        lack = [ITEM_LABEL[f] for f in ("ITEM_CODE", "MACHINE_GROUP", "CAT", "MC_GAUGE")
                if not _key(r[at[f]]) or _key(r[at[f]]) == "NAN"]
        if _pos(r[at["CAP"]]) is None:
            lack.append("Cap/Day (ต้องมากกว่า 0)")
        if lack:
            bad.append(f"แถว {n}" + (f" {item}" if item else "") + f": ขาด {', '.join(lack)}")
            continue
        g = _key(r[at["MC_GAUGE"]])
        if (item, g) in seen:
            dup += 1
        seen.add((item, g))
        keep.append(r)
    if bad:
        return {"ok": False, "bad": bad, "error": f"ข้อมูลไม่ครบ {len(bad)} แถว — ไม่บันทึก · "
                + " · ".join(bad[:5]) + (f" (และอีก {len(bad) - 5} แถว)" if len(bad) > 5 else "")}
    if not keep:
        return {"ok": False, "error": "ไม่มีแถวข้อมูล (ต้องมีรหัสผ้าอย่างน้อย 1 แถว)"}
    warn = [f"{dup} แถวรหัสผ้า+Guage ซ้ำกัน — ระบบใช้แถวแรก"] if dup else []
    return {"ok": True, "columns": cols, "rows": keep, "count": len(keep), "warn": warn}


# ---- ชีท S9 Only: คอลัมน์ชุดเดียวกับ Item S9 แต่ MC เว้นว่างได้ (Planning ไม่ได้ใช้ MACHINE_GROUP)
#   คีย์ = รหัสผ้าอย่างเดียว (Planning:2459 ใช้แถวแรกของรหัสผ้า) → รหัสซ้ำในไฟล์ = ไม่บันทึก
ONLY_NEED = ("ITEM_CODE", "CAT", "MC_GAUGE", "CAP")


def check_only(columns, rows) -> dict:
    """ตรวจข้อมูลชุดที่จะวางทับชีท S9 Only — ใช้ทั้งตอนดูตัวอย่างและตอนบันทึกจริง"""
    cols = _canon_cols(columns)
    miss = [n for n in ONLY_NEED if n not in cols]
    if miss:
        return {"ok": False, "error": "ไม่พบคอลัมน์ " + ", ".join(ITEM_LABEL[n] for n in miss)
                + " — แถวแรกต้องเป็นหัวคอลัมน์: Item · MC · CAT · Guage · กำลังผลิต/วัน (MC ไม่มีก็ได้)"}
    if "MACHINE_GROUP" not in cols:
        cols.append("MACHINE_GROUP")
    at = {n: cols.index(n) for n in ITEM_NEED}
    keep, seen, bad, dups = [], set(), [], []
    for n, r in enumerate(rows, start=2):          # เลขแถวตาม Excel (แถว 1 = หัวคอลัมน์)
        r = (list(r) + [""] * (len(cols) - len(r)))[:len(cols)]
        if not any(_s(v) for v in r):
            continue                               # แถวว่างทั้งแถว ข้าม
        item = _key(r[at["ITEM_CODE"]])
        lack = [ITEM_LABEL[f] for f in ("ITEM_CODE", "CAT", "MC_GAUGE")
                if not _key(r[at[f]]) or _key(r[at[f]]) == "NAN"]
        if _pos(r[at["CAP"]]) is None:
            lack.append("Cap/Day (ต้องมากกว่า 0)")
        if lack:
            bad.append(f"แถว {n}" + (f" {item}" if item else "") + f": ขาด {', '.join(lack)}")
            continue
        g = _key(r[at["MC_GAUGE"]])
        mg = _key(r[at["MACHINE_GROUP"]])
        r[at["ITEM_CODE"]] = item
        r[at["MC_GAUGE"]] = _num(g) if _num(g) is not None else g   # C0 คงเป็นข้อความ
        r[at["CAT"]] = _key(r[at["CAT"]])
        r[at["MACHINE_GROUP"]] = "" if mg == "NAN" else mg
        r[at["CAP"]] = _pos(r[at["CAP"]])
        if item in seen:
            dups.append(f"แถว {n} {item}")
        seen.add(item)
        keep.append(r)
    if dups:
        bad.append(f"รหัสผ้าซ้ำ {len(dups)} แถว (ต้องมีรหัสละแถวเดียว): " + ", ".join(dups[:5]))
    if bad:
        return {"ok": False, "bad": bad, "error": f"ข้อมูลไม่ถูกต้อง {len(bad)} จุด — ไม่บันทึก · "
                + " · ".join(bad[:5]) + (f" (และอีก {len(bad) - 5} จุด)" if len(bad) > 5 else "")}
    if not keep:
        return {"ok": False, "error": "ไม่มีแถวข้อมูล (ต้องมีรหัสผ้าอย่างน้อย 1 แถว)"}
    return {"ok": True, "columns": cols, "rows": keep, "count": len(keep), "warn": []}


def _parse_upload(content: bytes, sheet: str, checker) -> dict:
    """อ่านไฟล์ .xlsx ที่อัปโหลด — ใช้ชีทชื่อตรงกันถ้ามี ไม่งั้นชีทแรก → ผลตรวจ (ยังไม่บันทึก)"""
    import io
    from openpyxl import load_workbook
    try:
        wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception:
        raise ValueError("เปิดไฟล์ไม่ได้ — รองรับเฉพาะ .xlsx")
    ws = wb[sheet] if sheet in wb.sheetnames else wb.worksheets[0]
    data = [["" if v is None else v for v in r] for r in ws.iter_rows(values_only=True)]
    wb.close()
    if not data:
        raise ValueError("ไฟล์ว่าง")
    res = checker(data[0], data[1:])
    res["sheet"] = ws.title
    return res


def parse_items_file(content: bytes) -> dict:
    return _parse_upload(content, SH_ITEM, check_items)


def parse_only_file(content: bytes) -> dict:
    return _parse_upload(content, SH_ONLY, check_only)


# ---- ชีท MC S9 (เครื่องของผู้รับจ้าง): หัวคอลัมน์จริง + ชื่อไทยบนเว็บที่ยอมรับตอนนำเข้า
MC_HDR = {
    "Supp": ("SUPP", "ผู้รับจ้าง"),
    "Mc": ("MC", "เครื่อง"),
    "New CAT": ("NEW CAT", "CAT", "MC_CAT", "กลุ่มเครื่อง"),
    "New Gauge": ("NEW GAUGE", "GAUGE", "GUAGE", "เกจ"),
    "Total Mc": ("TOTAL MC", "จำนวนเครื่อง"),
    "Material": ("MATERIAL", "ชนิดผ้า"),
    "Status": ("STATUS", "สถานะ"),
    "Working day": ("WORKING DAY", "วันทำงาน", "วัน/สัปดาห์"),
    "Remark": ("REMARK", "หมายเหตุ"),
}
MC_NEED = ("Supp", "Mc", "New CAT", "New Gauge", "Total Mc")


def check_mc(columns, rows) -> dict:
    """ตรวจข้อมูลชุดที่จะวางทับชีท MC S9 — กติกาเดียวกับการเพิ่มเครื่องบนเว็บ
    ยกเว้นวันทำงานว่าง = เตือน (ชีทจริงมีหลายเจ้าที่ยังไม่ได้กรอก ไม่งั้น Export แล้ว Import กลับไม่ได้)"""
    alias = {_key(a): std for std, names in MC_HDR.items() for a in names}
    cols = [alias.get(_key(c), _s(c)) for c in columns]
    miss = [n for n in MC_NEED if n not in cols]
    if miss:
        return {"ok": False, "error": "ไม่พบคอลัมน์ " + ", ".join(miss)
                + " — แถวแรกต้องเป็นหัวคอลัมน์: Supp · Mc · New CAT · New Gauge (เลข Guage) · Total Mc (+ Working day · Status · Material · Remark)"}
    at = {n: cols.index(n) for n in MC_HDR if n in cols}
    keep, bad, no_wd = [], [], 0
    for n, r in enumerate(rows, start=2):          # เลขแถวตาม Excel (แถว 1 = หัวคอลัมน์)
        r = (list(r) + [""] * (len(cols) - len(r)))[:len(cols)]
        if not any(_s(v) for v in r):
            continue                               # แถวว่างทั้งแถว ข้าม
        tag = f"แถว {n} {_s(r[at['Supp']])}".rstrip()
        lack = [f for f in ("Supp", "Mc", "New CAT", "New Gauge") if not _s(r[at[f]])]
        if _num(r[at["Total Mc"]]) is None:
            lack.append("Total Mc (ตัวเลข ใส่ 0 ได้)")
        if lack:
            bad.append(f"{tag}: ขาด {', '.join(lack)}")
            continue
        # ไฟล์ต้นทางเขียน 28G34" ได้ — เก็บเลขเกจอย่างเดียว ; กระบอกขัดกับ CAT = ไม่บันทึก
        g, gerr = _mc_gauge(r[at["New Gauge"]], r[at["New CAT"]])
        if gerr:
            bad.append(f"{tag}: {gerr}")
            continue
        r[at["New Gauge"]] = g
        r[at["Total Mc"]] = _num(r[at["Total Mc"]])
        if "Working day" in at:
            v = _s(r[at["Working day"]])
            if not v:
                no_wd += 1
            elif _num(v) is None or not 0 <= float(_num(v)) <= 7:
                bad.append(f"{tag}: Working day ต้องอยู่ระหว่าง 0-7")
                continue
            else:
                r[at["Working day"]] = _num(v)
        else:
            no_wd += 1
        keep.append(r)
    if bad:
        return {"ok": False, "bad": bad, "error": f"ข้อมูลไม่ถูกต้อง {len(bad)} แถว — ไม่บันทึก · "
                + " · ".join(bad[:5]) + (f" (และอีก {len(bad) - 5} แถว)" if len(bad) > 5 else "")}
    if not keep:
        return {"ok": False, "error": "ไม่มีแถวข้อมูล (ต้องมีเครื่องอย่างน้อย 1 แถว)"}
    warn = [f"{no_wd} แถวไม่ได้กรอก Working day"] if no_wd else []
    # คอลัมน์เสริมที่ไฟล์ไม่มี = ค่าเดิมทั้งคอลัมน์หาย (เช่น Status → เครื่องที่ตั้ง Inactive กลับมานับ)
    lost = [n for n in MC_HDR if n not in MC_NEED and n not in cols]
    if lost:
        warn.append(f"ไฟล์ไม่มีคอลัมน์ {', '.join(lost)} — ค่าเดิมในคอลัมน์นี้จะหายทั้งหมด")
    return {"ok": True, "columns": cols, "rows": keep, "count": len(keep), "warn": warn}


def parse_mc_file(content: bytes) -> dict:
    return _parse_upload(content, SH_MC, check_mc)


# =============================================================================
def get() -> dict:
    """เครื่องจ้างทอทั้งหมด (จัดตามผู้รับจ้าง) + สรุปจำนวนผ้าทั้ง 2 ทะเบียน"""
    m = masters.read_sheet(MC_FILE, SH_MC)
    si, mi, ci, gi, ti, mati, sti, wdi, ri = _cols(
        m["columns"], "Supp", "Mc", "New CAT", "New Gauge", "Total Mc",
        "Material", "Status", "Working day", "Remark")

    supps = {}
    for idx, r in enumerate(m["rows"]):
        supp = _s(_get(r, si))
        mc = _s(_get(r, mi))
        if not (supp or mc):
            continue
        status = _s(_get(r, sti))
        row = {
            "row": idx, "supp": supp, "mc": mc,
            # โชว์เลขเกจอย่างเดียว (แถวเก่ารูป 28G34" ก็โชว์ 28) — กระบอกดูที่ mc_cat
            "mc_cat": _s(_get(r, ci)), "guage": _s(_mc_gauge(_get(r, gi))[0]),
            "total": _num(_get(r, ti)), "material": _s(_get(r, mati)),
            "status": status, "work_days": _num(_get(r, wdi)), "remark": _s(_get(r, ri)),
            # Planning ข้ามแถวที่ Status = Inactive (ไม่นับเป็นกำลังผลิต)
            "active": _key(status) != "INACTIVE",
        }
        supps.setdefault(supp or "(ไม่ระบุผู้รับจ้าง)", []).append(row)

    out = []
    for name in sorted(supps):
        rows = sorted(supps[name], key=lambda x: (x["mc"].upper(), _gsort(x["guage"])))
        out.append({
            "supp": name, "machines": rows,
            "total": sum(x["total"] or 0 for x in rows if x["active"]),
            "total_all": sum(x["total"] or 0 for x in rows),
            "off": sum(1 for x in rows if not x["active"]),
        })

    # ---- ผ้าที่จ้างทอได้ (มาจากระบบ แถวเยอะ) / ผ้าที่ต้องจ้างเสมอ (กรอกมือ)
    it = masters.read_sheet(MC_FILE, SH_ITEM)
    ii, gi2, mgi, capi, cati = _cols(
        it["columns"], "ITEM_CODE", "MC_GAUGE", "MACHINE_GROUP", "CAP", "CAT")
    items = [{
        "row": i, "item": _s(_get(r, ii)), "guage": _s(_get(r, gi2)),
        "mc_group": _s(_get(r, mgi)), "cap": _num(_get(r, capi)), "mc_cat": _s(_get(r, cati)),
    } for i, r in enumerate(it["rows"]) if _s(_get(r, ii))]

    # S9 Only หน้าตาเดียวกับ Item S9 (user 24 ก.ย. 2569) — MC / CAT / CAP เป็นคอลัมน์ที่เพิ่มทีหลัง
    #   ชีทเก่ายังไม่มี → ส่งค่าว่าง ; Planning.py:2403 ยังอ่านแค่ ITEM_CODE
    on = masters.read_sheet(MC_FILE, SH_ONLY)
    oi, ogi, omgi, ocati, ocapi = _cols(
        on["columns"], "ITEM_CODE", "MC_GAUGE", "MACHINE_GROUP", "CAT", "CAP")
    only = [{
        "row": i, "item": _s(_get(r, oi)), "guage": _s(_get(r, ogi)),
        "mc_group": _s(_get(r, omgi)), "mc_cat": _s(_get(r, ocati)), "cap": _num(_get(r, ocapi)),
    } for i, r in enumerate(on["rows"]) if _s(_get(r, oi))]

    return {
        "suppliers": out,
        "total_mc": sum(s["total"] for s in out),
        "items": items,
        "only": only,
        "columns": {"mc": m["columns"], "item": it["columns"], "only": on["columns"]},
        # item: ลบ/แก้อ้างเลขแถว → ต้องมีเลขรุ่นจริง กันเลขแถวเลื่อนหลังมีคนนำเข้าทับ/แก้แซง
        #   S9 Only ก็แก้อ้างเลขแถวแล้ว → ใช้เลขรุ่นจริงเหมือนกัน (read_sheet ไม่มีคีย์ rev)
        "revs": {"mc": livesync.rev(livesync.masters_res(MC_FILE, SH_MC)),
                 "only": livesync.rev(livesync.masters_res(MC_FILE, SH_ONLY)),
                 "item": livesync.rev(livesync.masters_res(MC_FILE, SH_ITEM))},
        "options": {
            "supps": sorted({x["supp"] for s in out for x in s["machines"] if x["supp"]}),
            "mcs": sorted({x["mc"] for s in out for x in s["machines"] if x["mc"]}),
            "cats": sorted({x["mc_cat"] for s in out for x in s["machines"] if x["mc_cat"]}),
            # เลขเกจล้วน (ไม่มีกระบอก) — รวมเกจที่ผ้าจ้างทอต้องใช้ด้วย เครื่องเจ้าใหม่มักเป็นเกจที่ยังไม่มีใครรับ
            "gauges": sorted({g for g in (
                [x["guage"] for s in out for x in s["machines"]]
                + [_key(x["guage"]) for x in items + only]) if g.isdigit()}, key=_gsort),
            "materials": ["POLY", "COTTON", "BLEND"],
            "statuses": ["Active", "Inactive"],
            # ช่องเลือกของฟอร์มเพิ่มผ้าที่จ้างทอได้
            "item_groups": sorted({x["mc_group"] for x in items if x["mc_group"]}),
            "item_cats": sorted({x["mc_cat"] for x in items if x["mc_cat"]}),
        },
    }


# =============================================================================
def save(changes: dict, revs: dict = None, user: str = "") -> dict:
    """บันทึกการแก้จากหน้ารวม

    changes = {
      "machines": [{"row": i, "total": 6, "status": "Inactive", "work_days": 6, ...}],
      "add_machines": [{...}],            # เครื่องใหม่ของผู้รับจ้าง
      "del_machines": [i, ...],           # แถวที่ลบ (index ในชีท)
      "add_only": [{"item", "mc_group", "mc_cat", "guage", "cap"}],
      "only": [{"row": i, "item": ..., "cap": ...}],   # แก้ผ้าที่ต้องจ้างเสมอทีละช่อง
      "del_only": [i, ...],
      "items": [{"row": i, "cap": 240}],  # แก้ Cap/Day ของผ้าที่จ้างทอได้
      "add_items": [{"item", "mc_group", "mc_cat", "guage", "cap"}],
      "del_items": [i, ...],
      "replace_items": {"columns": [...], "rows": [[...]]},   # นำเข้าทับทั้งชีท Item S9
      "replace_only": {"columns": [...], "rows": [[...]]},    # นำเข้าทับทั้งชีท S9 Only
    }
    ค่าที่เว้นว่างในช่องบังคับ (จำนวนเครื่อง/วันทำงาน) ถูกปฏิเสธที่นี่ — กติกาเดียวกับหน้าเครื่องจักร
    """
    revs = revs or {}
    done = []

    # ---- ด่านกรอกให้ครบ (กติกาเดียวกับหน้าเครื่องจักร — user สั่ง 24 ก.ย. 2569)
    bad = []
    for ch in (changes.get("machines") or []) + (changes.get("add_machines") or []):
        tag = _s(ch.get("supp")) or f"แถวที่ {ch.get('row')}"
        if "total" in ch and _num(ch.get("total")) is None:
            bad.append(f"{tag}: ต้องกรอกจำนวนเครื่อง (ใส่ 0 ได้)")
        if "work_days" in ch:
            v = _num(ch.get("work_days"))
            if v is None:
                bad.append(f"{tag}: ต้องกรอกวันทำงาน")
            elif not 0 <= float(v) <= 7:
                bad.append(f"{tag}: วันทำงานต้องอยู่ระหว่าง 0-7")
    for ch in (changes.get("add_machines") or []):
        miss = [lbl for f, lbl in (("supp", "ผู้รับจ้าง"), ("mc_cat", "กลุ่มเครื่อง (MC_CAT)"),
                                   ("mc", "ชื่อเครื่อง (MC)"), ("guage", "Guage"))
                if not _s(ch.get(f))]
        if miss:
            bad.append("เครื่องใหม่: ยังไม่ได้กรอก " + ", ".join(miss))
        gerr = _mc_gauge(ch.get("guage"), ch.get("mc_cat"))[1]
        if gerr:
            bad.append("เครื่องใหม่: " + gerr)
    for ch in (changes.get("machines") or []):
        if "guage" in ch:
            gerr = _mc_gauge(ch.get("guage"), ch.get("mc_cat"))[1]
            if gerr:
                bad.append(f"แถวที่ {ch.get('row')}: {gerr}")
    # ผ้าที่ต้องจ้างเสมอ: Item + CAT + Guage + Cap บังคับ ; MC เว้นว่างได้
    #   S9 Only จ้างทอเท่านั้น — Planning ใช้ Cap/Guage/CAT ของชีทนี้อย่างเดียว ขาด = ไม่ทำแผน (user 24 ก.ย. 2569)
    #   แถวเดิมที่ยังกรอกไม่ครบค้างได้ (แก้ช่องอื่นได้) แต่ถ้าแตะช่องบังคับต้องกรอกให้ถูก
    _only_need = (("item", "Item"), ("mc_cat", "CAT"), ("guage", "Guage"))
    for ch in (changes.get("add_only") or []):
        miss = [lbl for f, lbl in _only_need if not _s(ch.get(f))]
        if miss:
            bad.append("ผ้าที่ต้องจ้างเสมอ: ยังไม่ได้กรอก " + ", ".join(miss))
        if _pos(ch.get("cap")) is None:
            bad.append("ผ้าที่ต้องจ้างเสมอ: Cap/Day ต้องมากกว่า 0")
    for ch in (changes.get("only") or []):
        miss = [lbl for f, lbl in _only_need if f in ch and not _s(ch.get(f))]
        if miss:
            bad.append(f"ผ้าที่ต้องจ้างเสมอ แถวที่ {ch.get('row')}: เว้นว่างไม่ได้ — " + ", ".join(miss))
        if "cap" in ch and _pos(ch.get("cap")) is None:
            bad.append(f"ผ้าที่ต้องจ้างเสมอ แถวที่ {ch.get('row')}: Cap/Day ต้องมากกว่า 0")
    if (changes.get("add_only") or changes.get("only")) and not bad:
        # รหัสผ้าซ้ำ — Planning ใช้ S9 Only เป็นเซ็ตของรหัสผ้า แถวซ้ำไม่มีผลอะไรเพิ่ม
        on = masters.read_sheet(MC_FILE, SH_ONLY)
        oi = _cols(on["columns"], "ITEM_CODE")[0]
        odel = set(changes.get("del_only") or [])
        okeys = {i: _key(_get(r, oi)) for i, r in enumerate(on["rows"])
                 if i not in odel and _s(_get(r, oi))}
        omoved = []
        for ch in (changes.get("only") or []):
            i = ch.get("row")
            if isinstance(i, int) and i in okeys and "item" in ch and _key(ch["item"]) != okeys[i]:
                okeys[i] = _key(ch["item"])
                omoved.append(i)
        ocnt = {}
        for k in okeys.values():
            ocnt[k] = ocnt.get(k, 0) + 1
        for i in omoved:
            if ocnt[okeys[i]] > 1:
                bad.append(f"ผ้าที่ต้องจ้างเสมอ ซ้ำกับแถวอื่น: {okeys[i]}")
        ohave = set(okeys.values())
        for ch in (changes.get("add_only") or []):
            k = _key(ch.get("item"))
            if k in ohave:
                bad.append(f"ผ้าที่ต้องจ้างเสมอ มีอยู่แล้ว: {k}")
            ohave.add(k)
    for ch in (changes.get("items") or []):
        # แก้ได้ทุกคอลัมน์ — ตรวจเฉพาะช่องที่ส่งมา (ช่องที่ไม่ได้แก้ไม่ส่ง)
        miss = [lbl for f, lbl in ITEM_FIELDS if f != "cap" and f in ch and not _s(ch.get(f))]
        if miss:
            bad.append(f"ผ้าที่จ้างทอได้ แถวที่ {ch.get('row')}: เว้นว่างไม่ได้ — " + ", ".join(miss))
        if "cap" in ch and _pos(ch.get("cap")) is None:
            bad.append(f"ผ้าที่จ้างทอได้ แถวที่ {ch.get('row')}: Cap/Day ต้องมากกว่า 0")
    for ch in (changes.get("add_items") or []):
        miss = [lbl for f, lbl in (("item", "Item"), ("mc_group", "MC"),
                                   ("mc_cat", "CAT"), ("guage", "Guage"))
                if not _s(ch.get(f))]
        if miss:
            bad.append("ผ้าที่จ้างทอได้: ยังไม่ได้กรอก " + ", ".join(miss))
        if _pos(ch.get("cap")) is None:
            bad.append("ผ้าที่จ้างทอได้: Cap/Day ต้องมากกว่า 0")
    if (changes.get("add_items") or changes.get("items")) and not bad:
        # ซ้ำกับของเดิม (รหัสผ้า + Guage) — เช็คก่อนเขียนอะไรลงไฟล์ทั้งนั้น
        #   คีย์ซ้ำ = Planning ใช้แถวแรก แถวที่เพิ่ม/แก้จะไม่มีผลกับแผนแบบเงียบ ๆ
        #   แถวที่แก้: เตือนเฉพาะเมื่อคีย์เปลี่ยน (ของเดิมที่ซ้ำอยู่แล้วยังแก้ Cap/Day ได้)
        it = masters.read_sheet(MC_FILE, SH_ITEM)
        ii, gi = _cols(it["columns"], "ITEM_CODE", "MC_GAUGE")
        idel = set(changes.get("del_items") or [])
        keys = {i: (_key(_get(r, ii)), _key(_get(r, gi)))
                for i, r in enumerate(it["rows"]) if i not in idel and _s(_get(r, ii))}
        moved = []
        for ch in (changes.get("items") or []):
            i = ch.get("row")
            if not isinstance(i, int) or i not in keys:
                continue
            old = keys[i]
            new = (_key(ch["item"]) if "item" in ch else old[0],
                   _key(ch["guage"]) if "guage" in ch else old[1])
            keys[i] = new
            if new != old:
                moved.append(i)
        cnt = {}
        for k in keys.values():
            cnt[k] = cnt.get(k, 0) + 1
        for i in moved:
            if cnt[keys[i]] > 1:
                bad.append(f"ซ้ำกับแถวอื่น: {keys[i][0]} Guage {keys[i][1]}")
        have = set(keys.values())
        for ch in (changes.get("add_items") or []):
            k = (_key(ch.get("item")), _key(ch.get("guage")))
            if k in have:
                bad.append(f"มีอยู่แล้ว: {k[0]} Guage {k[1]}")
            have.add(k)
        bad = list(dict.fromkeys(bad))
    if bad:
        return {"ok": False, "error": " · ".join(bad[:5])
                + (f" (และอีก {len(bad) - 5} รายการ)" if len(bad) > 5 else "")}

    # ---- เครื่องของผู้รับจ้าง
    mrows = changes.get("machines") or []
    adds = changes.get("add_machines") or []
    dels = set(changes.get("del_machines") or [])
    if mrows or adds or dels:
        m = masters.read_sheet(MC_FILE, SH_MC)
        idx = dict(zip(
            ("supp", "mc", "mc_cat", "guage", "total", "material", "status", "work_days", "remark"),
            _cols(m["columns"], "Supp", "Mc", "New CAT", "New Gauge", "Total Mc",
                  "Material", "Status", "Working day", "Remark")))
        rows = [list(r) for r in m["rows"]]
        n_edit = 0
        for ch in mrows:
            i = ch.get("row")
            if not isinstance(i, int) or not 0 <= i < len(rows):
                continue
            for f, ci in idx.items():
                if ci >= 0 and f in ch:
                    v = ch[f]
                    rows[i][ci] = (_num(v) if f in ("total", "work_days")
                                   else _mc_gauge(v)[0] if f == "guage" else _s(v))
            n_edit += 1
        for ch in adds:
            new = [""] * len(m["columns"])
            for f, ci in idx.items():
                if ci >= 0 and ch.get(f) not in (None, ""):
                    new[ci] = (_num(ch[f]) if f in ("total", "work_days")
                               else _mc_gauge(ch[f])[0] if f == "guage" else _s(ch[f]))
            rows.append(new)
        if dels:
            rows = [r for i, r in enumerate(rows) if i not in dels]
        masters.write_sheet(MC_FILE, SH_MC, m["columns"], rows,
                            user=user, base_rev=revs.get("mc") or m.get("rev"))
        if n_edit:
            done.append(f"แก้เครื่องจ้างทอ {n_edit} รายการ")
        if adds:
            done.append(f"เพิ่มเครื่องจ้างทอ {len(adds)} รายการ")
        if dels:
            done.append(f"ลบเครื่องจ้างทอ {len(dels)} รายการ")

    # ---- ผ้าที่ต้องจ้างทอเสมอ
    add_only = changes.get("add_only") or []
    ed_only = changes.get("only") or []
    del_only = set(changes.get("del_only") or [])
    if add_only or ed_only or del_only:
        on = masters.read_sheet(MC_FILE, SH_ONLY)
        cols = list(on["columns"])
        rows = [list(r) for r in on["rows"]]
        # ชีทเดิมมีแค่ ITEM_CODE / MC_GAUGE → เติมหัวคอลัมน์ให้ครบชุดเดียวกับ Item S9
        for need in ("MACHINE_GROUP", "CAT", "CAP"):
            if _cols(cols, need)[0] < 0:
                cols.append(need)
                rows = [r + [""] for r in rows]
        oi, mgi, cati, ogi, capi = _cols(cols, "ITEM_CODE", "MACHINE_GROUP", "CAT", "MC_GAUGE", "CAP")
        at = {"item": oi, "mc_group": mgi, "mc_cat": cati, "guage": ogi, "cap": capi}

        def _put(row, f, v):
            ci = at[f]
            if ci < 0:
                return
            if f == "item":
                row[ci] = _key(v)
            elif f == "guage":                 # เก็บเป็นตัวเลขถ้าเป็นเลข (C0 คงเป็นข้อความ)
                g = _key(v)
                row[ci] = _num(g) if _num(g) is not None else g
            elif f == "cap":
                row[ci] = _pos(v)
            else:
                row[ci] = _s(v).upper()

        n_edit = 0
        for ch in ed_only:
            i = ch.get("row")
            if not (isinstance(i, int) and 0 <= i < len(rows)):
                continue
            for f in at:
                if f in ch:
                    _put(rows[i], f, ch[f])
            n_edit += 1
        for ch in add_only:
            new = [""] * len(cols)
            for f in at:
                if _s(ch.get(f)):
                    _put(new, f, ch[f])
            rows.append(new)
        if del_only:
            rows = [r for i, r in enumerate(rows) if i not in del_only]
        rows = [r for r in rows if any(_s(v) for v in r)]     # แถวว่างที่ Excel จำไว้
        masters.write_sheet(MC_FILE, SH_ONLY, cols, rows,
                            user=user, base_rev=revs.get("only"))
        if n_edit:
            done.append(f"แก้ผ้าที่ต้องจ้างทอเสมอ {n_edit} รายการ")
        if add_only:
            done.append(f"เพิ่มผ้าที่ต้องจ้างทอเสมอ {len(add_only)} รายการ")
        if del_only:
            done.append(f"ลบผ้าที่ต้องจ้างทอเสมอ {len(del_only)} รายการ")

    # ---- เครื่องของผู้รับจ้าง: Import ทับทั้งชีท MC S9 (ทำอย่างเดียว ไม่ปนกับการแก้ทีละแถว)
    rep = changes.get("replace_mc")
    if rep:
        chk = check_mc(rep.get("columns") or [], rep.get("rows") or [])
        if not chk["ok"]:
            return chk
        m = masters.read_sheet(MC_FILE, SH_MC)
        si = _cols(m["columns"], "Supp")[0]
        old = sum(1 for r in m["rows"] if _s(_get(r, si)))     # ไม่นับแถวว่างที่ Excel จำไว้
        masters.write_sheet(MC_FILE, SH_MC, chk["columns"], chk["rows"],
                            user=user, base_rev=revs.get("mc"))
        done.append(f"Import เครื่องของผู้รับจ้างทับทั้งชีท {chk['count']} รายการ (เดิม {old})")
        return {"ok": True, "done": done}

    # ---- ผ้าที่ต้องจ้างเสมอ: นำเข้าทับทั้งชีท (ทำอย่างเดียว ไม่ปนกับการแก้ทีละแถว)
    rep = changes.get("replace_only")
    if rep:
        chk = check_only(rep.get("columns") or [], rep.get("rows") or [])
        if not chk["ok"]:
            return chk
        on = masters.read_sheet(MC_FILE, SH_ONLY)
        oi = _cols(on["columns"], "ITEM_CODE")[0]
        old = sum(1 for r in on["rows"] if _s(_get(r, oi)))     # ไม่นับแถวว่างที่ Excel จำไว้
        masters.write_sheet(MC_FILE, SH_ONLY, chk["columns"], chk["rows"],
                            user=user, base_rev=revs.get("only"))
        done.append(f"นำเข้าผ้าที่ต้องจ้างทอเสมอทับทั้งชีท {chk['count']} รายการ (เดิม {old})")
        return {"ok": True, "done": done}

    # ---- ผ้าที่จ้างทอได้: นำเข้าทับทั้งชีท (ทำอย่างเดียว ไม่ปนกับการแก้ทีละแถว)
    rep = changes.get("replace_items")
    if rep:
        chk = check_items(rep.get("columns") or [], rep.get("rows") or [])
        if not chk["ok"]:
            return chk
        it = masters.read_sheet(MC_FILE, SH_ITEM)
        ii = _cols(it["columns"], "ITEM_CODE")[0]
        old = sum(1 for r in it["rows"] if _s(_get(r, ii)))     # ไม่นับแถวว่างที่ Excel จำไว้
        masters.write_sheet(MC_FILE, SH_ITEM, chk["columns"], chk["rows"],
                            user=user, base_rev=revs.get("item"))
        done.append(f"นำเข้าผ้าที่จ้างทอได้ทับทั้งชีท {chk['count']} รายการ (เดิม {old})")
        return {"ok": True, "done": done}

    # ---- ผ้าที่จ้างทอได้: แก้ Cap/Day · เพิ่ม · ลบ ทีละแถว
    ied = changes.get("items") or []
    iadd = changes.get("add_items") or []
    idel = set(changes.get("del_items") or [])
    if ied or iadd or idel:
        it = masters.read_sheet(MC_FILE, SH_ITEM)
        cols = list(it["columns"])
        rows = [list(r) for r in it["rows"]]
        # ชีทเก่าอาจยังไม่มีคอลัมน์ CAP / CAT → เติมหัวคอลัมน์ให้ (Planning อ่าน CAP ตามชื่อ)
        for need in ("MACHINE_GROUP", "CAP", "CAT"):
            if (iadd or ied or need == "CAP") and _cols(cols, need)[0] < 0:
                cols.append(need)
                rows = [r + [""] for r in rows]
        ii, mgi, gi, capi, cati = _cols(cols, "ITEM_CODE", "MACHINE_GROUP", "MC_GAUGE", "CAP", "CAT")
        at = {"item": ii, "mc_group": mgi, "mc_cat": cati, "guage": gi, "cap": capi}
        n_edit = 0
        for ch in ied:
            i = ch.get("row")
            if not (isinstance(i, int) and 0 <= i < len(rows)):
                continue
            for f, ci in at.items():
                if f not in ch or ci < 0:
                    continue
                v = ch[f]
                if f == "item":
                    rows[i][ci] = _key(v)
                elif f == "guage":                 # เก็บเป็นตัวเลขถ้าเป็นเลข (C0 คงเป็นข้อความ)
                    g = _key(v)
                    rows[i][ci] = _num(g) if _num(g) is not None else g
                elif f == "cap":
                    rows[i][ci] = _pos(v)
                else:
                    rows[i][ci] = _s(v).upper()
            n_edit += 1
        for ch in iadd:                   # ซ้ำเช็คไปแล้วที่ด่านบน
            item, g = _key(ch.get("item")), _key(ch.get("guage"))
            new = [""] * len(cols)
            new[ii], new[gi], new[capi] = item, _num(g) if _num(g) is not None else g, _pos(ch.get("cap"))
            new[mgi], new[cati] = _s(ch.get("mc_group")).upper(), _s(ch.get("mc_cat")).upper()
            rows.append(new)
        if idel:
            rows = [r for i, r in enumerate(rows) if i not in idel]
        # ชีทมีแถวว่างที่ Excel จำไว้หลายพันแถว — ตัดทิ้ง ไม่งั้นแถวที่เพิ่มไปต่อท้ายแถวว่าง
        rows = [r for r in rows if any(_s(v) for v in r)]
        # รุ่น 0 ก็ต้องเช็ค (ห้ามใช้ `or` — 0 จะกลายเป็นข้ามด่าน)
        masters.write_sheet(MC_FILE, SH_ITEM, cols, rows,
                            user=user, base_rev=revs.get("item"))
        if n_edit:
            done.append(f"แก้ผ้าที่จ้างทอได้ {n_edit} รายการ")
        if iadd:
            done.append(f"เพิ่มผ้าที่จ้างทอได้ {len(iadd)} รายการ")
        if idel:
            done.append(f"ลบผ้าที่จ้างทอได้ {len(idel)} รายการ")

    return {"ok": True, "done": done}

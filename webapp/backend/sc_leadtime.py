"""
sc_leadtime.py — Dashboard › SC Lead Time : ใบสั่งเปิดมาแล้วเหลือเวลาถึงกำหนดส่งกี่สัปดาห์

คำถามที่หน้านี้ตอบ: "วันที่เปิด SC กับ RDD ห่างกันกี่สัปดาห์" — ใบที่เปิดมากระชั้น
คือใบที่แผนต้องเบียดเครื่อง/ตั้งเครื่องด่วน ส่วนใบที่เปิดล่วงหน้ายาว ๆ คือใบที่จัดคิวได้สบาย

  ห่าง (สัปดาห์) = RDD − สัปดาห์ที่เปิดใบ        ← ตัวเลขหลักของทั้งหน้า
  RDD            = FGWEEK ของใบนั้น **ตรง ๆ ไม่หักอะไร** (ตรงกับ RDD_WEEK ที่ Planning เก็บ
                   = FG Week) — user ยืนยัน 2 ก.ย. 2569 ว่าให้เทียบสัปดาห์เปิดกับ FG week ตรง ๆ
  สัปดาห์ที่เปิด = สัปดาห์ของ DATE_IN ตามปฏิทินโรงงาน (ศุกร์–พฤหัส: +3 วันก่อนหา ISO week)
  ค่าติดลบ       = เปิดใบมาหลังกำหนดส่งไปแล้ว (สายตั้งแต่วันเปิด)

  gap_knit       = ระยะถึง "เป้าทอ" (FG หักเวลางานหลังทอ 3–4 สัปดาห์ ด้วยกติกาเดียวกับ
                   Planning ; LAB-DIP หัก 1 สัปดาห์ ดู _LABDIP_TARGET_OFFSET)
                   — ส่งไปโชว์เป็นคอลัมน์เสริมเท่านั้น ไม่เข้า KPI/กราฟ

**แหล่งข้อมูล = ทะเบียน SC สะสม (data_plan/sc_registry.xlsx)** ที่ View_SC เขียนต่อทุกครั้งที่รัน
ไม่ใช่ไฟล์ view_sc.xlsx ใบเดียว เพราะ view เก็บเฉพาะใบที่ "ยังค้างวางแผน" — ใบที่ confirm แล้ว
หลุดออกจาก view ทันที และ snapshot ที่ archive ไว้ก็ถูกลบเมื่อเกิน 14 วัน (View_SC.ARCHIVE_KEEP_DAYS)
ถ้าอ่านจากไฟล์เหล่านั้นตรง ๆ หน้านี้จะเป็น "หน้าต่างกลิ้ง 14 วัน" ไม่ใช่ประวัติที่สะสมขึ้นเรื่อย ๆ

ยังไม่มีทะเบียน (ครั้งแรก / เพิ่ง deploy) → backfill ให้เองจาก snapshot ที่ยังเหลือใน data_plan
โดยเรียง "เก่าไปใหม่" แล้วส่งเวลาไฟล์เป็น seen_at ทีละใบ เพื่อให้ DATE_IN_FIRST/FIRST_SEEN
ตรงกับความจริงย้อนหลัง ไม่ใช่วันที่กด backfill (กติกาการรวมอยู่ที่ View_SC.update_registry
ที่เดียว — ที่นี่แค่ป้อนไฟล์ให้ตามลำดับ)

ขอบเขต: ตัดใบ **FQC** และ **ผ้าเติม** (ORDER_TYPE — ดู _EXCLUDE_TYPES) กับใบ **Sample /
Salesman / Salesman-PO** (FOB_TYPE — ดู _EXCLUDE_FOB) ออกทั้งหมด — ไม่ใช่ออร์เดอร์ปกติที่เปิด
มาพร้อมกำหนดส่ง จึงไม่ใช่ lead time ที่หน้านี้ต้องการวัด

⚠️ ข้อจำกัดที่ต้องบอกผู้ใช้บนหน้าเว็บ (ไม่ใช่ซ่อนไว้):
  1. ครอบคลุมเฉพาะใบที่เคยผ่าน view_sc ตั้งแต่วันที่เริ่มเก็บทะเบียน — งาน booking เก่าที่
     confirm ไปก่อนหน้านั้นไม่มีวันเปิดให้ดู (ฝั่ง booking ไม่มีคอลัมน์วันเปิดใบเลย
     มีแต่ PO_IN_DATE = วันด้ายเข้า ซึ่งคนละ event ห้ามใช้แทน)
  2. DATE_IN ของ view_sc ยังไม่ได้รับการยืนยันว่าเป็น "วันเปิดใบ" ต้นทางจริง — ที่ตรวจพบคือ
     เลขที่ใบไม่ไล่ตามวัน, ใบ FQC กระจุกวันเดียวกันทั้งกลุ่ม, และหลายใบมีค่าเป๊ะวินาทีเดียวกัน
     (= คีย์เข้าเป็นชุด) จึงมีลักษณะเป็น "วันที่รายการเข้าคิวรอวางแผน" มากกว่า
"""
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import config
import plan_view
# เทียบสัปดาห์เป็นคู่ (ปี, สัปดาห์) — ใบคร่อมปีได้ (เปิด W50/2026 ส่ง W3/2027)
from cylinder_advisor import _week_delta

_REGISTRY = config.OUTPUT_DIR / "sc_registry.xlsx"
_REG_SHEET = "SC_REGISTRY"
_SNAPSHOT_GLOB = "view_sc_*.xlsx"
# ไฟล์ view ปัจจุบัน (ยังไม่ถูก archive ตอนที่หน้าเว็บเปิด) — ใช้เติมใบล่าสุดตอน backfill
_LIVE_VIEW = config.REPO_DIR / "Order" / "view_sc.xlsx"

# ประเภทออร์เดอร์ที่ไม่เอาเข้าหน้านี้เลย (user สั่ง 2 ก.ย. 2569: "ไม่ดู FQC", "เอาผ้าเติมออก")
#   FQC     = งานทดแทน/แก้ (FOB ส่วนใหญ่เป็น Replacement SO) ใบถูกสร้างทีหลังตอนงานมีปัญหาแล้ว
#             ทุกใบจึงมีวันเปิดหลังเดดไลน์ทอเสมอ (วัดจริง 42/42 ใบ ค่ากลาง −4 สัปดาห์)
#   ผ้าเติม  = งานเติมของ ไม่ใช่ออร์เดอร์ลูกค้าที่เปิดมาพร้อมกำหนดส่งตามปกติ
# ทั้งคู่ถ่วงค่ากลางจนไม่สะท้อน lead time ของงานปกติ — ตัดที่ backend เพื่อให้ทุกตัวเลขบนหน้า
# (KPI/กราฟ/ตาราง) มาจากชุดข้อมูลเดียวกันแน่นอน ; ค่าเทียบเป็นตัวพิมพ์ใหญ่ (ภาษาไทยไม่เปลี่ยนรูป)
_EXCLUDE_TYPES = {"FQC", "ผ้าเติม"}

# ประเภท FOB ที่ไม่เอาเข้าหน้านี้เลย (user สั่ง 9 ก.ย. 2569: "ไม่เอา Sample / Salesman-PO / Salesman")
# เป็นงานตัวอย่างและงานที่เซลส์เปิดเอง ไม่ใช่ออร์เดอร์ลูกค้าที่เปิดมาพร้อมกำหนดส่งตามรอบปกติ —
# สังเกตได้จาก FOB_TARGET_OFFSET_WEEKS (Planning.py) ให้กลุ่มนี้ offset = 1 สัปดาห์ ต่างจากงาน
# ผลิตจริงที่ 3–4 สัปดาห์ คือมันไม่มีงานหลังทอเต็มรูปแบบ จึงคนละสเกลกับ lead time ที่หน้านี้วัด
# คนละคอลัมน์กับ _EXCLUDE_TYPES (นั่นคือ ORDER_TYPE) จึงต้องเป็นด่านแยก ; เทียบเป็นตัวพิมพ์ใหญ่
_EXCLUDE_FOB = {"SAMPLE", "SALESMAN", "SALESMAN-PO"}

# LAB-DIP = ตัวอย่างย้อมทดสอบ ทอเสร็จแล้วส่งต่อได้เลย ไม่มีงานหลังทอ 3–4 สัปดาห์แบบงานผลิตจริง
# → เป้าทอ = RDD − 1 สัปดาห์ (user สั่ง 7 ก.ย. 2569) ไม่ใช่ offset ตามเกจ/ด้ายเหมือนงานทั่วไป
# หมายเหตุ: เป็นกติกา "ของหน้านี้" เท่านั้น — Planning ตัวจริงจัดคิว LAB-DIP ด้วยพื้น TODAY+2
# (Planning._target_knit_of) ซึ่งเป็นคนละคำถาม (ทอได้เร็วสุดเมื่อไร ≠ ต้องทอเสร็จเมื่อไร)
_LABDIP_TARGET_OFFSET = 1

# เพดานแถวที่ส่งขึ้นหน้าเว็บ — หน้าเว็บคิด KPI/กราฟใหม่จากแถวที่กรองเอง จึงต้องส่งให้ครบ
# (ทะเบียนโตปีละ ~20k ใบ ; เกินเพดานจะตัดใบเก่าสุดออกก่อนและบอกจำนวนที่ตัดไว้ใน truncated)
_MAX_ROWS = 8000


def _s(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "nat", "none") else s


def _sc_key(v) -> str:
    """SC_NO ในทะเบียน → ข้อความสำหรับแสดง/ค้นหา (รูปเต็มตามระบบ: 'S720398', 'L1260000045')"""
    s = _s(v)
    return s[:-2] if s.endswith(".0") and s[:-2].isdigit() else s


def _view_sc_mod():
    """โมดูล View_SC (อยู่ที่ root ของ pipeline) — import แบบ lazy เหมือน dyed_items

    ใช้เพื่อให้กติกาการรวมทะเบียนอยู่ที่เดียว (update_registry) ทั้งตอนรัน pipeline และตอน
    backfill จากหน้าเว็บ ; import ไม่ได้ → None แล้วผู้เรียกจัดการเอง (หน้า Dashboard ต้องเปิดได้เสมอ)
    """
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        import View_SC
        return View_SC
    except Exception:
        return None


def _mtime(p: Path):
    try:
        return p.stat().st_mtime
    except OSError:
        return None


def _snapshots() -> list:
    """snapshot view_sc ที่ยังเหลือใน data_plan เรียงเก่า→ใหม่ (ตาม mtime)"""
    if not config.OUTPUT_DIR.exists():
        return []
    return sorted(config.OUTPUT_DIR.glob(_SNAPSHOT_GLOB), key=lambda p: p.stat().st_mtime)


def _pending_sources() -> list:
    """snapshot ที่ยังไม่ถูกรวมเข้าทะเบียน (เทียบชื่อไฟล์กับชีท SOURCES) — ไม่ต้องเปิดอ่านไฟล์"""
    mod = _view_sc_mod()
    if mod is None:
        return []
    try:
        srcs = mod.registry_sources()
    except Exception:
        return []
    return [p for p in _snapshots() if p.name not in srcs]


def backfill(force: bool = False) -> dict:
    """รวม snapshot ที่ยังไม่เคยเข้าทะเบียน (และไฟล์ view ปัจจุบัน) เข้าทะเบียนสะสม

    เรียกได้ทั้งตอนหน้าเว็บพบว่าทะเบียนยังตามหลัง และตอนผู้ใช้กดปุ่ม "อัปเดตทะเบียน"
    การรวมทั้งชุดเขียนไฟล์ครั้งเดียว (update_registry_many) — ถ้าเขียนทีละไฟล์ 30 รอบ
    หน้าเว็บจะค้างเป็นนาที ; ไฟล์ที่เคยรวมแล้วถูกข้ามโดยไม่ต้องเปิดอ่าน
    """
    mod = _view_sc_mod()
    if mod is None:
        return {"ok": False, "note": "โหลดโมดูล View_SC ไม่ได้ — backfill ทะเบียนไม่สำเร็จ"}
    try:
        import pandas as pd
    except Exception:
        return {"ok": False, "note": "ไม่มี pandas ในสภาพแวดล้อมนี้"}

    srcs = {} if force else mod.registry_sources()
    items, skipped = [], 0
    for p in _snapshots():
        if p.name in srcs:
            skipped += 1
            continue
        try:
            df = pd.read_excel(p)
        except Exception:
            continue
        seen = date.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d") + " 00:00:00"
        items.append((df, seen, p.name))
    # ไฟล์ view ปัจจุบัน — ใบที่เพิ่งดึงมาแต่ยังไม่ถูก archive (source ผูกกับ mtime ของไฟล์
    # เพื่อให้รอบถัดไปที่ไฟล์ถูกเขียนทับด้วยข้อมูลใหม่ยังรวมเข้าได้อีก)
    if _LIVE_VIEW.exists():
        mt = int(_mtime(_LIVE_VIEW) or 0)
        name = f"view_sc.xlsx@{mt}"
        if name in srcs:
            skipped += 1
        else:
            try:
                items.append((pd.read_excel(_LIVE_VIEW),
                              date.fromtimestamp(mt).strftime("%Y-%m-%d") + " 00:00:00", name))
            except Exception:
                pass
    if not items:
        return {"ok": True, "sources_before": len(srcs), "merged": [], "added": 0,
                "skipped": skipped}
    # เรียงเก่า→ใหม่แล้ว (จาก _snapshots + view ปัจจุบันท้ายสุด) → IN_VIEW ยึดตามไฟล์ล่าสุด
    r = mod.update_registry_many(items)
    return {"ok": True, "sources_before": len(srcs), "merged": r["merged"],
            "added": r["added"], "skipped": skipped + r.get("skipped", 0),
            "rows": r.get("rows")}


def _registry_legacy() -> bool:
    """ทะเบียนที่มีอยู่เป็นสคีมาเก่าไหม (ถามโมดูล View_SC ซึ่งเป็นเจ้าของนิยามคีย์)"""
    mod = _view_sc_mod()
    if mod is None or not hasattr(mod, "registry_is_legacy"):
        return False
    try:
        return bool(mod.registry_is_legacy())
    except Exception:
        return False


def _load_registry() -> tuple:
    """(rows, meta) ของทะเบียนสะสม — ไม่มีไฟล์ **หรือเป็นสคีมาเก่า** → backfill แล้วอ่านซ้ำ

    ต้องเช็คสคีมาด้วย ไม่ใช่แค่ "มีไฟล์ไหม" — ทะเบียนรุ่นแรกใช้คีย์ (SC, ผ้า) ซึ่งทับแถวของ
    ใบเดียวกันที่แบ่งส่งหลายงวด/หลาย SO จนยอดหาย ถ้าอ่านไฟล์เก่าต่อไปเงียบ ๆ หน้าเว็บจะโชว์
    ข้อมูลไม่ครบทั้งที่โค้ดใหม่ถูกแล้ว (เจอจริงหลัง deploy 2 ก.ย. 2569)
    """
    meta = {"file": _REGISTRY.name, "mtime": _mtime(_REGISTRY), "backfilled": None}
    # ทะเบียน "ตามหลัง" ได้ 3 ทาง: ยังไม่มีไฟล์ · สคีมาเก่า · มี snapshot ที่ยังไม่ถูกรวม
    # (ทางที่สามเกิดจริงเมื่อทะเบียนสคีมาเก่าถูกทิ้งตอนรัน View_SC แล้วสร้างใหม่จากไฟล์รอบนั้น
    #  ไฟล์เดียว — snapshot ที่เหลือจะไม่มีวันถูกดึงกลับถ้าไม่ตรวจตรงนี้)
    pending = _pending_sources()
    if not _REGISTRY.exists() or _registry_legacy() or pending:
        meta["migrated"] = _REGISTRY.exists() and _registry_legacy()
        meta["pending_before"] = len(pending)
        meta["backfilled"] = backfill()
        meta["mtime"] = _mtime(_REGISTRY)
    if not _REGISTRY.exists():
        return [], meta
    try:
        import pandas as pd
        df = pd.read_excel(_REGISTRY, sheet_name=_REG_SHEET)
    except Exception as e:
        meta["error"] = str(e)
        return [], meta
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df.to_dict("records"), meta


def _open_week(v) -> tuple:
    """วันเปิดใบ → ('YYYY-MM-DD', สัปดาห์, ปีของสัปดาห์นั้น)

    ใช้กติกาสัปดาห์เดียวกับทั้งระบบ: สัปดาห์เริ่มวันศุกร์ → บวก 3 วันก่อนหา ISO week
    (Calendar.py) ไม่งั้นใบที่เปิดวันศุกร์จะถูกนับเป็นสัปดาห์ก่อนหน้า แล้วห่างเพี้ยนไป 1
    """
    s = _s(v)
    if not s:
        return "", None, None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return "", None, None
    else:
        try:
            import pandas as pd
            ts = pd.to_datetime(s, errors="coerce")
            if ts is None or ts != ts:
                return "", None, None
            d = ts.date()
        except Exception:
            return "", None, None
    iso = (d + timedelta(days=3)).isocalendar()
    return d.strftime("%Y-%m-%d"), int(iso[1]), int(iso[0])


def _fg_pair(v) -> tuple:
    """FGWEEK (YYYYWW / เลขสัปดาห์เปล่า) → (สัปดาห์, ปี) ; ปีเป็น พ.ศ. ก็แปลงให้"""
    n = plan_view._num(v)
    if n is None or n != n:
        return None, None
    n = int(n)
    if n >= 100000:
        y, w = n // 100, n % 100
        if y > 2400:
            y -= 543
        return (w if 1 <= w <= 53 else None), y
    return (n if 1 <= n <= 53 else None), None


def summary() -> dict:
    """สรุประยะห่าง "วันเปิดใบ → RDD (FG week)" ของทุกใบในทะเบียนสะสม

    ส่งแถวดิบขึ้นหน้าเว็บทั้งหมด (ไม่แบ่งกลุ่มมาให้) เพราะผู้ใช้ปรับช่วงกลุ่มเองได้บนหน้า —
    ถ้าแบ่งมาจากฝั่ง server แล้วผู้ใช้เปลี่ยนช่วง ตัวเลขจะต้องวิ่งกลับมาถามใหม่ทุกครั้ง
    """
    recs, meta = _load_registry()
    out = {"source": meta, "rows": [], "no_target": 0, "no_date": 0,
           "excluded": 0, "excluded_types": sorted(_EXCLUDE_TYPES | _EXCLUDE_FOB),
           "total": 0, "truncated": 0}
    if not recs:
        out["note"] = ("ยังไม่มีทะเบียน SC สะสม — ทะเบียนถูกสร้างอัตโนมัติทุกครั้งที่รัน "
                       "View_SC (ขั้น 'ดึงข้อมูล DB') หรือกดปุ่มอัปเดตทะเบียนบนหน้านี้")
        return out

    rows = []
    no_target = no_date = excluded = 0
    for r in recs:
        item = _s(r.get("ITEM_CODE")).upper()
        sc = _sc_key(r.get("SC_NO"))
        if not item and not sc:
            continue
        otype = _s(r.get("ORDER_TYPE"))
        # ตัดก่อนทุกด่าน — ใบที่ถูกตัดไม่เข้าทั้งสถิติ ตัวหาร และกลุ่ม "ไม่มีวันเปิด/ไม่มีเป้า"
        if otype.upper() in _EXCLUDE_TYPES:
            excluded += 1
            continue
        # ด่านเดียวกันแต่ดูคอลัมน์ FOB_TYPE — งานตัวอย่าง/งานเซลส์ (ดู _EXCLUDE_FOB)
        if _s(r.get("FOB_TYPE")).upper() in _EXCLUDE_FOB:
            excluded += 1
            continue
        # วันเปิดใบ = ค่าที่เห็น "ครั้งแรก" (ถ้าไม่มี ค่อยตกไปใช้ค่าล่าสุด)
        odate, ow, oy = _open_week(r.get("DATE_IN_FIRST") or r.get("DATE_IN_LAST"))
        fw, fy = _fg_pair(r.get("FGWEEK"))
        if not odate:
            no_date += 1
            continue
        if not fw:
            no_target += 1
            continue
        # ★ ตัวเลขหลักของหน้านี้ = สัปดาห์เปิดใบ → RDD (FG week) ตรง ๆ ไม่หักอะไรทั้งสิ้น
        # (user ยืนยัน 2 ก.ย. 2569) — ตรงกับที่ Planning เก็บ RDD_WEEK = FG Week
        gap = _week_delta(fy, fw, oy, ow)

        # เป้าทอ = FG หักเวลางานหลังทอ (3–4 สัปดาห์ตาม FOB/เครื่อง/เส้นด้าย) ด้วยกติกาเดียวกับ
        # Planning — ไม่ใช่ตัวตัดสินของหน้านี้แล้ว แต่ยังส่งไปโชว์เป็นคอลัมน์เสริม
        # เพราะ "เวลาที่เหลือให้ทอจริง" สั้นกว่าระยะถึง RDD อยู่เท่านี้เสมอ
        if otype.strip().upper() == "LAB-DIP":
            off = _LABDIP_TARGET_OFFSET
        else:
            off = plan_view._target_offset_weeks(
                r.get("FOB_TYPE"), r.get("MC_CAT"), r.get("MC_GUAGE"),
                r.get("YARN_ITEM") or r.get("MATERIAL_CONTENT"))
        tw, ty = (None, None)
        if fy:
            tw, ty = plan_view._week_minus(fy, fw, off)
        else:
            # ไม่รู้ปีของ FG → ถอยแบบเลขสัปดาห์ล้วน (ยังดีกว่าไม่มีค่าให้ดูเลย)
            tw = fw - off if fw - off >= 1 else fw - off + 52
        gap_knit = _week_delta(ty, tw, oy, ow) if tw else None
        rows.append({
            "sc": sc, "item": item,
            "order_type": otype, "fob": _s(r.get("FOB_TYPE")),
            "customer": _s(r.get("CUSTOMER_NAME")), "sale": _s(r.get("SALE_NAME")),
            # ใบเดียวมีได้หลายบรรทัด/หลายงวดส่ง — ส่ง SO และเลขบรรทัดไปด้วยเพื่อให้แยกออกบนตาราง
            "so": _s(r.get("SO_NO")), "po": _s(r.get("PO_NO")),
            "line": _s(r.get("SC_LINE_ID")),
            "mc_group": _s(r.get("MC_GROUP")), "cat": _s(r.get("MC_CAT")),
            "gauge": _s(r.get("MC_GUAGE")), "color": _s(r.get("NAY_COLOR")) or _s(r.get("SUB_COLOR")),
            "open_date": odate, "open_week": ow, "open_year": oy,
            "fg": fw, "fg_year": fy, "target": tw, "target_year": ty, "offset": off,
            # gap = ถึง RDD (ตัวหลัก) · gap_knit = ถึงเป้าทอ (คอลัมน์เสริมในตาราง)
            "gap": gap, "gap_knit": gap_knit,
            "qty": round(plan_view._f(r.get("ORDERS_QTY")), 2),
            "pending": round(plan_view._f(r.get("PENDING_PLAN")), 2),
            # ใบยังอยู่ใน view = ยังรอวางแผน ; หลุดแล้ว = confirm/ปิดใบไปแล้ว
            "in_view": 1 if plan_view._f(r.get("IN_VIEW")) else 0,
            "first_seen": _s(r.get("FIRST_SEEN"))[:10],
            "last_seen": _s(r.get("LAST_SEEN"))[:10],
            "date_changed": 1 if (_s(r.get("DATE_IN_LAST"))[:10]
                                  and _s(r.get("DATE_IN_FIRST"))[:10]
                                  and _s(r.get("DATE_IN_LAST"))[:10] != _s(r.get("DATE_IN_FIRST"))[:10]) else 0,
        })

    # ใบที่เปิดล่าสุดอยู่บนสุด — ถ้าเกินเพดานจะตัดใบเก่าสุดทิ้ง (ใบใหม่มีค่าต่อการตัดสินใจกว่า)
    rows.sort(key=lambda r: r["open_date"], reverse=True)
    out["total"] = len(rows)                       # = จำนวน "รายการ" (ใบ × งวดส่ง × สี)
    out["sc_count"] = len({r["sc"] for r in rows if r["sc"]})
    out["truncated"] = max(0, len(rows) - _MAX_ROWS)
    out["rows"] = rows[:_MAX_ROWS]
    out["no_target"] = no_target
    out["no_date"] = no_date
    out["excluded"] = excluded
    out["excluded_types"] = sorted(_EXCLUDE_TYPES | _EXCLUDE_FOB)
    out["snapshots"] = len(_snapshots())
    return out

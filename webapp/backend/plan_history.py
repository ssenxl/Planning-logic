"""
plan_history.py — ทะเบียนประวัติแผน (บล็อกงานรายสัปดาห์) ที่สะสมข้ามการลบไฟล์ทุก 14 วัน

คำถามที่โมดูลนี้ตอบ: "สัปดาห์ที่ผ่านไปแล้วเราทออะไรไว้บ้าง" ย้อนได้ถึง 52 สัปดาห์
โดยผู้ใช้เลือกจุดเริ่ม (ปี + สัปดาห์) เองจากหน้าแผน แล้ว Gantt เอาไป overlay เป็นบล็อกอ่านอย่างเดียว

**แหล่งข้อมูลจริงคือ DB ไม่ใช่ไฟล์ที่เหลือบนดิสก์**
view booking บน Oracle เก็บ booking ย้อนหลังเป็นปี (วัดจริง 202601..202808
= 61 สัปดาห์) ที่ pipeline เห็นแค่ ~7 สัปดาห์ เพราะ View_Booking.py กรองเองตอนดึง ไม่ใช่
เพราะ DB ไม่มี → "ดูย้อนหลังลึก" จึงเป็นการ **ดึงใหม่จาก DB ตามช่วงที่ผู้ใช้สั่ง** (ช้า จึงทำ
เบื้องหลังและให้ผู้ใช้รอ) แล้วรัน AVA_MC บนข้อมูลนั้นเพื่อให้ได้เลขเครื่อง/setup/carry
ด้วยตรรกะเดียวกับแผนจริง — ไม่ใช่ค่าที่ประมาณเอาเอง

**ทะเบียนไฟล์นี้ = cache ถาวรของสิ่งที่ดึงมาแล้ว**
  1. ดึง 7 สัปดาห์ใช้เวลา ~16 นาที (view วิ่งข้าม dblink) → ดึงซ้ำช่วงเดิมทุกครั้งที่กดดูไม่ไหว
  2. scheduler ลบไฟล์ output ที่เกิน 14 วันทิ้ง (scheduler.py:19-21) → เก็บไว้ในไฟล์ผลลัพธ์
     ปกติไม่ได้ ต้องมีทะเบียนของตัวเองที่ไม่ตรง pattern การลบ
  3. เก็บของฟรีที่ยังมีอยู่ด้วย: snapshot booking_final_ready* ที่ยังไม่ถูกลบ ถูกกวาดเข้าทะเบียน
     โดยไม่ต้องยิง DB เลย (_sync)
  (กติกาเดียวกับ sc_leadtime.py ที่ต้องมี sc_registry.xlsx ด้วยเหตุผลข้อ 2 เป๊ะ ๆ)

**คีย์ทะเบียนต้องมี "ปี"** เพราะทั้ง pipeline ใช้เลขสัปดาห์เปล่า — W45 ปีนี้กับ W45 ปีก่อน
เป็นคนละสัปดาห์แต่เลขเดียวกัน ถ้าไม่เก็บปี ประวัติเกิน ~1 ปีจะทับกันเอง
ปีของ snapshot ถอดจาก mtime ของไฟล์เทียบเลขสัปดาห์ (_iso_year_for_week) — booking ไม่มี
คอลัมน์ปีของ WEEK ให้ใช้ (มีแต่ FG_YEAR ซึ่งเป็นปีของกำหนดส่ง ไม่ใช่ปีที่ทอ)

**กติกาทับคีย์**: snapshot ใหม่ชนะเสมอ → ค่าที่เก็บไว้คือ "ภาพสุดท้ายก่อนสัปดาห์นั้นผ่านไป"
= แผนที่ทำจริง ไม่ใช่ร่างแรกที่ยังขยับได้

**กันบล็อกซ้ำบน Gantt**: items_since() คืนเฉพาะสัปดาห์ที่ "เก่ากว่าสัปดาห์เก่าสุดของไฟล์
booking ปัจจุบัน" (live floor) เพราะช่วงตั้งแต่ floor ขึ้นมาหน้าเว็บมี bookingItems สดอยู่แล้ว
ถ้าส่งซ้ำจะได้บล็อกซ้อนกันสองใบในช่องเดียว
"""
import os
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

import config
import plan_view

_REGISTRY = config.OUTPUT_DIR / "plan_history.xlsx"
_SHEET = "PLAN_HISTORY"
_META_SHEET = "META"
_SNAPSHOT_GLOB = "booking_final_ready*.xlsx"

# เพดานความลึกที่ให้เลือกได้ (สัปดาห์) — ตรงกับที่หน้าเว็บโฆษณาไว้
MAX_WEEKS_BACK = 52

# อายุที่เก็บในทะเบียน — เผื่อจากเพดานที่ดูได้ 1 ปี (ดูได้ 52 แต่เก็บ 110 สัปดาห์)
# ถ้าไม่ตัด ไฟล์จะโตขึ้นทุกสัปดาห์ตลอดอายุระบบ แล้วทุกคำขอต้องอ่านทั้งใบ
KEEP_WEEKS = 110

# อัปเดตทะเบียนพร้อมกันหลายทางได้ (thread อุ่นตอน start + คำขอจากหน้าเว็บ) → ล็อกไว้ก่อนเขียน
# ไม่งั้นสองฝั่งเขียนไฟล์ชั่วคราวชื่อเดียวกันทับกัน แล้ว replace ไฟล์ที่เขียนไม่จบไปเป็นทะเบียนจริง
_LOCK = threading.Lock()

# คีย์ประจำบล็อก 1 ช่องบน Gantt (ต้องตรงกับที่ booking_items_by_week ยุบมา + ปี)
_KEY_COLS = ("year", "week", "cat", "gauge", "mc_group", "item")

# ฟิลด์ที่เป็น list ตอนส่งขึ้นเว็บ แต่ต้องแบนเป็นข้อความตอนลงชีท
_LIST_COLS = ("so_list",)
_LIST_SEP = ","


# ขอบเขตที่ booking 1 ก้อนกินได้ (วัน นับจากวันที่ไฟล์ถูกเขียน) — ใช้ตัดสินปีของเลขสัปดาห์
# **ไม่สมมาตร** เพราะ booking เก็บใบที่ยังเปิดอยู่: ย้อนหลังได้แค่ ~6 สัปดาห์ (วัดจริงสูงสุด 9)
# แต่วางล่วงหน้าได้ถึงปลายปี (~6 เดือน) ถ้าใช้ "ใกล้ ref ที่สุด" เฉย ๆ งานที่วางไว้ W52 ในไฟล์
# เดือน มิ.ย. (+182 วัน) จะแพ้ W52 ของปีก่อน (−178 วัน) ไปแค่ 4 วัน แล้วถูกบันทึกผิดปี
_PAST_LIMIT_DAYS = 120      # เผื่อจากของจริง ~3 เท่า แต่ยังตัดสัปดาห์เดียวกันของปีก่อนออกได้
_FUTURE_LIMIT_DAYS = 400


def _iso_year_for_week(week: int, ref: date) -> int:
    """ปีของ "สัปดาห์ที่ N" ในไฟล์ที่ถูกเขียนวันที่ ref

    เลือกปีที่ทำให้สัปดาห์นั้นตกในช่วงที่ booking ครอบคลุมได้จริง (ดู _PAST/_FUTURE_LIMIT)
    ถ้ามีหลายปีเข้าเกณฑ์จึงค่อยเอาตัวที่ใกล้ ref ที่สุด — ไฟล์เดือน ส.ค. 2026 ที่มี W2
    = W2 ของ 2027 (อีก 147 วัน) ไม่ใช่ของ 2026 (ผ่านไป 218 วัน = เกินอดีตที่เป็นไปได้)
    """
    best_y, best_d, fallback_y, fallback_d = None, None, ref.year, None
    for y in (ref.year - 1, ref.year, ref.year + 1):
        try:
            th = date.fromisocalendar(y, int(week), 4)   # วันพฤหัส = หมุดกลางสัปดาห์ ISO
        except ValueError:                                # ปีนั้นไม่มี W53
            continue
        d = (th - ref).days
        if fallback_d is None or abs(d) < fallback_d:
            fallback_y, fallback_d = y, abs(d)
        if not (-_PAST_LIMIT_DAYS <= d <= _FUTURE_LIMIT_DAYS):
            continue
        if best_d is None or abs(d) < best_d:
            best_y, best_d = y, abs(d)
    return best_y if best_y is not None else fallback_y


def current_year_week() -> tuple:
    """(ปี, สัปดาห์) ของวันนี้ตามปฏิทินโรงงาน ศุกร์–พฤหัส (+3 วันก่อนคิด ISO week)
    — นิยามเดียวกับ Calendar.py และ currentPlanWeek() ของ Gantt"""
    iso = (date.today() + timedelta(days=3)).isocalendar()
    return int(iso[0]), int(iso[1])


def _yw(v) -> tuple:
    """(ปี, สัปดาห์) ที่เทียบมากกว่า/น้อยกว่ากันได้ — ใช้เป็นคีย์เรียงทุกที่ในโมดูลนี้"""
    try:
        return int(v[0]), int(v[1])
    except (TypeError, ValueError, IndexError):
        return (0, 0)


def _snapshots() -> list:
    """ไฟล์ booking snapshot ที่ยังเหลือใน data_plan เรียง "เก่า → ใหม่"

    ต้องเรียงเก่าไปใหม่ เพราะกติกาทับคีย์คือ "ใหม่ชนะ" — ถ้าป้อนสลับลำดับ ค่าที่ค้างอยู่
    จะเป็นร่างเก่าของสัปดาห์นั้นแทนภาพสุดท้าย
    """
    d = config.OUTPUT_DIR
    if not d.exists():
        return []
    return sorted(d.glob(_SNAPSHOT_GLOB), key=lambda p: p.stat().st_mtime)


# ทะเบียนอ่านครั้งละ ~3 วินาที (xlsx หลายพันแถว) และหน้าเว็บยิง range + items ติดกัน
# → จำไว้ตาม mtime ของไฟล์ ; ทะเบียนถูกเขียนใหม่เมื่อไร mtime เปลี่ยน แคชก็ตกเอง
_CACHE = {"mtime": None, "blocks": None, "meta": None}


def _read_registry() -> tuple:
    """(บล็อกตามคีย์, ไฟล์ที่เคยประมวลผลแล้ว) — ยังไม่มีทะเบียน = ({}, {})"""
    if not _REGISTRY.exists():
        return {}, {}
    try:
        mt = _REGISTRY.stat().st_mtime
    except OSError:
        mt = None
    if mt is not None and _CACHE["mtime"] == mt and _CACHE["blocks"] is not None:
        return _CACHE["blocks"], _CACHE["meta"]
    try:
        import pandas as pd
        df = pd.read_excel(_REGISTRY, sheet_name=_SHEET)
    except Exception:
        return {}, {}
    blocks = {}
    for rec in df.to_dict("records"):
        row = {k: ("" if _isna(v) else _py(v)) for k, v in rec.items()}
        for c in _LIST_COLS:
            row[c] = [s for s in str(row.get(c, "")).split(_LIST_SEP) if s]
        try:
            key = tuple(_norm_key(row.get(c)) for c in _KEY_COLS)
        except Exception:
            continue
        blocks[key] = row
    meta = {}
    try:
        import pandas as pd
        mdf = pd.read_excel(_REGISTRY, sheet_name=_META_SHEET)
        for rec in mdf.to_dict("records"):
            name = str(rec.get("FILE", "")).strip()
            if name:
                meta[name] = rec
    except Exception:
        pass
    _CACHE.update(mtime=mt, blocks=blocks, meta=meta)
    return blocks, meta


def _py(v):
    """numpy scalar (int64/float64/bool_) → ชนิดของ python

    pandas คืน numpy scalar เมื่อคอลัมน์นั้นเป็นตัวเลขล้วน ซึ่ง json ของ FastAPI แปลงไม่ได้
    → endpoint จะพังทั้งก้อนเฉพาะบางชุดข้อมูล (ตอนไม่มีเซลล์ว่างเลย) หาเหตุยากมาก"""
    it = getattr(v, "item", None)
    return it() if callable(it) and hasattr(v, "dtype") else v


def _isna(v) -> bool:
    try:
        import pandas as pd
        return bool(pd.isna(v))
    except Exception:
        return v is None


def _norm_key(v) -> str:
    """คีย์ต้องเทียบได้ข้ามรอบอ่าน — 22.0/'22 ' → '22', ตัวพิมพ์เล็ก/ใหญ่ปนกัน → ใหญ่"""
    s = "" if v is None else str(v).strip()
    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        s = s[:-2]
    return s.upper()


def _write_registry(blocks: dict, meta: dict) -> None:
    """เขียนทะเบียนแบบ temp-แล้ว-replace — กันไฟล์พังถ้าโดนขัดกลางคัน (scheduler รันคู่กันได้)

    ตัดบล็อกที่เก่ากว่า KEEP_WEEKS ทิ้งไปพร้อมกัน — ทะเบียนจึงไม่โตขึ้นเรื่อย ๆ ตลอดอายุระบบ
    (ตัดตอนเขียน ไม่ใช่ตอนอ่าน เพื่อให้ของที่ถูกตัดหายจากไฟล์จริง ไม่ใช่แค่ถูกซ่อน)
    """
    import pandas as pd
    cutoff = _minus_weeks(current_year_week(), KEEP_WEEKS)
    rows = []
    for key, row in list(blocks.items()):
        yw = (_i(row.get("year")), _i(row.get("week")))
        if yw[0] > 0 and cutoff[0] > 0 and yw < cutoff:
            blocks.pop(key, None)
            continue
        r = dict(row)
        for c in _LIST_COLS:
            v = r.get(c)
            r[c] = _LIST_SEP.join(str(x) for x in v) if isinstance(v, list) else str(v or "")
        rows.append(r)
    rows.sort(key=lambda r: (_i(r.get("year")), _i(r.get("week")), str(r.get("item", ""))))
    # ชื่อไฟล์ชั่วคราวผูกกับ process — คนละ process (uvicorn หลาย worker) จะได้ไม่เขียนทับกัน
    tmp = _REGISTRY.with_suffix(f".tmp{os.getpid()}.xlsx")
    try:
        with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
            pd.DataFrame(rows).to_excel(xw, sheet_name=_SHEET, index=False)
            pd.DataFrame(list(meta.values())).to_excel(xw, sheet_name=_META_SHEET, index=False)
        tmp.replace(_REGISTRY)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _i(v) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _ingest(path: Path, blocks: dict, kind: str = "snapshot", edge_week: tuple = None) -> dict:
    """ยัดผลลัพธ์ 1 ไฟล์ลงทะเบียน → META ของไฟล์นั้น (ชื่อ, mtime, ชนิด, สัปดาห์เก่าสุด)

    kind="snapshot" = ไฟล์ booking_final_ready* ของ pipeline (บอกได้ว่า booking สดเริ่มสัปดาห์ไหน)
    kind="history"  = ไฟล์ที่โหมดดูย้อนหลังสร้างเอง — **ห้ามนับเป็น live floor** เพราะสัปดาห์
    เก่าสุดของมันคือช่วงอดีตที่เพิ่งดึงมา ถ้านับด้วย floor จะร่วงไปอยู่ในอดีตแล้วตัดข้อมูล
    ที่เพิ่งดึงมาทิ้งทั้งหมด (เจอจริงตอนทดสอบ: ดึง W19 สำเร็จ แต่ items_since คืน 0 บล็อก)

    ใช้ plan_view.booking_items_by_week ตัวเดียวกับที่ /api/plan/booking-items ใช้ →
    บล็อกประวัติมีฟิลด์ครบเท่าบล็อกสด การ์ดรายละเอียดบน Gantt จึงอ่านได้เหมือนกัน

    **edge = สัปดาห์ขอบล่างของไฟล์นี้** — AVA_MC คิดลูกโซ่ carry/setup จากลำดับสัปดาห์
    "ภายในไฟล์ที่ได้รับ" สัปดาห์แรกสุดจึงไม่มีสัปดาห์ก่อนหน้าให้ยกเครื่องอุ่นมา แล้วทุกเครื่อง
    ถูกนับเป็น "ตั้งใหม่" ทั้งหมด (วัดจริง: W2/2026 setup 368 · carry 0 ทั้งที่ W3 ขึ้นไป
    carry อยู่ 240–280) → ติดธงไว้ให้หน้าเว็บไม่เอา job ของสัปดาห์นั้นไปนับในแถบโหลด
    edge_week = สัปดาห์ที่รู้ล่วงหน้าว่าเป็นขอบ (โหมดดึงประวัติสั่ง View_Booking ให้เผื่อ
    สัปดาห์ warm-up มา ขอบจริงจึงเป็นสัปดาห์ warm-up ไม่ใช่สัปดาห์ที่ผู้ใช้ขอ) ; None = ใช้
    สัปดาห์เก่าสุดที่พบในไฟล์
    """
    ref = datetime.fromtimestamp(path.stat().st_mtime).date()
    items = plan_view.booking_items_by_week(path=path)
    seen = datetime.now().isoformat(timespec="seconds")
    staged, lo = [], None
    for b in items:
        wk = _i(b.get("week"))
        if wk <= 0 or wk == 99:          # W99 = sentinel "งานล้น" ไม่ใช่สัปดาห์จริง
            continue
        # ปีที่ AVA_MC ติดมากับชีท DETAIL (จากปฏิทิน) แม่นกว่าเดาจาก mtime — ใช้ก่อนเสมอ
        y = _i(b.get("year")) or _iso_year_for_week(wk, ref)
        row = dict(b)
        row["year"], row["week"], row["seen_at"], row["src"] = y, wk, seen, path.name
        staged.append(((y, wk), row))
        if lo is None or (y, wk) < lo:
            lo = (y, wk)
    edge = _yw(edge_week) if edge_week else lo
    for yw, row in staged:
        row["edge"] = 1 if (edge and yw == edge) else 0
        blocks[tuple(_norm_key(row.get(c)) for c in _KEY_COLS)] = row
    return {"FILE": path.name, "MTIME": path.stat().st_mtime, "KIND": kind,
            "MIN_YEAR": (lo or (0, 0))[0], "MIN_WEEK": (lo or (0, 0))[1],
            "EDGE_YEAR": (edge or (0, 0))[0], "EDGE_WEEK": (edge or (0, 0))[1],
            "ROWS": len(items)}


def _sync() -> tuple:
    """อัปเดตทะเบียนจาก snapshot ที่ยังไม่เคยอ่าน (ครั้งแรก = backfill ทั้งโฟลเดอร์)
    → (บล็อกตามคีย์, META) ที่สดแล้ว — ผู้เรียกใช้ต่อได้เลยโดยไม่ต้องอ่านไฟล์ซ้ำ

    เทียบด้วย (ชื่อไฟล์, mtime) → ไฟล์เดิมที่อ่านไปแล้วไม่ถูกอ่านซ้ำ เปิดหน้าเว็บรัว ๆ จึงไม่แพง
    ทั้งก้อนอยู่ใต้ _LOCK — คำขอที่มาพร้อมกัน (หรือ thread อุ่นตอน start) จะรอคิว แล้วได้
    ทะเบียนที่อีกฝั่งเพิ่งอ่านเสร็จ ไม่ใช่ต่างคนต่างอ่านไฟล์เดียวกันซ้ำสองรอบ
    """
    with _LOCK:
        blocks, meta = _read_registry()
        added = []
        for p in _snapshots():
            try:
                mt = p.stat().st_mtime
            except OSError:
                continue
            old = meta.get(p.name)
            if old is not None and abs(float(old.get("MTIME") or 0) - mt) < 1:
                continue
            if not plan_view._booking_ok(p):     # ไฟล์ที่ AVA_MC เขียนไม่จบ (ไม่มี SUMMARY_MC_REMAIN)
                continue
            try:
                meta[p.name] = _ingest(p, blocks)
                added.append(p.name)
            except Exception:
                continue                          # ไฟล์เสีย 1 ก้อนต้องไม่ทำให้ทะเบียนทั้งใบพัง
        if added:
            _write_registry(blocks, meta)
        return blocks, meta


def update() -> dict:
    """สั่งอัปเดตทะเบียนอย่างเดียว (ให้ endpoint สถานะ/สคริปต์เรียก) → สรุปสั้น ๆ"""
    blocks, meta = _sync()
    floor = _floor(meta)
    return {"rows": len(blocks), "files": len(meta),
            "live": {"year": floor[0], "week": floor[1]}}


def _live_floor(meta: dict) -> tuple:
    """(ปี, สัปดาห์) เก่าสุดที่ไฟล์ booking ปัจจุบันครอบคลุม = เพดานบนของประวัติที่จะส่ง

    เอาจาก META ของ snapshot ที่ mtime ใหม่สุด (ตัวเดียวกับที่ _latest_booking_path เลือก
    เพราะข้ามไฟล์เสียด้วยกฎเดียวกัน) ; ไม่มีข้อมูล = สัปดาห์ปัจจุบัน
    """
    newest, best = None, -1.0
    for m in meta.values():
        # เฉพาะ snapshot ของ pipeline — ไฟล์ที่โหมดย้อนหลังสร้างเองไม่ใช่ "booking ปัจจุบัน"
        # ทะเบียนที่เขียนก่อนมีคอลัมน์ KIND จะอ่านกลับมาเป็น NaN (ไม่ใช่ค่าว่าง!) ซึ่ง str() แล้ว
        # ได้ 'nan' → ถ้าเทียบตรง ๆ จะไม่เข้าเงื่อนไข fallback แล้วข้าม snapshot ทิ้งหมด
        # ผลคือ live floor ร่วงไปเป็นสัปดาห์ปัจจุบัน แล้วประวัติทับบล็อก booking สดบน Gantt
        kv = m.get("KIND")
        kind = "" if _isna(kv) else str(kv).strip().lower()
        if kind in ("", "nan", "none"):
            kind = "history" if str(m.get("FILE", "")).startswith("history_") else "snapshot"
        if kind != "snapshot":
            continue
        mt = float(m.get("MTIME") or 0)
        if mt > best:
            newest, best = m, mt
    if newest and _i(newest.get("MIN_YEAR")) > 0:
        return _i(newest.get("MIN_YEAR")), _i(newest.get("MIN_WEEK"))
    return current_year_week()


def _floor(meta: dict) -> tuple:
    """เพดานบนที่ใช้จริง — กันกรณี live floor ล้ำไปข้างหน้าจนบังประวัติที่มีอยู่
    (ถ้าไฟล์ booking ล่าสุดบังเอิญไม่มีสัปดาห์อดีตเลย ประวัติก็ยังต้องโชว์ได้ถึงสัปดาห์ปัจจุบัน)"""
    return min(_live_floor(meta), current_year_week())


def range_info() -> dict:
    """ช่วงที่เลือกได้จริงให้ dropdown ของหน้าเว็บ (ไม่ให้ user เลือกสัปดาห์ที่ไม่มีข้อมูล)

    {"weeks": [{year, week, blocks}, ...  ใหม่→เก่า], "years": [...],
     "oldest": {year, week}, "live": {year, week}, "max_back": 52, "rows": n}
    """
    blocks, meta = _sync()
    floor = _floor(meta)
    cur = current_year_week()
    limit = _minus_weeks(cur, MAX_WEEKS_BACK)
    n = {}
    for row in blocks.values():
        yw = (_i(row.get("year")), _i(row.get("week")))
        if yw[0] <= 0 or not (limit <= yw < floor):
            continue
        n[yw] = n.get(yw, 0) + 1
    weeks = [{"year": y, "week": w, "blocks": c} for (y, w), c in sorted(n.items(), reverse=True)]
    # สัปดาห์ที่ "เลือกได้" = ที่ DB มี (ยังไม่ได้ดึงก็เลือกได้ แล้วค่อยกดดึง) ตัดที่เพดาน 52
    # ready = ดึงมาแล้ว โชว์ได้ทันที ; ไม่ ready = ต้องรอดึง (หน้าเว็บบอกเวลาโดยประมาณ)
    have = {(w["year"], w["week"]) for w in weeks}
    db = db_weeks()
    sel = []
    for x in db.get("weeks", []):
        yw = (x["year"], x["week"])
        if limit <= yw < floor:
            sel.append({"year": yw[0], "week": yw[1], "rows": x["rows"], "ready": yw in have})
    sel.sort(key=lambda x: (x["year"], x["week"]), reverse=True)
    return {
        "weeks": weeks,
        "years": sorted({d["year"] for d in weeks}, reverse=True),
        "oldest": weeks[-1] if weeks else None,
        "live": {"year": floor[0], "week": floor[1]},
        "current": {"year": cur[0], "week": cur[1]},
        "max_back": MAX_WEEKS_BACK,
        "rows": len(blocks),
        # ── ช่วงที่ดึงจาก DB ได้ (แหล่งจริง) ──
        "db": {"weeks": sel, "error": db.get("error", ""),
               "oldest": sel[-1] if sel else None,
               "years": sorted({x["year"] for x in sel}, reverse=True)},
    }


def _minus_weeks(yw: tuple, n: int) -> tuple:
    """ถอย n สัปดาห์จาก (ปี, สัปดาห์) แบบ ISO — ข้ามปี 52/53 สัปดาห์ได้ถูกต้อง"""
    try:
        d = date.fromisocalendar(int(yw[0]), int(yw[1]), 4) - timedelta(weeks=int(n))
        iso = d.isocalendar()
        return int(iso[0]), int(iso[1])
    except (ValueError, TypeError, OverflowError):
        return (0, 0)


def items_since(year, week) -> dict:
    """บล็อกประวัติตั้งแต่ (ปี, สัปดาห์) ที่เลือก ถึงก่อนช่วงที่ booking สดครอบคลุม

    {"items": [...], "from": {year, week}, "live": {...}, "weeks": จำนวนสัปดาห์ที่ได้จริง}
    แต่ละบล็อกมี "year" ติดมาด้วย → หน้าเว็บแยกคอลัมน์ W45 ปีนี้ กับ W45 ปีก่อนออกจากกันได้
    """
    blocks, meta = _sync()
    floor = _floor(meta)
    cur = current_year_week()
    start = _yw((year, week))
    hard = _minus_weeks(cur, MAX_WEEKS_BACK)
    if start < hard:                      # เลือกลึกเกินเพดาน → ตรึงไว้ที่ 52 สัปดาห์
        start = hard
    out, wks = [], set()
    for row in blocks.values():
        yw = (_i(row.get("year")), _i(row.get("week")))
        if yw[0] <= 0 or not (start <= yw < floor):
            continue
        b = {k: v for k, v in row.items() if k not in ("seen_at", "src")}
        b["year"], b["week"] = yw[0], yw[1]
        out.append(b)
        wks.add(yw)
    out.sort(key=lambda b: (b["year"], b["week"], str(b.get("item", ""))))
    return {"items": out, "from": {"year": start[0], "week": start[1]},
            "live": {"year": floor[0], "week": floor[1]}, "weeks": len(wks)}


# ══════════════════════════════════════════════════════════════════════════════
# ดึงประวัติใหม่จาก DB ตามที่ผู้ใช้สั่ง (ช้า → ทำเบื้องหลัง)
#
# 4 ขั้น:  หาสัปดาห์ที่ขาด → View_Booking (DB) → AVA_MC (คิดเครื่อง) → ลงทะเบียน
# ทั้งสองสคริปต์รันเป็น subprocess พร้อม env ที่ชี้ไฟล์เข้า/ออกไปโฟลเดอร์ชั่วคราว
# — AVA_MC.py เขียนโค้ดไว้ระดับ top-level (อ่านไฟล์ตั้งแต่ import) จึง import มาเรียกตรง ๆ ไม่ได้
# และ "ต้องไม่" ให้ไฟล์ผลลัพธ์ไปตกใน data_plan ด้วยชื่อ booking_final_ready* เพราะหน้าแผน
# หยิบไฟล์ล่าสุดที่ชื่อขึ้นต้นแบบนั้นมาเป็น booking ปัจจุบัน (plan_view._latest_booking_path)
# ══════════════════════════════════════════════════════════════════════════════

# โฟลเดอร์ทำงานของโหมดนี้ — อยู่ใต้ output (โวลุ่มของ server) แต่ชื่อไม่ตรง pattern ที่ scheduler ลบ
_WORK_DIR = config.OUTPUT_DIR / "history_fetch"

# สถานะงานล่าสุด (มีได้ทีละงาน) — หน้าเว็บ poll ผ่าน /api/plan/history-status
_JOB = {"running": False, "step": "", "detail": "", "from": None, "to": None,
        "weeks": 0, "started": None, "finished": None, "error": "", "added": 0,
        "forced": False, "log": ""}
_JOB_LOCK = threading.Lock()

# เวลาที่ใช้จริงต่อสัปดาห์ (วินาที) — วัดได้ ~16 นาที/7 สัปดาห์ ; ใช้บอก "ประมาณกี่นาที"
# บนหน้าเว็บเท่านั้น ไม่มีผลต่อการทำงาน
_SEC_PER_WEEK = 140

# สัปดาห์ที่ดึงเผื่อไว้ก่อนช่วงที่ผู้ใช้ขอ เพื่อให้ AVA_MC มีของเก่าไว้ต่อลูกโซ่ carry/setup
# 2 สัปดาห์พอ — กติกา carry ของ AVA_MC มองย้อนไม่เกิน 3 สัปดาห์ และยิ่งเผื่อยิ่งเสียเวลาดึง
_WARMUP_WEEKS = 2

# สัปดาห์ที่ DB มี — query เบา (นับที่ฝั่ง remote) แต่ก็ไม่ควรยิงทุกครั้งที่เปิดหน้า
_DB_CACHE = {"at": None, "weeks": None, "error": ""}
_DB_CACHE_SEC = 600


def _yw_int(yw: tuple) -> int:
    return int(yw[0]) * 100 + int(yw[1])


def _booking_mod():
    """โมดูล View_Booking (อยู่ที่ root ของ pipeline) — import แบบ lazy

    ใช้ค่าคอนฟิก/การต่อ DB ชุดเดียวกับ pipeline (host, credential, ชื่อ view) จะได้ไม่มี
    "การต่อ DB อีกทาง" ที่หลุดจากของจริงเวลามีการแก้ ; import ไม่ได้ → None แล้วผู้เรียกจัดการ"""
    try:
        import sys
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        import View_Booking
        return View_Booking
    except Exception:
        return None


def db_weeks(force: bool = False) -> dict:
    """สัปดาห์ที่ booking view มีให้ดึง → {"weeks": [{year, week, rows}], "error": ""}

    นับด้วย group by ที่ฝั่ง DB (ไม่ดึงแถวจริง) จึงเร็ว — ต่างจากการดึงข้อมูลจริงที่วิ่ง
    ข้าม dblink แล้วใช้เวลาหลักสิบนาที ; แคชไว้ 10 นาทีเพราะหน้าเว็บเรียกทุกครั้งที่เปิด"""
    now = datetime.now()
    if (not force and _DB_CACHE["weeks"] is not None and _DB_CACHE["at"]
            and (now - _DB_CACHE["at"]).total_seconds() < _DB_CACHE_SEC):
        return {"weeks": _DB_CACHE["weeks"], "error": _DB_CACHE["error"]}
    vb = _booking_mod()
    if vb is None:
        return {"weeks": [], "error": "import View_Booking ไม่ได้ (ไม่มีไฟล์/ไม่มี oracledb)"}
    try:
        con = vb._connect()
        try:
            cur = con.cursor()
            cur.execute(f"select YEAR, WEEK, count(*) from {vb.VIEW_NAME} "
                        f"where YEAR is not null and WEEK is not null group by YEAR, WEEK")
            weeks = [{"year": _i(y), "week": _i(w), "rows": _i(c)} for y, w, c in cur]
            cur.close()
        finally:
            con.close()
        weeks = [x for x in weeks if x["year"] > 0 and 0 < x["week"] <= 53]
        weeks.sort(key=lambda x: (x["year"], x["week"]), reverse=True)
        _DB_CACHE.update(at=now, weeks=weeks, error="")
        return {"weeks": weeks, "error": ""}
    except Exception as e:
        _DB_CACHE.update(at=now, weeks=[], error=str(e))
        return {"weeks": [], "error": str(e)}


def _weeks_range(start: tuple, stop: tuple) -> list:
    """ทุกสัปดาห์ตามปฏิทินในช่วง [start, stop) — ไล่เองทีละสัปดาห์ ไม่พึ่งรายการสัปดาห์จาก DB

    ใช้ตอนผู้ใช้สั่ง "ดึงใหม่": ช่วงที่จะยิง View_Booking ต้องเป็นช่วงที่ผู้ใช้เลือกจริง ๆ
    ไม่ใช่ช่วงที่ query นับสัปดาห์ (db_weeks) บังเอิญตอบมา — query นั้นล้มได้ และถ้าล้มแล้ว
    เงียบ ๆ จะกลายเป็น "ไม่มีอะไรต้องดึง" ทั้งที่ยังไม่เคยแตะ DB เลย
    """
    out = []
    cur = _yw(start)
    stop = _yw(stop)
    if cur[0] <= 0 or stop[0] <= 0:
        return out
    # กันลูปไม่รู้จบถ้าคู่ (ปี, สัปดาห์) ผิดรูปจนเทียบกันไม่ได้ — เพดานเท่าที่หน้าเว็บให้เลือก
    for _ in range(MAX_WEEKS_BACK + 8):
        if not (cur < stop):
            break
        out.append(cur)
        nxt = _minus_weeks(cur, -1)
        if nxt[0] <= 0 or nxt <= cur:
            break
        cur = nxt
    return out


def missing_weeks(year, week, force: bool = False) -> list:
    """สัปดาห์ในช่วง [ที่เลือก, live floor) ที่ต้องไปดึงจาก DB

    ปกติ = เฉพาะสัปดาห์ที่ทะเบียนยังไม่มี เพราะการดึงแพงมาก (~16 นาที/7 สัปดาห์)
    กดดูช่วงเดิมซ้ำจึงไม่ต้องรอใหม่

    force=True (ผู้ใช้กด "ดึงใหม่จาก DB") = ทั้งช่วงที่เลือกตามปฏิทิน ไม่สนว่าทะเบียนมีแล้ว
    หรือ db_weeks ตอบสัปดาห์ไหน — ของที่ค้างในทะเบียนอาจมาจาก snapshot เก่าที่ยังไม่มีคอลัมน์
    ลูกโซ่ setup/carry (บล็อกขึ้นเป็น 📋 ไม่มีเลข job) การดึงใหม่ทับจึงเป็นวิธีเดียวที่ได้ของครบ
    """
    blocks, meta = _sync()
    floor = _floor(meta)
    start = _yw((year, week))
    hard = _minus_weeks(current_year_week(), MAX_WEEKS_BACK)
    if start < hard:
        start = hard
    if force:
        return _weeks_range(start, floor)
    have = {(_i(r.get("year")), _i(r.get("week"))) for r in blocks.values()}
    want = []
    for x in db_weeks().get("weeks", []):
        yw = (x["year"], x["week"])
        if start <= yw < floor and yw not in have:
            want.append(yw)
    want.sort()
    return want


def _tail_line(p: Path, nbytes: int = 4096) -> str:
    """บรรทัดที่มีเนื้อความล่าสุดของไฟล์ log

    ขั้น "ดึง booking จาก DB" กินเวลาหลักสิบนาทีโดยข้อความสถานะไม่ขยับเลย ซึ่งแยกไม่ออกจาก
    "ค้าง" — View_Booking พิมพ์ "[INFO] Fetched so far: n rows..." ทุก 10,000 แถวอยู่แล้ว
    จึงหยิบบรรทัดท้าย log มาเป็น heartbeat ให้หน้าเว็บเห็นตัวเลขเดินขึ้นทุกรอบ poll
    (อ่านแค่ท้ายไฟล์ ไม่เปิดทั้งใบ — log โตได้หลายเมกะไบต์)
    """
    try:
        with open(p, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - nbytes))
            data = fh.read().decode("utf-8", "replace")
    except OSError:
        return ""
    for line in reversed(data.splitlines()):
        s = line.strip()
        if s:
            return s[:200]
    return ""


def fetch_status() -> dict:
    """สถานะงานดึง + เวลาที่ผ่านไป/ประมาณการ/บรรทัดล่าสุดของ log

    เวลาคิดที่ server เพราะนาฬิกาเครื่องผู้ใช้กับ server ไม่จำเป็นต้องตรงกัน — ถ้าให้หน้าเว็บ
    ลบเวลาเอง งานที่เพิ่งเริ่มอาจกลายเป็น "ผ่านไป 7 ชั่วโมง" เมื่อ timezone เพี้ยน
    """
    with _JOB_LOCK:
        st = dict(_JOB)
    if st.get("started"):
        try:
            base = datetime.fromisoformat(str(st["started"]))
            end = (datetime.fromisoformat(str(st["finished"]))
                   if st.get("finished") else datetime.now())
            st["elapsed_sec"] = max(0, int((end - base).total_seconds()))
        except (TypeError, ValueError):
            pass
    st["eta_sec"] = _i(st.get("weeks")) * _SEC_PER_WEEK
    if st.get("running") and st.get("log"):
        st["progress"] = _tail_line(Path(str(st["log"])))
    return st


def _set(**kw):
    with _JOB_LOCK:
        _JOB.update(kw)


def _run_script(name: str, env: dict, log: Path) -> int:
    """รันสคริปต์ pipeline 1 ตัวเป็น subprocess (cwd = root ของ repo เพราะมันอ่าน config.ini
    และ path แบบสัมพัทธ์กับตัวเอง) — เก็บ stdout ลงไฟล์ log ไว้ให้ไล่ปัญหาย้อนหลังได้"""
    import subprocess
    import sys
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "a", encoding="utf-8", errors="replace") as fh:
        fh.write(f"\n===== {name} @ {datetime.now():%Y-%m-%d %H:%M:%S} =====\n")
        fh.flush()
        p = subprocess.run([sys.executable, name], cwd=str(config.REPO_DIR),
                           env=env, stdout=fh, stderr=subprocess.STDOUT)
    return p.returncode


def _fetch_job(start: tuple, want: list, forced: bool = False):
    """งานเบื้องหลัง: ดึง booking ช่วงที่สั่งจาก DB → รัน AVA_MC → เขียนลงทะเบียน

    forced = ผู้ใช้สั่งดึงใหม่ทับของเดิม (บอกไว้ในข้อความสถานะให้รู้ว่ากำลังดึงทับ ไม่ใช่เติมส่วนที่ขาด)
    """
    import os as _os
    lo, hi = want[0], want[-1]
    # ดึงเผื่อสัปดาห์ก่อนหน้าไว้ "อุ่นลูกโซ่" — AVA_MC ต้องเห็นสัปดาห์ก่อน lo ถึงจะรู้ว่าเครื่องไหน
    # วิ่งอยู่แล้ว (carry) เครื่องไหนตั้งใหม่ (setup) ; ไม่เผื่อ = สัปดาห์แรกที่ผู้ใช้ขอถูกนับเป็น
    # ตั้งเครื่องใหม่ทั้งหมด (W2/2026 เคยได้ setup 368 · carry 0) ความเพี้ยนจึงย้ายไปตกที่
    # สัปดาห์ warm-up แทน ซึ่งติดธง edge ไว้ไม่ให้หน้าเว็บนับ
    warm = _minus_weeks(lo, _WARMUP_WEEKS)
    if warm[0] <= 0 or not (warm < lo):
        warm = lo
    _WORK_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    in_dir = _WORK_DIR / f"in_{stamp}"
    in_dir.mkdir(parents=True, exist_ok=True)
    out_file = _WORK_DIR / f"history_ava_{_yw_int(warm)}_{_yw_int(hi)}.xlsx"
    log = config.LOGS_DIR / f"history_fetch_{stamp}.log"
    # บอกหน้าเว็บว่าจะอ่าน heartbeat จากไฟล์ไหน (fetch_status หยิบบรรทัดท้ายมาแสดง)
    _set(log=str(log))
    try:
        # ── 1/3 ดึง booking จาก DB (ช้าสุด: view วิ่งข้าม dblink) ──
        _set(step="fetch_db",
             detail=("ดึง booking ใหม่ทับ" if forced else "ดึง booking")
                    + f" W{lo[1]}/{lo[0]}–W{hi[1]}/{hi[0]} จาก DB")
        env = _os.environ.copy()
        env["KP_BOOKING_FROM_YW"] = str(_yw_int(warm))   # เริ่มที่สัปดาห์ warm-up ไม่ใช่ lo
        env["KP_BOOKING_TO_YW"] = str(_yw_int(hi))
        env["KP_BOOKING_OUT"] = str(in_dir / "view_booking.xlsx")
        rc = _run_script("View_Booking.py", env, log)
        if rc != 0 or not (in_dir / "view_booking.xlsx").exists():
            raise RuntimeError(f"View_Booking ล้มเหลว (exit {rc}) — ดู {log.name}")

        # ── 2/3 คิดเครื่อง/setup/carry ด้วย AVA_MC ตัวเดียวกับแผนจริง ──
        _set(step="ava_mc", detail="คำนวณเครื่อง/setup/carry ด้วย AVA_MC")
        env2 = _os.environ.copy()
        env2["AVA_BOOKING_DIR"] = str(in_dir)
        env2["AVA_OUT_FILE"] = str(out_file)
        rc = _run_script("AVA_MC.py", env2, log)
        if rc != 0 or not out_file.exists():
            raise RuntimeError(f"AVA_MC ล้มเหลว (exit {rc}) — ดู {log.name}")

        # ── 3/3 เข้าทะเบียน (คีย์ซ้ำ = ของใหม่ชนะ ตามกติกาเดิม) ──
        _set(step="save", detail="บันทึกลงทะเบียนประวัติ")
        with _LOCK:
            blocks, meta = _read_registry()
            before = len(blocks)
            # ขอบข้อมูล = สัปดาห์ warm-up (สัปดาห์แรกของไฟล์) — ธง edge อยู่ที่นั่น
            # สัปดาห์ที่ผู้ใช้ขอ (lo) จึงมี carry/setup ที่คำนวณจากของจริงแล้ว
            meta[out_file.name] = _ingest(out_file, blocks, kind="history", edge_week=warm)
            _write_registry(blocks, meta)
        _set(added=len(blocks) - before)
        _set(step="done", detail=f"เสร็จแล้ว — ได้ {len(want)} สัปดาห์", finished=datetime.now().isoformat(timespec="seconds"))
    except Exception as e:
        _set(step="error", error=str(e), finished=datetime.now().isoformat(timespec="seconds"))
    finally:
        _set(running=False)
        # ไฟล์ input ชั่วคราวไม่ต้องเก็บ (ผลอยู่ในทะเบียนแล้ว)
        try:
            for f in in_dir.glob("*"):
                f.unlink()
            in_dir.rmdir()
        except OSError:
            pass
        # ไฟล์ผล AVA คงไว้ให้ไล่ปัญหาย้อนหลังได้ แต่เก็บแค่ 5 ก้อนล่าสุด — ก้อนละ ~0.3-1 MB
        # และ scheduler ไม่ได้ลบให้ (ชื่อไม่ตรง pattern) ถ้าไม่กวาดเองจะสะสมจนกินดิสก์ server
        try:
            old = sorted(_WORK_DIR.glob("history_ava_*.xlsx"),
                         key=lambda p: p.stat().st_mtime, reverse=True)[5:]
            for f in old:
                f.unlink()
        except OSError:
            pass


def start_fetch(year, week, force: bool = False) -> dict:
    """สั่งดึงประวัติตั้งแต่ (ปี, สัปดาห์) — คืนสถานะทันที ไม่รอให้เสร็จ

    force=True = ดึง booking ของทั้งช่วงที่เลือกใหม่จาก DB แล้วคำนวณ AVA_MC ทับทะเบียนเสมอ
    (ปุ่ม "ดึงใหม่จาก DB" บนหน้าแผน) ; force=False = ดึงเฉพาะสัปดาห์ที่ทะเบียนยังไม่มี

    ไม่มีอะไรต้องดึง = คืน reason บอกสาเหตุให้หน้าเว็บพูดได้ว่าทำไมไม่ได้ยิง DB:
      complete    ทะเบียนมีครบช่วงนั้นแล้ว (โหมดไม่ force)
      empty_range ช่วงที่เลือกไม่มีสัปดาห์เลย (เลือกทับช่วงที่ booking สดครอบคลุมอยู่แล้ว)
      db_error    ถาม DB ว่ามีสัปดาห์ไหนไม่ได้ — ต้องบอก ไม่ใช่กลืนเป็น "ครบแล้ว"
    กำลังดึงอยู่ = ไม่รับงานซ้อน
    """
    with _JOB_LOCK:
        if _JOB["running"]:
            return {"started": False, "reason": "busy", **_JOB}
    if not force:
        # โหมดปกติพึ่งรายการสัปดาห์จาก DB — ถามไม่ได้ต้องแจ้ง ไม่ใช่รายงานว่าครบแล้ว
        db = db_weeks()
        if db.get("error"):
            return {"started": False, "reason": "db_error", "error": db["error"]}
    want = missing_weeks(year, week, force=force)
    if not want:
        return {"started": False, "queued": 0,
                "reason": "empty_range" if force else "complete"}
    _set(running=True, step="queued", detail="เตรียมดึงข้อมูล", error="", added=0,
         forced=bool(force), log="",
         **{"from": {"year": want[0][0], "week": want[0][1]},
            "to": {"year": want[-1][0], "week": want[-1][1]},
            "weeks": len(want), "started": datetime.now().isoformat(timespec="seconds"),
            "finished": None})
    threading.Thread(target=_fetch_job, args=(_yw((year, week)), want, bool(force)),
                     daemon=True).start()
    return {"started": True, "queued": len(want), "forced": bool(force),
            "from": {"year": want[0][0], "week": want[0][1]},
            "to": {"year": want[-1][0], "week": want[-1][1]}}

"""
cyl_approve.py — Change Cylinder ที่ "user ติ๊กอนุมัติเอง" (ไม่ใช่ของที่แผนคิดเอง)

ไฟล์ที่เกี่ยวข้อง
    data_plan/cylinder_approved.json    สถานะปัจจุบัน = เครื่องที่ย้ายเกจไปแล้ว (ถาวรจนกว่าจะยกเลิก)
    data_plan/cylinder_change_log.jsonl สมุดบันทึกทุกเหตุการณ์ ต่อท้ายอย่างเดียว ไม่เคยลบ

ทำไมเก็บเป็น "สถานะถาวร" ไม่ใช่ "คำขอให้แผนลองทำ"
    user ติ๊ก = ตัดสินใจย้ายเครื่องข้ามเกจแล้วจริง ๆ ดังนั้นรอบวางแผนถัดไป (ทั้ง Planning ปกติ
    และรอบ Replan) ต้องเห็นเครื่องอยู่เกจใหม่ "ตั้งแต่ต้นรอบ" โดยไม่ต้องให้ user มาติ๊กซ้ำ
    → Planning.py seed cylinder_adjustments จากไฟล์นี้ (ไม่ผ่านกฎ JIT/โควตาของ cylinder change
      อัตโนมัติ เพราะไม่ได้ "ขอเปลี่ยนระหว่างวางแผน" แต่เครื่องถูกย้ายไปแล้วก่อนรอบจะเริ่ม)
    → plan_view.ava_by_week() บวก/ลบทับตอนอ่าน ทำให้เลขเครื่องว่างบนหน้าเว็บเปลี่ยนทันทีที่ติ๊ก
      โดยไม่แตะไฟล์ booking_final_*.xlsx ที่ AVA_MC เป็นเจ้าของ

เส้นเวลา (ตรงกับ Planning.py:1156 และ :6478)
    set_week   = สัปดาห์ที่สั่งเปลี่ยน Cylinder (ต้อง >= TODAY+2 เพราะสองสัปดาห์แรกถูก freeze)
    ready_week = สัปดาห์ที่เครื่องพร้อมผลิต = สัปดาห์ถัดจาก set_week (setup 3-5 วันหักจาก
                 วันผลิตของสัปดาห์นั้นเอง ไม่ได้กินทั้งสัปดาห์)
    เดลต้าเครื่องมีผลตั้งแต่ ready_week เป็นต้นไปจนจบ horizon (Cylinder เปลี่ยนแล้วอยู่ถาวร)
"""
import json
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import config
import livesync

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน — ไฟล์อยู่ราก repo)

STATE_FILE = config.OUTPUT_DIR / "cylinder_approved.json"
LOG_FILE = config.OUTPUT_DIR / "cylinder_change_log.jsonl"

# กันไฟล์ log โตไม่จำกัดตอนอ่านขึ้นหน้าเว็บ (ไฟล์จริงยังเก็บครบทุกบรรทัด)
_LOG_READ_MAX = 2000


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _s(v) -> str:
    return str(v if v is not None else "").strip()


def _gnorm(g) -> str:
    """เกจให้เป็นคีย์เดียวกับ ava/Planning: 28.0 / '28 ' / 'G28' / 'Gauge 28' → '28'"""
    s = _s(g).upper().replace("GAUGE", "").replace("G", "").strip()
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def _int(v, default=None):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def key_of(factory: str, cat: str, src_gauge: str, tgt_gauge: str, ready_week) -> str:
    """คีย์ของการอนุมัติหนึ่งรายการ — ติ๊กซ้ำที่เดิม = เพิ่มจำนวนเครื่องในรายการเดิม
    ไม่ใช่สร้างรายการใหม่ (ไม่งั้นตอนยกเลิกจะเหลือเศษที่ไม่มีใครรู้ว่าของใคร)"""
    return (f"{_s(factory).upper()}|{_s(cat).upper()}|"
            f"{_gnorm(src_gauge)}>{_gnorm(tgt_gauge)}|W{_int(ready_week, 0)}")


# ---------------- Cylinder สำรอง (MasterMC[Spare part]) ----------------
# อยู่ที่นี่เพราะเป็นเพดานของ "เครื่องที่ย้ายเกจได้" ซึ่งเป็น state ที่ไฟล์นี้ดูแล —
# ทั้งฝั่งที่เสนอ (cylinder_advisor) และฝั่งที่บันทึก (approve) ต้องใช้ตัวเลขชุดเดียวกัน
# ไม่งั้นหน้าเว็บบอกว่าเหลือ 2 แต่บันทึกได้ 16 (เครื่องผีหลุดเข้าแผน)
_SPARE_CACHE = {"key": None, "map": None}


def catnorm(cat) -> str:
    """normalize ชื่อกลุ่มเครื่องก่อนจับคู่กับชีท Spare part

    ⚠️ MasterMC สะกดไม่ตรงกันระหว่างชีท: ชีท 'Master MC' ใช้ 'SINGEL-16'..'SINGEL-26'
    (สะกดผิด) แต่ชีท 'Spare part' ใช้ 'SINGLE-16'.. → เทียบตรง ๆ จะหา Cylinder สำรองของ
    6 กลุ่มนี้ไม่เจอเลย แล้วรายงานว่า 'ต้องซื้อ Cylinder เพิ่ม' ทั้งที่มีของอยู่
    """
    return _s(cat).upper().replace("SINGEL", "SINGLE")


def spare_map() -> dict:
    """Cylinder สำรองจาก MasterMC[Spare part] → {(FACTORY, MC_CAT, GUAGE): total_spare}

    cache ตาม (path, mtime) — ทั้งการวิเคราะห์และการติ๊กเรียกใช้ทุกครั้ง และไฟล์อยู่บน
    OneDrive ซึ่งอ่านช้ากว่าการคำนวณทั้งก้อนหลายเท่า (สูตรเดียวกับ _calendar_weekly)
    """
    path = config.master_files().get("MasterMC")
    if path is None or not Path(path).exists():
        return {}
    try:
        ck = (str(path), Path(path).stat().st_mtime)
    except OSError:
        return {}
    if _SPARE_CACHE["key"] == ck:
        return _SPARE_CACHE["map"]
    out: dict = {}
    try:
        import pandas as pd
        df = pd.read_excel(path, sheet_name="Spare part")
        df.columns = df.columns.str.strip()
        for _, r in df.iterrows():
            fac = _s(r.get("Factory")).upper()
            cat = catnorm(r.get("MC_CAT"))
            g = _gnorm(r.get("Guage"))
            if not fac or not cat or not g:
                continue
            n = _int(r.get("Total Spare"), None)
            if n is not None:
                out[(fac, cat, g)] = n
    except Exception as e:
        print(f"[CYL APPROVE] อ่าน Cylinder สำรองจาก MasterMC ไม่ได้: {e}")
        return {}
    _SPARE_CACHE.update(key=ck, map=out)
    return out


def spare_row_key(factory, cat, gauge):
    """คีย์แถวในชีท Spare part ที่ตรงกับ (โรงงาน, กลุ่มเครื่อง, เกจ) — exact ก่อน แล้ว prefix match
    ('SINGLE' ในตารางหนึ่งกับ 'SINGLE-32' ในอีกตาราง = กลุ่มเดียวกัน)
    กติกาเดียวกับ Planning._get_spare_info (Planning.py:904) ; ไม่มีแถวนั้น = None

    ต้องคืน "คีย์ตามที่อยู่ในไฟล์" ไม่ใช่คีย์ที่ขอมา เพราะตอนหักยอดต้องเขียนลงแถวที่ match จริง
    """
    f = _s(factory).upper()
    c = catnorm(cat)
    g = _gnorm(gauge)
    smap = spare_map()
    if (f, c, g) in smap:
        return (f, c, g)
    prefix = c.split("-")[0]
    best_k, best_v = None, -1
    for k, v in smap.items():
        if k[0] == f and k[2] == g and k[1].split("-")[0] == prefix and int(v) > best_v:
            best_k, best_v = k, int(v)
    return best_k


def spare_total(factory, cat, gauge) -> int:
    """Cylinder สำรองที่ "ยังเหลือ" ของ (โรงงาน, กลุ่มเครื่อง, เกจ) — อ่านจากไฟล์ตรง ๆ

    ยอดในไฟล์ = ยอดคงเหลือจริง เพราะการติ๊กเปลี่ยน Cylinder หักลงไฟล์ทันที (ดู approve())
    → ห้ามหักด้วย approvals ซ้ำอีกชั้น ไม่งั้นนับสองรอบ
    """
    k = spare_row_key(factory, cat, gauge)
    return int(spare_map().get(k, 0)) if k else 0


# ---------------- โควตาแรงงาน: เปลี่ยน Cylinder ได้กี่เครื่องต่อสัปดาห์ ----------------
# ตรงกับ Planning.CYLINDER_CHANGE_LIMIT (Planning.py:909) และ _cylinder_quota_group
# (Planning.py:970) — เพดานของ "คนที่ลงมือเปลี่ยน" ไม่ใช่ของที่มี จึงคิดเป็นรายสัปดาห์และ
# นับที่สัปดาห์ที่ลงมือเปลี่ยน (set_week) ไม่ใช่สัปดาห์ที่เครื่องพร้อมใช้
def change_limit_per_week() -> int:
    """โควตาช่างเปลี่ยน Cylinder ต่อสัปดาห์ต่อกลุ่ม — ค่าเดียวกับ Planning.CYLINDER_CHANGE_LIMIT
    อ่านจากหน้า "ตั้งค่าระบบ" ทุกครั้ง (webapp รันค้าง ถ้าเก็บเป็นค่าคงที่จะค้างค่าเก่า)"""
    plan_config.reload()
    return int(plan_config.get("KP_CYLINDER_CHANGE_LIMIT"))


def freeze_weeks() -> int:
    """สัปดาห์ต้นที่แก้แผนไม่ได้ — ค่าเดียวกับ Planning.FREEZE_WEEKS และ Gantt"""
    plan_config.reload()
    return int(plan_config.get("KP_FREEZE_WEEKS"))


def quota_group(factory, cat):
    """กลุ่มโควตา: 'PHET_DOUBLE' / 'PHET_SINGLE' / 'OMNOI' — นอกนั้น None (ไม่จำกัด)
    กติกาเดียวกับ Planning._cylinder_quota_group ; ค่า FACTORY_TYPE ในแผนจริงคือ PHET/OMNOI
    (รับ 'OM' ด้วยเพราะบางที่เขียนสั้น) และ CAT มีทั้ง DOUBLE-*/SINGLE-*/SINGEL-* (สะกดผิด)"""
    f = _s(factory).upper()
    c = catnorm(cat)
    if f in ("OMNOI", "OM"):
        return "OMNOI"
    if f == "PHET":
        return "PHET_DOUBLE" if "DOUBLE" in c else "PHET_SINGLE"
    return None


def approved_by_week_group() -> dict:
    """เครื่องที่ติ๊กไว้แล้ว แยกตามสัปดาห์ที่ลงมือเปลี่ยน → {(set_week, group): machines}"""
    out: dict = {}
    for r in _read_state():
        g = quota_group(r.get("factory"), r.get("cat"))
        wk = _int(r.get("set_week"))
        if g is None or wk is None:
            continue
        out[(wk, g)] = out.get((wk, g), 0) + (_int(r.get("machines"), 0) or 0)
    return out


def apply_spare_deltas(deltas: dict) -> int:
    """แก้ยอด Total Spare ในชีท Spare part ของ MasterMC.xlsx (ไฟล์บน server ที่ user แก้เองได้)

    deltas = {(FAC, MC_CAT, GUAGE): delta}  ลบ = ติ๊กใช้ไป · บวก = ยกเลิกแล้วคืนของ
    คืนจำนวนแถวที่แก้ ; โยน ValueError ถ้าเขียนไม่ได้ (ผู้เรียกต้องไม่บันทึกการติ๊กต่อ)

    ทำไมเขียนลงไฟล์แทนที่จะหักตอนแสดง: ทีมเปิดไฟล์ MasterMC บน server (หน้า Masters) เพื่อดู
    ว่าเหลือ Cylinder กี่อัน ถ้าไฟล์ยังเป็นยอดเต็มก็อ่านผิดทุกครั้ง — และ Planning ก็อ่าน
    Total Spare จากไฟล์เดียวกันนี้ (Planning.py:546) จึงต้องเป็นยอดคงเหลือจริงทั้งระบบ
    แก้เฉพาะเซลล์ Total Spare ของแถวที่ตรง — เซลล์/ชีทอื่นไม่แตะ และสำรอง .bak ก่อนทุกครั้ง
    """
    rows = {k: int(v) for k, v in (deltas or {}).items() if int(v or 0) != 0}
    if not rows:
        return 0
    path = config.master_files().get("MasterMC")
    if path is None or not Path(path).exists():
        raise ValueError("ไม่พบไฟล์ MasterMC บน server — แก้ยอด Cylinder สำรองไม่ได้")
    try:
        from openpyxl import load_workbook
        livesync.backup_throttled(path)
        wb = load_workbook(path, data_only=False)
        if "Spare part" not in wb.sheetnames:
            wb.close()
            raise ValueError("ไฟล์ MasterMC ไม่มีชีท 'Spare part'")
        ws = wb["Spare part"]
        header = [_s(c.value).upper() for c in ws[1]]

        def col(*names):
            for n in names:
                if n in header:
                    return header.index(n) + 1
            return 0
        c_fac, c_cat, c_g = col("FACTORY"), col("MC_CAT"), col("GUAGE", "GAUGE")
        c_tot = col("TOTAL SPARE")
        if not all((c_fac, c_cat, c_g, c_tot)):
            wb.close()
            raise ValueError("ชีท Spare part ไม่มีคอลัมน์ Factory/MC_CAT/Guage/Total Spare ครบ")
        hit = 0
        for r in range(2, ws.max_row + 1):
            key = (_s(ws.cell(r, c_fac).value).upper(),
                   catnorm(ws.cell(r, c_cat).value),
                   _gnorm(ws.cell(r, c_g).value))
            d = rows.get(key)
            if not d:
                continue
            cur = _int(ws.cell(r, c_tot).value, 0) or 0
            new = cur + d
            if new < 0:
                wb.close()
                raise ValueError(
                    f"Cylinder สำรองไม่พอ: {key[0]} {key[1]} Guage {key[2]} เหลือ {cur} อัน "
                    f"แต่ต้องใช้ {abs(d)} อัน")
            ws.cell(r, c_tot).value = new
            hit += 1
            rows.pop(key)
        if rows:
            wb.close()
            miss = ", ".join(f"{k[0]} {k[1]} Guage {k[2]}" for k in rows)
            raise ValueError(f"ไม่พบแถวใน Spare part สำหรับ: {miss}")
        with livesync.atomic_save(path) as tmp:
            wb.save(tmp)
        wb.close()
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"เขียนยอด Cylinder สำรองลง MasterMC ไม่ได้: {e}")
    _SPARE_CACHE.update(key=None, map=None)      # ยอดในไฟล์เปลี่ยนแล้ว → อ่านใหม่รอบหน้า
    return hit


def sync_spare_file() -> int:
    """หักยอดในไฟล์ให้รายการที่ติ๊กไว้ "ก่อน" ระบบเปลี่ยนมาเขียนไฟล์ (ทำครั้งเดียวต่อรายการ)

    รายการเก่าไม่มีธง spare_applied → ยอดในไฟล์ยังเป็นยอดเต็มอยู่ ถ้าไม่หักให้ครั้งแรก
    ยอดคงเหลือจะสูงกว่าความจริงตลอดไป ; หักแล้วปักธงกันหักซ้ำ
    """
    rows = _read_state()
    pending = [r for r in rows if not r.get("spare_applied")
               and (_int(r.get("machines"), 0) or 0) > 0]
    if not pending:
        return 0
    deltas: dict = {}
    for r in pending:
        k = spare_row_key(r.get("factory"), r.get("cat"), r.get("tgt_gauge"))
        if k is None:
            continue
        deltas[k] = deltas.get(k, 0) - (_int(r.get("machines"), 0) or 0)
    if deltas:
        apply_spare_deltas(deltas)
    for r in pending:
        r["spare_applied"] = True
    _write_state(rows)
    print(f"[CYL APPROVE] sync ยอด Cylinder สำรองลง MasterMC สำหรับ {len(pending)} รายการเดิม")
    return len(pending)


def approved_by_target() -> dict:
    """เครื่องที่ติ๊กย้ายเข้าเกจปลายทางไปแล้ว → {(FACTORY, MC_CAT, GUAGE): machines}
    = จำนวน Cylinder สำรองที่ถูกใช้ไปแล้วของเกจนั้น"""
    out: dict = {}
    for r in _read_state():
        k = (_s(r.get("factory")).upper(), catnorm(r.get("cat")), _gnorm(r.get("tgt_gauge")))
        out[k] = out.get(k, 0) + (_int(r.get("machines"), 0) or 0)
    return out


# ---------------- state ----------------
def _read_state() -> list:
    if not STATE_FILE.exists():
        return []
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[CYL APPROVE] อ่าน {STATE_FILE.name} ไม่ได้: {e}")
        return []
    rows = data.get("approvals") if isinstance(data, dict) else data
    return [r for r in (rows or []) if isinstance(r, dict)]


def _write_state(rows: list) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with livesync.atomic_save(STATE_FILE) as tmp:
        tmp.write_text(
            json.dumps({"approvals": rows}, ensure_ascii=False, indent=2), encoding="utf-8")


@contextmanager
def _edit():
    """ครอบทุกคำสั่งที่ติ๊ก/ยกเลิกการเปลี่ยนสูบ — เข้าคิวกับคนอื่นที่แก้ MasterMC

    ใช้ล็อกของ MasterMC ตัวเดียวคุมทั้งสองไฟล์ (MasterMC + cylinder_approved.json)
    เพราะการติ๊ก 1 ครั้งแก้ทั้งคู่และต้องไปด้วยกัน: หักยอด Cylinder สำรองในชีท Spare part
    แล้วจึงบันทึกสถานะ ถ้าปล่อยให้อีกคนแทรกกลางทาง ยอดสำรองกับรายการที่ติ๊กจะไม่ตรงกัน
    """
    path = config.master_files().get("MasterMC") or STATE_FILE
    with livesync.file_lock(path):
        yield


def list_approvals() -> list:
    """รายการที่อนุมัติไว้ทั้งหมด (ใหม่สุดก่อน) — หน้าเว็บใช้โชว์ว่าเครื่องไหนถูกย้ายไปแล้ว"""
    rows = _read_state()
    rows.sort(key=lambda r: _s(r.get("at")), reverse=True)
    return rows


# ---------------- log (append-only) ----------------
def _append_log(event: dict) -> None:
    """เขียนต่อท้ายบรรทัดเดียว — ไม่อ่านไฟล์เดิมก่อน จึงไม่ชนกับใครและไฟล์ที่เปิดค้างไม่มีผล"""
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as e:
        # log เขียนไม่ได้ต้องไม่ทำให้การอนุมัติล้ม — แต่ต้องเห็นใน console ว่าขาดร่องรอย
        print(f"[CYL APPROVE] เขียน log ไม่ได้: {e}")


def read_log(limit: int = 200) -> list:
    """เหตุการณ์ล่าสุดก่อน — หน้าหลัก (admin เท่านั้น) ใช้แสดงประวัติ"""
    if not LOG_FILE.exists():
        return []
    try:
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        print(f"[CYL APPROVE] อ่าน log ไม่ได้: {e}")
        return []
    out = []
    for ln in lines[-_LOG_READ_MAX:]:
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            continue          # บรรทัดเสีย (เขียนค้าง) ข้ามไป ไม่ทำให้ทั้งหน้าพัง
    out.reverse()
    return out[:max(1, int(limit or 200))]


# ---------------- อนุมัติ / ยกเลิก ----------------
def _clean_entry(e: dict) -> dict:
    """ตรวจ + normalize รายการที่ติ๊กมา 1 รายการ — โยน ValueError ถ้าข้อมูลไม่พอเปลี่ยนเครื่องจริง"""
    factory = _s(e.get("factory")).upper()
    cat = _s(e.get("cat")).upper()
    src = _gnorm(e.get("src_gauge"))
    tgt = _gnorm(e.get("tgt_gauge") or e.get("gauge"))
    ready = _int(e.get("ready_week"))
    machines = _int(e.get("machines"), 0) or 0
    if not factory or not cat:
        raise ValueError("ไม่รู้โรงงาน/กลุ่มเครื่อง (factory/cat) ของรายการที่ติ๊ก")
    if not src or not tgt:
        raise ValueError("ต้องระบุทั้ง Guage ต้นทางและ Guage ปลายทาง")
    if src == tgt:
        raise ValueError(f"Guage ต้นทางกับปลายทางเป็น Guage เดียวกัน ({src}) — ไม่ต้องเปลี่ยน Cylinder")
    if ready is None:
        raise ValueError("ไม่รู้สัปดาห์ที่ต้องการใช้เครื่อง (ready_week)")
    if machines <= 0:
        raise ValueError("จำนวนเครื่องต้องมากกว่า 0")
    set_week = _int(e.get("set_week"))
    return {
        "factory": factory, "cat": cat,
        "src_gauge": src, "tgt_gauge": tgt,
        "ready_week": ready,
        # สัปดาห์ที่ลงมือเปลี่ยน — ปกติ = ready − 1 (ฝั่งที่เรียกส่งมาให้ตามแกนปฏิทินจริง
        # เพราะสัปดาห์ที่โรงปิดทั้งสัปดาห์ต้องข้าม ไม่ใช่ลบ 1 ตรง ๆ)
        "set_week": set_week if set_week is not None else ready - 1,
        "year": _int(e.get("year")),
        "machines": machines,
        "items": [_s(x).upper() for x in (e.get("items") or []) if _s(x)],
        "note": _s(e.get("note")),
    }


def _spare_wanted(cleaned: list) -> dict:
    """รวมเครื่องที่ติ๊กมาต่อแถวสำรองในไฟล์ → {(FAC, CAT, GAUGE): machines}"""
    want: dict = {}
    for e in cleaned:
        k = spare_row_key(e["factory"], e["cat"], e["tgt_gauge"])
        if k is None:
            raise ValueError(
                f"ไม่พบ Cylinder สำรองของ {e['factory']} {e['cat']} Guage {e['tgt_gauge']} "
                "ในชีท Spare part ของ MasterMC — เพิ่มแถวในไฟล์ก่อนถึงจะติ๊กได้")
        want[k] = want.get(k, 0) + e["machines"]
    return want


def _check_spare(want: dict) -> None:
    """กันติ๊กเกิน Cylinder สำรองที่เหลือจริงในไฟล์ — เช็ค "ทั้งชุดรวมกัน" ไม่ใช่ทีละรายการ

    ทำไมต้องมี: ทุกแถวในหน้าเสนอมองเห็นสำรองก้อนเดียวกันแล้วต่างคนต่างบอกว่าใช้ได้เท่ายอดนั้น
    (เกจ 28 เหลือ 2 อัน แต่ 8 แถวติ๊กแถวละ 2 = 16) ถ้าไม่กันตรงนี้ เครื่องที่ไม่มีอยู่จริงจะถูก
    บวกเข้า ava/Planning แล้วแผนทั้งใบวางบนเครื่องผี — ต้องกันที่ชั้นบันทึก เพราะเป็นด่านสุดท้าย
    ที่ทุกทางเข้า (หน้าเว็บ/เรียก API ตรง) ผ่านเหมือนกัน

    ยอดในไฟล์ = คงเหลือจริง (ติ๊กแล้วหักลงไฟล์ทันที) จึงไม่ต้องลบ approvals ซ้ำ
    """
    for (fac, cat, g), add in want.items():
        left = spare_total(fac, cat, g)
        if add > left:
            raise ValueError(
                f"Cylinder สำรองไม่พอ: {fac} {cat} Guage {g} เหลือ {left} อัน "
                f"แต่ที่ติ๊กมารอบนี้รวม {add} เครื่อง "
                "(ทุกแถวที่เปลี่ยนเป็น Guage นี้ใช้สำรองก้อนเดียวกัน) — ลดจำนวนลงหรือติ๊กน้อยแถวลง")


def _check_week_quota(cleaned: list) -> None:
    """กันติ๊กเกินกำลังคนที่เปลี่ยน Cylinder ได้จริงในสัปดาห์เดียว (โควตาตั้งที่หน้าตั้งค่าระบบ)

    ต่างจาก _check_spare: สำรองคือ "ของที่มี" (ถาวรทั้งแผน) ส่วนนี่คือ "แรงงานของสัปดาห์นั้น"
    → สัปดาห์หน้าโควตาเต็มใหม่ ติ๊กต่อได้ ถ้ายังมีสำรองเหลือ
    """
    limit = change_limit_per_week()
    used = approved_by_week_group()
    want: dict = {}
    for e in cleaned:
        g = quota_group(e["factory"], e["cat"])
        if g is None:
            continue                      # โรงงานนอกกติกา (เช่น OUTSOURCE) ไม่มีโควตา
        k = (e["set_week"], g)
        want[k] = want.get(k, 0) + e["machines"]
    for (wk, g), add in want.items():
        before = used.get((wk, g), 0)
        if before + add > limit:
            left = max(0, limit - before)
            raise ValueError(
                f"เปลี่ยน Cylinder ได้ไม่เกิน {limit} เครื่องต่อสัปดาห์ต่อกลุ่ม: "
                f"{g} สัปดาห์ที่สั่งเปลี่ยน W{wk}"
                + (f" ติ๊กไว้แล้ว {before} เครื่อง" if before else "")
                + f" → เหลือโควตา {left} เครื่อง แต่ที่ติ๊กมารอบนี้รวม {add} เครื่อง"
                + " — เลื่อนบางรายการไปสั่งเปลี่ยนสัปดาห์อื่น หรือลดจำนวนลง")


def approve(entries: list, user: str = "", sheet: str = "", plan_file: str = "") -> dict:
    """ติ๊กอนุมัติ (หลายรายการพร้อมกัน) → บันทึกสถานะ + ลง log

    ติ๊กคีย์เดิมซ้ำ = บวกจำนวนเครื่องเข้ารายการเดิม (เช่นเคยติ๊ก 1 เครื่อง แล้วติ๊กอีก 2 = 3)
    """
    if not entries:
        raise ValueError("ไม่มีรายการที่ติ๊ก")
    with _edit():
        return _approve_locked(entries, user, sheet, plan_file)


def _approve_locked(entries: list, user: str, sheet: str, plan_file: str) -> dict:
    # ตรวจให้ครบทั้งชุดก่อนเขียนอะไรลงไฟล์ — ล้มกลางทางแล้วเหลือครึ่ง ๆ กลาง ๆ ไม่ได้
    sync_spare_file()              # รายการเก่าที่ยังไม่ถูกหักในไฟล์ ต้องหักก่อนเทียบยอด
    cleaned = [_clean_entry(raw) for raw in entries]
    want = _spare_wanted(cleaned)
    _check_spare(want)             # ของที่มี (Cylinder สำรองคงเหลือในไฟล์ — ถาวรทั้งแผน)
    _check_week_quota(cleaned)     # กำลังคนของสัปดาห์นั้น (2 เครื่อง/สัปดาห์/กลุ่ม)
    # หักยอดลงไฟล์ MasterMC ก่อนบันทึกสถานะ — เขียนไฟล์ล้ม = ยกเลิกทั้งชุด ไม่มีอะไรถูกบันทึก
    apply_spare_deltas({k: -v for k, v in want.items()})
    rows = _read_state()
    by_key = {_s(r.get("id")): r for r in rows}
    added, updated = [], []
    for e in cleaned:
        k = key_of(e["factory"], e["cat"], e["src_gauge"], e["tgt_gauge"], e["ready_week"])
        cur = by_key.get(k)
        if cur:
            before = _int(cur.get("machines"), 0) or 0
            cur["machines"] = before + e["machines"]
            cur["items"] = sorted(set((cur.get("items") or []) + e["items"]))
            cur["at"] = _now()
            cur["by"] = _s(user)
            cur["spare_applied"] = True      # หักในไฟล์ MasterMC ไปแล้ว (กันหักซ้ำตอน sync)
            updated.append({**e, "id": k, "machines_before": before,
                            "machines": cur["machines"]})
        else:
            rec = {**e, "id": k, "at": _now(), "by": _s(user),
                   "sheet": _s(sheet), "plan_file": _s(plan_file),
                   "spare_applied": True}
            rows.append(rec)
            by_key[k] = rec
            added.append(rec)
    _write_state(rows)

    for rec in added + updated:
        _append_log({
            "at": _now(), "by": _s(user), "action": "APPROVE",
            "id": rec["id"], "factory": rec["factory"], "cat": rec["cat"],
            "src_gauge": rec["src_gauge"], "tgt_gauge": rec["tgt_gauge"],
            "set_week": rec["set_week"], "ready_week": rec["ready_week"],
            "machines": rec["machines"],
            "machines_before": rec.get("machines_before"),
            "items": rec.get("items") or [],
            "sheet": _s(sheet), "plan_file": _s(plan_file),
            "detail": (f"ย้ายเครื่อง {rec['machines']} ตัว {rec['factory']} {rec['cat']} "
                       f"G{rec['src_gauge']}→G{rec['tgt_gauge']} "
                       f"(สั่งเปลี่ยน W{rec['set_week']} พร้อมใช้ W{rec['ready_week']})"),
        })
    ent = livesync.bump(livesync.CYLINDER, user=user,
                        detail=f"ติ๊กเปลี่ยนสูบ {len(added) + len(updated)} รายการ")
    return {"ok": True, "added": len(added), "updated": len(updated),
            "approvals": list_approvals(), "rev": ent["rev"]}


def revoke(approval_id: str, user: str = "") -> dict:
    """ยกเลิกการอนุมัติ → เครื่องกลับไปอยู่เกจเดิม + คืน Cylinder เข้าคลังในไฟล์ MasterMC"""
    with _edit():
        return _revoke_locked(_s(approval_id), user)


def _revoke_locked(key: str, user: str = "") -> dict:
    rows = _read_state()
    gone = [r for r in rows if _s(r.get("id")) == key]
    if not gone:
        raise ValueError(f"ไม่พบรายการที่อนุมัติไว้: {key}")
    g = gone[0]
    # คืนยอดลงไฟล์ก่อนลบสถานะ — คืนไม่สำเร็จต้องไม่ลบ ไม่งั้น Cylinder หายจากทะเบียนถาวร
    n = _int(g.get("machines"), 0) or 0
    if n > 0 and g.get("spare_applied"):
        k = spare_row_key(g.get("factory"), g.get("cat"), g.get("tgt_gauge"))
        if k is None:
            raise ValueError(
                f"ไม่พบแถว Cylinder สำรองของ {g.get('factory')} {g.get('cat')} "
                f"Guage {g.get('tgt_gauge')} ในไฟล์ MasterMC — คืนยอดไม่ได้ จึงยังยกเลิกไม่ได้")
        apply_spare_deltas({k: n})
    rows = [r for r in rows if _s(r.get("id")) != key]
    _write_state(rows)
    _append_log({
        "at": _now(), "by": _s(user), "action": "REVOKE",
        "id": key, "factory": g.get("factory"), "cat": g.get("cat"),
        "src_gauge": g.get("src_gauge"), "tgt_gauge": g.get("tgt_gauge"),
        "set_week": g.get("set_week"), "ready_week": g.get("ready_week"),
        "machines": g.get("machines"), "items": g.get("items") or [],
        "detail": (f"ยกเลิก — คืนเครื่อง {g.get('machines')} ตัว {g.get('factory')} "
                   f"{g.get('cat')} G{g.get('tgt_gauge')}→G{g.get('src_gauge')} "
                   f"(เดิมพร้อมใช้ W{g.get('ready_week')})"),
    })
    ent = livesync.bump(livesync.CYLINDER, user=user, detail=f"ยกเลิกการติ๊ก {key}")
    return {"ok": True, "removed": key, "approvals": list_approvals(), "rev": ent["rev"]}


def log_moves(moves: list, user: str = "", sheet: str = "", plan_file: str = "",
              approval_ids: list = None, compare: list = None) -> dict:
    """บันทึกงานที่ถูกย้ายตามหลังการติ๊ก (หน้าเว็บส่งมาหลังคำนวณแผนใหม่เสร็จ)

    moves = [{item, so, from_week, to_week, qty, mc}] — เก็บไว้ให้ตอบได้ว่า "ติ๊กแล้ว
    แผนเปลี่ยนอะไรไปบ้าง" ไม่ใช่รู้แค่ว่าเครื่องย้ายเกจ
    compare = [{item, mc_group, before:{week:qty}, after:{week:qty}}] — ยอดรายสัปดาห์
    ก่อน/หลังติ๊ก ของทุก item ที่แผนเปลี่ยน (รวมตัวที่โดนขยับเพราะเครื่อง/setup ใหม่)
    เก็บลง log เพื่อให้เปิดหน้ามาดูย้อนหลังได้ ไม่ใช่เห็นแค่ตอนติ๊กแล้วหายไป
    """
    rows = [m for m in (moves or []) if isinstance(m, dict)]
    if not rows:
        return {"ok": True, "logged": 0}
    _append_log({
        "at": _now(), "by": _s(user), "action": "MOVE",
        "approval_ids": [_s(x) for x in (approval_ids or [])],
        "sheet": _s(sheet), "plan_file": _s(plan_file),
        "moves": rows,
        "compare": [c for c in (compare or []) if isinstance(c, dict)],
        "detail": "ย้ายงานตามการติ๊ก " + " ; ".join(
            f"{_s(m.get('item'))} W{_s(m.get('from_week'))}→W{_s(m.get('to_week'))}"
            for m in rows[:12]) + (" …" if len(rows) > 12 else ""),
    })
    return {"ok": True, "logged": len(rows)}


def recent_moves(limit: int = 100) -> list:
    """งานที่ถูกย้ายหลังติ๊ก แตกเป็นรายก้อน (ใหม่สุดก่อน) = "แผนเก่า → แผนใหม่"

    ตอบคำถาม "ติ๊กเปลี่ยน Cylinder ไปแล้ว แผนของ item นั้นเปลี่ยนจากอะไรเป็นอะไร"
    โดยไม่ต้องเปิด log ดิบ — ข้อมูลมาจากเหตุการณ์ MOVE ที่หน้าเว็บส่งมาหลังคำนวณแผนใหม่เสร็จ

    ⚠️ นี่คือสิ่งที่ "ระบบย้ายให้ตอนติ๊ก" ไม่ใช่สิ่งที่อยู่ในไฟล์แผนแล้ว — การย้ายจะลงไฟล์จริง
    ต่อเมื่อ user กด "บันทึกแผน" (หน้าเว็บมีข้อความกำกับไว้)
    """
    cap = max(1, int(limit or 100))
    out = []
    for ev in read_log(_LOG_READ_MAX):        # read_log คืนใหม่สุดก่อนอยู่แล้ว
        if _s(ev.get("action")).upper() != "MOVE":
            continue
        for m in (ev.get("moves") or []):
            if not isinstance(m, dict):
                continue
            frm, to = _int(m.get("from_week")), _int(m.get("to_week"))
            out.append({
                "at": _s(ev.get("at")), "by": _s(ev.get("by")),
                "sheet": _s(ev.get("sheet")), "plan_file": _s(ev.get("plan_file")),
                "approval_ids": [_s(x) for x in (ev.get("approval_ids") or [])],
                "item": _s(m.get("item")), "so": _s(m.get("so")),
                "from_week": frm, "to_week": to,
                # เร็วขึ้นกี่สัปดาห์ (บวก = ได้ผลิตเร็วขึ้น) — คิดที่นี่ที่เดียว หน้าเว็บแค่แสดง
                "gain_weeks": (frm - to) if (frm is not None and to is not None) else None,
                "qty": m.get("qty"), "mc": m.get("mc"),
            })
            if len(out) >= cap:
                return out
    return out


def recent_compares(limit: int = 10) -> list:
    """ตารางเทียบ "แผนก่อนติ๊ก → แผนหลัง Change Cylinder" ของการติ๊กล่าสุด (ใหม่สุดก่อน)

    1 รายการ = การกด "ใช้ตามที่ติ๊ก" 1 ครั้ง พร้อมยอดรายสัปดาห์ของทุก item ที่แผนเปลี่ยน
    คืนเฉพาะครั้งที่หน้าเว็บส่ง compare มาด้วย (ครั้งเก่าก่อนมีฟีเจอร์นี้จะไม่มี)
    """
    cap = max(1, int(limit or 10))
    out = []
    for ev in read_log(_LOG_READ_MAX):
        if _s(ev.get("action")).upper() != "MOVE":
            continue
        rows = [c for c in (ev.get("compare") or []) if isinstance(c, dict)]
        if not rows:
            continue
        # สัปดาห์ทั้งหมดที่มีของ (ก่อนหรือหลัง) — หน้าเว็บใช้เป็นคอลัมน์ของตาราง
        weeks = set()
        for c in rows:
            for bag in (c.get("before") or {}, c.get("after") or {}):
                for w in bag:
                    n = _int(w)
                    if n is not None:
                        weeks.add(n)
        out.append({
            "at": _s(ev.get("at")), "by": _s(ev.get("by")),
            "sheet": _s(ev.get("sheet")), "plan_file": _s(ev.get("plan_file")),
            "approval_ids": [_s(x) for x in (ev.get("approval_ids") or [])],
            "weeks": sorted(weeks),
            "items": rows,
            "moves_n": len(ev.get("moves") or []),
        })
        if len(out) >= cap:
            break
    return out


# ---------------- เดลต้าเครื่อง (ให้ plan_view / Planning ใช้) ----------------
_CAL_CACHE = {"key": None, "df": None}


def _calendar_weekly():
    """ตารางสัปดาห์จริงจาก Calendar.xlsx (YEAR/WEEK เรียงตามเวลา) — ใช้กางช่วง "ตั้งแต่สัปดาห์นี้
    เป็นต้นไป" ให้ถูกเมื่อ horizon คร่อมปี ; อ่านไม่ได้ = None (ถอยไปเทียบเลขสัปดาห์ตรง ๆ)

    cache ตาม (path, mtime) เพราะ ava_by_week เรียกทุก request และการอ่าน Calendar.xlsx
    บน SharePoint ช้ากว่าการคำนวณทั้งก้อนหลายเท่า"""
    cal = config.master_files().get("Calendar")
    if cal is None or not Path(cal).exists():
        return None
    try:
        ck = (str(cal), Path(cal).stat().st_mtime)
    except OSError:
        return None
    if _CAL_CACHE["key"] == ck:
        return _CAL_CACHE["df"]
    try:
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from Calendar import calendar_week_map, load_calendar
        cw = calendar_week_map(load_calendar(cal, sheet_name="Sheet1"))
        cw.columns = [str(c).strip().upper() for c in cw.columns]
        df = cw if "WEEK" in cw.columns else None
    except Exception as e:
        print(f"[CYL APPROVE] อ่านปฏิทินไม่ได้ ({e}) → ใช้การเทียบเลขสัปดาห์ตรง ๆ แทน")
        df = None
    _CAL_CACHE.update(key=ck, df=df)
    return df


def week_of_date(iso_date: str):
    """วัน 'YYYY-MM-DD' → เลขสัปดาห์ตามปฏิทินโรงงาน (ศุกร์–พฤหัส) ; อ่านไม่ได้ = None

    ใช้แปลงวันด้ายเข้า (PO_IN_DATE) เป็นสัปดาห์ เพื่อเลือกสัปดาห์ที่จะเปลี่ยน Cylinder รอไว้
    ไม่คำนวณสัปดาห์เอง — อ่านจากปฏิทินจริงชุดเดียวกับที่ timing() ใช้
    วันที่เลยปลายปฏิทิน = None (ไม่เดา) ; วันที่ก่อนต้นปฏิทิน = สัปดาห์แรกที่มี
    """
    s = _s(iso_date)
    if not s:
        return None
    cw = _calendar_weekly()
    if cw is None or not {"WEEK_START", "WEEK_END", "WEEK"} <= set(cw.columns):
        return None
    try:
        import pandas as pd
        d = pd.Timestamp(s).normalize()
    except Exception:
        return None
    hit = cw.index[(cw["WEEK_START"] <= d) & (cw["WEEK_END"] >= d)]
    if len(hit) > 0:
        return _int(cw.loc[hit[0], "WEEK"])
    if len(cw) and d < cw.iloc[0]["WEEK_START"]:
        return _int(cw.iloc[0]["WEEK"])       # ด้ายเข้าไปแล้วก่อนช่วงปฏิทิน = พร้อมตั้งแต่ต้น
    return None


def week_axis() -> list:
    """แกนสัปดาห์ตามเวลาจริง [(year, week), ...] — ต้องเป็นชุดเดียวกับที่ Planning ใช้นับ
    TODAY_IDX (นับทุกแถวในปฏิทิน รวมสัปดาห์ที่โรงปิดด้วย)"""
    cw = _calendar_weekly()
    if cw is None:
        return []
    out = []
    has_year = "YEAR" in cw.columns
    for _i, r in cw.iterrows():
        wk = _int(r.get("WEEK"))
        if wk is None:
            continue
        out.append((_int(r.get("YEAR")) if has_year else None, wk))
    return out


def _today_pos(axis: list):
    """ตำแหน่งของสัปดาห์ "วันนี้" บนแกน — เทียบจาก WEEK_START/WEEK_END ของปฏิทินจริง
    (นิยามสัปดาห์โรงงาน ศุกร์–พฤหัส อยู่ในปฏิทินอยู่แล้ว จึงไม่คำนวณเองซ้ำ)"""
    cw = _calendar_weekly()
    if cw is None or not axis:
        return None
    try:
        import pandas as pd
        today = pd.Timestamp.today().normalize()
    except Exception:
        return None
    if {"WEEK_START", "WEEK_END"} <= set(cw.columns):
        hit = cw.index[(cw["WEEK_START"] <= today) & (cw["WEEK_END"] >= today)]
        if len(hit) > 0:
            return int(cw.index.get_loc(hit[0]))
        before = cw.index[cw["WEEK_END"] < today]
        if len(before) > 0:
            return int(cw.index.get_loc(before[-1]))
    return None


def timing() -> dict:
    """กติกาเวลาของ Change Cylinder ที่หน้าเว็บกับ advisor ต้องใช้ชุดเดียวกัน

    earliest_set   = สัปดาห์แรกที่สั่งเปลี่ยน Cylinder ได้ = สัปดาห์ปัจจุบัน + "สัปดาห์ที่แตะไม่ได้"
                     (ค่าจากหน้าตั้งค่าระบบ = ค่าเดียวกับพื้น TODAY_IDX+FREEZE_WEEKS ที่ Planning
                      ใช้ทั้งการวางแผนและ _try_cylinder_change — ห้ามเขียนเลขไว้เอง)
    earliest_ready = สัปดาห์แรกที่เครื่องพร้อมผลิต = สัปดาห์ถัดจาก earliest_set บนแกนปฏิทิน
                     (ถัดไป 1 ช่องจริง ไม่ใช่ +1 เพราะสัปดาห์ที่โรงปิดต้องข้าม)
    """
    fw = freeze_weeks()
    axis = week_axis()
    pos = _today_pos(axis)
    if not axis or pos is None:
        # ไม่มีปฏิทิน → ถอยไปนิยามเดิม (ISO week ของวันนี้ + freeze) ให้ยังทำงานได้ ไม่ล้มทั้งหน้า
        from datetime import date, timedelta
        w = (date.today() + timedelta(days=3)).isocalendar()[1]
        return {"today_week": w, "earliest_set": w + fw, "earliest_ready": w + fw + 1,
                "axis_ok": False}
    def at(i):
        return axis[i] if 0 <= i < len(axis) else (None, None)
    _ty, tw = at(pos)
    sy, sw = at(pos + fw)
    ry, rw = at(pos + fw + 1)
    return {"today_week": tw, "earliest_set": sw, "earliest_set_year": sy,
            "earliest_ready": rw, "earliest_ready_year": ry, "axis_ok": True}


def set_week_of(ready_week, year=None):
    """สัปดาห์ที่ต้องลงมือเปลี่ยน Cylinder เพื่อให้เครื่องพร้อมใช้ที่ ready_week
    = ช่องก่อนหน้าบนแกนปฏิทินจริง (ข้ามสัปดาห์ที่โรงปิด) ; ไม่มีปฏิทิน = ready − 1"""
    ready = _int(ready_week)
    if ready is None:
        return None
    axis = week_axis()
    if not axis:
        return ready - 1
    for i, (y, w) in enumerate(axis):
        if w != ready:
            continue
        if year is not None and y is not None and y != _int(year):
            continue
        return axis[i - 1][1] if i > 0 else w - 1
    return ready - 1


def weeks_on_or_after(all_weeks, ready_week, year=None) -> set:
    """คีย์สัปดาห์ใน all_weeks ที่อยู่ "ตั้งแต่ ready_week เป็นต้นไป" ตามแกนปฏิทินจริง

    all_weeks = คีย์สัปดาห์ของหน้าเว็บ (plan_view.week_key: '40' / '12@2027') หรือเลขสัปดาห์เปล่า
    คืนค่าเป็นคีย์ชุดเดียวกับที่ส่งเข้ามา (ผู้เรียกเอาไปคีย์ต่อได้ตรง ๆ)
    ทำไมไม่เทียบ w >= ready ตรง ๆ: แผนคร่อมปี/ยาวหลายปีได้ ; เลขเปล่า = ครั้งที่ใกล้วันนี้สุด
    (กติกาเดียวกับ Calendar.WeekTimeline) ; ปฏิทินอ่านไม่ได้ค่อยถอยไปเทียบเลข
    """
    ready = _int(ready_week)
    if ready is None:
        return set()
    keys = [str(w) for w in all_weeks if _s(w)]

    cw = _calendar_weekly()
    tl = None
    if cw is not None and "YEAR" in cw.columns:
        try:
            if str(config.REPO_DIR) not in sys.path:
                sys.path.insert(0, str(config.REPO_DIR))
            from Calendar import WeekTimeline
            tl = WeekTimeline(cw)
        except Exception:
            tl = None

    def pos_of(k):
        s = str(k)
        if "@" in s:
            w, y = s.split("@", 1)
            return tl.idx(_int(w), _int(y))
        return tl.idx(_int(s))

    start = tl.idx(ready, _int(year)) if tl is not None else None
    if tl is None or start is None:
        out = set()
        for k in keys:
            n = _int(str(k).split("@", 1)[0])
            if n is not None and n >= ready:
                out.add(k)
        return out
    return {k for k in keys if (pos_of(k) is not None and pos_of(k) >= start)}


def machine_deltas(all_weeks) -> dict:
    """เดลต้าเครื่องต่อ (สัปดาห์, โรงงาน, กลุ่มเครื่อง, เกจ) จากทุกรายการที่อนุมัติไว้

    คืน {(week:int, factory, cat, gauge): delta}  (ต้นทางติดลบ / ปลายทางเป็นบวก)
    all_weeks = สัปดาห์ที่มีอยู่จริงในชุดข้อมูลปลายทาง (ava หรือ summary_mc)
    """
    out: dict = {}
    for r in _read_state():
        n = _int(r.get("machines"), 0) or 0
        if n <= 0:
            continue
        fac = _s(r.get("factory")).upper()
        cat = _s(r.get("cat")).upper()
        src = _gnorm(r.get("src_gauge"))
        tgt = _gnorm(r.get("tgt_gauge"))
        if not fac or not cat or not src or not tgt:
            continue
        for w in weeks_on_or_after(all_weeks, r.get("ready_week"), r.get("year")):
            out[(w, fac, cat, src)] = out.get((w, fac, cat, src), 0) - n
            out[(w, fac, cat, tgt)] = out.get((w, fac, cat, tgt), 0) + n
    return out

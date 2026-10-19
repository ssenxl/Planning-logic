"""
livesync.py — ทำให้ทุกคนที่เปิดเว็บอยู่เห็นข้อมูลชุดเดียวกัน

ปัญหาเดิม: ต่างคนต่างเปิดหน้าเดียวกัน แก้พร้อมกัน แล้ว
  1. เซฟชนกันกลางทาง → ไฟล์ Excel พังทั้งไฟล์
  2. เขียนทับกันเงียบ ๆ (ทุกการเซฟคือ "ล้างทั้งชีทแล้วเขียนใหม่") → งานของคนแรกหายโดยไม่มีใครรู้
  3. คนที่เปิดหน้าค้างไว้ไม่รู้ว่าคนอื่นแก้อะไรไปแล้ว → คุยกันคนละแผน

โมดูลนี้แก้ทั้งสามข้อด้วยของ 4 อย่าง:
  • file_lock(path)   — เข้าคิวการเขียนไฟล์เดียวกัน ไม่ให้สองคนเขียนซ้อน
  • atomic_save(path) — เขียนลงไฟล์ชั่วคราวก่อนแล้วค่อยสลับ ไฟล์จริงจึงไม่มีสถานะครึ่ง ๆ
  • bump(resource)    — เลื่อน "เลขรุ่น" ของข้อมูลชุดนั้น + จดว่าใครแก้ แล้วยิงข่าวให้ทุกจอ
  • subscribe()       — ช่องรับข่าวของหน้าเว็บแต่ละจอ (ใช้กับ SSE /api/events)

เลขรุ่น (rev) ใช้สองทาง:
  - หน้าเว็บส่งเลขรุ่นที่ตัวเองโหลดมาแนบตอนเซฟ → ถ้าไม่ตรงแปลว่ามีคนแก้แซง (ตอบ 409)
  - ข่าวที่ยิงออกไปพก rev ใหม่ไปด้วย → จออื่นรู้ว่าต้องโหลดใหม่

หมายเหตุ: ทะเบียนอยู่ในหน่วยความจำของโปรเซส — ใช้ได้เพราะ backend รันคอนเทนเนอร์เดียว
(uvicorn worker เดียว) ถ้าวันหลังขยายเป็นหลาย worker ต้องย้ายไปใช้ Redis แทน
"""
import json
import os
import queue
import shutil
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import config

# ล็อกข้ามโปรเซส (อยู่ที่ราก repo เพราะตัวรันแผนซึ่งเป็นคนละโปรเซสก็ต้องใช้ตัวเดียวกัน)
if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))
import file_guard  # noqa: E402

# ไฟล์บันทึกว่าใครแก้อะไรเมื่อไหร่ — อยู่ใน data_plan เพราะ mount เป็น volume (ไม่หายตอน deploy)
AUDIT_FILE = config.OUTPUT_DIR / "edit_log.jsonl"

# ทะเบียนเลขรุ่นล่าสุด — ต้องอยู่รอดข้ามการรีสตาร์ต
#
# ถ้าเก็บไว้ในหน่วยความจำอย่างเดียว พอ deploy รอบใหม่เลขรุ่นจะกลับไปเริ่มที่ 0 แล้วจอที่
# ยังเปิดค้างอยู่ (ถือเลขรุ่น 7) จะเซฟไม่ได้อีกเลย — ขึ้น "อีกคนหนึ่งเพิ่งแก้ข้อมูลนี้"
# ทั้งที่ไม่มีใครแก้ และติดค้างจนกว่าเจ้าตัวจะกด F5 เอง
STATE_FILE = config.OUTPUT_DIR / "livesync_state.json"

# รหัสรอบการทำงานของ server — เปลี่ยนทุกครั้งที่ start ใหม่
# หน้าเว็บใช้รู้ว่า "server เพิ่งเริ่มใหม่" แล้วโหลดข้อมูลสดมาแทนของที่ถือค้างไว้
EPOCH = datetime.now().strftime("%Y%m%d%H%M%S")

# ---------------------------------------------------------------- ชุดข้อมูล (resource)
# ชื่อชุดข้อมูลที่หน้าเว็บ subscribe ได้ — ใช้เป็นคีย์ของทะเบียนเลขรุ่น
# masters/<ไฟล์>/<ชีท> กับ plan/<ชีท> เป็นชื่อแบบไดนามิก ไม่ต้องขึ้นทะเบียนล่วงหน้า
PLAN_LOCK = "plan_lock"          # ชีท Lock_Plan (ล็อก/ปลดล็อกงานบนแผน)
PLAN_SHEET = "plan_sheet"        # ชีทแผนผลิต (ลาก Gantt / แก้ตาราง)
WORKDAY = "workday"              # แผงวันทำงาน (Calendar.xlsx)
PLAN_CONFIG = "plan_config"      # หน้าตั้งค่าระบบ
CYLINDER = "cylinder"            # ติ๊กอนุมัติเปลี่ยนสูบ
OUTSOURCE = "outsource"          # การแบ่งจ้างทอ
RUN = "run"                      # สถานะการรัน pipeline (รันจบ = ข้อมูลใหม่ทั้งชุด)
SCHEDULE = "schedule"            # รอบรันอัตโนมัติ (ตั้งค่าระบบ › ตั้งเวลารันอัตโนมัติ)


def masters_res(name: str, sheet: str = "") -> str:
    """ชื่อชุดข้อมูลของชีทใน Master — แยกรายชีท เพื่อให้คนแก้คนละชีทไม่ต้องโหลดใหม่ทั้งคู่"""
    return f"masters/{name}/{sheet}" if sheet else f"masters/{name}"


def plan_res(sheet: str = "") -> str:
    return f"plan/{sheet}" if sheet else PLAN_SHEET


# ---------------------------------------------------------------- ล็อกต่อไฟล์
_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()


def _lock_for(path) -> threading.RLock:
    key = str(Path(path).resolve()).lower()
    with _locks_guard:
        lk = _locks.get(key)
        if lk is None:
            lk = _locks[key] = threading.RLock()
        return lk


@contextmanager
def file_lock(path):
    """เข้าคิวก่อนแตะไฟล์นี้ — สองคนกดเซฟพร้อมกันจะทำทีละคน ไม่ทับกันกลางทาง

    endpoint ของ FastAPI ที่เป็น def ธรรมดาถูกรันใน threadpool จึงซ้อนกันได้จริง
    ครอบตั้งแต่ "อ่านไฟล์" ไม่ใช่แค่ตอนเขียน เพราะการเซฟคืออ่าน-แก้-เขียนทั้งก้อน

    ล็อกสองชั้น: ชั้นในโปรเซส (เร็ว, กันผู้ใช้สองคนบนเว็บ) + ล็อกไฟล์ (กันตัวรันแผน
    ที่เป็นคนละโปรเซสแต่เขียน MasterMC ชีทเดียวกัน — ดู file_guard.py)
    """
    lk = _lock_for(path)
    lk.acquire()
    try:
        with file_guard.lock(path):
            yield
    finally:
        lk.release()


# ---------------------------------------------------------------- เขียนแบบสลับไฟล์
# เขียนแบบสลับไฟล์ — ตัวเดียวกับที่ตัวรันแผนใช้ (file_guard.py) เพื่อให้พฤติกรรมตรงกันทั้งระบบ
#     with livesync.atomic_save(p) as tmp:
#         wb.save(tmp)
atomic_save = file_guard.atomic_save


def backup_throttled(path, every_minutes: int = 60, tag: str = "auto") -> str:
    """สำรองไฟล์ไว้ แต่ไม่เกิน 1 ครั้งต่อช่วงเวลาที่กำหนด → คืนชื่อไฟล์สำรอง (ว่าง = ข้ามรอบนี้)

    เดิมสำรองทุกครั้งที่เซฟ ซึ่งพอเปลี่ยนเป็นเซฟอัตโนมัติทุกการขยับแล้วจะได้ไฟล์ .bak
    เป็นพันไฟล์ต่อวันจนดิสก์เต็ม — ประวัติย้อนหลังละเอียดอยู่ใน edit_log.jsonl แทน

    ⚠️ ต้องนับเฉพาะไฟล์สำรอง "ของตัวเอง" (มี tag คั่น) — MasterMC.xlsx ถูกสำรองโดยหลาย
    ระบบที่คนละจังหวะกัน (ล็อกแผนวันละครั้ง .lockplan / ตั๋วโปรแกรม .autoprogram)
    ถ้านับรวมหมด พอระบบอื่นสำรองไปแล้ววันนี้ การสำรองก่อนที่ผู้ใช้แก้จะถูกข้ามไปทั้งวัน
    = ไม่มีไฟล์ "ก่อนแก้" ให้ย้อนเลย
    """
    path = Path(path)
    if not path.exists():
        return ""
    try:
        newest = 0.0
        for b in path.parent.glob(f"{path.name}.{tag}.*.bak"):
            newest = max(newest, b.stat().st_mtime)
        if newest and (time.time() - newest) < every_minutes * 60:
            return ""
        bak = path.with_suffix(path.suffix + f".{tag}.{datetime.now():%Y%m%d_%H%M%S}.bak")
        shutil.copy2(path, bak)
        return bak.name
    except OSError as e:
        print(f"[LIVESYNC] สำรอง {path.name} ไม่ได้: {e}")
        return ""


# ---------------------------------------------------------------- ทะเบียนเลขรุ่น + ข่าว
_state: dict[str, dict] = {}
_state_guard = threading.Lock()


def _load_state() -> None:
    """อ่านทะเบียนเลขรุ่นของรอบก่อนกลับมา (ไฟล์หาย/เสีย = เริ่มใหม่ ไม่ทำให้ server ล้ม)"""
    if not STATE_FILE.exists():
        return
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        for k, v in (data.get("revs") or {}).items():
            if isinstance(v, dict) and "rev" in v:
                _state[k] = v
        print(f"[LIVESYNC] อ่านทะเบียนเลขรุ่นเดิม {len(_state)} ชุดข้อมูล")
    except (OSError, json.JSONDecodeError) as e:
        print(f"[LIVESYNC] อ่าน {STATE_FILE.name} ไม่ได้ (เริ่มนับใหม่): {e}")


def _save_state() -> None:
    """เขียนทะเบียนลงไฟล์หลังทุกการแก้ — ไฟล์เล็ก เขียนเร็ว และต้องไม่ทำให้การเซฟของผู้ใช้ล้ม"""
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with file_guard.atomic_save(STATE_FILE) as tmp:
            tmp.write_text(json.dumps({"revs": _state}, ensure_ascii=False), encoding="utf-8")
    except OSError as e:
        print(f"[LIVESYNC] เขียน {STATE_FILE.name} ไม่ได้: {e}")
_subs: list[queue.SimpleQueue] = []
_subs_guard = threading.Lock()
_seq = 0        # เลขรันของข่าว ใช้ให้หน้าเว็บรู้ว่าพลาดข่าวไปหรือเปล่า


def rev(resource: str) -> int:
    """เลขรุ่นปัจจุบันของชุดข้อมูล (0 = ยังไม่มีใครแก้ตั้งแต่ server เริ่ม)"""
    with _state_guard:
        return int((_state.get(resource) or {}).get("rev", 0))


def info(resource: str) -> dict:
    with _state_guard:
        return dict(_state.get(resource) or {"rev": 0})


def snapshot() -> dict:
    """เลขรุ่นของทุกชุดข้อมูล — หน้าเว็บดึงตอนเปิดหน้า และใช้เป็นทางสำรองเวลา SSE ต่อไม่ได้"""
    with _state_guard:
        return {k: dict(v) for k, v in _state.items()}


def bump(resource: str, user: str = "", detail: str = "", changes: dict = None,
         **extra) -> dict:
    """ข้อมูลชุดนี้เพิ่งถูกแก้ — เลื่อนเลขรุ่น จดคนแก้ แล้วยิงข่าวให้ทุกจอที่เปิดอยู่

    เรียก "หลังเขียนไฟล์สำเร็จ" เท่านั้น ถ้าเรียกก่อนแล้วการเขียนล้ม จออื่นจะโหลด
    ข้อมูลเก่ามาแล้วเชื่อว่าเป็นของใหม่

    changes = {"lines": [...], "total": n} จาก edit_diff — ลงเฉพาะไฟล์ประวัติ (ไม่ส่งไปกับข่าว
    เพราะยาวได้เป็นร้อยบรรทัด) ; total = 0 แปลว่าเซฟค่าเดิมซ้ำ → เลื่อนเลขรุ่นแต่ไม่จดประวัติ
    None = ไม่ได้เทียบ (จดแค่ detail เหมือนเดิม)
    """
    global _seq
    now = datetime.now()
    with _state_guard:
        cur = _state.get(resource) or {"rev": 0}
        ent = {
            "rev": int(cur.get("rev", 0)) + 1,
            "by": str(user or ""),
            "at": now.isoformat(timespec="seconds"),
            "detail": str(detail or ""),
            **extra,
        }
        _state[resource] = ent
    _save_state()
    with _subs_guard:
        _seq += 1
        msg = {"seq": _seq, "resource": resource, **ent}
        dead = []
        for q in _subs:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            _subs.remove(q)
    if changes is None:
        _audit(resource, ent)
    elif changes.get("total"):
        _audit(resource, {**ent, "changes": list(changes.get("lines") or []),
                          "changes_total": int(changes["total"])})
    return dict(_state[resource])


def _audit(resource: str, ent: dict) -> None:
    """จดลงไฟล์ว่าใครแก้อะไรเมื่อไหร่ — เขียนต่อท้ายบรรทัดเดียว จึงไม่ชนกับใคร

    จำเป็นขึ้นมากตั้งแต่เปลี่ยนเป็นเซฟอัตโนมัติ เพราะไม่มีปุ่ม "บันทึก" ให้ผู้ใช้
    ยืนยันอีกแล้ว — ถ้ามีอะไรเพี้ยนต้องตามได้ว่าเกิดจากการขยับของใคร
    """
    try:
        AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with AUDIT_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"resource": resource, **ent}, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[LIVESYNC] เขียน {AUDIT_FILE.name} ไม่ได้: {e}")


def read_audit(limit: int = 200, resource: str = "") -> list:
    """เหตุการณ์ล่าสุดก่อน — หน้าเว็บใช้แสดง "ใครแก้อะไรไปบ้าง" ; limit <= 0 = ทั้งหมด"""
    if not AUDIT_FILE.exists():
        return []
    rows = []
    try:
        with AUDIT_FILE.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if resource and r.get("resource") != resource:
                    continue
                rows.append(r)
    except OSError as e:
        print(f"[LIVESYNC] อ่าน {AUDIT_FILE.name} ไม่ได้: {e}")
        return []
    return (rows[-limit:] if limit > 0 else rows)[::-1]


# ---------------------------------------------------------------- ช่องรับข่าวของแต่ละจอ
def subscribe() -> queue.SimpleQueue:
    q: queue.SimpleQueue = queue.SimpleQueue()
    with _subs_guard:
        _subs.append(q)
    return q


def unsubscribe(q) -> None:
    with _subs_guard:
        if q in _subs:
            _subs.remove(q)


def subscriber_count() -> int:
    with _subs_guard:
        return len(_subs)


# ---------------------------------------------------------------- ตรวจว่ามีใครแก้แซงไหม
class Conflict(Exception):
    """มีคนแก้ข้อมูลชุดนี้ไปแล้วหลังจากที่หน้าเว็บโหลดมา — ห้ามเขียนทับ"""

    def __init__(self, resource: str, expected, current: dict):
        self.resource = resource
        self.expected = expected
        self.current = current
        by = current.get("by") or "อีกคนหนึ่ง"
        at = str(current.get("at", ""))[11:16]
        super().__init__(f"{by} เพิ่งแก้ข้อมูลนี้ไปเมื่อ {at} น. — โหลดใหม่ก่อนบันทึกทับ")

    def as_dict(self) -> dict:
        return {
            "detail": str(self),
            "conflict": True,
            "resource": self.resource,
            "expected_rev": self.expected,
            "current": self.current,
        }


def check_rev(resource: str, base_rev) -> None:
    """ด่านกันเขียนทับ — base_rev คือเลขรุ่นที่หน้าเว็บโหลดไป (ไม่ส่งมา = ข้ามด่าน)

    ปกติจะไม่ค่อยได้ทำงาน เพราะทุกจอถูก push ให้อัปเดตอยู่แล้ว แต่ยังต้องมีไว้
    เผื่อจอที่เน็ตหลุด/แท็บถูกพักไว้ แล้วกลับมาเซฟทับด้วยข้อมูลเก่า
    """
    if base_rev in (None, "", -1):
        return
    try:
        base = int(base_rev)
    except (TypeError, ValueError):
        return
    cur = info(resource)
    if int(cur.get("rev", 0)) != base:
        raise Conflict(resource, base, cur)


_load_state()

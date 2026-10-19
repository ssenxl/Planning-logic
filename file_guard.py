"""
file_guard.py — เข้าคิวก่อนแก้ไฟล์เดียวกัน และเขียนแบบสลับไฟล์ (ใช้ได้ข้ามโปรเซส)

ทำไมต้องมีไฟล์นี้ทั้งที่ webapp มีล็อกของตัวเองแล้ว: ล็อกใน webapp เป็นล็อกของ "โปรเซส
เดียว" กันได้แค่ผู้ใช้สองคนที่ยิงเข้าเว็บ แต่ตัวรันแผน (run_all.py → program_lock.py /
Replan.py) เป็นคนละโปรเซส และมันก็เขียนชีท Lock_Plan ของ MasterMC.xlsx เหมือนกัน

    10:00  ตัวรันแผนเริ่มเขียนชีท Lock_Plan
    10:00  ผู้ใช้กดล็อกงานบนเว็บพอดี → เปิดไฟล์เดียวกันเขียนทับ
    ผลคือ  ล็อกฝั่งหนึ่งหายทั้งชุด หรือแย่กว่านั้นคือไฟล์ Excel เสียทั้งไฟล์

ล็อกที่นี่ใช้ "ไฟล์" เป็นตัวจอง (สร้างไฟล์ .lock แบบ exclusive) ซึ่งทุกโปรเซสบนเครื่อง
เดียวกันเห็นตรงกัน จึงกันได้ทั้งเว็บและตัวรันแผน

    with file_guard.lock(path):
        wb = load_workbook(path)
        ...
        with file_guard.atomic_save(path) as tmp:
            wb.save(tmp)
"""
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

# ถือล็อกนานเกินนี้ถือว่าโปรเซสนั้นตายไปแล้ว (ค้างไว้ไม่ปล่อย) → ยึดล็อกต่อ
# เขียน MasterMC ทั้งไฟล์ใช้เวลาไม่ถึง 5 วินาที ตั้ง 120 ไว้เผื่อไฟล์ใหญ่บนดิสก์ช้า
STALE_SECONDS = 120
# รอคิวนานสุดก่อนยอมแพ้ — ยาวกว่าเวลาเขียนไฟล์ปกติหลายเท่า แต่ไม่ค้างตลอดกาล
WAIT_SECONDS = 60


def _lock_path(path) -> Path:
    p = Path(path)
    return p.with_suffix(p.suffix + ".lock")


# นับชั้นการล็อก "ของเธรดตัวเอง" — ฟังก์ชันที่ถือล็อกอยู่แล้วเรียกฟังก์ชันที่ล็อกไฟล์เดิมซ้ำได้
# โดยไม่ค้างรอตัวเอง (ไฟล์ .lock ไม่มีคุณสมบัตินี้ ต้องนับเอง)
#
# ⚠️ ต้องนับแยกรายเธรด ไม่ใช่รายไฟล์: ถ้านับรวมทั้งโปรเซส เธรดที่สองจะเห็นว่า "มีคนถือ
# ล็อกไฟล์นี้อยู่แล้ว" แล้วเข้าใจผิดว่าเป็นการล็อกซ้อนของตัวเอง → ผ่านเข้าไปเขียนทับกันพอดี
_local = threading.local()


def _depth_of(key: str) -> int:
    return getattr(_local, "depth", {}).get(key, 0)


def _depth_add(key: str, n: int) -> None:
    d = getattr(_local, "depth", None)
    if d is None:
        d = _local.depth = {}
    d[key] = max(0, d.get(key, 0) + n)


@contextmanager
def lock(path, wait: float = WAIT_SECONDS):
    """เข้าคิวก่อนแตะไฟล์นี้ — ทุกโปรเซสที่ใช้ตัวนี้จะไม่เขียนทับกัน

    รอจนกว่าจะถึงคิว ; รอนานเกิน wait วินาทีแล้วยังไม่ได้ = โยน TimeoutError
    (ปล่อยให้ผู้เรียกตัดสินใจ ดีกว่าแอบเขียนทับตอนที่อีกฝ่ายยังเขียนไม่เสร็จ)
    """
    key = str(Path(path).resolve()).lower()
    n = _depth_of(key)
    _depth_add(key, 1)
    if n:                       # เธรดนี้ถือล็อกไฟล์นี้อยู่แล้ว (เรียกซ้อน) → ผ่านได้เลย
        try:
            yield
        finally:
            _depth_add(key, -1)
        return

    lp = _lock_path(path)
    # ไฟล์เป้าหมายอาจยังไม่มี และโฟลเดอร์ของมันก็อาจยังไม่ถูกสร้าง (ตั้งค่าครั้งแรกบนเครื่องใหม่
    # หรือคอนเทนเนอร์ที่เพิ่ง mount volume เปล่า) — ไม่สร้างให้ก่อน ตัวจองคิวจะสร้างไม่ได้
    # แล้วการบันทึกครั้งแรกของระบบนั้นจะพังทั้งที่ไม่มีอะไรผิด
    try:
        lp.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    fd = None
    deadline = time.time() + wait
    try:
        while True:
            try:
                fd = os.open(str(lp), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()} {time.time():.0f}".encode())
                break
            except FileExistsError:
                # เจ้าของล็อกตายคาไฟล์ไหม (เช่นโดน kill ระหว่างเขียน) → ยึดต่อ
                try:
                    age = time.time() - lp.stat().st_mtime
                except OSError:
                    age = 0
                if age > STALE_SECONDS:
                    try:
                        lp.unlink()
                        print(f"[FILE GUARD] ยึดล็อกค้างของ {lp.name} (ค้างมา {age:.0f} วินาที)")
                    except OSError:
                        pass
                    continue
                if time.time() > deadline:
                    raise TimeoutError(
                        f"รอแก้ไฟล์ {Path(path).name} นานเกิน {wait:.0f} วินาที "
                        f"— มีอีกคน/อีกงานกำลังเขียนอยู่ ลองใหม่อีกครั้ง")
                time.sleep(0.05)
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                lp.unlink()
            except OSError:
                pass
        _depth_add(key, -1)


@contextmanager
def atomic_save(path):
    """คืน path ชั่วคราวให้เขียน แล้วสลับเป็นไฟล์จริงเมื่อเขียนจบครบถ้วน

    เขียนทับไฟล์จริงตรง ๆ แล้วพังกลางทาง = ไฟล์เหลือครึ่งเดียว เปิดไม่ขึ้นทั้งไฟล์
    วิธีนี้ไฟล์จริงจะเป็นของเดิมจนวินาทีสุดท้ายที่สลับ (การสลับเป็นขั้นตอนเดียวของ filesystem)
    """
    path = Path(path)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}_{threading.get_ident()}")
    try:
        yield tmp
        if not tmp.exists():
            raise RuntimeError(f"atomic_save: ไม่มีไฟล์ชั่วคราวให้สลับ ({tmp.name})")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass

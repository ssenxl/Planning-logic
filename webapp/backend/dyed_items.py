"""dyed_items.py — รหัสผ้าที่ "ให้สีแล้ว" สำหรับระบายสีบล็อกบนหน้าแผน/Gantt

แยกออกมาจาก order_color_advisor.py ตอนถอดหน้า Order Color ทิ้ง (30 ก.ย. 2569)
— หน้านั้นเลิกใช้แล้ว แต่ส่วนนี้หน้าแผนยังใช้อยู่ จึงเก็บไว้เป็นไฟล์เล็กของตัวเอง

หน้าแผน/Gantt ใช้แยกสีบล็อกแค่ 2 แบบ: ให้สีแล้ว vs ยังไม่ให้สี
"""
import sys

import config


class _ColorOpenProxy:
    """เข้าถึง ColorOpen (โมดูล root ของ pipeline) — แหล่งเดียวของ "ยอดที่เปิดสีแล้ว"

    import แบบ lazy เพราะ backend รันจาก webapp/backend/ ต้องเติม REPO_DIR เข้า sys.path ก่อน
    (แพตเทิร์นเดียวกับ plan_view / workday / cyl_approve)
    ไฟล์ view หาย/โมดูลหาย → คืนค่าว่าง ไม่ raise เพราะหน้าแผนต้องเปิดได้เสมอ"""

    @staticmethod
    def _mod():
        try:
            if str(config.REPO_DIR) not in sys.path:
                sys.path.insert(0, str(config.REPO_DIR))
            import ColorOpen
            return ColorOpen
        except Exception:
            return None

    def color_item_codes(self) -> set:
        m = self._mod()
        return m.color_item_codes() if m else set()

    def source_info(self) -> dict:
        m = self._mod()
        return m.source_info() if m else {"name": None, "mtime": None}


_color_open = _ColorOpenProxy()


def dyed_item_codes() -> dict:
    """รหัส item ที่ "ให้สีแล้ว" = มี OPEN_COLOR_LOSS > 0 ใน view ยอดเปิดสีของใบ SC

    ⚠️ เกณฑ์เดียวกับ Planning เป๊ะ ๆ (user 27 ส.ค. 2569) — เดิมหน้านี้ดูจาก LOAD_DYE
    ของไฟล์ Order Color ซึ่งเป็นคนละนิยามกับตัววางแผน ทำให้บล็อกบน Gantt ขึ้นสี
    ทั้งที่ Planning ไม่ได้ถือว่าเป็นงานสี

    ยังไม่มีไฟล์ view → คืนลิสต์ว่าง (ทุกบล็อก = ยังไม่ให้สี) ไม่ raise เพราะหน้าแผนต้องเปิดได้"""
    info = _color_open.source_info()
    return {"name": info["name"], "mtime": info["mtime"],
            "codes": sorted(_color_open.color_item_codes())}

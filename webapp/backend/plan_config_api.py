"""
plan_config_api.py — สะพานระหว่างหน้า "ตั้งค่าระบบ" กับ plan_config.py ของ pipeline

อ่าน/เขียนไฟล์ config เดียวกับที่ Planning/AVA_MC/program_lock อ่านตอนรัน
→ แก้บนเว็บแล้วมีผลกับรอบรันถัดไปทันที ไม่ต้อง restart container

หมายเหตุเรื่อง process:
  webapp เป็น process ที่รันค้าง ส่วน pipeline รันเป็น subprocess ใหม่ทุกครั้ง
  หน้าเว็บจึงเห็นค่าใหม่ทันทีหลังบันทึก (plan_config อัปเดต cache ในตัวเอง)
  และรอบรันถัดไปอ่านไฟล์สด ๆ อยู่แล้ว
"""
import json
import sys
from pathlib import Path

import config
import livesync

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน)


def _meta() -> dict:
    """ข้อมูลหัวไฟล์ (แก้ล่าสุดเมื่อไหร่ โดยใคร) — ไฟล์ยังไม่มี = ยังไม่เคยแก้"""
    try:
        if plan_config.CONFIG_FILE.exists():
            data = json.loads(plan_config.CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {
                    "updated_at": data.get("updated_at", ""),
                    "updated_by": data.get("updated_by", ""),
                }
    except Exception:
        pass
    return {"updated_at": "", "updated_by": ""}


def read() -> dict:
    """ทะเบียนค่าทั้งหมด + ค่าปัจจุบัน + ข้อความอธิบาย → หน้าเว็บเอาไปแสดงได้ตรง ๆ
    ทุกอย่างที่คนอ่าน (ชื่อ/คำอธิบาย/ตัวอย่าง/ค่าที่ใช้อยู่) ประกอบมาจากที่นี่
    หน้าเว็บจึงไม่ต้องรู้จักชื่อตัวแปรหรือหน่วยของค่าใด ๆ เลย"""
    plan_config.reload()
    groups, changed = [], []
    for gname in plan_config.GROUPS:
        ginfo = plan_config.GROUP_BY_NAME.get(gname, {})
        items = []
        for s in plan_config.SPEC:
            if s["group"] != gname:
                continue
            key = s["key"]
            value = plan_config.get(key)
            is_default = value == s["default"]
            item = {
                "key": key,
                "label": s["label"],
                "help": s.get("help", ""),
                "example": s.get("example", ""),
                # bool: อธิบายว่าเปิด/ปิดแล้วเกิดอะไร — ผู้ใช้ตัดสินใจได้โดยไม่ต้องรู้เบื้องหลัง
                "on_means": s.get("on_means", ""),
                "off_means": s.get("off_means", ""),
                # ตัวเลข: ปรับขึ้น/ลงแล้วแผนเปลี่ยนทางไหน — ผู้ใช้ตัดสินใจได้ก่อนกด − +
                "more": s.get("more", ""),
                "less": s.get("less", ""),
                # ข้อที่เปลี่ยนแล้วมีผลแรง (เมลออกจริง / แผนใช้เครื่องเกินของจริง ฯลฯ)
                "warn": s.get("warn", ""),
                # ปกติไม่ต้องแตะ — หน้าเว็บพับไว้ใต้ "ดูเพิ่มเติม"
                "advanced": bool(s.get("advanced")),
                "type": s["type"],
                # รายการ/ตาราง: หน้าเว็บต้องรู้ชนิดช่องและคอลัมน์ ถึงจะทำช่องแก้ให้ถูกแบบ
                "item_type": s.get("item_type", "text"),
                "columns": s.get("columns", []),
                "default": s["default"],
                "value": value,
                "value_text": plan_config.show(s, value),
                "default_text": plan_config.show(s, s["default"]),
                "source": plan_config.source_of(key),   # env | file | default
                "min": s.get("min"),
                "max": s.get("max"),
                # ปุ่ม − + ขยับทีละเท่าไหร่ (ค่าที่เป็นทศนิยมเล็ก ๆ ขยับทีละ 1 ไม่ได้)
                "step": s.get("step", 1),
                "unit": s.get("unit", ""),
                "blank": bool(s.get("blank")),
                "is_default": is_default,
                "group": gname,
            }
            items.append(item)
            if not is_default:
                changed.append({
                    "key": key, "group": gname, "label": s["label"],
                    "value_text": item["value_text"], "default_text": item["default_text"],
                    "locked": item["source"] == "env",
                })
        groups.append({
            "name": gname,
            "icon": ginfo.get("icon", ""),
            "desc": ginfo.get("desc", ""),
            "items": items,
            "changed_count": sum(1 for it in items if not it["is_default"]),
        })

    return {
        "groups": groups,
        # สรุป "ตอนนี้ระบบทำงานต่างจากมาตรฐานตรงไหนบ้าง" — โชว์บนสุดของหน้า
        "changed": changed,
        "total": sum(len(g["items"]) for g in groups),
        # ค่าที่ถูกตั้งทับจากเซิร์ฟเวอร์ — แก้บนเว็บยังไม่มีผลจนกว่าจะเอาค่านั้นออก
        "env_locked": [s["key"] for s in plan_config.SPEC
                       if plan_config.source_of(s["key"]) == "env"],
        **_meta(),
    }


def write(values: dict, user: str = "") -> dict:
    """บันทึกค่าทั้งชุดจากหน้าเว็บ — คืนผลพร้อมรายการที่เปลี่ยนจริง (ไว้โชว์ให้ผู้ใช้เห็น)

    หน้านี้ยังกดปุ่ม "บันทึก" เองเหมือนเดิม (ไม่เซฟอัตโนมัติ) เพราะค่าตรงนี้เปลี่ยนที
    กระทบแผนทั้งใบ — แต่พอบันทึกแล้วต้องป่าวประกาศให้จออื่นเห็นค่าใหม่ทันที
    """
    with livesync.file_lock(plan_config.CONFIG_FILE):
        res = plan_config.save(values or {}, user=user)
        if not res.get("ok"):
            return res
        ent = livesync.bump(livesync.PLAN_CONFIG, user=user,
                            detail=f"แก้ค่าตั้งต้น {len(res.get('changed') or [])} ค่า")
    res["config"] = read()
    res["rev"] = ent["rev"]
    return res


# กลุ่มค่าของหน้า Monitor Yarn (ชื่อกลุ่มใน plan_config.SPEC)
YARN_GROUP = "Monitor Yarn"


def read_group(group: str) -> dict:
    """ค่าของกลุ่มเดียว — หน้าที่ดูแลค่าเฉพาะเรื่อง (เช่น Monitor Yarn) ไม่ต้องโหลดทะเบียนทั้งชุด
    items หน้าตาเดียวกับใน read() ; values = {key: ค่าที่ใช้อยู่} ให้หน้าที่แค่ "ใช้ค่า" หยิบได้ตรง ๆ"""
    cfg = read()
    g = next((x for x in cfg["groups"] if x["name"] == group), None) or {
        "name": group, "icon": "", "desc": "", "items": [], "changed_count": 0}
    return {
        **g,
        "values": {it["key"]: it["value"] for it in g["items"]},
        "updated_at": cfg["updated_at"],
        "updated_by": cfg["updated_by"],
    }


def write_group(group: str, values: dict, user: str = "") -> dict:
    """บันทึกเฉพาะค่าของกลุ่มเดียว — ค่าของกลุ่มอื่นในไฟล์คงเดิมทุกตัว

    ต้องมีทางนี้แยกจาก write() เพราะ plan_config.save() เขียนทับ "ทั้งชุด": key ที่ไม่ได้ส่งมา
    ถือว่ากลับไปใช้ค่ามาตรฐาน → หน้าที่ส่งมาแค่ค่าของตัวเองจะล้างค่าตั้งค่าอื่นทั้งหมดทิ้ง
    ที่นี่จึงอ่านค่าที่บันทึกไว้ทั้งชุดมาก่อน แล้ววางทับเฉพาะ key ของกลุ่มนี้ (แบบเดียวกับ reset())
    """
    keys = {s["key"] for s in plan_config.SPEC if s["group"] == group}
    mine = {k: v for k, v in (values or {}).items() if k in keys}
    with livesync.file_lock(plan_config.CONFIG_FILE):
        plan_config.reload()
        merged = dict(plan_config.load_file())
        merged.update(mine)
        res = plan_config.save(merged, user=user)
        if not res.get("ok"):
            return res
        ent = livesync.bump(livesync.PLAN_CONFIG, user=user,
                            detail=f"แก้ค่า {group} {len(res.get('changed') or [])} ค่า")
    res["group"] = read_group(group)
    res["rev"] = ent["rev"]
    return res


def reset(keys=None, user: str = "") -> dict:
    """คืนค่าเริ่มต้น — ส่ง keys มา = เฉพาะตัวนั้น ; ไม่ส่ง = ทั้งหมด
    ทำโดยเอา key ออกจากไฟล์ (ไฟล์เก็บเฉพาะ "สิ่งที่ตั้งเอง")"""
    plan_config.reload()
    current = dict(plan_config.load_file())
    if keys:
        for k in keys:
            current.pop(k, None)
    else:
        current = {}
    # ส่งค่าที่เหลือกลับเข้า save() → key ที่ถูกเอาออกจะกลับไปใช้ default
    with livesync.file_lock(plan_config.CONFIG_FILE):
        res = plan_config.save(current, user=user)
        if res.get("ok"):
            ent = livesync.bump(livesync.PLAN_CONFIG, user=user, detail="คืนค่าเริ่มต้น")
            res["rev"] = ent["rev"]
    if res.get("ok"):
        res["config"] = read()
    return res


def history(limit: int = 50) -> list:
    return plan_config.history(limit)


def run_summary_lines() -> list:
    """บรรทัดสรุปค่าที่ไม่ใช่ค่าเริ่มต้น — runner พิมพ์ลง log ทุกรอบรัน"""
    plan_config.reload()
    return plan_config.non_default_lines()

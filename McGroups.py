# -*- coding: utf-8 -*-
"""แหล่งเดียวของ "กลุ่มเครื่องที่ไม่ใช่เครื่องทอของเรา"

เดิมลิสต์ชื่อกลุ่มถูกพิมพ์ซ้ำ 3 ที่ (AVA_MC.EXCLUDE_MC_GROUP / Order.EXCLUDE_MC_GROUPS /
webapp/backend/workday.NON_KNIT_MC) — เพิ่มหรือลดที่เดียวแล้วลืมอีกสองที่ ตัวเลขจะเพี้ยนเงียบ ๆ

แบ่งเป็น 2 ชั้นตามว่า "อ่านจากข้อมูลจริงได้ไหม":

1. **จ้างทอ — อ่านจาก MasterMC ชีท "Master MC"** ไม่ต้องพิมพ์ชื่อไว้ในโค้ด
   เกณฑ์: Factory = OUTSOURCE **หรือ** ไม่มีจำนวนเครื่อง (Total MC ไม่ใช่ตัวเลข > 0)
   = กลุ่มที่ไม่มีเครื่องอยู่ในโรงเรา → ปัจจุบันได้ {"COMKN"}
   เพิ่มผู้รับจ้างในมาสเตอร์เมื่อไร ที่นี่รู้เองทันที

2. **ตรวจ/ตัด/ม้วน (CL, FQC, F-TSD)** — ไม่มีอยู่ในมาสเตอร์ไฟล์ไหนเลย จึง derive ไม่ได้
   ยังต้องเป็นลิสต์ แต่เก็บไว้ที่เดียวตรงนี้ ถ้าอยากให้หายไปจากโค้ดจริง ๆ
   ต้องเพิ่มแถวของกลุ่มพวกนี้ลง MasterMC (Total MC = 0) แล้วชั้นที่ 1 จะกินไปเอง

หมายเหตุสำหรับแถว "แผน": ตั้งแต่ 16 ก.ย. 2569 แถวงานจ้างทอในไฟล์แผนใช้ MC_GROUP = ชื่อผู้รับจ้าง
ไม่ใช่ COMKN แล้ว — การตัดสินว่าแถวไหนเป็นงานจ้างทอให้ใช้ธง S9_ROUTING/S9_SUPP
(Planning._is_outsource_row) ไม่ใช่ลิสต์ในไฟล์นี้ ; ไฟล์นี้ใช้กับข้อมูลต้นทาง (booking/order)
ที่ ERP ยังส่ง MC_GROUP = COMKN มาให้
"""
from __future__ import annotations

import configparser as _cp
import os
from pathlib import Path

import pandas as pd

import plan_config  # รายชื่อกลุ่มงานตัด/FQC ตั้งได้จากหน้า "ตั้งค่าระบบ"

# กลุ่มที่ไม่ใช่เครื่องทอและไม่มีอยู่ในมาสเตอร์ใด ๆ — derive ไม่ได้ จึงต้องระบุชื่อ
# (งานตัด/ม้วน กับงานตรวจผ้า FQC — ไม่ได้กินเครื่องทอของเรา)
# แก้รายชื่อได้ที่หน้า "ตั้งค่าระบบ" › กลุ่มงานตัด/ม้วน · กลุ่มงานตรวจผ้า
# เทียบแบบตัวพิมพ์ใหญ่เสมอ (ผู้เรียกทำ .str.upper() ก่อน isin)
CUT_GROUPS = frozenset(plan_config.get_list("KP_CUT_GROUPS", upper=True))
FQC_GROUPS = frozenset(plan_config.get_list("KP_FQC_GROUPS", upper=True))
NON_KNIT_EXTRA = frozenset(CUT_GROUPS | FQC_GROUPS)

_CACHE: dict = {}


def _norm(v) -> str:
    s = "" if v is None else str(v).strip()
    return "" if s.lower() in ("nan", "none") else s.upper()


def _master_mc_path(path=None):
    """path ของ MasterMC.xlsx — ส่งมาเองก็ได้ ไม่ส่ง = อ่านจาก config.ini [paths] master_mc"""
    if path:
        return Path(os.path.expandvars(str(path)))
    _ini = Path(__file__).with_name("config.ini")
    if not _ini.exists():
        return None
    # interpolation=None: config.ini ใช้ %USERPROFILE% ซึ่ง ConfigParser ปกติจะตีความเป็น %(…)s
    cfg = _cp.ConfigParser(interpolation=None)
    cfg.read(_ini, encoding="utf-8")
    try:
        return Path(os.path.expandvars(cfg["paths"]["master_mc"]))
    except (KeyError, TypeError):
        return None


def outsource_groups(master_path=None) -> frozenset:
    """กลุ่มเครื่องที่งานถูกส่งออกไปทอข้างนอก — อ่านจาก MasterMC ไม่ hard-code

    Factory = OUTSOURCE หรือไม่มีจำนวนเครื่อง (Total MC ไม่ใช่ตัวเลข > 0)
    อ่านไม่ได้ (ไม่มีไฟล์/ชีทเสีย) → คืน {"COMKN"} เป็นพื้น เพื่อไม่ให้ระบบเผลอนับงานจ้างทอ
    เป็นเครื่องในบ้านตอนมาสเตอร์มีปัญหา
    """
    p = _master_mc_path(master_path)
    key = str(p)
    if key in _CACHE:
        return _CACHE[key]
    found = set()
    try:
        df = pd.read_excel(p, sheet_name="Master MC")
        df.columns = df.columns.str.strip()
        df["_mc"] = df["MC"].map(_norm)
        df["_fac"] = df["Factory"].map(_norm)
        # ต้องรวมทุกเกจของกลุ่มก่อนตัดสิน — กลุ่มในบ้านมีแถวเกจที่เครื่องเป็น 0 ได้
        # (เช่น SKP 25G = 0 เครื่อง แต่ SKP ทั้งกลุ่มมี 60 เครื่อง — ห้ามนับเป็นจ้างทอ)
        df["_tot"] = pd.to_numeric(df.get("Total MC"), errors="coerce").fillna(0)
        for mc, grp in df[df["_mc"] != ""].groupby("_mc"):
            if (grp["_fac"] == "OUTSOURCE").all() or float(grp["_tot"].sum()) <= 0:
                found.add(mc)
    except Exception as e:  # ไฟล์หาย/ชีทเปลี่ยนชื่อ — อย่าปล่อยให้ทั้ง pipeline ล้ม
        print(f"[McGroups] อ่าน Master MC ไม่ได้ ({e}) — ใช้ค่าพื้น COMKN")
    if not found:
        found = {"COMKN"}
    _CACHE[key] = frozenset(found)
    return _CACHE[key]


def non_knit_groups(master_path=None) -> frozenset:
    """กลุ่มที่ไม่กินเครื่องทอของเรา = จ้างทอ (จากมาสเตอร์) + งานตรวจ/ตัด (ลิสต์เดียวข้างบน)"""
    return frozenset(outsource_groups(master_path) | NON_KNIT_EXTRA)


def is_outsource(group, master_path=None) -> bool:
    return _norm(group) in outsource_groups(master_path)


def is_non_knit(group, master_path=None) -> bool:
    return _norm(group) in non_knit_groups(master_path)

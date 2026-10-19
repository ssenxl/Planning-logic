"""
yarn_ava_live.py — ด้ายแต่ละตัว lot ไหน ยังเอาไปใช้ได้อีกกี่ kg
(เดิมเป็นหน้า Dashboard › Yarn Ava — ถอดหน้าออก 15 ต.ค. 2569 user ไม่ใช้แล้ว ; ตัวคิดยังอยู่ เพราะ
 YarnStockCover (แผนผลิต: ใช้ด้ายในสต๊อกแทนรอ PO IN) · yarn_plan (แผนด้าย) · yarn_lock (ล็อกด้าย) เรียกใช้)

    ใช้ได้ = สต๊อกในคลัง − ลูกเล็ก − ด้ายที่ใบ KP จองไว้แล้วแต่ยังไม่โอนออกจากคลัง − ด้ายที่ล็อกไว้ (SC / Item)

ล็อกให้ SC = ฝั่งด้ายกันด้ายของ lot ไว้ให้ SC + ผ้า (yarn_lock.py — user ขอ 15 ต.ค. 2569) กันเท่ายอดที่ล็อก
ส่วนที่เหลือของ lot ใบอื่นยังใช้ได้ ; แผน (YarnStockCover) ให้ใบอื่นยืมได้แค่ยอดใช้ได้หลังหักล็อก
ล็อกให้ Item = กันด้ายไว้ให้ทุกใบของผ้าตัวนั้น (yarn_item_lock.py — user ขอ 15 ต.ค. 2569 บ่าย) ระบุ lot หรือ "ทุก lot"
(แบ่งลง lot ที่ด้ายเข้าก่อน — _lock_any_lot)

ลูกเล็ก = **ทีละแถว** : แถวสต๊อกที่ YARN_WEIGHT (kg/ลูก ที่ view คิดมา — ตัวเดียวกับที่หน้า Stock Yarn ลงสีแดง)
ต่ำกว่า KP_YARN_WEIGHT_MIN (หน้า Monitor Yarn, 0.3) ไม่นับ kg ของแถวนั้น ; แถวอื่นใน lot เดียวกันยังใช้ได้
— user สั่งเปลี่ยนจาก "เฉลี่ยทั้ง lot" 15 ต.ค. 2569: แถวลูกเล็กเกือบทั้งหมดคือด้ายคืนจากใบ KP (PO_NO = เลข KP/KCK
เช่น 15.9 kg / 840 ลูก = หลอดเกือบเปล่า) ซึ่งดึงค่าเฉลี่ยของ lot ลง → ลูกเต็มจาก PO ซื้อติดไปด้วย 5,669 kg / 111 lot
(PC24/1R TRT-4X1-2608: YRK691626 285.9 kg 2.27 kg/ลูก ถูกตัดทั้งก้อน ; SPD20D AKT-308 ใช้ได้ 0 ทั้งที่ใบ KP โอนออกไป
2,546 kg ใน 1–14 ต.ค.) ส่วนหลอดเปล่าที่ปนอยู่ใน lot ที่ค่าเฉลี่ยผ่านกลับถูกนับว่าใช้ได้ 7,150 kg
lot ที่ตรงกติกา "ลูกค้าใช้ได้เฉพาะ lot" (หน้า Monitor Yarn) ยังนับในยอดใช้ได้ แต่บอกว่าสงวนให้ใคร
— user ยืนยัน 14 ต.ค. 2569: ด้ายผูก SO/BUYYER ใช้ร่วมได้ ยกเว้น lot ที่สงวนไว้ ; SC ที่ระบุ Stop-Yarn ("ไม่สั่งซื้อเส้นด้าย")
ไม่ได้กันด้ายในคลังไว้ จึงไม่หัก

**ด้ายถูกตัดออกจากสต๊อกตอน "โอนให้ใบ KP" (โอนไปเพชร) ไม่ใช่ตอนทอ** — user ยืนยัน 14 ต.ค. 2569 และตรงกับข้อมูล:
lot ที่ไม่ได้ทอเลยในช่วง 2→14 ต.ค. สต๊อกหายเท่ายอดที่โอนพอดี (NEL-X11306 ลด 1,245 kg / โอน 1,242.6 kg / ทอ 0) ;
lot ที่ทอไป 25,544 kg สต๊อกลดแค่ 7,200 kg ≈ ยอดที่โอนในช่วงนั้น
→ ใบ KP ที่โอนแล้ว ด้ายอยู่หน้าเครื่องแล้ว ไม่อยู่ในสต๊อก **ห้ามหัก "เหลือต้องใช้" ของหน้า Yarn Usage ซ้ำ**
→ ที่ต้องหักคือใบที่ออกแล้วแต่ยังไม่โอน ณ เวลาของไฟล์สต๊อก (TRANSFER_DATE ว่าง หรือโอนหลังเวลาสต๊อก — ไฟล์สต๊อก
  ดึงรอบ 06:00 ส่วนไฟล์ใบ KP กดดึงใหม่ระหว่างวันได้ ใบที่โอนหลังเวลาสต๊อกจึงยังมีด้ายอยู่ในตัวเลขสต๊อก)
ดีมานด์ booking ที่ยังไม่ออกใบ KP ไม่หัก (user เลือก 14 ต.ค. 2569)

แหล่งข้อมูล:
- สต๊อก = Yarn/view_yarn_aging_received_797.xlsx ผ่าน yarn_stock_live (ตัดแถว PO ท้าย -D/-T ตามหน้า Monitor Yarn
  เหมือนหน้า Stock Yarn) รวม TOTAL_WEIGHT ต่อ ITEM + LOT — ตรงกับมุมมอง "รวมต่อ LOT" ของหน้า Stock Yarn
- จองไว้ = Yarn/view_yarn_by_joknit.xlsx (View_Yarn_JoKnit.py) : YARN_PROD_KGS ต่อ ใบ × ด้าย × lot
  (ยอดที่โอนจริง = BOM + % สูญเสีย ; ว่าง → YARN_BOM_KG)
- ใบยังใช้งานอยู่ไหม = Booking/view_booking.xlsx — ใบที่ไม่เคยโอนมีใบเก่าค้างตั้งแต่ ม.ค. (14 ต.ค. 2569: 11 จาก 43 ใบ)
  ที่ไม่อยู่ใน booking แล้ว → ใบที่ยังไม่โอนนับว่าจองเมื่อ booking ยังเป็น Pending **หรือออกภายใน _NEW_KP_DAYS วัน** ;
  booking บอกปิดแล้ว (มีแต่แถว Closed) = ไม่นับ
  ⚠️ ห้ามใช้ "ออกหลังเวลาไฟล์ booking" แทนวันล่าสุด: view booking เป็นภาพที่ refresh ไว้ก่อน (เติมรอบละ ~70 นาที
  วันละไม่กี่รอบ) ไฟล์ที่เพิ่งดึงยังไม่มีใบที่ออกหลังรอบ refresh — 14 ต.ค. 2569 ไฟล์ 12:14 ไม่มีใบที่ออก 04:18–09:31
  ทั้ง 15 ใบ (52,167 kg) กติกาเดิมจึงตัดทิ้งเป็นใบค้าง ทำให้ "ใช้ได้" สูงเกินจริง

ใบที่ยังไม่โอนส่วนใหญ่ยังไม่ได้ระบุ lot (14 ต.ค. 2569: 44 จาก 70 แถว) → ลงแถว "ยังไม่ระบุ lot" ของด้ายนั้น หักได้แค่ยอดรวมของด้าย
จับคู่สต๊อกกับใบด้วย ชื่อด้าย + lot เสมอ — lot เบอร์เดียวอยู่ได้หลาย ITEM ในสต๊อก (NYT-0426-15P1SW อยู่ใต้ 14 ITEM)
"""
import threading
from datetime import datetime, timedelta

import config
import yarn_item_lock
import yarn_lock
import yarn_stock_live

_SPLIT = config.REPO_DIR / "Yarn" / "view_yarn_by_joknit.xlsx"
_SPLIT_SHEET = 0   # ชีทแรก — ไฟล์มีชีทเดียว (ชื่อชีท = ชื่อ view ไม่อ้างในโค้ด)
_BOOKING = config.REPO_DIR / "Booking" / "view_booking.xlsx"
_NEED_SP = ("KP_NO", "YARN_CODE", "YARN_LOT", "TRANSFER_DATE", "YARN_PROD_KGS", "YARN_BOM_KG", "CREATED_DATE")
# ใบที่ยังไม่โอนและไม่อยู่ใน booking: ออกภายในกี่วันยังนับว่าจอง (ใบใหม่ที่ view booking ยังไม่ refresh ถึง)
# ใบค้างที่ต้องตัดเป็นใบอายุหลายเดือน (ม.ค.–เม.ย.) ส่วนใบใหม่อายุไม่กี่ชั่วโมง → 14 วันแยกได้ชัด (วัด 14 ต.ค. 2569)
_NEW_KP_DAYS = 14
# ไม่มีไฟล์ booking ให้เช็ก → ถือว่าใบที่ยังไม่โอนและออกภายในกี่วันยังใช้งานอยู่
_FALLBACK_ACTIVE_DAYS = 60
# kg/ลูก ขั้นต่ำ — ค่าจริงมาจาก KP_YARN_WEIGHT_MIN หน้า Monitor Yarn ; ตัวนี้ใช้ตอนอ่านค่าไม่ได้ (ต้องตรง default ใน plan_config)
_CONE_MIN_DEFAULT = 0.3

_s = yarn_stock_live._s
_num = yarn_stock_live._num
_mtime = yarn_stock_live._mtime

_cache: dict = {}
_lock = threading.Lock()   # table() คิดทีละรอบ — ดู table


def _ts(v):
    """วันเวลาจากไฟล์ (Timestamp/datetime) → datetime ; ว่าง = None"""
    if v is None:
        return None
    try:
        import pandas as pd
        t = pd.Timestamp(v)
    except Exception:
        return None
    return None if pd.isna(t) else t.to_pydatetime()


def _booking_kps(notes: list):
    """เลข KP ใน booking → (ใบที่ยังมีแถว Pending, ใบทั้งหมดที่อยู่ใน booking) ; อ่านไม่ได้ = (None, None)"""
    import pandas as pd

    if _mtime(_BOOKING) is None:
        notes.append(f"ไม่มีไฟล์ {_BOOKING.name} — นับใบที่ยังไม่โอนว่าจองไว้เฉพาะที่ออกภายใน {_FALLBACK_ACTIVE_DAYS} วัน")
        return None, None
    try:
        bk = pd.read_excel(_BOOKING, usecols=lambda c: c in ("KP_NO", "STATUS"))
    except Exception as e:
        notes.append(f"อ่านไฟล์ {_BOOKING.name} ไม่ได้: {e}")
        return None, None
    if "KP_NO" not in bk.columns:
        notes.append(f"ไฟล์ {_BOOKING.name} ไม่มีคอลัมน์ KP_NO — นับใบที่ยังไม่โอนว่าจองไว้เฉพาะที่ออกภายใน {_FALLBACK_ACTIVE_DAYS} วัน")
        return None, None
    kp = bk["KP_NO"].map(lambda v: _s(v).upper())
    closed = (bk["STATUS"].astype(str).str.strip().str.lower() == "closed") if "STATUS" in bk.columns \
        else pd.Series(False, index=bk.index)
    return set(kp[~closed & (kp != "")]), set(kp[kp != ""])


def _reserved(stock_at: datetime, notes: list) -> tuple:
    """ด้ายที่ใบ KP จองไว้แต่ยังไม่โอน ณ เวลาไฟล์สต๊อก → ({(ด้าย, lot|None): {kg, kps: {KP: kg}}}, stats)"""
    import pandas as pd

    if _mtime(_SPLIT) is None:
        notes.append(f"ยังไม่มีไฟล์ {_SPLIT.name} — ยังไม่หักด้ายที่ใบ KP จองไว้ (กดปุ่ม “ดึงข้อมูลด้าย”)")
        return {}, {}
    try:
        try:
            df = pd.read_excel(_SPLIT, sheet_name=_SPLIT_SHEET)
        except ValueError:
            df = pd.read_excel(_SPLIT)
    except Exception as e:
        notes.append(f"อ่านไฟล์ {_SPLIT.name} ไม่ได้: {e}")
        return {}, {}
    miss = [c for c in _NEED_SP if c not in df.columns]
    if miss:
        # ไฟล์รุ่นก่อนเพิ่มคอลัมน์โอน (ดึงก่อน deploy) — หักไม่ได้ ดีกว่าหักผิด
        notes.append(f"ไฟล์ {_SPLIT.name} ยังเป็นรุ่นเก่า (ไม่มี {', '.join(miss)}) — ยังไม่หักด้ายที่ใบ KP จองไว้ "
                     f"กดปุ่ม “ดึงข้อมูลด้าย” ให้ได้ไฟล์รุ่นใหม่")
        return {}, {}

    pending, in_booking = _booking_kps(notes)
    now = datetime.now()
    new_cut = now - timedelta(days=_NEW_KP_DAYS)
    fallback_cut = now - timedelta(days=_FALLBACK_ACTIVE_DAYS)
    out: dict = {}
    seen = set()
    st = {"kp": set(), "after_stock": set(), "stale": set()}
    for r in df.to_dict("records"):
        kp, yarn = _s(r.get("KP_NO")).upper(), _s(r.get("YARN_CODE"))
        if not kp or not yarn:
            continue
        transfer, created = _ts(r.get("TRANSFER_DATE")), _ts(r.get("CREATED_DATE"))
        if transfer is not None and transfer <= stock_at:
            continue                        # โอนออกจากคลังก่อนเวลาสต๊อก → ไม่อยู่ในตัวเลขสต๊อกแล้ว
        if transfer is None:
            # ยังไม่โอน — ใบต้องยังใช้งานอยู่: booking Pending · หรือยังไม่อยู่ใน booking แต่ออกภายใน _NEW_KP_DAYS วัน
            # (view booking ยัง refresh ไม่ถึง) · booking มีแต่แถว Closed = ปิดแล้ว ไม่นับ
            if pending is None:
                ok = created is not None and created >= fallback_cut
            elif kp in pending:
                ok = True
            elif kp in in_booking:
                ok = False
            else:
                ok = created is not None and created >= new_cut
            if not ok:
                st["stale"].add(kp)
                continue
        else:
            st["after_stock"].add(kp)       # โอนหลังเวลาสต๊อก — ด้ายยังอยู่ในตัวเลขสต๊อก แต่เป็นของใบนี้แล้ว
        lot = _s(r.get("YARN_LOT")) or None
        kg = _num(r.get("YARN_PROD_KGS"))
        if kg is None:
            kg = _num(r.get("YARN_BOM_KG")) or 0.0
        # ไฟล์ดึงแบบ DISTINCT มาแล้ว — กันซ้ำอีกชั้นเผื่อไฟล์รุ่นอื่น (คีย์เต็มแถว ไม่ให้ทิ้งแถวที่ต่างกันจริง)
        k = (kp, _s(r.get("ITEM_CODE")), yarn, lot, _num(r.get("YARN_CONTAIN")), kg)
        if k in seen:
            continue
        seen.add(k)
        a = out.setdefault((yarn, lot), {"kg": 0.0, "kps": {}})
        a["kg"] += kg
        a["kps"][kp] = a["kps"].get(kp, 0.0) + kg
        st["kp"].add(kp)
    return out, {"kp_reserved": len(st["kp"]), "kp_after_stock": len(st["after_stock"]),
                 "kp_stale": len(st["stale"])}


def _lock_any_lot(ilocks: list, cells: dict, hide: tuple) -> None:
    """ล็อกด้ายให้ Item แบบไม่ระบุ lot → แบ่งลง lot ของด้าย + สีเดียวกัน ที่ด้ายเข้าก่อน (อายุมากสุด) ก่อน — แก้ cells ในที่
    ข้าม lot ลูกเล็กทั้ง lot / lot ที่สงวนให้ลูกค้า / lot ที่ใช้ได้ ≤ 0 ; ทำหลังล็อกที่ระบุ lot (ได้ที่เหลือ)
    กันได้ไม่เกิน รวม lot − ใบ KP ที่จองแต่ยังไม่ระบุ lot (ตรงกับยอดรวมที่แผนใช้ — YarnStockCover.usable_stock)"""
    if not ilocks:
        return
    import YarnStockCover
    lcolor, age, _note = YarnStockCover.lot_meta(hide)
    by_yarn: dict = {}
    nolot: dict = {}
    for (yarn, lot), c in cells.items():
        y = str(yarn).strip().upper()
        if lot is None:
            nolot[y] = nolot.get(y, 0.0) + c["res"]
        elif c["w"] is not None and not c["low"] and not c["own"]:
            by_yarn.setdefault(y, []).append((lot, c))
    for L in ilocks:
        y = L["yarn"]
        cand = [(lot, c) for lot, c in by_yarn.get(y, [])
                if (lcolor.get((y, lot), "") if lcolor else "") == L["color"]]
        cand.sort(key=lambda t: (-(age.get((y, t[0])) or 0), t[0]))
        need = min(L["rem"], max(sum(max(c["ava"], 0.0) for _l, c in cand) - nolot.get(y, 0.0), 0.0))
        for _lot, c in cand:
            if need <= 1e-9:
                break
            take = min(need, max(c["ava"], 0.0))
            if take <= 1e-9:
                continue
            need -= take
            c["ava"] -= take
            c["lk"] += take
            c["lke"] += take
            c["locks"].append({"id": L["id"], "kind": "item", "sc": None, "so": None, "fabric": L["fabric"],
                               "kg": round(take, 3), "want": round(take, 3)})


def table(hide_suffixes: tuple = (), cone_min: float = _CONE_MIN_DEFAULT, rules=None) -> dict:
    """hide_suffixes = ท้าย PO ที่ซ่อน · cone_min = kg/ลูก ขั้นต่ำ (KP_YARN_WEIGHT_MIN) · rules = KP_YARN_CUSTOMER_LOTS
    ทั้งสามค่ามาจากหน้า Monitor Yarn (server ส่งเข้ามา)
    คิดทีละรอบ : cache หมดอายุต้องอ่านไฟล์ใหม่ ~5 วิ — คำขอที่มาพร้อมกัน (แผนด้าย / ล็อกด้าย / Gantt) รอผลรอบเดียวกัน"""
    with _lock:
        return _table(hide_suffixes, cone_min, rules)


def _table(hide_suffixes: tuple, cone_min: float, rules) -> dict:
    import json

    stock = yarn_stock_live.table()
    src = {"stock": stock["source"],
           "split": {"file": _SPLIT.name, "mtime": _mtime(_SPLIT)},
           "booking": {"file": _BOOKING.name, "mtime": _mtime(_BOOKING)}}
    empty = {"source": src, "yarns": [], "rows": [], "stats": {}, "cone_min": cone_min}
    if stock["source"].get("mtime") is None or not stock["rows"]:
        return {**empty, "note": stock.get("note") or "ไม่มีข้อมูลสต๊อกด้าย"}

    key = (src["stock"]["mtime"], src["split"]["mtime"], src["booking"]["mtime"], tuple(hide_suffixes),
           cone_min, json.dumps(rules or [], sort_keys=True, ensure_ascii=False), yarn_item_lock.state_key())
    hit = _cache.get(key)
    if hit is not None:
        return hit

    stock_at = datetime.fromtimestamp(src["stock"]["mtime"])
    notes: list = []
    res, rst = _reserved(stock_at, notes)

    # สต๊อกต่อ (ด้าย, lot): TOTAL_WEIGHT + TOTAL_YARN — แถวที่ PO ลงท้ายตามที่ตั้งซ่อนไว้ไม่นับ
    # (หน้า Stock Yarn ก็ไม่แสดง / "ไม่เอาไปทำแผน")
    # ลูกเล็ก = ทีละแถว: YARN_WEIGHT ของแถว < ขั้นต่ำ → kg แถวนั้นใช้ไม่ได้ (sw/sc/sn = kg/ลูก/จำนวนแถว ที่เป็นลูกเล็ก)
    # ไฟล์ที่ไม่มีค่า YARN_WEIGHT ในแถว → คิด TOTAL_WEIGHT ÷ TOTAL_YARN เอง ; ไม่มีทั้งคู่ = ตัดสินไม่ได้ นับว่าใช้ได้
    cells: dict = {}
    for r in stock["rows"]:
        if r["w"] is None or yarn_stock_live._po_hidden(r["p"], tuple(hide_suffixes)):
            continue
        c = cells.setdefault((r["i"], r["l"]), {"w": 0.0, "cones": 0.0, "sw": 0.0, "sc": 0.0, "sn": 0,
                                                "res": 0.0, "kps": {}})
        c["w"] += r["w"]
        c["cones"] += r["y"] or 0.0
        yw = r["yw"] if r["yw"] is not None else (r["w"] / r["y"] if r["y"] else None)
        if yw is not None and yw < cone_min:
            c["sw"] += r["w"]
            c["sc"] += r["y"] or 0.0
            c["sn"] += 1
    for (yarn, lot), a in res.items():
        c = cells.setdefault((yarn, lot), {"w": None, "cones": None, "sw": 0.0, "sc": 0.0, "sn": 0,
                                           "res": 0.0, "kps": {}})   # w None = ไม่มีในสต๊อก
        c["res"] += a["kg"]
        for kp, kg in a["kps"].items():
            c["kps"][kp] = c["kps"].get(kp, 0.0) + kg

    # ใช้ได้ = TOTAL_WEIGHT − ลูกเล็ก − จองไว้ ; ติดลบ = ใบ KP จองเกินด้ายที่ใช้ได้ของ lot (โชว์ให้เห็น ไม่ปัดเป็น 0)
    # low = ทั้ง lot เป็นลูกเล็ก (ไม่มีลูกปกติเหลือเลย) — YarnStockCover ใช้ข้าม lot นี้
    for (yarn, lot), c in cells.items():
        c["low"] = c["w"] is not None and c["w"] > 0 and c["sw"] >= c["w"] - 1e-9
        c["ava"] = (c["w"] or 0.0) - c["sw"] - c["res"]
        c["own"] = yarn_stock_live.owners_of(rules, yarn, lot) if lot else []

    # ด้ายที่ฝั่งด้ายล็อกไว้ — หักต่อจาก ใช้ได้ = w − ลูกเล็ก − จองไว้ ; 2 แบบ:
    #   ล็อกให้ SC + ผ้า (yarn_lock.py ที่หน้าแผนด้าย) — ระบุ lot เสมอ
    #   ล็อกให้ Item ผ้า (yarn_item_lock.py ที่หน้า Monitor Yarn) — ระบุ lot หรือไม่ระบุ ("ทุก lot")
    # ล็อกที่ระบุ lot ทั้ง 2 แบบ: เรียงเวลารวมกัน ล็อกก่อนได้ก่อน กันได้ไม่เกินที่ lot มีให้จริง (ใช้ได้ ≥ 0) ;
    # lot ไม่มีในสต๊อก / ลูกเล็กทั้ง lot = กันไม่ได้ → ใช้ได้ของ lot ไม่ติดลบเพราะล็อก
    # YarnStockCover อ่าน locks[].kg (kind sc = ของใบ, item = ของทุกใบของผ้านั้น) เป็นด้ายของเจ้าของล็อก
    try:
        locks = yarn_lock.active_by_lot()
    except Exception as e:
        locks = {}
        notes.append(f"อ่านล็อกด้ายไม่ได้ ({e}) — ยังไม่หักยอดล็อก")
    try:
        ilocks = yarn_item_lock.active()
    except Exception as e:
        ilocks = []
        notes.append(f"อ่านล็อกด้ายให้ Item ไม่ได้ ({e}) — ยังไม่หักยอดล็อก Item")
    for L in ilocks:
        if L["lot"]:
            locks.setdefault((L["yarn"], L["lot"]), []).append({**L, "kind": "item"})
    for ls in locks.values():
        ls.sort(key=lambda d: (d["at"], d["id"]))
    for (yarn, lot), c in cells.items():
        c["lk"], c["lke"], c["locks"] = 0.0, 0.0, []
        ls = locks.get((str(yarn).strip().upper(), lot)) if lot else None
        if not ls:
            continue
        room = max(c["ava"], 0.0) if c["w"] is not None and not c["low"] else 0.0
        for L in ls:
            eff = min(L["rem"], room)
            room -= eff
            c["lk"] += L["rem"]
            c["lke"] += eff
            c["locks"].append({"id": L["id"], "kind": L.get("kind", "sc"), "sc": L.get("sc"), "so": L.get("so"),
                               "fabric": L["fabric"], "kg": round(eff, 3), "want": round(L["rem"], 3)})
        c["ava"] -= c["lke"]
    _lock_any_lot([L for L in ilocks if not L["lot"]], cells, tuple(hide_suffixes))

    yarns: dict = {}
    for (yarn, _lot), c in cells.items():
        y = yarns.setdefault(yarn, {"w": 0.0, "cones": 0.0, "sw": 0.0, "sc": 0.0, "sn": 0, "res": 0.0, "ava": 0.0,
                                    "lk": 0.0, "own": {}})
        y["w"] += c["w"] or 0.0
        y["cones"] += c["cones"] or 0.0
        y["sw"] += c["sw"]
        y["sc"] += c["sc"]
        y["sn"] += c["sn"]
        y["res"] += c["res"]
        y["ava"] += c["ava"]
        y["lk"] += c["lke"]
        # ยอดใช้ได้ที่อยู่ใน lot สงวน — ใช้ได้เฉพาะลูกค้านั้น
        for cu in c["own"]:
            y["own"][cu] = y["own"].get(cu, 0.0) + max(c["ava"], 0.0)
    names = sorted(yarns)
    gidx = {n: i for i, n in enumerate(names)}
    r3 = lambda v: None if v is None else round(v, 3)
    # lot เรียง A→Z ; แถว "ยังไม่ระบุ lot" (None) อยู่ท้ายด้ายของตัวเอง
    order = sorted(cells, key=lambda k: (gidx[k[0]], k[1] is None, k[1] or ""))
    in_stock = [c for c in cells.values() if c["w"] is not None]
    out = {
        "source": src,
        "stock_at": stock_at.isoformat(timespec="minutes"),
        "cone_min": cone_min,
        # own = {ลูกค้า: kg ใช้ได้ใน lot ที่สงวนให้} · small/small_cones/small_n = ลูกเล็ก kg/ลูก/จำนวนแถว
        "yarns": [{"name": n, "w": r3(yarns[n]["w"]), "cones": r3(yarns[n]["cones"]), "small": r3(yarns[n]["sw"]),
                   "small_cones": r3(yarns[n]["sc"]), "small_n": yarns[n]["sn"], "res": r3(yarns[n]["res"]),
                   "lk": r3(yarns[n]["lk"]),
                   "ava": r3(yarns[n]["ava"]), "own": {k: r3(v) for k, v in sorted(yarns[n]["own"].items())}}
                  for n in names],
        # l = lot (None = ใบที่ยังไม่ระบุ lot) · w/cones None = lot นี้ไม่มีในสต๊อก
        # small/small_cones/small_n = แถวลูกเล็กใน lot (kg/ลูก/จำนวนแถว) · low = ทั้ง lot เป็นลูกเล็ก
        # own = ลูกค้าที่ lot นี้สงวนให้ · kps = [[ใบ KP, kg]] ที่จองแถวนี้
        # lk = ด้ายที่ล็อกให้ SC (ยอดที่ lot กันให้ได้จริง) · lk_want = ยอดที่ล็อกขอไว้ (มากกว่า lk = lot ไม่พอ)
        # locks = [{id, kind (sc/item), sc, so, fabric, kg (กันได้จริง), want}] ล็อกก่อนอยู่ก่อน
        #         (ล็อก Item แบบ "ทุก lot" ตัวเดียวแตกได้หลาย lot — kg ของแต่ละ lot)
        "rows": [{"g": gidx[y], "l": l, "w": r3(c["w"]), "cones": r3(c["cones"]), "small": r3(c["sw"]),
                  "small_cones": r3(c["sc"]), "small_n": c["sn"],
                  "low": c["low"], "own": c["own"], "res": r3(c["res"]), "ava": r3(c["ava"]),
                  "lk": r3(c["lke"]), "lk_want": r3(c["lk"]), "locks": c["locks"],
                  "kps": [[kp, r3(kg)] for kp, kg in sorted(c["kps"].items())]}
                 for (y, l), c in ((k, cells[k]) for k in order)],
        "stats": {"lots": len(in_stock),
                  "stock": r3(sum(c["w"] for c in in_stock)),
                  "small_rows": sum(c["sn"] for c in in_stock),
                  "small_lots": sum(1 for c in in_stock if c["sn"]),
                  "small_kg": r3(sum(c["sw"] for c in in_stock)),
                  "res": r3(sum(y["res"] for y in yarns.values())),
                  "res_nolot": r3(sum(c["res"] for (y, l), c in cells.items() if l is None)),
                  "lock": r3(sum(y["lk"] for y in yarns.values())),
                  "lock_n": sum(len(c["locks"]) for c in cells.values()),
                  "ava": r3(sum(y["ava"] for y in yarns.values())),
                  **rst},
    }
    if notes:
        out["note"] = " · ".join(notes)
    _cache.clear()
    _cache[key] = out
    return out

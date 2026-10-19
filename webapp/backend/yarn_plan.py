"""
yarn_plan.py — หน้า 🧶 แผนด้าย (เดิมเป็นโหมดในหน้าแผนผลิต — แยกเป็นหน้าเอง 15 ต.ค.) : แผนทอใช้ด้ายอะไร สัปดาห์ไหน เท่าไหร่ มาจากไหน เหลือเท่าไหร่

user 15 ต.ค. 2569: "ควรเป็นแบบแผนทอที่เห็นเลยว่า week นี้เราจะมีการใช้เส้นด้ายอะไรบ้าง ใช้เท่าไหร่
                    ซึ่งตัวนี้ต้องมาจากการทำแผนของแผนทอ" — แล้วเลือก: เป็นโหมดในหน้าแผนผลิต ·
                    ช่อง = kg ที่ใช้ + ด้ายมาจากไหน + คงเหลือ · ล็อกด้ายจากหน้านี้ (คลิกช่อง → SC → lot)

kg ด้าย = ยอดผลิตของแถวแผน ÷ จำนวนบรรทัดที่แถวยุบไว้ × % ด้ายของบรรทัด × สูญเสีย
          (สูตรเดียวกับ YarnStockCover.plan_consumption — แผนไม่มียอดแยกต่อบรรทัด)
แหล่ง: ชีทแผนที่หน้าแผนผลิตเปิดอยู่ (ไฟล์ production_plan ล่าสุด — ลาก/แก้แผนแล้วเซฟ = ตัวเลขด้ายเปลี่ยนตาม)
       × Order/view_scfol_yarn.xlsx (บรรทัดใบ × ด้าย : %, สี TD, PO, วัน PO เข้า)

ที่มาของด้ายต่อแถว (ไล่ตามลำดับ):
  kp    = แถวที่ออกใบ KP แล้ว (PLAN_SOURCE = KP_KEEP) — ด้ายถูกจอง/โอนไปแล้ว ยอดใช้ได้หักไปก่อนแล้ว
  po    = บรรทัดนี้มี PO ของตัวเองที่ยังไม่เข้า และแผนไม่ได้ให้ยืมสต๊อก (YARN_STOCK ว่าง) — PO คลุมเอง
  lock  = ด้ายที่ฝั่งด้ายล็อกให้ SC + ผ้านี้ (yarn_lock.py) — กินล็อกก่อนตามลำดับสัปดาห์
  ilock = ด้ายที่ล็อกให้ Item ผ้านี้ (yarn_item_lock.py — หน้า Monitor Yarn) ทุกใบของผ้าใช้ร่วมกัน ตามลำดับสัปดาห์
          (สัปดาห์เดียวกันเรียงตามเลขใบ) ต่อจากล็อกของใบ
  stock = ที่เหลือ (รวมใบที่แผนให้ยืมสต๊อกแทนรอ PO — นับว่าใช้สต๊อกที่สัปดาห์ที่ทอ ไม่บวกคืนตอน PO เข้า: เข้มไว้ก่อน)
          = ดึงจากกองกลาง (ในคลัง + PO Pending ที่เข้ามาแล้ว) → หน้าเว็บแตกเป็น from_stk / from_po / gap
          + ใช้ lot ไหน / PO ไหน ต่อสัปดาห์และต่อใบ (_trace : 1 ใบ = 1 lot เท่าที่ทำได้ → PO เข้าก่อน ;
            ในสัปดาห์งานสีก่อน แล้ว FG)
PO Pending เข้า = PO ด้ายที่ยังค้างส่ง (Yarn/view_poyarn_830.xlsx) ตามสัปดาห์ที่ใช้ได้ (วันเข้า + lag ; เลยกำหนดแล้ว
          = สัปดาห์นี้ + lag) หักส่วนที่บรรทัดใบ "รอ PO ของตัวเอง" (ที่มา po) กันไว้แล้ว — ตัวเดียวกับ supply_cap ตอนวางแผน
คงเหลือ = ยอดใช้ได้ของด้าย (กองกลาง: สต๊อก − ลูกเล็ก − ใบ KP ที่จองแต่ยังไม่โอน − ล็อก : YarnStockCover.usable_stock)
          + PO Pending ที่เข้ามาสะสม − stock สะสม
          หน้าเว็บเรียก "สต๊อกคาดการณ์" — PO เข้าแล้วก็กลายเป็นสต๊อก (user 16 ต.ค. 2569)
          ด้ายที่ต้องคุม (ด้ายนำเข้า) ต้องไม่ติดลบเมื่อเปิด "วางแผนได้ไม่เกินด้ายที่มี" (user 15 ต.ค. 2569)
          ด้ายในประเทศ (internal = ไม่คุม ; src "Internal" นันยางผลิตเอง / "Local" ผู้ขายไทย — user 16 ต.ค. 2569)
          ติดลบ = ต้องสั่งเพิ่ม (หน้าเว็บลงสีส้ม ไม่ใช่สีแดง) — คีย์ (ด้าย, สี) ตาม YarnStockCover.exempt_keys
ด้าย TD แยกตามสี (ด้ายรหัสเดียวกันแต่ละ lot คนละสี) — คีย์ = (ด้าย, สี) ตาม YarnStockCover

แถวแผนที่หาบรรทัดใบไม่เจอ (เช่น YD-ORDERS ไม่มี SC_LINE_ID) ไม่รู้ว่าใช้ด้ายอะไร → นับรวมไว้ใน "unknown" ให้หน้าเว็บบอก
"""
import re
import sys
import threading

import config
import plan_view
import yarn_lock

if str(config.REPO_DIR) not in sys.path:
    sys.path.append(str(config.REPO_DIR))

_DEMAND = config.OUTPUT_DIR / "order_replan.xlsx"     # คู่ SC_NO / SO_NO ของรอบแผนล่าสุด (แปลงคีย์ล็อก)
_cache: dict = {}
_lock = threading.Lock()   # table() คิดทีละรอบ — ดู table


def _ysc():
    import YarnStockCover
    return YarnStockCover


def _mt(p):
    try:
        return p.stat().st_mtime
    except (OSError, AttributeError):
        return None


def _ylines():
    """ด้ายรายบรรทัดใบ (cache ตามเวลาไฟล์ + วันนี้ — PO 'ยังไม่เข้า' ขึ้นกับวันที่)"""
    import pandas as pd
    ysc = _ysc()
    key = (_mt(ysc.SCFOL_FILE), pd.Timestamp.today().normalize())
    hit = _cache.get("ylines")
    if hit and hit[0] == key:
        return hit[1]
    val = ysc.load_yarn_lines()
    _cache["ylines"] = (key, val)
    return val


def _po():
    """view PO ด้าย (cache ตามเวลาไฟล์)"""
    ysc = _ysc()
    key = _mt(ysc.PO_FILE)
    hit = _cache.get("po")
    if hit and hit[0] == key:
        return hit[1]
    val = ysc.load_po()
    _cache["po"] = (key, val)
    return val


def _demand_pairs():
    """SC_NO/SO_NO ของดีมานด์รอบล่าสุด — ให้ own_by_order แปลงคีย์ล็อก (เลข SC) เป็นคีย์แผน (SO_NO)"""
    import pandas as pd
    key = _mt(_DEMAND)
    hit = _cache.get("demand")
    if hit and hit[0] == key:
        return hit[1]
    val = None
    if key is not None:
        try:
            val = pd.read_excel(_DEMAND, usecols=lambda c: str(c).strip().upper() in ("SC_NO", "SO_NO"))
            val.columns = val.columns.str.strip().str.upper()
        except Exception as e:
            print(f"[YARN PLAN] อ่าน {_DEMAND.name} ไม่ได้: {e}")
    _cache["demand"] = (key, val)
    return val


def _plan(sheet):
    """ชีทแผน → DataFrame (cache ตามไฟล์ + เวลาไฟล์ + ชีท) ; ไม่มีไฟล์ = (None, None)"""
    import pandas as pd
    p = plan_view.latest_path()
    if p is None:
        return None, None
    sheet = sheet or "PLAN"
    key = (str(p), _mt(p), sheet)
    hit = _cache.get("plan")
    if hit and hit[0] == key:
        return hit[1], p
    df = pd.read_excel(p, sheet_name=sheet)
    df.columns = [str(c).strip().upper() for c in df.columns]
    _cache["plan"] = (key, df)
    return df, p


def _yw(d) -> int:
    """วันที่ → YYYYWW ตามนิยามสัปดาห์ศุกร์–พฤหัส (บวก 3 วันแล้วใช้ ISO week — ตรง Calendar.py)"""
    import pandas as pd
    y, w, _ = (pd.Timestamp(d) + pd.Timedelta(days=3)).isocalendar()
    return int(y) * 100 + int(w)


_LOT_RE = re.compile(r"lot (.+?) ([\d,]+(?:\.\d+)?) kg")
_SLACK = 1.0           # kg — ป้าย YARN_STOCK ปัด kg เป็นจำนวนเต็ม (ดู _trace)
_EPS = 1e-9


def _noted_lots(text) -> dict:
    """ป้าย "ใช้ด้ายสต๊อก: …" ของใบที่แผนให้ยืมสต๊อก (ช่อง YARN_STOCK — YarnStockCover.note_of) → {(ด้าย, สี): {lot: kg}}
    รูป: "<ด้าย[ สี X]> lot A 29 kg + lot B 23 kg (แทน PO 31/10/2026) · <ด้าย> lot C 14 kg"
    lot ที่ล็อกไว้ ("lot A (ล็อก) 29 kg") ไม่อยู่ในกองกลางอยู่แล้ว — ข้าม"""
    s = str(text or "").strip()
    if not s.startswith("ใช้ด้ายสต๊อก:"):
        return {}
    out: dict = {}
    for part in s.split(":", 1)[1].split(" · "):
        part = part.strip()
        i = part.find(" lot ")
        if i <= 0:
            continue
        y, _, c = part[:i].partition(" สี ")
        m = out.setdefault((y.strip().upper(), c.strip().upper()), {})
        for g in _LOT_RE.finditer(part[i + 1:]):
            lot = g.group(1).strip()
            if not lot.endswith(")"):
                m[lot] = m.get(lot, 0.0) + float(g.group(2).replace(",", ""))
    return {k: v for k, v in out.items() if v}


def _due_key(r) -> int:
    """กำหนดส่งของแถวแผน (FG) เป็น YYYYWW — FG_WEEK (202649) ; ไม่มี → เป้าทอ TARGET_YEAR + TARGET_KNIT ; ไม่มี = ท้ายคิว"""
    import pandas as pd
    fg = pd.to_numeric(r.get("FG_WEEK"), errors="coerce")
    if pd.notna(fg) and fg > 100000:
        return int(fg)
    y, w = pd.to_numeric(r.get("TARGET_YEAR"), errors="coerce"), pd.to_numeric(r.get("TARGET_KNIT"), errors="coerce")
    if pd.notna(y) and pd.notna(w) and 0 < w <= 53:
        return int(y) * 100 + int(w)
    return 999999


def _trace(base: float, lots: list, events: list, demand: list, noted: dict = None, order: list = None) -> dict:
    """ไล่ว่าแต่ละใบใช้ด้าย lot ไหน / PO ไหน (ด้าย 1 ตัว) — ของในคลังก่อน หมดแล้วค่อยกิน PO ที่เข้ามาแล้ว
    ที่ยังขาดเป็นหนี้ (PO ที่เข้าทีหลังจ่ายหนี้ก่อน) ; ทุกสัปดาห์ ในคลังเหลือ + PO เหลือ − หนี้ = คงเหลือ (bal) เสมอ
    (user 16 ต.ค. 2569: "stock มีแค่นี้ แต่ทำไมถึงใช้ได้มากกว่า stock ... มันก็ต้องไปใช้ของ pending หรือป่าว"
     แล้ว "ต้องบอกด้วยว่าเป็น lot อะไรใช้ lot ไหน ทั้งส่วนของ stock และ PO pending")

    คิด 2 ขั้น : (1) ยอดรายสัปดาห์ — ใบไหนใช้ในคลัง / PO / ไม่พอ เท่าไหร่ (ไม่ขึ้นกับว่าได้ lot ไหน)
                 (2) แจก lot ทั้งใบ (ทุกสัปดาห์ของใบรวมกัน) : 1 ใบ = 1 lot เท่าที่ทำได้ (YarnStockCover.pick_lots —
                     user 16 ต.ค. 2569 ; เดิม lot เก่าก่อนทีละสัปดาห์ → ใบเดียวได้เศษ lot เก่าหลาย lot / เปลี่ยน lot กลางใบ)
                     ใบที่แผนให้ยืมสต๊อกได้ lot ตามป้ายก่อน → ทุกใบตามลำดับ order : เติมจาก lot ที่ใบถืออยู่ก่อน แล้ว pick_lots
                     ใบหนึ่งใช้ lot ของตัวเองเก่าก่อน ไล่ตามสัปดาห์
    base   = ยอดใช้ได้ (usable) · lots = [{"lot", "kg"}] lot ในกองกลาง เก่าก่อน (usable_stock) — ตัดท้ายให้รวมเท่า base :
             ส่วนเกิน = ใบ KP ที่จองแต่ยังไม่ระบุ lot ไม่รู้ว่า lot ไหน ถือว่า lot ใหม่สุด ; lot ไม่ครบ base = lot "" (ไม่รู้ lot)
    events = [{"po", "kg", "wi"}] PO Pending ตามสัปดาห์ที่เข้า (เรียงตามวันเข้าแล้ว)
    demand = [[(ใบ, kg), ...] ต่อสัปดาห์] เรียงตามลำดับที่ได้ของก่อน (งานสีก่อน แล้ว FG — user เลือก 16 ต.ค. 2569)
    noted  = {ใบ: {lot: kg}} lot ที่แผนให้ใบยืมไว้แล้ว (ป้าย YARN_STOCK) — ใบนั้นได้ lot นี้ก่อน ให้ตรงกับป้ายบน Gantt
    order  = ลำดับใบที่ได้เลือก lot ก่อน (ไม่ส่ง = ตามลำดับที่ใช้ในคลังครั้งแรก)
    → {"from_stk", "from_po", "gap": [kg ต่อสัปดาห์], "lot_w", "po_w": [{lot / PO: kg} ต่อสัปดาห์],
       "by": {(สัปดาห์, ใบ): {"lots": {lot: kg}, "pos": {PO: kg}, "gap": kg}}, "pool": [(lot, kg)] ที่ประกอบเป็น base}"""
    n = len(demand)
    pool, room = [], max(base, 0.0)
    for d in lots:
        q = min(float(d["kg"]), room)
        if q > _EPS:
            pool.append({"lot": d["lot"], "kg": q})
            room -= q
    if room > 1e-6:
        pool.append({"lot": "", "kg": room})
    start = [(d["lot"], d["kg"]) for d in pool]
    stk = sum(d["kg"] for d in pool)     # ในคลังที่ยังเหลือ (ยอดรวม — lot แจกทีหลัง)
    use: dict = {}                       # ใบ → [[สัปดาห์, kg]] ที่ใช้ในคลัง เรียงตามสัปดาห์
    debt = max(-base, 0.0)
    arrived: list = []
    ev = sorted((e for e in events if e.get("wi") is not None), key=lambda e: e["wi"])
    j = 0
    out = {"from_stk": [0.0] * n, "from_po": [0.0] * n, "gap": [0.0] * n,
           "lot_w": [{} for _ in range(n)], "po_w": [{} for _ in range(n)], "by": {}, "pool": start}
    for wi in range(n):
        while j < len(ev) and ev[j]["wi"] <= wi:
            q = float(ev[j]["kg"])
            pay = min(debt, q)
            debt -= pay
            if q - pay > _EPS:
                arrived.append({"po": ev[j]["po"], "kg": q - pay})
            j += 1
        for o, need in demand[wi]:
            got = out["by"].setdefault((wi, o), {"lots": {}, "pos": {}, "gap": 0.0})
            rest = need
            q = min(rest, stk)
            if q > _EPS:                         # ในคลังก่อน
                stk -= q
                rest -= q
                out["from_stk"][wi] += q
                use.setdefault(o, []).append([wi, q])
            for e in arrived:                    # หมดคลัง → PO ที่เข้ามาแล้ว (เข้าก่อนใช้ก่อน)
                q = min(e["kg"], rest)
                if q > 1e-9:
                    e["kg"] -= q
                    rest -= q
                    got["pos"][e["po"]] = got["pos"].get(e["po"], 0.0) + q
                    out["po_w"][wi][e["po"]] = out["po_w"][wi].get(e["po"], 0.0) + q
                    out["from_po"][wi] += q
            if rest > _EPS:
                got["gap"] += rest
                out["gap"][wi] += rest
                debt += rest

    # (2) แจก lot ทั้งใบ
    pick = _ysc().pick_lots
    kgs = [d["kg"] for d in pool]
    at = {d["lot"]: i for i, d in enumerate(pool)}
    need = {o: sum(q for _, q in ws) for o, ws in use.items()}
    held: dict = {}                      # ใบ → {ตำแหน่ง lot: kg}

    def give(o, i, q):
        kgs[i] -= q
        need[o] -= q
        m = held.setdefault(o, {})
        m[i] = m.get(i, 0.0) + q

    seen = set(order or ())
    seq = [o for o in (order or []) if o in need] + [o for o in use if o not in seen]
    # ป้ายปัด kg เป็นจำนวนเต็ม (lot มี 13.3 ป้ายเขียน 13) — เศษ lot ที่เหลือจากป้าย < _SLACK ยกให้ใบเจ้าของป้าย
    # ไม่งั้นใบอื่นได้เศษ 0.1–0.5 kg ของ lot เก่า (เจอกับข้อมูลจริง DTY75/72/1 16 ต.ค. 2569)
    for o in seq:
        for lot, kg in ((noted or {}).get(o) or {}).items():
            i = at.get(lot)
            if i is not None:
                q = min(kgs[i] if kgs[i] - kg < _SLACK else kg, kgs[i], need[o])
                if q > _EPS:
                    give(o, i, q)
    for o in seq:
        for i in sorted(held.get(o, {})):    # ส่วนที่ป้ายไม่พอ → เติมจาก lot ที่ถืออยู่ก่อน
            q = min(kgs[i], need[o])
            if q > _EPS:
                give(o, i, q)
        if need[o] > _EPS:
            for i, q in pick(kgs, need[o]):
                give(o, i, q)
    # lot ของใบ (เก่าก่อน) ไล่ลงสัปดาห์ที่ใบใช้ในคลัง
    for o, ws in use.items():
        mine = [[i, q] for i, q in sorted(held.get(o, {}).items())]
        k = 0
        for wi, q in ws:
            got = out["by"][(wi, o)]["lots"]
            while q > _EPS and k < len(mine):
                t = min(q, mine[k][1])
                lot = pool[mine[k][0]]["lot"]
                got[lot] = got.get(lot, 0.0) + t
                out["lot_w"][wi][lot] = out["lot_w"][wi].get(lot, 0.0) + t
                mine[k][1] -= t
                q -= t
                if mine[k][1] <= _EPS:
                    k += 1
    return out


def build(plan, ylines: dict, usable: dict, own_kg: dict, po=None, today=None, lag: int = 1,
          internal: dict = None, item_kg: dict = None, lots: dict = None, own_lots: dict = None,
          item_lots: dict = None) -> dict:
    """คิดแผนด้ายจากแถวแผน (ฟังก์ชันล้วน — เทสต์ที่ tests/test_yarn_plan.py)

    plan   = DataFrame ชีทแผน (คอลัมน์ตัวพิมพ์ใหญ่) · ylines = YarnStockCover.load_yarn_lines()
    usable = กองกลางต่อ (ด้าย, สี) (YarnStockCover.usable_stock ตัวที่ 2) · own_kg = {(SO, ผ้า): {(ด้าย, สี): kg ล็อก}}
    po     = YarnStockCover.load_po() (ไม่ส่ง = ไม่มี PO Pending)
    internal = ด้ายที่ไม่คุม {(ด้าย, สี): "Internal" | "Local"} (YarnStockCover.exempt_keys)
    item_kg = ล็อกให้ Item {ผ้า: {(ด้าย, สี): kg}} (usable_stock ตัวที่ 6)
    lots   = lot ในกองกลาง {(ด้าย, สี): [{"lot", "kg", "age"}]} เก่าก่อน (usable_stock ตัวที่ 1) — ไม่ส่ง = ไม่รู้ lot
    own_lots / item_lots = lot ที่ล็อกให้ใบ {(SO, ผ้า): {คีย์ด้าย: [{"lot", "kg"}]}} / ให้ Item {ผ้า: {...}} เก่าก่อน
             (ยอดรวมต้องตรง own_kg / item_kg) — ไม่ส่ง = ไม่รู้ว่าล็อก lot ไหน
    → {"weeks", "yarns", "rows", "orders", "unknown", "lock_use"}
      yarns[] มี lot_w / po_w (ใช้ lot / PO ไหนต่อสัปดาห์), lock_w / ilock_w (lot ที่ล็อกที่ใช้ต่อสัปดาห์),
      in_w (PO ที่เข้าต่อสัปดาห์), lots (lot ที่ประกอบเป็นยอดใช้ได้), pom {PO: [ผู้ขาย, วันกำหนดเข้า, เลยกำหนด]} ;
      rows[] มี lots / pos / gap / lock_lots / ilock_lots ของใบ + po_nos (PO ที่บรรทัดใบรอ)
      lock_use = แผนใช้ล็อกเท่าไหร่ต่อสัปดาห์ {"sc": {"SO|ผ้า|ด้าย|สี": [kg]}, "item": {"ผ้า|ด้าย|สี": [kg]}} (หน้า Monitor Yarn)"""
    import pandas as pd

    ysc = _ysc()
    txt, so_of = ysc._txt, ysc._so
    item_kg = item_kg or {}
    internal = internal or {}
    need = {"PRODUCE_QTY", "PLAN_WEEK", "PLAN_YEAR", "SC_LINE_ID", "SC_SO_NO", "ITEM_CODE"}
    empty = {"weeks": [], "yarns": [], "rows": [], "orders": {}, "unknown": {"rows": 0, "kg": 0.0, "types": {}},
             "lock_use": {"sc": {}, "item": {}}}
    if plan is None or plan.empty or not need <= set(plan.columns):
        return empty
    p = plan.copy()
    p["_Q"] = pd.to_numeric(p["PRODUCE_QTY"], errors="coerce").fillna(0)
    p["_W"] = pd.to_numeric(p["PLAN_WEEK"], errors="coerce")
    p["_Y"] = pd.to_numeric(p["PLAN_YEAR"], errors="coerce")
    p = p[(p["_Q"] > 0) & p["_W"].notna() & p["_Y"].notna()]
    if p.empty:
        return empty
    weeks = sorted({(int(y), int(w)) for y, w in zip(p["_Y"], p["_W"])})
    widx = {k: i for i, k in enumerate(weeks)}

    recs: dict = {}        # (คีย์ด้าย, สัปดาห์, SO, ผ้า) → {stock, lock, po, kp, po_in}
    cus: dict = {}
    reserve: list = []     # บรรทัดที่รอ PO ของตัวเอง → กัน PO นั้นไว้ (ไม่นับเป็น PO Pending ของกองกลาง)
    prio: dict = {}        # (SO, ผ้า) → [งานสี, FG] ลำดับที่ได้ lot ก่อนในสัปดาห์เดียวกัน
    noted: dict = {}       # (SO, ผ้า) → lot ที่แผนให้ยืมไว้ {(ด้าย, สี): {lot: kg}}
    unknown = {"rows": 0, "kg": 0.0, "types": {}}
    for r in p.to_dict("records"):
        wi = widx[(int(r["_Y"]), int(r["_W"]))]
        so, fab = so_of(r.get("SC_SO_NO")), txt(r.get("ITEM_CODE")).upper()
        src = txt(r.get("PLAN_SOURCE")).upper()
        borrowed = bool(txt(r.get("YARN_STOCK")))
        pr = prio.setdefault((so, fab), [False, 999999])
        pr[0] = pr[0] or txt(r.get("COLOR_PART")).upper() in ("YES", "Y", "TRUE", "1")
        pr[1] = min(pr[1], _due_key(r))
        if borrowed and (so, fab) not in noted:
            noted[(so, fab)] = _noted_lots(txt(r.get("YARN_STOCK")))
        lines = ysc.lines_of(r.get("SC_LINE_ID"))
        known = [ln for ln in lines if ln in ylines]
        share = r["_Q"] / len(lines) if lines else r["_Q"]
        miss = r["_Q"] - share * len(known)
        if miss > 1e-6:
            unknown["rows"] += 1
            unknown["kg"] += miss
            t = txt(r.get("ORDER_TYPE")) or "-"
            unknown["types"][t] = unknown["types"].get(t, 0.0) + miss
        if not known:
            continue
        if so and fab and (so, fab) not in cus:
            cus[(so, fab)] = txt(r.get("CUSTOMER"))
        for ln in known:
            for yk, info in ylines[ln].items():
                kg = share * info["pct"] / 100.0 * info["loss"]
                if kg <= 0:
                    continue
                a = recs.setdefault((yk, wi, so, fab), {"stock": 0.0, "lock": 0.0, "ilock": 0.0, "po": 0.0,
                                                        "kp": 0.0, "po_in": None, "po_nos": set(),
                                                        "lock_lots": {}, "ilock_lots": {}})
                if src == "KP_KEEP":
                    a["kp"] += kg
                elif info["po_in"] is not None and not borrowed:
                    a["po"] += kg
                    a["po_in"] = info["po_in"] if a["po_in"] is None else max(a["po_in"], info["po_in"])
                    a["po_nos"].update(str(x).upper() for x in info.get("pos") or [])
                    reserve.append((info.get("pos") or [], yk, kg))
                else:
                    a["stock"] += kg

    def eat(ls: list, q: float, into: dict) -> None:
        """กิน q kg จาก lot ที่ล็อก (เก่าก่อน — แบบ YarnStockCover._own_left) → จด lot: kg ลง into"""
        for d in ls:
            if q <= 1e-9:
                break
            t = min(d["kg"], q)
            if t > 1e-9:
                d["kg"] -= t
                q -= t
                into[d["lot"]] = into.get(d["lot"], 0.0) + t

    # ล็อกของใบ: กินก่อนตามลำดับสัปดาห์ (ส่วนที่ใบใช้จากสต๊อก) — lot ที่คนเลือกล็อกให้ใบ (ปุ่ม "ล็อก lot ให้ใบนี้")
    # โชว์ชื่อ lot ใต้แถว ล็อก SC (user 16 ต.ค. 2569: "คนต้องสามารถ config ได้ด้วย ว่าอยากให้ใช้ lot ไหน" → เลือกล็อกต่อ SC)
    by_order: dict = {}
    for (yk, wi, so, fab), a in recs.items():
        if a["stock"] > 0 and yk in (own_kg.get((so, fab)) or {}):
            by_order.setdefault((so, fab, yk), []).append((wi, a))
    for (so, fab, yk), lst in by_order.items():
        left = own_kg[(so, fab)][yk]
        ll = [dict(d) for d in ((own_lots or {}).get((so, fab)) or {}).get(yk, [])]
        for _wi, a in sorted(lst, key=lambda t: t[0]):
            take = min(left, a["stock"])
            a["lock"] += take
            a["stock"] -= take
            left -= take
            eat(ll, take, a["lock_lots"])
    # ล็อกของ Item: ทุกใบของผ้านั้นใช้ร่วมกัน กินตามลำดับสัปดาห์ (ส่วนที่ล็อกของใบไม่พอ)
    by_item: dict = {}
    for (yk, wi, so, fab), a in recs.items():
        if a["stock"] > 0 and yk in (item_kg.get(fab) or {}):
            by_item.setdefault((fab, yk), []).append((wi, so, a))
    for (fab, yk), lst in by_item.items():
        left = item_kg[fab][yk]
        ll = [dict(d) for d in ((item_lots or {}).get(fab) or {}).get(yk, [])]
        for _wi, _so, a in sorted(lst, key=lambda t: (t[0], t[1])):
            take = min(left, a["stock"])
            a["ilock"] += take
            a["stock"] -= take
            left -= take
            eat(ll, take, a["ilock_lots"])

    keys = sorted({k[0] for k in recs}, key=lambda k: (k[0], k[1]))
    yidx = {k: i for i, k in enumerate(keys)}
    n = len(weeks)
    agg = {k: {f: [0.0] * n for f in ("stock", "lock", "ilock", "po", "kp", "inbound")} for k in keys}
    lock_use = {"sc": {}, "item": {}}
    lk_w = {k: {f: [{} for _ in range(n)] for f in ("lock_lots", "ilock_lots")} for k in keys}
    for (yk, wi, so, fab), a in recs.items():
        for f in ("stock", "lock", "ilock", "po", "kp"):
            agg[yk][f][wi] += a[f]
        for f in ("lock_lots", "ilock_lots"):
            m = lk_w[yk][f][wi]
            for lot, q in a[f].items():
                m[lot] = m.get(lot, 0.0) + q
        if a["lock"] > 0:
            lock_use["sc"].setdefault(f"{so}|{fab}|{yk[0]}|{yk[1]}", [0.0] * n)[wi] += a["lock"]
        if a["ilock"] > 0:
            lock_use["item"].setdefault(f"{fab}|{yk[0]}|{yk[1]}", [0.0] * n)[wi] += a["ilock"]
    # PO Pending ของกองกลาง → สัปดาห์แรกของแผนที่ใช้ได้ (ก่อนสัปดาห์แรก = นับที่สัปดาห์แรก ; หลังสัปดาห์สุดท้าย = ไม่โชว์)
    today = today or pd.Timestamp.today().normalize()
    evs: dict = {}         # คีย์ด้าย → [{"po", "kg", "wi"}] เรียงตามวันเข้า
    if po is not None and not po.empty:
        wkeys = [y * 100 + w for y, w in weeks]
        for e in _ysc().pending_events(po, today, lag, set(), reserve):
            if e["yk"] not in agg:
                continue
            yw = _yw(e["avail"])
            wi = next((i for i, k in enumerate(wkeys) if k >= yw), None)
            if wi is not None:
                agg[e["yk"]]["inbound"][wi] += e["kg"]
                evs.setdefault(e["yk"], []).append({"po": e["po"], "kg": e["kg"], "wi": wi, "avail": e["avail"]})
    # ผู้ขาย + วันกำหนดเข้าของ PO ต่อด้าย (ป้ายบนหน้าเว็บ) — วันเข้าเร็วสุดของบรรทัดที่ยังค้าง
    po_info: dict = {}
    if po is not None and not po.empty:
        op = po[(po["STATUS"] == "OPEN") & (po["REMAIN"] > 0.5)]
        for r in op.itertuples(index=False):
            k2 = (r.PO, r.ITEM, r.COLOR or "")
            pin = r.PIN if pd.notna(r.PIN) else None
            old = po_info.get(k2)
            if old is None or (pin is not None and (old[1] is None or pin < old[1])):
                po_info[k2] = (r.SUPPLIER, pin)
    # ใบที่ดึงจากกองกลาง ต่อด้าย ต่อสัปดาห์ เรียงตามลำดับที่ได้ของก่อน: งานสีก่อน แล้ว FG (user เลือก 16 ต.ค. 2569)
    dem: dict = {}
    for (yk, wi, so, fab), a in recs.items():
        if a["stock"] > 1e-9:
            dem.setdefault(yk, [[] for _ in range(n)])[wi].append(((so, fab), a["stock"]))
    order_key = lambda t: (not prio.get(t[0], (False,))[0], prio.get(t[0], (False, 999999))[1], t[0])
    wait_nos: dict = {}    # คีย์ด้าย → PO ที่บรรทัดใบรออยู่
    for (yk, _wi, _so, _fab), a in recs.items():
        wait_nos.setdefault(yk, set()).update(a["po_nos"])
    r1 = lambda v: round(v, 1)
    listed = lambda m, rank=None: [[x, r1(v)] for x, v in (sorted(m.items(), key=lambda kv: rank.get(kv[0], 1e9))
                                                          if rank else m.items()) if v >= 0.05]
    lots = lots or {}
    yarns, traced, ranks = [], {}, {}
    for k in keys:
        g = agg[k]
        use = [g["stock"][i] + g["lock"][i] + g["ilock"][i] + g["po"][i] + g["kp"][i] for i in range(n)]
        base = usable.get(k)
        bal, run, short = [], 0.0, None
        for i in range(n):
            run += g["stock"][i] - g["inbound"][i]
            b = (base or 0.0) - run
            bal.append(r1(b))
            if short is None and b < -0.5:
                short = i
        dk = [sorted(ws, key=order_key) for ws in dem.get(k, [[] for _ in range(n)])]
        first: dict = {}
        for wi, ws in enumerate(dk):
            for o, _q in ws:
                first.setdefault(o, wi)
        # ใบไหนได้เลือก lot ก่อน: งานสีก่อน แล้ว FG แล้วสัปดาห์ที่เริ่มใช้ (ลำดับเดียวกับ Replan แบ่ง lot ให้ใบที่ยืมสต๊อก)
        seq = sorted(first, key=lambda o: (not prio.get(o, (False,))[0], prio.get(o, (False, 999999))[1], first[o], o))
        tr = _trace(base or 0.0, lots.get(k, []), sorted(evs.get(k, []), key=lambda e: (e["wi"], e["avail"], e["po"])),
                    dk, {o: ys[k] for o, ys in noted.items() if k in ys}, seq)
        traced[k] = tr["by"]
        # หน้าเว็บโชว์ lot เรียงเก่า → ใหม่ (ลำดับในกอง) ไม่ใช่ลำดับที่หยิบ
        rank = ranks[k] = {lot: i for i, (lot, _q) in enumerate(tr["pool"])}
        in_w = [{} for _ in range(n)]
        for e in evs.get(k, []):
            in_w[e["wi"]][e["po"]] = in_w[e["wi"]].get(e["po"], 0.0) + e["kg"]
        # ป้าย PO: ที่เข้ากองกลาง + ที่บรรทัดใบรอ
        pom = {}
        for x in sorted({e["po"] for e in evs.get(k, [])} | wait_nos.get(k, set())):
            sup, pin = po_info.get((x, k[0], k[1]), ("", None))
            pom[x] = [sup, f"{pin:%d/%m/%Y}" if pin is not None else "", bool(pin is not None and pin < today)]
        age = {d["lot"]: int(d.get("age") or 0) for d in lots.get(k, [])}
        yarns.append({"yarn": k[0], "color": k[1], "usable": None if base is None else r1(base),
                      "use": [r1(v) for v in use], "stock": [r1(v) for v in g["stock"]],
                      "from_stk": [r1(v) for v in tr["from_stk"]], "from_po": [r1(v) for v in tr["from_po"]],
                      "gap": [r1(v) for v in tr["gap"]],
                      "lot_w": [listed(m, rank) for m in tr["lot_w"]], "po_w": [listed(m) for m in tr["po_w"]],
                      "in_w": [listed(m) for m in in_w],
                      "lock_w": [listed(m) for m in lk_w[k]["lock_lots"]],
                      "ilock_w": [listed(m) for m in lk_w[k]["ilock_lots"]],
                      "lots": [[lot, r1(q), age.get(lot)] for lot, q in tr["pool"] if q >= 0.05], "pom": pom,
                      "lock": [r1(v) for v in g["lock"]], "ilock": [r1(v) for v in g["ilock"]],
                      "po": [r1(v) for v in g["po"]],
                      "kp": [r1(v) for v in g["kp"]], "inbound": [r1(v) for v in g["inbound"]],
                      "bal": bal, "short": short, "total": r1(sum(use)),
                      "internal": k in internal, "src": internal.get(k)})

    rows, orders = [], {}
    for (yk, wi, so, fab), a in recs.items():
        t = traced.get(yk, {}).get((wi, (so, fab))) or {"lots": {}, "pos": {}, "gap": 0.0}
        rows.append({"y": yidx[yk], "w": wi, "so": so, "fab": fab,
                     "stock": r1(a["stock"]), "lock": r1(a["lock"]), "ilock": r1(a["ilock"]), "po": r1(a["po"]),
                     "kp": r1(a["kp"]), "po_in": None if a["po_in"] is None else f"{a['po_in']:%d/%m/%Y}",
                     "lots": listed(t["lots"], ranks.get(yk)), "pos": listed(t["pos"]), "gap": r1(t["gap"]),
                     "lock_lots": listed(a["lock_lots"]), "ilock_lots": listed(a["ilock_lots"]),
                     "po_nos": sorted(a["po_nos"])})
        o = orders.setdefault(f"{so}|{fab}", {"cus": cus.get((so, fab), ""), "need": {}})
        # ยอดที่ยังล็อกได้ = ทุกอย่างที่ยังไม่ออกใบ KP (ทุกสัปดาห์)
        o["need"][yidx[yk]] = r1(o["need"].get(yidx[yk], 0.0) + a["stock"] + a["lock"] + a["ilock"] + a["po"])
    rows.sort(key=lambda d: (d["y"], d["w"], d["so"], d["fab"]))
    unknown["kg"] = r1(unknown["kg"])
    unknown["types"] = {t: r1(v) for t, v in sorted(unknown["types"].items(), key=lambda kv: -kv[1])}
    lock_use = {kind: {k: [r1(v) for v in ws] for k, ws in m.items()} for kind, m in lock_use.items()}
    return {"weeks": [{"y": y, "w": w} for y, w in weeks], "yarns": yarns, "rows": rows,
            "orders": orders, "unknown": unknown, "lock_use": lock_use}


def table(sheet: str = None) -> dict:
    """แผนด้ายของชีทแผนที่เปิดอยู่ — ดูหัวไฟล์
    คิดทีละรอบ : ไฟล์ต้นทางเพิ่งเปลี่ยนต้องอ่าน Excel ใหม่หลายวินาที — คำขอที่มาระหว่างนั้น (หน้าแผนด้าย / Monitor Yarn /
    server คิดรอไว้ _warm_yarn) รอแล้วได้ผลจาก cache ไม่อ่านไฟล์ซ้อนกันแย่ง CPU"""
    with _lock:
        return _table(sheet)


def _table(sheet: str = None) -> dict:
    ysc = _ysc()
    notes = []
    plan, path = _plan(sheet)
    if plan is None:
        return {"note": "ยังไม่มีไฟล์แผนผลิต", "weeks": [], "yarns": [], "rows": [], "orders": {}}
    ylines = _ylines()
    if not ylines:
        notes.append(f"ยังไม่มีไฟล์ {ysc.SCFOL_FILE.name} (View_SCFol_Yarn) — ไม่รู้ว่าแต่ละบรรทัดใบใช้ด้ายอะไร")
    detail: dict = {}
    try:
        _lots, usable, stock_at, warn, own, item = ysc.usable_stock(detail)
    except Exception as e:
        _lots, usable, stock_at, warn, own, item = {}, {}, None, f"อ่านสต๊อกด้ายไม่ได้ ({e})", {}, {}
    if warn:
        notes.append(warn)
    # คู่ SC_NO/SO_NO ใช้แปลงเลขใบของล็อก SC อย่างเดียว — ไม่มีล็อก SC = ไม่อ่าน (ผลเหมือนกัน ไม่มีอะไรให้แปลง)
    # order_replan.xlsx 6 หมื่นแถว × 55 คอลัมน์ อ่าน 17–24 วิ (วัดบน server 16 ต.ค. 2569) = ตัวหลักที่ทำให้หน้าเปิดช้า
    pairs = _demand_pairs() if own else None
    own_lots = ysc.own_by_order(own, pairs)
    own_kg = {k: {yk: sum(d["kg"] for d in ls) for yk, ls in ys.items()} for k, ys in own_lots.items()}
    item_kg = {f: {yk: sum(d["kg"] for d in ls) for yk, ls in ys.items()} for f, ys in item.items()}
    # PO Pending + ด้ายที่นันยางผลิตเอง — ค่าตั้งเดียวกับตอนวางแผน (หน้า Monitor Yarn)
    import pandas as pd
    import plan_config
    po = _po()
    if po.empty:
        notes.append(f"ยังไม่มีไฟล์ {ysc.PO_FILE.name} (View_PO_Yarn) — ยังไม่นับ PO Pending ในคงเหลือ")
    today = pd.Timestamp.today().normalize()
    lag = int(plan_config.get("KP_YARN_POIN_LAG_WEEKS") or 0) if plan_config.get("KP_YARN_POIN_ON") else 0
    internal = ysc.exempt_keys(po, plan_config.get("KP_YARN_SUPPLY_EXEMPT") or [],
                               bool(plan_config.get("KP_YARN_SUPPLY_EXEMPT_LOCAL")), today,
                               keys={yk for ys in ylines.values() for yk in ys})
    out = build(plan, ylines, usable, own_kg, po=po, today=today, lag=lag, internal=internal, item_kg=item_kg,
                lots=_lots, own_lots=own_lots, item_lots=item)
    # ยอดใช้ได้มาจากไหน (ช่อง "สต๊อกใช้ได้") : ในคลัง − ลูกเล็ก − สงวนลูกค้า − ใบ KP จอง − ล็อก ; ไม่มีในสต๊อก = ไม่มีคีย์ stk
    for y in out["yarns"]:
        d = detail.get((y["yarn"], y["color"]))
        if d:
            y["stk"] = {f: round(v, 1) for f, v in d.items()}
    out["supply_on"] = bool(plan_config.get("KP_YARN_SUPPLY_ON"))
    # คีย์ SC ของ lock_use เป็นเลขใบของแผน (SO_NO) → แปลงกลับเป็นเลข SC ของล็อก (own_by_order แปลงไปทางเดียว)
    back = {}
    if pairs is not None and {"SC_NO", "SO_NO"} <= set(pairs.columns):
        m: dict = {}
        for sc, so in zip(pairs["SC_NO"], pairs["SO_NO"]):
            d, s = ysc._sc_digits(sc), ysc._so(so)
            if d and s:
                m.setdefault(d, set()).add(s)
        back = {next(iter(sos)): sc for sc, sos in m.items() if len(sos) == 1}
    sc_use: dict = {}
    for k, ws in out["lock_use"]["sc"].items():
        so, rest = k.split("|", 1)
        tgt = sc_use.setdefault(f"{back.get(so, so)}|{rest}", [0.0] * len(ws))
        for i, v in enumerate(ws):
            tgt[i] = round(tgt[i] + v, 1)
    out["lock_use"]["sc"] = sc_use
    u = out.get("unknown") or {}
    if u.get("rows"):
        notes.append(f"ไม่รู้ด้าย {u['rows']} แถว (ผ้า {u['kg']:,.0f} kg) — ไม่มีบรรทัดใบ: "
                     + ", ".join(f"{t} {v:,.0f} kg" for t, v in list(u["types"].items())[:4]))
    out.update({"plan": {"name": path.name, "mtime": _mt(path), "sheet": sheet or "PLAN"},
                "stock_at": stock_at, "note": " · ".join(notes)})
    return out


def lock_usage(sheet: str = None) -> dict:
    """แผนใช้ด้ายที่ล็อกไว้เท่าไหร่ต่อสัปดาห์ (หน้า Monitor Yarn › ล็อกด้าย) — ตัวเดียวกับแผนด้ายของชีทแผน
    → {"weeks", "item": {"ผ้า|ด้าย|สี": [kg]}, "sc": {"SC|ผ้า|ด้าย|สี": [kg]}, "plan", "note"}"""
    t = table(sheet)
    use = t.get("lock_use") or {}
    return {"weeks": t.get("weeks") or [], "item": use.get("item") or {}, "sc": use.get("sc") or {},
            "plan": t.get("plan"), "note": t.get("note") if not t.get("weeks") else ""}


def lots(ava_table: dict, yarn: str, color: str, so: str, fabric: str, hide=()) -> dict:
    """lot ที่เลือกล็อกให้ SC + ผ้าได้ (ด้าย + สีตรง, มียอดใช้ได้, ไม่ใช่ลูกเล็กทั้ง lot) + % ด้ายที่ใช้แปลง kg ผ้า
    lot ที่ด้ายเข้าก่อนขึ้นก่อน (แผนเลือก lot เก่าสุดที่พอทั้งใบ — YarnStockCover.pick_lots)"""
    ysc = _ysc()
    y, c = str(yarn or "").strip().upper(), str(color or "").strip().upper()
    lot_color, age, _note = ysc.lot_meta(hide)
    names = [str(n["name"]).strip().upper() for n in ava_table.get("yarns") or []]
    out = []
    for r in ava_table.get("rows") or []:
        if r.get("l") is None or names[r["g"]] != y or r.get("w") is None or r.get("low"):
            continue
        if (lot_color.get((y, r["l"]), "") if lot_color else "") != c:
            continue
        if (r.get("ava") or 0) <= 0.5:
            continue
        out.append({"lot": r["l"], "ava": r["ava"], "age": age.get((y, r["l"])), "own": r.get("own") or [],
                    "name": ava_table["yarns"][r["g"]]["name"]})
    out.sort(key=lambda d: (-(d["age"] or 0), d["lot"]))
    return {"lots": out, "factor": yarn_lock.factor_for(so, fabric, yarn, color)}

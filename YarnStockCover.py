"""
YarnStockCover.py — ใช้ด้ายในสต๊อกแทนการรอ PO IN (Replan รอบสอง)

user 14 ต.ค. 2569: "เราสามารถเอาเส้นด้ายใน stock มาใช้ได้นะถ้ามี stock"
                  + "ต้องไม่แย่งกันดิ เส้นด้ายจะเปลี่ยนไปตามแผนที่เรามีการทำแผน"
                  + "ต้องบอกด้วยนะด้ายนั้นเป็นของ lot อะไรบ้าง"
สวิตช์ KP_YARN_STOCK_ON (หน้า Monitor Yarn) ; user เลือก:
  - ใบต้องได้ด้ายจากสต๊อก "พอทุกเส้นที่ยังรอ PO" ทั้งใบ ไม่งั้นรอ PO IN ตามเดิม (ไม่แตกใบ)
  - หลายใบแย่งด้ายตัวเดียวกัน → งานสีได้ก่อน แล้วตามกำหนดทอเสร็จ
  - สต๊อกตั้งต้น = ยอด "ใช้ได้" ของหน้า Yarn Ava (ตัวคำนวณเดียวกัน — webapp/backend/yarn_ava_live)
    แถวลูกเล็ก (YARN_WEIGHT ของแถว < KP_YARN_WEIGHT_MIN) ไม่นับ — ทีละแถว ; ไม่นับ lot ที่สงวนให้ลูกค้า
    (กติกา KP_YARN_CUSTOMER_LOTS — ยังไม่ได้จับคู่ชื่อลูกค้าของใบกับ BUYYER)
  - 1 ใบ = 1 lot เท่าที่ทำได้ : lot เก่าสุด (อายุ AGEING_DAYS มากสุด) ที่พอทั้งใบ ; ไม่มี lot ไหนพอ = ผสมให้น้อย lot ที่สุด
    (เดิมผสมหลาย lot เก่าก่อน — user 16 ต.ค. 2569 เปลี่ยน ดู pick_lots)

ด้ายย้อม (TD) แต่ละ lot คนละสี (PCTD60/2RTW มี 101 lot สี BPM8364 / BPM4659 / ...) → ใบยืมได้เฉพาะ lot
ที่สีตรงกับที่บรรทัดใบต้องใช้ (Y_TOPDYED_COLOR ↔ COLOR_TD ของ lot) ; ทุกบัญชีในไฟล์นี้จึงคีย์ด้วย
(ด้าย, สี) — ด้ายดิบ (GY / NO-COLOR) สีว่าง ใช้ lot ไหนก็ได้
ไฟล์สต๊อกที่ยังไม่มีคอลัมน์ COLOR_TD (ดึงก่อนอัปเดต View_Yarn_Aging) → ด้าย TD ยืมไม่ได้ (รอ PO IN ตามเดิม)

บัญชีด้ายตามแผน (ทำไมต้องวาง 2 รอบ):
  สต๊อกลดลงตามสัปดาห์ที่แผนเอาด้ายไปใช้ → ต้องมีแผนก่อนถึงจะรู้ว่าแต่ละสัปดาห์เหลือเท่าไหร่
  รอบแรก = แผนปกติ (ใบรอ PO IN) → คิดว่าแถวงานใหม่ (PLAN_SOURCE=NEW) กินด้ายจากสต๊อกสัปดาห์ไหนเท่าไหร่
    = ยอดผ้า × % ด้าย × ตัวคูณสูญเสีย ของทุกเส้นที่ "ไม่มี PO ของตัวเองกำลังมา"
    (เส้นที่มี PO ของบรรทัดนั้นกำลังมา = PO ตัวเองคลุม ไม่กินสต๊อก ; ใบที่ออก KP แล้วถูกหักในยอด
    ใช้ได้ของ Yarn Ava อยู่แล้ว และไม่ใช่แถว NEW จึงไม่หักซ้ำ)
  ใบที่รอ PO IN "ยืม" ด้ายเส้นที่รอได้ ถ้ายอดคงเหลือ (สต๊อก − ที่แผนกินถึงสัปดาห์นั้น − ที่ใบก่อนหน้ายืม)
  ไม่ติดลบตลอดตั้งแต่สัปดาห์แรกที่วางได้ จนถึงสัปดาห์ก่อน PO ของตัวเองใช้ได้ (PO เข้าแล้วถือว่าคืนเท่าที่ยืม)
  เส้นที่ด้ายเข้าแล้วของใบนั้นก็ต้องขยับมาใช้เร็วขึ้นด้วย → เช็กช่วงก่อนสัปดาห์ที่รอบแรกวางไว้แบบเดียวกัน
  แล้วแบ่ง lot ให้ใบที่ยืมได้ตามลำดับเดียวกัน (lot หนึ่งห้ามแบ่งเกินยอดที่มี)
  รอบสอง = Replan วางใหม่โดยใบที่เลือกไม่ต้องรอ PO IN (คอลัมน์ YARN_STOCK) แล้วตรวจบัญชีซ้ำ (verify)

แหล่งข้อมูล:
  - Order/view_scfol_yarn.xlsx (View_SCFol_Yarn.py) : บรรทัดใบ (LINE_ID) × ด้าย → %, สี TD, PO, วัน PO เข้า
    booking มีแค่รายการวันด้ายเข้า ไม่บอกว่าวันไหนของเส้นไหน และไม่มี %
  - Yarn/view_yarn_aging_received_797.xlsx + Yarn/view_yarn_by_joknit.xlsx + Booking/view_booking.xlsx
    ผ่าน yarn_ava_live (สต๊อกดึงรอบ 06:00 แยกจาก run_all) ; สีและอายุของ lot อ่านจากไฟล์สต๊อกตรง ๆ
"""
import sys
from pathlib import Path

import pandas as pd

import plan_config

BASE_DIR = Path(__file__).parent
SCFOL_FILE = BASE_DIR / "Order" / "view_scfol_yarn.xlsx"
NOTE_COL = "YARN_STOCK"          # คอลัมน์ป้ายในดีมานด์ (order_replan) และชีท PLAN
LOSS_MAX = 1.3                   # ตัวคูณสูญเสียที่เกินนี้ถือว่าข้อมูลเพี้ยน → ใช้ 1
# เลขบรรทัดใบ SC เป็นเลข 6 หลัก — เลขเล็กกว่านี้เป็นของระบบอื่น จับคู่ไม่ได้ (กติกาเดียวกับ yarn_lock._load_booking)
# เจอ 15 ต.ค. 2569: view_scfol_yarn มีเอกสาร D25…-n (1,322 ใบ) ใช้ LINE_ID 1–18 ส่วนใบ FQC ใน booking มี SC_LINE_ID '1'
# → จับคู่กันมั่ว ใบ FQC "ต้องใช้ด้าย" หลายสิบตัว แล้วโดนตัดเป็น "ด้ายไม่พอ" ปลอม 3 ใบ 2,552 kg
_MIN_LINE_ID = 100000
_NO_COLOR = ("", "NO-COLOR", "NOCOLOR", "NONE")


def _txt(v) -> str:
    s = "" if v is None else str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat", "<na>") else s


def _so(v) -> str:
    s = _txt(v)
    return s[:-2] if s.endswith(".0") else s


def _sc_digits(v) -> str:
    """เลข SC ของดีมานด์ 'S721655' → '721655' (ตัวเดียวกับ yarn_lock.sc_digits) ; ไม่ใช่เลขใบ = ''"""
    s = _so(v).upper()
    s = s[1:] if s.startswith("S") else s
    return s if s.isdigit() else ""


def color_of(v) -> str:
    """สีด้ายย้อม → ตัวพิมพ์ใหญ่ ; ด้ายดิบ (NO-COLOR / ว่าง) = ''"""
    s = _txt(v).upper()
    return "" if s in _NO_COLOR else s


def key_label(key) -> str:
    """(ด้าย, สี) → ข้อความให้คนอ่าน"""
    y, c = key
    return f"{y} สี {c}" if c else y


def lines_of(v) -> list:
    """SC_LINE_ID ('775834' / '775834,775835' / 775834.0) → [775834, ...]"""
    out = []
    for x in _txt(v).replace("|", ",").split(","):
        try:
            out.append(int(float(x)))
        except ValueError:
            continue
    return out


def loss_factor(knit, lost) -> float:
    """ตัวคูณสูญเสียของบรรทัด = ด้ายหลังรวม % สูญเสีย ÷ ก่อนรวม (Y_YARN_QTY_LOST ÷ Y_KNIT_QTY)
    ไม่มีข้อมูล / นอกช่วง 1–LOSS_MAX (ข้อมูลเพี้ยน) → 1 ; หน้าล็อกด้าย (yarn_lock.py) ใช้ตัวเดียวกัน"""
    try:
        knit, lost = float(knit or 0), float(lost or 0)
    except (TypeError, ValueError):
        return 1.0
    loss = lost / knit if knit > 0 and lost > 0 else 1.0
    return loss if 1.0 <= loss <= LOSS_MAX else 1.0


def _from_pool(own, kg: float, w: int) -> float:
    """ยอดที่ใบต้องยืมจากกองกลางที่สัปดาห์ w — ด้ายที่ล็อกไว้ให้ใบนี้ใช้ก่อน
    own = {"kg": ยอดล็อก, "used": {สัปดาห์: kg ที่แผนกินจากล็อกไปแล้ว}} ; None = ไม่มีล็อก"""
    if not own:
        return kg
    left = own["kg"] - sum(q for i, q in own["used"].items() if i <= w)
    return max(kg - max(left, 0.0), 0.0)


def _fab_of(key) -> str:
    """คีย์ใบ (SO, ผ้า) → ผ้า ; คีย์แบบอื่น (เทสต์) = ''"""
    return key[1] if isinstance(key, tuple) and len(key) > 1 else ""


class _ItemPool:
    """บัญชีด้ายที่ล็อกให้ Item ผ้า (yarn_item_lock.py) — ทุกใบของผ้านั้นใช้ร่วมกัน (แบบเดียวกับกองกลาง แต่แยกต่อผ้า)
    acc  = {ผ้า: {คีย์ด้าย: {"kg": ยอดล็อก, "used": {สัปดาห์: kg ที่แผนกินจากล็อกแล้ว}}}}
    held = ใบที่ยืมถือยอดไว้ต่อ (ผ้า, คีย์ด้าย, สัปดาห์) ; free = ที่เหลือให้ใบถัดไปถือได้ ณ สัปดาห์นั้น"""

    def __init__(self, acc: dict = None):
        self.acc = acc or {}
        self.held: dict = {}

    def free(self, fab: str, y, w: int) -> float:
        a = self.acc.get(fab, {}).get(y)
        if not a:
            return 0.0
        return max(a["kg"] - sum(q for i, q in a["used"].items() if i <= w) - self.held.get((fab, y, w), 0.0), 0.0)

    def hold(self, fab: str, y, w: int, q: float) -> None:
        if q > 0:
            self.held[(fab, y, w)] = self.held.get((fab, y, w), 0.0) + q


# =============================================================================
# ข้อมูล
# =============================================================================
_DETAIL = ("stock", "small", "own", "kp", "lock", "usable")


def _split_lot(d: dict, r: dict, ava: float) -> None:
    """แตกด้าย lot ที่อยู่ในสต๊อก 1 lot ลงกอง : ในคลัง = ลูกเล็ก + สงวนลูกค้า + ใบ KP จอง + ล็อก + ใช้ได้ (บวกกลับได้พอดี)
    ใบ KP จองเกิน lot = lot ใช้ไม่ได้ทั้ง lot (กองใช้ได้ตัด lot นั้นทิ้ง ไม่ลามไป lot อื่น) → นับ KP จองเท่าที่ lot มี"""
    w = float(r.get("w") or 0)
    d["stock"] += w
    if r.get("low"):
        d["small"] += w
        return
    sw, lk, left = float(r.get("small") or 0), float(r.get("lk") or 0), max(ava, 0.0)
    d["small"] += sw
    d["lock"] += lk
    d["kp"] += max(w - sw - lk - left, 0.0)
    d["own" if r.get("own") else "usable"] += left


def usable_stock(detail: dict = None) -> tuple:
    """ด้ายใช้ได้แบบหน้า Yarn Ava ระดับ lot → (lots, totals, เวลาไฟล์สต๊อก, หมายเหตุ)

    lots   = {(ด้าย, สี): [{"lot", "kg", "age"}]} — เฉพาะ lot ที่ใช้ได้ (> 0 kg หลังหักแถวลูกเล็ก, ไม่ได้สงวนให้ลูกค้า)
    totals = {(ด้าย, สี): kg} = รวม lot − ใบ KP ที่จองด้ายนั้นแต่ยังไม่ระบุ lot
             (ด้าย TD ไม่รู้ว่าใบที่ยังไม่ระบุ lot จองสีไหน → หักจากทุกสีของด้ายนั้น เข้มไว้ก่อน)
    own    = ด้ายที่ฝั่งด้ายล็อกไว้ให้ใบ (หน้าแผนด้าย — webapp/backend/yarn_lock.py)
             {(SC เลขล้วน, ผ้า): {(ด้าย, สี): [{"lot", "kg", "age"}]}} ; kg = ยอดที่ lot กันให้ได้จริง
             lots ข้างบนคือ "กองกลาง" = ยอดใช้ได้ของหน้า Yarn Ava ที่หักล็อกออกแล้ว
    item   = ด้ายที่ฝั่งด้ายล็อกไว้ให้ Item ผ้า — ทุกใบของผ้านั้นใช้ร่วมกัน (หน้า Monitor Yarn — yarn_item_lock.py)
             {ผ้า: {(ด้าย, สี): [{"lot", "kg", "age"}]}} ; กองกลางหักออกแล้วเหมือนกัน → ผ้าอื่นใช้ไม่ได้
    → (lots, totals, เวลาไฟล์สต๊อก, หมายเหตุ, own, item)
    detail = dict ว่างที่ส่งมา → เติมที่มาของยอดใช้ได้ต่อ (ด้าย, สี) (หน้าแผนด้าย — user 16 ต.ค. 2569 "ไม่มีบอกว่ายอด Stock มีเท่าไหร่")
             {"stock" ในคลัง, "small" ลูกเล็ก, "own" สงวนลูกค้า, "kp" ใบ KP จองยังไม่โอน, "lock" ล็อก, "usable"}
             stock − small − own − kp − lock = usable = totals (มีแต่ด้ายที่ใช้ได้หมดแล้วด้วย — totals ไม่มีคีย์นั้น)
    ใช้ตัวคำนวณของหน้า Yarn Ava ตรง ๆ ตัวเลขจึงตรงกับที่ผู้ใช้เห็นบนเว็บ"""
    _webapp_path()
    import yarn_ava_live
    import yarn_stock_live

    vals = {k: plan_config.get(k) for k in ("KP_YARN_HIDE_PO_ON", "KP_YARN_HIDE_PO_SUFFIXES")}
    hide = yarn_stock_live.hidden_suffixes(vals)
    try:
        cone_min = float(plan_config.get("KP_YARN_WEIGHT_MIN"))
    except (TypeError, ValueError):
        cone_min = yarn_ava_live._CONE_MIN_DEFAULT
    t = yarn_ava_live.table(hide, cone_min, plan_config.get("KP_YARN_CUSTOMER_LOTS") or [])
    if not t.get("yarns"):
        return {}, {}, None, f"ไม่มีข้อมูลสต๊อกด้าย ({t.get('note') or 'ไฟล์ว่าง'})", {}, {}

    # สีและอายุของ lot — yarn_ava_live ไม่ได้ส่งมา อ่านจากไฟล์สต๊อกตัวเดียวกัน
    color, age, note = lot_meta(hide)
    has_color = bool(color)

    def lot_color(y, lot):
        # ไม่มีข้อมูลสี: ลงเป็นด้ายดิบ — คีย์ฝั่งใบของด้าย TD เป็น (ด้าย, สี) จึงไม่มีวันตรง = ใช้ไม่ได้
        return color.get((y, lot), "") if has_color else ""

    names = [str(y["name"]).strip().upper() for y in t["yarns"]]
    lots: dict = {}
    own: dict = {}
    item: dict = {}
    nolot: dict = {}                          # ด้าย → kg ที่ใบ KP จองแต่ยังไม่ระบุ lot (ค่าบวก)
    for r in t["rows"]:
        y = names[r["g"]]
        ava = float(r.get("ava") or 0)
        if r.get("l") is None:
            nolot[y] = nolot.get(y, 0.0) + max(-ava, 0.0)
            continue
        a = float(age.get((y, r["l"]), 0) or 0)
        # ล็อกของ SC / Item — kg = ยอดที่ lot กันให้ได้จริง (yarn_ava_live ตัดส่วนที่ lot ไม่พอ / ลูกเล็กทั้ง lot ออกแล้ว)
        for lk in r.get("locks") or []:
            kg = float(lk.get("kg") or 0)
            if kg <= 0:
                continue
            ent = {"lot": r["l"], "kg": kg, "age": a}
            if lk.get("kind") == "item":
                item.setdefault(_txt(lk.get("fabric")).upper(), {}) \
                    .setdefault((y, lot_color(y, r["l"])), []).append(ent)
            else:
                own.setdefault((_so(lk.get("so")), _txt(lk.get("fabric")).upper()), {}) \
                   .setdefault((y, lot_color(y, r["l"])), []).append(ent)
        if detail is not None and r.get("w") is not None:
            _split_lot(detail.setdefault((y, lot_color(y, r["l"])), dict.fromkeys(_DETAIL, 0.0)), r, ava)
        if ava <= 0 or r.get("low") or r.get("own"):
            continue
        lots.setdefault((y, lot_color(y, r["l"])), []).append({"lot": r["l"], "kg": ava, "age": a})
    totals = {}
    for k, ls in lots.items():
        ls.sort(key=lambda d: (-d["age"], d["lot"]))      # ด้ายเข้าก่อน (อายุมาก) ใช้ก่อน
        totals[k] = max(sum(d["kg"] for d in ls) - nolot.get(k[0], 0.0), 0.0)
    for k, d in (detail or {}).items():
        cut = min(nolot.get(k[0], 0.0), d["usable"])      # ใบ KP ที่ยังไม่ระบุ lot — หักแบบเดียวกับ totals
        d["kp"] += cut
        d["usable"] -= cut
    for ys in list(own.values()) + list(item.values()):
        for ls in ys.values():
            ls.sort(key=lambda d: (-d["age"], d["lot"]))
    return lots, totals, t.get("stock_at"), note, own, item


def _webapp_path() -> None:
    backend = BASE_DIR / "webapp" / "backend"
    if str(backend) not in sys.path:
        sys.path.append(str(backend))      # ต่อท้าย — ไม่ให้โมดูลของ webapp บังโมดูลชื่อเดียวกันที่ root


_META_CACHE: dict = {}


def lot_meta(hide) -> tuple:
    """สีและอายุของ lot จากไฟล์สต๊อก → ({(ด้าย, lot): สี}, {(ด้าย, lot): อายุวัน}, หมายเหตุ) — ด้ายพิมพ์ใหญ่
    สี dict ว่าง = ไฟล์ยังไม่มีคอลัมน์ COLOR_TD (ด้าย TD จับคู่สีไม่ได้)
    ใช้ทั้งตอนแผนยืมสต๊อก (usable_stock) และหน้าล็อกด้าย (yarn_lock.options) — cache ตามเวลาไฟล์"""
    _webapp_path()
    import yarn_stock_live

    key = (yarn_stock_live._mtime(yarn_stock_live._FILE), tuple(hide))
    if _META_CACHE.get("key") == key:
        return _META_CACHE["val"]
    note = ""
    color, age = {}, {}
    try:
        st = pd.read_excel(yarn_stock_live._FILE)
        st.columns = st.columns.str.strip().str.upper()
        # แถว PO ที่ซ่อน (-D/-T) ไม่นับในยอดใช้ได้ → ต้องไม่นับในอายุ/สีของ lot ด้วย
        # (เจอ 14 ต.ค.: lot SHK-B515WA มีแถว YRK671410-D อายุ 734 วัน ทำให้ lot ดูเก่ากว่าจริง 535 วัน)
        if "PO_NO" in st.columns:
            st = st[~st["PO_NO"].map(lambda p: yarn_stock_live._po_hidden(_txt(p), hide))]
        st["_I"] = st["ITEM_CODE"].map(_txt).str.upper()
        st["_L"] = st["YARN_LOT"].map(_txt)
        if "AGEING_DAYS" in st.columns:
            age = st.groupby(["_I", "_L"])["AGEING_DAYS"].max().to_dict()
        if "COLOR_TD" in st.columns:
            color = st.assign(_C=st["COLOR_TD"].map(color_of)).groupby(["_I", "_L"])["_C"].first().to_dict()
        else:
            note = "ไฟล์สต๊อกยังไม่มีสีของ lot (COLOR_TD) — ด้าย TD ยืมไม่ได้รอบนี้"
    except Exception as e:
        note = f"อ่านสี/อายุ lot จากไฟล์สต๊อกไม่ได้ ({e}) — ด้าย TD ยืมไม่ได้รอบนี้"
    _META_CACHE.update(key=key, val=(color, age, note))
    return color, age, note


def load_yarn_lines(path: Path | None = None, today=None) -> dict:
    """ไฟล์ด้ายรายบรรทัดใบ → {LINE_ID: {(ด้าย, สี): {"pct", "loss", "po_in", "pos"}}}

    สี = Y_TOPDYED_COLOR (ด้าย TD) ; ด้ายดิบ = '' — ไฟล์รุ่นแรกที่ไม่มีคอลัมน์สีถือเป็นด้ายดิบทั้งหมด
    po_in = วัน PO เข้าที่ช้าสุดของ PO จริงที่ "ยังไม่ถึง" (None = ไม่มี PO ของตัวเองกำลังมา = ใช้ของในสต๊อก)
    pos   = เลข PO จริงที่ยังไม่ถึงของบรรทัดนั้น (พิมพ์ใหญ่) — supply_cap กัน PO Pending ไว้ให้บรรทัดที่รอมันอยู่
    PO จริง = มีเลข PO และไม่ใช่ "01.ไม่สั่งซื้อเส้นด้าย" (Stop-Yarn / PR ที่ไม่ได้สั่ง)
    นับเฉพาะเลขบรรทัดใบ SC (≥ _MIN_LINE_ID) — ดู _MIN_LINE_ID"""
    path = Path(path) if path is not None else SCFOL_FILE
    if not path.exists():
        return {}
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip().str.upper()
    today = pd.Timestamp(today).normalize() if today is not None else pd.Timestamp.today().normalize()
    df["_LINE"] = pd.to_numeric(df["LINE_ID"], errors="coerce")
    df = df[df["_LINE"] >= _MIN_LINE_ID].copy()
    df["_Y"] = df["Y_YARN_ITEM"].map(_txt).str.upper()
    df = df[df["_Y"] != ""]
    df["_C"] = df["Y_TOPDYED_COLOR"].map(color_of) if "Y_TOPDYED_COLOR" in df.columns else ""
    df["_PCT"] = pd.to_numeric(df["Y_CONTAIN_PCT"], errors="coerce").fillna(0)
    df["_KNIT"] = pd.to_numeric(df.get("Y_KNIT_QTY"), errors="coerce").fillna(0)
    df["_LOST"] = pd.to_numeric(df.get("Y_YARN_QTY_LOST"), errors="coerce").fillna(0)
    po = df["PO_NO"].map(_txt)
    df["_PIN"] = pd.to_datetime(df["PO_IN_DATE"], errors="coerce").dt.normalize()
    real_future = (po != "") & ~po.str.startswith("01.") & (df["_PIN"] > today)
    out: dict = {}
    # % / ยอด ซ้ำทุกแถว PO ของ (บรรทัด, ด้าย, สี) เดียวกัน → ใช้ค่าสูงสุดตัวเดียว ไม่รวม
    keys = ["_LINE", "_Y", "_C"]
    g = df.groupby(keys).agg(pct=("_PCT", "max"), knit=("_KNIT", "max"), lost=("_LOST", "max"))
    pin = df[real_future].groupby(keys)["_PIN"].max()
    fut = df[real_future].assign(_PO=po[real_future].str.upper())
    pos = fut.groupby(keys)["_PO"].agg(lambda s: sorted(set(s)))
    for (line, y, c), r in g.iterrows():
        loss = loss_factor(r["knit"], r["lost"])
        out.setdefault(int(line), {})[(y, c)] = {
            "pct": float(r["pct"]), "loss": float(loss),
            "po_in": pin.get((line, y, c)), "pos": pos.get((line, y, c), []),
        }
    return out


# =============================================================================
# บัญชีด้ายตามแผน + เลือกใบที่ยืมได้ + แบ่ง lot
# =============================================================================
def plan_consumption(plan: pd.DataFrame, ylines: dict, week_idx, floor_idx: int,
                     own: dict = None, used: dict = None, item: dict = None, item_used: dict = None) -> dict:
    """แถวงานใหม่ของแผน → ด้ายที่กินจากสต๊อก (กองกลาง) {(ด้าย, สี): {index สัปดาห์: kg}}

    เส้นที่มี PO ของตัวเองกำลังมา (po_in) ไม่นับ — PO นั้นคลุมยอดของมันเอง
    แถวที่ยุบหลายบรรทัด แบ่งยอดเท่า ๆ กันต่อบรรทัด (แผนไม่มียอดแยกต่อบรรทัด)

    own  = ด้ายที่ฝั่งด้ายล็อกให้ใบ {(SO, ผ้า): {คีย์ด้าย: kg}} (yarn_lock.py) — ใบที่มีล็อกกินจากล็อกของ
           ตัวเองก่อนตามลำดับสัปดาห์ ส่วนเกินค่อยกินกองกลาง (กองกลางหักล็อกออกไปแล้วตั้งแต่ต้น)
    used = dict ว่างที่ผู้เรียกส่งมา → เติม {(SO, ผ้า): {คีย์ด้าย: {สัปดาห์: kg ที่กินจากล็อก}}}
    item = ด้ายที่ล็อกให้ Item ผ้า {ผ้า: {คีย์ด้าย: kg}} (yarn_item_lock.py) — ส่วนที่ล็อกของใบไม่พอ ทุกใบของผ้านั้น
           กินล็อก Item ร่วมกันตามลำดับสัปดาห์ ก่อนกองกลาง ; item_used = dict ว่าง → {ผ้า: {คีย์ด้าย: {สัปดาห์: kg}}}"""
    out: dict = {}
    if plan is None or plan.empty:
        return out
    item = item or {}
    p = plan[(plan["PLAN_SOURCE"].astype(str).str.upper() == "NEW")
             & (pd.to_numeric(plan["PRODUCE_QTY"], errors="coerce").fillna(0) > 0)]
    locked: dict = {}              # (ใบ, คีย์ด้าย) → {สัปดาห์: kg} ของใบที่มีล็อกด้ายเส้นนั้น — ตัดล็อกทีหลังตามสัปดาห์
    by_item: dict = {}             # (ผ้า, คีย์ด้าย) → {สัปดาห์: kg} ส่วนที่ไปกินล็อก Item

    def to_item_or_pool(fab, k, wi, kg):
        if k in item.get(fab, {}):
            o = by_item.setdefault((fab, k), {})
            o[wi] = o.get(wi, 0.0) + kg
        else:
            out.setdefault(k, {})
            out[k][wi] = out[k].get(wi, 0.0) + kg

    for r in p.itertuples(index=False):
        wi = week_idx(r.PLAN_WEEK, r.PLAN_YEAR)
        if wi is None or wi < floor_idx:
            continue
        lines = lines_of(r.SC_LINE_ID)
        if not lines:
            continue
        mine = own.get((_so(getattr(r, "SC_SO_NO", "")), _txt(getattr(r, "ITEM_CODE", "")).upper()), {}) \
            if own else {}
        share = float(r.PRODUCE_QTY) / len(lines)
        for ln in lines:
            for k, info in ylines.get(ln, {}).items():
                if info["po_in"] is not None:
                    continue
                kg = share * info["pct"] / 100.0 * info["loss"]
                if kg > 0:
                    if k in mine:
                        o = locked.setdefault(((_so(r.SC_SO_NO), _txt(r.ITEM_CODE).upper()), k), {})
                        o[wi] = o.get(wi, 0.0) + kg
                        continue
                    to_item_or_pool(_txt(getattr(r, "ITEM_CODE", "")).upper(), k, wi, kg)
    for (okey, k), weeks in locked.items():
        left = own[okey][k]
        for wi in sorted(weeks):
            q = weeks[wi]
            take = min(left, q)
            if take > 0:
                left -= take
                if used is not None:
                    u = used.setdefault(okey, {}).setdefault(k, {})
                    u[wi] = u.get(wi, 0.0) + take
            if q - take > 1e-9:
                to_item_or_pool(okey[1], k, wi, q - take)
    for (fab, k), weeks in by_item.items():
        left = item[fab][k]
        for wi in sorted(weeks):
            q = weeks[wi]
            take = min(left, q)
            if take > 0:
                left -= take
                if item_used is not None:
                    u = item_used.setdefault(fab, {}).setdefault(k, {})
                    u[wi] = u.get(wi, 0.0) + take
            if q - take > 1e-9:
                out.setdefault(k, {})
                out[k][wi] = out[k].get(wi, 0.0) + (q - take)
    return out


def select_borrowers(cands: list, cons: dict, usable: dict, floor_idx: int, item: dict = None) -> dict:
    """เลือกใบที่ยืมด้ายในสต๊อกได้ (ฟังก์ชันล้วน — เทสต์ที่ tests/test_yarn_stock_cover.py)

    cands = [{"key", "color": bool, "due": int, "needs": {คีย์ด้าย: (kg, สัปดาห์สุดท้ายที่ต้องถือไว้)}}]
      ต้องถือยอดไว้ช่วง floor_idx..สัปดาห์สุดท้าย (รวม) ; ช่วงว่าง (สุดท้าย < floor) = ไม่ต้องเช็กเส้นนั้น
    cons  = ด้ายที่แผนกินจากสต๊อก {คีย์ด้าย: {สัปดาห์: kg}} · usable = สต๊อกตั้งต้น {คีย์ด้าย: kg}
    → {key: {คีย์ด้าย: kg ที่ต้องใช้}} ; ลำดับ = งานสีก่อน แล้วกำหนดทอเสร็จเร็วก่อน
    ใบต้องได้ "ทุกเส้น" (ขาดเส้นเดียว = ไม่ยืมเลย) และยอดคงเหลือทุกสัปดาห์ในช่วงต้องไม่ติดลบ

    ใบที่ฝั่งด้ายล็อกด้ายไว้ให้ (c["own"] = {คีย์ด้าย: {"kg": ยอดล็อก, "used": {สัปดาห์: kg ที่แผนกินจากล็อกแล้ว}}})
    ใช้ล็อกของตัวเองก่อน ยืมกองกลางเฉพาะส่วนที่ล็อกไม่พอ (user เลือกข้อ ก 15 ต.ค. 2569) ;
    usable / cons เป็นของกองกลาง (หักล็อกออกแล้ว) ใบอื่นจึงยืมด้ายที่ล็อกไว้ไม่ได้
    ล็อก Item (item = บัญชีแบบ _ItemPool.acc ; c["item"] = ผ้าของใบ) : ส่วนที่ล็อกของใบไม่พอ ใช้ล็อก Item ของผ้าตัวเองก่อน
    (ใบของผ้าเดียวกันใช้ร่วมกันตามลำดับ) ที่เหลือค่อยยืมกองกลาง ; ใบของผ้าอื่นไม่เห็นล็อกนี้"""
    borrowed: dict = {}            # (คีย์ด้าย, สัปดาห์) → kg ที่ใบก่อนหน้ายืมไปแล้ว
    pool = _ItemPool(item)
    cum_cache: dict = {}

    def cum(y, w: int) -> float:
        k = (y, w)
        if k not in cum_cache:
            cum_cache[k] = sum(q for i, q in cons.get(y, {}).items() if i <= w)
        return cum_cache[k]

    def bal(y, w: int) -> float:
        return usable.get(y, 0.0) - cum(y, w) - borrowed.get((y, w), 0.0)

    picks: dict = {}
    for c in sorted(cands, key=lambda c: (not c["color"], c["due"], str(c["key"]))):
        needs = {y: (kg, end) for y, (kg, end) in c["needs"].items() if end >= floor_idx and kg > 0}
        if not needs:
            continue
        own = c.get("own") or {}
        fab = c.get("item") or ""

        def split(y, kg, w):
            """ยอดที่ต้องถือ ณ สัปดาห์ w หลังใช้ล็อกของใบ → (จากล็อก Item, จากกองกลาง)"""
            need = _from_pool(own.get(y), kg, w)
            ti = min(need, pool.free(fab, y, w)) if fab else 0.0
            return ti, need - ti

        ok = all(all(tc <= 1e-9 or (y in usable and bal(y, w) >= tc)
                     for w in range(floor_idx, end + 1) for _ti, tc in [split(y, kg, w)])
                 for y, (kg, end) in needs.items())
        if not ok:
            continue
        for y, (kg, end) in needs.items():
            for w in range(floor_idx, end + 1):
                ti, tc = split(y, kg, w)
                pool.hold(fab, y, w, ti)
                borrowed[(y, w)] = borrowed.get((y, w), 0.0) + tc
        picks[c["key"]] = {y: kg for y, (kg, _end) in needs.items()}
    return picks


def pick_lots(kgs: list, need: float) -> list:
    """เลือก lot ให้ยอด need ของใบเดียว — 1 ใบ = 1 lot เท่าที่ทำได้
    (user 16 ต.ค. 2569 เลือก ; เดิม lot เก่าก่อนผสมได้ไม่จำกัด → ข้อมูลจริง 97/217 ใบได้มากกว่า 1 lot
     ทั้งที่ 71 ใบในนั้นมี lot เดียวพอทั้งใบ, DTY75/72/1 ใบเดียวได้ 18 lot)
    kgs = ยอดที่เหลือของแต่ละ lot เรียงเก่าก่อน (ไม่แก้ค่า) → [(ตำแหน่ง lot, kg)] เรียงเก่าก่อน
      ทุกรอบ: มี lot ที่พอส่วนที่เหลือ → lot เก่าสุดที่พอ จบ (เศษ lot เก่าจึงตกที่ใบเล็กที่พอดี ใบใหญ่ข้ามไป — user เลือก)
              ไม่มี → เอา lot ใหญ่สุดทั้ง lot แล้ววนต่อ (= ใช้น้อย lot ที่สุด)
      ทุก lot รวมกันไม่พอ → ได้เท่าที่มี ผู้เรียกดูส่วนที่ขาดเอง"""
    eps = 1e-9
    have = [(i, q) for i, q in enumerate(kgs) if q > eps]
    big = sorted(have, key=lambda t: (-t[1], t[0]))
    out, used, rest = [], set(), need
    while rest > eps:
        fit = next((i for i, q in have if i not in used and q >= rest - eps), None)
        if fit is not None:
            out.append((fit, rest))
            break
        nxt = next(((i, q) for i, q in big if i not in used), None)
        if nxt is None:
            break
        out.append(nxt)
        used.add(nxt[0])
        rest -= nxt[1]
    return sorted(out)


def allocate_lots(order: list, borrow: dict, lots: dict, own_lots: dict = None, item_lots: dict = None) -> dict:
    """แบ่ง lot ให้ใบที่ยืมได้ ตามลำดับใบ (order) — 1 ใบ = 1 lot เท่าที่ทำได้ (pick_lots : lot เก่าสุดที่พอทั้งใบ
    ไม่มี lot ไหนพอ = ผสมให้น้อย lot ที่สุด) ; lot ที่ล็อกให้ใบ / Item ยังใช้ก่อนเก่าก่อนตามเดิม (คนเลือก lot เอง)

    lot หนึ่งแบ่งให้หลายใบได้ แต่รวมกันห้ามเกินยอดใช้ได้ของ lot
    own_lots = {key ใบ: {คีย์ด้าย: [{"lot", "kg"}]}} lot ที่ล็อกให้ใบนั้น (คงเหลือหลังแผนกินไปแล้ว) — ใช้ก่อนกองกลาง
               ป้ายเขียน "lot X (ล็อก)" ; ไม่ไปแบ่งให้ใบอื่น
    item_lots = {ผ้า: {คีย์ด้าย: [{"lot", "kg"}]}} lot ที่ล็อกให้ Item (คงเหลือหลังแผนกิน) — ใบของผ้านั้นใช้ร่วมกัน
               ต่อจากล็อกของใบ ก่อนกองกลาง ป้าย "lot X (ล็อก Item)"
    → {key ใบ: {คีย์ด้าย: [(lot, kg)]}} ; ใบที่ lot ไม่พอ (ไม่ควรเกิด — ยอดรวมผ่านบัญชีมาแล้ว) ไม่อยู่ในผล"""
    left = {k: [dict(d) for d in ls] for k, ls in lots.items()}
    ileft = {f: {k: [dict(d) for d in ls] for k, ls in ys.items()} for f, ys in (item_lots or {}).items()}
    out: dict = {}
    for key in order:
        if key not in borrow:
            continue
        got: dict = {}
        mine = (own_lots or {}).get(key, {})
        fab = _fab_of(key)
        trial = {k: [dict(d) for d in left.get(k, [])] for k in borrow[key]}
        itrial = {k: [dict(d) for d in ileft.get(fab, {}).get(k, [])] for k in borrow[key]}
        ok = True
        for yk, need in borrow[key].items():
            rest, take = need, []
            for d in mine.get(yk, []):
                if rest <= 1e-6:
                    break
                q = min(d["kg"], rest)
                if q > 1e-6:
                    take.append((f"{d['lot']} (ล็อก)", q))
                    rest -= q
            for d in itrial[yk]:
                if rest <= 1e-6:
                    break
                q = min(d["kg"], rest)
                if q > 1e-6:
                    take.append((f"{d['lot']} (ล็อก Item)", q))
                    d["kg"] -= q
                    rest -= q
            if rest > 1e-6:
                ls = trial[yk]
                for i, q in pick_lots([d["kg"] for d in ls], rest):
                    take.append((ls[i]["lot"], q))
                    ls[i]["kg"] -= q
                    rest -= q
            if rest > 0.5:
                ok = False
                break
            got[yk] = take
        if ok:
            left.update(trial)
            if fab in ileft:
                ileft[fab].update(itrial)
            out[key] = got
    return out


def build_candidates(demand: pd.DataFrame, plan: pd.DataFrame, ylines: dict, tl, floor_idx: int,
                     lag: int, last_idx: int) -> tuple:
    """ใบที่ยังรอ PO IN (วันด้ายเข้า + lag เลยสัปดาห์แรกที่วางได้) → (cands, audit rows)

    1 ใบ = SO + ผ้า (ทุกแถวดีมานด์ของใบนั้นที่มีวันด้ายเข้า) ; ยอด = PENDING_PLAN รวม
    งานสี = รอบแรกมีแถวไหนของใบนี้เป็นก้อนสี (COLOR_PART) ; กำหนด = TARGET ของรอบแรก (ไม่มี → ท้ายคิว)"""
    d = demand.copy()
    d["_PT"] = pd.to_datetime(d.get("POINT_DATE"), errors="coerce")
    d = d[d["_PT"].notna()]
    audit: list = []
    if d.empty:
        return [], audit

    def date_idx(dt):
        i = tl.idx_of_date(dt)
        if i is not None:
            return i
        return last_idx + 1 if pd.Timestamp(dt) > pd.Timestamp(tl.ends[-1]) else -1

    d["_PI"] = d["_PT"].map(date_idx)
    d = d[d["_PI"] + lag > floor_idx]
    if d.empty:
        return [], audit
    d["_KEY"] = list(zip(d["SO_NO"].map(_so), d["FABRIC_ITEM"].map(_txt).str.upper()))

    # รอบแรก: สัปดาห์แรกที่วาง / ก้อนสี / เป้าทอเสร็จ ต่อใบ
    first, color, due = {}, set(), {}
    if plan is not None and not plan.empty:
        p = plan[plan["PLAN_SOURCE"].astype(str).str.upper() == "NEW"]
        for r in p.itertuples(index=False):
            k = (_so(r.SC_SO_NO), _txt(r.ITEM_CODE).upper())
            wi = tl.idx(r.PLAN_WEEK, r.PLAN_YEAR)
            if wi is not None and float(r.PRODUCE_QTY or 0) > 0:
                first[k] = min(first.get(k, wi), wi)
            if _txt(getattr(r, "COLOR_PART", "")).upper() in ("YES", "Y", "TRUE", "1"):
                color.add(k)
            ti = tl.idx(getattr(r, "TARGET_KNIT", None), getattr(r, "TARGET_YEAR", None))
            if ti is not None:
                due[k] = min(due.get(k, ti), ti)

    cands = []
    for k, g in d.groupby("_KEY", sort=False):
        qty = float(pd.to_numeric(g["PENDING_PLAN"], errors="coerce").fillna(0).sum())
        lines = sorted({ln for v in g.get("SC_LINE_ID", pd.Series(dtype=object)) for ln in lines_of(v)})
        row = {"SO_NO": k[0], "ITEM_CODE": k[1], "QTY": round(qty, 2), "COLOR": "Yes" if k in color else "",
               "PO_IN_DATE": f"{g['_PT'].max():%d/%m/%Y}", "LINES": ",".join(map(str, lines))}
        if qty <= 0 or not lines or not any(ln in ylines for ln in lines):
            row["RESULT"] = "ไม่มีข้อมูลด้ายรายบรรทัด — รอ PO IN"
            audit.append(row)
            continue
        share = qty / len(lines)
        needs, pend = {}, {}
        for ln in lines:
            for yk, info in ylines.get(ln, {}).items():
                kg = share * info["pct"] / 100.0 * info["loss"]
                if kg <= 0:
                    continue
                if info["po_in"] is not None:
                    pi = date_idx(info["po_in"])
                    end = min(pi + lag - 1, last_idx)
                    pend[yk] = max(pend.get(yk, pd.Timestamp.min), info["po_in"])
                else:
                    # เส้นที่ด้ายเข้าแล้ว: รอบแรกเริ่มกินที่สัปดาห์แรกที่วาง → ย้ายมาเร็วขึ้นต้องมีของช่วงก่อนหน้านั้น
                    end = (first[k] - 1) if k in first else last_idx
                kg0, end0 = needs.get(yk, (0.0, end))
                needs[yk] = (kg0 + kg, max(end0, end))
        if not pend:
            row["RESULT"] = "ไม่รู้ว่ารอด้ายเส้นไหน (ไม่มี PO ที่ยังไม่เข้า) — รอ PO IN"
            audit.append(row)
            continue
        row["NEEDS"] = " · ".join(f"{key_label(yk)} {kg:,.0f} kg" for yk, (kg, _e) in needs.items())
        cands.append({"key": k, "color": k in color, "due": due.get(k, 10 ** 6), "needs": needs,
                      "pend": pend, "row": row})
    return cands, audit


def lots_text(lots: list) -> str:
    return " + ".join(f"lot {lot} {kg:,.0f} kg" for lot, kg in lots)


def note_of(alloc: dict, pend: dict) -> str:
    """ข้อความป้ายของใบที่ยืม — ด้าย (สี) ที่แทน PO + lot ที่ใช้ ; เส้นที่ด้ายเข้าแล้วแต่ต้องใช้เร็วขึ้นต่อท้าย"""
    parts = [f"{key_label(yk)} {lots_text(ls)} (แทน PO {pend[yk]:%d/%m/%Y})"
             for yk, ls in alloc.items() if yk in pend]
    more = [f"{key_label(yk)} {lots_text(ls)}" for yk, ls in alloc.items() if yk not in pend]
    return "ใช้ด้ายสต๊อก: " + " · ".join(parts + more)


def own_by_order(own: dict, demand: pd.DataFrame) -> dict:
    """ล็อกด้าย คีย์ (SC เลขล้วน, ผ้า) → คีย์ใบของดีมานด์/แผน (SO_NO, ผ้า)

    booking ส่วนใหญ่ SO_NO = เลข SC แต่ ~3% เป็นเลข SO อีกระบบ (เช่น S708940 ↔ 625060500) ; แผนใช้ SO_NO
    → แปลงผ่านคู่ SC_NO / SO_NO ของดีมานด์ ; SC ที่มี SO_NO หลายเลข = ไม่แปลง กันล็อกเดียวถูกนับให้สองใบ"""
    m: dict = {}
    if demand is not None and "SC_NO" in demand.columns and "SO_NO" in demand.columns:
        for sc, so in zip(demand["SC_NO"], demand["SO_NO"]):
            d, s = _sc_digits(sc), _so(so)
            if d and s:
                m.setdefault(d, set()).add(s)
    out: dict = {}
    for (sc, fab), ys in own.items():
        sos = m.get(sc)
        tgt = out.setdefault((next(iter(sos)), fab) if sos and len(sos) == 1 else (sc, fab), {})
        for yk, ls in ys.items():
            tgt.setdefault(yk, []).extend(dict(d) for d in ls)
    return out


def _own_left(own_lots: dict, used: dict) -> dict:
    """lot ที่ล็อกให้ใบ หักส่วนที่แผนกินไปแล้ว (lot เก่าก่อน) — เหลือไว้แทนการรอ PO"""
    out: dict = {}
    for k, ys in own_lots.items():
        for yk, ls in ys.items():
            eat = sum(used.get(k, {}).get(yk, {}).values())
            rest = []
            for d in ls:
                q = min(d["kg"], eat)
                eat -= q
                if d["kg"] - q > 1e-6:
                    rest.append({**d, "kg": d["kg"] - q})
            out.setdefault(k, {})[yk] = rest
    return out


def plan_cover(result_file: Path, demand: pd.DataFrame, tl, freeze_weeks: int, lag: int,
               today=None) -> dict:
    """รอบแรกของ Replan → ใบที่ยืมด้ายสต๊อกได้

    คืน {"picks": {(SO, ผ้า): ข้อความป้าย}, "borrow": {(SO, ผ้า): {คีย์ด้าย: kg}}, "pend": {...},
         "lots": {(SO, ผ้า): {คีย์ด้าย: [(lot, kg)]}}, "audit": DataFrame, "stock_at", "note"/"warn",
         "own_kg": ด้ายที่ฝั่งด้ายล็อกให้ใบ {(SO, ผ้า): {คีย์ด้าย: kg}}, "item_kg": ล็อกให้ Item {ผ้า: {คีย์ด้าย: kg}}}

    ด้ายที่ล็อกไว้ (หน้าแผนด้าย / Monitor Yarn) : กองกลางหักล็อกออกแล้ว — ใบอื่นยืมไม่ได้ ; ใบเจ้าของล็อกกินล็อกตัวเองก่อน
    และนับล็อกเป็นของตัวเองตอนเช็กว่าเลิกรอ PO ได้ไหม (user เลือกข้อ ก 15 ต.ค. 2569) ; ล็อกเองไม่เปลี่ยนวัน
    ล็อก Item = ของทุกใบของผ้านั้นร่วมกัน (ต่อจากล็อกของใบ ก่อนกองกลาง) ผ้าอื่นยืมไม่ได้"""
    empty = {"picks": {}, "borrow": {}, "pend": {}, "lots": {}, "audit": pd.DataFrame(), "stock_at": None}
    ylines = load_yarn_lines(today=today)
    if not ylines:
        return {**empty, "note": f"ไม่มีไฟล์ {SCFOL_FILE.name} (View_SCFol_Yarn)"}
    lots, usable, stock_at, warn, own, item_lots = usable_stock()
    if not lots and not own and not item_lots:
        return {**empty, "note": warn or "ไม่มีข้อมูลสต๊อกด้าย"}
    own_lots = own_by_order(own, demand)
    own_kg = {k: {yk: sum(d["kg"] for d in ls) for yk, ls in ys.items()} for k, ys in own_lots.items()}
    item_kg = {f: {yk: sum(d["kg"] for d in ls) for yk, ls in ys.items()} for f, ys in item_lots.items()}
    plan = pd.read_excel(result_file, sheet_name="PLAN")
    floor_idx = tl.today_idx + int(freeze_weeks)
    last_idx = len(tl.keys) - 1
    used: dict = {}
    item_used: dict = {}
    cons = plan_consumption(plan, ylines, tl.idx, floor_idx, own=own_kg, used=used, item=item_kg, item_used=item_used)
    cands, audit = build_candidates(demand, plan, ylines, tl, floor_idx, lag, last_idx)
    for c in cands:
        c["item"] = _fab_of(c["key"])
        o = own_kg.get(c["key"])
        if o:
            c["own"] = {yk: {"kg": kg, "used": used.get(c["key"], {}).get(yk, {})} for yk, kg in o.items()}
            c["row"]["LOCK"] = " · ".join(f"{key_label(yk)}: {lots_text([(d['lot'], d['kg']) for d in ls])}"
                                          for yk, ls in own_lots[c["key"]].items())
        if c["item"] in item_lots:
            c["row"]["LOCK_ITEM"] = " · ".join(f"{key_label(yk)}: {lots_text([(d['lot'], d['kg']) for d in ls])}"
                                               for yk, ls in item_lots[c["item"]].items())
    borrow = select_borrowers(cands, cons, usable, floor_idx, item=_item_acc(item_kg, item_used))
    order = [c["key"] for c in sorted(cands, key=lambda c: (not c["color"], c["due"], str(c["key"])))]
    alloc = allocate_lots(order, borrow, lots, _own_left(own_lots, used), _own_left(item_lots, item_used))
    picks, pend_of = {}, {}
    for c in cands:
        k = c["key"]
        if k in alloc:
            picks[k] = note_of(alloc[k], c["pend"])
            pend_of[k] = c["pend"]
            c["row"]["RESULT"] = picks[k]
            c["row"]["LOTS"] = " · ".join(f"{key_label(yk)}: {lots_text(ls)}" for yk, ls in alloc[k].items())
        else:
            short = [key_label(yk) for yk, (kg, end) in c["needs"].items()
                     if end >= floor_idx and usable.get(yk, 0.0) + own_kg.get(k, {}).get(yk, 0.0)
                     + item_kg.get(c["item"], {}).get(yk, 0.0) < kg]
            c["row"]["RESULT"] = ("สต๊อกไม่พอ: " + ", ".join(short)) if short else \
                "สต๊อกพอแต่แผน/ใบที่มาก่อนใช้ไปแล้ว — รอ PO IN"
        audit.append(c["row"])
    borrow = {k: v for k, v in borrow.items() if k in alloc}
    return {"picks": picks, "borrow": borrow, "pend": pend_of, "lots": alloc, "audit": pd.DataFrame(audit),
            "stock_at": stock_at, "usable": usable, "ylines": ylines, "floor_idx": floor_idx,
            "last_idx": last_idx, "warn": warn, "own_kg": own_kg, "item_kg": item_kg}


def _item_acc(item_kg: dict, item_used: dict) -> dict:
    """ล็อก Item {ผ้า: {คีย์ด้าย: kg}} + ที่แผนกินไปแล้ว → บัญชีของ _ItemPool"""
    return {f: {yk: {"kg": kg, "used": item_used.get(f, {}).get(yk, {})} for yk, kg in ys.items()}
            for f, ys in item_kg.items()}


def mark(demand: pd.DataFrame, picks: dict) -> pd.DataFrame:
    """ติดป้าย YARN_STOCK ให้แถวดีมานด์ที่มีวันด้ายเข้าของใบที่ยืมได้ (Planning: ไม่ต้องรอ PO IN)"""
    out = demand.copy()
    keys = list(zip(out["SO_NO"].map(_so), out["FABRIC_ITEM"].map(_txt).str.upper()))
    has_pt = pd.to_datetime(out.get("POINT_DATE"), errors="coerce").notna()
    out[NOTE_COL] = [picks.get(k, "") if h else "" for k, h in zip(keys, has_pt)]
    return out


def verify(result_file: Path, cover: dict, tl, lag: int) -> list:
    """ตรวจแผนรอบสอง: ด้ายที่ยืมไปต้องไม่ทำให้คงเหลือติดลบ → รายการข้อความเตือน ([] = ผ่าน)

    ใบที่ยืม: เส้นที่แทน PO ถือยอดไว้ตั้งแต่สัปดาห์แรกที่วางจริงจนก่อน PO ของตัวเองใช้ได้
    ส่วนที่ติดลบอยู่แล้วโดยไม่เกี่ยวกับการยืม (แผนกินเกินสต๊อกเอง) ไม่นับเป็นความผิดของรอบนี้
    ใบที่มีด้ายล็อก: ถือเฉพาะส่วนที่ล็อกไม่พอ (ล็อกที่เหลือหลังแผนรอบสองกินแล้ว ใช้ก่อน — ล็อกของใบ แล้วล็อก Item)"""
    plan = pd.read_excel(result_file, sheet_name="PLAN")
    floor_idx, last_idx = cover["floor_idx"], cover["last_idx"]
    own_kg = cover.get("own_kg") or {}
    item_kg = cover.get("item_kg") or {}
    used: dict = {}
    item_used: dict = {}
    cons = plan_consumption(plan, cover["ylines"], tl.idx, floor_idx, own=own_kg, used=used,
                            item=item_kg, item_used=item_used)
    pool = _ItemPool(_item_acc(item_kg, item_used))
    first = {}
    p = plan[plan["PLAN_SOURCE"].astype(str).str.upper() == "NEW"]
    for r in p.itertuples(index=False):
        k = (_so(r.SC_SO_NO), _txt(r.ITEM_CODE).upper())
        wi = tl.idx(r.PLAN_WEEK, r.PLAN_YEAR)
        if k in cover["borrow"] and wi is not None and float(r.PRODUCE_QTY or 0) > 0:
            first[k] = min(first.get(k, wi), wi)
    held: dict = {}
    for k, ys in cover["borrow"].items():
        if k not in first:
            continue
        for yk, kg in ys.items():
            pin = cover["pend"].get(k, {}).get(yk)
            if pin is None:
                continue
            pi = tl.idx_of_date(pin)
            end = min((pi if pi is not None else last_idx + 1) + lag - 1, last_idx)
            o = {"kg": own_kg[k][yk], "used": used.get(k, {}).get(yk, {})} if yk in own_kg.get(k, {}) else None
            for w in range(max(first[k], floor_idx), end + 1):
                need = _from_pool(o, kg, w)
                ti = min(need, pool.free(_fab_of(k), yk, w))
                pool.hold(_fab_of(k), yk, w, ti)
                held[(yk, w)] = held.get((yk, w), 0.0) + need - ti
    warn = []
    for yk in sorted({yk for yk, _w in held}):
        run = 0.0
        worst = None
        for w in range(floor_idx, last_idx + 1):
            run += cons.get(yk, {}).get(w, 0.0)
            before = cover["usable"].get(yk, 0.0) - run
            after = before - held.get((yk, w), 0.0)
            if after < -0.5 and before >= -0.5 and (worst is None or after < worst[1]):
                worst = (w, after)
        if worst:
            warn.append(f"{key_label(yk)} ติดลบ {-worst[1]:,.0f} kg ที่ {tl.at(worst[0])}")
    return warn


# =============================================================================
# แผนวางได้ไม่เกินด้ายที่มี = สต๊อก + PO Pending (Replan เรียกก่อนวางแผน)
#
# user 15 ต.ค. 2569: "มันต้องไม่มีติดลบ เราต้องวางแผนแค่ที่มีใน stock แล้วที่เหลือก็ไปตาม POIN"
#                   + "ต้องดู PO Pending ด้วย" + "local = นันยางผลิตเอง" (ไม่ต้องคุม)
# user เลือก: ด้ายพอบางส่วน = แตกใบ วางส่วนที่มีด้าย · ไม่มีด้ายและไม่มี PO = UNPLANNED ·
#            PO ที่เลยกำหนดแต่ยังค้างส่ง = นับว่าเข้า "สัปดาห์นี้ + lag" (ข้อ ก)
# user 16 ต.ค. 2569: ด้ายในประเทศ (Internal + Local) "เรียกใช้ได้เลย เขาหาด้ายมาให้เราได้ ทำแผนได้เลย"
#                   "แต่ถ้ายังไม่ถึงกำหนดเราต้องวางตาม POIN" → ไม่คุมด้ายในประเทศ (exempt_keys) ; คุมแค่ด้ายนำเข้า
#                   พื้นวันด้ายเข้าของบรรทัดที่รอ PO ตัวเองอยู่ที่ Planning (POINT_DATE) ไม่ได้อยู่ที่นี่ — ยังมีผลเหมือนเดิม
#
# ทำไมทำก่อนวาง (ไม่ใช่วาง 2 รอบ): แบ่งด้ายให้แต่ละใบตามลำดับ (งานสีก่อน แล้วกำหนดส่ง) แล้วให้ก้อนที่ได้ด้ายจาก PO
# วางไม่ก่อนสัปดาห์ที่ PO นั้นเข้า → ทุกก้อนใช้ด้ายที่ "มาถึงแล้ว" ณ สัปดาห์ที่วางเสมอ คงเหลือจึงไม่ติดลบ
# ไม่ว่า Planning จะเลื่อนงานไปหลังเท่าไหร่ (เลื่อนไปหลังได้อย่างเดียว ไม่ทำให้ใช้ด้ายก่อนของมา)
#
# คุมเฉพาะบรรทัดที่ "ไม่มี PO ของตัวเองกำลังมา" (บรรทัดที่รอ PO ของตัวเองอยู่แล้วโดนพื้นวันด้ายเข้าอยู่แล้ว)
# PO Pending ที่บรรทัดเหล่านั้นรออยู่ถูกกันไว้ให้ก่อน (ไม่นับซ้ำเป็นด้ายของใบอื่น)
# ข้อมูล PO: Yarn/view_poyarn_830.xlsx (View_PO_Yarn.py) — วันเข้าของ view นี้ = วันของ view SCFOL_YARN + 7–14 วัน
# เกือบทุกคู่ (ตรวจ 16 ต.ค. 5,633 คู่ มัธยฐาน +10 ทุกเดือน) = นับวันคนละแบบ ไม่ใช่การเลื่อน PO
# =============================================================================
PO_FILE = BASE_DIR / "Yarn" / "view_poyarn_830.xlsx"
READY_COL = "YARN_READY"     # วันแรกของสัปดาห์ที่ด้ายของก้อนนี้มาครบ — Planning ห้ามวางก่อน (ว่าง = ด้ายมีแล้ว)
SUPPLY_COL = "YARN_SUPPLY"   # ข้อความบอกว่าก้อนนี้ได้ด้ายจากไหน / ทำไมถูกแตก
SHORT_REASON = "ด้ายไม่พอ (สต๊อก + PO Pending)"
_INTERNAL_DAYS = 365
_MIN_KG = 1.0                # ก้อนผ้าที่เล็กกว่านี้ไม่แตกออกมา (รวมกับก้อนข้างเคียง)


def load_po(path=None) -> pd.DataFrame:
    """view PO ด้าย → DataFrame : PO, ITEM (ด้าย), COLOR, ORDER, REC, REMAIN, STATUS, PIN, DATE, SUPPLIER, CUR (สกุลเงิน)
    ไม่มีไฟล์ / คอลัมน์ไม่ครบ = DataFrame ว่าง (ผู้เรียกถือว่าไม่มี PO Pending)"""
    path = Path(path) if path is not None else PO_FILE
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip().str.upper()
    if not {"PO_NO", "ITEM_CODE", "ORDER_QTY", "REC_QTY", "PO_STATUS", "PO_IN_DATE", "YARN_SUPPLIER"} <= set(df.columns):
        return pd.DataFrame()
    out = pd.DataFrame({
        "PO": df["PO_NO"].map(_txt).str.upper(),
        "ITEM": df["ITEM_CODE"].map(_txt).str.upper(),
        "COLOR": df["COLOR_TD"].map(color_of) if "COLOR_TD" in df.columns else "",
        "ORDER": pd.to_numeric(df["ORDER_QTY"], errors="coerce").fillna(0.0),
        "REC": pd.to_numeric(df["REC_QTY"], errors="coerce").fillna(0.0),
        "STATUS": df["PO_STATUS"].map(_txt).str.upper(),
        "PIN": pd.to_datetime(df["PO_IN_DATE"], errors="coerce").dt.normalize(),
        "DATE": pd.to_datetime(df["PO_DATE"], errors="coerce") if "PO_DATE" in df.columns else pd.NaT,
        "SUPPLIER": df["YARN_SUPPLIER"].map(_txt),
        "CUR": df["CURRENCY_CODE"].map(_txt).str.upper() if "CURRENCY_CODE" in df.columns else "",
    })
    out["REMAIN"] = (out["ORDER"] - out["REC"]).clip(lower=0.0)
    return out


def exempt_keys(po: pd.DataFrame, keywords, local: bool = False, today=None, keys=None) -> dict:
    """(ด้าย, สี) ที่แผนไม่คุมตามด้ายที่มี → {คีย์: "Internal" | "Local"}

    Internal = ยอดสั่ง PO ≥ ครึ่งหนึ่งมาจากผู้ขายที่ชื่อมีคำใน keywords (นันยางผลิตเอง — user 15 ต.ค. 2569)
    Local    = (local เปิด) ยอดสั่ง PO ≥ ครึ่งหนึ่งเป็นเงินบาท = ผู้ขายในประเทศ (user 16 ต.ค. 2569 — ดูหัว section)
               PO ค้าง 16 ต.ค.: บาท = Internal/Local ทุกใบ, USD = Import ทุกใบ (ตรงกับ VENDOR_TYPE ของคลัง)
    ดู PO 1 ปีล่าสุด ; คีย์ที่ไม่มี PO ในช่วงนั้น → ดูทั้ง view (ย้อนถึงปี 2566) ; ไม่เคยมี PO = คุม
    นับต่อ (ด้าย, สี) ไม่ใช่ต่อชื่อด้าย — ด้าย TD แต่ละสีซื้อคนละที่ได้ (CTD20/1R สี B09B/B65 ซื้อ Brotex USD ทั้งหมด
    มีแค่สี H120R ที่ซื้อในไทย : นับรวมชื่อด้ายบาทเกินครึ่ง สีนำเข้าจะหลุดไม่ถูกคุม)
    keys = คีย์ที่ใบใช้ (ไม่ส่งได้) — ด้ายสีที่ไม่เคยมี PO สีนั้นเลยแต่มี PO ด้ายดิบตัวเดียวกัน ใช้ป้ายของด้ายดิบ
           (BCI32/1RCP สี B50 ฯลฯ: PO มีแต่ด้ายดิบจากนันยาง — ก่อนแยกตามสีไม่คุม ต้องคงเดิม)
    ใช้ผู้ขายของ PO ไม่ใช่ VENDOR_TYPE ของ lot ในสต๊อก — lot ที่ค้างคลังเป็นด้ายคืน/ซื้อเศษ ไม่บอกแหล่งหลัก
    (CVC24/1R: PO จากนันยาง 100% แต่ lot ที่เหลือในคลังเป็น Local มากกว่า)"""
    kws = [str(k).strip() for k in (keywords or []) if str(k).strip()]
    if po is None or po.empty or not (kws or local):
        return {}
    today = pd.Timestamp(today).normalize() if today is not None else pd.Timestamp.today().normalize()
    q = po["ORDER"]
    own = po["SUPPLIER"].map(lambda s: any(k in s for k in kws))
    thb = (po["CUR"] == "THB") if local and "CUR" in po.columns else pd.Series(False, index=po.index)
    p = pd.DataFrame({"Y": po["ITEM"], "C": po["COLOR"].map(_txt), "Q": q,
                      "Q_OWN": q.where(own, 0.0), "Q_THB": q.where(thb, 0.0)})
    cols = ["Q", "Q_OWN", "Q_THB"]
    recent = (po["DATE"] >= today - pd.Timedelta(days=_INTERNAL_DAYS) if po["DATE"].notna().any()
              else pd.Series(True, index=po.index))
    g1 = p[recent].groupby(["Y", "C"])[cols].sum()
    g1 = g1[g1["Q"] > 0]
    gall = p.groupby(["Y", "C"])[cols].sum()
    gall = gall[gall["Q"] > 0]
    g2 = gall[~gall.index.isin(g1.index)]
    out = {}
    for k, r in pd.concat([g1, g2]).iterrows():
        if r["Q_OWN"] / r["Q"] >= 0.5:
            out[k] = "Internal"
        elif r["Q_THB"] / r["Q"] >= 0.5:
            out[k] = "Local"
    seen = set(gall.index)
    for y, c in keys or ():
        if c and (y, c) not in seen and (y, "") in out:
            out[(y, c)] = out[(y, "")]
    return out


def pending_events(po: pd.DataFrame, today, lag: int, exempt, reserve: list) -> list:
    """PO Pending → [{"yk", "kg", "po", "pin", "late", "avail"}] หลังกันยอดให้บรรทัดที่รอ PO นั้นอยู่
    PO Pending = STATUS OPEN และ สั่ง − รับ > 0.5 kg ; คีย์ (ด้าย, สี) ใน exempt ไม่นับ (ไม่คุมอยู่แล้ว — exempt_keys)
    avail = วันที่เริ่มใช้ได้ = PO_IN_DATE + lag สัปดาห์ (กติกาวันด้ายเข้าเดิม) ;
            เลยกำหนดแล้วแต่ยังค้างส่ง → นับว่าเข้าวันนี้ + lag (user เลือกข้อ ก 15 ต.ค. 2569)
    reserve = [(เลข PO ที่บรรทัดรอ, คีย์ด้าย, kg ที่บรรทัดนั้นต้องใช้)] — หักออกจาก PO นั้นก่อน (ใบที่รอ PO ของตัวเอง)"""
    if po is None or po.empty:
        return []
    today = pd.Timestamp(today).normalize()
    ex = set(exempt or ())
    p = po[(po["STATUS"] == "OPEN") & (po["REMAIN"] > 0.5)]
    evs = []
    for r in p.itertuples(index=False):
        yk = (r.ITEM, r.COLOR or "")
        if yk in ex:
            continue
        pin = r.PIN if pd.notna(r.PIN) else today
        late = pin < today
        evs.append({"yk": yk, "kg": float(r.REMAIN), "po": r.PO, "pin": pin, "late": late,
                    "avail": max(pin, today) + pd.Timedelta(weeks=int(lag))})
    by_po: dict = {}
    for e in evs:
        by_po.setdefault((e["po"], e["yk"]), []).append(e)
    for pos, yk, kg in reserve:
        left = kg
        for pn in pos:
            for e in sorted(by_po.get((str(pn).upper(), yk), []), key=lambda e: e["avail"]):
                q = min(e["kg"], left)
                e["kg"] -= q
                left -= q
                if left <= 1e-9:
                    break
            if left <= 1e-9:
                break
    return [e for e in evs if e["kg"] > 0.5]


def allocate_supply(orders: list, events: dict, floor_idx: int, item_pool: dict = None) -> dict:
    """แบ่งด้ายให้ใบตามลำดับ งานสีก่อน แล้วกำหนดส่ง (ฟังก์ชันล้วน — เทสต์ที่ tests/test_yarn_supply.py)

    orders = [{"key", "color": bool, "due", "needs": {คีย์ด้าย: kg}, "own": {คีย์ด้าย: kg ล็อกของใบ}, "item": ผ้า}]
    events = {คีย์ด้าย: [[idx สัปดาห์ที่ใช้ได้, kg, ป้าย]]} เรียงตาม idx — ถูกหักตามที่แบ่งไป (แก้ในที่)
             สต๊อกใส่เป็นเหตุการณ์ที่ floor_idx ; ล็อกของใบใช้ก่อนเสมอ (ไม่อยู่ใน events)
    item_pool = ด้ายที่ล็อกให้ Item {ผ้า: {คีย์ด้าย: kg}} — ใบของผ้านั้นใช้ร่วมกัน ต่อจากล็อกของใบ ก่อน events
                (นับว่ามีแล้วตอนนี้ ; ถูกหักตามที่แบ่งไป — แก้ในที่) ; ใบของผ้าอื่นไม่เห็น
    → {key: {"f": สัดส่วนของใบที่มีด้าย, "fa": สัดส่วนที่ด้ายมีแล้วตอนนี้ (≤ floor_idx), "ready": idx ที่ด้ายส่วนที่เหลือมาครบ,
             "src": {คีย์ด้าย: [(ป้าย, kg)]}, "short": {คีย์ด้าย: kg ที่ขาด}}}
    ใบต้องได้ด้ายทุกเส้นในสัดส่วนเดียวกัน (ผ้าทอได้เท่าเส้นที่น้อยสุด) — เส้นอื่นแบ่งไปเท่าที่ใช้จริง ไม่กักเกิน"""
    out: dict = {}
    for o in sorted(orders, key=lambda o: (not o["color"], o["due"], str(o["key"]))):
        needs = {y: kg for y, kg in o["needs"].items() if kg > 1e-6}
        if not needs:
            continue
        own = o.get("own") or {}
        pool = (item_pool or {}).get(o.get("item") or "", {})
        have = {y: own.get(y, 0.0) + pool.get(y, 0.0) + sum(e[1] for e in events.get(y, [])) for y in needs}
        f = max(0.0, min([1.0] + [have[y] / kg for y, kg in needs.items()]))
        src, fa, ready = {}, f, None
        for y, kg in needs.items():
            want, now, take, last = kg * f, 0.0, [], None
            t = min(own.get(y, 0.0), want)
            if t > 1e-9:
                take.append(("ล็อก", t))
                now += t
                want -= t
            t = min(pool.get(y, 0.0), want)
            if t > 1e-9:
                take.append(("ล็อก Item", t))
                pool[y] -= t
                now += t
                want -= t
            for e in events.get(y, []):
                if want <= 1e-9:
                    break
                q = min(e[1], want)
                if q <= 1e-9:
                    continue
                e[1] -= q
                want -= q
                take.append((e[2], q))
                if e[0] <= floor_idx:
                    now += q
                else:
                    last = e[0] if last is None else max(last, e[0])
            src[y] = take
            fa = min(fa, now / kg)
            if last is not None:
                ready = last if ready is None else max(ready, last)
        if f - fa <= 1e-9:
            ready = None
        out[o["key"]] = {"f": f, "fa": fa, "ready": ready, "src": src,
                         "short": {y: kg - have[y] for y, kg in needs.items() if have[y] < kg - 1e-6}}
    return out


def split_rows(rows: list, qa: float, qb: float) -> tuple:
    """แถวดีมานด์ของใบ (เรียงตามกำหนดส่งแล้ว) → (ก้อนด้ายมีแล้ว, ก้อนรอ PO, ก้อนไม่มีด้าย)
    แถวที่กำหนดส่งเร็วได้ด้ายก่อน ; แถวที่คร่อมเส้นแบ่งถูกแตกเป็น 2 แถว (ยอด PENDING_PLAN)"""
    a, b, u = [], [], []
    left = [max(qa, 0.0), max(qb, 0.0)]
    for r in rows:
        q = float(r["PENDING_PLAN"])
        for bucket, i in ((a, 0), (b, 1)):
            if q <= 1e-9 or left[i] <= 1e-9:
                continue
            t = min(q, left[i])
            bucket.append({**r, "PENDING_PLAN": round(t, 2)})
            left[i] -= t
            q -= t
        if q > 1e-9:
            u.append({**r, "PENDING_PLAN": round(q, 2)})
    return a, b, u


def _order_key(r) -> tuple:
    sc = _sc_digits(r.get("SC_NO")) or _so(r.get("SO_NO"))
    fab = _txt(r.get("FABRIC_ITEM")) or _txt(r.get("ITEM_CODE"))
    return sc, fab.upper()


def _due_of(r) -> int:
    for c in ("FGWEEK", "BOOKING_WEEK_FROM"):
        try:
            v = int(float(r.get(c)))
            if v > 100000:
                return v
        except (TypeError, ValueError):
            continue
    return 999999


def supply_cap(demand: pd.DataFrame, tl, freeze_weeks: int, lag: int, exempt_kw=("นันยาง",), today=None,
               ylines=None, po=None, stock=None, color_keys=None, local: bool = False) -> dict:
    """ดีมานด์รอบนี้ → ดีมานด์ที่แผนวางได้ไม่เกินด้ายที่มี (ดูหัวข้อ section นี้)
    exempt_kw / local = ด้ายที่ไม่คุม (exempt_keys) : ผู้ขายตามรายชื่อ (Internal) / PO เงินบาท (Local)

    คืน {"demand": DataFrame ใหม่ (แตก/ตัดแล้ว + คอลัมน์ YARN_READY / YARN_SUPPLY),
         "short": DataFrame แถวที่ไม่มีด้าย (REASON) → ชีท YARN_SHORT → Planning ลง UNPLANNED,
         "audit": DataFrame ต่อใบ (ชีท YARN_SUPPLY), "stats", "note"}
    ylines / po / stock (= usable_stock() ทั้งชุด) / color_keys ส่งมาได้ตอนทดสอบ ; ไม่ส่ง = อ่านจากไฟล์"""
    today = pd.Timestamp(today).normalize() if today is not None else pd.Timestamp.today().normalize()
    ylines = load_yarn_lines(today=today) if ylines is None else ylines
    if not ylines:
        return {"demand": demand, "note": f"ไม่มีไฟล์ {SCFOL_FILE.name} (View_SCFol_Yarn) — ไม่รู้ว่าแต่ละบรรทัดใช้ด้ายอะไร"}
    po = load_po() if po is None else po
    if po.empty:
        return {"demand": demand, "note": f"ไม่มีไฟล์ {PO_FILE.name} (View_PO_Yarn) — ไม่รู้ PO Pending จึงยังไม่คุม"}
    _lots, usable, stock_at, warn, own, *rest = usable_stock() if stock is None else stock
    item_lots = rest[0] if rest else {}
    if color_keys is None:
        import ColorOpen
        color_keys = set(ColorOpen.quota_by_sc_item())
    exempt = exempt_keys(po, exempt_kw, local, today, keys={yk for ys in ylines.values() for yk in ys})
    floor_idx = tl.today_idx + int(freeze_weeks)
    last_idx = len(tl.keys) - 1

    # ดีมานด์ → ใบ (SC + ผ้า) ; ด้ายของบรรทัดที่ไม่มี PO ตัวเองกำลังมา = ต้องใช้จากด้ายที่มี
    d = demand.reset_index(drop=True)
    qty = pd.to_numeric(d.get("PENDING_PLAN"), errors="coerce").fillna(0.0)
    orders: dict = {}
    reserve = []
    for i, r in d[qty > 0].iterrows():
        rr = r.to_dict()
        rr["PENDING_PLAN"] = float(qty[i])
        k = _order_key(rr)
        if not k[0] or not k[1]:
            continue
        o = orders.setdefault(k, {"key": k, "color": k in color_keys, "due": 10 ** 6, "needs": {}, "rows": []})
        o["rows"].append((i, rr))
        o["due"] = min(o["due"], _due_of(rr))
        lines = lines_of(rr.get("SC_LINE_ID"))
        if not lines:
            continue
        share = rr["PENDING_PLAN"] / len(lines)
        for ln in lines:
            for yk, info in ylines.get(ln, {}).items():
                if yk in exempt:
                    continue
                kg = share * info["pct"] / 100.0 * info["loss"]
                if kg <= 0:
                    continue
                if info["po_in"] is not None:
                    reserve.append((info.get("pos") or [], yk, kg))     # รอ PO ของตัวเอง — กัน PO นั้นไว้ให้
                else:
                    o["needs"][yk] = o["needs"].get(yk, 0.0) + kg

    # ด้ายที่มี: สต๊อก (กองกลาง หักล็อกแล้ว) ที่สัปดาห์แรกที่วางได้ + PO Pending ตามสัปดาห์ที่ใช้ได้
    events: dict = {}
    for yk, kg in usable.items():
        if kg > 0.5 and yk not in exempt:
            events.setdefault(yk, []).append([floor_idx, float(kg), "สต๊อก"])
    for e in pending_events(po, today, lag, exempt, reserve):
        i = tl.idx_of_date(e["avail"])
        if i is None:
            if e["avail"] > pd.Timestamp(tl.ends[-1]):
                continue                     # เข้าหลังปฏิทิน — วางในช่วงแผนไม่ได้
            i = floor_idx
        tag = f"PO {e['po']} " + (f"เลยกำหนด {e['pin']:%d/%m} (นับว่าเข้าสัปดาห์หน้า)" if e["late"]
                                   else f"เข้า {e['pin']:%d/%m/%Y}")
        events.setdefault(e["yk"], []).append([max(i, floor_idx), e["kg"], tag])
    for evs in events.values():
        evs.sort(key=lambda e: e[0])
    own_kg = {}
    for (sc, fab), ys in (own or {}).items():
        own_kg[(sc, fab)] = {yk: sum(x["kg"] for x in ls) for yk, ls in ys.items()}
    # ล็อก Item (หน้า Monitor Yarn) — ทุกใบของผ้านั้นใช้ร่วมกัน ; กองกลาง (usable) หักออกไปแล้ว ผ้าอื่นจึงไม่ได้
    item_pool = {f: {yk: sum(x["kg"] for x in ls) for yk, ls in ys.items()} for f, ys in (item_lots or {}).items()}
    item_total = round(sum(sum(ys.values()) for ys in item_pool.values()), 1)
    olist = [{**o, "own": own_kg.get(k, {}), "item": k[1]} for k, o in orders.items() if o["needs"]]
    alloc = allocate_supply(olist, events, floor_idx, item_pool)

    # แตกแถวตามผลแบ่งด้าย — แถวใหม่ของใบวางแทนที่ตำแหน่งแถวแรกของใบ (ลำดับดีมานด์เดิมไม่เปลี่ยน)
    short, audit = [], []
    repl: dict = {}                          # index แถวแรกของใบ → แถวใหม่ทั้งหมดของใบ
    drop: set = set()                        # index แถวเดิมของใบที่ถูกแตก
    stats = {"orders": len(olist), "full": 0, "split": 0, "wait": 0, "short_orders": 0, "short_kg": 0.0, "wait_kg": 0.0}
    for k, o in orders.items():
        a = alloc.get(k)
        if a is None:
            continue
        rows = [rr for _i, rr in sorted(o["rows"], key=lambda t: (_due_of(t[1]), t[0]))]
        tot = sum(rr["PENDING_PLAN"] for rr in rows)
        qa, qb = tot * a["fa"], tot * (a["f"] - a["fa"])
        if qb < _MIN_KG:
            qa, qb = qa + qb, 0.0
        if tot - qa - qb < _MIN_KG:
            qb = tot - qa if qb > 0 else 0.0
            qa = tot - qb
        if qa >= tot - 1e-6:
            stats["full"] += 1
            continue                         # ด้ายมีพอทั้งใบ — ไม่แตะ
        ra, rb, ru = split_rows(rows, qa, qb)
        src = " · ".join(f"{key_label(y)}: " + " + ".join(f"{t} {q:,.0f} kg" for t, q in ts)
                         for y, ts in a["src"].items())
        ready_txt = ""
        if rb and a["ready"] is not None:
            ready_txt = f"W{tl.keys[a['ready']] % 100}/{tl.keys[a['ready']] // 100}"
            rdate = tl.starts[a["ready"]]
            for rr in rb:
                rr[READY_COL] = rdate
                rr[SUPPLY_COL] = f"รอด้าย: วางได้ตั้งแต่ {ready_txt} ({src})"
            stats["wait"] += 1
            stats["wait_kg"] += qb
        for rr in ra:
            rr[SUPPLY_COL] = f"ด้ายมีแล้ว: {src}"
        if ru:
            why = SHORT_REASON + ": " + " · ".join(f"{key_label(y)} ขาด {kg:,.0f} kg" for y, kg in a["short"].items())
            for rr in ru:
                rr["REASON"] = why
            short += ru
            stats["short_orders"] += 1
            stats["short_kg"] += sum(rr["PENDING_PLAN"] for rr in ru)
        if ra and (rb or ru):
            stats["split"] += 1
        idxs = [i for i, _r in o["rows"]]
        drop.update(idxs)
        repl[min(idxs)] = ra + rb
        audit.append({"SC": k[0], "ITEM": k[1], "COLOR": "Yes" if o["color"] else "", "DUE": o["due"],
                      "QTY": round(tot, 2), "NOW": round(qa, 2), "WAIT_PO": round(qb, 2),
                      "READY_WEEK": ready_txt, "SHORT": round(tot - qa - qb, 2),
                      "YARN": src, "SHORT_YARN": ", ".join(f"{key_label(y)} {kg:,.0f}" for y, kg in a["short"].items())})
    if drop:
        recs = d.to_dict("records")
        out_rows = []
        for i, rr in enumerate(recs):
            if i in repl:
                out_rows += repl[i]
            if i not in drop:
                out_rows.append(rr)
        new = pd.DataFrame(out_rows, columns=list(d.columns) + [c for c in (READY_COL, SUPPLY_COL)
                                                                if c not in d.columns])
    else:
        new = d
    stats["exempt_yarns"] = len(exempt)
    stats["exempt_local"] = sum(1 for v in exempt.values() if v == "Local")
    stats["item_lock_kg"] = item_total            # ด้ายที่ล็อกให้ Item (กันได้จริง) ทั้งหมด
    stats["item_lock_used"] = round(sum(q for a in alloc.values() for ts in a["src"].values()
                                        for t, q in ts if t == "ล็อก Item"), 1)
    return {"demand": new, "short": pd.DataFrame(short), "audit": pd.DataFrame(audit), "stats": stats,
            "stock_at": stock_at, "note": warn or ""}

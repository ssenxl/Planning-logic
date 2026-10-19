"""
replan_notify.py — สรุป "แผนเปลี่ยนอะไรบ้างในรอบ Replan" แล้วประกอบเป็นเนื้ออีเมล

คำถามที่โมดูลนี้ตอบ: งานที่เคยวางไว้ W10 ตอนนี้ไปอยู่ไหน ยอดเปลี่ยนไหม จบเร็วหรือช้ากว่าเดิม
**และเพราะอะไร** — ตอบเป็นราย item + ใบสั่ง (SC) ไม่ใช่ตัวเลขรวมทั้งแผน

**แหล่งข้อมูล = ชีทในไฟล์แผนล่าสุด ไม่ใช่การคำนวณใหม่**
  REPLAN          แถวแผนที่ Replan วางใหม่ — มี BOOKING_WEEK_FROM/TO = สัปดาห์เดิมที่ booking
                  จองไว้ (Replan.py:1225 groupby min/max) จึงไม่ต้องเดาจับคู่เก่า↔ใหม่เอง
                  และมีคอลัมน์ที่ใช้อธิบายเหตุผลได้จริง (PO_IN_WEEK, EARLIEST_PLAN_WEEK,
                  CYLINDER_CHANGE, OUTSOURCE, S9_ROUTING, PLAN_SOURCE, RDD_WEEK)
  REPLAN_COMPARE  long format ROW_KIND=OLD/NEW ต่อ (item, SC, สัปดาห์) — ใช้เป็น "ยอดเดิม"
                  และเป็นทางเดียวที่เห็นงานที่ **หลุดจากแผนใหม่ทั้งก้อน** (มี OLD ไม่มี NEW)

**จับคู่เก่า↔ใหม่ด้วย connected component ของ SC ไม่ใช่เทียบข้อความ SC ตรง ๆ**
แถวหนึ่งยุบได้หลายใบ และฝั่งเก่า/ใหม่ยุบไม่เหมือนกัน (เจอจริง: OLD='719035,721247,721142'
แต่ NEW='721142,721384') ถ้าเทียบข้อความจะกลายเป็น "ก้อนเดิมหายไป + ก้อนใหม่โผล่มา" ทั้งที่
เป็นงานเดียวกันที่ถูกจัดกลุ่มใหม่ → มองเป็นกราฟ: ใบที่อยู่แถวเดียวกัน = เชื่อมกัน,
ทุกใบที่เชื่อมถึงกันภายใน item เดียวกัน = งานก้อนเดียว 1 รายการในเมล
(แถวที่ไม่มีเลข SC เลยรวมเป็นถัง '—' ของ item นั้น — ห้ามยุบรวมกับก้อนที่มี SC เพราะ
จะทำให้ยอดของงานคนละใบมากองรวมกัน)

**เทียบสัปดาห์บนแกนเวลาจริง ไม่ใช่เลขสัปดาห์เปล่า** — ทั้ง pipeline เก็บ WEEK แบบไม่มีปี
(ดู [[multiyear_calendar_bareweek]]) ถ้าลบเลขกันตรง ๆ งานที่เลื่อนจาก W51 ไป W2 ของปีหน้า
จะกลายเป็น "เร็วขึ้น 49 สัปดาห์" → แปลงเป็น (ปี, สัปดาห์) ด้วย plan_view.week_year_map()
แล้วค่อยวัดระยะห่างเป็นจำนวนสัปดาห์จริง
"""
import json
import re
import sys
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path

import config
import mail_recipients
import plan_view

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน — ไฟล์อยู่ราก repo)

_SHEET_PLAN = "PLAN"
_SHEET_PLAN_LEGACY = "REPLAN"   # ไฟล์แผนรุ่นเก่า (ก่อน 2026-09-11) ที่ Planning รอบหลักเขียน
_SHEET_COMPARE = "REPLAN_COMPARE"

# ยอดต่างกันไม่เกินเท่านี้ (กก.) ถือว่าเท่าเดิม — ตรงกับ QTY_TOL ของหน้าเว็บ (KnitPlan.jsx)
QTY_TOL = 1.0

# สัปดาห์ที่แก้แผนไม่ได้ = สัปดาห์ปัจจุบัน + ค่านี้ — ใช้เป็นเหตุผล "สัปดาห์เดิมล็อกไปแล้ว"
# อ่านจากหน้า "ตั้งค่าระบบ" ชุดเดียวกับ Planning.FREEZE_WEEKS / Gantt / advisor อื่น
# (ห้ามเขียนเลขไว้เอง ไม่งั้นเมลอธิบายเหตุผลด้วยเส้นล็อกที่ไม่ตรงกับแผนจริง)
def freeze_weeks() -> int:
    plan_config.reload()
    return int(plan_config.get("KP_FREEZE_WEEKS"))

# จำนวนรายการสูงสุดในตารางของเนื้อเมล — 0 = ให้ระบบตัดเอาพอดีเพดานขนาด (ค่าที่ใช้จริง)
# **ทุกรายการยังถูกแจ้งครบเสมอ** ตามที่ user สั่ง เพราะรายละเอียดทั้งหมดอยู่ในไฟล์ Excel ที่แนบไป
# ตารางในเนื้อเมลเป็นแค่ "ตัวที่ต้องดูก่อน" เรียงตามความสำคัญ (เพิ่งเกิน RDD → เลื่อนไกลสุด)
MAX_ROWS = int(__import__("os").environ.get("KP_MAIL_MAX_ROWS", "0") or 0)

# เพดานขนาด payload ที่ยิงเข้า flow (ไบต์)
#
# **วัดจริงกับ flow ปัจจุบัน: payload 257 KB ส่งถึงผู้รับปกติ** → ไม่ได้ตั้งไว้เพราะข้อจำกัดของ
# Power Automate แต่กันไว้ไม่ให้เมลบวมเกินจำเป็นเท่านั้น
# (ประวัติที่ทำให้เข้าใจผิด: เมล 207 KB ฉบับแรกไม่ถึงผู้รับ ตอนนั้นสรุปว่า "ใหญ่เกิน" แต่สาเหตุจริง
#  คือ flow เก่าล้มที่ action โพสต์การ์ด Teams ก่อนถึงขั้นส่งเมล — ทุกฉบับที่ไม่มีการ์ดไม่ถึงเลย
#  ไม่ว่าขนาดเท่าไร ; flow ปัจจุบันไม่มี action นั้นแล้ว)
MAX_MAIL_BYTES = int(__import__("os").environ.get("KP_MAIL_MAX_BYTES", "240000") or 240000)

_LOG = config.OUTPUT_DIR / "replan_notify_log.jsonl"


# ── ตัวช่วยอ่านค่า ────────────────────────────────────────────────────────────

def _i(v):
    """ค่าเซลล์ → int ; ว่าง/NaN/อ่านไม่ออก = None (ห้ามคืน 0 — สัปดาห์ 0 ไม่มีจริง
    แล้วจะกลายเป็นงานที่ 'เคยวางไว้ W0' ปนขึ้นมาในเมล)"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:                       # NaN
        return None
    return int(f)


def _f(v) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if f != f else f


def _s(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat") else s


def _flag(v) -> bool:
    """คอลัมน์ธงในชีทแผน "มีค่าจริง" หรือไม่

    ห้ามใช้ `if _s(v)` เฉย ๆ — คอลัมน์อย่าง S9_ROUTING ทั้งคอลัมน์เป็น 0.0 (ตัวเลข ไม่ใช่ว่าง)
    เมื่อไม่มีงานเข้าเงื่อนไขเลย ซึ่ง str() แล้วได้ '0.0' = truthy → เมลจะขึ้นเหตุผล
    "เข้าเส้นทางจ้างทอ S9 (0.0)" ให้ทุกแถวในแผน (เจอจริงตอนทดสอบรอบแรก)
    """
    s = _s(v).upper()
    if s in ("", "0", "0.0", "-", "—", "NO", "N", "FALSE", "NAN", "NONE"):
        return False
    try:
        return float(s) != 0
    except ValueError:
        return True


_SC_SPLIT = re.compile(r"[,\|/;]+")


def _sc_tokens(v) -> list:
    """'720624,721136' → ['720624','721136'] (คีย์ใบสั่งที่เทียบข้ามฝั่งได้)

    ต้องล้าง '.0' ด้วย — SO_NO ที่ผ่าน pandas มาเป็นตัวเลขจะกลายเป็น '720624.0'
    แล้วเทียบกับฝั่งที่เป็นข้อความไม่ติด (ดู [[so_no_float_text_trap]])"""
    out = []
    for t in _SC_SPLIT.split(_s(v)):
        t = t.strip().upper()
        if t.endswith(".0") and t[:-2].isdigit():
            t = t[:-2]
        if t and t not in ("-", "—"):
            out.append(t)
    return out


# ── แกนสัปดาห์ (ปี, สัปดาห์) ─────────────────────────────────────────────────

def current_year_week(ref: date = None) -> tuple:
    """(ปี, สัปดาห์) ของวันที่ ref ตามปฏิทินโรงงาน ศุกร์–พฤหัส (+3 วันก่อนคิด ISO week)"""
    iso = ((ref or date.today()) + timedelta(days=3)).isocalendar()
    return int(iso[0]), int(iso[1])


class WeekAxis:
    """แปลงเลขสัปดาห์เปล่า → (ปี, สัปดาห์) แล้ววัดระยะห่างเป็นจำนวนสัปดาห์จริง

    ปีของเลขสัปดาห์มาจาก plan_view.week_year_map() (กางหน้าต่าง ±26 สัปดาห์บนปฏิทินจริง)
    — อ่านปฏิทินไม่ได้ก็ยังทำงานต่อได้ โดยถอยไปเดาว่า "เลขน้อยกว่าสัปดาห์ปัจจุบันมาก = ปีหน้า"
    ซึ่งหยาบกว่าแต่ดีกว่าพังทั้งฟีเจอร์
    """

    def __init__(self, ref: date = None):
        """ref = วันที่ใช้เป็น "ปัจจุบัน" ; ปกติคือวันที่ไฟล์แผนถูกสร้าง ไม่ใช่วันนี้

        เหตุผลอย่าง "สัปดาห์เดิมอยู่ในช่วงล็อก" ต้องตัดสินด้วยเส้นล็อก **ตอนที่ Replan รัน**
        ไม่ใช่ตอนเปิดอ่านเมล — ไฟล์แผนที่รันไว้อาทิตย์ก่อน ถ้าใช้วันนี้เป็นฐาน สัปดาห์ที่ตอนนั้น
        ยังแก้ได้จะถูกอธิบายผิดว่า "ล็อกอยู่" ทั้งกอง (เจอจริงตอนทดสอบกับไฟล์แผนอายุ 8 วัน)
        """
        try:
            self._map = plan_view.week_year_map() or {}
        except Exception:
            self._map = {}
        self.ref = ref or date.today()
        self.cur_year, self.cur_week = current_year_week(self.ref)

    def year_of(self, week, year=None) -> int:
        """ปีของเลขสัปดาห์ — year ที่ส่งมา (เช่น PLAN_YEAR) ชนะเสมอ"""
        y = _i(year)
        if y and y > 2000:
            return y
        w = _i(week)
        if w is None:
            return self.cur_year
        y = self._map.get(w)
        if y:
            return int(y)
        # ไม่มีปฏิทิน: เลขที่ต่ำกว่าสัปดาห์ปัจจุบันเกิน 20 สัปดาห์ = ปีหน้า (แผนมองไปข้างหน้า)
        return self.cur_year + 1 if w < self.cur_week - 20 else self.cur_year

    def idx(self, week, year=None):
        """(ปี, สัปดาห์) → ลำดับสัปดาห์บนแกนเวลา (นับจาก 1970) ; None = ระบุสัปดาห์ไม่ได้"""
        w = _i(week)
        if w is None or not (1 <= w <= 53):
            return None
        y = self.year_of(w, year)
        try:
            d = date.fromisocalendar(y, w, 4)
        except ValueError:
            return None
        return d.toordinal() // 7

    def diff(self, w_from, w_to, y_from=None, y_to=None):
        """จำนวนสัปดาห์จาก w_from ไป w_to (บวก = เลื่อนออกไป) ; None = เทียบไม่ได้"""
        a, b = self.idx(w_from, y_from), self.idx(w_to, y_to)
        return None if (a is None or b is None) else b - a

    def edit_from(self) -> int:
        """สัปดาห์แรกที่แก้แผนได้ (สัปดาห์ปัจจุบัน + สัปดาห์ที่แตะไม่ได้) เป็นเลขสัปดาห์เปล่า"""
        d = date.fromisocalendar(self.cur_year, self.cur_week, 4) + timedelta(weeks=freeze_weeks())
        return int(d.isocalendar()[1])

    def is_frozen(self, week, year=None) -> bool:
        """สัปดาห์นี้อยู่ในช่วงล็อก (แก้แผนไม่ได้แล้ว) หรือไม่"""
        i = self.idx(week, year)
        cur = self.idx(self.cur_week, self.cur_year)
        return i is not None and cur is not None and i < cur + freeze_weeks()


def _po_key(v) -> tuple:
    """คีย์เรียงวันที่รูปแบบ dd/mm/yyyy ของชีทแผน (ค่าว่างอยู่ท้ายสุด)"""
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", _s(v))
    return (int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else (0, 0, 0)


def _rdd_year_week(v) -> tuple:
    """RDD_WEEK เก็บเป็น 202636 (ปี+สัปดาห์ติดกัน) หรือเลขสัปดาห์เปล่า → (ปี|None, สัปดาห์|None)"""
    n = _i(v)
    if n is None:
        return None, None
    if n > 100000:                   # 202636
        return n // 100, n % 100
    return None, n if 1 <= n <= 53 else None


# ── โหลดชีท ──────────────────────────────────────────────────────────────────

def _team_map() -> dict:
    """{(item, SC): ทีม} จากไฟล์ booking — ใช้เติมทีมให้แถวแผนที่ไม่มี

    **ทำไมต้องดึงจาก booking**: คอลัมน์ TEAM ในชีทแผนมีค่าแค่ ~9% ของแถว (วัดจริง 98/1092)
    เพราะดีมานด์ฝั่ง SC ไม่มีทีมติดมา (ดู [[team_data_sources]]) ส่วน booking มีเกือบครบ
    (1895/1909) → จับคู่ที่ (ITEM_CODE, SO_NO) แล้วเติมย้อนกลับ ครอบคลุม 305/314 รายการ
    คีย์ (item, "*") เป็นตัวสำรองระดับผ้า สำหรับรายการที่เลขใบไม่ตรงกันข้ามระบบ
    """
    try:
        import pandas as pd
        p = plan_view._latest_booking_path()
        if p is None:
            return {}
        det = pd.read_excel(p, sheet_name="DETAIL")
    except Exception:
        return {}
    out = {}
    for row in det.to_dict("records"):
        item = _s(row.get("ITEM_CODE")).upper()
        team = _s(row.get("TEAM_NAME"))
        if not item or not team:
            continue
        for s in _sc_tokens(row.get("SO_NO")):
            out.setdefault((item, s), team)
        out.setdefault((item, "*"), team)
    return out


def _read_sheets(path: Path):
    import pandas as pd
    x = pd.ExcelFile(path)
    # ไฟล์รุ่นเก่ามีชีท REPLAN แยก (ชีท PLAN = แผน SC ล้วน) ; รุ่นใหม่แผน replan อยู่ในชีท PLAN
    if _SHEET_PLAN_LEGACY in x.sheet_names:
        sheet = _SHEET_PLAN_LEGACY
    elif _SHEET_PLAN in x.sheet_names:
        sheet = _SHEET_PLAN
    else:
        raise FileNotFoundError(
            f"ไฟล์แผน {path.name} ไม่มีชีท {_SHEET_PLAN} — รอบนี้ยังไม่ได้รันแผน")
    plan = x.parse(sheet)
    cmp_ = x.parse(_SHEET_COMPARE) if _SHEET_COMPARE in x.sheet_names else pd.DataFrame()
    return plan, cmp_


# ── จับก้อนงาน (item + กลุ่มใบสั่งที่เชื่อมถึงกัน) ───────────────────────────

class _Union:
    """union-find เล็ก ๆ — ใช้จับใบสั่งที่ถูกยุบรวมกันคนละแบบระหว่างแผนเก่ากับแผนใหม่"""

    def __init__(self):
        self.p = {}

    def find(self, a):
        self.p.setdefault(a, a)
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _group_key(item: str, scs: list, uf: _Union) -> tuple:
    """คีย์ก้อนงาน = (item, ตัวแทนของกลุ่มใบสั่ง) ; ไม่มีใบสั่ง = ถัง '—' ของ item นั้น"""
    if not scs:
        return (item, "")
    return (item, uf.find(f"{item}|{scs[0]}"))


def _build_groups(plan, cmp_) -> dict:
    """รวมทุกแถว (แผนใหม่ + แผนเก่า) เข้าก้อนงาน → {key: ก้อน}"""
    uf = _Union()

    def _link(item, scs):
        for s in scs[1:]:
            uf.union(f"{item}|{scs[0]}", f"{item}|{s}")

    plan_rows = plan.to_dict("records") if len(plan) else []
    cmp_rows = cmp_.to_dict("records") if len(cmp_) else []
    for r in plan_rows:
        _link(_s(r.get("ITEM_CODE")).upper(), _sc_tokens(r.get("SC_SO_NO")))
    for r in cmp_rows:
        _link(_s(r.get("ITEM_CODE")).upper(), _sc_tokens(r.get("SC_SO_NO")))

    groups = {}

    def _slot(item, scs):
        k = _group_key(item, scs, uf)
        g = groups.get(k)
        if g is None:
            g = {"item": item, "sc": set(), "mc_group": "", "customer": "", "team": "",
                 "old_weeks": {}, "new_weeks": {}, "old_mc": 0, "new_mc": 0,
                 "src": "", "rows": []}
            groups[k] = g
        g["sc"].update(scs)
        return g

    # 1) แถวแผนใหม่ — พาคอลัมน์ที่ใช้อธิบายเหตุผลมาด้วย
    for r in plan_rows:
        item = _s(r.get("ITEM_CODE")).upper()
        if not item:
            continue
        g = _slot(item, _sc_tokens(r.get("SC_SO_NO")))
        g["mc_group"] = g["mc_group"] or _s(r.get("MC_GROUP"))
        g["customer"] = g["customer"] or _s(r.get("CUSTOMER"))
        g["team"] = g["team"] or _s(r.get("TEAM"))
        g["src"] = g["src"] or _s(r.get("PLAN_SOURCE"))
        g["rows"].append(r)
        w, y = _i(r.get("PLAN_WEEK")), _i(r.get("PLAN_YEAR"))
        if w is not None:
            slot = g["new_weeks"].setdefault(w, {"qty": 0.0, "mc": 0, "year": y})
            slot["qty"] += _f(r.get("PRODUCE_QTY"))
            slot["mc"] += _f(r.get("ACTUAL_MC"))
            if slot["year"] is None:
                slot["year"] = y

    # 2) แถวเทียบ — OLD = booking เดิม, NEW เอาไว้เติมก้อนที่ไม่มีในชีทแผน (กันตกหล่น)
    for r in cmp_rows:
        item = _s(r.get("ITEM_CODE")).upper()
        w = _i(r.get("WEEK"))
        if not item or w is None:
            continue
        g = _slot(item, _sc_tokens(r.get("SC_SO_NO")))
        g["mc_group"] = g["mc_group"] or _s(r.get("MC_GROUP"))
        if _s(r.get("ROW_KIND")).upper() == "OLD":
            slot = g["old_weeks"].setdefault(w, {"qty": 0.0, "mc": 0.0})
            slot["qty"] += _f(r.get("QTY"))
            slot["mc"] += _f(r.get("MC"))
        elif w not in g["new_weeks"]:
            g["new_weeks"][w] = {"qty": _f(r.get("QTY")), "mc": _f(r.get("MC")), "year": None}
    return groups


# ── เหตุผล (กฎจากข้อมูลจริงในชีท) ────────────────────────────────────────────

# ── เหตุผลที่ Planning เขียนไว้เองตอนตัดสินใจ (คอลัมน์ LINE_REMARK) ──────────
#
# **ต้องคัดเฉพาะข้อความของระบบ ห้ามส่งทั้งคอลัมน์**: LINE_REMARK ปนสองอย่างอยู่ —
# เหตุผลการวางแผนที่ Planning เขียนเอง กับ**หมายเหตุการค้าที่ติดมากับใบสั่ง**
# (ราคา USD/หน่วย, เงื่อนไขขนส่ง, ชื่อผู้อนุมัติ) ซึ่งเมลฉบับนี้ถูกส่งต่อให้หลายฝ่าย
# ถ้าปล่อยผ่านคือทำข้อมูลราคาหลุดออกนอกวงโดยไม่ตั้งใจ → เทียบกับรายการข้างล่างเท่านั้น
# (ที่มา: Planning.py:8886, 8920, 14741, 15182, 15359, 15715)
_LR_RULES = (
    (re.compile(r"ไหลต่อจากสัปดาห์ที่ซ่อม"),
     lambda m: "ต่อคิวจากสัปดาห์ที่ซ่อมแผน เพื่อให้ทอต่อเนื่องไม่ขาดช่วง"),
    (re.compile(r"ลด carry ตามเครื่องที่ถูกตัดสัปดาห์ก่อน"),
     lambda m: "เครื่องถูกตัดตั้งแต่สัปดาห์ก่อน เครื่องที่ไหลต่อมาจึงน้อยลง"),
    (re.compile(r"ตัดเครื่องส่วนเกินพูล\s*W?(\d+)?\s*\(?([A-Z0-9]+)?"),
     lambda m: f"ตัดเครื่องส่วนเกินคืนพูล{' ' + m.group(2) if m.group(2) else ''}"
               f"{' W' + m.group(1) if m.group(1) else ''}"),
    (re.compile(r"ตัดเครื่องตั้งใหม่ให้อยู่ในโควตา setup"),
     lambda m: "ตัดเครื่องตั้งใหม่ให้อยู่ในโควตา setup ของสัปดาห์นั้น"),
)


_COLOR_RE = re.compile(r"เลื่อนออกให้งานสี\s+(\S+)")


def _planner_notes(rows: list) -> list:
    """ข้อความเหตุผลจาก LINE_REMARK ของทุกแถวในก้อน (ไม่ซ้ำ, เรียงตามที่พบ)

    งานที่ถูกถอดเครื่องให้งานสีมักโดนหลายสัปดาห์ติดกัน → LINE_REMARK เขียนซ้ำทุกแถว
    ต่างกันแค่เลข Target Knit (เจอจริง: item เดียวกันโผล่ 4 บรรทัดในรายการเดียว)
    → ยุบเป็นบรรทัดเดียวโดยเก็บ "ชื่อ item งานสีที่มาแทน" ไม่ซ้ำ ซึ่งเป็นข้อมูลที่ผู้อ่าน
    ต้องการจริง (ใครเอาเครื่องไป) ส่วนเลข Target Knit ต่างกันทุกแถวจนไม่มีความหมายในสรุป
    """
    colors, out = [], []
    for r in rows:
        # แถวหนึ่งพ่วงได้หลายเหตุผล คั่นด้วย ' | ' และครึ่งหนึ่งอาจเป็นหมายเหตุใบสั่ง
        for part in str(_s(r.get("LINE_REMARK"))).split("|"):
            part = part.strip()
            if not part:
                continue
            m = _COLOR_RE.search(part)
            if m:
                if m.group(1) not in colors:
                    colors.append(m.group(1))
                continue
            for pat, fmt in _LR_RULES:
                mm = pat.search(part)
                if mm:
                    txt = fmt(mm)
                    if txt not in out:
                        out.append(txt)
                    break
    if colors:
        who = ", ".join(colors[:3]) + (f" และอีก {len(colors) - 3} รายการ" if len(colors) > 3 else "")
        out.insert(0, f"ถอดเครื่องให้งานสี {who} (งานที่เปิดสีแล้วได้เครื่องก่อน)")
    return out


_SETUP_RE = re.compile(r"setup เครื่องใหม่\s*(\d+)\s*ตัว\s*\(([\d.]+)\s*วัน\)")


def _setup_note(rows: list) -> str:
    """สรุปการตั้งเครื่องใหม่จาก REMARK — ตัวเลขวัน setup คือเวลาที่หายไปจากการทอจริง"""
    mc, days = 0, 0.0
    for r in rows:
        m = _SETUP_RE.search(_s(r.get("REMARK")))
        if m:
            mc += int(m.group(1))
            days = max(days, float(m.group(2)))
    return f"ต้อง Setup ใหม่ {mc} ตัว (เสียเวลา setup {days:g} วัน)" if mc else ""


def _reasons(g: dict, axis: WeekAxis, old_from, old_to, new_from, new_to,
             d_start, d_end, d_qty) -> list:
    """ทำไมงานก้อนนี้ถึงเปลี่ยน — เรียงจากกฎที่ฟันธงได้แน่ ไปหากฎที่กว้างที่สุด

    ทุกข้อต้องอ้างเลขจากข้อมูลจริงเสมอ ("ด้ายเข้าครบ W34" ไม่ใช่ "ด้ายมาช้า") เพราะเมลนี้
    ถูกใช้ตอบกลับลูกค้า/ฝ่ายขาย ถ้าเหตุผลเดาเอาจะกลายเป็นข้อมูลผิดที่ส่งออกนอกทีม
    """
    out = []
    rows = g["rows"]

    # 0) งานที่ไม่มีของเดิม/ไม่มีของใหม่ — อธิบายด้วยกฎอื่นไม่ได้ ต้องบอกตรง ๆ ว่าเป็นคนละกรณี
    if old_from is None:
        src = _s(g.get("src")).upper()
        out.append("SC ใหม่ที่ยังไม่เคยอยู่ใน booking — วางแผนครั้งแรกในรอบนี้"
                   if src != "CORE_KEEP" else "งาน core ที่เพิ่งถูกเติมเข้าแผนรอบนี้")
        return out
    if new_from is None:
        out.append("ไม่ถูกวางในแผนรอบนี้ — ยอดค้างยกไปรอบถัดไป (ตรวจสอบ SC/ด้าย/เครื่องก่อนยืนยันกับลูกค้า)")
        return out

    # 1) core ที่ตั้งใจไม่ให้ replan แตะ — ต้องมาก่อนทุกข้อ ไม่งั้นจะถูกอธิบายด้วยเหตุผลอื่นที่ไม่ใช่
    if _s(g.get("src")).upper() == "CORE_KEEP":
        out.append("งาน core — คงแผนเดิมไว้ ไม่ถูกจัดใหม่ในรอบนี้")
        return out

    # 2) เหตุผลที่ Planning บันทึกไว้เองตอนตัดสินใจ — แม่นกว่ากฎที่เดาย้อนหลังทุกข้อ
    # (เช่น "ถอดเครื่องให้งานสี FD4BASRG25") จึงมาก่อนกฎอื่นทั้งหมด
    out += _planner_notes(rows)

    # 2) ด่านด้ายเข้า (บังคับรายใบใน Planning) — ถ้าสัปดาห์เดิมเร็วกว่าที่ด้ายพร้อม ต้องเลื่อนแน่นอน
    earliest = max([x for x in (_i(r.get("EARLIEST_PLAN_WEEK")) for r in rows) if x], default=None)
    po_in = max([x for x in (_i(r.get("PO_IN_WEEK")) for r in rows) if x], default=None)
    if earliest and old_from is not None and (axis.diff(old_from, earliest) or 0) > 0:
        txt = f"ด้ายเข้าครบสัปดาห์ W{po_in} " if po_in else ""
        out.append(f"{txt}ทอได้เร็วสุด W{earliest} — สัปดาห์เดิม W{old_from} ยังไม่มีด้าย")

    # 3) สัปดาห์เดิมล็อกไปแล้ว (freeze) — ยกกลับไปวางที่เดิมไม่ได้แม้เครื่องจะว่าง
    if old_from is not None and axis.is_frozen(old_from) and (d_start or 0) > 0:
        out.append(f"สัปดาห์เดิม W{old_from} อยู่ในช่วงล็อก (แก้ได้ตั้งแต่ W{axis.edit_from()})")

    # 4) เงื่อนไขเฉพาะงาน — อ่านจากคอลัมน์ที่ Planning ติดมากับแถว
    if any(_flag(r.get("CYLINDER_CHANGE")) for r in rows):
        out.append("ต้องรอเปลี่ยน Cylinder ก่อนขึ้นเครื่อง")
    if any(_flag(r.get("OUTSOURCE")) for r in rows):
        out.append("บางส่วนถูกย้ายไปจ้างทอข้างนอก")
    s9 = next((_s(r.get("S9_ROUTING")) for r in rows if _flag(r.get("S9_ROUTING"))), "")
    if s9:
        out.append(f"เข้าเส้นทางจ้างทอ S9 ({s9})")

    # 5) ยอดเปลี่ยน → เวลาทอเปลี่ยนตาม (อธิบายทั้งจบช้าและจบเร็ว)
    if abs(d_qty) > QTY_TOL:
        out.append(f"ยอดที่ต้องทอ{'เพิ่มขึ้น' if d_qty > 0 else 'ลดลง'} {abs(d_qty):,.0f} กก."
                   f" ({'ใช้เวลานานขึ้น' if d_qty > 0 else 'ใช้เวลาน้อยลง'})")

    # 6) จำนวนเครื่องที่ได้จริงเปลี่ยน — เหตุผลตรงที่สุดของ "จบเร็ว/ช้ากว่าเดิม"
    om, nm = g["old_mc"], g["new_mc"]
    if om and nm and nm != om:
        out.append(f"จำนวนเครื่องสูงสุดต่อสัปดาห์ {'เพิ่ม' if nm > om else 'ลด'}จาก {om:g} เป็น {nm:g} ตัว")

    # 6.5) ต้องตั้งเครื่องใหม่ = เสียวันทอไปกับ setup (อธิบาย "ทำไมช้ากว่าที่ควร" ได้ตรง)
    setup = _setup_note(rows)
    if setup and (d_start or d_end):
        out.append(setup)

    # 7) ยังไม่มีเหตุผลเจาะจง = ถูกจัดคิวใหม่ทั้งกระดาน (เครื่องกลุ่มนั้นในสัปดาห์เดิมไม่ว่าง)
    if not out:
        mcg = g.get("mc_group") or "กลุ่มเดิม"
        if (d_start or 0) > 0 or (d_end or 0) > 0:
            out.append(f"เครื่อง {mcg} ในสัปดาห์เดิมไม่ว่างพอ — ถูกจัดคิวใหม่ตามลำดับความเร่งด่วน")
        elif (d_start or 0) < 0 or (d_end or 0) < 0:
            out.append(f"เครื่อง {mcg} ว่างเร็วกว่าที่ booking จองไว้ — ดึงงานขึ้นมาทอก่อน")
    return out


# ── สรุปการเปลี่ยนแปลง ───────────────────────────────────────────────────────

def _wk_range(weeks: dict) -> tuple:
    """ช่วงสัปดาห์ที่มีงานจริง (ตัดสัปดาห์ยอด ~0 ทิ้ง) → (แรก, สุดท้าย, ยอดรวม, เครื่องสูงสุด)"""
    live = {w: v for w, v in weeks.items() if abs(v.get("qty", 0)) > QTY_TOL}
    if not live:
        return None, None, 0.0, 0
    ws = sorted(live)
    return (ws[0], ws[-1], sum(v["qty"] for v in live.values()),
            max((v.get("mc") or 0) for v in live.values()))


def changes(path: Path = None) -> dict:
    """รายการเปลี่ยนแปลงของรอบ Replan (ใช้ทั้งพรีวิวบนเว็บและเนื้อเมล)

    {"plan_file","generated_at","current":{...},"summary":{...},"items":[...]}
    """
    p = path or plan_view.latest_path()
    if p is None:
        raise FileNotFoundError("ยังไม่มีไฟล์แผนผลิต — กรุณากดรันแผนก่อน")
    plan, cmp_ = _read_sheets(p)
    # เส้นล็อก/สัปดาห์ปัจจุบันยึดวันที่ไฟล์แผนถูกสร้าง (= ตอนที่ Replan ตัดสินใจ) ไม่ใช่วันนี้
    axis = WeekAxis(datetime.fromtimestamp(p.stat().st_mtime).date())
    groups = _build_groups(plan, cmp_)
    teams = _team_map()

    items, summary = [], {"moved": 0, "qty": 0, "later": 0, "earlier": 0,
                          "dropped": 0, "added": 0, "late_rdd": 0, "late_before": 0}
    for g in groups.values():
        o_from, o_to, o_qty, o_mc = _wk_range(g["old_weeks"])
        n_from, n_to, n_qty, n_mc = _wk_range(g["new_weeks"])
        g["old_mc"], g["new_mc"] = o_mc, n_mc
        if o_from is None and n_from is None:
            continue

        # ทีม: ของในชีทแผนมาก่อน ไม่มีค่อยเติมจาก booking (ระดับใบ → ระดับผ้า)
        team = g["team"] or next((teams.get((g["item"], s)) for s in sorted(g["sc"])
                                  if teams.get((g["item"], s))), "") or teams.get((g["item"], "*"), "")
        # งาน core ไม่ต้องแจ้ง (ผู้ใช้สั่ง) — ผลิตเข้าสต๊อก ไม่มีลูกค้ารอรับตามกำหนด ;
        # ต้องข้ามก่อนนับ summary ไม่งั้นตัวเลขหัวหน้าจอจะรวมงานที่ไม่ได้โชว์
        if (_s(g.get("src")).upper() == "CORE_KEEP"
                or any(_s(r.get("IS_CORE_ITEM")).upper().startswith("CORE") for r in g["rows"])
                or plan_view.is_core_row(g["item"], g["customer"], team)):
            continue

        # ปีของฝั่งใหม่มาจาก PLAN_YEAR (แม่นกว่าเดา) ฝั่งเก่าไม่มีปีในข้อมูล → ให้แกนเดาให้
        y_new_from = g["new_weeks"].get(n_from, {}).get("year") if n_from is not None else None
        y_new_to = g["new_weeks"].get(n_to, {}).get("year") if n_to is not None else None
        d_start = axis.diff(o_from, n_from, None, y_new_from) if (o_from and n_from) else None
        d_end = axis.diff(o_to, n_to, None, y_new_to) if (o_to and n_to) else None
        d_qty = round(n_qty - o_qty, 1)

        kinds = []
        if o_from is None:
            kinds.append("ADDED")
            summary["added"] += 1
        elif n_from is None:
            kinds.append("DROPPED")
            summary["dropped"] += 1
        else:
            if d_start:
                kinds.append("MOVED")
                summary["moved"] += 1
            if d_end and d_end > 0:
                kinds.append("FINISH_LATER")
                summary["later"] += 1
            elif d_end and d_end < 0:
                kinds.append("FINISH_EARLIER")
                summary["earlier"] += 1
            if abs(d_qty) > QTY_TOL:
                kinds.append("QTY_UP" if d_qty > 0 else "QTY_DOWN")
                summary["qty"] += 1
        if not kinds:
            continue                     # ก้อนที่ไม่ขยับ ไม่เปลี่ยนยอด = ไม่ต้องแจ้ง

        # เลยกำหนดส่ง: เทียบสัปดาห์ที่ทอเสร็จกับ RDD ของใบที่เร่งที่สุดในก้อน
        # แยก "เพิ่งเลยเพราะรอบนี้" กับ "แผนเดิมก็เลยอยู่แล้ว" — ถ้าไม่แยก เมลจะเต็มไปด้วยงานค้าง
        # ที่เกินกำหนดมาก่อนหน้านี้แล้ว ทั้งที่ไม่ใช่ผลของการ replan รอบนี้
        rdd_w, rdd_y, late, late_before = None, None, False, False
        for r in g["rows"]:
            y, w = _rdd_year_week(r.get("RDD_WEEK"))
            if w is None:
                continue
            if rdd_w is None or (axis.diff(rdd_w, w, rdd_y, y) or 0) < 0:
                rdd_w, rdd_y = w, y
        if rdd_w is not None:
            over_new = (axis.diff(rdd_w, n_to, rdd_y, y_new_to) or 0) > 0 if n_to else False
            over_old = (axis.diff(rdd_w, o_to, rdd_y) or 0) > 0 if o_to else False
            late = over_new and not over_old
            late_before = over_new and over_old
            if late:
                summary["late_rdd"] += 1
            elif late_before:
                summary["late_before"] += 1

        # ยอดรายสัปดาห์ทั้งสองฝั่ง (พร้อมปี) — ตารางเทียบรายสัปดาห์ใน Excel ใช้ชุดนี้
        wk_old = sorted(((axis.year_of(w), w, round(v["qty"], 1))
                         for w, v in g["old_weeks"].items() if abs(v["qty"]) > QTY_TOL))
        wk_new = sorted(((v.get("year") or axis.year_of(w), w, round(v["qty"], 1))
                         for w, v in g["new_weeks"].items() if abs(v["qty"]) > QTY_TOL))
        # วันด้ายเข้าครบของก้อน (ใบที่ช้าสุด) + สัปดาห์แรกที่ทอได้ตามด่านนั้น
        po_in = max((_s(r.get("PO_IN_DATE")) for r in g["rows"]), default="", key=_po_key)
        earliest = max([x for x in (_i(r.get("EARLIEST_PLAN_WEEK")) for r in g["rows"]) if x],
                       default=None)
        po_week = max([x for x in (_i(r.get("PO_IN_WEEK")) for r in g["rows"]) if x], default=None)

        items.append({
            "item": g["item"],
            "sc": sorted(g["sc"]),
            "mc_group": g["mc_group"],
            "customer": g["customer"],
            "team": team,
            "weeks_old": wk_old, "weeks_new": wk_new,
            "po_in": po_in, "po_in_week": po_week, "earliest_week": earliest,
            "kinds": kinds,
            # ปีติดไปกับช่วงสัปดาห์ด้วย — ต้องใช้ตัดสินว่าจะโชว์ปีในตารางไหม (งานข้ามปี
            # ที่เขียนแต่เลขสัปดาห์จะอ่านกลับด้าน ดู _span)
            "old": {"from": o_from, "to": o_to, "qty": round(o_qty, 1), "mc": o_mc,
                    "year": axis.year_of(o_from) if o_from else None},
            "new": {"from": n_from, "to": n_to, "qty": round(n_qty, 1), "mc": n_mc,
                    "year": y_new_from or (axis.year_of(n_from) if n_from else None)},
            "shift_start": d_start, "shift_end": d_end, "d_qty": d_qty,
            "rdd_week": rdd_w, "late": late, "late_before": late_before,
            "reasons": _reasons(g, axis, o_from, o_to, n_from, n_to, d_start, d_end, d_qty),
        })

    # งานที่ "หลุดจากแผน" ต้องแยกให้ออกว่าหายจริง หรือแค่จับคู่ไม่ติด — item เดียวกันโผล่ใน
    # แผนใหม่ใต้เลขใบคนละชุดได้ (booking บันทึกเลข PO ลูกค้าอย่าง 6695107541 ไว้ในช่อง SO_NO
    # ส่วนแผนใหม่ใช้เลข SC 6 หลัก) ถ้าไม่บอกไว้ ผู้อ่านจะไล่ตามของที่ไม่ได้หายไปจริง
    placed = {i["item"] for i in items if i["new"]["from"] is not None}
    for i in items:
        if "DROPPED" in i["kinds"] and i["item"] in placed:
            i["kinds"].append("SAME_ITEM_ELSEWHERE")
            i["reasons"].insert(0, f"ผ้ารหัสนี้ถูกวางในแผนใหม่แล้ว แต่อยู่ใต้ SC อื่น "
                                   f"— ตรวจว่า SC {', '.join(i['sc'][:2])} ซ้ำกับ SC ที่วางไปแล้วหรือไม่")

    # เรียงตามความสำคัญ: เลยกำหนดส่งก่อน → เลื่อนออกไปไกลสุด → ยอดเปลี่ยนมากสุด
    items.sort(key=lambda x: (not x["late"], not x["late_before"], -(x["shift_end"] or 0),
                              -(x["shift_start"] or 0), -abs(x["d_qty"])))
    cur_y, cur_w = axis.cur_year, axis.cur_week
    return {
        "plan_file": p.name,
        "plan_mtime": p.stat().st_mtime,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current": {"year": cur_y, "week": cur_w, "edit_from": axis.edit_from()},
        "summary": {**summary, "total": len(items)},
        "items": items,
    }


# ── แยกเป็นชุดต่อทีมขาย (1 ชุด = เมล 1 ฉบับ) ─────────────────────────────────

UNASSIGNED = "ไม่ระบุทีม"

_SUM_OF_KIND = {"MOVED": "moved", "FINISH_LATER": "later", "FINISH_EARLIER": "earlier",
                "QTY_UP": "qty", "QTY_DOWN": "qty", "DROPPED": "dropped", "ADDED": "added"}


def _summarize(items: list) -> dict:
    """นับสรุปใหม่จากรายการชุดย่อย — ห้ามใช้ summary ของทั้งแผนกับเมลรายทีม

    ตัวเลขบนหัวเมล ("เลื่อน 42 รายการ") ถูกใช้ตัดสินใจว่าต้องรีบดูแค่ไหน ถ้าส่งยอดรวม
    ทั้งแผนไปกับเมลที่มีของทีมตัวเอง 3 รายการ ผู้รับจะตกใจกับตัวเลขที่ไม่ใช่ของตัวเอง
    """
    s = {"moved": 0, "qty": 0, "later": 0, "earlier": 0, "dropped": 0, "added": 0,
         "late_rdd": 0, "late_before": 0, "total": len(items)}
    for it in items:
        for k in set(_SUM_OF_KIND.get(k) for k in it["kinds"]):
            if k:
                s[k] += 1
        if it.get("late"):
            s["late_rdd"] += 1
        elif it.get("late_before"):
            s["late_before"] += 1
    return s


def split_by_team(data: dict) -> list:
    """{ทั้งแผน} → [{"team", "key", "data"}] ชุดละทีม เรียงจากทีมที่มีรายการมากสุด

    **งานก้อนเดียวเข้าได้หลายทีม** — booking ยุบชื่อทีมมาเป็น `"RTS,NYK4-1"` แปลว่าทั้งสองทีม
    ขายผ้าก้อนนี้ ต้องเห็นเหมือนกันทั้งคู่ (เหมือนที่ชีทรายทีมในไฟล์แนบทำอยู่) ไม่ใช่เลือกทีมแรก
    รายการที่ไม่มีทีมติดมาเลยไปรวมชุด "ไม่ระบุทีม" — ไม่ทิ้ง เพราะเป็นงานจริงที่ต้องมีคนดู
    """
    buckets = {}
    for it in data["items"]:
        names = mail_recipients.split_teams(it.get("team"))
        for key in (names or [UNASSIGNED]):
            b = buckets.setdefault(key, {"team": key, "key": key, "items": []})
            # ชื่อที่โชว์ใช้ตามที่พิมพ์มาในข้อมูล ไม่ใช่ตัวพิมพ์ใหญ่ที่ใช้เทียบ
            if key != UNASSIGNED and b["team"] == key:
                raw = next((t.strip() for t in re.split(r"[,;]+", it.get("team") or "")
                            if mail_recipients.norm_team(t) == key), "")
                b["team"] = raw or key
            b["items"].append(it)
    out = []
    for key, b in buckets.items():
        out.append({"team": b["team"], "key": key,
                    "data": {**data, "team": b["team"], "items": b["items"],
                             "summary": _summarize(b["items"])}})
    out.sort(key=lambda x: (x["key"] == UNASSIGNED, -len(x["data"]["items"]), x["key"]))
    return out


# ── เนื้ออีเมล ───────────────────────────────────────────────────────────────

_KIND_LABEL = {
    "MOVED": "เลื่อนสัปดาห์", "FINISH_LATER": "จบช้ากว่าเดิม", "FINISH_EARLIER": "จบเร็วกว่าเดิม",
    "QTY_UP": "ยอดเพิ่ม", "QTY_DOWN": "ยอดลด", "DROPPED": "หลุดจากแผน", "ADDED": "งานใหม่เข้าแผน",
    "SAME_ITEM_ELSEWHERE": "ผ้ารหัสนี้วางแล้วใต้ SC อื่น",
}


def subject(data: dict) -> str:
    s = data["summary"]
    parts = []
    if s["moved"]:
        parts.append(f"เลื่อน {s['moved']}")
    if s["later"]:
        parts.append(f"จบช้าลง {s['later']}")
    if s["earlier"]:
        parts.append(f"จบเร็วขึ้น {s['earlier']}")
    if s["qty"]:
        parts.append(f"ยอดเปลี่ยน {s['qty']}")
    if s["dropped"]:
        parts.append(f"หลุดแผน {s['dropped']}")
    if s["added"]:
        parts.append(f"งานใหม่ {s['added']}")
    cur = data["current"]
    # เมลรายทีมต้องมีชื่อทีมอยู่ในหัวข้อ — ผู้รับบางคนอยู่หลายทีมและได้หลายฉบับในนาทีเดียวกัน
    # ถ้าหัวข้อเหมือนกันหมด Outlook จะยุบเป็นเธรดเดียวจนแยกไม่ออกว่าฉบับไหนของทีมไหน
    who = f" ทีม {data['team']}" if data.get("team") else ""
    head = f"[Knit Plan]{who} แผนเปลี่ยน W{cur['week']}/{cur['year']}"
    return f"{head} — {' · '.join(parts) if parts else 'ไม่มีรายการเปลี่ยนแปลง'}"


def _wk(v) -> str:
    return f"W{v}" if v is not None else "—"


def _span(d: dict, year=None, show_year: bool = False) -> str:
    """ช่วงสัปดาห์แบบอ่านง่าย — สัปดาห์เดียวไม่ต้องเขียนซ้ำสองข้าง

    show_year: ติดปี 2 หลักท้าย (W39/26) **บังคับใช้เมื่อของเดิมกับของใหม่คนละปี** —
    เจอจริงในเมลฉบับแรก: booking W39/2026 ถูกเลื่อนไป W22–23/**2027** แต่เขียนแค่
    "W39 → W22–23" คนอ่านเข้าใจกลับด้านทันทีว่าดึงงานขึ้นมาเร็วกว่าเดิม 17 สัปดาห์
    ทั้งที่ความจริงคือช้าลง 36 สัปดาห์
    """
    a, b = d.get("from"), d.get("to")
    if a is None:
        return "—"
    core = _wk(a) if a == b else f"{_wk(a)}–{b}"
    if show_year and year:
        core += f"/{str(year)[-2:]}"
    return core


def _delta(n) -> str:
    if not n:
        return ""
    return f"ช้าลง {n} สัปดาห์" if n > 0 else f"เร็วขึ้น {abs(n)} สัปดาห์"


def impact_text(it: dict) -> str:
    """ผลกระทบของงานก้อนหนึ่งเป็นประโยคเดียว — ตัวเลขที่ "จำเป็นต้องรู้" เท่านั้น

    เดิมเขียนว่า "เลื่อนสัปดาห์ · จบช้ากว่าเดิม · เริ่ม ช้าลง 36 สัปดาห์ · จบ ช้าลง 37 สัปดาห์"
    ซึ่งพูดเรื่องเดียวกันซ้ำ 4 รอบจนอ่านไม่ทัน → เหลือตัวเลขเดียว (เริ่ม–จบ ถ้าต่างกัน)
    ส่วนคำว่า "เลื่อนสัปดาห์/จบช้ากว่าเดิม" ตัดทิ้งเพราะช่องสัปดาห์เดิม→ใหม่บอกอยู่แล้ว
    """
    ds, de = it.get("shift_start"), it.get("shift_end")
    if "DROPPED" in it["kinds"]:
        out = ("หลุดจากแผน (แต่ผ้ารหัสนี้วางแล้วใต้ SC อื่น)"
               if "SAME_ITEM_ELSEWHERE" in it["kinds"] else "หลุดจากแผน — ไม่พบในแผนใหม่เลย")
    elif "ADDED" in it["kinds"]:
        out = "งานใหม่เข้าแผน"
    elif ds and de and ds != de:
        # ทิศเดียวกัน (ทั้งคู่ช้าลง/เร็วขึ้น) รวบเป็นช่วงเดียว ; คนละทิศค่อยแยกพูด
        out = (f"{_delta(de).split()[0]} {min(abs(ds), abs(de))}–{max(abs(ds), abs(de))} สัปดาห์"
               if (ds > 0) == (de > 0) else f"เริ่ม{_delta(ds)} จบ{_delta(de)}")
    else:
        out = _delta(de or ds) or "ยอดเปลี่ยน"
    if abs(it["d_qty"]) > QTY_TOL:
        out += f" · ยอด {it['d_qty']:+,.0f} กก."
    return out


def xlsx_name(data: dict) -> str:
    """ชื่อไฟล์แนบ — ตั้งเป็นอังกฤษล้วน เพราะชื่อไฟล์ที่มีอักษรไทยผ่าน Power Automate →
    Outlook แล้วมีโอกาสเพี้ยน/ถูกตัดตามการเข้ารหัสของแต่ละ client (เนื้อในไฟล์เป็นไทยได้ปกติ)

    ชื่อทีมก็ต้องผ่านตัวกรองเดียวกัน — ทีมจริงมี `NYG/TRM` ที่ทับหน้าเป็นตัวแบ่งพาธ
    และชุด "ไม่ระบุทีม" ที่เป็นอักษรไทยล้วน (กรองแล้วไม่เหลือ → ใช้ Team เฉย ๆ)
    """
    cur = data["current"]
    tag = ""
    if data.get("team"):
        safe = re.sub(r"[^A-Za-z0-9]+", "-", str(data["team"])).strip("-")
        tag = f"_{safe or 'Team'}"
    return f"KnitPlan_ReplanChanges{tag}_W{cur['week']}-{cur['year']}.xlsx"


def build_xlsx(data: dict) -> bytes:
    """ไฟล์ Excel ที่มี **ทุกรายการที่เปลี่ยน** พร้อมเหตุผลเต็ม — ตัวจริงที่ผู้รับใช้ทำงานต่อ

    เนื้อเมลส่งได้จำกัดขนาด (ดู MAX_MAIL_BYTES) แต่คำสั่งคือ "ทุกตัวที่ขยับต้องแจ้ง" →
    เมลโชว์เฉพาะตัวที่ต้องดูก่อน ส่วนไฟล์นี้มีครบทุกแถว กรอง/เรียง/ก๊อปไปทำงานต่อได้
    """
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill

    wb = Workbook()
    ws = wb.active
    ws.title = "แผนเปลี่ยน"
    # มีคอลัมน์ "ปี" ของทั้งสองฝั่ง เพราะเลขสัปดาห์เปล่าอ่านข้ามปีไม่ได้ (W39/26 → W22/27
    # ถ้าดูแต่เลขจะนึกว่าเลื่อนขึ้นมาก่อน) และให้กรอง/เรียงในไฟล์ได้ตรงตามเวลาจริง
    head = ["ITEM", "ลูกค้า", "ทีม", "SC", "เครื่อง", "ผลกระทบ",
            "ปีเดิม", "สัปดาห์เดิม เริ่ม", "สัปดาห์เดิม จบ",
            "ปีใหม่", "สัปดาห์ใหม่ เริ่ม", "สัปดาห์ใหม่ จบ",
            "เริ่ม +/- สัปดาห์", "จบ +/- สัปดาห์", "ยอดเดิม (กก.)", "ยอดใหม่ (กก.)",
            "ยอดต่าง (กก.)", "RDD (สัปดาห์)", "สถานะกำหนดส่ง", "ประเภทการเปลี่ยนแปลง", "เหตุผล"]
    ws.append(head)
    for c in ws[1]:
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="374151")
        c.alignment = Alignment(vertical="center", wrap_text=True)
    late_fill = PatternFill("solid", fgColor="FDE8E8")
    for it in data["items"]:
        ws.append([
            it["item"], it["customer"], it["team"], ", ".join(it["sc"]), it["mc_group"],
            impact_text(it),
            it["old"].get("year"), it["old"]["from"], it["old"]["to"],
            it["new"].get("year"), it["new"]["from"], it["new"]["to"],
            it["shift_start"], it["shift_end"],
            it["old"]["qty"], it["new"]["qty"], it["d_qty"],
            it["rdd_week"],
            "เพิ่งเกินรอบนี้" if it["late"] else ("เกินอยู่ก่อนแล้ว" if it.get("late_before") else ""),
            " · ".join(_KIND_LABEL.get(k, k) for k in it["kinds"]),
            " · ".join(it["reasons"]),
        ])
        if it["late"]:
            ws.cell(ws.max_row, 19).fill = late_fill      # ทาเฉพาะช่องสถานะกำหนดส่ง
    for col, w in zip("ABCDEFGHIJKLMNOPQRSTU",
                      (18, 16, 10, 20, 9, 26, 8, 9, 9, 8, 9, 9, 9, 9, 12, 12, 11, 9, 14, 24, 60)):
        ws.column_dimensions[col].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    # ชีทสรุป — ให้เปิดไฟล์แล้วเห็นภาพรวมก่อนโดยไม่ต้องนับเอง
    s2 = wb.create_sheet("สรุป")
    s = data["summary"]
    s2.append(["ไฟล์แผน", data["plan_file"]])
    s2.append(["สร้างเมื่อ", data["generated_at"].replace("T", " ")])
    s2.append(["สัปดาห์ปัจจุบัน", f"W{data['current']['week']}/{data['current']['year']}"])
    s2.append(["แก้แผนได้ตั้งแต่", f"W{data['current']['edit_from']}"])
    s2.append([])
    for k, label in (("total", "รายการที่เปลี่ยนทั้งหมด"), ("moved", "เลื่อนสัปดาห์"),
                     ("later", "จบช้ากว่าเดิม"), ("earlier", "จบเร็วกว่าเดิม"),
                     ("qty", "ยอดเปลี่ยน"), ("dropped", "หลุดจากแผน"), ("added", "งานใหม่เข้าแผน"),
                     ("late_rdd", "เพิ่งเกิน RDD รอบนี้"), ("late_before", "เกิน RDD อยู่ก่อนแล้ว")):
        s2.append([label, s.get(k, 0)])
    s2.column_dimensions["A"].width = 28
    s2.column_dimensions["B"].width = 40

    _add_team_sheets(wb, data)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ชื่อชีท Excel ห้ามมี : \ / ? * [ ] และยาวได้ 31 ตัว (ทีมจริงมี "NYG/TRM" ที่ติดกฎนี้)
_SHEET_BAD = re.compile(r"[:\\/?*\[\]]")


def _sheet_name(name: str, used: set) -> str:
    s = _SHEET_BAD.sub("-", str(name or "ไม่ระบุทีม").strip())[:31] or "ไม่ระบุทีม"
    base, n = s, 2
    while s.lower() in used:
        s = f"{base[:28]}-{n}"
        n += 1
    used.add(s.lower())
    return s


def _add_team_sheets(wb, data: dict) -> None:
    """ชีทเทียบรายสัปดาห์ แยกตามทีมขาย — รูปแบบเดียวกับตารางเทียบบนหน้าเว็บ

    1 item = 3 แถว: **เก่า (booking)** / **Replan** / **POIN DATE**
    คอลัมน์เป็นสัปดาห์เรียงตามเวลาจริง (ข้ามปีเรียงถูก เพราะคีย์เป็น (ปี, สัปดาห์) ไม่ใช่เลขเปล่า)
    ทีมที่ระบุไม่ได้ไปรวมชีท "ไม่ระบุทีม" — ไม่ทิ้ง เพราะเป็นงานจริงที่ต้องมีคนดู
    """
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    # งานก้อนเดียวขายให้หลายทีมได้ (booking ยุบชื่อทีมมาเป็น "RTS,NYK4-1,NYK1-1") →
    # ใส่รายการนั้นในชีทของ **ทุกทีมที่เกี่ยวข้อง** ไม่ใช่สร้างชีทชื่อประหลาดที่มีรายการเดียว
    # (ซ้ำได้ เพราะแต่ละทีมต้องเห็นงานของตัวเองครบโดยไม่ต้องไปหาในชีทอื่น)
    by_team = {}
    for it in data["items"]:
        names = [t.strip() for t in re.split(r"[,;]+", it.get("team") or "") if t.strip()]
        for t in (names or ["ไม่ระบุทีม"]):
            by_team.setdefault(t, []).append(it)

    hdr_fill = PatternFill("solid", fgColor="374151")
    old_fill = PatternFill("solid", fgColor="FDECEC")     # แถวเก่า = โทนแดงจาง (ของที่หายไป)
    new_fill = PatternFill("solid", fgColor="EAF7EF")     # แถว Replan = โทนเขียวจาง (ของที่วางจริง)
    po_fill = PatternFill("solid", fgColor="FFF8E1")
    used = set()
    for team in sorted(by_team, key=lambda t: (t == "ไม่ระบุทีม", t)):
        rows = by_team[team]
        # ชุดสัปดาห์ของชีทนี้ = ทุกสัปดาห์ที่มีของจริง (ทั้งฝั่งเก่าและใหม่) เรียงตามเวลา
        weeks = sorted({(y, w) for it in rows for (y, w, _q) in it["weeks_old"] + it["weeks_new"]})
        multi_year = len({y for y, _w in weeks}) > 1
        ws = wb.create_sheet(_sheet_name(team, used))
        head = ["ITEM", "ลูกค้า", "เครื่อง", "SC", "แผน"]
        head += [(f"W{w}/{str(y)[-2:]}" if multi_year else f"W{w}") for y, w in weeks]
        head += ["รวม (กก.)", "ผลกระทบ / เหตุผล"]
        ws.append(head)
        for c in ws[1]:
            c.font = Font(bold=True, color="FFFFFF")
            c.fill = hdr_fill
            c.alignment = Alignment(vertical="center", wrap_text=True)

        for it in rows:
            impact = impact_text(it)
            if it["late"]:
                impact += f" · ⚠ เกิน RDD W{it['rdd_week']}"
            elif it.get("late_before"):
                impact += f" · เกิน RDD W{it['rdd_week']} อยู่ก่อนแล้ว"
            sc = ", ".join(it["sc"]) or "—"
            for label, bag, fill, note in (
                ("เก่า (booking)", it["weeks_old"], old_fill, ""),
                ("Replan", it["weeks_new"], new_fill, impact + " · " + " · ".join(it["reasons"][:3])),
            ):
                q = {(y, w): v for y, w, v in bag}
                ws.append([it["item"], it["customer"], it["mc_group"], sc, label]
                          + [q.get(k) for k in weeks]
                          + [round(sum(q.values()), 1) if q else 0, note])
                # ทาสีเฉพาะช่อง "แผน" ไม่ใช่ทั้งแถว — ทาทุกเซลล์ทำให้ไฟล์ใหญ่ขึ้นเท่าตัว
                # (วัดจริง 103 → 78 KB) โดยหน้าตาที่คนอ่านเห็นไม่ต่างกัน
                ws.cell(ws.max_row, 5).fill = fill
            # แถวด่านด้ายเข้า — ตอบคำถามแรกที่ทุกคนถามเวลาเห็นงานถูกเลื่อน
            po = it.get("po_in") or "—"
            wk = it.get("earliest_week")
            txt = (f"ด้ายเข้าครบ {po}"
                   + (f" (W{it['po_in_week']})" if it.get("po_in_week") else "")
                   + (f" → ทอได้เร็วสุด W{wk}" if wk else ""))
            ws.append([it["item"], "", "", sc, "POIN DATE"] + [None] * len(weeks) + [None, txt])
            ws.cell(ws.max_row, 5).fill = po_fill

        ws.freeze_panes = "F2"
        for col, w in zip("ABCDE", (18, 16, 9, 20, 14)):
            ws.column_dimensions[col].width = w
        for n in range(len(weeks)):
            ws.column_dimensions[get_column_letter(6 + n)].width = 8
        ws.column_dimensions[get_column_letter(6 + len(weeks))].width = 11
        ws.column_dimensions[get_column_letter(7 + len(weeks))].width = 80


def build_html(data: dict, note: str = "", sender: str = "", limit_bytes: int = 0,
               attach_name: str = "", banner: str = "") -> str:
    """เนื้อเมล HTML — ตารางเดียวจบ อ่านบนมือถือได้

    ใช้ inline style ล้วน: Outlook ตัด <style> ในหัวเอกสารทิ้งเมื่อ forward และไม่รองรับ
    CSS ส่วนใหญ่ ตารางที่จัดรูปด้วยคลาสจะกลายเป็นข้อความเรียงกันเป็นพืด
    """
    s, cur = data["summary"], data["current"]
    esc = escape

    def _row_bg(it):
        return "#fff5f5" if it["late"] else "#ffffff"

    # หัวเมลเหลือชื่อเรื่องอย่างเดียว — บรรทัดข้อมูลระบบ (ชื่อไฟล์แผน, เวลาสร้าง, ชื่อคนกดส่ง)
    # ถูกตัดออกตามที่ผู้ใช้สั่ง: ผู้รับเป็นฝ่ายขาย/ผลิต ไม่ได้ใช้ข้อมูลพวกนั้น และมันดันอยู่บรรทัดแรก
    # ที่ควรเป็นสาระ ; รายละเอียดครบยังตามได้จากไฟล์แนบและ log ฝั่งระบบ
    # เมลรายทีม: บอกตั้งแต่บรรทัดแรกว่าเป็นของทีมไหน และ**เห็นเฉพาะงานของทีมนั้น** —
    # ไม่งั้นผู้รับจะนึกว่านี่คือแผนทั้งโรงงานแล้วสรุปผิดว่างานทีมอื่นถูกยกเลิกไปหมด
    who = (f'<h2 style="margin:0 0 10px;font-size:18px">ทีม {esc(data["team"])} — '
           f'แผนผลิตทอเปลี่ยนแปลง (สัปดาห์ปัจจุบัน W{cur["week"]})</h2>'
           f'<div style="margin:-4px 0 10px;color:#57606a;font-size:12px">'
           f'รายการในเมลฉบับนี้เป็นเฉพาะงานของทีม {esc(data["team"])}</div>'
           if data.get("team") else
           f'<h2 style="margin:0 0 10px;font-size:18px">แผนผลิตทอเปลี่ยนแปลง — รอบ Replan '
           f'(สัปดาห์ปัจจุบัน W{cur["week"]})</h2>')
    head = ('<div style="font-family:Segoe UI,Tahoma,sans-serif;font-size:14px;color:#1f2328">'
            + who)
    # แถบโหมดทดสอบ — ต้องอยู่บนสุดก่อนทุกอย่าง ผู้รับต้องรู้ทันทีว่าฉบับนี้ไม่ใช่ของจริง
    if banner:
        head += (f'<div style="margin:0 0 12px;padding:10px 12px;background:#fff1f0;'
                 f'border:1px solid #d1242f;border-radius:6px;color:#b42318">'
                 f'<b>⚠ {esc(banner)}</b></div>')

    chips = []
    for key, label, color in (("late_rdd", "เพิ่งเกิน RDD รอบนี้", "#b42318"),
                              ("late_before", "เกิน RDD อยู่ก่อนแล้ว", "#9a6700"),
                              ("moved", "เลื่อนสัปดาห์", "#9a6700"),
                              ("later", "จบช้ากว่าเดิม", "#9a6700"),
                              ("earlier", "จบเร็วกว่าเดิม", "#1a7f37"),
                              ("qty", "ยอดเปลี่ยน", "#0969da"),
                              ("dropped", "หลุดจากแผน", "#b42318"),
                              ("added", "งานใหม่เข้าแผน", "#0969da")):
        if s.get(key):
            chips.append(
                f'<td style="padding:6px 12px;border:1px solid #d0d7de;border-radius:6px;'
                f'background:#f6f8fa"><b style="color:{color};font-size:16px">{s[key]}</b>'
                f'<span style="color:#57606a"> {label}</span></td>'
                f'<td style="width:8px"></td>')
    if chips:
        head += f'<table cellpadding="0" cellspacing="0"><tr>{"".join(chips)}</tr></table>'

    if note:
        head += (f'<div style="margin:12px 0;padding:10px 12px;background:#fff8c5;'
                 f'border:1px solid #d4a72c;border-radius:6px">📝 {esc(note)}</div>')
    # ไม่ต้องมีบรรทัดโฆษณาไฟล์แนบบนหัวเมล — Outlook โชว์ไฟล์แนบให้เห็นอยู่แล้ว
    # และท้ายตารางยังบอกอยู่ว่ารายการที่เหลืออยู่ในไฟล์นั้น

    # ตารางถูกบีบให้เล็กที่สุดเท่าที่ยังอ่านออก เพราะต้องแสดง **ทุกรายการที่ขยับ** (user สั่ง)
    # แล้ว inline style ที่ยาวคือส่วนที่กินขนาดเมลจริง ๆ ไม่ใช่ตัวข้อมูล (รอบแรกได้ 228 KB
    # ซึ่งเกินเพดานที่ Outlook ยอมแสดงเต็ม ~100 KB): ตัดเหลือ 6 คอลัมน์, ใช้ <small> แทน
    # <div style=...>, และวางเส้นคั่นด้วยแถบสีสลับแทน border รายเซลล์
    # ระยะห่าง/การจัดวางใช้ **attribute ของ HTML** (cellpadding, align, valign, bgcolor)
    # ไม่ใช่ inline style ต่อเซลล์ — เนื้อหาจริงต่อแถวมีไม่ถึงครึ่งของขนาดที่ style กิน
    # (ประหยัดได้ ~55 KB จาก 183 แถว โดยหน้าตาเหมือนเดิมและ Outlook รองรับ attribute พวกนี้ดีกว่า)
    th = 'align="left" style="border-bottom:2px solid #d0d7de;color:#57606a"'
    body = (
        '<table cellpadding="5" cellspacing="0" width="100%" '
        'style="margin-top:14px;border-collapse:collapse;font-size:13px">'
        f'<tr><th {th}>ITEM / ลูกค้า</th><th {th}>SC</th><th {th}>เครื่อง</th>'
        f'<th {th}>สัปดาห์ เดิม → ใหม่</th><th {th}>ยอด (กก.)</th><th {th}>ผลกระทบ / เหตุผล</th></tr>'
    )
    td = 'valign="top"'
    # จำนวนแถวที่ใส่ได้ = เท่าที่ยังอยู่ในเพดานขนาด (หักหัวเมล/ท้ายเมลออกก่อน)
    # ประมาณ 600 bytes/แถว จากของจริง แล้วเผื่อไว้ — รายการที่เหลืออยู่ในไฟล์แนบครบอยู่แล้ว
    shown = data["items"]
    if MAX_ROWS > 0:
        shown = shown[:MAX_ROWS]
    elif limit_bytes > 0:
        room = limit_bytes - len(head.encode("utf-8")) - 2000
        shown = shown[:max(1, room // 620)]
    for n, it in enumerate(shown):
        scs = ", ".join(it["sc"][:6]) + (" …" if len(it["sc"]) > 6 else "")
        # โชว์ปีเมื่อของเดิมกับของใหม่คนละปี — ไม่งั้น "W39 → W22" อ่านเป็นเลื่อนขึ้นมาก่อน
        yy = it["old"].get("year"), it["new"].get("year")
        show_y = bool(yy[0] and yy[1] and yy[0] != yy[1])
        rdd = ""
        if it["late"]:
            rdd = f' · <b style="color:#b42318">⚠ เกิน RDD W{it["rdd_week"]}</b>'
        elif it.get("late_before"):
            rdd = f' · <small>เกิน RDD W{it["rdd_week"]} อยู่ก่อนแล้ว</small>'
        cus = f'<br><small>{esc(it["customer"])}</small>' if it["customer"] else ""
        # แถบสี: แดงจาง = เพิ่งเกิน RDD (ต้องรีบดู) · เทาสลับ = อ่านตารางยาวไม่หลงบรรทัด
        bg = "#fff5f5" if it["late"] else ("#f6f8fa" if n % 2 else "#ffffff")
        # เมลโชว์เหตุผล 3 ข้อแรก (เรียงจากที่ฟันธงได้แน่ที่สุดอยู่แล้ว) — ครบทุกข้ออยู่ในไฟล์แนบ
        # ไม่งั้นบางรายการยาว 7 บรรทัดจนตารางอ่านไม่ทัน (เจอจริงตอนทดสอบ)
        rs = it["reasons"][:3]
        reasons = "<br>".join("• " + esc(r) for r in rs) or "—"
        if len(it["reasons"]) > 3:
            reasons += f'<br><small>(อีก {len(it["reasons"]) - 3} เหตุผล — ดูในไฟล์แนบ)</small>'
        qty_old = (f'<br><small>เดิม {it["old"]["qty"]:,.0f}</small>'
                   if abs(it["d_qty"]) > QTY_TOL else "")
        body += (
            f'<tr bgcolor="{bg}">'
            f'<td {td}><b>{esc(it["item"])}</b>{cus}</td>'
            f'<td {td}>{esc(scs) or "—"}</td>'
            f'<td {td}>{esc(it["mc_group"]) or "—"}</td>'
            f'<td {td}>{_span(it["old"], yy[0], show_y)} → '
            f'<b>{_span(it["new"], yy[1], show_y)}</b></td>'
            f'<td {td}>{it["new"]["qty"]:,.0f}{qty_old}</td>'
            f'<td {td}>{esc(impact_text(it))}{rdd}'
            f'<br><small>{reasons}</small></td>'
            f'</tr>'
        )
    body += "</table>"

    hidden = len(data["items"]) - len(shown)
    if hidden > 0:
        where = (f'อยู่ในไฟล์แนบ <b>{esc(attach_name)}</b> ครบทุกรายการ' if attach_name
                 else 'ดูทั้งหมดได้ในหน้าแผนผลิต (ชีท PLAN)')
        body += (f'<div style="margin-top:8px;padding:8px 12px;background:#f6f8fa;'
                 f'border:1px solid #d0d7de;border-radius:6px;font-size:12px">'
                 f'ตารางข้างบนแสดง {len(shown)} รายการที่ควรดูก่อน (เรียงตามความเร่งด่วน) — '
                 f'อีก {hidden} รายการ{" " + where if attach_name else where}</div>')

    foot = ('<div style="margin-top:16px;color:#8c959f;font-size:11px">'
            'อีเมลนี้ส่งอัตโนมัติจากระบบวางแผนทอ (Knit Plan) '
            'เมื่อผู้วางแผนกดแจ้งการเปลี่ยนแปลงรอบ Replan</div></div>')
    return head + body + foot


# ── บันทึกว่าส่งอะไรไปแล้วบ้าง ───────────────────────────────────────────────

def log_sent(user: str, to: list, cc: list, data: dict, note: str, ok: bool, error: str = "") -> None:
    """ต่อท้าย jsonl 1 บรรทัดต่อการส่ง — ตอบได้ว่าใครแจ้งอะไรไปให้ใครเมื่อไร

    เขียนแบบ append ไม่ใช่เขียนทับ และห้ามให้การเขียน log ทำให้การส่งเมลล้มเหลว
    (เมลออกไปแล้ว จะย้อนกลับไม่ได้ — log พังต้องเงียบ ไม่ใช่โยน error กลับหน้าเว็บ)
    """
    rec = {"at": datetime.now().isoformat(timespec="seconds"), "user": user,
           "to": to, "cc": cc, "ok": ok, "error": error, "note": note,
           "plan_file": data.get("plan_file"), "summary": data.get("summary"),
           "items": [{"item": i["item"], "sc": i["sc"], "kinds": i["kinds"],
                      "old": i["old"], "new": i["new"]} for i in data.get("items", [])]}
    try:
        _LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass

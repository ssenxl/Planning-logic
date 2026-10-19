"""
yarn_lock.py — ฝั่งด้ายล็อกด้ายของ lot ไว้ให้ SC + ผ้า (ทำที่หน้า 🧶 แผนด้าย — yarn_plan.py / YarnPlan.jsx)

ย้ายที่ล็อกจากหน้า Yarn Ava มาหน้าแผนด้าย 15 ต.ค. 2569 (user: "ต้องไม่ใช่ทำไว้หน้านี้ ควรเป็นแบบแผนทอที่เห็นเลยว่า
week นี้ใช้ด้ายอะไร เท่าไหร่") ; หน้า Yarn Ava ถอดออกทั้งหน้า (รวมการ์ดรายการล็อก) ในวันเดียวกัน — user ไม่ใช้แล้ว
options() ยังใช้ตรวจตอนบันทึก (ใบนี้ใช้ด้าย/สีของ lot นี้จริง + ยังค้างทอ)

user 15 ต.ค. 2569: "ฝั่งเส้นด้ายต้องล็อกเส้นด้ายของ lot นั้นไว้ให้ผ้าตัวไหนจำนวนเท่าไหร่ SC อะไร
                    แล้วแผนทอต้องทำตามที่ฝั่งเส้นด้ายล็อกไว้สำหรับ lot นั้น ๆ"
กติกาที่ user เลือก:
  - ล็อก = กันด้ายอย่างเดียว ไม่เปลี่ยนวันของใบ (ใบที่รอ PO IN ยังรอตามเดิม)
  - กันเท่ายอดที่ล็อก — ด้ายที่เหลือใน lot ใบอื่นยังใช้ได้ ; lot เดียวล็อกให้หลายใบได้ รวมกันห้ามเกินยอดใช้ได้
  - กรอกเป็น kg ผ้า → kg ด้าย = kg ผ้า × % ด้ายเส้นนั้นในผ้า × ตัวคูณสูญเสีย (สูตรเดียวกับแผนยืมสต๊อก)
  - ตอนแผนเช็กว่าใบเลิกรอ PO IN ได้ไหม (KP_YARN_STOCK_ON) ด้ายที่ล็อกให้ใบนั้นนับเป็นของใบนั้น (ข้อ ก)
    ใบอื่นยืมได้แค่กองกลาง (ยอดใช้ได้ที่หักล็อกแล้ว)
  - ออกใบ KP แล้วยอดล็อกลดเอง (ด้ายส่วนนั้นไปนับเป็น "ใบ KP จอง" / ถูกโอนออกจากสต๊อกแล้ว ไม่งั้นหักซ้ำ)
    ; booking ของ SC + ผ้านั้นปิด (Closed) หมดทุกแถว = ไม่นับ

สถานะ (ลดตาม KP / ปิดแล้ว) คิดใหม่ทุกครั้งที่อ่าน ไม่เขียนกลับไฟล์ — ไฟล์ booking ว่าง/ไม่ครบตอนกำลัง refresh
(ดู booking_empty_morning_run) จะทำให้ล็อกหายถาวรไม่ได้ ; ล็อกถูกลบออกจากไฟล์เฉพาะตอนคนกดปลดเท่านั้น

ใบ KP → SC ใช้ FR_CODE ของ booking (S{SC}-ORDER_TYPE ; ยุบหลายใบด้วย ' | ') — SO_NO ของ booking ปนเลข SO อีกระบบ
ใบ KP ใหม่ที่ booking ยัง refresh ไม่ถึง จะยังไม่ลดยอดล็อก (หักซ้อนกับยอดจองของใบ KP ชั่วคราว) จนรอบ refresh ถัดไป
ยอดด้ายของใบ KP = YARN_PROD_KGS (BOM + สูญเสีย ; ว่าง → YARN_BOM_KG) ของด้ายเส้นที่ล็อก จาก view JOKNIT
ใบ KP หนึ่งใบตัดล็อกของ SC + ผ้า + ด้าย เดียวกันตามลำดับเวลาล็อก (ล็อกเก่าก่อน) และนับเฉพาะใบที่ออกหลังเวลาล็อก

ไฟล์ (อยู่ใน data_plan = volume บน server ไม่หายตอน deploy):
  yarn_locks.json      ล็อกที่ยังอยู่ — ปลด = ลบออก
  yarn_lock_log.jsonl  ประวัติ ล็อก/ปลด ใคร เมื่อไหร่ ต่อท้ายอย่างเดียว ไม่เคยลบ

ผู้ใช้: yarn_ava_live.table (หักล็อกออกจากยอดใช้ได้ของ lot) → YarnStockCover (แผน) · server.py /api/yarn-locks
"""
import json
import math
import re
import sys
import uuid
from datetime import datetime

import config

if str(config.REPO_DIR) not in sys.path:
    sys.path.append(str(config.REPO_DIR))      # YarnStockCover อยู่ราก repo (ต่อท้าย — ไม่บังโมดูลของ webapp)

LOCK_FILE = config.OUTPUT_DIR / "yarn_locks.json"
LOG_FILE = config.OUTPUT_DIR / "yarn_lock_log.jsonl"
RESOURCE = "yarn_lock"                       # ชื่อชุดข้อมูลของ livesync (หน้า Yarn Ava / Gantt โหลดใหม่เอง)

_BOOKING = config.REPO_DIR / "Booking" / "view_booking.xlsx"
_SPLIT = config.REPO_DIR / "Yarn" / "view_yarn_by_joknit.xlsx"
_SPLIT_SHEET = 0   # ชีทแรก — ไฟล์มีชีทเดียว (ชื่อชีท = ชื่อ view ไม่อ้างในโค้ด)
_SCFOL = config.REPO_DIR / "Order" / "view_scfol_yarn.xlsx"
_SC_ITEM = config.REPO_DIR / "Order" / "view_scfol_item.xlsx"
_BOM = config.REPO_DIR / "Yarn" / "view_bom_yarn_pct.xlsx"

_EPS = 0.5          # kg — เศษที่ถือว่าหมด/พอดี (ตรงกับ allocate_lots ของ YarnStockCover)
_LOG_READ_MAX = 300


class LockError(Exception):
    """ล็อก/ปลดไม่ได้ — ข้อความให้คนอ่าน (server ตอบ 400)"""


# =============================================================================
# ตัวช่วย
# =============================================================================
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _txt(v) -> str:
    s = "" if v is None else str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat", "<na>") else s


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _mt(p):
    try:
        return p.stat().st_mtime
    except OSError:
        return None


def _ts(v):
    """วันเวลา (ข้อความ/Timestamp) → datetime ; อ่านไม่ได้ = None"""
    if v is None or v == "":
        return None
    try:
        import pandas as pd
        t = pd.Timestamp(v)
    except Exception:
        return None
    return None if pd.isna(t) else t.to_pydatetime().replace(tzinfo=None)


def sc_digits(v) -> str:
    """เลขใบ SC ให้เป็นตัวเลขล้วน : 'S721655' / '721655' / 721655.0 → '721655' ; ไม่ใช่เลขใบ = ''"""
    s = _txt(v).upper()
    if s.endswith(".0"):
        s = s[:-2]
    m = re.fullmatch(r"S?(\d+)", s)
    return m.group(1) if m else ""


def _fr_scs(fr) -> list:
    """FR_CODE ของ booking 'S721655-SC-ORDERS' (ยุบหลายใบด้วย ' | ') → ['721655', ...]
    ห้าม split('-') ทั้งช่อง (ดู booking_view_field_map) ; '-SC-ORDERS' ที่ไม่มีเลขใบ = ข้าม"""
    out = []
    for part in _txt(fr).split("|"):
        m = re.match(r"\s*S(\d+)", part.strip().upper())
        if m and m.group(1) not in out:
            out.append(m.group(1))
    return out


def _ysc():
    import YarnStockCover
    return YarnStockCover


# =============================================================================
# ไฟล์ล็อก + ประวัติ
# =============================================================================
def read_locks(strict: bool = False) -> list:
    """ล็อกทั้งหมดในไฟล์ ; strict=True (ทางเขียน) = ไฟล์เสียแล้วโยน error แทนการคืน [] — กันเขียนทับล็อกทั้งหมดหาย"""
    try:
        data = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError) as e:
        if strict:
            raise LockError(f"อ่านไฟล์ล็อกด้าย {LOCK_FILE.name} ไม่ได้ ({e}) — ยังไม่บันทึกทับ แจ้งผู้ดูแลระบบ")
        print(f"[YARN LOCK] อ่าน {LOCK_FILE.name} ไม่ได้: {e}")
        return []
    return [d for d in (data.get("locks") or []) if isinstance(d, dict) and d.get("id")]


def _write_locks(locks: list) -> None:
    import livesync
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with livesync.atomic_save(LOCK_FILE) as tmp:
        tmp.write_text(json.dumps({"locks": locks}, ensure_ascii=False, indent=1), encoding="utf-8")


def _log(action: str, by: str, lock: dict, **extra) -> None:
    """ต่อท้ายประวัติ 1 บรรทัด — ตอบได้ว่าใครล็อก/ปลดอะไรเมื่อไหร่ (ไม่ทำให้การบันทึกล้มถ้าเขียนไม่ได้)"""
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"at": _now(), "by": by or "", "action": action, "lock": lock, **extra},
                               ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[YARN LOCK] เขียน {LOG_FILE.name} ไม่ได้: {e}")


def read_log(limit: int = 100) -> list:
    """ประวัติล่าสุดก่อน"""
    if not LOG_FILE.exists():
        return []
    rows = []
    try:
        with LOG_FILE.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except OSError:
        return []
    return rows[-max(1, min(int(limit or 100), _LOG_READ_MAX)):][::-1]


# =============================================================================
# ข้อมูลประกอบ (cache ตามเวลาไฟล์)
# =============================================================================
_cache: dict = {}


def _cached(name: str, paths: tuple, fn):
    key = tuple(_mt(p) for p in paths)
    hit = _cache.get(name)
    if hit is not None and hit[0] == key:
        return hit[1]
    val = fn()
    _cache[name] = (key, val)
    return val


def _load_booking() -> dict:
    """booking → {"rows": {(SC, ผ้า): {"lines", "pending", "st", "cus"}}, "kp_sc": {KP: {SC}}, "n": จำนวนแถว}"""
    import pandas as pd

    out = {"rows": {}, "kp_sc": {}, "n": 0}
    if _mt(_BOOKING) is None:
        return out
    want = ("FR_CODE", "KP_NO", "ITEM_CODE", "STATUS", "SC_LINE_ID", "OUTSTANDING", "KNIT_CUS_NAME")
    try:
        bk = pd.read_excel(_BOOKING, usecols=lambda c: str(c).strip().upper() in want)
    except Exception as e:
        print(f"[YARN LOCK] อ่าน {_BOOKING.name} ไม่ได้: {e}")
        return out
    bk.columns = bk.columns.str.strip().str.upper()
    if "FR_CODE" not in bk.columns or "ITEM_CODE" not in bk.columns:
        return out
    ysc = _ysc()
    out["n"] = len(bk)
    for r in bk.to_dict("records"):
        scs = _fr_scs(r.get("FR_CODE"))
        fab = _txt(r.get("ITEM_CODE")).upper()
        if not scs or not fab:
            continue
        st = _txt(r.get("STATUS")).lower()
        # เลขบรรทัด 6 หลักเท่านั้น — CL/YD เป็น 0, FQC/LAB-DIP เป็นเลข 1–2 หลักคนละระบบ
        lines = {ln for ln in ysc.lines_of(r.get("SC_LINE_ID")) if ln >= 100000}
        outst = _num(r.get("OUTSTANDING")) or 0.0
        kp = _txt(r.get("KP_NO")).upper()
        for sc in scs:
            a = out["rows"].setdefault((sc, fab), {"lines": set(), "pending": 0.0, "st": set(), "cus": ""})
            a["lines"] |= lines
            a["st"].add(st)
            if st != "closed" and outst > 0:
                a["pending"] += outst
            if not a["cus"]:
                a["cus"] = _txt(r.get("KNIT_CUS_NAME"))
            if kp:
                out["kp_sc"].setdefault(kp, set()).add(sc)
    return out


def _booking() -> dict:
    return _cached("booking", (_BOOKING,), _load_booking)


def _load_kp_yarn() -> dict:
    """ด้ายที่ใบ KP ใช้ (ทุกใบในไฟล์) → {(KP, ผ้า, ด้าย): {"kg", "t": เวลาออกใบ}} — ด้าย/ผ้าพิมพ์ใหญ่
    ล็อก SC ผูกใบกับ SC ผ่าน booking (_load_kp_events) ; ล็อก Item (yarn_item_lock.py) ใช้ผ้าของใบตรง ๆ"""
    import pandas as pd

    if _mt(_SPLIT) is None:
        return {}
    try:
        try:
            df = pd.read_excel(_SPLIT, sheet_name=_SPLIT_SHEET)
        except ValueError:
            df = pd.read_excel(_SPLIT)
    except Exception as e:
        print(f"[YARN LOCK] อ่าน {_SPLIT.name} ไม่ได้: {e}")
        return {}
    df.columns = df.columns.str.strip().str.upper()
    if not {"KP_NO", "ITEM_CODE", "YARN_CODE", "CREATED_DATE"} <= set(df.columns):
        return {}
    seen, per = set(), {}
    for r in df.to_dict("records"):
        kp = _txt(r.get("KP_NO")).upper()
        if not kp:
            continue
        fab, yarn = _txt(r.get("ITEM_CODE")).upper(), _txt(r.get("YARN_CODE")).upper()
        kg = _num(r.get("YARN_PROD_KGS"))
        if kg is None:
            kg = _num(r.get("YARN_BOM_KG")) or 0.0
        # ไฟล์ดึงแบบ DISTINCT แล้ว — กันซ้ำอีกชั้นด้วยคีย์เต็มแถวแบบเดียวกับ yarn_ava_live._reserved
        k = (kp, fab, yarn, _txt(r.get("YARN_LOT")), _num(r.get("YARN_CONTAIN")), kg)
        if not fab or not yarn or kg <= 0 or k in seen:
            continue
        seen.add(k)
        t = _ts(r.get("CREATED_DATE"))
        a = per.setdefault((kp, fab, yarn), {"kg": 0.0, "t": t})
        a["kg"] += kg
        if t is not None and (a["t"] is None or t < a["t"]):
            a["t"] = t
    return per


def kp_yarn() -> dict:
    return _cached("kp_yarn", (_SPLIT,), _load_kp_yarn)


def _load_kp_events() -> dict:
    """ใบ KP → ด้ายที่ใบใช้ {(SC, ผ้า, ด้าย): [(เวลาออกใบ, KP, kg)]} — ผูกใบกับ SC ผ่าน FR_CODE ของ booking"""
    kp_sc = _booking()["kp_sc"]
    out: dict = {}
    for (kp, fab, yarn), a in kp_yarn().items():
        if kp not in kp_sc:
            continue
        if a["t"] is None:
            continue                          # ไม่รู้วันออกใบ → เทียบกับเวลาล็อกไม่ได้ ไม่ตัดยอด
        for sc in kp_sc[kp]:
            out.setdefault((sc, fab, yarn), []).append((a["t"], kp, a["kg"]))
    return out


def _kp_events() -> dict:
    return _cached("kp_events", (_SPLIT, _BOOKING), _load_kp_events)


def _load_scfol() -> dict:
    """ด้ายรายบรรทัดใบ → {(ด้าย, สี): {SC: {LINE_ID: (pct, loss)}}} — % / สูญเสียสูตรเดียวกับ YarnStockCover"""
    import pandas as pd

    if _mt(_SCFOL) is None:
        return {}
    ysc = _ysc()
    want = ("SC_NO", "LINE_ID", "Y_YARN_ITEM", "Y_TOPDYED_COLOR", "Y_CONTAIN_PCT", "Y_KNIT_QTY", "Y_YARN_QTY_LOST")
    df = pd.read_excel(_SCFOL, usecols=lambda c: str(c).strip().upper() in want)
    df.columns = df.columns.str.strip().str.upper()
    df["_SC"] = df["SC_NO"].map(sc_digits)
    df["_LINE"] = pd.to_numeric(df["LINE_ID"], errors="coerce")
    df["_Y"] = df["Y_YARN_ITEM"].map(_txt).str.upper()
    df = df[(df["_SC"] != "") & df["_LINE"].notna() & (df["_Y"] != "")].copy()
    df["_C"] = df["Y_TOPDYED_COLOR"].map(ysc.color_of) if "Y_TOPDYED_COLOR" in df.columns else ""
    for c in ("Y_CONTAIN_PCT", "Y_KNIT_QTY", "Y_YARN_QTY_LOST"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce").fillna(0)
    # % / ยอด ซ้ำทุกแถว PO ของ (บรรทัด, ด้าย, สี) เดียวกัน → ค่าสูงสุดตัวเดียว (ตรง load_yarn_lines)
    g = df.groupby(["_Y", "_C", "_SC", "_LINE"]).agg(
        pct=("Y_CONTAIN_PCT", "max"), knit=("Y_KNIT_QTY", "max"), lost=("Y_YARN_QTY_LOST", "max"))
    out: dict = {}
    for (y, c, sc, line), r in g.iterrows():
        if r["pct"] > 0:
            out.setdefault((y, c), {}).setdefault(sc, {})[int(line)] = (
                float(r["pct"]), ysc.loss_factor(r["knit"], r["lost"]))
    return out


def _scfol() -> dict:
    return _cached("scfol", (_SCFOL,), _load_scfol)


def _load_sc_items() -> dict:
    """ใบ SC ที่ยังค้างทอ (view_scfol_item) → {(SC, ผ้า): KNIT_KGS}"""
    import pandas as pd

    if _mt(_SC_ITEM) is None:
        return {}
    try:
        df = pd.read_excel(_SC_ITEM)
    except Exception as e:
        print(f"[YARN LOCK] อ่าน {_SC_ITEM.name} ไม่ได้: {e}")
        return {}
    df.columns = df.columns.str.strip().str.upper()
    out = {}
    for r in df.to_dict("records"):
        sc, fab = sc_digits(r.get("SC_NO")), _txt(r.get("ITEM_CODE")).upper()
        if sc and fab:
            out[(sc, fab)] = (out.get((sc, fab)) or 0.0) + (_num(r.get("KNIT_KGS")) or 0.0)
    return out


def _sc_items() -> dict:
    return _cached("sc_items", (_SC_ITEM,), _load_sc_items)


def _load_bom() -> dict:
    """% ด้ายในผ้าตาม BOM ของใบ KP → {(ผ้า, ด้าย): %} (ใช้เมื่อหาบรรทัดใบไม่เจอ)"""
    import pandas as pd

    if _mt(_BOM) is None:
        return {}
    try:
        df = pd.read_excel(_BOM)
    except Exception as e:
        print(f"[YARN LOCK] อ่าน {_BOM.name} ไม่ได้: {e}")
        return {}
    df.columns = df.columns.str.strip().str.upper()
    if not {"KNIT_ITEM", "YARN_ITEM", "ITEM_CONTAIN"} <= set(df.columns):
        return {}
    df["_F"] = df["KNIT_ITEM"].map(_txt).str.upper()
    df["_Y"] = df["YARN_ITEM"].map(_txt).str.upper()
    df["_P"] = pd.to_numeric(df["ITEM_CONTAIN"], errors="coerce")
    df = df[(df["_F"] != "") & (df["_Y"] != "") & (df["_P"] > 0)]
    # ผ้าเดียวมีหลายใบ KP — ใช้ค่าที่เจอบ่อยสุด (เสมอกันเอาค่ามาก)
    out = {}
    for (f, y), s in df.groupby(["_F", "_Y"])["_P"]:
        vc = s.round(2).value_counts()
        out[(f, y)] = float(max(vc[vc == vc.max()].index))
    return out


def _bom() -> dict:
    return _cached("bom", (_BOM,), _load_bom)


# =============================================================================
# สถานะล็อก (คิดใหม่ทุกครั้ง ไม่เขียนกลับ)
# =============================================================================
def statuses(locks: list = None) -> dict:
    """id → {"rem": kg ด้ายที่ยังล็อกอยู่, "kp": kg ที่ใบ KP ใช้ไปแล้ว, "kps": [ใบ KP], "state", "why"}
    state: active = ยังกันด้ายอยู่ · done = ออกใบ KP ครบยอดแล้ว · closed = booking ของ SC + ผ้าปิดหมดแล้ว"""
    return _settle(read_locks() if locks is None else locks)[0]


def kp_taken() -> dict:
    """ใบ KP ตัดล็อก SC ไปเท่าไหร่ → {(KP, ผ้า, ด้าย พิมพ์ใหญ่): kg}
    ล็อกด้ายให้ Item (yarn_item_lock.py) ตัดเฉพาะส่วนที่เหลือของใบ KP — ใบเดียวห้ามลดล็อกสองแบบซ้ำ"""
    locks = read_locks()
    return _settle(locks)[1] if locks else {}


def _settle(locks: list) -> tuple:
    """ไล่ใบ KP ตัดยอดล็อก → (สถานะต่อ id, {(KP, ผ้า, ด้าย): kg ที่ตัดล็อกไป})"""
    taken: dict = {}
    if not locks:
        return {}, taken
    bk = _booking()
    ev = _kp_events()
    groups: dict = {}
    for L in locks:
        groups.setdefault((L.get("so"), _txt(L.get("fabric")).upper(), _txt(L.get("yarn")).upper()), []).append(L)
    out = {}
    for (sc, fab, yarn), ls in groups.items():
        ls.sort(key=lambda L: (_ts(L.get("at")) or datetime.min, L["id"]))
        rem = {L["id"]: float(L.get("yarn_kg") or 0) for L in ls}
        used = {L["id"]: 0.0 for L in ls}
        kps = {L["id"]: [] for L in ls}
        for t, kp, kg in sorted(ev.get((sc, fab, yarn), [])):
            left = kg
            for L in ls:
                if left <= 1e-9:
                    break
                lt = _ts(L.get("at"))
                if lt is None or t < lt or rem[L["id"]] <= 1e-9:
                    continue                  # ใบที่ออกก่อนล็อก ไม่เกี่ยวกับล็อกนี้
                take = min(rem[L["id"]], left)
                rem[L["id"]] -= take
                used[L["id"]] += take
                kps[L["id"]].append(kp)
                left -= take
                taken[(kp, fab, yarn)] = taken.get((kp, fab, yarn), 0.0) + take
        info = bk["rows"].get((sc, fab))
        closed = bool(info and info["st"] and info["st"] <= {"closed"})
        for L in ls:
            i = L["id"]
            if closed:
                st, why = "closed", "booking ของ SC + ผ้านี้ปิด (Closed) หมดแล้ว — ไม่กันด้ายแล้ว"
            elif rem[i] <= _EPS:
                st, why = "done", "ออกใบ KP ครบยอดที่ล็อกแล้ว — ด้ายไปนับเป็นยอดจองของใบ KP"
            else:
                st, why = "active", ""
            out[i] = {"rem": round(max(rem[i], 0.0), 3), "kp": round(used[i], 3), "kps": kps[i],
                      "state": st, "why": why}
    return out, taken


def state_key() -> tuple:
    """ใส่ใน cache key ของ yarn_ava_live — ล็อกเปลี่ยน / booking / ใบ KP เปลี่ยน = คิดยอดใช้ได้ใหม่"""
    return (_mt(LOCK_FILE), _mt(_BOOKING), _mt(_SPLIT))


def active_by_lot() -> dict:
    """ล็อกที่ยังกันด้ายอยู่ ต่อ lot → {(ด้าย พิมพ์ใหญ่, lot): [{"id", "sc", "so", "fabric", "rem", "at"}]}
    เรียงตามเวลาล็อก (ล็อกก่อนได้ยอดก่อน ถ้า lot มีไม่พอทุกล็อก)"""
    locks = read_locks()
    if not locks:
        return {}
    st = statuses(locks)
    out: dict = {}
    for L in locks:
        s = st.get(L["id"])
        if not s or s["state"] != "active":
            continue
        out.setdefault((_txt(L.get("yarn")).upper(), _txt(L.get("lot"))), []).append({
            "id": L["id"], "kind": "sc", "sc": L.get("sc"), "so": L.get("so"), "fabric": L.get("fabric"),
            "rem": s["rem"], "at": L.get("at") or ""})
    for ls in out.values():
        ls.sort(key=lambda d: (d["at"], d["id"]))
    return out


# =============================================================================
# ตัวเลือกตอนล็อก : ใบไหนใช้ด้ายตัวนี้ + แปลง kg ผ้า → kg ด้าย
# =============================================================================
def _factor(sc: str, fab: str, yarn: str, sc_lines: dict, n_fabrics: int) -> dict | None:
    """% ด้าย + ตัวคูณสูญเสีย ของ SC + ผ้า → {"pct", "loss", "factor", "basis"} ; หาไม่ได้ = None
    ลำดับ: บรรทัดใบจาก booking (ตรงกับที่แผนใช้) → % ใน BOM ของผ้า → ใบมีผ้าตัวเดียวและ % ของด้ายในใบค่าเดียว"""
    bk = _booking()["rows"].get((sc, fab))
    if bk and bk["lines"]:
        hit = sorted(bk["lines"] & set(sc_lines))
        if not hit:
            return None                       # บรรทัดของผ้านี้ไม่ใช้ด้ายตัวนี้
        pct = max(sc_lines[ln][0] for ln in hit)
        loss = max(sc_lines[ln][1] for ln in hit)
        basis = "บรรทัดใบ " + ", ".join(map(str, hit))
    else:
        loss = max((v[1] for v in sc_lines.values()), default=1.0)
        pct = _bom().get((fab, yarn))
        basis = "% ด้ายใน BOM ของผ้า"
        if not pct:
            pcts = {round(v[0], 2) for v in sc_lines.values()}
            if n_fabrics != 1 or len(pcts) != 1:
                return None
            pct, basis = next(iter(pcts)), "% ด้ายของใบ (ใบมีผ้าตัวเดียว)"
    return {"pct": round(pct, 4), "loss": round(loss, 4), "factor": pct / 100.0 * loss, "basis": basis}


def _open_fabrics() -> dict:
    """ใบที่ยังมียอดต้องทอ → {SC: {ผ้า}} : booking ที่ยังไม่ปิดและค้าง (OUTSTANDING > 0) หรือ SC ค้างทอ (KNIT_KGS > 0)
    — booking ที่ STATUS ยัง Pending แต่ทอครบแล้วมีเยอะ (ข้อมูล 14 ต.ค.: SPD20D 866 ใบ ส่วนใหญ่ค้าง 0)"""
    out: dict = {}
    for (sc, fab), a in _booking()["rows"].items():
        if a["pending"] > 0:
            out.setdefault(sc, set()).add(fab)
    for (sc, fab), kg in _sc_items().items():
        if kg > 0:
            out.setdefault(sc, set()).add(fab)
    return out


def factor_for(so, fabric, yarn, color="") -> dict | None:
    """% ด้าย + สูญเสีย ของ SC + ผ้า สำหรับด้าย (สี) นี้ — ตัวเดียวกับที่ add() ใช้แปลง kg ผ้า → kg ด้าย
    (หน้าแผนด้ายใช้ตั้งยอดเริ่มต้นตอนล็อก) ; ใบปิดแล้ว / ไม่ใช้ด้ายนี้ / หา % ไม่ได้ = None"""
    sc, fab = sc_digits(so), _txt(fabric).upper()
    sc_lines = _scfol().get((_txt(yarn).upper(), _txt(color).upper()), {}).get(sc)
    fabs = _open_fabrics().get(sc)
    if not sc_lines or not fabs or fab not in fabs:
        return None
    return _factor(sc, fab, _txt(yarn).upper(), sc_lines, len(fabs))


def options(yarn: str, lot: str, hide=()) -> dict:
    """ใบที่ล็อกด้าย lot นี้ได้ → {"yarn", "lot", "color", "scs": [{"sc", "so", "cus", "fabrics": [...]}], "note"}
    ใบ = มีบรรทัดที่ใช้ด้ายตัวนี้ (สีตรงกับ lot สำหรับด้าย TD) และยังค้างอยู่ (booking ที่ยังไม่ปิด หรือ SC ค้างทอ)"""
    ysc = _ysc()
    y = _txt(yarn).upper()
    lot = _txt(lot)
    color, _age, meta_note = ysc.lot_meta(hide)
    c = color.get((y, lot), "") if color else ""
    notes = []
    if not color:
        notes.append(meta_note or "ไฟล์สต๊อกไม่มีสีของ lot — ล็อกได้เฉพาะใบที่ใช้ด้ายดิบ")
    if _mt(_SCFOL) is None:
        notes.append(f"ยังไม่มีไฟล์ {_SCFOL.name} (View_SCFol_Yarn) — ไม่รู้ว่าใบไหนใช้ด้ายตัวนี้")
    by_sc = _scfol().get((y, c), {})
    bk = _booking()["rows"]
    sci = _sc_items()
    fabs_of = _open_fabrics()
    scs = []
    for sc, sc_lines in by_sc.items():
        fabs = fabs_of.get(sc)
        if not fabs:
            continue                          # ใบปิดแล้ว / ไม่อยู่ในรายการค้าง
        rows = []
        for fab in sorted(fabs):
            f = _factor(sc, fab, y, sc_lines, len(fabs))
            if f is None:
                continue
            b = bk.get((sc, fab))
            rows.append({"fabric": fab, **f,
                         "pending": round(b["pending"], 1) if b and b["pending"] > 0 else None,
                         "sc_kg": round(sci[(sc, fab)], 1) if sci.get((sc, fab)) else None})
        if rows:
            cus = next((bk[(sc, r["fabric"])]["cus"] for r in rows if bk.get((sc, r["fabric"]), {}).get("cus")), "")
            scs.append({"sc": "S" + sc, "so": sc, "cus": cus, "fabrics": rows})
    scs.sort(key=lambda d: d["sc"])
    return {"yarn": yarn, "lot": lot, "color": c, "scs": scs, "note": " · ".join(notes)}


# =============================================================================
# ล็อก / ปลด
# =============================================================================
def _find_row(table: dict, yarn: str, lot: str):
    names = [str(y["name"]).strip().upper() for y in table.get("yarns") or []]
    y = _txt(yarn).upper()
    for r in table.get("rows") or []:
        if r.get("l") is not None and names[r["g"]] == y and str(r["l"]) == lot:
            return r
    return None


def add(user: str, yarn: str, lot: str, sc: str, fabric: str, fabric_kg, note: str,
        ava_table, hide=()) -> dict:
    """ล็อกด้าย lot ให้ SC + ผ้า ; ava_table() = ตาราง Yarn Ava ปัจจุบัน (คิดในล็อกไฟล์ — กันสองคนล็อกเกินพร้อมกัน)
    → {"lock", "warn"} ; ล็อกไม่ได้ = LockError"""
    import livesync

    yarn, lot, fab = _txt(yarn), _txt(lot), _txt(fabric).upper()
    so = sc_digits(sc)
    kg = _num(fabric_kg)
    if not yarn or not lot:
        raise LockError("ต้องระบุด้ายและ lot")
    if not so:
        raise LockError(f"เลข SC ไม่ถูกต้อง: {sc!r}")
    if not fab:
        raise LockError("ต้องเลือกผ้า")
    if kg is None or kg <= 0:
        raise LockError("จำนวน kg ผ้าต้องมากกว่า 0")

    with livesync.file_lock(LOCK_FILE):
        locks = read_locks(strict=True)
        opt = options(yarn, lot, hide)
        sc_opt = next((s for s in opt["scs"] if s["so"] == so), None)
        f = next((r for r in (sc_opt or {}).get("fabrics", []) if r["fabric"] == fab), None)
        if f is None:
            raise LockError(f"SC {so} ผ้า {fab} ไม่ได้ใช้ด้าย {yarn}"
                            + (f" สี {opt['color']}" if opt["color"] else "") + " (หรือใบปิดแล้ว / หา % ด้ายไม่ได้)")
        row = _find_row(ava_table(), yarn, lot)
        if row is None or row.get("w") is None:
            raise LockError(f"ไม่พบ lot {lot} ของด้าย {yarn} ในสต๊อก")
        if row.get("low"):
            raise LockError(f"lot {lot} เป็นลูกเล็กทั้ง lot — ใช้ไม่ได้ ล็อกไม่ได้")
        ykg = kg * f["factor"]
        ava = float(row.get("ava") or 0)
        if ykg > ava + _EPS:
            raise LockError(f"ด้ายไม่พอ: ผ้า {kg:,.1f} kg = ด้าย {ykg:,.1f} kg แต่ lot {lot} ใช้ได้อีก {max(ava, 0):,.1f} kg "
                            f"(หลังหักใบ KP ที่จองและล็อกอื่นแล้ว)")
        L = {"id": uuid.uuid4().hex[:12], "yarn": yarn, "lot": lot, "color": opt["color"],
             "sc": "S" + so, "so": so, "fabric": fab, "fabric_kg": round(kg, 3),
             "pct": f["pct"], "loss": f["loss"], "yarn_kg": round(ykg, 3), "basis": f["basis"],
             "note": _txt(note)[:300], "at": _now(), "by": user or ""}
        locks.append(L)
        _write_locks(locks)
    _log("lock", user, L)
    livesync.bump(RESOURCE, user, f"ล็อกด้าย {yarn} lot {lot} {ykg:,.1f} kg ให้ SC {so} {fab}")
    warn = []
    if row.get("own"):
        warn.append(f"lot นี้สงวนให้ลูกค้า {', '.join(row['own'])} ตามกติกาหน้า Monitor Yarn — ตรวจว่า SC นี้เป็นของลูกค้านั้น")
    return {"lock": L, "warn": warn}


def remove(user: str, lock_id: str) -> dict:
    """ปลดล็อก = ลบออกจากไฟล์ + จดประวัติ (พร้อมสถานะตอนปลด)"""
    import livesync

    with livesync.file_lock(LOCK_FILE):
        locks = read_locks(strict=True)
        L = next((d for d in locks if d["id"] == lock_id), None)
        if L is None:
            raise LockError("ไม่พบล็อกนี้ (อาจมีคนปลดไปแล้ว)")
        st = statuses([L]).get(lock_id, {})
        _write_locks([d for d in locks if d["id"] != lock_id])
    _log("unlock", user, L, state=st.get("state"), rem=st.get("rem"))
    livesync.bump(RESOURCE, user, f"ปลดล็อกด้าย {L['yarn']} lot {L['lot']} ของ SC {L['so']} {L['fabric']}")
    return {"ok": True}


def listing(table: dict) -> dict:
    """ล็อกทุกตัว + สถานะ + ยอดที่ lot กันให้จริง (จากตาราง Yarn Ava) + ประวัติล่าสุด — หน้า Yarn Ava / Gantt"""
    locks = read_locks()
    st = statuses(locks)
    eff = {}
    for r in table.get("rows") or []:
        for lk in r.get("locks") or []:
            eff[lk["id"]] = lk.get("kg")
    out = []
    for L in sorted(locks, key=lambda d: d.get("at") or "", reverse=True):
        s = st.get(L["id"], {"rem": L.get("yarn_kg"), "kp": 0.0, "kps": [], "state": "active", "why": ""})
        e = eff.get(L["id"])
        if s["state"] == "active" and e is None:
            s = {**s, "why": "ไม่พบ lot นี้ในสต๊อก (หรือเป็นลูกเล็กทั้ง lot) — ยังไม่ได้กันด้าย"}
        elif s["state"] == "active" and e is not None and e + _EPS < s["rem"]:
            s = {**s, "why": f"lot มีให้กันได้แค่ {e:,.1f} kg จาก {s['rem']:,.1f} kg"}
        f = float(L.get("pct") or 0) / 100.0 * float(L.get("loss") or 1)
        out.append({**L, **s, "eff": e if s["state"] == "active" else 0.0,
                    "rem_fabric": round(s["rem"] / f, 1) if f > 0 else None})
    return {"locks": out, "log": read_log(100)}

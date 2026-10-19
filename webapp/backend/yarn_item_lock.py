"""
yarn_item_lock.py — ฝั่งด้ายล็อกด้ายไว้ให้ Item ผ้า (ไม่ผูกใบ SC) : หน้า Monitor Yarn › แท็บ 🔒 ล็อกด้าย (YarnLockMonitor.jsx)

user 15 ต.ค. 2569: "ต้องมีอีกหน้านึงสำหรับให้ Yarn ไว้ล็อกเส้นด้ายตัวนั้นๆ ให้สำหรับ Item นั้นๆ จำนวนเท่าไหร่
                    เป็นหน้า Monitor ตัวนั้นโดยเฉพาะ โดยแผนทอก็ต้องดูจากตัวนี้ไม่สามารถเอาด้ายตัวที่ล็อกไปให้ตัวอื่นได้"
กติกาที่ user เลือก:
  - ระบุ lot หรือไม่ระบุก็ได้ — ไม่ระบุ = กันจากด้ายตัวนั้น (สีเดียวกัน) lot ที่ด้ายเข้าก่อนก่อน
    (ข้าม lot ลูกเล็กทั้ง lot / lot ที่สงวนให้ลูกค้าตามกติกา Monitor Yarn) — yarn_ava_live เป็นคนแบ่งลง lot
  - กรอกเป็น kg ด้าย (ไม่ใช่ kg ผ้าแบบล็อก SC)
  - หน้า = แท็บในหน้า Monitor Yarn ; ล็อกแบบ SC เดิม (yarn_lock.py ล็อกที่หน้าแผนด้าย) ยังอยู่ หน้านี้แสดงทั้ง 2 แบบ
ตามกติกาของล็อก SC เดิม: กันเท่ายอดที่ล็อก ที่เหลือของ lot ใบอื่นใช้ได้ · ล็อกได้ไม่เกินยอดใช้ได้ · ล็อกก่อนได้ก่อน
  (ล็อกที่ระบุ lot ทั้ง 2 แบบเรียงตามเวลารวมกัน แล้วล็อก "ทุก lot" ค่อยแบ่งจากที่เหลือ)

ด้ายที่ล็อกให้ Item:
  - หักออกจากยอดใช้ได้ (yarn_ava_live) → กองกลางของแผนไม่มีด้ายนี้แล้ว Item อื่นใช้ไม่ได้ทุกทาง
  - ทุกใบ (SC) ของ Item นั้นใช้ร่วมกัน : ล็อก SC ของใบเองก่อน → ล็อก Item → กองกลาง (YarnStockCover / yarn_plan)
    หลายใบแย่งกันใช้ลำดับเดียวกับแผนแบ่งด้าย (งานสีก่อน แล้วกำหนดส่ง)
  - ลดเองเมื่อออกใบ KP ของ Item นั้นที่ใช้ด้ายนี้หลังเวลาล็อก — ไม่งั้นหักซ้ำกับยอดจองของใบ KP
    ส่วนที่ใบ KP ตัดล็อก SC ไปแล้วไม่นับซ้ำ (yarn_lock.kp_taken) ; ไม่ดู lot ของใบ KP — ใบ KP ใช้ lot อื่น = Item ได้ด้ายแล้ว
  - ไม่ปลดเองตาม booking (Item อาจยังไม่มีใบค้าง เช่น core) — อยู่จนกดปลด หรือออก KP ครบยอด ;
    หน้าเว็บเตือนเมื่อไม่มีใบค้างของ Item นี้ (ด้ายถูกกันไว้เฉย ๆ)
สถานะคิดใหม่ทุกครั้งที่อ่าน ไม่เขียนกลับไฟล์ (เหตุผลเดียวกับล็อก SC — booking ว่างกลางรอบ refresh ต้องไม่ทำให้ล็อกหาย)

ไฟล์ (data_plan = volume บน server ไม่หายตอน deploy):
  yarn_item_locks.json      ล็อกที่ยังอยู่ — ปลด = ลบออก
  yarn_item_lock_log.jsonl  ประวัติ ล็อก/ปลด ใคร เมื่อไหร่ ต่อท้ายอย่างเดียว ไม่เคยลบ
"""
import json
import uuid
from datetime import datetime

import config
import yarn_lock

LOCK_FILE = config.OUTPUT_DIR / "yarn_item_locks.json"
LOG_FILE = config.OUTPUT_DIR / "yarn_item_lock_log.jsonl"
RESOURCE = "yarn_item_lock"                  # ชื่อชุดข้อมูลของ livesync (หน้า Monitor Yarn / แผนด้าย โหลดใหม่เอง)

LockError = yarn_lock.LockError              # server จับตัวเดียวกับล็อก SC → 400
_EPS = yarn_lock._EPS
_LOG_READ_MAX = 300
_txt, _num, _ts, _now, _mt = yarn_lock._txt, yarn_lock._num, yarn_lock._ts, yarn_lock._now, yarn_lock._mt


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
        print(f"[YARN ITEM LOCK] อ่าน {LOCK_FILE.name} ไม่ได้: {e}")
        return []
    return [d for d in (data.get("locks") or []) if isinstance(d, dict) and d.get("id")]


def _write_locks(locks: list) -> None:
    import livesync
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with livesync.atomic_save(LOCK_FILE) as tmp:
        tmp.write_text(json.dumps({"locks": locks}, ensure_ascii=False, indent=1), encoding="utf-8")


def _log(action: str, by: str, lock: dict, **extra) -> None:
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"at": _now(), "by": by or "", "action": action, "lock": lock, **extra},
                               ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[YARN ITEM LOCK] เขียน {LOG_FILE.name} ไม่ได้: {e}")


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
# สถานะล็อก (คิดใหม่ทุกครั้ง ไม่เขียนกลับ)
# =============================================================================
def _key(L: dict) -> tuple:
    return _txt(L.get("fabric")).upper(), _txt(L.get("yarn")).upper()


def statuses(locks: list = None) -> dict:
    """id → {"rem": kg ด้ายที่ยังล็อกอยู่, "kp": kg ที่ใบ KP ใช้ไปแล้ว, "kps": [ใบ KP], "state", "why"}
    state: active = ยังกันด้ายอยู่ · done = ออกใบ KP ครบยอดแล้ว
    ใบ KP ของผ้า + ด้ายเดียวกัน ที่ออกหลังเวลาล็อก ตัดล็อกเก่าก่อน ; ส่วนที่ใบนั้นตัดล็อก SC ไปแล้วไม่นับซ้ำ"""
    locks = read_locks() if locks is None else locks
    if not locks:
        return {}
    groups: dict = {}
    for L in locks:
        groups.setdefault(_key(L), []).append(L)
    per = yarn_lock.kp_yarn()
    taken = yarn_lock.kp_taken()
    ev: dict = {}
    for (kp, fab, yarn), a in per.items():
        if (fab, yarn) not in groups or a["t"] is None:
            continue                          # ไม่รู้วันออกใบ → เทียบกับเวลาล็อกไม่ได้ ไม่ตัดยอด
        left = a["kg"] - taken.get((kp, fab, yarn), 0.0)
        if left > 1e-9:
            ev.setdefault((fab, yarn), []).append((a["t"], kp, left))
    out = {}
    for gk, ls in groups.items():
        ls.sort(key=lambda L: (_ts(L.get("at")) or datetime.min, L["id"]))
        rem = {L["id"]: float(L.get("yarn_kg") or 0) for L in ls}
        used = {L["id"]: 0.0 for L in ls}
        kps = {L["id"]: [] for L in ls}
        for t, kp, kg in sorted(ev.get(gk, [])):
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
        for L in ls:
            i = L["id"]
            if rem[i] <= _EPS:
                st, why = "done", "ออกใบ KP ครบยอดที่ล็อกแล้ว — ด้ายไปนับเป็นยอดจองของใบ KP"
            else:
                st, why = "active", ""
            out[i] = {"rem": round(max(rem[i], 0.0), 3), "kp": round(used[i], 3), "kps": kps[i],
                      "state": st, "why": why}
    return out


def state_key() -> tuple:
    """ใส่ใน cache key ของ yarn_ava_live — ล็อก Item / ล็อก SC (ตัดใบ KP ก่อน) / booking / ใบ KP เปลี่ยน = คิดใหม่"""
    return (_mt(LOCK_FILE),) + tuple(yarn_lock.state_key())


def active() -> list:
    """ล็อกที่ยังกันด้ายอยู่ → [{"id", "yarn" (พิมพ์ใหญ่), "color", "lot" ('' = ทุก lot), "fabric", "rem", "at"}]
    เรียงตามเวลาล็อก (ล็อกก่อนได้ก่อน)"""
    locks = read_locks()
    if not locks:
        return []
    st = statuses(locks)
    out = []
    for L in locks:
        s = st.get(L["id"])
        if not s or s["state"] != "active":
            continue
        out.append({"id": L["id"], "yarn": _txt(L.get("yarn")).upper(), "color": _txt(L.get("color")).upper(),
                    "lot": _txt(L.get("lot")), "fabric": _txt(L.get("fabric")).upper(), "rem": s["rem"],
                    "at": L.get("at") or ""})
    out.sort(key=lambda d: (d["at"], d["id"]))
    return out


# =============================================================================
# ตัวเลือกตอนล็อก : lot / สี ของด้าย + Item ผ้าที่ใช้ด้ายตัวนี้
# =============================================================================
def _open_by_fabric() -> dict:
    """ใบที่ยังค้างทอ ต่อผ้า → {ผ้า: {SC}} (booking ยังค้าง หรือ SC ค้างทอ — ตัวเดียวกับตัวเลือกของล็อก SC)"""
    out: dict = {}
    for sc, fabs in yarn_lock._open_fabrics().items():
        for f in fabs:
            out.setdefault(f, set()).add(sc)
    return out


def fabrics_for(yarn: str, color: str = "") -> dict:
    """Item ผ้าที่ใช้ด้าย (สี) นี้ → {ผ้า: {"pct", "basis", "scs": [SC ที่ค้าง], "pending": kg ผ้าค้าง,
                                           "need": kg ด้ายที่ใบค้างยังต้องใช้ (ประมาณ)}}
    แหล่ง: % ด้ายใน BOM ของใบ KP (ผ้าที่เคยทอด้วยด้ายนี้ — ไม่แยกสี) + บรรทัดใบ SC ที่ยังค้างและใช้ด้าย/สีนี้
    need = ยอดค้างของ booking (ไม่มี = SC ค้างทอ) × % × สูญเสีย ของแต่ละใบ — ใช้บอกให้คนล็อกพอดียอด ไม่ได้บังคับ"""
    y, c = _txt(yarn).upper(), _txt(color).upper()
    out: dict = {}
    for (f, yy), pct in yarn_lock._bom().items():
        if yy == y:
            out[f] = {"pct": round(pct, 4), "basis": "BOM ใบ KP", "scs": [], "pending": 0.0, "need": 0.0}
    by_sc = yarn_lock._scfol().get((y, c), {})
    fabs_of = yarn_lock._open_fabrics()
    bk = yarn_lock._booking()["rows"]
    sci = yarn_lock._sc_items()
    for sc, sc_lines in by_sc.items():
        fabs = fabs_of.get(sc)
        if not fabs:
            continue
        for fab in fabs:
            f = yarn_lock._factor(sc, fab, y, sc_lines, len(fabs))
            if f is None:
                continue
            b = bk.get((sc, fab))
            pend = b["pending"] if b and b["pending"] > 0 else (sci.get((sc, fab)) or 0.0)
            o = out.setdefault(fab, {"pct": f["pct"], "basis": "บรรทัดใบ SC", "scs": [], "pending": 0.0, "need": 0.0})
            o["scs"].append(sc)
            o["pending"] += pend
            o["need"] += pend * f["factor"]
    for o in out.values():
        o["scs"] = sorted(set(o["scs"]))
        o["pending"] = round(o["pending"], 1)
        o["need"] = round(o["need"], 1)
    return out


def _lot_rows(table: dict, yarn: str) -> list:
    """แถว lot ของด้ายนี้ในตาราง yarn_ava_live (มีในสต๊อก)"""
    y = _txt(yarn).upper()
    names = [str(n["name"]).strip().upper() for n in table.get("yarns") or []]
    return [r for r in table.get("rows") or []
            if r.get("l") is not None and names[r["g"]] == y and r.get("w") is not None]


def _nolot(table: dict, yarn: str) -> float:
    """ด้ายที่ใบ KP จองแต่ยังไม่ระบุ lot (หักจากยอดรวมของด้าย — ด้าย TD ไม่รู้สี หักทุกสี เข้มไว้ก่อน)"""
    y = _txt(yarn).upper()
    names = [str(n["name"]).strip().upper() for n in table.get("yarns") or []]
    return sum(max(-(r.get("ava") or 0.0), 0.0) for r in table.get("rows") or []
               if r.get("l") is None and names[r["g"]] == y)


def options(yarn: str, lot: str, color: str, table: dict, hide=()) -> dict:
    """ตัวเลือกตอนล็อก → {"yarn" (ชื่อตามสต๊อก), "lot", "color", "colors", "lots": [...], "room", "fabrics": [...], "note"}
    lots  = lot ที่ยังใช้ได้ (ไม่ใช่ลูกเล็กทั้ง lot, ใช้ได้ > 0.5) ด้ายเข้าก่อนอยู่บน
    room  = ล็อกได้อีกเท่าไหร่ : ระบุ lot = ใช้ได้ของ lot นั้น ; ไม่ระบุ = รวม lot สีเดียวกันที่ไม่ได้สงวนให้ลูกค้า
            − ใบ KP ที่จองแต่ยังไม่ระบุ lot (ตรงกับที่ yarn_ava_live แบ่งลง lot จริง)
    color = สีของ lot ที่เลือก / สีที่ส่งมา / สีเดียวของด้าย (ด้ายดิบ = '')"""
    ysc = yarn_lock._ysc()
    y, lot = _txt(yarn).upper(), _txt(lot)
    lcolor, age, meta_note = ysc.lot_meta(hide)
    color_of = (lambda l: lcolor.get((y, l), "")) if lcolor else (lambda l: "")
    names = {str(n["name"]).strip().upper(): n["name"] for n in table.get("yarns") or []}
    lots = []
    for r in _lot_rows(table, y):
        if r.get("low") or (r.get("ava") or 0) <= 0.5:
            continue
        lots.append({"lot": r["l"], "ava": r["ava"], "age": age.get((y, r["l"])), "color": color_of(r["l"]),
                     "own": r.get("own") or []})
    lots.sort(key=lambda d: (-(d["age"] or 0), d["lot"]))
    colors = sorted({d["color"] for d in lots})
    notes = [] if lcolor else [meta_note or "ไฟล์สต๊อกไม่มีสีของ lot — ด้าย TD ล็อกแบบไม่ระบุ lot ไม่ได้"]
    if lot:
        hit = next((d for d in lots if d["lot"] == lot), None)
        c = hit["color"] if hit else color_of(lot)
        room = hit["ava"] if hit else 0.0
    else:
        c = _txt(color).upper() if color else (colors[0] if len(colors) == 1 else "")
        room = sum(d["ava"] for d in lots if d["color"] == c and not d["own"]) - _nolot(table, y)
    fabs = fabrics_for(y, c)
    open_by = _open_by_fabric()
    fl = [{"fabric": f, **v, "open": len(open_by.get(f, ()))} for f, v in fabs.items()]
    fl.sort(key=lambda d: (-d["need"], -d["open"], d["fabric"]))
    if _mt(yarn_lock._SCFOL) is None:
        notes.append(f"ยังไม่มีไฟล์ {yarn_lock._SCFOL.name} — รู้เฉพาะผ้าที่เคยใช้ด้ายนี้ตาม BOM ใบ KP")
    return {"yarn": names.get(y, _txt(yarn)), "lot": lot, "color": c, "colors": colors, "lots": lots,
            "room": round(max(room, 0.0), 3), "fabrics": fl, "note": " · ".join(n for n in notes if n)}


def catalog(table: dict) -> list:
    """ด้ายที่ยังมียอดใช้ได้ (ให้หน้าเว็บเลือก) → [{"name", "ava"}] เรียงตามชื่อ"""
    return [{"name": y["name"], "ava": y["ava"]} for y in sorted(table.get("yarns") or [], key=lambda y: str(y["name"]))
            if (y.get("ava") or 0) > 0.5]


# =============================================================================
# ล็อก / ปลด
# =============================================================================
def add(user: str, yarn: str, lot: str, color: str, fabric: str, yarn_kg, note: str, ava_table, hide=()) -> dict:
    """ล็อกด้ายให้ Item ผ้า ; ava_table() = ตารางยอดใช้ได้ปัจจุบัน (คิดในล็อกไฟล์ — กันสองคนล็อกเกินพร้อมกัน)
    → {"lock", "warn"} ; ล็อกไม่ได้ = LockError"""
    import livesync

    yarn, lot, fab = _txt(yarn), _txt(lot), _txt(fabric).upper()
    kg = _num(yarn_kg)
    if not yarn:
        raise LockError("ต้องเลือกด้าย")
    if not fab:
        raise LockError("ต้องเลือก Item ผ้า")
    if kg is None or kg <= 0:
        raise LockError("จำนวน kg ด้ายต้องมากกว่า 0")

    with livesync.file_lock(LOCK_FILE):
        locks = read_locks(strict=True)
        t = ava_table()
        opt = options(yarn, lot, color, t, hide)
        if not _lot_rows(t, yarn):
            raise LockError(f"ไม่มีด้าย {yarn} ในสต๊อก")
        if lot:
            row = next((r for r in _lot_rows(t, yarn) if str(r["l"]) == lot), None)
            if row is None:
                raise LockError(f"ไม่พบ lot {lot} ของด้าย {yarn} ในสต๊อก")
            if row.get("low"):
                raise LockError(f"lot {lot} เป็นลูกเล็กทั้ง lot — ใช้ไม่ได้ ล็อกไม่ได้")
            where = f"lot {lot}"
        else:
            if len(opt["colors"]) > 1 and not _txt(color):
                raise LockError(f"ด้าย {yarn} มีหลายสี ({', '.join(opt['colors'])}) — ต้องเลือกสี หรือเลือก lot")
            if _txt(color) and _txt(color).upper() not in opt["colors"]:
                raise LockError(f"ไม่มี lot สี {color} ของด้าย {yarn} ที่ยังใช้ได้")
            where = "ทุก lot" + (f" สี {opt['color']}" if opt["color"] else "")
        if kg > opt["room"] + _EPS:
            raise LockError(f"ด้ายไม่พอ: ขอล็อก {kg:,.1f} kg แต่ {yarn} {where} ใช้ได้อีก {opt['room']:,.1f} kg "
                            f"(หลังหักลูกเล็ก ใบ KP ที่จอง และล็อกอื่นแล้ว)")
        if fab not in {f["fabric"] for f in opt["fabrics"]}:
            raise LockError(f"Item {fab} ไม่ได้ใช้ด้าย {yarn}" + (f" สี {opt['color']}" if opt["color"] else "")
                            + " (ไม่พบใน BOM ใบ KP และบรรทัดใบ SC ที่ค้าง)")
        L = {"id": uuid.uuid4().hex[:12], "yarn": opt["yarn"], "color": opt["color"], "lot": lot, "fabric": fab,
             "yarn_kg": round(kg, 3), "note": _txt(note)[:300], "at": _now(), "by": user or ""}
        locks.append(L)
        _write_locks(locks)
    _log("lock", user, L)
    livesync.bump(RESOURCE, user, f"ล็อกด้าย {L['yarn']} {where} {kg:,.1f} kg ให้ Item {fab}")
    warn = []
    if lot:
        own = next((d["own"] for d in opt["lots"] if d["lot"] == lot), [])
        if own:
            warn.append(f"lot นี้สงวนให้ลูกค้า {', '.join(own)} ตามกติกาหน้า Monitor Yarn — ตรวจว่า Item นี้เป็นของลูกค้านั้น")
    f = next(d for d in opt["fabrics"] if d["fabric"] == fab)
    if not f["open"]:
        warn.append(f"ตอนนี้ไม่มีใบค้างของ Item {fab} — ด้ายถูกกันไว้จนกว่าจะมีใบมาใช้ หรือกดปลด")
    elif f["need"] > 0 and kg > f["need"] + _EPS:
        warn.append(f"ล็อกมากกว่าที่ใบค้างของ Item นี้ต้องใช้ (ประมาณ {f['need']:,.1f} kg) — ส่วนเกินถูกกันไว้เฉย ๆ")
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
    livesync.bump(RESOURCE, user, f"ปลดล็อกด้าย {L['yarn']} {L.get('lot') or 'ทุก lot'} ของ Item {L['fabric']}")
    return {"ok": True}


# =============================================================================
# หน้า Monitor : ล็อกทั้ง 2 แบบ + ยอดที่กันได้จริง + แผนใช้ไปเท่าไหร่ + ประวัติ
# =============================================================================
def _spread(locks: list, use: dict, key_of) -> None:
    """แบ่งยอดที่แผนใช้ของกลุ่ม (ผ้า/ใบ + ด้าย + สี) ให้ล็อกในกลุ่ม ล็อกเก่าก่อน (แผนกินล็อกเก่าก่อนเหมือนกัน)
    เติม L["use"] = [kg ต่อสัปดาห์] , L["use_total"] — ใส่ในที่"""
    groups: dict = {}
    for L in locks:
        groups.setdefault(key_of(L), []).append(L)
    for k, ls in groups.items():
        weeks = list(use.get(k) or [])
        ls.sort(key=lambda L: (L.get("at") or "", L["id"]))
        for L in ls:
            cap = float(L.get("eff") or 0.0) if L.get("state") == "active" else 0.0
            mine = []
            for i, q in enumerate(weeks):
                take = min(cap, q)
                mine.append(round(take, 1))
                weeks[i] -= take
                cap -= take
            L["use"] = mine
            L["use_total"] = round(sum(mine), 1)


def monitor(table: dict, sc_listing: dict, plan_use: dict) -> dict:
    """ข้อมูลหน้า Monitor Yarn › 🔒 ล็อกด้าย
    table = yarn_ava_live.table (ยอดที่ lot กันให้จริง) · sc_listing = yarn_lock.listing(table)
    plan_use = yarn_plan.lock_usage() {"weeks", "item": {"ผ้า|ด้าย|สี": [kg]}, "sc": {"SC|ผ้า|ด้าย|สี": [kg]}, ...}"""
    locks = read_locks()
    st = statuses(locks)
    eff: dict = {}
    lots: dict = {}
    names = [str(n["name"]).strip().upper() for n in table.get("yarns") or []]
    for r in table.get("rows") or []:
        for lk in r.get("locks") or []:
            if lk.get("kind") == "item":
                eff[lk["id"]] = eff.get(lk["id"], 0.0) + float(lk.get("kg") or 0)
                if (lk.get("kg") or 0) > 0:
                    lots.setdefault(lk["id"], []).append([r["l"], round(lk["kg"], 1)])
    open_by = _open_by_fabric()
    items = []
    for L in sorted(locks, key=lambda d: d.get("at") or "", reverse=True):
        s = st.get(L["id"], {"rem": L.get("yarn_kg"), "kp": 0.0, "kps": [], "state": "active", "why": ""})
        e = round(eff.get(L["id"], 0.0), 3)
        n_open = len(open_by.get(_txt(L.get("fabric")).upper(), ()))
        if s["state"] == "active":
            y = _txt(L.get("yarn")).upper()
            if y not in names:
                s = {**s, "why": f"ไม่มีด้าย {L.get('yarn')} ในสต๊อกแล้ว — ยังไม่ได้กันด้าย"}
            elif e + _EPS < s["rem"]:
                s = {**s, "why": f"สต๊อกมีให้กันได้แค่ {e:,.1f} kg จาก {s['rem']:,.1f} kg"
                                 + (f" (lot {L['lot']})" if L.get("lot") else "")}
            elif not n_open:
                s = {**s, "why": "ไม่มีใบค้างของ Item นี้ — ด้ายถูกกันไว้เฉย ๆ จนกว่าจะมีใบมาใช้"}
        items.append({**L, "kind": "item", **s, "eff": e if s["state"] == "active" else 0.0,
                      "lots": lots.get(L["id"], []), "open": n_open})
    scs = [{**L, "kind": "sc"} for L in sc_listing.get("locks") or []]
    item_use = plan_use.get("item") or {}
    sc_use = plan_use.get("sc") or {}
    _spread(items, item_use, lambda L: f"{_txt(L.get('fabric')).upper()}|{_txt(L.get('yarn')).upper()}|"
                                       f"{_txt(L.get('color')).upper()}")
    _spread(scs, sc_use, lambda L: f"{L.get('so')}|{_txt(L.get('fabric')).upper()}|{_txt(L.get('yarn')).upper()}|"
                                   f"{_txt(L.get('color')).upper()}")
    log = [{**g, "kind": "item"} for g in read_log(100)] + [{**g, "kind": "sc"} for g in sc_listing.get("log") or []]
    log.sort(key=lambda g: g.get("at") or "", reverse=True)
    act = lambda ls: [L for L in ls if L["state"] == "active"]
    r1 = lambda v: round(v, 1)
    return {"item": items, "sc": scs, "log": log[:150], "yarns": catalog(table),
            "weeks": plan_use.get("weeks") or [], "plan": plan_use.get("plan"), "stock_at": table.get("stock_at"),
            "stats": {"item_n": len(act(items)), "item_kg": r1(sum(L["eff"] for L in act(items))),
                      "sc_n": len(act(scs)), "sc_kg": r1(sum(float(L.get("eff") or 0) for L in act(scs)))},
            "note": " · ".join(n for n in (table.get("note"), plan_use.get("note")) if n)}

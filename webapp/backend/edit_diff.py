"""
edit_diff.py — เทียบชีท "ก่อนแก้" กับ "หลังแก้" ออกมาเป็นประโยคที่คนอ่านรู้เรื่อง

ใช้กับประวัติการแก้ไข (edit_log.jsonl) — เดิมจดแค่ "MasterMC/Master MC 62 แถว" ซึ่งไม่บอก
ว่าใครแก้อะไร ; ตอนนี้จดเป็นรายการ เช่น
    แก้ [F1 · SINGLE · 28] Total MC: 12 → 14
    เพิ่มแถว [F2 · RIB · 18]
    ลบคอลัมน์ Remark

หลักการ
  • เทียบด้วย "ค่าที่แสดง" (ตัดช่องว่าง, 12.0 = 12, วันที่ = YYYY-MM-DD) → เซฟซ้ำค่าเดิมไม่นับเป็นการแก้
  • แถวที่แค่สลับลำดับไม่นับ (ตัดแถวที่เหมือนกันทุกช่องออกก่อน)
  • ชีทที่มีคอลัมน์ตัวระบุแถว (KEY_HINTS เช่น Item code + Team Name): แถวที่ตัวระบุตรงกัน
    = แถวเดียวกัน → "แก้ช่องไหน" ; ตัวระบุที่ไม่เจออีกฝั่ง = เพิ่มแถว / ลบแถว
    (นำเข้าไฟล์ทั้งชีท = item ไหนถูกแก้ / เพิ่ม / ลบ — ห้ามจับคู่ด้วยความคล้าย เพราะแถวในทะเบียน
     หน้าตาคล้ายกันหมด พอ TARGET เปลี่ยนจะไปจับกับ item อื่นแล้วออกมาเป็น "แก้ Item code A → B")
    ยกเว้นทั้งรอบมีแถวหาย 1 + แถวใหม่ 1 ที่คล้ายกัน = พิมพ์ทับตัวระบุของแถวเดียว → ยังเป็น "แก้"
  • ชีทที่ไม่มีคอลัมน์ตัวระบุ: จับคู่กับแถวเดิมที่ค่าตรงกันมากที่สุด คล้ายไม่ถึงครึ่ง = ลบ + เพิ่ม
    ; ชีทใหญ่ที่เปลี่ยนเยอะไม่จับคู่ทีละแถว (ช้าเกิน) → รายงานเพิ่ม/ลบ
"""
from collections import Counter, defaultdict
from datetime import date, datetime

MAX_LINES = 300          # จำนวนบรรทัดสูงสุดที่เก็บต่อการแก้ 1 ครั้ง (ที่เกินนับรวมไว้ใน total)
MAX_PAIR_WORK = 200_000  # แถวเหลือ (เก่า × ใหม่) เกินนี้ ไม่จับคู่ทีละแถว
EMPTY = "(ว่าง)"


def canon(v) -> str:
    """ค่าเดียวกันต้องได้ข้อความเดียวกัน ไม่ว่าจะมาจาก Excel หรือจากหน้าเว็บ"""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, datetime):
        if (v.hour, v.minute, v.second) == (0, 0, 0):
            return v.date().isoformat()
        return v.isoformat(sep=" ", timespec="seconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, float):
        if v != v:          # NaN
            return ""
        if v.is_integer():
            return str(int(v))
        return f"{round(v, 6):g}" if abs(v) < 1e6 else str(round(v, 6))
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    # "12.0" จากหน้าเว็บ = 12 ในไฟล์
    try:
        f = float(s)
        if s.lower() not in ("nan", "inf", "-inf", "infinity") and f.is_integer() and "." in s:
            return str(int(f))
    except ValueError:
        pass
    return s


def _show(s: str) -> str:
    return s if s != "" else EMPTY


# คอลัมน์ที่ใช้ระบุ "แถวไหน" ในประวัติ (เทียบแบบตัวใหญ่) — ชีทที่ไม่มีคอลัมน์พวกนี้ใช้ 3 ช่องแรกแทน
KEY_HINTS = {
    "DATE", "FACTORY", "MC_CAT", "MC", "GUAGE", "GAUGE",
    "ITEM", "ITEM CODE", "ITEM_CODE", "ITEM_LIST", "TEAM", "TEAM NAME", "TEAM_NAME",
}


def _label(row: list, key_idx: list) -> str:
    """ป้ายของแถว เช่น [PHET · DOUBLE · DOUBLE-30 · 30] หรือ [2026-02-12]"""
    vals = [row[i] for i in key_idx if row[i] != ""]
    if not vals:
        vals = [x for x in row if x != ""][:3]
    return "[" + " · ".join(vals) + "]" if vals else "[แถวว่าง]"


def sheet_changes(old_cols: list, old_rows: list, new_cols: list, new_rows: list) -> dict:
    """→ {"lines": [...ไม่เกิน MAX_LINES], "total": จำนวนการเปลี่ยนทั้งหมด}"""
    oc = [canon(c) for c in (old_cols or [])]
    nc = [canon(c) for c in (new_cols or [])]
    lines: list = []

    for c in nc:
        if c and c not in oc:
            lines.append(f"เพิ่มคอลัมน์ {c}")
    for c in oc:
        if c and c not in nc:
            lines.append(f"ลบคอลัมน์ {c}")

    # เทียบเฉพาะคอลัมน์ที่มีทั้งสองฝั่ง (ลำดับตามของใหม่) — คอลัมน์ที่เพิ่ม/ลบรายงานไปแล้วข้างบน
    common = [c for c in dict.fromkeys(nc) if c and c in oc]
    oi = {c: oc.index(c) for c in common}
    ni = {c: nc.index(c) for c in common}

    def pick(row, idx):
        row = list(row or [])
        return tuple(canon(row[idx[c]]) if idx[c] < len(row) else "" for c in common)

    old_t = [t for t in (pick(r, oi) for r in (old_rows or [])) if any(t)]
    new_t = [t for t in (pick(r, ni) for r in (new_rows or [])) if any(t)]

    # ตัดแถวที่เหมือนกันทุกช่องออก (แถวที่แค่สลับที่ = ไม่ได้แก้)
    pool = Counter(old_t)
    new_left = []
    for t in new_t:
        if pool[t] > 0:
            pool[t] -= 1
        else:
            new_left.append(t)
    pool_new = Counter(new_t)
    old_left = []
    for t in old_t:
        if pool_new[t] > 0:
            pool_new[t] -= 1
        else:
            old_left.append(t)

    def score(o, t) -> float:
        filled = sum(1 for a, b in zip(o, t) if a != "" or b != "")
        same = sum(1 for a, b in zip(o, t) if a == b and a != "")
        return same / filled if filled else 0.0

    key_idx = [i for i, c in enumerate(common) if c.upper() in KEY_HINTS][:4]

    edits, added, removed = [], [], []
    if key_idx:
        # จับคู่ด้วยตัวระบุแถวก่อน — ตัวระบุซ้ำหลายแถว (เช่นแถวช่วงสัปดาห์ของเครื่องเดียวกัน)
        # จับคู่ที่ค่าตรงกันมากที่สุดในกลุ่มก่อน (ไม่ขึ้นกับลำดับแถว) ; งานเกินเพดานจับตามลำดับ (กันช้า)
        def kof(t):
            return tuple(t[i] for i in key_idx)

        by_old, by_new = defaultdict(list), defaultdict(list)
        for j, o in enumerate(old_left):
            if any(kof(o)):
                by_old[kof(o)].append(j)
        for i, t in enumerate(new_left):
            if kof(t) in by_old:
                by_new[kof(t)].append(i)
        pair, work = {}, 0                       # index แถวใหม่ → index แถวเก่า
        for k, ni in by_new.items():
            oj = by_old[k]
            work += len(ni) * len(oj)
            if (len(ni) == 1 and len(oj) == 1) or work > MAX_PAIR_WORK:
                pair.update(zip(ni, oj))
                continue
            taken = set()
            for _s, i, j in sorted((-score(old_left[j], new_left[i]), i, j) for i in ni for j in oj):
                if i not in pair and j not in taken:
                    pair[i] = j
                    taken.add(j)
        used = set(pair.values())
        edits = [(old_left[pair[i]], t) for i, t in enumerate(new_left) if i in pair]
        rest_new = [t for i, t in enumerate(new_left) if i not in pair]
        rest_old = [o for j, o in enumerate(old_left) if j not in used]
        # ตัวระบุเปลี่ยน: หาย 1 + ใหม่ 1 ที่คล้ายกัน = พิมพ์ทับตัวระบุของแถวเดียว ; มากกว่านั้น = เพิ่ม/ลบ
        if len(rest_old) == 1 and len(rest_new) == 1 and score(rest_old[0], rest_new[0]) >= 0.5:
            edits.append((rest_old[0], rest_new[0]))
        else:
            added, removed = rest_new, rest_old
    elif old_left and new_left and len(old_left) * len(new_left) <= MAX_PAIR_WORK:
        free = list(range(len(old_left)))
        for t in new_left:
            best, best_score = None, 0.0
            for j in free:
                s = score(old_left[j], t)
                if s > best_score:
                    best, best_score = j, s
            if best is not None and best_score >= 0.5:
                free.remove(best)
                edits.append((old_left[best], t))
            else:
                added.append(t)
        removed = [old_left[j] for j in free]
    else:
        added, removed = new_left, old_left

    for o, t in edits:
        diffs = [f"{c}: {_show(a)} → {_show(b)}" for c, a, b in zip(common, o, t) if a != b]
        # ป้ายใช้ค่าเดิม — ถ้าแก้ตัวระบุแถวเอง (เช่นรหัสผ้า) จะยังตามเจอว่าแถวไหน
        lines.append(f"แก้ {_label(list(o), key_idx)} " + ", ".join(diffs))
    for t in added:
        lines.append(f"เพิ่มแถว {_label(list(t), key_idx)}")
    for t in removed:
        lines.append(f"ลบแถว {_label(list(t), key_idx)}")

    return {"lines": lines[:MAX_LINES], "total": len(lines)}

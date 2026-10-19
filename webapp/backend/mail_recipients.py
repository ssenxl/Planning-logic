"""
mail_recipients.py — รายชื่อผู้รับเมลแยกตามทีมขาย (ชีท `Sales_Mail` ใน MasterMC.xlsx)

**ทำไมเก็บในชีทของ MasterMC ไม่ใช่ไฟล์ .env หรือค่าคงที่ในโค้ด**: รายชื่อฝ่ายขายเปลี่ยนบ่อย
(เข้า/ออก/ย้ายทีม) คนที่รู้ว่าใครอยู่ทีมไหนคือผู้วางแผน ไม่ใช่คนดูแลระบบ → ต้องแก้ได้เองบน
server ทันทีโดยไม่ต้อง deploy ใหม่ ซึ่ง MasterMC เป็นไฟล์ที่ webapp แก้ได้อยู่แล้ว
(mount เป็น volume `/data/knitplan/masters` และมีล็อกไฟล์/สำรอง/เลขรุ่นของ livesync ครบ)

คอลัมน์: TEAM | NAME | EMAIL | ROLE | ACTIVE | NOTE
  TEAM    ชื่อทีมที่ต้อง **ตรงกับ TEAM_NAME ในข้อมูล booking** (ระบบไม่เดาชื่อให้ ดูหัวข้อถัดไป)
          ค่า `*` = ได้รับทุกฉบับ ไม่ว่าเป็นทีมไหน (ใช้กับทีมกลาง เช่น CENTER)
  ROLE    TO = ผู้รับหลัก ; CC = สำเนา (ว่าง = TO)
  ACTIVE  N = พักไว้ ไม่ต้องลบแถวทิ้ง (คนลาออก/ย้ายทีมชั่วคราว)

**จับคู่ทีมแบบตรงตัวเท่านั้น** (ตัดช่องว่างหัวท้ายกับตัวพิมพ์เล็ก-ใหญ่เท่านั้น ไม่มีตาราง alias)
ตามที่ผู้ใช้สั่ง — ระบบเดาชื่อทีมให้เมื่อไร วันที่ชื่อทีมในไฟล์ HR เปลี่ยน เมลจะเงียบ ๆ วิ่งไป
ผิดทีมโดยไม่มีใครรู้ แทนที่จะเดา โมดูลนี้จึง**รายงานชื่อที่ไม่ตรงออกมาเป็น warning**
ให้คนแก้เอง (เช่น ไฟล์ HR เขียน `THVN` แต่ข้อมูลจริงมี `THVN-1`/`THVN-2`)

**ชื่อทีมห้ามมีลูกน้ำ**: ฝั่ง booking ยุบหลายทีมมาในช่องเดียวเป็น `"RTS,NYK4-1"` ระบบจึงใช้
ลูกน้ำเป็นตัวแยกทีมเสมอ — ทีมที่ตั้งชื่อว่า `NYK2,3` จะถูกอ่านเป็นทีม `NYK2` กับทีม `3`
(ไฟล์รายชื่อตั้งต้นมีชื่อแบบนี้อยู่จริง 1 ทีม) → ตรวจแล้วเตือน ไม่ใช่แก้ให้เงียบ ๆ
"""
import csv
import re
from pathlib import Path

import livesync
from config import master_files

MASTER = "MasterMC"
SHEET = "Sales_Mail"
COLUMNS = ["TEAM", "NAME", "EMAIL", "ROLE", "ACTIVE", "NOTE"]

# ทีม `*` = แถวที่ได้รับเมลทุกฉบับ (ทีมกลาง)
ALL_TEAMS = "*"

# ไฟล์ตั้งต้น — ใช้ครั้งเดียวตอนที่ยังไม่มีชีทในไฟล์ Master (ที่มา: Sales Email Update 22.09.26)
# อยู่ในโฟลเดอร์ backend ที่ Dockerfile `COPY webapp/backend` ไปอยู่แล้ว จึงติดไปกับ image เสมอ
SEED = Path(__file__).with_name("sales_mail_seed.csv")

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_TEAM_SPLIT = re.compile(r"[,;]+")


def norm_team(v) -> str:
    """ชื่อทีมสำหรับใช้เทียบ — ตัดช่องว่างหัวท้าย/ยุบช่องว่างซ้ำ/ตัวพิมพ์ใหญ่

    แค่นี้ ไม่แปลงอย่างอื่น: `NYG/TRM` กับ `NYG/TRM/GW` ต้องไม่เท่ากัน
    """
    return re.sub(r"\s+", " ", str(v or "").strip()).upper()


def split_teams(v) -> list:
    """ช่องทีมของข้อมูล → รายชื่อทีม ('RTS,NYK4-1' → ['RTS','NYK4-1'])"""
    return [t for t in (norm_team(x) for x in _TEAM_SPLIT.split(str(v or ""))) if t]


# ── ชีทในไฟล์ Master ─────────────────────────────────────────────────────────

def _path() -> Path:
    reg = master_files()
    if MASTER not in reg:
        raise FileNotFoundError("config.ini ไม่ได้ตั้ง path ของ MasterMC")
    p = reg[MASTER]
    if not p.exists():
        raise FileNotFoundError(f"ไม่พบไฟล์ {p}")
    return p


def _seed_rows() -> list:
    """แถวตั้งต้นจากไฟล์ CSV (ไม่มีไฟล์ = ชีทว่าง ให้ผู้ใช้กรอกเอง ไม่ใช่พัง)"""
    if not SEED.exists():
        return []
    out = []
    with open(SEED, encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            out.append([str(r.get(c) or "").strip() for c in COLUMNS])
    return out


def ensure_sheet(user: str = "") -> bool:
    """ยังไม่มีชีท `Sales_Mail` → สร้างจากไฟล์ตั้งต้น ; คืน True เมื่อเพิ่งสร้าง

    สร้างเองแทนที่จะให้คนไปเปิด Excel เพิ่มชีท เพราะบน server ไม่มี Excel และไฟล์ Master
    บนนั้นเป็นคนละก๊อปกับเครื่องผู้วางแผน — ถ้าไม่สร้างให้ ฟีเจอร์นี้จะใช้ไม่ได้เลยบน server
    """
    from openpyxl import load_workbook
    path = _path()
    # เช็กแบบ read-only ก่อน — เปิด MasterMC แบบเต็มทุกครั้งที่โหลดรายชื่อคือส่วนที่ช้าที่สุด
    # ของหน้านี้ ทั้งที่ชีทถูกสร้างครั้งเดียวในชีวิต ; เปิดเต็ม+ล็อกเฉพาะตอนต้องสร้างจริง
    wb = load_workbook(path, read_only=True)
    try:
        if SHEET in wb.sheetnames:
            return False
    finally:
        wb.close()
    with livesync.file_lock(path):
        wb = load_workbook(path)
        if SHEET in wb.sheetnames:
            wb.close()
            return False
        livesync.backup_throttled(path)
        ws = wb.create_sheet(SHEET)
        ws.append(COLUMNS)
        for row in _seed_rows():
            ws.append(row)
        ws.freeze_panes = "A2"
        for col, w in zip("ABCDEF", (28, 18, 38, 8, 8, 46)):
            ws.column_dimensions[col].width = w
        with livesync.atomic_save(path) as tmp:
            wb.save(tmp)
        wb.close()
        livesync.bump(livesync.masters_res(MASTER, SHEET), user=user,
                      detail=f"สร้างชีท {SHEET} ตั้งต้น")
    return True


def read_rows() -> dict:
    """{"rows":[{TEAM,NAME,EMAIL,ROLE,ACTIVE,NOTE}], "rev": n} — สร้างชีทให้ถ้ายังไม่มี"""
    from openpyxl import load_workbook
    ensure_sheet()
    path = _path()
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        if SHEET not in wb.sheetnames:
            return {"rows": [], "rev": livesync.rev(livesync.masters_res(MASTER, SHEET))}
        ws = wb[SHEET]
        raw = [list(r) for r in ws.iter_rows(values_only=True)]
    finally:
        wb.close()
    if not raw:
        return {"rows": [], "rev": livesync.rev(livesync.masters_res(MASTER, SHEET))}
    # จับคอลัมน์ตามชื่อหัวตาราง ไม่ใช่ตามลำดับ — คนแก้ไฟล์ด้วย Excel สลับคอลัมน์ได้
    head = {str(c or "").strip().upper(): i for i, c in enumerate(raw[0])}
    rows = []
    for r in raw[1:]:
        def get(col):
            i = head.get(col)
            return "" if i is None or i >= len(r) or r[i] is None else str(r[i]).strip()
        rec = {c: get(c) for c in COLUMNS}
        if not (rec["TEAM"] or rec["EMAIL"]):
            continue                          # แถวว่างที่ Excel ทิ้งไว้ท้ายชีท
        rows.append(rec)
    return {"rows": rows, "rev": livesync.rev(livesync.masters_res(MASTER, SHEET))}


def save_rows(rows: list, user: str = "", base_rev=None) -> dict:
    """เขียนรายชื่อทั้งชุดกลับลงชีท (แทนที่ของเดิมทั้งหมด)

    ตรวจก่อนเขียนเสมอ: อีเมลต้องเป็นอีเมล และทีมต้องไม่มีลูกน้ำ — ข้อมูลชุดนี้ถูกใช้ยิงเมล
    ออกนอกทีม ถ้าปล่อยค่าเพี้ยนลงไฟล์ ความผิดพลาดจะไปโผล่ตอนส่งจริงซึ่งย้อนกลับไม่ได้
    """
    from openpyxl import load_workbook
    clean, seen, errors = [], set(), []
    for n, r in enumerate(rows or [], 1):
        team = re.sub(r"\s+", " ", str(r.get("TEAM") or "").strip())
        mail = re.sub(r"\s+", "", str(r.get("EMAIL") or ""))
        name = re.sub(r"\s+", " ", str(r.get("NAME") or "").strip())
        role = (str(r.get("ROLE") or "").strip().upper() or "TO")
        active = (str(r.get("ACTIVE") or "").strip().upper() or "Y")
        note = str(r.get("NOTE") or "").strip()
        if not team and not mail:
            continue
        # ด่านชื่อทีมบังคับเฉพาะแถวที่เปิดใช้งาน — แถวที่พักไว้ยังส่งเมลไม่ได้อยู่แล้ว
        # จึงเก็บชื่อที่ยังแก้ไม่เสร็จไว้ได้ (รายชื่อตั้งต้นจาก HR มีทีมชื่อ 'NYK2,3' ที่ใช้ไม่ได้
        # จริง ๆ อยู่ 1 แถว — ถ้าบังคับทุกแถว คนที่เข้ามาแก้เรื่องอื่นจะกดบันทึกไม่ผ่านเลย
        # ทั้งที่ไม่ได้แตะแถวนั้น)
        if not active.startswith("N"):
            if not team:
                errors.append(f"แถว {n}: ยังไม่ได้ระบุทีม")
            elif "," in team or ";" in team:
                errors.append(f"แถว {n}: ชื่อทีม '{team}' มีลูกน้ำ — ระบบใช้ลูกน้ำแยกทีม "
                              f"ต้องแยกเป็นคนละแถว")
        if not _EMAIL_RE.match(mail):
            errors.append(f"แถว {n}: '{mail or '(ว่าง)'}' ไม่ใช่อีเมลที่ถูกต้อง")
        if role not in ("TO", "CC"):
            errors.append(f"แถว {n}: ROLE ต้องเป็น TO หรือ CC (ได้ '{role}')")
        key = (norm_team(team), mail.lower())
        if key in seen:
            errors.append(f"แถว {n}: {mail} ซ้ำในทีม {team}")
        seen.add(key)
        clean.append([team, name, mail, role, "N" if active.startswith("N") else "Y", note])
    if errors:
        raise ValueError(" · ".join(errors[:8])
                         + (f" (อีก {len(errors) - 8} ข้อ)" if len(errors) > 8 else ""))

    ensure_sheet(user)
    path = _path()
    res = livesync.masters_res(MASTER, SHEET)
    with livesync.file_lock(path):
        livesync.check_rev(res, base_rev)
        bak = livesync.backup_throttled(path)
        wb = load_workbook(path)
        ws = wb[SHEET] if SHEET in wb.sheetnames else wb.create_sheet(SHEET)
        if ws.max_row > 0:
            ws.delete_rows(1, ws.max_row)
        ws.append(COLUMNS)
        for row in clean:
            ws.append(row)
        ws.freeze_panes = "A2"
        with livesync.atomic_save(path) as tmp:
            wb.save(tmp)
        wb.close()
        ent = livesync.bump(res, user=user, detail=f"{SHEET} {len(clean)} แถว")
    return {"ok": True, "rows": len(clean), "backup": bak, "rev": ent["rev"]}


# ── รูปแบบที่เอาไปใช้ส่งเมล ───────────────────────────────────────────────────

def directory() -> dict:
    """รายชื่อที่จัดกลุ่มพร้อมใช้

    {"teams": {TEAMKEY: {"team": ชื่อตามที่พิมพ์, "to": [...], "cc": [...]}},
     "all_to": [...], "all_cc": [...],      # แถวทีม `*` — ได้รับทุกฉบับ
     "rows": [...], "rev": n, "warnings": [...]}
    """
    data = read_rows()
    teams, all_to, all_cc, warn = {}, [], [], []
    for r in data["rows"]:
        if (r["ACTIVE"] or "Y").upper().startswith("N"):
            continue
        mail = r["EMAIL"]
        if not _EMAIL_RE.match(mail):
            warn.append(f"อีเมลไม่ถูกต้อง: '{mail}' (ทีม {r['TEAM'] or '—'})")
            continue
        key = norm_team(r["TEAM"])
        cc = (r["ROLE"] or "TO").upper() == "CC"
        if key == ALL_TEAMS:
            (all_cc if cc else all_to).append(mail)
            continue
        if "," in r["TEAM"] or ";" in r["TEAM"]:
            warn.append(f"ชื่อทีม '{r['TEAM']}' มีลูกน้ำ — ระบบใช้ลูกน้ำแยกทีม "
                        f"ทีมนี้จะไม่ถูกจับคู่กับงานใด")
        t = teams.setdefault(key, {"team": r["TEAM"].strip(), "to": [], "cc": []})
        bag = t["cc"] if cc else t["to"]
        if mail not in bag:
            bag.append(mail)
    return {"teams": teams, "all_to": all_to, "all_cc": all_cc,
            "rows": data["rows"], "rev": data["rev"], "warnings": warn}


def for_team(team: str, dirc: dict = None) -> dict:
    """ผู้รับของทีมหนึ่ง (รวมแถว `*` ที่ได้รับทุกฉบับ) → {"to": [...], "cc": [...]}

    คนที่อยู่ทั้งในทีมและในกลุ่ม `*` ต้องไม่ได้เมลซ้ำสองช่อง — ช่อง To ชนะ CC
    """
    d = dirc or directory()
    t = d["teams"].get(norm_team(team)) or {"to": [], "cc": []}
    to = list(dict.fromkeys(t["to"] + d["all_to"]))
    cc = [m for m in dict.fromkeys(t["cc"] + d["all_cc"]) if m not in to]
    return {"to": to, "cc": cc}

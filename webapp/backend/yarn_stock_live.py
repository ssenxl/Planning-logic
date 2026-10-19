"""
yarn_stock_live.py — Dashboard › Stock Yarn : ด้ายแต่ละ ITEM มี LOT ไหน PO ไหน กี่ kg กี่ลูก

แหล่งข้อมูล = Yarn/view_yarn_aging_received_797.xlsx ที่ View_Yarn_Aging.py ดึงจาก view
สต๊อกด้ายบน Oracle (ตั้งเวลาดึงทุกเช้าผ่านโหมด "yarn" ของ runner)

**ไม่คำนวณอะไรเลย** (user สั่ง) — ส่งแถวดิบของ view ขึ้นไปทีละแถว 1:1
TOTAL_WEIGHT / TOTAL_YARN / YARN_WEIGHT = ค่าจากไฟล์ตรง ๆ ไม่รวม ไม่เฉลี่ย ไม่ปัดทศนิยม
(เดิมยุบแถวเป็น PO แล้ว sum + คิด kg/ลูก = sum(kg) ÷ sum(ลูก) → ไม่ตรงคอลัมน์ YARN_WEIGHT 3.5% ของแถว)

1 แถวของ view = ใบรับเข้า 1 ใบ × ที่เก็บ 1 ที่ (LOCATOR_ID) — PO เดียวกันจึงแตกได้หลายแถว
ส่ง LOCATOR_ID ขึ้นไปโชว์ด้วย (user สั่ง 15 ต.ค. 2569 — เห็น PO เดียวกันแยก 2 แถวแล้วนึกว่าซ้ำ ที่จริงคนละที่เก็บ)
ยังมีแถวที่ค่าทุกช่องที่ส่งไปเหมือนกันได้ (ใบรับคนละใบ ที่เก็บเดียวกัน) ไม่ใช่ข้อมูลซ้ำ
หน้าเว็บจัดวาง ITEM → YARN_LOT → PO_NO เอง (จัดกลุ่มอย่างเดียว ไม่รวมยอด)
กรองตามกติกา "CUSTOMER ใช้ได้เฉพาะ lot" (ตั้งที่หน้า Monitor Yarn) ทำที่หน้าเว็บ — ดู yarnRules.js

**ด้าย DEFECT / ด้ายขาย ตัดทิ้งที่นี่ตั้งแต่อ่านไฟล์** (user สั่ง 15 ต.ค. 2569 "ไม่เอามาคิด ตัดออกไปเลย") —
ทุกที่ที่อ่านสต๊อกด้ายผ่าน table() จึงไม่เห็นแถวพวกนี้เลย: หน้า Stock Yarn, catalog ของหน้า Monitor Yarn,
ยอดใช้ได้ (yarn_ava_live) → แผนผลิต YarnStockCover / แผนด้าย / ล็อกด้าย ; ไม่มีสวิตช์เปิดปิด
"""
import config

_FILE = config.REPO_DIR / "Yarn" / "view_yarn_aging_received_797.xlsx"
_SHEET = 0   # ชีทแรก — ไฟล์มีชีทเดียว (ชื่อชีท = ชื่อ view ไม่อ้างในโค้ด)
_NEED = ("ITEM_CODE", "YARN_LOT", "PO_NO", "TOTAL_WEIGHT", "TOTAL_YARN", "YARN_WEIGHT")
# ไม่บังคับ — ไฟล์เก่าที่ไม่มีคอลัมน์นี้ยังเปิดหน้าได้ แค่ช่อง BUYYER ว่าง
_BUYER = "BUYYER"
# แถวที่ไม่เอามาคิด (user สั่ง 15 ต.ค. 2569) — ดูจากที่เก็บ (LOCATOR_ID) อย่างเดียวทั้ง 2 เกณฑ์
# · ด้าย DEFECT = ที่เก็บขึ้นต้นด้วย DEFECT (รวม DEFECTU…) ; OYA-DEF… ของอ้อมน้อยยังนับ (user เลือก)
#   ⚠️ RECEIVED_GROUP ไม่มีค่า DEFECT (มีแค่ Supplier / เพชรบุรี / จ้างทอ / Others / จ้างย้อม) — ข้อมูลวันนั้น 1,757 แถว 56,865 kg
# · ด้ายขาย = ที่เก็บอยู่ในชีท "Locator ขายเส้นด้าย" ของไฟล์ 14.1 Reports (user ให้มา 15 ต.ค. 2569 "ตัดแค่ locator พวกนี้")
#   ชีทนั้นมี 496 ที่ = ช่วงต่อเนื่อง 6 ช่วงใน _SALE_LOCATOR_RANGES ; เทียบตรงตัวไม่สนตัวพิมพ์ เหมือน VLOOKUP ในไฟล์นั้น
#   แบ่งตามชนิดด้าย: SC ฝ้าย / SP โพลี / ST top dyed / SYC ย้อมเส้นฝ้าย / SYP ย้อมเส้นโพลี / SL สแปนเด็กซ์
#   ข้อมูลวันนั้น 1,962 แถว 28,426 kg (ทุกแถวค้าง ≥ 90 วัน) ; คลังเพิ่มช่องนอกช่วงนี้ = แก้ที่นี่
#   SCRAP-… / CTP ไม่อยู่ในชีท → ยังนับ
#   (รอบแรกดูหมายเหตุจอง REMARK_BOOK มีคำว่า "ขาย"+"ด้าย" — เลิกใช้แล้ว ใช้รายการ locator อย่างเดียว)
# ไฟล์รุ่นก่อนเพิ่มคอลัมน์ LOCATOR_ID (ดึงก่อน deploy) → ตัดไม่ได้ ขึ้นข้อความเตือนแทนการแสดงครบแบบเงียบ ๆ
_LOCATOR = "LOCATOR_ID"
# ประเภทการรับเข้า — view ไม่มีคอลัมน์โรงงาน user เลือกโชว์ตัวนี้แทน (15 ต.ค. 2569) ; ไม่บังคับ ไฟล์เก่าไม่มี = ช่องว่าง
_RECV_TYPE = "RECEIVED_TYPE"
_DEFECT_PREFIX = "DEFECT"
_SALE_LOCATOR_RANGES = {"SC": (100, 300), "SP": (100, 200), "ST": (100, 200),
                        "SYC": (100, 130), "SYP": (100, 130), "SL": (100, 130)}
_SALE_LOCATORS = frozenset(f"{p}{n}" for p, (lo, hi) in _SALE_LOCATOR_RANGES.items() for n in range(lo, hi + 1))

_cache: dict = {}


def _s(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "nat", "none") else s


def _raw_text(v) -> str:
    """ข้อความตามไฟล์ — ทิ้งแค่ช่องว่างจริง (NaN) ; ต่างจาก _s ตรงที่คำว่า "NONE" ที่ view เขียนมาเองยังอยู่
    (BUYYER มี 71 แถวที่เป็นคำว่า NONE — เป็นค่าของ view ไม่ใช่ช่องว่าง โชว์ตามจริง)"""
    if v is None or (isinstance(v, float) and v != v):
        return ""
    return str(v).strip()


def _num(v):
    """ตัวเลขตามไฟล์ (ไม่ปัด) — ว่าง/อ่านไม่ได้ = None ให้หน้าเว็บโชว์ '-' แทนที่จะกลายเป็น 0"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f      # NaN → None


def _defect(r) -> bool:
    return _s(r.get(_LOCATOR)).upper().startswith(_DEFECT_PREFIX)


def _for_sale(r) -> bool:
    return _s(r.get(_LOCATOR)).upper() in _SALE_LOCATORS


def _mtime(p):
    try:
        return p.stat().st_mtime
    except OSError:
        return None


def _read():
    import pandas as pd
    try:
        return pd.read_excel(_FILE, sheet_name=_SHEET)
    except Exception:
        return pd.read_excel(_FILE)            # ชื่อชีทเปลี่ยน → ชีทแรกก็ยังดีกว่าไม่มีข้อมูล


def table() -> dict:
    mtime = _mtime(_FILE)
    src = {"file": _FILE.name, "mtime": mtime}
    if mtime is None:
        return {"source": src, "rows": [],
                "note": f"ยังไม่มีไฟล์ {_FILE.name} — รอรอบดึงอัตโนมัติ หรือกดปุ่ม “ดึงข้อมูลด้าย” บนหน้า Stock Yarn"}

    hit = _cache.get(mtime)
    if hit is not None:
        return hit

    try:
        df = _read()
    except Exception as e:
        return {"source": src, "rows": [], "note": f"อ่านไฟล์ {_FILE.name} ไม่ได้: {e}"}
    miss = [c for c in _NEED if c not in df.columns]
    if miss:
        return {"source": src, "rows": [],
                "note": f"ไฟล์ {_FILE.name} ไม่มีคอลัมน์ {', '.join(miss)}"}

    # ตัดด้าย DEFECT / ด้ายขาย ได้เฉพาะเมื่อไฟล์มีคอลัมน์ LOCATOR_ID (ทั้ง 2 เกณฑ์ดูจากที่เก็บ)
    cut = (_defect, _for_sale) if _LOCATOR in df.columns else ()
    rows = []
    for r in df.to_dict("records"):
        item = _s(r.get("ITEM_CODE"))
        if not item or any(f(r) for f in cut):
            continue
        rows.append({
            "i": item,
            "l": _s(r.get("YARN_LOT")) or "-",
            "p": _s(r.get("PO_NO")) or "-",
            "w": _num(r.get("TOTAL_WEIGHT")),
            "y": _num(r.get("TOTAL_YARN")),
            "yw": _num(r.get("YARN_WEIGHT")),
            "b": _raw_text(r.get(_BUYER)),   # ลูกค้าที่ถือด้ายแถวนี้ไว้ (ว่าง = ยังไม่มีเจ้าของ)
            "lc": _raw_text(r.get(_LOCATOR)),  # ที่เก็บ — ไฟล์รุ่นเก่าไม่มีคอลัมน์นี้ = ว่าง
            "rt": _raw_text(r.get(_RECV_TYPE)),  # ประเภทการรับเข้า — ไฟล์รุ่นเก่าไม่มีคอลัมน์นี้ = ว่าง
        })

    out = {"source": src, "rows": rows}
    if not cut:
        out["note"] = (f"ไฟล์ {_FILE.name} ยังเป็นรุ่นเก่า (ไม่มีคอลัมน์ {_LOCATOR}) — ยังตัดด้าย DEFECT / ด้ายขายออกไม่ได้ "
                       f"ตัวเลขยังรวมด้ายพวกนี้อยู่ รอรอบดึงอัตโนมัติ หรือกดปุ่ม “ดึงข้อมูลด้าย” บนหน้า Stock Yarn")
    _cache.clear()          # แคชไว้ชุดเดียวพอ (ไฟล์เปลี่ยน = mtime เปลี่ยน)
    _cache[mtime] = out
    return out


_cat_cache: dict = {}


def hidden_suffixes(values: dict) -> tuple:
    """ท้าย PO ที่ต้องซ่อนตามค่าหน้า Monitor Yarn (สวิตช์ปิด = ไม่ซ่อนอะไร) — ตัวพิมพ์ใหญ่ ตัดช่องว่าง
    ตัวเดียวกับ hiddenPoSuffixes ใน webapp/frontend/src/yarnRules.js"""
    if not (values or {}).get("KP_YARN_HIDE_PO_ON"):
        return ()
    return tuple(s for s in (str(x).strip().upper() for x in (values.get("KP_YARN_HIDE_PO_SUFFIXES") or [])) if s)


def _po_hidden(po: str, suffixes: tuple) -> bool:
    return bool(suffixes) and str(po).strip().upper().endswith(suffixes)


def owners_of(rules, item: str, lot: str) -> list:
    """ลูกค้าที่ ITEM+LOT นี้ถูกสงวนไว้ให้ตามกติกา KP_YARN_CUSTOMER_LOTS ([] = ไม่ได้สงวนให้ใคร)
    ตัวเดียวกับ ownersOf ใน webapp/frontend/src/yarnRules.js — แก้ที่หนึ่งต้องแก้อีกที่
    (ไม่สนตัวพิมพ์ ตัดช่องว่าง · item ว่าง = ทุก ITEM · match 'exact' = lot ตรงตัว นอกนั้น = ขึ้นต้นด้วย)"""
    norm = lambda v: str(v if v is not None else "").strip().upper()
    out = []
    for r in rules or []:
        c = str((r or {}).get("customer") or "").strip()
        want = norm(r.get("lot"))
        if not c or c in out or not want:
            continue
        if norm(r.get("item")) and norm(r.get("item")) != norm(item):
            continue
        l = norm(lot)
        if (l == want) if r.get("match") == "exact" else l.startswith(want):
            out.append(c)
    return out


def catalog(hide_suffixes: tuple = ()) -> dict:
    """รายชื่อที่หน้า Monitor Yarn ใช้ตั้งกติกา "CUSTOMER ใช้ได้เฉพาะ lot":
    buyers     = ชื่อ BUYYER ทั้งหมดในไฟล์ (ให้เลือกแทนการพิมพ์เอง — ชื่อต้องตรงตัวอักษรกับข้อมูล)
    item_codes = ITEM_CODE ทั้งหมด
    held       = [ITEM, LOT, BUYYER, จำนวนแถว] ที่มีในคลัง — หน้า Monitor Yarn ใช้บอกว่ากติกาตรงกับ lot กี่เบอร์
                 และมีลูกค้าอื่นถือ lot ที่สงวนไว้อยู่กี่แถว (นับแถวอย่างเดียว ไม่ได้รวมน้ำหนักหรือค่าใด ๆ)
    hide_suffixes = ท้าย PO ที่หน้า Stock Yarn ซ่อนอยู่ (hidden_suffixes) — ไม่นับแถวพวกนั้น ตัวเลขสองหน้าจะได้ตรงกัน
    ⚠️ ห้ามตั้งชื่อช่องว่า "items" — server เอาผลนี้ไปต่อท้ายข้อมูลกลุ่มค่าตั้งค่า ซึ่งมี "items" (รายการค่า) อยู่แล้ว"""
    t = table()
    key = (t["source"].get("mtime"), tuple(hide_suffixes))
    hit = _cat_cache.get(key)
    if hit is not None:
        return hit
    rows = [r for r in t["rows"] if not _po_hidden(r["p"], key[1])]
    held: dict = {}
    for r in rows:
        if r["l"] != "-":
            k = (r["i"], r["l"], r["b"])
            held[k] = held.get(k, 0) + 1
    out = {
        "buyers": sorted({r["b"] for r in rows if r["b"]}, key=str.upper),
        "item_codes": sorted({r["i"] for r in rows}),
        "held": [[i, l, b, n] for (i, l, b), n in sorted(held.items())],
    }
    _cat_cache.clear()
    _cat_cache[key] = out
    return out

"""
teams_notify.py — การ์ด Machine Availability เข้า Microsoft Teams + ลิงก์ดูหน้านั้นแบบไม่ต้อง login

ส่งเองทุก **วันจันทร์ 08:00** (scheduler.py) เข้าแชท/ช่องเดียว — ไม่มีปุ่มส่งบนหน้าเว็บ (user สั่ง)
ลิงก์มีไว้ให้ "ใครก็ได้" ในแชทกดเข้าไปดูเฉย ๆ
ส่งทดสอบบน server: docker exec knitplan-app python -c "import teams_notify as t; print(t.send())"

**ช่องทาง = Power Automate Workflows** แบบเดียวกับ C:\\vscode\\Noti_booking\\notify_teams.py
(ใน Teams เปิดแชท/ช่องที่ต้องการ › … › Workflows › "Send webhook alerts to a chat" › คัดลอก URL)
แล้วตั้ง `KP_TEAMS_URL` ใน .env ของ server — URL มีลายเซ็น (sig) ใครถือก็โพสต์เข้าแชทนั้นได้
จึงห้ามอยู่ในโค้ด/ห้ามคืนให้หน้าเว็บ ; **ห้ามใช้ flow เมล (KP_MAIL_FLOW_URL) ร่วม** — flow ที่มีขั้น
โพสต์การ์ดเคยทำให้เมลหายเงียบ (ดู mailer.py)

**ลิงก์ + การ์ดโชว์เฉพาะสัปดาห์ที่ทำแผนได้** (≥ สัปดาห์นี้ + KP_FREEZE_WEEKS, ปกติ week+2 — plannable())
หน้า Machine Availability ที่ login ยังเห็นย้อนหลังครบ (user สั่ง 15 ต.ค.)

**ลิงก์ไม่ต้อง login** = /knitplan/?share=mc-ava&k=<รหัส> — หน้าเว็บเปิดได้แค่หน้า Machine Availability หน้าเดียว
backend เปิดให้แค่ GET /api/share/mc-ava* (server.py) ซึ่งเรียก mc_ava_live ตัวเดียวกับหน้าที่ login
→ ตัวเลขตรงกันเสมอ ห้ามคำนวณแยกที่นี่ ; รหัสไม่ตรง = 404
รหัส = `KP_SHARE_KEY` ถ้าตั้งไว้ ไม่ตั้ง = แตกจากกุญแจ sign token (auth_secret อยู่บน volume จึงคงเดิม
ข้าม deploy) — ลิงก์หลุดไปที่ที่ไม่ควร: ตั้ง KP_SHARE_KEY ใหม่ ลิงก์เก่าใช้ไม่ได้ทันที

**กับดักของ Teams**: การ์ดเกิน 28 KB จะไม่ขึ้นในแชท แต่ webhook ยังตอบ 202 เหมือนสำเร็จ
→ วัดขนาดเองแล้วตัดแถวก่อนส่ง (_fit) ; 202 = เข้าคิว flow เท่านั้น การ์ดไม่ขึ้นต้องดู Run history ของ flow
"""
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta

import auth
import config
import mc_ava_live

if str(config.REPO_DIR) not in sys.path:
    sys.path.insert(0, str(config.REPO_DIR))

import plan_config  # noqa: E402  (ต้องเติม sys.path ก่อน — ไฟล์อยู่ราก repo)

TIMEOUT = 60
MAX_BYTES = 25000            # Teams รับได้ 28 KB — กันไว้ต่ำกว่าเผื่อ
WARN, CRIT = 70, 90          # = TIER ใน McAvailability.jsx (สีเดียวกับหน้าเว็บ)
AHEAD_WEEKS = 8              # กราฟสัปดาห์ข้างหน้าบนการ์ด
TOP_GROUPS = 5               # กลุ่มเครื่องที่แน่นที่สุดกี่กลุ่ม
TOP_SCOPE_WEEKS = 4          # ...ดูจากกี่สัปดาห์ข้างหน้า
BAR_CHARS = 20
# รอบอัตโนมัติ: booking ใหม่กว่าตัวเลข → คำนวณให้เสร็จก่อนส่ง (รอ pipeline ที่รันค้างได้ด้วย)
WAIT_FRESH = 45 * 60
# ต้องเป็น query string บนหน้าแรก ไม่ใช่ path ย่อย — vite build ด้วย base './' ถ้าลิงก์เป็น
# /knitplan/share/mc-ava เบราว์เซอร์จะไปหา JS ที่ /knitplan/share/assets/... (ไม่มีจริง) → หน้าขาว
SHARE_PATH = "/?share=mc-ava"

_LOG = config.OUTPUT_DIR / "teams_notify_log.jsonl"
_send_lock = threading.Lock()

_TH_MONTH = ("ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.",
             "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค.")


class TeamsError(RuntimeError):
    pass


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def flow_url() -> str:
    return _env("KP_TEAMS_URL")


def public_base() -> str:
    """URL ที่คนในแชทกดเข้า — ทางการคือผ่าน nginx (เปิดได้ในเครือข่ายบริษัท/VPN เท่านั้น)"""
    return _env("KP_PUBLIC_URL", "http://docker-webchat/knitplan").rstrip("/")


# ---------------------------------------------------------------- ลิงก์ไม่ต้อง login
def share_key() -> str:
    k = _env("KP_SHARE_KEY")
    if k:
        return k
    return hmac.new(auth._get_secret(), b"share:mc-ava", hashlib.sha256).hexdigest()[:32]


def check_share_key(k) -> bool:
    return bool(k) and hmac.compare_digest(str(k), share_key())


def share_url() -> str:
    return f"{public_base()}{SHARE_PATH}&k={share_key()}"


# ---------------------------------------------------------------- ตัวเลขบนการ์ด
def _pct(c) -> float | None:
    return (c["used"] / c["total"] * 100) if c and c.get("total") else None


def kpi(tb: dict) -> dict:
    """KPI 4 ช่องชุดเดียวกับหัวหน้า Machine Availability ตอนเปิดหน้า (ทุกกลุ่ม ทุกสัปดาห์ ไม่กรอง)
    — สูตรลอกจาก useMemo `kpi` ใน McAvailability.jsx ทีละบรรทัด แก้ที่หนึ่งต้องแก้อีกที่"""
    weeks, rows = tb.get("weeks") or [], tb.get("rows") or []
    totals, full_weeks = [], 0
    for w in weeks:
        t = rm = 0
        for r in rows:
            c = r["cells"].get(w)
            if c:
                t += c["total"]
                rm += c["remain"]
        totals.append(t)
        full_weeks += rm <= 0
    hot = full_cells = 0
    for r in rows:
        h = False
        for w in weeks:
            p = _pct(r["cells"].get(w))
            if p is None:
                continue
            if p >= CRIT:
                h = True
                full_cells += 1
            elif p >= WARN:
                h = True
        hot += h
    return {"groups": len(rows),
            "factories": len({r["factory"] for r in rows if r.get("factory")}),
            "machines": max(totals) if totals else 0,
            "full_weeks": full_weeks, "hot": hot, "full_cells": full_cells,
            "weeks": len(weeks)}


def _wk(w) -> int:
    return mc_ava_live._wk_int(w)


def _friday(year, wk) -> date | None:
    """วันศุกร์ที่เริ่มสัปดาห์ (สัปดาห์ = ศุกร์–พฤหัส : วันที่ +3 วันแล้วคิด ISO week)"""
    try:
        return date.fromisocalendar(int(year), int(wk), 1) - timedelta(days=3)
    except (TypeError, ValueError):
        return None


def _thd(d: date) -> str:
    return f"{d.day} {_TH_MONTH[d.month - 1]}"


def _cur_week(today: date) -> tuple:
    """(ปี ISO, สัปดาห์) ของวันนี้ — สัปดาห์ศุกร์–พฤหัส : +3 วันแล้วคิด ISO week"""
    return tuple((today + timedelta(days=3)).isocalendar()[:2])


def first_plan_week(today: date | None = None) -> tuple:
    """(ปี ISO, สัปดาห์) แรกที่ทำแผนได้ = สัปดาห์นี้ + KP_FREEZE_WEEKS (หน้าตั้งค่าระบบ ; ปกติ 2)
    — ค่าเดียวกับพื้นที่ Planning/Replan วางงาน และแกนของ Gantt ห้ามเขียนเลขไว้เอง"""
    plan_config.reload()
    fw = int(plan_config.get("KP_FREEZE_WEEKS"))
    return tuple(((today or date.today()) + timedelta(days=3, weeks=fw)).isocalendar()[:2])


def plannable(tb: dict, today: date | None = None) -> dict:
    """ตาราง Machine Availability เฉพาะสัปดาห์ที่ทำแผนได้ (≥ week+KP_FREEZE_WEEKS)

    ใช้กับ **ลิงก์ไม่ต้อง login + การ์ด Teams เท่านั้น** — หน้าที่ login ยังเห็นย้อนหลังครบ (user สั่ง 15 ต.ค.)
    ตัดแค่ช่วงสัปดาห์ ตัวเลขทุกช่องยังมาจาก mc_ava_live.table() ชุดเดียวกัน ; ทำซ้ำได้ (ตัดแล้วตัดอีกได้ผลเดิม)
    """
    weeks, years = tb.get("weeks") or [], tb.get("years") or {}
    if not weeks:
        return tb
    fy, fw = first_plan_week(today)
    if all(w in years for w in weeks):
        keep = [w for w in weeks if (int(years[w]), _wk(w)) >= (fy, fw)]
    else:                                   # ไฟล์รุ่นเก่าไม่มีปี → หาเลขสัปดาห์ในลำดับที่เรียงตามเวลาแล้ว
        i = next((i for i, w in enumerate(weeks) if _wk(w) == fw), None)
        keep = weeks[i:] if i is not None else weeks
    ks = set(keep)
    rows = []
    for r in tb.get("rows") or []:
        cells = {w: c for w, c in r["cells"].items() if w in ks}
        if cells:                           # กลุ่มที่ไม่มีตัวเลขในช่วงนี้เลย ไม่ต้องโชว์แถวว่าง
            rows.append({**r, "cells": cells})
    return {**tb, "weeks": keep, "rows": rows,
            "years": {w: y for w, y in years.items() if w in ks}}


def _week_agg(tb: dict, w) -> dict:
    t = u = rm = 0
    for r in tb["rows"]:
        c = r["cells"].get(w)
        if c:
            t += c["total"]
            u += c["used"]
            rm += c["remain"]
    return {"total": t, "used": u, "remain": rm, "pct": (u / t * 100) if t else None}


def _tight(tb: dict, weeks: list) -> list:
    """กลุ่มเครื่องที่ % ใช้เครื่องสูงสุด (≥70%) ในช่วงสัปดาห์ที่ให้มา — ต่อกลุ่มเอาสัปดาห์ที่แน่นสุด"""
    out = []
    for r in tb["rows"]:
        best = None
        for w in weeks:
            c = r["cells"].get(w)
            p = _pct(c)
            if p is None:
                continue
            if best is None or p > best[0]:
                best = (p, w, c["remain"])
        if best and best[0] >= WARN:
            out.append({"cat": r.get("cat") or r["key"], "factory": r.get("factory") or "",
                        "pool": r.get("pool_label") or "", "pct": best[0], "week": best[1],
                        "remain": best[2]})
    out.sort(key=lambda x: (-x["pct"], x["remain"]))
    return out


# ---------------------------------------------------------------- Adaptive Card
def _tone(p):
    """(สีตัวอักษร, style พื้น) ตามเกณฑ์ 70/90 ของหน้าเว็บ"""
    if p is None:
        return "Default", "emphasis"
    if p >= CRIT:
        return "Attention", "attention"
    if p >= WARN:
        return "Warning", "warning"
    return "Good", "good"


def _t(text, **kw) -> dict:
    return {"type": "TextBlock", "text": str(text), "wrap": True, **kw}


def _run(text, **kw) -> dict:
    return {"type": "TextRun", "text": str(text), **kw}


def _tile(label, value, note, color="Default", style="emphasis") -> dict:
    return {"type": "Column", "width": "stretch", "style": style, "items": [
        _t(label, size="Small", weight="Bolder", isSubtle=True),
        _t(value, size="ExtraLarge", weight="Bolder", color=color, spacing="None"),
        _t(note, size="Small", isSubtle=True, spacing="None"),
    ]}


def _section(title, note="") -> dict:
    items = [_t(title, size="Medium", weight="Bolder")]
    if note:
        items.append(_t(note, size="Small", isSubtle=True, spacing="None"))
    return {"type": "Container", "spacing": "Large", "separator": True, "items": items}


def _bar(p) -> list:
    """แท่งกราฟตัวอักษร (Adaptive Card ไม่มีกราฟแท่ง) — ต้อง monospace ไม่งั้นแต่ละแถวยาวไม่เท่ากัน"""
    color, _ = _tone(p)
    fill = 0 if not p else max(1, round(min(p, 100) / 100 * BAR_CHARS))
    runs = []
    if fill:
        runs.append(_run("█" * fill, fontType="Monospace", color=color))
    if BAR_CHARS - fill:
        runs.append(_run("░" * (BAR_CHARS - fill), fontType="Monospace", isSubtle=True))
    return runs


def _free(remain: int) -> str:
    """เครื่องว่าง → ข้อความบนการ์ด ; 0 หรือติดลบ (booking จองเกิน) ห้ามขึ้น "ว่าง -1" (user สั่ง)"""
    if remain > 0:
        return f"ว่าง {remain:,}"
    return "ไม่ว่าง" if remain == 0 else f"ไม่ว่าง (เกิน {-remain:,})"


def _week_row(tb, w) -> dict:
    a = _week_agg(tb, w)
    color, _ = _tone(a["pct"])
    y = (tb.get("years") or {}).get(w)
    d = _friday(y, _wk(w)) if y else None
    label = [_run(f"W{_wk(w)}", weight="Bolder")]
    if d:
        label.append(_run(f"  {_thd(d)}", size="Small", isSubtle=True))
    right = (f"{_free(a['remain'])} / {a['total']:,}" if a["remain"] > 0
             else f"{_free(a['remain'])} · ทั้งหมด {a['total']:,}")
    return {"type": "ColumnSet", "spacing": "Small", "columns": [
        {"type": "Column", "width": "90px", "verticalContentAlignment": "Center",
         "items": [{"type": "RichTextBlock", "inlines": label}]},
        {"type": "Column", "width": "auto", "verticalContentAlignment": "Center",
         "items": [{"type": "RichTextBlock", "inlines": _bar(a["pct"])}]},
        {"type": "Column", "width": "50px", "verticalContentAlignment": "Center",
         "items": [_t("—" if a["pct"] is None else f"{a['pct']:.0f}%", weight="Bolder",
                      color=color, horizontalAlignment="Right", wrap=False)]},
        {"type": "Column", "width": "stretch", "verticalContentAlignment": "Center",
         "items": [_t(right, size="Small", isSubtle=True, horizontalAlignment="Right")]},
    ]}


def _tight_row(g) -> dict:
    color, _ = _tone(g["pct"])
    sub = " · ".join(x for x in (g["factory"], g["pool"]) if x) or "-"
    return {"type": "ColumnSet", "spacing": "Small", "separator": True, "columns": [
        {"type": "Column", "width": "stretch", "verticalContentAlignment": "Center", "items": [
            _t(g["cat"], weight="Bolder", size="Small"),
            _t(sub, size="Small", isSubtle=True, spacing="None"),
        ]},
        {"type": "Column", "width": "auto", "verticalContentAlignment": "Center", "items": [
            _t(f"{g['pct']:.0f}%", weight="Bolder", color=color, horizontalAlignment="Right",
               wrap=False),
            _t(f"W{_wk(g['week'])} · {_free(g['remain'])}", size="Small", isSubtle=True,
               spacing="None", horizontalAlignment="Right", wrap=False),
        ]},
    ]}


def build_card(tb: dict, *, fresh: bool = True, ahead_n: int = AHEAD_WEEKS,
               top_n: int = TOP_GROUPS, today: date | None = None) -> dict:
    today = today or date.today()
    # ทั้งการ์ดคิดเฉพาะสัปดาห์ที่ทำแผนได้ (≥ week+2) — ตัวเลขเดียวกับหน้าลิงก์ (server.py ตัดแบบเดียวกัน)
    tb = plannable(tb, today)
    k = kpi(tb)
    ahead = tb.get("weeks") or []
    cy, cw = _cur_week(today)
    fri = _friday(cy, cw)
    period = f"สัปดาห์นี้ W{cw}" + (f" · {_thd(fri)} – {_thd(fri + timedelta(days=6))} {fri.year + 543}"
                                    if fri else "")
    if ahead:
        period += f" · แสดงตั้งแต่ W{_wk(ahead[0])} (สัปดาห์ที่ทำแผนได้)"

    full_c, full_s = ("Attention", "attention") if k["full_weeks"] else ("Good", "good")
    hot_c, hot_s = ("Warning", "warning") if k["hot"] else ("Good", "good")
    body = [
        {"type": "Container", "style": "accent", "bleed": True, "items": [
            {"type": "ColumnSet", "columns": [
                {"type": "Column", "width": "auto", "verticalContentAlignment": "Center",
                 "items": [_t("📊", size="ExtraLarge", wrap=False)]},
                {"type": "Column", "width": "stretch", "verticalContentAlignment": "Center", "items": [
                    _t("KNIT PLAN · รายงานประจำสัปดาห์", size="Small", weight="Bolder",
                       color="Accent"),
                    _t("Machine Availability", size="ExtraLarge", weight="Bolder", spacing="None"),
                    _t(period, isSubtle=True, spacing="None"),
                ]},
            ]},
        ]},
        {"type": "ColumnSet", "spacing": "Medium", "columns": [
            _tile("กลุ่มเครื่อง", f"{k['groups']:,}", f"{k['factories']} โรงงาน · {k['weeks']} สัปดาห์"),
            _tile("เครื่องทั้งหมด", f"{k['machines']:,}", "รวมทุกกลุ่ม", color="Accent"),
        ]},
        {"type": "ColumnSet", "spacing": "Small", "columns": [
            _tile("⚠ สัปดาห์ที่เครื่องหมด", f"{k['full_weeks']:,}", "รวมทุกกลุ่มไม่เหลือเครื่องว่าง",
                  color=full_c, style=full_s),
            _tile("⚠ MC Used ≥70%", f"{k['hot']:,} กลุ่ม", f"มี {k['full_cells']:,} ช่องที่เกิน 90%",
                  color=hot_c, style=hot_s),
        ]},
    ]
    if not fresh:
        body.append({"type": "Container", "style": "warning", "spacing": "Medium", "items": [
            _t("⚠ booking ใหม่กว่าตัวเลขชุดนี้ และคำนวณใหม่ไม่ทันก่อนส่ง — "
               "เปิด Dashboard เพื่อดูตัวเลขล่าสุด", size="Small", weight="Bolder")]})

    shown = ahead[:ahead_n]
    if shown:
        body.append(_section(f"📈 เครื่องที่ใช้ไป {len(shown)} สัปดาห์แรกที่ทำแผนได้",
                             "รวมทุกกลุ่มเครื่อง · เขียว < 70% · เหลือง 70–90% · แดง ≥ 90%"))
        body += [_week_row(tb, w) for w in shown]

    scope = ahead[:TOP_SCOPE_WEEKS]
    tight = _tight(tb, scope)
    if top_n > 0 and scope:
        body.append(_section(f"🔥 กลุ่มเครื่องที่แน่นที่สุด · {len(scope)} สัปดาห์แรกที่ทำแผนได้",
                             f"กลุ่มที่ใช้เครื่องตั้งแต่ {WARN}% ขึ้นไป · {len(tight)} กลุ่ม"
                             + (f" (โชว์ {min(top_n, len(tight))} อันดับแรก)" if len(tight) > top_n else "")))
        if tight:
            body += [_tight_row(g) for g in tight[:top_n]]
        else:
            body.append({"type": "Container", "style": "good", "items": [
                _t(f"✅ ทุกกลุ่มยังใช้เครื่องไม่ถึง {WARN}%", weight="Bolder", color="Good")]})

    computed = tb.get("computed_at")
    when = datetime.fromtimestamp(computed) if computed else None
    foot = (f"ตัวเลขคำนวณจาก booking เมื่อ {when.day}/{when.month}/{when.year + 543} "
            f"{when:%H:%M} น." if when else "")
    foot += (" · " if foot else "") + "ลิงก์เปิดได้ในเครือข่ายบริษัท ไม่ต้อง login"
    body.append(_t(foot, size="Small", isSubtle=True, spacing="Large", separator=True))

    return {"type": "message", "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard", "version": "1.5",
            "msteams": {"width": "Full"},
            "body": body,
            "actions": [{"type": "Action.OpenUrl", "title": "📊 เปิด Dashboard Machine Availability",
                         "url": share_url(), "style": "positive"}],
        },
    }]}


def card_size(card: dict) -> int:
    return len(json.dumps(card, ensure_ascii=False).encode("utf-8"))


def _fit(tb: dict, **kw) -> tuple:
    """ตัดแถวกราฟ/กลุ่มแน่นลงจนการ์ดเล็กพอ — เกินเพดาน Teams จะกลืนการ์ดเงียบ ๆ (ยังตอบ 202)"""
    ahead_n, top_n = AHEAD_WEEKS, TOP_GROUPS
    while True:
        card = build_card(tb, ahead_n=ahead_n, top_n=top_n, **kw)
        size = card_size(card)
        if size <= MAX_BYTES or (ahead_n <= 1 and top_n <= 0):
            return card, size
        if top_n > 0 and top_n >= ahead_n // 2:
            top_n -= 1
        else:
            ahead_n -= 1


# ---------------------------------------------------------------- ความสดของตัวเลข
def _is_stale() -> bool:
    bk = mc_ava_live.booking_meta()
    _, path = mc_ava_live._current_source()
    return bool(bk and (path is None or bk["mtime"] > path.stat().st_mtime))


def _ensure_fresh(wait_s: int) -> bool:
    """booking ใหม่กว่าตัวเลข → สั่งคำนวณ (AVA_MC) แล้วรอจนเสร็จ ; คืน True = ตัวเลขสดแล้ว

    pipeline รันค้างอยู่ (start_refresh ตอบ busy) → รอแล้วลองใหม่ ; รอบที่สั่งเองล้ม → เลิกรอ
    (ไม่งั้นรัน AVA_MC ซ้ำทุก 15 วิไปจนหมดเวลา) ส่งการ์ดพร้อมคำเตือนแทน
    """
    deadline = time.time() + wait_s
    started = False
    while True:
        st = mc_ava_live.status()
        if not st.get("running"):
            if not _is_stale():
                return True
            if started and st.get("error"):
                return False
            if time.time() >= deadline:
                return False
            r = mc_ava_live.start_refresh()
            if r["ok"]:
                started = True
            elif r.get("reason") == "empty":
                return False
        elif time.time() >= deadline:
            return False
        time.sleep(15)


# ---------------------------------------------------------------- ส่ง + log
def _log(entry: dict) -> None:
    try:
        _LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({"at": datetime.now().isoformat(timespec="seconds"), **entry},
                               ensure_ascii=False) + "\n")
    except OSError:
        pass


def send(wait_s: int = 0, trigger: str = "manual") -> dict:
    """ประกอบการ์ดจากตาราง Machine Availability ปัจจุบันแล้วโพสต์เข้า Teams → {ok, status, bytes, ...}
    ผลทุกครั้ง (สำเร็จ/ล้ม) จดลง data_plan/teams_notify_log.jsonl"""
    url = flow_url()
    if not url:
        _log({"trigger": trigger, "ok": False, "error": "ยังไม่ได้ตั้ง KP_TEAMS_URL"})
        raise TeamsError("ยังไม่ได้ตั้งค่า KP_TEAMS_URL (URL ของ Workflow ใน Teams) ใน .env")
    if not _send_lock.acquire(blocking=False):
        raise TeamsError("กำลังส่งการ์ดอยู่ — รอสักครู่")
    try:
        fresh = _ensure_fresh(wait_s) if wait_s else not _is_stale()
        tb = plannable(mc_ava_live.table())
        if not tb.get("ok") or not tb.get("rows"):
            raise TeamsError(tb.get("reason") or "ยังไม่มีข้อมูล Machine Availability ในสัปดาห์ที่ทำแผนได้")
        card, size = _fit(tb, fresh=fresh)

        import requests
        try:
            resp = requests.post(url, json=card, timeout=TIMEOUT)
        except requests.RequestException as e:
            # ข้อความ error ของ requests มี URL เต็ม (รวม sig) ติดมา — เก็บแค่ชนิด error
            raise TeamsError(f"ต่อ Teams ไม่ได้ ({type(e).__name__})") from None
        if resp.status_code not in (200, 201, 202):
            raise TeamsError(f"Teams ตอบกลับไม่สำเร็จ (HTTP {resp.status_code}): {resp.text[:300]}")
        res = {"ok": True, "status": resp.status_code, "bytes": size, "fresh": fresh,
               "groups": len(tb["rows"]), "file": tb.get("file")}
        _log({"trigger": trigger, **res})
        return res
    except TeamsError as e:
        _log({"trigger": trigger, "ok": False, "error": str(e)})
        raise
    finally:
        _send_lock.release()


def weekly_job() -> None:
    """รอบอัตโนมัติจันทร์ 08:00 (scheduler.py) — คำนวณให้สดก่อน แล้วส่ง ; พลาดจดลง log ไม่โยนต่อ"""
    try:
        r = send(wait_s=WAIT_FRESH, trigger="schedule")
        print(f"[teams] ส่งการ์ด Machine Availability แล้ว (HTTP {r['status']}, {r['bytes']} bytes)")
    except TeamsError as e:
        print(f"[teams] ส่งการ์ดไม่สำเร็จ: {e}")
    except Exception as e:                       # noqa: BLE001 — job ล้มห้ามพา scheduler ล้ม
        _log({"trigger": "schedule", "ok": False, "error": f"{type(e).__name__}: {e}"})
        print(f"[teams] ส่งการ์ดล้ม: {e}")

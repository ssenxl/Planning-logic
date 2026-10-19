"""
mailer.py — ส่งอีเมลออกจาก webapp ผ่าน Power Automate flow (webhook)

**ทำไมเป็น flow ไม่ใช่ Microsoft Graph**: ระบบแจ้งเตือน %Loss ของบริษัท
(C:\\vscode\\AI_Production loss — `notify_teams.py`, `MAIL_MODE=flow`) ใช้วิธีนี้ส่งเมลได้จริง
ทุกวันจาก container บน docker-webchat เครื่องเดียวกับ KnitPlan → พิสูจน์แล้วว่า firewall
ขาออกผ่าน และไม่ต้องมี Azure app registration / client secret / admin consent ให้ต้องรอ IT
(Graph แบบ application permission ยังทำทีหลังได้ ถ้าวันหนึ่งอยากให้เมลออกจากกล่องกลาง —
เปลี่ยนเฉพาะฟังก์ชัน send_html() ตัวเดียว ผู้เรียกไม่ต้องแก้)

flow ฝั่ง Power Automate ต้องผูกช่องของ action "Send an email (V2)" กับคีย์เหล่านี้:
    To       triggerBody()?['mailTo']
    CC       triggerBody()?['mailCc']       (advanced options — ว่างได้)
    Subject  triggerBody()?['mailSubject']
    Body     triggerBody()?['mailHtml']     (ต้องเปิด advanced option "Is HTML" = Yes)

เมลจะออกจาก**บัญชีเจ้าของ connection ใน flow** ไม่ใช่บัญชีที่ล็อกอินหน้าเว็บ — ชื่อผู้กดส่ง
จึงถูกเขียนไว้ในเนื้อเมลแทน (replan_notify.build_html รับพารามิเตอร์ sender)
"""
import os

# ผู้รับหลายคนคั่นด้วย ; หรือ , (รูปแบบเดียวกับ MAIL_TO ของระบบ %Loss)
_SPLIT = (";", ",")

TIMEOUT = int(os.environ.get("KP_MAIL_TIMEOUT", "60") or 60)

# แนบการ์ด Teams ขั้นต่ำไปกับ payload ด้วยหรือไม่ (1 = แนบ)
#
# **ทำไมต้องมีสวิตช์นี้**: flow ที่สร้างจาก template ของ Teams มี action "Post card in a
# chat or channel" เป็นขั้นแรก ซึ่งอ่านการ์ดจาก `attachments` ของ body — ยิงคีย์เมลล้วนเข้าไป
# ขั้นนั้นล้มทันที flow เลยไม่เดินต่อไปถึง "Send an email" ผลคือ trigger ตอบ 202 แต่เมลไม่เคยออก
# (พิสูจน์แล้วด้วยการยิงคู่: ฉบับที่มีการ์ดมาถึง ฉบับที่ไม่มีการ์ดไม่มาเลยสักฉบับ ทุกขนาด)
#
# ตั้ง 1 = ใช้ flow ที่มี action โพสต์การ์ดร่วมกับระบบอื่นได้ทันที (แลกกับมีข้อความสั้น ๆ
# โผล่ในช่อง Teams ของ flow นั้น 1 ใบต่อการส่ง 1 ครั้ง)
# ตั้ง 0 = flow ของ KnitPlan เองที่ไม่มี action Teams เลย (ปลายทางที่ควรไปให้ถึง)
SEND_CARD = (os.environ.get("KP_MAIL_CARD", "0") or "0").strip().lower() in ("1", "true", "yes")

# ── โหมดทดสอบของเมลรายทีม: เลิกใช้แล้ว (user สั่ง 29 ก.ย. 2569) ─────────────────────
# เมลส่งถึงฝ่ายขายตามชีท Sales_Mail เสมอ ; ปุ่มบนหน้าตั้งค่าระบบถูกถอดออก (plan_config ไม่มี
# KP_MAIL_TEAM_TEST แล้ว) — คืน False ตายตัว ห้ามอ่านจาก plan_config/env อีก เพราะอ่านไม่เจอแล้ว
# โค้ดเดิมจะถอยกลับเป็นโหมดทดสอบเอง ; ผู้เรียก (server.py / status()) ยังเรียกได้เหมือนเดิม
def team_test_mode() -> bool:
    return False


def team_test_to() -> list:
    return parse_list(_env("KP_MAIL_TEAM_TEST_TO")) or default_to()


# ขนาดเนื้อเมลที่ Gmail เริ่มตัดแสดง (ยังเปิดอ่านส่วนที่เหลือได้ผ่านลิงก์ "ข้อความถูกตัดทอน")
# Outlook/Exchange แสดงได้เกินนี้ — ใช้เตือนผู้ส่งเท่านั้น ไม่ได้บล็อกการส่ง
CLIP_BYTES = 102 * 1024


class MailError(RuntimeError):
    pass


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def parse_list(v) -> list:
    """'a@x.com; b@x.com' หรือ ['a@x.com'] → ['a@x.com', 'b@x.com'] (ตัดค่าซ้ำ/ว่างทิ้ง)"""
    if v is None:
        return []
    parts = v if isinstance(v, (list, tuple)) else [v]
    out = []
    for p in parts:
        s = str(p or "")
        for sep in _SPLIT:
            s = s.replace(sep, "\n")
        for one in s.split("\n"):
            one = one.strip()
            if one and one not in out:
                out.append(one)
    return out


def flow_url() -> str:
    return _env("KP_MAIL_FLOW_URL")


def default_to() -> list:
    return parse_list(_env("KP_MAIL_TO"))


def default_cc() -> list:
    return parse_list(_env("KP_MAIL_CC"))


def status() -> dict:
    """ตั้งค่าเมลพร้อมส่งหรือยัง — ให้หน้าเว็บบอกสาเหตุได้ก่อนที่ผู้ใช้จะกดส่งแล้วเจอ error

    ไม่คืน URL กลับไปหน้าเว็บ (มันเป็นความลับ: ใครถือ URL นี้ยิงเมลออกในนามบริษัทได้)
    บอกแค่ว่า "ตั้งค่าแล้วหรือยัง"
    """
    url = flow_url()
    return {
        "ready": bool(url),
        "reason": "" if url else "ยังไม่ได้ตั้งค่า KP_MAIL_FLOW_URL (URL ของ Power Automate flow) ใน .env",
        "to": default_to(),
        "cc": default_cc(),
        "clip_bytes": CLIP_BYTES,
        # หน้าเว็บต้องโชว์ให้เห็นชัดว่ากำลังอยู่โหมดทดสอบ ไม่งั้นคนกดส่งจะนึกว่าฝ่ายขายได้เมลแล้ว
        "team_test": team_test_mode(),
        "team_test_to": team_test_to() if team_test_mode() else [],
    }


def send_html(to, subject: str, html: str, cc=None, attachments=None) -> dict:
    """ยิงเนื้อเมลเข้า flow → {"ok": True, "status": 202, "to": [...], "bytes": n}

    ถือว่าสำเร็จเมื่อได้ 200/201/202 — Power Automate ตอบ 202 Accepted เป็นปกติ
    (งานจริงไปทำต่อฝั่ง flow) จึง "ส่งเข้าคิวสำเร็จ" ไม่เท่ากับ "ผู้รับได้เมลแล้ว" ; ถ้าเมลไม่ถึง
    ต้องไปดู Run history ของ flow ไม่ใช่ log ของเว็บ — ข้อความ error บอกเรื่องนี้ไว้ด้วย

    attachments = [{"name","content_type","data": bytes}] → แปลงเป็นรูปแบบที่ flow ผูกไว้
    ส่ง `mailAttachments` เป็น array เสมอ (ว่างได้) เพื่อให้ flow ผูกช่อง Attachments
    ครั้งเดียวจบ — payload ที่ไม่มีคีย์นี้เลยจะทำให้ expression ฝั่ง flow ได้ null แล้ว run พัง
    """
    url = flow_url()
    if not url:
        raise MailError(status()["reason"])
    to = parse_list(to) or default_to()
    cc = parse_list(cc)
    if not to:
        raise MailError("ยังไม่ได้ระบุผู้รับ (และไม่มีค่าเริ่มต้น KP_MAIL_TO ใน .env)")
    if not str(subject or "").strip():
        raise MailError("หัวข้อเมลว่าง")

    import requests
    from base64 import b64encode
    files = []
    for a in (attachments or []):
        data = a.get("data") or b""
        files.append({
            "Name": a.get("name") or "attachment",
            # ต้องเป็นก้อน binary แบบที่ Logic Apps เข้าใจ ({"$content-type", "$content"}) —
            # ส่ง base64 เปล่า ๆ Power Automate จะ encode ซ้ำอีกชั้น แล้วไฟล์ที่แนบมาเปิดไม่ออก
            "ContentBytes": {
                "$content-type": a.get("content_type") or "application/octet-stream",
                "$content": b64encode(data).decode("ascii"),
            },
        })
    payload = {
        "mailTo": "; ".join(to),
        "mailCc": "; ".join(cc),
        "mailSubject": subject,
        "mailHtml": html,
        "mailAttachments": files,
    }
    if SEND_CARD:
        # การ์ดเล็กที่สุดที่ action "Post card" ยอมรับ — มีไว้ให้ flow เดินผ่านขั้นแรกเท่านั้น
        # จึงเขียนข้อความให้คนในช่องนั้นเข้าใจว่าทำไมมีข้อความนี้โผล่มา
        payload.update({
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard", "version": "1.4",
                    "body": [{"type": "TextBlock", "wrap": True,
                              "text": f"📧 Knit Plan ส่งอีเมลแจ้งแผนเปลี่ยนถึง {'; '.join(to)}"}],
                },
            }],
        })
    try:
        resp = requests.post(url, json=payload, timeout=TIMEOUT)
    except Exception as e:                      # requests โยนได้หลายชนิด (timeout/DNS/proxy)
        raise MailError(f"ต่อ Power Automate ไม่ได้: {e}") from e
    if resp.status_code not in (200, 201, 202):
        raise MailError(f"flow ตอบกลับไม่สำเร็จ (HTTP {resp.status_code}): {resp.text[:300]}")
    return {"ok": True, "status": resp.status_code, "to": to, "cc": cc,
            "bytes": len(html.encode("utf-8")),
            "attachments": [f["Name"] for f in files],
            "total_bytes": len(json_bytes(payload))}


def json_bytes(payload: dict) -> bytes:
    """ขนาด payload จริงที่วิ่งออกไป — ใช้ตรวจว่าเกินเพดานที่ flow รับไหวหรือยัง"""
    import json
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")

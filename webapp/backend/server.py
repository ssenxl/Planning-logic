"""
server.py — FastAPI app
- เสิร์ฟ React build (frontend/dist) ที่ /
- API ที่ /api/*
- เริ่ม APScheduler ตอน startup
รัน: uvicorn server:app --host 0.0.0.0 --port 8080
"""
import asyncio
import json
import queue
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (FileResponse, JSONResponse, RedirectResponse,
                               StreamingResponse)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
import runner
import masters
import database
import workday
import scheduler
import auth
import map_item_view
import mc_hub
import s9_hub
import mc_new
import substitute_view
import plan_view
import plan_history
import plan_lock
import dyed_items
import outsource_advisor
import cyl_approve
import cylinder_advisor
import mc_ava_live
import s9_ava_live
import job_setup_live
import core_stock_live
import yarn_stock_live
import yarn_usage_live
import yarn_ava_live
import yarn_item_lock
import yarn_lock
import yarn_plan
import sc_leadtime
import replan_notify
import mailer
import mail_recipients
import plan_config_api
import livesync
import teams_notify

FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"

# ไฟล์ Excel ที่ดาวน์โหลด "ห้ามแคช" — URL ดาวน์โหลดคงที่ ถ้าเบราว์เซอร์แคชไว้
# ผู้ใช้จะได้ไฟล์เก่าหลังกดบันทึก (FileResponse ส่งแต่ etag/last-modified ไม่มี Cache-Control)
NO_CACHE = {"Cache-Control": "no-store, must-revalidate", "Pragma": "no-cache"}
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _auto_extend_calendar():
    """เติมปฏิทินปีถัดไปลง Calendar.xlsx ของ server อัตโนมัติตอน start (idempotent)
    user ไม่ต้องอัปโหลด/กดเอง — ถ้ามีปีล่วงหน้าครบแล้วจะไม่ทำอะไร ไม่ crash server ถ้าพลาด"""
    try:
        cal = config.master_files().get("Calendar")
        if not cal:
            print("[CALENDAR AUTO-EXTEND] ข้าม: ไม่ได้ตั้ง path calendar ใน config")
            return
        if str(config.REPO_DIR) not in sys.path:
            sys.path.insert(0, str(config.REPO_DIR))
        from gen_calendar import ensure_calendar_extended
        res = ensure_calendar_extended(cal)
        if res.get("added"):
            print(f"[CALENDAR AUTO-EXTEND] เติม {res['added']} วัน → ถึงสิ้นปี {res.get('end_year')} "
                  f"(backup: {res.get('backup')})")
        else:
            print(f"[CALENDAR AUTO-EXTEND] ปฏิทินครบแล้ว ({res.get('reason','')})")
    except Exception as e:
        print(f"[CALENDAR AUTO-EXTEND] ข้ามการเติมปฏิทิน (error: {e})")


def _ensure_workday_years():
    """แถวยุบสัปดาห์/ค่ารายสัปดาห์ในแผงวันทำงานที่ยังไม่มีปี → เขียนปีลงไฟล์ (ครั้งเดียว, idempotent)
    ต้องหลังเติมปฏิทิน (ใช้ปฏิทินหาปี) ; พลาดไม่ crash server — pipeline ยังตีความแถวไม่มีปีได้"""
    try:
        r = workday.ensure_years()
        if r.get("fixed"):
            print(f"[WORKDAY YEARS] ใส่ปีให้แถวเดิม {r['fixed']} แถว (backup: {r.get('backup')})")
        elif not r.get("ok"):
            print(f"[WORKDAY YEARS] ข้าม: {r.get('reason', '')}")
    except Exception as e:
        print(f"[WORKDAY YEARS] ข้ามการใส่ปี (error: {e})")


def _warm_plan_history():
    """สร้าง/อัปเดตทะเบียนประวัติแผนเบื้องหลังตอน start (ไม่ถ่วง startup)

    ครั้งแรกต้องอ่าน snapshot booking ทุกก้อนที่ยังเหลือ (~3 วิ/ไฟล์) ถ้าปล่อยให้ไปเกิดตอน
    user กดดูประวัติย้อนหลัง หน้าจะค้างเป็นนาที ; รอบถัดไปอ่านเฉพาะไฟล์ใหม่จึงเร็ว"""
    def run():
        try:
            r = plan_history.update()
            print(f"[PLAN HISTORY] ทะเบียน {r['rows']} บล็อก จาก {r['files']} snapshot "
                  f"(booking สดเริ่ม W{r['live']['week']}/{r['live']['year']})")
        except Exception as e:
            print(f"[PLAN HISTORY] ข้ามการอัปเดตทะเบียน (error: {e})")
    threading.Thread(target=run, daemon=True).start()


def _yarn_source_key() -> tuple:
    """เวลาไฟล์ที่หน้า 🧶 แผนด้าย / Monitor Yarn › ล็อกด้ายอ่าน + ไฟล์ล็อก + วันที่ — เปลี่ยน = cache ของตัวคิดหมดอายุ"""
    ysc = yarn_plan._ysc()
    plan = plan_view.latest_path()
    files = (plan, ysc.SCFOL_FILE, ysc.PO_FILE, yarn_plan._DEMAND, yarn_stock_live._FILE,
             yarn_ava_live._SPLIT, yarn_ava_live._BOOKING)
    return (str(plan), time.strftime("%Y-%m-%d")) + tuple(yarn_plan._mt(f) for f in files) + yarn_item_lock.state_key()


def _warm_yarn():
    """คิดหน้า 🧶 แผนด้าย + Monitor Yarn › ล็อกด้ายรอไว้เบื้องหลัง ทุกครั้งที่ไฟล์ต้นทางเปลี่ยน
    ไม่งั้นคนแรกที่เปิดหน้าหลังรัน pipeline / ดึงข้อมูลด้าย / deploy ต้องรออ่าน Excel ใหม่ทั้งชุด
    (วัดบน server 16 ต.ค. 2569: ~35 วิ ; คิดรอไว้แล้ว 0.3 วิ) — ทุก 30 วิดูแค่เวลาไฟล์ ไม่เปิดไฟล์ ;
    คิดระหว่าง pipeline รันด้วย (pipeline เป็นอีก process + server 4 CPU ไม่แย่งกัน) อ่านใหม่เฉพาะไฟล์ที่เปลี่ยน
    คู่ SC/SO ของล็อก SC (order_replan ~20 วิ) ไม่อ่านล่วงหน้า — มีล็อก SC เมื่อไหร่ table อ่านเองในรอบนี้"""
    def run():
        last = None
        while True:
            try:
                key = _yarn_source_key()
                if key != last:
                    last = key                 # คิดไม่ผ่านก็ไม่ลองซ้ำทุกรอบ — รอไฟล์เปลี่ยนรอบหน้า
                    t0 = time.time()
                    api_yarn_item_locks()      # ตัวเดียวกับหน้าเว็บ: ด้ายใช้ได้ + แผนด้าย (yarn_plan.table) + ล็อก
                    print(f"[YARN WARM] คิดแผนด้าย + ล็อกด้ายรอไว้ {time.time() - t0:.1f} วิ")
            except Exception as e:
                print(f"[YARN WARM] ข้ามรอบนี้ (error: {e!r})")
            time.sleep(30)
    threading.Thread(target=run, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _auto_extend_calendar()
    _ensure_workday_years()
    _warm_plan_history()
    _warm_yarn()
    scheduler.start()
    yield


app = FastAPI(title="Knit Plan Web", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ---------------- Auth ----------------
# path ที่เข้าได้โดยไม่ต้อง login
# /api/events ตรวจ token เองใน endpoint (EventSource ของเบราว์เซอร์ส่ง header ไม่ได้
# จึงรับ token ทาง query แทน — ไม่ได้แปลว่าเปิดให้เข้าโดยไม่ต้อง login)
AUTH_OPEN_PATHS = {"/api/login", "/api/health", "/api/events"}

# path ที่เฉพาะ admin เท่านั้นถึงเรียกได้ (ตั้งเวลาอัตโนมัติ)
# gate ที่ (method, path) — endpoint อ่านสถานะ/log ยังเปิดให้ทุกคนเพราะหน้าอื่น poll อยู่
# หมายเหตุ: สั่งรัน/หยุด pipeline (/api/run, /api/run/stop) เปิดให้ user ธรรมดารันได้แล้ว
ADMIN_ONLY = {
    ("PUT", "/api/schedule"),
    # ประวัติ Change Cylinder (ใครติ๊กอะไรเมื่อไร) — โชว์บนหน้าหลักที่มีแต่ admin เข้าถึง
    ("GET", "/api/cylinder/log"),
    # ตั้งค่าระบบ (กติกาการวางแผน) — อ่านได้ทุกคน แต่แก้/รีเซ็ต/ดูประวัติเฉพาะ admin
    ("PUT", "/api/plan-config"),
    ("POST", "/api/plan-config/reset"),
    ("GET", "/api/plan-config/history"),
    # รายชื่อผู้รับเมล (master Sales_Mail) — อ่านได้ทุกคน (หน้าต่างส่งเมลใช้) แต่แก้เฉพาะ admin
    ("POST", "/api/mail-recipients"),
    # เพิ่มเครื่องใหม่ = เขียนทะเบียนเครื่อง → ชุดเดียวกับการแก้ข้อมูลหลัก
    ("POST", "/api/mc-new"),
    # แก้ข้อมูลเครื่อง (จำนวน/วันทำงาน/ชั่วโมง) — ชุดเดียวกับการแก้ข้อมูลหลัก
    ("POST", "/api/mc-hub"),
    ("POST", "/api/s9-hub"),
    # ย้ายค่าวันทำงานตามชื่อกลุ่มใหม่ = แก้แผงวันทำงาน → admin เท่านั้น
    ("POST", "/api/mc-new/fix-workday-names"),
    # วันทำงานตามกลุ่มเครื่อง — แผงนี้อยู่ในมุมมองปฏิทินของข้อมูลหลัก จึงล็อกชุดเดียวกัน
    ("PUT", "/api/workday"),
    ("PUT", "/api/workday/merges"),
    # เพิ่มปีในปฏิทิน = เขียน Calendar.xlsx → ชุดเดียวกับการแก้ข้อมูลหลัก
    ("POST", "/api/calendar/extend"),
    ("POST", "/api/workday/seed"),
    # จัดการผู้ใช้ — ด่านนี้กันแค่ "ต้องเป็น admin" ; ใครจัดการบัญชีไหนได้ auth.py ตรวจอีกชั้น
    # (admin จัดการได้แค่ user ธรรมดา, จัดการ admin ได้เฉพาะ superadmin)
    ("GET", "/api/users"),
    ("POST", "/api/users"),
}

# เส้นทางที่ path มีชื่อไฟล์/ชีทต่อท้าย จึงเทียบแบบ exact ไม่ได้ — ใช้ขึ้นต้นแทน
# ข้อมูลหลัก (ชีท Master): อ่านได้ทุกคน (แท็บ Master Data เป็นหน้าดูอย่างเดียว)
# แต่ "เขียน" ทำได้เฉพาะ admin ผ่านหน้า ตั้งค่าระบบ › ข้อมูลหลัก — กันการยิง API ตรงด้วย
ADMIN_ONLY_PREFIX = (
    ("PUT", "/api/masters/"),
    ("POST", "/api/masters/"),        # อ่านไฟล์นำเข้า (ยังไม่บันทึก แต่ใช้เฉพาะหน้าแก้ของ admin)
    # /api/users/{ชื่อ}/password · /role · ลบ
    ("POST", "/api/users/"),
    ("PUT", "/api/users/"),
    ("DELETE", "/api/users/"),
    # Import ทับทะเบียนเครื่องทั้ง 4 ชีท (Master MC / MC_Total / Lock_MC / MC Special) —
    # เขียนข้อมูลหลักเหมือน /api/masters/ จึงต้องล็อกชุดเดียวกัน ; ต้องเป็น prefix ไม่ใช่ (POST,"/api/mc-hub")
    # เพราะ path จริงมีท้าย /import และ /import/check (เทียบแบบตรงตัวจะหลุดทั้งคู่)
    # หน้าเครื่องจักรเปิดให้ทุกคน "ดู" ได้ — ดาวน์โหลด (GET /api/mc-hub/export) จึงไม่ล็อก
    ("POST", "/api/mc-hub/"),
    ("POST", "/api/s9-hub/"),         # เช่นกัน: ตรวจไฟล์ก่อนบันทึกทะเบียนจ้างทอ
    # Import ทับชีท Master_Item V2 (ทะเบียนผ้าที่ใช้แทนกัน) = ข้อมูลหลักเหมือนกัน
    ("POST", "/api/substitute/"),
)

# path ที่ต้องมีสิทธิ์เรื่องด้าย (yarn / admin / superadmin) — user ธรรมดาเรียกไม่ได้
YARN_ONLY = {
    # ค่าของหน้า Monitor Yarn — อ่านได้ทุกคน (หน้า Stock Yarn ใช้ลงสี) แต่แก้เฉพาะ yarn/admin
    ("PUT", "/api/yarn-monitor"),
    # ล็อกด้าย lot ให้ SC + ผ้า (หน้าแผนด้าย) — ดูได้ทุกคน ล็อกเฉพาะ yarn/admin
    ("POST", "/api/yarn-locks"),
    # ล็อกด้ายให้ Item ผ้า (หน้า Monitor Yarn › ล็อกด้าย)
    ("POST", "/api/yarn-item-locks"),
}
# ปลดล็อกด้าย — path มีเลขล็อกต่อท้าย เทียบตรงตัวไม่ได้ (บทเรียน webapp_permission_gate) ใช้ขึ้นต้นแทน
YARN_ONLY_PREFIX = (
    ("DELETE", "/api/yarn-locks/"),
    ("DELETE", "/api/yarn-item-locks/"),
)


def _is_download_path(path: str) -> bool:
    """endpoint ดาวน์โหลดไฟล์ — เปิดให้โหลดได้โดยไม่ต้องมี token
    (ใช้ลิงก์ <a href> โหลดตรงได้ทุกเมื่อ); ไม่รวม /api/outputs/booking ที่เป็น list"""
    if path == "/api/database/download":
        return True
    if path == "/api/map-item/download":
        return True
    if path == "/api/plan/download":
        return True
    if path.startswith("/api/outputs/") and path != "/api/outputs/booking":
        return True
    return False


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    path = request.url.path
    # endpoint ดาวน์โหลดเปิดเฉพาะ GET เท่านั้น — method อื่น (เช่น DELETE) ยังต้อง login
    is_download = _is_download_path(path) and request.method == "GET"
    # ลิงก์ดู Machine Availability แบบไม่ต้อง login (การ์ด Teams) — เปิดเฉพาะ GET ใต้ /api/share/
    # ซึ่งมีแค่ของหน้านั้น และทุกตัวตรวจรหัส k ในลิงก์เอง (_share_guard) ; ไม่ใช่ทางเข้าหน้าอื่น
    is_share = path.startswith("/api/share/") and request.method == "GET"
    # ป้องกันเฉพาะ /api/* (ยกเว้น path ที่เปิดไว้ + ดาวน์โหลด GET); preflight OPTIONS ปล่อยผ่าน
    if path.startswith("/api/") and path not in AUTH_OPEN_PATHS \
            and not is_download and not is_share \
            and request.method != "OPTIONS":
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else ""
        username = auth.check_token(token)
        if not username:
            return JSONResponse(status_code=401, content={"detail": "ยังไม่ได้เข้าสู่ระบบ หรือเซสชันหมดอายุ"})
        # endpoint สำหรับ admin เท่านั้น (สั่งรัน/ตั้งเวลา) — user ธรรมดาเรียกไม่ได้
        admin_only = (request.method, path) in ADMIN_ONLY or any(
            request.method == m and path.startswith(pfx) for m, pfx in ADMIN_ONLY_PREFIX)
        if admin_only and not auth.is_admin(username):
            return JSONResponse(status_code=403, content={"detail": "ต้องเป็นสิทธิ์ Knit หรือ Super Admin เท่านั้น"})
        yarn_only = (request.method, path) in YARN_ONLY or any(
            request.method == m and path.startswith(pfx) for m, pfx in YARN_ONLY_PREFIX)
        if yarn_only and not auth.can_yarn(username):
            return JSONResponse(status_code=403, content={"detail": "ต้องมีสิทธิ์ Yarn, Knit หรือ Super Admin เท่านั้น"})
    return await call_next(request)


class LoginReq(BaseModel):
    username: str
    password: str


@app.post("/api/login")
def api_login(req: LoginReq):
    if not auth.verify_login(req.username, req.password):
        raise HTTPException(status_code=401, detail="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")
    return {
        "token": auth.make_token(req.username),
        "username": req.username,
        "role": auth.get_role(req.username),
    }


@app.get("/api/me")
def api_me(request: Request):
    """คืนข้อมูล user ปัจจุบันจาก token — frontend ใช้เช็ค role หลังรีเฟรชหน้า"""
    header = request.headers.get("authorization", "")
    token = header[7:] if header.lower().startswith("bearer ") else ""
    username = auth.check_token(token)  # middleware ผ่านแล้ว แต่กันไว้อีกชั้น
    if not username:
        raise HTTPException(status_code=401, detail="เซสชันหมดอายุ")
    return {"username": username, "role": auth.get_role(username)}


class ChangeOwnPwReq(BaseModel):
    old_password: str
    new_password: str


@app.post("/api/me/password")
def api_me_password(req: ChangeOwnPwReq, request: Request):
    """เปลี่ยนรหัสตัวเองได้เฉพาะ superadmin (ต้องกรอกรหัสเดิม; auth.py ตรวจสิทธิ์)
    — คืน token ใบใหม่เพราะใบเดิมตกทันที"""
    try:
        return auth.change_own_password(_req_user(request), req.old_password, req.new_password)
    except auth.UserError as e:
        raise HTTPException(status_code=e.status, detail=str(e))


# ---------------- ผู้ใช้งาน (ตั้งค่าระบบ › ผู้ใช้งาน — admin) ----------------
class UserCreateReq(BaseModel):
    username: str
    password: str
    role: str = "user"


class UserPwReq(BaseModel):
    password: str


class UserRoleReq(BaseModel):
    role: str


def _user_call(fn, *args):
    try:
        return fn(*args)
    except auth.UserError as e:
        raise HTTPException(status_code=e.status, detail=str(e))


@app.get("/api/users")
def api_users(request: Request):
    me = _req_user(request)
    return {"users": auth.list_users(), "me": me, "my_role": auth.get_role(me)}


@app.post("/api/users")
def api_user_create(req: UserCreateReq, request: Request):
    return _user_call(auth.create_user, _req_user(request), req.username, req.password, req.role)


@app.post("/api/users/{username}/password")
def api_user_reset_pw(username: str, req: UserPwReq, request: Request):
    return _user_call(auth.reset_password, _req_user(request), username, req.password)


@app.put("/api/users/{username}/role")
def api_user_role(username: str, req: UserRoleReq, request: Request):
    return _user_call(auth.set_role, _req_user(request), username, req.role)


@app.delete("/api/users/{username}")
def api_user_delete(username: str, request: Request):
    return _user_call(auth.delete_user, _req_user(request), username)


# ---------------- Run / Pipeline ----------------
class RunReq(BaseModel):
    mode: str  # full | db | plan


@app.post("/api/run")
def api_run(req: RunReq):
    return runner.start_run(req.mode, trigger="manual")


@app.post("/api/run/stop")
def api_run_stop():
    return runner.stop_run()


@app.get("/api/run/status")
def api_run_status():
    return runner.get_status()


@app.get("/api/run/logs")
def api_run_logs(offset: int = 0):
    return runner.get_logs(offset)


# ---------------- Masters ----------------
@app.get("/api/masters")
def api_masters():
    return masters.list_masters()


@app.get("/api/masters/{name}/{sheet}")
def api_master_sheet(name: str, sheet: str):
    try:
        d = masters.read_sheet(name, sheet)
        # เลขรุ่น ณ ตอนอ่าน — หน้าเว็บส่งกลับมาตอนเซฟ ใช้ดูว่ามีใครแก้แซงระหว่างนั้นไหม
        d["rev"] = livesync.rev(livesync.masters_res(name, sheet))
        return d
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveReq(BaseModel):
    columns: list
    rows: list
    # เลขรุ่นที่หน้าเว็บโหลดไป — ไม่ตรงกับของจริง = มีคนแก้แซงระหว่างนั้น (ตอบ 409)
    base_rev: int | None = None


@app.post("/api/masters/{name}/{sheet}/import-preview")
async def api_master_import_preview(name: str, sheet: str, file: UploadFile = File(...)):
    """อัปโหลด .xlsx มานำเข้าทับชีท → คืนหัวคอลัมน์ + แถวให้หน้าเว็บตรวจก่อน (ยังไม่บันทึก)
    กดยืนยันแล้วหน้าเว็บใส่ลงตารางและเซฟผ่าน PUT ตามปกติ (เข้าประวัติ + ย้อนกลับได้)"""
    try:
        return masters.parse_upload(await file.read(), sheet, name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/masters/{name}/{sheet}/export")
def api_master_export(name: str, sheet: str):
    """Export ชีทเดียวเป็นไฟล์ .xlsx — หัวคอลัมน์/ชื่อชีทตามของจริง จึง Import กลับได้ทันที"""
    import io
    from urllib.parse import quote
    from openpyxl import Workbook
    from fastapi import Response
    try:
        d = masters.read_sheet(name, sheet)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    wb = Workbook()
    ws = wb.active
    ws.title = sheet[:31]                   # Excel จำกัดชื่อชีท 31 ตัว
    ws.append([str(c) for c in d["columns"]])
    for r in d["rows"]:
        ws.append([None if v == "" else v for v in r])
    buf = io.BytesIO()
    wb.save(buf)
    return Response(
        content=buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=\"export.xlsx\"; "
                 f"filename*=UTF-8''{quote(sheet + '.xlsx')}"},
    )


@app.put("/api/masters/{name}/{sheet}")
def api_master_save(name: str, sheet: str, req: SaveReq, request: Request):
    try:
        return masters.write_sheet(name, sheet, req.columns, req.rows,
                                   user=_req_user(request), base_rev=req.base_rev)
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Work Day (วันทำงานตามกลุ่มเครื่อง + ยุบสัปดาห์) ----------------
@app.get("/api/workday")
def api_workday_get():
    try:
        d = workday.get_workday()
        d["rev"] = livesync.rev(livesync.WORKDAY)
        return d
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class WorkDayReq(BaseModel):
    defaults: dict          # {"FACTORY|MC_CAT|GUAGE": วัน}
    weeks: dict = {}        # {"FACTORY|MC_CAT|GUAGE|YW": วัน}   YW = ปี*100+สัปดาห์
    hours: dict = {}        # {"FACTORY|MC_CAT|GUAGE": ชั่วโมง} (ค่ามาตรฐานต่อกลุ่ม)
    merges: dict = {}       # {"202631": 202632}
    base_rev: int | None = None


@app.put("/api/workday")
def api_workday_put(req: WorkDayReq, request: Request):
    try:
        # แผงนี้เป็นที่เดียวที่แก้ "วันรายสัปดาห์" ได้ → บังคับด่านสัปดาห์ที่ยุบรวมมาตรงนี้
        return workday.save_workday(req.defaults, req.weeks, req.hours, req.merges,
                                    user=_req_user(request), base_rev=req.base_rev,
                                    enforce_merge_days=True)
    except workday.MergeDayGap as e:
        raise HTTPException(status_code=422, detail=str(e))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class WeekMergeReq(BaseModel):
    merges: dict = {}       # {"202631": 202632}   YW = ปี*100+สัปดาห์
    base_rev: int | None = None


@app.put("/api/workday/merges")
def api_workday_merges(req: WeekMergeReq, request: Request):
    """ยุบสัปดาห์ (แผงในหน้าปฏิทิน) — บันทึกเฉพาะการยุบ ; วัน/ชั่วโมงของกองแก้ที่หน้าเครื่องจักร"""
    try:
        return workday.save_merges(req.merges, user=_req_user(request), base_rev=req.base_rev)
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class CalendarExtendReq(BaseModel):
    year: int


@app.post("/api/calendar/extend")
def api_calendar_extend(req: CalendarExtendReq, request: Request):
    """หน้าปฏิทิน › เพิ่มปี — เติม Calendar.xlsx ให้ถึงสิ้นปีที่ขอ (ไม่จำกัดปี, ทีละปีจากหน้าเว็บ)"""
    try:
        return workday.extend_calendar(req.year, user=_req_user(request))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/workday/seed")
def api_workday_seed(request: Request):
    """สร้างชีท Work Day ครั้งแรกจาก Working Day ของ MasterMC (ไม่ทับค่าที่ตั้งไว้แล้ว)"""
    try:
        return workday.seed_from_mastermc(user=_req_user(request))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Schedule ----------------
@app.get("/api/schedule")
def api_schedule_get():
    return scheduler.status()


class ScheduleReq(BaseModel):
    runs: list  # [{id, enabled, hour, minute, mode: full|db|plan, days: [0=จ. … 6=อา.]}]


@app.put("/api/schedule")
def api_schedule_put(req: ScheduleReq, request: Request):
    res = scheduler.save_runs(req.runs, user=_req_user(request))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=" · ".join(res.get("errors", ["บันทึกไม่สำเร็จ"])))
    return res


# ---------------- ตั้งค่าระบบ (กติกาการวางแผน) ----------------
def _req_user(request: Request) -> str:
    """ชื่อผู้ใช้จาก token (middleware ตรวจสิทธิ์มาแล้ว)

    ใช้ทุกที่ที่ต้องรู้ว่า "ใครเป็นคนแก้": คอลัมน์ LOCKED_BY, ประวัติการตั้งค่า และ
    ข่าวที่ยิงไปบอกจออื่นว่าใครเพิ่งแก้อะไร
    """
    header = request.headers.get("authorization", "")
    token = header[7:] if header.lower().startswith("bearer ") else ""
    return auth.check_token(token) or ""


class PlanConfigReq(BaseModel):
    values: dict


class PlanConfigResetReq(BaseModel):
    keys: list = []   # ว่าง = คืนค่าเริ่มต้นทั้งหมด


@app.get("/api/plan-config")
def api_plan_config_get():
    return plan_config_api.read()


@app.put("/api/plan-config")
def api_plan_config_put(req: PlanConfigReq, request: Request):
    res = plan_config_api.write(req.values, user=_req_user(request))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=" · ".join(res.get("errors", ["บันทึกไม่สำเร็จ"])))
    return res


@app.post("/api/plan-config/reset")
def api_plan_config_reset(req: PlanConfigResetReq, request: Request):
    res = plan_config_api.reset(req.keys or None, user=_req_user(request))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=" · ".join(res.get("errors", ["รีเซ็ตไม่สำเร็จ"])))
    return res


# ---------------- Monitor Yarn (ค่าที่หน้า Stock Yarn ใช้ตอนแสดงผล เช่น YARN_WEIGHT ขั้นต่ำ) ----------------
# เก็บในทะเบียนเดียวกับ plan-config (ได้ตรวจค่า/ประวัติ/แจ้งจออื่นมาด้วย) แต่บันทึกเฉพาะกลุ่มของตัวเอง
@app.get("/api/yarn-monitor")
def api_yarn_monitor_get(catalog: bool = False):
    """catalog=1 → แนบรายชื่อ BUYYER / ITEM / ใครถือ ITEM-LOT ไหน ไปด้วย (หน้า Monitor Yarn ใช้ตั้งกติกา)
    หน้า Stock Yarn ไม่ต้องขอ (มีแถวทั้งไฟล์อยู่แล้ว) — ไม่งั้นต้องโหลดรายชื่อซ้ำทุกครั้งที่มีคนบันทึกค่า"""
    out = plan_config_api.read_group(plan_config_api.YARN_GROUP)
    if catalog:
        # นับเฉพาะแถวที่หน้า Stock Yarn แสดง (ไม่นับ PO ที่ตั้งให้ซ่อน)
        out.update(yarn_stock_live.catalog(yarn_stock_live.hidden_suffixes(out["values"])))
    return out


@app.put("/api/yarn-monitor")
def api_yarn_monitor_put(req: PlanConfigReq, request: Request):
    res = plan_config_api.write_group(plan_config_api.YARN_GROUP, req.values, user=_req_user(request))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=" · ".join(res.get("errors", ["บันทึกไม่สำเร็จ"])))
    return res


@app.get("/api/plan-config/history")
def api_plan_config_history(request: Request, offset: int = 0, limit: int = 100):
    """ประวัติทุกอย่างที่แก้บนหน้า "ตั้งค่าระบบ" รวมกัน ใหม่สุดขึ้นก่อน — เฉพาะ superadmin
      • ค่าตั้งค่า (plan_config_log.jsonl) → {kind:"config", at, by, changed:[...]}
      • ข้อมูลหลัก + วันทำงาน + ตั้งเวลารันอัตโนมัติ (edit_log.jsonl) → {kind:"data", at, by, resource, changes:[...]}
    งานบนหน้าอื่น (ลาก Gantt, ล็อกแผน, ติ๊กสูบ ฯลฯ) ไม่เอามาปน ; ค่าตั้งค่าใน edit_log ก็ข้าม
    เพราะ plan_config_log จดละเอียดกว่าอยู่แล้ว

    ทั้งสองไฟล์ต่อท้ายอย่างเดียว ไม่เคยตัด (user สั่ง 24 ก.ย. 2569: เก็บตลอดไป) → อ่านทั้งไฟล์
    แล้วแบ่งหน้าด้วย offset/limit ; คืน {items, total}"""
    if not auth.is_superadmin(_req_user(request)):
        raise HTTPException(status_code=403, detail="ประวัติการแก้ไขดูได้เฉพาะ Super Admin")
    offset = max(0, int(offset or 0))
    limit = max(1, min(int(limit or 100), 1000))
    out = [{"kind": "config", **h} for h in plan_config_api.history(0)]
    for e in livesync.read_audit(0):
        res = str(e.get("resource") or "")
        if not (res.startswith("masters/") or res in (livesync.WORKDAY, livesync.SCHEDULE)):
            continue
        out.append({
            "kind": "data",
            "at": e.get("at"),
            "by": e.get("by") or "",
            "resource": res,
            "detail": e.get("detail") or "",
            "changes": e.get("changes"),
            "changes_total": e.get("changes_total"),
        })
    out.sort(key=lambda h: str(h.get("at") or ""), reverse=True)
    return {"items": out[offset:offset + limit], "total": len(out)}


# ---------------- Dashboard › Machine Availability (สดจาก booking) ----------------
@app.get("/api/mc-ava")
def api_mc_ava():
    """ตาราง Machine Availability กลุ่มเครื่อง × สัปดาห์ จาก booking (logic เดียวกับ AVA_MC)"""
    return mc_ava_live.table()


@app.post("/api/mc-ava/refresh")
def api_mc_ava_refresh():
    """สั่งคำนวณใหม่จาก Booking/view_booking.xlsx ล่าสุด (ทำ background)"""
    return mc_ava_live.start_refresh()


@app.get("/api/mc-ava/status")
def api_mc_ava_status():
    return mc_ava_live.status()


@app.get("/api/mc-ava/detail")
def api_mc_ava_detail(key: str, week: str):
    """รายละเอียดของช่องที่คลิกบน heatmap — เครื่องย่อยในกลุ่ม + รายการงาน booking"""
    return mc_ava_live.detail(key, week)


# ---------------- ลิงก์ดู Machine Availability แบบไม่ต้อง login (ปุ่มบนการ์ด Teams) ----------------
# การ์ดส่งเองทุกจันทร์ 08:00 (scheduler.py → teams_notify.weekly_job) — ไม่มีปุ่มส่งบนหน้าเว็บ (user สั่ง)
# เรียกฟังก์ชันเดียวกับ /api/mc-ava* ทุกตัว → ตัวเลขตรงกับหน้าที่ login เสมอ ; ดูได้อย่างเดียว
# (สั่งคำนวณใหม่ไม่ได้ — แต่ table() ยังสั่งคำนวณเองเมื่อ booking ใหม่กว่า เหมือนเปิดหน้าปกติ)
def _share_guard(k: str) -> None:
    if not teams_notify.check_share_key(k):
        raise HTTPException(status_code=404, detail="ลิงก์ไม่ถูกต้อง — เปิดจากปุ่มบนการ์ดล่าสุดใน Teams")


@app.get("/api/share/mc-ava")
def api_share_mc_ava(k: str = ""):
    """ตารางเดียวกับ /api/mc-ava แต่ตัดเหลือสัปดาห์ที่ทำแผนได้ (≥ week+KP_FREEZE_WEEKS) — ตัวเดียวกับการ์ด
    (หน้าที่ login ยังเห็นย้อนหลังครบ — user สั่ง 15 ต.ค.)"""
    _share_guard(k)
    return teams_notify.plannable(mc_ava_live.table())


@app.get("/api/share/mc-ava/status")
def api_share_mc_ava_status(k: str = ""):
    _share_guard(k)
    return mc_ava_live.status()


@app.get("/api/share/mc-ava/detail")
def api_share_mc_ava_detail(key: str, week: str, k: str = ""):
    _share_guard(k)
    return mc_ava_live.detail(key, week)


# ---------------- Dashboard › Outsource Availability (เครื่องผู้รับจ้างทอ จากแผนล่าสุด) ----------------
@app.get("/api/s9-ava")
def api_s9_ava():
    """เครื่องผู้รับจ้าง (ชีท MC S9) × สัปดาห์ เทียบงานจ้างทอในแผนล่าสุด + รายการงานทุกช่อง
    (งานจ้างทอมีไม่กี่ร้อยแถว ส่งมาทีเดียวให้หน้าเว็บกรองเองตอนคลิกช่อง ไม่ต้องยิง detail แยก)"""
    return s9_ava_live.table()


# ---------------- Dashboard โควตา Job Setup (สดจาก booking ใบเดียวกับหน้า Machine Availability) ----------
@app.get("/api/job-setup")
def api_job_setup():
    """โควตา Job Setup ที่ booking ใช้ไปแล้ว ต่อ (ประเภทโรงงาน × สัปดาห์) + ราย job ทั้งหมด
    ปุ่มคำนวณใหม่/สถานะใช้ร่วมกับ /api/mc-ava/refresh เพราะอ่านไฟล์ผลลัพธ์ใบเดียวกัน"""
    return job_setup_live.table()


# ---------------- Dashboard › Item Core Stock (สต๊อกผ้าดิบ สดจากไฟล์ Stock) ----------
@app.get("/api/core-stock")
def api_core_stock(mode: str = "core"):
    """สต๊อกผ้าดิบต่อ item + สถานะเทียบ STOCK MIN/MAX
    mode=core → เฉพาะทะเบียน Core Greige (Target_Stock) · mode=all → ทุก item ที่มีของในคลัง
    โหมด core แถม totals = แถวสรุป "รวมทุกทีม core" ของผ้าที่ตั้งเป้าไว้หลายทีม"""
    try:
        return core_stock_live.table(mode)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Dashboard › Stock Yarn (ด้ายต่อ ITEM → LOT → PO จากไฟล์ที่ View_Yarn_Aging ดึงไว้) ----------
@app.get("/api/yarn-stock")
def api_yarn_stock():
    """ใบ (ITEM × LOT × PO × YARN_TYPE) แบบแบน: kg รวม, ลูกรวม, ช่วง kg/ลูก ของแถวดิบ
    หน้าเว็บประกอบเป็นต้นไม้ 3 ชั้นเอง — กรองแล้วคิดยอดใหม่ฝั่ง client ไม่ต้องยิงซ้ำ"""
    try:
        return yarn_stock_live.table()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Dashboard › Yarn Usage (ผ้าแต่ละ ITEM ใช้ด้าย/lot ไหนไปแล้วเท่าไหร่ — booking × view ด้ายต่อใบ KP) ----------
@app.get("/api/yarn-usage")
def api_yarn_usage():
    """แถว (ITEM ผ้า × ด้าย × lot): ต้องใช้ทั้งใบ / ใช้ไปแล้ว / เหลือต้องใช้ (kg) รวมทุกใบ KP ใน booking
    ยอดจาก booking คูณ % ด้ายของใบ KP — หน้าเว็บจัดวาง ITEM → ด้าย → LOT เอง (จัดกลุ่มอย่างเดียว)"""
    try:
        return yarn_usage_live.table()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- ด้าย/lot ไหนยังใช้ได้อีกเท่าไหร่ (yarn_ava_live — สต๊อกด้าย − ลูกเล็ก − ใบ KP ที่จองไว้แต่ยังไม่โอน − ล็อก) ----------
# หน้า Dashboard › Yarn Ava + /api/yarn-ava ถอดออก 15 ต.ค. 2569 (user ไม่ใช้แล้ว) ; ตัวคิดยังอยู่ — ล็อกด้าย / แผนด้ายเรียกผ่านตัวนี้
def _yarn_hide() -> tuple:
    return yarn_stock_live.hidden_suffixes(plan_config_api.read_group(plan_config_api.YARN_GROUP)["values"])


def _yarn_ava_table() -> dict:
    """ตารางยอดด้ายใช้ได้ด้วยค่าปัจจุบันของหน้า Monitor Yarn (ที่ใช้ตรวจตอนล็อกด้าย + แผนด้าย)"""
    values = plan_config_api.read_group(plan_config_api.YARN_GROUP)["values"]
    try:
        cone_min = float(values.get("KP_YARN_WEIGHT_MIN"))
    except (TypeError, ValueError):
        cone_min = yarn_ava_live._CONE_MIN_DEFAULT
    return yarn_ava_live.table(yarn_stock_live.hidden_suffixes(values), cone_min,
                               values.get("KP_YARN_CUSTOMER_LOTS") or [])


# ---------------- ล็อกด้าย lot ให้ SC + ผ้า (ดูหัวไฟล์ yarn_lock.py) ----------------
# ล็อก/ปลดที่หน้าแผนด้าย เฉพาะสิทธิ์ yarn / admin (YARN_ONLY) ;
# อ่านได้ทุกคน (หน้าแผนด้าย / Gantt ป้าย "ด้ายล็อก" ในการ์ดงาน)
class YarnLockReq(BaseModel):
    yarn: str
    lot: str
    sc: str
    fabric: str
    fabric_kg: float
    note: str = ""


@app.get("/api/yarn-locks")
def api_yarn_locks():
    """ล็อกทุกตัว + สถานะ (ยังกันอยู่ / ออก KP ครบ / booking ปิด) + ยอดที่ lot กันได้จริง + ประวัติล่าสุด"""
    try:
        return yarn_lock.listing(_yarn_ava_table())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/yarn-locks")
def api_yarn_lock_add(req: YarnLockReq, request: Request):
    try:
        return yarn_lock.add(_req_user(request), req.yarn, req.lot, req.sc, req.fabric, req.fabric_kg, req.note,
                             _yarn_ava_table, _yarn_hide())
    except yarn_lock.LockError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/yarn-locks/{lock_id}")
def api_yarn_lock_remove(lock_id: str, request: Request):
    try:
        return yarn_lock.remove(_req_user(request), lock_id)
    except yarn_lock.LockError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------- ล็อกด้ายให้ Item ผ้า + หน้า Monitor Yarn › 🔒 ล็อกด้าย (ดูหัวไฟล์ yarn_item_lock.py) ----------------
# หน้านี้แสดงล็อกทั้ง 2 แบบ (Item / SC) ; ล็อก/ปลดเฉพาะสิทธิ์ yarn / admin (YARN_ONLY)
class YarnItemLockReq(BaseModel):
    yarn: str
    lot: str = ""          # ว่าง = ทุก lot (lot ที่ด้ายเข้าก่อนก่อน)
    color: str = ""        # ด้าย TD ที่ไม่ระบุ lot — สีที่จะกัน
    fabric: str
    yarn_kg: float
    note: str = ""


@app.get("/api/yarn-item-locks")
def api_yarn_item_locks():
    """ล็อก Item + ล็อก SC ทุกตัว: สถานะ / ยอดที่สต๊อกกันได้จริง / แผนล่าสุดใช้ไปสัปดาห์ไหนเท่าไหร่ + ประวัติ + รายชื่อด้าย"""
    try:
        t = _yarn_ava_table()
        try:
            use = yarn_plan.lock_usage()
        except Exception as e:             # แผนอ่านไม่ได้ ยังดูล็อกได้ แค่ไม่มีคอลัมน์ "แผนใช้"
            use = {"note": f"คิดยอดที่แผนใช้ไม่ได้ ({e})"}
        return yarn_item_lock.monitor(t, yarn_lock.listing(t), use)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/yarn-item-locks/options")
def api_yarn_item_lock_options(yarn: str, lot: str = "", color: str = ""):
    """ตัวเลือกตอนล็อก: lot / สีของด้ายนี้ + ล็อกได้อีกเท่าไหร่ + Item ผ้าที่ใช้ด้ายนี้ (พร้อมยอดที่ใบค้างยังต้องใช้)"""
    try:
        return yarn_item_lock.options(yarn, lot, color, _yarn_ava_table(), _yarn_hide())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/yarn-item-locks")
def api_yarn_item_lock_add(req: YarnItemLockReq, request: Request):
    try:
        return yarn_item_lock.add(_req_user(request), req.yarn, req.lot, req.color, req.fabric, req.yarn_kg, req.note,
                                  _yarn_ava_table, _yarn_hide())
    except yarn_item_lock.LockError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/yarn-item-locks/{lock_id}")
def api_yarn_item_lock_remove(lock_id: str, request: Request):
    try:
        return yarn_item_lock.remove(_req_user(request), lock_id)
    except yarn_item_lock.LockError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------- หน้า 🧶 แผนด้าย (แผนทอใช้ด้ายอะไร สัปดาห์ไหน เท่าไหร่ — ดูหัวไฟล์ yarn_plan.py) ----------
@app.get("/api/yarn-plan")
def api_yarn_plan(sheet: str = None):
    """ด้าย × สัปดาห์ ของชีทแผนที่เปิดอยู่: ใช้ / มาจากสต๊อก · ล็อก · PO ของใบ · ออก KP แล้ว / คงเหลือ + รายใบต่อช่อง"""
    try:
        return yarn_plan.table(sheet)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/yarn-plan/lots")
def api_yarn_plan_lots(yarn: str, so: str, fabric: str, color: str = ""):
    """lot ที่ล็อกให้ SC + ผ้านี้ได้ (ด้าย + สีตรง มียอดใช้ได้) + % ด้ายที่ใช้แปลง kg ผ้า → kg ด้าย"""
    try:
        return yarn_plan.lots(_yarn_ava_table(), yarn, color, so, fabric, _yarn_hide())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Dashboard › SC Lead Time (วันเปิดใบ → เป้าทอ ห่างกี่สัปดาห์) ----------
@app.get("/api/sc-leadtime")
def api_sc_leadtime():
    """ทุกใบในทะเบียน SC สะสม พร้อมระยะห่าง "วันเปิดใบ → TARGET_KNIT" เป็นสัปดาห์
    ส่งแถวดิบทั้งหมด — หน้าเว็บให้ผู้ใช้ปรับช่วงแบ่งกลุ่มเองแล้วคิด KPI/กราฟใหม่ฝั่ง client"""
    try:
        return sc_leadtime.summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sc-leadtime/backfill")
def api_sc_leadtime_backfill():
    """รวม snapshot view_sc ที่ยังไม่เคยเข้าทะเบียนสะสม (ปุ่ม "อัปเดตทะเบียน" บนหน้า)
    ปกติ View_SC เขียนทะเบียนให้เองทุกครั้งที่รัน — ปุ่มนี้ไว้ใช้ตอนเพิ่ง deploy
    หรือหลังกู้ไฟล์ ; ไฟล์ที่เคยรวมแล้วถูกข้ามอัตโนมัติ กดซ้ำได้ไม่ทำให้ตัวเลขเฟ้อ"""
    try:
        return sc_leadtime.backfill()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Outputs ----------------
@app.get("/api/outputs")
def api_outputs():
    out = []
    if config.OUTPUT_DIR.exists():
        for f in sorted(config.OUTPUT_DIR.glob("production_plan_*.xlsx"),
                        key=lambda p: p.stat().st_mtime, reverse=True):
            st = f.stat()
            out.append({"name": f.name, "size": st.st_size,
                        "mtime": st.st_mtime, "kind": "plan"})
    return out


@app.get("/api/outputs/booking")
def api_outputs_booking():
    """ไฟล์ booking_final (ที่อัปโหลดขึ้น SharePoint เป็น booking_final_auto.xlsx)"""
    out = []
    if config.OUTPUT_DIR.exists():
        for f in sorted(config.OUTPUT_DIR.glob("booking_final_ready_*.xlsx"),
                        key=lambda p: p.stat().st_mtime, reverse=True):
            st = f.stat()
            out.append({"name": f.name, "size": st.st_size,
                        "mtime": st.st_mtime, "kind": "booking"})
    return out


@app.get("/api/outputs/sc")
def api_outputs_sc():
    """ไฟล์ข้อมูล SC (view_sc_*.xlsx) ที่ View_SC.py เก็บติด timestamp ไว้ทุกครั้งที่รัน"""
    out = []
    if config.OUTPUT_DIR.exists():
        for f in sorted(config.OUTPUT_DIR.glob("view_sc_*.xlsx"),
                        key=lambda p: p.stat().st_mtime, reverse=True):
            st = f.stat()
            out.append({"name": f.name, "size": st.st_size,
                        "mtime": st.st_mtime, "kind": "sc"})
    return out


@app.get("/api/outputs/{fname}")
def api_output_download(fname: str):
    f = config.OUTPUT_DIR / fname
    if not f.exists() or f.parent != config.OUTPUT_DIR:
        raise HTTPException(status_code=404, detail="ไม่พบไฟล์")
    return FileResponse(str(f), filename=fname, media_type=XLSX_MIME, headers=NO_CACHE)


@app.delete("/api/outputs/{fname}")
def api_output_delete(fname: str):
    f = config.OUTPUT_DIR / fname
    if not f.exists() or f.parent != config.OUTPUT_DIR:
        raise HTTPException(status_code=404, detail="ไม่พบไฟล์")
    try:
        f.unlink()
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"ลบไม่ได้: {e}")
    return {"ok": True, "deleted": fname}


# ---------------- Database (ดูไฟล์ข้อมูลในโปรเจกต์ read-only) ----------------
@app.get("/api/database")
def api_database():
    return database.list_datasets()


@app.get("/api/database/sheet")
def api_database_sheet(file: str, sheet: str = None):
    try:
        return database.read_grid(file, sheet)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/database/download")
def api_database_download(file: str):
    try:
        p = database._resolve(file)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    return FileResponse(str(p), filename=p.name,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ---------------- Map Item (ผลลัพธ์ Datamining ↔ Booking) ----------------
@app.get("/api/map-item")
def api_map_item():
    return map_item_view.list_files()


@app.get("/api/map-item/sheet")
def api_map_item_sheet(file: str, sheet: str = None):
    try:
        return map_item_view.read_grid(file, sheet)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/map-item/download")
def api_map_item_download(file: str):
    try:
        p = map_item_view._resolve(file)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    return FileResponse(str(p), filename=p.name, media_type=XLSX_MIME, headers=NO_CACHE)


# ---------------- เพิ่มเครื่องใหม่ + ไฟตรวจความพร้อม ----------------
@app.get("/api/mc-new/check")
def api_mc_new_check(factory: str = "", mc_cat: str = "", mc: str = "", guage: str = ""):
    try:
        return mc_new.check(factory, mc_cat, mc, guage)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- เครื่องจักร (หน้าเดียวจบ: จำนวน/วันทำงาน/ชั่วโมง/กันเครื่อง) ----------------
@app.get("/api/mc-hub")
def api_mc_hub():
    try:
        return mc_hub.get()
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mc-hub")
async def api_mc_hub_save(req: Request):
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="รูปแบบข้อมูลไม่ถูกต้อง")
    try:
        res = mc_hub.save(body.get("changes") or {}, body.get("revs") or {},
                          user=_req_user(req))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "บันทึกไม่สำเร็จ"))
    return res


@app.get("/api/mc-hub/export")
def api_mc_hub_export():
    """Export 4 ชีทของหน้าเครื่องจักร (Master MC / MC_Total / Lock_MC / MC Special) เป็นไฟล์เดียว"""
    from fastapi import Response
    try:
        body = mc_hub.export_book()
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    return Response(
        content=body,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="MasterMC_machines.xlsx"'},
    )


@app.post("/api/mc-hub/import/check")
async def api_mc_hub_import_check(file: UploadFile = File(...)):
    """ตรวจไฟล์ที่จะ Import ทีละชีท (ยังไม่เขียน) — คืนจำนวนเดิม/ใหม่ + คำเตือน + เลขรุ่น"""
    try:
        res = mc_hub.import_check(await file.read())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # ไม่ต้องส่งแถวทั้งหมดกลับไปหน้าเว็บ — ตอนยืนยันส่งไฟล์เดิมมาตรวจซ้ำอีกรอบ
    for s in res["sheets"]:
        s.pop("rows", None)
        s.pop("columns", None)
    return res


@app.post("/api/mc-hub/import")
async def api_mc_hub_import(request: Request, file: UploadFile = File(...), revs: str = Form("{}")):
    """Import ทับชีทที่มีในไฟล์ — ตรวจซ้ำก่อนเขียน ; revs = {ชื่อชีท: เลขรุ่นจากขั้นตรวจ}"""
    try:
        res = mc_hub.import_apply(await file.read(), json.loads(revs or "{}"),
                                  user=_req_user(request))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "Import ไม่สำเร็จ"))
    return res


@app.get("/api/s9-hub")
def api_s9_hub():
    try:
        return s9_hub.get()
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/s9-hub")
async def api_s9_hub_save(req: Request):
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="รูปแบบข้อมูลไม่ถูกต้อง")
    try:
        res = s9_hub.save(body.get("changes") or {}, body.get("revs") or {}, user=_req_user(req))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "บันทึกไม่สำเร็จ"))
    return res


@app.post("/api/s9-hub/items/preview")
async def api_s9_items_preview(file: UploadFile = File(...)):
    """อัปโหลด .xlsx ผ้าที่จ้างทอได้ → ตรวจ + คืนข้อมูลให้หน้าเว็บดูก่อน (ยังไม่บันทึก)
    กดยืนยันแล้วหน้าเว็บส่งกลับมาทาง POST /api/s9-hub (replace_items)"""
    try:
        res = s9_hub.parse_items_file(await file.read())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "อ่านไฟล์ไม่สำเร็จ"))
    return res


@app.post("/api/s9-hub/items/check")
async def api_s9_items_check(req: Request):
    """ข้อมูลที่วางจาก Excel ({columns, rows}) → ตรวจแบบเดียวกับไฟล์ (ยังไม่บันทึก)"""
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="รูปแบบข้อมูลไม่ถูกต้อง")
    res = s9_hub.check_items(body.get("columns") or [], body.get("rows") or [])
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "ข้อมูลไม่ถูกต้อง"))
    return res


@app.post("/api/s9-hub/only/preview")
async def api_s9_only_preview(file: UploadFile = File(...)):
    """อัปโหลด .xlsx ผ้าที่ต้องจ้างทอเสมอ → ตรวจ + คืนข้อมูลให้หน้าเว็บดูก่อน (ยังไม่บันทึก)
    กดยืนยันแล้วหน้าเว็บส่งกลับมาทาง POST /api/s9-hub (replace_only)"""
    try:
        res = s9_hub.parse_only_file(await file.read())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "อ่านไฟล์ไม่สำเร็จ"))
    return res


@app.post("/api/s9-hub/only/check")
async def api_s9_only_check(req: Request):
    """ข้อมูลที่วางจาก Excel ({columns, rows}) → ตรวจแบบเดียวกับไฟล์ (ยังไม่บันทึก)"""
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="รูปแบบข้อมูลไม่ถูกต้อง")
    res = s9_hub.check_only(body.get("columns") or [], body.get("rows") or [])
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "ข้อมูลไม่ถูกต้อง"))
    return res


@app.post("/api/s9-hub/mc/preview")
async def api_s9_mc_preview(file: UploadFile = File(...)):
    """อัปโหลด .xlsx เครื่องของผู้รับจ้าง → ตรวจ + คืนข้อมูลให้หน้าเว็บดูก่อน (ยังไม่บันทึก)
    กดยืนยันแล้วหน้าเว็บส่งกลับมาทาง POST /api/s9-hub (replace_mc)"""
    try:
        res = s9_hub.parse_mc_file(await file.read())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "อ่านไฟล์ไม่สำเร็จ"))
    return res


@app.post("/api/s9-hub/mc/check")
async def api_s9_mc_check(req: Request):
    """ข้อมูลที่วางจาก Excel ({columns, rows}) → ตรวจแบบเดียวกับไฟล์ (ยังไม่บันทึก)"""
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="รูปแบบข้อมูลไม่ถูกต้อง")
    res = s9_hub.check_mc(body.get("columns") or [], body.get("rows") or [])
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "ข้อมูลไม่ถูกต้อง"))
    return res


@app.get("/api/mc-new/options")
def api_mc_new_options():
    """ค่าที่มีอยู่จริงของแต่ละช่อง — หน้าเว็บใช้ทำ dropdown แทนช่องพิมพ์อิสระ"""
    try:
        return mc_new.options()
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mc-new/fix-workday-names")
def api_mc_new_fix_workday(request: Request):
    """ย้ายค่าวันทำงานจากชื่อกลุ่มเดิมไปชื่อปัจจุบัน (หลังมีคนแก้ชื่อกลุ่มในทะเบียน)"""
    try:
        return mc_new.fix_workday_names(user=_req_user(request))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/mc-new/audit")
def api_mc_new_audit():
    """กลุ่มเครื่องที่ยังขาดวันทำงาน/กำลังผลิต — หน้าเว็บโชว์เป็นแถบเตือน"""
    try:
        return mc_new.audit()
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mc-new")
async def api_mc_new_create(req: Request):
    try:
        data = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="รูปแบบข้อมูลไม่ถูกต้อง")
    try:
        res = mc_new.create(data or {}, user=_req_user(req))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "เพิ่มเครื่องไม่สำเร็จ"))
    return res


# ---------------- Item ทดแทน (Master_Item ชีท "Master_Item V2") ----------------
@app.get("/api/substitute")
def api_substitute_summary():
    return substitute_view.summary()


@app.get("/api/substitute/search")
def api_substitute_search(q: str = "", only_multi: bool = True, limit: int = 200):
    try:
        return substitute_view.search(q, only_multi, limit)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/substitute/preview")
async def api_substitute_preview(file: UploadFile = File(...)):
    try:
        return substitute_view.preview_import(await file.read())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/substitute/import")
async def api_substitute_import(request: Request, file: UploadFile = File(...)):
    """อัปโหลด .xlsx → ทับเฉพาะชีท 'Master_Item V2' (สำรอง .bak ก่อน)"""
    try:
        return substitute_view.import_v2(await file.read(), file.filename or "",
                                         user=_req_user(request))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Plan (แผนผลิตล่าสุด — แก้ไข + export) ----------------
@app.get("/api/plan")
def api_plan():
    return plan_view.latest_meta()


@app.get("/api/plan/sheet")
def api_plan_sheet(sheet: str = None):
    try:
        d = plan_view.read_grid(sheet)
        d["rev"] = livesync.rev(livesync.plan_res(d.get("sheet") or sheet or ""))
        return d
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PlanSaveReq(BaseModel):
    sheet: str
    columns: list
    rows: list
    base_rev: int | None = None
    # ชื่อไฟล์แผนที่จอนั้นโหลดไป — กันเคส "รันแผนรอบใหม่แล้วจอเก่าเซฟทับไฟล์ใหม่"
    base_file: str | None = None


@app.put("/api/plan/sheet")
def api_plan_save(req: PlanSaveReq, request: Request):
    try:
        return plan_view.write_grid(req.sheet, req.columns, req.rows,
                                    user=_req_user(request), base_rev=req.base_rev,
                                    base_file=req.base_file or "")
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=e.as_dict())
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# sheet = ชีทที่หน้าเว็บเปิดอยู่ — ชีท REPLAN ใช้ "โลกของเครื่อง" คนละใบกับ PLAN
# (เครื่องของสัปดาห์ที่ปลดล็อกถูกคืนเป็นว่าง เพราะงาน booking กลายเป็นแถวแผนแล้ว)
# ไม่ส่งมา = PLAN ตามเดิม
@app.get("/api/plan/load")
def api_plan_load(sheet: str = None):
    return plan_view.load_by_week(sheet)


@app.get("/api/plan/ava")
def api_plan_ava(sheet: str = None):
    return plan_view.ava_by_week(sheet)


@app.get("/api/plan/setup-jobs")
def api_plan_setup_jobs(sheet: str = None):
    """รายการ job ที่ตั้งเครื่องใหม่ ต่อ (สัปดาห์ × ประเภทโรงงาน) จากชีท SETUP_TRACKING
    Gantt ใช้เปิดการ์ดเมื่อคลิกแถบโหลด — บอกว่าโควตา Set up ถูกใช้ไปกับ item ไหนบ้าง"""
    return plan_view.setup_jobs_by_week(sheet)


@app.get("/api/plan/s9-pool")
def api_plan_s9_pool():
    """เครื่องของผู้รับจ้างทอต่อ (กลุ่ม|เกจ) จากชีท MC S9 — Gantt จ้างทอใช้เป็นเพดานเครื่อง
    (พูลในบ้านใช้ไม่ได้: แถวจ้างทอเป็น MC_GROUP=COMKN ซึ่งไม่มีเครื่องในโรงงานเรา)"""
    return plan_view.s9_pool()


@app.get("/api/plan/s9-item-cap")
def api_plan_s9_item_cap():
    """CAP ของผู้รับจ้างต่อ (ITEM|เกจ) จากชีท Item S9 — ลาก/ส่งงานจ้างทอคิดเครื่องด้วย cap ของผ้าตัวนั้น"""
    return plan_view.s9_item_cap()


@app.get("/api/plan/s9-eligible")
def api_plan_s9_eligible():
    """คู่ (ITEM|เกจ) ที่จ้างทอได้ จากชีท Item S9 — เปิดปุ่ม "ส่งไปจ้างทอ" ในการ์ดงาน"""
    return plan_view.s9_eligible()


@app.get("/api/plan/pool-map")
def api_plan_pool_map():
    # { "CAT|GUAGE|MC_GROUP": pool_key } เฉพาะกลุ่มที่แยกพูล (เช่น SKP vs SKPTA/SKPLE)
    return plan_view.pool_map()


@app.get("/api/plan/mc-groups")
def api_plan_mc_groups():
    # { "CAT|GUAGE|MC_GROUP": pool_key } ของทุกเครื่อง — บอกว่ากลุ่มไหนอยู่ "ก้อนเดียวกัน"
    # (ใช้แทนกันได้ → งานของ item เดียวกันเป็นลูกโซ่เดียว ไม่แยกตามชื่อกลุ่ม)
    return plan_view.mc_group_map()


@app.get("/api/plan/booking-mc")
def api_plan_booking_mc(sheet: str = None):
    """เครื่องที่ booking ทอไอเทมนั้นอยู่แล้ว ต่อ (สัปดาห์ × ITEM|MC_GROUP|GUAGE)
    Gantt ใช้แยกเครื่อง booking (ไม่ย้ายตามงาน) ออกจากเครื่องที่แผนจองจากพูล"""
    return plan_view.booking_mc_by_item_week(sheet)


@app.get("/api/plan/booking-items")
def api_plan_booking_items(sheet: str = None, full: bool = False):
    """item ทั้งหมดจาก booking (แผนเก่า) — Gantt overlay เป็นบล็อกอ่านอย่างเดียว
    full=1 → booking เต็มทุกสัปดาห์ (ไม่ตัดตามสัปดาห์ล็อกของ replan) ใช้กับตารางเทียบในการ์ด"""
    return plan_view.booking_items_by_week(sheet, full=full)


@app.get("/api/plan/history-range")
def api_plan_history_range():
    """สัปดาห์/ปีที่มีประวัติให้เลือกจริง — หน้าเว็บเอาไปทำ dropdown "ย้อนถึง [ปี][สัปดาห์]"
    (ไม่ให้เลือกช่วงที่ไม่มีข้อมูลแล้วได้หน้าจอว่างโดยไม่รู้สาเหตุ)"""
    try:
        return plan_history.range_info()
    except Exception as e:
        # หน้าแผนต้องเปิดได้เสมอ — ทะเบียนพังก็แค่เลือกย้อนหลังไม่ได้ ไม่ใช่ทั้งหน้าล่ม
        return {"weeks": [], "years": [], "oldest": None, "rows": 0, "error": str(e)}


@app.post("/api/plan/history-fetch")
def api_plan_history_fetch(year: int, week: int, force: int = 0):
    """สั่งดึงประวัติจาก DB (ทำเบื้องหลัง — ดึง 7 สัปดาห์ใช้เวลา ~16 นาที)
    คืนทันทีว่ารับงานหรือไม่ ; ความคืบหน้าดูที่ /api/plan/history-status

    force=1 = ดึง booking ของทั้งช่วงที่เลือกใหม่แล้วคำนวณ AVA_MC ทับทะเบียน (ปุ่มบนหน้าแผน)
    force=0 = เติมเฉพาะสัปดาห์ที่ทะเบียนยังไม่มี"""
    try:
        return plan_history.start_fetch(year, week, force=bool(force))
    except Exception as e:
        return {"started": False, "reason": "error", "error": str(e)}


@app.get("/api/plan/history-status")
def api_plan_history_status():
    """ความคืบหน้าของงานดึงประวัติ (running/step/detail/weeks/error)"""
    try:
        return plan_history.fetch_status()
    except Exception as e:
        return {"running": False, "step": "error", "error": str(e)}


@app.get("/api/plan/history-items")
def api_plan_history_items(year: int, week: int):
    """บล็อกประวัติตั้งแต่ (ปี, สัปดาห์) ที่เลือก ถึงก่อนช่วงที่ไฟล์ booking ปัจจุบันครอบคลุม
    — ต่อกับ /api/plan/booking-items ได้พอดีโดยไม่มีบล็อกซ้ำ (ดู plan_history.items_since)"""
    try:
        return plan_history.items_since(year, week)
    except Exception as e:
        return {"items": [], "from": {"year": year, "week": week}, "weeks": 0, "error": str(e)}


# ---- แจ้งเมลแผนเปลี่ยน (รอบ Replan) ----
_CHANGES_CACHE = {"key": None, "at": 0.0, "data": None}
_CHANGES_LOCK = threading.Lock()


def _changes_cached() -> dict:
    """replan_notify.changes() แบบจำผลไว้ต่อไฟล์แผน+เวลาแก้ไข — **ใช้กับหน้าพรีวิวเท่านั้น**

    เปิดหน้าต่างเมลครั้งเดียวยิง 3 endpoint พร้อมกัน (changes / mail-teams / mail-recipients)
    ซึ่งเดิมต่างคนต่างอ่านไฟล์แผนแล้วเทียบใหม่ทั้งชุด = งานหนักเดียวกัน 3 รอบ ; ล็อกไว้ให้
    คำขอที่มาพร้อมกันรอผลของรอบแรกแทนที่จะคำนวณซ้อนกัน
    หมดอายุ 5 นาทีเผื่อข้อมูลชื่อทีม (booking/stock) ถูกดึงใหม่โดยที่ไฟล์แผนไม่เปลี่ยน
    ตอนกดส่งจริงไม่ใช้ตัวนี้ — อ่านไฟล์สดเสมอ เนื้อเมลต้องตรงกับไฟล์ ณ วินาทีที่ส่ง
    คืนสำเนาตื้น: ผู้เรียกเติมคีย์บนสุดได้ (เช่น "mail") โดยไม่ไปแก้ของใน cache
    """
    with _CHANGES_LOCK:
        p = plan_view.latest_path()
        key = (str(p), p.stat().st_mtime) if p else None
        c = _CHANGES_CACHE
        if key is None or c["key"] != key or time.time() - c["at"] >= 300:
            data = replan_notify.changes(p)
            c.update(key=key, at=time.time(), data=data)
        return dict(c["data"])


@app.get("/api/replan/changes")
def api_replan_changes():
    """รายการเปลี่ยนแปลงของรอบ Replan (เลื่อนสัปดาห์/ยอด/จบเร็ว-ช้า/หลุด/งานใหม่ + เหตุผล)
    — หน้าเว็บเอาไปพรีวิวก่อนกดส่ง ; ตัวเดียวกับที่ใช้ประกอบเนื้อเมล"""
    try:
        data = _changes_cached()
        # บอกไปด้วยว่าเมลจะโชว์ในตารางกี่รายการ และที่เหลือไปอยู่ในไฟล์แนบ
        # (หน้าเว็บโชว์ครบทุกรายการเสมอ — เพดานขนาดเป็นข้อจำกัดของเมลเท่านั้น)
        xlsx_bytes = len(replan_notify.build_xlsx(data))
        room = replan_notify.MAX_MAIL_BYTES - int(xlsx_bytes * 1.37)
        html = replan_notify.build_html(data, limit_bytes=max(8000, room),
                                        attach_name=replan_notify.xlsx_name(data))
        data["mail"] = {"subject": replan_notify.subject(data),
                        "bytes": len(html.encode("utf-8")),
                        "attach_name": replan_notify.xlsx_name(data),
                        "attach_bytes": xlsx_bytes,
                        "clip_bytes": mailer.CLIP_BYTES}
        return data
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/replan/mail-status")
def api_replan_mail_status():
    """ตั้งค่าเมลพร้อมหรือยัง + ผู้รับเริ่มต้น (ไม่คืน URL ของ flow — เป็นความลับ)"""
    return mailer.status()


class ReplanNotifyReq(BaseModel):
    to: list[str] = []
    cc: list[str] = []
    note: str = ""            # หมายเหตุของผู้ส่ง — โผล่เป็นกล่องสีเหลืองบนหัวเมล
    subject: str = ""         # แก้หัวข้อเองได้ ; ว่าง = ใช้หัวข้อที่ระบบสรุปให้


@app.post("/api/replan/notify")
def api_replan_notify(req: ReplanNotifyReq, request: Request):
    """ส่งเมลแจ้งการเปลี่ยนแปลงรอบ Replan แล้วบันทึกลง log

    เนื้อเมลประกอบใหม่จากไฟล์แผน ณ เวลาที่กดส่งเสมอ — ไม่รับ HTML จากหน้าเว็บ เพราะ
    endpoint นี้ส่งของออกนอกองค์กรได้ ถ้าให้ client ส่งเนื้อหามาเอง ใครก็ยิงเมลอะไรก็ได้
    ในนามระบบ
    """
    user = _req_user(request)
    try:
        data = replan_notify.changes()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not data["items"]:
        raise HTTPException(status_code=400, detail="รอบนี้ไม่มีรายการเปลี่ยนแปลงให้แจ้ง")

    subject = (req.subject or "").strip() or replan_notify.subject(data)
    # ไฟล์แนบมี "ทุกรายการ" ส่วนเนื้อเมลใส่ได้เท่าที่เหลือจากเพดานขนาด (ไฟล์แนบกินโควตาก่อน)
    # — เมล 207 KB เคยถูก flow กลืนหายเงียบ ๆ (ตอบ 202 แต่ผู้รับไม่ได้เมล)
    xlsx = replan_notify.build_xlsx(data)
    name = replan_notify.xlsx_name(data)
    room = replan_notify.MAX_MAIL_BYTES - int(len(xlsx) * 1.37)   # base64 + ค่าคุมของ JSON
    html = replan_notify.build_html(data, note=req.note, sender=user,
                                    limit_bytes=max(8000, room), attach_name=name)
    try:
        res = mailer.send_html(req.to, subject, html, cc=req.cc, attachments=[
            {"name": name, "data": xlsx,
             "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}])
    except mailer.MailError as e:
        replan_notify.log_sent(user, mailer.parse_list(req.to), mailer.parse_list(req.cc),
                               data, req.note, ok=False, error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    replan_notify.log_sent(user, res["to"], res["cc"], data, req.note, ok=True)
    return {**res, "subject": subject, "items": len(data["items"]),
            "summary": data["summary"]}


# ---- แจ้งเมลแยกตามทีมขาย ----

def _team_mail_plan(data: dict) -> dict:
    """ตารางว่า "รอบนี้จะส่งกี่ฉบับ ทีมไหน ถึงใคร" — ใช้ทั้งพรีวิวและตอนส่งจริง

    ประกอบจากที่เดียวกันเสมอ เพื่อไม่ให้สิ่งที่หน้าเว็บโชว์ก่อนกดกับสิ่งที่ยิงออกไปจริงต่างกัน
    ทีมที่ยังไม่มีผู้รับจะไม่ถูกส่ง แต่ต้อง**โผล่ในตารางพร้อมเหตุผล** ไม่ใช่หายเงียบ ๆ
    (ของจริงมีทีม RTS ที่งานเยอะเป็นอันดับสองแต่ยังไม่มีอีเมลในรายชื่อ)
    """
    dirc = mail_recipients.directory()
    parts = replan_notify.split_by_team(data)
    known = set(dirc["teams"])
    out, used = [], set()
    for p in parts:
        who = mail_recipients.for_team(p["key"], dirc)
        used.add(p["key"])
        out.append({
            "team": p["team"], "key": p["key"], "items": len(p["data"]["items"]),
            "summary": p["data"]["summary"], "to": who["to"], "cc": who["cc"],
            "ready": bool(who["to"]),
            "reason": "" if who["to"] else
                      ("ยังไม่มีผู้รับของทีมนี้ในรายชื่อ — เพิ่มได้ที่หน้า “รายชื่อผู้รับเมล”"
                       if p["key"] != replan_notify.UNASSIGNED else
                       "งานที่ไม่มีทีมติดมากับข้อมูล — ตั้งผู้รับด้วยทีมชื่อ “ไม่ระบุทีม” ถ้าต้องการส่ง"),
        })
    return {
        "teams": out,
        "total_items": len(data["items"]),
        # ทีมที่มีรายชื่อแต่รอบนี้ไม่มีงาน — บอกไว้ให้รู้ว่า "ไม่ได้ลืม" ไม่ใช่ระบบพลาด
        "idle_teams": sorted(dirc["teams"][k]["team"] for k in known - used),
        "all_cc": dirc["all_cc"], "all_to": dirc["all_to"],
        "warnings": dirc["warnings"],
        "test_mode": mailer.team_test_mode(),
        "test_to": mailer.team_test_to() if mailer.team_test_mode() else [],
    }


@app.get("/api/replan/mail-teams")
def api_replan_mail_teams():
    """พรีวิว: รอบนี้จะส่งเมลกี่ฉบับ ทีมไหน ถึงใคร ทีมไหนยังไม่มีผู้รับ"""
    try:
        return _team_mail_plan(_changes_cached())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ReplanNotifyTeamsReq(BaseModel):
    teams: list[str] = []     # คีย์ทีมที่จะส่ง ; ว่าง = ทุกทีมที่มีผู้รับ
    note: str = ""            # หมายเหตุของผู้ส่ง — ติดไปทุกฉบับ


@app.post("/api/replan/notify-teams")
def api_replan_notify_teams(req: ReplanNotifyTeamsReq, request: Request):
    """ส่งเมลแยกฉบับให้แต่ละทีมขาย — เนื้อเมลและไฟล์แนบมีเฉพาะงานของทีมนั้น

    ส่งทีละฉบับและ**ไม่หยุดเมื่อฉบับใดล้ม** — ฉบับที่ออกไปแล้วเรียกคืนไม่ได้อยู่แล้ว
    หยุดกลางคันมีแต่ทำให้ไม่รู้ว่าทีมไหนได้/ไม่ได้ ; ผลรายฉบับคืนกลับไปให้หน้าเว็บโชว์ครบ
    """
    user = _req_user(request)
    try:
        data = replan_notify.changes()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not data["items"]:
        raise HTTPException(status_code=400, detail="รอบนี้ไม่มีรายการเปลี่ยนแปลงให้แจ้ง")

    plan = _team_mail_plan(data)
    by_key = {p["key"]: p["data"] for p in replan_notify.split_by_team(data)}
    pick = set(req.teams or [])
    test, test_to = plan["test_mode"], plan["test_to"]
    if test and not test_to:
        raise HTTPException(status_code=400, detail=(
            "อยู่ในโหมดทดสอบแต่ยังไม่ได้ตั้งผู้รับสำหรับทดสอบ "
            "(KP_MAIL_TEAM_TEST_TO หรือ KP_MAIL_TO ใน .env)"))

    results = []
    for row in plan["teams"]:
        if pick and row["key"] not in pick:
            continue
        if not row["ready"]:
            results.append({**{k: row[k] for k in ("team", "key", "items")},
                            "ok": False, "skipped": True, "error": row["reason"]})
            continue
        sub = by_key[row["key"]]
        xlsx = replan_notify.build_xlsx(sub)
        name = replan_notify.xlsx_name(sub)
        room = replan_notify.MAX_MAIL_BYTES - int(len(xlsx) * 1.37)
        # โหมดทดสอบ: ผู้รับจริงถูกแทนที่ แต่ต้องเขียนไว้ในเนื้อเมลว่าของจริงจะไปถึงใคร
        # ไม่งั้นการทดสอบตอบไม่ได้ว่า "ถ้าส่งจริงจะส่งถูกคนไหม" ซึ่งเป็นสิ่งเดียวที่ต้องตรวจ
        banner = ""
        to, cc = row["to"], row["cc"]
        if test:
            banner = (f"อีเมลทดสอบ — ยังไม่ได้ส่งถึงฝ่ายขาย ฉบับจริงของทีม {row['team']} "
                      f"จะส่งถึง {', '.join(row['to'])}"
                      + (f" (สำเนา {', '.join(row['cc'])})" if row["cc"] else ""))
            to, cc = test_to, []
        subject = replan_notify.subject(sub)
        if test:
            subject = "[ทดสอบ] " + subject
        html = replan_notify.build_html(sub, note=req.note, sender=user,
                                        limit_bytes=max(8000, room), attach_name=name,
                                        banner=banner)
        try:
            res = mailer.send_html(to, subject, html, cc=cc, attachments=[
                {"name": name, "data": xlsx,
                 "content_type": "application/vnd.openxmlformats-officedocument."
                                 "spreadsheetml.sheet"}])
        except mailer.MailError as e:
            replan_notify.log_sent(user, to, cc, sub, req.note, ok=False, error=str(e))
            results.append({"team": row["team"], "key": row["key"], "items": row["items"],
                            "ok": False, "skipped": False, "error": str(e), "to": to})
            continue
        replan_notify.log_sent(user, res["to"], res["cc"], sub, req.note, ok=True)
        results.append({"team": row["team"], "key": row["key"], "items": row["items"],
                        "ok": True, "skipped": False, "to": res["to"], "cc": res["cc"],
                        "subject": subject, "bytes": res.get("total_bytes")})
    return {"test_mode": test, "test_to": test_to, "results": results,
            "sent": sum(1 for r in results if r["ok"]),
            "failed": sum(1 for r in results if not r["ok"] and not r["skipped"]),
            "skipped": sum(1 for r in results if r["skipped"])}


# ---- รายชื่อผู้รับเมลแยกตามทีม (ชีท Sales_Mail ใน MasterMC) ----
def _plan_teams_cached() -> list:
    """ทีมที่มีงานเปลี่ยนในแผนรอบล่าสุด [{team,key,items}] (ใช้ผลที่ cache ไว้)"""
    return [{"team": x["team"], "key": x["key"], "items": len(x["data"]["items"])}
            for x in replan_notify.split_by_team(_changes_cached())]


@app.get("/api/mail-recipients")
def api_mail_recipients():
    """รายชื่อทั้งหมด + ทีมที่ปรากฏในแผนรอบล่าสุด (ไว้ให้เลือกตอนเพิ่มคน จะได้ไม่พิมพ์ชื่อผิด)"""
    try:
        d = mail_recipients.directory()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # ชื่อทีมที่ "มีอยู่จริงในข้อมูล" — ต้องเป็นตัวเลือกให้กด ไม่ใช่ให้พิมพ์เอง เพราะการจับคู่
    # เป็นแบบตรงตัว พิมพ์ผิดตัวเดียว (THVN แทน THVN-1) = ทีมนั้นไม่ได้เมลโดยไม่มีอะไรเตือน
    known = []
    try:
        for t in _plan_teams_cached():
            known.append({**t, "has_mail": bool(mail_recipients.for_team(t["key"], d)["to"])})
    except Exception:
        pass
    return {"rows": d["rows"], "rev": d["rev"], "warnings": d["warnings"],
            "plan_teams": known, "sheet": f"{mail_recipients.MASTER}/{mail_recipients.SHEET}"}


class MailRecipientsReq(BaseModel):
    rows: list[dict] = []
    base_rev: int | None = None


@app.post("/api/mail-recipients")
def api_mail_recipients_save(req: MailRecipientsReq, request: Request):
    """บันทึกรายชื่อทั้งชุด (ตรวจอีเมล/ชื่อทีมก่อนเขียนเสมอ)"""
    try:
        return mail_recipients.save_rows(req.rows, user=_req_user(request),
                                         base_rev=req.base_rev)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except livesync.Conflict as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---- ล็อกแผน (ชีท Lock_Plan ใน MasterMC) — งาน booking ที่ห้าม Replan รื้อ ----
class PlanLockReq(BaseModel):
    week: str | int
    item: str
    mc_group: str = ""
    gauge: str | int = ""
    so: str = ""
    qty: float | None = None
    mc: float | None = None
    remark: str = ""
    # BOOKING = บล็อกที่จองเครื่องไว้แล้ว / PLAN = งานที่ Planning วางไว้ (ว่าง = BOOKING)
    source: str = ""
    # ชีทที่กดล็อก (PLAN/REPLAN) — ล็อกฝั่งแผนต้องรู้ว่าแถวอยู่ในไฟล์ไหน
    sheet: str = ""


@app.get("/api/plan/locks")
def api_plan_locks():
    """งานที่ถูกล็อกไว้ทั้งหมด — Gantt ใช้ขึ้น 🔒 บนบล็อก"""
    try:
        return plan_lock.list_locks()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/plan/locks")
def api_plan_lock_add(req: PlanLockReq, request: Request):
    try:
        return plan_lock.add_lock(req.week, req.item, req.mc_group, req.gauge,
                                  so=req.so, qty=req.qty, mc=req.mc,
                                  user=_req_user(request), remark=req.remark,
                                  source=req.source, sheet=req.sheet)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PlanLockBulkReq(BaseModel):
    """ล็อก/ปลดล็อกหลายบล็อกพร้อมกัน (การ์ดงานเลือกหลายสัปดาห์ทีเดียว)

    ส่ง "สถานะที่ต้องการ" ของสัปดาห์ที่ user แตะมาทั้งชุด — add = ให้ล็อก / remove = ให้ปลด
    เขียนชีท Lock_Plan รอบเดียว (ดู plan_lock.apply_locks)
    """
    add: list[PlanLockReq] = []
    remove: list[PlanLockReq] = []


@app.post("/api/plan/locks/bulk")
def api_plan_lock_bulk(req: PlanLockBulkReq, request: Request):
    try:
        return plan_lock.apply_locks(add=[d.model_dump() for d in req.add],
                                     remove=[d.model_dump() for d in req.remove],
                                     user=_req_user(request))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PlanLockMoveReq(BaseModel):
    """ลากงานที่ล็อกไว้ไปช่องอื่น → ย้ายล็อกตามไป (คีย์ล็อกมีสัปดาห์/เครื่องอยู่ด้วย)"""
    week: str | int
    item: str
    mc_group: str = ""
    gauge: str | int = ""
    to_week: str | int
    to_mc_group: str | None = None
    to_gauge: str | int | None = None
    qty: float | None = None
    mc: float | None = None
    source: str = "PLAN"


@app.post("/api/plan/locks/move")
def api_plan_lock_move(req: PlanLockMoveReq, request: Request):
    try:
        return plan_lock.move_lock(req.week, req.item, req.mc_group, req.gauge,
                                   req.to_week, req.to_mc_group, req.to_gauge,
                                   qty=req.qty, mc=req.mc,
                                   user=_req_user(request), source=req.source)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/plan/locks")
def api_plan_lock_remove(request: Request, week: str, item: str, mc_group: str = "",
                         gauge: str = "", source: str = ""):
    try:
        # ส่งชื่อคนกดไปด้วย — ตั๋วโปรแกรมที่ถูกปลดจะถูกจดลงชีท Program_Unlock (ใครปลด/เมื่อไร)
        return plan_lock.remove_lock(week, item, mc_group, gauge, source,
                                     user=_req_user(request))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/plan/program")
def api_plan_program():
    """ชีท Program ใน MasterMC → { ITEM_CODE: [TEAM, ...] }
    หน้าแผนใช้ทำตัวหนังสือสีเหลืองเมื่อ item + ทีมของแถวตรงกับที่ user กำหนดไว้"""
    return plan_view.program_map()


@app.get("/api/plan/download")
def api_plan_download():
    p = plan_view.latest_path()
    if p is None:
        raise HTTPException(status_code=404, detail="ยังไม่มีไฟล์แผนผลิต")
    return FileResponse(str(p), filename=p.name, media_type=XLSX_MIME, headers=NO_CACHE)


# ---------------- รหัสผ้าที่ให้สีแล้ว (หน้าแผนใช้ระบายสีบล็อกบน Gantt) ----------------
# เดิมเป็นส่วนหนึ่งของหน้า Order Color ซึ่งถอดทิ้งแล้ว (30 ก.ย. 2569) — เหลือเฉพาะ endpoint นี้
@app.get("/api/dyed-items")
def api_dyed_items():
    """รหัส item ที่ "ให้สีแล้ว" (OPEN_COLOR_LOSS > 0 ใน view ยอดเปิดสีของใบ SC)
    — Gantt หน้าแผนใช้แยกสีบล็อก เกณฑ์เดียวกับ Planning เป๊ะ ๆ
    ยังไม่ได้ดึง View_SCFol_Item → codes ว่าง (ไม่ 404 เพราะหน้าแผนต้องเปิดได้ปกติ)"""
    try:
        return dyed_items.dyed_item_codes()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Outsource Advisor (จ้างทอ AI) ----------------
@app.post("/api/outsource/advise")
def api_outsource_advise():
    try:
        return outsource_advisor.advise()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class OutsourceSplitReq(BaseModel):
    item_code: str
    outsource_qty: float = 0          # กก. ที่ส่งไปจ้างทอ (0 = ยกเลิกการแบ่ง)
    start_week: int | None = None     # สัปดาห์ที่จ้างทอ (user กำหนดเอง)


@app.get("/api/outsource/split")
def api_outsource_split_get():
    """การแบ่งจ้างทอที่ใช้อยู่ (ค้างจนกว่าจะลบ — รอบรันถัดไปใช้ค่านี้ด้วย)"""
    return {"items": outsource_advisor.load_split(),
            "weeks": outsource_advisor.plan_weeks()}


@app.post("/api/outsource/split")
def api_outsource_split_save(req: OutsourceSplitReq):
    try:
        return outsource_advisor.save_split(req.item_code, req.outsource_qty, req.start_week)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/outsource/split/{item_code:path}")
def api_outsource_split_delete(item_code: str):
    try:
        return outsource_advisor.delete_split(item_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Cylinder Advisor (Change Cylinder AI) ----------------
@app.post("/api/cylinder/advise")
def api_cylinder_advise(request: Request, sheet: str = None):
    """วิเคราะห์จากชีทที่ user เปิดอยู่ — โหมด REPLAN ต้องใช้แถวและ capacity ของรอบ replan"""
    try:
        return cylinder_advisor.advise(sheet, user=_req_user(request))
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cylinder/advise/last")
def api_cylinder_advise_last(sheet: str = None):
    """ผลวิเคราะห์ล่าสุดที่เก็บไว้ — เปิดหน้าใหม่แล้วเห็นของเดิมทันที ไม่ต้องกดวิเคราะห์ซ้ำ
    (stale=true = แผน/การติ๊กเปลี่ยนไปแล้ว หน้าเว็บจะสั่งวิเคราะห์ใหม่ให้เอง)"""
    try:
        return cylinder_advisor.last_advice(sheet)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class CylApproveEntry(BaseModel):
    factory: str
    cat: str
    src_gauge: str | int = ""          # เกจต้นทาง (ถอดเครื่องมาจากเกจนี้)
    tgt_gauge: str | int = ""          # เกจปลายทาง (เกจที่งานต้องใช้)
    ready_week: int                    # สัปดาห์ที่ต้องการใช้เครื่อง (set_week = สัปดาห์ก่อนหน้า)
    set_week: int | None = None
    year: int | None = None
    machines: int = 1
    items: list[str] = []
    note: str = ""


class CylApproveReq(BaseModel):
    entries: list[CylApproveEntry]
    sheet: str = ""
    plan_file: str = ""


class CylMove(BaseModel):
    """งานหนึ่งก้อนที่ถูกย้ายหลังติ๊ก — เก็บลง log ให้ตอบได้ว่าติ๊กแล้วแผนเปลี่ยนอะไรบ้าง"""
    item: str = ""
    so: str = ""
    from_week: str | int = ""
    to_week: str | int = ""
    qty: float | None = None
    mc: float | None = None


class CylCompare(BaseModel):
    """ยอดรายสัปดาห์ของ item หนึ่ง ก่อน/หลังติ๊ก — {"38": 1200.0, ...} (คีย์ = เลขสัปดาห์)"""
    item: str = ""
    mc_group: str = ""
    before: dict[str, float] = {}
    after: dict[str, float] = {}


class CylMovesReq(BaseModel):
    moves: list[CylMove]
    approval_ids: list[str] = []
    sheet: str = ""
    plan_file: str = ""
    compare: list[CylCompare] = []


@app.get("/api/cylinder/approved")
def api_cylinder_approved():
    """Change Cylinder ที่ติ๊กอนุมัติไว้ = เครื่องที่ย้ายเกจแล้ว (มีผลทุกรอบจนกว่าจะยกเลิก)"""
    try:
        # รายการที่ติ๊กไว้ก่อนระบบเปลี่ยนมาหักยอดลงไฟล์ MasterMC — หักให้ครั้งแรก (no-op หลังจากนั้น)
        try:
            cyl_approve.sync_spare_file()
        except Exception as e:
            print(f"[CYL] sync ยอดสำรองลงไฟล์ไม่ได้: {e}")
        return {"approvals": cyl_approve.list_approvals(),
                "timing": cyl_approve.timing()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cylinder/approved")
def api_cylinder_approve(req: CylApproveReq, request: Request):
    try:
        return cyl_approve.approve([e.model_dump() for e in req.entries],
                                   user=_req_user(request), sheet=req.sheet,
                                   plan_file=req.plan_file)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/cylinder/approved")
def api_cylinder_revoke(id: str, request: Request):
    """ยกเลิก → เครื่องกลับไปเกจเดิมทันที (ทั้งบนหน้าเว็บและรอบวางแผนถัดไป)"""
    try:
        return cyl_approve.revoke(id, user=_req_user(request))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cylinder/moves")
def api_cylinder_moves(req: CylMovesReq, request: Request):
    """บันทึกงานที่ระบบย้ายให้หลังติ๊ก (หน้าเว็บส่งมาหลังคำนวณแผนใหม่เสร็จ)"""
    try:
        return cyl_approve.log_moves([m.model_dump() for m in req.moves],
                                     user=_req_user(request), sheet=req.sheet,
                                     plan_file=req.plan_file,
                                     approval_ids=req.approval_ids,
                                     compare=[c.model_dump() for c in req.compare])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cylinder/moves")
def api_cylinder_moves_get(limit: int = 100, compares: int = 5):
    """แผนเก่า → แผนใหม่: งานที่ระบบย้ายให้หลังติ๊กเปลี่ยน Cylinder (ทุกคนดูได้ ไม่ใช่ log ดิบ)

    moves    = รายก้อน (W เดิม → W ใหม่)
    compares = ตารางยอดรายสัปดาห์ก่อน/หลัง ของการติ๊กแต่ละครั้ง
    """
    try:
        return {"moves": cyl_approve.recent_moves(limit),
                "compares": cyl_approve.recent_compares(compares)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cylinder/log")
def api_cylinder_log(limit: int = 200):
    """ประวัติ Change Cylinder ทั้งหมด — หน้าหลัก (admin เท่านั้น ดู ADMIN_ONLY)"""
    try:
        return {"events": cyl_approve.read_log(limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
def api_health():
    return {"ok": True}


# ---------------- Live sync (ทุกจอเห็นของชุดเดียวกัน) ----------------
@app.get("/api/state/rev")
def api_state_rev():
    """เลขรุ่นของข้อมูลทุกชุด + ใครแก้ล่าสุด

    หน้าเว็บใช้สองจังหวะ: ตอนเปิดหน้า (เอาเลขตั้งต้นไปแนบเวลาเซฟ) และเป็นทางสำรอง
    เวลา /api/events ต่อไม่ติด (เช่นถูก proxy ตัด) — จะถอยไปถามเลขรุ่นทุก 5 วินาทีแทน
    """
    return {"revs": livesync.snapshot(), "watchers": livesync.subscriber_count(),
            "epoch": livesync.EPOCH}


@app.get("/api/state/log")
def api_state_log(request: Request, limit: int = 200, resource: str = ""):
    """ใครแก้อะไรไปบ้าง (ล่าสุดก่อน) — จำเป็นเพราะหน้าเว็บเซฟให้เองโดยไม่มีปุ่มบันทึก
    เนื้อเดียวกับประวัติการแก้ไข (edit_log.jsonl) → เฉพาะ superadmin เหมือนกัน"""
    if not auth.is_superadmin(_req_user(request)):
        raise HTTPException(status_code=403, detail="ประวัติการแก้ไขดูได้เฉพาะ Super Admin")
    return {"events": livesync.read_audit(limit, resource)}


@app.get("/api/events")
async def api_events(request: Request, token: str = ""):
    """สายข่าวค้างไว้ 1 เส้นต่อจอ (SSE) — มีคนเซฟเมื่อไหร่ ทุกจอรู้ทันที

    ทำไมไม่ให้หน้าเว็บถามซ้ำ ๆ แทน: ถามทุก 5 วินาที = ช้าได้ถึง 5 วินาที และยิ่งมีคน
    เปิดหน้าไว้มากยิ่งถามถี่ ; ทางนี้เงียบสนิทจนกว่าจะมีคนแก้จริง

    token รับทาง query เพราะ EventSource ของเบราว์เซอร์แนบ header เองไม่ได้
    (middleware จึงปล่อย path นี้ผ่าน แล้วมาตรวจ token เองตรงนี้)
    """
    if not auth.check_token(token):
        raise HTTPException(status_code=401, detail="ยังไม่ได้เข้าสู่ระบบ หรือเซสชันหมดอายุ")

    async def stream():
        q = livesync.subscribe()
        try:
            # ส่งเลขรุ่นปัจจุบันให้ก่อนหนึ่งชุด — จอที่เพิ่งต่อ (หรือต่อใหม่หลังเน็ตหลุด)
            # จะได้รู้ทันทีว่าพลาดการแก้ของใครไปหรือเปล่า โดยไม่ต้องรอให้มีคนเซฟรอบถัดไป
            # ต้องส่ง epoch มาด้วย (รูปเดียวกับ /api/state/rev) ไม่งั้นจอที่ต่อ SSE ติดตั้งแต่แรก
            # จะไม่รู้จัก epoch เลย → ตรวจ "server เพิ่งรีสตาร์ต" ไม่ทำงาน (ดู live.js:onServerRestart)
            sync = {"epoch": livesync.EPOCH, "revs": livesync.snapshot()}
            yield f"event: sync\ndata: {json.dumps(sync, ensure_ascii=False)}\n\n"
            idle = 0.0
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = q.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.25)
                    idle += 0.25
                    if idle >= 15:       # เคาะให้ proxy/เบราว์เซอร์รู้ว่าสายยังอยู่
                        idle = 0.0
                        yield ": ping\n\n"
                    continue
                idle = 0.0
                yield f"event: changed\ndata: {json.dumps(msg, ensure_ascii=False)}\n\n"
        finally:
            livesync.unsubscribe(q)

    return StreamingResponse(stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        # ห้าม nginx (webchat-proxy) อมข้อมูลไว้ในบัฟเฟอร์ ไม่งั้นข่าวจะไปถึงจออื่นช้า
        # หรือไม่ถึงเลย — ตั้งจากฝั่งนี้ได้ ไม่ต้องไปแก้ config ของ proxy
        "X-Accel-Buffering": "no",
    })


# ---------------- Serve React build ----------------
if FRONTEND_DIST.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str, request: Request):
        # URL ทางการคือ http://docker-webchat/knitplan/ (ผ่าน nginx port 80)
        # ถ้าเข้าตรงที่ port 8080 จะไม่มี header X-Forwarded-Prefix ที่ nginx ใส่มา → เด้งไป URL ทางการ
        # เช็คชื่อโฮสต์ด้วย เพื่อไม่ให้ dev ที่รัน uvicorn บนเครื่องตัวเอง (localhost) โดนเด้งตามไป
        host = request.headers.get("host", "").split(":")[0]
        if host.startswith("docker-webchat") and not request.headers.get("x-forwarded-prefix"):
            return RedirectResponse(f"http://{host}/knitplan/{full_path}", status_code=302)
        # ถ้าเป็น path ไฟล์จริง ส่งไฟล์ ไม่งั้นส่ง index.html (SPA routing)
        candidate = FRONTEND_DIST / full_path
        if full_path and candidate.exists() and candidate.is_file():
            return FileResponse(str(candidate))
        # index.html ห้าม cache — ให้เบราว์เซอร์ดึง bundle เวอร์ชันล่าสุดทุกครั้ง
        return FileResponse(
            str(FRONTEND_DIST / "index.html"),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

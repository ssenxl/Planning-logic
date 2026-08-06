"""
server.py — FastAPI app
- เสิร์ฟ React build (frontend/dist) ที่ /
- API ที่ /api/*
- เริ่ม APScheduler ตอน startup
รัน: uvicorn server:app --host 0.0.0.0 --port 8080
"""
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
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
import substitute_view
import plan_view
import order_color_view
import order_color_advisor
import outsource_advisor
import cylinder_advisor

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    _auto_extend_calendar()
    scheduler.start()
    yield


app = FastAPI(title="Knit Plan Web", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ---------------- Auth ----------------
# path ที่เข้าได้โดยไม่ต้อง login
AUTH_OPEN_PATHS = {"/api/login", "/api/health"}

# path ที่เฉพาะ admin เท่านั้นถึงเรียกได้ (ตั้งเวลาอัตโนมัติ)
# gate ที่ (method, path) — endpoint อ่านสถานะ/log ยังเปิดให้ทุกคนเพราะหน้าอื่น poll อยู่
# หมายเหตุ: สั่งรัน/หยุด pipeline (/api/run, /api/run/stop) เปิดให้ user ธรรมดารันได้แล้ว
ADMIN_ONLY = {
    ("PUT", "/api/schedule"),
}


def _is_download_path(path: str) -> bool:
    """endpoint ดาวน์โหลดไฟล์ — เปิดให้โหลดได้โดยไม่ต้องมี token
    (ใช้ลิงก์ <a href> โหลดตรงได้ทุกเมื่อ); ไม่รวม /api/outputs/booking ที่เป็น list"""
    if path == "/api/database/download":
        return True
    if path == "/api/map-item/download":
        return True
    if path == "/api/plan/download":
        return True
    if path == "/api/order-color/download":
        return True
    if path.startswith("/api/outputs/") and path != "/api/outputs/booking":
        return True
    return False


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    path = request.url.path
    # endpoint ดาวน์โหลดเปิดเฉพาะ GET เท่านั้น — method อื่น (เช่น DELETE) ยังต้อง login
    is_download = _is_download_path(path) and request.method == "GET"
    # ป้องกันเฉพาะ /api/* (ยกเว้น path ที่เปิดไว้ + ดาวน์โหลด GET); preflight OPTIONS ปล่อยผ่าน
    if path.startswith("/api/") and path not in AUTH_OPEN_PATHS \
            and not is_download \
            and request.method != "OPTIONS":
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else ""
        username = auth.check_token(token)
        if not username:
            return JSONResponse(status_code=401, content={"detail": "ยังไม่ได้เข้าสู่ระบบ หรือเซสชันหมดอายุ"})
        # endpoint สำหรับ admin เท่านั้น (สั่งรัน/ตั้งเวลา) — user ธรรมดาเรียกไม่ได้
        if (request.method, path) in ADMIN_ONLY and not auth.is_admin(username):
            return JSONResponse(status_code=403, content={"detail": "ต้องเป็นผู้ดูแลระบบ (admin) เท่านั้น"})
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
        return masters.read_sheet(name, sheet)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SaveReq(BaseModel):
    columns: list
    rows: list


@app.put("/api/masters/{name}/{sheet}")
def api_master_save(name: str, sheet: str, req: SaveReq):
    try:
        return masters.write_sheet(name, sheet, req.columns, req.rows)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Work Day (วันทำงานตามกลุ่มเครื่อง + ยุบสัปดาห์) ----------------
@app.get("/api/workday")
def api_workday_get():
    try:
        return workday.get_workday()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class WorkDayReq(BaseModel):
    defaults: dict          # {"FACTORY|MC_CAT|GUAGE": วัน}
    weeks: dict = {}        # {"FACTORY|MC_CAT|GUAGE|WEEK": วัน}
    hours: dict = {}        # {"FACTORY|MC_CAT|GUAGE": ชั่วโมง} (ค่ามาตรฐานต่อกลุ่ม)
    merges: dict = {}       # {"31": 32}


@app.put("/api/workday")
def api_workday_put(req: WorkDayReq):
    try:
        return workday.save_workday(req.defaults, req.weeks, req.hours, req.merges)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/workday/seed")
def api_workday_seed():
    """สร้างชีท Work Day ครั้งแรกจาก Working Day ของ MasterMC (ไม่ทับค่าที่ตั้งไว้แล้ว)"""
    try:
        return workday.seed_from_mastermc()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- Schedule ----------------
@app.get("/api/schedule")
def api_schedule_get():
    return scheduler.next_runs()


class ScheduleReq(BaseModel):
    schedule: dict  # {db_pull:{enabled,hour,minute}, plan:{...}}


@app.put("/api/schedule")
def api_schedule_put(req: ScheduleReq):
    return scheduler.reschedule(req.schedule)


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
async def api_substitute_import(file: UploadFile = File(...)):
    """อัปโหลด .xlsx → ทับเฉพาะชีท 'Master_Item V2' (สำรอง .bak ก่อน)"""
    try:
        return substitute_view.import_v2(await file.read(), file.filename or "")
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
        return plan_view.read_grid(sheet)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PlanSaveReq(BaseModel):
    sheet: str
    columns: list
    rows: list


@app.put("/api/plan/sheet")
def api_plan_save(req: PlanSaveReq):
    try:
        return plan_view.write_grid(req.sheet, req.columns, req.rows)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/plan/load")
def api_plan_load():
    return plan_view.load_by_week()


@app.get("/api/plan/ava")
def api_plan_ava():
    return plan_view.ava_by_week()


@app.get("/api/plan/setup-jobs")
def api_plan_setup_jobs():
    """รายการ job ที่ตั้งเครื่องใหม่ ต่อ (สัปดาห์ × ประเภทโรงงาน) จากชีท SETUP_TRACKING
    Gantt ใช้เปิดการ์ดเมื่อคลิกแถบโหลด — บอกว่าโควตา Set up ถูกใช้ไปกับ item ไหนบ้าง"""
    return plan_view.setup_jobs_by_week()


@app.get("/api/plan/pool-map")
def api_plan_pool_map():
    # { "CAT|GUAGE|MC_GROUP": pool_key } เฉพาะกลุ่มที่แยกพูล (เช่น SKP vs SKPTA/SKPLE)
    return plan_view.pool_map()


@app.get("/api/plan/booking-mc")
def api_plan_booking_mc():
    """เครื่องที่ booking ถักไอเทมนั้นอยู่แล้ว ต่อ (สัปดาห์ × ITEM|MC_GROUP|GUAGE)
    Gantt ใช้แยกเครื่อง booking (ไม่ย้ายตามงาน) ออกจากเครื่องที่แผนจองจากพูล"""
    return plan_view.booking_mc_by_item_week()


@app.get("/api/plan/booking-items")
def api_plan_booking_items():
    """item ทั้งหมดจาก booking (แผนเก่า) — Gantt overlay เป็นบล็อกอ่านอย่างเดียว"""
    return plan_view.booking_items_by_week()


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


# ---------------- Order Color (Datamining → Booking — แก้ไข + export) ----------------
@app.get("/api/order-color")
def api_order_color():
    return order_color_view.latest_meta()


@app.get("/api/order-color/sheet")
def api_order_color_sheet(sheet: str = None):
    try:
        return order_color_view.read_grid(sheet)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class OrderColorSaveReq(BaseModel):
    sheet: str
    columns: list
    rows: list


@app.put("/api/order-color/sheet")
def api_order_color_save(req: OrderColorSaveReq):
    try:
        return order_color_view.write_grid(req.sheet, req.columns, req.rows)
    except (KeyError, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/order-color/download")
def api_order_color_download():
    p = order_color_view.latest_path()
    if p is None:
        raise HTTPException(status_code=404, detail="ยังไม่มีไฟล์ Order Color")
    return FileResponse(str(p), filename=p.name, media_type=XLSX_MIME, headers=NO_CACHE)


@app.post("/api/order-color/advise")
def api_order_color_advise():
    try:
        return order_color_advisor.analyze()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/order-color/plan")
def api_order_color_plan():
    """grid แผน what-if (แผนจริง + แทรก item สี) สำหรับเรนเดอร์ Gantt"""
    try:
        return order_color_advisor.build_plan()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/order-color/cat-history")
def api_order_color_cat_history():
    """default view — booking รายสัปดาห์จัดกลุ่มตาม CAT (เฉพาะ CAT ที่มี item สี)"""
    try:
        return order_color_advisor.cat_history()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/order-color/booking-gantt")
def api_order_color_booking_gantt():
    """Gantt จาก booking DETAIL (ทุก item ต่อสัปดาห์) สำหรับ PlanGantt"""
    try:
        return order_color_advisor.build_booking_gantt()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/order-color/booking-load")
def api_order_color_booking_load():
    """โควตา setup job ต่อสัปดาห์ (สำหรับ Gantt booking — job คิด live จาก NEW_MC)"""
    try:
        return order_color_advisor.booking_load()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/order-color/booking-ava")
def api_order_color_booking_ava():
    """เครื่องว่างต่อสัปดาห์แบบสอดคล้อง booking (planBase = used)"""
    try:
        return order_color_advisor.booking_ava()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ColorMovesReq(BaseModel):
    cat: str = ""
    gauge: str = ""
    items: list = []
    setup_load: dict = {}


@app.post("/api/order-color/advise-moves")
def api_order_color_advise_moves(req: ColorMovesReq):
    """AI จัดอันดับ + อธิบายผลกระทบการดึงงานสีเข้ามาแทนงานไม่มีสี (กลุ่ม CAT×เกจ ที่เลือก)"""
    try:
        return order_color_advisor.advise_color_moves(req.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class OrderColorExportReq(BaseModel):
    columns: list
    rows: list


@app.post("/api/order-color/plan/export")
def api_order_color_plan_export(req: OrderColorExportReq):
    """แปลง grid what-if (ที่ user จัดแล้ว) → ไฟล์ Excel ให้ดาวน์โหลด"""
    import io
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "PLAN_ORDER_COLOR"
    ws.append([str(c) for c in req.columns])
    for r in req.rows:
        ws.append(list(r))
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    from fastapi import Response
    return Response(
        content=buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="order_color_plan.xlsx"'},
    )


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
def api_cylinder_advise():
    try:
        return cylinder_advisor.advise()
    except (FileNotFoundError, KeyError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
def api_health():
    return {"ok": True}


# ---------------- Serve React build ----------------
if FRONTEND_DIST.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        # ถ้าเป็น path ไฟล์จริง ส่งไฟล์ ไม่งั้นส่ง index.html (SPA routing)
        candidate = FRONTEND_DIST / full_path
        if full_path and candidate.exists() and candidate.is_file():
            return FileResponse(str(candidate))
        # index.html ห้าม cache — ให้เบราว์เซอร์ดึง bundle เวอร์ชันล่าสุดทุกครั้ง
        return FileResponse(
            str(FRONTEND_DIST / "index.html"),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

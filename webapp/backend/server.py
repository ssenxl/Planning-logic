"""
server.py — FastAPI app
- เสิร์ฟ React build (frontend/dist) ที่ /
- API ที่ /api/*
- เริ่ม APScheduler ตอน startup
รัน: uvicorn server:app --host 0.0.0.0 --port 8080
"""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
import runner
import masters
import database
import scheduler

FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield


app = FastAPI(title="Knit Plan Web", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

api = FastAPI()  # sub-app ไม่จำเป็น แต่ใช้ router ตรงๆ ง่ายกว่า


# ---------------- Run / Pipeline ----------------
class RunReq(BaseModel):
    mode: str  # full | db | plan


@app.post("/api/run")
def api_run(req: RunReq):
    return runner.start_run(req.mode, trigger="manual")


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
        for f in sorted(config.OUTPUT_DIR.glob("booking_final_ready25.xlsx"),
                        key=lambda p: p.stat().st_mtime, reverse=True):
            st = f.stat()
            out.append({"name": f.name, "size": st.st_size,
                        "mtime": st.st_mtime, "kind": "booking"})
    return out


@app.get("/api/outputs/{fname}")
def api_output_download(fname: str):
    f = config.OUTPUT_DIR / fname
    if not f.exists() or f.parent != config.OUTPUT_DIR:
        raise HTTPException(status_code=404, detail="ไม่พบไฟล์")
    return FileResponse(str(f), filename=fname,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


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
        return FileResponse(str(FRONTEND_DIST / "index.html"))

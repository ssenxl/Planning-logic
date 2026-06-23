# Knit Plan Web

เว็บสำหรับรัน pipeline วางแผนการผลิต + แก้ไฟล์ Master + ตั้งเวลาอัตโนมัติ
รันเป็น container แยกอิสระบน server `docker-webchat` (ไม่กระทบ webchat)

## ฟีเจอร์
- **แดชบอร์ด** — สั่งรัน (ทั้ง pipeline / ดึง DB / รันแผน), ดู log สด, สถานะ, เวลา schedule ถัดไป
- **แก้ Master** — grid editor: MasterMC (ทุกชีท), Calendar, Target_Stock → Save กลับ .xlsx (สำรอง .bak อัตโนมัติ)
- **ตั้งเวลา** — ปรับเวลา/เปิด-ปิด: ดึง DB (06:00) และ รันแผน (07:00) เขตเวลาไทย
- **ผลลัพธ์** — ลิสต์/ดาวน์โหลด `production_plan_*.xlsx`

## สถาปัตยกรรม
```
Browser ──http://docker-webchat:8080──> knitplan-app (container)
                                          ├─ FastAPI (server.py) + APScheduler
                                          ├─ React build (frontend/dist)
                                          └─ subprocess → run_all.py (pipeline)
Volumes (host /home/scm/knitplan):
  masters/  → /data/knitplan/masters   (MasterMC, Calendar, Target_Stock)
  output/   → /app/data_plan
  logs/     → /app/webapp/logs
  config/   → settings.json (เวลา schedule)
  env/.env  → SF5_USER / SF5_PASSWORD (Oracle)
```
- Oracle `172.16.7.55:1521/NYTG` — server อยู่ LAN เดียวกัน ต่อตรงได้
- Calendar/MasterMC อ่านจากไฟล์ local (config.ini) = ไฟล์เดียวกับที่แก้ผ่านเว็บ → แก้แล้วมีผลทันที

## Deploy
```powershell
pwsh webapp\deploy.ps1
```
แพ็คซอร์ส → scp ขึ้น server → build + up (project `knitplan`). ใช้ port 8080

## รันโลคอล (dev)
```powershell
# backend
cd webapp\backend
uvicorn server:app --port 8080
# frontend (อีก terminal) — proxy /api ไป :8080
cd webapp\frontend
npm install; npm run dev
```

## โหมดการรัน (map กับ run_all.py)
| ปุ่ม | คำสั่ง |
|------|--------|
| รันทั้ง Pipeline | `run_all.py` |
| ดึงข้อมูล DB | `run_all.py --from Calendar --skip AVA_MC Order Planning` |
| รันแผน | `run_all.py --from AVA_MC` |

## seed Master รอบแรก
อัปไฟล์ปัจจุบันขึ้น server (ดูขั้นตอนใน Phase 4 / ถามผู้ดูแล)
```
scp -i ~/.ssh/docker-webchat "MasterMC.xlsx" scm@docker-webchat:/home/scm/knitplan/masters/
```

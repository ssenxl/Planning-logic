# Knit Plan Web

เว็บสำหรับรัน pipeline วางแผนการผลิต + แก้ไฟล์ Master + ตั้งเวลาอัตโนมัติ
รันเป็น container แยกอิสระบน server `docker-webchat` (ไม่กระทบ webchat)

## ฟีเจอร์
- **แดชบอร์ด** — สั่งรัน (ทั้ง pipeline / ดึง DB / รันแผน), ดู log สด, สถานะ, เวลา schedule ถัดไป
- **แก้ Master** — grid editor: MasterMC (ทุกชีท), Calendar, Target_Stock → Save กลับ .xlsx (สำรอง .bak อัตโนมัติ)
- **ตั้งเวลา** — ปรับเวลา/เปิด-ปิด: ดึง DB (06:00) และ รันแผน (07:00) เขตเวลาไทย
- **ผลลัพธ์** — ลิสต์/ดาวน์โหลด `production_plan_*.xlsx`
- **แจ้งเมลแผนเปลี่ยน** — ปุ่ม 📧 บนหน้าแผน (ชีท REPLAN): สรุปงานที่เลื่อนสัปดาห์ / ยอดเปลี่ยน /
  จบเร็ว-ช้ากว่าเดิม / หลุดจากแผน / งานใหม่ พร้อมเหตุผลรายไอเทม+ใบสั่ง แล้วส่งอีเมล (ดูหัวข้อข้างล่าง)

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
- Oracle (host/port/service ดูใน `.env` บน server) — server อยู่ LAN เดียวกัน ต่อตรงได้
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
| ดึงข้อมูล DB | `run_all.py --skip Calendar Stock AVA_MC Order ProgramLock Replan` |
| รันแผน | `run_all.py --from AVA_MC` |

## seed Master รอบแรก
อัปไฟล์ปัจจุบันขึ้น server (ดูขั้นตอนใน Phase 4 / ถามผู้ดูแล)
```
scp -i ~/.ssh/docker-webchat "MasterMC.xlsx" scm@docker-webchat:/home/scm/knitplan/masters/
```

## ไล่บั๊ก "แก้แล้วแต่หน้าเว็บไม่เปลี่ยน"

**เว็บที่ใช้งานจริงคือ container บน server ไม่ใช่ไฟล์ในเครื่อง dev** — ไฟล์แผน/master คนละชุดกัน
(`/home/scm/knitplan/output`, `/home/scm/knitplan/masters`) ถ้า item ที่ user พูดถึงหาไม่เจอใน `data_plan/`
ของเครื่อง dev แปลว่ากำลังดูคนละไฟล์ ห้ามสรุปจากไฟล์ในเครื่อง

ไล่ตามลำดับนี้:

1. **เบราว์เซอร์ใช้โค้ดรอบไหน** — แถบหัวหน้าแผนผลิตมีป้าย `build YYYY-MM-DD HH:MM` (ฝังตอน build จาก
   `vite.config.js`) ถ้าไม่ตรงกับรอบ deploy ล่าสุด = โหลดจากแคช ให้กด `Ctrl+Shift+R`
   เช็คฝั่ง server ว่ากำลังเสิร์ฟไฟล์ไหน:
   ```bash
   curl -s http://docker-webchat:8080/ | grep -o 'assets/index-[A-Za-z0-9_-]*\.js'
   ```
   แล้วเทียบกับชื่อไฟล์ใน `webapp/frontend/dist/assets/`

2. **โค้ดขึ้น server แล้วจริงไหม** — `python deploy_to_webchat.py` (~15 วิ) จากนั้นยืนยันในคอนเทนเนอร์:
   ```bash
   ssh scm@docker-webchat "docker exec knitplan-app grep -c <ชื่อฟังก์ชัน> /app/webapp/backend/<ไฟล์>.py"
   ```

3. **ตัวเลขผิด → ดึงข้อมูลจริงจาก server มาคำนวณเอง** (เร็วกว่าเดาจากหน้าจอ)
   ```bash
   scp -i ~/.ssh/docker-webchat scm@docker-webchat:/home/scm/knitplan/output/production_plan_*.xlsx .
   scp -i ~/.ssh/docker-webchat scm@docker-webchat:/home/scm/knitplan/masters/Calendar.xlsx .
   ```
   แล้ว dump ชีทเป็น JSON รันตรรกะ frontend (`webapp/frontend/src/*.js`) ใน node ตรง ๆ

4. **`/api/*` ต้อง login (Bearer token)** เรียกจาก curl เปล่า ๆ ไม่ได้ ให้ดู log แทน:
   `ssh scm@docker-webchat "docker logs --tail 200 knitplan-app"`

### กับดักที่เคยเจอ
- **ปฏิทินหลายปี** — `Calendar.xlsx` มีข้อมูลถึงปี 2031 และทั้งระบบใช้เลขสัปดาห์เปล่าไม่มีปี
  เคยทำให้ `workday.calendar_week_days()` รวมวันทำงานทุกปี (W41 = 30 วันแทน 6) หน้าเว็บเลยคิดว่า
  เครื่องพอตลอด ย้ายงานไปสัปดาห์ไหนก็ไม่ต้องเพิ่มเครื่อง — ต้องนับต่อ (ปี, สัปดาห์) แล้วเลือกปีที่ใกล้ปีปัจจุบัน
- **เครื่องชุดเดียวเขียนซ้ำหลายแถว** — item เดียวกัน เครื่อง+เกจ+สัปดาห์เดียวกัน แต่คนละ SC ใช้เครื่องชุดเดียวกัน
  Planning.py เขียน `ACTUAL_MC/NEW_MC/CARRYOVER_MC` ค่าเดิมซ้ำทุกแถว → ตอนรวมต้องใช้ `max` ไม่ใช่ `sum`
  (มีแต่ `PRODUCE_QTY` ที่บวกสะสม)

## แจ้งเมลแผนเปลี่ยน (ปุ่ม 📧 หน้าแผน — ชีท REPLAN)

**สิ่งที่เมลบอก** (คิดจากชีท `REPLAN` + `REPLAN_COMPARE` ของไฟล์แผนล่าสุด — ไม่ได้คำนวณแผนใหม่):
งานที่เลื่อนสัปดาห์ (เช่น W10 → W12), ยอดเปลี่ยน, จบเร็ว/ช้ากว่าเดิมกี่สัปดาห์, งานที่หลุดจากแผน,
งานใหม่ที่เพิ่งเข้า — แจ้ง **ทุกรายการที่ขยับ ไม่ตัดทิ้ง** พร้อมเหตุผลรายไอเทม+ใบสั่ง (SC)

เหตุผลมาจากกฎที่อ่านค่าจริงในชีท เรียงจากชัดสุดไปกว้างสุด: งาน core คงแผนเดิม → ด้ายเข้าไม่ทัน
(`PO_IN_WEEK`/`EARLIEST_PLAN_WEEK`) → สัปดาห์เดิมอยู่ในช่วงล็อก → รอเปลี่ยน Cylinder → ย้ายไปจ้างทอ/S9
→ ยอดเปลี่ยน → จำนวนเครื่องที่ได้จริงเปลี่ยน → (ถ้าไม่เข้าข้อไหนเลย) เครื่องกลุ่มนั้นในสัปดาห์เดิมไม่ว่าง

### ตั้งค่าครั้งเดียว: สร้าง flow ใน Power Automate
1. Teams → ช่องที่คุณเป็นเจ้าของ → `...` → **Workflows** →
   *Post to a channel when a webhook request is received* → ตั้งชื่อ `KnitPlan Replan Mail`
2. เปิด flow นั้นใน Power Automate → **ลบ action โพสต์การ์ด Teams ออก** → เพิ่ม
   **Send an email (V2)** (Office 365 Outlook) แทน
3. ผูกช่องกับ body ที่ระบบส่งมา:

   | ช่องใน flow | ค่า |
   |---|---|
   | To | `triggerBody()?['mailTo']` |
   | Subject | `triggerBody()?['mailSubject']` |
   | Body | `triggerBody()?['mailHtml']` — เปิด advanced option **Is HTML = Yes** |
   | CC *(advanced options)* | `triggerBody()?['mailCc']` |

4. คัดลอก HTTP POST URL → ใส่ `KP_MAIL_FLOW_URL` ใน `.env`
   (บน server: `/home/scm/knitplan/env/.env` แล้ว `docker compose -p knitplan up -d` ให้ container สร้างใหม่ —
   แก้ .env เฉย ๆ ไม่มีผลจนกว่าจะ recreate)

เมลออกจาก**บัญชีเจ้าของ connection ใน flow** ไม่ใช่บัญชีที่ล็อกอินหน้าเว็บ (ชื่อคนกดส่งอยู่ในเนื้อเมล)
วิธีนี้ไม่ต้องมี Azure app registration / client secret / admin consent — เป็นวิธีเดียวกับที่ระบบแจ้งเตือน
`%Loss` (`C:\vscode\AI_Production loss`) ใช้ส่งเมลได้จริงทุกวันจาก server ตัวเดียวกัน

### ตัวแปรที่เกี่ยวข้อง (`.env`)
| ตัวแปร | ความหมาย |
|---|---|
| `KP_MAIL_FLOW_URL` | HTTP POST URL ของ flow (ไม่ตั้ง = ดูรายการได้ แต่ปุ่มส่งจะบอกว่ายังไม่ได้ตั้งค่า) |
| `KP_MAIL_TO` / `KP_MAIL_CC` | ผู้รับเริ่มต้น (หลายคนคั่นด้วย `;` หรือ `,`) — หน้าเว็บแก้ได้ทุกครั้งก่อนส่ง |
| `KP_MAIL_MAX_ROWS` | จำกัดจำนวนรายการในเมล (ค่าเริ่มต้น `0` = ไม่จำกัด) |

### หลังส่ง
- ทุกครั้งที่กดส่ง (สำเร็จหรือไม่) เขียนลง `data_plan/replan_notify_log.jsonl` 1 บรรทัด —
  ใคร ส่งให้ใคร เมื่อไร ไฟล์แผนไหน และรายการทั้งหมดที่แจ้งไป
- `HTTP 202` = flow รับเข้าคิวแล้ว **ไม่ได้แปลว่าผู้รับได้เมลแล้ว** — ถ้าเมลไม่ถึง ให้ดู Run history
  ของ flow ใน Power Automate ไม่ใช่ log ของเว็บ
- เนื้อเมลใหญ่กว่า ~102 KB Gmail จะย่อแล้วให้กด "ข้อความถูกตัดทอน" (Outlook แสดงครบ) —
  หน้าเว็บเตือนขนาดให้ก่อนกดส่ง

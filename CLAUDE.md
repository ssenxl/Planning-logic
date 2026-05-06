# CLAUDE.md — Knit Plan AI Project

## กฎบังคับ

### ภาษา
- **สื่อสารและอธิบายเป็นภาษาไทยเสมอ** ทุกข้อความที่ตอบ user

### ทุกครั้งที่จะแก้ไขโค้ด
1. **แจ้ง user ก่อนว่าจะแก้อะไร** และ**รอให้ user ยืนยัน (confirm) ก่อนเสมอ**
2. หลังแก้แล้ว **รายงานสิ่งที่เปลี่ยนแปลงทุกครั้ง** โดยระบุ:
   - **ไฟล์ที่แก้** — ชื่อไฟล์และบรรทัดที่เปลี่ยน
   - **แก้อะไร** — สิ่งที่เปลี่ยนจาก → เป็น
   - **ทำไม** — เหตุผลที่ต้องแก้

---

## ภาพรวมโปรเจกต์

ระบบวางแผนการผลิตผ้าถัก (Knit Planning) ของบริษัท Nan Yang Textile ทำงานเป็น pipeline ตามลำดับ:

```
View_Stock.py → Calendar.py → AVA_MC.py → Planning.py
```

ไฟล์ทั้งหมดอยู่ที่ `C:\vscode\AI_plan\`

---

## ไฟล์หลักและหน้าที่

| ไฟล์ | หน้าที่ |
|------|---------|
| `run_all.py` | รัน pipeline ทั้งหมดตามลำดับ |
| `View_Stock.py` | ดึงข้อมูล Stock จาก Oracle DB (`172.16.7.55:1521`) → บันทึกเป็น `Stock/view_stock.xlsx` |
| `View_Booking.py` | ดึงข้อมูล Booking จาก Oracle DB view `nyf.DFIV_KP_BOOKING@NYKPB.WORLD` → บันทึกเป็น `Booking/view_booking.xlsx` |
| `Calendar.py` | โหลด Calendar จากไฟล์ local (`Calendar.xlsx`) — กำหนด working day/holiday, Week definition: **Friday–Thursday** (บวก 3 วันก่อนคำนวณ ISO week) |
| `AVA_MC.py` | คำนวณ Machine Availability (MC_USE, MC_USE_CEIL, TOTAL_MC_REMAIN) จากข้อมูล Booking → output: `data_plan/booking_final_ready25.xlsx` |
| `Planning.py` | รัน Planning หลัก — assign งานให้เครื่องแต่ละสัปดาห์ → output: `data_plan/production_plan_DD-MM-YYYY.xlsx` |
| `Stock.py` | โมดูลช่วยอ่านและประมวลผลข้อมูล Stock จากไฟล์ local |
| `Calendar.py` | โมดูลช่วย load calendar ใช้ร่วมกันทั้งโปรเจกต์ |

---

## External Files (อยู่นอก repo)

| ไฟล์ | ตำแหน่ง | ใช้ใน |
|------|---------|-------|
| `MasterMC.xlsx` | `C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\` | AVA_MC.py, Planning.py |
| `Calendar.xlsx` | `C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\` | Calendar.py |
| `Item Special.xlsx` | `C:\Users\WICHARIT\Nan Yang Textile\SCM_Cloud - Knit Plan (AI)\` | AVA_MC.py, Planning.py |
| `Target_Stock.xlsx` | `C:\vscode\AI_plan\Estimate_Core\` | Stock.py |

---

## โครงสร้างโฟลเดอร์

```
AI_plan/
├── Booking/          ← ไฟล์ Booking input (.xlsx/.xls)
├── Stock/            ← ไฟล์ Stock input (.xlsx)
├── data_plan/        ← output ทั้งหมด
├── data/
│   └── Itemcore/     ← Itemcore.xlsx
├── Estimate_Core/    ← Target_Stock.xlsx
├── run_all.py
├── View_Stock.py
├── View_Booking.py
├── Calendar.py
├── AVA_MC.py
├── Planning.py
└── Stock.py
```

---

## Business Logic สำคัญ

### Week Definition
- สัปดาห์เริ่ม **วันศุกร์** สิ้นสุด **วันพฤหัสบดี**
- คำนวณโดยบวก 3 วันก่อน ใช้ ISO week: `date + timedelta(days=3)`

### Setup Days
- COTTON บริสุทธิ์ (ไม่มี CD/POLY/TC) → **3 วัน**
- POLY / CD / TC หรือมีหลายเส้น (`+`) → **5 วัน**
- ถ้า item/MC เดิมรันต่อเนื่อง (gap ≤ 3 สัปดาห์) → ไม่ setup ซ้ำ

### 20/24 Rule
- MC group ที่มี `Working Hours. = 20` ใน MasterMC.xlsx → คูณ `20/24`
- Item Special.xlsx สามารถ override working_day และ working_hour ของแต่ละ item ได้

### Oracle DB Connection
- Host: `172.16.7.55`, Port: `1521`, Service: `NYTG`
- Credentials: `$env:SF5_USER` / `$env:SF5_PASSWORD` (default: hctr/HCTR#23)
- ลอง 3 วิธีตามลำดับ: service_name → full DSN → Easy Connect SID

### MC Exclusion (AVA_MC.py)
MC group ที่ถูก exclude: `CL-NP, CL-OM, COMKN, F-CL, CL, FQCCL-NP, FQCCL-OM, FQC-Omnoi, FQC-Phet, FQC, F-TSD`

---

## การรัน

```powershell
# รัน pipeline ทั้งหมด
python run_all.py

# ข้าม View_Stock (ถ้า DB ไม่พร้อม)
python run_all.py --skip View_Stock

# เริ่มจาก step ที่ระบุ
python run_all.py --from AVA_MC

# รัน script เดี่ยว
python AVA_MC.py
python Planning.py
```

---

## Dependencies

```
oracledb, pandas, openpyxl, xlrd, numpy, msal
```

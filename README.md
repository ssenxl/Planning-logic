# Knit Plan AI — Planning Logic System

## ภาพรวมของโปรเจกต์

ระบบวางแผนการผลิตผ้าถัก (Knit Planning) ของ Nan Yang Textile พัฒนาด้วย Python
ทำงานเป็น **automated pipeline** ตั้งแต่ดึงข้อมูลจาก Oracle DB → คำนวณเครื่องจักร → วางแผนผลิตรายสัปดาห์ → Export ผลลัพธ์

---

## Pipeline (ลำดับการทำงาน)

```
1. Calendar.py      – ตรวจสอบ Calendar (validation)
2. View_Booking.py  – ดึง Booking จาก Oracle DB → Booking/view_booking.xlsx
3. View_Stock.py    – ดึง Stock จาก Oracle DB   → Stock/view_stock.xlsx
4. View_SC.py       – ดึง SC Pending จาก Oracle DB → Order/view_sc.xlsx
5. Stock.py         – ประมวลผล Stock + Target Stock
6. AVA_MC.py        – คำนวณ Machine Availability → data_plan/booking_final_ready25.xlsx
7. Order.py         – กรอง/เตรียม Order data     → data_plan/order_ready.xlsx
8. Planning.py      – รันแผนการผลิต              → data_plan/production_plan_DD-MM-YYYY.xlsx
```

---

## Data Flow

```
Oracle DB (internal)
  ├── View_Booking.py ──→ Booking/view_booking.xlsx ─┐
  ├── View_Stock.py   ──→ Stock/view_stock.xlsx       │
  └── View_SC.py      ──→ Order/view_sc.xlsx ─────────┤
                                                      │
SharePoint (Calendar.xlsx / MasterMC.xlsx)            │
  └── AVA_MC.py ──→ data_plan/booking_final_ready25.xlsx
                         ├── DETAIL                   │
                         └── SUMMARY_MC_REMAIN        │
                                                      │
Order/view_sc.xlsx + Order/*.xlsx                     │
  └── Order.py ──→ data_plan/order_ready.xlsx         │
                                                      │
data/Itemcore/Itemcore.xlsx ──────────────────────────┤
config.ini (paths: MasterMC, Target Stock) ───────────┤
                                                      ▼
                                              Planning.py (Main Engine)
                                                      │
                    ┌─────────────────────────────────┼──────────────────┐
                    ▼                                 ▼                  ▼
          data_plan/production_plan_         PIVOT_PLAN sheet     CYLINDER_CHANGE
          DD-MM-YYYY(BE).xlsx               (Excel PivotTable)    sheet
            ├── PLAN
            ├── PLAN_NO_S9
            ├── REMAINING_JOBS
            ├── SETUP_TRACKING
            ├── UNPLANNED
            └── CYLINDER_CHANGE
```

---

## โครงสร้างโปรเจกต์

```
AI_plan/
├── run_all.py           # รัน pipeline ทั้งหมดตามลำดับ
├── Calendar.py          # โหลด Calendar (SharePoint URL) + ปฏิทิน Friday–Thursday week
├── View_Booking.py      # ดึง Booking จาก Oracle DB (nyf.DFIV_KP_BOOKING@NYKPB.WORLD)
├── View_Stock.py        # ดึง Stock จาก Oracle DB → Stock/view_stock.xlsx
├── View_SC.py           # ดึง SC Pending จาก Oracle DB (BI_DATA_SC_PENDING_HL)
├── Stock.py             # ประมวลผล Stock + อ่าน Target Stock (STOCK_MIN)
├── AVA_MC.py            # คำนวณเครื่องว่าง (MC_USE, MC_REMAIN) + Booking summary
├── Order.py             # กรอง Order (ตัด CL-ORDERS, FQC, F-CL, COMKN)
├── Planning.py          # Core planning engine: assign งานให้เครื่องรายสัปดาห์
├── ITEM_Cap.py          # โหลด Item Capacity (CAP ทอ, REVOLUTION/WEIGHT, GUAGE)
├── Master_MC.py         # Lookup Capability Group จาก MasterMC
├── Train.py             # Train ML model (Linear SVM) สำหรับทำนาย MC_GROUP
├── predict.py           # Predict MC_GROUP ของ Item ใหม่ (CLI + function)
├── config.ini           # กำหนด path ไฟล์ภายนอก (MasterMC, Calendar, Target Stock)
├── Booking/             # view_booking.xlsx (output ของ View_Booking.py)
├── Stock/               # view_stock.xlsx (output ของ View_Stock.py)
├── Order/               # view_sc.xlsx + ไฟล์ Order ดิบ (.xlsx/.xls/.csv)
├── data_plan/           # output ทั้งหมด
│   ├── order_ready.xlsx
│   ├── booking_final_ready25.xlsx
│   └── production_plan_DD-MM-YYYY(BE).xlsx
├── data/
│   └── Itemcore/        # Itemcore.xlsx (ข้อมูล RTS items)
└── Estimate_Core/       # Target_Stock.xlsx (fallback ถ้าไม่มี config.ini)
```

### External Files (config.ini)

| ไฟล์ | ใช้ใน |
|------|-------|
| `MasterMC.xlsx` | AVA_MC.py, Planning.py — MC group, gauge, factory, spare cylinder |
| `Calendar.xlsx` (SharePoint) | Calendar.py, AVA_MC.py, Planning.py — working days, holidays |
| `Target Stock MIN, MAX CORE GREIGE.xlsx` | Stock.py — STOCK_MIN per item |

---

## ฟีเจอร์หลัก

- **Weekly Production Planning** — วางแผนผลิตอัตโนมัติรายสัปดาห์ คำนวณเครื่อง, setup days, ปริมาณผลิตต่อสัปดาห์
- **Machine Capacity Management** — Shared Pool (โหลด dynamic จาก MasterMC), หัก MC ที่ถูกจอง, Capability Group
- **S9 Routing** — งานที่ไม่สามารถใช้เครื่องปกติได้ → route ไป COMKN (จ้างทอ S9); แถวสีแดงใน output
- **Cylinder Change** — ตรวจจับและวางแผนการเปลี่ยน Gauge cylinder ระหว่าง item; แถวสีเหลืองใน output
- **Carryover Logic** — เครื่องที่วิ่งอยู่ (carry-over) ไม่ต้อง setup ใหม่
- **Dynamic Setup Days** — COTTON=3 วัน, POLY/DTY=5 วัน (ตาม MATERIAL_CONTENT + YARN_USED)
- **Progressive Machine Reduction** — เริ่มต้นด้วยเครื่องเยอะ แล้วลดลงให้ทัน TARGET_KNIT
- **Dynamic Setup Limit** — ปรับจำนวนเครื่อง setup ใหม่ตาม urgency ของ RDD
- **MC_GROUP Redirect** — บาง MC_GROUP+Gauge ถูก redirect ไปใช้เครื่องกลุ่มอื่นแทนอัตโนมัติ (เช่น SKP G20 → FA G20)
- **Factory-aware Working Days** — วันทำงานแตกต่างตาม Factory + override สำหรับ week พิเศษ
- **Calendar Integration** — ปฏิทินแบบ Friday–Thursday week, กรองวันหยุด, โหลดจาก SharePoint URL
- **Core Item Detection** — ตรวจจับ Core Item จาก Itemcore.xlsx
- **YD-ORDERS LT** — คำนวณ Earliest Plan Week จาก Dye End Date สำหรับ YD-ORDERS
- **Pivot Table Generation** — สร้าง Excel PivotTable อัตโนมัติผ่าน win32com (Windows)
- **ML MC_GROUP Prediction** — ทำนาย MC_GROUP สำหรับ Item ใหม่ด้วย Linear SVM + TF-IDF

---

## Configuration (Planning.py)

| Parameter | ค่าปัจจุบัน | คำอธิบาย |
|---|---|---|
| `SETUP_DAYS` | 3 | วัน setup default (override โดย dynamic logic) |
| `SETUP_GAP_WEEK` | 3 | ถ้าผลิต item เดิมภายใน 3 week → ไม่ต้อง setup ใหม่ |
| `SKIP_WEEKS` | `{}` | สัปดาห์ที่ข้าม (ว่างเปล่า = ไม่ข้ามสัปดาห์ใด) |
| `ALLOW_CARRYOVER_ACROSS_SO` | False | ไม่อนุญาต carryover ข้าม SC/SO NO |
| `ALLOW_SAME_ITEM_WEEK_CARRY` | True | อนุญาต carry เฉพาะ FG ถัดไปของ item เดียวกัน |
| `USE_PROGRESSIVE_REDUCTION` | True | เปิดโหมดลดเครื่องแบบค่อยเป็นค่อยไป |
| `MAX_NEW_SETUP_MC` | 2 | จำนวนเครื่อง setup ใหม่สูงสุดต่อ item/week |
| `PREFER_FULL_MACHINE_TO_TARGET` | True | ใช้เครื่องให้มากที่สุดโดยยังจบตรง TARGET_KNIT |
| `MC_GROUP_REDIRECT` | `{("SKP","20"): ("FA","20")}` | Redirect MC_GROUP+Gauge เฉพาะ (SKP G20 → FA G20) |

### Setup Days Logic

| เงื่อนไข | Setup Days |
|---|---|
| COTTON + ไม่มี DTY | 3 วัน |
| COTTON + มี DTY ใน YARN_USED | 5 วัน |
| POLY | 5 วัน |
| CD / TC / CVC / CT + มี DTY | 5 วัน |
| อื่นๆ | 3 วัน (default) |

---

## วิธีการรัน

```powershell
# รัน pipeline ทั้งหมด
python run_all.py

# ข้าม step ที่ระบุ (เช่น DB ไม่พร้อม)
python run_all.py --skip View_Stock View_Booking View_SC

# เริ่มจาก step ที่ระบุ
python run_all.py --from AVA_MC

# ทำงานต่อแม้ step ก่อนหน้าจะ fail
python run_all.py --ignore-errors

# รัน script เดี่ยว
python AVA_MC.py
python Planning.py
```

---

## Output Sheets (production_plan_DD-MM-YYYY.xlsx)

| Sheet | คำอธิบาย |
|---|---|
| `PLAN` | แผนการผลิตหลัก (แถวสีแดง = S9 routing, สีเหลือง = Cylinder Change) |
| `PLAN_NO_S9` | แผนเดียวกันแต่ไม่มี S9 routing (ทุก item ใช้เครื่องปกติ) |
| `REMAINING_JOBS` | งานที่ยังไม่ได้วางแผน (ไม่มีเครื่องพอ/ไม่ทัน RDD) |
| `SETUP_TRACKING` | ประวัติการ setup เครื่องแต่ละ item/MC_GROUP ต่อสัปดาห์ |
| `UNPLANNED` | Order ที่ระบบข้ามไม่ได้วางแผน |
| `CYLINDER_CHANGE` | สรุปการเปลี่ยน Gauge cylinder รายสัปดาห์ (Factory/MC_CAT/Gauge_FROM→TO) |
| `PIVOT_PLAN` | Excel PivotTable (PLAN_WEEK × ITEM_CODE) สร้างอัตโนมัติผ่าน win32com |

---

## Output Columns (PLAN sheet)

| Column | คำอธิบาย |
|---|---|
| `ITEM_CODE` | รหัสสินค้า |
| `SC_SO_NO` | เลข SC/SO |
| `MC_GROUP` | กลุ่มเครื่องจักร |
| `MC_GUAGE` | Gauge ของเครื่อง |
| `FACTORY_TYPE` | ประเภทโรงงาน (PHET/OM/OUTSOURCE) |
| `PLAN_WEEK` | สัปดาห์ที่วางแผนผลิต |
| `PLAN_YEAR` | ปีของ PLAN_WEEK |
| `CAT` | ประเภทเครื่อง (SINGLE/DOUBLE/COMKN) |
| `PRODUCE_QTY` | ปริมาณผลิต (kg) |
| `REQUIRED_MC` | เครื่องที่คำนวณไว้ล่วงหน้า (RDD target) |
| `ACTUAL_MC` | เครื่องที่ใช้จริงในสัปดาห์นี้ |
| `CARRYOVER_MC` | เครื่อง carry-over จาก week ก่อน |
| `NEW_MC` | เครื่อง setup ใหม่ week นี้ |
| `SETUP_DAYS` | วัน setup จริง (0 ถ้า carryover) |
| `DAILY_CAPACITY` | กำลังผลิตต่อวันต่อเครื่อง (CAP ทอ) |
| `REVOLUTION_WEIGHT` | น้ำหนักต่อรอบ (kg/rev) |
| `FACTORY_WORKING_DAYS` | วันทำงานตาม Factory type |
| `CALENDAR_WORKING_DAYS` | วันทำงานตาม Calendar จริง |
| `ACTUAL_WORKING_DAYS` | วันทำงานสุทธิที่ใช้คำนวณ |
| `AVAILABLE_DAYS` | วันที่ผลิตได้จริง (หัก setup) |
| `ORDERS_QTY` | จำนวน Order ทั้งหมด |
| `PENDING_PLAN` | ปริมาณที่ยังต้องผลิต |
| `PLAN_QTY` | ปริมาณที่เหลือหลังแผนนี้ |
| `ORDER_TYPE` | ประเภท Order (STOCK/YD-ORDERS/ฯลฯ) |
| `FG_WEEK` | สัปดาห์กำหนดส่ง FG |
| `TARGET_KNIT` | สัปดาห์เป้าหมายทอเสร็จ |
| `MATERIAL_CONTENT` | ส่วนผสมเส้นด้าย (COTTON/POLY/CD/TC/ฯลฯ) |
| `YARN_USED` | เส้นด้ายที่ใช้ (เช่น DTY 150/48) |
| `IS_CORE_ITEM` | "CORE ITEM" ถ้าเป็น Core item |
| `CUSTOMER` | ชื่อลูกค้า |
| `PLAN_SOURCE` | NEW = แผนใหม่, OLD = booking เก่า |
| `LT_YARN` | Lead time เส้นด้าย (วัน) หรือ Dye End Date (YD) |
| `EARLIEST_PLAN_WEEK` | สัปดาห์เร็วสุดที่เริ่มผลิตได้ |
| `SUB_COLOR` | รหัสสี |
| `NAY_COLOR` / `COLOR_DESC` | สีและคำอธิบายสี |
| `PO_NO` | เลข PO |
| `SC_LINE_ID` | Line ID ใน SC |
| `RDD_WEEK` | สัปดาห์ RDD (= FG_WEEK) |
| `CYLINDER_CHANGE` | "Yes" ถ้าสัปดาห์นี้มีการเปลี่ยน Gauge cylinder |
| `S9_ROUTING` | True ถ้า route งานไป S9 (COMKN จ้างทอ) |

---

## Oracle DB Connection

| ข้อมูล | ค่า |
|---|---|
| Service | `NYTG` |
| User/Pass | `$env:SF5_USER` / `$env:SF5_PASSWORD` |
| วิธีเชื่อมต่อ | ลอง 3 วิธีตามลำดับ: service_name → full DSN → Easy Connect SID |

---

## Dependencies

```
oracledb
pandas
openpyxl
xlrd
numpy
msal
scikit-learn
joblib
```

Optional: `pywin32` (สำหรับสร้าง Excel PivotTable บน Windows)

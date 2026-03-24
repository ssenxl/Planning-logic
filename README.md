# Planning Logic System

## ภาพรวมของโปรเจกต์

Planning Logic System เป็นระบบวางแผนการผลิตรายสัปดาห์ (Weekly Production Planning) ที่พัฒนาด้วย Python
ออกแบบมาเพื่อแทนที่การวางแผนด้วย Excel ให้กลายเป็น **automated business logic** ที่ทำงานได้เร็ว แม่นยำ และทำซ้ำได้

ระบบครอบคลุมตั้งแต่ การโหลดข้อมูล → คำนวณเครื่องจักร → วางแผนผลิตรายสัปดาห์ → Optimize กำลังการผลิต → Export ผลลัพธ์
รวมถึงมี **ML Model** สำหรับทำนาย MC_GROUP ของ Item ใหม่ที่ยังไม่เคยเห็น

---

## ฟีเจอร์หลัก

- **Weekly Production Planning** — วางแผนผลิตอัตโนมัติรายสัปดาห์ โดยคำนวณจำนวนเครื่อง, วัน Setup, วันทำงานจริง และปริมาณผลิตต่อสัปดาห์
- **Machine Capacity Management** — จัดการเครื่องจักรแบบ Shared Pool (เช่น SKPLE+SKPTA ใช้เครื่องร่วมกัน), หัก MC ที่ถูกจองแล้ว, รองรับ Capability Group
- **Carryover Logic** — เครื่องที่วิ่งอยู่ (carry-over) ไม่ต้อง setup ใหม่ สามารถ carry across SC/SO ได้ (configurable)
- **Capacity Optimization** — ตรวจจับเครื่องว่างที่ยังมี capacity เหลือ แล้วเติม order ของ item เดียวกันจาก SC อื่นเข้าไปอัตโนมัติ
- **Job/Week Capacity Limit** — จำกัดจำนวน job ต่อสัปดาห์ตาม Factory Type (PHET DOUBLE=33, SINGLE=44, OM=13)
- **Dynamic Setup Limit** — ปรับจำนวนเครื่อง setup ใหม่ตาม urgency ของ RDD (ยิ่งใกล้กำหนดส่ง ยิ่งเปิดเครื่องมาก)
- **Factory-aware Working Days** — วันทำงานแตกต่างตาม Factory (PHET DOUBLE=7d, SINGLE/OM=6d) พร้อม override สำหรับ week พิเศษ
- **Calendar Integration** — ใช้ปฏิทินแบบ Friday–Thursday week, กรองวันหยุดออก, รองรับข้ามปี
- **Fiber Type Detection** — ระบุ POLY/None POLY จาก YARN-USED อัตโนมัติ
- **OLD vs NEW Plan Comparison** — รวม booking เก่า (OLD) กับแผนใหม่ (NEW) ในไฟล์เดียว เปรียบเทียบ week-by-week
- **Pivot Table Generation** — สร้าง Excel PivotTable อัตโนมัติผ่าน win32com (Windows)
- **ML MC_GROUP Prediction** — ทำนาย MC_GROUP สำหรับ Item ใหม่ด้วย Linear SVM + TF-IDF (ถ้าเคยเห็น → lookup ตรง, ถ้าใหม่ → ML predict)
- **API Integration Layer** — โครงสร้างสำหรับดึงข้อมูลจาก API (JSON/Excel/CSV) พร้อมใช้งาน

---

## Data Flow

```
Order/*.xlsx ──→ Order.py ──→ data_plan/order_ready.xlsx ─┐
                                                          │
Booking/*.xlsx ─→ AVA_MC.py ─→ data_plan/booking_final_ready25.xlsx
                                  ├── DETAIL sheet         ├──→ Planning.py
                                  └── SUMMARY_MC_REMAIN    │
                                                          │
data/Cap/*.xlsx ──→ ITEM_Cap.py ──────────────────────────┤
data/Yarn/*.xlsx ─→ Yarn_Master.py ───────────────────────┤
data/MC/Master_MC_5.xlsx ─────────────────────────────────┤
Calendar.xlsx ────→ Calendar.py ──────────────────────────┘
                                                          │
                                                          ▼
                                              Planning.py (Main Engine)
                                                          │
                          ┌───────────────────────────────┼──────────────────────┐
                          ▼                               ▼                      ▼
          data_plan/weekly_production_plan.xlsx    PIVOT_PLAN sheet     weekly_production_plan_
                ├── PLAN                          (Excel PivotTable)    combined_filtered.xlsx
                └── REMAINING_JOBS                                      ├── PLAN (OLD+NEW)
                                                                        └── NO_CAP
```

---

## โครงสร้างโปรเจกต์

```
Planning-logic/
├── Planning.py          # Core: วางแผนผลิตรายสัปดาห์ + Capacity Optimization + Export
├── Order.py             # โหลด/กรอง Orders จาก Excel (ตัด CL-ORDERS, FQC, F-CL, COMKN)
├── AVA_MC.py            # คำนวณเครื่องว่าง (Available MC) + Shared Pool + Booking summary
├── Master_MC.py         # Lookup Capability Group จาก Master MC data
├── ITEM_Cap.py          # โหลด Item Capacity (CAP ทอ, REVOLUTION/WEIGHT, GUAGE)
├── Calendar.py          # ปฏิทินการทำงาน (Friday–Thursday week, วันหยุด)
├── Yarn_Master.py       # โหลด Yarn Master + ระบุ FIBER_TYPE (POLY/None POLY)
├── Logic.py             # (Reserved) Logic กลาง / utility functions
├── Train.py             # Train ML model (Linear SVM) สำหรับทำนาย MC_GROUP
├── predict.py           # Predict MC_GROUP ของ Item ใหม่ (CLI + function)
├── Api.py               # HTTP client สำหรับดึงข้อมูลจาก API (JSON/Excel/CSV)
├── model/               # โมเดล ML ที่ train แล้ว (.joblib)
├── data/                # ข้อมูล Master
│   ├── Cap/             #   Item Capacity (item_cap2025.xlsx)
│   ├── MC/              #   Master MC (Master_MC_5.xlsx), DataITEM_Master.xlsx
│   └── Yarn/            #   Yarn Master
├── data_plan/           # ข้อมูลที่เตรียมแล้ว + Output
│   ├── order_ready.xlsx
│   ├── booking_final_ready25.xlsx
│   ├── weekly_production_plan.xlsx
│   └── weekly_production_plan_combined_filtered.xlsx
├── Booking/             # ข้อมูล Booking ดิบ (ประวัติการผลิตจริง)
├── Order/               # ข้อมูล Order ดิบ
├── Calendar.xlsx        # ไฟล์ปฏิทินหลัก
├── requirements.txt     # Dependencies
└── README.md
```

---

## Configuration (Planning.py)

| Parameter | ค่า Default | คำอธิบาย |
|---|---|---|
| `SETUP_DAYS` | 3 | จำนวนวัน setup ต่อเครื่อง (cold start) |
| `SETUP_GAP_WEEK` | 2 | ถ้าผลิต item เดิมภายใน 2 week → ไม่ต้อง setup ใหม่ |
| `SKIP_WEEKS` | {16} | สัปดาห์ที่ข้าม (หยุด/ปิดโรงงาน) |
| `ALLOW_CARRYOVER_ACROSS_SO` | True | อนุญาต carryover ข้าม SC/SO NO |
| `USE_PROGRESSIVE_REDUCTION` | False | โหมดลดเครื่องแบบค่อยเป็นค่อยไป |

### Shared Machine Pool

เครื่องจักรบางกลุ่มใช้ร่วมกัน เช่น

| Pool | Total MC | Members |
|---|---|---|
| SKP_SKPTA_14 | 5 | SKP-14, SKPTA-14 |
| SKPLE_SKPTA_26 | 40 | SKPLE-26, SKPTA-26 |
| SKPLE_SKPTA_36 | 19 | SKPLE-36, SKPTA-36 |
| GAUGE22 | 65 | IBLTA-22, IBP-22, RAO-22, RAP-22, RAP60-22, RAP98-22, SYN-22 |
| GAUGE28 | 47 | IBLTA-28, RAP-28, RAP60-28, RAP98-28, SYN-28 |

---

## วิธีการรันโปรเจกต์

### 1. Clone & Setup

```bash
git clone https://github.com/ssenxl/Planning-logic.git
cd Planning-logic
pip install -r requirements.txt
```

### 2. เตรียมข้อมูล

```bash
# เตรียม Orders (กรอง Order Type + MC GROUP)
python Order.py

# คำนวณเครื่องว่าง + Booking summary
python AVA_MC.py
```

### 3. รันแผนการผลิต

```bash
python Planning.py
```

**Output:**
- `data_plan/weekly_production_plan.xlsx` — แผนใหม่ (PLAN + REMAINING_JOBS + PIVOT_PLAN)
- `data_plan/weekly_production_plan_combined_filtered.xlsx` — รวม OLD+NEW เปรียบเทียบ

### 4. ML Prediction (ทำนาย MC_GROUP)

```bash
# Train model
python Train.py

# Predict (CLI)
python predict.py
```

---

## Output Columns (PLAN sheet)

| Column | คำอธิบาย |
|---|---|
| `ITEM_CODE` | รหัสสินค้า |
| `SC_SO_NO` | เลข SC/SO |
| `MC_GROUP` | กลุ่มเครื่องจักร |
| `MC_GUAGE` | Gauge ของเครื่อง |
| `PLAN_WEEK` | สัปดาห์ที่วางแผนผลิต |
| `PRODUCE_QTY` | ปริมาณผลิต (units) |
| `REQUIRED_MC` | จำนวนเครื่องที่ต้องใช้ |
| `CARRYOVER_MC` | เครื่อง carry-over จาก week ก่อน |
| `NEW_MC` | เครื่อง setup ใหม่ |
| `SETUP_DAYS` | วัน setup จริง |
| `DAILY_CAPACITY` | กำลังผลิตต่อวันต่อเครื่อง (CAP ทอ) |
| `TARGET_KNIT` | สัปดาห์เป้าหมายทอเสร็จ (FG Week - 3) |
| `TARGET_STATUS` | ทัน / ไม่ทัน ตาม TARGET_KNIT |
| `PLAN_SOURCE` | NEW = แผนใหม่, OLD = booking เก่า |

---

## Dependencies

```
pandas
numpy
openpyxl
xlrd==1.2.0
scikit-learn
joblib
```

Optional: `pywin32` (สำหรับสร้าง Excel PivotTable บน Windows)

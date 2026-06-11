# Knit Plan AI — Planning Logic System

ระบบวางแผนการผลิตผ้าถักอัตโนมัติ (Knit Planning) ของบริษัท Nan Yang Textile  
พัฒนาด้วย Python — ดึงข้อมูลจาก Oracle DB → คำนวณ Machine Availability → วางแผนผลิตรายสัปดาห์ → Export Excel

---

## Pipeline

```
View_Stock.py    →  ดึง Stock จาก Oracle DB        → Stock/view_stock.xlsx
View_Booking.py  →  ดึง Booking จาก Oracle DB      → Booking/view_booking.xlsx
View_SC.py       →  ดึง SC Pending จาก Oracle DB   → Order/view_sc.xlsx
       ↓
Calendar.py      →  โหลดปฏิทิน (Friday–Thursday week)
Stock.py         →  ประมวลผล Stock + Target Stock
AVA_MC.py        →  คำนวณ Machine Availability     → data_plan/booking_final_ready25.xlsx
Order.py         →  กรอง/เตรียม Order              → data_plan/order_ready.xlsx
Planning.py      →  วางแผนผลิตหลัก                → data_plan/production_plan_DD-MM-YYYY.xlsx
```

---

## วิธีรัน

```powershell
# รัน pipeline ทั้งหมด
python run_all.py

# ข้าม step ที่ DB ไม่พร้อม
python run_all.py --skip View_Stock View_Booking View_SC

# เริ่มจาก step ที่ระบุ
python run_all.py --from AVA_MC

# ทำงานต่อแม้ step ก่อนหน้า fail
python run_all.py --ignore-errors
```

---

## ไฟล์ในโปรเจกต์

| ไฟล์ | หน้าที่ |
|------|---------|
| `run_all.py` | รัน pipeline ทั้งหมดตามลำดับ |
| `Calendar.py` | โหลดปฏิทิน — Friday–Thursday week, วันหยุด |
| `Stock.py` | ประมวลผล Stock + อ่าน Target Stock |
| `AVA_MC.py` | คำนวณ MC_USE, MC_REMAIN จาก Booking |
| `Order.py` | กรอง Order (ตัด CL, FQC, COMKN ออก) |
| `Planning.py` | Core planning engine — assign งานให้เครื่องรายสัปดาห์ |
| `ITEM_Cap.py` | โหลด Item Capacity (CAP ทอ, Revolution, Gauge) |
| `Master_MC.py` | Lookup Capability Group จาก MasterMC |

---

## External Files (ต้องมีก่อนรัน)

| ไฟล์ | ตำแหน่ง |
|------|---------|
| `MasterMC.xlsx` | SharePoint — Knit Plan (AI) |
| `Calendar.xlsx` | SharePoint — Knit Plan (AI) |
| `Item Special.xlsx` | SharePoint — Knit Plan (AI) |
| `Target_Stock.xlsx` | `Estimate_Core/` |
| `Itemcore.xlsx` | `data/Itemcore/` |

---

## Output (production_plan_DD-MM-YYYY.xlsx)

| Sheet | คำอธิบาย |
|-------|----------|
| `PLAN` | แผนหลัก (แดง = S9 routing, เหลือง = Cylinder Change) |
| `PLAN_NO_S9` | แผนเดียวกัน ไม่มี S9 routing |
| `REMAINING_JOBS` | งานที่ไม่มีเครื่องพอ |
| `SETUP_TRACKING` | ประวัติ setup แต่ละ item/MC_GROUP |
| `UNPLANNED` | Order ที่ระบบข้ามไม่ได้วางแผน |
| `CYLINDER_CHANGE` | สรุปการเปลี่ยน Gauge cylinder รายสัปดาห์ |
| `PIVOT_PLAN` | PivotTable (PLAN_WEEK × ITEM_CODE) |


---

## Dependencies

```
oracledb  pandas  openpyxl  xlrd  numpy  msal
```

Optional: `pywin32` (สร้าง Excel PivotTable บน Windows — ติดตั้งอัตโนมัติเมื่อรัน)

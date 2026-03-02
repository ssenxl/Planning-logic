# 📦 Planning-logic

**Planning-logic** คือชุดสคริปต์ Python สำหรับ **วิเคราะห์และวางแผนการผลิต (Production Planning)**  
โดยอ้างอิงจากข้อมูลคำสั่งซื้อ (Orders), ประวัติการผลิตจริง (Booking), ความสามารถของเครื่องจักร (Capacity)  
และปฏิทินการทำงาน เพื่อสร้างแผนการผลิตรายสัปดาห์ที่สอดคล้องกับข้อจำกัดจริงของโรงงาน

เหมาะสำหรับ:
- โรงงานสิ่งทอ / การผลิตที่มีหลาย Machine Group
- การวางแผนตาม RDD (Required Delivery Date)
- การจัดสรรเครื่องจักรตาม capacity จริง
- การเตรียมข้อมูลเพื่อใช้ต่อใน ERP / Excel

---

## 🎯 สิ่งที่ระบบทำได้

- รวมและวิเคราะห์ Orders ที่ซ้ำกัน
- คำนวณจำนวนเครื่องจักรที่ต้องใช้ต่อสัปดาห์
- ตรวจสอบเครื่องจักรที่พร้อมใช้งานจริง
- คำนึงถึง **Setup Days / Setup Status**
- ใช้ข้อมูล **Booking History** เพื่อหลีกเลี่ยงการ setup ซ้ำ
- สร้างแผนการผลิตรายสัปดาห์ในรูปแบบ Excel

---

## 📂 โครงสร้างโปรเจกต์

```
AI_plan/
├── Planning.py        # หลัก: สร้างแผนการผลิตรายสัปดาห์
├── Order.py           # โหลดและเตรียมข้อมูล Orders
├── AVA_MC.py          # คำนวณเครื่องจักรที่พร้อมใช้งาน
├── Master_MC.py       # ข้อมูล Master ของเครื่องจักร
├── ITEM_Cap.py        # ข้อมูล Capacity ต่อ Item
├── Calendar.py        # ปฏิทินการทำงาน
├── Yarn_Master.py     # ข้อมูล Yarn Master
├── Logic.py           # Logic กลาง / utility functions
├── Train.py           # Train โมเดล ML
├── predict.py         # ทำนายผล (ML Prediction)
└── model/             # โมเดลที่ train แล้ว (.joblib)
```

---

## 📥 ข้อมูลที่ต้องเตรียม (Input)

วางไฟล์ทั้งหมดไว้ในโฟลเดอร์ `data_plan`

ตัวอย่างไฟล์หลัก:
- `order_ready.xlsx`  
- `booking_final_ready.xlsx`  
- `item_cap.xlsx`  
- `calendar.xlsx`  

> ⚠️ ชื่อคอลัมน์ในไฟล์ Excel ต้องสอดคล้องกับที่โค้ดใช้งาน

---

## 🛠️ การติดตั้ง

ใช้ Python 3.9+ แนะนำให้ใช้ virtual environment

```bash
pip install pandas openpyxl xlrd numpy scikit-learn joblib
```

---

## 🚀 การใช้งาน

```bash
python Planning.py
```

ผลลัพธ์จะถูกบันทึกไว้ในโฟลเดอร์ `data_plan/` เป็นไฟล์ Excel

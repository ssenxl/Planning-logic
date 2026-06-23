# ===== Stage 1: build React frontend =====
FROM node:20-slim AS fe
WORKDIR /fe
COPY webapp/frontend/package*.json ./
RUN npm install --no-audit --no-fund
COPY webapp/frontend/ ./
RUN npm run build

# ===== Stage 2: python runtime (pipeline + webapp) =====
FROM python:3.12-slim
ENV TZ=Asia/Bangkok \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8
WORKDIR /app

# deps
COPY webapp/requirements-docker.txt ./
RUN pip install --no-cache-dir -r requirements-docker.txt

# pipeline code (ทุกไฟล์ .py ที่ root เล็ก — copy ทั้งหมดให้ import ครบ)
COPY *.py ./
# input data dirs ที่ pipeline อ่าน
COPY data ./data
COPY Estimate_Core ./Estimate_Core
COPY Yarn_Master ./Yarn_Master
COPY Datamining ./Datamining
# webapp backend + frontend build
COPY webapp/backend ./webapp/backend
COPY --from=fe /fe/dist ./webapp/frontend/dist
# config.ini สำหรับ container (ชี้ master ไป /data/knitplan/masters)
COPY webapp/config.docker.ini ./config.ini

# โฟลเดอร์ทำงาน/ผลลัพธ์ (ถูก mount ทับด้วย volume บาง path)
RUN mkdir -p Booking Stock Order data_plan webapp/logs /data/knitplan/config

EXPOSE 8080
WORKDIR /app/webapp/backend
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]

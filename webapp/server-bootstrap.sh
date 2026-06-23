#!/usr/bin/env bash
# รันบน server docker-webchat โดย deploy.ps1 — ตั้งโฟลเดอร์ + build + up
# ไม่ต้องใช้ root: ทุกอย่างอยู่ใต้ /home/scm/knitplan
set -euo pipefail

ROOT=/home/scm/knitplan
SRC=$ROOT/src
TGZ=/home/scm/knitplan_src.tgz

echo "==> เตรียมโฟลเดอร์ที่ $ROOT"
mkdir -p "$ROOT"/{src,masters,output,logs,config,env}

echo "==> แตกซอร์สลง $SRC"
rm -rf "$SRC"/webapp "$SRC"/*.py 2>/dev/null || true
mkdir -p "$SRC"
tar -xzf "$TGZ" -C "$SRC"

# ---- .env (Oracle creds) : ดึงจาก webchat .env ถ้ายังไม่มี ----
ENV_FILE=$ROOT/env/.env
if [ ! -f "$ENV_FILE" ]; then
  echo "==> สร้าง $ENV_FILE จาก Oracle creds ของ webchat"
  OU=$(grep -E '^ORACLE_USER=' /opt/webchat/env/.env | head -1 | cut -d= -f2- || true)
  OP=$(grep -E '^ORACLE_PASSWORD=' /opt/webchat/env/.env | head -1 | cut -d= -f2- || true)
  {
    echo "# Knit Plan env (auto-generated)"
    echo "SF5_USER=${OU:-hctr}"
    echo "SF5_PASSWORD=${OP:-HCTR#23}"
  } > "$ENV_FILE"
  chmod 600 "$ENV_FILE"
else
  echo "==> ใช้ $ENV_FILE เดิม (ไม่ทับ)"
fi

# ---- build + up (project แยก: knitplan) ----
cd "$SRC"
echo "==> docker compose build + up (project=knitplan)"
docker compose -p knitplan -f webapp/docker-compose.yml up -d --build

echo "==> เคลียร์ build cache เก่า"
docker builder prune -f >/dev/null 2>&1 || true

echo "==> สถานะ container"
docker ps --filter "name=knitplan-app" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo "==> เสร็จ: http://docker-webchat:8080"

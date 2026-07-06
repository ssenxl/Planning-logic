#!/usr/bin/env bash
# รันบน server docker-webchat โดย deploy.ps1 — ตั้งโฟลเดอร์ + build + up
# ไม่ต้องใช้ root: ทุกอย่างอยู่ใต้ /home/scm/knitplan
set -euo pipefail

ROOT=/home/scm/knitplan
SRC=$ROOT/src
TGZ=/home/scm/knitplan_src.tgz

echo "==> เตรียมโฟลเดอร์ที่ $ROOT"
mkdir -p "$ROOT"/{src,masters,output,logs,config,env,stock,booking,order,datamining}

echo "==> แตกซอร์สลง $SRC"
# คืนสิทธิ์เขียน (ของเดิมบางโฟลเดอร์เป็น read-only) แล้วล้างทิ้งทั้งหมดก่อนแตกใหม่
chmod -R u+w "$SRC" 2>/dev/null || true
rm -rf "$SRC"
mkdir -p "$SRC"
# --delay-directory-restore: tarball พก dir mode read-only (555) มาบางโฟลเดอร์
# เลื่อนการตั้งสิทธิ์ dir ไปท้ายสุด ไม่งั้น mkdir ลูกข้างในไม่ได้ แล้วคืนสิทธิ์เขียนให้ทั้งหมด
tar --delay-directory-restore -xzf "$TGZ" -C "$SRC"
chmod -R u+w "$SRC"

# ---- .env (Oracle creds) : knitplan ใช้บัญชี hctr เฉพาะของตัวเอง ----
# หมายเหตุ: ห้ามดึงจาก webchat .env — webchat ต่อ Oracle คนละบัญชี/คนละ DB
#          เอามาใช้กับ NYTG จะโดน ORA-01017 (view booking/stock ไม่ได้)
#          รหัสนี้ตรงกับ default ใน View_*.py ที่พิสูจน์แล้วว่า view ได้บน local
ENV_FILE=$ROOT/env/.env
if [ ! -f "$ENV_FILE" ]; then
  echo "==> สร้าง $ENV_FILE (Oracle creds = hctr)"
  {
    echo "# Knit Plan env (auto-generated)"
    echo "SF5_USER=hctr"
    echo "SF5_PASSWORD=HCTR#23"
  } > "$ENV_FILE"
  chmod 600 "$ENV_FILE"
else
  echo "==> ใช้ $ENV_FILE เดิม (ไม่ทับ)"
fi

# ---- merge OPENAI_* จาก webapp/.env ที่ ship มากับ tarball (ฟีเจอร์จ้างทอ AI) ----
# webapp/.env เป็น gitignore แต่ deploy.ps1 pack ขึ้น server ด้วย → ใช้เป็นแหล่ง key
# docker-compose อ่าน env จาก $ENV_FILE ไม่ใช่ src/webapp/.env จึงต้อง merge เข้ามา
SRC_ENV=$SRC/webapp/.env
if [ -f "$SRC_ENV" ] && grep -qE '^OPENAI_(API_KEY|MODEL|BASE_URL)=' "$SRC_ENV"; then
  grep -vE '^OPENAI_(API_KEY|MODEL|BASE_URL)=' "$ENV_FILE" > "$ENV_FILE.tmp" || true
  grep -E  '^OPENAI_(API_KEY|MODEL|BASE_URL)=' "$SRC_ENV" >> "$ENV_FILE.tmp"
  mv "$ENV_FILE.tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "==> merge OPENAI_* จาก $SRC_ENV เข้า $ENV_FILE แล้ว"
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

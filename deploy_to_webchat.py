#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Deploy Knit Plan ขึ้น server docker-webchat (Docker Compose project = knitplan; เข้าที่ http://docker-webchat:8080).

ทำงาน 4 ขั้น:
  1) tar โปรเจกต์ทั้ง repo ในเครื่อง (ตัด Booking/Stock/data_plan/model/node_modules/.git/... — เก็บ webapp/.env)
  2) SFTP อัปโหลด tar + server-bootstrap.sh ไปที่ home ของ scm บน server
  3) รัน bootstrap: แตกไฟล์ลง ~/knitplan/src (คงโฟลเดอร์ masters/output/config/... เดิม)
  4) docker compose -p knitplan up -d --build

ใช้:  python deploy_to_webchat.py
auth: ใช้ SSH key ~/.ssh/docker-webchat ถ้ามี ไม่งั้นใช้รหัสผ่าน (ตั้ง env WEBCHAT_PW ได้)
"""
import io
import os
import sys
import tarfile
import time

# บังคับ stdout เป็น UTF-8 กัน console ไทย (cp874) crash ตอน stream log ที่มี ✓/✔
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

try:
    import paramiko
except ImportError:
    sys.exit("ต้องติดตั้ง paramiko ก่อน:  pip install paramiko")

# ---- ตั้งค่า ----------------------------------------------------------------
HOST = "docker-webchat"
PORT = 22
USER = "scm"
PASSWORD = os.environ.get("WEBCHAT_PW", "Webchat.2026")
SSH_KEY = os.path.expanduser("~/.ssh/docker-webchat")

REMOTE_TGZ = "/home/scm/knitplan_src.tgz"            # path ที่ server-bootstrap.sh คาดหวัง
REMOTE_BOOTSTRAP = "/home/scm/knitplan_bootstrap.sh"

LOCAL_ROOT = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP = os.path.join(LOCAL_ROOT, "webapp", "server-bootstrap.sh")

# โฟลเดอร์ (ชื่อไหนก็ตามในทุกระดับ) ที่ไม่ต้องส่งขึ้น image — ชุดเดียวกับ webapp/deploy.ps1
# หมายเหตุ: ไม่ตัด .xlsx ทั่วไป เพราะ image ต้องใช้ data/Itemcore + Estimate_Core
EXCLUDE_DIRS = {"node_modules", ".git", ".claude", "dist", "build", "logs",
                "__pycache__", "model", "data_plan", "Booking", "Stock",
                "Order", "KnitPlan_Release"}
EXCLUDE_EXT = {".zip", ".pyc", ".log"}
EXCLUDE_FILES = {"plan_log.txt", "deploy_to_webchat.py"}
# ---------------------------------------------------------------------------


def _skip_dir(name: str) -> bool:
    return name in EXCLUDE_DIRS or name.startswith(".venv") or name == "venv"


def _skip_file(name: str) -> bool:
    if name in EXCLUDE_FILES:
        return True
    return os.path.splitext(name)[1].lower() in EXCLUDE_EXT


def build_tar() -> bytes:
    """tar.gz ทั้ง repo (ยกเว้น EXCLUDE) เก็บลง memory แล้วคืนเป็น bytes"""
    buf = io.BytesIO()
    n = 0
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for root, dirs, files in os.walk(LOCAL_ROOT):
            rel_root = os.path.relpath(root, LOCAL_ROOT)
            # ตัด dir ที่ไม่เอาออกจากการเดินต่อ (เร็วขึ้น + ไม่หลงเข้า node_modules)
            dirs[:] = [d for d in dirs if not _skip_dir(d)]
            for f in files:
                if _skip_file(f):
                    continue
                full = os.path.join(root, f)
                rel = f if rel_root == "." else os.path.join(rel_root, f)
                tar.add(full, arcname=rel.replace("\\", "/"))
                n += 1
    print(f"  แพ็ก {n} ไฟล์ ({buf.tell()/1024/1024:.1f} MB)")
    return buf.getvalue()


def connect() -> "paramiko.SSHClient":
    """ต่อ SSH: มี key ~/.ssh/docker-webchat ใช้ key ก่อน ไม่มีค่อยใช้รหัสผ่าน"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if os.path.exists(SSH_KEY):
        print(f"      ใช้ SSH key {SSH_KEY}")
        ssh.connect(HOST, port=PORT, username=USER, key_filename=SSH_KEY, timeout=30)
    else:
        print("      ใช้รหัสผ่าน (env WEBCHAT_PW)")
        ssh.connect(HOST, port=PORT, username=USER, password=PASSWORD, timeout=30)
    return ssh


def run(ssh: "paramiko.SSHClient", cmd: str, stream: bool = False) -> int:
    """สั่งคำสั่งบน server; stream=True ให้พิมพ์ output สด (สำหรับ build)"""
    # ไม่ใช้ get_pty เพื่อกัน spinner ของ docker compose spam เต็มจอ
    stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=False)
    if stream:
        for line in iter(stdout.readline, ""):
            sys.stdout.write(line)
            sys.stdout.flush()
    else:
        stdout.read()
    rc = stdout.channel.recv_exit_status()
    err = stderr.read().decode("utf-8", "replace").strip()
    if err and rc != 0:
        print(err, file=sys.stderr)
    return rc


def main() -> None:
    if not os.path.exists(BOOTSTRAP):
        sys.exit(f"ไม่พบ {BOOTSTRAP}")

    print("[1/4] แพ็กโปรเจกต์...")
    data = build_tar()

    print(f"[2/4] เชื่อมต่อ {USER}@{HOST} ...")
    ssh = connect()

    try:
        sftp = ssh.open_sftp()
        print("      อัปโหลด tar...")
        with sftp.open(REMOTE_TGZ, "wb") as f:
            f.set_pipelined(True)
            f.write(data)
        print("      อัปโหลด bootstrap...")
        sftp.put(BOOTSTRAP, REMOTE_BOOTSTRAP)
        sftp.close()

        print("[3/4 + 4/4] รัน bootstrap บน server (แตกไฟล์ + docker compose up -d --build)...")
        t0 = time.time()
        # sed ตัด \r ของไฟล์จาก Windows ก่อนรัน; PYTHONUTF8 กัน log ไทยเพี้ยน
        rc = run(ssh, (
            f"sed -i 's/\\r$//' {REMOTE_BOOTSTRAP} && "
            f"PYTHONUTF8=1 bash {REMOTE_BOOTSTRAP} && "
            f"rm -f {REMOTE_TGZ}"
        ), stream=True)
        if rc != 0:
            sys.exit("deploy ล้มเหลว — ดู error ด้านบน")
        print(f"\n✔ เสร็จใน {time.time()-t0:.0f}s — http://{HOST}:8080")
    finally:
        ssh.close()


if __name__ == "__main__":
    main()

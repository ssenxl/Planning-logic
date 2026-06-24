"""
add_user.py — เครื่องมือจัดการ user สำหรับ login เว็บ (ผู้ดูแลใช้เท่านั้น)

วิธีใช้ (รันในโฟลเดอร์ webapp/backend):
    python add_user.py <username>          เพิ่ม/ตั้งรหัสผ่าน user (พิมพ์รหัสแบบซ่อน)
    python add_user.py --list              แสดงรายชื่อ user ทั้งหมด
    python add_user.py --delete <username> ลบ user

รหัสผ่านถูกเก็บเป็น hash (PBKDF2) ใน users.json — ไม่เก็บ plain text
"""
import sys
import getpass

import auth


def add_user(username: str) -> None:
    username = username.strip()
    if not username:
        print("ชื่อ user ว่างไม่ได้")
        sys.exit(1)
    users = auth.load_users()
    if username in users:
        print(f"มี user '{username}' อยู่แล้ว — กำลังตั้งรหัสผ่านใหม่")
    pw1 = getpass.getpass("รหัสผ่านใหม่: ")
    if not pw1:
        print("รหัสผ่านว่างไม่ได้")
        sys.exit(1)
    pw2 = getpass.getpass("ยืนยันรหัสผ่าน: ")
    if pw1 != pw2:
        print("รหัสผ่านไม่ตรงกัน")
        sys.exit(1)
    users[username] = auth.hash_password(pw1)
    auth.save_users(users)
    print(f"บันทึก user '{username}' เรียบร้อย → {auth.USERS_FILE}")


def list_users() -> None:
    users = auth.load_users()
    if not users:
        print("ยังไม่มี user")
        return
    print(f"user ทั้งหมด ({len(users)}):")
    for name in sorted(users):
        print(f"  - {name}")


def delete_user(username: str) -> None:
    users = auth.load_users()
    if username not in users:
        print(f"ไม่พบ user '{username}'")
        sys.exit(1)
    del users[username]
    auth.save_users(users)
    print(f"ลบ user '{username}' เรียบร้อย")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)
    if args[0] == "--list":
        list_users()
    elif args[0] == "--delete":
        if len(args) < 2:
            print("ระบุชื่อ user ที่จะลบ: python add_user.py --delete <username>")
            sys.exit(1)
        delete_user(args[1])
    else:
        add_user(args[0])


if __name__ == "__main__":
    main()

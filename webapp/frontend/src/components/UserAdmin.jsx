import React, { useEffect, useState } from 'react'
import { api } from '../api.js'

// ตั้งค่าระบบ › ผู้ใช้งาน — เพิ่ม/รีเซ็ตรหัส/ลบ/เปลี่ยนสิทธิ์บัญชีที่เข้าเว็บได้
// กติกา (backend auth.py ตรวจซ้ำอีกชั้น หน้าเว็บแค่ซ่อนปุ่มที่กดไม่ได้):
//   admin      → จัดการได้แค่ user / yarn (เพิ่ม/รีเซ็ตรหัส/ลบ/สลับ user ↔ yarn)
//   superadmin → จัดการ admin ได้ด้วย (เพิ่ม/เลื่อน/ถอด/ลบ)
//   บัญชี superadmin แตะผ่านเว็บไม่ได้ · จัดการบัญชีตัวเองไม่ได้
//   เปลี่ยนรหัสตัวเองที่มุมขวาบนได้เฉพาะ superadmin — admin/yarn/user ต้องให้คนที่มีสิทธิ์รีเซ็ตให้ที่หน้านี้
//   yarn = user + สิทธิ์เรื่องด้าย (แก้ค่าหน้า Monitor Yarn, ปุ่มดึงข้อมูลด้าย)

// ชื่อที่โชว์บนจอ — role 'admin' แสดงเป็น "Knit" (user สั่ง 15 ต.ค.) ; คีย์ในระบบยังเป็น 'admin' เหมือนเดิม
const ROLE_LABEL = { superadmin: 'Super Admin', admin: 'Knit', yarn: 'Yarn', user: 'User' }
const ROLE_DESC = {
  admin: 'จะเข้าหน้าหลัก/Setting และเพิ่ม/ลบ User/Yarn ได้',
  yarn: 'ใช้งานเหมือน User + แก้ค่า Monitor Yarn และกดดึงข้อมูลด้ายได้',
  user: 'ใช้งานทั่วไป',
}
const MIN_PW = 6

export function canManage(myRole, me, u) {
  if (u.username === me || u.role === 'superadmin') return false
  if (u.role === 'admin') return myRole === 'superadmin'
  return myRole === 'admin' || myRole === 'superadmin'
}

export default function UserAdmin() {
  const [data, setData] = useState(null)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)
  const [form, setForm] = useState({ username: '', password: '', confirm: '', role: 'user' })
  const [resetFor, setResetFor] = useState(null)   // ชื่อ user ที่กำลังรีเซ็ตรหัส (เปิดหน้าต่าง)

  async function load() {
    try { setData(await api.users()) } catch (e) { setErr('โหลดรายชื่อไม่ได้: ' + e.message) }
  }
  useEffect(() => { load() }, [])

  async function run(fn, okText) {
    setBusy(true); setErr(''); setMsg('')
    try { await fn(); setMsg(okText); await load(); return true }
    catch (e) { setErr(e.message); return false }
    finally { setBusy(false) }
  }

  const formErr = !form.username.trim() ? ''
    : /\s/.test(form.username.trim()) ? 'ชื่อผู้ใช้ห้ามมีช่องว่าง'
    : form.password && form.password.length < MIN_PW ? `รหัสผ่านต้องยาวอย่างน้อย ${MIN_PW} ตัว`
    : form.confirm && form.password !== form.confirm ? 'ยืนยันรหัสผ่านไม่ตรงกัน'
    : ''
  const canAdd = form.username.trim() && form.password.length >= MIN_PW
    && form.password === form.confirm && !formErr && !busy

  async function add(e) {
    e.preventDefault()
    if (!canAdd) return
    const name = form.username.trim()
    const ok = await run(() => api.userCreate(name, form.password, form.role),
      `เพิ่มผู้ใช้ ${name} (${ROLE_LABEL[form.role]}) แล้ว — แจ้งชื่อและรหัสผ่านให้เจ้าของบัญชีได้เลย`)
    if (ok) setForm({ username: '', password: '', confirm: '', role: 'user' })
  }

  function del(u) {
    if (!window.confirm(`ลบบัญชี ${u.username}?\nคนนี้จะเข้าเว็บไม่ได้ทันที (ถ้าเปิดค้างอยู่จะหลุดออก)`)) return
    run(() => api.userDelete(u.username), `ลบบัญชี ${u.username} แล้ว`)
  }

  function changeRole(u, role) {
    const t = u.role === 'admin'
      ? `ถอด ${u.username} จาก ${ROLE_LABEL.admin} เป็น ${ROLE_LABEL[role]}?\n${ROLE_DESC[role]}`
      : `เปลี่ยน ${u.username} จาก ${ROLE_LABEL[u.role]} เป็น ${ROLE_LABEL[role]}?\n${ROLE_DESC[role]}`
    if (!window.confirm(t)) return
    run(() => api.userSetRole(u.username, role), `${u.username} เป็น ${ROLE_LABEL[role]} แล้ว`)
  }

  if (!data) return <div className="card"><div className="hint">{err || 'กำลังโหลดรายชื่อ...'}</div></div>

  const { users, me, my_role: myRole } = data
  const isSuper = myRole === 'superadmin'
  const roleCount = r => users.filter(u => u.role === r).length

  return (
    <div className="card cfggroupcard ua-card">
      <div className="cfggrouptop">
        <span className="cfggico">👤</span>
        <div>
          <h3>ผู้ใช้งาน</h3>
          <div className="cfggdesc">
            บัญชีที่เข้าเว็บนี้ได้ ({users.length} บัญชี) ·{' '}
            {isSuper
              ? 'คุณเป็น Super Admin — เพิ่ม/ถอด/ลบ Knit ได้'
              : 'Knit เพิ่ม/รีเซ็ตรหัส/ลบได้เฉพาะ User และ Yarn — ตั้ง Knit ต้องให้ Super Admin ทำ'}
          </div>
        </div>
      </div>

      {msg && <div className="ua-msg ok">{msg}</div>}
      {err && <div className="ua-msg bad">{err}</div>}

      <form className="ua-add" onSubmit={add} autoComplete="off">
        <div className="ua-addtitle">➕ เพิ่มผู้ใช้</div>
        <div className="ua-fields">
          <label>ชื่อผู้ใช้ (ใช้ login)
            <input value={form.username} placeholder="เช่น name.s@nanyangtextile.com"
              autoComplete="off"
              onChange={e => setForm(f => ({ ...f, username: e.target.value }))} />
          </label>
          <label>รหัสผ่าน
            <input type="password" value={form.password} autoComplete="new-password"
              placeholder={`อย่างน้อย ${MIN_PW} ตัว`}
              onChange={e => setForm(f => ({ ...f, password: e.target.value }))} />
          </label>
          <label>ยืนยันรหัสผ่าน
            <input type="password" value={form.confirm} autoComplete="new-password"
              onChange={e => setForm(f => ({ ...f, confirm: e.target.value }))} />
          </label>
          <label>สิทธิ์
            <select value={form.role} onChange={e => setForm(f => ({ ...f, role: e.target.value }))}>
              <option value="user">User — ใช้งานทั่วไป</option>
              <option value="yarn">Yarn — User + แก้ Monitor Yarn/ดึงข้อมูลด้าย</option>
              {isSuper && <option value="admin">Knit — เข้าหน้าหลัก/Setting</option>}
            </select>
          </label>
          <button className="primary ua-addbtn" type="submit" disabled={!canAdd}>
            {busy ? 'กำลังบันทึก...' : 'เพิ่มผู้ใช้'}
          </button>
        </div>
        {formErr && <div className="ua-formerr">{formErr}</div>}
      </form>

      <div className="ua-summary">
        {['superadmin', 'admin', 'yarn', 'user'].map(r => (
          <span key={r} className={'ua-sumchip ' + r}>
            {ROLE_LABEL[r]} <b>{roleCount(r)}</b>
          </span>
        ))}
      </div>

      <div className="ua-tablewrap">
        <table className="ua-table">
          <thead>
            <tr><th>ชื่อผู้ใช้</th><th>สิทธิ์</th><th>เพิ่มเมื่อ</th><th></th></tr>
          </thead>
          <tbody>
            {users.map(u => {
              const manage = canManage(myRole, me, u)
              return (
                <tr key={u.username} className={u.username === me ? 'ua-me' : ''}>
                  <td>
                    <div className="ua-name">
                      <span className={'ua-avatar ' + u.role}>{(u.username[0] || '?').toUpperCase()}</span>
                      <span className="ua-uname">{u.username}</span>
                      {u.username === me && <span className="ua-you">คุณ</span>}
                    </div>
                  </td>
                  <td>
                    {/* manage = จัดการบัญชีนี้ได้ → admin สลับ User ↔ Yarn ได้ ; ตัวเลือก Admin เฉพาะ superadmin */}
                    {manage
                      ? (
                        <select className={'ua-rolesel ' + u.role} value={u.role} disabled={busy}
                          onChange={e => changeRole(u, e.target.value)}>
                          <option value="user">User</option>
                          <option value="yarn">Yarn</option>
                          {isSuper && <option value="admin">Knit</option>}
                        </select>
                      )
                      : <span className={'ua-role ' + u.role}>
                          {u.role === 'superadmin' && '⭐ '}{ROLE_LABEL[u.role]}
                        </span>}
                  </td>
                  <td className="ua-meta">
                    {u.created_at
                      ? <>{u.created_at}{u.created_by && <span className="ua-by"> · โดย {u.created_by}</span>}</>
                      : <span className="ua-nil">—</span>}
                  </td>
                  <td className="ua-acts">
                    {manage && <>
                      <button className="ua-btn" disabled={busy} onClick={() => setResetFor(u.username)}>
                        🔑 รีเซ็ตรหัส
                      </button>
                      <button className="ua-btn danger" disabled={busy} onClick={() => del(u)}>
                        🗑 ลบ
                      </button>
                    </>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {resetFor && (
        <PasswordDialog
          title={`ตั้งรหัสผ่านใหม่ให้ ${resetFor}`}
          note="คนนี้จะหลุดออกจากระบบทุกจอ และต้อง login ด้วยรหัสใหม่"
          onCancel={() => setResetFor(null)}
          onSubmit={async (pw) => {
            const ok = await run(() => api.userResetPw(resetFor, pw),
              `ตั้งรหัสผ่านใหม่ให้ ${resetFor} แล้ว — แจ้งรหัสใหม่ให้เจ้าของบัญชี`)
            if (ok) setResetFor(null)
            return ok
          }} />
      )}
    </div>
  )
}


// หน้าต่างกรอกรหัสผ่าน — ใช้ทั้ง "รีเซ็ตรหัสให้คนอื่น" (ไม่ต้องรู้รหัสเดิม)
// และ "เปลี่ยนรหัสตัวเอง" (askOld = ต้องกรอกรหัสเดิม)
export function PasswordDialog({ title, note, askOld, onCancel, onSubmit }) {
  const [oldPw, setOldPw] = useState('')
  const [pw, setPw] = useState('')
  const [pw2, setPw2] = useState('')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState('')

  const problem = pw && pw.length < MIN_PW ? `รหัสผ่านต้องยาวอย่างน้อย ${MIN_PW} ตัว`
    : pw2 && pw !== pw2 ? 'ยืนยันรหัสผ่านไม่ตรงกัน' : ''
  const ok = (!askOld || oldPw) && pw.length >= MIN_PW && pw === pw2 && !busy

  async function submit(e) {
    e.preventDefault()
    if (!ok) return
    setBusy(true); setErr('')
    try {
      const r = await onSubmit(pw, oldPw)
      if (r === false) setBusy(false)
    } catch (ex) { setErr(ex.message); setBusy(false) }
  }

  return (
    <div className="ua-modalbg" onClick={() => !busy && onCancel()}>
      <form className="ua-modal" onClick={e => e.stopPropagation()} onSubmit={submit}>
        <h3>{title}</h3>
        {note && <p className="ua-note">{note}</p>}
        {askOld && (
          <label>รหัสผ่านเดิม
            <input type="password" autoFocus value={oldPw} autoComplete="current-password"
              onChange={e => setOldPw(e.target.value)} />
          </label>
        )}
        <label>รหัสผ่านใหม่
          <input type="password" autoFocus={!askOld} value={pw} autoComplete="new-password"
            placeholder={`อย่างน้อย ${MIN_PW} ตัว`} onChange={e => setPw(e.target.value)} />
        </label>
        <label>ยืนยันรหัสผ่านใหม่
          <input type="password" value={pw2} autoComplete="new-password"
            onChange={e => setPw2(e.target.value)} />
        </label>
        {(problem || err) && <div className="ua-formerr">{err || problem}</div>}
        <div className="ua-modalacts">
          <button type="button" onClick={onCancel} disabled={busy}>ยกเลิก</button>
          <button className="primary" type="submit" disabled={!ok}>
            {busy ? 'กำลังบันทึก...' : 'บันทึกรหัสผ่าน'}
          </button>
        </div>
      </form>
    </div>
  )
}

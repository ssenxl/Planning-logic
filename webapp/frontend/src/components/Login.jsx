import React, { useState } from 'react'
import { api, auth } from '../api.js'

export default function Login({ onLogin }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)

  async function submit(e) {
    e.preventDefault()
    setErr('')
    setBusy(true)
    try {
      const res = await api.login(username.trim(), password)
      auth.set(res.token)
      onLogin(res.role)
    } catch (e) {
      setErr(e.message || 'เข้าสู่ระบบไม่สำเร็จ')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login-page">
      <form className="login-card" onSubmit={submit}>
        <div className="login-logo">
          <img src="/logo.png" alt="Nan Yang Textile Group" />
        </div>
        <h1>Knit Plan</h1>
        <p className="login-sub">ระบบวางแผนการผลิต — กรุณาเข้าสู่ระบบ</p>

        <label className="login-field">
          <span>ชื่อผู้ใช้</span>
          <input value={username} onChange={e => setUsername(e.target.value)}
            autoFocus autoComplete="username" />
        </label>
        <label className="login-field">
          <span>รหัสผ่าน</span>
          <input type="password" value={password} onChange={e => setPassword(e.target.value)}
            autoComplete="current-password" />
        </label>

        {err && <div className="login-err">{err}</div>}

        <button type="submit" className="primary login-btn" disabled={busy || !username || !password}>
          {busy ? 'กำลังเข้าสู่ระบบ…' : 'เข้าสู่ระบบ'}
        </button>
      </form>
    </div>
  )
}

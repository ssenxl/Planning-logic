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
      onLogin(res.username)
    } catch (e) {
      setErr(e.message || 'เข้าสู่ระบบไม่สำเร็จ')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login-page">
      <form className="login-card" onSubmit={submit}>
        <div className="login-logo" aria-hidden="true">
          <svg viewBox="0 0 44 44" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="2.2" y="2.2" width="39.6" height="39.6" rx="11" stroke="currentColor" strokeWidth="2.4" />
            <g fill="currentColor">
              <path d="M22 30.5C17.6 25 17.6 16.6 22 10.8C26.4 16.6 26.4 25 22 30.5Z" />
              <path d="M22 30.5C17.6 25 17.6 16.6 22 10.8C26.4 16.6 26.4 25 22 30.5Z" transform="rotate(35 22 30.5)" opacity="0.9" />
              <path d="M22 30.5C17.6 25 17.6 16.6 22 10.8C26.4 16.6 26.4 25 22 30.5Z" transform="rotate(-35 22 30.5)" opacity="0.9" />
            </g>
          </svg>
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

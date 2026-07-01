import React, { useState, useEffect } from 'react'
import Dashboard from './components/Dashboard.jsx'
import Masters from './components/Masters.jsx'
import Database from './components/Database.jsx'
import Schedule from './components/Schedule.jsx'
import Outputs from './components/Outputs.jsx'
import Login from './components/Login.jsx'
import { auth, setOnUnauthorized } from './api.js'

const TABS = [
  { id: 'dashboard', label: 'แดชบอร์ด', icon: '▶' },
  { id: 'masters', label: 'แก้ Master', icon: '✎' },
  { id: 'database', label: 'ฐานข้อมูล', icon: '🗄' },
  { id: 'schedule', label: 'ตั้งเวลา', icon: '⏰' },
  { id: 'outputs', label: 'ผลลัพธ์', icon: '📄' },
]

export default function App() {
  const [tab, setTab] = useState('dashboard')
  const [authed, setAuthed] = useState(() => !!auth.get())

  // เมื่อ token หมดอายุ (API คืน 401) → เด้งกลับหน้า login
  useEffect(() => {
    setOnUnauthorized(() => setAuthed(false))
  }, [])

  function logout() {
    auth.clear()
    setAuthed(false)
  }

  if (!authed) {
    return <Login onLogin={() => setAuthed(true)} />
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">
            <img src="/logo.png" alt="Nan Yang Textile Group" />
          </span>
          <span className="brandtext">
            <b>Knit Plan</b>
            <span>ระบบวางแผนการผลิต</span>
          </span>
        </div>
        <nav className="tabs">
          {TABS.map(t => (
            <button key={t.id}
              className={'tab' + (tab === t.id ? ' active' : '')}
              onClick={() => setTab(t.id)}>
              <span className="ico">{t.icon}</span>{t.label}
            </button>
          ))}
        </nav>
        <button className="logout-btn" onClick={logout} title="ออกจากระบบ">
          <span className="ico">⎋</span>ออกจากระบบ
        </button>
      </header>
      <main className="content">
        {tab === 'dashboard' && <Dashboard />}
        {tab === 'masters' && <Masters />}
        {tab === 'database' && <Database />}
        {tab === 'schedule' && <Schedule />}
        {tab === 'outputs' && <Outputs />}
      </main>
    </div>
  )
}

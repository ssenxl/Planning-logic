import React, { useState, useEffect } from 'react'
import Dashboard from './components/Dashboard.jsx'
import KnitPlan from './components/KnitPlan.jsx'
import Data from './components/Data.jsx'
import OrderColor from './components/OrderColor.jsx'
import Masters from './components/Masters.jsx'
import Outputs from './components/Outputs.jsx'
import Login from './components/Login.jsx'

import { auth, setOnUnauthorized } from './api.js'

const TABS = [
  { id: 'dashboard', label: 'หน้าหลัก', icon: '🏠' },
  { id: 'plan', label: 'แผนผลิต', icon: '📅' },
  { id: 'data', label: 'DATA', icon: '📊' },
  { id: 'order-color', label: 'ดึง Order Color', icon: '🎨' },
  { id: 'masters', label: 'แก้ Master', icon: '✎' },
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
        {/* หน้าแผนผลิต mount ค้างไว้เสมอ ซ่อนด้วย CSS ตอนไม่ได้อยู่ tab นี้
            เพื่อไม่ให้ state (แผนที่แก้ค้าง + filter) หายเมื่อสลับ tab */}
        <div style={{ display: tab === 'plan' ? undefined : 'none' }}>
          <KnitPlan active={tab === 'plan'} />
        </div>
        {tab === 'data' && <Data />}
        {tab === 'order-color' && <OrderColor />}
        {tab === 'masters' && <Masters />}
        {tab === 'outputs' && <Outputs />}
      </main>

    </div>
  )
}

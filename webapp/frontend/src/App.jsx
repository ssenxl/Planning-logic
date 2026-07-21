import React, { useState, useEffect, useRef } from 'react'
import Dashboard from './components/Dashboard.jsx'
import KnitPlan from './components/KnitPlan.jsx'
import Data from './components/Data.jsx'
import OrderColor from './components/OrderColor.jsx'
import Masters from './components/Masters.jsx'
import Outputs from './components/Outputs.jsx'
import Login from './components/Login.jsx'

import { api, auth, setOnUnauthorized } from './api.js'

// แท็บ 'plan' เป็น dropdown: เมนูย่อยคือหน้าแผนผลิต + Order Color
const PLAN_MENU = [
  { id: 'plan', label: 'แผนผลิต', icon: '📅' },
  { id: 'order-color', label: 'Order Color', icon: '🎨' },
]
const TABS = [
  { id: 'dashboard', label: 'หน้าหลัก', icon: '🏠' },
  { id: 'plan', label: 'แผนผลิต', icon: '📅', menu: PLAN_MENU },
  { id: 'data', label: 'DATA', icon: '📊' },
  { id: 'masters', label: 'Master Data', icon: '✎' },
  { id: 'outputs', label: 'History', icon: '📄' },
]


export default function App() {
  const [tab, setTab] = useState('dashboard')
  const [authed, setAuthed] = useState(() => !!auth.get())
  const [role, setRole] = useState(null)            // 'admin' | 'user' | null(ยังไม่รู้)
  const [planOpen, setPlanOpen] = useState(false)   // เปิด/ปิด dropdown ของแท็บแผนผลิต
  const planTabRef = useRef(null)
  const isAdmin = role === 'admin'

  // เมื่อ token หมดอายุ (API คืน 401) → เด้งกลับหน้า login
  useEffect(() => {
    setOnUnauthorized(() => { setAuthed(false); setRole(null) })
  }, [])

  // โหลด role หลัง login / ตอนรีเฟรชหน้า (มี token ค้างใน localStorage แต่ยังไม่รู้ role)
  useEffect(() => {
    if (!authed || role) return
    api.me().then(m => setRole(m.role)).catch(() => setRole('user'))
  }, [authed, role])

  // user ธรรมดาไม่มีสิทธิ์หน้าหลัก → ถ้าค้างอยู่ที่ tab นั้นให้เด้งไปแผนผลิต
  useEffect(() => {
    if (role && role !== 'admin' && tab === 'dashboard') setTab('plan')
  }, [role, tab])

  // ปิด dropdown เมื่อคลิกนอกเมนู
  useEffect(() => {
    if (!planOpen) return
    function onDown(e) {
      if (planTabRef.current && !planTabRef.current.contains(e.target)) setPlanOpen(false)
    }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [planOpen])

  function logout() {
    auth.clear()
    setAuthed(false)
    setRole(null)
  }

  if (!authed) {
    return <Login onLogin={(r) => {
      setRole(r)
      setAuthed(true)
      if (r !== 'admin') setTab('plan')   // user ธรรมดาเริ่มที่หน้าแผนผลิต (ไม่มีหน้าหลัก)
    }} />
  }

  // แท็บที่ผู้ใช้คนนี้เห็น — หน้าหลัก (สั่งรัน pipeline) เฉพาะ admin
  const visibleTabs = TABS.filter(t => t.id !== 'dashboard' || isAdmin)

  // ป้ายที่โชว์บนปุ่มแท็บแผนผลิต = เมนูย่อยที่กำลังเปิดอยู่ (default = แผนผลิต)
  const planActive = PLAN_MENU.some(m => m.id === tab)
  const planCurrent = PLAN_MENU.find(m => m.id === tab) || PLAN_MENU[0]

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
          {visibleTabs.map(t => t.menu ? (
            <div key={t.id} className="tab-dd" ref={planTabRef}>
              <button
                className={'tab' + (planActive ? ' active' : '')}
                onClick={() => setPlanOpen(o => !o)}>
                <span className="ico">{planCurrent.icon}</span>{planCurrent.label}
                <span className={'caret' + (planOpen ? ' up' : '')}>▾</span>
              </button>
              {planOpen && (
                <div className="tab-menu">
                  {t.menu.map(m => (
                    <button key={m.id}
                      className={'tab-menu-item' + (tab === m.id ? ' active' : '')}
                      onClick={() => { setTab(m.id); setPlanOpen(false) }}>
                      <span className="ico">{m.icon}</span>{m.label}
                    </button>
                  ))}
                </div>
              )}
            </div>
          ) : (
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
        {tab === 'dashboard' && isAdmin && <Dashboard />}
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

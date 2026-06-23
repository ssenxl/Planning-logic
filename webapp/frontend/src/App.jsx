import React, { useState } from 'react'
import Dashboard from './components/Dashboard.jsx'
import Masters from './components/Masters.jsx'
import Schedule from './components/Schedule.jsx'
import Outputs from './components/Outputs.jsx'

const TABS = [
  { id: 'dashboard', label: 'แดชบอร์ด', icon: '▶' },
  { id: 'masters', label: 'แก้ Master', icon: '✎' },
  { id: 'schedule', label: 'ตั้งเวลา', icon: '⏰' },
  { id: 'outputs', label: 'ผลลัพธ์', icon: '📄' },
]

export default function App() {
  const [tab, setTab] = useState('dashboard')
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">Knit Plan <span>ระบบวางแผนการผลิตผ้าถัก</span></div>
        <nav className="tabs">
          {TABS.map(t => (
            <button key={t.id}
              className={'tab' + (tab === t.id ? ' active' : '')}
              onClick={() => setTab(t.id)}>
              <span className="ico">{t.icon}</span>{t.label}
            </button>
          ))}
        </nav>
      </header>
      <main className="content">
        {tab === 'dashboard' && <Dashboard />}
        {tab === 'masters' && <Masters />}
        {tab === 'schedule' && <Schedule />}
        {tab === 'outputs' && <Outputs />}
      </main>
    </div>
  )
}

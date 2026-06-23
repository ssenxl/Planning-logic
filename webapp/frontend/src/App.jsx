import React, { useState } from 'react'
import Dashboard from './components/Dashboard.jsx'
import Masters from './components/Masters.jsx'
import Database from './components/Database.jsx'
import Schedule from './components/Schedule.jsx'
import Outputs from './components/Outputs.jsx'

const TABS = [
  { id: 'dashboard', label: 'แดชบอร์ด', icon: '▶' },
  { id: 'masters', label: 'แก้ Master', icon: '✎' },
  { id: 'database', label: 'ฐานข้อมูล', icon: '🗄' },
  { id: 'schedule', label: 'ตั้งเวลา', icon: '⏰' },
  { id: 'outputs', label: 'ผลลัพธ์', icon: '📄' },
]

export default function App() {
  const [tab, setTab] = useState('dashboard')
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden="true">
            <svg viewBox="0 0 44 44" fill="none" xmlns="http://www.w3.org/2000/svg">
              <rect x="2.2" y="2.2" width="39.6" height="39.6" rx="11" stroke="currentColor" strokeWidth="2.4" />
              <g fill="currentColor">
                <path d="M22 30.5C17.6 25 17.6 16.6 22 10.8C26.4 16.6 26.4 25 22 30.5Z" />
                <path d="M22 30.5C17.6 25 17.6 16.6 22 10.8C26.4 16.6 26.4 25 22 30.5Z" transform="rotate(35 22 30.5)" opacity="0.9" />
                <path d="M22 30.5C17.6 25 17.6 16.6 22 10.8C26.4 16.6 26.4 25 22 30.5Z" transform="rotate(-35 22 30.5)" opacity="0.9" />
              </g>
              <g stroke="currentColor" strokeWidth="1.3" strokeLinecap="round">
                <line x1="22" y1="31" x2="22" y2="37.5" />
                <line x1="22" y1="32.5" x2="17" y2="36.5" />
                <line x1="22" y1="32.5" x2="27" y2="36.5" />
                <line x1="22" y1="33" x2="15" y2="34.5" />
                <line x1="22" y1="33" x2="29" y2="34.5" />
              </g>
            </svg>
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

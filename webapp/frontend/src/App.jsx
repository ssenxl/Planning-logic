import React, { useState, useEffect, useRef } from 'react'
import Dashboard from './components/Dashboard.jsx'
import McAvailability from './components/McAvailability.jsx'
import S9Availability from './components/S9Availability.jsx'
import JobSetup from './components/JobSetup.jsx'
import CoreStock from './components/CoreStock.jsx'
import ScLeadTime from './components/ScLeadTime.jsx'
import YarnStock from './components/YarnStock.jsx'
import YarnUsage from './components/YarnUsage.jsx'
import MonitorYarn from './components/MonitorYarn.jsx'
import KnitPlan from './components/KnitPlan.jsx'
import YarnPlan from './components/YarnPlan.jsx'
import Data from './components/Data.jsx'
import Masters from './components/Masters.jsx'
import PlanConfig from './components/PlanConfig.jsx'
import Outputs from './components/Outputs.jsx'
import Login from './components/Login.jsx'
import ErrorBoundary from './components/ErrorBoundary.jsx'
import { PasswordDialog } from './components/UserAdmin.jsx'

import { api, auth, setOnUnauthorized, withBase } from './api.js'
import * as live from './live.js'
import LiveBanner from './components/LiveBanner.jsx'

// หน้าแผนผลิต (เดิมแท็บนี้เป็น dropdown ที่มีหน้า Order Color เป็นเมนูย่อย —
// ถอดหน้านั้นทิ้งแล้ว 30 ก.ย. 2569 เพราะเลิกใช้ ; เหลือเมนูเดียวจึงไม่กาง dropdown)
const PLAN_MENU = [
  { id: 'plan', label: 'แผนผลิต', icon: '📅' },
]
// แท็บ 'mc-ava' เป็น dropdown: หน้าสรุปที่คำนวณสดจาก booking (ไม่ต้องรอรันแผน)
//   Machine Availability = เครื่องที่เหลือรายสัปดาห์ · Job Setup = โควตางานตั้งเครื่องรายสัปดาห์
//   Outsource Availability = เครื่องผู้รับจ้างทอที่เหลือรายสัปดาห์ (ชีท MC S9 เทียบงานจ้างทอในแผนล่าสุด)
//   Item Core Stock = สต๊อกผ้าดิบต่อ item เทียบ STOCK MIN/MAX (สดจากไฟล์ Stock)
//   SC Lead Time = วันที่เปิดใบ → เป้าทอ ห่างกี่สัปดาห์ (ทะเบียน SC สะสมจากทุกครั้งที่ดึง view SC)
//   Stock Yarn = ด้ายในคลังแยก ITEM → LOT → PO (ไฟล์จาก View_Yarn_Aging ที่ตั้งเวลาดึงทุกเช้า)
//   Yarn Usage = ด้าย/lot ไหนใช้ไปแล้วเท่าไหร่รายสัปดาห์ (booking × % ด้ายของใบ KP จาก View_Yarn_JoKnit)
//   (หน้า Yarn Ava ถอดออก 15 ต.ค. 2569 — user ไม่ใช้แล้ว ; ตัวคิดยอดใช้ได้ yarn_ava_live.py ยังอยู่ แผนผลิต/แผนด้ายใช้)
const DASH_MENU = [
  { id: 'mc-ava', label: 'Machine Availability', icon: '⚙️' },
  { id: 's9-ava', label: 'Outsource Availability', icon: '🏭' },
  { id: 'job-setup', label: 'Job Setup', icon: '🛠' },
  { id: 'core-stock', label: 'Item Core Stock', icon: '🧵' },
  { id: 'sc-lead', label: 'SC Lead Time', icon: '⏳' },
  { id: 'yarn-stock', label: 'Stock Yarn', icon: '🧶' },
  { id: 'yarn-usage', label: 'Yarn Usage', icon: '🧾' },
]
// แท็บทั้งหมดที่ใช้ layout เต็มจอ (.content.full) — หน้า dashboard ที่คำนวณสดจาก booking + แผนด้าย (ตารางสัปดาห์กว้าง)
const FULL_TABS = [...DASH_MENU.map(m => m.id), 'yarn-plan']
// แท็บที่เห็นเฉพาะ admin — หน้าหลัก (สั่งรัน pipeline) และหน้าที่แก้ค่าของระบบ
const ADMIN_TABS = ['dashboard', 'plan-config']
// แท็บที่ต้องมีสิทธิ์เรื่องด้าย (yarn / admin / superadmin)
const YARN_TABS = ['yarn-monitor']
const TABS = [
  { id: 'dashboard', label: 'หน้าหลัก', icon: '🏠' },
  { id: 'mc-ava', label: 'Dashboard', icon: '📊', menu: DASH_MENU },
  // ตั้งค่าของงานติดตามด้าย (เช่น YARN_WEIGHT ขั้นต่ำที่หน้า Stock Yarn ใช้ลงสี) — หน้าแก้ค่า ไม่ใช่ dashboard
  // user ขอแยกเป็นปุ่มของตัวเอง ; ตัวหน้า Stock Yarn ยังอยู่ใต้ Dashboard ตามเดิม
  { id: 'yarn-monitor', label: 'Monitor Yarn', icon: '🧶' },
  // เหลือเมนูย่อยรายการเดียว → เป็นปุ่มธรรมดา ไม่ต้องมี dropdown
  { id: 'plan', label: 'แผนผลิต', icon: '📅', menu: PLAN_MENU.length > 1 ? PLAN_MENU : undefined },
  // แผนทอใช้ด้ายอะไร สัปดาห์ไหน เท่าไหร่ + ล็อก lot ให้ SC — เดิมเป็นโหมดสลับในหน้าแผนผลิต
  // user ขอแยกเป็นหน้าของตัวเอง 15 ต.ค. 2569 (ปุ่มแยก ถัดจากแผนผลิต)
  { id: 'yarn-plan', label: 'แผนด้าย', icon: '🧶' },
  { id: 'data', label: 'DATA', icon: '📊' },
  { id: 'masters', label: 'Master Data', icon: '✎' },
  // กติกาการวางแผน (setup / ทรงเครื่อง / งานสี / จ้างทอ ฯลฯ) — แก้ได้เฉพาะ admin
  { id: 'plan-config', label: 'Setting', icon: '⚙️' },
  { id: 'outputs', label: 'History', icon: '📄' },
]


export default function App() {
  const [tab, setTab] = useState('dashboard')
  const [authed, setAuthed] = useState(() => !!auth.get())
  const [role, setRole] = useState(null)            // 'superadmin' | 'admin' | 'yarn' | 'user' | null(ยังไม่รู้)
  const [me, setMe] = useState('')                  // ชื่อผู้ใช้ที่ login อยู่ (โชว์ที่ปุ่มมุมขวาบน)
  const [pwOpen, setPwOpen] = useState(false)       // หน้าต่างเปลี่ยนรหัสผ่านตัวเอง
  const [openMenu, setOpenMenu] = useState(null)    // id ของแท็บที่กาง dropdown อยู่ (กางได้ทีละอัน)
  // กดปุ่ม "ไปหน้าแก้ไข" จากแท็บ Master Data (หน้าดูอย่างเดียว) → เปิดตั้งค่าระบบที่หมวดข้อมูลหลักเลย
  const [cfgStart, setCfgStart] = useState(null)
  const menuTabRef = useRef(null)                   // element ของแท็บที่กางอยู่ — ใช้เช็คคลิกนอกเมนู
  // หน้าแผนด้าย mount ครั้งแรกตอนเปิดเข้ามา แล้วค้างไว้ (ไม่โหลดตอน login ให้ทุกคน ; สลับไปมาไม่ต้องรอคิดใหม่)
  const yarnPlanSeen = useRef(false)
  if (tab === 'yarn-plan') yarnPlanSeen.current = true
  // superadmin = admin ที่ตั้ง admin คนอื่นได้ → ทุกหน้าที่ admin เห็น superadmin ก็เห็น
  const isAdmin = role === 'admin' || role === 'superadmin'
  // yarn = user + สิทธิ์เรื่องด้าย (แก้ Monitor Yarn / ปุ่มดึงข้อมูลด้าย) — admin ได้ด้วยเสมอ
  const canYarn = isAdmin || role === 'yarn'

  // เมื่อ token หมดอายุ (API คืน 401) → เด้งกลับหน้า login
  useEffect(() => {
    setOnUnauthorized(() => { setAuthed(false); setRole(null) })
  }, [])

  // สายข่าว "มีคนแก้ข้อมูล" — เส้นเดียวต่อแท็บ ใช้ร่วมกันทุกหน้า (ดู live.js)
  useEffect(() => {
    if (!authed) { live.stop(); return }
    live.start()
    return () => live.stop()
  }, [authed])

  // โหลด role หลัง login / ตอนรีเฟรชหน้า (มี token ค้างใน localStorage แต่ยังไม่รู้ role)
  useEffect(() => {
    if (!authed || (role && me)) return
    // บอก live.js ด้วยว่าจอนี้เป็นใคร — ข่าวที่เกิดจากการเซฟของตัวเองจะได้ไม่เด้งใส่ตัวเอง
    api.me().then(m => { live.setMe(m.username); setMe(m.username); setRole(m.role) })
      .catch(() => setRole('user'))
  }, [authed, role, me])

  // user ธรรมดาไม่มีสิทธิ์หน้าหลัก/ตั้งค่าระบบ/Monitor Yarn → ถ้าค้างอยู่ที่ tab นั้นให้เด้งไปแผนผลิต
  // (yarn ไม่มีหน้าหลัก → เด้งไป Stock Yarn ที่เป็นงานของตัวเอง)
  useEffect(() => {
    if (!role) return
    if ((!isAdmin && ADMIN_TABS.includes(tab)) || (!canYarn && YARN_TABS.includes(tab))) {
      setTab(role === 'yarn' ? 'yarn-stock' : 'plan')
    }
  }, [role, tab, isAdmin, canYarn])

  // ออกจากแท็บตั้งค่าระบบแล้วลืมจุดเริ่มต้นที่สั่งไว้ — ไม่งั้นครั้งหน้าที่เข้ามาเองจะเด้งไป
  // หมวดข้อมูลหลักทุกครั้ง ทั้งที่ผู้ใช้อยากดูค่ากติกา
  useEffect(() => { if (tab !== 'plan-config' && cfgStart) setCfgStart(null) }, [tab, cfgStart])

  // ปิด dropdown เมื่อคลิกนอกเมนู
  useEffect(() => {
    if (!openMenu) return
    function onDown(e) {
      if (menuTabRef.current && !menuTabRef.current.contains(e.target)) setOpenMenu(null)
    }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [openMenu])

  function logout() {
    auth.clear()
    yarnPlanSeen.current = false
    setAuthed(false)
    setRole(null)
    setMe('')
  }

  if (!authed) {
    return <Login onLogin={(r) => {
      setRole(r)
      setAuthed(true)
      // user ธรรมดาเริ่มที่หน้าแผนผลิต (ไม่มีหน้าหลัก) · yarn เริ่มที่ Stock Yarn
      if (r === 'yarn') setTab('yarn-stock')
      else if (r !== 'admin' && r !== 'superadmin') setTab('plan')
    }} />
  }

  // แท็บที่ผู้ใช้คนนี้เห็น — หน้าหลัก (สั่งรัน pipeline) และตั้งค่าระบบ (กติกาการวางแผน) เฉพาะ admin
  // · Monitor Yarn เฉพาะคนที่มีสิทธิ์ด้าย
  const visibleTabs = TABS.filter(t =>
    (!ADMIN_TABS.includes(t.id) || isAdmin) && (!YARN_TABS.includes(t.id) || canYarn))


  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">
            <img src={withBase('/logo.png')} alt="Nan Yang Textile Group" />
          </span>
          <span className="brandtext">
            <b>Knit Plan</b>
            <span>ระบบวางแผนการผลิต</span>
          </span>
        </div>
        <nav className="tabs">
          {visibleTabs.map(t => t.menu ? (
            // ผูก ref เฉพาะแท็บที่กางอยู่ — ตัวจับ "คลิกนอกเมนู" ต้องรู้แค่กล่องที่เปิดค้าง
            <div key={t.id} className="tab-dd" ref={openMenu === t.id ? menuTabRef : null}>
              <button
                className={'tab' + (t.menu.some(m => m.id === tab) ? ' active' : '')}
                onClick={() => setOpenMenu(o => (o === t.id ? null : t.id))}>
                {/* ป้ายบนปุ่มคงที่เป็นชื่อกลุ่ม (เช่น Dashboard) — หน้าที่เลือกอยู่ดูได้จากติ๊กในเมนูย่อย
                    ไม่เปลี่ยนป้ายตามเมนูย่อย ไม่งั้นผู้ใช้หาปุ่มกลุ่มไม่เจอเพราะชื่อเปลี่ยนไปเรื่อย */}
                <span className="ico">{t.icon}</span>{t.label}
                <span className={'caret' + (openMenu === t.id ? ' up' : '')}>▾</span>
              </button>
              {openMenu === t.id && (
                <div className="tab-menu">
                  {t.menu.map(m => (
                    <button key={m.id}
                      className={'tab-menu-item' + (tab === m.id ? ' active' : '')}
                      onClick={() => { setTab(m.id); setOpenMenu(null) }}>
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
        {/* ชื่อคนที่ login อยู่ — กดแล้วเปลี่ยนรหัสผ่านตัวเอง เฉพาะ superadmin
            (admin/user เปลี่ยนรหัสเองไม่ได้ ต้องให้ superadmin รีเซ็ตให้ที่หน้าผู้ใช้งาน) */}
        {role === 'superadmin' && (
          <button className="logout-btn me-btn" onClick={() => setPwOpen(true)}
            title="เปลี่ยนรหัสผ่านของฉัน">
            <span className="ico">🔑</span>{me || 'บัญชีของฉัน'}
          </button>
        )}
        <button className="logout-btn" onClick={logout} title="ออกจากระบบ">
          <span className="ico">⎋</span>ออกจากระบบ
        </button>
      </header>
      {pwOpen && (
        <PasswordDialog askOld
          title="เปลี่ยนรหัสผ่านของฉัน"
          note={`บัญชี ${me} · จอนี้ใช้งานต่อได้เลย แต่จออื่นที่ login ค้างไว้จะต้อง login ใหม่`}
          onCancel={() => setPwOpen(false)}
          onSubmit={async (pw, oldPw) => {
            const r = await api.changeMyPassword(oldPw, pw)
            if (r.token) auth.set(r.token)   // ใบเดิมตกทันทีที่รหัสเปลี่ยน — เปลี่ยนเป็นใบใหม่
            setPwOpen(false)
            window.alert('เปลี่ยนรหัสผ่านแล้ว')
          }} />
      )}
      {/* ครอบด้วย ErrorBoundary แยก 2 ก้อน: แผนผลิต (mount ค้าง) กับแท็บอื่น (key=tab → สลับแท็บ
          แล้ว error เคลียร์เอง) — error ที่หน้าหนึ่งจะไม่ทำให้ทั้งแอปกลายเป็นหน้าขาว */}
      {/* หน้า Dashboard (Machine Availability / Job Setup) ใช้พื้นที่เต็มจอ (ปลด max-width/padding ของ .content) */}
      {/* แถบบอกว่าใครเพิ่งแก้อะไร — ขึ้นทุกจอทันทีที่มีคนเซฟ ไม่ต้องรอใครบอกกัน */}
      <LiveBanner />
      <main className={'content' + (FULL_TABS.includes(tab) ? ' full' : '')}>
        {/* หน้าแผนผลิต mount ค้างไว้เสมอ ซ่อนด้วย CSS ตอนไม่ได้อยู่ tab นี้
            เพื่อไม่ให้ state (แผนที่แก้ค้าง + filter) หายเมื่อสลับ tab */}
        <div style={{ display: tab === 'plan' ? undefined : 'none' }}>
          <ErrorBoundary>
            <KnitPlan active={tab === 'plan'} />
          </ErrorBoundary>
        </div>
        {/* แผนด้าย ค้างไว้แบบเดียวกัน (หลังเปิดครั้งแรก) — คำค้น/ตัวกรองไม่หาย ; ตอนซ่อนไม่คิดใหม่ตามข่าว แต่จดไว้คิดตอนกลับมา
            canLock = ล็อก/ปลดด้าย lot ให้ SC ได้ (สิทธิ์ yarn / admin) ; คนอื่นดูอย่างเดียว */}
        {yarnPlanSeen.current && (
          <div className="yp-page" style={{ display: tab === 'yarn-plan' ? undefined : 'none' }}>
            <ErrorBoundary>
              <YarnPlan active={tab === 'yarn-plan'} canLock={canYarn} />
            </ErrorBoundary>
          </div>
        )}
        <ErrorBoundary key={tab}>
          {tab === 'dashboard' && isAdmin && <Dashboard />}
          {tab === 'mc-ava' && <McAvailability />}
          {tab === 's9-ava' && <S9Availability />}
          {tab === 'job-setup' && <JobSetup />}
          {tab === 'core-stock' && <CoreStock />}
          {tab === 'sc-lead' && <ScLeadTime />}
          {tab === 'yarn-stock' && <YarnStock canRun={canYarn} />}
          {/* canRun = เห็นปุ่ม "ดึงข้อมูลด้าย" (ซ่อนจาก user ธรรมดาที่ระดับหน้าจอ — /api/run เองเปิดให้ทุกคนเหมือนปุ่มรันอื่น) */}
          {tab === 'yarn-usage' && <YarnUsage canRun={canYarn} />}
          {tab === 'yarn-monitor' && canYarn && <MonitorYarn />}
          {tab === 'data' && <Data canEdit={isAdmin} />}
          {/* Master Data = ดูอย่างเดียว · การแก้ทั้งหมดย้ายไปอยู่ใน "ตั้งค่าระบบ › ข้อมูลหลัก" (admin) */}
          {tab === 'masters' && (
            <Masters mode="view" canEdit={isAdmin}
              onGoEdit={() => { setCfgStart('master'); setTab('plan-config') }} />
          )}
          {tab === 'plan-config' && isAdmin && <PlanConfig startAt={cfgStart} role={role} />}
          {tab === 'outputs' && <Outputs />}
        </ErrorBoundary>
      </main>

    </div>
  )
}

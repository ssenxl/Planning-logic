import React, { useMemo, useState } from 'react'
import WorkDayPanel from './WorkDayPanel.jsx'

// มุมมองปฏิทินของชีท Calendar — คลิกวันเพื่อสลับ ทำงาน (status=1) ↔ วันหยุด (status=0)
// แก้ค่าลงใน grid เดิมของ Masters โดยตรง → ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน

const MONTHS_TH = ['ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.', 'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.']
const WEEKDAYS_TH = ['จ', 'อ', 'พ', 'พฤ', 'ศ', 'ส', 'อา']

// คีย์วันแบบ local (ไม่ใช้ toISOString ที่เป็น UTC — กันวันเลื่อน)
export function dayKey(d) {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

// ค่าในเซลล์ DATE → "YYYY-MM-DD" (รองรับทั้ง ISO string จาก backend และค่าที่ผู้ใช้พิมพ์เอง)
export function cellDayKey(v) {
  if (v == null || v === '') return null
  const s = String(v).trim()
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`
  const d = new Date(s)
  return isNaN(d.getTime()) ? null : dayKey(d)
}

// ค่า status → 1 = ทำงาน, 0 = วันหยุด (รองรับข้อความไทย "0 วันหยุด" / "1 ทำงาน")
function statusOf(v) {
  const s = String(v ?? '').trim()
  if (s === '') return null
  const n = Number(s)
  if (!isNaN(n)) return Math.round(n) === 0 ? 0 : 1
  return s.includes('หยุด') || s.toLowerCase().includes('holiday') ? 0 : 1
}

// วันทั้งหมดที่ต้องวาดในเดือน (เริ่มวันจันทร์ เติมวันของเดือนข้างเคียงให้เต็มแถว)
function monthGrid(year, month) {
  const first = new Date(year, month, 1)
  const lastDay = new Date(year, month + 1, 0).getDate()
  const lead = (first.getDay() + 6) % 7          // จันทร์ = 0
  const cells = []
  for (let i = 0; i < lead; i++) cells.push({ date: new Date(year, month, i - lead + 1), cur: false })
  for (let i = 1; i <= lastDay; i++) cells.push({ date: new Date(year, month, i), cur: true })
  while (cells.length % 7 !== 0) {
    cells.push({ date: new Date(year, month + 1, cells.length - lead - lastDay + 1), cur: false })
  }
  return cells
}

function MiniMonth({ year, month, days, today, onToggle }) {
  const cells = monthGrid(year, month)
  return (
    <div className="calmonth">
      <div className="calmhead">{MONTHS_TH[month]} {year + 543}</div>
      <div className="calgrid">
        {WEEKDAYS_TH.map((d, i) => (
          <div key={d} className={'caldow' + (i >= 5 ? ' wknd' : '')}>{d}</div>
        ))}
        {cells.map(({ date, cur }, i) => {
          const key = dayKey(date)
          const info = cur ? days.get(key) : null
          const sun = date.getDay() === 0        // อาทิตย์หยุดประจำอยู่แล้ว → ไม่ต้องเน้นสีแดง
          const cls = ['calday']
          if (!cur) cls.push('out')
          else if (!info) cls.push('missing')                 // ไม่มีแถวนี้ในไฟล์ → กดไม่ได้
          else {
            cls.push(info.status === 0 ? (sun ? 'sun' : 'off') : 'work')
            if (info.changed) cls.push('pending')             // แก้แล้วยังไม่บันทึก
          }
          if (cur && key === today) cls.push('today')
          const title = !cur ? ''
            : !info ? `${key} — ไม่มีวันนี้ในไฟล์ Calendar (เพิ่มแถวได้ในมุมมองตาราง)`
              : `${key}${info.week ? ` · W${info.week}` : ''} — ${info.status === 0 ? (sun ? 'วันอาทิตย์ (หยุดประจำ)' : 'วันหยุด') : 'วันทำงาน'}${info.changed ? ' (ยังไม่บันทึก)' : ''}`
          return (
            <button key={i} type="button" className={cls.join(' ')} title={title}
              disabled={!cur || !info}
              onClick={() => info && onToggle(info.ri, info.status === 0 ? 1 : 0)}>
              {date.getDate()}
            </button>
          )
        })}
      </div>
    </div>
  )
}

export default function CalendarEditor({ grid, dateCi, statusCi, weekCi, isChanged, onToggle }) {
  // วันทั้งหมดในไฟล์: "YYYY-MM-DD" -> { ri, status, week, changed }
  const days = useMemo(() => {
    const m = new Map()
    grid.rows.forEach((row, ri) => {
      const key = cellDayKey(row[dateCi])
      const st = statusOf(row[statusCi])
      if (!key || st == null) return
      m.set(key, {
        ri, status: st,
        week: weekCi >= 0 ? String(row[weekCi] ?? '').trim() : '',
        changed: isChanged(ri, statusCi),
      })
    })
    return m
  }, [grid, dateCi, statusCi, weekCi, isChanged])

  const years = useMemo(() => {
    const s = new Set()
    for (const k of days.keys()) s.add(Number(k.slice(0, 4)))
    return [...s].sort((a, b) => a - b)
  }, [days])

  const thisYear = new Date().getFullYear()
  const [year, setYear] = useState(() => (years.includes(thisYear) ? thisYear : years[0] ?? thisYear))
  const yi = years.indexOf(year)

  // สรุปของปีที่กำลังดู — "วันหยุด" ไม่นับวันอาทิตย์ (หยุดประจำอยู่แล้ว)
  const stat = useMemo(() => {
    let off = 0, work = 0, pending = 0
    for (const [k, v] of days) {
      if (Number(k.slice(0, 4)) !== year) continue
      const [y, m, d] = k.split('-').map(Number)
      const sun = new Date(y, m - 1, d).getDay() === 0
      if (v.status === 0) { if (!sun) off++ } else work++
      if (v.changed) pending++
    }
    return { off, work, pending }
  }, [days, year])

  const today = dayKey(new Date())
  const [showWorkday, setShowWorkday] = useState(false)

  return (
    <div className="calview">
      <div className="calbar">
        <button className={'wdtoggle' + (showWorkday ? ' on' : '')} onClick={() => setShowWorkday(v => !v)}>
          ⚙ วันทำงานตามกลุ่มเครื่อง
        </button>
        <div className="calyear">
          <button disabled={yi <= 0} onClick={() => setYear(years[yi - 1])} title="ปีก่อนหน้า">‹</button>
          <span>{year} / {year + 543}</span>
          <button disabled={yi < 0 || yi >= years.length - 1} onClick={() => setYear(years[yi + 1])} title="ปีถัดไป">›</button>
        </div>
        <span className="calstat">
          วันหยุด <b className="off">{stat.off}</b> วัน · วันทำงาน <b className="work">{stat.work}</b> วัน
          {stat.pending > 0 && <> · <b className="pend">แก้ไว้ {stat.pending} วัน ยังไม่บันทึก</b></>}
        </span>
        <span className="callegend">
          <span className="lg off" /> วันหยุด
          <span className="lg sun" /> อาทิตย์
          <span className="lg work" /> วันทำงาน
          <span className="lg pending" /> ยังไม่บันทึก
        </span>
      </div>

      {showWorkday && <WorkDayPanel />}

      <div className="calmonths">
        {Array.from({ length: 12 }, (_, m) => (
          <MiniMonth key={m} year={year} month={m} days={days} today={today} onToggle={onToggle} />
        ))}
      </div>

      <div className="gridhelp">
        คลิกวัน = สลับ วันทำงาน ↔ วันหยุด (คอลัมน์ status 1 ↔ 0) · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ · วันที่ไม่มีในไฟล์กดไม่ได้ — เพิ่มแถวได้ในมุมมองตาราง
      </div>
    </div>
  )
}

import React, { useEffect, useState } from 'react'
import { api, withBase } from '../api.js'

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}
function fmtDate(ms) {
  return new Date(ms).toLocaleDateString('th-TH', { dateStyle: 'medium' })
}
function fmtHM(ms) {
  const d = new Date(ms)
  return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0')
}
function dayKey(ms) {
  const d = new Date(ms)
  return d.getFullYear() + '-' + d.getMonth() + '-' + d.getDate()
}

// เวลารันจากชื่อไฟล์ (..._{วัน}-{เดือน}-{ปีพ.ศ.}_{HHMM}.xlsx) — ไม่ใช้ mtime
// เพราะไฟล์แผนถูกเขียนซ้ำทีหลังได้ (mtime เลยช้ากว่าเวลารันจริง) ; อ่านชื่อไม่ได้ค่อยถอยไปใช้ mtime
function runTs(f) {
  const m = /_(\d{1,2})-(\d{1,2})-(\d{4})_(\d{2})(\d{2})\.xlsx$/i.exec(f.name)
  if (!m) return f.mtime * 1000
  return new Date(+m[3] - 543, +m[2] - 1, +m[1], +m[4], +m[5]).getTime()
}

// ไฟล์ล่าสุดที่เกิดก่อน/พร้อม ts (list เรียงใหม่→เก่าแล้ว)
function latestAtOrBefore(list, ts) {
  return list.find(x => x.ts <= ts) || null
}

// 1 แถว = 1 รอบแผน : จับคู่ booking/SC ตัวล่าสุดที่เกิดก่อนแผน (= ชุดที่ Replan หยิบไปใช้
// เพราะ run_all รัน View_SC → AVA_MC → Replan) ; booking/SC ที่ไม่มีแผนไหนใช้ ขึ้นแถวของตัวเอง
function buildRows(plans, bookings, scs) {
  const tag = list => list.map(f => ({ ...f, ts: runTs(f) })).sort((a, b) => b.ts - a.ts)
  const P = tag(plans), B = tag(bookings), S = tag(scs)
  const usedB = new Set(), usedS = new Set()
  const rows = P.map(p => {
    const b = latestAtOrBefore(B, p.ts)
    const s = latestAtOrBefore(S, p.ts)
    if (b) usedB.add(b.name)
    if (s) usedS.add(s.name)
    return { key: p.name, ts: p.ts, plan: p, booking: b, sc: s }
  })
  for (const b of B) {
    if (usedB.has(b.name)) continue
    const s = latestAtOrBefore(S, b.ts)
    const sFree = s && !usedS.has(s.name) ? s : null
    if (sFree) usedS.add(sFree.name)
    rows.push({ key: b.name, ts: b.ts, plan: null, booking: b, sc: sFree })
  }
  for (const s of S) {
    if (!usedS.has(s.name)) rows.push({ key: s.name, ts: s.ts, plan: null, booking: null, sc: s })
  }
  rows.sort((a, b) => b.ts - a.ts)
  return rows
}

function FileCell({ f, rowTs, showTime, onDelete }) {
  if (!f) return <td className="ofile none">—</td>
  const otherDay = dayKey(f.ts) !== dayKey(rowTs)
  return (
    <td className="ofile" title={`${f.name}\nแก้ไขล่าสุด ${fmtTime(f.mtime)}`}>
      <div className="ofile-in">
        <div className="ofile-meta">
          {showTime && <b>{otherDay ? fmtDate(f.ts) + ' ' : ''}{fmtHM(f.ts)}</b>}
          <span>{fmtSize(f.size)}</span>
        </div>
        <div className="rowact">
          {/* withBase = เติม /knitplan เมื่อเข้าผ่าน nginx (เข้าตรง :8080 จะเป็นค่าว่าง)
              ถ้าไม่เติม ลิงก์จะชี้ไป root ของ docker-webchat แล้วได้ 404 จาก webchat */}
          <a className="dl" href={withBase(`/api/outputs/${encodeURIComponent(f.name)}`)}>ดาวน์โหลด</a>
          <button className="del" onClick={() => onDelete(f.name)}>ลบ</button>
        </div>
      </div>
    </td>
  )
}

function RunTable({ rows, onDelete }) {
  return (
    <table className="grid outputs runs">
      <thead>
        <tr><th>รอบแผน</th><th>แผนการผลิต</th><th>Booking</th><th>SC</th></tr>
      </thead>
      <tbody>
        {rows.map((r, i) => {
          const newDay = i > 0 && dayKey(r.ts) !== dayKey(rows[i - 1].ts)
          return (
            <tr key={r.key} className={newDay ? 'newday' : ''}>
              <td className="orun">
                <b>{fmtDate(r.ts)}</b> {fmtHM(r.ts)}
                {!r.plan && <div className="orun-note">ไม่มีไฟล์แผน</div>}
              </td>
              <FileCell f={r.plan} rowTs={r.ts} showTime={false} onDelete={onDelete} />
              <FileCell f={r.booking} rowTs={r.ts} showTime onDelete={onDelete} />
              <FileCell f={r.sc} rowTs={r.ts} showTime onDelete={onDelete} />
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

export default function Outputs() {
  const [rows, setRows] = useState([])
  const [msg, setMsg] = useState('')

  async function load() {
    setMsg('')
    try {
      const [plans, bks, scs] = await Promise.all([api.outputs(), api.outputsBooking(), api.outputsSc()])
      setRows(buildRows(plans, bks, scs))
    } catch (e) { setMsg('โหลดไม่ได้: ' + e.message) }
  }
  useEffect(() => { load() }, [])

  async function del(name) {
    if (!window.confirm(`ลบไฟล์ "${name}" ?\nการลบเป็นการถาวร เรียกคืนไม่ได้`)) return
    setMsg('')
    try {
      await api.deleteOutput(name)
      await load()   // load() ล้าง msg ก่อน → ตั้งข้อความหลังโหลดเสร็จ ไม่งั้นข้อความหายทันที
      setMsg(`ลบ ${name} แล้ว`)
    } catch (e) { setMsg('ลบไม่ได้: ' + e.message) }
  }

  return (
    <div className="card">
      <div className="editbar">
        <h2>ไฟล์แผนการผลิต + Booking + SC</h2>
        <button onClick={load}>รีเฟรช</button>
      </div>
      <div className="hint">1 แถว = 1 รอบแผน · Booking / SC = ไฟล์ล่าสุดที่ดึงก่อนแผนรอบนั้น (ชุดที่แผนใช้) · ชี้ที่ช่องเพื่อดูชื่อไฟล์</div>
      {msg && <div className="msg">{msg}</div>}
      {!rows.length && <div className="hint">ยังไม่มีไฟล์ผลลัพธ์</div>}
      {rows.length > 0 && <RunTable rows={rows} onDelete={del} />}
    </div>
  )
}

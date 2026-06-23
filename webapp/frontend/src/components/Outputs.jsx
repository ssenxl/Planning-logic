import React, { useEffect, useState } from 'react'
import { api } from '../api.js'

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

function FileTable({ files }) {
  return (
    <table className="grid outputs">
      <thead>
        <tr><th>ชื่อไฟล์</th><th>ขนาด</th><th>แก้ไขล่าสุด</th><th></th></tr>
      </thead>
      <tbody>
        {files.map(f => (
          <tr key={f.name}>
            <td>{f.name}</td>
            <td>{fmtSize(f.size)}</td>
            <td>{fmtTime(f.mtime)}</td>
            <td><a className="dl" href={`/api/outputs/${encodeURIComponent(f.name)}`}>ดาวน์โหลด</a></td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default function Outputs() {
  const [files, setFiles] = useState([])
  const [booking, setBooking] = useState([])
  const [msg, setMsg] = useState('')

  async function load() {
    setMsg('')
    try {
      const [plans, bks] = await Promise.all([api.outputs(), api.outputsBooking()])
      setFiles(plans)
      setBooking(bks)
    } catch (e) { setMsg('โหลดไม่ได้: ' + e.message) }
  }
  useEffect(() => { load() }, [])

  return (
    <>
      <div className="card">
        <div className="editbar">
          <h2>ไฟล์แผนการผลิต (production_plan)</h2>
          <button onClick={load}>รีเฟรช</button>
        </div>
        {msg && <div className="msg">{msg}</div>}
        {!files.length && <div className="hint">ยังไม่มีไฟล์ผลลัพธ์</div>}
        {files.length > 0 && <FileTable files={files} />}
      </div>

      <div className="card">
        <div className="editbar">
          <h2>ไฟล์ Booking (booking_final → SharePoint: booking_final_auto.xlsx)</h2>
        </div>
        {!booking.length && <div className="hint">ยังไม่มีไฟล์ booking</div>}
        {booking.length > 0 && <FileTable files={booking} />}
      </div>
    </>
  )
}

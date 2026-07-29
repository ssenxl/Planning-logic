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

function FileTable({ files, onDelete }) {
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
            <td className="rowact">
              <a className="dl" href={`/api/outputs/${encodeURIComponent(f.name)}`}>ดาวน์โหลด</a>
              <button className="del" onClick={() => onDelete(f.name)}>ลบ</button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default function Outputs() {
  const [files, setFiles] = useState([])
  const [booking, setBooking] = useState([])
  const [sc, setSc] = useState([])
  const [msg, setMsg] = useState('')

  async function load() {
    setMsg('')
    try {
      const [plans, bks, scs] = await Promise.all([api.outputs(), api.outputsBooking(), api.outputsSc()])
      setFiles(plans)
      setBooking(bks)
      setSc(scs)
    } catch (e) { setMsg('โหลดไม่ได้: ' + e.message) }
  }
  useEffect(() => { load() }, [])

  async function del(name) {
    if (!window.confirm(`ลบไฟล์ "${name}" ?\nการลบเป็นการถาวร เรียกคืนไม่ได้`)) return
    setMsg('')
    try {
      await api.deleteOutput(name)
      setMsg(`ลบ ${name} แล้ว`)
      load()
    } catch (e) { setMsg('ลบไม่ได้: ' + e.message) }
  }

  return (
    <>
      <div className="card">
        <div className="editbar">
          <h2>ไฟล์แผนการผลิต (production_plan)</h2>
          <button onClick={load}>รีเฟรช</button>
        </div>
        {msg && <div className="msg">{msg}</div>}
        {!files.length && <div className="hint">ยังไม่มีไฟล์ผลลัพธ์</div>}
        {files.length > 0 && <FileTable files={files} onDelete={del} />}
      </div>

      <div className="card">
        <div className="editbar">
          <h2>ไฟล์ Booking </h2>
        </div>
        {!booking.length && <div className="hint">ยังไม่มีไฟล์ booking</div>}
        {booking.length > 0 && <FileTable files={booking} onDelete={del} />}
      </div>

      <div className="card">
        <div className="editbar">
          <h2>ไฟล์ข้อมูล SC (view_sc)</h2>
        </div>
        {!sc.length && <div className="hint">ยังไม่มีไฟล์ SC</div>}
        {sc.length > 0 && <FileTable files={sc} onDelete={del} />}
      </div>
    </>
  )
}

import React, { useEffect, useRef, useState } from 'react'
import { api } from '../api.js'

const MODES = [
  { id: 'full', label: 'รันทั้ง Pipeline', desc: 'ดึง DB + รันแผนทั้งหมด', cls: 'primary' },
  { id: 'db', label: 'ดึงข้อมูล DB', desc: 'Calendar → Booking → Stock → SC', cls: '' },
  { id: 'plan', label: 'รันแผน', desc: 'AVA_MC → Order → Planning', cls: '' },
]

function fmt(iso) {
  if (!iso) return '-'
  const d = new Date(iso)
  return d.toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function Dashboard() {
  const [status, setStatus] = useState({})
  const [logs, setLogs] = useState([])
  const [sched, setSched] = useState({})
  const [msg, setMsg] = useState('')
  const offset = useRef(0)
  const logBox = useRef(null)

  async function pollLogs() {
    try {
      const r = await api.runLogs(offset.current)
      if (r.lines.length) {
        offset.current = r.next_offset
        setLogs(prev => [...prev, ...r.lines])
      }
    } catch {}
  }
  async function pollStatus() {
    try { setStatus(await api.runStatus()) } catch {}
  }
  async function loadSched() {
    try { setSched(await api.schedule()) } catch {}
  }

  useEffect(() => {
    pollStatus(); pollLogs(); loadSched()
    const t = setInterval(() => { pollStatus(); pollLogs() }, 1500)
    return () => clearInterval(t)
  }, [])

  useEffect(() => {
    if (logBox.current) logBox.current.scrollTop = logBox.current.scrollHeight
  }, [logs])

  async function run(mode) {
    setMsg('')
    setLogs([]); offset.current = 0
    try {
      const r = await api.run(mode)
      setMsg(r.message)
      if (!r.ok) return
      setTimeout(() => { pollStatus(); pollLogs() }, 300)
    } catch (e) { setMsg('ผิดพลาด: ' + e.message) }
  }

  const running = status.running
  const rc = status.returncode

  return (
    <div className="grid2">
      <div className="card">
        <h2>สั่งรัน</h2>
        <div className="run-btns">
          {MODES.map(m => (
            <button key={m.id} className={'runbtn ' + m.cls}
              disabled={running} onClick={() => run(m.id)}>
              <b>{m.label}</b><small>{m.desc}</small>
            </button>
          ))}
        </div>
        {msg && <div className="msg">{msg}</div>}

        <div className="status">
          <h3>สถานะ</h3>
          <div className={'badge ' + (running ? 'run' : rc === 0 ? 'ok' : rc == null ? 'idle' : 'fail')}>
            {running ? 'กำลังรัน: ' + (status.label || '') :
             rc === 0 ? 'สำเร็จ' : rc == null ? 'ว่าง' : 'ล้มเหลว (rc=' + rc + ')'}
          </div>
          <table className="kv">
            <tbody>
              <tr><td>งานล่าสุด</td><td>{status.label || '-'}</td></tr>
              <tr><td>ทริกเกอร์</td><td>{status.trigger === 'schedule' ? 'อัตโนมัติ' : status.trigger === 'manual' ? 'กดเอง' : '-'}</td></tr>
              <tr><td>เริ่ม</td><td>{fmt(status.started_at)}</td></tr>
              <tr><td>จบ</td><td>{fmt(status.finished_at)}</td></tr>
            </tbody>
          </table>
        </div>

        <div className="status">
          <h3>กำหนดการอัตโนมัติถัดไป</h3>
          <table className="kv">
            <tbody>
              <tr><td>ดึง DB</td><td>{sched.db_pull?.enabled ? fmt(sched.db_pull?.next_run) : 'ปิดอยู่'}</td></tr>
              <tr><td>รันแผน</td><td>{sched.plan?.enabled ? fmt(sched.plan?.next_run) : 'ปิดอยู่'}</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <div className="card logcard">
        <h2>Log สด</h2>
        <pre className="logbox" ref={logBox}>
          {logs.length ? logs.join('\n') : 'ยังไม่มี log — กดปุ่มสั่งรันทางซ้าย'}
        </pre>
      </div>
    </div>
  )
}

import React, { useEffect, useRef, useState } from 'react'
import { api } from '../api.js'
import Schedule from './Schedule.jsx'

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

function Progress({ status }) {
  const running = status.running
  const rc = status.returncode
  const cancelled = status.cancelled
  const total = status.total_steps
  const num = status.step_num || 0
  const pct = running ? (status.progress ?? 0) : rc === 0 ? 100 : (status.progress ?? 0)
  const state = running ? 'run' : cancelled ? 'fail' : rc === 0 ? 'ok' : rc == null ? 'idle' : 'fail'

  if (!status.started_at && !running) {
    return <div className="prog-idle">กดปุ่มสั่งรันทางซ้ายเพื่อเริ่ม แล้วดูความคืบหน้าที่นี่</div>
  }
  return (
    <div className={'prog ' + state}>
      <div className="prog-top">
        <span className="prog-label">
          {running
            ? (total ? `ขั้นที่ ${num}/${total}` : 'กำลังเริ่ม...')
            : cancelled ? `⛔ ยกเลิกที่ขั้น ${num}/${total || '?'}`
              : rc === 0 ? '✅ สำเร็จทั้งหมด'
                : `❌ ล้มเหลวที่ขั้น ${num}/${total || '?'}`}
        </span>
        <span className="prog-pct">{pct}%</span>
      </div>
      <div className="prog-track">
        <div className="prog-fill" style={{ width: pct + '%' }} />
      </div>
      <div className="prog-desc">
        {running ? (status.step_desc || 'กำลังประมวลผล...')
          : cancelled ? 'ผู้ใช้สั่งหยุดการรันรอบนี้'
            : rc === 0 ? 'เสร็จเรียบร้อย ดูไฟล์ผลลัพธ์ได้ที่แท็บ "ผลลัพธ์"'
              : 'ดูรายละเอียดข้อผิดพลาดได้ที่ log ด้านล่าง'}
      </div>
    </div>
  )
}

export default function Dashboard() {
  const [status, setStatus] = useState({})
  const [logs, setLogs] = useState([])
  const [sched, setSched] = useState({})
  const [showSched, setShowSched] = useState(false)
  const [msg, setMsg] = useState('')
  const offset = useRef(0)
  const logBox = useRef(null)

  async function pollLogs() {
    try {
      const r = await api.runLogs(offset.current)
      if (r.reset) {
        offset.current = r.next_offset
        setLogs(r.lines)                        // รอบใหม่ → แทนที่ log ทั้งหมด
      } else if (r.lines.length) {
        offset.current = r.next_offset
        setLogs(prev => [...prev, ...r.lines])   // ต่อท้ายตามปกติ
      }
    } catch { }
  }
  async function pollStatus() {
    try { setStatus(await api.runStatus()) } catch { }
  }
  async function loadSched() {
    try { setSched(await api.schedule()) } catch { }
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

  async function stop() {
    if (!window.confirm('ยืนยันหยุดการรัน? งานที่กำลังทำอยู่จะถูกยกเลิกทันที')) return
    setMsg('')
    try {
      const r = await api.runStop()
      setMsg(r.message)
      setTimeout(() => { pollStatus(); pollLogs() }, 300)
    } catch (e) { setMsg('ผิดพลาด: ' + e.message) }
  }

  const running = status.running
  const rc = status.returncode
  const cancelled = status.cancelled

  return (
    <div className="grid2">
      <div className="card">
        <h2>สั่งรัน</h2>
        <div className="run-btns">
          {running ? (
            <button className="runbtn stop" onClick={stop}>
              <b>⛔ หยุดรัน</b><small>ยกเลิกงานที่กำลังทำอยู่ทันที</small>
            </button>
          ) : (
            MODES.map(m => (
              <button key={m.id} className={'runbtn ' + m.cls}
                onClick={() => run(m.id)}>
                <b>{m.label}</b><small>{m.desc}</small>
              </button>
            ))
          )}
        </div>
        {msg && <div className="msg">{msg}</div>}

        <div className="status">
          <h3>สถานะ</h3>
          <div className={'badge ' + (running ? 'run' : cancelled ? 'fail' : rc === 0 ? 'ok' : rc == null ? 'idle' : 'fail')}>
            {running ? 'กำลังรัน: ' + (status.label || '') :
              cancelled ? '⛔ ยกเลิกแล้ว' :
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
          <div className="status-head">
            <h3>กำหนดการอัตโนมัติถัดไป</h3>
            <button className="ghost sm" onClick={() => setShowSched(true)}>⏰ ตั้งเวลา</button>
          </div>
          <table className="kv">
            <tbody>
              <tr><td>รันทั้ง Pipeline</td><td>{sched.full?.enabled ? fmt(sched.full?.next_run) : 'ปิดอยู่'}</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <div className="card logcard">
        <h2>ความคืบหน้า</h2>
        <Progress status={status} />
        <details className="logdetails">
          <summary>ดู log ละเอียด (สำหรับตรวจสอบ)</summary>
          <pre className="logbox" ref={logBox}>
            {logs.length ? logs.join('\n') : 'ยังไม่มี log — กดปุ่มสั่งรันทางซ้าย'}
          </pre>
        </details>
      </div>

      {showSched && (
        <div className="modal-backdrop" onClick={() => setShowSched(false)}>
          <div className="modal-box" onClick={e => e.stopPropagation()}>
            <button className="modal-close" title="ปิด" onClick={() => setShowSched(false)}>✕</button>
            <Schedule onSaved={loadSched} />
          </div>
        </div>
      )}
    </div>
  )
}

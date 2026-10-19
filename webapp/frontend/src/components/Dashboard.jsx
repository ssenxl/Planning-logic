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

/* ---------------- ประวัติ Change Cylinder ---------------- */

const CYL_KINDS = {
  APPROVE: { key: 'approve', icon: '🔄', label: 'เปลี่ยน Cylinder' },
  REVOKE: { key: 'revoke', icon: '↩', label: 'ยกเลิก' },
  MOVE: { key: 'move', icon: '📦', label: 'ย้ายงาน' },
}
const CYL_FILTERS = [
  { id: 'ALL', label: 'ทั้งหมด' },
  { id: 'APPROVE', label: '🔄 เปลี่ยน Cylinder' },
  { id: 'REVOKE', label: '↩ ยกเลิก' },
  { id: 'MOVE', label: '📦 ย้ายงาน' },
]
const TH_MON = ['ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.',
  'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.']

/** "2026-08-25 14:30:00" → "25 ส.ค. 14:30" (เก็บเป็นเวลาท้องถิ่นอยู่แล้ว ไม่ต้องแปลง tz) */
function fmtAt(at) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/.exec(String(at || ''))
  if (!m) return at || '-'
  return `${+m[3]} ${TH_MON[+m[2] - 1]} ${m[4]}:${m[5]}`
}

/** เครื่องที่ย้ายจริงในเหตุการณ์นั้น — ติ๊กซ้ำคีย์เดิม machines เป็นยอดสะสม ต้องหักยอดเดิมก่อน */
function evMachines(e) {
  const now = Number(e.machines) || 0
  const before = e.machines_before == null ? null : (Number(e.machines_before) || 0)
  return before == null ? now : now - before
}

function CylStats({ events }) {
  const approve = events.filter(e => e.action === 'APPROVE')
  const revoke = events.filter(e => e.action === 'REVOKE')
  const mcs = approve.reduce((s, e) => s + evMachines(e), 0)
  const moves = events.reduce((s, e) => s + ((e.moves || []).length), 0)
  const tiles = [
    { k: 'approve', icon: '🔄', n: approve.length, label: 'ครั้งที่เปลี่ยน Cylinder' },
    { k: 'revoke', icon: '↩', n: revoke.length, label: 'ครั้งที่ยกเลิก' },
    { k: 'mc', icon: '⚙️', n: mcs, label: 'เครื่องที่ย้ายรวม' },
    { k: 'move', icon: '📦', n: moves, label: 'งานที่ถูกย้ายตาม' },
  ]
  return (
    <div className="cyldash-stats">
      {tiles.map(t => (
        <div key={t.k} className={'cyldash-tile ' + t.k}>
          <span className="cyldash-tile-icon">{t.icon}</span>
          <b>{t.n.toLocaleString()}</b>
          <small>{t.label}</small>
        </div>
      ))}
    </div>
  )
}

/** การ์ดเหตุการณ์ 1 ใบบนไทม์ไลน์ */
function CylEvent({ e }) {
  const kind = CYL_KINDS[e.action] || { key: 'other', icon: '•', label: e.action || '-' }
  const items = e.items || []
  const moves = e.moves || []
  // ยกเลิก = คืนเครื่องกลับเกจเดิม → ลูกศรกลับทาง
  const [gFrom, gTo] = e.action === 'REVOKE'
    ? [e.tgt_gauge, e.src_gauge]
    : [e.src_gauge, e.tgt_gauge]
  const n = evMachines(e)

  return (
    <li className={'cyldash-ev ' + kind.key}>
      <span className="cyldash-ev-dot">{kind.icon}</span>
      <div className="cyldash-ev-body">
        <div className="cyldash-ev-head">
          <span className="cyldash-tag">{kind.label}</span>
          <span className="cyldash-when">{fmtAt(e.at)}</span>
          <span className="cyldash-who">👤 {e.by || 'ไม่ระบุ'}</span>
          {e.sheet && <span className="cyldash-sheet">{e.sheet}</span>}
        </div>

        {(gFrom || gTo) ? (
          <div className="cyldash-chips">
            <span className="cyldash-chip site">{[e.factory, e.cat].filter(Boolean).join(' · ')}</span>
            <span className="cyldash-chip gauge">
              G{gFrom} <i>→</i> G{gTo}
            </span>
            {n > 0 && (
              <span className="cyldash-chip mc">
                {n} เครื่อง
                {e.machines_before != null && <em> (รวม {e.machines})</em>}
              </span>
            )}
            {e.set_week != null && (
              <span className="cyldash-chip wk">สั่งเปลี่ยน W{e.set_week}</span>
            )}
            {e.ready_week != null && (
              <span className="cyldash-chip wk ready">พร้อมใช้ W{e.ready_week}</span>
            )}
          </div>
        ) : (
          <div className="cyldash-detail">{e.detail || '-'}</div>
        )}

        {items.length > 0 && (
          <div className="cyldash-items">
            {items.slice(0, 12).map((it, i) => <span key={i} className="cyldash-item">{it}</span>)}
            {items.length > 12 && <span className="cyldash-item more">+{items.length - 12}</span>}
          </div>
        )}

        {moves.length > 0 && (
          <details className="cyldash-moves">
            <summary>งานที่ถูกย้าย {moves.length} รายการ</summary>
            <table className="grid">
              <thead>
                <tr><th>Item</th><th>SO</th><th>สัปดาห์</th><th className="num">จำนวน</th><th className="num">เครื่อง</th></tr>
              </thead>
              <tbody>
                {moves.map((m, i) => (
                  <tr key={i}>
                    <td>{m.item || '-'}</td>
                    <td>{m.so || '-'}</td>
                    <td className="nowrap">W{m.from_week} → W{m.to_week}</td>
                    <td className="num">{m.qty ?? '-'}</td>
                    <td className="num">{m.mc ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        )}
      </div>
    </li>
  )
}

/**
 * ประวัติ Change Cylinder — ใครติ๊กเปลี่ยน Cylinder อะไรเมื่อไร และแผนถูกย้ายอะไรตามไปบ้าง
 * ปุ่มอยู่บนหน้าหลัก (admin เท่านั้น — endpoint กันด้วย ADMIN_ONLY ฝั่ง server อีกชั้น)
 * กดแล้วกางเป็น dashboard เต็มจอ ไม่กินที่หน้าหลักตลอดเวลาเหมือนตารางเดิม
 */
function CylinderHistory() {
  const [events, setEvents] = useState(null)
  const [err, setErr] = useState('')
  const [open, setOpen] = useState(false)
  const [filter, setFilter] = useState('ALL')
  const [q, setQ] = useState('')

  async function load() {
    setErr('')
    try {
      const r = await api.cylLog(200)
      setEvents(r.events || [])
    } catch (e) {
      setErr('อ่านประวัติไม่ได้: ' + e.message)
    }
  }

  useEffect(() => { load() }, [])

  const all = events || []
  const kw = q.trim().toLowerCase()
  const shown = all.filter(e => {
    if (filter !== 'ALL' && e.action !== filter) return false
    if (!kw) return true
    const hay = [e.by, e.factory, e.cat, e.detail, e.sheet,
      ...(e.items || []),
      ...(e.moves || []).flatMap(m => [m.item, m.so])].join(' ').toLowerCase()
    return hay.includes(kw)
  })

  return (
    <>
      <div className="status">
        <div className="status-head">
          <h3>ประวัติ Change Cylinder</h3>
          <button className="ghost sm" onClick={() => setOpen(true)}>
            🔄 ดูประวัติ{events ? ` (${all.length})` : ''}
          </button>
        </div>
        <table className="kv">
          <tbody>
            <tr>
              <td>ล่าสุด</td>
              <td>{all.length ? `${fmtAt(all[0].at)} · ${all[0].by || 'ไม่ระบุ'}` : (err ? '-' : 'ยังไม่มีใครติ๊ก')}</td>
            </tr>
          </tbody>
        </table>
        {err && <div className="msg">{err}</div>}
      </div>

      {open && (
        <div className="modal-backdrop" onClick={() => setOpen(false)}>
          <div className="modal-box" onClick={e => e.stopPropagation()}>
            <button className="modal-close" title="ปิด" onClick={() => setOpen(false)}>✕</button>
            <div className="card cyldash">
              <div className="cyldash-head">
                <div>
                  <h2>🔄 ประวัติ Change Cylinder</h2>
                  <span className="hint">ทุกครั้งที่มีคนติ๊ก/ยกเลิก ถูกบันทึกไว้ที่ data_plan/cylinder_change_log.jsonl</span>
                </div>
                <button className="ghost sm" onClick={load}>รีเฟรช</button>
              </div>

              {err && <div className="msg">{err}</div>}
              <CylStats events={all} />

              <div className="cyldash-bar">
                <div className="cyldash-filters">
                  {CYL_FILTERS.map(f => (
                    <button key={f.id}
                      className={'cyldash-fbtn' + (filter === f.id ? ' on' : '')}
                      onClick={() => setFilter(f.id)}>{f.label}</button>
                  ))}
                </div>
                <input className="cyldash-search" value={q} onChange={e => setQ(e.target.value)}
                  placeholder="ค้นหา item / โรงงาน / ผู้ทำ" />
              </div>

              {!events && <div className="hint">กำลังโหลด…</div>}
              {events && shown.length === 0 && (
                <div className="cyldash-empty">
                  {all.length === 0 ? 'ยังไม่มีใครติ๊ก Change Cylinder' : 'ไม่พบเหตุการณ์ที่ตรงกับตัวกรอง'}
                </div>
              )}
              {shown.length > 0 && (
                <ol className="cyldash-tl">
                  {shown.map((e, i) => <CylEvent key={i} e={e} />)}
                </ol>
              )}
            </div>
          </div>
        </div>
      )}
    </>
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
              {(sched.runs || []).length === 0 && <tr><td colSpan={2}>ยังไม่ได้ตั้งรอบ</td></tr>}
              {(sched.runs || []).map(r => (
                <tr key={r.id}>
                  <td>{String(r.hour).padStart(2, '0')}:{String(r.minute).padStart(2, '0')} · {r.mode_label}
                    <br /><small className="muted">{r.days_text}</small></td>
                  <td>{r.enabled ? fmt(r.next_run) : 'ปิดอยู่'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <CylinderHistory />
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

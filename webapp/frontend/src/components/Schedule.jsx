import React, { useEffect, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'

// ตั้งเวลารันอัตโนมัติ — หลายรอบต่อวัน แต่ละรอบเลือกงาน + วันได้
// อยู่ที่ ตั้งค่าระบบ › ตั้งเวลารันอัตโนมัติ และหน้าต่าง ⏰ บน Dashboard (component เดียวกัน)
// ระบบรันได้ทีละงาน: ถึงเวลาแล้วยังมีงานรันอยู่ รอบนั้นถูกข้าม (backend จดไว้ → โชว์เป็นป้ายเตือน)

const DAYS = ['จ.', 'อ.', 'พ.', 'พฤ.', 'ศ.', 'ส.', 'อา.']   // 0 = จันทร์ ตรงกับ backend
const ALL_DAYS = [0, 1, 2, 3, 4, 5, 6]
const MODE_DESC = {
  full: 'ดึงข้อมูลจาก DB แล้วจัดแผนใหม่ทั้งหมด',
  db: 'ดึงข้อมูลใหม่อย่างเดียว ยังไม่จัดแผน',
  plan: 'จัดแผนใหม่จากข้อมูลที่มีอยู่ ไม่ดึง DB',
}

const HOURS = Array.from({ length: 24 }, (_, i) => i)
// นาทีทีละ 5 พอสำหรับตั้งรอบ ; ค่าที่ตั้งไว้แล้วไม่ลงตัว 5 (เช่น 06:12) ยังโชว์ได้ ไม่ถูกปัดทิ้งเงียบ ๆ
const minutesFor = cur => {
  const ms = Array.from({ length: 12 }, (_, i) => i * 5)
  return ms.includes(cur) ? ms : [...ms, cur].sort((a, b) => a - b)
}
const pad = n => String(n ?? 0).padStart(2, '0')
const hhmm = r => `${pad(r.hour)}:${pad(r.minute)}`
const fmt = iso => (iso ? new Date(iso).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' }) : '-')
// รูปที่ส่งกลับ server / ใช้เทียบว่าแก้ค้างอยู่ไหม
const toPayload = r => ({
  id: r.id || '', enabled: !!r.enabled, hour: +r.hour, minute: +r.minute,
  mode: r.mode, days: [...r.days].sort((a, b) => a - b),
})
let newSeq = 0

// รอบที่เปิดอยู่ซึ่งอยู่ก่อนหน้ารอบนี้ในวันเดียวกัน และห่างกันน้อยกว่าเวลาที่รันจริง → รอบนี้อาจถูกข้าม
function tooClose(rows, r, runMin) {
  if (!r.enabled || !runMin) return null
  const t = r.hour * 60 + r.minute
  let hit = null
  for (const o of rows) {
    if (o === r || !o.enabled || !o.days.some(d => r.days.includes(d))) continue
    const gap = t - (o.hour * 60 + o.minute)
    if (gap >= 0 && gap < runMin && (!hit || gap < hit.gap)) hit = { gap, at: hhmm(o) }
  }
  return hit
}

export default function Schedule({ onSaved }) {
  const [data, setData] = useState(null)   // สถานะจาก server
  const [rows, setRows] = useState([])     // ที่กำลังแก้
  const [runMin, setRunMin] = useState(0)  // รอบล่าสุดใช้เวลากี่นาที (ไว้เตือนรอบที่ชิดกันเกิน)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [saving, setSaving] = useState(false)

  const base = data ? JSON.stringify(data.runs.map(toPayload)) : ''
  const dirty = !!data && JSON.stringify(rows.map(toPayload)) !== base
  const dirtyRef = useRef(false)
  dirtyRef.current = dirty

  function apply(d, keepDraft) {
    setData(d)
    if (!keepDraft) setRows(d.runs.map(r => ({ ...r })))
  }

  async function load(keepDraft) {
    try { apply(await api.schedule(), keepDraft) } catch (e) { setErr('โหลดไม่ได้: ' + e.message) }
  }

  useEffect(() => {
    load()
    api.runStatus().then(s => {
      if (s?.started_at && s?.finished_at && s.returncode === 0) {
        setRunMin(Math.ceil((new Date(s.finished_at) - new Date(s.started_at)) / 60000))
      }
    }).catch(() => { })
  }, [])

  // admin อีกคนบันทึกเวลาใหม่ → เห็นทันที ; ถ้าเรากำลังแก้ค้างอยู่ ไม่ทับของที่พิมพ์ไว้
  useEffect(() => live.subscribe(live.RES.schedule, (ent) => {
    const who = ent?.by || 'อีกคนหนึ่ง'
    if (live.isMe(ent?.by) && !dirtyRef.current) {
      load()   // เสียงสะท้อนจากการบันทึกของเราเอง — ไม่ต้องทับข้อความ "บันทึกแล้ว"
    } else if (dirtyRef.current) {
      setMsg(`${who} เพิ่งบันทึกเวลาใหม่ — ที่แก้ค้างอยู่บนจอยังไม่ได้บันทึก กด "ยกเลิก" เพื่อดูค่าล่าสุด`)
      load(true)
    } else {
      setMsg(`${who} เพิ่งบันทึกเวลาใหม่ — โหลดค่าล่าสุดให้แล้ว`)
      load()
    }
  }), [])

  const upd = (i, patch) => setRows(rs => rs.map((r, j) => (j === i ? { ...r, ...patch } : r)))

  function toggleDay(i, d) {
    const r = rows[i]
    upd(i, { days: r.days.includes(d) ? r.days.filter(x => x !== d) : [...r.days, d] })
  }

  function add() {
    setRows(rs => [...rs, {
      id: '', _k: 'new' + (++newSeq), enabled: true, hour: 12, minute: 0, mode: 'plan', days: [...ALL_DAYS],
    }])
  }

  async function save() {
    setSaving(true); setMsg(''); setErr('')
    try {
      const r = await api.saveSchedule(rows.map(toPayload))
      apply(r)
      setMsg(r.next
        ? `บันทึกแล้ว — รอบถัดไป ${fmt(r.next.next_run)} (${r.next.mode_label})`
        : 'บันทึกแล้ว — ตอนนี้ไม่มีรอบที่เปิดอยู่ ระบบจะไม่รันเอง')
      if (onSaved) onSaved(r)
    } catch (e) { setErr('บันทึกไม่ได้: ' + e.message) }
    finally { setSaving(false) }
  }

  function cancel() {
    setMsg(''); setErr('')
    setRows(data.runs.map(r => ({ ...r })))
  }

  if (!data) return <div className="card"><div className="hint">{err || 'กำลังโหลด...'}</div></div>

  const modes = data.modes || []
  const full = rows.length >= (data.max_runs || 12)

  return (
    <div className="card autorun">
      <div className="autorun-top">
        <span className="autorun-ico">⏰</span>
        <div>
          <h3>ตั้งเวลารันอัตโนมัติ</h3>
          <div className="autorun-desc">
            ระบบรันให้เองตามเวลาที่ตั้ง (เวลาไทย) · ตั้งได้หลายรอบต่อวัน ·
            รันได้<b>ทีละงาน</b> ถ้าถึงเวลาแล้วยังมีงานรันอยู่ รอบนั้นจะถูกข้าม
          </div>
        </div>
      </div>

      <div className="autorun-sum">
        {data.next
          ? <span>▶ รอบถัดไป <b>{fmt(data.next.next_run)}</b> · {data.next.mode_label}</span>
          : <span className="off">⏸ ไม่มีรอบที่เปิดอยู่ — ระบบจะไม่รันเอง</span>}
        {runMin > 0 && <span className="muted">รอบล่าสุดรันนาน ~{runMin} นาที</span>}
        {data.updated_at && (
          <span className="muted">แก้ล่าสุด {fmt(data.updated_at)}{data.updated_by ? ` โดย ${data.updated_by}` : ''}</span>
        )}
      </div>

      <div className="autorun-list">
        {rows.length === 0 && <div className="autorun-empty">ยังไม่มีรอบ — กด "+ เพิ่มรอบ" ด้านล่าง</div>}
        {rows.map((r, i) => {
          const close = tooClose(rows, r, runMin)
          const srv = data.runs.find(x => x.id && x.id === r.id)
          return (
            <div key={r.id || r._k} className={'autorun-row' + (r.enabled ? '' : ' disabled')}>
              <label className="autorun-switch" title={r.enabled ? 'เปิดอยู่ — กดเพื่อปิดรอบนี้' : 'ปิดอยู่ — กดเพื่อเปิด'}>
                <input type="checkbox" checked={!!r.enabled} onChange={e => upd(i, { enabled: e.target.checked })} />
                <span className="track"><span className="knob" /></span>
              </label>

              {/* ไม่ใช้ <input type="time"> เพราะเบราว์เซอร์ภาษาอังกฤษโชว์เป็น AM/PM — ที่โรงงานใช้ 24 ชม. */}
              <div className="autorun-time" title="เวลา (24 ชั่วโมง)">
                <select value={r.hour} onChange={e => upd(i, { hour: +e.target.value })}>
                  {HOURS.map(h => <option key={h} value={h}>{pad(h)}</option>)}
                </select>
                <span>:</span>
                <select value={r.minute} onChange={e => upd(i, { minute: +e.target.value })}>
                  {minutesFor(r.minute).map(m => <option key={m} value={m}>{pad(m)}</option>)}
                </select>
                <small>น.</small>
              </div>

              <div className="autorun-mode">
                <select value={r.mode} onChange={e => upd(i, { mode: e.target.value })}>
                  {modes.map(m => <option key={m.id} value={m.id}>{m.label}</option>)}
                </select>
                <small>{MODE_DESC[r.mode] || ''}</small>
              </div>

              <div className="autorun-days">
                {DAYS.map((d, di) => (
                  <button key={di} type="button"
                    className={'autorun-day' + (r.days.includes(di) ? ' on' : '') + (di >= 5 ? ' wkend' : '')}
                    onClick={() => toggleDay(i, di)}>{d}</button>
                ))}
              </div>

              <button className="autorun-del" title="ลบรอบนี้"
                onClick={() => setRows(rs => rs.filter((_, j) => j !== i))}>🗑</button>

              <div className="autorun-note">
                {!r.days.length && <span className="bad">ยังไม่ได้เลือกวัน</span>}
                {r.enabled && srv?.next_run && !dirty && <span>ถัดไป {fmt(srv.next_run)}</span>}
                {!r.enabled && <span>ปิดอยู่ — ไม่รัน</span>}
                {close && (
                  <span className="warn">
                    ⚠️ ห่างจากรอบ {close.at} แค่ {close.gap} นาที แต่รอบล่าสุดรันนาน ~{runMin} นาที — รอบนี้อาจถูกข้าม
                  </span>
                )}
                {srv?.last_skipped && (
                  <span className="warn">⚠️ ถูกข้ามเมื่อ {fmt(srv.last_skipped)} เพราะมีงานรันอยู่</span>
                )}
              </div>
            </div>
          )
        })}
      </div>

      <div className="autorun-foot">
        <button className="autorun-add" onClick={add} disabled={full}
          title={full ? `ตั้งได้ไม่เกิน ${data.max_runs} รอบ` : ''}>+ เพิ่มรอบ</button>
        <div className="grow" />
        {msg && <span className="autorun-msg ok">{msg}</span>}
        {err && <span className="autorun-msg bad">{err}</span>}
        {dirty && <button onClick={cancel} disabled={saving}>ยกเลิก</button>}
        <button className="primary" onClick={save} disabled={saving || !dirty}>
          {saving ? 'กำลังบันทึก...' : 'บันทึก'}
        </button>
      </div>
    </div>
  )
}

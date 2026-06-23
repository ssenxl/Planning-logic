import React, { useEffect, useState } from 'react'
import { api } from '../api.js'

const JOBS = [
  { id: 'db_pull', label: 'ดึงข้อมูลจาก DB', desc: 'Calendar → Booking → Stock → SC' },
  { id: 'plan', label: 'รันแผนการผลิต', desc: 'AVA_MC → Order → Planning' },
]

function fmt(iso) {
  if (!iso) return '-'
  return new Date(iso).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function Schedule() {
  const [sched, setSched] = useState(null)
  const [msg, setMsg] = useState('')
  const [saving, setSaving] = useState(false)

  async function load() {
    try { setSched(await api.schedule()) } catch (e) { setMsg('โหลดไม่ได้: ' + e.message) }
  }
  useEffect(() => { load() }, [])

  function upd(id, key, val) {
    setSched(s => ({ ...s, [id]: { ...s[id], [key]: val } }))
  }

  async function save() {
    setSaving(true); setMsg('')
    const payload = {}
    for (const j of JOBS) {
      const c = sched[j.id]
      payload[j.id] = {
        enabled: !!c.enabled,
        hour: Math.max(0, Math.min(23, parseInt(c.hour) || 0)),
        minute: Math.max(0, Math.min(59, parseInt(c.minute) || 0)),
      }
    }
    try {
      const r = await api.saveSchedule(payload)
      setSched(r); setMsg('บันทึกเวลาแล้ว')
    } catch (e) { setMsg('บันทึกไม่ได้: ' + e.message) }
    finally { setSaving(false) }
  }

  if (!sched) return <div className="card"><div className="hint">{msg || 'กำลังโหลด...'}</div></div>

  return (
    <div className="card schedcard">
      <h2>ตั้งเวลาทำงานอัตโนมัติ</h2>
      <p className="hint">เวลาอ้างอิงเขตเวลาไทย (Asia/Bangkok) ระบบจะรันให้เองทุกวัน</p>
      {JOBS.map(j => {
        const c = sched[j.id] || {}
        return (
          <div key={j.id} className="schedrow">
            <div className="schedinfo">
              <b>{j.label}</b><small>{j.desc}</small>
            </div>
            <label className="toggle">
              <input type="checkbox" checked={!!c.enabled}
                onChange={e => upd(j.id, 'enabled', e.target.checked)} />
              <span>{c.enabled ? 'เปิด' : 'ปิด'}</span>
            </label>
            <div className="timeinput">
              <input type="number" min="0" max="23" value={c.hour ?? 0}
                onChange={e => upd(j.id, 'hour', e.target.value)} />
              <span>:</span>
              <input type="number" min="0" max="59" value={c.minute ?? 0}
                onChange={e => upd(j.id, 'minute', e.target.value)} />
            </div>
            <div className="nextrun">ถัดไป: {c.enabled ? fmt(c.next_run) : 'ปิดอยู่'}</div>
          </div>
        )
      })}
      <div className="actions">
        <button className="primary" onClick={save} disabled={saving}>
          {saving ? 'กำลังบันทึก...' : 'บันทึก'}
        </button>
        {msg && <span className="msg inline">{msg}</span>}
      </div>
    </div>
  )
}

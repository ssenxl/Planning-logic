import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import MailRecipients from './MailRecipients.jsx'

/* แจ้งเมลแผนเปลี่ยน (รอบ Replan)
   ผู้วางแผนต้องเห็น "จะส่งอะไรออกไป" ครบก่อนกดส่ง — เมลฉบับนี้ถูกใช้ตอบกลับฝ่ายขาย/ลูกค้า
   ตารางในหน้านี้จึงเป็นรายการชุดเดียวกับที่จะอยู่ในเมลเป๊ะ ๆ (backend ประกอบเนื้อเมลเอง
   จากไฟล์แผน ไม่ได้รับ HTML จากหน้านี้ — กันไม่ให้ยิงเมลอะไรก็ได้ในนามระบบ) */

// ตัวกรองรายการ — นับจากรายการที่เห็นอยู่ (ทั้งแผน หรือเฉพาะทีมที่คลิก)
const TABS = [
  { key: 'all', label: 'ทั้งหมด', match: () => true },
  { key: 'late_rdd', label: 'เพิ่งเกิน RDD', match: it => it.late },
  { key: 'moved', label: 'เลื่อนสัปดาห์', match: it => it.kinds.includes('MOVED') },
  { key: 'later', label: 'จบช้าลง', match: it => it.kinds.includes('FINISH_LATER') },
  { key: 'earlier', label: 'จบเร็วขึ้น', match: it => it.kinds.includes('FINISH_EARLIER') },
  { key: 'qty', label: 'ยอดเปลี่ยน', match: it => it.kinds.some(k => k.startsWith('QTY')) },
  { key: 'dropped', label: 'หลุดจากแผน', match: it => it.kinds.includes('DROPPED') },
  { key: 'added', label: 'งานใหม่', match: it => it.kinds.includes('ADDED') },
]

const MODES = [
  { key: 'team', label: '📮 ส่งแยกตามทีม' },
  { key: 'one', label: '📧 ส่งรวมฉบับเดียว' },
  { key: 'people', label: '👥 รายชื่อผู้รับเมล' },
]

// ชุด "ไม่ระบุทีม" ของ backend (replan_notify.UNASSIGNED)
const UNASSIGNED = 'ไม่ระบุทีม'
const normTeam = s => String(s || '').trim().replace(/\s+/g, ' ').toUpperCase()
// ช่องทีมของงานยุบหลายทีมมาด้วยลูกน้ำ ('RTS,NYK4-1') — แตกแบบเดียวกับ split_teams ฝั่ง backend
const teamsOf = it => {
  const t = String(it.team || '').split(/[,;]+/).map(normTeam).filter(Boolean)
  return t.length ? t : [UNASSIGNED]
}

const fmt = v => (Number(v) || 0).toLocaleString('th-TH', { maximumFractionDigits: 0 })
const wk = v => (v == null ? '—' : 'W' + v)
// โชว์ปีเมื่อของเดิมกับของใหม่คนละปี — "W39 → W22" ที่ข้ามปีอ่านกลับด้านเป็นเลื่อนขึ้นมาก่อน
const span = (d, showYear) => {
  if (d.from == null) return '—'
  const core = d.from === d.to ? wk(d.from) : `${wk(d.from)}–${d.to}`
  return showYear && d.year ? `${core}/${String(d.year).slice(-2)}` : core
}
const delta = n => (!n ? '' : n > 0 ? `ช้าลง ${n} สัปดาห์` : `เร็วขึ้น ${Math.abs(n)} สัปดาห์`)

// ผลกระทบเป็นประโยคเดียว — ตรงกับที่อยู่ในเมล (replan_notify.impact_text)
function impactText(it) {
  const ds = it.shift_start, de = it.shift_end
  if (it.kinds.includes('DROPPED')) return 'หลุดจากแผน'
  if (it.kinds.includes('ADDED')) return 'งานใหม่เข้าแผน'
  let out
  if (ds && de && ds !== de) {
    out = (ds > 0) === (de > 0)
      ? `${de > 0 ? 'ช้าลง' : 'เร็วขึ้น'} ${Math.min(Math.abs(ds), Math.abs(de))}–${Math.max(Math.abs(ds), Math.abs(de))} สัปดาห์`
      : `เริ่ม${delta(ds)} จบ${delta(de)}`
  } else {
    out = delta(de || ds) || 'ยอดเปลี่ยน'
  }
  if (Math.abs(it.d_qty) > 1) out += ` · ยอด ${it.d_qty > 0 ? '+' : ''}${fmt(it.d_qty)} กก.`
  return out
}

// ผู้รับเป็นป้ายชื่อสั้น (ส่วนหน้า @) — อีเมลเต็มยาวจนแถวยืด 3 บรรทัด ; ชี้เมาส์ดูเต็มได้
function Who({ list, max = 4, empty = '—' }) {
  if (!list?.length) return <span className="rn-none">{empty}</span>
  const shown = list.slice(0, max)
  const rest = list.slice(max)
  return (
    <span className="rn-who">
      {shown.map(m => <span className="rn-chip" key={m} title={m}>{m.split('@')[0]}</span>)}
      {!!rest.length && <span className="rn-chip rn-more" title={rest.join('\n')}>+{rest.length}</span>}
    </span>
  )
}

export default function ReplanNotify() {
  const [data, setData] = useState(null)
  const [mail, setMail] = useState(null)      // สถานะการตั้งค่าเมล (flow URL + ผู้รับเริ่มต้น)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [busy, setBusy] = useState(false)
  const [tab, setTab] = useState('all')
  const [to, setTo] = useState('')
  const [cc, setCc] = useState('')
  const [note, setNote] = useState('')
  const [subject, setSubject] = useState('')
  // โหมดการส่ง: team = แยกฉบับตามทีมขาย (ทางหลัก) · one = รวมฉบับเดียว (ของเดิม)
  // · people = หน้าจัดการรายชื่อผู้รับ
  const [mode, setMode] = useState('team')
  const [teams, setTeams] = useState(null)    // ผลจาก /api/replan/mail-teams
  const [pick, setPick] = useState(null)      // Set ของคีย์ทีมที่ติ๊กไว้ (null = ยังไม่เคยแตะ)
  const [sendRes, setSendRes] = useState(null)
  const [focusTeam, setFocusTeam] = useState(null)   // คลิกแถวทีม → ตารางรายการเหลือเฉพาะทีมนั้น
  const [addFor, setAddFor] = useState(null)         // ทีมที่กด "+ เพิ่มผู้รับ" มาจากหน้าส่ง
  const [peopleDirty, setPeopleDirty] = useState(false)

  useEffect(() => { load() }, [])

  async function load() {
    setBusy(true); setErr('')
    try {
      const [d, m] = await Promise.all([api.replanChanges(), api.replanMailStatus()])
      setData(d)
      setMail(m)
      setTo((m.to || []).join('; '))
      setCc((m.cc || []).join('; '))
      setSubject(d.mail?.subject || '')
    } catch (e) {
      setErr(e.message || String(e))
    } finally {
      setBusy(false)
    }
    loadTeams()
  }

  async function loadTeams() {
    try {
      const t = await api.replanMailTeams()
      setTeams(t)
      // ค่าเริ่มต้น = ติ๊กเฉพาะทีมที่มีผู้รับครบ (ทีมที่ยังไม่มีอีเมลกดส่งไปก็ไม่ออกอยู่ดี)
      setPick(new Set(t.teams.filter(x => x.ready).map(x => x.key)))
    } catch (e) {
      setTeams({ error: e.message || String(e), teams: [] })
    }
  }

  // ออกจากหน้ารายชื่อตอนยังไม่บันทึก = ของที่พิมพ์ไว้หายทั้งหมด → ถามก่อน
  function switchMode(next) {
    if (next === mode) return
    if (mode === 'people' && peopleDirty
      && !window.confirm('รายชื่อผู้รับที่แก้ไว้ยังไม่ได้บันทึก — ออกจากหน้านี้แล้วจะหายทั้งหมด\n\nออกเลยไหม?')) return
    if (mode === 'people') { setPeopleDirty(false); setAddFor(null) }
    setMode(next); setMsg(''); setErr('')
  }

  function addRecipientFor(team) {
    setAddFor(team)
    setMode('people'); setMsg(''); setErr('')
  }

  async function sendTeams() {
    const chosen = (teams?.teams || []).filter(t => t.ready && pick?.has(t.key))
    if (!chosen.length) { setErr('ยังไม่ได้เลือกทีมที่จะส่ง'); return }
    const where = teams.test_mode
      ? `โหมดทดสอบ — ทุกฉบับจะส่งถึง ${(teams.test_to || []).join(', ')} เท่านั้น`
      : '⚠ ส่งถึงฝ่ายขายจริง'
    if (!window.confirm(`ส่งเมล ${chosen.length} ฉบับ (ทีม: ${chosen.map(t => t.team).join(', ')})`
      + `\n\n${where}\n\nยืนยันส่ง?`)) return
    setBusy(true); setErr(''); setMsg(''); setSendRes(null)
    try {
      const res = await api.replanNotifyTeams(chosen.map(t => t.key), note)
      setSendRes(res)
      setMsg(`ส่งเข้าคิว Power Automate แล้ว ${res.sent} ฉบับ`
        + (res.failed ? ` · ล้มเหลว ${res.failed}` : '')
        + (res.test_mode ? ' (โหมดทดสอบ — ยังไม่ถึงฝ่ายขาย)' : ''))
    } catch (e) {
      setErr(e.message || String(e))
    } finally {
      setBusy(false)
    }
  }

  async function send() {
    if (!window.confirm(`ส่งเมลแจ้ง ${data.items.length} รายการ ไปที่:\n${to}\n\nยืนยันส่ง?`)) return
    setBusy(true); setErr(''); setMsg('')
    try {
      const res = await api.replanNotify(splitAddr(to), splitAddr(cc), note, subject)
      setMsg(`ส่งเข้าคิว Power Automate แล้ว (${res.items} รายการ → ${res.to.join(', ')})`
        + ' — ถ้าเมลไม่ถึงผู้รับ ให้ดู Run history ของ flow')
    } catch (e) {
      setErr(e.message || String(e))
    } finally {
      setBusy(false)
    }
  }

  // รายการที่อยู่ในขอบเขตตอนนี้: ทั้งแผน หรือเฉพาะทีมที่คลิก (= สิ่งที่จะอยู่ในเมลฉบับของทีมนั้น)
  const scoped = useMemo(() => {
    if (!data) return []
    if (mode !== 'team' || !focusTeam) return data.items
    return data.items.filter(it => teamsOf(it).includes(focusTeam))
  }, [data, mode, focusTeam])

  const counts = useMemo(() => Object.fromEntries(
    TABS.map(t => [t.key, scoped.filter(t.match).length])), [scoped])

  const shown = useMemo(() => {
    const t = TABS.find(x => x.key === tab) || TABS[0]
    return scoped.filter(t.match)
  }, [scoped, tab])

  if (err && !data) return <div className="err">โหลดรายการเปลี่ยนแปลงไม่ได้: {err}</div>
  if (!data) return <div className="rn-loading">กำลังอ่านไฟล์แผน…</div>

  const s = data.summary
  // ตัวกรองรายทีมมีผลเฉพาะแท็บส่งแยกทีม (ดู scoped) — แท็บอื่นต้องไม่ขึ้นป้ายว่ากรองอยู่
  const focusName = mode === 'team' && focusTeam
    && (teams?.teams || []).find(t => t.key === focusTeam)?.team
  const chosen = (teams?.teams || []).filter(t => t.ready && pick?.has(t.key))

  return (
    <div className="replan-notify">
      <header className="rn-head">
        <div className="rn-title">
          <h3>📧 แจ้งเมลแผนเปลี่ยน</h3>
          <span className="rn-meta">
            <b>{data.items.length}</b> รายการที่เปลี่ยน · สัปดาห์ปัจจุบัน W{data.current.week}/{data.current.year}
            · แก้ได้ตั้งแต่ W{data.current.edit_from}
            <span className="rn-file" title={data.plan_file}> · {data.plan_file}</span>
          </span>
        </div>
        <nav className="rn-modes">
          {MODES.map(m => (
            <button key={m.key} className={'rn-mode' + (mode === m.key ? ' on' : '')}
              onClick={() => switchMode(m.key)}>{m.label}</button>
          ))}
        </nav>
      </header>

      {mode === 'people' && (
        <MailRecipients addTeam={addFor} onDirty={setPeopleDirty} onSaved={loadTeams} />
      )}

      {mode !== 'people' && <>
        {mail && !mail.ready && (
          <div className="warnbox">⚠ {mail.reason} — ดูรายการเปลี่ยนแปลงได้ แต่ยังกดส่งไม่ได้</div>
        )}

        {mode === 'team' && (
          <TeamPanel t={teams} pick={pick} setPick={setPick} res={sendRes}
            focus={focusTeam} setFocus={setFocusTeam} onAdd={addRecipientFor} />
        )}

        {mode === 'one' && <>
          {data.mail?.attach_name && (
            <div className="rn-note">
              📎 แนบไฟล์ <b>{data.mail.attach_name}</b> ({(data.mail.attach_bytes / 1024).toFixed(0)} KB)
              ที่มีครบทั้ง {data.items.length} รายการพร้อมเหตุผลเต็ม — ตารางในเนื้อเมล
              ({(data.mail.bytes / 1024).toFixed(0)} KB) แสดงเฉพาะรายการที่ควรดูก่อน
              เพราะ Power Automate ส่งเมลที่ใหญ่เกินไปไม่ผ่าน
            </div>
          )}
          <div className="rn-fields">
            <label>ถึง<input value={to} onChange={e => setTo(e.target.value)}
              placeholder="a@nanyangtextile.com; b@nanyangtextile.com" /></label>
            <label>สำเนา (CC)<input value={cc} onChange={e => setCc(e.target.value)} placeholder="เว้นว่างได้" /></label>
            <label className="wide">หัวข้อ<input value={subject} onChange={e => setSubject(e.target.value)} /></label>
          </div>
        </>}

        <section className="rn-section">
          <div className="rn-section-head">
            <h4>
              รายการที่เปลี่ยน
              {focusName && <> — เฉพาะทีม <b>{focusName}</b>
                <button className="rn-x" title="ดูทุกทีม" onClick={() => setFocusTeam(null)}>✕</button></>}
            </h4>
            <div className="rn-filters">
              {TABS.map(t => {
                const n = counts[t.key] || 0
                if (!n && t.key !== 'all' && tab !== t.key) return null
                return (
                  <button key={t.key}
                    className={'rn-filter' + (tab === t.key ? ' on' : '') + (t.key === 'late_rdd' ? ' warn' : '')}
                    onClick={() => setTab(t.key)}>{t.label} <b>{n}</b></button>
                )
              })}
            </div>
          </div>
          {!!s.late_before && !focusName && (
            <div className="rn-sub">อีก {s.late_before} รายการเกิน RDD อยู่ก่อนแล้ว ไม่ใช่ผลของรอบนี้</div>
          )}
          <div className="rn-table">
            <table className="rn-items">
              <thead>
                <tr>
                  <th>ITEM / ลูกค้า</th><th>Team</th><th>SC</th><th>เครื่อง</th>
                  <th>สัปดาห์ เดิม → ใหม่</th><th className="num">ยอด (กก.)</th><th>ผลกระทบ / เหตุผล</th>
                </tr>
              </thead>
              <tbody>
                {shown.map((it, i) => {
                  const sy = !!(it.old.year && it.new.year && it.old.year !== it.new.year)
                  return (
                    <tr key={it.item + '|' + it.sc.join(',') + i} className={it.late ? 'rn-late' : ''}>
                      <td>
                        <b>{it.item}</b>
                        {it.customer && <div className="rn-sub">{it.customer}</div>}
                      </td>
                      <td>{it.team || '—'}</td>
                      <td className="rn-sc">{it.sc.slice(0, 6).join(', ') || '—'}{it.sc.length > 6 ? ' …' : ''}</td>
                      <td>{it.mc_group || '—'}</td>
                      <td className="nowrap">{span(it.old, sy)} → <b>{span(it.new, sy)}</b></td>
                      <td className="num">
                        {fmt(it.new.qty)}
                        {Math.abs(it.d_qty) > 1 && <div className="rn-sub">เดิม {fmt(it.old.qty)}</div>}
                      </td>
                      <td>
                        {impactText(it)}
                        {it.late && <b className="rn-warn"> · ⚠ เกิน RDD W{it.rdd_week}</b>}
                        {!it.late && it.late_before && <span className="rn-sub"> · เกิน RDD W{it.rdd_week} อยู่ก่อนแล้ว</span>}
                        {!!it.reasons.length && (
                          <ul className="rn-reasons">{it.reasons.map((r, j) => <li key={j}>{r}</li>)}</ul>
                        )}
                      </td>
                    </tr>
                  )
                })}
                {!shown.length && <tr><td colSpan={7} className="rn-empty">ไม่มีรายการในกลุ่มนี้</td></tr>}
              </tbody>
            </table>
          </div>
        </section>

        {/* แถบส่ง — ติดขอบล่างของหน้าต่างเสมอ ไม่ต้องเลื่อนหาหลังตารางยาว */}
        <footer className="rn-foot">
          {msg && <div className="okbox">✅ {msg}</div>}
          {err && <div className="err">{err}</div>}
          <div className="rn-foot-row">
            <input className="rn-note-input" value={note} onChange={e => setNote(e.target.value)}
              placeholder={'หมายเหตุถึงผู้รับ' + (mode === 'team' ? ' (ติดไปทุกฉบับ)' : '')
                + ' — เช่น เลื่อนเพราะรอด้ายล็อตใหม่ ยืนยันกับลูกค้าแล้ว'} />
            <button className="rn-btn" onClick={load} disabled={busy} title="อ่านไฟล์แผนใหม่">↻ รีเฟรช</button>
            {mode === 'team'
              ? <button className="primary rn-send" onClick={sendTeams}
                  disabled={busy || !mail?.ready || !chosen.length}>
                  {busy ? 'กำลังส่ง…' : `📮 ส่ง ${chosen.length} ฉบับ`}
                </button>
              : <button className="primary rn-send" onClick={send}
                  disabled={busy || !mail?.ready || !data.items.length}>
                  {busy ? 'กำลังส่ง…' : `📧 ส่ง ${data.items.length} รายการ`}
                </button>}
          </div>
        </footer>
      </>}
    </div>
  )
}

/* ตารางการส่งรายทีม — ต้องเห็นครบก่อนกดว่า "ฉบับไหนไปหาใคร" เพราะกดครั้งเดียวยิงหลายฉบับ
   ทีมที่ยังไม่มีผู้รับก็ต้องอยู่ในตารางนี้ด้วย (ติ๊กไม่ได้) ไม่ใช่หายไปเงียบ ๆ จนไม่มีใครรู้ว่าตกหล่น
   ผู้รับกลุ่ม `*` (ได้ทุกฉบับ) แยกขึ้นไปบรรทัดเดียวด้านบน — เดิมซ้ำกันทุกแถวจนอ่านตารางไม่ออก */
function TeamPanel({ t, pick, setPick, res, focus, setFocus, onAdd }) {
  if (!t) return <div className="rn-loading">กำลังจัดกลุ่มตามทีม…</div>
  if (t.error) return <div className="err">อ่านรายชื่อผู้รับไม่ได้: {t.error}</div>
  const rows = t.teams || []
  const readyKeys = rows.filter(r => r.ready).map(r => r.key)
  const allOn = readyKeys.length > 0 && readyKeys.every(k => pick?.has(k))
  const toggle = k => setPick(p => {
    const n = new Set(p || []); n.has(k) ? n.delete(k) : n.add(k); return n
  })
  const toggleAll = () => setPick(allOn ? new Set() : new Set(readyKeys))
  const byKey = Object.fromEntries((res?.results || []).map(r => [r.key, r]))
  const allTo = new Set(t.all_to || []), allCc = new Set(t.all_cc || [])
  const common = [...(t.all_to || []), ...(t.all_cc || [])]

  return (
    <section className="rn-section">
      {t.test_mode ? (
        <div className="rn-mode-bar test">
          🧪 <b>โหมดทดสอบ</b> — ทุกฉบับส่งถึง <b>{(t.test_to || []).join(', ') || '(ยังไม่ได้ตั้ง)'}</b> เท่านั้น
          <span className="rn-sub"> · ปิดได้ที่ Setting → 📧 Mail</span>
        </div>
      ) : (
        <div className="rn-mode-bar live">
          ⚠ <b>โหมดส่งจริง</b> — กดส่งแล้วเมลออกไปหาฝ่ายขายทันที เรียกคืนไม่ได้
        </div>
      )}
      {(t.warnings || []).map((w, i) => <div className="warnbox" key={i}>⚠ {w}</div>)}

      {!!common.length && (
        <div className="rn-common">
          <span className="rn-common-label">ได้ทุกฉบับ</span>
          <Who list={t.all_to} max={8} empty="" />
          {!!(t.all_cc || []).length && <><span className="rn-sub">สำเนา</span><Who list={t.all_cc} max={8} /></>}
        </div>
      )}

      <div className="rn-table rn-team-table">
        <table className="rn-teams-grid">
          <thead>
            <tr>
              <th className="chk">
                <input type="checkbox" checked={allOn} disabled={!readyKeys.length}
                  onChange={toggleAll} title="เลือก/ยกเลิกทุกทีมที่พร้อมส่ง" />
              </th>
              <th>Team</th><th className="num">รายการ</th><th>ผู้รับ (To)</th><th>สำเนา (CC)</th><th>สถานะ</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => {
              const done = byKey[r.key]
              const own = r.to.filter(m => !allTo.has(m))
              const ownCc = r.cc.filter(m => !allCc.has(m))
              return (
                <tr key={r.key}
                  className={(r.ready ? '' : 'off') + (focus === r.key ? ' focus' : '')}
                  onClick={() => setFocus(focus === r.key ? null : r.key)}
                  title="คลิกเพื่อดูเฉพาะรายการของทีมนี้ด้านล่าง">
                  <td className="chk" onClick={e => e.stopPropagation()}>
                    <input type="checkbox" disabled={!r.ready} checked={!!pick?.has(r.key)}
                      onChange={() => toggle(r.key)} />
                  </td>
                  <td><b>{r.team}</b></td>
                  <td className="num">{r.items}</td>
                  <td><Who list={own} empty={r.ready ? 'เฉพาะผู้รับทุกฉบับ' : '—'} /></td>
                  <td><Who list={ownCc} /></td>
                  <td onClick={e => e.stopPropagation()}>
                    {done
                      ? (done.ok ? <span className="rn-badge ok">✅ ส่งแล้ว</span>
                        : <span className="rn-badge bad" title={done.error}>✖ ส่งไม่สำเร็จ</span>)
                      : r.ready ? <span className="rn-badge ok">● พร้อมส่ง</span>
                        : r.key === UNASSIGNED
                          ? <span className="rn-badge muted" title={r.reason}>ไม่มีทีมในข้อมูล</span>
                          : <button className="rn-add" onClick={() => onAdd(r.team)}
                              title={r.reason}>+ เพิ่มผู้รับ</button>}
                  </td>
                </tr>
              )
            })}
            {!rows.length && <tr><td colSpan={6} className="rn-empty">ไม่มีรายการเปลี่ยนแปลงให้แจ้ง</td></tr>}
          </tbody>
        </table>
      </div>

      {!!(t.idle_teams || []).length && (
        <div className="rn-sub" style={{ marginTop: 6 }}>
          ทีมที่มีรายชื่อแต่รอบนี้ไม่มีงานเปลี่ยน (ไม่ต้องส่ง): {t.idle_teams.join(', ')}
        </div>
      )}
    </section>
  )
}

function splitAddr(v) {
  return String(v || '').split(/[;,]/).map(s => s.trim()).filter(Boolean)
}

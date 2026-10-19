import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'

/* รายชื่อผู้รับเมล (master ถาวร ชีท Sales_Mail ใน MasterMC) — ตั้งครั้งเดียว ใช้ทุกรอบจนกว่าจะแก้
   หน้าจอเป็น "คนละ 1 แถว" แล้วเลือกจาก dropdown ว่าคนนี้รับเมลของทีมไหนบ้าง (ได้หลายทีม)
   ส่วนในไฟล์ยังเก็บแบบเดิม 1 แถว = 1 ทีม × 1 คน — ตอนโหลดยุบรวมตามอีเมล ตอนบันทึกแตกกลับ
   (backend / การจับคู่ตอนส่งเมลไม่ต้องรู้เรื่องหน้าตานี้เลย)

   คนที่ไม่ได้แตะ → เขียนแถวเดิมกลับไปทุกตัวอักษร : ชีทเดิมมีคนเดียวกันที่เป็น To ทีมหนึ่ง
   แต่ CC อีกทีมได้ ถ้ายุบแล้วแตกกลับทุกคน ค่าที่ต่างกันรายทีมจะหายเงียบ ๆ

   **ชื่อทีมต้องตรงกับที่อยู่ในข้อมูลเป๊ะ ๆ** ระบบไม่เดาชื่อให้ (THVN ไม่เท่ากับ THVN-1)
   dropdown จึงเสนอ "ทีมที่มีงานจริงในแผนรอบนี้" ก่อน การพิมพ์ชื่อทีมใหม่เป็นทางรอง

   props: addTeam = ทีมที่กด "+ เพิ่มผู้รับ" มาจากหน้าส่ง (เปิดมาแล้วเพิ่มคนใหม่ที่ติ๊กทีมนั้นไว้ให้)
          onDirty(bool) = บอกหน้าแม่ว่ามีของยังไม่บันทึก (กันเปลี่ยนแท็บแล้วหาย)
          onSaved() = บันทึกสำเร็จ → หน้าแม่โหลดตารางส่งรายทีมใหม่ */

const ALL = '*'
const norm = s => String(s || '').trim().replace(/\s+/g, ' ').toUpperCase()
const isMail = s => /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(String(s || '').trim())
const isOff = v => String(v || 'Y').toUpperCase().startsWith('N')
const newId = () => 'n' + Date.now() + Math.random().toString(36).slice(2)
const FIELDS = ['TEAM', 'NAME', 'EMAIL', 'ROLE', 'ACTIVE', 'NOTE']
const NONE = '\u0000none'   // ตัวกรอง "ยังไม่ได้เลือกทีม" — ไม่ชนชื่อทีมจริงแน่นอน
// สีวงกลมตัวอักษรหน้าชื่อ — คนเดิมได้สีเดิมทุกครั้ง
const hue = s => [...String(s || '')].reduce((h, c) => (h * 31 + c.charCodeAt(0)) % 360, 7)

// แถวในชีท → คนละ 1 รายการ (ยุบตามอีเมล ; แถวที่ยังไม่มีอีเมลเป็นคนแยกของมันเอง)
function toPeople(rows) {
  const byMail = new Map(), out = []
  rows.forEach((r, i) => {
    const row = Object.fromEntries(FIELDS.map(k => [k, String(r[k] ?? '')]))
    const k = row.EMAIL.trim().toLowerCase()
    let p = k && byMail.get(k)
    if (!p) {
      p = { _id: 'r' + i, NAME: '', EMAIL: row.EMAIL.trim(), ROLE: '', ACTIVE: '', NOTE: '',
        teams: [], orig: [], mixed: false, dirty: false }
      if (k) byMail.set(k, p)
      out.push(p)
    }
    p.orig.push(row)
    if (!p.NAME) p.NAME = row.NAME
    if (!p.NOTE) p.NOTE = row.NOTE
    const role = (row.ROLE || 'TO').toUpperCase(), act = isOff(row.ACTIVE) ? 'N' : 'Y'
    if (p.ROLE && (p.ROLE !== role || p.ACTIVE !== act)) p.mixed = true
    if (!p.ROLE) { p.ROLE = role; p.ACTIVE = act }
    const t = row.TEAM.trim()
    if (t && !p.teams.some(x => norm(x) === norm(t))) p.teams.push(t)
  })
  // กลุ่ม * (ได้ทุกฉบับ) ขึ้นก่อน แล้วเรียงตามทีมแรก → ชื่อ
  const first = p => (p.teams.some(t => t === ALL) ? ' ' : norm(p.teams[0] || '~'))
  return out.sort((a, b) => first(a).localeCompare(first(b)) || a.NAME.localeCompare(b.NAME))
}

// คน → แถวในชีท (คนที่ไม่ได้แก้ = แถวเดิม)
function toRows(people) {
  return people.flatMap(p => {
    if (!p.dirty) return p.orig
    if (!p.EMAIL.trim() && !p.teams.length) return []
    const base = { NAME: p.NAME.trim(), EMAIL: p.EMAIL.trim(), ROLE: p.ROLE || 'TO',
      ACTIVE: p.ACTIVE || 'Y', NOTE: p.NOTE }
    return p.teams.length ? p.teams.map(t => ({ TEAM: t, ...base })) : [{ TEAM: '', ...base }]
  })
}

export default function MailRecipients({ addTeam, onDirty, onSaved }) {
  const [people, setPeople] = useState(null)
  const [rev, setRev] = useState(null)
  const [meta, setMeta] = useState({ plan_teams: [], warnings: [], sheet: '' })
  const [dirty, setDirtyState] = useState(false)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [busy, setBusy] = useState(false)
  const [q, setQ] = useState('')
  const [sel, setSel] = useState(() => (addTeam ? norm(addTeam) : ''))   // ทีมที่เลือกในแถบซ้าย
  const [focusId, setFocusId] = useState(null)   // คนที่เพิ่งเพิ่ม → เคอร์เซอร์ไปรอที่ช่องอีเมล
  const [canEdit, setCanEdit] = useState(false)  // แก้ master ได้เฉพาะ admin (backend กันซ้ำอีกชั้น)
  const saveRef = useRef(null)

  const setDirty = v => { setDirtyState(v); onDirty?.(v) }

  useEffect(() => {
    api.me().then(m => {
      const ok = m.role === 'admin' || m.role === 'superadmin'
      setCanEdit(ok)
      load(true, ok)
    }).catch(() => load(true, false))
  }, [])

  // Ctrl+S = บันทึก
  useEffect(() => {
    const h = e => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
        e.preventDefault(); saveRef.current?.()
      }
    }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [])

  async function load(first = false, editable = canEdit) {
    if (!first && dirty && !window.confirm('ทิ้งสิ่งที่แก้ไว้แล้วโหลดรายชื่อใหม่จากไฟล์?')) return
    setBusy(true); setErr(''); setMsg('')
    try {
      const d = await api.mailRecipients()
      let ps = toPeople(d.rows || [])
      setRev(d.rev)
      setMeta({ plan_teams: d.plan_teams || [], warnings: d.warnings || [], sheet: d.sheet || '' })
      // มาจากปุ่ม "+ เพิ่มผู้รับ" ของทีมที่ยังไม่มีคนรับ → เพิ่มคนใหม่ที่ติ๊กทีมนั้นไว้ให้
      if (first && addTeam && editable) {
        const p = blank([addTeam])
        ps = [p, ...ps]
        setFocusId(p._id)
        setPeople(ps); setDirty(true)
      } else {
        setPeople(ps); setDirty(false)
      }
    } catch (e) { setErr(e.message || String(e)) } finally { setBusy(false) }
  }

  async function save() {
    if (!people || busy) return
    const noTeam = people.filter(p => p.dirty && p.EMAIL.trim() && !p.teams.length && !isOff(p.ACTIVE))
    if (noTeam.length) {
      setErr(`ยังไม่ได้เลือกทีมให้: ${noTeam.map(p => p.NAME || p.EMAIL).join(', ')}`); return
    }
    setBusy(true); setErr(''); setMsg('')
    try {
      const rows = toRows(people)
      const res = await api.saveMailRecipients(rows, rev)
      // ไม่โหลดใหม่ทั้งชุด (อ่าน MasterMC + คำนวณแผนซ้ำ = ช้า) — แถวที่เพิ่งเขียนคือของจริงแล้ว
      setPeople(ps => ps.filter(p => p.EMAIL.trim() || p.teams.length)
        .map(p => (p.dirty ? { ...p, dirty: false, mixed: false, orig: toRows([p]) } : p)))
      setRev(res.rev); setDirty(false)
      setMsg(`บันทึกแล้ว — ${res.rows} แถว (ใช้กับการส่งเมลทุกรอบต่อจากนี้)`)
      onSaved?.()
    } catch (e) { setErr(e.message || String(e)) } finally { setBusy(false) }
  }
  saveRef.current = dirty && canEdit ? save : null

  function blank(teams = []) {
    return { _id: newId(), NAME: '', EMAIL: '', ROLE: 'TO', ACTIVE: 'Y', NOTE: '',
      teams, orig: [], mixed: false, dirty: true }
  }
  const touch = () => { setDirty(true); setMsg('') }
  const patch = (id, k, v) => {
    setPeople(ps => ps.map(p => (p._id === id ? { ...p, [k]: v, dirty: true } : p)))
    touch()
  }
  const removePerson = id => { setPeople(ps => ps.filter(p => p._id !== id)); touch() }
  const addPerson = (teams = []) => {
    const p = blank(teams)
    setPeople(ps => [p, ...ps])
    setFocusId(p._id); setQ('')
    touch()
  }

  // ตัวเลือกทีมใน dropdown: * → ทีมที่มีงานรอบนี้ (มากไปน้อย) → ทีมอื่นที่มีอยู่ในรายชื่อ
  const teamOptions = useMemo(() => {
    const seen = new Set([ALL]), plan = [], other = []
    for (const t of meta.plan_teams) {
      if (seen.has(t.key)) continue
      seen.add(t.key); plan.push({ team: t.team, key: t.key, items: t.items })
    }
    for (const p of people || []) for (const t of p.teams) {
      const k = norm(t)
      if (seen.has(k)) continue
      seen.add(k); other.push({ team: t, key: k, items: null })
    }
    other.sort((a, b) => a.key.localeCompare(b.key))
    return { plan, other }
  }, [people, meta])

  // ตัวกรองจากแถบซ้าย : sel = '' ทุกคน | '*' ★ ทุกทีม | NONE ยังไม่เลือกทีม | คีย์ทีม (norm)
  const inSel = (p, key) => key === '' ? true
    : key === NONE ? !p.teams.length
      : p.teams.some(t => norm(t) === key)

  // คนที่โผล่ในมุมมองนี้แล้วให้ค้างอยู่จนกว่าจะเปลี่ยนมุมมอง — กดเอาทีมออกแล้วแถวไม่หายไปต่อหน้า
  const stick = useRef({ key: null, ids: new Set() })
  const visible = useMemo(() => {
    if (!people) return []
    const vkey = sel + '\u0001' + q
    if (stick.current.key !== vkey) stick.current = { key: vkey, ids: new Set() }
    const ids = stick.current.ids
    const s = q.trim().toLowerCase()
    return people.filter(p => {
      const ok = ids.has(p._id)
        || (!p.EMAIL && !p.NAME)   // คนที่เพิ่งเพิ่ม (ยังว่าง) โชว์เสมอ
        || (inSel(p, sel) && (!s || [p.NAME, p.EMAIL, p.NOTE, ...p.teams]
          .some(v => String(v || '').toLowerCase().includes(s))))
      if (ok) ids.add(p._id)
      return ok
    })
  }, [people, sel, q])

  // ทีมที่มีงานในแผนรอบนี้แต่ยังไม่มีใครรับเมล — เรื่องที่ต้องเห็นก่อนอย่างอื่น
  const missing = useMemo(() => {
    const have = new Set()
    for (const p of people || []) {
      if (!isMail(p.EMAIL) || isOff(p.ACTIVE)) continue
      p.teams.forEach(t => have.add(norm(t)))
    }
    return meta.plan_teams.filter(t => !have.has(t.key))
  }, [people, meta])

  // จำนวนคนที่ใช้งานอยู่ต่อทีม (สำหรับแถบซ้าย)
  const counts = useMemo(() => {
    const c = new Map()
    for (const p of people || []) {
      if (isOff(p.ACTIVE)) continue
      const ks = p.teams.length ? new Set(p.teams.map(norm)) : [NONE]
      ks.forEach(k => c.set(k, (c.get(k) || 0) + 1))
    }
    return c
  }, [people])

  if (err && !people) return <div className="err">โหลดรายชื่อไม่ได้: {err}</div>
  if (!people) return <div className="rn-loading">กำลังอ่านรายชื่อ…</div>

  const active = people.filter(p => !isOff(p.ACTIVE)).length
  const badCount = people.filter(p => p.EMAIL && !isMail(p.EMAIL)).length
  const noTeamCount = people.filter(p => !p.teams.length).length
  const ro = !canEdit

  const missKeys = new Set(missing.map(t => t.key))
  const planOk = teamOptions.plan.filter(t => !missKeys.has(t.key))
  const allTeams = [...teamOptions.plan, ...teamOptions.other]
  const selTeam = allTeams.find(t => t.key === sel)
  const selMissing = missKeys.has(sel)
  const starCount = counts.get(ALL) || 0
  const title = sel === '' ? 'ทุกคน' : sel === ALL ? '★ รับทุกทีม' : sel === NONE ? 'ยังไม่ได้เลือกทีม'
    : (selTeam ? selTeam.team : sel)
  // เพิ่มคนจากมุมมองทีมไหน → ติ๊กทีมนั้นไว้ให้เลย
  const addHere = () => addPerson(sel === '' || sel === NONE ? [] : sel === ALL ? [ALL] : [selTeam ? selTeam.team : sel])
  const pick = k => { setSel(k); setQ('') }

  const sideItem = (k, label, n, extra = {}) => (
    <button key={k} type="button" className={'mr-side-item' + (sel === k ? ' on' : '') + (extra.cls ? ' ' + extra.cls : '')}
      onClick={() => pick(k)} title={extra.title}>
      <span className="mr-side-name">{label}</span>
      {extra.items != null && <span className="mr-side-items" title="รายการที่เปลี่ยนรอบนี้">{extra.items} งาน</span>}
      <span className="mr-side-n">{n}</span>
    </button>
  )

  return (
    <div className="mr-wrap">
      <div className="mr-intro">
        รายชื่อ <b>master</b> — ตั้งครั้งเดียว ใช้ทุกรอบจนกว่าจะแก้ · คนหนึ่งรับได้หลายทีม ·
        <b> ★ ทุกทีม</b> = ได้ทุกฉบับ
        {ro && <span className="mr-ro"> · 🔒 ดูได้อย่างเดียว — แก้ได้เฉพาะ Knit / Super Admin</span>}
      </div>
      {meta.warnings.map((w, i) => <div className="warnbox" key={i}>⚠ {w}</div>)}

      <div className="mr-layout">
        {/* ---- แถบซ้าย : ทีม ---- */}
        <aside className="mr-side">
          <input className="mr-search" type="search" value={q}
            onChange={e => { setQ(e.target.value); if (e.target.value) setSel('') }}
            placeholder="🔍 ค้นหาชื่อ / อีเมล" />
          <div className="mr-side-list">
            {sideItem('', 'ทุกคน', people.length, { title: `ใช้งาน ${active} คน` })}
            {sideItem(ALL, '★ รับทุกทีม', starCount, { cls: 'star' })}
            {noTeamCount > 0 && sideItem(NONE, 'ยังไม่ได้เลือกทีม', noTeamCount, { cls: 'warn' })}

            {!!missing.length && <div className="mr-side-sec miss">⚠ ยังไม่มีผู้รับ — จะไม่ได้เมล</div>}
            {missing.map(t => sideItem(t.key, t.team, 0, { cls: 'miss', items: t.items }))}

            {!!planOk.length && <div className="mr-side-sec">มีงานเปลี่ยนรอบนี้</div>}
            {planOk.map(t => sideItem(t.key, t.team, counts.get(t.key) || 0, { items: t.items }))}

            {!!teamOptions.other.length && <div className="mr-side-sec">ทีมอื่น</div>}
            {teamOptions.other.map(t => sideItem(t.key, t.team, counts.get(t.key) || 0))}
          </div>
        </aside>

        {/* ---- ฝั่งขวา : คนในทีมที่เลือก ---- */}
        <section className="mr-main">
          <div className="mr-head">
            <div className="mr-head-t">
              <h3>{q ? `ค้นหา “${q}”` : title}</h3>
              <span className="mr-head-sub">
                {visible.length} คน
                {selTeam?.items != null && <> · มีงานเปลี่ยนรอบนี้ {selTeam.items} รายการ</>}
                {selTeam && starCount > 0 && <> · <span className="mr-star-note">+ อีก {starCount} คนที่รับ ★ ทุกทีมได้เมลนี้ด้วย</span></>}
              </span>
            </div>
            {!ro && <button className="primary mr-add" onClick={addHere}>+ เพิ่มคน{selTeam ? 'ในทีมนี้' : ''}</button>}
          </div>

          {selMissing && !visible.length ? (
            <div className="mr-empty miss">
              <div className="mr-empty-ic">📭</div>
              <b>ทีม {title} ยังไม่มีผู้รับเมล</b>
              <span>มีงานเปลี่ยน {selTeam?.items ?? 0} รายการรอบนี้ แต่จะไม่มีใครได้เมลของทีมนี้</span>
              {!ro && <button className="primary" onClick={addHere}>+ เพิ่มผู้รับให้ทีมนี้</button>}
            </div>
          ) : !visible.length ? (
            <div className="mr-empty">{q ? `ไม่พบ “${q}”` : 'ยังไม่มีรายชื่อ'}</div>
          ) : (
            <div className="mr-list">
              {visible.map(p => {
                const bad = p.EMAIL && !isMail(p.EMAIL)
                const off = isOff(p.ACTIVE)
                const role = (p.ROLE || 'TO').toUpperCase()
                const ini = (p.NAME || p.EMAIL || '?').trim().charAt(0).toUpperCase() || '?'
                return (
                  <div key={p._id} className={'mr-row' + (off ? ' mr-off' : '') + (p.dirty ? ' mr-changed' : '')}>
                    <span className="mr-ava" style={{ '--h': hue(p.EMAIL || p.NAME) }}>{ini}</span>
                    <div className="mr-who">
                      <input className="mr-name" value={p.NAME} placeholder="ชื่อ" disabled={ro}
                        onChange={e => patch(p._id, 'NAME', e.target.value)} />
                      <input className={'mr-mail' + (bad ? ' mr-bad' : '')} value={p.EMAIL} disabled={ro}
                        autoFocus={p._id === focusId}
                        placeholder="name@nanyangtextile.com" title={bad ? 'รูปแบบอีเมลไม่ถูกต้อง' : ''}
                        onChange={e => patch(p._id, 'EMAIL', e.target.value)} />
                    </div>
                    <div className="mr-teams">
                      <TeamPicker value={p.teams} options={teamOptions} disabled={ro}
                        onChange={v => patch(p._id, 'teams', v)} />
                      {p.mixed && !p.dirty && (
                        <div className="mr-mixed" title="ในไฟล์ตั้ง To/CC หรือเปิด/พัก ต่างกันตามทีม — ถ้าแก้แถวนี้ จะใช้ค่าเดียวกันทุกทีม">
                          ตั้ง To/CC ต่างกันตามทีม
                        </div>
                      )}
                    </div>
                    <div className="mr-seg" role="group">
                      {['TO', 'CC'].map(v => (
                        <button key={v} className={role === v ? 'on' : ''} disabled={ro}
                          title={v === 'TO' ? 'ผู้รับหลัก' : 'สำเนา'}
                          onClick={() => role !== v && patch(p._id, 'ROLE', v)}>{v === 'TO' ? 'To' : 'CC'}</button>
                      ))}
                    </div>
                    <label className="mr-switch" title={off ? 'พักไว้ — ไม่ได้รับเมล' : 'ใช้งาน'}>
                      <input type="checkbox" checked={!off} disabled={ro}
                        onChange={e => patch(p._id, 'ACTIVE', e.target.checked ? 'Y' : 'N')} />
                      <span />
                    </label>
                    {ro ? <span /> : <button className="mr-del" title="ลบคนนี้ (ทุกทีม)"
                      onClick={() => removePerson(p._id)}>✕</button>}
                  </div>
                )
              })}
            </div>
          )}
        </section>
      </div>

      {/* แถบบันทึก — ติดขอบล่างของหน้าต่าง */}
      <footer className={'rn-foot mr-foot' + (dirty ? ' dirty' : '')}>
        {err && <div className="err">{err}</div>}
        <div className="rn-foot-row">
          <span className="mr-state">
            {dirty ? <>● มีการแก้ไขที่ยังไม่บันทึก{badCount ? ` · อีเมลผิดรูปแบบ ${badCount} คน` : ''}</>
              : msg ? <>✅ {msg}</>
                : <>เก็บที่ {meta.sheet || 'MasterMC/Sales_Mail'}</>}
          </span>
          <button className="rn-btn" onClick={() => load()} disabled={busy}>
            {dirty ? 'ยกเลิกการแก้ไข' : '↻ โหลดใหม่'}
          </button>
          {!ro && (
            <button className="primary rn-send" disabled={busy || !dirty} onClick={save} title="Ctrl+S">
              {busy ? 'กำลังบันทึก…' : '💾 บันทึก'}
            </button>
          )}
        </div>
      </footer>
    </div>
  )
}

/* dropdown เลือกทีม (ติ๊กได้หลายทีม) + พิมพ์ค้นหา / พิมพ์ชื่อทีมใหม่แล้ว Enter */
function TeamPicker({ value, options, disabled, onChange }) {
  const [open, setOpen] = useState(false)
  const [q, setQ] = useState('')
  const [hint, setHint] = useState('')
  const box = useRef(null)
  const keys = new Set(value.map(norm))

  useEffect(() => {
    if (!open) return
    const onDown = e => { if (box.current && !box.current.contains(e.target)) setOpen(false) }
    const onKey = e => { if (e.key === 'Escape') setOpen(false) }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => { document.removeEventListener('mousedown', onDown); document.removeEventListener('keydown', onKey) }
  }, [open])

  const toggle = team => {
    const k = norm(team)
    onChange(keys.has(k) ? value.filter(t => norm(t) !== k) : [...value, team])
  }

  const s = norm(q)
  const hit = o => !s || o.key.includes(s)
  const plan = options.plan.filter(hit)
  const other = options.other.filter(hit)
  const exact = s && (s === ALL || [...options.plan, ...options.other].some(o => o.key === s))

  function addTyped() {
    const t = q.trim().replace(/\s+/g, ' ')
    if (!t) return
    if (/[,;]/.test(t)) { setHint('ชื่อทีมห้ามมีลูกน้ำ — เลือกทีละทีม'); return }
    const found = [...options.plan, ...options.other].find(o => o.key === norm(t))
    if (!keys.has(norm(t))) onChange([...value, found ? found.team : t])
    setQ(''); setHint('')
  }

  const row = o => (
    <label key={o.key} className={'mr-opt' + (keys.has(o.key) ? ' on' : '')}>
      <input type="checkbox" checked={keys.has(o.key)} onChange={() => toggle(o.team)} />
      <span className="mr-opt-name">{o.team}</span>
      {o.items != null && <span className="mr-opt-n">{o.items}</span>}
    </label>
  )

  const allOn = keys.has(ALL)
  const named = value.filter(t => t !== ALL)
  const chip = (t, label, cls = '') => (
    <span className={'mr-tchip' + cls} key={t}>
      {label}
      {!disabled && <button type="button" title="เอาทีมนี้ออก" onClick={() => toggle(t)}>×</button>}
    </span>
  )
  return (
    <div className="mr-pick" ref={box}>
      <div className="mr-pick-chips">
        {!value.length && disabled && <span className="mr-pick-ph">ยังไม่ได้เลือกทีม</span>}
        {allOn && chip(ALL, '★ ทุกทีม', ' all')}
        {named.map(t => chip(t, t))}
        {!disabled && (
          <button type="button" className={'mr-pick-add' + (value.length ? '' : ' empty') + (open ? ' on' : '')}
            onClick={() => setOpen(o => !o)}>
            {value.length ? '+ ทีม' : '+ เลือกทีม'}
          </button>
        )}
      </div>
      {open && (
        <div className="mr-pop">
          <input className="mr-pop-q" autoFocus value={q} placeholder="🔍 ค้นหา / พิมพ์ชื่อทีมใหม่แล้ว Enter"
            onChange={e => { setQ(e.target.value); setHint('') }}
            onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); addTyped() } }} />
          <div className="mr-pop-list">
            {(!s || '★ ทุกทีม *'.includes(q.trim())) && (
              <label className={'mr-opt all' + (allOn ? ' on' : '')}>
                <input type="checkbox" checked={allOn} onChange={() => toggle(ALL)} />
                <span className="mr-opt-name">★ ทุกทีม</span>
                <span className="mr-opt-sub">ได้ทุกฉบับ</span>
              </label>
            )}
            {!!plan.length && <div className="mr-pop-sec">มีงานในแผนรอบนี้</div>}
            {plan.map(row)}
            {!!other.length && <div className="mr-pop-sec">ทีมอื่น</div>}
            {other.map(row)}
            {s && !exact && (
              <button type="button" className="mr-opt-new" onClick={addTyped}>
                + ใช้ชื่อทีม “{q.trim()}” <span className="mr-opt-sub">ต้องตรงกับชื่อทีมในข้อมูลเป๊ะ ๆ</span>
              </button>
            )}
          </div>
          {hint && <div className="mr-pop-hint">{hint}</div>}
          <div className="mr-pop-foot">
            <span>เลือกแล้ว {value.length} ทีม</span>
            {!!value.length && <button type="button" onClick={() => onChange([])}>ล้าง</button>}
            <button type="button" className="ok" onClick={() => setOpen(false)}>เสร็จ</button>
          </div>
        </div>
      )}
    </div>
  )
}

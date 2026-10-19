import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import Masters from './Masters.jsx'
import UserAdmin from './UserAdmin.jsx'
import McHub from './McHub.jsx'
import S9Hub from './S9Hub.jsx'
import Schedule from './Schedule.jsx'
import MailRecipients from './MailRecipients.jsx'
import { catalogFor, sheetsOfGroup, MOVED_KEYS, ownerOfKey, historyArea } from '../masterCatalog.js'

// หน้า "ตั้งค่าระบบ" — กติกาที่ระบบใช้ตอนจัดแผน
//
// หลักการของหน้านี้: ผู้ใช้ไม่ต้องรู้อะไรเกี่ยวกับโค้ดเลย
//   • ไม่โชว์ชื่อตัวแปร / ชื่อไฟล์ / ศัพท์เทคนิค
//   • ทุกค่าบอกชัดว่า "ตอนนี้เป็นแบบไหน" และ "ผลคืออะไร"
//   • เปิด/ปิด = ปุ่ม 2 ทาง · ตัวเลข = ปุ่ม − + · รายการ = ป้ายชื่อ · ตาราง = แถวเพิ่ม/ลบได้
//   • กดบันทึกแล้วมีหน้าต่างสรุปให้อ่านก่อนยืนยันเสมอ
//
// การจัดหน้า: เมนูกลุ่มอยู่ซ้าย (ทีละกลุ่ม ไม่ต้องเลื่อนผ่าน 59 ข้อ) แถบบันทึกลอยอยู่ล่างจอ
// ข้อที่ปกติไม่ต้องแตะ (advanced) พับไว้ใต้ปุ่ม "ดูเพิ่มเติม" ของแต่ละกลุ่ม
// ชื่อ/คำอธิบาย/ตัวอย่าง/หน่วย ทั้งหมดมาจาก backend (plan_config.py) — เพิ่มค่าใหม่ที่นั่นที่เดียว

const fmtTime = iso => (iso ? new Date(iso).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' }) : '-')

const isSimpleNum = it => it.type === 'int' || it.type === 'float'

// ค่าจาก server → ค่าที่ใช้ในช่องกรอก
//   bool   → boolean            ตัวเลข → ข้อความ (เพื่อให้ลบจนว่างได้)
//   list   → array ของข้อความ   table  → array ของ {คอลัมน์: ข้อความ}
export function toDraft(it) {
  if (it.type === 'bool') return !!it.value
  if (it.type === 'list') return (it.value || []).map(v => String(v))
  if (it.type === 'table') {
    return (it.value || []).map(r =>
      Object.fromEntries((it.columns || []).map(c => [c.key, String(r?.[c.key] ?? '')])))
  }
  return (it.value ?? '') === '' ? '' : String(it.value)
}

export function defDraft(it) {
  if (it.type === 'bool') return !!it.default
  if (it.type === 'list') return (it.default || []).map(v => String(v))
  if (it.type === 'table') {
    return (it.default || []).map(r =>
      Object.fromEntries((it.columns || []).map(c => [c.key, String(r?.[c.key] ?? '')])))
  }
  return (it.default ?? '') === '' ? '' : String(it.default)
}

export const same = (a, b) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null)

const flat = cfg => cfg.groups.flatMap(g => g.items)

// ข้อความค่าปัจจุบันของช่องที่ผู้ใช้กำลังแก้ (ยังไม่บันทึก) — ให้เห็นหน่วยเหมือนฝั่ง server
export function draftText(it, v) {
  if (it.type === 'bool') return v ? 'เปิด' : 'ปิด'
  if (it.type === 'list') {
    const xs = (v || []).map(x => String(x).trim()).filter(Boolean)
    if (!xs.length) return 'ไม่มีรายการ'
    return `${xs.length} รายการ: ${xs.slice(0, 4).join(', ')}` + (xs.length > 4 ? ' …' : '')
  }
  if (it.type === 'table') {
    const rows = (v || []).filter(r => Object.values(r).some(x => String(x).trim() !== ''))
    return rows.length ? `${rows.length} รายการ` : 'ไม่มีรายการ'
  }
  if (String(v ?? '').trim() === '') return 'ไม่จำกัด'
  return `${v} ${it.unit || ''}`.trim()
}

// ค่าที่ส่งกลับ server
export function toPayload(it, v) {
  if (it.type === 'bool') return !!v
  if (it.type === 'list') return (v || []).map(x => String(x).trim()).filter(Boolean)
  if (it.type === 'table') {
    return (v || []).filter(r => Object.values(r).some(x => String(x).trim() !== ''))
  }
  return String(v ?? '').trim() === '' ? null : v
}

// ค่าในช่องหลังกดปุ่ม − / + หนึ่งครั้ง (dir = -1 | +1) — ขยับทีละ it.step (ไม่ระบุ = 1) แล้วหนีบเข้าช่วง min–max
export function stepped(it, cur, dir) {
  const n = Number(String(cur ?? '').trim())
  let v = (Number.isFinite(n) ? n : Number(it.default) || 0) + dir * (it.step || 1)
  if (it.min !== null && it.min !== undefined) v = Math.max(it.min, v)
  if (it.max !== null && it.max !== undefined) v = Math.min(it.max, v)
  return String(Math.round(v * 100) / 100)
}

// startAt = 'master' → เปิดหน้ามาที่หมวด "ข้อมูลหลัก" เลย (มาจากปุ่ม "ไปหน้าแก้ไข" ของแท็บ Master Data)
export default function PlanConfig({ startAt, role }) {
  // ประวัติการแก้ไขเห็นได้เฉพาะ superadmin (backend ก็ตอบ 403 ให้คนอื่น)
  const isSuper = role === 'superadmin'
  const [cfg, setCfg] = useState(null)
  const [draft, setDraft] = useState({})
  const [q, setQ] = useState('')
  // group | changed | history | master
  const [view, setView] = useState(
    startAt === 'master' ? { kind: 'data', id: 'mc-hub' } : { kind: 'group', name: '' })
  const [openAdv, setOpenAdv] = useState({})     // {ชื่อกลุ่ม: true} = กาง "ดูเพิ่มเติม" ไว้
  const [openWhy, setOpenWhy] = useState({})     // {key: true} = กาง "ปรับแล้วเกิดอะไร"
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [saving, setSaving] = useState(false)
  const [confirm, setConfirm] = useState(null)   // รายการที่จะเปลี่ยน (เปิดหน้าต่างยืนยัน)
  const [hist, setHist] = useState(null)         // {items, total} ; null = ยังไม่โหลด
  const [histBusy, setHistBusy] = useState(false)
  // loadHist ถูกเรียกจาก subscription ที่จับ closure เก่าไว้ → อ่านค่าล่าสุดผ่าน ref
  const histRef = useRef(null)
  histRef.current = hist
  // เคยเข้าหมวด "ข้อมูลหลัก" แล้วหรือยัง — เข้าแล้วเก็บ component ไว้ (ซ่อนด้วย CSS)
  // ไม่งั้นสลับหมวดทีเดียว ชีทที่เปิดค้างกับสิ่งที่พิมพ์ค้างหายหมด
  const [files, setFiles] = useState(null)      // รายการไฟล์ Master (ไว้เช็กว่าชีทมีจริงไหม)
  const [sheet, setSheet] = useState(null)      // ตารางที่กำลังเปิดจากท้ายกลุ่ม
  const mainRef = useRef(null)

  const items = useMemo(() => (cfg ? flat(cfg) : []), [cfg])
  const byKey = useMemo(() => Object.fromEntries(items.map(it => [it.key, it])), [items])

  function apply(c) {
    setCfg(c)
    setDraft(Object.fromEntries(flat(c).map(it => [it.key, toDraft(it)])))
    setView(v => (v.kind === 'group' && !v.name ? { kind: 'group', name: c.groups[0]?.name || '' } : v))
  }

  async function load(quiet) {
    if (!quiet) setErr('')
    try { apply(await api.planConfig()) } catch (e) { setErr('เปิดหน้านี้ไม่ได้: ' + e.message) }
  }
  useEffect(() => { load() }, [])
  useEffect(() => { api.masters().then(setFiles).catch(() => setFiles([])) }, [])

  // หน้านี้ยังกดปุ่มบันทึกเอง (ค่าตรงนี้เปลี่ยนทีกระทบแผนทั้งใบ) — แต่ถ้า admin อีกคนบันทึก
  // ค่าใหม่ไป เราต้องเห็นทันที ไม่งั้นจะนั่งแก้ทับของเขาโดยไม่รู้ตัว
  useEffect(() => live.subscribe(live.RES.planConfig, (ent) => {
    setErr('')
    setMsg(`${ent?.by || 'อีกคนหนึ่ง'} เพิ่งบันทึกค่าในหน้า Setting — กำลังโหลดค่าล่าสุดให้`)
    load(true)
  }), [])

  // เปิดหน้าประวัติค้างไว้ → ใครแก้อะไรบนหน้านี้ (ค่าตั้งค่า / ข้อมูลหลัก / วันทำงาน) ขึ้นให้เห็นเอง
  useEffect(() => {
    if (view.kind !== 'history' || !isSuper) return undefined
    return live.subscribeAll((ent, res) => {
      const r = String(res || '')
      if (r === live.RES.planConfig || r === live.RES.workday || r === live.RES.schedule
        || r.startsWith('masters/')) loadHist()
    })
  }, [view.kind, isSuper])

  // ค่าที่แก้ค้างอยู่ (ยังไม่กดบันทึก)
  const dirtyKeys = useMemo(
    () => new Set(items.filter(it => !same(draft[it.key], toDraft(it))).map(it => it.key)),
    [items, draft])

  const set = (key, v) => setDraft(d => ({ ...d, [key]: v }))
  const useDefault = it => set(it.key, defDraft(it))

  const step = (it, d) => set(it.key, stepped(it, draft[it.key], d))

  function askSave() {
    setErr(''); setMsg('')
    setConfirm([...dirtyKeys].map(k => byKey[k]).filter(Boolean).map(it => ({
      key: it.key, label: it.label, group: it.group,
      from: it.value_text,
      to: draftText(it, draft[it.key]),
      warn: it.warn || '',
      locked: it.source === 'env',
    })))
  }

  async function doSave() {
    setSaving(true); setErr(''); setMsg('')
    const values = {}
    for (const it of items) values[it.key] = toPayload(it, draft[it.key])
    try {
      const r = await api.planConfigSave(values)
      apply(r.config)
      setConfirm(null)
      setMsg(r.changed?.length
        ? `บันทึกแล้ว ${r.changed.length} รายการ — จะใช้กับการจัดแผนรอบถัดไป`
        : 'ไม่มีอะไรเปลี่ยน')
      if (hist !== null) loadHist()
    } catch (e) { setErr('บันทึกไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }

  async function resetAll() {
    if (!window.confirm('คืนค่ามาตรฐานทั้งหมด? ค่าที่ตั้งไว้เองจะถูกล้าง')) return
    setSaving(true); setErr(''); setMsg('')
    try {
      const r = await api.planConfigReset([])
      apply(r.config)
      setMsg('คืนค่ามาตรฐานทั้งหมดแล้ว')
      if (hist !== null) loadHist()
    } catch (e) { setErr('คืนค่าไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }

  // more = true → ต่อท้ายอีกหน้า ; ไม่งั้นโหลดใหม่ตั้งแต่ต้น เท่าจำนวนที่กางดูอยู่ (รายการไม่หดเอง)
  async function loadHist(more) {
    if (!isSuper) return
    const cur = more ? (histRef.current?.items || []) : []
    const n = more ? HIST_PAGE : Math.max(HIST_PAGE, histRef.current?.items?.length || 0)
    setHistBusy(true)
    try {
      const r = await api.planConfigHistory(cur.length, n)
      setHist({ items: [...cur, ...(r.items || [])], total: r.total || 0 })
    } catch (e) { setErr('ดูประวัติไม่ได้: ' + e.message) }
    finally { setHistBusy(false) }
  }

  const go = v => { setView(v); if (mainRef.current) mainRef.current.scrollTop = 0; window.scrollTo({ top: 0, behavior: 'smooth' }) }

  if (!cfg) return <div className="card"><div className="hint">{err || 'กำลังเปิดหน้า...'}</div></div>

  const query = q.trim().toLowerCase()
  const match = it => !query ||
    (it.label + ' ' + it.help + ' ' + it.example + ' ' + it.group).toLowerCase().includes(query)

  // ข้อที่จะแสดงตอนนี้ — ค้นหาและ "ที่ปรับไว้" มองข้ามกลุ่ม ไม่ต้องไล่เปิดทีละกลุ่ม
  const searching = !!query
  const showChanged = view.kind === 'changed'
  // หมวดข้อมูลหลัก = ตัวแก้ชีท Master เต็มหน้า (ซ่อนเมนูกลุ่มไว้ชั่วคราว แต่ไม่ถอด component ทิ้ง
  // เพราะถอดแล้วชีทที่เปิดค้างอยู่กับสิ่งที่กำลังพิมพ์จะหายทุกครั้งที่สลับหมวด)
  // ข้อมูลหลัก = รายการใน masterCatalog (เครื่องจักร / จ้างทอ / Target Stock / ปฏิทิน)
  const dataItems = catalogFor(files || [])
  const dataEntry = view.kind === 'data'
    ? dataItems.find(c => (c.view || c.file) === view.id) : null
  const dataView = !!dataEntry && !searching
  // ค่าที่ยกไปอยู่บนหน้าข้อมูลหลักแล้ว ไม่โชว์ซ้ำในกลุ่มเดิม (ค้นหา/ที่ปรับไว้ ยังเห็นตามปกติ)
  const visible = searching
    ? items.filter(match)
    : showChanged
      ? items.filter(it => !it.is_default || dirtyKeys.has(it.key))
      : items.filter(it => it.group === view.name && !MOVED_KEYS.has(it.key))
  const dataKeyItems = dataEntry
    ? (dataEntry.keys || []).map(k => byKey[k]).filter(Boolean) : []

  const changedTotal = items.filter(it => !it.is_default).length

  return (
    <div className="cfgpage">
      {/* ---------- แจ้งเตือน/ผลบันทึก — โชว์เฉพาะตอนมีข้อความ ---------- */}
      {cfg.env_locked?.length > 0 && (
        <div className="cfgwarn">
          ⚠️ มี {cfg.env_locked.length} ข้อที่ผู้ดูแลระบบตั้งทับไว้จากเซิร์ฟเวอร์ —
          ข้อเหล่านั้นแก้ที่หน้านี้จะ<b>ยังไม่มีผล</b> จนกว่าจะเอาค่าที่ตั้งทับออกก่อน
        </div>
      )}
      {msg && <div className="cfgmsg ok">{msg}</div>}
      {err && <div className="cfgmsg bad">{err}</div>}

      {/* ---------- เมนูกลุ่ม (ซ้าย) + เนื้อหา (ขวา) ---------- */}
      <div className={'cfgbody' + (dataView ? ' wide' : '')}>
        <aside className="cfgnav">
          {/* บน — อยู่กับที่ */}
          <div className="cfgnavtop">
            <input className="cfgsearch2" placeholder="🔍 ค้นหา เช่น Setup, สี, จ้างทอ"
              value={q} onChange={e => setQ(e.target.value)} />
            {searching && (
              <div className="cfgnavnote">
                ผลค้นหา {visible.length} ข้อ
                <button className="cfglink" onClick={() => setQ('')}>ล้าง</button>
              </div>
            )}

            <button className={'cfgnavitem wide' + (showChanged && !searching ? ' on' : '')}
              onClick={() => go({ kind: 'changed' })}>
              <span className="ic">⭐</span>
              <span className="nm">เฉพาะที่ปรับไว้</span>
              {changedTotal > 0 && <span className="ct on">{changedTotal}</span>}
            </button>
          </div>

          {/* กลาง — เลื่อนได้ */}
          <div className="cfgnavscroll">
          <div className="cfgnavsep">ข้อมูลหลัก</div>
          {dataItems.map(c => {
            const id = c.view || c.file
            return (
              <button key={id}
                className={'cfgnavitem' + (!searching && view.kind === 'data' && view.id === id ? ' on' : '')}
                title={c.what || ''}
                onClick={() => go({ kind: 'data', id })}>
                <span className="ic">{c.icon}</span>
                <span className="nm">{c.label}</span>
              </button>
            )
          })}

          <div className="cfgnavsep">กลุ่มค่าทั้งหมด</div>
          {cfg.groups.map(g => {
            const d = g.items.filter(it => dirtyKeys.has(it.key)).length
            return (
              <button key={g.name}
                className={'cfgnavitem' + (!searching && view.kind === 'group' && view.name === g.name ? ' on' : '')}
                onClick={() => go({ kind: 'group', name: g.name })}>
                <span className="ic">{g.icon}</span>
                <span className="nm">{g.name}</span>
                {d > 0 && <span className="ct dirty">{d}</span>}
                {d === 0 && g.changed_count > 0 && <span className="ct on">{g.changed_count}</span>}
              </button>
            )
          })}
          {/* แจ้งเมลฝ่ายขาย — ไม่มีค่าให้ตั้ง (เลิกโหมดทดสอบแล้ว) เหลือแค่รายชื่อผู้รับ */}
          <button className={'cfgnavitem' + (view.kind === 'mail' && !searching ? ' on' : '')}
            onClick={() => { setQ(''); go({ kind: 'mail' }) }}>
            <span className="ic">📧</span><span className="nm">แจ้งเมลฝ่ายขาย</span>
          </button>
          </div>

          {/* ล่าง — อยู่กับที่ */}
          <div className="cfgnavfoot">
            <button className={'cfgnavitem' + (view.kind === 'schedule' && !searching ? ' on' : '')}
              onClick={() => { setQ(''); go({ kind: 'schedule' }) }}>
              <span className="ic">⏰</span><span className="nm">ตั้งเวลารันอัตโนมัติ</span>
            </button>
            <button className={'cfgnavitem' + (view.kind === 'users' && !searching ? ' on' : '')}
              onClick={() => { setQ(''); go({ kind: 'users' }) }}>
              <span className="ic">👤</span><span className="nm">ผู้ใช้งาน</span>
            </button>
            {isSuper && (
              <button className={'cfgnavitem' + (view.kind === 'history' && !searching ? ' on' : '')}
                onClick={() => { go({ kind: 'history' }); loadHist() }}>
                <span className="ic">🕘</span><span className="nm">ประวัติการแก้ไข</span>
              </button>
            )}
            <button className="cfgnavitem danger" onClick={resetAll} disabled={saving}>
              <span className="ic">↩</span><span className="nm">คืนค่ามาตรฐานทั้งหมด</span>
            </button>
          </div>
        </aside>

        <main className="cfgmain" ref={mainRef}>
          {dataView
            ? (
              <div className="card cfgdatacard">
                <div className="cfggrouptop">
                  <span className="cfggico">{dataEntry.icon}</span>
                  <div>
                    <h3>{dataEntry.label}</h3>
                    {dataEntry.effect && <div className="cfggdesc">⚡ {dataEntry.effect}</div>}
                  </div>
                </div>
                {/* ค่าที่ยกมาไว้กับตารางของหน้านี้ — แก้แล้วบันทึกที่แถบล่างจอเหมือนค่าอื่น */}
                {dataKeyItems.length > 0 && (
                  <div className="cfgitems cfgdatakeys">
                    {dataKeyItems.map(it => (
                      <ConfigItem key={it.key} it={it} v={draft[it.key]} isDirty={dirtyKeys.has(it.key)}
                        why={!!openWhy[it.key]}
                        toggleWhy={() => setOpenWhy(w => ({ ...w, [it.key]: !w[it.key] }))}
                        set={set} step={step} useDefault={useDefault} />
                    ))}
                  </div>
                )}
                {dataEntry.view === 'mc-hub' && <McHub />}
                {dataEntry.view === 's9-hub' && <S9Hub />}
                {!dataEntry.view && <Masters mode="edit" only={{
                  file: dataEntry.file,
                  sheet: dataEntry.sheet
                    || (files || []).find(f => f.name === dataEntry.file)?.sheets?.[0] || '',
                }} />}
              </div>
            )
            : view.kind === 'schedule' && !searching
            ? <Schedule />
            : view.kind === 'users' && !searching
            ? <UserAdmin />
            : view.kind === 'mail' && !searching
            ? (
              <div className="card cfgdatacard">
                <div className="cfggrouptop">
                  <span className="cfggico">📧</span>
                  <div>
                    <h3>แจ้งเมลฝ่ายขาย</h3>
                    <div className="cfggdesc">รายชื่อผู้รับเมลแจ้งแผนเปลี่ยน — ส่งแยกฉบับให้แต่ละทีมขาย</div>
                  </div>
                </div>
                <MailRecipients />
              </div>
            )
            : view.kind === 'history' && !searching && isSuper
            ? <HistoryCard hist={hist} busy={histBusy} onMore={() => loadHist(true)} />
            : <GroupView
              cfg={cfg} visible={visible} searching={searching} showChanged={showChanged}
              view={view} draft={draft} dirtyKeys={dirtyKeys}
              openAdv={openAdv} setOpenAdv={setOpenAdv}
              openWhy={openWhy} setOpenWhy={setOpenWhy}
              set={set} step={step} useDefault={useDefault}
              files={files} onOpenSheet={setSheet}
              onGoData={e => go({ kind: 'data', id: e.view || e.file })} />}
        </main>
      </div>

      {/* ---------- แถบบันทึกลอยล่างจอ ---------- */}
      {dirtyKeys.size > 0 && (
        <div className="cfgsavebar">
          <span className="cfgsavetxt">✏️ แก้ค้างไว้ <b>{dirtyKeys.size}</b> ข้อ ยังไม่ได้บันทึก</span>
          <button onClick={() => load()} disabled={saving}>ยกเลิกที่แก้</button>
          <button className="primary" onClick={askSave} disabled={saving}>บันทึก {dirtyKeys.size} ข้อ</button>
        </div>
      )}

      {/* ---------- ตารางข้อมูลของหมวด (เปิดเต็มจอ) ---------- */}
      {sheet && (
        <div className="cfgmodalbg" onClick={() => setSheet(null)}>
          <div className="cfgsheetmodal" onClick={e => e.stopPropagation()}>
            <div className="cfgsheettop">
              <h3>{sheet.icon} {sheet.label}</h3>
              <span className="src">{sheet.file} / {sheet.sheet}</span>
              <button onClick={() => setSheet(null)}>ปิด</button>
            </div>
            {sheet.effect && <div className="cfgsheeteff">⚡ {sheet.effect}</div>}
            <Masters mode="edit" only={sheet} />
          </div>
        </div>
      )}

      {/* ---------- หน้าต่างยืนยันก่อนบันทึก ---------- */}
      {confirm && (
        <div className="cfgmodalbg" onClick={() => !saving && setConfirm(null)}>
          <div className="cfgmodal" onClick={e => e.stopPropagation()}>
            <h3>ยืนยันการเปลี่ยนแปลง {confirm.length} ข้อ</h3>
            <ul className="cfgdifflist">
              {confirm.map(c => (
                <li key={c.key}>
                  <div className="cfgdiffname"><span className="g">{c.group}</span>{c.label}</div>
                  <div className="cfgdiffval"><span className="from">{c.from}</span> → <b>{c.to}</b></div>
                  {c.warn && <div className="cfgdiffwarn">⚠️ {c.warn}</div>}
                  {c.locked && <div className="cfgdiffwarn">⚠️ ข้อนี้ถูกตั้งทับไว้จากเซิร์ฟเวอร์ — บันทึกได้ แต่ยังไม่มีผลจนกว่าจะเอาค่านั้นออก</div>}
                </li>
              ))}
            </ul>
            <p className="hint">จะใช้กับการจัดแผน<b>รอบถัดไป</b> — แผนที่รันไปแล้วไม่เปลี่ยน</p>
            <div className="actions">
              <button onClick={() => setConfirm(null)} disabled={saving}>ยกเลิก</button>
              <button className="primary" onClick={doSave} disabled={saving}>
                {saving ? 'กำลังบันทึก...' : 'ยืนยันบันทึก'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}


/* ======================= เนื้อหาฝั่งขวา ======================= */
function GroupView({ cfg, visible, searching, showChanged, view, draft, dirtyKeys,
  openAdv, setOpenAdv, openWhy, setOpenWhy, set, step, useDefault, files, onOpenSheet, onGoData }) {

  const g = cfg.groups.find(x => x.name === view.name)
  const header = searching
    ? { icon: '🔍', name: 'ผลการค้นหา', desc: `เจอ ${visible.length} ข้อจากทุกกลุ่ม` }
    : showChanged
      ? { icon: '⭐', name: 'เฉพาะที่ปรับไว้', desc: 'ข้อที่ต่างจากค่ามาตรฐาน หรือที่กำลังแก้ค้างไว้' }
      : { icon: g?.icon || '', name: g?.name || '', desc: g?.desc || '' }

  // ในมุมมองกลุ่มเดียว: ข้อขั้นสูงพับไว้ · มุมมองค้นหา/ที่ปรับไว้ โชว์ทั้งหมด
  const grouped = !searching && !showChanged
  const basics = grouped ? visible.filter(it => !it.advanced) : visible
  const advs = grouped ? visible.filter(it => it.advanced) : []
  const advOpen = !!openAdv[view.name]
  const groupSheets = grouped ? sheetsOfGroup(view.name, files) : []
  // ค่าของกลุ่มนี้ที่ย้ายไปอยู่หน้าข้อมูลหลักแล้ว — ขึ้นป้ายบอกทางไว้ที่เดิม
  const movedAway = grouped ? (g?.items || []).filter(it => MOVED_KEYS.has(it.key)) : []

  const row = it => (
    <ConfigItem key={it.key} it={it} v={draft[it.key]} isDirty={dirtyKeys.has(it.key)}
      showGroup={searching || showChanged}
      why={!!openWhy[it.key]} toggleWhy={() => setOpenWhy(w => ({ ...w, [it.key]: !w[it.key] }))}
      set={set} step={step} useDefault={useDefault} />
  )

  return (
    <div className="card cfggroupcard">
      <div className="cfggrouptop">
        <span className="cfggico">{header.icon}</span>
        <div>
          <h3>{header.name}</h3>
          {header.desc && <div className="cfggdesc">{header.desc}</div>}
        </div>
      </div>

      {!visible.length && !movedAway.length && <div className="hint">ไม่มีข้อที่ตรงกับเงื่อนไข</div>}

      <div className="cfgitems">{basics.map(row)}</div>

      {/* ค่าที่ย้ายไปอยู่กับตารางข้อมูลของมันแล้ว — กดแล้วพาไปหน้านั้น */}
      {movedAway.map(it => {
        const o = ownerOfKey(it.key)
        if (!o) return null
        return (
          <button key={it.key} className="cfgmovedrow" onClick={() => onGoData?.(o)}>
            <span className="ic">{o.icon}</span>
            <span className="tx">
              <b>{it.label}</b>
              {dirtyKeys.has(it.key) && <span className="cfgtag dirty">ยังไม่ได้บันทึก</span>}
              <span className="go">ย้ายไปอยู่หน้า “{o.label}” แล้ว เพราะต้องดูคู่กับตารางนั้น — กดเพื่อไปแก้ที่นั่น →</span>
            </span>
          </button>
        )
      })}

      {/* ตารางข้อมูลที่ผูกกับกติกากลุ่มนี้ — เดิมต้องไปหาเองในเมนูข้อมูลหลัก */}
      {grouped && groupSheets.length > 0 && (
        <div className="cfgsheets">
          <div className="cfgsheetshead">📋 ตารางข้อมูลของหมวดนี้</div>
          <div className="cfgsheetlist">
            {groupSheets.map(s => (
              <button key={`${s.file}/${s.sheet}`} className={'cfgsheetcard' + (s.unused ? ' off' : '')}
                onClick={() => onOpenSheet?.(s)}>
                <div className="top"><span className="ico">{s.icon}</span><b>{s.label}</b>
                  {s.unused && <span className="tag">ไม่ได้ใช้</span>}
                </div>
                {s.what && <div className="what">{s.what}</div>}
                {s.effect && <div className="eff">→ {s.effect}</div>}
              </button>
            ))}
          </div>
        </div>
      )}

      {advs.length > 0 && (
        <div className="cfgadv">
          <button className="cfgadvbtn" onClick={() => setOpenAdv(o => ({ ...o, [view.name]: !o[view.name] }))}>
            {advOpen ? '▲ ซ่อนข้อขั้นสูง' : `▼ ดูเพิ่มเติม — ข้อขั้นสูงอีก ${advs.length} ข้อ`}
          </button>
          {!advOpen && <div className="cfgadvnote">ปกติไม่ต้องแตะ เปลี่ยนเมื่อมีคนแนะนำเท่านั้น</div>}
          {advOpen && <div className="cfgitems">{advs.map(row)}</div>}
        </div>
      )}
    </div>
  )
}


/* ======================= ค่าหนึ่งข้อ ======================= */
// export: หน้า Monitor Yarn ใช้การ์ดตัวเดียวกัน (ค่าของหน้านั้นอยู่ในทะเบียนเดียวกับหน้านี้)
export function ConfigItem({ it, v, isDirty, showGroup, why, toggleWhy, set, step, useDefault }) {
  const atDefault = same(v, defDraft(it))
  // รายชื่อ/ตาราง: ป้ายชื่อคือค่าปัจจุบันอยู่แล้ว → วางกว้างเต็มกล่อง ไม่พิมพ์รายชื่อซ้ำเป็นข้อความ
  const wide = it.type === 'list' || it.type === 'table'
  const meaning = it.type === 'bool' ? (v ? it.on_means : it.off_means) : (wide ? null : it.example)
  const hasWhy = !!(it.more || it.less)

  return (
    <div className={'cfgitem' + (isDirty ? ' dirty' : '') + (wide ? ' wide' : '')}>
      <div className="cfgitemmain">
        <div className="cfgitemlabel">
          {showGroup && <span className="cfgitemgrp">{it.group}</span>}
          {it.label}
          {it.source === 'env' && <span className="cfgtag env">ตั้งจากเซิร์ฟเวอร์</span>}
          {!isDirty && it.source === 'file' && <span className="cfgtag set">ปรับไว้เอง</span>}
          {isDirty && <span className="cfgtag dirty">ยังไม่ได้บันทึก</span>}
          {wide && <span className="cfgcount">{(v || []).length} {it.type === 'table' ? 'แถว' : 'รายการ'}</span>}
        </div>
        {it.help && <div className="cfgitemhelp">{it.help}</div>}
        {meaning && <div className="cfgmeaning">{meaning}</div>}
        {it.warn && <div className="cfgitemwarn">⚠️ {it.warn}</div>}

        {hasWhy && (
          <div className="cfgwhy">
            <button className="cfglink" onClick={toggleWhy}>
              {why ? 'ซ่อน' : 'ปรับขึ้น-ลงแล้วเกิดอะไร?'}
            </button>
            {why && (
              <div className="cfgwhybox">
                {it.more && <div><span className="up">▲ เพิ่ม</span> {it.more}</div>}
                {it.less && <div><span className="dn">▼ ลด</span> {it.less}</div>}
              </div>
            )}
          </div>
        )}
      </div>

      {wide ? (
        <div className="cfgitemwide">
          {it.type === 'list' && <ListEditor it={it} v={v || []} set={set} />}
          {it.type === 'table' && <TableEditor it={it} v={v || []} set={set} />}
          <div className="cfgdefault">
            {atDefault
              ? <span className="cfgatdef">✓ ตรงค่ามาตรฐาน</span>
              : <>ค่ามาตรฐาน: {it.default_text}
                <button className="cfglink" onClick={() => useDefault(it)}>ใช้ค่ามาตรฐาน</button></>}
          </div>
        </div>
      ) : (
      <div className="cfgitemctl">
        <div className="cfgnowval">
          ตอนนี้: <b>{isDirty ? draftText(it, v) : it.value_text}</b>
        </div>

        {it.type === 'bool' && (
          <div className="cfgseg">
            <button className={v ? 'on' : ''} onClick={() => set(it.key, true)}>เปิด</button>
            <button className={!v ? 'off' : ''} onClick={() => set(it.key, false)}>ปิด</button>
          </div>
        )}

        {isSimpleNum(it) && (
          <div className="cfgstepper">
            <button onClick={() => step(it, -1)} title="ลด">−</button>
            <input inputMode="decimal" value={v}
              placeholder={it.blank ? 'ไม่จำกัด' : ''}
              onChange={e => set(it.key, e.target.value.replace(/[^0-9.]/g, ''))} />
            <button onClick={() => step(it, +1)} title="เพิ่ม">+</button>
            {it.unit && <small>{it.unit}</small>}
            {it.blank && <button className="cfglink" onClick={() => set(it.key, '')}>ไม่จำกัด</button>}
          </div>
        )}

        <div className="cfgdefault">
          ค่ามาตรฐาน: {it.default_text}
          {!atDefault && <button className="cfglink" onClick={() => useDefault(it)}>ใช้ค่ามาตรฐาน</button>}
        </div>
      </div>
      )}
    </div>
  )
}


/* ---------- รายชื่อ: ป้ายชื่อกดลบได้ + ช่องเพิ่ม ---------- */
function ListEditor({ it, v, set }) {
  const [add, setAdd] = useState('')
  const numeric = it.item_type === 'int'

  const push = () => {
    const s = add.trim()
    if (!s) return
    if (!v.some(x => String(x).trim() === s)) set(it.key, [...v, s])
    setAdd('')
  }

  const ph = (numeric ? 'พิมพ์ตัวเลขแล้วกด Enter' : 'พิมพ์แล้วกด Enter')
    + (it.example ? ` · เช่น ${it.example}` : '')

  // ป้ายชื่อ + ช่องพิมพ์อยู่ในกรอบเดียว (แบบช่องใส่ tag) — คลิกที่ว่างในกรอบแล้วไปที่ช่องพิมพ์
  return (
    <div className="cfglist" onClick={e => e.currentTarget.querySelector('input')?.focus()}>
      {v.map((x, i) => (
        <span key={i} className="cfgchip">
          {x}
          <button title="ลบออก" onClick={e => { e.stopPropagation(); set(it.key, v.filter((_, j) => j !== i)) }}>×</button>
        </span>
      ))}
      <input value={add} inputMode={numeric ? 'numeric' : 'text'}
        placeholder={v.length === 0 ? 'ยังไม่มีรายการ — ' + ph : ph}
        onChange={e => setAdd(numeric ? e.target.value.replace(/[^0-9]/g, '') : e.target.value)}
        onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); push() } }} />
      <button className="cfgchipaddbtn" onClick={push} disabled={!add.trim()}>+ เพิ่ม</button>
    </div>
  )
}


/* ---------- ตาราง: แถวเพิ่ม/ลบได้ ---------- */
function TableEditor({ it, v, set }) {
  const cols = it.columns || []
  const blank = Object.fromEntries(cols.map(c => [c.key, '']))

  const edit = (i, ck, val) =>
    set(it.key, v.map((r, j) => (j === i ? { ...r, [ck]: val } : r)))

  return (
    <div className="cfgtable">
      <table>
        <thead>
          <tr>{cols.map(c => <th key={c.key}>{c.label}</th>)}<th /></tr>
        </thead>
        <tbody>
          {v.length === 0 && (
            <tr><td colSpan={cols.length + 1} className="cfgtblempty">ยังไม่มีรายการ</td></tr>
          )}
          {v.map((r, i) => (
            <tr key={i}>
              {cols.map(c => (
                <td key={c.key}>
                  {c.choices
                    // ช่องที่ต้องเลือกจากตัวเลือก (เช่น แบบ = ขึ้นต้นด้วย / ตรงตัว) — พิมพ์เองไม่ได้ กันค่าที่ระบบไม่รู้จัก
                    ? (
                      <select value={r[c.key] ?? ''} onChange={e => edit(i, c.key, e.target.value)}>
                        <option value="">— เลือก —</option>
                        {c.choices.map(ch => <option key={ch.value} value={ch.value}>{ch.label}</option>)}
                      </select>
                    )
                    : (
                      <input value={r[c.key] ?? ''}
                        placeholder={c.optional ? 'เว้นว่างได้' : ''}
                        inputMode={c.type === 'int' ? 'numeric' : 'text'}
                        onChange={e => edit(i, c.key,
                          c.type === 'int' ? e.target.value.replace(/[^0-9]/g, '') : e.target.value)} />
                    )}
                </td>
              ))}
              <td>
                <button className="cfgrowdel" title="ลบแถวนี้"
                  onClick={() => set(it.key, v.filter((_, j) => j !== i))}>×</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <button className="cfgaddrow" onClick={() => set(it.key, [...v, blank])}>+ เพิ่มแถว</button>
    </div>
  )
}


/* ======================= ประวัติการแก้ไข ======================= */
// รวมทุกอย่างที่แก้บนหน้านี้: ค่าตั้งค่า (kind=config) + ข้อมูลหลัก/วันทำงาน (kind=data)
// backend รวมและเรียงเวลาให้แล้ว (GET /api/plan-config/history) — เฉพาะ superadmin
// เก็บทุกครั้งตลอดไป ไม่ตัด → โหลดทีละหน้า แล้วกด "โหลดเพิ่ม" ย้อนไปจนถึงครั้งแรก
const HIST_PAGE = 100
const HIST_SHOW = 5        // บรรทัดที่โชว์ต่อ 1 การแก้ ก่อนต้องกด "ดูอีก"

function histArea(h) {
  if (h.kind !== 'data') return { icon: '⚙️', label: 'ค่าตั้งค่า' }
  return historyArea(h.resource)
}

function histLines(h) {
  if (h.kind !== 'data') {
    return (h.changed || []).map(c =>
      `${c.label || c.key}: ${c.from_text ?? String(c.from)} → ${c.to_text ?? String(c.to)}`)
  }
  // บันทึกก่อนมีการจดรายละเอียด มีแค่ "ชีทนี้ X แถว"
  if (!Array.isArray(h.changes)) return [`บันทึก${h.detail ? ' — ' + h.detail : ''} (ไม่มีรายละเอียด)`]
  const more = (h.changes_total || 0) - h.changes.length
  return more > 0 ? [...h.changes, `…และอีก ${more.toLocaleString()} รายการที่ไม่ได้จดรายละเอียด`] : h.changes
}

function HistoryCard({ hist, busy, onMore }) {
  const [open, setOpen] = useState({})   // {index: true} = กางดูทุกบรรทัด
  const items = hist?.items || []
  const left = (hist?.total || 0) - items.length
  return (
    <div className="card cfggroupcard">
      <div className="cfggrouptop">
        <span className="cfggico">🕘</span>
        <div>
          <h3>ประวัติการแก้ไข</h3>
          <div className="cfggdesc">
            ทุกอย่างที่แก้ในหน้านี้ ทั้งค่าตั้งค่าและข้อมูลหลัก: แก้อะไร เมื่อไหร่ ใครแก้
            — เก็บทุกครั้งตั้งแต่เริ่มใช้{hist ? ` · ทั้งหมด ${hist.total.toLocaleString()} ครั้ง` : ''}
            {' '}· ดูได้เฉพาะ Super Admin
          </div>
        </div>
      </div>
      {hist === null ? <div className="hint">กำลังโหลด...</div>
        : items.length === 0 ? <div className="hint">ยังไม่เคยมีการแก้ไข</div>
          : <><table className="cfghist">
            <thead><tr><th>เมื่อ</th><th>โดย</th><th>หมวด</th><th>เปลี่ยนอะไร</th></tr></thead>
            <tbody>
              {items.map((h, i) => {
                const area = histArea(h)
                const lines = histLines(h)
                const shown = open[i] ? lines : lines.slice(0, HIST_SHOW)
                return (
                  <tr key={i}>
                    <td className="nowrap">{fmtTime(h.at)}</td>
                    <td>{h.by || <span className="hint">ไม่ทราบ</span>}</td>
                    <td className="nowrap">{area.icon} {area.label}</td>
                    <td>
                      {shown.map((t, j) => <div key={j}>{t}</div>)}
                      {lines.length > HIST_SHOW && (
                        <button className="cfglink" onClick={() => setOpen(o => ({ ...o, [i]: !o[i] }))}>
                          {open[i] ? 'ย่อ' : `ดูอีก ${lines.length - HIST_SHOW} รายการ`}
                        </button>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <div className="hint" style={{ marginTop: 10 }}>
            แสดง {items.length.toLocaleString()} จาก {hist.total.toLocaleString()} ครั้ง
            {left > 0 && (
              <>{' '}<button className="cfglink" disabled={busy} onClick={onMore}>
                {busy ? 'กำลังโหลด…' : `โหลดเพิ่มอีก ${Math.min(HIST_PAGE, left).toLocaleString()} ครั้ง (เก่ากว่า)`}
              </button></>
            )}
          </div></>}
    </div>
  )
}

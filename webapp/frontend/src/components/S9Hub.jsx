import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { PickOrNew } from './McNewWizard.jsx'

// หน้า "จ้างทอ" — ที่เดียวจบ: เครื่องของผู้รับจ้าง · ผ้าที่ต้องจ้างเสมอ · ผ้าที่จ้างทอได้
//
// เดิมแยก 3 ชีท (MC S9 / S9 Only / Item S9) ที่ต้องรู้ชื่อเองว่าอะไรอยู่ไหน
// จัดตาม "ผู้รับจ้าง" เพราะเป็นหน่วยที่คนใช้งานคิดจริง
//   • เครื่องของผู้รับจ้าง  → แก้ได้ (จำนวน / สถานะ / วันทำงาน) เพิ่ม-ลบได้
//   • ผ้าที่ต้องจ้างเสมอ    → กรอกมือ เพิ่ม-ลบได้ + นำเข้าทับทั้งชีท (MC เว้นว่างได้, รหัสผ้าห้ามซ้ำ)
//   • ผ้าที่จ้างทอได้       → หลายพันแถว ปกตินำเข้าทับทั้งชีท (ไฟล์ / วางจาก Excel)
//                             + เพิ่ม-ลบ-แก้ Cap/Day ทีละแถว (แถวที่เติมเองหายตอนนำเข้ารอบถัดไป)

const fmt = v => (v === null || v === undefined || v === '' ? '' : String(v))

// สรุปเครื่องของผู้รับจ้าง 1 เจ้าบนหัวแถว: "RAP 22/28, SKP 22/26/28"
//   เดิมโชว์แค่ชื่อเครื่อง → "RAP, RAP, SKP, SKP" ดูเหมือนกรอกซ้ำ ทั้งที่เป็นคนละเกจ
//   แถวเรียงตาม (เครื่อง, เกจ) มาจาก backend แล้ว
const mcSummary = machines => {
  const by = new Map()
  for (const m of machines) {
    if (!m.mc) continue
    if (!by.has(m.mc)) by.set(m.mc, [])
    if (m.guage && !by.get(m.mc).includes(m.guage)) by.get(m.mc).push(m.guage)
  }
  return [...by].map(([mc, gs]) => gs.length ? `${mc} ${gs.join('/')}` : mc).join(', ')
}

// เกจของเครื่องผู้รับจ้าง = เลขล้วน (ว่าง = ยังไม่กรอก ให้ด่าน "ยังไม่ได้กรอก" จับแทน)
const gaugeOk = g => !String(g ?? '').trim() || /^\d+$/.test(String(g).trim())

// ผ้าที่จ้างทอได้: แก้ได้ทุกคอลัมน์ — คีย์ในตัวแก้ค้าง ('icode:12') → ชื่อบนจอ / ชื่อฟิลด์ที่ส่งไป backend
const ITEM_EDIT = { icode: 'Item', igrp: 'MC', icat: 'CAT', igau: 'Guage', cap: 'Cap/Day' }
const ITEM_FIELD = { icode: 'item', igrp: 'mc_group', icat: 'mc_cat', igau: 'guage', cap: 'cap' }
// ผ้าที่ต้องจ้างเสมอ: คอลัมน์ชุดเดียวกัน (user 24 ก.ย. 2569) — Item/CAT/Guage/Cap บังคับ, MC เว้นว่างได้
//   S9 Only จ้างทอเท่านั้น ระบบใช้ Cap/Guage/CAT ของชีทนี้อย่างเดียว ขาดตัวไหน = ไม่ทำแผน (ลง UNPLANNED)
//   แถวเดิมที่ยังกรอกไม่ครบค้างอยู่ได้ แต่ไม่ถูกวาง
const ONLY_EDIT = { ocode: 'Item', ogrp: 'MC', ocat: 'CAT', ogau: 'Guage', ocap: 'Cap/Day' }
const ONLY_FIELD = { ocode: 'item', ogrp: 'mc_group', ocat: 'mc_cat', ogau: 'guage', ocap: 'cap' }
const ONLY_NEED = ['ocode', 'ocat', 'ogau']
// เครื่องของผู้รับจ้าง: คีย์ในตัวแก้ค้าง ('total:12') — Import ทับชีทแล้วต้องล้างค่าที่แก้ค้างของชุดนี้ทิ้ง
const MC_EDIT = { total: 1, status: 1, wd: 1, mat: 1, rmk: 1 }
// ช่องในตาราง → class ความกว้าง (ใช้ชุดเดียวกับตารางผ้าที่จ้างทอได้)
const CELL_CLS = { icode: 'icode', igrp: 'igrp', icat: 'icat', igau: 'igau', cap: 'cap',
  ocode: 'icode', ogrp: 'igrp', ocat: 'icat', ogau: 'igau', ocap: 'cap' }

export default function S9Hub({ ro }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [open, setOpen] = useState({})
  const [edit, setEdit] = useState({})        // 'total:12' | 'status:12' | 'wd:12'
  const [adds, setAdds] = useState([])        // เครื่องใหม่ที่ยังไม่บันทึก
  const [dels, setDels] = useState([])        // แถวเครื่องที่สั่งลบ
  const blankItem = { item: '', mc_group: '', mc_cat: '', guage: '', cap: '' }
  const [onlyAdd, setOnlyAdd] = useState(blankItem)
  const [onlyDel, setOnlyDel] = useState([])
  const [itemAdd, setItemAdd] = useState(blankItem)
  const [itemDel, setItemDel] = useState([])
  const [imp, setImp] = useState(null)        // null = ปิด | { kind: 'only'|'items', text, check } — นำเข้าทับทั้งชีท
  const fileRef = useRef(null)
  const [q, setQ] = useState('')
  const [tab, setTab] = useState('mc')        // mc | only | items
  const [saving, setSaving] = useState(false)

  const load = () => api.s9Hub().then(d => { setData(d); setErr('') })
    .catch(e => setErr('โหลดข้อมูลจ้างทอไม่ได้: ' + e.message))
  useEffect(() => { load() }, [])

  const set = (k, v) => setEdit(e => ({ ...e, [k]: v }))
  const val = (k, cur) => (k in edit ? edit[k] : fmt(cur))

  // ด่านกรอกให้ครบ (กติกาเดียวกับหน้าเครื่องจักร)
  const bad = useMemo(() => {
    const out = []
    for (const [k, v] of Object.entries(edit)) {
      const kind = k.slice(0, k.indexOf(':'))
      const s = String(v ?? '').trim()
      if (kind === 'total' && s === '') out.push('จำนวนเครื่อง: เว้นว่างไม่ได้ (ใส่ 0 ได้)')
      if (kind === 'wd') {
        if (s === '') out.push('วันทำงาน: เว้นว่างไม่ได้')
        else if (!(Number(s) >= 0 && Number(s) <= 7)) out.push('วันทำงาน: ต้องอยู่ระหว่าง 0-7')
      }
      if (kind === 'cap' && !(Number(s) > 0)) out.push('Cap/Day: ต้องมากกว่า 0')
      if (kind in ITEM_EDIT && kind !== 'cap' && s === '') out.push(ITEM_EDIT[kind] + ': เว้นว่างไม่ได้')
      if (kind === 'ocap' && !(Number(s) > 0)) out.push('Cap/Day: ต้องมากกว่า 0')
      if (ONLY_NEED.includes(kind) && s === '') out.push(ONLY_EDIT[kind] + ': เว้นว่างไม่ได้')
    }
    for (const a of adds) {
      const miss = [['supp', 'ผู้รับจ้าง'], ['mc_cat', 'กลุ่มเครื่อง (MC_CAT)'], ['mc', 'ชื่อเครื่อง (MC)'],
      ['guage', 'Guage'], ['total', 'จำนวนเครื่อง'], ['work_days', 'วันทำงาน']]
        .filter(([f]) => !String(a[f] ?? '').trim()).map(([, l]) => l)
      if (miss.length) out.push('เครื่องใหม่: ยังไม่ได้กรอก ' + miss.join(', '))
      if (!gaugeOk(a.guage)) out.push(`เครื่องใหม่: Guage "${a.guage}" ใส่เลข Guage อย่างเดียว เช่น 28 (กระบอกดูจากกลุ่มเครื่อง)`)
    }
    return [...new Set(out)]
  }, [edit, adds])

  const dirty = Object.keys(edit).length + adds.length + dels.length + onlyDel.length + itemDel.length
  const isBad = k => {
    if (!(k in edit)) return false
    const s = String(edit[k] ?? '').trim()
    if (k.startsWith('total:')) return s === ''
    if (k.startsWith('wd:')) return s === '' || !(Number(s) >= 0 && Number(s) <= 7)
    if (k.startsWith('cap:')) return !(Number(s) > 0)
    if (k.startsWith('ocap:')) return !(Number(s) > 0)
    const kind = k.slice(0, k.indexOf(':'))
    if (kind in ITEM_EDIT || ONLY_NEED.includes(kind)) return s === ''
    return false
  }
  const clearAll = () => { setEdit({}); setAdds([]); setDels([]); setOnlyDel([]); setItemDel([]) }

  async function save() {
    setSaving(true); setErr(''); setMsg('')
    const byRow = {}, byItem = {}, byOnly = {}
    for (const [k, v] of Object.entries(edit)) {
      const kind = k.slice(0, k.indexOf(':')), row = Number(k.slice(k.indexOf(':') + 1))
      if (kind in ITEM_EDIT) {                    // แถวของชีท Item S9 ไม่ใช่ MC S9 — รวมทุกช่องของแถวเดียวกัน
        const it = byItem[row] = byItem[row] || { row }
        it[ITEM_FIELD[kind]] = v
        continue
      }
      if (kind in ONLY_EDIT) {                    // แถวของชีท S9 Only
        const it = byOnly[row] = byOnly[row] || { row }
        it[ONLY_FIELD[kind]] = v
        continue
      }
      const slot = byRow[row] = byRow[row] || { row }
      if (kind === 'total') slot.total = v
      else if (kind === 'status') slot.status = v
      else if (kind === 'wd') slot.work_days = v
      else if (kind === 'mat') slot.material = v
      else if (kind === 'rmk') slot.remark = v
    }
    try {
      const r = await api.s9HubSave({
        machines: Object.values(byRow),
        add_machines: adds,
        del_machines: dels,
        only: Object.values(byOnly),
        del_only: onlyDel,
        items: Object.values(byItem),
        del_items: itemDel,
      }, data.revs)
      clearAll()
      setMsg('บันทึกแล้ว: ' + (r.done?.join(' · ') || 'ไม่มีอะไรเปลี่ยน'))
      await load()
    } catch (e) {
      setErr(e.conflict ? 'มีคนแก้แซงไป — กดโหลดใหม่แล้วแก้อีกครั้ง' : 'บันทึกไม่สำเร็จ: ' + e.message)
    } finally { setSaving(false) }
  }

  const onlyMiss = [['item', 'Item'], ['mc_cat', 'CAT'], ['guage', 'Guage']]
    .filter(([f]) => !onlyAdd[f].trim()).map(([, l]) => l)
    .concat(Number(onlyAdd.cap) > 0 ? [] : ['Cap/Day'])
  async function addOnly() {
    if (onlyMiss.length) return
    setSaving(true); setErr('')
    try {
      const r = await api.s9HubSave({ add_only: [onlyAdd] }, data.revs)
      setOnlyAdd(blankItem)
      setMsg('บันทึกแล้ว: ' + (r.done?.join(' · ') || ''))
      await load()
    } catch (e) {
      setErr(e.conflict ? 'มีคนแก้แซงไป — กดโหลดใหม่แล้วแก้อีกครั้ง' : 'เพิ่มไม่สำเร็จ: ' + e.message)
    } finally { setSaving(false) }
  }

  // ช่องกรอกในตาราง (ทั้ง 2 ทะเบียนผ้า) — Guage รับตัวอักษรด้วย (มีค่าอย่าง C0 อยู่ในชีท)
  const cellInput = (kind, row, cur, extra = {}) => {
    const k = `${kind}:${row}`
    const isCap = kind === 'cap' || kind === 'ocap'
    return (
      <input className={'s9cap s9c-' + CELL_CLS[kind] + ' ' + (isBad(k) ? 'bad ' : '') + (k in edit ? 'chg' : '')}
        value={val(k, cur)} placeholder={isCap ? 'ไม่มี' : (kind in ONLY_EDIT && !ONLY_NEED.includes(kind) ? '' : 'ต้องกรอก')}
        inputMode={isCap ? 'decimal' : undefined}
        onChange={e => set(k, isCap ? e.target.value.replace(/[^0-9.]/g, '') : e.target.value.toUpperCase())}
        {...extra} />
    )
  }

  // ---- ผ้าที่จ้างทอได้: เพิ่มทีละแถว (บันทึกทันที แบบเดียวกับผ้าที่ต้องจ้างเสมอ)
  const itemMiss = [['item', 'Item'], ['mc_group', 'MC'], ['mc_cat', 'CAT'], ['guage', 'Guage']]
    .filter(([f]) => !itemAdd[f].trim()).map(([, l]) => l)
    .concat(Number(itemAdd.cap) > 0 ? [] : ['Cap/Day'])
  async function addItem() {
    if (itemMiss.length) return
    setSaving(true); setErr('')
    try {
      const r = await api.s9HubSave({ add_items: [itemAdd] }, data.revs)
      setItemAdd(blankItem)
      setMsg('บันทึกแล้ว: ' + (r.done?.join(' · ') || ''))
      await load()
    } catch (e) {
      setErr(e.conflict ? 'มีคนแก้แซงไป — กดโหลดใหม่แล้วแก้อีกครั้ง' : 'เพิ่มไม่สำเร็จ: ' + e.message)
    } finally { setSaving(false) }
  }

  // ---- นำเข้าทับทั้งชีท (ผ้าที่จ้างทอได้ / ผ้าที่ต้องจ้างเสมอ) — ตรวจก่อน (ไฟล์ .xlsx หรือวางจาก Excel) แล้วค่อยยืนยัน
  const IMP = {
    items: { title: 'ผ้าที่จ้างทอได้', preview: api.s9ItemsPreview, check: api.s9ItemsCheck,
      key: 'replace_items', edits: ITEM_EDIT, clearDel: () => setItemDel([]) },
    only: { title: 'ผ้าที่ต้องจ้างทอเสมอ', preview: api.s9OnlyPreview, check: api.s9OnlyCheck,
      key: 'replace_only', edits: ONLY_EDIT, clearDel: () => setOnlyDel([]) },
    mc: { title: 'เครื่องของผู้รับจ้าง', preview: api.s9McPreview, check: api.s9McCheck,
      key: 'replace_mc', edits: MC_EDIT, clearDel: () => { setDels([]); setAdds([]) } },
  }
  // จำนวนรายการเดิมของแต่ละชีท (ขึ้นในหน้ายืนยัน "เดิม → ใหม่")
  const impCur = kind => kind === 'mc'
    ? (data?.suppliers || []).reduce((a, s) => a + s.machines.length, 0)
    : ((kind === 'only' ? data?.only : data?.items) || []).length
  async function checkFile(file) {
    if (!file || !imp) return
    const cfg = IMP[imp.kind]
    setSaving(true); setErr('')
    setImp(s => ({ ...s, check: null }))
    try { const c = await cfg.preview(file); setImp(s => ({ ...s, check: { ...c, src: file.name } })) }
    catch (e) { setErr('อ่านไฟล์ไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }
  async function checkPaste() {
    // Excel คัดลอกออกมาเป็นแถวคั่นบรรทัด คอลัมน์คั่น tab — แถวแรกต้องเป็นหัวคอลัมน์
    const lines = (imp?.text || '').split(/\r?\n/).filter(l => l.trim())
    if (lines.length < 2) { setErr('วางข้อมูลพร้อมหัวคอลัมน์ในแถวแรก อย่างน้อย 2 แถว'); return }
    const [head, ...rest] = lines.map(l => l.split('\t'))
    setSaving(true); setErr('')
    try { const c = await IMP[imp.kind].check(head, rest); setImp(s => ({ ...s, check: { ...c, src: 'วางจาก Excel' } })) }
    catch (e) { setErr('ข้อมูลไม่ถูกต้อง: ' + e.message) }
    finally { setSaving(false) }
  }
  async function doImport() {
    const c = imp?.check
    if (!c) return
    const cfg = IMP[imp.kind]
    if (!window.confirm(`ทับชีท${cfg.title}ทั้งหมด\nเดิม ${impCur(imp.kind)} รายการ → ใหม่ ${c.count} รายการ\n\n`
      + 'แถวที่เพิ่ม/แก้บนเว็บไว้ก่อนหน้าจะหายทั้งหมด ยืนยัน?')) return
    setSaving(true); setErr('')
    try {
      const r = await api.s9HubSave({ [cfg.key]: { columns: c.columns, rows: c.rows } }, data.revs)
      setImp(null)
      setEdit(e => Object.fromEntries(Object.entries(e).filter(([k]) => !(k.slice(0, k.indexOf(':')) in cfg.edits))))
      cfg.clearDel()
      setMsg('บันทึกแล้ว: ' + (r.done?.join(' · ') || ''))
      await load()
    } catch (e) {
      setErr(e.conflict ? 'มีคนแก้แซงไป — กดโหลดใหม่แล้วนำเข้าอีกครั้ง' : 'นำเข้าไม่สำเร็จ: ' + e.message)
    } finally { setSaving(false) }
  }

  // Export ชีทของแท็บที่เปิดอยู่ — ไฟล์เดียวกับที่ Import กลับได้
  const [exporting, setExporting] = useState(false)
  async function doExport() {
    const sheet = { mc: 'MC S9', only: 'S9 Only', items: 'Item S9' }[tab]
    setExporting(true); setErr('')
    try { await api.masterExport('MasterMC', sheet) }
    catch (e) { setErr('Export ไม่สำเร็จ: ' + e.message) }
    finally { setExporting(false) }
  }

  if (!data) return <div className="hint">{err || 'กำลังโหลดข้อมูลจ้างทอ...'}</div>

  const sup = data.suppliers || []
  const items = data.items || []
  const only = data.only || []
  const query = q.trim().toLowerCase()
  const shownItems = query
    ? items.filter(x => (x.item + ' ' + x.mc_group + ' ' + x.mc_cat).toLowerCase().includes(query))
    : items.slice(0, 200)
  const shownOnly = query
    ? only.filter(x => (x.item + ' ' + x.mc_group + ' ' + x.mc_cat).toLowerCase().includes(query))
    : only

  // ปุ่มเปิด/ปิด + แผง Import ทับทั้งชีท — ใช้ร่วมกันทั้งแท็บ S9 Only และ Master Item/S9
  const importToggle = kind => !ro && (
    <button className="ghost sm" disabled={saving}
      onClick={() => setImp(s => (s?.kind === kind ? null : { kind, text: '', check: null }))}>
      {imp?.kind === kind ? '✕ ปิด Import' : '📥 Import'}
    </button>
  )
  const importPanel = kind => !ro && imp?.kind === kind && (
    <div className="s9import">
      <div className="s9improw">
        <button className="ghost sm" disabled={saving} onClick={() => fileRef.current?.click()}>
          📄 เลือกไฟล์ .xlsx
        </button>
        <input ref={fileRef} type="file" accept=".xlsx,.xlsm" hidden
          onChange={e => { checkFile(e.target.files?.[0]); e.target.value = '' }} />
        {kind === 'mc'
          ? <span className="hint small">หรือคัดลอกจาก Excel (รวมหัวคอลัมน์) มาวางด้านล่าง · ใช้ไฟล์จากปุ่ม Export ได้เลย ·
            หัวคอลัมน์ที่ต้องมี (สลับลำดับได้): <b> Supp · Mc · New CAT · New Gauge · Total Mc</b>
            (+ Working day · Status · Material · Remark) · Working day ถ้ากรอกต้องอยู่ระหว่าง 0-7 ·
            New Gauge เก็บเลข Guage อย่างเดียว (ไฟล์ที่เขียน 28G34" ระบบตัดเหลือ 28 ให้ — กระบอกต้องตรงกับ New CAT)</span>
          : kind === 'only'
          ? <span className="hint small">หรือคัดลอกจาก Excel (รวมหัวคอลัมน์) มาวางด้านล่าง ·
            หัวคอลัมน์ (ชื่อต้องตรง สลับลำดับได้): <b> Item · MC · CAT · Guage · กำลังผลิต/วัน</b> ·
            MC เว้นว่างได้ ที่เหลือทุกแถวต้องกรอกครบ · รหัสผ้าห้ามซ้ำ · ไม่ครบระบบไม่บันทึก</span>
          : <span className="hint small">หรือคัดลอกจาก Excel (รวมหัวคอลัมน์) มาวางด้านล่าง ·
            หัวคอลัมน์ต้องมีครบ 5 ตัว (ชื่อต้องตรง สลับลำดับได้):
            <b> Item · MC · CAT · Guage · กำลังผลิต/วัน</b> ·
            ทุกแถวต้องกรอกครบ ไม่ครบระบบไม่บันทึก</span>}
      </div>
      <textarea rows={4} value={imp.text}
        placeholder={kind === 'mc'
          ? 'Supp\tMc\tNew CAT\tNew Gauge\tTotal Mc\tWorking day\nPNT\tRAP\tDOUBLE-30\t22\t1\t6'
          : kind === 'only'
          ? 'Item\tMC\tCAT\tGuage\tกำลังผลิต/วัน\nFD4DRTLH140/02A0\t\tDOUBLE-34\t28\t150'
          : 'Item\tMC\tCAT\tGuage\tกำลังผลิต/วัน\nFD1BASIJ22/1A0\tTFK\tDOUBLE-34\t19\t261'}
        onChange={e => setImp(s => ({ ...s, text: e.target.value, check: null }))} />
      <div className="s9improw">
        <button className="ghost sm" onClick={checkPaste} disabled={saving || !imp.text.trim()}>ตรวจข้อมูลที่วาง</button>
        {imp.check && (
          <>
            <span className="s9impsum">
              {imp.check.src}{imp.check.sheet ? ` (ชีท ${imp.check.sheet})` : ''} ·
              เดิม <b>{impCur(kind)}</b> → ใหม่ <b>{imp.check.count}</b> รายการ
            </span>
            <button className="primary" onClick={doImport} disabled={saving || dirty > 0}
              title={dirty > 0 ? 'บันทึกหรือยกเลิกการแก้ที่ค้างอยู่ก่อน' : ''}>
              ยืนยันทับทั้งชีท
            </button>
          </>
        )}
      </div>
      {imp.check?.warn?.map(w => <div key={w} className="s9impwarn">⚠️ {w}</div>)}
      {imp.check && dirty > 0 && <div className="s9impwarn">⚠️ มีการแก้ค้างอยู่ — บันทึกหรือยกเลิกก่อนนำเข้า</div>}
    </div>
  )

  return (
    <div className="mchub s9hub">
      <div className="mchubbar">
        <span className="mchubtotal">
          🤝 ผู้รับจ้าง <b>{sup.length}</b> เจ้า · เครื่องที่ใช้ได้ <b className="n">{data.total_mc}</b> เครื่อง
        </span>
        <div className="s9tabs">
          <button className={tab === 'mc' ? 'on' : ''} onClick={() => setTab('mc')}>Master MC/S9</button>
          <button className={tab === 'only' ? 'on' : ''} onClick={() => setTab('only')}>
            S9 Only <small>{only.length}</small>
          </button>
          <button className={tab === 'items' ? 'on' : ''} onClick={() => setTab('items')}>
            Master Item/S9 <small>{items.length}</small>
          </button>
        </div>
        <button className="ghost sm" onClick={load}>⟳ โหลดใหม่</button>
        <button className="ghost sm" onClick={doExport} disabled={exporting}
          title="ดาวน์โหลดข้อมูลของแท็บนี้เป็นไฟล์ .xlsx (Import กลับได้)">
          {exporting ? 'กำลัง Export...' : '📤 Export'}
        </button>
      </div>

      {err && <div className="msg err">{err}</div>}
      {msg && <div className="msg ok">{msg}</div>}

      {/* ช่องเลือก MC / CAT — ใช้ร่วมกันทั้งแท็บ S9 Only และ Master Item/S9 */}
      <datalist id="s9i-group">{(data.options.item_groups || []).map(o => <option key={o} value={o} />)}</datalist>
      <datalist id="s9i-cat">{(data.options.item_cats || []).map(o => <option key={o} value={o} />)}</datalist>

      {/* ---------------- เครื่องของผู้รับจ้าง ---------------- */}
      {tab === 'mc' && (
        <>
          {!ro && (
            <div className="s9mcbar">
              <button className="mchubadd s9add"
                onClick={() => setAdds(a => [...a, { supp: '', mc: '', mc_cat: '', guage: '', total: '', work_days: '6', status: 'Active', material: '' }])}>
                ➕ เพิ่มเครื่องของผู้รับจ้าง
              </button>
              {importToggle('mc')}
            </div>
          )}
          {importPanel('mc')}

          {adds.map((a, i) => {
            const put = f => v => setAdds(s => s.map((x, j) => j === i ? { ...x, [f]: v } : x))
            // กระบอกอยู่ในชื่อกลุ่มเครื่องแล้ว (SINGLE-32 = 32 นิ้ว) — ช่องเกจจึงรับเลขเกจอย่างเดียว เหมือนหน้าในบ้าน
            const cyl = (a.mc_cat.match(/(\d+)\s*$/) || [])[1]
            return (
            <div key={'new' + i} className="s9new">
              <b>เครื่องใหม่</b>
              {/* ลำดับ/ชื่อช่องเดียวกับฟอร์มเพิ่มเครื่องในบ้าน (McNewWizard) — เลือกจากของที่มี พิมพ์เองต้องกด "สร้างใหม่" */}
              <PickOrNew className="s9pick" label="ผู้รับจ้าง" value={a.supp} options={data.options.supps || []}
                onChange={put('supp')} placeholder="ชื่อผู้รับจ้างใหม่" />
              <PickOrNew className="s9pick" label="กลุ่มเครื่อง (MC_CAT)" value={a.mc_cat} options={data.options.cats || []}
                onChange={put('mc_cat')} placeholder="เช่น SINGLE-32" />
              <PickOrNew className="s9pick" label="ชื่อเครื่อง (MC)" value={a.mc} options={data.options.mcs || []}
                onChange={put('mc')} placeholder="เช่น SKP" />
              <PickOrNew className="s9pick" label="Guage" value={a.guage} options={data.options.gauges || []}
                onChange={put('guage')} placeholder="เช่น 28" numeric bad={!gaugeOk(a.guage)}
                hint={!gaugeOk(a.guage) ? 'เลข Guage อย่างเดียว' : cyl ? `กระบอก ${cyl}" ตามกลุ่มเครื่อง` : ''} />
              <label><span>จำนวนเครื่อง</span>
                <input inputMode="numeric" value={a.total} placeholder="ต้องกรอก"
                  onChange={e => setAdds(s => s.map((x, j) => j === i ? { ...x, total: e.target.value.replace(/[^0-9]/g, '') } : x))} />
              </label>
              <label><span>วัน/สัปดาห์</span>
                <input inputMode="decimal" value={a.work_days} placeholder="ต้องกรอก"
                  onChange={e => setAdds(s => s.map((x, j) => j === i ? { ...x, work_days: e.target.value.replace(/[^0-9.]/g, '') } : x))} />
              </label>
              <button className="s9del" onClick={() => setAdds(s => s.filter((_, j) => j !== i))}>✕</button>
            </div>
            )
          })}

          <div className="mchubhead s9head">
            <span>ผู้รับจ้าง</span><span className="c">เครื่อง</span><span className="c">ใช้งาน</span><span className="c" />
          </div>

          {sup.map(s => {
            const isOpen = !!open[s.supp]
            return (
              <div key={s.supp} className={'mchubpool' + (isOpen ? ' open' : '')}>
                <div className="mchubrow s9row">
                  <button className="mchubname" onClick={() => setOpen(o => ({ ...o, [s.supp]: !o[s.supp] }))}>
                    <span className="caret">{isOpen ? '▾' : '▸'}</span>
                    <b>{s.supp}</b>
                    <small>{mcSummary(s.machines)}</small>
                  </button>
                  <span className="c n">{s.total}</span>
                  <span className="c">{s.machines.length - s.off}/{s.machines.length}</span>
                  <span className="c">{s.off > 0 && <span className="s9off">หยุด {s.off}</span>}</span>
                </div>

                {isOpen && (
                  <div className="mchubdetail">
                    {s.machines.map(m => {
                      const del = dels.includes(m.row)
                      return (
                        <div key={m.row} className={'s9mc' + (del ? ' del' : '')}>
                          <span className="nm">{m.mc}</span>
                          <span className="ct">{m.mc_cat} · Guage {m.guage}</span>
                          {ro ? <span className="n">{fmt(m.total)} เครื่อง</span> : (
                            <>
                              <label><span>เครื่อง</span>
                                <input className={(isBad(`total:${m.row}`) ? 'bad ' : '') + (`total:${m.row}` in edit ? 'chg' : '')}
                                  inputMode="numeric" value={val(`total:${m.row}`, m.total)} placeholder="ต้องกรอก"
                                  onChange={e => set(`total:${m.row}`, e.target.value.replace(/[^0-9]/g, ''))} />
                              </label>
                              <label><span>วัน/สัปดาห์</span>
                                <input className={(isBad(`wd:${m.row}`) ? 'bad ' : '') + (`wd:${m.row}` in edit ? 'chg' : '')}
                                  inputMode="decimal" value={val(`wd:${m.row}`, m.work_days)} placeholder="ต้องกรอก"
                                  onChange={e => set(`wd:${m.row}`, e.target.value.replace(/[^0-9.]/g, ''))} />
                              </label>
                              <label><span>ชนิดผ้า</span>
                                <select className={`mat:${m.row}` in edit ? 'chg' : ''}
                                  value={val(`mat:${m.row}`, m.material)}
                                  onChange={e => set(`mat:${m.row}`, e.target.value)}>
                                  <option value="">— ไม่เลือก = ผ้าผสม —</option>
                                  {data.options.materials.map(o => <option key={o} value={o}>{o}</option>)}
                                </select>
                              </label>
                              <label><span>สถานะ</span>
                                <select className={`status:${m.row}` in edit ? 'chg' : ''}
                                  value={val(`status:${m.row}`, m.status)}
                                  onChange={e => set(`status:${m.row}`, e.target.value)}>
                                  <option value="">— ไม่เลือก = ใช้งาน —</option>
                                  {data.options.statuses.map(o => <option key={o} value={o}>{o}</option>)}
                                </select>
                              </label>
                              <button className="s9del" title={del ? 'ยกเลิกการลบ' : 'ลบเครื่องนี้'}
                                onClick={() => setDels(d => del ? d.filter(x => x !== m.row) : [...d, m.row])}>
                                {del ? '↩' : '✕'}
                              </button>
                            </>
                          )}
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}
        </>
      )}

      {/* ---------------- ผ้าที่ต้องจ้างทอเสมอ ---------------- */}
      {tab === 'only' && (
        <div className="s9items">
          <div className="hint small s9itemhint">
            <span>
              ผ้าในรายการนี้ ({only.length} รายการ) จ้างทอเท่านั้น ห้ามทอในบ้าน ·
              ระบบใช้ Cap/Day + Guage + CAT ตามที่กรอกในหน้านี้อย่างเดียว · MC ไว้ดูอย่างเดียว ·
              <b> ขาด Cap/Day, Guage หรือ CAT หรือไม่มีผู้รับจ้างที่รับได้ = ไม่ทำแผน</b> (ดูเหตุผลที่ชีท UNPLANNED)
              {only.some(x => !x.cap || !x.mc_cat || !x.guage) && (
                <span className="s9impwarn"> ⚠️ กรอกยังไม่ครบ {only.filter(x => !x.cap || !x.mc_cat || !x.guage).length} รายการ — ผ้าเหล่านี้จะไม่ถูกวางแผน</span>
              )}
            </span>
            {importToggle('only')}
          </div>

          {importPanel('only')}

          {!ro && (
            <div className="s9onlyadd s9itemadd">
              <input value={onlyAdd.item} placeholder="Item เช่น FD4DRTLH140/02A0"
                onChange={e => setOnlyAdd(s => ({ ...s, item: e.target.value.toUpperCase() }))} />
              <input list="s9i-group" value={onlyAdd.mc_group} placeholder="MC"
                onChange={e => setOnlyAdd(s => ({ ...s, mc_group: e.target.value.toUpperCase() }))} />
              <input list="s9i-cat" value={onlyAdd.mc_cat} placeholder="CAT"
                onChange={e => setOnlyAdd(s => ({ ...s, mc_cat: e.target.value.toUpperCase() }))} />
              <input value={onlyAdd.guage} placeholder="Guage" inputMode="numeric"
                onChange={e => setOnlyAdd(s => ({ ...s, guage: e.target.value.replace(/[^0-9]/g, '') }))} />
              <input value={onlyAdd.cap} placeholder="Cap/Day" inputMode="decimal"
                onChange={e => setOnlyAdd(s => ({ ...s, cap: e.target.value.replace(/[^0-9.]/g, '') }))} />
              <button className="primary" onClick={addOnly} disabled={saving || onlyMiss.length > 0}
                title={onlyMiss.length ? 'ยังไม่ได้กรอก ' + onlyMiss.join(', ') : ''}>+ เพิ่ม</button>
            </div>
          )}

          <input className="mchubsearch" placeholder="🔍 ค้นหา Item / MC"
            value={q} onChange={e => setQ(e.target.value)} />
          <table className="s9tbl">
            <thead><tr><th>Item</th><th>MC</th><th>CAT</th><th>Guage</th><th>Cap/Day</th>{!ro && <th />}</tr></thead>
            <tbody>
              {shownOnly.map(x => {
                if (ro) return (
                  <tr key={x.row}>
                    <td>{x.item}</td><td>{x.mc_group}</td><td>{x.mc_cat}</td><td>{x.guage}</td>
                    <td>{x.cap != null ? Math.round(x.cap) : ''}</td>
                  </tr>
                )
                const del = onlyDel.includes(x.row)
                return (
                  <tr key={x.row} className={del ? 'del' : ''}>
                    <td>{cellInput('ocode', x.row, x.item)}</td>
                    <td>{cellInput('ogrp', x.row, x.mc_group, { list: 's9i-group' })}</td>
                    <td>{cellInput('ocat', x.row, x.mc_cat, { list: 's9i-cat' })}</td>
                    <td>{cellInput('ogau', x.row, x.guage)}</td>
                    <td>{cellInput('ocap', x.row, x.cap)}</td>
                    <td>
                      <button className="s9del" title={del ? 'ยกเลิกการลบ' : 'ลบผ้านี้'}
                        onClick={() => setOnlyDel(d => del ? d.filter(r => r !== x.row) : [...d, x.row])}>
                        {del ? '↩' : '✕'}
                      </button>
                    </td>
                  </tr>
                )
              })}
              {!shownOnly.length && <tr><td colSpan={6} className="hint">{query ? 'ไม่พบรายการที่ค้นหา' : 'ยังไม่มีรายการ'}</td></tr>}
            </tbody>
          </table>
        </div>
      )}

      {/* ---------------- ผ้าที่จ้างทอได้ ---------------- */}
      {tab === 'items' && (
        <div className="s9items">
          <div className="hint small s9itemhint">
            <span>
              ผ้าในรายการนี้ ({items.length} รายการ) ส่งจ้างทอได้เมื่อเครื่องในบ้านไม่พอ ·
              ไม่มี Cap/Day = ระบบไม่ส่งจ้างทอ
              {!ro && <> · แถวที่เพิ่มเองจะหายเมื่อ Import รอบถัดไป</>}
            </span>
            {importToggle('items')}
          </div>

          {importPanel('items')}

          {!ro && (
            <div className="s9onlyadd s9itemadd">
              <input value={itemAdd.item} placeholder="Item เช่น FD1BASIJ22/1A0"
                onChange={e => setItemAdd(s => ({ ...s, item: e.target.value.toUpperCase() }))} />
              <input list="s9i-group" value={itemAdd.mc_group} placeholder="MC"
                onChange={e => setItemAdd(s => ({ ...s, mc_group: e.target.value.toUpperCase() }))} />
              <input list="s9i-cat" value={itemAdd.mc_cat} placeholder="CAT"
                onChange={e => setItemAdd(s => ({ ...s, mc_cat: e.target.value.toUpperCase() }))} />
              <input value={itemAdd.guage} placeholder="Guage" inputMode="numeric"
                onChange={e => setItemAdd(s => ({ ...s, guage: e.target.value.replace(/[^0-9]/g, '') }))} />
              <input value={itemAdd.cap} placeholder="Cap/Day" inputMode="decimal"
                onChange={e => setItemAdd(s => ({ ...s, cap: e.target.value.replace(/[^0-9.]/g, '') }))} />
              <button className="primary" onClick={addItem} disabled={saving || itemMiss.length > 0}
                title={itemMiss.length ? 'ยังไม่ได้กรอก ' + itemMiss.join(', ') : ''}>+ เพิ่ม</button>
            </div>
          )}

          <input className="mchubsearch" placeholder="🔍 ค้นหา Item / MC"
            value={q} onChange={e => setQ(e.target.value)} />
          <table className="s9tbl">
            <thead><tr><th>Item</th><th>MC</th><th>CAT</th><th>Guage</th><th>Cap/Day</th>{!ro && <th />}</tr></thead>
            <tbody>
              {shownItems.map(x => {
                const del = itemDel.includes(x.row)
                const cell = (kind, cur, extra) => cellInput(kind, x.row, cur, extra)
                if (ro) return (
                  <tr key={x.row}>
                    <td>{x.item}</td><td>{x.mc_group}</td><td>{x.mc_cat}</td><td>{x.guage}</td>
                    <td>{x.cap != null ? Math.round(x.cap) : ''}</td>
                  </tr>
                )
                return (
                  <tr key={x.row} className={del ? 'del' : ''}>
                    <td>{cell('icode', x.item)}</td>
                    <td>{cell('igrp', x.mc_group, { list: 's9i-group' })}</td>
                    <td>{cell('icat', x.mc_cat, { list: 's9i-cat' })}</td>
                    <td>{cell('igau', x.guage)}</td>
                    <td>{cell('cap', x.cap)}</td>
                    {!ro && <td>
                      <button className="s9del" title={del ? 'ยกเลิกการลบ' : 'ลบผ้านี้'}
                        onClick={() => setItemDel(d => del ? d.filter(r => r !== x.row) : [...d, x.row])}>
                        {del ? '↩' : '✕'}
                      </button>
                    </td>}
                  </tr>
                )
              })}
            </tbody>
          </table>
          {!query && items.length > 200 && (
            <div className="hint small">แสดง 200 รายการแรก — พิมพ์ค้นหาเพื่อดูรายการอื่น</div>
          )}
        </div>
      )}

      {!ro && dirty > 0 && (
        <div className={'mchubsave' + (bad.length ? ' blocked' : '')}>
          {bad.length
            ? <span className="bad">⛔ ยังบันทึกไม่ได้ — {bad.join(' · ')}</span>
            : <span>✏️ แก้ค้างไว้ <b>{dirty}</b> รายการ</span>}
          <button onClick={clearAll} disabled={saving}>ยกเลิก</button>
          <button className="primary" onClick={save} disabled={saving || bad.length > 0}>
            {saving ? 'กำลังบันทึก...' : 'บันทึก'}
          </button>
        </div>
      )}

      <div className="gridhelp">
        เครื่องที่ตั้งสถานะ <b>Inactive</b> จะไม่ถูกนับเป็นกำลังผลิต ·
        ชนิดผ้าเว้นว่าง = ผ้าผสม (BLEND) · จ้างทอเริ่มทอได้เร็วสุดกี่สัปดาห์ข้างหน้า ตั้งที่ Setting › จ้างทอข้างนอก
      </div>
    </div>
  )
}

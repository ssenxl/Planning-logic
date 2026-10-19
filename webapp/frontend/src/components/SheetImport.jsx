import React, { useRef, useState } from 'react'
import { api } from '../api.js'

// แผง "นำเข้าทับทั้งชีท" ของตารางข้อมูลหลัก (ชีทที่เปิดเป็นตารางธรรมดา)
// ที่มาได้ 2 ทาง: ไฟล์ .xlsx (backend อ่านให้) หรือคัดลอกจาก Excel มาวาง (tab คั่นคอลัมน์)
// จับคู่คอลัมน์ด้วย "ชื่อหัว" กับชีทเดิม — ลำดับในไฟล์สลับได้, คอลัมน์ในไฟล์ที่ชีทไม่มีถูกข้าม
// ชีทคงรูปคอลัมน์เดิมทุกอย่าง แค่แถวถูกแทนทั้งหมด
// ยืนยันแล้วส่งผ่าน onApply(rows) → Masters.replaceGrid = เซฟเอง + เข้าประวัติ + ย้อนกลับได้ทั้งก้อน

// ชื่อหัวเทียบกันแบบไม่สนตัวพิมพ์/ช่องว่าง/ขีดล่าง ("Item code" = "ITEM_CODE")
const hkey = c => String(c ?? '').trim().replace(/[\s_]+/g, ' ').toUpperCase()
const blank = v => String(v ?? '').trim() === ''
const cell = v => (typeof v === 'string' ? v.trim() : v)

// แถวดิบ (แถวแรก = หัว) → ผลตรวจเทียบกับคอลัมน์ของชีท
function check(columns, raw, curRows) {
  const [head, ...body] = raw
  const at = new Map()
  ;(head || []).forEach((h, i) => { const k = hkey(h); if (k && !at.has(k)) at.set(k, i) })
  const map = columns.map(c => (at.has(hkey(c)) ? at.get(hkey(c)) : -1))
  if (!map.some(i => i >= 0)) {
    return { error: 'หัวคอลัมน์ในไฟล์ไม่ตรงกับชีทนี้เลยสักคอลัมน์ — แถวแรกต้องเป็นหัวคอลัมน์: '
      + columns.join(' · ') }
  }
  const rows = []
  let empty = 0
  for (const r of body) {
    const row = map.map(i => (i >= 0 ? cell(r[i] ?? '') : ''))
    if (row.every(blank)) { empty++; continue }
    rows.push(row)
  }
  if (!rows.length) return { error: 'ไม่มีแถวข้อมูล (ต้องมีอย่างน้อย 1 แถวใต้หัวคอลัมน์)' }

  const warn = []
  // คอลัมน์ของชีทที่ไฟล์ไม่มี → ว่างทั้งคอลัมน์หลังนำเข้า (บอกชัด ๆ ถ้าตอนนี้มีข้อมูลอยู่)
  const missing = columns.filter((_, ci) => map[ci] < 0)
  const losing = missing.filter(c => {
    const ci = columns.indexOf(c)
    return curRows.some(r => !blank(r[ci]))
  })
  if (losing.length) warn.push(`ไฟล์ไม่มีคอลัมน์ ${losing.join(', ')} — ข้อมูลเดิมในคอลัมน์นี้จะหายทั้งหมด`)
  const quiet = missing.filter(c => !losing.includes(c))
  if (quiet.length) warn.push(`ไฟล์ไม่มีคอลัมน์ ${quiet.join(', ')} — จะเว้นว่าง`)
  const known = new Set(columns.map(hkey))
  const extra = (head || []).filter(h => !blank(h) && !known.has(hkey(h)))
  if (extra.length) warn.push(`คอลัมน์ในไฟล์ที่ชีทนี้ไม่มี (ข้าม): ${extra.join(', ')}`)
  const seen = new Set()
  let dup = 0
  for (const r of rows) {
    const k = JSON.stringify(r)
    if (seen.has(k)) dup++
    seen.add(k)
  }
  if (dup) warn.push(`${dup} แถวซ้ำกันทุกช่อง — นำเข้าตามไฟล์ (ไม่ตัดทิ้ง)`)
  if (empty) warn.push(`ข้าม ${empty} แถวที่ว่างทั้งแถว`)
  return { rows, warn, found: map.filter(i => i >= 0).length }
}

export default function SheetImport({ name, sheet, grid, onApply, onClose }) {
  const [text, setText] = useState('')
  const [res, setRes] = useState(null)       // ผลตรวจ { rows, warn, found, src } | { error }
  const [busy, setBusy] = useState(false)
  const fileRef = useRef(null)
  const columns = grid.columns

  async function fromFile(file) {
    if (!file) return
    setBusy(true); setRes(null)
    try {
      const d = await api.sheetImportPreview(name, sheet, file)
      setRes({ ...check(columns, [d.columns, ...d.rows], grid.rows), src: `${file.name} (ชีท ${d.sheet})` })
    } catch (e) { setRes({ error: 'อ่านไฟล์ไม่สำเร็จ: ' + e.message }) }
    finally { setBusy(false) }
  }

  function fromPaste() {
    // Excel คัดลอกออกมาเป็นแถวคั่นบรรทัด คอลัมน์คั่น tab — แถวแรกต้องเป็นหัวคอลัมน์
    const lines = text.split(/\r?\n/).filter(l => l.trim())
    if (lines.length < 2) { setRes({ error: 'วางข้อมูลพร้อมหัวคอลัมน์ในแถวแรก อย่างน้อย 2 แถว' }); return }
    setRes({ ...check(columns, lines.map(l => l.split('\t')), grid.rows), src: 'วางจาก Excel' })
  }

  function apply() {
    if (!res?.rows) return
    if (!window.confirm(`แทนที่ข้อมูลทั้งชีท "${sheet}"\nเดิม ${grid.rows.length} แถว → ใหม่ ${res.rows.length} แถว\n\n`
      + 'ระบบบันทึกให้เองทันที — ถ้าผิดกด "↩ ย้อนกลับ" (Ctrl+Z) คืนได้ทั้งก้อน · ยืนยัน?')) return
    onApply(res.rows)
  }

  return (
    <div className="s9import">
      <div className="s9improw">
        <button className="ghost sm" disabled={busy} onClick={() => fileRef.current?.click()}>
          📄 เลือกไฟล์ .xlsx
        </button>
        <input ref={fileRef} type="file" accept=".xlsx,.xlsm" hidden
          onChange={e => { fromFile(e.target.files?.[0]); e.target.value = '' }} />
        <span className="hint small">
          หรือคัดลอกจาก Excel (รวมหัวคอลัมน์) มาวางด้านล่าง · ชื่อหัวต้องตรงกับชีท (สลับลำดับได้):
          <b> {columns.join(' · ')}</b> · ข้อมูลเดิมทั้งชีทจะถูกแทนที่
        </span>
        <button className="ghost sm" onClick={onClose} style={{ marginLeft: 'auto' }}>✕ ปิด</button>
      </div>
      <textarea rows={4} value={text} placeholder={columns.join('\t')}
        onChange={e => { setText(e.target.value); setRes(null) }} />
      <div className="s9improw">
        <button className="ghost sm" onClick={fromPaste} disabled={busy || !text.trim()}>ตรวจข้อมูลที่วาง</button>
        {busy && <span className="hint small">กำลังอ่านไฟล์...</span>}
        {res?.rows && (
          <>
            <span className="s9impsum">
              {res.src} · ตรงกัน {res.found}/{columns.length} คอลัมน์ ·
              เดิม <b>{grid.rows.length}</b> → ใหม่ <b>{res.rows.length}</b> แถว
            </span>
            <button className="primary" onClick={apply}>ยืนยันแทนที่ทั้งชีท</button>
          </>
        )}
      </div>
      {res?.error && <div className="s9impwarn">⚠️ {res.error}</div>}
      {res?.warn?.map(w => <div key={w} className="s9impwarn">⚠️ {w}</div>)}
      {res?.rows && (
        <div className="shimp-prev">
          <table>
            <thead><tr>{columns.map((c, i) => <th key={i}>{c}</th>)}</tr></thead>
            <tbody>
              {res.rows.slice(0, 5).map((r, ri) => (
                <tr key={ri}>{r.map((v, ci) => <td key={ci}>{String(v ?? '')}</td>)}</tr>
              ))}
            </tbody>
          </table>
          {res.rows.length > 5 && <div className="hint small">ตัวอย่าง 5 แถวแรก จาก {res.rows.length} แถว</div>}
        </div>
      )}
    </div>
  )
}

import React, { useLayoutEffect, useMemo, useRef, useState } from 'react'

// ---- helpers ใช้ร่วมกันทั้งหน้า DATA และ Map Item ----
export const norm = (v) => (v === '' || v == null) ? '' : String(v)
export const label = (v) => v === '' ? '(ว่าง)' : v

// ---- helpers ตารางกริด (ใช้ร่วม DATA + แผนผลิต) ----
export const ROWNUM_W = 46

// คอลัมน์ที่เป็น "รหัส/เลขที่/สัปดาห์/เกจ" — ชิดขวาได้แต่ห้ามใส่ comma
export const isIdName = (name) => /(?:^|_)(?:NO|CODE|ID|SO|WEEK|GUAGE|GAUGE|YEAR)(?:_|$)/i.test(String(name))

// เซ็ตของ index คอลัมน์ที่ค่าไม่ว่างทุกช่องเป็นตัวเลข (sample ได้ N แถวแรกเพื่อความเร็ว)
export function numericCols(grid, sample = Infinity) {
  const s = new Set()
  if (!grid) return s
  const lim = Math.min(grid.rows.length, sample)
  grid.columns.forEach((c, ci) => {
    let hasVal = false, allNum = true
    for (let i = 0; i < lim; i++) {
      const v = grid.rows[i][ci]
      if (v === '' || v == null) continue
      hasVal = true
      if (isNaN(Number(v))) { allNum = false; break }
    }
    if (hasVal && allNum) s.add(ci)
  })
  return s
}

// แสดงค่าตัวเลขพร้อม comma หลักพัน (คงทศนิยม); ค่าที่ไม่ใช่ตัวเลขคืนเดิม
export function fmtNum(v) {
  const s = norm(v)
  if (s === '') return s
  const n = Number(s)
  return isNaN(n) ? s : n.toLocaleString('en-US', { maximumFractionDigits: 6 })
}

// ความกว้างเริ่มต้นของแต่ละคอลัมน์ — คำนวณจากหัว + เนื้อหา (64–320px)
export function autoColWidths(grid, sample = 60) {
  const w = {}
  if (!grid) return w
  const lim = Math.min(grid.rows.length, sample)
  grid.columns.forEach((c, ci) => {
    let maxLen = String(c).length
    for (let i = 0; i < lim; i++) {
      const l = norm(grid.rows[i][ci]).length
      if (l > maxLen) maxLen = l
    }
    w[ci] = Math.max(64, Math.min(320, maxLen * 8 + 26))
  })
  return w
}

// สร้าง handler ลากปรับกว้างคอลัมน์ ci — เรียกจาก onMouseDown ของที่จับ
export function makeColResizer(colW, setColW) {
  return (e, ci) => {
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startW = colW[ci] ?? 120
    const move = ev => {
      const nw = Math.max(48, startW + (ev.clientX - startX))
      setColW(w => ({ ...w, [ci]: nw }))
    }
    const up = () => {
      window.removeEventListener('mousemove', move)
      window.removeEventListener('mouseup', up)
      document.body.classList.remove('col-resizing')
    }
    window.addEventListener('mousemove', move)
    window.addEventListener('mouseup', up)
    document.body.classList.add('col-resizing')
  }
}

const matchSearch = (row, s) =>
  !s || row.some(c => String(c ?? '').toLowerCase().includes(s))

// แถวที่ผ่านตัวกรอง (ค้นหารวม + ตัวกรองทุกคอลัมน์) — แบบ AND
export function filterRows(grid, search, filters) {
  const s = search.trim().toLowerCase()
  const fcols = Object.keys(filters).map(Number)
  return grid.rows
    .map((row, idx) => ({ row, idx }))
    .filter(({ row }) => {
      if (!matchSearch(row, s)) return false
      for (const ci of fcols) {
        if (!filters[ci].has(norm(row[ci]))) return false
      }
      return true
    })
}

// ค่าที่ยัง "เลือกได้จริง" ของคอลัมน์ ci (cascade จากตัวกรองคอลัมน์อื่น + ค้นหารวม)
// คืนค่า available: [{value, count}] และ domain: Set ของค่าทั้งหมดในคอลัมน์
export function columnFilterData(grid, filters, search, ci) {
  const s = search.trim().toLowerCase()
  const others = Object.keys(filters).map(Number).filter(c => c !== ci)
  const counts = new Map()
  const domain = new Set()
  for (const row of grid.rows) {
    const v = norm(row[ci])
    domain.add(v)
    if (!matchSearch(row, s)) continue
    let ok = true
    for (const c of others) {
      if (!filters[c].has(norm(row[c]))) { ok = false; break }
    }
    if (!ok) continue
    counts.set(v, (counts.get(v) || 0) + 1)
  }
  const available = Array.from(counts, ([value, count]) => ({ value, count }))
  return { available, domain }
}

// ---- ป็อปอัพตัวกรองแบบ Excel ----
export function ColumnFilter({ available, domain, selected, anchor, onApply, onClose }) {
  // draft = ค่าที่ถูกติ๊ก; ถ้ายังไม่เคยกรอง (selected == null) ให้ถือว่าเลือกทั้งหมด (ทั้งโดเมน)
  const [draft, setDraft] = useState(() => new Set(selected ?? domain))
  const [q, setQ] = useState('')
  const [sortDir, setSortDir] = useState('asc')
  const popRef = useRef(null)
  const [style, setStyle] = useState({ visibility: 'hidden' })

  // กันป็อปอัพหลุดขอบจอ + เด้งขึ้นถ้าใกล้ขอบล่าง
  useLayoutEffect(() => {
    const el = popRef.current
    if (!el) return
    const w = el.offsetWidth, h = el.offsetHeight
    const vw = window.innerWidth, vh = window.innerHeight
    let left = Math.max(8, Math.min(anchor.right - w, vw - w - 8))
    let top = anchor.bottom + 4
    if (top + h > vh - 8) top = Math.max(8, anchor.top - h - 4)
    setStyle({ top, left })
  }, [anchor, available.length])

  const shown = useMemo(() => {
    const qq = q.trim().toLowerCase()
    const arr = available.filter(a => label(a.value).toLowerCase().includes(qq))
    arr.sort((a, b) => label(a.value).localeCompare(label(b.value), 'th', { numeric: true }))
    if (sortDir === 'desc') arr.reverse()
    return arr
  }, [available, q, sortDir])

  const shownChecked = shown.filter(a => draft.has(a.value)).length
  const allShownChecked = shown.length > 0 && shownChecked === shown.length
  const someShownChecked = shownChecked > 0 && shownChecked < shown.length

  const allRef = useRef(null)
  useLayoutEffect(() => {
    if (allRef.current) allRef.current.indeterminate = someShownChecked
  }, [someShownChecked])

  function toggle(v) {
    setDraft(d => { const n = new Set(d); n.has(v) ? n.delete(v) : n.add(v); return n })
  }
  function toggleAll() {
    setDraft(d => {
      const n = new Set(d)
      if (allShownChecked) shown.forEach(a => n.delete(a.value))
      else shown.forEach(a => n.add(a.value))
      return n
    })
  }
  function apply() {
    // ถ้ากำลังค้นหาอยู่ = แบบ Excel: กรองเฉพาะผลการค้นหาที่ติ๊กไว้ (ตัดค่าที่ไม่ตรงคำค้นทิ้ง)
    const result = q.trim()
      ? new Set(shown.filter(a => draft.has(a.value)).map(a => a.value))
      : new Set(draft)
    // ถือว่า "ไม่กรอง" ก็ต่อเมื่อเลือกครบทุกค่าในโดเมนของคอลัมน์
    const full = domain.size > 0 && Array.from(domain).every(v => result.has(v))
    onApply(full ? null : result)
  }

  return (
    <>
      <div className="filterbackdrop" onClick={onClose} />
      <div className="filterpop" ref={popRef} style={style}>
        <div className="popsort">
          <button className={sortDir === 'asc' ? 'on' : ''} onClick={() => setSortDir('asc')}>▲ A→Z</button>
          <button className={sortDir === 'desc' ? 'on' : ''} onClick={() => setSortDir('desc')}>▼ Z→A</button>
        </div>
        <input className="popsearch" autoFocus placeholder="ค้นหาค่า..."
          value={q} onChange={e => setQ(e.target.value)} />
        <label className="popitem all">
          <input type="checkbox" ref={allRef} checked={allShownChecked} onChange={toggleAll} />
          <b>{q.trim() ? '(เลือกผลการค้นหาทั้งหมด)' : '(เลือกทั้งหมด)'}</b>
          <span className="popcount">{shown.length}</span>
        </label>
        <div className="poplist">
          {shown.map((a, i) => (
            <label key={i} className="popitem">
              <input type="checkbox" checked={draft.has(a.value)} onChange={() => toggle(a.value)} />
              <span className="popval">{label(a.value)}</span>
              <span className="popcount">{a.count}</span>
            </label>
          ))}
          {!shown.length && <div className="hint small">ไม่พบค่า</div>}
        </div>
        <div className="popbtns">
          <button onClick={() => onApply(null)}>ล้าง</button>
          <button onClick={onClose}>ยกเลิก</button>
          <button className="primary" onClick={apply}>ตกลง</button>
        </div>
      </div>
    </>
  )
}

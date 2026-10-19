import React, { useDeferredValue, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { ColumnFilter, norm } from './ColumnFilter.jsx'

// ตารางแก้ข้อมูลกลางของหน้า Master Data — ทุกชีทใช้ตัวนี้ตัวเดียว หน้าตา/คีย์ลัดจึงเหมือนกันหมด
//  - เรนเดอร์เฉพาะแถวที่อยู่ในจอ (virtual scroll) → ชีทหลักหมื่นแถวก็เลื่อนลื่น
//  - กรองด้วยช่องเดียวครอบทุกคอลัมน์ · คลิกหัวคอลัมน์ = เรียง · กด ▾ = กรองรายคอลัมน์แบบ Excel
//  - เดินเซลล์แบบ Excel · แถวใหม่ที่ยังไม่บันทึกขึ้นบนสุดเสมอ (★) · ลบแถวต้องกดยืนยันก่อน
// แก้ลง grid ของหน้าแม่โดยตรง → ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกับหน้านั้น
//
// ⚠️ ROW_H / HEAD_H ต้องตรงกับความสูงจริงของ .dtrow / .dthead ใน styles.css
//    ไม่ตรงเมื่อไหร่ ตำแหน่งแถวตอนเลื่อนจะเพี้ยน

const ROW_H = 34
const HEAD_H = 36
const OVERSCAN = 8
const MIN_W = 70
const MAX_W = 260

const val = v => String(v ?? '').trim()

// ความกว้างคอลัมน์ — เดาจากหัวคอลัมน์ + ค่าตัวอย่าง 40 แถวแรก (กติกาเดียวกับตารางเดิม)
function guessWidth(columns, rows, ci) {
  let len = String(columns[ci] ?? '').length
  for (let i = 0; i < Math.min(rows.length, 40); i++) {
    len = Math.max(len, String(rows[i][ci] ?? '').length)
  }
  return Math.round(Math.max(MIN_W, Math.min(MAX_W, 46 + len * 7.4)))
}

// คอลัมน์ที่เลือกค่าจากรายการ (enums) — { [ci]: { options: [{value,label}], placeholder } }
// ช่องพวกนี้เรนเดอร์เป็น <select> แทน <input> เพื่อกันพิมพ์ค่าที่ระบบไม่รู้จัก
// ro = โหมดดูอย่างเดียว (หน้า Master Data) — ล็อกเฉพาะช่องแก้/ปุ่มเพิ่ม-ลบ
// ค้นหา เรียง กรองรายคอลัมน์ ยังใช้ได้ครบ เพราะเป็นการ "ดู" ไม่ใช่การแก้
export default function DataTable({
  grid, rids, cols: colsIn, titleCi, hint, addLabel = '+ เพิ่มแถว', unit = 'แถว', height,
  setCell, addRow, delRow, isChanged, isNewRow, enums, ro,
}) {
  const [q, setQ] = useState('')
  const dq = useDeferredValue(q)             // กรองตามหลังการพิมพ์ → ช่องกรองไม่กระตุกบนชีทใหญ่
  const [sort, setSort] = useState(null)     // { ci, dir } — เรียงเพื่อดูเท่านั้น ไม่เปลี่ยนลำดับในไฟล์
  const [filters, setFilters] = useState({}) // { ci: Set(ค่าที่ติ๊กไว้) } — ตัวกรองรายคอลัมน์แบบ Excel
  const [openCol, setOpenCol] = useState(null)  // คอลัมน์ที่เปิดป็อปอัพกรองอยู่ { ci, anchor }
  const [top, setTop] = useState(0)          // ตำแหน่งที่เลื่อนอยู่
  const [vh, setVh] = useState(560)          // ความสูงกล่องตาราง
  const boxRef = useRef(null)
  const focusRef = useRef(null)              // ช่องที่รอโฟกัส (แถวปลายทางอาจยังไม่ถูกเรนเดอร์)

  // ไม่ส่ง cols มา = แสดงทุกคอลัมน์ของชีทตามลำดับในไฟล์
  const cols = useMemo(
    () => colsIn || grid.columns.map((label, ci) => ({ ci, label })),
    [colsIn, grid.columns])

  // กว้างคอลัมน์คิดครั้งเดียวต่อโครงคอลัมน์ — ไม่คิดใหม่ตอนพิมพ์ ไม่งั้นคอลัมน์ขยับหนีมือ
  const rowsRef = useRef(grid.rows)
  rowsRef.current = grid.rows
  const widths = useMemo(
    () => cols.map(c => c.w ?? guessWidth(grid.columns, rowsRef.current, c.ci)),
    [cols, grid.columns])
  // คอลัมน์ที่ระบุ flex (ไม่ระบุ = คอลัมน์สุดท้าย) ยืดเต็มพื้นที่ที่เหลือ → ไม่มีช่องว่างค้างทางขวา
  const flexAt = cols.findIndex(c => c.flex)
  const fi = flexAt >= 0 ? flexAt : widths.length - 1
  const tpl = `54px ${widths.map((w, i) =>
    i === fi ? `minmax(${w}px, 1fr)` : `${w}px`).join(' ')} 44px`
  const minW = 54 + widths.reduce((a, b) => a + b, 0) + 44

  const query = dq.trim().toLowerCase()
  const filterCis = useMemo(() => Object.keys(filters).map(Number), [filters])
  const hasFilter = filterCis.length > 0

  // เปลี่ยนชีท (โครงคอลัมน์เปลี่ยน) → ล้างตัวกรองเก่าทิ้ง ไม่งั้นตัวกรองของชีทก่อนหน้า
  // จะไปกรองคอลัมน์ตำแหน่งเดียวกันของชีทใหม่ แล้วตารางขึ้นว่างโดยไม่มีสาเหตุที่มองเห็น
  useEffect(() => { setFilters({}); setOpenCol(null) }, [grid.columns])

  // ลำดับแถวที่แสดง: แถวใหม่ (ยังไม่บันทึก) บนสุดเสมอ → แถวที่ผ่านคำค้น + ตัวกรองรายคอลัมน์
  const { ordered, newCount, total } = useMemo(() => {
    const news = [], rest = []
    grid.rows.forEach((_, ri) => (isNewRow(ri) ? news : rest).push(ri))
    const cis = cols.map(c => c.ci)
    const hit = ri => cis.some(c => String(grid.rows[ri][c] ?? '').toLowerCase().includes(query))
    // ตัวกรองรายคอลัมน์เป็นแบบ AND เหมือน Excel — ค่าที่ไม่ถูกติ๊กคือถูกซ่อน
    const pass = ri => filterCis.every(c => filters[c].has(norm(grid.rows[ri][c])))
    let filtered = query ? rest.filter(hit) : rest
    if (hasFilter) filtered = filtered.filter(pass)
    if (sort) {
      const s = sort.dir === 'asc' ? 1 : -1
      filtered.sort((a, b) => {
        const x = val(grid.rows[a][sort.ci]), y = val(grid.rows[b][sort.ci])
        if (!x && !y) return 0
        if (!x) return 1                    // ช่องว่างไปท้ายเสมอ ไม่ว่าเรียงทางไหน
        if (!y) return -1
        return s * String(x).localeCompare(String(y), 'th', { numeric: true })
      })
    }
    return { ordered: [...news, ...filtered], newCount: news.length, total: grid.rows.length }
  }, [grid, query, sort, cols, isNewRow, filters, filterCis, hasFilter])

  // ค่าที่ยังเลือกได้จริงของคอลัมน์ที่เปิดป็อปอัพ — cascade จากคำค้น + ตัวกรองคอลัมน์อื่น
  // (เขียนเองแทน columnFilterData ของ ColumnFilter.jsx เพราะที่นี่คำค้นดูเฉพาะคอลัมน์ที่โชว์
  //  ตัวเลขนับในป็อปอัพจึงต้องคิดด้วยกติกาเดียวกัน ไม่งั้นนับไม่ตรงกับแถวที่เห็น)
  const colData = useMemo(() => {
    if (!openCol) return null
    const ci = openCol.ci
    const cis = cols.map(c => c.ci)
    const others = filterCis.filter(c => c !== ci)
    const counts = new Map()
    const domain = new Set()
    grid.rows.forEach((row, ri) => {
      if (isNewRow(ri)) return           // แถวใหม่ยังไม่บันทึก โชว์บนสุดเสมอ ไม่เข้าตัวกรอง
      const v = norm(row[ci])
      domain.add(v)
      if (query && !cis.some(c => String(row[c] ?? '').toLowerCase().includes(query))) return
      if (!others.every(c => filters[c].has(norm(row[c])))) return
      counts.set(v, (counts.get(v) || 0) + 1)
    })
    return { available: Array.from(counts, ([value, count]) => ({ value, count })), domain }
  }, [openCol, grid, cols, filters, filterCis, query, isNewRow])

  function openColMenu(e, ci) {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setOpenCol({ ci, anchor: { top: r.top, bottom: r.bottom, left: r.left, right: r.right } })
  }

  // set = null → เลิกกรองคอลัมน์นั้น (ป็อปอัพส่ง null มาเมื่อเลือกครบทุกค่า)
  function applyFilter(ci, set) {
    setFilters(f => {
      const n = { ...f }
      if (set == null) delete n[ci]
      else n[ci] = set
      return n
    })
    setOpenCol(null)
  }

  // ---- หน้าต่างแถวที่เรนเดอร์จริง ----
  const start = Math.max(0, Math.floor((top - HEAD_H) / ROW_H) - OVERSCAN)
  const end = Math.min(ordered.length, Math.ceil((top - HEAD_H + vh) / ROW_H) + OVERSCAN)
  const shown = ordered.slice(start, end)

  useLayoutEffect(() => {
    const el = boxRef.current
    if (!el || typeof ResizeObserver === 'undefined') return
    const upd = () => setVh(el.clientHeight)
    upd()
    const ro = new ResizeObserver(upd)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // โฟกัสช่องที่ขอไว้ — ทำหลังเรนเดอร์ เพราะแถวปลายทางอาจเพิ่งถูกเรนเดอร์จากการเลื่อน
  useEffect(() => {
    const f = focusRef.current
    if (!f) return
    const el = boxRef.current?.querySelector(`[data-dt="${f.vi}-${f.cj}"]`)
    if (el) { focusRef.current = null; el.focus(); el.select?.() }
  })

  function focusAt(vi, cj) {
    const box = boxRef.current
    if (box) {
      const y = HEAD_H + vi * ROW_H
      if (y < box.scrollTop + HEAD_H) box.scrollTop = y - HEAD_H          // ใต้หัวตารางที่ลอยอยู่
      else if (y + ROW_H > box.scrollTop + box.clientHeight) box.scrollTop = y + ROW_H - box.clientHeight
    }
    focusRef.current = { vi, cj }
    const el = box?.querySelector(`[data-dt="${vi}-${cj}"]`)
    if (el) { focusRef.current = null; el.focus(); el.select?.() }
  }

  // เดินเซลล์แบบ Excel — ←/→ ย้ายคอลัมน์เฉพาะตอนเคอร์เซอร์อยู่สุดขอบข้อความ
  function onKey(e, vi, cj) {
    const el = e.currentTarget
    const go = (dv, dc) => {
      const nvi = vi + dv, ncj = cj + dc
      if (nvi < 0 || nvi >= ordered.length || ncj < 0 || ncj >= cols.length) return
      e.preventDefault()
      focusAt(nvi, ncj)
    }
    // <select> ไม่มีเคอร์เซอร์ข้อความ — ถือว่าอยู่สุดขอบเสมอ (←/→ ใช้ย้ายคอลัมน์ได้เลย)
    // และ ↑/↓ ต้องปล่อยให้เป็นการ "เลือกค่าในรายการ" ตามปกติของ select
    if (el.tagName === 'SELECT') {
      if (e.key === 'Enter') go(e.shiftKey ? -1 : 1, 0)
      else if (e.key === 'ArrowRight') go(0, 1)
      else if (e.key === 'ArrowLeft') go(0, -1)
      else if (e.key === 'Escape') el.blur()
      return
    }
    const atEnd = el.selectionStart === el.value.length && el.selectionStart === el.selectionEnd
    const atStart = el.selectionStart === 0 && el.selectionEnd === 0
    if (e.key === 'Enter') go(e.shiftKey ? -1 : 1, 0)
    else if (e.key === 'ArrowDown') go(1, 0)
    else if (e.key === 'ArrowUp') go(-1, 0)
    else if (e.key === 'ArrowRight' && atEnd) go(0, 1)
    else if (e.key === 'ArrowLeft' && atStart) go(0, -1)
    else if (e.key === 'Escape') el.blur()
  }

  // ลบแล้วแถวหายทันที (กู้ได้แค่ Ctrl+Z ที่ user อาจไม่รู้) → ถามยืนยันพร้อมบอกว่าแถวไหน
  function askDel(ri) {
    const row = grid.rows[ri]
    const key = titleCi != null && titleCi >= 0 ? val(row[titleCi]) : ''
    const rest = cols.filter(c => c.ci !== titleCi).map(c => val(row[c.ci])).filter(Boolean)
    const name = key || rest.shift() || 'แถวเปล่า (ยังไม่ได้กรอกอะไร)'
    const more = rest.slice(0, 5).join(' · ')
    if (!confirm(`ลบแถวนี้ออกจากชีท?\n\n${name}${more ? `\n${more}` : ''}\n\n` +
      'ยังไม่เขียนลงไฟล์จนกว่าจะกด "บันทึก" — กด "ย้อนกลับ" หรือ Ctrl+Z เรียกคืนได้')) return
    delRow(ri)
  }

  // แถวใหม่ต่อท้ายไฟล์ แต่โผล่บนสุดของตาราง (ถัดจากแถวใหม่ก่อนหน้า) → โฟกัสที่ตำแหน่งนั้น
  function add() {
    addRow()
    setTimeout(() => focusAt(newCount, 0), 0)
  }

  const toggleSort = ci =>
    setSort(s => (!s || s.ci !== ci) ? { ci, dir: 'asc' } : s.dir === 'asc' ? { ci, dir: 'desc' } : null)

  return (
    <div className="dtview">
      <div className="dtbar">
        {hint}
        <input className="dtsearch" placeholder="🔍 กรองทุกคอลัมน์..."
          value={q} onChange={e => { setQ(e.target.value); if (boxRef.current) boxRef.current.scrollTop = 0 }} />
        <span className="dtcount">
          {(query || hasFilter) && <>พบ <b>{(ordered.length - newCount).toLocaleString()}</b> / </>}
          {total.toLocaleString()} {unit}
          {hasFilter && <> · กรอง <b>{filterCis.length}</b> คอลัมน์</>}
        </span>
        {(query || sort || hasFilter) && (
          <button className="dtclear"
            onClick={() => { setQ(''); setSort(null); setFilters({}); setOpenCol(null) }}>ล้างตัวกรอง</button>
        )}
        {addRow && !ro && <button className="dtaddbtn" onClick={add}>{addLabel}</button>}
      </div>

      <div className="dtbl" ref={boxRef} style={height ? { height } : undefined}
        onScroll={e => setTop(e.currentTarget.scrollTop)}>
        <div className="dthead" style={{ gridTemplateColumns: tpl, minWidth: minW }}>
          <span className="dth dthnum">#</span>
          {cols.map(c => (
            // หัวคอลัมน์มี 2 ปุ่มในช่องเดียว (เรียง + กรอง) → ห่อด้วย div ไม่ใช่ button ซ้อน button
            <div key={c.ci} className={'dth dthcol' + (sort?.ci === c.ci ? ' on' : '')}>
              <button className={'dtsort' + (sort?.ci === c.ci ? ' on' : '')}
                onClick={() => toggleSort(c.ci)} title={`${c.label}\nคลิก = เรียงลำดับ`}>
                <span className="dthlabel">{c.label}</span>
                <span className="dtarrow">{sort?.ci === c.ci ? (sort.dir === 'asc' ? '▲' : '▼') : ''}</span>
              </button>
              <button className={'funnel' + (filters[c.ci] ? ' on' : '')}
                title={`กรองคอลัมน์ "${c.label}"`}
                onClick={e => openColMenu(e, c.ci)}>▾</button>
            </div>
          ))}
          <span className="dth" />
        </div>

        <div style={{ height: start * ROW_H }} />
        {shown.map((ri, i) => {
          const vi = start + i
          return (
            <div key={rids?.[ri] ?? ri} className={'dtrow' + (isNewRow(ri) ? ' newrow' : '')}
              style={{ gridTemplateColumns: tpl, minWidth: minW }}>
              <span className="dtnum">{isNewRow(ri) ? '★' : vi + 1 - newCount}</span>
              {cols.map((c, cj) => {
                const en = enums?.[c.ci]
                const cls = 'dtcell' + (c.cls ? ' ' + c.cls : '') + (isChanged(ri, c.ci) ? ' chg' : '')
                const cur = grid.rows[ri][c.ci] ?? ''
                if (en && !ro) {
                  // ค่าที่อยู่ในไฟล์แต่ไม่มีในรายการ (พิมพ์มาจาก Excel) ต้องยังเห็นและไม่ถูกลบทิ้งเงียบ ๆ
                  const known = en.options.some(o => o.value === cur)
                  return (
                    <select key={c.ci} data-dt={`${vi}-${cj}`} disabled={ro}
                      className={cls + ' dtsel' + (!known && cur ? ' unknown' : '')}
                      value={cur} onChange={e => setCell(ri, c.ci, e.target.value)}
                      onKeyDown={e => onKey(e, vi, cj)}>
                      <option value="">{en.placeholder || '—'}</option>
                      {en.options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                      {!known && cur && <option value={cur}>{cur} (ค่าที่ระบบไม่รู้จัก)</option>}
                    </select>
                  )
                }
                // โหมดดู: วาดเป็นข้อความ ไม่ใช่ช่องกรอกที่ถูกล็อก — ตารางอ่านง่ายกว่ากันมาก
                if (ro) return <span key={c.ci} className={cls + ' rocell'}>{cur}</span>
                return (
                  <input key={c.ci} data-dt={`${vi}-${cj}`} autoComplete="off"
                    className={cls} value={cur} placeholder={c.ph}
                    onChange={e => setCell(ri, c.ci, e.target.value)}
                    onKeyDown={e => onKey(e, vi, cj)} />
                )
              })}
              {ro
                ? <span className="dtdel ro" />
                : <button className="dtdel" tabIndex={-1} title="ลบแถวนี้ (ถามยืนยันก่อน)"
                  onClick={() => askDel(ri)}>✕</button>}
            </div>
          )
        })}
        <div style={{ height: Math.max(0, (ordered.length - end) * ROW_H) }} />

        {!ordered.length && (
          <div className="dtempty">
            {query || hasFilter ? 'ไม่พบแถวที่ตรงกับคำค้น/ตัวกรอง' : 'ยังไม่มีข้อมูล'}
          </div>
        )}
      </div>

      {openCol && colData && (
        <ColumnFilter
          key={openCol.ci}
          available={colData.available}
          domain={colData.domain}
          selected={filters[openCol.ci]}
          anchor={openCol.anchor}
          onApply={set => applyFilter(openCol.ci, set)}
          onClose={() => setOpenCol(null)}
        />
      )}

      <div className="gridhelp">
        คลิกหัวคอลัมน์ = เรียงลำดับ · <b>▾ = กรองคอลัมน์นั้น</b> (เลือกค่าแบบ Excel) ·
        Enter / ↑ ↓ = ย้ายแถว · ← → = ย้ายคอลัมน์
        {ro
          ? <> · <b>หน้านี้ดูอย่างเดียว</b> — แก้ได้ที่ ⚙️ Setting › ข้อมูลหลัก</>
          : <> · ★ = แถวใหม่ยังไม่บันทึก · <span className="chgdot" /> = ช่องที่แก้แล้วยังไม่บันทึก ·
            แก้เสร็จกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ</>}
      </div>
    </div>
  )
}

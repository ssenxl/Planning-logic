import React, { useMemo, useState, useRef, useLayoutEffect } from 'react'
import { norm } from './ColumnFilter.jsx'

// จานสีแยกประเภท (Tableau-20) — รองรับหลายกลุ่ม CAT/เกจ
const PALETTE = ['#4e79a7', '#f28e2b', '#59a14f', '#e15759', '#76b7b2',
  '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac',
  '#a0cbe8', '#ffbe7d', '#8cd17d', '#ff9d9a', '#86bcb6',
  '#f1ce63', '#d4a6c8', '#fabfd2', '#d7b5a6', '#79706e']

// คอลัมน์ที่ใช้เป็น "หัวแถว" ฝั่งซ้าย (เรียงซ้าย→ขวา) + ป้ายภาษาไทย + ความกว้าง(px)
const GROUP_DEF = [
  { col: 'CAT', label: 'CAT', width: 76 },
  { col: 'MC_GUAGE', label: 'Guage', width: 64 },
  { col: 'MC_GROUP', label: 'เครื่อง', width: 96 },
]

// คอลัมน์ที่ใช้กำหนด "สี" ของบล็อก (แยกสีตาม CAT + เกจ)
const COLOR_DEF = ['CAT', 'MC_GUAGE']

// คู่เครื่องที่สลับกันทำแทนกันได้ (เครื่อง pool เดียวกัน CAT|เกจ) — ต้องสอดคล้องกับ
// MC_GROUP_REDIRECT ใน Planning.py (เช่น SKP G20 ถูก redirect ไป FA G20)
// ถ้าแถวใดมี (CAT, เกจ) ตรงกลุ่มนี้ จะโชว์ทุก MC_GROUP ในกลุ่มแม้ยังไม่มีงานวาง
// เพื่อให้ลากงานมาวาง (ย้ายข้ามเครื่อง) ได้
const SWAP_GROUPS = [
  { cat: 'SINGLE-32', gauge: '20', groups: ['FA', 'SKP'] },
]
// normalize สำหรับเทียบค่า (trim + upper) เพราะ norm() ไม่ได้ trim/upper ให้
const nkey = (v) => String(v ?? '').trim().toUpperCase()

/**
 * Gantt แผนผลิต: แกนตั้ง = เครื่อง/เกจ/CAT, แกนนอน = PLAN_WEEK
 * - บล็อก = งาน 1 รายการ (ITEM_CODE) กว้าง 1 สัปดาห์ สีตาม CAT + เกจ
 * - ลากบล็อกไปคอลัมน์สัปดาห์อื่น → เปลี่ยน PLAN_WEEK (กลุ่มแถวคงเดิม) แล้ว sync ตาราง
 *
 * props:
 *   columns : string[]              — หัวคอลัมน์ของ grid
 *   rows    : [{ row, idx }]        — แถวที่ผ่านตัวกรอง (จาก filterRows) พร้อม index จริง
 *   onMoveWeek(idx, weekStr)        — เรียกเมื่อผู้ใช้ลากบล็อกไปสัปดาห์ใหม่
 */
// ประเภทงาน (ตรงกับ REMAINING_JOBS) + ป้ายแสดงผล
const LOAD_TYPES = [
  { key: 'OM', label: 'OM' },
  { key: 'PHET_DOUBLE', label: 'PHET DOUBLE' },
  { key: 'PHET_SINGLE', label: 'PHET SINGLE' },
]

// เลขสัปดาห์ปัจจุบันตามนิยาม Fri–Thu (บวก 3 วันก่อนคิด ISO week) — ต้องตรงกับ Calendar.py
function currentPlanWeek() {
  const now = new Date()
  const d = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate() + 3))
  const dayNum = (d.getUTCDay() + 6) % 7          // จันทร์=0 ... อาทิตย์=6
  d.setUTCDate(d.getUTCDate() - dayNum + 3)        // เลื่อนไปวันพฤหัสของสัปดาห์ ISO
  const firstThursday = d.getTime()
  d.setUTCMonth(0, 1)
  if (d.getUTCDay() !== 4)
    d.setUTCMonth(0, 1 + ((4 - d.getUTCDay()) + 7) % 7)
  return 1 + Math.ceil((firstThursday - d.getTime()) / 604800000)
}

// จำแนกแถว PLAN เป็นประเภท OM / PHET_DOUBLE / PHET_SINGLE (null = ไม่นับ เช่น OUTSOURCE)
function classifyType(factory, cat) {
  const f = String(factory).trim().toUpperCase()
  if (f.startsWith('OM')) return 'OM'
  if (f === 'PHET') return String(cat).toUpperCase().includes('DOUBLE') ? 'PHET_DOUBLE' : 'PHET_SINGLE'
  return null
}

export default function PlanGantt({ columns, rows, load = {}, ava = {}, onMoveWeek, colorRows, onRemove, onEditQty, lockBefore = null }) {
  const [dragIdx, setDragIdx] = useState(null)
  const [overWeek, setOverWeek] = useState(null)
  // double click บล็อก → แก้ตัวเลขจำนวน (กก.) inline แล้วส่งค่าใหม่ผ่าน onEditQty(idx, qty)
  const [editIdx, setEditIdx] = useState(null)
  const [editVal, setEditVal] = useState('')
  // สัปดาห์ที่ล็อก (freeze) — โชว์ได้แต่ลาก/ถอด/วางไม่ได้
  const isLocked = (w) => lockBefore != null && Number(w) < Number(lockBefore)

  const ci = useMemo(() => {
    const at = (name) => columns.indexOf(name)
    return {
      week: at('PLAN_WEEK'), item: at('ITEM_CODE'), qty: at('PRODUCE_QTY'),
      reqmc: at('REQUIRED_MC'), factory: at('FACTORY_TYPE'), cat: at('CAT'),
      newmc: at('NEW_MC'), gauge: at('MC_GUAGE'), actualmc: at('ACTUAL_MC'),
    }
  }, [columns])

  // คอลัมน์หัวแถวที่มีอยู่จริง + ตำแหน่ง sticky (left สะสม)
  const groups = useMemo(() => {
    let left = 0
    return GROUP_DEF
      .filter(g => columns.includes(g.col))
      .map(g => { const item = { ...g, idx: columns.indexOf(g.col), left }; left += g.width; return item })
  }, [columns])

  // คอลัมน์กำหนดสี (CAT + เกจ) ที่มีจริง
  const colorCols = useMemo(
    () => COLOR_DEF.filter(n => columns.includes(n)).map(n => columns.indexOf(n)),
    [columns])

  const supported = groups.length > 0 && ci.week >= 0
  const rowKey = (row) => groups.map(g => norm(row[g.idx])).join('')
  // ตำแหน่งคอลัมน์ CAT / เกจ ใน group (ใช้ทำ key ของ AVA = "CAT|เกจ")
  const avaCatI = groups.findIndex(g => g.col === 'CAT')
  const avaGaugeI = groups.findIndex(g => g.col === 'MC_GUAGE')
  const mcGroupI = groups.findIndex(g => g.col === 'MC_GROUP')
  const colorKey = (row) => colorCols.length
    ? colorCols.map(i => norm(row[i]) || '(ว่าง)').join(' / ')
    : 'ทั้งหมด'

  // แกนสัปดาห์ — โชว์ทุกสัปดาห์ที่ทำแผนได้ (ลากงานไปได้ทุก week)
  // รวมสัปดาห์จาก rows(งาน) + load(capacity) + ava(เครื่องว่าง) เพื่อครอบคลุม horizon เต็ม
  // ตัดสัปดาห์ที่ freeze ออก: เริ่มที่ current+2 (สัปดาห์ปัจจุบัน+สัปดาห์หน้าแก้แผนไม่ได้)
  const weeks = useMemo(() => {
    if (!supported) return []
    const vals = new Set()
    for (const { row } of rows) { const v = norm(row[ci.week]); if (v !== '') vals.add(v) }
    for (const w of Object.keys(load || {})) vals.add(String(w))
    for (const w of Object.keys(ava || {})) vals.add(String(w))
    const arr = [...vals]
    const nums = arr.map(Number).filter(Number.isFinite)
    if (arr.length && nums.length === arr.length) {
      // ปกติเริ่ม current+2 (freeze ซ่อน); ถ้า lockBefore ส่งมา = โชว์รวมสัปดาห์ freeze (ล็อกไว้)
      const lo = lockBefore != null ? Number(lockBefore) - 2 : currentPlanWeek() + 2
      const mx = Math.max(...nums)
      if (mx < lo) return []
      const range = Array.from({ length: mx - lo + 1 }, (_, i) => String(lo + i))
      // โหมด lockBefore: ตัดสัปดาห์หยุด (ไม่มีทั้งงาน/AVA/โหลด เช่น W31) ออก — วางงานที่นั่นไม่ได้
      if (lockBefore != null) return range.filter(w => vals.has(w))
      return range
    }
    return arr.sort((a, b) => String(a).localeCompare(String(b), 'th', { numeric: true }))
  }, [rows, ci, supported, load, ava, lockBefore])

  // แถว (unique combo ของ เครื่อง/เกจ/CAT)
  const gantRows = useMemo(() => {
    if (!supported) return []
    const m = new Map()
    const add = (vals) => {
      const key = vals.join('')          // ต้องตรงกับ rowKey(row)
      if (!m.has(key)) m.set(key, { key, vals })
    }
    for (const { row } of rows) add(groups.map(g => norm(row[g.idx])))

    // เติมแถวเครื่องคู่ swap ที่ยังไม่มี (เช่น SKP20 เมื่อมี FA20) ให้เป็นที่ลากมาวางได้
    if (avaCatI >= 0 && avaGaugeI >= 0 && mcGroupI >= 0) {
      for (const { vals } of [...m.values()]) {
        const sg = SWAP_GROUPS.find(s =>
          nkey(s.cat) === nkey(vals[avaCatI]) && nkey(s.gauge) === nkey(vals[avaGaugeI]))
        if (!sg) continue
        for (const gname of sg.groups) {
          if (nkey(gname) === nkey(vals[mcGroupI])) continue
          const nv = vals.slice()
          nv[mcGroupI] = gname
          add(nv)
        }
      }
    }
    return [...m.values()].sort((a, b) =>
      a.vals.join('|').localeCompare(b.vals.join('|'), 'th', { numeric: true }))
  }, [rows, groups, supported, avaCatI, avaGaugeI, mcGroupI])

  // ค่าที่ใช้แยกสี (เรียงเพื่อ map สีคงที่)
  const colorKeys = useMemo(() => {
    if (!supported) return []
    const s = new Set()
    for (const { row } of rows) s.add(colorKey(row))
    return [...s].sort((a, b) => a.localeCompare(b, 'th', { numeric: true }))
  }, [rows, colorCols, supported])

  const colorOf = (key) => {
    const i = colorKeys.indexOf(key)
    return PALETTE[(i < 0 ? 0 : i) % PALETTE.length]
  }

  // จัดงานลงช่อง (rowKey × week)
  const cells = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const { row, idx } of rows) {
      const key = rowKey(row) + '||' + norm(row[ci.week])
      if (!m.has(key)) m.set(key, [])
      m.get(key).push({
        idx,
        item: norm(row[ci.item]),
        ck: colorKey(row),
        qty: ci.qty >= 0 ? norm(row[ci.qty]) : '',
        actualmc: ci.actualmc >= 0 ? norm(row[ci.actualmc]) : '',
      })
    }
    return m
  }, [rows, ci, groups, colorCols, supported])

  // ผลรวม NEW_MC (job = setup 1 เครื่อง) ต่อ (สัปดาห์ × ประเภท) จากแถวที่วางจริง — live ตามที่ลาก/แก้
  const planNewByWeekType = useMemo(() => {
    const m = {}
    if (!supported) return m
    for (const { row } of rows) {
      const w = norm(row[ci.week]); if (w === '') continue
      const t = classifyType(ci.factory >= 0 ? row[ci.factory] : '', ci.cat >= 0 ? row[ci.cat] : '')
      if (!t) continue
      const nm = ci.newmc >= 0 ? (Number(norm(row[ci.newmc])) || 0) : 0
      m[w + '|' + t] = (m[w + '|' + t] || 0) + nm
    }
    return m
  }, [rows, ci, supported])

  // ผลรวม ACTUAL_MC (เครื่องที่แผนใช้) ต่อ (สัปดาห์ × CAT|เกจ) — live ตามที่ลาก → ใช้คิดเครื่องว่าง
  const planMcByWeekCat = useMemo(() => {
    const m = {}
    if (!supported || ci.cat < 0 || ci.gauge < 0) return m
    for (const { row } of rows) {
      const w = norm(row[ci.week]); if (w === '') continue
      const key = norm(row[ci.cat]) + '|' + norm(row[ci.gauge])
      const mc = ci.actualmc >= 0 ? (Number(norm(row[ci.actualmc])) || 0) : 0
      m[w + '@@' + key] = (m[w + '@@' + key] || 0) + mc
    }
    return m
  }, [rows, ci, supported])

  // วัดความสูงหัวตาราง + แต่ละแถวโหลด → คำนวณ top ให้แถวโหลด sticky ค้างซ้อนใต้หัวตารางพอดี
  // (ความสูงหัวตาราง/ฟอนต์ไม่แน่นอน จึงวัดจริงแทนกำหนดตายตัว)
  const headRef = useRef(null)
  const loadRowRefs = useRef([])
  const [loadTops, setLoadTops] = useState([])
  useLayoutEffect(() => {
    const headH = headRef.current ? headRef.current.offsetHeight : 0
    const tops = []
    let acc = headH
    for (let i = 0; i < LOAD_TYPES.length; i++) {
      tops[i] = acc
      acc += loadRowRefs.current[i] ? loadRowRefs.current[i].offsetHeight : 0
    }
    setLoadTops(prev =>
      (prev.length === tops.length && prev.every((v, i) => v === tops[i])) ? prev : tops)
  }, [weeks, gantRows, groups, load])

  if (!supported) {
    return <div className="hint small">ชีทนี้ไม่มีคอลัมน์ <b>MC_GROUP</b> / <b>PLAN_WEEK</b> — Gantt แสดงได้เฉพาะชีท PLAN</div>
  }
  if (!weeks.length || !gantRows.length) {
    return <div className="hint small">ไม่มีข้อมูลสำหรับแสดง Gantt (ลองล้างตัวกรอง)</div>
  }

  function startEditQty(j, locked) {
    if (locked || !onEditQty || j.qty === '') return
    setEditIdx(j.idx)
    setEditVal(String(j.qty))
  }
  function commitQty() {
    if (editIdx == null) return
    const v = parseFloat(editVal)
    if (Number.isFinite(v) && v > 0) onEditQty(editIdx, Math.round(v * 100) / 100)
    setEditIdx(null)
  }

  function onDrop(e, week, targetRow) {
    e.preventDefault()
    setOverWeek(null)
    const idx = Number(e.dataTransfer.getData('text/plain'))
    setDragIdx(null)
    if (Number.isNaN(idx)) return
    // MC_GROUP ปลายทาง = เครื่องของแถวที่ปล่อยลง (ใช้เช็คว่าย้ายข้ามเครื่องไหม)
    const targetMc = mcGroupI >= 0 ? targetRow.vals[mcGroupI] : null
    onMoveWeek(idx, week, targetMc)
  }

  return (
    <div className="gantt">
      <div className="gantt-head">
        <span className="hint small" style={{ padding: 0 }}>
          ลากบล็อกไปคอลัมน์สัปดาห์อื่นเพื่อเปลี่ยน <b>PLAN_WEEK</b> (กลุ่มแถวคงเดิม) — ตารางด้านล่างจะอัปเดตตาม
        </span>
        {colorCols.length > 0 && (
          <div className="gantt-legend">
            <span className="glegend-title">สีตาม CAT / Guage:</span>
            {colorKeys.map(k => (
              <span key={k} className="glegend">
                <i style={{ background: colorOf(k) }} />{k}
              </span>
            ))}
          </div>
        )}
      </div>

      <div className="gantt-scroll">
        <table className="gantt-grid">
          <thead>
            <tr ref={headRef}>
              {groups.map((g, n) => (
                <th key={g.col}
                  className={'gantt-glabel gantt-ghead' + (n === groups.length - 1 ? ' gantt-glast' : '')}
                  style={{ left: g.left, width: g.width, minWidth: g.width }}>
                  {g.label}
                </th>
              ))}
              {weeks.map(w => <th key={w} className={'gantt-wk' + (isLocked(w) ? ' locked' : '')}>{isLocked(w) && '🔒'}W{w}</th>)}
            </tr>
          </thead>
          <tbody>
            {LOAD_TYPES.map((t, ti) => (
              <tr key={t.key} className="gantt-load-row" ref={el => { loadRowRefs.current[ti] = el }}>
                <th className="gantt-glabel gantt-glast" style={{ left: 0, top: loadTops[ti], zIndex: 5 }} colSpan={groups.length}>
                  โหลด {t.label}
                </th>
                {weeks.map(w => {
                  const info = (load[w] && load[w][t.key]) || {}
                  const cap = info.cap
                  const old = info.old || 0
                  // ใหม่(live) = job จาก booking (คงที่) + ผลรวม NEW_MC ของแถวที่วางจริง (ขยับตามการลาก)
                  const nw = Math.max(0, (info.bookingNew || 0) + (planNewByWeekType[w + '|' + t.key] || 0))
                  const total = old + nw
                  const hasCap = cap != null && cap !== ''
                  const over = hasCap && total > cap
                  const oldPct = hasCap && cap > 0 ? Math.min(100, (old / cap) * 100) : 0
                  const newPct = hasCap && cap > 0 ? Math.min(100 - oldPct, (nw / cap) * 100) : (nw > 0 && !hasCap ? 100 : 0)
                  const empty = old === 0 && nw === 0 && !hasCap
                  return (
                    <td key={w} className="gantt-load-cell" style={{ top: loadTops[ti] }}>
                      {empty ? <span className="loadtxt dim">–</span> : (
                        <>
                          <div className={'loadbar' + (over ? ' over' : '') + (hasCap ? '' : ' nocap')}
                            title={`${t.label} • สัปดาห์ ${w}\nเดิม ${old} + ใหม่ ${nw} = ${total}${hasCap ? ` / cap ${cap}` : ''}`}>
                            <span className="seg old" style={{ width: oldPct + '%' }} />
                            <span className="seg new" style={{ width: newPct + '%' }} />
                          </div>
                          <span className={'loadtxt' + (over ? ' over' : '')}>
                            <b className="ln-new">{nw}</b>{old ? <span className="ln-old">+{old}</span> : ''}{hasCap ? `/${cap}` : ''}
                          </span>
                        </>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
            {gantRows.map(r => (
              <tr key={r.key}>
                {groups.map((g, n) => (
                  <th key={g.col}
                    className={'gantt-glabel' + (n === groups.length - 1 ? ' gantt-glast' : '')}
                    style={{ left: g.left, width: g.width, minWidth: g.width }}>
                    {r.vals[n]}
                  </th>
                ))}
                {weeks.map(w => {
                  const jobs = cells.get(r.key + '||' + w) || []
                  const isOver = overWeek === w
                  const avaKey = avaCatI >= 0 && avaGaugeI >= 0 ? r.vals[avaCatI] + '|' + r.vals[avaGaugeI] : null
                  const av = avaKey && ava[w] ? ava[w][avaKey] : null
                  // เครื่องว่าง live = remain − (ACTUAL_MC ปัจจุบัน − ACTUAL_MC ตอนโหลด)
                  const remainLive = av
                    ? av.remain - ((planMcByWeekCat[w + '@@' + avaKey] || 0) - (av.planBase || 0))
                    : null
                  const locked = isLocked(w)
                  return (
                    <td key={w}
                      className={'gantt-cell' + (isOver ? ' over' : '') + (locked ? ' locked' : '')}
                      onDragOver={locked ? undefined : e => { e.preventDefault(); if (overWeek !== w) setOverWeek(w) }}
                      onDragLeave={locked ? undefined : () => setOverWeek(o => (o === w ? null : o))}
                      onDrop={locked ? undefined : e => onDrop(e, w, r)}>
                      {av && (() => {
                        const rsv = av.reserved || null
                        const rsvP = rsv ? (rsv.poly || 0) : 0
                        const rsvC = rsv ? (rsv.cotton || 0) : 0
                        const hasRsv = rsvP > 0 || rsvC > 0
                        const rsvTxt = [rsvP ? `POLY ${rsvP}` : '', rsvC ? `COTTON ${rsvC}` : ''].filter(Boolean).join(', ')
                        return (
                          <span className={'cellava' + (remainLive <= 0 ? ' none' : '')}
                            title={`เครื่องว่าง ${remainLive} / ปกติ ${av.total} (AVA_MC ตั้งต้น ${av.remain})`
                              + (hasRsv ? `\nกันไว้: ${rsvTxt} เครื่อง (POLY/COTTON ใช้แทนงานปกติไม่ได้)` : '')}>
                            ว่าง {remainLive}
                            {hasRsv && <span className="cellava-rsv">🔒{rsvP ? ` P${rsvP}` : ''}{rsvC ? ` C${rsvC}` : ''}</span>}
                          </span>
                        )
                      })()}
                      {jobs.map(j => {
                        const isColor = colorRows && colorRows.has(j.idx)
                        const editing = editIdx === j.idx
                        return (
                          <div key={j.idx}
                            className={'gbar' + (dragIdx === j.idx ? ' dragging' : '') + (isColor ? ' gbar-color' : '') + (locked ? ' locked' : '')}
                            draggable={!locked && !editing}
                            onDragStart={locked ? undefined : e => { e.dataTransfer.setData('text/plain', String(j.idx)); e.dataTransfer.effectAllowed = 'move'; setDragIdx(j.idx) }}
                            onDragEnd={locked ? undefined : () => { setDragIdx(null); setOverWeek(null) }}
                            onDoubleClick={() => startEditQty(j, locked)}
                            style={{ background: colorOf(j.ck) }}
                            title={`${j.item}\n${r.vals.join(' • ')} • สัปดาห์ ${w}${j.qty !== '' ? `\nจำนวน ${j.qty}` : ''}${j.actualmc !== '' ? ` • ใช้ ${j.actualmc} เครื่อง` : ''}\nสี: ${j.ck}${isColor ? '\n★ งานสี (ต้องย้อม)' : ''}${onEditQty && !locked && j.qty !== '' ? '\n✏ double click เพื่อแก้จำนวน' : ''}${locked ? '\n🔒 สัปดาห์ freeze — แก้ไม่ได้' : ''}`}>
                            {locked && <span className="gbar-star">🔒</span>}
                            {isColor && !locked && <span className="gbar-star">★</span>}
                            <span className="gbar-item">{j.item}</span>
                            {editing ? (
                              <input className="gbar-qty-edit" type="number" step="any" autoFocus
                                value={editVal}
                                onChange={e => setEditVal(e.target.value)}
                                onMouseDown={e => e.stopPropagation()}
                                onDoubleClick={e => e.stopPropagation()}
                                onKeyDown={e => { if (e.key === 'Enter') commitQty(); else if (e.key === 'Escape') setEditIdx(null) }}
                                onBlur={commitQty} />
                            ) : (
                              j.qty !== '' && <span className="gbar-qty">{j.qty}</span>
                            )}
                            {onRemove && !locked && (
                              <button className="gbar-del" title="เอางานนี้ออกจากแผน"
                                onMouseDown={e => e.stopPropagation()}
                                onClick={e => { e.stopPropagation(); onRemove(j.idx) }}>✕</button>
                            )}
                          </div>
                        )
                      })}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

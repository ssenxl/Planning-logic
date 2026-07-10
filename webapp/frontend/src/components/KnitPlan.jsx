import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { ColumnFilter, columnFilterData, filterRows, norm } from './ColumnFilter.jsx'
import PlanGantt from './PlanGantt.jsx'
import OutsourceAdvisor from './OutsourceAdvisor.jsx'

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function KnitPlan() {
  const [meta, setMeta] = useState(null)         // ข้อมูลไฟล์แผนล่าสุด
  const [grid, setGrid] = useState(null)          // { sheet, sheets, columns, rows, name, mtime }
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [dirty, setDirty] = useState(false)
  const [msg, setMsg] = useState('')
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})
  const [openCol, setOpenCol] = useState(null)
  const [runStatus, setRunStatus] = useState({})
  const [showGantt, setShowGantt] = useState(true)
  const [showOutsource, setShowOutsource] = useState(false)
  const [load, setLoad] = useState({})
  const [ava, setAva] = useState({})
  // เครื่องที่ booking ถักไอเทมนั้นอยู่แล้ว ต่อ (สัปดาห์ × ITEM|MC_GROUP|GUAGE)
  const [bookingMc, setBookingMc] = useState({})
  // วันทำงานตามปฏิทินต่อสัปดาห์ — ใช้คำนวณเครื่องใหม่เมื่อลากงานข้ามสัปดาห์
  const [weekDays, setWeekDays] = useState({})
  const prevRunning = useRef(false)

  async function loadMeta() {
    const m = await api.planLatest()
    setMeta(m)
    return m
  }
  async function loadRunStatus() {
    try { setRunStatus(await api.runStatus()) } catch { }
  }
  async function loadLoad() {
    try { setLoad(await api.planLoad()) } catch { setLoad({}) }
  }
  async function loadAva() {
    try { setAva(await api.planAva()) } catch { setAva({}) }
    try { setBookingMc(await api.planBookingMc()) } catch { setBookingMc({}) }
    try { setWeekDays(await api.planWeekDays()) } catch { setWeekDays({}) }
  }
  async function loadSheet(sheet) {
    setLoading(true); setMsg('')
    setSearch(''); setFilters({}); setOpenCol(null)
    try {
      const d = await api.planSheet(sheet)
      setGrid(d)
      setDirty(false)
    } catch (e) {
      setGrid(null)
      setMsg('อ่านแผนไม่ได้: ' + e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    (async () => {
      try {
        const m = await loadMeta()
        if (m.exists) await loadSheet()
      } catch (e) { setMsg('โหลดแผนไม่ได้: ' + e.message) }
    })()
    loadRunStatus()
    loadLoad()
    loadAva()
    const t = setInterval(loadRunStatus, 2000)
    return () => clearInterval(t)
  }, [])

  // เมื่อรัน (โหมด plan) เสร็จ → โหลดแผนล่าสุดใหม่อัตโนมัติ
  useEffect(() => {
    if (prevRunning.current && !runStatus.running) {
      (async () => {
        try {
          const m = await loadMeta()
          if (m.exists) await loadSheet()
          loadLoad()
          loadAva()
        } catch { }
      })()
    }
    prevRunning.current = !!runStatus.running
  }, [runStatus.running])

  async function refresh() {
    setMsg('')
    try {
      const m = await loadMeta()
      await loadRunStatus()
      loadLoad()
      loadAva()
      if (m.exists) await loadSheet(grid?.sheet)
      else { setGrid(null); setMsg('ยังไม่มีไฟล์แผน — กดรันแผนก่อน') }
    } catch (e) { setMsg('รีเฟรชไม่ได้: ' + e.message) }
  }

  async function runPlan() {
    setMsg('')
    try {
      const r = await api.run('plan')
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) { setMsg('สั่งรันไม่ได้: ' + e.message) }
  }

  async function changeSheet(sheet) {
    if (dirty && !window.confirm('มีการแก้ไขที่ยังไม่บันทึก จะทิ้งแล้วเปลี่ยนชีทไหม?')) return
    await loadSheet(sheet)
  }

  async function save() {
    if (!grid) return
    setSaving(true); setMsg('')
    try {
      const r = await api.planSave(grid.sheet, grid.columns, grid.rows)
      setMsg(`บันทึกแล้ว (${r.rows} แถว) — สำรองไฟล์เดิมเป็น ${r.backup}`)
      setDirty(false)
      loadMeta().catch(() => { })
    } catch (e) { setMsg('บันทึกไม่ได้: ' + e.message) }
    finally { setSaving(false) }
  }

  function setCell(ri, ci, val) {
    setGrid(g => {
      const rows = g.rows.slice()
      const row = rows[ri].slice()
      row[ci] = val
      rows[ri] = row
      return { ...g, rows }
    })
    setDirty(true)
  }
  function delRow(ri) {
    setGrid(g => ({ ...g, rows: g.rows.filter((_, i) => i !== ri) }))
    setDirty(true)
  }
  // วันทำงานจริงของแถวในสัปดาห์ w — ตรงกับ get_working_days_by_factory() ใน Planning.py
  //   w == 17        → 8 วัน
  //   w == 32        → WD_W32 (ต่อเครื่อง จาก REMARK ใน MasterMC)
  //   นอกนั้น        → max(1, WD_BASE − max(0, 6 − วันทำงานตามปฏิทินของ w))
  function actualWdAt(gv, w) {
    const cal = Number(weekDays?.[String(w)]) || 0
    if (!gv('WD_BASE') || !cal) return null   // ไฟล์แผนเก่าไม่มีข้อมูล
    if (w === 17) return 8
    if (w === 32) return gv('WD_W32')
    return Math.max(1, gv('WD_BASE') - Math.max(0, 6 - cal))
  }

  // กำลังผลิตของแถวถ้าอยู่สัปดาห์ w (กก.) โดยใช้เครื่องชุดเดิมที่แถวถืออยู่
  //   เครื่อง carry ได้วันทำงานเต็ม / เครื่อง setup ใหม่ได้ (วันทำงาน − setup ต่อเครื่อง)
  // ⚠️ ห้ามคำนวณ ACTUAL_MC ใหม่จาก PRODUCE_QTY — เครื่อง carry ถูกกำหนดโดยสัปดาห์ก่อนหน้า
  //    และอาจถือเครื่องเกินที่งานต้องการ ตัวเลขที่ถูกต้องต้องรันแผนใหม่เท่านั้น
  function capacityAt(gv, w) {
    const awd = actualWdAt(gv, w)
    if (awd == null) return null
    const n = gv('NEW_MC'), c = gv('CARRYOVER_MC')
    const setupPerMc = n > 0 ? gv('SETUP_DAYS') / n : 0
    const mcDays = c * awd + n * Math.max(0.5, awd - setupPerMc)
    return mcDays * gv('DAILY_CAPACITY')
  }

  // อัปเดตคอลัมน์ที่เป็นฟังก์ชันของสัปดาห์ล้วนๆ (คำนวณได้แม่นยำ) หลังย้ายงาน
  function recalcRowForWeek(row, cols, week) {
    const ix = (n) => cols.indexOf(n)
    const gv = (n) => { const k = ix(n); return k >= 0 ? (Number(norm(row[k])) || 0) : 0 }
    const set = (n, v) => { const k = ix(n); if (k >= 0) row[k] = v }

    const w = Number(week)
    const awd = actualWdAt(gv, w)
    if (awd == null) return
    const n = gv('NEW_MC')
    const avail = n > 0 ? Math.max(0.5, awd - gv('SETUP_DAYS') / n) : awd

    set('CALENDAR_WORKING_DAYS', Number(weekDays[String(w)]) || 0)
    set('FACTORY_WORKING_DAYS', w === 17 ? 8 : w === 32 ? gv('WD_W32') : gv('WD_BASE'))
    set('ACTUAL_WORKING_DAYS', awd)
    set('AVAILABLE_DAYS', avail)

    // เครื่อง booking ผูกกับสัปดาห์ ไม่ย้ายตามงาน → อ่านค่าของสัปดาห์ปลายทาง
    // ⚠️ ถ้ายังไม่มีข้อมูล booking (API ล้ม / ไฟล์แผนเก่า) ห้ามเขียนทับเป็น 0
    //    มิฉะนั้นแถวที่นั่งบนเครื่อง booking จะกลายเป็น carry ทันทีที่ลาก
    const ici = ix('ITEM_CODE'), mci = ix('MC_GROUP'), gci = ix('MC_GUAGE')
    if (ici >= 0 && mci >= 0 && gci >= 0 && Object.keys(bookingMc || {}).length > 0) {
      const gz = norm(row[gci]).replace(/\.0$/, '')
      const k = `${norm(row[ici]).toUpperCase()}|${norm(row[mci]).toUpperCase()}|${gz}`
      set('MC_BOOKING', Number(bookingMc[String(w)]?.[k]) || 0)
    }
  }

  // ลากบล็อกใน Gantt → เปลี่ยน PLAN_WEEK (+ MC_GROUP ถ้าย้ายข้ามเครื่อง) ของงานนั้น
  // ย้ายข้ามเครื่อง (เช่น FA↔SKP) ต้องยืนยันก่อน
  function moveJob(ri, week, mcGroup) {
    if (!grid) return
    const wci = grid.columns.indexOf('PLAN_WEEK')
    const mci = grid.columns.indexOf('MC_GROUP')
    const ici = grid.columns.indexOf('ITEM_CODE')
    const item = ici >= 0 ? norm(grid.rows[ri][ici]) : ''
    const curMc = mci >= 0 ? norm(grid.rows[ri][mci]) : ''
    const crossMc = mcGroup && mci >= 0 && String(mcGroup).trim().toUpperCase() !== curMc.trim().toUpperCase()
    if (crossMc && !window.confirm(`ยืนยันย้ายงาน ${item} จากเครื่อง ${curMc} → ${mcGroup} (สัปดาห์ ${week})?`)) return

    // วันทำงานแต่ละสัปดาห์ไม่เท่ากัน (W32 = 10 วัน, W33 = 5 วัน) → เครื่องชุดเดิมอาจผลิตไม่ทัน
    const oldRow = grid.rows[ri]
    const gvOld = (n) => { const k = grid.columns.indexOf(n); return k >= 0 ? (Number(norm(oldRow[k])) || 0) : 0 }
    const sharedRow = gvOld('MC_SHARED') > 0
    const cap = sharedRow ? null : capacityAt(gvOld, Number(week))
    const need = gvOld('PRODUCE_QTY')
    if (cap != null && need > 0 && cap + 1e-6 < need) {
      const msg = `${item} → สัปดาห์ ${week}: เครื่องที่ถืออยู่ (carry ${gvOld('CARRYOVER_MC')} + ใหม่ ${gvOld('NEW_MC')}) `
        + `ผลิตได้แค่ ${Math.round(cap).toLocaleString()} กก. แต่ต้องผลิต ${Math.round(need).toLocaleString()} กก.\n\n`
        + `สัปดาห์ปลายทางมีวันทำงานน้อยกว่า — ระบบไม่คำนวณจำนวนเครื่องใหม่ให้ (ต้องรันแผนใหม่)\n\n`
        + `ยืนยันย้ายหรือไม่?`
      if (!window.confirm(msg)) return
    }

    setGrid(g => {
      const rows = g.rows.slice()
      const row = rows[ri].slice()
      const oldWeek = wci >= 0 ? norm(row[wci]) : ''
      if (wci >= 0) row[wci] = week
      if (crossMc) row[mci] = mcGroup
      if (String(oldWeek) !== String(week) || crossMc) recalcRowForWeek(row, g.columns, week)
      rows[ri] = row
      return { ...g, rows }
    })
    setDirty(true)
  }

  // double click ตัวเลขบนบล็อก Gantt → แก้ PRODUCE_QTY ของแถวนั้น (ตารางด้านล่างอัปเดตตาม + ต้องกดบันทึก)
  function editQty(ri, qty) {
    if (!grid) return
    const qci = grid.columns.indexOf('PRODUCE_QTY')
    if (qci >= 0) setCell(ri, qci, qty)
  }

  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  const visible = useMemo(() => grid ? filterRows(grid, search, filters) : [], [grid, search, filters])

  const ganttReady = !!grid && grid.columns.includes('MC_GROUP') && grid.columns.includes('PLAN_WEEK')
  const hasFilter = search.trim() || Object.keys(filters).length
  const isRunning = !!runStatus.running
  const runLabel = isRunning ? `กำลังรัน: ${runStatus.label || '-'}` : 'ยังไม่มีงานกำลังรัน'

  function openColMenu(e, ci) {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setOpenCol({ ci, anchor: { top: r.top, bottom: r.bottom, left: r.left, right: r.right } })
  }
  function applyFilter(ci, set) {
    setFilters(f => {
      const n = { ...f }
      if (set == null) delete n[ci]
      else n[ci] = set
      return n
    })
    setOpenCol(null)
  }

  return (
    <div className="knitplan">
      <div className="editbar">
        <div>
          <h2>แผนผลิต {dirty && <span className="dot">●</span>}</h2>
          <div className="data-selected-meta">
            {meta?.exists
              ? `${meta.name} • อัปเดตล่าสุด ${fmtTime(meta.mtime)} • ${fmtSize(meta.size)}`
              : 'ยังไม่มีไฟล์แผน — กดรันแผนเพื่อสร้าง'}
          </div>
        </div>
        <div className="actions">
          {grid && grid.sheets && grid.sheets.length > 1 && (
            <select value={grid.sheet} onChange={e => changeSheet(e.target.value)}>
              {grid.sheets.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          )}
          <button className="primary" onClick={runPlan} disabled={isRunning}>▶ รันแผนใหม่</button>
          <button className="outsource-btn" onClick={() => setShowOutsource(true)}>🧵 จ้างทอ (AI)</button>
          <button onClick={save} disabled={!grid || saving || !dirty}>
            {saving ? 'กำลังบันทึก...' : '💾 บันทึก'}
          </button>
          {meta?.exists && <a className="dl" href={api.planDownloadUrl()}>⬇ ดาวน์โหลด Excel</a>}
          <button onClick={refresh}>รีเฟรช</button>
        </div>
      </div>

      <div className="map-runbox">
        <span className={'badge ' + (isRunning ? 'run' : 'idle')}>{runLabel}</span>
        {isRunning && runStatus.progress != null && <small>ความคืบหน้า {runStatus.progress}%</small>}
        <small>ปุ่ม <b>รันแผนใหม่</b> จะรัน pipeline ตั้งแต่ AVA_MC → Planning แล้วโหลดแผนล่าสุดให้อัตโนมัติ</small>
      </div>

      {grid && (
        <div className="filterbar">
          <input className="search" placeholder="🔍 ค้นหาทุกคอลัมน์..."
            value={search} onChange={e => setSearch(e.target.value)} />
          <span className="count">แสดง {visible.length} / {grid.rows.length} แถว</span>
          {hasFilter ? (
            <button className="clearf" onClick={() => { setSearch(''); setFilters({}) }}>ล้างตัวกรองทั้งหมด</button>
          ) : <span className="hint small" style={{ padding: 0 }}>กด ▾ ที่หัวคอลัมน์เพื่อกรอง</span>}
        </div>
      )}

      {ganttReady && !loading && (
        <div className="gantt-section">
          <div className="gantt-bar">
            <b>Gantt แผนผลิต (เครื่อง × สัปดาห์)</b>
            <button onClick={() => setShowGantt(s => !s)}>{showGantt ? 'ซ่อน' : 'แสดง'}</button>
          </div>
          {showGantt && <PlanGantt columns={grid.columns} rows={visible} load={load} ava={ava} bookingMc={bookingMc} onMoveWeek={moveJob} onEditQty={editQty} />}
        </div>
      )}

      {msg && <div className="msg">{msg}</div>}
      {loading && <div className="hint">กำลังโหลด...</div>}
      {!loading && !meta?.exists && <div className="hint">ยังไม่มีไฟล์แผน กด <b>รันแผนใหม่</b> แล้วรอให้สถานะจบก่อน ระบบจะโหลดให้อัตโนมัติ</div>}

      {grid && !loading && (
        <div className="gridwrap">
          <table className="grid">
            <thead>
              <tr>
                <th className="rownum">#</th>
                {grid.columns.map((c, ci) => (
                  <th key={ci}>
                    <div className="thcell">
                      <span className="thlabel" title={c}>{c}</span>
                      <button
                        className={'funnel' + (filters[ci] ? ' on' : '')}
                        title="กรองคอลัมน์นี้"
                        onClick={e => openColMenu(e, ci)}>▾</button>
                    </div>
                  </th>
                ))}
                <th className="rownum"></th>
              </tr>
            </thead>
            <tbody>
              {visible.map(({ row, idx }) => (
                <tr key={idx}>
                  <td className="rownum">{idx + 1}</td>
                  {row.map((cell, ci) => (
                    <td key={ci}>
                      <input value={norm(cell)} onChange={e => setCell(idx, ci, e.target.value)} />
                    </td>
                  ))}
                  <td className="rownum">
                    <button className="del" title="ลบแถว" onClick={() => delRow(idx)}>✕</button>
                  </td>
                </tr>
              ))}
              {!visible.length && (
                <tr><td className="rownum"></td>
                  <td colSpan={grid.columns.length + 1} className="hint">ไม่มีแถวตรงตัวกรอง</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

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

      {showOutsource && (
        <div className="modal-backdrop" onClick={() => setShowOutsource(false)}>
          <div className="modal-box" onClick={e => e.stopPropagation()}>
            <button className="modal-close" title="ปิด" onClick={() => setShowOutsource(false)}>✕</button>
            <OutsourceAdvisor />
          </div>
        </div>
      )}
    </div>
  )
}

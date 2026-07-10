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
  // ลากบล็อกใน Gantt → เปลี่ยน PLAN_WEEK (+ MC_GROUP ถ้าย้ายข้ามเครื่อง) ของงานนั้น
  // ย้ายข้ามเครื่อง (เช่น FA↔SKP) ต้องยืนยันก่อน
  function moveJob(ri, week, mcGroup) {
    if (!grid) return
    const wci = grid.columns.indexOf('PLAN_WEEK')
    const mci = grid.columns.indexOf('MC_GROUP')
    const ici = grid.columns.indexOf('ITEM_CODE')
    const curMc = mci >= 0 ? norm(grid.rows[ri][mci]) : ''
    const crossMc = mcGroup && mci >= 0 && String(mcGroup).trim().toUpperCase() !== curMc.trim().toUpperCase()
    if (crossMc) {
      const item = ici >= 0 ? norm(grid.rows[ri][ici]) : ''
      if (!window.confirm(`ยืนยันย้ายงาน ${item} จากเครื่อง ${curMc} → ${mcGroup} (สัปดาห์ ${week})?`)) return
    }
    setGrid(g => {
      const rows = g.rows.slice()
      const row = rows[ri].slice()
      if (wci >= 0) row[wci] = week
      if (crossMc) row[mci] = mcGroup
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

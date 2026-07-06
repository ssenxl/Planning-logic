import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { ColumnFilter, columnFilterData, filterRows, norm } from './ColumnFilter.jsx'

// คอลัมน์ที่ไม่แสดงบนหน้าเว็บ (ข้อมูลในไฟล์ยังอยู่ครบ)
const HIDDEN_COLS = new Set(['BK_ORDER_WEIGHT'])

// แสดงตัวเลขทศนิยมไม่เกิน 2 ตำแหน่ง (จำนวนเต็ม/ข้อความคงเดิม)
const fmtCell = (v) => (typeof v === 'number' && !Number.isInteger(v))
  ? String(Math.round(v * 100) / 100)
  : norm(v)

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function MapItem({ embedded = false }) {
  const [files, setFiles] = useState([])
  const [sel, setSel] = useState(null)
  const [grid, setGrid] = useState(null)
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})
  const [openCol, setOpenCol] = useState(null)
  const [runStatus, setRunStatus] = useState({})
  const prevRunning = useRef(false)

  function findFile(list, fileId) {
    return list.find(f => f.id === fileId) || null
  }

  async function loadList() {
    const data = await api.mapItemFiles()
    setFiles(data)
    return data
  }

  async function loadRunStatus() {
    try { setRunStatus(await api.runStatus()) } catch { }
  }

  useEffect(() => {
    loadList().catch(e => setMsg('โหลดรายการไม่ได้: ' + e.message))
    loadRunStatus()
    const t = setInterval(loadRunStatus, 2000)
    return () => clearInterval(t)
  }, [])

  useEffect(() => {
    if (prevRunning.current && !runStatus.running) {
      refresh()
    }
    prevRunning.current = !!runStatus.running
  }, [runStatus.running])

  async function openFile(file, sheet) {
    if (!file?.exists) return
    setLoading(true)
    setMsg('')
    setGrid(null)
    setSel(file)
    setSearch('')
    setFilters({})
    setOpenCol(null)
    try {
      const d = await api.mapItemSheet(file.id, sheet)
      setGrid(d)
    } catch (e) {
      setMsg('อ่านไฟล์ไม่ได้: ' + e.message)
    } finally {
      setLoading(false)
    }
  }

  async function changeSheet(sheet) {
    if (!sel) return
    setLoading(true)
    setMsg('')
    setSearch('')
    setFilters({})
    setOpenCol(null)
    try {
      const d = await api.mapItemSheet(sel.id, sheet)
      setGrid(d)
    } catch (e) {
      setMsg('อ่านชีทไม่ได้: ' + e.message)
    } finally {
      setLoading(false)
    }
  }

  async function refresh() {
    setMsg('')
    try {
      const freshFiles = await loadList()
      await loadRunStatus()
      if (sel) {
        const freshSel = findFile(freshFiles, sel.id)
        setSel(freshSel)
        if (!freshSel?.exists) {
          setGrid(null)
          setMsg('ยังไม่พบไฟล์ที่เลือก กรุณารัน Map Item ก่อน')
          return
        }
      }
      if (sel && grid) {
        setLoading(true)
        try {
          setGrid(await api.mapItemSheet(sel.id, grid.sheet))
        } catch (e) {
          setMsg('รีเฟรชไม่ได้: ' + e.message)
        } finally {
          setLoading(false)
        }
      }
    } catch (e) {
      setMsg('รีเฟรชไม่ได้: ' + e.message)
    }
  }

  async function runMapItem() {
    setMsg('')
    try {
      const r = await api.run('map-item')
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) {
      setMsg('สั่งรันไม่ได้: ' + e.message)
    }
  }

  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  const visible = useMemo(() => grid ? filterRows(grid, search, filters) : [], [grid, search, filters])

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
    <div className="data-page map-item-page">
      <aside className="data-sidebar">
        <div className="data-sidehead">
          <div>
            <h2>{embedded ? 'DATA / Map Item' : 'Map Item'}</h2>
            <p>ดูผลลัพธ์การเชื่อม item ระหว่าง Datamining, ORA Item และ Booking</p>
          </div>
          <div className="data-sideactions">
            <button className="primary" onClick={runMapItem} disabled={isRunning}>รัน Map Item</button>
            <button onClick={refresh}>รีเฟรช</button>
          </div>
        </div>

        <div className="map-help">
          <b>ไฟล์ในหน้านี้</b>
          <ul>
            <li><b>datamining_mapped</b> = ITEM จาก datamining + ORA_ITEM_CODE</li>
            <li><b>datamining_booking_mapped</b> = ITEM จาก datamining + booking รายสัปดาห์</li>
          </ul>
        </div>

        <div className="map-runbox">
          <span className={'badge ' + (isRunning ? 'run' : 'idle')}>{runLabel}</span>
          <small>ปุ่มนี้จะรันไฟล์ <b>MapItem.py</b> เพื่อสร้าง/อัปเดตผลลัพธ์ map</small>
        </div>

        <div className="data-note">
          ถ้ายังไม่พบไฟล์ ให้กด <b>รัน Map Item</b> แล้วรอให้สถานะจบก่อน ระบบจะรีเฟรชรายการให้อัตโนมัติ
        </div>

        <div className="data-groups">
          {files.map(f => {
            const active = sel && sel.id === f.id
            return (
              <button key={f.id}
                className={'datafilebtn' + (active ? ' active' : '') + (!f.exists ? ' missing' : '')}
                onClick={() => openFile(f)}
                disabled={!f.exists}>
                <span className="datafile-name">{f.label}</span>
                <span className="datafile-meta">
                  {f.exists
                    ? `${f.name} • โหลด ${fmtTime(f.mtime)} • ${fmtSize(f.size)}`
                    : `${f.name} • ยังไม่พบไฟล์`}
                </span>
              </button>
            )
          })}
        </div>
      </aside>

      <section className="editor">
        {msg && !sel && <div className="msg">{msg}</div>}
        {!sel && <div className="hint">เลือกไฟล์ด้านซ้ายเพื่อดูผลการ map item</div>}

        {sel && (
          <>
            <div className="editbar">
              <div>
                <h2>{sel.label}</h2>
                <div className="data-selected-meta">
                  {sel.desc} {sel.exists ? `• อัปเดตล่าสุด ${fmtTime(sel.mtime)} • ขนาด ${fmtSize(sel.size)}` : ''}
                </div>
              </div>
              <div className="actions">
                {grid && grid.sheets && grid.sheets.length > 1 && (
                  <select value={grid.sheet} onChange={e => changeSheet(e.target.value)}>
                    {grid.sheets.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                )}
                {sel.exists && <a className="dl" href={api.mapItemDownloadUrl(sel.id)}>⬇ ดาวน์โหลด</a>}
              </div>
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

            {grid && grid.truncated && (
              <div className="msg note">
                ℹ️ ไฟล์มี {grid.total.toLocaleString()} แถว — แสดงเฉพาะ {grid.rows.length.toLocaleString()} แถวแรกเพื่อความเร็ว กด<b>ดาวน์โหลด</b>เพื่อดูข้อมูลครบทุกแถว
              </div>
            )}
            {msg && <div className="msg">{msg}</div>}
            {loading && <div className="hint">กำลังโหลด...</div>}

            {grid && !loading && (
              <div className="gridwrap">
                <table className="grid">
                  <thead>
                    <tr>
                      <th className="rownum">#</th>
                      {grid.columns.map((c, ci) => HIDDEN_COLS.has(c) ? null : (
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
                    </tr>
                  </thead>
                  <tbody>
                    {visible.map(({ row, idx }) => (
                      <tr key={idx}>
                        <td className="rownum">{idx + 1}</td>
                        {grid.columns.map((c, ci) => HIDDEN_COLS.has(c) ? null : (
                          <td key={ci} className="rocell">{fmtCell(row[ci])}</td>
                        ))}
                      </tr>
                    ))}
                    {!visible.length && (
                      <tr><td className="rownum"></td>
                        <td colSpan={grid.columns.filter(c => !HIDDEN_COLS.has(c)).length} className="hint">ไม่มีแถวตรงตัวกรอง</td>
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
          </>
        )}
      </section>
    </div>
  )
}

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import MapItem from './MapItem.jsx'
import Substitute from './Substitute.jsx'
import {
  ColumnFilter, columnFilterData, filterRows, norm,
  ROWNUM_W, isIdName, numericCols, fmtNum, autoColWidths, makeColResizer,
} from './ColumnFilter.jsx'

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}
function fmtIso(iso) {
  if (!iso) return '-'
  return new Date(iso).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}
function fmtClock(hour, minute) {
  const h = String(hour ?? 0).padStart(2, '0')
  const m = String(minute ?? 0).padStart(2, '0')
  return `${h}:${m}`
}

export default function Data() {
  const [view, setView] = useState('source')
  const [groups, setGroups] = useState([])
  const [sched, setSched] = useState(null)
  const [runStatus, setRunStatus] = useState({})
  const [sel, setSel] = useState(null)
  const [grid, setGrid] = useState(null)
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})
  const [openCol, setOpenCol] = useState(null)
  const [colW, setColW] = useState({})
  const prevRunning = useRef(false)

  function withGroupMeta(list) {
    return list.map(g => ({
      ...g,
      latestMtime: g.files.length ? Math.max(...g.files.map(f => f.mtime)) : null,
      files: g.files.map(f => ({ ...f, groupLabel: g.label })),
    }))
  }

  function findFile(list, fileId) {
    for (const g of list) {
      const found = g.files.find(f => f.id === fileId)
      if (found) return found
    }
    return null
  }

  async function loadList() {
    const data = withGroupMeta(await api.database())
    setGroups(data)
    return data
  }

  async function loadSchedule() {
    try { setSched(await api.schedule()) } catch { }
  }

  async function loadRunStatus() {
    try { setRunStatus(await api.runStatus()) } catch { }
  }

  useEffect(() => {
    Promise.all([loadList(), loadSchedule(), loadRunStatus()])
      .catch(e => setMsg('โหลดข้อมูลไม่ได้: ' + e.message))

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
    setLoading(true)
    setMsg('')
    setGrid(null)
    setSel(file)
    setSearch('')
    setFilters({})
    setOpenCol(null)
    try {
      const d = await api.databaseSheet(file.id, sheet)
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
      const d = await api.databaseSheet(sel.id, sheet)
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
      const freshGroups = await loadList()
      await Promise.all([loadSchedule(), loadRunStatus()])
      if (sel) {
        const freshSel = findFile(freshGroups, sel.id)
        if (freshSel) setSel(freshSel)
      }
      if (sel && grid) {
        setLoading(true)
        try {
          setGrid(await api.databaseSheet(sel.id, grid.sheet))
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

  async function runDbPull() {
    setMsg('')
    try {
      const r = await api.run('db')
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) {
      setMsg('สั่งดึงข้อมูลไม่ได้: ' + e.message)
    }
  }

  // หยุดงานที่กำลังรัน — backend จะ kill ทั้ง process tree (run_all.py + step ลูก)
  async function stopRun() {
    if (!window.confirm('ยืนยันหยุดการรัน? งานที่กำลังทำอยู่จะถูกยกเลิกทันที')) return
    setMsg('')
    try {
      const r = await api.runStop()
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) {
      setMsg('สั่งหยุดไม่ได้: ' + e.message)
    }
  }

  async function runGroup(groupId) {
    setMsg('')
    try {
      const r = await api.run(groupId)
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) {
      setMsg('สั่งดึงข้อมูลไม่ได้: ' + e.message)
    }
  }

  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  const visible = useMemo(() => grid ? filterRows(grid, search, filters) : [], [grid, search, filters])

  // คอลัมน์ที่ค่าไม่ว่างทุกช่องเป็นตัวเลข → ชิดขวา (num); ถ้าไม่ใช่รหัส/เลขที่ ก็ใส่ comma
  const numCols = useMemo(() => numericCols(grid), [grid])

  function cellText(v, ci) {
    const s = norm(v)
    if (s === '' || !numCols.has(ci)) return s
    if (isIdName(grid.columns[ci])) return s
    return fmtNum(s)
  }

  // ความกว้างเริ่มต้นของแต่ละคอลัมน์ — คำนวณอัตโนมัติจากหัว + เนื้อหา (ผู้ใช้ลากปรับได้)
  useEffect(() => { setColW(autoColWidths(grid)) }, [grid])

  const startResize = makeColResizer(colW, setColW)

  const totalW = grid
    ? ROWNUM_W + grid.columns.reduce((s, _, ci) => s + (colW[ci] || 120), 0)
    : 0

  const hasFilter = search.trim() || Object.keys(filters).length
  const autoCfg = sched?.full
  const isRunning = !!runStatus.running
  const runLabel = isRunning ? `กำลังรัน: ${runStatus.label || '-'}` : 'ยังไม่มีงานกำลังรัน'
  const autoLoadLabel = sched == null
    ? 'กำลังโหลดเวลาตั้งอัตโนมัติ...'
    : autoCfg?.enabled === false
      ? 'ปิดการโหลดอัตโนมัติอยู่'
      : `ตั้งโหลดทุกวันเวลา ${fmtClock(autoCfg?.hour, autoCfg?.minute)} น.`
  const nextRunLabel = sched == null
    ? 'กำลังโหลด...'
    : autoCfg?.enabled === false ? 'ปิดอยู่' : fmtIso(autoCfg?.next_run)

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
    <div className="data-stack">
      <div className="data-viewtabs">
        <button
          className={'data-viewtab' + (view === 'source' ? ' active' : '')}
          onClick={() => setView('source')}>
          ข้อมูลต้นทาง
        </button>
        <button
          className={'data-viewtab' + (view === 'map-item' ? ' active' : '')}
          onClick={() => setView('map-item')}>
          Map Item
        </button>
        <button
          className={'data-viewtab' + (view === 'substitute' ? ' active' : '')}
          onClick={() => setView('substitute')}>
          Item ทดแทน
        </button>
      </div>

      {view === 'map-item' ? (
        <MapItem embedded />
      ) : view === 'substitute' ? (
        <Substitute />
      ) : (
        <div className="data-page">
          <aside className="data-sidebar">
            <div className="data-sidehead">
              <div>
                <h2>DATA</h2>
                <p>ดูข้อมูลที่ระบบใช้จริง พร้อมเวลาโหลดล่าสุดของแต่ละชุดข้อมูล</p>
              </div>
              <div className="data-sideactions">
                <button className="primary" onClick={runDbPull} disabled={isRunning}>ดึงข้อมูลล่าสุด</button>
                <button onClick={refresh}>รีเฟรช</button>
              </div>
            </div>

            <div className="data-autoload">
              <span className="data-chip">AUTO LOAD</span>
              <b>{autoLoadLabel}</b>
              <small>รอบถัดไป: {nextRunLabel}</small>
            </div>

            <div className="map-runbox">
              <span className={'badge ' + (isRunning ? 'run' : 'idle')}>{runLabel}</span>
              {isRunning && <button className="stopbtn" onClick={stopRun}>⛔ หยุดรัน</button>}
              <small>ผู้ใช้สามารถกด <b>ดึงข้อมูลล่าสุด</b> เพื่อให้ระบบรันขั้นดึงข้อมูลจาก DB ได้ทันที</small>
            </div>

            <div className="data-note">
              เวลาโหลดอ้างอิงจากเวลาแก้ไขไฟล์ล่าสุดของข้อมูลที่ pipeline ใช้งาน
            </div>

            <div className="data-groups">
              {groups.map(g => (
                <details key={g.id} className="datagroup" open>
                  <summary>
                    <div className="datagroup-title">
                      <b>{g.label}</b>
                      <small>{g.files.length ? `${g.files.length} ไฟล์` : 'ไม่มีไฟล์'}</small>
                    </div>
                    <span className="datagroup-meta">
                      {g.latestMtime ? `โหลดล่าสุด ${fmtTime(g.latestMtime)}` : 'ยังไม่มีข้อมูล'}
                    </span>
                    <button
                      className="groupfetch"
                      disabled={isRunning}
                      onClick={e => { e.preventDefault(); runGroup(g.id) }}>
                      ดึงเฉพาะ {g.label}
                    </button>
                  </summary>

                  <div className="datafiles">
                    {!g.files.length && <div className="hint small">ไม่มีไฟล์ในกลุ่มนี้</div>}
                    {g.files.map(f => {
                      const active = sel && sel.id === f.id
                      return (
                        <button key={f.id}
                          className={'datafilebtn' + (active ? ' active' : '')}
                          onClick={() => openFile(f)}>
                          <span className="datafile-name">{f.name}</span>
                          <span className="datafile-meta">โหลด {fmtTime(f.mtime)} • {fmtSize(f.size)}</span>
                        </button>
                      )
                    })}
                  </div>
                </details>
              ))}
            </div>
          </aside>

          <section className="editor">
            {msg && !sel && <div className="msg">{msg}</div>}
            {!sel && <div className="hint">เลือกไฟล์จาก dropdown ด้านซ้ายเพื่อดูข้อมูลที่ระบบใช้งาน</div>}

            {sel && (
              <>
                <div className="editbar">
                  <div>
                    <h2>{sel.name}</h2>
                    <div className="data-selected-meta">
                      กลุ่ม {sel.groupLabel} • โหลดล่าสุด {fmtTime(sel.mtime)} • ขนาด {fmtSize(sel.size)}
                    </div>
                  </div>
                  <div className="actions">
                    {grid && grid.sheets && grid.sheets.length > 1 && (
                      <select value={grid.sheet} onChange={e => changeSheet(e.target.value)}>
                        {grid.sheets.map(s => <option key={s} value={s}>{s}</option>)}
                      </select>
                    )}
                    <a className="dl" href={api.databaseDownloadUrl(sel.id)}>⬇ ดาวน์โหลด</a>
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
                    <table className="grid" style={{ tableLayout: 'fixed', width: totalW }}>
                      <colgroup>
                        <col style={{ width: ROWNUM_W }} />
                        {grid.columns.map((_, ci) => (
                          <col key={ci} style={{ width: colW[ci] || 120 }} />
                        ))}
                      </colgroup>
                      <thead>
                        <tr>
                          <th className="rownum">#</th>
                          {grid.columns.map((c, ci) => (
                            <th key={ci} className={ci === 0 ? 'frozen' : undefined}>
                              <div className="thcell">
                                <span className="thlabel" title={c}>{c}</span>
                                <button
                                  className={'funnel' + (filters[ci] ? ' on' : '')}
                                  title="กรองคอลัมน์นี้"
                                  onClick={e => openColMenu(e, ci)}>▾</button>
                              </div>
                              <span className="colresize" onMouseDown={e => startResize(e, ci)} />
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {visible.map(({ row, idx }) => (
                          <tr key={idx}>
                            <td className="rownum">{idx + 1}</td>
                            {grid.columns.map((_, ci) => {
                              const raw = norm(row[ci])
                              return (
                                <td key={ci}
                                  className={'rocell' + (ci === 0 ? ' frozen' : '') + (numCols.has(ci) ? ' num' : '')}
                                  title={raw}>{cellText(row[ci], ci)}</td>
                              )
                            })}
                          </tr>
                        ))}
                        {!visible.length && (
                          <tr><td className="rownum"></td>
                            <td colSpan={grid.columns.length} className="hint">ไม่มีแถวตรงตัวกรอง</td>
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
      )}
    </div>
  )
}

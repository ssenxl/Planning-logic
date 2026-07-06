import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import { ColumnFilter, columnFilterData, filterRows, norm } from './ColumnFilter.jsx'

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function Database() {
  const [groups, setGroups] = useState([])
  const [sel, setSel] = useState(null)        // { id, name }
  const [grid, setGrid] = useState(null)      // { sheet, sheets, columns, rows, total, truncated }
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})  // { colIndex: Set(ค่าที่เลือก) }
  const [openCol, setOpenCol] = useState(null)

  async function loadList() {
    setMsg('')
    try { setGroups(await api.database()) }
    catch (e) { setMsg('โหลดรายการไม่ได้: ' + e.message) }
  }
  useEffect(() => { loadList() }, [])

  async function openFile(file, sheet) {
    setLoading(true); setMsg(''); setGrid(null)
    setSel({ id: file.id, name: file.name })
    setSearch(''); setFilters({}); setOpenCol(null)
    try {
      const d = await api.databaseSheet(file.id, sheet)
      setGrid(d)
    } catch (e) { setMsg('อ่านไฟล์ไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }

  async function changeSheet(sheet) {
    if (!sel) return
    setLoading(true); setMsg(''); setSearch(''); setFilters({}); setOpenCol(null)
    try {
      const d = await api.databaseSheet(sel.id, sheet)
      setGrid(d)
    } catch (e) { setMsg('อ่านชีทไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }

  async function refresh() {
    await loadList()
    if (sel && grid) {
      setLoading(true)
      try { setGrid(await api.databaseSheet(sel.id, grid.sheet)) }
      catch (e) { setMsg('รีเฟรชไม่ได้: ' + e.message) }
      finally { setLoading(false) }
    }
  }

  // ค่าที่เลือกได้ของคอลัมน์ที่เปิด popup (cascading จากตัวกรองอื่น + ค้นหา) + โดเมนเต็ม
  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  const visible = useMemo(() => grid ? filterRows(grid, search, filters) : [], [grid, search, filters])

  const hasFilter = search.trim() || Object.keys(filters).length

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
    <div className="masters">
      <aside className="filelist">
        <div className="editbar" style={{ padding: '0 0 8px' }}>
          <h2>ไฟล์ข้อมูล</h2>
          <button onClick={refresh}>รีเฟรช</button>
        </div>
        {groups.map(g => (
          <div key={g.id} className="filegroup">
            <div className="filename">{g.label}</div>
            {!g.files.length && <div className="hint small">ไม่มีไฟล์</div>}
            {g.files.map(f => {
              const active = sel && sel.id === f.id
              return (
                <button key={f.id} className={'sheetbtn' + (active ? ' active' : '')}
                  title={`${fmtSize(f.size)} • แก้ไข ${fmtTime(f.mtime)}`}
                  onClick={() => openFile(f)}>
                  {f.name}
                </button>
              )
            })}
          </div>
        ))}
      </aside>

      <section className="editor">
        {!sel && <div className="hint">เลือกไฟล์จากด้านซ้ายเพื่อดูข้อมูล</div>}
        {sel && (
          <>
            <div className="editbar">
              <h2>{sel.name}</h2>
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
                    </tr>
                  </thead>
                  <tbody>
                    {visible.map(({ row, idx }) => (
                      <tr key={idx}>
                        <td className="rownum">{idx + 1}</td>
                        {grid.columns.map((_, ci) => (
                          <td key={ci} className="rocell">{norm(row[ci])}</td>
                        ))}
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
  )
}

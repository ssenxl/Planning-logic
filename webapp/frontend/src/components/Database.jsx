import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'

const norm = (v) => (v === '' || v == null) ? '' : String(v)
const label = (v) => v === '' ? '(ว่าง)' : v

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

// ---- popup ติ๊กเลือกค่าแบบ Excel (เหมือนหน้าแก้ Master) ----
function ColumnFilter({ values, selected, pos, onApply, onClose }) {
  const [draft, setDraft] = useState(() => selected ? new Set(selected) : new Set(values))
  const [q, setQ] = useState('')
  const shown = useMemo(
    () => values.filter(v => label(v).toLowerCase().includes(q.trim().toLowerCase())),
    [values, q]
  )
  const allShownChecked = shown.length > 0 && shown.every(v => draft.has(v))

  function toggle(v) {
    setDraft(d => { const n = new Set(d); n.has(v) ? n.delete(v) : n.add(v); return n })
  }
  function toggleAll() {
    setDraft(d => {
      const n = new Set(d)
      if (allShownChecked) shown.forEach(v => n.delete(v))
      else shown.forEach(v => n.add(v))
      return n
    })
  }
  function apply() {
    onApply(draft.size === values.length ? null : draft)
  }

  return (
    <>
      <div className="filterbackdrop" onClick={onClose} />
      <div className="filterpop" style={{ top: pos.top, left: pos.left }}>
        <input className="popsearch" autoFocus placeholder="ค้นหาค่า..."
          value={q} onChange={e => setQ(e.target.value)} />
        <label className="popitem all">
          <input type="checkbox" checked={allShownChecked} onChange={toggleAll} />
          <b>(เลือกทั้งหมด)</b>
        </label>
        <div className="poplist">
          {shown.map((v, i) => (
            <label key={i} className="popitem">
              <input type="checkbox" checked={draft.has(v)} onChange={() => toggle(v)} />
              <span>{label(v)}</span>
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

  // ค่าที่เลือกได้ของคอลัมน์ที่เปิด popup (cascading จากตัวกรองอื่น + ค้นหา)
  const openColValues = useMemo(() => {
    if (!grid || !openCol) return []
    const ci = openCol.ci
    const s = search.trim().toLowerCase()
    const others = Object.keys(filters).map(Number).filter(c => c !== ci)
    const set = new Set()
    for (const row of grid.rows) {
      if (s && !row.some(c => String(c ?? '').toLowerCase().includes(s))) continue
      let ok = true
      for (const c of others) { if (!filters[c].has(norm(row[c]))) { ok = false; break } }
      if (!ok) continue
      set.add(norm(row[ci]))
    }
    return Array.from(set).sort((a, b) => label(a).localeCompare(label(b), 'th', { numeric: true }))
  }, [grid, filters, search, openCol])

  const visible = useMemo(() => {
    if (!grid) return []
    const s = search.trim().toLowerCase()
    const fcols = Object.keys(filters).map(Number)
    return grid.rows
      .map((row, idx) => ({ row, idx }))
      .filter(({ row }) => {
        if (s && !row.some(c => String(c ?? '').toLowerCase().includes(s))) return false
        for (const ci of fcols) {
          if (!filters[ci].has(norm(row[ci]))) return false
        }
        return true
      })
  }, [grid, search, filters])

  const hasFilter = search.trim() || Object.keys(filters).length

  function openColMenu(e, ci) {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setOpenCol({ ci, top: r.bottom + 2, left: Math.max(8, r.right - 240) })
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

            {openCol && (
              <ColumnFilter
                key={openCol.ci}
                values={openColValues}
                selected={filters[openCol.ci]}
                pos={openCol}
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

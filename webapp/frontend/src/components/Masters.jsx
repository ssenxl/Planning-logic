import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { ColumnFilter, columnFilterData, filterRows } from './ColumnFilter.jsx'

// ชีทที่ pipeline ใช้จริง — แสดง hint ให้ user
const USED_SHEETS = {
  Calendar: ['Sheet1'],
}

export default function Masters() {
  const [files, setFiles] = useState([])
  const [sel, setSel] = useState(null)
  const [grid, setGrid] = useState(null)      // ข้อมูลเต็ม (source of truth)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState('')
  const [dirty, setDirty] = useState(false)
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})  // { colIndex: Set(ค่าที่เลือก) }
  const [openCol, setOpenCol] = useState(null) // { ci, top, left }

  useEffect(() => { api.masters().then(setFiles).catch(e => setMsg('โหลดรายการไม่ได้: ' + e.message)) }, [])

  async function openSheet(name, sheet) {
    if (dirty && !confirm('มีการแก้ที่ยังไม่บันทึก จะทิ้งและเปิดชีทใหม่ไหม?')) return
    setLoading(true); setMsg(''); setGrid(null); setSel({ name, sheet })
    setSearch(''); setFilters({}); setOpenCol(null)
    try {
      const d = await api.sheet(name, sheet)
      setGrid(d); setDirty(false)
    } catch (e) { setMsg('อ่านชีทไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }

  function setCell(ri, ci, val) {
    setGrid(g => { const rows = g.rows.map(r => r.slice()); rows[ri][ci] = val; return { ...g, rows } })
    setDirty(true)
  }
  function addRow() {
    setSearch(''); setFilters({})
    setGrid(g => ({ ...g, rows: [...g.rows, g.columns.map(() => '')] }))
    setDirty(true)
  }
  function delRow(ri) {
    setGrid(g => ({ ...g, rows: g.rows.filter((_, i) => i !== ri) }))
    setDirty(true)
  }
  function renameCol(ci, val) {
    setGrid(g => { const cols = g.columns.slice(); cols[ci] = val; return { ...g, columns: cols } })
    setDirty(true)
  }
  function addCol() {
    const nm = (prompt('ชื่อคอลัมน์ใหม่:', 'คอลัมน์ใหม่') || '').trim()
    if (!nm) return
    setSearch(''); setFilters({}); setOpenCol(null)
    setGrid(g => ({ ...g, columns: [...g.columns, nm], rows: g.rows.map(r => [...r, '']) }))
    setDirty(true)
  }
  function delCol(ci) {
    if (!confirm(`ลบคอลัมน์ "${grid.columns[ci]}" และข้อมูลในคอลัมน์นี้ทั้งหมด?`)) return
    setSearch(''); setFilters({}); setOpenCol(null)
    setGrid(g => ({
      ...g,
      columns: g.columns.filter((_, i) => i !== ci),
      rows: g.rows.map(r => r.filter((_, i) => i !== ci)),
    }))
    setDirty(true)
  }

  async function save() {
    if (!grid || !sel) return
    setSaving(true); setMsg('')
    try {
      const r = await api.saveSheet(sel.name, sel.sheet, grid.columns, grid.rows)
      setMsg(`บันทึกแล้ว (${r.rows} แถว) — สำรองไฟล์เดิมเป็น ${r.backup}`)
      setDirty(false)
    } catch (e) { setMsg('บันทึกไม่ได้: ' + e.message) }
    finally { setSaving(false) }
  }

  // ค่าที่เลือกได้ของคอลัมน์ที่เปิด popup (cascading แบบ Excel) + โดเมนเต็มของคอลัมน์
  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  // แถวที่ผ่าน filter — เก็บ index จริง
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
        <h2>ไฟล์ Master</h2>
        {files.map(f => (
          <div key={f.name} className="filegroup">
            <div className="filename">{f.name} {!f.exists && <span className="warn">ไม่พบไฟล์</span>}</div>
            {f.error && <div className="warn small">{f.error}</div>}
            {f.sheets.map(s => {
              const used = USED_SHEETS[f.name]?.includes(s)
              const active = sel && sel.name === f.name && sel.sheet === s
              return (
                <button key={s} className={'sheetbtn' + (active ? ' active' : '')}
                  onClick={() => openSheet(f.name, s)}>
                  {s} {used && <span className="usedtag">ใช้รันแผน</span>}
                </button>
              )
            })}
          </div>
        ))}
      </aside>

      <section className="editor">
        {!sel && <div className="hint">เลือกชีทจากด้านซ้ายเพื่อแก้ไข</div>}
        {sel && (
          <>
            <div className="editbar">
              <h2>{sel.name} / {sel.sheet} {dirty && <span className="dot">●</span>}</h2>
              <div className="actions">
                <button onClick={addRow} disabled={!grid}>+ เพิ่มแถว</button>
                <button onClick={addCol} disabled={!grid}>+ เพิ่มคอลัมน์</button>
                <button className="primary" onClick={save} disabled={!grid || saving || !dirty}>
                  {saving ? 'กำลังบันทึก...' : 'บันทึก'}
                </button>
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

            {sel.name === 'Target_Stock' && (
              <div className="msg note">
                ℹ️ คอลัมน์ <b>Match core, TARGET SCM, STOCK MIN, STOCK MAX, Stock 5 Week</b> คำนวณอัตโนมัติจาก <b>TARGET/MONTH</b> ตอนกดบันทึก — แก้ TARGET/MONTH แล้วค่าที่เหลือจะอัปเดตเอง
              </div>
            )}
            {msg && <div className="msg">{msg}</div>}
            {loading && <div className="hint">กำลังโหลด...</div>}

            {grid && (
              <div className="gridwrap">
                <table className="grid">
                  <thead>
                    <tr>
                      <th className="rownum">#</th>
                      {grid.columns.map((c, ci) => (
                        <th key={ci}>
                          <div className="thcell">
                            <input className="thinput" value={c}
                              title="คลิกเพื่อแก้ชื่อคอลัมน์"
                              onChange={e => renameCol(ci, e.target.value)} />
                            <button
                              className={'funnel' + (filters[ci] ? ' on' : '')}
                              title="กรองคอลัมน์นี้"
                              onClick={e => openColMenu(e, ci)}>▾</button>
                            <button className="coldel" title="ลบคอลัมน์นี้"
                              onClick={() => delCol(ci)}>✕</button>
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
                            <input value={cell ?? ''} onChange={e => setCell(idx, ci, e.target.value)} />
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
          </>
        )}
      </section>
    </div>
  )
}

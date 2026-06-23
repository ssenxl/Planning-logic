import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'

// ชีทที่ pipeline ใช้จริง — แสดง hint ให้ user
const USED_SHEETS = {
  Calendar: ['Sheet1'],
}

const norm = (v) => (v === '' || v == null) ? '' : String(v)
const label = (v) => v === '' ? '(ว่าง)' : v

// ---- popup ติ๊กเลือกค่าแบบ Excel ----
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
    // เลือกครบทุกค่า = ไม่กรอง
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

  // ค่าที่เลือกได้ของคอลัมน์ที่เปิด popup — คิดจากแถวที่ผ่าน "ตัวกรองอื่น" + ค้นหา (cascading แบบ Excel)
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

  // แถวที่ผ่าน filter — เก็บ index จริง
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
                            <span className="thname">{c}</span>
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

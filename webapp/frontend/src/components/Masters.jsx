import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { ColumnFilter, columnFilterData, filterRows, label as valLabel, norm } from './ColumnFilter.jsx'
import CalendarEditor from './CalendarEditor.jsx'
import LockMcEditor from './LockMcEditor.jsx'
import S9Editor from './S9Editor.jsx'
import ConfigEditor from './ConfigEditor.jsx'
import LookupEditor from './LookupEditor.jsx'
import MasterMcTable from './MasterMcTable.jsx'
import McTotalEditor from './McTotalEditor.jsx'

// ชีทที่ pipeline ใช้จริง — แสดง hint ให้ user
const USED_SHEETS = {
  Calendar: ['Sheet1'],
}

const ROWNUM_W = 46
const ACT_W = 40
const MIN_COL_W = 70
const MAX_INIT_W = 240

// ความกว้างเริ่มต้นของคอลัมน์ — เดาจากหัวคอลัมน์ + ค่าตัวอย่าง 40 แถวแรก
function guessWidth(columns, rows, ci) {
  let len = String(columns[ci] ?? '').length
  for (let i = 0; i < Math.min(rows.length, 40); i++) {
    len = Math.max(len, String(rows[i][ci] ?? '').length)
  }
  return Math.round(Math.max(MIN_COL_W, Math.min(MAX_INIT_W, 46 + len * 7.4)))
}

const isNum = v => v !== '' && v != null && !isNaN(Number(v))

// เรียงค่า: ตัวเลขเทียบเป็นตัวเลข, ว่างไปท้ายเสมอ
function cmpVal(a, b) {
  const x = norm(a), y = norm(b)
  if (x === y) return 0
  if (x === '') return 1
  if (y === '') return -1
  if (isNum(x) && isNum(y)) return Number(x) - Number(y)
  return String(x).localeCompare(String(y), 'th', { numeric: true })
}

// normalize เกจให้ตรงกับที่ Planning/AVA ใช้ (ตัด G / GAUGE ออก) เช่น "20G" → "20"
const normGauge = v => String(v ?? '').trim().toUpperCase().replace('GAUGE', '').replace('G', '').trim()
const numCmp = (a, b) => String(a).localeCompare(String(b), 'th', { numeric: true })
const sortSet = s => [...s].sort(numCmp)

// สร้าง option ให้ dropdown ของ Lock_MC / MC_Total จากชีทหลัก MasterMC + รายการสัปดาห์ของแผน
// - cats: รายชื่อกลุ่มเครื่อง (MC_CAT) ทั้งหมด
// - gaugeByCat / mcByCat / facByCat: เกจ / เครื่อง (FA/SKP) / โรงงาน ของแต่ละกลุ่ม (cascading)
// - totalByCatGauge: จำนวนเครื่องเดิม "CAT|เกจ" → { all, byFac } ให้ MC_Total เทียบค่าเดิมได้
// รองรับ merged cell ในไฟล์ (MC_CAT/MC/Factory เว้นว่างในแถวถัดไป) ด้วยการ forward-fill
function buildLockOpts(main, weekMap) {
  const cats = []
  const gaugeByCat = {}
  const mcByCat = {}
  const facByCat = {}
  const factories = new Set()
  const totalByCatGauge = {}
  if (main && main.columns) {
    const up = main.columns.map(c => String(c).trim().toUpperCase())
    const catCi = up.indexOf('MC_CAT')
    const gCi = up.indexOf('GUAGE') >= 0 ? up.indexOf('GUAGE') : up.indexOf('GAUGE')
    const mcCi = up.indexOf('MC')
    const facCi = up.indexOf('FACTORY')
    const totCi = up.indexOf('TOTAL MC')
    let lastCat = '', lastMc = '', lastFac = ''
    for (const r of main.rows) {
      let cat = String(r[catCi] ?? '').trim(); if (cat) lastCat = cat; else cat = lastCat
      let mc = mcCi >= 0 ? String(r[mcCi] ?? '').trim() : ''; if (mc) lastMc = mc; else mc = lastMc
      let fac = facCi >= 0 ? String(r[facCi] ?? '').trim() : ''; if (fac) lastFac = fac; else fac = lastFac
      const g = gCi >= 0 ? normGauge(r[gCi]) : ''
      if (!cat) continue
      if (!gaugeByCat[cat]) {
        gaugeByCat[cat] = new Set(); mcByCat[cat] = new Set(); facByCat[cat] = new Set()
        cats.push(cat)
      }
      if (g) gaugeByCat[cat].add(g)
      if (mc) mcByCat[cat].add(mc)
      if (fac) { facByCat[cat].add(fac); factories.add(fac) }
      // ยอดเครื่องเดิม: รวมทั้ง CAT+เกจ และแยกรายโรงงาน (ตรงกับที่ AVA_MC ใช้ตัดสิน)
      const tot = totCi >= 0 ? Number(String(r[totCi] ?? '').trim()) : NaN
      if (g && !isNaN(tot) && String(r[totCi] ?? '').trim() !== '') {
        const k = `${cat.toUpperCase()}|${g}`
        const slot = totalByCatGauge[k] || (totalByCatGauge[k] = { all: 0, byFac: {} })
        slot.all += tot
        if (fac) {
          const fk = fac.toUpperCase()
          slot.byFac[fk] = (slot.byFac[fk] || 0) + tot
        }
      }
    }
  }
  const gOut = {}, mOut = {}, fOut = {}
  for (const c of cats) {
    gOut[c] = sortSet(gaugeByCat[c]); mOut[c] = sortSet(mcByCat[c]); fOut[c] = sortSet(facByCat[c])
  }
  return {
    cats: cats.sort(numCmp),
    gaugeByCat: gOut,
    mcByCat: mOut,
    facByCat: fOut,
    factories: sortSet(factories),
    totalByCatGauge,
    weeks: Object.keys(weekMap || {}).sort(numCmp),
  }
}

const insertAt = (arr, i, v) => { const n = arr.slice(); n.splice(i, 0, v); return n }
const removeAt = (arr, i) => arr.filter((_, j) => j !== i)

// เลื่อน key ของตัวกรอง (ที่คีย์ด้วย index คอลัมน์) เมื่อมีการแทรก/ลบคอลัมน์
function shiftFilters(filters, at, delta) {
  const out = {}
  for (const k of Object.keys(filters)) {
    const ci = Number(k)
    if (delta < 0 && ci === at) continue          // คอลัมน์ที่ถูกลบ
    out[ci >= at ? ci + delta : ci] = filters[k]
  }
  return out
}

export default function Masters() {
  const [files, setFiles] = useState([])
  const [fq, setFq] = useState('')            // ค้นหาไฟล์/ชีทใน sidebar
  const [collapsed, setCollapsed] = useState({})
  const [sel, setSel] = useState(null)
  const [grid, setGrid] = useState(null)      // ข้อมูลเต็ม (source of truth)
  const [ids, setIds] = useState({ rid: [], cid: [] })  // id คงที่ของแถว/คอลัมน์ ใช้เทียบว่าเซลล์ไหนถูกแก้
  const [orig, setOrig] = useState(new Map()) // `${rid}:${cid}` -> ค่าตอนโหลด
  const [origRids, setOrigRids] = useState(new Set())
  const [hist, setHist] = useState([])        // stack สำหรับ undo
  const [widths, setWidths] = useState([])
  const [sort, setSort] = useState(null)      // { ci, dir } — เรียงเพื่อดูเท่านั้น ไม่เปลี่ยนลำดับในไฟล์
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState('')
  const [dirty, setDirty] = useState(false)
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})  // { colIndex: Set(ค่าที่เลือก) }
  const [openCol, setOpenCol] = useState(null)   // popup ตัวกรอง
  const [menuCol, setMenuCol] = useState(null)   // popup เมนู ⋮ ของคอลัมน์
  const [renameCi, setRenameCi] = useState(null) // คอลัมน์ที่กำลังเปลี่ยนชื่อ
  const [dlg, setDlg] = useState(null)           // dialog เพิ่มคอลัมน์: { name, at }
  const [view, setView] = useState('cal')        // ชีทแบบปฏิทิน: 'cal' = ปฏิทิน, 'table' = ตารางดิบ
  const nextId = useRef({ r: 0, c: 0 })

  useEffect(() => { api.masters().then(setFiles).catch(e => setMsg('โหลดรายการไม่ได้: ' + e.message)) }, [])

  // เตือนก่อนปิด/รีเฟรชหน้า ถ้ายังมีของที่ไม่ได้บันทึก
  useEffect(() => {
    if (!dirty) return
    const h = e => { e.preventDefault(); e.returnValue = '' }
    window.addEventListener('beforeunload', h)
    return () => window.removeEventListener('beforeunload', h)
  }, [dirty])

  async function openSheet(name, sheet, force = false) {
    if (!force && dirty && !confirm('มีการแก้ที่ยังไม่บันทึก จะทิ้งและเปิดชีทใหม่ไหม?')) return
    setLoading(true); setMsg(''); setGrid(null); setSel({ name, sheet })
    setSearch(''); setFilters({}); setOpenCol(null); setMenuCol(null); setSort(null); setRenameCi(null)
    setView('cal')
    try {
      const d = await api.sheet(name, sheet)
      const rid = d.rows.map((_, i) => i)
      const cid = d.columns.map((_, i) => i)
      nextId.current = { r: d.rows.length, c: d.columns.length }
      const m = new Map()
      d.rows.forEach((r, i) => d.columns.forEach((_, j) => m.set(`${rid[i]}:${cid[j]}`, norm(r[j]))))
      setGrid(d)
      setIds({ rid, cid })
      setOrig(m); setOrigRids(new Set(rid))
      setHist([])
      setWidths(d.columns.map((_, ci) => guessWidth(d.columns, d.rows, ci)))
      setDirty(false)
    } catch (e) { setMsg('อ่านชีทไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }

  const pushHist = p => setHist(h => [...h, p].slice(-200))

  // ---- แก้ข้อมูล ----
  function setCell(ri, ci, val) {
    // รวมการพิมพ์ต่อเนื่องในเซลล์เดียวเป็น undo ก้อนเดียว
    setHist(h => {
      const last = h[h.length - 1]
      if (last && last.t === 'cell' && last.ri === ri && last.ci === ci) return h
      return [...h, { t: 'cell', ri, ci, prev: grid.rows[ri][ci] }].slice(-200)
    })
    setGrid(g => {
      const rows = g.rows.slice()
      rows[ri] = rows[ri].slice()
      rows[ri][ci] = val
      return { ...g, rows }
    })
    setDirty(true)
  }

  function addRow() {
    setSearch(''); setFilters({}); setSort(null)
    pushHist({ t: 'addRow' })
    setGrid(g => ({ ...g, rows: [...g.rows, g.columns.map(() => '')] }))
    setIds(s => ({ ...s, rid: [...s.rid, nextId.current.r++] }))
    setDirty(true)
    setTimeout(() => {
      const els = document.querySelectorAll('.masters input[data-cell]')
      const first = Array.from(els).filter(e => e.dataset.cell.endsWith('-0')).pop()
      if (first) { first.scrollIntoView({ block: 'nearest' }); first.focus() }
    }, 0)
  }

  function delRow(ri) {
    pushHist({ t: 'delRow', ri, row: grid.rows[ri], rid: ids.rid[ri] })
    setGrid(g => ({ ...g, rows: removeAt(g.rows, ri) }))
    setIds(s => ({ ...s, rid: removeAt(s.rid, ri) }))
    setDirty(true)
  }

  function commitRename(ci, val) {
    setRenameCi(null)
    const v = String(val ?? '').trim()
    if (!v || v === grid.columns[ci]) return
    pushHist({ t: 'renameCol', ci, prev: grid.columns[ci] })
    setGrid(g => { const columns = g.columns.slice(); columns[ci] = v; return { ...g, columns } })
    setDirty(true)
  }

  function addColAt(name, at) {
    const ci = at == null ? grid.columns.length : at
    pushHist({ t: 'addCol', ci })
    setGrid(g => ({ ...g, columns: insertAt(g.columns, ci, name), rows: g.rows.map(r => insertAt(r, ci, '')) }))
    setIds(s => ({ ...s, cid: insertAt(s.cid, ci, nextId.current.c++) }))
    setWidths(w => insertAt(w, ci, 130))
    setFilters(f => shiftFilters(f, ci, +1))
    setSort(s => (s && s.ci >= ci ? { ...s, ci: s.ci + 1 } : s))
    setOpenCol(null); setDlg(null)
    setDirty(true)
  }

  function delCol(ci) {
    if (!confirm(`ลบคอลัมน์ "${grid.columns[ci]}" และข้อมูลในคอลัมน์นี้ทั้งหมด?\n(กด "ย้อนกลับ" หรือ Ctrl+Z เพื่อเรียกคืนได้)`)) return
    pushHist({
      t: 'delCol', ci, name: grid.columns[ci], cid: ids.cid[ci],
      vals: grid.rows.map(r => r[ci]), w: widths[ci],
    })
    setGrid(g => ({ ...g, columns: removeAt(g.columns, ci), rows: g.rows.map(r => removeAt(r, ci)) }))
    setIds(s => ({ ...s, cid: removeAt(s.cid, ci) }))
    setWidths(w => removeAt(w, ci))
    setFilters(f => shiftFilters(f, ci, -1))
    setSort(s => (!s ? s : s.ci === ci ? null : s.ci > ci ? { ...s, ci: s.ci - 1 } : s))
    setOpenCol(null); setMenuCol(null)
    setDirty(true)
  }

  // ---- undo ----
  function undo() {
    const p = hist[hist.length - 1]
    if (!p) return
    setHist(h => h.slice(0, -1))
    if (p.t === 'cell') {
      setGrid(g => {
        const rows = g.rows.slice()
        rows[p.ri] = rows[p.ri].slice()
        rows[p.ri][p.ci] = p.prev
        return { ...g, rows }
      })
    } else if (p.t === 'addRow') {
      setGrid(g => ({ ...g, rows: g.rows.slice(0, -1) }))
      setIds(s => ({ ...s, rid: s.rid.slice(0, -1) }))
    } else if (p.t === 'delRow') {
      setGrid(g => ({ ...g, rows: insertAt(g.rows, p.ri, p.row) }))
      setIds(s => ({ ...s, rid: insertAt(s.rid, p.ri, p.rid) }))
    } else if (p.t === 'renameCol') {
      setGrid(g => { const columns = g.columns.slice(); columns[p.ci] = p.prev; return { ...g, columns } })
    } else if (p.t === 'addCol') {
      setGrid(g => ({ ...g, columns: removeAt(g.columns, p.ci), rows: g.rows.map(r => removeAt(r, p.ci)) }))
      setIds(s => ({ ...s, cid: removeAt(s.cid, p.ci) }))
      setWidths(w => removeAt(w, p.ci))
      setFilters(f => shiftFilters(f, p.ci, -1))
      setSort(s => (!s ? s : s.ci === p.ci ? null : s.ci > p.ci ? { ...s, ci: s.ci - 1 } : s))
    } else if (p.t === 'delCol') {
      setGrid(g => ({
        ...g,
        columns: insertAt(g.columns, p.ci, p.name),
        rows: g.rows.map((r, i) => insertAt(r, p.ci, p.vals[i])),
      }))
      setIds(s => ({ ...s, cid: insertAt(s.cid, p.ci, p.cid) }))
      setWidths(w => insertAt(w, p.ci, p.w))
      setFilters(f => shiftFilters(f, p.ci, +1))
      setSort(s => (s && s.ci >= p.ci ? { ...s, ci: s.ci + 1 } : s))
    }
    setDirty(true)
  }

  async function save() {
    if (!grid || !sel) return
    setSaving(true); setMsg('')
    try {
      const r = await api.saveSheet(sel.name, sel.sheet, grid.columns, grid.rows)
      setMsg(`บันทึกแล้ว (${r.rows} แถว) — สำรองไฟล์เดิมเป็น ${r.backup}`)
      // ยึดข้อมูลปัจจุบันเป็นค่าตั้งต้นใหม่ → ไฮไลต์เซลล์ที่แก้หายไป
      const m = new Map()
      grid.rows.forEach((r2, i) => grid.columns.forEach((_, j) => m.set(`${ids.rid[i]}:${ids.cid[j]}`, norm(r2[j]))))
      setOrig(m); setOrigRids(new Set(ids.rid)); setHist([])
      setDirty(false)
    } catch (e) { setMsg('บันทึกไม่ได้: ' + e.message) }
    finally { setSaving(false) }
  }

  function revert() {
    if (!confirm('ทิ้งการแก้ทั้งหมดแล้วโหลดชีทใหม่จากไฟล์?')) return
    openSheet(sel.name, sel.sheet, true)
  }

  // ---- คีย์ลัด: Ctrl+Z ย้อนกลับ, Ctrl+S บันทึก ----
  useEffect(() => {
    function onKey(e) {
      if (!(e.ctrlKey || e.metaKey)) return
      const k = e.key.toLowerCase()
      if (k === 'z') { e.preventDefault(); undo() }
      else if (k === 's') { e.preventDefault(); if (dirty && !saving) save() }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [hist, grid, ids, dirty, saving, sel])

  // ---- เดินเซลล์ด้วยคีย์บอร์ดแบบ Excel ----
  function onCellKey(e, vi, ci) {
    const el = e.currentTarget
    const move = (dv, dc) => {
      const t = document.querySelector(`.masters input[data-cell="${vi + dv}-${ci + dc}"]`)
      if (!t) return
      e.preventDefault()
      t.focus(); t.select()
      t.scrollIntoView({ block: 'nearest', inline: 'nearest' })
    }
    const caretEnd = el.selectionStart === el.value.length && el.selectionStart === el.selectionEnd
    const caretStart = el.selectionStart === 0 && el.selectionEnd === 0
    if (e.key === 'Enter') move(e.shiftKey ? -1 : 1, 0)
    else if (e.key === 'ArrowDown') move(1, 0)
    else if (e.key === 'ArrowUp') move(-1, 0)
    else if (e.key === 'ArrowRight' && caretEnd) move(0, 1)
    else if (e.key === 'ArrowLeft' && caretStart) move(0, -1)
    else if (e.key === 'Escape') el.blur()
  }

  // ---- ลากขอบปรับความกว้างคอลัมน์ ----
  function startResize(e, ci) {
    e.preventDefault(); e.stopPropagation()
    const x0 = e.clientX, w0 = widths[ci]
    const move = ev => {
      const w = Math.max(MIN_COL_W, Math.round(w0 + (ev.clientX - x0)))
      setWidths(ws => { const n = ws.slice(); n[ci] = w; return n })
    }
    const up = () => {
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerup', up)
      document.body.classList.remove('col-resizing')
    }
    window.addEventListener('pointermove', move)
    window.addEventListener('pointerup', up)
    document.body.classList.add('col-resizing')
  }
  function autoFit(ci) {
    setWidths(w => { const n = w.slice(); n[ci] = guessWidth(grid.columns, grid.rows, ci); return n })
  }

  function toggleSort(ci) {
    setSort(s => (!s || s.ci !== ci) ? { ci, dir: 'asc' } : s.dir === 'asc' ? { ci, dir: 'desc' } : null)
  }

  // หัวคอลัมน์: คลิกเดี่ยว = เรียง, ดับเบิลคลิก = เปลี่ยนชื่อ — หน่วง 200ms กันคลิกเดี่ยวยิงตอนดับเบิลคลิก
  const clickTimer = useRef(null)
  function headClick(ci) {
    if (clickTimer.current) return
    clickTimer.current = setTimeout(() => { clickTimer.current = null; toggleSort(ci) }, 200)
  }
  function headDouble(ci) {
    clearTimeout(clickTimer.current)
    clickTimer.current = null
    setRenameCi(ci)
  }

  // ค่าที่เลือกได้ของคอลัมน์ที่เปิด popup (cascading แบบ Excel) + โดเมนเต็มของคอลัมน์
  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  // แถวที่ผ่าน filter (+เรียงตามคอลัมน์ที่เลือก) — เก็บ index จริงไว้อ้างอิงตอนแก้/ลบ
  const visible = useMemo(() => {
    if (!grid) return []
    const v = filterRows(grid, search, filters)
    if (sort) {
      const s = sort.dir === 'asc' ? 1 : -1
      v.sort((a, b) => s * cmpVal(a.row[sort.ci], b.row[sort.ci]))
    }
    return v
  }, [grid, search, filters, sort])

  const fchips = Object.keys(filters).map(Number)
  const hasFilter = search.trim() || fchips.length || sort

  // ชีทที่เป็น "ปฏิทินรายวัน" (มีคอลัมน์ DATE + status) → เปิดมุมมองปฏิทินได้
  const calInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase())
    const dateCi = up.indexOf('DATE')
    const statusCi = up.indexOf('STATUS')
    if (dateCi < 0 || statusCi < 0) return null
    const weekCi = up.indexOf('W') >= 0 ? up.indexOf('W') : up.indexOf('WEEK')
    return { dateCi, statusCi, weekCi }
  }, [grid])
  const calView = !!calInfo && view === 'cal'

  // ชีท Lock_MC (มีคอลัมน์ MC_CAT + MC Lock) → เปิดมุมมองการ์ด "กันเครื่อง" ได้
  const lockInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase())
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const catCi = find('MC_CAT', 'MC CAT')
    const lockCi = find('MC LOCK', 'LOCK')
    if (catCi < 0 || lockCi < 0) return null
    return { weekCi: find('WEEK', 'W'), catCi, gaugeCi: find('GUAGE', 'GAUGE'), mcCi: find('MC'), lockCi, itemCi: find('ITEM', 'ITEM_CODE') }
  }, [grid])
  const lockView = !!lockInfo && view === 'cal'

  // ชีท MC_Total (WEEK + MC_CAT + TOTAL MC แต่ไม่มี SUBGROUP = ไม่ใช่ Master MC หลัก)
  // → การ์ด "จำนวนเครื่องรายสัปดาห์" ที่ใช้แทน Total MC เฉพาะสัปดาห์ที่ระบุ
  const mcTotalInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase().replace(/\s+/g, ' '))
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const weekCi = find('WEEK', 'W')
    const catCi = find('MC_CAT', 'MC CAT')
    const totalCi = find('TOTAL MC')
    if (weekCi < 0 || catCi < 0 || totalCi < 0) return null
    if (find('SUBGROUP', 'SUB GROUP') >= 0 || find('MC LOCK', 'LOCK') >= 0) return null
    return {
      weekCi, catCi, totalCi,
      facCi: find('FACTORY'), gaugeCi: find('GUAGE', 'GAUGE'), remarkCi: find('REMARK'),
      weekToCi: find('WEEK TO', 'ถึงสัปดาห์'),
    }
  }, [grid])
  const mcTotalView = !!mcTotalInfo && view === 'cal'

  // ชีทกลุ่ม S9 (จ้างทอ) ในไฟล์ MasterMC — ตรวจจากหัวคอลัมน์:
  //   MC S9   : MC_CAT + (MC Group/Cap-Day) ไม่มี ITEM_CODE → การ์ด pool เครื่อง
  //   Item S9 : ITEM_CODE + MACHINE_GROUP            → ค้นหา item จ้างทอได้
  //   S9 Only : ITEM_CODE + MC_GAUGE (ไม่มี MACHINE_GROUP) → รายการ item จ้างทอเสมอ
  const s9Info = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase().replace(/\s+/g, ' '))
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const itemCi = find('ITEM_CODE', 'ITEM CODE')
    const mgCi = find('MACHINE_GROUP', 'MACHINE GROUP')
    const gaugeCi = find('MC_GAUGE', 'MC GAUGE', 'GUAGE', 'GAUGE')
    if (itemCi >= 0 && mgCi >= 0) return { kind: 'items9', itemCi, mgCi, gaugeCi }
    if (itemCi >= 0 && gaugeCi >= 0) return { kind: 's9only', itemCi, mgCi: -1, gaugeCi }
    const catCi = find('MC_CAT', 'MC CAT')
    const mcGroupCi = find('MC GROUP', 'MC_GROUP')
    const capCi = find('CAP/DAY', 'CAP / DAY', 'CAP DAY')
    if (itemCi < 0 && catCi >= 0 && (mcGroupCi >= 0 || capCi >= 0)) {
      return { kind: 'mcs9', catCi, mcGroupCi, gaugeCi, totalCi: find('TOTAL MC'), capCi, wdCi: find('WORKING DAY'), remarkCi: find('REMARK') }
    }
    return null
  }, [grid])
  const s9View = !!s9Info && view === 'cal'

  // ชีท "config เล็ก" ในไฟล์ MasterMC — ตรวจจากหัวคอลัมน์แล้วสร้าง descriptor ให้ ConfigEditor
  //   MC Special (POLY+COTTON) · Item Special (ITEM+วันทำงาน) · Spare part (TOTAL SPARE) · MC substitute (MC_CAT+GROUP ล้วน)
  const configInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase().replace(/\s+/g, ' '))
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const F = (ci, label) => ci >= 0 ? { ci, label } : null
    const clean = arr => arr.filter(Boolean)
    const factoryCi = find('FACTORY'), typeCi = find('TYPE'), catCi = find('MC_CAT', 'MC CAT')
    const mcCi = find('MC'), gaugeCi = find('GUAGE', 'GAUGE')

    const poly = find('POLY'), cotton = find('COTTON')
    if (poly >= 0 && cotton >= 0) return {
      kind: 'mcspecial', headline: '🧵 กันเครื่องให้ POLY / COTTON',
      hint: 'เครื่องที่กันไว้จะถูกสงวนสำหรับงาน POLY/COTTON โดยเฉพาะ',
      addLabel: '+ เพิ่มการกันเครื่อง', itemWord: 'รายการ',
      idFields: clean([F(factoryCi, 'Factory'), F(typeCi, 'Type'), F(catCi, 'MC_CAT'), F(mcCi, 'MC'), F(gaugeCi, 'เกจ')]),
      numFields: clean([poly >= 0 && { ci: poly, label: 'POLY (เครื่อง)', accent: 'poly' }, cotton >= 0 && { ci: cotton, label: 'COTTON (เครื่อง)', accent: 'cotton' }]),
      noteFields: clean([F(find('DESCRIPTION'), 'หมายเหตุ')]),
      summary: clean([F(poly, 'POLY'), F(cotton, 'COTTON')]),
    }

    const itemCi = find('ITEM'), wdCi = find('WORKING DAY'), whCi = find('WORKING HOUR')
    if (itemCi >= 0 && (wdCi >= 0 || whCi >= 0)) return {
      kind: 'itemspecial', headline: '🎯 ตั้งค่าวันทำงาน/ชั่วโมง เฉพาะ item',
      hint: 'ค่านี้จะแทนที่วันทำงาน/ชั่วโมงมาตรฐานของ item ที่ระบุ',
      addLabel: '+ เพิ่ม item', itemWord: 'item',
      titleField: F(itemCi, 'Item'),
      idFields: clean([F(mcCi, 'เครื่อง (MC)'), F(gaugeCi, 'เกจ')]),
      numFields: clean([F(wdCi, 'Working day'), F(whCi, 'Working hour')]),
      noteFields: [], summary: [],
    }

    const spareCi = find('TOTAL SPARE')
    if (spareCi >= 0) return {
      kind: 'spare', headline: '🧰 เครื่องสำรอง (Spare)',
      addLabel: '+ เพิ่มรายการ', itemWord: 'รายการ',
      groupCi: factoryCi, spareCi,
      lineCis: clean([F(catCi, 'MC_CAT'), F(gaugeCi, 'เกจ')]),
      summary: clean([F(spareCi, 'สำรองรวม')]),
    }

    const groupCol = find('GROUP')
    if (catCi >= 0 && groupCol >= 0 && find('SUBGROUP', 'SUB GROUP') < 0 && find('TOTAL MC') < 0 && wdCi < 0) return {
      kind: 'substitute', headline: '🔁 เครื่องทดแทน (Substitute)',
      hint: 'กำหนดว่าเครื่อง/เกจใดใช้แทนกลุ่มใดได้',
      addLabel: '+ เพิ่มเครื่องทดแทน', itemWord: 'รายการ',
      idFields: clean([F(factoryCi, 'Factory'), F(typeCi, 'Type'), F(catCi, 'MC_CAT'), F(mcCi, 'เครื่อง'), F(gaugeCi, 'เกจ'), F(groupCol, 'กลุ่ม')]),
      numFields: [], noteFields: [], summary: [],
    }
    return null
  }, [grid])
  const configView = !!configInfo && view === 'cal'

  // ชีท Master MC (คอลัมน์คีย์ MC_CAT + TOTAL MC + SUBGROUP) → ตารางตรึงคีย์ + คั่นกลุ่ม Factory
  const mastermcInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase().replace(/\s+/g, ' '))
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const catCi = find('MC_CAT', 'MC CAT'), totalCi = find('TOTAL MC'), subCi = find('SUBGROUP', 'SUB GROUP')
    if (catCi < 0 || totalCi < 0 || subCi < 0) return null
    const keyCis = [catCi, find('MC'), find('GUAGE', 'GAUGE')].filter(c => c >= 0)
    return { factoryCi: find('FACTORY'), keyCis, totalCi, catCi }
  }, [grid])
  const mastermcView = !!mastermcInfo && view === 'cal'

  // ชีทใหญ่มาก (เช่น Master_Item) — ตารางดิบเปิดไม่ไหว → บังคับมุมมองค้นหาเสมอ
  const bigView = !!grid && !calInfo && !lockInfo && !mcTotalInfo && !s9Info && !configInfo && !mastermcInfo && grid.rows.length > 1500

  const specialView = calView || lockView || mcTotalView || s9View || configView || mastermcView || bigView

  // โหลดรายการ MC_CAT/เกจ/เครื่อง/โรงงาน + ยอดเครื่องเดิม (ชีทหลัก MasterMC) + สัปดาห์ (ปฏิทิน)
  // มาทำ dropdown ของ Lock_MC และ MC_Total
  const [lockOpts, setLockOpts] = useState(null)
  const isLock = !!lockInfo || !!mcTotalInfo
  useEffect(() => {
    if (!isLock || !sel) { setLockOpts(null); return }
    let dead = false
    const file = files.find(f => f.name === sel.name)
    const aux = new Set(['Lock_MC', 'MC_Total', 'MC Special', 'Item Special'])
    const mainSheet = file?.sheets.find(s => s !== sel.sheet && !aux.has(s)) || null
    Promise.all([
      // สัปดาห์ที่มีในปฏิทิน (สดจาก Calendar.xlsx บน server) — ไม่ผูกกับไฟล์แผนรอบก่อน
      api.workday().then(d => d?.cal_days || {}).catch(() => ({})),
      mainSheet ? api.sheet(sel.name, mainSheet).catch(() => null) : Promise.resolve(null),
    ]).then(([wk, main]) => { if (!dead) setLockOpts(buildLockOpts(main, wk)) })
    return () => { dead = true }
  }, [isLock, sel, files])

  // ใส่ค่า Item ในการ์ด — ถ้าชีทยังไม่มีคอลัมน์ ITEM ให้สร้างท้ายสุดแล้วใส่ค่าในแถวนั้นทันที
  // (undo ครั้งเดียวคืนทั้งคอลัมน์ได้) → ผู้ใช้แค่พิมพ์ในการ์ด ไม่ต้องกดเพิ่มคอลัมน์เอง
  function setItemCell(ri, val) {
    const ci = lockInfo?.itemCi
    if (ci != null && ci >= 0) { setCell(ri, ci, val); return }
    const newCi = grid.columns.length
    pushHist({ t: 'addCol', ci: newCi })
    setGrid(g => ({
      ...g,
      columns: [...g.columns, 'ITEM'],
      rows: g.rows.map((r, i) => [...r, i === ri ? val : '']),
    }))
    setIds(s => ({ ...s, cid: [...s.cid, nextId.current.c++] }))
    setWidths(w => [...w, 130])
    setDirty(true)
  }

  // ใส่ "ถึงสัปดาห์" ในการ์ด MC_Total — ถ้าชีทยังไม่มีคอลัมน์ Week To ให้สร้างท้ายสุดแล้วใส่ค่าทันที
  // (เหมือน setItemCell) → row/แถวเดิมที่ไม่เคยกรอกช่องนี้ไม่กระทบ เพราะค่าว่าง = สัปดาห์เดียวเหมือนเดิม
  function setMcWeekToCell(ri, val) {
    const ci = mcTotalInfo?.weekToCi
    if (ci != null && ci >= 0) { setCell(ri, ci, val); return }
    const newCi = grid.columns.length
    pushHist({ t: 'addCol', ci: newCi })
    setGrid(g => ({
      ...g,
      columns: [...g.columns, 'Week To'],
      rows: g.rows.map((r, i) => [...r, i === ri ? val : '']),
    }))
    setIds(s => ({ ...s, cid: [...s.cid, nextId.current.c++] }))
    setWidths(w => [...w, 110])
    setDirty(true)
  }

  function openColMenu(e, ci) {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setMenuCol(null)
    setOpenCol({ ci, anchor: { top: r.top, bottom: r.bottom, left: r.left, right: r.right } })
  }
  function openKebab(e, ci) {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setOpenCol(null)
    setMenuCol({ ci, top: r.bottom + 4, left: Math.min(r.left, window.innerWidth - 220) })
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

  // เซลล์นี้ถูกแก้จากค่าเดิมไหม (เทียบด้วย id คงที่ ไม่ใช่ index → เพิ่ม/ลบแถวแล้วไม่เพี้ยน)
  function isChanged(ri, ci) {
    const key = `${ids.rid[ri]}:${ids.cid[ci]}`
    const v = norm(grid.rows[ri][ci])
    return orig.has(key) ? orig.get(key) !== v : v !== ''
  }
  const isNewRow = ri => !origRids.has(ids.rid[ri])

  // ---- sidebar: ค้นหา + ยุบ/ขยายไฟล์ ----
  const q = fq.trim().toLowerCase()
  const shownFiles = useMemo(() => files.map(f => {
    const fileHit = f.name.toLowerCase().includes(q)
    const sheets = q && !fileHit ? f.sheets.filter(s => s.toLowerCase().includes(q)) : f.sheets
    return { ...f, sheets, hide: q && !fileHit && !sheets.length }
  }).filter(f => !f.hide), [files, q])

  const totalW = ROWNUM_W + ACT_W + widths.reduce((a, b) => a + b, 0)

  return (
    <div className="masters">
      <aside className="filelist">
        <h2>ไฟล์ Master</h2>
        <input className="sheetsearch" placeholder="🔍 ค้นหาไฟล์ / ชีท..."
          value={fq} onChange={e => setFq(e.target.value)} />
        {!shownFiles.length && <div className="hint small">ไม่พบไฟล์ที่ค้นหา</div>}
        {shownFiles.map(f => {
          const open = q ? true : !collapsed[f.name]
          // ไฟล์ที่คลิก "ชื่อไฟล์" แล้วเปิดชีทได้เลย: ชีทที่ pipeline ใช้ หรือไฟล์ที่มีชีทเดียว
          // → ไม่ต้องโชว์รายการชีทย่อย (ชีทอื่นในไฟล์เหล่านี้ไม่ได้ใช้)
          const quick = USED_SHEETS[f.name]?.[0] || (f.sheets.length === 1 ? f.sheets[0] : null)
          const quickActive = quick && sel && sel.name === f.name && sel.sheet === quick
          const toggle = () => setCollapsed(c => ({ ...c, [f.name]: !c[f.name] }))
          return (
            <div key={f.name} className="filegroup">
              <button className={'filename' + (quickActive ? ' active' : '')}
                title={quick ? `เปิด ${f.name} / ${quick}` : 'กางดูชีท'}
                onClick={() => (quick ? openSheet(f.name, quick) : toggle())}>
                {!quick && <span className="caret">{open ? '▾' : '▸'}</span>}
                <span className="fname">{f.name}</span>
                {!f.exists && <span className="warn">ไม่พบไฟล์</span>}
                {!quick && <span className="scount">{f.sheets.length}</span>}
              </button>
              {f.error && <div className="warn small">{f.error}</div>}
              {open && !quick && f.sheets.map(s => {
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
          )
        })}
      </aside>

      <section className="editor">
        {!sel && <div className="hint">เลือกชีทจากด้านซ้ายเพื่อแก้ไข</div>}
        {sel && (
          <>
            <div className="editbar">
              <h2>
                {sel.name} / {sel.sheet}
                {dirty && <span className="dot" title="ยังไม่ได้บันทึก">●</span>}
              </h2>
              <div className="actions">
                {calInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของชีทปฏิทิน">
                    <button className={calView ? 'on' : ''} onClick={() => setView('cal')}>📅 ปฏิทิน</button>
                    <button className={!calView ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {lockInfo && !calInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของชีท Lock_MC">
                    <button className={lockView ? 'on' : ''} onClick={() => setView('cal')}>🔒 กันเครื่อง</button>
                    <button className={!lockView ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {mcTotalInfo && !calInfo && !lockInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของชีท MC_Total">
                    <button className={mcTotalView ? 'on' : ''} onClick={() => setView('cal')}>🔢 จำนวนเครื่อง</button>
                    <button className={!mcTotalView ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {s9Info && !calInfo && !lockInfo && !mcTotalInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของชีท S9 (จ้างทอ)">
                    <button className={s9View ? 'on' : ''} onClick={() => setView('cal')}>
                      {s9Info.kind === 'mcs9' ? '📇 การ์ด' : '🔍 ค้นหา'}
                    </button>
                    <button className={!s9View ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {configInfo && !calInfo && !lockInfo && !mcTotalInfo && !s9Info && (
                  <div className="viewtoggle" title="สลับมุมมองของชีท config">
                    <button className={configView ? 'on' : ''} onClick={() => setView('cal')}>
                      {configInfo.kind === 'spare' ? '🧰 ลิสต์' : '📇 การ์ด'}
                    </button>
                    <button className={!configView ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {mastermcInfo && !calInfo && !lockInfo && !mcTotalInfo && !s9Info && !configInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของ Master MC">
                    <button className={mastermcView ? 'on' : ''} onClick={() => setView('cal')}>📋 ตรึงคีย์</button>
                    <button className={!mastermcView ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {bigView && (
                  <span className="viewtoggle" title={`ชีทใหญ่ ${grid.rows.length.toLocaleString()} แถว — ใช้มุมมองค้นหา`}>
                    <button className="on">🔍 ค้นหา</button>
                  </span>
                )}
                <button onClick={undo} disabled={!hist.length} title="ย้อนกลับ (Ctrl+Z)">↩ ย้อนกลับ</button>
                {dirty && <button className="ghost" onClick={revert} title="ทิ้งการแก้ทั้งหมด">คืนค่าเดิม</button>}
                {!specialView && <button onClick={addRow} disabled={!grid}>+ เพิ่มแถว</button>}
                {!specialView && <button onClick={() => setDlg({ name: '', at: null })} disabled={!grid}>+ เพิ่มคอลัมน์</button>}
                <button className="primary" onClick={save} disabled={!grid || saving || !dirty} title="บันทึก (Ctrl+S)">
                  {saving ? 'กำลังบันทึก...' : 'บันทึก'}
                </button>
              </div>
            </div>

            {grid && !specialView && (
              <>
                <div className="filterbar">
                  <input className="search" placeholder="🔍 ค้นหาทุกคอลัมน์..."
                    value={search} onChange={e => setSearch(e.target.value)} />
                  <span className="count">แสดง {visible.length} / {grid.rows.length} แถว</span>
                  {hasFilter
                    ? <button className="clearf" onClick={() => { setSearch(''); setFilters({}); setSort(null) }}>ล้างทั้งหมด</button>
                    : <span className="hint small" style={{ padding: 0 }}>คลิกหัวคอลัมน์ = เรียงลำดับ · ▾ = กรอง · ⋮ = เปลี่ยนชื่อ/ลบ</span>}
                </div>

                {(fchips.length > 0 || sort) && (
                  <div className="chips">
                    {sort && (
                      <span className="fchip sortchip">
                        เรียง <b>{grid.columns[sort.ci]}</b> {sort.dir === 'asc' ? '▲ น้อย→มาก' : '▼ มาก→น้อย'}
                        <button onClick={() => setSort(null)} title="เลิกเรียง">✕</button>
                      </span>
                    )}
                    {fchips.map(ci => {
                      const vals = Array.from(filters[ci])
                      const txt = vals.length <= 2 ? vals.map(valLabel).join(', ') : `${vals.length} ค่า`
                      return (
                        <span key={ci} className="fchip" title={vals.map(valLabel).join(', ')}>
                          <b>{grid.columns[ci]}</b>: {txt}
                          <button onClick={() => applyFilter(ci, null)} title="ล้างตัวกรองนี้">✕</button>
                        </span>
                      )
                    })}
                  </div>
                )}
              </>
            )}

            {sel.name === 'Target_Stock' && (
              <div className="msg note">
                ℹ️ คอลัมน์ <b>Match core, TARGET SCM, STOCK MIN, STOCK MAX, Stock 5 Week</b> คำนวณอัตโนมัติจาก <b>TARGET/MONTH</b> ตอนกดบันทึก — แก้ TARGET/MONTH แล้วค่าที่เหลือจะอัปเดตเอง
              </div>
            )}
            {msg && <div className="msg">{msg}</div>}
            {loading && <div className="hint">กำลังโหลด...</div>}

            {grid && calView && (
              <CalendarEditor
                grid={grid}
                dateCi={calInfo.dateCi}
                statusCi={calInfo.statusCi}
                weekCi={calInfo.weekCi}
                isChanged={isChanged}
                onToggle={(ri, val) => setCell(ri, calInfo.statusCi, String(val))}
              />
            )}

            {grid && lockView && (
              <LockMcEditor
                grid={grid}
                cols={lockInfo}
                opts={lockOpts}
                rids={ids.rid}
                setCell={setCell}
                addRow={addRow}
                delRow={delRow}
                isChanged={isChanged}
                onItemInput={setItemCell}
              />
            )}

            {grid && mcTotalView && (
              <McTotalEditor
                grid={grid}
                cols={mcTotalInfo}
                opts={lockOpts}
                rids={ids.rid}
                setCell={setCell}
                addRow={addRow}
                delRow={delRow}
                isChanged={isChanged}
                onWeekToInput={setMcWeekToCell}
              />
            )}

            {grid && s9View && (
              <S9Editor
                grid={grid}
                info={s9Info}
                rids={ids.rid}
                setCell={setCell}
                addRow={addRow}
                delRow={delRow}
                isChanged={isChanged}
                isNewRow={isNewRow}
              />
            )}

            {grid && configView && (
              <ConfigEditor
                grid={grid}
                info={configInfo}
                rids={ids.rid}
                setCell={setCell}
                addRow={addRow}
                delRow={delRow}
                isChanged={isChanged}
                isNewRow={isNewRow}
              />
            )}

            {grid && mastermcView && (
              <MasterMcTable
                grid={grid}
                info={mastermcInfo}
                rids={ids.rid}
                setCell={setCell}
                delRow={delRow}
                addRow={addRow}
                isChanged={isChanged}
                isNewRow={isNewRow}
              />
            )}

            {grid && bigView && (
              <LookupEditor
                grid={grid}
                rids={ids.rid}
                setCell={setCell}
                addRow={addRow}
                delRow={delRow}
                isChanged={isChanged}
                isNewRow={isNewRow}
              />
            )}

            {grid && !specialView && (
              <div className="gridwrap">
                <table className="grid" style={{ tableLayout: 'fixed', width: totalW }}>
                  <colgroup>
                    <col style={{ width: ROWNUM_W }} />
                    {grid.columns.map((_, ci) => <col key={ci} style={{ width: widths[ci] }} />)}
                    <col style={{ width: ACT_W }} />
                  </colgroup>
                  <thead>
                    <tr>
                      <th className="rownum">#</th>
                      {grid.columns.map((c, ci) => (
                        <th key={ci} className={ci === 0 ? 'frz' : ''}>
                          <div className="thcell">
                            {renameCi === ci ? (
                              <input className="thinput" autoFocus defaultValue={c}
                                onBlur={e => commitRename(ci, e.target.value)}
                                onKeyDown={e => {
                                  if (e.key === 'Enter') e.currentTarget.blur()
                                  else if (e.key === 'Escape') { e.currentTarget.value = c; e.currentTarget.blur() }
                                }} />
                            ) : (
                              <button className="thsort" onClick={() => headClick(ci)} onDoubleClick={() => headDouble(ci)}
                                title={`${c}\nคลิก = เรียงลำดับ · ดับเบิลคลิก = เปลี่ยนชื่อ`}>
                                <span className="thlabel">{c}</span>
                                {sort?.ci === ci && <span className="sarrow">{sort.dir === 'asc' ? '▲' : '▼'}</span>}
                              </button>
                            )}
                            <button tabIndex={-1} className={'funnel' + (filters[ci] ? ' on' : '')}
                              title="กรองคอลัมน์นี้" onClick={e => openColMenu(e, ci)}>▾</button>
                            <button tabIndex={-1} className="kebab" title="ตัวเลือกคอลัมน์"
                              onClick={e => openKebab(e, ci)}>⋮</button>
                            <span className="resizer" title="ลากเพื่อปรับความกว้าง (ดับเบิลคลิก = พอดีเนื้อหา)"
                              onPointerDown={e => startResize(e, ci)} onDoubleClick={() => autoFit(ci)} />
                          </div>
                        </th>
                      ))}
                      <th className="actcol"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {visible.map(({ row, idx }, vi) => (
                      <tr key={ids.rid[idx]} className={isNewRow(idx) ? 'newrow' : ''}>
                        <td className="rownum" title={isNewRow(idx) ? 'แถวใหม่ (ยังไม่บันทึก)' : ''}>{idx + 1}</td>
                        {row.map((cell, ci) => (
                          <td key={ci} className={(ci === 0 ? 'frz' : '') + (isChanged(idx, ci) ? ' chg' : '')}>
                            <input value={cell ?? ''} data-cell={`${vi}-${ci}`}
                              onChange={e => setCell(idx, ci, e.target.value)}
                              onKeyDown={e => onCellKey(e, vi, ci)} />
                          </td>
                        ))}
                        <td className="actcol">
                          <button tabIndex={-1} className="del" title="ลบแถว" onClick={() => delRow(idx)}>✕</button>
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

            {grid && !specialView && <div className="gridhelp">Enter / ↑ ↓ = ย้ายแถว · Tab = ช่องถัดไป · Ctrl+Z = ย้อนกลับ · Ctrl+S = บันทึก · <span className="chgdot" /> = ช่องที่แก้แล้วยังไม่บันทึก</div>}

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

            {menuCol && (
              <>
                <div className="filterbackdrop" onClick={() => setMenuCol(null)} />
                <div className="colmenu" style={{ top: menuCol.top, left: menuCol.left }}>
                  <div className="cmhead">{grid.columns[menuCol.ci]}</div>
                  <button onClick={() => { setRenameCi(menuCol.ci); setMenuCol(null) }}>✏️ เปลี่ยนชื่อคอลัมน์</button>
                  <button onClick={() => { setDlg({ name: '', at: menuCol.ci + 1 }); setMenuCol(null) }}>➕ แทรกคอลัมน์ทางขวา</button>
                  <button onClick={() => { autoFit(menuCol.ci); setMenuCol(null) }}>↔ ปรับความกว้างพอดีเนื้อหา</button>
                  <div className="cmsep" />
                  <button className="danger" onClick={() => delCol(menuCol.ci)}>🗑 ลบคอลัมน์นี้</button>
                </div>
              </>
            )}

            {dlg && (
              <div className="modal-backdrop" onClick={() => setDlg(null)}>
                <div className="mini-dlg" onClick={e => e.stopPropagation()}>
                  <h3>เพิ่มคอลัมน์</h3>
                  <input autoFocus placeholder="ชื่อคอลัมน์" value={dlg.name}
                    onChange={e => setDlg(d => ({ ...d, name: e.target.value }))}
                    onKeyDown={e => {
                      if (e.key === 'Enter' && dlg.name.trim()) addColAt(dlg.name.trim(), dlg.at)
                      else if (e.key === 'Escape') setDlg(null)
                    }} />
                  <p className="hint small" style={{ padding: 0 }}>
                    {dlg.at == null ? 'จะเพิ่มไว้ท้ายสุด' : `จะแทรกหลังคอลัมน์ "${grid.columns[dlg.at - 1]}"`}
                  </p>
                  <div className="dlgbtns">
                    <button onClick={() => setDlg(null)}>ยกเลิก</button>
                    <button className="primary" disabled={!dlg.name.trim()}
                      onClick={() => addColAt(dlg.name.trim(), dlg.at)}>เพิ่มคอลัมน์</button>
                  </div>
                </div>
              </div>
            )}
          </>
        )}
      </section>
    </div>
  )
}

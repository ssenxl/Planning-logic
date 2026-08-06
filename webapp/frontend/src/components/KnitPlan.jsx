import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import {
  ColumnFilter, columnFilterData, filterRows, norm,
  ROWNUM_W, isIdName, numericCols, fmtNum, autoColWidths, makeColResizer,
} from './ColumnFilter.jsx'
import { makeWorkDayResolver } from '../workday.js'
import PlanGantt, { LOAD_TYPES, BAR_FIELDS_DEFAULT, barFieldsFor, loadBarFields, saveBarFields } from './PlanGantt.jsx'
import OutsourceAdvisor from './OutsourceAdvisor.jsx'
import CylinderAdvisor from './CylinderAdvisor.jsx'

// ค่าความคลาดเคลื่อนที่ยอมรับได้เวลาเทียบยอดสั่ง (กก.)
// PRODUCE_QTY ถูกปัด "ลงเป็นพับเต็ม" (fold_round) ทีละสัปดาห์ → ออร์เดอร์ที่แบ่งวางหลายสัปดาห์
// เศษปัดพับจะสะสมกันได้เกิน 0.01 (เช่น ขาด 0.02 ทั้งที่วางครบ) ซึ่งไม่มีความหมายเชิงธุรกิจ
// ยอมได้ถึง 1 กก. เกินกว่านี้ = ขาด/เกินจริง ต้องเตือน
const QTY_TOL = 1

// อาร์เรย์ว่างค่าคงที่ — ส่งเป็น prop แทน [] สดๆ ไม่ให้ useMemo ฝั่ง PlanGantt คิดใหม่ทุกเรนเดอร์
const EMPTY_ROWS = []

// แปลงเวลาไฟล์แผน (mtime = epoch วินาที) → วันที่+เวลาแบบไทย เช่น "17 ก.ค. 2569 14:32 น."
// ใช้บอก user ว่าแผนที่กำลังดูอยู่รันเสร็จเมื่อไหร่ (mtime = เวลาที่ Planning.py เขียนไฟล์เสร็จ)
function fmtPlanTime(mtime) {
  if (!mtime) return ''
  const d = new Date(mtime * 1000)
  const date = d.toLocaleDateString('th-TH', { day: 'numeric', month: 'short', year: 'numeric' })
  const time = d.toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit' })
  return `${date} ${time} น.`
}

// คอลัมน์ที่ระบุ "ออร์เดอร์ 1 รายการ" — ค่าคงที่ภายในออร์เดอร์เดียวกัน แต่ต่างกันระหว่างออร์เดอร์
// SC เดียวมีได้หลายออร์เดอร์: ต่างที่ PO / ORDERS_QTY / สัปดาห์ FG (FG_WEEK)
// ไม่รวม PLAN_WEEK เพราะออร์เดอร์เดียวถูกแบ่งวางได้หลายสัปดาห์ (ต้องจับรวมกัน)
// แก้ที่นี่ที่เดียวถ้าพบว่ายังมีคอลัมน์อื่นที่แยกออร์เดอร์
const ORDER_KEY_COLS = ['SC_SO_NO', 'SC_LINE_ID', 'PO_NO', 'ORDERS_QTY', 'FG_WEEK']
function orderKeyOf(row, columns) {
  return ORDER_KEY_COLS.map(c => { const i = columns.indexOf(c); return i >= 0 ? norm(row[i]) : '' }).join('|')
}

// คอลัมน์ที่มี dropdown กรองด่วนบนแถบค้นหา (ใช้ตัวกรองชุดเดียวกับปุ่ม ▾ ที่หัวคอลัมน์)
const QUICK_COLS = [
  { col: 'CAT', label: 'CAT' },
  { col: 'MC_GUAGE', label: 'Gauge' },
]

// ชีทที่ไม่ต้องโชว์ใน dropdown ของหน้าแผนผลิต — ซ่อนแค่ UI เท่านั้น
// ไฟล์ Excel ยังมีชีทครบ (ดาวน์โหลดได้) และ backend ยังอ่านไปคำนวณ cap/Change Cylinder ตามเดิม
const HIDDEN_SHEETS = new Set(['REMAININGJOBS', 'CYLINDERCHANGE'])
// ตัด _ / ช่องว่าง / ขีด แล้วทำเป็นตัวใหญ่ — กันชื่อชีทเขียนต่างกันเล็กน้อย (REMAINING JOBS ฯลฯ)
const sheetKey = s => String(s || '').toUpperCase().replace(/[\s_-]/g, '')

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

// ยิง fetch ซ้ำถ้าพลาด หรือได้ค่าที่ยัง "ไม่พร้อม" (ตรวจด้วย ok) — กัน ava/load ค้างว่างถาวร
// เพราะพลาดจังหวะเดียวตอนโหลด: server อ่าน Excel ก้อนใหญ่ตอน cold start อาจตอบช้า/พลาด
// คำขอที่ยิงพร้อมกัน แล้วโค้ดเดิม catch → setAva({}) ทำให้เครื่อง/คอลัมน์ 🔒 หายทั้งที่ข้อมูลมี
async function retry(fn, { tries = 3, delay = 700, ok = () => true } = {}) {
  let last
  for (let i = 0; i < tries; i++) {
    try {
      last = await fn()
      if (ok(last) || i === tries - 1) return last
    } catch (e) {
      if (i === tries - 1) throw e
    }
    await new Promise(r => setTimeout(r, delay * (i + 1)))
  }
  return last
}

export default function KnitPlan({ active = true }) {
  const [meta, setMeta] = useState(null)         // ข้อมูลไฟล์แผนล่าสุด
  const [grid, setGrid] = useState(null)          // { sheet, sheets, columns, rows, name, mtime }
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [dirty, setDirty] = useState(false)
  const [msg, setMsg] = useState('')
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState({})
  const [openCol, setOpenCol] = useState(null)
  const [colW, setColW] = useState({})       // ความกว้างคอลัมน์ (ผู้ใช้ลากปรับได้)
  const [editKey, setEditKey] = useState(null) // ช่องที่กำลังแก้ (โชว์ค่าดิบไม่มี comma)
  const [runStatus, setRunStatus] = useState({})
  const [showGantt, setShowGantt] = useState(true)
  // overlay item จาก booking (แผนเก่า) บน Gantt: 'off' | 'all' (ทุก item) | 'plan' (เฉพาะ item ที่ทำแผนวันนี้)
  const [bookingMode, setBookingMode] = useState('off')
  const [bookingItems, setBookingItems] = useState([])   // item จาก booking DETAIL ต่อ (สัปดาห์×CAT×เกจ×เครื่อง)
  const [bookingUnpick, setBookingUnpick] = useState(() => new Set())  // ITEM_CODE ที่ user "ติ๊กออก" ในโหมด plan (ว่าง = ติ๊กหมด)
  const [showBkPick, setShowBkPick] = useState(false)    // เปิด dropdown เลือก item ในโหมด plan
  const [bkSearch, setBkSearch] = useState('')           // คำค้นใน dropdown เลือก item (กรองการแสดงผลอย่างเดียว)
  const bkPickRef = useRef(null)
  // พับ dropdown เลือก item เมื่อคลิกนอกกล่อง หรือกด Esc (ไม่ล้างคำค้น เพราะคำค้นยังกรอง Gantt อยู่)
  useEffect(() => {
    if (!showBkPick) return
    function onDown(e) {
      if (bkPickRef.current && !bkPickRef.current.contains(e.target)) setShowBkPick(false)
    }
    function onKey(e) { if (e.key === 'Escape') setShowBkPick(false) }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [showBkPick])
  const [loadFilter, setLoadFilter] = useState(null)   // กรองประเภทโหลด Gantt (ปุ่มอยู่แถบบน)
  const [barFields, setBarFields] = useState(loadBarFields)   // ฟิลด์ที่โชว์บนบล็อก Gantt (ปุ่มอยู่แถบ Gantt)
  const [showFieldBar, setShowFieldBar] = useState(false)
  useEffect(() => { saveBarFields(barFields) }, [barFields])
  // chip "ข้อมูลบนบล็อก" ที่ใช้ได้กับชีทที่กำลังดู — ชีทต่างกันมีคอลัมน์ไม่เหมือนกัน
  // (เช่น SETUP_TRACKING ไม่มี PO/RDD/ลูกค้า/สี/เนื้อผ้า → ติ๊กแล้วไม่มีอะไรขึ้น จึงไม่โชว์ chip)
  const sheetBarFields = useMemo(() => barFieldsFor(grid?.columns || []), [grid?.columns])
  const [showOutsource, setShowOutsource] = useState(false)
  const [showCylinder, setShowCylinder] = useState(false)
  // แถวที่เลือกจากการคลิกบล็อก Gantt (idx ของแถวในชีท) → เปิด modal ตาราง + การ์ดคู่กัน
  const [selJob, setSelJob] = useState(null)
  const modalRowRef = useRef(null)
  // เปิด modal แล้วเลื่อนตารางไปหาแถวที่คลิก + ไฮไลต์
  useEffect(() => {
    if (selJob == null) return
    const t = setTimeout(() => modalRowRef.current?.scrollIntoView({ block: 'center' }), 40)
    return () => clearTimeout(t)
  }, [selJob])
  const [load, setLoad] = useState({})
  // รายการ job ที่ตั้งเครื่องใหม่ (ชีท SETUP_TRACKING) — ใช้ในการ์ดที่เปิดจากแถบโหลดบน Gantt
  const [setupJobs, setSetupJobs] = useState([])
  const [ava, setAva] = useState({})
  // map เครื่อง→พูล (เช่น SKP vs SKPTA/SKPLE) สำหรับหา ava/reserved ต่อพูล
  const [poolMap, setPoolMap] = useState({})
  // เครื่องที่ booking ถักไอเทมนั้นอยู่แล้ว ต่อ (สัปดาห์ × ITEM|MC_GROUP|GUAGE)
  const [bookingMc, setBookingMc] = useState({})
  // payload วันทำงานตามกลุ่มเครื่อง (ชีท Work Day + ปฏิทินสด) — ใช้คำนวณวันทำงานราย (mc,gauge,week) ตอนลากงาน
  const [workdayData, setWorkdayData] = useState(null)
  // ชีท Program ใน MasterMC → { ITEM_CODE: [TEAM, ...] } ที่ต้องโชว์เป็นตัวหนังสือสีน้ำเงิน
  const [program, setProgram] = useState({})
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
    try { setLoad(await retry(() => api.planLoad())) } catch { setLoad({}) }
    // รายการ job รายตัวของแถบโหลด (การ์ดที่เปิดจากการคลิกช่องแถบโหลด) — มาจากไฟล์แผนเดียวกัน
    try { setSetupJobs(await retry(() => api.planSetupJobs())) } catch { setSetupJobs([]) }
  }
  async function loadAva() {
    // ava ควรมีข้อมูลเมื่อมีไฟล์แผน → ถ้าได้ว่าง ลองซ้ำก่อน (กันเครื่อง/คอลัมน์ 🔒 หายชั่วคราว)
    const notEmpty = o => o && Object.keys(o).length > 0
    try { setAva(await retry(() => api.planAva(), { ok: notEmpty })) } catch { setAva({}) }
    try { setPoolMap(await retry(() => api.planPoolMap())) } catch { setPoolMap({}) }
    try { setBookingMc(await retry(() => api.planBookingMc())) } catch { setBookingMc({}) }
    try { setBookingItems(await retry(() => api.planBookingItems())) } catch { setBookingItems([]) }
    try { setWorkdayData(await retry(() => api.workday())) } catch { setWorkdayData(null) }
    try { setProgram(await retry(() => api.planProgram())) } catch { setProgram({}) }
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
  }, [])

  // polling สถานะการรัน — ทำงานเฉพาะตอนหน้าแผนถูกแสดง (active) เพื่อไม่ให้เปลืองตอนถูกซ่อน
  useEffect(() => {
    if (!active) return
    loadRunStatus()
    const t = setInterval(loadRunStatus, 2000)
    return () => clearInterval(t)
  }, [active])

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
      const r = await api.run('full')
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) { setMsg('สั่งรันไม่ได้: ' + e.message) }
  }

  // หยุดงานที่กำลังรัน — backend จะ kill ทั้ง process tree (run_all.py + step ลูก)
  async function stopRun() {
    if (!window.confirm('ยืนยันหยุดการรัน? งานที่กำลังทำอยู่จะถูกยกเลิกทันที')) return
    setMsg('')
    try {
      const r = await api.runStop()
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) { setMsg('สั่งหยุดไม่ได้: ' + e.message) }
  }

  async function changeSheet(sheet) {
    if (dirty && !window.confirm('มีการแก้ไขที่ยังไม่บันทึก จะทิ้งแล้วเปลี่ยนชีทไหม?')) return
    await loadSheet(sheet)
  }

  // ทุกออร์เดอร์ (นิยามด้วย ORDER_KEY_COLS) ที่วางไม่ครบ/เกิน — ใช้เตือนก่อนบันทึก
  function ordersOutOfBalance() {
    if (!grid) return []
    const cols = grid.columns
    const oq = cols.indexOf('ORDERS_QTY'), pq = cols.indexOf('PRODUCE_QTY')
    const sc = cols.indexOf('SC_SO_NO')
    if (oq < 0 || pq < 0) return []
    const g = new Map()
    grid.rows.forEach(r => {
      const ordered = Number(norm(r[oq])) || 0
      if (ordered <= 0) return
      const key = orderKeyOf(r, cols)
      const cur = g.get(key) || { key, sc: sc >= 0 ? norm(r[sc]) : '', ordered: 0, placed: 0 }
      cur.ordered = Math.max(cur.ordered, ordered)
      cur.placed += Number(norm(r[pq])) || 0
      g.set(key, cur)
    })
    return [...g.values()]
      .map(o => ({ ...o, diff: Math.round((o.ordered - o.placed) * 100) / 100 }))
      .filter(o => Math.abs(o.diff) > QTY_TOL)
  }

  async function save() {
    if (!grid) return
    const bad = ordersOutOfBalance()
    if (bad.length) {
      const lines = bad.slice(0, 12).map(o =>
        `• SC ${o.sc || '-'}: สั่ง ${o.ordered.toLocaleString()} วาง ${o.placed.toLocaleString()} `
        + (o.diff > 0 ? `(ขาด ${o.diff.toLocaleString()})` : `(เกิน ${(-o.diff).toLocaleString()})`)).join('\n')
      const more = bad.length > 12 ? `\n…และอีก ${bad.length - 12} ออร์เดอร์` : ''
      if (!window.confirm(`⚠ มี ${bad.length} ออร์เดอร์ที่วางไม่ตรงยอดสั่ง (ORDERS_QTY):\n\n${lines}${more}\n\nยืนยันบันทึกทั้งที่ยอดไม่ตรงหรือไม่?`)) return
    }
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
  // resolver วันทำงานราย (mc_group, gauge, week) จากชีท Work Day (แหล่งเดียว — user ปรับเอง)
  // ตรงกับ WorkDay.get_working_days() ใน backend: min(ค่าในแผง, วันที่ปฏิทินเปิด), รวมยุบสัปดาห์
  // ปฏิทินมากับ payload /api/workday (อ่านสดจาก Calendar.xlsx บน server)
  const wdResolver = useMemo(
    () => (workdayData ? makeWorkDayResolver(workdayData) : null),
    [workdayData]
  )

  // วันทำงานจริงของแถวในสัปดาห์ w — อ่านจากชีท Work Day ตาม (MC_GROUP, MC_GUAGE) ของแถว
  function actualWdAt(mc, gauge, w) {
    if (!wdResolver) return null            // ยังโหลด workday ไม่เสร็จ
    return wdResolver.workDays(mc, gauge, w)
  }

  // กำลังผลิตของแถวถ้าอยู่สัปดาห์ w (กก.) โดยใช้เครื่องชุดเดิมที่แถวถืออยู่
  //   เครื่อง carry ได้วันทำงานเต็ม / เครื่อง setup ใหม่ได้ (วันทำงาน − setup ต่อเครื่อง)
  // ⚠️ ห้ามคำนวณ ACTUAL_MC ใหม่จาก PRODUCE_QTY — เครื่อง carry ถูกกำหนดโดยสัปดาห์ก่อนหน้า
  //    และอาจถือเครื่องเกินที่งานต้องการ ตัวเลขที่ถูกต้องต้องรันแผนใหม่เท่านั้น
  function capacityAt(gv, mc, gauge, w) {
    const awd = actualWdAt(mc, gauge, w)
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
    const gs = (n) => { const k = ix(n); return k >= 0 ? norm(row[k]) : '' }
    const set = (n, v) => { const k = ix(n); if (k >= 0) row[k] = v }

    const w = Number(week)
    const mc = gs('MC_GROUP'), gauge = gs('MC_GUAGE')
    const awd = actualWdAt(mc, gauge, w)
    if (awd == null) return
    const n = gv('NEW_MC')
    const avail = n > 0 ? Math.max(0.5, awd - gv('SETUP_DAYS') / n) : awd

    set('CALENDAR_WORKING_DAYS', Number(wdResolver?.calDays(w)) || 0)
    set('FACTORY_WORKING_DAYS', wdResolver ? wdResolver.workDays(mc, gauge, null) : awd)
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

    // วันทำงานแต่ละสัปดาห์ไม่เท่ากัน (ตามชีท Work Day) → เครื่องชุดเดิมอาจผลิตไม่ทัน
    const oldRow = grid.rows[ri]
    const gvOld = (n) => { const k = grid.columns.indexOf(n); return k >= 0 ? (Number(norm(oldRow[k])) || 0) : 0 }
    const sharedRow = gvOld('MC_SHARED') > 0
    const gsOld = (n) => { const k = grid.columns.indexOf(n); return k >= 0 ? norm(oldRow[k]) : '' }
    const cap = sharedRow ? null : capacityAt(gvOld, gsOld('MC_GROUP'), gsOld('MC_GUAGE'), Number(week))
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

  // จำนวนสูงสุดที่แถว ri วางได้ = ORDERS_QTY ของออร์เดอร์ − ผลรวม PRODUCE_QTY ของแถวอื่นในออร์เดอร์เดียวกัน
  // ออร์เดอร์นิยามด้วย ORDER_KEY_COLS → ห้ามวางรวมเกินยอดสั่ง
  // คืน Infinity ถ้าไม่มี ORDERS_QTY (ปล่อยผ่าน)
  function maxQtyForRow(ri) {
    if (!grid) return Infinity
    const cols = grid.columns
    const oq = cols.indexOf('ORDERS_QTY'), pq = cols.indexOf('PRODUCE_QTY')
    if (oq < 0 || pq < 0) return Infinity
    const row = grid.rows[ri]
    const ordered = Number(norm(row[oq]))
    if (!Number.isFinite(ordered) || ordered <= 0) return Infinity
    const k = orderKeyOf(row, cols)
    let others = 0
    grid.rows.forEach((r, i) => { if (i !== ri && orderKeyOf(r, cols) === k) others += Number(norm(r[pq])) || 0 })
    return Math.max(0, Math.round((ordered - others) * 100) / 100)
  }

  // ถ้า PRODUCE_QTY ของแถว ri เกินยอดสั่ง → เตือนแล้วปรับลงมาเท่าที่วางได้ (คืน true ถ้าปรับ)
  function clampOrderQty(ri) {
    if (!grid) return false
    const pq = grid.columns.indexOf('PRODUCE_QTY')
    if (pq < 0) return false
    const cur = Number(norm(grid.rows[ri][pq])) || 0
    const max = maxQtyForRow(ri)
    if (cur > max + QTY_TOL) {
      window.alert(`วางเกินยอดสั่ง (ORDERS_QTY) ไม่ได้\nออร์เดอร์นี้วางแถวนี้ได้สูงสุด ${max.toLocaleString()} กก. — ปรับลงให้อัตโนมัติ`)
      setCell(ri, pq, max)
      return true
    }
    return false
  }

  // double click ตัวเลขบนบล็อก Gantt → แก้ PRODUCE_QTY ของแถวนั้น (ตารางด้านล่างอัปเดตตาม + ต้องกดบันทึก)
  function editQty(ri, qty) {
    if (!grid) return
    const qci = grid.columns.indexOf('PRODUCE_QTY')
    if (qci < 0) return
    const max = maxQtyForRow(ri)
    if (qty > max + QTY_TOL) {
      window.alert(`วางเกินยอดสั่ง (ORDERS_QTY) ไม่ได้\nออร์เดอร์นี้วางแถวนี้ได้สูงสุด ${max.toLocaleString()} กก. (กรอก ${qty.toLocaleString()})`)
      return
    }
    setCell(ri, qci, qty)
  }

  // แบ่งงาน 1 บล็อกออกเป็น 2 สัปดาห์: ลด PRODUCE_QTY ของแถวเดิม + สร้างแถวใหม่ (copy) จำนวนที่แบ่ง
  // วางที่สัปดาห์ปลายทาง แล้ว recalc คอลัมน์ที่ผูกกับสัปดาห์ (วันทำงาน/booking) เหมือนตอนลาก
  // ⚠️ ไม่คำนวณ ACTUAL_MC/SETUP ใหม่ — copy มาตามเดิม (ต้องรันแผนใหม่เพื่อได้เลขเครื่องที่ถูกต้อง)
  function splitJob(ri, splitQty, week) {
    if (!grid) return
    const qci = grid.columns.indexOf('PRODUCE_QTY')
    const wci = grid.columns.indexOf('PLAN_WEEK')
    if (qci < 0) return
    const row = grid.rows[ri]
    const cur = Number(norm(row[qci])) || 0
    const q = Math.round((Number(splitQty) || 0) * 100) / 100
    if (!(q > 0) || q >= cur) {
      window.alert(`แบ่งไม่ได้: จำนวนที่แบ่ง (${q}) ต้องมากกว่า 0 และน้อยกว่าจำนวนก้อนเดิม (${cur})`)
      return
    }
    const ici = grid.columns.indexOf('ITEM_CODE')
    const item = ici >= 0 ? norm(row[ici]) : ''
    const left = Math.round((cur - q) * 100) / 100
    const msg = `แบ่งงาน ${item}: ${q} กก. → สัปดาห์ ${week} (เหลือ ${left} กก. ที่สัปดาห์เดิม)\n\n`
      + `⚠ จำนวนเครื่อง/setup จะ copy มาตามเดิม ไม่คำนวณใหม่ให้ — ต้องรันแผนใหม่เพื่อได้เลขเครื่องที่ถูกต้อง\n\n`
      + `ยืนยันแบ่งหรือไม่?`
    if (!window.confirm(msg)) return

    setGrid(g => {
      const rows = g.rows.slice()
      const orig = rows[ri].slice()
      orig[qci] = left
      const copy = rows[ri].slice()
      copy[qci] = q
      if (wci >= 0) copy[wci] = week
      recalcRowForWeek(copy, g.columns, week)
      rows[ri] = orig
      rows.splice(ri + 1, 0, copy)
      return { ...g, rows }
    })
    setDirty(true)
  }

  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol])

  const visible = useMemo(() => grid ? filterRows(grid, search, filters) : [], [grid, search, filters])

  // ── ชีท Program (Master) → ทำตัวหนังสือเป็นสีน้ำเงิน ────────────────────────
  // เงื่อนไข: ITEM_CODE ของแถว ต้องตรงกับชีท Program "และ" ทีมในคอลัมน์ TEAM ต้องตรงด้วย
  // ⚠ ไฟล์แผนเก่า/ชีทที่ยังไม่มีคอลัมน์ TEAM (สร้างก่อนฟีเจอร์นี้) → เทียบด้วย ITEM อย่างเดียวไปก่อน
  //    ไม่งั้นจะไม่มีอะไรเหลืองเลยจนกว่าจะรันแผนใหม่
  const progIdx = useMemo(() => ({
    item: grid ? grid.columns.indexOf('ITEM_CODE') : -1,
    team: grid ? grid.columns.indexOf('TEAM') : -1,
  }), [grid?.columns])

  // คืนชื่อทีมที่ match (ใช้เป็นทั้งเงื่อนไขสีและข้อความ tooltip) — ไม่ match = ''
  function programTeam(row) {
    const { item: ii, team: ti } = progIdx
    if (ii < 0 || !row) return ''
    const teams = program[norm(row[ii]).trim().toUpperCase()]
    if (!teams || !teams.length) return ''
    if (ti < 0) return teams.join(' , ')
    // ทั้งสองฝั่งเก็บหลายทีมคั่นด้วย , ได้ → ตรงกันสักทีมก็ถือว่า match
    const mine = norm(row[ti]).split(',').map(s => s.trim().toUpperCase()).filter(Boolean)
    if (!mine.length) return ''
    const hit = teams.filter(t => mine.includes(String(t).trim().toUpperCase()))
    return hit.length ? hit.join(' , ') : ''
  }

  // { idx แถว → "ทีม" } ของแถวที่เข้าเงื่อนไข — ใช้ทั้งตารางและ Gantt (คิดที่เดียว)
  const programRows = useMemo(() => {
    const m = {}
    if (!grid) return m
    grid.rows.forEach((row, idx) => { const t = programTeam(row); if (t) m[idx] = t })
    return m
  }, [grid, program, progIdx])

  // ITEM_CODE ที่กำลังทำแผนวันนี้ (จากแผนปัจจุบัน) ที่ "มีประวัติใน booking" — ใช้ทำ dropdown โหมด plan
  const bookingHistCodes = useMemo(() => {
    if (!grid) return []
    const ici = grid.columns.indexOf('ITEM_CODE')
    if (ici < 0) return []
    const planSet = new Set(grid.rows.map(r => norm(r[ici]).toUpperCase()).filter(Boolean))
    const bk = new Set(bookingItems.map(b => String(b.item).toUpperCase()))
    return [...planSet].filter(c => bk.has(c)).sort()
  }, [grid, bookingItems])

  // รายการที่ผ่านคำค้น (ว่าง = ทั้งหมด) — ใช้ทั้งรายการใน dropdown และเป็นขอบเขตของ overlay บน Gantt
  const bkShownCodes = useMemo(() => {
    const q = bkSearch.trim().toUpperCase()
    return q ? bookingHistCodes.filter(c => c.toUpperCase().includes(q)) : bookingHistCodes
  }, [bookingHistCodes, bkSearch])

  // ชุด ITEM_CODE ที่จะโชว์ประวัติในโหมด plan = ตัวที่ผ่านคำค้น − ตัวที่ user ติ๊กออก
  const bookingPick = useMemo(
    () => new Set(bkShownCodes.filter(c => !bookingUnpick.has(c))),
    [bkShownCodes, bookingUnpick])

  // คอลัมน์ตัวเลข (sample 200 แถวแรกพอ — ตารางนี้แก้ไขได้ คำนวณใหม่ทุกครั้งที่พิมพ์)
  const numCols = useMemo(() => numericCols(grid, 200), [grid])

  // ค่าที่โชว์ในช่อง: ตัวเลข (ไม่ใช่รหัส) ใส่ comma เฉพาะตอน "ไม่ได้แก้ช่องนี้"
  function displayCell(cell, ci, key) {
    if (editKey === key) return norm(cell)
    if (numCols.has(ci) && !isIdName(grid.columns[ci])) return fmtNum(cell)
    return norm(cell)
  }

  // ความกว้างเริ่มต้น — ตั้งครั้งเดียวต่อชุดคอลัมน์ (ไม่ reset ตอนพิมพ์แก้เซลล์)
  const gridKey = grid ? `${grid.name}|${grid.sheet}|${grid.columns.length}` : ''
  useEffect(() => { setColW(autoColWidths(grid)) }, [gridKey])

  // ชีทที่โชว์ใน dropdown — ตัด HIDDEN_SHEETS ออก แต่ถ้ากำลังเปิดชีทที่ซ่อนอยู่ (เช่นค้างจากก่อนหน้า)
  // ยังคงไว้ในลิสต์ ไม่งั้น select จะไม่มี option ที่ตรงกับค่าปัจจุบัน
  const sheetOptions = grid?.sheets
    ? grid.sheets.filter(s => !HIDDEN_SHEETS.has(sheetKey(s)) || s === grid.sheet)
    : []

  const startResize = makeColResizer(colW, setColW)
  // มีคอลัมน์เลขแถวหัว-ท้าย (# และปุ่มลบ) รวม ROWNUM_W × 2
  const totalW = grid
    ? ROWNUM_W * 2 + grid.columns.reduce((s, _, ci) => s + (colW[ci] || 120), 0)
    : 0

  // dropdown กรองด่วน — ค่าที่เลือกได้ cascade ตามตัวกรองคอลัมน์อื่น + คำค้นหา (เหมือนป็อปอัพ ▾)
  const quickCols = useMemo(() => {
    if (!grid) return []
    return QUICK_COLS.map(({ col, label }) => {
      const ci = grid.columns.indexOf(col)
      if (ci < 0) return null
      const { available } = columnFilterData(grid, filters, search, ci)
      const values = available
        .map(a => a.value)
        .sort((a, b) => String(a).localeCompare(String(b), 'th', { numeric: true }))
      const sel = filters[ci]
      // dropdown เลือกได้ทีละค่า — ถ้าถูกกรองหลายค่าจากปุ่ม ▾ ให้แสดงเป็น "(หลายค่า)"
      const value = !sel ? '' : (sel.size === 1 ? Array.from(sel)[0] : '__multi__')
      return { col, label, ci, values, value }
    }).filter(Boolean)
  }, [grid, filters, search])

  function setQuickFilter(ci, v) {
    if (v === '__multi__') return
    applyFilter(ci, v === '' ? null : new Set([v]))
  }

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

  // ตารางแผน (ใช้ซ้ำทั้งท้ายหน้า + ใน modal ที่เด้งตอนคลิกบล็อก Gantt)
  // highlight = idx แถวที่จะไฮไลต์/ผูก ref ไว้เลื่อนหา (เฉพาะใน modal)
  // rowsList = ชุดแถวที่จะโชว์ (default = ทั้งหมดที่ผ่านตัวกรอง; modal ส่งเฉพาะแถวของ item ที่คลิก)
  // allowDelete = false → ตัดคอลัมน์ปุ่มลบทิ้งทั้งคอลัมน์ (ชีทรายงานอย่าง UNPLANNED ห้ามลบแถว)
  const renderGrid = (highlight = null, rowsList = visible, allowDelete = true) => (
    <table className="grid" style={{ tableLayout: 'fixed', width: allowDelete ? totalW : totalW - ROWNUM_W }}>
      <colgroup>
        <col style={{ width: ROWNUM_W }} />
        {grid.columns.map((_, ci) => (
          <col key={ci} style={{ width: colW[ci] || 120 }} />
        ))}
        {allowDelete && <col style={{ width: ROWNUM_W }} />}
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
          {allowDelete && <th className="rownum actcol"></th>}
        </tr>
      </thead>
      <tbody>
        {rowsList.map(({ row, idx }) => (
          <tr key={idx}
            ref={highlight === idx ? modalRowRef : null}
            title={programRows[idx] ? `Program: ทีม ${programRows[idx]}` : undefined}
            className={highlight === idx ? 'rowsel' : undefined}>
            <td className="rownum">{idx + 1}</td>
            {row.map((cell, ci) => {
              const key = `${idx}:${ci}`
              return (
                <td key={ci} className={(ci === 0 ? 'frozen' : '') + (numCols.has(ci) ? ' num' : '')}>
                  <input
                    value={displayCell(cell, ci, key)}
                    onChange={e => setCell(idx, ci, e.target.value)}
                    onFocus={() => setEditKey(key)}
                    onBlur={() => { setEditKey(null); if (grid.columns[ci] === 'PRODUCE_QTY') clampOrderQty(idx) }}
                  />
                </td>
              )
            })}
            {allowDelete && (
              <td className="rownum actcol">
                <button className="del" title="ลบแถว" onClick={() => delRow(idx)}>✕</button>
              </td>
            )}
          </tr>
        ))}
        {!rowsList.length && (
          <tr><td className="rownum"></td>
            <td colSpan={grid.columns.length + (allowDelete ? 1 : 0)} className="hint">ไม่มีแถวตรงตัวกรอง</td>
          </tr>
        )}
      </tbody>
    </table>
  )

  return (
    <div className="knitplan">
      <div className="editbar plan-head">
        <h2>แผนผลิต {dirty && <span className="dot">●</span>}</h2>
        <div className="actions">
          {sheetOptions.length > 1 && (
            <select value={grid.sheet} onChange={e => changeSheet(e.target.value)}>
              {sheetOptions.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          )}
          <button className="primary" onClick={runPlan} disabled={isRunning}>▶ รันแผนใหม่</button>
          <button className="outsource-btn" onClick={() => setShowOutsource(true)}>🧵 จ้างทอ (AI)</button>
          <button className="cylinder-btn" onClick={() => setShowCylinder(true)}>🔩 Change Cylinder</button>
          <button onClick={save} disabled={!grid || saving || !dirty}>
            {saving ? 'กำลังบันทึก...' : '💾 บันทึก'}
          </button>
          {meta?.exists && <a className="dl" href={api.planDownloadUrl(meta.mtime)}>⬇ ดาวน์โหลด Excel</a>}
          <button onClick={refresh}>รีเฟรช</button>
          {isRunning && <button className="stopbtn" onClick={stopRun}>⛔ หยุดรัน</button>}
          <span className={'badge ' + (isRunning ? 'run' : 'idle')}>{runLabel}</span>
          {isRunning && runStatus.progress != null && <small className="run-hint">ความคืบหน้า {runStatus.progress}%</small>}
          {meta?.exists && meta.mtime && <small className="run-hint">แผนล่าสุด: {fmtPlanTime(meta.mtime)}</small>}
        </div>
      </div>

      {grid && (
        <div className="filterbar">
          <input className="search" placeholder="🔍 ค้นหาทุกคอลัมน์..."
            value={search} onChange={e => setSearch(e.target.value)} />
          {quickCols.map(q => (
            <label key={q.col} className={'quickf' + (filters[q.ci] ? ' on' : '')}>
              <span>{q.label}</span>
              <select value={q.value} onChange={e => setQuickFilter(q.ci, e.target.value)}>
                <option value="">ทั้งหมด</option>
                {q.value === '__multi__' && <option value="__multi__">(หลายค่า — กด ▾ ที่หัวคอลัมน์)</option>}
                {q.values.map(v => (
                  <option key={v} value={v}>{v === '' ? '(ว่าง)' : v}</option>
                ))}
              </select>
            </label>
          ))}
          {!!hasFilter && (
            <button className="clearf" onClick={() => { setSearch(''); setFilters({}) }}>ล้างตัวกรองทั้งหมด</button>
          )}
          {ganttReady && (
            <div className="gload-filter">
              <span className="gload-filter-label">Select</span>
              <button className={'gfilter-btn' + (loadFilter === null ? ' active' : '')}
                onClick={() => setLoadFilter(null)}>ทั้งหมด</button>
              {LOAD_TYPES.map(t => (
                <button key={t.key}
                  className={'gfilter-btn' + (loadFilter === t.key ? ' active' : '')}
                  onClick={() => setLoadFilter(f => (f === t.key ? null : t.key))}>
                  {t.long || t.label}
                </button>
              ))}
            </div>
          )}
          {ganttReady && showGantt && (
            <span className="gbooking-toggle" title="overlay item จาก History แผนเดิม (ย้อนหลัง 5 week) บน Gantt — ดูอย่างเดียว ลาก/แก้ไม่ได้">
              <span className="gbk-label">History แผนเดิม (ย้อนหลัง 5 week)</span>
              <button className={'gbk-btn' + (bookingMode === 'off' ? ' on' : '')}
                onClick={() => { setBookingMode('off'); setShowBkPick(false); setBkSearch('') }}>ปิด</button>
              <button className={'gbk-btn' + (bookingMode === 'all' ? ' on' : '')}
                onClick={() => { setBookingMode('all'); setShowBkPick(false); setBkSearch('') }}
                title="ทุก item ทุกสัปดาห์ที่มีใน booking + โชว์ทุกกลุ่มเครื่อง/CAT ใน MasterMC แม้ยังไม่มีงาน">Item (ทั้งหมด)</button>
              <button className={'gbk-btn' + (bookingMode === 'plan' ? ' on' : '')}
                onClick={() => setBookingMode('plan')}
                title="เฉพาะ item ที่กำลังทำแผนวันนี้ ที่มีประวัติใน booking">Item (วางแผนวันนี้)</button>
              {bookingMode === 'plan' && (
                <span className="gbk-pick" ref={bkPickRef}>
                  <button className={'gbk-btn gbk-pick-toggle' + (bkSearch.trim() ? ' searching' : '')}
                    onClick={() => setShowBkPick(s => !s)}
                    title="เลือกว่าจะดูประวัติของ item ไหนบ้าง">
                    เลือก item ({bookingPick.size}/{bkShownCodes.length}
                    {bkSearch.trim() ? ` จาก "${bkSearch.trim()}"` : ''}) ▾
                  </button>
                  {showBkPick && (
                    <div className="gbk-pick-list">
                      {bookingHistCodes.length === 0 && (
                        <div className="gbk-pick-empty">ไม่มี item ในแผนที่มีประวัติใน booking</div>
                      )}
                      {bookingHistCodes.length > 0 && (
                        <div className="gbk-pick-head">
                          {/* คำค้นกรอง overlay บน Gantt ด้วย จึงค้างไว้ตอนปิด dropdown — ล้างด้วยปุ่ม ✕ */}
                          <span className="gbk-pick-searchwrap">
                            <input className="gbk-pick-search" autoFocus type="text"
                              placeholder="🔍 พิมพ์ค้นหา item..."
                              value={bkSearch} onChange={e => setBkSearch(e.target.value)} />
                            {!!bkSearch && (
                              <button className="gbk-pick-clear" title="ล้างคำค้น"
                                onClick={() => setBkSearch('')}>✕</button>
                            )}
                          </span>
                          <div className="gbk-pick-actions">
                            {/* ทำงานกับเฉพาะรายการที่ค้นเจอ — ไม่ได้พิมพ์ค้นหา = ทั้งหมด */}
                            <button onClick={() => setBookingUnpick(s => {
                              const n = new Set(s); bkShownCodes.forEach(c => n.delete(c)); return n
                            })}>All</button>
                            <button onClick={() => setBookingUnpick(s => {
                              const n = new Set(s); bkShownCodes.forEach(c => n.add(c)); return n
                            })}>ล้างทั้งหมด</button>
                          </div>
                        </div>
                      )}
                      {bookingHistCodes.length > 0 && bkShownCodes.length === 0 && (
                        <div className="gbk-pick-empty">ไม่พบ item ที่ค้นหา</div>
                      )}
                      {bkShownCodes.map(c => (
                        <label key={c} className={'gbk-pick-item' + (bookingUnpick.has(c) ? '' : ' on')}>
                          <input type="checkbox" checked={!bookingUnpick.has(c)}
                            onChange={e => setBookingUnpick(s => {
                              const n = new Set(s)
                              if (e.target.checked) n.delete(c); else n.add(c)
                              return n
                            })} />
                          {c}
                        </label>
                      ))}
                    </div>
                  )}
                </span>
              )}
            </span>
          )}
        </div>
      )}

      {ganttReady && !loading && (
        <div className="gantt-section">
          <div className="gantt-bar">
            <b>Gantt แผนผลิต (เครื่อง × สัปดาห์)</b>
            <div className="gantt-bar-right">
              {showGantt && (
                <div className="gantt-fields">
                  <button className="gfield-toggle" onClick={() => setShowFieldBar(s => !s)}
                    title="เลือกว่าจะให้บล็อกโชว์ข้อมูลอะไรบ้าง">
                    ⚙ ข้อมูลบนบล็อก ({sheetBarFields.filter(f => barFields[f.key]).length})
                  </button>
                  {showFieldBar && (
                    <div className="gfield-list">
                      {sheetBarFields.map(f => (
                        <label key={f.key} className={'gfield-chip' + (barFields[f.key] ? ' on' : '')}>
                          <input type="checkbox" checked={!!barFields[f.key]}
                            onChange={e => setBarFields(s => ({ ...s, [f.key]: e.target.checked }))} />
                          {f.label}
                        </label>
                      ))}
                      <button className="gfield-reset" onClick={() => setBarFields(BAR_FIELDS_DEFAULT)}>ค่าเริ่มต้น</button>
                    </div>
                  )}
                </div>
              )}
              {showGantt && (
                <span className="gload-legend">
                  <span className="glg"><i className="glg-dot old" />แผนเก่า</span>
                  <span className="glg"><i className="glg-dot new" />แผนใหม่</span>
                  <span className="glg"><i className="glg-dot free" />ว่าง</span>
                  <span className="glg"><i className="glg-dot over" />เต็ม</span>
                </span>
              )}
              <button onClick={() => setShowGantt(s => !s)}>{showGantt ? 'ซ่อน' : 'แสดง'}</button>
            </div>
          </div>
          {showGantt && <PlanGantt columns={grid.columns} rows={visible} sheet={grid.sheet} load={load} setupJobs={setupJobs} ava={ava} bookingMc={bookingMc} poolMap={poolMap} onMoveWeek={moveJob} onEditQty={editQty} onSplit={splitJob} onRemove={delRow} bookingItems={bookingItems} bookingMode={bookingMode} bookingPick={bookingPick} allMcRows={workdayData?.mc_rows || EMPTY_ROWS} loadFilter={loadFilter} setLoadFilter={setLoadFilter} barFields={barFields} selIdx={selJob} setSelIdx={setSelJob} programRows={programRows} />}
        </div>
      )}

      {/* ชีทที่ไม่มี MC_GROUP+PLAN_WEEK (เช่น UNPLANNED, DETAIL) ไม่มี Gantt ให้โชว์ →
          ต้องมีตารางธรรมดาให้เห็น ไม่งั้นเลือกชีทแล้วหน้าจะว่างเปล่าไม่มีอะไรขึ้นเลย */}
      {grid && !ganttReady && !loading && (
        <div className="gridwrap">
          {renderGrid(null, visible, false)}
        </div>
      )}

      {msg && <div className="msg">{msg}</div>}
      {loading && <div className="hint">กำลังโหลด...</div>}
      {!loading && !meta?.exists && <div className="hint">ยังไม่มีไฟล์แผน กด <b>รันแผนใหม่</b> แล้วรอให้สถานะจบก่อน ระบบจะโหลดให้อัตโนมัติ</div>}

      {/* คลิกบล็อกใน Gantt → เด้ง modal โชว์เฉพาะแถวของ item ที่คลิก (แก้ไขได้) คู่กับการ์ดรายละเอียด
          modal เว้นที่ฝั่งขวาให้การ์ด (JobPanel = fixed) จึงไม่ทับกัน */}
      {selJob != null && grid && (() => {
        const itemCi = grid.columns.indexOf('ITEM_CODE')
        const selItem = itemCi >= 0 ? grid.rows[selJob]?.[itemCi] : null
        // เฉพาะแถวของ item เดียวกัน (อ่านจาก grid.rows ทั้งหมด ไม่สนตัวกรองที่เปิดอยู่)
        const itemRows = itemCi >= 0
          ? grid.rows.map((row, idx) => ({ row, idx })).filter(({ row }) => row[itemCi] === selItem)
          : [{ row: grid.rows[selJob], idx: selJob }]
        // สรุปยอดแยกรายออร์เดอร์ (SC_SO_NO + SC_LINE_ID) ที่อยู่ใน item นี้ — item เดียวมีหลาย SC ได้
        const scI = grid.columns.indexOf('SC_SO_NO')
        const lnI = grid.columns.indexOf('SC_LINE_ID')
        const oqI = grid.columns.indexOf('ORDERS_QTY')
        const pqI = grid.columns.indexOf('PRODUCE_QTY')
        // ออร์เดอร์นิยามด้วย ORDER_KEY_COLS (SC เดียวมีหลายออร์เดอร์: ต่าง PO / ORDERS_QTY / FG week)
        const okey = row => orderKeyOf(row, grid.columns)
        const selKey = okey(grid.rows[selJob])
        // LINE_REMARK ผูกกับ SC — เก็บแยกต่อออร์เดอร์ ไม่รวมข้าม SC (item เดียวกันแต่คนละ SC อาจมีหมายเหตุคนละอัน)
        const lrI = grid.columns.indexOf('LINE_REMARK')
        const ordMap = new Map()
        itemRows.forEach(({ row }) => {
          const key = okey(row)
          const m = ordMap.get(key) || { key, sc: scI >= 0 ? norm(row[scI]) : '', line: lnI >= 0 ? norm(row[lnI]) : '', ordered: 0, placed: 0, remark: '' }
          m.ordered = Math.max(m.ordered, Number(norm(row[oqI])) || 0)
          m.placed += Number(norm(row[pqI])) || 0
          if (!m.remark && lrI >= 0) {
            const r = norm(row[lrI])
            if (r) m.remark = r
          }
          ordMap.set(key, m)
        })
        const orders = [...ordMap.values()]
          .filter(o => o.ordered > 0)
          .map(o => ({ ...o, diff: Math.round((o.ordered - o.placed) * 100) / 100 }))
        return (
          <div className="plan-modal-backdrop" onClick={() => setSelJob(null)}>
            <div className="plan-modal" onClick={e => e.stopPropagation()}>
              <div className="plan-modal-head">
                <b>{selItem || 'ตารางแผนผลิต'}</b>
                <span className="plan-modal-hint">{itemRows.length} แถว · {orders.length} ออร์เดอร์ · แก้ค่าในช่องได้เลย</span>
                <button className="plan-modal-close" onClick={() => setSelJob(null)} title="ปิด (Esc)">✕</button>
              </div>
              {orders.length > 0 && (
                <div className="plan-modal-orders">
                  {orders.map(o => (
                    <div key={o.key} className="order-item">
                      <span
                        className={'order-status ' + (Math.abs(o.diff) <= QTY_TOL ? 'ok' : o.diff > 0 ? 'short' : 'over') + (o.key === selKey ? ' cur' : '')}>
                        SC {o.sc || '-'}{o.line ? `/${o.line}` : ''} · สั่ง {o.ordered.toLocaleString()} · วาง {o.placed.toLocaleString()}
                        {Math.abs(o.diff) <= QTY_TOL ? ' ✓' : o.diff > 0 ? ` ⚠ ขาด ${o.diff.toLocaleString()}` : ` ⚠ เกิน ${(-o.diff).toLocaleString()}`}
                      </span>
                      {o.remark && <div className="order-remark">📝 {o.remark}</div>}
                    </div>
                  ))}
                </div>
              )}
              <div className="gridwrap plan-modal-grid">
                {renderGrid(selJob, itemRows)}
              </div>
            </div>
          </div>
        )
      })()}

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

      {showCylinder && (
        <div className="modal-backdrop" onClick={() => setShowCylinder(false)}>
          <div className="modal-box" onClick={e => e.stopPropagation()}>
            <button className="modal-close" title="ปิด" onClick={() => setShowCylinder(false)}>✕</button>
            <CylinderAdvisor />
          </div>
        </div>
      )}
    </div>
  )
}

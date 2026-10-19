import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import { ColumnFilter, norm } from './ColumnFilter.jsx'
import { weekKey, wkTagYear } from '../weekKeys.js'

/**
 * PlanPivot — ตาราง Pivot ของชีทแผน (เหมือน PivotTable ใน Excel)
 *
 * แถว = ฟิลด์ที่ user ติ๊กเลือกเอง (ค่าเริ่มต้นชุดเดียวกับ pivot ที่ planner ทำใน Excel)
 * คอลัมน์ = สัปดาห์ (PLAN_WEEK) — เปลี่ยนเป็นฟิลด์อื่นได้
 * ค่า = ผลรวม PRODUCE_QTY (ยอดที่แผนวางให้ทอจริง) — เปลี่ยนคอลัมน์/วิธีรวมได้
 *
 * ข้อมูลมาจาก "ชีทแผนที่วางแล้ว" (ปกติคือ REPLAN) ผ่าน /api/plan/sheet ตัวเดิม
 * ไม่มีการคำนวณใหม่ฝั่ง backend — ตัวเลขทุกช่องคือผลรวมของแถวในชีทตรง ๆ
 */

// ฟิลด์แถวเริ่มต้น = ชุดเดียวกับ pivot ที่ทำใน Excel
// (ชีทแผนไม่มี SALE_NAME → ใช้ CUSTOMER แทน, TEAM_NAME → TEAM, SO_NO → SC_SO_NO, GUAGE → MC_GUAGE)
const DEFAULT_ROW_FIELDS = ['MC_GROUP', 'MC_GUAGE', 'ITEM_CODE', 'TEAM', 'CUSTOMER', 'SC_SO_NO', 'YARN_USED']
const DEFAULT_COL_FIELD = 'PLAN_WEEK'
const DEFAULT_VAL_FIELD = 'PRODUCE_QTY'

// มุมมองที่ใช้บ่อย — กดปุ่มเดียวได้ชุดฟิลด์แถวเลย ไม่ต้องมาติ๊กทีละตัว
const PRESETS = [
  { key: 'item', label: 'ตาม Item', fields: ['MC_GROUP', 'MC_GUAGE', 'ITEM_CODE'] },
  { key: 'order', label: 'SC', fields: ['ITEM_CODE', 'SC_SO_NO'] },
  { key: 'team', label: 'Team', fields: ['TEAM', 'ITEM_CODE'] },
  { key: 'yarn', label: 'เส้นด้าย', fields: ['YARN_USED', 'ITEM_CODE'] },
  { key: 'mc', label: 'จำนวนเครื่อง', fields: ['MC_GROUP', 'MC_GUAGE', 'ITEM_CODE'] },
  { key: 'full', label: 'Pivot', fields: DEFAULT_ROW_FIELDS },
]
// "จำนวนเครื่อง" ใช้ฟิลด์แถวชุดเดียวกับ "ตาม Item" ต่างกันแค่คอลัมน์ค่า (เครื่องที่ใช้จริง ไม่ใช่ กก.)
const PRESET_EXTRA = { mc: { val: 'ACTUAL_MC' } }

// ชีทที่ pivot ไม่มีความหมาย (ไม่ใช่ตารางแถวงาน) — ยังเลือกได้ถ้าเป็นชีทที่เปิดค้างอยู่
const SKIP_SHEETS = new Set(['PIVOT_PLAN', 'REMAINING_JOBS'])

const AGGS = [
  { key: 'sum', label: 'ผลรวม (Sum)' },
  { key: 'count', label: 'นับแถว (Count)' },
  { key: 'avg', label: 'เฉลี่ย (Average)' },
  { key: 'max', label: 'มากสุด (Max)' },
]

// ค่าที่ผู้ใช้ตั้งไว้ (ฟิลด์แถว/ชีท/คอลัมน์/วิธีรวม) — เก็บในเบราว์เซอร์ เปิดครั้งหน้าได้ชุดเดิม
const CFG_KEY = 'kp_pivot_cfg_v1'

function loadCfg() {
  try { return JSON.parse(localStorage.getItem(CFG_KEY)) || {} } catch { return {} }
}

function fmtVal(n, agg) {
  if (n == null) return ''
  if (agg === 'count') return n.toLocaleString('en-US')
  // ยอด กก. ทศนิยมไม่มีความหมายในการอ่านแผน — ปัดให้เหมือนที่ Excel โชว์
  return n.toLocaleString('en-US', { maximumFractionDigits: 1 })
}

// ค่าที่จะเอามาเป็นชื่อกลุ่ม — เซลล์ว่าง/NaN ให้เป็น "(ว่าง)" เหมือน Excel ไม่ใช่ทิ้งแถว
function cellKey(v) {
  const s = norm(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'nat') return '(ว่าง)'
  return s
}

// คีย์เรียงหัวคอลัมน์: ตัวเลขเรียงก่อนตามค่า (W99 = ยังไม่วาง ไปท้ายสุด) แล้วค่อยตัวอักษร
function headSortKey(label) {
  const n = Number(label)
  if (Number.isFinite(n)) return [0, n === 99 ? Number.MAX_SAFE_INTEGER : n, '']
  return [1, 0, label]
}

function cmpHead(a, b) {
  const ka = headSortKey(a), kb = headSortKey(b)
  return (ka[0] - kb[0]) || (ka[1] - kb[1]) || String(ka[2]).localeCompare(String(kb[2]), 'th')
}

export default function PlanPivot({ sheet, sheets, columns, allRows, filteredRows, hasFilter }) {
  const cfg0 = useMemo(loadCfg, [])
  // ชีทที่ pivot กำลังอ่าน — ชีทที่จำไว้ (ถ้ายังมีในไฟล์) ไม่งั้นใช้ชีทที่เปิดอยู่
  const [pvSheet, setPvSheet] = useState(
    () => ((sheets || []).includes(cfg0.sheet) ? cfg0.sheet : (sheet || '')))
  const [ext, setExt] = useState(null)        // ข้อมูลชีทอื่นที่โหลดมาเอง { columns, rows }
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  // ใช้แถวที่ผ่านตัวกรองของหน้าแผน (ได้เฉพาะตอนดูชีทเดียวกับที่เปิดอยู่)
  const [useFilter, setUseFilter] = useState(true)
  const [rowFields, setRowFields] = useState(() => cfg0.rowFields || DEFAULT_ROW_FIELDS)
  const [colField, setColField] = useState(() => cfg0.colField ?? DEFAULT_COL_FIELD)
  const [valField, setValField] = useState(() => cfg0.valField || DEFAULT_VAL_FIELD)
  const [agg, setAgg] = useState(() => cfg0.agg || 'sum')
  const [showSub, setShowSub] = useState(() => !!cfg0.showSub)   // แถวยอดรวมราย Item
  const [showMc, setShowMc] = useState(() => !!cfg0.showMc)      // ต่อท้ายทุกช่องด้วย (จำนวนเครื่อง)
  // ทาสีช่องที่มี "งานสี" — เปิดไว้ตั้งแต่แรก (ปิดแล้วค่อยจำค่าไว้ให้)
  const [showClr, setShowClr] = useState(() => cfg0.showClr !== false)
  const [collapsed, setCollapsed] = useState(() => new Set())
  const [pick, setPick] = useState(false)          // เปิดลิสต์เลือกฟิลด์แถว
  const [pickSearch, setPickSearch] = useState('')
  const [search, setSearch] = useState('')
  // ตัวกรองแบบ Excel ที่หัวคอลัมน์ฟิลด์แถว: ชื่อคอลัมน์ → Set ของค่าที่ติ๊กไว้ (ไม่มีคีย์ = ไม่กรอง)
  // ไม่จำลง localStorage — เปิด Pivot ใหม่เริ่มแบบไม่กรอง จะได้ไม่มีตัวกรองค้างแบบไม่รู้ตัว
  const [fieldFilters, setFieldFilters] = useState({})
  const [fpop, setFpop] = useState(null)           // ป็อปอัพกรองที่เปิดอยู่ { field, anchor }

  const sameSheet = pvSheet === sheet

  // สลับชีท → โหลดจาก backend (ชีทเดียวกับที่เปิดอยู่ใช้ข้อมูลในหน้าเลย ไม่ต้องยิงซ้ำ)
  useEffect(() => {
    if (sameSheet) { setExt(null); setErr(''); return }
    let dead = false
    setLoading(true); setErr('')
    api.planSheet(pvSheet)
      .then(d => { if (!dead) setExt({ columns: d.columns || [], rows: d.rows || [] }) })
      .catch(e => { if (!dead) { setExt(null); setErr('อ่านชีทไม่ได้: ' + e.message) } })
      .finally(() => { if (!dead) setLoading(false) })
    return () => { dead = true }
  }, [pvSheet, sameSheet])

  // จำค่าที่ตั้งไว้ (ต่อเบราว์เซอร์) — เปิด Pivot ครั้งหน้าได้มุมมองเดิมทันที
  useEffect(() => {
    try {
      localStorage.setItem(CFG_KEY, JSON.stringify(
        { sheet: pvSheet, rowFields, colField, valField, agg, showSub, showMc, showClr }))
    } catch { /* โหมดส่วนตัว/พื้นที่เต็ม → ไม่จำก็ใช้งานได้ปกติ */ }
  }, [pvSheet, rowFields, colField, valField, agg, showSub, showMc, showClr])

  const cols = sameSheet ? (columns || []) : (ext?.columns || [])

  // แถวดิบที่จะนับ — ตามตัวกรองหน้าแผน (ถ้าเลือกไว้และเป็นชีทเดียวกัน) หรือทั้งชีท
  const srcRows = useMemo(() => {
    if (!sameSheet) return ext?.rows || []
    if (useFilter && filteredRows) return filteredRows.map(x => x.row)
    return allRows || []
  }, [sameSheet, ext, useFilter, filteredRows, allRows])

  // ฟิลด์ที่เลือกไว้อาจไม่มีในชีทที่เพิ่งสลับมา → ถอยกลับไปค่าที่ชีทนั้นมีจริง
  useEffect(() => {
    if (!cols.length) return
    setRowFields(f => {
      const keep = f.filter(c => cols.includes(c))
      return keep.length === f.length ? f
        : (keep.length ? keep : DEFAULT_ROW_FIELDS.filter(c => cols.includes(c)))
    })
    setColField(c => (cols.includes(c) ? c : (cols.includes(DEFAULT_COL_FIELD) ? DEFAULT_COL_FIELD : '')))
    setValField(c => (cols.includes(c) ? c : (cols.includes(DEFAULT_VAL_FIELD) ? DEFAULT_VAL_FIELD : '')))
    setCollapsed(new Set())
  }, [cols])

  // สลับชีท = ข้อมูลคนละชุด → ล้างตัวกรองหัวคอลัมน์ทิ้ง (ค่าที่ติ๊กไว้อาจไม่มีในชีทใหม่)
  useEffect(() => { setFieldFilters({}); setFpop(null) }, [pvSheet])

  // เอาฟิลด์ออกจากแถว → ตัวกรองของฟิลด์นั้นต้องหายด้วย ไม่งั้นกรองค้างอยู่โดยมองไม่เห็นหัวคอลัมน์
  useEffect(() => {
    setFieldFilters(f => {
      const keys = Object.keys(f)
      const keep = keys.filter(k => rowFields.includes(k))
      if (keep.length === keys.length) return f
      return Object.fromEntries(keep.map(k => [k, f[k]]))
    })
  }, [rowFields])

  const rowIdx = useMemo(() => rowFields.map(f => cols.indexOf(f)).filter(i => i >= 0), [rowFields, cols])
  // ตัวกรองหัวคอลัมน์ในรูป [index คอลัมน์, Set ค่า, ชื่อฟิลด์] — ใช้ทั้งตอนสร้าง pivot และตอนเปิดป็อปอัพ
  const fIdx = useMemo(() => Object.entries(fieldFilters)
    .map(([f, set]) => [cols.indexOf(f), set, f])
    .filter(x => x[0] >= 0), [fieldFilters, cols])
  const passF = (r, list) => list.every(([i, set]) => set.has(norm(r[i])))
  const rowNames = useMemo(() => rowIdx.map(i => cols[i]), [rowIdx, cols])
  const colI = cols.indexOf(colField)
  const valI = cols.indexOf(valField)
  const yearI = cols.indexOf('PLAN_YEAR')
  // สัปดาห์ในชีทแผนเป็น "เลขสัปดาห์เปล่า" ไม่มีปี → W1 ของปีหน้าต้องต่อท้าย W52 ของปีนี้
  // ไม่ใช่โผล่หน้าสุดเพราะเลขน้อย (เกณฑ์เดียวกับที่ตารางแผนใช้เรียงแถว)
  const weekMode = colField === 'PLAN_WEEK' && yearI >= 0
  // ระดับของ ITEM_CODE ในฟิลด์แถว — ยอดรวมย่อยออกที่ระดับนี้ระดับเดียว (ตามที่ planner ใช้จริง)
  const itemLevel = rowNames.indexOf('ITEM_CODE')
  const subLevel = itemLevel >= 0 && itemLevel < rowNames.length - 1 ? itemLevel : -1
  // คอลัมน์ที่ใช้นับเครื่อง — ต้องมีครบ 4 ตัวถึงจะแยก "ชุดเครื่องเดียวกัน" ออกจากกันได้
  const mcI = cols.indexOf('ACTUAL_MC')
  const mcKeyIdx = ['ITEM_CODE', 'MC_GROUP', 'MC_GUAGE'].map(c => cols.indexOf(c))
  const canMc = mcI >= 0 && mcKeyIdx.every(i => i >= 0)
  // เปิด ACTUAL_MC เป็นคอลัมน์ค่าอยู่แล้ว (preset จำนวนเครื่อง) → ไม่ต้องต่อท้าย (n) ให้ซ้ำ
  const mcOn = showMc && canMc && valField !== 'ACTUAL_MC'
  // คอลัมน์ธง "งานสี" รายแถว — Planning.py แตกยอดตามโควตาสีต่อ (SC+ITEM) แล้วเขียน
  // COLOR_PART: Yes = อยู่ในยอดที่เปิดสีแล้ว / No = ส่วนเกินโควตา / ว่าง = ไม่ใช่ item สี
  // ⚠️ ต้องดูรายแถว ไม่ใช่ระดับ item — item เดียวที่วางหลายสัปดาห์มักเปิดสีมาแค่บางก้อน
  //    (เช่น W39 = Yes, W40 = No) ถ้าดูแค่รหัส item จะทาสีทั้งแถวจนแยกไม่ออกว่าสัปดาห์ไหนเป็นงานสี
  //    (เกณฑ์เดียวกับที่ Gantt ใช้ — ดู hasColorPart ใน PlanGantt.jsx)
  const clrI = cols.indexOf('COLOR_PART')

  // ── สร้าง pivot: tree ของกลุ่มตามลำดับฟิลด์แถว + ยอดต่อหัวคอลัมน์ ────────────
  const pivot = useMemo(() => {
    const heads = new Map()   // คีย์หัวคอลัมน์ → { y, w, text } (แยกคีย์กับข้อความที่โชว์)
    const root = { children: new Map(), vals: new Map(), cnt: new Map() }
    const s = search.trim().toLowerCase()
    let anyClr = false        // ชีทนี้มีแถวงานสีจริงไหม (ไม่มี = ปิดสวิตช์ไฮไลต์ให้เลย)

    for (const r of srcRows) {
      if (s && !rowIdx.some(i => norm(r[i]).toLowerCase().includes(s))) continue
      if (fIdx.length && !passF(r, fIdx)) continue
      let head, info
      if (colI < 0) {
        head = 'รวม'; info = { y: 0, w: 0, text: 'รวม' }
      } else if (weekMode) {
        const w = Number(norm(r[colI]))
        const y = Number(norm(r[yearI]))
        const wv = Number.isFinite(w) ? w : 0
        const yv = Number.isFinite(y) && y > 0 ? y : 0
        // คีย์ต้องรวมปีด้วย ไม่งั้น W1 ปีนี้กับ W1 ปีหน้าถูกบวกรวมกันเป็นช่องเดียว
        head = yv + '|' + wv
        // ปีเดียวกันกับที่ Gantt ติดป้าย (weekKey ติด @ปี เมื่อไกลเกินครึ่งปี) → โชว์ "45 '28" แยกจาก "45"
        const wk = weekKey(wv, yv)
        const ty = wkTagYear(wk)
        info = { y: yv, w: wv === 99 ? Number.MAX_SAFE_INTEGER : wv,
          text: ty != null ? `${wv} '${String(ty).slice(-2)}` : String(wv) }
      } else {
        head = cellKey(r[colI]); info = null
      }
      if (!heads.has(head)) heads.set(head, info)
      const raw = valI >= 0 ? Number(norm(r[valI])) : NaN
      const v = Number.isFinite(raw) ? raw : 0
      // ── เครื่องของแถวนี้ ─────────────────────────────────────────────────
      // ⚠ ACTUAL_MC ของ (item × เครื่อง × เกจ × สัปดาห์) เดียวกัน คือ "เครื่องชุดเดียวกัน"
      // ที่ Planning.py เขียนค่าซ้ำทุกแถว (หลาย SC แบ่งเวลากันบนเครื่องชุดเดิม) → ต้อง max
      // ต่อชุด แล้วค่อยบวกข้าม item ไม่งั้นเลขเครื่องบานเป็นเท่าตัว (กติกาเดียวกับ Gantt)
      // แถวที่ยืมเครื่องเดิม (MC_SHARED) Planning.py เขียน ACTUAL_MC = 0 อยู่แล้ว จึงไม่ซ้ำซ้อน
      const mcv = canMc ? (Number(norm(r[mcI])) || 0) : 0
      const mcKey = canMc ? mcKeyIdx.map(i => norm(r[i]).trim().toUpperCase()).join('|') : ''
      // แถวนี้เป็นงานสีไหม — มีแต่ Yes ที่ใช่ (No = ส่วนเกินโควตา, ว่าง = item ไม่มียอดเปิดสี)
      const isClr = clrI >= 0 && norm(r[clrI]).trim().toUpperCase() === 'YES'
      if (isClr) anyClr = true

      // เดินลงไปทีละระดับ แล้วบวกยอดสะสมไว้ที่ "ทุกระดับ" — ยุบกลุ่มไหนก็อ่านยอดตรงนั้นได้ทันที
      let node = root
      const path = []
      for (let d = 0; d <= rowIdx.length; d++) {
        // บวกยอดเข้าโหนดปัจจุบัน (root = Grand Total)
        node.vals.set(head, (node.vals.get(head) || 0) + v)
        node.cnt.set(head, (node.cnt.get(head) || 0) + 1)
        node.total = (node.total || 0) + v
        node.totalCnt = (node.totalCnt || 0) + 1
        if (node.max == null || v > node.max) node.max = v
        const mk = node.maxBy || (node.maxBy = new Map())
        if (!mk.has(head) || v > mk.get(head)) mk.set(head, v)
        // งานสี: สะสมแยกอีกชุด (ยอด + จำนวนแถว) เพื่อบอกได้ว่าช่องนี้เป็นงานสี "ทั้งก้อน" หรือ "บางส่วน"
        if (isClr) {
          const cv = node.clr || (node.clr = new Map())
          const cc = node.clrCnt || (node.clrCnt = new Map())
          cv.set(head, (cv.get(head) || 0) + v)
          cc.set(head, (cc.get(head) || 0) + 1)
          node.clrTot = (node.clrTot || 0) + v
          node.clrTotCnt = (node.clrTotCnt || 0) + 1
        }
        // เครื่อง: เก็บเป็น head → (ชุดเครื่อง → เครื่องมากสุดของชุดนั้น) แล้วค่อยบวกตอนแสดง
        if (canMc && mcv > 0) {
          const byHead = node.mcm || (node.mcm = new Map())
          let set = byHead.get(head)
          if (!set) byHead.set(head, set = new Map())
          if ((set.get(mcKey) || 0) < mcv) set.set(mcKey, mcv)
        }
        if (d === rowIdx.length) break
        const label = cellKey(r[rowIdx[d]])
        path.push(label)
        let kid = node.children.get(label)
        if (!kid) {
          // คีย์กลุ่ม = เส้นทางทั้งหมด คั่นด้วยอักขระที่ไม่มีในข้อมูล
          // (ต่อกันเฉย ๆ จะทำให้ 'A'+'BC' ชนกับ 'AB'+'C' = ยุบผิดกลุ่ม)
          kid = { label, depth: d, key: path.join('\u0001'), children: new Map(), vals: new Map(), cnt: new Map() }
          node.children.set(label, kid)
        }
        node = kid
      }
    }
    // เรียงหัวคอลัมน์ — โหมดสัปดาห์เรียงตาม (ปี, สัปดาห์) ที่เหลือเรียงเลข/ตัวอักษรตามปกติ
    // ถ้าชีทมีแผนหลายปี เติมปีต่อท้ายเฉพาะปีหลัง ๆ (เช่น 1/2027) จะได้ไม่สับสนกับ W1 ปีนี้
    const list = Array.from(heads, ([key, info]) => ({ key, info, text: info ? info.text : key }))
    if (weekMode) {
      list.sort((a, b) => (a.info.y - b.info.y) || (a.info.w - b.info.w))
      const years = Array.from(new Set(list.map(x => x.info.y).filter(Boolean))).sort((a, b) => a - b)
      if (years.length > 1) {
        for (const x of list) if (x.info.y && x.info.y !== years[0]) x.text += '/' + x.info.y
      }
    } else {
      list.sort((a, b) => cmpHead(a.key, b.key))
    }
    return { root, heads: list, anyClr }
  }, [srcRows, rowIdx, colI, valI, yearI, weekMode, search, fIdx, canMc, mcI, clrI, cols])

  // ค่าในป็อปอัพกรองของฟิลด์ที่เปิดอยู่ — cascade แบบ Excel: ตัดตามคำค้น + ตัวกรองคอลัมน์อื่น
  // (ไม่ตัดตามตัวกรองของตัวเอง ไม่งั้นติ๊กออกแล้วค่านั้นหายจากลิสต์ ติ๊กกลับไม่ได้)
  const fpData = useMemo(() => {
    if (!fpop) return null
    const ci = cols.indexOf(fpop.field)
    if (ci < 0) return null
    const s = search.trim().toLowerCase()
    const others = fIdx.filter(x => x[2] !== fpop.field)
    const counts = new Map()
    const domain = new Set()
    for (const r of srcRows) {
      const v = norm(r[ci])
      domain.add(v)
      if (s && !rowIdx.some(i => norm(r[i]).toLowerCase().includes(s))) continue
      if (others.length && !passF(r, others)) continue
      counts.set(v, (counts.get(v) || 0) + 1)
    }
    return { available: Array.from(counts, ([value, count]) => ({ value, count })), domain }
  }, [fpop, cols, search, fIdx, srcRows, rowIdx])

  function openFieldFilter(e, field) {
    const r = e.currentTarget.getBoundingClientRect()
    setFpop(p => (p?.field === field ? null : { field, anchor: r }))
  }

  function applyFieldFilter(field, set) {
    setFieldFilters(f => {
      const n = { ...f }
      if (set) n[field] = set
      else delete n[field]
      return n
    })
    setFpop(null)
    setCollapsed(new Set())
  }

  // ค่าที่จะโชว์ในช่อง = ตามวิธีรวมที่เลือก (โหนดเก็บ sum/count/max ไว้แล้ว)
  function cellOf(node, head) {
    const sum = node.vals.get(head)
    if (sum == null) return null
    const c = node.cnt.get(head) || 0
    if (agg === 'count') return c
    if (agg === 'avg') return c ? sum / c : 0
    if (agg === 'max') return node.maxBy?.get(head) ?? null
    return sum
  }

  // เครื่องของช่อง = ผลรวมเครื่องของแต่ละชุด (item×เครื่อง×เกจ) ที่อยู่ในกลุ่มนั้น
  function mcOf(node, head) {
    const set = node.mcm?.get(head)
    if (!set) return 0
    let n = 0
    for (const v of set.values()) n += v
    return n
  }

  // เครื่องของคอลัมน์ "รวม" = สัปดาห์ที่ใช้เครื่องมากสุด (peak) — บวกข้ามสัปดาห์ไม่มีความหมาย
  // เพราะเครื่องตัวเดิมวิ่งต่อหลายสัปดาห์ ไม่ได้ใช้เครื่องใหม่ทุกสัปดาห์
  function mcPeak(node) {
    if (!node.mcm) return 0
    let peak = 0
    for (const head of node.mcm.keys()) peak = Math.max(peak, mcOf(node, head))
    return peak
  }

  // ── งานสีของช่อง ────────────────────────────────────────────────────────────
  // คืน null = ไม่มีงานสี / { lv, v, tot, cnt } — lv: 'full' = ทั้งช่องเป็นงานสี, 'part' = ปนกับงานปกติ
  // เกณฑ์ "เต็ม" ดูจำนวนแถวเป็นหลัก (ใช้ได้กับทุกวิธีรวม) แล้วผ่อนด้วยยอด กก. อีกชั้น —
  // แถวงานปกติที่ยอด 0 กก. ปนอยู่ไม่ควรทำให้ช่องที่สีกินยอดทั้งหมดกลายเป็น "บางส่วน"
  function clrOf(node, head) {
    const cnt = node.clrCnt?.get(head) || 0
    if (!cnt) return null
    const v = node.clr?.get(head) || 0
    const tot = node.vals.get(head) || 0
    const full = cnt >= (node.cnt.get(head) || 0) || (tot > 0 && v >= tot - 0.5)
    return { lv: full ? 'full' : 'part', v, tot, cnt }
  }

  // เดียวกันแต่ของคอลัมน์ "รวม" (ยอดสะสมทุกสัปดาห์ของกลุ่มนั้น)
  function clrTotOf(node) {
    const cnt = node.clrTotCnt || 0
    if (!cnt) return null
    const v = node.clrTot || 0
    const tot = node.total || 0
    const full = cnt >= (node.totalCnt || 0) || (tot > 0 && v >= tot - 0.5)
    return { lv: full ? 'full' : 'part', v, tot, cnt }
  }

  // คำอธิบายใต้เมาส์ของช่องงานสี — บอกว่าในช่องนี้เป็นยอดที่เปิดสีแล้วเท่าไร จากทั้งหมดเท่าไร
  function clrTitle(c) {
    if (!c) return undefined
    const head = c.lv === 'full' ? '🎨 งานสีทั้งช่อง' : '🎨 มีงานสีปนอยู่'
    return `${head}\nยอดที่เปิดสีแล้ว ${fmtVal(c.v, 'sum')} จาก ${fmtVal(c.tot, 'sum')} กก. (${c.cnt} แถว)`
  }

  function totalOf(node) {
    if (node.total == null) return null
    if (agg === 'count') return node.totalCnt || 0
    if (agg === 'avg') return node.totalCnt ? node.total / node.totalCnt : 0
    if (agg === 'max') return node.max ?? null
    return node.total
  }

  // ── แผ่ tree เป็นแถวตาราง (เคารพกลุ่มที่ถูกยุบ + ยอดรวมราย Item) ─────────────
  // show[i] = แถวนี้ต้อง "พิมพ์" ค่าคอลัมน์ที่ i ไหม — ค่าที่ซ้ำกับแถวบนจะเว้นว่างแบบ Excel
  // sep    = ระดับตื้นสุดที่เปลี่ยนกลุ่มเทียบกับแถวบน (ใช้ตีเส้นคั่นให้เห็นขอบกลุ่มชัด)
  const lines = useMemo(() => {
    const out = []
    const walk = (node, labels) => {
      const kids = Array.from(node.children.values()).sort((a, b) => cmpHead(a.label, b.label))
      for (const kid of kids) {
        const isLeaf = kid.depth === rowIdx.length - 1
        const shut = collapsed.has(kid.key)
        const labs = [...labels, kid.label]
        if (isLeaf || shut) {
          out.push({ kind: 'row', node: kid, labels: labs, depth: kid.depth, foldable: !isLeaf, shut })
          continue
        }
        // กลุ่มที่กางอยู่ = ไม่ต้องมีแถวของตัวเอง (แบบ tabular ของ Excel) ลูก ๆ โชว์แทน
        walk(kid, labs)
        // ยอดรวมย่อยออกที่ระดับ ITEM_CODE ระดับเดียว — ระดับอื่นเป็นแค่หัวข้อ ไม่ต้องมีแถวรวม
        if (showSub && kid.depth === subLevel) {
          out.push({ kind: 'sub', node: kid, labels: labs, depth: kid.depth })
        }
      }
    }
    walk(pivot.root, [])

    // เทียบกับแถวก่อนหน้าเพื่อรู้ว่าต้องพิมพ์ชื่อกลุ่มซ้ำหรือเว้นว่าง
    let prev = null
    for (const ln of out) {
      if (ln.kind === 'sub') {
        // แถวรวมพิมพ์ชื่อ item ของตัวเองเสมอ แล้วรีเซ็ต — แถวถัดไปเริ่มพิมพ์ชื่อกลุ่มใหม่ทั้งชุด
        ln.show = ln.labels.map((_, i) => i === ln.depth)
        ln.sep = ln.depth
        prev = null
        continue
      }
      let diff = 0
      if (prev) { while (diff < ln.labels.length && prev[diff] === ln.labels[diff]) diff++ }
      ln.show = ln.labels.map((_, i) => !prev || i >= diff)
      ln.sep = prev ? diff : -1
      prev = ln.labels
    }
    return out
  }, [pivot, collapsed, rowIdx.length, showSub, subLevel])

  // ยุบทุกกลุ่มที่ลึกกว่าระดับ n (n = 0 → กางหมด) — ปุ่ม 1/2/3 เหมือนปุ่ม outline ของ Excel
  // ต้องมีตัวนี้เพราะกลุ่มที่กางอยู่ไม่มีแถวของตัวเองในตารางแบบ tabular จึงไม่มีปุ่ม ⊟ ให้กด
  function applyLevel(n) {
    if (!n) { setCollapsed(new Set()); return }
    const keys = new Set()
    const walk = node => {
      for (const kid of node.children.values()) {
        if (kid.depth === n - 1) { if (kid.children.size) keys.add(kid.key) }
        else walk(kid)
      }
    }
    walk(pivot.root)
    setCollapsed(keys)
  }

  function toggleFold(key) {
    setCollapsed(s => {
      const n = new Set(s)
      n.has(key) ? n.delete(key) : n.add(key)
      return n
    })
  }

  function moveField(name, dir) {
    setRowFields(f => {
      const i = f.indexOf(name)
      const j = i + dir
      if (i < 0 || j < 0 || j >= f.length) return f
      const n = [...f]
      n[i] = n[j]; n[j] = name
      return n
    })
  }

  function applyPreset(p) {
    const fields = p.fields.filter(c => cols.includes(c))
    if (!fields.length) return
    setRowFields(fields)
    const extra = PRESET_EXTRA[p.key]
    if (extra?.val && cols.includes(extra.val)) setValField(extra.val)
    else if (cols.includes(DEFAULT_VAL_FIELD)) setValField(DEFAULT_VAL_FIELD)
    setCollapsed(new Set())
  }

  // preset ที่ตรงกับฟิลด์แถวชุดปัจจุบัน (ใช้ไฮไลต์ปุ่ม) — ดูค่าคอลัมน์ค่าประกอบด้วย
  const activePreset = PRESETS.find(p =>
    p.fields.filter(c => cols.includes(c)).join('|') === rowFields.join('|')
    && (!PRESET_EXTRA[p.key] || PRESET_EXTRA[p.key].val === valField)
    && (PRESET_EXTRA[p.key] || valField === DEFAULT_VAL_FIELD))?.key

  // ── ดาวน์โหลดตารางที่เห็นเป็น CSV (เปิดใน Excel ต่อได้เลย) ───────────────────
  function downloadCsv() {
    const esc = v => {
      const s = String(v ?? '')
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s
    }
    const head = [...rowNames, ...pivot.heads.map(h => (weekMode ? 'W' : '') + h.text), 'รวมทั้งหมด']
    // ไฟล์ CSV พิมพ์ชื่อกลุ่มครบทุกแถว (ไม่เว้นว่างแบบบนจอ) — เอาไปกรอง/ทำ pivot ต่อใน Excel ได้
    // เปิดโหมดโชว์เครื่องอยู่ → เขียนเหมือนที่เห็นบนจอ "609.4 (2)" ไม่งั้นเป็นตัวเลขล้วน
    const cellTxt = (node, key) => {
      const v = cellOf(node, key)
      if (v == null) return ''
      const n = mcOn ? mcOf(node, key) : 0
      return n > 0 ? `${v} (${n})` : v
    }
    const totTxt = node => {
      const v = totalOf(node)
      if (v == null) return ''
      const n = mcOn ? mcPeak(node) : 0
      return n > 0 ? `${v} (${n})` : v
    }
    const body = lines.map(ln => {
      const labs = rowNames.map((_, i) => (ln.kind === 'sub' && i === ln.depth
        ? ln.labels[i] + ' รวม' : (ln.labels[i] ?? '')))
      return [...labs, ...pivot.heads.map(h => cellTxt(ln.node, h.key)), totTxt(ln.node)]
    })
    const grand = [...rowNames.map((_, i) => (i === 0 ? 'รวมทั้งหมด' : '')),
    ...pivot.heads.map(h => cellTxt(pivot.root, h.key)), totTxt(pivot.root)]
    const csv = [head, ...body, grand].map(r => r.map(esc).join(',')).join('\r\n')
    // BOM = เปิดใน Excel แล้วภาษาไทยไม่กลายเป็นขยะ
    const url = URL.createObjectURL(new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8' }))
    const a = document.createElement('a')
    const d = new Date()
    const stamp = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`
    a.href = url
    a.download = `pivot_${pvSheet || 'PLAN'}_${stamp}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const sheetList = (sheets || []).filter(s => !SKIP_SHEETS.has(String(s).toUpperCase()) || s === pvSheet)
  const pickList = cols.filter(c => !rowFields.includes(c)
    && (!pickSearch.trim() || String(c).toLowerCase().includes(pickSearch.trim().toLowerCase())))
  const grandTotal = totalOf(pivot.root)
  const groupCount = lines.filter(l => l.kind === 'row').length
  const unit = agg === 'count' ? 'แถว' : (valField === DEFAULT_VAL_FIELD ? 'กก.' : '')
  // ชีทที่ไม่มีคอลัมน์ COLOR_PART หรือไม่มีแถวงานสีเลย → ปิดสวิตช์ไฮไลต์ (กดไม่ได้)
  const canClr = clrI >= 0 && pivot.anyClr
  const clrOn = showClr && canClr

  return (
    <div className="card pivotcard">
      <div className="pv-head">
        <h2>📊 Pivot แผนผลิต — ทุก Item</h2>
        {/* สรุปสั้น ๆ ให้เห็นทันทีว่ากำลังดูข้อมูลชุดไหน กี่แถว รวมเท่าไร */}
        <div className="pv-stats">
          <span className="pv-stat"><b>{srcRows.length.toLocaleString('en-US')}</b> แถว</span>
          <span className="pv-stat"><b>{groupCount.toLocaleString('en-US')}</b> กลุ่ม</span>
          <span className="pv-stat big"><b>{fmtVal(grandTotal, agg)}</b> {unit}</span>
          {clrOn && pivot.root.clrTot > 0 && (
            <span className="pv-stat clr" title="ยอดที่เปิดสีแล้ว (COLOR_PART = Yes) ในข้อมูลชุดที่กำลังดู">
              🎨 <b>{fmtVal(pivot.root.clrTot, 'sum')}</b> งานสี
            </span>
          )}
          {sameSheet && useFilter && hasFilter && <span className="pv-stat warn">ตามตัวกรองหน้าแผน</span>}
        </div>
      </div>

      {/* ── แถวที่ 1: มุมมองสำเร็จรูป + ชีท ─────────────────────────────────── */}
      <div className="pivotbar">
        <span className="pv-lab">มุมมอง</span>
        {PRESETS.map(p => (
          <button key={p.key}
            className={'pv-preset' + (activePreset === p.key ? ' on' : '')}
            title={'ฟิลด์แถว: ' + p.fields.join(' → ')
              + (PRESET_EXTRA[p.key]?.val ? ` · ค่า: ${PRESET_EXTRA[p.key].val}` : '')}
            onClick={() => applyPreset(p)}>{p.label}</button>
        ))}
        <span className="pv-sep" />
        <label className="pv-f">
          <span>ชีท</span>
          <select value={pvSheet} onChange={e => setPvSheet(e.target.value)}>
            {sheetList.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <input className="pv-search" placeholder="🔍 ค้นในฟิลด์แถว..."
          value={search} onChange={e => setSearch(e.target.value)} />
        {fIdx.length > 0 && (
          <button className="pv-fclear" onClick={() => { setFieldFilters({}); setFpop(null) }}
            title={'กรองอยู่: ' + fIdx.map(x => `${x[2]} (${x[1].size})`).join(', ')}>
            ✕ ล้างตัวกรองหัวคอลัมน์ ({fIdx.length})
          </button>
        )}
        <button className="pv-csv" onClick={downloadCsv} disabled={!lines.length}>⬇ CSV</button>
      </div>

      {/* ── แถวที่ 2: ฟิลด์แถว (ติ๊กเลือกเอง เรียงซ้าย→ขวาตามลำดับที่จัด) ───────── */}
      <div className="pivotfields">
        <span className="pv-lab">แถว</span>
        {rowFields.map((f, i) => (
          <span key={f} className={'pv-chip' + (f === 'ITEM_CODE' ? ' item' : '')}>
            <button className="pv-mv" title="เลื่อนซ้าย" disabled={i === 0} onClick={() => moveField(f, -1)}>‹</button>
            {f}
            <button className="pv-mv" title="เลื่อนขวา" disabled={i === rowFields.length - 1} onClick={() => moveField(f, 1)}>›</button>
            <button className="pv-x" title="เอาออก" onClick={() => setRowFields(x => x.filter(c => c !== f))}>✕</button>
          </span>
        ))}
        <span className="pv-addwrap">
          <button className="pv-add" onClick={() => { setPick(p => !p); setPickSearch('') }}>+ เพิ่มฟิลด์</button>
          {pick && (
            <div className="pv-pop">
              <input autoFocus placeholder="ค้นหาคอลัมน์..." value={pickSearch}
                onChange={e => setPickSearch(e.target.value)} />
              <div className="pv-poplist">
                {pickList.map(c => (
                  <button key={c} onClick={() => { setRowFields(x => [...x, c]); setPick(false) }}>{c}</button>
                ))}
                {!pickList.length && <div className="hint">ไม่มีคอลัมน์ที่ตรง</div>}
              </div>
            </div>
          )}
        </span>
        {/* ปุ่ม outline: ยุบให้เหลือถึงระดับที่กด (กลุ่มที่กางอยู่ไม่มีแถวของตัวเอง จึงต้องยุบจากตรงนี้) */}
        {rowFields.length > 1 && (
          <span className="pv-levels">
            <span className="pv-lab">ยุบถึง</span>
            {rowFields.slice(0, -1).map((f, i) => (
              <button key={f} className="pv-lv" title={`ยุบให้เหลือถึง ${f}`}
                onClick={() => applyLevel(i + 1)}>{i + 1}</button>
            ))}
            <button className="pv-lv wide" onClick={() => applyLevel(0)}>กางหมด</button>
          </span>
        )}
      </div>

      {/* ── แถวที่ 3: คอลัมน์/ค่า/วิธีรวม + สวิตช์ ──────────────────────────── */}
      <div className="pivotbar sub">
        <label className="pv-f">
          <span>คอลัมน์</span>
          <select value={colField} onChange={e => setColField(e.target.value)}>
            <option value="">(ไม่แบ่งคอลัมน์)</option>
            {cols.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </label>
        <label className="pv-f">
          <span>ค่า</span>
          <select value={valField} onChange={e => setValField(e.target.value)}>
            {cols.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </label>
        <label className="pv-f">
          <span>วิธีรวม</span>
          <select value={agg} onChange={e => setAgg(e.target.value)}>
            {AGGS.map(a => <option key={a.key} value={a.key}>{a.label}</option>)}
          </select>
        </label>
        <label className={'pv-chk' + (showSub ? ' on' : '') + (subLevel < 0 ? ' off' : '')}
          title={subLevel < 0
            ? 'ต้องมี ITEM_CODE อยู่ในฟิลด์แถว และมีฟิลด์ที่ลึกกว่า ถึงจะมีแถวยอดรวมให้แสดง'
            : 'แทรกแถวยอดรวมของแต่ละ item (ระดับอื่นไม่ต้องมีแถวรวม)'}>
          <input type="checkbox" checked={showSub && subLevel >= 0} disabled={subLevel < 0}
            onChange={e => setShowSub(e.target.checked)} />
          ยอดรวมราย Item
        </label>
        <label className={'pv-chk' + (mcOn ? ' on' : '') + (canMc && valField !== 'ACTUAL_MC' ? '' : ' off')}
          title={!canMc
            ? 'ชีทนี้ไม่มีคอลัมน์ ACTUAL_MC / ITEM_CODE / MC_GROUP / MC_GUAGE ครบ จึงนับเครื่องไม่ได้'
            : (valField === 'ACTUAL_MC'
              ? 'คอลัมน์ค่าเป็น ACTUAL_MC อยู่แล้ว — ตัวเลขในช่องคือจำนวนเครื่องตรง ๆ'
              : 'ต่อท้ายทุกช่องด้วยจำนวนเครื่อง เช่น 609.4 (2) — เครื่องชุดเดียวกันของ item นับครั้งเดียว ไม่บวกซ้ำตามใบสั่ง')}>
          <input type="checkbox" checked={mcOn} disabled={!canMc || valField === 'ACTUAL_MC'}
            onChange={e => setShowMc(e.target.checked)} />
          โชว์จำนวนเครื่อง (n)
        </label>
        <label className={'pv-chk' + (clrOn ? ' on' : '') + (canClr ? '' : ' off')}
          title={clrI < 0
            ? 'ชีทนี้ไม่มีคอลัมน์ COLOR_PART จึงไม่รู้ว่าก้อนไหนเป็นงานสี'
            : (!pivot.anyClr
              ? 'ข้อมูลชุดที่กำลังดูไม่มีแถวงานสี (COLOR_PART = Yes) เลย'
              : 'ทาสีช่องที่มีงานสีในสัปดาห์นั้น — ชมพูเข้ม = ทั้งช่องเป็นยอดที่เปิดสีแล้ว, ชมพูอ่อน+ขีดทแยง = มีงานสีปนกับงานปกติ')}>
          <input type="checkbox" checked={clrOn} disabled={!canClr}
            onChange={e => setShowClr(e.target.checked)} />
          🎨 เน้นงานสี
        </label>
        {/* ตัวกรองของหน้าแผน (คำค้น + ตัวกรองคอลัมน์ + ติ๊ก item) ใช้ได้เฉพาะชีทที่เปิดอยู่ */}
        <label className={'pv-chk' + (useFilter && sameSheet ? ' on' : '') + (sameSheet ? '' : ' off')}
          title={sameSheet ? 'นับเฉพาะแถวที่ผ่านตัวกรองด้านบนของหน้าแผน' : 'ดูชีทอื่นอยู่ — ตัวกรองหน้าแผนใช้กับชีทที่เปิดอยู่เท่านั้น'}>
          <input type="checkbox" checked={useFilter && sameSheet} disabled={!sameSheet}
            onChange={e => setUseFilter(e.target.checked)} />
          ตามตัวกรองหน้าแผน
        </label>
        {rowFields.join('|') !== DEFAULT_ROW_FIELDS.join('|') && (
          <button className="pv-reset" onClick={() => setRowFields(DEFAULT_ROW_FIELDS.filter(c => cols.includes(c)))}>
            คืนค่าเริ่มต้น
          </button>
        )}
      </div>

      {err && <div className="err">{err}</div>}
      {loading && <div className="hint">กำลังโหลดชีท {pvSheet}...</div>}

      {!loading && !err && (
        <div className="pivotwrap">
          <table className="pivottbl">
            <thead>
              <tr>
                {/* กดหัวคอลัมน์ = เปิดตัวกรองแบบ Excel ของฟิลด์นั้น */}
                {rowNames.map((f, i) => {
                  const on = !!fieldFilters[f]
                  return (
                    <th key={f}
                      className={'pv-rh' + (i === 0 ? ' first' : '') + (on ? ' fon' : '')
                        + (fpop?.field === f ? ' fopen' : '')}
                      title={on ? `กรอง ${f} อยู่ ${fieldFilters[f].size} ค่า — กดเพื่อแก้ตัวกรอง` : `กดเพื่อกรอง ${f}`}
                      onClick={e => openFieldFilter(e, f)}>
                      {f}<span className="pv-fic">{on ? ' 🔽' : ' ▾'}</span>
                    </th>
                  )
                })}
                {pivot.heads.map(h => (
                  <th key={h.key} className="pv-ch">{colField === 'PLAN_WEEK' ? 'W' + h.text : h.text}</th>
                ))}
                <th className="pv-tot">รวม</th>
              </tr>
            </thead>
            <tbody>
              {lines.map((ln, li) => {
                // เส้นคั่น: เปลี่ยนกลุ่มระดับบนสุด = เส้นหนา, ระดับรองลงมา = เส้นบาง
                const sepCls = ln.sep === 0 ? ' sep0' : (ln.sep === 1 ? ' sep1' : '')
                const cTot = clrOn ? clrTotOf(ln.node) : null
                return (
                  <tr key={li} className={(ln.kind === 'sub' ? 'pv-subrow' : '') + sepCls}>
                    {rowNames.map((_, i) => {
                      const label = ln.labels[i] ?? ''
                      const shown = ln.kind === 'sub'
                        ? (i === ln.depth ? label + ' — รวม' : '')
                        : (ln.show[i] ? label : '')
                      const foldHere = ln.kind === 'row' && ln.foldable && i === ln.depth
                      return (
                        <td key={i}
                          className={'pv-rc' + (i === 0 ? ' first' : '') + (shown ? '' : ' dup')}
                          title={label}>
                          {foldHere && (
                            <button className="pv-fold" title={ln.shut ? 'กางกลุ่ม' : 'ยุบกลุ่ม'}
                              onClick={() => toggleFold(ln.node.key)}>{ln.shut ? '⊞' : '⊟'}</button>
                          )}
                          {shown}
                        </td>
                      )
                    })}
                    {pivot.heads.map(h => {
                      const v = cellOf(ln.node, h.key)
                      const n = mcOn && v != null ? mcOf(ln.node, h.key) : 0
                      const c = clrOn ? clrOf(ln.node, h.key) : null
                      return (
                        <td key={h.key}
                          className={'pv-v' + (v == null ? ' zero' : '') + (c ? ' pv-clr ' + c.lv : '')}
                          title={clrTitle(c)}>
                          {fmtVal(v, agg)}
                          {n > 0 && (
                            <span className="pv-mc"
                              title={`ใช้ ${n} เครื่องในสัปดาห์นี้`
                                + (itemLevel >= 0 && ln.depth > itemLevel
                                  ? '\n(= เครื่องของ item ทั้งชุด — ใบสั่งในกลุ่มนี้แบ่งเวลากันบนเครื่องชุดเดียวกัน จึงเห็นเลขเท่ากันทุกแถว)'
                                  : '')}> ({n})</span>
                          )}
                        </td>
                      )
                    })}
                    <td className={'pv-v pv-tot' + (cTot ? ' pv-clr ' + cTot.lv : '')}
                      title={clrTitle(cTot)}>
                      {fmtVal(totalOf(ln.node), agg)}
                      {mcOn && mcPeak(ln.node) > 0 && (
                        <span className="pv-mc" title="สัปดาห์ที่ใช้เครื่องมากสุด (เครื่องตัวเดิมวิ่งต่อหลายสัปดาห์ จึงไม่บวกข้ามสัปดาห์)">
                          {' '}({mcPeak(ln.node)})
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
              {!lines.length && (
                <tr><td className="hint" colSpan={rowNames.length + pivot.heads.length + 1}>
                  ไม่มีข้อมูล — ลองเปลี่ยนชีท ปิดตัวกรอง หรือเลือกฟิลด์แถวอย่างน้อย 1 ฟิลด์
                </td></tr>
              )}
            </tbody>
            {!!lines.length && (
              <tfoot>
                <tr className="pv-grand">
                  <td className="pv-rc first" colSpan={rowNames.length}>รวมทั้งหมด</td>
                  {pivot.heads.map(h => {
                    const n = mcOn ? mcOf(pivot.root, h.key) : 0
                    // แถวรวมทั้งหมดเกือบทุกสัปดาห์มีงานสีปนอยู่แล้ว → ไม่ทาสีซ้ำ (รกตา)
                    // แต่ยังบอกยอดงานสีของสัปดาห์นั้นใต้เมาส์ไว้ให้
                    return (
                      <td key={h.key} className="pv-v" title={clrOn ? clrTitle(clrOf(pivot.root, h.key)) : undefined}>
                        {fmtVal(cellOf(pivot.root, h.key), agg)}
                        {n > 0 && <span className="pv-mc" title={`${n} เครื่องทั้งหมดในสัปดาห์นี้`}> ({n})</span>}
                      </td>
                    )
                  })}
                  <td className="pv-v pv-tot">
                    {fmtVal(grandTotal, agg)}
                    {mcOn && mcPeak(pivot.root) > 0 && (
                      <span className="pv-mc" title="สัปดาห์ที่ใช้เครื่องมากสุด"> ({mcPeak(pivot.root)})</span>
                    )}
                  </td>
                </tr>
              </tfoot>
            )}
          </table>
        </div>
      )}

      {fpop && fpData && (
        <ColumnFilter
          key={fpop.field}
          available={fpData.available} domain={fpData.domain}
          selected={fieldFilters[fpop.field]} anchor={fpop.anchor}
          onApply={set => applyFieldFilter(fpop.field, set)}
          onClose={() => setFpop(null)} />
      )}
    </div>
  )
}

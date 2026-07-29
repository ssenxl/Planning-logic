import React, { useMemo, useState, useRef, useLayoutEffect, useEffect } from 'react'
import { norm } from './ColumnFilter.jsx'

// จานสีแยกประเภท (Tableau-20) — รองรับหลายกลุ่ม CAT/เกจ
const PALETTE = ['#4e79a7', '#f28e2b', '#59a14f', '#e15759', '#76b7b2',
  '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac',
  '#a0cbe8', '#ffbe7d', '#8cd17d', '#ff9d9a', '#86bcb6',
  '#f1ce63', '#d4a6c8', '#fabfd2', '#d7b5a6', '#79706e']

// ไล่เฉดสีตามประเภท CAT (เข้มสุด → อ่อนสุด): DOUBLE = Teal, SINGLE = ส้ม → เหลืองอ่อน
const CAT_GRAD = {
  DOUBLE: ['#004d40', '#00695c', '#00796b', '#00897b', '#009688',
    '#26a69a', '#4db6ac', '#80cbc4', '#b2dfdb', '#e0f2f1'],
  SINGLE: ['#006064', '#00838f', '#0097a7', '#00acc1', '#00bcd4',
    '#26c6da', '#4dd0e1', '#80deea', '#b2ebf2', '#e0f7fa'],
}
const _hexRgb = h => { const n = parseInt(h.slice(1), 16); return [(n >> 16) & 255, (n >> 8) & 255, n & 255] }
const _rgbHex = a => '#' + a.map(x => Math.round(x).toString(16).padStart(2, '0')).join('')
const _lerpHex = (a, b, t) => { const A = _hexRgb(a), B = _hexRgb(b); return _rgbHex([0, 1, 2].map(i => A[i] + (B[i] - A[i]) * t)) }
// ไล่ข้ามหลายสต็อป: t∈[0,1] → เลือกช่วงในอาเรย์แล้ว lerp ต่อ
const _lerpStops = (stops, t) => {
  if (stops.length <= 1) return stops[0]
  const x = t * (stops.length - 1)
  const i = Math.min(stops.length - 2, Math.floor(x))
  return _lerpHex(stops[i], stops[i + 1], x - i)
}
// สีตัวอักษรที่อ่านออกบนพื้นสีนั้น (พื้นสว่าง → ตัวเข้ม, พื้นเข้ม → ตัวขาว)
const readableText = h => { const [r, g, b] = _hexRgb(h); return (0.299 * r + 0.587 * g + 0.114 * b) / 255 > 0.62 ? '#1f2430' : '#fff' }
// จับกลุ่มแบบ prefix เพื่อรองรับการสะกดต่างกัน (SINGLE / SINGEL / SINGLE TUBE, DOUBLE ฯลฯ)
const catGroupOf = key => { const u = String(key).toUpperCase(); return u.includes('DOUB') ? 'DOUBLE' : u.includes('SING') ? 'SINGLE' : null }

// คอลัมน์ที่ใช้เป็น "หัวแถว" ฝั่งซ้าย (เรียงซ้าย→ขวา) + ป้ายภาษาไทย + ความกว้าง(px)
const GROUP_DEF = [
  { col: 'CAT', label: 'Category', width: 92 },
  { col: 'MC_GUAGE', label: 'Guage', width: 64 },
  { col: 'MC_GROUP', label: 'Machine', width: 96 },
]

// คอลัมน์หัวแถวเพิ่มเติม (ไม่ได้มาจาก grid) — เครื่องที่กันไว้ให้ POLY/COTTON ของกลุ่ม CAT|เกจ
// ยอดกันไว้ไม่ขึ้นกับสัปดาห์ → โชว์เป็นคอลัมน์ sticky ให้เช็คได้ทีเดียวว่าแถวไหนมีเครื่องล็อกไว้
const RSV_COL = { label: 'กันเครื่อง', width: 120 }

// คอลัมน์ที่ใช้กำหนด "สี" ของบล็อก (แยกสีตาม CAT + เกจ)
const COLOR_DEF = ['CAT', 'MC_GUAGE']

// คู่เครื่องที่สลับกันทำแทนกันได้ (เครื่อง pool เดียวกัน CAT|เกจ) — ต้องสอดคล้องกับ
// MC_GROUP_REDIRECT ใน Planning.py (เช่น SKP G20 ถูก redirect ไป FA G20)
// ถ้าแถวใดมี (CAT, เกจ) ตรงกลุ่มนี้ จะโชว์ทุก MC_GROUP ในกลุ่มแม้ยังไม่มีงานวาง
// เพื่อให้ลากงานมาวาง (ย้ายข้ามเครื่อง) ได้
const SWAP_GROUPS = [
  { cat: 'SINGLE-32', gauge: '20', groups: ['FA', 'SKP'] },
]
// normalize สำหรับเทียบค่า (trim + upper) เพราะ norm() ไม่ได้ trim/upper ให้
const nkey = (v) => String(v ?? '').trim().toUpperCase()

// จำแนกประเภท item ตาม prefix (ให้ตรงกับ _get_item_cotton_poly ใน Planning.py):
// FD5/F5 → COTTON, FD4/F4 → POLY, อื่นๆ → '' (งานปกติ)
// ใช้แยกว่าแถวแผนนี้กินเครื่อง sub-pool ที่กันไว้ (POLY/COTTON) หรือกินเครื่องปกติ
function itemPoolType(item) {
  const s = nkey(item)
  if (s.startsWith('FD5') || s.startsWith('F5')) return 'cotton'
  if (s.startsWith('FD4') || s.startsWith('F4')) return 'poly'
  return ''
}

/**
 * Gantt แผนผลิต: แกนตั้ง = เครื่อง/เกจ/CAT, แกนนอน = PLAN_WEEK
 * - บล็อก = งาน 1 รายการ (ITEM_CODE) กว้าง 1 สัปดาห์ สีตาม CAT + เกจ
 * - ลากบล็อกไปคอลัมน์สัปดาห์อื่น → เปลี่ยน PLAN_WEEK (กลุ่มแถวคงเดิม) แล้ว sync ตาราง
 *
 * props:
 *   columns : string[]              — หัวคอลัมน์ของ grid
 *   rows    : [{ row, idx }]        — แถวที่ผ่านตัวกรอง (จาก filterRows) พร้อม index จริง
 *   onMoveWeek(idx, weekStr)        — เรียกเมื่อผู้ใช้ลากบล็อกไปสัปดาห์ใหม่
 */
// ประเภทงาน (ตรงกับ REMAINING_JOBS) + ป้ายแสดงผล
export const LOAD_TYPES = [
  { key: 'OM', label: 'OM', long: 'OMNOI' },
  { key: 'PHET_DOUBLE', label: 'PHET DOUBLE', long: 'PHET (DOUBLE)' },
  { key: 'PHET_SINGLE', label: 'PHET SINGLE', long: 'PHET (SINGLE)' },
]

// ประเภทเครื่องของแถวแผน → emoji ติดหน้า item บนบล็อก
// 🔧 setup เครื่องใหม่ — กินเครื่องจากพูล + เสียเวลา setup
// 📦 เติมเข้าเครื่องที่วิ่งอยู่ — เครื่องของ booking (MC_BOOKING) หรือของ SO อื่นในแผน
//    (MC_SHARED) ที่ถักไอเทมนี้อยู่แล้ว → ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup
//    (รายละเอียดว่าเครื่องของใคร ดูใน REMARK ที่ tooltip)
// ⏩ เครื่องของแผนเองวิ่งต่อจากสัปดาห์ก่อน — ไม่ต้อง setup
// 🧵 จ้างทอ (S9) — user แบ่งงานก้อนนี้ออกไปทอข้างนอก ใช้เครื่องจ้างทอ ไม่กินเครื่องในบ้าน
const MC_KINDS = {
  outsource: { icon: '🧵', label: 'จ้างทอ (ไม่ใช้เครื่องในบ้าน)' },
  setup: { icon: '🔧', label: 'setup เครื่องใหม่' },
  onExisting: { icon: '📦', label: 'เติมเข้าเครื่องที่วิ่งอยู่ (ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup)' },
  carry: { icon: '⏩', label: 'Continue' },
}
function mcKind(newMc, carryMc, sharedMc, bookingMc, outsource) {
  if (outsource) return 'outsource'
  if (newMc > 0) return 'setup'
  if (sharedMc > 0 || bookingMc > 0) return 'onExisting'
  if (carryMc > 0) return 'carry'
  return ''
}

// เครื่องหมายอื่นบนบล็อก (นอกจาก emoji ประเภทเครื่อง) — ใช้เรนเดอร์ legend ใต้ Gantt
// sample = ตัวอย่างหน้าตาจริงบนบล็อก, cls = คลาสที่ทำให้ตัวอย่างสีตรงกับของจริง
const BAR_MARKS = [
  {
    key: 'core', cls: 'lg-core', sample: 'FD5xxxx', label: 'Core item',
    tip: 'ชื่อ item เป็นสีแดงหนา = item หลัก (IS_CORE_ITEM) — ผลิตประจำ ควรรักษาเครื่องไว้',
  },
  {
    key: 'color', cls: 'lg-color', sample: '★', label: 'งานสี (ต้องย้อม)',
    tip: 'บล็อกสีส้ม + ★ = งานที่ต้องผ่านย้อมสี (มาจากหน้า Order Color)',
  },
  {
    key: 'fold', cls: 'lg-fold', sample: '⚠', label: 'order หาร 6 พับไม่ลงตัว',
    tip: 'บล็อกแดงทั้งก้อน = ยอดที่ลูกค้าเปิดมา (ทั้ง SC) แบ่งพับแล้วหาร 6 ไม่ลงตัว (เฉพาะ IRMT/SJT)\n'
      + 'ติดทุกสัปดาห์ของ SC นั้น — ต้องไปแก้ที่ยอดเปิด order ไม่ใช่แก้บนแผน',
  },
  {
    key: 'rdd', cls: 'lg-rdd', sample: 'RDD W32', label: 'วางเลยกำหนดถักเสร็จ',
    tip: 'ป้าย RDD สีแดง = PLAN_WEEK เลย TARGET_KNIT (RDD หักเวลางานหลังถักแล้ว) → ถักไม่ทันกำหนดส่ง\n'
      + 'ต้องลากขึ้นสัปดาห์ก่อนหน้า หรือแบ่งงาน/จ้างทอ',
  },
  {
    key: 'lock', cls: 'lg-lock', sample: '🔒', label: 'สัปดาห์ freeze (แก้ไม่ได้)',
    tip: 'สัปดาห์ที่ล็อกไว้แล้ว — บล็อกจางลง ช่องเป็นลายทาง ลาก/แก้จำนวน/ลบไม่ได้',
  },
  {
    key: 'booking', cls: 'lg-booking', sample: '📋', label: 'History แผนเดิม (ดูอย่างเดียว)',
    tip: 'บล็อกลายเส้นประ = แผนเดิมย้อนหลังสูงสุด 2 สัปดาห์ ไว้เทียบว่าเครื่องเดิมถักอะไรอยู่\n'
      + 'ไม่ใช่งานของแผนรอบนี้ — ลาก/แก้/หักเครื่องว่างไม่ได้',
  },
]

// ชิปเครื่องว่างมุมขวาบนของแต่ละช่อง (คลาส .cellava) — เขียว/เหลือง/แดง
const AVA_MARKS = [
  {
    key: 'free', cls: '', sample: 'ว่าง 3', label: 'ยังมีเครื่องว่าง',
    tip: 'เครื่องว่าง live = เครื่องทั้งหมด − booking − ที่แผนจองเพิ่มในสัปดาห์นั้น',
  },
  {
    key: 'full', cls: 'none', sample: 'เต็ม', label: 'ใช้ครบพอดี',
    tip: 'เครื่องถูกใช้หมดพอดี — วางงานเพิ่มในช่องนี้จะเกินทันที',
  },
  {
    key: 'over', cls: 'over', sample: 'ไม่ว่าง (เกิน 2)', label: 'วางเกินเครื่องที่มี',
    tip: 'วางงานเกินจำนวนเครื่องที่มีจริง — ต้องย้ายงานออกไปสัปดาห์อื่น หรือลดจำนวน',
  },
  {
    key: 'rsv', cls: 'lg-rsv', sample: '🔒 P2 C1', label: 'เครื่องกันไว้ POLY/COTTON คงเหลือ',
    tip: 'เครื่องที่กันไว้ให้งาน POLY (P) / COTTON (C) โดยเฉพาะ — ใช้แทนงานปกติไม่ได้\n'
      + 'เลข = กันไว้ − booking ที่ใช้ไปแล้ว − ที่แผนใช้ในสัปดาห์นั้น (แดง = วางเกิน)',
  },
]

// เลขสัปดาห์ปัจจุบันตามนิยาม Fri–Thu (บวก 3 วันก่อนคิด ISO week) — ต้องตรงกับ Calendar.py
function currentPlanWeek() {
  const now = new Date()
  const d = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate() + 3))
  const dayNum = (d.getUTCDay() + 6) % 7          // จันทร์=0 ... อาทิตย์=6
  d.setUTCDate(d.getUTCDate() - dayNum + 3)        // เลื่อนไปวันพฤหัสของสัปดาห์ ISO
  const firstThursday = d.getTime()
  d.setUTCMonth(0, 1)
  if (d.getUTCDay() !== 4)
    d.setUTCMonth(0, 1 + ((4 - d.getUTCDay()) + 7) % 7)
  return 1 + Math.ceil((firstThursday - d.getTime()) / 604800000)
}

// จำแนกแถว PLAN เป็นประเภท OM / PHET_DOUBLE / PHET_SINGLE (null = ไม่นับ เช่น OUTSOURCE)
function classifyType(factory, cat) {
  const f = String(factory).trim().toUpperCase()
  if (f.startsWith('OM')) return 'OM'
  if (f === 'PHET') return String(cat).toUpperCase().includes('DOUBLE') ? 'PHET_DOUBLE' : 'PHET_SINGLE'
  return null
}

// ─── ข้อมูลเสริมบนบล็อก (chip ติ๊กเปิด/ปิด) ───────────────────────────────
// user ต่างคนดูข้อมูลคนละอย่าง → ให้ติ๊กเองว่าจะโชว์อะไรบนบล็อก แล้วจำไว้ใน localStorage
// build(v) : v(col) = ค่าคอลัมน์ของแถวนั้น → คืน string ที่จะโชว์ ('' = ไม่โชว์)
export const BAR_FIELDS = [
  { key: 'sc', label: 'SC', build: v => v('SC_SO_NO') },
  { key: 'mc', label: 'เครื่องที่ใช้', build: v => { const m = v('ACTUAL_MC'); return Number(m) > 0 ? `${m} เครื่อง` : '' } },
  { key: 'po', label: 'PO', build: v => v('PO_NO') },
  { key: 'rdd', label: 'RDD', build: v => { const w = rddWeekNo(v); return w ? `RDD W${w}` : '' } },
  { key: 'setup', label: 'setup (วัน)', build: v => { const d = v('SETUP_DAYS'); return Number(d) > 0 ? `setup ${d} ว.` : '' } },
  { key: 'customer', label: 'ลูกค้า', build: v => v('CUSTOMER') },
  { key: 'left', label: 'คงเหลือ', build: v => { const q = v('PLAN_QTY'); return q !== '' && Number(q) > 0 ? `เหลือ ${q}` : '' } },
  { key: 'color', label: 'สี', build: v => v('COLOR_DESC') || v('NAY_COLOR') },
  { key: 'material', label: 'เนื้อผ้า', build: v => v('MATERIAL_CONTENT') },
]
// v2 = ชุดฟิลด์ใหม่ (แยก SC/PO, default = SC + เครื่องที่ใช้) — key ใหม่เพื่อล้างค่าที่ user เคยติ๊กไว้ในชุดเก่า
const BAR_FIELDS_KEY = 'knitplan.gantt.barFields.v2'
export const BAR_FIELDS_DEFAULT = { sc: true, mc: true }

// RDD_WEEK / FG_WEEK เก็บเป็น YYYYWW (เช่น 202632) → คืนเลขสัปดาห์ล้วน (32) ไว้แสดงผล
function rddWeekNo(v) {
  const raw = String(v('RDD_WEEK') || v('FG_WEEK') || '').trim()
  if (!/^\d+$/.test(raw)) return ''
  return raw.length > 2 ? String(Number(raw.slice(-2))) : String(Number(raw))
}
// เดดไลน์ถัก = TARGET_KNIT (RDD หักเวลางานหลังถักแล้ว) → เทียบกับ PLAN_WEEK ตรงๆ ได้
// PLAN_WEEK เลย TARGET_KNIT = ถักไม่ทันกำหนดส่ง
function isLateRdd(v, week) {
  const t = Number(v('TARGET_KNIT'))
  return Number(t) > 0 && week !== '' && Number(week) > t
}
export function loadBarFields() {
  try {
    const s = JSON.parse(localStorage.getItem(BAR_FIELDS_KEY))
    if (s && typeof s === 'object') return s
  } catch { /* ค่าเสีย → ใช้ default */ }
  return BAR_FIELDS_DEFAULT
}
export function saveBarFields(fields) {
  try { localStorage.setItem(BAR_FIELDS_KEY, JSON.stringify(fields)) } catch { /* โควตาเต็ม — ข้ามได้ */ }
}

// ─── Panel รายละเอียดงาน (คลิกบล็อก) ────────────────────────────────────
// จัดกลุ่มคอลัมน์ของชีท PLAN ให้อ่านง่าย — คอลัมน์ที่ไม่อยู่ในนี้จะไปรวมที่ท้าย "คอลัมน์อื่นๆ"
// เพื่อให้เห็นข้อมูลครบเท่า Excel โดยไม่ต้องเปิดไฟล์
const PANEL_GROUPS = [
  {
    title: 'งาน', fields: [
      ['ITEM_CODE', 'Item'], ['CAT', 'CAT'], ['MC_GUAGE', 'เกจ'], ['MC_GROUP', 'เครื่อง'],
      ['PLAN_WEEK', 'สัปดาห์ผลิต'], ['FACTORY_TYPE', 'โรงงาน'],
    ]
  },
  {
    title: 'ลูกค้า / ออร์เดอร์', fields: [
      ['CUSTOMER', 'ลูกค้า'], ['SC_SO_NO', 'SC/SO'], ['SC_LINE_ID', 'SC Line'], ['PO_NO', 'PO'],
      ['ORDER_TYPE', 'ประเภทออร์เดอร์'], ['ORDER_DATE', 'วันที่ order'],
    ]
  },
  {
    title: 'สี / เนื้อผ้า', fields: [
      ['COLOR_DESC', 'สี'], ['NAY_COLOR', 'สี NAY'], ['SUB_COLOR', 'Sub color'],
      ['MATERIAL_CONTENT', 'เนื้อผ้า'], ['IS_CORE_ITEM', 'Core item'],
    ]
  },
  {
    title: 'จำนวน (กก.)', fields: [
      ['ORDERS_QTY', 'สั่งทั้งหมด'], ['PRODUCE_QTY', 'ผลิตสัปดาห์นี้'], ['PLAN_QTY', 'คงเหลือหลังสัปดาห์นี้'],
      ['PENDING_PLAN', 'รอวางแผน'], ['DAILY_CAPACITY', 'กำลังผลิต/วัน'], ['REVOLUTION_WEIGHT', 'Revolution/Weight'],
    ]
  },
  {
    title: 'เครื่อง / setup', fields: [
      ['SETUP_DAYS', 'setup (วัน)'], ['ACTUAL_MC', 'เครื่องที่ใช้จริง'], ['REQUIRED_MC', 'เครื่องที่ต้องใช้'],
      ['NEW_MC', 'เครื่อง setup ใหม่'], ['CARRYOVER_MC', 'เครื่องต่อเนื่อง'], ['MC_SHARED', 'เครื่องใช้ร่วม'],
      ['MC_BOOKING', 'เครื่อง booking'], ['ACTUAL_WORKING_DAYS', 'วันทำงานจริง'],
      ['CALENDAR_WORKING_DAYS', 'วันทำงานปฏิทิน'], ['AVAILABLE_DAYS', 'วันที่ใช้ผลิตได้'],
    ]
  },
  {
    title: 'กำหนดส่ง / วัตถุดิบ', fields: [
      ['RDD_WEEK', 'RDD (สัปดาห์)'], ['FG_WEEK', 'FG week'], ['TARGET_KNIT', 'เป้าถัก (สัปดาห์)'],
      ['EARLIEST_PLAN_WEEK', 'เร็วสุดที่ทำได้'], ['LT_YARN', 'LT ด้าย'], ['YARN_USED', 'ด้ายที่ใช้'],
      ['DATE_IN', 'Date in'],
    ]
  },
  {
    title: 'หมายเหตุ', wide: true, fields: [
      ['REMARK', 'REMARK'], ['OUTSOURCE', 'จ้างทอ (user สั่ง)'], ['PLAN_SOURCE', 'ที่มาของแผน'],
    ]
  },
]
const PANEL_KNOWN = new Set(PANEL_GROUPS.flatMap(g => g.fields.map(f => f[0])))

export default function PlanGantt({ columns, rows, load = {}, ava = {}, bookingMc = {}, poolMap = {}, onMoveWeek, colorRows, onRemove, onEditQty, onSplit, lockBefore = null, bookingItems = [], bookingMode = 'off', bookingPick = null, loadFilter = null, setLoadFilter = () => {}, barFields = BAR_FIELDS_DEFAULT, selIdx = null, setSelIdx = () => {} }) {
  const [dragIdx, setDragIdx] = useState(null)
  const [overWeek, setOverWeek] = useState(null)
  // double click บล็อก → แก้ตัวเลขจำนวน (กก.) inline แล้วส่งค่าใหม่ผ่าน onEditQty(idx, qty)
  const [editIdx, setEditIdx] = useState(null)
  const [editVal, setEditVal] = useState('')
  // คลิกบล็อก 1 ครั้ง → เปิด panel รายละเอียด (idx ของแถวใน grid)
  // selIdx/setSelIdx ยกไปไว้ที่ KnitPlan → เปิด modal ตารางคู่กับการ์ดได้
  const [showOther, setShowOther] = useState(false)   // กาง "คอลัมน์อื่นๆ" ใน panel
  // barFields (ฟิลด์ที่โชว์บนบล็อก) + loadFilter → สถานะยกไปไว้ที่ KnitPlan (ปุ่มอยู่แถบบน)
  // ย่อ/ขยาย คำอธิบาย (hint + legend) ใต้ตาราง — เริ่มต้นย่อไว้ กดค่อยโชว์
  const [showFoot, setShowFoot] = useState(false)
  // กรองหัวแถวซ้าย: คลิกช่อง Category/Guage → กรอง CAT+เกจ, คลิก Machine → กรองถึงเครื่อง
  // ค่า = { cat, gauge, mcgroup(หรือ null) } ที่ normalize แล้ว, null = แสดงทั้งหมด
  const [catFilter, setCatFilter] = useState(null)
  // สัปดาห์ที่ล็อก (freeze) — โชว์ได้แต่ลาก/ถอด/วางไม่ได้
  // ยกเว้นสัปดาห์ "ปีหน้า" (เลขเล็กที่วนข้ามปี) ที่เลข < lockBefore แต่จริง ๆ อยู่อนาคต
  const isLocked = (w) => {
    if (nextYearWeeks.has(String(w)) || Number(w) === 99) return false
    if (lockBefore != null && Number(w) < Number(lockBefore)) return true
    // booking overlay อาจเผยสัปดาห์ที่ freeze/ผ่านมาแล้ว (< current+2) → ล็อกไว้ดูอย่างเดียว
    // กันลากงานแผนย้อนไปวางในสัปดาห์อดีต
    if (bookingMode !== 'off' && Number(w) < currentPlanWeek() + 2) return true
    return false
  }

  // Esc = ปิด panel รายละเอียด
  useEffect(() => {
    if (selIdx == null) return
    const onKey = (e) => { if (e.key === 'Escape') setSelIdx(null) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [selIdx])

  // ชื่อคอลัมน์ → index (ใช้อ่านค่าฟิลด์ใดก็ได้ของแถว โดยไม่ต้องประกาศทีละตัวใน ci)
  const colIdx = useMemo(() => {
    const m = {}
    columns.forEach((c, i) => { m[c] = i })
    return m
  }, [columns])
  const valOf = (row) => (name) => {
    const i = colIdx[name]
    return i == null ? '' : norm(row[i])
  }

  const ci = useMemo(() => {
    const at = (name) => columns.indexOf(name)
    return {
      week: at('PLAN_WEEK'), item: at('ITEM_CODE'), qty: at('PRODUCE_QTY'),
      reqmc: at('REQUIRED_MC'), factory: at('FACTORY_TYPE'), cat: at('CAT'),
      newmc: at('NEW_MC'), gauge: at('MC_GUAGE'), actualmc: at('ACTUAL_MC'),
      carrymc: at('CARRYOVER_MC'), sharedmc: at('MC_SHARED'),
      bookingmc: at('MC_BOOKING'), remark: at('REMARK'), mcgroup: at('MC_GROUP'),
      outsource: at('OUTSOURCE'), sc: at('SC_SO_NO'),
    }
  }, [columns])

  // คอลัมน์หัวแถวที่มีอยู่จริง + ตำแหน่ง sticky (left สะสม)
  const groups = useMemo(() => {
    let left = 0
    return GROUP_DEF
      .filter(g => columns.includes(g.col))
      .map(g => { const item = { ...g, idx: columns.indexOf(g.col), left }; left += g.width; return item })
  }, [columns])

  // คอลัมน์กำหนดสี (CAT + เกจ) ที่มีจริง
  const colorCols = useMemo(
    () => COLOR_DEF.filter(n => columns.includes(n)).map(n => columns.indexOf(n)),
    [columns])

  const supported = groups.length > 0 && ci.week >= 0
  const rowKey = (row) => groups.map(g => norm(row[g.idx])).join('')
  // ตำแหน่งคอลัมน์ CAT / เกจ ใน group (ใช้ทำ key ของ AVA = "CAT|เกจ")
  const avaCatI = groups.findIndex(g => g.col === 'CAT')
  const avaGaugeI = groups.findIndex(g => g.col === 'MC_GUAGE')
  const mcGroupI = groups.findIndex(g => g.col === 'MC_GROUP')
  const colorKey = (row) => colorCols.length
    ? colorCols.map(i => norm(row[i]) || '(ว่าง)').join(' / ')
    : 'ทั้งหมด'

  // เครื่องที่กันไว้ (POLY/COTTON) ต่อ CAT|เกจ — ยอดกันไว้เท่ากันทุกสัปดาห์
  // → รวบจาก ava ของสัปดาห์ไหนก็ได้ (เอาสัปดาห์แรกที่เจอ key นั้น) มาเป็นค่าคงที่ต่อแถว
  const rsvByCat = useMemo(() => {
    const m = new Map()
    for (const wk of Object.keys(ava || {})) {
      for (const [key, slot] of Object.entries(ava[wk] || {})) {
        if (m.has(key)) continue
        const rv = slot?.reserved
        if (rv && (rv.poly || rv.cotton)) m.set(key, { poly: rv.poly || 0, cotton: rv.cotton || 0 })
      }
    }
    return m
  }, [ava])
  // โชว์คอลัมน์ "กันเครื่อง" เฉพาะเมื่อมีเครื่องกันไว้จริง และรู้ CAT|เกจ ของแถว
  const showRsv = avaCatI >= 0 && avaGaugeI >= 0 && rsvByCat.size > 0
  const groupsW = groups.reduce((s, g) => s + g.width, 0)
  // key ที่ใช้หา ava/reserved — รองรับ "พูลแยก" (เช่น SKP vs SKPTA/SKPLE):
  // ถ้า (cat,gauge,เครื่อง) นั้นอยู่ใน poolMap → ใช้ pool key เฉพาะพูล; ไม่งั้น = cat|gauge เดิม
  const poolKeyOf = (cat, gauge, mcgroup) => {
    const cg = norm(cat) + '|' + norm(gauge)
    const mg = String(mcgroup ?? '').trim().toUpperCase()
    return (mg && poolMap[cg + '|' + mg]) || cg
  }
  const rsvOfRow = (vals) => rsvByCat.get(poolKeyOf(vals[avaCatI], vals[avaGaugeI], mcGroupI >= 0 ? vals[mcGroupI] : '')) || null

  // item จาก booking (แผนเก่าที่ commit แล้ว) ที่จะ overlay บน Gantt เป็นบล็อกอ่านอย่างเดียว
  //   off  = ไม่โชว์
  //   all  = ทุก item ทุกสัปดาห์ใน booking
  //   plan = เฉพาะ item ที่กำลังทำแผนวันนี้ (ITEM_CODE ∈ bookingPick ที่ user ติ๊กเลือก) — โชว์ประวัติของมัน
  // จัดเป็น cells (key=rowKey||week), rows (rowKey→vals ให้เพิ่มแถวเครื่องที่แผนไม่มี), weeks (ไว้ขยายแกน)
  const bookingData = useMemo(() => {
    const empty = { cells: new Map(), rows: new Map(), weeks: new Set() }
    if (bookingMode === 'off' || !supported || !bookingItems.length) return empty
    const FIELD = { CAT: 'cat', MC_GUAGE: 'gauge', MC_GROUP: 'mc_group' }
    // ต้อง map ทุกคอลัมน์หัวแถวเป็นฟิลด์ booking ได้ ไม่งั้น key ไม่ตรงแถวแผน → ไม่ overlay
    if (!groups.every(g => FIELD[g.col])) return empty
    // โหมด plan: กรองด้วยชุด ITEM_CODE ที่ติ๊กไว้ (null/ไม่มี = ไม่โชว์อะไร) ; all = ไม่กรอง item
    const allowed = bookingMode === 'plan' ? (bookingPick || new Set()) : null
    // โชว์เฉพาะสัปดาห์ปัจจุบันเป็นต้นไป — ไม่ดึงสัปดาห์อดีต (booking มี week ย้อนหลังถึง 24) มายืดแกนซ้าย
    const wkFloor = currentPlanWeek()
    const cells = new Map(), rowsM = new Map(), weeks = new Set()
    for (const b of bookingItems) {
      const wk = norm(b.week)
      if (wk === '' || Number(b.week) < wkFloor || Number(b.week) === 99) continue
      if (allowed && !allowed.has(String(b.item).toUpperCase())) continue
      const vals = groups.map(g => norm(b[FIELD[g.col]]))
      const key = vals.join('')             // ต้องตรงกับ rowKey(row) ของแผน
      if (!rowsM.has(key)) rowsM.set(key, vals)
      weeks.add(wk)
      const ck = key + '||' + wk
      let arr = cells.get(ck)
      if (!arr) { arr = []; cells.set(ck, arr) }
      arr.push(b)
    }
    return { cells, rows: rowsM, weeks }
  }, [bookingItems, bookingMode, bookingPick, groups, supported])

  // แกนสัปดาห์ — โชว์ทุกสัปดาห์ที่ทำแผนได้ (ลากงานไปได้ทุก week)
  // รวมสัปดาห์จาก rows(งาน) + load(capacity) + ava(เครื่องว่าง) เพื่อครอบคลุม horizon เต็ม
  // ตัดสัปดาห์ที่ freeze ออก: เริ่มที่ current+2 (สัปดาห์ปัจจุบัน+สัปดาห์หน้าแก้แผนไม่ได้)
  const weeks = useMemo(() => {
    if (!supported) return []
    const vals = new Set()
    const jobW = new Set()   // สัปดาห์ที่มีงานวางจริง (จาก rows) — ใช้ระบุสัปดาห์ "ปีหน้า"
    for (const { row } of rows) { const v = norm(row[ci.week]); if (v !== '') { vals.add(v); jobW.add(v) } }
    for (const w of Object.keys(load || {})) vals.add(String(w))
    for (const w of Object.keys(ava || {})) vals.add(String(w))
    // สัปดาห์จาก booking overlay (แผนเก่า) — ต้องโผล่เป็นคอลัมน์แม้แผนไม่มีงานในสัปดาห์นั้น
    for (const w of bookingData.weeks) vals.add(String(w))
    const arr = [...vals]
    const nums = arr.map(Number).filter(Number.isFinite)
    if (arr.length && nums.length === arr.length) {
      // ปกติเริ่ม current+2 (freeze ซ่อน); ถ้า lockBefore ส่งมา = โชว์รวมสัปดาห์ freeze (ล็อกไว้)
      let lo = lockBefore != null ? Number(lockBefore) - 2 : currentPlanWeek() + 2
      // เปิด booking overlay: ขยายขอบซ้ายให้ครอบสัปดาห์ booking ที่เก่าสุด (เป็นสัปดาห์ในปีนี้ที่ผ่านมา)
      if (bookingData.weeks.size) {
        const bk = [...bookingData.weeks].map(Number).filter(n => Number.isFinite(n) && n !== 99)
        if (bk.length) lo = Math.min(lo, ...bk)
      }
      // W99 = sentinel "งานล้น" → ไม่โชว์ใน Gantt (ผู้ใช้ขอ)
      // สัปดาห์ปีนี้ = เลข >= lo ; สัปดาห์ปีหน้า = สัปดาห์ที่มีงานจริงแต่เลข < lo (แผนวางไป
      //   ข้างหน้าเท่านั้น เลขเล็กจึงคือปีหน้าที่วนรอบ 52/53 → 1,2,3) นำมาต่อท้ายปีนี้
      // ตัดสัปดาห์หยุด (ไม่มีทั้งงาน/AVA/โหลด เช่น W31) ออกโดยปริยาย เพราะกรองจาก vals อยู่แล้ว
      const thisYear = arr.filter(w => { const n = Number(w); return n >= lo && n !== 99 })
        .sort((a, b) => Number(a) - Number(b))
      const nextYear = arr.filter(w => { const n = Number(w); return n < lo && n !== 99 && jobW.has(w) })
        .sort((a, b) => Number(a) - Number(b))
      return [...thisYear, ...nextYear]
    }
    return arr.filter(w => w !== '99').sort((a, b) => String(a).localeCompare(String(b), 'th', { numeric: true }))
  }, [rows, ci, supported, load, ava, lockBefore, bookingData])

  // สัปดาห์ "ปีหน้า" (เลขเล็กที่วนข้ามปี) — ไม่ควรถูกล็อกในโหมด freeze แม้เลข < lockBefore
  const nextYearWeeks = useMemo(() => {
    const s = new Set()
    if (!supported) return s
    const lo = lockBefore != null ? Number(lockBefore) - 2 : currentPlanWeek() + 2
    for (const { row } of rows) {
      const v = norm(row[ci.week])
      if (v !== '' && Number(v) < lo && Number(v) !== 99) s.add(v)
    }
    return s
  }, [rows, ci, supported, lockBefore])

  // แถว (unique combo ของ เครื่อง/เกจ/CAT)
  const gantRows = useMemo(() => {
    if (!supported) return []
    const m = new Map()
    const add = (vals) => {
      const key = vals.join('')          // ต้องตรงกับ rowKey(row)
      if (!m.has(key)) m.set(key, { key, vals })
    }
    for (const { row } of rows) add(groups.map(g => norm(row[g.idx])))

    // เติมแถวเครื่องคู่ swap ที่ยังไม่มี (เช่น SKP20 เมื่อมี FA20) ให้เป็นที่ลากมาวางได้
    if (avaCatI >= 0 && avaGaugeI >= 0 && mcGroupI >= 0) {
      for (const { vals } of [...m.values()]) {
        const sg = SWAP_GROUPS.find(s =>
          nkey(s.cat) === nkey(vals[avaCatI]) && nkey(s.gauge) === nkey(vals[avaGaugeI]))
        if (!sg) continue
        for (const gname of sg.groups) {
          if (nkey(gname) === nkey(vals[mcGroupI])) continue
          const nv = vals.slice()
          nv[mcGroupI] = gname
          add(nv)
        }
      }
    }
    // เติมแถวเครื่องจาก booking overlay ที่แผนไม่มี — ให้เห็น item booking ครบทุกเครื่อง
    for (const vals of bookingData.rows.values()) add(vals)
    return [...m.values()].sort((a, b) =>
      a.vals.join('|').localeCompare(b.vals.join('|'), 'th', { numeric: true }))
  }, [rows, groups, supported, avaCatI, avaGaugeI, mcGroupI, bookingData])

  // ประเภทโหลด (OM / PHET_DOUBLE / PHET_SINGLE) ของแต่ละแถว gantt
  // ใช้ classifyType เดียวกับแถวสรุปโหลด → กด label แถวโหลดแล้วกรองแถวงานให้ตรงกัน
  // งานที่ classify ไม่เข้าประเภท (เช่น จ้างทอ) ไม่มีใน map → ถูกซ่อนเมื่อเปิดกรอง
  const rowType = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const { row } of rows) {
      const t = classifyType(ci.factory >= 0 ? row[ci.factory] : '', ci.cat >= 0 ? row[ci.cat] : '')
      if (t) m.set(rowKey(row), t)
    }
    return m
  }, [rows, ci, groups, supported])

  // ค่าที่ใช้แยกสี (เรียงเพื่อ map สีคงที่)
  const colorKeys = useMemo(() => {
    if (!supported) return []
    const s = new Set()
    for (const { row } of rows) s.add(colorKey(row))
    return [...s].sort((a, b) => a.localeCompare(b, 'th', { numeric: true }))
  }, [rows, colorCols, supported])

  // สีพื้นของแต่ละ key: ไล่เฉดภายในกลุ่ม DOUBLE / SINGLE ตามลำดับ colorKeys (sort แล้ว)
  // เช่น DOUBLE หลายเกจ → น้ำเงินเข้ม (เกจแรก) ค่อยๆ อ่อนลงเป็นฟ้า (เกจท้าย)
  const colorMap = useMemo(() => {
    const m = new Map()
    const buckets = { DOUBLE: [], SINGLE: [] }
    for (const k of colorKeys) { const g = catGroupOf(k); if (g) buckets[g].push(k) }
    for (const g of ['DOUBLE', 'SINGLE']) {
      const arr = buckets[g]
      arr.forEach((k, i) => m.set(k, _lerpStops(CAT_GRAD[g], arr.length <= 1 ? 0 : i / (arr.length - 1))))
    }
    return m
  }, [colorKeys])
  const colorOf = (key) => colorMap.get(key)
    || PALETTE[(colorKeys.indexOf(key) < 0 ? 0 : colorKeys.indexOf(key)) % PALETTE.length]
  const textOf = (key) => readableText(colorOf(key))

  // จัดงานลงช่อง (rowKey × week)
  const cells = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const { row, idx } of rows) {
      const week = norm(row[ci.week])
      const key = rowKey(row) + '||' + week
      if (!m.has(key)) m.set(key, [])
      const num = (i) => (i >= 0 ? Number(norm(row[i])) || 0 : 0)
      const isOutsource = ci.outsource >= 0 && nkey(row[ci.outsource]) === 'YES'
      const v = valOf(row)
      // ป้ายข้อมูลเสริมบนบล็อก — เฉพาะฟิลด์ที่ user ติ๊กไว้ และมีค่าจริง
      const lateRdd = isLateRdd(v, week)
      const tags = []
      for (const f of BAR_FIELDS) {
        if (!barFields[f.key]) continue
        const text = f.build(v)
        if (text) tags.push({ key: f.key, text, warn: f.key === 'rdd' && lateRdd })
      }
      m.get(key).push({
        idx, row, tags, lateRdd,
        item: norm(row[ci.item]),
        ck: colorKey(row),
        qty: ci.qty >= 0 ? norm(row[ci.qty]) : '',
        actualmc: ci.actualmc >= 0 ? norm(row[ci.actualmc]) : '',
        mc: mcKind(num(ci.newmc), num(ci.carrymc), num(ci.sharedmc), num(ci.bookingmc), isOutsource),
        remark: ci.remark >= 0 ? norm(row[ci.remark]) : '',
        setup: v('SETUP_DAYS'),
        // ยอดที่ลูกค้าเปิดมาแบ่งพับแล้วหาร 6 ไม่ลงตัว (คิดจากยอดรวมทั้ง order ไม่ใช่รายสัปดาห์)
        foldWarn: String(v('FOLD_WARN')) === '1',
        foldQty: v('FOLD_QTY'),
        foldRem: v('FOLD_REMAINDER'),
        core: String(v('IS_CORE_ITEM')).trim() !== '',
      })
    }
    return m
  }, [rows, ci, groups, colorCols, supported, barFields, colIdx])

  // ผลรวม NEW_MC (job = setup 1 เครื่อง) ต่อ (สัปดาห์ × ประเภท) จากแถวที่วางจริง — live ตามที่ลาก/แก้
  const planNewByWeekType = useMemo(() => {
    const m = {}
    if (!supported) return m
    for (const { row } of rows) {
      const w = norm(row[ci.week]); if (w === '') continue
      const t = classifyType(ci.factory >= 0 ? row[ci.factory] : '', ci.cat >= 0 ? row[ci.cat] : '')
      if (!t) continue
      const nm = ci.newmc >= 0 ? (Number(norm(row[ci.newmc])) || 0) : 0
      m[w + '|' + t] = (m[w + '|' + t] || 0) + nm
    }
    return m
  }, [rows, ci, supported])

  // capacity สูงสุด (จำนวน job ที่ทำได้สูงสุด) ต่อประเภท — จาก REMAINING_JOBS.CAPACITY (คงที่ 13/33/44)
  // โชว์บน label แถวสรุปโหลด เช่น "โหลด OM · สูงสุด 13 job"
  const capByType = useMemo(() => {
    const m = {}
    for (const w in load) {
      const wk = load[w] || {}
      for (const t of Object.keys(wk)) {
        const c = wk[t] ? wk[t].cap : null
        if (c != null && c !== '' && (m[t] == null || c > m[t])) m[t] = c
      }
    }
    return m
  }, [load])

  // เครื่องที่ "แผนจองเพิ่มจากพูล" ต่อ (สัปดาห์ × CAT|เกจ) — live ตามที่ลาก → ใช้คิดเครื่องว่าง
  //
  // TOTAL_MC_REMAIN (av.remain) หักเครื่องที่ booking จองไว้ไปแล้ว แต่ยังไม่หักเครื่องของแผน
  // เครื่องของแผนที่กินพูลจริง = ACTUAL_MC − เครื่อง booking ของ item นั้นในสัปดาห์นั้น
  //   • แถวที่นั่งบนเครื่อง booking (📦) → หักออกได้พอดี = 0 (ไม่กินเครื่องเพิ่ม)
  //   • ลากงานออกจากสัปดาห์ → เครื่อง booking ไม่ได้ย้ายตาม จึงไม่ถูกคืนให้พูล
  //
  // เครื่อง booking อ่านจากคอลัมน์ MC_BOOKING ของแถวโดยตรง (ตรงกับสัปดาห์ปัจจุบันของแถว
  // เพราะ moveJob อัปเดตค่านี้ตอนลาก) — fallback ไป API bookingMc ถ้าไฟล์แผนเก่าไม่มีคอลัมน์
  // รวม ACTUAL_MC ต่อ (สัปดาห์ × item × เครื่อง × เกจ) ก่อน แล้วค่อยหัก booking ครั้งเดียว
  // มิฉะนั้นแถวหลายแถวของ item เดียวกันจะหักเครื่อง booking ซ้ำ
  // ผลลัพธ์ต่อ (สัปดาห์@@CAT|เกจ) = { normal, poly, cotton } เครื่องที่แผนจองเพิ่มจากพูล
  //   • normal → หักออกจากเลข "ว่าง" (เครื่องปกติ)
  //   • poly/cotton → หักออกจากเครื่องที่กันไว้ (ชิป 🔒) ไม่แตะเครื่องปกติ
  // แยกตาม prefix item เฉพาะกลุ่มที่มีเครื่องกันไว้จริง (ava.reserved) — ให้ตรงกับ
  // Planning.py ที่งาน POLY/COTTON กินเฉพาะ sub-pool ของตัวเอง งานปกติกินเครื่องปกติ
  const planMcByWeekCat = useMemo(() => {
    const m = {}
    if (!supported || ci.cat < 0 || ci.gauge < 0) return m
    const hasBkCol = ci.bookingmc >= 0
    const byItem = new Map()
    for (const { row } of rows) {
      const w = norm(row[ci.week]); if (w === '') continue
      const g = norm(row[ci.gauge])
      const item = norm(row[ci.item])
      const mcg = ci.mcgroup >= 0 ? norm(row[ci.mcgroup]) : ''
      const catKey = poolKeyOf(norm(row[ci.cat]), g, mcg)   // key ต่อพูล (แยก SKP/SKPTA·SKPLE)
      const bkKey = `${item.toUpperCase()}|${mcg.toUpperCase()}|${g}`
      const k = w + '@@' + catKey + '@@' + bkKey
      const mc = ci.actualmc >= 0 ? (Number(norm(row[ci.actualmc])) || 0) : 0
      // MC_BOOKING เท่ากันทุกแถวของ (item×เครื่อง×เกจ×สัปดาห์) เดียวกัน → เก็บค่าเดียว (max)
      const bkCol = hasBkCol ? (Number(norm(row[ci.bookingmc])) || 0) : null
      const cur = byItem.get(k)
      if (cur) { cur.mc += mc; if (bkCol != null) cur.bk = Math.max(cur.bk, bkCol) }
      else byItem.set(k, { w, catKey, bkKey, item, mc, bk: bkCol })
    }
    for (const { w, catKey, bkKey, item, mc, bk } of byItem.values()) {
      const bkVal = bk != null ? bk : (Number(bookingMc?.[w]?.[bkKey]) || 0)
      const net = Math.max(0, mc - bkVal)
      if (net === 0) continue
      const slot = m[w + '@@' + catKey] || (m[w + '@@' + catKey] = { normal: 0, poly: 0, cotton: 0 })
      // เข้าถัง POLY/COTTON เฉพาะเมื่อกลุ่มนี้มีเครื่องกันไว้จริง — ไม่งั้นงาน POLY
      // ในกลุ่มที่ไม่มี reservation จะกินเครื่องปกติ (ตรงกับ Planning.py)
      const rsv = ava?.[w]?.[catKey]?.reserved
      const t = itemPoolType(item)
      if (t === 'poly' && rsv && rsv.poly > 0) slot.poly += net
      else if (t === 'cotton' && rsv && rsv.cotton > 0) slot.cotton += net
      else slot.normal += net
    }
    return m
  }, [rows, ci, supported, bookingMc, ava, poolMap])

  // วัดความสูงหัวตาราง + แต่ละแถวโหลด → คำนวณ top ให้แถวโหลด sticky ค้างซ้อนใต้หัวตารางพอดี
  // (ความสูงหัวตาราง/ฟอนต์ไม่แน่นอน จึงวัดจริงแทนกำหนดตายตัว)
  const headRef = useRef(null)
  const loadRowRefs = useRef([])
  const clickTimer = useRef(null)
  const [loadTops, setLoadTops] = useState([])
  const [colHeadTop, setColHeadTop] = useState(0)   // ตำแหน่ง sticky ของแถวหัวคอลัมน์ (ใต้แถวสรุปโหลด)
  // แถวสรุปโหลดที่มองเห็น — กรอง OM ก็เหลือเฉพาะแถว OM (แถวงานด้านล่างก็กรองตามชุดเดียวกัน)
  const visLoadTypes = loadFilter ? LOAD_TYPES.filter(t => t.key === loadFilter) : LOAD_TYPES
  useLayoutEffect(() => {
    const headH = headRef.current ? headRef.current.offsetHeight : 0
    const tops = []
    let acc = headH
    for (let i = 0; i < visLoadTypes.length; i++) {
      tops[i] = acc
      acc += loadRowRefs.current[i] ? loadRowRefs.current[i].offsetHeight : 0
    }
    setLoadTops(prev =>
      (prev.length === tops.length && prev.every((v, i) => v === tops[i])) ? prev : tops)
    setColHeadTop(acc)   // หัวคอลัมน์ pin ต่อท้ายแถวสรุปโหลด
  }, [weeks, gantRows, groups, load, loadFilter])

  // แถวที่เลือกอยู่ (panel รายละเอียด) — ถ้าแถวถูกลบ/ตัวกรองซ่อนไป panel จะหายไปเอง
  const selRow = useMemo(
    () => (selIdx == null ? null : (rows.find(r => r.idx === selIdx)?.row ?? null)),
    [rows, selIdx])

  if (!supported) {
    return <div className="hint small">ชีทนี้ไม่มีคอลัมน์ <b>MC_GROUP</b> / <b>PLAN_WEEK</b> — Gantt แสดงได้เฉพาะชีท PLAN</div>
  }
  if (!weeks.length || !gantRows.length) {
    return <div className="hint small">ไม่มีข้อมูลสำหรับแสดง Gantt (ลองล้างตัวกรอง)</div>
  }

  function startEditQty(j, locked) {
    if (locked || !onEditQty || j.qty === '') return
    setEditIdx(j.idx)
    setEditVal(String(j.qty))
  }

  // คลิกเดี่ยว = เปิด panel / double click = แก้จำนวน
  // เบราว์เซอร์ยิง click ก่อน dblclick เสมอ → หน่วง 200ms แล้วยกเลิกถ้ามี dblclick ตามมา
  function onBarClick(j) {
    clearTimeout(clickTimer.current)
    clickTimer.current = setTimeout(() => setSelIdx(cur => (cur === j.idx ? null : j.idx)), 200)
  }
  function onBarDblClick(j, locked) {
    clearTimeout(clickTimer.current)
    startEditQty(j, locked)
  }
  function commitQty() {
    if (editIdx == null) return
    const v = parseFloat(editVal)
    if (Number.isFinite(v) && v > 0) onEditQty(editIdx, Math.round(v * 100) / 100)
    setEditIdx(null)
  }

  function onDrop(e, week, targetRow) {
    e.preventDefault()
    setOverWeek(null)
    const idx = Number(e.dataTransfer.getData('text/plain'))
    setDragIdx(null)
    if (Number.isNaN(idx)) return
    // MC_GROUP ปลายทาง = เครื่องของแถวที่ปล่อยลง (ใช้เช็คว่าย้ายข้ามเครื่องไหม)
    const targetMc = mcGroupI >= 0 ? targetRow.vals[mcGroupI] : null
    onMoveWeek(idx, week, targetMc)
  }

  // คลิกหัวแถวซ้ายได้เมื่อรู้ตำแหน่ง CAT + เกจ
  const catClickable = avaCatI >= 0 && avaGaugeI >= 0
  // แถวหนึ่งเข้าเงื่อนไขตัวกรองหรือไม่ (mcgroup=null → เทียบแค่ CAT+เกจ)
  const catMatch = (vals, f) => !!f
    && nkey(vals[avaCatI]) === f.cat && nkey(vals[avaGaugeI]) === f.gauge
    && (f.mcgroup == null || (mcGroupI >= 0 && nkey(vals[mcGroupI]) === f.mcgroup))
  // คลิกช่องหัวแถว group ที่ตำแหน่ง n → ตั้ง/สลับตัวกรอง (คลิกซ้ำช่องเดิม = ล้าง)
  const clickGroupCell = (vals, n) => {
    if (!catClickable) return
    const next = {
      cat: nkey(vals[avaCatI]),
      gauge: nkey(vals[avaGaugeI]),
      mcgroup: (n === mcGroupI && mcGroupI >= 0) ? nkey(vals[mcGroupI]) : null,
    }
    setCatFilter(cur =>
      cur && cur.cat === next.cat && cur.gauge === next.gauge && cur.mcgroup === next.mcgroup
        ? null : next)
  }
  // แถวที่แสดงจริง = ผ่านตัวกรองประเภทโหลด (loadFilter) + ตัวกรอง CAT/เกจ/เครื่อง (catFilter)
  const visRows = (loadFilter ? gantRows.filter(r => rowType.get(r.key) === loadFilter) : gantRows)
    .filter(r => !catFilter || catMatch(r.vals, catFilter))

  return (
    <div className="gantt">
      <div className="gantt-scroll">
        <table className="gantt-grid">
          <thead>
            <tr ref={headRef}>
              <th className="gantt-glabel gantt-ghead gantt-glast gantt-weekcorner"
                colSpan={groups.length + (showRsv ? 1 : 0)}
                style={{ left: 0, width: groupsW + (showRsv ? RSV_COL.width : 0), minWidth: groupsW + (showRsv ? RSV_COL.width : 0) }}>
                {catFilter ? (
                  <span className="gantt-catfilter">
                    <span className="gcf-txt" title="ตัวกรองที่ใช้อยู่ (คลิกช่องหัวแถวเพื่อเปลี่ยน)">
                      🔎 {catFilter.cat} / {catFilter.gauge}{catFilter.mcgroup ? ` / ${catFilter.mcgroup}` : ''}
                    </span>
                    <button className="gcf-clear" onClick={() => setCatFilter(null)} title="ล้างตัวกรอง">✕</button>
                  </span>
                ) : 'Factory/Week'}
              </th>
              {weeks.map(w => <th key={w} className={'gantt-wk' + (isLocked(w) ? ' locked' : '')}>{isLocked(w) && '🔒'}W{w}</th>)}
            </tr>
          </thead>
          <tbody>
            {visLoadTypes.map((t, ti) => (
              <tr key={t.key} className="gantt-load-row" ref={el => { loadRowRefs.current[ti] = el }}>
                <th className="gantt-glabel gantt-glast"
                  style={{ left: 0, top: loadTops[ti], zIndex: 5 }}
                  colSpan={groups.length + (showRsv ? 1 : 0)}>
                  {t.long || t.label}
                  {capByType[t.key] != null &&
                    <span className="loadjobs-total"> · Set up สูงสุด {capByType[t.key]} Job/Week</span>}
                </th>
                {weeks.map(w => {
                  const info = (load[w] && load[w][t.key]) || {}
                  // ไม่มี cap รายสัปดาห์ → ใช้ความจุรวมของประเภท (capByType คงที่) เป็นตัวสำรอง
                  // → สัปดาห์ที่ไม่มีงานก็รู้ว่า "ว่าง = เต็มความจุ" แสดงเลขในแถบเทาได้
                  const cap = (info.cap != null && info.cap !== '') ? info.cap : capByType[t.key]
                  const old = info.old || 0
                  // ใหม่(live) = job จาก booking (คงที่) + ผลรวม NEW_MC ของแถวที่วางจริง (ขยับตามการลาก)
                  const nw = Math.max(0, (info.bookingNew || 0) + (planNewByWeekType[w + '|' + t.key] || 0))
                  const total = old + nw
                  const hasCap = cap != null && cap !== ''
                  const over = hasCap && total > cap
                  const empty = old === 0 && nw === 0 && !hasCap
                  const free = hasCap ? cap - total : null
                  const tip = `${t.label} • สัปดาห์ ${w}\nแผนเดิม ${old} + ใหม่ ${nw} = ใช้ ${total}`
                    + (hasCap ? ` / ทั้งหมด ${cap}\n${over ? `⚠ เกินเครื่องที่มี ${total - cap}` : `ว่าง ${free}`}` : '')
                  return (
                    <td key={w} className="gantt-load-cell" style={{ top: loadTops[ti] }}>
                      {empty ? (
                        <div className="loadwrap" title={`${t.label} • สัปดาห์ ${w}\nไม่มีงาน`}>
                          <div className="loadbar nocap">
                            <span className="seg free" style={{ flexGrow: 1 }} />
                          </div>
                        </div>
                      ) : (
                        <div className="loadwrap" title={tip}>
                          {/* แถบสัดส่วน: เดิม(เขียวอ่อน) + วันนี้(เขียวเข้ม) + ว่าง(เทา) — ตัวเลขอยู่ในแถบ */}
                          <div className={'loadbar' + (over ? ' over' : '') + (hasCap ? '' : ' nocap')}>
                            {old > 0 && <span className="seg old" style={{ flexGrow: old }}>{old}</span>}
                            {nw > 0 && <span className="seg new" style={{ flexGrow: nw }}>{nw}</span>}
                            {hasCap && free > 0 && <span className="seg free" style={{ flexGrow: free }}>{free}</span>}
                          </div>
                          {over && <span className="loadlbl over">เกิน {total - cap}</span>}
                        </div>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
            {/* หัวคอลัมน์ (ย้ายลงมาจากบนสุด) — pin ต่อท้ายแถวสรุปโหลด อยู่เหนือแถวงาน */}
            <tr className="gantt-colhead">
              {groups.map((g, n) => (
                <th key={g.col}
                  className={'gantt-glabel gantt-ghead' + (!showRsv && n === groups.length - 1 ? ' gantt-glast' : '')}
                  style={{ left: g.left, top: colHeadTop, width: g.width, minWidth: g.width }}>
                  {g.label}
                </th>
              ))}
              {showRsv && (
                <th className="gantt-glabel gantt-ghead gantt-glast gantt-rsvcol"
                  style={{ left: groupsW, top: colHeadTop, width: RSV_COL.width, minWidth: RSV_COL.width }}
                  title="เครื่องที่กันไว้ให้งาน POLY / COTTON ของกลุ่ม CAT|เกจ นี้ (ใช้แทนงานปกติไม่ได้)">
                  🔒{RSV_COL.label}
                </th>
              )}
              {weeks.map(w => <td key={w} className="gantt-colhead-cell" style={{ top: colHeadTop }} />)}
            </tr>
            {visRows.map(r => (
              <tr key={r.key}>
                {groups.map((g, n) => {
                  // ช่องที่ตรงกับตัวกรองปัจจุบัน → ไฮไลต์ให้เห็นว่ากรองด้วยค่าไหน
                  const on = catFilter && catMatch(r.vals, catFilter)
                    && (n === avaCatI || n === avaGaugeI
                      || (catFilter.mcgroup != null && n === mcGroupI))
                  return (
                    <th key={g.col}
                      className={'gantt-glabel'
                        + (!showRsv && n === groups.length - 1 ? ' gantt-glast' : '')
                        + (catClickable ? ' gantt-gclick' : '')
                        + (on ? ' gantt-gfilter-on' : '')}
                      onClick={catClickable ? () => clickGroupCell(r.vals, n) : undefined}
                      title={catClickable ? 'คลิกเพื่อกรอง CAT/เกจ (Machine = กรองถึงเครื่อง) — คลิกซ้ำเพื่อล้าง' : undefined}
                      style={{ left: g.left, width: g.width, minWidth: g.width }}>
                      {r.vals[n]}
                    </th>
                  )
                })}
                {showRsv && (() => {
                  const rv = rsvOfRow(r.vals)
                  return (
                    <th className="gantt-glabel gantt-glast gantt-rsvcol"
                      style={{ left: groupsW, width: RSV_COL.width, minWidth: RSV_COL.width }}
                      title={rv
                        ? `เครื่องกันไว้ของ ${r.vals[avaCatI]} เกจ ${r.vals[avaGaugeI]} (ใช้แทนงานปกติไม่ได้)\n`
                          + `${rv.poly ? `POLY ${rv.poly} เครื่อง\n` : ''}${rv.cotton ? `COTTON ${rv.cotton} เครื่อง\n` : ''}`
                          + 'คงเหลือรายสัปดาห์ดูที่ชิป 🔒 ในแต่ละช่อง'
                        : 'ไม่มีเครื่องกันไว้ — งาน POLY/COTTON กลุ่มนี้กินเครื่องปกติ'}>
                      {rv ? (
                        <>
                          {rv.poly ? <span className="rsvtag poly">Poly = {rv.poly}</span> : null}
                          {rv.cotton ? <span className="rsvtag cotton">Cotton = {rv.cotton}</span> : null}
                        </>
                      ) : <span className="rsvtag dim">–</span>}
                    </th>
                  )
                })()}
                {weeks.map(w => {
                  const jobs = cells.get(r.key + '||' + w) || []
                  const isOver = overWeek === w
                  const avaKey = avaCatI >= 0 && avaGaugeI >= 0
                    ? poolKeyOf(r.vals[avaCatI], r.vals[avaGaugeI], mcGroupI >= 0 ? r.vals[mcGroupI] : '')
                    : null
                  const av = avaKey && ava[w] ? ava[w][avaKey] : null
                  // เครื่องที่แผนจองเพิ่ม แยกเป็น ปกติ / POLY / COTTON
                  const planSlot = planMcByWeekCat[w + '@@' + avaKey] || null
                  const planNormal = planSlot ? planSlot.normal : 0
                  const planPoly = planSlot ? planSlot.poly : 0
                  const planCotton = planSlot ? planSlot.cotton : 0
                  // เครื่องว่าง live = เครื่องว่างหลัง booking − เครื่องที่แผนจองเพิ่ม (เฉพาะงานปกติ)
                  //   งาน POLY/COTTON ไม่หักตรงนี้ เพราะกินเครื่องที่กันไว้ (ชิป 🔒) ต่างหาก
                  const remainLive = av ? av.remain - planNormal : null
                  const locked = isLocked(w)
                  return (
                    <td key={w}
                      className={'gantt-cell' + (isOver ? ' over' : '') + (locked ? ' locked' : '')}
                      onDragOver={locked ? undefined : e => { e.preventDefault(); if (overWeek !== w) setOverWeek(w) }}
                      onDragLeave={locked ? undefined : () => setOverWeek(o => (o === w ? null : o))}
                      onDrop={locked ? undefined : e => onDrop(e, w, r)}>
                      {av && (() => {
                        const rsv = av.reserved || null
                        const rsvP = rsv ? (rsv.poly || 0) : 0
                        const rsvC = rsv ? (rsv.cotton || 0) : 0
                        const hasRsv = rsvP > 0 || rsvC > 0
                        // เครื่องกันไว้คงเหลือ = กันไว้ − booking − ที่แผนใช้ในสัปดาห์นั้น (ติดลบ = วางเกิน)
                        // booking ที่กินเครื่องกันไว้ไปแล้ว (จาก MC_RESERVED_WEEKLY) ต้องหักด้วย
                        // ไม่งั้นชิปจะโชว์เหมือนยังว่าง ทั้งที่ booking ใช้เต็มแล้ว
                        const rsvPused = rsv ? (rsv.poly_used || 0) : 0
                        const rsvCused = rsv ? (rsv.cotton_used || 0) : 0
                        const polyLeft = rsvP - rsvPused - planPoly
                        const cottonLeft = rsvC - rsvCused - planCotton
                        const rsvOver = polyLeft < 0 || cottonLeft < 0
                        const rsvTxt = [
                          rsvP ? `POLY ${rsvP} (booking ${rsvPused} + แผน ${planPoly} → เหลือ ${polyLeft})` : '',
                          rsvC ? `COTTON ${rsvC} (booking ${rsvCused} + แผน ${planCotton} → เหลือ ${cottonLeft})` : '',
                        ].filter(Boolean).join('\n')
                        // ติดลบ = วางเกินเครื่องที่มี → ไม่โชว์ "ว่าง -5" (ตีความยาก) แต่บอกตรงๆ ว่าเกินเท่าไร
                        const avaTxt = remainLive > 0 ? `ว่าง ${remainLive}`
                          : remainLive === 0 ? 'เต็ม'
                            : `ไม่ว่าง (เกิน ${-remainLive})`
                        return (
                          <span className={'cellava' + (remainLive <= 0 ? ' none' : '') + (remainLive < 0 ? ' over' : '')}
                            title={`${avaTxt} • เครื่องปกติทั้งหมด ${av.total}\nว่างหลัง booking ${av.remain} − แผนจองเพิ่ม(ปกติ) ${planNormal} = ${remainLive}`
                              + (hasRsv ? `\nเครื่องกันไว้ (ใช้แทนงานปกติไม่ได้):\n${rsvTxt}` : '')}>
                            {avaTxt}
                            {hasRsv && <span className={'cellava-rsv' + (rsvOver ? ' over' : '')}>🔒{rsvP ? ` P${polyLeft < 0 ? ` ไม่ว่าง (เกิน ${-polyLeft})` : polyLeft}` : ''}{rsvC ? ` C${cottonLeft < 0 ? ` ไม่ว่าง (เกิน ${-cottonLeft})` : cottonLeft}` : ''}</span>}
                          </span>
                        )
                      })()}
                      {jobs.map(j => {
                        const isColor = colorRows && colorRows.has(j.idx)
                        const editing = editIdx === j.idx
                        const sc = valOf(j.row)('SC_SO_NO')
                        return (
                          <div key={j.idx}
                            className={'gbar' + (dragIdx === j.idx ? ' dragging' : '') + (isColor ? ' gbar-color' : '')
                              + (locked ? ' locked' : '') + (selIdx === j.idx ? ' selected' : '')
                              + (j.foldWarn ? ' gbar-foldwarn' : '')}
                            draggable={!locked && !editing}
                            onDragStart={locked ? undefined : e => { e.dataTransfer.setData('text/plain', String(j.idx)); e.dataTransfer.effectAllowed = 'move'; setDragIdx(j.idx) }}
                            onDragEnd={locked ? undefined : () => { setDragIdx(null); setOverWeek(null) }}
                            onClick={() => onBarClick(j)}
                            onDoubleClick={() => onBarDblClick(j, locked)}
                            style={j.foldWarn ? undefined : (isColor ? undefined : { background: colorOf(j.ck), color: textOf(j.ck) })}
                            title={`${j.item}${sc ? ` • SC ${sc}` : ''}\n${r.vals.join(' • ')} • สัปดาห์ ${w}${j.qty !== '' ? `\nจำนวน ${j.qty}` : ''}${j.actualmc !== '' ? ` • ใช้ ${j.actualmc} เครื่อง` : ''}${Number(j.setup) > 0 ? ` • setup ${j.setup} วัน` : ''}${j.mc ? `\n${MC_KINDS[j.mc].icon} ${MC_KINDS[j.mc].label}` : ''}${j.remark ? `\n${j.remark}` : ''}\nสี: ${j.ck}${isColor ? '\n★ งานสี (ต้องย้อม)' : ''}${j.lateRdd ? '\n⚠ วางเลยสัปดาห์ RDD' : ''}${j.foldWarn ? `\n⚠ order เปิดมา ${j.foldQty} พับ — หาร 6 ไม่ลงตัว (เหลือเศษ ${j.foldRem} พับ)` : ''}\n👆 คลิกเพื่อดูรายละเอียดครบ${onEditQty && !locked && j.qty !== '' ? ' • double click เพื่อแก้จำนวน' : ''}${locked ? '\n🔒 สัปดาห์ freeze — แก้ไม่ได้' : ''}`}>
                            {locked && <span className="gbar-star">🔒</span>}
                            {isColor && !locked && <span className="gbar-star">★</span>}
                            {j.mc && <span className="gbar-mc">{MC_KINDS[j.mc].icon}</span>}
                            {j.foldWarn && (
                              <span className="gbar-fold"
                                title={`order เปิดมา ${j.foldQty} พับ — หาร 6 ไม่ลงตัว (เหลือเศษ ${j.foldRem} พับ)`}>⚠</span>
                            )}
                            <span className={'gbar-item' + (j.core ? ' core' : '')}>{j.item}</span>
                            {j.tags.map(t => (
                              <span key={t.key} className={'gbar-tag' + (t.warn ? ' warn' : '')}>{t.text}</span>
                            ))}
                            {editing ? (
                              <input className="gbar-qty-edit" type="number" step="any" autoFocus
                                value={editVal}
                                onChange={e => setEditVal(e.target.value)}
                                onMouseDown={e => e.stopPropagation()}
                                onClick={e => e.stopPropagation()}
                                onDoubleClick={e => e.stopPropagation()}
                                onKeyDown={e => { if (e.key === 'Enter') commitQty(); else if (e.key === 'Escape') setEditIdx(null) }}
                                onBlur={commitQty} />
                            ) : (
                              j.qty !== '' && <span className="gbar-qty" title={`จำนวน ${j.qty} กก.`}>{j.qty}<small className="gbar-qty-unit">kg</small></span>
                            )}
                          </div>
                        )
                      })}
                      {/* overlay History แผนเดิม (สูงสุด 2 week) — บล็อกอ่านอย่างเดียว ลาก/แก้/หักเครื่องว่างไม่ได้ */}
                      {(bookingData.cells.get(r.key + '||' + w) || []).map((b, bi) => (
                        <div key={'bk' + bi} className="gbar gbar-booking"
                          title={`📋 History แผนเดิม (สูงสุด 2 week)\n${b.item}${b.so ? ` • SO ${b.so}` : ''}\n${r.vals.join(' • ')} • สัปดาห์ ${w}`
                            + `${b.qty > 0 ? `\nจำนวน ${b.qty} กก.` : ''}${b.mc > 0 ? ` • ใช้ ${b.mc} เครื่อง` : ''}`
                            + `${b.material ? `\n${b.material}` : ''}\n(ดูอย่างเดียว — ลาก/แก้ไม่ได้)`}>
                          <span className="gbar-mc">📋</span>
                          <span className="gbar-item">{b.item}</span>
                          {b.mc > 0 && <span className="gbar-tag">{b.mc} เครื่อง</span>}
                          {b.qty > 0 && <span className="gbar-qty">{b.qty}<small className="gbar-qty-unit">kg</small></span>}
                        </div>
                      ))}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="gantt-foot">
        <button className="gfoot-toggle" onClick={() => setShowFoot(s => !s)}
          title="คำอธิบายการใช้งาน + ความหมายของสี/เครื่อง">
          {showFoot ? '▾' : '▸'} คำอธิบาย / สัญลักษณ์
        </button>
        {showFoot && (
          <div className="gantt-foot-body">
            <span className="hint small" style={{ padding: 0 }}>
              ลากบล็อกไปคอลัมน์สัปดาห์อื่นเพื่อเปลี่ยน <b>PLAN_WEEK</b> — <b>คลิก</b>บล็อกเพื่อดูรายละเอียดครบทุกช่อง, <b>double click</b> เพื่อแก้จำนวน
            </span>
            {ci.sharedmc >= 0 && (
              <div className="gantt-legend">
                <span className="glegend-title">เครื่อง:</span>
                {Object.entries(MC_KINDS).map(([k, v]) => (
                  <span key={k} className="glegend" title={v.label}>
                    <b className="gbar-mc">{v.icon}</b>{v.label}
                  </span>
                ))}
              </div>
            )}
            <div className="gantt-legend">
              <span className="glegend-title">สัญลักษณ์บนบล็อก:</span>
              {BAR_MARKS.map(m => (
                <span key={m.key} className="glegend" title={m.tip}>
                  <span className={'glegend-chip ' + m.cls}>{m.sample}</span>{m.label}
                </span>
              ))}
            </div>
            <div className="gantt-legend">
              <span className="glegend-title">เครื่องว่าง (มุมขวาบนของช่อง):</span>
              {AVA_MARKS.map(m => (
                <span key={m.key} className="glegend" title={m.tip}>
                  <span className={'cellava glegend-ava' + (m.cls ? ' ' + m.cls : '')}>{m.sample}</span>{m.label}
                </span>
              ))}
            </div>
            {colorCols.length > 0 && (
              <div className="gantt-legend">
                <span className="glegend-title">สีตาม CAT / Guage:</span>
                {colorKeys.map(k => (
                  <span key={k} className="glegend">
                    <i style={{ background: colorOf(k) }} />{k}
                  </span>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {selRow && (
        <JobPanel row={selRow} columns={columns} colIdx={colIdx}
          idx={selIdx} weeks={weeks} isLocked={isLocked} onSplit={onSplit} onRemove={onRemove}
          showOther={showOther} onToggleOther={() => setShowOther(s => !s)}
          onClose={() => setSelIdx(null)} />
      )}
    </div>
  )
}

/**
 * Panel รายละเอียดงาน 1 แถว — โชว์ทุกคอลัมน์ของชีท PLAN (จัดกลุ่มตาม PANEL_GROUPS
 * + ที่เหลือรวมใน "คอลัมน์อื่นๆ") เพื่อให้ตัดสินใจได้บนหน้าเว็บโดยไม่ต้องเปิด Excel
 */
function JobPanel({ row, columns, colIdx, idx, weeks = [], isLocked = () => false, onSplit, onRemove, showOther, onToggleOther, onClose }) {
  const v = (name) => {
    const i = colIdx[name]
    return i == null ? '' : norm(row[i])
  }
  const week = v('PLAN_WEEK')
  const rddW = rddWeekNo(v)
  const lateRdd = isLateRdd(v, week)
  const isOutsource = nkey(v('OUTSOURCE')) === 'YES'
  const kind = mcKind(Number(v('NEW_MC')) || 0, Number(v('CARRYOVER_MC')) || 0,
    Number(v('MC_SHARED')) || 0, Number(v('MC_BOOKING')) || 0, isOutsource)

  // ── แบ่งงานเป็น 2 สัปดาห์ (ปุ่มอยู่ในการ์ด กันเผลอกดโดนบนบล็อก) ──
  const curQty = Number(v('PRODUCE_QTY')) || 0
  const canSplit = !!onSplit && idx != null && curQty > 0 && !isLocked(week)
  const [splitOpen, setSplitOpen] = useState(false)
  const [splitQty, setSplitQty] = useState('')
  const [splitWeek, setSplitWeek] = useState('')
  const openSplit = () => {
    const i = weeks.indexOf(String(week))
    let tgt = ''
    for (let k = i + 1; k < weeks.length; k++) { if (!isLocked(weeks[k])) { tgt = weeks[k]; break } }
    if (!tgt) tgt = weeks.find(wk => wk !== String(week) && !isLocked(wk)) || ''
    setSplitQty(String(Math.round((curQty / 2) * 100) / 100))
    setSplitWeek(tgt)
    setSplitOpen(true)
  }
  const doSplit = () => {
    const q = parseFloat(splitQty)
    if (Number.isFinite(q) && q > 0 && splitWeek && splitWeek !== String(week))
      onSplit(idx, Math.round(q * 100) / 100, splitWeek)
    setSplitOpen(false)
  }
  // ลบงานก้อนนี้ออกจากแผน (วางผิด/ไม่ต้องการ) — ยืนยันก่อน แล้วปิดการ์ด
  const canDelete = !!onRemove && idx != null && !isLocked(week)
  const doRemove = () => {
    if (!window.confirm(`ลบงาน ${v('ITEM_CODE')} (สัปดาห์ W${week}) ออกจากแผน?\n\nลบก้อนนี้ก้อนเดียว — กด 💾 บันทึกเพื่อให้มีผลถาวร`)) return
    onRemove(idx)
    onClose()
  }
  const others = columns.filter(c => !PANEL_KNOWN.has(c) && norm(row[colIdx[c]]) !== '')
  // RDD/FG week เก็บเป็น YYYYWW → โชว์เลขสัปดาห์นำหน้าให้อ่านง่าย (คงค่าดิบไว้ในวงเล็บ)
  const cellText = (c) => {
    const raw = norm(row[colIdx[c]])
    if ((c === 'RDD_WEEK' || c === 'FG_WEEK') && /^\d{5,}$/.test(raw)) return `W${Number(raw.slice(-2))} (${raw})`
    return raw
  }

  return (
    <aside className="jobpanel">
      <div className="jobpanel-head">
        <div>
          <b className="jobpanel-item">{v('ITEM_CODE')}</b>
          <div className="jobpanel-sub">
            สัปดาห์ผลิต <b>W{week}</b> • {v('MC_GROUP')} เกจ {v('MC_GUAGE')} • {v('CAT')}
          </div>
        </div>
        <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
      </div>

      <div className="jobpanel-flags">
        {kind && <span className="jobflag">{MC_KINDS[kind].icon} {MC_KINDS[kind].label}</span>}
        {Number(v('SETUP_DAYS')) > 0 && <span className="jobflag setup">🔧 setup {v('SETUP_DAYS')} วัน</span>}
        {lateRdd && <span className="jobflag warn">⚠ วางเลย RDD (W{rddW})</span>}
        {String(v('FOLD_WARN')) === '1' && (
          <span className="jobflag fold" title="ยอดรวมทั้ง SC หารด้วย 6 พับไม่ลงตัว — ต้องแก้ที่ยอดเปิด order">
            ⚠ order {v('FOLD_QTY')} พับ — หาร 6 ไม่ลงตัว (เหลือ {v('FOLD_REMAINDER')})
          </span>
        )}
        {v('IS_CORE_ITEM') && <span className="jobflag core">★ {v('IS_CORE_ITEM')}</span>}
      </div>

      {(canSplit || canDelete) && (
        <div className="jobsplit">
          {splitOpen ? (
            <div className="jobsplit-form">
              <div className="gsplit-row">
                <span>แบ่ง</span>
                <input type="number" step="any" autoFocus value={splitQty}
                  onChange={e => setSplitQty(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') doSplit(); else if (e.key === 'Escape') setSplitOpen(false) }} />
                <span>กก. ไป</span>
                <select value={splitWeek} onChange={e => setSplitWeek(e.target.value)}>
                  {weeks.filter(wk => wk !== String(week) && !isLocked(wk)).map(wk =>
                    <option key={wk} value={wk}>W{wk}</option>)}
                </select>
              </div>
              <div className="gsplit-hint">เลขเครื่อง/setup จะ copy มาตามเดิม — รันแผนใหม่เพื่อคำนวณเครื่องให้ตรง</div>
              <div className="gsplit-actions">
                <button className="gsplit-ok" onClick={doSplit}>แบ่ง</button>
                <button className="gsplit-cancel" onClick={() => setSplitOpen(false)}>ยกเลิก</button>
              </div>
            </div>
          ) : (
            <div className="jobsplit-btns">
              {canSplit && <button className="jobsplit-btn" onClick={openSplit}>✂ แบ่งงานเป็น 2 สัปดาห์</button>}
              {canDelete && <button className="jobdel-btn" onClick={doRemove}>🗑 ลบงานนี้ออกจากแผน</button>}
            </div>
          )}
        </div>
      )}

      <div className="jobpanel-body">
        {PANEL_GROUPS.map(g => {
          const fields = g.fields.filter(([c]) => colIdx[c] != null && norm(row[colIdx[c]]) !== '')
          if (!fields.length) return null
          return (
            <section key={g.title} className="jobsec">
              <h4>{g.title}</h4>
              <dl className={g.wide ? 'jobdl wide' : 'jobdl'}>
                {fields.map(([c, label]) => (
                  <React.Fragment key={c}>
                    <dt>{label}</dt>
                    <dd className={(c === 'RDD_WEEK' || c === 'TARGET_KNIT') && lateRdd ? 'warn' : undefined}>{cellText(c)}</dd>
                  </React.Fragment>
                ))}
              </dl>
            </section>
          )
        })}

        {others.length > 0 && (
          <section className="jobsec">
            <button className="jobmore" onClick={onToggleOther}>
              {showOther ? '▾' : '▸'} คอลัมน์อื่นๆ ({others.length})
            </button>
            {showOther && (
              <dl className="jobdl wide">
                {others.map(c => (
                  <React.Fragment key={c}>
                    <dt>{c}</dt>
                    <dd>{norm(row[colIdx[c]])}</dd>
                  </React.Fragment>
                ))}
              </dl>
            )}
          </section>
        )}
      </div>
    </aside>
  )
}

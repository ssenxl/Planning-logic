import React, { useMemo, useState, useRef, useLayoutEffect, useEffect } from 'react'
import { norm } from './ColumnFilter.jsx'

// จานสีแยกประเภท (Tableau-20) — รองรับหลายกลุ่ม CAT/เกจ
const PALETTE = ['#4e79a7', '#f28e2b', '#59a14f', '#e15759', '#76b7b2',
  '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac',
  '#a0cbe8', '#ffbe7d', '#8cd17d', '#ff9d9a', '#86bcb6',
  '#f1ce63', '#d4a6c8', '#fabfd2', '#d7b5a6', '#79706e']

// ไล่เฉดสีตามประเภท CAT (เข้มสุด → อ่อนสุด): DOUBLE = Teal, SINGLE = สีฟ้า (Light Blue)
// สองกลุ่มต้องอยู่คนละโทน ไม่งั้นตัวเข้มสุดของทั้งคู่แยกกันไม่ออกใน legend
const CAT_GRAD = {
  DOUBLE: ['#004d40', '#00695c', '#00796b', '#00897b', '#009688',
    '#26a69a', '#4db6ac', '#80cbc4', '#b2dfdb', '#e0f2f1'],
  SINGLE: ['#01579b', '#0277bd', '#0288d1', '#039be5', '#03a9f4',
    '#29b6f6', '#4fc3f7', '#81d4fa', '#b3e5fc', '#e1f5fe'],
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
// สีต่อ item (โหมดสีตาม ITEM_CODE) — กระจาย hue ด้วยมุมทอง 137.508° ตามลำดับ item ที่ sort แล้ว
// → item ที่อยู่ติดกันสีต่างกันชัด และวนซ้ำยากกว่าจานสี 20 สี (สลับความสด/ความสว่างเพิ่มความต่าง)
function _hslHex(h, s, l) {
  const S = s / 100, L = l / 100
  const c = (1 - Math.abs(2 * L - 1)) * S
  const hp = h / 60
  const x = c * (1 - Math.abs((hp % 2) - 1))
  const seg = [[c, x, 0], [x, c, 0], [0, c, x], [0, x, c], [x, 0, c], [c, 0, x]][Math.floor(hp) % 6]
  const m = L - c / 2
  return _rgbHex(seg.map(v => (v + m) * 255))
}
const itemColor = (i) => _hslHex((i * 137.508) % 360, 58 + (i % 3) * 9, 42 + (i % 2) * 10)

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

// หัวแถวเพิ่ม "Item" — ใช้เฉพาะชีท SETUP_TRACKING (ดูประวัติ setup ของ item เดียวไล่ซ้าย→ขวา)
// ไม่ใส่ในชีท PLAN เพราะ 1 แถว = เครื่อง+item จะทำให้แถวเยอะและลากงานข้ามเครื่องยากขึ้น
const ITEM_GROUP = { col: 'ITEM_CODE', label: 'Item', width: 148 }
const ITEM_ROW_SHEETS = new Set(['SETUP_TRACKING'])

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
    // sample ว่าง = ไม่ต้องมีชิปตัวอย่าง — ใช้ pre แทน: ข้อความส่วนที่บอกสี ระบายเป็นสีนั้นจริง
    key: 'core', cls: 'lg-core', sample: '', pre: 'Text สีแดง', label: ' = Item Core Greige',
    tip: 'ชื่อ item เป็นสีแดงหนา = Item Core Greige (IS_CORE_ITEM) — ผลิตประจำ ควรรักษาเครื่องไว้',
  },
  {
    key: 'program', cls: 'lg-program', sample: '', pre: 'Text สีน้ำเงิน', label: ' = Item Program',
    tip: 'ชื่อ item เป็นสีน้ำเงินหนา = Item Program — ITEM_CODE และ TEAM ของแถวนี้ ตรงกับที่กำหนดไว้ในชีท Program (MasterMC)\n'
      + 'แก้รายการได้ที่หน้า Master Data → MasterMC → Program',
  },
  {
    key: 'color', cls: 'lg-color', sample: '★', label: 'งานสี (ต้องย้อม)',
    tip: 'บล็อกสีส้ม + ★ = งานที่ต้องผ่านย้อมสี (มาจากหน้า Order Color)',
  },
  {
    key: 'fold', cls: 'lg-fold', sample: '⚠', label: 'จำนวนพับหารไม่ลงคู่',
    tip: 'บล็อกแดงทั้งก้อน = ยอดที่ลูกค้าเปิดมา (ทั้ง SC) จำนวนพับหารไม่ลงคู่ (เฉพาะ IRMT/SJT)\n'
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
    tip: 'บล็อกลายเส้นประ = แผนเดิมย้อนหลังสูงสุด 5 สัปดาห์ ไว้เทียบว่าเครื่องเดิมถักอะไรอยู่\n'
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

// จำนวนสัปดาห์อดีตที่ Gantt ยอมโชว์ (อ่านอย่างเดียว) — ชีท SETUP_TRACKING/booking มี week ย้อนถึง 24
// นับจากสัปดาห์ปัจจุบัน: 5 → ปัจจุบัน W32 เห็นตั้งแต่ W27 (สัปดาห์อดีตที่ไม่มีข้อมูลเลย เช่น W31 หยุด จะไม่โผล่)
const HISTORY_WEEKS_BACK = 5

// สัปดาห์ "ปีหน้า" (แผนวางข้ามปี 52/53 → 1,2,3) แยกจาก "สัปดาห์อดีตของปีนี้" ด้วยระยะที่ใกล้กว่า:
// ไปข้างหน้า (n+52−cur) < ย้อนหลัง (cur−n) → ปีหน้า ; ไม่งั้นคืออดีต
// (เดิมใช้เกณฑ์ "เลข < ขอบซ้ายแกน" ล้วน ทำให้ W24–W30 ของ SETUP_TRACKING ถูกมองเป็นปีหน้า
//  → ไปโผล่ท้ายแกนขวา และไม่ถูกล็อก)
function isNextYearWeek(n, cur = currentPlanWeek()) {
  if (!Number.isFinite(n) || n === 99 || n >= cur) return false
  return (n + 52 - cur) < (cur - n)
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
// cols     : คอลัมน์ที่ฟิลด์นี้ต้องใช้ (มีอย่างน้อย 1 คอลัมน์ในชีท = ใช้ได้) — ชีทที่ไม่มีเลย
//            จะไม่โชว์ chip นี้ (เดิมโชว์ทุก chip ทุกชีท → ติ๊กในชีท SETUP_TRACKING แล้วไม่มีอะไรขึ้น)
export const BAR_FIELDS = [
  { key: 'sc', label: 'SC', cols: ['SC_SO_NO'], build: v => v('SC_SO_NO') },
  // ชีท SETUP_TRACKING ไม่มี ACTUAL_MC → ใช้ MC_THIS_WEEK (เครื่องที่ item นั้นใช้ในสัปดาห์นั้น)
  {
    key: 'mc', label: 'เครื่องที่ใช้', cols: ['ACTUAL_MC', 'MC_THIS_WEEK'],
    build: v => { const m = v('ACTUAL_MC') || v('MC_THIS_WEEK'); return Number(m) > 0 ? `${m} เครื่อง` : '' },
  },
  { key: 'po', label: 'PO', cols: ['PO_NO'], build: v => v('PO_NO') },
  { key: 'rdd', label: 'RDD', cols: ['RDD_WEEK', 'TARGET_KNIT'], build: v => { const w = rddWeekNo(v); return w ? `RDD W${w}` : '' } },
  { key: 'setup', label: 'setup (วัน)', cols: ['SETUP_DAYS'], build: v => { const d = v('SETUP_DAYS'); return Number(d) > 0 ? `setup ${d} ว.` : '' } },
  { key: 'customer', label: 'ลูกค้า', cols: ['CUSTOMER'], build: v => v('CUSTOMER') },
  { key: 'left', label: 'คงเหลือ', cols: ['PLAN_QTY'], build: v => { const q = v('PLAN_QTY'); return q !== '' && Number(q) > 0 ? `เหลือ ${q}` : '' } },
  { key: 'color', label: 'สี', cols: ['COLOR_DESC', 'NAY_COLOR'], build: v => v('COLOR_DESC') || v('NAY_COLOR') },
  { key: 'material', label: 'เนื้อผ้า', cols: ['MATERIAL_CONTENT'], build: v => v('MATERIAL_CONTENT') },
  // ── เฉพาะชีท SETUP_TRACKING ──
  {
    key: 'source', label: 'ที่มา (booking/แผนใหม่)', cols: ['PLAN_SOURCE'],
    build: v => { const s = v('PLAN_SOURCE'); return s === 'OLD' ? 'booking' : s === 'NEW' ? 'แผนใหม่' : s },
  },
  {
    key: 'prevmc', label: 'เครื่องสัปดาห์ก่อน', cols: ['MC_PREV_WEEK'],
    build: v => { const m = v('MC_PREV_WEEK'); return Number(m) > 0 ? `ก่อนหน้า ${m} เครื่อง` : '' },
  },
]

// chip ที่ใช้ได้กับชีทที่กำลังดู (ต้องมีคอลัมน์ที่ฟิลด์นั้นใช้อย่างน้อย 1 คอลัมน์)
export function barFieldsFor(columns = []) {
  const has = new Set(columns)
  return BAR_FIELDS.filter(f => !f.cols || f.cols.some(c => has.has(c)))
}
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
      ['LINE_REMARK', 'หมายเหตุออร์เดอร์'], ['REMARK', 'REMARK'], ['OUTSOURCE', 'จ้างทอ (user สั่ง)'], ['PLAN_SOURCE', 'ที่มาของแผน'],
    ]
  },
]
const PANEL_KNOWN = new Set(PANEL_GROUPS.flatMap(g => g.fields.map(f => f[0])))

export default function PlanGantt({ columns, rows, sheet = '', load = {}, setupJobs = [], ava = {}, bookingMc = {}, poolMap = {}, onMoveWeek, colorRows, onRemove, onEditQty, onSplit, lockBefore = null, bookingItems = [], bookingMode = 'off', bookingPick = null, allMcRows = [], loadFilter = null, setLoadFilter = () => {}, barFields = BAR_FIELDS_DEFAULT, selIdx = null, setSelIdx = () => {}, programRows = {} }) {
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
  // คลิกชิปเครื่องว่างมุมขวาบนของช่อง → การ์ดรายละเอียดว่าเครื่องถูกใช้ไปกับ item ไหนบ้าง
  // เก็บ { w, avaKey, vals } ของช่องที่คลิก (vals = หัวแถว: CAT / เกจ / เครื่อง)
  const [avaSel, setAvaSel] = useState(null)
  // คลิกช่องแถบโหลด (Set up Job/Week) → การ์ดว่าโควตา setup ของสัปดาห์นั้นถูกใช้ไปกับ item ไหน
  // เก็บ { w, t } (t = key ประเภทโรงงาน: OM / PHET_DOUBLE / PHET_SINGLE)
  const [loadSel, setLoadSel] = useState(null)
  // กรองหัวแถวซ้าย: คลิกช่อง Category/Guage → กรอง CAT+เกจ, คลิก Machine → กรองถึงเครื่อง
  // ค่า = { cat, gauge, mcgroup(หรือ null) } ที่ normalize แล้ว, null = แสดงทั้งหมด
  const [catFilter, setCatFilter] = useState(null)
  // สัปดาห์ที่ล็อก (freeze) — โชว์ได้แต่ลาก/ถอด/วางไม่ได้
  // ยกเว้นสัปดาห์ "ปีหน้า" (เลขเล็กที่วนข้ามปี) ที่เลข < lockBefore แต่จริง ๆ อยู่อนาคต
  const isLocked = (w) => {
    if (nextYearWeeks.has(String(w)) || Number(w) === 99) return false
    if (lockBefore != null && Number(w) < Number(lockBefore)) return true
    // แกนอาจเผยสัปดาห์ที่ freeze/ผ่านมาแล้ว (< current+2) — จาก overlay booking หรือจากแถว
    // อดีตในชีทเอง (SETUP_TRACKING) → ล็อกไว้ดูอย่างเดียว กันลากงานแผนย้อนไปวางในอดีต
    if (Number(w) < currentPlanWeek() + 2) return true
    return false
  }

  // Esc = ปิด panel รายละเอียด
  useEffect(() => {
    if (selIdx == null) return
    const onKey = (e) => { if (e.key === 'Escape') setSelIdx(null) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [selIdx])

  // Esc = ปิดการ์ดรายละเอียดเครื่องว่าง
  useEffect(() => {
    if (!avaSel) return
    const onKey = (e) => { if (e.key === 'Escape') setAvaSel(null) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [avaSel])

  // Esc = ปิดการ์ดโควตา setup (แถบโหลด)
  useEffect(() => {
    if (!loadSel) return
    const onKey = (e) => { if (e.key === 'Escape') setLoadSel(null) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [loadSel])

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
      // TYPE = ประเภท job ที่ Planning.py คิดไว้แล้ว (OM/PHET_DOUBLE/PHET_SINGLE) — มีในชีท
      // SETUP_TRACKING ที่ไม่มี FACTORY_TYPE/CAT ให้ classifyType คิดเอง
      type: at('TYPE'),
      // ชื่อคอลัมน์สำรองของชีท SETUP_TRACKING (ไม่มี PRODUCE_QTY / ACTUAL_MC)
      qtyAlt: at('KP_WEIGHT'), mcAlt: at('MC_THIS_WEEK'),
    }
  }, [columns])

  // ชีท SETUP_TRACKING = มุมมองประวัติ setup → เพิ่ม Item เป็นหัวแถว (1 แถว = เครื่อง+item)
  const itemRowSheet = ITEM_ROW_SHEETS.has(String(sheet).trim().toUpperCase())

  // คอลัมน์หัวแถวที่มีอยู่จริง + ตำแหน่ง sticky (left สะสม)
  const groups = useMemo(() => {
    let left = 0
    return (itemRowSheet ? [...GROUP_DEF, ITEM_GROUP] : GROUP_DEF)
      .filter(g => columns.includes(g.col))
      .map(g => { const item = { ...g, idx: columns.indexOf(g.col), left }; left += g.width; return item })
  }, [columns, itemRowSheet])
  // item เป็นหัวแถวของชีทนี้ → ใช้เป็นตัวกำหนดสีของบล็อกด้วย (สีต่าง item ต่างกัน)
  const itemInHeader = groups.some(g => g.col === 'ITEM_CODE')

  // เปลี่ยนชีท/ชุดคอลัมน์หัวแถว → ล้างตัวกรองเก่า (keys ของชีทเดิมจะไม่ตรงกับชีทใหม่ = แถวหายหมด)
  // ต้องอยู่ "เหนือ" early return ทุกจุดด้านล่าง — ไม่งั้นตอนกรอง/ค้นหาจนเหลือ 0 แถว hook นี้จะไม่ถูก
  // เรียก → React error "Rendered fewer hooks than expected" → แอปดับทั้งหน้า
  const groupSig = groups.map(g => g.col).join('|')
  useEffect(() => { setCatFilter(null) }, [groupSig])

  // คอลัมน์กำหนดสี — ปกติ CAT + เกจ ; ชีทที่แยกแถวตาม item (SETUP_TRACKING) = สีตาม ITEM_CODE
  // เพื่อให้ item ต่างกันสีต่างกัน ไล่ดูประวัติของ item เดียวข้ามสัปดาห์ได้ด้วยสี
  const colorByItem = itemInHeader && columns.includes('ITEM_CODE')
  const colorCols = useMemo(
    () => (colorByItem
      ? [columns.indexOf('ITEM_CODE')]
      : COLOR_DEF.filter(n => columns.includes(n)).map(n => columns.indexOf(n))),
    [columns, colorByItem])

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
    const FIELD = { CAT: 'cat', MC_GUAGE: 'gauge', MC_GROUP: 'mc_group', ITEM_CODE: 'item' }
    // ต้อง map ทุกคอลัมน์หัวแถวเป็นฟิลด์ booking ได้ ไม่งั้น key ไม่ตรงแถวแผน → ไม่ overlay
    if (!groups.every(g => FIELD[g.col])) return empty
    // โหมด plan: กรองด้วยชุด ITEM_CODE ที่ติ๊กไว้ (null/ไม่มี = ไม่โชว์อะไร) ; all = ไม่กรอง item
    const allowed = bookingMode === 'plan' ? (bookingPick || new Set()) : null
    // ย้อนหลังได้ HISTORY_WEEKS_BACK สัปดาห์ (booking มี week ถึง 24 — เก่ากว่านั้นไม่ยืดแกนซ้าย)
    const wkFloor = currentPlanWeek() - HISTORY_WEEKS_BACK
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
      const cur = currentPlanWeek()
      let lo = lockBefore != null ? Number(lockBefore) - 2 : cur + 2
      // ขยายขอบซ้ายให้ครอบ "สัปดาห์อดีตที่มีของจริง" — ทั้งแถวในชีท (เช่น SETUP_TRACKING ที่มี
      // แถว OLD ย้อนหลัง) และ overlay booking — แต่ไม่เกิน HISTORY_WEEKS_BACK สัปดาห์
      const pastFloor = cur - HISTORY_WEEKS_BACK
      const hasContent = new Set([...jobW, ...bookingData.weeks].map(Number))
      const past = arr.map(Number).filter(n =>
        Number.isFinite(n) && n !== 99 && n < lo && n >= pastFloor
        && !isNextYearWeek(n, cur) && hasContent.has(n))
      if (past.length) lo = Math.min(lo, ...past)
      // W99 = sentinel "งานล้น" → ไม่โชว์ใน Gantt (ผู้ใช้ขอ)
      // สัปดาห์ปีนี้ = เลข >= lo ; สัปดาห์ปีหน้า = สัปดาห์ที่มีงานจริงและเลขวนข้ามปี (ดู
      //   isNextYearWeek) นำมาต่อท้ายปีนี้ ; สัปดาห์อดีตที่เก่ากว่า pastFloor = ไม่โชว์
      // ตัดสัปดาห์หยุด (ไม่มีทั้งงาน/AVA/โหลด เช่น W31) ออกโดยปริยาย เพราะกรองจาก vals อยู่แล้ว
      const thisYear = arr.filter(w => { const n = Number(w); return n >= lo && n !== 99 })
        .sort((a, b) => Number(a) - Number(b))
      const nextYear = arr.filter(w => { const n = Number(w); return n !== 99 && n < lo && jobW.has(w) && isNextYearWeek(n, cur) })
        .sort((a, b) => Number(a) - Number(b))
      return [...thisYear, ...nextYear]
    }
    return arr.filter(w => w !== '99').sort((a, b) => String(a).localeCompare(String(b), 'th', { numeric: true }))
  }, [rows, ci, supported, load, ava, lockBefore, bookingData])

  // สัปดาห์ "ปีหน้า" (เลขเล็กที่วนข้ามปี) — ไม่ควรถูกล็อกในโหมด freeze แม้เลข < lockBefore
  const nextYearWeeks = useMemo(() => {
    const s = new Set()
    if (!supported) return s
    const cur = currentPlanWeek()
    for (const { row } of rows) {
      const v = norm(row[ci.week])
      // เฉพาะเลขที่วนข้ามปีจริง — สัปดาห์อดีตของปีนี้ (SETUP_TRACKING แถว OLD) ต้องยังถูกล็อก
      if (v !== '' && isNextYearWeek(Number(v), cur)) s.add(v)
    }
    return s
  }, [rows, ci, supported])

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

    // โหมด Item (ทั้งหมด): เติมทุกกลุ่มเครื่องใน MasterMC ที่ยังไม่มีแถว — ให้เห็นครบทุกเครื่อง
    // ทุก CAT แม้สัปดาห์นั้นไม่มีทั้งงานแผนและงาน booking (เดิมแถวมาจากงานที่มีอยู่เท่านั้น)
    // เทียบด้วยคู่ (เกจ|เครื่อง) ไม่ใช่ key เต็ม เพราะเครื่องตัวเดียวถูกเรียก CAT คนละชื่อได้
    // (booking = SYN-30|22|SYN แต่ MasterMC = DOUBLE-30|22|SYN) → ถ้าเทียบ key เต็มจะได้แถวซ้ำ
    if (bookingMode === 'all' && allMcRows.length && avaCatI >= 0 && avaGaugeI >= 0 && mcGroupI >= 0) {
      const seen = new Set([...m.values()].map(r => nkey(r.vals[avaGaugeI]) + '|' + nkey(r.vals[mcGroupI])))
      for (const r of allMcRows) {
        if (seen.has(nkey(r.gauge) + '|' + nkey(r.mc_group))) continue
        seen.add(nkey(r.gauge) + '|' + nkey(r.mc_group))
        const vals = groups.map(() => '')
        vals[avaCatI] = r.cat
        vals[avaGaugeI] = r.gauge
        vals[mcGroupI] = r.mc_group
        add(vals)
      }
    }
    return [...m.values()].sort((a, b) =>
      a.vals.join('|').localeCompare(b.vals.join('|'), 'th', { numeric: true }))
  }, [rows, groups, supported, avaCatI, avaGaugeI, mcGroupI, bookingData, bookingMode, allMcRows])

  // ประเภทโหลด (OM / PHET_DOUBLE / PHET_SINGLE) ของแต่ละแถว gantt
  // 1) ถ้าชีทมีคอลัมน์ TYPE (SETUP_TRACKING) ใช้ค่านั้น — Planning.py คิดจาก MC_GROUP ตอนหัก job
  //    ชีทนี้ไม่มี FACTORY_TYPE/CAT ให้ classifyType คิด → เดิมทุกแถวไม่มีประเภท กดกรอง Select
  //    แล้วแถวหายทั้งหมด
  // 2) ไม่มี TYPE → classifyType(FACTORY_TYPE, CAT) เหมือนแถวสรุปโหลด (ชีท PLAN)
  // งานที่ไม่เข้าประเภท (เช่น จ้างทอ / OUTSOURCE_COMKN) ไม่มีใน map → ถูกซ่อนเมื่อเปิดกรอง
  const LOAD_TYPE_KEYS = useMemo(() => new Set(LOAD_TYPES.map(t => t.key)), [])
  const rowType = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const { row } of rows) {
      const raw = ci.type >= 0 ? nkey(row[ci.type]) : ''
      const t = LOAD_TYPE_KEYS.has(raw)
        ? raw
        : classifyType(ci.factory >= 0 ? row[ci.factory] : '', ci.cat >= 0 ? row[ci.cat] : '')
      if (t) m.set(rowKey(row), t)
    }
    // โหมด Item (ทั้งหมด): แถวเครื่องที่เติมจาก MasterMC (allMcRows) ไม่มีงานจริงใน `rows`
    // → ไม่มีประเภทให้ Select (OMNOI/PHET…) กรอง เลยถูกกรองทิ้งหมดเมื่อกด Select ที่ไม่ใช่ "ทั้งหมด"
    // จำแนกจาก FACTORY ของ MasterMC เอง (key ต้องตรงรูปแบบเดียวกับที่ gantRows เติมแถว)
    if (bookingMode === 'all' && allMcRows.length && avaCatI >= 0 && avaGaugeI >= 0 && mcGroupI >= 0) {
      for (const r of allMcRows) {
        const vals = groups.map(() => '')
        vals[avaCatI] = r.cat
        vals[avaGaugeI] = r.gauge
        vals[mcGroupI] = r.mc_group
        const key = vals.join('')
        if (m.has(key)) continue
        const t = classifyType(r.factory, r.cat)
        if (t) m.set(key, t)
      }
    }
    return m
  }, [rows, ci, groups, supported, LOAD_TYPE_KEYS, bookingMode, allMcRows, avaCatI, avaGaugeI, mcGroupI])

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
    // โหมดสีตาม item: ให้ทุก item สีต่างกัน (ไม่เข้าเฉด DOUBLE/SINGLE เพราะ key เป็นรหัส item)
    if (colorByItem) {
      colorKeys.forEach((k, i) => m.set(k, itemColor(i)))
      return m
    }
    const buckets = { DOUBLE: [], SINGLE: [] }
    for (const k of colorKeys) { const g = catGroupOf(k); if (g) buckets[g].push(k) }
    for (const g of ['DOUBLE', 'SINGLE']) {
      const arr = buckets[g]
      arr.forEach((k, i) => m.set(k, _lerpStops(CAT_GRAD[g], arr.length <= 1 ? 0 : i / (arr.length - 1))))
    }
    return m
  }, [colorKeys, colorByItem])
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
      // ชีท SETUP_TRACKING (มี SETUP_MC + CARRYOVER_MC): บอกบนบล็อกว่าสัปดาห์นั้น setup เครื่องใหม่
      // กี่เครื่อง และ carry (วิ่งต่อจากสัปดาห์ก่อน ไม่ต้อง setup) กี่เครื่อง — รวม = MC_THIS_WEEK
      const hasSetupMc = colIdx['SETUP_MC'] != null && colIdx['CARRYOVER_MC'] != null
      const stSetup = hasSetupMc ? (Number(v('SETUP_MC')) || 0) : 0
      const stCarry = hasSetupMc ? (Number(v('CARRYOVER_MC')) || 0) : 0
      m.get(key).push({
        idx, row, tags, lateRdd,
        item: norm(row[ci.item]),
        stMc: hasSetupMc ? `setup ${stSetup} · carry ${stCarry}` : '',
        stSetup, stCarry,
        stTotal: hasSetupMc ? (Number(v('MC_THIS_WEEK')) || stSetup + stCarry) : 0,
        stSource: v('PLAN_SOURCE'),
        ck: colorKey(row),
        qty: ci.qty >= 0 ? norm(row[ci.qty]) : (ci.qtyAlt >= 0 ? norm(row[ci.qtyAlt]) : ''),
        actualmc: ci.actualmc >= 0 ? norm(row[ci.actualmc]) : (ci.mcAlt >= 0 ? norm(row[ci.mcAlt]) : ''),
        mc: mcKind(num(ci.newmc), num(ci.carrymc), num(ci.sharedmc), num(ci.bookingmc), isOutsource),
        remark: ci.remark >= 0 ? norm(row[ci.remark]) : '',
        setup: v('SETUP_DAYS'),
        // ยอดที่ลูกค้าเปิดมาแบ่งพับแล้วไม่เป็นพับคู่ (คิดจากยอดรวมทั้ง order ไม่ใช่รายสัปดาห์)
        foldWarn: String(v('FOLD_WARN')) === '1',
        foldQty: v('FOLD_QTY'),
        foldRem: v('FOLD_REMAINDER'),
        core: String(v('IS_CORE_ITEM')).trim() !== '',
        // ทีมจากชีท Program (Master) — มีค่า = item+ทีมตรงกัน → ชื่อ item เป็นสีน้ำเงิน
        progTeam: programRows[idx] || '',
      })
    }
    return m
  }, [rows, ci, groups, colorCols, supported, barFields, colIdx, programRows])

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
  // คืน { mc, detail }:
  //   mc[w@@catKey]     = { normal, poly, cotton } (ยอดรวม — ใช้คิดเครื่องว่างบนชิป)
  //   detail[w@@catKey] = [{ item, mcg, gauge, kind, mc, bk, net, qty }] (รายตัว — ใช้ในการ์ด
  //     "รายละเอียดเครื่องว่าง" ให้เห็นว่า item ไหนกินเครื่องเท่าไร) มาจากลูปเดียวกัน ตัวเลขจึงตรงกันเสมอ
  const planMcAgg = useMemo(() => {
    const m = {}, detail = {}
    if (!supported || ci.cat < 0 || ci.gauge < 0) return { mc: m, detail }
    const hasBkCol = ci.bookingmc >= 0
    const qtyI = ci.qty >= 0 ? ci.qty : ci.qtyAlt
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
      const qty = qtyI >= 0 ? (Number(norm(row[qtyI])) || 0) : 0
      // NEW_MC = ตั้งเครื่องใหม่, CARRYOVER_MC = เครื่องอุ่นที่ยกมา (รวมกัน = ACTUAL_MC ทุกแถว)
      const setup = ci.newmc >= 0 ? (Number(norm(row[ci.newmc])) || 0) : 0
      const carry = ci.carrymc >= 0 ? (Number(norm(row[ci.carrymc])) || 0) : 0
      // MC_BOOKING เท่ากันทุกแถวของ (item×เครื่อง×เกจ×สัปดาห์) เดียวกัน → เก็บค่าเดียว (max)
      const bkCol = hasBkCol ? (Number(norm(row[ci.bookingmc])) || 0) : null
      const cur = byItem.get(k)
      if (cur) {
        cur.mc += mc; cur.qty += qty; cur.setup += setup; cur.carry += carry
        if (bkCol != null) cur.bk = Math.max(cur.bk, bkCol)
      }
      else byItem.set(k, { w, catKey, bkKey, item, mcg, gauge: g, mc, qty, setup, carry, bk: bkCol })
    }
    for (const e of byItem.values()) {
      const { w, catKey, bkKey, item, mc, bk } = e
      const bkVal = bk != null ? bk : (Number(bookingMc?.[w]?.[bkKey]) || 0)
      const net = Math.max(0, mc - bkVal)
      // เข้าถัง POLY/COTTON เฉพาะเมื่อกลุ่มนี้มีเครื่องกันไว้จริง — ไม่งั้นงาน POLY
      // ในกลุ่มที่ไม่มี reservation จะกินเครื่องปกติ (ตรงกับ Planning.py)
      const rsv = ava?.[w]?.[catKey]?.reserved
      const t = itemPoolType(item)
      const kind = (t === 'poly' && rsv && rsv.poly > 0) ? 'poly'
        : (t === 'cotton' && rsv && rsv.cotton > 0) ? 'cotton' : 'normal'
      const dk = w + '@@' + catKey
      // รายละเอียดเก็บทุกแถวที่มีเครื่อง (รวม net = 0 ที่นั่งบนเครื่อง booking) — การ์ดต้องอธิบายได้
      // ว่าทำไมงานก้อนนั้นไม่กินเครื่องเพิ่ม ; ยอดรวมยังนับเฉพาะ net > 0 เหมือนเดิม
      if (mc > 0 || net > 0) (detail[dk] || (detail[dk] = [])).push({ ...e, bk: bkVal, net, kind })
      if (net === 0) continue
      const slot = m[dk] || (m[dk] = { normal: 0, poly: 0, cotton: 0 })
      slot[kind] += net
    }
    for (const k in detail) detail[k].sort((a, b) => b.net - a.net || b.mc - a.mc)
    return { mc: m, detail }
  }, [rows, ci, supported, bookingMc, ava, poolMap])
  const planMcByWeekCat = planMcAgg.mc

  // งาน booking (แผนเก่าที่ commit แล้ว) ของช่องที่เปิดการ์ดอยู่ — เครื่องที่ booking จองไปแล้ว
  // ถูกหักออกจาก TOTAL_MC_REMAIN ตั้งแต่ต้นทาง ช่องจึงเกินได้ทั้งที่แผนรอบนี้ไม่มีงานเลย
  // → การ์ดต้องบอกได้ว่าเป็น item ไหน (ต่างจาก overlay 📋 ที่โชว์แค่ 5 สัปดาห์ย้อนหลังและปิดได้)
  // คิดเฉพาะตอนเปิดการ์ด (avaSel) เพราะ bookingItems ยาวหลักพันแถว
  const avaBooking = useMemo(() => {
    if (!avaSel) return []
    const out = []
    for (const b of bookingItems) {
      if (norm(b.week) !== avaSel.w) continue
      if (poolKeyOf(b.cat, b.gauge, b.mc_group) !== avaSel.avaKey) continue
      const rsv = ava?.[avaSel.w]?.[avaSel.avaKey]?.reserved
      const t = itemPoolType(b.item)
      out.push({
        item: norm(b.item), mcg: norm(b.mc_group), gauge: norm(b.gauge),
        mc: Number(b.mc) || 0, qty: Number(b.qty) || 0, so: norm(b.so),
        // setup = เครื่องที่ต้องตั้งใหม่, carry = เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (setup + carry = mc)
        setup: Number(b.setup) || 0, carry: Number(b.carry) || 0,
        kind: (t === 'poly' && rsv && rsv.poly > 0) ? 'poly'
          : (t === 'cotton' && rsv && rsv.cotton > 0) ? 'cotton' : 'normal',
      })
    }
    out.sort((a, b) => b.mc - a.mc || b.qty - a.qty)
    return out
  }, [avaSel, bookingItems, ava, poolMap])

  // รายการ job ของช่องแถบโหลดที่เปิดการ์ดอยู่ (สัปดาห์ × ประเภทโรงงาน) — 2 ชุด
  //   plan = งานของแผนรอบนี้ที่ตั้งเครื่องใหม่ (NEW_MC > 0) อ่านสดจากแถวบน Gantt → ขยับตามการลาก
  //          รวมแถวที่แตกไว้ (item × เครื่อง × เกจ เดียวกัน) เป็นรายการเดียว ไม่งั้นนับ job ซ้ำตา
  //   old  = job ของแผนเดิม/booking จากชีท SETUP_TRACKING (คงที่ ย้ายบน Gantt ไม่ได้)
  // คิดเฉพาะตอนเปิดการ์ด (loadSel) — setupJobs ยาวหลายร้อยแถว
  const loadDetail = useMemo(() => {
    const empty = { plan: [], old: [] }
    if (!loadSel) return empty
    const qtyI = ci.qty >= 0 ? ci.qty : ci.qtyAlt
    const byKey = new Map()
    if (supported) {
      for (const { row } of rows) {
        if (norm(row[ci.week]) !== loadSel.w) continue
        const t = classifyType(ci.factory >= 0 ? row[ci.factory] : '', ci.cat >= 0 ? row[ci.cat] : '')
        if (t !== loadSel.t) continue
        const setup = ci.newmc >= 0 ? (Number(norm(row[ci.newmc])) || 0) : 0
        if (setup <= 0) continue                     // ไม่ตั้งเครื่องใหม่ = ไม่กินโควตา job
        const item = norm(row[ci.item])
        const mcg = ci.mcgroup >= 0 ? norm(row[ci.mcgroup]) : ''
        const gauge = ci.gauge >= 0 ? norm(row[ci.gauge]) : ''
        const k = `${item}|${mcg}|${gauge}`
        const carry = ci.carrymc >= 0 ? (Number(norm(row[ci.carrymc])) || 0) : 0
        const mc = ci.actualmc >= 0 ? (Number(norm(row[ci.actualmc])) || 0) : 0
        const qty = qtyI >= 0 ? (Number(norm(row[qtyI])) || 0) : 0
        const cur = byKey.get(k)
        if (cur) { cur.setup += setup; cur.carry += carry; cur.mc += mc; cur.qty += qty }
        else byKey.set(k, { item, mcg, gauge, setup, carry, mc, qty, so: ci.sc >= 0 ? norm(row[ci.sc]) : '' })
      }
    }
    const plan = [...byKey.values()].sort((a, b) => b.setup - a.setup || b.qty - a.qty)
    const old = setupJobs
      .filter(j => j.source === 'OLD' && String(j.week) === loadSel.w && j.type === loadSel.t)
      .sort((a, b) => b.jobs - a.jobs || b.qty - a.qty)
    return { plan, old }
  }, [loadSel, rows, ci, supported, setupJobs])

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

  // คลิกหัวแถวซ้าย = กรองด้วย "prefix ของคอลัมน์หัวแถว" — คลิกช่องที่ n → เอาค่าคอลัมน์ 0..n
  // เช่น PLAN: คลิก Category = กรอง CAT / คลิก Machine = CAT+เกจ+เครื่อง
  //      SETUP_TRACKING (Guage|Machine|Item): คลิก Machine = เกจ+เครื่อง / คลิก Item = ถึง item
  // ทำแบบ prefix เพื่อให้ใช้ได้ทุกชีทโดยไม่ต้องมีคอลัมน์ CAT (เดิมบังคับต้องมี CAT+เกจ
  // → SETUP_TRACKING กดหัวแถวไม่ได้เลย)
  const catClickable = groups.length > 0
  const catMatch = (vals, f) => !!f && f.keys.every((k, i) => nkey(vals[i]) === k)
  // คลิกช่องหัวแถว group ที่ตำแหน่ง n → ตั้ง/สลับตัวกรอง (คลิกซ้ำช่องเดิม = ล้าง)
  const clickGroupCell = (vals, n) => {
    if (!catClickable) return
    const keys = vals.slice(0, n + 1).map(nkey)
    setCatFilter(cur =>
      cur && cur.upto === n && cur.keys.join('|') === keys.join('|')
        ? null : { upto: n, keys })
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
                      🔎 {catFilter.keys.join(' / ')}
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
                    + '\n👆 คลิกเพื่อดูว่า job ถูกใช้ไปกับ item ไหนบ้าง'
                  return (
                    <td key={w} className="gantt-load-cell clickable" style={{ top: loadTops[ti] }}
                      onClick={() => setLoadSel(s => (s && s.w === w && s.t === t.key ? null : { w, t: t.key }))}>
                      {empty ? (
                        <div className="loadwrap" title={`${t.label} • สัปดาห์ ${w}\nไม่มีงาน\n👆 คลิกเพื่อดูรายละเอียด`}>
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
                  // ช่องที่ตรงกับตัวกรองปัจจุบัน → ไฮไลต์ให้เห็นว่ากรองด้วยค่าไหน (ทุกช่องใน prefix)
                  const on = catFilter && n <= catFilter.upto && catMatch(r.vals, catFilter)
                  return (
                    <th key={g.col}
                      className={'gantt-glabel'
                        + (!showRsv && n === groups.length - 1 ? ' gantt-glast' : '')
                        + (catClickable ? ' gantt-gclick' : '')
                        + (on ? ' gantt-gfilter-on' : '')}
                      onClick={catClickable ? () => clickGroupCell(r.vals, n) : undefined}
                      title={catClickable
                        ? `คลิกเพื่อกรองถึงคอลัมน์ "${g.label}" (${groups.slice(0, n + 1).map(x => x.label).join(' + ')}) — คลิกซ้ำเพื่อล้าง`
                        : undefined}
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
                          {rv.poly ? <span className="rsvtag poly">Poly สูงสุด {rv.poly} เครื่อง</span> : null}
                          {rv.cotton ? <span className="rsvtag cotton">Cotton สูงสุด {rv.cotton} เครื่อง</span> : null}
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
                          <span className={'cellava cellava-click' + (remainLive <= 0 ? ' none' : '') + (remainLive < 0 ? ' over' : '')}
                            onClick={e => { e.stopPropagation(); setAvaSel({ w, avaKey, vals: r.vals }) }}
                            title={`${avaTxt} • เครื่องปกติทั้งหมด ${av.total}\nว่างหลัง booking ${av.remain} − แผนจองเพิ่ม(ปกติ) ${planNormal} = ${remainLive}`
                              + (hasRsv ? `\nเครื่องกันไว้ (ใช้แทนงานปกติไม่ได้):\n${rsvTxt}` : '')
                              + '\n👆 คลิกเพื่อดูว่าเครื่องถูกใช้ไปกับ item ไหนบ้าง'}>
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
                            title={`${j.item}${sc ? ` • SC ${sc}` : ''}\n${r.vals.join(' • ')} • สัปดาห์ ${w}${j.qty !== '' ? `\nจำนวน ${j.qty}` : ''}${j.actualmc !== '' ? ` • ใช้ ${j.actualmc} เครื่อง` : ''}${Number(j.setup) > 0 ? ` • setup ${j.setup} วัน` : ''}${j.mc ? `\n${MC_KINDS[j.mc].icon} ${MC_KINDS[j.mc].label}` : ''}${j.remark ? `\n${j.remark}` : ''}\nสี: ${j.ck}${isColor ? '\n★ งานสี (ต้องย้อม)' : ''}${j.lateRdd ? '\n⚠ วางเลยสัปดาห์ RDD' : ''}${j.foldWarn ? `\n⚠ order เปิดมา ${j.foldQty} พับ — ไม่เป็นพับคู่ (เหลือเศษ ${j.foldRem} พับ)` : ''}${j.progTeam ? `\n🏷 Program • ทีม ${j.progTeam}` : ''}\n👆 คลิกเพื่อดูรายละเอียดครบ${onEditQty && !locked && j.qty !== '' ? ' • double click เพื่อแก้จำนวน' : ''}${locked ? '\n🔒 สัปดาห์ freeze — แก้ไม่ได้' : ''}`}>
                            {locked && <span className="gbar-star">🔒</span>}
                            {isColor && !locked && <span className="gbar-star">★</span>}
                            {j.mc && <span className="gbar-mc">{MC_KINDS[j.mc].icon}</span>}
                            {j.foldWarn && (
                              <span className="gbar-fold"
                                title={`order เปิดมา ${j.foldQty} พับ — ไม่เป็นพับคู่ (เหลือเศษ ${j.foldRem} พับ)`}>⚠</span>
                            )}
                            <span className={'gbar-item' + (j.core ? ' core' : '') + (j.progTeam ? ' program' : '')}>{j.item}</span>
                            {j.stMc && (
                              <span className="gbar-tag"
                                title={`setup เครื่องใหม่ ${j.stSetup} เครื่อง (= job ที่หัก)`
                                  + `\ncarry ${j.stCarry} เครื่อง — วิ่งต่อจากสัปดาห์ก่อน ไม่ต้อง setup ไม่หัก job`
                                  + `\nรวมสัปดาห์นี้ ${j.stTotal} เครื่อง`
                                  + (j.stSource ? `\nที่มา: ${j.stSource}` : '')}>{j.stMc}</span>
                            )}
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
                      {/* overlay History แผนเดิม (ย้อนหลัง 5 week) — บล็อกอ่านอย่างเดียว ลาก/แก้/หักเครื่องว่างไม่ได้ */}
                      {(bookingData.cells.get(r.key + '||' + w) || []).map((b, bi) => (
                        <div key={'bk' + bi} className="gbar gbar-booking"
                          title={`📋 History แผนเดิม (ย้อนหลัง ${HISTORY_WEEKS_BACK} week)\n${b.item}${b.so ? ` • SO ${b.so}` : ''}\n${r.vals.join(' • ')} • สัปดาห์ ${w}`
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
                  {m.sample && <span className={'glegend-chip ' + m.cls}>{m.sample}</span>}
                  {m.pre && <b className={'glegend-txt ' + m.cls}>{m.pre}</b>}{m.label}
                </span>
              ))}
            </div>
            <div className="gantt-legend">
              <span className="glegend-title">เครื่องว่าง (คลิกที่ชิปเพื่อดูรายละเอียด):</span>
              {AVA_MARKS.map(m => (
                <span key={m.key} className="glegend" title={m.tip}>
                  <span className={'cellava glegend-ava' + (m.cls ? ' ' + m.cls : '')}>{m.sample}</span>{m.label}
                </span>
              ))}
            </div>
            {colorCols.length > 0 && (
              colorByItem ? (
                // สีตาม item: มีหลายร้อย item — ไม่ไล่ทั้งหมด (ชื่อ item อยู่ที่หัวแถวแล้ว)
                <div className="gantt-legend">
                  <span className="glegend-title">สีตาม Item:</span>
                  <span className="glegend">แต่ละ item สีต่างกัน ({colorKeys.length} item) — ดูชื่อ item ที่หัวแถวซ้าย</span>
                </div>
              ) : (
                <div className="gantt-legend">
                  <span className="glegend-title">สีตาม CAT / Guage:</span>
                  {colorKeys.map(k => (
                    <span key={k} className="glegend">
                      <i style={{ background: colorOf(k) }} />{k}
                    </span>
                  ))}
                </div>
              )
            )}
          </div>
        )}
      </div>

      {selRow && (
        <JobPanel row={selRow} columns={columns} colIdx={colIdx}
          idx={selIdx} weeks={weeks} isLocked={isLocked} onSplit={onSplit} onRemove={onRemove}
          showOther={showOther} onToggleOther={() => setShowOther(s => !s)}
          progTeam={programRows[selIdx] || ''}
          onClose={() => setSelIdx(null)} />
      )}

      {avaSel && (() => {
        const av = ava?.[avaSel.w]?.[avaSel.avaKey]
        if (!av) return null
        const dk = avaSel.w + '@@' + avaSel.avaKey
        return (
          <AvaPanel week={avaSel.w} vals={avaSel.vals} groups={groups}
            mcGroup={mcGroupI >= 0 ? avaSel.vals[mcGroupI] : ''}
            av={av} plan={planMcAgg.mc[dk] || { normal: 0, poly: 0, cotton: 0 }}
            detail={planMcAgg.detail[dk] || []} booking={avaBooking}
            onClose={() => setAvaSel(null)} />
        )
      })()}

      {loadSel && (() => {
        const t = LOAD_TYPES.find(x => x.key === loadSel.t)
        if (!t) return null
        const info = (load[loadSel.w] && load[loadSel.w][loadSel.t]) || {}
        const cap = (info.cap != null && info.cap !== '') ? info.cap : capByType[loadSel.t]
        // ใหม่(live) = job จาก booking + NEW_MC ของแถวที่วางจริง — สูตรเดียวกับแถบ ตัวเลขจึงตรงกัน
        const nw = Math.max(0, (info.bookingNew || 0) + (planNewByWeekType[loadSel.w + '|' + loadSel.t] || 0))
        return (
          <LoadPanel week={loadSel.w} type={t} cap={cap} old={info.old || 0} nw={nw}
            bookingNew={info.bookingNew || 0}
            plan={loadDetail.plan} oldJobs={loadDetail.old}
            onClose={() => setLoadSel(null)} />
        )
      })()}
    </div>
  )
}

/**
 * การ์ดโควตา Set up ของ 1 ช่องแถบโหลด (ประเภทโรงงาน × สัปดาห์) — เปิดจากคลิกช่องแถบโหลด
 * ตอบคำถาม "job เต็ม/เกินเพราะ item ไหนบ้าง และย้ายตัวไหนออกได้"
 *
 * 1 job = ตั้งเครื่องใหม่ 1 เครื่อง (NEW_MC / SETUP_MC) — งานที่รันต่อจากสัปดาห์ก่อนไม่กินโควตา
 * งานแผนรอบนี้อ่านสดจากแถวบน Gantt (ลากแล้วตัวเลขขยับทันที) ส่วนแผนเดิมมาจากชีท SETUP_TRACKING
 */
function LoadPanel({ week, type, cap, old, nw, bookingNew = 0, plan = [], oldJobs = [], onClose }) {
  const nf = (n) => (Math.round((Number(n) || 0) * 100) / 100).toLocaleString()
  const total = old + nw
  const hasCap = cap != null && cap !== ''
  const free = hasCap ? cap - total : null
  const over = hasCap && total > cap

  // คลิกนอกการ์ด = ปิด (เหมือนการ์ดเครื่อง) — ใช้ mousedown ให้เปิดช่องอื่นต่อได้ทันที
  const boxRef = useRef(null)
  useEffect(() => {
    const onDown = (e) => { if (boxRef.current && !boxRef.current.contains(e.target)) onClose() }
    window.addEventListener('mousedown', onDown)
    return () => window.removeEventListener('mousedown', onDown)
  }, [onClose])

  const planJobs = plan.reduce((s, d) => s + (d.setup || 0), 0)
  const planQty = plan.reduce((s, d) => s + (d.qty || 0), 0)
  const oldJ = oldJobs.reduce((s, d) => s + (d.jobs || 0), 0)
  const oldQty = oldJobs.reduce((s, d) => s + (d.qty || 0), 0)

  return (
    <aside className="jobpanel avapanel" ref={boxRef}>
      <div className="jobpanel-head">
        <div>
          <b className="jobpanel-item">Set up ในสัปดาห์ W{week}</b>
          <div className="jobpanel-sub">{type.long || type.label}</div>
        </div>
        <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
      </div>

      <div className="jobpanel-flags">
        {over
          ? <span className="jobflag warn">⚠ เกินโควตา {nf(total - cap)} job</span>
          : hasCap
            ? <span className="jobflag">{free === 0 ? 'เต็มโควตาพอดี' : `ว่างอีก ${nf(free)} job`}</span>
            : <span className="jobflag">ไม่มีโควตากำหนดไว้สำหรับสัปดาห์นี้</span>}
      </div>

      <div className="jobpanel-body">
        <div className="jobsec">
          <h4>โควตา Set up</h4>
          <dl className="jobdl">
            <dt>โควตาทั้งหมด</dt><dd>{hasCap ? `${nf(cap)} job/week` : '—'}</dd>
            <dt>แผนเดิม (booking)</dt><dd>− {nf(old)}</dd>
            <dt>แผนรอบนี้ (live)</dt><dd>− {nf(nw)}</dd>
            <dt>คงเหลือ</dt>
            <dd className={over ? 'warn' : undefined}>
              {!hasCap ? `ใช้ไป ${nf(total)}` : over ? `เกิน ${nf(total - cap)}` : free === 0 ? 'เต็มพอดี (0)' : nf(free)}
            </dd>
          </dl>
        </div>

        <div className="jobsec">
          <h4>🆕 แผนรอบนี้ ({nf(planJobs)} job)</h4>
          {plan.length === 0 ? (
            <div className="hint small" style={{ padding: 0 }}>
              แผนรอบนี้ไม่มีงานที่ตั้งเครื่องใหม่ในสัปดาห์นี้ — โควตาถูกใช้โดยแผนเดิมทั้งหมด
            </div>
          ) : (
            <table className="avatbl">
              <thead>
                <tr>
                  <th>Item</th><th>เครื่อง</th><th className="num">เกจ</th>
                  <th className="num">จำนวน (กก.)</th>
                  <th className="num" title="job ที่กินโควตา = เครื่องที่ตั้งใหม่ (NEW_MC)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (CARRYOVER_MC) — ไม่กินโควตา">Continue</th>
                </tr>
              </thead>
              <tbody>
                {plan.map((d, i) => (
                  <tr key={i}
                    title={`${d.item} • ${d.mcg} เกจ ${d.gauge}${d.so ? `\nSO ${d.so}` : ''}\n`
                      + `ใช้เครื่อง ${nf(d.mc)} = Setup ${nf(d.setup)} + Continue ${nf(d.carry)}\n`
                      + '🆕 ย้ายงานนี้ไปสัปดาห์อื่นบน Gantt = คืนโควตา job ให้สัปดาห์นี้'}>
                    <td>{d.item}</td>
                    <td>{d.mcg}</td>
                    <td className="num">{d.gauge}</td>
                    <td className="num">{nf(d.qty)}</td>
                    <td className="num setup">🔧 {nf(d.setup)}</td>
                    <td className={'num' + (d.carry > 0 ? '' : ' zero')}>{d.carry > 0 ? nf(d.carry) : '–'}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td colSpan={3}>รวม</td><td className="num">{nf(planQty)}</td>
                  <td className="num">{nf(planJobs)}</td><td className="num" />
                </tr>
              </tfoot>
            </table>
          )}
          {bookingNew > 0 && (
            <div className="hint small" style={{ padding: 0 }}>
              + อีก {nf(bookingNew)} job มาจาก booking (ไม่มีแถวบน Gantt ให้ย้าย)
            </div>
          )}
        </div>

        <div className="jobsec">
          <h4>📋 แผนเดิม / booking ({nf(oldJ)} job)</h4>
          {oldJobs.length === 0 ? (
            <div className="hint small" style={{ padding: 0 }}>ไม่มี job ของแผนเดิมในสัปดาห์นี้</div>
          ) : (
            <table className="avatbl">
              <thead>
                <tr>
                  <th>Item</th><th>เครื่อง</th><th className="num">เกจ</th>
                  <th className="num">จำนวน (กก.)</th>
                  <th className="num" title="job ที่กินโควตา = เครื่องที่ตั้งใหม่ (SETUP_MC)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (CARRYOVER_MC) — ไม่กินโควตา">Continue</th>
                </tr>
              </thead>
              <tbody>
                {oldJobs.map((d, i) => (
                  <tr key={i}
                    title={`${d.item} • ${d.mcg} เกจ ${d.gauge}${d.so ? `\nSO ${d.so}` : ''}\n`
                      + `ใช้เครื่อง ${nf(d.mc)} = Setup ${nf(d.setup)} + Continue ${nf(d.carry)}`
                      + (d.days ? `\nวัน setup ${nf(d.days)} วัน` : '')
                      + '\n📋 แผนเก่าที่ commit แล้ว — ย้าย/แก้บน Gantt ไม่ได้'}>
                    <td>{d.item}</td>
                    <td>{d.mcg}</td>
                    <td className="num">{d.gauge}</td>
                    <td className="num">{nf(d.qty)}</td>
                    <td className="num setup">🔧 {nf(d.setup || d.jobs)}</td>
                    <td className={'num' + (d.carry > 0 ? '' : ' zero')}>{d.carry > 0 ? nf(d.carry) : '–'}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td colSpan={3}>รวม</td><td className="num">{nf(oldQty)}</td>
                  <td className="num">{nf(oldJ)}</td><td className="num" />
                </tr>
              </tfoot>
            </table>
          )}
        </div>

        <div className="jobsec">
          <div className="hint small" style={{ padding: 0 }}>
            {over
              ? <>1 job = ตั้งเครื่องใหม่ 1 เครื่อง — ต้องย้ายงาน 🆕 ออกไปสัปดาห์อื่นรวม <b>{nf(total - cap)} job</b>
                ช่องนี้ถึงจะพอดี (งาน 📋 แผนเดิมย้ายไม่ได้ ถ้าเกินจากแผนเดิมล้วนต้องแก้ที่ booking)</>
              : <>1 job = ตั้งเครื่องใหม่ 1 เครื่อง (Setup) — งานที่รันต่อจากสัปดาห์ก่อน (Continue) ไม่กินโควตา</>}
          </div>
        </div>
      </div>
    </aside>
  )
}

/**
 * การ์ดรายละเอียดเครื่องว่างของ 1 ช่อง (พูล CAT|เกจ × สัปดาห์) — เปิดจากคลิกชิปมุมขวาบน
 * ตอบคำถาม "เกินมากี่เครื่อง และเครื่องถูกใช้ไปกับ item ไหนบ้าง วางไว้กี่กิโล"
 *
 * เครื่องแชร์กันทั้งพูล (CAT|เกจ) → รายการที่โชว์คือ "ทุก item ในพูลเดียวกัน" ไม่ใช่เฉพาะแถวที่คลิก
 * เพราะการเกินเกิดจากยอดรวมทั้งพูล ไม่ใช่จากเครื่องกลุ่มเดียว (แถวของเครื่องที่คลิกทำตัวหนาไว้)
 */
function AvaPanel({ week, vals, groups, mcGroup = '', av, plan, detail, booking = [], onClose }) {
  const nf = (n) => (Math.round((Number(n) || 0) * 100) / 100).toLocaleString()
  const total = Number(av.total) || 0
  const remain = Number(av.remain) || 0
  // เครื่องที่ booking จองไปแล้ว = MC_USE_CEIL (ถ้าไม่มีค่อยอนุมานจาก ทั้งหมด − ว่างหลัง booking)
  const bkUsed = av.used != null ? (Number(av.used) || 0) : Math.max(0, total - remain)
  const remainLive = remain - plan.normal
  const rsv = av.reserved || null
  const rsvP = rsv ? (rsv.poly || 0) : 0
  const rsvC = rsv ? (rsv.cotton || 0) : 0
  const rsvPused = rsv ? (rsv.poly_used || 0) : 0
  const rsvCused = rsv ? (rsv.cotton_used || 0) : 0
  const polyLeft = rsvP - rsvPused - plan.poly
  const cottonLeft = rsvC - rsvCused - plan.cotton
  // ถังที่ล้น → ใช้ทำแถบแดงในตาราง (งานปกติล้น ไม่ได้แปลว่างาน POLY ผิด)
  const over = {
    normal: remainLive < 0,
    poly: rsvP > 0 && polyLeft < 0,
    cotton: rsvC > 0 && cottonLeft < 0,
  }
  const KIND = { normal: 'ปกติ', poly: '🔒 POLY', cotton: '🔒 COTTON' }

  // คลิกที่ไหนก็ได้นอกการ์ด = ปิด (ไม่ต้องเล็งกากบาท) — ใช้ mousedown เพื่อให้ปิดทันทีที่กด
  // และคลิกชิปช่องอื่นต่อได้เลย (mousedown ปิดใบเก่า → click เปิดใบใหม่)
  const boxRef = useRef(null)
  useEffect(() => {
    const onDown = (e) => { if (boxRef.current && !boxRef.current.contains(e.target)) onClose() }
    window.addEventListener('mousedown', onDown)
    return () => window.removeEventListener('mousedown', onDown)
  }, [onClose])
  const sumQty = detail.reduce((s, d) => s + (d.qty || 0), 0)
  const sumNet = detail.reduce((s, d) => s + (d.net || 0), 0)
  const sumSetup = detail.reduce((s, d) => s + (d.setup || 0), 0)
  const sumCarry = detail.reduce((s, d) => s + (d.carry || 0), 0)
  const bkQty = booking.reduce((s, d) => s + (d.qty || 0), 0)
  const bkMc = booking.reduce((s, d) => s + (d.mc || 0), 0)
  const bkSetup = booking.reduce((s, d) => s + (d.setup || 0), 0)
  const bkCarry = booking.reduce((s, d) => s + (d.carry || 0), 0)
  const nOver = (over.normal ? -remainLive : 0) + (over.poly ? -polyLeft : 0) + (over.cotton ? -cottonLeft : 0)
  // แผนรอบนี้มีส่วนทำให้ถังที่ล้นเกินหรือไม่ — ถ้าไม่มีเลย ย้ายงานบน Gantt ก็ไม่ช่วย (ต้องแก้ที่ booking)
  const planInOver = (over.normal ? plan.normal : 0) + (over.poly ? plan.poly : 0) + (over.cotton ? plan.cotton : 0)

  return (
    <aside className="jobpanel avapanel" ref={boxRef}>
      <div className="jobpanel-head">
        <div>
          <b className="jobpanel-item">เครื่องในสัปดาห์ W{week}</b>
          <div className="jobpanel-sub">
            {groups.map((g, i) => vals[i]).filter(v => v !== '' && v != null).join(' • ')}
          </div>
        </div>
        <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
      </div>

      <div className="jobpanel-flags">
        {over.normal
          ? <span className="jobflag warn">⚠ เครื่องปกติเกิน {nf(-remainLive)} เครื่อง</span>
          : <span className="jobflag">{remainLive === 0 ? 'เครื่องปกติเต็มพอดี' : `เครื่องปกติว่าง ${nf(remainLive)}`}</span>}
        {over.poly && <span className="jobflag warn">⚠ POLY เกิน {nf(-polyLeft)} เครื่อง</span>}
        {over.cotton && <span className="jobflag warn">⚠ COTTON เกิน {nf(-cottonLeft)} เครื่อง</span>}
      </div>

      <div className="jobpanel-body">
        <div className="jobsec">
          <h4>เครื่องปกติ</h4>
          <dl className="jobdl">
            <dt>เครื่องทั้งหมด</dt><dd>{nf(total)}</dd>
            <dt>booking จองไป</dt><dd>− {nf(bkUsed)}</dd>
            <dt>ว่างหลัง booking</dt><dd>{nf(remain)}</dd>
            <dt>แผนจองเพิ่ม</dt><dd>− {nf(plan.normal)}</dd>
            <dt>คงเหลือ</dt>
            <dd className={remainLive < 0 ? 'warn' : undefined}>
              {remainLive < 0 ? `ไม่ว่าง (เกิน ${nf(-remainLive)})` : remainLive === 0 ? 'เต็มพอดี (0)' : nf(remainLive)}
            </dd>
          </dl>
        </div>

        {(rsvP > 0 || rsvC > 0) && (
          <div className="jobsec">
            <h4>🔒 เครื่องกันไว้ (ใช้แทนงานปกติไม่ได้)</h4>
            <dl className="jobdl">
              {rsvP > 0 && (<>
                <dt>POLY</dt>
                <dd className={polyLeft < 0 ? 'warn' : undefined}>
                  กันไว้ {nf(rsvP)} − booking {nf(rsvPused)} − แผน {nf(plan.poly)} = {polyLeft < 0 ? `เกิน ${nf(-polyLeft)}` : `เหลือ ${nf(polyLeft)}`}
                </dd>
              </>)}
              {rsvC > 0 && (<>
                <dt>COTTON</dt>
                <dd className={cottonLeft < 0 ? 'warn' : undefined}>
                  กันไว้ {nf(rsvC)} − booking {nf(rsvCused)} − แผน {nf(plan.cotton)} = {cottonLeft < 0 ? `เกิน ${nf(-cottonLeft)}` : `เหลือ ${nf(cottonLeft)}`}
                </dd>
              </>)}
            </dl>
          </div>
        )}

        <div className="jobsec">
          <h4>งานของแผนรอบนี้ ({detail.length} รายการ)</h4>
          {detail.length === 0 ? (
            <div className="hint small" style={{ padding: 0 }}>
              แผนรอบนี้ไม่มีงานในกลุ่มนี้ — เครื่องถูกใช้โดย booking ทั้งหมด (ดูรายการข้างล่าง)
            </div>
          ) : (
            <table className="avatbl">
              <thead>
                <tr>
                  <th>Item</th><th>เครื่อง</th>
                  <th className="num">จำนวน (กก.)</th><th className="num">ใช้เครื่อง</th>
                  <th className="num" title="เครื่องที่ต้องตั้งใหม่ (NEW_MC)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (CARRYOVER_MC)">Continue</th>
                </tr>
              </thead>
              <tbody>
                {detail.map((d, i) => (
                  <tr key={i}
                    className={(over[d.kind] ? 'bad' : '') + (d.net === 0 ? ' dim' : '') + (mcGroup && d.mcg === mcGroup ? ' cur' : '')}
                    title={`${d.item} • ${d.mcg} เกจ ${d.gauge} • ${KIND[d.kind]}\n`
                      + `ใช้เครื่องจริง (ACTUAL_MC) ${nf(d.mc)} − เครื่อง booking ${nf(d.bk)} = กินพูล ${nf(d.net)}\n`
                      + `ในเครื่องที่ใช้ ${nf(d.mc)}: Setup ${nf(d.setup)} + Continue ${nf(d.carry)}`
                      + (d.net === 0 ? '\n📦 นั่งบนเครื่องที่วิ่งอยู่แล้ว — ไม่กินเครื่องเพิ่ม' : '')}>
                    <td>
                      {d.item}
                      {d.kind !== 'normal' && <span className={'avatag ' + d.kind}>{KIND[d.kind]}</span>}
                    </td>
                    <td>{d.mcg}</td>
                    <td className="num">{nf(d.qty)}</td>
                    <td className="num">{d.net === 0 ? '📦 0' : nf(d.net)}</td>
                    <td className={'num' + (d.setup > 0 ? ' setup' : ' zero')}>{d.setup > 0 ? `🔧 ${nf(d.setup)}` : '–'}</td>
                    <td className={'num' + (d.carry > 0 ? '' : ' zero')}>{d.carry > 0 ? nf(d.carry) : '–'}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td colSpan={2}>รวม</td><td className="num">{nf(sumQty)}</td><td className="num">{nf(sumNet)}</td>
                  <td className="num">{nf(sumSetup)}</td><td className="num">{nf(sumCarry)}</td>
                </tr>
              </tfoot>
            </table>
          )}
        </div>

        <div className="jobsec">
          <h4>📋 งาน booking ที่จองเครื่องไว้แล้ว ({booking.length} รายการ)</h4>
          {booking.length === 0 ? (
            <div className="hint small" style={{ padding: 0 }}>ไม่มีงาน booking ในกลุ่มนี้</div>
          ) : (
            <table className="avatbl">
              <thead>
                <tr>
                  <th>Item</th><th>เครื่อง</th>
                  <th className="num">จำนวน (กก.)</th><th className="num">ใช้เครื่อง</th>
                  <th className="num" title="เครื่องที่ต้องตั้งใหม่ (_mc_increase)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (_prev_mc_use_ceil)">Continue</th>
                </tr>
              </thead>
              <tbody>
                {booking.map((d, i) => (
                  <tr key={i}
                    className={(over[d.kind] ? 'bad' : '') + (mcGroup && d.mcg === mcGroup ? ' cur' : '')}
                    title={`${d.item} • ${d.mcg} เกจ ${d.gauge} • ${KIND[d.kind]}${d.so ? `\nSO ${d.so}` : ''}\n`
                      + `booking จองไว้ ${nf(d.mc)} เครื่อง (MC_USE_CEIL) • ${nf(d.qty)} กก.\n`
                      + `Setup ${nf(d.setup)} + Continue (ต่อจากสัปดาห์ก่อน) ${nf(d.carry)} = ${nf(d.mc)}\n`
                      + '📋 แผนเก่าที่ commit แล้ว — ย้าย/แก้บน Gantt ไม่ได้'}>
                    <td>
                      {d.item}
                      {d.kind !== 'normal' && <span className={'avatag ' + d.kind}>{KIND[d.kind]}</span>}
                    </td>
                    <td>{d.mcg}</td>
                    <td className="num">{nf(d.qty)}</td>
                    <td className="num">{nf(d.mc)}</td>
                    <td className={'num' + (d.setup > 0 ? ' setup' : ' zero')}>{d.setup > 0 ? `🔧 ${nf(d.setup)}` : '–'}</td>
                    <td className={'num' + (d.carry > 0 ? '' : ' zero')}>{d.carry > 0 ? nf(d.carry) : '–'}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td colSpan={2}>รวม</td><td className="num">{nf(bkQty)}</td><td className="num">{nf(bkMc)}</td>
                  <td className="num">{nf(bkSetup)}</td><td className="num">{nf(bkCarry)}</td>
                </tr>
              </tfoot>
            </table>
          )}
        </div>

        <div className="jobsec">
          <div className="hint small" style={{ padding: 0 }}>
            {nOver > 0 && planInOver === 0
              ? <>เกิน <b>{nf(nOver)} เครื่อง</b> มาจาก <b>booking ทั้งหมด</b> — ย้ายงานบน Gantt ไม่ช่วย
                ต้องไปแก้ที่ booking (จ้างทอ/เลื่อนงานเดิม) หรือแก้จำนวนเครื่องที่ MC_Total ของกลุ่มนี้</>
              : nOver > 0
                ? <>ต้องย้ายงานของแผนออกไปสัปดาห์อื่น (หรือลดจำนวน) รวมประมาณ <b>{nf(nOver)} เครื่อง</b> ช่องนี้ถึงจะพอดี —
                  งาน 📋 booking ย้ายไม่ได้ และ 📦 = นั่งบนเครื่องที่วิ่งอยู่แล้ว ย้ายออกก็ไม่คืนเครื่องให้พูล</>
                : <>ตัวเลข "ใช้เครื่อง" ของแผน = ACTUAL_MC − เครื่อง booking ของ item นั้น (📦 = นั่งบนเครื่องที่วิ่งอยู่แล้ว ไม่กินเครื่องเพิ่ม)</>}
          </div>
        </div>
      </div>
    </aside>
  )
}

/**
 * Panel รายละเอียดงาน 1 แถว — โชว์ทุกคอลัมน์ของชีท PLAN (จัดกลุ่มตาม PANEL_GROUPS
 * + ที่เหลือรวมใน "คอลัมน์อื่นๆ") เพื่อให้ตัดสินใจได้บนหน้าเว็บโดยไม่ต้องเปิด Excel
 */
function JobPanel({ row, columns, colIdx, idx, weeks = [], isLocked = () => false, onSplit, onRemove, showOther, onToggleOther, onClose, progTeam = '' }) {
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
          <b className={'jobpanel-item' + (progTeam ? ' program' : '')}>{v('ITEM_CODE')}</b>
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
          <span className="jobflag fold" title="ยอดรวมทั้ง SC แบ่งพับแล้วไม่เป็นพับคู่ — ต้องแก้ที่ยอดเปิด order">
            ⚠ order {v('FOLD_QTY')} พับ — ไม่เป็นพับคู่ (เหลือเศษ {v('FOLD_REMAINDER')} พับ)
          </span>
        )}
        {v('IS_CORE_ITEM') && <span className="jobflag core">★ {v('IS_CORE_ITEM')}</span>}
        {progTeam && (
          <span className="jobflag program" title="item + ทีมนี้ ตรงกับชีท Program ใน MasterMC">
            🏷 Program • ทีม {progTeam}
          </span>
        )}
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

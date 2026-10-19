import React, { useMemo, useState, useRef, useLayoutEffect, useEffect, useSyncExternalStore } from 'react'
import { norm } from './ColumnFilter.jsx'
import { PLAN_HIDDEN_COLS } from '../planColumns.js'
import { useYarnLocks, yarnLockKey } from '../useYarnLocks.js'
import {
  CUR_YEAR, currentPlanWeek, wkNum, wkPastYear, wkFarYear, wkTagYear, weekKey, wkLabel,
  isNextYearWeek, weekSortKey, keyYear,
} from '../weekKeys.js'
// ตัวช่วยคีย์สัปดาห์ย้ายไปอยู่ weekKeys.js (ใช้ร่วมกับ KnitPlan/mcRecalc) — export ต่อไว้ให้ผู้เรียกเดิม
export { wkNum, wkPastYear, wkFarYear, weekKey, wkLabel, isNextYearWeek, weekSortKey, keyYear }

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
// สีบล็อกแบบ "ให้สีแล้ว / ยังไม่ให้สี" (ใช้แทนสีตาม CAT+เกจ เมื่อหน้าแผนส่ง dyeCodes มา)
// ให้สีแล้ว = item ที่มี LOAD_DYE ในไฟล์ Order Color (ลูกค้าสั่งย้อมแล้ว)
// export ให้หน้าแผน (KnitPlan) ใช้วาด legend สีบล็อกบนแถบเครื่องมือเหนือ Gantt ด้วยสีชุดเดียวกัน
export const DYE_ON = '#C9BFFC'    // ให้สีแล้ว (ม่วงอ่อน)
export const DYE_OFF = '#41026B'   // ยังไม่ให้สี
// โหมดนี้ไม่มีขอบบล็อก — ทาสีพื้นล้วน ; ยังตั้ง borderColor เป็น transparent (ไม่ใช่ border: none)
// เพราะ .gbar กันที่ไว้ให้ขอบ 2px อยู่แล้ว ถ้าถอดขอบทิ้งจริง บล็อกจะกว้าง/สูงไม่เท่าโหมดอื่น
export const DYE_EDGE = 'transparent'
// โหมด "SC โครงร่าง" (prop scOutline) — ก้อน Forecast (ยังไม่ให้สี) เอาสีพื้นออก เหลือแค่กรอบ
// สีเดิมของมัน (DYE_OFF) ย้ายไปอยู่ที่กรอบ ; ตัวหนังสือต้องเป็นสีเข้มเพราะพื้นเป็นขาว
// ใช้ร่วมกับ legend ในหน้าแผน (KnitPlan) ให้ชิปตัวอย่างหน้าตาตรงกับบล็อกจริง
export const SC_OUTLINE_BG = '#ffffff'
export const SC_OUTLINE_EDGE = DYE_OFF
export const SC_OUTLINE_INK = '#1f2430'
// งานจ้างทอ (Gantt ตัวที่สอง) = น้ำเงินทั้งตาราง — ไม่ไล่สีตาม CAT/เกจ และไม่ใช้โทนสีย้อม
// เพื่อให้กวาดตาแล้วรู้ทันทีว่าก้อนไหนอยู่ข้างนอกโรงงาน
export const S9_BAR = '#297AA0'
// หัวแถว Category ของงานจ้างทอ = "S9-<พูล>" — ขึ้นต้นด้วย S9- เพื่อไม่ให้ชนกับ CAT ของเครื่อง
// ในบ้าน (DOUBLE-30 ฯลฯ) ทั้งตอนจัดกลุ่มแถวและตอนหาเครื่องว่าง (คีย์ ava = "CAT|เกจ")
export const S9_CAT_PREFIX = 'S9-'
// ตัวคั่นชื่อพูลกับเกจในหัวแถว "เครื่อง" ของงานจ้างทอ ("RAP·22") — พูลเดียวกันมีหลายเกจ และ
// ตอนลาก Gantt ส่งกลับมาแค่ค่าคอลัมน์ Machine จึงต้องผูกเกจไว้ด้วย ; ใช้ · เพราะไม่มีในชื่อจริง
export const S9_MC_SEP = '·'
// ชื่อพูลผู้รับจ้างของแถวหนึ่ง ("RAP·22") จากคอลัมน์ S9_MC/S9_GAUGE — '' ถ้าไม่ใช่งานจ้างทอ
// S9_GAUGE ในไฟล์เป็น float ("22.0") จึงต้องปัดก่อน ไม่งั้นป้ายบนบล็อกเขียน "RAP·22.0"
const s9PoolText = (v) => {
  const mc = String(v('S9_MC') ?? '').trim()
  if (!mc || mc.toUpperCase() === 'COMKN') return mc
  const g = String(v('S9_GAUGE') ?? '').trim()
  const n = Number(g)
  return g === '' ? mc : mc + S9_MC_SEP + (Number.isFinite(n) ? String(Math.round(n)) : g)
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
// ── หน้าตาบล็อกงาน: พื้นไล่เฉดจากสีเดิม + ขอบสีเดียวกันแต่เข้มกว่า ─────────────
// พื้นยังเป็น "สีประจำกลุ่ม" เหมือนเดิม แค่ไล่เฉดบน→ล่าง: บนอ่อนลง ล่างเข้มขึ้นเล็กน้อย
// (ไม่ใช่ทำเป็นพาสเทล — ยังอ่านเป็นสีเดิมของกลุ่มนั้นอยู่)
// ล่างต้องเข้มขึ้นด้วย ไม่งั้นสีที่อ่อนอยู่แล้ว (เช่น DYE_ON) จะไล่จนแทบไม่เห็นความต่าง
const barFill = base =>
  `linear-gradient(180deg, ${_lerpHex(base, '#ffffff', 0.24)}, ${_lerpHex(base, '#000000', 0.10)})`
// ขอบ = สีเดียวกับพื้นแต่เข้มกว่า → เห็นเป็นกรอบชัดบนทุกสี
const barEdge = base => _lerpHex(base, '#000000', 0.38)
// ตัวหนังสือ = ขาว/เข้ม ตามความสว่างของ "จุดกึ่งกลาง" ของเฉด (ไม่ใช่ขอบบนที่อ่อนสุด)
const barInk = base => readableText(_lerpHex(base, '#ffffff', 0.07))

// จับกลุ่มแบบ prefix เพื่อรองรับการสะกดต่างกัน (SINGLE / SINGEL / SINGLE TUBE, DOUBLE ฯลฯ)
const catGroupOf = key => { const u = String(key).toUpperCase(); return u.includes('DOUB') ? 'DOUBLE' : u.includes('SING') ? 'SINGLE' : null }

// คอลัมน์ที่ใช้เป็น "หัวแถว" ฝั่งซ้าย (เรียงซ้าย→ขวา) + ป้ายภาษาไทย + ความกว้าง(px)
const GROUP_DEF = [
  // CAT ยาวสุด 9 ตัว ("SINGLE-32"/"SINGEL-26") ตัวหนา 12px — 92px ไม่พอ ตัวท้ายจะถูกพื้นทึบ
  // ของคอลัมน์ Guage (sticky z-index เท่ากัน วาดทีหลัง) ทับบัง อ่านได้แค่ "SINGLE-3"
  { col: 'CAT', label: 'Category', width: 110 },
  { col: 'MC_GUAGE', label: 'Guage', width: 64 },
  { col: 'MC_GROUP', label: 'Machine', width: 96 },
]

// หัวแถวเพิ่ม "Item" — ใส่ทุกชีทที่มีคอลัมน์ ITEM_CODE (1 แถว = เครื่อง + item)
// เห็นได้ทันทีว่าแต่ละเครื่องทอ item อะไร โดยไม่ต้องไล่อ่านชื่อบนบล็อกทีละก้อน
// การลากยังส่งแค่ (สัปดาห์, MC_GROUP ปลายทาง) → หย่อนข้ามแถว item ได้ แต่บล็อกจะไปอยู่
// แถว item ของตัวเองในเครื่องปลายทาง (ไม่ได้เปลี่ยน item)
const ITEM_GROUP = { col: 'ITEM_CODE', label: 'Item', width: 130 }
// ชีทที่ "สีบล็อก" ใช้ item เป็นตัวแยก (ไม่ใช่ CAT+เกจ) — ดูประวัติ setup ของ item เดียวไล่ซ้าย→ขวา
const ITEM_COLOR_SHEETS = new Set(['SETUP_TRACKING'])

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

// อาร์เรย์ว่างค่าคงที่ — ช่องที่ไม่มีบล็อกใช้ตัวนี้ร่วมกัน (ไม่สร้าง [] ใหม่ทุกเซลล์ทุกเรนเดอร์)
const EMPTY_LAID = []

// ── ขนาดที่ใช้คำนวณตำแหน่งแถวล่วงหน้า (virtualization) ───────────────────────
// ⚠️ ต้องตรงกับ styles.css เสมอ — แก้ที่นั่นแล้วต้องแก้ที่นี่ด้วย ไม่งั้นแถวจะเลื่อนไม่ตรง
//   SLOT_H      = .gbar / .gbar-gap : height 30 + margin-bottom 3
//   CELL_PAD    = .gantt-cell : padding-top 15 + padding-bottom 3 + เส้นขอบล่าง 1
//   EMPTY_ROW_H = แถวที่ไม่มีบล็อกเลย — ความสูงมาจากหัวแถวฝั่งซ้าย
// (ยืนยันด้วยการวัดจริงในเบราว์เซอร์ 349 แถว: 0 slot = 30px, n slot = 19 + n×33 ตรงทุกแถว)
const SLOT_H = 33
const CELL_PAD = 19
const EMPTY_ROW_H = 30
// เริ่ม virtualize เมื่อแถวเยอะกว่านี้ — ชีท PLAN ปกติ ~35 แถว เรนเดอร์ครบเหมือนเดิม
// (ต่ำกว่านี้ไม่คุ้มกับการต้องวัด/ตรึงความกว้างคอลัมน์)
const VIRT_MIN_ROWS = 60
// ── ความกว้างคอลัมน์ที่วัดไว้แล้ว (เก็บข้ามการเปิดหน้า) ─────────────────────────
// การวัดต้องเรนเดอร์ตารางครบทุกแถว 1 รอบ ซึ่งค้างราว 2 วินาทีบนชีทใหญ่
// → จำค่าไว้ เปิดหน้า/สลับชีทครั้งถัดไปจะ virtualize ได้ทันทีโดยไม่ต้องวัดซ้ำ
// (localStorage ใช้ไม่ได้ในบางบริบท เช่น โหมดส่วนตัว → ห่อ try/catch แล้วถอยไปวัดใหม่)
const COLW_KEY = 'knitplan.gantt.colw.v1'
const COLW_MAX = 12          // เก็บกี่ชุด (ชีท × ชุดสัปดาห์ × ชิปที่ติ๊ก) ก่อนทิ้งของเก่า
function loadColW(key, nGroup, nWeek) {
  try {
    const v = JSON.parse(localStorage.getItem(COLW_KEY) || '{}')[key]
    // จำนวนคอลัมน์ต้องตรงเป๊ะ ไม่งั้น <colgroup> เพี้ยนทั้งตาราง
    // ⚠️ มีคอลัมน์กว้าง 0 = เคยวัดตอนหน้าถูกซ่อน (display:none) → ค่าเสีย ห้ามเอามาตรึง
    //    ไม่งั้นทั้งตารางกว้าง 0 ตัวหนังสือกองทับกันชิดซ้าย และติดค้างข้ามการรีเฟรช
    if (v && Array.isArray(v.group) && Array.isArray(v.week)
      && v.group.length === nGroup && v.week.length === nWeek
      && [...v.group, ...v.week].every(w => Number(w) > 0)) return v
  } catch { /* อ่านไม่ได้ = ถือว่ายังไม่เคยวัด */ }
  return null
}
function saveColW(key, val) {
  try {
    const all = JSON.parse(localStorage.getItem(COLW_KEY) || '{}')
    all[key] = val
    const ks = Object.keys(all)
    for (let i = 0; i < ks.length - COLW_MAX; i++) delete all[ks[i]]
    localStorage.setItem(COLW_KEY, JSON.stringify(all))
  } catch { /* เขียนไม่ได้ก็แค่ต้องวัดใหม่รอบหน้า */ }
}

// เรนเดอร์เผื่อบน/ล่างนอกจอกี่แถว — ยิ่งเผื่อมาก ยิ่งวาดแถวใหม่ห่างกัน (เลื่อนลื่นขึ้น)
// แลกกับ DOM ที่ใหญ่ขึ้น ; 24 แถว ≈ เลื่อนได้ ~1,000px กว่าจะต้องวาดใหม่รอบหนึ่ง
const OVERSCAN = 24
// เหลือเผื่อไม่ถึงกี่แถวจึงคิดช่วงใหม่ — กันวาดใหม่ทุกเฟรมตอนเลื่อนช้า ๆ
const EDGE = 4

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
 *   dyeCodes : Set<ITEM_CODE>|null  — รหัส item ที่ "ให้สีแล้ว" (มียอดเปิดสี OPEN_COLOR_LOSS)
 *                                     มีค่า = ระบายสีบล็อกแค่ 2 แบบ ให้สีแล้ว/ยังไม่ให้สี
 *                                     null  = ใช้สีตาม CAT+เกจ แบบเดิม (หน้า Order Color)
 *                                     ⚠️ ใช้เป็น fallback เท่านั้น — ถ้าชีทมีคอลัมน์ COLOR_PART
 *                                        จะตัดสิน "งานสี" รายบล็อกจากธงนั้นแทน (ดู hasColorPart)
 *   markLate : boolean              — true = บล็อกที่ PLAN_WEEK เลย TARGET_KNIT ทำคาดทแยงแดง
 *                                     + ป้าย "Over TG W.." (เห็นแม้ไม่ติ๊ก chip RDD)
 *                                     ใช้เฉพาะหน้าแผน (KnitPlan) ที่ชีทมีคอลัมน์ TARGET_KNIT
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
//    (MC_SHARED) ที่ทอไอเทมนี้อยู่แล้ว → ไม่ใช้เครื่องเพิ่ม ไม่ต้อง setup
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
    key: 'core', cls: 'lg-core', sample: '', pre: 'Text สีเขียวมะนาว', label: ' = Item Core Greige',
    tip: 'ชื่อ item เป็นสีเขียวมะนาวหนา = Item Core Greige (IS_CORE_ITEM) — ผลิตประจำ ควรรักษาเครื่องไว้',
  },
  {
    key: 'program', cls: 'lg-program', sample: '', pre: 'Text สีส้ม', label: ' = Item Program',
    tip: 'ชื่อ item เป็นสีส้มหนา = Item Program — ITEM_CODE และ TEAM ของแถวนี้ ตรงกับที่กำหนดไว้ในชีท Program (MasterMC)\n'
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
    key: 'rdd', cls: 'lg-rdd', sample: 'RDD Wxx', label: 'เกิน RDD Week',
    tip: 'ป้าย RDD สีแดง = PLAN_WEEK เลย TARGET_KNIT (RDD หักเวลางานหลังทอแล้ว) → ทอไม่ทันกำหนดส่ง\n'
      + 'ต้องลากขึ้นสัปดาห์ก่อนหน้า หรือแบ่งงาน/จ้างทอ',
  },
  {
    key: 'late', cls: 'lg-late', sample: '⚠ Over TG Wxx', label: 'เกิน Target Knit',
    tip: 'ป้ายนี้บนบล็อก = PLAN_WEEK เลย TARGET_KNIT → ทอเสร็จไม่ทันกำหนด\n'
      + 'โชว์ทุกบล็อกที่ไม่ทันเสมอ ไม่ต้องติ๊ก chip RDD — เลขบนป้าย = สัปดาห์ที่ต้องทอเสร็จ\n'
      + 'ต้องลากขึ้นสัปดาห์ก่อนหน้า หรือแบ่งงาน/จ้างทอ',
  },
  {
    key: 'booking', cls: 'lg-booking', sample: '📋', label: 'ประวัติการวางแผน',
    tip: 'บล็อกที่มีไอคอน 📋 = แผนเดิมย้อนหลังสูงสุด 5 สัปดาห์ ไว้เทียบว่าเครื่องเดิมทออะไรอยู่\n'
      + 'ไม่ใช่งานของแผนรอบนี้ — ลาก/แก้/หักเครื่องว่างไม่ได้',
  },
  {
    key: 'planlock', cls: 'lg-planlock', sample: '🔐', label: 'ล็อกแผนไว้ (ลายคาดเหลือง)',
    tip: 'บล็อกลายคาดเหลือง + 🔐 = งานที่ล็อกแผนไว้ — Replan รอบถัดไปจะคงไว้สัปดาห์/เครื่องเดิม\n'
      + 'กดล็อกเอง หรือระบบล็อกให้อัตโนมัติเพราะเป็น "ตั๋วโปรแกรม" (item อยู่ในชีท Program) หรือ core item\n'
      + 'ล็อกอัตโนมัติที่ปลดแล้ว ระบบจะจดสัปดาห์นั้นไว้ (ชีท Program_Unlock / Core_Unlock) แล้วไม่ล็อกกลับให้อีก\n'
      + 'กดที่บล็อกเพื่อล็อก/ปลดล็อก ; รายการล็อกเก็บในชีท Lock_Plan (MasterMC) แก้มือได้จากหน้า Master\n'
      + 'คนละเรื่องกับ 🔒 ซึ่งหมายถึงสัปดาห์ freeze / เครื่องที่กันไว้',
  },
]

// ข้อความอธิบายล็อกหนึ่งอัน (ใช้ทั้ง tooltip ของบล็อก booking และบล็อกแผน)
// แยกข้อความล็อกอัตโนมัติออกมา เพราะ "ปลด" มันมีผลต่างจากล็อกที่ user กดเอง —
// ระบบจะจดสัปดาห์ที่ปลดไว้แล้วไม่ล็อกกลับให้อีก (ดู Program_Unlock / Core_Unlock ใน program_lock.py)
export function lockTip(l) {
  if (!l) return ''
  const who = l.by && !l.auto ? ` (โดย ${l.by}${l.at ? ` ${l.at}` : ''})` : ''
  return l.auto
    ? `\n🔐 ล็อกอัตโนมัติ: ${autoLockInfo(l).label} — Replan จะไม่ย้ายงานนี้ (ปลดแล้วจะไม่ล็อกกลับให้อีก)`
    : `\n🔐 ล็อกแผนไว้ — Replan จะไม่ย้ายงานนี้${who}`
}

// ล็อกอัตโนมัติแต่ละชนิด → ชื่อที่โชว์ + ชีทที่จดสัปดาห์ที่ปลด (auto_kind มาจาก plan_lock.list_locks)
export function autoLockInfo(l) {
  return l?.auto_kind === 'core'
    ? { label: 'core item', sheet: 'Core_Unlock' }
    : { label: 'ตั๋วโปรแกรม', sheet: 'Program_Unlock' }
}

// คีย์ล็อกแผน — ต้อง normalize ให้ตรงกับ key_of() ใน plan_lock.py
//   'WEEK|ITEM|MC_GROUP|GUAGE|SOURCE'
// อยู่ระดับโมดูล + export เพราะ KnitPlan ต้องใช้สูตรเดียวกันตอนย้ายล็อกตามงานที่ลาก
// ถ้าแยกเขียนสองที่แล้วสูตรเพี้ยนกัน ล็อกจะ "หาย" ทันทีที่ลาก
export function lockKeyOf(week, item, mcGroup, gauge, source = 'BOOKING') {
  const g = Number(gauge)
  return [
    String(week ?? '').trim(),
    String(item ?? '').trim().toUpperCase(),
    String(mcGroup ?? '').trim().toUpperCase(),
    Number.isFinite(g) ? String(Math.trunc(g)) : String(gauge ?? '').trim(),
    String(source || 'BOOKING').trim().toUpperCase(),
  ].join('|')
}

// ชิปเครื่องว่างมุมขวาบนของแต่ละช่อง (คลาส .cellava) — เขียว/เหลือง/แดง
const AVA_MARKS = [
  {
    key: 'free', cls: '', sample: 'ว่าง 3', label: 'เครื่องว่าง',
    tip: 'เครื่องว่าง live = เครื่องทั้งหมด − booking − ที่แผนจองเพิ่มในสัปดาห์นั้น',
  },
  {
    key: 'full', cls: 'none', sample: 'เต็ม', label: 'เครื่องเต็ม',
    tip: 'เครื่องถูกใช้หมดพอดี — วางงานเพิ่มในช่องนี้จะเกินทันที',
  },
  {
    key: 'over', cls: 'over', sample: 'ไม่ว่าง (เกิน 2)', label: 'วางเกินเครื่องที่มี',
    tip: 'วางงานเกินจำนวนเครื่องที่มีจริง — ต้องย้ายงานออกไปสัปดาห์อื่น หรือลดจำนวน\n'
      + '(เลขนี้ไม่รวมเครื่องที่ Lock MC กันไว้ — ส่วนนั้นแยกไปอยู่ป้าย "Lock MC")',
  },
  {
    key: 'lock', cls: 'lockonly', sample: 'Lock MC 11 SKP', label: 'เครื่องถูกกันไว้ (Lock MC)',
    tip: 'เครื่องที่กันไว้ในชีท Lock_MC ของ MasterMC — แผนเอาไปใช้ไม่ได้ (ไม่ใช่การวางงานเกิน)\n'
      + 'ชื่อท้ายเลข = กลุ่มเครื่องที่เขียนไว้ในคอลัมน์ MC\n'
      + '⚠️ ล็อกคิดที่ระดับ CAT+Guage จึงหักกับทุกกลุ่มเครื่องของ Guage นั้น รวมกลุ่มที่ไม่ได้ระบุชื่อไว้\n'
      + 'ช่องที่ยังว่าง/เต็ม/เกินจริงจะเห็นป้าย "Lock MC" ต่อท้ายเลขแทน\n'
      + '(ไอคอน 🔒 เป็นของเครื่องที่กันไว้ให้ POLY/COTTON คนละเรื่องกัน)',
  },
  {
    key: 'rsv', cls: 'lg-rsv', sample: 'PX,CX', label: 'จำนวนเครื่องที่กัน(POLY/COTTON)',
    tip: 'เครื่องที่กันไว้ให้งาน POLY (P) / COTTON (C) โดยเฉพาะ — ใช้แทนงานปกติไม่ได้\n'
      + 'เลข = กันไว้ − booking ที่ใช้ไปแล้ว − ที่แผนใช้ในสัปดาห์นั้น (แดง = วางเกิน)',
  },
]

// จำนวนสัปดาห์อดีตที่ Gantt ยอมโชว์ (อ่านอย่างเดียว) — ชีท SETUP_TRACKING/booking มี week ย้อนถึง 24
// นับจากสัปดาห์ปัจจุบัน: 5 → ปัจจุบัน W32 เห็นตั้งแต่ W27 (สัปดาห์อดีตที่ไม่มีข้อมูลเลย เช่น W31 หยุด จะไม่โผล่)
const HISTORY_WEEKS_BACK = 5

// PLAN_SOURCE ของแถวที่ "คงแผนเดิมและห้ามแก้" — ใบที่ออกเลขใบจอง KP แล้ว (Replan.py: KP_PLAN_SOURCE)
// ยืนยันกับฝ่ายผลิตไปแล้ว → ย้ายสัปดาห์/เครื่องไม่ได้ ; แถว core ที่เบรก (CORE_KEEP) ไม่เข้าข่ายนี้
const KEEP_LOCKED_SOURCE = 'KP_KEEP'

// ── คีย์คอลัมน์สัปดาห์ ───────────────────────────────────────────────────────
// ตัวช่วยทั้งชุด (wkNum / weekKey / wkLabel / isNextYearWeek / weekSortKey / currentPlanWeek)
// อยู่ที่ ../weekKeys.js — คอลัมน์ในกรอบ 1 ปีเป็นเลขเปล่าเหมือนเดิม ; ปีก่อน = '45@2025' ;
// ไกลเกิน 1 ปีข้างหน้า = '45@2028' (แผนยาวหลายปีได้แล้ว เลขสัปดาห์จึงชนกันได้)

// ── สัปดาห์ที่แก้แผนไม่ได้ (freeze) ──────────────────────────────────────────
// จำนวนสัปดาห์มาจากหน้า "ตั้งค่าระบบ" (KP_FREEZE_WEEKS) = ค่าเดียวกับพื้น TODAY_IDX+FREEZE_WEEKS
// ที่ Planning ใช้เป็นสัปดาห์แรกที่วางงานได้ ; ตัวเลขข้างล่างเป็นแค่ค่าเริ่มต้นก่อนโหลดค่าจริง
// ถ้าปล่อยเป็นค่าคงที่ ผู้ใช้เปลี่ยนค่าแล้วจะลากงานเข้าสัปดาห์ที่ engine ไม่ยอมวาง
// แล้วรอบถัดไปงานถูกเลื่อนเอง โดยบนจอไม่มีอะไรบอก
let FREEZE_WEEKS = 2
// คอมโพเนนต์ที่ต้องวาดใหม่เมื่อค่าเปลี่ยน (ผ่าน useFreezeWeeks) — แค่เปลี่ยนตัวแปรธรรมดา React ไม่รู้
// แกนสัปดาห์กับแถว Gantt เป็น memo จึงค้างค่าเก่า: แก้ค่าที่หน้า Setting แล้วกลับมาหน้าแผน
// (หน้าแผนไม่ได้ปิด แค่ถูกซ่อน) ช่องสัปดาห์ที่เพิ่งเข้าช่วง freeze ยังรับการวางงานอยู่
const freezeSubs = new Set()

/** ตั้งจำนวนสัปดาห์ freeze จาก /api/plan-config (KnitPlan เรียกตอนโหลดและตอนมีคนบันทึกค่าใหม่) */
export function setFreezeWeeks(v) {
  // รับเฉพาะตัวเลข/ข้อความตัวเลขที่เป็นจำนวนเต็ม >= 0 (ชนิด int เดียวกับ KP_FREEZE_WEEKS ของ engine)
  // null / '' / true / [] แปลงด้วย Number() ได้ 0 หรือ 1 แต่ไม่ใช่ค่าที่ตั้งไว้จริง → ไม่รับ
  if (!(typeof v === 'number' || (typeof v === 'string' && v.trim() !== ''))) return
  const n = Number(v)
  if (!Number.isInteger(n) || n < 0 || n === FREEZE_WEEKS) return
  FREEZE_WEEKS = n
  for (const fn of [...freezeSubs]) fn()
}

/** ค่าที่ใช้อยู่จริง — ให้หน้าอื่น/เทสต์อ่านได้ */
export const freezeWeeks = () => FREEZE_WEEKS

function subscribeFreeze(fn) {
  freezeSubs.add(fn)
  return () => { freezeSubs.delete(fn) }
}

/** จำนวนสัปดาห์ freeze + วาดคอมโพเนนต์ใหม่ทันทีที่มีคนตั้งค่าใหม่ (ใช้ใน deps ของ memo ที่ขึ้นกับค่านี้) */
export function useFreezeWeeks() {
  return useSyncExternalStore(subscribeFreeze, freezeWeeks)
}

// สัปดาห์ freeze ตามปฏิทิน = แก้แผนไม่ได้ (สัปดาห์ปัจจุบัน ถึง ปัจจุบัน+FREEZE_WEEKS−1)
// เกณฑ์เดียวกับ isLocked ของ Gantt (ส่วนที่ไม่ขึ้นกับ lockBefore/ชีท) — export ให้ตารางเทียบแผน
// ใน KnitPlan ลงลายคาดคอลัมน์เดียวกันได้ ; W99 = sentinel งานล้น ไม่ใช่สัปดาห์จริง
// nextYear = คำตอบที่ผู้เรียก "รู้จริง" จากคอลัมน์ปี (PLAN_YEAR) — ใส่มาแล้วไม่ต้องเดาจากระยะ
export function isFrozenWeek(w, nextYear) {
  if (wkFarYear(w) != null) return false          // ไกลเกิน 1 ปี = อนาคตแน่นอน
  if (wkPastYear(w) != null) return true          // ปีก่อน = ผ่านไปแล้ว
  const n = Number(w)
  if (!Number.isFinite(n) || n === 99) return false
  const cur = currentPlanWeek()
  if (nextYear ?? isNextYearWeek(n, cur)) return false
  return n < cur + FREEZE_WEEKS
}

// จำแนกแถว PLAN เป็นประเภท OM / PHET_DOUBLE / PHET_SINGLE (null = ไม่นับ เช่น OUTSOURCE)
// export ให้ KnitPlan ใช้เช็คโควตา job setup ตอนลากงาน — ต้องเป็นตรรกะเดียวกับแถบโหลดบน Gantt
export function classifyType(factory, cat) {
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
  // สัปดาห์ที่ด้ายเข้าครบ (PO IN) — Planning.py คิดมาจาก "ล็อตที่ไกลสุด" ของงานนั้นแล้ว
  // (ด้ายหลายล็อต = ทอได้ต่อเมื่อล็อตสุดท้ายมาถึง) ; ⚠ = ด้ายเข้าหลังสัปดาห์ที่วางงานไว้ = ยังทอไม่ได้
  {
    key: 'poin', label: 'POIN DATE (สัปดาห์)', cols: ['PO_IN_WEEK'],
    build: v => {
      const w = Number(v('PO_IN_WEEK'))
      if (!(w > 0)) return ''
      const cur = Number(v('PLAN_WEEK'))
      return `POIN W${w}${cur > 0 && w > cur ? ' ⚠' : ''}`
    },
  },
  { key: 'setup', label: 'setup (วัน)', cols: ['SETUP_DAYS'], build: v => { const d = v('SETUP_DAYS'); return Number(d) > 0 ? `setup ${d} ว.` : '' } },
  { key: 'customer', label: 'ลูกค้า', cols: ['CUSTOMER'], build: v => v('CUSTOMER') },
  { key: 'left', label: 'คงเหลือ', cols: ['PLAN_QTY'], build: v => { const q = v('PLAN_QTY'); return q !== '' && Number(q) > 0 ? `เหลือ ${q}` : '' } },
  { key: 'color', label: 'สี', cols: ['COLOR_DESC', 'NAY_COLOR'], build: v => v('COLOR_DESC') || v('NAY_COLOR') },
  { key: 'material', label: 'เนื้อผ้า', cols: ['MATERIAL_CONTENT'], build: v => v('MATERIAL_CONTENT') },
  // ── เฉพาะชีท SETUP_TRACKING ──
  {
    // ชีท REPLAN: ทุกแถวเป็น PLAN_SOURCE=NEW เหมือนกันหมด → บอกไม่ได้ว่าเป็นงาน booking ที่ถูกยกมาวางใหม่
    // BOOKING_WEEK_FROM/TO (Replan.py → Planning.py) = สัปดาห์เดิมที่ booking จองไว้ → บอกว่า Move มาจาก W ไหน
    key: 'source', label: 'ที่มา (booking/แผนใหม่)', cols: ['PLAN_SOURCE', 'BOOKING_WEEK_FROM'],
    build: v => {
      const from = Number(v('BOOKING_WEEK_FROM'))
      if (from > 0) {
        const to = Number(v('BOOKING_WEEK_TO')) || from
        const wk = to > from ? `W${from}-${to}` : `W${from}`
        // ตรงกับสัปดาห์ที่วางอยู่ = แผนใหม่ให้ที่เดิม ยังไม่ได้ขยับ → เขียน Move จะเข้าใจผิด
        const cur = Number(v('PLAN_WEEK'))
        return (cur === from && to === from) ? `booking ${wk} (เดิม)` : `Move ${wk}`
      }
      const s = v('PLAN_SOURCE'); return s === 'OLD' ? 'booking' : s === 'NEW' ? 'แผนใหม่' : s
    },
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

// ── ยุบบล็อกของผ้าตัวเดียวกันในช่องเดียวกัน (ต่างกันแค่ใบสั่ง) ─────────────────
// ดูเหตุผลเต็มที่คอมเมนต์หัว memo `cells` — ที่นี่เป็นแค่กลไกรวมค่า
// คืน list ใหม่ที่เรียงตามลำดับเดิม (ก้อนที่ยุบอยู่ตำแหน่งของใบแรกที่เจอ)
function mergeJobs(list) {
  const g = new Map()
  for (const j of list) {
    const k = j.gkey
    if (!g.has(k)) g.set(k, [])
    g.get(k).push(j)
  }
  const out = []
  for (const subs of g.values()) out.push(subs.length === 1 ? subs[0] : foldJobs(subs))
  return out
}
// ลำดับความสำคัญของ emoji ประเภทเครื่อง เวลารวมหลายใบ — ใบไหน setup เครื่องใหม่ ทั้งก้อนถือว่า setup
const KIND_RANK = ['outsource', 'setup', 'onExisting', 'carry']
// เลขเครื่องของก้อนที่ยุบ = max ภายในใบสั่งเดียวกัน แล้วบวกข้ามใบสั่ง
//   · หลายแถวของ SC เดียวกัน = เครื่องชุดเดียวที่ Planning เขียนค่าซ้ำทุกแถว → max
//   · คนละ SC = คนละชุดเครื่องจริง → บวก
// ต้องตรงกับ planMcAgg ในไฟล์นี้ และ _pool_blocks_from_plan() ใน Planning.py เป๊ะ ๆ
// ไม่งั้นเลขบนบล็อกกับชิป "ว่าง/เต็ม" ของช่องเดียวกันจะขัดกันเอง
function sumBySo(subs, field) {
  const bySo = new Map()
  for (const s of subs) {
    const k = s.so || ''
    bySo.set(k, Math.max(bySo.get(k) || 0, Number(s[field]) || 0))
  }
  let n = 0
  for (const v of bySo.values()) n += v
  return n
}
function foldJobs(subs) {
  const numOf = (s, f) => Number(s[f]) || 0
  const maxOf = (f) => subs.reduce((a, s) => Math.max(a, numOf(s, f)), 0)
  // แถวหลัก = แถวที่ถือเลขเครื่องมากสุด — ใช้เป็นตัวแทนก้อนตอนคลิกเปิดการ์ด / ล็อก
  // และอ่านค่าที่เหมือนกันทุกใบ (เครื่อง/เกจ/CAT/ทีม)
  let p = subs[0]
  for (const s of subs) if (numOf(s, 'actualmc') > numOf(p, 'actualmc')) p = s
  const mcTotal = sumBySo(subs, 'actualmc')
  const qtySum = subs.reduce((a, s) => a + (Number(s.qty) || 0), 0)
  const hasQty = subs.some(s => s.qty !== '')
  const late = subs.filter(s => s.lateRdd)
  const lateTk = late.map(s => Number(s.tk) || 0).filter(x => x > 0)
  const fold = subs.find(s => s.foldWarn)
  const kind = KIND_RANK.find(k => subs.some(s => s.mc === k)) || ''
  const stSetup = sumBySo(subs, 'stSetup'), stCarry = sumBySo(subs, 'stCarry')
  return {
    ...p,
    subs,                                   // ใบสั่งทั้งหมดในก้อน (เรียงตามที่เจอในชีท)
    tags: foldTags(subs, mcTotal),
    qty: hasQty ? String(Math.round(qtySum * 100) / 100) : '',
    actualmc: mcTotal > 0 ? String(mcTotal) : '',
    mc: kind,
    // setup เป็น "จำนวนวัน" ไม่ใช่จำนวนเครื่อง — ตั้งเครื่องขนานกัน จึงใช้ค่ามากสุด ไม่ใช่บวก
    setup: maxOf('setup') > 0 ? String(maxOf('setup')) : '',
    // เดดไลน์ของก้อน = ใบที่เร่งที่สุด (ต้องทอเสร็จเร็วสุด) ; เลยไปกี่สัปดาห์ = ใบที่เลยมากสุด
    lateRdd: late.length > 0,
    tk: lateTk.length ? Math.min(...lateTk) : p.tk,
    tkLate: late.length ? Math.max(...late.map(s => Number(s.tkLate) || 0)) : p.tkLate,
    foldWarn: !!fold, foldQty: fold ? fold.foldQty : '', foldRem: fold ? fold.foldRem : '',
    stSetup, stCarry,
    stMc: p.stMc ? `setup ${stSetup} · carry ${stCarry}` : '',
    stTotal: sumBySo(subs, 'stTotal'),
    core: subs.some(s => s.core),
  }
}
// ป้ายบนบล็อกที่ยุบแล้ว: ค่าตรงกันทุกใบ → โชว์ค่านั้น ; ต่างกัน → "ค่าแรก +N" (hover เห็นครบ)
//   · ป้ายเครื่อง (mc) เขียนทับด้วยเลขเครื่องรวมของก้อน (sumBySo) ไม่ใช่ "+N"
//   · ป้าย SC ตัดทิ้ง — จำนวนใบไปอยู่ที่ปุ่มกาง "▾ N ใบ" ซึ่งบอกเรื่องเดียวกันแต่กดดูรายใบได้
function foldTags(subs, mcTotal) {
  const order = [], seen = new Map()
  for (const s of subs) for (const t of s.tags) {
    if (!seen.has(t.key)) { seen.set(t.key, { key: t.key, vals: [], warn: false }); order.push(t.key) }
    const e = seen.get(t.key)
    if (!e.vals.includes(t.text)) e.vals.push(t.text)
    if (t.warn) e.warn = true
  }
  const out = []
  for (const k of order) {
    if (k === 'sc') continue
    const e = seen.get(k)
    if (k === 'mc') {
      if (mcTotal > 0) out.push({ key: k, text: `${mcTotal} เครื่อง`, warn: e.warn })
      continue
    }
    out.push({
      key: k, warn: e.warn,
      text: e.vals.length > 1 ? `${e.vals[0]} +${e.vals.length - 1}` : e.vals[0],
      tip: e.vals.length > 1 ? e.vals.join('\n') : '',
    })
  }
  return out
}
// ⚠️ บล็อก History (booking) ไม่ต้องยุบซ้ำที่นี่ — backend รวมให้ตั้งแต่ต้นทางแล้ว
// (plan_view.booking_items_by_week คีย์ด้วย (สัปดาห์, CAT, เกจ, เครื่อง, item) และเก็บ SO
//  ทุกใบไว้ในก้อนเดียว) ; ถ้ามายุบซ้ำบนเว็บ เคสที่บล็อกเดียวมาจากสองแหล่ง (booking + ประวัติ)
// จะถูกบวกยอดเป็นสองเท่า

// RDD_WEEK / FG_WEEK เก็บเป็น YYYYWW (เช่น 202632) → คืนเลขสัปดาห์ล้วน (32) ไว้แสดงผล
function rddWeekNo(v) {
  const raw = String(v('RDD_WEEK') || v('FG_WEEK') || '').trim()
  if (!/^\d+$/.test(raw)) return ''
  return raw.length > 2 ? String(Number(raw.slice(-2))) : String(Number(raw))
}
// จำนวนสัปดาห์ ISO ของปีนั้น (52 หรือ 53) — ปีที่ 1 ม.ค. ตรงวันพฤหัส หรือปีอธิกสุรทินที่
// 1 ม.ค. ตรงวันพุธ จะมี 53 สัปดาห์ ; ใช้นับระยะห่างสัปดาห์ข้ามปีให้ตรง ไม่ใช่บวก 52 ตายตัว
export function isoWeeksInYear(y) {
  const jan1 = new Date(Date.UTC(y, 0, 1)).getUTCDay()
  const leap = (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0
  return (jan1 === 4 || (leap && jan1 === 3)) ? 53 : 52
}
// เลขสัปดาห์สะสมจากปีฐาน — ใช้เทียบ/ลบกันข้ามปีได้ (base ต้อง <= ทั้งสองปีที่เทียบ)
function absWeek(y, w, base) {
  let n = Number(w)
  for (let k = base; k < y; k++) n += isoWeeksInYear(k)
  return n
}
// เทียบสัปดาห์แบบรู้ปี: คืนผลต่างเป็นจำนวนสัปดาห์ (บวก = a มาหลัง b)
// ปีไหนไม่รู้ (0/ว่าง) → ตกไปเทียบเลขสัปดาห์ล้วนเหมือนเดิม
function weekDelta(aY, aW, bY, bW) {
  if (!(aY > 0) || !(bY > 0)) return Number(aW) - Number(bW)
  const base = Math.min(aY, bY)
  return absWeek(aY, aW, base) - absWeek(bY, bW, base)
}
// ปีของเดดไลน์ทอ: ใช้ TARGET_YEAR ที่ Planning.py เขียนมาก่อน ; ไฟล์แผนรุ่นเก่าที่ยังไม่มี
// คอลัมน์นี้ → ถอดจาก FG_WEEK (YYYYWW) แทน — TARGET_KNIT = FG หัก 3-4 สัปดาห์ ดังนั้น
// target ที่เลขมากกว่าสัปดาห์ FG แปลว่าถอยข้ามปีมา = ปีก่อนหน้า FG หนึ่งปี
function targetYear(v) {
  const ty = Number(v('TARGET_YEAR'))
  if (ty > 0) return ty
  const raw = String(v('FG_WEEK') || v('RDD_WEEK') || '').trim()
  if (!/^\d{5,}$/.test(raw)) return 0
  const fgY = Number(raw.slice(0, -2)), fgW = Number(raw.slice(-2))
  const t = Number(v('TARGET_KNIT'))
  if (!(fgY > 0) || !(fgW > 0) || !(t > 0)) return 0
  return t <= fgW ? fgY : fgY - 1
}
// เดดไลน์ทอ = TARGET_KNIT (RDD หักเวลางานหลังทอแล้ว) → เทียบกับ PLAN_WEEK
// PLAN_WEEK เลย TARGET_KNIT = ทอไม่ทันกำหนดส่ง
// ⚠ ต้องเทียบเป็นคู่ (ปี, สัปดาห์) — แผนคร่อมปีได้ ถ้าเทียบเลขล้วน งานที่วางไว้ W2 ปีหน้า
// จะดูเหมือน "มาก่อน" target W46 ปีนี้ แล้วหลุดเป็นทันทั้งที่สายไป 8 สัปดาห์
// ⚠ core item ไม่นับว่าสาย — ผลิตเป็นรอบตาม trigger ของสต๊อก ไม่ได้ยึด TARGET_KNIT
// (Planning ก็เว้น TARGET_STATUS ให้ core มาตลอด — ดู "CORE ITEM ไม่ต้องบอกทัน/ไม่ทัน")
// ตัวเลข FG/Target ยังโชว์ในตารางตามปกติ แค่ไม่ทำคาดแดง/ป้ายเตือน
function isLateRdd(v, week) {
  const t = Number(v('TARGET_KNIT'))
  if (!(t > 0) || week === '') return false
  if (String(v('IS_CORE_ITEM') ?? '').trim() !== '') return false
  // week อาจเป็นคีย์คอลัมน์ที่ติดปี ('45@2028') → ใช้เลขสัปดาห์ ; ปีมาจาก PLAN_YEAR ของแถว
  return weekDelta(Number(v('PLAN_YEAR')) || 0, wkNum(week), targetYear(v), t) > 0
}
// สัปดาห์เดดไลน์ทอของแถวนั้น (0 = ไม่มีค่า) — ใช้บอกบนป้าย/tooltip ว่าเลยไปกี่สัปดาห์
function targetKnitWeek(v) {
  const t = Number(v('TARGET_KNIT'))
  return t > 0 ? t : 0
}
// เลยเดดไลน์ไปกี่สัปดาห์ (สำหรับข้อความบนป้าย) — นับข้ามปีถูกต้อง
function lateWeeks(v, week) {
  return Math.max(0, weekDelta(Number(v('PLAN_YEAR')) || 0, wkNum(week), targetYear(v), Number(v('TARGET_KNIT'))))
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

// โหมด "SC โครงร่าง" (ก้อน Forecast เหลือแค่กรอบ) — จำไว้ต่อเครื่อง เหมือนชุดฟิลด์บนบล็อก
const SC_OUTLINE_KEY = 'knitplan.gantt.scOutline.v1'
export function loadScOutline() {
  try { return localStorage.getItem(SC_OUTLINE_KEY) === '1' } catch { return false }
}
export function saveScOutline(on) {
  try { localStorage.setItem(SC_OUTLINE_KEY, on ? '1' : '0') } catch { /* โควตาเต็ม — ข้ามได้ */ }
}

// ─── Panel รายละเอียดงาน (คลิกบล็อก) ────────────────────────────────────
// จัดกลุ่มคอลัมน์ของชีท PLAN ให้อ่านง่าย — คอลัมน์ที่ไม่อยู่ในนี้จะไปรวมที่ท้าย "คอลัมน์อื่นๆ"
// เพื่อให้เห็นข้อมูลครบเท่า Excel โดยไม่ต้องเปิดไฟล์
const PANEL_GROUPS = [
  {
    title: 'งาน', fields: [
      ['ITEM_CODE', 'Item'], ['CAT', 'CAT'], ['MC_GUAGE', 'Guage'], ['MC_GROUP', 'MC_GROUP'],
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
      ['ORDERS_QTY', 'สั่งทั้งหมด'], ['ORDER_PENDING', 'ต้องวางในแผนนี้'],
      ['PRODUCE_QTY', 'ผลิตสัปดาห์นี้'], ['PLAN_QTY', 'คงเหลือหลังสัปดาห์นี้'],
      ['PENDING_PLAN', 'รอวางแผน'], ['DAILY_CAPACITY', 'Capacity/Day'], ['REVOLUTION_WEIGHT', 'Revolution/Weight'],
    ]
  },
  {
    title: 'เครื่อง / setup', fields: [
      ['SETUP_DAYS', 'Setup Days'], ['ACTUAL_MC', 'เครื่องที่ใช้จริง'], ['REQUIRED_MC', 'เครื่องที่ต้องใช้'],
      ['NEW_MC', 'Setup MC'], ['CARRYOVER_MC', 'Continue'], ['ACTUAL_WORKING_DAYS', 'วันทำงานจริง'],
      ['AVAILABLE_DAYS', 'วันที่ใช้ผลิตได้'],
    ]
  },
  {
    title: 'กำหนดส่ง / วัตถุดิบ', fields: [
      ['RDD_WEEK', 'RDD (สัปดาห์)'], ['FG_WEEK', 'FG week'], ['TARGET_KNIT', 'เป้าทอ (สัปดาห์)'],
      ['TARGET_YEAR', 'เป้าทอ (ปี)'],
      ['EARLIEST_PLAN_WEEK', 'เร็วสุดที่ทำได้'], ['LT_YARN', 'LT ด้าย'], ['YARN_USED', 'ด้ายที่ใช้'],
      ['PO_IN_DATE', 'POIN DATE (วัน)'], ['PO_IN_WEEK', 'POIN DATE (สัปดาห์)'],
      // ใบที่ Replan ให้ยืมด้ายในสต๊อก ไม่ต้องรอ PO IN (YarnStockCover.py)
      ['YARN_STOCK', 'ใช้ด้ายสต๊อก'],
      ['DATE_IN', 'Date in'],
    ]
  },
  {
    title: 'หมายเหตุ', wide: true, fields: [
      ['REMARK', 'REMARK'], ['OUTSOURCE', 'จ้างทอ (user สั่ง)'], ['PLAN_SOURCE', 'ที่มาของแผน'],
    ]
  },
]
// คอลัมน์ที่ซ่อน (PLAN_HIDDEN_COLS ชุดเดียวกับตาราง popup ของ item) ต้องนับเป็น "รู้จักแล้ว"
// ไม่งั้นจะไปโผล่ในหัวข้อ "คอลัมน์อื่นๆ" แทน ; ในหมวดก็ถูกข้ามตอนวาดเช่นกัน
const PANEL_KNOWN = new Set([...PANEL_GROUPS.flatMap(g => g.fields.map(f => f[0])), ...PLAN_HIDDEN_COLS])

export default function PlanGantt({ columns, rows, allRows = null, sheet = '', load = {}, setupJobs = [], ava = {}, bookingMc = {}, poolMap = {}, onMoveWeek, colorRows, onRemove, onEditQty, onSplit, lockBefore = null, bookingItems = [], bookingMode = 'off', bookingPick = null, bookingAllWeeks = false, showReplanned = false, historyItems = [], historyFrom = null, allMcRows = [], loadFilter = null, setLoadFilter = () => {}, barFields = BAR_FIELDS_DEFAULT, selIdx = null, setSelIdx = () => {}, programRows = {}, programMap = {}, dyeCodes = null, scOutline = false, markLate = false, planLocks = [], onToggleLock = null, onApplyLocks = null, outsourceIdx = null, onBringHome = null, outsourceChoices = null, onSendOutsource = null, outsourceItems = null, mergedIdx = null, onOpenItem = null, openItem = null }) {
  // outsourceIdx = Set ของ idx แถวที่เป็นงานจ้างทอ (S9) — อยู่ในตารางเดียวกับงานในบ้าน แต่:
  //   • บล็อกเป็นสีเดียวทั้งกลุ่ม (S9_BAR) ไม่ไล่สีตาม CAT/เกจ และไม่เข้าโหมดสีย้อม
  //   • การ์ดงานมีปุ่ม "ดึงกลับมาทอในบ้าน"
  //   เพดานเครื่องของผู้รับจ้างมาทาง ava (ผู้เรียกผสมคีย์ "S9-<พูล>|เกจ" เข้ามาให้แล้ว)
  // mergedIdx = Set ของ idx แถวจ้างทอที่ผู้เรียก "ยุบ" เข้าไปนั่งในแถวเครื่องในบ้านแล้ว
  //   หัวแถวของมันเป็นเครื่องในบ้าน แต่งานจริงทออยู่ข้างนอก → ห้ามเอาไปหักเครื่องของพูลในบ้าน
  //   (แถวจ้างทอที่ยังไม่ถูกยุบยังต้องหักตามปกติ ไม่งั้นชิปเพดานของผู้รับจ้างจะโชว์ว่างเต็มตลอด)
  // outsourceItems = item ที่มีงานทั้งในบ้านและจ้างทอ → ติดป้ายบนบล็อกทั้งสองฝั่ง
  // rows = แถวที่ผ่านตัวกรอง/คำค้นแล้ว → ใช้ "วาดบล็อก" เท่านั้น
  // boardRows = แถวแผนทั้งกระดาน (ไม่สนตัวกรอง) → ใช้กับทุกตัวเลข "ความจุ" คือ โควตา job
  //   ต่อสัปดาห์ และเครื่องที่แผนจองไปแล้ว ; ถ้าคิดจาก rows จะกลายเป็นว่าพอค้นหา item เดียว
  //   แถบ job ว่างและเครื่องเหลือเพียบ ซึ่งตรงข้ามกับสิ่งที่ต้องใช้ตัดสินว่าลากไปแล้วเกินไหม
  //   (ผู้เรียกไม่ส่งมา = ไม่มีตัวกรอง ใช้ rows ได้ตรง ๆ)
  const boardRows = allRows || rows
  // ด้ายที่ฝั่งด้ายล็อกให้ SC + ผ้า (หน้าแผนด้าย) → ป้ายในการ์ดงาน
  const yarnLocks = useYarnLocks()
  // จำนวนสัปดาห์ freeze — เป็นค่าที่ React เห็น (ต้องอยู่ใน deps ของแกนสัปดาห์ + dataVer ของแถว)
  // ไม่งั้นมีคนแก้ค่าระหว่างเปิดหน้าค้าง แกน/ช่องที่วางงานได้ยังเป็นของค่าเดิม
  const freezeW = useFreezeWeeks()
  const [dragIdx, setDragIdx] = useState(null)
  // ── ไฮไลต์คอลัมน์สัปดาห์ตอนลาก ─────────────────────────────────────────────
  // เดิมเก็บเป็น state → ลากข้ามคอลัมน์ทีนึง React วาดทั้งตารางใหม่ทีนึง = ลากแล้วสะดุด
  // ตอนนี้สลับ class บน <td data-w> ตรง ๆ ผ่าน DOM (หน้าตาเหมือนเดิมทุกอย่าง แต่ไม่เรนเดอร์ใหม่)
  const gridRef = useRef(null)
  const overRef = useRef(null)
  const markOver = (w) => {
    if (overRef.current === w) return
    const t = gridRef.current
    if (t) {
      if (overRef.current != null)
        t.querySelectorAll('td.gantt-cell.over').forEach(el => el.classList.remove('over'))
      if (w != null)
        t.querySelectorAll(`td.gantt-cell[data-w="${String(w).replace(/"/g, '\\"')}"]`)
          .forEach(el => el.classList.add('over'))
    }
    overRef.current = w
  }
  // ออกจากช่องของสัปดาห์ w → ล้างไฮไลต์เฉพาะเมื่อยังค้างอยู่ที่สัปดาห์นั้น (กันล้างทับตอนย้ายเข้าช่องใหม่)
  const clearOver = (w) => { if (overRef.current === w) markOver(null) }
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
  // คลิกบล็อก History แผนเดิม (booking) → การ์ดรายละเอียดอ่านอย่างเดียว
  // เก็บ { b, w, vals } (b = item จาก /api/plan/booking-items, vals = หัวแถว CAT/เกจ/เครื่อง)
  const [bkSel, setBkSel] = useState(null)
  // ก้อนที่ user กด ▾ กางดูรายใบสั่ง — คีย์ = "<rowKey>||<week>||<gkey>" (กดซ้ำ = ยุบกลับ)
  // กางแล้วบล็อกย่อยถูกแทรกต่อท้ายก้อนหลักในช่องนั้นช่องเดียว (item อื่นในแถวจึงเลื่อนลงเฉพาะช่องนี้)
  const [expanded, setExpanded] = useState(() => new Set())
  const toggleExpand = (k) => setExpanded(s => {
    const n = new Set(s)
    if (n.has(k)) n.delete(k); else n.add(k)
    return n
  })
  // กรองหัวแถวซ้าย: คลิกช่อง Category/Guage → กรอง CAT+เกจ, คลิก Machine → กรองถึงเครื่อง
  // ค่า = { cat, gauge, mcgroup(หรือ null) } ที่ normalize แล้ว, null = แสดงทั้งหมด
  const [catFilter, setCatFilter] = useState(null)
  // สัปดาห์ที่ล็อก (freeze) — โชว์ได้แต่ลาก/ถอด/วางไม่ได้
  // ยกเว้นสัปดาห์ "ปีหน้า" (เลขเล็กที่วนข้ามปี) ที่เลข < lockBefore แต่จริง ๆ อยู่อนาคต
  const isLocked = (w) => {
    // คอลัมน์ที่ติดปีไว้ = ประวัติของปีก่อน — ผ่านไปแล้วแน่นอน ล็อกก่อนเช็คอย่างอื่น
    // (ไม่งั้น Number('45@2025') = NaN แล้วเงื่อนไขข้างล่างเป็นเท็จหมด = ลากงานไปวางในอดีตได้)
    if (wkPastYear(w) != null) return true
    // ติดปีอนาคต ('45@2028') = ไกลเกิน 1 ปีข้างหน้า → ไม่มีทาง freeze
    if (wkFarYear(w) != null) return false
    if (nextYearWeeks.has(String(w)) || Number(w) === 99) return false
    if (lockBefore != null && Number(w) < Number(lockBefore)) return true
    // แกนอาจเผยสัปดาห์ที่ freeze/ผ่านมาแล้ว (< สัปดาห์แรกที่แก้ได้) — จาก overlay booking หรือ
    // จากแถวอดีตในชีทเอง (SETUP_TRACKING) → ล็อกไว้ดูอย่างเดียว กันลากงานแผนย้อนไปวางในอดีต
    // ⚠️ ต้องใช้ค่า freeze ตัวเดียวกับ isFrozenWeek (ลายคาด) และกับพื้นของ engine —
    // เดิมตรึงไว้ 2 ทำให้ผู้ใช้เปลี่ยนค่าแล้วลายคาดกับ "ลากได้จริง" ไม่ตรงกัน
    if (Number(w) < currentPlanWeek() + freezeW) return true
    return false
  }

  // ── ล็อกแผน (ชีท Lock_Plan ใน MasterMC) ────────────────────────────────────
  // งานที่ผู้ใช้กดล็อกไว้ = ห้าม Replan รื้อไปวางใหม่ (คงสัปดาห์/เครื่องเดิม)
  // ล็อกได้ 2 ฝั่ง — source BOOKING = บล็อกที่จองเครื่องแล้ว / PLAN = งานที่ Planning วางไว้
  const lockMap = useMemo(() => {
    const m = new Map()
    for (const l of planLocks || []) if (l && l.key) m.set(l.key, l)
    return m
  }, [planLocks])

  // ── สี Core Greige / Program ของบล็อก History (booking) ────────────────────
  // ตัดสินจาก "ตัว item เอง" เหมือนบล็อกแผน ไม่ใช่จากว่า item อยู่ในแผนรอบนี้หรือเปล่า:
  //   core    = b.core จาก API (backend เช็คกับ Itemcore.xlsx + ลูกค้า core ตามกฎ Planning.py)
  //   program = ITEM_CODE + ทีม ของ booking ตรงกับชีท Program (programMap) — กฎเดียวกับบล็อกแผน
  const bkProgTeam = (item, team) => {
    const teams = programMap[nkey(item)]
    if (!teams || !teams.length) return ''
    // ทั้งสองฝั่งเก็บหลายทีมคั่นด้วย , ได้ → ตรงกันสักทีมก็ถือว่า match (เหมือน programTeam ในหน้าแผน)
    const mine = String(team ?? '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean)
    if (!mine.length) return ''
    const hit = teams.filter(t => mine.includes(String(t).trim().toUpperCase()))
    return hit.length ? hit.join(' , ') : ''
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

  // Esc = ปิดการ์ด History แผนเดิม (booking)
  useEffect(() => {
    if (!bkSel) return
    const onKey = (e) => { if (e.key === 'Escape') { setBkSel(null); onOpenItem?.(null) } }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [bkSel, onOpenItem])

  // ปิด modal ตารางจากฝั่งพ่อ (✕ / คลิกฉากหลัง) → ต้องปิดการ์ด booking ที่เปิดคู่กันด้วย
  // การ์ดกับ modal เปิดพร้อมกันตอนคลิกบล็อก History แต่ state อยู่คนละ component
  useEffect(() => { if (openItem == null) setBkSel(null) }, [openItem])

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
  // ล็อกของ "แถวในแผน" (source=PLAN) — อ่านคีย์จากคอลัมน์ของไฟล์แผนเอง
  // ใช้ MC_GUAGE (ชื่อคอลัมน์ในไฟล์แผน) ไม่ใช่ GUAGE ซึ่งเป็นชื่อฝั่ง booking
  const planLockOf = (row) => {
    if (!row) return null
    const g = valOf(row)
    // สัปดาห์ในคีย์ล็อก = คีย์คอลัมน์ (ติดปีเมื่อไกลเกิน 1 ปี) — ตรงกับ plan_lock.key_of
    return lockMap.get(lockKeyOf(weekKey(g('PLAN_WEEK'), g('PLAN_YEAR')), g('ITEM_CODE'),
      g('MC_GROUP'), g('MC_GUAGE'), 'PLAN')) || null
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
      // ปีของสัปดาห์ที่วาง — แผนข้ามปีได้ แต่ PLAN_WEEK เป็นเลขเปล่า ต้องอ่านปีจากคอลัมน์นี้
      year: at('PLAN_YEAR'),
      // TYPE = ประเภท job ที่ Planning.py คิดไว้แล้ว (OM/PHET_DOUBLE/PHET_SINGLE) — มีในชีท
      // SETUP_TRACKING ที่ไม่มี FACTORY_TYPE/CAT ให้ classifyType คิดเอง
      type: at('TYPE'),
      // ชื่อคอลัมน์สำรองของชีท SETUP_TRACKING (ไม่มี PRODUCE_QTY / ACTUAL_MC)
      qtyAlt: at('KP_WEIGHT'), mcAlt: at('MC_THIS_WEEK'),
    }
  }, [columns])

  // ชีท SETUP_TRACKING = มุมมองประวัติ setup → สีบล็อกแยกตาม item (ชีทอื่นใช้สีตาม CAT+เกจ)
  const itemColorSheet = ITEM_COLOR_SHEETS.has(String(sheet).trim().toUpperCase())

  // ธง "ก้อนนี้อยู่ในโควตาสีของใบสั่งไหม" รายแถวแผน — Planning.py แตกยอดตามโควตาสีต่อ (SC+ITEM)
  // แล้วเขียนคอลัมน์ COLOR_PART: Yes = อยู่ในยอดที่เปิดสีแล้ว / No = ส่วนเกินโควตา / ว่าง = ไม่ใช่ item สี
  // ⚠️ ต้องใช้ธงรายแถวแทนเกณฑ์ระดับ item (dyeCodes) เพราะ item เดียวที่วางหลายสัปดาห์
  //    มักเปิดสีมาแค่บางก้อน (เช่น W39 = Yes, W40 = No) — ถ้าดูแค่รหัส item บล็อกจะขึ้นสีทั้งแถว
  //    แยกไม่ออกว่าสัปดาห์ไหนคืองานสีจริง
  // ชีทที่ไม่มีคอลัมน์นี้ (REPLAN / ไฟล์แผนที่รันก่อนมีคอลัมน์) → false = ถอยไปใช้เกณฑ์ item เดิม
  // ⚠️ ต้องดูจาก boardRows (ทั้งกระดาน) ไม่ใช่ rows ที่ผ่านตัวกรอง — ไม่งั้นพอค้นหา item
  //    ตัวเดียวแล้วแถวที่เหลือไม่มี Yes/No เลย ธงจะกลายเป็น false → ถอยไปใช้เกณฑ์ระดับ item
  //    ทำให้บล็อกชุดเดียวกัน "ตอนกรอง" กับ "ตอนไม่กรอง" ขึ้นคนละสี
  const hasColorPart = useMemo(() => {
    const i = columns.indexOf('COLOR_PART')
    if (i < 0) return false
    return boardRows.some(({ row }) => { const s = nkey(row[i]); return s === 'YES' || s === 'NO' })
  }, [columns, boardRows])
  // บล็อกนี้เป็น "งานสี" ไหม — เชื่อธงรายแถวก่อนเสมอ (มีแต่ YES ที่เป็นงานสี ; NO = ส่วนเกินโควตา,
  // ว่าง = item ไม่มียอดเปิดสี) ; ชีทที่ไม่มีธงค่อยถอยไปดูว่ารหัส item อยู่ในลิสต์ item ที่เปิดสีไหม
  const dyedOf = (j) => (hasColorPart ? j.cpart === 'YES' : !!dyeCodes && dyeCodes.has(nkey(j.item)))

  // คอลัมน์หัวแถวที่มีอยู่จริง + ตำแหน่ง sticky (left สะสม)
  // Item อยู่ท้ายสุดเสมอ — .filter ด้านล่างตัดทิ้งเองถ้าชีทนั้นไม่มีคอลัมน์ ITEM_CODE
  const groups = useMemo(() => {
    let left = 0
    return [...GROUP_DEF, ITEM_GROUP]
      .filter(g => columns.includes(g.col))
      .map(g => { const item = { ...g, idx: columns.indexOf(g.col), left }; left += g.width; return item })
  }, [columns])

  // เปลี่ยนชีท/ชุดคอลัมน์หัวแถว → ล้างตัวกรองเก่า (keys ของชีทเดิมจะไม่ตรงกับชีทใหม่ = แถวหายหมด)
  // ต้องอยู่ "เหนือ" early return ทุกจุดด้านล่าง — ไม่งั้นตอนกรอง/ค้นหาจนเหลือ 0 แถว hook นี้จะไม่ถูก
  // เรียก → React error "Rendered fewer hooks than expected" → แอปดับทั้งหน้า
  const groupSig = groups.map(g => g.col).join('|')
  useEffect(() => { setCatFilter(null) }, [groupSig])

  // คอลัมน์กำหนดสี — ปกติ CAT + เกจ ; เฉพาะ SETUP_TRACKING = สีตาม ITEM_CODE
  // เพื่อให้ item ต่างกันสีต่างกัน ไล่ดูประวัติของ item เดียวข้ามสัปดาห์ได้ด้วยสี
  // ⚠️ ผูกกับ "ชีท" ไม่ใช่ "มี item ที่หัวแถวไหม" — ตอนนี้หน้าแผนก็มี Item เป็นหัวแถวแล้ว
  //    ถ้าผูกกับหัวแถว สีทั้งหน้าแผนจะเปลี่ยนจาก CAT+เกจ ไปเป็นสีรายตัว item
  const colorByItem = itemColorSheet && columns.includes('ITEM_CODE')
  const colorCols = useMemo(
    () => (colorByItem
      ? [columns.indexOf('ITEM_CODE')]
      : COLOR_DEF.filter(n => columns.includes(n)).map(n => columns.indexOf(n))),
    [columns, colorByItem])

  const supported = groups.length > 0 && ci.week >= 0
  const rowKey = (row) => groups.map(g => norm(row[g.idx])).join('')
  // คีย์คอลัมน์สัปดาห์ของแถวแผน = เลขสัปดาห์ + ปีของแถว (PLAN_YEAR) ตามกติกา weekKey
  // (ในกรอบ 1 ปี = เลขเปล่าเหมือนเดิม ; ไกลกว่านั้นติดปี) — ทุกจุดที่จัดแถวลงคอลัมน์ต้องใช้ตัวนี้
  const rowWk = (row) => weekKey(norm(row[ci.week]), ci.year >= 0 ? norm(row[ci.year]) : '')
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
    const src = historyItems.length ? [...bookingItems, ...historyItems] : bookingItems
    // History = ปิด ก็ยังต้องเห็นงานที่ "ล็อกแผนไว้" (🔐 ชีท Lock_Plan) — Replan จะคงงานพวกนี้
    // ไว้สัปดาห์/เครื่องเดิม ถ้าซ่อนไปด้วยจะอ่านเป็น "ช่องว่าง" ทั้งที่เครื่องถูกจองไว้แล้ว
    const lockedOnly = bookingMode === 'off'
    if (!supported || !src.length || (lockedOnly && !lockMap.size)) return empty
    const FIELD = { CAT: 'cat', MC_GUAGE: 'gauge', MC_GROUP: 'mc_group', ITEM_CODE: 'item' }
    // ต้อง map ทุกคอลัมน์หัวแถวเป็นฟิลด์ booking ได้ ไม่งั้น key ไม่ตรงแถวแผน → ไม่ overlay
    if (!groups.every(g => FIELD[g.col])) return empty
    // กรองด้วยชุด ITEM_CODE ที่ผู้เรียกส่งมา (null = ไม่กรอง โชว์ทุก item)
    // ผู้เรียกเป็นคนรวมเงื่อนไขทั้งหมด (ติ๊กเลือก item + คำค้น + ตัวกรองชนิด item) มาให้แล้ว
    const allowed = bookingPick || null
    // เพดานย้อนหลังของบล็อก booking (ไฟล์ปัจจุบัน) — ปกติ HISTORY_WEEKS_BACK สัปดาห์
    //   · ค้นหา (bookingAllWeeks) → ปลดเพดาน ต้องเจอ item ที่รันไปนานแล้วด้วย
    //   · ผู้ใช้ดึงประวัติ (historyFrom) → ปลดเพดานเช่นกัน เพราะจุดเริ่มมาจากที่ user เลือกแล้ว
    //     (บล็อกที่เก่ากว่าไฟล์ booking ปัจจุบันมาทาง historyItems ซึ่ง backend ตัดไม่ให้ทับกัน)
    const wkFloor = (bookingAllWeeks || historyFrom) ? -Infinity : currentPlanWeek() - HISTORY_WEEKS_BACK
    const cells = new Map(), rowsM = new Map(), weeks = new Set()
    for (const b of src) {
      const wk = weekKey(norm(b.week), b.year)   // ประวัติปีก่อน → คีย์ติดปี ('45@2025')
      if (norm(b.week) === '' || Number(b.week) === 99) continue
      if (wkPastYear(wk) == null && Number(b.week) < wkFloor) continue
      if (allowed && !allowed.has(String(b.item).toUpperCase())) continue
      // โหมดปิด: ผ่านเฉพาะก้อนที่ user กดล็อกแผนไว้ (คีย์เดียวกับที่ใช้วาดไอคอน 🔐)
      if (lockedOnly && !lockMap.has(lockKeyOf(wk, b.item, b.mc_group, b.gauge))) continue
      // งานที่ Replan ยกไปวางใหม่ที่อื่นแล้ว — ของเดิมที่สัปดาห์นี้ไม่จริงอีกต่อไป
      // ซ่อนเป็นค่าเริ่มต้น ไม่งั้นอ่านเป็น "แผนยังค้างงานไว้ตรงนี้" ทั้งที่ย้ายไปแล้ว
      if (!showReplanned && Number(b.replanned) > 0) continue
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
  }, [bookingItems, historyItems, historyFrom, bookingMode, bookingPick, bookingAllWeeks, showReplanned, groups, supported, lockMap])

  // เลขสัปดาห์ของ "ประวัติปีปัจจุบัน" (คีย์เลขเปล่า ไม่ติดปี) — ดึงลึกเกิน ~27 สัปดาห์เมื่อไร
  // เลขจะเล็กจน isNextYearWeek เดาว่าเป็นปีหน้าแล้วส่งคอลัมน์ไปต่อท้ายแกนขวา ทั้งที่เป็นอดีต
  // ที่นี่รู้ปีแน่นอนจาก backend จึงใช้ทับคำเดาได้
  // (ข้อจำกัดที่เหลือ: ถ้าแผนวางงานไว้ W1–W9 ของ "ปีหน้า" ในเลขเดียวกับประวัติของปีนี้ด้วย
  //  สองอย่างจะแชร์คอลัมน์เดียวกัน — เกิดได้ยากและไม่ทำให้ข้อมูลผิด แค่ยืนช่องเดียวกัน)
  const historyPastWeeks = useMemo(() => {
    const s = new Set()
    for (const b of historyItems) if (Number(b.year) === CUR_YEAR) s.add(String(b.week))
    return s
  }, [historyItems])

  // ปีของแต่ละเลขสัปดาห์ ตามคอลัมน์ PLAN_YEAR ของชีทแผน
  // จำเป็นเพราะแกน Gantt เก็บแค่เลขสัปดาห์เปล่า: งานปี 2027 W29 ถ้าไม่ดูปีจะถูก isNextYearWeek
  // (ที่เดาจากระยะทาง) ตัดสินเป็น "สัปดาห์อดีตของปีนี้" → ลากคอลัมน์ freeze ย้อนหลังโผล่ทั้งแถบ
  const weekYears = useMemo(() => {
    const m = new Map()
    if (!supported || !(ci.year >= 0)) return m
    for (const { row } of rows) {
      const w = rowWk(row)
      const y = Number(norm(row[ci.year]))
      if (w === '' || !Number.isFinite(y) || y <= 0) continue
      if (!m.has(w)) m.set(w, new Set())
      m.get(w).add(y)
    }
    return m
  }, [rows, ci, supported])

  // สัปดาห์นี้เป็นของปีหน้าไหม — ใช้ PLAN_YEAR ถ้าชีทมี ไม่มี (เช่น SETUP_TRACKING) ค่อยเดาจากระยะทาง
  // เงื่อนไข "ทุกแถวของสัปดาห์นั้นเป็นปีหน้า" เพราะ 1 คอลัมน์ = เลขสัปดาห์เดียว ถ้าปีนี้ยังมีงาน
  // อยู่ในเลขสัปดาห์เดียวกัน คอลัมน์ต้องเป็นของปีนี้ (คอลัมน์เดียวแทนสองปีพร้อมกันไม่ได้)
  const isNextYearW = (w, cur = currentPlanWeek()) => {
    // คอลัมน์ที่รู้ปีแน่นอนว่าเป็นอดีต (ประวัติ) ไม่ต้องเดา — ปีก่อนติดมากับคีย์, ปีนี้ดูจากทะเบียน
    if (wkPastYear(w) != null || historyPastWeeks.has(String(w))) return false
    if (wkFarYear(w) != null) return true
    const ys = weekYears.get(String(w))
    if (ys && ys.size) return [...ys].every(y => y > CUR_YEAR)
    return isNextYearWeek(Number(w), cur)
  }

  // แกนสัปดาห์ — โชว์ทุกสัปดาห์ที่ทำแผนได้ (ลากงานไปได้ทุก week)
  // รวมสัปดาห์จาก rows(งาน) + load(capacity) + ava(เครื่องว่าง) เพื่อครอบคลุม horizon เต็ม
  // ตัดสัปดาห์ที่ freeze ออก: เริ่มที่ current+freezeW (ค่า KP_FREEZE_WEEKS จากหน้า Setting ; ปกติ 2)
  const weeks = useMemo(() => {
    if (!supported) return []
    const vals = new Set()
    const pastYears = new Set()   // คอลัมน์ประวัติของปีก่อน ('45@2025') — เก่ากว่าทุกคอลัมน์เสมอ
    // คอลัมน์ไกลเกิน 1 ปีข้างหน้า ('45@2028') — ต่อท้ายสุด ; โชว์เฉพาะสัปดาห์ที่มีงาน/บล็อกจริง
    // (ava/load ของ backend ยาวถึงสุดปฏิทิน ถ้าเปิดทุกคอลัมน์แกนจะยาวหลายร้อยช่อง — เหมือนกติกา
    //  คอลัมน์ "ปีหน้า" เดิมที่โชว์เฉพาะสัปดาห์ที่มีงาน)
    const farYears = new Set()
    const jobW = new Set()   // สัปดาห์ที่มีงานวางจริง (จาก rows) — ใช้ระบุสัปดาห์ "ปีหน้า"
    const addTagged = (w, withContent) => {
      if (wkPastYear(w) != null) { pastYears.add(String(w)); return true }
      if (wkFarYear(w) != null) { if (withContent) farYears.add(String(w)); return true }
      return false
    }
    for (const { row } of rows) {
      const v = rowWk(row)
      if (v === '' || addTagged(v, true)) continue
      vals.add(v); jobW.add(v)
    }
    for (const w of Object.keys(load || {})) if (!addTagged(w, false)) vals.add(String(w))
    for (const w of Object.keys(ava || {})) if (!addTagged(w, false)) vals.add(String(w))
    // สัปดาห์จาก booking overlay (แผนเก่า) — ต้องโผล่เป็นคอลัมน์แม้แผนไม่มีงานในสัปดาห์นั้น
    for (const w of bookingData.weeks) {
      if (!addTagged(w, true)) vals.add(String(w))
    }
    // เรียงตาม (ปี, สัปดาห์) จริง แล้วต่อไว้หน้าสุด — ปนกับเลขเปล่าไม่ได้ (Number() = NaN)
    const pastCols = [...pastYears].sort((a, b) => wkPastYear(a) - wkPastYear(b) || wkNum(a) - wkNum(b))
    const farCols = [...farYears].sort((a, b) => wkFarYear(a) - wkFarYear(b) || wkNum(a) - wkNum(b))
    const arr = [...vals]
    const nums = arr.map(Number).filter(Number.isFinite)
    if (arr.length && nums.length === arr.length) {
      // ปกติเริ่มที่สัปดาห์แรกที่แก้ได้ (freeze ซ่อน) = ปัจจุบัน + freezeW
      // ถ้า lockBefore ส่งมา = โชว์รวมสัปดาห์ freeze (ล็อกไว้) โดยถอยไปให้เห็นช่วง freeze ครบ
      const cur = currentPlanWeek()
      let lo = lockBefore != null ? Number(lockBefore) - freezeW : cur + freezeW
      // ขยายขอบซ้ายให้ครอบ "สัปดาห์อดีตที่มีของจริง" — ทั้งแถวในชีท (เช่น SETUP_TRACKING ที่มี
      // แถว OLD ย้อนหลัง) และ overlay booking — แต่ไม่เกิน HISTORY_WEEKS_BACK สัปดาห์
      // ตอนค้นหา (bookingAllWeeks) สัปดาห์ของ booking ที่ค้นเจอไม่ติดเพดานย้อนหลัง —
      // ไม่งั้นบล็อกมีอยู่แต่ไม่มีคอลัมน์สัปดาห์ให้ยืน = ค้นเจอแล้วก็ยังไม่เห็น
      // ผู้ใช้ดึงประวัติเอง (historyFrom) ก็ปลดเพดานเช่นกัน — จุดเริ่มคือสัปดาห์ที่เขาเลือกแล้ว
      // History = ปิด → ไม่ขยายขอบซ้ายเลย ปุ่มนั้นต้องปิด "อดีตทั้งแผง" ไม่ใช่แค่บล็อก 📋
      // (แถวงานสัปดาห์อดีตที่ค้างอยู่ในชีทเองก็ดึงคอลัมน์ถอยได้ → กดปิดแล้วยังเห็นแถบ
      //  History คาดอยู่ ทั้งที่สั่งปิดไปแล้ว ; อยากดูอดีตให้กด Item (ทั้งหมด))
      const pastFloor = historyFrom ? -Infinity : cur - HISTORY_WEEKS_BACK
      const hasContent = new Set([...jobW, ...bookingData.weeks].map(Number))
      const bkAny = (bookingAllWeeks || historyFrom) ? new Set([...bookingData.weeks].map(Number)) : null
      const past = bookingMode === 'off' ? [] : arr.map(Number).filter(n =>
        Number.isFinite(n) && n !== 99 && n < lo && !isNextYearW(n, cur)
        && ((n >= pastFloor && hasContent.has(n)) || (bkAny && bkAny.has(n))))
      if (past.length) lo = Math.min(lo, ...past)
      // W99 = sentinel "งานล้น" → ไม่โชว์ใน Gantt (ผู้ใช้ขอ)
      // สัปดาห์ปีนี้ = เลข >= lo ; สัปดาห์ปีหน้า = สัปดาห์ที่มีงานจริงและเลขวนข้ามปี (ดู
      //   isNextYearWeek) นำมาต่อท้ายปีนี้ ; สัปดาห์อดีตที่เก่ากว่า pastFloor = ไม่โชว์
      // ตัดสัปดาห์หยุด (ไม่มีทั้งงาน/AVA/โหลด เช่น W31) ออกโดยปริยาย เพราะกรองจาก vals อยู่แล้ว
      const thisYear = arr.filter(w => { const n = Number(w); return n >= lo && n !== 99 })
        .sort((a, b) => Number(a) - Number(b))
      const nextYear = arr.filter(w => { const n = Number(w); return n !== 99 && n < lo && jobW.has(w) && isNextYearW(n, cur) })
        .sort((a, b) => Number(a) - Number(b))
      return [...pastCols, ...thisYear, ...nextYear, ...farCols]
    }
    return [...pastCols,
    ...arr.filter(w => w !== '99').sort((a, b) => String(a).localeCompare(String(b), 'th', { numeric: true })),
    ...farCols]
  }, [rows, ci, supported, load, ava, lockBefore, freezeW, bookingData, bookingMode, bookingAllWeeks, historyFrom, weekYears])

  // สัปดาห์ "ปีหน้า" (เลขเล็กที่วนข้ามปี) — ไม่ควรถูกล็อกในโหมด freeze แม้เลข < lockBefore
  const nextYearWeeks = useMemo(() => {
    const s = new Set()
    if (!supported) return s
    const cur = currentPlanWeek()
    for (const { row } of rows) {
      const v = rowWk(row)
      // เฉพาะเลขที่วนข้ามปีจริง — สัปดาห์อดีตของปีนี้ (SETUP_TRACKING แถว OLD) ต้องยังถูกล็อก
      if (v !== '' && isNextYearW(v, cur)) s.add(v)
    }
    return s
  }, [rows, ci, supported, weekYears])

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

  // จัดงานลงช่อง (rowKey × week)
  //
  // ผ้าตัวเดียวกัน เครื่อง+เกจ+สัปดาห์เดียวกัน แต่คนละใบสั่ง (SC/SO) ถูกเขียนเป็นคนละแถว →
  // ถ้าวาดแยกใบละบล็อก ช่องเดียวจะมีก้อนหน้าตาเหมือนกันซ้อนกัน 4 ก้อน แต่ละก้อนโชว์เลขเครื่อง
  // ของใบตัวเอง อ่านแล้วบอกไม่ได้ว่าตกลงผ้าตัวนี้ในช่องนี้ใช้เครื่องกี่ตัว จึง "ยุบ" เหลือบล็อกเดียว
  // ต่อ (ผ้า × ความหมายของบล็อก) แล้วกดกาง ▾ ดูรายใบได้ (ดู foldJobs/expanded)
  //   • เลขเครื่องของก้อนที่ยุบ = max ภายในใบสั่งเดียวกัน แล้ว "บวกข้ามใบสั่ง" — สูตรเดียวกับ
  //     planMcAgg และ _pool_blocks_from_plan() ใน Planning.py เป๊ะ ๆ :
  //     หลายแถวของ SC เดียวกัน = เครื่องชุดเดียวเขียนซ้ำ (max) ; คนละ SC = คนละชุดเครื่องจริง (บวก)
  //     → เลขบนก้อนที่ยุบจึงตรงกับเครื่องที่ชิป "ว่าง/เต็ม" ของช่องหักไปจริง
  //   • ยุบเฉพาะบล็อกที่ความหมาย/สีเดียวกัน — ธงโควตาสี (COLOR_PART), แถว HOLD และงานจ้างทอ
  //     ยังแยกก้อนกันอยู่ เพราะพื้นบล็อกคนละสีและอ่านคนละเรื่อง
  //   ⚠️ ยุบที่ "ชั้นวาดภาพ" เท่านั้น — ชีท/ไฟล์ Excel ยังเป็น 1 แถวต่อ 1 ใบสั่งเหมือนเดิม และ
  //     ตัวเลขเครื่องว่าง/โหลด job คิดจาก rows ดิบ (planNewByWeekType/planMcByWeekCat) ไม่ผ่านตรงนี้
  const cells = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const { row, idx } of rows) {
      const week = rowWk(row)
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
        // เดดไลน์ทอของแถวนี้ — ใช้เขียนป้าย/tooltip ว่าเลย Target Knit สัปดาห์ไหน
        tk: targetKnitWeek(v),
        // เลยไปกี่สัปดาห์ — คิดที่นี่เพราะต้องใช้ปี (PLAN_YEAR/TARGET_YEAR) ที่มีอยู่ในแถว
        tkLate: lateWeeks(v, week),
        item: norm(row[ci.item]),
        stMc: hasSetupMc ? `setup ${stSetup} · carry ${stCarry}` : '',
        stSetup, stCarry,
        stTotal: hasSetupMc ? (Number(v('MC_THIS_WEEK')) || stSetup + stCarry) : 0,
        stSource: v('PLAN_SOURCE'),
        // แถวกันเครื่อง (Planning.py: PLAN_SOURCE = HOLD_COLOR / HOLD_TARGET) — ไม่ได้ผลิต
        // แต่ถือเครื่องไว้ ไม่ให้ item อื่นแย่ง และกลับมาทอต่อโดยไม่ต้อง setup ใหม่
        //   HOLD_COLOR  = เว้นรอสีเปิดรอบใหม่ · HOLD_TARGET = ผ้า core เว้นรอรอบผลิตถัดไป
        hold: nkey(v('PLAN_SOURCE')) === 'HOLD_COLOR' || nkey(v('PLAN_SOURCE')) === 'HOLD_TARGET',
        holdKind: nkey(v('PLAN_SOURCE')) === 'HOLD_TARGET' ? 'target' : 'color',
        // ใบที่ออกเลข KP แล้ว (Replan.py: KP_PLAN_SOURCE) — ยืนยันกับฝ่ายผลิตไปแล้ว
        // โชว์ได้แต่ลาก/แก้จำนวน/ลบ/แบ่ง/ส่งจ้างทอ ไม่ได้ (ต่างจาก core ที่เบรกซึ่งยังแก้ได้)
        keepLocked: nkey(v('PLAN_SOURCE')) === KEEP_LOCKED_SOURCE,
        ck: colorKey(row),
        // ธงโควตาสีรายแถว (YES / NO / '') — ใช้ตัดสินว่าบล็อกนี้เป็นงานสีจริงไหม
        cpart: nkey(v('COLOR_PART')),
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
        // พูลของผู้รับจ้างที่ทอก้อนนี้ ("RAP·22") — บล็อกจ้างทอที่ถูกยุบเข้าแถวเครื่องในบ้าน
        // ไม่มีหัวแถวบอกแล้วว่าใครทอ จึงต้องเขียนไว้บนบล็อกเอง
        s9pool: s9PoolText(v),
        // ใบสั่งของแถวนี้ — ตอนยุบต้องใช้แยกว่า "แถวซ้ำของใบเดิม" (เครื่องชุดเดียว) กับ
        // "คนละใบ" (คนละชุดเครื่อง) ไม่งั้นบวกเลขเครื่องผิดทั้งสองทาง
        so: v('SC_SO_NO'),
        // คีย์ยุบบล็อก — ดูคอมเมนต์หัว memo (ผ้า + ความหมายของบล็อก ; ใบสั่งไม่อยู่ในคีย์)
        // งาน KP แยกก้อนต่างหาก — ถ้ายุบรวมกับงานปกติของผ้าตัวเดียวกัน ทั้งก้อนจะลากไม่ได้ตามมัน
        gkey: [nkey(row[ci.item]), nkey(v('COLOR_PART')), nkey(v('PLAN_SOURCE')).startsWith('HOLD') ? nkey(v('PLAN_SOURCE')) : '',
          outsourceIdx && outsourceIdx.has(idx) ? (s9PoolText(v) || 'S9') : '',
          nkey(v('PLAN_SOURCE')) === KEEP_LOCKED_SOURCE ? KEEP_LOCKED_SOURCE : ''].join('|'),
      })
    }
    // ยุบใบสั่งของผ้าตัวเดียวกันในช่องเดียวกันให้เหลือบล็อกเดียว (ใบเดียว = ปล่อยผ่าน)
    for (const [k, list] of m) if (list.length > 1) m.set(k, mergeJobs(list))
    return m
  }, [rows, ci, groups, colorCols, supported, barFields, colIdx, programRows, outsourceIdx])

  // idx ของแถว → ใบสั่งทุกใบที่อยู่ในบล็อกที่ยุบก้อนเดียวกัน — การ์ดงานใช้ทำรายการให้สลับดูรายใบ
  // (ไม่มีคีย์ = แถวนั้นอยู่ก้อนเดียวใบเดียว ไม่ต้องโชว์รายการ)
  const siblingsByIdx = useMemo(() => {
    const m = new Map()
    for (const arr of cells.values())
      for (const j of arr)
        if (j.subs && j.subs.length > 1) for (const s of j.subs) m.set(s.idx, j.subs)
    return m
  }, [cells])

  // ผังบรรทัดของแต่ละแถว — ให้ item เดียวกันอยู่ "บรรทัดเดียวกัน" ทุกสัปดาห์ ไล่สายตาตามแนวนอนได้
  // slot = 1 บรรทัดในช่อง ; เรียงตามชื่อ item (A→Z) ; item ที่สัปดาห์เดียวมีหลายบล็อก (คนละ SC
  // หรือมีทั้งงานแผนและ History) จองบรรทัดเท่าจำนวนสูงสุดที่เจอ เพื่อไม่ให้ดัน item ตัวถัดไปเลื่อนลง
  // เฉพาะบางสัปดาห์ — ต้องนับรวมบล็อก History (booking) ด้วย ไม่งั้นแถวที่มีแต่ History ไม่ถูกจัดบรรทัด
  const rowSlots = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const r of gantRows) {
      const maxN = new Map()          // item → จำนวนบล็อกสูงสุดที่อยู่ในสัปดาห์เดียวกัน
      for (const w of weeks) {
        const ck = r.key + '||' + w
        const jobs = cells.get(ck) || []
        const bks = bookingData.cells.get(ck) || []
        if (!jobs.length && !bks.length) continue
        const c = new Map()
        const bump = (it) => { const k = nkey(it); c.set(k, (c.get(k) || 0) + 1) }
        for (const j of jobs) bump(j.item)
        for (const b of bks) bump(b.item)
        for (const [k, n] of c) if (n > (maxN.get(k) || 0)) maxN.set(k, n)
      }
      if (!maxN.size) continue
      const slots = []
      for (const k of [...maxN.keys()].sort((a, b) => a.localeCompare(b, 'th', { numeric: true })))
        for (let i = 0; i < maxN.get(k); i++) slots.push(k)
      m.set(r.key, slots)
    }
    return m
  }, [gantRows, weeks, cells, bookingData, supported])

  // ผลรวม NEW_MC (job = setup 1 เครื่อง) ต่อ (สัปดาห์ × ประเภท) จากแถวที่วางจริง — live ตามที่ลาก/แก้
  //
  // ⚠️ item เดียวกัน เครื่อง+เกจเดียวกัน สัปดาห์เดียวกัน แต่คนละ SC = ใช้ "เครื่องชุดเดียวกัน" แบ่งเวลากัน
  // Planning.py เขียน ACTUAL_MC/CARRYOVER_MC/NEW_MC ค่าเดียวกันซ้ำทุกแถว (ไม่ใช่แยกเครื่องต่อแถว)
  // → ต้องยุบเป็นค่าเดียว (max) ก่อนรวม ไม่งั้นนับ job ซ้ำเป็นเท่าตัว
  const planNewByWeekType = useMemo(() => {
    const m = {}
    if (!supported) return m
    const byItem = new Map()      // (สัปดาห์|ประเภท|item|เครื่อง|เกจ) → NEW_MC ของเครื่องชุดนั้น
    for (const { row, idx } of boardRows) {
      // งานจ้างทอตั้งเครื่องที่โรงผู้รับจ้าง ไม่กินโควตา setup ของเรา (ตรงกับ _is_outsource_row
      // ใน Planning.py) — ยิ่งจำเป็นในโหมดยุบแถว เพราะ CAT ของแถวถูกสลับเป็นของเครื่องในบ้าน
      if (outsourceIdx && outsourceIdx.has(idx)) continue
      const w = rowWk(row); if (w === '') continue
      const t = classifyType(ci.factory >= 0 ? row[ci.factory] : '', ci.cat >= 0 ? row[ci.cat] : '')
      if (!t) continue
      // แถวที่นั่งบนเครื่องที่ booking จองไว้ครบแล้ว (MC_BOOKING ≥ ACTUAL_MC) — เครื่องถูกตั้งไป
      // แล้วฝั่ง booking และ job ถูกนับในช่อง "เก่า" (SETUP_TRACKING แถว OLD) → นับซ้ำไม่ได้
      // (เจอกับแถว KP_KEEP 23 ก.ย. 2569: W41 เกินจริง 8 job) ; ต้องตรงกับ _sits_on_booking_mc
      // ใน plan_view.py เป๊ะ ๆ ไม่งั้นเลขตอนโหลดกับตอนลากไม่ตรงกัน
      // ลากงานเมื่อไร mcRecalc อัปเดต MC_BOOKING ให้ → job กลับมาถูกนับเองอัตโนมัติ
      const amc = ci.actualmc >= 0 ? (Number(norm(row[ci.actualmc])) || 0) : 0
      const bmc = ci.bookingmc >= 0 ? (Number(norm(row[ci.bookingmc])) || 0) : 0
      if (amc > 0 && bmc >= amc) continue
      const nm = ci.newmc >= 0 ? (Number(norm(row[ci.newmc])) || 0) : 0
      const k = [w, t, nkey(row[ci.item]),
        ci.mcgroup >= 0 ? nkey(row[ci.mcgroup]) : '',
        ci.gauge >= 0 ? norm(row[ci.gauge]) : ''].join('|')
      byItem.set(k, Math.max(byItem.get(k) || 0, nm))
    }
    for (const [k, nm] of byItem) {
      const [w, t] = k.split('|')
      m[w + '|' + t] = (m[w + '|' + t] || 0) + nm
    }
    return m
  }, [boardRows, ci, supported, outsourceIdx])

  // job setup ของ "สัปดาห์ประวัติ" ต่อ (สัปดาห์ × ประเภทโรงงาน) — คิดจากบล็อกประวัติเอง
  //
  // แถบโหลดปกติอ่านจากไฟล์แผนปัจจุบันล้วน ๆ (SETUP_TRACKING + booking สด + NEW_MC ของแถวที่วาง)
  // ซึ่งไม่มีสัปดาห์อดีตที่ผู้ใช้กดดึงประวัติมา → ช่องสัปดาห์นั้นเลยว่างทั้งที่บล็อกมีงานเต็มไปหมด
  // 1 job = ตั้งเครื่องใหม่ 1 เครื่อง = b.setup (_mc_increase จาก AVA_MC) เหมือนกติกาที่ใช้ทุกที่
  // — เครื่องที่วิ่งต่อจากสัปดาห์ก่อน (carry) ไม่กินโควตา
  // บล็อกที่มาจากไฟล์ booking เก่าที่ยังไม่มีคอลัมน์ลูกโซ่จะได้ setup = 0 (นับไม่ได้จริง ๆ
  // ไม่ใช่ไม่มีงาน) → กดปุ่ม "ดึงใหม่จาก DB" ให้ AVA_MC คำนวณใหม่แล้วตัวเลขจะขึ้นครบ
  //
  // ⚠ ข้าม "สัปดาห์ขอบข้อมูล" (b.edge = สัปดาห์แรกสุดของไฟล์ที่ดึงมา) — สัปดาห์นั้นไม่มี
  // สัปดาห์ก่อนหน้าให้ AVA_MC ยกเครื่องอุ่นมา ทุกเครื่องจึงถูกนับเป็น setup ใหม่หมด
  // (เคยเห็น 368 job ในสัปดาห์เดียว) ถ้านับด้วยแถบจะขึ้นแดง "เกิน" ทั้งที่ของจริงไม่เกิน
  const histJobsByWeekType = useMemo(() => {
    const m = {}, edge = new Set(), stat = new Map()
    for (const b of historyItems) {
      const w = weekKey(norm(b.week), b.year)
      let st = stat.get(w)
      if (!st) { st = { carry: 0, n: 0 }; stat.set(w, st) }
      st.carry += Number(b.carry) || 0
      st.n++
      if (Number(b.edge) > 0) edge.add(w)
      const s = Number(b.setup) || 0
      if (s <= 0) continue
      const t = classifyType(b.factory, b.cat)
      if (!t) continue
      const k = w + '|' + t
      m[k] = (m[k] || 0) + s
    }
    // ทะเบียนที่ดึงมาก่อนจะมีธง edge → เดาจากรูปข้อมูลแทน: ทั้งสัปดาห์ไม่มีเครื่องวิ่งต่อเลย
    // สักเครื่องทั้งที่มีงานเป็นร้อยบล็อก = สัปดาห์แรกของชุดแน่ ๆ (สัปดาห์ปกติ carry 150–280)
    for (const [w, st] of stat) if (st.n >= 10 && st.carry === 0) edge.add(w)
    for (const w of edge) for (const k of Object.keys(m)) if (k.startsWith(w + '|')) delete m[k]
    return { jobs: m, edge }
  }, [historyItems])

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
    for (const { row, idx } of boardRows) {
      // แถวจ้างทอที่ถูกยุบเข้าแถวในบ้าน — หัวแถวเป็นเครื่องของเรา แต่เครื่องที่ใช้เป็นของผู้รับจ้าง
      // ถ้านับด้วยจะไปหักเลข "ว่าง" ของพูลในบ้านทั้งที่ไม่ได้ใช้เครื่องเราสักตัว
      if (mergedIdx && mergedIdx.has(idx)) continue
      const w = rowWk(row); if (w === '') continue
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
      // ⚠️ ต้องใช้สูตรเดียวกับ _pool_blocks_from_plan() ใน Planning.py เป๊ะ ๆ ไม่งั้นเลข "ว่าง"
      // บนหน้าเว็บกับบัญชีพูลที่ตัววางแผนใช้จะไม่ตรงกัน (เคสจริง W40 DOUBLE-30 เกจ 28:
      // เว็บบอกว่าง 13 แต่ Planning บอก pool remain=0 แล้วเลื่อนงานออกไปทั้งสัปดาห์)
      //   • หลายแถวของ "ใบสั่งเดียวกัน" = เครื่องชุดเดียวกันที่เขียนค่าซ้ำทุกแถว → max
      //   • คนละใบสั่ง = คนละชุดเครื่องจริง (W40 F4SYNTREL247/1A0 = 6+5+5 = 16) → บวก
      // แถวที่ยืมเครื่องใบอื่น (MC_SHARED > 0) เขียน ACTUAL_MC = 0 อยู่แล้ว จึงไม่ถูกนับซ้ำ
      // ไฟล์แผนเก่าที่ไม่มีคอลัมน์ SC_SO_NO → ทุกแถวตกลงคีย์เดียวกัน = max ทั้ง item (พฤติกรรมเดิม)
      const so = ci.sc >= 0 ? norm(row[ci.sc]) : ''
      let cur = byItem.get(k)
      if (!cur) {
        cur = { w, catKey, bkKey, item, mcg, gauge: g, qty: 0, bk: bkCol, bySo: new Map() }
        byItem.set(k, cur)
      }
      // มีแต่ยอดผลิต (qty) ที่เป็นของแต่ละ SC จริง จึงบวกสะสมเสมอ
      cur.qty += qty
      if (bkCol != null) cur.bk = cur.bk == null ? bkCol : Math.max(cur.bk, bkCol)
      const p = cur.bySo.get(so)
      if (p) {
        p.mc = Math.max(p.mc, mc)
        p.setup = Math.max(p.setup, setup)
        p.carry = Math.max(p.carry, carry)
      }
      else cur.bySo.set(so, { mc, setup, carry })
    }
    for (const e of byItem.values()) {
      const { w, catKey, bkKey, item, bk } = e
      // ยุบด้วย max ในใบสั่งเดียวกันไปแล้ว ตรงนี้บวกข้ามใบสั่ง (เครื่องคนละชุด)
      let mc = 0, setup = 0, carry = 0
      for (const p of e.bySo.values()) { mc += p.mc; setup += p.setup; carry += p.carry }
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
      // ⚠️ ห้าม spread `e` ตรง ๆ — มี bySo (Map) ติดไปด้วย ; ระบุฟิลด์ที่การ์ดใช้ให้ครบแทน
      if (mc > 0 || net > 0) (detail[dk] || (detail[dk] = [])).push({
        w, catKey, bkKey, item, mcg: e.mcg, gauge: e.gauge, qty: e.qty,
        mc, setup, carry, bk: bkVal, net, kind,
      })
      if (net === 0) continue
      const slot = m[dk] || (m[dk] = { normal: 0, poly: 0, cotton: 0 })
      slot[kind] += net
    }
    for (const k in detail) detail[k].sort((a, b) => b.net - a.net || b.mc - a.mc)
    return { mc: m, detail }
  }, [boardRows, ci, supported, bookingMc, ava, poolMap, mergedIdx])
  const planMcByWeekCat = planMcAgg.mc

  // งาน booking (แผนเก่าที่ commit แล้ว) ของช่องที่เปิดการ์ดอยู่ — เครื่องที่ booking จองไปแล้ว
  // ถูกหักออกจาก TOTAL_MC_REMAIN ตั้งแต่ต้นทาง ช่องจึงเกินได้ทั้งที่แผนรอบนี้ไม่มีงานเลย
  // → การ์ดต้องบอกได้ว่าเป็น item ไหน (ต่างจาก overlay 📋 ที่โชว์แค่ 5 สัปดาห์ย้อนหลังและปิดได้)
  // คิดเฉพาะตอนเปิดการ์ด (avaSel) เพราะ bookingItems ยาวหลักพันแถว
  const avaBooking = useMemo(() => {
    if (!avaSel) return []
    const out = []
    for (const b of bookingItems) {
      if (weekKey(norm(b.week), b.year) !== avaSel.w) continue
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
        if (rowWk(row) !== loadSel.w) continue
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
        // เครื่องชุดเดียวกันเขียนซ้ำหลายแถว (หลาย SC) → max ; ยอดผลิตของแต่ละ SC → บวก
        const cur = byKey.get(k)
        if (cur) {
          cur.qty += qty
          cur.setup = Math.max(cur.setup, setup)
          cur.carry = Math.max(cur.carry, carry)
          cur.mc = Math.max(cur.mc, mc)
        }
        else byKey.set(k, { item, mcg, gauge, setup, carry, mc, qty, so: ci.sc >= 0 ? norm(row[ci.sc]) : '' })
      }
    }
    const plan = [...byKey.values()].sort((a, b) => b.setup - a.setup || b.qty - a.qty)
    let old = setupJobs
      .filter(j => j.source === 'OLD' && weekKey(String(j.week), j.year) === loadSel.w && j.type === loadSel.t)
      .sort((a, b) => b.jobs - a.jobs || b.qty - a.qty)
    // สัปดาห์ประวัติ: ไฟล์แผนปัจจุบันไม่มีสัปดาห์นั้นแล้ว → รายการ job มาจากบล็อกประวัติแทน
    // (เงื่อนไขเดียวกับที่แถบโหลดเติมเลขให้ ไม่งั้นแถบมีเลขแต่กดเข้ามาแล้วว่าง — รวมถึง
    //  สัปดาห์ขอบข้อมูลที่แถบไม่นับ การ์ดก็ต้องไม่ลิสต์ ไม่งั้นหัวการ์ดบอก 0 job แต่มีตารางยาว)
    if (!old.length && !plan.length && !histJobsByWeekType.edge.has(loadSel.w)) {
      old = historyItems
        .filter(b => weekKey(norm(b.week), b.year) === loadSel.w
          && classifyType(b.factory, b.cat) === loadSel.t && (Number(b.setup) || 0) > 0)
        .map(b => ({
          item: norm(b.item), mcg: norm(b.mc_group), gauge: norm(b.gauge),
          qty: Number(b.qty) || 0, setup: Number(b.setup) || 0, carry: Number(b.carry) || 0,
          mc: Number(b.mc) || 0, so: norm(b.so), days: Number(b.setup_days) || 0,
        }))
        .sort((a, b) => b.setup - a.setup || b.qty - a.qty)
    }
    return { plan, old }
  }, [loadSel, rows, ci, supported, setupJobs, historyItems, histJobsByWeekType])

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
  // งานที่เปิดการ์ดอยู่เป็นงานจ้างทอที่ถูกยุบเข้าแถวเครื่องในบ้านไหม — ถ้าใช่ ต้องปิดส่วนล็อก
  // (หัวแถวของมันเป็นเครื่องเจ้าบ้านแล้ว คีย์ล็อกจะไปทับล็อกของงานในบ้านผ้าตัวเดียวกัน)
  const selMerged = selIdx != null && !!mergedIdx && mergedIdx.has(selIdx)

  // ── สัปดาห์ทั้งหมดของ "งานเดียวกัน" — กล่องเลือกสัปดาห์ตอนกดล็อกในการ์ดงาน ──────
  // งานเดียวกัน = ITEM_CODE + MC_GROUP + MC_GUAGE ตรงกัน (คีย์ล็อกทุกส่วน ยกเว้นสัปดาห์)
  // ล็อก 1 ใบ = 1 สัปดาห์ (คีย์ไม่มี SO) → หลายแถวในสัปดาห์เดียวกันยุบเป็นบรรทัดเดียว:
  //   qty บวกกัน (ยอดของแต่ละ SC เป็นของจริงคนละก้อน)
  //   mc ต้อง max ไม่ใช่บวก — Planning.py เขียน ACTUAL_MC ซ้ำทุกแถวของเครื่องชุดเดียวกัน
  //   (กติกาเดียวกับ planMcAgg ; ถ้าบวกจะได้เลขเครื่องเป็นเท่าตัวไปเขียนในชีท Lock_Plan)
  const lockWeeks = useMemo(() => {
    if (!selRow || !onApplyLocks) return []
    const g = valOf(selRow)
    const item = g('ITEM_CODE'), mcg = g('MC_GROUP'), gauge = g('MC_GUAGE')
    const groupKey = lockKeyOf('', item, mcg, gauge, 'PLAN')
    const qtyI = ci.qty >= 0 ? ci.qty : ci.qtyAlt
    const mcI = ci.actualmc >= 0 ? ci.actualmc : ci.mcAlt
    const byWeek = new Map()
    for (const { row } of rows) {
      const w = rowWk(row); if (w === '') continue
      const k = lockKeyOf('', norm(row[ci.item]), ci.mcgroup >= 0 ? norm(row[ci.mcgroup]) : '',
        ci.gauge >= 0 ? norm(row[ci.gauge]) : '', 'PLAN')
      if (k !== groupKey) continue
      let e = byWeek.get(w)
      if (!e) byWeek.set(w, e = { week: w, qty: 0, mc: 0, so: new Set() })
      e.qty += qtyI >= 0 ? (Number(norm(row[qtyI])) || 0) : 0
      const mc = mcI >= 0 ? (Number(norm(row[mcI])) || 0) : 0
      if (mc > e.mc) e.mc = mc
      if (ci.sc >= 0) for (const s of String(norm(row[ci.sc])).split(/[,\s;]+/)) if (s) e.so.add(s)
    }
    // เรียงตามแกนสัปดาห์ของ Gantt (จัดลำดับข้ามปีไว้แล้ว) — สัปดาห์ที่ไม่อยู่บนแกนไว้หน้าสุด
    const ord = (w) => { const i = weeks.indexOf(String(w)); return i >= 0 ? i : weekSortKey(w) - 1e7 }
    return [...byWeek.values()].sort((a, b) => ord(a.week) - ord(b.week)).map(e => ({
      ...e, so: [...e.so].join(','),
      lock: lockMap.get(lockKeyOf(e.week, item, mcg, gauge, 'PLAN')) || null,
    }))
  }, [selRow, rows, ci, weeks, lockMap, onApplyLocks])

  // ── ผังบล็อกในช่อง (แถว × สัปดาห์) — คิดครั้งเดียวต่อการเปลี่ยนข้อมูล ─────────────
  // เดิมคิดสดในทุกช่องทุกครั้งที่เรนเดอร์ (สร้าง Map/Array ใหม่หลักหมื่นชุดต่อรอบ) = หน่วง
  // ค่าไม่ขึ้นกับ state ของการโต้ตอบเลย จึงยกมาไว้ใน useMemo ได้ทั้งก้อน
  const laidMap = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const r of gantRows) {
      const slots = rowSlots.get(r.key)
      for (const w of weeks) {
        const ck = r.key + '||' + w
        const jobs = cells.get(ck) || []
        const bks = bookingData.cells.get(ck) || []
        if (!jobs.length && !bks.length) continue
        const ents = [...jobs.map(j => ({ j })), ...bks.map((b, bi) => ({ b, bi }))]
        if (!slots || !slots.length) { m.set(ck, ents); continue }
        const pool = new Map()
        for (const e of ents) {
          const k = nkey(e.j ? e.j.item : e.b.item)
          if (!pool.has(k)) pool.set(k, [])
          pool.get(k).push(e)
        }
        const out = slots.map(k => {
          const q = pool.get(k)
          return q && q.length ? q.shift() : null
        })
        // บล็อกที่หา slot ไม่เจอ (ไม่ควรเกิด) — ต่อท้ายไว้ ดีกว่าหายไปจากหน้าจอ
        for (const q of pool.values()) for (const e of q) out.push(e)
        while (out.length && out[out.length - 1] == null) out.pop()
        // ยุบช่องว่างที่ติดกันเป็นก้อนเดียว ({ gap: n }) — ช่องส่วนใหญ่ในแผนว่างเป็นแถบยาว
        // ถ้าปล่อยเป็น div ละบรรทัดจะเกิด element ฟรี ๆ หลายพันตัว (ทำให้ mount/เลื่อนช้า)
        const packed = []
        let run = 0
        for (const e of out) {
          if (e == null) { run++; continue }
          if (run) { packed.push({ gap: run }); run = 0 }
          packed.push(e)
          // ก้อนที่ถูกกาง ▾ → แทรกบล็อกย่อยรายใบต่อท้ายทันที (ดันเฉพาะบล็อกที่อยู่ใต้มันในช่องนี้)
          const subs = e.j && e.j.subs
          if (!subs || subs.length < 2 || !expanded.has(ck + '||' + e.j.gkey)) continue
          for (const s of subs) packed.push({ j: s, sub: true })
        }
        m.set(ck, packed)
      }
    }
    return m
  }, [gantRows, weeks, cells, bookingData, rowSlots, supported, expanded])

  // idx ของแถวในชีท → คีย์แถว Gantt ที่บล็อกนั้นอยู่ — ใช้ส่ง selIdx/dragIdx/editIdx
  // ให้ "เฉพาะแถวที่เกี่ยวข้อง" แถวอื่นจึงไม่ต้องเรนเดอร์ใหม่ตอนคลิก/ลาก
  const rowOfIdx = useMemo(() => {
    const m = new Map()
    if (!supported) return m
    for (const { row, idx } of rows) m.set(idx, rowKey(row))
    return m
  }, [rows, groups, supported])

  // ── ประสิทธิภาพ: แถว Gantt เป็นคอมโพเนนต์ memo แยกตัว (ดู GanttRow ท้ายไฟล์) ──────
  // ตารางใหญ่หลักพัน–หมื่นช่อง ถ้าอยู่ใน JSX ก้อนเดียว การเปลี่ยน state เล็ก ๆ (คลิกบล็อก,
  // poll สถานะการรัน, พิมพ์ค้นหา) จะทำให้ React ไล่เทียบใหม่ทั้งตาราง = หน่วง
  //   ctxRef  = ข้อมูล+ฟังก์ชันที่แถวต้องใช้ เก็บใน ref → identity คงที่ ไม่ทำให้ memo หลุด
  //             (อัปเดตค่าใหม่ทุกเรนเดอร์ แถวที่วาดใหม่จึงอ่านค่าล่าสุดเสมอ)
  //   dataVer = ตัวชี้ว่า "ข้อมูลที่แถวใช้" เปลี่ยนจริง → identity เปลี่ยน = สั่งให้แถววาดใหม่
  //             ⚠️ เพิ่ม/แก้ค่าที่แถวอ่านจาก ctx เมื่อไร ต้องเพิ่มใน deps ชุดนี้ด้วย ไม่งั้นแถวค้างค่าเก่า
  const ctxRef = useRef({})
  const dataVer = useMemo(() => ({}), [
    cells, laidMap, rowSlots, bookingData, weeks, ava, planMcByWeekCat, rsvByCat,
    colorMap, colorKeys, lockMap, colIdx, groups, weekYears, nextYearWeeks,
    dyeCodes, scOutline, colorRows, hasColorPart, markLate, bookingAllWeeks, programMap,
    poolMap, catFilter, showRsv, lockBefore, onEditQty, outsourceIdx, outsourceItems, mergedIdx,
    freezeW,   // ช่องไหนวางงานได้ (c.isLocked) เปลี่ยนตามค่า freeze — แถวที่ memo ไว้ต้องวาดใหม่
  ])

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
  const visRows = useMemo(
    () => (loadFilter ? gantRows.filter(r => rowType.get(r.key) === loadFilter) : gantRows)
      .filter(r => !catFilter || catMatch(r.vals, catFilter)),
    [gantRows, rowType, loadFilter, catFilter])

  // ── virtualization: เรนเดอร์เฉพาะแถวที่อยู่ในจอ ─────────────────────────────
  // ชีท REPLAN + โหมด Item(ทั้งหมด) = ~350 แถว × 22 สัปดาห์ ≈ 7,700 ช่อง / 28,000 element
  // ทั้งที่กรอบ Gantt สูงแค่ 460px (เห็นจริง ~7 แถว) → ทุกครั้งที่เลื่อน เบราว์เซอร์ต้องจัดการ
  // ทั้งตาราง (วัดจริงแล้ว: เลื่อนแนวนอน 60 ก้าว = 153ms → เหลือ 21ms เมื่อเหลือ 30 แถว)
  //
  // ความสูงแถวคำนวณล่วงหน้าได้แม่นยำ เพราะบล็อกสูงคงที่ (ยืนยันด้วยการวัดจริง 349 แถว):
  //   แถวที่มีบล็อก n บรรทัด = CELL_PAD + n × SLOT_H ; แถวที่ไม่มีบล็อกเลย = EMPTY_ROW_H
  // → รู้ตำแหน่งทุกแถวโดยไม่ต้องเรนเดอร์ ใส่แถวเปล่า (spacer) แทนส่วนที่ไม่ได้วาด
  const rowTop = useMemo(() => {
    const a = new Array(visRows.length + 1)
    a[0] = 0
    for (let i = 0; i < visRows.length; i++) {
      const n = (rowSlots.get(visRows[i].key) || EMPTY_LAID).length
      a[i + 1] = a[i] + (n ? CELL_PAD + n * SLOT_H : EMPTY_ROW_H)
    }
    return a
  }, [visRows, rowSlots])

  // ช่วงแถวที่วาดอยู่ — อัปเดตตอนเลื่อน (คุมด้วย rAF ไม่ให้ยิงถี่เกินเฟรม)
  //
  // ⚠️ ต้องมี "เผื่อขอบ" (OVERSCAN) + อัปเดตเฉพาะตอนจอใกล้หลุดช่วงที่วาดไว้ (hysteresis)
  // ถ้าคำนวณช่วงใหม่ทุกครั้งที่เลื่อน จะต้องสร้าง/ทิ้งแถวทุกเฟรม — วัดแล้วช้ากว่าไม่ virtualize
  // ถึง 4 เท่า (เลื่อนแนวตั้ง 60 ก้าว: 703ms → 2931ms) เพราะการ mount แถวแพงกว่าการเลื่อนเฉย ๆ
  const scrollRef = useRef(null)
  const colHeadRef = useRef(null)
  const rafRef = useRef(0)
  const [range, setRange] = useState({ from: 0, to: 0, n: -1 })
  // ค้นหาแถวที่ตำแหน่ง y (พิกัดสะสมของ tbody) แบบไบนารี — เร็วพอแม้แถวเป็นพัน
  const rowAt = (y) => {
    let a = 0, b = visRows.length
    while (a < b) { const m = (a + b) >> 1; if (rowTop[m + 1] <= y) a = m + 1; else b = m }
    return a
  }
  const readRange = () => {
    const el = scrollRef.current
    if (!el) return
    // bodyTop = ความสูงหัวตารางที่ sticky ค้างอยู่ (หัวสัปดาห์ + แถบโหลด + หัวคอลัมน์)
    // แถวงานที่ "เห็นจริง" คือช่วงใต้ก้อนนั้นลงมา
    const bodyTop = colHeadTop + (colHeadRef.current ? colHeadRef.current.offsetHeight : 0)
    const n = visRows.length
    const lo = el.scrollTop
    const s = rowAt(lo)
    const e = Math.min(n, rowAt(lo + Math.max(0, el.clientHeight - bodyTop)) + 1)
    // ⚠️ ห้ามหุ้มด้วย startTransition — วัดแล้วเลื่อนลื่นพอกัน แต่ถ้าเลื่อนเร็วกว่าที่ React
    // วาดทัน จะเห็น "แถวว่าง" วูบขึ้นมา ซึ่งบนหน้าแผนอ่านผิดเป็นว่างานหายไป
    setRange(cur => {
      // ยังอยู่ลึกในช่วงที่วาดไว้ (ห่างขอบเกิน EDGE แถว) → ไม่ต้องวาดใหม่เลย
      const safe = cur.n === n
        && (cur.from === 0 || s - cur.from > EDGE)
        && (cur.to === n || cur.to - e > EDGE)
      return safe ? cur : { from: Math.max(0, s - OVERSCAN), to: Math.min(n, e + OVERSCAN), n }
    })
  }
  // ── ตรึงตำแหน่งที่มองอยู่เมื่อข้อมูลเปลี่ยนจากนอกจอ ───────────────────────────
  // อีกคนแยกงาน/ลบแถวอยู่คนละจอ → จำนวนแถวเหนือจุดที่เรามองอยู่เปลี่ยน ถ้าปล่อยตามธรรมชาติ
  // เบราว์เซอร์คง scrollTop ไว้เท่าเดิม = ของที่จ้องอยู่เลื่อนหนีขึ้น/ลงเอง (อ่านผิดว่างานหาย)
  // จึงจำ "แถวบนสุดที่เห็น + เลยขอบไปกี่ px" ไว้ แล้วดึง scroll กลับให้ตรงเดิมหลังข้อมูลเปลี่ยน
  // — ทำใน useLayoutEffect = เสร็จก่อนเบราว์เซอร์วาด จึงไม่มีจังหวะกระตุกให้เห็น
  const anchorRef = useRef(null)
  // เลื่อนเพื่อดันเนื้อหาขึ้น d พิกเซล — ใช้กรอบ Gantt ก่อน ที่เหลือให้หน้าเว็บช่วยรับ
  //
  // ต้องมีทั้งสองทาง เพราะตอนกรอบเลื่อนอยู่สุดบน (scrollTop 0) แล้วแถบเหนือกรอบสูงขึ้น
  // (ปุ่มตัวกรองโผล่/หาย แล้วแถบขึ้นบรรทัดใหม่) กรอบจะรับการชดเชยไม่ได้เลย — ต้องให้หน้าเลื่อนแทน
  // ทั้งหมดนี้อยู่ในตัวตรึงตัวเดียว จึงไม่มีตัวไหนไปแย่งแก้ของอีกตัว
  const holdAt = (el, d) => {
    if (Math.abs(d) <= 1) return
    const max = Math.max(0, el.scrollHeight - el.clientHeight)
    const want = Math.min(max, Math.max(0, el.scrollTop + d))
    const rest = d - (want - el.scrollTop)
    el.scrollTop = want
    if (Math.abs(rest) <= 1) return
    for (let p = el.parentElement; p; p = p.parentElement) {
      const oy = getComputedStyle(p).overflowY
      if ((oy === 'auto' || oy === 'scroll') && p.scrollHeight > p.clientHeight) { p.scrollTop += rest; return }
    }
    const doc = document.scrollingElement || document.documentElement
    doc.scrollTop += rest
  }

  // ⚠️ จุดอ้างอิงวัดเป็น "ตำแหน่งบนหน้าจอ" (viewport) ไม่ใช่ตำแหน่งในกรอบ
  // เพราะของที่เปลี่ยนไม่ได้มีแต่แถวในตาราง — แถบตัวกรอง/แถบเตือนเหนือกรอบก็สูงขึ้น-เตี้ยลงได้
  // (เช่นข้อความในแถบขึ้นบรรทัดใหม่) แล้วดันทั้งกรอบลงมา ถ้าตรึงแค่ "ในกรอบ" สิ่งที่ตาจ้องอยู่
  // ก็ยังขยับตามกรอบอยู่ดี ; ยึดพิกัดหน้าจอจึงครอบคลุมทั้งสองกรณีด้วยตัวเดียว
  const rowYOf = (el, key) => {
    const tr = el.querySelector(`tbody tr[data-rk="${CSS.escape(key)}"]`)
    return tr ? tr.getBoundingClientRect().top : null
  }
  // จำหลายแถวสำรองไว้ (ไม่ใช่แถวเดียว) เพราะแถวที่ยึดไว้อาจถูกคนอื่นลบ/ย้ายหายไปจากจอพอดี
  // แล้วจะไม่เหลืออะไรให้เทียบ — จอก็เลื่อนหนีโดยไม่มีใครดึงกลับ
  const ANCHOR_KEEP = 4
  const rememberAnchor = () => {
    const el = scrollRef.current
    if (!el || !visRows.length) return
    // แถวบนสุดที่ยัง "โผล่พ้น" หัวตารางที่ sticky ค้างอยู่ = แถวที่สายตาจับอยู่
    const bodyTop = el.getBoundingClientRect().top + colHeadTop
      + (colHeadRef.current ? colHeadRef.current.offsetHeight : 0)
    const list = []
    for (const tr of el.querySelectorAll('tbody tr[data-rk]')) {
      const r = tr.getBoundingClientRect()
      if (r.bottom <= bodyTop) continue
      list.push({ key: tr.dataset.rk, off: r.top })
      if (list.length >= ANCHOR_KEEP) break
    }
    if (list.length) anchorRef.current = list
  }
  const onScroll = () => {
    // ⚠️ จำตำแหน่ง "ทันที" ไม่หน่วงด้วย rAF และต้องทำงานทุกโหมด (เดิมผูกกับ onScroll ที่ต่อเฉพาะ
    // ตอน virt ตารางแถวน้อยจึงไม่เคยอัปเดตจุดอ้างอิง แล้วดีดจอกลับบนสุดตอนข้อมูลเปลี่ยน)
    // ถ้าหน่วง แล้วมีการวาดใหม่แทรกกลางคัน ตัวตรึงจะดึงจอกลับไปจุดก่อนที่ผู้ใช้เพิ่งเลื่อนเอง
    rememberAnchor()
    if (rafRef.current || !virt) return
    rafRef.current = requestAnimationFrame(() => { rafRef.current = 0; readRange() })
  }
  useEffect(() => () => cancelAnimationFrame(rafRef.current), [])

  // ผู้ใช้เลื่อน "ทั้งหน้า" (ไม่ใช่เลื่อนในกรอบ) — กรอบไม่ได้ยิง event ให้รู้ ต้องมาอัปเดตจุดอ้างอิงเอง
  // ไม่งั้นการวาดใหม่ครั้งถัดไปจะเข้าใจว่าจอเลื่อนเพราะข้อมูลเปลี่ยน แล้วดึงกลับไปจุดเดิม
  useEffect(() => {
    const on = () => rememberAnchor()
    window.addEventListener('scroll', on, { capture: true, passive: true })
    return () => window.removeEventListener('scroll', on, { capture: true })
  })

  // ตรวจทุกครั้งที่วาดใหม่ (ไม่ผูกกับ deps ชุดใดชุดหนึ่ง) เพราะของที่ดันจอให้ขยับมีหลายทาง
  // และมาคนละจังหวะ: แถวในตาราง, แถบโหลดบนหัวตาราง, แถบตัวกรอง/แถบเตือนเหนือกรอบที่ขึ้น
  // บรรทัดใหม่เมื่อข้อมูลเปลี่ยน — ผูกไว้กับชุดใดชุดหนึ่งจะพลาดทางที่เหลือ แล้วจอขยับให้เห็น
  useLayoutEffect(() => {
    const el = scrollRef.current
    const list = anchorRef.current
    if (!el || !list) { rememberAnchor(); return }
    // จุดตั้งต้นของพิกัด rowTop บนหน้าจอ — วัดจากแถวจริงที่วาดอยู่สักแถว (rowTop[j] ↔ ตำแหน่งจริง)
    // ห้ามคิดจากความสูง thead เอาเอง เพราะแถบสรุปโหลดที่ sticky อยู่เป็น <tr> ใน tbody ไม่ใช่ thead
    // (คิดผิดไป ~110px = ดีดจอขึ้นไปบนสุด)
    let originY = null
    const any = el.querySelector('tbody tr[data-rk]')
    if (any) {
      const j = visRows.findIndex(r => r.key === any.dataset.rk)
      if (j >= 0) originY = any.getBoundingClientRect().top - rowTop[j]
    }
    for (const a of list) {
      let now = rowYOf(el, a.key)
      if (now == null) {
        // แถวนั้นยังไม่ถูกวาด (โหมด virtual เพิ่งรีเซ็ตช่วงแถวเพราะจำนวนแถวเปลี่ยน) →
        // คิดตำแหน่งจาก rowTop แทน ซึ่งโหมดนี้แม่นอยู่แล้ว เพราะความสูงของแถวเว้นระยะมาจากชุดนี้
        const i = visRows.findIndex(r => r.key === a.key)
        if (i < 0 || originY == null) continue      // แถวนี้หายไปจริง → ลองตัวสำรองถัดไป
        now = originY + rowTop[i]
      }
      holdAt(el, now - a.off)
      break
    }
    // ไม่เหลือแถวสำรองสักตัว (ถูกกรองออก/ลบหมด) = ไม่มีอะไรให้ยึด ปล่อยไว้ ดีกว่าเดาแล้วเลื่อนมั่ว
    rememberAnchor()
  })

  // ความกว้างคอลัมน์ที่ "วัดจากของจริง" — จำเป็นเพราะ virtualize แล้วตาราง auto-layout
  // จะคิดความกว้างจากเฉพาะแถวที่วาด → คอลัมน์ขยับตอนเลื่อน (ตาลาย)
  // จึงเรนเดอร์ครบ 1 รอบเพื่อวัด แล้วตรึงด้วย <colgroup> + table-layout: fixed
  // sig ไม่รวม rows → ลาก/แก้จำนวนแล้วไม่ต้องกลับไปเรนเดอร์ครบใหม่
  const widthSig = [sheet, weeks.join(','), groups.map(g => g.col + ':' + g.width).join(','),
    showRsv, Object.keys(barFields).filter(k => barFields[k]).sort().join(',')].join('|')
  const [colPx, setColPx] = useState(null)
  const needVirt = visRows.length > VIRT_MIN_ROWS
  const nHeadCols = groups.length + (showRsv ? 1 : 0)
  // คีย์ของค่าที่เก็บไว้ใน localStorage — เติม "จำนวนแถว" เข้าไปด้วย เพราะข้อมูลคนละชุด
  // (ไฟล์แผนใหม่) ความกว้างธรรมชาติอาจไม่เท่ากัน ; ไม่เอาไปใส่ใน widthSig เพราะ split/ลบแถว
  // จะทำให้ต้องกลับไปเรนเดอร์ครบเพื่อวัดใหม่ทุกครั้ง
  const widthKey = widthSig + '|' + rows.length
  // ค่าที่เคยวัดไว้รอบก่อน (คนละ session ก็ใช้ได้) — ต้องอ่านตั้งแต่ตอนเรนเดอร์ ไม่ใช่ใน effect
  // ไม่งั้นรอบแรกจะเรนเดอร์ครบทุกแถวไปแล้ว (ค้าง ~2 วิ) กว่า effect จะทำงาน
  const cachedPx = (needVirt && (!colPx || colPx.sig !== widthSig))
    ? loadColW(widthKey, nHeadCols, weeks.length) : null
  const colNow = (colPx && colPx.sig === widthSig) ? colPx
    : (cachedPx ? { sig: widthSig, ...cachedPx } : null)
  const virt = needVirt && colNow != null
  // เก็บค่าที่หยิบจาก cache ลง state ด้วย เพื่อไม่ต้องอ่าน localStorage ซ้ำทุกเรนเดอร์
  useEffect(() => { if (cachedPx) setColPx({ sig: widthSig, ...cachedPx }) }, [cachedPx])
  // เพิ่มค่าเมื่อหน้าที่ถูกซ่อนกลับมาแสดง → สั่งให้วัดใหม่
  const [measureTick, setMeasureTick] = useState(0)
  useLayoutEffect(() => {
    if (!needVirt || colNow) return
    const el = gridRef.current
    if (!el) return
    const head = [...el.querySelectorAll('.gantt-colhead th')]        // คอลัมน์หัวแถวซ้าย (+กันเครื่อง)
    const wk = [...el.querySelectorAll('thead th.gantt-wk')]          // คอลัมน์สัปดาห์
    if (!wk.length) return
    const px = (t) => Math.ceil(t.getBoundingClientRect().width)
    const val = { group: head.map(px), week: wk.map(px) }
    // หน้าแผนถูกซ่อนด้วย display:none ตอนอยู่แท็บอื่น (App.jsx) แต่ยังโหลดข้อมูลเบื้องหลัง
    // → วัดได้ 0 ทุกคอลัมน์ ถ้าตรึง/เก็บค่านี้ ตารางจะกว้าง 0 ทับกันหมด
    // จึงยังไม่ตรึง (ตารางวาดแบบ auto ไปก่อน) แล้วรอให้หน้าแสดงจริงค่อยวัดใหม่
    if (![...val.group, ...val.week].every(w => w > 0)) {
      if (typeof ResizeObserver === 'undefined') return
      const ro = new ResizeObserver(() => {
        if (el.getBoundingClientRect().width > 0) setMeasureTick(t => t + 1)
      })
      ro.observe(el)
      return () => ro.disconnect()
    }
    saveColW(widthKey, val)
    setColPx({ sig: widthSig, ...val })
  }, [widthSig, needVirt, colNow, measureTick])

  // ตั้งช่วงแถวรอบแรก (และเมื่อจำนวนแถว/ขนาดหน้าต่างเปลี่ยน) — ตอนเลื่อนใช้ onScroll
  useLayoutEffect(() => {
    if (!virt) return
    readRange()
    const onResize = () => readRange()
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [virt, colHeadTop, visRows.length])

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
    setBkSel(null)   // การ์ด History แผนเดิมใช้ตำแหน่งเดียวกัน — เปิดของงานแผนต้องปิดใบนั้นก่อน
    clickTimer.current = setTimeout(() => setSelIdx(cur => (cur === j.idx ? null : j.idx)), 200)
  }
  function onBarDblClick(j, locked, expKey) {
    clearTimeout(clickTimer.current)
    // ก้อนที่ยุบหลายใบ: แก้จำนวนตรง ๆ ไม่ได้ (ไม่รู้ว่าจะลง/หักใบไหน) → กางรายใบให้แทน
    if (j.subs && j.subs.length > 1) { toggleExpand(expKey); return }
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
    markOver(null)
    // ก้อนที่ยุบหลายใบสั่งส่ง idx มาเป็นรายการคั่นด้วย , — ต้องย้ายทุกใบพร้อมกันในครั้งเดียว
    // (เรียกทีละใบไม่ได้ ผู้เรียกอ่าน grid ตัวเดิมทุกครั้ง → ใบหลังทับผลของใบแรก)
    const ids = String(e.dataTransfer.getData('text/plain')).split(',')
      .map(Number).filter(n => Number.isFinite(n))
    setDragIdx(null)
    if (!ids.length) return
    // MC_GROUP ปลายทาง = เครื่องของแถวที่ปล่อยลง (ใช้เช็คว่าย้ายข้ามเครื่องไหม)
    const targetMc = mcGroupI >= 0 ? targetRow.vals[mcGroupI] : null
    // ปีของคอลัมน์ปลายทาง — ผู้เรียกต้องเขียน PLAN_YEAR ด้วย (pipeline คีย์สัปดาห์ด้วยปี+สัปดาห์)
    // คีย์ติดปีบอกปีเอง ; เลขเปล่าใช้คำตอบของแกน (isNextYearW รู้ปีจริงจาก PLAN_YEAR ของแถวในคอลัมน์)
    onMoveWeek(ids.length === 1 ? ids[0] : ids, week, targetMc, keyYear(week, isNextYearW(week)))
  }

  // ช่วงสัปดาห์ย้อนหลัง (freeze) แบบต่อเนื่อง — ใช้กางแถบ "History" คาดไว้เหนือหัวสัปดาห์
  // แบ่งเป็นช่วง ๆ แทนการเดาว่า "ล็อกทั้งหมดอยู่ซ้ายสุด" เพราะสัปดาห์ปีหน้า (เลขเล็กที่วนข้ามปี)
  // ไม่ถูกล็อก แต่ถูก sort มาอยู่ซ้ายได้ → ช่วงล็อกอาจไม่ติดกัน
  const weekRuns = []
  for (const w of weeks) {
    const lk = isLocked(w)
    const last = weekRuns[weekRuns.length - 1]
    if (last && last.locked === lk) { last.len++; last.to = w }
    else weekRuns.push({ locked: lk, len: 1, from: w, to: w })
  }

  // ข้อมูล+ฟังก์ชันทั้งหมดที่ <GanttRow> ต้องใช้ — เขียนทับทุกเรนเดอร์ (identity ของ ref คงที่)
  // แถวที่ถูกสั่งวาดใหม่จึงอ่านค่าล่าสุดเสมอ ส่วนแถวที่ props ไม่เปลี่ยนจะถูกข้ามไป
  ctxRef.current = {
    groups, groupsW, showRsv, catFilter, catClickable, catMatch, clickGroupCell,
    rsvOfRow, avaCatI, avaGaugeI, mcGroupI, weeks, laidMap, ava, planMcByWeekCat,
    poolKeyOf, isLocked, isNextYearW, markOver, clearOver, onDrop, setAvaSel, setBkSel,
    setSelIdx, lockMap, bkProgTeam, dyeCodes, scOutline, dyedOf, colorOf, colorRows, valOf,
    planLockOf, onBarClick, onBarDblClick, commitQty, setEditVal, setEditIdx,
    setDragIdx, onEditQty, markLate, bookingAllWeeks, historyFrom,
    outsourceIdx, outsourceItems, mergedIdx, onOpenItem, expanded, toggleExpand,
  }

  // ── ช่วงแถวที่ต้องวาดจริง ─────────────────────────────────────────────────
  // virt = ตรึงความกว้างคอลัมน์ไว้แล้ว → วาดเฉพาะแถวในจอ (+เผื่อ OVERSCAN) ที่เหลือแทนด้วย
  // แถวเปล่าความสูงเท่ายอดรวม เพื่อให้แถบเลื่อนและตำแหน่งทุกแถวเท่าเดิมเป๊ะ
  const nRows = visRows.length
  let rowFrom = 0, rowTo = nRows
  if (virt) {
    // range.n ยังไม่ตรง = จำนวนแถวเพิ่งเปลี่ยน (กรอง/ลากงาน) — useLayoutEffect จะตั้งช่วงใหม่
    // ให้ก่อนวาดจริง ระหว่างนั้นวาดจากบนสุดไว้ก่อน กันแถวหายชั่วขณะ
    rowFrom = range.n === nRows ? Math.min(range.from, nRows) : 0
    rowTo = range.n === nRows ? Math.min(range.to, nRows) : Math.min(nRows, OVERSCAN * 2)
  }
  const nCols = groups.length + (showRsv ? 1 : 0) + weeks.length

  return (
    <div className="gantt">
      <div className="gantt-scroll" ref={scrollRef} onScroll={onScroll}>
        <table className="gantt-grid" ref={gridRef}
          style={virt ? { tableLayout: 'fixed', width: [...colNow.group, ...colNow.week].reduce((s, w) => s + w, 0) } : undefined}>
          {/* ตรึงความกว้างคอลัมน์ตอน virtualize — ไม่งั้น auto-layout คิดจากเฉพาะแถวที่วาด
              แล้วคอลัมน์จะกว้าง/แคบสลับไปมาระหว่างเลื่อน */}
          {virt && (
            <colgroup>
              {colNow.group.map((w, i) => <col key={'g' + i} style={{ width: w }} />)}
              {colNow.week.map((w, i) => <col key={'w' + i} style={{ width: w }} />)}
            </colgroup>
          )}
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
              {weeks.map(w => {
                const lk = isLocked(w)
                // คอลัมน์ประวัติของปีก่อนต้องเห็นปีที่หัวคอลัมน์ ไม่งั้น W45 '25 กับ W45 ปีนี้
                // แยกกันไม่ออกด้วยตา (แกนเดียวมีได้ทั้งสอง)
                // คอลัมน์ไกลเกิน 1 ปีข้างหน้า ('45@2028') ก็ต้องเห็นปีเหมือนกัน
                const py = wkTagYear(w)
                return (
                  <th key={w} className={'gantt-wk' + (lk ? ' locked' : '')}
                    title={lk ? `W${wkNum(w)}${py ? ` ปี ${py}` : ''} — สัปดาห์ย้อนหลัง (freeze) : ดูได้ แต่ลาก/แก้ไม่ได้` : undefined}>
                    W{wkNum(w)}{py != null && <span className="gwk-year">'{String(py).slice(-2)}</span>}
                    {lk && <span className="gwk-back">🔒 ย้อนหลัง</span>}
                  </th>
                )
              })}
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
                  // ใหม่(live) = job จาก booking (คงที่) + ผลรวม NEW_MC ของแถวที่วางจริง (ขยับตามการลาก)
                  const nw = Math.max(0, (info.bookingNew || 0) + (planNewByWeekType[w + '|' + t.key] || 0))
                  // สัปดาห์ประวัติ (ไฟล์แผนปัจจุบันไม่มีข้อมูลของสัปดาห์นั้นเลย) → เอา job setup
                  // จากบล็อกประวัติมาแสดงแทน ; สัปดาห์ที่ไฟล์แผน/booking มีข้อมูลอยู่แล้วไม่แตะ
                  // เพราะจะนับซ้ำกับ old/bookingNew ที่มาจากไฟล์เดียวกัน
                  const histEdge = histJobsByWeekType.edge.has(w)
                  const histJobs = ((info.old || 0) + nw) === 0 && !histEdge
                    ? (histJobsByWeekType.jobs[w + '|' + t.key] || 0) : 0
                  const old = (info.old || 0) + histJobs
                  const total = old + nw
                  const hasCap = cap != null && cap !== ''
                  const over = hasCap && total > cap
                  const empty = old === 0 && nw === 0 && !hasCap
                  const free = hasCap ? cap - total : null
                  const tip = `${t.label} • สัปดาห์ ${wkLabel(w)}\nแผนเดิม ${old} + ใหม่ ${nw} = ใช้ ${total}`
                    + (histJobs > 0 ? `\n(${histJobs} job มาจากบล็อกประวัติที่ดึงมา — ไฟล์แผนปัจจุบันไม่มีสัปดาห์นี้แล้ว)` : '')
                    + (histEdge ? '\n⚠ สัปดาห์ขอบของชุดข้อมูลที่ดึงมา — ไม่มีสัปดาห์ก่อนหน้าให้เทียบว่าเครื่องไหนวิ่งต่อ'
                      + '\nจึงแยก setup/carry ไม่ได้ และไม่นับ job ของสัปดาห์นี้ (ดึงย้อนไปอีกจะได้ตัวเลขจริง)' : '')
                    + (hasCap ? ` / ทั้งหมด ${cap}\n${over ? `⚠ เกินเครื่องที่มี ${total - cap}` : `ว่าง ${free}`}` : '')
                    + '\n👆 คลิกเพื่อดูว่า job ถูกใช้ไปกับ item ไหนบ้าง'
                  return (
                    <td key={w} style={{ top: loadTops[ti] }}
                      className={'gantt-load-cell clickable' + (isLocked(w) ? ' locked' : '')}
                      onClick={() => setLoadSel(s => (s && s.w === w && s.t === t.key ? null : { w, t: t.key }))}>
                      {empty ? (
                        <div className="loadwrap" title={`${t.label} • สัปดาห์ ${wkLabel(w)}\nไม่มีงาน\n👆 คลิกเพื่อดูรายละเอียด`}>
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
            <tr className="gantt-colhead" ref={colHeadRef}>
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
                  title="เครื่องที่กันไว้ให้งาน POLY / COTTON ของกลุ่ม CAT|Guage นี้ (ใช้แทนงานปกติไม่ได้)">
                  🔒{RSV_COL.label}
                </th>
              )}
              {/* ช่องว่างแถวหัวคอลัมน์ = ที่วางป้าย "History (ย้อนหลัง N สัปดาห์)" กางคร่อมช่วงสัปดาห์
                  ย้อนหลัง (freeze) — บอกช่วงจากข้างนอก แทนการลงลาย/สีในช่อง Gantt
                  แบ่งเป็นช่วง (weekRuns) แทน 1 ช่องต่อสัปดาห์ เพื่อ colSpan คร่อมได้ */}
              {weekRuns.map((s, i) => (
                <td key={'ch' + i} colSpan={s.len} style={{ top: colHeadTop }}
                  className={'gantt-colhead-cell' + (s.locked ? ' hist' : '')}>
                  {s.locked && (
                    <span className="ghb-pill"
                      title={`สัปดาห์ย้อนหลัง ${wkLabel(s.from)}–${wkLabel(s.to)} — โชว์ History แผนเดิมได้ แต่ลาก/แก้ไม่ได้`}>
                      History (ย้อนหลัง {s.len} สัปดาห์)
                    </span>
                  )}
                </td>
              ))}
            </tr>
            {/* แถวเปล่าแทนแถวที่อยู่เหนือจอ — สูงเท่าของจริง แถบเลื่อนจึงไม่กระตุก */}
            {rowFrom > 0 && (
              <tr className="gantt-vspacer" style={{ height: rowTop[rowFrom] }}><td colSpan={nCols} /></tr>
            )}
            {/* แถวงาน — คอมโพเนนต์ memo แยกตัว (ดู GanttRow ท้ายไฟล์)
                ver/selIdx/dragIdx/editIdx ส่งเฉพาะที่เกี่ยวกับแถวนั้น → คลิก/ลาก/แก้จำนวน
                วาดใหม่แค่แถวที่เกี่ยว ไม่ใช่ทั้งตาราง */}
            {visRows.slice(rowFrom, rowTo).map(r => {
              const editHere = rowOfIdx.get(editIdx) === r.key
              return (
                <GanttRow key={r.key} r={r} ctx={ctxRef} ver={dataVer}
                  selIdx={rowOfIdx.get(selIdx) === r.key ? selIdx : null}
                  dragIdx={rowOfIdx.get(dragIdx) === r.key ? dragIdx : null}
                  editIdx={editHere ? editIdx : null}
                  editVal={editHere ? editVal : ''}
                  bkSelKey={bkSel && bkSel.key.startsWith(r.key + '||') ? bkSel.key : null} />
              )
            })}
            {/* แถวเปล่าแทนแถวที่อยู่ใต้จอ */}
            {rowTo < nRows && (
              <tr className="gantt-vspacer" style={{ height: rowTop[nRows] - rowTop[rowTo] }}><td colSpan={nCols} /></tr>
            )}
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
              {/* โหมดแยกสีตามสถานะให้สี: ★ ไม่ได้คู่กับบล็อกสีส้มแล้ว → อธิบายที่ legend สีบล็อกแทน */}
              {/* คาดแดง "ไม่ทัน Target Knit" มีแค่หน้าแผน (markLate) → หน้าอื่นไม่ต้องอธิบาย */}
              {BAR_MARKS.filter(m => !(dyeCodes && m.key === 'color')).filter(m => markLate || m.key !== 'late').map(m => (
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
            {/* โหมดแยกสีตามสถานะให้สี (dyeCodes): legend ย้ายขึ้นไปบนสุดของ Gantt แล้ว
                และสีตาม CAT/เกจ ไม่ถูกใช้ในโหมดนั้น จึงไม่ต้องมี legend ของมันที่นี่ */}
            {!dyeCodes && colorCols.length > 0 && (
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
                      <i style={{ background: barFill(colorOf(k)), borderColor: barEdge(colorOf(k)) }} />{k}
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
          outsource={!!outsourceIdx && outsourceIdx.has(selIdx)} onBringHome={onBringHome}
          outsourceChoices={outsourceChoices} onSendOutsource={onSendOutsource}
          showOther={showOther} onToggleOther={() => setShowOther(s => !s)}
          progTeam={programRows[selIdx] || ''}
          siblings={siblingsByIdx.get(selIdx) || null} onPickSibling={setSelIdx}
          lock={selMerged ? null : planLockOf(selRow)} onToggleLock={selMerged ? null : onToggleLock}
          lockWeeks={selMerged ? EMPTY_LAID : lockWeeks} onApplyLocks={selMerged ? null : onApplyLocks}
          mergedS9={selMerged} yarnLocks={yarnLocks}
          onClose={() => setSelIdx(null)} />
      )}

      {bkSel && (
        <BookingPanel b={bkSel.b} week={bkSel.w} vals={bkSel.vals} groups={groups}
          dyed={dyeCodes ? dyeCodes.has(nkey(bkSel.b.item)) : null}
          core={bkSel.b.core === true} progTeam={bkProgTeam(bkSel.b.item, bkSel.b.team)}
          lock={lockMap.get(lockKeyOf(bkSel.w, bkSel.b.item, bkSel.b.mc_group, bkSel.b.gauge)) || null}
          onToggleLock={onToggleLock}
          onClose={() => { setBkSel(null); onOpenItem?.(null) }} />
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
        // สัปดาห์ประวัติ → job มาจากบล็อกประวัติ (เงื่อนไข/สูตรเดียวกับแถบโหลด)
        const histJobs = ((info.old || 0) + nw) === 0 && !histJobsByWeekType.edge.has(loadSel.w)
          ? (histJobsByWeekType.jobs[loadSel.w + '|' + loadSel.t] || 0) : 0
        return (
          <LoadPanel week={loadSel.w} type={t} cap={cap} old={(info.old || 0) + histJobs} nw={nw}
            bookingNew={info.bookingNew || 0}
            plan={loadDetail.plan} oldJobs={loadDetail.old}
            onClose={() => setLoadSel(null)} />
        )
      })()}
    </div>
  )
}

/**
 * 1 แถวของ Gantt (เครื่อง/เกจ/CAT × item) — แยกเป็นคอมโพเนนต์ memo เพื่อประสิทธิภาพ
 *
 * ตาราง Gantt ใหญ่หลักพัน–หมื่นช่อง ถ้าอยู่ใน JSX ก้อนเดียวกับ PlanGantt ทุกการเปลี่ยน state
 * (คลิกบล็อก, poll สถานะการรันทุก 2 วิ, พิมพ์ค้นหา) จะทำให้ React ไล่เทียบใหม่ทั้งตาราง = หน่วง
 *
 * props ที่ทำให้แถววาดใหม่ มีแค่ของที่ "เกี่ยวกับแถวนี้จริง ๆ" เท่านั้น:
 *   ver      — identity เปลี่ยนเมื่อชุดข้อมูลของ Gantt เปลี่ยน (ดู dataVer ใน PlanGantt)
 *   selIdx / dragIdx / editIdx — ส่งค่ามาเฉพาะเมื่อ "บล็อกนั้นอยู่ในแถวนี้" ไม่งั้นเป็น null
 *   bkSelKey — คีย์บล็อก History ที่เปิดการ์ดอยู่ (เฉพาะที่อยู่ในแถวนี้)
 *   ctx      — ref ที่ถือข้อมูล/ฟังก์ชันร่วม (identity คงที่ ค่าอัปเดตทุกเรนเดอร์ของตัวแม่)
 */
const GanttRow = React.memo(function GanttRow({ r, ctx, selIdx, dragIdx, editIdx, editVal, bkSelKey }) {
  const c = ctx.current
  const groups = c.groups
  // แถวของงานจ้างทอ — Category ขึ้นต้น "S9-" (ผู้เรียกสลับหัวแถวให้เป็นพูลของผู้รับจ้าง)
  // ย้อมหัวแถวโทนเดียวกับบล็อก ให้เห็นได้ทันทีว่ากลุ่มนี้เป็นเครื่องนอกโรงงาน
  const s9Row = String(r.vals[0] ?? '').startsWith(S9_CAT_PREFIX)
  return (
    // data-rk = คีย์แถว ใช้หาแถวเดิมใน DOM ตอนตรึงตำแหน่งที่มองอยู่ (ดู rememberAnchor)
    <tr data-rk={r.key}>
      {groups.map((g, n) => {
        // ช่องที่ตรงกับตัวกรองปัจจุบัน → ไฮไลต์ให้เห็นว่ากรองด้วยค่าไหน (ทุกช่องใน prefix)
        const on = c.catFilter && n <= c.catFilter.upto && c.catMatch(r.vals, c.catFilter)
        return (
          <th key={g.col}
            className={'gantt-glabel'
              + (!c.showRsv && n === groups.length - 1 ? ' gantt-glast' : '')
              + (c.catClickable ? ' gantt-gclick' : '')
              + (s9Row ? ' s9-row' : '')
              + (on ? ' gantt-gfilter-on' : '')}
            onClick={c.catClickable ? () => c.clickGroupCell(r.vals, n) : undefined}
            title={c.catClickable
              ? `คลิกเพื่อกรองถึงคอลัมน์ "${g.label}" (${groups.slice(0, n + 1).map(x => x.label).join(' + ')}) — คลิกซ้ำเพื่อล้าง`
              : undefined}
            style={{ left: g.left, width: g.width, minWidth: g.width }}>
            {r.vals[n]}
          </th>
        )
      })}
      {c.showRsv && (() => {
        const rv = c.rsvOfRow(r.vals)
        return (
          <th className="gantt-glabel gantt-glast gantt-rsvcol"
            style={{ left: c.groupsW, width: RSV_COL.width, minWidth: RSV_COL.width }}
            title={rv
              ? `เครื่องกันไว้ของ ${r.vals[c.avaCatI]} Guage ${r.vals[c.avaGaugeI]} (ใช้แทนงานปกติไม่ได้)\n`
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
      {c.weeks.map(w => {
        // ผังบล็อกในช่องคิดไว้แล้วที่ laidMap (แถว × สัปดาห์) — ที่นี่แค่หยิบมาวาด
        const laid = c.laidMap.get(r.key + '||' + w) || EMPTY_LAID
        const avaKey = c.avaCatI >= 0 && c.avaGaugeI >= 0
          ? c.poolKeyOf(r.vals[c.avaCatI], r.vals[c.avaGaugeI], c.mcGroupI >= 0 ? r.vals[c.mcGroupI] : '')
          : null
        const av = avaKey && c.ava[w] ? c.ava[w][avaKey] : null
        // เครื่องที่แผนจองเพิ่ม แยกเป็น ปกติ / POLY / COTTON
        const planSlot = c.planMcByWeekCat[w + '@@' + avaKey] || null
        const planNormal = planSlot ? planSlot.normal : 0
        const planPoly = planSlot ? planSlot.poly : 0
        const planCotton = planSlot ? planSlot.cotton : 0
        // เครื่องว่าง live = เครื่องว่างหลัง booking − เครื่องที่แผนจองเพิ่ม (เฉพาะงานปกติ)
        //   งาน POLY/COTTON ไม่หักตรงนี้ เพราะกินเครื่องที่กันไว้ (ชิป 🔒) ต่างหาก
        const remainLive = av ? av.remain - planNormal : null
        const locked = c.isLocked(w)
        return (
          <td key={w} data-w={w}
            className={'gantt-cell' + (locked ? ' locked' : '')}
            onDragOver={locked ? undefined : e => { e.preventDefault(); c.markOver(w) }}
            onDragLeave={locked ? undefined : () => c.clearOver(w)}
            onDrop={locked ? undefined : e => c.onDrop(e, w, r)}>
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
              // เครื่องที่ user กันไว้ในชีท Lock_MC (backend หักออกจาก av.remain มาแล้ว)
              // ⚠️ Lock_MC คีย์ด้วย (สัปดาห์, CAT, เกจ) ไม่ใช่พูล → พูลอื่นของเกจเดียวกันถูกหัก
              // ไปด้วย (เช่น ล็อก SKP 11 เครื่อง แต่พูล SKPTA ที่มี 2 เครื่องก็โดนหัก 11)
              // จึงต้องบอกทั้งจำนวนและกลุ่มที่ล็อกไว้ ไม่งั้นดูเหมือนเครื่องหายไปเฉย ๆ
              // ⚠️ ห้ามใช้ไอคอน 🔒 กับ Lock_MC — กุญแจสงวนไว้ให้เครื่องที่กันไว้ให้ POLY/COTTON
              // (ชิป .cellava-rsv) คนละเรื่องกัน ; ป้ายนี้เขียนคำว่า "Lock MC" ตรง ๆ แทน
              const lockMc = Number(av.lock) || 0
              const lockWho = String(av.lock_mc || '').trim()
              const lockTag = lockMc > 0 ? `Lock MC ${lockMc}${lockWho ? ' ' + lockWho : ''}` : ''
              // ติดลบเพราะ "ล็อกล้วน" (ถ้าไม่มีล็อกก็ยังว่าง) ≠ แผนวางงานเกินเครื่อง
              // → ช่องแบบนี้ต้องบอกว่าเครื่องถูกกันไว้ ไม่ใช่ขึ้นแดงว่าวางเกิน
              const overNoLock = remainLive + lockMc      // เครื่องที่จะเหลือถ้าไม่มี Lock_MC
              const lockOnly = remainLive < 0 && lockMc > 0 && overNoLock >= 0
              // ติดลบ = วางเกินเครื่องที่มี → ไม่โชว์ "ว่าง -5" (ตีความยาก) แต่บอกตรงๆ ว่าเกินเท่าไร
              // (มีล็อกด้วย → เลข "เกิน" นับเฉพาะส่วนที่งานเกินจริง ส่วนที่ล็อกกินไปแยกไปอยู่ป้าย Lock MC)
              const avaTxt = lockOnly ? `Lock MC ${lockMc}${lockWho ? ' ' + lockWho : ''}`
                : remainLive > 0 ? `ว่าง ${remainLive}`
                  : remainLive === 0 ? 'เต็ม'
                    : `ไม่ว่าง (เกิน ${-overNoLock})`
              return (
                <span className={'cellava cellava-click'
                  + (remainLive <= 0 ? ' none' : '')
                  + (remainLive < 0 && !lockOnly ? ' over' : '')
                  + (lockOnly ? ' lockonly' : '')}
                  onClick={e => { e.stopPropagation(); c.setAvaSel({ w, avaKey, vals: r.vals }) }}
                  title={`${avaTxt} • เครื่องปกติทั้งหมด ${av.total}\nว่างหลัง booking ${av.remain} − แผนจองเพิ่ม(ปกติ) ${planNormal} = ${remainLive}`
                    // เครื่องที่ user กันไว้ในชีท Lock_MC ถูกหักออกจาก av.remain มาแล้ว (backend)
                    // → บอกไว้ในทูลทิป ไม่งั้นเลข "ว่างหลัง booking" ดูหายไปเฉย ๆ
                    + (lockMc > 0
                      ? `\nLock MC: กันเครื่องไว้ ${lockMc} เครื่อง${lockWho ? ` — ระบุกลุ่ม ${lockWho}` : ''} — หักออกให้แล้ว`
                        + `\n   (ล็อกคิดที่ระดับ CAT+Guage จึงหักกับทุกกลุ่มเครื่องของ Guage นี้)`
                        + (lockOnly ? '\n   ช่องนี้ไม่ได้วางงานเกิน — เครื่องถูกกันไว้ทั้งหมด' : '')
                      : '')
                    // เครื่องที่ย้ายเข้า/ออกจากการติ๊ก Change Cylinder — ต้องบอกที่มา
                    // ไม่งั้นเลขเครื่องเปลี่ยนไปเองโดยไม่มีใครอธิบายได้
                    + ((av.cyl || 0) !== 0
                      ? `\n🔄 Change Cylinder ที่อนุมัติไว้: ${av.cyl > 0 ? '+' : ''}${av.cyl} เครื่อง (รวมในยอดนี้แล้ว)`
                      : '')
                    + (hasRsv ? `\nเครื่องกันไว้ (ใช้แทนงานปกติไม่ได้):\n${rsvTxt}` : '')
                    + '\n👆 คลิกเพื่อดูว่าเครื่องถูกใช้ไปกับ item ไหนบ้าง'}>
                  {avaTxt}
                  {/* ช่องที่ยังว่าง/เต็ม/เกินจริง แต่มีเครื่องถูกกันไว้ → ต่อป้าย "Lock MC" ให้เห็นว่า
                      เลขว่างถูกหักไปเท่าไรและกันไว้ที่กลุ่มไหน (lockOnly โชว์ในข้อความหลักแล้ว) */}
                  {lockMc > 0 && !lockOnly && <span className="cellava-lock">{lockTag}</span>}
                  {hasRsv && <span className={'cellava-rsv' + (rsvOver ? ' over' : '')}>🔒{rsvP ? ` P${polyLeft < 0 ? ` ไม่ว่าง (เกิน ${-polyLeft})` : polyLeft}` : ''}{rsvC ? ` C${cottonLeft < 0 ? ` ไม่ว่าง (เกิน ${-cottonLeft})` : cottonLeft}` : ''}</span>}
                </span>
              )
            })()}
            {laid.map((ent, si) => {
              // ช่องว่าง n บรรทัดติดกัน = div เดียวสูง n×SLOT_H (หัก margin-bottom 3 ที่อยู่ใน CSS)
              if (ent.gap) return <div key={'gap' + si} className="gbar-gap" style={{ height: ent.gap * SLOT_H - 3 }} />
              // overlay History แผนเดิม (ย้อนหลัง 5 week) — บล็อกอ่านอย่างเดียว ลาก/แก้/หักเครื่องว่างไม่ได้
              if (ent.b) {
                const b = ent.b, bi = ent.bi
                // บล็อกประวัติก็ต้องบอกได้ว่า item นั้นให้สีแล้วหรือยัง — ใช้สีชุดเดียวกับ
                // บล็อกแผน (พื้นเรียบ ไม่ลงลาย) ; ที่แยกออกว่าเป็นของเก่าอ่านอย่างเดียว
                // คือขอบเส้นประ + ไอคอน 📋 + หัวสัปดาห์ที่คาดไว้ว่าเป็นช่วงย้อนหลัง
                // ⚠️ ยังตัดสินระดับ item (ไม่ใช่รายก้อนเหมือนบล็อกแผน) เพราะ booking ที่ backend
                //    ส่งมามีแค่ ITEM_CODE ไม่มีธง COLOR_PART ว่าก้อนไหนอยู่ในโควตาสีของใบสั่ง
                const bDyed = c.dyeCodes ? c.dyeCodes.has(nkey(b.item)) : null
                const bBase = bDyed == null ? null : (bDyed ? DYE_ON : DYE_OFF)
                // โหมด "SC โครงร่าง" — บล็อกประวัติของ item ที่ยังไม่ให้สี ก็เอาพื้นออกเหมือนบล็อกแผน
                // ไม่งั้นเปิดแถบประวัติแล้วจะมีพื้นทึบปนกับบล็อกโครงร่างในแถวเดียวกัน
                const bOutline = c.scOutline && bDyed === false
                // เดดไลน์ทอของงาน booking — backend คิด TARGET_KNIT จาก FG_YEAR/FG_WEEK ของ
                // ไฟล์ booking (tk = 0 → ไฟล์รันก่อนเพิ่มคอลัมน์ ยังไม่มีข้อมูล จึงไม่ตัดสินอะไร)
                const bTk = Number(b.tk) || 0
                // แกน Gantt เก็บแค่เลขสัปดาห์ → ปีของคอลัมน์เอาจาก isNextYearW (เกณฑ์
                // เดียวกับที่ใช้เรียงคอลัมน์) แล้วเทียบกับ tky (ปีของ target ที่ backend ส่งมา)
                // ถ้าเทียบเลขสัปดาห์ล้วน งาน W2 ปีหน้าจะดูเหมือนยังไม่เกิน target W46 ปีนี้
                // คอลัมน์ประวัติของปีก่อนมีปีติดมากับคีย์อยู่แล้ว — ใช้ตรง ๆ ไม่ต้องเดา
                const bWy = keyYear(w, c.isNextYearW(w))
                const bOver = bTk > 0 ? weekDelta(bWy, wkNum(w), Number(b.tky) || 0, bTk) : 0
                const bLate = bTk > 0 && bOver > 0
                const bDue = bTk > 0
                  ? `\nTarget Knit W${bTk}${b.fg ? ` (FG W${b.fg})` : ''} • ${bLate ? `ไม่ทัน — เลย ${bOver} สัปดาห์` : 'ยังไม่เกิน'}`
                  : ''
                // RDD ลูกค้า — โชว์ป้ายเฉพาะตอนไม่มี Target Knit (tk = 0 คือไฟล์ booking
                // ที่ยังไม่มีคอลัมน์ FG week) ; ถ้ามี tk แล้วป้าย Target Knit บีบกว่าอยู่แล้ว
                // จะขึ้น 2 ป้ายซ้อนกันเปล่า ๆ
                const bRdd = Number(b.rdd) || 0
                const bRddTxt = bRdd > 0 ? `\nRDD W${bRdd}` : ''
                const bLateRdd = bTk === 0 && bRdd > 0 && wkNum(w) > bRdd
                // คีย์ระบุบล็อกประวัติแต่ละก้อน (แถว×สัปดาห์×ลำดับในช่อง) — ใช้ไฮไลต์ก้อนที่เปิดการ์ดอยู่
                const bKey = r.key + '||' + w + '||' + bi
                // ล็อกแผน: งานนี้ถูกกดล็อกไว้แล้วหรือยัง (คีย์ตรงกับชีท Lock_Plan)
                const bLock = c.lockMap.get(lockKeyOf(w, b.item, b.mc_group, b.gauge))
                // Core Greige / Program ของบล็อก History — ต้องทาสีชื่อ item เหมือนบล็อกแผน
                const bCore = b.core === true
                const bProg = c.bkProgTeam(b.item, b.team)
                // ประเภทเครื่องของบล็อกประวัติ — booking ไม่รู้ว่าเครื่องไหนแชร์กับ item อื่น
                // จึงอ่านได้แค่ 🔧 ตั้งเครื่องใหม่ (_mc_increase > 0) / ⏩ เครื่องเดิมวิ่งต่อ
                // ไฟล์ booking เก่าที่ไม่มีคอลัมน์ลูกโซ่ → ไม่มีทั้งคู่ ตกกลับเป็น 📋
                const bKind = Number(b.setup) > 0 ? 'setup' : (Number(b.carry) > 0 ? 'carry' : null)
                // งานที่ Replan ยกไปวางใหม่แล้ว (ปกติถูกซ่อน — เห็นได้เมื่อกดปุ่ม ♻ เปิดดู)
                const bReplanned = Number(b.replanned) > 0
                return (
                  <div key={'bk' + bi}
                    className={'gbar gbar-booking' + (bkSelKey === bKey ? ' selected' : '')
                      + (bLock ? ' plan-locked' : '') + (bReplanned ? ' replanned' : '')
                      + (bOutline ? ' gbar-outline' : '')}
                    style={bOutline
                      ? { background: SC_OUTLINE_BG, color: SC_OUTLINE_INK, borderColor: SC_OUTLINE_EDGE }
                      : (bBase ? { background: bBase, color: barInk(bBase), borderColor: DYE_EDGE } : undefined)}
                    // การ์ดของ booking กับของงานแผนเป็น panel ตำแหน่งเดียวกัน → เปิดใบใหม่ต้องปิดใบเก่า
                    // + เปิด modal ตาราง/เทียบ booking ของ item นี้ด้วย (ผู้เรียกหาแถวจากชื่อ item) —
                    //   งานล็อกที่อยู่แต่ใน booking ไม่มีแถวในชีทแผน เปิดผ่าน selIdx ไม่ได้
                    onClick={() => {
                      const same = bkSelKey === bKey
                      c.setSelIdx(null)
                      c.setBkSel(same ? null : { key: bKey, b, w, vals: r.vals })
                      c.onOpenItem?.(same ? null : b.item)
                    }}
                    title={`📋 History แผนเดิม${c.bookingAllWeeks ? ' (ทุกสัปดาห์ที่ค้นเจอ)'
                      : (c.historyFrom ? ` (ย้อนถึง W${c.historyFrom.week}/${c.historyFrom.year})` : ` (ย้อนหลัง ${HISTORY_WEEKS_BACK} week)`)}`
                      + `\n${b.item}${b.so ? ` • SO ${b.so}` : ''}\n${r.vals.join(' • ')} • สัปดาห์ ${wkLabel(w)}`
                      + `${b.qty > 0 ? `\nจำนวน ${b.qty} กก.` : ''}${b.mc > 0 ? ` • ใช้ ${b.mc} เครื่อง` : ''}`
                      + `${b.material ? `\n${b.material}` : ''}${bRddTxt}${bDue}`
                      + `${bKind ? `\n${MC_KINDS[bKind].icon} ${MC_KINDS[bKind].label}` : ''}`
                      // สัปดาห์แรกสุดของชุดที่ดึงมา — ไม่มีสัปดาห์ก่อนหน้าให้เทียบ ทุกเครื่องเลย
                      // ถูกนับเป็น "ตั้งใหม่" ต้องบอกไว้ ไม่งั้นอ่านเลข setup ของบล็อกนี้ผิด
                      + `${Number(b.edge) > 0 ? '\n⚠ สัปดาห์ขอบของชุดข้อมูล — เลข setup/carry ของบล็อกนี้เชื่อไม่ได้' : ''}`
                      + `${bDyed == null ? '' : (bDyed ? '\nให้สีแล้ว (ต้องย้อม)' : '\nยังไม่ให้สี')}`
                      + `${bCore ? '\n★ Item Core Greige' : ''}${bProg ? `\n🏷 Program • ทีม ${bProg}` : ''}`
                      + lockTip(bLock)
                      + `${bReplanned ? '\n♻ Replan ยกงานนี้ไปวางใหม่ที่อื่นแล้ว — ตำแหน่งนี้ไม่ใช่แผนจริง' : ''}`
                      + '\n👆 คลิกเพื่อดูรายละเอียด • ดูอย่างเดียว (ลาก/แก้ไม่ได้)'}>
                    {bLock && <span className="gbar-star" title={lockTip(bLock).trim()}>🔐</span>}
                    {bReplanned && (
                      <span className="gbar-star" title="Replan ยกงานนี้ไปวางใหม่ที่อื่นแล้ว — ตำแหน่งนี้ไม่ใช่แผนจริง">♻</span>
                    )}
                    {/* 📋 ต้องติดทุกบล็อกเสมอ = เครื่องหมายเดียวที่บอกว่า "ของเก่าอ่านอย่างเดียว"
                        (เดิมถูกไอคอนประเภทเครื่องแทนที่ ทำให้บล็อกแผนเก่ากับบล็อกแผนหน้าตาเหมือนกันเป๊ะ) */}
                    <span className="gbar-mc" title="แผนเดิมจาก booking — ดูอย่างเดียว">📋</span>
                    {bKind && <span className="gbar-mc" title={MC_KINDS[bKind].label}>{MC_KINDS[bKind].icon}</span>}
                    <span className={'gbar-item' + (bCore ? ' core' : '') + (bProg ? ' program' : '')}>{b.item}</span>
                    {bLate && (
                      <span className="gbar-tag warn late"
                        title={`ทอไม่ทัน — ต้องทอเสร็จภายใน Target Knit W${bTk} แต่ booking วางไว้ ${wkLabel(w)} (เลย ${bOver} สัปดาห์)`}>
                        ⚠ Over TG W{bTk}
                      </span>
                    )}
                    {bLateRdd && (
                      <span className="gbar-tag warn late"
                        title={`RDD ลูกค้าคือ W${bRdd} แต่ booking วางทอไว้ ${wkLabel(w)} (เลย ${wkNum(w) - bRdd} สัปดาห์)`
                          + '\n(ไฟล์ booking นี้ยังไม่มี FG week จึงเทียบ RDD ตรง ๆ)'}>
                        ⚠ เลย RDD W{bRdd}
                      </span>
                    )}
                    {b.mc > 0 && <span className="gbar-tag">{b.mc} เครื่อง</span>}
                    {b.qty > 0 && <span className="gbar-qty">{b.qty}<small className="gbar-qty-unit">kg</small></span>}
                  </div>
                )
              }
              const j = ent.j
              const isColor = c.colorRows && c.colorRows.has(j.idx)
              const editing = editIdx === j.idx
              const sc = c.valOf(j.row)('SC_SO_NO')
              // โหมดแยกสีตาม "ให้สีแล้ว/ยังไม่ให้สี" (null = โหมดเดิม สีตาม CAT+เกจ)
              // ตารางจ้างทอไม่เข้าโหมดนี้ — ทั้งตารางเป็นน้ำเงินก้อนเดียวกันหมด
              const isS9Job = !!c.outsourceIdx && c.outsourceIdx.has(j.idx)
              const dyed = isS9Job ? null : (c.dyeCodes ? c.dyedOf(j) : null)
              const barBg = isS9Job ? S9_BAR
                : (dyed == null ? c.colorOf(j.ck) : (dyed ? DYE_ON : DYE_OFF))
              // โหมด "SC โครงร่าง": ก้อน Forecast (ยังไม่ให้สี) เอาพื้นออก เหลือแค่กรอบสีเดิม
              // ก้อนที่มีสีเฉพาะของตัวเอง (★ งานสี / ⚠ พับไม่คู่ / ⏸ hold / 🧵 จ้างทอ) ไม่เข้าโหมดนี้
              // — สองชุดแรกไม่ตั้ง inline style อยู่แล้ว ส่วนจ้างทอมี dyed === null จึงไม่เข้าเงื่อนไข
              // ⚠️ ก้อนที่ออกใบจอง KP แล้ว (🎫 keepLocked) คงสีทึบเสมอ แม้ยังไม่มียอดเปิดสี —
              //    ยืนยันกับฝ่ายผลิตไปแล้ว ไม่ใช่ของที่ "ยังไม่ยืนยัน" ที่โหมดนี้ต้องการทำให้จาง
              const jOutline = c.scOutline && dyed === false && !j.keepLocked
              const dyeTip = dyed == null ? `\nสี: ${j.ck}`
                : (dyed ? '\nให้สีแล้ว (ต้องย้อม)'
                  // ก้อนเกินโควตา: item มีสี แต่ยอดก้อนนี้อยู่นอกยอดที่ลูกค้าเปิดสีมา
                  : (j.cpart === 'NO' ? '\nยังไม่ให้สี (ก้อนนี้เกินยอดที่เปิดสีของใบสั่ง)' : '\nยังไม่ให้สี'))
              // ล็อกแผนที่ user กดเอง (คนละเรื่องกับ locked = สัปดาห์ freeze)
              // บล็อกจ้างทอที่ยุบเข้าแถวในบ้าน: หัวแถว (MC_GROUP/เกจ) เป็นของเครื่องเจ้าบ้านแล้ว
              // → คีย์ล็อกจะไปตรงกับ "งานในบ้าน" ของผ้าตัวเดียวกันสัปดาห์เดียวกัน แล้วโชว์ 🔐 ผิดตัว
              // ล็อกงานจ้างทอต้องกดในโหมดแยกแถว (คีย์เป็นพูลของผู้รับจ้าง)
              const jMerged = !!c.mergedIdx && c.mergedIdx.has(j.idx)
              const jLock = jMerged ? null : c.planLockOf(j.row)
              // ก้อนที่ยุบมาจากหลายใบสั่ง — ลากทีเดียวย้ายทุกใบ (งานผ้าตัวเดียวกันในช่องเดียวกัน
              // มักต้องไปด้วยกัน) ; อยากย้ายเฉพาะใบใดใบหนึ่งให้กาง ▾ แล้วลากบล็อกย่อย
              const jSubs = j.subs ? j.subs.length : 1
              const jExpKey = r.key + '||' + w + '||' + j.gkey
              const jOpen = jSubs > 1 && c.expanded.has(jExpKey)
              const dragIds = jSubs > 1 ? j.subs.map(s => s.idx).join(',') : String(j.idx)
              const scAll = jSubs > 1
                ? j.subs.map(s => c.valOf(s.row)('SC_SO_NO')).filter(Boolean).join(', ') : sc
              // แก้ไม่ได้ = สัปดาห์ freeze หรือใบที่ออกเลข KP แล้ว (🎫 — ยืนยันกับฝ่ายผลิตไปแล้ว)
              // ใช้ตัวนี้แทน locked ทุกที่ที่เป็น "การแก้บล็อกนี้" ; ส่วนช่อง (td) ยังรับ drop
              // งานอื่นได้ตามปกติ — ล็อกที่ตัวงาน ไม่ใช่ล็อกทั้งช่อง
              const jFixed = locked || j.keepLocked
              return (
                <div key={(ent.sub ? 'js' : 'j') + j.idx}
                  className={'gbar' + (dragIdx === j.idx ? ' dragging' : '') + (isColor ? ' gbar-color' : '')
                    + (jFixed ? ' locked' : '') + (selIdx === j.idx ? ' selected' : '')
                    + (jLock ? ' plan-locked' : '')
                    + (jOutline ? ' gbar-outline' : '')
                    + (j.foldWarn ? ' gbar-foldwarn' : '')
                    + (j.hold ? ' gbar-hold' : '')
                    + (ent.sub ? ' gbar-sub' : '')}
                  draggable={!jFixed && !editing}
                  onDragStart={jFixed ? undefined : e => { e.dataTransfer.setData('text/plain', dragIds); e.dataTransfer.effectAllowed = 'move'; c.setDragIdx(j.idx) }}
                  onDragEnd={jFixed ? undefined : () => { c.setDragIdx(null); c.markOver(null) }}
                  onClick={() => c.onBarClick(j)}
                  onDoubleClick={() => c.onBarDblClick(j, jFixed, jExpKey)}
                  style={(j.foldWarn || j.hold) ? undefined : ((isColor && !isS9Job) ? undefined : (jOutline ? {
                    // SC โครงร่าง: พื้นขาว + กรอบสีของ Forecast + ตัวหนังสือเข้ม (พื้นสว่าง)
                    background: SC_OUTLINE_BG,
                    borderColor: SC_OUTLINE_EDGE,
                    color: SC_OUTLINE_INK,
                  } : {
                    // โหมดให้สี/ไม่ให้สี = ใช้สีที่กำหนดมาตรง ๆ (พื้นเรียบ ไม่ไล่เฉด)
                    // ไม่งั้นสีบนจอไม่ตรงกับ #ECD7FD / #6310A7 ที่สั่งไว้
                    background: dyed == null ? barFill(barBg) : barBg,
                    borderColor: dyed == null ? barEdge(barBg) : DYE_EDGE,
                    color: barInk(barBg),
                  }))}
                  title={`${j.item}${scAll ? ` • SC ${scAll}` : ''}${jSubs > 1 ? ` (${jSubs} ใบ — ยอด/เครื่องบนบล็อกรวมทุกใบแล้ว)` : ''}\n${r.vals.join(' • ')} • สัปดาห์ ${wkLabel(w)}${j.qty !== '' ? `\nจำนวน ${j.qty}` : ''}${j.actualmc !== '' ? ` • ใช้ ${j.actualmc} เครื่อง` : ''}${Number(j.setup) > 0 ? ` • setup ${j.setup} วัน` : ''}${j.mc ? `\n${MC_KINDS[j.mc].icon} ${MC_KINDS[j.mc].label}` : ''}${j.remark ? `\n${j.remark}` : ''}${dyeTip}${j.hold ? `\n⏸ ${j.holdKind === 'target' ? 'Hold รอ target' : 'Hold รอสี'} — กันเครื่องไว้ ${j.actualmc || 0} เครื่อง ไม่วางงานสัปดาห์นี้ (กลับมาทอต่อโดยไม่ต้อง setup ใหม่)` : ''}${isColor ? '\n★ งานสี (ต้องย้อม)' : ''}${j.lateRdd ? `\n⚠ ทอไม่ทัน — ต้องทอเสร็จภายใน Target Knit W${j.tk} แต่วางไว้ ${wkLabel(w)} (เลย ${j.tkLate} สัปดาห์)` : ''}${j.foldWarn ? `\n⚠ order เปิดมา ${j.foldQty} พับ — ไม่เป็นพับคู่ (เหลือเศษ ${j.foldRem} พับ)` : ''}${j.progTeam ? `\n🏷 Program • ทีม ${j.progTeam}` : ''}\n👆 คลิกเพื่อดูรายละเอียดครบ${c.onEditQty && !jFixed && j.qty !== '' && jSubs === 1 ? ' • double click เพื่อแก้จำนวน' : ''}${jSubs > 1 ? '\n▸ กดปุ่ม "ใบ" เพื่อกางดูรายใบ (แก้จำนวน/แบ่ง/ลบ รายใบได้ที่นั่น)\n🖱 ลากก้อนนี้ = ย้ายทุกใบพร้อมกัน' : ''}${locked ? '\n🔒 สัปดาห์ freeze — แก้ไม่ได้' : ''}${j.keepLocked ? '\n🎫 ออกใบจอง KP แล้ว — ยืนยันกับฝ่ายผลิตไปแล้ว ย้ายสัปดาห์/เครื่องไม่ได้' : ''}${lockTip(jLock)}`}>
                  {/* 🔒 = สัปดาห์ freeze (ระบบล็อกเอง) · 🔐 = งานที่ user กดล็อกไว้
                      🎫 = ออกเลขใบจอง KP แล้ว (ระบบล็อกให้เอง คนละเหตุผลกับสองตัวบน) */}
                  {locked && <span className="gbar-star">🔒</span>}
                  {j.keepLocked && !locked && (
                    <span className="gbar-star"
                      title="ออกใบจอง KP แล้ว — คงแผนเดิม ลาก/แก้ไม่ได้">🎫</span>
                  )}
                  {jLock && !locked && !j.keepLocked && <span className="gbar-star" title={lockTip(jLock).trim()}>🔐</span>}
                  {isColor && !locked && !jLock && !j.keepLocked && <span className="gbar-star">★</span>}
                  {/* ให้สีแล้ว/ยังไม่ให้สี ดูจากสีพื้นบล็อกอยู่แล้ว → หน้าบล็อกคงไอคอนประเภทเครื่อง
                      🔧 setup / 📦 เติมเข้าเครื่องเดิม / ⏩ วิ่งต่อ ไว้เหมือนเดิมทุกบล็อก */}
                  {j.mc && <span className="gbar-mc">{MC_KINDS[j.mc].icon}</span>}
                  {j.foldWarn && (
                    <span className="gbar-fold"
                      title={`order เปิดมา ${j.foldQty} พับ — ไม่เป็นพับคู่ (เหลือเศษ ${j.foldRem} พับ)`}>⚠</span>
                  )}
                  <span className={'gbar-item' + (j.core ? ' core' : '') + (j.progTeam ? ' program' : '')}>{j.item}</span>
                  {/* ก้อนที่ยุบหลายใบสั่งไว้ — กดกาง/ยุบรายใบ (stopPropagation กันไปเปิดการ์ด) */}
                  {jSubs > 1 && (
                    <span className="gbar-exp" role="button"
                      onClick={e => { e.stopPropagation(); c.toggleExpand(jExpKey) }}
                      title={`${jSubs} SC ของผ้าตัวนี้ในช่องนี้ — ยุบให้เหลือก้อนเดียว`
                        + `\nเลข "เครื่อง" บนบล็อก = เครื่องรวมทุก SC (คนละ SC = คนละชุดเครื่อง)`
                        + `\nยอด kg = ยอดรวมทุก SC`
                        + `\nSC: ${scAll || '-'}\n👆 คลิกเพื่อ${jOpen ? 'ยุบกลับ' : 'กางดูราย SC'}`}>
                      {jOpen ? '▾' : '▸'} {jSubs} SC
                    </span>
                  )}
                  {/* บล็อกจ้างทอที่ถูกยุบมานั่งในแถวเครื่องในบ้าน — หัวแถวไม่ได้บอกแล้วว่าใครทอ
                      จึงเขียนพูลผู้รับจ้างไว้บนบล็อก */}
                  {isS9Job && c.mergedIdx && c.mergedIdx.has(j.idx) && (
                    <span className="gbar-tag"
                      title={`ก้อนนี้ส่งไปจ้างทอที่ผู้รับจ้าง ${j.s9pool || '(ไม่ระบุพูล)'}`
                        + '\nไม่ได้ใช้เครื่องในบ้าน — วางรวมแถวเดียวกับงานในบ้านของผ้าตัวนี้ให้อ่านต่อเนื่อง'
                        + '\nลากในแถวนี้ = เลื่อนสัปดาห์จ้างทอ · ลากไปเครื่องอื่น = ดึงกลับมาทอในบ้าน'}>
                      🧵 {j.s9pool || 'จ้างทอ'}
                    </span>
                  )}
                  {/* สัปดาห์ที่เว้น — ไม่ได้ทอ แต่กันเครื่องไว้ให้ item นี้
                      รอสีรอบใหม่ (งานสี) หรือรอรอบผลิตถัดไปตามสต๊อก (ผ้า core) */}
                  {j.hold && (
                    <span className="gbar-tag hold"
                      title={j.holdKind === 'target'
                        ? `กันเครื่องไว้ ${j.actualmc || 0} เครื่อง รอรอบผลิตถัดไปของผ้า core — สัปดาห์นี้ไม่วางงาน`
                          + '\n(สต๊อกยังไม่ลดถึงจุดที่ต้องผลิต) กลับมาทอรอบหน้าโดยไม่ต้อง setup ใหม่'
                        : `กันเครื่องไว้ ${j.actualmc || 0} เครื่อง รอสีรอบใหม่ — สัปดาห์นี้ไม่วางงาน`
                          + '\nกลับมาทอสัปดาห์ถัดไปโดยไม่ต้อง setup ใหม่'}>
                      ⏸ {j.holdKind === 'target' ? 'Hold รอ target' : 'Hold รอสี'}
                    </span>
                  )}
                  {/* ทอไม่ทันเดดไลน์ → ป้ายแดงติดถาวร ไม่ต้องรอ user ติ๊ก chip RDD
                      (chip RDD บอกสัปดาห์ RDD ของลูกค้า ส่วนป้ายนี้บอกสัปดาห์ที่ต้องทอเสร็จ) */}
                  {c.markLate && j.lateRdd && (
                    <span className="gbar-tag warn late"
                      title={`ทอไม่ทัน — ต้องทอเสร็จภายใน Target Knit W${j.tk} แต่วางไว้ ${wkLabel(w)} (เลย ${j.tkLate} สัปดาห์)`
                        + '\nต้องลากขึ้นสัปดาห์ก่อนหน้า หรือแบ่งงาน/จ้างทอ'}>⚠ Over TG W{j.tk}</span>
                  )}
                  {j.stMc && (
                    <span className="gbar-tag"
                      title={`setup เครื่องใหม่ ${j.stSetup} เครื่อง (= job ที่หัก)`
                        + `\ncarry ${j.stCarry} เครื่อง — วิ่งต่อจากสัปดาห์ก่อน ไม่ต้อง setup ไม่หัก job`
                        + `\nรวมสัปดาห์นี้ ${j.stTotal} เครื่อง`
                        + (j.stSource ? `\nที่มา: ${j.stSource}` : '')}>{j.stMc}</span>
                  )}
                  {j.tags.map(t => (
                    // tip มีเฉพาะป้ายของก้อนที่ยุบแล้วค่าไม่ตรงกันทุกใบ ("ค่าแรก +N") — hover เห็นครบ
                    <span key={t.key} className={'gbar-tag' + (t.warn ? ' warn' : '')}
                      title={t.tip || undefined}>{t.text}</span>
                  ))}
                  {editing ? (
                    <input className="gbar-qty-edit" type="number" step="any" autoFocus
                      value={editVal}
                      onChange={e => c.setEditVal(e.target.value)}
                      onMouseDown={e => e.stopPropagation()}
                      onClick={e => e.stopPropagation()}
                      onDoubleClick={e => e.stopPropagation()}
                      onKeyDown={e => { if (e.key === 'Enter') c.commitQty(); else if (e.key === 'Escape') c.setEditIdx(null) }}
                      onBlur={c.commitQty} />
                  ) : (
                    // แถว Hold ไม่มียอดผลิต → ไม่ต้องขึ้น "0 kg" ให้รก
                    !j.hold && j.qty !== '' && <span className="gbar-qty" title={`จำนวน ${j.qty} กก.`}>{j.qty}<small className="gbar-qty-unit">kg</small></span>
                  )}
                </div>
              )
            })}
          </td>
        )
      })}
    </tr>
  )
})

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
              แผนรอบนี้ไม่มีงานที่ Setup ใหม่ในสัปดาห์นี้ — โควตาถูกใช้โดยแผนเดิมทั้งหมด
            </div>
          ) : (
            <table className="avatbl">
              <thead>
                <tr>
                  <th>Item</th><th>MC_GROUP</th><th className="num">Guage</th>
                  <th className="num">จำนวน (กก.)</th>
                  <th className="num" title="job ที่กินโควตา = เครื่องที่ตั้งใหม่ (NEW_MC)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (CARRYOVER_MC) — ไม่กินโควตา">Continue</th>
                </tr>
              </thead>
              <tbody>
                {plan.map((d, i) => (
                  <tr key={i}
                    title={`${d.item} • ${d.mcg} Guage ${d.gauge}${d.so ? `\nSO ${d.so}` : ''}\n`
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
                  <th>Item</th><th>MC_GROUP</th><th className="num">Guage</th>
                  <th className="num">จำนวน (กก.)</th>
                  <th className="num" title="job ที่กินโควตา = เครื่องที่ตั้งใหม่ (SETUP_MC)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (CARRYOVER_MC) — ไม่กินโควตา">Continue</th>
                </tr>
              </thead>
              <tbody>
                {oldJobs.map((d, i) => (
                  <tr key={i}
                    title={`${d.item} • ${d.mcg} Guage ${d.gauge}${d.so ? `\nSO ${d.so}` : ''}\n`
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
              ? <>1 job = Setup ใหม่ 1 เครื่อง — ต้องย้ายงาน 🆕 ออกไปสัปดาห์อื่นรวม <b>{nf(total - cap)} job</b>
                ช่องนี้ถึงจะพอดี (งาน 📋 แผนเดิมย้ายไม่ได้ ถ้าเกินจากแผนเดิมล้วนต้องแก้ที่ booking)</>
              : <>1 job = Setup ใหม่ 1 เครื่อง — งานที่รันต่อจากสัปดาห์ก่อน (Continue) ไม่กินโควตา</>}
          </div>
        </div>
      </div>
    </aside>
  )
}

/**
 * การ์ดรายละเอียดบล็อก History แผนเดิม (booking) — เปิดจากคลิกบล็อกเส้นประบน Gantt
 *
 * อ่านอย่างเดียว: เป็นงานที่ commit ลง booking ไปแล้ว แก้/ลาก/แบ่งจากหน้าแผนไม่ได้
 * ฟิลด์ทั้งหมดมาจาก /api/plan/booking-items (ยุบ DETAIL ต่อ สัปดาห์×CAT×เกจ×เครื่อง×item)
 */
function BookingPanel({ b, week, vals, groups, dyed = null, core = false, progTeam = '', lock = null, onToggleLock = null, onClose }) {
  const nf = (n) => (Math.round((Number(n) || 0) * 100) / 100).toLocaleString()
  // ล็อกแผน — กดแล้วเขียนลงชีท Lock_Plan (MasterMC) ; busy กันกดรัวระหว่างรอ backend เขียนไฟล์
  const [lockBusy, setLockBusy] = useState(false)
  const [lockErr, setLockErr] = useState('')
  const num = (k) => Number(b[k]) || 0
  const mc = num('mc'), qty = num('qty')
  const tk = num('tk'), fg = num('fg'), rdd = num('rdd')
  // ปีของคอลัมน์สัปดาห์ (เกณฑ์เดียวกับแกน Gantt) เทียบกับ tky = ปีของ Target Knit
  // ถ้าเทียบเลขสัปดาห์ล้วน งานที่วางไว้ต้นปีหน้าจะดูเหมือนยังไม่เกินเดดไลน์ปลายปีนี้
  // (บล็อกประวัติของปีก่อนมีปีติดมากับคีย์คอลัมน์ → ใช้ปีนั้นตรง ๆ ไม่ต้องเดา)
  const wNum = wkNum(week)
  const wYear = wkPastYear(week) ?? (isNextYearWeek(wNum) ? CUR_YEAR + 1 : CUR_YEAR)
  const over = tk > 0 ? weekDelta(wYear, wNum, num('tky'), tk) : 0
  const late = tk > 0 && over > 0
  const lateRdd = rdd > 0 && wNum > rdd
  // SO_NO 1 ช่องเก็บได้หลาย SO ต่อกันด้วยจุลภาค (backend ส่ง so_list มาให้แล้วถ้าไฟล์ใหม่พอ)
  const soList = Array.isArray(b.so_list) && b.so_list.length
    ? b.so_list
    : String(b.so || '').split(',').map(s => s.trim()).filter(Boolean)
  const soN = Number(b.so_n) || soList.length

  // คลิกนอกการ์ด = ปิด (เหมือน AvaPanel) — mousedown เพื่อให้กดบล็อกอื่นต่อได้ทันที
  // ยกเว้นคลิกใน modal ตารางของ item เดียวกัน (เปิดคู่กัน) — ไม่งั้นแตะตารางแล้วปิดทั้งคู่
  const boxRef = useRef(null)
  useEffect(() => {
    const onDown = (e) => {
      if (e.target.closest?.('.plan-modal')) return
      if (boxRef.current && !boxRef.current.contains(e.target)) onClose()
    }
    window.addEventListener('mousedown', onDown)
    return () => window.removeEventListener('mousedown', onDown)
  }, [onClose])

  // หัวแถว (CAT / เกจ / เครื่อง) — ใช้ค่าที่ map เข้าแถวจริง ไม่ใช่ค่าดิบ เพื่อให้ตรงกับที่เห็นบน Gantt
  const sub = groups && vals
    ? groups.map((g, i) => vals[i]).filter(v => v !== '' && v != null).join(' • ')
    : [b.mc_group, b.gauge, b.cat].filter(Boolean).join(' • ')

  // เรียงหมวดตามการ์ดของงานแผน (PANEL_GROUPS) เพื่อให้อ่านสลับกันได้โดยไม่ต้องหาใหม่
  // [ป้าย, ค่า, กว้าง?] — ค่าว่าง/0 = ตัดบรรทัดทิ้ง (ไฟล์ booking เก่าไม่มีคอลัมน์เหล่านี้)
  const SECTIONS = [
    ['งาน', [
      ['Item', b.item], ['CAT', b.cat], ['Guage', b.gauge], ['MC_GROUP', b.mc_group],
      ['หมวดเครื่อง', b.mc_cat && b.mc_cat !== b.cat ? b.mc_cat : ''],
      ['สัปดาห์ผลิต', wkLabel(week) + (wkPastYear(week) != null ? ` (ปี ${wkPastYear(week)})` : '')],
      ['โรงงาน', b.factory], ['ทีม', b.team],
    ]],
    ['ลูกค้า / ออร์เดอร์', [
      ['ลูกค้า', b.customer, true],
      ['SC/SO' + (soN > 1 ? ` (${soN})` : ''), soList.join(', '), true],
      ['PO', b.po, true], ['ประเภทออร์เดอร์', b.otype], ['วันที่ order', b.po_in],
    ]],
    ['สี / เนื้อผ้า', [
      ['สี', b.color, true], ['สี NAY', b.nay_color, true],
      ['เนื้อผ้า', b.material, true], ['รายละเอียดผ้า', b.desc, true],
    ]],
    ['จำนวน (กก.)', [
      ['ผลิตสัปดาห์นี้', qty > 0 ? nf(qty) : ''],
      ['ทอไปแล้ว', num('knit') > 0 ? nf(num('knit')) : ''],
      ['ค้างทอ', num('outstanding') > 0 ? nf(num('outstanding')) : ''],
      ['กำลังผลิต/วัน', num('cap') > 0 ? nf(num('cap')) : ''],
      ['Revolution/Weight', num('rev_weight') > 0 ? nf(num('rev_weight')) : ''],
    ]],
    ['เครื่อง / setup', [
      ['setup (วัน)', num('setup_days') > 0 ? nf(num('setup_days')) : ''],
      ['เครื่องที่ใช้จริง', nf(mc)],
      // mc = ปัดขึ้นแล้ว — ค่าดิบบอกว่าเครื่องสุดท้ายถูกใช้ไปกี่ส่วน
      ['เครื่องที่ต้องใช้', num('mc_use') > 0 ? nf(num('mc_use')) : ''],
      // setup + carry = mc เสมอ (ลูกโซ่ carry/setup จาก AVA_MC)
      ['เครื่อง setup ใหม่', nf(num('setup'))], ['เครื่องต่อเนื่อง', nf(num('carry'))],
      ['วันทำงานจริง', num('wdays') > 0 ? nf(num('wdays')) : ''],
      ['วันทำงานปฏิทิน', num('cal_days') > 0 && num('cal_days') !== num('wdays') ? nf(num('cal_days')) : ''],
    ]],
    ['กำหนดส่ง / วัตถุดิบ', [
      ['RDD (สัปดาห์)', rdd > 0 ? `W${rdd}` : '', false, lateRdd],
      ['FG week', fg > 0 ? `W${fg}` : ''],
      ['เป้าทอ (สัปดาห์)', tk > 0 ? `W${tk}` : '', false, late],
      ['ด้ายที่ใช้', b.yarn, true],
    ]],
  ]

  return (
    <aside className="jobpanel" ref={boxRef}>
      <div className="jobpanel-head">
        <div>
          <b className={'jobpanel-item' + (progTeam ? ' program' : '')}>{b.item}</b>
          <div className="jobpanel-sub">สัปดาห์ผลิต <b>{wkLabel(week)}</b>{sub ? ` • ${sub}` : ''}</div>
        </div>
        <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
      </div>

      <div className="jobpanel-flags">
        <span className="jobflag" title="งานที่ commit ลง booking แล้ว — หน้าแผนแก้ไม่ได้">
          📋 History แผนเดิม (จาก booking)
        </span>
        {lock && (
          <span className="jobflag lock"
            title={`ล็อกไว้ในชีท Lock_Plan — Replan จะไม่ยกงานนี้ไปวางใหม่`
              + `${lock.by ? `\nล็อกโดย ${lock.by}` : ''}${lock.at ? ` เมื่อ ${lock.at}` : ''}`
              + `${lock.so ? `\nเฉพาะ SO ${lock.so}` : '\nทุก SO ของ item นี้ในช่องนี้'}`}>
            🔐 ล็อกแผนไว้
          </span>
        )}
        {num('setup_days') > 0 && <span className="jobflag setup">🔧 setup {nf(num('setup_days'))} วัน</span>}
        {core && <span className="jobflag core" title="item นี้เป็น Core Greige ในแผนรอบนี้">★ CORE ITEM</span>}
        {progTeam && (
          <span className="jobflag program" title="item + ทีมนี้ ตรงกับชีท Program ใน MasterMC">
            🏷 Program • ทีม {progTeam}
          </span>
        )}
        {dyed === true && <span className="jobflag">🎨 ให้สีแล้ว (ต้องย้อม)</span>}
        {dyed === false && <span className="jobflag">ยังไม่ให้สี</span>}
        {lateRdd && (
          <span className="jobflag warn" title={`RDD ลูกค้าคือ W${rdd} แต่ booking วางทอไว้ ${wkLabel(week)}`}>
            ⚠ วางเลย RDD (W{rdd})
          </span>
        )}
        {late && (
          <span className="jobflag warn"
            title={`ต้องทอเสร็จภายใน Target Knit W${tk} แต่ booking วางไว้ ${wkLabel(week)}`}>
            ⚠ เลย Target Knit W{tk} ({over} สัปดาห์)
          </span>
        )}
      </div>

      <div className="jobpanel-body">
        {SECTIONS.map(([title, fields]) => {
          const rows = fields.filter(([, v]) => v !== '' && v != null)
          if (!rows.length) return null
          // บรรทัดข้อความยาว (ลูกค้า/สี/รายละเอียดผ้า) ต้องขึ้นบรรทัดเต็มความกว้าง ไม่งั้นล้นคอลัมน์
          const wide = rows.filter(f => f[2]), narrow = rows.filter(f => !f[2])
          const dl = (list, cls) => (
            <dl className={cls}>
              {list.map(([label, v, , warn]) => (
                <React.Fragment key={label}>
                  <dt>{label}</dt>
                  <dd className={warn ? 'warn' : undefined}>{v}</dd>
                </React.Fragment>
              ))}
            </dl>
          )
          return (
            <section key={title} className="jobsec">
              <h4>{title}</h4>
              {narrow.length > 0 && dl(narrow, 'jobdl')}
              {wide.length > 0 && dl(wide, 'jobdl wide')}
            </section>
          )
        })}

        <section className="jobsec">
          <div className="hint small" style={{ padding: 0 }}>
            ข้อมูลจากไฟล์ booking ล่าสุด — ดูอย่างเดียว ลาก/แก้จำนวน/ลบไม่ได้
          </div>
        </section>

        {/* ล็อกแผน — บันทึกลงชีท Lock_Plan (MasterMC) ; Replan รอบถัดไปจะไม่รื้องานนี้
            ไปวางใหม่ และไม่คืนเครื่องของงานนี้ให้เป็นเครื่องว่าง */}
        {/* บล็อกประวัติของปีก่อนล็อกไม่ได้ — ล็อกไว้กัน Replan ย้ายงาน "ในอนาคต" เท่านั้น
            และคีย์สัปดาห์ที่ติดปี ('45@2025') เขียนลงชีท Lock_Plan ไม่ได้อยู่แล้ว */}
        {onToggleLock && wkPastYear(week) == null && (
          <section className="jobsec joblock">
            <button className={'joblock-btn' + (lock ? ' on' : '')}
              disabled={lockBusy}
              title={lock
                ? (lock.auto
                  ? `ปลดล็อก${autoLockInfo(lock).label} — Replan จะจัดสัปดาห์นี้ใหม่ได้ และระบบจะไม่ล็อกกลับให้อีก`
                  : 'ปลดล็อก — Replan รอบถัดไปจะยกงานนี้ไปจัดใหม่ได้')
                : 'ล็อกงานนี้ไว้ที่สัปดาห์/เครื่องเดิม — Replan รอบถัดไปจะไม่ย้าย'}
              onClick={async () => {
                setLockBusy(true); setLockErr('')
                try {
                  await onToggleLock({
                    week: String(week), item: b.item, mc_group: b.mc_group, gauge: b.gauge,
                    so: soList.join(','), qty, mc,
                  }, !!lock)
                } catch (e) {
                  setLockErr(e?.message || 'บันทึกล็อกไม่สำเร็จ')
                } finally {
                  setLockBusy(false)
                }
              }}>
              {lockBusy ? '⏳ กำลังบันทึก…' : lock ? '🔓 ปลดล็อกงานนี้' : '🔐 ล็อกงานนี้ (ห้าม Replan รื้อ)'}
            </button>
            <div className="hint small" style={{ padding: 0 }}>
              {lock
                ? (lock.auto ? `🏷 ล็อกอัตโนมัติเพราะเป็น${autoLockInfo(lock).label} — คง W` + week
                  + ` เครื่องเดิมไว้ ; ปลดแล้วระบบจะจดสัปดาห์นี้ไว้ (ชีท ${autoLockInfo(lock).sheet}) ไม่ล็อกกลับให้อีก`
                  : 'ล็อกอยู่ — Replan จะคงงานนี้ไว้ที่ W' + week + ' เครื่องเดิม และไม่ปล่อยเครื่องให้งานอื่น')
                : 'ล็อกแล้วเก็บในชีท Lock_Plan (MasterMC) แก้มือได้จากหน้า Master'}
            </div>
            {lockErr && <div className="hint small warn" style={{ padding: 0 }}>⚠ {lockErr}</div>}
          </section>
        )}
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
  // เครื่องที่ user กันไว้ในชีท Lock_MC — backend หักออกจาก remain และบวกเข้า used มาแล้ว
  // จึงต้องถอดออกจาก bkUsed ไม่งั้นบรรทัด "booking จองไป" จะบวมเกินจริง
  const lockMc = Number(av.lock) || 0
  const lockWho = String(av.lock_mc || '').trim()   // กลุ่มเครื่องที่ระบุในคอลัมน์ MC ของ Lock_MC
  // เครื่องที่ booking จองไปแล้ว = MC_USE_CEIL (ถ้าไม่มีค่อยอนุมานจาก ทั้งหมด − ว่างหลัง booking)
  const bkUsed = (av.used != null ? (Number(av.used) || 0) : Math.max(0, total - remain)) - lockMc
  const remainLive = remain - plan.normal
  // ติดลบเพราะเครื่องถูกกันไว้ล้วน ๆ (ถ้าไม่มี Lock_MC ก็ยังว่าง) — คนละเรื่องกับวางงานเกิน
  const lockOnly = remainLive < 0 && lockMc > 0 && remainLive + lockMc >= 0
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
  // ส่วนที่ "งานเกินเครื่องจริง" — ไม่รวมเครื่องที่ Lock_MC กันไว้ ไม่งั้นข้อความจะสั่งให้ย้ายงาน
  // ออกทั้งที่ช่องนั้นไม่มีงานของแผนอยู่เลย (เกินเพราะล็อกล้วน)
  const nOver = (over.normal ? Math.max(0, -(remainLive + lockMc)) : 0)
    + (over.poly ? -polyLeft : 0) + (over.cotton ? -cottonLeft : 0)
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
        {lockOnly
          ? <span className="jobflag">Lock MC — เครื่องถูกกันไว้ {nf(lockMc)} เครื่อง{lockWho ? ` (ระบุกลุ่ม ${lockWho})` : ''} · ไม่ได้วางงานเกิน</span>
          : over.normal
            ? <span className="jobflag warn">⚠ เครื่องปกติเกิน {nf(-(remainLive + lockMc))} เครื่อง{lockMc > 0 ? ` (ไม่รวมที่ Lock MC กันไว้ ${nf(lockMc)})` : ''}</span>
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
            {lockMc > 0 && (<>
              <dt title={'ล็อกคิดที่ระดับ CAT+Guage จึงหักกับทุกกลุ่มเครื่องของ Guage นี้'
                + (lockWho ? ` ถึงจะเขียนระบุกลุ่ม ${lockWho} ไว้ก็ตาม` : '')}>
                Lock MC (กันเครื่องไว้){lockWho ? ` · ${lockWho}` : ''}
              </dt><dd>− {nf(lockMc)}</dd>
            </>)}
            <dt>ว่างหลัง booking</dt><dd>{nf(remain)}</dd>
            <dt>แผนจองเพิ่ม</dt><dd>− {nf(plan.normal)}</dd>
            <dt>คงเหลือ</dt>
            {/* ล็อกล้วน = ไม่ใช่ความผิดของแผน → ไม่ทำเป็นแถบเตือนแดง แต่บอกว่าติดลบเพราะอะไร */}
            <dd className={remainLive < 0 && !lockOnly ? 'warn' : undefined}>
              {lockOnly ? `เครื่องถูกกันไว้หมด (${nf(remainLive)} — มาจาก Lock MC ${nf(lockMc)})`
                : remainLive < 0
                  ? `ไม่ว่าง (เกิน ${nf(-remainLive)})${lockMc > 0 ? ` — ในนี้เป็นล็อก ${nf(lockMc)}` : ''}`
                  : remainLive === 0 ? 'เต็มพอดี (0)' : nf(remainLive)}
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
                  <th>Item</th><th>MC_GROUP</th>
                  <th className="num">จำนวน (กก.)</th><th className="num">ใช้เครื่อง</th>
                  <th className="num" title="เครื่องที่ต้องตั้งใหม่ (NEW_MC)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (CARRYOVER_MC)">Continue</th>
                </tr>
              </thead>
              <tbody>
                {detail.map((d, i) => (
                  <tr key={i}
                    className={(over[d.kind] ? 'bad' : '') + (d.net === 0 ? ' dim' : '') + (mcGroup && d.mcg === mcGroup ? ' cur' : '')}
                    title={`${d.item} • ${d.mcg} Guage ${d.gauge} • ${KIND[d.kind]}\n`
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
                  <th>Item</th><th>MC_GROUP</th>
                  <th className="num">จำนวน (กก.)</th><th className="num">ใช้เครื่อง</th>
                  <th className="num" title="เครื่องที่ต้องตั้งใหม่ (_mc_increase)">Setup</th>
                  <th className="num" title="เครื่องอุ่นที่ยกมาจากสัปดาห์ก่อน (_prev_mc_use_ceil)">Continue</th>
                </tr>
              </thead>
              <tbody>
                {booking.map((d, i) => (
                  <tr key={i}
                    className={(over[d.kind] ? 'bad' : '') + (mcGroup && d.mcg === mcGroup ? ' cur' : '')}
                    title={`${d.item} • ${d.mcg} Guage ${d.gauge} • ${KIND[d.kind]}${d.so ? `\nSO ${d.so}` : ''}\n`
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
            {lockOnly && nOver === 0
              ? <>ช่องนี้ติดลบเพราะ <b>Lock MC กันเครื่องไว้ {nf(lockMc)} เครื่อง</b>{lockWho ? <> (เขียนระบุกลุ่ม <b>{lockWho}</b>)</> : null} —
                ไม่ใช่การวางงานเกิน ; ล็อกคิดที่ระดับ CAT+Guage จึงหักกับทุกกลุ่มเครื่องของ Guage นี้
                แก้ได้ที่ชีท <b>Lock_MC</b> ใน MasterMC.xlsx</>
              : nOver > 0 && planInOver === 0
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
function JobPanel({ row, columns, colIdx, idx, weeks = [], isLocked = () => false, onSplit, onRemove, showOther, onToggleOther, onClose, progTeam = '', lock = null, onToggleLock = null, lockWeeks = [], onApplyLocks = null, outsource = false, onBringHome = null, outsourceChoices = null, onSendOutsource = null, mergedS9 = false, siblings = null, onPickSibling = null, yarnLocks = null }) {
  const v = (name) => {
    const i = colIdx[name]
    return i == null ? '' : norm(row[i])
  }
  const nf = (n) => (Math.round((Number(n) || 0) * 100) / 100).toLocaleString()
  // ด้ายที่ฝั่งด้ายล็อกให้ SC + ผ้านี้ (หน้าแผนด้าย) — SC_SO_NO อาจยุบหลายใบคั่นจุลภาค
  const myYarnLocks = yarnLocks
    ? v('SC_SO_NO').split(',').flatMap(so => yarnLocks.get(yarnLockKey(so, v('ITEM_CODE'))) || [])
    : []
  // คีย์คอลัมน์ของงานนี้ (ปี+สัปดาห์) — ใช้หาช่องบนแกน / แบ่งงาน / ล็อก ; โชว์ด้วย wkLabel
  const week = weekKey(v('PLAN_WEEK'), v('PLAN_YEAR'))
  const rddW = rddWeekNo(v)
  const lateRdd = isLateRdd(v, week)
  const isOutsource = nkey(v('OUTSOURCE')) === 'YES'
  const kind = mcKind(Number(v('NEW_MC')) || 0, Number(v('CARRYOVER_MC')) || 0,
    Number(v('MC_SHARED')) || 0, Number(v('MC_BOOKING')) || 0, isOutsource)
  // ใบที่ออกเลข KP แล้ว — แก้ไม่ได้ทุกทาง (ย้าย/แบ่ง/ลบ/ส่งจ้างทอ) เหมือนอยู่ในสัปดาห์ freeze
  // → ห่อ isLocked ไว้ตัวเดียว ทุกปุ่มในการ์ดนี้จึงปิดตามกันเองโดยไม่ต้องไล่แก้ทีละที่
  const keepLocked = nkey(v('PLAN_SOURCE')) === KEEP_LOCKED_SOURCE
  const wkLocked = (w) => keepLocked || isLocked(w)

  // ── แบ่งงานเป็น 2 สัปดาห์ (ปุ่มอยู่ในการ์ด กันเผลอกดโดนบนบล็อก) ──
  const curQty = Number(v('PRODUCE_QTY')) || 0
  const canSplit = !!onSplit && idx != null && curQty > 0 && !wkLocked(week)
  const [splitOpen, setSplitOpen] = useState(false)
  const [splitQty, setSplitQty] = useState('')
  const [splitWeek, setSplitWeek] = useState('')
  const openSplit = () => {
    const i = weeks.indexOf(String(week))
    let tgt = ''
    for (let k = i + 1; k < weeks.length; k++) { if (!wkLocked(weeks[k])) { tgt = weeks[k]; break } }
    if (!tgt) tgt = weeks.find(wk => wk !== String(week) && !wkLocked(wk)) || ''
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
  // ── ส่งงานในบ้านไปจ้างทอ — เฉพาะผ้าที่อยู่ในชีท Item S9 (ผู้เรียกคืนรายการพูลที่ไปได้) ──
  // ผลเหมือนลากบล็อกไปวางแถวผู้รับจ้าง (sendJobToOutsource) : แก้แถวบนหน้าเว็บ แล้วกด 💾 บันทึก
  const sendChoices = !outsource && outsourceChoices && idx != null && !wkLocked(week) ? outsourceChoices(idx) : []
  const canSendOut = !!onSendOutsource && sendChoices.length > 0
  const [sendOpen, setSendOpen] = useState(false)
  const [sendPool, setSendPool] = useState('')
  const [sendWeek, setSendWeek] = useState('')
  const openSend = () => {
    setSendPool(sendChoices[0].key)
    setSendWeek(String(week))
    setSplitOpen(false)
    setSendOpen(true)
  }
  const doSend = () => {
    if (sendPool && sendWeek) onSendOutsource(idx, sendWeek, sendPool)
    setSendOpen(false)
  }
  // ── ล็อกแผน — ตรึงงานนี้ไว้ที่สัปดาห์เดิม (Replan รอบถัดไปไม่ยกไปวางใหม่) ──
  // ล็อกทีละบล็อก = ทีละสัปดาห์ ; SC ใบเดียวกันที่กระจายหลายสัปดาห์ สัปดาห์อื่นยังถูกจัดใหม่ได้
  const [lockBusy, setLockBusy] = useState(false)
  const [lockErr, setLockErr] = useState('')
  // งานเดียวกัน (item+เครื่อง+เกจ) ที่กระจายหลายสัปดาห์ → กดปุ่มแล้วกางให้ติ๊กเลือกทีเดียว
  // ไม่ต้องไล่เปิดการ์ดกดล็อกทีละบล็อก ; pick = set สัปดาห์ที่ "อยากให้ล็อก" (null = ยังไม่กาง)
  // ตอนกดยืนยันค่อยเทียบ pick กับสถานะจริงในชีท → ได้ทั้งฝั่งล็อกเพิ่มและฝั่งปลด
  const multiLock = !!onApplyLocks && lockWeeks.length > 1
  const [pick, setPick] = useState(null)
  const openPick = () => {
    const s = new Set(lockWeeks.filter(x => x.lock).map(x => String(x.week)))
    // ตั้งต้นให้เหมือนกดปุ่มเดิม: สลับสถานะของสัปดาห์ที่เปิดการ์ดอยู่ให้เลย ที่เหลือคงตามชีท
    if (lock) s.delete(String(week)); else s.add(String(week))
    setLockErr(''); setPick(s)
  }
  // เปลี่ยนไปคลิกบล็อกอื่น (การ์ดตัวเดิมถูกใช้ซ้ำ) → ปิดกล่องเลือก ไม่งั้นค้างรายการสัปดาห์ของงานเก่า
  useEffect(() => { setPick(null); setLockErr(''); setSendOpen(false) }, [idx])
  const togglePick = (w) => setPick(s => {
    const n = new Set(s)
    if (n.has(String(w))) n.delete(String(w)); else n.add(String(w))
    return n
  })
  const pickAdd = pick ? lockWeeks.filter(x => pick.has(String(x.week)) && !x.lock) : []
  const pickDel = pick ? lockWeeks.filter(x => !pick.has(String(x.week)) && x.lock) : []
  // qty/mc ของแต่ละสัปดาห์มาจาก lockWeeks (รวมทุกแถวในสัปดาห์นั้นแล้ว) ไม่ใช่ของแถวที่เปิดการ์ดอยู่
  const lockPayload = (x) => ({
    week: String(x.week), item: v('ITEM_CODE'),
    mc_group: v('MC_GROUP'), gauge: v('MC_GUAGE'),
    so: x.so, qty: x.qty || null, mc: x.mc || null, source: 'PLAN',
  })
  const doApply = async () => {
    setLockBusy(true); setLockErr('')
    try {
      await onApplyLocks(pickAdd.map(lockPayload), pickDel.map(lockPayload))
      setPick(null)
    } catch (e) {
      setLockErr(e?.message || 'บันทึกล็อกไม่สำเร็จ')
    } finally {
      setLockBusy(false)
    }
  }

  // ลบงานก้อนนี้ออกจากแผน (วางผิด/ไม่ต้องการ) — ยืนยันก่อน แล้วปิดการ์ด
  const canDelete = !!onRemove && idx != null && !wkLocked(week)
  const doRemove = () => {
    if (!window.confirm(`ลบงาน ${v('ITEM_CODE')} (สัปดาห์ ${wkLabel(week)}) ออกจากแผน?\n\nลบก้อนนี้ก้อนเดียว — กด 💾 บันทึกเพื่อให้มีผลถาวร`)) return
    onRemove(idx)
    onClose()
  }
  // ดึงงานจ้างทอกลับมาทอในบ้าน — ผู้เรียกเป็นคนเลือกเครื่อง/คำนวณใหม่ (ดู bringOutsourceHome)
  const canBringHome = outsource && !!onBringHome && idx != null && !wkLocked(week)
  const doBringHome = () => { onBringHome(idx); onClose() }
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
            สัปดาห์ผลิต <b>{wkLabel(week)}</b> • {v('MC_GROUP')} Guage {v('MC_GUAGE')} • {v('CAT')}
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
        {keepLocked && (
          <span className="jobflag warn"
            title="ใบนี้ออกเลขใบจองแล้ว (KP_NO) — ยืนยันกับฝ่ายผลิตไปแล้ว ระบบไม่จัดใหม่และแก้บนหน้าเว็บไม่ได้">
            🎫 ออกใบจอง KP แล้ว — ลาก/แก้ไม่ได้
          </span>
        )}
        {progTeam && (
          <span className="jobflag program" title="item + ทีมนี้ ตรงกับชีท Program ใน MasterMC">
            🏷 Program • ทีม {progTeam}
          </span>
        )}
        {myYarnLocks.map(l => (
          <span key={l.id} className="jobflag yarnlock"
            title={`ฝั่งด้ายล็อกไว้ให้ ${l.sc} ${l.fabric} (หน้าแผนด้าย) — ผ้า ${nf(l.fabric_kg)} กก.`
              + ` · ยังกันอยู่ ${nf(l.eff ?? l.rem)} กก. ด้าย · ล็อกโดย ${l.by || '-'} ${String(l.at || '').slice(0, 16)}`
              + (l.note ? `\n${l.note}` : '')}>
            🧶 ด้ายล็อก {l.yarn} lot {l.lot} {nf(l.eff ?? l.rem)} กก.
          </span>
        ))}
      </div>

      {/* ใบสั่งอื่นของผ้าตัวเดียวกันในช่องเดียวกัน (Gantt ยุบเป็นบล็อกเดียว)
          → การ์ดเป็นที่เดียวที่เห็นครบ และสลับไปแก้/แบ่ง/ลบ/ล็อกทีละใบได้ */}
      {siblings && siblings.length > 1 && (
        <div className="jobsibs">
          <div className="jobsibs-title">
            {siblings.length} ใบสั่งของผ้าตัวนี้ในสัปดาห์/เครื่องเดียวกัน — บน Gantt ยุบเป็นบล็อกเดียว
            (เลขในการ์ดนี้เป็นของใบที่เลือกอยู่)
          </div>
          <div className="jobsibs-list">
            {siblings.map(s => {
              const sv = (n) => { const i = colIdx[n]; return i == null ? '' : norm(s.row[i]) }
              return (
                <button key={s.idx}
                  className={'jobsib' + (s.idx === idx ? ' on' : '')}
                  disabled={!onPickSibling}
                  onClick={() => onPickSibling && onPickSibling(s.idx)}
                  title={`SC ${sv('SC_SO_NO') || '-'} • ${nf(sv('PRODUCE_QTY'))} กก.`
                    + `\n👆 คลิกเพื่อดู/แก้ใบนี้ในการ์ด`}>
                  <b>{sv('SC_SO_NO') || '(ไม่มี SC)'}</b>
                  <span>{nf(sv('PRODUCE_QTY'))} กก.</span>
                </button>
              )
            })}
          </div>
        </div>
      )}

      {(canSplit || canDelete || canBringHome || canSendOut) && (
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
                  {weeks.filter(wk => wk !== String(week) && !wkLocked(wk)).map(wk =>
                    <option key={wk} value={wk}>W{wk}</option>)}
                </select>
              </div>
              <div className="gsplit-hint">เลขเครื่อง/setup จะ copy มาตามเดิม — รันแผนใหม่เพื่อคำนวณเครื่องให้ตรง</div>
              <div className="gsplit-actions">
                <button className="gsplit-ok" onClick={doSplit}>แบ่ง</button>
                <button className="gsplit-cancel" onClick={() => setSplitOpen(false)}>ยกเลิก</button>
              </div>
            </div>
          ) : sendOpen && canSendOut ? (
            <div className="jobsplit-form">
              <div className="gsplit-row">
                <span>ผู้รับจ้าง</span>
                <select value={sendPool} onChange={e => setSendPool(e.target.value)}>
                  {sendChoices.map(c => <option key={c.key} value={c.key}>{c.label}</option>)}
                </select>
              </div>
              <div className="gsplit-row">
                <span>สัปดาห์</span>
                <select value={sendWeek} onChange={e => setSendWeek(e.target.value)}>
                  {weeks.filter(wk => !wkLocked(wk)).map(wk =>
                    <option key={wk} value={wk}>{wkLabel(wk)}</option>)}
                </select>
              </div>
              <div className="gsplit-hint">
                ส่งทั้งก้อน {nf(curQty)} กก. — เครื่องคิดจาก CAP ผู้รับจ้าง (ชีท Item S9 / MC S9) ;
                อยากส่งบางส่วนให้แบ่งงานก่อน · กด 💾 บันทึกเพื่อให้มีผลถาวร
              </div>
              <div className="gsplit-actions">
                <button className="gsplit-ok" onClick={doSend}>🧵 ส่งจ้างทอ</button>
                <button className="gsplit-cancel" onClick={() => setSendOpen(false)}>ยกเลิก</button>
              </div>
            </div>
          ) : (
            <div className="jobsplit-btns">
              {canSplit && <button className="jobsplit-btn" onClick={openSplit}>✂ แบ่งงานเป็น 2 สัปดาห์</button>}
              {canSendOut && <button className="jobout-btn" onClick={openSend}>🧵 ส่งไปจ้างทอ</button>}
              {canBringHome && <button className="jobhome-btn" onClick={doBringHome}>🏠 ดึงกลับมาทอในบ้าน</button>}
              {canDelete && <button className="jobdel-btn" onClick={doRemove}>🗑 ลบงานนี้ออกจากแผน</button>}
            </div>
          )}
        </div>
      )}

      {/* ล็อกแผน — บันทึกลงชีท Lock_Plan (MasterMC) ; Replan รอบถัดไปจะตรึงงานนี้ไว้ที่
          สัปดาห์เดิม ไม่ยกยอดไปวางใหม่ และกันเครื่องของสัปดาห์นั้นไว้ให้
          ใช้ .jobsplit/.jobsplit-btns ชุดเดียวกับปุ่มแบ่งงาน/ลบงาน เพื่อให้ความกว้างเท่ากัน
          (.jobsec ไม่มี padding ซ้ายขวา ปุ่มจะกว้างกว่าอีกสองปุ่ม) */}
      {/* ปิดส่วนล็อกเพราะงานจ้างทอก้อนนี้ถูกยุบเข้าแถวเครื่องในบ้าน (โหมดรวมแถวเปิดตลอด) */}
      {mergedS9 && (
        <div className="jobsplit joblock">
          <div className="joblock-note">
            🧵 ก้อนนี้ทออยู่ที่ผู้รับจ้าง และแสดงรวมแถวกับงานในบ้านของผ้าตัวเดียวกัน
            <br />จึงล็อกแผนของงานจ้างทอจากหน้านี้ไม่ได้
          </div>
        </div>
      )}
      {onToggleLock && (
        <div className="jobsplit joblock">
          {pick ? (
            /* งานนี้มีหลายสัปดาห์ → เลือกทีเดียวว่าจะล็อก/ปลดสัปดาห์ไหนบ้าง แล้วบันทึกรอบเดียว */
            <div className="joblock-pick">
              <div className="joblock-pick-head">
                เลือกสัปดาห์ที่จะล็อก — {v('ITEM_CODE')} • {v('MC_GROUP')} Guage {v('MC_GUAGE')}
              </div>
              <div className="joblock-pick-list">
                {lockWeeks.map(x => {
                  const cur = String(x.week) === String(week)
                  return (
                    <label key={x.week} className={'joblock-wk' + (cur ? ' cur' : '')}>
                      <input type="checkbox" checked={pick.has(String(x.week))}
                        onChange={() => togglePick(x.week)} />
                      <b>{wkLabel(x.week)}</b>
                      <span className="joblock-wk-num">{nf(x.qty)} กก.{x.mc > 0 ? ` • ${nf(x.mc)} เครื่อง` : ''}</span>
                      {x.lock && <span className="joblock-wk-tag">🔒 ล็อกอยู่</span>}
                      {cur && <span className="joblock-wk-tag cur">ช่องนี้</span>}
                    </label>
                  )
                })}
              </div>
              <div className="joblock-pick-tools">
                <button onClick={() => setPick(new Set(lockWeeks.map(x => String(x.week))))}>เลือกทั้งหมด</button>
                <button onClick={() => setPick(new Set())}>ล้าง</button>
              </div>
              {/* ลิสต์นี้สร้างจาก rows ที่ Gantt ได้รับ = แถวที่ผ่านตัวกรองแล้ว (KnitPlan ส่ง visible)
                  ถ้า user กรองคอลัมน์อยู่ สัปดาห์ที่ถูกซ่อนจะไม่โผล่ → ต้องบอก ไม่งั้นกด
                  "เลือกทั้งหมด" แล้วเข้าใจว่าล็อกครบทั้งงาน ทั้งที่ขาดสัปดาห์ที่ถูกกรองไป */}
              <div className="joblock-pick-note">
                ⓘ ลิสต์นี้ = สัปดาห์ที่แสดงอยู่บน Gantt ตอนนี้ — ถ้ากรองคอลัมน์/ติ๊กเลือก item อยู่
                สัปดาห์ที่ถูกซ่อนจะไม่อยู่ในลิสต์ (ล้างตัวกรองก่อนถ้าต้องการล็อกทั้งงาน)
              </div>
              <div className="gsplit-actions">
                <button className="gsplit-ok" disabled={lockBusy || (!pickAdd.length && !pickDel.length)}
                  onClick={doApply}>
                  {lockBusy ? '⏳ กำลังบันทึก…'
                    : `💾 บันทึก (ล็อก ${pickAdd.length} · ปลด ${pickDel.length})`}
                </button>
                <button className="gsplit-cancel" disabled={lockBusy} onClick={() => setPick(null)}>ยกเลิก</button>
              </div>
              {lockErr && <div className="hint small warn" style={{ padding: 0 }}>⚠ {lockErr}</div>}
            </div>
          ) : (
            <>
              <div className="jobsplit-btns">
                <button className={'joblock-btn' + (lock ? ' on' : '')}
                  disabled={lockBusy}
                  title={multiLock
                    ? `งานนี้มี ${lockWeeks.length} สัปดาห์ — กดแล้วเลือกได้ว่าจะล็อกสัปดาห์ไหนบ้าง`
                    : lock
                      ? 'ปลดล็อก — Replan รอบถัดไปจะยกงานนี้ไปจัดใหม่ได้'
                      : 'ตรึงงานนี้ไว้ที่สัปดาห์เดิม — Replan รอบถัดไปจะไม่ย้าย'}
                  onClick={async () => {
                    // งานกระจายหลายสัปดาห์ = กางให้เลือกก่อน ; สัปดาห์เดียว = ล็อก/ปลดทันทีเหมือนเดิม
                    if (multiLock) { openPick(); return }
                    setLockBusy(true); setLockErr('')
                    try {
                      await onToggleLock({
                        week: String(week), item: v('ITEM_CODE'),
                        mc_group: v('MC_GROUP'), gauge: v('MC_GUAGE'),
                        so: v('SC_SO_NO'),
                        qty: Number(v('PRODUCE_QTY')) || null,
                        mc: Number(v('ACTUAL_MC')) || null,
                        source: 'PLAN',
                      }, !!lock)
                    } catch (e) {
                      setLockErr(e?.message || 'บันทึกล็อกไม่สำเร็จ')
                    } finally {
                      setLockBusy(false)
                    }
                  }}>
                  {lockBusy ? '⏳ กำลังบันทึก…'
                    : multiLock ? `🔐 Lock Plan… (${lockWeeks.length} สัปดาห์)`
                      : lock ? '🔓 Unlock Plan' : '🔐 Lock Plan'}
                </button>
              </div>
              <div className="hint small" style={{ padding: '6px 0 0' }}>
                {multiLock
                  ? `งานนี้ (${v('ITEM_CODE')} • ${v('MC_GROUP')}) วางไว้ ${lockWeeks.length} สัปดาห์`
                  + ` — กดปุ่มแล้วติ๊กเลือกได้ว่าจะล็อกสัปดาห์ไหนบ้าง (ล็อกอยู่แล้ว ${lockWeeks.filter(x => x.lock).length} สัปดาห์)`
                  : lock
                    ? (lock.auto
                      ? `🏷 ล็อกอัตโนมัติเพราะเป็น${autoLockInfo(lock).label} — คงไว้ที่ ${wkLabel(week)} เครื่องเดิม ;`
                      + ` ปลดแล้วระบบจะจดสัปดาห์นี้ไว้ (ชีท ${autoLockInfo(lock).sheet}) ไม่ล็อกกลับให้อีก`
                      : `ล็อกอยู่ — Replan จะคงงานนี้ไว้ที่ ${wkLabel(week)} เครื่องเดิม และหักยอดนี้ออกจากดีมานด์ที่จัดใหม่`)
                    : 'ล็อกเฉพาะสัปดาห์นี้ (สัปดาห์อื่นของ SC เดียวกันยังจัดใหม่ได้) — เก็บในชีท Lock_Plan (MasterMC) แก้มือได้จากหน้า Master'}
              </div>
              {lockErr && <div className="hint small warn" style={{ padding: 0 }}>⚠ {lockErr}</div>}
            </>
          )}
        </div>
      )}

      <div className="jobpanel-body">
        {PANEL_GROUPS.map(g => {
          const fields = g.fields.filter(([c]) => !PLAN_HIDDEN_COLS.has(c) && colIdx[c] != null && norm(row[colIdx[c]]) !== '')
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

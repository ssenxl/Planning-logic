import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { useAutoSave } from '../autosave.js'
import SaveStatus from './SaveStatus.jsx'
import {
  ColumnFilter, columnFilterData, filterRows, norm,
  ROWNUM_W, isIdName, numericCols, fmtNum, autoColWidths, makeColResizer,
} from './ColumnFilter.jsx'
import { makeWorkDayResolver, makeWeekOfDate, makeWeekAxis, toIsoDate } from '../workday.js'
import { recalcChains, makeSetupDays, makeWarmSeed, newRunDays, chainKeyOf, colIx, canRecalc, setPlanConstants } from '../mcRecalc.js'
import { wkNum, wkLabel, rowWeekKey, writeWeekKey, keyFromSortKey } from '../weekKeys.js'
import { PLAN_HIDDEN_COLS, PLAN_COL_LABEL } from '../planColumns.js'
import PlanGantt, { LOAD_TYPES, BAR_FIELDS_DEFAULT, barFieldsFor, loadBarFields, saveBarFields, loadScOutline, saveScOutline, classifyType, isFrozenWeek, setFreezeWeeks, useFreezeWeeks, isNextYearWeek, weekSortKey, lockKeyOf, DYE_ON, DYE_OFF, DYE_EDGE, SC_OUTLINE_BG, SC_OUTLINE_EDGE, SC_OUTLINE_INK, S9_CAT_PREFIX, S9_MC_SEP } from './PlanGantt.jsx'
import OutsourceAdvisor from './OutsourceAdvisor.jsx'
import CylinderAdvisor from './CylinderAdvisor.jsx'
import PlanPivot from './PlanPivot.jsx'
import ReplanNotify from './ReplanNotify.jsx'

// ค่าความคลาดเคลื่อนที่ยอมรับได้เวลาเทียบยอดสั่ง (กก.)
// PRODUCE_QTY ถูกปัด "ลงเป็นพับเต็ม" (fold_round) ทีละสัปดาห์ → ออร์เดอร์ที่แบ่งวางหลายสัปดาห์
// เศษปัดพับจะสะสมกันได้เกิน 0.01 (เช่น ขาด 0.02 ทั้งที่วางครบ) ซึ่งไม่มีความหมายเชิงธุรกิจ
// ยอมได้ถึง 1 กก. เกินกว่านี้ = ขาด/เกินจริง ต้องเตือน
const QTY_TOL = 1

// อาร์เรย์ว่างค่าคงที่ — ส่งเป็น prop แทน [] สดๆ ไม่ให้ useMemo ฝั่ง PlanGantt คิดใหม่ทุกเรนเดอร์
const EMPTY_ROWS = []
const EMPTY_OBJ = {}


// เกจในไฟล์แผนออกมาเป็น float ("28.0") ส่วนคีย์พูลจากชีท MC S9 เป็นจำนวนเต็ม ("28")
// → ปัดให้เป็นรูปเดียวกันทุกที่ ไม่งั้นแถวจ้างทอหาพูลของตัวเองไม่เจอ แล้วเพดานเครื่องหายทั้งแถว
const gnum = (v) => {
  const s = String(norm(v)).trim()
  const n = Number(s)
  return Number.isFinite(n) && s !== '' ? String(Math.round(n)) : s
}

// คีย์เรียงแถวบนแกนเวลาจริง = คีย์สัปดาห์จาก PLAN_WEEK + PLAN_YEAR (weekKeys.js)
// แผนของปีถัดไป (เช่น W10) ต้องต่อท้าย W52 ของปีนี้ ไม่ใช่โผล่บนสุดเพราะเลขน้อย
// ไม่มีคอลัมน์ปี (ไฟล์แผนรุ่นเก่า) → คีย์เลขเปล่า weekSortKey เดาปีจากระยะห่างแบบเดิม
function rowTimelineKey(row, wI, yI) {
  const k = rowWeekKey(row, wI, yI)
  return k === '' || !Number.isFinite(wkNum(k)) ? Number.POSITIVE_INFINITY : weekSortKey(k)
}

// เรียง [{row, idx}] ตามแกนเวลาจริง แล้วค่อยตามลำดับแถวเดิมในไฟล์ (แถวสัปดาห์เดียวกันไม่สลับ)
// idx ยังเป็นเลขแถวจริงในไฟล์ → แก้ค่า/ลบแถวยังเขียนกลับตำแหน่งเดิมถูกต้อง
function sortRowsByTimeline(list, columns) {
  const wI = columns ? columns.indexOf('PLAN_WEEK') : -1
  if (wI < 0) return list
  const yI = columns.indexOf('PLAN_YEAR')
  return list
    .map((x, i) => ({ x, i, k: rowTimelineKey(x.row, wI, yI) }))
    .sort((a, b) => (a.k - b.k) || (a.i - b.i))
    .map(({ x }) => x)
}

// แปลงเวลาไฟล์แผน (epoch วินาที) → วันที่+เวลาแบบไทย เช่น "17 ก.ค. 2569 14:32 น."
// ใช้บอก user ว่าแผนที่กำลังดูอยู่ "รันรอบไหน" → ส่ง run_at (เวลารันจากชื่อไฟล์) ไม่ใช่ mtime
// ซึ่งขยับทุกครั้งที่มีคนลาก Gantt เพราะเซฟอัตโนมัติเขียนทับไฟล์เดิม
function fmtPlanTime(ts) {
  if (!ts) return ''
  const d = new Date(ts * 1000)
  const date = d.toLocaleDateString('th-TH', { day: 'numeric', month: 'short', year: 'numeric' })
  const time = d.toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit' })
  return `${date} ${time} น.`
}

// คอลัมน์ที่ระบุ "ออร์เดอร์ 1 รายการ" — ค่าคงที่ภายในออร์เดอร์เดียวกัน แต่ต่างกันระหว่างออร์เดอร์
// SC เดียวมีได้หลายออร์เดอร์: ต่างที่ PO / ORDERS_QTY / สัปดาห์ FG (FG_WEEK) / สี (SUB_COLOR)
// ไม่รวม PLAN_WEEK เพราะออร์เดอร์เดียวถูกแบ่งวางได้หลายสัปดาห์ (ต้องจับรวมกัน)
// SUB_COLOR จำเป็นสำหรับ YD-ORDERS: SO เดียวมีได้หลายสี สีละใบ ยอดสั่งของแต่ละสีเท่ากันได้
// ถ้าไม่แยกสี ยอดวางของทุกสีจะถูกบวกรวมแล้วเทียบกับยอดสั่งของสีเดียว → ขึ้น "เกิน" ทั้งที่วางถูก
// ITEM_CODE จำเป็นเพราะ SC_LINE_ID ของงาน YD/CL เป็น 0 ทั้งไฟล์ (ต้นทางส่งมาแบบนั้น) →
// ใบเดียวที่มี 2 ผ้ายอดสั่งเท่ากันจะถูกนับรวมเป็นใบเดียวถ้าไม่แยกด้วยรหัสผ้า
// แก้ที่นี่ที่เดียวถ้าพบว่ายังมีคอลัมน์อื่นที่แยกออร์เดอร์
const ORDER_KEY_COLS = ['SC_SO_NO', 'SC_LINE_ID', 'ITEM_CODE', 'PO_NO', 'ORDERS_QTY', 'FG_WEEK', 'SUB_COLOR']
function orderKeyOf(row, columns) {
  return ORDER_KEY_COLS.map(c => { const i = columns.indexOf(c); return i >= 0 ? norm(row[i]) : '' }).join('|')
}

// ยอดที่ออร์เดอร์นี้ "ต้องวางในแผนรอบนี้" — ฐานเทียบยอดวางทุกจุดบนหน้านี้
// ORDERS_QTY คือยอดสั่งทั้งใบ ซึ่งรวมส่วนที่เคยวางแผน/ทอไปแล้ว → ใช้เป็นฐานไม่ได้
// Planning เขียนยอดที่ต้องวางจริงมาให้ที่คอลัมน์ ORDER_PENDING (ค่าเท่ากันทุกแถวของใบ)
// ไฟล์แผนที่รันก่อนมีคอลัมน์นี้ = ไม่มีค่า → ถอยไปใช้ ORDERS_QTY แบบเดิม
function orderBaseOf(row, columns) {
  const bi = columns.indexOf('ORDER_PENDING')
  if (bi >= 0) {
    const b = Number(norm(row[bi]))
    if (Number.isFinite(b) && b > 0) return b
  }
  const oi = columns.indexOf('ORDERS_QTY')
  return oi >= 0 ? (Number(norm(row[oi])) || 0) : 0
}

// คอลัมน์ที่ "ต่างกันได้" ตอนเทียบว่า 2 แถวเป็นงานเดียวกันหรือไม่ (ตอนลากมารวมก้อน)
// = ยอดผลิต + ทุกค่าที่คำนวณจากยอด/สัปดาห์ (เครื่อง วัน setup หมายเหตุ) — นอกนั้นต้องตรงกันหมด
// (ITEM, เครื่อง, เกจ, SC, PO, FG, สี, RDD ฯลฯ) ตามที่ user ต้องการ: "ถ้าอย่างอื่นเหมือนกันหมดให้รวมกัน"
const MERGE_IGNORE_COLS = new Set([
  'PRODUCE_QTY', 'REQUIRED_MC', 'ACTUAL_MC', 'CARRYOVER_MC', 'NEW_MC', 'SETUP_DAYS',
  'AVAILABLE_DAYS', 'ACTUAL_WORKING_DAYS', 'CALENDAR_WORKING_DAYS', 'FACTORY_WORKING_DAYS',
  'MC_BOOKING', 'MC_SHARED', 'REMARK', 'LINE_REMARK', 'PLAN_QTY', 'PENDING_PLAN',
])

// 2 แถวเป็น "งานเดียวกันคนละก้อน" ไหม — ต่างได้เฉพาะยอดผลิตกับค่าที่คำนวณตามมา
function sameJobExceptQty(a, b, columns) {
  for (let i = 0; i < columns.length; i++) {
    if (MERGE_IGNORE_COLS.has(columns[i])) continue
    if (norm(a[i]).trim() !== norm(b[i]).trim()) return false
  }
  return true
}

// เพดานคอลัมน์สัปดาห์ที่ยอมเติมให้ต่อเนื่องในตารางเทียบ — กันตารางบานเป็นร้อยคอลัมน์
// เมื่อข้อมูลเพี้ยน (booking ปีที่แล้วโผล่คู่กับแผนปีหน้า) ; เกินเพดาน = โชว์เฉพาะสัปดาห์ที่มีของแบบเดิม
const MAX_GAP_FILL = 80

// ── ตารางเทียบ "booking (แผนเก่า) → แผนใหม่" ในการ์ด modal (โครงเดียวกับหน้า Order Color) ──
// แผนเก่า = booking DETAIL (bookingItems — ชุดเดียวกับบล็อก History บน Gantt) ไม่ใช่ไฟล์แผนรอบก่อน
// แผนใหม่ = แถวแผนปัจจุบัน (รวมที่ user ลาก/ลบ/แบ่ง/แก้ยอดบนหน้าจอแล้ว)
// origRows ใช้แค่ "ตรวจว่า item ไหนถูกแตะในเซสชันนี้" เพื่อดึงมาโชว์คู่กับ item ที่คลิก
// (ถอด item อื่นออกเพื่อเปิดที่ → เห็นทั้งตัวที่ถูกถอดและตัวที่ได้ที่ไปในตารางเดียวกัน)
// weekOfDate = ตัวแปลงวันด้ายเข้า → เลขสัปดาห์ ; แปลงไม่ได้ = ไม่โชว์แถวด้ายเข้าของก้อนนั้น
// กติกาวันด้ายเข้า (หน้า Monitor Yarn: KP_YARN_POIN_ON / KP_YARN_POIN_LAG_WEEKS) — ค่าสำรองตอนอ่านค่าตั้งไม่ได้
// ต้องตรงกับ default ใน plan_config.py ; engine ใช้ค่าชุดเดียวกันที่ Planning.get_point_date_start_idx
const POIN_DEFAULT = { on: true, lag: 1 }

// axis = แกนสัปดาห์ตามปฏิทินจริง ใช้เทียบ "ก่อน/หลัง" ข้ามปี (ปฏิทินอ่านไม่ได้ → เทียบเลขสัปดาห์เปล่า)
// poin = กติกาวันด้ายเข้า {on, lag} — ปิด (on=false) = ไม่ขึ้น ⚠ วางก่อนด้ายเข้า เหมือน engine ที่ไม่ดูวันด้ายเข้า
function buildPlanCompare(columns, origRows, rows, selItem, weekOfDate, bookingItems, axis, poin = POIN_DEFAULT) {
  const ci = c => columns.indexOf(c)
  const iI = ci('ITEM_CODE'), wI = ci('PLAN_WEEK'), qI = ci('PRODUCE_QTY')
  if (iI < 0 || wI < 0 || qI < 0) return null
  // วันด้ายเข้า = PO_IN_DATE เท่านั้น (วันด้ายเข้าครบ ล็อตหลังสุด — Planning.po_in_fields)
  // ห้ามใช้ DATE_IN เด็ดขาด: ของงาน SC มันคือ "วันที่รับ order" ไม่ใช่วันด้ายเข้า
  const mI = ci('MC_GROUP'), dI = ci('PO_IN_DATE'), sI = ci('SC_SO_NO'), yI = ci('PLAN_YEAR')
  // YARN_STOCK = ใบที่ Replan ให้ยืมด้ายในสต๊อก ไม่ต้องรอ PO IN (YarnStockCover.py) → ไม่ตรวจ ⚠ วางก่อนด้ายเข้า
  const ysI = ci('YARN_STOCK')
  const sel = upcode(selItem)
  const eq = (a, b) => {
    const ks = new Set([...Object.keys(a), ...Object.keys(b)])
    for (const k of ks) if (Math.abs((a[k] || 0) - (b[k] || 0)) > QTY_TOL) return false
    return true
  }

  // 1) item ที่ถูกแก้ในเซสชันนี้ = ยอดต่อสัปดาห์ต่างจากไฟล์ตอนโหลด (เทียบที่ระดับ item)
  // คีย์สัปดาห์ของแถว (ปี+สัปดาห์ — weekKeys.js) ; '' = ไม่มีสัปดาห์
  const rwk = (r) => { const k = rowWeekKey(r, wI, yI); return Number.isFinite(wkNum(k)) && k !== '' ? k : '' }
  const byItem = (rs) => {
    const m = new Map()
    for (const r of (rs || [])) {
      const k = upcode(r[iI]), w = rwk(r)
      if (!k || !w) continue
      const bag = m.get(k) || {}
      bag[w] = (bag[w] || 0) + (Number(norm(r[qI])) || 0)
      m.set(k, bag)
    }
    return m
  }
  const wasByItem = byItem(origRows), nowByItem = byItem(rows)
  const edited = new Set()
  for (const k of new Set([...wasByItem.keys(), ...nowByItem.keys()])) {
    if (!eq(wasByItem.get(k) || {}, nowByItem.get(k) || {})) edited.add(k)
  }
  const want = new Set(edited)
  if (sel) want.add(sel)
  if (!want.size) return null

  // 2) บล็อก (item × เครื่อง) ของ item ที่สนใจ — เก่ามาจาก booking, ใหม่มาจากแถวแผน
  const groups = new Map()
  const slotOf = (item, mcg) => {
    const key = upcode(item) + '|' + upcode(mcg)
    let g = groups.get(key)
    if (!g) {
      g = {
        // so = SC ของแผนใหม่ ; soOld = SC ของ booking — ต้องแยกกัน เพราะ item เดียวกัน
        // carry ข้าม SC ได้ (Planning ใช้คีย์ item+เครื่อง ไม่มี SC) → booking ที่โผล่คู่กับ
        // แผนใหม่มักเป็นคนละใบ ถ้าโชว์ SC ตัวเดียวคร่อมทั้งก้อนจะอ่านเป็น "ของเก่าถูกยกมา replan"
        key, item, mcg, before: {}, after: {}, so: new Set(), soOld: new Set(),
        soOldMore: false, yarn: '',
        // ด่านด้ายเข้าเป็นของ "รายใบสั่ง" ไม่ใช่ของทั้งก้อน — scPo เก็บวันด้ายเข้า + สัปดาห์ที่วาง
        // แยกตาม SC ของแถวแผน ; yarnBk = วัน PO เข้าจาก booking ใช้เป็นตัวโชว์สำรองเท่านั้น
        scPo: new Map(), yarnBk: '',
        // แยกยอดแผนใหม่ตามที่มาของงาน — คนละเรื่องกันแต่เดิมรวมอยู่ช่องเดียว:
        //   afterR = ใบที่มีใน booking อยู่แล้ว ถูกยกมาวางใหม่ (replan จริง)
        //   afterN = ใบที่ไม่เคยมีใน booking วางแผนครั้งแรก (ไม่ใช่การแก้แผน)
        afterR: {}, afterN: {}, scReplan: new Set(), scNew: new Set(), scDrop: new Set(),
        isSel: upcode(item) === sel, edited: edited.has(upcode(item)),
      }
      groups.set(key, g)
    }
    return g
  }
  for (const b of (bookingItems || [])) {
    if (!want.has(upcode(b.item))) continue
    const g = slotOf(b.item, b.mc_group)
    const w = String(b.week ?? '')              // backend ส่งคีย์คอลัมน์มาแล้ว (plan_view.week_key)
    if (!w || !Number.isFinite(wkNum(w))) continue
    g.before[w] = (g.before[w] || 0) + (Number(b.qty) || 0)
    // SC ของ booking ก้อนนี้ — so_list = ทุกใบที่ยุบอยู่ในบล็อก (backend ตัดที่ 30 ใบ,
    // so_n = จำนวนจริง) ; ไฟล์เก่าที่ยังไม่มี so_list ถอยไปใช้ so ตัวเดียว
    const _bso = b.so_list && b.so_list.length ? b.so_list : (b.so ? [b.so] : [])
    for (const s of _bso) for (const t of scList(s)) g.soOld.add(t)
    if (Number(b.so_n || 0) > _bso.length) g.soOldMore = true
    // ไม่มี PO_IN_DATE ฝั่งแผน (เช่นก้อนที่มีแต่ใน booking) → ใช้วัน PO เข้าของ booking มา "โชว์"
    // ❗ ห้ามเอาไปตัดสิน ⚠ : booking ก้อนนี้ยุบหลายใบ (รวมใบที่หลุดจากแผนใหม่ไปแล้ว) และ backend
    // ส่งมาเป็นวันเข้า "เร็วสุด" ของบล็อก (plan_view: slot["po_in"]) ไม่ใช่วันด้ายเข้าครบของใบใด
    const d = toIsoDate(b.po_in)
    if (d && d > g.yarnBk) g.yarnBk = d
  }
  for (const r of (rows || [])) {
    if (!want.has(upcode(r[iI]))) continue
    const g = slotOf(norm(r[iI]), mI >= 0 ? norm(r[mI]) : '')
    const w = rwk(r)
    // booking ส่ง SC มาไม่เกิน 30 ใบต่อก้อน (plan_view.so_list) → ก้อนที่เกินจะเทียบไม่ครบ
    // ฟันว่า "ใบใหม่" ไม่ได้ ถือเป็น replan ทั้งก้อนไว้ก่อน แล้วโชว์ … ตามเดิม
    const scs = sI >= 0 ? scList(r[sI]) : []
    scs.forEach(s => g.so.add(s))
    // แถวที่ยุบหลายใบ แล้วมีใบใดใบหนึ่งอยู่ใน booking = งานเดิมถูกยกมาวางใหม่
    // (ยอดในแถวแยกต่อใบไม่ได้ จึงนับเข้าถัง replan ทั้งแถว)
    // แถวที่ไม่มีเลข SC เลย (ชีทไม่มีคอลัมน์ / ช่องว่าง) ห้ามเดาว่าเป็นใบใหม่ —
    // ถ้าก้อนนี้มี booking อยู่ ให้ถือเป็น replan ไว้ก่อน
    const isRe = g.soOldMore
      || (scs.length ? scs.some(s => g.soOld.has(s)) : Object.keys(g.before).length > 0)
    const q = Number(norm(r[qI])) || 0
    if (w) {
      g.after[w] = (g.after[w] || 0) + q
      const bag = isRe ? g.afterR : g.afterN
      bag[w] = (bag[w] || 0) + q
    }
    // ป้าย SC แยกรายใบตามความจริงของใบนั้น ๆ (ไม่ยึดผลรวมของแถวเหมือนฝั่งยอด)
    scs.forEach(s => (g.soOldMore || g.soOld.has(s) ? g.scReplan : g.scNew).add(s))
    // วันด้ายเข้าครบ "ของแถวนี้" (PO_IN_DATE ในชีทแผนเป็น 'dd/mm/yyyy' = ล็อตหลังสุดของใบนั้น)
    // เก็บแยกตาม SC พร้อมสัปดาห์ที่แถวนี้ถูกวาง เพื่อตรวจ ⚠ แบบเดียวกับที่ Planning บังคับจริง
    // (Planning.py: get_point_date_start_idx / POINT DATE CLAMP — บังคับต่อ order ไม่ใช่ต่อก้อน)
    // แถวที่ไม่มี SC เลยเก็บรวมเป็นถัง '—' ; แถวที่ยุบหลายใบใช้เลขใบทั้งชุดเป็นคีย์เดียว
    if (dI >= 0) {
      const sk = scs.length ? scs.join(', ') : '—'
      let b = g.scPo.get(sk)
      if (!b) { b = { sc: sk, yarn: '', weeks: new Map(), stock: '' }; g.scPo.set(sk, b) }
      const d = toIsoDate(r[dI])
      if (d && d > b.yarn) b.yarn = d
      if (ysI >= 0 && !b.stock) b.stock = norm(r[ysI]) || ''
      if (w) b.weeks.set(w, (b.weeks.get(w) || 0) + q)
    }
  }
  const list = [...groups.values()]
  if (!list.length) return null

  const weekSet = new Set()
  const _now = new Date()
  const todayIso = `${_now.getFullYear()}-${String(_now.getMonth() + 1).padStart(2, '0')}-${String(_now.getDate()).padStart(2, '0')}`
  // วันด้ายเข้าครบ → สัปดาห์แรกที่ทอได้ = สัปดาห์ที่ด้ายเข้า + poin.lag (ค่าเริ่มต้น 1 = สัปดาห์ถัดไป)
  // เทียบบนแกนปฏิทินจริง ไม่ใช่เลขสัปดาห์เปล่า — ด้ายที่เข้าไปแล้ว (เช่น 19/11/2025 = W47 ปีที่แล้ว)
  // ต้องไม่ดันแผนของปีนี้ (W36) ไป W48 ทั้งที่ด้ายมาถึงตั้งนานแล้ว
  const lag = Math.max(0, Number.isFinite(Number(poin?.lag)) ? Math.trunc(Number(poin.lag)) : POIN_DEFAULT.lag)
  const poinOn = poin?.on !== false
  const startOfYarn = (iso) => {
    const yIdx = iso && axis && axis.usable ? axis.idxOfDate(iso) : null
    if (yIdx != null) return { idx: yIdx + lag, week: axis.weekAt(yIdx + lag) }
    const yw = iso && weekOfDate ? weekOfDate(iso) : null
    // ไม่มีปฏิทิน → เลื่อนคีย์ไป lag ช่องบนแกนเรียง (ข้ามปีได้)
    return { idx: null, week: yw != null ? keyFromSortKey(weekSortKey(yw) + lag) : null }
  }
  for (const g of list) {
    // ❗ ตรวจ ⚠ รายใบสั่ง: เทียบ PO_IN_DATE ของแถวนั้น กับ PLAN_WEEK ของแถวนั้นเอง
    // ก้อน item×เครื่องเดียวกันยุบหลายใบที่ด้ายเข้าคนละวัน — ถ้าเทียบด้วยวันหลังสุดของทั้งก้อน
    // (โค้ดเดิม) ใบที่ด้ายมาถึงแล้วจะติด ⚠ ตามใบที่ด้ายยังไม่มา ทั้งที่แผนวางถูกกติกา
    g.poList = []
    for (const b of g.scPo.values()) {
      if (!b.yarn) continue                       // ใบที่ไม่มีวันด้ายเข้า (SC ใหม่) = ไม่มีด่านนี้
      const s = startOfYarn(b.yarn)
      const bad = []
      // ปิดกติกาวันด้ายเข้า = engine ไม่ดูวันด้ายเข้า → ไม่มีอะไรให้เตือน (ยังโชว์วันด้ายเข้าตามเดิม)
      // ใบที่ยืมด้ายในสต๊อก (b.stock) ก็เหมือนกัน — engine ไม่ใช้วันด้ายเข้าเป็นพื้นของใบนี้
      if (poinOn && !b.stock) for (const [w, q] of b.weeks) {
        if (Math.abs(q) <= QTY_TOL) continue      // สัปดาห์ที่ยอดเป็น 0 ไม่ถือว่าวางงาน
        const pIdx = s.idx != null && axis ? axis.idxOfWeek(w) : null
        const late = s.idx != null
          ? (pIdx != null && pIdx < s.idx)
          : (s.week != null && weekSortKey(w) < weekSortKey(s.week))
        if (late) bad.push(w)
      }
      bad.sort((x, y) => weekSortKey(x) - weekSortKey(y))
      g.poList.push({ sc: b.sc, yarn: b.yarn, week: s.week, past: b.yarn < todayIso, bad, stock: b.stock })
    }
    g.poList.sort((a, b) => (a.yarn < b.yarn ? -1 : a.yarn > b.yarn ? 1 : 0))
    g.poPushed = g.poList.some(p => p.bad.length > 0)
    g.poinOff = !poinOn
    // คอลัมน์ POIN DATE (ซ้ายมือ) สรุปด้วยใบที่ด้ายมาช้าสุด — รายละเอียดรายใบอยู่แถวล่าง
    // ไม่มีใบไหนในแผนใหม่มีวันด้ายเข้าเลย → ถอยไปโชว์วันจาก booking (โชว์เฉย ๆ ไม่ตัดสิน ⚠)
    const _last = g.poList.length ? g.poList[g.poList.length - 1] : null
    g.yarn = _last ? _last.yarn : (g.yarnBk || '')
    g.yarnWeek = g.yarn && weekOfDate ? weekOfDate(g.yarn) : null
    g.yarnStartWeek = _last ? _last.week : startOfYarn(g.yarn).week
    g.yarnPast = !!g.yarn && g.yarn < todayIso     // ด้ายเข้าไปแล้ว = ไม่เป็นข้อจำกัดของแผน
    g.yarnFromBooking = !_last && !!g.yarn
    g.hasPo = !!g.yarn && g.yarnStartWeek != null
    g.hasBooking = Object.keys(g.before).length > 0
    Object.keys(g.before).forEach(w => weekSet.add(w))        // คีย์สัปดาห์ (ปี+สัปดาห์) — ห้ามแปลงเป็นเลข
    Object.keys(g.after).forEach(w => weekSet.add(w))
    // แถว POIN DATE โชว์เป็นข้อความ "สัปดาห์ที่พร้อมวางแผน" ไม่ใช่ยอดรายสัปดาห์แล้ว
    // → ไม่ต้องเปิดคอลัมน์สัปดาห์เพิ่มให้มัน (เดิมเปิดไว้จนตารางกว้างเกินจำเป็น)
    const sum = o => Math.round(Object.values(o).reduce((s, v) => s + v, 0) * 10) / 10
    g.totalBefore = sum(g.before); g.totalAfter = sum(g.after)
    g.totalR = sum(g.afterR); g.totalN = sum(g.afterN)
    // ใบที่ booking มีแต่แผนใหม่ไม่หยิบไปเลย — ช่องแดงบอกแค่ว่า "สัปดาห์นี้หายไป" ไม่ได้บอกว่าใบไหนหลุด
    for (const s of g.soOld) if (!g.so.has(s)) g.scDrop.add(s)
  }
  // ── เรียงคอลัมน์บนแกนเวลาจริง ─────────────────────────────────────────────────────
  // คอลัมน์เป็นคีย์สัปดาห์ (ปี+สัปดาห์ — weekKeys.js) อยู่แล้ว ทั้งฝั่งแผน (PLAN_YEAR) และ booking
  // (backend ติดปีมาให้) → W2..W5 ของปีถัดไปต่อท้าย W47 ของปีนี้ และ W35 สองปีไม่ยุบเป็นคอลัมน์เดียว
  const wkKey = (w) => weekSortKey(w)
  const weeks0 = [...weekSet].filter(w => w && Number.isFinite(wkNum(w))).sort((a, b) => wkKey(a) - wkKey(b))
  // ── เติมสัปดาห์ที่ "ไม่มีของ" ระหว่างสัปดาห์แรก–สุดท้าย ให้แกนคอลัมน์ต่อเนื่อง ─────────
  // เดิมโชว์เฉพาะสัปดาห์ที่มียอด → W40 กับ W42 มาชิดกัน คนอ่านเข้าใจว่าทอต่อเนื่อง
  // ทั้งที่จริงเว้น W41 ไว้ (ไม่ได้วางแผน) ; เดินบนแกนปฏิทินจริงเพื่อให้ข้ามปี/W53 ถูกต้อง
  // W99 = sentinel งานล้น ไม่ใช่เวลาจริง → ไม่นับในช่วงเติม แต่ยังต่อท้ายเหมือนเดิม
  const realW = weeks0.filter(w => wkNum(w) !== 99)
  const tailW = weeks0.filter(w => wkNum(w) === 99)
  const filled = []
  if (realW.length >= 2) {
    const wFirst = realW[0], wLast = realW[realW.length - 1]
    const i0 = axis && axis.usable ? axis.idxOfWeek(wFirst) : null
    const i1 = axis && axis.usable ? axis.idxOfWeek(wLast) : null
    if (i0 != null && i1 != null && i1 > i0 && i1 - i0 <= MAX_GAP_FILL) {
      for (let i = i0; i <= i1; i++) { const w = axis.weekAt(i); if (w != null) filled.push(w) }
    } else {
      // ปฏิทินอ่านไม่ได้ → เดินบนคีย์เรียง (เลขสัปดาห์เปล่า +53 ต่อปีถัดไป) แล้วแปลงกลับเป็นเลขสัปดาห์
      const k0 = wkKey(wFirst), k1 = wkKey(wLast)
      if (Number.isFinite(k0) && Number.isFinite(k1) && k1 > k0 && k1 - k0 <= MAX_GAP_FILL) {
        const known = new Map(realW.map(w => [wkKey(w), w]))
        for (let k = k0; k <= k1; k++) filled.push(known.has(k) ? known.get(k) : keyFromSortKey(k))
      }
    }
  }
  // เติมแล้วต้องไม่ทำสัปดาห์ที่ "มีของจริง" หายไป (แกนแมตช์ผิดรอบปีได้) → ไม่ครบก็ใช้ชุดเดิม
  const okFill = filled.length > 0 && realW.every(w => filled.includes(w))
  const weeks = (okFill ? filled : realW).concat(tailW)
  // สัปดาห์ที่เติมเข้ามาเอง (ไม่มีทั้ง booking และแผนใหม่) → หัวคอลัมน์/ช่องจางลง อ่านออกว่า "ไม่มีแผน"
  const emptyWeeks = new Set(weeks.filter(w => !weekSet.has(w)))
  // คอลัมน์ที่ freeze จริง — สัปดาห์ปีหน้าเลขน้อย (W10) ต้องไม่ถูกล็อกแม้เลขจะน้อยกว่าสัปดาห์ปัจจุบัน
  // คีย์คอลัมน์ติดปีเมื่อไกล → isFrozenWeek ตัดสินปีจากคีย์เองได้ (เลขเปล่า = ครั้งที่ใกล้วันนี้สุด)
  const frozen = new Set(weeks.filter(w => isFrozenWeek(w)))
  list.sort((a, b) => (b.isSel - a.isSel) || (b.edited - a.edited) || a.item.localeCompare(b.item))
  return { weeks, list, nEdited: edited.size, frozen, emptyWeeks }
}

// คอลัมน์ที่มี dropdown กรองด่วนบนแถบค้นหา (ใช้ตัวกรองชุดเดียวกับปุ่ม ▾ ที่หัวคอลัมน์)
const QUICK_COLS = [
  { col: 'CAT', label: 'CAT' },
  { col: 'MC_GUAGE', label: 'Gauge' },
]

// ค้นหาใน record ของ booking (แผนเก่า) ซึ่งเป็น object ไม่ใช่แถว/คอลัมน์เหมือนตารางแผน
// → เทียบทุกฟิลด์ที่ planner ใช้ค้นจริง ให้ผลเหมือนช่อง "ค้นหาทุกคอลัมน์" ของตาราง
const BK_SEARCH_FIELDS = ['item', 'so', 'material', 'mc_group', 'cat', 'gauge', 'week']

// ขั้นตอนของงาน "ดึงประวัติจาก DB" (plan_history._fetch_job) → ข้อความไทยบนแถบสถานะ
const JOB_STEP = {
  queued: 'เตรียมดึงข้อมูล',
  fetch_db: '1/3 ดึง booking จาก DB',
  ava_mc: '2/3 คำนวณเครื่อง (AVA_MC)',
  save: '3/3 บันทึกลงทะเบียน',
  done: 'เสร็จแล้ว',
  error: 'ผิดพลาด',
}
// เวลางานดึงประวัติที่ backend คิดมาให้ (วินาที) → ข้อความไทยสั้น ๆ
const jobMins = (sec) => {
  const m = Math.round((Number(sec) || 0) / 60)
  return m >= 60 ? `${Math.floor(m / 60)} ชม. ${m % 60} นาที` : `${m} นาที`
}
// งานดึงประวัติรอบล่าสุดที่ "แจ้งผู้ใช้ไปแล้ว" (เวลาที่งานจบ) — งานทำเบื้องหลังที่ server
// จบตอนไม่ได้เปิดหน้าไว้ได้ ต้องแจ้งครั้งแรกที่กลับเข้ามา แต่ไม่ใช่แจ้งซ้ำทุกครั้งที่เปิดหน้า
const HIST_SEEN_KEY = 'kp-hist-job-seen'
const bkMatch = (b, s) => !s || BK_SEARCH_FIELDS.some(f => String(b[f] ?? '').toLowerCase().includes(s))
const upcode = v => String(v ?? '').trim().toUpperCase()
// SC_SO_NO ช่องเดียวเก็บได้หลายใบ ('721282,721257' — Planning ยุบงานที่รวมกันได้ไว้แถวเดียว)
// → ต้องแตกเป็นรายใบก่อนเทียบกับ booking ไม่งั้นก้อนที่ยุบมาจะไม่เคยแมตช์เลย
const scList = v => String(v ?? '').split(/[,;|/]+/).map(s => s.trim())
  .filter(s => s && s.toLowerCase() !== 'nan')

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

// สถานะการรัน (จาก /api/run/status) เท่ากันไหม — เป็น dict แบน ๆ ไม่กี่คีย์ เทียบด้วย JSON ได้
// ใช้กัน setState ซ้ำจาก polling ทุก 2 วินาที ซึ่งทำให้ Gantt ทั้งตารางถูกเรนเดอร์ใหม่เปล่า ๆ
function sameStatus(a, b) {
  if (a === b) return true
  if (!a || !b) return false
  try { return JSON.stringify(a) === JSON.stringify(b) } catch { return false }
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

// เงียบจากข่าว "มีคนแก้แผน" นานเท่านี้ = อีกฝั่งหยุดมือแล้ว ค่อยดึงของใหม่มาแปะทีเดียว
const REMOTE_QUIET_MS = 1500

export default function KnitPlan({ active = true }) {
  const [meta, setMeta] = useState(null)         // ข้อมูลไฟล์แผนล่าสุด
  const [grid, setGrid] = useState(null)          // { sheet, sheets, columns, rows, name, mtime }
  // ตัวชี้ไปยัง grid ล่าสุด — ตัวรับข่าวจากจออื่นถูกผูกครั้งเดียวตอนเปิดหน้า จึงอ่าน state
  // ผ่าน closure ไม่ได้ (จะได้ค่าตอนผูก ไม่ใช่ค่าปัจจุบัน)
  const gridRef = useRef(null)
  gridRef.current = grid
  const planRevRef = useRef(null)      // เลขรุ่นของชีทแผนที่จอนี้ถืออยู่ (กันเขียนทับคนอื่น)
  const remoteTimer = useRef(null)     // ตัวหน่วง "รอให้อีกฝั่งหยุดมือ" ก่อนดึงของใหม่
  // กำลังลากบล็อกค้างอยู่ไหม — ห้ามสลับแถวใต้เมาส์ระหว่างลาก (เลขแถวที่ถืออยู่จะชี้ผิดงาน)
  const draggingRef = useRef(false)
  // ของใหม่จากจออื่นที่ยังแปะไม่ได้ (เรามีของค้างยังไม่เซฟ / กำลังลาก) — { by, at, sheet }
  const [remoteWait, setRemoteWait] = useState(null)
  const [dragTick, setDragTick] = useState(0)   // +1 ทุกครั้งที่ปล่อยเมาส์จากการลาก
  const [savedAt, setSavedAt] = useState(null)
  const [showBalance, setShowBalance] = useState(false)   // กางรายการออร์เดอร์ที่ยอดไม่ตรง
  const [loading, setLoading] = useState(false)
  const [dirty, setDirty] = useState(false)
  // ตัวชี้ไปยัง dirty ล่าสุด — เหตุผลเดียวกับ gridRef (ตัวรับข่าวอ่านค่าปัจจุบันไม่ได้ทาง closure)
  const dirtyRef = useRef(false)
  dirtyRef.current = dirty
  const [msg, setMsg] = useState('')
  const [search, setSearch] = useState('')
  // ตัวกรอง item แบบ Excel (ติ๊กหลายตัว) — Set ของ ITEM_CODE ที่เลือก, null = ทั้งหมด
  // โดเมนรวม item จากทั้งแผนวันนี้และแผนเก่า (booking) จึงเลือก item เก่าที่ไม่ได้วางวันนี้ได้ด้วย
  const [itemPick, setItemPick] = useState(null)
  const [itemPop, setItemPop] = useState(null)     // anchor ของป็อปอัพ (null = ปิด)
  const searchRef = useRef(null)                   // กล่องครอบช่องค้นหา + ป็อปอัพ (ใช้เช็คคลิกนอกกล่อง)
  // เปิดป็อปอัพใต้ช่องค้นหา — ยัด right = left+300 (ความกว้าง .filterpop) ให้ป็อปอัพชิดซ้ายตรงกับช่อง
  const openItemPop = () => {
    const r = searchRef.current?.getBoundingClientRect()
    if (r) setItemPop({ top: r.top, bottom: r.bottom, left: r.left, right: r.left + 300 })
  }
  // ปิดเมื่อคลิกนอกกล่อง/กด Esc — ป็อปอัพนี้ไม่มีฉากหลังคลุมจอ (ไม่งั้นพิมพ์ต่อในช่องค้นหาไม่ได้)
  useEffect(() => {
    if (!itemPop) return
    const onDown = e => { if (searchRef.current && !searchRef.current.contains(e.target)) setItemPop(null) }
    const onKey = e => { if (e.key === 'Escape') setItemPop(null) }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [itemPop])
  const [filters, setFilters] = useState({})
  // ตัวกรองคอลัมน์ของ "ตารางใน modal" (แยกจากตัวกรองหน้าหลัก) — กรองแล้วกระทบเฉพาะตารางเล็ก
  // ไม่ไปขยับตารางใหญ่/Gantt ข้างหลัง ; { ci: Set(ค่าที่ติ๊กไว้) } รูปแบบเดียวกับ filters
  const [modalFilters, setModalFilters] = useState({})
  const [openCol, setOpenCol] = useState(null)   // { ci, scope: 'main' | 'modal', anchor }
  const [colW, setColW] = useState({})       // ความกว้างคอลัมน์ (ผู้ใช้ลากปรับได้)
  const [editKey, setEditKey] = useState(null) // ช่องที่กำลังแก้ (โชว์ค่าดิบไม่มี comma)
  const [runStatus, setRunStatus] = useState({})
  // (โหมด 🧶 แผนด้ายที่เคยสลับในส่วน Gantt แยกไปเป็นหน้าของตัวเองแล้ว 15 ต.ค. 2569 — YarnPlan.jsx / App.jsx)
  const [showGantt, setShowGantt] = useState(true)
  // overlay item จาก booking (แผนเก่า) บน Gantt: 'off' | 'all' (ทุก item) | 'plan' (เฉพาะ item ที่ทำแผนวันนี้)
  // เริ่มที่ 'off' — เปิดหน้ามาให้เห็นแผนใหม่ล้วนก่อน ไม่ให้บล็อกประวัติมาบังตั้งแต่แรก
  // ; ใครอยากดูประวัติค่อยกดปุ่ม "Item (ทั้งหมด)" / "Item (วางแผนวันนี้)" เอง
  // (งานที่ล็อกแผนไว้ 🔐 ยังโชว์ต่อแม้โหมด off เพราะ Replan คงงานนั้นไว้ที่เดิม)
  const [bookingMode, setBookingMode] = useState('off')
  // งานที่ Replan ยกไปวางใหม่ที่อื่นแล้ว — ปิดไว้เสมอตอนเปิดหน้า (ตำแหน่งเดิมไม่ใช่แผนจริงแล้ว
  // เห็นแล้วอ่านเป็น "แผนยังค้างงานไว้ตรงนั้น") ; เปิดได้เมื่ออยากเทียบว่าของเดิมอยู่ตรงไหน
  const [showReplanned, setShowReplanned] = useState(false)
  const [bookingItems, setBookingItems] = useState([])   // item จาก booking DETAIL ต่อ (สัปดาห์×CAT×เกจ×เครื่อง)
  // booking "เต็มทุกสัปดาห์" — ใช้เฉพาะตารางเทียบในการ์ด ; ชีท REPLAN เท่านั้นที่ต้องมี เพราะ
  // bookingItems ของชีทนั้นถูกตัดเหลือแต่สัปดาห์ที่ล็อก (กันบล็อกซ้ำบน Gantt) ซึ่งทำให้แถว
  // "เก่า (booking)" ว่างทั้งที่งานนั้นมีอยู่จริงในสัปดาห์ที่ถูกปลดล็อกมาวางใหม่
  const [bookingAll, setBookingAll] = useState(null)
  // ── ประวัติแผนย้อนหลังลึก (ทะเบียนสะสม plan_history.xlsx) ─────────────────
  // ไฟล์ booking ก้อนเดียวมีอดีตแค่ ~6 สัปดาห์ (ใบที่ส่งจบแล้วหลุดออกจาก view) และไฟล์เก่า
  // ถูกลบทุก 14 วัน → ย้อนลึกกว่านั้นต้องดึงจากทะเบียนที่ backend สะสมไว้ ทีละครั้งที่ user สั่ง
  //   histRange = ปี/สัปดาห์ที่ "มีข้อมูลจริง" ให้เลือก (ไม่ให้เลือกช่วงว่างแล้วงงว่าทำไมไม่ขึ้น)
  //   histSel   = ที่เลือกใน dropdown (ยังไม่ได้กดดึง) / histFrom = จุดเริ่มของข้อมูลที่ดึงมาแล้ว
  const [histRange, setHistRange] = useState(null)
  const [histSel, setHistSel] = useState(null)
  const [histFrom, setHistFrom] = useState(null)
  const [histItems, setHistItems] = useState([])
  const [histBusy, setHistBusy] = useState(false)
  const [histErr, setHistErr] = useState('')
  // ข้อความแจ้งผลงานดึง (✅ เสร็จแล้ว …) — คนละช่องกับ histErr เพราะไม่ใช่ความผิดพลาด
  const [histNote, setHistNote] = useState('')
  // สถานะงานดึงจาก DB (ยาว ~16 นาที/7 สัปดาห์) — poll จาก backend ระหว่างที่ user ทำอย่างอื่นได้
  const [histJob, setHistJob] = useState(null)
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
  const [progOnly, setProgOnly] = useState(false)      // กรองเหลือเฉพาะ Item Program (ปุ่มอยู่แถบตัวกรอง)
  const [coreOnly, setCoreOnly] = useState(false)      // กรองเหลือเฉพาะ Item Core (ปุ่มคู่กับ Item Program)
  const [holdOnly, setHoldOnly] = useState(false)      // กรองเหลือเฉพาะ item ที่มีสัปดาห์ Hold (สี/target)
  const [barFields, setBarFields] = useState(loadBarFields)   // ฟิลด์ที่โชว์บนบล็อก Gantt (ปุ่มอยู่แถบ Gantt)
  const [showFieldBar, setShowFieldBar] = useState(false)
  useEffect(() => { saveBarFields(barFields) }, [barFields])
  // โหมด "SC โครงร่าง" — บล็อก Forecast (ยังไม่ให้สี) เหลือแค่กรอบ พื้นขาว
  // → กวาดตาเห็นทันทีว่าก้อนไหนเป็นยอดที่ลูกค้าเปิดสีแล้ว (ทึบ) ก้อนไหนยังไม่ยืนยัน
  const [scOutline, setScOutline] = useState(loadScOutline)
  useEffect(() => { saveScOutline(scOutline) }, [scOutline])
  // chip "ข้อมูลบนบล็อก" ที่ใช้ได้กับชีทที่กำลังดู — ชีทต่างกันมีคอลัมน์ไม่เหมือนกัน
  // (เช่น SETUP_TRACKING ไม่มี PO/RDD/ลูกค้า/สี/เนื้อผ้า → ติ๊กแล้วไม่มีอะไรขึ้น จึงไม่โชว์ chip)
  const sheetBarFields = useMemo(() => barFieldsFor(grid?.columns || []), [grid?.columns])
  const [showOutsource, setShowOutsource] = useState(false)
  const [showCylinder, setShowCylinder] = useState(false)
  const [showPivot, setShowPivot] = useState(false)   // ตาราง Pivot ทุก item (modal)
  const [showMail, setShowMail] = useState(false)     // แจ้งเมลแผนเปลี่ยน (เฉพาะชีท REPLAN)
  // แถวที่เลือกจากการคลิกบล็อก Gantt (idx ของแถวในชีท) → เปิด modal ตาราง + การ์ดคู่กัน
  const [selJob, setSelJob] = useState(null)
  // เปิด modal จากบล็อก booking (History/งานล็อก) → รู้แค่ชื่อ item ไม่มีเลขแถว
  // งานล็อกที่อยู่แต่ใน booking ไม่มีแถวในชีทแผนเลย แต่ตารางเทียบ booking → แผนใหม่ ยังต้องดูได้
  const [modalItem, setModalItem] = useState(null)
  const closeModal = () => { setSelJob(null); setModalItem(null) }
  useEffect(() => { if (selJob != null) setModalItem(null) }, [selJob])
  const modalRowRef = useRef(null)
  // เปิด modal แล้วเลื่อนตารางไปหาแถวที่คลิก + ไฮไลต์
  // ล้างตัวกรองของตารางใน modal ทุกครั้งที่เปลี่ยนบล็อก — ค่าที่ติ๊กไว้เป็นของ item เดิม
  // ถ้าไม่ล้าง item ใหม่จะไม่มีแถวไหนตรงเลย = เปิดมาแล้วตารางว่างโดยไม่มีสาเหตุ
  useEffect(() => {
    setModalFilters({})
    setOpenCol(o => (o?.scope === 'modal' ? null : o))
    if (selJob == null) return
    const t = setTimeout(() => modalRowRef.current?.scrollIntoView({ block: 'center' }), 40)
    return () => clearTimeout(t)
  }, [selJob, modalItem])
  const [load, setLoad] = useState({})
  // รายการ job ที่ตั้งเครื่องใหม่ (ชีท SETUP_TRACKING) — ใช้ในการ์ดที่เปิดจากแถบโหลดบน Gantt
  const [setupJobs, setSetupJobs] = useState([])
  const [ava, setAva] = useState({})
  // map เครื่อง→พูล (เช่น SKP vs SKPTA/SKPLE) สำหรับหา ava/reserved ต่อพูล
  const [poolMap, setPoolMap] = useState({})
  // เครื่องของผู้รับจ้างทอต่อ (กลุ่ม|เกจ) จากชีท MC S9 → { "RAP|22": {total, cap, wd, cats} }
  // Gantt จ้างทอใช้เป็นเพดานเครื่องแทนพูลในบ้าน (แถวจ้างทอเป็น COMKN ซึ่งไม่มีเครื่องในโรงงานเรา)
  const [s9Pool, setS9Pool] = useState({})
  // CAP ของผู้รับจ้างต่อผ้า { "ITEM|เกจ": กก./เครื่อง/วัน } จากชีท Item S9 — มีค่า = ใช้แทน cap ของพูล
  const [s9ItemCap, setS9ItemCap] = useState({})
  // คู่ "ITEM|เกจ" ที่จ้างทอได้ (ชีท Item S9) — การ์ดงานในบ้านของคู่เหล่านี้มีปุ่ม "🧵 ส่งไปจ้างทอ"
  const [s9Elig, setS9Elig] = useState(() => new Set())
  // ยุบแถวจ้างทอเข้ากับแถวเครื่องในบ้านของผ้าตัวเดียวกัน (item + เกจ) — ใบสั่งเดียวกันที่แบ่ง
  // บางส่วนออกไปจ้างทอจะได้อ่านต่อเนื่องในแถวเดียว ไม่ต้องไล่หาแถว S9- อีกที่หนึ่ง
  // เปิดตลอด (เอาปุ่มสลับ รวม/แยก ออกแล้ว) — ผลคือแถวพูลผู้รับจ้างหายไปเมื่อผ้าตัวนั้นมีงานในบ้าน
  // จึงลากงานในบ้านไปจ้างทอ และล็อกแผนงานจ้างทอจาก Gantt ไม่ได้
  const mergeS9 = true
  // map เครื่อง→"ก้อน" ของทุกกลุ่ม (โรง+CAT+เกจ+Group) — เครื่องก้อนเดียวกันใช้แทนกันได้
  // → ลูกโซ่ carry/setup คิดเป็นก้อน ไม่แยกตามชื่อกลุ่ม (ตรงกับ _ck ใน Planning.py)
  const mcGroupsRef = useRef(null)
  // เครื่องที่ booking ทอไอเทมนั้นอยู่แล้ว ต่อ (สัปดาห์ × ITEM|MC_GROUP|GUAGE)
  const [bookingMc, setBookingMc] = useState({})
  // payload วันทำงานตามกลุ่มเครื่อง (ชีท Work Day + ปฏิทินสด) — ใช้คำนวณวันทำงานราย (mc,gauge,week) ตอนลากงาน
  const [workdayData, setWorkdayData] = useState(null)
  // ชีท Program ใน MasterMC → { ITEM_CODE: [TEAM, ...] } ที่ต้องโชว์เป็นตัวหนังสือสีน้ำเงิน
  const [program, setProgram] = useState({})
  // รหัส item ที่ "ให้สีแล้ว" (มี LOAD_DYE ในไฟล์ Order Color) — Gantt ใช้แยกสีบล็อก 2 แบบ
  const [dyeCodes, setDyeCodes] = useState(() => new Set())
  // ล็อกแผน (ชีท Lock_Plan ใน MasterMC) — งาน booking ที่ห้าม Replan รื้อไปวางใหม่
  const [planLocks, setPlanLocks] = useState([])
  const prevRunning = useRef(false)
  // setup ต่อเครื่อง (วัน) รายแถว — สร้างครั้งเดียวตอนโหลดชีทจากค่าที่ Planning.py เขียนไว้
  const setupDaysRef = useRef(null)
  // เครื่องอุ่นจากนอกแผน (booking / แผนรอบก่อน) ต่อลูกโซ่ — ต้องอ่านจากไฟล์ก่อน user แก้เช่นกัน
  const warmSeedRef = useRef({})

  async function loadMeta() {
    const m = await api.planLatest()
    setMeta(m)
    return m
  }

  // ---- มีคนอื่นแก้ → เอาของใหม่มาแสดงเอง ไม่ต้องรอใครกดรีเฟรช ----
  // ค่าตั้งเครื่องที่หน้านี้ต้องคำนวณเองตอนลาก Gantt (เว้นกี่สัปดาห์ยังอุ่น · วัน setup)
  // ต้องใช้ค่าชุดเดียวกับที่ engine วางแผนจริง ไม่งั้นตัวเลขเครื่องบนจอไม่ตรงกับแผน
  // โหลดตอนเปิดหน้า + ตามทุกครั้งที่มีคนบันทึกค่าใหม่ (ไม่ต้องรีเฟรชจอเอง)
  // poinRule = กติกาวันด้ายเข้า (หน้า Monitor Yarn) ใช้ตัดสิน ⚠ "วางก่อนด้ายเข้า" ในตารางเทียบแผน
  const [poinRule, setPoinRule] = useState(POIN_DEFAULT)
  useEffect(() => {
    let dead = false
    let retry = null
    let tries = 0
    const pull = () => api.planConfig().then(d => {
      if (dead) return
      tries = 0
      const items = (d?.groups || []).flatMap(g => g.items || [])
      const val = k => items.find(it => it.key === k)?.value
      setPlanConstants({
        KP_SETUP_GAP_WEEK: val('KP_SETUP_GAP_WEEK'),
        KP_SETUP_DAYS: val('KP_SETUP_DAYS'),
        KP_SETUP_DAYS_HEAVY: val('KP_SETUP_DAYS_HEAVY'),
      })
      // สัปดาห์ที่แก้แผนไม่ได้ — ต้องเท่ากับพื้นที่ engine ใช้ ไม่งั้นลากงานเข้าสัปดาห์ที่วางไม่ได้
      // (PlanGantt ฟังค่านี้ผ่าน useFreezeWeeks → แกนสัปดาห์/ช่องที่วางได้วาดใหม่เอง)
      setFreezeWeeks(val('KP_FREEZE_WEEKS'))
      // กติกาวันด้ายเข้า — ต้องตรงกับที่ engine ใช้ ไม่งั้น ⚠ ขึ้น/ไม่ขึ้นไม่ตรงกับแผนจริง
      const _on = val('KP_YARN_POIN_ON'), _lag = Number(val('KP_YARN_POIN_LAG_WEEKS'))
      setPoinRule({
        on: _on == null ? POIN_DEFAULT.on : _on !== false,
        lag: Number.isFinite(_lag) ? _lag : POIN_DEFAULT.lag,
      })
    }).catch(() => {
      // โหลดไม่ได้ (เน็ตสะดุด / server กำลังรีสตาร์ท) → ลองใหม่ ไม่งั้นจอค้างค่าเริ่มต้น
      // (freeze 2 สัปดาห์ ฯลฯ) ไปจนกว่าจะมีคนบันทึกค่าใหม่ ซึ่งอาจไม่ตรงกับที่ engine ใช้
      if (dead || tries >= 5) return
      tries += 1
      clearTimeout(retry)
      retry = setTimeout(pull, 3000 * tries)
    })
    pull()
    const off = live.subscribe(live.RES.planConfig, pull)
    return () => { dead = true; clearTimeout(retry); off?.() }
  }, [])
  // ตารางเทียบแผนลงลายคาดคอลัมน์ freeze ตอนเรนเดอร์ (isFrozenWeek) — ต้องวาดใหม่เมื่อค่าเปลี่ยนด้วย
  useFreezeWeeks()

  // ล็อกแผน: ดึงรายการล็อกใหม่อย่างเดียวพอ (ไม่ต้องโหลดตารางทั้งใบ) — เครื่องหมายกุญแจ
  // บนบล็อกจะขยับตามคนอื่นทันที คนสองคนจะได้ไม่ไปล็อกงานเดียวกันคนละทาง
  useEffect(() => live.subscribe(live.RES.planLock, () => {
    api.planLocks().then(setPlanLocks).catch(() => { })
  }), [])

  // ติ๊กเปลี่ยนสูบ: เครื่องว่างรายสัปดาห์เปลี่ยนไปทั้งกระดาน → ต้องโหลดชุดเครื่องใหม่
  useEffect(() => live.subscribe(live.RES.cylinder, () => {
    const sh = gridRef.current?.sheet
    api.planAva(sh).then(setAva).catch(() => { })
  }), [])

  // รันแผนรอบใหม่จบ: ไฟล์แผนเป็นคนละใบแล้ว — ของบนจอเก่าทั้งหมด
  useEffect(() => live.subscribe(live.RES.run, () => {
    loadMeta().catch(() => { })
    setMsg('📢 มีการรันแผนรอบใหม่เสร็จแล้ว — กด "โหลดแผนล่าสุด" เพื่อดูของใหม่')
  }), [])

  // คนอื่นลาก Gantt อยู่คนละจอ → เอาของใหม่มาแปะให้เอง
  //
  // รวมข่าวที่มาติด ๆ กันแล้วค่อยดึงครั้งเดียว: คนที่กำลังจัดแผนอยู่ถูกเซฟให้ทุก 3 วินาที
  // ถ้าดึงตามทุกข่าว บล็อกบนจอนี้จะขยับตามมือเขาเป็นระยะ ๆ ตลอดเวลาที่เขาจัด
  // → รอให้เงียบ 1.5 วิก่อน แล้วขยับทีเดียวตอนเขาหยุดมือ
  useEffect(() => {
    const sheet = grid?.sheet
    if (!sheet) return
    const off = live.subscribe(live.RES.plan(sheet), (ent) => {
      // ข่าวของการเซฟของเราเอง (เลขรุ่นตรงกับที่ถืออยู่) — ไม่ต้องดึงอะไรทั้งนั้น
      if (!ent || ent.rev == null || ent.rev === planRevRef.current) return
      clearTimeout(remoteTimer.current)
      remoteTimer.current = setTimeout(() => pullRemote(sheet, ent), REMOTE_QUIET_MS)
    })
    return () => { off(); clearTimeout(remoteTimer.current) }
  }, [grid?.sheet])
  // ⚡ สถานะการรันถูก poll ทุก 2 วินาที — ถ้า setState ด้วย object ใหม่ทุกครั้ง (แม้ค่าเท่าเดิม)
  // React จะเรนเดอร์หน้าแผน + Gantt ทั้งตารางใหม่ทุก 2 วินาทีตลอดเวลา = หน่วงเป็นจังหวะ
  // → เทียบค่าก่อน เหมือนเดิมก็คืน state เดิม (ไม่เกิดการเรนเดอร์)
  async function loadRunStatus() {
    try {
      const s = await api.runStatus()
      setRunStatus(cur => (sameStatus(cur, s) ? cur : s))
    } catch { }
  }
  // sheet = ชีทที่เปิดอยู่ — ชีท REPLAN มีเครื่องว่าง/โควตา setup คนละชุดกับ PLAN
  // (งาน booking ที่ปลดล็อกกลายเป็นแถวแผนแล้ว เครื่องของสัปดาห์นั้นจึงถูกคืนเป็นว่าง)
  async function loadLoad(sheet) {
    try { setLoad(await retry(() => api.planLoad(sheet))) } catch { setLoad({}) }
    // รายการ job รายตัวของแถบโหลด (การ์ดที่เปิดจากการคลิกช่องแถบโหลด) — มาจากไฟล์แผนเดียวกัน
    try { setSetupJobs(await retry(() => api.planSetupJobs(sheet))) } catch { setSetupJobs([]) }
  }
  // replan = ชีทนี้เป็นแผน replan (backend ตัดสินจากรุ่นไฟล์ ส่งมาเป็น grid.replan)
  // ไฟล์รุ่นใหม่ชีท PLAN คือแผน replan ; รุ่นเก่ามีชีท REPLAN แยก → ค่าเริ่มต้นเช็คชื่อไว้เผื่อ
  async function loadAva(sheet, replan = sheetKey(sheet) === 'REPLAN') {
    // ava ควรมีข้อมูลเมื่อมีไฟล์แผน → ถ้าได้ว่าง ลองซ้ำก่อน (กันเครื่อง/คอลัมน์ 🔒 หายชั่วคราว)
    const notEmpty = o => o && Object.keys(o).length > 0
    try { setAva(await retry(() => api.planAva(sheet), { ok: notEmpty })) } catch { setAva({}) }
    try { setPoolMap(await retry(() => api.planPoolMap())) } catch { setPoolMap({}) }
    // ยังไม่มีชีท MC S9 / อ่านไม่ได้ = ไม่มีพูลจ้างทอ → Gantt จ้างทอไม่ขึ้น (หน้าแผนยังใช้ได้ปกติ)
    try { setS9Pool(await retry(() => api.planS9Pool())) } catch { setS9Pool({}) }
    try { setS9ItemCap(await retry(() => api.planS9ItemCap())) } catch { setS9ItemCap({}) }
    try { setS9Elig(new Set(await retry(() => api.planS9Eligible()))) } catch { setS9Elig(new Set()) }
    // ไม่มีข้อมูลก้อน → ลูกโซ่คิดตามชื่อกลุ่มเหมือนเดิม (ไฟล์แผนใหม่ยุบกลุ่มมาให้แล้ว)
    try { mcGroupsRef.current = await retry(() => api.planMcGroups()) } catch { mcGroupsRef.current = null }
    try { setBookingMc(await retry(() => api.planBookingMc(sheet))) } catch { setBookingMc({}) }
    try { setBookingItems(await retry(() => api.planBookingItems(sheet))) } catch { setBookingItems([]) }
    // ชีทอื่น: bookingItems คือ booking เต็มอยู่แล้ว → ไม่ต้องยิงซ้ำ (ไฟล์ก้อนใหญ่)
    if (replan) {
      // ต้องขอ full — ไม่ส่ง sheet ไฟล์รุ่นใหม่ก็ยังได้ booking_final_replan ที่ตัดแล้ว
      try { setBookingAll(await retry(() => api.planBookingItems(undefined, true))) } catch { setBookingAll(null) }
    } else setBookingAll(null)
    try { setWorkdayData(await retry(() => api.workday())) } catch { setWorkdayData(null) }
    try { setProgram(await retry(() => api.planProgram())) } catch { setProgram({}) }
    // ยังไม่มีชีท Lock_Plan (ยังไม่เคยล็อก) = ไม่มีล็อก — ไม่ใช่ error ของหน้า
    try { setPlanLocks(await retry(() => api.planLocks())) } catch { setPlanLocks([]) }
    // ยังไม่ได้ดึง Order Color → codes ว่าง = ทุกบล็อกถือว่า "ยังไม่ให้สี" (หน้ายังเปิดได้ปกติ)
    try {
      const d = await retry(() => api.dyedItemCodes())
      setDyeCodes(new Set((d?.codes || []).map(c => String(c).trim().toUpperCase())))
    } catch { setDyeCodes(new Set()) }
    // ช่วงประวัติที่เลือกได้ — เบา (นับสัปดาห์ ไม่ส่งบล็อก) ; ล้มก็แค่ไม่มีตัวเลือกให้กด
    try {
      const r = await api.planHistoryRange()
      setHistRange(r && !r.error ? r : null)
      // ค่าเริ่มต้น = สัปดาห์ที่ "ดึงมาแล้ว" เก่าสุด (กดดูได้ทันทีไม่ต้องรอ) ; ไม่มีเลยก็เอาเก่าสุดที่ DB มี
      const dft = r?.oldest || r?.db?.oldest
      setHistSel(s => s || (dft ? { year: dft.year, week: dft.week } : null))
    } catch { setHistRange(null) }
  }
  // แสดงบล็อกประวัติตั้งแต่ (ปี, สัปดาห์) ที่เลือก — อ่านจากทะเบียนที่ backend เก็บไว้ (เร็ว)
  // ช่วงที่ทะเบียนยังไม่มีต้องกด "ดึงจาก DB" ก่อน (fetchHistory ข้างล่าง)
  async function loadHistory(sel) {
    if (!sel) return
    setHistBusy(true); setHistErr('')
    try {
      const d = await api.planHistoryItems(sel.year, sel.week)
      if (d?.error) throw new Error(d.error)
      setHistItems(d.items || [])
      setHistFrom(d.from || sel)
      // ดึงประวัติมาแล้วต้องเห็น "ทุก item ที่ทอจริงในสัปดาห์นั้น" — โหมด plan กรองเหลือเฉพาะ
      // item ที่บังเอิญอยู่ในแผนรอบนี้ (W18 มี 181 บล็อก เหลือบนจอไม่กี่ตัว) ซึ่งขัดกับเหตุผล
      // ที่กดดึงมาตั้งแต่แรก → สลับเป็น "ทุก item" ให้เอง (กดกลับเป็นโหมดเดิมได้ตลอด)
      setBookingMode('all')
      if (!(d.items || []).length) setHistErr('ช่วงที่เลือกยังไม่มีข้อมูล — กด "ดึงจาก DB" ก่อน')
    } catch (e) {
      setHistItems([]); setHistFrom(null)
      setHistErr(String(e?.message || e))
    } finally { setHistBusy(false) }
  }
  // สั่ง backend ไปดึง booking ของช่วงที่เลือกจาก DB ใหม่ แล้วรัน AVA_MC คิดเครื่อง/setup/carry ให้
  // — ใช้เวลานาน (~16 นาที/7 สัปดาห์ เพราะ view วิ่งข้าม dblink) จึงทำเบื้องหลังแล้ว poll ทุก 10 วิ
  // ระหว่างนี้ user ทำงานอื่นบนหน้าแผนได้ตามปกติ เสร็จแล้วบล็อกจะขึ้นเอง
  //
  // force = true (ปุ่มบนหน้าแผน) → ดึงทั้งช่วงใหม่ทับทะเบียนเสมอ ไม่ใช่เติมเฉพาะสัปดาห์ที่ขาด
  // เพราะของที่ค้างในทะเบียนอาจมาจาก snapshot เก่าที่ยังไม่มีคอลัมน์ลูกโซ่ setup/carry
  // (บล็อกขึ้นเป็น 📋 ไม่มีเลข job) — เดิมโหมด "เติมที่ขาด" ทำให้กดปุ่มแล้วไม่ได้ยิง DB เลย
  const histPollRef = useRef(null)
  async function fetchHistory(sel, force = true) {
    if (!sel) return
    setHistErr('')
    try {
      const r = await api.planHistoryFetch(sel.year, sel.week, force)
      // ไม่ได้เริ่มงาน → ต้องบอกสาเหตุให้ชัด ไม่ใช่เงียบแล้วโชว์ของเดิมเหมือนเพิ่งดึงมา
      if (r?.reason === 'complete') {
        setHistErr('ทะเบียนมีครบช่วงนี้แล้ว — ไม่ได้ดึงใหม่')
        await loadRange(); await loadHistory(sel); return
      }
      if (r?.reason === 'empty_range') {
        setHistErr('ช่วงที่เลือกอยู่ในช่วงที่ booking ปัจจุบันครอบคลุมอยู่แล้ว — ไม่มีสัปดาห์ให้ดึง')
        await loadHistory(sel); return
      }
      if (r?.reason === 'db_error') throw new Error('ต่อ DB ไม่ได้: ' + (r.error || ''))
      if (r?.reason === 'busy') throw new Error('มีงานดึงข้อมูลค้างอยู่ — รอให้รอบนี้จบก่อน')
      if (r?.reason === 'error') throw new Error(r.error || 'สั่งดึงไม่สำเร็จ')
      setHistNote('')
      setHistJob({ running: true, step: 'queued', detail: 'เตรียมดึงข้อมูล', weeks: r?.queued || 0 })
      pollHistoryJob(sel)
    } catch (e) { setHistErr(String(e?.message || e)) }
  }
  function pollHistoryJob(sel) {
    clearInterval(histPollRef.current)
    histPollRef.current = setInterval(async () => {
      try {
        const s = await api.planHistoryStatus()
        setHistJob(s)
        if (!s.running) {
          clearInterval(histPollRef.current)
          // จบรอบนี้แล้ว — จำไว้ว่าแจ้งผลนี้ไปแล้ว เพื่อไม่แจ้งซ้ำตอนเปิดหน้าครั้งถัดไป
          if (s.finished) { try { localStorage.setItem(HIST_SEEN_KEY, s.finished) } catch { } }
          if (s.error) setHistErr(s.error)
          else {
            // ดึงเสร็จ → ทะเบียนมีของใหม่แล้ว: รีเฟรชช่วงที่เลือกได้ + แสดงผลทันที
            setHistNote(`✅ ดึงเสร็จแล้ว${s.weeks ? ` ${s.weeks} สัปดาห์` : ''}`
              + `${s.elapsed_sec ? ` (ใช้เวลา ${jobMins(s.elapsed_sec)})` : ''}`)
            await loadRange()
            await loadHistory(sel)
          }
        }
      } catch { /* เน็ตสะดุดชั่วคราว — รอบหน้าค่อยถามใหม่ */ }
    }, 10000)
  }
  // งานดึงทำเบื้องหลังที่ server — state ของหน้าเว็บหายไปกับการ unmount/refresh แต่ thread
  // ฝั่ง server ยังวิ่งต่อ → เข้าหน้าแผนทุกครั้งต้องถามสถานะก่อน ไม่งั้นผู้ใช้เห็นเหมือน
  // "กดแล้วหายไปเฉย ๆ" แล้วกดซ้ำจนได้ busy ; งานที่จบตอนปิดหน้าไว้ก็ต้องรู้ว่าเสร็จแล้ว
  useEffect(() => {
    let dead = false
    ;(async () => {
      try {
        const s = await api.planHistoryStatus()
        if (dead || !s) return
        const sel = s.from ? { year: s.from.year, week: s.from.week } : null
        if (s.running) {
          setHistJob(s)
          if (sel) setHistSel(cur => cur || sel)
          pollHistoryJob(sel)
          return
        }
        // จบไปแล้วและยังไม่เคยแจ้ง → บอกผล + เอาบล็อกขึ้นให้เลย (ไม่ต้องกดเอง)
        let seen = ''
        try { seen = localStorage.getItem(HIST_SEEN_KEY) || '' } catch { }
        if (!s.finished || s.finished === seen) return
        try { localStorage.setItem(HIST_SEEN_KEY, s.finished) } catch { }
        if (s.error) { setHistErr(`งานดึงรอบก่อนล้มเหลว: ${s.error}`); return }
        setHistNote(`✅ ดึงเสร็จแล้วเมื่อ ${String(s.finished).slice(11, 16)} น.`
          + `${s.weeks ? ` (${s.weeks} สัปดาห์` : ''}${s.weeks && s.elapsed_sec ? ` · ${jobMins(s.elapsed_sec)}` : ''}${s.weeks ? ')' : ''}`)
        if (sel) { setHistSel(sel); await loadHistory(sel) }
      } catch { /* ถามสถานะไม่ได้ = หน้าแผนยังใช้งานได้ตามปกติ */ }
    })()
    return () => { dead = true }
  }, [])
  async function loadRange() {
    try {
      const r = await api.planHistoryRange()
      if (r && !r.error) setHistRange(r)
    } catch { /* ไม่ critical */ }
  }
  // ออกจากหน้าแผน = หยุดถามสถานะ (งานฝั่ง server ยังทำต่อจนจบ กลับมาเปิดใหม่ก็เห็นผล)
  useEffect(() => () => clearInterval(histPollRef.current), [])
  function clearHistory() { setHistItems([]); setHistFrom(null); setHistErr(''); setHistNote('') }
  // ล็อก/ปลดล็อกงานหนึ่งบล็อก → เขียนชีท Lock_Plan (MasterMC) แล้วดึงรายการใหม่
  // (ดึงใหม่แทนการแก้ state เอง เพื่อให้ตรงกับชีทเสมอ — ชีทแก้มือจากหน้า Master ได้ด้วย)
  // payload.source: BOOKING = บล็อก booking / PLAN = งานในแผน (ต้องบอกชีทที่กดด้วย
  // เพราะชีท PLAN กับ REPLAN อ่านคนละไฟล์ — Replan ต้องย้อนไปอ่านแถวให้ถูกใบ)
  async function toggleLock(payload, isLocked) {
    const src = payload.source || 'BOOKING'
    if (isLocked) await api.planLockRemove(payload.week, payload.item, payload.mc_group, payload.gauge, src)
    else await api.planLockAdd({ ...payload, source: src, sheet: grid?.sheet || '' })
    setPlanLocks(await api.planLocks())
    setMsg(isLocked
      ? `🔓 ปลดล็อก ${payload.item} ${wkLabel(payload.week)} แล้ว`
      : `🔒 ล็อก ${payload.item} ${wkLabel(payload.week)} แล้ว — Replan จะไม่ย้ายงานนี้`)
  }

  // ล็อก/ปลดล็อกหลายสัปดาห์ในครั้งเดียว (การ์ดงาน → เลือกสัปดาห์ที่จะล็อก)
  // add/remove = payload แบบเดียวกับ toggleLock ; ยิง bulk ครั้งเดียวแล้วดึงรายการล็อกใหม่
  async function applyLocks(add = [], remove = []) {
    if (!add.length && !remove.length) return
    const sheet = grid?.sheet || ''
    const fill = (p) => ({ ...p, source: p.source || 'PLAN', sheet })
    const r = await api.planLockBulk({ add: add.map(fill), remove: remove.map(fill) })
    setPlanLocks(await api.planLocks())
    const item = (add[0] || remove[0]).item
    const wk = (a) => a.map(p => wkLabel(p.week)).join(' ')
    setMsg([
      add.length ? `🔒 ล็อก ${item} ${wk(add)}` : '',
      remove.length ? `🔓 ปลดล็อก ${wk(remove)}` : '',
    ].filter(Boolean).join(' · ') + ` (ล็อกทั้งหมด ${r?.count ?? '-'} รายการ)`)
  }

  async function loadSheet(sheet) {
    setLoading(true); setMsg('')
    setSearch(''); setItemPick(null); setItemPop(null); setFilters({}); setOpenCol(null)
    // โหลดใหม่ทั้งใบแล้ว = ได้ของล่าสุดอยู่ดี ของที่ค้างรอแปะไม่เหลือความหมาย
    setRemoteWait(null)
    clearTimeout(remoteTimer.current)
    try {
      const d = await api.planSheet(sheet)
      planRevRef.current = d.rev ?? null
      // setup ต่อเครื่องของแต่ละ item อ่านจากไฟล์แผน "ก่อนถูกแก้" เท่านั้น — หลังลากงานแล้ว
      // SETUP_DAYS ของแถวถูกเขียนทับ ถ้าคิดใหม่จากแถวที่แก้แล้วค่าจะเพี้ยนสะสม
      setupDaysRef.current = makeSetupDays(d.columns, d.rows)
      warmSeedRef.current = makeWarmSeed(d.columns, d.rows, mcGroupsRef.current)
      // จำนวนเครื่องตั้งต้นจากไฟล์ (พื้นของการคำนวณใหม่) — ไม่ให้เลขบานปลายเวลาลากไป-กลับ
      const aci = d.columns.indexOf('ACTUAL_MC')
      // origRows = แผน "ก่อนถูกแก้" ทั้งชุด — ตารางเทียบในการ์ดใช้เป็นฝั่ง "แผนเก่า"
      // ทุก op แก้แถวแบบ copy (setCell/delRow/splitJob) จึงถือ reference เดิมไว้ได้ ไม่โดนเขียนทับ
      setGrid({
        ...d,
        baseMc: aci >= 0 ? d.rows.map(r => Number(norm(r[aci])) || 0) : [],
        origRows: d.rows,
      })
      setDirty(false)
      // เครื่องว่าง/โควตา setup/บล็อก booking ผูกกับชีท → ต้องโหลดใหม่ทุกครั้งที่สลับชีท
      // (d.sheet = ชื่อชีทจริงที่ backend เปิดให้ ไม่ใช่ค่าที่ขอไป ซึ่งอาจเป็น undefined ตอนเปิดหน้าแรก)
      loadLoad(d.sheet)
      loadAva(d.sheet, !!d.replan)
    } catch (e) {
      setGrid(null)
      setMsg('อ่านแผนไม่ได้: ' + e.message)
    } finally {
      setLoading(false)
    }
  }

  // ---- เอาแผนฉบับที่คนอื่นเพิ่งแก้มาแปะ โดยจอต้องไม่กระพริบแม้แต่ครั้งเดียว ----
  //
  // ห้ามใช้ loadSheet เด็ดขาด: มันตั้ง loading → Gantt ถูกถอดออกจากจอทั้งแผงแล้ววาดใหม่
  // (เด้งกลับหัวตาราง ช่องค้นหา/ตัวกรองถูกล้าง) คนที่นั่งดูอยู่จะหาของที่มองค้างไว้ไม่เจอ
  // ที่นี่จึงดึงให้ครบก่อนแล้วค่อยสลับทีเดียว — แถวที่ข้อมูลไม่เปลี่ยน React ไม่แตะ DOM เลย
  // (GanttRow เป็น memo + key คงที่) จะเห็นแค่บล็อกที่ขยับจริงเท่านั้นที่เปลี่ยน
  async function pullRemote(sheet, ent) {
    // ระหว่างหน่วงรอ เราอาจเพิ่งเซฟของตัวเองไป (เลขรุ่นแซงข่าวนี้แล้ว) = ข่าวของเราเอง
    if (ent && ent.rev != null && planRevRef.current != null && ent.rev <= planRevRef.current) return
    // มีของค้างยังไม่เซฟ/กำลังลาก → ห้ามทับ (ที่ลากค้างไว้จะหาย) แค่ขึ้นปุ่มให้กดเอาเอง
    if (dirtyRef.current || draggingRef.current) {
      setRemoteWait({ by: ent?.by || '', at: ent?.at || '', sheet })
      return
    }
    await loadSheetQuiet(sheet, ent)
  }

  /** โหลดชีทใหม่แบบไม่กระพริบ — ไม่แตะ loading/ตัวกรอง/ช่องค้นหา สลับข้อมูลทีเดียวตอนได้ครบ
   *  ent = ข่าวที่เป็นต้นเหตุ (ไม่มีก็ได้ เช่นผู้ใช้กดปุ่ม "เอาของใหม่มาแสดง") */
  async function loadSheetQuiet(sheet, ent = null) {
    try {
      const d = await api.planSheet(sheet)
      // ระหว่างรอผล ผู้ใช้อาจเปลี่ยนชีท/เริ่มแก้/เริ่มลาก → ของที่ได้มาใช้ไม่ได้แล้ว
      if (!gridRef.current || gridRef.current.sheet !== d.sheet) return
      if (dirtyRef.current || draggingRef.current) {
        setRemoteWait({ by: ent?.by || '', at: ent?.at || '', sheet })
        return
      }
      planRevRef.current = d.rev ?? null
      setupDaysRef.current = makeSetupDays(d.columns, d.rows)
      warmSeedRef.current = makeWarmSeed(d.columns, d.rows, mcGroupsRef.current)
      const aci = d.columns.indexOf('ACTUAL_MC')
      setGrid({
        ...d,
        baseMc: aci >= 0 ? d.rows.map(r => Number(norm(r[aci])) || 0) : [],
        origRows: d.rows,
      })
      setRemoteWait(null)
      // แถบโหลด + เครื่องว่าง เปลี่ยนตามแผนที่เพิ่งแก้ → ต้องตามไปด้วย
      // ที่เหลือ (booking/วันทำงาน/Program/S9) การลากไม่ทำให้เปลี่ยน และเป็นก้อนใหญ่ จึงไม่ยิงซ้ำ
      // ล้มก็คงของเดิมไว้ — ตั้งเป็นค่าว่างจะทำให้แถบโหลดวูบหายทั้งแถว
      try { setLoad(await retry(() => api.planLoad(d.sheet))) } catch { }
      try { setSetupJobs(await retry(() => api.planSetupJobs(d.sheet))) } catch { }
      try { setAva(await retry(() => api.planAva(d.sheet))) } catch { }
      loadMeta().catch(() => { })
    } catch { /* ดึงไม่สำเร็จ — ข่าวถัดไป (หรือปุ่มรีเฟรช) ค่อยว่ากัน */ }
  }

  // ลากบล็อกค้างอยู่ = ห้ามสลับข้อมูลใต้มือ ; ปล่อยเมื่อไหร่ค่อยเอาของที่ค้างมาแปะ
  //
  // ปล่อยแล้วต้อง "นับรอบเรนเดอร์ใหม่" (dragTick) ไม่ใช่สั่งแปะทันทีในตัวรับ event นี้:
  // ตอนเมาส์ถูกปล่อย React ยังไม่ได้ commit ผลการลากของเราเอง (dirtyRef ยังเป็น false อยู่)
  // ถ้าแปะเลยจะเอาของคนอื่นมาทับงานที่เราเพิ่งลากไปหมาด ๆ
  useEffect(() => {
    const on = () => { draggingRef.current = true }
    const off = () => { draggingRef.current = false; setDragTick(t => t + 1) }
    document.addEventListener('dragstart', on)
    document.addEventListener('dragend', off)
    document.addEventListener('drop', off)
    return () => {
      document.removeEventListener('dragstart', on)
      document.removeEventListener('dragend', off)
      document.removeEventListener('drop', off)
    }
  }, [])

  // ของที่ค้างรออยู่ + เราเซฟของตัวเองเสร็จ/ปล่อยเมาส์แล้ว → แปะให้เองเลย ไม่ต้องให้กดปุ่ม
  useEffect(() => {
    if (!remoteWait || dirty) return
    const t = setTimeout(() => {
      if (!dirtyRef.current && !draggingRef.current) pullRemote(remoteWait.sheet, null)
    }, 200)
    return () => clearTimeout(t)
  }, [remoteWait, dirty, dragTick])

  useEffect(() => {
    (async () => {
      try {
        const m = await loadMeta()
        // มีไฟล์แผน → loadSheet โหลด load/ava ตามชีทที่เปิดให้เอง (ไม่ต้องเรียกซ้ำที่นี่)
        if (m.exists) await loadSheet()
        else { loadLoad(); loadAva() }
      } catch (e) { setMsg('โหลดแผนไม่ได้: ' + e.message) }
    })()
    loadRunStatus()
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
          // รันเสร็จ = ไฟล์แผนใหม่ → กลับไปชีทเริ่มต้น (PLAN) แล้ว loadSheet โหลด load/ava ให้เอง
          if (m.exists) await loadSheet()
          else { loadLoad(); loadAva() }
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
      // loadSheet โหลด load/ava ของชีทที่เปิดอยู่ให้เอง — เรียกซ้ำที่นี่จะได้ตัวเลขของชีท PLAN มาทับ
      if (m.exists) await loadSheet(grid?.sheet)
      else { setGrid(null); setMsg('ยังไม่มีไฟล์แผน — กดรันแผนก่อน'); loadLoad(); loadAva() }
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
    // ต้องรอให้ของค้างคิวลงไฟล์ก่อนเปลี่ยนชีท ไม่งั้นการลากล่าสุดหาย — หรือแย่กว่านั้นคือ
    // ไปเซฟตอนที่หน้าเปลี่ยนเป็นชีทใหม่แล้ว (เอาแถวของชีทเก่าไปทับชีทใหม่)
    await autosave.flush()
    await loadSheet(sheet)
  }

  // ทุกออร์เดอร์ (นิยามด้วย ORDER_KEY_COLS) ที่วางไม่ครบ/เกิน — ใช้เตือนก่อนบันทึก
  // เทียบกับยอดที่ "ต้องวางรอบนี้" (orderBaseOf) ไม่ใช่ยอดสั่งทั้งใบ — ใบที่เคยวางแผนไปแล้วบางส่วน
  // แผนรอบนี้วางแค่ส่วนที่เหลือ ถ้าเทียบกับยอดสั่งจะขึ้น "ขาด" ทุกใบทั้งที่แผนถูก
  function ordersOutOfBalance() {
    if (!grid) return []
    const cols = grid.columns
    const oq = cols.indexOf('ORDERS_QTY'), pq = cols.indexOf('PRODUCE_QTY')
    const sc = cols.indexOf('SC_SO_NO')
    if (oq < 0 || pq < 0) return []
    const g = new Map()
    grid.rows.forEach(r => {
      const need = orderBaseOf(r, cols)
      if (need <= 0) return
      const key = orderKeyOf(r, cols)
      const cur = g.get(key) || {
        key, sc: sc >= 0 ? norm(r[sc]) : '', ordered: 0, placed: 0, full: 0,
      }
      cur.ordered = Math.max(cur.ordered, need)
      cur.full = Math.max(cur.full, Number(norm(r[oq])) || 0)   // ยอดสั่งทั้งใบ — ไว้แสดงประกอบ
      cur.placed += Number(norm(r[pq])) || 0
      g.set(key, cur)
    })
    return [...g.values()]
      .map(o => ({ ...o, diff: Math.round((o.ordered - o.placed) * 100) / 100 }))
      .filter(o => Math.abs(o.diff) > QTY_TOL)
  }

  // ออร์เดอร์ที่วางไม่ตรงยอดสั่ง — ป้ายเตือนค้างบนจอ ไม่ขัดการเซฟ
  // (ระหว่างจัดแผน ยอดไม่ตรงชั่วคราวเป็นเรื่องปกติ เช่นลากงานออกมาแล้วยังไม่ได้วางที่ใหม่)
  const balanceWarn = useMemo(() => (grid ? ordersOutOfBalance() : []), [grid])

  // ---- เซฟให้เองหลังหยุดมือ ----
  // หน่วง 3 วินาที (ไม่ใช่ทันทีที่ปล่อยเมาส์) เพราะการจัดแผนคือลากติด ๆ กันหลายงาน:
  // ถ้าเซฟทุกการลาก คนอื่นจะเห็นแผนกระพริบระหว่างที่เรายังจัดไม่เสร็จ และไฟล์ถูกเขียนถี่เกินจำเป็น
  // ยอดสั่ง-ยอดวางไม่ตรงไม่ขัดการเซฟอีกแล้ว (เดิมเด้งถามก่อนบันทึก) — ระหว่างจัดแผนยอดไม่ตรง
  // เป็นเรื่องปกติ จึงย้ายไปเป็นป้ายเตือนค้างบนจอแทน (ดู balanceWarn)
  const autosave = useAutoSave(async (g) => {
    if (!g) return
    try {
      // g.name = ไฟล์แผนที่แถวชุดนี้ถูกอ่านมา — ส่งไปให้ backend ยืนยันว่ายังเป็นใบเดียวกัน
      const r = await api.planSave(g.sheet, g.columns, g.rows, planRevRef.current, g.name)
      planRevRef.current = r.rev
      setSavedAt(Date.now())
      setDirty(false)
      // เซฟแล้ว = แถวชุดนี้กลายเป็นแผนในไฟล์ → ตั้งเป็นพื้นใหม่ของตารางเทียบ
      // ต้องใช้ g.rows (ชุดที่เพิ่งเข้าไฟล์) ไม่ใช่แถวล่าสุดบนจอ — ระหว่างรอเซฟผู้ใช้อาจลากต่อ
      // ไปแล้ว ถ้าเอาแถวล่าสุดมาเป็นพื้น การลากนั้นจะถูกนับว่า "บันทึกแล้ว" ทั้งที่ยังไม่เข้าไฟล์
      setGrid(g2 => (g2 ? { ...g2, origRows: g.rows } : g2))
      loadMeta().catch(() => { })
    } catch (e) {
      // มีคนแก้แซงระหว่างที่เราแก้ค้างอยู่ → เซฟไม่ได้ (ของเขาจะหาย) ; ขึ้นแถบให้เลือกเองว่า
      // จะเอาฉบับของเขามาแสดงไหม แล้วค่อยลากใหม่ — ห้ามโหลดทับให้เองตรงนี้ ที่เราทำค้างจะหาย
      //
      // ต้องโยนต่อ (ไม่ใช่ return เฉย ๆ) ไม่งั้นป้ายมุมจอขึ้น "บันทึกแล้ว" ทั้งที่ยังไม่เข้าไฟล์
      if (e.conflict) {
        // คนละไฟล์ (มีคนรันแผนรอบใหม่) ต้องบอกให้ตรง — "เอาของใหม่มาแสดง" ดึงชีทจากไฟล์ใหม่
        // ซึ่งเป็นคนละแผนทั้งใบ ไม่ใช่การขยับของใครคนหนึ่งเหมือนกรณีเลขรุ่นไม่ตรง
        if (e.current?.file_changed) {
          setRemoteWait(w => w || { by: '', at: '', sheet: g.sheet })
          throw new Error(e.message)
        }
        setRemoteWait(w => w || { by: '', at: '', sheet: g.sheet })
        throw new Error('มีคนแก้แผนแซงไปแล้ว — กด "เอาของใหม่มาแสดง" ที่แถบด้านบน แล้วค่อยจัดใหม่')
      }
      throw e
    }
  }, { delay: 3000 })

  // ผู้ใช้ขยับอะไรในแผนก็ตาม (ลาก แยกงาน ลบแถว แก้ช่อง) → เข้าคิวเซฟ
  // ดักที่ dirty จุดเดียว แทนการไล่ผูกทุกคำสั่งแก้ทีละอัน (มี 8 จุด — พลาดจุดเดียวของก็หาย)
  useEffect(() => {
    if (!grid || !dirty) return
    autosave.saveSoon(grid)
  }, [grid, dirty])

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
  // ลบงานออกจากแผน — ลูกโซ่ที่เหลือต้องคิดใหม่ด้วย เพราะแถวถัดไปอาจเคย carry เครื่องต่อจากแถวนี้
  // (พอต้นทางหายไป เครื่องก็ไม่มีใครส่งต่อ → ต้องตั้งเครื่องใหม่เอง)
  function delRow(ri) {
    // เก็บแถวไว้ก่อนลบ — ต้องใช้คีย์ของมันไปลบล็อกในชีท Lock_Plan ด้วย
    const gone = grid?.rows?.[ri]
    setGrid(g => {
      const rows = g.rows.filter((_, i) => i !== ri)
      const base = (g.baseMc || []).filter((_, i) => i !== ri)
      const res = recalcPlanRows(g.columns, rows, [g.rows[ri]], base)
      return { ...g, rows: res.rows, baseMc: base }
    })
    setDirty(true)
    if (gone) dropLock(gone)
  }
  // resolver วันทำงานราย (mc_group, gauge, week) จากชีท Work Day (แหล่งเดียว — user ปรับเอง)
  // ตรงกับ WorkDay.get_working_days() ใน backend: min(ค่าในแผง, วันที่ปฏิทินเปิด), รวมยุบสัปดาห์
  // ปฏิทินมากับ payload /api/workday (อ่านสดจาก Calendar.xlsx บน server)
  const wdResolver = useMemo(
    () => (workdayData ? makeWorkDayResolver(workdayData) : null),
    [workdayData]
  )

  // วันด้ายเข้า (PO_IN_DATE) → เลขสัปดาห์ ตามปฏิทินจริงบน server (ตารางเทียบในการ์ดใช้)
  const weekOfDate = useMemo(() => makeWeekOfDate(workdayData), [workdayData])
  // แกนสัปดาห์ตามปฏิทินจริง — ตารางเทียบใช้ตัดสินว่าวันด้ายเข้าอยู่ก่อนหรือหลังสัปดาห์ที่วางแผน
  const weekAxis = useMemo(() => makeWeekAxis(workdayData), [workdayData])

  // วันทำงานจริงของแถวในสัปดาห์ w — อ่านจากชีท Work Day ตาม (MC_GROUP, MC_GUAGE) ของแถว
  function actualWdAt(mc, gauge, w) {
    if (!wdResolver) return null            // ยังโหลด workday ไม่เสร็จ
    return wdResolver.workDays(mc, gauge, w)
  }

  // คำนวณลูกโซ่ carry/setup ใหม่หลังจากแถวถูกย้าย/แบ่ง/ลบ (ดู mcRecalc.js)
  // seedRows = แถวที่ทำให้ต้องคิดใหม่ (ก่อนย้าย + หลังย้าย) → คิดใหม่ทั้ง run ของทั้งสองฝั่ง
  // คืนแถวชุดเดิมถ้ายังโหลด workday ไม่เสร็จ (คำนวณวันทำงานไม่ได้ ห้ามเดา)
  // ok=false → คำนวณ "ไม่ได้" (ไม่ใช่ "ไม่มีอะไรเปลี่ยน") ต้องบอก user ไม่งั้นเข้าใจว่าระบบไม่ทำงาน
  function recalcPlanRows(cols, rows, seedRows, baseMc) {
    if (!wdResolver || !canRecalc(cols)) return { rows, changes: [], ok: false }
    const ix = colIx(cols)
    const chains = new Set(seedRows.filter(Boolean).map(r => chainKeyOf(r, ix, mcGroupsRef.current)))
    const res = recalcChains(cols, rows, {
      chains,
      groupMap: mcGroupsRef.current,
      baseMc: baseMc || [],
      workDays: (mc, gauge, w) => wdResolver.workDays(mc, gauge, w),
      // สัปดาห์ที่ถูกยุบเข้าสัปดาห์อื่น / โรงปิดทั้งสัปดาห์ ไม่นับเป็นช่องบนแกน (เครื่องไม่ได้เย็นลง)
      weekActive: (w) => {
        if (wdResolver.mergedInto(w) != null) return false
        const c = wdResolver.calDays(w)
        return c == null || c > 0
      },
      bookingMc,
      // เครื่องว่างต่อพูล — ใช้ตัดสินว่าเครื่องยังอุ่นข้ามสัปดาห์ที่เว้นได้ไหม (เหมือน _carry_cap_by_gap)
      ava,
      poolMap,
      warmSeed: warmSeedRef.current,
      setupDaysOf: setupDaysRef.current,
    })
    return { ...res, ok: true }
  }

  // โควตา job setup ต่อประเภท (13/33/44) — เอาค่าสูงสุดที่เคยเห็นในแถบโหลด เหมือน capByType ใน PlanGantt
  function capOfType(t) {
    let cap = null
    for (const w in load) {
      const c = load[w]?.[t]?.cap
      if (c != null && c !== '' && (cap == null || c > cap)) cap = c
    }
    return cap
  }

  // เช็คว่าแถวชุดใหม่ทำให้สัปดาห์ไหน "ตั้งเครื่องใหม่" เกินโควตา job หรือไม่ → คืนข้อความเตือน
  // นับแบบเดียวกับแถบโหลดบน Gantt: แผนเดิม(old) + booking(bookingNew) + ผลรวม NEW_MC ของแถวที่วาง
  function jobQuotaWarnings(cols, rows, changes) {
    const ix = colIx(cols)
    if (ix.neu < 0 || ix.week < 0) return []
    const hit = new Set()
    for (const c of changes) {
      if (c.after.neu === c.before.neu) continue
      const t = classifyType(c.factory, c.cat)
      if (t) hit.add(`${c.week}|${t}`)
    }
    const out = []
    for (const key of hit) {
      const [w, t] = key.split('|')
      const cap = load[w]?.[t]?.cap ?? capOfType(t)
      if (cap == null || cap === '') continue
      // หลาย SC ของ item เดียวกันบนเครื่องชุดเดียวกัน = NEW_MC ค่าเดิมซ้ำทุกแถว → ยุบด้วย max ก่อนรวม
      const byItem = new Map()
      for (const r of rows) {
        if (rowWeekKey(r, ix.week, ix.year) !== w) continue
        if (classifyType(ix.factory >= 0 ? r[ix.factory] : '', ix.cat >= 0 ? r[ix.cat] : '') !== t) continue
        const k = chainKeyOf(r, ix, mcGroupsRef.current)
        byItem.set(k, Math.max(byItem.get(k) || 0, Number(norm(r[ix.neu])) || 0))
      }
      let plan = 0
      for (const v of byItem.values()) plan += v
      const total = (load[w]?.[t]?.old || 0) + (load[w]?.[t]?.bookingNew || 0) + plan
      if (total > cap) {
        const label = LOAD_TYPES.find(x => x.key === t)?.long || t
        out.push(`• ${label} สัปดาห์ ${wkLabel(w)}: ใช้ ${total} job / โควตา ${cap} (เกิน ${total - cap})`)
      }
    }
    return out
  }

  // job setup ที่ยังว่างของ (สัปดาห์ × ประเภท) — นับแบบเดียวกับ jobQuotaWarnings
  // ใช้ตอน Change Cylinder: ย้ายงานมาสัปดาห์นี้ได้อีกกี่เครื่องก่อนชนโควตา
  // คืน null = ไม่รู้โควตา (ไม่จำกัด) — ห้ามเดาแทนข้อมูลที่ไม่มี
  function jobSlotsLeftAt(cols, rows, week, factory, cat) {
    const t = classifyType(factory, cat)
    if (!t) return null
    const w = String(week)
    const cap = load[w]?.[t]?.cap ?? capOfType(t)
    if (cap == null || cap === '') return null
    const ix = colIx(cols)
    if (ix.neu < 0 || ix.week < 0) return null
    // NEW_MC ค่าเดิมซ้ำทุกแถวของบล็อกเดียวกัน → ยุบด้วย max ก่อนรวม (เหมือน jobQuotaWarnings)
    const byItem = new Map()
    for (const r of rows) {
      if (rowWeekKey(r, ix.week, ix.year) !== w) continue
      if (classifyType(ix.factory >= 0 ? r[ix.factory] : '', ix.cat >= 0 ? r[ix.cat] : '') !== t) continue
      const k = chainKeyOf(r, ix, mcGroupsRef.current)
      byItem.set(k, Math.max(byItem.get(k) || 0, Number(norm(r[ix.neu])) || 0))
    }
    let plan = 0
    for (const v of byItem.values()) plan += v
    return Math.max(0, cap - ((load[w]?.[t]?.old || 0) + (load[w]?.[t]?.bookingNew || 0) + plan))
  }

  // สรุปให้ user เห็นว่าแถวที่ลากเปลี่ยนเครื่อง/setup ไปเท่าไร (โชว์บนแถบข้อความใต้ปุ่ม)
  function changeSummary(item, week, res, ri) {
    const changes = res.changes
    if (!res.ok) {
      return `ย้าย ${item} → สัปดาห์ ${wkLabel(week)} · ⚠ ยังคำนวณจำนวนเครื่อง/setup ใหม่ให้ไม่ได้ — `
        + (wdResolver ? 'ชีทนี้ไม่มีคอลัมน์ที่ต้องใช้' : 'ยังโหลดข้อมูลวันทำงานไม่เสร็จ (กดรีเฟรชแล้วลากใหม่)')
    }
    // ที่มาของตัวเลข — โชว์ทุกครั้งเพื่อให้ตรวจได้ว่าระบบคิดจากวันทำงาน/setup/กำลังผลิตเท่าไร
    const row = res.rows[ri] || grid.rows[ri]
    const gv = (n) => { const k = grid.columns.indexOf(n); return k >= 0 ? (Number(norm(row[k])) || 0) : 0 }
    const gs = (n) => { const k = grid.columns.indexOf(n); return k >= 0 ? norm(row[k]) : '' }
    const awd = actualWdAt(gs('MC_GROUP'), gs('MC_GUAGE'), week)
    const neu = gv('NEW_MC')
    const setupPer = neu > 0 ? gv('SETUP_DAYS') / neu : 0
    const runDays = neu > 0 ? newRunDays(awd || 0, setupPer) : awd
    const how = `[วันทำงาน ${awd ?? '?'}`
      + (neu > 0 ? ` − setup ${setupPer} วัน/เครื่อง = ผลิต ${runDays} วัน` : '')
      + ` · ${gv('DAILY_CAPACITY').toLocaleString()} กก./เครื่อง/วัน · ต้องผลิต ${gv('PRODUCE_QTY').toLocaleString()} กก.]`

    const mine = changes.find(c => c.idx === ri)
    if (!mine) return `ย้าย ${item} → สัปดาห์ ${wkLabel(week)}: ใช้เครื่องชุดเดิม ไม่ต้อง setup เพิ่ม ${how}`
    const b = mine.before, a = mine.after
    const kind = a.neu > 0 ? `🔧 setup ใหม่ ${a.neu} เครื่อง` : (a.carry > 0 ? '⏩ ใช้เครื่องเดิมที่วิ่งอยู่' : '')
    const mcTxt = b.mc === a.mc ? `${a.mc} เครื่อง` : `เครื่อง ${b.mc} → ${a.mc}`
    const others = changes.filter(c => c.idx !== ri).length
    return `ย้าย ${item} → สัปดาห์ ${wkLabel(week)}: ${mcTxt}${kind ? ' · ' + kind : ''} ${how}`
      + (others ? ` (คำนวณลูกโซ่ใหม่อีก ${others} แถว)` : '')
  }

  // อัปเดตคอลัมน์ที่เป็นฟังก์ชันของสัปดาห์ล้วนๆ (คำนวณได้แม่นยำ) หลังย้ายงาน
  function recalcRowForWeek(row, cols, week) {
    const ix = (n) => cols.indexOf(n)
    const gv = (n) => { const k = ix(n); return k >= 0 ? (Number(norm(row[k])) || 0) : 0 }
    const gs = (n) => { const k = ix(n); return k >= 0 ? norm(row[k]) : '' }
    const set = (n, v) => { const k = ix(n); if (k >= 0) row[k] = v }

    const w = String(week)            // คีย์คอลัมน์ (ปี+สัปดาห์) — ตรงกับคีย์ ava/booking/ปฏิทินจาก backend
    const mc = gs('MC_GROUP'), gauge = gs('MC_GUAGE')
    const awd = actualWdAt(mc, gauge, w)
    if (awd == null) return
    const n = gv('NEW_MC')
    // วันผลิตของเครื่องใหม่ใช้สูตรเดียวกับ prod_days_new ใน Planning.py (mcRecalc.newRunDays)
    const avail = n > 0 ? newRunDays(awd, gv('SETUP_DAYS') / n) : awd

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
  // ── ล็อกต้องตามงานเสมอ ────────────────────────────────────────────────────
  // ล็อกหนึ่งแถวผูกกับ (สัปดาห์ + item + เครื่อง + เกจ) ไม่ได้ผูกกับเลขแถว → ทุกคำสั่งที่
  // ทำให้คีย์เปลี่ยนหรือแถวหายไป ต้องแก้ชีท Lock_Plan ตามด้วย ไม่งั้นล็อกจะชี้ช่องที่ไม่มีงาน
  // (Replan จะเตือน "หาแถวไม่เจอ" ทุกรอบ แล้วงานที่ตั้งใจล็อกก็ถูกจัดใหม่)
  const cellOf = (row, c) => { const k = grid.columns.indexOf(c); return k >= 0 ? norm(row[k]) : '' }
  // คีย์สัปดาห์ของแถว (PLAN_WEEK + PLAN_YEAR) = คีย์คอลัมน์ Gantt = สัปดาห์ในคีย์ล็อก (plan_lock.key_of)
  const rowKeyOf = (row) => (grid ? rowWeekKey(row, grid.columns.indexOf('PLAN_WEEK'), grid.columns.indexOf('PLAN_YEAR')) : '')
  // เขียนคีย์ปลายทางกลับลงแถว = PLAN_WEEK + PLAN_YEAR (pipeline อ่านปีจากคอลัมน์นี้ — ลากไปคอลัมน์ปีหน้า
  // แล้วไม่เขียนปี งานจะถูกตีกลับเป็นปีเดิม)
  const setRowWeek = (row, cols, key) => writeWeekKey(row, cols.indexOf('PLAN_WEEK'), cols.indexOf('PLAN_YEAR'), key)
  const isRowLocked = (row) => planLocks.some(l => l.key === lockKeyOf(
    rowKeyOf(row), cellOf(row, 'ITEM_CODE'),
    cellOf(row, 'MC_GROUP'), cellOf(row, 'MC_GUAGE'), 'PLAN'))
  // payload ของล็อกจากแถวแผนหนึ่งแถว (ยอด/เครื่องเก็บไว้อ้างอิงในชีท)
  const lockPayload = (row) => ({
    week: rowKeyOf(row), item: cellOf(row, 'ITEM_CODE'),
    mc_group: cellOf(row, 'MC_GROUP'), gauge: cellOf(row, 'MC_GUAGE'),
    so: cellOf(row, 'SC_SO_NO'),
    qty: Number(cellOf(row, 'PRODUCE_QTY')) || null,
    mc: Number(cellOf(row, 'ACTUAL_MC')) || null,
    source: 'PLAN', sheet: grid?.sheet || '',
  })
  const lockWarn = (what, e) =>
    setMsg(`⚠ ${what} แต่แก้ล็อกตามไม่สำเร็จ: ${e.message} — ตรวจชีท Lock_Plan ที่หน้า Master`)

  // ลากงานที่ล็อกไว้ → ย้ายล็อกไปช่องใหม่ (สั่งเดียว เขียนชีทครั้งเดียว)
  async function followLock(oldRow, newRow) {
    if (!isRowLocked(oldRow)) return
    try {
      const p = lockPayload(newRow)
      await api.planLockMove({
        week: rowKeyOf(oldRow), item: cellOf(oldRow, 'ITEM_CODE'),
        mc_group: cellOf(oldRow, 'MC_GROUP'), gauge: cellOf(oldRow, 'MC_GUAGE'),
        to_week: p.week, to_mc_group: p.mc_group, to_gauge: p.gauge,
        qty: p.qty, mc: p.mc, source: 'PLAN',
      })
      setPlanLocks(await api.planLocks())
    } catch (e) { lockWarn('ย้ายงานแล้ว', e) }
  }

  // ลบงานที่ล็อกไว้ → ลบล็อกทิ้งด้วย (ล็อกที่ชี้ช่องว่างไม่มีความหมาย)
  async function dropLock(row) {
    if (!isRowLocked(row)) return
    try {
      await api.planLockRemove(rowKeyOf(row), cellOf(row, 'ITEM_CODE'),
        cellOf(row, 'MC_GROUP'), cellOf(row, 'MC_GUAGE'), 'PLAN')
      setPlanLocks(await api.planLocks())
      setMsg(m => (m ? m + ' · ' : '') + '🔓 ลบล็อกของงานนี้ด้วยแล้ว')
    } catch (e) { lockWarn('ลบงานแล้ว', e) }
  }

  // แบ่งงานที่ล็อกไว้ → ล็อกทั้งสองก้อน (ก้อนเดิมอัปเดตยอด, ก้อนใหม่ล็อกที่สัปดาห์ปลายทาง)
  async function splitLock(origRow, newRow) {
    try {
      await api.planLockAdd(lockPayload(origRow))     // คีย์เดิม = อัปเดตยอด/เครื่องในแถวเดิม
      await api.planLockAdd(lockPayload(newRow))      // ก้อนที่แยกออกไป = ล็อกแถวใหม่
      setPlanLocks(await api.planLocks())
      setMsg(m => (m ? m + ' · ' : '') + `🔐 ล็อกก้อนใหม่ที่ ${wkLabel(rowKeyOf(newRow))} ด้วยแล้ว`)
    } catch (e) { lockWarn('แบ่งงานแล้ว', e) }
  }

  function moveJob(ri, week, mcGroup) {
    if (!grid) return
    const wci = grid.columns.indexOf('PLAN_WEEK')
    const mci = grid.columns.indexOf('MC_GROUP')
    const ici = grid.columns.indexOf('ITEM_CODE')
    const item = ici >= 0 ? norm(grid.rows[ri][ici]) : ''
    const curMc = mci >= 0 ? norm(grid.rows[ri][mci]) : ''
    const crossMc = mcGroup && mci >= 0 && String(mcGroup).trim().toUpperCase() !== curMc.trim().toUpperCase()
    if (crossMc && !window.confirm(`ยืนยันย้ายงาน ${item} จากเครื่อง ${curMc} → ${mcGroup} (สัปดาห์ ${wkLabel(week)})?`)) return

    // สัปดาห์ปลายทางที่ไม่มีวันทำงานเลย (โรงปิด/ถูกยุบเข้าสัปดาห์อื่น) → ผลิตไม่ได้ ต้องยืนยันก่อน
    const oldRow = grid.rows[ri]
    const gsOld = (n) => { const k = grid.columns.indexOf(n); return k >= 0 ? norm(oldRow[k]) : '' }
    const awdTo = actualWdAt(gsOld('MC_GROUP'), gsOld('MC_GUAGE'), week)
    if (awdTo != null && awdTo <= 0
      && !window.confirm(`สัปดาห์ ${wkLabel(week)} ไม่มีวันทำงาน (โรงปิด/ยุบเข้าสัปดาห์อื่น) — วางแล้วผลิตไม่ได้\n\nยืนยันย้ายหรือไม่?`)) return

    // ย้ายแถวก่อน แล้วคำนวณลูกโซ่ carry/setup ใหม่ทั้ง run ต้นทาง+ปลายทาง
    // (ทำนอก setGrid เพราะต้องเอาผลไปเช็คโควตา job ให้ user ยืนยัน "ก่อน" ลงแผนจริง)
    const rows = grid.rows.slice()
    const row = rows[ri].slice()
    const oldWeek = rowKeyOf(row)
    if (wci >= 0) setRowWeek(row, grid.columns, week)
    if (crossMc) row[mci] = mcGroup
    if (String(oldWeek) !== String(week) || crossMc) recalcRowForWeek(row, grid.columns, week)
    rows[ri] = row

    // ลากมาทับงานเดียวกันในสัปดาห์เดียวกัน (SC/PO/FG/สี ตรงกันหมด) → รวมเป็นก้อนเดียว
    // ไม่งั้น Gantt จะมี 2 บล็อกของ item เดียวกันในช่องเดียว อ่านแล้วงงว่าตกลงใช้เครื่องกี่ตัว
    const qci = grid.columns.indexOf('PRODUCE_QTY')
    const twin = qci < 0 ? -1
      : rows.findIndex((r, i) => i !== ri && sameJobExceptQty(r, row, grid.columns))
    let base = grid.baseMc || []
    let keep = ri                      // แถวที่เหลืออยู่หลังรวม (ใช้อ้างอิงตอนสรุปผล)
    let mergedQty = 0
    if (twin >= 0) {
      const merged = rows[twin].slice()
      mergedQty = Math.round(((Number(norm(merged[qci])) || 0) + (Number(norm(row[qci])) || 0)) * 100) / 100
      merged[qci] = mergedQty
      rows[twin] = merged
      rows.splice(ri, 1)                                   // ก้อนที่ลากมาถูกยุบเข้าก้อนเดิม
      base = base.length ? base.filter((_, i) => i !== ri) : base
      keep = twin > ri ? twin - 1 : twin
    }
    const res = recalcPlanRows(grid.columns, rows, [oldRow, rows[keep]], base)

    // รวมก้อนแล้วยอดโตขึ้น เครื่องมักต้องเพิ่ม — บอกให้ user ตัดสินใจก่อนลงแผนจริง
    if (twin >= 0) {
      const mc = (c) => { const k = grid.columns.indexOf(c); return k >= 0 ? (Number(norm(res.rows[keep][k])) || 0) : 0 }
      const wasMc = Number(norm(rows[keep][grid.columns.indexOf('ACTUAL_MC')])) || 0
      const now = mc('ACTUAL_MC'), neu = mc('NEW_MC')
      if (!window.confirm(
        `สัปดาห์ ${wkLabel(week)} มีงาน ${item} (SC/PO/FG เดียวกัน) อยู่แล้ว — จะรวมเป็นก้อนเดียว\n\n`
        + `ยอดผลิต: ${mergedQty.toLocaleString()} กก.\n`
        + `เครื่อง: ${wasMc} → ${now} เครื่อง` + (neu > 0 ? ` (Setup ใหม่ ${neu} เครื่อง)` : ' (ใช้เครื่องเดิมที่วิ่งอยู่)')
        + `\n\nยืนยันรวมหรือไม่?`)) return
    }

    // ตั้งเครื่องใหม่ = กินโควตา job setup ของสัปดาห์นั้น — เกินโควตาต้องให้ user ตัดสินใจเอง
    const warn = jobQuotaWarnings(grid.columns, res.rows, res.changes)
    if (warn.length && !window.confirm(
      `⚠ ย้ายแล้ว job setup เกินโควตาสัปดาห์:\n\n${warn.join('\n')}\n\nยืนยันย้ายหรือไม่?`)) return

    setGrid(g => ({ ...g, rows: res.rows, baseMc: base }))
    setDirty(true)
    // ล็อกตามงานไปช่องใหม่ (ทำหลังยืนยันครบทุก dialog แล้ว — ยกเลิกกลางทางจะไม่ย้ายล็อก)
    followLock(oldRow, res.rows[keep])
    // รวมก้อนแล้วเลขแถวขยับ — ปิด panel/modal ที่ชี้แถวเดิมไว้ กันไปโชว์งานผิดตัว
    if (twin >= 0) setSelJob(null)
    setMsg((twin >= 0 ? `รวมกับก้อนเดิมใน ${wkLabel(week)} เป็น ${mergedQty.toLocaleString()} กก. · ` : '')
      + changeSummary(item, week, res, keep))
  }

  // ══ แก้งานบน Gantt จ้างทอ ═════════════════════════════════════════════════
  // แถวจ้างทอมีโลกของเครื่องคนละใบกับแผนในบ้าน จึงใช้ moveJob/recalcPlanRows ไม่ได้:
  //   - MC_GROUP ของแถวจริงต้องคง COMKN ไว้ (พูลจริงอยู่ที่ S9_MC/S9_GAUGE)
  //   - กำลังผลิตคิดจาก Cap/Day × Working day ของชีท MC S9 ไม่ใช่ปฏิทินวันทำงานเครื่องในบ้าน
  //   - เพดานคือ Total MC ของพูลนั้น ไม่ใช่โควตา setup ของโรงเรา
  const s9PoolAt = (mc, g) => (s9Pool || {})[`${String(mc).toUpperCase()}|${String(g)}`] || null
  // พูลย่อย = 1 แถวในชีท MC S9 เพดานแข็งแยกกัน (เช่น RAP เกจ 28: ทั่วไป 17 / Poly Only 6)
  // ต้องตรงกับ Planning.py (_s9_pool_key / _s9_pool_row / คอลัมน์ S9_POOL):
  //   แถวมี S9_POOL ของคู่นี้ → ใช้ค่านั้น ; ไม่มี (ไฟล์เก่า / ลากมาจากในบ้าน / ย้ายคู่) → เลือกแบบเดียวกับ
  //   Planning: CAT ตรงก่อน → งาน POLY ไป Poly Only, งานอื่นไปแถวทั่วไป → Total MC มากสุด
  // คืน null = ชีทไม่มีรายการพูลย่อย (backend รุ่นเก่า) → ผู้เรียกถอยไปใช้ภาพรวมของคู่
  const s9ColIx = (n) => (grid ? grid.columns.indexOf(n) : -1)
  function s9SubPoolFor(row, mc, g) {
    const subs = s9PoolAt(mc, g)?.subpools || []
    if (!subs.length) return null
    const pi = s9ColIx('S9_POOL')
    const own = pi >= 0 ? String(norm(row[pi])).trim() : ''
    const hit = own ? subs.find(s => s.id === own) : null
    if (hit) return hit
    let cand = subs
    const ci = s9ColIx('CAT')
    const cat = ci >= 0 ? upcode(norm(row[ci])) : ''
    const byCat = cand.filter(s => s.cat && s.cat.toUpperCase() === cat)
    if (byCat.length) cand = byCat
    const mi = s9ColIx('MATERIAL_CONTENT')
    // หมวดผ้าของงาน → POLY / COTTON / BLEND (ตรงกับ _s9_material_class ใน Planning.py)
    // เจ้าที่รับหมวดนั้นเท่านั้นถึงจะเป็นตัวเลือก ; ไฟล์รุ่นเก่าที่ backend ส่ง mats มาไม่ได้ ใช้ธง poly เดิม
    const mat = mi >= 0 ? upcode(norm(row[mi])) : ''
    const cls = (!mat || mat.includes('CD') || mat.includes('TC')
      || (mat.includes('COTTON') && mat.includes('POLY'))) ? 'BLEND'
      : mat.includes('POLY') ? 'POLY'
        : mat.includes('COTTON') ? 'COTTON' : 'BLEND'
    const byMat = cand.filter(s => (s.mats ? s.mats.includes(cls) : !!s.poly === (cls === 'POLY')))
    if (byMat.length) cand = byMat
    return cand.slice().sort((a, b) => (b.total || 0) - (a.total || 0))[0]
  }
  // cap ต่อเครื่อง/วันของงานจ้างทอ = CAP ของผ้าตัวนั้นในชีท Item S9 (ตรงกับ _s9_pool_row ใน Planning.py)
  // ไม่มีในชีท → cap ของพูลย่อย → ภาพรวมของคู่
  const s9CapFor = (item, g, sub, pool) => {
    const c = Number((s9ItemCap || {})[`${upcode(item)}|${String(g)}`])
    return c > 0 ? c : (sub ? sub.cap : pool?.cap)
  }
  const s9SubLabel = (mc, g, sub) => `${mc} Guage ${g}${sub && sub.remark ? ` (${sub.remark})` : ''}`
  // เครื่องของพูลนั้นที่แถวแผนอื่นจองไว้แล้วในสัปดาห์นั้น (ทั้งกระดาน ไม่สนตัวกรอง)
  // subId = นับเฉพาะงานที่อยู่พูลย่อยเดียวกัน (เพดานแยกกัน) ; ไม่ส่ง = ทั้งคู่กลุ่ม·เกจ
  function s9UsedAt(week, mc, g, skipIdx, subId = null) {
    if (!grid || !s9Ci) return 0
    const ai = grid.columns.indexOf('ACTUAL_MC')
    if (ai < 0) return 0
    let n = 0
    grid.rows.forEach((row, i) => {
      if (i === skipIdx || !isS9Row(row)) return
      if (rowKeyOf(row) !== String(week)) return
      if (norm(row[s9Ci.s9mc]).toUpperCase() !== String(mc).toUpperCase()) return
      if (gnum(row[s9Ci.s9g]) !== String(g)) return
      if (subId && s9SubPoolFor(row, mc, g)?.id !== subId) return
      n += Number(norm(row[ai])) || 0
    })
    return n
  }
  function moveOutsourceJob(ri, week, mcKey) {
    if (!grid || !s9Ci) return
    const cols = grid.columns
    const ix = (n) => cols.indexOf(n)
    const row = grid.rows[ri].slice()
    const gs = (n) => { const k = ix(n); return k >= 0 ? norm(row[k]) : '' }
    const set = (n, v) => { const k = ix(n); if (k >= 0) row[k] = v }
    const item = gs('ITEM_CODE')
    const oldWeek = rowKeyOf(row)
    const oldMc = norm(row[s9Ci.s9mc]).toUpperCase(), oldG = gnum(row[s9Ci.s9g])
    // หย่อนข้ามแถว → mcKey = "พูล·เกจ" ของแถวปลายทาง ; หย่อนแถวเดิม = พูลเดิม
    const [tgtMc0, tgtG0] = String(mcKey || '').split(S9_MC_SEP)
    const mc = (tgtMc0 || oldMc).toUpperCase(), g = tgtG0 != null && tgtG0 !== '' ? tgtG0 : oldG
    const pool = s9PoolAt(mc, g)
    if (!pool) { setMsg(`ไม่พบพูลเครื่องจ้างทอ ${mc} Guage ${g} ในชีท MC S9`); return }
    if (mc !== oldMc || g !== oldG) {
      if (!window.confirm(`ย้ายงาน ${item} ไปพูลจ้างทออื่น\n\n${oldMc} Guage ${oldG} → ${mc} Guage ${g} (สัปดาห์ ${wkLabel(week)})\n\n`
        + `⚠ Guage ต่างกันแปลว่าเครื่องคนละแบบ — ยืนยันว่าผู้รับจ้างทอผ้าตัวนี้ด้วยเครื่องนี้ได้จริง`)) return
    }
    // เครื่องที่ต้องใช้ = ยอด ÷ (Cap/วัน × วันทำงานของผู้รับจ้าง) ; สัปดาห์แรกของก้อนถือว่าตั้งเครื่องใหม่
    // เพดานของ "พูลย่อย" ที่งานนี้ลง ; กำลังผลิต = CAP ของผ้าตัวนี้ (ชีท Item S9) ก่อน cap ของพูล
    const sub = s9SubPoolFor(row, mc, g)
    const cap = s9CapFor(item, g, sub, pool)
    const total = sub ? sub.total : pool.total
    const where = s9SubLabel(mc, g, sub)
    const qty = Number(gs('PRODUCE_QTY')) || 0
    const perMc = (cap || 0) * (pool.wd || 0)
    const need = perMc > 0 ? Math.max(1, Math.ceil(qty / perMc)) : Number(gs('ACTUAL_MC')) || 1
    const remain = (total || 0) - s9UsedAt(week, mc, g, ri, sub?.id)
    if (need > remain && !window.confirm(
      `สัปดาห์ ${wkLabel(week)} พูล ${where} เหลือ ${remain} เครื่อง แต่งานนี้ต้องใช้ ${need} เครื่อง\n\n`
      + `วางแล้วจะเกินเครื่องที่ผู้รับจ้างมี — ยืนยันวางหรือไม่?`)) return

    setRowWeek(row, cols, week)
    set('S9_MC', mc); set('S9_GAUGE', g); set('S9_POOL', sub?.id || '')
    set('ACTUAL_MC', need); set('NEW_MC', need); set('CARRYOVER_MC', 0)
    set('DAILY_CAPACITY', cap || 0)
    set('FACTORY_WORKING_DAYS', pool.wd || 0)
    set('ACTUAL_WORKING_DAYS', pool.wd || 0)
    set('AVAILABLE_DAYS', pool.wd || 0)
    set('REMARK', `จ้างทอ: ${where} — ${need} เครื่อง × ${pool.wd} วัน × ${cap} กก./วัน`)
    const rows = grid.rows.slice()
    rows[ri] = row
    setGrid(g2 => ({ ...g2, rows }))
    setDirty(true)
    setMsg(`ย้ายงานจ้างทอ ${item} ${wkLabel(oldWeek)} → ${wkLabel(week)}`
      + (mc !== oldMc || g !== oldG ? ` · พูล ${oldMc} Guage ${oldG} → ${mc} Guage ${g}` : '')
      + ` · ใช้ ${need} เครื่อง (พูลเหลือ ${Math.max(0, remain - need)})`)
  }
  // ลากงานบน Gantt — แถวจ้างทอกับแถวในบ้านอยู่ตารางเดียวกัน จึงต้องแยกเองว่าก้อนนี้เป็นแบบไหน
  // และ "ลากข้ามฝั่ง" มีความหมายชัดในตัว: ลากเข้าแถวเครื่องในบ้าน = ดึงกลับมาทอเอง /
  // ลากไปแถวพูลผู้รับจ้าง = ส่งออกไปจ้างทอ (หัวแถวของผู้รับจ้างมี "·" คั่นพูลกับเกจเสมอ)
  // ลากก้อนที่ Gantt ยุบใบสั่งหลายใบไว้ = ย้ายทุกแถวในก้อนพร้อมกัน (ผู้เรียกส่ง ri มาเป็น array)
  //
  // ⚠️ ต้องทำในรอบเดียว — moveJob อ่าน `grid` ของเรนเดอร์ปัจจุบันทุกครั้ง ถ้าวนเรียกทีละใบ
  // ใบที่ 2 จะเริ่มจาก grid ตัวเดิม (ยังไม่มีผลของใบที่ 1) แล้ว setGrid ทับผลใบแรกทิ้ง
  //
  // ต่างจาก moveJob ตรง "ไม่รวมกับก้อนแฝดที่ปลายทาง" (twin) — ย้ายมาหลายใบพร้อมกัน การยุบรวม
  // ยอดจะทำให้ยอดของใบไหนไปอยู่กับใบไหนไม่ชัด ; ปล่อยเป็นคนละแถวได้ เพราะ Gantt ยุบให้เห็น
  // เป็นก้อนเดียวอยู่แล้ว (ยอด/เครื่องบนบล็อกรวมมาให้)
  function moveJobs(idxs, week, mcGroup) {
    if (!grid) return
    const list = [...new Set(idxs)].filter(i => grid.rows[i])
    if (!list.length) return
    if (list.length === 1) return moveAnyJob(list[0], week, mcGroup)
    // งานจ้างทออยู่คนละโลกของเครื่อง (พูลผู้รับจ้าง + CAP คนละชุด) — ย้ายรวมทีเดียวไม่ได้
    if (hasS9Cols && list.some(i => isS9Row(grid.rows[i]))) {
      setMsg('งานจ้างทอย้ายทีละใบ — กดกาง ▾ บนบล็อกแล้วลากทีละใบ')
      return
    }
    const cols = grid.columns
    const wci = cols.indexOf('PLAN_WEEK'), mci = cols.indexOf('MC_GROUP'), ici = cols.indexOf('ITEM_CODE')
    const first = grid.rows[list[0]]
    const item = ici >= 0 ? norm(first[ici]) : ''
    const curMc = mci >= 0 ? norm(first[mci]) : ''
    const crossMc = mcGroup && mci >= 0 && String(mcGroup).trim().toUpperCase() !== curMc.trim().toUpperCase()
    if (crossMc && !window.confirm(
      `ยืนยันย้ายงาน ${item} ทั้ง ${list.length} ใบสั่ง จากเครื่อง ${curMc} → ${mcGroup} (สัปดาห์ ${wkLabel(week)})?`)) return
    const gsOld = (n) => { const k = cols.indexOf(n); return k >= 0 ? norm(first[k]) : '' }
    const awdTo = actualWdAt(gsOld('MC_GROUP'), gsOld('MC_GUAGE'), week)
    if (awdTo != null && awdTo <= 0
      && !window.confirm(`สัปดาห์ ${wkLabel(week)} ไม่มีวันทำงาน (โรงปิด/ยุบเข้าสัปดาห์อื่น) — วางแล้วผลิตไม่ได้\n\nยืนยันย้ายหรือไม่?`)) return

    const rows = grid.rows.slice()
    const olds = list.map(i => grid.rows[i])
    for (const i of list) {
      const row = rows[i].slice()
      const oldWeek = rowKeyOf(row)
      if (wci >= 0) setRowWeek(row, cols, week)
      if (crossMc) row[mci] = mcGroup
      if (String(oldWeek) !== String(week) || crossMc) recalcRowForWeek(row, cols, week)
      rows[i] = row
    }
    // เมล็ดคำนวณลูกโซ่ = ทั้ง run ต้นทาง (แถวเดิม) และปลายทาง (แถวใหม่) ของทุกใบ
    const res = recalcPlanRows(cols, rows, [...olds, ...list.map(i => rows[i])], grid.baseMc || [])
    const warn = jobQuotaWarnings(cols, res.rows, res.changes)
    if (warn.length && !window.confirm(
      `⚠ ย้ายแล้ว job setup เกินโควตาสัปดาห์:\n\n${warn.join('\n')}\n\nยืนยันย้ายหรือไม่?`)) return

    setGrid(g => ({ ...g, rows: res.rows }))
    setDirty(true)
    list.forEach((i, k) => followLock(olds[k], res.rows[i]))
    setMsg(`ย้าย ${item} ${list.length} ใบสั่ง → ${wkLabel(week)}`
      + (crossMc ? ` · เครื่อง ${mcGroup}` : '') + ' · ' + changeSummary(item, week, res, list[0]))
  }

  function moveAnyJob(ri, week, mcKey) {
    // Gantt ยุบใบสั่งของผ้าตัวเดียวกันเป็นบล็อกเดียว → ลากก้อนนั้นส่ง idx มาเป็น array
    if (Array.isArray(ri)) return moveJobs(ri, week, mcKey)
    if (!grid) return
    const wasS9 = hasS9Cols && isS9Row(grid.rows[ri])
    const toS9 = String(mcKey || '').includes(S9_MC_SEP)
    if (wasS9 && toS9) return moveOutsourceJob(ri, week, mcKey)
    // โหมดยุบแถว: บล็อกจ้างทอนั่งอยู่ในแถวเครื่องในบ้าน (เจ้าบ้าน) — ลากภายในแถวเดิม
    // = เลื่อนสัปดาห์ของงานจ้างทอเฉย ๆ ไม่ใช่ "ดึงกลับมาทอในบ้าน" (ต้องลากไปเครื่องอื่นถึงจะใช่)
    if (wasS9 && s9HostByIdx) {
      const host = s9HostByIdx.get(ri)
      if (host && upcode(mcKey) === upcode(host.mcg)) return moveOutsourceJob(ri, week, '')
    }
    if (wasS9) return bringOutsourceHome(ri, week, mcKey)
    if (toS9) return sendJobToOutsource(ri, week, mcKey)
    return moveJob(ri, week, mcKey)
  }
  // ส่งงานในบ้านออกไปจ้างทอด้วยการลากไปวางบนแถวพูลของผู้รับจ้าง
  // ไม่เช็คว่าอยู่ในชีท "Item S9" ไหม — คนลากเป็นคนตัดสินใจ (แค่เตือนถ้าเครื่องไม่พอ)
  function sendJobToOutsource(ri, week, mcKey) {
    if (!grid || !s9Ci) return
    const cols = grid.columns
    const ix = (n) => cols.indexOf(n)
    const oldRow = grid.rows[ri]
    const item = upcode(norm(oldRow[s9Ci.item]))
    const [mc0, g0] = String(mcKey).split(S9_MC_SEP)
    const mc = String(mc0 || '').toUpperCase(), g = gnum(g0)
    const pool = s9PoolAt(mc, g)
    if (!pool) { setMsg(`ไม่พบพูลเครื่องจ้างทอ ${mc} Guage ${g} ในชีท MC S9`); return }
    // พูลย่อยเลือกจากแถวในบ้าน (CAT + ชนิดเส้น) ก่อนแถวถูกเขียนทับเป็น COMKN
    const sub = s9SubPoolFor(oldRow, mc, g)
    const cap = s9CapFor(item, g, sub, pool)
    const total = sub ? sub.total : pool.total
    const where = s9SubLabel(mc, g, sub)
    const qty = Number(norm(oldRow[s9Ci.qty])) || 0
    const perMc = (cap || 0) * (pool.wd || 0)
    const need = perMc > 0 ? Math.max(1, Math.ceil(qty / perMc)) : 1
    const remain = (total || 0) - s9UsedAt(week, mc, g, ri, sub?.id)
    if (!window.confirm(
      `ส่งงาน ${item} ออกไปจ้างทอ\n\nเครื่อง ${norm(oldRow[s9Ci.mcg])} → ผู้รับจ้าง ${where} สัปดาห์ ${wkLabel(week)}\n`
      + `ใช้ ${need} เครื่อง (พูลเหลือ ${remain})`
      + (need > remain ? '\n\n⚠ เกินเครื่องที่ผู้รับจ้างมีในสัปดาห์นี้' : ''))) return

    const rows = grid.rows.slice()
    const row = oldRow.slice()
    const set = (n, v) => { const k = ix(n); if (k >= 0) row[k] = v }
    // แถวจริงต้องเป็น COMKN/C0 เหมือนที่ Planning.py เขียน — พูลจริงเก็บที่ S9_MC/S9_GAUGE
    set('S9_ROUTING', 1); set('S9_MC', mc); set('S9_GAUGE', g); set('S9_POOL', sub?.id || '')
    setRowWeek(row, cols, week)
    set('MC_GROUP', 'COMKN'); set('MC_GUAGE', 'C0'); set('CAT', 'COMKN')
    set('FACTORY_TYPE', 'OUTSOURCE')
    set('ACTUAL_MC', need); set('NEW_MC', need); set('CARRYOVER_MC', 0)
    set('DAILY_CAPACITY', cap || 0)
    set('FACTORY_WORKING_DAYS', pool.wd || 0)
    set('ACTUAL_WORKING_DAYS', pool.wd || 0)
    set('AVAILABLE_DAYS', pool.wd || 0)
    set('REMARK', `จ้างทอ: ${where} — ${need} เครื่อง × ${pool.wd} วัน × ${cap} กก./วัน (ย้ายจากในบ้าน)`)
    rows[ri] = row
    // เครื่องในบ้านของสัปดาห์เดิมถูกคืนพูล → ลูกโซ่ของ run เดิมต้องคิด carry/setup ใหม่
    const res = recalcPlanRows(cols, rows, [oldRow], grid.baseMc || [])
    setGrid(g2 => ({ ...g2, rows: res.ok ? res.rows : rows }))
    setDirty(true)
    setMsg(`ส่ง ${item} ไปจ้างทอ ${wkLabel(week)} · ${mc} Guage ${g} ใช้ ${need} เครื่อง`
      + (need > remain ? ' ⚠ เกินเครื่องที่ผู้รับจ้างมี' : ` (พูลเหลือ ${Math.max(0, remain - need)})`))
  }
  // พูลผู้รับจ้างที่ส่งงานในบ้านแถวนี้ไปได้ — ใช้กับปุ่ม "🧵 ส่งไปจ้างทอ" ในการ์ดงาน
  // เข้าเกณฑ์ = (item, เกจ) อยู่ในชีท Item S9 (ตรงกับ _s9_eligible_items) + มีพูลเกจเดียวกันในชีท MC S9
  // เรียงแบบ _s9_pool_row: CAT ตรงก่อน → Total MC มากสุด (พูลย่อย POLY/ทั่วไป sendJobToOutsource เลือกเอง)
  // คืน [] = ไม่มีปุ่ม (แถวจ้างทออยู่แล้ว / ไม่อยู่ในชีท / ไม่มีพูล) — ยังลากไปแถวผู้รับจ้างได้เหมือนเดิม
  function s9ChoicesFor(ri) {
    if (!grid || !hasS9Cols || s9Ci.gauge < 0 || s9Ci.item < 0) return []
    const row = grid.rows[ri]
    if (!row || isS9Row(row)) return []
    const item = upcode(norm(row[s9Ci.item])), g = gnum(row[s9Ci.gauge])
    if (!item || !g || !s9Elig.has(`${item}|${g}`)) return []
    const cat = s9Ci.cat >= 0 ? upcode(norm(row[s9Ci.cat])) : ''
    return Object.entries(s9Pool || {})
      .filter(([k]) => k.split('|')[1] === g)
      .map(([k, p]) => {
        const mc = k.split('|')[0]
        const sub = s9SubPoolFor(row, mc, g)
        // CAT ไม่ตรง = ผู้รับจ้างไม่มีเครื่องชนิดนี้ — รอบวางแผนไม่เลือกให้แล้ว (S9_CAT_STRICT ใน Planning.py)
        // ยังเลือกเองได้เหมือนการลาก แต่ต้องเห็นว่ากำลังข้ามชนิดเครื่อง
        const catHit = (p.cats || []).some(c => upcode(c) === cat) ? 1 : 0
        return {
          key: s9RowKey(mc, g),
          label: `${s9SubLabel(mc, g, sub)} · ${sub ? sub.total : (p.total || 0)} เครื่อง`
            + (catHit ? '' : ' ⚠ CAT ไม่ตรง'),
          catHit,
          total: p.total || 0,
        }
      })
      .sort((a, b) => (b.catHit - a.catHit) || (b.total - a.total))
  }
  // ดึงงานจ้างทอกลับมาทอในบ้าน — ล้างธง S9 แล้วคืนแถวให้เป็นงานของเครื่องเราตามปกติ
  // เครื่องปลายทาง: ใช้กลุ่มที่ item นี้ทออยู่แล้วในแผน ถ้าไม่มีก็ให้ user เลือกจากเครื่องเกจเดียวกัน
  // เกินพูล/โควตา setup ไม่ห้าม — แค่เตือนให้ตัดสินใจเอง (เหมือนลากงานปกติ)
  function bringOutsourceHome(ri, toWeek = null, toMc = null) {
    if (!grid || !s9Ci) return
    const cols = grid.columns
    const ix = (n) => cols.indexOf(n)
    const oldRow = grid.rows[ri]
    const item = upcode(norm(oldRow[s9Ci.item]))
    // ลากไปหย่อนแถวเครื่องในบ้าน = บอกทั้งสัปดาห์และเครื่องปลายทางมาแล้ว ไม่ต้องถามซ้ำ
    const week = String(toWeek != null && toWeek !== '' ? toWeek : rowKeyOf(oldRow))
    const s9g = gnum(oldRow[s9Ci.s9g])
    // 1) กลุ่มเครื่องที่ item นี้ใช้อยู่แล้วในแผน (ในบ้าน) — ตรงที่สุด เพราะมี CAP จริงแน่นอน
    const cand = new Map()
    grid.rows.forEach((r) => {
      if (isS9Row(r)) return
      if (upcode(norm(r[s9Ci.item])) !== item) return
      const mc = norm(r[s9Ci.mcg]); if (!mc) return
      cand.set(mc, { mc, gauge: norm(r[s9Ci.gauge]), cat: s9Ci.cat >= 0 ? norm(r[s9Ci.cat]) : '' })
    })
    // 2) ไม่มีเลย → เครื่องในบ้านที่เกจตรงกับที่จ้างทออยู่ (จาก MasterMC)
    if (!cand.size) {
      for (const m of (workdayData?.mc_rows || [])) {
        const g = gnum(m.gauge ?? m.GUAGE ?? '')
        if (g !== s9g) continue
        const mc = String(m.mc_group ?? m.MC_GROUP ?? m.mc ?? '').trim()
        if (mc) cand.set(mc, { mc, gauge: g, cat: String(m.cat ?? m.CAT ?? '').trim() })
      }
    }
    const list = [...cand.values()]
    // ลากมาหย่อนแถวเครื่องไหน = เลือกเครื่องนั้น (เอาหัวแถวของปลายทางมาใช้ตรง ๆ)
    if (toMc) {
      const hit = [...cand.values()].find(c => c.mc.toUpperCase() === String(toMc).toUpperCase())
      const pickDrag = hit || { mc: String(toMc), gauge: s9g, cat: '' }
      if (!window.confirm(`ดึงงาน ${item} กลับมาทอในบ้าน

จ้างทอ → เครื่อง ${pickDrag.mc}`
        + `${pickDrag.gauge ? ` Guage ${pickDrag.gauge}` : ''} สัปดาห์ ${wkLabel(week)}?`)) return
      return applyBringHome(ri, oldRow, cols, ix, item, week, pickDrag)
    }
    if (!list.length) { setMsg(`ไม่พบกลุ่มเครื่องในบ้านที่ทอ ${item} Guage ${s9g} ได้ — ดึงกลับไม่ได้`); return }
    let pick = list[0]
    if (list.length > 1) {
      const ans = window.prompt(
        `ดึงงาน ${item} (${wkLabel(week)}) กลับมาทอในบ้าน — เลือกกลุ่มเครื่อง:\n\n`
        + list.map((c, i) => `${i + 1}. ${c.mc}${c.gauge ? ` Guage ${c.gauge}` : ''}${c.cat ? ` (${c.cat})` : ''}`).join('\n')
        + `\n\nพิมพ์หมายเลข`, '1')
      if (ans == null) return
      const k = parseInt(ans, 10)
      if (!(k >= 1 && k <= list.length)) { setMsg('เลือกกลุ่มเครื่องไม่ถูกต้อง — ยกเลิกการดึงกลับ'); return }
      pick = list[k - 1]
    } else if (!window.confirm(`ดึงงาน ${item} (${wkLabel(week)}) กลับมาทอในบ้านด้วยเครื่อง ${pick.mc}${pick.gauge ? ` Guage ${pick.gauge}` : ''}?`)) return
    applyBringHome(ri, oldRow, cols, ix, item, week, pick)
  }
  // เขียนแถวกลับเป็นงานในบ้าน + คำนวณวัน/เครื่อง/ลูกโซ่ใหม่ (ใช้ร่วมกันทั้งลากกลับและกดปุ่ม)
  function applyBringHome(ri, oldRow, cols, ix, item, week, pick) {
    const rows = grid.rows.slice()
    const row = oldRow.slice()
    const set = (n, v) => { const k = ix(n); if (k >= 0) row[k] = v }
    set('S9_ROUTING', ''); set('S9_MC', ''); set('S9_GAUGE', ''); set('S9_POOL', ''); set('OUTSOURCE', '')
    setRowWeek(row, cols, week)  // ลากกลับข้ามสัปดาห์ได้ → ต้องเขียนสัปดาห์ปลายทางลงแถวด้วย
    set('MC_GROUP', pick.mc)
    if (pick.gauge) set('MC_GUAGE', pick.gauge)
    if (pick.cat) set('CAT', pick.cat)
    set('CARRYOVER_MC', 0)
    rows[ri] = row
    // คำนวณวันทำงาน/กำลังผลิต/เครื่องของสัปดาห์นั้นใหม่ตามเครื่องในบ้าน แล้วซ่อมลูกโซ่ carry ทั้ง run
    recalcRowForWeek(row, cols, week)
    const res = recalcPlanRows(cols, rows, [oldRow, rows[ri]], grid.baseMc || [])
    const warn = jobQuotaWarnings(cols, res.rows, res.changes)
    if (warn.length && !window.confirm(
      `⚠ ดึงกลับแล้ว job setup เกินโควตาสัปดาห์:\n\n${warn.join('\n')}\n\nยืนยันดึงกลับหรือไม่?`)) return
    setGrid(g2 => ({ ...g2, rows: res.rows }))
    setDirty(true)
    setSelJob(null)
    setMsg(`ดึง ${item} กลับมาทอในบ้าน (${pick.mc}) · ` + changeSummary(item, week, res, ri))
  }

    // ── ตารางเทียบ "แผนก่อนติ๊ก → แผนหลัง Change Cylinder" ─────────────────────
  // ต่างจาก buildPlanCompare ตรงที่ "แผนเก่า" คือแถวแผนก่อนติ๊ก ไม่ใช่ booking
  // (Change Cylinder เทียบผลของการติ๊กครั้งนั้น ไม่ได้เทียบกับใบจองเดิม)
  // คืนเฉพาะ item ที่ยอดรายสัปดาห์เปลี่ยนจริง — รวม item อื่นที่โดนขยับเพราะเครื่อง/setup ใหม่
  function buildCylCompare(cols, before, after) {
    const iI = cols.indexOf('ITEM_CODE'), wI = cols.indexOf('PLAN_WEEK')
    const qI = cols.indexOf('PRODUCE_QTY'), mI = cols.indexOf('MC_GROUP')
    if (iI < 0 || wI < 0 || qI < 0) return []
    const bag = (rs) => {
      const m = new Map()
      for (const r of rs || []) {
        const k = norm(r[iI]).toUpperCase()
        const w = rowWeekKey(r, wI, cols.indexOf('PLAN_YEAR'))
        if (!k || !w) continue
        const e = m.get(k) || { mcg: mI >= 0 ? norm(r[mI]) : '', weeks: {} }
        e.weeks[w] = Math.round(((e.weeks[w] || 0) + (Number(norm(r[qI])) || 0)) * 100) / 100
        m.set(k, e)
      }
      return m
    }
    const B = bag(before), A = bag(after)
    const out = []
    for (const item of new Set([...B.keys(), ...A.keys()])) {
      const b = B.get(item)?.weeks || {}, a = A.get(item)?.weeks || {}
      const ws = new Set([...Object.keys(b), ...Object.keys(a)])
      if (![...ws].some(w => Math.abs((b[w] || 0) - (a[w] || 0)) > 0.01)) continue
      out.push({ item, mc_group: (A.get(item) || B.get(item)).mcg, before: b, after: a })
      if (out.length >= 40) break     // กัน log บวมเวลาแผนขยับทั้งใบ
    }
    return out
  }

  // ── ติ๊ก Change Cylinder แล้วให้ระบบย้ายงานให้เอง ────────────────────────────
  // เครื่องที่เพิ่งเปลี่ยน Cylinder พร้อมใช้ที่ need_week → ดึงงานของ item นั้นที่ถูกดันไปไว้หลัง
  // need_week กลับมาผลิตให้เร็วขึ้น เท่าที่เครื่องที่เพิ่งได้มารับไหว
  //
  // ใช้ recalcPlanRows / recalcRowForWeek ตัวเดียวกับตอนลากบล็อก → carry/setup/วันผลิต/
  // MC_BOOKING คิดด้วยสูตรเดียวกันเป๊ะ (ห้ามคิดสูตรใหม่ที่นี่ ไม่งั้นตัวเลขสองทางหลุดจากกัน)
  // ไม่แตะงานที่ล็อกไว้ — ล็อกคือเจตนาของ user ที่แรงกว่าข้อเสนอของระบบ
  async function applyCylinderPicks(picks, approvalIds) {
    // เครื่องว่างชุดใหม่ (backend บวกเครื่องที่เพิ่งติ๊กให้แล้ว) — โหลดก่อนเสมอ แม้ไม่มีงานให้ย้าย
    await loadAva(grid?.sheet, !!grid?.replan)
    if (!grid || !picks || picks.length === 0) return 0
    // เกจในแผนเขียนได้หลายแบบ (22 / 22.0 / '22 ') → ยุบให้เทียบกับค่าจาก backend ได้
    const gkey = (g) => {
      const n = Number(norm(g))
      return Number.isFinite(n) ? String(Math.trunc(n)) : norm(g).trim().toUpperCase()
    }
    const cols = grid.columns
    const ci = {
      week: cols.indexOf('PLAN_WEEK'), item: cols.indexOf('ITEM_CODE'),
      cat: cols.indexOf('CAT'), gauge: cols.indexOf('MC_GUAGE'),
      mcg: cols.indexOf('MC_GROUP'), fac: cols.indexOf('FACTORY_TYPE'),
      qty: cols.indexOf('PRODUCE_QTY'), amc: cols.indexOf('ACTUAL_MC'),
      so: cols.indexOf('SC_SO_NO'),
    }
    if (ci.week < 0 || ci.item < 0) return 0

    const rows = grid.rows.slice()
    const base = (grid.baseMc || []).slice()
    const seeds = []
    const moves = []
    for (const p of picks) {
      // สัปดาห์พร้อมจาก advisor = คีย์คอลัมน์ (เลขเปล่า = ครั้งที่ใกล้วันนี้สุด) → เทียบบนแกนเรียงข้ามปี
      const ready = String(p.need_week ?? '').trim().replace(/\.0+$/, '')
      if (!ready || !Number.isFinite(wkNum(ready))) continue
      // ห้ามดึงงานมาผลิตก่อนด้ายเข้าครบ — เครื่องพร้อมแต่ไม่มีด้ายก็ทอไม่ได้
      // (advisor เลื่อน need_week ให้ตรงกับด้ายเข้าอยู่แล้ว ตรงนี้เป็นด่านกันซ้ำฝั่งย้ายงาน
      //  เพราะ user แก้จำนวน/สัปดาห์เองได้ก่อนกดใช้)
      const yarnW = String(p.yarn_week ?? '').trim().replace(/\.0+$/, '')
      if (yarnW && Number.isFinite(wkNum(yarnW)) && weekSortKey(ready) < weekSortKey(yarnW)) continue
      let budget = Number(p.machines) || 0          // เครื่องที่เพิ่งเปลี่ยน Cylinder มาให้
      // งานของ item นี้ที่วางไว้ "หลัง" สัปดาห์ที่เครื่องพร้อม → ดึงกลับมาได้
      const cand = []
      rows.forEach((r, i) => {
        if (norm(r[ci.item]).toUpperCase() !== String(p.item_code).toUpperCase()) return
        const w = rowKeyOf(r)
        if (!w || !Number.isFinite(wkNum(w)) || weekSortKey(w) <= weekSortKey(ready)) return
        if (ci.gauge >= 0 && gkey(norm(r[ci.gauge])) !== gkey(p.gauge)) return
        if (ci.fac >= 0 && p.factory && norm(r[ci.fac]).toUpperCase() !== String(p.factory).toUpperCase()) return
        if (isRowLocked(r)) return                  // ล็อกไว้ = user สั่งให้อยู่ตรงนั้น
        cand.push({ i, w, mc: ci.amc >= 0 ? (Number(norm(r[ci.amc])) || 0) : 1 })
      })
      cand.sort((a, b) => weekSortKey(a.w) - weekSortKey(b.w))                // ก้อนที่ใกล้ที่สุดก่อน (ขยับน้อยสุด)
      // job setup ที่ยังตั้งเครื่องใหม่ได้ในสัปดาห์ปลายทาง — ย้ายมาแล้วต้องตั้งเครื่อง
      // ตัวเลขชุดเดียวกับแถบ Set up Job/Week บน Gantt (old + booking + NEW_MC ที่วางแล้ว)
      let jobBudget = jobSlotsLeftAt(cols, rows, ready, p.factory, p.cat)
      for (const c of cand) {
        const need = Math.max(1, c.mc)
        if (budget < need) break                    // เครื่องที่ได้มาไม่พอรับก้อนนี้
        // ย้ายแล้ว job เกินโควตา = ทำจริงไม่ได้ → ข้ามก้อนนี้ (ไม่สร้างแผนที่เกินโควตาเอง)
        if (jobBudget != null && jobBudget < need) continue
        const oldRow = rows[c.i]
        const row = oldRow.slice()
        setRowWeek(row, cols, ready)
        recalcRowForWeek(row, cols, ready)
        rows[c.i] = row
        base[c.i] = 0                               // ยอด/สัปดาห์เปลี่ยน → เลขเครื่องเดิมใช้เป็นพื้นไม่ได้
        seeds.push(oldRow, row)
        budget -= need
        if (jobBudget != null) jobBudget -= need
        moves.push({
          item: norm(oldRow[ci.item]),
          so: ci.so >= 0 ? norm(oldRow[ci.so]) : '',
          from_week: c.w, to_week: ready,
          qty: ci.qty >= 0 ? (Number(norm(oldRow[ci.qty])) || 0) : null,
          mc: c.mc,
        })
      }
    }
    if (moves.length === 0) {
      setMsg('🔄 เปลี่ยน Cylinder แล้ว — เลขเครื่องว่างอัปเดตแล้ว (ยังไม่มีงานที่ย้ายกลับมาได้อัตโนมัติ ลากเองได้)')
      return 0
    }

    const res = recalcPlanRows(cols, rows, seeds, base)
    if (!res.ok) {
      setMsg('⚠ เปลี่ยน Cylinder แล้ว แต่คำนวณเครื่อง/setup ใหม่ให้ไม่ได้ (ยังโหลดข้อมูลวันทำงานไม่เสร็จ) — ลากงานเองได้')
      return 0
    }
    // ตั้งเครื่องใหม่กินโควตา job setup — ย้ายให้เลยแล้ว "เตือน" เท่านั้น ไม่ถามซ้ำและไม่ยกเลิกให้
    // (user แก้เองได้: ลากบล็อกบน Gantt หรือไม่กดบันทึกแผน — ระบบไม่ควรตัดสินใจแทน)
    const warn = jobQuotaWarnings(cols, res.rows, res.changes)

    setGrid(g => ({ ...g, rows: res.rows, baseMc: base }))
    setDirty(true)
    try {
      // เทียบกับแถวชุดเดิมที่โหลดมา (grid.rows) ไม่ใช่ rows ที่ถูกแก้ระหว่างทาง
      const compare = buildCylCompare(cols, grid.rows, res.rows)
      await api.cylLogMoves(moves, approvalIds, grid.sheet, grid.name, compare)
    } catch { /* log ล้มเหลวต้องไม่ทำให้แผนที่ย้ายแล้วหาย — เห็นใน console ของ server */ }
    setMsg(`🔄 เปลี่ยน Cylinder + ย้ายงานให้ ${moves.length} ก้อน: `
      + moves.slice(0, 4).map(m => `${m.item} ${wkLabel(m.from_week)}→${wkLabel(m.to_week)}`).join(' · ')
      + (moves.length > 4 ? ` และอีก ${moves.length - 4} ก้อน` : '')
      + (warn.length ? `\n⚠ job setup เกินโควตา: ${warn.join(' ')}` : '')
      + '\nไม่ถูกใจแก้เองได้ — ลากบล็อกบน Gantt แล้วกด "บันทึกแผน" (ถ้าไม่บันทึก การย้ายนี้จะไม่มีผลกับไฟล์แผน '
      + 'ส่วนเครื่องที่เปลี่ยน Cylinder บันทึกแล้ว ยกเลิกได้ที่ปุ่ม Change Cylinder)')
    return moves.length
  }

  // จำนวนสูงสุดที่แถว ri วางได้ = ยอดที่ต้องวางรอบนี้ (orderBaseOf) − ผลรวม PRODUCE_QTY ของแถวอื่นในออร์เดอร์เดียวกัน
  // ออร์เดอร์นิยามด้วย ORDER_KEY_COLS → ห้ามวางรวมเกินยอดที่ต้องวาง
  // ต้องใช้ฐานเดียวกับป้ายเตือน ไม่งั้นขัดกันเอง: ป้ายบอกวางครบแล้ว แต่ช่องยังให้พิมพ์เพิ่มได้จนถึงยอดสั่งทั้งใบ
  // คืน Infinity ถ้าไม่มียอดให้ยึด (ปล่อยผ่าน)
  function maxQtyForRow(ri) {
    if (!grid) return Infinity
    const cols = grid.columns
    const oq = cols.indexOf('ORDERS_QTY'), pq = cols.indexOf('PRODUCE_QTY')
    if (oq < 0 || pq < 0) return Infinity
    const row = grid.rows[ri]
    const ordered = orderBaseOf(row, cols)
    if (!Number.isFinite(ordered) || ordered <= 0) return Infinity
    const k = orderKeyOf(row, cols)
    let others = 0
    grid.rows.forEach((r, i) => { if (i !== ri && orderKeyOf(r, cols) === k) others += Number(norm(r[pq])) || 0 })
    return Math.max(0, Math.round((ordered - others) * 100) / 100)
  }

  // ถ้า PRODUCE_QTY ของแถว ri เกินยอดที่ต้องวาง → เตือนแล้วปรับลงมาเท่าที่วางได้ (คืน true ถ้าปรับ)
  function clampOrderQty(ri) {
    if (!grid) return false
    const pq = grid.columns.indexOf('PRODUCE_QTY')
    if (pq < 0) return false
    const cur = Number(norm(grid.rows[ri][pq])) || 0
    const max = maxQtyForRow(ri)
    if (cur > max + QTY_TOL) {
      window.alert(`วางเกินยอดที่ต้องวางของออร์เดอร์นี้ไม่ได้\nแถวนี้วางได้สูงสุด ${max.toLocaleString()} กก. — ปรับลงให้อัตโนมัติ`)
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
  // จำนวนเครื่อง/carry/setup คิดใหม่ทั้งลูกโซ่จาก "ยอดที่แบ่งแล้ว" — ทั้งสองแถวตั้งพื้นเครื่องเป็น 0
  // เพราะยอดเปลี่ยนไปจากที่ Planning.py วางไว้ เลขเดิมใช้เป็นพื้นไม่ได้อีก
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
    // ต้องเช็คก่อนแก้ยอด — ถ้าก้อนเดิมถูกล็อกไว้ ก้อนที่แยกออกไปต้องถูกล็อกด้วย
    // (งานก้อนเดียวกันที่ user ตั้งใจตรึง แค่ถูกผ่าเป็นสองสัปดาห์)
    const wasLocked = isRowLocked(row)
    const left = Math.round((cur - q) * 100) / 100
    const msg = `แบ่งงาน ${item}: ${q} กก. → สัปดาห์ ${wkLabel(week)} (เหลือ ${left} กก. ที่สัปดาห์เดิม)\n\n`
      + `ระบบจะคำนวณจำนวนเครื่อง / carry / setup ใหม่ให้ทั้งลูกโซ่ตามยอดที่แบ่ง\n\n`
      + `ยืนยันแบ่งหรือไม่?`
    if (!window.confirm(msg)) return

    const rows = grid.rows.slice()
    const orig = rows[ri].slice()
    orig[qci] = left
    const copy = rows[ri].slice()
    copy[qci] = q
    if (wci >= 0) setRowWeek(copy, grid.columns, week)
    recalcRowForWeek(copy, grid.columns, week)
    rows[ri] = orig
    rows.splice(ri + 1, 0, copy)
    const base = (grid.baseMc || []).slice()
    if (base.length) { base[ri] = 0; base.splice(ri + 1, 0, 0) }
    const res = recalcPlanRows(grid.columns, rows, [orig, copy], base)

    const warn = jobQuotaWarnings(grid.columns, res.rows, res.changes)
    if (warn.length && !window.confirm(
      `⚠ แบ่งแล้ว job setup เกินโควตาสัปดาห์:\n\n${warn.join('\n')}\n\nยืนยันแบ่งหรือไม่?`)) return

    setGrid(g => ({ ...g, rows: res.rows, baseMc: base }))
    setDirty(true)
    setMsg(`แบ่ง ${item}: ${q.toLocaleString()} กก. → สัปดาห์ ${wkLabel(week)}`
      + (res.changes.length ? ` (คำนวณเครื่องใหม่ ${res.changes.length} แถว)` : ''))
    // res.rows[ri] = ก้อนเดิม (ยอดลดลง) · res.rows[ri+1] = ก้อนที่แยกออกไปสัปดาห์ใหม่
    if (wasLocked) splitLock(res.rows[ri], res.rows[ri + 1])
  }

  // ── ตารางใน modal (คลิกบล็อกบน Gantt) = ทุกแถวของ item นั้น ไม่สนตัวกรองหน้าหลัก ─────
  // ยกออกมานอก JSX เพราะป็อปอัพตัวกรองของ modal ต้องใช้ชุดแถวนี้คิดลิสต์ค่าที่เลือกได้
  const modalRows = useMemo(() => {
    if ((selJob == null && !modalItem) || !grid) return null
    const ici = grid.columns.indexOf('ITEM_CODE')
    if (ici < 0) return selJob != null ? [{ row: grid.rows[selJob], idx: selJob }] : []
    // เปิดจากบล็อก booking → เทียบชื่อ item แบบตัดช่องว่าง/ไม่สนตัวพิมพ์ (booking กับชีทแผนคนละแหล่ง)
    // item ที่ไม่มีแถวในแผน (งานล็อกใน booking) ได้ชุดว่าง — modal ยังโชว์ตารางเทียบได้
    const match = selJob != null
      ? (v => v === grid.rows[selJob]?.[ici])
      : (v => upcode(v) === upcode(modalItem))
    // เรียงตามแกนเวลาจริง — ลำดับแถวในไฟล์เอาแผนของปีหน้า (เลขสัปดาห์น้อย) ขึ้นก่อนแผนปีนี้ได้
    return sortRowsByTimeline(
      grid.rows.map((row, idx) => ({ row, idx })).filter(({ row }) => match(row[ici])),
      grid.columns)
  }, [grid, selJob, modalItem])

  // แถวที่โชว์จริงใน modal = แถวของ item นั้น กรองด้วยตัวกรองของ modal เอง
  // คง idx จริงของทุกแถวไว้ → แก้ค่า/ลบแถวยังเขียนกลับ grid.rows ตำแหน่งเดิม Gantt จึงขยับตาม
  const modalVisible = useMemo(() => {
    if (!modalRows) return null
    const fcols = Object.keys(modalFilters).map(Number)
    if (!fcols.length) return modalRows
    return modalRows.filter(({ row }) => fcols.every(ci => modalFilters[ci].has(norm(row[ci]))))
  }, [modalRows, modalFilters])

  const colData = useMemo(() => {
    if (!grid || !openCol) return null
    // ป็อปอัพที่เปิดจากหัวตารางใน modal → ลิสต์ค่า/จำนวนคิดจากแถวของ item นั้นเท่านั้น (ไม่ใช่ทั้งชีท)
    // และไม่เอาคำค้นของหน้าหลักมาตัด เพราะตารางใน modal ไม่ได้ใช้คำค้นนั้นอยู่แล้ว
    if (openCol.scope === 'modal') {
      if (!modalRows) return null
      const mgrid = { columns: grid.columns, rows: modalRows.map(r => r.row) }
      return columnFilterData(mgrid, modalFilters, '', openCol.ci)
    }
    return columnFilterData(grid, filters, search, openCol.ci)
  }, [grid, filters, search, openCol, modalRows, modalFilters])

  const itemCi = grid ? grid.columns.indexOf('ITEM_CODE') : -1

  // แถวที่ผ่านคำค้น + ตัวกรองคอลัมน์ (ยังไม่กรองด้วยตัวกรอง Item) — ใช้เป็นฐานนับในป็อปอัพ
  // ต้องแยกจาก visible เพราะแบบ Excel: ลิสต์ค่าของตัวกรองตัวเอง ต้องไม่ถูกตัวมันเองตัดทิ้ง
  const baseRows = useMemo(() => (grid ? filterRows(grid, search, filters) : []), [grid, search, filters])

  // ── ข้อมูลป้อนป็อปอัพตัวกรอง Item (ตัวเดียวกับที่กด ▾ ที่หัวคอลัมน์) ─────────────
  // domain  = ITEM_CODE ทุกตัวที่ระบบรู้จัก — รวมจากแผนวันนี้ "และ" booking (แผนเก่า)
  //           จึงติ๊กเลือก item ที่ไม่ได้ถูกวางในแผนวันนี้ได้ ซึ่งเป็นสิ่งที่ตัวกรองคอลัมน์เดิมทำไม่ได้
  // available = ตัวที่ยังเหลือจริงหลังคำค้น/ตัวกรองคอลัมน์ พร้อมจำนวนที่พบ (แถวแผน + ช่องแผนเก่า)
  const itemData = useMemo(() => {
    const domain = new Set()
    const counts = new Map()
    const s = search.trim().toLowerCase()
    const bump = c => counts.set(c, (counts.get(c) || 0) + 1)
    if (grid && itemCi >= 0) {
      for (const r of grid.rows) { const c = upcode(norm(r[itemCi])); if (c) domain.add(c) }
      for (const { row } of baseRows) { const c = upcode(norm(row[itemCi])); if (c) bump(c) }
    }
    for (const b of bookingItems) {
      const c = upcode(b.item)
      if (!c) continue
      domain.add(c)
      if (!bkMatch(b, s)) continue
      bump(c)
    }
    const available = Array.from(counts, ([value, count]) => ({ value, count }))
    return { available, domain }
  }, [grid, itemCi, baseRows, bookingItems, search])

  // ── ชีท Program (Master) → ทำตัวหนังสือเป็นสีน้ำเงิน ────────────────────────
  // เงื่อนไข: ITEM_CODE ของแถว ต้องตรงกับชีท Program "และ" ทีมของแถวต้องตรงด้วย
  // ⚠ ทีมที่ใช้เทียบคือ TEAM_PROGRAM (ทีมตามชีท Program ล้วน) ไม่ใช่ TEAM ซึ่งตอนนี้เป็น
  //    "ทีมจริง" ที่ map มาจาก booking/สต๊อก — เทียบกับ TEAM จะได้ไฮไลต์ตามข้อมูล booking
  //    ไม่ใช่ตามที่ user กำหนดในชีท Program
  // ⚠ ไฟล์แผนเก่าที่ยังไม่มี TEAM_PROGRAM → ถอยไปใช้ TEAM (สมัยที่ TEAM ยังเป็นทีมจากชีท
  //    Program) ; ไม่มีทั้งคู่ → เทียบด้วย ITEM อย่างเดียว ไม่งั้นจะไม่มีอะไรไฮไลต์เลย
  // (บล็อกนี้ต้องอยู่ก่อน visible เพราะปุ่ม "Item Program" ใช้ programRows กรองแถว)
  const progIdx = useMemo(() => {
    const cols = grid ? grid.columns : []
    const tp = cols.indexOf('TEAM_PROGRAM')
    return {
      item: grid ? cols.indexOf('ITEM_CODE') : -1,
      team: tp >= 0 ? tp : (grid ? cols.indexOf('TEAM') : -1),
    }
  }, [grid?.columns])

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

  // จำนวนแถว Program ที่ผ่านตัวกรองอื่น ๆ อยู่แล้ว — โชว์บนปุ่มให้รู้ว่ากดแล้วจะเหลือกี่แถว
  const progCount = useMemo(
    () => baseRows.filter(({ idx }) => programRows[idx]).length, [baseRows, programRows])

  // ── Item Core (คอลัมน์ IS_CORE_ITEM ในไฟล์แผน) → ปุ่มกรองคู่กับ Item Program ──────
  // ธงนี้ Planning เขียนมาให้แล้วต่อแถว (ค่า "CORE ITEM") จึงไม่ต้องถาม backend ซ้ำ
  // เกณฑ์เดียวกับ Planning.py (_is_core_row): ขึ้นต้นด้วย CORE — ไม่เช็คแค่ "ไม่ว่าง"
  // เพราะช่องว่างของ pandas ออกมาเป็น "nan" ได้ ซึ่งจะถูกนับเป็น core ไปทั้งกระดาน
  const coreCi = useMemo(
    () => (grid ? grid.columns.indexOf('IS_CORE_ITEM') : -1), [grid?.columns])

  // { idx แถว → true } ของแถว core — คิดที่เดียวแล้วใช้ทั้งตัวนับบนปุ่มและการกรอง
  const coreRows = useMemo(() => {
    const m = {}
    if (!grid || coreCi < 0) return m
    grid.rows.forEach((row, idx) => {
      if (norm(row[coreCi]).trim().toUpperCase().startsWith('CORE')) m[idx] = true
    })
    return m
  }, [grid, coreCi])

  const coreCount = useMemo(
    () => baseRows.filter(({ idx }) => coreRows[idx]).length, [baseRows, coreRows])

  // ชีทอื่น (เช่น SETUP_TRACKING) ไม่มีคอลัมน์ IS_CORE_ITEM → ปุ่มหายไปแต่สถานะยังค้าง
  // ถ้าปล่อยให้กรองต่อ ตารางจะว่างทั้งที่ไม่มีปุ่มให้กดปิด — กลับมาชีทแผนแล้วตัวกรองยังอยู่เหมือนเดิม
  const coreOn = coreOnly && coreCi >= 0

  // ITEM_CODE ของงาน core ในแผน — ใช้กรอง overlay ประวัติบน Gantt (บล็อกประวัติมีแต่ item)
  const coreItemCodes = useMemo(() => {
    const s = new Set()
    if (!grid || itemCi < 0) return s
    grid.rows.forEach((row, idx) => { if (coreRows[idx]) s.add(upcode(norm(row[itemCi]))) })
    return s
  }, [grid, coreRows, itemCi])

  // ── Hold (คอลัมน์ PLAN_SOURCE = HOLD_COLOR / HOLD_TARGET) → ปุ่มกรองใบที่ 3 ────────
  // ปุ่มเดียวคุมทั้ง "Hold รอสี" และ "Hold รอ target" (Planning.py: HOLD_SOURCES) — ฝั่ง user
  // มองเป็นเรื่องเดียวกันคือ "สัปดาห์ที่กันเครื่องไว้แต่ไม่ได้ทอ" ต่างแค่เหตุผลบนป้าย
  // เทียบด้วย startsWith('HOLD_') เพื่อให้ค่า HOLD_* ที่อาจเพิ่มภายหลังเข้าปุ่มนี้เองโดยไม่ต้องแก้
  const srcCi = useMemo(
    () => (grid ? grid.columns.indexOf('PLAN_SOURCE') : -1), [grid?.columns])

  // ITEM_CODE ของ item ที่ "มีสัปดาห์ Hold อยู่ที่ไหนสักแห่งในแผน" + จำนวนแถว Hold ทั้งหมด
  // (ไม่จำกัดแค่แถวที่ผ่านตัวกรองอื่น เพราะจุดประสงค์คือหา item ที่ถูกกันเครื่องไว้ให้เจอ)
  const holdData = useMemo(() => {
    const codes = new Set()
    let spans = 0
    if (!grid || srcCi < 0) return { codes, spans }
    grid.rows.forEach(row => {
      if (!norm(row[srcCi]).trim().toUpperCase().startsWith('HOLD_')) return
      spans += 1
      if (itemCi >= 0) codes.add(upcode(norm(row[itemCi])))
    })
    return { codes, spans }
  }, [grid, srcCi, itemCi])

  // { idx แถว → true } = "ทุกแถวของ item ที่มี hold" (ไม่ใช่เฉพาะแถว hold) — user เลือกแบบนี้
  // เพื่อให้เห็นบริบทว่า item นั้นทอถึงสัปดาห์ไหนแล้วค้างรออะไร ไม่ใช่บล็อกลายทางลอย ๆ
  // ไฟล์แผนที่ไม่มีคอลัมน์ ITEM_CODE → ถอยไปกรองเฉพาะแถว hold (ยังดีกว่าไม่กรองอะไรเลย)
  const holdRows = useMemo(() => {
    const m = {}
    if (!grid || srcCi < 0) return m
    grid.rows.forEach((row, idx) => {
      if (itemCi >= 0) {
        if (holdData.codes.has(upcode(norm(row[itemCi])))) m[idx] = true
      } else if (norm(row[srcCi]).trim().toUpperCase().startsWith('HOLD_')) m[idx] = true
    })
    return m
  }, [grid, srcCi, itemCi, holdData])

  // ตัวเลขบนปุ่ม = จำนวน "item" ที่มี hold (ตัวกรองนี้ทำงานระดับ item ไม่ใช่ระดับแถว)
  const holdCount = itemCi >= 0 ? holdData.codes.size : holdData.spans

  // ชีทอื่นไม่มีคอลัมน์ PLAN_SOURCE → ปุ่มหาย แต่สถานะค้าง (เหตุผลเดียวกับ coreOn)
  const holdOn = holdOnly && srcCi >= 0

  // แถวในตาราง/Gantt = ฐานข้างบน แล้วกรองด้วย item ที่ติ๊กเลือกไว้อีกชั้น
  // (ติ๊กเฉพาะ item ที่มีแต่ในแผนเก่า → ตารางว่างเป็นเรื่องปกติ ของจริงอยู่บนบล็อก Gantt)
  // ปุ่ม "Item Program" / "Item Core" เป็นชั้นสุดท้าย และรวมกันแบบ OR: กดคู่กัน = เห็นงาน
  // ทั้งสองกลุ่มพร้อมกัน (ใช้ AND จะเหลือเฉพาะ item ที่เป็นทั้งสองอย่าง ซึ่งมีน้อยมากจนจอว่าง)
  const visible = useMemo(() => {
    let rows = baseRows
    if (itemPick && itemCi >= 0) rows = rows.filter(({ row }) => itemPick.has(upcode(norm(row[itemCi]))))
    if (progOnly || coreOn || holdOn) {
      rows = rows.filter(({ idx }) => (progOnly && programRows[idx]) || (coreOn && coreRows[idx])
        || (holdOn && holdRows[idx]))
    }
    return rows
  }, [baseRows, itemPick, itemCi, progOnly, programRows, coreOn, coreRows, holdOn, holdRows])

  // แถวแผน "ทั้งกระดาน" (ไม่ผ่านคำค้น/ตัวกรองใด ๆ) — Gantt ใช้คิดโควตา job ต่อสัปดาห์และ
  // เครื่องที่แผนจองไปแล้ว ตัวเลขสองตัวนี้ต้องเป็นภาพของทั้งแผนเสมอ ไม่งั้นพอค้นหา item เดียว
  // แถบ job จะว่างและเครื่องจะดูเหลือเพียบ — ตรงข้ามกับสิ่งที่ต้องใช้ตัดสินว่าลากไปแล้วเกินไหม
  // (grid.rows เป็นชุดเดียวกับที่ลาก/แก้สดอยู่ ตัวเลขจึงขยับตามทันทีเหมือนเดิม)
  const allRowsIdx = useMemo(
    () => (grid ? grid.rows.map((row, idx) => ({ row, idx })) : []), [grid?.rows])

  // ลำดับที่โชว์ในตารางแผน = แกนเวลาจริง (แผนปีหน้าต่อท้ายแผนปีนี้)
  // แยกจาก visible เพราะ Gantt ยังรับชุดลำดับเดิม — บล็อกบน Gantt วางตามสัปดาห์ของตัวเองอยู่แล้ว
  const visibleSorted = useMemo(
    () => (grid ? sortRowsByTimeline(visible, grid.columns) : visible), [visible, grid])

  // ══ งานจ้างทอ (S9) — อยู่ในตาราง Gantt เดียวกับงานในบ้าน ═══════════════════
  // แถวจ้างทอถูกเขียนเป็น MC_GROUP=COMKN / เกจ C0 (กันปนพูลเครื่องในบ้าน) → ถ้าปล่อยตามนั้น
  // ทุกพูลของผู้รับจ้างจะยุบเป็นแถวเดียวชื่อ COMKN อ่านไม่ออกว่าเครื่องพูลไหนเต็ม
  // ทางแก้: สลับ "หัวแถว" ของเฉพาะแถวจ้างทอให้เป็นพูลจริง แล้วส่งรวมไปกับแถวอื่นในตารางเดิม
  //   Category = S9-<พูล> (ขึ้นต้น S9- ให้แยกออกจาก CAT ในบ้าน และคีย์เครื่องว่างไม่ชนกัน)
  //   Guage    = เกจของพูล
  //   Machine  = "<พูล>·<เกจ>" — ต้องผูกเกจไว้ในชื่อเครื่อง เพราะพูลเดียวกันมีหลายเกจ
  //              (RAP 22/24/28) และตอนลาก PlanGantt ส่งกลับมาแค่ค่าคอลัมน์ Machine
  // idx ของแต่ละ entry ยังชี้แถวจริงใน grid.rows → ลาก/แก้/ล็อกได้เหมือนแถวอื่นทุกอย่าง
  const s9Ci = useMemo(() => {
    if (!grid) return null
    const f = (n) => grid.columns.indexOf(n)
    return {
      flag: f('S9_ROUTING'), s9mc: f('S9_MC'), s9g: f('S9_GAUGE'),
      mcg: f('MC_GROUP'), cat: f('CAT'), gauge: f('MC_GUAGE'),
      week: f('PLAN_WEEK'), qty: f('PRODUCE_QTY'), item: f('ITEM_CODE'),
    }
  }, [grid?.columns])
  const hasS9Cols = !!s9Ci && s9Ci.flag >= 0 && s9Ci.s9mc >= 0
  // ธง S9_ROUTING ในไฟล์แผนออกมาเป็น 1.0 / 0.0 (ทั้งคอลัมน์เป็นตัวเลข ไม่ใช่ TRUE/ว่าง)
  // → ห้ามเช็ค truthy ของ string เฉย ๆ ("0.0" เป็น true) และห้ามเทียบ '1' ตรง ๆ
  const isS9Row = (row) => {
    if (!s9Ci) return false
    if (s9Ci.flag >= 0) {
      const v = String(norm(row[s9Ci.flag])).trim().toUpperCase()
      if (v === 'TRUE' || v === 'YES') return true
      const n = Number(v)
      if (Number.isFinite(n) && n > 0) return true
    }
    return s9Ci.mcg >= 0 && String(norm(row[s9Ci.mcg])).trim().toUpperCase() === 'COMKN'
  }
  const s9CatOf = (mc) => `${S9_CAT_PREFIX}${mc}`
  const s9RowKey = (mc, g) => `${mc}${S9_MC_SEP}${g}`
  // idx ของแถวจ้างทอ — Gantt ใช้ระบายบล็อกเป็นสีน้ำเงินและเปิดปุ่ม "ดึงกลับมาทอในบ้าน"
  // ต้องประกาศก่อน s9HostByIdx/toS9View ที่อ้างถึงมัน (const มี TDZ — ใช้ก่อนประกาศไม่ได้)
  const s9Idx = useMemo(() => {
    if (!hasS9Cols || !grid) return null
    const s = new Set()
    grid.rows.forEach((row, i) => { if (isS9Row(row)) s.add(i) })
    return s.size ? s : null
  }, [grid?.rows, hasS9Cols, s9Ci])
  // ── เจ้าบ้านของงานจ้างทอ (โหมดยุบแถว) ────────────────────────────────────
  // งานที่แบ่งออกไปจ้างทอเป็นผ้าตัวเดียวกับที่ยังทออยู่ในบ้าน → ควรอ่านได้ในแถวเดียวกัน
  // เกณฑ์ = ITEM + เกจ ตรงกัน (เกจของแถวจ้างทอคือ S9_GAUGE = เกจเครื่องผู้รับจ้าง)
  // เจ้าบ้าน = กลุ่มเครื่องในบ้านที่ทอผ้าตัวนั้นด้วยยอดรวมมากที่สุด (ผ้าตัวเดียวอาจอยู่หลายเครื่อง)
  // ผ้าที่ไม่มีงานในบ้านเลย (S9 Only) จะหาเจ้าบ้านไม่เจอ → คงแถวพูลผู้รับจ้างของตัวเองไว้เหมือนเดิม
  // คืน Map: idx แถวจ้างทอ → { cat, gauge, mcg } ของแถวเจ้าบ้าน
  const s9HostByIdx = useMemo(() => {
    if (!mergeS9 || !hasS9Cols || !grid || !s9Idx || s9Ci.item < 0 || s9Ci.mcg < 0) return null
    const byKey = new Map()      // "ITEM|เกจ" → Map("CAT|เครื่อง" → {cat,gauge,mcg,qty})
    grid.rows.forEach((row, i) => {
      if (s9Idx.has(i)) return
      const item = upcode(norm(row[s9Ci.item])); if (!item) return
      const mcg = norm(row[s9Ci.mcg]); if (!mcg) return
      const g = s9Ci.gauge >= 0 ? gnum(row[s9Ci.gauge]) : ''
      const cat = s9Ci.cat >= 0 ? norm(row[s9Ci.cat]) : ''
      const k = `${item}|${g}`
      let sub = byKey.get(k); if (!sub) { sub = new Map(); byKey.set(k, sub) }
      const sk = `${cat}|${mcg}`
      const cur = sub.get(sk) || { cat, gauge: g, mcg, qty: 0 }
      cur.qty += Number(norm(row[s9Ci.qty])) || 0
      sub.set(sk, cur)
    })
    const best = new Map()
    for (const [k, sub] of byKey) {
      let top = null
      for (const e of sub.values()) if (!top || e.qty > top.qty) top = e
      if (top) best.set(k, top)
    }
    const out = new Map()
    for (const i of s9Idx) {
      const row = grid.rows[i]
      const item = upcode(norm(row[s9Ci.item])); if (!item) continue
      const host = best.get(`${item}|${gnum(row[s9Ci.s9g])}`)
      if (host) out.set(i, host)
    }
    return out.size ? out : null
  }, [grid?.rows, s9Idx, hasS9Cols, s9Ci, mergeS9])
  // แถวจ้างทอที่ถูกยุบเข้าแถวในบ้านแล้ว — Gantt ต้องไม่เอาไปหักเครื่องของพูลในบ้าน
  // (หัวแถวเป็นเครื่องในบ้านแล้ว ถ้าไม่บอกไว้ ตัวเลข "ว่าง" จะหายไปเท่าเครื่องที่ทออยู่ข้างนอก)
  const mergedS9Idx = useMemo(
    () => (s9HostByIdx ? new Set(s9HostByIdx.keys()) : null), [s9HostByIdx])
  const toS9View = (entry) => {
    if (!hasS9Cols || !isS9Row(entry.row)) return entry
    const r = entry.row.slice()
    // ยุบเข้าแถวเจ้าบ้าน → ใช้หัวแถวของเครื่องในบ้าน (rowKey ของ Gantt จึงตรงกัน = แถวเดียวกัน)
    const host = s9HostByIdx && s9HostByIdx.get(entry.idx)
    if (host) {
      if (s9Ci.cat >= 0) r[s9Ci.cat] = host.cat
      if (s9Ci.gauge >= 0) r[s9Ci.gauge] = host.gauge
      if (s9Ci.mcg >= 0) r[s9Ci.mcg] = host.mcg
      return { ...entry, row: r }
    }
    const mc = (norm(entry.row[s9Ci.s9mc]) || 'COMKN').toUpperCase()
    const g = gnum(entry.row[s9Ci.s9g])
    if (s9Ci.cat >= 0) r[s9Ci.cat] = s9CatOf(mc)
    if (s9Ci.gauge >= 0) r[s9Ci.gauge] = g
    if (s9Ci.mcg >= 0) r[s9Ci.mcg] = s9RowKey(mc, g)
    return { ...entry, row: r }
  }
  // แถวที่ส่งเข้า Gantt = ชุดเดียวกับเดิม เพียงแต่แถวจ้างทอถูกสลับหัวแถวเป็นพูลของผู้รับจ้าง
  const ganttVisible = useMemo(
    () => (hasS9Cols ? visible.map(toS9View) : visible), [visible, hasS9Cols, s9Ci, s9HostByIdx])
  const ganttAll = useMemo(
    () => (hasS9Cols ? allRowsIdx.map(toS9View) : allRowsIdx), [allRowsIdx, hasS9Cols, s9Ci, s9HostByIdx])
  // item ที่มีงานทั้งในบ้านและจ้างทอ — ติดป้ายบนบล็อกทั้งสองฝั่ง กันเข้าใจว่างานหายไปข้างเดียว
  // (การตัดสินจ้างทอเป็นรายใบสั่ง ผ้าตัวเดียวกันจึงอยู่ได้ทั้งสองแบบ)
  const bothSideItems = useMemo(() => {
    if (!hasS9Cols || !grid || s9Ci.item < 0 || !s9Idx) return null
    const out = new Set(), home = new Set()
    grid.rows.forEach((row, i) => {
      const k = upcode(norm(row[s9Ci.item]))
      if (!k) return
      ;(s9Idx.has(i) ? out : home).add(k)
    })
    const both = new Set()
    for (const k of out) if (home.has(k)) both.add(k)
    return both.size ? both : null
  }, [grid?.rows, s9Idx, hasS9Cols, s9Ci])
  // เพดานเครื่องผู้รับจ้าง ผสมเข้ากับ ava ของพูลในบ้าน (คนละคีย์ ไม่ชนกัน เพราะขึ้นต้น "S9-")
  // remain = total เต็ม เพราะ PlanGantt หักเครื่องที่แถวแผนจองเองอยู่แล้ว
  const avaWithS9 = useMemo(() => {
    const keys = Object.keys(s9Pool || {})
    if (!keys.length || !hasS9Cols || !s9Idx) return ava
    const wks = new Set(Object.keys(ava || {}))
    if (grid) for (const i of s9Idx) {
      const w = rowKeyOf(grid.rows[i] || []); if (w !== '') wks.add(w)
    }
    const out = {}
    for (const w of wks) {
      const slot = { ...(ava?.[w] || {}) }
      for (const k of keys) {
        const [mc, g] = k.split('|')
        const p = s9Pool[k] || {}
        // คีย์ต้องเป็นรูปเดียวกับที่ PlanGantt คิดจากหัวแถว (poolKeyOf = "CAT|เกจ")
        slot[`${s9CatOf(mc)}|${g}`] = { total: p.total || 0, remain: p.total || 0, used: 0 }
      }
      out[w] = slot
    }
    return out
  }, [s9Pool, ava, grid?.rows, s9Idx, hasS9Cols, s9Ci])

  // ITEM_CODE ที่กำลังทำแผนวันนี้ (จากแผนปัจจุบัน) ที่ "มีประวัติใน booking" — ใช้ทำ dropdown โหมด plan
  // นับบล็อกจากทะเบียนประวัติ (histItems) ด้วย ไม่งั้น item ที่เคยทอเมื่อ 3 เดือนก่อนแต่ไม่มีใน
  // ไฟล์ booking ปัจจุบันแล้ว จะไม่อยู่ในลิสต์ → bookingPick ไม่มีมัน → บล็อกที่ดึงมาถูกกรองทิ้งเงียบ ๆ
  const bookingHistCodes = useMemo(() => {
    if (!grid) return []
    const ici = grid.columns.indexOf('ITEM_CODE')
    if (ici < 0) return []
    const planSet = new Set(grid.rows.map(r => norm(r[ici]).toUpperCase()).filter(Boolean))
    const bk = new Set(bookingItems.map(b => String(b.item).toUpperCase()))
    for (const b of histItems) bk.add(String(b.item).toUpperCase())
    return [...planSet].filter(c => bk.has(c)).sort()
  }, [grid, bookingItems, histItems])

  // รายการที่ผ่านคำค้น (ว่าง = ทั้งหมด) — ใช้ทั้งรายการใน dropdown และเป็นขอบเขตของ overlay บน Gantt
  const bkShownCodes = useMemo(() => {
    const q = bkSearch.trim().toUpperCase()
    return q ? bookingHistCodes.filter(c => c.toUpperCase().includes(q)) : bookingHistCodes
  }, [bookingHistCodes, bkSearch])

  // ชุด ITEM_CODE ที่จะโชว์ประวัติในโหมด plan = ตัวที่ผ่านคำค้น − ตัวที่ user ติ๊กออก
  const bookingPick = useMemo(
    () => new Set(bkShownCodes.filter(c => !bookingUnpick.has(c))),
    [bkShownCodes, bookingUnpick])

  // ── overlay แผนเก่าบน Gantt: รวมผลของปุ่ม History + คำค้นหลัก + ตัวกรอง Item ──────
  // คำค้น/ตัวกรอง Item "ชนะ" ปุ่ม History: เลือก/ค้นเจอ item ที่มีแต่ในแผนเก่า → โชว์ให้เลย
  // แม้ History = ปิด (ไม่งั้นเลือกแล้วหน้าจอว่าง ทั้งที่ระบบรู้ว่ามี) พร้อมปลดเพดานย้อนหลัง
  // 5 สัปดาห์ เพราะ item ที่รันไปนานแล้วอยู่นอกกรอบนั้น จะไม่โผล่ทั้งที่มีข้อมูล
  // mode ยังคงค่าเดิมของปุ่ม (all = ยังกาง MC group ครบตาม MasterMC) — เปลี่ยนแค่ชุด item ที่ยอมให้ผ่าน
  const bkView = useMemo(() => {
    const OFF = { mode: 'off', pick: null, allWeeks: false }
    if (!bookingItems.length && !histItems.length) return OFF
    const s = search.trim().toLowerCase()
    const searching = !!s
    // ปุ่ม "Item Program"/"Item Core" กรอง overlay ประวัติด้วย ไม่งั้นกดแล้วบล็อกจาง ๆ ของ item
    // อื่นยังเต็มจอ
    // ⚠ บล็อกประวัติมีแต่ ITEM ไม่มีทีมให้เทียบ → ตรงนี้จึงเทียบด้วย item ในชีท Program ล้วน
    //    (เป็น superset ของแถวแผนที่ ITEM+ทีม ตรง จึงไม่มีทางตัดงาน program ทิ้งเกินจริง)
    // กดสองปุ่มพร้อมกัน = รวมชุด item ของทั้งสองกลุ่ม (OR) ให้ตรงกับการกรองแถวในตาราง
    const progSet = (progOnly || coreOn || holdOn)
      ? new Set([...(progOnly ? Object.keys(program) : []), ...(coreOn ? coreItemCodes : []),
                 ...(holdOn ? holdData.codes : [])])
      : null
    if (!searching && !itemPick && !progSet) {
      return { mode: bookingMode, pick: bookingMode === 'plan' ? bookingPick : null, allWeeks: false }
    }
    // ปุ่ม Program/Core "กรอง" ประวัติเท่านั้น ไม่ได้สั่งเปิด — History = ปิด แล้วกดปุ่มนี้ ต้องยังปิดอยู่
    if (!searching && !itemPick && bookingMode === 'off') return OFF
    // ดรอปดาวน์ "เลือก item" มีเฉพาะ item ที่อยู่ในแผนวันนี้ ∩ booking → การติ๊กออกของ user
    // จึงคุมได้แค่ item ในลิสต์นั้น ; item ที่มีแต่ในแผนเก่าอยู่นอกลิสต์ ถ้าเอา bookingPick
    // มาตัดตรง ๆ จะโดนตัดทิ้งทั้งหมด = ค้น item เก่าไม่เจอตอนตั้ง History เป็น "วางแผนวันนี้"
    const histSet = new Set(bookingHistCodes)
    const pick = new Set()
    // ค้นหา/กรอง item ต้องกวาดบล็อกประวัติที่ดึงมาด้วย — ไม่งั้นค้นชื่อ item ที่ทอไปเมื่อ 20
    // สัปดาห์ก่อนแล้วไม่เจอ ทั้งที่บล็อกอยู่บนจออยู่แล้ว
    for (const b of (histItems.length ? [...bookingItems, ...histItems] : bookingItems)) {
      const c = upcode(b.item)
      if (!c) continue
      if (itemPick && !itemPick.has(c)) continue
      if (progSet && !progSet.has(c)) continue
      if (searching && !bkMatch(b, s)) continue
      if (bookingMode === 'plan' && histSet.has(c) && !bookingPick.has(c)) continue
      pick.add(c)
    }
    if (!pick.size) return OFF
    // ปลดเพดานย้อนหลัง 5 สัปดาห์เฉพาะตอนค้นหา/เลือก item เอง — กด Program เฉย ๆ ยังคงกรอบเดิม
    return { mode: bookingMode === 'all' ? 'all' : 'plan', pick, allWeeks: searching || !!itemPick }
  }, [bookingItems, histItems, bookingMode, bookingPick, bookingHistCodes, search, itemPick,
      progOnly, program, coreOn, coreItemCodes, holdOn, holdData])

  // คอลัมน์ตัวเลข (sample 200 แถวแรกพอ — ตารางนี้แก้ไขได้ คำนวณใหม่ทุกครั้งที่พิมพ์)
  const numCols = useMemo(() => numericCols(grid, 200), [grid])

  // SUB_COLOR: ค่าเดียวกัน = สีพื้นเดียวกัน — ไล่ hue ไปเรื่อย ๆ ตามลำดับที่เจอในชีท
  // (golden angle 137.5° ทำให้สีที่อยู่ติดกันในลิสต์ต่างกันชัด; อ่อนพอให้ตัวหนังสือดำยังอ่านออก)
  const subColor = useMemo(() => {
    const ci = grid ? grid.columns.indexOf('SUB_COLOR') : -1
    if (ci < 0) return null
    const map = new Map()
    for (const row of grid.rows) {
      const v = norm(row[ci]).trim()
      if (!v || map.has(v)) continue
      map.set(v, `hsl(${(map.size * 137.5) % 360}, 70%, 88%)`)
    }
    return { ci, map }
  }, [grid])

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
  const hasFilter = search.trim() || Object.keys(filters).length || !!itemPick || progOnly || coreOn || holdOn
  const isRunning = !!runStatus.running
  const runLabel = isRunning ? `กำลังรัน: ${runStatus.label || '-'}` : 'ยังไม่มีงานกำลังรัน'

  // scope = ตารางที่กดปุ่ม ▾ มา ('main' = ตารางหน้าหลัก/Gantt, 'modal' = ตารางเล็กใน modal)
  function openColMenu(e, ci, scope = 'main') {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setOpenCol({ ci, scope, anchor: { top: r.top, bottom: r.bottom, left: r.left, right: r.right } })
  }
  function applyFilter(ci, set, scope = 'main') {
    const upd = f => {
      const n = { ...f }
      if (set == null) delete n[ci]
      else n[ci] = set
      return n
    }
    if (scope === 'modal') setModalFilters(upd)
    else setFilters(upd)
    setOpenCol(null)
  }

  // ตารางแผน (ใช้ซ้ำทั้งท้ายหน้า + ใน modal ที่เด้งตอนคลิกบล็อก Gantt)
  // highlight = idx แถวที่จะไฮไลต์/ผูก ref ไว้เลื่อนหา (เฉพาะใน modal)
  // rowsList = ชุดแถวที่จะโชว์ (default = ทั้งหมดที่ผ่านตัวกรอง; modal ส่งเฉพาะแถวของ item ที่คลิก)
  // allowDelete = false → ตัดคอลัมน์ปุ่มลบทิ้งทั้งคอลัมน์ (ชีทรายงานอย่าง UNPLANNED ห้ามลบแถว)
  // scope = ตัวกรองหัวคอลัมน์ของตารางนี้ผูกกับ state ชุดไหน (modal มีตัวกรองของตัวเอง)
  // ตาราง popup ของ item ซ่อนคอลัมน์ใน PLAN_HIDDEN_COLS + ใช้ชื่อที่แสดงจาก PLAN_COL_LABEL
  // (ตารางหลักยังโชว์ครบด้วยชื่อจริง) — ข้ามแค่ตอนวาด ci ยังเป็นลำดับจริงของ grid.columns
  // ตัวกรอง/การแก้ช่อง/ความกว้างคอลัมน์จึงอ้างคอลัมน์เดิมถูกตัว
  const renderGrid = (highlight = null, rowsList = visibleSorted, allowDelete = true, scope = 'main') => {
    const shown = grid.columns.map((_, ci) => ci)
      .filter(ci => scope !== 'modal' || !PLAN_HIDDEN_COLS.has(grid.columns[ci]))
    const hiddenW = grid.columns.reduce((s, c, ci) => s + (shown.includes(ci) ? 0 : (colW[ci] || 120)), 0)
    const tblW = totalW - hiddenW - (allowDelete ? 0 : ROWNUM_W)
    const label = c => (scope === 'modal' && PLAN_COL_LABEL[c]) || c
    return (
    <table className="grid" style={{ tableLayout: 'fixed', width: tblW }}>
      <colgroup>
        <col style={{ width: ROWNUM_W }} />
        {shown.map(ci => (
          <col key={ci} style={{ width: colW[ci] || 120 }} />
        ))}
        {allowDelete && <col style={{ width: ROWNUM_W }} />}
      </colgroup>
      <thead>
        <tr>
          <th className="rownum">#</th>
          {shown.map(ci => { const c = grid.columns[ci]; return (
            <th key={ci} className={ci === 0 ? 'frozen' : undefined}>
              <div className="thcell">
                <span className="thlabel" title={c}>{label(c)}</span>
                <button
                  className={'funnel' + ((scope === 'modal' ? modalFilters : filters)[ci] ? ' on' : '')}
                  title={scope === 'modal' ? 'กรองคอลัมน์นี้ (เฉพาะตารางนี้)' : 'กรองคอลัมน์นี้'}
                  onClick={e => openColMenu(e, ci, scope)}>▾</button>
              </div>
              <span className="colresize" onMouseDown={e => startResize(e, ci)} />
            </th>
          ) })}
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
            {shown.filter(ci => ci < row.length).map(ci => {
              const cell = row[ci]
              const key = `${idx}:${ci}`
              // ช่อง SUB_COLOR ทาสีตามค่า (ยกเว้นแถวที่ถูกไฮไลต์ใน modal — ปล่อยให้เห็นสีไฮไลต์)
              const subBg = (subColor && ci === subColor.ci && highlight !== idx)
                ? subColor.map.get(norm(cell).trim())
                : null
              return (
                <td key={ci} className={(ci === 0 ? 'frozen' : '') + (numCols.has(ci) ? ' num' : '')}
                  style={subBg ? { background: subBg } : undefined}>
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
            <td colSpan={shown.length + (allowDelete ? 1 : 0)} className="hint">ไม่มีแถวตรงตัวกรอง</td>
          </tr>
        )}
      </tbody>
    </table>
    )
  }

  return (
    <div className="knitplan">
      <div className="editbar plan-head">
        <h2>แผนผลิต {autosave.dirty && <span className="dot" title="กำลังจะบันทึกให้เอง">●</span>}</h2>
        <div className="actions">
          {sheetOptions.length > 1 && (
            <select value={grid.sheet} onChange={e => changeSheet(e.target.value)}>
              {sheetOptions.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          )}
          <button className="primary" onClick={runPlan} disabled={isRunning}>▶ รันแผนใหม่</button>
          <button className="outsource-btn" onClick={() => setShowOutsource(true)}>🧵 จ้างทอ (AI)</button>
          <button className="cylinder-btn" onClick={() => setShowCylinder(true)}>🔩 Change Cylinder</button>
          <button className="pivot-btn" onClick={() => setShowPivot(true)}
            title="ตาราง Pivot ทุก item แบบ Excel — แถวเลือกฟิลด์เองได้ คอลัมน์เป็นสัปดาห์">📊 Pivot</button>
          {/* เฉพาะแผน replan (ไฟล์รุ่นใหม่ = ชีท PLAN, รุ่นเก่า = ชีท REPLAN) — แผนเดียวที่มี
              "booking เดิม" ให้เทียบ (แผน SC ล้วนของไฟล์รุ่นเก่าไม่มีของเดิมให้บอกว่าเลื่อนจากไหน) */}
          {grid?.replan && (
            <button className="mail-btn" onClick={() => setShowMail(true)}
              title="สรุปงานที่เลื่อนสัปดาห์ / ยอดเปลี่ยน / จบเร็ว-ช้ากว่าเดิม พร้อมเหตุผล แล้วส่งอีเมลแจ้ง">
              📧 แจ้งเมลแผนเปลี่ยน</button>
          )}
          <SaveStatus status={autosave.status} error={autosave.error} at={savedAt}
            onRetry={autosave.flush} />
          {meta?.exists && <a className="dl" href={api.planDownloadUrl(meta.mtime)}>⬇ ดาวน์โหลด Excel</a>}
          <button onClick={refresh}>รีเฟรช</button>
          {isRunning && <button className="stopbtn" onClick={stopRun}>⛔ หยุดรัน</button>}
          <span className={'badge ' + (isRunning ? 'run' : 'idle')}>{runLabel}</span>
          {isRunning && runStatus.progress != null && <small className="run-hint">ความคืบหน้า {runStatus.progress}%</small>}
          {/* เวลาที่ "รันแผนรอบนี้" ไม่ใช่เวลาที่ไฟล์ถูกเขียนล่าสุด — แผนใบหนึ่งคือผลของการรัน
              รอบหนึ่ง ส่วนการลาก/แก้คือการแก้ของรอบนั้น เลขตรงนี้จึงต้องนิ่งจนกว่าจะรันใหม่
              (run_at อ่านจากชื่อไฟล์ ; ไฟล์รุ่นเก่าที่ชื่อไม่มีเวลา ถอยไปใช้ mtime เหมือนเดิม) */}
          {meta?.exists && (meta.run_at || meta.mtime) &&
            <small className="run-hint">แผนล่าสุด: {fmtPlanTime(meta.run_at || meta.mtime)}</small>}
        </div>
      </div>

      {/* มีแผนฉบับใหม่จากคนอื่นรออยู่ แต่เรายังมีของค้างไม่ได้เซฟ (หรือกำลังลากอยู่) —
          ทับให้เลยไม่ได้ ของที่ทำค้างไว้จะหาย จึงค้างปุ่มไว้ให้ตัดสินใจเอง
          เซฟของตัวเองสำเร็จเมื่อไหร่ ระบบจะแปะให้เองโดยไม่ต้องกด */}
      {remoteWait && (
        <div className="msg warn">
          📥 {remoteWait.by || 'มีคน'} แก้แผนไว้อีกฉบับ
          {remoteWait.at ? ` (${remoteWait.at.slice(11, 16)} น.)` : ''}
          {' '}— ของที่พี่แก้ค้างไว้ยังไม่ได้บันทึก จึงยังไม่เอามาทับให้
          <button className="linkbtn" onClick={() => {
            setRemoteWait(null)
            // ยอมทิ้งของที่ค้าง = ไม่ต้องเซฟทับของเขาอีก ; ต้องล้าง ref ด้วยมือ เพราะ state
            // ยังไม่ commit ตอน loadSheetQuiet อ่านค่า (ไม่งั้นมันจะเด้งกลับมาขึ้นปุ่มนี้อีก)
            dirtyRef.current = false
            setDirty(false)
            loadSheetQuiet(remoteWait.sheet)
          }}>เอาของใหม่มาแสดง (ทิ้งที่แก้ค้างไว้)</button>
        </div>
      )}

      {/* ยอดที่ต้องวาง vs ยอดวางไม่ตรง — เตือนค้างไว้ ไม่ขัดจังหวะการจัดแผน กดดูรายชื่อได้
          ใบที่เคยวางแผนไปแล้วบางส่วนจะบอกยอดสั่งทั้งใบต่อท้าย เพื่อไม่ให้สับสนว่าทำไมยอดไม่เท่าใบสั่ง */}
      {balanceWarn.length > 0 && (
        <div className="msg warn balance-warn">
          ⚠ มี {balanceWarn.length} ออร์เดอร์ที่วางไม่ตรงยอดที่ต้องวาง
          <button className="linkbtn" onClick={() => setShowBalance(v => !v)}>
            {showBalance ? 'ซ่อนรายการ' : 'ดูรายการ'}
          </button>
          {showBalance && (
            <ul className="balance-list">
              {balanceWarn.slice(0, 30).map((o, i) => (
                <li key={i}>
                  SC {o.sc || '-'}: ต้องวาง {o.ordered.toLocaleString()} · วาง {o.placed.toLocaleString()}
                  {' '}<b>{o.diff > 0 ? `ขาด ${o.diff.toLocaleString()}` : `เกิน ${(-o.diff).toLocaleString()}`}</b>
                  {o.full > o.ordered + QTY_TOL && (
                    <span className="balance-note">
                      {' '}(สั่งทั้งใบ {o.full.toLocaleString()} · วางแผนไปแล้ว {(Math.round((o.full - o.ordered) * 100) / 100).toLocaleString()})
                    </span>
                  )}
                </li>
              ))}
              {balanceWarn.length > 30 && <li>…และอีก {balanceWarn.length - 30} ออร์เดอร์</li>}
            </ul>
          )}
        </div>
      )}

      {grid && (
        <div className="filterbar">
          {/* ช่องค้นหา = ตัวเปิดตัวกรอง Item แบบ Excel ในตัว — คลิกหรือพิมพ์แล้วลิสต์เด้งใต้ช่องเลย
              (ลิสต์รวม item ครบทั้งแผนวันนี้และแผนเก่า ติ๊กได้หลายตัว) */}
          <div className="searchwrap" ref={searchRef}>
            <input className="search"
              placeholder="🔍 ค้นหาทุกคอลัมน์ + item ในแผนเก่า..."
              title="พิมพ์เพื่อค้นทั้งแถวในแผนวันนี้และ item ในแผนเก่า — ลิสต์ item จะเด้งขึ้นมาให้ติ๊กเลือกได้หลายตัว"
              value={search}
              onFocus={openItemPop}
              onClick={openItemPop}
              onChange={e => { setSearch(e.target.value); openItemPop() }} />
            {itemPick && (
              <span className="search-pick" title={`กรอง item อยู่ ${itemPick.size} ตัว จากทั้งหมด ${itemData.domain.size}`}>
                {itemPick.size}/{itemData.domain.size}
              </span>
            )}
            {/* ป็อปอัพตัวเดียวกับที่กด ▾ ที่หัวคอลัมน์ — ไม่ autofocus/ไม่คลุมจอ จะได้พิมพ์ต่อในช่องค้นหาได้ */}
            {itemPop && (
              <ColumnFilter
                available={itemData.available} domain={itemData.domain}
                selected={itemPick} anchor={itemPop}
                autoFocusSearch={false} backdrop={false} narrowed={!!search.trim()}
                onApply={s => { setItemPick(s); setItemPop(null) }}
                onClose={() => setItemPop(null)} />
            )}
          </div>
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
          {/* กรองเหลือเฉพาะ item+ทีม ที่อยู่ในชีท Program (ตัวหนังสือสีน้ำเงิน) — มีผลทั้งตาราง
              Gantt และ Pivot พร้อมกัน ; ไม่มีของให้กรองก็ไม่ต้องมีปุ่ม */}
          {Object.keys(program).length > 0 && (
            <button className={'progf' + (progOnly ? ' on' : '')}
              onClick={() => setProgOnly(v => !v)}
              title={'โชว์เฉพาะงานที่อยู่ในชีท Program ของ MasterMC (ITEM_CODE + ทีม ต้องตรงกัน)\n'
                + 'บล็อกประวัติบน Gantt ถูกกรองตามไปด้วย'}>
              ★ Item Program{progCount ? ` (${progCount})` : ''}
            </button>
          )}
          {/* กรองเหลือเฉพาะงาน Core Greige (คอลัมน์ IS_CORE_ITEM — ตัวหนังสือเขียวมะนาวบน Gantt)
              กดคู่กับปุ่ม Program ได้ = เห็นทั้งสองกลุ่มพร้อมกัน ; ไม่มีงาน core ก็ไม่ต้องมีปุ่ม */}
          {coreCount > 0 && (
            <button className={'coref' + (coreOnly ? ' on' : '')}
              onClick={() => setCoreOnly(v => !v)}
              title={'โชว์เฉพาะงาน Item Core Greige (คอลัมน์ IS_CORE_ITEM ของแผน)\n'
                + 'กดพร้อมปุ่ม Item Program ได้ — จะเห็นงานทั้งสองกลุ่มรวมกัน\n'
                + 'บล็อกประวัติบน Gantt ถูกกรองตามไปด้วย'}>
              ★ Item Core{coreCount ? ` (${coreCount})` : ''}
            </button>
          )}
          {/* กรองเหลือเฉพาะ item ที่มีสัปดาห์ Hold (รอสี + รอ target รวมปุ่มเดียว) — โชว์ "ทุกแถว"
              ของ item นั้น จะได้เห็นว่าทอถึงไหนแล้วค้างรออะไร ; กดคู่กับอีกสองปุ่มได้ (OR เหมือนกัน) */}
          {holdCount > 0 && (
            <button className={'holdf' + (holdOnly ? ' on' : '')}
              onClick={() => setHoldOnly(v => !v)}
              title={`โชว์เฉพาะ item ที่มีสัปดาห์ Hold — รวมทั้ง "Hold รอสี" และ "Hold รอ target"\n`
                + `ตอนนี้ ${holdCount} item · ${holdData.spans} สัปดาห์ที่กันเครื่องไว้แต่ไม่ได้ทอ\n`
                + 'เห็นทุกแถวของ item นั้น (ไม่ใช่แค่บล็อกลายทาง) จะได้รู้ว่าทอถึงไหนแล้วค้างรออะไร\n'
                + 'กดพร้อมปุ่ม Item Program / Item Core ได้ — จะเห็นงานทุกกลุ่มรวมกัน'}>
              ⏸ Hold{holdCount ? ` (${holdCount})` : ''}
            </button>
          )}
          {!!hasFilter && (
            <button className="clearf" onClick={() => { setSearch(''); setFilters({}); setItemPick(null); setProgOnly(false); setCoreOnly(false); setHoldOnly(false) }}>ล้างตัวกรองทั้งหมด</button>
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
            <span className="gbooking-toggle" title="overlay item จาก History แผนเดิมบน Gantt — ดูอย่างเดียว ลาก/แก้ไม่ได้">
              <span className="gbk-label">
                History แผนเดิม ({histFrom ? `ย้อนถึง W${histFrom.week}/${histFrom.year}` : 'ย้อนหลัง 5 week'})
              </span>
              {/* ไม่มีข้อมูล booking = กดปุ่มยังไงก็ไม่ขึ้น — บอกเหตุผลไปเลย ดีกว่าปล่อยให้กดแล้วเงียบ */}
              {bookingItems.length === 0 && (
                <span className="gbk-none"
                  title={'/api/plan/booking-items คืนค่าว่าง — ไม่พบไฟล์ booking_final_ready*.xlsx ที่ใช้ได้ในโฟลเดอร์ output ของเครื่องนี้\n'
                    + 'ต้องรัน AVA_MC (หรืออัปโหลด/รัน pipeline บนเครื่องที่เปิดเว็บอยู่) ก่อน History ถึงจะขึ้น'}>
                  ⚠ ไม่มีข้อมูล booking — ต้องรัน AVA_MC ก่อน
                </span>
              )}
              <button className={'gbk-btn' + (bookingMode === 'off' ? ' on' : '')}
                onClick={() => { setBookingMode('off'); setShowBkPick(false); setBkSearch('') }}
                title={'ซ่อนบล็อกประวัติ 📋 ทั้งหมด — ยกเว้นงานที่ล็อกแผนไว้ (🔐 ชีท Lock_Plan)\n'
                  + 'ที่ยังโชว์ต่อ เพราะ Replan จะคงงานนั้นไว้สัปดาห์/เครื่องเดิม (ดูอย่างเดียว)'}>ปิด</button>
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
              {/* งานที่ Replan ยกไปวางใหม่แล้ว — ปิดไว้เสมอ กดเปิดเมื่ออยากเทียบว่าเดิมอยู่ตรงไหน
                  โผล่เฉพาะตอนมีของให้เปิดดูจริง ไม่งั้นเป็นปุ่มที่กดแล้วไม่มีอะไรเกิดขึ้น */}
              {bookingItems.some(b => Number(b.replanned) > 0) && (
                <button className={'gbk-btn' + (showReplanned ? ' on' : '')}
                  onClick={() => setShowReplanned(s => !s)}
                  title={'บล็อกของงานที่ Replan ยกไปวางใหม่ที่อื่นแล้ว — ตำแหน่งเดิมไม่ใช่แผนจริง\n'
                    + 'ปกติซ่อนไว้ ; เปิดเมื่ออยากเทียบว่าเดิม booking วางไว้สัปดาห์ไหน (บล็อกจะจางลง + ติด ♻)'}>
                  ♻ งานที่ replan แล้ว
                </button>
              )}
              {/* ── ย้อนหลังลึกกว่า 5 week: เลือกปี+สัปดาห์แล้วดึงจากทะเบียนประวัติ ──
                  แยกเป็นกลุ่มของตัวเอง เพราะเป็นคนละแกนกับปุ่มโหมด (ปุ่ม = โชว์ item ไหน,
                  ตรงนี้ = ย้อนไปถึงเมื่อไร) และต้องกดดึงเอง — ข้อมูลก้อนใหญ่กว่า booking ปกติ */}
              {histRange && (histRange.db?.weeks?.length > 0 || histRange.weeks?.length > 0) && (() => {
                // ตัวเลือก = สัปดาห์ที่ "ดึงได้จาก DB" (ready = ดึงมาแล้ว โชว์ทันที / ไม่ ready = ต้องรอดึง)
                const opts = histRange.db?.weeks?.length ? histRange.db.weeks
                  : histRange.weeks.map(w => ({ ...w, ready: true }))
                const years = [...new Set(opts.map(o => o.year))].sort((a, b) => b - a)
                const y0 = histSel?.year ?? years[0]
                const wsOfYear = opts.filter(o => o.year === y0)
                // สัปดาห์ในช่วง [ที่เลือก → ปัจจุบัน] : ทั้งหมด / ที่ยังไม่เคยดึง
                // ปุ่มดึงคิดจาก "ทั้งช่วง" เพราะกดแล้วดึงใหม่ทับทั้งช่วงเสมอ (ของเดิมในทะเบียน
                // อาจมาจาก snapshot เก่าที่ยังไม่มีเลข setup/carry) ; nMissing ใช้บอกใน tooltip
                const inRange = o => histSel && (o.year > histSel.year
                  || (o.year === histSel.year && o.week >= histSel.week))
                const nAll = histSel ? opts.filter(inRange).length : 0
                const nMissing = histSel ? opts.filter(o => !o.ready && inRange(o)).length : 0
                const nHave = nAll - nMissing
                const job = histJob && histJob.running ? histJob : null
                return (
                  <span className="gbk-hist">
                    <span className="gbk-hist-label"
                      title={'ดูแผนย้อนหลัง — ช่วงที่ยังไม่เคยดึงจะไปดึงจาก booking view ใน DB ให้ใหม่\n'
                        + '(pipeline ปกติดึงแค่ 7 สัปดาห์ล่าสุด ที่เหลืออยู่ใน DB ครบ)\n'
                        + 'ดึงแล้วเก็บไว้ถาวร กดดูช่วงเดิมซ้ำไม่ต้องรออีก • ดูอย่างเดียว ลาก/แก้ไม่ได้'}>
                      ย้อนถึง
                    </span>
                    <select className="gbk-hist-sel" value={y0 ?? ''} disabled={!!job}
                      onChange={e => {
                        const y = Number(e.target.value)
                        // เปลี่ยนปี → เลือกสัปดาห์เก่าสุดของปีนั้นให้ (ได้ช่วงกว้างสุด)
                        const ws = opts.filter(x => x.year === y)
                        setHistSel(ws.length ? { year: y, week: ws[ws.length - 1].week } : null)
                      }}>
                      {years.map(y => <option key={y} value={y}>{y}</option>)}
                    </select>
                    <select className="gbk-hist-sel" value={histSel?.week ?? ''} disabled={!!job}
                      onChange={e => setHistSel({ year: y0, week: Number(e.target.value) })}>
                      {wsOfYear.map(x => (
                        // ● = ดึงมาแล้ว กดดูได้ทันที / ○ = ต้องรอดึงจาก DB
                        <option key={x.week} value={x.week}>{x.ready ? '●' : '○'} W{x.week}</option>
                      ))}
                    </select>
                    {/* ปุ่มหลัก = ดึง booking ของทั้งช่วงที่เลือกใหม่จาก DB แล้วคำนวณ AVA_MC ทับทะเบียน
                        (ไม่ใช่ "เติมเฉพาะที่ขาด" — ของเดิมอาจมาจาก snapshot เก่าที่ไม่มีเลข setup/carry)
                        ปุ่มรอง = ดูของที่มีอยู่แล้วทันทีโดยไม่ยิง DB */}
                    <button className="gbk-btn on" disabled={!!job || !histSel}
                      onClick={() => fetchHistory(histSel, true)}
                      title={`ดึง booking W${histSel?.week}/${histSel?.year} → ปัจจุบัน (${nAll} สัปดาห์) จาก DB ใหม่ `
                        + 'แล้วคำนวณเครื่อง/setup/carry ด้วย AVA_MC ทับของเดิมในทะเบียน\n'
                        + (nHave > 0 ? `(ในนี้มีในทะเบียนแล้ว ${nHave} สัปดาห์ — จะถูกดึงทับให้ข้อมูลครบชุดเดียวกัน)\n` : '')
                        + `ประมาณ 1–2 นาทีต่อสัปดาห์ (ครั้งนี้ราว ${Math.max(2, Math.round(nAll * 1.5))} นาที)\n`
                        + 'ระหว่างรอทำงานอื่นได้ ไม่ต้องเปิดหน้านี้ค้างไว้'}>
                      ⬇ ดึงใหม่จาก DB ({nAll} สัปดาห์)
                    </button>
                    <button className="gbk-btn" disabled={histBusy || !!job || !histSel}
                      onClick={() => loadHistory(histSel)}
                      title={'แสดงบล็อกจากทะเบียนที่มีอยู่แล้ว ไม่ยิง DB (เร็ว)'
                        + (nMissing > 0 ? `\n⚠ ช่วงนี้ยังไม่เคยดึง ${nMissing} สัปดาห์ — จะเห็นประวัติแหว่ง` : '')}>
                      {histBusy ? 'กำลังโหลด…' : 'แสดงจากที่มี'}
                    </button>
                    {histFrom && !histBusy && !job && (
                      <button className="gbk-btn" onClick={clearHistory}
                        title="เอาบล็อกประวัติออกจาก Gantt (กลับไปย้อนหลัง 5 สัปดาห์ตามปกติ)">ล้าง</button>
                    )}
                    {/* แถบสถานะ: กำลังดึง → ขั้นที่ทำอยู่ + เวลาที่ผ่านไป/ประมาณการ + บรรทัดล่าสุด
                        ของ log (เลขแถวเดินขึ้น = ยืนยันว่าไม่ค้าง) ; ว่างงาน → ผลรอบล่าสุด/ช่วงที่มี */}
                    <span className={'gbk-hist-info' + (job ? ' busy' : '')}
                      title={job
                        ? `${JOB_STEP[job.step] || job.step}\n${job.detail || ''}`
                        + `${job.progress ? `\nล่าสุดจาก log: ${job.progress}` : ''}`
                        + '\nงานทำที่ server — ปิดหน้านี้/รีเฟรชได้ กลับมาแล้วสถานะยังอยู่'
                        : (histRange.db?.error
                          ? `ต่อ DB ไม่ได้: ${histRange.db.error} — เลือกได้เฉพาะช่วงที่เคยดึงไว้แล้ว`
                          : `ตั้งแต่ W${histRange.live.week}/${histRange.live.year} ขึ้นมาเป็นข้อมูลสดจากไฟล์ booking`)}>
                      {job
                        ? `⏳ ${JOB_STEP[job.step] || job.step}`
                        + `${job.weeks ? ` · ${job.weeks} สัปดาห์` : ''}`
                        + `${job.elapsed_sec != null ? ` · ผ่านไป ${jobMins(job.elapsed_sec)}` : ''}`
                        + `${job.eta_sec ? ` / ประมาณ ${jobMins(job.eta_sec)}` : ''}`
                        + `${job.progress ? ` · ${String(job.progress).slice(0, 48)}` : ''}`
                        : (histErr
                          ? `⚠ ${histErr}`
                          : (histFrom
                            ? `${histNote ? histNote + ' · ' : ''}แสดง ${histItems.length} บล็อก ตั้งแต่ W${histFrom.week}/${histFrom.year}`
                            + (bookingMode === 'plan' ? ' (เฉพาะ item ที่ทำแผนวันนี้ — กด "Item (ทั้งหมด)" เพื่อดูทุกตัว)' : '')
                            : (histNote
                              || `● มีแล้ว ${opts.filter(o => o.ready).length} / เลือกได้ ${opts.length} สัปดาห์`)))}
                    </span>
                  </span>
                )
              })()}
            </span>
          )}
        </div>
      )}

      {ganttReady && !loading && (
        <div className="gantt-section">
          <div className="gantt-bar">
            <b>Gantt แผนผลิต (เครื่อง × สัปดาห์)</b>
            {/* legend สีบล็อก — อยู่บนแถบหัว Gantt ให้เห็นตลอด ไม่ต้องเลื่อนไปกางคำอธิบายท้ายตาราง */}
            {showGantt && dyeCodes && (
              <div className="gantt-legend gantt-legend-top">
                <span className="glegend-title">สีบล็อก:</span>
                <span className="glegend" title="Order — ยอดของก้อนนี้อยู่ในยอดที่ลูกค้าเปิดสีแล้วของใบสั่งนั้น (ต้องผ่านย้อม) ; คิดรายบล็อก ไม่ใช่ทั้ง item">
                  <i style={{ background: DYE_ON, borderColor: DYE_EDGE }} />Order (SO)
                </span>
                <span className="glegend" title="Forecast — ยังไม่ให้สี : item ไม่มียอดเปิดสี หรือก้อนนี้เกินยอดที่เปิดสีของใบสั่ง (item เดียวกันจึงมีทั้ง Order และ Forecast คนละสัปดาห์ได้)">
                  {/* ชิปตัวอย่างต้องหน้าตาตรงกับบล็อกจริง — โหมด SC โครงร่างเปิดอยู่ = พื้นขาว กรอบม่วง */}
                  <i style={scOutline
                    ? { background: SC_OUTLINE_BG, borderColor: SC_OUTLINE_EDGE }
                    : { background: DYE_OFF, borderColor: DYE_EDGE }} />Forecast (SC)
                </span>
              </div>
            )}
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
                  {/* โหมด SC โครงร่าง — ก้อน Forecast เหลือแค่กรอบ ให้ก้อน Order (เปิดสีแล้ว) เด่นขึ้น */}
                  <button className={'gfield-toggle' + (scOutline ? ' on' : '')}
                    onClick={() => setScOutline(s => !s)}
                    title={scOutline
                      ? 'กำลังเปิด: บล็อก Forecast (SC ยังไม่ให้สี) เป็นโครงร่างพื้นขาว — กดเพื่อกลับไปสีทึบ'
                      : 'เอาสีพื้นของบล็อก Forecast (SC ยังไม่ให้สี) ออก เหลือแค่กรอบ — เหลือแต่ก้อน Order (เปิดสีแล้ว) ที่เป็นสีทึบ'}>
                    ▢ SC โครงร่าง
                  </button>
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
          {showGantt && <PlanGantt columns={grid.columns} rows={ganttVisible} allRows={ganttAll} showReplanned={showReplanned} sheet={grid.sheet} load={load} setupJobs={setupJobs} ava={avaWithS9} bookingMc={bookingMc} poolMap={poolMap} onMoveWeek={moveAnyJob} onEditQty={editQty} onSplit={splitJob} onRemove={delRow} bookingItems={bookingItems} bookingMode={bkView.mode} bookingPick={bkView.pick} bookingAllWeeks={bkView.allWeeks} historyItems={histItems} historyFrom={histFrom} allMcRows={workdayData?.mc_rows || EMPTY_ROWS} loadFilter={loadFilter} setLoadFilter={setLoadFilter} barFields={barFields} selIdx={selJob} setSelIdx={setSelJob} programRows={programRows} programMap={program} dyeCodes={dyeCodes} scOutline={scOutline} planLocks={planLocks} onToggleLock={toggleLock} onApplyLocks={applyLocks} outsourceIdx={s9Idx} onBringHome={bringOutsourceHome} outsourceChoices={s9ChoicesFor} onSendOutsource={sendJobToOutsource}outsourceItems={bothSideItems} mergedIdx={mergedS9Idx} onOpenItem={setModalItem} openItem={modalItem} markLate />}
        </div>
      )}

      {/* ชีทที่ไม่มี MC_GROUP+PLAN_WEEK (เช่น UNPLANNED, DETAIL) ไม่มี Gantt ให้โชว์ →
          ต้องมีตารางธรรมดาให้เห็น ไม่งั้นเลือกชีทแล้วหน้าจะว่างเปล่าไม่มีอะไรขึ้นเลย */}
      {grid && !ganttReady && !loading && (
        <div className="gridwrap">
          {renderGrid(null, visibleSorted, false)}
        </div>
      )}

      {msg && <div className="msg">{msg}</div>}
      {loading && <div className="hint">กำลังโหลด...</div>}
      {!loading && !meta?.exists && <div className="hint">ยังไม่มีไฟล์แผน กด <b>รันแผนใหม่</b> แล้วรอให้สถานะจบก่อน ระบบจะโหลดให้อัตโนมัติ</div>}

      {/* คลิกบล็อกใน Gantt → เด้ง modal โชว์เฉพาะแถวของ item ที่คลิก (แก้ไขได้) คู่กับการ์ดรายละเอียด
          modal เว้นที่ฝั่งขวาให้การ์ด (JobPanel = fixed) จึงไม่ทับกัน */}
      {(selJob != null || modalItem) && grid && (() => {
        const itemCi = grid.columns.indexOf('ITEM_CODE')
        const selItem = selJob != null ? (itemCi >= 0 ? grid.rows[selJob]?.[itemCi] : null) : modalItem
        // แถวของ item เดียวกันทั้งหมด (ไม่สนตัวกรองหน้าหลัก) — ยอดสรุปด้านล่างต้องนับจากชุดนี้
        // ไม่ใช่ชุดที่ผ่านตัวกรองของ modal ไม่งั้นกรองแล้วยอด "วาง" จะดูขาดทั้งที่ของอยู่ครบ
        const itemRows = modalRows || []
        const shownRows = modalVisible || itemRows
        // สรุปยอดแยกรายออร์เดอร์ (SC_SO_NO + SC_LINE_ID) ที่อยู่ใน item นี้ — item เดียวมีหลาย SC ได้
        const scI = grid.columns.indexOf('SC_SO_NO')
        const lnI = grid.columns.indexOf('SC_LINE_ID')
        const oqI = grid.columns.indexOf('ORDERS_QTY')
        const pqI = grid.columns.indexOf('PRODUCE_QTY')
        // ออร์เดอร์นิยามด้วย ORDER_KEY_COLS (SC เดียวมีหลายออร์เดอร์: ต่าง PO / ORDERS_QTY / FG week)
        const okey = row => orderKeyOf(row, grid.columns)
        const selKey = selJob != null ? okey(grid.rows[selJob]) : null
        // LINE_REMARK ผูกกับ SC — เก็บแยกต่อออร์เดอร์ ไม่รวมข้าม SC (item เดียวกันแต่คนละ SC อาจมีหมายเหตุคนละอัน)
        const lrI = grid.columns.indexOf('LINE_REMARK')
        const ordMap = new Map()
        itemRows.forEach(({ row }) => {
          const key = okey(row)
          const m = ordMap.get(key) || { key, sc: scI >= 0 ? norm(row[scI]) : '', line: lnI >= 0 ? norm(row[lnI]) : '', ordered: 0, placed: 0, full: 0, remark: '' }
          // ordered = ยอดที่ต้องวางรอบนี้ (ฐานเดียวกับป้ายเตือน) ; full = ยอดสั่งทั้งใบ ไว้แสดงประกอบ
          m.ordered = Math.max(m.ordered, orderBaseOf(row, grid.columns))
          m.full = Math.max(m.full, Number(norm(row[oqI])) || 0)
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
        // ตารางเทียบ booking (แผนเก่า) → แผนใหม่ ของ item นี้ + ทุก item ที่ถูกขยับ/ถอดออกไปด้วยกัน
        // โชว์เฉพาะแผน replan (grid.replan) — งานที่ถูกยกจาก booking มาวางใหม่ การเทียบเก่า/ใหม่จึงมีความหมาย
        // ส่วนแผน SC ล้วน (ชีท PLAN ของไฟล์รุ่นเก่า) เป็นแผนตั้งต้น ไม่ต้องมีตารางเทียบให้รก
        const cmp = grid.replan
          ? buildPlanCompare(grid.columns, grid.origRows, grid.rows, selItem, weekOfDate, bookingAll || bookingItems, weekAxis, poinRule)
          : null
        const cell = q => (q ? fmtNum(Math.round(q * 10) / 10) : '')
        // คอลัมน์สัปดาห์ที่ freeze (แก้ไม่ได้) — ลงลายคาดทั้งคอลัมน์เหมือน Gantt
        // เพื่อให้อ่านออกทันทีว่าเลขในคอลัมน์นั้นล็อกแล้ว ไม่ใช่ช่องที่ Replan เลือกไม่วาง
        // ใช้ชุดสัปดาห์ freeze ที่ buildPlanCompare คิดจากปีจริง (PLAN_YEAR) ไม่ใช่เดาจากเลขสัปดาห์
        const isFrz = w => !!cmp && cmp.frozen.has(String(w))
        // สัปดาห์ที่ไม่มีแผนเลย (เติมเข้ามาให้แกนต่อเนื่อง) — จางลงทั้งคอลัมน์ ให้เห็นว่าเป็นช่องว่างจริง
        const isEmptyW = w => !!cmp && cmp.emptyWeeks.has(String(w))
        const frz = w => (isFrz(w) ? ' wk-frozen' : '') + (isEmptyW(w) ? ' wk-empty' : '')
        // ยอดรวมนับเฉพาะสัปดาห์ที่ยังแก้แผนได้ (ช่องที่ไม่มีลายคาด) — ของในสัปดาห์ freeze
        // ยกมาวางใหม่ไม่ได้อยู่แล้ว ถ้านับด้วยแถว "เก่า" จะดูมากกว่าแถว Replan เสมอ
        // ทั้งที่ยกมาครบ (เทียบไม่ได้ว่าตกหล่นจริงหรือแค่ล็อกอยู่)
        const sumOpen = bag => Math.round(Object.entries(bag || {})
          .reduce((s, [w, q]) => s + (isFrz(w) ? 0 : q), 0) * 10) / 10
        return (
          <div className="plan-modal-backdrop" onClick={closeModal}>
            <div className="plan-modal" onClick={e => e.stopPropagation()}>
              <div className="plan-modal-head">
                <b>{selItem || 'ตารางแผนผลิต'}</b>
                <span className="plan-modal-hint">
                  {shownRows.length !== itemRows.length
                    ? `${shownRows.length}/${itemRows.length} แถว (กรองอยู่)`
                    : `${itemRows.length} แถว`} · {orders.length} ออร์เดอร์ · แก้ค่าในช่องได้เลย
                </span>
                {Object.keys(modalFilters).length > 0 && (
                  <button className="clearf" onClick={() => setModalFilters({})}>ล้างตัวกรองตารางนี้</button>
                )}
                <button className="plan-modal-close" onClick={closeModal} title="ปิด (Esc)">✕</button>
              </div>
              {orders.length > 0 && (
                <div className="plan-modal-orders">
                  {orders.map(o => (
                    <div key={o.key} className="order-item">
                      <span
                        className={'order-status ' + (Math.abs(o.diff) <= QTY_TOL ? 'ok' : o.diff > 0 ? 'short' : 'over') + (o.key === selKey ? ' cur' : '')}>
                        SC {o.sc || '-'}{o.line ? `/${o.line}` : ''} · ต้องวาง {o.ordered.toLocaleString()} · วาง {o.placed.toLocaleString()}
                        {Math.abs(o.diff) <= QTY_TOL ? ' ✓' : o.diff > 0 ? ` ⚠ ขาด ${o.diff.toLocaleString()}` : ` ⚠ เกิน ${(-o.diff).toLocaleString()}`}
                        {o.full > o.ordered + QTY_TOL
                          && ` (สั่งทั้งใบ ${o.full.toLocaleString()} · วางแผนไปแล้ว ${(Math.round((o.full - o.ordered) * 100) / 100).toLocaleString()})`}
                      </span>
                      {o.remark && <div className="order-remark">📝 {o.remark}</div>}
                    </div>
                  ))}
                </div>
              )}
              <div className={'gridwrap plan-modal-grid' + (cmp ? ' has-compare' : '')}>
                {itemRows.length > 0
                  ? renderGrid(selJob, shownRows, true, 'modal')
                  : <div className="hint">item นี้ไม่มีแถวในแผนใหม่ (ล็อก/อยู่ใน booking) — ดูตารางเทียบ booking → แผนใหม่ด้านล่าง</div>}
              </div>
              {cmp && (
                <div className="plan-modal-compare">
                  <div className="hint small" style={{ padding: '0 0 6px' }}>
                    ตารางเทียบ <b>booking (แผนเก่า) → แผนใหม่</b> — กก. ต่อสัปดาห์ ·{' '}
                    {cmp.nEdited > 0
                      ? <>ปรับบนหน้าจอแล้ว <b>{cmp.nEdited}</b> item (รวม item อื่นที่ถูกขยับ/ถอดออกเพื่อเปิดที่)</>
                      : <>ยังไม่มีการปรับบนหน้าจอ — แผนใหม่ = แผนในไฟล์</>} ·{' '}
                    <span className="cell-out-demo">เก่า</span> = booking มีแต่แผนใหม่ไม่มีแล้ว ·{' '}
                    <span className="cell-in-demo">Replan</span> = ใบที่มีใน booking แล้วถูกยกมาวางใหม่ ·{' '}
                    <span className="cell-in-demo">SC ใหม่</span> = ใบที่ไม่เคยมีใน booking (วางครั้งแรก ไม่ใช่การแก้แผน) ·{' '}
                    ✎ = item ที่ปรับบนหน้าจอแล้ว ·{' '}
                    <span className="wk-empty-demo">W41</span> = สัปดาห์ที่ไม่มีแผน (โชว์ไว้ให้เห็นช่องว่าง ไม่ใช่ทอต่อเนื่อง) ·{' '}
                    <span className="cell-po-demo">POIN DATE WEEK</span> = สัปดาห์แรกที่ใบนั้นพร้อมวางแผน (ทอได้เร็วสุดสัปดาห์ถัดจาก POIN DATE ของใบเอง) · ⚠ = ใบนั้นถูกวางก่อนสัปดาห์ที่ด้ายเข้าครบ (ใบอื่นในก้อนเดียวกันไม่เกี่ยว) ·{' '}
                    <span className="cell-frz-demo">🔒 ลายคาด</span> = สัปดาห์ freeze แก้แผนไม่ได้ (เหมือนคอลัมน์ลายคาดบน Gantt)
                  </div>
                  <table className="grid chg-table">
                    <thead>
                      <tr>
                        <th>ITEM</th><th>MC_GROUP</th><th>POIN DATE</th><th>แผน</th>
                        {cmp.weeks.map(w => (
                          <th key={w} className={'wk-col' + frz(w)}
                            title={isEmptyW(w) ? `${wkLabel(w)} — ไม่มีแผนในสัปดาห์นี้` : undefined}>
                            {wkLabel(w)}{isFrz(w) && <span className="wk-frz-tag">🔒</span>}
                          </th>
                        ))}
                        <th className="wk-col">
                          รวม<div style={{ fontSize: '0.8em', fontWeight: 400, opacity: 0.75 }}>(แก้ได้)</div>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {cmp.list.map(g => {
                        // แถวผลลัพธ์แยกตามที่มาของงาน — โชว์เฉพาะถังที่มีของจริง
                        // ก้อนที่ทั้ง replan ของเดิมและรับใบใหม่เข้ามาด้วย จะได้ 2 แถว เห็นยอดแยกกัน
                        const aRows = [
                          { k: 'R', label: 'Replan', note: 'ยกจาก booking มาวางใหม่', bag: g.afterR, tot: g.totalR, open: sumOpen(g.afterR), sc: g.scReplan, more: g.soOldMore, hl: 'cell-in', diff: true },
                          { k: 'N', label: 'SC ใหม่', note: 'ไม่เคยมีใน booking — วางครั้งแรก', bag: g.afterN, tot: g.totalN, open: sumOpen(g.afterN), sc: g.scNew, more: false, hl: 'cell-in', diff: false },
                        ].filter(r => r.tot > QTY_TOL || r.sc.size > 0)
                        // ก้อนที่หลุดจากแผนใหม่ทั้งก้อน ยังต้องมีแถวศูนย์ไว้เทียบกับ booking
                        if (!aRows.length) aRows.push({ k: 'R', label: 'Replan', note: '', bag: {}, tot: 0, open: 0, sc: g.scReplan, more: false, hl: 'cell-in', diff: true })
                        const openBefore = sumOpen(g.before)
                        const span = 1 + aRows.length + (g.hasPo ? 1 : 0)
                        return (
                          <React.Fragment key={g.key}>
                            <tr className="chg-before">
                              <td className="rocell" rowSpan={span}>
                                {g.isSel ? '★ ' : ''}<b>{g.item}</b>
                                {/* SC ย้ายไปอยู่บนแถวเก่า/ใหม่ของตัวเอง — ตัวเดียวคร่อมทั้งก้อนทำให้
                                    booking ของ SC อื่นดูเหมือนเป็นใบเดียวกับแผนใหม่ */}
                                {!g.hasBooking && <div className="ai-reason" style={{ fontSize: '0.85em' }}>ไม่มีใน booking (งานใหม่)</div>}
                                {g.totalAfter === 0 && <div className="ai-impact" style={{ fontSize: '0.85em' }}>ไม่อยู่ในแผนใหม่</div>}
                              </td>
                              <td className="rocell" rowSpan={span}>{g.mcg}</td>
                              <td className="rocell" rowSpan={span}>
                                {/* วันด้ายเข้าของ "ใบที่ช้าสุด" ในก้อน — ด่านจริงคิดรายใบ (แถว POIN DATE ข้างล่าง) */}
                                {g.yarn
                                  ? <>{g.yarn}{g.yarnWeek != null && <div className="ai-reason" style={{ fontSize: '0.85em' }}>{wkLabel(g.yarnWeek)}{g.yarnStartWeek != null && <> → ทอได้ {wkLabel(g.yarnStartWeek)}</>}{g.yarnPast && ' (POIN DATE ผ่านแล้ว)'}{g.poList.length > 1 && ' · ใบที่ช้าสุด'}{g.yarnFromBooking && ' · จาก booking'}</div>}</>
                                  : '—'}
                              </td>
                              <td className="rocell">เก่า (booking)
                                {g.soOld.size > 0 && (
                                  <div className="ai-reason" style={{ fontSize: '0.85em' }}>
                                    SC {[...g.soOld].join(', ')}{g.soOldMore ? ' …' : ''}
                                  </div>
                                )}
                                {g.scDrop.size > 0 && (
                                  <div className="ai-impact" style={{ fontSize: '0.85em' }}>
                                    หลุดจากแผน: {[...g.scDrop].join(', ')}
                                  </div>
                                )}
                              </td>
                              {cmp.weeks.map(w => {
                                const b = g.before[String(w)] || 0, a = g.after[String(w)] || 0
                                return <td key={w} className={'wk-col' + (b && Math.abs(b - a) > QTY_TOL ? ' cell-out' : '') + frz(w)}>{cell(b)}</td>
                              })}
                              {/* ยอดเต็ม (รวมสัปดาห์ freeze) โชว์เป็นตัวเล็กไว้ — ของในสัปดาห์ที่ล็อกยังผลิตจริง */}
                              <td className="wk-col">{fmtNum(openBefore)}
                                {Math.abs(openBefore - g.totalBefore) > QTY_TOL && (
                                  <div className="ai-reason" style={{ fontSize: '0.85em' }}>ทั้งหมด {fmtNum(g.totalBefore)}</div>
                                )}
                              </td>
                            </tr>
                            {aRows.map((r, ri) => (
                              /* เส้นคั่นก้อนหนาอยู่ที่แถวสุดท้ายของก้อนเท่านั้น (mid = เส้นบาง) */
                              <tr key={r.k} className={'chg-after' + (g.hasPo || ri < aRows.length - 1 ? ' mid' : '')}>
                                <td className="rocell">
                                  <b>{r.label}</b>{r.k === 'R' && g.edited ? ' ✎' : ''}
                                  {r.sc.size > 0 && (
                                    <div className="ai-reason" style={{ fontSize: '0.85em' }}>
                                      SC {[...r.sc].join(', ')}{r.more ? ' …' : ''}
                                    </div>
                                  )}
                                  {/* ใบใหม่ที่ถูกยุบอยู่แถวเดียวกับใบเก่า → ยอดไปกองที่แถว Replan
                                      บอกไว้ ไม่งั้นเห็นแถวนี้เป็นศูนย์แล้วคิดว่าไม่ได้วาง */}
                                  {r.k === 'N' && r.tot <= QTY_TOL && r.sc.size > 0 && (
                                    <div className="ai-reason" style={{ fontSize: '0.85em' }}>
                                      ยอดยุบรวมอยู่แถว Replan (แถวแผนเดียวมีหลายใบ)
                                    </div>
                                  )}
                                </td>
                                {cmp.weeks.map(w => {
                                  const a = r.bag[String(w)] || 0
                                  // Replan: ไฮไลต์เฉพาะช่องที่ต่างจาก booking (เท่าเดิม = ไม่ได้ขยับ)
                                  // SC ใหม่: ไฮไลต์ทุกช่องที่มีของ เพราะไม่มีฝั่ง booking ให้เทียบอยู่แล้ว
                                  const b = r.diff ? (g.before[String(w)] || 0) : 0
                                  return <td key={w} className={'wk-col' + (a && Math.abs(a - b) > QTY_TOL ? ' ' + r.hl : '') + frz(w)}>{cell(a)}</td>
                                })}
                                <td className="wk-col"><b>{fmtNum(r.open)}</b>
                                  {Math.abs(r.open - r.tot) > QTY_TOL && (
                                    <div className="ai-reason" style={{ fontSize: '0.85em' }}>ทั้งหมด {fmtNum(r.tot)}</div>
                                  )}
                                </td>
                              </tr>
                            ))}
                            {g.hasPo && (
                              <tr className="chg-po">
                                <td className="rocell"><b>POIN DATE</b>{!g.poPushed && !g.poinOff && ' ✓'}</td>
                                <td colSpan={cmp.weeks.length + 1} style={{ textAlign: 'left' }}>
                                  {/* รายใบ: ด่านด้ายเข้าเป็นของแต่ละใบสั่ง ไม่ใช่ของทั้งก้อน
                                      ⚠ ขึ้นเฉพาะใบที่แถวแผนของมันเองถูกวางก่อนสัปดาห์ที่ด้ายเข้าครบ */}
                                  {g.poList.length > 0 ? g.poList.map(p => (
                                    <div key={p.sc}>
                                      <b>SC {p.sc} · POIN DATE WEEK {p.week != null ? wkLabel(p.week).slice(1) : '—'}</b>
                                      {p.bad.length > 0
                                        ? <span className="ai-impact"> ⚠ วางก่อนสัปดาห์นี้ (ด้ายยังมาไม่ครบ): {p.bad.map(wkLabel).join(', ')}</span>
                                        : p.stock
                                          ? <span className="ai-reason" style={{ display: 'inline', marginLeft: 6 }}>🧶 {p.stock} — ไม่ต้องรอ PO IN</span>
                                          : g.poinOff
                                          ? <span className="ai-reason" style={{ display: 'inline', marginLeft: 6 }}>ไม่ตรวจ — ปิดกติกาวันด้ายเข้าที่หน้า Monitor Yarn</span>
                                          : ' ✓'}
                                      <span className="ai-reason" style={{ display: 'inline', marginLeft: 6 }}>
                                        ด้ายเข้าครบ {p.yarn}{p.past && ' (ผ่านแล้ว)'}
                                      </span>
                                    </div>
                                  )) : (
                                    <>
                                      <b>POIN DATE WEEK {g.yarnStartWeek != null ? wkLabel(g.yarnStartWeek).slice(1) : '—'}</b>
                                      <span className="ai-reason" style={{ display: 'inline', marginLeft: 6 }}>
                                        จาก booking — ใบในแผนใหม่ไม่มีวันด้ายเข้า จึงไม่ใช้ตัดสิน
                                      </span>
                                    </>
                                  )}
                                </td>
                              </tr>
                            )}
                          </React.Fragment>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )
      })()}

      {openCol && colData && (
        <ColumnFilter
          key={openCol.scope + ':' + openCol.ci}
          available={colData.available}
          domain={colData.domain}
          selected={(openCol.scope === 'modal' ? modalFilters : filters)[openCol.ci]}
          anchor={openCol.anchor}
          onApply={set => applyFilter(openCol.ci, set, openCol.scope)}
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
            {/* ต้องส่งชีทที่เปิดอยู่ไปด้วย — โหมด REPLAN มีแถวและเครื่องว่างคนละชุดกับ PLAN
                onApplied = ติ๊กเสร็จแล้วให้หน้าแผนย้ายงาน + โหลดเครื่องว่างใหม่ทันที */}
            <CylinderAdvisor sheet={grid?.sheet || ''} planFile={grid?.name || ''}
              onApplied={applyCylinderPicks} />
          </div>
        </div>
      )}

      {showMail && (
        <div className="modal-backdrop" onClick={() => setShowMail(false)}>
          <div className="modal-box mail-modal" onClick={e => e.stopPropagation()}>
            <button className="modal-close" title="ปิด" onClick={() => setShowMail(false)}>✕</button>
            <ReplanNotify onClose={() => setShowMail(false)} />
          </div>
        </div>
      )}

      {showPivot && grid && (
        <div className="modal-backdrop" onClick={() => setShowPivot(false)}>
          <div className="modal-box pivot-modal" onClick={e => e.stopPropagation()}>
            <button className="modal-close" title="ปิด" onClick={() => setShowPivot(false)}>✕</button>
            {/* ส่งทั้งชีทและแถวที่ผ่านตัวกรองไปด้วย — เลือกได้ว่าจะ pivot ทั้งชีทหรือเฉพาะที่กรองอยู่
                (visibleSorted = แถวหลังคำค้น + ตัวกรองคอลัมน์ + ติ๊ก item ชุดเดียวกับตารางข้างล่าง) */}
            <PlanPivot
              sheet={grid.sheet} sheets={sheetOptions}
              columns={grid.columns} allRows={grid.rows}
              filteredRows={visibleSorted} hasFilter={!!hasFilter} />
          </div>
        </div>
      )}
    </div>
  )
}

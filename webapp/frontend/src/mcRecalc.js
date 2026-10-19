// คำนวณ carry / setup / จำนวนเครื่อง ใหม่ เมื่อผู้ใช้ลาก–แบ่ง–ลบงานบน Gantt
//
// ทำไมต้องมี: ไฟล์แผนเก็บแค่ "ผลลัพธ์" ที่ Planning.py คำนวณไว้ พอ user ลากบล็อกข้ามสัปดาห์
// เครื่องที่วิ่งต่อจากสัปดาห์ก่อน (CARRYOVER_MC) อาจไม่มีอยู่จริงแล้ว → งานก้อนนั้นต้องตั้งเครื่องใหม่
// (NEW_MC) ซึ่งมีผลต่อเนื่อง 3 อย่าง: กินเครื่องจากพูล (เลข "ว่าง") · กินโควตา job setup รายสัปดาห์ ·
// เสียวัน setup ทำให้วันผลิตต่อเครื่องหด จึงต้องใช้เครื่องมากกว่าเดิมเพื่อผลิตยอดเท่าเดิม
//
// กติกาทั้งหมดต้องตรงกับ Planning.py:
//   • ลูกโซ่ (run) = ITEM_CODE + MC_GROUP + MC_GUAGE           (_carry_key)
//   • เครื่องยังอุ่นเมื่อห่างจากสัปดาห์ก่อนหน้าของลูกโซ่ ≤ SETUP_GAP_WEEK สัปดาห์ (ตั้งได้บนหน้าเว็บ)
//   • SETUP_DAYS ของแถว = setup ต่อเครื่อง × NEW_MC             (setup_days_used)
//   • วันผลิตของเครื่องใหม่ = newRunDays() ด้านล่าง              (prod_days_new, Planning.py:10730)
//   • กำลังผลิต = (carry × วันทำงาน + ใหม่ × วันผลิตเครื่องใหม่) × DAILY_CAPACITY
//
// ฟังก์ชันในไฟล์นี้เป็น pure ทั้งหมด (ไม่แตะ state/DOM) — คำนวณล่วงหน้าเพื่อพรีวิว/ถามยืนยันได้
// ก่อนค่อย commit ลง grid จริง

import { weekSortKey, keyFromSortKey, rowWeekKey, wkNum } from './weekKeys.js'

// ── ค่าที่ผู้ใช้ตั้งได้จากหน้า "ตั้งค่าระบบ" ────────────────────────────────────
// ตัวเลขข้างล่างเป็นแค่ "ค่าเริ่มต้นก่อนโหลดค่าจริง" — ต้องตรงกับ default ใน plan_config.py
// (KP_SETUP_GAP_WEEK / KP_SETUP_DAYS / KP_SETUP_DAYS_HEAVY) ; KnitPlan เรียก setPlanConstants()
// ทันทีที่โหลด /api/plan-config เสร็จ และทุกครั้งที่มีคนบันทึกค่าใหม่
// ถ้าปล่อยเป็นค่าคงที่ ผู้ใช้แก้ค่าบนเว็บแล้วตัวเลขเครื่องบน Gantt จะไม่ตรงกับแผนที่ engine วางจริง
export let SETUP_GAP_WEEK = 3
let DEFAULT_SETUP_DAYS = 3
let POLY_SETUP_DAYS = 5

/** รับค่าจาก /api/plan-config → {KP_SETUP_GAP_WEEK, KP_SETUP_DAYS, KP_SETUP_DAYS_HEAVY}
 *  ค่าที่อ่านไม่ได้/ติดลบ ให้คงค่าเดิมไว้ (ดีกว่าคำนวณด้วย NaN) */
export function setPlanConstants(v = {}) {
  const n = (x, cur) => {
    const f = Number(x)
    return Number.isFinite(f) && f >= 0 ? f : cur
  }
  SETUP_GAP_WEEK = n(v.KP_SETUP_GAP_WEEK, SETUP_GAP_WEEK)
  DEFAULT_SETUP_DAYS = n(v.KP_SETUP_DAYS, DEFAULT_SETUP_DAYS)
  POLY_SETUP_DAYS = n(v.KP_SETUP_DAYS_HEAVY, POLY_SETUP_DAYS)
}

/** ค่าที่ใช้อยู่จริงตอนนี้ — ให้หน้าอื่น/เทสต์ตรวจได้ */
export function planConstants() {
  return { SETUP_GAP_WEEK, DEFAULT_SETUP_DAYS, POLY_SETUP_DAYS }
}

const MIN_RUN_DAYS = 0.5            // เครื่องที่เพิ่ง setup ยังต้องได้เดินอย่างน้อยครึ่งวัน
const MAX_MACHINES = 999            // เพดานกันลูปหนี เมื่อวัน setup กินวันทำงานจนเกือบไม่เหลือ

const txt = (v) => String(v ?? '').trim()
const up = (v) => txt(v).toUpperCase()
const num = (v) => { const n = Number(txt(v)); return Number.isFinite(n) ? n : 0 }
// เกจในไฟล์แผนมีทั้ง "26" และ "26.0" — ตัดหางให้เทียบกันได้ (เหมือน PlanGantt/KnitPlan)
const gz = (v) => txt(v).replace(/\.0+$/, '')

// ตำแหน่งคอลัมน์ที่ต้องใช้ (−1 = ชีทนี้ไม่มี)
export function colIx(columns = []) {
  const g = (n) => columns.indexOf(n)
  return {
    item: g('ITEM_CODE'), mc: g('MC_GROUP'), gauge: g('MC_GUAGE'), week: g('PLAN_WEEK'),
    year: g('PLAN_YEAR'),
    qty: g('PRODUCE_QTY'), daily: g('DAILY_CAPACITY'),
    actual: g('ACTUAL_MC'), carry: g('CARRYOVER_MC'), neu: g('NEW_MC'),
    setup: g('SETUP_DAYS'), avail: g('AVAILABLE_DAYS'), awd: g('ACTUAL_WORKING_DAYS'),
    booking: g('MC_BOOKING'), shared: g('MC_SHARED'), outsource: g('OUTSOURCE'), sc: g('SC_SO_NO'),
    mat: g('MATERIAL_CONTENT'), yarn: g('YARN_USED'),
    factory: g('FACTORY_TYPE'), cat: g('CAT'),
  }
}

// ชีทที่คำนวณต่อได้ต้องมีคอลัมน์ครบชุดนี้ (ชีท PLAN เท่านั้น)
export function canRecalc(columns = []) {
  const ix = colIx(columns)
  return ['item', 'mc', 'gauge', 'week', 'qty', 'daily', 'actual', 'carry', 'neu']
    .every(k => ix[k] >= 0)
}

// คีย์ลูกโซ่ carry — ต้องตรงกับ _ck ใน Planning.py (item + "ก้อนเครื่อง" + เกจ)
//
// ก้อนเครื่อง = เครื่องที่อยู่พูลเดียวกัน (โรง+CAT+เกจ+Group) ใช้แทนกันได้จริง เช่น RAP กับ
// RAP98 ใน PHET|DOUBLE-30 22G → งานของ item เดียวกันต้องเป็นลูกโซ่เดียว ไม่งั้นสลับกลุ่มที
// นับเป็น setup ใหม่ทุกที และปัดเศษเครื่องคนละรอบจนเครื่องเกินจริง
// groupMap = { "CAT|GAUGE|MC": pool_key } จาก /api/plan/mc-groups (ไม่มี → ใช้ชื่อกลุ่มตามเดิม)
export function groupKeyOf(row, ix, groupMap) {
  const mc = up(row[ix.mc])
  if (!groupMap || ix.cat < 0) return mc
  const g = groupMap[`${txt(row[ix.cat])}|${gz(row[ix.gauge])}|${mc}`]
  return g || mc
}

export function chainKeyOf(row, ix, groupMap) {
  return `${up(row[ix.item])}|${groupKeyOf(row, ix, groupMap)}|${gz(row[ix.gauge])}`
}

// คีย์เครื่อง booking (backend ส่งเป็น ITEM|MC_GROUP|GAUGE ตามชื่อกลุ่มจริง)
export function bookingKeyOf(row, ix) {
  return `${up(row[ix.item])}|${up(row[ix.mc])}|${gz(row[ix.gauge])}`
}

// แถวที่ไม่กินเครื่องในบ้าน / ไม่ได้ถือเครื่องเอง → ไม่คำนวณใหม่ ปล่อยค่าเดิมไว้
//   OUTSOURCE=YES  = จ้างทอข้างนอก (🧵)
//   MC_SHARED>0    = เติมเข้าเครื่องที่ SO อื่นทอไอเทมนี้อยู่แล้ว (📦) เครื่องเป็นของแถวนั้น ไม่ใช่ของแถวนี้
function isPassenger(row, ix) {
  if (ix.outsource >= 0 && up(row[ix.outsource]) === 'YES') return true
  if (ix.shared >= 0 && num(row[ix.shared]) > 0) return true
  return false
}

// setup ต่อเครื่อง (วัน) ของแต่ละแถว
//
// แหล่งที่แม่นที่สุด = แผนเอง: แถวที่ตั้งเครื่องใหม่จริงมี SETUP_DAYS = setup ต่อเครื่อง × NEW_MC
// → หารกลับได้ค่าที่ Planning.py ใช้ (ซึ่งมาจากชีท Item Setup / MC setup time ที่ frontend อ่านไม่ได้)
// ไล่จากเจาะจงสุด: (item+เครื่อง) → item → เครื่อง → สุดท้ายเดาจากเนื้อผ้า (POLY/DTY = 5 วัน)
export function makeSetupDays(columns, rows = []) {
  const ix = colIx(columns)
  const byItemMc = new Map(), byItem = new Map(), byMc = new Map()
  if (ix.neu >= 0 && ix.setup >= 0) {
    for (const r of rows) {
      const n = num(r[ix.neu]), d = num(r[ix.setup])
      if (!(n > 0 && d > 0)) continue
      const per = Math.round((d / n) * 100) / 100
      const item = up(r[ix.item]), mc = up(r[ix.mc])
      if (!byItemMc.has(`${item}|${mc}`)) byItemMc.set(`${item}|${mc}`, per)
      if (!byItem.has(item)) byItem.set(item, per)
      if (!byMc.has(mc)) byMc.set(mc, per)
    }
  }
  return (row) => {
    const item = up(row[ix.item]), mc = up(row[ix.mc])
    const hit = byItemMc.get(`${item}|${mc}`) ?? byItem.get(item) ?? byMc.get(mc)
    if (hit != null) return hit
    const mat = ix.mat >= 0 ? up(row[ix.mat]) : ''
    const yarn = ix.yarn >= 0 ? up(row[ix.yarn]) : ''
    if (mat.includes('POLY') && !mat.includes('COTTON')) return POLY_SETUP_DAYS
    if (yarn.includes('DTY')) return POLY_SETUP_DAYS
    return DEFAULT_SETUP_DAYS
  }
}

// คีย์สัปดาห์ของแถว (weekKeys.js) — แผนยาวข้ามปีแล้ว W35/2027 กับ W35/2028 ต้องเป็นคนละลูกโซ่ช่อง
// '' = แถวไม่มีสัปดาห์
const rowWk = (row, ix) => rowWeekKey(row, ix.week, ix.year)
const hasWk = (w) => w !== '' && Number.isFinite(wkNum(w))
// เลื่อนคีย์ไป d สัปดาห์บนแกนเรียง (ข้ามปีได้) — W53 → W1 ของปีหน้าคือสัปดาห์ติดกัน
const shiftWk = (w, d) => keyFromSortKey(weekSortKey(w) + d)

// แกนสัปดาห์สำหรับวัด "ห่างกี่สัปดาห์" — สัปดาห์ที่ถูกยุบเข้าสัปดาห์อื่น/โรงปิดทั้งสัปดาห์
// ไม่นับเป็นช่อง (เครื่องไม่ได้เย็นลงเพราะสัปดาห์ที่ไม่มีใครเดินเครื่อง)
// weeksWithJobs = Set ของคีย์สัปดาห์ ; เดินแกนด้วย weekSortKey (ปีละ 53 ช่อง) แล้วกรองช่องที่ไม่มีจริง
// ด้วย isActive ; W99 (งานล้น) ไม่ใช่เวลาจริง → ต่อท้ายแกน ไม่เดินช่วงไปถึง
function makeAxis(weeksWithJobs, isActive) {
  const real = [...weeksWithJobs].filter(w => hasWk(w) && wkNum(w) !== 99)
  const tail = [...weeksWithJobs].filter(w => hasWk(w) && wkNum(w) === 99)
  const bySort = new Map(real.map(w => [weekSortKey(w), w]))
  const sorts = [...bySort.keys()].sort((a, b) => a - b)
  const pos = new Map()
  const byIdx = []                                           // ช่องบนแกน → คีย์สัปดาห์ (ไว้ไล่สัปดาห์ที่เว้น)
  const put = (w) => { pos.set(w, byIdx.length); byIdx.push(w) }
  if (sorts.length) {
    for (let s = sorts[0]; s <= sorts[sorts.length - 1]; s++) {
      const job = bySort.get(s)
      if (job != null) { put(job); continue }
      const w = keyFromSortKey(s)
      if (isActive(w)) put(w)
    }
  }
  tail.forEach(put)
  return {
    idxOf: (w) => (pos.has(String(w)) ? pos.get(String(w)) : null),
    weekAt: (idx) => (idx >= 0 && idx < byIdx.length ? byIdx[idx] : null),
  }
}

// วันผลิตของเครื่องที่เพิ่ง setup — ต้องตรงกับ prod_days_new ใน Planning.py:10730
//   วันทำงาน < setup → หัก setup แค่ 0.5 วัน (ไม่งั้นสัปดาห์สั้นจะผลิตไม่ได้เลย)
//   ปกติ → วันทำงาน − setup แต่ไม่ต่ำกว่า 0.5 วัน
export function newRunDays(wd, setupPer) {
  if (wd < setupPer) return Math.max(0, wd - MIN_RUN_DAYS)
  return Math.max(MIN_RUN_DAYS, wd - setupPer)
}

// กำลังผลิตรวมของสัปดาห์ในหน่วย "เครื่อง-วัน" — carry ได้วันเต็ม, เครื่องใหม่เสียวัน setup ไปก่อน
function machineDays(total, carryAvail, wd, setupPer) {
  const c = Math.min(total, Math.max(0, carryAvail))
  const fresh = total - c
  return c * wd + fresh * newRunDays(wd, setupPer)
}

// จำนวนเครื่องน้อยที่สุดที่ผลิต needDays (เครื่อง-วัน) ทัน แต่ไม่ต่ำกว่า floor
// floor = จำนวนเครื่องที่ Planning.py วางไว้ในไฟล์ — ไม่ลดให้ต่ำกว่านั้น (แผนอาจตั้งเครื่องเกิน
// ขั้นต่ำด้วยเหตุผลอื่น เช่น พับ/rev weight) แต่เพิ่มให้เมื่อวันผลิตหด
function minMachines(needDays, carryAvail, wd, setupPer, floor) {
  let n = Math.max(1, Math.round(floor) || 0)
  if (!(needDays > 0)) return Math.max(0, Math.round(floor) || 0)
  if (!(wd > 0)) return n                       // สัปดาห์ไม่มีวันทำงาน — คำนวณต่อไม่ได้ คงเลขเดิม
  while (n < MAX_MACHINES && machineDays(n, carryAvail, wd, setupPer) + 1e-6 < needDays) n++
  return n
}

/**
 * เครื่องอุ่นที่ "มาจากนอกแผน" ของแต่ละลูกโซ่ — ต้องสร้างจากไฟล์แผนตอนโหลด (ก่อน user แก้อะไร)
 *
 * ถ้า "สัปดาห์แรกสุด" ของลูกโซ่มี CARRYOVER_MC > 0 ทั้งที่ไม่มีสัปดาห์ก่อนหน้าให้ carry แปลว่า
 * เครื่องวิ่งอยู่แล้วจาก booking/แผนรอบก่อนซึ่งอยู่นอกไฟล์ ถ้าไม่รู้จักเครื่องพวกนี้ การคำนวณใหม่
 * จะเห็นเป็น "ต้อง setup ใหม่" ทั้งที่ user ยังไม่ได้แตะอะไร
 *
 * ⚠️ ต้องดูเฉพาะสัปดาห์แรกสุดของลูกโซ่เท่านั้น — carry ของสัปดาห์ถัด ๆ ไปมาจากสัปดาห์ก่อนหน้า
 * "ในแผนเอง" ไม่ใช่จากนอกแผน ถ้าเก็บมาเป็น seed ด้วย จะกลายเป็นเครื่องอุ่นผีที่แจกให้ตลอด
 * (อาการ: ย้ายงานออกจากลูกโซ่จนหมดแล้วย้ายกลับมา ระบบยังบอก carry ทั้งที่ไม่มีงานอยู่เลย)
 *
 * เครื่องอุ่นผูกกับ "สัปดาห์" ไม่ใช่กับงาน → ลากงานออกไปไกลเกิน 3 สัปดาห์ ก็หมดสิทธิ์ใช้ (ต้อง setup เอง)
 * @returns { chainKey: { week, mc } }
 */
export function makeWarmSeed(columns, rows = [], groupMap = null) {
  const out = {}
  if (!canRecalc(columns)) return out
  const ix = colIx(columns)
  const first = {}                       // ลูกโซ่ → { key: ลำดับสัปดาห์แรกสุด, week, mc }
  for (const row of rows) {
    if (isPassenger(row, ix)) continue
    const w = rowWk(row, ix)
    if (!hasWk(w)) continue
    const key = chainKeyOf(row, ix, groupMap)
    const k = weekSortKey(w)
    const carry = num(row[ix.carry])
    const cur0 = first[key]
    if (!cur0 || k < cur0.key) first[key] = { key: k, week: w, mc: carry }
    else if (k === cur0.key) cur0.mc = Math.max(cur0.mc, carry)   // หลาย SC = เครื่องชุดเดียว (max)
  }
  // สัปดาห์แรกของลูกโซ่ไม่มี carry = ลูกโซ่นี้ตั้งเครื่องเองทั้งหมด ไม่มีเครื่องอุ่นจากนอกแผน
  for (const [key, v] of Object.entries(first)) {
    if (v.mc > 0) out[key] = { week: v.week, mc: v.mc }
  }
  return out
}

// จำแนกประเภท item ตาม prefix — ต้องตรงกับ itemPoolType ใน PlanGantt.jsx / _get_item_cotton_poly
// ใน Planning.py (FD5/F5 = COTTON, FD4/F4 = POLY, อื่น ๆ = งานปกติ)
function itemPoolType(item) {
  const s = up(item)
  if (s.startsWith('FD5') || s.startsWith('F5')) return 'cotton'
  if (s.startsWith('FD4') || s.startsWith('F4')) return 'poly'
  return ''
}

/**
 * เครื่องว่างจริงต่อ (สัปดาห์ × พูล CAT|เกจ) — ตรรกะเดียวกับ planMcAgg + ชิป "ว่าง N" ใน PlanGantt.jsx
 * ต่างกันที่นี่คิดจากแถว "ทั้งแผน" ไม่ใช่แค่แถวที่ผ่านตัวกรอง (ลูกโซ่ต้องเห็นเครื่องครบทุก item)
 *
 * ว่าง(ปกติ)   = ava.remain − Σ เครื่องที่แผนจองเพิ่ม (ACTUAL_MC − MC_BOOKING ของ item นั้น)
 * ว่าง(POLY/COTTON) = เครื่องที่กันไว้ − booking ที่ใช้ไป − ที่แผนใช้ (งาน sub-pool ไม่กินเครื่องปกติ)
 *
 * @returns (row, week) → จำนวนเครื่องว่าง หรือ null เมื่อไม่มีข้อมูล ava ของสัปดาห์/พูลนั้น (= ไม่จำกัด)
 */
export function makeFreeAt(columns, rows = [], opts = {}) {
  const { ava = {}, poolMap = {}, bookingMc = {} } = opts
  const ix = colIx(columns)
  if (ix.cat < 0 || ix.gauge < 0) return () => null

  // key ของพูล — รองรับพูลแยก (SKP vs SKPTA/SKPLE) เหมือน poolKeyOf ใน PlanGantt.jsx
  const poolKeyOf = (cat, gauge, mc) => {
    const cg = txt(cat) + '|' + gz(gauge)
    const mg = up(mc)
    return (mg && poolMap[cg + '|' + mg]) || cg
  }

  // รวมเครื่องของแผนต่อ (สัปดาห์ × พูล) — สูตรเดียวกับ _pool_blocks_from_plan() ใน Planning.py
  // และ planMcAgg ใน PlanGantt.jsx: หลายแถวของ "ใบสั่งเดียวกัน" = เครื่องชุดเดียวที่เขียนค่าซ้ำ → max
  // แต่ "คนละใบสั่ง" = คนละชุดเครื่องจริง → บวก (ถ้า max ทั้ง item เลข "ว่าง" จะบวมเกินจริง)
  const byItem = new Map()
  for (const r of rows) {
    const w = rowWk(r, ix); if (!w) continue       // คีย์เดียวกับ ava/bookingMc จาก backend (week_key)
    if (isPassenger(r, ix)) continue
    const item = up(r[ix.item]), mc = up(r[ix.mc]), g = gz(r[ix.gauge])
    const pool = poolKeyOf(r[ix.cat], g, mc)
    const k = `${w}@@${pool}@@${item}|${mc}|${g}`
    const so = ix.sc >= 0 ? txt(r[ix.sc]) : ''   // ไม่มีคอลัมน์ = ตกลงคีย์เดียว → max ทั้ง item (เดิม)
    const rowMc = num(r[ix.actual])
    const rowBk = ix.booking >= 0 ? num(r[ix.booking]) : null
    let cur = byItem.get(k)
    if (!cur) {
      cur = { w, pool, item, bkKey: `${item}|${mc}|${g}`, bk: rowBk, bySo: new Map() }
      byItem.set(k, cur)
    } else if (rowBk != null) cur.bk = Math.max(cur.bk || 0, rowBk)
    cur.bySo.set(so, Math.max(cur.bySo.get(so) || 0, rowMc))
  }
  const used = {}   // [w@@pool] → { normal, poly, cotton }
  for (const e of byItem.values()) {
    let mcTotal = 0
    for (const v of e.bySo.values()) mcTotal += v
    const bk = e.bk != null ? e.bk : num(bookingMc?.[e.w]?.[e.bkKey])
    const net = Math.max(0, mcTotal - bk)
    if (!net) continue
    const rsv = ava?.[e.w]?.[e.pool]?.reserved
    const t = itemPoolType(e.item)
    const kind = (t === 'poly' && rsv?.poly > 0) ? 'poly'
      : (t === 'cotton' && rsv?.cotton > 0) ? 'cotton' : 'normal'
    const slot = used[e.w + '@@' + e.pool] || (used[e.w + '@@' + e.pool] = { normal: 0, poly: 0, cotton: 0 })
    slot[kind] += net
  }

  return (row, week) => {
    const w = String(week)
    const pool = poolKeyOf(row[ix.cat], row[ix.gauge], row[ix.mc])
    const av = ava?.[w]?.[pool]
    if (!av) return null                       // ไม่มีข้อมูลเครื่องของสัปดาห์นี้ → ไม่เดา
    const u = used[w + '@@' + pool] || { normal: 0, poly: 0, cotton: 0 }
    const rsv = av.reserved
    const t = itemPoolType(row[ix.item])
    if (t === 'poly' && rsv?.poly > 0) return (rsv.poly || 0) - (rsv.poly_used || 0) - u.poly
    if (t === 'cotton' && rsv?.cotton > 0) return (rsv.cotton || 0) - (rsv.cotton_used || 0) - u.cotton
    return num(av.remain) - u.normal
  }
}

/**
 * คำนวณลูกโซ่ carry/setup ใหม่ทั้ง run ที่ได้รับผลกระทบ
 *
 * @param columns  หัวคอลัมน์ของ grid
 * @param rows     แถวทั้งหมด (ไม่ใช่แค่ที่ผ่านตัวกรอง — ลูกโซ่ต้องเห็นครบทุกสัปดาห์)
 * @param opts.chains   Set ของคีย์ลูกโซ่ที่ต้องคำนวณใหม่ (chainKeyOf)
 * @param opts.baseMc   จำนวนเครื่องตั้งต้นจากไฟล์แผน (array ขนานกับ rows) — ใช้เป็นพื้น ไม่ให้เลขบานปลาย
 *                      เวลาลากไป-กลับ
 * @param opts.workDays (mc, gauge, week) → วันทำงานจริงของสัปดาห์นั้น
 * @param opts.weekActive (week) → สัปดาห์นั้นนับเป็นช่องบนแกนไหม (false = ถูกยุบ/โรงปิด)
 * @param opts.bookingMc  { week: { 'ITEM|MC|GAUGE': เครื่อง } } เครื่องของ booking ที่ทอไอเทมนี้อยู่
 * @param opts.warmSeed   เครื่องอุ่นจากนอกแผน (makeWarmSeed — สร้างจากไฟล์ตอนโหลด)
 * @param opts.setupDaysOf (row) → setup ต่อเครื่อง (วัน)
 * @returns { rows, changes } — rows ชุดใหม่ (คัดลอกเฉพาะแถวที่เปลี่ยน), changes = รายการที่ตัวเลขขยับ
 */
export function recalcChains(columns, rows, opts = {}) {
  const {
    chains, baseMc = [], workDays, weekActive = () => true,
    bookingMc = {}, warmSeed = {}, setupDaysOf, groupMap = null,
    ava = null, poolMap = {},
  } = opts
  const out = rows
  const changes = []
  if (!chains || !chains.size || !canRecalc(columns) || typeof workDays !== 'function') {
    return { rows: out, changes }
  }
  const ix = colIx(columns)
  const setupOf = setupDaysOf || makeSetupDays(columns, rows)

  // แกนสัปดาห์คิดจากทุกแถวในชีท (ไม่ใช่แค่ลูกโซ่ที่แก้) ให้ช่วงห่างเทียบกันได้ทั้งแผน
  const weeksWithJobs = new Set()
  for (const r of rows) {
    const w = rowWk(r, ix)
    if (hasWk(w)) weeksWithJobs.add(w)
  }
  const axis = makeAxis(weeksWithJobs, weekActive)
  // เครื่องว่างต่อ (สัปดาห์ × พูล) จากแถวชุดปัจจุบัน (หลังลาก) — ใช้ตัดสินเครื่องอุ่นข้ามสัปดาห์ที่เว้น
  // ไม่มี ava (ยังโหลดไม่เสร็จ) = ไม่รู้เครื่องว่าง → ไม่จำกัด (พฤติกรรมเดิม)
  const freeAt = ava ? makeFreeAt(columns, rows, { ava, poolMap, bookingMc }) : () => null

  // จัดแถวของแต่ละลูกโซ่ลงตะกร้า (ลูกโซ่ → สัปดาห์ → แถว)
  // เครื่องกลุ่มไหนอยู่ก้อนเดียวกัน — ใช้รวมเครื่อง booking ของทั้งก้อน (backend ส่ง booking
  // มาเป็นชื่อกลุ่มจริง ถ้าดูแค่กลุ่มของแถวจะมองไม่เห็นเครื่องอุ่นที่อยู่กลุ่มพี่น้อง)
  const poolMembers = new Map()          // "poolKey|gauge" → [MC_GROUP...]
  if (groupMap) {
    for (const k in groupMap) {
      const parts = k.split('|')                     // CAT|GAUGE|MC
      if (parts.length < 3) continue
      const mk = `${groupMap[k]}|${gz(parts[1])}`
      const arr = poolMembers.get(mk) || []
      if (!arr.includes(parts[2])) arr.push(parts[2])
      poolMembers.set(mk, arr)
    }
  }

  const byChain = new Map()
  rows.forEach((row, i) => {
    const key = chainKeyOf(row, ix, groupMap)
    if (!chains.has(key)) return
    if (isPassenger(row, ix)) return
    const w = rowWk(row, ix)
    if (!hasWk(w)) return
    const chain = byChain.get(key) || new Map()
    const cell = chain.get(w) || { w, idxs: [] }
    cell.idxs.push(i)
    chain.set(w, cell)
    byChain.set(key, chain)
  })

  const rowsOut = rows.slice()
  let touched = false

  for (const [key, chain] of byChain) {
    // คีย์เครื่อง booking = ITEM|MC_GROUP|GAUGE (ชื่อกลุ่มจริง) — ถ้าก้อนนี้มีหลายกลุ่ม
    // ต้องรวมทุกกลุ่มในก้อน เพราะ booking ของ item เดียวกันอาจอยู่กลุ่มพี่น้อง
    const firstRow = rows[[...chain.values()][0].idxs[0]]
    const bkGauge = gz(firstRow[ix.gauge])
    const bkItem = up(firstRow[ix.item])
    const bkMembers = groupMap
      ? (poolMembers.get(`${groupKeyOf(firstRow, ix, groupMap)}|${bkGauge}`) || [up(firstRow[ix.mc])])
      : [up(firstRow[ix.mc])]
    const bkKeys = bkMembers.map(m => `${bkItem}|${m}|${bkGauge}`)
    const bkAt = (w) => bkKeys.reduce((s, k) => s + num(bookingMc?.[String(w)]?.[k]), 0)
    // เพดาน carry ข้าม "สัปดาห์ที่เว้น" — ต้องตรงกับ _carry_cap_by_gap() ใน Planning.py
    // เครื่องยังอุ่นข้าม gap ได้ก็ต่อเมื่อจอดว่างอยู่จริงตลอดสัปดาห์ที่ไม่ได้ผลิต ถ้าสัปดาห์กลาง
    // พูลว่างเหลือน้อยกว่าเครื่องที่จะ carry = เครื่องชุดนั้นถูก item อื่นยึดไปแล้ว → ต้อง setup ใหม่
    // (เคสจริง F100145B0 SBP 26: W45 → W48 ห่าง 3 แต่ W46 พูลเต็ม — Planning ให้ setup 2
    //  แต่หน้าเว็บเคยคิดเป็น carry 2 เพราะเช็กแค่ระยะห่าง ≤ 3)
    // สัปดาห์ที่ item นี้เองยังผลิตอยู่ → ข้าม (เครื่องไม่ได้ถูกปล่อย) · ไม่มีข้อมูลเครื่องว่าง → ไม่เดา
    // คืน null = ไม่มีสัปดาห์กลาง (ติดกัน/สัปดาห์เดียวกัน) → ไม่จำกัด
    const gapCap = (row, fromIdx, toIdx) => {
      if (fromIdx == null || toIdx == null || toIdx - fromIdx <= 1) return null
      let cap = null
      for (let k = fromIdx + 1; k < toIdx; k++) {
        const w = axis.weekAt(k)
        if (w == null || chain.has(w) || bkAt(w) > 0) continue
        const f = freeAt(row, w)
        if (f == null) continue
        cap = cap == null ? f : Math.min(cap, f)
        if (cap <= 0) return 0
      }
      return cap
    }
    const cells = [...chain.values()].sort((a, b) => {
      const ia = axis.idxOf(a.w), ib = axis.idxOf(b.w)
      return (ia == null || ib == null) ? weekSortKey(a.w) - weekSortKey(b.w) : ia - ib
    })
    let prev = null                                      // { idx, total } สัปดาห์ก่อนหน้าของลูกโซ่นี้

    for (const cell of cells) {
      const first = rows[cell.idxs[0]]
      const mc = txt(first[ix.mc]), gauge = txt(first[ix.gauge])
      const wd = Number(workDays(mc, gauge, cell.w)) || 0
      const setupPer = Number(setupOf(first)) || 0
      const wIdx = axis.idxOf(cell.w)

      // เครื่องอุ่นที่รับช่วงต่อได้ = ของแผนเอง (สัปดาห์ก่อนหน้าในลูกโซ่ ห่าง ≤ 3) หรือของ booking
      // ที่ทอไอเทมนี้อยู่บนเครื่องเดียวกัน (สัปดาห์นี้ หรือย้อนหลังไม่เกิน 3 สัปดาห์)
      let carryAvail = 0
      if (prev && wIdx != null && prev.idx != null && (wIdx - prev.idx) <= SETUP_GAP_WEEK) {
        carryAvail = prev.total
        const cap = gapCap(first, prev.idx, wIdx)
        if (cap != null) carryAvail = Math.min(carryAvail, Math.max(0, cap))
      } else if (!prev) {
        // (ไม่ตรวจสัปดาห์ที่เว้น — Planning ก็ไม่ตรวจย้อนหลังให้ carry ที่มาจากนอกแผน)
        // สัปดาห์แรกของลูกโซ่ — ใช้เครื่องที่วิ่งอยู่แล้วจากนอกแผน (booking/แผนรอบก่อน) ได้
        // ถ้ายังอยู่ในระยะ 3 สัปดาห์จากตำแหน่งเดิมของมัน (ลากไปไกลกว่านั้น = เครื่องเย็นแล้ว)
        const seed = warmSeed[key]
        const sIdx = seed ? axis.idxOf(seed.week) : null
        if (seed && sIdx != null && wIdx != null && wIdx >= sIdx && (wIdx - sIdx) <= SETUP_GAP_WEEK) {
          carryAvail = seed.mc
        }
      }
      // เครื่องของ booking ที่ยังอุ่น = ของ "สัปดาห์นี้" ถ้ามี ไม่งั้นของสัปดาห์ก่อนหน้าที่ใกล้ที่สุด
      // ⚠️ ห้ามใช้ max ย้อนหลังหลายสัปดาห์ — booking ปล่อยเครื่องคืนได้ระหว่างทาง เช่น
      // F4SYNTREL247/1A0 RAP98: booking W41=2 → W42=1 → W43=0 เครื่องที่ยังอุ่นที่ W43 คือ 1 (ของ W42)
      // ไม่ใช่ 2 ของ W41 ที่ถูกปล่อยไปแล้ว (ถ้าใช้ max จะ carry เกิน → นับ job setup ขาด)
      let bkWarm = bkAt(cell.w)
      let bkBack = 0
      for (let back = 1; !bkWarm && back <= SETUP_GAP_WEEK; back++) {
        bkWarm = bkAt(shiftWk(cell.w, -back))           // ย้อนข้ามปีได้ (W1 → W52 ปีก่อน)
        bkBack = back
      }
      // booking หยุดไปแล้วเว้นสัปดาห์ → ผ่านเพดานเดียวกัน (_warm_mc_prev_week ใน Planning.py)
      if (bkWarm && bkBack >= 2) {
        const cap = gapCap(first, axis.idxOf(shiftWk(cell.w, -bkBack)), wIdx)
        if (cap != null) bkWarm = Math.min(bkWarm, Math.max(0, cap))
      }
      carryAvail = Math.max(carryAvail, bkWarm)

      // ยอดที่ต้องผลิตสัปดาห์นี้ → แปลงเป็น "เครื่อง-วัน" ด้วยกำลังผลิตต่อเครื่องต่อวัน
      //
      // ⚠️ หลาย SC ของ item เดียวกันในสัปดาห์เดียวกัน = ใช้ "เครื่องชุดเดียวกัน" แบ่งเวลากัน
      // Planning.py เขียน ACTUAL_MC/CARRYOVER_MC/NEW_MC ค่าเดียวกันซ้ำทุกแถว (ไม่ใช่แยกเครื่องต่อแถว)
      // เช่น FD5PRTJJ20/37A0 W35: 2 แถว mc=7 ทั้งคู่ ผลิตรวม 10,521 กก. = 7 เครื่อง (ไม่ใช่ 14)
      // → เครื่องตั้งต้นใช้ max ของกลุ่ม และเขียนผลลัพธ์กลับเท่ากันทุกแถว
      let qty = 0, daily = 0, base = 0
      for (const i of cell.idxs) {
        qty += num(rows[i][ix.qty])
        daily = Math.max(daily, num(rows[i][ix.daily]))
        base = Math.max(base, Math.round(num(baseMc[i] != null ? baseMc[i] : rows[i][ix.actual])))
      }
      const needDays = daily > 0 ? qty / daily : 0
      const total = minMachines(needDays, carryAvail, wd, setupPer, base)
      const carryTot = Math.min(total, carryAvail)
      const newTot = total - carryTot
      const availDays = newTot > 0 ? newRunDays(wd, setupPer) : wd

      for (const i of cell.idxs) {
        const before = {
          mc: num(rows[i][ix.actual]), carry: num(rows[i][ix.carry]), neu: num(rows[i][ix.neu]),
        }
        const after = { mc: total, carry: carryTot, neu: newTot }
        if (before.mc === after.mc && before.carry === after.carry && before.neu === after.neu) continue
        const row = rowsOut[i].slice()
        if (ix.actual >= 0) row[ix.actual] = total
        if (ix.carry >= 0) row[ix.carry] = carryTot
        if (ix.neu >= 0) row[ix.neu] = newTot
        if (ix.setup >= 0) row[ix.setup] = Math.round(setupPer * newTot * 100) / 100
        // วันผลิตที่เหลือหลังหักวัน setup — สูตรเดียวกับ prod_days_new/prod_days_old ใน Planning.py
        if (ix.avail >= 0) row[ix.avail] = availDays
        rowsOut[i] = row
        touched = true
        changes.push({
          idx: i, week: cell.w, item: txt(first[ix.item]), mcGroup: mc,
          factory: ix.factory >= 0 ? txt(rows[i][ix.factory]) : '',
          cat: ix.cat >= 0 ? txt(rows[i][ix.cat]) : '',
          before, after,
        })
      }
      prev = { idx: wIdx, total }
    }
  }

  return { rows: touched ? rowsOut : out, changes }
}

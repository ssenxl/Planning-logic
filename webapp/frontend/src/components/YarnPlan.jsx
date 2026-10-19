import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { weekKey, wkLabel, currentPlanWeek } from '../weekKeys.js'

/* หน้า แผนด้าย — แผนทอใช้ด้ายอะไร สัปดาห์ไหน เท่าไหร่ มาจากไหน เหลือเท่าไหร่
   (user ขอ 15 ต.ค. 2569: "ควรเป็นแบบแผนทอที่เห็นเลยว่า week นี้เราจะใช้เส้นด้ายอะไรบ้าง ใช้เท่าไหร่ ... ต้องมาจากการทำแผนทอ")
   เดิมเป็นโหมดสลับในหน้าแผนผลิต → user ขอแยกเป็นหน้าของตัวเอง (แท็บ yarn-plan ใน App.jsx) วันเดียวกัน
   ตัวเลขทั้งหมดคิดที่ backend (yarn_plan.py) จากชีท PLAN ของไฟล์แผนล่าสุด — หน้านี้จัดวาง/กรอง/ล็อกอย่างเดียว

   16 ต.ค. 2569 user: "มันไม่สวยและใช้งานยาก คำพูดดูเป็น AI ไป" + "ด้านบนอยากให้มีกราฟว่าแต่ละ week ใช้ด้ายเท่าไหร่ ด้ายอะไรบ้าง"
   → กราฟ: ซ้ายยอดด้ายรวมรายสัปดาห์ · ขวาด้ายที่ใช้ในสัปดาห์ที่เลือก (คลิกชื่อด้าย = กราฟ + ตารางเหลือด้ายนั้น)
   → ตารางด้ายละ 2 แถว (ใช้ / คงเหลือ) กดชื่อด้ายเปิดที่มา (user เลือกแบบนี้) · ตัดอีโมจิ ย่อคำ สูตรย้ายไปอยู่ในคำอธิบายตอนชี้

   ที่มาของด้าย: ใช้สต๊อก · ใช้ PO · ไม่พอ · ล็อก SC · ล็อก Item · รอ PO ของใบ · ออก KP แล้ว + PO เข้า
   ใช้สต๊อก / ใช้ PO / ไม่พอ = ที่แผนดึงจากกองกลาง แตกตามลำดับ: ของในคลังก่อน หมดแล้วใช้ PO ที่เข้ามาแล้ว ที่เหลือ = ไม่พอ
   (เดิมแถวเดียว "เบิกสต๊อก" — user 16 ต.ค. 2569 งงว่า "stock มีแค่นี้ แต่ทำไมถึงใช้ได้มากกว่า stock")
   สต๊อกคาดการณ์ (เดิมชื่อ คงเหลือ) = ยอดใช้ได้ (yarn_ava_live หลังหักล็อก) + PO Pending ที่เข้ามาสะสม − ที่แผนดึงจากกองกลางสะสม
   — PO เข้าแล้วก็กลายเป็นสต๊อก ; ช่อง "สต๊อกใช้ได้" โชว์ยอดในคลังด้วย ชี้ดูว่าหักอะไรไปบ้าง (user 16 ต.ค. 2569)
   ด้ายที่ต้องคุม: แผนวางไม่เกินด้ายที่มี (YarnStockCover.supply_cap) จึงไม่ควรติดลบ — ติดลบ (แดง) = ข้อมูลเปลี่ยนหลังรันแผน
   ด้ายในประเทศ (internal ; y.src = Internal นันยางผลิตเอง / Local ผู้ขายไทย): ไม่คุม — ติดลบ (ส้ม) = ต้องสั่งเพิ่ม
   (user 15 ต.ค. 2569 นันยาง → 16 ต.ค. ขยายเป็น Internal + Local "เรียกใช้ได้เลย เขาหาด้ายมาให้เราได้")
   คลิกช่อง → รายการ SC + ผ้าที่ใช้ด้ายนั้นในสัปดาห์นั้น + ล็อก/ปลดด้าย (สิทธิ์ yarn / admin)
   ปุ่มกรอง "หลาย lot" + ป้าย "ใช้ N lot" ใต้ชื่อด้าย → แผง lot (LotPanel) : lot ไหนไปที่ SC / Item ไหน
   + เลือก lot เองด้วยปุ่มล็อก lot ให้ SC ตัวเดิม (user 16 ต.ค. 2569)
   แถว SC / Item ใต้แถว ใช้ (ตอนเปิดที่มา) + ชี้ช่อง lot / เลข PO บอกว่าไปที่ SC / Item ไหน
   (user 16 ต.ค. 2569: "ต้องบอกด้วยว่าเอาไปทำให้ Item อะไร SC อะไร") */

const PAGE = 120         // ด้ายต่อครั้ง (ปกติด้ายละ 2 แถว)
const SUBS = [
  ['from_stk', 'ใช้สต๊อก', 'ใช้ด้ายในคลัง (สต๊อกใช้ได้) ใช้ของในคลังก่อน หมดแล้วค่อยใช้ PO ที่เข้ามา '
    + '(รวมใบที่แผนให้ยืมสต๊อกแทนรอ PO)'],
  ['from_po', 'ใช้ PO', 'ของในคลังหมดแล้ว ใช้ด้ายจาก PO ที่เข้ามาแล้ว (แถว PO เข้า)'],
  ['gap', 'ไม่พอ', 'ใช้เกินของในคลัง + PO ที่เข้ามาถึงสัปดาห์นั้น'],
  ['lock', 'ล็อก SC', 'ด้ายที่ล็อกไว้ให้ SC นั้น'],
  ['ilock', 'ล็อก Item', 'ด้ายที่ล็อกไว้ให้ Item ผ้านั้น (หน้า Monitor Yarn) ทุกใบของผ้านี้ใช้ร่วมกัน ผ้าอื่นใช้ไม่ได้'],
  ['po', 'รอ PO ของใบ', 'ใบนี้มี PO ด้ายของตัวเองที่ยังไม่เข้า'],
  ['kp', 'ออก KP แล้ว', 'ออกใบ KP แล้ว ด้ายถูกจอง/โอนไปแล้ว'],
  ['inbound', 'PO เข้า', 'PO ด้ายค้างส่งที่จะเข้ามาเติม นับที่วันเข้า + สัปดาห์รอ (เลยกำหนดแล้วนับเป็นสัปดาห์หน้า) '
    + 'ไม่รวม PO ที่ใบจองไว้เอง'],
]
// แถวชื่อ lot / เลข PO ใต้แถวที่มา : ที่มา → [ช่องข้อมูล, ชื่อแถว, คำอธิบายชื่อแถว, หัวคำอธิบายตอนชี้ช่อง, ช่องของใบใน d.rows]
// (lock / ilock = lot ที่คนเลือกล็อกเอง — user 16 ต.ค. 2569 "คนต้องสามารถ config ได้ด้วย ว่าอยากให้ใช้ lot ไหน")
const NAMES = {
  from_stk: ['lot_w', 'lot', 'lot ที่ใช้ในสัปดาห์นั้น — แผนให้ 1 ใบใช้ lot เดียวเท่าที่ทำได้ (lot เก่าสุดที่พอทั้งใบ) '
    + '(ชี้ดู kg / อายุ / SC ที่ได้ ; รวม = จำนวน lot)', 'ใช้ lot', 'lots'],
  from_po: ['po_w', 'เลข PO', 'PO ที่ใช้ในสัปดาห์นั้น เข้าก่อนใช้ก่อน (ชี้ดูผู้ขาย / วันกำหนดเข้า / SC ที่ได้ ; รวม = จำนวน PO)',
    'ใช้ PO (เข้าก่อนใช้ก่อน)', 'pos'],
  lock: ['lock_w', 'lot', 'lot ที่ล็อกให้ SC (ปุ่ม "ล็อก lot ให้ใบนี้" ในแผงตอนคลิกตัวเลข) ; รวม = จำนวน lot', 'lot ที่ล็อกให้ SC',
    'lock_lots'],
  ilock: ['ilock_w', 'lot', 'lot ที่ล็อกให้ Item ผ้า (หน้า Monitor Yarn) ; รวม = จำนวน lot', 'lot ที่ล็อกให้ Item', 'ilock_lots'],
}
// แถว SC / Item : ช่องละกี่ใบ (ใช้มากก่อน) · คำอธิบายตอนชี้บอกได้กี่ใบ (ที่เหลือคลิกดูในแผง)
const SC_SHOW = 2
const SC_TIP_MAX = 25
// lot ที่แผนให้ใบใช้ (d.rows) : ใช้สต๊อก · ล็อก SC · ล็อก Item — ส่วนที่ใช้ PO / รอ PO / ออก KP แล้ว ไม่มี lot
const LOT_FIELDS = ['lots', 'lock_lots', 'ilock_lots']
const LOT_KIND = { lots: '', lock_lots: 'ล็อก SC', ilock_lots: 'ล็อก Item' }
const CH = 170           // ความสูงพื้นที่แท่งของกราฟ (px)

const kg = (n) => (n == null ? '-' : Math.round(n).toLocaleString())
const kg1 = (n) => (n == null ? '-' : n.toLocaleString(undefined, { maximumFractionDigits: 1 }))
const yname = (y) => y.yarn + (y.color ? ` สี ${y.color}` : '')
// ยอดใช้ได้มาจากไหน (backend yarn_plan → stk) — โชว์เฉพาะที่หักจริง
const STK_CUT = [['small', 'ลูกเล็ก'], ['own', 'สงวนให้ลูกค้า'], ['kp', 'ใบ KP จองแต่ยังไม่โอน'], ['lock', 'ล็อก']]
const LOT_TIP_MAX = 15
const stkTip = (s, lots) => [`ในคลัง ${kg(s.stock)} kg`,
  ...STK_CUT.filter(([f]) => s[f] > 0.5).map(([f, t]) => `− ${t} ${kg(s[f])}`),
  `= ใช้ได้ ${kg(s.usable)} kg`,
  ...(lots?.length ? ['', 'lot ที่ใช้ได้ (เก่า → ใหม่):',
    ...lots.slice(0, LOT_TIP_MAX).map(([l, v, a]) => `lot ${lotName(l)}  ${kg(v)} kg` + (a != null ? ` · อายุ ${a} วัน` : '')),
    ...(lots.length > LOT_TIP_MAX ? [`… อีก ${lots.length - LOT_TIP_MAX} lot`] : [])] : [])].join('\n')
const ykey = (y) => `${y.yarn}|${y.color || ''}`
// ใช้ lot ไหน / PO ไหน (backend yarn_plan._trace — user 16 ต.ค. 2569 "ต้องบอกด้วยว่าเป็น lot อะไรใช้ lot ไหน
// ทั้งส่วนของ stock และ PO pending") ; lot ว่าง = ยอดใช้ได้ที่ไม่รู้ lot
function lotName(l) { return l || 'ไม่ทราบ lot' }
const lotLabel = (y, l) => {
  const a = (y.lots || []).find(x => x[0] === l)?.[2]
  return `lot ${lotName(l)}` + (a != null ? ` (อายุ ${a} วัน)` : '')
}
// PO: เลข · ผู้ขาย · วันกำหนดเข้า (เลยกำหนด) — y.pom = {PO: [ผู้ขาย, วันกำหนดเข้า, เลยกำหนด]}
const poLabel = (y, p) => {
  const m = y.pom?.[p]
  return `PO ${p}` + (m ? (m[0] ? ` · ${m[0]}` : '') + (m[1] ? ` · กำหนดเข้า ${m[1]}` : '') + (m[2] ? ' (เลยกำหนด)' : '') : '')
}
// [[ชื่อ, kg]] → คำอธิบายตอนชี้ บรรทัดละรายการ
const listTip = (head, items, label) =>
  items?.length ? [head, ...items.map(([x, v]) => `${label(x)}  ${kg1(v)} kg`)].join('\n') : undefined
const sumKg = (a) => (a || []).reduce((s, [, v]) => s + v, 0)
// ด้ายที่ใบ (SC + ผ้า) ใช้ในสัปดาห์นั้น ทุกที่มา
const rowKg = (r) => r.stock + r.lock + (r.ilock || 0) + r.po + r.kp
// ใบนี้ได้ด้ายจากไหน บรรทัดเดียว (คำอธิบายตอนชี้แถว SC / Item) — ชุดเดียวกับชิปในแผงช่อง
const srcOf = (y, r) => [
  ...(r.lock_lots?.length ? r.lock_lots.map(([l, v]) => `lot ${lotName(l)} (ล็อก SC) ${kg1(v)}`)
    : r.lock > 0 ? [`ล็อก SC ${kg1(r.lock)}`] : []),
  ...(r.ilock_lots?.length ? r.ilock_lots.map(([l, v]) => `lot ${lotName(l)} (ล็อก Item) ${kg1(v)}`)
    : r.ilock > 0 ? [`ล็อก Item ${kg1(r.ilock)}`] : []),
  ...(r.lots === undefined ? (r.stock > 0 ? [`ใช้สต๊อก/PO ${kg1(r.stock)}`] : [])
    : r.lots.map(([l, v]) => `lot ${lotName(l)} ${kg1(v)}`)),
  ...(r.pos || []).map(([p, v]) => `PO ${p} ${kg1(v)}`),
  ...(r.gap > 0 ? [`${y.internal ? 'ต้องสั่งเพิ่ม' : 'ไม่พอ'} ${kg1(r.gap)}`] : []),
  ...(r.po > 0 ? [`รอ PO${r.po_nos?.length ? ' ' + r.po_nos.join(', ') : ''} ${kg1(r.po)}`] : []),
  ...(r.kp > 0 ? [`ออก KP แล้ว ${kg1(r.kp)}`] : []),
].join(' · ')
const kshort = (n) => (n >= 1000 ? (n / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 }) + 'K' : String(Math.round(n)))

// "16/10 08:06" — เวลาไฟล์แผน (วินาที) / เวลาสต๊อกด้าย (ข้อความ ISO)
function dt(v) {
  if (v == null || v === '') return ''
  const t = typeof v === 'number' ? new Date(v * 1000) : new Date(String(v).replace(' ', 'T'))
  if (Number.isNaN(t.getTime())) return String(v)
  const p = (n) => String(n).padStart(2, '0')
  return `${p(t.getDate())}/${p(t.getMonth() + 1)} ${p(t.getHours())}:${p(t.getMinutes())}`
}

// สัปดาห์ปัจจุบัน {ปี, เลข} ตามนิยามศุกร์–พฤหัส — ปีของสัปดาห์ = ปีของวันพฤหัสในสัปดาห์ ISO นั้น
function thisWeek() {
  const d = new Date()
  d.setDate(d.getDate() + 3)
  d.setDate(d.getDate() - ((d.getDay() + 6) % 7) + 3)
  return { y: d.getFullYear(), w: currentPlanWeek() }
}

// ขั้นแกน y เป็นเลขกลม ๆ (1 / 2 / 2.5 / 5 × 10^n) ราว 4 ขั้น ขั้นบนสุด ≥ ค่าสูงสุด
function ticksOf(max) {
  if (!(max > 0)) return [0, 1]
  const raw = max / 4
  const pow = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 2.5, 5, 10].map(m => m * pow).find(s => s >= raw)
  return Array.from({ length: Math.ceil(max / step - 1e-9) + 1 }, (_, i) => i * step)
}

// active = แท็บนี้กำลังโชว์อยู่ (App.jsx ค้างหน้าไว้หลังเปิดครั้งแรก) — ตอนซ่อนไม่คิดใหม่ตามข่าว จดไว้คิดตอนกลับมา
export default function YarnPlan({ sheet = '', canLock = false, active = true }) {
  const [d, setD] = useState(null)
  const [locks, setLocks] = useState([])
  const [err, setErr] = useState('')
  const [loading, setLoading] = useState(false)
  const [q, setQ] = useState('')
  const [only, setOnly] = useState('all')     // all | short (ด้ายไม่พอ) | need (ด้ายนันยางผลิต ต้องสั่งเพิ่ม) | multi (แผนใช้มากกว่า 1 lot)
  const [pick, setPick] = useState(null)      // ด้ายที่คลิกจากกราฟ (ykey) → กราฟ + ตารางเหลือด้ายนั้น
  const [sortBy, setSortBy] = useState('name')
  const [open, setOpen] = useState(() => new Set())   // ด้ายที่เปิดดูที่มา (ykey)
  const [limit, setLimit] = useState(PAGE)
  const [sel, setSel] = useState(null)        // {y, w} ช่องที่เปิดรายละเอียด
  const [lotSel, setLotSel] = useState(null)  // ด้าย (index) ที่เปิดแผง lot
  const [dlg, setDlg] = useState(null)        // {y, so, fab} กำลังล็อก

  async function load() {
    setLoading(true); setErr('')
    try {
      const [a, l] = await Promise.all([api.yarnPlan(sheet), api.yarnLocks().catch(() => null)])
      setD(a); setLocks(l?.locks || [])
    } catch (e) { setErr('โหลดแผนด้ายไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }
  // แผนถูกลาก/แก้ (เซฟอัตโนมัติ), รันแผนรอบใหม่, มีคนล็อกด้าย, แก้ค่าหน้า Monitor Yarn → โหลดใหม่
  // หน่วงไว้นิดนึง: ลาก Gantt เซฟถี่ ๆ ไม่ต้องคิดใหม่ทุกครั้ง
  const timer = useRef(null)
  const activeRef = useRef(active)
  activeRef.current = active
  const stale = useRef(false)          // มีข่าวเข้ามาตอนหน้าซ่อนอยู่ → คิดใหม่ตอนกลับมาเปิด
  useEffect(() => {
    load()
    const soon = () => {
      if (!activeRef.current) { stale.current = true; return }
      clearTimeout(timer.current); timer.current = setTimeout(load, 1500)
    }
    const offs = [live.subscribe(live.RES.plan(sheet || 'PLAN'), soon), live.subscribe(live.RES.run, soon),
      live.subscribe(live.RES.yarnLock, soon), live.subscribe(live.RES.yarnItemLock, soon),
      live.subscribe(live.RES.planConfig, soon)]
    return () => { clearTimeout(timer.current); offs.forEach(f => f()) }
  }, [sheet])
  useEffect(() => { if (active && stale.current) { stale.current = false; load() } }, [active])

  const weeks = d?.weeks || []
  const yarns = d?.yarns || []
  const wlabels = useMemo(() => weeks.map(w => wkLabel(weekKey(w.w, w.y))), [weeks])
  const cur = useMemo(() => { const t = thisWeek(); return weeks.findIndex(w => w.y === t.y && w.w === t.w) }, [weeks])

  const isShort = (y) => y.short != null && !y.internal
  const isNeed = (y) => y.short != null && y.internal

  // lot ที่แผนให้แต่ละด้ายใช้ ทุกสัปดาห์ ทุกใบ — ชุดเดียวกับแผง lot (ปุ่มกรอง "หลาย lot" / ป้าย "ใช้ N lot")
  const lotsOf = useMemo(() => {
    const m = yarns.map(() => new Set())
    for (const r of d?.rows || []) for (const f of LOT_FIELDS) for (const [l] of r[f] || []) m[r.y]?.add(l)
    return m
  }, [yarns, d])
  const isMulti = (i) => lotsOf[i]?.size > 1

  // ใบที่ใช้ด้ายแต่ละช่อง "ด้าย|สัปดาห์" ใช้มากก่อน — แถว SC / Item + คำอธิบายตอนชี้แถว lot / เลข PO
  const cellRows = useMemo(() => {
    const m = new Map()
    for (const r of d?.rows || []) {
      const k = r.y + '|' + r.w
      if (!m.has(k)) m.set(k, [])
      m.get(k).push(r)
    }
    for (const a of m.values()) a.sort((x, z) => rowKg(z) - rowKg(x))
    return m
  }, [d])

  // คำค้นตรงชื่อด้าย หรือ SC / ผ้าที่ใช้ด้ายตัวนั้น
  const kw = q.trim().toLowerCase()
  const shown = useMemo(() => {
    let hit = null
    if (kw) {
      hit = new Set()
      yarns.forEach((y, i) => { if (yname(y).toLowerCase().includes(kw)) hit.add(i) })
      for (const r of d?.rows || []) if (`${r.so} ${r.fab}`.toLowerCase().includes(kw)) hit.add(r.y)
    }
    const idx = yarns.map((_, i) => i).filter(i => {
      const y = yarns[i]
      if (hit && !hit.has(i)) return false
      if (only === 'short' && !isShort(y)) return false
      if (only === 'need' && !isNeed(y)) return false
      if (only === 'multi' && !isMulti(i)) return false
      if (pick && ykey(y) !== pick) return false
      return true
    })
    const by = {
      name: (a, b) => yname(yarns[a]).localeCompare(yname(yarns[b])),
      use: (a, b) => yarns[b].total - yarns[a].total,
      short: (a, b) => (yarns[a].short ?? 1e9) - (yarns[b].short ?? 1e9) || yarns[b].total - yarns[a].total,
    }[sortBy]
    return idx.sort(by)
  }, [yarns, d, kw, only, pick, sortBy, lotsOf])

  // ล็อกที่ยังกันอยู่ ต่อ SC|ผ้า|ด้าย (ใช้ในแผงรายละเอียด)
  const lockMap = useMemo(() => {
    const m = new Map()
    for (const l of locks) {
      if (l.state !== 'active') continue
      const k = `${l.so}|${l.fabric}|${String(l.yarn).toUpperCase()}`
      if (!m.has(k)) m.set(k, [])
      m.get(k).push(l)
    }
    return m
  }, [locks])

  // PO ที่บรรทัดใบรออยู่ ต่อ "ด้าย|สัปดาห์" (คำอธิบายตอนชี้แถว รอ PO ของใบ)
  const waitPo = useMemo(() => {
    const m = new Map()
    for (const r of d?.rows || []) {
      if (!(r.po > 0) || !r.po_nos?.length) continue
      const k = r.y + '|' + r.w
      if (!m.has(k)) m.set(k, new Set())
      r.po_nos.forEach(p => m.get(k).add(p))
    }
    return m
  }, [d])

  const nShort = yarns.filter(isShort).length
  const nNeed = yarns.filter(isNeed).length
  const nMulti = yarns.filter((_, i) => isMulti(i)).length
  const page = shown.slice(0, limit)
  const allOpen = page.length > 0 && page.every(i => open.has(ykey(yarns[i])))
  const toggle = (k) => setOpen(s => { const n = new Set(s); if (n.has(k)) n.delete(k); else n.add(k); return n })
  const pickYarn = (k) => {
    setPick(k); setLimit(PAGE)
    if (k) setOpen(s => new Set(s).add(k))   // เลือกด้าย → เปิดที่มาให้เลย
  }
  const pickName = !pick ? '' : yname(yarns.find(y => ykey(y) === pick) || { yarn: pick.split('|')[0] })
  // แผงช่อง / แผง lot เปิดได้ทีละแผง (อยู่ตำแหน่งเดียวกัน)
  const pickCell = (v) => { setLotSel(null); setSel(v) }
  const openLots = (i) => { setSel(null); setLotSel(i) }
  // กรอง "หลาย lot" → เปิดที่มาให้เลย จะได้เห็นแถว lot
  const openMulti = () => setOpen(s => {
    const n = new Set(s)
    yarns.forEach((y, i) => { if (isMulti(i)) n.add(ykey(y)) })
    return n
  })

  const body = []
  for (const i of page) {
    const y = yarns[i]
    const k = ykey(y)
    const subs = SUBS.filter(([f]) => (y[f] || []).some(v => v > 0))
    const isOpen = open.has(k) && subs.length > 0
    // แถวชื่อ lot / เลข PO ใต้แถว ใช้สต๊อก / ใช้ PO / ล็อก (user เลือก 16 ต.ค. 2569: โชว์ในตาราง + ชี้ดู + แผงแยกต่อ SC)
    const named = isOpen ? subs.map(s => s[0]).filter(f => NAMES[f] && (y[NAMES[f][0]] || []).some(a => a.length)) : []
    const span = 2 + (isOpen ? subs.length + 1 : 0) + named.length   // + 1 = แถว SC / Item
    const flag = isShort(y) ? ' short' : isNeed(y) ? ' need' : ''
    const on = (wi) => (sel && sel.y === i && sel.w === wi ? ' on' : '')
    const cell = (v, wi, pre = '', tip) => (
      <td key={wi} className={'n yp-c' + (wi === cur ? ' cur' : '') + on(wi)} title={tip} onClick={() => pickCell({ y: i, w: wi })}>
        {v > 0 ? pre + kg(v) : ''}
      </td>
    )
    const rowsAt = (wi) => cellRows.get(i + '|' + wi) || []
    // ชี้ช่อง lot / เลข PO → แต่ละ lot / PO ไปที่ SC / Item ไหน เท่าไหร่
    const namesTip = (f, wi) => {
      const [w, , , head, rf] = NAMES[f]
      const items = y[w]?.[wi]
      if (!items?.length) return undefined
      const label = f === 'from_po' ? (p => poLabel(y, p)) : f === 'from_stk' ? (l => lotLabel(y, l)) : (l => `lot ${lotName(l)}`)
      return [head, ...items.flatMap(([x, v]) => [`${label(x)}  ${kg1(v)} kg`,
        ...rowsAt(wi).flatMap(r => (r[rf] || []).filter(p => p[0] === x).map(([, q]) => [r, q]))
          .sort((a, b) => b[1] - a[1]).map(([r, q]) => `   → SC ${r.so} · ${r.fab}  ${kg1(q)} kg`)])].join('\n')
    }
    // ชี้ช่องแถว SC / Item → ทุกใบของสัปดาห์นั้น + ลูกค้า + lot / PO ที่ได้
    const scTip = (wi) => {
      const rs = rowsAt(wi)
      if (!rs.length) return undefined
      return [`${wlabels[wi]} · ใช้ ${kg1(y.use[wi])} kg · ${rs.length} ใบ (ใช้มากก่อน)`,
        ...rs.slice(0, SC_TIP_MAX).flatMap(r => {
          const cus = d.orders[`${r.so}|${r.fab}`]?.cus
          return [`SC ${r.so} · ${r.fab}  ${kg1(rowKg(r))} kg` + (cus ? ` · ${cus}` : ''), `   ${srcOf(y, r)}`]
        }),
        ...(rs.length > SC_TIP_MAX ? [`… อีก ${rs.length - SC_TIP_MAX} ใบ (คลิกดูทั้งหมด)`] : [])].join('\n')
    }
    const names = (f) => {
      const [w, label, tip] = NAMES[f]
      const name = f === 'from_po' ? (p => p) : lotName
      return (
        <tr key={k + f + 'n'} className={'yp-sub yp-names ' + f}>
          <td className="yp-kind" title={tip}>{label}</td>
          {y[w].map((it, wi) => (
            <td key={wi} className={'n yp-c' + (wi === cur ? ' cur' : '') + on(wi)} title={namesTip(f, wi)}
              onClick={() => pickCell({ y: i, w: wi })}>
              {it.slice(0, 2).map(([x]) => <div key={x}>{name(x)}</div>)}
              {it.length > 2 && <div className="more">+{it.length - 2}</div>}
            </td>
          ))}
          <td className="n yp-tot">{new Set(y[w].flatMap(a => a.map(p => p[0]))).size || ''}</td>
        </tr>
      )
    }
    body.push(
      <tr key={k} className="yp-main">
        <td className={'yp-name' + flag} rowSpan={span} onClick={() => subs.length && toggle(k)}
          title={subs.length ? (isOpen ? 'ซ่อนที่มาด้าย' : 'ดูว่าด้ายมาจากไหน') : undefined}>
          <span className="yp-tog">{subs.length ? (isOpen ? '▾' : '▸') : ''}</span>
          <b>{y.yarn}</b>
          {(y.color || y.internal || y.short != null || isMulti(i)) && (
            <div className="yp-tags">
              {y.color && <span className="yp-tag color">สี {y.color}</span>}
              {y.internal && <span className="yp-tag internal"
                title={y.src === 'Local'
                  ? 'ด้าย Local (ผู้ขายในประเทศ หาด้ายมาให้ได้) แผนไม่คุมตามสต๊อก (ตั้งที่หน้า Monitor Yarn)'
                  : 'ด้าย Internal (นันยางผลิตเอง) แผนไม่คุมตามสต๊อก (ตั้งที่หน้า Monitor Yarn)'}>{y.src || 'Internal'}</span>}
              {isShort(y) && <span className="yp-tag short">ไม่พอ {wlabels[y.short]}</span>}
              {isNeed(y) && <span className="yp-tag need">{y.src === 'Local' ? 'สั่งซื้อก่อน' : 'สั่งผลิตก่อน'} {wlabels[y.short]}</span>}
              {isMulti(i) && (
                <button className="yp-tag lots" title="ดูว่าแต่ละ lot ไปที่ SC / Item ไหน และเลือก lot เอง"
                  onClick={e => { e.stopPropagation(); openLots(i) }}>ใช้ {lotsOf[i].size} lot</button>
              )}
            </div>
          )}
        </td>
        <td className="n yp-usable" rowSpan={span} title={y.stk ? stkTip(y.stk, y.lots) : 'ไม่มีด้ายนี้ในสต๊อก'}>
          {y.usable != null ? kg(y.usable) : y.stk ? '0' : '-'}
          {y.stk && <div className="yp-stk">ในคลัง {kg(y.stk.stock)}</div>}
        </td>
        <td className="yp-kind" title="kg ด้าย = ยอดทอของแผน × % ด้ายของบรรทัดใบ × สูญเสีย">ใช้</td>
        {y.use.map((v, wi) => cell(v, wi))}
        <td className="n yp-tot">{kg(y.total)}</td>
      </tr>,
    )
    if (isOpen) {
      const nOrd = new Set(y.use.flatMap((_, wi) => rowsAt(wi).map(r => r.so + '|' + r.fab))).size
      body.push(
        <tr key={k + 'sc'} className="yp-sub yp-scs">
          <td className="yp-kind" title={'SC / Item ที่ใช้ด้ายนี้ในสัปดาห์นั้น ใช้มากก่อน ช่องละ ' + SC_SHOW + ' ใบ '
            + '(ชี้ดูทุกใบ + lot / PO ที่ได้ ; คลิกเปิดแผง ; รวม = จำนวนใบทั้งแผน)'}>SC / Item</td>
          {y.use.map((_, wi) => {
            const rs = rowsAt(wi)
            return (
              <td key={wi} className={'n yp-c' + (wi === cur ? ' cur' : '') + on(wi)} title={scTip(wi)}
                onClick={() => pickCell({ y: i, w: wi })}>
                {rs.slice(0, SC_SHOW).map(r => <div key={r.so + '|' + r.fab}><b>{r.so}</b><span>{r.fab}</span></div>)}
                {rs.length > SC_SHOW && <div className="more">+{rs.length - SC_SHOW}</div>}
              </td>
            )
          })}
          <td className="n yp-tot">{nOrd || ''}</td>
        </tr>,
      )
      subs.forEach(([f, label, tip]) => {
        const pre = f === 'inbound' ? '+' : ''
        const sum = y[f].reduce((a, b) => a + b, 0)
        // ด้าย Internal/Local แผนไม่คุม → ไม่พอ = ต้องสั่งเพิ่ม (ส้ม) ; ด้ายที่คุม = ข้อมูลด้ายเปลี่ยนหลังรันแผน (แดง)
        const gapNeed = f === 'gap' && y.internal
        const tipOf = NAMES[f] ? (wi) => namesTip(f, wi) : {
          inbound: (wi) => listTip('PO ที่เข้า', y.in_w?.[wi], p => poLabel(y, p)),
          po: (wi) => {
            const s = waitPo.get(i + '|' + wi)
            return s ? ['PO ที่ใบรอ', ...[...s].map(p => poLabel(y, p))].join('\n') : undefined
          },
        }[f]
        body.push(
          <tr key={k + f} className={'yp-sub ' + f + (gapNeed ? ' need' : '')}>
            <td className="yp-kind" title={f !== 'gap' ? tip : tip + (gapNeed ? ' ต้องสั่งเพิ่ม' : ' (ข้อมูลด้ายเปลี่ยนหลังรันแผน)')}>
              {gapNeed ? 'ต้องสั่งเพิ่ม' : label}
            </td>
            {y[f].map((v, wi) => cell(v, wi, pre, tipOf?.(wi)))}
            <td className="n yp-tot">{sum > 0 ? pre + kg(sum) : ''}</td>
          </tr>,
        )
        if (named.includes(f)) body.push(names(f))
      })
    }
    body.push(
      <tr key={k + 'bal'} className="yp-bal">
        <td className="yp-kind" title={'สต๊อกที่คาดว่าจะมีปลายสัปดาห์นั้น ถ้า PO เข้าตามกำหนด (PO เข้าแล้วก็กลายเป็นสต๊อก)\n'
          + '= สต๊อกใช้ได้ + PO เข้า (สะสม) − ใช้สต๊อก − ใช้ PO − ไม่พอ (สะสม) ยังไม่รวม PO ที่ยังไม่ได้สั่ง'}>สต๊อกคาดการณ์</td>
        {y.bal.map((v, wi) => {
          const neg = v < -0.5
          const same = !neg && wi > 0 && Math.round(y.bal[wi - 1]) === Math.round(v)
          return (
            <td key={wi} className={'n yp-c' + (wi === cur ? ' cur' : '') + (neg ? (y.internal ? ' need' : ' neg') : same ? ' same' : '') + on(wi)}
              title={neg && y.internal ? `ต้องสั่ง${y.src === 'Local' ? 'ซื้อ' : 'ผลิต'}เพิ่ม ${kg(-v)} kg` : undefined}
              onClick={() => pickCell({ y: i, w: wi })}>{kg(v)}</td>
          )
        })}
        <td className="n yp-tot" />
      </tr>,
    )
  }

  return (
    <div className="yp">
      <div className="yp-head">
        <h2 className="yp-title">แผนด้าย <span>ด้ายที่แผนทอใช้ แยกรายสัปดาห์</span></h2>
        <span className="yp-meta" title={d?.plan?.name || ''}>
          {d?.plan ? `แผน ${dt(d.plan.mtime)}` : ''}{d?.stock_at ? ` · สต๊อกด้าย ${dt(d.stock_at)}` : ''}
        </span>
        <button className="yp-btn" onClick={load} disabled={loading}>{loading ? 'กำลังโหลด…' : 'โหลดใหม่'}</button>
      </div>
      {(err || d?.note) && <div className="yp-note">{err || d.note}</div>}

      {yarns.length > 0 && wlabels.length > 0 && (
        <UseChart yarns={yarns} wlabels={wlabels} cur={cur} pick={pick}
          onPick={k => pickYarn(pick === k ? null : k)} />
      )}

      <div className="yp-tools">
        <input className="yp-search" value={q} onChange={e => { setQ(e.target.value); setLimit(PAGE) }}
          placeholder="ค้นหาด้าย / SC / ผ้า" />
        <div className="yp-seg" role="group" aria-label="กรองด้าย">
          {[['all', 'ทั้งหมด', yarns.length], ['short', 'ไม่พอ', nShort], ['need', 'ต้องสั่งผลิต', nNeed],
            ['multi', 'หลาย lot', nMulti, 'ด้ายที่แผนใช้มากกว่า 1 lot (รวม lot ที่ล็อก) — กดป้าย "ใช้ N lot" ใต้ชื่อด้าย '
              + 'เพื่อดูว่าแต่ละ lot ไปที่ SC / Item ไหน และเลือก lot เอง']].map(([v, t, n, tip]) => (
            <button key={v} className={v + (only === v ? ' on' : '') + (n ? '' : ' zero')} title={tip}
              onClick={() => { setOnly(v); setLimit(PAGE); if (v === 'multi') openMulti() }}>{t} <b>{n}</b></button>
          ))}
        </div>
        {pick && (
          <button className="yp-pick" onClick={() => pickYarn(null)} title="ล้างตัวกรอง">ด้าย: {pickName} ✕</button>
        )}
        <select value={sortBy} onChange={e => setSortBy(e.target.value)} title="เรียงด้าย">
          <option value="name">เรียงตามชื่อ</option>
          <option value="use">ใช้มากก่อน</option>
          <option value="short">ไม่พอก่อน</option>
        </select>
        <button className="yp-btn" disabled={!page.length}
          onClick={() => setOpen(allOpen ? new Set() : new Set(shown.map(i => ykey(yarns[i]))))}>
          {allOpen ? 'ซ่อนที่มาทั้งหมด' : 'ดูที่มาทั้งหมด'}
        </button>
        <span className="yp-key">
          <i className="neg" title={d?.supply_on ? 'แผนวางไม่เกินด้ายที่มีอยู่แล้ว ถ้าติดลบแปลว่าข้อมูลด้าย (สต๊อก/PO) เปลี่ยนหลังรันแผน' : undefined} />ติดลบ
          <i className="need" />Internal/Local ต้องสั่งเพิ่ม
          <span className="yp-key-tip">· คลิกตัวเลขเพื่อดู SC</span>
        </span>
      </div>

      {!d && !err ? <div className="yp-empty">กำลังโหลด…</div>
        : shown.length === 0 ? <div className="yp-empty">{yarns.length ? 'ไม่มีด้ายที่ตรงกับตัวกรอง' : 'ยังไม่มีข้อมูล'}</div>
          : (
            <div className="yp-box">
              <table className="yp-table">
                <thead>
                  <tr>
                    <th className="yp-name">ด้าย</th>
                    <th className="n yp-usable" title="สต๊อกใช้ได้ตอนนี้ = ในคลัง − ลูกเล็ก − สงวนลูกค้า − ใบ KP ที่จองแต่ยังไม่โอน − ล็อก (ชี้ที่ตัวเลขของแต่ละด้ายเพื่อดูยอดที่หัก ; ไม่มีในสต๊อก = -)">สต๊อกใช้ได้</th>
                    <th className="yp-kind" />
                    {wlabels.map((w, wi) => (
                      <th key={wi} className={'n' + (wi === cur ? ' cur' : '')} title={wi === cur ? 'สัปดาห์นี้' : undefined}>{w}</th>
                    ))}
                    <th className="n yp-tot">รวม</th>
                  </tr>
                </thead>
                <tbody>{body}</tbody>
              </table>
            </div>
          )}
      {shown.length > limit && (
        <div className="yp-more"><button onClick={() => setLimit(l => l + PAGE)}>แสดงเพิ่ม (อีก {shown.length - limit} ด้าย)</button></div>
      )}

      {sel && yarns[sel.y] && (
        <CellPanel d={d} yi={sel.y} wi={sel.w} label={wlabels[sel.w]} lockMap={lockMap} canLock={canLock}
          onLock={(so, fab) => setDlg({ y: sel.y, so, fab })} onClose={() => setSel(null)} onChanged={load} />
      )}
      {lotSel != null && yarns[lotSel] && (
        <LotPanel d={d} yi={lotSel} wlabels={wlabels} lockMap={lockMap} canLock={canLock}
          onLock={(so, fab) => setDlg({ y: lotSel, so, fab })} onClose={() => setLotSel(null)} onChanged={load} />
      )}
      {dlg && (
        <LockDialog d={d} yi={dlg.y} so={dlg.so} fab={dlg.fab} lockMap={lockMap}
          onClose={() => setDlg(null)} onDone={() => { setDlg(null); load() }} />
      )}
    </div>
  )
}

/* ---------- กราฟใช้ด้ายรายสัปดาห์ ----------
   ซ้าย: ยอดด้ายรวมต่อสัปดาห์ (แท่งเดียว สีเดียว) — คลิกแท่ง = เลือกสัปดาห์
   ขวา: ด้ายทุกตัวที่ใช้ในสัปดาห์ที่เลือก เรียงมาก → น้อย — คลิกชื่อด้าย = แท่งซ้ายโชว์เฉพาะด้ายนั้น + ตารางเหลือด้ายนั้น
   (รอบแรกทำแท่งซ้อนแยกสี 8 ด้าย + "อื่น ๆ" — ข้อมูลจริงแต่ละสัปดาห์ใช้ 30–90 ด้าย 8 ด้ายแรกได้แค่ 42%
    สีเทา "อื่น ๆ" กินเกินครึ่งแท่ง ดูไม่ออกว่าด้ายอะไร จึงแยกเป็น 2 ฝั่งแบบนี้)
   ตัวเลขทุกตัวในกราฟมีในตารางด้านล่างอยู่แล้ว (แถว "ใช้") */
function UseChart({ yarns, wlabels, cur, pick, onPick }) {
  const [selW, setSelW] = useState(() => Math.max(cur, 0))
  useEffect(() => { if (selW >= wlabels.length) setSelW(Math.max(cur, 0)) }, [wlabels.length, cur, selW])
  const totals = useMemo(() => wlabels.map((_, wi) => yarns.reduce((a, y) => a + (y.use[wi] || 0), 0)), [yarns, wlabels])
  const py = pick ? yarns.find(y => ykey(y) === pick) : null   // ด้ายที่เลือก → แท่งเป็นของด้ายนั้น สเกลของมันเอง
  const vals = py ? wlabels.map((_, wi) => py.use[wi] || 0) : totals
  const sum = totals.reduce((a, b) => a + b, 0)
  const ticks = ticksOf(Math.max(0, ...vals))
  const vmax = ticks[ticks.length - 1]
  const px = (v) => v / vmax * CH

  const wi = Math.min(selW, wlabels.length - 1)
  const list = useMemo(() => yarns.filter(y => y.use[wi] > 0)
    .map(y => ({ k: ykey(y), name: yname(y), v: y.use[wi] })).sort((a, b) => b.v - a.v), [yarns, wi])
  const lmax = list[0]?.v || 1
  const wt = totals[wi] || 0
  const pct = (v) => { const p = wt ? v / wt * 100 : 0; return p > 0 && p < 1 ? '<1%' : Math.round(p) + '%' }

  return (
    <section className="yp-chart">
      <div className="yp-chart-main">
        <div className="yp-chart-hd">
          <b>ใช้ด้ายรายสัปดาห์ (kg)</b>
          {py
            ? <span>เฉพาะ <b>{yname(py)}</b> · ทั้งแผน {kg(py.total)} kg <button className="yp-link" onClick={() => onPick(pick)}>ดูทุกด้าย</button></span>
            : <span>รวมทั้งแผน {kg(sum)} kg · คลิกแท่งเพื่อดูว่าใช้ด้ายอะไร</span>}
        </div>
        <div className="yp-plot">
          <div className="yp-plot-in" style={{ '--ch': CH + 'px', minWidth: 40 + wlabels.length * 40 }}>
            <div className="yp-yax">
              {ticks.map(t => <span key={t} style={{ bottom: px(t) }}>{kshort(t)}</span>)}
            </div>
            <div className="yp-cols">
              <div className="yp-grid">{ticks.map(t => <i key={t} style={{ bottom: px(t) }} />)}</div>
              {vals.map((v, i) => (
                <div key={i} role="button" tabIndex={0}
                  className={'yp-col' + (i === wi ? ' sel' : '') + (i === cur ? ' cur' : '') + (py ? ' pk' : '')}
                  title={(i === cur ? 'สัปดาห์นี้ · ' : '') + `${wlabels[i]} ${kg(v)} kg`}
                  onClick={() => setSelW(i)}
                  onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setSelW(i) } }}>
                  <span className="yp-cap">{v > 0 ? kg(v) : ''}</span>
                  <div className="yp-bar" style={{ height: px(v) }} />
                  <span className="yp-xl">{wlabels[i]}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="yp-wk">
        <div className="yp-wk-hd">
          <b>{wlabels[wi]}</b>
          {wi === cur && <span className="yp-wk-now">สัปดาห์นี้</span>}
          <span className="yp-wk-sum">{kg(wt)} kg · {list.length} ด้าย</span>
        </div>
        <div className="yp-wk-list">
          {list.length === 0 && <div className="yp-empty">สัปดาห์นี้ไม่ได้ใช้ด้าย</div>}
          {list.map(r => (
            <button key={r.k} className={'yp-wk-r' + (pick === r.k ? ' on' : '')} onClick={() => onPick(r.k)}
              title={pick === r.k ? 'คลิกอีกครั้งเพื่อดูทุกด้าย' : `ดู ${r.name} ทุกสัปดาห์`}>
              <span className="nm">{r.name}</span>
              <span className="tr"><i style={{ width: (r.v / lmax * 100) + '%' }} /></span>
              <span className="v">{kg(r.v)}</span>
              <span className="pc">{pct(r.v)}</span>
            </button>
          ))}
        </div>
      </div>
    </section>
  )
}

/* ---------- แผงรายละเอียดของช่อง (ด้าย × สัปดาห์) ---------- */
function CellPanel({ d, yi, wi, label, lockMap, canLock, onLock, onClose, onChanged }) {
  const y = d.yarns[yi]
  const tot = rowKg
  const rows = d.rows.filter(r => r.y === yi && r.w === wi).sort((a, b) => tot(b) - tot(a))
  // ยอดสัปดาห์นี้ที่ดึงจากกองกลาง แยก ใช้สต๊อก / ใช้ PO / ไม่พอ (หัวแผง) — ต่อใบดู r.lots / r.pos / r.gap
  // (ในสัปดาห์เดียวกันงานสีได้ของในคลังก่อน แล้วตาม FG ; lot แจกทั้งใบ 1 ใบ 1 lot เท่าที่ทำได้ — user เลือก 16 ต.ค. 2569)
  const gapTxt = y.internal ? 'ต้องสั่งเพิ่ม' : 'ไม่พอ'
  const parts = [[y.from_stk, 'ใช้สต๊อก'], [y.from_po, 'ใช้ PO'], [y.gap, gapTxt]]
    .filter(([a]) => (a?.[wi] || 0) > 0.05)
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  async function unlock(l) {
    if (!window.confirm(`ปลดล็อกด้าย ${l.yarn} lot ${l.lot} ของ ${l.sc} ${l.fabric}?`)) return
    setBusy(l.id); setMsg('')
    try { await api.yarnLockRemove(l.id); onChanged() }
    catch (e) { setMsg('ปลดล็อกไม่ได้: ' + e.message) }
    finally { setBusy('') }
  }

  return (
    <aside className="jobpanel yp-panel">
      <div className="jobpanel-head">
        <div>
          <b className="jobpanel-item">{yname(y)}</b>
          <div className="jobpanel-sub">
            <b>{label}</b> · ใช้ {kg1(y.use[wi])} kg · สต๊อกคาดการณ์{' '}
            <b className={y.bal[wi] < -0.5 ? (y.internal ? 'yp-needtxt' : 'yp-negtxt') : undefined}>{kg1(y.bal[wi])}</b> kg
            {(y.inbound || [])[wi] > 0 && <> · PO เข้า +{kg1(y.inbound[wi])} kg</>}
            {parts.some(p => p[1] !== 'ใช้สต๊อก') && parts.map(([a, t]) => (
              <React.Fragment key={t}> · {t} {kg1(a[wi])} kg</React.Fragment>
            ))}
          </div>
        </div>
        <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
      </div>
      {msg && <div className="yp-note">{msg}</div>}
      <div className="jobpanel-body">
        {rows.length === 0 && <div className="yp-empty">สัปดาห์นี้ไม่ได้ใช้ด้ายนี้</div>}
        {rows.map(r => {
          const o = d.orders[`${r.so}|${r.fab}`] || {}
          const ls = lockMap.get(`${r.so}|${r.fab}|${y.yarn.toUpperCase()}`) || []
          const lockable = canLock && (o.need?.[yi] || 0) > 0
          return (
            <div key={r.so + r.fab} className="yp-ord">
              <div className="yp-ord-hd">
                <b>SC {r.so}</b> <span>{r.fab}</span>
                <span className="yp-kg">{kg1(tot(r))} kg</span>
              </div>
              {o.cus && <div className="yp-cus">{o.cus}</div>}
              <div className="yp-chips">
                {r.lots === undefined && r.stock > 0 && <span className="yp-chip stock">ใช้สต๊อก/PO {kg1(r.stock)}</span>}
                {sumKg(r.lots) > 0 && <span className="yp-chip stock">ใช้สต๊อก {kg1(sumKg(r.lots))}</span>}
                {sumKg(r.pos) > 0 && <span className="yp-chip frompo">ใช้ PO {kg1(sumKg(r.pos))}</span>}
                {r.gap > 0 && <span className={'yp-chip gap' + (y.internal ? ' need' : '')}>{gapTxt} {kg1(r.gap)}</span>}
                {r.lock > 0 && <span className="yp-chip lock">ล็อก SC {kg1(r.lock)}</span>}
                {r.ilock > 0 && <span className="yp-chip ilock">ล็อก Item {kg1(r.ilock)}</span>}
                {r.po > 0 && (
                  <span className="yp-chip po" title={(r.po_nos || []).map(p => poLabel(y, p)).join('\n') || undefined}>
                    รอ PO{r.po_nos?.length ? ' ' + r.po_nos.join(', ') : ''} {kg1(r.po)}{r.po_in ? ` (เข้า ${r.po_in})` : ''}
                  </span>
                )}
                {r.kp > 0 && <span className="yp-chip kp">ออก KP แล้ว {kg1(r.kp)}</span>}
              </div>
              {(r.lots?.length > 0 || r.pos?.length > 0 || r.lock_lots?.length > 0 || r.ilock_lots?.length > 0) && (
                <div className="yp-src">
                  {(r.lock_lots || []).map(([l, v]) => (
                    <span key={'k' + l} className="lk">lot <b>{lotName(l)}</b> (ล็อก) {kg1(v)}</span>
                  ))}
                  {(r.ilock_lots || []).map(([l, v]) => (
                    <span key={'i' + l} className="ilk">lot <b>{lotName(l)}</b> (ล็อก Item) {kg1(v)}</span>
                  ))}
                  {(r.lots || []).map(([l, v]) => (
                    <span key={'l' + l} title={lotLabel(y, l)}>lot <b>{lotName(l)}</b> {kg1(v)}</span>
                  ))}
                  {(r.pos || []).map(([p, v]) => (
                    <span key={'p' + p} title={poLabel(y, p)}>
                      PO <b>{p}</b> {kg1(v)}{y.pom?.[p]?.[2] && <i className="late"> เลยกำหนด</i>}
                    </span>
                  ))}
                </div>
              )}
              {ls.map(l => (
                <div key={l.id} className="yp-lockline">
                  <span className="yp-lockwhat">ล็อก lot <b>{l.lot}</b> · {kg1(l.eff ?? l.rem)} kg</span>
                  {canLock && (
                    <button className="yp-unlock" disabled={busy === l.id} onClick={() => unlock(l)}>
                      {busy === l.id ? 'กำลังปลด…' : 'ปลด'}
                    </button>
                  )}
                  <span className="yp-by">ล็อกโดย {l.by || '-'} · {String(l.at || '').slice(0, 16)}</span>
                </div>
              ))}
              {lockable && (
                <button className="yp-lockbtn" onClick={() => onLock(r.so, r.fab)}>
                  ล็อก lot ให้ใบนี้ (ต้องใช้ {kg1(o.need[yi])} kg)
                </button>
              )}
            </div>
          )
        })}
      </div>
    </aside>
  )
}

/* ---------- แผง lot ของด้าย: lot ไหนไปที่ SC / Item ไหน + เลือก lot เอง ----------
   (user 16 ต.ค. 2569: ปุ่มกรองด้ายที่ใช้มากกว่า 1 lot → "ต้องบอกด้วยว่าทำกับ SC อะไรบ้าง Item อะไรบ้าง"
    แล้ว "user ต้องสามารถเลือกได้ว่าจะใช้ lot ไหน ถ้าไม่โอเคกับที่เราทำมาให้" → ปุ่มล็อก lot ให้ SC ตัวเดิม (LockDialog))
   นับทุกสัปดาห์ของแผน เฉพาะส่วนที่รู้ lot (LOT_FIELDS) */
function LotPanel({ d, yi, wlabels, lockMap, canLock, onLock, onClose, onChanged }) {
  const y = d.yarns[yi]
  const { groups, mixed } = useMemo(() => {
    const by = new Map()       // lot → {lot, kg, ords: Map(SC|ผ้า|ที่มา → {so, fab, f, kg, ws})}
    const per = new Map()      // SC|ผ้า → lot ที่ได้
    for (const r of d.rows) {
      if (r.y !== yi) continue
      for (const f of LOT_FIELDS) {
        for (const [l, v] of r[f] || []) {
          if (!by.has(l)) by.set(l, { lot: l, kg: 0, ords: new Map() })
          const g = by.get(l)
          g.kg += v
          const k = `${r.so}|${r.fab}|${f}`
          if (!g.ords.has(k)) g.ords.set(k, { so: r.so, fab: r.fab, f, kg: 0, ws: new Set() })
          const o = g.ords.get(k)
          o.kg += v
          o.ws.add(r.w)
          const ok = `${r.so}|${r.fab}`
          if (!per.has(ok)) per.set(ok, new Set())
          per.get(ok).add(l)
        }
      }
    }
    // lot เรียงเก่า → ใหม่ตามกองกลาง (y.lots) ; lot ที่ล็อกไว้ (ไม่อยู่ในกองกลาง) ต่อท้าย ; ไม่ทราบ lot ท้ายสุด
    const rank = new Map((y.lots || []).map(([l], i) => [l, i]))
    const rk = (l) => (l === '' ? 2e9 : rank.get(l) ?? 1e9)
    return {
      groups: [...by.values()].sort((a, b) => rk(a.lot) - rk(b.lot) || a.lot.localeCompare(b.lot))
        .map(g => ({ ...g, ords: [...g.ords.values()].sort((a, b) => Math.min(...a.ws) - Math.min(...b.ws) || b.kg - a.kg) })),
      mixed: new Map([...per].filter(([, s]) => s.size > 1).map(([k, s]) => [k, s.size])),
    }
  }, [d, yi, y])
  const total = groups.reduce((a, g) => a + g.kg, 0)
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  async function unlock(l) {
    if (!window.confirm(`ปลดล็อกด้าย ${l.yarn} lot ${l.lot} ของ ${l.sc} ${l.fabric}?`)) return
    setBusy(l.id); setMsg('')
    try { await api.yarnLockRemove(l.id); onChanged() }
    catch (e) { setMsg('ปลดล็อกไม่ได้: ' + e.message) }
    finally { setBusy('') }
  }

  return (
    <aside className="jobpanel yp-panel">
      <div className="jobpanel-head">
        <div>
          <b className="jobpanel-item">{yname(y)}</b>
          <div className="jobpanel-sub">
            ใช้ <b>{groups.length}</b> lot · {kg1(total)} kg ทั้งแผน
            {!canLock && ' · เลือก lot ได้เฉพาะสิทธิ์ yarn / admin'}
          </div>
        </div>
        <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
      </div>
      {msg && <div className="yp-note">{msg}</div>}
      <div className="jobpanel-body">
        {groups.length === 0 && <div className="yp-empty">แผนไม่ได้ใช้ด้ายนี้จากสต๊อก</div>}
        {mixed.size > 0 && (
          <div className="yp-mix">
            SC ที่ได้ด้ายมากกว่า 1 lot: {[...mixed.keys()].map(k => { const [so, fab] = k.split('|'); return `SC ${so} ${fab}` }).join(', ')}
          </div>
        )}
        {groups.map(g => (
          <section key={g.lot} className="yp-lotgrp">
            <div className="yp-lotgrp-hd">
              <b>{lotLabel(y, g.lot)}</b>
              <span className="yp-kg">{kg1(g.kg)} kg</span>
            </div>
            {g.ords.map(o => {
              const ok = `${o.so}|${o.fab}`
              const ord = d.orders[ok] || {}
              const ls = o.f === 'lock_lots'
                ? (lockMap.get(`${ok}|${y.yarn.toUpperCase()}`) || []).filter(l => l.lot === g.lot) : []
              return (
                <div key={ok + o.f} className="yp-ord">
                  <div className="yp-ord-hd">
                    <b>SC {o.so}</b> <span>{o.fab}</span>
                    <span className="yp-kg">{kg1(o.kg)} kg</span>
                  </div>
                  {ord.cus && <div className="yp-cus">{ord.cus}</div>}
                  <div className="yp-chips">
                    <span className="yp-chip" title="สัปดาห์ที่ใช้">{[...o.ws].sort((a, b) => a - b).map(w => wlabels[w]).join(', ')}</span>
                    {LOT_KIND[o.f] && <span className={'yp-chip ' + (o.f === 'lock_lots' ? 'lock' : 'ilock')}>{LOT_KIND[o.f]}</span>}
                    {mixed.has(ok) && <span className="yp-chip mix">ใบนี้ใช้ {mixed.get(ok)} lot</span>}
                  </div>
                  {ls.map(l => (
                    <div key={l.id} className="yp-lockline">
                      <span className="yp-lockwhat">ล็อก lot <b>{l.lot}</b> · {kg1(l.eff ?? l.rem)} kg</span>
                      {canLock && (
                        <button className="yp-unlock" disabled={busy === l.id} onClick={() => unlock(l)}>
                          {busy === l.id ? 'กำลังปลด…' : 'ปลด'}
                        </button>
                      )}
                      <span className="yp-by">ล็อกโดย {l.by || '-'} · {String(l.at || '').slice(0, 16)}</span>
                    </div>
                  ))}
                  {canLock && (ord.need?.[yi] || 0) > 0 && (
                    <button className="yp-lockbtn" onClick={() => onLock(o.so, o.fab)}>เลือก lot ให้ SC นี้</button>
                  )}
                </div>
              )
            })}
          </section>
        ))}
      </div>
    </aside>
  )
}

/* ---------- หน้าต่างเลือก lot ล็อกให้ SC + ผ้า ---------- */
// ยอดตั้งต้น = ด้ายที่แผนต้องใช้ทุกสัปดาห์ของ SC + ผ้านี้ − ที่ล็อกไว้แล้ว (ไม่เกินที่ lot มี) ; แก้ได้เป็น kg ผ้า
// kg ด้าย = kg ผ้า × % ด้าย × สูญเสีย ของใบนี้ (backend ตัวเดียวกับที่ตรวจตอนบันทึก)
function LockDialog({ d, yi, so, fab, lockMap, onClose, onDone }) {
  const y = d.yarns[yi]
  const need = d.orders[`${so}|${fab}`]?.need?.[yi] || 0
  const locked = (lockMap.get(`${so}|${fab}|${y.yarn.toUpperCase()}`) || []).reduce((a, l) => a + (l.eff ?? l.rem ?? 0), 0)
  const [res, setRes] = useState(null)
  const [err, setErr] = useState('')
  const [lot, setLot] = useState(null)
  const [qty, setQty] = useState('')
  const [note, setNote] = useState('')
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    let alive = true
    api.yarnPlanLots({ yarn: y.yarn, color: y.color, so, fabric: fab })
      .then(r => { if (alive) setRes(r) })
      .catch(e => { if (alive) setErr('โหลดรายการ lot ไม่ได้: ' + e.message) })
    return () => { alive = false }
  }, [y.yarn, y.color, so, fab])
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const f = res?.factor
  const pick = (l) => {
    setLot(l)
    if (f) {
      const want = Math.max(need - locked, 0)
      setQty(String(Math.floor(Math.min(want, l.ava) / f.factor * 10) / 10))
    }
  }
  const fq = parseFloat(qty)
  const ykg = f && Number.isFinite(fq) ? fq * f.factor : null
  const over = lot && ykg != null && ykg > lot.ava + 0.5

  async function save() {
    if (!lot || !f || !(fq > 0) || over) return
    setSaving(true); setErr('')
    try {
      const r = await api.yarnLockAdd({ yarn: lot.name, lot: lot.lot, sc: so, fabric: fab, fabric_kg: fq, note })
      if (r.warn?.length) window.alert('ล็อกแล้ว — ' + r.warn.join('\n'))
      onDone()
    } catch (e) { setErr(e.message) }
    finally { setSaving(false) }
  }

  return (
    <div className="yp-dlg-back" onMouseDown={e => { if (e.target === e.currentTarget) onClose() }}>
      <div className="yp-dlg" role="dialog" aria-label="ล็อกด้ายให้ SC">
        <div className="yp-dlg-hd">
          <div>
            <b>ล็อก {yname(y)} ให้ SC {so} · {fab}</b>
            <div className="yp-dlg-sub">
              ต้องใช้ {kg1(need)} kg (ยังไม่ออก KP){locked > 0 && ` · ล็อกแล้ว ${kg1(locked)} kg`}
              {' '}— ล็อกแค่กันด้ายไว้ ไม่เลื่อนวันทอ
            </div>
          </div>
          <button className="jobpanel-close" onClick={onClose} title="ปิด (Esc)">✕</button>
        </div>
        {!res && !err && <div className="yp-empty">กำลังโหลด lot…</div>}
        {res && !f && (
          <div className="yp-note">ล็อกไม่ได้: ไม่มี % ด้ายของ SC {so} {fab} (ใบปิดแล้ว หรือไม่มีข้อมูลด้ายรายบรรทัด)</div>
        )}
        {res && f && (
          <>
            <div className="yp-dlg-sub">
              ด้าย {f.pct}% · สูญเสีย ×{f.loss} ({f.basis}) · เลือก lot (lot เก่าอยู่บน)
            </div>
            <div className="yp-lots">
              {res.lots.length === 0 && <div className="yp-empty">ไม่มี lot ว่างของด้ายนี้{y.color ? ` สี ${y.color}` : ''}</div>}
              {res.lots.length > 0 && (
                <table className="yp-table yp-lottable">
                  <thead><tr><th>LOT</th><th className="n">ใช้ได้ (kg)</th><th className="n">อายุ (วัน)</th><th>สงวนให้</th></tr></thead>
                  <tbody>
                    {res.lots.map(l => (
                      <tr key={l.lot} className={'yp-lotrow' + (lot?.lot === l.lot ? ' on' : '')} onClick={() => pick(l)}>
                        <td>{l.lot}</td><td className="n">{kg1(l.ava)}</td><td className="n">{l.age ?? '-'}</td>
                        <td className="yp-own">{l.own.join(', ')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            {lot && (
              <div className="yp-dlg-form">
                <span>lot {lot.lot}</span>
                <input type="number" min="0" step="any" value={qty} onChange={e => setQty(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') save() }} />
                <span>kg ผ้า</span>
                <b className={over ? 'yp-negtxt' : 'yp-calc'}>= ด้าย {ykg == null ? '-' : kg1(ykg)} kg</b>
                {over && <span className="yp-negtxt">เกินที่ lot ใช้ได้ ({kg1(lot.ava)} kg)</span>}
                <input className="yp-note-in" value={note} maxLength={300} onChange={e => setNote(e.target.value)}
                  placeholder="หมายเหตุ (ไม่บังคับ)" />
              </div>
            )}
          </>
        )}
        {err && <div className="yp-note">{err}</div>}
        <div className="yp-dlg-actions">
          <button className="yp-primary" disabled={!lot || !f || !(fq > 0) || over || saving} onClick={save}>
            {saving ? 'กำลังบันทึก…' : 'ล็อกด้าย'}
          </button>
          <button onClick={onClose}>ยกเลิก</button>
        </div>
      </div>
    </div>
  )
}

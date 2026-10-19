import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import {
  RULE_KEY, rulesOf, lotAllowedFor, heldByOther, ruleCustomers, ruleText, hiddenPoSuffixes, poHidden,
} from '../yarnRules.js'
import { useYarnPull } from '../useYarnPull.js'

/* Dashboard › Stock Yarn — ด้ายแต่ละ ITEM มี LOT ไหน PO ไหน กี่ kg กี่ลูก
   ข้อมูลจากไฟล์ที่ View_Yarn_Aging.py ดึงจาก view สต๊อกด้ายบน Oracle (ตั้งเวลาดึงทุกเช้า)

   มี 2 มุมมอง สลับด้วยปุ่มบนหัวการ์ด (user เลือก 9 ต.ค. 2569):

   • รวมต่อ LOT (หน้าหลัก) — 1 แถว = 1 lot : ITEM ก่อน ตามด้วย lot ของด้ายตัวนั้น + ยอดรวมของ lot
     (user: "เอา total รวมของแต่ละ lot มาดู" · "Item ก่อน ตามด้วย lot ของ yarn นั้นๆ")
     ยอดรวม = ผลรวม TOTAL_WEIGHT / TOTAL_YARN ของทั้ง ITEM+LOT — ยอดเต็มของ lot เสมอ ไม่เปลี่ยนตามคำค้น (user เลือก)
     YARN_WEIGHT เป็น kg ต่อลูก รวมไม่ได้ จึงไม่มีในมุมมองนี้ (BUYYER / PO ก็ไม่มี — lot เดียวมีได้หลายค่า)
     นี่คือค่าเดียวบนหน้านี้ที่คำนวณเอง

   • รายละเอียด PO — **ไม่คำนวณอะไรเลย** (user สั่ง) 1 แถว = 1 แถวของ view
     TOTAL_WEIGHT / TOTAL_YARN / YARN_WEIGHT โชว์ค่าจากไฟล์ตรง ๆ ไม่รวม ไม่เฉลี่ย ไม่ปัดทศนิยม
     (แถว "รวม LOT" แทรกใต้ PO ของ lot user ไม่เอา — ยอดรวมให้ไปดูที่มุมมองรวมต่อ LOT)

   ทั้งสองมุมมองไม่มียอดรวม ITEM, ไม่มี KPI ผลรวม และไม่เรียงตามยอด

   วางแบบ pivot กางครบทุกชั้น (user ไม่อยากคลิกกางทีละชั้น): ITEM / LOT / PO แยกคนละคอลัมน์
   (user ไม่เอาแบบ LOT เยื้องซ้อนใต้ ITEM ในคอลัมน์เดียว) · แต่ละช่องรวมเซลล์คร่อมแถวของกลุ่มตัวเอง ·
   เรียง ITEM → LOT → PO แบบ A→Z
   แถวใน PO เดียวกันเรียงตาม YARN_WEIGHT
   view แตกแถวตามใบรับ × ที่เก็บ → PO เดียวกันแยกหลายแถวได้ ; คอลัมน์ LOCATOR_ID (ที่เก็บ) บอกว่าต่างกันตรงไหน
   (user สั่งเพิ่ม 15 ต.ค. 2569 — เห็น lot+PO เดียวกันแยก 2 แถวแล้วนึกว่าซ้ำ) ยังมีแถวที่ค่าทุกช่องเหมือนกันได้ (คนละใบรับ)
   คอลัมน์ RECEIVED_TYPE (ประเภทการรับเข้า เช่น Received From Production Omnoi) แทนโรงงานที่ view ไม่มี — user เลือก 15 ต.ค. 2569
   โชว์เฉพาะรายละเอียด PO (lot เดียวมีได้หลายค่า เหมือน BUYYER)

   ตัวเลือก CUSTOMER = กติกา "ลูกค้าใช้ได้เฉพาะ lot" ที่ตั้งไว้หน้า Monitor Yarn (ตัวเทียบอยู่ที่ yarnRules.js)
   กติกาสงวน lot สองทาง: เลือกลูกค้าแล้วเหลือทุกแถวในคลังที่ลูกค้านั้นใช้ได้ (ตัด lot ที่สงวนให้ลูกค้าอื่นออก)
   ไม่ว่า BUYYER จะเป็นใคร — คอลัมน์ BUYYER บอกว่าด้ายถือไว้ให้ใคร ; สีส้ม = ถือ lot ที่สงวนให้คนอื่น
   PO ที่ลงท้ายตามที่ตั้งไว้ (ค่าเริ่มต้น -D / -T, เปิด/ปิดที่หน้า Monitor Yarn) ไม่แสดงเลย — ตัดออกก่อนตัวกรองอื่น
   ด้าย DEFECT (ที่เก็บขึ้นต้น DEFECT) และด้ายขาย (ที่เก็บในชีท "Locator ขายเส้นด้าย") backend ตัดทิ้งมาแล้ว
   (yarn_stock_live — user สั่ง 15 ต.ค. 2569 "ไม่เอามาคิด") หน้านี้ไม่ได้รับแถวพวกนั้นเลย

   ปุ่ม ซ่อน/แสดง แถว YARN_WEIGHT ต่ำกว่าขั้นต่ำ (ลูกเล็ก — แถวเดียวกับที่ลงสีแดง) — user สั่ง 15 ต.ค. 2569
   เปิดหน้ามา = ซ่อน (user เลือก) · มีผลทั้งสองมุมมอง: ซ่อนอยู่ = ยอดรวม LOT ไม่นับแถวลูกเล็ก (ตรงกับยอดใช้ได้ที่แผนใช้ — yarn_ava_live)
   และ lot ที่เป็นลูกเล็กทั้ง lot หายจากตาราง · แถวไม่มีค่า YARN_WEIGHT ไม่ถูกซ่อน · กรองแถวอย่างเดียว ไม่แตะค่า

   ธีม/เลย์เอาต์ใช้ชุดเดียวกับหน้า Dashboard อื่น (.mcav-dash + ตัวแปร --d-*) — คลาสเฉพาะหน้านี้ขึ้นต้น .ys- */

const THEME_KEY = 'mcav-theme'      // ใช้ค่าเดียวกับหน้า Dashboard อื่น — สลับธีมหน้าหนึ่งแล้วอีกหน้าตาม
// แถวที่ render ต่อครั้ง (นับแถวไม่ใช่ ITEM) — ทั้งไฟล์ ~11,000 แถว render หมดแล้วพิมพ์ค้นหาหน่วง
// ITEM ไม่ถูกตัดกลางกลุ่ม: ใส่ทั้งก้อนจนเกินงบแล้วค่อยหยุด
const PAGE = 800

/** ค่าตามไฟล์ — ใส่แค่คอมมาหลักพัน ไม่ปัดทศนิยม (ไฟล์มีมากสุด 4 ตำแหน่ง) */
const raw = (n) => (n == null ? '-' : n.toLocaleString(undefined, { maximumFractionDigits: 10 }))
// สีของช่อง YARN_WEIGHT (user สั่ง): น้อยกว่าขั้นต่ำ = แดง · ตั้งแต่ขั้นต่ำขึ้นไป = เขียว · ไม่มีค่า = ไม่ลงสี
// ลงสีอย่างเดียว ตัวเลขที่โชว์ยังเป็นค่าจาก view ตรง ๆ
// ขั้นต่ำตั้งที่หน้า Monitor Yarn (KP_YARN_WEIGHT_MIN) — ตัวนี้เป็นค่าสำรองตอนอ่านค่าตั้งไม่ได้ ต้องตรงกับ default ใน plan_config.py
const YW_MIN_KEY = 'KP_YARN_WEIGHT_MIN'
const YW_MIN_FALLBACK = 0.3
// แถวลูกเล็ก — ตัวเทียบเดียวทั้งลงสีแดงและปุ่มซ่อน จะได้ซ่อนตรงกับแถวที่เห็นเป็นสีแดงเสมอ
const ywLow = (v, min) => v != null && v < min
const ywCls = (v, min) => (v == null ? '' : ywLow(v, min) ? ' ys-yw-low' : ' ys-yw-ok')
// ยอดรวม LOT บวกเป็นจำนวนเต็ม (× 10,000) แล้วค่อยหารกลับ — ไฟล์มีทศนิยมมากสุด 4 ตำแหน่ง
// บวก float ตรง ๆ ได้เศษแบบ 222.28330000000003
const SUM_SCALE = 10000
const lotKey = (item, lot) => item + '\u0001' + lot
const byName = ([a], [b]) => a.localeCompare(b)
const byYw = (a, b) => (a.yw ?? 0) - (b.yw ?? 0) || (a.w ?? 0) - (b.w ?? 0)

function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

// ปุ่ม "ดึงข้อมูลด้าย" = runner โหมด yarn (ด้ายต่อใบ KP + สต๊อกด้าย ตัวเดียวกับรอบ 06:00) — ใช้ราว 17 นาที
// รันในคิวด้ายแยกจากคิวหลัก ไม่บล็อกการรันแผน (user เลือก 15 ต.ค. 2569) ; ดูรายละเอียดที่ useYarnPull.js
const PULL_MODE = 'yarn'

/* ---------- หน้าหลัก ---------- */
// canRun = โชว์ปุ่ม "ดึงข้อมูลด้าย" (App.jsx ส่งให้สิทธิ์ Yarn / Admin — user เลือก)
export default function YarnStock({ canRun = false }) {
  const [theme, setTheme] = useState(() => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'))
  const [d, setD] = useState(null)
  const [err, setErr] = useState('')
  const [loading, setLoading] = useState(false)
  const [q, setQ] = useState('')
  const [limit, setLimit] = useState(PAGE)
  const [ywMin, setYwMin] = useState(YW_MIN_FALLBACK)
  const [rules, setRules] = useState([])          // กติกา CUSTOMER ใช้ได้เฉพาะ lot (หน้า Monitor Yarn)
  const [cust, setCust] = useState('')            // CUSTOMER ที่เลือกกรอง ('' = ไม่กรอง)
  const [poSufs, setPoSufs] = useState([])        // ท้าย PO ที่ไม่แสดง (หน้า Monitor Yarn) — [] = สวิตช์ปิด
  const [view, setView] = useState('lot')         // 'lot' = รวมต่อ LOT (1 แถว = 1 lot) · 'po' = รายละเอียด PO (แถวของ view)
  const [hideLow, setHideLow] = useState(true)    // ซ่อนแถว YARN_WEIGHT ต่ำกว่าขั้นต่ำ — เปิดหน้ามาซ่อนไว้ก่อน (user เลือก)

  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])

  // ค่าจากหน้า Monitor Yarn (ขั้นต่ำ YARN_WEIGHT + กติกาลูกค้า + ซ่อน PO) — โหลดตอนเปิดหน้า และโหลดใหม่ทันทีที่มีคนบันทึกค่า
  // (หน้า Monitor Yarn / Setting) จอที่เปิดค้างจึงเปลี่ยนสี/กรองตามเอง ; อ่านไม่ได้ก็ใช้ค่าสำรองไปก่อน หน้านี้ยังดูได้ตามปกติ
  // ⚠️ อ่านไม่ได้ = ไม่ซ่อน PO (แสดงครบดีกว่าซ่อนเงียบ ๆ โดยไม่รู้ว่าซ่อนอะไร)
  useEffect(() => {
    const pull = () => api.yarnMonitor().then(g => {
      const v = Number(g?.values?.[YW_MIN_KEY])
      setYwMin(Number.isFinite(v) ? v : YW_MIN_FALLBACK)
      setRules(Array.isArray(g?.values?.[RULE_KEY]) ? g.values[RULE_KEY] : [])
      setPoSufs(hiddenPoSuffixes(g?.values))
    }).catch(() => {})
    pull()
    return live.subscribe(live.RES.planConfig, pull)
  }, [])

  async function load() {
    setLoading(true); setErr('')
    try { setD(await api.yarnStock()) }
    catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }
  // โหลดตอนเปิดหน้า และโหลดใหม่เองเมื่อมีรอบรันจบ (ดึงด้ายจากปุ่มของจอไหนก็ได้ / รอบ 06:00) — จอที่เปิดค้างไม่ต้องกดเอง
  useEffect(() => {
    load()
    return live.subscribe(live.RES.run, load)
  }, [])
  // ปุ่ม "ดึงข้อมูลด้าย" — ดึงจบแล้วโหลดหน้าใหม่ ; since = เวลาเริ่มของงานที่กำลังดึง
  const { pulling, since, pullMsg, pull } = useYarnPull(load, PULL_MODE)

  // แถวที่หน้านี้แสดงได้ = แถวในไฟล์ ตัด PO ที่ลงท้ายตามที่ตั้งไว้ (เช่น -D / -T) ออกก่อนตัวกรองอื่นทั้งหมด
  // (user: "ไม่เอาโชว์ใน dashboard" และ "ไม่เอาพวกนี้มาทำแผน") — ตัวเลือก CUSTOMER / สีส้ม ดูจากชุดนี้
  const fileRows = d?.rows
  const all = useMemo(() => {
    const rs = fileRows || []
    return poSufs.length ? rs.filter(r => !poHidden(r.p, poSufs)) : rs
  }, [fileRows, poSufs])
  // ตัดแถวลูกเล็กเมื่อกดซ่อน — ตารางทั้งสองมุมมองและยอดรวม LOT ใช้ชุดนี้
  // (รายชื่อ CUSTOMER ยังดูจาก all : กดซ่อนแล้ว BUYYER ที่มีแต่ลูกเล็กไม่หายจากตัวเลือก และลูกค้าที่เลือกไว้ไม่ถูกล้าง)
  const kept = useMemo(() => (hideLow ? all.filter(r => !ywLow(r.yw, ywMin)) : all), [all, hideLow, ywMin])

  // ตัวเลือก CUSTOMER = ลูกค้าที่มีกติกา + ชื่อ BUYYER อื่นทั้งหมดในไฟล์ — ลูกค้าที่ไม่มีกติกาก็ต้องเลือกได้
  // เพราะกติกาสงวน lot สองทาง (เลือก UNIQLO แล้วต้องไม่เห็น lot ที่สงวนให้ NIKE / PUMA)
  const custList = useMemo(() => ruleCustomers(rules), [rules])
  const otherCusts = useMemo(() => {
    const ruled = new Set(custList)
    return [...new Set(all.map(r => r.b).filter(b => b && !ruled.has(b)))].sort((a, b) => a.localeCompare(b))
  }, [all, custList])
  // ชื่อที่เลือกอยู่หายไปจากทั้งสองรายการ (อีกจอลบกติกา + ไม่มีในไฟล์) → กลับไปไม่กรอง ไม่ให้ตัวเลือกค้างชื่อที่ไม่มีแล้ว
  useEffect(() => {
    if (cust && d && !custList.includes(cust) && !otherCusts.includes(cust)) setCust('')
  }, [cust, d, custList, otherCusts])
  const custRules = useMemo(() => (cust ? rulesOf(rules, cust) : []), [rules, cust])
  // กติกาของลูกค้าอื่น = lot ที่ลูกค้าที่เลือกอยู่ใช้ไม่ได้ (โชว์ในบรรทัดบอกกติกา)
  const otherRules = useMemo(() => (cust ? rules.filter(r => String(r.customer ?? '').trim() !== cust) : []),
    [rules, cust])

  // กรอง 2 ชั้น: CUSTOMER (เหลือเฉพาะ lot ที่ลูกค้าใช้ได้ — ดูจาก lot ทุกแถวในคลัง ไม่ใช่เฉพาะแถวที่ BUYYER เป็นลูกค้านั้น)
  // · คำค้น ; กรองอย่างเดียว ไม่แตะค่าในแถว
  const kw = q.trim().toLowerCase()
  const shown = useMemo(() => kept.filter(r =>
    (!cust || lotAllowedFor(rules, cust, r.i, r.l)) &&
    (!kw || r.i.toLowerCase().includes(kw) || r.l.toLowerCase().includes(kw) ||
      r.p.toLowerCase().includes(kw) || (r.b || '').toLowerCase().includes(kw))),
  [kept, kw, cust, rules])

  // ยอดรวมต่อ ITEM+LOT — บวกจากทุกแถวที่หน้านี้แสดงได้ (kept) ไม่ใช่แถวที่ผ่านคำค้น → ค้น PO เดียวก็ยังได้ยอดเต็มของ lot
  // (PO ที่ซ่อนตามหน้า Monitor Yarn ไม่นับ ; แถวลูกเล็กไม่นับตอนกดซ่อน ; ตัวกรอง CUSTOMER ตัดทั้ง lot จึงไม่ทำให้ยอดขาด)
  const lotTotals = useMemo(() => {
    const m = new Map()
    for (const r of kept) {
      const k = lotKey(r.i, r.l)
      let t = m.get(k)
      if (!t) m.set(k, (t = { w: 0, y: 0 }))
      t.w += Math.round((r.w ?? 0) * SUM_SCALE)
      t.y += Math.round((r.y ?? 0) * SUM_SCALE)
    }
    for (const t of m.values()) { t.w /= SUM_SCALE; t.y /= SUM_SCALE }
    return m
  }, [kept])

  /* จัดกลุ่ม ITEM → LOT → PO → แถวของ view
     tot ของ LOT = ยอดรวมทั้ง lot (มุมมองรวมต่อ LOT ใช้)
     n ของ ITEM / LOT = จำนวนแถวของ view ที่ช่องนั้นต้องคร่อมในมุมมองรายละเอียด PO (rowSpan) ไม่ได้เอาไปโชว์ */
  const tree = useMemo(() => {
    const items = new Map()
    for (const r of shown) {
      let lots = items.get(r.i)
      if (!lots) items.set(r.i, (lots = new Map()))
      let pos = lots.get(r.l)
      if (!pos) lots.set(r.l, (pos = new Map()))
      let rs = pos.get(r.p)
      if (!rs) pos.set(r.p, (rs = []))
      rs.push(r)
    }
    return [...items].sort(byName).map(([item, lots]) => {
      const lotArr = [...lots].sort(byName).map(([lot, pos]) => {
        const poArr = [...pos].sort(byName).map(([po, rs]) => ({ name: po, rows: rs.sort(byYw) }))
        return {
          name: lot, pos: poArr, tot: lotTotals.get(lotKey(item, lot)),
          n: poArr.reduce((s, p) => s + p.rows.length, 0),
        }
      })
      return { name: item, lots: lotArr, n: lotArr.reduce((s, l) => s + l.n, 0) }
    })
  }, [shown, lotTotals])

  const filterOn = !!kw || !!cust
  const src = d?.source || {}

  /* แถวที่จะ render —
     รวมต่อ LOT: 1 แถว = 1 lot (ITEM คร่อมทุก lot ของตัวเอง)
     รายละเอียด PO: แถวของ view ทุกแถว ช่อง ITEM / LOT / PO อยู่แถวแรกของกลุ่มแล้วคร่อมลงมาทั้งกลุ่ม */
  const byLot = view === 'lot'
  const rows = []
  let nItems = 0
  for (const it of tree) {
    if (rows.length >= limit) break
    nItems++
    if (byLot) {
      it.lots.forEach((lot, li) => {
        rows.push(
          <tr key={lotKey(it.name, lot.name)} className={li === 0 ? 'ys-itfirst' : undefined}>
            {li === 0 && (
              <td className="ys-item" rowSpan={it.lots.length}>
                <div className="nm">{it.name}</div>
              </td>
            )}
            <td className="ys-lot"><div className="nm">{lot.name}</div></td>
            <td className="n">{raw(lot.tot.w)}</td>
            <td className="n">{raw(lot.tot.y)}</td>
          </tr>,
        )
      })
      continue
    }
    it.lots.forEach((lot, li) => {
      const lk = lotKey(it.name, lot.name)
      lot.pos.forEach((po, pi) => {
        po.rows.forEach((r, ri) => {
          const lotFirst = pi === 0 && ri === 0
          const itFirst = li === 0 && lotFirst
          // BUYYER ถือ lot ที่สงวนให้ลูกค้าอื่น → ช่อง BUYYER สีส้ม (ลงสีอย่างเดียว ค่าไม่เปลี่ยน)
          const owners = heldByOther(rules, r.b, r.i, r.l)
          rows.push(
            <tr key={lk + '\u0001' + po.name + '\u0001' + ri}
              className={itFirst ? 'ys-itfirst' : lotFirst ? 'ys-first' : undefined}>
              {itFirst && (
                <td className="ys-item" rowSpan={it.n}>
                  <div className="nm">{it.name}</div>
                </td>
              )}
              {lotFirst && (
                <td className="ys-lot" rowSpan={lot.n}>
                  <div className="nm">{lot.name}</div>
                </td>
              )}
              {ri === 0 && <td className="nw ys-po" rowSpan={po.rows.length}>{po.name}</td>}
              <td className="nw">{r.lc || '-'}</td>
              <td className="nw">{r.rt || '-'}</td>
              {owners.length
                ? <td className="ys-buyer bad" title={`lot นี้สงวนให้ ${owners.join(', ')} — ${r.b} ใช้ไม่ได้`}>{r.b}</td>
                : <td className="ys-buyer">{r.b || '-'}</td>}
              <td className="n">{raw(r.w)}</td>
              <td className="n">{raw(r.y)}</td>
              <td className={'n' + ywCls(r.yw, ywMin)}>{raw(r.yw)}</td>
            </tr>,
          )
        })
      })
    })
  }

  return (
    <div className={'mcav-dash ' + theme}>
      <div className="mcav-top">
        <div className="mcav-title">
          <h1>Stock Yarn<span className="live">LIVE</span></h1>
          <div className="sub">
            ด้ายในคลังจาก Oracle (ดึงทุกเช้า) · รวมต่อ LOT = ยอดรวมของแต่ละ lot ·
            รายละเอียด PO = ค่าจาก view ตรง ๆ ไม่มีการคำนวณ (1 แถว = 1 แถวของ view) ·
            ไม่รวมด้าย DEFECT (ที่เก็บ DEFECT…) และด้ายขาย (ที่เก็บขายเส้นด้าย SC / SP / ST / SYC / SYP / SL) — ไม่เอามาคิด
          </div>
        </div>
        <div className="mcav-topright">
          <div className="clock">
            <b>{fmtTime(src.mtime)}</b>
            <span>{src.file || 'ไม่พบไฟล์ Stock Yarn'}</span>
          </div>
          <button className="tbtn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☀ ธีมสว่าง' : '🌙 ธีมมืด'}
          </button>
          {canRun && (
            <button className="tbtn" onClick={pull} disabled={pulling}
              title="ดึงสต๊อกด้าย + ด้ายต่อใบ KP ใหม่จาก DB (ราว 17 นาที) — รันแยกคิว ไม่บล็อกการรันแผน ; ปิดหน้านี้ไปก่อนได้ ดึงจบแล้วทุกจอโหลดใหม่เอง">
              {pulling
                ? `⏳ กำลังดึงข้อมูลด้าย${since ? ` (เริ่ม ${since.slice(11, 16)} · ราว 17 นาที)` : ''}`
                : '⇩ ดึงข้อมูลด้าย'}
            </button>
          )}
          <button className="tbtn go" onClick={load} disabled={loading}>
            {loading ? '⏳ กำลังโหลด' : '↻ โหลดใหม่'}
          </button>
        </div>
      </div>

      {(err || d?.note || pullMsg) && (
        <div className="mcav-alerts">
          {err && <span className="al bad">{err}</span>}
          {pullMsg && <span className={'al ' + pullMsg.cls}>{pullMsg.text}</span>}
          {d?.note && <span className="al warn">{d.note}</span>}
        </div>
      )}

      <div className="mcav-filters">
        <input className="otd-search ys-search" value={q}
          onChange={e => { setQ(e.target.value); setLimit(PAGE) }}
          placeholder="ค้นหา ITEM / LOT / PO / BUYYER…" />
        {/* ลูกค้าที่ตั้งกติกาไว้ที่หน้า Monitor Yarn ขึ้นก่อน ตามด้วยชื่อ BUYYER อื่นในไฟล์ */}
        <label className="mcav-range" title="ตั้งกติกาลูกค้าได้ที่หน้า Monitor Yarn">
          <span className="lb">CUSTOMER</span>
          <select value={cust} onChange={e => { setCust(e.target.value); setLimit(PAGE) }}>
            <option value="">ทุกลูกค้า (ไม่กรอง)</option>
            {custList.length > 0 && (
              <optgroup label="มีกติกา lot">
                {custList.map(c => <option key={c} value={c}>{c}</option>)}
              </optgroup>
            )}
            {otherCusts.length > 0 && (
              <optgroup label="ลูกค้าอื่น">
                {otherCusts.map(c => <option key={c} value={c}>{c}</option>)}
              </optgroup>
            )}
          </select>
        </label>
        {/* ซ่อน/แสดง แถวลูกเล็ก (แถวสีแดง) — มีผลทั้งสองมุมมอง รวมยอดรวม LOT ; ไม่อยู่ใน "ล้างตัวกรองทั้งหมด"
            เพราะค่าเริ่มต้นคือซ่อน ถ้านับเป็นตัวกรอง ปุ่มล้างจะโผล่ทุกครั้งที่เปิดหน้า */}
        <div className="mcav-range ys-lowtoggle"
          title={`แถวที่ YARN_WEIGHT ต่ำกว่า ${raw(ywMin)} (ขั้นต่ำตั้งที่หน้า Monitor Yarn) — ซ่อนแล้วยอดรวม LOT ไม่นับแถวพวกนี้`}>
          <span className="lb">YARN_WEIGHT &lt; {raw(ywMin)}</span>
          <button className={'chip' + (hideLow ? ' on' : '')}
            onClick={() => { setHideLow(true); setLimit(PAGE) }}>ซ่อน</button>
          <button className={'chip' + (hideLow ? '' : ' on')}
            onClick={() => { setHideLow(false); setLimit(PAGE) }}>แสดง</button>
        </div>
        {filterOn && (
          <button className="chip" onClick={() => { setQ(''); setCust(''); setLimit(PAGE) }}>
            ล้างตัวกรองทั้งหมด
          </button>
        )}
      </div>
      {cust && (
        <div className="ys-rulenote">
          <b>{cust}</b>{' '}
          {custRules.length
            ? <>ใช้ได้เฉพาะ {custRules.map(ruleText).join(' หรือ ')}
              {custRules.some(r => String(r.item ?? '').trim()) && ' · ITEM อื่นใช้ได้ทุก lot ที่ไม่ได้สงวนให้ลูกค้าอื่น'}</>
            : 'ไม่มีกติกาของตัวเอง — ใช้ได้ทุก lot ที่ไม่ได้สงวนให้ลูกค้าอื่น'}
          {otherRules.length > 0 && (
            <> · ไม่รวม lot ที่สงวนให้{' '}
              {ruleCustomers(otherRules)
                .map(c => `${c} (${rulesOf(otherRules, c).map(ruleText).join(' / ')})`).join(', ')}</>
          )}
          {' '}· แสดงทุกแถวในคลังที่ใช้ได้ ไม่ว่า BUYYER จะเป็นใคร
        </div>
      )}

      <div className="mcav-card">
        <div className="hd">
          <div className="ys-hdtext">
            <h2>{byLot ? 'ITEM → LOT' : 'ITEM → LOT → PO'}</h2>
            <div className="sub">
              {byLot ? (
                <>
                  1 แถว = 1 lot · ITEM ก่อน ตามด้วย lot ของด้ายตัวนั้น เรียง A→Z ·
                  TOTAL_WEIGHT / TOTAL_YARN = ผลรวมของทั้ง lot (ไม่เปลี่ยนตามคำค้น)
                  {hideLow && <> ไม่นับแถว YARN_WEIGHT ต่ำกว่า {raw(ywMin)} (กด “แสดง” ด้านบนเพื่อนับด้วย)</>} ·
                  ดู PO / BUYYER / YARN_WEIGHT รายแถวได้ที่ปุ่ม “รายละเอียด PO”
                  {poSufs.length > 0 && <> · ไม่นับ PO ที่ลงท้ายด้วย {poSufs.join(', ')} (ไม่เอาไปทำแผน · เปิด/ปิดได้ที่หน้า Monitor Yarn)</>}
                </>
              ) : (
                <>
                  ช่อง ITEM / LOT / PO คร่อมทุกแถวของกลุ่มตัวเอง · 1 แถว = 1 แถวของ view
                  (view แยกแถวตามใบรับเข้า × ที่เก็บ — PO เดียวกันจึงมีหลายแถวได้ ดูที่เก็บได้ที่ LOCATOR_ID) ·
                  เรียง ITEM → LOT → PO แบบ A→Z ·
                  YARN_WEIGHT <span className="ys-yw-low">ต่ำกว่า {raw(ywMin)} สีแดง</span> /
                  <span className="ys-yw-ok"> ตั้งแต่ {raw(ywMin)} ขึ้นไปสีเขียว</span>
                  {hideLow && <> (แถวสีแดงซ่อนอยู่ — กด “แสดง” ด้านบนเพื่อดู)</>}
                  {rules.length > 0 && <> · <span className="ys-buyer-bad">BUYYER สีส้ม</span> = ถือ lot ที่สงวนให้ลูกค้าอื่น</>}
                  {poSufs.length > 0 && <> · ไม่แสดง PO ที่ลงท้ายด้วย {poSufs.join(', ')} (ไม่เอาไปทำแผน · เปิด/ปิดได้ที่หน้า Monitor Yarn)</>}
                </>
              )}
            </div>
          </div>
          {/* สลับมุมมอง — เปลี่ยนแล้วเริ่มนับแถวที่ render ใหม่ (จำนวนแถวของสองมุมมองต่างกันมาก) */}
          <div className="ys-views">
            <button className={'chip' + (byLot ? ' on' : '')}
              onClick={() => { setView('lot'); setLimit(PAGE) }}>รวมต่อ LOT</button>
            <button className={'chip' + (byLot ? '' : ' on')}
              onClick={() => { setView('po'); setLimit(PAGE) }}>รายละเอียด PO</button>
          </div>
        </div>
        {tree.length === 0
          ? <div className="mcav-empty sm">{all.length ? 'ไม่มีรายการที่ตรงกับตัวกรอง' : 'ยังไม่มีข้อมูล'}</div>
          : (
            <div className="otd-tablebox ys-tablebox">
              <table className={'otd-table ys-table' + (byLot ? ' ys-bylot' : '')}>
                <thead>
                  <tr>
                    <th>ITEM</th>
                    <th>LOT</th>
                    {!byLot && <th>PO</th>}
                    {!byLot && <th>LOCATOR_ID</th>}
                    {!byLot && <th title="ประเภทการรับเข้า — ด้ายรับเข้ามาจากไหน (view ไม่มีคอลัมน์โรงงาน)">RECEIVED_TYPE</th>}
                    {!byLot && <th>BUYYER</th>}
                    <th className="n">TOTAL_WEIGHT</th>
                    <th className="n">TOTAL_YARN</th>
                    {!byLot && <th className="n">YARN_WEIGHT</th>}
                  </tr>
                </thead>
                <tbody>{rows}</tbody>
              </table>
            </div>
          )}
        {tree.length > nItems && (
          <div className="cs-more">
            <button className="tbtn" onClick={() => setLimit(l => l + PAGE)}>แสดงเพิ่ม</button>
          </div>
        )}
      </div>
    </div>
  )
}

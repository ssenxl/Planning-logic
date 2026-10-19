import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { useAutoSave } from '../autosave.js'
import SaveStatus from './SaveStatus.jsx'
import { norm } from './ColumnFilter.jsx'
import CalendarEditor from './CalendarEditor.jsx'
import LockMcEditor from './LockMcEditor.jsx'
import S9Editor, { s9Enums } from './S9Editor.jsx'
import ConfigEditor from './ConfigEditor.jsx'
import DataTable from './DataTable.jsx'
import MasterMcTable from './MasterMcTable.jsx'
import McTotalEditor from './McTotalEditor.jsx'
import JobDoneEditor from './JobDoneEditor.jsx'
import McHub from './McHub.jsx'
import S9Hub from './S9Hub.jsx'
import SheetImport from './SheetImport.jsx'
import { catalogFor, describe, sheetOf, splitColumns } from '../masterCatalog.js'

// ชื่อชีท/คำอธิบาย/ชีทไหนใช้บ่อย ย้ายไปอยู่ทะเบียนเดียวที่ masterCatalog.js
// (เดิมมี USED_SHEETS / KEY_SHEETS กระจายอยู่ที่นี่ — เมนูซ้ายไม่ได้โชว์ชื่อชีทดิบแล้ว)

// normalize เกจให้ตรงกับที่ Planning/AVA ใช้ (ตัด G / GAUGE ออก) เช่น "20G" → "20"
const normGauge = v => String(v ?? '').trim().toUpperCase().replace('GAUGE', '').replace('G', '').trim()
const numCmp = (a, b) => String(a).localeCompare(String(b), 'th', { numeric: true })
const sortSet = s => [...s].sort(numCmp)

// สร้าง option ให้ dropdown ของ Lock_MC / MC_Total จากชีทหลัก MasterMC + รายการสัปดาห์ของแผน
// - cats: รายชื่อกลุ่มเครื่อง (MC_CAT) ทั้งหมด
// - gaugeByCat / mcByCat / facByCat: เกจ / เครื่อง (FA/SKP) / โรงงาน ของแต่ละกลุ่ม (cascading)
// - totalByCatGauge: จำนวนเครื่องเดิม "CAT|เกจ" → { all, byFac } ให้ MC_Total เทียบค่าเดิมได้
// - mcByCatGauge: เครื่องที่มีจริงใน "CAT|เกจ" นั้น (mcByCat รวมทุกเกจ → กว้างไป)
// - poolByMc: "CAT|เกจ|MC" → { label, pool, total, reserved, mcs } = พูลของเครื่องนั้น
//   พูล = หน่วยที่ระบบนับเครื่องจริง — กติกาเดียวกับ _MC_TO_POOL ใน AVA_MC:
//     SINGLE-32 ที่มี Group → "Factory|Group" · Group ที่คร่อมหลาย cat/เกจ → "Factory|Group"
//     ที่เหลือ → "Factory|CAT:เกจ" (เครื่องทุกตัวใน cat+เกจ นั้นเป็นพูลเดียว)
//   total = ยอดทั้งกอง "รวม" เครื่องที่ MC Special กันไว้ให้ POLY/COTTON (reserved = ส่วนที่กันไว้)
//   — หน่วยเดียวกับยอดที่กรอกใน MC_Total : AVA_MC หักเครื่องกันไว้ออกจากยอดที่กรอกเอง (McTotal.py)
// รองรับ merged cell ในไฟล์ (MC_CAT/MC/Factory เว้นว่างในแถวถัดไป) ด้วยการ forward-fill
// - calWeeks: ลำดับ [ปี, สัปดาห์] ตามเวลาจริง (ใช้กางช่วงข้ามปีให้ตรงกับ Calendar.week_span)
function buildLockOpts(main, weekMap, calWeeks, special) {
  const cats = []
  const gaugeByCat = {}
  const mcByCat = {}
  const facByCat = {}
  const factories = new Set()
  const totalByCatGauge = {}
  const mcByCatGauge = {}
  const rows = []              // แถว Master MC ที่ใช้ได้ (หลัง forward-fill)
  const groupSpan = {}         // Group → { Factory → Set("CAT|เกจ") } ใช้หา cross-pool group
  if (main && main.columns) {
    const up = main.columns.map(c => String(c).trim().toUpperCase())
    const catCi = up.indexOf('MC_CAT')
    const gCi = up.indexOf('GUAGE') >= 0 ? up.indexOf('GUAGE') : up.indexOf('GAUGE')
    const mcCi = up.indexOf('MC')
    const facCi = up.indexOf('FACTORY')
    const totCi = up.indexOf('TOTAL MC')
    const grpCi = up.indexOf('GROUP')
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
      const hasTot = !isNaN(tot) && String(r[totCi] ?? '').trim() !== ''
      if (g && hasTot) {
        const k = `${cat.toUpperCase()}|${g}`
        const slot = totalByCatGauge[k] || (totalByCatGauge[k] = { all: 0, byFac: {} })
        slot.all += tot
        if (fac) {
          const fk = fac.toUpperCase()
          slot.byFac[fk] = (slot.byFac[fk] || 0) + tot
        }
      }
      if (g && mc) {
        const cg = `${cat.toUpperCase()}|${g}`
        ;(mcByCatGauge[cg] || (mcByCatGauge[cg] = new Set())).add(mc)
        const grp = grpCi >= 0 ? String(r[grpCi] ?? '').trim() : ''
        rows.push({ cat, g, mc, fac, grp, tot: hasTot ? tot : 0 })
        if (grp) {
          const byFac = groupSpan[grp] || (groupSpan[grp] = {})
          ;(byFac[fac] || (byFac[fac] = new Set())).add(cg)
        }
      }
    }
  }
  // Group ที่คร่อมหลาย (cat, เกจ) ในโรงงานเดียว = cross-pool → ใช้ Group เป็นพูล
  const crossGroups = new Set(
    Object.entries(groupSpan)
      .filter(([, byFac]) => Object.values(byFac).some(s => s.size > 1))
      .map(([grp]) => grp))
  // ป้ายพูล — ต้องสะกดตรงกับ MC_POOL ที่ AVA_MC เขียนลงชีท SUMMARY_MC_REMAIN
  const poolKeyOf = ({ cat, g, fac, grp }) => {
    const pfx = fac ? `${fac}|` : ''
    if (grp && (cat.toUpperCase() === 'SINGLE-32' || crossGroups.has(grp))) return `${pfx}${grp}`
    return `${pfx}${cat.toUpperCase()}:${g}`
  }
  const poolAcc = {}      // poolKey → { label, total, reserved, mcs:Set }
  const poolOfMc = {}     // "CAT|เกจ|MC" → poolKey
  for (const r of rows) {
    const pk = poolKeyOf(r)
    const p = poolAcc[pk] || (poolAcc[pk] = {
      label: r.grp || `${r.cat} Guage ${r.g}`, total: 0, reserved: 0, mcs: new Set(),
    })
    p.mcs.add(r.mc)
    p.total += r.tot
    poolOfMc[`${r.cat.toUpperCase()}|${r.g}|${r.mc.toUpperCase()}`] = pk
  }
  // MC Special: เครื่องที่กันไว้ให้ POLY/COTTON = ส่วนหนึ่งของกองเดียวกัน → จดไว้ที่ reserved
  // ไม่หักออกจาก total (ยอดที่กรอกใน MC_Total คือทั้งกอง — ถ้าหักที่นี่ เลข "เดิม" จะต่ำกว่าจริง)
  // (AVA_MC ใช้ Group ของ "MC ตัวแรก" ที่ระบุ — ลอกกติกาเดียวกันเพื่อให้จับพูลตรงกัน)
  if (special && special.columns) {
    const su = special.columns.map(c => String(c).trim().toUpperCase())
    const idx = n => su.indexOf(n)
    const sCat = idx('MC_CAT'), sMc = idx('MC'), sG = idx('GUAGE') >= 0 ? idx('GUAGE') : idx('GAUGE')
    const sFac = idx('FACTORY'), sPoly = idx('POLY'), sCot = idx('COTTON')
    const grpOfMc = {}
    for (const r of rows) if (r.grp) grpOfMc[`${r.mc.toUpperCase()}|${r.g}`] = r.grp
    for (const r of special.rows || []) {
      const cat = sCat >= 0 ? String(r[sCat] ?? '').trim() : ''
      const g = sG >= 0 ? normGauge(r[sG]) : ''
      const fac = sFac >= 0 ? String(r[sFac] ?? '').trim() : ''
      if (!cat || !g) continue
      const num = ci => { const v = Number(String(r[ci] ?? '').trim()); return isNaN(v) ? 0 : v }
      const keep = (sPoly >= 0 ? num(sPoly) : 0) + (sCot >= 0 ? num(sCot) : 0)
      if (keep <= 0) continue
      const mc0 = (sMc >= 0 ? String(r[sMc] ?? '').trim().split(',')[0].trim() : '').toUpperCase()
      const grp = mc0 ? grpOfMc[`${mc0}|${g}`] || '' : ''
      const pfx = fac ? `${fac}|` : ''
      const pk = grp ? `${pfx}${grp}` : `${pfx}${cat.toUpperCase()}:${g}`
      const p = poolAcc[pk]
      if (!p) continue
      p.reserved += keep
    }
  }
  const gOut = {}, mOut = {}, fOut = {}
  for (const c of cats) {
    gOut[c] = sortSet(gaugeByCat[c]); mOut[c] = sortSet(mcByCat[c]); fOut[c] = sortSet(facByCat[c])
  }
  const mcgOut = {}
  for (const k of Object.keys(mcByCatGauge)) mcgOut[k] = sortSet(mcByCatGauge[k])
  const poolByMc = {}
  for (const [k, pk] of Object.entries(poolOfMc)) {
    const p = poolAcc[pk]
    poolByMc[k] = {
      pool: pk, group: p.label, total: p.total, reserved: p.reserved, mcs: sortSet(p.mcs),
    }
  }
  return {
    cats: cats.sort(numCmp),
    gaugeByCat: gOut,
    mcByCat: mOut,
    mcByCatGauge: mcgOut,
    poolByMc,
    facByCat: fOut,
    factories: sortSet(factories),
    totalByCatGauge,
    weeks: Object.keys(weekMap || {}).sort(numCmp),
    calWeeks: (calWeeks || []).map(([y, w]) => [Number(y), Number(w)]),
    years: [...new Set((calWeeks || []).map(([y]) => String(y)))].sort(numCmp),
  }
}

const insertAt = (arr, i, v) => { const n = arr.slice(); n.splice(i, 0, v); return n }
const removeAt = (arr, i) => arr.filter((_, j) => j !== i)

// mode = 'view' (ค่าเริ่มต้น) : ดูอย่างเดียว — แท็บ "Master Data" ใช้โหมดนี้
//        'edit'               : แก้ได้จริง — ใช้เฉพาะในหน้า "ตั้งค่าระบบ › ข้อมูลหลัก" (admin เท่านั้น)
// โหมดดูยังรับข่าว live เหมือนเดิม → มีคนแก้จากหน้าตั้งค่าระบบ จอนี้เห็นของใหม่ทันทีโดยไม่ต้องกดอะไร
// canEdit / onGoEdit : ถ้าคนที่เปิดอยู่เป็น admin ให้มีปุ่มพาไปหน้าที่แก้ได้
// only = {file, sheet, ...} → โหมด "ชีทเดียว": ไม่มีเมนูซ้าย เปิดชีทนั้นให้เลย
// ใช้ตอนฝังตารางไว้ท้ายกลุ่มในหน้าตั้งค่าระบบ (ตารางที่ผูกกับกติกาข้อนั้น ๆ)
export default function Masters({ mode = 'view', canEdit = false, onGoEdit, only }) {
  const ro = mode !== 'edit'
  const [files, setFiles] = useState([])
  const [fq, setFq] = useState('')            // ค้นหาไฟล์/ชีทใน sidebar
  const [collapsed, setCollapsed] = useState({})
  const [sel, setSel] = useState(null)
  const [grid, setGrid] = useState(null)      // ข้อมูลเต็ม (source of truth)
  const [ids, setIds] = useState({ rid: [], cid: [] })  // id คงที่ของแถว/คอลัมน์ ใช้เทียบว่าเซลล์ไหนถูกแก้
  const [orig, setOrig] = useState(new Map()) // `${rid}:${cid}` -> ค่าตอนโหลด
  const [origRids, setOrigRids] = useState(new Set())
  const [hist, setHist] = useState([])        // stack สำหรับ undo
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [dirty, setDirty] = useState(false)
  const [dlg, setDlg] = useState(null)           // dialog เพิ่มคอลัมน์: { name, at }
  const [importing, setImporting] = useState(false) // แผงนำเข้าทับทั้งชีท (ไฟล์ / วางจาก Excel)
  // ปีที่หน้าปฏิทินกำลังดู — เก็บไว้ที่นี่เพราะโหลดชีทซ้ำ (มีคนบันทึก / เพิ่มปี) หน้าปฏิทินถูกสร้างใหม่
  // null = ปีปัจจุบัน ; เปิดชีทใหม่จากเมนูทุกครั้งกลับเป็น null (user: เข้าหน้าปฏิทินต้องเริ่มที่ปีปัจจุบัน)
  const [calYear, setCalYear] = useState(null)
  // มุมมองของชีท: 'auto' = ค่าเริ่มต้นของชีทนั้น · 'cal' = มุมมองเฉพาะ (ปฏิทิน/การ์ด) · 'table' = ตาราง
  // ชีทที่เคยเปิดมาเป็น "การ์ด" (Lock_MC, MC_Total, config, MC S9) ตอนนี้เปิดมาเป็นตารางก่อน
  // แต่ยังกดปุ่มสลับกลับไปการ์ดได้ — การ์ดมีช่องช่วยเลือกค่าที่ตารางเปล่าไม่มี
  const [view, setView] = useState('auto')
  const nextId = useRef({ r: 0, c: 0 })
  const revRef = useRef(null)                 // เลขรุ่นของชีทนี้ที่จอนี้ถืออยู่
  const selRef = useRef(null); selRef.current = sel
  const idsRef = useRef(null); idsRef.current = ids
  // ตัวเซฟอ่านตารางล่าสุดจาก ref ไม่ใช่จาก payload ที่คำนวณตอนกด — คำสั่งแก้บางตัว
  // (เช่น LockMcEditor ตั้งกลุ่มเครื่องแล้วตั้งเกจต่อทันที) เรียก setCell สองครั้งในจังหวะ
  // เดียว ถ้าแต่ละครั้งคำนวณตารางใหม่จากค่าที่อ่านมาตอนเริ่ม ครั้งหลังจะทับครั้งแรก
  const gridRef = useRef(null); gridRef.current = grid
  const [savedAt, setSavedAt] = useState(null)
  const [conflict, setConflict] = useState(null)   // มีคนแก้แซง → ให้เลือกว่าจะทำยังไง
  const [liveNote, setLiveNote] = useState(null)   // (โหมดดู) เพิ่งมีคนแก้ชีทนี้ → เราโหลดใหม่ให้แล้ว

  // เซฟให้เองทุกครั้งที่ขยับ — ไม่มีปุ่มบันทึกแล้ว (ดู autosave.js)
  const autosave = useAutoSave(async () => {
    const s = selRef.current
    const g = gridRef.current
    if (!s || !g) return
    try {
      const r = await api.saveSheet(s.name, s.sheet, g.columns, g.rows, revRef.current)
      revRef.current = r.rev
      setSavedAt(Date.now())
      setConflict(null)
      // ยึดข้อมูลที่เพิ่งเซฟเป็นค่าตั้งต้นใหม่ → ไฮไลต์เซลล์ที่แก้หายไป
      const m = new Map()
      const idn = idsRef.current
      g.rows.forEach((r2, i) => g.columns.forEach((_, j) => m.set(`${idn.rid[i]}:${idn.cid[j]}`, norm(r2[j]))))
      setOrig(m); setOrigRids(new Set(idn.rid))
      setDirty(false)
    } catch (e) {
      // 409 = คนอื่นเซฟแซงไปแล้ว ห้ามเขียนทับ — ให้ผู้ใช้เลือกเองว่าจะเอาของใคร
      if (e.conflict) { setConflict(e); return }
      throw e
    }
  })

  useEffect(() => { api.masters().then(setFiles).catch(e => setMsg('โหลดรายการไม่ได้: ' + e.message)) }, [])

  // โหมดชีทเดียว — เปิดให้เลย ไม่ต้องให้ผู้ใช้เลือกจากเมนู
  useEffect(() => {
    if (only?.file && only?.sheet) openSheet(only.file, only.sheet)
  }, [only?.file, only?.sheet])

  // ผู้ใช้แก้อะไรก็ตามในตาราง → เข้าคิวเซฟ (ดักที่นี่จุดเดียว ไม่ต้องไล่ผูกทุกคำสั่งแก้
  // ซึ่งพลาดง่ายและต้องคำนวณตารางใหม่เองทุกจุด)
  useEffect(() => {
    if (!grid || !dirty || ro) return     // โหมดดูอย่างเดียว: ไม่มีทางเขียนกลับไฟล์
    autosave.saveSoon(true)
  }, [grid, dirty, ro])

  // มีคนอื่นเซฟชีทที่เรากำลังเปิดอยู่ → เอาของใหม่มาแสดงทันที
  //   • เราไม่ได้ค้างพิมพ์อะไร → สลับให้เงียบ ๆ (ตัวเลขบนจอจะตรงกับคนอื่นเสมอ)
  //   • เราพิมพ์ค้างอยู่        → ห้ามทับสิ่งที่ยังไม่ได้เซฟ ขึ้นแถบให้กดเอง
  useEffect(() => {
    if (!sel || sel.view) return
    const res = live.RES.master(sel.name, sel.sheet)
    return live.subscribe(res, (ent) => {
      if (ent && ent.rev === revRef.current) return       // ข่าวการเซฟของเราเอง
      // โหมดดูอย่างเดียว: ไม่มีของค้าง → เอาของใหม่มาแสดงเลย แล้วบอกว่าใครเพิ่งแก้
      // (คนแก้อยู่ที่หน้า "ตั้งค่าระบบ › ข้อมูลหลัก" — จอที่เปิดดูอยู่ต้องไม่ค้างของเก่า)
      if (ro) {
        setLiveNote({ by: ent?.by || 'อีกคนหนึ่ง', at: Date.now() })
        openSheet(sel.name, sel.sheet, true)
        return
      }
      if (autosave.dirty || autosave.status === 'saving') {
        setConflict({ message: `${ent?.by || 'อีกคนหนึ่ง'} เพิ่งแก้ชีทนี้`, current: ent, soft: true })
        return
      }
      openSheet(sel.name, sel.sheet, true)
    })
  }, [sel, autosave.dirty, autosave.status, ro])

  // Export ชีทที่เปิดอยู่ — เซฟของที่ค้างคิวลงไฟล์ก่อน ไม่งั้นไฟล์ที่ได้ไม่มีค่าที่เพิ่งพิมพ์
  const [exporting, setExporting] = useState(false)
  async function doExport() {
    if (!sel) return
    setExporting(true); setMsg('')
    try {
      await autosave.flush()
      await api.masterExport(sel.name, sel.sheet)
    } catch (e) { setMsg('Export ไม่สำเร็จ: ' + e.message) }
    finally { setExporting(false) }
  }

  async function openSheet(name, sheet, force = false) {
    // ไม่ต้องถามว่าจะทิ้งของที่ยังไม่บันทึกไหมแล้ว — ระบบเซฟให้เองตลอด
    // แต่ต้อง "รอ" ให้ของที่ค้างคิวลงไฟล์ก่อน ไม่ใช่แค่สั่งแล้วไปต่อ — ไม่งั้นการเซฟที่ค้าง
    // จะไปเกิดตอนที่หน้าเปลี่ยนไปชีทใหม่แล้ว = เอาข้อมูลชีทเก่าไปทับชีทใหม่
    await autosave.flush()
    setLoading(true); setMsg(''); setGrid(null); setSel({ name, sheet })
    setView('auto')
    if (!force) {                        // เปลี่ยนชีท → ปิดแผงนำเข้า (โหลดชีทเดิมซ้ำไม่ต้องปิด)
      setImporting(false)
      setCalYear(null)                   // เข้าหน้าปฏิทินใหม่ = ปีปัจจุบัน ; โหลดซ้ำคงปีที่ดูอยู่
    }
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
      setDirty(false)
      revRef.current = d.rev ?? null       // เลขรุ่น ณ ตอนอ่าน — ใช้กันเขียนทับตอนเซฟ
      setConflict(null)
    } catch (e) { setMsg('อ่านชีทไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }

  // หน้าปฏิทิน › เพิ่มปี: เซฟของที่ค้างก่อน (ไม่งั้นการเซฟที่ตามมาจะชนเลขรุ่นใหม่) → เติมปีบน server
  // → โหลดชีทใหม่แล้วเปิดปีที่เพิ่งเพิ่ม
  async function extendCalendarYear(y) {
    await autosave.flush()
    await api.extendCalendar(y)
    setCalYear(y)
    await openSheet(sel.name, sel.sheet, true)
  }

  const pushHist = p => setHist(h => [...h, p].slice(-200))

  // ---- แก้ข้อมูล ----
  // ด่านสุดท้าย: โหมดดูอย่างเดียวไม่มีทางเปลี่ยนข้อมูลได้เลย แม้ตัวแก้ตัวไหนหลุดมาเรียก
  function setCell(ri, ci, val) {
    if (ro) return
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

  // แถวใหม่ต่อท้ายชีทเสมอ — ตาราง (DataTable) เป็นคนยกขึ้นไปโชว์บนสุดและโฟกัสให้เอง
  function addRow() {
    if (ro) return
    pushHist({ t: 'addRow' })
    setGrid(g => ({ ...g, rows: [...g.rows, g.columns.map(() => '')] }))
    setIds(s => ({ ...s, rid: [...s.rid, nextId.current.r++] }))
    setDirty(true)
  }

  function delRow(ri) {
    if (ro) return
    pushHist({ t: 'delRow', ri, row: grid.rows[ri], rid: ids.rid[ri] })
    setGrid(g => ({ ...g, rows: removeAt(g.rows, ri) }))
    setIds(s => ({ ...s, rid: removeAt(s.rid, ri) }))
    setDirty(true)
  }

  // แทนทั้งชีท (คอลัมน์ + แถว) — ใช้กับมุมมองที่ต้องจัดรูปชีทใหม่ทั้งก้อน (JobDoneEditor)
  // tag เดียวกันติดกัน = พิมพ์ต่อเนื่องในช่องเดิม → undo ก้อนเดียว (เหมือน setCell)
  function replaceGrid(columns, rows, tag) {
    if (ro) return
    setHist(h => {
      const last = h[h.length - 1]
      if (tag && last && last.t === 'grid' && last.tag === tag) return h
      return [...h, { t: 'grid', tag, grid: gridRef.current, ids: idsRef.current }].slice(-200)
    })
    setGrid(g => ({ ...g, columns, rows }))
    setIds({ rid: rows.map(() => nextId.current.r++), cid: columns.map(() => nextId.current.c++) })
    setDirty(true)
  }

  function addColAt(name, at) {
    if (ro) return
    const ci = at == null ? grid.columns.length : at
    pushHist({ t: 'addCol', ci })
    setGrid(g => ({ ...g, columns: insertAt(g.columns, ci, name),
                    rows: g.rows.map(r => insertAt(r, ci, '')) }))
    setIds(s => ({ ...s, cid: insertAt(s.cid, ci, nextId.current.c++) }))
    setDlg(null)
    setDirty(true)
  }

  // ---- undo ----
  // ย้อนกลับ — เซฟทันทีเช่นกัน เพราะการแก้ที่จะถอยนั้นถูกเซฟลงไฟล์ไปแล้ว
  // (ไม่ใช่แค่ค้างอยู่บนจอเหมือนตอนที่ยังมีปุ่มบันทึก) ถ้าไม่เซฟ ของบนจอกับในไฟล์จะคนละเรื่อง
  function undo() {
    if (ro) return
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
    } else if (p.t === 'addCol') {
      setGrid(g => ({ ...g, columns: removeAt(g.columns, p.ci),
                      rows: g.rows.map(r => removeAt(r, p.ci)) }))
      setIds(s => ({ ...s, cid: removeAt(s.cid, p.ci) }))
    } else if (p.t === 'grid') {
      setGrid(p.grid)
      setIds(p.ids)
    }
    setDirty(true)
  }

  // โหลดของจริงจากไฟล์มาแสดงใหม่ (ของที่แก้ไปแล้วถูกเซฟไปแล้ว — ปุ่มนี้ไม่ได้ทิ้งอะไร
  // นอกจากการพิมพ์ที่ยังค้างอยู่ในช่อง) ใช้ตอนอยากเห็นของที่คนอื่นเพิ่งแก้
  function revert() {
    openSheet(sel.name, sel.sheet, true)
  }

  // ---- คีย์ลัด: Ctrl+Z ย้อนกลับ, Ctrl+S บันทึก ----
  useEffect(() => {
    function onKey(e) {
      if (ro) return                       // โหมดดูอย่างเดียว: ไม่มีอะไรให้ย้อน/บันทึก
      if (!(e.ctrlKey || e.metaKey)) return
      const k = e.key.toLowerCase()
      if (k === 'z') { e.preventDefault(); undo() }
      // Ctrl+S ไม่จำเป็นแล้ว (ระบบเซฟให้เอง) แต่คนคุ้นมือจะกด — ให้แปลว่า "เซฟของที่ค้างเดี๋ยวนี้"
      else if (k === 's') { e.preventDefault(); autosave.flush() }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [hist, grid, ids, dirty, sel, ro])

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
  const calView = !!calInfo && view !== 'table'

  // ชีท Lock_MC (มีคอลัมน์ MC_CAT + MC Lock) → เปิดมุมมองการ์ด "กันเครื่อง" ได้
  const lockInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase())
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const catCi = find('MC_CAT', 'MC CAT')
    const lockCi = find('MC LOCK', 'LOCK')
    if (catCi < 0 || lockCi < 0) return null
    return {
      weekCi: find('WEEK', 'W'), catCi, gaugeCi: find('GUAGE', 'GAUGE'), mcCi: find('MC'), lockCi,
      itemCi: find('ITEM', 'ITEM_CODE'), weekToCi: find('WEEK TO', 'ถึงสัปดาห์'),
      yearCi: find('YEAR', 'ปี'), yearToCi: find('YEAR TO', 'ถึงปี'),
    }
  }, [grid])
  // เปิดมาเป็นการ์ดก่อน (user 16 ก.ย. 2569) — กดปุ่ม "🧾 ตาราง" สลับได้เหมือนเดิม
  const lockView = !!lockInfo && view !== 'table'

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
      facCi: find('FACTORY'), gaugeCi: find('GUAGE', 'GAUGE'), mcCi: find('MC'),
      remarkCi: find('REMARK'),
      weekToCi: find('WEEK TO', 'ถึงสัปดาห์'),
      yearCi: find('YEAR', 'ปี'), yearToCi: find('YEAR TO', 'ถึงปี'),
    }
  }, [grid])
  // เปิดมาเป็นการ์ดก่อน (user 16 ก.ย. 2569)
  const mcTotalView = !!mcTotalInfo && view !== 'table'

  // ชีทกลุ่ม S9 (จ้างทอ) ในไฟล์ MasterMC — ตรวจจากหัวคอลัมน์:
  //   MC S9   : Supp (รุ่นใหม่) หรือ MC_CAT + (MC Group/Cap-Day) ไม่มี ITEM_CODE → การ์ด pool เครื่อง
  //   Item S9 : ITEM_CODE + MACHINE_GROUP            → ค้นหา item จ้างทอได้
  //   S9 Only : ITEM_CODE + MC_GAUGE (ไม่มี MACHINE_GROUP) → รายการ item จ้างทอเสมอ
  const s9Info = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase().replace(/\s+/g, ' '))
    const find = (...names) => { for (const n of names) { const i = up.indexOf(n); if (i >= 0) return i } return -1 }
    const itemCi = find('ITEM_CODE', 'ITEM CODE')
    const mgCi = find('MACHINE_GROUP', 'MACHINE GROUP')
    const gaugeCi = find('MC_GAUGE', 'MC GAUGE', 'GUAGE', 'GAUGE')
    // S9 Only มีคอลัมน์ MACHINE_GROUP ได้แล้ว (หน้าจ้างทอเติมให้ 24 ก.ย. 2569) → ชี้ขาดด้วยชื่อชีทก่อน
    if (itemCi >= 0 && String(sel?.sheet || '').trim().toUpperCase() === 'S9 ONLY')
      return { kind: 's9only', itemCi, mgCi, gaugeCi }
    if (itemCi >= 0 && mgCi >= 0) return { kind: 'items9', itemCi, mgCi, gaugeCi }
    if (itemCi >= 0 && gaugeCi >= 0) return { kind: 's9only', itemCi, mgCi: -1, gaugeCi }
    const catCi = find('MC_CAT', 'MC CAT')
    const mcGroupCi = find('MC GROUP', 'MC_GROUP')
    const capCi = find('CAP/DAY', 'CAP / DAY', 'CAP DAY')
    const totalCi = find('TOTAL MC')           // 'Total MC' (เก่า) และ 'Total Mc' (ใหม่) เข้ารูปเดียวกันหลัง toUpperCase
    const wdCi = find('WORKING DAY'), remarkCi = find('REMARK')
    // 2 คอลัมน์ที่เลือกจาก dropdown (user 16 ก.ย. 2569) — ชนิดผ้าที่เจ้านั้นทอ + เครื่องยังใช้อยู่ไหม
    const matCi = find('MATERIAL', 'ประเภทผ้า'), statusCi = find('STATUS', 'สถานะ')
    // ชีทรุ่นใหม่ (16 ก.ย. 2569): Supp | Mc | New CAT | New Gauge | Total Mc | Working day | Remark
    // — ไม่มี MC_CAT / MC Group / Cap/Day แล้ว (Planning.py `_s9_rows_init` อ่านหัวชุดนี้อยู่ก่อนแล้ว)
    // ต้องดู SUPP เป็นตัวชี้ขาด: ถ้าเอา 'MC' ไปเป็นชื่อสำรองของ MC Group เฉย ๆ ชีทอื่นที่มี
    // MC_CAT + MC (Master MC, MC Special, MC substitute) จะถูกจับผิดเป็น MC S9
    const suppCi = find('SUPP', 'SUPPLIER', 'ผู้รับจ้าง')
    if (itemCi < 0 && suppCi >= 0) {
      return {
        kind: 'mcs9', suppCi,
        catCi: catCi >= 0 ? catCi : find('NEW CAT', 'NEW_CAT'),
        mcGroupCi: mcGroupCi >= 0 ? mcGroupCi : find('MC'),
        gaugeCi: gaugeCi >= 0 ? gaugeCi : find('NEW GAUGE', 'NEW_GAUGE'),
        totalCi, capCi, wdCi, remarkCi, matCi, statusCi,
      }
    }
    if (itemCi < 0 && catCi >= 0 && (mcGroupCi >= 0 || capCi >= 0)) {
      return { kind: 'mcs9', suppCi: -1, catCi, mcGroupCi, gaugeCi, totalCi, capCi, wdCi, remarkCi, matCi, statusCi }
    }
    return null
  }, [grid, sel])
  // Item S9 / S9 Only ใช้ตารางของ S9Editor อย่างเดียว — ตารางดิบ 5พันกว่าแถวเปิดแล้วค้าง
  // (MC S9 ยังสลับการ์ด ↔ ตารางได้ เพราะแถวน้อยและบางทีต้องแก้คอลัมน์)
  const s9View = !!s9Info && (view === 'cal' || s9Info.kind !== 'mcs9')

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
      idFields: clean([F(factoryCi, 'Factory'), F(typeCi, 'Type'), F(catCi, 'MC_CAT'), F(mcCi, 'MC'), F(gaugeCi, 'Guage')]),
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
      idFields: clean([F(mcCi, 'เครื่อง (MC)'), F(gaugeCi, 'Guage')]),
      numFields: clean([F(wdCi, 'Working day'), F(whCi, 'Working hour')]),
      noteFields: [], summary: [],
    }

    const spareCi = find('TOTAL SPARE')
    if (spareCi >= 0) return {
      kind: 'spare', headline: '🧰 เครื่องสำรอง (Spare)',
      addLabel: '+ เพิ่มรายการ', itemWord: 'รายการ',
      groupCi: factoryCi, spareCi,
      lineCis: clean([F(catCi, 'MC_CAT'), F(gaugeCi, 'Guage')]),
      summary: clean([F(spareCi, 'สำรองรวม')]),
    }

    const groupCol = find('GROUP')
    if (catCi >= 0 && groupCol >= 0 && find('SUBGROUP', 'SUB GROUP') < 0 && find('TOTAL MC') < 0 && wdCi < 0) return {
      kind: 'substitute', headline: '🔁 เครื่องทดแทน (Substitute)',
      hint: 'กำหนดว่าเครื่อง / Guage ใดใช้แทนกลุ่มใดได้',
      addLabel: '+ เพิ่มเครื่องทดแทน', itemWord: 'รายการ',
      idFields: clean([F(factoryCi, 'Factory'), F(typeCi, 'Type'), F(catCi, 'MC_CAT'), F(mcCi, 'เครื่อง'), F(gaugeCi, 'Guage'), F(groupCol, 'กลุ่ม')]),
      numFields: [], noteFields: [], summary: [],
    }
    return null
  }, [grid])
  // MC Special เปิดมาเป็นการ์ดก่อน (user 16 ก.ย. 2569) — ชีท config อื่น (Item Special /
  // Spare part / MC substitute) ยังเปิดมาเป็นตารางเหมือนเดิม เพราะแถวเยอะกว่าและแก้เป็นตารางไวกว่า
  const CARD_FIRST_CONFIG = new Set(['mcspecial'])
  const configView = !!configInfo
    && (view === 'cal' || (view === 'auto' && CARD_FIRST_CONFIG.has(configInfo.kind)))

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
  const mastermcView = !!mastermcInfo && view !== 'table'

  // ชีท "Job " (การเปลี่ยนเกจที่ทำไปแล้ว): คอลัมน์แรก Type + หัวคอลัมน์เป็นเลขสัปดาห์ 1–53
  // (+ Year ถ้าเป็นรุ่นใหม่) → ตารางแยกปี มีวันที่กำกับทุกสัปดาห์
  const jobInfo = useMemo(() => {
    if (!grid) return null
    const up = grid.columns.map(c => String(c).trim().toUpperCase())
    if (up[0] !== 'TYPE') return null
    const yearCi = up.findIndex(c => c === 'YEAR' || c === 'ปี')
    const weekCols = []
    up.forEach((c, ci) => {
      if (ci === 0 || ci === yearCi || c === '') return
      const w = Number(c)
      if (Number.isInteger(w) && w >= 1 && w <= 53) weekCols.push({ ci, w })
    })
    if (weekCols.length < 10) return null
    return { typeCi: 0, yearCi, weekCols }
  }, [grid])
  const jobView = !!jobInfo && view !== 'table'

  const specialView = calView || lockView || mcTotalView || s9View || configView || mastermcView || jobView

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
    // ชีท MC Special ด้วย — เครื่องที่กันไว้ให้ POLY/COTTON ถูกหักออกจากพูลปกติใน AVA_MC
    // → ต้องหักเหมือนกัน ไม่งั้นยอด "เดิม" บนการ์ดสูงกว่าที่ระบบใช้จริง
    const specialSheet = file?.sheets.find(s => String(s).trim() === 'MC Special') || null
    Promise.all([
      // สัปดาห์ที่มีในปฏิทิน (สดจาก Calendar.xlsx บน server) — ไม่ผูกกับไฟล์แผนรอบก่อน
      // + ลำดับ (ปี, สัปดาห์) ตามเวลาจริง สำหรับกางช่วงที่ข้ามปี
      api.workday().then(d => [d?.cal_days || {}, d?.cal_weeks || []]).catch(() => [{}, []]),
      mainSheet ? api.sheet(sel.name, mainSheet).catch(() => null) : Promise.resolve(null),
      specialSheet ? api.sheet(sel.name, specialSheet).catch(() => null) : Promise.resolve(null),
    ]).then(([[wk, cw], main, special]) => {
      if (!dead) setLockOpts(buildLockOpts(main, wk, cw, special))
    })
    return () => { dead = true }
  }, [isLock, sel, files])

  // ใส่ค่า Item ในการ์ด — ถ้าชีทยังไม่มีคอลัมน์ ITEM ให้สร้างท้ายสุดแล้วใส่ค่าในแถวนั้นทันที
  // (undo ครั้งเดียวคืนทั้งคอลัมน์ได้) → ผู้ใช้แค่พิมพ์ในการ์ด ไม่ต้องกดเพิ่มคอลัมน์เอง
  function setItemCell(ri, val) {
    if (ro) return
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
    setDirty(true)
  }

  // ใส่ค่าช่องเสริมของการ์ด MC_Total / Lock_MC (Week To / Year / Year To) — ถ้าชีทยังไม่มี
  // คอลัมน์นั้น ให้สร้างท้ายสุดแล้วใส่ค่าทันที (เหมือน setItemCell) → แถวเดิมที่ไม่เคยกรอก
  // ไม่กระทบ เพราะค่าว่าง = พฤติกรรมเดิมทั้งฝั่ง AVA_MC และ Planning
  function setAuxCell(ci, colName, ri, val) {
    if (ro) return
    if (ci != null && ci >= 0) { setCell(ri, ci, val); return }
    const newCi = grid.columns.length
    pushHist({ t: 'addCol', ci: newCi })
    setGrid(g => ({
      ...g,
      columns: [...g.columns, colName],
      rows: g.rows.map((r, i) => [...r, i === ri ? val : '']),
    }))
    setIds(s => ({ ...s, cid: [...s.cid, nextId.current.c++] }))
    setDirty(true)
  }
  // ส่งให้การ์ดเป็นชุดเดียว — การ์ดเรียก onAux('weekTo'|'year'|'yearTo'|'mc', ri, val)
  const auxCols = { weekTo: 'Week To', year: 'Year', yearTo: 'Year To', mc: 'MC' }
  const makeAux = info => (kind, ri, val) =>
    setAuxCell(info?.[`${kind}Ci`], auxCols[kind], ri, val)
  const setMcAux = makeAux(mcTotalInfo)
  const setLockAux = makeAux(lockInfo)

  // เซลล์นี้ถูกแก้จากค่าเดิมไหม (เทียบด้วย id คงที่ ไม่ใช่ index → เพิ่ม/ลบแถวแล้วไม่เพี้ยน)
  function isChanged(ri, ci) {
    const key = `${ids.rid[ri]}:${ids.cid[ci]}`
    const v = norm(grid.rows[ri][ci])
    return orig.has(key) ? orig.get(key) !== v : v !== ''
  }
  const isNewRow = ri => !origRids.has(ids.rid[ri])

  // ---- เมนูซ้าย: รายการ "งาน" ภาษาคน จัดหมวด (ไม่ใช่ชื่อไฟล์/ชีทดิบอีกแล้ว) ----
  // ค้นหาได้ทั้งชื่อไทย คำอธิบาย และชื่อชีทจริง (คนที่คุ้นชื่อ Excel เดิมยังพิมพ์ Lock_MC หาเจอ)
  const q = fq.trim().toLowerCase()
  const catalog = useMemo(() => catalogFor(files), [files])
  const shownEntries = useMemo(() => catalog.filter(c => !q ||
    [c.label, c.what, c.effect, c.sheet, c.file].filter(Boolean).join(' ').toLowerCase().includes(q)),
    [catalog, q])



  // รายการที่เป็น "หน้าเฉพาะ" (เช่น Master MC) ไม่ได้ผูกกับชีทเดียว → เปิดเป็น view
  const openEntry = c => (c.view ? setSel({ view: c.view, entry: c }) : openSheet(c.file, sheetOf(c, files)))
  const isActive = c => sel && (c.view
    ? sel.view === c.view
    : sel.name === c.file && String(sel.sheet).trim() === String(sheetOf(c, files)).trim())
  // ข้อมูลอธิบายของชีทที่เปิดอยู่ — ใช้ทำหัวข้อภาษาคน + บอกว่าแก้แล้วกระทบแผนตรงไหน
  const cur = sel ? (sel.view ? sel.entry : describe(sel.name, sel.sheet)) : null
  // คอลัมน์ที่ระบบวางแผนอ่านจริงของชีทนี้ (ไม่มีทะเบียน = แสดงทุกคอลัมน์ตามเดิม)
  const cmap = useMemo(
    () => (sel && !sel.view && grid ? splitColumns(sel.name, sel.sheet, grid.columns) : null),
    [sel, grid])

  return (
    <div className={'masters' + (only ? ' onesheet' : '')}>
      {!only && <aside className="filelist">
        <h2>{ro ? 'ดูข้อมูลหลัก' : 'แก้ข้อมูลหลัก'}</h2>
        <input className="sheetsearch" placeholder="🔍 ค้นหา เช่น กันเครื่อง, จ้างทอ, วันหยุด"
          value={fq} onChange={e => setFq(e.target.value)} />
        {sel && (
          <button className="mcatback" onClick={() => setSel(null)}>‹ ดูรายการทั้งหมด</button>
        )}
        {!shownEntries.length && <div className="hint small">ไม่พบรายการที่ค้นหา</div>}
        {shownEntries.map(c => (
          <button key={c.view || `${c.file}/${c.sheet || ''}`}
            className={'sheetbtn mcat' + (isActive(c) ? ' active' : '')}
            title={[c.what, c.effect].filter(Boolean).join(' | ')}
            onClick={() => openEntry(c)}>
            <span className="mcatico">{c.icon}</span>
            <span className="mcatname">{c.label}</span>
          </button>
        ))}
      </aside>}

      <section className="editor">
        {/* แถบบอกว่าหน้านี้ดูอย่างเดียว + ทางไปแก้ (admin เท่านั้นที่เห็นปุ่ม) */}
        {ro && (
          <div className="romsg">
            <span className="roicon">👁</span>
            <span>
              หน้านี้ <b>ดูอย่างเดียว</b> — ข้อมูลหลักทั้งหมดแก้ได้ที่หน้า <b>⚙️ Setting › 📁 ข้อมูลหลัก</b>
              {' '}(แก้ที่นั่นแล้วหน้านี้อัปเดตให้เองทันที)
            </span>
            {canEdit && onGoEdit && (
              <button className="primary" onClick={() => onGoEdit(sel)}>ไปหน้าแก้ไข →</button>
            )}
            {!canEdit && <span className="rohint">ต้องเป็นสิทธิ์ Knit หรือ Super Admin ถึงจะแก้ได้</span>}
          </div>
        )}
        {ro && liveNote && (
          <div className="msg note">
            🔄 {liveNote.by} เพิ่งแก้ชีทนี้ — หน้านี้โหลดของล่าสุดให้แล้วเมื่อ{' '}
            {new Date(liveNote.at).toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit' })} น.
          </div>
        )}
        {/* ยังไม่ได้เลือกอะไร → แคตตาล็อก "อยากแก้อะไร" เป็นการ์ด พร้อมบอกผลกับแผน
            (เดิมเป็นข้อความบรรทัดเดียวให้ไปเลือกชีทเอง ซึ่งต้องรู้ชื่อชีทมาก่อน) */}
        {!sel && !only && (
          <div className="mcatalog">
            <div className="mcathead">
              <h2>{ro ? 'ข้อมูลหลักที่ดูได้' : 'จะแก้อะไรดี?'}</h2>
              <div className="hint small">
                เลือกจากรายการด้านล่าง — แต่ละอันบอกไว้ว่าแก้แล้วกระทบการวางแผนตรงไหน
                {q && <> · กำลังค้นหา "{fq}"</>}
              </div>
            </div>
            {!shownEntries.length && <div className="hint">ไม่พบรายการที่ค้นหา</div>}
            <div className="mcatcards">
              {shownEntries.map(c => (
                <button key={c.view || `${c.file}/${c.sheet || ''}`} className="mcatcard key"
                  onClick={() => openEntry(c)}>
                  <div className="mcatcardtop">
                    <span className="ico">{c.icon}</span>
                    <span className="nm">{c.label}</span>
                  </div>
                  {c.what && <div className="mcatwhat">{c.what}</div>}
                  {c.effect && <div className="mcateffect">→ {c.effect}</div>}
                </button>
              ))}
            </div>
          </div>
        )}
        {/* หน้าเฉพาะ (ไม่ผูกกับชีทเดียว) — Master MC รวม 5 ชีทไว้ที่เดียว */}
        {sel?.view === 'mc-hub' && (
          <>
            <div className="editbar">
              <h2><span className="shico">{cur?.icon}</span>{cur?.label || 'Master MC'}</h2>
              <div className="actions">
                {ro
                  ? <span className="robadge">👁 ดูอย่างเดียว</span>
                  : <span className="robadge">รวมจาก 5 ชีทเดิมไว้ที่นี่</span>}
              </div>
            </div>
            {cur?.effect && (
              <div className="sheetabout"><div className="saeffect">⚡ {cur.effect}</div></div>
            )}
            <McHub ro={ro} />
          </>
        )}
        {sel?.view === 's9-hub' && (
          <>
            <div className="editbar">
              <h2><span className="shico">{cur?.icon}</span>{cur?.label || 'Master/S9'}</h2>
              <div className="actions">
                <span className="robadge">{ro ? '👁 ดูอย่างเดียว' : 'รวมจาก 3 ชีทเดิมไว้ที่นี่'}</span>
              </div>
            </div>
            {cur?.effect && (
              <div className="sheetabout"><div className="saeffect">⚡ {cur.effect}</div></div>
            )}
            <S9Hub ro={ro} />
          </>
        )}
        {sel && !sel.view && (
          <>
            <div className="editbar">
              {/* หัวข้อเป็นชื่อภาษาคน ชื่อไฟล์/ชีทจริงย้ายไปเป็นตัวเล็กใต้ชื่อ (ไว้อ้างอิงเวลาคุยกัน) */}
              <h2>
                {cur ? <><span className="shico">{cur.icon}</span>{cur.label}</> : <>{sel.name} / {sel.sheet}</>}
                {autosave.dirty && <span className="dot" title="กำลังจะบันทึกให้เอง">●</span>}
                <span className="shsrc">{sel.name} / {sel.sheet}</span>
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
                {s9Info?.kind === 'mcs9' && !calInfo && !lockInfo && !mcTotalInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของชีท MC S9 (จ้างทอ)">
                    <button className={s9View ? 'on' : ''} onClick={() => setView('cal')}>📇 การ์ด</button>
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
                {jobInfo && (
                  <div className="viewtoggle" title="สลับมุมมองของชีท Job">
                    <button className={jobView ? 'on' : ''} onClick={() => setView('cal')}>📅 แยกปี</button>
                    <button className={!jobView ? 'on' : ''} onClick={() => setView('table')}>🧾 ตาราง</button>
                  </div>
                )}
                {!ro && <button onClick={undo} disabled={!hist.length} title="ย้อนกลับ (Ctrl+Z)">↩ ย้อนกลับ</button>}
                <button className="ghost" onClick={revert} title="โหลดข้อมูลล่าสุดจากไฟล์ใหม่">⟳ โหลดใหม่</button>
                {!ro && (
                  <button onClick={() => setImporting(v => !v)} disabled={!grid}
                    title="แทนที่ข้อมูลทั้งชีทด้วยไฟล์ .xlsx หรือข้อมูลที่คัดลอกจาก Excel">📥 Import</button>
                )}
                <button onClick={doExport} disabled={!grid || exporting}
                  title="ดาวน์โหลดชีทนี้เป็นไฟล์ .xlsx (Import กลับได้)">
                  {exporting ? 'กำลัง Export...' : '📤 Export'}
                </button>
                {!ro && !specialView && <button onClick={() => setDlg({ name: '', at: null })} disabled={!grid}>+ เพิ่มคอลัมน์</button>}
                {ro
                  ? <span className="robadge" title="แก้ได้ที่หน้า Setting">👁 ดูอย่างเดียว</span>
                  : <SaveStatus status={autosave.status} error={autosave.error} at={savedAt}
                    onRetry={autosave.flush} />}
              </div>
            </div>

            {/* ชีทนี้คืออะไร + แก้แล้วกระทบแผนตรงไหน (มาจากทะเบียน masterCatalog.js) */}
            {cur && (
              <div className="sheetabout">
                {cur.what && <div className="sawhat">{cur.what}</div>}
                {cur.effect && <div className="saeffect">⚡ {cur.effect}</div>}
                {cur.via && <div className="savia">💡 {cur.via}</div>}
                {cur.unused && (
                  <div className="saunused">ℹ️ ระบบวางแผนไม่ได้อ่านชีทนี้ — แก้แล้วแผนไม่เปลี่ยน</div>
                )}
                {!ro && !cur.unused && (
                  <div className="sanote">บันทึกเองอัตโนมัติ · รอบจัดแผนถัดไปจะใช้ค่าใหม่</div>
                )}
              </div>
            )}

            {sel.name === 'Target_Stock' && (
              <div className="msg note">
                ℹ️ คอลัมน์ <b>Match core, TARGET SCM, STOCK MIN, STOCK MAX, Stock 5 Week</b> คำนวณอัตโนมัติจาก <b>TARGET/MONTH</b> ตอนกดบันทึก — แก้ TARGET/MONTH แล้วค่าที่เหลือจะอัปเดตเอง
              </div>
            )}
            {conflict && (
              <div className="msg warn conflict-bar">
                ⚠️ {conflict.current?.by || 'อีกคนหนึ่ง'} แก้ชีทนี้ไปแล้ว
                {conflict.current?.at ? ` เมื่อ ${String(conflict.current.at).slice(11, 16)} น.` : ''}
                {conflict.soft
                  ? ' — ของที่คุณพิมพ์ค้างยังไม่ถูกทับ'
                  : ' — การบันทึกครั้งล่าสุดของคุณยังไม่เข้า'}
                <button className="primary" onClick={() => openSheet(sel.name, sel.sheet, true)}>
                  โหลดของล่าสุด
                </button>
              </div>
            )}
            {msg && <div className="msg">{msg}</div>}
            {loading && <div className="hint">กำลังโหลด...</div>}

            {/* Import ทับทั้งชีท — ใช้ได้ทุกมุมมอง (ตาราง/การ์ด/ปฏิทิน) เพราะทุกมุมมองอ่านจาก grid ก้อนเดียวกัน */}
            {grid && !ro && importing && (
              <SheetImport name={sel.name} sheet={sel.sheet} grid={grid}
                onApply={rows => { replaceGrid(grid.columns, rows); setImporting(false) }}
                onClose={() => setImporting(false)} />
            )}

            {grid && calView && (
              <CalendarEditor
                grid={grid}
                dateCi={calInfo.dateCi}
                statusCi={calInfo.statusCi}
                weekCi={calInfo.weekCi}
                isChanged={isChanged}
                onToggle={(ri, val) => setCell(ri, calInfo.statusCi, String(val))}
                ro={ro}
                year={calYear}
                onYear={setCalYear}
                onExtendYear={ro ? null : extendCalendarYear}
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
                onAux={setLockAux}
                ro={ro}
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
                onAux={setMcAux}
                ro={ro}
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
                ro={ro}
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
                ro={ro}
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
                ro={ro}
                cmap={cmap}
              />
            )}

            {grid && jobView && (
              <JobDoneEditor grid={grid} info={jobInfo} replaceGrid={replaceGrid} ro={ro} />
            )}

            {grid && !specialView && (
              <DataTable
                grid={grid}
                rids={ids.rid}
                setCell={setCell}
                addRow={addRow}
                delRow={delRow}
                isChanged={isChanged}
                isNewRow={isNewRow}
                enums={s9Enums(s9Info)}
                ro={ro}
              />
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

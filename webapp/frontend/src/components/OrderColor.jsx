import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { norm } from './ColumnFilter.jsx'
import PlanGantt from './PlanGantt.jsx'

// แสดงตัวเลขทศนิยมไม่เกิน 2 ตำแหน่ง (จำนวนเต็ม/ข้อความคงเดิม)
const fmtNum = (v) => (typeof v === 'number' && !Number.isInteger(v))
  ? String(Math.round(v * 100) / 100)
  : norm(v)

function fmtSize(b) {
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}
function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

// จำแนกประเภทโควตา job (ต้องตรงกับ classifyType ใน PlanGantt.jsx)
const jobTypeOf = (f, cat) => {
  const s = String(f ?? '').trim().toUpperCase()
  if (s.startsWith('OM')) return 'OM'
  if (s === 'PHET') return String(cat ?? '').toUpperCase().includes('DOUBLE') ? 'PHET_DOUBLE' : 'PHET_SINGLE'
  return null
}

// สถานะ item สี → ป้าย (label + สี)
const STATUS = {
  OK: { label: 'ทันย้อม', color: '#1a7f37', bg: '#e6f4ea' },
  LATE: { label: 'ช้าเกิน', color: '#b7791f', bg: '#fdf3e0' },
  MISSING: { label: 'ยังไม่มีในแผน', color: '#c0392b', bg: '#fdeceb' },
}
const wk = (w) => (w || w === 0) ? 'W' + w : '-'

export default function OrderColor() {
  const [meta, setMeta] = useState(null)       // { exists, name, size, mtime, sheets }
  const [catHist, setCatHist] = useState(null) // { current_week, weeks, cats, note }
  const [chLoading, setChLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [runStatus, setRunStatus] = useState({})
  const [advice, setAdvice] = useState(null)
  const [advising, setAdvising] = useState(false)
  const [basePlan, setBasePlan] = useState(null)  // {columns, rows, color_codes, color_meta, ...} — ไม่แก้ (source of truth)
  const [choices, setChoices] = useState({})       // { colorBaseIdx: removeBaseIdx }  — งานสีนี้เลือกถอดงานไหน
  const [overrides, setOverrides] = useState({})   // { baseIdx: week }                — ลากย้ายเอง
  const [removed, setRemoved] = useState(() => new Set())  // Set(baseIdx)             — คลิกลบเอง
  const [qtyEdits, setQtyEdits] = useState({})     // { baseIdx: qty }                 — double click แก้จำนวน (กก.) บนบล็อก
  const [catFilter, setCatFilter] = useState('')   // ดูทีละ CAT ('' = ยังไม่เลือก)
  const [pLoad, setPLoad] = useState({})
  const [pAva, setPAva] = useState({})
  const [poolMap, setPoolMap] = useState({})   // map เครื่อง→พูล (SKP vs SKPTA/SKPLE)
  const [planning, setPlanning] = useState(false)
  const [aiAdvice, setAiAdvice] = useState(null)   // { summary, ranking, ai, note }
  const [aiLoading, setAiLoading] = useState(false)
  const [aiPreview, setAiPreview] = useState(null) // รายการปรับที่รอ user ยืนยัน (ไม่ apply อัตโนมัติ)
  const [showHelp, setShowHelp] = useState(false)  // ซ่อน/แสดงคำอธิบายวิธีใช้ (กันหน้ารก)
  const prevRunning = useRef(false)

  async function loadMeta() {
    const m = await api.orderColorLatest()
    setMeta(m)
    return m
  }
  async function loadRunStatus() {
    try { setRunStatus(await api.runStatus()) } catch { }
  }

  // มุมมองหลัก: booking รายสัปดาห์จัดกลุ่มตาม CAT (เฉพาะ CAT ที่มี item สี)
  async function loadCatHist() {
    setChLoading(true); setMsg('')
    try {
      setCatHist(await api.orderColorCatHistory())
    } catch (e) {
      setCatHist(null)
      setMsg('โหลดข้อมูลไม่ได้: ' + e.message)
    } finally { setChLoading(false) }
  }

  useEffect(() => {
    loadMeta()
      .then(m => { if (m?.exists) buildPlan() })   // default = Gantt ที่ลาก/ถอดได้ (PlanGantt)
      .catch(e => setMsg('โหลดไม่ได้: ' + e.message))
    loadRunStatus()
    const t = setInterval(loadRunStatus, 2000)
    return () => clearInterval(t)
  }, [])

  // เมื่อรอบรัน (pull) เพิ่งจบ → รีเฟรชไฟล์ + สร้าง Gantt ใหม่
  useEffect(() => {
    if (prevRunning.current && !runStatus.running) {
      loadMeta()
        .then(m => { if (m?.exists) buildPlan() })
        .catch(() => { })
    }
    prevRunning.current = !!runStatus.running
  }, [runStatus.running])

  // fallback: ถ้ายังไม่มี Gantt (เช่น ไม่มีไฟล์แผน หรือกดปิด) → โหลดสรุป booking ตาม CAT×เกจ (อ่านอย่างเดียว)
  useEffect(() => {
    if (!basePlan && !advice && !planning && meta?.exists && !catHist && !chLoading) loadCatHist()
  }, [basePlan, advice, planning, meta, catHist, chLoading])

  async function pull() {
    setMsg('')
    try {
      const r = await api.run('map-item')
      setMsg(r.message)
      setTimeout(loadRunStatus, 300)
    } catch (e) { setMsg('สั่งดึงข้อมูลไม่ได้: ' + e.message) }
  }

  async function runAdvise() {
    setAdvising(true); setMsg(''); setBasePlan(null)
    try {
      setAdvice(await api.orderColorAdvise())
    } catch (e) {
      setMsg('วิเคราะห์ไม่ได้: ' + e.message)
      setAdvice(null)
    } finally { setAdvising(false) }
  }

  async function buildPlan() {
    setPlanning(true); setMsg(''); setAdvice(null)
    try {
      // default = ทุก item จาก booking (ไม่ใช่แผนที่บาง) → ลาก/ถอดได้บน PlanGantt
      // โหลด job ใช้ booking-load (baseline 0 — job คิด live จาก NEW_MC ของแถว ขยับตามการย้าย)
      const [d, ld, av, pm] = await Promise.all([
        api.orderColorBookingGantt(), api.orderColorBookingLoad(), api.orderColorBookingAva(),
        api.planPoolMap().catch(() => ({}))])
      setBasePlan(d); setChoices({}); setOverrides({}); setRemoved(new Set()); setQtyEdits({}); setCatFilter('')
      setAiAdvice(null); setAiPreview(null)
      setPLoad(ld || {}); setPAva(av || {}); setPoolMap(pm || {})
    } catch (e) {
      setMsg('สร้าง Gantt ไม่ได้: ' + e.message); setBasePlan(null)
    } finally { setPlanning(false) }
  }

  // ---- helper คำนวณเครื่องว่าง live (ให้ตรงกับ PlanGantt) ----
  const gnorm = (g) => {
    const s = String(g ?? '').trim().toUpperCase().replace('GAUGE', '').replace('G', '')
    const n = parseInt(parseFloat(s), 10)
    return Number.isFinite(n) ? String(n) : s
  }
  const cidx = useMemo(() => {
    if (!basePlan) return {}
    const at = (n) => basePlan.columns.indexOf(n)
    const due = at('RDD_WEEK') >= 0 ? at('RDD_WEEK') : at('FG_WEEK')
    const qty = at('PRODUCE_QTY') >= 0 ? at('PRODUCE_QTY') : at('PLAN_QTY')
    return { week: at('PLAN_WEEK'), item: at('ITEM_CODE'), cat: at('CAT'), gauge: at('MC_GUAGE'), amc: at('ACTUAL_MC'), mcg: at('MC_GROUP'), qty, due, so: at('SO_NO'), team: at('TEAM_NAME'), po: at('PO_WEEK') }
  }, [basePlan])
  const avaWeeks = useMemo(
    () => Object.keys(pAva || {}).map(Number).filter(Number.isFinite).sort((a, b) => a - b),
    [pAva])
  const colorCodeSet = useMemo(
    () => new Set((basePlan?.color_codes || []).map(c => String(c).trim().toUpperCase())),
    [basePlan])
  const isColorRow = (row) => cidx.item >= 0 && colorCodeSet.has(String(row[cidx.item] ?? '').trim().toUpperCase())
  // cgView = cat|gauge (ใช้จัดกลุ่ม/กรองแท็บ) · cgKey = key ต่อพูล (ใช้หาเครื่องว่าง/ถอดงาน)
  //   พูลแยก เช่น SKP กับ SKPTA/SKPLE ต้องคิดเครื่องแยกกัน → ต้องใช้ pool key ตาม poolMap
  const cgView = (row) => String(row[cidx.cat] ?? '').trim() + '|' + gnorm(row[cidx.gauge])
  const poolKeyOf = (cat, gauge, mcgroup) => {
    const cg = String(cat ?? '').trim() + '|' + gnorm(gauge)
    const mg = String(mcgroup ?? '').trim().toUpperCase()
    return (mg && poolMap[cg + '|' + mg]) || cg
  }
  const cgKey = (row) => poolKeyOf(row[cidx.cat], row[cidx.gauge], cidx.mcg >= 0 ? row[cidx.mcg] : '')

  function occAt(rows, w, cg) {
    let s = 0
    for (const r of rows) {
      if (String(r[cidx.week]) === String(w) && cgKey(r) === cg) s += Number(r[cidx.amc]) || 0
    }
    return s
  }
  function remainAt(rows, w, cg) {
    const slot = pAva[String(w)]?.[cg]
    if (!slot) return null
    return slot.remain - (occAt(rows, w, cg) - (slot.planBase || 0))
  }
  // เครื่องว่าง live ของ week โดยข้ามแถวใน skip (หาง run ที่กำลังจะย้ายออก — ไม่นับเครื่องที่เดิม)
  function remainSkip(rowsW, w, cg, skip) {
    const slot = pAva[String(w)]?.[cg]
    if (!slot) return null
    let occ = 0
    for (const x of rowsW) {
      if (skip && skip.has(x.bi)) continue
      if (String(x.row[cidx.week]) === String(w) && cgKey(x.row) === cg) occ += Number(x.row[cidx.amc]) || 0
    }
    return slot.remain - (occ - (slot.planBase || 0))
  }
  // โควตา job setup ที่เหลือของ week ตามประเภทเครื่อง (ไม่มีข้อมูลโควตา = ไม่จำกัด)
  function jobFreeAt(rowsW, w, t, skip) {
    const info = pLoad?.[String(w)]?.[t]
    const iNm = basePlan ? basePlan.columns.indexOf('NEW_MC') : -1
    const iFac = basePlan ? basePlan.columns.indexOf('FACTORY_TYPE') : -1
    if (!info || iNm < 0) return Infinity
    let used = Number(info.old) || 0
    for (const x of rowsW) {
      if (skip && skip.has(x.bi)) continue
      if (String(x.row[cidx.week]) !== String(w)) continue
      if (jobTypeOf(iFac >= 0 ? x.row[iFac] : '', x.row[cidx.cat]) !== t) continue
      used += Number(x.row[iNm]) || 0
    }
    return (Number(info.cap) || 0) - used
  }
  // หา week ปลายทางให้แถวที่ถูกถอด (ไล่จาก afterWeek+1 บนแกนทำงาน):
  //   (ก) เครื่องว่าง ≥ เครื่องที่แถวใช้
  //   (ข) ถ้าลงแล้วต้อง setup ใหม่ (แถวเป็น setup เอง หรือ gap จาก anchor > 3 สัปดาห์)
  //       → job setup ของ week นั้นต้องเหลือพอ
  // ไม่มี week ผ่านเงื่อนไข → week ท้ายสุดของแกน (ok=false ให้ UI เตือน)
  function placeDisplaced(rowsW, xrow, afterWeek, anchorWeek, skip) {
    const cg = cgKey(xrow)
    const needMc = Math.max(1, Number(xrow[cidx.amc]) || 1)
    const iSd = basePlan.columns.indexOf('SETUP_DAYS')
    const iNm = basePlan.columns.indexOf('NEW_MC')
    const iFac = basePlan.columns.indexOf('FACTORY_TYPE')
    const hasSetup = iSd >= 0 && (Number(xrow[iSd]) || 0) > 0
    const t = jobTypeOf(iFac >= 0 ? xrow[iFac] : '', xrow[cidx.cat])
    for (const w of avaWeeks) {
      if (w <= afterWeek) continue
      if ((remainSkip(rowsW, w, cg, skip) ?? 0) < needMc) continue
      const newSetup = !hasSetup && (anchorWeek == null || w - anchorWeek > 3)
      if ((hasSetup || newSetup) && t) {
        const needJobs = hasSetup ? Math.max(1, Number(xrow[iNm]) || needMc) : needMc
        if (jobFreeAt(rowsW, w, t, skip) < needJobs) continue
      }
      return { week: w, ok: true, newSetup }
    }
    const last = avaWeeks.length ? avaWeeks[avaWeeks.length - 1] : afterWeek + 1
    const w = Math.max(last, afterWeek + 1)
    return { week: w, ok: false, newSetup: !hasSetup && (anchorWeek == null || w - anchorWeek > 3) }
  }

  // แถวที่เป็น "งานสี" ระบุด้วย index (เฉพาะ run ที่ครอบจำนวนให้สี — ส่วนเกิน = งานไม่มีสี)
  const colorIdxSet = useMemo(() => new Set(basePlan?.color_idx || []), [basePlan])
  const metaByIdx = useMemo(
    () => new Map((basePlan?.color_meta || []).map(m => [m.idx, m])),
    [basePlan])

  // ---- แผน what-if ที่ derive จาก base + choices + overrides + removed ----
  // choices[firstIdx] = {val, type:'free'|'displace', week?, removeIdx?}
  // การย้ายงานสี = เลื่อน "ทั้ง run" ตามแกนสัปดาห์ทำงานจริง (ข้ามสัปดาห์หยุด เช่น W31) — คงการ carry/setup
  const work = useMemo(() => {
    if (!basePlan || cidx.week < 0) return []
    const wi = cidx.week
    const rows = basePlan.rows.map((r, bi) => ({ row: r.slice(), bi })).filter(x => !removed.has(x.bi))
    const byIdx = new Map(rows.map(x => [x.bi, x]))
    // แกนสัปดาห์ทำงานจริง (จาก AVA — ไม่มีสัปดาห์หยุด) ใช้เลื่อนเป็น "ตำแหน่ง" ไม่ใช่ลบเลขสัปดาห์
    const axis = avaWeeks
    const posOf = new Map(axis.map((w, i) => [w, i]))
    const shiftWeek = (w, offsetPos) => {
      const p = posOf.get(Number(w))
      if (p == null) return Number(w)
      const np = Math.min(Math.max(p - offsetPos, 0), axis.length - 1)
      return axis[np]
    }
    const displacedSet = new Set()   // แถวถูกถอดที่ขาดจาก run เดิม → กลายเป็น setup ใหม่ (กิน job)
    const placeWarns = []            // แถวถูกถอดที่ไม่มี week ไหนเครื่อง/job ว่างพอ (วางไว้ท้ายแผน)
    const iNmW = basePlan.columns.indexOf('NEW_MC')
    for (const [ck, sel] of Object.entries(choices)) {
      if (!sel) continue
      const ci0 = Number(ck)
      const first = byIdx.get(ci0)
      if (!first) continue
      let target = null
      let pending = null             // หาง run ที่ถูกถอด — รอวางหลังย้ายงานสีเข้า target แล้ว
      if (sel.type === 'free') {
        target = Number(sel.week)
      } else {
        const rrIdx = Number(sel.removeIdx)
        const rr = byIdx.get(rrIdx)
        if (!rr) continue
        target = Number(basePlan.rows[rrIdx][wi])
        // งานที่ถูกถอด: เลื่อน "หางของ run" (แถวนี้ + แถวถัดๆ ไปของแผนต่อเนื่องเดียวกัน)
        const iRun = basePlan.columns.indexOf('RUN_ID')
        const rid = iRun >= 0 ? basePlan.rows[rrIdx][iRun] : null
        const tail = []
        let prevW = null             // สัปดาห์หัว run ที่เหลือก่อน target — ใช้เช็ค gap > 3 (setup ใหม่)
        if (rid != null) {
          for (const x of rows) {
            if (basePlan.rows[x.bi][iRun] !== rid) continue
            if (Number(basePlan.rows[x.bi][wi]) >= target) tail.push(x)
            else prevW = Math.max(prevW ?? -Infinity, Number(x.row[wi]))
          }
          tail.sort((a, b) => Number(basePlan.rows[a.bi][wi]) - Number(basePlan.rows[b.bi][wi]))
          if (prevW === -Infinity) prevW = null
        } else tail.push(rr)
        pending = { tail, prevW }
      }
      // ย้ายงานสี (ทั้ง run) เข้า target ก่อน → ตอนหา week ให้หางที่ถูกถอด เครื่องที่งานสีใช้ถูกนับแล้ว
      const p0 = posOf.get(Number(basePlan.rows[ci0][wi]))
      const pt = posOf.get(Number(target))
      if (p0 != null && pt != null && p0 !== pt) {
        const offsetPos = p0 - pt
        const runRows = metaByIdx.get(ci0)?.run_rows || [ci0]
        for (const bi of runRows) {
          const x = byIdx.get(bi)
          if (x) x.row[wi] = shiftWeek(basePlan.rows[bi][wi], offsetPos)
        }
      }
      // วางหางทีละแถวตามลำดับ: week ต้องมีเครื่องว่างพอ และถ้าขาดจากแถวก่อนหน้า
      // (gap > 3 = setup ใหม่) ต้องมี job setup ว่างพอ — แถวหางที่ยังไม่วางไม่นับเครื่อง/job
      if (pending) {
        const skip = new Set(pending.tail.map(x => x.bi))
        let anchor = pending.prevW
        let after = Number(target)
        for (const x of pending.tail) {
          const res = placeDisplaced(rows, x.row, after, anchor, skip)
          x.row[wi] = res.week
          skip.delete(x.bi)          // วางแล้ว — นับเครื่อง/job ที่ week ใหม่
          if (res.newSetup) {
            displacedSet.add(x.bi)
            // ตั้ง job ชั่วคราวให้แถวถัดไปเห็นโควตาที่ถูกกิน (ค่าจริงคำนวณใหม่ท้าย memo)
            if (iNmW >= 0) x.row[iNmW] = Math.max(1, Number(x.row[cidx.amc]) || 1)
          }
          if (!res.ok) placeWarns.push(`${String(x.row[cidx.item] ?? '')} → W${res.week}`)
          anchor = res.week
          after = res.week
        }
      }
    }
    for (const [bi, w] of Object.entries(overrides)) { const x = byIdx.get(Number(bi)); if (x) x.row[wi] = w }

    // แก้จำนวน (กก.) จาก double click บนบล็อก Gantt → เขียนค่าใหม่ลงแถว
    if (cidx.qty >= 0) {
      for (const [bi, q] of Object.entries(qtyEdits)) {
        const x = byIdx.get(Number(bi)); if (x) x.row[cidx.qty] = q
      }
    }

    // แถวที่ย้ายสัปดาห์ หรือแก้จำนวน → คำนวณจำนวนเครื่องใหม่
    // จำนวนใหม่ scale MC_DAYS ตามสัดส่วน (machine-days ∝ กก.)
    // ACTUAL_MC = ceil( MC_DAYS / (WD_FRAC × วันทำงานสัปดาห์ใหม่ − SETUP_DAYS) )
    // และอัปเดต job (NEW_MC): แถว setup เดิม = เครื่องใหม่, แถวถูกถอดย้ายเดี่ยว = setup ใหม่ (เริ่มกิน job)
    const iMd = basePlan.columns.indexOf('MC_DAYS')
    const iWf = basePlan.columns.indexOf('WD_FRAC')
    const iSd = basePlan.columns.indexOf('SETUP_DAYS')
    const iNm = basePlan.columns.indexOf('NEW_MC')
    if (iMd >= 0 && cidx.amc >= 0) {
      for (const x of rows) {
        const newW = Number(x.row[wi]), baseW = Number(basePlan.rows[x.bi][wi])
        const qe = cidx.qty >= 0 ? qtyEdits[x.bi] : undefined
        const baseQty = cidx.qty >= 0 ? Number(basePlan.rows[x.bi][cidx.qty]) : 0
        const ratio = (qe != null && baseQty > 0) ? qe / baseQty : 1
        if (newW === baseW && ratio === 1) continue
        const md = (Number(basePlan.rows[x.bi][iMd]) || 0) * ratio
        if (ratio !== 1) x.row[iMd] = Math.round(md * 1000) / 1000
        const wf = (iWf >= 0 ? Number(x.row[iWf]) : 1) || 1
        const sd = (iSd >= 0 ? Number(x.row[iSd]) : 0) || 0
        const wm = Number(basePlan.week_days?.[String(newW)]) || 0
        if (md > 0 && wm > 0) {
          const eff = Math.max(1, wf * wm - sd)
          x.row[cidx.amc] = Math.max(1, Math.ceil(md / eff - 1e-9))
        }
        if (iNm >= 0) {
          const baseNm = Number(basePlan.rows[x.bi][iNm]) || 0
          if (displacedSet.has(x.bi) || sd > 0) {
            // แถว setup เต็ม (มีวัน setup) หรือถูกถอดย้ายเดี่ยว → job = จำนวนเครื่องใหม่ทั้งหมด
            x.row[iNm] = Number(x.row[cidx.amc]) || 1
          } else if (baseNm > 0) {
            // แถวเครื่องเพิ่มกลาง run (_mc_increase) → คงจำนวนส่วนเพิ่มเดิม
            x.row[iNm] = baseNm
          }
        }
      }
    }
    rows.warns = placeWarns          // แถวถูกถอดที่หา week ลงไม่ได้ — โชว์แถบเตือนบน UI
    return rows
  }, [basePlan, choices, overrides, removed, qtyEdits, pAva, pLoad, cidx, metaByIdx, avaWeeks])

  const planRows = useMemo(() => work.map(x => ({ row: x.row, idx: x.bi })), [work])
  const colorRows = useMemo(() => {
    const s = new Set()
    for (const x of work) if (colorIdxSet.has(x.bi)) s.add(x.bi)
    return s
  }, [work, colorIdxSet])

  // ---- งานสี "ทุกตัว" ในแผน + สถานะ (พอเครื่อง/ต้องถอด) + ตัวเลือกถอด (AI แนะนำ) ----
  const swapList = useMemo(() => {
    if (!basePlan || !basePlan.color_meta) return []
    const workRows = work.map(x => x.row)
    const out = []
    for (const m of basePlan.color_meta) {
      if (removed.has(m.idx)) continue
      const wx = work.find(x => x.bi === m.idx)
      if (!wx) continue
      const curWeek = Number(wx.row[cidx.week])
      const cg = poolKeyOf(m.cat, m.gauge, cidx.mcg >= 0 ? wx.row[cidx.mcg] : '')
      const editFrom = Number(basePlan.edit_from ?? 0)
      const deadline = m.deadline
      const remain = remainAt(workRows, curWeek, cg)
      const locked = curWeek < editFrom                 // อยู่ในช่วง freeze — แก้ไม่ได้
      // เป้าหมาย: งานสีต้องได้ทอเร็วสุด — ทุกตัวที่ทออยู่ week ไกลกว่า editFrom ลองดึงเข้ามาก่อน
      const canEarlier = !locked && curWeek > editFrom
      let cands = []
      if (canEarlier) {
        // (1) week ก่อนหน้า (editFrom..curWeek-1) ที่เครื่องว่าง → ย้ายเข้าได้เลย ไม่ต้องถอดใคร
        for (const w of avaWeeks) {
          if (w < editFrom || w >= curWeek) continue
          if ((remainAt(workRows, w, cg) ?? 0) > 0) {
            cands.push({ free: true, week: w, moveTo: w, target: w, item: 'เครื่องว่าง', late: 0 })
          }
        }
        // (2) เครื่องเต็ม → ถอดงานไม่มีสี CAT|เกจเดียวกันใน week ก่อนหน้า
        //     ปลายทางของงานที่ถอด = week แรกที่เครื่องว่างพอ (+ job setup ว่างพอ ถ้าต้อง setup ใหม่)
        const runSet = new Set(m.run_rows || [m.idx])
        const iRun = basePlan.columns.indexOf('RUN_ID')
        for (const x of work) {
          if (runSet.has(x.bi) || colorIdxSet.has(x.bi)) continue
          if (cgKey(x.row) !== cg) continue
          const w = Number(x.row[cidx.week])
          if (w < editFrom || w >= curWeek) continue
          // หาง run เดียวกันที่จะเลื่อนตาม (รวมแถวนี้) + สัปดาห์หัว run ที่เหลือ (เช็ค gap > 3)
          let tailBis = [x.bi], prevW = null
          if (iRun >= 0) {
            const rid2 = basePlan.rows[x.bi][iRun]
            tailBis = []
            for (const y of work) {
              if (basePlan.rows[y.bi][iRun] !== rid2) continue
              const yw = Number(y.row[cidx.week])
              if (yw >= w) tailBis.push(y.bi)
              else prevW = Math.max(prevW ?? -Infinity, yw)
            }
            if (prevW === -Infinity) prevW = null
          }
          const res = placeDisplaced(work, x.row, w, prevW, new Set(tailBis))
          const nf = res.week
          const due = cidx.due >= 0 ? Number(x.row[cidx.due]) : NaN
          const lt = Number.isFinite(due) ? Math.max(0, nf - due) : 0
          cands.push({ idx: x.bi, item: String(x.row[cidx.item] ?? ''), week: w, moveTo: nf, target: w, late: lt, due, tailLen: tailBis.length, full: !res.ok })
        }
        // แนะนำ: ดึงเข้าเร็วสุด (target น้อยสุด) → เครื่องว่างก่อนถอดงาน → ตัวที่หา week ลงได้ → สายน้อยสุด
        cands.sort((a, b) => a.target - b.target || (a.free ? 0 : 1) - (b.free ? 0 : 1) || (a.full ? 1 : 0) - (b.full ? 1 : 0) || a.late - b.late)
        if (cands.length) cands[0].best = true
      }
      // fits = ไม่ต้องทำอะไร (เร็วสุดแล้ว หรือไม่มีที่ให้เร็วขึ้น)
      const fits = !canEarlier || !cands.length
      const gain = cands.length ? curWeek - cands[0].target : 0   // ดึงเข้ามาเร็วขึ้นได้กี่สัปดาห์ (best)
      const lateWeeks = (deadline != null && curWeek > deadline) ? curWeek - deadline : 0
      const runLen = (m.run_rows || [m.idx]).length               // แผนต่อเนื่องกี่สัปดาห์ (ขยับทั้งชุด)
      out.push({ meta: m, curWeek, remain: remain ?? 0, fits, canEarlier, locked, lateWeeks, gain, runLen, cands })
    }
    return out
  }, [basePlan, work, removed, pAva, pLoad, avaWeeks, cidx, colorIdxSet])

  function planMoveWeek(idx, week) { setOverrides(o => ({ ...o, [idx]: week })) }
  // เลือกวิธีจัดงานสี (ทั้ง run): 'free:<week>' = ย้ายเข้าเครื่องว่าง | '<idx>' = ถอดงานไม่มีสีนั้น | '' = ล้าง
  function chooseCand(colorIdx, val) {
    setChoices(c => {
      const n = { ...c }
      if (!val) delete n[colorIdx]
      else if (String(val).startsWith('free:'))
        n[colorIdx] = { val, type: 'free', week: Number(String(val).slice(5)) }
      else
        n[colorIdx] = { val, type: 'displace', removeIdx: Number(val) }
      return n
    })
  }
  // apply candidate 'best' (ใช้ตอน user กดยืนยันพรีวิว AI)
  function applyCand(colorIdx, c) {
    if (!c) return
    chooseCand(colorIdx, c.free ? 'free:' + c.week : String(c.idx))
  }
  function planRemove(idx) {
    setRemoved(s => { const n = new Set(s); n.add(idx); return n })
    setChoices(c => {
      const n = { ...c }
      delete n[idx]
      for (const k of Object.keys(n)) if (n[k]?.removeIdx === idx) delete n[k]
      return n
    })
  }
  async function downloadPlan() {
    if (!basePlan) return
    try { await api.orderColorPlanExport(basePlan.columns, work.map(x => x.row)) }
    catch (e) { setMsg('ดาวน์โหลดไม่ได้: ' + e.message) }
  }

  // key ของกลุ่มเครื่อง = CAT|เกจ (pool เครื่องจริง)
  const metaKey = (cat, gauge) => String(cat ?? '').trim() + '|' + gnorm(gauge)

  // รายชื่อกลุ่ม CAT×เกจ (มีอยู่ในแผน) + จำนวนงานสีที่ต้องแก้ต่อกลุ่ม — เรียงกลุ่มที่มีปัญหาขึ้นก่อน
  const catList = useMemo(() => {
    if (!basePlan || cidx.cat < 0) return []
    const need = {}
    for (const s of swapList) if (!s.fits) {
      const k = metaKey(s.meta.cat, s.meta.gauge)
      need[k] = (need[k] || 0) + 1
    }
    const map = new Map()
    for (const r of basePlan.rows) {
      const cat = String(r[cidx.cat] ?? '').trim()
      if (!cat) continue
      const g = gnorm(r[cidx.gauge])
      const key = cat + '|' + g
      if (!map.has(key)) map.set(key, { key, cat, gauge: g, need: need[key] || 0 })
    }
    return [...map.values()].sort((a, b) => b.need - a.need
      || a.cat.localeCompare(b.cat, 'th', { numeric: true })
      || ((parseInt(a.gauge) || 999) - (parseInt(b.gauge) || 999)))
  }, [basePlan, swapList, cidx])

  // เลือกกลุ่มแรก (ที่มีปัญหา) ให้อัตโนมัติเมื่อเปิดแผน
  useEffect(() => {
    if (basePlan && catList.length && !catFilter) {
      setCatFilter((catList.find(c => c.need > 0) || catList[0]).key)
    }
  }, [basePlan, catList])

  // ป้ายกลุ่มที่เลือก (แปลง key "cat|เกจ" → "cat · เกจ N")
  const catFilterLabel = catFilter ? catFilter.split('|')[0] + ' · เกจ ' + (catFilter.split('|')[1] || '-') : ''

  const viewRows = useMemo(
    () => catFilter ? planRows.filter(x => cgView(x.row) === catFilter) : planRows,
    [planRows, catFilter, cidx])
  const swapView = useMemo(
    () => (catFilter ? swapList.filter(s => metaKey(s.meta.cat, s.meta.gauge) === catFilter) : swapList)
      .filter(s => !s.locked),   // ซ่อนงานที่อยู่ช่วง freeze — แก้ไม่ได้ ไม่ต้องแสดง
    [swapList, catFilter])
  const nSwapNeed = swapView.filter(s => !s.fits).length
  const nSwapDone = swapView.filter(s => !s.fits && choices[s.meta.idx]).length

  // ---- โหลด job สำหรับ Gantt: โควตาเป็นของรวมทุกกลุ่มต่อสัปดาห์ ----
  // PlanGantt คิด live เฉพาะแถวที่มองเห็น (กลุ่มที่กรอง) → ใส่ job ของ "กลุ่มอื่น" เป็น bookingNew (baseline)
  // ผลรวมบนแถบ = กลุ่มอื่น (คงที่) + กลุ่มที่ดูอยู่ (ขยับตามการลาก) = ตรงกับหน้าแผนผลิต
  const liveLoad = useMemo(() => {
    if (!basePlan) return pLoad
    const iNm = basePlan.columns.indexOf('NEW_MC')
    const iFac = basePlan.columns.indexOf('FACTORY_TYPE')
    if (iNm < 0) return pLoad
    const other = {}
    for (const x of work) {
      if (catFilter && cgView(x.row) === catFilter) continue  // แถวกลุ่มที่ดูอยู่ — PlanGantt คิด live เอง
      const t = jobTypeOf(iFac >= 0 ? x.row[iFac] : '', x.row[cidx.cat])
      if (!t) continue
      const w = String(x.row[cidx.week])
      other[w + '|' + t] = (other[w + '|' + t] || 0) + (Number(x.row[iNm]) || 0)
    }
    const out = {}
    for (const [w, types] of Object.entries(pLoad || {})) {
      out[w] = {}
      for (const [t, info] of Object.entries(types)) {
        out[w][t] = { ...info, bookingNew: other[w + '|' + t] || 0 }
      }
    }
    return out
  }, [basePlan, work, pLoad, catFilter, cidx])

  // ---- AI แนะนำการปรับ (เฉพาะกลุ่ม CAT×เกจ ที่เลือก) ----
  async function runAiAdvise() {
    if (!swapView.length) { setAiAdvice({ summary: 'กลุ่มนี้ไม่มีงานสีให้แนะนำ', ranking: [], ai: false }); return }
    setAiLoading(true); setAiAdvice(null); setMsg('')
    try {
      const [cat, gauge] = (catFilter || '|').split('|')
      const items = swapView.map(s => {
        const best = s.cands.find(c => c.best) || s.cands[0] || null
        return {
          color_item: s.meta.code, deadline: s.meta.deadline, dye_week: s.meta.dye_week,
          fg_weeks: s.meta.fg_weeks, cur_week: s.curWeek, fits: s.fits,
          run_weeks: s.runLen || 1,   // แผนต่อเนื่องกี่สัปดาห์ (ย้ายทั้งชุด — คงการ setup/carry)
          gain_weeks: s.gain || 0, late_weeks: s.lateWeeks || 0,
          best_move: best ? { type: best.free ? 'เครื่องว่าง' : 'ถอดงานไม่มีสี', item: best.item, from_week: best.week, to_week: best.moveTo, place_at: best.target, displaced_late: best.late, displaced_tail_weeks: best.tailLen || 0 } : null,
          candidates: s.cands.slice(0, 6).map(c => ({ type: c.free ? 'free' : 'displace', item: c.item, place_at: c.target, displaced_to: c.moveTo, displaced_late: c.late, displaced_tail_weeks: c.tailLen || 0 })),
        }
      })
      // setup_load = โควตา job setup ต่อสัปดาห์ (≥ สัปดาห์ที่ปรับได้)
      // used คิด live จาก NEW_MC ของแถวที่วางอยู่จริง (ขยับตามการปรับ)
      const iNm = basePlan.columns.indexOf('NEW_MC')
      const iFac = basePlan.columns.indexOf('FACTORY_TYPE')
      const usedLive = {}
      if (iNm >= 0) {
        for (const x of work) {
          const t = jobTypeOf(iFac >= 0 ? x.row[iFac] : '', x.row[cidx.cat])
          if (!t) continue
          const w = String(x.row[cidx.week])
          usedLive[w + '|' + t] = (usedLive[w + '|' + t] || 0) + (Number(x.row[iNm]) || 0)
        }
      }
      const setup_load = {}
      const from = basePlan?.edit_from ?? 0
      for (const [w, types] of Object.entries(pLoad || {})) {
        if (Number(w) < from) continue
        const o = {}
        for (const [t, info] of Object.entries(types)) {
          // used = booking (live) + งานใหม่จากแผน (baseline old)
          o[t] = { used: (usedLive[w + '|' + t] || 0) + (info.old || 0), cap: info.cap }
        }
        setup_load[w] = o
      }
      setAiAdvice(await api.orderColorAdviseMoves({ cat, gauge, items, setup_load }))
    } catch (e) {
      setMsg('AI แนะนำไม่ได้: ' + e.message); setAiAdvice(null)
    } finally { setAiLoading(false) }
  }

  // สร้าง "พรีวิว" การปรับตามคำแนะนำ (ไม่ apply ทันที — รอ user ยืนยัน)
  function buildAiPreview() {
    const moves = []
    for (const s of swapView) {
      if (s.fits) continue
      const best = s.cands.find(c => c.best) || s.cands[0]
      if (!best) continue
      moves.push({
        colorIdx: s.meta.idx, code: s.meta.code,
        from: s.curWeek, to: best.free ? best.week : best.target,
        runLen: s.runLen || 1,
        removeItem: best.free ? null : best.item,
        removeFrom: best.free ? null : best.week,
        removeTo: best.free ? null : best.moveTo,
        removeLate: best.free ? 0 : best.late,
        removeTail: best.free ? 0 : (best.tailLen || 1),
        cand: best,
      })
    }
    setAiPreview(moves)
    if (!moves.length) setMsg('ไม่มีรายการให้ปรับ')
  }

  // user กดยืนยัน → ค่อย apply ลง Gantt (what-if — ยังไม่แตะไฟล์จริง)
  function confirmAiPreview() {
    if (!aiPreview?.length) return
    for (const m of aiPreview) applyCand(m.colorIdx, m.cand)
    setMsg(`ปรับแล้ว ${aiPreview.length} รายการ — ดูแถบ "การเปลี่ยนแปลง" และ Gantt (ปรับเพิ่ม/ย้อนได้)`)
    setAiPreview(null)
  }

  // ---- เทียบการเปลี่ยนแปลง: what-if ปัจจุบัน vs booking เดิม ----
  const changesList = useMemo(() => {
    if (!basePlan || cidx.week < 0) return []
    const wi = cidx.week
    const out = []
    const byIdx = new Map(work.map(x => [x.bi, x]))
    for (let bi = 0; bi < basePlan.rows.length; bi++) {
      const baseWeek = Number(basePlan.rows[bi][wi])
      const item = String(basePlan.rows[bi][cidx.item] ?? '')
      const so = cidx.so >= 0 ? String(basePlan.rows[bi][cidx.so] ?? '').trim() : ''
      const team = cidx.team >= 0 ? String(basePlan.rows[bi][cidx.team] ?? '').trim() : ''
      const isC = colorIdxSet.has(bi)
      if (removed.has(bi)) {
        out.push({ bi, item, so, team, isColor: isC, type: 'removed', from: baseWeek })
        continue
      }
      const x = byIdx.get(bi)
      if (!x) continue
      const curW = Number(x.row[wi])
      if (curW !== baseWeek) out.push({ bi, item, so, team, isColor: isC, type: 'moved', from: baseWeek, to: curW })
      // แก้จำนวน (กก.) — แสดงแยกรายการ (แถวเดียวอาจทั้งย้ายและแก้จำนวน)
      const qe = cidx.qty >= 0 ? qtyEdits[bi] : undefined
      const baseQty = cidx.qty >= 0 ? Number(basePlan.rows[bi][cidx.qty]) : NaN
      if (qe != null && qe !== baseQty)
        out.push({ bi, item, so, team, isColor: isC, type: 'qty', from: baseQty, to: qe, week: curW })
    }
    // งานสีขึ้นก่อน แล้วเรียงตาม week ใหม่ (รายการแก้จำนวนใช้ week ของแถว)
    const kw = (c) => c.type === 'qty' ? c.week : (c.to ?? c.from)
    out.sort((a, b) => (b.isColor - a.isColor) || (kw(a) - kw(b)))
    return out
  }, [basePlan, work, removed, qtyEdits, cidx, colorIdxSet])

  function resetChanges() {
    setChoices({}); setOverrides({}); setRemoved(new Set()); setQtyEdits({}); setAiPreview(null)
    setMsg('ล้างการปรับทั้งหมด — กลับเป็น booking เดิม')
  }

  // ---- ตารางเทียบ "แผนเดิม vs แผนใหม่" (pivot: item×เครื่อง ต่อสัปดาห์ = กก. เหมือน Excel) ----
  // เฉพาะ item ที่มีการเปลี่ยน — โชว์ทุกสัปดาห์ของ item นั้น ให้เห็นทั้ง run ก่อน/หลัง
  const changeTable = useMemo(() => {
    if (!basePlan || cidx.week < 0 || !changesList.length) return null
    const keyOf = (row) => String(row[cidx.item] ?? '') + '|' + (cidx.mcg >= 0 ? String(row[cidx.mcg] ?? '') : '')
    const affected = new Set(changesList.map(c => keyOf(basePlan.rows[c.bi])))
    const items = new Map()
    for (let bi = 0; bi < basePlan.rows.length; bi++) {
      const brow = basePlan.rows[bi]
      const k = keyOf(brow)
      if (!affected.has(k)) continue
      const e = items.get(k) || {
        item: String(brow[cidx.item] ?? ''),
        mcg: cidx.mcg >= 0 ? String(brow[cidx.mcg] ?? '') : '',
        before: {}, after: {}, po: {}, hasPo: false, poPushed: false,
        isColor: false, so: new Set(), team: new Set(),
      }
      // TEAM/SO ต่อแถวเป็น comma-join จาก booking — แตกเป็นรายตัวแล้วรวม unique ของทั้ง item
      if (cidx.so >= 0) String(brow[cidx.so] ?? '').split(',').forEach(s => { const t = s.trim(); if (t) e.so.add(t) })
      if (cidx.team >= 0) String(brow[cidx.team] ?? '').split(',').forEach(s => { const t = s.trim(); if (t) e.team.add(t) })
      const w = String(Number(brow[cidx.week]))
      e.before[w] = (e.before[w] || 0) + (Number(brow[cidx.qty]) || 0)
      if (colorIdxSet.has(bi)) e.isColor = true
      items.set(k, e)
    }
    for (const x of work) {
      const e = items.get(keyOf(x.row))
      if (!e) continue
      const w = String(Number(x.row[cidx.week]))
      e.after[w] = (e.after[w] || 0) + (Number(x.row[cidx.qty]) || 0)
      // แถว PO_IN (เหลือง) = แผนใหม่เช็คด้ายเข้า — ทอได้เร็วสุด "สัปดาห์ถัดจากด้ายเข้าครบ" (PO_WEEK + 1)
      // ช่องที่แผนใหม่ทอก่อนนั้นถูกดันไป PO_WEEK + 1
      if (cidx.po >= 0) {
        const pv = basePlan.rows[x.bi][cidx.po]
        const pw = (pv === '' || pv == null) ? NaN : Number(pv)
        const cw = Number(x.row[cidx.week])
        if (Number.isFinite(pw)) e.hasPo = true
        const yw = (Number.isFinite(pw) && pw + 1 > cw) ? pw + 1 : cw
        if (yw !== cw) e.poPushed = true
        e.po[String(yw)] = (e.po[String(yw)] || 0) + (Number(x.row[cidx.qty]) || 0)
      }
    }
    const weekSet = new Set()
    for (const e of items.values()) {
      Object.keys(e.before).forEach(w => weekSet.add(Number(w)))
      Object.keys(e.after).forEach(w => weekSet.add(Number(w)))
      if (e.hasPo) Object.keys(e.po).forEach(w => weekSet.add(Number(w)))
    }
    const weeks = [...weekSet].filter(Number.isFinite).sort((a, b) => a - b)
    const list = [...items.values()].sort((a, b) => (b.isColor - a.isColor) || a.item.localeCompare(b.item))
    for (const e of list) {
      e.totalBefore = Object.values(e.before).reduce((s, v) => s + v, 0)
      e.totalAfter = Object.values(e.after).reduce((s, v) => s + v, 0)
      e.totalPo = Object.values(e.po).reduce((s, v) => s + v, 0)
      // จำนวนให้สี (TOTAL_QTY) + Stock (STOCK_BALANCE_KG) จากไฟล์ Order Color — มีเฉพาะงานสี
      const info = basePlan.code_info?.[e.item.trim().toUpperCase()]
      e.orderQty = info?.qty
      e.stockBal = info?.stock
    }
    return { weeks, list }
  }, [basePlan, work, changesList, cidx, colorIdxSet])

  const isRunning = !!runStatus.running
  const runLabel = isRunning ? `กำลังดึงข้อมูล: ${runStatus.label || '-'}` : 'ยังไม่มีงานกำลังรัน'

  return (
    <div className="masters oc-full">
      <section className="editor">
        {basePlan && (
          <div className="advice-panel">
            <div className="editbar">
              <div>
                <h2>แผนถักงานสี (Gantt) — ทุก item ต่อสัปดาห์ × เครื่อง</h2>
                <div className="data-selected-meta">
                  {basePlan.plan_name ? `จาก booking ${basePlan.plan_name}` : ''}
                  {' '}
                  <button type="button" className="help-toggle" onClick={() => setShowHelp(v => !v)}>
                    {showHelp ? '▾ ซ่อนวิธีใช้' : '▸ วิธีใช้'}
                  </button>
                </div>
              </div>
              <div className="actions">
                <button className="build" onClick={buildPlan} disabled={planning || isRunning}>
                  {planning ? 'กำลังสร้างแผน...' : '🗓 จัดแผนสี (Gantt)'}
                </button>
                <button onClick={runAiAdvise} disabled={aiLoading || !swapView.length}>
                  {aiLoading ? '🤖 กำลังวิเคราะห์...' : '🤖 AI แนะนำการปรับ'}
                </button>
                <button className="primary" onClick={downloadPlan}>⬇ ดาวน์โหลดแผนนี้</button>
              </div>
            </div>
            {showHelp && (
              <div className="help-box">
                <div>★ บล็อกสีทอง = งานสี (ทอก่อน) • ลากบล็อกเปลี่ยนสัปดาห์ • คลิก ✕ ถอดงานไม่มีสีเพื่อเปิดที่ให้งานสี</div>
                <div>งานสีต้องได้ทอก่อน — ตัวที่ทออยู่ week ไกลจะเสนอดึงเข้ามาเร็วขึ้น • เครื่องว่าง = ย้ายเลย • เครื่อง/job ไม่พอ = ถอดงานไม่มีสี (⭐ แนะนำ) — งานที่ถอดได้แผนใหม่อัตโนมัติ</div>
              </div>
            )}
            {basePlan.note && <div className="msg note">ℹ️ {basePlan.note}</div>}
            {work.warns?.length > 0 && (
              <div className="msg note">
                ⚠ งานที่ถูกถอดไม่มีสัปดาห์ที่เครื่อง/job setup ว่างพอ — วางไว้ท้ายแผนไปก่อน:{' '}
                {[...new Set(work.warns)].join(' • ')}
              </div>
            )}

            <div className="cat-chips">
              <span className="hint small" style={{ padding: 0 }}>👁 ดูทีละ CAT × เกจ:</span>
              <select className="cat-select" value={catFilter}
                onChange={e => { setCatFilter(e.target.value); setAiAdvice(null); setAiPreview(null) }}>
                {catList.map(c => (
                  <option key={c.key} value={c.key}>
                    {c.cat} · เกจ {c.gauge}{c.need > 0 ? ` — ต้องถอด ${c.need}` : ''}
                  </option>
                ))}
              </select>
            </div>

            <div className="swap-panel">
              <div className="swap-head">
                <b>งานสีใน {catFilterLabel || '(ทั้งหมด)'} — {swapView.length} ตัว (ดึงเข้ามาเร็วขึ้นได้ {nSwapNeed}, จัดแล้ว {nSwapDone})</b>
              </div>
              {!swapView.length && <div className="hint small" style={{ padding: '4px 0' }}>กลุ่มนี้ไม่มีงานสี</div>}
              {swapView.map(s => {
                const sel = choices[s.meta.idx]?.val || ''
                const cval = c => c.free ? 'free:' + c.week : String(c.idx)
                const chosen = sel ? s.cands.find(c => cval(c) === sel) : null
                const booking = s.meta.fg_weeks?.length ? s.meta.fg_weeks.map(w => 'W' + w).join(', ') : '—'
                const runTxt = s.runLen > 1 ? ` • run ต่อเนื่อง ${s.runLen} สัปดาห์ (ขยับทั้งชุด)` : ''
                return (
                  <div key={s.meta.idx} className={'swap-row' + (s.fits ? ' fit' : '')}>
                    <div className="swap-item">
                      <b>{s.locked ? '🔒 ' : (s.fits ? '' : '⚠ ')}{s.meta.code}</b>
                      <span className="swap-sub">
                        booking {booking} • ย้อม W{s.meta.dye_week} • ส่ง(FG) W{s.meta.deadline} • เริ่มทอ W{s.curWeek}{runTxt} • เกจ {s.meta.gauge}
                        {!s.fits && <b className="swap-short"> • ดึงเข้ามาได้เร็วขึ้น {s.gain} สัปดาห์</b>}
                        {s.lateWeeks > 0 && <b className="swap-short"> • เลยกำหนดส่ง {s.lateWeeks} สัปดาห์</b>}
                      </span>
                    </div>
                    {s.locked ? (
                      <span className="swap-impact">🔒 อยู่ช่วง freeze (แก้ไม่ได้)</span>
                    ) : s.fits ? (
                      <span className="swap-impact ok">✓ {s.canEarlier ? 'ไม่มีที่ว่าง/ตัวถอดก่อน W' + s.curWeek + ' — คงเดิม' : 'ทอเร็วสุดแล้ว (W' + s.curWeek + ')'}</span>
                    ) : (
                      <>
                        <select className="swap-select" value={sel} onChange={e => chooseCand(s.meta.idx, e.target.value)}>
                          <option value="">— เลือกวิธีดึงเข้ามา —</option>
                          {s.cands.map((c, ci) => (
                            <option key={ci} value={cval(c)}>
                              {c.best ? '⭐ ' : ''}{c.free
                                ? `เริ่มทอ W${c.week} (เครื่องว่าง เร็วขึ้น ${s.curWeek - c.week} สัปดาห์${s.runLen > 1 ? ' ทั้ง run' : ''})`
                                : `W${c.week}: ถอด ${c.item} ออก → เลื่อนไป W${c.moveTo}${c.tailLen > 1 ? ` (หาง run ${c.tailLen} สัปดาห์เลื่อนต่อกัน)` : ''} ${c.late > 0 ? `สาย ${c.late} สัปดาห์` : 'ทันเวลา'}${c.full ? ' ⚠ ไม่มีสัปดาห์ที่เครื่อง/job ว่างพอ' : ''}`}
                            </option>
                          ))}
                        </select>
                        {chosen && (
                          <span className={'swap-impact' + (chosen.free ? ' ok' : (chosen.late > 0 ? ' late' : ' ok'))}>
                            {chosen.free
                              ? `→ เริ่มทอ W${chosen.week}${s.runLen > 1 ? ` (run ${s.runLen} สัปดาห์ขยับตาม)` : ' (เครื่องว่าง)'}`
                              : `→ เริ่มทอ W${chosen.week} • ${chosen.item} เลื่อนไป W${chosen.moveTo}${chosen.tailLen > 1 ? ` (หาง ${chosen.tailLen} สัปดาห์เลื่อนต่อกัน)` : ''} ${chosen.late > 0 ? `(สาย ${chosen.late})` : '(ทันเวลา)'}${chosen.full ? ' ⚠ ไม่มีสัปดาห์ที่เครื่อง/job ว่างพอ' : ''}`}
                          </span>
                        )}
                      </>
                    )}
                  </div>
                )
              })}
            </div>

            {(aiLoading || aiAdvice) && (
              <div className="ai-panel">
                <div className="ai-head">
                  <b>🤖 AI แนะนำลำดับการดึงงานสีเข้ามา + ผลกระทบ</b>
                  {aiAdvice?.ranking?.length > 0 && !aiPreview && (
                    <button onClick={buildAiPreview}>👁 ดูรายการที่จะปรับ ({nSwapNeed})</button>
                  )}
                </div>
                {aiLoading && <div className="hint small" style={{ padding: '4px 0' }}>กำลังให้ AI วิเคราะห์...</div>}
                {aiAdvice && !aiLoading && (
                  <>
                    {aiAdvice.summary && <div className="msg note">{aiAdvice.ai ? '' : '⚠️ '}{aiAdvice.summary}</div>}
                    {aiAdvice.note && <div className="hint small" style={{ padding: 0 }}>{aiAdvice.note}</div>}
                    {aiAdvice.ranking?.length > 0 && (
                      <ol className="ai-rank">
                        {aiAdvice.ranking.map((r, i) => (
                          <li key={i}>
                            <b>{r.color_item}</b>
                            {r.reason && <span className="ai-reason"> — {r.reason}</span>}
                            {r.impact && <div className="ai-impact">↪ ผลกระทบ: {r.impact}</div>}
                          </li>
                        ))}
                      </ol>
                    )}
                  </>
                )}

                {aiPreview && (
                  <div className="ai-preview">
                    <div className="ai-head">
                      <b>⚠ รายการที่จะปรับ ({aiPreview.length}) — ยังไม่ปรับจนกว่าจะกดยืนยัน</b>
                      <span>
                        <button className="primary" onClick={confirmAiPreview}>✓ ยืนยันปรับตามนี้</button>{' '}
                        <button onClick={() => setAiPreview(null)}>ยกเลิก</button>
                      </span>
                    </div>
                    {!aiPreview.length && <div className="hint small">ไม่มีรายการให้ปรับ</div>}
                    <ul className="ai-moves">
                      {aiPreview.map((m, i) => (
                        <li key={i}>
                          ★ <b>{m.code}</b>: เริ่มทอ W{m.from} → <b>W{m.to}</b>
                          {m.runLen > 1 && <span className="ai-reason"> (run ต่อเนื่อง {m.runLen} สัปดาห์ ขยับทั้งชุด)</span>}
                          {m.removeItem
                            ? <span className="ai-impact"> • ถอด {m.removeItem} ออกจาก W{m.removeFrom} → เลื่อนไป W{m.removeTo}{m.removeTail > 1 ? ` (หาง run ${m.removeTail} สัปดาห์เลื่อนต่อกัน)` : ''} {m.removeLate > 0 ? `(สาย ${m.removeLate} สัปดาห์)` : '(ทันเวลา)'}</span>
                            : <span className="ai-reason"> • เครื่องว่าง ไม่ต้องถอดใคร</span>}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            {changesList.length > 0 && (
              <div className="changes-panel">
                <div className="ai-head">
                  <b>📝 การเปลี่ยนแปลงจาก booking เดิม ({changesList.length} รายการ)</b>
                  <button onClick={resetChanges}>↩ ล้างการปรับทั้งหมด</button>
                </div>
                <ul className="ai-moves">
                  {changesList.map((c, i) => (
                    <li key={i} className={c.isColor ? 'chg-color' : ''}>
                      {c.isColor ? '★ ' : ''}<b>{c.item}</b>
                      {(c.team || c.so) && (
                        <span className="ai-reason"> ({[c.team && `TEAM ${c.team}`, c.so && `SO ${c.so}`].filter(Boolean).join(' • ')})</span>
                      )}
                      {c.type === 'removed'
                        ? <span className="ai-impact"> — เอาออกจากแผน (เดิม W{c.from})</span>
                        : c.type === 'qty'
                          ? <span> : W{c.week} จำนวน {fmtNum(c.from)} → <b>{fmtNum(c.to)}</b> กก.</span>
                          : <span> : W{c.from} → <b>W{c.to}</b>{c.to < c.from ? <span className="ai-reason"> (เร็วขึ้น {c.from - c.to} สัปดาห์)</span> : <span className="ai-impact"> (เลื่อนออก {c.to - c.from} สัปดาห์)</span>}</span>}
                    </li>
                  ))}
                </ul>

                {changeTable && (
                  <>
                    <div className="hint small" style={{ padding: '8px 0 2px' }}>
                      ตารางเทียบ <b>แผนเดิม → แผนใหม่</b> (กก. ต่อสัปดาห์ เฉพาะ item ที่มีการปรับ) •
                      <span className="cell-out-demo"> เดิม</span> = ตำแหน่งที่ย้ายออก • <span className="cell-in-demo">ใหม่</span> = ตำแหน่งใหม่ (ตาม AVA) •
                      <span className="cell-po-demo"> PO_IN</span> = ตำแหน่งเมื่อรอด้ายเข้าครบ (ช่องเหลือง = ด้ายมาไม่ทันแผนใหม่ ทอได้เร็วสุดสัปดาห์ถัดจากด้ายเข้าครบ)
                    </div>
                    <div className="gridwrap">
                      <table className="grid chg-table">
                        <thead>
                          <tr>
                            <th>ITEM</th><th>เครื่อง</th>
                            <th className="wk-col">จำนวนให้สี</th>
                            <th className="wk-col">Stock (กก.)</th>
                            <th>แผน</th>
                            {changeTable.weeks.map(w => <th key={w} className="wk-col">W{w}</th>)}
                            <th className="wk-col">รวม</th>
                          </tr>
                        </thead>
                        <tbody>
                          {changeTable.list.map((e, i) => {
                            const showPo = e.hasPo
                            return (
                              <React.Fragment key={i}>
                                <tr className="chg-before">
                                  <td className="rocell" rowSpan={showPo ? 3 : 2}>
                                    {e.isColor ? '★ ' : ''}<b>{e.item}</b>
                                    {e.team.size > 0 && <div className="ai-reason" style={{ fontSize: '0.85em' }}>TEAM {[...e.team].join(', ')}</div>}
                                    {e.so.size > 0 && <div className="ai-reason" style={{ fontSize: '0.85em' }}>SO {[...e.so].join(', ')}</div>}
                                  </td>
                                  <td className="rocell" rowSpan={showPo ? 3 : 2}>{e.mcg}</td>
                                  <td className="rocell wk-col" rowSpan={showPo ? 3 : 2}>
                                    {e.orderQty != null ? <b>{fmtNum(e.orderQty)}</b> : '—'}
                                  </td>
                                  <td className="rocell wk-col" rowSpan={showPo ? 3 : 2}>
                                    {e.stockBal != null ? fmtNum(e.stockBal) : '—'}
                                  </td>
                                  <td className="rocell">เดิม</td>
                                  {changeTable.weeks.map(w => {
                                    const b = e.before[String(w)] || 0, a = e.after[String(w)] || 0
                                    return <td key={w} className={'wk-col' + (b && b !== a ? ' cell-out' : '')}>{b ? fmtNum(Math.round(b * 10) / 10) : ''}</td>
                                  })}
                                  <td className="wk-col">{fmtNum(Math.round(e.totalBefore * 10) / 10)}</td>
                                </tr>
                                <tr className={'chg-after' + (showPo ? ' mid' : '')}>
                                  <td className="rocell"><b>ใหม่</b></td>
                                  {changeTable.weeks.map(w => {
                                    const b = e.before[String(w)] || 0, a = e.after[String(w)] || 0
                                    return <td key={w} className={'wk-col' + (a && a !== b ? ' cell-in' : '')}>{a ? fmtNum(Math.round(a * 10) / 10) : ''}</td>
                                  })}
                                  <td className="wk-col"><b>{fmtNum(Math.round(e.totalAfter * 10) / 10)}</b></td>
                                </tr>
                                {showPo && (
                                  <tr className="chg-po">
                                    <td className="rocell"><b>PO_IN</b>{!e.poPushed && ' ✓'}</td>
                                    {changeTable.weeks.map(w => {
                                      const a = e.after[String(w)] || 0, p = e.po[String(w)] || 0
                                      return <td key={w} className={'wk-col' + (p && p !== a ? ' cell-po' : '')}>{p ? fmtNum(Math.round(p * 10) / 10) : ''}</td>
                                    })}
                                    <td className="wk-col"><b>{fmtNum(Math.round(e.totalPo * 10) / 10)}</b></td>
                                  </tr>
                                )}
                              </React.Fragment>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </div>
            )}

            {catFilter && (
              <div className="catitems">
                <div className="swap-head"><b>งานทั้งหมดใน {catFilterLabel} ({viewRows.length} รายการ)</b>
                  <span className="hint small" style={{ padding: 0 }}>★ = งานสี • ไม่มี ★ = งานไม่มีสี (ถอดได้) • เรียงตามสัปดาห์</span>
                </div>
                <div className="gridwrap" style={{ maxHeight: 260 }}>
                  <table className="grid">
                    <thead>
                      <tr><th className="rownum">#</th><th>ประเภท</th><th>ITEM</th><th>เครื่อง</th><th>เกจ</th><th>สัปดาห์</th><th>จำนวน</th></tr>
                    </thead>
                    <tbody>
                      {[...viewRows]
                        .sort((a, b) => (Number(a.row[cidx.week]) || 0) - (Number(b.row[cidx.week]) || 0))
                        .map((x, i) => {
                          const isC = colorRows.has(x.idx)
                          return (
                            <tr key={x.idx} className={isC ? 'catitem-color' : ''}>
                              <td className="rownum">{i + 1}</td>
                              <td className="rocell">{isC ? '★ สี' : 'ไม่มีสี'}</td>
                              <td className="rocell">{norm(x.row[cidx.item])}</td>
                              <td className="rocell">{cidx.mcg >= 0 ? norm(x.row[cidx.mcg]) : ''}</td>
                              <td className="rocell">{norm(x.row[cidx.gauge])}</td>
                              <td className="rocell">W{norm(x.row[cidx.week])}</td>
                              <td className="rocell">{cidx.qty >= 0 ? norm(x.row[cidx.qty]) : ''}</td>
                            </tr>
                          )
                        })}
                      {!viewRows.length && (
                        <tr><td className="rownum"></td><td colSpan={6} className="hint">ไม่มีงานใน CAT นี้</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            <PlanGantt columns={basePlan.columns} rows={viewRows} load={liveLoad} ava={pAva} poolMap={poolMap}
              colorRows={colorRows} onMoveWeek={planMoveWeek} onRemove={planRemove}
              onEditQty={(idx, q) => setQtyEdits(s => ({ ...s, [idx]: q }))}
              lockBefore={basePlan.edit_from} />
          </div>
        )}

        {!basePlan && advice && (
          <div className="advice-panel">
            <div className="editbar">
              <div>
                <h2>ผลวิเคราะห์แผนสี (ต้องถักเสร็จก่อนย้อม {advice.lead_weeks} สัปดาห์)</h2>
                <div className="data-selected-meta">
                  {advice.order_color_name} {advice.plan_name ? `• เทียบแผน ${advice.plan_name}` : ''}
                </div>
              </div>
              <div className="actions">
                <button onClick={() => setAdvice(null)}>✕ ปิด กลับไปตาราง</button>
              </div>
            </div>

            {advice.summary && advice.summary.total_color !== undefined && (
              <div className="advice-stats">
                <span className="chip">item มีสี {advice.summary.total_color}</span>
                <span className="chip warn">ต้องแก้ {advice.summary.need_action}</span>
                <span className="chip">ช้าเกิน {advice.summary.late}</span>
                <span className="chip">ยังไม่มีในแผน {advice.summary.missing}</span>
                <span className="chip ok">ทันย้อม {advice.summary.ok}</span>
                {advice.summary.manual > 0 && <span className="chip">ต้องดูเอง {advice.summary.manual}</span>}
              </div>
            )}
            {advice.note && <div className="msg note">ℹ️ {advice.note}</div>}

            {advice.items?.length ? (
              <div className="gridwrap">
                <table className="grid">
                  <thead>
                    <tr>
                      <th className="rownum">#</th>
                      <th>ITEM</th><th>อบ</th><th>ย้อม</th><th>กำหนดถัก</th>
                      <th>แผนปัจจุบัน</th><th>CAT/เกจ</th><th>จำนวน</th>
                      <th>สถานะ</th><th>คำแนะนำ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {advice.items.map((it, i) => {
                      const s = STATUS[it.status] || { label: it.status, color: '#555', bg: '#eee' }
                      return (
                        <tr key={i}>
                          <td className="rownum">{i + 1}</td>
                          <td className="rocell"><b>{it.item}</b></td>
                          <td className="rocell">{it.tubular}</td>
                          <td className="rocell">{wk(it.dye_week)}</td>
                          <td className="rocell">≤ {wk(it.deadline)}</td>
                          <td className="rocell">{it.plan_weeks?.length ? it.plan_weeks.map(wk).join(', ') : '—'}</td>
                          <td className="rocell">{it.cat || it.gauge ? `${it.cat}/${it.gauge}` : '—'}</td>
                          <td className="rocell">{it.qty}</td>
                          <td className="rocell">
                            <span style={{ background: s.bg, color: s.color, padding: '2px 8px', borderRadius: 10, fontWeight: 600, whiteSpace: 'nowrap' }}>{s.label}</span>
                          </td>
                          <td className="rocell">{it.advice}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="hint">ไม่พบ item ที่มีสี (LOAD_DYE) ในไฟล์นี้</div>
            )}
          </div>
        )}

        {!advice && !basePlan && (
          <>
            <div className="editbar">
              <div>
                <h2>แผนถักงานสี (Gantt) — CAT × เกจ {catHist ? `— ตั้งแต่ W${catHist.current_week}` : ''}</h2>
                <div className="data-selected-meta">
                  งานสีต้องทอก่อนงานไม่มีสี • วางบน "สัปดาห์ถัก = ย้อม − {catHist?.lead_weeks ?? 2}" แล้วดึงเข้าสัปดาห์ว่างที่เร็วสุด
                  <br />② เครื่องว่างต่อสัปดาห์ (เขียว = ว่าง, แดง = เต็ม) • ① บล็อกงานสี (ตัวเลข = กก. ถัก) • ③ แผนปัจจุบัน — งานไม่มีสี = ตัวที่ถอดออกได้
                </div>
              </div>
              <div className="actions">
                <button className="build" onClick={buildPlan} disabled={planning || isRunning}>
                  {planning ? 'กำลังสร้างแผน...' : '🗓 จัดแผนสี (Gantt)'}
                </button>
              </div>
            </div>

            {msg && <div className="msg">{msg}</div>}
            {chLoading && <div className="hint">กำลังโหลด...</div>}
            {catHist?.note && <div className="msg note">ℹ️ {catHist.note}</div>}

            {!chLoading && !meta?.exists && (
              <div className="hint">ยังไม่มีไฟล์ Order Color — กด <b>ดึงข้อมูล</b> ด้านซ้ายเพื่อสร้าง</div>
            )}

            {catHist && !chLoading && catHist.groups?.length === 0 && (
              <div className="hint">ยังไม่มีกลุ่ม (CAT × เกจ) ที่มี item สีตั้งแต่สัปดาห์ปัจจุบัน</div>
            )}

            {catHist && !chLoading && catHist.groups?.map(g => (
              <div key={g.cat + '|' + g.gauge} className="cathist-block">
                <div className="swap-head">
                  <b>CAT {g.cat} · เกจ {g.gauge || '-'}</b>
                  <span className="hint small" style={{ padding: 0 }}>
                    งานสี {g.n_color} ตัว • ★ = งานสี (ทอก่อน) → บล็อกวางบน "สัปดาห์ถัก" • 🟩 ทันย้อม 🟧 เครื่องเต็ม/ต้องถอด 🟥 เลยกำหนด
                  </span>
                </div>
                <div className="gridwrap">
                  <table className="grid gantt">
                    <thead>
                      <tr>
                        <th className="lane-col">งาน ↓ / สัปดาห์ →</th>
                        {catHist.weeks.map(w => <th key={w} className="wk-col">W{w}</th>)}
                      </tr>
                    </thead>
                    <tbody>
                      {/* ② เครื่องว่างต่อสัปดาห์ */}
                      <tr className="ava-row">
                        <td className="rocell lane-col"><b>② เครื่องว่าง</b></td>
                        {catHist.weeks.map(w => {
                          const r = g.ava?.[String(w)]
                          const cls = r == null ? '' : (r > 0 ? 'ava-free' : 'ava-full')
                          return <td key={w} className={'wk-col ' + cls}>{r == null ? '' : r}</td>
                        })}
                      </tr>

                      {/* ① งานสี — วางบล็อกที่สัปดาห์ถักที่แนะนำ (greedy ดึงเข้าเร็วสุด) */}
                      <tr className="sec-sep"><td className="lane-col" colSpan={catHist.weeks.length + 1}>① งานสี (ทอก่อน) — วางบนสัปดาห์ถัก = ย้อม − {catHist.lead_weeks ?? 2}</td></tr>
                      {g.items.map((it, i) => (
                        <tr key={'c' + i} className="catitem-color">
                          <td className="rocell lane-col">
                            ★ <b>{it.item}</b> <span className="lane-sub">ย้อม {wk(it.dye_week)} · ถัก ≤{wk(it.deadline)}</span>
                          </td>
                          {catHist.weeks.map(w => {
                            if (String(w) === String(it.place_week)) {
                              return <td key={w} className={'wk-col gblock place-' + it.status}
                                title={`${it.item} • ถัก W${it.place_week} • ${it.knit_kg} กก.`}>
                                {fmtNum(it.knit_kg)}
                              </td>
                            }
                            const dead = it.deadline != null && String(w) === String(it.deadline)
                            return <td key={w} className={'wk-col' + (dead ? ' deadline-mark' : '')}></td>
                          })}
                        </tr>
                      ))}

                      {/* ③ แผนปัจจุบัน — งานไม่มีสี = ตัวที่ถอดออกได้เพื่อเปิดที่ให้งานสี */}
                      <tr className="sec-sep"><td className="lane-col" colSpan={catHist.weeks.length + 1}>③ แผนที่วางอยู่ตอนนี้ (เครื่อง) — ไม่มี ★ = ถอดออกได้</td></tr>
                      {g.plan?.map((p, i) => (
                        <tr key={'p' + i} className={p.is_color ? 'catitem-color' : 'plan-removable'}>
                          <td className="rocell lane-col">{p.is_color ? '★ ' : ''}{p.item}</td>
                          {catHist.weeks.map(w => {
                            const v = p.weeks[String(w)]
                            return <td key={w} className={'wk-col' + (v ? ' gblock-plan' : '')}>{v || ''}</td>
                          })}
                        </tr>
                      ))}
                      {!g.plan?.length && (
                        <tr><td className="hint lane-col" colSpan={catHist.weeks.length + 1}>ยังไม่มีงานไม่มีสีในแผนกลุ่มนี้ (ไม่มีตัวให้ถอด)</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </>
        )}
      </section>
    </div>
  )
}

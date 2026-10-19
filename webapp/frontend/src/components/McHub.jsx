import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import { makeWorkDayResolver } from '../workday.js'
import { weekKey } from '../weekKeys.js'
import McNewWizard from './McNewWizard.jsx'

// หน้า "เครื่องจักร" — ที่เดียวจบสำหรับทุกอย่างที่เกี่ยวกับเครื่อง
//
// เดิมข้อมูลของเครื่องกองเดียวกระจายอยู่ 5 ชีท คนละไฟล์ คนละคีย์ (ดู mc_hub.py)
// ผู้ใช้ต้องจำว่าอะไรอยู่ชีทไหน และแก้ที่หนึ่งแล้วอีกที่ไม่ตาม (เคยทำให้ 12 กองใช้วันทำงานผิด)
// หน้านี้จึงจัดตาม "กองเครื่อง" (โรงงาน · กลุ่ม · เกจ) = หน่วยเดียวกับคีย์ของแผงวันทำงาน
//   • วันทำงาน / ชั่วโมง / กันเครื่อง / ค่าเฉพาะสัปดาห์ → ของทั้งกอง
//   • จำนวนเครื่อง / กันให้ POLY-COTTON               → ของเครื่องแต่ละตัวในกอง
// แก้เสร็จกดบันทึกครั้งเดียว ระบบกระจายลงไฟล์เดิมให้ครบ
//
// วัน + ชั่วโมง "เฉพาะสัปดาห์" ก็อยู่ที่นี่ที่เดียว (หน้าปฏิทินเหลือแค่การยุบสัปดาห์ — user สั่ง
// 29 ก.ย. 2569: แผงกลุ่มเครื่องในหน้าปฏิทินซ้ำกับหน้านี้) — สัปดาห์ที่ยุบรวมมาบังคับต้องระบุทั้งวัน
// และชั่วโมง ไม่งั้นบันทึกไม่ผ่าน เพราะสัปดาห์ที่รวมกันแล้วมักเดินไม่เท่าสัปดาห์ปกติ

const fmt = v => (v === null || v === undefined || v === '' ? '' : String(v))
const r1 = v => Math.round(v * 10) / 10

export default function McHub({ ro }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState('')
  const [q, setQ] = useState('')
  const [open, setOpen] = useState({})        // กองที่กางดูรายละเอียดอยู่
  const [edit, setEdit] = useState({})        // ค่าที่แก้ค้าง: {'mc:12': '4', 'wd:KEY': '5.5'}
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState('')
  const [wizard, setWizard] = useState(false)
  const [fixing, setFixing] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [imp, setImp] = useState(null)       // null = ปิด | { file, check } — Import ทับชีท
  const fileRef = useRef(null)

  const [wdPayload, setWdPayload] = useState(null)   // /api/workday — ใช้คำนวณวันทำงานให้ตรงกับแผน

  const load = () => Promise.all([
    api.mcHub(),
    // วันทำงานจริงของสัปดาห์ (หักปฏิทิน + ยุบสัปดาห์แล้ว) คิดที่ตัวเดียวกับ Gantt เพื่อไม่ให้เลขคนละชุด
    api.workday().catch(() => null),
  ]).then(([d, wd]) => { setData(d); setWdPayload(wd); setErr('') })
    .catch(e => setErr('โหลดข้อมูลเครื่องไม่ได้: ' + e.message))
  useEffect(() => { load() }, [])

  const pools = data?.pools || []
  const wdRes = useMemo(() => (wdPayload ? makeWorkDayResolver(wdPayload) : null), [wdPayload])
  const mergeTargets = data?.merge_targets || []
  const merges = data?.merges || {}

  const shown = useMemo(() => {
    const s = q.trim().toLowerCase()
    if (!s) return pools
    return pools.filter(p => [p.factory, p.mc_cat, p.guage, ...p.machines.flatMap(m => [m.mc, m.group])]
      .join(' ').toLowerCase().includes(s))
  }, [pools, q])

  const set = (k, v) => setEdit(e => ({ ...e, [k]: v }))
  const val = (k, cur) => (k in edit ? edit[k] : fmt(cur))

  // ด่านกรอกให้ครบ: เว้นว่างไม่ได้ (user สั่ง 24 ก.ย. 2569)
  // เดิม "ไม่กรอก" กับ "ตั้งใจใช้ค่าพื้นฐาน" แยกกันไม่ออก ระบบเลยใช้ 6 วัน 24 ชม. เงียบ ๆ
  const bad = useMemo(() => {
    const out = []
    for (const [k, v] of Object.entries(edit)) {
      const kind = k.slice(0, k.indexOf(':'))
      const s = String(v ?? '').trim()
      const label = kind === 'wd' ? 'วันทำงาน'
        : kind === 'wh' ? 'ชั่วโมงทำงาน'
          : kind === 'ww' ? 'ชั่วโมงเฉพาะสัปดาห์' : 'จำนวนเครื่อง'
      // ชั่วโมงเฉพาะสัปดาห์เว้นว่างได้ = กลับไปใช้ชั่วโมงของกอง (ยกเว้นสัปดาห์ที่ยุบรวมมา
      // ซึ่งถูกดักด้วย gapsNow ต่างหาก เพราะต้องตรวจกองที่ยังไม่เคยแตะด้วย)
      if (s === '') { if (kind !== 'ww') out.push(`${label}: เว้นว่างไม่ได้`); continue }
      const n = Number(s)
      if (!Number.isFinite(n)) { out.push(`${label}: ต้องเป็นตัวเลข`); continue }
      if (kind === 'wd' && (n < 0 || n > 7)) out.push('วันทำงาน: ต้องอยู่ระหว่าง 0-7')
      if ((kind === 'wh' || kind === 'ww') && (n < 0 || n > 24)) out.push(`${label}: ต้องอยู่ระหว่าง 0-24`)
    }
    return [...new Set(out)]
  }, [edit])
  const isBad = k => k in edit && (() => {
    const s = String(edit[k] ?? '').trim()
    if (s === '') return !k.startsWith('ww:')
    const n = Number(s)
    if (!Number.isFinite(n)) return true
    if (k.startsWith('wd:')) return n < 0 || n > 7
    if (k.startsWith('wh:') || k.startsWith('ww:')) return n < 0 || n > 24
    return false
  })()

  // ---- ค่ารายสัปดาห์ของกอง: ชม./วัน + จำนวนเครื่อง (user สั่ง 25 ก.ย. 2569)
  //   ชม./วัน : ใช้เฉพาะสัปดาห์นั้น (ว่าง = ชั่วโมงของ Cat) — เก็บที่แผงวันทำงาน
  //   เครื่อง : ใช้เฉพาะสัปดาห์นั้นเหมือนกัน (ว่าง = Total ในทะเบียน) — ชีท MC_Total
  //            (เคยลองยกค่าล่าสุดไปเรื่อย ๆ แล้ว user ไม่เอา: สัปดาห์ที่ไม่ได้กรอกต้องกลับเป็น Total)
  //   บรรทัดหนึ่งกรอกเป็นช่วงได้ (ถึงสัปดาห์) — ไม่ต้องไล่กรอกทีละสัปดาห์
  // ตารางทั้งปีแสดงผลทุกสัปดาห์ ; รายการด้านบนมีเฉพาะสัปดาห์ที่ user เปิดแก้เอง (คลิกช่อง / + เพิ่มสัปดาห์)
  // ค่าที่บันทึกไว้แล้วไม่เด้งขึ้นมาเป็นบรรทัดเอง (user สั่ง: ต้องแก้ก่อนถึงจะขึ้น) และแก้ได้ทุกสัปดาห์
  const [drafts, setDrafts] = useState({})     // {poolKey: [{id, yw, to, h, mc, clear}]}
  const timeline = data?.timeline || []
  const todayYw = data?.today_yw
  const todayIdx = Math.max(0, timeline.indexOf(todayYw))
  const ywLabel = yw => {
    if (yw == null) return '—'
    const y = Math.floor(yw / 100), ty = todayYw ? Math.floor(todayYw / 100) : y
    return `W${yw % 100}` + (y !== ty ? `/${String(y).slice(-2)}` : '')
  }
  // เลขสัปดาห์เปล่า → YW ครั้งที่ใกล้วันนี้สุด (ห่างเท่ากันเลือกอนาคต) — กติกาเดียวกับ Calendar.resolve_bare
  const resolveBare = w => {
    let best = null, bi = -1
    timeline.forEach((yw, i) => {
      if (yw % 100 !== Number(w)) return
      const d = Math.abs(i - todayIdx), bd = bi < 0 ? Infinity : Math.abs(bi - todayIdx)
      if (d < bd || (d === bd && i >= todayIdx)) { best = yw; bi = i }
    })
    return best
  }
  // สัปดาห์ยุบจาก backend เป็น YW แล้ว (ผูกปี) ; เลขเปล่า = ข้อมูลรุ่นเก่า → ครั้งที่ใกล้วันนี้สุด
  const toYw = w => { const n = Number(w); return n >= 100000 ? n : resolveBare(n) }
  const mergeYw = new Set(mergeTargets.map(toYw).filter(Boolean))
  // ด่านบังคับกรอกดูเฉพาะสัปดาห์ยุบตั้งแต่สัปดาห์นี้ — ของปีที่ผ่านไปอยู่ในไฟล์ตลอด ถ้าตรวจด้วยจะบันทึกไม่ได้อีกเลย
  const liveMergeTargets = mergeTargets.filter(w => todayYw == null || toYw(w) >= todayYw)
  const poolBase = p => p.machines.reduce((a, m) => a + (Number(val(`mc:${m.row}`, m.total)) || 0), 0)
  const catHours = p => val(`wh:${p.key}`, p.work_hours)
  const origWeeks = p => Object.fromEntries((p.mc_weeks || []).map(x => [x.yw, x.value]))
  // สัปดาห์ที่บรรทัดนี้ครอบ (ตามลำดับปฏิทิน) — ไม่มี "ถึง" = สัปดาห์เดียว ; "ถึง" มาก่อน "ตั้งแต่" = ไม่มี (ด่านจับ)
  const spanOf = r => {
    if (r.yw == null) return []
    if (r.to == null || r.to === r.yw) return [r.yw]
    const i0 = timeline.indexOf(r.yw), i1 = timeline.indexOf(r.to)
    return i0 < 0 || i1 < i0 ? [] : timeline.slice(i0, i1 + 1)
  }
  // ชั่วโมงรายสัปดาห์ที่บันทึกไว้ → {yw: {v, keys}} — คีย์ในแผงเป็นเลขเปล่า (= ครั้งที่ใกล้วันนี้สุด)
  // หรือ YW (ปี*100+สัปดาห์ — ใช้กับสัปดาห์ที่เลขเปล่าชี้ไม่ถึง ; WorkDay.py/Calendar.py อ่านได้ทั้งคู่)
  const savedHours = p => {
    const out = {}
    for (const [w, v] of Object.entries(p.week_hours || {})) {
      const n = Number(w), yw = n >= 100000 ? n : resolveBare(n)
      if (yw == null) continue
      const s = (out[yw] = out[yw] || { v: Number(v), keys: [] })
      s.keys.push(String(w))
      if (n >= 100000) s.v = Number(v)
    }
    return out
  }
  // วันเฉพาะสัปดาห์ที่บันทึกไว้ → {yw: วัน}
  const savedDays = p => {
    const out = {}
    for (const [w, v] of Object.entries(p.week_days || {})) {
      const yw = toYw(w)
      if (yw != null) out[yw] = Number(v)
    }
    return out
  }
  // เพดานวันของสัปดาห์ = วันที่ปฏิทินเปิดของปีนั้น รวมสัปดาห์ที่ยุบเข้ามา (null = ไม่มีข้อมูลปฏิทิน)
  const calYw = data?.cal_days_yw || {}
  const capOf = yw => {
    if (yw == null) return null
    const bucket = [yw, ...Object.entries(merges).filter(([, d]) => toYw(d) === yw).map(([s]) => toYw(s))]
    const vals = bucket.map(b => calYw[String(b)])
    if (vals.every(v => v == null)) return null
    return vals.reduce((s, v) => s + Number(v || 0), 0)
  }
  // สถานะหลังรวมที่แก้ค้าง → {mc: {yw: เครื่อง}, hours: {yw: ชม.}, days: {yw: วัน}}
  // เฉพาะสัปดาห์ที่ตั้งไว้ (ที่เหลือ = ค่าของ Cat)
  const stateOf = p => {
    const base = poolBase(p), ch = Number(catHours(p))
    const mc = origWeeks(p)
    const hours = Object.fromEntries(Object.entries(savedHours(p)).map(([yw, s]) => [yw, s.v]))
    const days = savedDays(p)
    for (const r of drafts[p.key] || []) {
      const m = String(r.mc ?? '').trim(), h = String(r.h ?? '').trim(), d = String(r.d ?? '').trim()
      const mv = m === '' ? null : Number(m), hv = h === '' ? null : Number(h), dv = d === '' ? null : Number(d)
      for (const yw of spanOf(r)) {
        if (r.clear) { delete mc[yw]; delete hours[yw]; delete days[yw]; continue }
        // เท่า Total ในทะเบียน = ไม่ต้องตั้ง
        if (mv === null || mv === base) delete mc[yw]
        else if (Number.isInteger(mv) && mv >= 0) mc[yw] = mv
        // ชั่วโมงเท่าของ Cat = ไม่ต้องตั้ง (ยกเว้นสัปดาห์ที่ยุบรวมมา ซึ่งต้องระบุเสมอ)
        if (hv === null || (!mergeYw.has(yw) && hv === ch)) delete hours[yw]
        else if (Number.isFinite(hv) && hv >= 0 && hv <= 24) hours[yw] = hv
        // วัน: ว่าง = คิดจากค่ามาตรฐานของ Cat + ปฏิทิน ; ใส่ตัวเลข = ใช้ตรง ๆ (ไม่หักวันหยุดซ้ำ)
        if (dv === null) delete days[yw]
        else if (Number.isFinite(dv) && dv >= 0) days[yw] = dv
      }
    }
    return { mc, hours, days }
  }
  const mcOf = (st, yw, base) => st.mc[yw] ?? base
  const mcNow = p => mcOf(stateOf(p), todayYw, poolBase(p))
  const editRows = (p, fn) => setDrafts(d => ({ ...d, [p.key]: fn(d[p.key] || []) }))
  const editRow = (p, id, patch) => editRows(p, rows => rows.map(r => (r.id === id ? { ...r, ...patch } : r)))
  // บรรทัดใหม่เติม "ค่าที่ใช้อยู่ ณ สัปดาห์นั้น" — เปิดดูเฉย ๆ แล้วบันทึกก็ไม่มีอะไรเปลี่ยน
  const rowAt = (p, yw) => {
    const s = savedHours(p)[yw]
    const st = stateOf(p)
    return {
      id: 'n' + yw + '-' + Date.now(), yw, to: null,
      d: st.days[yw] != null ? String(st.days[yw]) : '',
      h: st.hours[yw] != null ? String(st.hours[yw]) : s ? String(s.v) : (mergeYw.has(yw) ? '' : fmt(catHours(p))),
      mc: String(yw != null ? mcOf(st, yw, poolBase(p)) : poolBase(p)),
    }
  }
  // at = สัปดาห์ที่คลิกจากตาราง ; ไม่ระบุ = สัปดาห์ถัดจากบรรทัดสุดท้าย (หรือสัปดาห์นี้)
  const addRow = (p, at) => editRows(p, rows => {
    const used = new Set(rows.flatMap(spanOf))
    if (at != null && used.has(at)) return rows
    const lastI = Math.max(todayIdx, ...rows.map(r => timeline.indexOf(r.to ?? r.yw) + 1))
    const yw = at ?? timeline.slice(lastI).find(y => !used.has(y)) ?? null
    return [...rows, rowAt(p, yw)]
  })
  const delRow = (p, id) => editRows(p, rows => rows.filter(r => r.id !== id))
  const [yearSel, setYearSel] = useState({})   // ปีที่เลือกดูในตารางทั้งปี ต่อกอง
  const [flash, setFlash] = useState(null)     // บรรทัดที่เพิ่งกระโดดไปจากตารางทั้งปี
  // คลิกช่องในตารางทั้งปี → มีบรรทัดที่ครอบสัปดาห์นั้นแล้วเลื่อนไปหา / ยังไม่มีเปิดบรรทัดสัปดาห์นั้นให้แก้
  const jumpRow = (p, yw) => {
    const hit = (drafts[p.key] || []).find(r => spanOf(r).includes(yw))
    const id = hit ? hit.id : rowAt(p, yw).id
    if (!hit) editRows(p, rows => [...rows, { ...rowAt(p, yw), id }])
    setFlash(`${p.key}|${id}`)
    setTimeout(() => {
      document.getElementById(`wl-${p.key}-${id}`)?.scrollIntoView({ block: 'center', behavior: 'smooth' })
    }, 0)
    setTimeout(() => setFlash(f => (f === `${p.key}|${id}` ? null : f)), 1600)
  }

  // สิ่งที่เปลี่ยนจากของเดิมของกองนี้ → {key, week_hours?, mc_weeks?} ; ไม่มีอะไรเปลี่ยน = null
  const poolDiff = p => {
    if (!(drafts[p.key] || []).length) return null
    const out = { key: p.key }
    const base = poolBase(p), ch = Number(catHours(p))
    const st = stateOf(p)
    // เครื่อง: เทียบผลจริงรายสัปดาห์ (ไม่ได้ตั้ง = Total ในทะเบียน)
    const oldM = origWeeks(p), newM = st.mc
    for (const k of new Set([...Object.keys(oldM), ...Object.keys(newM)])) {
      if ((oldM[k] ?? base) !== (newM[k] ?? base)) (out.mc_weeks = out.mc_weeks || {})[k] = newM[k] ?? null
    }
    // ชั่วโมง: เทียบผลจริง (ไม่ได้ตั้ง = ชั่วโมงของ Cat) ยกเว้นสัปดาห์ที่ยุบรวมมา ที่ "ไม่ได้ตั้ง" ≠ "ตั้งเท่า Cat"
    // เก็บเป็น YW ทุกสัปดาห์ (รวมสัปดาห์ยุบ) — ค่าของปีนี้ต้องไม่ไหลไปใช้กับปีหน้า
    const sh = savedHours(p)
    for (const k of new Set([...Object.keys(sh), ...Object.keys(st.hours)])) {
      const yw = Number(k), merge = mergeYw.has(yw)
      const a = sh[k]?.v, b = st.hours[k]
      if (merge ? a === b : (a ?? ch) === (b ?? ch)) continue
      const wh = (out.week_hours = out.week_hours || {})
      const canon = String(yw)
      for (const old of sh[k]?.keys || []) if (old !== canon) wh[old] = null
      wh[canon] = b ?? null
    }
    // วัน: เทียบค่าที่ตั้งไว้ตรง ๆ (ไม่ได้ตั้ง = คิดจากค่ามาตรฐาน + ปฏิทิน)
    const sd = savedDays(p)
    for (const k of new Set([...Object.keys(sd), ...Object.keys(st.days)])) {
      if ((sd[k] ?? null) === (st.days[k] ?? null)) continue
      ;(out.week_days = out.week_days || {})[String(k)] = st.days[k] ?? null
    }
    return out.mc_weeks || out.week_hours || out.week_days ? out : null
  }
  const poolDiffs = pools.map(poolDiff).filter(Boolean)

  // ด่านของรายการรายสัปดาห์: เลือกสัปดาห์ครบ ช่วงถูกทิศ ไม่ซ้อนกัน ตัวเลขถูกช่วง
  const rowBad = (p, r, rows) => {
    const span = spanOf(r)
    const s = { yw: r.yw == null, to: r.yw != null && span.length === 0, dup: false, h: false, mc: false, d: false }
    s.dup = span.some(yw => rows.some(x => x !== r && spanOf(x).includes(yw)))
    if (r.clear) return s
    const h = String(r.h ?? '').trim(), m = String(r.mc ?? '').trim(), d = String(r.d ?? '').trim()
    if (h !== '') { const n = Number(h); s.h = !Number.isFinite(n) || n < 0 || n > 24 }
    // วันต้องไม่เกินวันที่ปฏิทินเปิด (รวมสัปดาห์ที่ยุบเข้ามา) ของทุกสัปดาห์ในช่วง
    if (d !== '') {
      const n = Number(d)
      s.d = !Number.isFinite(n) || n < 0 || span.some(yw => { const c = capOf(yw); return c != null && n > c + 1e-9 })
    }
    if (m !== '') { const n = Number(m); s.mc = !Number.isInteger(n) || n < 0 }
    return s
  }
  // เพดานวันของบรรทัด (ช่วงหลายสัปดาห์ = ตัวที่น้อยสุดในช่วง)
  const span1Cap = r => {
    const caps = spanOf(r).map(capOf).filter(c => c != null)
    return caps.length ? Math.min(...caps) : null
  }
  const draftBad = []
  for (const p of pools) {
    const rows = drafts[p.key]
    if (!rows) continue
    for (const r of rows) {
      const s = rowBad(p, r, rows)
      if (s.yw) draftBad.push(`${p.mc_cat} ${p.guage}: ยังไม่ได้เลือกสัปดาห์`)
      if (s.to) draftBad.push(`${p.mc_cat} ${p.guage}: ${ywLabel(r.yw)} "ถึงสัปดาห์" ต้องไม่อยู่ก่อนสัปดาห์เริ่ม`)
      if (s.dup) draftBad.push(`${p.mc_cat} ${p.guage}: ${ywLabel(r.yw)} ซ้อนกับบรรทัดอื่น`)
      if (s.h) draftBad.push(`${p.mc_cat} ${p.guage} ${ywLabel(r.yw)}: ชม./วัน ต้องอยู่ระหว่าง 0-24`)
      if (s.d) draftBad.push(`${p.mc_cat} ${p.guage} ${ywLabel(r.yw)}: วันต้องไม่ติดลบ และไม่เกินวันที่ปฏิทินเปิด`)
      if (s.mc) draftBad.push(`${p.mc_cat} ${p.guage} ${ywLabel(r.yw)}: จำนวนเครื่องต้องเป็นจำนวนเต็ม 0 ขึ้นไป`)
    }
  }
  const allBad = [...new Set([...bad, ...draftBad])]
  const dirty = Object.keys(edit).length + poolDiffs.length

  // ค่าชั่วโมงของสัปดาห์ยุบ ที่ "กำลังจะเป็น" (รวมที่แก้ค้างไว้) — '' = ยังไม่ได้ระบุ
  const weekHour = (p, w) => {
    const v = stateOf(p).hours[toYw(w)]
    return v == null ? '' : String(v)
  }

  const weekDay = (p, w) => {
    const v = stateOf(p).days[toYw(w)]
    return v == null ? '' : String(v)
  }

  // ด่านบังคับ: สัปดาห์ที่ยุบรวมมา ต้องระบุวัน + ชั่วโมงครบทุกกองที่มีเครื่อง และวันไม่เกินเพดานปฏิทิน
  // (สัปดาห์ที่รวมกันแล้วมักเดินไม่เท่าปกติ ปล่อยว่าง = ระบบรวมวัน/ใช้ชั่วโมงของกองเงียบ ๆ)
  const gapsNow = useMemo(() => {
    const out = []
    for (const w of liveMergeTargets) {
      const yw = toYw(w), cap = capOf(yw)
      const noDay = [], noHour = [], over = []
      for (const p of pools) {
        if (!((p.total || 0) > 0)) continue
        const d = weekDay(p, w)
        if (d === '') noDay.push(p)
        else if (cap != null && Number(d) > cap + 1e-9) over.push(p)
        if (weekHour(p, w) === '') noHour.push(p)
      }
      const miss = [...new Set([...noDay, ...noHour, ...over])]
      if (miss.length) out.push({ week: w, yw, cap, pools: miss, noDay, noHour, over })
    }
    return out
  }, [pools, mergeTargets, drafts, timeline, data])

  // กรอกวันของสัปดาห์ยุบทีเดียวให้ทุก Cat ที่ยังขาด (เปิดบรรทัดให้เห็นว่าเติมอะไร แก้รายกองทีหลังได้)
  // ไม่เติมให้เอง — ผู้วางแผนต้องพิมพ์ตัวเลขที่เครื่องเดินได้จริง (ยอดรวมอัตโนมัติแยกไม่ออกจากค่าที่ตั้งใจ)
  const [fillDay, setFillDay] = useState({})   // {yw: 'วัน'} ช่องกรอกในแถบเตือน
  const fillDays = g => {
    const v = String(fillDay[g.yw] ?? '').trim(), n = Number(v)
    if (v === '' || !Number.isFinite(n) || n < 0 || (g.cap != null && n > g.cap + 1e-9)) return
    for (const p of g.noDay) {
      editRows(p, rows => {
        const hit = rows.find(r => spanOf(r).includes(g.yw))
        if (!hit) return [...rows, { ...rowAt(p, g.yw), d: v }]
        return rows.map(r => (r === hit && String(r.d ?? '').trim() === '' ? { ...r, d: v, clear: false } : r))
      })
    }
  }

  // เติมชั่วโมงมาตรฐานของกองให้ทุกช่องที่ยังว่าง — ไม่ต้องไล่กรอกทีละกอง (เปิดบรรทัดให้เห็นว่าเติมอะไร)
  const fillGaps = () => {
    for (const g of gapsNow) {
      const yw = toYw(g.week)
      for (const p of g.noHour) {
        const base = p.work_hours
        if (base === null || base === undefined || base === '' || yw == null) continue
        editRows(p, rows => {
          const hit = rows.find(r => spanOf(r).includes(yw))
          if (!hit) return [...rows, { ...rowAt(p, yw), h: String(base) }]
          return rows.map(r => (r === hit && String(r.h ?? '').trim() === '' ? { ...r, h: String(base), clear: false } : r))
        })
      }
    }
  }

  async function save() {
    setSaving(true); setMsg(''); setErr('')
    const machines = [], byPool = {}
    for (const [k, v] of Object.entries(edit)) {
      const [kind, id] = [k.slice(0, k.indexOf(':')), k.slice(k.indexOf(':') + 1)]
      if (kind === 'mc') machines.push({ row: Number(id), total: v })
      else if (kind === 'wd') (byPool[id] = byPool[id] || { key: id }).work_days = v === '' ? null : v
      else if (kind === 'wh') (byPool[id] = byPool[id] || { key: id }).work_hours = v === '' ? null : v
      else if (kind === 'ww') {
        // id = "FACTORY|MC_CAT|GUAGE|WEEK" — สัปดาห์อยู่ท้ายสุด
        const at = id.lastIndexOf('|')
        const pk = id.slice(0, at), wk = id.slice(at + 1)
        const slot = (byPool[pk] = byPool[pk] || { key: pk })
        slot.week_hours = slot.week_hours || {}
        slot.week_hours[wk] = String(v ?? '').trim() === '' ? null : v
      }
    }
    // รายการรายสัปดาห์: ชั่วโมง → แผงวันทำงาน (YW) · เครื่อง → MC_Total (YW)
    for (const d of poolDiffs) {
      const slot = (byPool[d.key] = byPool[d.key] || { key: d.key })
      if (d.week_hours) slot.week_hours = { ...(slot.week_hours || {}), ...d.week_hours }
      if (d.week_days) slot.week_days = d.week_days
      if (d.mc_weeks) slot.mc_weeks = d.mc_weeks
    }
    try {
      const r = await api.mcHubSave({ machines, pools: Object.values(byPool) }, data.revs)
      setEdit({}); setDrafts({})
      setMsg('บันทึกแล้ว: ' + (r.done?.join(' · ') || 'ไม่มีอะไรเปลี่ยน'))
      await load()
    } catch (e) {
      setErr(e.conflict ? 'มีคนแก้แซงไป — กดโหลดใหม่แล้วแก้อีกครั้ง' : 'บันทึกไม่สำเร็จ: ' + e.message)
    } finally { setSaving(false) }
  }

  async function fixNames() {
    setFixing(true)
    try {
      const r = await api.mcFixWorkdayNames()
      setMsg(r.moved ? `ย้ายค่าวันทำงานตามชื่อใหม่แล้ว ${r.moved} Cat` : 'ไม่มีอะไรต้องย้าย')
      await load()
    } catch (e) { setErr('ย้ายไม่สำเร็จ: ' + e.message) }
    finally { setFixing(false) }
  }

  // ---- Export / Import 4 ชีท (Master MC · MC_Total · Lock_MC · MC Special) เป็นไฟล์เดียว
  async function doExport() {
    setExporting(true); setErr('')
    try { await api.mcHubExport() }
    catch (e) { setErr('Export ไม่สำเร็จ: ' + e.message) }
    finally { setExporting(false) }
  }
  async function checkFile(file) {
    if (!file) return
    setSaving(true); setErr(''); setImp({ file, check: null })
    try { setImp({ file, check: await api.mcHubImportCheck(file) }) }
    catch (e) { setErr('อ่านไฟล์ไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }
  async function doImport() {
    const c = imp?.check
    if (!c?.ok) return
    if (!window.confirm('Import ทับชีท ' + c.sheets.map(s => `${s.sheet} (เดิม ${s.old} → ใหม่ ${s.count} แถว)`).join(', ')
      + '\n\nชีทที่ไม่มีในไฟล์คงเดิม · ย้อนดูได้ที่ประวัติการแก้ไข ยืนยัน?')) return
    setSaving(true); setErr(''); setMsg('')
    try {
      const r = await api.mcHubImport(imp.file, Object.fromEntries(c.sheets.map(s => [s.sheet, s.rev])))
      setImp(null)
      setMsg('บันทึกแล้ว: ' + (r.done?.join(' · ') || ''))
      await load()
    } catch (e) {
      setErr(e.conflict ? 'มีคนแก้ชีทนี้แซงไป — เลือกไฟล์ใหม่อีกครั้งเพื่อตรวจกับข้อมูลล่าสุด' : 'Import ไม่สำเร็จ: ' + e.message)
    } finally { setSaving(false) }
  }

  if (!data) return <div className="hint">{err || 'กำลังโหลดข้อมูลเครื่อง...'}</div>

  const totalMc = pools.reduce((a, p) => a + (p.total || 0), 0)
  const noWd = pools.filter(p => p.total > 0 && (p.work_days === null || p.work_days === undefined))

  return (
    <div className="mchub">
      <div className="mchubbar">
        <span className="mchubtotal">
          🛠 <b>{pools.length}</b> Cat · รวม <b className="n">{totalMc}</b> เครื่อง
        </span>
        <input className="mchubsearch" placeholder="🔍 ค้นหา Factory / CAT / Group / Guage"
          value={q} onChange={e => setQ(e.target.value)} />
        {!ro && <button className="mchubadd" onClick={() => setWizard(true)}>➕ เพิ่มเครื่องใหม่</button>}
        <button className="ghost sm" onClick={load}>⟳ โหลดใหม่</button>
        {!ro && (
          <button className="ghost sm" disabled={saving} onClick={() => setImp(s => (s ? null : { file: null, check: null }))}>
            {imp ? '✕ ปิด Import' : '📥 Import'}
          </button>
        )}
        <button className="ghost sm" onClick={doExport} disabled={exporting}
          title="ดาวน์โหลด Master MC · MC_Total · Lock_MC · MC Special เป็นไฟล์เดียว (Import กลับได้)">
          {exporting ? 'กำลัง Export...' : '📤 Export'}
        </button>
      </div>

      {err && <div className="msg err">{err}</div>}
      {msg && <div className="msg ok">{msg}</div>}

      {!ro && imp && (
        <div className="s9import">
          <div className="s9improw">
            <button className="ghost sm" disabled={saving} onClick={() => fileRef.current?.click()}>
              📄 เลือกไฟล์ .xlsx
            </button>
            <input ref={fileRef} type="file" accept=".xlsx,.xlsm" hidden
              onChange={e => { checkFile(e.target.files?.[0]); e.target.value = '' }} />
            <span className="hint small">
              ใช้ไฟล์จากปุ่ม Export ได้เลย · ทับเฉพาะชีทที่มีในไฟล์ (ชื่อชีทต้องตรง:
              <b> Master MC · MC_Total · Lock_MC · MC Special</b>) · สลับลำดับคอลัมน์ได้ ·
              วันทำงาน/ชั่วโมงไม่อยู่ในไฟล์นี้ (แก้ในหน้านี้ตามเดิม)
            </span>
          </div>
          {imp.check && (
            <>
              {imp.check.sheets.map(s => (
                <div key={s.sheet} className={s.ok ? 's9impsum' : 's9impwarn'}>
                  {s.ok
                    ? <>{imp.file?.name} · <b>{s.sheet}</b> เดิม <b>{s.old}</b> → ใหม่ <b>{s.count}</b> แถว</>
                    : <>⛔ <b>{s.sheet}</b>: {s.error}</>}
                  {(s.warn || []).map(w => <div key={w} className="s9impwarn">⚠️ {w}</div>)}
                </div>
              ))}
              <div className="s9improw">
                <button className="primary" onClick={doImport} disabled={saving || !imp.check.ok || dirty > 0}
                  title={dirty > 0 ? 'บันทึกหรือยกเลิกการแก้ที่ค้างอยู่ก่อน' : !imp.check.ok ? 'แก้ไฟล์ให้ผ่านทุกชีทก่อน' : ''}>
                  ยืนยัน Import
                </button>
                {dirty > 0 && <span className="s9impwarn">⚠️ มีการแก้ค้างอยู่ — บันทึกหรือยกเลิกก่อน Import</span>}
              </div>
            </>
          )}
        </div>
      )}
      {!data.workday_ok && (
        <div className="msg warn">⚠️ อ่านแผงวันทำงานไม่ได้ ({data.workday_error}) — ช่องวัน/ชั่วโมงจะว่างไว้ก่อน</div>
      )}
      {noWd.length > 0 && (
        <div className="msg warn">
          ⚠️ {noWd.length} Cat ยังไม่ได้ตั้งวันทำงาน — ระบบจะใช้ 6 วัน/สัปดาห์กับ Cat เหล่านี้
          {!ro && <button className="mchubfix" onClick={fixNames} disabled={fixing}>
            {fixing ? 'กำลังตรวจ...' : '🔧 ถ้าเป็นเพราะเปลี่ยนชื่อกลุ่ม กดย้ายค่าเดิมมาให้'}
          </button>}
        </div>
      )}

      {gapsNow.length > 0 && (
        <div className="msg err">
          ⛔ มีสัปดาห์ที่ยุบรวมกันแล้วยังไม่ได้ระบุวัน/ชั่วโมงทำงาน — บันทึกไม่ได้จนกว่าจะกรอกครบ
          <div className="mchubgaplist">
            {gapsNow.map(g => {
              const v = String(fillDay[g.yw] ?? '').trim(), n = Number(v)
              const okFill = v !== '' && Number.isFinite(n) && n >= 0 && (g.cap == null || n <= g.cap + 1e-9)
              return (
                <div key={g.week}>
                  <b>{ywLabel(g.yw)}</b> (รวมจาก {Object.entries(merges).filter(([, d]) => toYw(d) === g.yw)
                    .map(([s]) => ywLabel(toYw(s))).join(', ')})
                  {g.cap != null && <> · ปฏิทินเปิดรวม <b>{g.cap}</b> วัน</>}
                  {g.noDay.length > 0 && <> · ขาดวัน <b>{g.noDay.length}</b> Cat</>}
                  {g.noHour.length > 0 && <> · ขาดชม. <b>{g.noHour.length}</b> Cat</>}
                  {g.over.length > 0 && <> · <b>วันเกินเพดาน {g.over.length} Cat</b></>}
                  {!ro && g.noDay.length > 0 && (
                    <span className="mchubfillday">
                      {' '}→ วัน{' '}
                      <input inputMode="decimal" value={fillDay[g.yw] ?? ''} placeholder={g.cap != null ? `≤ ${g.cap}` : ''}
                        className={v !== '' && !okFill ? 'bad' : ''}
                        onChange={e => setFillDay(f => ({ ...f, [g.yw]: e.target.value.replace(/[^0-9.]/g, '') }))} />
                      <button className="ghost sm" disabled={!okFill} onClick={() => fillDays(g)}>
                        ใส่ให้ทุก Cat ที่ยังขาด
                      </button>
                    </span>
                  )}
                </div>
              )
            })}
          </div>
          {!ro && gapsNow.some(g => g.noHour.length > 0) && <button className="mchubfix" onClick={fillGaps}>
            ↳ เติมชั่วโมงมาตรฐานของแต่ละ Cat ให้ช่องชั่วโมงที่ว่างทั้งหมด (แก้ทีหลังได้)
          </button>}
        </div>
      )}

      <div className="mchubhead">
        <span>โรงงาน · กลุ่มเครื่อง · Guage</span>
        <span className="c">เครื่อง</span>
        <span className="c">วัน/สัปดาห์</span>
        <span className="c">ชม./วัน</span>
        <span className="c">อื่น ๆ</span>
      </div>

      {shown.map(p => {
        const isOpen = !!open[p.key]
        const wdKey = `wd:${p.key}`, whKey = `wh:${p.key}`
        const futurePts = (p.mc_weeks || []).filter(x => !todayYw || x.yw >= todayYw).length
        const extra = (p.weeks?.length || 0) + (p.locks?.length || 0) + (p.reserved?.length || 0) + futurePts
        const nowMc = timeline.length ? mcNow(p) : undefined
        return (
          <div key={p.key} className={'mchubpool' + (isOpen ? ' open' : '')}>
            <div className="mchubrow">
              <button className="mchubname" onClick={() => setOpen(o => ({ ...o, [p.key]: !o[p.key] }))}>
                <span className="caret">{isOpen ? '▾' : '▸'}</span>
                <b>{p.factory}</b> · {p.mc_cat} · Guage {p.guage}
                <small>{p.machines.map(m => m.mc).filter(Boolean).join(', ')}</small>
              </button>
              <span className="c n">
                {p.total}
                {nowMc !== undefined && nowMc !== p.total && (
                  <span className="mchubnow" title="จำนวนเครื่องที่ใช้สัปดาห์นี้ (ตั้งไว้ในค่ารายสัปดาห์)">
                    สัปดาห์นี้ {nowMc}
                  </span>
                )}
              </span>
              <span className="c">
                {ro ? fmt(p.work_days) : (
                  <input className={(isBad(wdKey) ? 'bad ' : '') + (wdKey in edit ? 'chg' : '')}
                    inputMode="decimal" title="ต้องกรอก 0-7 · เว้นว่างไม่ได้"
                    value={val(wdKey, p.work_days)} placeholder="ต้องกรอก"
                    onChange={e => set(wdKey, e.target.value.replace(/[^0-9.]/g, ''))} />
                )}
              </span>
              <span className="c">
                {ro ? fmt(p.work_hours) : (
                  <input className={(isBad(whKey) ? 'bad ' : '') + (whKey in edit ? 'chg' : '')}
                    inputMode="decimal" title="ต้องกรอก 0-24 · เว้นว่างไม่ได้"
                    value={val(whKey, p.work_hours)} placeholder="ต้องกรอก"
                    onChange={e => set(whKey, e.target.value.replace(/[^0-9.]/g, ''))} />
                )}
              </span>
              <span className="c">{extra > 0 && <span className="mchubtag">{extra}</span>}</span>
            </div>

            {isOpen && (
              <div className="mchubdetail">
                <div className="mchubsec">
                  <b>เครื่องใน Cat นี้</b>
                  {p.machines.map(m => {
                    const k = `mc:${m.row}`
                    return (
                      <div key={m.row} className="mchubmc">
                        <span className="nm">{m.mc || '(ไม่ระบุชื่อ)'}</span>
                        <span className="gp">{m.group}</span>
                        {ro ? <span className="n">{fmt(m.total)}</span> : (
                          <input className={(isBad(k) ? 'bad ' : '') + (k in edit ? 'chg' : '')}
                            inputMode="numeric" title="ใส่ 0 ได้ถ้าไม่มีเครื่อง แต่เว้นว่างไม่ได้"
                            value={val(k, m.total)} placeholder="ต้องกรอก"
                            onChange={e => set(k, e.target.value.replace(/[^0-9]/g, ''))} />
                        )}
                        <span className="u">เครื่อง</span>
                      </div>
                    )
                  })}
                </div>

                {(() => {
                  const rows = [...(drafts[p.key] || [])].sort((a, b) => (a.yw ?? 9e9) - (b.yw ?? 9e9))
                  const base = poolBase(p), ch = Number(catHours(p))
                  const st = stateOf(p), sh = savedHours(p), ow = origWeeks(p), sdays = savedDays(p)
                  const mcRep = p.machines[0]?.mc
                  const colKey = yw => weekKey(yw % 100, Math.floor(yw / 100))
                  const isGone = yw => (wdRes ? wdRes.mergedInto(colKey(yw)) != null : false)
                  // วันทำงานจริงของสัปดาห์นั้น (หักปฏิทิน/ยุบสัปดาห์แล้ว — ตัวเดียวกับ Gantt)
                  // ชั่วโมงไม่เปลี่ยนวันทำงาน: เดิน 24 ชม. ใน Cat 20 ชม. = วันเท่าเดิม แต่ผลิตต่อวันได้มากขึ้น
                  // (ภายในระบบคูณกำลังผลิตที่วัน แต่หน้าจอแสดงวันจริงเท่านั้น — user สั่ง: ไม่ให้เห็น "7.2 วัน")
                  const dayOf = yw => {
                    if (yw == null || !wdRes || !mcRep) return null
                    const eq = Number(wdRes.workDays(mcRep, p.guage, colKey(yw)))
                    const ratio = Number(wdRes.hourRatio(mcRep, p.guage, colKey(yw)))
                    return ratio > 0 ? r1(eq / ratio) : null
                  }
                  const opts = (cur, from) => {
                    const out = timeline.filter(yw => !isGone(yw) && (from == null || yw >= from))
                    return cur != null && !out.includes(cur) ? [cur, ...out] : out
                  }
                  const years = [...new Set(timeline.map(yw => Math.floor(yw / 100)))]
                  const tlSet = new Set(timeline)
                  const year = yearSel[p.key] ?? Math.floor(todayYw / 100)
                  const openYw = new Set(rows.flatMap(spanOf))
                  return (
                    <div className="mchubsec">
                      <b>ค่ารายสัปดาห์ (วัน · ชม./วัน · จำนวนเครื่อง)</b>
                      <span className="hint small">
                        {ro ? '' : 'คลิกช่องในตารางเพื่อแก้สัปดาห์นั้น · '}
                        วัน ชม./วัน และจำนวนเครื่อง ใช้เฉพาะสัปดาห์ที่กรอก (ใส่ "ถึง" = ทั้งช่วง) · สัปดาห์ที่ไม่ได้กรอกใช้
                        {' '}{fmt(p.work_days) || '—'} วัน (ปรับตามวันหยุดในปฏิทิน), {fmt(catHours(p)) || '—'} ชม.
                        และ {base} เครื่องตามทะเบียน · วันที่กรอกใช้ตรง ๆ ไม่หักวันหยุดซ้ำ ·
                        กรอบน้ำเงิน = สัปดาห์ที่ตั้งค่าไว้ · กรอบส้ม = สัปดาห์ที่ยุบรวมมาแต่ยังไม่มีวัน/ชั่วโมง ·
                        การยุบสัปดาห์ตั้งที่หน้าปฏิทิน (Calendar › ⚙ ยุบสัปดาห์)
                      </span>
                      {!timeline.length && <div className="msg warn">อ่านปฏิทินไม่ได้ — ยังแก้ค่ารายสัปดาห์ไม่ได้</div>}

                      {rows.length > 0 && (
                        <div className="mchubwl">
                          <div className="mchubwlh">
                            <span>สัปดาห์</span><span>ถึง</span><span>วัน</span><span>ชม./วัน</span><span>เครื่อง</span><span>ผล</span><span />
                          </div>
                          {rows.map(r => {
                            const s = rowBad(p, r, rows)
                            const merge = mergeYw.has(r.yw)
                            const h = String(r.h ?? '').trim(), dd = String(r.d ?? '').trim()
                            const needH = merge && h === '' && !r.clear
                            const needD = merge && dd === '' && !r.clear
                            const capR = span1Cap(r)
                            const from = merge ? Object.entries(merges)
                              .filter(([, d]) => toYw(d) === r.yw).map(([x]) => ywLabel(toYw(x))) : []
                            const span = spanOf(r)
                            const hadSaved = span.some(yw => sh[yw] != null || ow[yw] != null || sdays[yw] != null)
                            const hv = r.clear ? (st.hours[r.yw] ?? ch) : (h !== '' ? Number(h) : ch)
                            // ผล: วันที่พิมพ์ไว้ (ยังไม่บันทึก) ก่อน ไม่งั้นวันจริงตามที่บันทึกแล้ว
                            const days = span.length !== 1 ? null
                              : (dd !== '' && !r.clear && Number.isFinite(Number(dd)) ? Number(dd) : dayOf(r.yw))
                            const eff = r.yw != null ? mcOf(st, r.yw, base) : null
                            return (
                              <div key={r.id} id={`wl-${p.key}-${r.id}`}
                                className={'mchubwlr' + (merge ? ' recv' : '') + (r.clear ? ' clear' : '')
                                  + (flash === `${p.key}|${r.id}` ? ' flash' : '')}>
                                <span className="wk">
                                  {ro ? <b>{ywLabel(r.yw)}</b> : (
                                    <select className={s.yw || s.dup ? 'bad' : ''} value={r.yw ?? ''}
                                      onChange={e => editRow(p, r.id, { yw: e.target.value === '' ? null : Number(e.target.value) })}>
                                      {r.yw == null && <option value="">เลือก</option>}
                                      {opts(r.yw).map(yw => <option key={yw} value={yw}>{ywLabel(yw)}</option>)}
                                    </select>
                                  )}
                                  {from.length > 0 && <span className="tag e" title="สัปดาห์ที่ยุบรวมเข้ามา">◀{from.join(',')}</span>}
                                </span>
                                <span className="wk">
                                  {ro ? (r.to != null ? ywLabel(r.to) : '') : (
                                    <select className={s.to ? 'bad' : ''} value={r.to ?? ''} disabled={r.yw == null}
                                      title="ไม่บังคับ — ใส่เมื่อจะใช้ค่าเดียวกันทั้งช่วง"
                                      onChange={e => editRow(p, r.id, { to: e.target.value === '' ? null : Number(e.target.value) })}>
                                      <option value="">—</option>
                                      {opts(r.to, r.yw).filter(yw => yw !== r.yw).map(yw => <option key={yw} value={yw}>{ywLabel(yw)}</option>)}
                                    </select>
                                  )}
                                </span>
                                {r.clear ? (
                                  <span className="clr">จะล้างค่าที่ตั้งไว้ของสัปดาห์นี้</span>
                                ) : (
                                  <>
                                    <span>
                                      <input className={(s.d ? 'bad ' : '') + (needD ? 'need' : '')}
                                        inputMode="decimal" value={r.d ?? ''} disabled={ro}
                                        placeholder={needD ? 'ต้องกรอก' : (span.length === 1 && dayOf(r.yw) != null ? String(dayOf(r.yw)) : '')}
                                        title={`วันทำงานเฉพาะสัปดาห์นี้ · ว่าง = คิดจากค่ามาตรฐานของ Cat + ปฏิทิน`
                                          + (capR != null ? ` · ไม่เกิน ${capR} วัน (ปฏิทินเปิด${merge ? 'รวมทั้งก้อน' : ''})` : '')}
                                        onChange={e => editRow(p, r.id, { d: e.target.value.replace(/[^0-9.]/g, '') })} />
                                    </span>
                                    <span>
                                      <input className={(s.h ? 'bad ' : '') + (needH ? 'need' : '')}
                                        inputMode="decimal" value={r.h ?? ''} disabled={ro}
                                        placeholder={needH ? 'ต้องกรอก' : fmt(catHours(p))}
                                        title="ชม./วัน เฉพาะสัปดาห์นี้ · ว่าง = ชั่วโมงของ Cat"
                                        onChange={e => editRow(p, r.id, { h: e.target.value.replace(/[^0-9.]/g, '') })} />
                                    </span>
                                    <span>
                                      <input className={s.mc ? 'bad' : ''} inputMode="numeric" value={r.mc ?? ''} disabled={ro}
                                        placeholder={String(base)}
                                        title="จำนวนเครื่องของสัปดาห์นี้ (หรือทั้งช่วง) · ว่าง = จำนวนในทะเบียน"
                                        onChange={e => editRow(p, r.id, { mc: e.target.value.replace(/[^0-9]/g, '') })} />
                                    </span>
                                  </>
                                )}
                                <span className="eq">
                                  {span.length > 1 ? <><b>{span.length}</b> สัปดาห์</> : (
                                    <>
                                      {days != null && <><b>{days}</b> วัน · </>}
                                      {eff != null && <b className={eff !== base ? 'diff' : ''}>{eff}</b>} เครื่อง
                                    </>
                                  )}
                                </span>
                                <span className="act">
                                  {!ro && hadSaved && (
                                    <button className="mchubclr" onClick={() => editRow(p, r.id, { clear: !r.clear })}
                                      title={r.clear ? 'ไม่ล้างแล้ว' : 'ล้างค่าที่ตั้งไว้ของสัปดาห์นี้ (วัน/ชั่วโมงกลับเป็นของ Cat · เครื่องกลับเป็นจำนวนในทะเบียน)'}>
                                      {r.clear ? '↶' : 'ล้าง'}
                                    </button>
                                  )}
                                  {!ro && (
                                    <button className="mchubdel" title="ปิดบรรทัดนี้ (ไม่เอาที่แก้ในบรรทัดนี้)"
                                      onClick={() => delRow(p, r.id)}>✕</button>
                                  )}
                                </span>
                              </div>
                            )
                          })}
                        </div>
                      )}
                      {!ro && timeline.length > 0 && (
                        <div className="mchubwlbar">
                          <button className="ghost sm" onClick={() => addRow(p)}>+ เพิ่มสัปดาห์</button>
                        </div>
                      )}

                      {/* ตารางทั้งปี — แสดงผลทุกสัปดาห์ (รวมที่แก้ค้าง) ; คลิกช่อง = เปิดบรรทัดของสัปดาห์นั้นให้แก้ */}
                      {timeline.length > 0 && (
                        <>
                          <div className="mchubyr">
                            {years.map(y => (
                              <button key={y} className={'ghost sm' + (y === year ? ' on' : '')}
                                onClick={() => setYearSel(o => ({ ...o, [p.key]: y }))}>{y}</button>
                            ))}
                          </div>
                          <div className="mchubwkgrid year">
                            {(() => {
                              // ครบทุกเลขสัปดาห์ของปี — ปฏิทินไม่มีบางสัปดาห์ (ยุบไปแล้ว / ปิดทั้งสัปดาห์)
                              // ต้องยังเห็นช่องนั้นพร้อมเหตุผล ไม่ใช่หายไปเฉย ๆ
                              const inYear = timeline.filter(yw => Math.floor(yw / 100) === year)
                              const last = Math.max(52, ...inYear.map(yw => yw % 100))
                              return Array.from({ length: last }, (_, i) => year * 100 + i + 1)
                            })().map(yw => {
                              const inCal = tlSet.has(yw)
                              const gone = wdRes ? wdRes.mergedInto(colKey(yw)) : null
                              if (!inCal && !gone) {
                                return (
                                  <div key={yw} className="mchubwk gone" title="ไม่มีสัปดาห์นี้ในปฏิทิน (ปิดทั้งสัปดาห์)">
                                    <div className="mchubwkh"><span>W{yw % 100}</span></div>
                                    <span className="ro">ไม่มีในปฏิทิน</span>
                                  </div>
                                )
                              }
                              const src = wdRes && !gone ? wdRes.mergeSourcesOf(colKey(yw)) : []
                              const merge = mergeYw.has(yw)
                              const hSet = st.hours[yw], dSet = st.days[yw]
                              const needH = merge && hSet == null && (p.total || 0) > 0
                              const needD = merge && dSet == null && (p.total || 0) > 0
                              const need = needH || needD
                              const hv = hSet ?? ch
                              const isSet = hSet != null || dSet != null || st.mc[yw] != null
                              const days = gone ? null : (dSet ?? dayOf(yw))
                              const mc = mcOf(st, yw, base)
                              const click = !ro && !gone && inCal
                              return (
                                <div key={yw}
                                  className={'mchubwk' + (gone ? ' gone' : src.length ? ' recv' : '')
                                    + (isSet ? ' set' : '') + (need ? ' need' : '') + (openYw.has(yw) ? ' editing' : '')
                                    + (yw < todayYw ? ' past' : '') + (yw === todayYw ? ' today' : '')
                                    + (click ? ' click' : '')}
                                  title={gone ? `ยุบไปรวมที่ W${gone}` : click ? 'คลิกเพื่อแก้สัปดาห์นี้' : ''}
                                  onClick={click ? () => jumpRow(p, yw) : undefined}>
                                  <div className="mchubwkh">
                                    <span>W{yw % 100}</span>
                                    {gone ? <span className="tag o">→W{gone}</span>
                                      : src.length ? <span className="tag e">◀W{src.join(',W')}</span> : null}
                                  </div>
                                  {gone ? <span className="ro">—</span> : (
                                    <>
                                      <span className={'yv' + (needH ? ' need' : '')}>{needH ? 'ต้องกรอก ชม.' : `${hv} ชม.`}</span>
                                      <span className={'yv' + (mc !== base ? ' diff' : '')}><b>{mc}</b> เครื่อง</span>
                                      {needD ? <span className="yv need">ต้องกรอก วัน</span>
                                        : days != null && <span className={'eq' + (dSet != null ? ' set' : '')}>{days} วัน</span>}
                                    </>
                                  )}
                                </div>
                              )
                            })}
                          </div>
                        </>
                      )}
                    </div>
                  )
                })()}

                {p.weeks?.length > 0 && (
                  <div className="mchubsec">
                    <b>ค่าเฉพาะบางสัปดาห์</b>
                    {p.weeks.map((w, i) => (
                      <div key={i} className="mchubline">
                        {w.kind === 'total'
                          ? <>W{w.week}{w.week_to ? `–${w.week_to}` : ''}: มีเครื่อง <b>{w.value}</b> ตัว
                            {w.mc ? ` (เฉพาะ ${w.mc})` : ''}{w.remark ? ` — ${w.remark}` : ''}</>
                          : <>{w.week >= 100000 ? ywLabel(w.week) : `W${w.week}`}: ทำงาน <b>{w.value}</b> วัน</>}
                      </div>
                    ))}
                  </div>
                )}

                {p.locks?.length > 0 && (
                  <div className="mchubsec">
                    <b>กันเครื่องไม่ให้ระบบใช้</b>
                    {p.locks.map((l, i) => (
                      <div key={i} className="mchubline">
                        W{l.week}{l.week_to ? `–${l.week_to}` : ''}: กัน <b>{l.qty ?? 0}</b> เครื่อง
                        {l.mc ? ` (เฉพาะ ${l.mc})` : ''}{l.item ? ` · ให้ผ้า ${l.item}` : ''}
                        {l.shared_with?.length > 0 && (
                          <span className="warn"> · รายการนี้ใช้ร่วมกับ {l.shared_with.join(', ')}</span>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {p.reserved?.length > 0 && (
                  <div className="mchubsec">
                    <b>กันไว้ให้ POLY / COTTON</b>
                    {p.reserved.map((r, i) => (
                      <div key={i} className="mchubline">
                        {r.mc ? `${r.mc}: ` : ''}
                        {r.poly ? `POLY ${r.poly} เครื่อง ` : ''}{r.cotton ? `COTTON ${r.cotton} เครื่อง` : ''}
                        {r.note ? ` — ${r.note}` : ''}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )
      })}

      {!shown.length && <div className="hint">ไม่พบ Cat ที่ค้นหา</div>}

      {!ro && (dirty > 0 || allBad.length > 0 || gapsNow.length > 0) && (
        <div className={'mchubsave' + (allBad.length || gapsNow.length ? ' blocked' : '')}>
          {allBad.length > 0
            ? <span className="bad">⛔ กรอกไม่ครบ — {allBad.join(' · ')}</span>
            : gapsNow.length > 0
              ? <span className="bad">⛔ สัปดาห์ที่ยุบรวมมายังขาดวัน/ชั่วโมง {gapsNow.map(g => `${ywLabel(g.yw)} (${g.pools.length} Cat)`).join(' · ')}</span>
              : <span>✏️ แก้ค้างไว้ <b>{dirty}</b> รายการ</span>}
          <button onClick={() => { setEdit({}); setDrafts({}) }}
            disabled={saving || (!dirty && !Object.keys(drafts).length)}>ยกเลิก</button>
          <button className="primary" onClick={save} disabled={saving || !dirty || allBad.length > 0 || gapsNow.length > 0}
            title={allBad.length || gapsNow.length ? 'ต้องกรอกให้ครบก่อนถึงจะบันทึกได้' : ''}>
            {saving ? 'กำลังบันทึก...' : 'บันทึก'}
          </button>
        </div>
      )}

      {wizard && (
        <McNewWizard
          rows={pools.flatMap(p => p.machines.map(m => ({
            factory: p.factory, type: p.type, mc_cat: p.mc_cat, mc: m.mc,
            guage: p.guage, group: m.group,
          })))}
          onClose={() => setWizard(false)} onDone={load} />
      )}

      <div className="gridhelp">
        หนึ่งแถว = หนึ่ง Cat (Factory · CAT · Guage) — หน่วยเดียวกับที่ระบบใช้คิดวันทำงาน ·
        กดชื่อ Cat เพื่อดู/แก้เครื่องแต่ละตัว · แก้ที่นี่แล้วระบบเขียนลงไฟล์เดิมให้ครบทุกที่
      </div>
    </div>
  )
}

import React, { useState, useEffect, useMemo, useRef } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'

function fmtQty(v) {
  const n = Number(v) || 0
  return n.toLocaleString('th-TH', { maximumFractionDigits: 0 })
}

// เกจต้นทางที่ถอดเครื่องมาเปลี่ยนได้ → "26 (ว่าง 4), 22 (ว่าง 3)"
function fmtSources(sources) {
  if (!sources || sources.length === 0) return '-'
  return sources.map(s => `Guage ${s.gauge} (ว่าง ${s.free})`).join(', ')
}

const keyOf = (c) => `${c.factory}|${c.cat}|${c.src_gauge}>${c.gauge}|W${c.need_week}`

// ทุกแถวที่เปลี่ยน "เข้า" เกจเดียวกันของโรงงาน+MC_CAT เดียวกัน ใช้ Cylinder สำรองก้อนเดียวกัน
// (คอลัมน์ "Cylinder เหลือ" ที่ขึ้นเลขเดียวกันทุกแถว = ยอดก้อนนี้ ไม่ใช่ของใครของมัน)
const tgtKeyOf = (c) => `${c.factory}|${String(c.cat).toUpperCase()}|${c.gauge}`
// เครื่องว่างของเกจต้นทางเป็นของ "สัปดาห์นั้น" — คนละสัปดาห์ไม่แย่งกัน แต่สัปดาห์เดียวกันแย่ง
const srcKeyOf = (c) => `${c.factory}|${String(c.cat).toUpperCase()}|${c.src_gauge}|W${c.need_week}`
// โควตาแรงงาน: เปลี่ยนได้กี่เครื่อง/สัปดาห์/กลุ่ม (PHET_DOUBLE, PHET_SINGLE, OMNOI)
// — จำนวนมาจากหน้า "ตั้งค่าระบบ" (backend ส่งมาใน limits.cylinder_per_week)
// นับที่สัปดาห์ที่ "ลงมือเปลี่ยน" (set_week) — ไม่ใช่สัปดาห์ที่เครื่องพร้อมใช้
const quotaKeyOf = (c) => `${c.quota_group || ''}|W${c.set_week}`
// job setup: ตั้งเครื่องใหม่ 1 เครื่อง = 1 job ของสัปดาห์ที่ "เครื่องพร้อมใช้" (need_week)
// คนละ pool กับโควตาเปลี่ยน Cylinder — job นับรวมงานทั้งโรงในสัปดาห์นั้น
// (เพดานต่อประเภทโรงงานมาจากหน้า "ตั้งค่าระบบ" → limits.job_cap)
const jobKeyOf = (c) => `${c.job_type || ''}|W${c.need_week}`

/**
 * Change Cylinder (AI) — เสนอว่าควรเปลี่ยน Cylinder ให้ item ไหนก่อน แล้วให้ user "ติ๊กเลือก"
 *
 * ติ๊ก = ย้ายเครื่องข้ามเกจจริง ไม่ใช่แค่คำขอ:
 *   - เลขเครื่องว่างบน Gantt เปลี่ยนทันที (backend บวก/ลบทับตอนอ่าน)
 *   - งานที่ติดถูกดึงมาผลิตให้เอง (KnitPlan ใช้เครื่องคำนวณตัวเดียวกับตอนลากบล็อก)
 *   - รอบวางแผนถัดไป (ทั้ง Planning และ Replan) เห็นเครื่องอยู่เกจใหม่ตั้งแต่ต้นรอบ
 * ทุกครั้งที่ติ๊ก/ยกเลิกถูกบันทึกลง log (ดูได้ที่หน้าหลัก — admin เท่านั้น)
 */
export default function CylinderAdvisor({ sheet = '', planFile = '', onApplied }) {
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState(null)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [picked, setPicked] = useState({})     // key → จำนวนเครื่องที่ติ๊ก
  const [approvals, setApprovals] = useState([])
  const [saving, setSaving] = useState(false)
  // ผลวิเคราะห์ครั้งล่าสุดถูกเก็บที่ server → ปิดหน้าแล้วเปิดใหม่ยังเห็นของเดิม
  const [stamp, setStamp] = useState(null)     // {at, by, stale, reason}
  const [moves, setMoves] = useState([])       // แผนเก่า → แผนใหม่ (งานที่ย้ายหลังติ๊ก)
  const [compares, setCompares] = useState([]) // ตารางยอดรายสัปดาห์ ก่อน/หลัง ของแต่ละครั้งที่ติ๊ก
  const autoRef = useRef('')                   // ชีทที่สั่งวิเคราะห์ใหม่อัตโนมัติไปแล้ว (กันยิงซ้ำ)

  // อีกคนติ๊ก/ยกเลิกการเปลี่ยนสูบ → รายการที่ติ๊กไว้กับ Cylinder สำรองที่เหลือเปลี่ยนทันที
  // (ของที่เหลือเป็นเพดานร่วมกันทั้งโรงงาน — ถ้าเห็นคนละเลข จะติ๊กเกินของที่มี)
  useEffect(() => live.subscribe(live.RES.cylinder, () => { loadApprovals() }), [])

  async function loadApprovals() {
    try {
      const r = await api.cylApprovedGet()
      setApprovals(r.approvals || [])
    } catch { /* ไม่มีสิทธิ์/ไฟล์ยังไม่มี = ถือว่ายังไม่เคยติ๊ก */ }
  }

  async function loadMoves() {
    try {
      const r = await api.cylMovesGet(60)
      setMoves(r.moves || [])
      setCompares(r.compares || [])
    } catch { /* ยังไม่เคยมีการย้ายงาน = ไม่มีตารางให้แสดง */ }
  }

  // เปิดหน้ามา → เอาผลวิเคราะห์ล่าสุดที่ server เก็บไว้มาแสดงทันที (ไม่เรียก LLM)
  // ถ้าผลนั้นวิเคราะห์ไว้ก่อนที่แผนหรือการติ๊กจะเปลี่ยน → สั่งวิเคราะห์ใหม่ให้เองเลย
  useEffect(() => {
    let alive = true
    loadMoves()
    ;(async () => {
      try {
        const r = await api.cylinderAdviseLast(sheet)
        if (!alive) return
        if (r.cached && r.data) {
          setData(r.data)
          setStamp({ at: r.at || '', by: r.by || '', stale: !!r.stale, reason: r.stale_reason || '' })
          if (r.data.approvals) setApprovals(r.data.approvals)
          if (r.stale && autoRef.current !== sheet) {
            autoRef.current = sheet
            analyze(true)
          }
          return
        }
      } catch { /* อ่านผลเก่าไม่ได้ = เริ่มจากจอเปล่าเหมือนเดิม */ }
      if (alive) loadApprovals()     // ยังไม่เคยวิเคราะห์ — อย่างน้อยต้องเห็นรายการที่ติ๊กไว้แล้ว
    })()
    return () => { alive = false }
  }, [sheet])

  // เวลา/ผู้วิเคราะห์มาจาก server ที่เดียว (ไม่ประกอบเองฝั่ง browser ให้เพี้ยนกัน)
  async function refreshStamp() {
    try {
      const r = await api.cylinderAdviseLast(sheet)
      if (r.cached) setStamp({ at: r.at || '', by: r.by || '', stale: !!r.stale, reason: r.stale_reason || '' })
    } catch { setStamp(null) }
  }

  async function analyze(auto = false) {
    setLoading(true)
    setErr(''); if (!auto) setMsg('')
    try {
      const res = await api.cylinderAdvise(sheet)
      setData(res)
      setPicked({})
      if (res.approvals) setApprovals(res.approvals)
      await refreshStamp()
    } catch (e) {
      setErr((auto ? 'อัปเดตผลวิเคราะห์ตามแผนใหม่ไม่สำเร็จ: ' : 'วิเคราะห์ไม่ได้: ') + e.message)
    } finally {
      setLoading(false)
    }
  }

  const cands = data?.candidates || []
  // เพดานที่ผู้ใช้ตั้งไว้ที่หน้า "ตั้งค่าระบบ" (backend ส่งมาใน limits) — ทุกเพดาน/คำอธิบาย
  // บนหน้านี้ต้องใช้เลขชุดนี้ ห้ามเขียนเลขตรึงไว้ ไม่งั้นเปลี่ยนค่าแล้วจอบอกเลขที่ระบบไม่ได้ใช้
  // ต้องประกาศก่อน limitOf() ข้างล่างที่หยิบไปใช้เป็นค่าสำรอง
  const limits = data?.limits || {}
  const cylPerWeek = limits.cylinder_per_week ?? 2
  const jobCap = limits.job_cap || {}
  const frzWeeks = limits.freeze_weeks ?? 2
  // "PHET DOUBLE 33 · SINGLE 44 · OM 13" — ประกอบจากค่าจริง ; ไม่มีค่า = ไม่โชว์วงเล็บนี้
  const jobCapText = [
    ['PHET DOUBLE', jobCap.PHET_DOUBLE], ['SINGLE', jobCap.PHET_SINGLE], ['OM', jobCap.OM],
  ].filter(([, v]) => v != null).map(([k, v]) => `${k} ${v}`).join(' · ')
  const pickedList = useMemo(
    () => cands.filter(c => (picked[keyOf(c)] || 0) > 0),
    [cands, picked])
  const pickedMc = pickedList.reduce((s, c) => s + (picked[keyOf(c)] || 0), 0)

  // เครื่องที่ "แถวอื่นบนจอนี้จองไว้แล้ว" — ทุกแถวที่เข้าเกจเดียวกันแย่ง Cylinder สำรองก้อนเดียวกัน
  // และแถวที่ดึงเครื่องจากเกจต้นทางเดียวกันในสัปดาห์เดียวกันก็แย่งเครื่องว่างชุดเดียวกัน
  // ไม่หักให้เห็นสด ๆ ตรงนี้ = ติ๊ก 8 แถว × 2 เครื่อง ทั้งที่มีสำรองแค่ 2 (backend จะปฏิเสธทั้งชุด)
  const booked = useMemo(() => {
    const tgt = {}, src = {}, quota = {}, job = {}
    for (const c of cands) {
      const n = picked[keyOf(c)] || 0
      if (n <= 0) continue
      tgt[tgtKeyOf(c)] = (tgt[tgtKeyOf(c)] || 0) + n
      if (c.src_gauge) src[srcKeyOf(c)] = (src[srcKeyOf(c)] || 0) + n
      if (c.quota_group) quota[quotaKeyOf(c)] = (quota[quotaKeyOf(c)] || 0) + n
      if (c.job_type) job[jobKeyOf(c)] = (job[jobKeyOf(c)] || 0) + n
    }
    return { tgt, src, quota, job }
  }, [cands, picked])

  // เพดานของแถวนี้ ณ ขณะนี้ (หักสิ่งที่แถวอื่นจองไปแล้ว แต่ไม่หักของตัวเอง)
  // 3 เพดานคนละธรรมชาติ: สำรอง = ของถาวรทั้งแผน · เครื่องต้นทาง = รายสัปดาห์ · โควตา = แรงงานรายสัปดาห์
  function limitOf(c) {
    const mine = picked[keyOf(c)] || 0
    const otherTgt = (booked.tgt[tgtKeyOf(c)] || 0) - mine
    const otherSrc = (booked.src[srcKeyOf(c)] || 0) - mine
    const otherQuota = c.quota_group ? (booked.quota[quotaKeyOf(c)] || 0) - mine : 0
    const otherJob = c.job_type ? (booked.job[jobKeyOf(c)] || 0) - mine : 0
    const spareLeft = Math.max(0, (c.spare_left || 0) - otherTgt)
    const srcLeft = Math.max(0, (c.sources?.[0]?.free || 0) - otherSrc)
    const quotaLeft = Math.max(0, (c.quota_left ?? c.quota_limit ?? cylPerWeek) - otherQuota)
    // job_left = null แปลว่าอ่านโควตา job ไม่ได้ → ไม่จำกัด (ไม่เดาแทนข้อมูลที่ไม่มี)
    const jobLeft = c.job_left == null ? null : Math.max(0, c.job_left - otherJob)
    const caps = [c.machines_needed || 1, spareLeft, srcLeft, quotaLeft]
    if (jobLeft != null) caps.push(jobLeft)
    return {
      spareLeft, srcLeft, quotaLeft, jobLeft, otherTgt, otherSrc, otherQuota, otherJob,
      max: c.feasible ? Math.max(0, Math.min(...caps)) : 0,
    }
  }

  function toggle(c) {
    const k = keyOf(c)
    const lim = limitOf(c)
    setPicked(p => ({ ...p, [k]: p[k] ? 0 : Math.min(c.machines_max || 1, lim.max) }))
  }

  function setMc(c, v) {
    const k = keyOf(c)
    const n = Math.max(0, Math.min(Number(v) || 0, limitOf(c).max))
    setPicked(p => ({ ...p, [k]: n }))
  }

  // ติ๊กเสร็จ → บันทึกการอนุมัติ แล้วให้หน้าแผนเอาไปย้ายงาน + โหลดเครื่องว่างใหม่
  async function applyPicked() {
    if (pickedList.length === 0) return
    setSaving(true); setErr(''); setMsg('')
    try {
      const entries = pickedList.map(c => ({
        factory: c.factory, cat: c.cat,
        src_gauge: c.src_gauge, tgt_gauge: c.gauge,
        ready_week: c.need_week, set_week: c.set_week,
        machines: picked[keyOf(c)],
        items: [c.item_code],
      }))
      const r = await api.cylApprove(entries, sheet, planFile || data?.plan_name || '')
      setApprovals(r.approvals || [])
      const ids = (r.approvals || []).map(a => a.id)
      let moved = 0
      if (onApplied) {
        // หน้าแผนย้ายงานให้ตามเครื่องที่เพิ่งได้มา แล้วคืนจำนวนก้อนที่ย้าย
        moved = (await onApplied(pickedList.map(c => ({ ...c, machines: picked[keyOf(c)] })), ids)) || 0
      }
      setPicked({})
      setMsg(`✅ บันทึกแล้ว ${entries.length} รายการ (${pickedMc} เครื่อง)`
        + (moved ? ` · ย้ายงานให้ ${moved} ก้อน — ตรวจแล้วกด "บันทึกแผน"` : '')
        + ' · เลขเครื่องว่างอัปเดตแล้ว')
      await loadMoves()          // ตาราง "แผนเก่า → แผนใหม่" ต้องขึ้นก้อนที่เพิ่งย้ายทันที
      await analyzeQuiet()
    } catch (e) {
      setErr('บันทึกไม่สำเร็จ: ' + e.message)
    } finally {
      setSaving(false)
    }
  }

  // วิเคราะห์ซ้ำเงียบ ๆ หลังติ๊ก (รายการที่ทำไปแล้วจะหายไปเองเพราะเครื่องพอแล้ว)
  // ต้องทำทุกครั้งที่ติ๊ก/ยกเลิก — ไม่งั้นผลที่ server เก็บไว้จะเป็นของก่อนติ๊ก แล้วรอบหน้าที่เปิด
  // หน้ามาระบบจะเห็นว่า stale แล้ววิเคราะห์ใหม่ซ้ำอีกครั้งโดยไม่จำเป็น
  async function analyzeQuiet() {
    try {
      const res = await api.cylinderAdvise(sheet)
      setData(res)
      if (res.approvals) setApprovals(res.approvals)
      await refreshStamp()
    } catch { /* เงียบ — ข้อความหลักแสดงผลสำเร็จไปแล้ว */ }
  }

  async function revoke(a) {
    if (!window.confirm(
      `ยกเลิกการเปลี่ยน Cylinder นี้?\n\n${a.factory} ${a.cat} G${a.src_gauge}→G${a.tgt_gauge}`
      + ` ${a.machines} เครื่อง (พร้อมใช้ W${a.ready_week})\n\n`
      + 'เครื่องจะกลับไปอยู่ Guage เดิมทันที ทั้งบนหน้าแผนและรอบวางแผนถัดไป')) return
    try {
      const r = await api.cylRevoke(a.id)
      setApprovals(r.approvals || [])
      setMsg('↩ ยกเลิกแล้ว — เครื่องกลับไป Guage เดิม')
      if (onApplied) await onApplied([], [])   // ให้หน้าแผนโหลดเครื่องว่างใหม่
      await analyzeQuiet()
    } catch (e) {
      setErr('ยกเลิกไม่สำเร็จ: ' + e.message)
    }
  }

  const t = data?.timing || {}
  const sheetLabel = (data?.sheet || sheet || 'PLAN').toUpperCase()
  // สำรองคงเหลือรายเกจ — ทีมดูยอดจากหน้านี้แทนการเปิด MasterMC บน OneDrive
  const spares = data?.spare_summary || []
  const spareTotal = spares.reduce((s, x) => s + (x.total || 0), 0)
  const spareUsed = spares.reduce((s, x) => s + (x.used || 0), 0)
  const spareLeft = spareTotal - spareUsed

  return (
    <div className="card">
      <div className="editbar">
        <h2>🔩 Change Cylinder — ติ๊กเลือกแล้วเป็นแผนนั้นเลย</h2>
        <button onClick={() => analyze()} disabled={loading}>
          {loading ? 'กำลังวิเคราะห์…' : (data ? 'วิเคราะห์ใหม่' : 'วิเคราะห์ Change Cylinder')}
        </button>
      </div>

      <div className="hint">
        วิเคราะห์จากชีท <b>{sheetLabel}</b> ที่เปิดอยู่ — งานที่ส่งไม่ทัน/ผลิตไม่ครบเพราะเครื่องใน Guage นั้นเต็ม
        จะปลดล็อกได้ด้วยการย้ายเครื่องว่างจาก Guage อื่นมาเปลี่ยน Cylinder (ต้องอยู่ใน MC_CAT และโรงงานเดียวกัน)
        <br />
        <b>ติ๊กแล้วมีผลทันที</b>: เครื่องย้าย Guage → เลขว่างเปลี่ยน → ระบบดึงงานที่ติดมาผลิตให้ →
        รอบวางแผนถัดไปก็ใช้ค่านี้ ไม่ต้องติ๊กซ้ำ (ยกเลิกได้ทุกเมื่อ ทุกการกดถูกบันทึกไว้)
        <br />
        <b>Cylinder สำรอง 1 อัน = ย้ายได้ 1 เครื่อง และอยู่กับเครื่องนั้นถาวร</b> —
        ทุกแถวที่เปลี่ยนเข้า Guage เดียวกัน (โรงงาน + MC_CAT เดียวกัน) แย่งสำรองก้อนเดียวกัน
        ใช้หมดที่สัปดาห์หนึ่งแล้ว สัปดาห์ถัด ๆ ไปก็ไม่เหลือ จนกว่าจะยกเลิกรายการเดิมหรือซื้อเพิ่ม
        ติ๊กแถวหนึ่งแล้วยอดของแถวอื่นจะลดให้เห็นทันที
        <br />
        <b>ลงมือเปลี่ยนได้ {cylPerWeek} เครื่อง/สัปดาห์/กลุ่ม</b> (PHET_DOUBLE · PHET_SINGLE · OMNOI) —
        นับที่สัปดาห์ที่สั่งเปลี่ยน โควตาเต็มแล้วต้องเลื่อนไปสัปดาห์อื่น (สัปดาห์ถัดไปเต็มโควตาใหม่
        ถ้ายังมี Cylinder สำรองเหลือ)
        <br />
        <b>เครื่องที่เปลี่ยนแล้วต้อง "Setup ใหม่" = กิน job setup</b> ของสัปดาห์ที่เครื่องพร้อมใช้
        {jobCapText && ` (${jobCapText} job/สัปดาห์ — ยอดเดียวกับแถบบน Gantt)`}
        สัปดาห์ไหน job เต็ม ระบบจะเลื่อนไปเสนอสัปดาห์ถัดไปที่ยัง Setup ได้ให้เอง
        {t.earliest_ready != null && (
          <> <br />เร็วสุดที่ทำได้: สั่งเปลี่ยน <b>W{t.earliest_set}</b> → เครื่องพร้อมใช้ <b>W{t.earliest_ready}</b>
            {' '}({frzWeeks} สัปดาห์แรกนับจากสัปดาห์นี้ถูกล็อกไว้แล้ว)</>
        )}
      </div>

      {err && <div className="msg">{err}</div>}
      {msg && <div className="msg">{msg}</div>}

      {/* ผลวิเคราะห์ถูกเก็บที่ server — บอกให้ชัดว่าตัวเลขที่เห็นเป็นของ "เมื่อไร"
          และเก่าไปแล้วหรือยัง (แผนถูกรันใหม่/มีคนติ๊กเพิ่ม = ตัวเลขเดิมใช้ไม่ได้) */}
      {stamp && (
        <div className={'cyl-stamp' + (stamp.stale ? ' stale' : '')}>
          {stamp.stale
            ? `⚠ ผลที่แสดงเป็นของการวิเคราะห์เมื่อ ${stamp.at}${stamp.by ? ` โดย ${stamp.by}` : ''}`
              + ` — ${stamp.reason}`
              + (loading ? ' · กำลังวิเคราะห์ใหม่ให้อัตโนมัติ…' : ' · กดปุ่ม "วิเคราะห์ใหม่" เพื่ออัปเดต')
            : `🕒 ผลวิเคราะห์เมื่อ ${stamp.at}${stamp.by ? ` โดย ${stamp.by}` : ''}`
              + ' — ตรงกับแผนและรายการที่ติ๊กไว้ปัจจุบัน (ปิดหน้าแล้วเปิดใหม่ก็ยังอยู่)'}
        </div>
      )}

      {approvals.length > 0 && (
        <div className="cyl-approved">
          <b>🔄 เปลี่ยน Cylinder ไว้แล้ว {approvals.length} รายการ</b> (มีผลกับแผนทุกรอบ)
          <table className="grid">
            <thead>
              <tr>
                <th>โรงงาน / MC_CAT</th><th>เปลี่ยน</th><th>เครื่อง</th>
                <th>สั่ง W</th><th>พร้อมใช้ W</th><th>ใครติ๊ก / เมื่อไร</th><th></th>
              </tr>
            </thead>
            <tbody>
              {approvals.map(a => (
                <tr key={a.id}>
                  <td>{a.factory} / {a.cat}</td>
                  <td>G{a.src_gauge} → <b>G{a.tgt_gauge}</b></td>
                  <td className="num">{a.machines}</td>
                  <td className="num">{a.set_week}</td>
                  <td className="num">{a.ready_week}</td>
                  <td className="reason">{a.by || '-'}<br />{a.at || ''}</td>
                  <td><button className="linkbtn" onClick={() => revoke(a)}>ยกเลิก</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {compares.length > 0 && (
        // ตารางกริดรายสัปดาห์แบบเดียวกับตารางเทียบในโหมด Replan — 1 กล่อง = การติ๊ก 1 ครั้ง
        // "แผนเดิม" ที่นี่คือแผนก่อนกดติ๊ก (ไม่ใช่ booking) เพราะเทียบผลของการติ๊กครั้งนั้น
        <details className="cyl-moves" open>
          <summary>
            📊 จำลองแผนเดิม → แผนใหม่ หลัง Change Cylinder ({compares.length} ครั้งล่าสุด)
          </summary>
          <div className="hint">
            ยอด กก. ต่อสัปดาห์ ของทุก item ที่แผนเปลี่ยนจากการติ๊กครั้งนั้น
            (รวม item อื่นที่ถูกขยับเพราะเครื่อง/setup ถูกคำนวณใหม่)
            {' · '}<span className="cell-out-demo">เดิม</span> = ก่อนติ๊ก
            {' · '}<span className="cell-in-demo">ใหม่</span> = หลังติ๊ก
          </div>
          {compares.map((cp, ci2) => (
            <div key={cp.at + ci2} className="cyl-cmp-block">
              <div className="hint" style={{ padding: '4px 0' }}>
                🕒 {cp.at} · {cp.by || '-'} · ชีท {cp.sheet || '-'} · ย้าย {cp.moves_n} ก้อน
                {cp.plan_file ? ` · ${cp.plan_file}` : ''}
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table className="grid chg-table">
                  <thead>
                    <tr>
                      <th>ITEM</th><th>MC_GROUP</th><th>แผน</th>
                      {cp.weeks.map(w => <th key={w} className="wk-col">W{w}</th>)}
                      <th className="wk-col">รวม</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cp.items.map((g, gi) => {
                      const sum = bag => Math.round(cp.weeks
                        .reduce((s, w) => s + (bag[String(w)] || 0), 0) * 10) / 10
                      return (
                        <React.Fragment key={g.item + gi}>
                          <tr className="chg-before">
                            <td className="rocell" rowSpan={2}><b>{g.item}</b></td>
                            <td className="rocell" rowSpan={2}>{g.mc_group || '-'}</td>
                            <td className="rocell">เดิม (ก่อนติ๊ก)</td>
                            {cp.weeks.map(w => {
                              const b = g.before[String(w)] || 0, a = g.after[String(w)] || 0
                              return <td key={w} className={'wk-col' + (b && Math.abs(b - a) > 0.01 ? ' cell-out' : '')}>
                                {b ? fmtQty(b) : ''}
                              </td>
                            })}
                            <td className="wk-col">{fmtQty(sum(g.before))}</td>
                          </tr>
                          <tr className="chg-after">
                            <td className="rocell"><b>หลัง Change Cylinder</b></td>
                            {cp.weeks.map(w => {
                              const b = g.before[String(w)] || 0, a = g.after[String(w)] || 0
                              return <td key={w} className={'wk-col' + (a && Math.abs(a - b) > 0.01 ? ' cell-in' : '')}>
                                {a ? fmtQty(a) : ''}
                              </td>
                            })}
                            <td className="wk-col"><b>{fmtQty(sum(g.after))}</b></td>
                          </tr>
                        </React.Fragment>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </details>
      )}

      {moves.length > 0 && (
        // แผนเก่า → แผนใหม่: ก้อนงานที่ระบบดึงกลับมาผลิตเร็วขึ้นให้ หลังจากติ๊กเปลี่ยน Cylinder
        // (มาจาก log ฝั่ง server → ปิดหน้า/เปลี่ยนเครื่องแล้วก็ยังย้อนดูได้)
        <details className="cyl-moves" open>
          <summary>📋 แผนเก่า → แผนใหม่ หลังเปลี่ยน Cylinder ({moves.length} ก้อนล่าสุด)</summary>
          <div className="hint">
            งานที่ระบบดึงมาผลิตเร็วขึ้นให้เพราะได้เครื่องจากการเปลี่ยน Cylinder
            <br />
            ⚠️ การย้ายงานจะลงไฟล์แผนจริงต่อเมื่อกด <b>"บันทึกแผน"</b> ที่หน้าแผนแล้วเท่านั้น —
            ส่วนเครื่องที่เปลี่ยน Guage ถูกบันทึกทันทีตั้งแต่ตอนติ๊ก
          </div>
          <table className="grid">
            <thead>
              <tr>
                <th>Item</th><th>SC/SO</th><th>แผนเดิม</th><th>แผนใหม่</th><th>ผล</th>
                <th>กก.</th><th>เครื่อง</th><th>ชีท</th><th>ใครติ๊ก / เมื่อไร</th>
              </tr>
            </thead>
            <tbody>
              {moves.map((m, i) => (
                <tr key={`${m.at}|${m.item}|${m.so}|${i}`}>
                  <td>{m.item || '-'}</td>
                  <td>{m.so || '-'}</td>
                  <td className="num">{m.from_week != null ? `W${m.from_week}` : '-'}</td>
                  <td className="num to-week">{m.to_week != null ? <b>W{m.to_week}</b> : '-'}</td>
                  <td className="num">
                    {m.gain_weeks > 0
                      ? <b className="ok">เร็วขึ้น {m.gain_weeks} สัปดาห์</b>
                      : (m.gain_weeks < 0 ? <b className="late">ช้าลง {-m.gain_weeks} สัปดาห์</b> : '-')}
                  </td>
                  <td className="num">{m.qty ? fmtQty(m.qty) : '-'}</td>
                  <td className="num">{m.mc || '-'}</td>
                  <td>{m.sheet || '-'}</td>
                  <td className="reason">{m.by || '-'}<br />{m.at || ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}

      {spares.length > 0 && (
        // เปิดค้างไว้เสมอ — ยอดสำรองคือตัวเลขที่ต้องดูก่อนตัดสินใจติ๊ก ไม่ควรต้องกดเปิดหา
        <details className="cyl-spare" open>
          <summary>
            🧰 Cylinder สำรอง: มีทั้งหมด <b>{spareTotal}</b> อัน
            {' · '}ติ๊กใช้ไปแล้ว <b>{spareUsed}</b>
            {' · '}เหลือ <b className={spareLeft > 0 ? 'ok' : 'late'}>{spareLeft}</b>
            {' '}({spares.length} Guage)
          </summary>
          <div className="hint">
            <b>"เหลือ" = ยอดจริงในไฟล์ MasterMC บน server</b> — ติ๊กเปลี่ยน Cylinder แล้วระบบ
            หักยอดลงไฟล์ให้ทันที (ยกเลิกแล้วคืนยอดกลับเข้าไฟล์เอง) เปิดหน้า Masters → MasterMC →
            ชีท Spare part จะเห็นเลขเดียวกันนี้ และแก้เองได้ถ้าซื้อ Cylinder เพิ่ม
            {' · '}"มีทั้งหมด" = เหลือ + ที่ติ๊กใช้ไปแล้ว
            <br />
            ⚠️ นับเฉพาะที่ติ๊กผ่านหน้านี้ — ถ้าหน้างานถอด Cylinder ไปเปลี่ยนเองโดยไม่ได้ติ๊ก
            ยอด "เหลือ" จะสูงกว่าของจริง (แก้ยอดในไฟล์เองได้)
          </div>
          <table className="grid">
            <thead>
              <tr><th>โรงงาน</th><th>MC_CAT</th><th>Guage</th><th>มีทั้งหมด</th><th>ติ๊กใช้ไป</th><th>เหลือ</th></tr>
            </thead>
            <tbody>
              {spares.map((s, i) => (
                <tr key={i} className={s.left <= 0 ? 'cyl-blocked' : ''}>
                  <td>{s.factory}</td>
                  <td>{s.cat}</td>
                  <td className="num">{s.gauge}</td>
                  <td className="num">{s.total}</td>
                  <td className="num">{s.used > 0 ? s.used : '-'}</td>
                  <td className="num">{s.left > 0 ? <b>{s.left}</b> : <b className="late">0</b>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}

      {data && (
        <>
          {data.summary && (
            <div className="ai-summary"><b>สรุปจาก AI:</b> {data.summary}</div>
          )}
          {data.note && <div className="msg">{data.note}</div>}
          {!data.ai && cands.length > 0 && !data.note && (
            <div className="hint">* แสดงอันดับจากการคำนวณ (ยังไม่ได้ใช้ AI)</div>
          )}
          <div className="hint">
            ไฟล์แผน: {data.plan_name || '-'} · ชีท {sheetLabel} · พบงานที่ติดเครื่อง {data.total_found ?? cands.length} รายการ
          </div>

          {cands.length === 0 ? (
            <div className="hint">ไม่มีงานที่ต้อง Change Cylinder — แผนนี้ไม่มีงานสายหรือค้างผลิตจากเครื่องไม่พอ</div>
          ) : (
            <>
              <table className="grid">
                <thead>
                  <tr>
                    <th>เลือก</th>
                    <th>อันดับ</th>
                    <th>Item</th>
                    <th>CAT / Guage</th>
                    <th>ต้องทำอะไร</th>
                    <th>เครื่อง</th>
                    <th>Cylinder เหลือ</th>
                    <th title={`เปลี่ยน Cylinder ได้ ${cylPerWeek} เครื่อง/สัปดาห์/กลุ่ม — นับที่สัปดาห์ที่ลงมือเปลี่ยน`}>
                      โควตาสัปดาห์
                    </th>
                    <th title={'Setup ใหม่ 1 เครื่อง = 1 job ของสัปดาห์ที่เครื่องพร้อมใช้'
                      + (jobCapText ? ` (${jobCapText})` : '')}>
                      Job setup เหลือ
                    </th>
                    <th>สาย (สัปดาห์)</th>
                    <th>ค้าง (กก.)</th>
                    <th>ทำไมลำดับนี้</th>
                  </tr>
                </thead>
                <tbody>
                  {cands.map((c, i) => {
                    const k = keyOf(c)
                    const n = picked[k] || 0
                    const lim = limitOf(c)
                    // ทำได้อยู่ แต่ของถูกแถวอื่นจองไปหมดแล้ว = ติ๊กเพิ่มไม่ได้จนกว่าจะลดแถวอื่นลง
                    const takenOut = c.feasible && lim.max <= 0 && n === 0
                    const takenWhy = (lim.jobLeft != null && lim.jobLeft <= 0)
                      ? `job setup ของ ${c.job_type} ที่ W${c.need_week} เต็มแล้ว`
                        + ` (${c.job_used}/${c.job_cap} job) — Setup ใหม่ในสัปดาห์นั้นไม่ได้`
                      : lim.quotaLeft <= 0
                      ? `โควตาเปลี่ยน Cylinder ของ ${c.quota_group} ที่ W${c.set_week} เต็มแล้ว`
                        + ` (${c.quota_limit ?? cylPerWeek} เครื่อง/สัปดาห์) — ต้องเลื่อนไปสั่งเปลี่ยนสัปดาห์อื่น`
                      : (lim.spareLeft <= 0
                        ? `Cylinder สำรอง Guage ${c.gauge} ถูกแถวอื่นที่ติ๊กไว้จองไปหมดแล้ว (${lim.otherTgt} เครื่อง)`
                        : `เครื่องว่าง Guage ${c.src_gauge} ที่ W${c.need_week} ถูกแถวอื่นจองไปหมดแล้ว (${lim.otherSrc} เครื่อง)`)
                    return (
                      <tr key={k + i}
                        className={c.feasible ? (n > 0 ? 'cyl-picked' : (takenOut ? 'cyl-taken' : '')) : 'cyl-blocked'}>
                        <td>
                          <input type="checkbox" checked={n > 0} disabled={!c.feasible || takenOut}
                            title={!c.feasible ? c.blocker
                              : (takenOut ? takenWhy : `ติ๊กได้มากสุด ${lim.max} เครื่อง ณ ตอนนี้`)}
                            onChange={() => toggle(c)} />
                        </td>
                        <td className="rank">{c.rank ?? i + 1}</td>
                        <td>
                          {c.item_code}
                          {c.feasible
                            ? <span className="tag-ok" title="เปลี่ยนได้เลย">ทำได้</span>
                            : <span className="tag-block" title={c.blocker}>ติด</span>}
                          {/* ด้ายเข้าครบสัปดาห์ไหน — เปลี่ยนเครื่องก่อนด้ายมาก็ทอไม่ได้
                              yarn_shifted = ระบบเลื่อนสัปดาห์เปลี่ยนให้รอด้ายแล้ว */}
                          {c.yarn_week != null && (
                            <div className="hint" style={{ fontSize: '0.85em' }}
                              title={c.yarn_shifted
                                ? 'ด้ายเข้าช้ากว่าสัปดาห์ที่เครื่องตัน — เลื่อนไปเปลี่ยนรอไว้ให้พร้อมพอดีตอนด้ายมา'
                                : 'ด้ายเข้าครบก่อนสัปดาห์ที่ต้องใช้เครื่องแล้ว'}>
                              🧵 ด้ายครบ W{c.yarn_week}{c.yarn_shifted ? ' → เปลี่ยนรอไว้' : ''}
                            </div>
                          )}
                        </td>
                        <td>{c.cat || '-'} / {c.gauge || '-'}</td>
                        <td className="cyl-action" title={`Guage ต้นทางที่ว่าง: ${fmtSources(c.sources)}`}>
                          {c.action}
                        </td>
                        <td className="num">
                          {c.feasible ? (
                            <input type="number" className="cyl-mc" min="0" max={lim.max}
                              value={n} disabled={takenOut}
                              onChange={e => setMc(c, e.target.value)}
                              title={takenOut ? takenWhy : `ติ๊กได้มากสุด ${lim.max} เครื่อง ณ ตอนนี้`
                                + ` (งานต้องใช้ ${c.machines_needed} · Cylinder เหลือ ${lim.spareLeft}`
                                + ` · เครื่องว่าง Guage ${c.src_gauge} ${lim.srcLeft}`
                                + ` · โควตา W${c.set_week} เหลือ ${lim.quotaLeft})`} />
                          ) : '-'}
                        </td>
                        <td className="num">
                          {/* ยอดที่เหลือ "หลังหักแถวอื่นที่ติ๊กค้างอยู่" — เลขนี้เป็นก้อนเดียวกันทั้งเกจ
                              ไม่ใช่โควตาของแถวนี้คนเดียว */}
                          {lim.spareLeft > 0 ? lim.spareLeft : <b className="late">0</b>}
                          {lim.otherTgt > 0 && (
                            <span className="hint" title="ถูกแถวอื่นที่ติ๊กไว้จองไปแล้ว (ยังไม่ได้บันทึก)">
                              {' '}(แถวอื่นจอง {lim.otherTgt})
                            </span>
                          )}
                          {c.approved_machines > 0 && (
                            <span className="hint" title="ติ๊กบันทึกไปแล้วสำหรับ Guage นี้"> (ใช้ไป {c.approved_machines})</span>
                          )}
                        </td>
                        <td className="num">
                          {/* แรงงานที่เปลี่ยน Cylinder ได้จริงในสัปดาห์นั้น — สัปดาห์หน้าเต็มใหม่
                              (ต่างจาก Cylinder สำรองที่ใช้แล้วหมดไปทั้งแผน) */}
                          {c.quota_group ? (
                            <span title={`${c.quota_group} · W${c.set_week} · ใช้ได้ ${c.quota_limit ?? cylPerWeek} เครื่อง/สัปดาห์`
                              + (c.quota_used > 0 ? ` · ติ๊กบันทึกไปแล้ว ${c.quota_used}` : '')
                              + (lim.otherQuota > 0 ? ` · แถวอื่นบนจอจอง ${lim.otherQuota}` : '')}>
                              W{c.set_week}:{' '}
                              {lim.quotaLeft > 0
                                ? <b>{lim.quotaLeft}</b>
                                : <b className="late">เต็ม</b>}
                            </span>
                          ) : '-'}
                        </td>
                        <td className="num">
                          {/* ตั้งเครื่องใหม่กิน job ของสัปดาห์ที่เครื่องพร้อมใช้ — คนละ pool กับ
                              โควตาเปลี่ยน Cylinder และแชร์กับงานทั้งโรงในสัปดาห์นั้น */}
                          {lim.jobLeft == null ? '-' : (
                            <span title={`${c.job_type} · W${c.need_week} · ใช้ไป ${c.job_used}/${c.job_cap} job`
                              + (lim.otherJob > 0 ? ` · แถวอื่นบนจอจอง ${lim.otherJob}` : '')
                              + (c.job_shifted ? ' · เลื่อนมาสัปดาห์นี้เพราะสัปดาห์เดิม job เต็ม' : '')}>
                              W{c.need_week}:{' '}
                              {lim.jobLeft > 0 ? <b>{lim.jobLeft}</b> : <b className="late">เต็ม</b>}
                              {c.job_shifted && <div className="hint" style={{ fontSize: '0.85em' }}>เลื่อนสัปดาห์ให้</div>}
                            </span>
                          )}
                        </td>
                        <td className="num">{c.late_weeks > 0 ? <b className="late">{c.late_weeks}</b> : '-'}</td>
                        <td className="num">{c.pending_qty > 0 ? fmtQty(c.pending_qty) : '-'}</td>
                        <td className="reason">{c.reason || '-'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>

              <div className="editbar cyl-apply">
                <span className="hint">
                  {pickedList.length > 0
                    ? `เลือกไว้ ${pickedList.length} รายการ รวม ${pickedMc} เครื่อง`
                    : 'ติ๊กรายการที่ต้องการเปลี่ยน Cylinder'}
                </span>
                <button className="primary" disabled={pickedList.length === 0 || saving}
                  onClick={applyPicked}>
                  {saving ? 'กำลังใช้…' : `✅ ใช้ตามที่ติ๊ก (${pickedList.length})`}
                </button>
              </div>
            </>
          )}
        </>
      )}

      {!data && !loading && (
        <div className="hint">กดปุ่ม "วิเคราะห์ Change Cylinder" เพื่อดูว่าควรเปลี่ยน Cylinder ให้งานไหนก่อน</div>
      )}
    </div>
  )
}

import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'

/* Dashboard › SC Lead Time — ใบสั่งเปิดมาแล้วเหลือเวลาถึงกำหนดส่งกี่สัปดาห์

   ห่าง (สัปดาห์) = RDD − สัปดาห์ที่เปิดใบ  โดย RDD = FG week ตรง ๆ ไม่หักอะไร
     ค่าติดลบ = เปิดใบมาหลังกำหนดส่งไปแล้ว = ต้องเร่งตั้งแต่วันแรกที่เห็นใบ
     คอลัมน์ "ถึงเป้าทอ" (r.gap_knit) = ระยะถึง FG หักเวลางานหลังทอ — ข้อมูลเสริมในตาราง
     เท่านั้น ไม่เข้า KPI/กราฟ/การแบ่งกลุ่ม (ทุกตัวเลขหลักของหน้านี้ยึด RDD อย่างเดียว)

   ฐานข้อมูล = ทะเบียน SC สะสม (data_plan/sc_registry.xlsx) ที่ View_SC เขียนต่อทุกครั้งที่รัน
   ไม่ใช่ไฟล์ view_sc ใบเดียว — view เก็บเฉพาะใบที่ยังค้างวางแผน ใบที่ confirm แล้วหลุดออกไป

   ผู้ใช้ปรับ "ช่วงแบ่งกลุ่ม" ได้เองบนหน้า (ช่องขอบล่างของแต่ละกลุ่ม) → KPI/กราฟ/ป้ายในตาราง
   คิดใหม่ฝั่ง client ทั้งหมดจากแถวดิบที่ backend ส่งมา ไม่ต้องวิ่งถาม server ใหม่ทุกครั้ง

   ธีม/เลย์เอาต์ใช้ชุดเดียวกับหน้า Machine Availability (.mcav-dash + ตัวแปร --d-*)
   คลาสเฉพาะหน้านี้ขึ้นต้น .scl- */

const THEME_KEY = 'mcav-theme'          // ใช้ค่าเดียวกับหน้า Dashboard อื่น — สลับหน้าหนึ่งแล้วตามกันหมด
const BUCKET_KEY = 'scl-buckets'        // ช่วงกลุ่มที่ผู้ใช้ตั้งไว้ (จำไว้ให้ในเครื่องนั้น)
// เกณฑ์เดียวพอ (user สั่ง 2 ก.ย. 2569): ต่ำกว่า 5 สัปดาห์ = เวลาไม่พอ · 5 สัปดาห์ขึ้นไป = พอวางแผนได้
// ใส่หลายค่าคั่นจุลภาคก็ยังแบ่งละเอียดได้เหมือนเดิม (เช่น "0,2,5") — ค่านี้แค่ค่าเริ่มต้น
const DEFAULT_BUCKETS = '5'

const fmt = (n) => Math.round(n || 0).toLocaleString()
const TH_MON = ['ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.',
  'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.']

/** '2026-08-11' → '11 ส.ค. 69' */
function dateTH(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || ''))
  if (!m) return '-'
  return `${+m[3]} ${TH_MON[+m[2] - 1]} ${(+m[1] + 543) % 100}`
}
const wkLabel = (w, y) => (w ? `W${w}${y ? '/' + String(y).slice(-2) : ''}` : '-')

function median(arr) {
  if (!arr.length) return 0
  const a = [...arr].sort((x, y) => x - y)
  const m = a.length >> 1
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2
}

/** "0,2,4,7" → กลุ่ม: <0 · 0–1 · 2–3 · 4–6 · ≥7   (ขอบล่างของแต่ละกลุ่ม เรียงจากน้อยไปมาก)
    ค่าผิดรูป/ว่าง → คืน null เพื่อให้หน้าเว็บเตือนแล้วใช้ค่าเดิมต่อ ไม่ใช่พังทั้งหน้า */
function parseBuckets(text) {
  const nums = String(text || '').split(/[,\s]+/).map(s => s.trim()).filter(Boolean).map(Number)
  if (!nums.length || nums.some(n => !Number.isFinite(n))) return null
  const cuts = [...new Set(nums)].sort((a, b) => a - b)
  const out = []
  // label = ป้ายสั้นใต้แท่งกราฟ · full = ประโยคเต็มสำหรับการ์ด KPI ("5 สัปดาห์ขึ้นไป")
  // กลุ่มก่อนขอบแรกมีเสมอ — ตั้งเกณฑ์เดียว (เช่น 5) จึงได้ "ต่ำกว่า 5" คู่กับ "5 ขึ้นไป"
  out.push({
    id: 'lt', label: `น้อยกว่า ${cuts[0]}`, full: `น้อยกว่า ${cuts[0]} สัปดาห์`,
    lo: -Infinity, hi: cuts[0] - 1, tone: 'bad',
  })
  cuts.forEach((c, i) => {
    const next = cuts[i + 1]
    if (next == null) {
      out.push({ id: `${c}+`, label: `${c} ขึ้นไป`, full: `${c} สัปดาห์ขึ้นไป`, lo: c, hi: Infinity, tone: 'ok' })
    } else if (next - c === 1) {
      out.push({ id: `${c}`, label: `${c}`, full: `${c} สัปดาห์`, lo: c, hi: c, tone: i === 0 ? 'warn' : 'mid' })
    } else {
      out.push({
        id: `${c}-${next - 1}`, label: `${c}–${next - 1}`, full: `${c}–${next - 1} สัปดาห์`,
        lo: c, hi: next - 1, tone: i === 0 ? 'warn' : 'mid',
      })
    }
  })
  return out
}
const bucketOf = (buckets, gap) => buckets.find(b => gap >= b.lo && gap <= b.hi) || null

/* ---------- tooltip ลอยตามเมาส์ (สไตล์ร่วมกับหน้า Machine Availability) ---------- */
function Tip({ tip }) {
  if (!tip) return null
  return (
    <div className="mcav-tip" style={{ left: tip.x, top: tip.y }}>
      <div className="mcav-tip-title">{tip.title}</div>
      {tip.lines.map((l, i) => <div key={i} className="mcav-tip-line"><span>{l}</span></div>)}
    </div>
  )
}
const tipPos = (e) => ({ x: Math.min(e.clientX + 14, window.innerWidth - 260), y: e.clientY + 16 })

/* ---------- กราฟแท่ง: ใบสั่งกี่ใบในแต่ละช่วงความกระชั้น ---------- */
function Histogram({ buckets, rows, sel, onSel, setTip }) {
  const counts = buckets.map(b => rows.filter(r => r.gap >= b.lo && r.gap <= b.hi).length)
  const max = Math.max(1, ...counts)
  return (
    <div className="scl-hist">
      {buckets.map((b, i) => {
        const n = counts[i]
        const pct = n ? Math.max(4, Math.round((n / max) * 100)) : 0
        const qty = rows.filter(r => r.gap >= b.lo && r.gap <= b.hi)
          .reduce((s, r) => s + (r.qty || 0), 0)
        return (
          <button key={b.id}
            className={'scl-bar t-' + b.tone + (sel === b.id ? ' on' : '')}
            onClick={() => onSel(sel === b.id ? null : b.id)}
            onMouseMove={e => setTip({
              ...tipPos(e), title: `ห่าง ${b.full}`,
              lines: [`${fmt(n)} ใบ (${rows.length ? Math.round(n / rows.length * 1000) / 10 : 0}%)`,
              `ยอดรวม ${fmt(qty)} kg`, 'คลิกเพื่อกรองเฉพาะกลุ่มนี้'],
            })}
            onMouseLeave={() => setTip(null)}>
            <span className="v">{fmt(n)}</span>
            <span className="bar" style={{ height: pct + '%' }} />
            <span className="lb">{b.label}</span>
          </button>
        )
      })}
    </div>
  )
}

/* ---------- แยกตามประเภทออร์เดอร์: ค่ากลางของแต่ละประเภท ---------- */
function ByType({ rows, buckets, setTip }) {
  const groups = useMemo(() => {
    const m = new Map()
    rows.forEach(r => {
      const k = r.order_type || '(ไม่ระบุ)'
      if (!m.has(k)) m.set(k, [])
      m.get(k).push(r)
    })
    return [...m.entries()]
      .map(([k, rs]) => ({
        key: k, n: rs.length, med: median(rs.map(r => r.gap)),
        neg: rs.filter(r => r.gap < 0).length,
        qty: rs.reduce((s, r) => s + (r.qty || 0), 0),
      }))
      .sort((a, b) => a.med - b.med)
  }, [rows])
  if (!groups.length) return null
  const worst = Math.min(...groups.map(g => g.med))
  const best = Math.max(...groups.map(g => g.med), 1)
  const span = Math.max(1, best - Math.min(0, worst))
  return (
    <div className="scl-bytype">
      <div className="scl-h">ค่ากลางของระยะห่าง แยกตามประเภทออร์เดอร์</div>
      {groups.map(g => {
        const b = bucketOf(buckets, g.med)
        return (
          <div key={g.key} className="scl-row"
            onMouseMove={e => setTip({
              ...tipPos(e), title: g.key,
              lines: [`${fmt(g.n)} ใบ · ยอด ${fmt(g.qty)} kg`,
              `ค่ากลาง ${g.med} สัปดาห์`,
              `เปิดหลังกำหนดส่งแล้ว ${fmt(g.neg)} รายการ`],
            })}
            onMouseLeave={() => setTip(null)}>
            <span className="k">{g.key}</span>
            <span className="track">
              <i className={'fill t-' + (b ? b.tone : 'mid')}
                style={{ width: Math.max(3, Math.round((Math.max(0, g.med) / span) * 100)) + '%' }} />
            </span>
            <b className={'med' + (g.med < 0 ? ' neg' : '')}>{g.med}</b>
            <span className="n">{fmt(g.n)} ใบ</span>
            {g.neg > 0 && <span className="neg-pill">เกินกำหนด {fmt(g.neg)}</span>}
          </div>
        )
      })}
    </div>
  )
}

export default function ScLeadTime() {
  const [d, setD] = useState(null)
  const [err, setErr] = useState('')
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [theme, setTheme] = useState(() => localStorage.getItem(THEME_KEY) || 'dark')
  const [tip, setTip] = useState(null)

  const [bucketText, setBucketText] = useState(() => localStorage.getItem(BUCKET_KEY) || DEFAULT_BUCKETS)
  const [buckets, setBuckets] = useState(() => parseBuckets(localStorage.getItem(BUCKET_KEY) || DEFAULT_BUCKETS))
  const [selBucket, setSelBucket] = useState(null)
  // ประเภทออร์เดอร์ = ติ๊กได้หลายอัน (ว่าง = ทุกประเภท) — user ต้องเทียบ 2 ประเภทพร้อมกันได้
  const [otypes, setOtypes] = useState([])
  const [q, setQ] = useState('')

  async function load() {
    setLoading(true); setErr('')
    try { setD(await api.scLeadtime()) } catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
    setLoading(false)
  }
  async function backfill() {
    setLoading(true); setMsg('')
    try {
      const r = await api.scLeadtimeBackfill()
      setMsg(r.ok
        ? `รวม snapshot เข้าทะเบียนแล้ว ${r.merged.length} ไฟล์ · ใบใหม่ ${fmt(r.added)} · ข้ามที่เคยรวมแล้ว ${r.skipped}`
        : (r.note || 'อัปเดตทะเบียนไม่สำเร็จ'))
      await load()
    } catch (e) { setMsg('อัปเดตทะเบียนไม่สำเร็จ: ' + e.message) }
    setLoading(false)
  }
  useEffect(() => { load() }, [])
  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])

  function applyBuckets(text) {
    setBucketText(text)
    const b = parseBuckets(text)
    if (b) { setBuckets(b); setSelBucket(null); localStorage.setItem(BUCKET_KEY, text) }
  }

  const all = d?.rows || []

  // ชุดที่ผ่าน "คำค้น" อย่างเดียว — ใช้เป็นฐานนับจำนวนบนชิปประเภท เพื่อให้ประเภทที่ยังไม่ติ๊ก
  // ยังเห็นจำนวนใบของตัวเอง (ถ้านับจาก base ตัวที่ไม่ได้ติ๊กจะเป็น 0 หมด เทียบอะไรไม่ได้เลย)
  const byQ = useMemo(() => {
    const kw = q.trim().toLowerCase()
    if (!kw) return all
    return all.filter(r => [r.sc, r.item, r.customer, r.sale, r.so, r.po, r.mc_group, r.color, r.fob]
      .join(' ').toLowerCase().includes(kw))
  }, [all, q])

  // รายการชิป: ประเภท + จำนวนใบ เรียงจากมากไปน้อย (ตัวที่มีผลต่อภาพรวมมากสุดอยู่ซ้าย)
  const types = useMemo(() => {
    const m = new Map()
    byQ.forEach(r => { if (r.order_type) m.set(r.order_type, (m.get(r.order_type) || 0) + 1) })
    return [...m.entries()].map(([key, n]) => ({ key, n })).sort((a, b) => b.n - a.n)
  }, [byQ])

  // ตัวกรองทุกชั้นยกเว้นกลุ่มความกระชั้น — กราฟต้องเห็นการกระจายทั้งหมดของชุดที่กรองไว้
  // ไม่กรองตามสถานะใบ (ยังรอวางแผน/ปิดแล้ว) — หน้านี้ตอบว่า "แต่ละ SC ห่างเท่าไร" ล้วน ๆ
  // สถานะไม่เกี่ยวกับระยะห่าง ทุกใบในทะเบียนจึงถูกนับเสมอ
  const base = useMemo(
    () => (otypes.length ? byQ.filter(r => otypes.includes(r.order_type)) : byQ),
    [byQ, otypes])
  const toggleType = (t) =>
    setOtypes(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t])

  const shown = useMemo(() => {
    if (!selBucket || !buckets) return base
    const b = buckets.find(x => x.id === selBucket)
    return b ? base.filter(r => r.gap >= b.lo && r.gap <= b.hi) : base
  }, [base, selBucket, buckets])

  // KPI สองใบขวาอิงกลุ่ม "แรก" กับ "สุดท้าย" ของช่วงที่ตั้งไว้ — ตั้งเกณฑ์เดียว (เช่น 5)
  // จะได้ "ต่ำกว่า 5" คู่กับ "5 ขึ้นไป" ; ตั้งหลายช่วงก็ยังได้หัว-ท้ายที่มีความหมายเสมอ
  const bFirst = buckets ? buckets[0] : null
  const bLast = buckets ? buckets[buckets.length - 1] : null
  // 1 แถว = 1 รายการ (ใบ × งวดส่ง × สี) — ใบเดียวแบ่งส่งหลายงวดได้ แต่ละงวดคนละกำหนดส่ง
  // จึงคนละ lead time ; การ์ดใบแรกนับ "SC ไม่ซ้ำ" ส่วนกลุ่ม/กราฟนับเป็นรายการตามเดิม
  const kpi = {
    total: base.length,
    scs: new Set(base.map(r => r.sc).filter(Boolean)).size,
    neg: base.filter(r => r.gap < 0).length,
    below: bFirst ? base.filter(r => r.gap <= bFirst.hi).length : 0,
    reach: bLast ? base.filter(r => r.gap >= bLast.lo).length : 0,
    qty: base.reduce((s, r) => s + (r.qty || 0), 0),
  }
  const pctOf = (n) => (kpi.total ? Math.round((n / kpi.total) * 1000) / 10 : 0)
  const filterOn = !!(otypes.length || selBucket || q.trim())

  return (
    <div className={'mcav-dash ' + theme} onMouseLeave={() => setTip(null)}>
      <Tip tip={tip} />

      <div className="mcav-top">
        <div className="mcav-title">
          <h1>SC Lead Time<span className="live">สะสม</span></h1>
          {/* ข้อจำกัด 2 ข้อ (ขอบเขตทะเบียน · งานที่ถูกตัดออก) อยู่ใน tooltip ของบรรทัดนี้
              แทนแถบเตือนถาวรที่ user สั่งเอาออก — ยังตรวจสอบได้ แต่ไม่เบียดพื้นที่หน้าจอ */}
          <div className="sub" title={
            'นับเฉพาะใบที่เคยผ่าน view SC ตั้งแต่วันที่เริ่มเก็บทะเบียน — งาน booking เก่าที่ปิดใบ'
            + 'ไปก่อนหน้านั้นไม่มีวันเปิดให้ดู (ฝั่ง booking มีแต่วันด้ายเข้า ซึ่งเป็นคนละเหตุการณ์)'
            + (d?.excluded > 0
              ? `\nตัดงาน ${(d.excluded_types || []).join(' / ')} ออก ${fmt(d.excluded)} รายการ`
              + ' — ไม่ใช่ออร์เดอร์ปกติที่เปิดมาพร้อมกำหนดส่ง จึงไม่ใช่ lead time ที่หน้านี้วัด'
              : '')}>
            <b>สัปดาห์ที่เปิด SC → RDD (FG week)</b> ห่างกันกี่สัปดาห์ — เทียบตรง ๆ ไม่หักเวลางานหลังทอ ·
            <b> ค่าติดลบ = เปิดใบมาหลังกำหนดส่งไปแล้ว</b> ·
            ฐานข้อมูลคือทะเบียน SC สะสมที่เก็บเพิ่มทุกครั้งที่ดึงข้อมูล SC ·
            <b>ไม่รวมงาน FQC และผ้าเติม</b>
          </div>
        </div>
        <div className="mcav-topright">
          <div className="clock">
            <b>{fmt(d?.total)} ใบสะสม</b>
            <span>{d?.source?.file || 'ยังไม่มีทะเบียน'}</span>
          </div>
          <button className="tbtn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☀ ธีมสว่าง' : '🌙 ธีมมืด'}
          </button>
          <button className="tbtn" onClick={backfill} disabled={loading}
            title="รวมไฟล์ snapshot ที่ยังไม่เคยเข้าทะเบียน (กดซ้ำได้ ไฟล์ที่รวมแล้วถูกข้าม)">
            ⤵ อัปเดตทะเบียน
          </button>
          <button className="tbtn go" onClick={load} disabled={loading}>
            {loading ? '⏳ กำลังโหลด' : '↻ โหลดใหม่'}
          </button>
        </div>
      </div>

      <div className="mcav-alerts">
        {err && <span className="al bad">{err}</span>}
        {msg && <span className="al info">{msg}</span>}
        {d?.note && <span className="al warn">{d.note}</span>}
        {/* ข้อจำกัด "นับเฉพาะใบที่เคยผ่าน view SC" ย้ายไปเป็น tooltip ของหัวเรื่องแทน
            (user สั่งเอาแถบออก 7 ก.ย. 2569) — ยังอยู่ในหน้า แต่ไม่กินพื้นที่ถาวร */}
        {d?.source?.migrated && (
          <span className="al info">
            ย้ายทะเบียนเป็นโครงใหม่ให้แล้ว (เดิมเก็บใบละแถวเดียว ทำให้ใบที่แบ่งส่งหลายงวด/หลาย SO
            ข้อมูลหาย) — สร้างใหม่จาก snapshot ที่เก็บไว้
            {d.source.backfilled?.added != null && ` ได้ ${fmt(d.source.backfilled.added)} รายการ`}
          </span>
        )}
        {d?.no_date > 0 && (
          <span className="al warn">{fmt(d.no_date)} ใบไม่มีวันเปิด — ไม่นับในสถิติ</span>
        )}
        {d?.no_target > 0 && (
          <span className="al warn">{fmt(d.no_target)} ใบไม่มีกำหนดส่ง (FG week) — ไม่มีเป้าให้เทียบ</span>
        )}
        {d?.truncated > 0 && (
          <span className="al info">แสดง {fmt(d.rows.length)} ใบล่าสุด (ตัดใบเก่าออก {fmt(d.truncated)})</span>
        )}
      </div>

      {!d || !all.length ? (
        <div className="mcav-empty">
          {d?.note || 'ยังไม่มีใบในทะเบียน — กด "อัปเดตทะเบียน" หรือรันขั้นดึงข้อมูล DB ก่อน'}
        </div>
      ) : (
        <>
          <div className="mcav-filters">
            {/* ติ๊กได้หลายประเภทพร้อมกัน (ไม่ติ๊กเลย = ทุกประเภท) — ต้องเทียบ 2 ประเภท
                ในกราฟ/KPI ชุดเดียวกันได้ ซึ่ง dropdown เลือกค่าเดียวทำไม่ได้
                จำนวนบนชิปนับจาก byQ (ผ่านคำค้นแล้ว) ไม่ใช่ base — ตัวที่ยังไม่ติ๊กจึงไม่เป็น 0 */}
            <div className="scl-tchips">
              <button className={'chip' + (otypes.length ? '' : ' on')}
                onClick={() => setOtypes([])}
                title="แสดงทุกประเภทออร์เดอร์">ทุกประเภท</button>
              {types.map(t => (
                <button key={t.key}
                  className={'chip scl-tchip' + (otypes.includes(t.key) ? ' on' : '')}
                  onClick={() => toggleType(t.key)}
                  title={otypes.includes(t.key) ? 'คลิกเพื่อเอาออกจากการเทียบ' : 'คลิกเพื่อเพิ่มเข้าการเทียบ'}>
                  {t.key}<span className="n">{fmt(t.n)}</span>
                </button>
              ))}
            </div>
            <input className="scl-search" value={q} onChange={e => setQ(e.target.value)}
              placeholder="ค้นหา SC / Item / ลูกค้า / PO" />
            <label className="scl-bucketbox" title="ขอบล่างของแต่ละกลุ่ม คั่นด้วยจุลภาค เช่น 0,2,4,7">
              <span>ช่วงกลุ่ม</span>
              <input value={bucketText} onChange={e => applyBuckets(e.target.value)}
                className={parseBuckets(bucketText) ? '' : 'bad'} />
              <button className="rst" onClick={() => applyBuckets(DEFAULT_BUCKETS)}
                title="กลับค่าเริ่มต้น">↺</button>
            </label>
            {filterOn && (
              <button className="chip" onClick={() => {
                setOtypes([]); setQ(''); setSelBucket(null)
              }}>ล้างตัวกรองทั้งหมด</button>
            )}
          </div>

          <div className="mcav-kpis">
            <div className="kpi k1">
              <div className="lb">📄 SC ทั้งหมด</div>
              <div className="vl">{fmt(kpi.scs)}</div>
              <div className="ft">
                {fmt(kpi.total)} รายการ (ใบเดียวแบ่งส่งได้หลายงวด) · ยอดรวม {fmt(kpi.qty)} kg
              </div>
            </div>
            <div className="kpi k3">
              <div className="lb">⏱ ห่าง{bFirst ? bFirst.full : '-'}</div>
              <div className="vl">{fmt(kpi.below)}<span className="pc"> · {pctOf(kpi.below)}%</span></div>
              <div className="ft">
                เวลาไม่พอตั้งแต่วันเปิดใบ — ในนี้เปิดมาหลังกำหนดส่งแล้ว {fmt(kpi.neg)} รายการ
              </div>
            </div>
            <div className="kpi k4">
              <div className="lb">✅ ห่าง {bLast ? bLast.full : '-'}</div>
              <div className="vl">{fmt(kpi.reach)}<span className="pc"> · {pctOf(kpi.reach)}%</span></div>
              <div className="ft">เปิดใบล่วงหน้าพอให้จัดคิวเครื่องได้</div>
            </div>
          </div>

          {buckets && (
            <div className="scl-charts">
              <div className="scl-card">
                <div className="scl-h">
                  การกระจายของระยะห่าง (ใบ)
                  {selBucket && (
                    <button className="chip on" onClick={() => setSelBucket(null)}>
                      กรอง: {buckets.find(b => b.id === selBucket)?.label} ✕
                    </button>
                  )}
                </div>
                <Histogram buckets={buckets} rows={base} sel={selBucket}
                  onSel={setSelBucket} setTip={setTip} />
              </div>
              <div className="scl-card">
                <ByType rows={base} buckets={buckets} setTip={setTip} />
              </div>
            </div>
          )}

          <div className="scl-tablebox">
            <table className="scl-table">
              <thead>
                <tr>
                  <th>SC</th><th>SO</th><th>Item</th><th>ประเภท</th><th>FOB</th><th>ลูกค้า</th>
                  <th>วันที่เปิด SC</th><th>สัปดาห์เปิด</th><th>RDD</th><th>Target Knit</th>
                  <th className="n">ระยะห่าง (สัปดาห์)</th><th className="n">Order (kg)</th>
                </tr>
              </thead>
              <tbody>
                {shown.slice(0, 500).map((r, i) => {
                  const b = buckets ? bucketOf(buckets, r.gap) : null
                  return (
                    <tr key={i}>
                      <td className="b nw">{r.sc || '-'}</td>
                      {/* ใบเดียวมีได้หลาย SO/หลายงวด — SO กับกำหนดส่ง FG คือสิ่งที่แยกบรรทัดออกจากกัน */}
                      <td className="nw dim" title={r.line ? `บรรทัดใบ ${r.line}` : ''}>{r.so || '-'}</td>
                      <td className="b">{r.item}</td>
                      <td className="nw">{r.order_type || '-'}</td>
                      {/* FOB_TYPE = ตัวที่กำหนด offset เป้าทอ (PILOT_RUN/Sample/Salesman = FG−1)
                          จึงอยู่ติดกับประเภทออร์เดอร์ ไม่ใช่แยกไปท้ายตาราง */}
                      <td className="nw dim">{r.fob || '-'}</td>
                      <td className="cust">{r.customer || '-'}</td>
                      <td className="nw" title={r.date_changed ? 'วันเปิดในระบบเคยถูกแก้ — แสดงค่าที่เห็นครั้งแรก' : ''}>
                        {dateTH(r.open_date)}{r.date_changed ? <span className="more"> ⓘ</span> : null}
                      </td>
                      <td className="nw dim">{wkLabel(r.open_week, r.open_year)}</td>
                      <td className="nw">{wkLabel(r.fg, r.fg_year)}</td>
                      {/* Target Knit เป็นข้อมูลเสริม ไม่ใช่ตัวคิดระยะห่างแล้ว — hover เห็นว่า
                          ถ้านับถึงเวลาที่ต้องลงเครื่องจริงจะเหลือกี่สัปดาห์ (สั้นกว่าเสมอ) */}
                      <td className="nw dim" title={`FG หัก ${r.offset} สัปดาห์`
                        + (r.gap_knit != null ? ` · ห่างจากสัปดาห์เปิด ${r.gap_knit} สัปดาห์` : '')}>
                        {wkLabel(r.target, r.target_year)}
                      </td>
                      <td className={'n gap t-' + (b ? b.tone : 'mid')}>{r.gap}</td>
                      <td className="n">{fmt(r.qty)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            {shown.length > 500 && (
              <div className="scl-more">แสดง 500 จาก {fmt(shown.length)} ใบ — ใช้ตัวกรองเพื่อดูให้แคบลง</div>
            )}
            {!shown.length && <div className="mcav-empty sm">ไม่มีใบที่ตรงกับตัวกรอง</div>}
          </div>
        </>
      )}
    </div>
  )
}

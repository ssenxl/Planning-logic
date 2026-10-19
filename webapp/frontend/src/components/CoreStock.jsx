import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'

/* Dashboard › Item Core Stock — ผ้าดิบตัวไหนมีสต๊อกเท่าไหร่ พอใช้กี่สัปดาห์ ตัวไหนต้องผลิตแล้ว
   2 โหมด:
     Core Greige   1 แถว = 1 ผ้า × ลูกค้า × ทีม — ครบสามอย่างถึงเป็นสต๊อก core ของแถวนั้น
                   ของรหัสเดียวกันที่ผูกลูกค้าอื่น/อยู่คนละทีม = คนละก้อน ไม่เอามารวม
     สต๊อกทั้งหมด  1 แถว = 1 ผ้า รวมทั้งคลัง (ทุกลูกค้า ทุกทีม)
   MIN/MAX มาจากไฟล์ Target_Stock ที่ user แก้เองได้ที่หน้า Master Data

   โหมด Core: ผ้าที่ทะเบียนตั้งเป้าไว้หลายทีม จะมีแถว **“รวม N ทีม”** ต่อท้ายแถวรายทีมของมัน
   (ทั้งในกราฟและตาราง) — ยอดรวมเทียบ MIN/MAX ที่บวกเป้าของทีมเหล่านั้นแล้ว
   ทีมหนึ่งขาดแต่อีกทีมมีเหลือ = โยกของแทนสั่งทอใหม่ได้ (ป้าย “โยกได้ ~X kg”)
   แถวรวมมาจาก totals ของ API ไม่ได้อยู่ใน rows จึงไม่ถูกนับซ้ำใน KPI / ชิปสถานะ
   และเป็นยอดของ “ทุกทีม core ของผ้านั้น” เสมอ แม้ตัวกรองจะซ่อนบางทีมอยู่

   ตัวเลขยึดกติกาเดียวกับ pipeline (ดู core_stock_live.py):
     สต๊อกที่ใช้ได้ = ล็อต QA ว่าง และ ≥ 100 kg · Inventory = สต๊อก + งานที่จะทอเข้า 2 สัปดาห์
     cover = Inventory ÷ STOCK MIN (สัปดาห์) · cover < 2 = ถึงจุดสั่งผลิตของ Planning

   ธีม/เลย์เอาต์ใช้ชุดเดียวกับหน้า Machine Availability
   (.mcav-dash + ตัวแปร --d-*) — คลาสเฉพาะหน้านี้ขึ้นต้น .cs- */

const THEME_KEY = 'mcav-theme'      // ใช้ค่าเดียวกับหน้า Dashboard อื่น — สลับธีมหน้าหนึ่งแล้วอีกหน้าตาม

const MODES = [
  { id: 'core', label: '🧵 Core Greige', hint: 'จับคู่ ผ้า + ลูกค้า + ทีม ตามทะเบียน Target Stock' },
  { id: 'all', label: '📦 สต๊อกทั้งหมด', hint: 'ทุก item ที่มีของในคลัง (รวมทุกลูกค้า/ทีม)' },
]

// สถานะสต๊อก — ต้องตรงกับ _status() ใน core_stock_live.py
const STATUS = {
  crit: { label: 'ต้องผลิต', full: 'ต้องผลิต (สต๊อกพอใช้ < 2 สัปดาห์)', color: 'var(--d-crit)' },
  ok: { label: 'ปกติ', full: 'ปกติ (อยู่ในกรอบ MIN–MAX)', color: 'var(--d-free)' },
  over: { label: 'สต๊อกล้น', full: 'สต๊อกล้น (Inventory เกิน STOCK MAX)', color: 'var(--d-ok)' },
  none: { label: 'ไม่มีเป้า', full: 'ไม่มีเป้า MIN/MAX ให้เทียบ (นอกทะเบียน Core)', color: 'var(--d-muted)' },
}
const STATUS_ORDER = ['crit', 'ok', 'over', 'none']

/* สถานะ none คนละความหมายในสองโหมด:
     core = แถวนี้อยู่ในทะเบียนแล้ว แต่ยังไม่ได้ใส่ TARGET/MONTH จึงยังไม่มี MIN/MAX
     all  = ผ้าตัวนี้อยู่นอกทะเบียน core เลย */
const stLabel = (id, mode) => (id === 'none' && mode === 'core' ? 'ยังไม่ตั้งเป้า' : STATUS[id].label)
const stFull = (id, mode) => (id === 'none' && mode === 'core'
  ? 'แถวนี้อยู่ในทะเบียนแล้วแต่ยังไม่ได้ใส่ TARGET/MONTH — เติมได้ที่หน้า Master Data'
  : STATUS[id].full)

const SORTS = [
  { id: 'cover', label: 'ขาดมากสุด' },
  { id: 'kg', label: 'สต๊อกมากสุด' },
  { id: 'aging', label: 'ค้างนานสุด' },
  { id: 'item', label: 'รหัสผ้า' },
]

const PAGE = 300                    // แถวที่ render ต่อครั้ง (โหมดทั้งหมดมี ~1,700 item)
// กราฟระดับสต๊อกโชว์ครบทุกแถว (เลื่อนดูในกรอบ) — โหมด "ทั้งหมด" มีเป็นพันแถว
// จึงทยอย render ทีละก้อนเหมือนตาราง ไม่งั้นเปิดหน้าแล้วค้าง
const BAR_PAGE = 300

const SCALES = [
  // เทียบกันทุกแถว: แท่งยาวเท่าไหร่เทียบกันได้ทั้งการ์ด แต่ผ้าเป้าเล็กจะเหลือแท่งจิ๋ว
  { id: 'shared', label: 'สเกล: เทียบทุกแถว' },
  // เทียบเป้าตัวเอง: แต่ละแถวเต็มกรอบที่ STOCK MAX ของตัวเอง → เห็นว่าห่างเป้าแค่ไหนทุกแถว
  { id: 'own', label: 'สเกล: เทียบเป้าของตัวเอง' },
]

const fmt = (n) => Math.round(n || 0).toLocaleString()
const fmt1 = (n) => (Math.round((n || 0) * 10) / 10).toLocaleString()

function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

/** cover เป็นข้อความ — ไม่มีเป้า = '-' , มากกว่า 99 สัปดาห์ไม่ต้องโชว์เลขยาว */
function coverText(r) {
  if (!r.stock_min) return '-'
  if (r.cover >= 99) return '99+ สัปดาห์'
  return `${(Math.round(r.cover * 10) / 10).toLocaleString()} สัปดาห์`
}

/* ---------- tooltip ลอยตามเมาส์ (สไตล์ .mcav-tip ร่วมกับหน้า Dashboard อื่น) ---------- */
function Tip({ tip }) {
  if (!tip) return null
  return (
    <div className="mcav-tip" style={{ left: tip.x, top: tip.y }}>
      <div className="mcav-tip-title">{tip.title}</div>
      {tip.lines.map((l, i) => (
        <div key={i} className="mcav-tip-line">
          {l.swatch && <i style={{ background: l.swatch }} />}
          <span>{l.text}</span>
        </div>
      ))}
    </div>
  )
}
const tipPos = (e) => ({ x: Math.min(e.clientX + 14, window.innerWidth - 300), y: e.clientY + 16 })

/* ---------- แถบระดับสต๊อกรายไอเทม ----------
   ทึบ = ของที่มีอยู่จริง · ลาย = งานที่จะทอเข้าอีก 2 สัปดาห์ · ขีด = STOCK MIN / MAX
   items = [{ r, total }] — total:true คือแถวสรุป "รวม N ทีม" ที่แทรกท้ายกลุ่มของผ้านั้น */
function StockBars({ items, mode, scale, setTip, onPick }) {
  if (!items.length) return <div className="mcav-empty sm">ไม่มีรายการที่ตรงกับตัวกรอง</div>
  // shared = สเกลเดียวทั้งการ์ด (เทียบขนาดข้ามแถวได้ แต่ผ้าเป้าเล็กจะเหลือแท่งจิ๋ว)
  // own    = แต่ละแถวเต็มกรอบที่เป้า/ของตัวเอง (ทุกแถวอ่านออก แต่เทียบข้ามแถวไม่ได้)
  const shared = Math.max(1, ...items.map(({ r }) => Math.max(r.inv, r.stock_max || 0)))
  const base = (r) => (scale === 'own'
    ? Math.max(1, r.stock_max || 0, r.inv, r.onhand + r.in1 + r.in2)
    : shared)

  const show = (e, r, total) => setTip({
    ...tipPos(e),
    title: r.item + (total ? ` · รวม ${r.n_teams} ทีม` : (r.team ? ` · ${r.team}` : '')),
    lines: [
      ...(r.cus ? [{ text: `ลูกค้า ${r.cus}` }] : []),
      { swatch: STATUS[r.status].color, text: stFull(r.status, mode) },
      { text: `สต๊อกใช้ได้ ${fmt(r.onhand)} kg (${r.lots} ล็อต)` },
      { text: `งานที่จะทอเข้า 2 สัปดาห์ ${fmt(r.in1 + r.in2)} kg` },
      { text: `Inventory ${fmt(r.inv)} kg = พอใช้ ${coverText(r)}` },
      ...(r.stock_min ? [{ text: `MIN ${fmt(r.stock_min)} · MAX ${fmt(r.stock_max)} kg` }] : []),
      ...(total ? totalTipLines(r) : []),
    ],
  })

  return (
    <div className="cs-bars">
      {items.map(({ r, total }) => {
        const m = base(r)
        const pc = (v) => Math.min(100, (Math.max(0, v) / m) * 100)
        return (
        <div key={r.key} className={'cs-bar-row' + (total ? ' tot' : '')}
          onMouseMove={e => show(e, r, total)} onMouseLeave={() => setTip(null)}
          onClick={() => onPick(r.item)}>
          <span className="lb" title={r.item + (r.team ? ' · ' + r.team : '')}>
            {total
              ? <>รวม {r.n_teams} ทีม{r.rescue && <i className="mv">🔄 {fmt(r.movable)} kg</i>}</>
              : <>{r.item}{r.team && <i className="tm">{r.team}</i>}</>}
          </span>
          <span className="track">
            {/* ขีดกรอบเป้า: MIN = พื้นที่ที่ต้องมีเป็นอย่างน้อย, MAX = เพดาน */}
            {r.stock_min > 0 && <i className="mk min" style={{ left: pc(r.stock_min) + '%' }} />}
            {r.stock_max > 0 && <i className="mk max" style={{ left: pc(r.stock_max) + '%' }} />}
            <b className="seg on" style={{ width: pc(r.onhand) + '%', background: STATUS[r.status].color }} />
            <b className="seg inc" style={{ width: pc(r.in1 + r.in2) + '%' }} />
          </span>
          <em className="kg">{fmt(r.inv)}</em>
          <em className={'cov ' + r.status}>{coverText(r)}</em>
        </div>
        )
      })}
    </div>
  )
}

/** บรรทัด tooltip ที่มีเฉพาะแถวสรุป — บอกที่มาของยอดและโอกาสโยกของข้ามทีม */
function totalTipLines(r) {
  return [
    { text: `ยอดรวมของทีม core ทั้ง ${r.n_teams} ทีม (มีของจริง ${r.n_stock_teams} ทีม)` },
    { text: 'MIN/MAX = เป้าของทีมเหล่านั้นบวกกัน · เป็นยอดของทุกทีมเสมอ แม้ตัวกรองจะซ่อนบางทีม' },
    ...(r.need ? [{ text: `ทีมที่ขาดต้องเติมอีก ${fmt(r.need)} kg · ทีมอื่นมีเหลือปล่อยได้ ${fmt(r.spare)} kg` }] : []),
    ...(r.rescue
      ? [{ text: `→ โยกข้ามทีมได้ ${fmt(r.movable)} kg โดยไม่ต้องสั่งทอใหม่` }]
      : (r.movable ? [{ text: `โยกช่วยกันได้ ${fmt(r.movable)} kg แต่รวมแล้วยังต่ำกว่ากรอบ — ต้องสั่งทอเพิ่ม` }] : [])),
  ]
}

/* ---------- กระจายตามสถานะ ---------- */
function StatusSpread({ rows, mode, setTip, sel, onSel }) {
  const buckets = STATUS_ORDER.map(id => {
    const hit = rows.filter(r => r.status === id)
    return { id, ...STATUS[id], label: stLabel(id, mode), full: stFull(id, mode),
      n: hit.length, kg: hit.reduce((s, r) => s + r.onhand, 0) }
  }).filter(b => b.n > 0)
  const max = Math.max(1, ...buckets.map(b => b.n))
  if (!buckets.length) return <div className="mcav-empty sm">ไม่มีรายการที่ตรงกับตัวกรอง</div>
  return (
    <div className="otd-spread">
      {buckets.map(b => (
        <div key={b.id} className={'otd-spread-row cs-spread' + (sel && sel !== b.id ? ' dim' : '')}
          onClick={() => onSel(b.id)}
          onMouseMove={e => setTip({
            ...tipPos(e), title: b.full,
            lines: [{ text: `${fmt(b.n)} รายการ` }, { text: `${fmt(b.kg)} kg` },
            { text: 'คลิก = กรองเฉพาะกลุ่มนี้' }],
          })}
          onMouseLeave={() => setTip(null)}>
          <span className="lb">{b.label}</span>
          <span className="track">
            <i style={{ width: (b.n / max) * 100 + '%', background: b.color }} />
          </span>
          <b>{fmt(b.n)}</b>
          <em>{fmt(b.kg)} kg</em>
        </div>
      ))}
    </div>
  )
}

/* ---------- ตารางรายการ (items = [{ r, total }] เหมือน StockBars) ---------- */
function StockTable({ items, mode, weeks, setTip }) {
  if (!items.length) return <div className="mcav-empty sm">ไม่มีรายการที่ตรงกับตัวกรอง</div>
  const w1 = weeks[0], w2 = weeks[1]
  return (
    <div className="otd-tablebox">
      <table className="otd-table cs-table">
        <thead>
          <tr>
            <th>Item</th><th>ลูกค้า / ทีม</th>
            <th className="n">สต๊อกใช้ได้ (kg)</th>
            <th className="n">ติด QA / เศษ</th>
            <th className="n">{w1 ? `เข้า W${w1.w}` : 'เข้า W+1'}</th>
            <th className="n">{w2 ? `เข้า W${w2.w}` : 'เข้า W+2'}</th>
            <th className="n">Inventory</th>
            <th className="n">MIN</th><th className="n">MAX</th>
            <th className="n">พอใช้</th>
            {/* AGING_WW ในไฟล์ Stock นับเป็น "สัปดาห์" (เทียบกับ KNIT_WW/KNIT_YEAR ของล็อตแล้วตรง) */}
            <th className="n">ล็อต / ค้างนานสุด (สัปดาห์)</th>
            <th>สถานะ</th>
          </tr>
        </thead>
        <tbody>
          {items.map(({ r, total }) => (
            <tr key={r.key} className={total ? 'cs-xtot' : undefined}
              onMouseMove={total ? (e => setTip({ ...tipPos(e), title: `${r.item} · รวม ${r.n_teams} ทีม`,
                lines: totalTipLines(r) })) : undefined}
              onMouseLeave={total ? (() => setTip(null)) : undefined}>
              <td className="b">
                {total ? '' : r.item}
                {!total && mode === 'all' && r.is_core && <span className="cs-tag core">core</span>}
              </td>
              <td className="cs-who">
                {/* แถวสรุป = ยอดของทีม core ทั้งหมดของผ้านั้น
                    โหมด core = ลูกค้า+ทีมของแถวนั้น (คีย์ที่ใช้จับ core)
                    โหมด all = ทุกทีมที่ถือของ item นี้ */}
                {total
                  ? <><b>รวม {r.n_teams} ทีม</b>
                    {r.rescue && <span className="cs-tag move">🔄 โยกได้ ~{fmt(r.movable)} kg</span>}</>
                  : mode === 'core'
                    ? <>{r.cus || '-'}<span className="sub">{r.team || '-'}</span></>
                    : (r.teams.map(t => t.team).join(', ') || '-')}
              </td>
              <td className="n b">{fmt(r.onhand)}</td>
              <td className="n dim">{!total && (r.blocked || r.small) ? `${fmt(r.blocked)} / ${fmt(r.small)}` : '-'}</td>
              <td className="n">{r.in1 ? fmt(r.in1) : '-'}</td>
              <td className="n">{r.in2 ? fmt(r.in2) : '-'}</td>
              <td className="n">{fmt(r.inv)}</td>
              <td className="n dim">{r.stock_min ? fmt(r.stock_min) : '-'}</td>
              <td className="n dim">{r.stock_max ? fmt(r.stock_max) : '-'}</td>
              <td className={'n cs-cov ' + r.status}>{coverText(r)}</td>
              <td className="n dim">{total ? '-' : `${r.lots} / ${r.aging_max ? r.aging_max + ' สัปดาห์' : '-'}`}</td>
              <td className="nw">
                <span className={'cs-dot ' + r.status} style={{ background: STATUS[r.status].color }} />
                {stLabel(r.status, mode)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ---------- หน้าหลัก ---------- */
export default function CoreStock() {
  const [theme, setTheme] = useState(() => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'))
  const [mode, setMode] = useState('core')
  const [d, setD] = useState(null)
  const [err, setErr] = useState('')
  const [loading, setLoading] = useState(false)
  const [tip, setTip] = useState(null)
  const [team, setTeam] = useState('')
  const [status, setStatus] = useState('')
  const [sort, setSort] = useState('cover')
  const [q, setQ] = useState('')
  const [limit, setLimit] = useState(PAGE)
  const [barLimit, setBarLimit] = useState(BAR_PAGE)
  const [scale, setScale] = useState('shared')

  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])

  async function load(m = mode) {
    setLoading(true); setErr('')
    try { setD(await api.coreStock(m)) }
    catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }
  useEffect(() => { load(mode); setLimit(PAGE); setBarLimit(BAR_PAGE) }, [mode])

  const rows = d?.rows || []
  const weeks = d?.weeks || []

  const teams = useMemo(() => {
    const s = new Set()
    for (const r of rows) { if (r.team) s.add(r.team); for (const t of r.teams) s.add(t.team) }
    return [...s].sort()
  }, [rows])

  const kw = q.trim().toLowerCase()
  const shown = useMemo(() => {
    const out = rows.filter(r => {
      // โหมด core แถวเป็นของทีมเดียวอยู่แล้ว · โหมด all ดูจากทีมที่ถือของ
      if (team && r.team !== team && !r.teams.some(t => t.team === team)) return false
      if (status && r.status !== status) return false
      if (kw && ![r.item, r.team, r.cus, r.family, r.structure, r.yarn, r.mc_group, r.customer]
        .join(' ').toLowerCase().includes(kw)) return false
      return true
    })
    const by = {
      cover: (a, b) => (a.stock_min ? 0 : 1) - (b.stock_min ? 0 : 1) ||
        (a.stock_min ? a.cover - b.cover : b.onhand - a.onhand),
      kg: (a, b) => b.onhand - a.onhand,
      aging: (a, b) => b.aging_max - a.aging_max,
      item: (a, b) => a.item.localeCompare(b.item),
    }
    return [...out].sort(by[sort] || by.cover)
  }, [rows, team, status, kw, sort])

  /* แถวที่เอาไปแสดงจริง: โหมด Core จับแถวของผ้ารหัสเดียวกันมาไว้ติดกัน แล้วต่อท้ายด้วย
     แถวสรุป "รวม N ทีม" — ถ้าไม่จับมาติดกันก่อน (การเรียงตาม cover แยกสองทีมไปคนละที่)
     แถวรวมจะไปโผล่ห่างจากแถวที่มันสรุปจนอ่านไม่รู้เรื่อง
     แถวสรุปมาจาก d.totals ไม่ได้อยู่ใน shown → ไม่ถูกนับซ้ำใน KPI / ชิปสถานะ / กราฟสถานะ */
  const list = useMemo(() => {
    const totals = d?.totals || {}
    if (mode !== 'core' || !Object.keys(totals).length) return shown.map(r => ({ r }))
    const out = []
    const done = new Set()
    for (const r of shown) {
      if (!totals[r.item]) { out.push({ r }); continue }
      if (done.has(r.item)) continue          // แถวพี่น้องถูกดึงขึ้นมาไว้ในกลุ่มแรกแล้ว
      done.add(r.item)
      for (const s of shown) if (s.item === r.item) out.push({ r: s })
      out.push({ r: totals[r.item], total: true })
    }
    return out
  }, [shown, d, mode])

  // KPI คิดจากแถวที่ผ่านตัวกรอง (ไม่ใช่ทั้งไฟล์) ตัวเลขบนหัวจึงตรงกับตารางที่เห็นเสมอ
  const kpi = useMemo(() => {
    const crit = shown.filter(r => r.status === 'crit')
    const over = shown.filter(r => r.status === 'over')
    return {
      items: shown.length,
      codes: new Set(shown.map(r => r.item)).size,
      kg: shown.reduce((s, r) => s + r.onhand, 0),
      incoming: shown.reduce((s, r) => s + r.in1 + r.in2, 0),
      crit: crit.length,
      empty: crit.filter(r => r.onhand <= 0).length,
      // ยอดที่ต้องเติมให้ถึงกรอบล่าง (2 สัปดาห์) — บอกได้ทันทีว่าต้องสั่งทอรวมกี่ kg
      need: crit.reduce((s, r) => s + Math.max(0, r.stock_min * 2 - r.inv), 0),
      over: over.length,
      overKg: over.reduce((s, r) => s + Math.max(0, r.inv - r.stock_max), 0),
    }
  }, [shown])

  const filterOn = !!(team || status || kw)
  const src = d?.source || {}


  return (
    <div className={'mcav-dash ' + theme} onMouseLeave={() => setTip(null)}>
      <Tip tip={tip} />

      <div className="mcav-top">
        <div className="mcav-title">
          <h1>Item Core Stock<span className="live">LIVE</span></h1>
          <div className="sub">
            สต๊อกผ้าดิบจากไฟล์ Stock · นับเฉพาะล็อตที่ QA ว่างและ ≥ 100 kg (กติกาเดียวกับ pipeline) ·
            Inventory = สต๊อก + งานที่จองเครื่องไว้ว่าจะทอเสร็จอีก 2 สัปดาห์ ·
            พอใช้ = Inventory ÷ STOCK MIN — ต่ำกว่า 2 สัปดาห์ = จุดที่ Planning สั่งผลิต core ·
            โหมด Core จับคู่ด้วย ผ้า + ลูกค้า + ทีม (ครบสามอย่างถึงเป็นของก้อนเดียวกัน) ·
            ผ้าที่ตั้งเป้าไว้หลายทีมมีแถว “รวม N ทีม” ต่อท้าย = เอาของทุกทีม core มากองรวม
            เทียบ MIN/MAX ที่บวกเป้ากันแล้ว
          </div>
        </div>
        <div className="mcav-topright">
          <div className="clock">
            <b>{fmtTime(src.stock?.mtime)}</b>
            <span>{src.stock?.file || 'ไม่พบไฟล์ Stock'}</span>
          </div>
          <button className="tbtn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☀ ธีมสว่าง' : '🌙 ธีมมืด'}
          </button>
          <button className="tbtn go" onClick={() => load()} disabled={loading}>
            {loading ? '⏳ กำลังโหลด' : '↻ โหลดใหม่'}
          </button>
        </div>
      </div>

      {(err || d?.note || (mode === 'core' && d?.source?.booking?.per_customer === false)) && (
        <div className="mcav-alerts">
          {err && <span className="al bad">{err}</span>}
          {d?.note && <span className="al warn">{d.note}</span>}
          {/* ไฟล์ booking เก่าไม่มีชีท DETAIL_SC → ไม่มีคอลัมน์ลูกค้าให้จับคู่
              ยอด "กำลังทอเข้า" จึงจับด้วยผ้า+ทีมเท่านั้น (อาจรวมงานของลูกค้าอื่นในทีมเดียวกัน) */}
          {mode === 'core' && d?.source?.booking?.per_customer === false && (
            <span className="al info">
              ไฟล์ booking นี้ไม่มีคอลัมน์ลูกค้า — ยอด “กำลังทอเข้า” จับด้วยผ้า + ทีม เท่านั้น
              (รัน AVA_MC ใหม่เพื่อให้ได้ชีท DETAIL_SC แล้วจะจับครบทั้ง 3 อย่าง)
            </span>
          )}
        </div>
      )}

      <div className="cs-modes">
        {MODES.map(m => (
          <button key={m.id} className={'cs-mode' + (mode === m.id ? ' on' : '')}
            onClick={() => { setMode(m.id); setStatus(''); setTeam('') }}>
            <b>{m.label}</b><span>{m.hint}</span>
          </button>
        ))}
        <div className="cs-modenote">
          {mode === 'core'
            ? `1 แถว = 1 ผ้า × ลูกค้า × ทีม — ครบสามอย่างถึงนับเป็นสต๊อก core ของแถวนั้น · ` +
            `ทะเบียน ${fmt(d?.core_total)} แถว (${fmt(d?.core_items)} รหัสผ้า) · ` +
            `MIN/MAX มาจากไฟล์ Target_Stock แก้ได้ที่หน้า Master Data · ` +
            `${fmt(Object.keys(d?.totals || {}).length)} รหัสที่ตั้งเป้าไว้หลายทีม มีแถว “รวม N ทีม” ` +
            `ต่อท้าย (นับเฉพาะทีมที่เป็น core ของรหัสนั้น ของที่กองอยู่ทีมอื่นไม่นับ)`
            : `1 แถว = 1 ผ้า รวมทั้งคลัง (ทุกลูกค้า/ทีม) · มีของในคลัง ${fmt(d?.stock_items)} รหัส · ` +
            `นอกทะเบียน core ไม่มี MIN/MAX ให้เทียบ`}
        </div>
      </div>

      <div className="mcav-filters">
        <select value={team} onChange={e => setTeam(e.target.value)}>
          <option value="">ทุกทีม</option>
          {teams.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        <select value={sort} onChange={e => setSort(e.target.value)}>
          {SORTS.map(s => <option key={s.id} value={s.id}>เรียง: {s.label}</option>)}
        </select>
        {STATUS_ORDER.map(id => (
          rows.some(r => r.status === id) && (
            <button key={id} className={'chip cs-' + id + (status === id ? ' on' : '')}
              onClick={() => setStatus(s => (s === id ? '' : id))}>
              {stLabel(id, mode)} ({fmt(rows.filter(r => r.status === id).length)})
            </button>
          )
        ))}
        {filterOn && (
          <button className="chip" onClick={() => { setTeam(''); setStatus(''); setQ('') }}>
            ล้างตัวกรองทั้งหมด
          </button>
        )}
      </div>

      <div className="mcav-kpis">
        <div className="kpi k1">
          <div className="lb">{mode === 'core' ? '🧵 รายการ (ผ้า × ลูกค้า × ทีม)' : '🧵 รายการผ้า'}</div>
          <div className="vl">{fmt(kpi.items)}</div>
          <div className="ft">
            {filterOn ? 'ตามตัวกรอง'
              : (mode === 'core' ? `${fmt(kpi.codes)} รหัสผ้า ใน ${fmt(teams.length)} ทีม` : 'มีของในคลัง')}
          </div>
        </div>
        <div className="kpi k2">
          <div className="lb">📦 สต๊อกใช้ได้รวม</div>
          <div className="vl">{fmt(kpi.kg)}<span className="pc"> kg</span></div>
          <div className="ft">+ กำลังทอเข้าอีก {fmt(kpi.incoming)} kg ใน 2 สัปดาห์</div>
        </div>
        <div className="kpi k3">
          <div className="lb">🔴 ต้องผลิต (พอใช้ &lt; 2 สัปดาห์)</div>
          <div className="vl">{fmt(kpi.crit)}</div>
          <div className="ft">ของหมดเกลี้ยง {fmt(kpi.empty)} รายการ · ต้องเติมอีก ~{fmt(kpi.need)} kg</div>
        </div>
        <div className="kpi k4">
          <div className="lb">🔵 สต๊อกล้น (เกิน MAX)</div>
          <div className="vl">{fmt(kpi.over)}</div>
          <div className="ft">เกินเพดานรวม {fmt(kpi.overKg)} kg</div>
        </div>
      </div>

      <div className="otd-grid2">
        <div className="mcav-card">
          <div className="hd">
            <div>
              <h2>ระดับสต๊อกรายไอเทม</h2>
              <div className="sub">
                ทั้งหมด {fmt(shown.length)} รายการ เรียงตาม “{SORTS.find(s => s.id === sort)?.label}” ·
                เลื่อนดูในกรอบ · คลิกแท่ง = ค้นหาเฉพาะตัวนั้น
                {mode === 'core' && ' · แท่งหนาใต้กลุ่ม = ยอดรวมทุกทีม core ของผ้านั้น เทียบ MIN/MAX ที่บวกกันแล้ว'}
              </div>
            </div>
            <div className="lg">
              <select className="cs-scale" value={scale} onChange={e => setScale(e.target.value)}>
                {SCALES.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
              </select>
              <span className="lg-i"><i className="sw tot" />ของที่มี</span>
              <span className="lg-i"><i className="cs-sw-inc" />กำลังทอเข้า</span>
              <span className="lg-i"><i className="cs-sw-min" />MIN</span>
              <span className="lg-i"><i className="cs-sw-max" />MAX</span>
            </div>
          </div>
          <StockBars items={list.slice(0, barLimit)} mode={mode} scale={scale}
            setTip={setTip} onPick={setQ} />
          {shown.length > barLimit && (
            <div className="cs-more">
              <button className="tbtn" onClick={() => setBarLimit(l => l + BAR_PAGE)}>
                แสดงเพิ่ม (เหลืออีก {fmt(shown.length - barLimit)} รายการ)
              </button>
            </div>
          )}
        </div>

        <div className="mcav-card">
          <div className="hd">
            <div>
              <h2>กระจายตามสถานะ</h2>
              <div className="sub">คลิกแถบ = กรองเฉพาะกลุ่มนั้นทั้งหน้า</div>
            </div>
          </div>
          <StatusSpread rows={shown} mode={mode} setTip={setTip} sel={status}
            onSel={id => setStatus(s => (s === id ? '' : id))} />
        </div>
      </div>

      <div className="mcav-card">
        <div className="hd">
          <div>
            <h2>รายการสต๊อก</h2>
            <div className="sub">
              {mode === 'core' && 'แถวหนา “รวม N ทีม” = ยอดรวมทุกทีม core ของผ้านั้น · '}
              สต๊อกใช้ได้ = ยอดคงเหลือหลังหักที่ถูกจองแล้ว (ล็อต ≥ 100 kg และ QA ว่าง)
              {' · '}ค้นหาได้ด้วยรหัสผ้า / โครงสร้าง / ด้าย / กลุ่มเครื่อง
              {src.booking?.file ? ` · งานที่จะทอเข้าอ่านจาก ${src.booking.file}` : ''}
            </div>
          </div>
          <input className="otd-search" value={q} onChange={e => { setQ(e.target.value); setLimit(PAGE); setBarLimit(BAR_PAGE) }}
            placeholder="ค้นหา…" />
        </div>
        <StockTable items={list.slice(0, limit)} mode={mode} weeks={weeks} setTip={setTip} />
        {shown.length > limit && (
          <div className="cs-more">
            <button className="tbtn" onClick={() => setLimit(l => l + PAGE)}>
              แสดงเพิ่ม (เหลืออีก {fmt(shown.length - limit)} รายการ)
            </button>
          </div>
        )}
        {d?.truncated > 0 && (
          <div className="mcav-empty sm">ข้อมูลถูกตัดที่ {fmt(d.rows.length)} รายการ — ยังมีอีก {fmt(d.truncated)} รายการ</div>
        )}
      </div>
    </div>
  )
}

import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import {
  THEME_KEY, TIER, tierOf, fmt, pctFmt, fmtTime,
  Tip, WeekBars, CatFilter, Heatmap,
} from './McAvailability.jsx'

/* Dashboard › Outsource Availability — เครื่องของผู้รับจ้างทอที่เหลือรายสัปดาห์
   หน้าตาเดียวกับ Machine Availability แต่ตัวเลขมาจากคนละที่:
     เครื่องทั้งหมด = Total Mc ของแต่ละแถวในชีท MC S9 (1 แถว = เครื่องของผู้รับจ้าง 1 เจ้า เพดานแข็ง)
     ใช้ไป         = งานจ้างทอในแผนล่าสุด (ACTUAL_MC ต่อ S9_POOL) — booking ไม่มีข้อมูลว่าจ้างเจ้าไหน
   ไม่มีขั้นคำนวณ: backend อ่านไฟล์แผนสด ๆ → แค่โหลดใหม่เมื่อมีคนรันแผน/ลาก Gantt/แก้ชีท MC S9 */

const MODES = [
  { id: 'pct', label: '% ใช้เครื่อง' },
  { id: 'remain', label: 'เครื่องเหลือ' },
  { id: 'used', label: 'ใช้ไป' },
]

/* ---------- การ์ดรายละเอียดของช่องที่คลิก (งานจ้างทอของเจ้านั้นในสัปดาห์นั้น) ---------- */
function DetailCard({ row, week, year, jobs, onClose }) {
  useEffect(() => {
    const esc = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', esc)
    return () => window.removeEventListener('keydown', esc)
  }, [onClose])

  const c = row.cells[week] || { total: row.total, used: 0, remain: row.total }
  const pct = c.total ? (c.used / c.total) * 100 : null
  const tier = tierOf(pct) || 'ok'
  const kg = jobs.reduce((s, j) => s + j.kp, 0)

  return (
    <div className="mcav-modal" onClick={onClose}>
      <div className="mcav-modal-box" onClick={e => e.stopPropagation()}>
        <div className="dt-head">
          <div>
            <h3>{row.cat} · W{String(week).split('@')[0]}{year ? `/${year}` : ''}</h3>
            <div className="sub">
              {row.supp || '-'}{row.pool_label ? ' · ' + row.pool_label : ''} ·
              {' '}ใช้ {fmt(c.used)} จาก {fmt(c.total)} เครื่อง · เหลือ {fmt(c.remain)} ·
              {' '}{fmt(jobs.length)} รายการ · {fmt(kg)} kg
            </div>
          </div>
          <div className="dt-pct">
            <div className={'big tier-' + tier}>{pctFmt(pct)}</div>
            <div className="cap">
              {row.missing ? '⚠ แผนอ้างเครื่องชุดนี้ แต่ไม่มีในชีท MC S9 แล้ว'
                : c.remain < 0 ? '⚠ จ้างเกินเครื่องที่ผู้รับจ้างมี — ต้องเลื่อนงานออก'
                  : pct >= TIER.crit ? '⚠ เกือบเต็ม'
                    : pct >= TIER.warn ? 'เริ่มแน่น' : 'ยังว่าง'}
            </div>
          </div>
          <button className="dt-close" onClick={onClose} title="ปิด (Esc)">✕</button>
        </div>

        <div className="dt-body">
          <div className="dt-sec">งานจ้างทอในแผนที่ลงเครื่องชุดนี้สัปดาห์นี้</div>
          <div className="dt-table-wrap">
            <table className="dt-table">
              <thead>
                <tr>
                  <th>ITEM</th><th>SO</th><th>ทีม</th><th>ลูกค้า</th><th>ผ้า</th>
                  <th className="n">kg</th><th className="n">CAP/วัน</th><th className="n">เครื่อง</th>
                  <th>เป้าทอ</th><th>ที่มา</th><th>หมายเหตุ</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((j, n) => (
                  <tr key={n}>
                    <td className="mono">{j.item}</td>
                    <td className="mono">{j.so}</td>
                    <td>{j.team}</td>
                    <td className="desc" title={j.customer}>{j.customer}</td>
                    <td>{j.material}</td>
                    <td className="n">{fmt(j.kp)}</td>
                    <td className="n" title="CAP ต่อเครื่องต่อวันของผ้าตัวนี้ (ชีท Item S9 / S9 Only)">{fmt(j.cap)}</td>
                    <td className="n b" title={`เครื่องใหม่ ${j.new} · ทอต่อจากสัปดาห์ก่อน ${j.carry}`}>
                      {j.mc}<span className="raw"> (ใหม่ {j.new})</span>
                    </td>
                    <td>{j.target ? `W${j.target}` : '—'}</td>
                    <td>{j.forced ? 'สั่งเอง' : 'ระบบส่ง'}</td>
                    <td className="desc" title={j.remark}>{j.remark}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {!jobs.length && <div className="mcav-empty sm">สัปดาห์นี้ยังไม่มีงานจ้างทอที่ลงเครื่องชุดนี้ — ว่างทั้ง {fmt(c.total)} เครื่อง</div>}
          </div>
        </div>
      </div>
    </div>
  )
}

/* ---------- หน้าหลัก ---------- */
export default function S9Availability() {
  const [theme, setTheme] = useState(() => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'))
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [mode, setMode] = useState('pct')
  const [supp, setSupp] = useState('')
  const [cats, setCats] = useState([])          // CAT+เกจ ที่ติ๊กไว้ (ว่าง = ทุกชนิด)
  const [tightOnly, setTightOnly] = useState(false)
  const [selWeek, setSelWeek] = useState(null)
  // ช่วงสัปดาห์ที่แสดง — index ของ array weeks (รายการข้ามปีได้ W41→W53→W1)
  const [rFrom, setRFrom] = useState(null)
  const [rTo, setRTo] = useState(null)
  const [mounted, setMounted] = useState(false)
  const [tip, setTip] = useState(null)
  const [detail, setDetail] = useState(null)        // { key, week } ของช่องที่คลิก

  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])

  async function load() {
    setLoading(true)
    try { setData(await api.s9Ava()); setErr('') }
    catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => {
    load()
    const t = setTimeout(() => setMounted(true), 60)   // ให้แท่งกราฟไต่ขึ้นตอนเปิดหน้า
    // ตัวเลขเปลี่ยนได้ 3 ทาง: รันแผนใหม่ / มีคนลากหรือแก้แผน / แก้เครื่องในชีท MC S9
    const offs = [
      live.subscribe(live.RES.run, load),
      live.subscribe(live.RES.plan('PLAN'), load),
      live.subscribe(live.RES.master('MasterMC', 'MC S9'), load),
    ]
    return () => { clearTimeout(t); offs.forEach(off => off()) }
  }, [])

  const weeks = data?.weeks || []

  const viewWeeks = useMemo(() => {
    if (rFrom == null && rTo == null) return weeks
    const a = rFrom ?? 0, b = rTo ?? weeks.length - 1
    return weeks.slice(Math.min(a, b), Math.max(a, b) + 1)
  }, [weeks, rFrom, rTo])

  const wkNum = (w) => String(w).split('@')[0]
  const wkLabel = (w) => `W${wkNum(w)}${data?.years?.[w] ? ' · ' + data.years[w] : ''}`
  const rangeOn = rFrom != null || rTo != null

  function setRange(a, b) {
    if (a != null && b != null && a > b) { const t = a; a = b; b = t }
    setRFrom(a); setRTo(b)
  }
  function pickWeek(w, e) {
    const i = weeks.indexOf(w)
    if (e?.shiftKey && i >= 0) {
      const anchor = selWeek != null ? weeks.indexOf(selWeek) : (rFrom ?? weeks.indexOf(viewWeeks[0]))
      if (anchor >= 0) { setRange(anchor, i); setSelWeek(null); return }
    }
    setSelWeek(s => (s === w ? null : w))
  }

  useEffect(() => {
    if (selWeek != null && !viewWeeks.includes(selWeek)) setSelWeek(null)
  }, [viewWeeks, selWeek])

  const supps = useMemo(() => {
    const s = new Set((data?.rows || []).map(r => r.supp).filter(Boolean))
    return [...s].sort((a, b) => a.localeCompare(b, 'th'))
  }, [data])

  const catOptions = useMemo(() => {
    const m = new Map()
    for (const r of data?.rows || []) {
      if (supp && r.supp !== supp) continue
      for (const c of r.cats || []) m.set(c, (m.get(c) || 0) + 1)
    }
    return [...m.entries()].map(([cat, n]) => ({ cat, n }))
      .sort((a, b) => a.cat.localeCompare(b.cat, 'en', { numeric: true }))
  }, [data, supp])

  // เปลี่ยนผู้รับจ้าง → ตัด CAT ที่เลือกไว้แต่เจ้านี้ไม่มีทิ้ง (ไม่งั้นตารางว่างโดยไม่รู้สาเหตุ)
  useEffect(() => {
    if (!cats.length) return
    const ok = new Set(catOptions.map(o => o.cat))
    const keep = cats.filter(c => ok.has(c))
    if (keep.length !== cats.length) setCats(keep)
  }, [catOptions])

  const rows = useMemo(() => {
    const pick = new Set(cats)
    return (data?.rows || []).filter(r => {
      if (supp && r.supp !== supp) return false
      if (pick.size && !(r.cats || []).some(c => pick.has(c))) return false
      if (tightOnly) {
        const hot = viewWeeks.some(w => {
          const c = r.cells[w]
          return c && (c.total ? (c.used / c.total) * 100 >= TIER.warn : c.used > 0)
        })
        if (!hot) return false
      }
      return true
    })
  }, [data, supp, cats, tightOnly, viewWeeks])

  // ยอดรวมต่อสัปดาห์ของแถวที่แสดงอยู่ — แถวละเจ้า ไม่ซ้ำกัน บวกตรง ๆ ได้
  const agg = useMemo(() => {
    const out = {}
    for (const w of viewWeeks) {
      let total = 0, used = 0, remain = 0
      for (const r of rows) {
        const c = r.cells[w]
        if (!c) continue
        total += c.total; used += c.used; remain += c.remain
      }
      out[w] = { total, used, remain, pct: total ? (used / total) * 100 : null }
    }
    return out
  }, [rows, viewWeeks])

  // KPI — คลิกสัปดาห์บนกราฟ = คิดเฉพาะสัปดาห์นั้น
  const kpi = useMemo(() => {
    const vw = selWeek && agg[selWeek] ? [selWeek] : viewWeeks
    const vset = new Set(vw)
    const keys = new Set(rows.map(r => r.key))
    let hotGroups = 0, fullCells = 0, overCells = 0, mcWeeks = 0
    for (const r of rows) {
      let hot = false
      for (const w of vw) {
        const c = r.cells[w]
        if (!c) continue
        mcWeeks += c.used
        if (c.used > 0 && c.remain <= 0) overCells++
        const p = c.total ? (c.used / c.total) * 100 : (c.used > 0 ? Infinity : 0)
        if (p >= TIER.crit) { hot = true; fullCells++ }
        else if (p >= TIER.warn) hot = true
      }
      if (hot) hotGroups++
    }
    const kg = (data?.jobs || []).reduce((s, j) => (keys.has(j.pool) && vset.has(j.week) ? s + j.kp : s), 0)
    return {
      supps: new Set(rows.map(r => r.supp).filter(Boolean)).size,
      pools: rows.length,
      machines: rows.reduce((s, r) => s + (r.total || 0), 0),
      hotGroups, fullCells, overCells, mcWeeks, kg,
      scope: selWeek && agg[selWeek]
        ? `W${wkNum(selWeek)}`
        : rangeOn && viewWeeks.length
          ? `W${wkNum(viewWeeks[0])}–W${wkNum(viewWeeks[viewWeeks.length - 1])} · ${viewWeeks.length} สัปดาห์`
          : `${viewWeeks.length} สัปดาห์`,
    }
  }, [rows, viewWeeks, agg, selWeek, rangeOn, data])

  const detailRow = detail ? (data?.rows || []).find(r => r.key === detail.key) : null
  const detailJobs = useMemo(() => (detail
    ? (data?.jobs || []).filter(j => j.pool === detail.key && j.week === detail.week)
    : []), [detail, data])

  const plan = data?.plan
  const empty = !data?.ok || !data.rows?.length

  return (
    <div className={'mcav-dash ' + theme} onMouseLeave={() => setTip(null)}>
      <Tip tip={tip} />

      <div className="mcav-top">
        <div className="mcav-title">
          <h1>Outsource Availability<span className={'live' + (loading ? ' run' : '')}>{loading ? 'กำลังโหลด' : 'LIVE'}</span></h1>
          <div className="sub">
            เครื่องของผู้รับจ้างทอ (ชีท MC S9) − ที่แผนล่าสุดส่งไปจ้างทอ · 1 แถว = เครื่องของผู้รับจ้าง 1 เจ้า
            (จ้างหลายเจ้ารวมกันได้ แต่ห้ามเกินเครื่องของแต่ละเจ้า)
          </div>
        </div>
        <div className="mcav-topright">
          <div className="clock">
            <b>{fmtTime(plan?.run_at || plan?.mtime)}</b>
            <span>แผน: {plan ? `${plan.name} · แก้ล่าสุด ${fmtTime(plan.mtime)}` : 'ไม่พบไฟล์'}</span>
          </div>
          <button className="tbtn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☀ ธีมสว่าง' : '🌙 ธีมมืด'}
          </button>
          <button className="tbtn go" onClick={load} disabled={loading}
            title="อ่านแผนล่าสุดใหม่ — ปกติหน้านี้อัปเดตเองเมื่อมีคนรันแผนหรือแก้แผน">
            {loading ? '⏳ กำลังโหลด' : '↻ โหลดใหม่'}
          </button>
        </div>
      </div>

      {err && (
        <div className="mcav-alerts"><span className="al bad">{err}</span></div>
      )}

      {empty ? (
        <div className="mcav-empty">{data ? (data.reason || 'ยังไม่มีข้อมูล') : 'กำลังโหลด...'}</div>
      ) : (
        <>
          <div className="mcav-filters">
            <select value={supp} onChange={e => setSupp(e.target.value)}>
              <option value="">ทุกผู้รับจ้าง</option>
              {supps.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <CatFilter options={catOptions} value={cats} onChange={setCats} />
            <div className="mcav-range">
              <span className="lb">สัปดาห์</span>
              <select value={rFrom ?? ''} onChange={e => setRange(e.target.value === '' ? null : +e.target.value, rTo)}>
                <option value="">ตั้งแต่สัปดาห์</option>
                {weeks.map((w, i) => <option key={w} value={i}>{wkLabel(w)}</option>)}
              </select>
              <span className="dash">–</span>
              <select value={rTo ?? ''} onChange={e => setRange(rFrom, e.target.value === '' ? null : +e.target.value)}>
                <option value="">ถึงสัปดาห์</option>
                {weeks.map((w, i) => <option key={w} value={i}>{wkLabel(w)}</option>)}
              </select>
            </div>
            <button className={'chip' + (tightOnly ? ' on' : '')} onClick={() => setTightOnly(v => !v)}>
              ⚠ เฉพาะ MC Used ≥70%
            </button>
            {rangeOn && !!viewWeeks.length && (
              <button className="chip on" onClick={() => setRange(null, null)} title="ล้างช่วงสัปดาห์">
                W{wkNum(viewWeeks[0])}–W{wkNum(viewWeeks[viewWeeks.length - 1])} ✕
              </button>
            )}
            {selWeek && (
              <button className="chip on" onClick={() => setSelWeek(null)}>W{wkNum(selWeek)} ✕</button>
            )}
          </div>

          <div className="mcav-kpis">
            <div className="kpi k1">
              <div className="lb">ผู้รับจ้าง</div>
              <div className="vl">{kpi.supps}</div>
              <div className="ft">{kpi.pools} ชุดเครื่อง · {kpi.scope}</div>
            </div>
            <div className="kpi k2">
              <div className="lb">เครื่องทั้งหมด</div>
              <div className="vl">{fmt(kpi.machines)}</div>
              <div className="ft">จ้างไปรวม {fmt(kpi.mcWeeks)} เครื่อง·สัปดาห์ · {fmt(kpi.kg)} kg</div>
            </div>
            <div className="kpi k3">
              <div className="lb">⚠ ช่องที่เครื่องหมด</div>
              <div className="vl">{kpi.overCells}</div>
              <div className="ft">เจ้า × สัปดาห์ ที่ใช้ครบ/เกินเครื่องของเจ้านั้น · {kpi.scope}</div>
            </div>
            <div className="kpi k4">
              <div className="lb">⚠ MC Used ≥70%</div>
              <div className="vl">{kpi.hotGroups}</div>
              <div className="ft">มี {kpi.fullCells} ช่องที่เกิน 90%</div>
            </div>
          </div>

          <div className="mcav-card">
            <div className="hd">
              <div>
                <h2>เครื่องผู้รับจ้างที่ใช้ไป เทียบเครื่องทั้งหมด — ต่อสัปดาห์</h2>
                <div className="sub">รวมทุกเจ้าที่เลือก · สีแท่งตาม % ใช้เครื่อง (70/90) · คลิกคอลัมน์เพื่อ filter · Shift+คลิก = เลือกเป็นช่วง</div>
              </div>
              <div className="lg">
                <span className="lg-i"><i className="sw ok" />ยังว่าง &lt; 70%</span>
                <span className="lg-i"><i className="sw warn" />เริ่มแน่น 70–90%</span>
                <span className="lg-i"><i className="sw crit" />⚠ เกือบเต็ม &gt; 90%</span>
                <span className="lg-i"><i className="sw tot" />เครื่องทั้งหมด</span>
              </div>
            </div>
            <WeekBars weeks={viewWeeks} agg={agg} years={data.years} mounted={mounted}
              selected={selWeek} onSelect={pickWeek} setTip={setTip} />
          </div>

          <div className="mcav-card">
            <div className="hd">
              <div>
                <h2>% ใช้เครื่อง รายผู้รับจ้าง × สัปดาห์</h2>
                <div className="sub">
                  % = เครื่องที่แผนส่งไปจ้างทอ ÷ เครื่องของเจ้านั้น · <b>กดที่ช่อง = ดูงานจ้างทอในช่องนั้น</b> · กดชื่อสัปดาห์ = filter (Shift+คลิก = ช่วง)
                </div>
              </div>
              <div className="modes">
                {MODES.map(m => (
                  <button key={m.id} className={'chip' + (mode === m.id ? ' on' : '')}
                    onClick={() => setMode(m.id)}>{m.label}</button>
                ))}
              </div>
            </div>
            <Heatmap rows={rows} weeks={viewWeeks} mode={mode} nameHead="เครื่องผู้รับจ้าง"
              selected={selWeek} onSelect={pickWeek}
              onOpen={(r, w) => setDetail({ key: r.key, week: w })} setTip={setTip} />
            {!rows.length && <div className="mcav-empty sm">ไม่มีเครื่องผู้รับจ้างที่ตรงกับตัวกรอง</div>}
          </div>
        </>
      )}

      {detail && detailRow && (
        <DetailCard row={detailRow} week={detail.week} year={data?.years?.[detail.week]}
          jobs={detailJobs} onClose={() => setDetail(null)} />
      )}
    </div>
  )
}

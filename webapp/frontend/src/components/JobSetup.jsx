import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'

/* Dashboard โควตา Job Setup — จำนวนงานตั้งเครื่องที่ booking ใช้ไปแล้ว เทียบโควตา job/สัปดาห์
   ใช้ธีม/คลาสชุดเดียวกับหน้าเครื่องว่าง (.mcav-*) เพื่อให้สองหน้าหน้าตาเป็นชุดเดียวกัน
   ตัวเลขมาจากชีท DETAIL ของไฟล์ผลลัพธ์ AVA_MC ใบเดียวกัน → ปุ่ม "คำนวณใหม่" ใช้ตัวเดียวกัน */

const THEME_KEY = 'mcav-theme'          // ใช้ค่าธีมร่วมกับหน้าเครื่องว่าง (สลับหน้าแล้วธีมไม่เด้ง)
const OPEN_KEY = 'jset-open'            // ประเภทที่กางดูกลุ่มเครื่องย่อยค้างไว้

// เกณฑ์สี: <70% ยังว่าง · 70–100% เริ่มแน่น · >100% = ใช้เกินโควตาที่ระบบอนุญาต
const TIER = { warn: 70, crit: 100 }
const tierOf = (pct) => (pct == null ? null : pct > TIER.crit ? 'crit' : pct >= TIER.warn ? 'warn' : 'ok')

const MODES = [
  { id: 'used', label: 'job ที่ใช้ไป' },
  { id: 'pct', label: '% ของโควตา' },
  { id: 'left', label: 'โควตาที่เหลือ' },
]

const fmt = (n) => Math.round(n).toLocaleString()
const pctFmt = (p) => (p == null ? '—' : Math.round(p) + '%')

function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}
function fmtSize(b) {
  if (b == null) return '-'
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}

/* ---------- tooltip ลอยตามเมาส์ (เหมือนหน้าเครื่องว่าง) ---------- */
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
const tipPos = (e) => ({ x: Math.min(e.clientX + 14, window.innerWidth - 280), y: e.clientY + 16 })

/* ---------- ตารางโควตา: ประเภทโรงงาน × สัปดาห์ ---------- */
function QuotaTable({ types, weeks, years, mode, open, onToggle, sel, onPick, setTip }) {
  const cellText = (c, cap) => {
    if (!c || !c.jobs) return ''
    if (mode === 'pct') return cap ? Math.round((c.jobs / cap) * 100) + '%' : '—'
    if (mode === 'left') return cap ? cap - c.jobs : '—'
    // โควตาย้ายไปโชว์เป็นป้ายหน้าแถวแล้ว → ในช่องเหลือแค่จำนวน job ที่ใช้ไป อ่านง่ายกว่า
    return c.jobs
  }

  const show = (e, name, w, c, cap) => {
    if (!c) return setTip(null)
    const pct = cap ? (c.jobs / cap) * 100 : null
    setTip({
      ...tipPos(e),
      title: `${name} · สัปดาห์ ${w}${years?.[w] ? ` (ปี ${years[w]})` : ''}`,
      lines: [
        { swatch: `var(--d-${tierOf(pct) || 'ok'})`, text: `ใช้ไป ${c.jobs} job${cap ? ` จากโควตา ${cap}` : ''} (${pctFmt(pct)})` },
        { swatch: 'var(--d-free)', text: cap ? `เหลือ ${cap - c.jobs} job` : 'ไม่มีโควตากำหนดไว้' },
        { text: `setup เครื่องใหม่ ${c.setup} · เพิ่มเครื่องของงานเดิม ${c.inc}` },
        { text: `${c.rows} รายการ — คลิกช่องเพื่อดูรายละเอียด` },
      ],
    })
  }

  const cellCls = (c, cap) => {
    if (!c || !c.jobs) return 'hm-none'
    const pct = cap ? (c.jobs / cap) * 100 : null
    return 'hm-' + (tierOf(pct) || 'ok')
  }

  return (
    <div className="mcav-hm-wrap">
      <table className="mcav-hm jset-hm">
        <thead>
          <tr>
            <th className="c-name">ประเภทโรงงาน</th>
            {weeks.map(w => (
              <th key={w} className={'c-wk' + (sel && sel.week === w ? ' on' : '')}>W{w}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {types.map(t => {
            const isOpen = !!open[t.type]
            return (
              <React.Fragment key={t.type}>
                <tr>
                  <th className="c-name">
                    <button className="jset-exp" onClick={() => onToggle(t.type)}
                      title={isOpen ? 'ย่อกลุ่มเครื่อง' : 'กางดูรายกลุ่มเครื่อง'}>
                      {isOpen ? '▾' : '▸'}
                    </button>
                    <span className="nm">{t.label}</span>
                    <span className={'jset-cap' + (t.cap ? '' : ' none')}
                      title={t.cap ? `โควตา ${t.cap} job/สัปดาห์` : 'ประเภทนี้ไม่ได้กำหนดโควตาไว้'}>
                      {t.cap ? <><b>{t.cap}</b> job/สัปดาห์</> : 'ไม่มีโควตา'}
                    </span>
                    <span className="sub">{t.groups.length} กลุ่มเครื่อง</span>
                  </th>
                  {weeks.map(w => {
                    const c = t.cells[w]
                    const on = sel && sel.type === t.type && sel.week === w
                    return (
                      <td key={w}
                        className={cellCls(c, t.cap) + (on ? ' pick' : '')}
                        style={{ cursor: c ? 'pointer' : 'default' }}
                        onMouseMove={e => show(e, t.label, w, c, t.cap)}
                        onMouseLeave={() => setTip(null)}
                        onClick={() => c && onPick(t.type, w)}>
                        {cellText(c, t.cap)}
                      </td>
                    )
                  })}
                </tr>
                {isOpen && t.groups.map(g => (
                  <tr key={t.type + '|' + g.mcg} className="jset-sub">
                    <th className="c-name">
                      <span className="nm sm">↳ {g.mcg}</span>
                    </th>
                    {weeks.map(w => {
                      const c = g.cells[w]
                      // แถวย่อยไม่มีโควตาของตัวเอง (โควตาใช้ร่วมกันทั้งประเภท) → โชว์จำนวน job ล้วน
                      return (
                        <td key={w} className={c ? 'jset-g' : 'hm-none'}
                          onMouseMove={e => show(e, `${t.label} · ${g.mcg}`, w, c, t.cap)}
                          onMouseLeave={() => setTip(null)}>
                          {c ? c.jobs : ''}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </React.Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

/* ---------- หน้าหลัก ---------- */
export default function JobSetup() {
  const [theme, setTheme] = useState(() => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'))
  const [data, setData] = useState(null)
  const [status, setStatus] = useState({})
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [mode, setMode] = useState('used')
  const [fromNow, setFromNow] = useState(true)      // ตัดสัปดาห์ที่ผ่านมาแล้วออก (แบบเดียวกับที่ Planning นับโควตา)
  const [hotOnly, setHotOnly] = useState(false)
  const [search, setSearch] = useState('')
  const [sel, setSel] = useState(null)              // ช่องที่คลิก { type, week }
  const [open, setOpen] = useState(() => {
    try { return JSON.parse(localStorage.getItem(OPEN_KEY)) || {} } catch { return {} }
  })
  const [tip, setTip] = useState(null)
  const wasRunning = useRef(false)

  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])
  useEffect(() => { localStorage.setItem(OPEN_KEY, JSON.stringify(open)) }, [open])

  async function load() {
    try {
      const d = await api.jobSetup()
      setData(d); setErr('')
      if (d.auto_started) { wasRunning.current = true; loadStatus() }
    } catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
  }
  async function loadStatus() {
    try { setStatus(await api.mcAvaStatus()) } catch { }
  }

  useEffect(() => {
    load(); loadStatus()
    const iv = setInterval(loadStatus, 2000)
    return () => clearInterval(iv)
  }, [])

  // คำนวณ (AVA_MC) จบเมื่อไหร่ → โหลดตัวเลขใหม่เอง ไม่ต้องให้ผู้ใช้รีเฟรช
  useEffect(() => {
    if (wasRunning.current && !status.running) {
      load()
      if (status.error) setErr('คำนวณไม่สำเร็จ: ' + status.error)
    }
    wasRunning.current = !!status.running
  }, [status.running])

  async function refresh() {
    setMsg(''); setErr('')
    try {
      const r = await api.mcAvaRefresh()
      setMsg(r.message)
      if (r.ok) { wasRunning.current = true; loadStatus() }
    } catch (e) { setErr('สั่งคำนวณไม่ได้: ' + e.message) }
  }

  const years = data?.years || {}
  const allWeeks = data?.weeks || []

  // backend ส่ง weeks มาเรียงตามเวลาจริงแล้ว (จัดการข้ามปีให้) + today_index = ตำแหน่งสัปดาห์นี้
  // → ตัดสัปดาห์ที่ผ่านมาแล้วด้วยการ slice เท่านั้น ไม่ต้องเดาปีของแต่ละสัปดาห์เองที่นี่
  const weeks = useMemo(
    () => (fromNow ? allWeeks.slice(data?.today_index || 0) : allWeeks),
    [allWeeks, fromNow, data])

  const types = useMemo(() => {
    const src = data?.types || []
    if (!hotOnly) return src
    return src.filter(t => weeks.some(w => {
      const c = t.cells[w]
      return c && t.cap && (c.jobs / t.cap) * 100 >= TIER.warn
    }))
  }, [data, weeks, hotOnly])

  // KPI: นับจากช่องที่แสดงอยู่จริง (ตาม filter สัปดาห์) เท่านั้น
  const kpi = useMemo(() => {
    let jobs = 0, setup = 0, inc = 0, over = 0, tight = 0, full = 0
    for (const t of types) {
      for (const w of weeks) {
        const c = t.cells[w]
        if (!c) continue
        jobs += c.jobs; setup += c.setup; inc += c.inc
        if (!t.cap) continue
        const p = (c.jobs / t.cap) * 100
        if (p > TIER.crit) over++
        else if (p === TIER.crit) full++
        else if (p >= TIER.warn) tight++
      }
    }
    return { jobs, setup, inc, over, tight, full, weeks: weeks.length }
  }, [types, weeks])

  // รายการ job ของช่องที่คลิก (+ ค้นหา) — backend ส่งมาทั้งก้อนแล้ว จึงกรองในเบราว์เซอร์ได้เลย
  const picked = useMemo(() => {
    if (!sel) return []
    const q = search.trim().toLowerCase()
    return (data?.jobs || []).filter(j =>
      j.type === sel.type && j.week === sel.week &&
      (!q || (j.item + ' ' + j.so + ' ' + j.mcg + ' ' + j.team + ' ' + j.desc).toLowerCase().includes(q)))
  }, [data, sel, search])

  const selType = sel ? (data?.types || []).find(t => t.type === sel.type) : null
  const selCell = selType ? selType.cells[sel.week] : null

  const bk = data?.booking || status.booking
  const running = !!status.running
  const empty = !data?.ok || !data.types?.length

  return (
    <div className={'mcav-dash ' + theme} onMouseLeave={() => setTip(null)}>
      <Tip tip={tip} />

      <div className="mcav-top">
        <div className="mcav-title">
          <h1>โควตา Job Setup รายสัปดาห์<span className={'live' + (running ? ' run' : '')}>{running ? 'กำลังคำนวณ' : 'LIVE'}</span></h1>
          <div className="sub">
            งาน Setup (job) ที่ booking ใช้ไปแล้ว เทียบโควตาต่อสัปดาห์ของแต่ละประเภทโรงงาน ·
            นับ "setup เครื่องใหม่" ทั้งชุด + "เพิ่มเครื่อง" ของงานเดิม ด้วยสูตรเดียวกับ Planning ·
            <b> ยังไม่รวมงานใหม่ที่แผนจะวางเพิ่ม</b>
            {data?.source === 'pipeline' && ' · ตัวเลขชุดนี้มาจากผลรันแผนรอบล่าสุด'}
          </div>
        </div>
        <div className="mcav-topright">
          <div className="clock">
            <b>{fmtTime(data?.computed_at)}</b>
            <span>booking: {bk ? `${fmtTime(bk.mtime)} · ${fmtSize(bk.size)}` : 'ไม่พบไฟล์'}</span>
          </div>
          <button className="tbtn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☀ ธีมสว่าง' : '🌙 ธีมมืด'}
          </button>
          <button className="tbtn go" onClick={refresh} disabled={running}>
            {running ? '⏳ กำลังคำนวณ' : '↻ คำนวณใหม่'}
          </button>
        </div>
      </div>

      {(data?.stale || msg || err) && (
        <div className="mcav-alerts">
          {data?.stale && (
            <span className="al warn">
              {data.auto_started || running
                ? 'booking ใหม่กว่าตัวเลขที่แสดง — กำลังคำนวณให้อัตโนมัติ'
                : 'booking ใหม่กว่าตัวเลขที่แสดง — กดคำนวณใหม่เพื่ออัปเดต'}
            </span>
          )}
          {data?.ok && data.has_flags === false && (
            <span className="al warn">
              ไฟล์ผลลัพธ์รุ่นเก่าไม่มีธง _is_new_setup — ตัวเลขนับ MC_USE_CEIL ทั้งหมด (สูงกว่าจริง) กดคำนวณใหม่เพื่อได้ไฟล์รุ่นใหม่
            </span>
          )}
          {msg && <span className="al info">{msg}</span>}
          {err && <span className="al bad">{err}</span>}
        </div>
      )}

      {empty ? (
        <div className="mcav-empty">{data?.reason || 'ยังไม่มีข้อมูล'}</div>
      ) : (
        <>
          <div className="mcav-filters">
            <button className={'chip' + (fromNow ? ' on' : '')} onClick={() => setFromNow(v => !v)}>
              📅 ตั้งแต่ W{data.today_week} (สัปดาห์นี้)
            </button>
            <button className={'chip' + (hotOnly ? ' on' : '')} onClick={() => setHotOnly(v => !v)}>
              ⚠ เฉพาะประเภทที่ตึง ≥70%
            </button>
            {sel && (
              <button className="chip on" onClick={() => setSel(null)}>
                {selType?.label} · W{sel.week} ✕
              </button>
            )}
            <div className="modes">
              {MODES.map(m => (
                <button key={m.id} className={'chip' + (mode === m.id ? ' on' : '')}
                  onClick={() => setMode(m.id)}>{m.label}</button>
              ))}
            </div>
          </div>

          <div className="mcav-kpis">
            <div className="kpi k1">
              <div className="lb">job Setup รวม</div>
              <div className="vl">{fmt(kpi.jobs)}</div>
              <div className="ft">ตั้งใหม่ {fmt(kpi.setup)} · เพิ่มเครื่อง {fmt(kpi.inc)} · {kpi.weeks} สัปดาห์</div>
            </div>
            <div className="kpi k3">
              <div className="lb">⚠ ช่องที่ใช้เกินโควตา</div>
              <div className="vl">{kpi.over}</div>
              <div className="ft">booking จองเกินเพดาน job/สัปดาห์ไปแล้ว</div>
            </div>
            <div className="kpi k4">
              <div className="lb">ช่องที่เต็มพอดี</div>
              <div className="vl">{kpi.full}</div>
              <div className="ft">ใช้ครบโควตา — แผนจะเพิ่ม setup ในช่องนี้ไม่ได้อีก</div>
            </div>
            <div className="kpi k2">
              <div className="lb">ช่องที่เริ่มแน่น 70–100%</div>
              <div className="vl">{kpi.tight}</div>
              <div className="ft">ยังเหลือโควตา แต่ใกล้เต็มแล้ว</div>
            </div>
          </div>

          <div className="mcav-card">
            <div className="hd">
              <div>
                <h2>job ที่ใช้ไป เทียบโควตา — ประเภทโรงงาน × สัปดาห์</h2>
                <div className="sub">
                  คลิกช่องเพื่อดูว่าโควตาถูกใช้ไปกับงานไหนบ้าง · กด ▸ หน้าชื่อประเภทเพื่อกางดูรายกลุ่มเครื่อง
                  (กลุ่มเครื่องในประเภทเดียวกันใช้โควตาก้อนเดียวกัน)
                </div>
              </div>
              <div className="lg">
                <span className="lg-i"><i className="sw ok" />ยังว่าง &lt; 70%</span>
                <span className="lg-i"><i className="sw warn" />เริ่มแน่น 70–100%</span>
                <span className="lg-i"><i className="sw crit" />⚠ เกินโควตา</span>
              </div>
            </div>
            <QuotaTable types={types} weeks={weeks} years={years} mode={mode}
              open={open} onToggle={t => setOpen(o => ({ ...o, [t]: !o[t] }))}
              sel={sel} onPick={(type, week) => setSel(s => (s && s.type === type && s.week === week ? null : { type, week }))}
              setTip={setTip} />
            {!types.length && <div className="mcav-empty sm">ไม่มีประเภทที่ตรงกับตัวกรอง</div>}
          </div>

          {sel && (
            <div className="mcav-card">
              <div className="hd">
                <div>
                  <h2>{selType?.label} · สัปดาห์ {sel.week}{years[sel.week] ? ` (ปี ${years[sel.week]})` : ''}</h2>
                  <div className="sub">
                    ใช้ไป {selCell?.jobs || 0} job{selType?.cap ? ` จากโควตา ${selType.cap} (เหลือ ${selType.cap - (selCell?.jobs || 0)})` : ''}
                    {' · '}setup เครื่องใหม่ {selCell?.setup || 0} · เพิ่มเครื่องของงานเดิม {selCell?.inc || 0}
                    {' · '}{picked.length} รายการ
                  </div>
                </div>
                <input placeholder="ค้นหา item / SC / กลุ่มเครื่อง / ทีม"
                  value={search} onChange={e => setSearch(e.target.value)} />
              </div>
              <div className="jset-list">
                <table>
                  <thead>
                    <tr>
                      <th>item</th><th>รายละเอียด</th><th>SC</th><th>ทีม</th>
                      <th>กลุ่มเครื่อง</th><th>Guage</th>
                      <th className="n">job</th><th>ชนิด</th>
                      <th className="n">เครื่องสัปดาห์นี้</th><th className="n">สัปดาห์ก่อน</th>
                      <th className="n">กก.</th>
                    </tr>
                  </thead>
                  <tbody>
                    {picked.map((j, i) => (
                      <tr key={i}>
                        <td className="mono">{j.item}</td>
                        <td className="desc" title={j.desc}>{j.desc}</td>
                        <td className="mono">{j.so}</td>
                        <td>{j.team}</td>
                        <td>{j.mcg}{j.unknown_mc && <span className="jset-warn" title="ไม่พบกลุ่มเครื่องนี้ใน MasterMC — ระบบนับเป็น PHET_DOUBLE ตามค่า default ของ Planning">⚠</span>}</td>
                        <td>{j.gauge}</td>
                        <td className="n b">{j.jobs}</td>
                        <td>
                          <span className={'jset-kind ' + j.kind}>
                            {j.kind === 'setup' ? 'setup เครื่องใหม่' : 'เพิ่มเครื่อง'}
                          </span>
                        </td>
                        <td className="n">{j.mc}</td>
                        <td className="n">{j.prev || '-'}</td>
                        <td className="n">{fmt(j.qty)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!picked.length && <div className="mcav-empty sm">ไม่มีรายการที่ตรงกับคำค้น</div>}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

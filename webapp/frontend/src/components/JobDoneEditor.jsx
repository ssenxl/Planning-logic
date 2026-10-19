import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'

// มุมมอง "ตารางแยกปี" ของชีท "Job " (MasterMC) — จำนวนครั้งที่เปลี่ยนเกจจริงไปแล้ว
// ต่อ (โรง × สัปดาห์) ; Planning.py หักตัวเลขนี้ออกจากโควตาย้ายเกจของสัปดาห์นั้น
//
// รูปชีทที่หน้านี้เขียน:  Type | Year | 1 | 2 | ... | 53   (1 แถว = 1 โรง × 1 ปี)
// ชีทรุ่นเก่าไม่มี Year (หัวคอลัมน์เป็นเลขสัปดาห์เปล่า) → Planning ตีเป็น "ครั้งที่ใกล้วันนี้สุด"
// ซึ่งพอเวลาผ่านไปเกินครึ่งปี ค่า W21/2026 จะกลายเป็น W21/2027 เงียบ ๆ
// หน้านี้เดาปีให้ด้วยกติกาเดียวกัน แล้วเขียนปีลงชีทจริงทันทีที่แก้ (หรือกดปุ่มยืนยันปี)
//
// ทุกการแก้ = คำนวณทั้งชีทใหม่จากโมเดล แล้วส่งให้ Masters.replaceGrid (autosave / Ctrl+Z ชุดเดิม)

const TYPE_ORDER = ['OMNOI', 'PHET_SINGLE', 'PHET_DOUBLE']
const TYPE_LABEL = {
  OMNOI: 'โรงอ้อมน้อย',
  PHET_SINGLE: 'โรงเพชร · Single',
  PHET_DOUBLE: 'โรงเพชร · Double',
}
const TH_MON = ['ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.',
  'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.']
const MAX_WEEK = 53

const val = v => String(v ?? '').trim()
const toInt = v => {
  const s = val(v)
  if (!s) return null
  const n = Number(s)
  return Number.isFinite(n) ? Math.trunc(n) : null
}
const dmy = s => {           // "2026-09-18" → { d: 18, m: 8 }
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(val(s))
  return m ? { d: Number(m[3]), m: Number(m[2]) - 1 } : null
}
const shortDate = s => { const x = dmy(s); return x ? `${x.d} ${TH_MON[x.m]}` : '' }
const rangeText = (a, b) => {
  const x = dmy(a), y = dmy(b)
  if (!x || !y) return ''
  return x.m === y.m ? `${x.d}–${y.d} ${TH_MON[x.m]}` : `${x.d} ${TH_MON[x.m]} – ${y.d} ${TH_MON[y.m]}`
}
const todayStr = () => {
  const t = new Date()
  const p = n => String(n).padStart(2, '0')
  return `${t.getFullYear()}-${p(t.getMonth() + 1)}-${p(t.getDate())}`
}
const cellKey = (type, year, week) => `${type}|${year}|${week}`

export default function JobDoneEditor({ grid, info, replaceGrid, ro }) {
  const { typeCi, yearCi, weekCols } = info
  const [cal, setCal] = useState(null)          // { weeks: [[y, w]], dates: [[start, end]] }
  const [cfg, setCfg] = useState(null)          // { limit, enabled } จากหน้าตั้งค่าระบบ
  const [year, setYear] = useState(null)
  const scrollRef = useRef(null)
  const nowRef = useRef(null)

  useEffect(() => {
    let dead = false
    api.workday()
      .then(d => { if (!dead) setCal({ weeks: d?.cal_weeks || [], dates: d?.cal_week_dates || [] }) })
      .catch(() => { if (!dead) setCal({ weeks: [], dates: [] }) })
    api.planConfig().then(d => {
      if (dead) return
      const items = (d?.groups || []).flatMap(g => g.items || [])
      const get = k => items.find(it => it.key === k)?.value
      setCfg({ limit: toInt(get('KP_CYLINDER_CHANGE_LIMIT')), enabled: !!get('KP_CYLINDER_CHANGE') })
    }).catch(() => {})
    return () => { dead = true }
  }, [])

  // ---- ปฏิทิน: ตำแหน่งสัปดาห์ปัจจุบัน + ลำดับเวลาของทุกสัปดาห์ ----
  const timeline = useMemo(() => {
    const weeks = (cal?.weeks || []).map(([y, w]) => [Number(y), Number(w)])
    const dates = cal?.dates || []
    const t = todayStr()
    let todayIdx = dates.findIndex(d => d && d[0] <= t && t <= d[1])
    if (todayIdx < 0 && weeks.length) {
      // ไม่มีช่วงวันที่ (backend รุ่นเก่า) → เดาจากปีปัจจุบันเฉย ๆ ไม่ไฮไลต์สัปดาห์
      todayIdx = weeks.findIndex(([y]) => y === new Date().getFullYear())
    }
    return { weeks, dates, todayIdx }
  }, [cal])

  // เลขสัปดาห์เปล่า → ปีของครั้งที่ใกล้วันนี้สุด (±เท่ากัน = อนาคต) — กติกาเดียวกับ Calendar.resolve_bare
  const bareYear = w => {
    const { weeks, todayIdx } = timeline
    let best = -1
    weeks.forEach(([, ww], i) => {
      if (ww !== w) return
      if (best < 0) { best = i; return }
      const d = Math.abs(i - todayIdx), bd = Math.abs(best - todayIdx)
      if (d < bd || (d === bd && i >= todayIdx)) best = i
    })
    return best >= 0 ? weeks[best][0] : new Date().getFullYear()
  }

  // ---- โมเดล: ค่าทุกช่องจากชีท (รุ่นใหม่มีปีต่อแถว / รุ่นเก่าเดาปีต่อช่อง) ----
  const model = useMemo(() => {
    const data = new Map()
    const types = [...TYPE_ORDER]
    let legacy = 0
    grid.rows.forEach(r => {
      const type = val(r[typeCi]).toUpperCase()
      if (!type) return
      if (!types.includes(type)) types.push(type)
      const rowYear = yearCi >= 0 ? toInt(r[yearCi]) : null
      weekCols.forEach(({ ci, w }) => {
        const n = toInt(r[ci])
        if (!n) return
        const y = rowYear ?? bareYear(w)
        if (rowYear == null) legacy++
        data.set(cellKey(type, y, w), n)
      })
    })
    return { data, types, legacy }
  }, [grid, typeCi, yearCi, weekCols, timeline])

  // ปีที่มีให้เลือก = ปีในปฏิทิน + ปีที่มีข้อมูลอยู่แล้ว
  const years = useMemo(() => {
    const s = new Set(timeline.weeks.map(([y]) => y))
    model.data.forEach((_, k) => s.add(Number(k.split('|')[1])))
    if (!s.size) s.add(new Date().getFullYear())
    return [...s].sort((a, b) => a - b)
  }, [timeline, model])
  const curYear = timeline.todayIdx >= 0 && timeline.weeks[timeline.todayIdx]
    ? timeline.weeks[timeline.todayIdx][0] : new Date().getFullYear()
  const shownYear = year ?? (years.includes(curYear) ? curYear : years[years.length - 1])

  const countOfYear = y => {
    let n = 0
    model.data.forEach((v, k) => { if (Number(k.split('|')[1]) === y) n += v })
    return n
  }

  // คอลัมน์สัปดาห์ของปีที่เลือก (จากปฏิทินจริง — มี W53 เฉพาะปีที่มี)
  const cols = useMemo(() => {
    const out = []
    timeline.weeks.forEach(([y, w], i) => {
      if (y !== shownYear) return
      const d = timeline.dates[i] || []
      const state = timeline.todayIdx < 0 ? 'future'
        : i < timeline.todayIdx ? 'past' : i === timeline.todayIdx ? 'now' : 'future'
      out.push({ w, from: d[0], to: d[1], state })
    })
    if (!out.length) {
      for (let w = 1; w <= 52; w++) out.push({ w, state: 'future' })
    }
    return out
  }, [timeline, shownYear])

  // เปิดปีปัจจุบัน → เลื่อนให้เห็นสัปดาห์นี้ (เหลือสัปดาห์ก่อนหน้าให้เห็นนิดหน่อย)
  useEffect(() => {
    const box = scrollRef.current, th = nowRef.current
    if (!box) return
    if (!th) { box.scrollLeft = 0; return }
    const first = box.querySelector('th.jd-type')
    box.scrollLeft = Math.max(0, th.offsetLeft - (first?.offsetWidth || 0) - th.offsetWidth * 4)
  }, [shownYear, cols.length])

  // ---- เขียนกลับ: โมเดล → ชีท Type | Year | 1..53 ----
  function write(data, tag) {
    const ys = [...new Set([...data.keys()].map(k => Number(k.split('|')[1])))].sort((a, b) => a - b)
    if (!ys.length) ys.push(curYear)
    const columns = ['Type', 'Year', ...Array.from({ length: MAX_WEEK }, (_, i) => String(i + 1))]
    const rows = []
    ys.forEach(y => model.types.forEach(t => {
      rows.push([t, String(y), ...Array.from({ length: MAX_WEEK }, (_, i) => {
        const n = data.get(cellKey(t, y, i + 1))
        return n ? String(n) : ''
      })])
    }))
    replaceGrid(columns, rows, tag)
  }

  function setVal(type, w, text) {
    if (ro) return
    const n = toInt(String(text).replace(/[^0-9]/g, ''))
    const data = new Map(model.data)
    const k = cellKey(type, shownYear, w)
    if (n) data.set(k, n); else data.delete(k)
    write(data, k)
  }

  // ลูกศร / Enter เดินไปช่องข้าง ๆ เหมือน Excel
  function onKey(e, ti, wi) {
    const mv = { ArrowLeft: [0, -1], ArrowRight: [0, 1], ArrowUp: [-1, 0], ArrowDown: [1, 0], Enter: [1, 0] }[e.key]
    if (!mv) return
    const el = scrollRef.current?.querySelector(`input[data-cell="${ti + mv[0]}:${wi + mv[1]}"]`)
    if (el) { e.preventDefault(); el.focus(); el.select() }
  }

  const limit = cfg?.limit || null
  const nowCol = cols.find(c => c.state === 'now')

  return (
    <div className="jdview">
      <div className="jdtop">
        <div className="jdyears" role="tablist">
          {years.map(y => {
            const n = countOfYear(y)
            return (
              <button key={y} role="tab" aria-selected={y === shownYear}
                className={'jdyear' + (y === shownYear ? ' on' : '')} onClick={() => setYear(y)}>
                {y}{y === curYear && <span className="jdnowtag">ปีนี้</span>}
                {n > 0 && <span className="jdbadge">{n}</span>}
              </button>
            )
          })}
        </div>
        <div className="jdinfo">
          {nowCol && <span>สัปดาห์นี้ <b>W{nowCol.w}</b> ({rangeText(nowCol.from, nowCol.to)})</span>}
          {limit && <span>โควตา <b>{limit}</b> ครั้ง/สัปดาห์/โรง</span>}
        </div>
      </div>

      {cfg && !cfg.enabled && (
        <div className="jdnote">
          ℹ️ ตอนนี้ปิด “ให้ระบบย้าย Guage เองในแผน” อยู่ — ตัวเลขในหน้านี้ยังไม่มีผลกับแผน
          จะถูกนำไปหักโควตาเมื่อเปิดข้อนั้น (หมวด Change Cylinder)
        </div>
      )}

      {model.legacy > 0 && (
        <div className="jdnote warn">
          ⚠ ข้อมูลเดิม {model.legacy} ช่องยังไม่ได้ระบุปี — หน้านี้เดาปีให้ตามสัปดาห์ที่ใกล้วันนี้ที่สุด
          (ซึ่งจะเลื่อนไปเป็นปีหน้าเองเมื่อเวลาผ่านไป) ตรวจว่าอยู่ปีถูกแล้วกดยืนยัน หรือแก้ช่องไหนก็ได้ ระบบจะบันทึกปีให้
          {!ro && (
            <button className="primary" disabled={!cal} onClick={() => write(model.data, 'migrate')}>
              ✔ ยืนยันปีตามนี้
            </button>
          )}
        </div>
      )}

      <div className="jdscroll" ref={scrollRef}>
        <table className="jdtable">
          <thead>
            <tr>
              <th className="jd-type">โรง</th>
              {cols.map(c => (
                <th key={c.w} ref={c.state === 'now' ? nowRef : null}
                  className={'jd-wk ' + c.state}
                  title={c.from ? `W${c.w}/${shownYear} · ${rangeText(c.from, c.to)}` : `W${c.w}/${shownYear}`}>
                  <div className="w">W{c.w}</div>
                  {c.from && <div className="d">{shortDate(c.from)}</div>}
                </th>
              ))}
              <th className="jd-sum">รวม</th>
            </tr>
          </thead>
          <tbody>
            {model.types.map((t, ti) => {
              let sum = 0
              return (
                <tr key={t}>
                  <th className="jd-type">
                    <div className="nm">{TYPE_LABEL[t] || t}</div>
                    <div className="code">{t}</div>
                  </th>
                  {cols.map((c, wi) => {
                    const n = model.data.get(cellKey(t, shownYear, c.w)) || 0
                    sum += n
                    const over = limit && n > limit
                    return (
                      <td key={c.w} className={'jd-cell ' + c.state + (n ? ' has' : '') + (over ? ' over' : '')}
                        title={over ? `เกินโควตา ${limit} ครั้ง/สัปดาห์` : undefined}>
                        <input data-cell={`${ti}:${wi}`} inputMode="numeric" readOnly={ro}
                          value={n || ''} placeholder="·"
                          onChange={e => setVal(t, c.w, e.target.value)}
                          onFocus={e => e.target.select()}
                          onKeyDown={e => onKey(e, ti, wi)} />
                      </td>
                    )
                  })}
                  <td className="jd-sum">{sum || '–'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="gridhelp">
        ตัวเลข = จำนวนครั้งที่เปลี่ยน Guage<b>ไปแล้วจริง</b>ในสัปดาห์นั้น · ช่องว่าง = ไม่ได้เปลี่ยน ·
        สัปดาห์นับศุกร์–พฤหัส (วันที่ใต้หัวคอลัมน์ = วันแรกของสัปดาห์ ชี้ค้างดูช่วงเต็ม) · ลูกศร/Enter = ย้ายช่อง ·
        สลับปีที่แถบด้านบน
        {ro
          ? <> · <b>หน้านี้ดูอย่างเดียว</b> — แก้ได้ที่ ⚙️ Setting</>
          : <> · บันทึกให้เองอัตโนมัติ · Ctrl+Z = ย้อนกลับ</>}
      </div>
    </div>
  )
}

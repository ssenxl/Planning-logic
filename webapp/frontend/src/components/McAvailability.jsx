import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'

/* Dashboard › Machine Availability — เครื่องที่เหลือรายสัปดาห์ สรุปจาก booking ด้วย logic เดียวกับ AVA_MC
   สไตล์เดียวกับ Pending Dashboard (AI_Dye): ธีมมืดเป็นค่าเริ่มต้น สลับสว่างได้ จำไว้ใน localStorage
   กราฟวาดด้วย div ล้วน ไม่พึ่ง chart library (โปรเจกต์นี้มีแค่ react)
   ชิ้นส่วนที่ export ไว้ (กราฟ/heatmap/ตัวกรอง/tooltip) ใช้ซ้ำในหน้า Outsource Availability (S9Availability.jsx) */

// ธีมจำร่วมกับหน้า Outsource Availability — สลับที่หน้าไหนก็ได้ผลทั้งสองหน้า
export const THEME_KEY = 'mcav-theme'

// เกณฑ์สีเดียวกันทั้งกราฟและ heatmap: <70% ยังว่าง · 70–90% เริ่มแน่น · >90% เกือบเต็ม/เกิน
export const TIER = { warn: 70, crit: 90 }
export const tierOf = (pct) => (pct == null ? null : pct >= TIER.crit ? 'crit' : pct >= TIER.warn ? 'warn' : 'ok')

const MODES = [
  { id: 'pct', label: '% ใช้เครื่อง' },
  { id: 'remain', label: 'เครื่องเหลือ' },
  { id: 'used', label: 'ใช้ไป' },
]

export const fmt = (n) => Math.round(n).toLocaleString()
export const pctFmt = (p) => (p == null ? '—' : Math.round(p) + '%')

export function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}
function fmtSize(b) {
  if (b == null) return '-'
  if (b < 1024) return b + ' B'
  if (b < 1024 * 1024) return (b / 1024).toFixed(0) + ' KB'
  return (b / 1024 / 1024).toFixed(1) + ' MB'
}

/** แกน y ที่ลงตัว (ปัดขึ้นเป็นเลขกลม) เพื่อให้เส้น grid อ่านง่าย */
function niceMax(v) {
  if (v <= 5) return 5
  const mag = Math.pow(10, Math.floor(Math.log10(v)))
  for (const s of [1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10]) {
    if (v <= s * mag) return s * mag
  }
  return 10 * mag
}

/* ---------- tooltip ลอยตามเมาส์ ---------- */
export function Tip({ tip }) {
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
export const tipPos = (e) => ({ x: Math.min(e.clientX + 14, window.innerWidth - 260), y: e.clientY + 16 })

/* ---------- กราฟแท่งรายสัปดาห์: ใช้ไป vs เครื่องทั้งหมด ---------- */
export function WeekBars({ weeks, agg, years, mounted, selected, onSelect, setTip }) {
  const PLOT_H = 210
  const yMax = niceMax(Math.max(1, ...weeks.map(w => Math.max(agg[w].total, agg[w].used))))
  const TICKS = [0.25, 0.5, 0.75, 1]
  const h = (v) => (mounted ? Math.round((Math.max(0, v) / yMax) * PLOT_H) : 0)

  const show = (e, w) => {
    const a = agg[w]
    setTip({
      ...tipPos(e),
      title: `สัปดาห์ ${w}${years?.[w] ? ` · ปี ${years[w]}` : ''}`,
      lines: [
        { swatch: 'var(--d-grid-bar)', text: `เครื่องทั้งหมด: ${fmt(a.total)} เครื่อง` },
        { swatch: `var(--d-${tierOf(a.pct) || 'ok'})`, text: `ใช้ไป: ${fmt(a.used)} เครื่อง (${pctFmt(a.pct)})` },
        { swatch: 'var(--d-free)', text: `เหลือ: ${fmt(a.remain)} เครื่อง` },
        ...(a.lock ? [{ text: `รวมเครื่องที่กันไว้ (Lock_MC) ${fmt(a.lock)} เครื่อง` }] : []),
      ],
    })
  }

  return (
    <div className="mcav-chart">
      <div className="mcav-yaxis" style={{ height: PLOT_H }}>
        {TICKS.map(f => (
          <span key={f} style={{ bottom: f * PLOT_H }}>{fmt(yMax * f)}</span>
        ))}
      </div>
      <div className="mcav-plotwrap">
        <div className="mcav-plot" style={{ height: PLOT_H, minWidth: weeks.length * 58 }}>
          {TICKS.map(f => <i key={f} className="mcav-gridline" style={{ bottom: f * PLOT_H }} />)}
          <i className="mcav-gridline base" />
          <div className="mcav-bars">
            {weeks.map(w => {
              const a = agg[w]
              const tier = tierOf(a.pct) || 'ok'
              return (
                <div key={w}
                  className={'mcav-barcol' + (selected && selected !== w ? ' dim' : '')}
                  onMouseMove={e => show(e, w)}
                  onMouseLeave={() => setTip(null)}
                  onClick={e => onSelect(w, e)}>
                  <div className="mcav-barpair">
                    <span className={'mcav-barval tier-' + tier}>{fmt(a.used)}</span>
                    <span className={'mcav-bar tier-' + tier} style={{ height: h(a.used) }} />
                    <span className="mcav-bar total" style={{ height: h(a.total) }} />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
        <div className="mcav-xaxis" style={{ minWidth: weeks.length * 58 }}>
          {weeks.map(w => {
            const a = agg[w]
            return (
              <div key={w}
                className={'mcav-xtick' + (selected === w ? ' on' : '') + (selected && selected !== w ? ' dim' : '')}
                title="คลิก = เลือกสัปดาห์นี้ · Shift+คลิก = กำหนดเป็นช่วง"
                onClick={e => onSelect(w, e)}>
                <div className="wk">W{w}</div>
                <div className={'pc tier-' + (tierOf(a.pct) || 'ok')}>{pctFmt(a.pct)}</div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

/* ---------- การ์ดรายละเอียดของช่องที่คลิก ---------- */
function DetailCard({ d, loading, onClose }) {
  const [q, setQ] = useState('')
  useEffect(() => {
    const esc = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', esc)
    return () => window.removeEventListener('keydown', esc)
  }, [onClose])

  const items = useMemo(() => {
    const s = q.trim().toLowerCase()
    if (!s || !d?.items) return d?.items || []
    return d.items.filter(i =>
      (i.item + ' ' + i.desc + ' ' + i.so + ' ' + i.team + ' ' + i.mc_group + ' ' + i.material)
        .toLowerCase().includes(s))
  }, [d, q])

  const sm = d?.summary
  const tier = sm ? tierOf(sm.pct) || 'ok' : 'ok'
  // เครื่องที่ MC Special กันไว้ให้ POLY/COTTON — อยู่ในกองเดียวกันแต่นับแยกจากเครื่องปกติ
  const rsv = sm?.reserved || []
  const pile = sm?.pile_total ?? sm?.total ?? 0     // ทั้งกอง = ปกติ + กันไว้

  return (
    <div className="mcav-modal" onClick={onClose}>
      <div className="mcav-modal-box" onClick={e => e.stopPropagation()}>
        {loading && <div className="mcav-empty sm">กำลังโหลดรายละเอียด...</div>}
        {!loading && d && !d.ok && <div className="mcav-empty sm">{d.reason}</div>}
        {!loading && d?.ok && (
          <>
            <div className="dt-head">
              <div>
                <h3>{d.cat} · W{d.week}{d.year ? `/${d.year}` : ''}</h3>
                <div className="sub">
                  {d.factory || '-'}{d.pool_label ? ' · ' + d.pool_label : ''} ·
                  {rsv.length > 0 && (
                    <> ทั้งกอง {fmt(pile)} เครื่อง (ปกติ {fmt(sm.total)}
                      {rsv.map(x => ` + ${x.kind} ${fmt(x.total)}`).join('')}) ·</>
                  )}
                  {' '}{rsv.length > 0 ? 'เครื่องปกติใช้' : 'ใช้'} {fmt(sm.used)} จาก {fmt(sm.total)} เครื่อง · เหลือ {fmt(sm.remain)} ·
                  {' '}{sm.groups_used} กลุ่มเครื่องที่มีงาน จาก {sm.groups_all} · {fmt(sm.items)} รายการ · {fmt(sm.kp)} kg
                </div>
              </div>
              <div className="dt-pct">
                <div className={'big tier-' + tier}>{pctFmt(sm.pct)}</div>
                <div className="cap">
                  {sm.remain < 0 ? '⚠ จองเกินเครื่องที่มี — ต้องเลื่อนงานออก'
                    : sm.pct >= TIER.crit ? '⚠ เกือบเต็ม'
                      : sm.pct >= TIER.warn ? 'เริ่มแน่น' : 'ยังว่าง'}
                  {sm.lock ? ` · รวมเครื่องที่กันไว้ ${sm.lock}` : ''}
                </div>
              </div>
              <button className="dt-close" onClick={onClose} title="ปิด (Esc)">✕</button>
            </div>

            <div className="dt-body">
              {/* กลุ่มในพูลเดียวกันใช้เครื่องก้อนเดียวกัน — ห้ามแสดงเป็น "x / ยอดพูล" ต่อกลุ่ม
                  เพราะอ่านได้ว่าแต่ละกลุ่มมีเครื่องเท่ายอดพูล (2 กลุ่ม = เครื่อง 2 เท่า) */}
              <div className="dt-sec">
                กลุ่มเครื่องในพูล — เครื่องที่ booking จองไว้สัปดาห์นี้
                {d.groups.length > 1 && ` · ${d.groups.length} กลุ่มนี้ใช้เครื่องก้อนเดียวกัน รวม ${fmt(pile)} เครื่อง`}
              </div>
              <div className="dt-groups">
                {d.groups.map(g => {
                  const p = pile ? Math.round((g.mc / pile) * 100) : 0
                  return (
                    <div key={g.mc_group} className="gcard">
                      <div className="gtop">
                        <b>{g.mc_group}</b>
                        <span className={'gp tier-' + (tierOf(p) || 'ok')}
                          title={`สัดส่วนต่อเครื่องทั้งกอง ${fmt(pile)} เครื่อง (ใช้ร่วมกันทุกกลุ่ม)`}>{p}%</span>
                      </div>
                      <div className="gbar">
                        <i className={'tier-' + (tierOf(p) || 'ok')} style={{ width: Math.min(100, p) + '%' }} />
                      </div>
                      <div className="gft">{g.mc} เครื่อง
                        {g.reserved_mc > 0 && ` (ในโควตา POLY/COTTON ${g.reserved_mc})`}
                        {' '}· {g.items} รายการ · {fmt(g.kp)} kg
                        {g.setup ? ` · Setup ใหม่ ${g.setup}` : ''}</div>
                    </div>
                  )
                })}
                {!d.groups.length && <div className="gnone">สัปดาห์นี้ไม่มีงาน booking ในกลุ่มนี้</div>}
              </div>

              {rsv.length > 0 && (
                <div className="dt-note">
                  {rsv.map(x => {
                    const left = x.total - x.used
                    return `เครื่องที่กันไว้ให้ ${x.kind} ${fmt(x.total)} เครื่อง — booking ใช้ ${fmt(x.used)} ` +
                      (left < 0 ? `(เกิน ${fmt(-left)})` : `(เหลือ ${fmt(left)})`)
                  }).join(' · ')}
                  {' '}· นับแยกจากเครื่องปกติ ยอด "ใช้ {fmt(sm.used)} จาก {fmt(sm.total)}" ข้างบนจึงไม่รวมส่วนนี้
                  {' '}· รวมทั้งกอง booking ใช้ {fmt(sm.pile_used ?? sm.used)} จาก {fmt(pile)} เครื่อง
                </div>
              )}

              <div className="dt-sec row">
                <span>รายการงาน — {fmt(items.length)} รายการ{items.length !== d.items.length ? ` (จาก ${fmt(d.items.length)})` : ''}</span>
                <input placeholder="ค้นหา ITEM / SO / ทีม / กลุ่มเครื่อง" value={q} onChange={e => setQ(e.target.value)} />
              </div>
              <div className="dt-table-wrap">
                <table className="dt-table">
                  <thead>
                    <tr>
                      <th>กลุ่มเครื่อง</th><th>ITEM</th><th>รายละเอียด</th><th>SO</th><th>ทีม</th>
                      <th className="n">kg</th><th className="n">CAP/วัน</th><th className="n">เครื่อง</th><th>setup</th>
                    </tr>
                  </thead>
                  <tbody>
                    {items.map((i, n) => (
                      <tr key={n}>
                        <td>{i.mc_group}{i.reserved && <span className="rsv">{i.reserved}</span>}</td>
                        <td className="mono">{i.item}</td>
                        <td className="desc" title={i.desc}>{i.desc}</td>
                        <td className="mono">{i.so}</td>
                        <td>{i.team}</td>
                        <td className="n">{fmt(i.kp)}</td>
                        <td className="n" title={`CAP ต้นทาง ${fmt(i.cap_raw)} กก./วัน (ฐาน 24 ชม.) → แปลงตามชั่วโมงทำงาน = ${fmt(i.cap)}\nเครื่อง = ${fmt(i.kp)} ÷ (${fmt(i.cap)} × ${i.work_days} วัน) = ${i.mc_raw}`}>{fmt(i.cap)}</td>
                        <td className="n b">{i.mc}<span className="raw"> ({i.mc_raw})</span></td>
                        <td>{i.setup ? `ใหม่ ${i.setup_days} วัน` : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!items.length && <div className="mcav-empty sm">ไม่พบรายการที่ตรงกับคำค้น</div>}
                {d.truncated && <div className="dt-note">แสดงเฉพาะ 1,500 รายการแรก</div>}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

/* ---------- ตัวกรอง Cat+เกจ: ปุ่ม + กล่องติ๊กเลือกหลายค่า ---------- */
export function CatFilter({ options, value, onChange }) {
  const [open, setOpen] = useState(false)
  const [q, setQ] = useState('')
  const box = useRef(null)

  // ปิดกล่องเมื่อคลิกนอกกล่องหรือกด Esc (กล่องลอยทับกราฟ ถ้าไม่ปิดจะบังตัวเลข)
  useEffect(() => {
    if (!open) return
    const away = (e) => { if (box.current && !box.current.contains(e.target)) setOpen(false) }
    const esc = (e) => { if (e.key === 'Escape') setOpen(false) }
    document.addEventListener('mousedown', away)
    window.addEventListener('keydown', esc)
    return () => { document.removeEventListener('mousedown', away); window.removeEventListener('keydown', esc) }
  }, [open])

  const shown = useMemo(() => {
    const s = q.trim().toLowerCase()
    return s ? options.filter(o => o.cat.toLowerCase().includes(s)) : options
  }, [options, q])

  const sel = new Set(value)
  const toggle = (cat) => {
    const next = new Set(sel)
    next.has(cat) ? next.delete(cat) : next.add(cat)
    onChange([...next])
  }
  // "ทั้งหมด/ล้าง" ทำเฉพาะรายการที่มองเห็นอยู่ (ตอนพิมพ์ค้น จะได้เลือกทีละชุดได้)
  const allShown = shown.length > 0 && shown.every(o => sel.has(o.cat))
  const toggleAll = () => {
    const next = new Set(sel)
    for (const o of shown) allShown ? next.delete(o.cat) : next.add(o.cat)
    onChange([...next])
  }

  const label = value.length === 0 ? 'ทุกกลุ่มเครื่อง (Cat)'
    : value.length === 1 ? value[0]
      : `${value.length} กลุ่มเครื่อง`

  return (
    <div className={'mcav-cf' + (open ? ' open' : '')} ref={box}>
      <button className={'cf-btn' + (value.length ? ' on' : '')} onClick={() => setOpen(v => !v)}>
        <span className="cf-lb">{label}</span>
        {!!value.length && (
          <i className="cf-x" title="ล้างตัวกรอง"
            onClick={e => { e.stopPropagation(); onChange([]); }}>✕</i>
        )}
        <i className="cf-ar">▾</i>
      </button>
      {open && (
        <div className="cf-pop">
          <div className="cf-head">
            <input autoFocus placeholder="ค้นหา Cat / Guage" value={q} onChange={e => setQ(e.target.value)} />
            <button className="cf-all" onClick={toggleAll}>{allShown ? 'ล้าง' : 'ทั้งหมด'}</button>
          </div>
          <div className="cf-list">
            {shown.map(o => (
              <label key={o.cat} className={'cf-it' + (sel.has(o.cat) ? ' on' : '')}>
                <input type="checkbox" checked={sel.has(o.cat)} onChange={() => toggle(o.cat)} />
                <span className="nm">{o.cat}</span>
                <span className="ct">{o.n}</span>
              </label>
            ))}
            {!shown.length && <div className="cf-none">ไม่พบ Cat ที่ตรงกับคำค้น</div>}
          </div>
          <div className="cf-foot">
            เลือกไว้ {value.length} จาก {options.length} · ติ๊กได้หลายอัน
          </div>
        </div>
      )}
    </div>
  )
}

/* ---------- heatmap: กลุ่มเครื่อง × สัปดาห์ ---------- */
// nameHead = หัวคอลัมน์ชื่อแถว (หน้าจ้างทอใช้ "เครื่องผู้รับจ้าง")
export function Heatmap({ rows, weeks, mode, selected, onSelect, onOpen, setTip, nameHead = 'กลุ่มเครื่อง' }) {
  const show = (e, r, w, c) => {
    if (!c) return setTip(null)
    const pct = c.total ? (c.used / c.total) * 100 : null
    setTip({
      ...tipPos(e),
      title: `${r.factory || '-'} · ${r.cat}${r.pool_label ? ' · ' + r.pool_label : ''}`,
      lines: [
        { text: `สัปดาห์ ${w} — ใช้ cap ${pctFmt(pct)}` },
        { swatch: 'var(--d-grid-bar)', text: `ทั้งหมด ${c.total} เครื่อง` },
        { swatch: `var(--d-${tierOf(pct) || 'ok'})`, text: `ใช้ไป ${c.used} เครื่อง` },
        { swatch: 'var(--d-free)', text: `เหลือ ${c.remain} เครื่อง` },
        ...(c.lock ? [{ text: `ในนี้เป็นเครื่องที่กันไว้ ${c.lock} เครื่อง` }] : []),
      ],
    })
  }

  const cellText = (c) => {
    if (!c) return ''
    if (mode === 'used') return c.used
    if (mode === 'remain') return c.remain
    return c.total ? Math.round((c.used / c.total) * 100) + '%' : '—'
  }

  return (
    <div className="mcav-hm-wrap">
      <table className="mcav-hm">
        <thead>
          <tr>
            <th className="c-name">{nameHead}</th>
            {weeks.map(w => (
              <th key={w}
                className={'c-wk' + (selected === w ? ' on' : '') + (selected && selected !== w ? ' dim' : '')}
                title="คลิก = เลือกสัปดาห์นี้ · Shift+คลิก = กำหนดเป็นช่วง"
                onClick={e => onSelect(w, e)}>W{w}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.key}>
              <th className="c-name">
                <span className="nm">{r.cat}</span>
                <span className="sub">{r.factory || '-'}{r.pool_label ? ' · ' + r.pool_label : ''}</span>
              </th>
              {weeks.map(w => {
                const c = r.cells[w]
                const pct = c && c.total ? (c.used / c.total) * 100 : null
                const cls = c ? 'hm-' + (tierOf(pct) || 'ok') : 'hm-none'
                // ความเข้มของน้ำเงินไล่ตาม % (ช่วง 0–70%) ให้อ่านความหนาแน่นได้ในตาเดียว
                const alpha = pct == null ? 0 : Math.min(1, 0.18 + (Math.min(pct, TIER.warn) / TIER.warn) * 0.82)
                return (
                  <td key={w}
                    className={cls + (selected && selected !== w ? ' dim' : '') + (c ? ' hit' : '')}
                    style={cls === 'hm-ok' ? { '--a': alpha } : undefined}
                    onMouseMove={e => show(e, r, w, c)}
                    onMouseLeave={() => setTip(null)}
                    onClick={() => { if (c) { setTip(null); onOpen(r, w) } }}>
                    {cellText(c)}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ---------- หน้าหลัก ---------- */
// shareKey = เปิดจากลิงก์บนการ์ด Teams (ไม่ต้อง login, ดูอย่างเดียว) → ดึงผ่าน /api/share/mc-ava* ซึ่ง
//   ตอบตัวเดียวกับ /api/mc-ava* ; ไม่มีปุ่มคำนวณใหม่
export default function McAvailability({ shareKey = null }) {
  const src = useMemo(() => (shareKey ? {
    table: () => api.shareMcAva(shareKey),
    status: () => api.shareMcAvaStatus(shareKey),
    detail: (key, week) => api.shareMcAvaDetail(shareKey, key, week),
  } : { table: api.mcAva, status: api.mcAvaStatus, detail: api.mcAvaDetail }), [shareKey])
  const [theme, setTheme] = useState(() => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'))
  const [data, setData] = useState(null)
  const [status, setStatus] = useState({})
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [mode, setMode] = useState('pct')
  const [factory, setFactory] = useState('')
  const [cats, setCats] = useState([])          // Cat+เกจ ที่ติ๊กไว้ (ว่าง = ทุกกลุ่ม)
  const [tightOnly, setTightOnly] = useState(false)
  const [selWeek, setSelWeek] = useState(null)
  // ช่วงสัปดาห์ที่แสดง — เก็บเป็น index ของ array weeks (ไม่ใช่เลขสัปดาห์ดิบ เพราะรายการข้ามปี W29→W53→W1)
  const [rFrom, setRFrom] = useState(null)
  const [rTo, setRTo] = useState(null)
  const [mounted, setMounted] = useState(false)
  const [tip, setTip] = useState(null)
  const [detail, setDetail] = useState(null)        // การ์ดรายละเอียดของช่องที่คลิก
  const [detailLoading, setDetailLoading] = useState(false)
  const wasRunning = useRef(false)

  async function openDetail(row, week) {
    setDetail({ ok: true, pending: true }); setDetailLoading(true)
    try { setDetail(await src.detail(row.key, week)) }
    catch (e) { setDetail({ ok: false, reason: 'โหลดรายละเอียดไม่ได้: ' + e.message }) }
    finally { setDetailLoading(false) }
  }

  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])

  async function load() {
    try {
      const d = await src.table()
      setData(d); setErr('')
      // backend เห็นว่า booking ใหม่กว่า เลยสั่งคำนวณให้เอง → ติดธงไว้ให้ effect ด้านล่าง
      // โหลดตารางซ้ำเมื่อรันเสร็จ (ไม่งั้นหน้าจะค้างตัวเลขเก่าจนกว่าผู้ใช้จะรีเฟรชเอง)
      if (d.auto_started) { wasRunning.current = true; loadStatus() }
    } catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
  }
  async function loadStatus() {
    try { setStatus(await src.status()) } catch { }
  }

  useEffect(() => {
    load(); loadStatus()
    const t = setTimeout(() => setMounted(true), 60)   // ให้แท่งกราฟไต่ขึ้นตอนเปิดหน้า
    const iv = setInterval(loadStatus, 2000)
    return () => { clearTimeout(t); clearInterval(iv) }
  }, [])

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

  const weeks = data?.weeks || []

  // ตัดให้เหลือเฉพาะช่วงที่เลือก — ตัวนี้คือ "สัปดาห์ที่ใช้จริง" ของทั้งหน้า (กราฟ / heatmap / KPI / กลุ่มตึง)
  const viewWeeks = useMemo(() => {
    if (rFrom == null && rTo == null) return weeks
    const a = rFrom ?? 0, b = rTo ?? weeks.length - 1
    return weeks.slice(Math.min(a, b), Math.max(a, b) + 1)
  }, [weeks, rFrom, rTo])

  const wkLabel = (w) => `W${w}${data?.years?.[w] ? ' · ' + data.years[w] : ''}`
  const rangeOn = rFrom != null || rTo != null

  function setRange(a, b) {
    if (a != null && b != null && a > b) { const t = a; a = b; b = t }
    setRFrom(a); setRTo(b)
  }
  // คลิกหัวสัปดาห์ = เลือกสัปดาห์เดียว · Shift+คลิก = ลากเป็นช่วงจากสัปดาห์/ช่วงที่เลือกไว้
  function pickWeek(w, e) {
    const i = weeks.indexOf(w)
    if (e?.shiftKey && i >= 0) {
      const anchor = selWeek != null ? weeks.indexOf(selWeek) : (rFrom ?? weeks.indexOf(viewWeeks[0]))
      if (anchor >= 0) { setRange(anchor, i); setSelWeek(null); return }
    }
    setSelWeek(s => (s === w ? null : w))
  }

  // สัปดาห์ที่คลิกไว้หลุดออกนอกช่วง → ล้างทิ้ง ไม่งั้น KPI จะอ้างสัปดาห์ที่ไม่ได้แสดงแล้ว
  useEffect(() => {
    if (selWeek != null && !viewWeeks.includes(selWeek)) setSelWeek(null)
  }, [viewWeeks, selWeek])

  const factories = useMemo(() => {
    const s = new Set((data?.rows || []).map(r => r.factory).filter(Boolean))
    return [...s].sort()
  }, [data])

  // รายการ Cat+เกจ ให้เลือก — นับจากแถวที่ผ่าน filter โรงงานแล้ว (ไม่นับ filter Cat เอง
  // ไม่งั้นพอติ๊กอันแรก ตัวเลือกที่เหลือจะหายไปหมด) แถวพูลเดียวมีได้หลาย cat → นับทุกตัว
  const catOptions = useMemo(() => {
    const m = new Map()
    for (const r of data?.rows || []) {
      if (factory && r.factory !== factory) continue
      for (const c of r.cats || []) m.set(c, (m.get(c) || 0) + 1)
    }
    return [...m.entries()].map(([cat, n]) => ({ cat, n }))
      .sort((a, b) => a.cat.localeCompare(b.cat, 'en', { numeric: true }))
  }, [data, factory])

  // เปลี่ยนโรงงาน → ตัด Cat ที่เลือกไว้แต่ไม่มีในโรงงานนี้ทิ้ง (ไม่งั้นตารางว่างโดยไม่รู้สาเหตุ)
  useEffect(() => {
    if (!cats.length) return
    const ok = new Set(catOptions.map(o => o.cat))
    const keep = cats.filter(c => ok.has(c))
    if (keep.length !== cats.length) setCats(keep)
  }, [catOptions])

  const rows = useMemo(() => {
    const pick = new Set(cats)
    return (data?.rows || []).filter(r => {
      if (factory && r.factory !== factory) return false
      if (pick.size && !(r.cats || []).some(c => pick.has(c))) return false
      if (tightOnly) {
        const hot = viewWeeks.some(w => {
          const c = r.cells[w]
          return c && c.total && (c.used / c.total) * 100 >= TIER.warn
        })
        if (!hot) return false
      }
      return true
    })
  }, [data, factory, cats, tightOnly, viewWeeks])

  // ยอดรวมต่อสัปดาห์ของกลุ่มที่แสดงอยู่ — บวกได้ตรง ๆ (backend ยุบแถวซ้ำของพูลเดียวกันแล้ว)
  const agg = useMemo(() => {
    const out = {}
    for (const w of viewWeeks) {
      let total = 0, used = 0, remain = 0, lock = 0
      for (const r of rows) {
        const c = r.cells[w]
        if (!c) continue
        total += c.total; used += c.used; remain += c.remain; lock += c.lock || 0
      }
      out[w] = { total, used, remain, lock, pct: total ? (used / total) * 100 : null }
    }
    return out
  }, [rows, viewWeeks])

  // KPI: เครื่องทั้งหมด = ค่าสูงสุดรายสัปดาห์ (MC_Total override ทำให้บางสัปดาห์ไม่เท่ากัน)
  // คลิกสัปดาห์บนกราฟ = คิด KPI เฉพาะสัปดาห์นั้น (ไม่งั้นตัวเลขไม่ขยับ ทั้งที่หน้าจอ highlight อยู่)
  const kpi = useMemo(() => {
    const vw = selWeek && agg[selWeek] ? [selWeek] : viewWeeks
    const totals = vw.map(w => agg[w].total)
    const fullWeeks = vw.filter(w => agg[w].remain <= 0).length
    let hotGroups = 0, fullCells = 0
    for (const r of rows) {
      let hot = false
      for (const w of vw) {
        const c = r.cells[w]
        if (!c || !c.total) continue
        const p = (c.used / c.total) * 100
        if (p >= TIER.crit) { hot = true; fullCells++ }
        else if (p >= TIER.warn) hot = true
      }
      if (hot) hotGroups++
    }
    return {
      groups: rows.length,
      factories: new Set(rows.map(r => r.factory).filter(Boolean)).size,
      machines: totals.length ? Math.max(...totals) : 0,
      fullWeeks, hotGroups, fullCells,
      scope: selWeek && agg[selWeek]
        ? `W${selWeek}`
        : rangeOn && viewWeeks.length
          ? `W${viewWeeks[0]}–W${viewWeeks[viewWeeks.length - 1]} · ${viewWeeks.length} สัปดาห์`
          : `${viewWeeks.length} สัปดาห์`,
    }
  }, [rows, viewWeeks, agg, selWeek, rangeOn])

  const bk = data?.booking || status.booking
  const running = !!status.running
  const empty = !data?.ok || !data.rows?.length

  return (
    <div className={'mcav-dash ' + theme} onMouseLeave={() => setTip(null)}>
      <Tip tip={tip} />

      <div className="mcav-top">
        <div className="mcav-title">
          <h1>Machine Availability<span className={'live' + (running ? ' run' : '')}>{running ? 'กำลังคำนวณ' : 'LIVE'}</span></h1>
          <div className="sub">
            สรุปจาก booking ด้วย logic เดียวกับ AVA_MC · เครื่องทั้งหมด − ที่ booking จองไว้ − เครื่องที่กันไว้ (Lock_MC)
            {data?.source === 'pipeline' && ' · ตัวเลขชุดนี้มาจากผลรันแผนรอบล่าสุด'}
            {shareKey && ' · 🔗 ลิงก์ดูอย่างเดียว ข้อมูลชุดเดียวกับในระบบ'}
            {/* backend ตัดสัปดาห์ก่อน week+2 ออกให้ลิงก์แล้ว (teams_notify.plannable) — บอกไว้ว่าไม่ได้ข้อมูลหาย */}
            {shareKey && !!weeks.length && ` · แสดงตั้งแต่ W${weeks[0]} (สัปดาห์ที่ทำแผนได้)`}
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
          {!shareKey && (
            <button className="tbtn go" onClick={refresh} disabled={running}>
              {running ? '⏳ กำลังคำนวณ' : '↻ คำนวณใหม่'}
            </button>
          )}
        </div>
      </div>

      {(data?.stale || msg || err) && (
        <div className="mcav-alerts">
          {data?.stale && (
            <span className="al warn">
              {data.auto_started || running
                ? 'booking ใหม่กว่าตัวเลขที่แสดง — กำลังคำนวณให้อัตโนมัติ'
                : shareKey
                  ? 'booking ใหม่กว่าตัวเลขที่แสดง — ตัวเลขยังไม่รวม booking ล่าสุด'
                  : 'booking ใหม่กว่าตัวเลขที่แสดง — กดคำนวณใหม่เพื่ออัปเดต'}
            </span>
          )}
          {msg && <span className="al info">{msg}</span>}
          {err && <span className="al bad">{err}</span>}
        </div>
      )}

      {!!status.log_tail?.length && status.error && (
        <details className="mcav-log"><summary>ดู log ท้าย ๆ ของการคำนวณ</summary>
          <pre>{status.log_tail.join('\n')}</pre></details>
      )}

      {empty ? (
        <div className="mcav-empty">{data?.reason || 'ยังไม่มีข้อมูล'}</div>
      ) : (
        <>
          <div className="mcav-filters">
            <select value={factory} onChange={e => setFactory(e.target.value)}>
              <option value="">ทุกโรงงาน</option>
              {factories.map(f => <option key={f} value={f}>{f}</option>)}
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
              <button className="chip on" onClick={() => setRange(null, null)}
                title="ล้างช่วงสัปดาห์">
                W{viewWeeks[0]}–W{viewWeeks[viewWeeks.length - 1]} ✕
              </button>
            )}
            {selWeek && (
              <button className="chip on" onClick={() => setSelWeek(null)}>W{selWeek} ✕</button>
            )}
          </div>

          <div className="mcav-kpis">
            <div className="kpi k1">
              <div className="lb">กลุ่มเครื่อง</div>
              <div className="vl">{kpi.groups}</div>
              <div className="ft">{kpi.factories} โรงงาน · {kpi.scope}</div>
            </div>
            <div className="kpi k2">
              <div className="lb">เครื่องทั้งหมด</div>
              <div className="vl">{fmt(kpi.machines)}</div>
              <div className="ft">รวมทุกกลุ่มที่แสดงอยู่</div>
            </div>
            <div className="kpi k3">
              <div className="lb">⚠ สัปดาห์ที่เครื่องหมด</div>
              <div className="vl">{kpi.fullWeeks}</div>
              <div className="ft">รวมทุกกลุ่มแล้วไม่เหลือเครื่องว่าง · {kpi.scope}</div>
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
                <h2>เครื่องที่ใช้ไป เทียบเครื่องทั้งหมด — ต่อสัปดาห์</h2>
                <div className="sub">รวมทุกกลุ่มที่เลือก · สีแท่งตาม % ใช้เครื่อง (70/90) · คลิกคอลัมน์เพื่อ filter · Shift+คลิก = เลือกเป็นช่วง</div>
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
                <h2>% ใช้เครื่อง รายกลุ่ม × สัปดาห์</h2>
                <div className="sub">
                  % = เครื่องที่ booking จองไว้ ÷ เครื่องทั้งหมดของกลุ่มนั้น · <b>กดที่ช่อง = ดูรายละเอียด (เครื่อง / รายการงาน)</b> · กดชื่อสัปดาห์ = filter (Shift+คลิก = ช่วง)
                </div>
              </div>
              <div className="modes">
                {MODES.map(m => (
                  <button key={m.id} className={'chip' + (mode === m.id ? ' on' : '')}
                    onClick={() => setMode(m.id)}>{m.label}</button>
                ))}
              </div>
            </div>
            <Heatmap rows={rows} weeks={viewWeeks} mode={mode}
              selected={selWeek} onSelect={pickWeek}
              onOpen={openDetail} setTip={setTip} />
            {!rows.length && <div className="mcav-empty sm">ไม่มีกลุ่มเครื่องที่ตรงกับตัวกรอง</div>}
          </div>
        </>
      )}

      {detail && (
        <DetailCard d={detail.pending ? null : detail} loading={detailLoading}
          onClose={() => setDetail(null)} />
      )}
    </div>
  )
}

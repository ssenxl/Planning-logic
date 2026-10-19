import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { useYarnPull } from '../useYarnPull.js'

/* Dashboard › Yarn Usage — ด้ายแต่ละตัว (เช่น TC20/1R) lot ไหน ใช้ไปกี่ kg ในสัปดาห์ไหน
   ด้ายเป็นตัวตั้ง ไม่ใช่ ITEM ผ้า (user สั่ง: "ไม่ใช่มาตาม Item แต่ต้องแยกด้วย yarn" + "ต้องเป็นตาม week")
   → แถว = ด้าย × lot รวมจากผ้าทุก ITEM ที่ใช้ด้ายนั้น · สัปดาห์เป็นคอลัมน์ ช่อง = ใช้ไปแล้วของสัปดาห์นั้น
   ท้ายแถว = ยอดรวมทุกสัปดาห์: ใช้ไปแล้ว / ต้องใช้ทั้งใบ / เหลือต้องใช้

   ยอดจาก booking (เฉพาะงานที่ออกใบ KP แล้ว ไม่รวม COLLAR) × % ด้ายของใบ KP จาก view ด้ายต่อใบ KP
   — การคูณ/รวมทำที่ backend (yarn_usage_live.py) ทั้งแถว lot และแถวรวมของด้าย ; หน้านี้จัดกลุ่มกับกรองอย่างเดียว
   "สัปดาห์" = สัปดาห์ของแถว booking ที่ยอดทอถูกลงไว้ (booking ไม่มีวันเดินเครื่องจริง)

   ด้ายที่มีหลาย lot มีแถว "รวม" นำหน้า (ยอดของด้ายทั้งตัว) — โชว์เฉพาะตอนเห็นครบทุก lot
   (ค้นด้วยชื่อ lot แล้วเหลือบาง lot → ซ่อนแถวรวม ไม่ให้ยอดรวมไปอยู่กับ lot ที่ไม่ได้โชว์)
   LOT "-" = ไม่ทราบ lot — รวมงานจ้างทอ ซึ่ง view ด้ายต่อใบ KP ไม่เก็บ backend จึงใช้ % ด้ายจาก BOM (ที่ไม่มี lot) แทน
   กลุ่ม "ยังแยกเส้นไม่ได้" = ใบ KP ที่ไม่มีข้อมูลด้ายทั้งสองที่ (ใบใหม่ที่ยังไม่จัดด้าย) — ยอดเป็นยอดผ้าทั้งใบ
   ชื่อเป็น YARN_USED ของ booking ตามที่เขียนมา อยู่ท้ายตาราง ; ใส่ไว้ให้ยอดรวมทั้งหน้าครบเท่ากับ booking

   ใช้คลาสตารางของหน้า Stock Yarn (.ys-*) ร่วมกัน คลาสเฉพาะหน้านี้ขึ้นต้น .yu- */

const THEME_KEY = 'mcav-theme'      // ใช้ค่าเดียวกับหน้า Dashboard อื่น — สลับธีมหน้าหนึ่งแล้วอีกหน้าตาม
// แถวที่ render ต่อครั้ง — ด้ายไม่ถูกตัดกลางกลุ่ม: ใส่ทั้งก้อนจนเกินงบแล้วค่อยหยุด
const PAGE = 800

const kg = (n) => (n == null ? '-' : n.toLocaleString(undefined, { maximumFractionDigits: 1 }))

function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

/* ---------- หน้าหลัก ---------- */
// canRun = โชว์ปุ่ม "ดึงข้อมูลด้าย" (App.jsx ส่งให้เฉพาะ admin)
export default function YarnUsage({ canRun = false }) {
  const [theme, setTheme] = useState(() => (localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'))
  const [d, setD] = useState(null)
  const [err, setErr] = useState('')
  const [loading, setLoading] = useState(false)
  const [q, setQ] = useState('')
  const [limit, setLimit] = useState(PAGE)

  useEffect(() => { localStorage.setItem(THEME_KEY, theme) }, [theme])

  async function load() {
    setLoading(true); setErr('')
    try { setD(await api.yarnUsage()) }
    catch (e) { setErr('โหลดข้อมูลไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }
  // โหลดตอนเปิดหน้า และโหลดใหม่เองเมื่อมีรอบรันจบ (ดึง booking / ดึงด้ายรอบใหม่) — จอที่เปิดค้างไม่ต้องกดเอง
  useEffect(() => {
    load()
    return live.subscribe(live.RES.run, load)
  }, [])

  // ปุ่ม "ดึงข้อมูลด้าย" (useYarnPull) — ดึงจบแล้วโหลดหน้าใหม่
  const { pulling, pullMsg, pull } = useYarnPull(load)

  const yarns = d?.yarns
  const all = d?.rows
  const weeks = d?.weeks || []
  // ข้อมูลคร่อม 2 ปี → หัวคอลัมน์ต้องมีปี (W01 ของปีหน้ากับปีนี้เป็นคนละสัปดาห์)
  const multiYear = new Set(weeks.map(w => w.slice(0, 4))).size > 1
  const wkLabel = (yw) => (multiYear ? yw.slice(2, 4) + '/W' : 'W') + yw.slice(5)

  // จำนวน lot ทั้งหมดของด้ายแต่ละตัว (ก่อนกรอง) — ใช้ตัดสินว่าตอนนี้เห็นครบทุก lot หรือไม่
  const lotCount = useMemo(() => {
    const n = new Map()
    for (const r of all || []) n.set(r.g, (n.get(r.g) || 0) + 1)
    return n
  }, [all])

  // กรองอย่างเดียว ไม่แตะค่าในแถว: คำค้นตรงชื่อด้าย = เห็นทุก lot ของด้ายนั้น · ตรงชื่อ lot = เห็นเฉพาะ lot นั้น
  const kw = q.trim().toLowerCase()
  const groups = useMemo(() => {
    const byYarn = new Map()
    for (const r of all || []) {
      if (kw && !yarns[r.g].y.toLowerCase().includes(kw) && !r.l.toLowerCase().includes(kw)) continue
      let rs = byYarn.get(r.g)
      if (!rs) byYarn.set(r.g, (rs = []))
      rs.push(r)
    }
    // backend เรียงมาแล้ว: ด้ายจริง A→Z แล้วค่อยกลุ่มที่ยังแยกเส้นไม่ได้ ; lot A→Z
    return [...byYarn].sort(([a], [b]) => a - b).map(([g, rs]) => ({ g, yarn: yarns[g], lots: rs }))
  }, [all, yarns, kw])

  const src = d?.source || {}
  const st = d?.stats || {}
  // เตือนเฉพาะตอนมี "ยอด" ค้างอยู่นอกด้ายจริง — ใบที่ยังแยกเส้นไม่ได้แต่ยอดใบ = 0 ไม่ทำให้ตัวเลขด้ายตัวไหนขาด
  const nosplitKg = st.nosplit_need > 0 || st.nosplit_used > 0

  /* ช่องของ 1 แถว (ใช้ทั้งแถว lot และแถวรวมของด้าย): สัปดาห์ละช่อง + ยอดรวม 3 ช่อง
     ช่องสัปดาห์: ไม่มีใบ KP = "-" · มีใบ KP = ใช้ไปแล้ว (0 = ยังไม่ได้ทอ) ชี้แล้วเห็นยอดที่ต้องใช้ของสัปดาห์นั้น */
  const cells = (x) => [
    ...weeks.map(yw => {
      const c = x.w[yw]
      return c
        ? <td key={yw} className={'n yu-wk' + (c[0] > 0 ? '' : ' yu-zero')}
            title={`สัปดาห์ ${yw} · ใช้ไปแล้ว ${kg(c[0])} kg จากที่ต้องใช้ ${kg(c[1])} kg`}>{kg(c[0])}</td>
        : <td key={yw} className="n yu-wk yu-zero">-</td>
    }),
    x.o > 0
      ? <td key="used" className="n yu-sum yu-used yu-over"
          title={`${x.o} จาก ${x.k} ใบ KP ยอดทอใน booking มากกว่ายอดใบเกิน ${d.over_pct}% (ทอเกิน หรือยอดใบถูกแก้ลดหลังทอ)`}>
          {kg(x.used)}
        </td>
      : <td key="used" className="n yu-sum yu-used">{kg(x.used)}</td>,
    <td key="need" className="n">{kg(x.need)}</td>,
    <td key="rem" className={'n' + (x.rem > 0 ? '' : ' yu-zero')}>{kg(x.rem)}</td>,
  ]

  const rows = []
  let nShown = 0
  for (const grp of groups) {
    if (rows.length >= limit) break
    nShown++
    const { g, yarn, lots } = grp
    // แถวรวมของด้าย: เฉพาะด้ายที่มีหลาย lot และตอนนี้เห็นครบทุก lot (lot เดียว = แถว lot คือยอดของด้ายอยู่แล้ว)
    const total = lots.length > 1 && lots.length === lotCount.get(g)
    const yarnCell = (
      <td className={'ys-item' + (yarn.x ? ' yu-nosplit' : '')} rowSpan={lots.length + (total ? 1 : 0)}
        title={yarn.x
          ? 'ใบ KP กลุ่มนี้ยังไม่มีข้อมูลด้ายทั้งใน view ด้ายต่อใบ KP และ BOM — ไม่รู้ % ของแต่ละเส้น ยอดในแถวเป็นยอดผ้าทั้งใบ'
          : undefined}>
        <div className="nm">
          {yarn.y}
          {yarn.x && <span className="yu-tag">ยังแยกเส้นไม่ได้</span>}
        </div>
      </td>
    )
    if (total) {
      rows.push(
        <tr key={g + '\u0001'} className="ys-itfirst yu-total">
          {yarnCell}
          <td className="nw">รวม {lots.length} lot</td>
          {cells(yarn)}
        </tr>,
      )
    }
    lots.forEach((r, ri) => {
      rows.push(
        <tr key={g + '\u0001' + r.l} className={!total && ri === 0 ? 'ys-itfirst' : undefined}>
          {!total && ri === 0 && yarnCell}
          <td className="nw">{r.l}</td>
          {cells(r)}
        </tr>,
      )
    })
  }

  return (
    <div className={'mcav-dash ' + theme}>
      <div className="mcav-top">
        <div className="mcav-title">
          <h1>Yarn Usage<span className="live">LIVE</span></h1>
          <div className="sub">
            ด้ายแต่ละตัว lot ไหน ใช้ไปเท่าไหร่ในสัปดาห์ไหน · ยอดจาก booking เฉพาะงานที่ออกใบ KP แล้ว (ไม่รวม COLLAR)
            × % ด้ายของใบ KP จาก Oracle
          </div>
        </div>
        <div className="mcav-topright">
          <div className="clock">
            <b>{fmtTime(src.booking?.mtime)}</b>
            <span>booking · ด้ายต่อใบ KP {fmtTime(src.split?.mtime)}</span>
          </div>
          <button className="tbtn" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>
            {theme === 'dark' ? '☀ ธีมสว่าง' : '🌙 ธีมมืด'}
          </button>
          {canRun && (
            <button className="tbtn" onClick={pull} disabled={pulling}
              title="ดึงไฟล์ด้ายต่อใบ KP ใหม่จาก DB (ไม่กี่วินาที) — ไม่ต้องรอรอบดึงอัตโนมัติ ; ยอดจาก booking ไม่ได้ดึงใหม่ด้วย">
              {pulling ? '⏳ กำลังดึงข้อมูลด้าย' : '⇩ ดึงข้อมูลด้าย'}
            </button>
          )}
          <button className="tbtn go" onClick={load} disabled={loading}>
            {loading ? '⏳ กำลังโหลด' : '↻ โหลดใหม่'}
          </button>
        </div>
      </div>

      {(err || d?.note || nosplitKg || pullMsg) && (
        <div className="mcav-alerts">
          {err && <span className="al bad">{err}</span>}
          {pullMsg && <span className={'al ' + pullMsg.cls}>{pullMsg.text}</span>}
          {d?.note && <span className="al warn">{d.note}</span>}
          {nosplitKg && (
            <span className="al warn">
              ใบ KP {st.kp_nosplit.toLocaleString()} จาก {st.kp.toLocaleString()} ใบ ยังไม่มีข้อมูลด้าย
              (ไม่มีทั้งใน view ด้ายต่อใบ KP และ BOM) — ยอดใบ {kg(st.nosplit_need)} kg ทอแล้ว {kg(st.nosplit_used)} kg
              ยังไม่ได้นับเข้าด้ายตัวไหน อยู่ในกลุ่ม “ยังแยกเส้นไม่ได้” ท้ายตาราง
            </span>
          )}
        </div>
      )}

      <div className="mcav-filters">
        <input className="otd-search ys-search" value={q}
          onChange={e => { setQ(e.target.value); setLimit(PAGE) }}
          placeholder="ค้นหา YARN / LOT…" />
        {kw && <button className="chip" onClick={() => { setQ(''); setLimit(PAGE) }}>ล้างตัวกรอง</button>}
      </div>

      <div className="mcav-card">
        <div className="hd">
          <div>
            <h2>YARN → LOT รายสัปดาห์</h2>
            <div className="sub">
              {weeks.length > 0 && <>booking สัปดาห์ {weeks[0]} ถึง {weeks[weeks.length - 1]} · </>}
              ทุกช่องเป็น kg ด้าย · ช่องสัปดาห์ = ใช้ไปแล้วในสัปดาห์นั้น รวมผ้าทุก ITEM ที่ใช้ด้ายนี้ ·
              “-” = สัปดาห์นั้นไม่มีใบ KP · 0 = มีใบ KP แต่ยังไม่ได้ทอ (ชี้ที่ช่องเพื่อดูยอดที่ต้องใช้) ·
              LOT “-” = ไม่ทราบ lot
              {st.kp_bom > 0 && (
                <> (รวม {st.kp_bom.toLocaleString()} ใบ KP ที่ใช้ % ด้ายจาก BOM ซึ่งไม่มี lot — ส่วนใหญ่งานจ้างทอ)</>
              )} ·
              สัปดาห์ = สัปดาห์ของแถว booking ไม่ใช่วันเดินเครื่องจริง ·
              ใช้ไปแล้ว = ยอดทอจริง × % ด้าย · ต้องใช้ทั้งใบ = ยอดใบ KP × % ·
              เหลือต้องใช้ = ยอดที่ยังไม่ได้ทอ × % (ใบที่ปิดแล้วหรือทอเกินยอดใบ นับ 0) · ไม่บวก % สูญเสียตอนทอ
              {d?.over_pct != null && (
                <> · <span className="yu-over">ใช้ไปแล้วสีส้ม</span> = มีใบ KP ที่ booking ลงยอดทอมากกว่ายอดใบเกิน {d.over_pct}%</>
              )}
            </div>
          </div>
        </div>
        {groups.length === 0
          ? <div className="mcav-empty sm">{all?.length ? 'ไม่มีรายการที่ตรงกับตัวกรอง' : 'ยังไม่มีข้อมูล'}</div>
          : (
            <div className="otd-tablebox ys-tablebox">
              <table className="otd-table ys-table yu-table">
                <thead>
                  <tr>
                    <th>YARN</th>
                    <th>LOT</th>
                    {weeks.map(yw => <th key={yw} className="n yu-wk" title={'สัปดาห์ ' + yw}>{wkLabel(yw)}</th>)}
                    {/* หัวสั้น ไม่ใส่ (kg) — 10 สัปดาห์ + ยอดรวม 3 ช่องต้องอยู่ในจอเดียว ; หน่วยบอกไว้ในบรรทัดอธิบาย */}
                    <th className="n yu-sum">รวมใช้ไปแล้ว</th>
                    <th className="n">ต้องใช้ทั้งใบ</th>
                    <th className="n">เหลือต้องใช้</th>
                  </tr>
                </thead>
                <tbody>{rows}</tbody>
              </table>
            </div>
          )}
        {groups.length > nShown && (
          <div className="cs-more">
            <button className="tbtn" onClick={() => setLimit(l => l + PAGE)}>แสดงเพิ่ม</button>
          </div>
        )}
      </div>
    </div>
  )
}

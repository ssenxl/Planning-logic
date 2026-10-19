import React, { useMemo, useRef } from 'react'
import { weekSpan, spanLabel } from '../weekSpan.js'

// มุมมอง "การ์ด" ของชีท MC_Total — กำหนดจำนวนเครื่องทั้งหมดของสัปดาห์นั้น ๆ
// แต่ละการ์ด = 1 แถวในชีท อ่านเป็นประโยค: "สัปดาห์ W__ กลุ่ม __ เกจ __ มีเครื่อง __ ตัว"
// แก้ค่าลงใน grid เดิมของ Masters โดยตรง → ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน
//
// ความหมายของตัวเลข = "จำนวนเครื่องทั้งหมด" ของสัปดาห์นั้น (แทนที่ Total MC ในชีท Master MC)
//   · ทั้งกอง รวมเครื่องที่ MC Special กันไว้ให้ POLY/COTTON (AVA_MC หักส่วนกันไว้ออกเอง — McTotal.py)
//   · มีผลเฉพาะสัปดาห์ที่ระบุ — สัปดาห์อื่นกลับไปใช้ค่าเดิม
//   · booking / Lock_MC ยังถูกหักต่ออีกชั้นตามปกติ (เครื่องว่าง = ยอดนี้ − ที่จองไว้)
//   · โรงงานเว้นว่าง = ทุกโรงงาน (AVA_MC กระจายยอดตามสัดส่วนเครื่องเดิมของแต่ละพูล)
//   · เครื่อง (MC) เว้นว่าง = ทั้งกลุ่ม+เกจ — ถ้าจะลดเฉพาะ SKP ต้องเลือกเครื่อง ไม่งั้น
//     ยอดจะถูกกระจายลงทุกพูลของ SINGLE-32 26G (SKP + SKPLE/SKPTA) = ลดผิดตัว
//
// การ์ดโชว์ "ค่าเดิมจาก Master MC" เทียบให้เห็นว่าเพิ่ม/ลดกี่เครื่อง และเตือนเมื่อ
// คีย์ซ้ำ (สัปดาห์+กลุ่ม+เกจ+โรงงานเดียวกัน) เพราะระบบจะใช้ค่าของแถวหลังสุดเท่านั้น

const val = v => String(v ?? '').trim()
const distinct = arr => [...new Set(arr.map(val).filter(Boolean))].sort((a, b) =>
  String(a).localeCompare(String(b), 'th', { numeric: true }))
const normGauge = v => val(v).toUpperCase().replace('GAUGE', '').replace('G', '').trim()

// dropdown ที่ "ไม่ทิ้ง" ค่าปัจจุบันแม้ไม่อยู่ในรายการ (กันข้อมูลเก่าหาย) — เติมเป็น option แรก
// โหมดดูอย่างเดียว (หน้า Master Data) — ส่งผ่าน context เพราะ dropdown ถูกเรียกหลายจุดในการ์ด
const RoCtx = React.createContext(false)

function Select({ value, options, onChange, placeholder, className }) {
  const ro = React.useContext(RoCtx)
  const cur = val(value)
  const has = cur && options.includes(cur)
  return (
    <select className={'lksel ' + (className || '')} value={cur} disabled={ro}
      onChange={e => onChange(e.target.value)}>
      <option value="">{placeholder || '— เลือก —'}</option>
      {cur && !has && <option value={cur}>{cur} (ไม่อยู่ในรายการ)</option>}
      {options.map(o => <option key={o} value={o}>{o}</option>)}
    </select>
  )
}

export default function McTotalEditor({ grid, cols, opts, rids, setCell, addRow, delRow, isChanged, onAux, ro }) {
  const { weekCi, facCi, catCi, gaugeCi, mcCi, totalCi, remarkCi, weekToCi, yearCi, yearToCi } = cols
  const wrapRef = useRef(null)
  const useDrop = !!opts
  const calWeeks = opts?.calWeeks || []
  const yearOpts = opts?.years || []

  // ช่องเสริม (ถึงสัปดาห์ / ปี / ถึงปี) — ไม่บังคับ ว่าง = พฤติกรรมเดิม
  // ถ้าชีทยังไม่มีคอลัมน์นั้น ให้สร้างผ่าน onAux ตอนกรอกครั้งแรก (ไม่กระทบแถวเดิม)
  const ciOf = { weekTo: weekToCi, year: yearCi, yearTo: yearToCi, mc: mcCi }
  const auxVal = (r, kind) => (ciOf[kind] >= 0 ? val(r[ciOf[kind]]) : '')
  const setAux = (ri, kind, v) => {
    // ช่องสัปดาห์/ปีเป็นตัวเลขล้วน ส่วน MC เป็นชื่อกลุ่มเครื่อง (SKP/SKPTA) → เก็บตามที่เลือก
    const out = kind === 'mc' ? String(v).trim() : String(v).replace(/[^0-9]/g, '')
    if (ciOf[kind] >= 0) setCell(ri, ciOf[kind], out)
    else if (out) onAux(kind, ri, out)   // ค่าว่าง + ยังไม่มีคอลัมน์ = ไม่ต้องสร้างคอลัมน์เปล่า
  }

  // สัปดาห์ทั้งหมดที่แถวนี้กำหนดยอด — ตรรกะเดียวกับ Calendar.week_span ฝั่ง pipeline
  const rowSpan = r => weekSpan(calWeeks, val(r[weekCi]), auxVal(r, 'weekTo'),
    auxVal(r, 'year'), auxVal(r, 'yearTo'))

  const allGauges = useMemo(() => opts
    ? distinct(Object.values(opts.gaugeByCat).flat()) : [], [opts])
  const weekOpts = useMemo(() => {
    if (!opts) return []
    return distinct([...opts.weeks, ...grid.rows.map(r => r[weekCi])])
  }, [opts, grid, weekCi])

  // fallback datalist (เมื่อโหลด Master MC ไม่ได้)
  const catDL = useMemo(() => distinct(grid.rows.map(r => r[catCi])), [grid, catCi])
  const gaugeDL = useMemo(() => gaugeCi >= 0 ? distinct(grid.rows.map(r => r[gaugeCi])) : [], [grid, gaugeCi])
  const mcDL = useMemo(() => mcCi >= 0 ? distinct(grid.rows.map(r => r[mcCi])) : [], [grid, mcCi])

  // พูลของเครื่องที่เลือก (จาก Master MC) — พูลคือหน่วยที่ระบบนับเครื่องจริง
  // เช่น SKPLE กับ SKPTA อยู่ Group เดียวกัน → เลือกตัวไหนก็คุมเครื่องชุดเดียวกัน
  const poolInfo = (cat, gauge, mc) => {
    if (!opts?.poolByMc || !cat || !gauge || !mc) return null
    return opts.poolByMc[`${cat.toUpperCase()}|${normGauge(gauge)}|${mc.toUpperCase()}`] || null
  }
  const poolKey = (cat, gauge, mc) => {
    if (!mc) return ''
    const p = poolInfo(cat, gauge, mc)
    return p?.pool || mc.toUpperCase()
  }
  // พูลทั้งหมดของ CAT+เกจ — มีมากกว่า 1 = ต้องเลือกเครื่อง ไม่งั้นยอดจะกระจายลงทุกพูล
  const poolsOf = (cat, gauge) => {
    if (!opts?.poolByMc || !cat || !gauge) return []
    const pfx = `${cat.toUpperCase()}|${normGauge(gauge)}|`
    const seen = new Map()
    for (const [k, p] of Object.entries(opts.poolByMc)) {
      if (!k.startsWith(pfx)) continue
      const kk = p.pool || k.slice(pfx.length)
      if (!seen.has(kk)) seen.set(kk, p)
    }
    return [...seen.values()]
  }

  // กระจายทุกแถวเป็น "รายสัปดาห์" ตามคีย์ที่ระบบใช้จริง (สัปดาห์/กลุ่ม/เกจ/โรงงาน/พูล)
  // เพราะช่วงสัปดาห์ทำให้แถวที่สัปดาห์เริ่มต่างกันยังทับกันกลางช่วงได้ → เตือนตามที่ทับจริง
  // คืน { dups, mixes }
  //   dups  = แถวที่ทับกันสนิท (สัปดาห์+กลุ่ม+เกจ+โรงงาน+พูล เดียวกัน) → ระบบใช้แถวล่างสุด
  //   mixes = สัปดาห์เดียวกันมีทั้งแถว "ไม่ระบุเครื่อง" (คุมทั้ง cat+เกจ) และแถวที่ระบุเครื่อง
  //           → แถวที่ระบุเครื่องเจาะจงกว่า จึงชนะเฉพาะพูลนั้น (คนละคีย์ dup แต่ user ควรรู้)
  const overlaps = useMemo(() => {
    const byWeek = new Map()
    const byCat = new Map()
    grid.rows.forEach((r, ri) => {
      const cat = val(r[catCi]).toUpperCase()
      if (!cat) return
      const gRow = gaugeCi >= 0 ? normGauge(r[gaugeCi]) : ''
      const mcRow = auxVal(r, 'mc')
      const kCat = [cat, gRow, facCi >= 0 ? val(r[facCi]).toUpperCase() : ''].join('|')
      const k0 = [kCat, poolKey(cat, gRow, mcRow)].join('|')
      for (const w of rowSpan(r).weeks) {
        const k = `${w}|${k0}`
        if (!byWeek.has(k)) byWeek.set(k, [])
        byWeek.get(k).push(ri)
        const kc = `${w}|${kCat}`
        if (!byCat.has(kc)) byCat.set(kc, { bare: [], withMc: new Map() })
        const slot = byCat.get(kc)
        if (mcRow) slot.withMc.set(ri, mcRow)
        else slot.bare.push(ri)
      }
    })
    const dups = new Map()   // ri → { week, count }
    byWeek.forEach((ris, k) => {
      if (ris.length < 2) return
      const week = Number(k.split('|')[0])
      ris.forEach(ri => { if (!dups.has(ri)) dups.set(ri, { week, count: ris.length }) })
    })
    const mixes = new Map()  // ri → { week, mcs, bare }
    byCat.forEach((slot, kc) => {
      if (!slot.bare.length || !slot.withMc.size) return
      const week = Number(kc.split('|')[0])
      const mcs = [...new Set(slot.withMc.values())]
      slot.bare.forEach(ri => { if (!mixes.has(ri)) mixes.set(ri, { week, mcs, bare: true }) })
      slot.withMc.forEach((_m, ri) => { if (!mixes.has(ri)) mixes.set(ri, { week, mcs, bare: false }) })
    })
    return { dups, mixes }
  }, [grid, weekCi, weekToCi, yearCi, yearToCi, catCi, gaugeCi, facCi, mcCi, calWeeks, opts])

  // จำนวนเครื่องเดิมจาก Master MC ของ (กลุ่ม + เกจ + โรงงาน) — ไม่รู้ = null
  // ระบุเครื่องแล้ว = ยอดของ "พูล" เครื่องนั้น (ที่ AVA_MC จะไปแทนค่าให้) ไม่ใช่ยอดทั้ง CAT
  function baseTotal(cat, gauge, fac, mc) {
    if (!opts || !cat || !gauge) return null
    const p = poolInfo(cat, gauge, mc)
    if (p) return p.total
    const slot = opts.totalByCatGauge[`${cat.toUpperCase()}|${normGauge(gauge)}`]
    if (!slot) return null
    if (!fac) return slot.all
    const v = slot.byFac[fac.toUpperCase()]
    return v == null ? null : v
  }

  const setNum = (ri, ci, v) => setCell(ri, ci, String(v).replace(/[^0-9]/g, ''))

  // เลือกกลุ่มเครื่อง → ถ้ากลุ่มนั้นมีเกจเดียว เติมเกจให้อัตโนมัติ
  function pickCat(ri, cat) {
    setCell(ri, catCi, cat)
    if (opts && gaugeCi >= 0) {
      const gs = opts.gaugeByCat[cat] || []
      if (gs.length === 1) setCell(ri, gaugeCi, gs[0])
    }
  }

  function add() {
    addRow()
    setTimeout(() => {
      const cards = wrapRef.current?.querySelectorAll('.lockcard')
      const last = cards?.[cards.length - 1]
      const el = last?.querySelector('select, input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  const chg = (ri, ci) => ci >= 0 && isChanged(ri, ci)

  return (
    <RoCtx.Provider value={!!ro}>
    <div className="lockview" ref={wrapRef}>
      <div className="lockbar">
        <span className="lktotal">
          🔢 กำหนดจำนวนเครื่อง <b>{grid.rows.length}</b> รายการ
        </span>
        <span className="lkhint">
          ระบุว่า "สัปดาห์นั้นกลุ่มนี้มีเครื่องกี่ตัว" — ใช้แทนจำนวนเครื่องในชีท Master MC เฉพาะสัปดาห์ที่ระบุ ·
          เลือก "เครื่อง" (เช่น SKP) เมื่อจะลด/เพิ่มเฉพาะกลุ่มนั้น เว้นว่าง = ทั้งกลุ่ม+Guage ·
          กรอก "ถึงสัปดาห์" เพื่อใช้ยอดเดียวกันทั้งช่วง (เว้นว่าง = สัปดาห์เดียว) · ใส่ "ปี" เมื่อช่วงข้ามปี
        </span>
        {!ro && <button className="lkaddbtn" onClick={add}>+ เพิ่มจำนวนเครื่อง</button>}
      </div>

      {!grid.rows.length && (
        <div className="lockempty">
          ยังไม่มีการกำหนด — ทุกสัปดาห์ใช้จำนวนเครื่องจากชีท <b>Master MC</b>
          {!ro && <><br />กด <b>+ เพิ่มจำนวนเครื่อง</b> เพื่อกำหนดเฉพาะสัปดาห์ที่ต่างออกไป</>}
        </div>
      )}

      <div className="lockcards">
        {grid.rows.map((r, ri) => {
          const cat = val(r[catCi])
          const gauge = gaugeCi >= 0 ? val(r[gaugeCi]) : ''
          const fac = facCi >= 0 ? val(r[facCi]) : ''
          const mc = auxVal(r, 'mc')
          const dup = overlaps.dups.get(ri)
          const mix = overlaps.mixes.get(ri)
          const gauges = opts ? (cat && opts.gaugeByCat[cat] ? opts.gaugeByCat[cat] : allGauges) : []
          const facs = opts ? (cat && opts.facByCat[cat] ? opts.facByCat[cat] : opts.factories) : []
          // เครื่องที่มีจริงใน CAT+เกจ นั้น (ไม่มีเกจ → เครื่องทั้งหมดของ CAT)
          const mcs = opts
            ? ((cat && gauge && opts.mcByCatGauge?.[`${cat.toUpperCase()}|${normGauge(gauge)}`])
              || (cat && opts.mcByCat?.[cat]) || [])
            : []
          const pool = poolInfo(cat, gauge, mc)
          const pools = poolsOf(cat, gauge)
          const base = baseTotal(cat, gauge, fac, mc)
          const cur = Number(val(r[totalCi]))
          const hasCur = val(r[totalCi]) !== '' && !isNaN(cur)
          const diff = base != null && hasCur ? cur - base : null
          // เครื่องที่กันไว้ให้ POLY/COTTON ในขอบเขตของแถวนี้ (ระบุเครื่อง = พูลนั้น / ไม่ระบุ = ทุกพูลของโรงงานที่เลือก)
          const rsvKeep = mc
            ? (pool?.reserved || 0)
            : pools.filter(p => !fac || String(p.pool).toUpperCase().startsWith(fac.toUpperCase() + '|'))
              .reduce((a, p) => a + (p.reserved || 0), 0)
          const span = rowSpan(r)
          const isRange = span.weeks.length > 1
          const [labFrom, labTo] = spanLabel(val(r[weekCi]), auxVal(r, 'weekTo'),
            auxVal(r, 'year'), auxVal(r, 'yearTo'))
          return (
            <div key={rids?.[ri] ?? ri} className={'lockcard mct' + (dup ? ' dup' : '')}>
              {!ro && <button className="lkdel" title="ลบรายการนี้" onClick={() => delRow(ri)}>✕</button>}

              <div className="lkrow">
                {weekCi >= 0 && (
                  <label className="lkfield wk">
                    <span>สัปดาห์</span>
                    {useDrop && weekOpts.length ? (
                      <Select className={'wk ' + (chg(ri, weekCi) ? 'chg' : '')}
                        value={r[weekCi]} options={weekOpts} placeholder="— W —"
                        onChange={v => setCell(ri, weekCi, v)} />
                    ) : (
                      <div className="wkbox">
                        <span className="wprefix">W</span>
                        <input readOnly={ro} inputMode="numeric" className={chg(ri, weekCi) ? 'chg' : ''}
                          value={r[weekCi] ?? ''} placeholder="—"
                          onChange={e => setNum(ri, weekCi, e.target.value)} />
                      </div>
                    )}
                  </label>
                )}
                {weekCi >= 0 && (
                  <label className="lkfield yr">
                    <span>ปี</span>
                    <Select className={yearCi >= 0 && chg(ri, yearCi) ? 'chg' : ''}
                      value={auxVal(r, 'year')} options={yearOpts} placeholder="— ปีแผน —"
                      onChange={v => setAux(ri, 'year', v)} />
                  </label>
                )}
                {weekCi >= 0 && (
                  <label className="lkfield wk2">
                    <span>ถึงสัปดาห์</span>
                    <div className="wkbox">
                      <span className="wprefix">W</span>
                      <input readOnly={ro} inputMode="numeric"
                        className={weekToCi >= 0 && chg(ri, weekToCi) ? 'chg' : ''}
                        value={weekToCi >= 0 ? (r[weekToCi] ?? '') : ''}
                        placeholder={val(r[weekCi]) || '— เดียว —'}
                        onChange={e => setAux(ri, 'weekTo', e.target.value)} />
                    </div>
                  </label>
                )}
                {weekCi >= 0 && (
                  <label className="lkfield yr">
                    <span>ถึงปี</span>
                    <Select className={yearToCi >= 0 && chg(ri, yearToCi) ? 'chg' : ''}
                      value={auxVal(r, 'yearTo')} options={yearOpts}
                      placeholder={auxVal(r, 'year') ? `— ${auxVal(r, 'year')} —` : '— ปีเดียวกัน —'}
                      onChange={v => setAux(ri, 'yearTo', v)} />
                  </label>
                )}
                <label className="lkfield qty">
                  <span>มีเครื่อง (ตัว)</span>
                  <input readOnly={ro} inputMode="numeric" className={'big ' + (chg(ri, totalCi) ? 'chg' : '')}
                    value={r[totalCi] ?? ''} placeholder="0"
                    onChange={e => setNum(ri, totalCi, e.target.value)} />
                </label>
                <div className="lkfield grow">
                  <span>เทียบกับ Master MC</span>
                  {base == null ? (
                    <div className="mctbase none">
                      {cat && gauge ? 'ไม่พบใน Master MC' : 'เลือกกลุ่ม + Guage ก่อน'}
                    </div>
                  ) : (
                    <div className="mctbase">
                      {mc ? <span className="mctscope">{pool?.group || mc}: </span> : null}
                      เดิม <b>{base}</b>
                      {diff != null && diff !== 0 && (
                        <span className={diff > 0 ? 'up' : 'down'}>
                          {' '}→ {cur} ({diff > 0 ? '+' : ''}{diff})
                        </span>
                      )}
                      {diff === 0 && <span className="same"> → เท่าเดิม</span>}
                    </div>
                  )}
                </div>
              </div>

              <div className="lkrow">
                <label className="lkfield grow">
                  <span>กลุ่มเครื่อง</span>
                  {useDrop ? (
                    <Select className={chg(ri, catCi) ? 'chg' : ''}
                      value={r[catCi]} options={opts.cats} placeholder="— เลือกกลุ่ม —"
                      onChange={v => pickCat(ri, v)} />
                  ) : (
                    <input readOnly={ro} list="mct-cats" className={chg(ri, catCi) ? 'chg' : ''}
                      value={r[catCi] ?? ''} placeholder="เช่น SINGLE-32"
                      onChange={e => setCell(ri, catCi, e.target.value)} />
                  )}
                </label>
                {gaugeCi >= 0 && (
                  <label className="lkfield gauge">
                    <span>Guage</span>
                    {useDrop ? (
                      <Select className={chg(ri, gaugeCi) ? 'chg' : ''}
                        value={r[gaugeCi]} options={gauges} placeholder="— Guage —"
                        onChange={v => setCell(ri, gaugeCi, v)} />
                    ) : (
                      <input readOnly={ro} list="mct-gauges" className={chg(ri, gaugeCi) ? 'chg' : ''}
                        value={r[gaugeCi] ?? ''} placeholder="เช่น 20"
                        onChange={e => setCell(ri, gaugeCi, e.target.value)} />
                    )}
                  </label>
                )}
                <label className="lkfield gauge">
                  <span>เครื่อง</span>
                  {useDrop && mcs.length ? (
                    <Select className={mcCi >= 0 && chg(ri, mcCi) ? 'chg' : ''}
                      value={mc} options={mcs} placeholder="— ทุกเครื่อง —"
                      onChange={v => setAux(ri, 'mc', v)} />
                  ) : (
                    <input readOnly={ro} list="mct-mcs" className={mcCi >= 0 && chg(ri, mcCi) ? 'chg' : ''}
                      value={mc} placeholder="เช่น SKP"
                      onChange={e => setAux(ri, 'mc', e.target.value)} />
                  )}
                </label>
                {facCi >= 0 && (
                  <label className="lkfield note">
                    <span>โรงงาน</span>
                    {useDrop ? (
                      <Select className={chg(ri, facCi) ? 'chg' : ''}
                        value={r[facCi]} options={facs} placeholder="— ทุกโรงงาน —"
                        onChange={v => setCell(ri, facCi, v)} />
                    ) : (
                      <input readOnly={ro} className={chg(ri, facCi) ? 'chg' : ''}
                        value={r[facCi] ?? ''} placeholder="เว้นว่าง = ทุกโรงงาน"
                        onChange={e => setCell(ri, facCi, e.target.value)} />
                    )}
                  </label>
                )}
              </div>

              {remarkCi >= 0 && (
                <div className="lkrow">
                  <label className="lkfield grow">
                    <span>หมายเหตุ (ไม่มีผลกับการคำนวณ)</span>
                    <input readOnly={ro} className={chg(ri, remarkCi) ? 'chg' : ''}
                      value={r[remarkCi] ?? ''} placeholder="เช่น เครื่องใหม่เข้า / ส่งซ่อม 3 ตัว"
                      onChange={e => setCell(ri, remarkCi, e.target.value)} />
                  </label>
                </div>
              )}

              {isRange && (
                <div className="lkrange">
                  📅 ใช้ยอดนี้ตั้งแต่ <b>{labFrom}</b> ถึง <b>{labTo}</b> ({span.weeks.length} สัปดาห์)
                </div>
              )}

              {/* พูล = ชุดเครื่องที่ใช้แทนกันได้จริง (คอลัมน์ Group ใน Master MC)
                  ระบุเครื่อง = ปรับพูลนั้นพูลเดียว / ไม่ระบุ แต่มีหลายพูล = กระจายลงทุกพูล */}
              {mc && pool && (
                <div className="lkrange">
                  🎯 ปรับเฉพาะพูล <b>{pool.group || mc}</b>
                  {pool.mcs.length > 1
                    ? <> (เครื่อง {pool.mcs.join(', ')} ใช้เครื่องชุดเดียวกัน)</>
                    : <> (เครื่อง {mc})</>}
                  {pools.length > 1 && <> — พูลอื่นของ {cat} Guage {gauge} ไม่กระทบ</>}
                </div>
              )}
              {/* ยอดที่กรอก = ทั้งกอง รวมเครื่องที่กันไว้ให้ POLY/COTTON — บอกให้เห็นว่าเครื่องปกติเหลือเท่าไหร่ */}
              {rsvKeep > 0 && (
                <div className="lkrange">
                  🔒 ยอดนี้<b>รวม</b>เครื่องที่ MC Special กันไว้ให้ POLY/COTTON {rsvKeep} ตัวแล้ว
                  {hasCur && <> → เครื่องปกติ {Math.max(0, cur - rsvKeep)} ตัว</>}
                  {hasCur && cur < rsvKeep && <> (น้อยกว่าที่กันไว้ — เครื่องกันไว้ยังนับ {rsvKeep} ตามชีท MC Special)</>}
                </div>
              )}
              {mc && !pool && cat && gauge && (
                <div className="lkwarn">
                  ⚠ ไม่พบเครื่อง <b>{mc}</b> ใน {cat} Guage {gauge} ของ Master MC → AVA_MC จะข้ามแถวนี้
                </div>
              )}
              {!mc && pools.length > 1 && (
                <div className="lkwarn">
                  ⚠ {cat} Guage {gauge} มี {pools.length} พูลที่ใช้แทนกันไม่ได้
                  ({pools.map(p => p.group || p.mcs.join(',')).join(' · ')}) —
                  ไม่เลือก "เครื่อง" = ยอดนี้คุมทุกพูล (ระบบกระจายตามสัดส่วนเครื่องเดิม)
                  ถ้าจะลดเฉพาะกลุ่มเดียวให้เลือกเครื่องก่อน
                </div>
              )}

              {span.warn && (
                <div className="lkwarn">⚠ {span.warn}</div>
              )}

              {mix && (
                <div className="lkwarn">
                  ⚠ W{mix.week} มีทั้งรายการที่ระบุเครื่อง ({mix.mcs.join(', ')}) และรายการที่ไม่ระบุ —
                  {mix.bare
                    ? <> ยอดของรายการนี้จะถูก<b>ทับเฉพาะพูลของเครื่องนั้น</b> (พูลอื่นยังใช้ยอดนี้)</>
                    : <> รายการนี้เจาะจงกว่าจึง<b>ชนะ</b>เฉพาะพูลของ {mc}</>}
                </div>
              )}

              {dup && (
                <div className="lkwarn">
                  ⚠ สัปดาห์ทับกับอีก {dup.count - 1} รายการ (W{dup.week} · {cat}
                  {gauge ? ` · Guage ${gauge}` : ''}{mc ? ` · ${pool?.group || mc}` : ''}
                  {fac ? ` · ${fac}` : ''}) —
                  ระบบจะใช้ <b>ค่าของแถวล่างสุด</b> เท่านั้น (ไม่บวกรวม)
                </div>
              )}
            </div>
          )
        })}
      </div>

      {!useDrop && <>
        <datalist id="mct-cats">{catDL.map(c => <option key={c} value={c} />)}</datalist>
        <datalist id="mct-gauges">{gaugeDL.map(g => <option key={g} value={g} />)}</datalist>
        <datalist id="mct-mcs">{mcDL.map(m => <option key={m} value={m} />)}</datalist>
      </>}

      <div className="gridhelp">
        แต่ละการ์ด = จำนวนเครื่องของ 1 สัปดาห์ (หรือ 1 ช่วงถ้ากรอก "ถึงสัปดาห์") ·
        ต้องรันแผนใหม่ค่าถึงจะมีผล · ถ้ากรอกน้อยกว่าที่ booking จองไว้ เครื่องว่างจะติดลบ (= จองเกิน)
        {ro
          ? <> · <b>หน้านี้ดูอย่างเดียว</b> — แก้ได้ที่ ⚙️ Setting › ข้อมูลหลัก</>
          : <> · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ</>}
      </div>
    </div>
    </RoCtx.Provider>
  )
}

import React, { useMemo, useRef } from 'react'

// มุมมอง "การ์ด" ของชีท MC_Total — กำหนดจำนวนเครื่องทั้งหมดของสัปดาห์นั้น ๆ
// แต่ละการ์ด = 1 แถวในชีท อ่านเป็นประโยค: "สัปดาห์ W__ กลุ่ม __ เกจ __ มีเครื่อง __ ตัว"
// แก้ค่าลงใน grid เดิมของ Masters โดยตรง → ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน
//
// ความหมายของตัวเลข = "จำนวนเครื่องทั้งหมด" ของสัปดาห์นั้น (แทนที่ Total MC ในชีท Master MC)
//   · มีผลเฉพาะสัปดาห์ที่ระบุ — สัปดาห์อื่นกลับไปใช้ค่าเดิม
//   · booking / Lock_MC ยังถูกหักต่ออีกชั้นตามปกติ (เครื่องว่าง = ยอดนี้ − ที่จองไว้)
//   · โรงงานเว้นว่าง = ทุกโรงงาน (AVA_MC กระจายยอดตามสัดส่วนเครื่องเดิมของแต่ละพูล)
//
// การ์ดโชว์ "ค่าเดิมจาก Master MC" เทียบให้เห็นว่าเพิ่ม/ลดกี่เครื่อง และเตือนเมื่อ
// คีย์ซ้ำ (สัปดาห์+กลุ่ม+เกจ+โรงงานเดียวกัน) เพราะระบบจะใช้ค่าของแถวหลังสุดเท่านั้น

const val = v => String(v ?? '').trim()
const distinct = arr => [...new Set(arr.map(val).filter(Boolean))].sort((a, b) =>
  String(a).localeCompare(String(b), 'th', { numeric: true }))
const normGauge = v => val(v).toUpperCase().replace('GAUGE', '').replace('G', '').trim()

// dropdown ที่ "ไม่ทิ้ง" ค่าปัจจุบันแม้ไม่อยู่ในรายการ (กันข้อมูลเก่าหาย) — เติมเป็น option แรก
function Select({ value, options, onChange, placeholder, className }) {
  const cur = val(value)
  const has = cur && options.includes(cur)
  return (
    <select className={'lksel ' + (className || '')} value={cur} onChange={e => onChange(e.target.value)}>
      <option value="">{placeholder || '— เลือก —'}</option>
      {cur && !has && <option value={cur}>{cur} (ไม่อยู่ในรายการ)</option>}
      {options.map(o => <option key={o} value={o}>{o}</option>)}
    </select>
  )
}

export default function McTotalEditor({ grid, cols, opts, rids, setCell, addRow, delRow, isChanged, onWeekToInput }) {
  const { weekCi, facCi, catCi, gaugeCi, totalCi, remarkCi, weekToCi } = cols
  const wrapRef = useRef(null)
  const useDrop = !!opts

  // "ถึงสัปดาห์" (ไม่บังคับ) — ว่าง/น้อยกว่าสัปดาห์เริ่ม = สัปดาห์เดียวเหมือนเดิม
  // ถ้าชีทยังไม่มีคอลัมน์นี้ ให้สร้างผ่าน onWeekToInput ตอนกรอกครั้งแรก (ไม่กระทบแถวเดิม)
  const setWeekTo = (ri, v) => {
    const num = String(v).replace(/[^0-9]/g, '')
    if (weekToCi >= 0) setCell(ri, weekToCi, num)
    else onWeekToInput(ri, num)
  }

  const allGauges = useMemo(() => opts
    ? distinct(Object.values(opts.gaugeByCat).flat()) : [], [opts])
  const weekOpts = useMemo(() => {
    if (!opts) return []
    return distinct([...opts.weeks, ...grid.rows.map(r => r[weekCi])])
  }, [opts, grid, weekCi])

  // fallback datalist (เมื่อโหลด Master MC ไม่ได้)
  const catDL = useMemo(() => distinct(grid.rows.map(r => r[catCi])), [grid, catCi])
  const gaugeDL = useMemo(() => gaugeCi >= 0 ? distinct(grid.rows.map(r => r[gaugeCi])) : [], [grid, gaugeCi])

  // จับกลุ่มแถวตามคีย์ที่ระบบใช้จริง (สัปดาห์/กลุ่ม/เกจ/โรงงาน) → เตือนคีย์ซ้ำ
  const groups = useMemo(() => {
    const m = new Map()
    grid.rows.forEach((r, ri) => {
      const k = [val(r[weekCi]), val(r[catCi]).toUpperCase(),
        gaugeCi >= 0 ? normGauge(r[gaugeCi]) : '',
        facCi >= 0 ? val(r[facCi]).toUpperCase() : ''].join('|')
      if (!m.has(k)) m.set(k, [])
      m.get(k).push(ri)
    })
    return m
  }, [grid, weekCi, catCi, gaugeCi, facCi])

  // จำนวนเครื่องเดิมจาก Master MC ของ (กลุ่ม + เกจ + โรงงาน) — ไม่รู้ = null
  function baseTotal(cat, gauge, fac) {
    if (!opts || !cat || !gauge) return null
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
    <div className="lockview" ref={wrapRef}>
      <div className="lockbar">
        <span className="lktotal">
          🔢 กำหนดจำนวนเครื่อง <b>{grid.rows.length}</b> รายการ
        </span>
        <span className="lkhint">
          ระบุว่า "สัปดาห์นั้นกลุ่มนี้มีเครื่องกี่ตัว" — ใช้แทนจำนวนเครื่องในชีท Master MC เฉพาะสัปดาห์ที่ระบุ ·
          กรอก "ถึงสัปดาห์" เพื่อใช้ยอดเดียวกันทั้งช่วง (เว้นว่าง = สัปดาห์เดียว)
        </span>
        <button className="lkaddbtn" onClick={add}>+ เพิ่มจำนวนเครื่อง</button>
      </div>

      {!grid.rows.length && (
        <div className="lockempty">
          ยังไม่มีการกำหนด — ทุกสัปดาห์ใช้จำนวนเครื่องจากชีท <b>Master MC</b><br />
          กด <b>+ เพิ่มจำนวนเครื่อง</b> เพื่อกำหนดเฉพาะสัปดาห์ที่ต่างออกไป
        </div>
      )}

      <div className="lockcards">
        {grid.rows.map((r, ri) => {
          const cat = val(r[catCi])
          const gauge = gaugeCi >= 0 ? val(r[gaugeCi]) : ''
          const fac = facCi >= 0 ? val(r[facCi]) : ''
          const key = [val(r[weekCi]), cat.toUpperCase(), normGauge(gauge), fac.toUpperCase()].join('|')
          const g = groups.get(key) || [ri]
          const isDup = g.length > 1 && val(r[weekCi]) && cat
          const gauges = opts ? (cat && opts.gaugeByCat[cat] ? opts.gaugeByCat[cat] : allGauges) : []
          const facs = opts ? (cat && opts.facByCat[cat] ? opts.facByCat[cat] : opts.factories) : []
          const base = baseTotal(cat, gauge, fac)
          const cur = Number(val(r[totalCi]))
          const hasCur = val(r[totalCi]) !== '' && !isNaN(cur)
          const diff = base != null && hasCur ? cur - base : null
          const weekToVal = weekToCi >= 0 ? val(r[weekToCi]) : ''
          const isRange = weekToVal && Number(weekToVal) > Number(val(r[weekCi]) || 0)
          return (
            <div key={rids?.[ri] ?? ri} className={'lockcard mct' + (isDup ? ' dup' : '')}>
              <button className="lkdel" title="ลบรายการนี้" onClick={() => delRow(ri)}>✕</button>

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
                        <input inputMode="numeric" className={chg(ri, weekCi) ? 'chg' : ''}
                          value={r[weekCi] ?? ''} placeholder="—"
                          onChange={e => setNum(ri, weekCi, e.target.value)} />
                      </div>
                    )}
                  </label>
                )}
                {weekCi >= 0 && (
                  <label className="lkfield wk2">
                    <span>ถึงสัปดาห์</span>
                    <div className="wkbox">
                      <span className="wprefix">W</span>
                      <input inputMode="numeric"
                        className={weekToCi >= 0 && chg(ri, weekToCi) ? 'chg' : ''}
                        value={weekToCi >= 0 ? (r[weekToCi] ?? '') : ''}
                        placeholder={val(r[weekCi]) || '— เดียว —'}
                        onChange={e => setWeekTo(ri, e.target.value)} />
                    </div>
                  </label>
                )}
                <label className="lkfield qty">
                  <span>มีเครื่อง (ตัว)</span>
                  <input inputMode="numeric" className={'big ' + (chg(ri, totalCi) ? 'chg' : '')}
                    value={r[totalCi] ?? ''} placeholder="0"
                    onChange={e => setNum(ri, totalCi, e.target.value)} />
                </label>
                <div className="lkfield grow">
                  <span>เทียบกับ Master MC</span>
                  {base == null ? (
                    <div className="mctbase none">
                      {cat && gauge ? 'ไม่พบใน Master MC' : 'เลือกกลุ่ม + เกจ ก่อน'}
                    </div>
                  ) : (
                    <div className="mctbase">
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
                    <input list="mct-cats" className={chg(ri, catCi) ? 'chg' : ''}
                      value={r[catCi] ?? ''} placeholder="เช่น SINGLE-32"
                      onChange={e => setCell(ri, catCi, e.target.value)} />
                  )}
                </label>
                {gaugeCi >= 0 && (
                  <label className="lkfield gauge">
                    <span>เกจ (Gauge)</span>
                    {useDrop ? (
                      <Select className={chg(ri, gaugeCi) ? 'chg' : ''}
                        value={r[gaugeCi]} options={gauges} placeholder="— เกจ —"
                        onChange={v => setCell(ri, gaugeCi, v)} />
                    ) : (
                      <input list="mct-gauges" className={chg(ri, gaugeCi) ? 'chg' : ''}
                        value={r[gaugeCi] ?? ''} placeholder="เช่น 20"
                        onChange={e => setCell(ri, gaugeCi, e.target.value)} />
                    )}
                  </label>
                )}
                {facCi >= 0 && (
                  <label className="lkfield note">
                    <span>โรงงาน</span>
                    {useDrop ? (
                      <Select className={chg(ri, facCi) ? 'chg' : ''}
                        value={r[facCi]} options={facs} placeholder="— ทุกโรงงาน —"
                        onChange={v => setCell(ri, facCi, v)} />
                    ) : (
                      <input className={chg(ri, facCi) ? 'chg' : ''}
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
                    <input className={chg(ri, remarkCi) ? 'chg' : ''}
                      value={r[remarkCi] ?? ''} placeholder="เช่น เครื่องใหม่เข้า / ส่งซ่อม 3 ตัว"
                      onChange={e => setCell(ri, remarkCi, e.target.value)} />
                  </label>
                </div>
              )}

              {isRange && (
                <div className="lkrange">
                  📅 ใช้ยอดนี้ตั้งแต่ <b>W{val(r[weekCi])}</b> ถึง <b>W{weekToVal}</b> ({Number(weekToVal) - Number(val(r[weekCi])) + 1} สัปดาห์)
                </div>
              )}

              {isDup && (
                <div className="lkwarn">
                  ⚠ คีย์ซ้ำกับอีก {g.length - 1} รายการ (W{val(r[weekCi])} · {cat}
                  {gauge ? ` · เกจ ${gauge}` : ''}{fac ? ` · ${fac}` : ''}) —
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
      </>}

      <div className="gridhelp">
        แต่ละการ์ด = จำนวนเครื่องของ 1 สัปดาห์ (หรือ 1 ช่วงถ้ากรอก "ถึงสัปดาห์") · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ ·
        ต้องรันแผนใหม่ค่าถึงจะมีผล · ถ้ากรอกน้อยกว่าที่ booking จองไว้ เครื่องว่างจะติดลบ (= จองเกิน)
      </div>
    </div>
  )
}

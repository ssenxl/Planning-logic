import React, { useMemo, useRef } from 'react'
import { weekSpan, spanLabel } from '../weekSpan.js'

// มุมมอง "การ์ด" ของชีท Lock_MC — กันเครื่องไม่ให้ระบบเอาไปวางแผน
// แต่ละการ์ด = 1 แถวในชีท อ่านเป็นประโยค: "สัปดาห์ W__ กัน __ เครื่อง กลุ่ม __ เกจ __"
// แก้ค่าลงใน grid เดิมของ Masters โดยตรง → ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน
//
// การหักเครื่องออกจาก pool สนใจแค่ (สัปดาห์ + กลุ่ม + เกจ + จำนวนกัน) — แถวที่คีย์ซ้ำกันจะถูก
// "บวกรวม" → การ์ดจึงเตือนเมื่อคีย์ซ้ำ พร้อมโชว์ผลรวมจริงที่ระบบจะเอาไปหักเครื่อง
//
// ถ้ากรอก ITEM ด้วยจะได้ผลอีกชั้น = "คงความอุ่น" (AVA_MC.py) ซึ่งชั้นนี้ใช้คอลัมน์ MC ด้วย:
//   ระบุ MC → อุ่นเฉพาะกลุ่มเครื่องนั้น · เว้นว่าง → อุ่นทุกกลุ่มเครื่องใน MC_CAT
//   จำนวนที่กัน = จำนวนเครื่องที่อุ่นได้ (กัน 3 → อุ่น 3 ตัว ตัวที่ 4 เป็นเครื่องใหม่ต้อง setup)
//
// กรอก "ถึงสัปดาห์" (คอลัมน์ Week To) = กันเครื่องชุดเดิมยาวทั้งช่วง — เว้นว่าง = สัปดาห์เดียว
// กรอก "ปี"/"ถึงปี" (Year / Year To) เมื่อช่วงข้ามปี → กางช่วงตาม timeline ปฏิทินจริง
// (ไม่ต้องเดาว่าปีนั้นมี 52 หรือ 53 สัปดาห์) · เว้นว่าง = ใช้เลขสัปดาห์ตรง ๆ เหมือนเดิม
// ⚠️ คีย์ที่ระบบใช้หักเครื่องยังเป็น "เลขสัปดาห์เปล่า" ทั้ง pipeline — ปีมีผลแค่ตอนกางช่วง
//
// ช่องกลุ่ม/เกจ/เครื่อง/สัปดาห์ เป็น dropdown เลือกจากรายการจริง (opts) กัน user พิมพ์ผิด
// ถ้าโหลด opts ไม่ได้ (เช่น หา MasterMC ไม่เจอ) จะ fallback เป็นช่องพิมพ์ + datalist

const val = v => String(v ?? '').trim()
const distinct = arr => [...new Set(arr.map(val).filter(Boolean))].sort((a, b) =>
  String(a).localeCompare(String(b), 'th', { numeric: true }))

// โหมดดูอย่างเดียว (หน้า Master Data) — ส่งผ่าน context เพราะ dropdown ถูกเรียกหลายจุดในการ์ด
const RoCtx = React.createContext(false)

// dropdown ที่ "ไม่ทิ้ง" ค่าปัจจุบันแม้ไม่อยู่ในรายการ (กันข้อมูลเก่าหาย) — เติมเป็น option แรก
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

export default function LockMcEditor({ grid, cols, opts, rids, setCell, addRow, delRow, isChanged, onItemInput, onAux, ro }) {
  const { weekCi, catCi, gaugeCi, mcCi, lockCi, itemCi, weekToCi, yearCi, yearToCi } = cols
  const wrapRef = useRef(null)
  const useDrop = !!opts   // มี master → ใช้ dropdown, ไม่มี → ช่องพิมพ์
  const calWeeks = opts?.calWeeks || []
  const yearOpts = opts?.years || []

  // ช่องเสริม (ถึงสัปดาห์ / ปี / ถึงปี) — ไม่บังคับ ว่าง = พฤติกรรมเดิม
  // ถ้าชีทยังไม่มีคอลัมน์นั้น ให้สร้างผ่าน onAux ตอนกรอกครั้งแรก (ไม่กระทบแถวเดิม)
  const ciOf = { weekTo: weekToCi, year: yearCi, yearTo: yearToCi }
  const auxVal = (r, kind) => (ciOf[kind] >= 0 ? val(r[ciOf[kind]]) : '')
  const setAux = (ri, kind, v) => {
    const num = String(v).replace(/[^0-9]/g, '')
    if (ciOf[kind] >= 0) setCell(ri, ciOf[kind], num)
    else onAux(kind, ri, num)
  }

  // สัปดาห์ทั้งหมดที่แถวนี้กันเครื่อง — ใช้ตรรกะเดียวกับ Calendar.week_span ฝั่ง pipeline
  const rowSpan = r => weekSpan(calWeeks, val(r[weekCi]), auxVal(r, 'weekTo'),
    auxVal(r, 'year'), auxVal(r, 'yearTo'))

  // รายการ item ที่เคยกรอกในชีท (ใช้เป็น datalist ช่วยเตือนตอนพิมพ์)
  const itemOpts = useMemo(() => itemCi >= 0 ? distinct(grid.rows.map(r => r[itemCi])) : [], [grid, itemCi])

  // ตัวเลือกรวมทุกกลุ่ม (ใช้ตอนยังไม่เลือกกลุ่ม)
  const allGauges = useMemo(() => opts
    ? distinct(Object.values(opts.gaugeByCat).flat()) : [], [opts])
  const allMcs = useMemo(() => opts
    ? distinct(Object.values(opts.mcByCat).flat()) : [], [opts])
  // สัปดาห์: รวมของแผน + สัปดาห์ที่มีในชีทอยู่แล้ว
  const weekOpts = useMemo(() => {
    if (!opts) return []
    return distinct([...opts.weeks, ...grid.rows.map(r => r[weekCi])])
  }, [opts, grid, weekCi])

  // fallback datalist (เมื่อไม่มี opts)
  const catOpts = useMemo(() => distinct(grid.rows.map(r => r[catCi])), [grid, catCi])
  const gaugeDL = useMemo(() => gaugeCi >= 0 ? distinct(grid.rows.map(r => r[gaugeCi])) : [], [grid, gaugeCi])

  // กระจายทุกแถวเป็น "รายสัปดาห์" ตามคีย์ที่ระบบใช้จริง (สัปดาห์/กลุ่ม/เกจ) เพราะช่วงสัปดาห์
  // ทำให้แถวที่คีย์เริ่มต่างกันยังทับกันกลางช่วงได้ → เตือนตามสัปดาห์ที่ทับกันจริง
  // แต่ละแถวเก็บสัปดาห์ที่ "ยอดรวมสูงสุด" ไว้โชว์ (ยอดที่ระบบจะหักออกจาก pool จริง)
  const overlaps = useMemo(() => {
    const byWeek = new Map()
    grid.rows.forEach((r, ri) => {
      const cat = val(r[catCi]).toUpperCase()
      if (!cat) return
      const g = gaugeCi >= 0 ? val(r[gaugeCi]).toUpperCase() : ''
      for (const w of rowSpan(r).weeks) {
        const k = `${w}|${cat}|${g}`
        if (!byWeek.has(k)) byWeek.set(k, [])
        byWeek.get(k).push(ri)
      }
    })
    const info = new Map()   // ri → { week, count, sum }
    byWeek.forEach((ris, k) => {
      if (ris.length < 2) return
      const week = Number(k.split('|')[0])
      const sum = ris.reduce((a, i) => a + (Number(val(grid.rows[i][lockCi])) || 0), 0)
      ris.forEach(ri => {
        const cur = info.get(ri)
        if (!cur || sum > cur.sum) info.set(ri, { week, count: ris.length, sum })
      })
    })
    return info
  }, [grid, weekCi, weekToCi, yearCi, yearToCi, catCi, gaugeCi, lockCi, calWeeks])

  const totalLock = useMemo(
    () => grid.rows.reduce((a, r) => a + (Number(val(r[lockCi])) || 0), 0),
    [grid, lockCi])

  // เขียนค่าเฉพาะตัวเลข (จำนวนเครื่อง)
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
          🔒 กันเครื่อง <b>{grid.rows.length}</b> รายการ · รวม <b className="lknum">{totalLock}</b> เครื่อง
        </span>
        <span className="lkhint">
          เครื่องที่ "กัน" จะถูกหักออกจากเครื่องว่าง ระบบจะไม่เอาไปจัดงานในสัปดาห์นั้น ·
          กรอก "ถึงสัปดาห์" เพื่อกันยาวทั้งช่วง (เว้นว่าง = สัปดาห์เดียว) · ใส่ "ปี" เมื่อช่วงข้ามปี
        </span>
        {!ro && <button className="lkaddbtn" onClick={add}>+ เพิ่มการกันเครื่อง</button>}
      </div>

      {!grid.rows.length && (
        <div className="lockempty">
          {ro ? 'ยังไม่มีการกันเครื่อง' : <>ยังไม่มีการกันเครื่อง — กด <b>+ เพิ่มการกันเครื่อง</b> เพื่อเริ่ม</>}
        </div>
      )}

      <div className="lockcards">
        {grid.rows.map((r, ri) => {
          const cat = val(r[catCi])
          const dup = overlaps.get(ri)
          const span = rowSpan(r)
          const isRange = span.weeks.length > 1
          const [labFrom, labTo] = spanLabel(val(r[weekCi]), auxVal(r, 'weekTo'),
            auxVal(r, 'year'), auxVal(r, 'yearTo'))
          // ตัวเลือกเกจ/เครื่องของกลุ่มที่เลือก (cascading) — ยังไม่เลือกกลุ่ม → แสดงรวมทุกกลุ่ม
          const gauges = opts ? (cat && opts.gaugeByCat[cat] ? opts.gaugeByCat[cat] : allGauges) : []
          const mcs = opts ? (cat && opts.mcByCat[cat] ? opts.mcByCat[cat] : allMcs) : []
          return (
            <div key={rids?.[ri] ?? ri} className={'lockcard' + (dup ? ' dup' : '')}>
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
                  <span>กันไว้ (เครื่อง)</span>
                  <input readOnly={ro} inputMode="numeric" className={'big ' + (chg(ri, lockCi) ? 'chg' : '')}
                    value={r[lockCi] ?? ''} placeholder="0"
                    onChange={e => setNum(ri, lockCi, e.target.value)} />
                </label>
              </div>

              <div className="lkrow">
                <label className="lkfield grow">
                  <span>กลุ่มเครื่อง</span>
                  {useDrop ? (
                    <Select className={chg(ri, catCi) ? 'chg' : ''}
                      value={r[catCi]} options={opts.cats} placeholder="— เลือกกลุ่ม —"
                      onChange={v => pickCat(ri, v)} />
                  ) : (
                    <input readOnly={ro} list="lk-cats" className={chg(ri, catCi) ? 'chg' : ''}
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
                      <input readOnly={ro} list="lk-gauges" className={chg(ri, gaugeCi) ? 'chg' : ''}
                        value={r[gaugeCi] ?? ''} placeholder="เช่น 20"
                        onChange={e => setCell(ri, gaugeCi, e.target.value)} />
                    )}
                  </label>
                )}
                {mcCi >= 0 && (
                  <label className="lkfield note">
                    <span>เครื่อง (MC)</span>
                    {useDrop ? (
                      <Select className={chg(ri, mcCi) ? 'chg' : ''}
                        value={r[mcCi]} options={mcs} placeholder="— ทุกเครื่องในกลุ่ม —"
                        onChange={v => setCell(ri, mcCi, v)} />
                    ) : (
                      <input readOnly={ro} className={chg(ri, mcCi) ? 'chg' : ''}
                        value={r[mcCi] ?? ''} placeholder="เช่น FA (เว้นว่าง = ทุกเครื่อง)"
                        onChange={e => setCell(ri, mcCi, e.target.value)} />
                    )}
                  </label>
                )}
              </div>

              <div className="lkrow">
                <label className="lkfield grow item">
                  <span>🎯 Item ที่จะกันเครื่องให้ (ใส่ = เครื่องที่กันไว้ยังอุ่น ไม่ setup ใหม่ตอนกลับมาผลิต · เว้นว่าง = กันเครื่องเฉยๆ)</span>
                  <input readOnly={ro} list="lk-items" className={itemCi >= 0 && chg(ri, itemCi) ? 'chg' : ''}
                    value={itemCi >= 0 ? (r[itemCi] ?? '') : ''} placeholder="เช่น รหัส item — เว้นว่างได้"
                    onChange={e => onItemInput(ri, e.target.value)} />
                </label>
              </div>

              {isRange && (
                <div className="lkrange">
                  📅 กันเครื่องชุดนี้ตั้งแต่ <b>{labFrom}</b> ถึง <b>{labTo}</b> ({span.weeks.length} สัปดาห์)
                  — ทุกสัปดาห์ในช่วงถูกหักเครื่องเท่ากัน
                </div>
              )}

              {span.warn && (
                <div className="lkwarn">⚠ {span.warn}</div>
              )}

              {dup && (
                <div className="lkwarn">
                  ⚠ สัปดาห์ทับกับอีก {dup.count - 1} รายการ (W{dup.week} · {cat}
                  {gaugeCi >= 0 && val(r[gaugeCi]) ? ` · Guage ${val(r[gaugeCi])}` : ''}) —
                  ระบบจะ<b>บวกรวม = {dup.sum} เครื่อง</b>
                </div>
              )}
            </div>
          )
        })}
      </div>

      {!useDrop && <>
        <datalist id="lk-cats">{catOpts.map(c => <option key={c} value={c} />)}</datalist>
        <datalist id="lk-gauges">{gaugeDL.map(g => <option key={g} value={g} />)}</datalist>
      </>}
      {itemCi >= 0 && <datalist id="lk-items">{itemOpts.map(i => <option key={i} value={i} />)}</datalist>}

      <div className="gridhelp">
        แต่ละการ์ด = การกัน 1 รายการ (1 สัปดาห์ หรือ 1 ช่วงถ้ากรอก "ถึงสัปดาห์") ·
        ช่อง "เครื่อง (MC)" มีผลเฉพาะตอนกรอก Item — เว้นว่าง = อุ่นทุกกลุ่มเครื่องในกลุ่มนั้น
        {ro
          ? <> · <b>หน้านี้ดูอย่างเดียว</b> — แก้ได้ที่ ⚙️ Setting › ข้อมูลหลัก</>
          : <> · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ</>}
      </div>
    </div>
    </RoCtx.Provider>
  )
}

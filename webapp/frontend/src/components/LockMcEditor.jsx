import React, { useMemo, useRef } from 'react'

// มุมมอง "การ์ด" ของชีท Lock_MC — กันเครื่องไม่ให้ระบบเอาไปวางแผน
// แต่ละการ์ด = 1 แถวในชีท อ่านเป็นประโยค: "สัปดาห์ W__ กัน __ เครื่อง กลุ่ม __ เกจ __"
// แก้ค่าลงใน grid เดิมของ Masters โดยตรง → ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน
//
// การวางแผนสนใจแค่ (สัปดาห์ + กลุ่มเครื่อง + เกจ + จำนวนกัน) ส่วนคอลัมน์ MC (FA/SKP)
// เป็นแค่หมายเหตุ ไม่ถูกใช้คำนวณ และแถวที่คีย์ (สัปดาห์/กลุ่ม/เกจ) ซ้ำกันจะถูก "บวกรวม"
// → การ์ดจึงเตือนเมื่อคีย์ซ้ำ พร้อมโชว์ผลรวมจริงที่ระบบจะเอาไปหักเครื่อง
//
// ช่องกลุ่ม/เกจ/เครื่อง/สัปดาห์ เป็น dropdown เลือกจากรายการจริง (opts) กัน user พิมพ์ผิด
// ถ้าโหลด opts ไม่ได้ (เช่น หา MasterMC ไม่เจอ) จะ fallback เป็นช่องพิมพ์ + datalist

const val = v => String(v ?? '').trim()
const distinct = arr => [...new Set(arr.map(val).filter(Boolean))].sort((a, b) =>
  String(a).localeCompare(String(b), 'th', { numeric: true }))

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

export default function LockMcEditor({ grid, cols, opts, rids, setCell, addRow, delRow, isChanged, onItemInput }) {
  const { weekCi, catCi, gaugeCi, mcCi, lockCi, itemCi } = cols
  const wrapRef = useRef(null)
  const useDrop = !!opts   // มี master → ใช้ dropdown, ไม่มี → ช่องพิมพ์

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

  // จับกลุ่มแถวตามคีย์ที่ระบบใช้จริง (สัปดาห์/กลุ่ม/เกจ) → ใช้เตือนคีย์ซ้ำ + รวมยอด
  const groups = useMemo(() => {
    const m = new Map()
    grid.rows.forEach((r, ri) => {
      const k = [val(r[weekCi]), val(r[catCi]).toUpperCase(),
        gaugeCi >= 0 ? val(r[gaugeCi]).toUpperCase() : ''].join('|')
      if (!m.has(k)) m.set(k, [])
      m.get(k).push(ri)
    })
    return m
  }, [grid, weekCi, catCi, gaugeCi])

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
    <div className="lockview" ref={wrapRef}>
      <div className="lockbar">
        <span className="lktotal">
          🔒 กันเครื่อง <b>{grid.rows.length}</b> รายการ · รวม <b className="lknum">{totalLock}</b> เครื่อง
        </span>
        <span className="lkhint">
          เครื่องที่ "กัน" จะถูกหักออกจากเครื่องว่าง ระบบจะไม่เอาไปจัดงานในสัปดาห์นั้น
        </span>
        <button className="lkaddbtn" onClick={add}>+ เพิ่มการกันเครื่อง</button>
      </div>

      {!grid.rows.length && (
        <div className="lockempty">
          ยังไม่มีการกันเครื่อง — กด <b>+ เพิ่มการกันเครื่อง</b> เพื่อเริ่ม
        </div>
      )}

      <div className="lockcards">
        {grid.rows.map((r, ri) => {
          const cat = val(r[catCi])
          const key = [val(r[weekCi]), cat.toUpperCase(),
            gaugeCi >= 0 ? val(r[gaugeCi]).toUpperCase() : ''].join('|')
          const g = groups.get(key) || [ri]
          const isDup = g.length > 1 && val(r[weekCi]) && cat
          const sum = isDup ? g.reduce((a, i) => a + (Number(val(grid.rows[i][lockCi])) || 0), 0) : 0
          // ตัวเลือกเกจ/เครื่องของกลุ่มที่เลือก (cascading) — ยังไม่เลือกกลุ่ม → แสดงรวมทุกกลุ่ม
          const gauges = opts ? (cat && opts.gaugeByCat[cat] ? opts.gaugeByCat[cat] : allGauges) : []
          const mcs = opts ? (cat && opts.mcByCat[cat] ? opts.mcByCat[cat] : allMcs) : []
          return (
            <div key={rids?.[ri] ?? ri} className={'lockcard' + (isDup ? ' dup' : '')}>
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
                <label className="lkfield qty">
                  <span>กันไว้ (เครื่อง)</span>
                  <input inputMode="numeric" className={'big ' + (chg(ri, lockCi) ? 'chg' : '')}
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
                    <input list="lk-cats" className={chg(ri, catCi) ? 'chg' : ''}
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
                      <input list="lk-gauges" className={chg(ri, gaugeCi) ? 'chg' : ''}
                        value={r[gaugeCi] ?? ''} placeholder="เช่น 20"
                        onChange={e => setCell(ri, gaugeCi, e.target.value)} />
                    )}
                  </label>
                )}
                {mcCi >= 0 && (
                  <label className="lkfield note">
                    <span>หมายเหตุ / เครื่อง</span>
                    {useDrop ? (
                      <Select className={chg(ri, mcCi) ? 'chg' : ''}
                        value={r[mcCi]} options={mcs} placeholder="— ไม่ระบุ —"
                        onChange={v => setCell(ri, mcCi, v)} />
                    ) : (
                      <input className={chg(ri, mcCi) ? 'chg' : ''}
                        value={r[mcCi] ?? ''} placeholder="เช่น FA (ไม่บังคับ)"
                        onChange={e => setCell(ri, mcCi, e.target.value)} />
                    )}
                  </label>
                )}
              </div>

              <div className="lkrow">
                <label className="lkfield grow item">
                  <span>🎯 Item ที่จะกันเครื่องให้ (ใส่ = ไม่ setup ใหม่ตอนกลับมาผลิต · เว้นว่าง = กันเครื่องเฉยๆ)</span>
                  <input list="lk-items" className={itemCi >= 0 && chg(ri, itemCi) ? 'chg' : ''}
                    value={itemCi >= 0 ? (r[itemCi] ?? '') : ''} placeholder="เช่น รหัส item — เว้นว่างได้"
                    onChange={e => onItemInput(ri, e.target.value)} />
                </label>
              </div>

              {isDup && (
                <div className="lkwarn">
                  ⚠ คีย์ซ้ำกับอีก {g.length - 1} รายการ (W{val(r[weekCi])} · {cat}
                  {gaugeCi >= 0 && val(r[gaugeCi]) ? ` · เกจ ${val(r[gaugeCi])}` : ''}) —
                  ระบบจะ<b>บวกรวม = {sum} เครื่อง</b>
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
        แต่ละการ์ด = การกัน 1 รายการ · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ ·
        คอลัมน์ "หมายเหตุ" (FA/SKP) เป็นแค่บันทึกช่วยจำ ไม่มีผลต่อการคำนวณ
      </div>
    </div>
  )
}

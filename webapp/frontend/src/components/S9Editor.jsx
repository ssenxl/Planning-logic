import React, { useMemo, useRef, useState } from 'react'

// มุมมองเฉพาะของชีทกลุ่ม S9 (จ้างทอ) ในไฟล์ MasterMC — แก้ลง grid เดิมของ Masters โดยตรง
// จึงใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน มี 3 แบบตามชนิดชีท:
//   - mcs9   : MC S9      → การ์ด pool เครื่องจ้างทอ (ความจุต่อกลุ่ม)
//   - items9 : Item S9    → ค้นหา item ที่ "จ้างทอได้" (fallback) — 5พันกว่าแถว เรนเดอร์เฉพาะผลค้นหา
//   - s9only : S9 Only    → รายการ item ที่ "ต้องจ้างทอเสมอ"

const val = v => String(v ?? '').trim()
const distinct = arr => [...new Set(arr.map(val).filter(Boolean))]
  .sort((a, b) => String(a).localeCompare(String(b), 'th', { numeric: true }))

export default function S9Editor(props) {
  if (props.info.kind === 'mcs9') return <McS9Pool {...props} />
  return <ItemS9List {...props} />
}

// ---------- MC S9: การ์ด pool เครื่องจ้างทอ ----------
function McS9Pool({ grid, info, rids, setCell, addRow, delRow, isChanged }) {
  const { catCi, mcGroupCi, gaugeCi, totalCi, capCi, wdCi, remarkCi } = info
  const wrapRef = useRef(null)
  const chg = (ri, ci) => ci >= 0 && isChanged(ri, ci)
  const sumCol = ci => grid.rows.reduce((a, r) => a + (Number(val(r[ci])) || 0), 0)
  const totMC = totalCi >= 0 ? sumCol(totalCi) : null
  const totCap = capCi >= 0 ? sumCol(capCi) : null
  const setNum = (ri, ci, v) => setCell(ri, ci, String(v).replace(/[^0-9.]/g, ''))

  function add() {
    addRow()
    setTimeout(() => {
      const cards = wrapRef.current?.querySelectorAll('.s9card')
      const el = cards?.[cards.length - 1]?.querySelector('input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  // เรียกเป็นฟังก์ชัน (ไม่ใช่ <Field/>) เพื่อไม่ให้ input remount ทุกครั้งที่พิมพ์ → โฟกัสไม่หลุด
  const field = (label, ri, ci, { numeric, ph } = {}) => ci < 0 ? null : (
    <label className="s9field" key={label}>
      <span>{label}</span>
      <input className={chg(ri, ci) ? 'chg' : ''}
        inputMode={numeric ? 'numeric' : undefined}
        value={grid.rows[ri][ci] ?? ''} placeholder={ph || ''}
        onChange={e => numeric ? setNum(ri, ci, e.target.value) : setCell(ri, ci, e.target.value)} />
    </label>
  )

  return (
    <div className="s9view" ref={wrapRef}>
      <div className="s9bar">
        <span className="s9total">
          🧵 pool จ้างทอ <b>{grid.rows.length}</b> กลุ่ม
          {totMC != null && <> · รวม <b className="s9num">{totMC}</b> เครื่อง</>}
          {totCap != null && <> · กำลังผลิตรวม <b className="s9num">{totCap}</b>/วัน</>}
        </span>
        <span className="s9hint">เครื่องกลุ่มนี้ใช้รับงานที่ถูกส่งจ้างทอ (S9)</span>
        <button className="s9addbtn" onClick={add}>+ เพิ่มกลุ่มเครื่อง</button>
      </div>

      {!grid.rows.length && (
        <div className="s9empty">ยังไม่มีกลุ่มเครื่องจ้างทอ — กด <b>+ เพิ่มกลุ่มเครื่อง</b> เพื่อเริ่ม</div>
      )}

      <div className="s9cards">
        {grid.rows.map((r, ri) => (
          <div key={rids?.[ri] ?? ri} className="s9card">
            <button className="s9del" title="ลบกลุ่มนี้" onClick={() => delRow(ri)}>✕</button>
            <div className="s9cardhead">
              {field('กลุ่ม (MC_CAT)', ri, catCi, { ph: 'เช่น SINGLE-32' })}
              {field('MC Group', ri, mcGroupCi, { ph: 'เช่น FA' })}
              {field('เกจ', ri, gaugeCi, { ph: '20' })}
            </div>
            <div className="s9cardbody">
              {field('Total MC (เครื่อง)', ri, totalCi, { numeric: true, ph: '0' })}
              {field('Cap/Day', ri, capCi, { numeric: true, ph: '0' })}
              {field('Working day', ri, wdCi, { numeric: true, ph: '0' })}
              {field('Remark', ri, remarkCi, { ph: 'เช่น Poly' })}
            </div>
          </div>
        ))}
      </div>

      <div className="gridhelp">
        แต่ละการ์ด = 1 กลุ่มเครื่องจ้างทอ · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ
      </div>
    </div>
  )
}

// ---------- Item S9 / S9 Only: รายการ item แบบค้นหาก่อน ----------
function ItemS9List({ grid, info, rids, setCell, addRow, delRow, isChanged, isNewRow }) {
  const { kind, itemCi, mgCi, gaugeCi } = info
  const isOnly = kind === 's9only'
  const [q, setQ] = useState('')
  const [limit, setLimit] = useState(80)
  const rowsRef = useRef(null)
  const chg = (ri, ci) => ci >= 0 && isChanged(ri, ci)

  const mgOpts = useMemo(() => mgCi >= 0 ? distinct(grid.rows.map(r => r[mgCi])) : [], [grid, mgCi])
  const gOpts = useMemo(() => gaugeCi >= 0 ? distinct(grid.rows.map(r => r[gaugeCi])) : [], [grid, gaugeCi])

  const query = q.trim().toLowerCase()
  // แถวใหม่ (ยังไม่บันทึก) โชว์บนสุดเสมอ ตามด้วยแถวที่ match คำค้น
  const { ordered, matchCount, total } = useMemo(() => {
    const news = [], rest = []
    grid.rows.forEach((_, ri) => (isNewRow(ri) ? news : rest).push(ri))
    const cols = [itemCi, mgCi, gaugeCi].filter(c => c >= 0)
    const match = ri => !query ||
      cols.map(c => String(grid.rows[ri][c] ?? '').toLowerCase()).join(' ').includes(query)
    const filtered = rest.filter(match)
    return { ordered: [...news, ...filtered], matchCount: news.length + filtered.length, total: grid.rows.length }
  }, [grid, query, itemCi, mgCi, gaugeCi, isNewRow])

  const shown = ordered.slice(0, limit)

  function add() {
    addRow()
    setTimeout(() => {
      const el = rowsRef.current?.querySelector('.s9row.newrow input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  return (
    <div className="s9view">
      <div className="s9bar">
        {isOnly
          ? <span className="s9warn">⚠️ item กลุ่มนี้ระบบจะ<b>ส่งจ้างทอเสมอ</b> (plan = วันนี้+3)</span>
          : <span className="s9hint">item ที่ <b>จ้างทอได้</b> — ระบบใช้เป็นทางเลือกเมื่อเครื่องปกติไม่พอ</span>}
        <input className="s9search" placeholder="🔍 ค้นหา ITEM_CODE / กลุ่ม / เกจ..."
          value={q} onChange={e => { setQ(e.target.value); setLimit(80) }} />
        <span className="s9count">
          {query && <>พบ <b>{matchCount}</b> / </>}{total.toLocaleString()} รายการ
        </span>
        <button className="s9addbtn" onClick={add}>+ เพิ่ม item</button>
      </div>

      <div className="s9rows" ref={rowsRef}>
        {shown.map(ri => (
          <div key={rids?.[ri] ?? ri} className={'s9row' + (isNewRow(ri) ? ' newrow' : '')}>
            <input className={'s9item ' + (chg(ri, itemCi) ? 'chg' : '')}
              value={grid.rows[ri][itemCi] ?? ''} placeholder="ITEM_CODE"
              onChange={e => setCell(ri, itemCi, e.target.value)} />
            {mgCi >= 0 && (
              <input list="s9-mg" className={'s9mg ' + (chg(ri, mgCi) ? 'chg' : '')}
                value={grid.rows[ri][mgCi] ?? ''} placeholder="กลุ่มเครื่อง"
                onChange={e => setCell(ri, mgCi, e.target.value)} />
            )}
            {gaugeCi >= 0 && (
              <input list="s9-g" className={'s9g ' + (chg(ri, gaugeCi) ? 'chg' : '')}
                value={grid.rows[ri][gaugeCi] ?? ''} placeholder="เกจ"
                onChange={e => setCell(ri, gaugeCi, e.target.value)} />
            )}
            <button className="s9del" title="ลบรายการนี้" onClick={() => delRow(ri)}>✕</button>
          </div>
        ))}
        {!shown.length && (
          <div className="s9empty">{query ? 'ไม่พบ item ตรงคำค้น' : 'ยังไม่มีข้อมูล — กด + เพิ่ม item'}</div>
        )}
      </div>

      {ordered.length > limit && (
        <button className="s9more" onClick={() => setLimit(l => l + 200)}>
          แสดงเพิ่ม (เหลืออีก {(ordered.length - limit).toLocaleString()} รายการ)
        </button>
      )}

      <datalist id="s9-mg">{mgOpts.map(o => <option key={o} value={o} />)}</datalist>
      <datalist id="s9-g">{gOpts.map(o => <option key={o} value={o} />)}</datalist>

      <div className="gridhelp">
        พิมพ์เพื่อค้นหา · แก้/ลบได้ทันที · เพิ่มแล้วแถวใหม่จะขึ้นบนสุด · แก้เสร็จกด <b>บันทึก</b> (Ctrl+S)
      </div>
    </div>
  )
}

import React, { useMemo, useRef, useState } from 'react'

// มุมมองเฉพาะของชีท "config เล็ก" ในไฟล์ MasterMC — แก้ลง grid เดิมของ Masters โดยตรง
// (ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน) ขับด้วย descriptor (info) ที่ Masters สร้างจากหัวคอลัมน์:
//   mcspecial   : MC Special    → การ์ดกันเครื่องให้ POLY/COTTON
//   itemspecial : Item Special  → การ์ด override วันทำงาน/ชั่วโมง ราย item
//   substitute  : MC substitute → การ์ดเครื่องทดแทน
//   spare       : Spare part    → ลิสต์เครื่องสำรอง จัดกลุ่มตาม Factory + ค้นหา

const val = v => String(v ?? '').trim()

export default function ConfigEditor(props) {
  if (props.info.kind === 'spare') return <SpareList {...props} />
  return <CardList {...props} />
}

const sumCol = (rows, ci) => ci < 0 ? 0 : rows.reduce((a, r) => a + (Number(val(r[ci])) || 0), 0)

// ---------- การ์ด (mcspecial / itemspecial / substitute) ----------
function CardList({ grid, info, rids, setCell, addRow, delRow, isChanged }) {
  const { headline, hint, addLabel, itemWord, titleField, idFields = [], numFields = [], noteFields = [], summary = [] } = info
  const wrapRef = useRef(null)
  const chg = (ri, ci) => ci >= 0 && isChanged(ri, ci)
  const setNum = (ri, ci, v) => setCell(ri, ci, String(v).replace(/[^0-9.]/g, ''))

  function add() {
    addRow()
    setTimeout(() => {
      const cards = wrapRef.current?.querySelectorAll('.cfgcard')
      const el = cards?.[cards.length - 1]?.querySelector('input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  const textInput = (ri, ci, ph) => (
    <input className={chg(ri, ci) ? 'chg' : ''} value={grid.rows[ri][ci] ?? ''} placeholder={ph || ''}
      onChange={e => setCell(ri, ci, e.target.value)} />
  )

  return (
    <div className="cfgview" ref={wrapRef}>
      <div className="cfgbar">
        <span className="cfgtotal">{headline} · <b>{grid.rows.length}</b> {itemWord}
          {summary.map(f => <span key={f.ci}> · {f.label} รวม <b className="cfgnum">{sumCol(grid.rows, f.ci)}</b></span>)}
        </span>
        {hint && <span className="cfghint">{hint}</span>}
        <button className="cfgaddbtn" onClick={add}>{addLabel}</button>
      </div>

      {!grid.rows.length && <div className="cfgempty">ยังไม่มีข้อมูล — กด <b>{addLabel}</b> เพื่อเริ่ม</div>}

      <div className="cfgcards">
        {grid.rows.map((r, ri) => (
          <div key={rids?.[ri] ?? ri} className="cfgcard">
            <button className="cfgdel" title="ลบรายการนี้" onClick={() => delRow(ri)}>✕</button>

            {titleField && (
              <label className="cfgfield title">
                <span>🎯 {titleField.label}</span>
                {textInput(ri, titleField.ci, titleField.label)}
              </label>
            )}

            {idFields.length > 0 && (
              <div className="cfgline">
                {idFields.map(f => (
                  <label key={f.ci} className="cfgfield">
                    <span>{f.label}</span>
                    {textInput(ri, f.ci)}
                  </label>
                ))}
              </div>
            )}

            {numFields.length > 0 && (
              <div className="cfgline nums">
                {numFields.map(f => (
                  <label key={f.ci} className={'cfgfield num' + (f.accent ? ' ' + f.accent : '')}>
                    <span>{f.label}</span>
                    <input className={'big ' + (chg(ri, f.ci) ? 'chg' : '')} inputMode="numeric"
                      value={grid.rows[ri][f.ci] ?? ''} placeholder="0"
                      onChange={e => setNum(ri, f.ci, e.target.value)} />
                  </label>
                ))}
              </div>
            )}

            {noteFields.map(f => (
              <label key={f.ci} className="cfgfield note">
                <span>{f.label}</span>
                {textInput(ri, f.ci)}
              </label>
            ))}
          </div>
        ))}
      </div>

      <div className="gridhelp">แต่ละการ์ด = 1 {itemWord} · แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ</div>
    </div>
  )
}

// ---------- Spare part: ลิสต์จัดกลุ่มตาม Factory + ค้นหา ----------
function SpareList({ grid, info, rids, setCell, addRow, delRow, isChanged, isNewRow }) {
  const { headline, addLabel, groupCi, spareCi, lineCis = [], summary = [] } = info
  const [q, setQ] = useState('')
  const rowsRef = useRef(null)
  const chg = (ri, ci) => ci >= 0 && isChanged(ri, ci)
  const setNum = (ri, ci, v) => setCell(ri, ci, String(v).replace(/[^0-9.]/g, ''))

  const query = q.trim().toLowerCase()
  const searchCis = [groupCi, ...lineCis.map(f => f.ci)].filter(c => c >= 0)
  const matches = ri => !query ||
    searchCis.map(c => String(grid.rows[ri][c] ?? '').toLowerCase()).join(' ').includes(query)

  // แถวใหม่โชว์บนสุดเสมอ ที่เหลือจัดกลุ่มตาม Factory
  const { newRows, groups } = useMemo(() => {
    const news = [], byGroup = new Map()
    grid.rows.forEach((r, ri) => {
      if (isNewRow(ri)) { news.push(ri); return }
      if (!matches(ri)) return
      const g = groupCi >= 0 ? (val(r[groupCi]) || '— ไม่ระบุ —') : '—'
      if (!byGroup.has(g)) byGroup.set(g, [])
      byGroup.get(g).push(ri)
    })
    const groups = [...byGroup.entries()].sort((a, b) =>
      String(a[0]).localeCompare(String(b[0]), 'th', { numeric: true }))
    return { newRows: news, groups }
  }, [grid, query, groupCi, isNewRow])

  function add() {
    addRow()
    setTimeout(() => {
      const el = rowsRef.current?.querySelector('.cfgrow.newrow input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  const rowEl = ri => (
    <div key={rids?.[ri] ?? ri} className={'cfgrow' + (isNewRow(ri) ? ' newrow' : '')}>
      {lineCis.map(f => (
        <input key={f.ci} className={'cfgcell ' + (chg(ri, f.ci) ? 'chg' : '')}
          value={grid.rows[ri][f.ci] ?? ''} placeholder={f.label}
          onChange={e => setCell(ri, f.ci, e.target.value)} />
      ))}
      <label className="cfgsparebox">
        <span>สำรอง</span>
        <input className={'big ' + (chg(ri, spareCi) ? 'chg' : '')} inputMode="numeric"
          value={grid.rows[ri][spareCi] ?? ''} placeholder="0"
          onChange={e => setNum(ri, spareCi, e.target.value)} />
      </label>
      <button className="cfgdel" title="ลบรายการนี้" onClick={() => delRow(ri)}>✕</button>
    </div>
  )

  return (
    <div className="cfgview">
      <div className="cfgbar">
        <span className="cfgtotal">{headline}
          {summary.map(f => <span key={f.ci}> · {f.label} <b className="cfgnum">{sumCol(grid.rows, f.ci)}</b> เครื่อง</span>)}
        </span>
        <input className="cfgsearch" placeholder="🔍 ค้นหา Factory / กลุ่ม / เกจ..."
          value={q} onChange={e => setQ(e.target.value)} />
        <button className="cfgaddbtn" onClick={add}>{addLabel}</button>
      </div>

      <div className="cfgrows" ref={rowsRef}>
        {newRows.length > 0 && (
          <div className="cfggroup">
            <div className="cfggrouphead">➕ รายการใหม่ (ยังไม่บันทึก)</div>
            {newRows.map(rowEl)}
          </div>
        )}
        {groups.map(([g, ris]) => (
          <div key={g} className="cfggroup">
            <div className="cfggrouphead">
              {g} <span className="cfggcount">{ris.length} รายการ · สำรอง {ris.reduce((a, ri) => a + (Number(val(grid.rows[ri][spareCi])) || 0), 0)}</span>
            </div>
            {ris.map(rowEl)}
          </div>
        ))}
        {!newRows.length && !groups.length && (
          <div className="cfgempty">{query ? 'ไม่พบรายการตรงคำค้น' : 'ยังไม่มีข้อมูล'}</div>
        )}
      </div>

      <div className="gridhelp">จัดกลุ่มตาม Factory · แก้/ลบได้ทันที · แก้เสร็จกด <b>บันทึก</b> (Ctrl+S)</div>
    </div>
  )
}

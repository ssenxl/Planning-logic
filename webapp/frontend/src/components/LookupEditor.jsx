import React, { useMemo, useRef, useState } from 'react'

// มุมมอง "ค้นหาก่อน" สำหรับชีทใหญ่มาก (เช่น Master_Item 6 หมื่นแถว) ที่เปิดเป็นตารางดิบไม่ไหว
// - เรนเดอร์เฉพาะแถวที่ตรงคำค้น (จำกัดทีละก้อน) → เปิดไม่ค้าง
// - แก้/เพิ่ม/ลบได้ลง grid เดิมของ Masters โดยตรง (ใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน)
// - แถวใหม่ (ยังไม่บันทึก) โชว์บนสุดเสมอ

const PAGE = 60

export default function LookupEditor({ grid, rids, setCell, addRow, delRow, isChanged, isNewRow }) {
  const cols = grid.columns
  const [q, setQ] = useState('')
  const [limit, setLimit] = useState(PAGE)
  const rowsRef = useRef(null)
  const chg = (ri, ci) => isChanged(ri, ci)

  const query = q.trim().toLowerCase()
  const { ordered, matchCount, total } = useMemo(() => {
    const news = [], rest = []
    grid.rows.forEach((_, ri) => (isNewRow(ri) ? news : rest).push(ri))
    const match = ri => !query ||
      grid.rows[ri].some(v => String(v ?? '').toLowerCase().includes(query))
    const filtered = query ? rest.filter(match) : rest
    return { ordered: [...news, ...filtered], matchCount: news.length + filtered.length, total: grid.rows.length }
  }, [grid, query, isNewRow])

  const shown = ordered.slice(0, limit)

  function add() {
    addRow()
    setTimeout(() => {
      const el = rowsRef.current?.querySelector('.lkuprow.newrow input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  return (
    <div className="lkupview">
      <div className="lkupbar">
        <input className="lkupsearch" autoFocus placeholder="🔍 ค้นหาทุกคอลัมน์..."
          value={q} onChange={e => { setQ(e.target.value); setLimit(PAGE) }} />
        <span className="lkupcount">
          {query && <>พบ <b>{matchCount.toLocaleString()}</b> / </>}{total.toLocaleString()} แถว
        </span>
        <button className="lkupaddbtn" onClick={add}>+ เพิ่มแถว</button>
      </div>

      {!query && (
        <div className="lkuphint">พิมพ์เพื่อค้นหา — ชีทนี้ใหญ่มาก จึงแสดงเฉพาะผลค้นหา (แก้/ลบได้ทันที)</div>
      )}

      <div className="lkuprows" ref={rowsRef}>
        <div className="lkuphead">
          {cols.map((c, ci) => <span key={ci} className="lkuphcell" title={c}>{c}</span>)}
          <span className="lkuphact" />
        </div>
        {shown.map(ri => (
          <div key={rids?.[ri] ?? ri} className={'lkuprow' + (isNewRow(ri) ? ' newrow' : '')}>
            {cols.map((_, ci) => (
              <input key={ci} className={'lkupcell' + (chg(ri, ci) ? ' chg' : '')}
                value={grid.rows[ri][ci] ?? ''}
                onChange={e => setCell(ri, ci, e.target.value)} />
            ))}
            <button className="lkupdel" title="ลบแถวนี้" onClick={() => delRow(ri)}>✕</button>
          </div>
        ))}
        {!shown.length && (
          <div className="lkupempty">{query ? 'ไม่พบแถวตรงคำค้น' : 'ยังไม่มีข้อมูล'}</div>
        )}
      </div>

      {ordered.length > limit && (
        <button className="lkupmore" onClick={() => setLimit(l => l + 200)}>
          แสดงเพิ่ม (เหลืออีก {(ordered.length - limit).toLocaleString()} แถว)
        </button>
      )}

      <div className="gridhelp">
        แสดงเฉพาะผลค้นหาเพื่อความเร็ว · แถวใหม่ขึ้นบนสุด · แก้เสร็จกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ
      </div>
    </div>
  )
}

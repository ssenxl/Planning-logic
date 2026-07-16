import React, { useMemo, useState } from 'react'

// มุมมองตารางของชีท Master MC — คงเป็นตารางแก้ได้ แต่ช่วยให้อ่านง่ายขึ้น:
//   - ตรึง (freeze) คอลัมน์คีย์ MC_CAT / MC / เกจ ไว้ซ้าย เวลาเลื่อนดูคอลัมน์ความจุทางขวา
//   - คั่นด้วยหัวกลุ่ม Factory + ยอดรวมเครื่องต่อกลุ่ม
//   - ค้นหา + แถวใหม่ขึ้นบนสุด
// แก้ลง grid เดิมของ Masters โดยตรง (บันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน)
// ฟีเจอร์ขั้นสูง (กรองรายคอลัมน์ / เปลี่ยนชื่อ / เพิ่มคอลัมน์) ใช้ปุ่ม "🧾 ตาราง" แบบเดิม

const ROWNUM_W = 44
const toNum = v => Number(String(v ?? '').trim()) || 0

export default function MasterMcTable({ grid, info, rids, setCell, delRow, addRow, isChanged, isNewRow }) {
  const { factoryCi, keyCis, totalCi } = info
  const cols = grid.columns
  const keySet = useMemo(() => new Set(keyCis), [keyCis])
  const [q, setQ] = useState('')

  // เดาความกว้างจากหัว + ค่าตัวอย่าง 60 แถวแรก
  const widths = useMemo(() => cols.map((c, ci) => {
    let len = String(c).length
    for (let i = 0; i < Math.min(grid.rows.length, 60); i++) len = Math.max(len, String(grid.rows[i][ci] ?? '').length)
    return Math.round(Math.max(64, Math.min(190, 40 + len * 7.4)))
  }), [grid])

  // left offset ของคอลัมน์ที่ตรึง (rownum + คีย์ตามลำดับที่ปรากฏ)
  const leftOf = useMemo(() => {
    const m = {}; let acc = ROWNUM_W
    cols.forEach((_, ci) => { if (keySet.has(ci)) { m[ci] = acc; acc += widths[ci] } })
    return m
  }, [cols, keySet, widths])

  const query = q.trim().toLowerCase()
  const { news, groups } = useMemo(() => {
    const news = [], byG = new Map()
    grid.rows.forEach((r, ri) => {
      if (isNewRow(ri)) { news.push(ri); return }
      if (query && !r.some(v => String(v ?? '').toLowerCase().includes(query))) return
      const g = factoryCi >= 0 ? (String(r[factoryCi] ?? '').trim() || '— ไม่ระบุ —') : '—'
      if (!byG.has(g)) byG.set(g, [])
      byG.get(g).push(ri)
    })
    const groups = [...byG.entries()].sort((a, b) => String(a[0]).localeCompare(String(b[0]), 'th', { numeric: true }))
    return { news, groups }
  }, [grid, query, factoryCi, isNewRow])

  const totalMC = totalCi >= 0 ? grid.rows.reduce((a, r) => a + toNum(r[totalCi]), 0) : null
  const colCount = cols.length + 2

  const cls = ci => keySet.has(ci) ? 'mmcfrz' : ''
  const sty = ci => keySet.has(ci)
    ? { left: leftOf[ci], minWidth: widths[ci], width: widths[ci] }
    : { minWidth: widths[ci] }

  const rowEls = ris => ris.map(ri => (
    <tr key={rids?.[ri] ?? ri} className={isNewRow(ri) ? 'newrow' : ''}>
      <td className="mmcrownum">{ri + 1}</td>
      {cols.map((_, ci) => (
        <td key={ci} className={cls(ci)} style={sty(ci)}>
          <input className={isChanged(ri, ci) ? 'chg' : ''} value={grid.rows[ri][ci] ?? ''}
            onChange={e => setCell(ri, ci, e.target.value)} />
        </td>
      ))}
      <td className="mmcact"><button className="mmcdel" title="ลบแถว" onClick={() => delRow(ri)}>✕</button></td>
    </tr>
  ))

  const groupHead = (label, extra) => (
    <tr className="mmcgrouprow">
      <td className="mmcgroupcell" colSpan={colCount}>
        <span className="mmcgroupinner">{label}{extra && <span className="mmcgcount">{extra}</span>}</span>
      </td>
    </tr>
  )

  return (
    <div className="mmcview">
      <div className="mmcbar">
        <span className="mmctotal">
          🛠 เครื่องทั้งหมด <b>{grid.rows.length}</b> กลุ่ม
          {totalMC != null && <> · รวม <b className="mmcnum">{totalMC}</b> เครื่อง</>}
        </span>
        <input className="mmcsearch" placeholder="🔍 ค้นหาทุกคอลัมน์..." value={q} onChange={e => setQ(e.target.value)} />
        <button className="mmcaddbtn" onClick={addRow}>+ เพิ่มแถว</button>
      </div>

      <div className="mmcwrap">
        <table className="mmctable">
          <thead>
            <tr>
              <th className="mmcrownum">#</th>
              {cols.map((c, ci) => <th key={ci} className={cls(ci)} style={sty(ci)} title={c}>{c}</th>)}
              <th className="mmcact"></th>
            </tr>
          </thead>
          <tbody>
            {news.length > 0 && <>{groupHead('➕ แถวใหม่ (ยังไม่บันทึก)')}{rowEls(news)}</>}
            {groups.map(([g, ris]) => (
              <React.Fragment key={g}>
                {groupHead(g, `${ris.length} กลุ่ม${totalCi >= 0 ? ` · รวม ${ris.reduce((a, ri) => a + toNum(grid.rows[ri][totalCi]), 0)} เครื่อง` : ''}`)}
                {rowEls(ris)}
              </React.Fragment>
            ))}
            {!news.length && !groups.length && (
              <tr><td className="mmcrownum" /><td colSpan={colCount - 1} className="hint">ไม่พบแถวตรงคำค้น</td></tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="gridhelp">
        คอลัมน์ <b>MC_CAT / MC / เกจ</b> ตรึงไว้ซ้าย · คั่นกลุ่มตาม Factory · แก้/ลบได้ทันที ·
        ต้องการกรองรายคอลัมน์/เปลี่ยนชื่อ กด <b>🧾 ตาราง</b> · บันทึก (Ctrl+S)
      </div>
    </div>
  )
}

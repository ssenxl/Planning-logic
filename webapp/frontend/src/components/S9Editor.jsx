import React, { useMemo, useRef } from 'react'
import DataTable from './DataTable.jsx'

// มุมมองเฉพาะของชีทกลุ่ม S9 (จ้างทอ) ในไฟล์ MasterMC — แก้ลง grid เดิมของ Masters โดยตรง
// จึงใช้ปุ่มบันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน มี 3 แบบตามชนิดชีท:
//   - mcs9   : MC S9      → การ์ด pool เครื่องจ้างทอ (ความจุต่อกลุ่ม)
//   - items9 : Item S9    → ตาราง item ที่ "จ้างทอได้" (fallback) — 5พันกว่าแถว
//   - s9only : S9 Only    → ตาราง item ที่ "ต้องจ้างทอเสมอ"
// สองแบบหลังใช้ตารางกลาง DataTable เหมือนชีทอื่น ต่างแค่ป้ายอธิบายบนแถบเครื่องมือ

const val = v => String(v ?? '').trim()

// ตัวเลือกของ 2 คอลัมน์ที่เป็น dropdown ในชีท MC S9 (ค่าที่เก็บลงไฟล์ = value ล้วน)
//   Material : ชนิดผ้าที่เจ้านั้นทอ — ไม่เลือก = BLEND (ผ้าผสม) ตามที่ user กำหนด 16 ก.ย. 2569
//   Status   : เครื่องชุดนี้ยังใช้อยู่ไหม — ไม่เลือก = Active (ไม่งั้นเครื่องหายจากแผนเงียบ ๆ)
// Planning.py `_s9_rows_init` อ่านค่าชุดเดียวกันนี้ — แก้ที่นี่ต้องแก้ที่นั่นด้วย
export const S9_MATERIALS = [
  { value: 'POLY', label: 'POLY (โพลีล้วน)' },
  { value: 'COTTON', label: 'COTTON (ฝ้ายล้วน)' },
  { value: 'BLEND', label: 'BLEND (ผ้าผสม TC/CD)' },
]
export const S9_STATUS = [
  { value: 'Active', label: 'Active (ทำงาน)' },
  { value: 'Inactive', label: 'Inactive (หยุดใช้)' },
]

// map ให้ DataTable: คอลัมน์ไหนเป็น dropdown บ้าง (ใช้ตอนดูแบบ "ตาราง")
export function s9Enums(info) {
  if (!info || info.kind !== 'mcs9') return undefined
  const e = {}
  if (info.matCi >= 0) e[info.matCi] = { options: S9_MATERIALS, placeholder: '— ไม่เลือก = BLEND' }
  if (info.statusCi >= 0) e[info.statusCi] = { options: S9_STATUS, placeholder: '— ไม่เลือก = Active' }
  return Object.keys(e).length ? e : undefined
}

export default function S9Editor(props) {
  if (props.info.kind === 'mcs9') return <McS9Pool {...props} />
  return <ItemS9List {...props} />
}

// ---------- MC S9: การ์ด pool เครื่องจ้างทอ ----------
function McS9Pool({ grid, info, rids, setCell, addRow, delRow, isChanged, ro }) {
  const { suppCi = -1, catCi, mcGroupCi, gaugeCi, totalCi, capCi, wdCi, remarkCi,
    matCi = -1, statusCi = -1 } = info
  const wrapRef = useRef(null)
  const chg = (ri, ci) => ci >= 0 && isChanged(ri, ci)
  // แถวที่ Status = Inactive ไม่ถูกนับเป็นกำลังผลิต (Planning.py ข้ามแถวพวกนี้เหมือนกัน)
  const isOff = r => statusCi >= 0 && val(r[statusCi]).toUpperCase() === 'INACTIVE'
  const sumCol = ci => grid.rows.reduce((a, r) => a + (isOff(r) ? 0 : Number(val(r[ci])) || 0), 0)
  const totMC = totalCi >= 0 ? sumCol(totalCi) : null
  const totCap = capCi >= 0 ? sumCol(capCi) : null
  const nOff = grid.rows.filter(isOff).length
  // ชีทรุ่นใหม่ 1 แถว = เครื่องของผู้รับจ้าง 1 เจ้า (เพดานแยกรายเจ้า) → บอกจำนวนเจ้าไว้ด้วย
  const nSupp = suppCi >= 0
    ? new Set(grid.rows.map(r => val(r[suppCi])).filter(Boolean)).size
    : null
  const setNum = (ri, ci, v) => setCell(ri, ci, String(v).replace(/[^0-9.]/g, ''))

  function add() {
    addRow()
    setTimeout(() => {
      const cards = wrapRef.current?.querySelectorAll('.s9card')
      const el = cards?.[cards.length - 1]?.querySelector('input')
      if (el) { el.scrollIntoView({ block: 'nearest' }); el.focus() }
    }, 0)
  }

  // ช่องที่เลือกจากรายการ (dropdown) — ค่าที่ไม่รู้จักยังโชว์ไว้ ไม่ลบทิ้งเงียบ ๆ
  const picker = (label, ri, ci, options, placeholder) => ci < 0 ? null : (
    <label className="s9field" key={label}>
      <span>{label}</span>
      {(() => {
        const cur = val(grid.rows[ri][ci])
        const known = options.some(o => o.value === cur)
        return (
          <select className={(chg(ri, ci) ? 'chg ' : '') + 's9sel' + (!known && cur ? ' unknown' : '')}
            value={cur} disabled={ro} onChange={e => setCell(ri, ci, e.target.value)}>
            <option value="">{placeholder}</option>
            {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            {!known && cur && <option value={cur}>{cur} (ค่าที่ระบบไม่รู้จัก)</option>}
          </select>
        )
      })()}
    </label>
  )

  // เรียกเป็นฟังก์ชัน (ไม่ใช่ <Field/>) เพื่อไม่ให้ input remount ทุกครั้งที่พิมพ์ → โฟกัสไม่หลุด
  const field = (label, ri, ci, { numeric, ph } = {}) => ci < 0 ? null : (
    <label className="s9field" key={label}>
      <span>{label}</span>
      <input className={chg(ri, ci) ? 'chg' : ''} readOnly={ro}
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
          {nSupp != null && <> · <b className="s9num">{nSupp}</b> เจ้า</>}
          {totMC != null && <> · รวม <b className="s9num">{totMC}</b> เครื่อง</>}
          {!!nOff && <> · <b className="s9off">หยุดใช้ {nOff}</b></>}
          {totCap != null && <> · กำลังผลิตรวม <b className="s9num">{totCap}</b>/วัน</>}
        </span>
        <span className="s9hint">เครื่องกลุ่มนี้ใช้รับงานที่ถูกส่งจ้างทอ (S9)</span>
        {!ro && <button className="s9addbtn" onClick={add}>+ เพิ่มกลุ่มเครื่อง</button>}
      </div>

      {!grid.rows.length && (
        <div className="s9empty">
          {ro ? 'ยังไม่มีกลุ่มเครื่องจ้างทอ' : <>ยังไม่มีกลุ่มเครื่องจ้างทอ — กด <b>+ เพิ่มกลุ่มเครื่อง</b> เพื่อเริ่ม</>}
        </div>
      )}

      <div className="s9cards">
        {grid.rows.map((r, ri) => (
          <div key={rids?.[ri] ?? ri} className="s9card">
            {!ro && <button className="s9del" title="ลบกลุ่มนี้" onClick={() => delRow(ri)}>✕</button>}
            <div className="s9cardhead">
              {field('Supp (ผู้รับจ้าง)', ri, suppCi, { ph: 'เช่น INTERCOT' })}
              {field('กลุ่ม (MC_CAT)', ri, catCi, { ph: 'เช่น SINGLE-32' })}
              {field('MC Group', ri, mcGroupCi, { ph: 'เช่น FA' })}
              {/* เลขเกจอย่างเดียว — กระบอกอยู่ที่ MC_CAT แล้ว (user 2 ต.ค. 2569 เลิกรูป 28G34") */}
              {field('Guage', ri, gaugeCi, { ph: suppCi >= 0 ? '28' : '20' })}
            </div>
            <div className="s9cardbody">
              {field('Total MC (เครื่อง)', ri, totalCi, { numeric: true, ph: '0' })}
              {picker('Material (ชนิดผ้า)', ri, matCi, S9_MATERIALS, '— ไม่เลือก = BLEND')}
              {picker('Status', ri, statusCi, S9_STATUS, '— ไม่เลือก = Active')}
              {field('Cap/Day', ri, capCi, { numeric: true, ph: '0' })}
              {field('Working day', ri, wdCi, { numeric: true, ph: '0' })}
              {field('Remark', ri, remarkCi, { ph: 'เช่น Poly' })}
            </div>
          </div>
        ))}
      </div>

      <div className="gridhelp">
        แต่ละการ์ด = 1 กลุ่มเครื่องจ้างทอ ·{' '}
        {ro
          ? <><b>หน้านี้ดูอย่างเดียว</b> — แก้ได้ที่ ⚙️ Setting › ข้อมูลหลัก</>
          : <>แก้แล้วกด <b>บันทึก</b> (Ctrl+S) · Ctrl+Z = ย้อนกลับ</>}
      </div>
    </div>
  )
}

// ---------- Item S9 / S9 Only: ตาราง item ----------
// ใช้ตารางกลาง (DataTable) เหมือนชีทอื่น — ที่นี่แค่กำหนดว่าโชว์คอลัมน์ไหน ป้ายอะไร
function ItemS9List({ grid, info, rids, setCell, addRow, delRow, isChanged, isNewRow, ro }) {
  const { kind, itemCi, mgCi, gaugeCi } = info
  const isOnly = kind === 's9only'

  const cols = useMemo(() => [
    { ci: itemCi, label: 'ITEM_CODE', w: 220, cls: 'key', ph: 'ITEM_CODE', flex: true },
    mgCi >= 0 && { ci: mgCi, label: 'กลุ่มเครื่อง', w: 180, ph: 'เช่น SKP' },
    gaugeCi >= 0 && { ci: gaugeCi, label: 'Guage', w: 120, ph: 'เช่น 24' },
  ].filter(Boolean), [itemCi, mgCi, gaugeCi])

  const hint = isOnly
    ? <span className="dtwarn">⚠️ item กลุ่มนี้ระบบจะ<b>ส่งจ้างทอเสมอ</b> (plan = วันนี้+3)</span>
    : <span className="dthint">item ที่ <b>จ้างทอได้</b> — ระบบใช้เป็นทางเลือกเมื่อเครื่องปกติไม่พอ</span>

  return (
    <DataTable
      grid={grid} rids={rids} cols={cols} titleCi={itemCi} hint={hint}
      addLabel="+ เพิ่ม item" unit="รายการ"
      setCell={setCell} addRow={addRow} delRow={delRow}
      isChanged={isChanged} isNewRow={isNewRow} ro={ro}
    />
  )
}

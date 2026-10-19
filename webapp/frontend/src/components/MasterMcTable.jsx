import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import McNewWizard from './McNewWizard.jsx'

// มุมมองตารางของชีท Master MC — คงเป็นตารางแก้ได้ แต่ช่วยให้อ่านง่ายขึ้น:
//   - ตรึง (freeze) คอลัมน์คีย์ MC_CAT / MC / เกจ ไว้ซ้าย เวลาเลื่อนดูคอลัมน์ความจุทางขวา
//   - คั่นด้วยหัวกลุ่ม Factory + ยอดรวมเครื่องต่อกลุ่ม
//   - ค้นหา + แถวใหม่ขึ้นบนสุด
// แก้ลง grid เดิมของ Masters โดยตรง (บันทึก / Ctrl+Z / Ctrl+S ชุดเดียวกัน)
// ฟีเจอร์ขั้นสูง (กรองรายคอลัมน์ / เปลี่ยนชื่อ / เพิ่มคอลัมน์) ใช้ปุ่ม "🧾 ตาราง" แบบเดิม

const ROWNUM_W = 44
const toNum = v => Number(String(v ?? '').trim()) || 0

const NEWOPT = '\u0000new'

// ช่องในตารางที่ให้ "เลือกจากค่าที่มีจริง" แทนการพิมพ์อิสระ
// ค่าที่อยู่ในไฟล์แต่ไม่มีในรายการยังแสดงไว้เสมอ (ไม่ลบทิ้งเงียบ ๆ) และพิมพ์ใหม่ได้ถ้าตั้งใจ
function PickCell({ value, options, changed, onChange }) {
  const [free, setFree] = useState(false)
  const cur = String(value ?? '')
  const known = options.includes(cur)
  if (free) {
    return (
      <input autoFocus className={changed ? 'chg' : ''} value={cur}
        onBlur={() => setFree(false)} onChange={e => onChange(e.target.value)} />
    )
  }
  return (
    <select className={'mmcpick' + (changed ? ' chg' : '')} value={cur}
      onChange={e => { if (e.target.value === NEWOPT) setFree(true); else onChange(e.target.value) }}>
      <option value="">—</option>
      {!known && cur && <option value={cur}>{cur}</option>}
      {options.map(o => <option key={o} value={o}>{o}</option>)}
      <option value={NEWOPT}>＋ พิมพ์ใหม่...</option>
    </select>
  )
}

// ro   = โหมดดูอย่างเดียว — ช่องข้อมูลล็อก แต่ช่องค้นหายังพิมพ์ได้
// cmap = ทะเบียนคอลัมน์ (splitColumns) — โชว์เฉพาะคอลัมน์ที่ระบบวางแผนอ่านจริง
//        คอลัมน์ที่เหลือซ่อนไว้ กดปุ่มกางดูได้ (ไม่ได้ลบออกจากไฟล์)
export default function MasterMcTable({ grid, info, rids, setCell, delRow, addRow, isChanged, isNewRow, ro, cmap }) {
  const { factoryCi, keyCis, totalCi } = info
  const [showAll, setShowAll] = useState(false)
  // ลำดับคอลัมน์ที่จะวาด: ตามทะเบียน (ใช้จริงก่อน) → กาง = ต่อท้ายด้วยคอลัมน์ที่ไม่ได้ใช้
  const view = useMemo(() => {
    if (!cmap || !cmap.used.length) {
      return grid.columns.map((c, ci) => ({ ci, label: String(c), used: true }))
    }
    const used = cmap.used.map(u => ({ ...u, used: true }))
    return showAll ? [...used, ...cmap.rest.map(r => ({ ...r, used: false }))] : used
  }, [cmap, showAll, grid.columns])
  const cols = grid.columns
  const keySet = useMemo(() => new Set(keyCis), [keyCis])
  const [q, setQ] = useState('')
  const [wizard, setWizard] = useState(false)
  const [audit, setAudit] = useState(null)      // ไฟตรวจ: กลุ่มไหนยังไม่พร้อมเข้าแผน
  const [auditOpen, setAuditOpen] = useState(false)

  const [opts, setOpts] = useState(null)        // ค่าที่มีอยู่จริงของแต่ละคอลัมน์ (ทำ dropdown)
  const [fixing, setFixing] = useState(false)
  const [fixMsg, setFixMsg] = useState('')

  // ไฟตรวจความพร้อม — โหลดครั้งเดียวตอนเปิดชีท และโหลดใหม่หลังเพิ่ม/แก้เครื่อง
  const loadAudit = () => api.mcNewAudit().then(setAudit).catch(() => setAudit(null))
  useEffect(() => { loadAudit() }, [])
  useEffect(() => { api.mcNewOptions().then(setOpts).catch(() => setOpts(null)) }, [])

  // คอลัมน์ที่ "ห้ามพิมพ์เอง" — สะกดต่างนิดเดียวแล้วค่าที่ตั้งในแผงวันทำงานหลุดทันที
  // จึงให้เลือกจากค่าที่มีจริงเป็นหลัก (ยังพิมพ์ใหม่ได้ผ่านตัวเลือก "＋ พิมพ์ใหม่")
  const pickCols = useMemo(() => {
    if (!opts) return {}
    const find = n => grid.columns.findIndex(c => String(c).trim().toUpperCase() === n)
    const m = {}
    const put = (name, list) => { const i = find(name); if (i >= 0 && list?.length) m[i] = list }
    put('FACTORY', opts.factories)
    put('TYPE', opts.types)
    put('MC_CAT', opts.cats)
    put('MC', opts.mcs)
    put('GROUP', opts.groups)
    return m
  }, [opts, grid.columns])

  async function fixNames() {
    setFixing(true); setFixMsg('')
    try {
      const r = await api.mcFixWorkdayNames()
      setFixMsg(r.moved ? `ย้ายค่าวันทำงานให้ตรงชื่อใหม่แล้ว ${r.moved} กลุ่ม: ${r.detail.join(' · ')}`
        : 'ไม่มีอะไรต้องย้าย')
      loadAudit()
    } catch (e) { setFixMsg('ย้ายไม่สำเร็จ: ' + e.message) }
    finally { setFixing(false) }
  }

  // แถวในทะเบียนแบบอ่านง่าย — ส่งให้ตัวช่วยทำรายการตัวเลือก (ไม่ต้องยิง API ซ้ำ)
  const simpleRows = useMemo(() => {
    const ci = n => {
      const i = grid.columns.findIndex(c => String(c).trim().toUpperCase() === n)
      return i
    }
    const idx = {
      factory: ci('FACTORY'), type: ci('TYPE'), mc_cat: ci('MC_CAT'),
      mc: ci('MC'), guage: ci('GUAGE') >= 0 ? ci('GUAGE') : ci('GAUGE'), group: ci('GROUP'),
    }
    return grid.rows.map(r => Object.fromEntries(
      Object.entries(idx).map(([k, i]) => [k, i >= 0 ? String(r[i] ?? '').trim() : ''])))
  }, [grid])

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
  const colCount = view.length + 2

  const cls = ci => keySet.has(ci) ? 'mmcfrz' : ''
  const sty = ci => keySet.has(ci)
    ? { left: leftOf[ci], minWidth: widths[ci], width: widths[ci] }
    : { minWidth: widths[ci] }

  const rowEls = ris => ris.map(ri => (
    <tr key={rids?.[ri] ?? ri} className={isNewRow(ri) ? 'newrow' : ''}>
      <td className="mmcrownum">{ri + 1}</td>
      {view.map(v => (
        <td key={v.ci} className={cls(v.ci) + (v.main ? ' mmcmain' : '') + (v.used ? '' : ' mmcextra')}
          style={sty(v.ci)}>
          {/* โหมดดู: เป็นข้อความธรรมดา ไม่ใช่ช่องกรอกที่ล็อกไว้ (ตารางอ่านง่ายกว่า)
              โหมดแก้: คอลัมน์คีย์เป็นช่องเลือก — กันพิมพ์ชื่อกลุ่มเพี้ยนจนวันทำงานหลุด */}
          {ro
            ? <span className="rocell">{grid.rows[ri][v.ci] ?? ''}</span>
            : pickCols[v.ci]
              ? <PickCell value={grid.rows[ri][v.ci] ?? ''} options={pickCols[v.ci]}
                changed={isChanged(ri, v.ci)} onChange={val => setCell(ri, v.ci, val)} />
              : <input className={isChanged(ri, v.ci) ? 'chg' : ''} value={grid.rows[ri][v.ci] ?? ''}
                onChange={e => setCell(ri, v.ci, e.target.value)} />}
        </td>
      ))}
      <td className="mmcact">
        {!ro && <button className="mmcdel" title="ลบแถว" onClick={() => delRow(ri)}>✕</button>}
      </td>
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
        {cmap && cmap.rest.length > 0 && (
          <button className="mmccolbtn" onClick={() => setShowAll(v => !v)}
            title={cmap.note}>
            {showAll ? '▲ ซ่อนคอลัมน์ที่ไม่ได้ใช้' : `▼ คอลัมน์ที่ระบบไม่ได้ใช้ (${cmap.rest.length})`}
          </button>
        )}
        {!ro && <button className="mmcaddbtn wiz" onClick={() => setWizard(true)}>➕ เพิ่มเครื่องใหม่</button>}
        {!ro && <button className="mmcaddbtn" onClick={addRow} title="เพิ่มแถวเปล่าแล้วกรอกเอง">+ แถวเปล่า</button>}
      </div>

      {/* ไฟตรวจความพร้อม — เครื่องที่ยังขาดวันทำงาน/กำลังผลิต จะวางแผนไม่ได้ตามที่คิด */}
      {audit && (audit.no_workday.length || audit.no_cap.length || audit.cat_mismatch?.length) > 0 && (
        <div className={'mmcaudit' + (audit.no_cap.length ? ' bad' : '')}>
          <button className="mmcauditbtn" onClick={() => setAuditOpen(v => !v)}>
            {auditOpen ? '▲ ซ่อน' : '▼ ดู'}
          </button>
          <span>
            {audit.no_cap.length > 0 && <b>⚠️ {audit.no_cap.length} กลุ่มยังไม่มีกำลังผลิตในฐานข้อมูล </b>}
            {audit.no_workday.length > 0 && <b>⚠️ {audit.no_workday.length} กลุ่มยังไม่ได้ตั้งวันทำงาน </b>}
            {audit.cat_mismatch?.length > 0 && <>ℹ️ {audit.cat_mismatch.length} กลุ่มจัดหมวดไม่ตรงกับฐานข้อมูล</>}
          </span>
          {auditOpen && (
            <div className="mmcauditbody">
              {audit.no_cap.length > 0 && (
                <div>
                  <b>ยังไม่มีกำลังผลิต</b> (งานจะวางไม่ได้ · ต้องให้ฝ่ายข้อมูลเพิ่มใน ERP):
                  <ul>{audit.no_cap.map(x => <li key={x}>{x}</li>)}</ul>
                </div>
              )}
              {audit.renamed?.length > 0 && (
                <div>
                  <b>ชื่อกลุ่มไม่ตรงกับแผงวันทำงาน</b> — ค่าที่ตั้งไว้ไม่ถูกใช้ ระบบใช้ 6 วัน/สัปดาห์แทน:
                  <ul>{audit.renamed.map(x => <li key={x}>{x}</li>)}</ul>
                  {!ro && (
                    <button className="mmcfixbtn" onClick={fixNames} disabled={fixing}>
                      {fixing ? 'กำลังย้าย...' : '🔧 ย้ายค่าวันทำงานไปชื่อใหม่ให้เลย'}
                    </button>
                  )}
                  {fixMsg && <div className="mmcfixmsg">{fixMsg}</div>}
                </div>
              )}
              {audit.no_workday.filter(x => !(audit.renamed || []).some(r => r.startsWith(x))).length > 0 && (
                <div>
                  <b>ยังไม่ได้ตั้งวันทำงาน</b> (ระบบจะใช้ 6 วัน/สัปดาห์ — แก้ที่ปฏิทิน › วันทำงานตามกลุ่มเครื่อง):
                  <ul>
                    {audit.no_workday.filter(x => !(audit.renamed || []).some(r => r.startsWith(x)))
                      .map(x => <li key={x}>{x}</li>)}
                  </ul>
                </div>
              )}
              {audit.cat_mismatch?.length > 0 && (
                <div>
                  <b>จัดหมวดไม่ตรงกับฐานข้อมูล</b> (ไม่ใช่ปัญหาเสมอไป — ใบสั่งที่มาจากหมวดนั้นยังวางได้):
                  <ul>{audit.cat_mismatch.map(x => <li key={x}>{x}</li>)}</ul>
                </div>
              )}
              {audit.cap_err && <div className="bad">ตรวจกำลังผลิตไม่ได้: {audit.cap_err}</div>}
            </div>
          )}
        </div>
      )}

      {wizard && (
        <McNewWizard rows={simpleRows} onClose={() => setWizard(false)} onDone={loadAudit} />
      )}

      <div className="mmcwrap">
        <table className="mmctable">
          <thead>
            <tr>
              <th className="mmcrownum">#</th>
              {/* หัวคอลัมน์เป็นชื่อไทย ชื่อจริงในไฟล์อยู่ใน tooltip (ไว้อ้างอิงเวลาเทียบกับ Excel) */}
              {view.map(v => (
                <th key={v.ci} className={cls(v.ci) + (v.main ? ' mmcmain' : '') + (v.used ? '' : ' mmcextra')}
                  style={sty(v.ci)} title={String(v.col)}>
                  {v.label}
                  {!v.used && <span className="mmcoff">ไม่ได้ใช้</span>}
                </th>
              ))}
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
        {cmap && cmap.rest.length > 0 && <>{cmap.note} · </>}
        คอลัมน์ <b>กลุ่มเครื่อง / เครื่อง / Guage</b> ตรึงไว้ซ้าย · คั่นกลุ่มตามโรงงาน ·
        {ro
          ? <> <b>หน้านี้ดูอย่างเดียว</b> — แก้ได้ที่ ⚙️ Setting › ข้อมูลหลัก</>
          : <> แก้/ลบได้ทันที · ต้องการกรองรายคอลัมน์/เปลี่ยนชื่อ กด <b>🧾 ตาราง</b> · บันทึก (Ctrl+S)</>}
      </div>
    </div>
  )
}

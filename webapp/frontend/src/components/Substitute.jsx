import React, { useEffect, useRef, useState } from 'react'
import { api } from '../api.js'

// หน้า "Item ทดแทน" — รหัสที่อยู่ใน ITEM_LIST เดียวกันของชีท Master_Item V2
// = spec เครื่อง/ด้ายเหมือนกันทุกอย่าง จึงใช้แทนกันได้ (stock ของทั้งกลุ่มใช้ร่วมกันได้)

const fmtKg = n => Number(n || 0).toLocaleString('th-TH', { maximumFractionDigits: 1 })
const fmtInt = n => Number(n || 0).toLocaleString('th-TH')
function fmtTime(ts) {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleString('th-TH', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function Substitute() {
  const [info, setInfo] = useState(null)
  const [q, setQ] = useState('')
  const [onlyMulti, setOnlyMulti] = useState(true)
  const [res, setRes] = useState(null)
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  // import
  const [file, setFile] = useState(null)
  const [preview, setPreview] = useState(null)
  const [importing, setImporting] = useState(false)
  const fileRef = useRef(null)

  async function loadInfo() {
    try {
      const s = await api.substituteSummary()
      setInfo(s)
      return s
    } catch (e) {
      setErr('โหลดข้อมูล Master_Item ไม่ได้: ' + e.message)
      return null
    }
  }

  async function runSearch(keyword = q, multi = onlyMulti) {
    setLoading(true)
    setErr('')
    try {
      setRes(await api.substituteSearch(keyword, multi, 200))
    } catch (e) {
      setErr('ค้นหาไม่ได้: ' + e.message)
      setRes(null)
    } finally {
      setLoading(false)
    }
  }

  // ค้นหาเฉพาะเมื่อไฟล์พร้อมใช้ — ถ้าไฟล์ยังไม่มีชีท V2 จะโชว์คำแนะนำให้ import แทน error
  useEffect(() => { loadInfo().then(s => { if (s?.ok) runSearch('', true) }) }, [])

  function onSubmit(e) {
    e.preventDefault()
    runSearch()
  }

  function toggleMulti(v) {
    setOnlyMulti(v)
    runSearch(q, v)
  }

  // ---------- import ----------
  async function pickFile(f) {
    setFile(f || null)
    setPreview(null)
    setMsg('')
    setErr('')
    if (!f) return
    try {
      setPreview(await api.substitutePreview(f))
    } catch (e) {
      setErr('ไฟล์นี้ใช้ไม่ได้: ' + e.message)
      setFile(null)
      if (fileRef.current) fileRef.current.value = ''
    }
  }

  async function doImport() {
    if (!file || !preview) return
    const ok = window.confirm(
      `ยืนยันนำเข้า?\n\nไฟล์: ${file.name}\nจะเขียนทับชีท "Master_Item V2" ` +
      `ด้วยข้อมูล ${fmtInt(preview.rows)} แถว\n\nชีทอื่นในไฟล์คงเดิม และระบบจะสำรองไฟล์เก่าเป็น .bak ให้ก่อน`
    )
    if (!ok) return
    setImporting(true)
    setMsg('')
    setErr('')
    try {
      const r = await api.substituteImport(file)
      setMsg(`นำเข้าสำเร็จ — ${fmtInt(r.rows)} แถว / ${fmtInt(r.codes)} รหัส ` +
        `/ กลุ่มที่ใช้แทนกันได้ ${fmtInt(r.multi_groups)} กลุ่ม (สำรองไฟล์เดิมไว้ที่ ${r.backup})`)
      setFile(null)
      setPreview(null)
      if (fileRef.current) fileRef.current.value = ''
      const s = await loadInfo()
      if (s?.ok) await runSearch()
    } catch (e) {
      setErr('นำเข้าไม่สำเร็จ: ' + e.message)
    } finally {
      setImporting(false)
    }
  }

  const rows = res?.rows || []

  return (
    <div className="data-page">
      <aside className="data-sidebar">
        <div className="data-sidehead">
          <div>
            <h2>Item ทดแทน</h2>
            <p>รหัสที่อยู่ใน ITEM_LIST เดียวกัน = spec เครื่องและด้ายตรงกันทุกอย่าง จึงใช้แทนกันได้</p>
          </div>
          <div className="data-sideactions">
            <button onClick={() => { loadInfo(); runSearch() }}>รีเฟรช</button>
          </div>
        </div>

        {info?.ok && (
          <div className="sub-stats">
            <div className="sub-stat"><b>{fmtInt(info.multi_groups)}</b><small>กลุ่มที่มีตัวแทน</small></div>
            <div className="sub-stat"><b>{fmtInt(info.multi_codes)}</b><small>รหัสที่แทนกันได้</small></div>
            <div className="sub-stat"><b>{fmtInt(info.codes)}</b><small>รหัสทั้งหมด</small></div>
            <div className="sub-stat"><b>{fmtInt(info.max_group)}</b><small>กลุ่มใหญ่สุด (รหัส)</small></div>
          </div>
        )}

        {info && !info.ok && (
          <div className="msg">{info.error || 'อ่านชีท Master_Item V2 ไม่ได้'}</div>
        )}

        <div className="map-help">
          <b>ที่มาของข้อมูล</b>
          <ul>
            <li>ไฟล์ <b>{info?.name || 'Master_Item.xlsx'}</b> ชีท <b>Master_Item V2</b></li>
            <li>อัปเดตล่าสุด {fmtTime(info?.mtime)}</li>
            <li>stock อ่านจาก <b>view_stock</b> เฉพาะแถวที่ไม่ติด QA</li>
          </ul>
        </div>

        <div className="sub-import">
          <b>นำเข้าไฟล์ (import)</b>
          <input
            ref={fileRef}
            type="file"
            accept=".xlsx"
            onChange={e => pickFile(e.target.files?.[0])}
            disabled={importing}
          />
          {preview && (
            <div className="sub-preview">
              <div>ชีทที่อ่าน: <b>{preview.sheet}</b></div>
              <div>{fmtInt(preview.rows)} แถว • {fmtInt(preview.codes)} รหัส • มีตัวแทน {fmtInt(preview.multi_groups)} กลุ่ม</div>
            </div>
          )}
          <button className="primary" onClick={doImport} disabled={!preview || importing}>
            {importing ? 'กำลังนำเข้า...' : 'นำเข้า ทับชีท Master_Item V2'}
          </button>
          <small>
            ทับ<b>เฉพาะชีท Master_Item V2</b> ชีทอื่นในไฟล์คงเดิม และสำรองไฟล์เก่าเป็น <b>.bak</b> ให้ทุกครั้ง
          </small>
        </div>

        <div className="data-note">
          ไฟล์ที่นำเข้าต้องมีคอลัมน์ <b>ITEM_LIST</b> และ <b>SPEC_KEY</b> เป็นอย่างน้อย
          (รูปแบบเดียวกับชีท Master_Item V2)
        </div>
      </aside>

      <section className="editor">
        <form className="filterbar" onSubmit={onSubmit}>
          <input
            className="search"
            placeholder="🔍 ค้นหาด้วยรหัสเต็ม / ITEM Color / กลุ่มเครื่อง เช่น FD7PRTPK109/01"
            value={q}
            onChange={e => setQ(e.target.value)}
          />
          <button className="primary" type="submit">ค้นหา</button>
          <label className="sub-check">
            <input type="checkbox" checked={onlyMulti} onChange={e => toggleMulti(e.target.checked)} />
            เฉพาะที่มีตัวแทน
          </label>
          {res && (
            <span className="count">
              พบ {fmtInt(res.total)} กลุ่ม{res.truncated ? ` (แสดง ${fmtInt(res.shown)} กลุ่มแรก)` : ''}
            </span>
          )}
        </form>

        {info && !info.ok && (
          <div className="msg note">
            ⚠ {info.error || `ยังอ่านชีท ${info.sheet || 'Master_Item V2'} ไม่ได้`}
            <br />ใช้กล่อง <b>นำเข้าไฟล์ (import)</b> ด้านซ้าย อัปโหลดไฟล์ที่มีชีท <b>Master_Item V2</b> เพื่อเริ่มใช้งานหน้านี้
          </div>
        )}
        {err && <div className="msg">{err}</div>}
        {msg && <div className="msg note">{msg}</div>}
        {loading && <div className="hint">กำลังโหลด...</div>}

        {!loading && res && !rows.length && (
          <div className="hint">
            ไม่พบกลุ่มที่ตรงกับ "{q}"
            {onlyMulti && ' — ลองเอาเครื่องหมายถูก "เฉพาะที่มีตัวแทน" ออก เผื่อ item นี้ไม่มีตัวแทน'}
          </div>
        )}

        <div className="sub-list">
          {rows.map(g => (
            <div className="sub-card" key={g.spec_key}>
              <div className="sub-cardhead">
                <div className="sub-tags">
                  <span className="sub-tag mc">{g.mc_group || '-'}</span>
                  <span className="sub-tag">{g.knit_mc_cat || '-'}</span>
                  <span className="sub-tag">เกจ {g.mc_gauge || '-'}</span>
                  <span className="sub-tag">{g.suffix === 'B0' ? 'อบกลม (B0)' : g.suffix === 'A0' ? 'อบผ่า (A0)' : g.suffix}</span>
                </div>
                <div className="sub-cardmeta">
                  <b>{g.count}</b> รหัสใช้แทนกันได้ • stock รวม <b>{fmtKg(g.stock_total)}</b> kg
                </div>
              </div>

              <table className="sub-table">
                <thead>
                  <tr><th>รหัสเต็ม</th><th>ITEM Color</th><th className="num">stock (kg)</th></tr>
                </thead>
                <tbody>
                  {g.codes.map(c => (
                    <tr key={c.code} className={c.stock > 0 ? 'has-stock' : undefined}>
                      <td>{c.code}</td>
                      <td>{c.item}</td>
                      <td className="num">{c.stock > 0 ? fmtKg(c.stock) : '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="sub-spec">
                <span>ด้าย: <b>{g.yarn_item || '-'}</b></span>
                <span>SL: {g.yarn_sl || '-'}</span>
                <span>เข็ม: {g.mc_needle || '-'}</span>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}

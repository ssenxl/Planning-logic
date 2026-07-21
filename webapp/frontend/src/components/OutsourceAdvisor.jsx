import React, { useEffect, useState } from 'react'
import { api } from '../api.js'

function fmtQty(v) {
  const n = Number(v) || 0
  return n.toLocaleString('th-TH', { maximumFractionDigits: 0 })
}

export default function OutsourceAdvisor() {
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState(null)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  // การแบ่งที่บันทึกไว้แล้ว (ค้างในระบบจนกว่าจะลบ) + สัปดาห์ที่เลือกได้
  const [split, setSplit] = useState({})
  const [weeks, setWeeks] = useState([])
  // ค่าที่ user กำลังกรอก (ยังไม่บันทึก) ต่อ item → { qty, week }
  const [draft, setDraft] = useState({})
  const [busy, setBusy] = useState('')

  useEffect(() => { loadSplit() }, [])

  async function loadSplit() {
    try {
      const s = await api.outsourceSplitGet()
      setSplit(s.items || {})
      setWeeks(s.weeks || [])
    } catch { /* ยังไม่มีแผน → ปล่อยว่าง */ }
  }

  async function analyze() {
    setLoading(true); setErr(''); setMsg('')
    try {
      const res = await api.outsourceAdvise()
      setData(res)
      setSplit(res.split || {})
      if (res.weeks?.length) setWeeks(res.weeks)
      setDraft({})
    } catch (e) {
      setErr('วิเคราะห์ไม่ได้: ' + e.message)
    } finally {
      setLoading(false)
    }
  }

  // ค่าในช่องกรอก = draft (ถ้าแก้อยู่) → ค่าที่บันทึกไว้ → ว่าง
  const qtyOf = (item) => draft[item]?.qty ?? (split[item]?.outsource_qty ?? '')
  const weekOf = (item) => draft[item]?.week ?? (split[item]?.start_week ?? '')
  const setDraftVal = (item, key, val) =>
    setDraft(d => ({ ...d, [item]: { qty: qtyOf(item), week: weekOf(item), ...d[item], [key]: val } }))

  async function saveSplit(item) {
    const qty = Number(qtyOf(item)) || 0
    const week = weekOf(item)
    if (qty > 0 && !week) { setErr(`${item}: ต้องเลือกสัปดาห์ที่จ้างทอด้วย`); return }
    setBusy(item); setErr(''); setMsg('')
    try {
      const r = await api.outsourceSplitSave(item, qty, qty > 0 ? Number(week) : null)
      setSplit(r.items || {})
      setDraft(d => { const n = { ...d }; delete n[item]; return n })
      setMsg(qty > 0
        ? `บันทึกแล้ว: ${item} จ้างทอ ${fmtQty(qty)} กก. สัปดาห์ ${week} — กด "รันแผนใหม่" เพื่อให้ระบบวางแผนให้`
        : `ยกเลิกการจ้างทอของ ${item} แล้ว — กด "รันแผนใหม่" เพื่ออัปเดตแผน`)
    } catch (e) {
      setErr('บันทึกไม่ได้: ' + e.message)
    } finally { setBusy('') }
  }

  async function removeSplit(item) {
    if (!window.confirm(`ยกเลิกการจ้างทอของ ${item}? (แผนรอบถัดไปจะทอในบ้านทั้งหมด)`)) return
    setBusy(item); setErr(''); setMsg('')
    try {
      const r = await api.outsourceSplitDelete(item)
      setSplit(r.items || {})
      setDraft(d => { const n = { ...d }; delete n[item]; return n })
      setMsg(`ยกเลิกการจ้างทอของ ${item} แล้ว — กด "รันแผนใหม่" เพื่ออัปเดตแผน`)
    } catch (e) {
      setErr('ยกเลิกไม่ได้: ' + e.message)
    } finally { setBusy('') }
  }

  async function runPlan() {
    setErr(''); setMsg('')
    try {
      const r = await api.run('full')
      setMsg(r.message + ' — เมื่อรันเสร็จ Gantt จะโชว์ก้อนจ้างทอเป็น 🧵 (ไม่กินเครื่องในบ้าน)')
    } catch (e) { setErr('สั่งรันไม่ได้: ' + e.message) }
  }

  const cands = data?.candidates || []
  const splitKeys = Object.keys(split)
  // item ที่บันทึกจ้างทอไว้แต่ไม่อยู่ในตาราง candidate (เช่นแผนใหม่ไม่มีแรงกดดันแล้ว)
  const orphanSplits = splitKeys.filter(k => !cands.some(c => c.item_code === k))

  return (
    <div className="card">
      <div className="editbar">
        <h2>🧵 จ้างทอ (AI) — แนะนำ item ที่คุ้มค่าที่สุด</h2>
        <div className="actions">
          <button onClick={analyze} disabled={loading}>
            {loading ? 'กำลังวิเคราะห์…' : 'วิเคราะห์การจ้างทอ'}
          </button>
          <button className="primary" onClick={runPlan}>▶ รันแผนใหม่ตามการแบ่ง</button>
        </div>
      </div>

      <div className="hint">
        พิจารณา item ที่ <b>จ้างทอได้ (S9)</b> แล้วจัดอันดับจาก 3 ปัจจัย: ความเร่งด่วน (วางแผนช้ากว่ากำหนดส่ง),
        การปลดคอขวดเครื่อง (เครื่องในเกจนั้นไม่พอ), และปริมาณค้างผลิต — ตัวเลขคำนวณจากแผนล่าสุด, AI ช่วยจัดอันดับ+อธิบาย
      </div>
      <div className="hint">
        กรอก <b>จ้างทอ (กก.)</b> + <b>สัปดาห์ที่จ้างทอ</b> แล้วกดบันทึก → ส่วนที่จ้างทอจะ<b>ไม่ใช้เครื่องในบ้าน</b> (วิ่งบนเครื่องจ้างทอ S9)
        ส่วนที่เหลือระบบจะ<b>วางแผนในบ้านให้เองตามเครื่องที่ว่าง</b> เมื่อกดรันแผนใหม่ —
        การแบ่งจะค้างอยู่ทุกรอบรัน (รวมรันอัตโนมัติ) จนกว่าจะกดยกเลิก
      </div>

      {err && <div className="msg">{err}</div>}
      {msg && <div className="msg">{msg}</div>}

      {data && (
        <>
          {data.summary && (
            <div className="ai-summary">
              <b>สรุปจาก AI:</b> {data.summary}
            </div>
          )}
          {data.note && <div className="msg">{data.note}</div>}
          {!data.ai && cands.length > 0 && !data.note && (
            <div className="hint">* แสดงอันดับจากการคำนวณ (ยังไม่ได้ใช้ AI)</div>
          )}
          <div className="hint">
            ไฟล์แผน: {data.plan_name || '-'} | พบ item จ้างทอได้ที่มีแรงกดดัน {data.total_eligible ?? cands.length} รายการ
          </div>

          {cands.length === 0 ? (
            <div className="hint">ไม่พบ item ที่ควรจ้างทอในแผนปัจจุบัน</div>
          ) : (
            <div className="gridwrap">
            <table className="grid os-table">
              <thead>
                <tr>
                  <th>อันดับ</th>
                  <th>Item</th>
                  <th>ลูกค้า</th>
                  <th>CAT / เกจ</th>
                  <th>ค้าง (กก.)</th>
                  <th>สาย (สัปดาห์)</th>
                  <th>เครื่องขาด</th>
                  <th>จ้างทอ (กก.)</th>
                  <th>สัปดาห์จ้างทอ</th>
                  <th>ในบ้าน (กก.)</th>
                  <th></th>
                  <th>เหตุผล</th>
                </tr>
              </thead>
              <tbody>
                {cands.map((c, i) => {
                  const item = c.item_code
                  const os = Number(qtyOf(item)) || 0
                  const inhouse = Math.max(0, (Number(c.qty) || 0) - os)
                  const saved = !!split[item]
                  const changed = !!draft[item]
                  return (
                    <tr key={item + i} className={saved ? 'row-outsourced' : undefined}>
                      <td className="rank">{c.rank ?? i + 1}</td>
                      <td>
                        {item}
                        {c.s9_only && <span className="tag-s9" title="ต้องจ้างทอเสมอ">S9 Only</span>}
                      </td>
                      <td>{c.customer || '-'}</td>
                      <td>{c.cat || '-'} / {c.gauge || '-'}</td>
                      <td className="num">{fmtQty(c.qty)}</td>
                      <td className="num">{c.late_weeks > 0 ? <b className="late">{c.late_weeks}</b> : '-'}</td>
                      <td className="num">{c.machine_shortage > 0 ? <b className="late">{c.machine_shortage}</b> : '-'}</td>
                      <td>
                        <input className="os-qty" type="number" min="0" step="any"
                          value={qtyOf(item)}
                          placeholder={`0 – ${fmtQty(c.qty)}`}
                          title={`ใส่ได้สูงสุด ${fmtQty(c.qty)} กก. (ของค้างทั้งหมดของ item นี้)`}
                          onChange={e => setDraftVal(item, 'qty', e.target.value)} />
                      </td>
                      <td>
                        <select className="os-week"
                          value={weekOf(item)}
                          onChange={e => setDraftVal(item, 'week', e.target.value)}>
                          <option value="">— เลือก —</option>
                          {weeks.map(w => <option key={w} value={w}>W{w}</option>)}
                        </select>
                      </td>
                      <td className="num">{fmtQty(inhouse)}</td>
                      <td className="os-actions">
                        <button className="primary" disabled={busy === item || (!changed && !saved)}
                          title="บันทึกการแบ่งนี้"
                          onClick={() => saveSplit(item)}>💾 บันทึก</button>
                        {saved && (
                          <button className="del" disabled={busy === item}
                            title="ยกเลิกการจ้างทอของ item นี้"
                            onClick={() => removeSplit(item)}>✕</button>
                        )}
                      </td>
                      <td className="reason">{c.reason || '-'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            </div>
          )}
        </>
      )}

      {orphanSplits.length > 0 && (
        <>
          <div className="hint">
            <b>การจ้างทอที่บันทึกไว้</b> (ไม่อยู่ในตารางแนะนำด้านบน แต่ยังมีผลทุกรอบรัน):
          </div>
          <table className="grid">
            <thead>
              <tr><th>Item</th><th>จ้างทอ (กก.)</th><th>สัปดาห์</th><th>บันทึกเมื่อ</th><th></th></tr>
            </thead>
            <tbody>
              {orphanSplits.map(k => (
                <tr key={k} className="row-outsourced">
                  <td>{k}</td>
                  <td className="num">{fmtQty(split[k].outsource_qty)}</td>
                  <td className="num">W{split[k].start_week}</td>
                  <td>{split[k].at || '-'}</td>
                  <td className="os-actions">
                    <button className="del" disabled={busy === k}
                      title="ยกเลิกการจ้างทอของ item นี้"
                      onClick={() => removeSplit(k)}>✕</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {!data && !loading && (
        <div className="hint">กดปุ่ม "วิเคราะห์การจ้างทอ" เพื่อให้ AI แนะนำ item ที่คุ้มค่าที่สุด</div>
      )}
    </div>
  )
}

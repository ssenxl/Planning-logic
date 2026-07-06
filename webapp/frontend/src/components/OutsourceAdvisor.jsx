import React, { useState } from 'react'
import { api } from '../api.js'

function fmtQty(v) {
  const n = Number(v) || 0
  return n.toLocaleString('th-TH', { maximumFractionDigits: 0 })
}

export default function OutsourceAdvisor() {
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState(null)
  const [err, setErr] = useState('')

  async function analyze() {
    setLoading(true)
    setErr('')
    try {
      const res = await api.outsourceAdvise()
      setData(res)
    } catch (e) {
      setErr('วิเคราะห์ไม่ได้: ' + e.message)
    } finally {
      setLoading(false)
    }
  }

  const cands = data?.candidates || []

  return (
    <div className="card">
      <div className="editbar">
        <h2>🧵 จ้างทอ (AI) — แนะนำ item ที่คุ้มค่าที่สุด</h2>
        <button onClick={analyze} disabled={loading}>
          {loading ? 'กำลังวิเคราะห์…' : 'วิเคราะห์การจ้างทอ'}
        </button>
      </div>

      <div className="hint">
        พิจารณา item ที่ <b>จ้างทอได้ (S9)</b> แล้วจัดอันดับจาก 3 ปัจจัย: ความเร่งด่วน (วางแผนช้ากว่ากำหนดส่ง),
        การปลดคอขวดเครื่อง (เครื่องในเกจนั้นไม่พอ), และปริมาณค้างผลิต — ตัวเลขคำนวณจากแผนล่าสุด, AI ช่วยจัดอันดับ+อธิบาย
      </div>

      {err && <div className="msg">{err}</div>}

      {data && (
        <>
          {data.summary && (
            <div className="ai-summary">
              <b>สรุปจาก AI:</b> {data.summary}
            </div>
          )}
          {data.note && <div className="msg">{data.note}</div>}
          {!data.ai && data.candidates?.length > 0 && !data.note && (
            <div className="hint">* แสดงอันดับจากการคำนวณ (ยังไม่ได้ใช้ AI)</div>
          )}
          <div className="hint">
            ไฟล์แผน: {data.plan_name || '-'} | พบ item จ้างทอได้ที่มีแรงกดดัน {data.total_eligible ?? cands.length} รายการ
          </div>

          {cands.length === 0 ? (
            <div className="hint">ไม่พบ item ที่ควรจ้างทอในแผนปัจจุบัน</div>
          ) : (
            <table className="grid">
              <thead>
                <tr>
                  <th>อันดับ</th>
                  <th>Item</th>
                  <th>ลูกค้า</th>
                  <th>CAT / เกจ</th>
                  <th>ค้าง (กก.)</th>
                  <th>สาย (สัปดาห์)</th>
                  <th>เครื่องขาด</th>
                  <th>เหตุผล</th>
                </tr>
              </thead>
              <tbody>
                {cands.map((c, i) => (
                  <tr key={c.item_code + i}>
                    <td className="rank">{c.rank ?? i + 1}</td>
                    <td>
                      {c.item_code}
                      {c.s9_only && <span className="tag-s9" title="ต้องจ้างทอเสมอ">S9 Only</span>}
                    </td>
                    <td>{c.customer || '-'}</td>
                    <td>{c.cat || '-'} / {c.gauge || '-'}</td>
                    <td className="num">{fmtQty(c.qty)}</td>
                    <td className="num">{c.late_weeks > 0 ? <b className="late">{c.late_weeks}</b> : '-'}</td>
                    <td className="num">{c.machine_shortage > 0 ? <b className="late">{c.machine_shortage}</b> : '-'}</td>
                    <td className="reason">{c.reason || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}

      {!data && !loading && (
        <div className="hint">กดปุ่ม "วิเคราะห์การจ้างทอ" เพื่อให้ AI แนะนำ item ที่คุ้มค่าที่สุด</div>
      )}
    </div>
  )
}

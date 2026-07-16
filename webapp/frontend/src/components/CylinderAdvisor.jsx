import React, { useState } from 'react'
import { api } from '../api.js'

function fmtQty(v) {
  const n = Number(v) || 0
  return n.toLocaleString('th-TH', { maximumFractionDigits: 0 })
}

// เกจต้นทางที่ถอดเครื่องมาเปลี่ยนได้ → "26 (ว่าง 4), 22 (ว่าง 3)"
function fmtSources(sources) {
  if (!sources || sources.length === 0) return '-'
  return sources.map(s => `เกจ ${s.gauge} (ว่าง ${s.free})`).join(', ')
}

/**
 * เปลี่ยนกระบอก (AI) — เสนอว่าควรเปลี่ยนกระบอกให้ item ไหนก่อน
 * Planning ไม่เปลี่ยนกระบอกเองแล้ว (คนตัดสินใจ) โมดูลนี้แค่ "แนะนำ"
 * ตัวเลขคำนวณจากแผนล่าสุด + เครื่องว่างจริง + กระบอกสำรองใน MasterMC
 */
export default function CylinderAdvisor() {
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState(null)
  const [err, setErr] = useState('')

  async function analyze() {
    setLoading(true)
    setErr('')
    try {
      const res = await api.cylinderAdvise()
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
        <h2>🔩 เปลี่ยนกระบอก (AI) — แนะนำว่าควรเปลี่ยนให้ item ไหน</h2>
        <button onClick={analyze} disabled={loading}>
          {loading ? 'กำลังวิเคราะห์…' : 'วิเคราะห์การเปลี่ยนกระบอก'}
        </button>
      </div>

      <div className="hint">
        แผนจะไม่เปลี่ยนกระบอกให้เอง — ที่นี่คือ<b>ข้อเสนอ</b>ว่างานที่ส่งไม่ทัน/ผลิตไม่ครบเพราะเครื่องในเกจนั้นหมด
        จะปลดล็อกได้ถ้าถอดเครื่องว่างจากเกจอื่น (ใน MC_CAT และโรงงานเดียวกัน) มาเปลี่ยนกระบอก
        ต้องมีทั้งเครื่องว่างในเกจต้นทาง และกระบอกสำรองของเกจปลายทาง — คุณเป็นคนตัดสินใจเอง
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
          {!data.ai && cands.length > 0 && !data.note && (
            <div className="hint">* แสดงอันดับจากการคำนวณ (ยังไม่ได้ใช้ AI)</div>
          )}
          <div className="hint">
            ไฟล์แผน: {data.plan_name || '-'} | พบงานที่ติดเครื่อง {data.total_found ?? cands.length} รายการ
          </div>

          {cands.length === 0 ? (
            <div className="hint">ไม่มีงานที่ต้องเปลี่ยนกระบอก — แผนปัจจุบันไม่มีงานสายหรือค้างผลิตจากเครื่องไม่พอ</div>
          ) : (
            <table className="grid">
              <thead>
                <tr>
                  <th>อันดับ</th>
                  <th>Item</th>
                  <th>CAT / เกจ</th>
                  <th>ต้องทำอะไร</th>
                  <th>กระบอกสำรอง</th>
                  <th>สาย (สัปดาห์)</th>
                  <th>ค้าง (กก.)</th>
                  <th>ทำไมลำดับนี้</th>
                </tr>
              </thead>
              <tbody>
                {cands.map((c, i) => (
                  <tr key={c.item_code + i} className={c.feasible ? '' : 'cyl-blocked'}>
                    <td className="rank">{c.rank ?? i + 1}</td>
                    <td>
                      {c.item_code}
                      {c.feasible
                        ? <span className="tag-ok" title="เปลี่ยนกระบอกได้เลย">ทำได้</span>
                        : <span className="tag-block" title={c.blocker}>ติด</span>}
                    </td>
                    <td>{c.cat || '-'} / {c.gauge || '-'}</td>
                    <td className="cyl-action" title={`เกจต้นทางที่ว่าง: ${fmtSources(c.sources)}`}>
                      {c.action}
                    </td>
                    <td className="num">{c.spare_available > 0 ? c.spare_available : <b className="late">0</b>}</td>
                    <td className="num">{c.late_weeks > 0 ? <b className="late">{c.late_weeks}</b> : '-'}</td>
                    <td className="num">{c.pending_qty > 0 ? fmtQty(c.pending_qty) : '-'}</td>
                    <td className="reason">{c.reason || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}

      {!data && !loading && (
        <div className="hint">กดปุ่ม "วิเคราะห์การเปลี่ยนกระบอก" เพื่อให้ AI แนะนำว่าควรเปลี่ยนกระบอกให้ item ไหนก่อน</div>
      )}
    </div>
  )
}

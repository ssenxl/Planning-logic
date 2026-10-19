// LiveBanner.jsx — ป้ายลอยมุมขวาล่าง: "สมชายแก้ล็อกแผนเมื่อ 10:05"
// (ลอยทับ ไม่แทรกในเนื้อหน้า — เดิมเป็นแถบใต้เมนูที่ดันทั้งหน้าขยับตอนโผล่/หาย ดู styles.css)
//
// ทำไมต้องมีทั้งที่แต่ละหน้าโหลดข้อมูลใหม่ให้เองอยู่แล้ว: ตารางที่รีเฟรชเงียบ ๆ ทำให้คนที่
// จ้องอยู่สงสัยว่า "เมื่อกี้เลขไม่ใช่แบบนี้นี่" — แถบนี้ตอบว่าใครเปลี่ยนและเปลี่ยนเมื่อไหร่
// จอที่ไม่ได้เปิดหน้านั้นอยู่ก็ยังได้รู้ความเคลื่อนไหวของทีม
import React, { useEffect, useState } from 'react'
import * as live from '../live.js'

// ชื่อที่คนอ่านรู้เรื่อง — ชุดข้อมูลชื่อแบบ masters/MasterMC/Lock_MC ไม่มีความหมายกับผู้ใช้
function labelOf(resource) {
  if (!resource) return 'ข้อมูล'
  if (resource === 'plan_lock') return 'ล็อกแผน'
  if (resource === 'workday') return 'วันทำงาน'
  if (resource === 'plan_config') return 'Setting'
  if (resource === 'cylinder') return 'การเปลี่ยนสูบ'
  if (resource === 'outsource') return 'การจ้างทอ'
  if (resource === 'yarn_lock') return 'ล็อกด้าย'
  if (resource === 'yarn_item_lock') return 'ล็อกด้ายให้ Item'
  if (resource === 'run') return 'แผนผลิต (รันรอบใหม่)'
  const [kind, a, b] = resource.split('/')
  if (kind === 'masters') return `Master ${a} · ${b || ''}`.trim()
  if (kind === 'plan') return `ชีทแผน ${a || ''}`.trim()
  return resource
}

const SHOW_MS = 12000    // โชว์สักพักแล้วหายเอง — ไม่ใช่เรื่องที่ต้องให้กดปิดทุกครั้ง

export default function LiveBanner() {
  const [msg, setMsg] = useState(null)

  useEffect(() => {
    let timer = null
    const off = live.subscribeAll((ent, resource) => {
      if (!ent) return
      // ข่าวที่เกิดจากการกระทำของเราเอง — ไม่ต้องบอกตัวเองว่าตัวเองเพิ่งทำอะไร
      // (หน้าแผนเซฟอัตโนมัติทุก 3 วิ ถ้าไม่กรอง แถบนี้จะเด้งรัว ๆ ตลอดเวลาที่กำลังลากงาน)
      if (live.isMe(ent.by)) return
      setMsg({
        text: `${ent.by || 'มีคน'} แก้${labelOf(resource)}แล้ว`,
        detail: ent.detail || '',
        at: ent.at || '',
        // คีย์ผูกกับ "ชุดข้อมูล" ไม่ใช่เลขรุ่น — ข่าวใหม่ของชุดเดิม (คนเดียวกันลากต่อ) จะเป็น
        // การอัปเดตข้อความในแถบเดิม ไม่ใช่การสร้างแถบใหม่ที่เล่นอนิเมชันสไลด์เข้าซ้ำทุกครั้ง
        key: resource,
      })
      clearTimeout(timer)
      timer = setTimeout(() => setMsg(null), SHOW_MS)
    })
    return () => { off(); clearTimeout(timer) }
  }, [])

  if (!msg) return null
  const time = msg.at ? msg.at.slice(11, 16) : ''
  return (
    <div className="live-banner" key={msg.key}>
      <span className="live-dot" />
      <b>{msg.text}</b>
      {msg.detail ? <span className="live-detail">{msg.detail}</span> : null}
      {time ? <span className="live-time">{time} น.</span> : null}
      <button type="button" className="live-close" onClick={() => setMsg(null)} title="ปิด">×</button>
    </div>
  )
}

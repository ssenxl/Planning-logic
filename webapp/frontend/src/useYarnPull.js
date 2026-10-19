import { useEffect, useRef, useState } from 'react'
import { api } from './api.js'

/* ปุ่ม "ดึงข้อมูลด้าย" — ใช้ 2 หน้า ต่างโหมดกัน (เดิมใช้ร่วมกับหน้า Yarn Ava — ถอดหน้านั้นแล้ว 15 ต.ค. 2569)
   · หน้า Yarn Usage  — โหมด 'yarn-usage' = ไฟล์ด้ายต่อใบ KP (View_Yarn_JoKnit.py ~6 วินาที)
   · หน้า Stock Yarn — โหมด 'yarn' = ด้ายต่อใบ KP + สต๊อกด้าย (View_Yarn_Aging.py ~17 นาที) ตัวเดียวกับรอบ 06:00
   ไม่ต้องรอรอบดึงอัตโนมัติ ; ดึงจบแล้วเรียก onDone
   งานด้ายรันใน "คิวด้าย" ของ runner แยกจากคิวหลัก (user เลือก 15 ต.ค. 2569) — ไม่บล็อกการรันแผน
   สถานะอยู่ที่ runStatus().yarn ; คิวด้ายรันได้ทีละงาน — มีงานด้ายรันอยู่ = สั่งไม่ได้ (ขึ้นข้อความของ runner ตรง ๆ)
   เปิดหน้ามาตอนที่มีคนสั่งโหมดเดียวกันไว้แล้ว → เกาะงานนั้นรอจนจบ (ไม่ต้องกดซ้ำ)
   ถามสถานะเองทุก 1.5 วิ ไม่พึ่งข่าว live อย่างเดียว เพราะงานที่ล้มไม่มีข่าวส่งมา */

// mode ต้องตรงกับ runner.YARN_MODES
const PULL_POLL_MS = 1500
// บรรทัดใน log ที่บอกสาเหตุตอนดึงไม่สำเร็จ (สคริปต์ดึงพิมพ์ [ERROR] … ; runner พิมพ์ ❌ …)
const ERR_LINE = /\[ERROR\]|Error|❌|Traceback/

export function useYarnPull(onDone, mode = 'yarn-usage') {
  const [pulling, setPulling] = useState(false)
  const [since, setSince] = useState(null)         // started_at ของงานที่รออยู่ (เวลาเซิร์ฟเวอร์ YYYY-MM-DDTHH:MM:SS)
  const [pullMsg, setPullMsg] = useState(null)     // {text, cls} ผลของการกดครั้งล่าสุด
  const alive = useRef(true)
  const busy = useRef(false)                        // กันวนรอซ้อนกัน 2 ชุด (กดปุ่ม + เกาะงานตอนเปิดหน้า)
  const done = useRef(onDone)
  done.current = onDone                             // เรียกตัวล่าสุดเสมอ (หน้าสร้าง load ใหม่ทุก render)

  // เปิดหน้ามาตอนงานโหมดนี้รันอยู่ (อีกจอกดไว้ / รอบตั้งเวลา) → เกาะรอจนจบ
  useEffect(() => {
    alive.current = true
    api.runStatus().then(s => {
      const y = s?.yarn
      if (alive.current && y?.running && y.mode === mode) watch()
    }).catch(() => {})
    return () => { alive.current = false }
  }, [])

  async function watch() {
    if (busy.current) return
    busy.current = true
    setPulling(true); setPullMsg(null)
    try {
      for (;;) {
        // รอก่อนถามทุกรอบ — กดปุ่มแล้วถามทันที อาจได้สถานะของรอบก่อนที่จบไปแล้ว (ตีความผิดว่าดึงเสร็จ)
        await new Promise(res => setTimeout(res, PULL_POLL_MS))
        if (!alive.current) return
        const y = (await api.runStatus())?.yarn || {}
        if (!alive.current) return
        if (y.running && y.mode === mode) { setSince(y.started_at || null); continue }
        // งานของเราจบแล้ว (หรือมีงานด้ายโหมดอื่นเข้าคิวต่อทันที — แยกผลไม่ได้ ก็โหลดหน้าใหม่ไว้ก่อน)
        if (y.mode === mode && y.returncode !== 0) {
          const why = [...(y.tail || [])].reverse().find(l => ERR_LINE.test(l))
          setPullMsg({ text: 'ดึงข้อมูลด้ายไม่สำเร็จ' + (why ? ' — ' + why.trim().slice(0, 200) : ''), cls: 'bad' })
        } else {
          setPullMsg({ text: 'ดึงข้อมูลด้ายแล้ว', cls: 'info' })
          await done.current()
        }
        return
      }
    } catch (e) {
      if (alive.current) setPullMsg({ text: 'ติดตามสถานะการดึงข้อมูลด้ายไม่ได้: ' + e.message, cls: 'bad' })
    } finally {
      busy.current = false
      if (alive.current) { setPulling(false); setSince(null) }
    }
  }

  async function pull() {
    if (busy.current) return
    setPullMsg(null)
    try {
      const r = await api.run(mode)
      if (!r.ok) { setPullMsg({ text: 'ยังดึงไม่ได้: ' + r.message, cls: 'warn' }); return }
    } catch (e) {
      setPullMsg({ text: 'สั่งดึงข้อมูลด้ายไม่ได้: ' + e.message, cls: 'bad' })
      return
    }
    await watch()
  }

  return { pulling, since, pullMsg, pull }
}

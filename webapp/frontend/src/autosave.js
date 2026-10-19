// autosave.js — เซฟให้เองทุกครั้งที่ผู้ใช้ขยับ + ปุ่มเลิกทำ + ป้ายสถานะ
//
// ไม่มีปุ่ม "บันทึก" อีกแล้ว (ยกเว้นหน้าตั้งค่าระบบ) จึงต้องมีของสามอย่างคู่กันเสมอ:
//   1. เซฟเอง แต่ต้องไม่เซฟทุกตัวอักษร  → useAutoSave
//   2. ถอยกลับได้เมื่อทำพลาด             → useUndo
//   3. บอกให้เห็นว่าบันทึกหรือยัง         → components/SaveStatus.jsx
//
// จังหวะการเซฟแบ่งตามชนิดการกระทำ (ตกลงกับผู้ใช้ไว้):
//   • ขยับ = ตัดสินใจแล้ว (ลาก Gantt, ติ๊ก, เลือก dropdown) → save() ทันที
//   • พิมพ์ในช่อง                                        → saveSoon() แล้วปล่อยให้หน่วง
//     ถ้าเซฟทุกตัวอักษร คนที่จะพิมพ์ 150 จะถูกเซฟเป็น 1 แล้ว 15 แล้ว 150 และจออื่นเห็น
//     เลขกระพริบสามรอบ ซึ่งคือความเข้าใจผิดที่ระบบนี้ตั้งใจจะกำจัด
import { useCallback, useEffect, useRef, useState } from 'react'

export const TYPING_DELAY = 1500     // หยุดพิมพ์นานเท่านี้ = พิมพ์จบ

/**
 * ตัวจัดการการเซฟอัตโนมัติของหนึ่งหน้า
 *   doSave(payload) → Promise ; ต้องโยน error ถ้าเซฟไม่สำเร็จ
 * คืน { save, saveSoon, flush, status, error, dirty }
 */
// เบรกฉุกเฉิน: เซฟสำเร็จรัว ๆ เกินเท่านี้ในช่วงเวลาที่กำหนด = มีอะไรผิดปกติ
// (เช่นหน้าเว็บคิดว่า "ยังมีของที่ยังไม่เซฟ" อยู่ตลอดแม้เพิ่งเซฟไป → เซฟวนไม่รู้จบ)
// ปล่อยไว้จะเขียนไฟล์ Excel รัว ๆ ทั้งวัน — หยุดแล้วบอกผู้ใช้ดีกว่า
const LOOP_LIMIT = 12
const LOOP_WINDOW_MS = 10000

export function useAutoSave(doSave, { delay = TYPING_DELAY } = {}) {
  const [status, setStatus] = useState('idle')   // idle | pending | saving | saved | error
  const [error, setError] = useState(null)
  const timer = useRef(null)
  const pending = useRef(null)      // payload ที่รอเซฟ
  const running = useRef(false)     // กำลังเซฟอยู่ไหม (กันยิงซ้อน)
  const again = useRef(false)       // มีของใหม่เข้ามาระหว่างที่กำลังเซฟ
  const recent = useRef([])         // เวลาของการเซฟล่าสุด ๆ (ใช้จับการเซฟวน)
  const stopped = useRef(false)     // เบรกฉุกเฉินทำงานแล้ว
  const failed = useRef(false)      // รอบล่าสุดเซฟไม่สำเร็จ (ของยังค้างใน pending)
  const saveRef = useRef(doSave)
  saveRef.current = doSave

  const run = useCallback(async () => {
    if (stopped.current) return
    if (running.current) { again.current = true; return }
    const payload = pending.current
    if (payload === null) return
    pending.current = null
    running.current = true
    setStatus('saving')
    try {
      await saveRef.current(payload)
      failed.current = false
      setError(null)
      setStatus(again.current ? 'pending' : 'saved')

      const now = Date.now()
      recent.current = [...recent.current, now].filter(t => now - t < LOOP_WINDOW_MS)
      if (recent.current.length > LOOP_LIMIT) {
        stopped.current = true
        again.current = false
        pending.current = null
        setError(new Error('หยุดบันทึกอัตโนมัติไว้ก่อน เพราะตรวจพบการบันทึกซ้ำผิดปกติ '
          + '— กดโหลดใหม่แล้วลองอีกครั้ง'))
        setStatus('error')
        return
      }
    } catch (e) {
      // เก็บของที่เซฟไม่สำเร็จไว้ในคิวต่อ — ถ้าทิ้งไป ปุ่ม "ลองใหม่" จะไม่มีอะไรให้ลอง
      // (flush เช็ก pending) และตัวกันปิดแท็บก็จะไม่เตือน ทั้งที่ของยังไม่เข้าไฟล์เลย
      // มีของใหม่เข้าคิวแล้วไม่ต้องคืน — ของใหม่กว่าครอบของเก่าอยู่แล้ว
      if (pending.current === null) pending.current = payload
      failed.current = true
      setError(e)
      setStatus('error')
    } finally {
      running.current = false
      // มีการแก้เข้ามาระหว่างเซฟ → เซฟรอบต่อไปให้ด้วย ไม่งั้นการขยับครั้งสุดท้ายหาย
      if (again.current) { again.current = false; if (pending.current !== null) run() }
    }
  }, [])

  /** เซฟทันที — ใช้กับการกระทำที่จบในตัว (ลาก ติ๊ก เลือก) */
  const save = useCallback((payload) => {
    if (stopped.current) return          // เบรกฉุกเฉินทำงานแล้ว — อย่าแตะไฟล์ต่อ
    clearTimeout(timer.current)
    pending.current = payload
    failed.current = false               // ของใหม่ = เริ่มนับใหม่ ยังไม่ถือว่าล้ม
    run()
  }, [run])

  /** เซฟเมื่อหยุดพิมพ์ — ใช้กับช่องกรอก */
  const saveSoon = useCallback((payload) => {
    // หลังเบรกฉุกเฉินต้องไม่กลับไปขึ้น "กำลังจะบันทึก" อีก ไม่งั้นผู้ใช้เห็นเหมือนระบบ
    // ยังเซฟให้อยู่ ทั้งที่หยุดไปแล้ว — ของที่แก้ต่อจากนั้นจะไม่ได้ลงไฟล์เลย
    if (stopped.current) return
    pending.current = payload
    failed.current = false
    setStatus('pending')
    clearTimeout(timer.current)
    timer.current = setTimeout(run, delay)
  }, [run, delay])

  /** เซฟของที่ค้างเดี๋ยวนี้ แล้วรอจนเสร็จ — ต้อง await ก่อนเปลี่ยนชีท/เปลี่ยนไฟล์ที่กำลังแก้
   *  ไม่งั้นการเซฟที่ค้างอยู่จะไปลงชีทใหม่ที่เพิ่งเปิด (ข้อมูลของชีทเก่าทับชีทใหม่) */
  const flush = useCallback(async () => {
    if (stopped.current) return
    clearTimeout(timer.current)
    failed.current = false               // กด "ลองใหม่" = ขอให้ลองอีกครั้งจริง ๆ
    if (pending.current !== null) await run()
    // เซฟรอบที่ถูกต่อคิวไว้ระหว่างนั้นก็ต้องจบด้วย
    //
    // ต้องหยุดรอเมื่อรอบล่าสุดล้ม: ตั้งแต่เก็บ payload ที่ล้มไว้ในคิว pending จะไม่กลับเป็น
    // null อีกจนกว่าจะเซฟสำเร็จ — ถ้าวนรอต่อ การเปลี่ยนชีท/ปิดแท็บจะค้างไม่รู้จบ
    while (running.current || (pending.current !== null && !failed.current)) {
      await new Promise(r => setTimeout(r, 30))
    }
  }, [run])

  // ปิดแท็บ/กดถอยหลังทั้งที่ยังมีของค้าง → เซฟให้ก่อน แล้วเตือนถ้ายังไม่ทัน
  useEffect(() => {
    const onLeave = (e) => {
      if (pending.current === null && !running.current) return
      flush()
      e.preventDefault()
      e.returnValue = ''
    }
    window.addEventListener('beforeunload', onLeave)
    return () => { window.removeEventListener('beforeunload', onLeave); clearTimeout(timer.current) }
  }, [flush])

  return { save, saveSoon, flush, status, error, dirty: status === 'pending' }
}

/**
 * ประวัติการแก้ในรอบที่เปิดหน้าอยู่ — ย้อนได้เรื่อย ๆ (ผู้ใช้เลือกไว้)
 *   push(snapshot) ก่อนแก้แต่ละครั้ง · undo() คืนสถานะก่อนหน้า (null = ไม่มีให้ย้อนแล้ว)
 * เก็บเป็น snapshot ทั้งก้อน เพราะการแก้หนึ่งครั้งกระทบหลายแถว (ลากงาน = ย้ายทั้งบล็อก)
 */
export function useUndo(limit = 200) {
  const stack = useRef([])
  const [depth, setDepth] = useState(0)

  const push = useCallback((snapshot, label = '') => {
    stack.current.push({ snapshot, label, at: Date.now() })
    if (stack.current.length > limit) stack.current.shift()
    setDepth(stack.current.length)
  }, [limit])

  const undo = useCallback(() => {
    const last = stack.current.pop()
    setDepth(stack.current.length)
    return last || null
  }, [])

  const clear = useCallback(() => { stack.current = []; setDepth(0) }, [])

  return { push, undo, clear, canUndo: depth > 0, depth }
}

/** ผูก Ctrl+Z เข้ากับ undo ของหน้านั้น (ไม่แย่งช่องกรอกที่ผู้ใช้กำลังพิมพ์อยู่) */
export function useUndoHotkey(onUndo, enabled = true) {
  useEffect(() => {
    if (!enabled) return
    const h = (e) => {
      if (!(e.ctrlKey || e.metaKey) || e.key.toLowerCase() !== 'z' || e.shiftKey) return
      const el = document.activeElement
      const tag = el && el.tagName
      // อยู่ในช่องพิมพ์ → ปล่อยให้ Ctrl+Z ของช่องนั้นทำงานตามปกติ
      if (tag === 'INPUT' || tag === 'TEXTAREA' || (el && el.isContentEditable)) return
      e.preventDefault()
      onUndo()
    }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [onUndo, enabled])
}

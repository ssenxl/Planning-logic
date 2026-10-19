import { useEffect, useState } from 'react'
import { api } from './api.js'
import * as live from './live.js'

/* ด้ายที่ฝั่งด้ายล็อกให้ SC + ผ้า (ล็อกที่หน้าแผนด้าย — yarn_lock.py) สำหรับโชว์ในการ์ดงานของ Gantt
   อ่านสดจาก /api/yarn-locks — ล็อก/ปลดแล้วเห็นทันที ไม่ต้องรอรันแผนใหม่ (ล็อกไม่เปลี่ยนวันของงานอยู่แล้ว)
   ไม่เขียนลงชีทแผน: ชีทแผนถูกเซฟกลับทั้งตาราง คอลัมน์ที่เติมตอนแสดงจะติดลง Excel ไปด้วย

   คืน Map "SC เลขล้วน|ผ้า" → [ล็อกที่ยังกันด้ายอยู่] */
export const yarnLockKey = (so, item) =>
  `${String(so ?? '').trim().toUpperCase().replace(/^S/, '').replace(/\.0$/, '')}|${String(item ?? '').trim().toUpperCase()}`

export function useYarnLocks() {
  const [map, setMap] = useState(() => new Map())
  useEffect(() => {
    let alive = true
    const load = () => api.yarnLocks()
      .then(d => {
        if (!alive) return
        const m = new Map()
        for (const l of d?.locks || []) {
          if (l.state !== 'active') continue
          const k = yarnLockKey(l.so, l.fabric)
          if (!m.has(k)) m.set(k, [])
          m.get(k).push(l)
        }
        setMap(m)
      })
      .catch(() => { /* โหลดไม่ได้ = ไม่โชว์ป้าย — ไม่ให้การ์ดงานพังเพราะเรื่องด้าย */ })
    load()
    const off = live.subscribe(live.RES.yarnLock, load)
    return () => { alive = false; off() }
  }, [])
  return map
}

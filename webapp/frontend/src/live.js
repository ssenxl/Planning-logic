// live.js — ทำให้ทุกจอที่เปิดหน้าเดียวกันเห็นข้อมูลชุดเดียวกัน
//
// backend ยิงข่าวมาทาง /api/events (SSE) ทุกครั้งที่มีคนเซฟ — จอที่เปิดค้างอยู่จึงอัปเดต
// เองภายใน ~1 วินาที ไม่ต้องกด F5 และไม่ต้องบอกกันในไลน์ว่า "แก้แล้วนะ"
//
// สายข่าวมีเส้นเดียวต่อแท็บ ใช้ร่วมกันทุกหน้า (เบราว์เซอร์จำกัดจำนวน connection ต่อโดเมน
// ถ้าให้แต่ละหน้าเปิดเส้นของตัวเอง เปิดไว้หลายหน้าแล้วหน้าอื่นจะโหลดไม่ออก)
//
// ต่อไม่ติด (เช่น proxy ตัด SSE ทิ้ง) → ถอยไปถามเลขรุ่นทุก 5 วินาทีเอง ผู้ใช้ไม่ต้องรู้เรื่อง
import { auth, withBase } from './api.js'

const POLL_MS = 5000          // จังหวะถามเลขรุ่นตอนใช้ทางสำรอง
const RETRY_MS = 3000         // สายหลุด → ต่อใหม่

let es = null                 // EventSource ปัจจุบัน
let pollTimer = null
let retryTimer = null
let started = false
let revs = {}                 // { resource: {rev, by, at, detail} } ที่รู้ล่าสุด
const subs = new Map()        // resource → Set<fn(ent)>
const anySubs = new Set()     // ผู้ฟังทุก resource (แถบแจ้งเตือนรวม)

function emit(resource, ent) {
  const prev = revs[resource]
  // ข่าวซ้ำ (เช่นได้ทั้งจาก SSE และ poll) → ไม่ต้องปลุกให้โหลดซ้ำ
  if (prev && ent && prev.rev === ent.rev) return
  revs[resource] = ent
  const fns = subs.get(resource)
  if (fns) fns.forEach((fn) => { try { fn(ent, resource) } catch (e) { console.error(e) } })
  anySubs.forEach((fn) => { try { fn(ent, resource) } catch (e) { console.error(e) } })
}

// เทียบเลขรุ่นทั้งชุดกับที่รู้อยู่ แล้วแจ้งเฉพาะตัวที่ขยับ
//
// ชุดแรกที่ได้ตอนเพิ่งต่อสาย = สภาพ "ณ ตอนนี้" ไม่ใช่ข่าวที่เพิ่งเกิด ต้องจำไว้เงียบ ๆ
// ไม่งั้นทุกครั้งที่เปิดหน้า จะขึ้นแถบ "สมชายแก้ล็อกแผนแล้ว" ของการแก้เมื่อ 3 ชั่วโมงก่อน
// และหน้าแผนจะขึ้น "มีการรันแผนรอบใหม่เสร็จแล้ว" ทั้งที่รันไปตั้งแต่เช้า
function applySnapshot(snap, silent = false) {
  Object.entries(snap || {}).forEach(([res, ent]) => {
    if (revs[res] && revs[res].rev === ent.rev) return
    if (silent) { revs[res] = ent; return }
    emit(res, ent)
  })
}

// เคยรู้สภาพข้อมูลแล้วหรือยัง — ชุดแรกเท่านั้นที่จำเงียบ ๆ ; ต่อใหม่หลังเน็ตหลุดต้องแจ้งตามปกติ
let seeded = false
// รอบการทำงานของ server ที่เราคุยด้วยอยู่ — เปลี่ยน = server ถูกรีสตาร์ต (deploy รอบใหม่)
let epoch = null

// server เริ่มใหม่ → เลขรุ่นที่จอนี้ถืออยู่ใช้อ้างอิงไม่ได้แล้ว ต้องทิ้งแล้วโหลดของสดมาใหม่
// (ถ้าไม่ทำ จอที่เปิดค้างไว้ตั้งแต่ก่อน deploy จะเซฟไม่ได้ — โดนตีว่า "มีคนแก้แซง" ตลอด)
function onServerRestart(snap) {
  const known = new Set([...Object.keys(revs), ...Object.keys(snap || {})])
  revs = {}
  Object.entries(snap || {}).forEach(([res, ent]) => { revs[res] = ent })
  known.forEach((res) => {
    const ent = revs[res] || { rev: 0, by: '', at: '', detail: '' }
    const fns = subs.get(res)
    if (fns) fns.forEach((fn) => { try { fn(ent, res) } catch (e) { console.error(e) } })
  })
}

async function pollOnce() {
  try {
    const token = auth.get()
    if (!token) return
    const r = await fetch(withBase('/api/state/rev'), {
      headers: { Authorization: `Bearer ${token}` },
    })
    if (!r.ok) return
    const j = await r.json()
    if (j.epoch && epoch && j.epoch !== epoch) { epoch = j.epoch; onServerRestart(j.revs); return }
    epoch = j.epoch || epoch
    applySnapshot(j.revs, !seeded)
    seeded = true
  } catch { /* เน็ตสะดุด — รอบหน้าค่อยว่ากัน */ }
}

function startPolling() {
  if (pollTimer) return
  pollTimer = setInterval(pollOnce, POLL_MS)
  pollOnce()
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
}

function connect() {
  const token = auth.get()
  if (!token) return
  try {
    // EventSource แนบ header เองไม่ได้ → ส่ง token ทาง query (backend ตรวจให้เหมือนกัน)
    es = new EventSource(withBase('/api/events') + '?token=' + encodeURIComponent(token))
  } catch {
    startPolling()
    return
  }
  es.addEventListener('open', () => { stopPolling() })
  es.addEventListener('sync', (e) => {
    // ชุดเลขรุ่นตอนต่อติด — จอที่เพิ่งกลับมาจากหลับ/เน็ตหลุดจะรู้ทันทีว่าพลาดอะไรไป
    try {
      const d = JSON.parse(e.data)
      const snap = d.revs || d           // รองรับรูปแบบเดิมที่ส่งเฉพาะเลขรุ่น
      if (d.epoch && epoch && d.epoch !== epoch) { epoch = d.epoch; onServerRestart(snap); return }
      epoch = d.epoch || epoch
      applySnapshot(snap, !seeded)
    } catch { }
    seeded = true
  })
  es.addEventListener('changed', (e) => {
    try {
      const m = JSON.parse(e.data)
      emit(m.resource, m)
    } catch { }
  })
  es.addEventListener('error', () => {
    // หลุด = ระหว่างนี้ยังต้องรู้ว่าใครแก้อะไร → ถามเป็นระยะไปก่อน แล้วลองต่อใหม่
    try { es.close() } catch { }
    es = null
    startPolling()
    clearTimeout(retryTimer)
    retryTimer = setTimeout(connect, RETRY_MS)
  })
}

/** เริ่มรับข่าว — เรียกครั้งเดียวหลัง login (App.jsx) */
export function start() {
  if (started) return
  started = true
  connect()
  // แท็บที่ถูกพักไว้นาน ๆ เบราว์เซอร์อาจตัดสายเงียบ ๆ — กลับมาดูเมื่อไหร่เช็กให้แน่ใจทันที
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') pollOnce()
  })
}

/** หยุดรับข่าว (ตอน logout) */
export function stop() {
  started = false
  seeded = false
  epoch = null
  me = ''          // logout แล้วอาจมีคนอื่นมาล็อกอินต่อบนจอเดียวกัน
  if (es) { try { es.close() } catch { } ; es = null }
  stopPolling()
  clearTimeout(retryTimer)
  revs = {}
}

// ชื่อผู้ใช้ของจอนี้ — ใช้แยก "ข่าวของคนอื่น" ออกจาก "ข่าวที่เกิดจากการเซฟของเราเอง"
// (เซฟอัตโนมัติยิงข่าวทุก 3 วินาทีระหว่างที่จัดแผน ถ้าไม่แยก แถบแจ้งเตือนจะเด้งใส่คนที่
//  กำลังลากอยู่ตลอดเวลา ทั้งที่เป็นการกระทำของตัวเอง)
let me = ''

/** บอกว่าจอนี้ล็อกอินเป็นใคร (เรียกหลัง api.me() — ดู App.jsx) */
export function setMe(name) { me = String(name || '') }

/** ข่าวนี้เกิดจากการกระทำของจอ/ผู้ใช้คนนี้เองหรือเปล่า */
export function isMe(by) { return !!me && String(by || '') === me }

/** เลขรุ่นล่าสุดที่รู้ของชุดข้อมูลนั้น — แนบไปตอนเซฟเพื่อกันเขียนทับของคนอื่น */
export function revOf(resource) {
  const ent = revs[resource]
  return ent ? ent.rev : null
}

export function infoOf(resource) {
  return revs[resource] || null
}

/** ฟังว่าชุดข้อมูลนี้ถูกใครแก้ → คืนฟังก์ชันสำหรับเลิกฟัง */
export function subscribe(resource, fn) {
  if (!subs.has(resource)) subs.set(resource, new Set())
  subs.get(resource).add(fn)
  return () => {
    const s = subs.get(resource)
    if (s) { s.delete(fn); if (!s.size) subs.delete(resource) }
  }
}

/** ฟังทุกชุดข้อมูล (ใช้กับแถบ "ใครเพิ่งแก้อะไร" ด้านบนจอ) */
export function subscribeAll(fn) {
  anySubs.add(fn)
  return () => anySubs.delete(fn)
}

// ชื่อชุดข้อมูล — ต้องตรงกับฝั่ง backend (livesync.py)
export const RES = {
  planLock: 'plan_lock',
  workday: 'workday',
  planConfig: 'plan_config',
  cylinder: 'cylinder',
  outsource: 'outsource',
  run: 'run',
  schedule: 'schedule',
  yarnLock: 'yarn_lock',        // ล็อกด้าย lot ให้ SC (หน้าแผนด้าย → Gantt)
  yarnItemLock: 'yarn_item_lock', // ล็อกด้ายให้ Item ผ้า (หน้า Monitor Yarn → แผนด้าย)
  master: (name, sheet) => `masters/${name}/${sheet}`,
  plan: (sheet) => `plan/${sheet}`,
}

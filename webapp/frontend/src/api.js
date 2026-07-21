// ---------------- token (เก็บใน localStorage) ----------------
const TOKEN_KEY = 'knitplan_token'
export const auth = {
  get: () => localStorage.getItem(TOKEN_KEY),
  set: (t) => localStorage.setItem(TOKEN_KEY, t),
  clear: () => localStorage.removeItem(TOKEN_KEY),
}

// callback ให้ App เคลียร์สถานะเมื่อ token หมดอายุ (401)
let onUnauthorized = null
export function setOnUnauthorized(fn) { onUnauthorized = fn }

// helper เรียก API (relative path → origin เดียวกับ backend)
async function req(method, url, body) {
  const opt = { method, headers: {} }
  const token = auth.get()
  if (token) opt.headers['Authorization'] = `Bearer ${token}`
  if (body !== undefined) {
    opt.headers['Content-Type'] = 'application/json'
    opt.body = JSON.stringify(body)
  }
  const r = await fetch(url, opt)
  if (r.status === 401 && url !== '/api/login') {
    auth.clear()
    if (onUnauthorized) onUnauthorized()
  }
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try { const j = await r.json(); msg = j.detail || msg } catch { }
    throw new Error(msg)
  }
  return r.json()
}

export const api = {
  // auth
  login: (username, password) => req('POST', '/api/login', { username, password }),
  me: () => req('GET', '/api/me'),
  // run
  run: (mode) => req('POST', '/api/run', { mode }),
  runStop: () => req('POST', '/api/run/stop'),
  runStatus: () => req('GET', '/api/run/status'),
  runLogs: (offset) => req('GET', `/api/run/logs?offset=${offset}`),
  // masters
  masters: () => req('GET', '/api/masters'),
  sheet: (name, sheet) => req('GET', `/api/masters/${encodeURIComponent(name)}/${encodeURIComponent(sheet)}`),
  saveSheet: (name, sheet, columns, rows) =>
    req('PUT', `/api/masters/${encodeURIComponent(name)}/${encodeURIComponent(sheet)}`, { columns, rows }),
  // work day (วันทำงานตามกลุ่มเครื่อง + ยุบสัปดาห์)
  workday: () => req('GET', '/api/workday'),
  saveWorkday: (defaults, weeks, hours, merges) => req('PUT', '/api/workday', { defaults, weeks, hours, merges }),
  seedWorkday: () => req('POST', '/api/workday/seed'),
  // schedule
  schedule: () => req('GET', '/api/schedule'),
  saveSchedule: (schedule) => req('PUT', '/api/schedule', { schedule }),
  // database (ดูไฟล์ข้อมูลต้นทางในโปรเจกต์ read-only)
  database: () => req('GET', '/api/database'),
  databaseSheet: (file, sheet) =>
    req('GET', `/api/database/sheet?file=${encodeURIComponent(file)}` +
      (sheet ? `&sheet=${encodeURIComponent(sheet)}` : '')),
  databaseDownloadUrl: (file) => `/api/database/download?file=${encodeURIComponent(file)}`,
  // outputs
  outputs: () => req('GET', '/api/outputs'),
  outputsBooking: () => req('GET', '/api/outputs/booking'),
  deleteOutput: (name) => req('DELETE', '/api/outputs/' + encodeURIComponent(name)),
  // map item (ผลลัพธ์การเชื่อม Datamining ↔ Booking)
  mapItemFiles: () => req('GET', '/api/map-item'),
  mapItemSheet: (file, sheet) =>
    req('GET', `/api/map-item/sheet?file=${encodeURIComponent(file)}` +
      (sheet ? `&sheet=${encodeURIComponent(sheet)}` : '')),
  mapItemDownloadUrl: (file, v) =>
    `/api/map-item/download?file=${encodeURIComponent(file)}` + (v ? `&t=${Math.round(v)}` : ''),
  // plan (แผนผลิตล่าสุด — แก้ไข + export กลับ Excel)
  planLatest: () => req('GET', '/api/plan'),
  planSheet: (sheet) => req('GET', '/api/plan/sheet' + (sheet ? `?sheet=${encodeURIComponent(sheet)}` : '')),
  planSave: (sheet, columns, rows) => req('PUT', '/api/plan/sheet', { sheet, columns, rows }),
  planLoad: () => req('GET', '/api/plan/load'),
  planAva: () => req('GET', '/api/plan/ava'),
  planPoolMap: () => req('GET', '/api/plan/pool-map'),
  planBookingMc: () => req('GET', '/api/plan/booking-mc'),
  planWeekDays: () => req('GET', '/api/plan/week-days'),
  // v = mtime ของไฟล์ — บังคับให้ URL เปลี่ยนหลังบันทึก เบราว์เซอร์จะได้ไม่หยิบไฟล์เก่าจากแคช
  planDownloadUrl: (v) => '/api/plan/download' + (v ? `?t=${Math.round(v)}` : ''),
  // order color (Datamining → Booking ระดับ ITEM Color — แก้ไข + export)
  orderColorLatest: () => req('GET', '/api/order-color'),
  orderColorSheet: (sheet) => req('GET', '/api/order-color/sheet' + (sheet ? `?sheet=${encodeURIComponent(sheet)}` : '')),
  orderColorSave: (sheet, columns, rows) => req('PUT', '/api/order-color/sheet', { sheet, columns, rows }),
  orderColorDownloadUrl: (v) => '/api/order-color/download' + (v ? `?t=${Math.round(v)}` : ''),
  orderColorAdvise: () => req('POST', '/api/order-color/advise'),
  orderColorCatHistory: () => req('GET', '/api/order-color/cat-history'),
  orderColorPlan: () => req('GET', '/api/order-color/plan'),
  orderColorBookingGantt: () => req('GET', '/api/order-color/booking-gantt'),
  orderColorBookingAva: () => req('GET', '/api/order-color/booking-ava'),
  orderColorBookingLoad: () => req('GET', '/api/order-color/booking-load'),
  orderColorAdviseMoves: (payload) => req('POST', '/api/order-color/advise-moves', payload),
  // export what-if plan → ดาวน์โหลด blob เป็น Excel
  orderColorPlanExport: async (columns, rows) => {
    const token = auth.get()
    const r = await fetch('/api/order-color/plan/export', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify({ columns, rows }),
    })
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    const blob = await r.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = 'order_color_plan.xlsx'
    document.body.appendChild(a); a.click(); a.remove()
    URL.revokeObjectURL(url)
  },
  // จ้างทอ (AI) — แนะนำ item ที่คุ้มค่าจะ outsource
  outsourceAdvise: () => req('POST', '/api/outsource/advise'),
  // การแบ่งงานไปจ้างทอ (qty + สัปดาห์) — Planning.py อ่านไปใช้ตอนรันแผน
  outsourceSplitGet: () => req('GET', '/api/outsource/split'),
  outsourceSplitSave: (item_code, outsource_qty, start_week) =>
    req('POST', '/api/outsource/split', { item_code, outsource_qty, start_week }),
  outsourceSplitDelete: (item_code) =>
    req('DELETE', '/api/outsource/split/' + encodeURIComponent(item_code)),
  // เปลี่ยนกระบอก (AI) — แนะนำว่าควรเปลี่ยนกระบอกให้ item ไหนเพื่อปลดล็อกงานที่ติดเครื่อง
  cylinderAdvise: () => req('POST', '/api/cylinder/advise'),
}

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
    try { const j = await r.json(); msg = j.detail || msg } catch {}
    throw new Error(msg)
  }
  return r.json()
}

export const api = {
  // auth
  login: (username, password) => req('POST', '/api/login', { username, password }),
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
  // schedule
  schedule: () => req('GET', '/api/schedule'),
  saveSchedule: (schedule) => req('PUT', '/api/schedule', { schedule }),
  // outputs
  outputs: () => req('GET', '/api/outputs'),
  outputsBooking: () => req('GET', '/api/outputs/booking'),
  deleteOutput: (name) => req('DELETE', '/api/outputs/' + encodeURIComponent(name)),
  // database (ดูไฟล์ข้อมูลในโปรเจกต์ read-only)
  database: () => req('GET', '/api/database'),
  databaseSheet: (file, sheet) =>
    req('GET', `/api/database/sheet?file=${encodeURIComponent(file)}` +
      (sheet ? `&sheet=${encodeURIComponent(sheet)}` : '')),
  databaseDownloadUrl: (file) => `/api/database/download?file=${encodeURIComponent(file)}`,
}

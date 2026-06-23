// helper เรียก API (relative path → origin เดียวกับ backend)
async function req(method, url, body) {
  const opt = { method, headers: {} }
  if (body !== undefined) {
    opt.headers['Content-Type'] = 'application/json'
    opt.body = JSON.stringify(body)
  }
  const r = await fetch(url, opt)
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try { const j = await r.json(); msg = j.detail || msg } catch {}
    throw new Error(msg)
  }
  return r.json()
}

export const api = {
  // run
  run: (mode) => req('POST', '/api/run', { mode }),
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
  // database (ดูไฟล์ข้อมูลในโปรเจกต์ read-only)
  database: () => req('GET', '/api/database'),
  databaseSheet: (file, sheet) =>
    req('GET', `/api/database/sheet?file=${encodeURIComponent(file)}` +
      (sheet ? `&sheet=${encodeURIComponent(sheet)}` : '')),
  databaseDownloadUrl: (file) => `/api/database/download?file=${encodeURIComponent(file)}`,
}

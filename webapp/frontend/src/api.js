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

// query string ชื่อชีทของหน้าแผน (ไม่ส่งเมื่อว่าง → backend ใช้ค่า default = ชีท PLAN)
const qsSheet = (sheet) => (sheet ? `?sheet=${encodeURIComponent(sheet)}` : '')

// prefix ของ path ที่เว็บถูกเสิร์ฟอยู่:
//   ผ่าน nginx (http://docker-webchat/knitplan/) → '/knitplan'  · เข้าตรง container (:8080) → ''
// nginx ตัด prefix ออกก่อนส่งเข้า backend จึงต้องเติมเฉพาะฝั่งเบราว์เซอร์เท่านั้น
// ใช้กับทั้ง URL ของ API และไฟล์ static ใน public/ (เช่น /logo.png)
const BASE = location.pathname.startsWith('/knitplan') ? '/knitplan' : ''
export const withBase = (path) => BASE + path

// helper เรียก API (relative path → origin เดียวกับ backend)
async function req(method, url, body) {
  const opt = { method, headers: {} }
  const token = auth.get()
  if (token) opt.headers['Authorization'] = `Bearer ${token}`
  if (body !== undefined) {
    opt.headers['Content-Type'] = 'application/json'
    opt.body = JSON.stringify(body)
  }
  const r = await fetch(withBase(url), opt)
  if (r.status === 401 && url !== '/api/login') {
    auth.clear()
    if (onUnauthorized) onUnauthorized()
  }
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    let detail = null
    try { const j = await r.json(); detail = j.detail } catch { }
    if (detail && typeof detail === 'object') {
      // 409 = มีคนแก้แซงไปแล้ว — ส่ง payload ติดไปกับ error ให้หน้าเว็บโหลดของจริงมาแสดง
      // แล้วค่อยให้ผู้ใช้ตัดสินใจ (ดู livesync.Conflict ฝั่ง backend)
      const err = new Error(detail.detail || msg)
      err.conflict = detail.conflict === true
      err.resource = detail.resource
      err.current = detail.current
      err.status = r.status
      throw err
    }
    const err = new Error(detail || msg)
    err.status = r.status
    throw err
  }
  return r.json()
}

// อัปโหลดไฟล์ (multipart) — ใช้ FormData จึงไม่ตั้ง Content-Type เอง ให้เบราว์เซอร์ใส่ boundary
async function upload(url, file) {
  const fd = new FormData()
  fd.append('file', file)
  const token = auth.get()
  const r = await fetch(withBase(url), {
    method: 'POST',
    headers: token ? { Authorization: `Bearer ${token}` } : {},
    body: fd,
  })
  if (r.status === 401) {
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

// ดาวน์โหลดไฟล์แบบแนบ token (ลิงก์ธรรมดาส่ง header ไม่ได้) → เซฟเป็นชื่อ filename
async function download(url, filename) {
  const token = auth.get()
  const r = await fetch(withBase(url), { headers: token ? { Authorization: `Bearer ${token}` } : {} })
  if (r.status === 401) {
    auth.clear()
    if (onUnauthorized) onUnauthorized()
  }
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try { const j = await r.json(); msg = j.detail || msg } catch { }
    throw new Error(msg)
  }
  const href = URL.createObjectURL(await r.blob())
  const a = document.createElement('a')
  a.href = href; a.download = filename
  document.body.appendChild(a); a.click(); a.remove()
  URL.revokeObjectURL(href)
}

export const api = {
  // auth
  login: (username, password) => req('POST', '/api/login', { username, password }),
  me: () => req('GET', '/api/me'),
  // เปลี่ยนรหัสผ่านตัวเอง — ได้ token ใบใหม่กลับมา (ใบเดิมใช้ไม่ได้ทันทีที่รหัสเปลี่ยน)
  changeMyPassword: (old_password, new_password) =>
    req('POST', '/api/me/password', { old_password, new_password }),
  // ผู้ใช้งาน (admin) — admin จัดการได้แค่ user ธรรมดา · จัดการ admin ได้เฉพาะ superadmin
  users: () => req('GET', '/api/users'),
  userCreate: (username, password, role) => req('POST', '/api/users', { username, password, role }),
  userResetPw: (username, password) =>
    req('POST', `/api/users/${encodeURIComponent(username)}/password`, { password }),
  userSetRole: (username, role) => req('PUT', `/api/users/${encodeURIComponent(username)}/role`, { role }),
  userDelete: (username) => req('DELETE', `/api/users/${encodeURIComponent(username)}`),
  // run
  run: (mode) => req('POST', '/api/run', { mode }),
  runStop: () => req('POST', '/api/run/stop'),
  runStatus: () => req('GET', '/api/run/status'),
  runLogs: (offset) => req('GET', `/api/run/logs?offset=${offset}`),
  // dashboard › Machine Availability (สรุปจาก booking ด้วย logic เดียวกับ AVA_MC)
  mcAva: () => req('GET', '/api/mc-ava'),
  mcAvaRefresh: () => req('POST', '/api/mc-ava/refresh'),
  mcAvaStatus: () => req('GET', '/api/mc-ava/status'),
  // dashboard โควตา Job Setup (ชีท DETAIL ใบเดียวกับหน้า Machine Availability — ปุ่มคำนวณใหม่ใช้ตัวเดียวกัน)
  jobSetup: () => req('GET', '/api/job-setup'),
  // dashboard สต๊อกผ้าดิบ (mode: 'core' = ทะเบียน Core Greige · 'all' = ทุก item ที่มีของในคลัง)
  coreStock: (mode) => req('GET', `/api/core-stock?mode=${encodeURIComponent(mode || 'core')}`),
  // dashboard Stock Yarn — ใบ ITEM × LOT × PO (หน้าเว็บประกอบเป็นต้นไม้ + รวมยอดเอง)
  yarnStock: () => req('GET', '/api/yarn-stock'),
  // dashboard Yarn Usage — แถว ITEM ผ้า × ด้าย × lot: ต้องใช้ทั้งใบ / ใช้ไปแล้ว / เหลือต้องใช้ (booking × % ด้ายของใบ KP)
  yarnUsage: () => req('GET', '/api/yarn-usage'),
  // ล็อกด้าย lot ให้ SC + ผ้า (ล็อกที่หน้าแผนด้าย ; Gantt ดูอย่างเดียว)
  // ดูได้ทุกคน · ล็อก/ปลดเฉพาะสิทธิ์ yarn / admin
  yarnLocks: () => req('GET', '/api/yarn-locks'),
  yarnLockAdd: (body) => req('POST', '/api/yarn-locks', body),
  yarnLockRemove: (id) => req('DELETE', `/api/yarn-locks/${encodeURIComponent(id)}`),
  // ล็อกด้ายให้ Item ผ้า (หน้า Monitor Yarn › ล็อกด้าย — หน้านี้แสดงล็อก SC ด้วย)
  // body = { yarn, lot ('' = ทุก lot), color (ด้าย TD ไม่ระบุ lot), fabric, yarn_kg, note }
  yarnItemLocks: () => req('GET', '/api/yarn-item-locks'),
  yarnItemLockOptions: ({ yarn, lot, color }) => req('GET', '/api/yarn-item-locks/options?' + new URLSearchParams(
    { yarn, lot: lot || '', color: color || '' }).toString()),
  yarnItemLockAdd: (body) => req('POST', '/api/yarn-item-locks', body),
  yarnItemLockRemove: (id) => req('DELETE', `/api/yarn-item-locks/${encodeURIComponent(id)}`),
  // หน้าแผนด้าย — ด้าย × สัปดาห์ จากชีทแผน (ไม่ส่ง = PLAN ของไฟล์ล่าสุด) + lot ที่ล็อกให้ SC + ผ้าได้
  yarnPlan: (sheet) => req('GET', '/api/yarn-plan' + qsSheet(sheet)),
  yarnPlanLots: ({ yarn, color, so, fabric }) => req('GET', '/api/yarn-plan/lots?' + new URLSearchParams(
    { yarn, color: color || '', so, fabric }).toString()),
  // หน้า Monitor Yarn — ค่าที่หน้า Stock Yarn ใช้ตอนแสดงผล (อ่านได้ทุกคน · บันทึกเฉพาะ admin)
  // บันทึกเฉพาะค่าของกลุ่มนี้ ไม่แตะค่าตั้งค่าอื่น (ต่างจาก planConfigSave ที่ต้องส่งทุกค่า)
  // withCatalog = แนบรายชื่อ BUYYER / ITEM / ใครถือ ITEM-LOT ไหน (หน้า Monitor Yarn ใช้ตอนตั้งกติกาลูกค้า)
  yarnMonitor: (withCatalog) => req('GET', '/api/yarn-monitor' + (withCatalog ? '?catalog=1' : '')),
  yarnMonitorSave: (values) => req('PUT', '/api/yarn-monitor', { values }),
  // dashboard SC Lead Time — ทะเบียน SC สะสม + ระยะห่าง "วันเปิดใบ → เป้าทอ" (สัปดาห์)
  scLeadtime: () => req('GET', '/api/sc-leadtime'),
  scLeadtimeBackfill: () => req('POST', '/api/sc-leadtime/backfill'),
  // รายละเอียดของช่องที่คลิกบน heatmap (เครื่องย่อยในกลุ่ม + รายการงาน booking ของสัปดาห์นั้น)
  mcAvaDetail: (key, week) =>
    req('GET', `/api/mc-ava/detail?key=${encodeURIComponent(key)}&week=${encodeURIComponent(week)}`),
  // ลิงก์ดู Machine Availability แบบไม่ต้อง login (ปุ่มบนการ์ด Teams) — k = รหัสในลิงก์
  // backend เปิดให้แค่ 3 เส้นนี้ และตอบตัวเดียวกับ mcAva / mcAvaStatus / mcAvaDetail
  shareMcAva: (k) => req('GET', `/api/share/mc-ava?k=${encodeURIComponent(k)}`),
  shareMcAvaStatus: (k) => req('GET', `/api/share/mc-ava/status?k=${encodeURIComponent(k)}`),
  shareMcAvaDetail: (k, key, week) =>
    req('GET', `/api/share/mc-ava/detail?k=${encodeURIComponent(k)}&key=${encodeURIComponent(key)}&week=${encodeURIComponent(week)}`),
  // dashboard › Outsource Availability — เครื่องผู้รับจ้าง (ชีท MC S9) เทียบงานจ้างทอในแผนล่าสุด
  s9Ava: () => req('GET', '/api/s9-ava'),
  // masters
  masters: () => req('GET', '/api/masters'),
  sheet: (name, sheet) => req('GET', `/api/masters/${encodeURIComponent(name)}/${encodeURIComponent(sheet)}`),
  // base_rev = เลขรุ่นที่จอนี้โหลดมา — ไม่ตรงกับของจริง backend จะตอบ 409 แทนที่จะเขียนทับ
  saveSheet: (name, sheet, columns, rows, base_rev) =>
    req('PUT', `/api/masters/${encodeURIComponent(name)}/${encodeURIComponent(sheet)}`,
      { columns, rows, base_rev }),
  // อ่านไฟล์ .xlsx ที่จะนำเข้าทับชีท → {sheet, columns, rows} (ยังไม่บันทึก)
  sheetImportPreview: (name, sheet, file) =>
    upload(`/api/masters/${encodeURIComponent(name)}/${encodeURIComponent(sheet)}/import-preview`, file),
  // Export ชีทเดียวเป็น .xlsx (ดาวน์โหลดแบบแนบ token) — ไฟล์ที่ได้ Import กลับได้ทันที
  masterExport: (name, sheet) =>
    download(`/api/masters/${encodeURIComponent(name)}/${encodeURIComponent(sheet)}/export`, `${sheet}.xlsx`),
  // work day (วันทำงานตามกลุ่มเครื่อง + ยุบสัปดาห์)
  workday: () => req('GET', '/api/workday'),
  saveWorkday: (defaults, weeks, hours, merges, base_rev) =>
    req('PUT', '/api/workday', { defaults, weeks, hours, merges, base_rev }),
  seedWorkday: () => req('POST', '/api/workday/seed'),
  // ยุบสัปดาห์อย่างเดียว (แผงในหน้าปฏิทิน) — merges {YW: YW}
  saveWeekMerges: (merges, base_rev) => req('PUT', '/api/workday/merges', { merges, base_rev }),
  // เพิ่มปีในปฏิทิน (เติม Calendar.xlsx ถึงสิ้นปีนั้น — จันทร์–เสาร์ทำงาน หยุดอาทิตย์)
  extendCalendar: (year) => req('POST', '/api/calendar/extend', { year }),
  // เพิ่มเครื่องใหม่ + ไฟตรวจความพร้อม (ดูได้ทุกคน · เพิ่มได้เฉพาะ admin)
  mcNewCheck: (p) => req('GET', '/api/mc-new/check?' + new URLSearchParams(p).toString()),
  mcNewAudit: () => req('GET', '/api/mc-new/audit'),
  mcNewCreate: (data) => req('POST', '/api/mc-new', data),
  mcNewOptions: () => req('GET', '/api/mc-new/options'),
  // หน้า "เครื่องจักร" รวม — จำนวนเครื่อง/วันทำงาน/ชั่วโมง/กันเครื่อง อยู่ที่เดียว
  mcHub: () => req('GET', '/api/mc-hub'),
  mcHubSave: (changes, revs) => req('POST', '/api/mc-hub', { changes, revs }),
  // Export / Import 4 ชีท (Master MC · MC_Total · Lock_MC · MC Special) เป็นไฟล์เดียว
  mcHubExport: () => download('/api/mc-hub/export', 'MasterMC_machines.xlsx'),
  mcHubImportCheck: (file) => upload('/api/mc-hub/import/check', file),
  // revs = {ชื่อชีท: เลขรุ่นจากขั้นตรวจ} — มีคนแก้ชีทไหนแซงระหว่างนั้น backend ตอบ 409
  mcHubImport: async (file, revs) => {
    const fd = new FormData()
    fd.append('file', file)
    fd.append('revs', JSON.stringify(revs || {}))
    const token = auth.get()
    const r = await fetch(withBase('/api/mc-hub/import'), {
      method: 'POST', headers: token ? { Authorization: `Bearer ${token}` } : {}, body: fd,
    })
    if (r.status === 401) {
      auth.clear()
      if (onUnauthorized) onUnauthorized()
    }
    if (!r.ok) {
      let detail = null
      try { detail = (await r.json()).detail } catch { }
      const err = new Error((detail && typeof detail === 'object' ? detail.detail : detail) || `HTTP ${r.status}`)
      err.conflict = r.status === 409
      throw err
    }
    return r.json()
  },
  // หน้า "จ้างทอ" รวม — เครื่องผู้รับจ้าง + ผ้าที่ต้องจ้างเสมอ + ผ้าที่จ้างทอได้
  s9Hub: () => req('GET', '/api/s9-hub'),
  s9HubSave: (changes, revs) => req('POST', '/api/s9-hub', { changes, revs }),
  // นำเข้าผ้าที่จ้างทอได้ทับทั้งชีท: อัปโหลดไฟล์ → ได้ผลตรวจกลับมาดูก่อน (ยังไม่บันทึก)
  s9ItemsPreview: (file) => upload('/api/s9-hub/items/preview', file),
  s9ItemsCheck: (columns, rows) => req('POST', '/api/s9-hub/items/check', { columns, rows }),
  // นำเข้าผ้าที่ต้องจ้างทอเสมอ (S9 Only) ทับทั้งชีท — ตรวจก่อนแบบเดียวกัน
  s9OnlyPreview: (file) => upload('/api/s9-hub/only/preview', file),
  s9OnlyCheck: (columns, rows) => req('POST', '/api/s9-hub/only/check', { columns, rows }),
  // Import เครื่องของผู้รับจ้าง (MC S9) ทับทั้งชีท — ตรวจก่อนแบบเดียวกัน
  s9McPreview: (file) => upload('/api/s9-hub/mc/preview', file),
  s9McCheck: (columns, rows) => req('POST', '/api/s9-hub/mc/check', { columns, rows }),
  // ย้ายค่าวันทำงานตามชื่อกลุ่มที่เปลี่ยนไป (แผงยังเก็บไว้ใต้ชื่อเดิม = ค่าที่ตั้งไม่ถูกใช้)
  mcFixWorkdayNames: () => req('POST', '/api/mc-new/fix-workday-names'),
  // schedule
  schedule: () => req('GET', '/api/schedule'),
  // runs = [{id, enabled, hour, minute, mode, days}] ; ส่งทั้งชุด (รอบที่ไม่ส่งมา = ลบ)
  saveSchedule: (runs) => req('PUT', '/api/schedule', { runs }),
  // ตั้งค่าระบบ (กติกาการวางแผน) — แก้แล้วมีผลกับการรันแผนรอบถัดไป ; แก้ได้เฉพาะ admin
  planConfig: () => req('GET', '/api/plan-config'),
  planConfigSave: (values) => req('PUT', '/api/plan-config', { values }),
  // keys ว่าง = คืนค่าเริ่มต้นทั้งหมด
  planConfigReset: (keys) => req('POST', '/api/plan-config/reset', { keys: keys || [] }),
  // เฉพาะ superadmin ; คืน {items, total} — เก็บทุกครั้งไม่ตัด จึงโหลดทีละหน้า
  planConfigHistory: (offset = 0, limit = 100) =>
    req('GET', `/api/plan-config/history?offset=${offset}&limit=${limit}`),
  // database (ดูไฟล์ข้อมูลต้นทางในโปรเจกต์ read-only)
  database: () => req('GET', '/api/database'),
  databaseSheet: (file, sheet) =>
    req('GET', `/api/database/sheet?file=${encodeURIComponent(file)}` +
      (sheet ? `&sheet=${encodeURIComponent(sheet)}` : '')),
  databaseDownloadUrl: (file) => withBase(`/api/database/download?file=${encodeURIComponent(file)}`),
  // outputs
  outputs: () => req('GET', '/api/outputs'),
  outputsBooking: () => req('GET', '/api/outputs/booking'),
  outputsSc: () => req('GET', '/api/outputs/sc'),
  deleteOutput: (name) => req('DELETE', '/api/outputs/' + encodeURIComponent(name)),
  // map item (ผลลัพธ์การเชื่อม Datamining ↔ Booking)
  mapItemFiles: () => req('GET', '/api/map-item'),
  mapItemSheet: (file, sheet) =>
    req('GET', `/api/map-item/sheet?file=${encodeURIComponent(file)}` +
      (sheet ? `&sheet=${encodeURIComponent(sheet)}` : '')),
  mapItemDownloadUrl: (file, v) =>
    withBase(`/api/map-item/download?file=${encodeURIComponent(file)}` + (v ? `&t=${Math.round(v)}` : '')),
  // item ทดแทน (Master_Item ชีท "Master_Item V2" — รหัสใน ITEM_LIST เดียวกันใช้แทนกันได้)
  substituteSummary: () => req('GET', '/api/substitute'),
  substituteSearch: (q, onlyMulti = true, limit = 200) =>
    req('GET', `/api/substitute/search?q=${encodeURIComponent(q || '')}` +
      `&only_multi=${onlyMulti ? 'true' : 'false'}&limit=${limit}`),
  substitutePreview: (file) => upload('/api/substitute/preview', file),
  substituteImport: (file) => upload('/api/substitute/import', file),
  // plan (แผนผลิตล่าสุด — แก้ไข + export กลับ Excel)
  planLatest: () => req('GET', '/api/plan'),
  planSheet: (sheet) => req('GET', '/api/plan/sheet' + (sheet ? `?sheet=${encodeURIComponent(sheet)}` : '')),
  // base_file = ชื่อไฟล์แผนที่จอนี้โหลดมา — ถ้ามีคนรันแผนรอบใหม่คั่น backend จะตอบ 409
  // แทนที่จะเอาแถวของแผนใบเก่าไปทับแผนใบใหม่ (เลขรุ่นอย่างเดียวจับเคสนี้ไม่ได้)
  planSave: (sheet, columns, rows, base_rev, base_file) =>
    req('PUT', '/api/plan/sheet', { sheet, columns, rows, base_rev, base_file }),
  // เลขรุ่นของข้อมูลทุกชุด + ใครแก้ล่าสุด (live.js ใช้เป็นทางสำรองเมื่อ SSE ต่อไม่ติด)
  stateRev: () => req('GET', '/api/state/rev'),
  // ใครแก้อะไรไปบ้าง — จำเป็นขึ้นมากตั้งแต่เปลี่ยนเป็นเซฟอัตโนมัติ (ไม่มีปุ่มบันทึกให้ยืนยัน)
  stateLog: (limit, resource) => req('GET', '/api/state/log'
    + `?limit=${limit || 200}` + (resource ? `&resource=${encodeURIComponent(resource)}` : '')),
  // sheet = ชีทที่กำลังเปิด — ชีท REPLAN ใช้ตัวเลขเครื่อง/โควตา setup จากรอบ replan
  // (สัปดาห์ที่ปลดล็อกเครื่องถูกคืนเป็นว่างแล้ว) ไม่ส่ง = ของชีท PLAN ตามเดิม
  planLoad: (sheet) => req('GET', '/api/plan/load' + qsSheet(sheet)),
  planAva: (sheet) => req('GET', '/api/plan/ava' + qsSheet(sheet)),
  // รายการ job ที่ตั้งเครื่องใหม่ (ชีท SETUP_TRACKING) — การ์ดที่เปิดจากแถบโหลดบน Gantt
  planSetupJobs: (sheet) => req('GET', '/api/plan/setup-jobs' + qsSheet(sheet)),
  planPoolMap: () => req('GET', '/api/plan/pool-map'),
  // เครื่องของผู้รับจ้างทอต่อ (กลุ่ม|เกจ) จากชีท MC S9 — เพดานของ Gantt จ้างทอ
  planS9Pool: () => req('GET', '/api/plan/s9-pool'),
  // CAP ของผู้รับจ้างต่อผ้า (ITEM|เกจ) จากชีท Item S9 — ใช้ก่อน cap ของพูล
  planS9ItemCap: () => req('GET', '/api/plan/s9-item-cap'),
  // คู่ (ITEM|เกจ) ที่จ้างทอได้ จากชีท Item S9 — เปิดปุ่ม "ส่งไปจ้างทอ" ในการ์ดงาน
  planS9Eligible: () => req('GET', '/api/plan/s9-eligible'),
  // เครื่องกลุ่มไหนอยู่ "ก้อนเดียวกัน" (ใช้แทนกันได้) — ลูกโซ่ carry คิดเป็นก้อน ไม่ใช่ตามชื่อกลุ่ม
  planMcGroups: () => req('GET', '/api/plan/mc-groups'),
  planBookingMc: (sheet) => req('GET', '/api/plan/booking-mc' + qsSheet(sheet)),
  // full = booking เต็มทุกสัปดาห์ — ไม่ส่ง sheet เฉย ๆ ไม่พอ (backend ถือว่าชีทว่าง = PLAN = replan)
  planBookingItems: (sheet, full) => req('GET', '/api/plan/booking-items'
    + (full ? (qsSheet(sheet) ? qsSheet(sheet) + '&full=1' : '?full=1') : qsSheet(sheet))),
  // ประวัติแผนย้อนหลัง (ทะเบียนสะสม plan_history.xlsx) — ลึกกว่าที่ไฟล์ booking ก้อนเดียวมี
  //   Range = ปี/สัปดาห์ที่ "มีข้อมูลจริง" ให้เลือก ; Items = บล็อกตั้งแต่จุดที่เลือกถึงปัจจุบัน
  // บล็อกที่ได้ต่อกับ planBookingItems พอดี ไม่ทับกัน (backend ตัดที่สัปดาห์เก่าสุดของ booking สด)
  planHistoryRange: () => req('GET', '/api/plan/history-range'),
  planHistoryItems: (year, week) =>
    req('GET', `/api/plan/history-items?year=${encodeURIComponent(year)}&week=${encodeURIComponent(week)}`),
  // สั่งดึงจาก DB (ทำเบื้องหลัง ~16 นาที/7 สัปดาห์) แล้ว poll สถานะเอา
  //   force = ดึง booking ทั้งช่วงที่เลือกใหม่แล้วคำนวณ AVA_MC ทับทะเบียน (ปุ่ม "ดึงใหม่จาก DB")
  //   ไม่ force = เติมเฉพาะสัปดาห์ที่ทะเบียนยังไม่มี
  planHistoryFetch: (year, week, force = false) =>
    req('POST', `/api/plan/history-fetch?year=${encodeURIComponent(year)}&week=${encodeURIComponent(week)}`
      + `&force=${force ? 1 : 0}`),
  planHistoryStatus: () => req('GET', '/api/plan/history-status'),
  // ชีท Program ใน MasterMC → { ITEM_CODE: [TEAM, ...] } (item+ทีมที่ต้องโชว์เป็นตัวหนังสือสีเหลือง)
  planProgram: () => req('GET', '/api/plan/program'),
  // ล็อกแผน (ชีท Lock_Plan ใน MasterMC) — งานที่ห้าม Replan รื้อไปวางใหม่
  // source: BOOKING = บล็อกที่จองเครื่องแล้ว / PLAN = งานที่ Planning วางไว้ (ว่าง = BOOKING)
  planLocks: () => req('GET', '/api/plan/locks'),
  planLockAdd: (payload) => req('POST', '/api/plan/locks', payload),
  // ล็อก/ปลดล็อกหลายสัปดาห์ทีเดียว { add: [...], remove: [...] } — เขียนชีทรอบเดียว
  // (ยิงทีละใบจะเปิด+เซฟ MasterMC.xlsx ทุกครั้ง ช้าและอาจเข้าครึ่ง ๆ ถ้าพังกลางทาง)
  planLockBulk: (payload) => req('POST', '/api/plan/locks/bulk', payload),
  // ลากงานที่ล็อกไว้ไปช่องอื่น → ย้ายล็อกตามไป (ไม่งั้นล็อกค้างอยู่ช่องเดิมแล้วงานหลุดล็อก)
  planLockMove: (payload) => req('POST', '/api/plan/locks/move', payload),
  planLockRemove: (week, item, mcGroup, gauge, source) =>
    req('DELETE', `/api/plan/locks?week=${encodeURIComponent(week)}&item=${encodeURIComponent(item)}` +
      `&mc_group=${encodeURIComponent(mcGroup || '')}&gauge=${encodeURIComponent(gauge || '')}` +
      `&source=${encodeURIComponent(source || '')}`),
  // v = mtime ของไฟล์ — บังคับให้ URL เปลี่ยนหลังบันทึก เบราว์เซอร์จะได้ไม่หยิบไฟล์เก่าจากแคช
  planDownloadUrl: (v) => withBase('/api/plan/download' + (v ? `?t=${Math.round(v)}` : '')),
  // รหัส item ที่ "ให้สีแล้ว" (OPEN_COLOR_LOSS > 0) — Gantt หน้าแผนใช้แยกสีบล็อก
  // (เดิมอยู่ใต้หน้า Order Color ซึ่งถอดทิ้งแล้ว 30 ก.ย. 2569)
  dyedItemCodes: () => req('GET', '/api/dyed-items'),
  // จ้างทอ (AI) — แนะนำ item ที่คุ้มค่าจะ outsource
  outsourceAdvise: () => req('POST', '/api/outsource/advise'),
  // การแบ่งงานไปจ้างทอ (qty + สัปดาห์) — Planning.py อ่านไปใช้ตอนรันแผน
  outsourceSplitGet: () => req('GET', '/api/outsource/split'),
  outsourceSplitSave: (item_code, outsource_qty, start_week) =>
    req('POST', '/api/outsource/split', { item_code, outsource_qty, start_week }),
  outsourceSplitDelete: (item_code) =>
    req('DELETE', '/api/outsource/split/' + encodeURIComponent(item_code)),
  // Change Cylinder (AI) — แนะนำว่าควรทำ Change Cylinder ให้ item ไหนเพื่อปลดล็อกงานที่ติดเครื่อง
  // sheet = ชีทที่เปิดอยู่ (PLAN/REPLAN) — โหมด Replan ใช้แถวและเครื่องว่างคนละชุดกับ PLAN
  cylinderAdvise: (sheet) => req('POST', '/api/cylinder/advise' + qsSheet(sheet)),
  // ผลวิเคราะห์ล่าสุดที่ backend เก็บไว้ — เปิดหน้าใหม่แล้วเห็นของเดิมทันที ไม่ต้องรอ LLM ซ้ำ
  cylinderAdviseLast: (sheet) => req('GET', '/api/cylinder/advise/last' + qsSheet(sheet)),
  // Change Cylinder ที่ user ติ๊กอนุมัติ = เครื่องย้ายเกจแล้วจริง (มีผลทุกรอบจนกว่าจะยกเลิก)
  cylApprovedGet: () => req('GET', '/api/cylinder/approved'),
  cylApprove: (entries, sheet, plan_file) =>
    req('POST', '/api/cylinder/approved', { entries, sheet: sheet || '', plan_file: plan_file || '' }),
  cylRevoke: (id) => req('DELETE', '/api/cylinder/approved?id=' + encodeURIComponent(id)),
  // งานที่ระบบย้ายให้หลังติ๊ก — ส่งไปเก็บใน log ให้ตรวจย้อนหลังได้ว่าแผนเปลี่ยนอะไรไปบ้าง
  // compare = ยอดรายสัปดาห์ก่อน/หลังติ๊ก ต่อ item (ตารางเทียบแผนเก่า→แผนใหม่ ดูย้อนหลังได้)
  cylLogMoves: (moves, approval_ids, sheet, plan_file, compare) =>
    req('POST', '/api/cylinder/moves',
      { moves, approval_ids: approval_ids || [], sheet: sheet || '', plan_file: plan_file || '',
        compare: compare || [] }),
  // แผนเก่า → แผนใหม่: งานที่ถูกย้ายหลังติ๊ก (ดูย้อนหลังได้ ไม่ต้องเป็น admin)
  cylMovesGet: (limit) => req('GET', '/api/cylinder/moves' + (limit ? `?limit=${limit}` : '')),
  // ประวัติทั้งหมด — admin เท่านั้น (หน้าหลัก)
  cylLog: (limit) => req('GET', '/api/cylinder/log' + (limit ? `?limit=${limit}` : '')),
  // ── แจ้งเมลแผนเปลี่ยน (ชีท REPLAN) ──
  // รายการที่ขยับ/ยอดเปลี่ยน/จบเร็ว-ช้า/หลุด/งานใหม่ พร้อมเหตุผล — ใช้พรีวิวก่อนส่ง
  replanChanges: () => req('GET', '/api/replan/changes'),
  // ตั้งค่าเมลพร้อมหรือยัง + ผู้รับเริ่มต้นจาก .env
  replanMailStatus: () => req('GET', '/api/replan/mail-status'),
  // ส่งจริง — เนื้อเมล backend ประกอบเองจากไฟล์แผน (หน้าเว็บส่งได้แค่ผู้รับ/หัวข้อ/หมายเหตุ)
  replanNotify: (to, cc, note, subject) =>
    req('POST', '/api/replan/notify',
      { to: to || [], cc: cc || [], note: note || '', subject: subject || '' }),
  // ── แจ้งเมลแยกตามทีมขาย ──
  // พรีวิว: รอบนี้จะส่งกี่ฉบับ ทีมไหน ถึงใคร ทีมไหนยังไม่มีผู้รับ
  replanMailTeams: () => req('GET', '/api/replan/mail-teams'),
  // ส่งจริง — teams ว่าง = ทุกทีมที่มีผู้รับ
  replanNotifyTeams: (teams, note) =>
    req('POST', '/api/replan/notify-teams', { teams: teams || [], note: note || '' }),
  // ── รายชื่อผู้รับเมลรายทีม (ชีท Sales_Mail ใน MasterMC) ──
  mailRecipients: () => req('GET', '/api/mail-recipients'),
  saveMailRecipients: (rows, baseRev) =>
    req('POST', '/api/mail-recipients', { rows, base_rev: baseRev ?? null }),
}

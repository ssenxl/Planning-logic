// ตัวช่วยฝั่ง frontend: แปลง payload จาก /api/workday → วันทำงานราย (mc_group, gauge, week)
// ต้องให้ผลตรงกับ WorkDay.get_working_days() ใน backend (แหล่งเดียวคือชีท Work Day)

const DEFAULT_WORK_DAYS = 6

function norm(v) {
  let s = String(v ?? '').trim().toUpperCase()
  if (s === 'NAN' || s === 'NONE') return ''
  if (/^\d+\.0+$/.test(s)) s = s.split('.')[0]   // 12.0 → 12
  return s
}

// สร้าง resolver จาก payload ของ /api/workday
// calWorkDays(week) : จำนวนวันทำงานตามปฏิทินของสัปดาห์นั้น (ใช้เช็คสัปดาห์ปิดทั้งก้อน)
export function makeWorkDayResolver(payload, calWorkDays) {
  const { groups = [], defaults = {}, weeks = {}, merges = {} } = payload || {}

  // MC(+gauge) → group key "FACTORY|MC_CAT|GUAGE"
  const byMcGauge = new Map(), byMc = new Map()
  for (const g of groups) {
    const key = [g.factory, g.mc_cat, g.guage].map(norm).join('|')
    byMcGauge.set(`${norm(g.mc)}|${norm(g.guage)}`, key)
    if (!byMc.has(norm(g.mc))) byMc.set(norm(g.mc), key)
  }

  // merges: {src: dst} (คลาย chain มาแล้วจาก backend) → sources ต่อ dst
  const mergeInto = {}, mergeSources = {}
  for (const [src, dst] of Object.entries(merges)) {
    const s = Number(src), d = Number(dst)
    mergeInto[s] = d
    ;(mergeSources[d] ||= []).push(s)
  }

  const groupKey = (mc, gauge) =>
    byMcGauge.get(`${norm(mc)}|${norm(gauge)}`) || byMc.get(norm(mc)) || null

  // วันทำงาน "ดิบ" ของกลุ่มในสัปดาห์เดียว (ยังไม่คิดยุบ/ปฏิทิน)
  const rawDays = (key, w) => {
    if (key == null) return DEFAULT_WORK_DAYS
    const wk = weeks[`${key}|${w}`]
    if (wk != null) return Number(wk)
    const d = defaults[key]
    return d != null ? Number(d) : DEFAULT_WORK_DAYS
  }

  return {
    mergedInto: (w) => (mergeInto[Number(w)] ?? null),
    // วันทำงานจริงที่ใช้วางแผน (ค่าสุดท้าย)
    workDays(mc, gauge, week) {
      if (week == null) return Number(defaults[groupKey(mc, gauge)] ?? DEFAULT_WORK_DAYS)
      const w = Number(week)
      if (mergeInto[w] != null) return 0                    // ถูกยุบเข้าสัปดาห์อื่น
      const sources = mergeSources[w] || []
      const bucket = [w, ...sources]                        // สัปดาห์นี้ + ที่ยุบเข้ามา
      if (calWorkDays && bucket.reduce((s, b) => s + (Number(calWorkDays(b)) || 0), 0) === 0) return 0
      const key = groupKey(mc, gauge)
      // สัปดาห์ปลายทางของการยุบ: ถ้ามีค่ารายสัปดาห์ตั้งไว้ = ยอดรวมทั้งก้อน (user แก้เอง)
      if (sources.length && key != null) {
        const override = weeks[`${key}|${w}`]
        if (override != null) return Number(override)
      }
      return bucket.reduce((s, b) => s + rawDays(key, b), 0)
    },
  }
}

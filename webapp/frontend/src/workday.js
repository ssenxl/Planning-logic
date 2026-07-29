// ตัวช่วยฝั่ง frontend: แปลง payload จาก /api/workday → วันทำงานราย (mc_group, gauge, week)
// ต้องให้ผลตรงกับ WorkDay.get_working_days() ใน backend:
//   วันทำงาน = ค่าที่ตั้ง + (ปฏิทินเปิด − 6)   เลื่อนตามปฏิทิน (5.5,เปิด5→4.5 · เปิด8→7.5)
// ปฏิทินมาใน payload.cal_days (อ่านสดจาก Calendar.xlsx บน server)

const DEFAULT_WORK_DAYS = 6   // หากลุ่มไม่เจอ → 6 วัน (ต้องตรงกับ backend)
const NORMAL_CAL_DAYS = 6     // สัปดาห์ปกติเปิดกี่วัน (ต้องตรงกับ Calendar.NORMAL_CAL_DAYS)

function norm(v) {
  let s = String(v ?? '').trim().toUpperCase()
  if (s === 'NAN' || s === 'NONE') return ''
  if (/^\d+\.0+$/.test(s)) s = s.split('.')[0]   // 12.0 → 12
  return s
}

// สร้าง resolver จาก payload ของ /api/workday
export function makeWorkDayResolver(payload) {
  const { groups = [], defaults = {}, weeks = {}, merges = {}, cal_days = {} } = payload || {}
  // วันที่ปฏิทินเปิดของสัปดาห์นั้น — null = ไม่มีข้อมูลปฏิทิน (ไม่ cap)
  const calRaw = (w) => {
    const v = cal_days[String(w)]
    return v == null ? null : Number(v)
  }

  // (เครื่อง|เกจ) → group key "FACTORY|MC_CAT|GUAGE"
  const byMcGauge = new Map()
  // groups เก็บกลุ่มละ 1 เครื่อง (ตัวแทน ไว้ทำ tree) — ใช้เป็นฐาน
  for (const g of groups) {
    byMcGauge.set(`${norm(g.mc)}|${norm(g.guage)}`, [g.factory, g.mc_cat, g.guage].map(norm).join('|'))
  }
  // mc_map = ทุกเครื่องใน MasterMC (เช่น TSB/26) — กันเครื่องที่ไม่ใช่ตัวแทนกลุ่มตกไป fallback
  for (const [k, v] of Object.entries(payload?.mc_map || {})) byMcGauge.set(k, v)
  // aliases = เครื่องที่ไม่มีใน MasterMC (SKPHF/SKPSL/...) จับกลุ่มจาก FACTORY+CAT+เกจ
  for (const [k, v] of Object.entries(payload?.aliases || {})) {
    if (!byMcGauge.has(k)) byMcGauge.set(k, v)
  }

  // merges: {src: dst} (คลาย chain มาแล้วจาก backend) → sources ต่อ dst
  const mergeInto = {}, mergeSources = {}
  for (const [src, dst] of Object.entries(merges)) {
    const s = Number(src), d = Number(dst)
    mergeInto[s] = d
    ;(mergeSources[d] ||= []).push(s)
  }

  const groupKey = (mc, gauge) => byMcGauge.get(`${norm(mc)}|${norm(gauge)}`) || null

  // วันทำงานของกลุ่มในสัปดาห์เดียว (ยังไม่คิดยุบสัปดาห์)
  // ตั้งค่ารายสัปดาห์ไว้เอง = ใช้ตรง ๆ · ไม่ได้ตั้ง = ค่ามาตรฐาน + (ปฏิทินเปิด − 6) เลื่อนตามปฏิทิน
  const weekDays = (key, w) => {
    if (key != null) {
      const wk = weeks[`${key}|${w}`]
      if (wk != null) return Number(wk)
    }
    const base = key != null && defaults[key] != null ? Number(defaults[key]) : DEFAULT_WORK_DAYS
    const cal = calRaw(w)
    if (cal == null) return base
    if (cal <= 0) return 0                    // โรงปิดทั้งสัปดาห์ → ผลิตไม่ได้
    const over = base - NORMAL_CAL_DAYS
    // กลุ่มที่เดินเกินสัปดาห์ปกติ (6.5) + สัปดาห์ที่ปฏิทินเปิดเกินปกติ (8) → เดินขาด (8−0.5=7.5)
    if (cal > NORMAL_CAL_DAYS && over > 0) return Math.max(0, cal - over)
    return Math.max(0, base + (cal - NORMAL_CAL_DAYS))
  }

  return {
    mergedInto: (w) => (mergeInto[Number(w)] ?? null),
    // วันที่ปฏิทินเปิดของสัปดาห์นั้น (รวมสัปดาห์ที่ยุบเข้ามา) — null = ไม่มีข้อมูลปฏิทิน
    calDays(week) {
      const w = Number(week)
      const bucket = [w, ...(mergeSources[w] || [])]
      if (bucket.every(b => calRaw(b) == null)) return null
      return bucket.reduce((s, b) => s + (calRaw(b) ?? 0), 0)
    },
    // วันทำงานจริงที่ใช้วางแผน (ค่าสุดท้าย) = ค่าในแผง − วันหยุดส่วนเกินของสัปดาห์นั้น
    workDays(mc, gauge, week) {
      if (week == null) return Number(defaults[groupKey(mc, gauge)] ?? DEFAULT_WORK_DAYS)
      const w = Number(week)
      if (mergeInto[w] != null) return 0                    // ถูกยุบเข้าสัปดาห์อื่น
      const sources = mergeSources[w] || []
      const key = groupKey(mc, gauge)
      // สัปดาห์ปลายทางของการยุบ: ถ้ามีค่ารายสัปดาห์ตั้งไว้ = ยอดรวมทั้งก้อน ใช้ตรง ๆ
      if (sources.length && key != null) {
        const override = weeks[`${key}|${w}`]
        if (override != null) return Number(override)
      }
      // สัปดาห์นี้ + ที่ยุบเข้ามา (แต่ละสัปดาห์หักวันหยุดของตัวเองก่อนค่อยรวม)
      return [w, ...sources].reduce((s, b) => s + weekDays(key, b), 0)
    },
  }
}

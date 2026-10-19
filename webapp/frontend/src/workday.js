// ตัวช่วยฝั่ง frontend: แปลง payload จาก /api/workday → วันทำงานราย (mc_group, gauge, week)
// ต้องให้ผลตรงกับ WorkDay.get_working_days() ใน backend:
//   วันทำงาน = ค่าที่ตั้ง + (ปฏิทินเปิด − 6)   เลื่อนตามปฏิทิน (5.5,เปิด5→4.5 · เปิด8→7.5)
//   แล้วคูณ (ชั่วโมงของสัปดาห์นั้น ÷ ชั่วโมงมาตรฐานของกอง) ถ้าสัปดาห์นั้นตั้งชั่วโมงไว้เอง
// ปฏิทินมาใน payload.cal_days (อ่านสดจาก Calendar.xlsx บน server)

import { weekKey } from './weekKeys.js'

const DEFAULT_WORK_DAYS = 6   // หากลุ่มไม่เจอ → 6 วัน (ต้องตรงกับ backend)
const NORMAL_CAL_DAYS = 6     // สัปดาห์ปกติเปิดกี่วัน (ต้องตรงกับ Calendar.NORMAL_CAL_DAYS)
const DEFAULT_WORK_HOURS = 24 // ไม่ตั้งชั่วโมง = 24 (ต้องตรงกับ Calendar.DEFAULT_WORK_HOURS)

function norm(v) {
  let s = String(v ?? '').trim().toUpperCase()
  if (s === 'NAN' || s === 'NONE') return ''
  if (/^\d+\.0+$/.test(s)) s = s.split('.')[0]   // 12.0 → 12
  return s
}

// วันที่ (string/Date) → เลขสัปดาห์เปล่า ตามนิยามของโปรเจกต์ (สัปดาห์ = ศุกร์–พฤหัส)
// ใช้เมื่อ payload ไม่มี cal_week_dates (backend รุ่นเก่า) — กติกาเดียวกับ Calendar.py:
// บวก 3 วันก่อนแล้วคิด ISO week
function isoWeekPlus3(d) {
  const t = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate() + 3))
  // ISO: เลื่อนไปวันพฤหัสของสัปดาห์นั้น แล้วนับสัปดาห์จากต้นปี
  const day = (t.getUTCDay() + 6) % 7            // จันทร์ = 0
  t.setUTCDate(t.getUTCDate() - day + 3)
  const jan4 = new Date(Date.UTC(t.getUTCFullYear(), 0, 4))
  const jan4day = (jan4.getUTCDay() + 6) % 7
  jan4.setUTCDate(jan4.getUTCDate() - jan4day + 3)
  return 1 + Math.round((t - jan4) / (7 * 24 * 3600 * 1000))
}

// ค่าวันที่จากไฟล์/API → 'YYYY-MM-DD' ; อ่านไม่ออก = ''
// รับ 'YYYY-MM-DD [HH:MM]' (จาก API) และ 'dd/mm/yyyy' (จากไฟล์ booking/ชีทแผน — รวมปี พ.ศ.)
export function toIsoDate(v) {
  if (v == null || v === '') return ''
  const s = String(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'nat' || s.toLowerCase() === 'none') return ''
  let iso = s.slice(0, 10)
  const dmy = /^(\d{1,2})\/(\d{1,2})\/(\d{4})/.exec(s)
  if (dmy) {
    let y = Number(dmy[3])
    if (y > 2400) y -= 543                       // พ.ศ. → ค.ศ.
    iso = `${y}-${String(dmy[2]).padStart(2, '0')}-${String(dmy[1]).padStart(2, '0')}`
  }
  return /^\d{4}-\d{2}-\d{2}$/.test(iso) ? iso : ''
}

// สร้างฟังก์ชันแปลง "วันที่" → เลขสัปดาห์เปล่า จาก payload ของ /api/workday
// ใช้ปฏิทินจริงบน server (cal_week_dates + cal_weeks) เป็นหลัก — ตรงกับที่ pipeline ใช้
// คืน null ถ้าแปลงวันที่ไม่ได้ (ค่าว่าง / รูปแบบไม่รู้จัก)
// คืน "คีย์คอลัมน์สัปดาห์" (weekKeys.js — ติดปีเมื่อไกล) เมื่ออ่านจากปฏิทินได้ ;
// นอกปฏิทิน/ไม่มีปฏิทิน = เลขสัปดาห์เปล่าจากสูตร (ตีความเป็นครั้งที่ใกล้วันนี้สุด)
export function makeWeekOfDate(payload) {
  const weeks = payload?.cal_weeks || []
  const dates = payload?.cal_week_dates || []
  const usable = dates.length > 0 && dates.length === weeks.length
  return (v) => {
    const iso = toIsoDate(v)
    if (!iso) return null
    if (usable) {
      for (let i = 0; i < dates.length; i++) {
        if (iso >= dates[i][0] && iso <= dates[i][1]) return weekKey(weeks[i][1], weeks[i][0])
      }
      // นอกช่วงปฏิทิน → ตกไปใช้สูตร (ดีกว่าไม่โชว์อะไรเลย)
    }
    const d = new Date(iso + 'T00:00:00')
    return Number.isNaN(d.getTime()) ? null : String(isoWeekPlus3(d))
  }
}

// แกนสัปดาห์ตามปฏิทินจริง — ใช้เทียบ "ก่อน/หลัง" ข้ามปีได้ ต่างจากเลขสัปดาห์เปล่าที่
// W47 (ปีที่แล้ว) ดูเหมือนมาทีหลัง W36 (ปีนี้) ทั้งที่ผ่านมาแล้ว
//   idxOfDate(v)  : วันที่ → index แถวปฏิทิน (null = แปลงไม่ได้/นอกปฏิทิน)
//   idxOfWeek(w)  : คีย์คอลัมน์สัปดาห์ ('45' / '45@2028') → index แถวปฏิทิน (null = ไม่มีในปฏิทิน)
//   weekAt(i)     : index → คีย์คอลัมน์สัปดาห์
//   todayIdx      : index ของสัปดาห์ปัจจุบัน (null = หาไม่เจอ)
// ปฏิทินอ่านไม่ได้ → usable=false (ผู้เรียกถอยไปใช้เลขสัปดาห์เปล่าแบบเดิม)
export function makeWeekAxis(payload) {
  const weeks = payload?.cal_weeks || []
  const dates = payload?.cal_week_dates || []
  const usable = dates.length > 0 && dates.length === weeks.length
  // คีย์คอลัมน์ของแต่ละแถวปฏิทิน — กติกาคีย์ทำให้ไม่ชนกันเอง จึงหาตำแหน่งด้วย Map ได้ตรง ๆ
  const keys = usable ? weeks.map(([y, w]) => weekKey(w, y)) : []
  const idxByKey = new Map(keys.map((k, i) => [k, i]))
  const idxOfIso = (iso) => {
    if (!usable || !iso) return null
    for (let i = 0; i < dates.length; i++) {
      if (iso >= dates[i][0] && iso <= dates[i][1]) return i
    }
    // ก่อนต้นปฏิทิน = อดีตแน่นอน → -1 ; หลังปลายปฏิทิน = ไกลกว่าทุกสัปดาห์ที่วางได้
    if (iso < dates[0][0]) return -1
    return dates.length
  }
  const now = new Date()
  const todayIso = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
  const todayIdx = usable ? idxOfIso(todayIso) : null
  return {
    usable,
    todayIdx: todayIdx != null && todayIdx >= 0 && todayIdx < dates.length ? todayIdx : null,
    idxOfDate: (v) => idxOfIso(toIsoDate(v)),
    weekAt: (i) => (usable && i >= 0 && i < keys.length ? keys[i] : null),
    idxOfWeek: (w) => {
      if (!usable || w == null || w === '') return null
      const i = idxByKey.get(String(w))
      return i == null ? null : i
    },
  }
}

// สร้าง resolver จาก payload ของ /api/workday
// สัปดาห์ของค่ารายสัปดาห์/การยุบมาเป็น YW ปี*100+สัปดาห์ (ผูกปีเสมอ) → แปลงเป็นคีย์คอลัมน์
// (weekKeys.js) ให้ Gantt หาเจอ ; เลขเปล่า (backend รุ่นเก่า) = ครั้งที่ใกล้วันนี้สุด = คีย์เลขเปล่าพอดี
const colKeyOf = (n) => (n >= 100000 ? weekKey(n % 100, Math.floor(n / 100)) : String(n))

// คีย์ "FAC|CAT|G|W" ของค่ารายสัปดาห์ → "FAC|CAT|G|คีย์คอลัมน์"
function byColumnKey(obj) {
  const out = {}
  for (const [k, v] of Object.entries(obj || {})) {
    const i = k.lastIndexOf('|'), n = Number(k.slice(i + 1))
    if (n >= 100000) out[`${k.slice(0, i)}|${colKeyOf(n)}`] = v
    else if (!(k in out)) out[k] = v
  }
  return out
}

export function makeWorkDayResolver(payload) {
  const { groups = [], defaults = {}, hours = {}, merges = {}, cal_days = {} } = payload || {}
  const weeks = byColumnKey(payload?.weeks)
  const week_hours = byColumnKey(payload?.week_hours)
  // ปฏิทินรายคีย์คอลัมน์ (ติดปีเมื่อไกล — weekKeys.js) ครอบทุกปีในปฏิทิน ; backend รุ่นเก่ามีแค่
  // cal_days (เลขเปล่า = ปีที่ใกล้สุด) → คีย์ติดปีหาไม่เจอ = ไม่ cap เหมือนสัปดาห์นอกปฏิทิน
  const calMap = payload?.cal_days_key || cal_days
  // วันที่ปฏิทินเปิดของสัปดาห์นั้น — null = ไม่มีข้อมูลปฏิทิน (ไม่ cap)
  const calRaw = (w) => {
    const v = calMap[String(w)]
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

  // merges: {src: dst} (YW ทั้งคู่) → คีย์คอลัมน์ + sources ต่อ dst
  // การยุบเป็นของปีนั้น — สัปดาห์เลขเดียวกันของปีอื่นไม่ถูกยุบตาม
  // แผงเก็บทีละคู่ (31→32, 32→33) → คลาย chain เหมือน Calendar.unchain_merge ฝั่ง pipeline
  const mergeRaw = {}
  for (const [src, dst] of Object.entries(merges)) mergeRaw[colKeyOf(Number(src))] = colKeyOf(Number(dst))
  const mergeInto = {}, mergeSources = {}
  for (const s of Object.keys(mergeRaw)) {
    const seen = new Set([s])
    let d = mergeRaw[s]
    while (mergeRaw[d] != null && !seen.has(d)) { seen.add(d); d = mergeRaw[d] }
    mergeInto[s] = d
    ;(mergeSources[d] ||= []).push(s)
  }

  const groupKey = (mc, gauge) => byMcGauge.get(`${norm(mc)}|${norm(gauge)}`) || null

  // ตัวคูณชั่วโมงของสัปดาห์นั้น (ต้องตรงกับ WorkDay._hour_ratio)
  // CAP ถูกคูณชั่วโมงของกองไว้ตั้งแต่ต้นและไม่รู้จักสัปดาห์ → คูณกลับที่ "วัน"
  const hourRatio = (key, w) => {
    if (key == null) return 1
    const wh = week_hours[`${key}|${w}`]
    if (wh == null) return 1
    const base = hours[key] != null ? Number(hours[key]) : DEFAULT_WORK_HOURS
    return base > 0 ? Number(wh) / base : 1
  }

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
    // มีปฏิทินให้เทียบไหม — ผู้เรียกใช้แยก "สัปดาห์นอกปฏิทิน (เช่น W53 ของปีที่ไม่มี)" ออกจาก "อ่านปฏิทินไม่ได้"
    hasCalendar: Object.keys(calMap).length > 0,
    mergedInto: (w) => (mergeInto[String(w)] ?? null),
    // สัปดาห์ที่ยุบเข้ามาที่สัปดาห์นี้ (ปลายทางของการยุบ)
    mergeSourcesOf: (w) => (mergeSources[String(w)] || []),
    // ตัวคูณชั่วโมงของสัปดาห์นั้น — หน้าเครื่องจักรใช้แยก "วันจริง" ออกจาก "วันเทียบเท่า"
    hourRatio: (mc, gauge, week) => hourRatio(groupKey(mc, gauge), String(week)),
    // วันที่ปฏิทินเปิดของสัปดาห์นั้น (รวมสัปดาห์ที่ยุบเข้ามา) — null = ไม่มีข้อมูลปฏิทิน
    calDays(week) {
      const w = String(week)
      const bucket = [w, ...(mergeSources[w] || [])]
      if (bucket.every(b => calRaw(b) == null)) return null
      return bucket.reduce((s, b) => s + (calRaw(b) ?? 0), 0)
    },
    // วันทำงานจริงที่ใช้วางแผน (ค่าสุดท้าย) = ค่าในแผง − วันหยุดส่วนเกินของสัปดาห์นั้น
    workDays(mc, gauge, week) {
      if (week == null) return Number(defaults[groupKey(mc, gauge)] ?? DEFAULT_WORK_DAYS)
      // คีย์คอลัมน์ (weekKeys.js) — ค่ารายสัปดาห์ในแผงกรอกเป็นเลขเปล่า จึงมีผลกับคีย์เลขเปล่าเท่านั้น
      const w = String(week)
      if (mergeInto[w] != null) return 0                    // ถูกยุบเข้าสัปดาห์อื่น
      const sources = mergeSources[w] || []
      const key = groupKey(mc, gauge)
      const ratio = hourRatio(key, w)   // ชั่วโมงสัปดาห์นี้ต่างจากปกติ → คูณชดเชยที่วัน
      // สัปดาห์ปลายทางของการยุบ: ถ้ามีค่ารายสัปดาห์ตั้งไว้ = ยอดรวมทั้งก้อน ใช้ตรง ๆ
      if (sources.length && key != null) {
        const override = weeks[`${key}|${w}`]
        if (override != null) return Number(override) * ratio
      }
      // สัปดาห์นี้ + ที่ยุบเข้ามา (แต่ละสัปดาห์หักวันหยุดของตัวเองก่อนค่อยรวม)
      return [w, ...sources].reduce((s, b) => s + weekDays(key, b), 0) * ratio
    },
  }
}

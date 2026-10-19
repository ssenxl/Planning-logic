// กางช่วงสัปดาห์ของการ์ด Lock_MC / MC_Total — ต้องให้ผลตรงกับ Calendar.week_span
// ฝั่ง pipeline (AVA_MC.py / Planning.py) เป๊ะ ไม่งั้นหน้าเว็บโชว์คนละอย่างกับที่แผนคิดจริง
//
// - ไม่ระบุปี → เดินด้วยเลขสัปดาห์ตรง ๆ (weekTo < week = สัปดาห์เดียว)
// - ระบุปี   → เดินบน timeline จริงของปฏิทิน (calWeeks = [[ปี, สัปดาห์], ...] เรียงตามเวลา
//              จาก /api/workday) → ข้ามปีถูกต้องโดยไม่ต้องเดาว่าปีไหนมี 52 หรือ 53 สัปดาห์
//
// คืน { weeks, warn } · weeks = เลขสัปดาห์เปล่าเรียงตามเวลา · warn = เหตุผลที่ถอยไปใช้แบบไม่มีปี

const LIMIT = 260

const num = v => {
  const s = String(v ?? '').trim()
  if (s === '') return null
  const n = Number(s)
  return isNaN(n) ? null : Math.trunc(n)
}

export function weekSpan(calWeeks, week, weekTo, year, yearTo) {
  const w0 = num(week)
  if (w0 == null) return { weeks: [], warn: null }
  const w1 = num(weekTo) ?? w0
  const bare = w1 >= w0 ? Array.from({ length: w1 - w0 + 1 }, (_, i) => w0 + i) : [w0]

  const y0raw = num(year)
  const y1raw = num(yearTo)
  if (y0raw == null && y1raw == null) return { weeks: bare, warn: null }

  const y0 = y0raw ?? y1raw
  const y1 = y1raw ?? y0
  if (!calWeeks || !calWeeks.length) return { weeks: bare, warn: 'ยังไม่มีข้อมูลปฏิทิน → ใช้ช่วงแบบไม่มีปี' }

  const i0 = calWeeks.findIndex(([y, w]) => y === y0 && w === w0)
  const i1 = calWeeks.findIndex(([y, w]) => y === y1 && w === w1)
  if (i0 < 0 || i1 < 0) {
    const miss = i0 < 0 ? `W${w0}/${y0}` : `W${w1}/${y1}`
    return { weeks: bare, warn: `ไม่พบ ${miss} ในปฏิทิน → ใช้ช่วงแบบไม่มีปี` }
  }
  if (i1 < i0) return { weeks: [w0], warn: `W${w1}/${y1} มาก่อน W${w0}/${y0} → ใช้สัปดาห์เดียว` }
  const end = Math.min(i1, i0 + LIMIT - 1)
  return {
    weeks: calWeeks.slice(i0, end + 1).map(([, w]) => w),
    warn: end < i1 ? `ช่วงยาวเกิน ${LIMIT} สัปดาห์ → ตัดเหลือ ${LIMIT}` : null,
  }
}

// ป้ายบอกช่วงบนการ์ด เช่น "W50/2026 → W3/2027"
export const spanLabel = (week, weekTo, year, yearTo) => {
  const y0 = num(year), y1 = num(yearTo) ?? num(year)
  const a = `W${num(week)}${y0 != null ? `/${y0}` : ''}`
  const b = `W${num(weekTo)}${y1 != null ? `/${y1}` : ''}`
  return [a, b]
}

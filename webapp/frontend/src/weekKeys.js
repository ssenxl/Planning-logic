// คีย์คอลัมน์สัปดาห์ของหน้าเว็บ — แหล่งเดียว (PlanGantt / KnitPlan / mcRecalc / workday ใช้ร่วมกัน)
//
// pipeline คีย์สัปดาห์ด้วย "ปี+สัปดาห์" แล้ว แผนจึงยาวเกิน 1 ปีได้ (W35/2027 กับ W35/2028 อยู่คนละที่)
// แต่หน้าเว็บทั้งหน้าคีย์คอลัมน์ด้วยเลขสัปดาห์เปล่า + เดาปีจากระยะห่าง → ถ้าเปลี่ยนคีย์ทุกคอลัมน์
// จะกระทบทุกจุดที่แกนใช้ ; จึงคงเลขเปล่าไว้กับ "ครั้งที่ใกล้วันนี้ที่สุด" ของเลขสัปดาห์นั้น (±26 สัปดาห์
// = เกณฑ์เดียวกับ isNextYearWeek ที่แกนใช้เดาปีมาตลอด) แล้วติดปีให้ทุกครั้งที่ไกลกว่านั้น:
//
//   ปีก่อน                                   → '45@2025'   (มีอยู่เดิม — ประวัติย้อนหลัง)
//   ปีนี้ (ที่ไม่ใช่อดีตไกลเกินครึ่งปี)          → '45'
//   ปีนี้ อดีตไกลเกินครึ่งปี                    → '5@2026'    (เลขชนกับต้นปีหน้า)
//   ปีหน้า ส่วนที่อยู่ในครึ่งปีข้างหน้า            → '12'        (ส่วนที่วนข้ามปี — เดิม)
//   ปีหน้า ส่วนที่ไกลกว่า / ปีถัดไป             → '45@2027'   (แผนยาวเกินครึ่งปี — เลขชนกับปีนี้)
//
// คีย์เลขเปล่าจึงไม่มีทางชนกันเอง และตีปีกลับได้ด้วย isNextYearWeek ; backend (plan_view.week_key)
// ใช้กติกาเดียวกันทุกตัวอักษร

export const YEAR_SEP = '@'
export const CUR_YEAR = new Date().getFullYear()

// เลขสัปดาห์ปัจจุบันตามนิยาม Fri–Thu (บวก 3 วันก่อนคิด ISO week) — ต้องตรงกับ Calendar.py
export function currentPlanWeek() {
  const now = new Date()
  const d = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate() + 3))
  const dayNum = (d.getUTCDay() + 6) % 7          // จันทร์=0 ... อาทิตย์=6
  d.setUTCDate(d.getUTCDate() - dayNum + 3)        // เลื่อนไปวันพฤหัสของสัปดาห์ ISO
  const firstThursday = d.getTime()
  d.setUTCMonth(0, 1)
  if (d.getUTCDay() !== 4)
    d.setUTCMonth(0, 1 + ((4 - d.getUTCDay()) + 7) % 7)
  return 1 + Math.ceil((firstThursday - d.getTime()) / 604800000)
}

// เลขสัปดาห์ของคีย์ ('45@2027' → 45, '12' → 12)
export function wkNum(w) {
  const s = String(w)
  const i = s.indexOf(YEAR_SEP)
  return Number(i < 0 ? s : s.slice(0, i))
}
// ปีที่ติดมากับคีย์ (null = คีย์เลขเปล่า ต้องเดาปีจากบริบท)
export function wkTagYear(w) {
  const s = String(w)
  const i = s.indexOf(YEAR_SEP)
  return i < 0 ? null : Number(s.slice(i + 1))
}
// ปีของคอลัมน์ เมื่อเป็นคีย์ติดปีที่อยู่ "อดีต" (ปีก่อน หรืออดีตไกลของปีนี้) ; null = ไม่ใช่
// คีย์ติดปีของปีนี้เกิดได้เฉพาะอดีตไกลเกินครึ่งปี (ดูหัวไฟล์) จึงนับเป็นอดีตเสมอ
export function wkPastYear(w) {
  const y = wkTagYear(w)
  return y != null && y <= CUR_YEAR ? y : null
}
// ปีของคอลัมน์ เมื่อเป็นสัปดาห์ "ไกลเกิน 1 ปีข้างหน้า" ; null = ไม่ใช่
export function wkFarYear(w) {
  const y = wkTagYear(w)
  return y != null && y > CUR_YEAR ? y : null
}

// สัปดาห์ "ปีหน้า" ของคีย์เลขเปล่า (แผนวางข้ามปี 52/53 → 1,2,3) — เดาจากระยะที่ใกล้กว่า
// ไปข้างหน้า (n+52−cur) < ย้อนหลัง (cur−n) → ปีหน้า ; ใช้เป็น "ตัวถอย" เมื่อไม่รู้ปีจริงเท่านั้น
export function isNextYearWeek(n, cur = currentPlanWeek()) {
  if (!Number.isFinite(n) || n === 99 || n >= cur) return false
  return (n + 52 - cur) < (cur - n)
}

// (เลขสัปดาห์, ปี) → คีย์คอลัมน์ ตามกติกาหัวไฟล์ ; ไม่รู้ปี = เลขเปล่าเหมือนเดิม
export function weekKey(wk, year) {
  const s = String(wk ?? '').trim()
  if (s === '' || s.includes(YEAR_SEP)) return s
  const y = Number(year), n = Number(s)
  if (!(y > 0) || !Number.isFinite(n) || n === 99) return s
  const tag = `${n}${YEAR_SEP}${y}`
  if (y < CUR_YEAR) return tag
  const cur = currentPlanWeek()
  if (y === CUR_YEAR) return isNextYearWeek(n, cur) ? tag : String(n)
  if (y === CUR_YEAR + 1) return isNextYearWeek(n, cur) ? String(n) : tag
  return tag
}

// ปีจริงของคีย์ — ติดปีมาก็ใช้เลย ; เลขเปล่า: nextYear (คำตอบที่ผู้เรียกรู้จริงจากคอลัมน์ปี)
// ก่อน แล้วค่อยเดาจากระยะห่าง
export function keyYear(w, nextYear) {
  const t = wkTagYear(w)
  if (t != null) return t
  const n = Number(w)
  return (nextYear ?? isNextYearWeek(n)) ? CUR_YEAR + 1 : CUR_YEAR
}

// คีย์ → { week, year } สำหรับเขียนกลับลงแถว (PLAN_WEEK / PLAN_YEAR)
export function keyToWeekYear(w, nextYear) {
  return { week: wkNum(w), year: keyYear(w, nextYear) }
}

// ป้ายให้คนอ่าน: "W45" / "W45 '25" / "W45 '28"
export function wkLabel(w) {
  const y = wkTagYear(w)
  return `W${wkNum(w)}` + (y != null ? ` '${String(y).slice(-2)}` : '')
}

// คีย์เรียงคอลัมน์บนแกนเวลาจริง — ปีละ 53 ช่อง (+53 ไม่ใช่ +52 เพื่อไม่ให้ W1 ปีหน้าชนกับ W53 ปีนี้)
// W99 = sentinel งานล้น → ท้ายสุดเสมอ ; nextYear = คำตอบที่ผู้เรียกรู้จริงจากคอลัมน์ปี
export function weekSortKey(w, nextYear) {
  const t = wkTagYear(w)
  if (t != null) return (t - CUR_YEAR) * 53 + wkNum(w)
  const n = Number(w)
  if (!Number.isFinite(n)) return Number.POSITIVE_INFINITY
  if (n === 99) return 1e6
  return (nextYear ?? isNextYearWeek(n)) ? n + 53 : n
}

// คีย์เรียง → คีย์คอลัมน์ (กลับด้านของ weekSortKey) — ใช้เดินแกนทีละช่อง / เลื่อน ±n สัปดาห์
// ⚠ ปีละ 53 ช่องเสมอ → ปีที่ไม่มี W53 จะได้คีย์ '53' ที่ไม่มีในปฏิทิน (ผู้เรียกกรองด้วยปฏิทินเอง)
export function keyFromSortKey(s) {
  const y = CUR_YEAR + Math.floor((s - 1) / 53)
  return weekKey(s - (y - CUR_YEAR) * 53, y)
}

// แถวแผน → คีย์คอลัมน์ จาก PLAN_WEEK (+ PLAN_YEAR ถ้ามีคอลัมน์) ; ว่าง = ''
export function rowWeekKey(row, wI, yI = -1) {
  if (wI < 0) return ''
  const s = String(row[wI] ?? '').trim().replace(/\.0+$/, '')
  if (s === '' || s.toLowerCase() === 'nan') return ''
  const y = yI >= 0 ? String(row[yI] ?? '').trim() : ''
  return weekKey(s, y)
}

// เขียนคีย์คอลัมน์กลับลงแถว: PLAN_WEEK = เลขสัปดาห์, PLAN_YEAR = ปีจริง (ไม่มีคอลัมน์ปี = เขียนแค่สัปดาห์)
// pipeline อ่านปีจากคอลัมน์นี้ — ลากงานไปคอลัมน์ปีหน้าแล้วไม่เขียนปี งานจะถูกตีเป็นปีผิด
export function writeWeekKey(row, wI, yI, key) {
  const { week, year } = keyToWeekYear(key)
  if (wI >= 0) row[wI] = week
  if (yI >= 0) row[yI] = year
}

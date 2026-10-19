// yarnRules.js — กติกา "CUSTOMER ใช้ได้เฉพาะ lot" (ตั้งที่หน้า Monitor Yarn, ค่า KP_YARN_CUSTOMER_LOTS)
//
// 1 กติกา = { customer, item, match, lot }
//   customer : ชื่อตรงตัวกับช่อง BUYYER ของ view (เลือกจากรายชื่อตอนตั้ง)
//   item     : ว่าง = ทุก ITEM · ใส่ = เฉพาะ ITEM นั้น
//   match    : 'prefix' = lot ขึ้นต้นด้วย · 'exact' = lot ตรงตัว
// เทียบ ITEM/LOT ไม่สนตัวพิมพ์เล็ก-ใหญ่ และตัดช่องว่างหัวท้าย
//
// กติกาเป็นการ "สงวน lot" สองทาง (user 2 ต.ค. 2569: "customer อื่นก็ไม่สามารถมาใช้ lot นี้ได้"):
//   • lot ที่ตรงกติกาของลูกค้าไหน = ของลูกค้านั้นคนเดียว ลูกค้าอื่นใช้ไม่ได้
//   • ลูกค้าที่มีกติกาใช้กับ ITEM นั้น ใช้ได้เฉพาะ lot ที่ตรงกติกาของตัวเอง
//   • นอกนั้นใช้ได้ (ลูกค้าที่ไม่มีกติกา = ทุก lot ยกเว้น lot ที่สงวนให้คนอื่น)
// กติกาที่ระบุ ITEM สงวนเฉพาะ lot ของ ITEM นั้น

export const RULE_KEY = 'KP_YARN_CUSTOMER_LOTS'

const norm = (s) => String(s ?? '').trim().toUpperCase()
const custOf = (r) => String(r?.customer ?? '').trim()

/** กติกาข้อนี้ใช้กับ ITEM นี้ไหม */
export const ruleCoversItem = (r, item) => !norm(r.item) || norm(r.item) === norm(item)

/** lot นี้ตรงกติกาข้อนี้ไหม (ไม่ดู ITEM) */
export function lotMatches(r, lot) {
  const want = norm(r.lot)
  if (!want) return false
  const l = norm(lot)
  return r.match === 'exact' ? l === want : l.startsWith(want)
}

/** กติกาทั้งหมดของลูกค้าคนนี้ (ชื่อต้องตรงตัว) */
export const rulesOf = (rules, customer) => (rules || []).filter(r => custOf(r) === customer)

/** ลูกค้าที่ ITEM+LOT นี้ถูกสงวนไว้ให้ (ตรงกติกาของใครบ้าง) — [] = ไม่ได้สงวนให้ใคร */
export function ownersOf(rules, item, lot) {
  const out = []
  for (const r of rules || []) {
    const c = custOf(r)
    if (c && !out.includes(c) && ruleCoversItem(r, item) && lotMatches(r, lot)) out.push(c)
  }
  return out
}

/** ลูกค้าคนนี้ใช้ ITEM+LOT นี้ได้ไหม (ดูกติกาของทุกคน ไม่ใช่แค่ของตัวเอง) */
export function lotAllowedFor(rules, customer, item, lot) {
  const own = rulesOf(rules, customer).filter(r => ruleCoversItem(r, item))
  if (own.some(r => lotMatches(r, lot))) return true                  // ตรงกติกาของตัวเอง
  if (ownersOf(rules, item, lot).some(c => c !== customer)) return false  // สงวนให้ลูกค้าอื่น
  return !own.length                                                   // มีกติกาของตัวเองแต่ไม่ตรง = ใช้ไม่ได้
}

/** ITEM+LOT นี้ลูกค้า (BUYYER) ถืออยู่ทั้งที่สงวนให้คนอื่น → คืนชื่อเจ้าของ ; ไม่ผิดกติกา = [] */
export function heldByOther(rules, buyer, item, lot) {
  const b = String(buyer ?? '').trim()
  if (!b) return []
  const owners = ownersOf(rules, item, lot)
  return owners.length && !owners.includes(b) ? owners : []
}

/** กติกา 2 ข้อของ "คนละลูกค้า" มี lot ที่ตรงทั้งคู่ได้ไหม (ทับกัน = ตัดสินไม่ได้ว่าเป็นของใคร) */
export function rulesOverlap(a, b) {
  if (custOf(a) === custOf(b)) return false
  if (norm(a.item) && norm(b.item) && norm(a.item) !== norm(b.item)) return false
  const A = norm(a.lot), B = norm(b.lot)
  if (!A || !B) return false
  const ea = a.match === 'exact', eb = b.match === 'exact'
  if (ea && eb) return A === B
  if (ea) return A.startsWith(B)
  if (eb) return B.startsWith(A)
  return A.startsWith(B) || B.startsWith(A)
}

/** ชื่อลูกค้าที่มีกติกา (เรียง A→Z ไม่ซ้ำ) */
export const ruleCustomers = (rules) =>
  [...new Set((rules || []).map(custOf).filter(Boolean))].sort((a, b) => a.localeCompare(b))

// ---------- ซ่อน PO ตามท้ายเลข (KP_YARN_HIDE_PO_ON + KP_YARN_HIDE_PO_SUFFIXES) ----------
// ตัวเทียบเดียวกับ yarn_stock_live._po_hidden ฝั่ง backend (ตัวนับหน้า Monitor Yarn) — แก้ที่หนึ่งต้องแก้อีกที่
export const HIDE_PO_ON_KEY = 'KP_YARN_HIDE_PO_ON'
export const HIDE_PO_SUFFIX_KEY = 'KP_YARN_HIDE_PO_SUFFIXES'

/** ท้าย PO ที่ต้องซ่อนตามค่าที่ตั้ง (สวิตช์ปิด = []) */
export function hiddenPoSuffixes(values) {
  if (!values?.[HIDE_PO_ON_KEY]) return []
  return (values?.[HIDE_PO_SUFFIX_KEY] || []).map(norm).filter(Boolean)
}

/** PO นี้ลงท้ายด้วยท้ายที่ต้องซ่อนไหม — suffixes มาจาก hiddenPoSuffixes() (ตัวพิมพ์ใหญ่แล้ว) */
export const poHidden = (po, suffixes) => {
  if (!suffixes.length) return false
  const p = norm(po)
  return suffixes.some(s => p.endsWith(s))
}

/** ข้อความอ่านง่ายของกติกา 1 ข้อ เช่น "lot ขึ้นต้นด้วย NEL (เฉพาะ ITEM CVC20/1R)" */
export function ruleText(r) {
  const how = r.match === 'exact' ? 'lot ตรงตัว' : 'lot ขึ้นต้นด้วย'
  const it = String(r.item ?? '').trim()
  return `${how} ${String(r.lot ?? '').trim()}` + (it ? ` (เฉพาะ ITEM ${it})` : '')
}

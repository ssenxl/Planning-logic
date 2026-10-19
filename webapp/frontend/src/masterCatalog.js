// ทะเบียน "ข้อมูลหลัก" ฉบับภาษาคน
//
// 24 ก.ย. 2569 — user สั่งให้เหลือแค่ไม่กี่หัวข้อ ไม่ต้องมีเมนูย่อยให้ไล่เปิดทีละชีทอีก:
//   🛠 Master MC · 🤝 Master/S9 · 📦 Target Stock/Core Item · 📋 Item Program · 📅 ปฏิทิน
// ชีทอื่นในไฟล์ Master (Item Special / Item Setup / Lock_Plan / Spare part /
// Job / Sales_Mail / Master_Item / Core_Unlock / Program_Unlock / MC substitute)
// ไม่โผล่ในหน้านี้แล้ว — ข้อมูลยังอยู่ในไฟล์และ pipeline อ่านที่เดิมทุกอย่าง
//
// view  : หน้าเฉพาะที่รวมหลายชีทไว้ที่เดียว (ไม่ผูกกับชีทใดชีทหนึ่ง)
// file/sheet : เปิดชีทตรง ๆ (ใช้กับปฏิทินและสต๊อกเป้าหมายที่เป็นชีทเดียวอยู่แล้ว)
// what   : เก็บอะไร   effect: แก้แล้วกระทบแผนตรงไหน (ทุกบรรทัดตรวจจากโค้ดจริง)

export const CATALOG = [
  {
    view: 'mc-hub', icon: '🛠',
    label: 'Master MC',
    what: 'เครื่องแต่ละ Cat (Factory · CAT · Guage): มีกี่เครื่อง ทำงานกี่วัน/สัปดาห์ กี่ชั่วโมง/วัน '
      + 'รวมถึงสัปดาห์ที่เครื่องไม่เท่าปกติ เครื่องที่กันไว้ และเครื่องที่สงวนให้ POLY/COTTON',
    effect: 'เป็นเพดานของทั้งแผน — จำนวนเครื่องและวันทำงานที่นี่คือสิ่งที่ระบบใช้คิดกำลังผลิตทุกสัปดาห์',
  },
  {
    view: 's9-hub', icon: '🤝',
    label: 'Master/S9',
    what: 'เครื่องของผู้รับจ้างแต่ละเจ้า · ผ้าที่ต้องจ้างทอเสมอ · ผ้าที่จ้างทอได้เมื่อในบ้านไม่ทัน',
    effect: 'เป็นเพดานของงานที่ส่งออกไปจ้างทอ · เครื่องที่ตั้ง Inactive จะไม่ถูกนับเป็นกำลังผลิต',
  },
  {
    file: 'Target_Stock', icon: '📦',
    label: 'Target Stock/Core Item',
    what: 'เป้าหมายต่อเดือนของผ้า core — ระบบคำนวณสต๊อกต่ำสุด/สูงสุดให้จาก TARGET/MONTH',
    effect: 'ผ้า core ถูกวางเป็นรอบผลิตตามสต๊อกนี้ แทนการวางตามวันส่งของใบสั่ง',
    // ค่าตั้งค่าที่ยกมาไว้บนหน้านี้ (user สั่ง 24 ก.ย. 2569): ทะเบียนผ้า core กับรายชื่อ
    // ลูกค้าที่ถือว่าเป็นงานผ้า core ต้องดูคู่กัน — ตัวใดตัวหนึ่งอย่างเดียวไม่ทำให้เป็นงาน core
    // + กันเครื่องระหว่างรอรอบผลิต core (ย้ายมาตอนถอดหน้า "ผ้า core และชนิดใบสั่ง" 24 ก.ย. 2569)
    keys: ['KP_CORE_CUSTOMERS', 'KP_CORE_HOLD_TARGET'],
  },
  // ยกขึ้นมาเป็นหัวข้อของตัวเอง (user สั่ง 24 ก.ย. 2569) — เดิมเปิดได้แค่ท้ายกลุ่ม "ขอบเขตแผน"
  {
    file: 'MasterMC', sheet: 'Program', icon: '📋',
    label: 'Item Program',
    what: 'จับคู่รหัสผ้ากับทีมขายที่ถือตั๋วโปรแกรม (ITEM_CODE + TEAM_NAME ต้องตรงทั้งคู่)',
    effect: 'ผ้าในนี้ถูกล็อกอัตโนมัติก่อนจัดแผนใหม่ (เฉพาะงาน booking) และขึ้นตัวหนังสือสีเหลืองในหน้าแผน',
  },
  {
    file: 'Calendar', sheet: 'Sheet1', icon: '📅',
    label: 'ปฏิทิน',
    what: 'ทุกวันของปี ระบุว่าเป็นวันทำงานหรือวันหยุด (กดสลับได้บนปฏิทิน) · รวมการยุบสัปดาห์',
    effect: 'เป็นฐานของกำลังผลิตทุกสัปดาห์ · สัปดาห์นับศุกร์ถึงพฤหัส',
  },
]

// =============================================================================
// ตารางข้อมูลที่ "ผูกกับกติกาข้อไหน" — โผล่ท้ายกลุ่มนั้นในหน้าตั้งค่าระบบ
// (user สั่ง 24 ก.ย. 2569: ชีทที่เหลือไม่ต้องมีเมนูของตัวเอง ให้ไปอยู่ตามหมวดที่เกี่ยวข้อง)
//
// คีย์ = ชื่อกลุ่มใน plan_config.py (GROUP_INFO) ต้องสะกดตรงเป๊ะ
// ทุก effect ตรวจจากโค้ดจริง — ดูอ้างอิงไฟล์:บรรทัดใน masterCatalog เวอร์ชันก่อนหน้า/mc_new.py
export const GROUP_SHEETS = {
  'Setup': [
    {
      file: 'MasterMC', sheet: 'Item Setup', icon: '⏱',
      label: 'Setup/Days ตามรหัสผ้า',
      what: 'กำหนด Setup/Days ตามตัวขึ้นต้นของรหัสผ้า (ตัวที่ยาวกว่าชนะ)',
      effect: 'ใช้แทนค่า Setup/Days ข้างบน — มีผลเฉพาะเครื่องชนิด SINGLE',
    },
    {
      file: 'MasterMC', sheet: 'Item Special', icon: '🎯',
      label: 'วันทำงาน / ชั่วโมง เฉพาะผ้าบางตัว',
      what: 'ผ้าที่เดินเครื่องไม่เท่ามาตรฐาน ระบุวันทำงานและชั่วโมงต่อวันของผ้านั้นเอง',
      effect: 'ชนะค่าของ Cat เสมอ เมื่อคิดกำลังผลิตของผ้าตัวนั้น',
    },
  ],
  'Change Cylinder': [
    {
      file: 'MasterMC', sheet: 'Spare part', icon: '🧰',
      label: 'อะไหล่ Guage สำรองที่มี',
      what: 'จำนวนชุด Guage สำรองของแต่ละกลุ่มเครื่อง แยกตามโรงงาน',
      effect: 'ไม่มีอะไหล่ = ระบบแนะนำเปลี่ยน Guage ให้ไม่ได้',
    },
    {
      file: 'MasterMC', sheet: 'Job ', icon: '🔩',
      label: 'การเปลี่ยน Guage ที่ทำไปแล้ว',
      what: 'จำนวนครั้งที่เปลี่ยน Guage จริงในแต่ละสัปดาห์ แยกตามโรงและปี',
      effect: 'หักออกจากโควตาการเปลี่ยน Guage ที่เหลือของสัปดาห์นั้น (มีผลเมื่อเปิดให้ระบบย้าย Guage เอง)',
    },
  ],
  'Plan Scope': [
    // ชีท Program ย้ายไปเป็นหัวข้อ "Item Program" ในข้อมูลหลักแล้ว (24 ก.ย. 2569)
    {
      file: 'MasterMC', sheet: 'Program_Unlock', icon: '🔓',
      label: 'ตั๋วโปรแกรมที่ขอไม่ให้ล็อก',
      what: 'รายการตั๋วโปรแกรมที่ยกเว้นจากการล็อกอัตโนมัติ',
      effect: 'ใบในนี้ถูกจัดใหม่ได้เหมือนงานทั่วไป',
    },
    {
      file: 'MasterMC', sheet: 'Core_Unlock', icon: '🔓',
      label: 'ผ้า core ที่ขอไม่ให้ล็อก',
      what: 'รายการงานผ้า core ที่ยกเว้นจากการล็อกอัตโนมัติ',
      effect: 'ระบบไม่เขียนชีทนี้เพิ่มแล้ว แต่ยังอ่านของเดิมอยู่',
    },
    // ย้ายมาจากหน้า "ผ้า core และชนิดใบสั่ง" ที่ถอดออก (24 ก.ย. 2569)
    {
      file: 'Master_Item', sheet: 'Master_Item V2', icon: '🧬',
      label: 'ผ้าที่ใช้แทนกันได้',
      what: 'รหัสผ้าที่สเปกตรงกัน จัดอยู่ ITEM_LIST เดียวกัน',
      effect: 'ผ้าใน ITEM_LIST เดียวกันถูกมองเป็นผ้าตัวเดียวกันตอนดูสต๊อกและจับคู่งาน',
    },
  ],
  'Mail': [
    {
      file: 'MasterMC', sheet: 'Sales_Mail', icon: '📧',
      label: 'รายชื่อผู้รับเมลของแต่ละทีมขาย',
      what: 'ทีมขาย → อีเมลผู้รับ',
      effect: 'ใช้ตอนส่งเมลแจ้งแผนเปลี่ยน แยกฉบับตามทีม',
    },
  ],
  'ขั้นสูง': [
    {
      file: 'MasterMC', sheet: 'MC substitute ', icon: '🗂',
      label: 'เครื่องทดแทน (ของเดิม)',
      what: 'ตารางจับคู่เครื่อง/Guage กับกลุ่มที่ใช้แทนได้',
      effect: 'ระบบวางแผนไม่ได้อ่านชีทนี้ — เก็บไว้ดูเฉย ๆ',
      unused: true,
    },
  ],
}

/** ตารางที่ผูกกับกลุ่มนี้ (เฉพาะที่มีไฟล์/ชีทอยู่จริง) */
export function sheetsOfGroup(groupName, files) {
  const list = GROUP_SHEETS[groupName] || []
  if (!files) return list
  return list.filter(s => {
    const f = files.find(x => x.name === s.file)
    if (!f || !f.exists) return false
    return (f.sheets || []).some(x => String(x).trim().toLowerCase()
      === String(s.sheet).trim().toLowerCase())
  })
}

// คีย์ค่าตั้งค่าที่ "ยกมาอยู่บนหน้าข้อมูลหลัก" — หน้าตั้งค่าจะไม่โชว์ซ้ำในกลุ่มเดิม
// (กลุ่มเดิมขึ้นเป็นป้ายบอกทางแทน เพื่อให้คนที่เคยหาที่เดิมยังตามเจอ)
export const MOVED_KEYS = new Set(CATALOG.flatMap(c => c.keys || []))

/** หน้าข้อมูลหลักที่รับค่านี้ไปดูแล (null = ไม่ได้ย้ายไปไหน) */
export function ownerOfKey(key) {
  return CATALOG.find(c => (c.keys || []).includes(key)) || null
}

const norm = s => String(s ?? '').trim().toLowerCase()

/** ข้อมูลอธิบายของชีทที่เปิดอยู่ (ไม่มีในทะเบียน = null → หน้าเว็บโชว์ชื่อชีทดิบแทน) */
export function describe(file, sheet) {
  return CATALOG.find(c => c.file === file && (!c.sheet || norm(c.sheet) === norm(sheet)))
    || null
}

// ชีทที่หน้ารวม (Master MC / จ้างทอ) เขียนแทนผู้ใช้ — ใช้บอกหมวดในประวัติการแก้ไข
// ต้องตรงกับ SH_* ใน webapp/backend/mc_hub.py, s9_hub.py
const HUB_SHEETS = {
  'mc-hub': ['Master MC', 'MC_Total', 'Lock_MC', 'MC Special'],
  's9-hub': ['MC S9', 'Item S9', 'S9 Only'],
}

/** หมวดของรายการในประวัติ ("masters/<ไฟล์>/<ชีท>" หรือ "workday") → {icon, label} */
export function historyArea(resource) {
  const res = String(resource || '')
  const mc = CATALOG.find(c => c.view === 'mc-hub')
  if (res === 'workday') return { icon: mc.icon, label: `${mc.label} › วันทำงาน / ชั่วโมง` }
  if (res === 'schedule') return { icon: '⏰', label: 'ตั้งเวลารันอัตโนมัติ' }
  const m = res.match(/^masters\/([^/]+)\/?(.*)$/)
  if (!m) return { icon: '📄', label: res }
  const [, file, sheet] = m
  for (const [view, sheets] of Object.entries(HUB_SHEETS)) {
    if (file === 'MasterMC' && sheets.some(s => norm(s) === norm(sheet))) {
      const c = CATALOG.find(x => x.view === view)
      return { icon: c.icon, label: `${c.label} › ${sheet}` }
    }
  }
  const hit = CATALOG.find(c => c.file === file && (!c.sheet || norm(c.sheet) === norm(sheet)))
    || Object.values(GROUP_SHEETS).flat().find(c => c.file === file && norm(c.sheet) === norm(sheet))
  if (hit) return { icon: hit.icon, label: hit.label }
  return { icon: '📄', label: sheet ? `${file} › ${sheet}` : file }
}

/** รายการที่จะโชว์ในเมนู — หน้าเฉพาะโชว์เสมอ ส่วนชีทตรงต้องมีไฟล์อยู่จริง */
export function catalogFor(files) {
  const has = (file, sheet) => {
    const f = (files || []).find(x => x.name === file)
    if (!f || !f.exists) return false
    if (!sheet) return true
    return (f.sheets || []).some(s => norm(s) === norm(sheet))
  }
  return CATALOG.filter(c => c.view || has(c.file, c.sheet))
}

/** ชีทที่ต้องเปิดจริงสำหรับรายการนี้ (ไฟล์ชีทเดียวไม่ต้องระบุชื่อชีทในทะเบียน) */
export function sheetOf(entry, files) {
  if (entry.sheet) return entry.sheet
  const f = (files || []).find(x => x.name === entry.file)
  return f?.sheets?.[0] || ''
}

// =============================================================================
// คอลัมน์ที่ "ระบบวางแผนอ่านจริง" ของแต่ละชีท — คอลัมน์นอกรายการนี้ถูกซ่อนจากหน้าจอ
// (ซ่อนเฉย ๆ ข้อมูลในไฟล์ Excel ยังอยู่ครบ กดปุ่มกางดูได้ตลอด)
export const COLUMNS = {}

export function columnsOf(file, sheet) {
  return COLUMNS[`${file}::${norm(sheet)}`] || null
}

export function splitColumns(file, sheet, columns) {
  const reg = columnsOf(file, sheet)
  if (!reg) return null
  const idx = new Map((columns || []).map((c, i) => [norm(c), i]))
  const used = []
  for (const u of reg.used) {
    const i = idx.get(norm(u.col))
    if (i !== undefined) used.push({ ci: i, label: u.label, col: columns[i], main: !!u.main })
  }
  const usedSet = new Set(used.map(u => u.ci))
  const rest = (columns || []).map((c, i) => ({ ci: i, label: String(c), col: c }))
    .filter(c => !usedSet.has(c.ci))
  return { used, rest, note: reg.unusedNote || '' }
}

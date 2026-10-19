// คอลัมน์ของชีทแผนที่ "ซ่อน" / "เปลี่ยนชื่อที่แสดง" บนหน้าเว็บ — ใช้ร่วมกันระหว่าง
// ตาราง popup ของ item (KnitPlan) กับการ์ดรายละเอียดงานบน Gantt (PlanGantt)
// แค่ป้ายบนจอเท่านั้น: ข้อมูลในแถว การคำนวณ การเซฟ และ Excel ที่ export ยังใช้ชื่อจริงครบทุกคอลัมน์

export const PLAN_HIDDEN_COLS = new Set([
  'REQUIRED_MC', 'FACTORY_WORKING_DAYS', 'CALENDAR_WORKING_DAYS', 'WD_BASE', 'WD_W32',
  'MC_SHARED', 'MC_BOOKING', 'MC_HOLD_SPLIT', 'CYLINDER_CHANGE', 'LINE_REMARK',
  'S9_ROUTING', 'S9_SUPP', 'S9_MC', 'S9_GAUGE', 'S9_POOL', 'COLOR_PART',
  'FOLD_QTY', 'FOLD_REMAINDER', 'FOLD_WARN',
  'BOOKING_WEEK_FROM', 'BOOKING_WEEK_TO', 'DUE_BASIS',
])

export const PLAN_COL_LABEL = {
  SETUP_DAYS: 'Setup Days',
  NEW_MC: 'Setup MC',
  CARRYOVER_MC: 'Continue',
  DAILY_CAPACITY: 'Capacity/Day',
}

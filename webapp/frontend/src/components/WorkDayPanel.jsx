import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { useAutoSave } from '../autosave.js'
import SaveStatus from './SaveStatus.jsx'

// แผง "ยุบสัปดาห์" ในหน้าปฏิทิน — แหล่งเดียว = ชีท Week Merge ใน Calendar.xlsx
//   สัปดาห์วันหยุดเยอะ → รวมเข้าสัปดาห์ข้างเคียง (วันทำงานไปรวมที่ปลายทาง) ใช้ทั้งระบบ
//   การยุบเป็นของปีที่หน้าปฏิทินกำลังดู (คีย์ YW = ปี*100+สัปดาห์) — ไม่ผูกปีเมื่อไร
//   ค่าของปีนี้จะไหลไปใช้กับสัปดาห์เลขเดียวกันของปีหน้าเอง
// วัน/ชั่วโมงของกองเครื่อง (ค่ามาตรฐาน + รายสัปดาห์ รวมสัปดาห์ที่ยุบ) แก้ที่หน้าเครื่องจักร
// (ข้อมูลหลัก › Master MC) ที่เดียว — แผงนี้แค่บอกว่าสัปดาห์ยุบยังขาดอะไร ไม่บล็อกการยุบ
// (ถ้าบล็อก จะยุบสัปดาห์ไม่ได้เลย เพราะวันของสัปดาห์ยุบกรอกได้หลังยุบแล้วเท่านั้น)

const WEEKS = Array.from({ length: 53 }, (_, i) => i + 1)
const gkey = (g) => [g.factory, g.mc_cat, g.guage].map(v => String(v ?? '').trim().toUpperCase()).join('|')
const ywLabel = (yw) => `W${yw % 100}/${Math.floor(yw / 100)}`
const toDraft = (d) => {
  const mf = {}
  for (const [s, t] of Object.entries(d?.merges || {})) mf[Number(s)] = Number(t)
  return mf
}

// ro = โหมดดูอย่างเดียว (หน้า Master Data) — ปุ่มยุบสัปดาห์ถูกล็อก (แก้ได้ที่หน้าตั้งค่าระบบ)
// year = ปีที่หน้าปฏิทินกำลังดู
export default function WorkDayPanel({ ro, year: yearProp }) {
  const [data, setData] = useState(null)             // payload จาก /api/workday
  const [mergeDraft, setMergeDraft] = useState({})   // YW -> YW ปลายทาง
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [savedAt, setSavedAt] = useState(null)
  const revRef = useRef(null)          // เลขรุ่นของแผงวันทำงานที่จอนี้ถืออยู่
  const skipRef = useRef(false)        // กันไม่ให้การโหลดข้อมูลใหม่ถูกนับเป็น "ผู้ใช้แก้"

  const load = async () => {
    setLoading(true); setErr('')
    try {
      const d = await api.workday()
      revRef.current = d.rev ?? null
      skipRef.current = true          // ค่าที่เพิ่งโหลดมาไม่ใช่การแก้ของผู้ใช้ → อย่าเซฟย้อนกลับ
      setData(d)
      setMergeDraft(toDraft(d))
    } catch (e) { setErr('โหลดข้อมูลยุบสัปดาห์ไม่สำเร็จ: ' + e.message) }
    finally { setLoading(false) }
  }
  useEffect(() => { load() }, [])

  const mergeSources = useMemo(() => {
    const m = {}
    for (const [s, t] of Object.entries(mergeDraft)) (m[t] ||= []).push(Number(s))
    return m
  }, [mergeDraft])

  // ปีที่กำลังดู + สัปดาห์ที่มีในปฏิทินของปีนั้น (บางปีไม่มี W53 / ปฏิทินยุบบางสัปดาห์ไว้แล้ว)
  const todayYw = data?.today_yw ?? null
  const year = yearProp ?? (todayYw ? Math.floor(todayYw / 100) : new Date().getFullYear())
  const ywOf = (w) => year * 100 + w
  const calYw = data?.cal_days_yw || {}
  const hasCal = Object.keys(calYw).length > 0
  const inCal = (w) => !hasCal || calYw[String(ywOf(w))] != null
  const yearWeeks = WEEKS.filter(w => inCal(w) || mergeDraft[ywOf(w)] != null || mergeSources[ywOf(w)])

  const dirty = useMemo(() => {
    if (!data) return false
    const saved = toDraft(data)
    for (const k of new Set([...Object.keys(saved), ...Object.keys(mergeDraft)])) {
      if ((saved[k] ?? null) !== (mergeDraft[k] ?? null)) return true
    }
    return false
  }, [data, mergeDraft])

  // สัปดาห์ยุบ (ตั้งแต่สัปดาห์นี้) ที่หน้าเครื่องจักรยังกรอกวัน/ชั่วโมงไม่ครบ — ดูจากของที่บันทึกแล้ว
  // นับเฉพาะกองที่มีเครื่องจริง (กองเปล่าวางแผนลงไม่ได้อยู่แล้ว)
  const gaps = useMemo(() => {
    if (!data) return []
    const liveGroups = (data.groups || []).filter(g => (g.total ?? 1) > 0)
    const wk = data.weeks || {}, wh = data.week_hours || {}
    const targets = [...new Set(Object.values(data.merges || {}).map(Number))]
      .filter(w => todayYw == null || w >= todayYw).sort((a, b) => a - b)
    return targets.map(w => ({
      week: w,
      days: liveGroups.filter(g => wk[`${gkey(g)}|${w}`] == null).length,
      hours: liveGroups.filter(g => wh[`${gkey(g)}|${w}`] == null).length,
      over: ((data.day_gaps || []).find(x => x.week === w)?.over || []).length,
    })).filter(x => x.days || x.hours || x.over)
  }, [data, todayYw])

  // ---- เซฟให้เองทุกครั้งที่กดยุบ/เลิกยุบ
  const autosave = useAutoSave(async () => {
    const merges = {}
    for (const [s, t] of Object.entries(mergeDraft)) if (t != null) merges[String(s)] = Number(t)
    try {
      const r = await api.saveWeekMerges(merges, revRef.current)
      revRef.current = r.rev
      setSavedAt(Date.now()); setErr('')
      // โหลดใหม่ให้แถบ "สัปดาห์ยุบที่ยังขาดวัน/ชั่วโมง" ตรงกับไฟล์ (ไม่แตะที่กำลังแก้ค้าง)
      const d = await api.workday()
      revRef.current = d.rev ?? revRef.current
      setData(d)
    } catch (e) {
      if (e.conflict) { setErr(e.message + ' — กด "โหลดใหม่" เพื่อดูของล่าสุด'); return }
      throw e
    }
  }, { delay: 800 })

  useEffect(() => {
    if (!data || ro) return                  // โหมดดูอย่างเดียว: ไม่เขียนกลับไฟล์เด็ดขาด
    if (skipRef.current) { skipRef.current = false; return }
    if (dirty) autosave.saveSoon(true)
  }, [mergeDraft, dirty, data, ro])

  // อีกจอแก้วันทำงาน/ยุบสัปดาห์ → โหลดของใหม่มาแสดง (ถ้าเราไม่ได้ค้างแก้อะไรอยู่)
  useEffect(() => live.subscribe(live.RES.workday, (ent) => {
    if (ent && ent.rev === revRef.current) return
    if (autosave.dirty || autosave.status === 'saving') {
      setMsg(`${ent?.by || 'อีกคนหนึ่ง'} เพิ่งแก้วันทำงาน — กดโหลดใหม่เมื่อพร้อม`)
      return
    }
    load()
  }), [autosave.dirty, autosave.status])

  if (loading && !data) return <div className="wdpanel"><div className="hint">กำลังโหลด...</div></div>

  // w / target = YW ของปีที่กำลังดู
  const setMerge = (w, target) => setMergeDraft(p => {
    const n = { ...p }
    if (target == null || n[w] === target) delete n[w]
    else n[w] = target
    return n
  })

  return (
    <div className="wdpanel">
      {err && <div className="msg err">{err}</div>}
      {msg && <div className="msg ok">{msg}</div>}
      {gaps.length > 0 && (
        <div className="msg warn">
          ⚠️ สัปดาห์ที่ยุบแล้วยังกรอกวัน/ชั่วโมงไม่ครบ —{' '}
          {gaps.map(g => `${ywLabel(g.week)}: ` + [
            g.days ? `ขาดวัน ${g.days} Cat` : '',
            g.hours ? `ขาดชม. ${g.hours} Cat` : '',
            g.over ? `วันเกินเพดาน ${g.over} Cat` : '',
          ].filter(Boolean).join(' · ')).join(' | ')}
          <div className="hint small">
            กรอกที่ <b>ข้อมูลหลัก › Master MC</b> → กดชื่อ Cat → ค่ารายสัปดาห์ (หรือปุ่มกรอกทีเดียวทุก Cat ที่แถบเตือนด้านบนของหน้านั้น)
            · Replan จะไม่รันจนกว่าจะครบ
          </div>
        </div>
      )}

      {/* ยุบสัปดาห์ — ระดับปฏิทินรวม ใช้ทั้งระบบ (ของปีที่กำลังดูเท่านั้น) */}
      <div className="wdmergecal">
        <div className="wdmergehead">
          <b>ยุบสัปดาห์ของปี {year} (ใช้ทั้งระบบ)</b>
          <span className="hint small">
            สัปดาห์วันหยุดเยอะ → ยุบเข้าสัปดาห์ข้างเคียง วันทำงานไปรวมที่ปลายทาง · ใช้กับปีนี้ปีเดียว
            ปีอื่นเลือกปีที่ปฏิทินด้านบน
          </span>
        </div>
        <div className="wdmgrid">
          {yearWeeks.map(w => {
            const yw = ywOf(w)
            const merged = mergeDraft[yw]
            const receives = (mergeSources[yw] || []).length > 0
            return (
              <div key={w} className={'wdmcell' + (merged != null ? ' merged' : receives ? ' recv' : '')}>
                <div className="wdmwk">
                  <span>W{w}</span>
                  {merged != null ? <span className="tag o">→W{merged % 100}</span>
                    : receives ? <span className="tag e">◀รวม</span> : null}
                </div>
                <div className="wdmerge">
                  <button disabled={ro || w <= 1 || !inCal(w - 1)} title={`ยุบเข้า W${w - 1}`}
                    className={merged === ywOf(w - 1) ? 'on' : ''}
                    onClick={() => setMerge(yw, ywOf(w - 1))}>◀</button>
                  <button disabled={ro || w >= 53 || !inCal(w + 1)} title={`ยุบเข้า W${w + 1}`}
                    className={merged === ywOf(w + 1) ? 'on' : ''}
                    onClick={() => setMerge(yw, ywOf(w + 1))}>▶</button>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* ไม่มีปุ่มบันทึก — ป้ายนี้คือสิ่งที่บอกว่าของเข้าไฟล์หรือยัง (ดู autosave.js) */}
      {(autosave.status !== 'idle' || dirty) && (
        <div className="wdsave">
          <SaveStatus status={autosave.status} error={autosave.error} at={savedAt}
            onRetry={autosave.flush} />
          <button className="ghost" onClick={load} title="โหลดค่าล่าสุดจากไฟล์">⟳ โหลดใหม่</button>
        </div>
      )}
      <div className="hint small wdnote">
        ยุบสัปดาห์มีผลกับปี {year} เท่านั้น · วันทำงาน/ชั่วโมงของแต่ละ Cat (ค่ามาตรฐาน รายสัปดาห์ และสัปดาห์ที่ยุบ)
        แก้ที่ <b>ข้อมูลหลัก › Master MC</b> · มีผลกับ AVA_MC / Planning / การลากงานในแผน
      </div>
    </div>
  )
}

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'

// แผงจัดการ "วันทำงานตามกลุ่มเครื่อง" (Factory / MC_CAT / Guage) + ยุบสัปดาห์
// แหล่งเดียว = ชีท Work Day / Week Merge ใน Calendar.xlsx (user ปรับเองทั้งหมด ไม่มีกฎ 17/32)
//   - ค่ามาตรฐานของกลุ่ม (ใช้ทุกสัปดาห์)  ·  ค่าเฉพาะรายสัปดาห์ (เว้นว่าง = ใช้ค่ามาตรฐาน)
//   - ยุบสัปดาห์: W ที่วันหยุดเยอะ → รวมเข้าสัปดาห์ข้างเคียง (วันทำงานไปรวมที่ปลายทาง)
//   - panel ซ้ายเป็น tree (Factory → MC_CAT → Guage) ยุบ-กางได้ + เลือกหลายกลุ่มพร้อมกัน
//     กรอกค่าครั้งเดียว → เขียนลงทุกกลุ่มที่เลือก

const FALLBACK = 6
const FALLBACK_HOURS = 24   // ไม่ตั้ง = 24 ชม. (ไม่ลดกำลังผลิต)
const WEEKS = Array.from({ length: 53 }, (_, i) => i + 1)
const MIXED = Symbol('mixed')   // ค่าปัจจุบันของกลุ่มที่เลือกไม่ตรงกัน

const gkey = (g) => [g.factory, g.mc_cat, g.guage].map(v => String(v ?? '').trim().toUpperCase()).join('|')
const num = (s) => { const v = Number(String(s).trim()); return String(s).trim() === '' || isNaN(v) ? null : v }

// checkbox 3 สถานะ (เลือกครบ / เลือกบางส่วน / ไม่เลือก)
function Tri({ checked, indeterminate, onChange }) {
  const ref = useRef(null)
  useEffect(() => { if (ref.current) ref.current.indeterminate = !!indeterminate && !checked }, [indeterminate, checked])
  return <input ref={ref} type="checkbox" className="wdchk" checked={checked}
    onClick={e => e.stopPropagation()} onChange={onChange} />
}

export default function WorkDayPanel() {
  const [data, setData] = useState(null)         // payload จาก /api/workday
  const [selected, setSelected] = useState(new Set()) // group keys ที่เลือก (หลายกลุ่มได้)
  const [collapsed, setCollapsed] = useState(new Set()) // id หัวข้อที่ยุบ (F:.. / C:..)
  const [q, setQ] = useState('')                 // ค้นหากลุ่ม
  const [defDraft, setDefDraft] = useState({})   // key -> string (ค่ามาตรฐานที่กำลังแก้)
  const [hourDraft, setHourDraft] = useState({}) // key -> string (ชั่วโมงมาตรฐานที่กำลังแก้)
  const [wkDraft, setWkDraft] = useState({})     // "key|W" -> string (ค่ารายสัปดาห์ที่กำลังแก้)
  const [mergeDraft, setMergeDraft] = useState({}) // week(number) -> targetWeek(number)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  const load = async () => {
    setLoading(true); setErr('')
    try {
      const d = await api.workday()
      setData(d)
      resetDrafts(d)
      setSelected(prev => (prev.size ? prev : new Set(d.groups.length ? [gkey(d.groups[0])] : [])))
    } catch (e) { setErr('โหลดข้อมูลวันทำงานไม่สำเร็จ: ' + e.message) }
    finally { setLoading(false) }
  }
  useEffect(() => { load() }, [])

  function resetDrafts(d) {
    const df = {}; for (const [k, v] of Object.entries(d.defaults || {})) df[k] = String(v)
    const hf = {}; for (const [k, v] of Object.entries(d.hours || {})) hf[k] = String(v)
    const wf = {}; for (const [k, v] of Object.entries(d.weeks || {})) wf[k] = String(v)
    const mf = {}; for (const [s, t] of Object.entries(d.merges || {})) mf[Number(s)] = Number(t)
    setDefDraft(df); setHourDraft(hf); setWkDraft(wf); setMergeDraft(mf)
  }

  // กลุ่ม unique (จาก payload) + กรองด้วยคำค้น
  const groups = useMemo(() => {
    if (!data) return []
    const seen = new Set(), out = []
    for (const g of data.groups) {
      const k = gkey(g)
      if (seen.has(k)) continue
      seen.add(k); out.push({ ...g, key: k })
    }
    // ค้นแบบแยกคำ (เว้นวรรค) ต้องเจอทุกคำ — ตัวคั่น · . | ที่พิมพ์มาถือเป็นช่องว่าง
    const terms = q.toLowerCase().split(/[\s·.|]+/).filter(Boolean)
    return terms.length ? out.filter(g => { const k = g.key.toLowerCase(); return terms.every(t => k.includes(t)) }) : out
  }, [data, q])

  // tree: Factory → MC_CAT → [items]
  const tree = useMemo(() => {
    const facs = new Map()
    for (const g of groups) {
      if (!facs.has(g.factory)) facs.set(g.factory, new Map())
      const cats = facs.get(g.factory)
      if (!cats.has(g.mc_cat)) cats.set(g.mc_cat, [])
      cats.get(g.mc_cat).push(g)
    }
    return [...facs.entries()].map(([factory, cats]) => ({
      factory,
      keys: [].concat(...[...cats.values()].map(items => items.map(i => i.key))),
      cats: [...cats.entries()].map(([mc_cat, items]) => ({ mc_cat, items, keys: items.map(i => i.key) })),
    }))
  }, [groups])

  // ค้นหาอยู่ = กางทุกหัวข้อ (ไม่สนสถานะ collapsed)
  const collapsedEff = q.trim() ? new Set() : collapsed
  const toggleCollapse = (id) => setCollapsed(p => { const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n })

  const allGroupsByKey = useMemo(() => {
    const m = new Map()
    if (data) for (const g of data.groups) m.set(gkey(g), { ...g, key: gkey(g) })
    return m
  }, [data])

  const selKeys = useMemo(() => [...selected], [selected])
  const selectOnly = (key) => setSelected(new Set([key]))
  const toggleKey = (key) => setSelected(p => { const n = new Set(p); n.has(key) ? n.delete(key) : n.add(key); return n })
  const toggleMany = (keys, allSel) => setSelected(p => {
    const n = new Set(p); allSel ? keys.forEach(k => n.delete(k)) : keys.forEach(k => n.add(k)); return n
  })

  // diff กับที่บันทึกไว้ = มีอะไรยังไม่บันทึก
  const dirty = useMemo(() => {
    if (!data) return false
    const savedDef = data.defaults || {}, savedWk = data.weeks || {}, savedMg = data.merges || {}, savedHr = data.hours || {}
    const defKeys = new Set([...Object.keys(savedDef), ...Object.keys(defDraft)])
    for (const k of defKeys) {
      const a = num(defDraft[k] ?? ''), b = savedDef[k] ?? null
      if ((a ?? null) !== (b ?? null)) return true
    }
    const hrKeys = new Set([...Object.keys(savedHr), ...Object.keys(hourDraft)])
    for (const k of hrKeys) {
      const a = num(hourDraft[k] ?? ''), b = savedHr[k] ?? null
      if ((a ?? null) !== (b ?? null)) return true
    }
    const wkKeys = new Set([...Object.keys(savedWk), ...Object.keys(wkDraft)])
    for (const k of wkKeys) {
      const a = num(wkDraft[k] ?? ''), b = savedWk[k] ?? null
      if ((a ?? null) !== (b ?? null)) return true
    }
    const mgKeys = new Set([...Object.keys(savedMg).map(Number), ...Object.keys(mergeDraft).map(Number)])
    for (const w of mgKeys) {
      if ((mergeDraft[w] ?? null) !== (savedMg[w] ?? null)) return true
    }
    return false
  }, [data, defDraft, hourDraft, wkDraft, mergeDraft])

  // targets ของ merge (คลาย chain) เพื่อคำนวณ "วันทำงานรวมของก้อน" ไว้โชว์
  const mergeSources = useMemo(() => {
    const m = {}
    for (const [s, t] of Object.entries(mergeDraft)) (m[t] ||= []).push(Number(s))
    return m
  }, [mergeDraft])

  const wDefaultDays = (key) => { const dv = num(defDraft[key] ?? ''); return dv != null ? dv : FALLBACK }
  const explicitWk = (key, w) => num(wkDraft[`${key}|${w}`] ?? '')
  const rawDays = (key, w) => { const wv = explicitWk(key, w); return wv != null ? wv : wDefaultDays(key) }
  // ผลรวมอัตโนมัติของก้อนที่ยุบ = วันมาตรฐานของสัปดาห์นี้ + วันของสัปดาห์ที่ยุบเข้ามา
  const autoSumDays = (key, w) => {
    const bucket = [w, ...(mergeSources[w] || [])]
    return bucket.reduce((s, b) => s + (b === w ? wDefaultDays(key) : rawDays(key, b)), 0)
  }
  // วันทำงานสุทธิที่ใช้จริง — สัปดาห์ปลายทางถ้าตั้งค่ารายสัปดาห์ = ยอดรวมทั้งก้อน (แก้เอง)
  const resolvedDays = (key, w) => {
    if (mergeDraft[w] != null) return 0
    const hasSrc = (mergeSources[w] || []).length > 0
    const ex = explicitWk(key, w)
    if (hasSrc) return ex != null ? ex : autoSumDays(key, w)
    return ex != null ? ex : wDefaultDays(key)
  }

  // ค่าที่ตรงกันของทุกกลุ่มที่เลือก (ไม่ตรง → MIXED, ไม่มีที่เลือก → '')
  const uniformOf = (getter) => {
    if (!selKeys.length) return ''
    const first = getter(selKeys[0]) ?? ''
    for (const k of selKeys) if ((getter(k) ?? '') !== first) return MIXED
    return first
  }

  async function save() {
    setSaving(true); setErr(''); setMsg('')
    try {
      const defaults = {}
      for (const [k, v] of Object.entries(defDraft)) { const n = num(v); if (n != null) defaults[k] = n }
      const hours = {}
      for (const [k, v] of Object.entries(hourDraft)) { const n = num(v); if (n != null) hours[k] = n }
      const weeks = {}
      for (const [k, v] of Object.entries(wkDraft)) { const n = num(v); if (n != null) weeks[k] = n }
      const merges = {}
      for (const [s, t] of Object.entries(mergeDraft)) if (t != null) merges[String(s)] = Number(t)
      const r = await api.saveWorkday(defaults, weeks, hours, merges)
      setMsg(`บันทึกแล้ว (${r.rows} แถว, ยุบ ${r.merges} สัปดาห์) — สำรองไฟล์เดิมเป็น ${r.backup}`)
      await load()
    } catch (e) { setErr('บันทึกไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }

  async function seed() {
    if (!window.confirm('สร้างค่าเริ่มต้นจาก Working Day ใน MasterMC (ไม่ทับค่าที่ตั้งไว้แล้ว)?')) return
    setSaving(true); setErr(''); setMsg('')
    try {
      const r = await api.seedWorkday()
      setMsg(r.added ? `เพิ่มค่าเริ่มต้น ${r.added} กลุ่มจาก MasterMC` : 'มีค่าครบทุกกลุ่มแล้ว')
      await load()
    } catch (e) { setErr('seed ไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }

  if (loading && !data) return <div className="wdpanel"><div className="hint">กำลังโหลด...</div></div>

  const setMerge = (w, target) => setMergeDraft(p => {
    const n = { ...p }
    const off = target == null || n[w] === target
    if (off) delete n[w]
    else {
      n[w] = target
      // สัปดาห์ที่ถูกยุบไม่มีวันทำงานของตัวเอง → ล้างค่ารายสัปดาห์ที่เคยตั้งของทุกกลุ่มที่ key|w
      setWkDraft(d => {
        const c = { ...d }
        for (const k of Object.keys(c)) if (k.endsWith(`|${w}`)) delete c[k]
        return c
      })
    }
    return n
  })

  // เขียนค่า (ค่ามาตรฐาน / รายสัปดาห์) ลงทุกกลุ่มที่เลือก
  const setDefAll = (val) => setDefDraft(p => { const n = { ...p }; for (const k of selKeys) n[k] = val; return n })
  const setHourAll = (val) => setHourDraft(p => { const n = { ...p }; for (const k of selKeys) n[k] = val; return n })
  const setWkAll = (w, val) => setWkDraft(p => { const n = { ...p }; for (const k of selKeys) n[`${k}|${w}`] = val; return n })

  const one = selKeys.length === 1 ? allGroupsByKey.get(selKeys[0]) : null
  const detailTitle = selKeys.length === 0 ? 'ยังไม่ได้เลือกกลุ่ม'
    : one ? `${one.factory} · ${one.mc_cat} · G${one.guage}`
      : `เลือก ${selKeys.length} กลุ่ม (กรอกครั้งเดียว → ใช้ทุกกลุ่มที่เลือก)`

  const defU = uniformOf(k => defDraft[k] ?? '')
  const hourU = uniformOf(k => hourDraft[k] ?? '')

  return (
    <div className="wdpanel">
      <div className="wdbar">
        <div className="wdtitle">วันทำงานตามกลุ่มเครื่อง (Factory · MC_CAT · Guage)</div>
        <button className="ghost" onClick={seed} disabled={saving}>↻ ตั้งค่าเริ่มต้นจาก MasterMC</button>
      </div>
      {err && <div className="msg err">{err}</div>}
      {msg && <div className="msg ok">{msg}</div>}

      {/* ยุบสัปดาห์ — ระดับปฏิทินรวม ใช้ทั้งระบบ (ไม่ผูกกับกลุ่มที่เลือก) */}
      <div className="wdmergecal">
        <div className="wdmergehead">
          <b>ยุบสัปดาห์ (ปฏิทินรวม — ใช้ทั้งระบบ)</b>
          <span className="hint small">สัปดาห์วันหยุดเยอะ → ยุบเข้าสัปดาห์ข้างเคียง วันทำงานไปรวมที่ปลายทาง (ปรับยอดรวมได้ที่ตารางรายกลุ่ม)</span>
        </div>
        <div className="wdmgrid">
          {WEEKS.map(w => {
            const merged = mergeDraft[w]
            const receives = (mergeSources[w] || []).length > 0
            return (
              <div key={w} className={'wdmcell' + (merged != null ? ' merged' : receives ? ' recv' : '')}>
                <div className="wdmwk">
                  <span>W{w}</span>
                  {merged != null ? <span className="tag o">→W{merged}</span>
                    : receives ? <span className="tag e">◀รวม</span> : null}
                </div>
                <div className="wdmerge">
                  <button disabled={w <= 1} title={`ยุบเข้า W${w - 1}`}
                    className={merged === w - 1 ? 'on' : ''}
                    onClick={() => setMerge(w, w - 1)}>◀</button>
                  <button disabled={w >= 53} title={`ยุบเข้า W${w + 1}`}
                    className={merged === w + 1 ? 'on' : ''}
                    onClick={() => setMerge(w, w + 1)}>▶</button>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      <div className="wdbody">
        {/* tree รายชื่อกลุ่ม (ยุบ-กางได้ + เลือกหลายกลุ่ม) */}
        <div className="wdgroups">
          <input className="wdsearch" placeholder="🔍 ค้นหา เช่น phet double-30 (เว้นวรรค=ทุกคำ)" value={q} onChange={e => setQ(e.target.value)} />
          <div className="wdglist">
            {tree.map(fac => {
              const fid = 'F:' + fac.factory
              const facOpen = !collapsedEff.has(fid)
              const facAll = fac.keys.length > 0 && fac.keys.every(k => selected.has(k))
              const facSome = fac.keys.some(k => selected.has(k))
              return (
                <div key={fid} className="wdfac">
                  <div className="wdhdr" onClick={() => toggleCollapse(fid)}>
                    <span className="wdtree-toggle">{facOpen ? '▾' : '▸'}</span>
                    <Tri checked={facAll} indeterminate={facSome} onChange={() => toggleMany(fac.keys, facAll)} />
                    <span className="wdhdrname">{fac.factory}</span>
                    <span className="wdhdrcount">{fac.keys.length}</span>
                  </div>
                  {facOpen && fac.cats.map(cat => {
                    const cid = `C:${fac.factory}|${cat.mc_cat}`
                    const catOpen = !collapsedEff.has(cid)
                    const catAll = cat.keys.every(k => selected.has(k))
                    const catSome = cat.keys.some(k => selected.has(k))
                    return (
                      <div key={cid} className="wdcat">
                        <div className="wdhdr sub" onClick={() => toggleCollapse(cid)}>
                          <span className="wdtree-toggle">{catOpen ? '▾' : '▸'}</span>
                          <Tri checked={catAll} indeterminate={catSome} onChange={() => toggleMany(cat.keys, catAll)} />
                          <span className="wdhdrname">{cat.mc_cat}</span>
                          <span className="wdhdrcount">{cat.keys.length}</span>
                        </div>
                        {catOpen && cat.items.map(g => {
                          const hasWk = Object.keys(wkDraft).some(k => k.startsWith(g.key + '|') && wkDraft[k] !== '')
                          const on = selected.has(g.key)
                          return (
                            <div key={g.key} className="wditemrow">
                              <Tri checked={on} onChange={() => toggleKey(g.key)} />
                              <button className={'wdgitem' + (on ? ' on' : '')} onClick={() => selectOnly(g.key)}>
                                <span className="wdgname">G{g.guage}</span>
                                <span className="wdgdef">{defDraft[g.key] ?? FALLBACK} วัน{hourDraft[g.key] != null && hourDraft[g.key] !== '' ? ` · ${hourDraft[g.key]} ชม.` : ''}{hasWk ? ' ⚙' : ''}</span>
                              </button>
                            </div>
                          )
                        })}
                      </div>
                    )
                  })}
                </div>
              )
            })}
            {!tree.length && <div className="hint small">ไม่พบกลุ่ม</div>}
          </div>
        </div>

        {/* ค่ามาตรฐาน + รายสัปดาห์ ของกลุ่มที่เลือก (รองรับหลายกลุ่ม) */}
        {selKeys.length > 0 && (
          <div className="wddetail">
            <div className="wdseltitle">{detailTitle}</div>
            <div className="wddefault">
              <span>ค่ามาตรฐาน{selKeys.length > 1 ? 'ของกลุ่มที่เลือก' : 'ของกลุ่มนี้'} (ใช้ทุกสัปดาห์):</span>
              <input type="number" min="0" step="0.5"
                placeholder={defU === MIXED ? 'หลายค่า' : String(FALLBACK)}
                value={defU === MIXED ? '' : defU}
                onWheel={e => e.currentTarget.blur()}
                onChange={e => setDefAll(e.target.value)} />
              <span className="hint small">วัน — เว้นว่าง = {FALLBACK} วัน</span>
              <span className="wddefsep">·</span>
              <span>ชั่วโมง/วัน:</span>
              <input type="number" min="0" max="24" step="0.5"
                placeholder={hourU === MIXED ? 'หลายค่า' : String(FALLBACK_HOURS)}
                value={hourU === MIXED ? '' : hourU}
                onWheel={e => e.currentTarget.blur()}
                onChange={e => setHourAll(e.target.value)} />
              <span className="hint small">เว้นว่าง = {FALLBACK_HOURS} ชม. · Item Special ชนะ</span>
            </div>
            <div className="wdwkgrid">
              {WEEKS.map(w => {
                const merged = mergeDraft[w]
                const receives = (mergeSources[w] || []).length > 0
                const changed = merged != null || selKeys.some(k => wkDraft[`${k}|${w}`] != null && wkDraft[`${k}|${w}`] !== '')
                const wkU = uniformOf(k => wkDraft[`${k}|${w}`] ?? '')
                const autoU = uniformOf(k => autoSumDays(k, w))
                const resU = uniformOf(k => resolvedDays(k, w))
                const autoTxt = autoU === MIXED ? 'หลายค่า' : String(autoU)
                const resTxt = resU === MIXED ? 'ต่างกัน' : String(resU)
                return (
                  <div key={w} className={'wdwk' + (merged != null ? ' merged' : receives ? ' recv' : '') + (changed ? ' chg' : '')}>
                    <div className="wdwkh">
                      <span>W{w}</span>
                      {merged != null ? <span className="tag o">→W{merged}</span>
                        : receives ? <span className="tag e">◀รวม {resTxt}</span> : null}
                    </div>
                    <input type="number" min="0" step="0.5"
                      value={merged != null ? '' : (wkU === MIXED ? '' : wkU)}
                      disabled={merged != null}
                      title={receives ? 'ยอดรวมของสัปดาห์ที่ยุบ — พิมพ์แก้ได้ (เว้นว่าง = ใช้ผลรวมอัตโนมัติ)' : ''}
                      placeholder={merged != null ? '—' : (wkU === MIXED ? 'หลายค่า' : autoTxt)}
                      onWheel={e => e.currentTarget.blur()}
                      onChange={e => setWkAll(w, e.target.value)} />
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </div>

      {dirty && (
        <div className="wdsave">
          <span className="hint small">มีการเปลี่ยนแปลงที่ยังไม่บันทึก</span>
          <button className="ghost" onClick={() => resetDrafts(data)} disabled={saving}>ยกเลิก</button>
          <button className="primary" onClick={save} disabled={saving}>{saving ? 'กำลังบันทึก...' : 'บันทึกวันทำงาน'}</button>
        </div>
      )}
      <div className="hint small wdnote">
        ค่าที่กรอกใช้ตรง ๆ ไม่หักวันหยุดซ้ำ · ยุบสัปดาห์ตั้งที่ "ปฏิทินรวม" ด้านบน (ใช้ทั้งระบบ) · สัปดาห์ปลายทางที่รับรวม พิมพ์เลขในตารางกลุ่ม = แก้ยอดรวมของก้อนนั้นได้ (เว้นว่าง = ผลรวมอัตโนมัติ) · มีผลกับ AVA_MC / Planning / การลากงานในแผน
      </div>
    </div>
  )
}

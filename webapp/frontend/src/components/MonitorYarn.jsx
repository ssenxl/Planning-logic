import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { ConfigItem, toDraft, defDraft, draftText, toPayload, same, stepped } from './PlanConfig.jsx'
import { RULE_KEY, ruleCoversItem, lotMatches, ruleText, rulesOverlap } from '../yarnRules.js'
import YarnLockMonitor from './YarnLockMonitor.jsx'

/* หน้า Monitor Yarn (สิทธิ์ yarn / admin) มี 2 แท็บ:
   ⚙️ ตั้งค่า   = YarnSettings ข้างล่าง
   🔒 ล็อกด้าย = YarnLockMonitor.jsx — ล็อกด้ายให้ Item ผ้า + ดูล็อกทุกตัว (user ขอ 15 ต.ค. 2569 เป็นแท็บในหน้านี้)
   จำแท็บล่าสุดไว้ในเบราว์เซอร์ (sessionStorage — อ่าน/เขียนไม่ได้ก็เริ่มที่ตั้งค่า) */
const VIEW_KEY = 'ymon.view'

export default function MonitorYarn() {
  const [view, setView] = useState(() => {
    try { return sessionStorage.getItem(VIEW_KEY) === 'locks' ? 'locks' : 'settings' } catch { return 'settings' }
  })
  const pick = (v) => {
    setView(v)
    try { sessionStorage.setItem(VIEW_KEY, v) } catch { /* เบราว์เซอร์ไม่ให้เก็บ — ไม่เป็นไร */ }
  }
  return (
    <div className="ymwrap">
      <div className="ymtabs" role="tablist">
        <button role="tab" aria-selected={view === 'settings'} className={view === 'settings' ? 'on' : ''}
          onClick={() => pick('settings')}>⚙️ ตั้งค่า</button>
        <button role="tab" aria-selected={view === 'locks'} className={view === 'locks' ? 'on' : ''}
          onClick={() => pick('locks')}>🔒 ล็อกด้าย</button>
      </div>
      {view === 'locks' ? <YarnLockMonitor canEdit /> : <YarnSettings />}
    </div>
  )
}

/* แท็บตั้งค่า — ตั้งค่าที่งานติดตามด้ายใช้
   • YARN_WEIGHT ขั้นต่ำ — หน้า Dashboard › Stock Yarn ใช้ลงสีแดง/เขียว
   • CUSTOMER ใช้ได้เฉพาะ lot — หน้า Stock Yarn เลือก CUSTOMER แล้วเหลือเฉพาะ lot ที่ใช้ได้ (ตัวเทียบอยู่ที่ yarnRules.js)
   • ซ่อน PO ที่ลงท้ายด้วย -D / -T (เปิด/ปิด + แก้รายการท้ายได้) — หน้า Stock Yarn ไม่แสดงแถวพวกนั้น
   • กติกาวันด้ายเข้า (PO IN ของ booking) เปิด/ปิด + รอกี่สัปดาห์ + ใช้ด้ายในสต๊อกแทนการรอ PO IN
     — ค่าของแผนผลิต (Planning.get_point_date_start_idx / YarnStockCover.py + ตัวเตือน ⚠ ใน KnitPlan.jsx)
     มีผลรอบรันแผนถัดไป (KP_YARN_POIN_* + KP_YARN_STOCK_ON)

   ค่าของหน้านี้อยู่ในทะเบียนเดียวกับหน้า Setting (plan_config.py กลุ่ม "Monitor Yarn") จึงใช้การ์ด ConfigItem
   ตัวเดียวกัน และได้ตรวจค่า/ประวัติ/แจ้งจออื่นมาด้วย — เพิ่มค่าใหม่ = เพิ่ม entry ในกลุ่มนั้น หน้านี้โชว์เอง
   ยกเว้นตารางกติกาลูกค้า ที่มีตัวแก้ของตัวเอง (เลือกชื่อจาก BUYYER + บอกว่าตรง lot ในคลังกี่เบอร์)
   ต่างจากหน้า Setting ตรงที่บันทึกเฉพาะค่าของกลุ่มตัวเอง (api.yarnMonitorSave) ไม่ส่งค่าตั้งค่าอื่นไปด้วย
   หลักการเดียวกับหน้า Setting: ไม่โชว์ชื่อตัวแปร · บอกว่าตอนนี้เป็นค่าอะไร · ยืนยันก่อนบันทึกทุกครั้ง */

const NEW_RULE = { customer: '', item: '', match: 'prefix', lot: '' }
const HIT_SHOW = 6           // ตัวอย่างเบอร์ lot ที่ตรงกติกา โชว์กี่เบอร์ก่อน "…"

// แถวที่ยังไม่กรอกอะไรเลย (แบบมีค่าตั้งต้นอยู่แล้ว ไม่นับ) = ผู้ใช้กดเพิ่มแล้วยังไม่ใช้ → ไม่ส่งไปบันทึก
const blankRule = r => !['customer', 'item', 'lot'].some(k => String(r?.[k] ?? '').trim())

function YarnSettings() {
  const [grp, setGrp] = useState(null)
  const [cat, setCat] = useState({ buyers: [], itemCodes: [], held: [] })
  const [draft, setDraft] = useState({})
  const [openWhy, setOpenWhy] = useState({})     // {key: true} = กาง "ปรับขึ้น-ลงแล้วเกิดอะไร"
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [saving, setSaving] = useState(false)
  const [confirm, setConfirm] = useState(null)   // รายการที่จะเปลี่ยน (เปิดหน้าต่างยืนยัน)

  const items = grp?.items || []

  function apply(g) {
    setGrp(g)
    setDraft(Object.fromEntries((g.items || []).map(it => [it.key, toDraft(it)])))
  }

  // รายชื่อ BUYYER / ITEM / ใครถือ lot ไหน (catalog) ขึ้นกับค่า "ซ่อน PO" ที่บันทึกไว้ (backend ไม่นับแถว PO ที่ซ่อน)
  // → ขอใหม่ทุกครั้งที่ค่าถูกบันทึก ตัวนับใต้กติกาจะได้ตรงกับที่หน้า Stock Yarn แสดง
  async function load(quiet, withCatalog) {
    if (!quiet) setErr('')
    try {
      const g = await api.yarnMonitor(withCatalog)
      if (withCatalog) setCat({ buyers: g.buyers || [], itemCodes: g.item_codes || [], held: g.held || [] })
      apply(g)
    } catch (e) { setErr('เปิดหน้านี้ไม่ได้: ' + e.message) }
  }
  useEffect(() => { load(false, true) }, [])

  // admin อีกคนบันทึกค่าไป (จากหน้านี้หรือหน้า Setting) → โหลดค่าล่าสุด ไม่งั้นจะแก้ทับของเขาโดยไม่รู้ตัว
  // ข่าวจากการบันทึกของเราเองไม่ต้องขึ้นข้อความ (ทับข้อความ "บันทึกแล้ว") แต่ยังโหลดตัวนับใหม่
  useEffect(() => live.subscribe(live.RES.planConfig, (ent) => {
    if (!live.isMe(ent?.by)) {
      setErr('')
      setMsg(`${ent?.by || 'อีกคนหนึ่ง'} เพิ่งบันทึกค่า — กำลังโหลดค่าล่าสุดให้`)
    }
    load(true, true)
  }), [])

  // ค่าที่แก้ค้างอยู่ (ยังไม่กดบันทึก)
  const dirty = useMemo(() => items.filter(it => !same(draft[it.key], toDraft(it))), [items, draft])
  const dirtyKeys = useMemo(() => new Set(dirty.map(it => it.key)), [dirty])

  const set = (key, v) => setDraft(d => ({ ...d, [key]: v }))
  const step = (it, d) => set(it.key, stepped(it, draft[it.key], d))
  const useDefault = it => set(it.key, defDraft(it))

  // ค่าที่จะส่งไปบันทึก — ตารางกติกาตัดแถวที่ยังไม่ได้กรอกทิ้งก่อน
  const payloadOf = it => (it.key === RULE_KEY
    ? toPayload(it, (draft[it.key] || []).filter(r => !blankRule(r)))
    : toPayload(it, draft[it.key]))

  function askSave() {
    setErr(''); setMsg('')
    setConfirm(dirty.map(it => ({
      key: it.key, label: it.label,
      from: it.value_text,
      to: draftText(it, it.key === RULE_KEY ? payloadOf(it) : draft[it.key]),
      // ตารางกติกา: โชว์กติกาทั้งชุดหลังบันทึก ให้อ่านทวนก่อนยืนยัน
      lines: it.key === RULE_KEY ? payloadOf(it).map(r => `${r.customer}: ${ruleText(r)}`) : [],
      warn: it.warn || '',
      locked: it.source === 'env',
    })))
  }

  async function doSave() {
    setSaving(true); setErr(''); setMsg('')
    // ส่งเฉพาะข้อที่แก้ — ข้ออื่นของกลุ่มนี้และค่าตั้งค่าอื่นทั้งหมด backend คงไว้ตามเดิม
    const values = Object.fromEntries(dirty.map(it => [it.key, payloadOf(it)]))
    try {
      const r = await api.yarnMonitorSave(values)
      apply(r.group)
      setConfirm(null)
      // กติกาวันด้ายเข้า (KP_YARN_POIN_*) + ใช้ด้ายในสต๊อก (KP_YARN_STOCK_ON) เป็นของแผนผลิต — engine
      // อ่านตอนเริ่มรัน จึงมีผลรอบรันแผนถัดไป ; ค่าที่เหลือหน้าเว็บใช้ทันที (แผนใช้ตอนคิดยอดด้ายในสต๊อกรอบถัดไป)
      const isPlanKey = c => /^KP_YARN_(POIN_|STOCK_ON$)/.test(String(c?.key || ''))
      const planChg = (r.changed || []).some(isPlanKey)
      const viewChg = (r.changed || []).some(c => !isPlanKey(c))
      setMsg(r.changed?.length
        ? `บันทึกแล้ว ${r.changed.length} รายการ — ` + [
            viewChg && 'หน้า Stock Yarn ใช้ค่าใหม่ทันที',
            planChg && 'กติกาด้ายของแผนผลิตมีผลรอบรันแผนถัดไป',
          ].filter(Boolean).join(' · ')
        : 'ไม่มีอะไรเปลี่ยน')
      // ค่า "ซ่อน PO" อาจเปลี่ยน → ตัวนับใต้กติกาต้องนับใหม่ (ไม่รอข่าวจากสายข่าว เผื่อสายหลุด)
      load(true, true)
    } catch (e) { setErr('บันทึกไม่สำเร็จ: ' + e.message) }
    finally { setSaving(false) }
  }

  if (!grp) return <div className="card"><div className="hint">{err || 'กำลังเปิดหน้า...'}</div></div>

  const locked = items.filter(it => it.source === 'env')

  return (
    <div className="cfgpage ymon">
      {locked.length > 0 && (
        <div className="cfgwarn">
          ⚠️ มี {locked.length} ข้อที่ผู้ดูแลระบบตั้งทับไว้จากเซิร์ฟเวอร์ —
          ข้อเหล่านั้นแก้ที่หน้านี้จะ<b>ยังไม่มีผล</b> จนกว่าจะเอาค่าที่ตั้งทับออกก่อน
        </div>
      )}
      {msg && <div className="cfgmsg ok">{msg}</div>}
      {err && <div className="cfgmsg bad">{err}</div>}

      <div className="card cfggroupcard">
        <div className="cfggrouptop">
          <span className="cfggico">{grp.icon || '🧶'}</span>
          <div>
            <h3>Monitor Yarn</h3>
            <div className="cfggdesc">
              {grp.desc}{grp.desc ? ' · ' : ''}แก้แล้วกดบันทึก หน้า Stock Yarn ของทุกคนจะใช้ค่าใหม่ทันที
            </div>
          </div>
        </div>

        {!items.length && <div className="hint">ยังไม่มีค่าให้ตั้งในหน้านี้</div>}

        <div className="cfgitems">
          {items.map(it => (it.key === RULE_KEY
            ? <RuleEditor key={it.key} it={it} v={draft[it.key] || []} isDirty={dirtyKeys.has(it.key)}
              set={set} cat={cat} />
            : <ConfigItem key={it.key} it={it} v={draft[it.key]} isDirty={dirtyKeys.has(it.key)}
              why={!!openWhy[it.key]}
              toggleWhy={() => setOpenWhy(w => ({ ...w, [it.key]: !w[it.key] }))}
              set={set} step={step} useDefault={useDefault} />))}
        </div>
      </div>

      {/* ---------- แถบบันทึกลอยล่างจอ ---------- */}
      {dirty.length > 0 && (
        <div className="cfgsavebar">
          <span className="cfgsavetxt">✏️ แก้ค้างไว้ <b>{dirty.length}</b> ข้อ ยังไม่ได้บันทึก</span>
          <button onClick={() => load()} disabled={saving}>ยกเลิกที่แก้</button>
          <button className="primary" onClick={askSave} disabled={saving}>บันทึก {dirty.length} ข้อ</button>
        </div>
      )}

      {/* ---------- หน้าต่างยืนยันก่อนบันทึก ---------- */}
      {confirm && (
        <div className="cfgmodalbg" onClick={() => !saving && setConfirm(null)}>
          <div className="cfgmodal" onClick={e => e.stopPropagation()}>
            <h3>ยืนยันการเปลี่ยนแปลง {confirm.length} ข้อ</h3>
            <ul className="cfgdifflist">
              {confirm.map(c => (
                <li key={c.key}>
                  <div className="cfgdiffname">{c.label}</div>
                  <div className="cfgdiffval"><span className="from">{c.from}</span> → <b>{c.to}</b></div>
                  {c.lines.length > 0 && (
                    <ul className="ymdifflines">{c.lines.map((l, i) => <li key={i}>{l}</li>)}</ul>
                  )}
                  {c.warn && <div className="cfgdiffwarn">⚠️ {c.warn}</div>}
                  {c.locked && <div className="cfgdiffwarn">⚠️ ข้อนี้ถูกตั้งทับไว้จากเซิร์ฟเวอร์ — บันทึกได้ แต่ยังไม่มีผลจนกว่าจะเอาค่านั้นออก</div>}
                </li>
              ))}
            </ul>
            <p className="hint">มีผลกับหน้า Stock Yarn <b>ทันที</b> ไม่เกี่ยวกับการจัดแผน</p>
            <div className="actions">
              <button onClick={() => setConfirm(null)} disabled={saving}>ยกเลิก</button>
              <button className="primary" onClick={doSave} disabled={saving}>
                {saving ? 'กำลังบันทึก...' : 'ยืนยันบันทึก'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}


/* ======================= ตารางกติกา CUSTOMER ใช้ได้เฉพาะ lot =======================
   CUSTOMER / ITEM พิมพ์ค้นแล้วเลือกจากรายชื่อในไฟล์ด้าย (ชื่อต้องตรงตัวอักษรกับ BUYYER / ITEM_CODE)
   ใต้แต่ละแถวบอกผลตรวจกับคลังตอนนี้ — พิมพ์ lot ผิดจะเห็นทันทีว่า "ไม่ตรง lot ไหนเลย"
   + ลูกค้าอื่นที่ถือ lot ที่สงวนไว้อยู่แล้ว + กติกาที่ทับกับของลูกค้าอื่น (lot เดียวกันตัดสินไม่ได้ว่าเป็นของใคร) */
function RuleEditor({ it, v, isDirty, set, cat }) {
  const cols = it.columns || []
  const matchCol = cols.find(c => c.key === 'match')
  const buyers = useMemo(() => new Set(cat.buyers), [cat.buyers])
  const itemSet = useMemo(() => new Set(cat.itemCodes.map(x => x.toUpperCase())), [cat.itemCodes])

  const edit = (i, k, val) => set(it.key, v.map((r, j) => (j === i ? { ...r, [k]: val } : r)))

  // ผลตรวจของแถว: เบอร์ lot ในคลังที่ตรงกติกา (ไม่ซ้ำ) + ลูกค้าอื่นที่ถือ lot พวกนั้นอยู่ + คำเตือน
  function check(r, idx) {
    const notes = []
    const cust = String(r.customer ?? '').trim()
    const item = String(r.item ?? '').trim()
    if (cust && cat.buyers.length && !buyers.has(cust)) notes.push(`ไม่มี CUSTOMER “${cust}” ในคลังตอนนี้ (ต้องตรงกับ BUYYER)`)
    if (item && cat.itemCodes.length && !itemSet.has(item.toUpperCase())) notes.push(`ไม่มี ITEM “${item}” ในคลังตอนนี้`)
    // กติกาของลูกค้าอื่นที่ทับกัน — บอกเลขแถวให้ไปแก้ได้ถูกที่
    v.forEach((o, j) => {
      if (j !== idx && rulesOverlap(r, o)) {
        notes.push(`ทับกับกติกาแถวที่ ${j + 1} ของ ${String(o.customer).trim()} — lot ที่ตรงทั้งสองข้อตัดสินไม่ได้ว่าเป็นของใคร`)
      }
    })
    if (!String(r.lot ?? '').trim()) return { notes, lots: null, others: [] }
    const hit = cat.held.filter(([i, l]) => ruleCoversItem(r, i) && lotMatches(r, l))
    const lots = [...new Set(hit.map(h => h[1]))]
    // ลูกค้าอื่นที่ถือ lot ที่สงวนให้ลูกค้าในแถวนี้อยู่แล้ว (แถวที่ BUYYER ว่าง = ยังไม่มีเจ้าของ ไม่นับ)
    const by = new Map()
    for (const [, , b, n] of hit) if (b && b !== cust) by.set(b, (by.get(b) || 0) + n)
    const others = [...by].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    return { notes, lots, others }
  }

  return (
    <div className={'cfgitem wide ymrules' + (isDirty ? ' dirty' : '')}>
      <div className="cfgitemmain">
        <div className="cfgitemlabel">
          {it.label}
          {it.source === 'env' && <span className="cfgtag env">ตั้งจากเซิร์ฟเวอร์</span>}
          {!isDirty && it.source === 'file' && <span className="cfgtag set">ปรับไว้เอง</span>}
          {isDirty && <span className="cfgtag dirty">ยังไม่ได้บันทึก</span>}
          <span className="cfgcount">{v.length} แถว</span>
        </div>
        {it.help && <div className="cfgitemhelp">{it.help}</div>}
        {it.example && <div className="cfgmeaning">{it.example}</div>}
      </div>

      <div className="cfgitemwide">
        <datalist id="ym-buyers">{cat.buyers.map(b => <option key={b} value={b} />)}</datalist>
        <datalist id="ym-items">{cat.itemCodes.map(x => <option key={x} value={x} />)}</datalist>
        <div className="cfgtable">
          <table>
            <thead>
              <tr><th>CUSTOMER</th><th>ITEM</th><th>แบบ</th><th>LOT</th><th /></tr>
            </thead>
            <tbody>
              {v.length === 0 && (
                <tr><td colSpan={5} className="cfgtblempty">ยังไม่มีกติกา — ทุก CUSTOMER ใช้ได้ทุก lot</td></tr>
              )}
              {v.map((r, i) => {
                const c = check(r, i)
                return (
                  <React.Fragment key={i}>
                    <tr>
                      <td className="ymcust">
                        <input list="ym-buyers" value={r.customer ?? ''} placeholder="พิมพ์ค้นชื่อ BUYYER"
                          onChange={e => edit(i, 'customer', e.target.value)} />
                      </td>
                      <td className="ymitem">
                        <input list="ym-items" value={r.item ?? ''} placeholder="ทุก ITEM"
                          onChange={e => edit(i, 'item', e.target.value)} />
                      </td>
                      <td className="ymmatch">
                        <select value={r.match ?? ''} onChange={e => edit(i, 'match', e.target.value)}>
                          <option value="">— เลือก —</option>
                          {(matchCol?.choices || []).map(ch => <option key={ch.value} value={ch.value}>{ch.label}</option>)}
                        </select>
                      </td>
                      <td>
                        <input value={r.lot ?? ''} placeholder="เช่น NEL"
                          onChange={e => edit(i, 'lot', e.target.value)} />
                      </td>
                      <td>
                        <button className="cfgrowdel" title="ลบกติกานี้"
                          onClick={() => set(it.key, v.filter((_, j) => j !== i))}>×</button>
                      </td>
                    </tr>
                    {(c.lots !== null || c.notes.length > 0 || c.others.length > 0) && (
                      <tr>
                        <td colSpan={5}>
                          {c.lots !== null && (c.lots.length
                            ? <div className="ymrulehit ok">
                                ตรงกับ lot ในคลัง <b>{c.lots.length.toLocaleString()} เบอร์</b>:{' '}
                                {c.lots.slice(0, HIT_SHOW).join(', ')}{c.lots.length > HIT_SHOW ? ' …' : ''}
                              </div>
                            : <div className="ymrulehit bad">⚠️ <b>ยังไม่ตรงกับ lot ไหนในคลังตอนนี้</b> — ตรวจตัวสะกด</div>)}
                          {c.others.length > 0 && (
                            <div className="ymrulehit bad">
                              ⚠️ ลูกค้าอื่นถือ lot เหล่านี้อยู่ <b>{c.others.reduce((s, o) => s + o[1], 0).toLocaleString()} แถว</b>:{' '}
                              {c.others.map(([b, n]) => `${b} (${n.toLocaleString()})`).join(', ')}
                            </div>
                          )}
                          {c.notes.map(n => <div key={n} className="ymrulehit bad">⚠️ {n}</div>)}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
          <button className="cfgaddrow" onClick={() => set(it.key, [...v, { ...NEW_RULE }])}>+ เพิ่มกติกา</button>
        </div>
      </div>
    </div>
  )
}

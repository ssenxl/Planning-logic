import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'

// ตัวช่วย "เพิ่มเครื่องใหม่" — ถามทีละขั้นแทนการให้กรอกแถวเปล่าในตาราง 14 ช่อง
//
// ทำไมต้องเป็นตัวช่วย ไม่ใช่ปุ่ม "+ เพิ่มแถว": การเพิ่มเครื่องต้องครบ 3 ที่ถึงจะเข้าแผนได้จริง
//   1. ทะเบียนเครื่อง (Master MC)        ← ปุ่มเพิ่มแถวทำได้แค่อันนี้
//   2. วันทำงานของกลุ่ม (แผงวันทำงาน)    ← ไม่ตั้ง = ระบบใช้ 6 วันเงียบ ๆ
//   3. กำลังผลิตในฐานข้อมูล (ERP)        ← แก้บนเว็บไม่ได้ ต้องรู้ล่วงหน้าว่ายังไม่มี
// และถ้าเครื่องเข้ากลางปี ต้องไปเปิดจำนวนที่ "จำนวนเครื่องเฉพาะบางสัปดาห์" ด้วย
// ไม่งั้นแผนจะคิดว่ามีเครื่องครบตั้งแต่สัปดาห์แรก
//
// ค่าที่เดาให้ (กลุ่มที่ใช้แทนกัน / ชนิดเครื่อง) มาจากแถวที่มีอยู่จริงในทะเบียน ไม่ได้แต่งขึ้นเอง
// ผู้ใช้แก้ทับได้ทุกช่อง

const ICON = { ok: '✅', warn: '⚠️', todo: '➕', info: 'ℹ️', unknown: '❓' }

const NEW = '\u0000new'   // ค่าพิเศษของ dropdown = "ขอพิมพ์ชื่อใหม่เอง"

// ช่องเลือกค่าที่ "มีอยู่จริง" เป็นหลัก — จะตั้งชื่อใหม่ต้องเลือก "＋ สร้างใหม่" ก่อน
// เหตุผล: พิมพ์ชื่อกลุ่มเองแล้วสะกดต่างนิดเดียว (SINGEL/SINGLE) ค่าที่ตั้งในแผงวันทำงานจะหลุด
// ทั้งที่หน้าจอดูเหมือนถูก — เคยเกิดจริงกับ 12 กลุ่มบนเซิร์ฟเวอร์
// ใช้ร่วมกับฟอร์มเพิ่มเครื่องของผู้รับจ้าง (S9Hub) — className ให้เข้ากับแถบของหน้านั้น,
//   numeric = แป้นตัวเลข (ไม่กรองตัวอักษรทิ้ง: พิมพ์ 30G32" แล้วกลายเป็น 3032 เงียบ ๆ อันตรายกว่า → ให้ผู้เรียกตรวจเอง)
export function PickOrNew({ label, value, options, onChange, placeholder, hint, warn, className = 'mcwf', numeric, bad }) {
  const known = options.includes(value)
  const [free, setFree] = useState(false)
  const showFree = free || (!!value && !known)
  return (
    <label className={className}>
      <span>{label}</span>
      {showFree ? (
        <div className="mcwnew">
          <input autoFocus value={value} placeholder={placeholder} className={bad ? 'bad' : undefined}
            inputMode={numeric ? 'numeric' : undefined} onChange={e => onChange(e.target.value)} />
          <button type="button" onClick={() => { setFree(false); onChange('') }}
            title="กลับไปเลือกจากรายการ">↩</button>
        </div>
      ) : (
        <select value={known ? value : ''} onChange={e => {
          if (e.target.value === NEW) { setFree(true); onChange('') } else onChange(e.target.value)
        }}>
          <option value="">— เลือก —</option>
          {options.map(o => <option key={o} value={o}>{o}</option>)}
          <option value={NEW}>＋ สร้างใหม่ (พิมพ์เอง)</option>
        </select>
      )}
      {showFree && warn && <small className="mcwwarn">⚠️ {warn}</small>}
      {hint && <small>{hint}</small>}
    </label>
  )
}

export default function McNewWizard({ rows, onClose, onDone }) {
  // rows = แถวในทะเบียนตอนนี้ (มาจากตารางที่เปิดอยู่) — ใช้ทำรายการให้เลือก ไม่ต้องพิมพ์เอง
  const [f, setF] = useState({
    factory: '', type: '', mc_cat: '', mc: '', guage: '', group: '',
    total_mc: '', when: 'now', start_week: '', start_year: '',
    work_days: '', work_hours: '', remark: '',
  })
  const [chk, setChk] = useState(null)
  const [checking, setChecking] = useState(false)
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')
  const [done, setDone] = useState(null)

  const set = (k, v) => setF(s => ({ ...s, [k]: v }))

  // ตัวเลือกจากทะเบียนจริง (คำนวณจากแถวที่ตารางถืออยู่ — ไม่ต้องรอ API)
  const local = useMemo(() => {
    const uniq = (xs) => [...new Set(xs.map(x => String(x || '').trim()).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, 'th', { numeric: true }))
    return {
      factories: uniq(rows.map(r => r.factory)),
      types: uniq(rows.map(r => r.type)),
      cats: uniq(rows.map(r => r.mc_cat)),
      mcs: uniq(rows.map(r => r.mc)),
      gauges: uniq(rows.map(r => r.guage)),
      groups: uniq(rows.map(r => r.group)),
    }
  }, [rows])

  // ตัวเลือกจาก server (รู้ด้วยว่าแผงวันทำงานมีชื่อกลุ่มอะไรบ้าง) — ยังไม่มาก็ใช้ของ local ไปก่อน
  const [srv, setSrv] = useState(null)
  useEffect(() => { api.mcNewOptions().then(setSrv).catch(() => setSrv(null)) }, [])
  const opts = srv || local

  // กลุ่มที่เลือกอยู่ มีในแผงวันทำงานไหม — ไม่มี = วันทำงานที่ตั้งไว้จะไม่ถูกใช้
  const catInPanel = !srv?.panel_cats?.length || !f.mc_cat
    || srv.panel_cats.some(c => c.trim().toUpperCase() === f.mc_cat.trim().toUpperCase())

  // เกจ/กอง ที่เคยใช้กับกลุ่มที่เลือก — แคบตัวเลือกให้ตรงบริบท ลดโอกาสกรอกมั่ว
  const gaugeOpts = (srv?.gauges_by_cat?.[f.mc_cat]?.length ? srv.gauges_by_cat[f.mc_cat] : opts.gauges) || []
  const groupOpts = (srv?.groups_by_cat_gauge?.[`${f.mc_cat}|${f.guage}`]?.length
    ? srv.groups_by_cat_gauge[`${f.mc_cat}|${f.guage}`] : opts.groups) || []

  // เลือก "เข้ามากลางปี" แล้วไม่กรอกสัปดาห์ = ระบบจะนับเครื่องตั้งแต่สัปดาห์แรกของแผนเงียบ ๆ
  // (เพราะฝั่ง server ถือว่าไม่ได้ระบุสัปดาห์เริ่ม) → ต้องบังคับกรอกก่อนกดเพิ่ม
  const needWeek = f.when === 'week' && !String(f.start_week).trim()
  // กองที่ยังไม่เคยตั้งค่า ต้องกรอกวันทำงาน + ชั่วโมงให้ครบก่อน (ไม่งั้นได้กองที่ระบบเดาค่าเอง)
  const wdCheck = chk?.checks?.find(c => c.id === 'workday')
  const needSetup = !!chk && wdCheck?.status !== 'ok'
  const missSetup = needSetup
    && (!String(f.work_days).trim() || !String(f.work_hours).trim())
  const ready = f.factory && f.type && f.mc_cat && f.mc && f.guage
    && String(f.total_mc).trim() && !needWeek && !missSetup

  // ถามระบบว่ากลุ่มนี้พร้อมแค่ไหน — ทุกครั้งที่คีย์หลักครบ/เปลี่ยน
  useEffect(() => {
    if (!f.factory || !f.mc_cat || !f.mc || !f.guage) { setChk(null); return }
    let dead = false
    setChecking(true)
    api.mcNewCheck({ factory: f.factory, mc_cat: f.mc_cat, mc: f.mc, guage: f.guage })
      .then(r => {
        if (dead) return
        setChk(r)
        // เติมค่าที่ระบบเดาให้ เฉพาะช่องที่ผู้ใช้ยังไม่ได้กรอกเอง
        setF(s => ({
          ...s,
          group: s.group || r.group_suggest || '',
          type: s.type || r.type_suggest || '',
        }))
      })
      .catch(e => { if (!dead) { setChk(null); setErr('ตรวจสภาพเครื่องไม่ได้: ' + e.message) } })
      .finally(() => { if (!dead) setChecking(false) })
    return () => { dead = true }
  }, [f.factory, f.mc_cat, f.mc, f.guage])

  async function save() {
    setSaving(true); setErr('')
    try {
      const payload = {
        factory: f.factory, type: f.type, mc_cat: f.mc_cat, mc: f.mc, guage: f.guage,
        group: f.group, total_mc: f.total_mc, remark: f.remark,
        work_days: f.work_days === '' ? null : f.work_days,
        work_hours: f.work_hours === '' ? null : f.work_hours,
      }
      if (f.when === 'week') {
        payload.start_week = f.start_week
        payload.start_year = f.start_year
      }
      const r = await api.mcNewCreate(payload)
      setDone(r)
      onDone?.()
    } catch (e) { setErr(e.message) }
    finally { setSaving(false) }
  }

  const exists = chk?.exists

  return (
    <div className="modal-backdrop" onClick={() => !saving && onClose()}>
      <div className="mcwiz" onClick={e => e.stopPropagation()}>
        <h3>➕ เพิ่มเครื่องใหม่</h3>

        {done ? (
          /* ---------- เพิ่มเสร็จแล้ว: สรุปว่าทำอะไรไปบ้าง + เหลืออะไรต้องทำต่อ ---------- */
          <div className="mcwdone">
            <div className="mcwok">✅ เพิ่ม {f.mc_cat} · {f.mc} Guage {f.guage} เรียบร้อย</div>
            <ul className="mcwlist">
              {(done.done || []).map((d, i) => <li key={i}>✓ {d}</li>)}
              {(done.warn || []).map((w, i) => <li key={'w' + i} className="bad">⚠️ {w}</li>)}
            </ul>
            <div className="mcwafter">สถานะตอนนี้</div>
            <ul className="mcwchecks">
              {(done.check?.checks || []).map(c => (
                <li key={c.id} className={c.status}>
                  <b>{ICON[c.status] || '•'} {c.label}</b>
                  <div>{c.detail}</div>
                </li>
              ))}
            </ul>
            <div className="dlgbtns">
              <button className="primary" onClick={onClose}>เสร็จแล้ว</button>
            </div>
          </div>
        ) : (
          <>
            <div className="mcwgrid">
              <PickOrNew label="โรงงาน" value={f.factory} options={opts.factories}
                onChange={v => set('factory', v)} placeholder="ชื่อโรงงานใหม่"
                warn="โรงงานใหม่ต้องตั้งวันทำงานให้ทุกกลุ่มของโรงนี้เอง" />
              <PickOrNew label="ชนิดเครื่อง" value={f.type} options={opts.types}
                onChange={v => set('type', v)} placeholder="เช่น DOUBLE" />
              <PickOrNew label="กลุ่มเครื่อง (MC_CAT)" value={f.mc_cat}
                options={(srv?.cats_by_factory?.[f.factory]?.length
                  ? srv.cats_by_factory[f.factory] : opts.cats) || []}
                onChange={v => set('mc_cat', v)} placeholder="ชื่อกลุ่มใหม่"
                warn="ชื่อกลุ่มใหม่ = แผงวันทำงานยังไม่มีค่าของกลุ่มนี้ ระบบจะใช้ 6 วัน/สัปดาห์จนกว่าจะตั้ง" />
              <PickOrNew label="ชื่อเครื่อง (MC)" value={f.mc} options={opts.mcs}
                onChange={v => set('mc', v)} placeholder="ชื่อเครื่องใหม่" />
              <PickOrNew label="Guage" value={f.guage} options={gaugeOpts}
                onChange={v => set('guage', v)} placeholder="เช่น 22"
                hint={f.mc_cat && srv?.gauges_by_cat?.[f.mc_cat]?.length
                  ? `Guage ที่กลุ่ม ${f.mc_cat} ใช้อยู่: ${srv.gauges_by_cat[f.mc_cat].join(', ')}` : ''} />
              <label className="mcwf">
                <span>จำนวนเครื่อง</span>
                <input inputMode="numeric" value={f.total_mc}
                  onChange={e => set('total_mc', e.target.value.replace(/[^0-9]/g, ''))}
                  placeholder="0" />
              </label>
              <div className="mcwf wide">
                <PickOrNew label="Group ที่สามารถแทนกันได้ เช่น IBP 22G"
                  value={f.group} options={groupOpts} onChange={v => set('group', v)}
                  placeholder="เช่น IBP 22G"
                  hint={chk?.pool_mates?.length
                    ? `เครื่องที่อยู่กลุ่ม+Guage เดียวกันอยู่แล้ว: ${chk.pool_mates.join(', ')}` : ''} />
              </div>
            </div>

            {needSetup && (
              <div className="mcwexists">
                Cat <b>{f.mc_cat} Guage {f.guage}</b> ของโรง <b>{f.factory}</b> ยังไม่เคยตั้งวันทำงาน —
                ต้องกรอก <b>วันทำงาน</b> และ <b>ชั่วโมงทำงาน</b> ด้านล่างให้ครบก่อน
                ไม่งั้นระบบจะเดาเป็น 6 วัน 24 ชม. โดยไม่มีใครรู้
              </div>
            )}

            {/* ---- เครื่องมีตั้งแต่เมื่อไร ---- */}
            <div className="mcwwhen">
              <div className="mcwqtitle">เครื่องชุดนี้ใช้งานได้ตั้งแต่เมื่อไร?</div>
              <label className={f.when === 'now' ? 'on' : ''}>
                <input type="radio" checked={f.when === 'now'} onChange={() => set('when', 'now')} />
                <b>มีอยู่แล้ว</b> — นับเครื่องได้ทุกสัปดาห์ในแผน
              </label>
              <label className={f.when === 'week' ? 'on' : ''}>
                <input type="radio" checked={f.when === 'week'} onChange={() => set('when', 'week')} />
                <b>เข้ามากลางปี</b> — เริ่มนับตั้งแต่สัปดาห์ที่ระบุ
              </label>
              {f.when === 'week' && (
                <div className="mcwweek">
                  <label>สัปดาห์ที่เริ่มมี
                    <input inputMode="numeric" value={f.start_week}
                      onChange={e => set('start_week', e.target.value.replace(/[^0-9]/g, ''))}
                      placeholder="เช่น 45" />
                  </label>
                  <label>ปี
                    <input inputMode="numeric" value={f.start_year}
                      onChange={e => set('start_year', e.target.value.replace(/[^0-9]/g, ''))}
                      placeholder="เช่น 2026" />
                  </label>
                  <small className={needWeek ? 'needwk' : ''}>
                    {needWeek
                      ? 'ต้องกรอกสัปดาห์ที่เริ่มมีก่อน ไม่งั้นระบบจะนับเครื่องตั้งแต่สัปดาห์แรกของแผน'
                      : 'ก่อนสัปดาห์นี้ระบบจะถือว่ายังไม่มีเครื่องชุดนี้'}
                  </small>
                </div>
              )}
            </div>

            <div className="mcwgrid">
              <label className="mcwf">
                <span>วันทำงานต่อสัปดาห์{needSetup ? ' (ต้องกรอก)' : ' (เว้นว่าง = ใช้ค่าเดิมของ Cat)'}</span>
                <input inputMode="decimal" value={f.work_days}
                  className={needSetup && !String(f.work_days).trim() ? 'bad' : ''}
                  onChange={e => set('work_days', e.target.value.replace(/[^0-9.]/g, ''))}
                  placeholder={needSetup ? 'ต้องกรอก 0-7' : 'เช่น 6'} />
              </label>
              <label className="mcwf">
                <span>ชั่วโมงทำงานต่อวัน{needSetup ? ' (ต้องกรอก)' : ' (เว้นว่าง = ใช้ค่าเดิมของ Cat)'}</span>
                <input inputMode="decimal" value={f.work_hours}
                  className={needSetup && !String(f.work_hours).trim() ? 'bad' : ''}
                  onChange={e => set('work_hours', e.target.value.replace(/[^0-9.]/g, ''))}
                  placeholder={needSetup ? 'ต้องกรอก 0-24' : 'เช่น 20'} />
              </label>
              <label className="mcwf">
                <span>หมายเหตุ</span>
                <input value={f.remark} onChange={e => set('remark', e.target.value)}
                  placeholder="เช่น เครื่องใหม่เข้าล็อต 2" />
              </label>
            </div>

            {/* ---- ไฟตรวจความพร้อม ---- */}
            <div className="mcwcheckbox">
              <div className="mcwqtitle">
                ความพร้อมของเครื่องกลุ่มนี้ {checking && <small>กำลังตรวจ...</small>}
              </div>
              {!chk && !checking && <div className="hint small">กรอกโรงงาน กลุ่มเครื่อง ชื่อเครื่อง และ Guage แล้วระบบจะตรวจให้</div>}
              {chk && (
                <ul className="mcwchecks">
                  {chk.checks.map(c => (
                    <li key={c.id} className={c.status}>
                      <b>{ICON[c.status] || '•'} {c.label}</b>
                      <div>{c.detail}</div>
                    </li>
                  ))}
                </ul>
              )}
              {exists && (
                <div className="mcwexists">
                  เครื่องนี้มีอยู่ในทะเบียนแล้ว — ถ้าจะ<b>เพิ่มจำนวน</b> ให้ปิดหน้าต่างนี้
                  แล้วแก้ช่อง "จำนวนเครื่อง" ของแถวเดิมแทน
                </div>
              )}
            </div>

            {err && <div className="msg err">{err}</div>}

            <div className="dlgbtns">
              <button onClick={onClose} disabled={saving}>ยกเลิก</button>
              <button className="primary" disabled={!ready || saving || exists} onClick={save}>
                {saving ? 'กำลังเพิ่ม...' : 'เพิ่มเครื่องนี้'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

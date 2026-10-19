import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api.js'
import * as live from '../live.js'
import { weekKey, wkLabel } from '../weekKeys.js'

/* หน้า Monitor Yarn › แท็บล็อกด้าย — ฝั่งด้ายล็อกด้ายไว้ให้ Item ผ้า + ดูล็อกทุกตัว (Item / SC) ที่เดียว
   (user ขอ 15 ต.ค. 2569: "ต้องมีอีกหน้านึงสำหรับให้ Yarn ไว้ล็อกเส้นด้ายตัวนั้นๆ ให้สำหรับ Item นั้นๆ จำนวนเท่าไหร่
    เป็นหน้า Monitor ตัวนั้นโดยเฉพาะ โดยแผนทอก็ต้องดูจากตัวนี้ไม่สามารถเอาด้ายตัวที่ล็อกไปให้ตัวอื่นได้")

   ล็อก Item (yarn_item_lock.py): ด้าย + lot (หรือทุก lot = lot ที่ด้ายเข้าก่อนก่อน) + Item ผ้า + kg ด้าย
     ทุกใบของ Item นั้นใช้ร่วมกัน ผ้าอื่นใช้ไม่ได้ · ออกใบ KP แล้วยอดลดเอง · ไม่ปลดเองตาม booking
   ล็อก SC (yarn_lock.py): ล็อกที่หน้าแผนด้าย — หน้านี้ดู/ปลดได้
   ตัวเลขทั้งหมดคิดที่ backend — "กันได้จริง" = ยอดที่สต๊อกกันให้ได้ตอนนี้ · "แผนใช้" = แผนล่าสุดใช้ด้ายที่ล็อกสัปดาห์ไหนเท่าไหร่

   หน้าตา (user 16 ต.ค. 2569 "ไม่หรูดูไม่ดีเลย" + ส่งรูปหน้าต่างปรับ OT ของอีกระบบมาเทียบ):
   หน้าหลักมีแค่สรุป + ตารางล็อก ; ฟอร์มล็อกเป็นหน้าต่างเด้ง — ซ้ายกรอก (ด้าย → lot เป็นการ์ด → Item เป็นการ์ด → kg)
   ขวาโชว์ผลก่อนบันทึก (เหลือให้งานอื่นเท่าไหร่ · ครอบคลุมยอดที่ใบค้างต้องใช้กี่ %) · ปุ่มล็อกกดไม่ได้ = แถบล่างบอกว่ายังขาดอะไร
   ไม่ใช้อีโมจิ (แบบเดียวกับหน้าแผนด้าย) */

const kg1 = (n) => (n == null ? '-' : Number(n).toLocaleString(undefined, { maximumFractionDigits: 1 }))
const STATE = {
  active: ['กันอยู่', 'on'],
  done: ['ออก KP ครบแล้ว', 'done'],
  closed: ['booking ปิดแล้ว', 'done'],
}
const STEP = 50        // ปุ่ม − / + ข้างช่อง kg ด้าย ขยับทีละกี่ kg
const LIST_MAX = 60    // รายชื่อด้าย / การ์ด Item ในหน้าต่างล็อก โชว์กี่ตัวก่อนให้พิมพ์ค้นให้แคบลง

export default function YarnLockMonitor({ canEdit = true }) {
  const [d, setD] = useState(null)
  const [err, setErr] = useState('')
  const [msg, setMsg] = useState('')
  const [warn, setWarn] = useState([])
  const [loading, setLoading] = useState(false)
  const [q, setQ] = useState('')
  const [kind, setKind] = useState('all')
  const [showDone, setShowDone] = useState(false)
  const [showLog, setShowLog] = useState(false)
  const [busy, setBusy] = useState('')
  const [adding, setAdding] = useState(false)

  async function load() {
    setLoading(true)
    try { setD(await api.yarnItemLocks()); setErr('') }
    catch (e) { setErr('โหลดรายการล็อกไม่ได้: ' + e.message) }
    finally { setLoading(false) }
  }
  // มีคนล็อก/ปลด (ทั้ง 2 แบบ), รันแผนใหม่, แผนถูกแก้, แก้ค่าหน้า Monitor Yarn → โหลดใหม่ (หน่วงนิดนึง กันโหลดรัว)
  const timer = useRef(null)
  useEffect(() => {
    load()
    const soon = () => { clearTimeout(timer.current); timer.current = setTimeout(load, 1200) }
    const offs = [live.subscribe(live.RES.yarnItemLock, soon), live.subscribe(live.RES.yarnLock, soon),
      live.subscribe(live.RES.plan('PLAN'), soon), live.subscribe(live.RES.run, soon),
      live.subscribe(live.RES.planConfig, soon)]
    return () => { clearTimeout(timer.current); offs.forEach(f => f()) }
  }, [])

  const wlabels = useMemo(() => (d?.weeks || []).map(w => wkLabel(weekKey(w.w, w.y))), [d])
  const all = useMemo(() => [...(d?.item || []), ...(d?.sc || [])]
    .sort((a, b) => String(b.at || '').localeCompare(String(a.at || ''))), [d])
  const kw = q.trim().toLowerCase()
  const rows = all.filter(L => (kind === 'all' || L.kind === kind)
    && (showDone || L.state === 'active')
    && (!kw || [L.yarn, L.lot, L.fabric, L.sc, L.color, L.by, L.note].some(v => String(v || '').toLowerCase().includes(kw))))
  const nHidden = all.filter(L => L.state !== 'active').length
  const nShort = all.filter(L => L.state === 'active' && L.eff + 0.5 < L.rem).length

  async function unlock(L) {
    const what = L.kind === 'item'
      ? `ด้าย ${L.yarn} ${L.lot ? 'lot ' + L.lot : 'ทุก lot'} ที่ล็อกให้ Item ${L.fabric}`
      : `ด้าย ${L.yarn} lot ${L.lot} ที่ล็อกให้ ${L.sc} ${L.fabric}`
    if (!window.confirm(`ปลดล็อก${what}?\nด้ายส่วนนี้จะกลับไปให้ทุกงานใช้ได้ — แผนเห็นผลรอบรันถัดไป`)) return
    setBusy(L.id); setMsg(''); setWarn([]); setErr('')
    try {
      await (L.kind === 'item' ? api.yarnItemLockRemove(L.id) : api.yarnLockRemove(L.id))
      setMsg(`ปลดล็อก${what}แล้ว`)
      load()
    } catch (e) { setErr('ปลดล็อกไม่ได้: ' + e.message) }
    finally { setBusy('') }
  }

  const openAdd = () => { setMsg(''); setWarn([]); setErr(''); setAdding(true) }
  const st = d?.stats || {}
  return (
    <div className="ylm">
      <div className="card ylm-head">
        <div className="ylm-top">
          <div>
            <div className="ylm-eyebrow">Monitor Yarn</div>
            <h3>ล็อกด้าย</h3>
            <div className="ylm-desc">
              ล็อกด้ายไว้ให้ Item ผ้า — ทุกใบของ Item นั้นใช้ร่วมกัน <b>Item อื่นใช้ไม่ได้</b> (แผนทอยึดตามนี้ตอนแบ่งด้าย)
              · ออกใบ KP ของ Item นั้นแล้วยอดล็อกลดเอง · ล็อกให้ใบ SC ทำที่หน้าแผนด้าย
            </div>
          </div>
          <div className="ylm-topbtns">
            <button className="ylm-btn" onClick={load} disabled={loading}>{loading ? 'กำลังโหลด…' : '↻ โหลดใหม่'}</button>
            {canEdit && <button className="ylm-btn pri" onClick={openAdd} disabled={!d}>+ ล็อกด้าย</button>}
          </div>
        </div>
        <div className="ylm-tiles">
          <div className="ylm-tile item">
            <span>ล็อก Item ที่กันอยู่</span><b>{st.item_n ?? '-'}</b><small>{kg1(st.item_kg)} kg ด้าย</small>
          </div>
          <div className="ylm-tile sc">
            <span>ล็อก SC ที่กันอยู่</span><b>{st.sc_n ?? '-'}</b><small>{kg1(st.sc_kg)} kg ด้าย</small>
          </div>
          <div className={'ylm-tile' + (nShort ? ' warn' : ' fine')}>
            <span>กันได้ไม่ครบ</span><b>{d ? nShort : '-'}</b>
            <small>{!d ? '' : nShort ? 'สต๊อกมีให้กันน้อยกว่ายอดล็อก — ดูคอลัมน์กันได้จริง' : 'ทุกล็อกกันได้ครบ'}</small>
          </div>
        </div>
        <div className="ylm-meta">
          {d?.plan?.name ? `แผน ${d.plan.name}` : ''}{d?.stock_at ? ` · สต๊อกด้าย ${String(d.stock_at).replace('T', ' ')}` : ''}
        </div>
        {d?.note && <div className="ylm-note">{d.note}</div>}
      </div>

      {msg && <div className="ylm-msg ok">{msg}</div>}
      {warn.map(w => <div key={w} className="ylm-msg warn">{w}</div>)}
      {err && <div className="ylm-msg bad">{err}</div>}

      <div className="card ylm-listcard">
        <div className="ylm-listhead">
          <h4>รายการล็อก</h4>
          {all.length > 0 && <span>{rows.length.toLocaleString()} จาก {all.length.toLocaleString()} รายการ</span>}
        </div>
        {all.length > 0 && (
          <div className="ylm-tools">
            <input className="ylm-search" value={q} onChange={e => setQ(e.target.value)}
              placeholder="ค้นหาด้าย / lot / Item / SC / คนล็อก…" />
            <div className="ylm-seg">
              {[['all', 'ทั้งหมด'], ['item', 'ล็อก Item'], ['sc', 'ล็อก SC']].map(([k, t]) => (
                <button key={k} className={kind === k ? 'on' : ''} onClick={() => setKind(k)}>{t}</button>
              ))}
            </div>
            <label className="ylm-chk">
              <input type="checkbox" checked={showDone} onChange={e => setShowDone(e.target.checked)} />
              แสดงล็อกที่หมดหน้าที่แล้ว ({nHidden})
            </label>
          </div>
        )}
        {!d && !err ? <div className="ylm-empty">กำลังโหลด…</div>
          : all.length === 0 ? (
            <div className="ylm-emptybox">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round"
                strokeLinejoin="round" aria-hidden="true">
                <rect x="4.5" y="10.5" width="15" height="10" rx="2.5" />
                <path d="M8 10.5V7.5a4 4 0 0 1 8 0v3" />
                <path d="M12 14.5v2.5" />
              </svg>
              <b>ยังไม่มีล็อกด้าย</b>
              <span>ล็อกด้ายไว้ให้ Item ผ้า เพื่อไม่ให้ Item อื่นในแผนทอดึงด้ายส่วนนั้นไปใช้</span>
              {canEdit && d && <button className="ylm-btn pri" onClick={openAdd}>+ ล็อกด้ายตัวแรก</button>}
            </div>
          )
            : rows.length === 0 ? <div className="ylm-empty">ไม่มีล็อกที่ตรงกับตัวกรอง</div>
              : (
                <div className="ylm-box">
                  <table className="ylm-table">
                    <thead>
                      <tr>
                        <th>ประเภท</th><th>YARN</th><th>LOT</th><th>ล็อกให้</th>
                        <th className="n" title="ยอดที่ล็อกไว้ตอนแรก (kg ด้าย)">ล็อก</th>
                        <th className="n" title="ใบ KP ที่ออกหลังเวลาล็อกใช้ไปแล้ว — ด้ายส่วนนั้นไปนับเป็นยอดจองของใบ KP">ออก KP แล้ว</th>
                        <th className="n" title="ล็อก − ออก KP แล้ว">คงเหลือ</th>
                        <th className="n" title="ยอดที่สต๊อกกันให้ได้จริงตอนนี้ (lot มีไม่พอ / ลูกเล็ก = กันได้น้อยกว่าคงเหลือ)">กันได้จริง</th>
                        <th title="แผนล่าสุดใช้ด้ายที่ล็อกนี้ สัปดาห์ไหนเท่าไหร่">แผนใช้</th>
                        <th>สถานะ</th><th>ล็อกโดย</th>{canEdit && <th />}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map(L => (
                        <LockRow key={L.kind + L.id} L={L} wlabels={wlabels} canEdit={canEdit}
                          busy={busy === L.id} onUnlock={() => unlock(L)} />
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
      </div>

      <div className="card ylm-logcard">
        <button className="ylm-logbtn" onClick={() => setShowLog(s => !s)}>
          {showLog ? '▾' : '▸'} ประวัติล็อก / ปลด ({(d?.log || []).length})
        </button>
        {showLog && (
          <table className="ylm-log">
            <thead><tr><th>เวลา</th><th>ใคร</th><th>ทำอะไร</th><th>รายละเอียด</th></tr></thead>
            <tbody>
              {(d?.log || []).map((g, i) => {
                const L = g.lock || {}
                const who = g.kind === 'item' ? `Item ${L.fabric}` : `${L.sc || 'SC ' + L.so} ${L.fabric}`
                return (
                  <tr key={i}>
                    <td>{String(g.at || '').slice(0, 16)}</td>
                    <td>{g.by || '-'}</td>
                    <td><span className={g.action === 'lock' ? 'ylm-lk' : 'ylm-ul'}>{g.action === 'lock' ? 'ล็อก' : 'ปลด'}</span></td>
                    <td>
                      {L.yarn}{L.color ? ` สี ${L.color}` : ''} {L.lot ? `lot ${L.lot}` : 'ทุก lot'} {kg1(L.yarn_kg)} kg → {who}
                      {g.action === 'unlock' && g.rem != null && <span className="ylm-sub"> (ตอนปลดเหลือ {kg1(g.rem)} kg)</span>}
                      {L.note && <span className="ylm-sub"> · {L.note}</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>

      {adding && d && (
        <LockDialog yarns={d.yarns || []} onClose={() => setAdding(false)}
          onDone={(m, w) => { setAdding(false); setMsg(m); setWarn(w || []); setErr(''); load() }} />
      )}
    </div>
  )
}

/* ---------- 1 แถวล็อก ---------- */
function LockRow({ L, wlabels, canEdit, busy, onUnlock }) {
  const [label, cls] = STATE[L.state] || [L.state, '']
  const short = L.state === 'active' && L.eff + 0.5 < L.rem
  const use = (L.use || []).map((v, i) => [i, v]).filter(([, v]) => v > 0)
  const idle = L.state === 'active' && (L.eff || 0) - (L.use_total || 0) > 0.5
  return (
    <tr className={L.state !== 'active' ? 'off' : ''}>
      <td><span className={'ylm-kind ' + L.kind}>{L.kind === 'item' ? 'Item' : 'SC'}</span></td>
      <td><b>{L.yarn}</b>{L.color && <div className="ylm-sub">สี {L.color}</div>}</td>
      <td>
        {L.lot || <i>ทุก lot</i>}
        {L.kind === 'item' && !L.lot && (L.lots || []).length > 0 && (
          <div className="ylm-sub">{L.lots.map(([lot, k]) => `${lot} ${kg1(k)}`).join(' · ')}</div>
        )}
      </td>
      <td>
        {L.kind === 'item'
          ? <><b>{L.fabric}</b><div className="ylm-sub">{L.open ? `ใบค้าง ${L.open} ใบ` : 'ไม่มีใบค้าง'}</div></>
          : <><b>{L.sc}</b><div className="ylm-sub">{L.fabric}</div></>}
      </td>
      <td className="n">{kg1(L.yarn_kg)}</td>
      <td className="n" title={(L.kps || []).join(', ')}>{L.kp > 0 ? kg1(L.kp) : '·'}</td>
      <td className="n">{kg1(L.rem)}</td>
      <td className={'n' + (short ? ' ylm-neg' : '')}>{L.state === 'active' ? kg1(L.eff) : '·'}</td>
      <td className="ylm-use">
        {use.length === 0 ? <span className="ylm-sub">{L.state === 'active' ? 'ยังไม่มีแผนใช้' : '·'}</span> : (
          <>
            <b>{kg1(L.use_total)}</b>
            <div className="ylm-weeks">
              {use.slice(0, 6).map(([i, v]) => <span key={i}>{wlabels[i]} {kg1(v)}</span>)}
              {use.length > 6 && <span>…</span>}
            </div>
          </>
        )}
        {use.length > 0 && idle && <div className="ylm-sub">ยังไม่มีแผนใช้ {kg1(L.eff - L.use_total)}</div>}
      </td>
      <td>
        <span className={'ylm-state ' + cls}>{label}</span>
        {L.why && <div className={'ylm-why' + (short ? ' bad' : '')}>{L.why}</div>}
      </td>
      <td>
        {L.by || '-'}<div className="ylm-sub">{String(L.at || '').slice(0, 16)}</div>
        {L.note && <div className="ylm-sub">หมายเหตุ: {L.note}</div>}
      </td>
      {canEdit && (
        <td><button className="ylm-btn sm" disabled={busy} onClick={onUnlock}>{busy ? '…' : 'ปลด'}</button></td>
      )}
    </tr>
  )
}

/* ---------- หน้าต่างล็อกด้ายให้ Item ----------
   ซ้าย: ด้าย → lot (การ์ด ; ด้ายหลายสีเลือกสีก่อนถ้าจะล็อกทุก lot) → Item ผ้า (การ์ด เฉพาะที่ใช้ด้ายนี้) → kg ด้าย
   ขวา: ผลก่อนบันทึก — ตัวเลขมาจาก backend (ตัวเดียวกับที่ตรวจตอนบันทึก) ฝั่งนี้แค่ลบกันให้ดู
   ยอดที่ใบค้างยังต้องใช้เป็นตัวช่วยกรอก ไม่ได้บังคับ · กดล็อกแล้วถามยืนยันที่แถบล่างอีกครั้งก่อนบันทึก */
function LockDialog({ yarns, onClose, onDone }) {
  const [yarn, setYarn] = useState('')
  const [lot, setLot] = useState('')
  const [color, setColor] = useState(null)       // null = ยังไม่เลือก ('' = ด้ายดิบ)
  const [opt, setOpt] = useState(null)
  const [allRoom, setAllRoom] = useState(null)   // ล็อกได้แบบ "ทุก lot" — จำไว้ตอนเลือก lot แล้ว (ตัวเลือกแบบระบุ lot ให้แค่ยอดของ lot นั้น)
  const [fab, setFab] = useState('')
  const [fabQ, setFabQ] = useState('')
  const [qty, setQty] = useState('')
  const [note, setNote] = useState('')
  const [err, setErr] = useState('')
  const [saving, setSaving] = useState(false)
  const [loadingOpt, setLoadingOpt] = useState(false)
  const [asking, setAsking] = useState(false)    // กดล็อกแล้ว → แถบล่างถามยืนยันอีกครั้งก่อนบันทึก

  const yrow = useMemo(() => {
    const k = yarn.trim().toUpperCase()
    return yarns.find(y => String(y.name).toUpperCase() === k) || null
  }, [yarn, yarns])
  const yname = yrow?.name || ''

  // ด้าย / lot / สีเปลี่ยน → ขอตัวเลือกใหม่ (หน่วงตอนพิมพ์) ; ระหว่างรอใช้ของเดิมแสดงไปก่อน แต่ยังกดล็อกไม่ได้
  useEffect(() => {
    if (!yname) { setOpt(null); setLoadingOpt(false); return }
    let alive = true
    setLoadingOpt(true)
    const t = setTimeout(() => {
      api.yarnItemLockOptions({ yarn: yname, lot, color: color ?? '' })
        .then(r => {
          if (!alive) return
          setOpt(r); setErr('')
          if (!lot && ((r.colors || []).length <= 1 || color)) setAllRoom(r.room)
        })
        .catch(e => { if (alive) setErr('โหลดตัวเลือกไม่ได้: ' + e.message) })
        .finally(() => { if (alive) setLoadingOpt(false) })
    }, 250)
    return () => { alive = false; clearTimeout(t) }
  }, [yname, lot, color])

  // แก้อะไรก็ตาม → ยกเลิกการถามยืนยันที่ค้างอยู่ (กันยืนยันค่าที่ไม่ใช่ค่าที่เห็นตอนถาม)
  useEffect(() => { setAsking(false) }, [yname, lot, color, fab, qty])

  // Esc = ถอยการถามยืนยันก่อน แล้วค่อยปิดหน้าต่าง
  useEffect(() => {
    const onKey = (e) => {
      if (e.key !== 'Escape' || saving) return
      if (asking) setAsking(false)
      else onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [asking, saving, onClose])

  // เปลี่ยนด้าย = เริ่มใหม่ทั้งชุด (kg ที่กรอกไว้เป็นของด้ายเดิม)
  const pickYarn = (v) => {
    setYarn(v); setLot(''); setColor(null); setFab(''); setFabQ(''); setQty(''); setOpt(null); setAllRoom(null)
  }
  const pickColor = (c) => { setColor(c); setLot(''); setAllRoom(null) }

  const colors = opt?.colors || []
  const multiColor = colors.length > 1
  // ล็อกทุก lot ของด้ายหลายสีต้องรู้สี — ด้ายดิบที่ปนกับด้ายสีต้องเลือก lot (backend ไม่รับแบบไม่ระบุสี)
  const allOk = !multiColor || !!color
  const lots = (opt?.lots || []).filter(l => !multiColor || color === null || l.color === color)
  const lotHit = lot ? (opt?.lots || []).find(l => l.lot === lot) : null
  const fabrics = opt?.fabrics || []
  const fabHit = fabrics.find(f => f.fabric === fab)
  const kwF = fabQ.trim().toUpperCase()
  const fabList = kwF ? fabrics.filter(f => f.fabric.includes(kwF)) : fabrics
  const nOpen = fabrics.filter(f => f.open > 0).length
  const roomKnown = !!opt && !loadingOpt && (!!lot || allOk)
  const room = roomKnown ? (opt.room ?? 0) : 0
  const n = parseFloat(qty)
  const kg = Number.isFinite(n) && n > 0 ? n : 0
  const over = roomKnown && kg > room + 0.5
  const left = room - kg
  const where = lot ? `lot ${lot}` : `ทุก lot${opt?.color ? ' สี ' + opt.color : ''}`

  const missing = []
  if (!yname) missing.push(yarn.trim() ? 'เลือกด้ายจากรายการ (ต้องเป็นด้ายที่ยังมียอดใช้ได้)' : 'เลือกด้าย')
  else if (!opt || loadingOpt) missing.push('กำลังโหลดตัวเลือก…')
  else {
    if (!lot && !allOk) missing.push(color === '' ? 'ด้ายดิบของด้ายหลายสีต้องเลือก lot' : 'เลือกสีด้าย หรือเลือก lot')
    if (!fabHit) missing.push('เลือก Item ผ้า')
    if (!kg) missing.push('ใส่ kg ด้าย')
    else if (over) missing.push(`kg เกินยอดที่ล็อกได้ (${kg1(room)} kg)`)
  }
  const ready = missing.length === 0

  // คำเตือนชุดเดียวกับที่ backend ตอบกลับหลังล็อก — ให้เห็นก่อนกด
  const warns = []
  if (lotHit?.own?.length) warns.push(`lot ${lotHit.lot} สงวนให้ลูกค้า ${lotHit.own.join(', ')} — ตรวจว่า Item นี้เป็นของลูกค้านั้น`)
  if (fabHit && !fabHit.open) warns.push('ตอนนี้ไม่มีใบค้างของ Item นี้ — ด้ายจะถูกกันไว้จนกว่าจะมีใบมาใช้ หรือกดปลด')
  else if (fabHit && fabHit.need > 0 && kg > fabHit.need + 0.5) {
    warns.push(`ล็อกมากกว่าที่ใบค้างต้องใช้ (~${kg1(fabHit.need)} kg) — ส่วนเกินถูกกันไว้เฉย ๆ`)
  }

  const setKg = (v) => setQty(String(Math.max(0, Math.floor(v * 10) / 10)))
  const bump = (dlt) => setKg(roomKnown ? Math.min(Math.max(kg + dlt, 0), room) : Math.max(kg + dlt, 0))
  const go = () => { if (ready) { if (asking) save(); else setAsking(true) } }

  async function save() {
    if (!ready || saving) return
    setSaving(true); setErr('')
    try {
      const r = await api.yarnItemLockAdd({ yarn: opt.yarn, lot, color: lot ? '' : opt.color, fabric: fabHit.fabric,
        yarn_kg: kg, note })
      onDone(`ล็อกด้าย ${opt.yarn} ${where} ${kg1(kg)} kg ให้ Item ${fabHit.fabric} แล้ว — แผนทอเห็นผลรอบรันถัดไป`, r.warn || [])
    } catch (e) { setErr(e.message); setAsking(false) }
    finally { setSaving(false) }
  }

  return (
    <div className="modal-backdrop" onClick={e => { if (e.target === e.currentTarget && !saving && !yarn) onClose() }}>
      <div className="modal-box ylm-dlg" role="dialog" aria-modal="true" aria-labelledby="ylm-dlg-t">
        <header className="ylm-dhead">
          <div>
            <div className="ylm-eyebrow">Monitor Yarn · ล็อกด้าย</div>
            <h3 id="ylm-dlg-t">ล็อกด้ายให้ Item ผ้า</h3>
            <div className="ylm-desc">
              ทุกใบของ Item นั้นใช้ด้ายที่ล็อกร่วมกัน · <b>Item อื่นใช้ไม่ได้</b> · ออกใบ KP แล้วยอดล็อกลดเอง
            </div>
          </div>
          <button className="ylm-x" onClick={onClose} disabled={saving} aria-label="ปิด" title="ปิด">×</button>
        </header>

        <div className="ylm-dbody">
          <div className="ylm-dform">
            <section className="ylm-field">
              <div className="ylm-flabel"><span className="ylm-no">1</span>ด้าย
                <small>{yarns.length.toLocaleString()} ตัวที่ยังมียอดใช้ได้</small>
              </div>
              <YarnPick yarns={yarns} value={yarn} onChange={pickYarn} />
              {yrow && <div className="ylm-fhint ok">ใช้ได้รวม {kg1(yrow.ava)} kg (ทุก lot ทุกสี)</div>}
            </section>

            <section className={'ylm-field' + (opt ? '' : ' wait')}>
              <div className="ylm-flabel"><span className="ylm-no">2</span>LOT
                <small>{opt ? `${(opt.lots || []).length} lot ที่ยังใช้ได้ · ด้ายเข้าก่อนอยู่หน้า` : 'เลือกด้ายก่อน'}</small>
              </div>
              {multiColor && (
                <div className="ylm-colors">
                  <span>สีด้าย</span>
                  {colors.map(c => (
                    <button key={c || '-'} className={'ylm-chip' + (color === c ? ' on' : '')} onClick={() => pickColor(c)}>
                      {c || 'ด้ายดิบ'}
                    </button>
                  ))}
                </div>
              )}
              {opt && (
                <div className="ylm-cards" role="radiogroup" aria-label="LOT">
                  <button role="radio" aria-checked={!lot && allOk} disabled={!allOk}
                    className={'ylm-card all' + (!lot && allOk ? ' on' : '')} onClick={() => setLot('')}>
                    <b>ทุก lot</b>
                    <span>ด้ายเข้าก่อนใช้ก่อน · ข้าม lot สงวน</span>
                    <em>{!allOk ? 'เลือกสีก่อน' : allRoom != null ? `ล็อกได้ ${kg1(allRoom)} kg` : '…'}</em>
                  </button>
                  {lots.map(l => (
                    <button key={l.lot} role="radio" aria-checked={lot === l.lot}
                      className={'ylm-card' + (lot === l.lot ? ' on' : '')} onClick={() => setLot(l.lot)}>
                      <b>{l.lot}</b>
                      <span>{[l.age != null && `${l.age} วัน`, l.color && `สี ${l.color}`].filter(Boolean).join(' · ') || ' '}</span>
                      <em>{kg1(l.ava)} kg</em>
                      {l.own.length > 0 && <i>สงวน {l.own.join(', ')}</i>}
                    </button>
                  ))}
                </div>
              )}
            </section>

            <section className={'ylm-field' + (opt ? '' : ' wait')}>
              <div className="ylm-flabel"><span className="ylm-no">3</span>Item ผ้า
                <small>{opt ? `${fabrics.length} Item ใช้ด้ายนี้ · มีใบค้าง ${nOpen}` : 'เลือกด้ายก่อน'}</small>
              </div>
              {opt && fabrics.length > 6 && (
                <input className="ylm-input sm" value={fabQ} onChange={e => setFabQ(e.target.value)} placeholder="ค้น Item…" />
              )}
              {opt && (fabList.length === 0
                ? <div className="ylm-fhint">{fabrics.length ? 'ไม่มี Item ที่ตรงคำค้น'
                  : 'ไม่พบ Item ที่ใช้ด้ายนี้ (ไม่มีใน BOM ใบ KP และบรรทัดใบ SC ที่ค้าง)'}</div>
                : (
                  <div className="ylm-cards" role="radiogroup" aria-label="Item ผ้า">
                    {fabList.slice(0, LIST_MAX).map(f => (
                      <button key={f.fabric} role="radio" aria-checked={fab === f.fabric}
                        className={'ylm-card' + (fab === f.fabric ? ' on' : '') + (f.open ? '' : ' idle')}
                        onClick={() => setFab(f.fabric)}
                        title={`${f.basis} · % ด้าย ${f.pct}${f.scs.length ? ' · SC ' + f.scs.join(', ') : ''}`}>
                        <b>{f.fabric}</b>
                        <span>{f.open ? `ใบค้าง ${f.open} ใบ` : 'ไม่มีใบค้าง'}</span>
                        <em>{f.need > 0 ? `ต้องใช้ ~${kg1(f.need)} kg` : ' '}</em>
                      </button>
                    ))}
                  </div>
                ))}
              {fabList.length > LIST_MAX && (
                <div className="ylm-fhint">แสดง {LIST_MAX} จาก {fabList.length.toLocaleString()} Item — พิมพ์ค้นเพื่อหาตัวอื่น</div>
              )}
            </section>

            <section className={'ylm-field' + (opt ? '' : ' wait')}>
              <div className="ylm-flabel"><span className="ylm-no">4</span>kg ด้าย
                <small>{roomKnown ? `ล็อกได้ไม่เกิน ${kg1(room)} kg` : ''}</small>
              </div>
              <div className="ylm-kgrow">
                <div className={'ylm-step' + (over ? ' bad' : '')}>
                  <button onClick={() => bump(-STEP)} disabled={!opt} aria-label={`ลด ${STEP} kg`}>−{STEP}</button>
                  <input type="number" min="0" step="any" value={qty} placeholder="0" disabled={!opt}
                    onChange={e => setQty(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') go() }} />
                  <span>kg</span>
                  <button onClick={() => bump(STEP)} disabled={!opt} aria-label={`เพิ่ม ${STEP} kg`}>+{STEP}</button>
                </div>
                <div className="ylm-quick">
                  {roomKnown && fabHit?.need > 0 && room > 0 && (
                    <button onClick={() => setKg(Math.min(fabHit.need, room))}>
                      ตามที่ใบค้างต้องใช้ {kg1(Math.min(fabHit.need, room))}
                    </button>
                  )}
                  {roomKnown && room > 0 && <button onClick={() => setKg(room)}>ทั้งหมดที่ล็อกได้ {kg1(room)}</button>}
                </div>
              </div>
            </section>

            <section className="ylm-field">
              <div className="ylm-flabel">หมายเหตุ<small>ไม่บังคับ</small></div>
              <input className="ylm-input" value={note} maxLength={300} onChange={e => setNote(e.target.value)}
                placeholder="เช่น กันไว้ให้ order ลูกค้า …" />
            </section>

            {opt?.note && <div className="ylm-note">{opt.note}</div>}
            {err && <div className="ylm-msg bad">{err}</div>}
          </div>

          <aside className="ylm-sum" aria-live="polite">
            <div className="ylm-sumbox">
              <div className="ylm-sumh">หลังล็อก เหลือให้งานอื่น</div>
              <div className={'ylm-big' + (!roomKnown ? ' none' : left < -0.5 ? ' bad' : '')}>
                {roomKnown ? kg1(left) : '-'}<small>kg</small>
              </div>
              <div className="ylm-scope">{!yname ? 'เลือกด้ายก่อน' : `${yname} · ${where}`}</div>
              <dl className="ylm-sumrows">
                <div><dt>ล็อกได้ตอนนี้</dt><dd>{roomKnown ? kg1(room) : '-'}</dd></div>
                <div><dt>ล็อกครั้งนี้</dt><dd className="lock">{kg ? '− ' + kg1(kg) : '0'}</dd></div>
              </dl>
              {roomKnown && <Meter v={kg} max={room} />}
            </div>

            <div className="ylm-sumbox">
              <div className="ylm-sumh">Item ผ้า</div>
              {!fabHit ? <div className="ylm-sumempty">ยังไม่ได้เลือก Item</div> : (
                <>
                  <div className="ylm-sumfab">{fabHit.fabric}</div>
                  <dl className="ylm-sumrows">
                    <div><dt>ใบค้าง</dt><dd>{fabHit.open ? `${fabHit.open} ใบ` : 'ไม่มี'}</dd></div>
                    <div><dt>ใบค้างยังต้องใช้ด้ายนี้</dt><dd>{fabHit.need > 0 ? `~${kg1(fabHit.need)} kg` : '-'}</dd></div>
                    {fabHit.need > 0 && (
                      <div className="tot"><dt>ล็อกครั้งนี้ครอบคลุม</dt><dd>{Math.round(kg / fabHit.need * 100)}%</dd></div>
                    )}
                  </dl>
                  {fabHit.need > 0 && <Meter v={kg} max={fabHit.need} />}
                  {fabHit.scs.length > 0 && (
                    <div className="ylm-scs">
                      {fabHit.scs.slice(0, 8).map(s => <span key={s}>{s}</span>)}
                      {fabHit.scs.length > 8 && <span>+{fabHit.scs.length - 8}</span>}
                    </div>
                  )}
                  <div className="ylm-sumbasis">ยอดต้องใช้ประมาณจาก {fabHit.basis}</div>
                </>
              )}
            </div>

            {warns.length > 0 && <div className="ylm-warns">{warns.map(w => <div key={w}>{w}</div>)}</div>}
          </aside>
        </div>

        <footer className="ylm-dfoot">
          {asking ? (
            <>
              <div className="ylm-need ask">
                ยืนยันล็อก <b>{opt.yarn}</b> {where} <b>{kg1(kg)} kg</b> ให้ Item <b>{fabHit.fabric}</b>?
                Item อื่นจะใช้ด้ายส่วนนี้ไม่ได้ — แผนทอเห็นผลรอบรันถัดไป
              </div>
              <button className="ylm-btn" onClick={() => setAsking(false)} disabled={saving}>กลับไปแก้</button>
              <button className="ylm-btn pri" onClick={save} disabled={saving}>{saving ? 'กำลังบันทึก…' : 'ยืนยันล็อก'}</button>
            </>
          ) : (
            <>
              <div className="ylm-need">
                {ready ? <span className="ok">พร้อมล็อก</span> : <>ยังล็อกไม่ได้: {missing.join(' · ')}</>}
              </div>
              <button className="ylm-btn" onClick={onClose}>ยกเลิก</button>
              <button className="ylm-btn pri" onClick={go} disabled={!ready}>ล็อกด้าย</button>
            </>
          )}
        </footer>
      </div>
    </div>
  )
}

/* แถบสัดส่วน v / max (เกิน = แดง) */
function Meter({ v, max }) {
  const pct = max > 0 ? Math.min(v / max, 1) * 100 : 0
  return <div className="ylm-meter"><i className={v > max + 0.5 ? 'over' : ''} style={{ width: pct + '%' }} /></div>
}

/* ช่องเลือกด้าย: พิมพ์ค้นแล้วเลือกจากรายการ (ขึ้นต้นด้วยคำค้นอยู่บน) — ↑ ↓ Enter ได้ , Esc ปิดรายการก่อนปิดหน้าต่าง */
function YarnPick({ yarns, value, onChange }) {
  const [show, setShow] = useState(false)
  const [hi, setHi] = useState(0)
  const inp = useRef(null)
  const box = useRef(null)
  const kw = value.trim().toUpperCase()
  const hits = useMemo(() => {
    if (!kw) return yarns
    const head = [], mid = []
    for (const y of yarns) {
      const nm = String(y.name).toUpperCase()
      if (nm.startsWith(kw)) head.push(y)
      else if (nm.includes(kw)) mid.push(y)
    }
    return [...head, ...mid]
  }, [yarns, kw])
  const list = hits.slice(0, LIST_MAX)

  useEffect(() => { inp.current?.focus() }, [])
  useEffect(() => { setHi(0) }, [kw])
  useEffect(() => { box.current?.querySelector('li.hi')?.scrollIntoView({ block: 'nearest' }) }, [hi, show])

  const pick = (y) => { onChange(y.name); setShow(false) }
  function onKey(e) {
    if (e.key === 'ArrowDown') { e.preventDefault(); setShow(true); setHi(i => Math.min(i + 1, list.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setHi(i => Math.max(i - 1, 0)) }
    else if (e.key === 'Enter' && show && list[hi]) { e.preventDefault(); pick(list[hi]) }
    else if (e.key === 'Escape' && show) { e.stopPropagation(); setShow(false) }
  }
  return (
    <div className="ylm-combo">
      <input ref={inp} className="ylm-input" value={value} placeholder="พิมพ์ค้นชื่อด้าย"
        role="combobox" aria-expanded={show} aria-autocomplete="list"
        onChange={e => { onChange(e.target.value); setShow(true) }} onMouseDown={() => setShow(true)}
        onBlur={() => setShow(false)} onKeyDown={onKey} />
      {show && (
        <ul className="ylm-pop" role="listbox" ref={box}>
          {list.length === 0 ? <li className="none">ไม่มีด้ายที่ตรงคำค้น</li> : list.map((y, i) => (
            <li key={y.name} role="option" aria-selected={i === hi} className={i === hi ? 'hi' : ''}
              onMouseDown={e => e.preventDefault()} onMouseEnter={() => setHi(i)} onClick={() => pick(y)}>
              <b>{y.name}</b><span>ใช้ได้ {kg1(y.ava)} kg</span>
            </li>
          ))}
          {hits.length > LIST_MAX && (
            <li className="more">แสดง {LIST_MAX} จาก {hits.length.toLocaleString()} ตัว — พิมพ์ต่อเพื่อหาให้แคบลง</li>
          )}
        </ul>
      )}
    </div>
  )
}

// SaveStatus.jsx — ป้ายบอกสถานะการบันทึกมุมจอ
//
// ต้องมีคู่กับการเซฟอัตโนมัติเสมอ: พอไม่มีปุ่ม "บันทึก" ให้กด ผู้ใช้จะไม่มีทางรู้ว่า
// สิ่งที่เพิ่งแก้เข้าไฟล์แล้วหรือยัง — ป้ายนี้คือคำตอบนั้น (และเป็นที่อยู่ของปุ่มเลิกทำ)
import React from 'react'

const STATUS_TEXT = {
  idle: '',
  pending: 'กำลังจะบันทึก…',
  saving: 'กำลังบันทึก…',
  saved: 'บันทึกแล้ว',
  error: 'บันทึกไม่สำเร็จ',
}

/** ป้ายสถานะมุมจอ — ต้องมี เพราะไม่มีปุ่มบันทึกให้ผู้ใช้ยืนยันด้วยตาอีกแล้ว */
export default function SaveStatus({ status, error, at, onRetry, onUndo, canUndo }) {
  if (status === 'idle' && !canUndo) return null
  const time = at ? new Date(at).toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit' }) : ''
  return (
    <div className={`save-status save-${status}`}>
      <span className="save-dot" />
      <span>
        {STATUS_TEXT[status] || ''}
        {status === 'saved' && time ? ` ${time}` : ''}
        {status === 'error' && error ? ` — ${error.message || error}` : ''}
      </span>
      {status === 'error' && onRetry && (
        <button type="button" className="save-btn" onClick={onRetry}>ลองใหม่</button>
      )}
      {canUndo && onUndo && (
        <button type="button" className="save-btn" onClick={onUndo} title="Ctrl+Z">เลิกทำ</button>
      )}
    </div>
  )
}

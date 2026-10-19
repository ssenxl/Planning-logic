import React from 'react'
import McAvailability from './McAvailability.jsx'
import ErrorBoundary from './ErrorBoundary.jsx'

/* หน้าที่ลิงก์บนการ์ด Teams พามา (/knitplan/?share=mc-ava&k=...) — Machine Availability หน้าเดียว ไม่ต้อง login
   ไม่มีแถบเมนู จึงไปหน้าอื่นไม่ได้ ; ฝั่ง backend ก็เปิดให้แค่ API ของหน้านี้ (/api/share/mc-ava*) */
export default function ShareMcAva() {
  const k = new URLSearchParams(location.search).get('k') || ''
  return (
    <div className="app">
      <main className="content full">
        <ErrorBoundary>
          {k ? <McAvailability shareKey={k} />
            : <div className="share-bad">ลิงก์ไม่ครบ — เปิดจากปุ่มบนการ์ด Machine Availability ใน Teams</div>}
        </ErrorBoundary>
      </main>
    </div>
  )
}

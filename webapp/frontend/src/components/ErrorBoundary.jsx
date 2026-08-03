import React from 'react'

// กันหน้าขาว: ถ้ามี error ระหว่าง render ที่ไหนสักที่ React จะ unmount ทั้งต้นไม้ (หน้าว่างเปล่า
// ไม่มีแม้แต่แถบเมนู) ทำให้ user ไม่รู้ว่าเกิดอะไรขึ้น — ครอบด้วย boundary นี้แล้วจะได้ข้อความ
// + ปุ่มลองใหม่/รีโหลด แทน และมีข้อความ error ให้ก๊อปส่งให้ผู้ดูแลได้
export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { err: null }
  }

  static getDerivedStateFromError(err) {
    return { err }
  }

  componentDidCatch(err, info) {
    // log ไว้ที่ console ให้เปิด F12 ดู stack เต็มได้
    console.error('[ErrorBoundary]', err, info?.componentStack)
  }

  render() {
    const { err } = this.state
    if (!err) return this.props.children
    return (
      <div className="errbound">
        <h3>⚠ หน้านี้เกิดข้อผิดพลาด</h3>
        <p>
          ลองกด <b>ลองใหม่</b> ก่อน — ถ้ายังไม่หาย ให้กด <b>โหลดหน้าใหม่</b>
          {' '}และส่งข้อความด้านล่างให้ผู้ดูแลระบบ
        </p>
        <pre className="errbound-msg">{String(err?.stack || err?.message || err)}</pre>
        <div className="errbound-btns">
          <button className="primary" onClick={() => this.setState({ err: null })}>ลองใหม่</button>
          <button onClick={() => window.location.reload()}>โหลดหน้าใหม่</button>
        </div>
      </div>
    )
  }
}

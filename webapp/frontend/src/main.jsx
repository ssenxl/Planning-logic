import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import ShareMcAva from './components/ShareMcAva.jsx'
import './styles.css'

// ลิงก์จากการ์ด Teams (/knitplan/?share=mc-ava&k=...) → หน้า Machine Availability หน้าเดียว ไม่ผ่านหน้า login
// (query string ไม่ใช่ path ย่อย: build ด้วย base './' ถ้าเป็น /share/... ไฟล์ JS จะถูกหาผิดที่ → หน้าขาว)
const isShare = new URLSearchParams(location.search).get('share') === 'mc-ava'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    {isShare ? <ShareMcAva /> : <App />}
  </React.StrictMode>,
)

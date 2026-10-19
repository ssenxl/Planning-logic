import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// dev: proxy /api → backend (port 8080). prod: เสิร์ฟจาก origin เดียวกับ backend
export default defineConfig({
  plugins: [react()],
  // asset path แบบสัมพัทธ์ — ใช้ได้ทั้งเสิร์ฟใต้ /knitplan/ (ผ่าน nginx port 80)
  // และเข้าตรงที่ root ของ container (:8080) ตอน debug
  base: './',
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})

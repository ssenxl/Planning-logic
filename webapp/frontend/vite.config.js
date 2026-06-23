import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// dev: proxy /api → backend (port 8080). prod: เสิร์ฟจาก origin เดียวกับ backend
export default defineConfig({
  plugins: [react()],
  base: '/',
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

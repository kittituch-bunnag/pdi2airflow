import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// During development, Vite serves on :5173 and proxies /api to FastAPI on :8765.
// For production, run `npm run build` and let FastAPI serve the dist/ folder.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.BACKEND_URL ?? 'http://localhost:8765',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Build straight into the FastAPI static dir; proxy /api to a local uvicorn in dev.
export default defineConfig({
  plugins: [react()],
  build: { outDir: '../backend/static', emptyOutDir: true, chunkSizeWarningLimit: 1500 },
  server: { proxy: { '/api': 'http://localhost:8000' } },
})

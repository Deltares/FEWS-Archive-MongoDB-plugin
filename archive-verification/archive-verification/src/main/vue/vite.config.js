import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path';

export default defineConfig(({mode}) => ({
  plugins: [vue()],
  base: "/verification/",
  build: {
    outDir: resolve(__dirname, '../resources/static'),
    minify: mode !== 'development',
    sourcemap: mode === 'development'
  },
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  }
}))

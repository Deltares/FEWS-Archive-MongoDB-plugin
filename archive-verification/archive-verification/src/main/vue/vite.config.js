import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path';
import { mockGraphql } from './mock/graphql.js'

export default defineConfig(({mode}) => ({
  plugins: [vue(), mockGraphql()],
  base: "./",
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

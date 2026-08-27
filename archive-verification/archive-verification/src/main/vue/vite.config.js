import {resolve} from 'node:path'
import {defineConfig} from 'vite'
import vue from '@vitejs/plugin-vue'
import vuetify from 'vite-plugin-vuetify'

export default defineConfig(({mode}) => ({
  plugins: [vue(), vuetify({autoImport: true})],
  base: './',
  build: {
    outDir: resolve(import.meta.dirname, '../resources/static'),
    minify: mode !== 'development',
    sourcemap: mode === 'development',
  },
  resolve: {
    alias: {
      '@': resolve(import.meta.dirname, 'src'),
    },
  },
}))

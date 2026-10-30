import {mergeConfig, defineConfig} from 'vitest/config'
import viteConfig from './vite.config.js'

// Reuse the app's own config (notably the @ alias) so tests resolve imports the
// same way the build does, rather than keeping a second copy that can drift.
export default mergeConfig(
  viteConfig({mode: 'test', command: 'serve'}),
  defineConfig({
    test: {
      environment: 'happy-dom',
      // Both suffixes: Vitest's docs use .test.js, Vue's scaffold uses .spec.js.
      // Matching only one means a file written the other way is silently skipped.
      include: ['src/**/*.{test,spec}.js'],
      // vite-plugin-vuetify rewrites <v-icon> into a real import of VIcon, which
      // pulls in VIcon.css. Node cannot load .css on its own, so Vuetify has to
      // go through Vite's transform rather than be treated as an external module.
      server: {deps: {inline: ['vuetify']}},
    },
  }),
)

import js from '@eslint/js'
import globals from 'globals'
import pluginVue from 'eslint-plugin-vue'
import skipFormatting from '@vue/eslint-config-prettier/skip-formatting'

export default [
  {ignores: ['dist/**', '../resources/static/**']},
  js.configs.recommended,
  ...pluginVue.configs['flat/recommended'],
  skipFormatting,
  {
    files: ['src/**/*.{js,vue}'],
    languageOptions: {globals: globals.browser},
  },
  {
    files: ['*.config.js'],
    languageOptions: {globals: globals.node},
  },
]

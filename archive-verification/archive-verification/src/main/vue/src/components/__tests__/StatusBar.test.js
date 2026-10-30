import {describe, it, expect} from 'vitest'
import {mount} from '@vue/test-utils'
import StatusBar from '../StatusBar.vue'

// Stub the Vuetify wrappers so the assertions are about StatusBar's own logic -
// which alert is shown, and what text it carries.
const global = {
  stubs: {
    'v-overlay': {props: ['modelValue'], template: '<div class="overlay" :data-shown="String(modelValue)"><slot /></div>'},
    'v-alert': {
      props: ['modelValue', 'type'],
      template: '<div class="alert" :data-type="type" :data-shown="String(modelValue)"><slot /></div>',
    },
    'v-progress-circular': true,
  },
}

const bar = (props = {}) => mount(StatusBar, {props, global})
const alert = (w, type) => w.findAll('.alert').find((a) => a.attributes('data-type') === type)
const shown = (w, type) => alert(w, type).attributes('data-shown') === 'true'

describe('StatusBar', () => {
  it('shows nothing when the page is idle', () => {
    const w = bar()
    expect(w.get('.overlay').attributes('data-shown')).toBe('false')
    for (const type of ['error', 'warning', 'success']) expect(shown(w, type)).toBe(false)
  })

  it('shows the overlay while loading', () => {
    expect(bar({loading: true}).get('.overlay').attributes('data-shown')).toBe('true')
  })

  it('shows an error alert carrying the message, not the Error object', () => {
    const w = bar({error: new Error('locations must be an array')})
    expect(shown(w, 'error')).toBe(true)
    expect(alert(w, 'error').text()).toBe('locations must be an array')
  })

  // error is an Error instance and the alert wants a boolean. The stub renders
  // String(modelValue), so "true" proves the `!!` ran - an un-coerced Error would
  // stringify to "Error: x" and Vue would warn about the prop type.
  it('coerces the error object to a boolean for the alert', () => {
    expect(alert(bar({error: new Error('x')}), 'error').attributes('data-shown')).toBe('true')
  })

  it('shows success and warning independently', () => {
    const w = bar({success: '{"createClass":"7"}', warning: 'two filters failed'})
    expect(alert(w, 'success').text()).toBe('{"createClass":"7"}')
    expect(alert(w, 'warning').text()).toBe('two filters failed')
    expect(shown(w, 'error')).toBe(false)
  })

  // The Test buttons join their per-filter results with newlines, so the alerts
  // that can carry them must preserve line breaks.
  it('preserves newlines in the success and warning alerts', () => {
    const w = bar({success: 'a\nb', warning: 'c\nd'})
    for (const type of ['success', 'warning']) {
      expect(alert(w, type).attributes('style')).toContain('pre-line')
    }
    expect(alert(w, 'error').attributes('style') ?? '').not.toContain('pre-line')
  })
})

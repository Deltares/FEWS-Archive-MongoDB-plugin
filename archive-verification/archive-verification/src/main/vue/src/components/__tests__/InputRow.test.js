import {describe, it, expect} from 'vitest'
import {mount} from '@vue/test-utils'
import InputRow from '../InputRow.vue'

describe('InputRow', () => {
  it('wires the label to the field it renders', () => {
    const w = mount(InputRow, {props: {label: 'Name', modelValue: 'x'}})
    const id = w.get('input').attributes('id')
    expect(id).toBeTruthy()
    expect(w.get('label').attributes('for')).toBe(id)
  })

  it('passes arbitrary attributes through to the input', () => {
    const w = mount(InputRow, {props: {label: 'Port'}, attrs: {type: 'number', min: '1024', readonly: true}})
    const input = w.get('input')
    expect(input.attributes('type')).toBe('number')
    expect(input.attributes('min')).toBe('1024')
    expect(input.attributes('readonly')).toBeDefined()
  })

  it('emits a string for a text field', async () => {
    const w = mount(InputRow, {props: {label: 'Name', modelValue: ''}})
    await w.get('input').setValue('hello')
    expect(w.emitted('update:modelValue')[0]).toEqual(['hello'])
  })

  // Binding :value/@input by hand loses the numeric casting that v-model on a
  // native number input gives you, so InputRow restores it - Int! variables
  // would otherwise be sent as strings.
  it('emits a number for a number field', async () => {
    const w = mount(InputRow, {props: {label: 'Port', modelValue: 0}, attrs: {type: 'number'}})
    await w.get('input').setValue('25')
    const [emitted] = w.emitted('update:modelValue')[0]
    expect(emitted).toBe(25)
    expect(typeof emitted).toBe('number')
  })

  it('emits null rather than NaN when a number field is cleared', async () => {
    const w = mount(InputRow, {props: {label: 'Port', modelValue: 25}, attrs: {type: 'number'}})
    await w.get('input').setValue('')
    expect(w.emitted('update:modelValue')[0]).toEqual([null])
  })

  it('replaces the input with the default slot and hands it the id and classes', () => {
    const w = mount(InputRow, {
      props: {label: 'Filters'},
      slots: {default: '<textarea :id="params.id" :class="params.fieldClass" />'},
      global: {},
    })
    expect(w.find('input').exists()).toBe(false)
    const textarea = w.get('textarea')
    expect(textarea.attributes('id')).toBe(w.get('label').attributes('for'))
    expect(textarea.classes()).toContain('input-data')
  })
  // .input-label is a fixed width and ellipsises long names, so the hover text
  // falls back to the label rather than being annotated row by row.
  it('falls back to the label for hover text', () => {
    const w = mount(InputRow, {props: {label: 'fewsVerificationDbAesPassword'}})
    expect(w.get('label').attributes('title')).toBe('fewsVerificationDbAesPassword')
  })

  it('lets an explicit title win', () => {
    const w = mount(InputRow, {props: {label: 'lastUpdated', title: 'abc123'}})
    expect(w.get('label').attributes('title')).toBe('abc123')
  })
})

describe('InputRow field types', () => {
  it('renders a checkbox and emits a boolean', async () => {
    const w = mount(InputRow, {props: {label: 'Active', type: 'checkbox', modelValue: false}})
    await w.get('input[type="checkbox"]').setValue(true)
    expect(w.emitted('update:modelValue')[0]).toEqual([true])
  })

  it('renders a textarea and emits its text', async () => {
    const w = mount(InputRow, {props: {label: 'Template', type: 'textarea', modelValue: ''}})
    await w.get('textarea').setValue('line1\nline2')
    expect(w.emitted('update:modelValue')[0]).toEqual(['line1\nline2'])
  })

  it('builds select options from plain values', () => {
    const w = mount(InputRow, {props: {label: 'Time', type: 'select', options: ['local', 'UTC'], modelValue: 'UTC'}})
    expect(w.findAll('option').map((o) => o.attributes('value'))).toEqual(['local', 'UTC'])
    expect(w.get('select').element.value).toBe('UTC')
  })

  it('builds select options from objects via optionKey', () => {
    const w = mount(InputRow, {
      props: {
        label: 'Observed',
        type: 'select',
        options: [{Name: 'a'}, {Name: 'b'}],
        optionKey: 'Name',
        modelValue: null,
      },
    })
    expect(w.findAll('option').map((o) => o.attributes('value'))).toEqual(['a', 'b'])
  })

  // <select multiple> reads and writes an array; this is why InputRow uses a real
  // v-model internally rather than binding :value/@change by hand.
  it('emits an array from a multiple select', async () => {
    const w = mount(InputRow, {
      props: {label: 'Forecasts', type: 'select', options: ['a', 'b', 'c'], modelValue: []},
      attrs: {multiple: true},
    })
    const options = w.findAll('option')
    options[0].element.selected = true
    options[2].element.selected = true
    await w.get('select').trigger('change')
    expect(w.emitted('update:modelValue').at(-1)[0]).toEqual(['a', 'c'])
  })
})

import {describe, it, expect} from 'vitest'
import {mount} from '@vue/test-utils'
import SelectTable from '../SelectTable.vue'

const items = [
  {_id: '1', Name: 'alpha', Extra: 'x'},
  {_id: '2', Name: 'beta', Extra: 'y'},
]

// v-table is a Vuetify component; stub it so these tests stay about SelectTable.
const global = {stubs: {'v-table': {template: '<table><slot /></table>'}, 'v-icon': true}}

const mountTable = (props = {}, slots = {}) => mount(SelectTable, {props: {items, labelHeader: 'Name', modelValue: {}, ...props}, slots, global})

describe('SelectTable', () => {
  it('renders one row per item with a radio and a label', () => {
    const w = mountTable()
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(w.findAll('tbody label').map((l) => l.text())).toEqual(['alpha', 'beta'])
  })

  it('pairs each radio with its own label', () => {
    const rows = mountTable().findAll('tbody tr')
    for (const row of rows) {
      expect(row.get('input[type="radio"]').attributes('id')).toBe(row.get('label').attributes('for'))
    }
  })

  it('shows the label column header', () => {
    expect(mountTable({labelHeader: 'environment'}).get('thead').text()).toContain('environment')
  })

  it('takes the label from a custom accessor', () => {
    const w = mountTable({label: (item) => item.Extra})
    expect(w.findAll('tbody label').map((l) => l.text())).toEqual(['x', 'y'])
  })

  it('checks the radio matching the current selection', () => {
    const w = mountTable({modelValue: {_id: '2'}})
    const radios = w.findAll('input[type="radio"]')
    expect(radios[0].element.checked).toBe(false)
    expect(radios[1].element.checked).toBe(true)
  })

  it('emits a copy of the row on select, not the row itself', async () => {
    const w = mountTable()
    await w.findAll('input[type="radio"]')[0].trigger('change')
    const [emitted] = w.emitted('update:modelValue')[0]
    expect(emitted).toEqual(items[0])
    expect(emitted).not.toBe(items[0])
  })

  it('applies onSelect so a page can reshape the row for its form', async () => {
    const w = mountTable({onSelect: (item) => ({...item, Extra: JSON.stringify(item.Extra)})})
    await w.findAll('input[type="radio"]')[0].trigger('change')
    expect(w.emitted('update:modelValue')[0][0].Extra).toBe('"x"')
  })

  it('renders the headers and cells slots, handing cells the item and row id', () => {
    const w = mountTable(
      {},
      {
        headers: '<th>Extra</th>',
        cells: '<td class="extra">{{ params.item.Extra }}-{{ params.id }}</td>',
      },
    )
    expect(w.get('thead').text()).toContain('Extra')
    expect(w.findAll('td.extra').map((c) => c.text())).toEqual(['x-r_1', 'y-r_2'])
  })
})

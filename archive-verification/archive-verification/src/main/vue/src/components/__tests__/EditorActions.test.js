import {describe, it, expect} from 'vitest'
import {mount} from '@vue/test-utils'
import EditorActions from '../EditorActions.vue'

const global = {stubs: {'v-btn': {template: '<button><slot /></button>'}}}
const actions = (slots = {}) => mount(EditorActions, {slots, global})

describe('EditorActions', () => {
  it('offers Create, Update and Delete in that order', () => {
    expect(
      actions()
        .findAll('button')
        .map((b) => b.text()),
    ).toEqual(['Create', 'Update', 'Delete'])
  })

  it('emits one event per button, and only that one', async () => {
    const events = ['create', 'update', 'remove']
    for (const [index, event] of events.entries()) {
      const w = actions()
      await w.findAll('button')[index].trigger('click')
      expect(w.emitted(event)).toHaveLength(1)
      for (const other of events.filter((e) => e !== event)) expect(w.emitted(other)).toBeUndefined()
    }
  })

  // Forecast, Normal and Observed add a Test button through the slot.
  it('appends slotted buttons after the standard three', () => {
    const w = actions({default: '<button>Test</button>'})
    expect(w.findAll('button').map((b) => b.text())).toEqual(['Create', 'Update', 'Delete', 'Test'])
  })
})

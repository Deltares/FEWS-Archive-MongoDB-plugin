import {describe, it, expect, vi, afterEach} from 'vitest'
import {mount, flushPromises} from '@vue/test-utils'
import {useEditor} from '../useEditor'

// useEditor calls onMounted, so it has to run inside a component instance.
function harness(collection, sortKey) {
  let api
  mount({
    setup() {
      api = useEditor('query {x}', collection, sortKey)
      return () => null
    },
  })
  return api
}

function respondWith(...responses) {
  let call = 0
  globalThis.fetch = vi.fn(async () => {
    const body = responses[Math.min(call++, responses.length - 1)]
    if (body instanceof Error) throw body
    return {ok: true, status: 200, json: async () => ({data: body})}
  })
}

afterEach(() => vi.restoreAllMocks())

describe('useEditor', () => {
  it('loads the collection on mount', async () => {
    respondWith({thingN: [{_id: '1', Name: 'a'}]})
    const api = harness('thingN')
    await flushPromises()
    expect(api.items.value).toEqual([{_id: '1', Name: 'a'}])
  })

  it('keeps the whole response so sibling root fields stay reachable', async () => {
    respondWith({thingN: [], otherN: [{_id: '9'}]})
    const api = harness('thingN')
    await flushPromises()
    expect(api.data.value.otherN).toEqual([{_id: '9'}])
  })

  it('yields an empty list rather than undefined before the first load', () => {
    respondWith({thingN: []})
    expect(harness('thingN').items.value).toEqual([])
  })

  it('sorts by Name by default without mutating the source array', async () => {
    const rows = [{Name: 'c'}, {Name: 'a'}, {Name: 'b'}]
    respondWith({thingN: rows})
    const api = harness('thingN')
    await flushPromises()
    expect(api.sorted.value.map((r) => r.Name)).toEqual(['a', 'b', 'c'])
    expect(rows.map((r) => r.Name)).toEqual(['c', 'a', 'b'])
  })

  it('sorts by a custom key, stringifying so booleans compare', async () => {
    respondWith({thingN: [{flag: true}, {flag: false}]})
    const api = harness('thingN', (r) => `${r.flag}`)
    await flushPromises()
    expect(api.sorted.value.map((r) => r.flag)).toEqual([false, true])
  })

  it('reports the mutation result and reloads', async () => {
    respondWith({thingN: [{Name: 'before'}]}, {thingN: [{Name: 'after'}]})
    const api = harness('thingN')
    await flushPromises()

    await api.run(async () => ({createThing: '7'}))
    expect(api.success.value).toBe('{"createThing":"7"}')
    expect(api.items.value).toEqual([{Name: 'after'}])
    expect(api.error.value).toBeNull()
  })

  it('captures a thrown mutation into error and clears it next run', async () => {
    respondWith({thingN: []})
    const api = harness('thingN')
    await flushPromises()

    await api.run(() => {
      throw new Error('bad json')
    })
    expect(api.error.value.message).toBe('bad json')
    expect(api.success.value).toBeNull()

    await api.run()
    expect(api.error.value).toBeNull()
  })

  it('clears loading even when the mutation throws', async () => {
    respondWith({thingN: []})
    const api = harness('thingN')
    await flushPromises()

    await api.run(() => {
      throw new Error('boom')
    })
    expect(api.loading.value).toBe(false)
  })
})

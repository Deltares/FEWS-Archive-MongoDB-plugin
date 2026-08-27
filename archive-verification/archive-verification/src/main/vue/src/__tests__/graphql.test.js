import {describe, it, expect, vi, afterEach} from 'vitest'
import {graphql} from '../graphql'

function mockFetch(status, body) {
  const calls = []
  globalThis.fetch = vi.fn(async (url, init) => {
    calls.push({url, init})
    return {
      ok: status >= 200 && status < 300,
      status,
      statusText: {200: 'OK', 401: 'Unauthorized', 500: 'Server Error'}[status],
      json: async () => body,
    }
  })
  return calls
}

afterEach(() => vi.restoreAllMocks())

describe('graphql', () => {
  it('POSTs the query and variables to ./graphql as JSON', async () => {
    const calls = mockFetch(200, {data: {classN: []}})
    await graphql('query {classN {_id}}', {limit: 5})

    expect(calls).toHaveLength(1)
    expect(calls[0].url).toBe('./graphql')
    expect(calls[0].init.method).toBe('POST')
    expect(calls[0].init.headers).toEqual({'content-type': 'application/json'})
    expect(JSON.parse(calls[0].init.body)).toEqual({
      query: 'query {classN {_id}}',
      variables: {limit: 5},
    })
  })

  it('returns the data payload, not the envelope', async () => {
    mockFetch(200, {data: {classN: [{_id: '1'}]}})
    await expect(graphql('query {classN {_id}}')).resolves.toEqual({classN: [{_id: '1'}]})
  })

  it('throws with every GraphQL error message joined', async () => {
    mockFetch(200, {data: null, errors: [{message: 'first'}, {message: 'second'}]})
    await expect(graphql('query {bogus}')).rejects.toThrow('first\nsecond')
  })

  it('throws on a non-2xx response before parsing the body', async () => {
    mockFetch(500, {})
    await expect(graphql('query {x}')).rejects.toThrow('500 Server Error')
  })

  it('treats an empty errors array as success', async () => {
    mockFetch(200, {data: {ok: 1}, errors: []})
    await expect(graphql('query {ok}')).resolves.toEqual({ok: 1})
  })

  it('sends undefined variables rather than an empty object', async () => {
    const calls = mockFetch(200, {data: {}})
    await graphql('query {x}')
    expect(JSON.parse(calls[0].init.body)).toEqual({query: 'query {x}'})
  })
})

import { afterEach, expect, it, vi } from 'vitest'
import { forward } from './server'

vi.mock('server-only', () => ({}))
vi.mock('next/headers', () => ({ cookies: () => Promise.resolve({ get: () => undefined }) }))

afterEach(() => {
  vi.unstubAllGlobals()
})

it('passes the controller answer through with its status', async () => {
  vi.stubGlobal('fetch', () => Promise.resolve(Response.json({ error: 'busy' }, { status: 429 })))
  const res = await forward('/api/chaos')
  expect(res.status).toBe(429)
  expect(await res.json()).toEqual({ error: 'busy' })
})

it('answers 502 when the controller cannot be reached', async () => {
  vi.stubGlobal('fetch', () => Promise.reject(new TypeError('fetch failed')))
  expect((await forward('/api/heal')).status).toBe(502)
})

it('answers 502 when the controller body breaks off', async () => {
  const body = new ReadableStream({
    start(c) {
      c.error(new Error('connection reset'))
    },
  })
  vi.stubGlobal('fetch', () => Promise.resolve(new Response(body)))
  expect((await forward('/api/snapshot')).status).toBe(502)
})
